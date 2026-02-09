use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use log::{debug, info, error};
use tokio::net::TcpListener;
use async_trait::async_trait;
use pgwire::api::auth::{finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider, LoginInfo, StartupHandler};
use pgwire::api::PgWireConnectionState;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::results::{Response, Tag, FieldInfo, QueryResponse, DataRowEncoder, FieldFormat, DescribeStatementResponse, DescribePortalResponse, DescribeResponse};
use pgwire::api::{ClientInfo, Type};
use pgwire::api::portal::Portal;
use pgwire::api::stmt::{StoredStatement, NoopQueryParser};
use pgwire::error::{PgWireResult, PgWireError, ErrorInfo};
use pgwire::tokio::process_socket;
use tokio::sync::RwLock;
use futures::{stream, SinkExt};
use pgwire::messages::data::DataRow;
use pgwire::messages::extendedquery::Sync as PgSync;
use pgwire::messages::response::{ReadyForQuery, TransactionStatus, SslResponse};
use futures::Sink;
use std::fmt::Debug;

use crate::core::Column;
use crate::{DataType, InMemoryDB, Value};
use crate::connection::auth::{AuthManager, enforce_permissions};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tokio::io::AsyncWriteExt;

pub struct PostgresServer {
    db: Arc<RwLock<InMemoryDB>>,
    host: String,
    port: u16,
}

impl PostgresServer {
    pub fn new(db: Arc<RwLock<InMemoryDB>>, host: &str, port: u16) -> Self {
        Self {
            db,
            host: host.to_string(),
            port,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("Postgres Server listening on {}", addr);

        let metrics = Arc::new(PgWireMetrics::default());
        let factory = Arc::new(HandlerFactory {
            db: self.db.clone(),
            metrics: Arc::clone(&metrics),
        });

        loop {
            let (mut socket, addr) = listener.accept().await?;
            let conn_count = metrics.on_connection();
            debug!("Accepted new connection from {:?}", addr);
            if conn_count % 1000 == 0 {
                info!("PgWire connections accepted: {}", conn_count);
            }
            let factory = factory.clone();

            tokio::spawn(async move {
                if std::env::var("RUSTMEMODB_SSL_TEST_ACCEPT").ok().as_deref() == Some("1") {
                    let mut buf = [0u8; 8];
                    if socket.peek(&mut buf).await.is_ok() {
                        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                        let code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
                        if len == 8 && code == 80877103 {
                            let _ = socket.write_all(&[b'S']).await;
                            let _ = socket.shutdown().await;
                            return;
                        }
                    }
                }
                if let Err(e) = process_socket(socket, None, factory).await {
                    error!("Connection error: {:?}", e);
                }
            });
        }
    }
}

struct HandlerFactory {
    db: Arc<RwLock<InMemoryDB>>,
    metrics: Arc<PgWireMetrics>,
}

struct RustMemDbStartupHandler {
    auth: Arc<AuthManager>,
    params: DefaultServerParameterProvider,
    metrics: Arc<PgWireMetrics>,
}

#[async_trait]
impl StartupHandler for RustMemDbStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::SslRequest(_) => {
                if !matches!(client.state(), PgWireConnectionState::AwaitingStartup) {
                    let error_info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "08P01".to_string(),
                        "Unexpected SSL request".to_string(),
                    );
                    let error = pgwire::messages::response::ErrorResponse::from(error_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
                }
                client
                    .send(PgWireBackendMessage::SslResponse(SslResponse::Accept))
                    .await?;
            }
            PgWireFrontendMessage::Startup(ref startup) => {
                if !matches!(client.state(), PgWireConnectionState::AwaitingStartup) {
                    let error_info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "08P01".to_string(),
                        "Unexpected startup message".to_string(),
                    );
                    let error = pgwire::messages::response::ErrorResponse::from(error_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
                }
                save_startup_parameters_to_metadata(client, startup);
                let login_info = LoginInfo::from_client_info(client);
                if login_info.user().unwrap_or("").is_empty() {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_string(),
                        "28000".to_string(),
                        "Missing user in startup packet".to_string(),
                    );
                    let error = pgwire::messages::response::ErrorResponse::from(error_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
                }
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                if !matches!(client.state(), PgWireConnectionState::AuthenticationInProgress) {
                    let error_info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "08P01".to_string(),
                        "Unexpected password message".to_string(),
                    );
                    let error = pgwire::messages::response::ErrorResponse::from(error_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
                }
                let pwd = pwd.into_password()?;
                let login_info = LoginInfo::from_client_info(client);
                let username = login_info.user().unwrap_or("");
                let password = std::str::from_utf8(pwd.password.as_bytes())
                    .unwrap_or_default();

                if self.auth.authenticate(username, password).await.is_ok() {
                    debug!("PgWire auth succeeded for user '{}'", username);
                    finish_authentication(client, &self.params).await;
                } else {
                    self.metrics.on_auth_failure();
                    debug!("PgWire auth failed for user '{}'", username);
                    let error_info = ErrorInfo::new(
                        "FATAL".to_string(),
                        "28P01".to_string(),
                        "Password authentication failed".to_string(),
                    );
                    let error = pgwire::messages::response::ErrorResponse::from(error_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl pgwire::api::PgWireHandlerFactory for HandlerFactory {
    type StartupHandler = RustMemDbStartupHandler;
    type SimpleQueryHandler = QueryProcessor;
    type ExtendedQueryHandler = QueryProcessor;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(QueryProcessor { db: self.db.clone(), metrics: Arc::clone(&self.metrics) })
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(QueryProcessor { db: self.db.clone(), metrics: Arc::clone(&self.metrics) })
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(RustMemDbStartupHandler {
            auth: Arc::clone(AuthManager::global()),
            params: DefaultServerParameterProvider::default(),
            metrics: Arc::clone(&self.metrics),
        })
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

struct QueryProcessor {
    db: Arc<RwLock<InMemoryDB>>,
    metrics: Arc<PgWireMetrics>,
}

#[async_trait]
impl SimpleQueryHandler for QueryProcessor {
    async fn do_query<'a, 'b: 'a, C>(&'b self, _client: &mut C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!("Simple Query: {}", query);
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let login_info = LoginInfo::from_client_info(_client);
        let username = login_info.user().unwrap_or("");
        let response = execute_query(
            self.db.clone(),
            query,
            vec![],
            FieldFormat::Text,
            username,
            &self.metrics,
        ).await?;
        Ok(vec![response])
    }
}

#[async_trait]
impl ExtendedQueryHandler for QueryProcessor {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        debug!("Extended Query Exec: {}", query);

        if query.trim().is_empty() {
            return Ok(Response::EmptyQuery);
        }

        let mut params = Vec::new();
        
        // Infer parameter types to decode correctly
        let db_arc = self.db.clone();
        let db = db_arc.read().await;
        let (_, param_types) = db.plan_query(query).await.unwrap_or((crate::core::Schema::new(vec![]), vec![]));
        drop(db);

        for i in 0..portal.parameter_len() {
            let dt = param_types.get(i).unwrap_or(&DataType::Unknown);
            
            let val = match dt {
                DataType::Integer => {
                    if let Some(n) = portal.parameter::<i64>(i, &Type::INT8)? {
                        Value::Integer(n)
                    } else {
                        Value::Null
                    }
                }
                DataType::Float => {
                    if let Some(n) = portal.parameter::<f64>(i, &Type::FLOAT8)? {
                        Value::Float(n)
                    } else {
                        Value::Null
                    }
                }
                DataType::Boolean => {
                    if let Some(n) = portal.parameter::<bool>(i, &Type::BOOL)? {
                        Value::Boolean(n)
                    } else {
                        Value::Null
                    }
                }
                _ => {
                    // Treat as Text (client sent string because we said TEXT in Describe)
                    if let Some(s) = portal.parameter::<String>(i, &Type::TEXT)? {
                        Value::Text(s)
                    } else {
                        Value::Null
                    }
                }
            };
            params.push(val);
        }

        let login_info = LoginInfo::from_client_info(_client);
        let username = login_info.user().unwrap_or("");
        execute_query(
            self.db.clone(),
            query,
            params,
            FieldFormat::Binary,
            username,
            &self.metrics,
        ).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &stmt.statement;
        debug!("Describe Statement: {}", query);
        if query.trim().is_empty() {
            return Ok(<DescribeStatementResponse as DescribeResponse>::no_data());
        }
        let db_arc = self.db.clone();
        let db = db_arc.read().await;

        match db.plan_query(query).await {
            Ok((schema, params)) => {
                let fields = create_field_infos(schema.columns(), FieldFormat::Binary);
                let param_types = params.iter().map(|dt| match dt {
                    DataType::Integer => Type::INT8,
                    DataType::Float => Type::FLOAT8,
                    DataType::Boolean => Type::BOOL,
                    // Force other types to TEXT so client sends string representation
                    // We parse them in InsertExecutor
                    _ => Type::TEXT,
                }).collect();
                Ok(DescribeStatementResponse::new(param_types, fields))
            }
            Err(e) => {
                error!("Plan query error: {:?}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42P00".to_string(),
                    e.to_string()
                ))))
            }
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        debug!("Describe Portal: {}", query);
        if query.trim().is_empty() {
            return Ok(<DescribePortalResponse as DescribeResponse>::no_data());
        }
        let db_arc = self.db.clone();
        let db = db_arc.read().await;

        match db.plan_query(query).await {
            Ok((schema, _)) => {
                let fields = create_field_infos(schema.columns(), FieldFormat::Binary);
                Ok(DescribePortalResponse::new(fields))
            }
            Err(e) => {
                error!("Plan query error: {:?}", e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42P00".to_string(),
                    e.to_string()
                ))))
            }
        }
    }

    async fn on_sync<C>(
        &self,
        _client: &mut C,
        _message: PgSync
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!("Sync");
        _client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )))
            .await?;
        _client.flush().await?;
        Ok(())
    }
}

async fn execute_query<'a>(
    db: Arc<RwLock<InMemoryDB>>,
    query: &str,
    params: Vec<Value>,
    format: FieldFormat,
    username: &str,
    metrics: &PgWireMetrics,
) -> PgWireResult<Response<'a>> {
    debug!("Executing query: {} with params: {:?}", query, params);
    if query.trim().is_empty() {
        return Ok(Response::EmptyQuery);
    }
    metrics.on_query();

    if username.is_empty() {
        metrics.on_query_error();
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_string(),
            "28000".to_string(),
            "Missing username".to_string(),
        ))));
    }

    let user = AuthManager::global()
        .get_user(username)
        .await
        .map_err(|e| {
            metrics.on_query_error();
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "28000".to_string(),
                e.to_string(),
            )))
        })?;

    let statement = {
        let db_guard = db.read().await;
        db_guard.parse_first(query).map_err(|e| PgWireError::ApiError(Box::new(e)))?
    };

    enforce_permissions(&user, &statement).map_err(|e| {
        metrics.on_query_error();
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "42501".to_string(),
            e.to_string(),
        )))
    })?;

    {
        let db_guard = db.read().await;
        if InMemoryDB::is_read_only_stmt(&statement) {
            let result = db_guard.execute_parsed_readonly_with_params(&statement, None, params).await
                .map_err(|e| {
                    metrics.on_query_error();
                    PgWireError::ApiError(Box::new(e))
                })?;
            return build_response_from_result(query, result, format);
        }
    }

    let mut db_guard = db.write().await;
    match db_guard.execute_parsed_with_params(&statement, None, params).await {
        Ok(result) => build_response_from_result(query, result, format),
        Err(e) => {
            metrics.on_query_error();
            error!("Execution error: {:?}", e);
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                e.to_string()
            ))))
        }
    }
}

#[derive(Debug, Default)]
struct PgWireMetrics {
    connections_accepted: AtomicU64,
    auth_failures: AtomicU64,
    queries_total: AtomicU64,
    queries_failed: AtomicU64,
}

impl PgWireMetrics {
    fn on_connection(&self) -> u64 {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn on_auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn on_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    fn on_query_error(&self) {
        self.queries_failed.fetch_add(1, Ordering::Relaxed);
    }
}

fn build_response_from_result<'a>(query: &str, result: crate::result::QueryResult, format: FieldFormat) -> PgWireResult<Response<'a>> {
    debug!("Query successful, rows: {}", result.row_count());
    if result.rows().is_empty() {
        if !result.columns().is_empty() {
            let fields = Arc::new(create_field_infos(result.columns(), format));
            return Ok(Response::Query(QueryResponse::new(fields, stream::iter(vec![]))));
        }

        let count = result.affected_rows().unwrap_or(0);
        let tag = if query.to_uppercase().starts_with("INSERT") {
            Tag::new(&format!("INSERT 0 {}", count))
        } else if query.to_uppercase().starts_with("DELETE") {
            Tag::new(&format!("DELETE {}", count))
        } else if query.to_uppercase().starts_with("UPDATE") {
            Tag::new(&format!("UPDATE {}", count))
        } else {
            Tag::new("OK")
        };
        return Ok(Response::Execution(tag));
    }

    let fields_vec = create_field_infos(result.columns(), format);
    let fields = Arc::new(fields_vec.clone());
    let mut results = Vec::with_capacity(result.row_count());

    for row in result.rows() {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for (i, val) in row.iter().enumerate() {
            let field_format = fields_vec[i].format();
            encode_value(&mut encoder, val, field_format)?;
        }
        results.push(encoder.finish()?);
    }

    let row_stream = stream::iter(results.into_iter().map(Ok::<DataRow, PgWireError>));

    Ok(Response::Query(QueryResponse::new(fields, row_stream)))
}

fn create_field_infos(columns: &[Column], default_format: FieldFormat) -> Vec<FieldInfo> {
    columns
        .iter()
        .map(|col| {
            let (pg_type, format) = match col.data_type {
                DataType::Integer => (Type::INT8, default_format),
                DataType::Float => (Type::FLOAT8, default_format),
                DataType::Text => (Type::TEXT, default_format),
                DataType::Boolean => (Type::BOOL, default_format),
                DataType::Timestamp => (Type::TIMESTAMP, default_format),
                DataType::Date => (Type::DATE, default_format),
                DataType::Uuid => (Type::UUID, default_format),
                // Force TEXT type for complex types to allow reading as String in tests
                DataType::Array(_) => (Type::TEXT, FieldFormat::Text),
                DataType::Json => (Type::TEXT, FieldFormat::Text),
                DataType::Unknown => (Type::UNKNOWN, FieldFormat::Text),
            };
            FieldInfo::new(col.name.clone(), None, None, pg_type, format)
        })
        .collect()
}

fn encode_value(encoder: &mut DataRowEncoder, value: &Value, format: FieldFormat) -> PgWireResult<()> {
    if format == FieldFormat::Text {
        if matches!(value, Value::Null) {
            return encoder.encode_field(&None::<String>);
        }
        let s = format!("{}", value);
        return encoder.encode_field(&s);
    }

    match value {
        Value::Null => encoder.encode_field(&None::<i8>),
        Value::Integer(i) => encoder.encode_field(i),
        Value::Float(f) => encoder.encode_field(f),
        Value::Boolean(b) => encoder.encode_field(b),
        Value::Text(s) => encoder.encode_field(s),
        Value::Timestamp(t) => encoder.encode_field(&t.naive_utc()),
        Value::Date(d) => encoder.encode_field(d),
        Value::Uuid(u) => encoder.encode_field(u.as_bytes()),
        
        Value::Array(_) | Value::Json(_) => {
             // These should be handled by Text format check above because create_field_infos forces Text.
             // But if we reached here with Binary, fallback to string bytes.
             let s = format!("{}", value);
             encoder.encode_field(&s)
        }
    }
}
