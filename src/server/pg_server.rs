use std::sync::{Arc};
use tokio::net::TcpListener;
use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
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
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use futures::Sink;
use std::fmt::Debug;

use crate::core::Column;
use crate::{DataType, InMemoryDB, Value};

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
        println!("🚀 Postgres Server listening on {}", addr);

        let factory = Arc::new(HandlerFactory {
            db: self.db.clone(),
        });

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("Accepted new connection from {:?}", addr);
            let factory = factory.clone();

            tokio::spawn(async move {
                if let Err(e) = process_socket(socket, None, factory).await {
                    eprintln!("Connection error: {:?}", e);
                }
            });
        }
    }
}

struct HandlerFactory {
    db: Arc<RwLock<InMemoryDB>>,
}

impl pgwire::api::PgWireHandlerFactory for HandlerFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = QueryProcessor;
    type ExtendedQueryHandler = QueryProcessor;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(QueryProcessor { db: self.db.clone() })
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(QueryProcessor { db: self.db.clone() })
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

struct QueryProcessor {
    db: Arc<RwLock<InMemoryDB>>,
}

#[async_trait]
impl SimpleQueryHandler for QueryProcessor {
    async fn do_query<'a, 'b: 'a, C>(&'b self, _client: &mut C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("Simple Query: {}", query);
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let response = execute_query(self.db.clone(), query).await?;
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
        println!("Extended Query Exec: {}", query);

        if query.trim().is_empty() {
            return Ok(Response::EmptyQuery);
        }

        if portal.parameter_len() > 0 {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "0A000".to_string(),
                "Parameters not supported in RustMemDB MVP".to_string()
            ))));
        }

        execute_query(self.db.clone(), query).await
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
        println!("Describe Statement: {}", query);
        if query.trim().is_empty() {
            return Ok(<DescribeStatementResponse as DescribeResponse>::no_data());
        }
        let db_arc = self.db.clone();
        let db = db_arc.read().await;

        match db.plan_query(query) {
            Ok(schema) => {
                let fields = create_field_infos(schema.columns());
                Ok(DescribeStatementResponse::new(vec![], fields))
            }
            Err(e) => {
                eprintln!("Plan query error: {:?}", e);
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
        println!("Describe Portal: {}", query);
        if query.trim().is_empty() {
            return Ok(<DescribePortalResponse as DescribeResponse>::no_data());
        }
        let db_arc = self.db.clone();
        let db = db_arc.read().await;

        match db.plan_query(query) {
            Ok(schema) => {
                let fields = create_field_infos(schema.columns());
                Ok(DescribePortalResponse::new(fields))
            }
            Err(e) => {
                eprintln!("Plan query error: {:?}", e);
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
        println!("Sync");
        _client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )))
            .await?;
        _client.flush().await?;
        Ok(())
    }
}

async fn execute_query<'a>(db: Arc<RwLock<InMemoryDB>>, query: &str) -> PgWireResult<Response<'a>> {
    let mut db_guard = db.write().await;
    println!("Executing query: {}", query);
    if query.trim().is_empty() {
        return Ok(Response::EmptyQuery);
    }
    match db_guard.execute(query).await {
        Ok(result) => {
            println!("Query successful, rows: {}", result.row_count());
            if result.rows().is_empty() {
                if !result.columns().is_empty() {
                    let fields = Arc::new(create_field_infos(result.columns()));
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

            let fields = Arc::new(create_field_infos(result.columns()));
            let mut results = Vec::with_capacity(result.row_count());

            for row in result.rows() {
                let mut encoder = DataRowEncoder::new(fields.clone());
                for val in row {
                    encode_value(&mut encoder, val)?;
                }
                results.push(encoder.finish()?);
            }

            let row_stream = stream::iter(results.into_iter().map(Ok::<DataRow, PgWireError>));

            Ok(Response::Query(QueryResponse::new(fields, row_stream)))
        }
        Err(e) => {
            eprintln!("Execution error: {:?}", e);
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                e.to_string()
            ))))
        }
    }
}

fn create_field_infos(columns: &[Column]) -> Vec<FieldInfo> {
    columns
        .iter()
        .map(|col| {
            let pg_type = match col.data_type {
                DataType::Integer => Type::INT8,
                DataType::Float => Type::FLOAT8,
                DataType::Text => Type::TEXT,
                DataType::Boolean => Type::BOOL,
                DataType::Timestamp => Type::TIMESTAMP,
                DataType::Date => Type::DATE,
                DataType::Uuid => Type::UUID,
            };
            FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}

fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&None::<i8>),
        Value::Integer(i) => encoder.encode_field(i),
        Value::Float(f) => encoder.encode_field(f),
        Value::Boolean(b) => encoder.encode_field(b),
        Value::Text(s) => encoder.encode_field(s),
        Value::Timestamp(t) => encoder.encode_field(&t.naive_utc()),
        Value::Date(d) => encoder.encode_field(d),
        Value::Uuid(u) => encoder.encode_field(&u.to_string()),
    }
}
