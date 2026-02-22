#[derive(Clone)]
struct PersistSchemaRestState {
    session: PersistSession,
    schemas_dir: Arc<PathBuf>,
    collections: Arc<tokio::sync::RwLock<std::collections::HashMap<String, PersistSchemaCollection>>>,
    reload_lock: Arc<Mutex<()>>,
    last_seen_schema_signature: Arc<Mutex<Option<String>>>,
    last_reload_check_at: Arc<Mutex<std::time::Instant>>,
    reload_check_interval: Duration,
}

#[derive(Clone)]
struct PersistSchemaCollection {
    table_name: String,
    schema: crate::persist::DynamicSchema,
}

impl PersistApp {
    /// Builds a zero-handler REST router from `*.json` JSON Schema files in a directory.
    ///
    /// Generated routes:
    /// - `GET /:collection`
    /// - `POST /:collection`
    /// - `GET /:collection/:id`
    /// - `PATCH /:collection/:id`
    /// - `DELETE /:collection/:id`
    /// - `GET /_openapi.json`
    ///
    /// This API is intentionally high-level: application code provides schemas only,
    /// while persist owns table creation, payload validation, CRUD SQL, serialization,
    /// hot schema reload and OpenAPI description generation.
    pub async fn serve_json_schema_dir(&self, schemas_dir: impl AsRef<Path>) -> Result<axum::Router> {
        let schemas_dir = schemas_dir.as_ref();
        let collections = load_schema_collections(&self.session, schemas_dir).await?;
        let signature = compute_schema_signature(schemas_dir).await?;

        let state = PersistSchemaRestState {
            session: self.session.clone(),
            schemas_dir: Arc::new(schemas_dir.to_path_buf()),
            collections: Arc::new(tokio::sync::RwLock::new(collections)),
            reload_lock: Arc::new(Mutex::new(())),
            last_seen_schema_signature: Arc::new(Mutex::new(Some(signature))),
            last_reload_check_at: Arc::new(Mutex::new(std::time::Instant::now())),
            reload_check_interval: Duration::from_millis(250),
        };

        Ok(axum::Router::new()
            .route("/_openapi.json", axum::routing::get(schema_openapi))
            .route("/:collection", axum::routing::get(schema_list).post(schema_create))
            .route(
                "/:collection/:id",
                axum::routing::get(schema_get)
                    .patch(schema_patch)
                    .delete(schema_delete),
            )
            .with_state(state))
    }
}

async fn schema_openapi(
    axum::extract::State(state): axum::extract::State<PersistSchemaRestState>,
) -> crate::web::Result<axum::Json<serde_json::Value>> {
    ensure_schema_state_fresh(&state).await?;
    let collections = state.collections.read().await;
    Ok(axum::Json(build_openapi_document(&collections)))
}

async fn schema_list(
    axum::extract::State(state): axum::extract::State<PersistSchemaRestState>,
    axum::extract::Path(collection): axum::extract::Path<String>,
) -> crate::web::Result<axum::Json<serde_json::Value>> {
    let meta = collection_meta(&state, &collection).await?;
    let projection = select_projection(&meta.schema);
    let sql = format!(
        "SELECT {} FROM {} ORDER BY __updated_at DESC",
        projection, meta.table_name
    );
    let query = state.session.query(&sql).await?;
    let docs = query
        .rows()
        .iter()
        .map(|row| row_to_document_json(row, &meta.schema))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(axum::Json(serde_json::Value::Array(docs)))
}

async fn schema_get(
    axum::extract::State(state): axum::extract::State<PersistSchemaRestState>,
    axum::extract::Path((collection, id)): axum::extract::Path<(String, String)>,
) -> crate::web::Result<axum::Json<serde_json::Value>> {
    let meta = collection_meta(&state, &collection).await?;
    let query = select_by_id(&state.session, &meta, &id).await?;
    let Some(row) = query.rows().first() else {
        return Err(crate::web::WebError::NotFound(format!(
            "document '{}' not found in '{}'",
            id, collection
        )));
    };
    Ok(axum::Json(row_to_document_json(row, &meta.schema)?))
}

async fn schema_create(
    axum::extract::State(state): axum::extract::State<PersistSchemaRestState>,
    axum::extract::Path(collection): axum::extract::Path<String>,
    axum::extract::Json(payload): axum::extract::Json<serde_json::Value>,
) -> crate::web::Result<axum::response::Response> {
    let meta = collection_meta(&state, &collection).await?;
    let object = payload.as_object().ok_or_else(|| {
        crate::web::WebError::Input("request body must be a JSON object".to_string())
    })?;

    ensure_no_unknown_fields(object, &meta.schema)?;
    let row_values = build_create_row_values(object, &meta.schema)?;

    let persist_id = crate::persist::new_persist_id();
    let now = Utc::now().to_rfc3339();

    let mut columns = vec![
        "__persist_id".to_string(),
        "__version".to_string(),
        "__schema_version".to_string(),
        "__touch_count".to_string(),
        "__created_at".to_string(),
        "__updated_at".to_string(),
        "__last_touch_at".to_string(),
    ];
    let mut values = vec![
        format!("'{}'", crate::persist::sql_escape_string(&persist_id)),
        "1".to_string(),
        "1".to_string(),
        "0".to_string(),
        format!("'{}'", crate::persist::sql_escape_string(&now)),
        format!("'{}'", crate::persist::sql_escape_string(&now)),
        format!("'{}'", crate::persist::sql_escape_string(&now)),
    ];

    for field in &meta.schema.fields {
        columns.push(field.name.clone());
        let value = row_values.get(&field.name).ok_or_else(|| {
            crate::web::WebError::Internal(format!(
                "missing prepared value for field '{}'",
                field.name
            ))
        })?;
        values.push(crate::persist::value_to_sql_literal(value));
    }

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        meta.table_name,
        columns.join(", "),
        values.join(", ")
    );
    state.session.execute(&sql).await?;

    let query = select_by_id(&state.session, &meta, &persist_id).await?;
    let row = query.rows().first().ok_or_else(|| {
        crate::web::WebError::Internal(format!(
            "inserted document '{}' was not found in '{}'",
            persist_id, collection
        ))
    })?;
    let doc = row_to_document_json(row, &meta.schema)?;

    Ok(axum::response::IntoResponse::into_response((
        axum::http::StatusCode::CREATED,
        axum::Json(doc),
    )))
}

async fn schema_patch(
    axum::extract::State(state): axum::extract::State<PersistSchemaRestState>,
    axum::extract::Path((collection, id)): axum::extract::Path<(String, String)>,
    axum::extract::Json(payload): axum::extract::Json<serde_json::Value>,
) -> crate::web::Result<axum::Json<serde_json::Value>> {
    let meta = collection_meta(&state, &collection).await?;
    let patch = payload.as_object().ok_or_else(|| {
        crate::web::WebError::Input("request body must be a JSON object".to_string())
    })?;
    if patch.is_empty() {
        return Err(crate::web::WebError::Input(
            "patch payload must contain at least one field".to_string(),
        ));
    }

    ensure_no_unknown_fields(patch, &meta.schema)?;

    let version_query = state
        .session
        .query(&format!(
            "SELECT __version, __touch_count FROM {} WHERE __persist_id = '{}'",
            meta.table_name,
            crate::persist::sql_escape_string(&id)
        ))
        .await?;

    let Some(version_row) = version_query.rows().first() else {
        return Err(crate::web::WebError::NotFound(format!(
            "document '{}' not found in '{}'",
            id, collection
        )));
    };

    if version_row.len() < 2 {
        return Err(crate::web::WebError::Internal(format!(
            "version row for '{}' in '{}' is malformed",
            id, collection
        )));
    }

    let current_version = match version_row.first() {
        Some(crate::core::Value::Integer(v)) => *v,
        _ => {
            return Err(crate::web::WebError::Internal(format!(
                "invalid __version for '{}' in '{}'",
                id, collection
            )));
        }
    };
    let current_touch_count = match version_row.get(1) {
        Some(crate::core::Value::Integer(v)) => *v,
        _ => {
            return Err(crate::web::WebError::Internal(format!(
                "invalid __touch_count for '{}' in '{}'",
                id, collection
            )));
        }
    };

    let mut updates = Vec::<String>::new();
    for (field_name, raw_value) in patch {
        let field = meta
            .schema
            .field(field_name)
            .ok_or_else(|| crate::web::WebError::Input(format!("unknown field '{}'", field_name)))?;
        let parsed = parse_json_for_field(raw_value, field)?;
        updates.push(format!(
            "{} = {}",
            field.name,
            crate::persist::value_to_sql_literal(&parsed)
        ));
    }

    let now = Utc::now().to_rfc3339();
    updates.push(format!("__version = {}", current_version + 1));
    updates.push(format!("__touch_count = {}", current_touch_count + 1));
    updates.push(format!(
        "__updated_at = '{}'",
        crate::persist::sql_escape_string(&now)
    ));
    updates.push(format!(
        "__last_touch_at = '{}'",
        crate::persist::sql_escape_string(&now)
    ));

    let sql = format!(
        "UPDATE {} SET {} WHERE __persist_id = '{}'",
        meta.table_name,
        updates.join(", "),
        crate::persist::sql_escape_string(&id)
    );
    state.session.execute(&sql).await?;

    let query = select_by_id(&state.session, &meta, &id).await?;
    let row = query.rows().first().ok_or_else(|| {
        crate::web::WebError::Internal(format!(
            "patched document '{}' disappeared from '{}'",
            id, collection
        ))
    })?;
    Ok(axum::Json(row_to_document_json(row, &meta.schema)?))
}

async fn schema_delete(
    axum::extract::State(state): axum::extract::State<PersistSchemaRestState>,
    axum::extract::Path((collection, id)): axum::extract::Path<(String, String)>,
) -> crate::web::Result<axum::response::Response> {
    let meta = collection_meta(&state, &collection).await?;
    let sql = format!(
        "DELETE FROM {} WHERE __persist_id = '{}'",
        meta.table_name,
        crate::persist::sql_escape_string(&id)
    );
    let result = state.session.execute(&sql).await?;
    if result.affected_rows().unwrap_or(0) == 0 {
        return Err(crate::web::WebError::NotFound(format!(
            "document '{}' not found in '{}'",
            id, collection
        )));
    }
    Ok(axum::response::IntoResponse::into_response(
        axum::http::StatusCode::NO_CONTENT,
    ))
}

async fn collection_meta(
    state: &PersistSchemaRestState,
    collection: &str,
) -> crate::web::Result<PersistSchemaCollection> {
    ensure_schema_state_fresh(state).await?;
    let collections = state.collections.read().await;
    collections.get(collection).cloned().ok_or_else(|| {
        crate::web::WebError::NotFound(format!("collection '{}' is not registered", collection))
    })
}

async fn select_by_id(
    session: &PersistSession,
    meta: &PersistSchemaCollection,
    persist_id: &str,
) -> Result<crate::result::QueryResult> {
    let projection = select_projection(&meta.schema);
    let sql = format!(
        "SELECT {} FROM {} WHERE __persist_id = '{}'",
        projection, meta.table_name,
        crate::persist::sql_escape_string(persist_id)
    );
    session.query(&sql).await
}

async fn ensure_schema_state_fresh(state: &PersistSchemaRestState) -> Result<()> {
    {
        let mut last_check = state.last_reload_check_at.lock().await;
        if last_check.elapsed() < state.reload_check_interval {
            return Ok(());
        }
        *last_check = std::time::Instant::now();
    }

    let signature = compute_schema_signature(&state.schemas_dir).await?;
    {
        let seen = state.last_seen_schema_signature.lock().await;
        if seen.as_deref() == Some(signature.as_str()) {
            return Ok(());
        }
    }

    let _reload_guard = state.reload_lock.lock().await;

    let signature = compute_schema_signature(&state.schemas_dir).await?;
    {
        let seen = state.last_seen_schema_signature.lock().await;
        if seen.as_deref() == Some(signature.as_str()) {
            return Ok(());
        }
    }

    let collections = load_schema_collections(&state.session, &state.schemas_dir).await?;
    {
        let mut writable = state.collections.write().await;
        *writable = collections;
    }
    {
        let mut seen = state.last_seen_schema_signature.lock().await;
        *seen = Some(signature);
    }

    Ok(())
}

async fn load_schema_collections(
    session: &PersistSession,
    schemas_dir: &Path,
) -> Result<std::collections::HashMap<String, PersistSchemaCollection>> {
    let mut entries = fs::read_dir(schemas_dir).await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to read schemas directory '{}': {}",
            schemas_dir.display(),
            err
        ))
    })?;

    let mut collections = std::collections::HashMap::<String, PersistSchemaCollection>::new();

    while let Some(entry) = entries.next_entry().await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to iterate schemas directory '{}': {}",
            schemas_dir.display(),
            err
        ))
    })? {
        let file_type = entry.file_type().await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to inspect schema file type '{}': {}",
                entry.path().display(),
                err
            ))
        })?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let stem = path.file_stem().and_then(|v| v.to_str()).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Schema filename '{}' has invalid UTF-8",
                path.display()
            ))
        })?;

        let collection_name = normalize_collection_name(stem);
        if collection_name.is_empty() {
            return Err(DbError::ExecutionError(format!(
                "Schema filename '{}' produced empty collection name",
                path.display()
            )));
        }

        if collections.contains_key(&collection_name) {
            return Err(DbError::ExecutionError(format!(
                "Duplicate collection name '{}' derived from schemas in '{}'",
                collection_name,
                schemas_dir.display()
            )));
        }

        let schema_source = fs::read_to_string(&path).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to read schema file '{}': {}",
                path.display(),
                err
            ))
        })?;

        let table_name = format!("schema_{}", collection_name);
        let known_schema_version = session.get_table_schema_version(&table_name).await?;
        let schema =
            crate::persist::dynamic_schema_from_json_schema(&schema_source, table_name.clone())?;
        validate_dynamic_schema_identifiers(&schema, &path)?;
        session.execute(&schema.create_table_sql()).await?;
        let added_columns = ensure_table_matches_schema(session, &schema).await?;
        let mut next_schema_version = known_schema_version.unwrap_or(1).max(1);
        if known_schema_version.is_some() && added_columns {
            next_schema_version = next_schema_version.saturating_add(1);
        }
        session
            .set_table_schema_version(&table_name, next_schema_version)
            .await?;

        collections.insert(collection_name, PersistSchemaCollection { table_name, schema });
    }

    if collections.is_empty() {
        return Err(DbError::ExecutionError(format!(
            "No '*.json' schema files found in '{}'",
            schemas_dir.display()
        )));
    }

    Ok(collections)
}

async fn compute_schema_signature(schemas_dir: &Path) -> Result<String> {
    let mut entries = fs::read_dir(schemas_dir).await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to read schemas directory '{}': {}",
            schemas_dir.display(),
            err
        ))
    })?;

    let mut parts = Vec::<String>::new();

    while let Some(entry) = entries.next_entry().await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to iterate schemas directory '{}': {}",
            schemas_dir.display(),
            err
        ))
    })? {
        let file_type = entry.file_type().await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to inspect schema file type '{}': {}",
                entry.path().display(),
                err
            ))
        })?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let file_name = path
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Schema filename '{}' has invalid UTF-8",
                    path.display()
                ))
            })?
            .to_string();

        let contents = fs::read_to_string(&path).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to read schema file '{}': {}",
                path.display(),
                err
            ))
        })?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&contents, &mut hasher);
        let hash = std::hash::Hasher::finish(&hasher);
        parts.push(format!("{file_name}:{hash}:{}", contents.len()));
    }

    if parts.is_empty() {
        return Err(DbError::ExecutionError(format!(
            "No '*.json' schema files found in '{}'",
            schemas_dir.display()
        )));
    }

    parts.sort();
    Ok(parts.join("|"))
}

fn dynamic_field_projection(schema: &crate::persist::DynamicSchema) -> String {
    schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn select_projection(schema: &crate::persist::DynamicSchema) -> String {
    let dynamic_projection = dynamic_field_projection(schema);
    if dynamic_projection.is_empty() {
        "__persist_id, __version".to_string()
    } else {
        format!("__persist_id, __version, {}", dynamic_projection)
    }
}

async fn ensure_table_matches_schema(
    session: &PersistSession,
    schema: &crate::persist::DynamicSchema,
) -> Result<bool> {
    let mut added_any_column = false;
    for field in &schema.fields {
        let not_null = if field.nullable { "" } else { " NOT NULL" };
        let default_sql = if field.nullable {
            String::new()
        } else {
            format!(" DEFAULT {}", default_sql_for_type(&field.sql_type))
        };
        let alter_sql = format!(
            "ALTER TABLE {} ADD COLUMN {} {}{}{}",
            schema.table_name, field.name, field.sql_type, not_null, default_sql
        );
        match session.execute(&alter_sql).await {
            Ok(_) => {
                added_any_column = true;
            }
            Err(DbError::ExecutionError(message)) if message.contains("already exists") => {}
            Err(err) => return Err(err),
        }
    }

    Ok(added_any_column)
}

fn default_sql_for_type(sql_type: &str) -> &'static str {
    let upper = sql_type.to_ascii_uppercase();
    let base = upper
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default();
    match base {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => "0",
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => "0",
        "BOOL" | "BOOLEAN" => "false",
        "JSON" | "JSONB" => "'{}'",
        _ => "''",
    }
}

fn row_to_document_json(
    row: &[crate::core::Value],
    schema: &crate::persist::DynamicSchema,
) -> crate::web::Result<serde_json::Value> {
    let expected_len = schema.fields.len() + 2;
    if row.len() != expected_len {
        return Err(crate::web::WebError::Internal(format!(
            "malformed row: expected {} columns, got {}",
            expected_len,
            row.len()
        )));
    }

    let id = match row.first() {
        Some(crate::core::Value::Text(v)) => v.clone(),
        Some(other) => other.to_string(),
        None => {
            return Err(crate::web::WebError::Internal(
                "malformed row: missing id".to_string(),
            ));
        }
    };
    let version = match row.get(1) {
        Some(crate::core::Value::Integer(v)) => *v,
        _ => 0,
    };

    let mut object = serde_json::Map::new();
    object.insert("id".to_string(), serde_json::Value::String(id));
    object.insert(
        "version".to_string(),
        serde_json::Value::Number(serde_json::Number::from(version)),
    );

    for (index, field) in schema.fields.iter().enumerate() {
        let value = row.get(index + 2).ok_or_else(|| {
            crate::web::WebError::Internal("malformed row field index".to_string())
        })?;
        object.insert(field.name.clone(), persist_value_to_json(value));
    }

    Ok(serde_json::Value::Object(object))
}

fn persist_value_to_json(value: &crate::core::Value) -> serde_json::Value {
    match value {
        crate::core::Value::Null => serde_json::Value::Null,
        crate::core::Value::Integer(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        crate::core::Value::Float(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        crate::core::Value::Text(v) => serde_json::Value::String(v.clone()),
        crate::core::Value::Boolean(v) => serde_json::Value::Bool(*v),
        crate::core::Value::Timestamp(v) => serde_json::Value::String(v.to_rfc3339()),
        crate::core::Value::Date(v) => serde_json::Value::String(v.to_string()),
        crate::core::Value::Uuid(v) => serde_json::Value::String(v.to_string()),
        crate::core::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(persist_value_to_json).collect())
        }
        crate::core::Value::Json(v) => v.clone(),
    }
}

fn ensure_no_unknown_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    schema: &crate::persist::DynamicSchema,
) -> crate::web::Result<()> {
    for name in object.keys() {
        if !schema.has_field(name) {
            return Err(crate::web::WebError::Input(format!(
                "unknown field '{}'",
                name
            )));
        }
    }
    Ok(())
}

fn build_create_row_values(
    object: &serde_json::Map<String, serde_json::Value>,
    schema: &crate::persist::DynamicSchema,
) -> crate::web::Result<std::collections::HashMap<String, crate::core::Value>> {
    let mut row_values = std::collections::HashMap::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let parsed = match object.get(&field.name) {
            Some(raw) => parse_json_for_field(raw, field)?,
            None if field.nullable => crate::core::Value::Null,
            None => {
                return Err(crate::web::WebError::Input(format!(
                    "missing required field '{}'",
                    field.name
                )));
            }
        };
        row_values.insert(field.name.clone(), parsed);
    }
    Ok(row_values)
}

fn parse_json_for_field(
    value: &serde_json::Value,
    field: &crate::persist::DynamicFieldDef,
) -> crate::web::Result<crate::core::Value> {
    if value.is_null() {
        if !field.nullable {
            return Err(crate::web::WebError::Input(format!(
                "field '{}' cannot be null",
                field.name
            )));
        }
        return Ok(crate::core::Value::Null);
    }

    let base_type = field
        .sql_type
        .to_ascii_uppercase()
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default()
        .to_string();

    let mapped = match base_type.as_str() {
        "TEXT" | "STRING" | "CHAR" | "VARCHAR" => value
            .as_str()
            .map(|v| crate::core::Value::Text(v.to_string()))
            .ok_or_else(|| {
                crate::web::WebError::Input(format!(
                    "field '{}' expects string value",
                    field.name
                ))
            })?,
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => value
            .as_i64()
            .map(crate::core::Value::Integer)
            .ok_or_else(|| {
                crate::web::WebError::Input(format!(
                    "field '{}' expects integer value",
                    field.name
                ))
            })?,
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => value
            .as_f64()
            .map(crate::core::Value::Float)
            .ok_or_else(|| {
                crate::web::WebError::Input(format!(
                    "field '{}' expects number value",
                    field.name
                ))
            })?,
        "BOOL" | "BOOLEAN" => value
            .as_bool()
            .map(crate::core::Value::Boolean)
            .ok_or_else(|| {
                crate::web::WebError::Input(format!(
                    "field '{}' expects boolean value",
                    field.name
                ))
            })?,
        "DATE" | "TIMESTAMP" | "DATETIME" | "UUID" => value
            .as_str()
            .map(|v| crate::core::Value::Text(v.to_string()))
            .ok_or_else(|| {
                crate::web::WebError::Input(format!(
                    "field '{}' expects string value",
                    field.name
                ))
            })?,
        "JSON" | "JSONB" => crate::core::Value::Json(value.clone()),
        _ => crate::core::Value::Json(value.clone()),
    };

    if !crate::persist::value_matches_sql_type(&mapped, &field.sql_type) {
        return Err(crate::web::WebError::Input(format!(
            "field '{}' expects sql type '{}'",
            field.name, field.sql_type
        )));
    }
    Ok(mapped)
}

fn build_openapi_document(
    collections: &std::collections::HashMap<String, PersistSchemaCollection>,
) -> serde_json::Value {
    let mut collection_names = collections.keys().cloned().collect::<Vec<_>>();
    collection_names.sort();

    let mut paths = serde_json::Map::new();
    let mut schemas = serde_json::Map::new();

    for collection in collection_names {
        let Some(meta) = collections.get(&collection) else {
            continue;
        };

        let model_name = to_pascal_case(&collection);
        let doc_name = format!("{}Document", model_name);
        let create_name = format!("{}CreateRequest", model_name);
        let patch_name = format!("{}PatchRequest", model_name);

        schemas.insert(doc_name.clone(), openapi_document_schema(&meta.schema));
        schemas.insert(create_name.clone(), openapi_create_schema(&meta.schema));
        schemas.insert(patch_name.clone(), openapi_patch_schema(&meta.schema));

        paths.insert(
            format!("/{}", collection),
            serde_json::json!({
                "get": {
                    "operationId": format!("list_{}", collection),
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": format!("#/components/schemas/{}", doc_name) }
                                    }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "operationId": format!("create_{}", collection),
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": format!("#/components/schemas/{}", create_name) }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": format!("#/components/schemas/{}", doc_name) }
                                }
                            }
                        }
                    }
                }
            }),
        );

        paths.insert(
            format!("/{}/{{id}}", collection),
            serde_json::json!({
                "get": {
                    "operationId": format!("get_{}", collection),
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": format!("#/components/schemas/{}", doc_name) }
                                }
                            }
                        },
                        "404": { "description": "Not found" }
                    }
                },
                "patch": {
                    "operationId": format!("patch_{}", collection),
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": format!("#/components/schemas/{}", patch_name) }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": format!("#/components/schemas/{}", doc_name) }
                                }
                            }
                        },
                        "404": { "description": "Not found" }
                    }
                },
                "delete": {
                    "operationId": format!("delete_{}", collection),
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "204": { "description": "Deleted" },
                        "404": { "description": "Not found" }
                    }
                }
            }),
        );
    }

    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "RustMemDB Schema REST",
            "version": "1.0.0"
        },
        "paths": serde_json::Value::Object(paths),
        "components": {
            "schemas": serde_json::Value::Object(schemas)
        }
    })
}

fn openapi_document_schema(schema: &crate::persist::DynamicSchema) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    properties.insert(
        "id".to_string(),
        serde_json::json!({ "type": "string" }),
    );
    properties.insert(
        "version".to_string(),
        serde_json::json!({ "type": "integer", "format": "int64" }),
    );

    let mut required = vec!["id".to_string(), "version".to_string()];
    for field in &schema.fields {
        properties.insert(field.name.clone(), openapi_schema_for_field(field));
        if !field.nullable {
            required.push(field.name.clone());
        }
    }

    serde_json::json!({
        "type": "object",
        "properties": serde_json::Value::Object(properties),
        "required": required,
        "additionalProperties": false
    })
}

fn openapi_create_schema(schema: &crate::persist::DynamicSchema) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for field in &schema.fields {
        properties.insert(field.name.clone(), openapi_schema_for_field(field));
        if !field.nullable {
            required.push(field.name.clone());
        }
    }

    serde_json::json!({
        "type": "object",
        "properties": serde_json::Value::Object(properties),
        "required": required,
        "additionalProperties": false
    })
}

fn openapi_patch_schema(schema: &crate::persist::DynamicSchema) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    for field in &schema.fields {
        properties.insert(field.name.clone(), openapi_schema_for_field(field));
    }

    serde_json::json!({
        "type": "object",
        "properties": serde_json::Value::Object(properties),
        "additionalProperties": false
    })
}

fn openapi_schema_for_field(field: &crate::persist::DynamicFieldDef) -> serde_json::Value {
    let sql_type_upper = field.sql_type.to_ascii_uppercase();
    let base = sql_type_upper
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default();

    let mut schema = match base {
        "TEXT" | "STRING" | "CHAR" | "VARCHAR" => serde_json::json!({ "type": "string" }),
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => {
            serde_json::json!({ "type": "integer", "format": "int64" })
        }
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => {
            serde_json::json!({ "type": "number" })
        }
        "BOOL" | "BOOLEAN" => serde_json::json!({ "type": "boolean" }),
        "DATE" => serde_json::json!({ "type": "string", "format": "date" }),
        "TIMESTAMP" | "DATETIME" => {
            serde_json::json!({ "type": "string", "format": "date-time" })
        }
        "UUID" => serde_json::json!({ "type": "string", "format": "uuid" }),
        "JSON" | "JSONB" => serde_json::json!({
            "type": "object",
            "additionalProperties": true
        }),
        _ => serde_json::json!({ "type": "string" }),
    };

    if field.nullable {
        schema = serde_json::json!({
            "anyOf": [
                schema,
                { "type": "null" }
            ]
        });
    }

    schema
}

fn to_pascal_case(name: &str) -> String {
    let mut result = String::new();
    for part in name.split('_').filter(|p| !p.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            result.push(first.to_ascii_uppercase());
            for ch in chars {
                result.push(ch.to_ascii_lowercase());
            }
        }
    }

    if result.is_empty() {
        "Collection".to_string()
    } else {
        result
    }
}

fn validate_dynamic_schema_identifiers(
    schema: &crate::persist::DynamicSchema,
    source_path: &Path,
) -> Result<()> {
    if !is_safe_sql_identifier(&schema.table_name) {
        return Err(DbError::ExecutionError(format!(
            "Schema '{}' produced unsafe table name '{}'",
            source_path.display(),
            schema.table_name
        )));
    }

    for field in &schema.fields {
        if !is_safe_sql_identifier(&field.name) {
            return Err(DbError::ExecutionError(format!(
                "Schema '{}' contains unsupported field name '{}' (allowed: [A-Za-z_][A-Za-z0-9_]*)",
                source_path.display(),
                field.name
            )));
        }
    }
    Ok(())
}

fn is_safe_sql_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn normalize_collection_name(raw: &str) -> String {
    let mut normalized = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();

    normalized = normalized.trim_matches('_').to_string();
    if normalized.is_empty() {
        return normalized;
    }

    if normalized
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        normalized.insert(0, '_');
    }

    normalized
}
