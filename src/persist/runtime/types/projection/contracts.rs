/// Configuration for a single field in a projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProjectionField {
    /// The field name in the entity's state.
    pub state_field: String,
    /// The column name in the projection table.
    pub column_name: String,
    /// The data type of the payload.
    pub payload_type: RuntimePayloadType,
    /// Whether this field should be indexed for lookup.
    pub indexed: bool,
}

impl RuntimeProjectionField {
    /// Creates a new projection field definition.
    pub fn new(
        state_field: impl Into<String>,
        column_name: impl Into<String>,
        payload_type: RuntimePayloadType,
    ) -> Self {
        Self {
            state_field: state_field.into(),
            column_name: column_name.into(),
            payload_type,
            indexed: false,
        }
    }

    /// Enables indexing for this field.
    pub fn indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }
}

/// A contract defining a projection table associated with an entity type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProjectionContract {
    /// The entity type this projection applies to.
    pub entity_type: String,
    /// The name of the projection table.
    pub table_name: String,
    /// The schema version of the projection.
    pub schema_version: u32,
    /// The list of fields to project.
    pub fields: Vec<RuntimeProjectionField>,
}

impl RuntimeProjectionContract {
    /// Creates a new projection contract.
    pub fn new(entity_type: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            table_name: table_name.into(),
            schema_version: 1,
            fields: Vec::new(),
        }
    }

    /// Sets the schema version for the projection.
    pub fn with_schema_version(mut self, schema_version: u32) -> Self {
        self.schema_version = schema_version.max(1);
        self
    }

    /// Adds a field to the projection.
    pub fn with_field(mut self, field: RuntimeProjectionField) -> Self {
        self.fields.push(field);
        self
    }

    /// Validates the projection contract configuration.
    pub fn validate(&self) -> Result<()> {
        if self.entity_type.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Projection contract entity_type must not be empty".to_string(),
            ));
        }

        if self.table_name.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Projection contract table_name must not be empty".to_string(),
            ));
        }

        if self.fields.is_empty() {
            return Err(DbError::ExecutionError(format!(
                "Projection contract '{}' must declare at least one field",
                self.entity_type
            )));
        }

        let mut state_fields = HashSet::<String>::new();
        let mut column_names = HashSet::<String>::new();
        for field in &self.fields {
            if field.state_field.trim().is_empty() {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has empty state_field",
                    self.entity_type
                )));
            }
            if field.column_name.trim().is_empty() {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has empty column_name",
                    self.entity_type
                )));
            }

            if !state_fields.insert(field.state_field.clone()) {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has duplicate state_field '{}'",
                    self.entity_type, field.state_field
                )));
            }
            if !column_names.insert(field.column_name.clone()) {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has duplicate column_name '{}'",
                    self.entity_type, field.column_name
                )));
            }
        }

        Ok(())
    }
}

/// A single row in a projection table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeProjectionRow {
    /// The ID of the entity this row belongs to.
    pub entity_id: String,
    /// The projected values (columns).
    pub values: serde_json::Map<String, serde_json::Value>,
    /// When this row was last updated.
    pub updated_at: DateTime<Utc>,
}
