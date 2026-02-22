/// Describes a single field in a dynamic schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicFieldDef {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
}

/// Represents the schema of a persisted object, typically used for runtime reflection or dynamic types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSchema {
    pub table_name: String,
    pub fields: Vec<DynamicFieldDef>,
    pub source_kind: String,
    pub source: String,
}

impl DynamicSchema {
    /// Generates the SQL statement to create the table for this schema.
    ///
    /// Includes standard system columns (`__persist_id`, `__version`, etc.) automatically.
    pub fn create_table_sql(&self) -> String {
        let mut columns = vec![
            "__persist_id TEXT PRIMARY KEY".to_string(),
            "__version INTEGER NOT NULL".to_string(),
            "__schema_version INTEGER NOT NULL".to_string(),
            "__touch_count INTEGER NOT NULL".to_string(),
            "__created_at TIMESTAMP NOT NULL".to_string(),
            "__updated_at TIMESTAMP NOT NULL".to_string(),
            "__last_touch_at TIMESTAMP NOT NULL".to_string(),
        ];

        for field in &self.fields {
            let mut col = format!("{} {}", field.name, field.sql_type);
            if !field.nullable {
                col.push_str(" NOT NULL");
            }
            columns.push(col);
        }

        format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            self.table_name,
            columns.join(", ")
        )
    }

    /// Finds a field definition by name.
    pub fn field(&self, name: &str) -> Option<&DynamicFieldDef> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Checks if a field with the given name exists in the schema.
    pub fn has_field(&self, name: &str) -> bool {
        self.field(name).is_some()
    }

    /// Creates a map of default values for all fields in the schema (initialized to Null).
    pub fn default_value_map(&self) -> BTreeMap<String, Value> {
        let mut values = BTreeMap::new();
        for field in &self.fields {
            values.insert(field.name.clone(), Value::Null);
        }
        values
    }
}
