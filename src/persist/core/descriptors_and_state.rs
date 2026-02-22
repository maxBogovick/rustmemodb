/// Describes a function exposed by a persisted object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDescriptor {
    pub name: String,
    pub arg_count: usize,
    pub mutates_state: bool,
}

/// Describes the schema and capabilities of a persisted object type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDescriptor {
    pub type_name: String,
    pub table_name: String,
    pub functions: Vec<FunctionDescriptor>,
}

/// Represents the raw state of a persisted item as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistState {
    pub persist_id: String,
    pub type_name: String,
    pub table_name: String,
    pub metadata: PersistMetadata,
    pub fields: serde_json::Value,
}

impl PersistState {
    /// Returns the fields as a JSON object, or an error if they are not an object.
    pub fn fields_object(&self) -> Result<&serde_json::Map<String, serde_json::Value>> {
        self.fields.as_object().ok_or_else(|| {
            DbError::ExecutionError("Persist state fields must be a JSON object".to_string())
        })
    }

    /// Returns a mutable reference to the fields as a JSON object.
    pub fn fields_object_mut(&mut self) -> Result<&mut serde_json::Map<String, serde_json::Value>> {
        self.fields.as_object_mut().ok_or_else(|| {
            DbError::ExecutionError("Persist state fields must be a JSON object".to_string())
        })
    }

    /// sets a specific field in the JSON fields object.
    pub fn set_json_field(
        &mut self,
        name: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<()> {
        let fields = self.fields_object_mut()?;
        fields.insert(name.into(), value);
        Ok(())
    }
}
