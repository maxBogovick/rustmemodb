/// Supported data types for command payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimePayloadType {
    Null,
    Boolean,
    Integer,
    Float,
    Text,
    Array,
    Object,
}

/// Contract for a single field in a payload schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePayloadFieldContract {
    pub name: String,
    pub payload_type: RuntimePayloadType,
    pub required: bool,
}

/// Schema definition for a command payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeCommandPayloadSchema {
    pub root_type: RuntimePayloadType,
    pub fields: Vec<RuntimePayloadFieldContract>,
    pub allow_extra_fields: bool,
}

impl Default for RuntimeCommandPayloadSchema {
    fn default() -> Self {
        Self {
            root_type: RuntimePayloadType::Object,
            fields: Vec::new(),
            allow_extra_fields: true,
        }
    }
}

impl RuntimeCommandPayloadSchema {
    /// Creates a default object-based schema.
    pub fn object() -> Self {
        Self::default()
    }

    /// Sets the expected root type of the payload.
    pub fn with_root_type(mut self, root_type: RuntimePayloadType) -> Self {
        self.root_type = root_type;
        self
    }

    /// Adds a required field to the schema.
    pub fn require_field(
        mut self,
        name: impl Into<String>,
        payload_type: RuntimePayloadType,
    ) -> Self {
        self.fields.push(RuntimePayloadFieldContract {
            name: name.into(),
            payload_type,
            required: true,
        });
        self
    }

    /// Adds an optional field to the schema.
    pub fn optional_field(
        mut self,
        name: impl Into<String>,
        payload_type: RuntimePayloadType,
    ) -> Self {
        self.fields.push(RuntimePayloadFieldContract {
            name: name.into(),
            payload_type,
            required: false,
        });
        self
    }

    /// Sets whether extra fields are allowed in the payload object.
    pub fn allow_extra_fields(mut self, allow: bool) -> Self {
        self.allow_extra_fields = allow;
        self
    }

    /// Validates a JSON payload against the schema.
    fn validate(&self, payload: &serde_json::Value) -> Result<()> {
        if !payload_matches_type(payload, &self.root_type) {
            return Err(DbError::ExecutionError(format!(
                "Payload root type mismatch: expected {:?}, got {}",
                self.root_type,
                json_type_name(payload)
            )));
        }

        if self.root_type != RuntimePayloadType::Object {
            return Ok(());
        }

        let object = payload
            .as_object()
            .ok_or_else(|| DbError::ExecutionError("Payload must be a JSON object".to_string()))?;

        let mut declared_names = HashSet::new();
        for field in &self.fields {
            declared_names.insert(field.name.as_str());

            match object.get(field.name.as_str()) {
                Some(value) => {
                    if !payload_matches_type(value, &field.payload_type) {
                        return Err(DbError::ExecutionError(format!(
                            "Field '{}' type mismatch: expected {:?}, got {}",
                            field.name,
                            field.payload_type,
                            json_type_name(value)
                        )));
                    }
                }
                None if field.required => {
                    return Err(DbError::ExecutionError(format!(
                        "Missing required field '{}'",
                        field.name
                    )));
                }
                None => {}
            }
        }

        if !self.allow_extra_fields {
            for key in object.keys() {
                if !declared_names.contains(key.as_str()) {
                    return Err(DbError::ExecutionError(format!(
                        "Unexpected field '{}' in payload",
                        key
                    )));
                }
            }
        }

        Ok(())
    }
}
