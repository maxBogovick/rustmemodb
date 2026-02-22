/// Converts a JSON Schema definition into a `DynamicSchema`.
///
/// Maps JSON types to SQL types:
/// - `string` -> `TEXT` (or `TIMESTAMP`, `DATE`, `UUID` based on format)
/// - `integer` -> `INTEGER`
/// - `number` -> `FLOAT`
/// - `boolean` -> `BOOLEAN`
/// - `object`, `array` -> `JSONB`
pub fn dynamic_schema_from_json_schema(
    json_schema: &str,
    table_name: impl Into<String>,
) -> Result<DynamicSchema> {
    let root: serde_json::Value = serde_json::from_str(json_schema)
        .map_err(|err| DbError::ParseError(format!("Invalid JSON schema: {}", err)))?;

    let obj = root
        .as_object()
        .ok_or_else(|| DbError::ParseError("JSON schema root must be an object".to_string()))?;

    let properties = obj
        .get("properties")
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            DbError::ParseError("JSON schema must contain object 'properties'".to_string())
        })?;

    let required_fields: BTreeSet<String> = obj
        .get("required")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let mut fields = Vec::new();
    for (name, prop) in properties {
        let prop_obj = prop.as_object().ok_or_else(|| {
            DbError::ParseError(format!(
                "Property '{}' in JSON schema must be an object",
                name
            ))
        })?;

        let (sql_type, nullable_from_type) = json_property_to_sql_type(prop_obj)?;
        let nullable = nullable_from_type || !required_fields.contains(name);

        fields.push(DynamicFieldDef {
            name: name.clone(),
            sql_type,
            nullable,
        });
    }

    if fields.is_empty() {
        return Err(DbError::ParseError(
            "JSON schema contains no properties".to_string(),
        ));
    }

    fields.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(DynamicSchema {
        table_name: table_name.into(),
        fields,
        source_kind: "json_schema".to_string(),
        source: json_schema.to_string(),
    })
}

fn json_property_to_sql_type(
    prop: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, bool)> {
    let mut nullable = false;

    let json_type_value = prop
        .get("type")
        .ok_or_else(|| DbError::ParseError("JSON schema property missing 'type'".to_string()))?;

    let json_type = if let Some(type_name) = json_type_value.as_str() {
        type_name.to_string()
    } else if let Some(type_array) = json_type_value.as_array() {
        let mut chosen = None;
        for item in type_array {
            if let Some(type_name) = item.as_str() {
                if type_name == "null" {
                    nullable = true;
                } else if chosen.is_none() {
                    chosen = Some(type_name.to_string());
                }
            }
        }
        chosen.ok_or_else(|| {
            DbError::ParseError("JSON schema type array must contain non-null type".to_string())
        })?
    } else {
        return Err(DbError::ParseError(
            "JSON schema 'type' must be string or array".to_string(),
        ));
    };

    if json_type == "string" {
        if let Some(format) = prop.get("format").and_then(|v| v.as_str()) {
            match format {
                "date-time" => return Ok(("TIMESTAMP".to_string(), nullable)),
                "date" => return Ok(("DATE".to_string(), nullable)),
                "uuid" => return Ok(("UUID".to_string(), nullable)),
                _ => {}
            }
        }
    }

    let sql_type = match json_type.as_str() {
        "string" => "TEXT".to_string(),
        "integer" => "INTEGER".to_string(),
        "number" => "FLOAT".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        "object" | "array" => "JSONB".to_string(),
        other => {
            return Err(DbError::ParseError(format!(
                "Unsupported JSON schema type '{}'",
                other
            )));
        }
    };

    Ok((sql_type, nullable))
}
