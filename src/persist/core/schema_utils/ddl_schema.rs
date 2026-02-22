/// Parses a SQL DDL statement to infer a `DynamicSchema`.
///
/// This provides a heuristic parsing of `CREATE TABLE` statements to extract column definitions.
/// It ignores constraints like PRIMARY KEY, FOREIGN KEY, etc., focusing on column names and types.
pub fn dynamic_schema_from_ddl(ddl: &str, table_name: impl Into<String>) -> Result<DynamicSchema> {
    let ddl = ddl.trim().trim_end_matches(';').trim();
    if ddl.is_empty() {
        return Err(DbError::ParseError("DDL is empty".to_string()));
    }

    let open_idx = ddl.find('(').ok_or_else(|| {
        DbError::ParseError("DDL must contain '(' with column declarations".to_string())
    })?;
    let close_idx = ddl.rfind(')').ok_or_else(|| {
        DbError::ParseError("DDL must contain ')' with column declarations".to_string())
    })?;
    if close_idx <= open_idx {
        return Err(DbError::ParseError(
            "DDL has invalid parenthesis order".to_string(),
        ));
    }

    let columns_body = &ddl[open_idx + 1..close_idx];
    let segments = split_top_level_commas(columns_body);
    let mut fields = Vec::new();

    for raw_segment in segments {
        let segment = raw_segment.trim();
        if segment.is_empty() {
            continue;
        }

        let upper = segment.to_ascii_uppercase();
        if upper.starts_with("PRIMARY KEY")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CHECK")
            || upper.starts_with("CONSTRAINT")
        {
            continue;
        }

        let mut parts = segment.split_whitespace();
        let Some(raw_name) = parts.next() else {
            continue;
        };
        let col_name = trim_sql_identifier(raw_name);

        let modifiers = [
            "NOT",
            "NULL",
            "PRIMARY",
            "KEY",
            "UNIQUE",
            "REFERENCES",
            "CHECK",
            "DEFAULT",
            "CONSTRAINT",
        ];

        let mut type_tokens = Vec::new();
        for token in parts {
            let token_upper = token.to_ascii_uppercase();
            if modifiers.contains(&token_upper.as_str()) {
                break;
            }
            type_tokens.push(token);
        }

        if type_tokens.is_empty() {
            return Err(DbError::ParseError(format!(
                "DDL column '{}' has no SQL type",
                col_name
            )));
        }

        let sql_type = type_tokens.join(" ");
        let nullable = !upper.contains("NOT NULL");
        fields.push(DynamicFieldDef {
            name: col_name,
            sql_type,
            nullable,
        });
    }

    if fields.is_empty() {
        return Err(DbError::ParseError(
            "DDL does not contain any parseable columns".to_string(),
        ));
    }

    Ok(DynamicSchema {
        table_name: table_name.into(),
        fields,
        source_kind: "ddl".to_string(),
        source: ddl.to_string(),
    })
}

fn split_top_level_commas(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;

    for ch in input.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                result.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

fn trim_sql_identifier(value: &str) -> String {
    value
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}
