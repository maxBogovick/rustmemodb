use crate::core::{DataType, DbError, Result};
use crate::facade::InMemoryDB;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelProgram {
    pub structs: Vec<StructDecl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructDecl {
    pub name: String,
    pub fields: Vec<FieldDecl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDecl {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub unique: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Scalar(DataType),
    Composition { target: String },
}

#[derive(Debug, Clone)]
struct PrimaryKeyRef {
    column: String,
    data_type: DataType,
}

impl ModelProgram {
    /// Parse a tiny schema DSL into a model program.
    ///
    /// Syntax:
    /// ```text
    /// struct users {
    ///   id: int pk
    ///   name: text not_null
    /// }
    ///
    /// struct posts {
    ///   id: int pk
    ///   author: users not_null
    ///   title: text
    /// }
    /// ```
    pub fn parse(input: &str) -> Result<Self> {
        let mut structs = Vec::new();
        let mut current: Option<StructDecl> = None;

        for (line_idx, raw_line) in input.lines().enumerate() {
            let line_no = line_idx + 1;
            let line = strip_comment(raw_line).trim();

            if line.is_empty() {
                continue;
            }

            if let Some(active) = current.as_mut() {
                if line == "}" {
                    let finished = current.take().ok_or_else(|| {
                        DbError::ParseError("Internal parser error: missing active struct".into())
                    })?;
                    structs.push(finished);
                    continue;
                }

                let field = parse_field(line, line_no)?;
                active.fields.push(field);
                continue;
            }

            current = Some(parse_struct_header(line, line_no)?);
        }

        if let Some(unclosed) = current {
            return Err(DbError::ParseError(format!(
                "Unclosed struct '{}' (missing closing '}}')",
                unclosed.name
            )));
        }

        if structs.is_empty() {
            return Err(DbError::ParseError(
                "Model program is empty; add at least one struct".into(),
            ));
        }

        Ok(Self { structs })
    }

    /// Generate CREATE TABLE statements in dependency-safe order.
    /// Compositions are transformed into FK columns.
    pub fn to_create_table_sql(&self) -> Result<Vec<String>> {
        let model = self.validate_and_index()?;
        let creation_order = model.topological_order()?;

        let mut statements = Vec::with_capacity(creation_order.len());

        for struct_name in creation_order {
            let entity = model.structs_by_name.get(&struct_name).ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Internal model error: '{}' disappeared during generation",
                    struct_name
                ))
            })?;

            let mut columns = Vec::with_capacity(entity.fields.len());
            let mut seen_column_names = HashSet::new();
            for field in &entity.fields {
                let column_name = model.resolved_column_name(field)?;
                if !seen_column_names.insert(column_name.clone()) {
                    return Err(DbError::ExecutionError(format!(
                        "Struct '{}': duplicate resulting column name '{}'",
                        entity.name, column_name
                    )));
                }
                columns.push(model.render_column_sql(entity, field)?);
            }

            statements.push(format!(
                "CREATE TABLE IF NOT EXISTS {} ({})",
                entity.name,
                columns.join(", ")
            ));
        }

        Ok(statements)
    }

    /// Materialize all structs as SQL tables in the provided database.
    pub async fn materialize(&self, db: &mut InMemoryDB) -> Result<Vec<String>> {
        let statements = self.to_create_table_sql()?;
        for statement in &statements {
            db.execute(statement).await?;
        }
        Ok(statements)
    }
}

pub async fn parse_and_materialize_models(
    source: &str,
    db: &mut InMemoryDB,
) -> Result<Vec<String>> {
    ModelProgram::parse(source)?.materialize(db).await
}

fn parse_struct_header(line: &str, line_no: usize) -> Result<StructDecl> {
    let Some(rest) = line.strip_prefix("struct") else {
        return Err(DbError::ParseError(format!(
            "Line {}: expected 'struct <name> {{'",
            line_no
        )));
    };

    if !rest
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_whitespace())
    {
        return Err(DbError::ParseError(format!(
            "Line {}: expected whitespace after 'struct'",
            line_no
        )));
    }

    let rest = rest.trim();
    let Some(name_part) = rest.strip_suffix('{') else {
        return Err(DbError::ParseError(format!(
            "Line {}: struct declaration must end with '{{'",
            line_no
        )));
    };

    let name = name_part.trim();
    if !is_identifier(name) {
        return Err(DbError::ParseError(format!(
            "Line {}: invalid struct name '{}'",
            line_no, name
        )));
    }

    Ok(StructDecl {
        name: name.to_string(),
        fields: Vec::new(),
    })
}

fn parse_field(line: &str, line_no: usize) -> Result<FieldDecl> {
    let line = line.trim_end_matches(',').trim();
    let (name_raw, rhs) = line.split_once(':').ok_or_else(|| {
        DbError::ParseError(format!(
            "Line {}: expected field format '<name>: <type> [modifiers]'",
            line_no
        ))
    })?;

    let name = name_raw.trim();
    if !is_identifier(name) {
        return Err(DbError::ParseError(format!(
            "Line {}: invalid field name '{}'",
            line_no, name
        )));
    }

    let mut tokens = rhs.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(DbError::ParseError(format!(
            "Line {}: field '{}' is missing type",
            line_no, name
        )));
    }

    let type_token = tokens.remove(0);
    let field_type = if let Some(data_type) = parse_scalar_type(type_token) {
        FieldType::Scalar(data_type)
    } else if is_identifier(type_token) {
        FieldType::Composition {
            target: type_token.to_string(),
        }
    } else {
        return Err(DbError::ParseError(format!(
            "Line {}: unknown type '{}'",
            line_no, type_token
        )));
    };

    let mut nullable = true;
    let mut unique = false;
    let mut primary_key = false;

    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i].to_ascii_lowercase();
        match token.as_str() {
            "pk" | "primary_key" => {
                primary_key = true;
            }
            "primary" => {
                if tokens
                    .get(i + 1)
                    .is_some_and(|next| next.eq_ignore_ascii_case("key"))
                {
                    primary_key = true;
                    i += 1;
                } else {
                    return Err(DbError::ParseError(format!(
                        "Line {}: expected 'key' after 'primary'",
                        line_no
                    )));
                }
            }
            "not_null" | "required" => {
                nullable = false;
            }
            "nullable" => {
                nullable = true;
            }
            "unique" => {
                unique = true;
            }
            _ => {
                return Err(DbError::ParseError(format!(
                    "Line {}: unknown field modifier '{}'",
                    line_no, token
                )));
            }
        }
        i += 1;
    }

    if primary_key {
        nullable = false;
        unique = true;
    }

    Ok(FieldDecl {
        name: name.to_string(),
        field_type,
        nullable,
        unique,
        primary_key,
    })
}

fn parse_scalar_type(token: &str) -> Option<DataType> {
    if let Some(base) = token.strip_suffix("[]") {
        return parse_scalar_type(base).map(|inner| DataType::Array(Box::new(inner)));
    }

    match token.to_ascii_lowercase().as_str() {
        "int" | "integer" | "i64" => Some(DataType::Integer),
        "float" | "double" | "f64" => Some(DataType::Float),
        "text" | "string" => Some(DataType::Text),
        "bool" | "boolean" => Some(DataType::Boolean),
        "timestamp" | "datetime" => Some(DataType::Timestamp),
        "date" => Some(DataType::Date),
        "uuid" => Some(DataType::Uuid),
        "json" | "jsonb" => Some(DataType::Json),
        _ => None,
    }
}

fn strip_comment(line: &str) -> &str {
    let line = line.split_once("//").map_or(line, |(head, _)| head);
    line.split_once('#').map_or(line, |(head, _)| head)
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

struct IndexedModel<'a> {
    structs_by_name: HashMap<String, &'a StructDecl>,
    struct_order: HashMap<String, usize>,
    dependencies: HashMap<String, HashSet<String>>,
    primary_keys: HashMap<String, PrimaryKeyRef>,
}

impl ModelProgram {
    fn validate_and_index(&self) -> Result<IndexedModel<'_>> {
        let mut structs_by_name = HashMap::new();
        let mut struct_order = HashMap::new();

        for (idx, entity) in self.structs.iter().enumerate() {
            if entity.fields.is_empty() {
                return Err(DbError::ExecutionError(format!(
                    "Struct '{}' must have at least one field",
                    entity.name
                )));
            }

            if structs_by_name
                .insert(entity.name.clone(), entity)
                .is_some()
            {
                return Err(DbError::ExecutionError(format!(
                    "Struct '{}' is declared more than once",
                    entity.name
                )));
            }
            struct_order.insert(entity.name.clone(), idx);
        }

        let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();
        let mut primary_keys = HashMap::new();

        for entity in &self.structs {
            let mut seen_fields = HashSet::new();
            let mut pk_fields = Vec::new();
            let mut deps = HashSet::new();

            for field in &entity.fields {
                if !seen_fields.insert(field.name.as_str()) {
                    return Err(DbError::ExecutionError(format!(
                        "Struct '{}': duplicate field '{}'",
                        entity.name, field.name
                    )));
                }

                match &field.field_type {
                    FieldType::Scalar(data_type) => {
                        if matches!(data_type, DataType::Unknown) {
                            return Err(DbError::ExecutionError(format!(
                                "Struct '{}': field '{}' cannot use UNKNOWN type",
                                entity.name, field.name
                            )));
                        }
                    }
                    FieldType::Composition { target } => {
                        if field.primary_key {
                            return Err(DbError::ExecutionError(format!(
                                "Struct '{}': composition field '{}' cannot be PRIMARY KEY",
                                entity.name, field.name
                            )));
                        }
                        deps.insert(target.clone());
                    }
                }

                if field.primary_key {
                    pk_fields.push(field);
                }
            }

            if pk_fields.len() != 1 {
                return Err(DbError::ExecutionError(format!(
                    "Struct '{}' must define exactly one PRIMARY KEY field",
                    entity.name
                )));
            }

            let pk = pk_fields[0];
            let pk_type = match &pk.field_type {
                FieldType::Scalar(data_type) => data_type.clone(),
                FieldType::Composition { .. } => {
                    return Err(DbError::ExecutionError(format!(
                        "Struct '{}': primary key must be a scalar field",
                        entity.name
                    )));
                }
            };

            primary_keys.insert(
                entity.name.clone(),
                PrimaryKeyRef {
                    column: pk.name.clone(),
                    data_type: pk_type,
                },
            );
            dependencies.insert(entity.name.clone(), deps);
        }

        for (entity_name, deps) in &dependencies {
            for dep in deps {
                if !structs_by_name.contains_key(dep) {
                    return Err(DbError::ExecutionError(format!(
                        "Struct '{}': composition references unknown struct '{}'",
                        entity_name, dep
                    )));
                }

                if dep == entity_name {
                    return Err(DbError::ExecutionError(format!(
                        "Struct '{}': self-composition is not supported",
                        entity_name
                    )));
                }
            }
        }

        Ok(IndexedModel {
            structs_by_name,
            struct_order,
            dependencies,
            primary_keys,
        })
    }
}

impl IndexedModel<'_> {
    fn resolved_column_name(&self, field: &FieldDecl) -> Result<String> {
        match &field.field_type {
            FieldType::Scalar(_) => Ok(field.name.clone()),
            FieldType::Composition { target } => {
                let pk = self.primary_keys.get(target).ok_or_else(|| {
                    DbError::ExecutionError(format!(
                        "Composition target '{}' does not define primary key",
                        target
                    ))
                })?;
                Ok(compose_fk_column_name(&field.name, &pk.column))
            }
        }
    }

    fn topological_order(&self) -> Result<Vec<String>> {
        let mut indegree = HashMap::new();
        let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();

        for (name, deps) in &self.dependencies {
            indegree.insert(name.clone(), deps.len());
            for dep in deps {
                outgoing.entry(dep.clone()).or_default().push(name.clone());
            }
        }

        let mut ready = BinaryHeap::new();
        for (name, degree) in &indegree {
            if *degree == 0 {
                ready.push(Reverse((self.struct_order[name], name.clone())));
            }
        }

        let mut order = Vec::with_capacity(self.dependencies.len());
        while let Some(Reverse((_, current))) = ready.pop() {
            order.push(current.clone());

            if let Some(dependents) = outgoing.get(&current) {
                for dependent in dependents {
                    let next_degree = indegree.get_mut(dependent).ok_or_else(|| {
                        DbError::ExecutionError(format!(
                            "Internal model error: missing indegree for '{}'",
                            dependent
                        ))
                    })?;
                    *next_degree -= 1;
                    if *next_degree == 0 {
                        ready.push(Reverse((self.struct_order[dependent], dependent.clone())));
                    }
                }
            }
        }

        if order.len() != self.dependencies.len() {
            let mut unresolved = indegree
                .into_iter()
                .filter_map(|(name, degree)| (degree > 0).then_some(name))
                .collect::<Vec<_>>();
            unresolved.sort();
            return Err(DbError::ExecutionError(format!(
                "Composition cycle detected: {}",
                unresolved.join(", ")
            )));
        }

        Ok(order)
    }

    fn render_column_sql(&self, entity: &StructDecl, field: &FieldDecl) -> Result<String> {
        match &field.field_type {
            FieldType::Scalar(data_type) => {
                let mut col = format!("{} {}", field.name, data_type);
                if field.primary_key {
                    col.push_str(" PRIMARY KEY");
                } else {
                    if !field.nullable {
                        col.push_str(" NOT NULL");
                    }
                    if field.unique {
                        col.push_str(" UNIQUE");
                    }
                }
                Ok(col)
            }
            FieldType::Composition { target } => {
                let pk = self.primary_keys.get(target).ok_or_else(|| {
                    DbError::ExecutionError(format!(
                        "Struct '{}': target '{}' has no primary key",
                        entity.name, target
                    ))
                })?;
                let fk_column = compose_fk_column_name(&field.name, &pk.column);

                let mut col = format!("{} {}", fk_column, pk.data_type);
                if !field.nullable {
                    col.push_str(" NOT NULL");
                }
                if field.unique {
                    col.push_str(" UNIQUE");
                }
                col.push_str(&format!(" REFERENCES {}({})", target, pk.column));
                Ok(col)
            }
        }
    }
}

fn compose_fk_column_name(field_name: &str, pk_name: &str) -> String {
    if field_name == pk_name {
        return field_name.to_string();
    }

    let suffix = format!("_{}", pk_name);
    if field_name.ends_with(&suffix) {
        return field_name.to_string();
    }

    format!("{}{}", field_name, suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_models_and_generate_sql() {
        let source = r#"
            struct users {
              id: int pk
              name: text not_null
            }

            struct posts {
              id: int pk
              author: users not_null
              title: text
            }
        "#;

        let program = ModelProgram::parse(source).unwrap();
        let sql = program.to_create_table_sql().unwrap();

        assert_eq!(sql.len(), 2);
        assert!(sql[0].starts_with("CREATE TABLE IF NOT EXISTS users"));
        assert!(sql[1].starts_with("CREATE TABLE IF NOT EXISTS posts"));
        assert!(sql[1].contains("author_id INTEGER NOT NULL REFERENCES users(id)"));
    }

    #[test]
    fn reject_composition_cycles() {
        let source = r#"
            struct a {
              id: int pk
              b_ref: b
            }

            struct b {
              id: int pk
              a_ref: a
            }
        "#;

        let program = ModelProgram::parse(source).unwrap();
        let err = program.to_create_table_sql().unwrap_err();

        match err {
            DbError::ExecutionError(msg) => assert!(msg.contains("Composition cycle detected")),
            _ => panic!("Expected cycle error, got {:?}", err),
        }
    }
}
