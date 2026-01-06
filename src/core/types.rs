use super::{DbError, Result, DataType, Value};
use serde::{Deserialize, Serialize};

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false; // Primary keys imply NOT NULL
        self.unique = true;    // Primary keys imply UNIQUE
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn validate(&self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            if !self.nullable {
                return Err(DbError::ConstraintViolation(format!(
                    "Column '{}' cannot be NULL",
                    self.name
                )));
            }
            return Ok(());
        }

        if !self.data_type.is_compatible(value) {
            return Err(DbError::TypeMismatch(format!(
                "Column '{}' expects type {}, got {}",
                self.name,
                self.data_type,
                value.type_name()
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        // 1. Exact match
        if let Some(idx) = self.columns.iter().position(|col| col.name == name) {
            return Some(idx);
        }

        // 2. Unqualified match (suffix)
        let matches: Vec<usize> = self.columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.name.ends_with(&format!(".{}", name)))
            .map(|(idx, _)| idx)
            .collect();

        if matches.len() == 1 {
            return Some(matches[0]);
        }

        None
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.find_column_index(name).map(|idx| &self.columns[idx])
    }

        pub fn column_count(&self) -> usize {

            self.columns.len()

        }

    

        pub fn merge(left: &Schema, right: &Schema) -> Self {

            let mut columns = left.columns.clone();

            columns.extend(right.columns.clone());

            Self { columns }

        }

    

        pub fn qualify_columns(&self, table_name: &str) -> Self {

            let columns = self.columns.iter().map(|col| {

                let mut new_col = col.clone();

                if !new_col.name.contains('.') {

                    new_col.name = format!("{}.{}", table_name, col.name);

                }

                new_col

            }).collect();

            Self { columns }

        }

    }

    