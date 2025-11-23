use super::{DbError, Result, DataType, Value};

pub type Row = Vec<Value>;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
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

#[derive(Debug, Clone)]
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
        self.columns.iter().position(|col| col.name == name)
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.find_column_index(name).map(|idx| &self.columns[idx])
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}