use super::*;

impl PersistValue for i64 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for i32 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for u64 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for usize {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for f64 {
    fn sql_type() -> &'static str {
        "FLOAT"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for f32 {
    fn sql_type() -> &'static str {
        "FLOAT"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for bool {
    fn sql_type() -> &'static str {
        "BOOLEAN"
    }

    fn to_sql_literal(&self) -> String {
        if *self {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }
    }
}

impl PersistValue for String {
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", sql_escape_string(self))
    }
}

impl PersistValue for Uuid {
    fn sql_type() -> &'static str {
        "UUID"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", self)
    }
}

impl PersistValue for DateTime<Utc> {
    fn sql_type() -> &'static str {
        "TIMESTAMP"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", self.to_rfc3339())
    }
}

impl PersistValue for NaiveDate {
    fn sql_type() -> &'static str {
        "DATE"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d"))
    }
}

impl<T: PersistValue> PersistValue for Option<T> {
    fn sql_type() -> &'static str {
        T::sql_type()
    }

    fn to_sql_literal(&self) -> String {
        match self {
            Some(value) => value.to_sql_literal(),
            None => "NULL".to_string(),
        }
    }
}
