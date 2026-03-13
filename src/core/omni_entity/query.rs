use crate::core::omni_entity::{OmniValue, SqlEntity};
use crate::core::{DbError, Result, Value};
use crate::facade::InMemoryDB;
use std::marker::PhantomData;

/// Extension trait for InMemoryDB allowing OmniEntity typed queries
pub trait OmniQueryExt {
    fn query_as<'a, T: SqlEntity>(&'a self) -> OmniQueryBuilder<'a, T>;
    fn query_join<'a, T: TupleSqlEntity>(&'a self) -> OmniTupleQueryBuilder<'a, T>;
}

impl OmniQueryExt for InMemoryDB {
    fn query_as<'a, T: SqlEntity>(&'a self) -> OmniQueryBuilder<'a, T> {
        OmniQueryBuilder {
            db: self,
            sql: None,
            params: vec![],
            _marker: PhantomData,
        }
    }

    fn query_join<'a, T: TupleSqlEntity>(&'a self) -> OmniTupleQueryBuilder<'a, T> {
        OmniTupleQueryBuilder {
            db: self,
            sql: None,
            params: vec![],
            _marker: PhantomData,
        }
    }
}

pub struct OmniQueryBuilder<'a, T> {
    db: &'a InMemoryDB,
    sql: Option<String>,
    params: Vec<Value>,
    _marker: PhantomData<T>,
}

impl<'a, T: SqlEntity> OmniQueryBuilder<'a, T> {
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.sql = Some(sql.to_string());
        self
    }

    pub fn with_param<P: OmniValue>(mut self, param: P) -> Self {
        self.params.push(param.into_db_value());
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<T>> {
        let sql = self.sql.ok_or_else(|| {
            DbError::ExecutionError("SQL query missing for OmniQueryBuilder".into())
        })?;

        let result = self
            .db
            .execute_readonly_with_params(&sql, None, self.params)
            .await?;

        let mut out = Vec::with_capacity(result.row_count());
        for row in result.rows() {
            if let Some(entity) = T::from_sql_row(row, 0).map_err(DbError::ExecutionError)? {
                out.push(entity);
            }
        }
        Ok(out)
    }

    pub async fn fetch_optional(self) -> Result<Option<T>> {
        let mut all = self.fetch_all().await?;
        Ok(if all.is_empty() {
            None
        } else {
            Some(all.remove(0))
        })
    }
}

pub struct OmniTupleQueryBuilder<'a, T> {
    db: &'a InMemoryDB,
    sql: Option<String>,
    params: Vec<Value>,
    _marker: PhantomData<T>,
}

impl<'a, T: TupleSqlEntity> OmniTupleQueryBuilder<'a, T> {
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.sql = Some(sql.to_string());
        self
    }

    pub fn with_param<P: OmniValue>(mut self, param: P) -> Self {
        self.params.push(param.into_db_value());
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<T>> {
        let sql = self.sql.ok_or_else(|| {
            DbError::ExecutionError("SQL query missing for OmniTupleQueryBuilder".into())
        })?;

        let result = self
            .db
            .execute_readonly_with_params(&sql, None, self.params)
            .await?;

        let mut out = Vec::with_capacity(result.row_count());
        for row in result.rows() {
            if let Some(tuple) = T::from_sql_row(row, 0).map_err(DbError::ExecutionError)? {
                out.push(tuple);
            }
        }
        Ok(out)
    }

    pub async fn fetch_optional(self) -> Result<Option<T>> {
        let mut all = self.fetch_all().await?;
        Ok(if all.is_empty() {
            None
        } else {
            Some(all.remove(0))
        })
    }
}

/// Helper trait for nested tuple deserialization across JOIN outputs
pub trait TupleSqlEntity: Sized {
    /// Returns (EntityTuple, parsed_fields_count)
    fn from_sql_row(row: &[Value], offset: usize) -> std::result::Result<Option<Self>, String>;
}

// Implement up to 4 arity for JOINs
impl<A: SqlEntity> TupleSqlEntity for (A,) {
    fn from_sql_row(row: &[Value], offset: usize) -> std::result::Result<Option<Self>, String> {
        let a = A::from_sql_row(row, offset)?;
        match a {
            Some(a) => Ok(Some((a,))),
            None => Ok(None),
        }
    }
}

impl<A: SqlEntity, B: SqlEntity> TupleSqlEntity for (A, B) {
    fn from_sql_row(row: &[Value], offset: usize) -> std::result::Result<Option<Self>, String> {
        let a = A::from_sql_row(row, offset)?;
        let b = B::from_sql_row(row, offset + A::fields().len())?;
        match (a, b) {
            (Some(a), Some(b)) => Ok(Some((a, b))),
            _ => Ok(None),
        }
    }
}

impl<A: SqlEntity, B: SqlEntity, C: SqlEntity> TupleSqlEntity for (A, B, C) {
    fn from_sql_row(row: &[Value], offset: usize) -> std::result::Result<Option<Self>, String> {
        let a = A::from_sql_row(row, offset)?;
        let b_offset = offset + A::fields().len();
        let b = B::from_sql_row(row, b_offset)?;
        let c_offset = b_offset + B::fields().len();
        let c = C::from_sql_row(row, c_offset)?;
        match (a, b, c) {
            (Some(a), Some(b), Some(c)) => Ok(Some((a, b, c))),
            _ => Ok(None),
        }
    }
}

impl<A: SqlEntity, B: SqlEntity, C: SqlEntity, D: SqlEntity> TupleSqlEntity for (A, B, C, D) {
    fn from_sql_row(row: &[Value], offset: usize) -> std::result::Result<Option<Self>, String> {
        let a = A::from_sql_row(row, offset)?;
        let b_offset = offset + A::fields().len();
        let b = B::from_sql_row(row, b_offset)?;
        let c_offset = b_offset + B::fields().len();
        let c = C::from_sql_row(row, c_offset)?;
        let d_offset = c_offset + C::fields().len();
        let d = D::from_sql_row(row, d_offset)?;
        match (a, b, c, d) {
            (Some(a), Some(b), Some(c), Some(d)) => Ok(Some((a, b, c, d))),
            _ => Ok(None),
        }
    }
}
