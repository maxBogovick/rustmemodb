use super::{ExecutionContext, Executor};
use crate::core::{DbError, Result, Row, Value};
use crate::parser::ast::{Expr, Statement};
use crate::planner::{LogicalPlan, QueryPlanner};
use crate::result::QueryResult;
use crate::storage::Catalog;
use std::sync::{Arc, RwLock};

pub struct QueryExecutor {
    planner: QueryPlanner,
    catalog: Catalog,
}

impl QueryExecutor {
    pub fn new(catalog: Catalog) -> Self {
        Self {
            planner: QueryPlanner::new(),
            catalog,
        }
    }

    /// Обновить catalog - заменяем на новую версию
    pub fn update_catalog(&mut self, new_catalog: Catalog) {
        self.catalog = new_catalog;
    }

    /// Выполнить план - каждая таблица блокируется независимо
    fn execute_plan(&self, plan: &LogicalPlan, ctx: &ExecutionContext) -> Result<Vec<Row>> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                // Read lock только на одну таблицу
                ctx.storage.scan_table(&scan.table_name)
            }

            LogicalPlan::Filter(filter) => {
                let input_rows = self.execute_plan(&filter.input, ctx)?;
                let table_name = self.get_table_name(plan)?;
                let schema = ctx.storage.get_schema(table_name.as_str())?;

                Ok(input_rows
                    .into_iter()
                    .filter(|row| {
                        self.evaluate_filter(&filter.predicate, row, schema.schema())
                            .unwrap_or(false)
                    })
                    .collect())
            }

            LogicalPlan::Projection(proj) => {
                let input_rows = self.execute_plan(&proj.input, ctx)?;
                let table_name = self.get_table_name(plan)?;
                let schema = ctx.storage.get_schema(table_name.as_str())?;

                Ok(input_rows
                    .into_iter()
                    .map(|row| {
                        self.project_row(&proj.expressions, &row, schema.schema())
                            .unwrap_or_default()
                    })
                    .collect())
            }

            LogicalPlan::Sort(_) => Err(DbError::UnsupportedOperation(
                "Sort not yet implemented".into(),
            )),

            LogicalPlan::Limit(limit) => {
                let mut input_rows = self.execute_plan(&limit.input, ctx)?;
                input_rows.truncate(limit.limit);
                Ok(input_rows)
            }
        }
    }

    fn get_table_name(&self, plan: &LogicalPlan) -> Result<String> {
        match plan {
            LogicalPlan::TableScan(scan) => Ok(scan.table_name.clone()),
            LogicalPlan::Filter(filter) => self.get_table_name(&filter.input),
            LogicalPlan::Projection(proj) => self.get_table_name(&proj.input),
            LogicalPlan::Sort(sort) => self.get_table_name(&sort.input),
            LogicalPlan::Limit(limit) => self.get_table_name(&limit.input),
        }
    }

    fn evaluate_filter(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &crate::core::Schema,
    ) -> Result<bool> {
        let value = self.evaluate_expr(expr, row, schema)?;
        Ok(value.as_bool())
    }

    fn project_row(
        &self,
        expressions: &[Expr],
        row: &Row,
        schema: &crate::core::Schema,
    ) -> Result<Row> {
        expressions
            .iter()
            .map(|expr| self.evaluate_expr(expr, row, schema))
            .collect()
    }

    fn evaluate_expr(&self, expr: &Expr, row: &Row, schema: &crate::core::Schema) -> Result<Value> {
        match expr {
            Expr::Column(name) => {
                let idx = schema
                    .find_column_index(name)
                    .ok_or_else(|| DbError::ColumnNotFound(name.clone(), "table".into()))?;
                Ok(row[idx].clone())
            }

            Expr::Literal(val) => Ok(val.clone()),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left, row, schema)?;
                let right_val = self.evaluate_expr(right, row, schema)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }

            Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                let text_val = self.evaluate_expr(expr, row, schema)?;
                let pattern_val = self.evaluate_expr(pattern, row, schema)?;

                let result = match (&text_val, &pattern_val) {
                    (Value::Text(text), Value::Text(pat)) => {
                        crate::expression::pattern::eval_like(text, pat, true)?
                    }
                    _ => false,
                };

                Ok(Value::Boolean(if *negated { !result } else { result }))
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = self.evaluate_expr(expr, row, schema)?;
                let low_val = self.evaluate_expr(low, row, schema)?;
                let high_val = self.evaluate_expr(high, row, schema)?;

                let ge_low = self.compare(&val, &low_val, &crate::parser::ast::BinaryOp::GtEq)?;
                let le_high = self.compare(&val, &high_val, &crate::parser::ast::BinaryOp::LtEq)?;
                let result = ge_low && le_high;

                Ok(Value::Boolean(if *negated { !result } else { result }))
            }

            Expr::IsNull { expr, negated } => {
                let val = self.evaluate_expr(expr, row, schema)?;
                let is_null = matches!(val, Value::Null);
                Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
            }

            _ => Err(DbError::UnsupportedOperation(format!(
                "Expression not yet implemented: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &crate::parser::ast::BinaryOp,
        right: &Value,
    ) -> Result<Value> {
        use crate::parser::ast::BinaryOp::*;

        match op {
            And => {
                if !left.as_bool() {
                    return Ok(Value::Boolean(false));
                }
                Ok(Value::Boolean(right.as_bool()))
            }

            Or => {
                if left.as_bool() {
                    return Ok(Value::Boolean(true));
                }
                Ok(Value::Boolean(right.as_bool()))
            }

            Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                let result = self.compare(left, right, op)?;
                Ok(Value::Boolean(result))
            }

            Add | Subtract | Multiply | Divide | Modulo => {
                self.evaluate_arithmetic(left, op, right)
            }
        }
    }

    fn compare(
        &self,
        left: &Value,
        right: &Value,
        op: &crate::parser::ast::BinaryOp,
    ) -> Result<bool> {
        use crate::parser::ast::BinaryOp::*;

        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),

            (Value::Integer(a), Value::Integer(b)) => Ok(match op {
                Eq => a == b,
                NotEq => a != b,
                Lt => a < b,
                LtEq => a <= b,
                Gt => a > b,
                GtEq => a >= b,
                _ => unreachable!(),
            }),

            (Value::Float(a), Value::Float(b)) => Ok(match op {
                Eq => (a - b).abs() < f64::EPSILON,
                NotEq => (a - b).abs() >= f64::EPSILON,
                Lt => a < b,
                LtEq => a <= b,
                Gt => a > b,
                GtEq => a >= b,
                _ => unreachable!(),
            }),

            (Value::Text(a), Value::Text(b)) => Ok(match op {
                Eq => a == b,
                NotEq => a != b,
                Lt => a < b,
                LtEq => a <= b,
                Gt => a > b,
                GtEq => a >= b,
                _ => unreachable!(),
            }),

            (Value::Boolean(a), Value::Boolean(b)) => Ok(match op {
                Eq => a == b,
                NotEq => a != b,
                _ => {
                    return Err(DbError::TypeMismatch(
                        "Booleans only support equality".into(),
                    ));
                }
            }),

            _ => Err(DbError::TypeMismatch(format!(
                "Cannot compare {} with {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }

    fn evaluate_arithmetic(
        &self,
        left: &Value,
        op: &crate::parser::ast::BinaryOp,
        right: &Value,
    ) -> Result<Value> {
        use crate::parser::ast::BinaryOp::*;

        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => {
                let result = match op {
                    Add => a + b,
                    Subtract => a - b,
                    Multiply => a * b,
                    Divide => {
                        if *b == 0 {
                            return Err(DbError::ExecutionError("Division by zero".into()));
                        }
                        a / b
                    }
                    Modulo => {
                        if *b == 0 {
                            return Err(DbError::ExecutionError("Modulo by zero".into()));
                        }
                        a % b
                    }
                    _ => unreachable!(),
                };
                Ok(Value::Integer(result))
            }

            (Value::Float(a), Value::Float(b)) => {
                let result = match op {
                    Add => a + b,
                    Subtract => a - b,
                    Multiply => a * b,
                    Divide => a / b,
                    Modulo => a % b,
                    _ => unreachable!(),
                };
                Ok(Value::Float(result))
            }

            _ => Err(DbError::TypeMismatch(
                "Arithmetic requires numeric types".into(),
            )),
        }
    }
}

impl Executor for QueryExecutor {
    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Query(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        let Statement::Query(query) = stmt else {
            unreachable!();
        };

        let plan = self
            .planner
            .plan(&Statement::Query(query.clone()), &self.catalog)?;

        let rows = self.execute_plan(&plan, ctx)?;
        let columns = self.get_output_columns(&plan, ctx)?;

        Ok(QueryResult::new(columns, rows))
    }
}

impl QueryExecutor {
    fn get_output_columns(
        &self,
        plan: &LogicalPlan,
        ctx: &ExecutionContext,
    ) -> Result<Vec<String>> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let schema = ctx.storage.get_schema(&scan.table_name)?;
                Ok(schema
                    .schema()
                    .columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect())
            }

            LogicalPlan::Projection(proj) => Ok(proj
                .expressions
                .iter()
                .enumerate()
                .map(|(i, expr)| match expr {
                    Expr::Column(name) => name.clone(),
                    _ => format!("col_{}", i),
                })
                .collect()),

            LogicalPlan::Filter(filter) => self.get_output_columns(&filter.input, ctx),
            LogicalPlan::Sort(sort) => self.get_output_columns(&sort.input, ctx),
            LogicalPlan::Limit(limit) => self.get_output_columns(&limit.input, ctx),
        }
    }
}
