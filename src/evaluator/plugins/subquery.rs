use super::super::{EvaluationContext, ExpressionEvaluator};
use crate::core::{DbError, Result, Row, Schema, Value};
use crate::parser::ast::Expr;

use async_trait::async_trait;

pub struct SubqueryEvaluator;

#[async_trait]
impl ExpressionEvaluator for SubqueryEvaluator {
    fn name(&self) -> &'static str {
        "SUBQUERY"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }
        )
    }

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        let handler = context.subquery_handler.ok_or_else(|| {
            DbError::UnsupportedOperation("Subqueries not supported in this context".into())
        })?;

        match expr {
            Expr::Subquery(query) => {
                let rows = handler.execute(query).await?;
                if rows.len() > 1 {
                    return Err(DbError::ExecutionError(
                        "Scalar subquery returned more than one row".into(),
                    ));
                }
                if rows.is_empty() {
                    return Ok(Value::Null);
                }
                let row = &rows[0];
                if row.len() != 1 {
                    return Err(DbError::ExecutionError(
                        "Scalar subquery returned more than one column".into(),
                    ));
                }
                Ok(row[0].clone())
            }
            Expr::InSubquery {
                expr: left_expr,
                subquery,
                negated,
            } => {
                let left_val = context.evaluate(left_expr, row, schema).await?;
                let rows = handler.execute(subquery).await?;

                // Check if any row in subquery matches left_val
                // Assuming subquery returns 1 column
                // Optimization: Hash Set for large results? For now linear scan.
                let mut found = false;
                for r in rows {
                    if r.len() != 1 {
                        return Err(DbError::ExecutionError(
                            "Subquery in IN clause must return exactly one column".into(),
                        ));
                    }
                    if r[0] == left_val {
                        found = true;
                        break;
                    }
                }

                if *negated {
                    Ok(Value::Boolean(!found))
                } else {
                    Ok(Value::Boolean(found))
                }
            }
            Expr::Exists { subquery, negated } => {
                let rows = handler.execute(subquery).await?;
                let exists = !rows.is_empty();

                if *negated {
                    Ok(Value::Boolean(!exists))
                } else {
                    Ok(Value::Boolean(exists))
                }
            }
            _ => unreachable!(),
        }
    }
}
