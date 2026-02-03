use crate::core::{DbError, Result, Row, Schema, Value};
use crate::evaluator::{EvaluationContext, ExpressionEvaluator};
use crate::parser::ast::Expr;

use async_trait::async_trait;

pub struct InListEvaluator;

#[async_trait]
impl ExpressionEvaluator for InListEvaluator {
    fn name(&self) -> &'static str {
        "IN_LIST"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::In { .. })
    }

    async fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext<'_>) -> Result<Value> {
        let Expr::In { expr, list, negated } = expr else {
            return Err(DbError::ExecutionError("Invalid IN expression".into()));
        };

        let left = context.evaluate(expr, row, schema).await?;
        if matches!(left, Value::Null) {
            return Ok(Value::Null);
        }

        let mut saw_null = false;

        for item in list {
            let right = context.evaluate(item, row, schema).await?;
            if matches!(right, Value::Null) {
                saw_null = true;
                continue;
            }
            if left == right {
                return Ok(Value::Boolean(!*negated));
            }
        }

        if saw_null {
            return Ok(Value::Null);
        }

        Ok(Value::Boolean(*negated))
    }
}
