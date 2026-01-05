use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::Expr;
use crate::core::{Result, Value, Row, Schema};

use async_trait::async_trait;

pub struct IsNullEvaluator;

#[async_trait]
impl ExpressionEvaluator for IsNullEvaluator {
    fn name(&self) -> &'static str {
        "IS_NULL"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::IsNull { .. })
    }

    async fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext<'_>) -> Result<Value> {
        let Expr::IsNull { expr, negated } = expr else {
            unreachable!();
        };

        let val = context.evaluate(expr, row, schema).await?;

        let is_null = matches!(val, Value::Null);
        let result = if *negated { !is_null } else { is_null };

        Ok(Value::Boolean(result))
    }
}