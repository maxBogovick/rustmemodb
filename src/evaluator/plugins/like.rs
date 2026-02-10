use super::super::{EvaluationContext, ExpressionEvaluator};
use crate::core::{Result, Row, Schema, Value};
use crate::parser::ast::Expr;

use async_trait::async_trait;

pub struct LikeEvaluator;

#[async_trait]
impl ExpressionEvaluator for LikeEvaluator {
    fn name(&self) -> &'static str {
        "LIKE"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Like { .. })
    }

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        let Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } = expr
        else {
            unreachable!();
        };

        let text_val = context.evaluate(expr, row, schema).await?;
        let pattern_val = context.evaluate(pattern, row, schema).await?;

        let result = match (&text_val, &pattern_val) {
            (Value::Text(text), Value::Text(pat)) => {
                crate::expression::pattern::eval_like(text, pat, !case_insensitive)?
            }
            _ => false,
        };

        Ok(Value::Boolean(if *negated { !result } else { result }))
    }
}
