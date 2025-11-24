use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::Expr;
use crate::core::{Result, Value, Row, Schema};

pub struct LikeEvaluator;

impl ExpressionEvaluator for LikeEvaluator {
    fn name(&self) -> &'static str {
        "LIKE"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Like { .. })
    }

    fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext) -> Result<Value> {
        let Expr::Like { expr, pattern, negated, case_insensitive } = expr else {
            unreachable!();
        };

        let text_val = context.evaluate(expr, row, schema)?;
        let pattern_val = context.evaluate(pattern, row, schema)?;

        let result = match (&text_val, &pattern_val) {
            (Value::Text(text), Value::Text(pat)) => {
                crate::expression::pattern::eval_like(text, pat, !case_insensitive)?
            }
            _ => false,
        };

        Ok(Value::Boolean(if *negated { !result } else { result }))
    }
}