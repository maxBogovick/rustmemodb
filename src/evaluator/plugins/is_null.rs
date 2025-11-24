use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::Expr;
use crate::core::{Result, Value, Row, Schema};

pub struct IsNullEvaluator;

impl ExpressionEvaluator for IsNullEvaluator {
    fn name(&self) -> &'static str {
        "IS_NULL"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::IsNull { .. })
    }

    fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext) -> Result<Value> {
        let Expr::IsNull { expr, negated } = expr else {
            unreachable!();
        };

        let val = context.evaluate(expr, row, schema)?;
        let is_null = matches!(val, Value::Null);

        Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
    }
}