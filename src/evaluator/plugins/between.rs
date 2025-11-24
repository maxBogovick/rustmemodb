use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::{Expr, BinaryOp};
use crate::core::{Result, Value, Row, Schema};
use crate::evaluator::plugins::comparison::ComparisonEvaluator;

pub struct BetweenEvaluator;

impl ExpressionEvaluator for BetweenEvaluator {
    fn name(&self) -> &'static str {
        "BETWEEN"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Between { .. })
    }

    fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext) -> Result<Value> {
        let Expr::Between { expr, low, high, negated } = expr else {
            unreachable!();
        };

        let val = context.evaluate(expr, row, schema)?;
        let low_val = context.evaluate(low, row, schema)?;
        let high_val = context.evaluate(high, row, schema)?;

        if matches!(val, Value::Null) {
            return Ok(Value::Boolean(false));
        }

        // Используем компаратор из другого evaluator'а (композиция!)
        let comparator = ComparisonEvaluator;

        let ge_low = comparator.compare(&val, &low_val, &BinaryOp::GtEq)?;
        let le_high = comparator.compare(&val, &high_val, &BinaryOp::LtEq)?;
        let result = ge_low && le_high;

        Ok(Value::Boolean(if *negated { !result } else { result }))
    }
}