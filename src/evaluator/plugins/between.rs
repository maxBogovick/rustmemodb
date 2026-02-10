use super::super::{EvaluationContext, ExpressionEvaluator};
use crate::core::{Result, Row, Schema, Value};
use crate::evaluator::plugins::comparison::ComparisonEvaluator;
use crate::parser::ast::{BinaryOp, Expr};

use async_trait::async_trait;

pub struct BetweenEvaluator;

#[async_trait]
impl ExpressionEvaluator for BetweenEvaluator {
    fn name(&self) -> &'static str {
        "BETWEEN"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Between { .. })
    }

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        let Expr::Between {
            expr,
            low,
            high,
            negated,
        } = expr
        else {
            unreachable!();
        };

        let val = context.evaluate(expr, row, schema).await?;
        let low_val = context.evaluate(low, row, schema).await?;
        let high_val = context.evaluate(high, row, schema).await?;

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
