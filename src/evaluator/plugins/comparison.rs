use super::super::{EvaluationContext, ExpressionEvaluator};
use crate::core::{DbError, Result, Row, Schema, Value};
use crate::parser::ast::{BinaryOp, Expr};

pub struct ComparisonEvaluator;

impl ExpressionEvaluator for ComparisonEvaluator {
    fn name(&self) -> &'static str {
        "COMPARISON"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        if let Expr::BinaryOp { op, .. } = expr {
            matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            )
        } else {
            false
        }
    }

    fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext,
    ) -> Result<Value> {
        let Expr::BinaryOp { left, op, right } = expr else {
            unreachable!();
        };

        let left_val = context.evaluate(left, row, schema)?;
        let right_val = context.evaluate(right, row, schema)?;

        let result = self.compare(&left_val, &right_val, op)?;
        Ok(Value::Boolean(result))
    }
}

impl ComparisonEvaluator {
    pub fn compare(&self, left: &Value, right: &Value, op: &BinaryOp) -> Result<bool> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),

            (Value::Integer(a), Value::Integer(b)) => Ok(match op {
                BinaryOp::Eq => a == b,
                BinaryOp::NotEq => a != b,
                BinaryOp::Lt => a < b,
                BinaryOp::LtEq => a <= b,
                BinaryOp::Gt => a > b,
                BinaryOp::GtEq => a >= b,
                _ => unreachable!(),
            }),

            (Value::Float(a), Value::Float(b)) => Ok(match op {
                BinaryOp::Eq => (a - b).abs() < f64::EPSILON,
                BinaryOp::NotEq => (a - b).abs() >= f64::EPSILON,
                BinaryOp::Lt => a < b,
                BinaryOp::LtEq => a <= b,
                BinaryOp::Gt => a > b,
                BinaryOp::GtEq => a >= b,
                _ => unreachable!(),
            }),

            (Value::Text(a), Value::Text(b)) => Ok(match op {
                BinaryOp::Eq => a == b,
                BinaryOp::NotEq => a != b,
                BinaryOp::Lt => a < b,
                BinaryOp::LtEq => a <= b,
                BinaryOp::Gt => a > b,
                BinaryOp::GtEq => a >= b,
                _ => unreachable!(),
            }),

            _ => Err(DbError::TypeMismatch(format!(
                "Cannot compare {} with {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }
}
