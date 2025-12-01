use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::{Expr, BinaryOp};
use crate::core::{Result, Value, Row, Schema, DbError};

pub struct BooleanEvaluator;

impl ExpressionEvaluator for BooleanEvaluator {
    fn name(&self) -> &'static str {
        "BOOLEAN"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryOp { op, .. } => {
                matches!(op, BinaryOp::And | BinaryOp::Or)
            }
            Expr::Not { .. } => true,
            _ => false,
        }
    }

    fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext) -> Result<Value> {
        match expr {
            // Handle AND/OR
            Expr::BinaryOp { left, op, right } => {
                // ВАЖНО: Сначала проверяем, что это именно And/Or
                if !matches!(op, BinaryOp::And | BinaryOp::Or) {
                    unreachable!("BooleanEvaluator: expected And/Or, got {:?}", op);
                }

                let left_val = context.evaluate(left, row, schema)?;
                let right_val = context.evaluate(right, row, schema)?;

                match (left_val, right_val) {
                    (Value::Boolean(a), Value::Boolean(b)) => {
                        let result = match op {
                            BinaryOp::And => a && b,
                            BinaryOp::Or => a || b,
                            _ => unreachable!(),
                        };
                        Ok(Value::Boolean(result))
                    }
                    (a, b) => Err(DbError::TypeMismatch(format!(
                        "Boolean operation requires boolean types, got {} and {}",
                        a.type_name(), b.type_name()
                    ))),
                }
            }

            // Handle NOT
            Expr::Not { expr } => {
                let val = context.evaluate(expr, row, schema)?;

                match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    other => Err(DbError::TypeMismatch(format!(
                        "NOT operation requires boolean type, got {}",
                        other.type_name()
                    ))),
                }
            }

            _ => unreachable!("BooleanEvaluator called with non-boolean expression"),
        }
    }
}