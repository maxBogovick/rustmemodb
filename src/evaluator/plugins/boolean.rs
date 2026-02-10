use super::super::{EvaluationContext, ExpressionEvaluator};
use crate::core::{DbError, Result, Row, Schema, Value};
use crate::parser::ast::{BinaryOp, Expr};

use async_trait::async_trait;

pub struct BooleanEvaluator;

#[async_trait]
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

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        match expr {
            // Handle AND/OR
            Expr::BinaryOp { left, op, right } => {
                // ВАЖНО: Сначала проверяем, что это именно And/Or
                if !matches!(op, BinaryOp::And | BinaryOp::Or) {
                    unreachable!("BooleanEvaluator: expected And/Or, got {:?}", op);
                }

                let left_val = context.evaluate(left, row, schema).await?;
                let right_val = context.evaluate(right, row, schema).await?;

                match (left_val, right_val) {
                    (Value::Boolean(a), Value::Boolean(b)) => {
                        let result = match op {
                            BinaryOp::And => Value::Boolean(a && b),
                            BinaryOp::Or => Value::Boolean(a || b),
                            _ => unreachable!(),
                        };
                        Ok(result)
                    }
                    (Value::Null, Value::Boolean(b)) => Ok(match op {
                        BinaryOp::And => {
                            if b {
                                Value::Null
                            } else {
                                Value::Boolean(false)
                            }
                        }
                        BinaryOp::Or => {
                            if b {
                                Value::Boolean(true)
                            } else {
                                Value::Null
                            }
                        }
                        _ => unreachable!(),
                    }),
                    (Value::Boolean(a), Value::Null) => Ok(match op {
                        BinaryOp::And => {
                            if a {
                                Value::Null
                            } else {
                                Value::Boolean(false)
                            }
                        }
                        BinaryOp::Or => {
                            if a {
                                Value::Boolean(true)
                            } else {
                                Value::Null
                            }
                        }
                        _ => unreachable!(),
                    }),
                    (Value::Null, Value::Null) => Ok(Value::Null),
                    (a, b) => Err(DbError::TypeMismatch(format!(
                        "Boolean operation requires boolean types, got {} and {}",
                        a.type_name(),
                        b.type_name()
                    ))),
                }
            }

            // Handle NOT
            Expr::Not { expr } => {
                let val = context.evaluate(expr, row, schema).await?;

                match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    Value::Null => Ok(Value::Null),
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
