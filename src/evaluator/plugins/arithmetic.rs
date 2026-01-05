use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::{Expr, BinaryOp};
use crate::core::{Result, Value, Row, Schema, DbError};

use async_trait::async_trait;

pub struct ArithmeticEvaluator;

#[async_trait]
impl ExpressionEvaluator for ArithmeticEvaluator {
    fn name(&self) -> &'static str {
        "ARITHMETIC"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        if let Expr::BinaryOp { op, .. } = expr {
            matches!(
                op,
                BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply
                | BinaryOp::Divide | BinaryOp::Modulo
            )
        } else {
            false
        }
    }

    async fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext<'_>) -> Result<Value> {
        let Expr::BinaryOp { left, op, right } = expr else {
            unreachable!();
        };

        let left_val = context.evaluate(left, row, schema).await?;
        let right_val = context.evaluate(right, row, schema).await?;

        match (left_val, right_val) {
            (Value::Integer(a), Value::Integer(b)) => {
                let result = match op {
                    BinaryOp::Add => a + b,
                    BinaryOp::Subtract => a - b,
                    BinaryOp::Multiply => a * b,
                    BinaryOp::Divide => {
                        if b == 0 {
                            return Err(DbError::ExecutionError("Division by zero".into()));
                        }
                        a / b
                    }
                    BinaryOp::Modulo => {
                        if b == 0 {
                            return Err(DbError::ExecutionError("Modulo by zero".into()));
                        }
                        a % b
                    }
                    _ => unreachable!(),
                };
                Ok(Value::Integer(result))
            }

            (Value::Float(a), Value::Float(b)) => {
                let result = match op {
                    BinaryOp::Add => a + b,
                    BinaryOp::Subtract => a - b,
                    BinaryOp::Multiply => a * b,
                    BinaryOp::Divide => a / b,
                    BinaryOp::Modulo => a % b,
                    _ => unreachable!(),
                };
                Ok(Value::Float(result))
            }

            // Mixed Integer/Float arithmetic - always returns Float
            (Value::Integer(a), Value::Float(b)) => {
                let a_float = a as f64;
                let result = match op {
                    BinaryOp::Add => a_float + b,
                    BinaryOp::Subtract => a_float - b,
                    BinaryOp::Multiply => a_float * b,
                    BinaryOp::Divide => a_float / b,
                    BinaryOp::Modulo => a_float % b,
                    _ => unreachable!(),
                };
                Ok(Value::Float(result))
            }

            (Value::Float(a), Value::Integer(b)) => {
                let b_float = b as f64;
                let result = match op {
                    BinaryOp::Add => a + b_float,
                    BinaryOp::Subtract => a - b_float,
                    BinaryOp::Multiply => a * b_float,
                    BinaryOp::Divide => a / b_float,
                    BinaryOp::Modulo => a % b_float,
                    _ => unreachable!(),
                };
                Ok(Value::Float(result))
            }

            (a, b) => Err(DbError::TypeMismatch(format!(
                "Arithmetic requires numeric types, got {} and {}",
                a.type_name(), b.type_name()
            ))),
        }
    }
}