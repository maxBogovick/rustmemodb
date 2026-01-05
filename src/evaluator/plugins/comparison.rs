use super::super::{EvaluationContext, ExpressionEvaluator};
use crate::core::{DbError, Result, Row, Schema, Value};
use crate::parser::ast::{BinaryOp, Expr};

use async_trait::async_trait;

pub struct ComparisonEvaluator;

#[async_trait]
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

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        let Expr::BinaryOp { left, op, right } = expr else {
            unreachable!();
        };

        let left_val = context.evaluate(left, row, schema).await?;
        let right_val = context.evaluate(right, row, schema).await?;

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

            // Mixed Integer/Float comparisons
            (Value::Integer(a), Value::Float(b)) => {
                let a_float = *a as f64;
                Ok(match op {
                    BinaryOp::Eq => (a_float - b).abs() < f64::EPSILON,
                    BinaryOp::NotEq => (a_float - b).abs() >= f64::EPSILON,
                    BinaryOp::Lt => a_float < *b,
                    BinaryOp::LtEq => a_float <= *b,
                    BinaryOp::Gt => a_float > *b,
                    BinaryOp::GtEq => a_float >= *b,
                    _ => unreachable!(),
                })
            }

            (Value::Float(a), Value::Integer(b)) => {
                let b_float = *b as f64;
                Ok(match op {
                    BinaryOp::Eq => (a - b_float).abs() < f64::EPSILON,
                    BinaryOp::NotEq => (a - b_float).abs() >= f64::EPSILON,
                    BinaryOp::Lt => *a < b_float,
                    BinaryOp::LtEq => *a <= b_float,
                    BinaryOp::Gt => *a > b_float,
                    BinaryOp::GtEq => *a >= b_float,
                    _ => unreachable!(),
                })
            }

            (Value::Text(a), Value::Text(b)) => Ok(match op {
                BinaryOp::Eq => a == b,
                BinaryOp::NotEq => a != b,
                BinaryOp::Lt => a < b,
                BinaryOp::LtEq => a <= b,
                BinaryOp::Gt => a > b,
                BinaryOp::GtEq => a >= b,
                _ => unreachable!(),
            }),

            // ДОБАВЛЕНО: Поддержка сравнения Boolean значений
            (Value::Boolean(a), Value::Boolean(b)) => Ok(match op {
                BinaryOp::Eq => a == b,
                BinaryOp::NotEq => a != b,
                // Для булевых значений: false < true
                BinaryOp::Lt => !a && *b,      // false < true
                BinaryOp::LtEq => a <= b,
                BinaryOp::Gt => *a && !b,      // true > false
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
