use super::super::{ExpressionEvaluator, EvaluationContext};
use crate::parser::ast::{Expr, BinaryOp};
use crate::core::{Result, Value, Row, Schema};

pub struct LogicalEvaluator;

impl ExpressionEvaluator for LogicalEvaluator {
    fn name(&self) -> &'static str {
        "LOGICAL"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        if let Expr::BinaryOp { op, .. } = expr {
            matches!(op, BinaryOp::And | BinaryOp::Or)
        } else {
            false
        }
    }

    fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext) -> Result<Value> {
        let Expr::BinaryOp { left, op, right } = expr else {
            unreachable!();
        };

        match op {
            BinaryOp::And => {
                let left_val = context.evaluate(left, row, schema)?;
                if !left_val.as_bool() {
                    return Ok(Value::Boolean(false));
                }
                let right_val = context.evaluate(right, row, schema)?;
                Ok(Value::Boolean(right_val.as_bool()))
            }

            BinaryOp::Or => {
                let left_val = context.evaluate(left, row, schema)?;
                if left_val.as_bool() {
                    return Ok(Value::Boolean(true));
                }
                let right_val = context.evaluate(right, row, schema)?;
                Ok(Value::Boolean(right_val.as_bool()))
            }

            _ => unreachable!(),
        }
    }
}