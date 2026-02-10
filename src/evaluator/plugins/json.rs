use crate::core::{DbError, Result, Row, Schema, Value};
use crate::evaluator::{EvaluationContext, ExpressionEvaluator};
use crate::parser::ast::{BinaryOp, Expr};
use async_trait::async_trait;

pub struct JsonEvaluator;

#[async_trait]
impl ExpressionEvaluator for JsonEvaluator {
    fn name(&self) -> &'static str {
        "JSON"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::Arrow | BinaryOp::LongArrow,
                ..
            }
        )
    }

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if let Expr::BinaryOp { left, op, right } = expr {
            let left_val = context.evaluate(left, row, schema).await?;
            let right_val = context.evaluate(right, row, schema).await?;

            if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                return Ok(Value::Null);
            }

            let json_value = match left_val {
                Value::Json(j) => j,
                Value::Text(s) => serde_json::from_str(&s)
                    .map_err(|e| DbError::ExecutionError(format!("Invalid JSON: {}", e)))?,
                _ => {
                    return Err(DbError::TypeMismatch(
                        "Left operand of JSON op must be JSON".into(),
                    ));
                }
            };

            let res = match right_val {
                Value::Text(s) => json_value.get(&s),
                Value::Integer(i) => {
                    if i < 0 {
                        return Ok(Value::Null);
                    }
                    json_value.get(i as usize)
                }
                _ => {
                    return Err(DbError::TypeMismatch(
                        "Right operand of JSON op must be Text or Integer".into(),
                    ));
                }
            };

            match res {
                Some(v) => {
                    if matches!(op, BinaryOp::LongArrow) {
                        // ->> returns Text unquoted
                        if let serde_json::Value::String(s) = v {
                            Ok(Value::Text(s.clone()))
                        } else {
                            Ok(Value::Text(v.to_string()))
                        }
                    } else {
                        // -> returns JSON
                        Ok(Value::Json(v.clone()))
                    }
                }
                None => Ok(Value::Null),
            }
        } else {
            unreachable!()
        }
    }
}
