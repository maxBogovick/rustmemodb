use crate::core::{DbError, Result, Row, Schema, Value};
use crate::evaluator::{EvaluationContext, ExpressionEvaluator};
use crate::parser::ast::Expr;
use async_trait::async_trait;

pub struct FunctionEvaluator;

#[async_trait]
impl ExpressionEvaluator for FunctionEvaluator {
    fn name(&self) -> &'static str {
        "FUNCTION"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Function { .. })
    }

    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if let Expr::Function { name, args, .. } = expr {
            let mut eval_args = Vec::with_capacity(args.len());
            for arg in args {
                eval_args.push(context.evaluate(arg, row, schema).await?);
            }

            match name.to_uppercase().as_str() {
                "UPPER" => self.upper(&eval_args),
                "LOWER" => self.lower(&eval_args),
                "LENGTH" => self.length(&eval_args),
                "COALESCE" => self.coalesce(&eval_args),
                "NOW" => self.now(),
                _ => Err(DbError::UnsupportedOperation(format!(
                    "Unknown function: {}",
                    name
                ))),
            }
        } else {
            unreachable!()
        }
    }
}

impl FunctionEvaluator {
    fn upper(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(DbError::ExecutionError("UPPER expects 1 argument".into()));
        }
        match &args[0] {
            Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
            Value::Null => Ok(Value::Null),
            v => Ok(Value::Text(v.to_string().to_uppercase())), // Auto-cast?
        }
    }

    fn lower(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(DbError::ExecutionError("LOWER expects 1 argument".into()));
        }
        match &args[0] {
            Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
            Value::Null => Ok(Value::Null),
            v => Ok(Value::Text(v.to_string().to_lowercase())),
        }
    }

    fn length(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(DbError::ExecutionError("LENGTH expects 1 argument".into()));
        }
        match &args[0] {
            Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
            Value::Null => Ok(Value::Null),
            _ => Err(DbError::TypeMismatch("LENGTH expects Text".into())),
        }
    }

    fn coalesce(&self, args: &[Value]) -> Result<Value> {
        for arg in args {
            if !matches!(arg, Value::Null) {
                return Ok(arg.clone());
            }
        }
        Ok(Value::Null)
    }

    fn now(&self) -> Result<Value> {
        Ok(Value::Timestamp(chrono::Utc::now()))
    }
}
