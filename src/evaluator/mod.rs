pub mod plugins;

use crate::core::{Result, Row, Schema, Value};
use crate::parser::ast::Expr;

use async_trait::async_trait;

/// Trait для оценки выражений
#[async_trait]
pub trait ExpressionEvaluator: Send + Sync {
    /// Имя evaluator'а
    fn name(&self) -> &'static str;

    /// Может ли evaluator обработать это выражение?
    fn can_evaluate(&self, expr: &Expr) -> bool;

    /// Вычислить выражение
    async fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext<'_>) -> Result<Value>;
}

/// Контекст для оценки выражений
pub struct EvaluationContext<'a> {
    /// Реестр evaluators
    registry: &'a EvaluatorRegistry,
}

impl<'a> EvaluationContext<'a> {
    pub fn new(registry: &'a EvaluatorRegistry) -> Self {
        Self { registry }
    }

    /// Вычислить выражение через подходящий evaluator
    pub async fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
        // Базовые случаи (всегда напрямую)
        match expr {
            Expr::Column(name) => {
                let idx = schema
                    .find_column_index(name)
                    .ok_or_else(|| crate::core::DbError::ColumnNotFound(
                        name.clone(),
                        "table".into()
                    ))?;
                return Ok(row[idx].clone());
            }
            Expr::CompoundIdentifier(parts) => {
                let name = parts.join(".");
                let idx = schema
                    .find_column_index(&name)
                    .ok_or_else(|| crate::core::DbError::ColumnNotFound(
                        name.clone(),
                        "table".into()
                    ))?;
                return Ok(row[idx].clone());
            }
            Expr::Literal(val) => {
                return Ok(val.clone());
            }
            _ => {}
        }

        // Ищем подходящий evaluator
        if let Some(evaluator) = self.registry.find_evaluator(expr) {
            return evaluator.evaluate(expr, row, schema, self).await;
        }

        Err(crate::core::DbError::UnsupportedOperation(format!(
            "No evaluator found for expression: {:?}",
            expr
        )))
    }
}

/// Registry для evaluators
pub struct EvaluatorRegistry {
    evaluators: Vec<Box<dyn ExpressionEvaluator>>,
}

impl EvaluatorRegistry {
    pub fn new() -> Self {
        Self {
            evaluators: Vec::new(),
        }
    }

    pub fn register(&mut self, evaluator: Box<dyn ExpressionEvaluator>) {
        println!("🧮 Registered evaluator: {}", evaluator.name());
        self.evaluators.push(evaluator);
    }

    /// Автоматическая регистрация всех встроенных evaluators
    pub fn with_default_evaluators() -> Self {
        use plugins::*;

        let mut registry = Self::new();

        // Автоматически регистрируем все evaluators
        registry.register(Box::new(boolean::BooleanEvaluator));
        registry.register(Box::new(comparison::ComparisonEvaluator));
        registry.register(Box::new(arithmetic::ArithmeticEvaluator));
        registry.register(Box::new(logical::LogicalEvaluator));
        registry.register(Box::new(like::LikeEvaluator));
        registry.register(Box::new(between::BetweenEvaluator));
        registry.register(Box::new(is_null::IsNullEvaluator));

        registry
    }

    fn find_evaluator(&self, expr: &Expr) -> Option<&dyn ExpressionEvaluator> {
        self.evaluators
            .iter()
            .find(|ev| ev.can_evaluate(expr))
            .map(|boxed| &**boxed)
    }
}

impl Default for EvaluatorRegistry {
    fn default() -> Self {
        Self::with_default_evaluators()
    }
}