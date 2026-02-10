pub mod plugins;

use crate::core::{Result, Row, Schema, Value};
use crate::parser::ast::{Expr, QueryStmt};

use async_trait::async_trait;

/// Trait for executing subqueries
#[async_trait]
pub trait SubqueryHandler: Send + Sync {
    async fn execute(&self, query: &QueryStmt) -> Result<Vec<Row>>;
}

/// Trait для оценки выражений
#[async_trait]
pub trait ExpressionEvaluator: Send + Sync {
    /// Имя evaluator'а
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Может ли evaluator обработать это выражение?
    fn can_evaluate(&self, expr: &Expr) -> bool;

    /// Вычислить выражение
    async fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext<'_>,
    ) -> Result<Value>;
}

/// Контекст для оценки выражений
pub struct EvaluationContext<'a> {
    /// Реестр evaluators
    registry: &'a EvaluatorRegistry,
    /// Обработчик подзапросов
    pub subquery_handler: Option<&'a dyn SubqueryHandler>,
    /// Параметры запроса ($1, $2...)
    pub params: &'a [Value],
    column_cache: std::sync::Mutex<
        std::collections::HashMap<usize, std::collections::HashMap<String, usize>>,
    >,
}

impl<'a> EvaluationContext<'a> {
    pub fn new(
        registry: &'a EvaluatorRegistry,
        subquery_handler: Option<&'a dyn SubqueryHandler>,
    ) -> Self {
        Self {
            registry,
            subquery_handler,
            params: &[],
            column_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn with_params(
        registry: &'a EvaluatorRegistry,
        subquery_handler: Option<&'a dyn SubqueryHandler>,
        params: &'a [Value],
    ) -> Self {
        Self {
            registry,
            subquery_handler,
            params,
            column_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Вычислить выражение через подходящий evaluator
    pub async fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
        // Базовые случаи (всегда напрямую)
        match expr {
            Expr::Column(name) => {
                let idx = self.find_column_index_cached(schema, name).ok_or_else(|| {
                    crate::core::DbError::ColumnNotFound(name.clone(), "table".into())
                })?;
                return Ok(row[idx].clone());
            }
            Expr::CompoundIdentifier(parts) => {
                let name = parts.join(".");
                let idx = self
                    .find_column_index_cached(schema, &name)
                    .ok_or_else(|| {
                        crate::core::DbError::ColumnNotFound(name.clone(), "table".into())
                    })?;
                return Ok(row[idx].clone());
            }
            Expr::Literal(val) => {
                return Ok(val.clone());
            }
            Expr::Parameter(idx) => {
                // Parameters are 1-based in SQL ($1), but 0-based in array
                if *idx == 0 || *idx > self.params.len() {
                    return Err(crate::core::DbError::ExecutionError(format!(
                        "Parameter index out of range: ${}",
                        idx
                    )));
                }
                return Ok(self.params[*idx - 1].clone());
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

    fn find_column_index_cached(&self, schema: &Schema, name: &str) -> Option<usize> {
        let key = schema as *const Schema as usize;
        let mut cache = self
            .column_cache
            .lock()
            .expect("column cache mutex poisoned");
        let entry = cache
            .entry(key)
            .or_insert_with(|| build_column_index_cache(schema));
        entry.get(name).copied()
    }
}

fn build_column_index_cache(schema: &Schema) -> std::collections::HashMap<String, usize> {
    let mut exact = std::collections::HashMap::new();
    let mut suffix_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut suffix_index: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for (idx, col) in schema.columns().iter().enumerate() {
        exact.insert(col.name.clone(), idx);
        if let Some((_, suffix)) = col.name.rsplit_once('.') {
            *suffix_counts.entry(suffix.to_string()).or_insert(0) += 1;
            suffix_index.insert(suffix.to_string(), idx);
        }
    }

    for (suffix, count) in suffix_counts {
        if count == 1 {
            if let Some(idx) = suffix_index.get(&suffix) {
                exact.insert(suffix, *idx);
            }
        }
    }

    exact
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
        self.evaluators.push(evaluator);
    }

    /// Автоматическая регистрация всех встроенных evaluators
    pub fn with_default_evaluators() -> Self {
        use plugins::*;

        let mut registry = Self::new();

        // Автоматически регистрируем все evaluators
        registry.register(Box::new(subquery::SubqueryEvaluator)); // Register subquery evaluator
        registry.register(Box::new(boolean::BooleanEvaluator));
        registry.register(Box::new(comparison::ComparisonEvaluator));
        registry.register(Box::new(arithmetic::ArithmeticEvaluator));
        registry.register(Box::new(in_list::InListEvaluator));
        registry.register(Box::new(like::LikeEvaluator));
        registry.register(Box::new(between::BetweenEvaluator));
        registry.register(Box::new(is_null::IsNullEvaluator));
        registry.register(Box::new(json::JsonEvaluator));
        registry.register(Box::new(function::FunctionEvaluator));

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
