pub mod arithmetic;
pub mod between;
pub mod comparison;
pub mod in_list;
pub mod is_null;
pub mod like;
pub mod nested;
pub mod function;
pub mod subquery;
mod boolean;

use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

/// Trait for converting subqueries (to avoid circular dependency)
pub trait QueryConverter {
    fn convert_query(&self, query: sql_ast::Query) -> Result<crate::parser::ast::QueryStmt>;
}

/// Трейт для конвертации SQL выражения в наш AST
pub trait ExpressionPlugin: Send + Sync {
    /// Имя плагина для отладки
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Может ли плагин обработать это выражение?
    fn can_handle(&self, expr: &sql_ast::Expr) -> bool;

    /// Конвертировать SQL выражение в наш Expr
    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter, query_converter: &dyn QueryConverter) -> Result<Expr>;
}

/// Реестр плагинов для выражений
pub struct ExpressionPluginRegistry {
    plugins: Vec<Box<dyn ExpressionPlugin>>,
}

impl ExpressionPluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    /// Зарегистрировать плагин
    pub fn register(&mut self, plugin: Box<dyn ExpressionPlugin>) {
        self.plugins.push(plugin);
    }

    /// Автоматическая регистрация всех встроенных плагинов
    pub fn with_default_plugins() -> Self {
        let mut registry = Self::new();

        // Автоматически регистрируем все плагины
        registry.register(Box::new(subquery::SubqueryPlugin)); // Check subqueries first
        registry.register(Box::new(nested::NestedPlugin));
        registry.register(Box::new(function::FunctionPlugin));
        registry.register(Box::new(like::LikePlugin));
        registry.register(Box::new(between::BetweenPlugin));
        registry.register(Box::new(is_null::IsNullPlugin));
        registry.register(Box::new(arithmetic::ArithmeticPlugin));
        registry.register(Box::new(comparison::ComparisonPlugin));
        registry.register(Box::new(in_list::InListPlugin));
        registry.register(Box::new(boolean::BooleanPlugin));

        registry
    }

    /// Найти подходящий плагин для выражения
    pub fn find_plugin(&self, expr: &sql_ast::Expr) -> Option<&dyn ExpressionPlugin> {
        self.plugins
            .iter()
            .find(|plugin| plugin.can_handle(expr))
            .map(|boxed| &**boxed)
    }
}

impl Default for ExpressionPluginRegistry {
    fn default() -> Self {
        Self::with_default_plugins()
    }
}

/// Конвертер выражений с поддержкой плагинов
pub struct ExpressionConverter {
    registry: ExpressionPluginRegistry,
}

impl ExpressionConverter {
    pub fn new() -> Self {
        Self {
            registry: ExpressionPluginRegistry::with_default_plugins(),
        }
    }

    #[allow(dead_code)]
    pub fn with_custom_plugins(registry: ExpressionPluginRegistry) -> Self {
        Self { registry }
    }

    /// Конвертировать выражение используя плагины
    pub fn convert(&self, expr: sql_ast::Expr, query_converter: &dyn QueryConverter) -> Result<Expr> {
        // Базовые случаи (всегда обрабатываются напрямую)
        match &expr {
            sql_ast::Expr::Identifier(ident) => {
                return Ok(Expr::Column(ident.value.clone()));
            }
            sql_ast::Expr::CompoundIdentifier(idents) => {
                let parts: Vec<String> = idents.iter().map(|i| i.value.clone()).collect();
                return Ok(Expr::CompoundIdentifier(parts));
            }
            sql_ast::Expr::Value(val) => {
                return Ok(Expr::Literal(self.convert_value(&val.value)?));
            }
            sql_ast::Expr::Subquery(query) => {
                let subquery = query_converter.convert_query(*query.clone())?;
                return Ok(Expr::Subquery(Box::new(subquery)));
            }
            sql_ast::Expr::InSubquery { expr, subquery, negated } => {
                let left = self.convert(*expr.clone(), query_converter)?;
                let sub = query_converter.convert_query(*subquery.clone())?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(sub),
                    negated: *negated,
                });
            }
            sql_ast::Expr::Exists { subquery, negated } => {
                let sub = query_converter.convert_query(*subquery.clone())?;
                return Ok(Expr::Exists {
                    subquery: Box::new(sub),
                    negated: *negated,
                });
            }
            _ => {}
        }

        // Попытка обработать через плагины
        if let Some(plugin) = self.registry.find_plugin(&expr) {
            return plugin.convert(expr, self, query_converter);
        }

        // Не найдено подходящего плагина
        Err(crate::core::DbError::UnsupportedOperation(format!(
            "No plugin found for expression: {:?}",
            expr
        )))
    }

    /// Helper для конвертации значений
    pub fn convert_value(&self, val: &sql_ast::Value) -> Result<crate::core::Value> {
        use crate::core::Value;

        match val {
            sql_ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Err(crate::core::DbError::TypeMismatch(format!(
                        "Invalid number: {}",
                        n
                    )))
                }
            }
            sql_ast::Value::SingleQuotedString(s) | sql_ast::Value::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            }
            sql_ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
            sql_ast::Value::Null => Ok(Value::Null),
            _ => Err(crate::core::DbError::UnsupportedOperation(format!(
                "Unsupported value: {:?}",
                val
            ))),
        }
    }

    /// Helper для конвертации бинарных операторов
    pub fn convert_binary_op(
        &self,
        op: &sql_ast::BinaryOperator,
    ) -> Result<crate::parser::ast::BinaryOp> {
        use crate::parser::ast::BinaryOp;
        use sql_ast::BinaryOperator as SqlOp;

        match op {
            SqlOp::Plus => Ok(BinaryOp::Add),
            SqlOp::Minus => Ok(BinaryOp::Subtract),
            SqlOp::Multiply => Ok(BinaryOp::Multiply),
            SqlOp::Divide => Ok(BinaryOp::Divide),
            SqlOp::Modulo => Ok(BinaryOp::Modulo),

            SqlOp::Eq => Ok(BinaryOp::Eq),
            SqlOp::NotEq => Ok(BinaryOp::NotEq),
            SqlOp::Lt => Ok(BinaryOp::Lt),
            SqlOp::LtEq => Ok(BinaryOp::LtEq),
            SqlOp::Gt => Ok(BinaryOp::Gt),
            SqlOp::GtEq => Ok(BinaryOp::GtEq),

            SqlOp::And => Ok(BinaryOp::And),
            SqlOp::Or => Ok(BinaryOp::Or),

            _ => Err(crate::core::DbError::UnsupportedOperation(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }
}

impl Default for ExpressionConverter {
    fn default() -> Self {
        Self::new()
    }
}
