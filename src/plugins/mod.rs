pub mod arithmetic;
pub mod between;
mod boolean;
pub mod comparison;
pub mod function;
pub mod in_list;
pub mod is_null;
pub mod json;
pub mod like;
pub mod nested;
pub mod subquery;

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
    fn convert(
        &self,
        expr: sql_ast::Expr,
        converter: &ExpressionConverter,
        query_converter: &dyn QueryConverter,
    ) -> Result<Expr>;
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
        registry.register(Box::new(json::JsonPlugin));

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
    pub fn convert(
        &self,
        expr: sql_ast::Expr,
        query_converter: &dyn QueryConverter,
    ) -> Result<Expr> {
        // Базовые случаи (всегда обрабатываются напрямую)
        match &expr {
            sql_ast::Expr::Identifier(ident) => {
                return Ok(Expr::Column(ident.value.clone()));
            }
            sql_ast::Expr::CompoundIdentifier(idents) => {
                let parts: Vec<String> = idents.iter().map(|i| i.value.clone()).collect();
                return Ok(Expr::CompoundIdentifier(parts));
            }
            sql_ast::Expr::Value(val_with_span) => {
                let val = &val_with_span.value;
                if let sql_ast::Value::Placeholder(s) = val {
                    // Handle $1, $2, etc.
                    let idx_str = s.trim_start_matches('$');
                    if let Ok(idx) = idx_str.parse::<usize>() {
                        return Ok(Expr::Parameter(idx));
                    }
                }
                return Ok(Expr::Literal(self.convert_value(val)?));
            }
            sql_ast::Expr::Subquery(query) => {
                let subquery = query_converter.convert_query(*query.clone())?;
                return Ok(Expr::Subquery(Box::new(subquery)));
            }
            sql_ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
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
            sql_ast::Expr::Cast {
                expr, data_type, ..
            } => {
                let left = self.convert(*expr.clone(), query_converter)?;
                let target_type = convert_sql_data_type(data_type)?;
                return Ok(Expr::Cast {
                    expr: Box::new(left),
                    data_type: target_type,
                });
            }
            sql_ast::Expr::Array(sql_ast::Array { elem, .. }) => {
                let list = elem
                    .iter()
                    .map(|e| self.convert(e.clone(), query_converter))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Expr::Array(list));
            }
            sql_ast::Expr::UnaryOp { op, expr } => {
                use crate::parser::ast::UnaryOp as AstUnary;
                if matches!(op, sql_ast::UnaryOperator::Not) {
                    return Ok(Expr::Not {
                        expr: Box::new(self.convert(*expr.clone(), query_converter)?),
                    });
                }
                let converted = self.convert(*expr.clone(), query_converter)?;
                let op = match op {
                    sql_ast::UnaryOperator::Minus => AstUnary::Minus,
                    sql_ast::UnaryOperator::Plus => AstUnary::Plus,
                    _ => {
                        return Err(crate::core::DbError::UnsupportedOperation(
                            "Unsupported unary operator".into(),
                        ));
                    }
                };
                return Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(converted),
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

            SqlOp::Arrow => Ok(BinaryOp::Arrow),
            SqlOp::LongArrow => Ok(BinaryOp::LongArrow),

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

fn convert_sql_data_type(dt: &sql_ast::DataType) -> Result<crate::core::DataType> {
    use crate::core::DataType;
    match dt {
        sql_ast::DataType::Int(_)
        | sql_ast::DataType::Integer(_)
        | sql_ast::DataType::BigInt(_) => Ok(DataType::Integer),
        sql_ast::DataType::Float(_) | sql_ast::DataType::Double(_) | sql_ast::DataType::Real => {
            Ok(DataType::Float)
        }
        sql_ast::DataType::Text
        | sql_ast::DataType::Varchar(_)
        | sql_ast::DataType::Char(_)
        | sql_ast::DataType::String(_) => Ok(DataType::Text),
        sql_ast::DataType::Boolean | sql_ast::DataType::Bool => Ok(DataType::Boolean),
        sql_ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
        sql_ast::DataType::Date => Ok(DataType::Date),
        sql_ast::DataType::Uuid => Ok(DataType::Uuid),
        sql_ast::DataType::JSON | sql_ast::DataType::JSONB => Ok(DataType::Json),
        sql_ast::DataType::Array(elem) => match elem {
            sql_ast::ArrayElemTypeDef::AngleBracket(inner)
            | sql_ast::ArrayElemTypeDef::SquareBracket(inner, _)
            | sql_ast::ArrayElemTypeDef::Parenthesis(inner) => {
                let t = convert_sql_data_type(inner)?;
                Ok(DataType::Array(Box::new(t)))
            }
            _ => Ok(DataType::Array(Box::new(DataType::Text))),
        },
        _ => Err(crate::core::DbError::TypeMismatch(format!(
            "Unsupported type: {:?}",
            dt
        ))),
    }
}
