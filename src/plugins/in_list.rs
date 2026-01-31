use super::{ExpressionPlugin, ExpressionConverter, QueryConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

/// Плагин для IN (...) выражений
/// Разработчик пишет это в отдельном файле
pub struct InListPlugin;

impl ExpressionPlugin for InListPlugin {
    fn name(&self) -> &'static str {
        "IN"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(expr, sql_ast::Expr::InList { .. })
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter, query_converter: &dyn QueryConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let converted_list = list
                    .into_iter()
                    .map(|e| converter.convert(e, query_converter))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Expr::In {
                    expr: Box::new(converter.convert(*expr, query_converter)?),
                    list: converted_list,
                    negated,
                })
            }
            _ => unreachable!("InListPlugin called with non-IN expression"),
        }
    }
}