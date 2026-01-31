use super::{ExpressionConverter, ExpressionPlugin, QueryConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct LikePlugin;

impl ExpressionPlugin for LikePlugin {
    fn name(&self) -> &'static str {
        "LIKE"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(
            expr,
            sql_ast::Expr::Like { .. } | sql_ast::Expr::ILike { .. }
        )
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter, query_converter: &dyn QueryConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                if escape_char.is_some() {
                    return Err(crate::core::DbError::UnsupportedOperation(
                        "LIKE ESCAPE not supported".into(),
                    ));
                }

                Ok(Expr::Like {
                    expr: Box::new(converter.convert(*expr, query_converter)?),
                    pattern: Box::new(converter.convert(*pattern, query_converter)?),
                    negated,
                    case_insensitive: false,
                })
            }

            sql_ast::Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                if escape_char.is_some() {
                    return Err(crate::core::DbError::UnsupportedOperation(
                        "ILIKE ESCAPE not supported".into(),
                    ));
                }

                Ok(Expr::Like {
                    expr: Box::new(converter.convert(*expr, query_converter)?),
                    pattern: Box::new(converter.convert(*pattern, query_converter)?),
                    negated,
                    case_insensitive: true,
                })
            }

            _ => unreachable!("LikePlugin called with non-LIKE expression"),
        }
    }
}
