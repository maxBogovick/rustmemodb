use super::{ExpressionPlugin, ExpressionConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct IsNullPlugin;

impl ExpressionPlugin for IsNullPlugin {
    fn name(&self) -> &'static str {
        "IS NULL"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(expr, sql_ast::Expr::IsNull(_) | sql_ast::Expr::IsNotNull(_))
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::IsNull(e) => Ok(Expr::IsNull {
                expr: Box::new(converter.convert(*e)?),
                negated: false,
            }),
            sql_ast::Expr::IsNotNull(e) => Ok(Expr::IsNull {
                expr: Box::new(converter.convert(*e)?),
                negated: true,
            }),
            _ => unreachable!("IsNullPlugin called with wrong expression"),
        }
    }
}