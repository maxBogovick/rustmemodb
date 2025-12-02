use super::{ExpressionPlugin, ExpressionConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct NestedPlugin;

impl ExpressionPlugin for NestedPlugin {
    fn name(&self) -> &'static str {
        "NESTED"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(expr, sql_ast::Expr::Nested(_))
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Nested(inner) => {
                // Recursively convert the nested expression
                converter.convert(*inner)
            }
            _ => unreachable!("NestedPlugin called with non-nested expression"),
        }
    }
}
