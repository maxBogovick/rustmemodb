use super::{ExpressionPlugin, ExpressionConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct BetweenPlugin;

impl ExpressionPlugin for BetweenPlugin {
    fn name(&self) -> &'static str {
        "BETWEEN"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(expr, sql_ast::Expr::Between { .. })
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(converter.convert(*expr)?),
                low: Box::new(converter.convert(*low)?),
                high: Box::new(converter.convert(*high)?),
                negated,
            }),
            _ => unreachable!("BetweenPlugin called with non-BETWEEN expression"),
        }
    }
}