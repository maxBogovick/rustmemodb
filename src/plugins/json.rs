use super::{ExpressionConverter, ExpressionPlugin, QueryConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct JsonPlugin;

impl ExpressionPlugin for JsonPlugin {
    fn name(&self) -> &'static str {
        "JSON"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        if let sql_ast::Expr::BinaryOp { op, .. } = expr {
            matches!(
                op,
                sql_ast::BinaryOperator::Arrow | sql_ast::BinaryOperator::LongArrow
            )
        } else {
            false
        }
    }

    fn convert(
        &self,
        expr: sql_ast::Expr,
        converter: &ExpressionConverter,
        query_converter: &dyn QueryConverter,
    ) -> Result<Expr> {
        match expr {
            sql_ast::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(converter.convert(*left, query_converter)?),
                op: converter.convert_binary_op(&op)?,
                right: Box::new(converter.convert(*right, query_converter)?),
            }),
            _ => unreachable!("JsonPlugin called with non-json expression"),
        }
    }
}
