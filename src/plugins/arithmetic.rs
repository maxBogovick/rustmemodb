use super::{ExpressionConverter, ExpressionPlugin, QueryConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct ArithmeticPlugin;

impl ExpressionPlugin for ArithmeticPlugin {
    fn name(&self) -> &'static str {
        "ARITHMETIC"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        if let sql_ast::Expr::BinaryOp { op, .. } = expr {
            matches!(
                op,
                sql_ast::BinaryOperator::Plus
                    | sql_ast::BinaryOperator::Minus
                    | sql_ast::BinaryOperator::Multiply
                    | sql_ast::BinaryOperator::Divide
                    | sql_ast::BinaryOperator::Modulo
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
            _ => unreachable!("ArithmeticPlugin called with non-arithmetic expression"),
        }
    }
}
