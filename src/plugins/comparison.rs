use super::{ExpressionConverter, ExpressionPlugin, QueryConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct ComparisonPlugin;

impl ExpressionPlugin for ComparisonPlugin {
    fn name(&self) -> &'static str {
        "COMPARISON"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        if let sql_ast::Expr::BinaryOp { op, .. } = expr {
            matches!(
                op,
                sql_ast::BinaryOperator::Eq
                    | sql_ast::BinaryOperator::NotEq
                    | sql_ast::BinaryOperator::Lt
                    | sql_ast::BinaryOperator::LtEq
                    | sql_ast::BinaryOperator::Gt
                    | sql_ast::BinaryOperator::GtEq
                    | sql_ast::BinaryOperator::And
                    | sql_ast::BinaryOperator::Or
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
            _ => unreachable!("ComparisonPlugin called with non-comparison expression"),
        }
    }
}
