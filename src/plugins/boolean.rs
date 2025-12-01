use super::{ExpressionPlugin, ExpressionConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct BooleanPlugin;

impl ExpressionPlugin for BooleanPlugin {
    fn name(&self) -> &'static str {
        "BOOLEAN"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        match expr {
            // Handle AND/OR binary operations
            sql_ast::Expr::BinaryOp { op, .. } => {
                matches!(
                    op,
                    sql_ast::BinaryOperator::And | sql_ast::BinaryOperator::Or
                )
            }
            // Handle NOT unary operation
            sql_ast::Expr::UnaryOp { op, .. } => {
                matches!(op, sql_ast::UnaryOperator::Not)
            }
            _ => false,
        }
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter) -> Result<Expr> {
        match expr {
            // Handle AND/OR
            sql_ast::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(converter.convert(*left)?),
                op: converter.convert_binary_op(&op)?,
                right: Box::new(converter.convert(*right)?),
            }),
            // Handle NOT
            sql_ast::Expr::UnaryOp { op, expr } => {
                match op {
                    sql_ast::UnaryOperator::Not => Ok(Expr::Not {
                        expr: Box::new(converter.convert(*expr)?),
                    }),
                    _ => unreachable!("BooleanPlugin called with non-NOT unary operator"),
                }
            }
            _ => unreachable!("BooleanPlugin called with non-boolean expression"),
        }
    }
}