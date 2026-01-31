use crate::core::Result;
use crate::parser::ast::Expr;
use crate::plugins::{ExpressionConverter, ExpressionPlugin, QueryConverter};
use sqlparser::ast as sql_ast;

pub struct SubqueryPlugin;

impl ExpressionPlugin for SubqueryPlugin {
    fn name(&self) -> &'static str {
        "SUBQUERY"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(
            expr,
            sql_ast::Expr::Subquery(_)
                | sql_ast::Expr::InSubquery { .. }
                | sql_ast::Expr::Exists { .. }
        )
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter, query_converter: &dyn QueryConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Subquery(query) => {
                let subquery = query_converter.convert_query(*query)?;
                Ok(Expr::Subquery(Box::new(subquery)))
            }
            sql_ast::Expr::InSubquery { expr, subquery, negated } => {
                let left = converter.convert(*expr, query_converter)?;
                let sub = query_converter.convert_query(*subquery)?;
                Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(sub),
                    negated,
                })
            }
            sql_ast::Expr::Exists { subquery, negated } => {
                let sub = query_converter.convert_query(*subquery)?;
                Ok(Expr::Exists {
                    subquery: Box::new(sub),
                    negated,
                })
            }
            _ => unreachable!("SubqueryPlugin called with non-subquery expression"),
        }
    }
}
