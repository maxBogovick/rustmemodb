use super::{ExpressionPlugin, ExpressionConverter};
use crate::core::Result;
use crate::parser::ast::Expr;
use sqlparser::ast as sql_ast;

pub struct FunctionPlugin;

impl ExpressionPlugin for FunctionPlugin {
    fn name(&self) -> &'static str {
        "FUNCTION"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(expr, sql_ast::Expr::Function(_))
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();

                // Convert arguments
                let args = if let sql_ast::FunctionArguments::List(arg_list) = func.args {
                    arg_list.args
                        .into_iter()
                        .map(|arg| {
                            match arg {
                                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(e)) => {
                                    converter.convert(e)
                                }
                                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Wildcard) => {
                                    // For COUNT(*), AVG(*), etc.
                                    Ok(Expr::Literal(crate::core::Value::Text("*".into())))
                                }
                                _ => Err(crate::core::DbError::UnsupportedOperation(
                                    "Only unnamed expression arguments supported in functions".into()
                                )),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    Vec::new()
                };

                Ok(Expr::Function { name, args })
            }
            _ => unreachable!("FunctionPlugin called with non-function expression"),
        }
    }
}
