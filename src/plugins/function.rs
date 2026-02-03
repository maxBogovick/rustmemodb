use super::{ExpressionPlugin, ExpressionConverter, QueryConverter};
use crate::core::Result;
use crate::parser::ast::{Expr, WindowSpec, OrderByExpr};
use sqlparser::ast as sql_ast;

pub struct FunctionPlugin;

impl ExpressionPlugin for FunctionPlugin {
    fn name(&self) -> &'static str {
        "FUNCTION"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        matches!(expr, sql_ast::Expr::Function(_))
    }

    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter, query_converter: &dyn QueryConverter) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();

                // Convert arguments
                let (args, distinct) = if let sql_ast::FunctionArguments::List(arg_list) = func.args {
                    let distinct = matches!(arg_list.duplicate_treatment, Some(sql_ast::DuplicateTreatment::Distinct));
                    
                    let args = arg_list.args
                        .into_iter()
                        .map(|arg| {
                            match arg {
                                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(e)) => {
                                    converter.convert(e, query_converter)
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
                        .collect::<Result<Vec<_>>>()?;
                    (args, distinct)
                } else {
                    (Vec::new(), false)
                };

                let over = if let Some(sql_ast::WindowType::WindowSpec(spec)) = func.over {
                    let partition_by = spec.partition_by.into_iter()
                        .map(|e| converter.convert(e, query_converter))
                        .collect::<Result<Vec<_>>>()?;
                    
                    let order_by = spec.order_by.into_iter()
                        .map(|o| {
                            let expr = converter.convert(o.expr, query_converter)?;
                            let descending = o.options.asc.map(|a| !a).unwrap_or(false);
                            Ok(OrderByExpr { expr, descending })
                        })
                        .collect::<Result<Vec<_>>>()?;
                        
                    Some(WindowSpec { partition_by, order_by })
                } else {
                    None
                };

                Ok(Expr::Function { name, args, distinct, over })
            }
            _ => unreachable!("FunctionPlugin called with non-function expression"),
        }
    }
}
