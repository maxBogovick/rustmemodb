use sqlparser::ast as sql_ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use crate::core::{DbError, Result, DataType};
use crate::parser::ast::*;
use crate::plugins::ExpressionConverter;

pub struct SqlParserAdapter {
    dialect: PostgreSqlDialect,
    expr_converter: ExpressionConverter,
}

impl SqlParserAdapter {
    pub fn new() -> Self {
        Self {
            dialect: PostgreSqlDialect {},
            expr_converter: ExpressionConverter::new(),
        }
    }

    /// Создать адаптер с кастомными плагинами
    pub fn with_expression_converter(expr_converter: ExpressionConverter) -> Self {
        Self {
            dialect: PostgreSqlDialect {},
            expr_converter,
        }
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        let external_stmts = Parser::parse_sql(&self.dialect, sql)
            .map_err(|e| DbError::ParseError(e.to_string()))?;

        external_stmts
            .into_iter()
            .map(|stmt| self.convert_statement(stmt))
            .collect()
    }

    fn convert_statement(&self, stmt: sql_ast::Statement) -> Result<Statement> {
        match stmt {
            sql_ast::Statement::CreateTable(create) => {
                Ok(Statement::CreateTable(self.convert_create_table(create)?))
            }
            sql_ast::Statement::Insert(insert) => {
                Ok(Statement::Insert(self.convert_insert(insert)?))
            }
            sql_ast::Statement::Query(query) => {
                Ok(Statement::Query(self.convert_query(*query)?))
            }
            _ => Err(DbError::UnsupportedOperation(format!(
                "Statement type not supported: {:?}",
                stmt
            ))),
        }
    }

    fn convert_create_table(&self, create: sql_ast::CreateTable) -> Result<CreateTableStmt> {
        let table_name = extract_table_name(&create.name)?;
        let columns = create
            .columns
            .into_iter()
            .map(|col| self.convert_column_def(col))
            .collect::<Result<Vec<_>>>()?;

        Ok(CreateTableStmt {
            table_name,
            columns,
            if_not_exists: create.if_not_exists,
        })
    }

    fn convert_column_def(&self, col: sql_ast::ColumnDef) -> Result<ColumnDef> {
        let data_type = self.convert_data_type(&col.data_type)?;
        let nullable = !col
            .options
            .iter()
            .any(|opt| matches!(opt.option, sql_ast::ColumnOption::NotNull));

        Ok(ColumnDef {
            name: col.name.value,
            data_type,
            nullable,
            default: None,
        })
    }

    fn convert_data_type(&self, dt: &sql_ast::DataType) -> Result<DataType> {
        match dt {
            sql_ast::DataType::Int(_)
            | sql_ast::DataType::Integer(_)
            | sql_ast::DataType::BigInt(_) => Ok(DataType::Integer),

            sql_ast::DataType::Float(_)
            | sql_ast::DataType::Double(_)
            | sql_ast::DataType::Real => Ok(DataType::Float),

            sql_ast::DataType::Text
            | sql_ast::DataType::Varchar(_)
            | sql_ast::DataType::Char(_)
            | sql_ast::DataType::String(_) => Ok(DataType::Text),

            sql_ast::DataType::Boolean
            | sql_ast::DataType::Bool => Ok(DataType::Boolean),

            _ => Err(DbError::TypeMismatch(format!(
                "Unsupported data type: {:?}",
                dt
            ))),
        }
    }

    fn convert_insert(&self, insert: sql_ast::Insert) -> Result<InsertStmt> {
        let table_name = insert.table.to_string();

        let columns = if insert.columns.is_empty() {
            None
        } else {
            Some(insert.columns.into_iter().map(|id| id.value).collect())
        };

        let values = if let Some(source) = insert.source {
            if let sql_ast::SetExpr::Values(vals) = *source.body {
                vals.rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| self.expr_converter.convert(expr))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                return Err(DbError::UnsupportedOperation(
                    "Only VALUES clause supported".into()
                ));
            }
        } else {
            Vec::new()
        };

        Ok(InsertStmt {
            table_name,
            columns,
            values,
        })
    }

    fn convert_query(&self, query: sql_ast::Query) -> Result<QueryStmt> {
        let sql_ast::SetExpr::Select(select) = *query.body else {
            return Err(DbError::UnsupportedOperation(
                "Only SELECT queries supported".into()
            ));
        };

        let projection = select
            .projection
            .into_iter()
            .map(|item| self.convert_select_item(item))
            .collect::<Result<Vec<_>>>()?;

        let from = select
            .from
            .into_iter()
            .map(|table| self.convert_table_ref(table))
            .collect::<Result<Vec<_>>>()?;

        let selection = select
            .selection
            .map(|expr| self.expr_converter.convert(expr))
            .transpose()?;

        Ok(QueryStmt {
            projection,
            from,
            selection,
            order_by: Vec::new(),
            limit: None,
        })
    }

    fn convert_select_item(&self, item: sql_ast::SelectItem) -> Result<SelectItem> {
        match item {
            sql_ast::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
            sql_ast::SelectItem::UnnamedExpr(expr) => {
                Ok(SelectItem::Expr {
                    expr: self.expr_converter.convert(expr)?,
                    alias: None,
                })
            }
            sql_ast::SelectItem::ExprWithAlias { expr, alias } => {
                Ok(SelectItem::Expr {
                    expr: self.expr_converter.convert(expr)?,
                    alias: Some(alias.value),
                })
            }
            _ => Err(DbError::UnsupportedOperation(
                "Unsupported select item".into()
            )),
        }
    }

    fn convert_table_ref(&self, table: sql_ast::TableWithJoins) -> Result<TableRef> {
        match table.relation {
            sql_ast::TableFactor::Table { name, alias, .. } => {
                let table_name = extract_table_name(&name)?;
                let table_alias = alias.map(|a| a.name.value);
                Ok(TableRef {
                    name: table_name,
                    alias: table_alias,
                })
            }
            _ => Err(DbError::UnsupportedOperation(
                "Complex table references not supported".into()
            )),
        }
    }
}

impl Default for SqlParserAdapter {
    fn default() -> Self {
        Self::new()
    }
}

fn extract_table_name(name: &sql_ast::ObjectName) -> Result<String> {
    name.0
        .last()
        .map(|ident| ident.to_string())
        .ok_or_else(|| DbError::ParseError("Invalid table name".into()))
}