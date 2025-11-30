// ============================================================================
// src/parser/adapter.rs - ИСПРАВЛЕННЫЙ для актуальной версии sqlparser
// ============================================================================

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

    // ========================================================================
    // ИСПРАВЛЕННЫЙ convert_query с актуальным API sqlparser
    // ========================================================================
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

        // ✅ Парсинг ORDER BY через OrderBy struct
        let order_by = self.convert_order_by(query.order_by)?;

        // ✅ Парсинг LIMIT через limit_clause
        let limit = self.convert_limit_clause(&query.limit_clause)?;

        Ok(QueryStmt {
            projection,
            from,
            selection,
            order_by,
            limit,
        })
    }

    // ========================================================================
    // Конвертация ORDER BY из нового формата
    // ========================================================================
    fn convert_order_by(&self, order_by: Option<sql_ast::OrderBy>) -> Result<Vec<OrderByExpr>> {
        let Some(order_by) = order_by else {
            return Ok(Vec::new());
        };

        // OrderBy.kind содержит сами выражения
        match order_by.kind {
            sql_ast::OrderByKind::Expressions(exprs) => {
                exprs
                    .into_iter()
                    .map(|expr| self.convert_order_by_expr(expr))
                    .collect()
            }
            sql_ast::OrderByKind::All(all) => {
                // ORDER BY ALL - сортировка по всем колонкам
                Err(DbError::UnsupportedOperation(
                    format!("ORDER BY ALL not supported: {:?}", all)
                ))
            }
        }
    }

    // ========================================================================
    // Конвертация одного ORDER BY выражения
    // ========================================================================
    fn convert_order_by_expr(&self, order: sql_ast::OrderByExpr) -> Result<OrderByExpr> {
        let expr = self.expr_converter.convert(order.expr)?;

        // ASC по умолчанию, DESC если явно указано
        // order.asc: Option<bool> - Some(true) = ASC, Some(false) = DESC, None = default (ASC)
        let descending = order.options.asc.map(|asc| !asc).unwrap_or(false);

        Ok(OrderByExpr {
            expr,
            descending,
        })
    }

    // ========================================================================
    // ИСПРАВЛЕННЫЙ: Конвертация LIMIT через LimitClause с ValueWithSpan
    // ========================================================================
    fn convert_limit_clause(&self, limit_clause: &Option<sql_ast::LimitClause>) -> Result<Option<usize>> {
        let Some(clause) = limit_clause else {
            return Ok(None);
        };

        match clause {
            sql_ast::LimitClause::LimitOffset { limit, .. } => {
                // LIMIT expr [OFFSET expr]
                match limit {
                    Some(sql_ast::Expr::Value(value_with_span)) => {
                        // ✅ ИСПРАВЛЕНО: ValueWithSpan содержит value поле
                        self.extract_limit_number(&value_with_span.value)
                    }
                    Some(_) => Err(DbError::UnsupportedOperation(
                        "Only numeric LIMIT supported".into()
                    )),
                    None => Ok(None),
                }
            }
            sql_ast::LimitClause::OffsetCommaLimit { limit, .. } => {
                // MySQL style: LIMIT offset, limit
                match limit {
                    sql_ast::Expr::Value(value_with_span) => {
                        // ✅ ИСПРАВЛЕНО: ValueWithSpan содержит value поле
                        self.extract_limit_number(&value_with_span.value)
                    }
                    _ => Err(DbError::UnsupportedOperation(
                        "Only numeric LIMIT supported".into()
                    )),
                }
            }
        }
    }

    // ========================================================================
    // Вспомогательный метод для извлечения числа из Value
    // ========================================================================
    fn extract_limit_number(&self, value: &sql_ast::Value) -> Result<Option<usize>> {
        match value {
            sql_ast::Value::Number(n, _) => {
                n.parse::<usize>()
                    .map(Some)
                    .map_err(|_| DbError::ParseError(
                        format!("Invalid LIMIT value: {}", n)
                    ))
            }
            _ => Err(DbError::UnsupportedOperation(
                format!("Only numeric LIMIT supported, got: {:?}", value)
            )),
        }
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

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_by_asc() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse("SELECT * FROM users ORDER BY age").unwrap();

        let Statement::Query(query) = &stmts[0] else {
            panic!("Expected Query");
        };

        assert_eq!(query.order_by.len(), 1);
        assert!(!query.order_by[0].descending);

        if let Expr::Column(name) = &query.order_by[0].expr {
            assert_eq!(name, "age");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_parse_order_by_desc() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse("SELECT * FROM users ORDER BY age DESC").unwrap();

        let Statement::Query(query) = &stmts[0] else {
            panic!("Expected Query");
        };

        assert_eq!(query.order_by.len(), 1);
        assert!(query.order_by[0].descending);
    }

    #[test]
    fn test_parse_multiple_order_by() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse(
            "SELECT * FROM users ORDER BY age DESC, name ASC"
        ).unwrap();

        let Statement::Query(query) = &stmts[0] else {
            panic!("Expected Query");
        };

        assert_eq!(query.order_by.len(), 2);
        assert!(query.order_by[0].descending);  // age DESC
        assert!(!query.order_by[1].descending); // name ASC
    }

    #[test]
    fn test_parse_order_by_with_limit() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse(
            "SELECT * FROM users ORDER BY age LIMIT 10"
        ).unwrap();

        let Statement::Query(query) = &stmts[0] else {
            panic!("Expected Query");
        };

        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_parse_order_by_expression() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse(
            "SELECT * FROM users ORDER BY age + 1 DESC"
        ).unwrap();

        let Statement::Query(query) = &stmts[0] else {
            panic!("Expected Query");
        };

        assert_eq!(query.order_by.len(), 1);
        assert!(query.order_by[0].descending);

        // Проверяем что это BinaryOp выражение
        assert!(matches!(query.order_by[0].expr, Expr::BinaryOp { .. }));
    }

    #[test]
    fn test_parse_no_order_by() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse("SELECT * FROM users").unwrap();

        let Statement::Query(query) = &stmts[0] else {
            panic!("Expected Query");
        };

        assert!(query.order_by.is_empty());
        assert!(query.limit.is_none());
    }
}