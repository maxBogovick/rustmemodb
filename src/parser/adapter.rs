// ============================================================================
// src/parser/adapter.rs - Updated for Subquery Support
// ============================================================================

use sqlparser::ast as sql_ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use crate::core::{DbError, Result, DataType, ForeignKey};
use crate::parser::ast::*;
use crate::plugins::ExpressionConverter;

pub struct SqlParserAdapter {
    dialect: PostgreSqlDialect,
    expr_converter: ExpressionConverter,
}

impl crate::plugins::QueryConverter for SqlParserAdapter {
    fn convert_query(&self, query: sql_ast::Query) -> Result<QueryStmt> {
        self.convert_query(query)
    }
}

impl SqlParserAdapter {
    pub fn new() -> Self {
        Self {
            dialect: PostgreSqlDialect {},
            expr_converter: ExpressionConverter::new(),
        }
    }

    /// Создать адаптер с кастомными плагинами
    #[allow(dead_code)]
    pub fn with_expression_converter(expr_converter: ExpressionConverter) -> Self {
        Self {
            dialect: PostgreSqlDialect {},
            expr_converter,
        }
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        // Handle transaction control statements that sqlparser might not parse
        let trimmed = sql.trim().to_uppercase();
        if trimmed == "BEGIN" || trimmed == "BEGIN TRANSACTION" || trimmed == "START TRANSACTION" {
            return Ok(vec![Statement::Begin]);
        }
        if trimmed == "COMMIT" || trimmed == "COMMIT TRANSACTION" {
            return Ok(vec![Statement::Commit]);
        }
        if trimmed == "ROLLBACK" || trimmed == "ROLLBACK TRANSACTION" {
            return Ok(vec![Statement::Rollback]);
        }

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
            sql_ast::Statement::CreateIndex(ci) => {
                Ok(Statement::CreateIndex(self.convert_create_index(&ci)?))
            }
            sql_ast::Statement::AlterTable { name, operations, .. } => {
                if operations.len() != 1 {
                    return Err(DbError::UnsupportedOperation("Only single ALTER TABLE operation supported".into()));
                }
                Ok(Statement::AlterTable(self.convert_alter_table(name, operations.into_iter().next().unwrap())?))
            }
            sql_ast::Statement::CreateView { name, query, or_replace, .. } => {
                let view_name = extract_table_name(&name)?;
                let query_stmt = self.convert_query(*query)?;
                Ok(Statement::CreateView(CreateViewStmt {
                    name: view_name,
                    query: Box::new(query_stmt),
                    or_replace,
                }))
            }
            sql_ast::Statement::Drop { object_type, names, if_exists, .. } => {
                match object_type {
                    sql_ast::ObjectType::Table => {
                        Ok(Statement::DropTable(self.convert_drop_table(names, if_exists)?))
                    }
                    sql_ast::ObjectType::View => {
                        if names.len() != 1 {
                             return Err(DbError::UnsupportedOperation("Only single view DROP supported".into()));
                        }
                        let view_name = extract_table_name(&names[0])?;
                        Ok(Statement::DropView(DropViewStmt {
                            name: view_name,
                            if_exists,
                        }))
                    }
                    _ => Err(DbError::UnsupportedOperation(format!(
                        "Only DROP TABLE and DROP VIEW supported, got: {:?}",
                        object_type
                    )))
                }
            }
            sql_ast::Statement::Insert(insert) => {
                Ok(Statement::Insert(self.convert_insert(insert)?))
            }
            sql_ast::Statement::Query(query) => {
                Ok(Statement::Query(self.convert_query(*query)?))
            }
            sql_ast::Statement::Delete(delete) => {
                Ok(Statement::Delete(self.convert_delete(delete)?))
            }
            sql_ast::Statement::Update { table, assignments, selection, .. } => {
                Ok(Statement::Update(self.convert_update(table, assignments, selection)?))
            }
            sql_ast::Statement::Explain { statement, analyze, .. } => {
                Ok(Statement::Explain(ExplainStmt {
                    statement: Box::new(self.convert_statement(*statement)?),
                    analyze,
                }))
            }
            _ => Err(DbError::UnsupportedOperation(format!(
                "Statement type not supported: {:?}",
                stmt
            ))),
        }
    }

    fn convert_create_table(&self, create: sql_ast::CreateTable) -> Result<CreateTableStmt> {
        let table_name = extract_table_name(&create.name)?;
        let mut columns = create
            .columns
            .into_iter()
            .map(|col| self.convert_column_def(col))
            .collect::<Result<Vec<_>>>()?;

        for constraint in create.constraints {
            match constraint {
                sql_ast::TableConstraint::ForeignKey { columns: cols, foreign_table, referred_columns, .. } => {
                    if cols.len() != 1 || referred_columns.len() != 1 {
                        return Err(DbError::UnsupportedOperation("Composite foreign keys not supported yet".into()));
                    }
                    
                    let col_name = cols[0].value.clone();
                    let ref_table = extract_table_name(&foreign_table)?;
                    let ref_col = referred_columns[0].value.clone();

                    if let Some(column) = columns.iter_mut().find(|c| c.name == col_name) {
                        column.references = Some(ForeignKey {
                            table: ref_table,
                            column: ref_col,
                        });
                    } else {
                        return Err(DbError::ColumnNotFound(col_name, table_name.clone()));
                    }
                }
                _ => {}
            }
        }

        Ok(CreateTableStmt {
            table_name,
            columns,
            if_not_exists: create.if_not_exists,
        })
    }

    fn convert_drop_table(&self, names: Vec<sql_ast::ObjectName>, if_exists: bool) -> Result<DropTableStmt> {
        if names.len() != 1 {
            return Err(DbError::UnsupportedOperation(
                "Only single table DROP supported".into()
            ));
        }

        let table_name = extract_table_name(&names[0])?;

        Ok(DropTableStmt {
            table_name,
            if_exists,
        })
    }

    fn convert_delete(&self, delete: sql_ast::Delete) -> Result<DeleteStmt> {
        let table_name = match delete.from {
            sql_ast::FromTable::WithFromKeyword(tables) => {
                if tables.is_empty() {
                    return Err(DbError::ParseError("DELETE requires FROM clause".into()));
                }
                match &tables[0].relation {
                    sql_ast::TableFactor::Table { name, .. } => extract_table_name(name)?,
                    _ => return Err(DbError::UnsupportedOperation(
                        "Complex table references not supported in DELETE".into()
                    )),
                }
            }
            sql_ast::FromTable::WithoutKeyword(tables) => {
                if tables.is_empty() {
                    return Err(DbError::ParseError("DELETE requires table name".into()));
                }
                match &tables[0].relation {
                    sql_ast::TableFactor::Table { name, .. } => extract_table_name(name)?,
                    _ => return Err(DbError::UnsupportedOperation(
                        "Complex table references not supported in DELETE".into()
                    )),
                }
            }
        };

        let selection = delete
            .selection
            .map(|expr| self.expr_converter.convert(expr, self))
            .transpose()?;

        Ok(DeleteStmt {
            table_name,
            selection,
        })
    }

    fn convert_update(
        &self,
        table: sql_ast::TableWithJoins,
        assignments: Vec<sql_ast::Assignment>,
        selection: Option<sql_ast::Expr>,
    ) -> Result<UpdateStmt> {
        let table_name = match table.relation {
            sql_ast::TableFactor::Table { name, .. } => extract_table_name(&name)?,
            _ => return Err(DbError::UnsupportedOperation(
                "Complex table references not supported in UPDATE".into()
            )),
        };

        let assignments = assignments
            .into_iter()
            .map(|assign| {
                let column = match assign.target {
                    sql_ast::AssignmentTarget::ColumnName(col_name) => {
                        if col_name.0.len() == 1 {
                            col_name.0[0].to_string()
                        } else {
                            return Err(DbError::UnsupportedOperation(
                                "Qualified column names not supported in UPDATE".into()
                            ));
                        }
                    }
                    _ => return Err(DbError::UnsupportedOperation(
                        "Only simple column names supported in UPDATE".into()
                    )),
                };

                let value = self.expr_converter.convert(assign.value, self)?;

                Ok(Assignment { column, value })
            })
            .collect::<Result<Vec<_>>>()?;

        let selection = selection
            .map(|expr| self.expr_converter.convert(expr, self))
            .transpose()?;

        Ok(UpdateStmt {
            table_name,
            assignments,
            selection,
        })
    }

    fn convert_column_def(&self, col: sql_ast::ColumnDef) -> Result<ColumnDef> {
        let data_type = self.convert_data_type(&col.data_type)?;
        let mut nullable = true;
        let mut primary_key = false;
        let mut unique = false;
        let mut references = None;

        for opt in &col.options {
            match &opt.option {
                sql_ast::ColumnOption::NotNull => nullable = false,
                sql_ast::ColumnOption::Unique { is_primary: true, .. } => {
                    primary_key = true;
                    nullable = false;
                    unique = true;
                }
                sql_ast::ColumnOption::Unique { is_primary: false, .. } => unique = true,
                sql_ast::ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                    let table = extract_table_name(foreign_table)?;
                    if referred_columns.len() != 1 {
                        return Err(DbError::UnsupportedOperation("Composite FKs not supported in column definition".into()));
                    }
                    let column = referred_columns[0].value.clone();
                    references = Some(ForeignKey { table, column });
                }
                _ => {}
            }
        }

        Ok(ColumnDef {
            name: col.name.value,
            data_type,
            nullable,
            default: None,
            primary_key,
            unique,
            references,
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

            sql_ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            sql_ast::DataType::Date => Ok(DataType::Date),
            sql_ast::DataType::Uuid => Ok(DataType::Uuid),

            sql_ast::DataType::Array(elem_def) => {
                match elem_def {
                    sql_ast::ArrayElemTypeDef::AngleBracket(inner) => {
                        let inner_type = self.convert_data_type(inner)?;
                        Ok(DataType::Array(Box::new(inner_type)))
                    }
                    sql_ast::ArrayElemTypeDef::SquareBracket(inner, _size) => {
                        let inner_type = self.convert_data_type(inner)?;
                        Ok(DataType::Array(Box::new(inner_type)))
                    }
                    sql_ast::ArrayElemTypeDef::Parenthesis(inner) => {
                        let inner_type = self.convert_data_type(inner)?;
                        Ok(DataType::Array(Box::new(inner_type)))
                    }
                    sql_ast::ArrayElemTypeDef::None => {
                        // Default to array of text if type not specified? Or error?
                        // Postgres allows just ARRAY keyword in some contexts, but usually with type.
                        // Let's assume Text for now or error.
                        Ok(DataType::Array(Box::new(DataType::Text)))
                    }
                }
            }

            sql_ast::DataType::JSON | sql_ast::DataType::JSONB => Ok(DataType::Json),

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
                            .map(|expr| self.expr_converter.convert(expr, self))
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
        let with = if let Some(with) = query.with {
            Some(self.convert_with(with)?)
        } else {
            None
        };

        let sql_ast::SetExpr::Select(select) = *query.body else {
            return Err(DbError::UnsupportedOperation(
                "Only SELECT queries supported".into()
            ));
        };

        let distinct = match select.distinct {
            Some(sql_ast::Distinct::Distinct) => true,
            Some(sql_ast::Distinct::On(_)) => {
                return Err(DbError::UnsupportedOperation("DISTINCT ON not supported".into()));
            }
            None => false,
        };

        let projection = select
            .projection
            .into_iter()
            .map(|item| self.convert_select_item(item))
            .collect::<Result<Vec<_>>>()?;

        let from = select
            .from
            .into_iter()
            .map(|table| self.convert_table_with_joins(table))
            .collect::<Result<Vec<_>>>()?;

        let selection = select
            .selection
            .map(|expr| self.expr_converter.convert(expr, self))
            .transpose()?;

        let group_by = match select.group_by {
            sql_ast::GroupByExpr::Expressions(exprs, _) => {
                exprs.into_iter()
                    .map(|expr| self.expr_converter.convert(expr, self))
                    .collect::<Result<Vec<_>>>()?
            }
            sql_ast::GroupByExpr::All(_) => {
                return Err(DbError::UnsupportedOperation("GROUP BY ALL not supported".into()));
            }
        };

        let having = select
            .having
            .map(|expr| self.expr_converter.convert(expr, self))
            .transpose()?;

        let order_by = self.convert_order_by(query.order_by)?;
        let limit = self.convert_limit_clause(&query.limit_clause)?;

        Ok(QueryStmt {
            with,
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
            limit,
        })
    }

    fn convert_with(&self, with: sql_ast::With) -> Result<With> {
        let cte_tables = with.cte_tables
            .into_iter()
            .map(|cte| self.convert_cte(cte))
            .collect::<Result<Vec<_>>>()?;

        Ok(With {
            recursive: with.recursive,
            cte_tables,
        })
    }

    fn convert_cte(&self, cte: sql_ast::Cte) -> Result<Cte> {
        let alias = cte.alias.name.value;
        let query = self.convert_query(*cte.query)?;
        Ok(Cte {
            alias,
            query: Box::new(query),
        })
    }

    fn convert_table_with_joins(&self, table: sql_ast::TableWithJoins) -> Result<TableWithJoins> {
        let relation = self.convert_table_factor(table.relation)?;
        let joins = table
            .joins
            .into_iter()
            .map(|join| self.convert_join(join))
            .collect::<Result<Vec<_>>>()?;

        Ok(TableWithJoins { relation, joins })
    }

    fn convert_table_factor(&self, factor: sql_ast::TableFactor) -> Result<TableFactor> {
        match factor {
            sql_ast::TableFactor::Table { name, alias, .. } => {
                let table_name = extract_table_name(&name)?;
                let table_alias = alias.map(|a| a.name.value);
                Ok(TableFactor::Table {
                    name: table_name,
                    alias: table_alias,
                })
            }
            sql_ast::TableFactor::Derived { subquery, alias, .. } => {
                let sub_stmt = self.convert_query(*subquery)?;
                let sub_alias = alias.map(|a| a.name.value);
                Ok(TableFactor::Derived {
                    subquery: Box::new(sub_stmt),
                    alias: sub_alias,
                })
            }
            _ => Err(DbError::UnsupportedOperation(
                "Complex table references not supported".into()
            )),
        }
    }

    fn convert_join(&self, join: sql_ast::Join) -> Result<Join> {
        let relation = self.convert_table_factor(join.relation)?;
        let join_operator = self.convert_join_operator(join.join_operator)?;

        Ok(Join {
            relation,
            join_operator,
        })
    }

    fn convert_join_operator(&self, op: sql_ast::JoinOperator) -> Result<JoinOperator> {
        match op {
            sql_ast::JoinOperator::Inner(constraint) | sql_ast::JoinOperator::Join(constraint) => {
                Ok(JoinOperator::Inner(self.convert_join_constraint(constraint)?))
            }
            sql_ast::JoinOperator::Left(constraint) => {
                Ok(JoinOperator::LeftOuter(self.convert_join_constraint(constraint)?))
            }
            sql_ast::JoinOperator::Right(constraint) => {
                Ok(JoinOperator::RightOuter(self.convert_join_constraint(constraint)?))
            }
            sql_ast::JoinOperator::FullOuter(constraint) => {
                Ok(JoinOperator::FullOuter(self.convert_join_constraint(constraint)?))
            }
            sql_ast::JoinOperator::CrossJoin(_) => Ok(JoinOperator::CrossJoin),
            _ => Err(DbError::UnsupportedOperation(format!(
                "Unsupported join type: {:?}",
                op
            ))),
        }
    }

    fn convert_join_constraint(&self, constraint: sql_ast::JoinConstraint) -> Result<JoinConstraint> {
        match constraint {
            sql_ast::JoinConstraint::On(expr) => {
                Ok(JoinConstraint::On(self.expr_converter.convert(expr, self)?))
            }
            sql_ast::JoinConstraint::None => Ok(JoinConstraint::None),
            _ => Err(DbError::UnsupportedOperation(
                "Only ON constraint supported in JOIN".into()
            )),
        }
    }
    fn convert_order_by(&self, order_by: Option<sql_ast::OrderBy>) -> Result<Vec<OrderByExpr>> {
        let Some(order_by) = order_by else {
            return Ok(Vec::new());
        };

        match order_by.kind {
            sql_ast::OrderByKind::Expressions(exprs) => {
                exprs
                    .into_iter()
                    .map(|expr| self.convert_order_by_expr(expr))
                    .collect()
            }
            sql_ast::OrderByKind::All(all) => {
                Err(DbError::UnsupportedOperation(
                    format!("ORDER BY ALL not supported: {:?}", all)
                ))
            }
        }
    }

    fn convert_order_by_expr(&self, order: sql_ast::OrderByExpr) -> Result<OrderByExpr> {
        let expr = self.expr_converter.convert(order.expr, self)?;
        let descending = order.options.asc.map(|asc| !asc).unwrap_or(false);

        Ok(OrderByExpr {
            expr,
            descending,
        })
    }

    fn convert_limit_clause(&self, limit_clause: &Option<sql_ast::LimitClause>) -> Result<Option<usize>> {
        let Some(clause) = limit_clause else {
            return Ok(None);
        };

        match clause {
            sql_ast::LimitClause::LimitOffset { limit, .. } => {
                match limit {
                    Some(sql_ast::Expr::Value(value_with_span)) => {
                        self.extract_limit_number(&value_with_span.value)
                    }
                    Some(_) => Err(DbError::UnsupportedOperation(
                        "Only numeric LIMIT supported".into()
                    )),
                    None => Ok(None),
                }
            }
            sql_ast::LimitClause::OffsetCommaLimit { limit, .. } => {
                match limit {
                    sql_ast::Expr::Value(value_with_span) => {
                        self.extract_limit_number(&value_with_span.value)
                    }
                    _ => Err(DbError::UnsupportedOperation(
                        "Only numeric LIMIT supported".into()
                    )),
                }
            }
        }
    }

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

    fn convert_create_index(
        &self,
        ci: &sql_ast::CreateIndex,
    ) -> Result<CreateIndexStmt> {
        let index_name = match &ci.name {
            Some(n) => extract_table_name(n)?,
            None => {
                return Err(DbError::ParseError("Index name is required".into()));
            }
        };

        let table_name_str = extract_table_name(&ci.table_name)?;

        if ci.columns.len() != 1 {
            return Err(DbError::UnsupportedOperation(
                "Multi-column indexes are not supported yet".into()
            ));
        }

        let column = match &ci.columns[0].column.expr {
             sql_ast::Expr::Identifier(ident) => ident.value.clone(),
             _ => return Err(DbError::UnsupportedOperation("Index column must be an identifier".into())),
        };

        Ok(CreateIndexStmt {
            index_name,
            table_name: table_name_str,
            column,
            if_not_exists: ci.if_not_exists,
            unique: ci.unique,
        })
    }

    fn convert_alter_table(
        &self,
        name: sql_ast::ObjectName,
        operation: sql_ast::AlterTableOperation,
    ) -> Result<AlterTableStmt> {
        let table_name = extract_table_name(&name)?;
        let op = match operation {
            sql_ast::AlterTableOperation::AddColumn { column_def, .. } => {
                let col_def = self.convert_column_def(column_def)?;
                AlterTableOperation::AddColumn(col_def)
            }
            sql_ast::AlterTableOperation::DropColumn { column_names, .. } => {
                if column_names.len() != 1 {
                    return Err(DbError::UnsupportedOperation(
                        "Only single column drop supported".into()
                    ));
                }
                AlterTableOperation::DropColumn(column_names[0].value.clone())
            }
            sql_ast::AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                AlterTableOperation::RenameColumn {
                    old_name: old_column_name.value,
                    new_name: new_column_name.value,
                }
            }
            sql_ast::AlterTableOperation::RenameTable { table_name } => {
                let name = match table_name {
                    sql_ast::RenameTableNameKind::To(n) | sql_ast::RenameTableNameKind::As(n) => n,
                };
                let new_name = extract_table_name(&name)?;
                AlterTableOperation::RenameTable(new_name)
            }
            _ => return Err(DbError::UnsupportedOperation(format!(
                "Unsupported ALTER TABLE operation: {:?}", operation
            ))),
        };

        Ok(AlterTableStmt {
            table_name,
            operation: op,
        })
    }

    fn convert_select_item(&self, item: sql_ast::SelectItem) -> Result<SelectItem> {
        match item {
            sql_ast::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
            sql_ast::SelectItem::UnnamedExpr(expr) => {
                Ok(SelectItem::Expr {
                    expr: self.expr_converter.convert(expr, self)?,
                    alias: None,
                })
            }
            sql_ast::SelectItem::ExprWithAlias { expr, alias } => {
                Ok(SelectItem::Expr {
                    expr: self.expr_converter.convert(expr, self)?,
                    alias: Some(alias.value),
                })
            }
            _ => Err(DbError::UnsupportedOperation(
                "Unsupported select item".into()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_references() {
        let adapter = SqlParserAdapter::new();
        let stmts = adapter.parse("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))").unwrap();
        
        let Statement::CreateTable(create) = &stmts[0] else { panic!("Expected CreateTable"); };
        
        assert_eq!(create.columns.len(), 2);
        let col = &create.columns[1];
        assert_eq!(col.name, "parent_id");
        assert!(col.references.is_some());
        let fk = col.references.as_ref().unwrap();
        assert_eq!(fk.table, "parent");
        assert_eq!(fk.column, "id");
    }
}