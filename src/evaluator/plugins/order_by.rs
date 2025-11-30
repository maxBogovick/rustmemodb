// ============================================================================
// src/executor/plugins/order_by.rs - ORDER BY sorting plugin
// ============================================================================

use crate::parser::ast::{Expr, OrderByExpr, BinaryOp};
use crate::core::{Result, Value, Row, Schema, DbError};
use crate::evaluator::EvaluationContext;
use std::cmp::Ordering;

/// Trait для сортировщиков (plugin interface)
pub trait RowSorter: Send + Sync {
    fn name(&self) -> &'static str;

    /// Сортировать строки согласно ORDER BY выражениям
    fn sort(
        &self,
        rows: Vec<Row>,
        order_by: &[OrderByExpr],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Vec<Row>>;
}

// ============================================================================
// ORDER BY SORTER - Основная реализация
// ============================================================================

pub struct OrderBySorter;

impl OrderBySorter {
    pub fn new() -> Self {
        Self
    }

    /// Сравнить два значения с учётом NULL (NULL LAST по умолчанию)
    fn compare_values(&self, a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            // NULL handling: NULL считается "больше" всех значений (NULL LAST)
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,

            // Integer comparison
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),

            // Float comparison
            (Value::Float(a), Value::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }

            // Text comparison
            (Value::Text(a), Value::Text(b)) => a.cmp(b),

            // Boolean comparison (false < true)
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),

            // Mixed numeric types
            (Value::Integer(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }

            // Type mismatch - compare as strings
            _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
        }
    }

    /// Сравнить две строки по списку ORDER BY выражений
    fn compare_rows(
        &self,
        row_a: &Row,
        row_b: &Row,
        order_by: &[OrderByExpr],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Ordering> {
        for order_expr in order_by {
            // Вычисляем значения выражения для обеих строк
            let val_a = eval_ctx.evaluate(&order_expr.expr, row_a, schema)?;
            let val_b = eval_ctx.evaluate(&order_expr.expr, row_b, schema)?;

            // Сравниваем
            let mut cmp = self.compare_values(&val_a, &val_b);

            // Инвертируем порядок для DESC
            if order_expr.descending {
                cmp = cmp.reverse();
            }

            // Если не равны - возвращаем результат
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
            // Иначе переходим к следующему ORDER BY выражению
        }

        // Все выражения равны
        Ok(Ordering::Equal)
    }
}

impl Default for OrderBySorter {
    fn default() -> Self {
        Self::new()
    }
}

impl RowSorter for OrderBySorter {
    fn name(&self) -> &'static str {
        "ORDER BY"
    }

    fn sort(
        &self,
        mut rows: Vec<Row>,
        order_by: &[OrderByExpr],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Vec<Row>> {
        // Если нет ORDER BY - возвращаем как есть
        if order_by.is_empty() {
            return Ok(rows);
        }

        // Собираем ошибки сортировки (если будут)
        let mut sort_error: Option<DbError> = None;

        rows.sort_by(|a, b| {
            // Если уже была ошибка - не продолжаем вычисления
            if sort_error.is_some() {
                return Ordering::Equal;
            }

            match self.compare_rows(a, b, order_by, schema, eval_ctx) {
                Ok(ordering) => ordering,
                Err(e) => {
                    sort_error = Some(e);
                    Ordering::Equal
                }
            }
        });

        // Если была ошибка во время сортировки - возвращаем её
        if let Some(err) = sort_error {
            return Err(err);
        }

        Ok(rows)
    }
}

// ============================================================================
// NULLS FIRST / NULLS LAST SORTER - Расширенная версия
// ============================================================================

/// Политика обработки NULL при сортировке
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

/// Расширенное выражение ORDER BY с поддержкой NULLS FIRST/LAST
#[derive(Debug, Clone)]
pub struct ExtendedOrderByExpr {
    pub expr: Expr,
    pub descending: bool,
    pub null_ordering: NullOrdering,
}

impl From<OrderByExpr> for ExtendedOrderByExpr {
    fn from(expr: OrderByExpr) -> Self {
        Self {
            expr: expr.expr,
            descending: expr.descending,
            // По умолчанию: ASC = NULLS LAST, DESC = NULLS FIRST
            null_ordering: if expr.descending {
                NullOrdering::NullsFirst
            } else {
                NullOrdering::NullsLast
            },
        }
    }
}

pub struct ExtendedOrderBySorter;

impl ExtendedOrderBySorter {
    pub fn new() -> Self {
        Self
    }

    /// Сравнить два значения с настраиваемой обработкой NULL
    fn compare_values_with_nulls(
        &self,
        a: &Value,
        b: &Value,
        null_ordering: NullOrdering,
    ) -> Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => match null_ordering {
                NullOrdering::NullsFirst => Ordering::Less,
                NullOrdering::NullsLast => Ordering::Greater,
            },
            (_, Value::Null) => match null_ordering {
                NullOrdering::NullsFirst => Ordering::Greater,
                NullOrdering::NullsLast => Ordering::Less,
            },
            // Остальное - стандартное сравнение
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Integer(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
        }
    }

    /// Сортировать с расширенными опциями
    pub fn sort_extended(
        &self,
        mut rows: Vec<Row>,
        order_by: &[ExtendedOrderByExpr],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Vec<Row>> {
        if order_by.is_empty() {
            return Ok(rows);
        }

        let mut sort_error: Option<DbError> = None;

        rows.sort_by(|row_a, row_b| {
            if sort_error.is_some() {
                return Ordering::Equal;
            }

            for order_expr in order_by {
                let val_a = match eval_ctx.evaluate(&order_expr.expr, row_a, schema) {
                    Ok(v) => v,
                    Err(e) => {
                        sort_error = Some(e);
                        return Ordering::Equal;
                    }
                };
                let val_b = match eval_ctx.evaluate(&order_expr.expr, row_b, schema) {
                    Ok(v) => v,
                    Err(e) => {
                        sort_error = Some(e);
                        return Ordering::Equal;
                    }
                };

                let mut cmp = self.compare_values_with_nulls(
                    &val_a,
                    &val_b,
                    order_expr.null_ordering,
                );

                if order_expr.descending {
                    cmp = cmp.reverse();
                }

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }

            Ordering::Equal
        });

        if let Some(err) = sort_error {
            return Err(err);
        }

        Ok(rows)
    }
}

impl Default for ExtendedOrderBySorter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// VALUE COMPARATOR - Утилита для сравнения значений (переиспользуемая)
// ============================================================================

/// Компаратор значений для переиспользования в других модулях
pub struct ValueComparator;

impl ValueComparator {
    /// Сравнить два Value, возвращая Ordering
    pub fn compare(a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Integer(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            _ => Ordering::Equal,
        }
    }

    /// Проверить отношение согласно BinaryOp
    pub fn compare_with_op(a: &Value, b: &Value, op: &BinaryOp) -> bool {
        let ordering = Self::compare(a, b);
        match op {
            BinaryOp::Eq => ordering == Ordering::Equal,
            BinaryOp::NotEq => ordering != Ordering::Equal,
            BinaryOp::Lt => ordering == Ordering::Less,
            BinaryOp::LtEq => ordering != Ordering::Greater,
            BinaryOp::Gt => ordering == Ordering::Greater,
            BinaryOp::GtEq => ordering != Ordering::Less,
            _ => false,
        }
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Column, DataType};
    use crate::evaluator::EvaluatorRegistry;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text),
            Column::new("age", DataType::Integer),
        ])
    }

    fn create_test_rows() -> Vec<Row> {
        vec![
            vec![Value::Integer(1), Value::Text("Alice".into()), Value::Integer(30)],
            vec![Value::Integer(2), Value::Text("Bob".into()), Value::Integer(25)],
            vec![Value::Integer(3), Value::Text("Charlie".into()), Value::Integer(35)],
            vec![Value::Integer(4), Value::Text("Diana".into()), Value::Integer(25)],
        ]
    }

    #[test]
    fn test_sort_by_single_column_asc() {
        let schema = create_test_schema();
        let rows = create_test_rows();
        let sorter = OrderBySorter::new();
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let order_by = vec![OrderByExpr {
            expr: Expr::Column("age".to_string()),
            descending: false,
        }];

        let sorted = sorter.sort(rows, &order_by, &schema, &eval_ctx).unwrap();

        // age: 25, 25, 30, 35
        assert_eq!(sorted[0][2], Value::Integer(25));
        assert_eq!(sorted[1][2], Value::Integer(25));
        assert_eq!(sorted[2][2], Value::Integer(30));
        assert_eq!(sorted[3][2], Value::Integer(35));
    }

    #[test]
    fn test_sort_by_single_column_desc() {
        let schema = create_test_schema();
        let rows = create_test_rows();
        let sorter = OrderBySorter::new();
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let order_by = vec![OrderByExpr {
            expr: Expr::Column("age".to_string()),
            descending: true,
        }];

        let sorted = sorter.sort(rows, &order_by, &schema, &eval_ctx).unwrap();

        // age: 35, 30, 25, 25
        assert_eq!(sorted[0][2], Value::Integer(35));
        assert_eq!(sorted[1][2], Value::Integer(30));
        assert_eq!(sorted[2][2], Value::Integer(25));
        assert_eq!(sorted[3][2], Value::Integer(25));
    }

    #[test]
    fn test_sort_by_multiple_columns() {
        let schema = create_test_schema();
        let rows = create_test_rows();
        let sorter = OrderBySorter::new();
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        // ORDER BY age ASC, name ASC
        let order_by = vec![
            OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: false,
            },
            OrderByExpr {
                expr: Expr::Column("name".to_string()),
                descending: false,
            },
        ];

        let sorted = sorter.sort(rows, &order_by, &schema, &eval_ctx).unwrap();

        // age 25: Bob, Diana (alphabetically)
        // age 30: Alice
        // age 35: Charlie
        assert_eq!(sorted[0][1], Value::Text("Bob".into()));
        assert_eq!(sorted[1][1], Value::Text("Diana".into()));
        assert_eq!(sorted[2][1], Value::Text("Alice".into()));
        assert_eq!(sorted[3][1], Value::Text("Charlie".into()));
    }

    #[test]
    fn test_sort_with_nulls() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("value", DataType::Integer),
        ]);

        let rows = vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Integer(5)],
            vec![Value::Integer(4), Value::Null],
        ];

        let sorter = OrderBySorter::new();
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let order_by = vec![OrderByExpr {
            expr: Expr::Column("value".to_string()),
            descending: false,
        }];

        let sorted = sorter.sort(rows, &order_by, &schema, &eval_ctx).unwrap();

        // NULL LAST by default: 5, 10, NULL, NULL
        assert_eq!(sorted[0][1], Value::Integer(5));
        assert_eq!(sorted[1][1], Value::Integer(10));
        assert_eq!(sorted[2][1], Value::Null);
        assert_eq!(sorted[3][1], Value::Null);
    }

    #[test]
    fn test_value_comparator() {
        assert_eq!(
            ValueComparator::compare(&Value::Integer(5), &Value::Integer(10)),
            Ordering::Less
        );
        assert_eq!(
            ValueComparator::compare(&Value::Text("abc".into()), &Value::Text("xyz".into())),
            Ordering::Less
        );
        assert!(ValueComparator::compare_with_op(
            &Value::Integer(5),
            &Value::Integer(10),
            &BinaryOp::Lt
        ));
    }
}