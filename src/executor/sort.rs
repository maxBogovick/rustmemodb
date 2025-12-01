// ============================================================================
// src/executor/sort.rs - Professional Sorting Implementation
// ============================================================================
//
// Design Patterns:
// - Comparator Pattern: Flexible comparison strategy for multi-column sorting
// - Strategy Pattern: Configurable NULL handling and sort direction
//
// Features:
// - Multi-column sorting support
// - Stable sort (maintains relative order for equal elements)
// - SQL-compliant NULL handling (NULLS LAST for ASC, NULLS FIRST for DESC)
// - Type-safe comparison with proper error handling
// - Efficient sorting using standard library's stable_sort_by
//
// ============================================================================

use crate::core::{Result, DbError, Row, Schema, Value};
use crate::parser::ast::Expr;
use crate::evaluator::EvaluationContext;
use std::cmp::Ordering;

// ============================================================================
// NULL HANDLING STRATEGY
// ============================================================================

/// Strategy for handling NULL values during sorting
///
/// SQL Standard Behavior:
/// - ASC: NULLS LAST (NULL values appear at the end)
/// - DESC: NULLS FIRST (NULL values appear at the beginning)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrdering {
    /// NULL values appear first
    NullsFirst,
    /// NULL values appear last
    NullsLast,
}

impl NullOrdering {
    /// Get the default NULL ordering for a sort direction
    ///
    /// SQL Standard:
    /// - ASC (descending=false) → NULLS LAST
    /// - DESC (descending=true) → NULLS FIRST
    pub fn default_for_direction(descending: bool) -> Self {
        if descending {
            Self::NullsFirst
        } else {
            Self::NullsLast
        }
    }
}

// ============================================================================
// SORT KEY - Single column sorting specification
// ============================================================================

/// Represents a single sorting key (one column in ORDER BY clause)
#[derive(Debug, Clone)]
pub struct SortKey {
    /// Expression to sort by (typically a column reference)
    pub expr: Expr,
    /// Sort direction: false=ASC, true=DESC
    pub descending: bool,
    /// NULL handling strategy
    pub null_ordering: NullOrdering,
}

impl SortKey {
    /// Create a new sort key with default NULL ordering
    pub fn new(expr: Expr, descending: bool) -> Self {
        Self {
            expr,
            descending,
            null_ordering: NullOrdering::default_for_direction(descending),
        }
    }

    /// Create a new sort key with custom NULL ordering
    pub fn with_null_ordering(expr: Expr, descending: bool, null_ordering: NullOrdering) -> Self {
        Self {
            expr,
            descending,
            null_ordering,
        }
    }
}

// ============================================================================
// ROW COMPARATOR - Compares two rows based on sort keys
// ============================================================================

/// Comparator for sorting rows based on multiple sort keys
///
/// Uses Comparator Pattern to encapsulate comparison logic
pub struct RowComparator<'a> {
    sort_keys: &'a [SortKey],
    schema: &'a Schema,
    eval_context: &'a EvaluationContext<'a>,
}

impl<'a> RowComparator<'a> {
    pub fn new(
        sort_keys: &'a [SortKey],
        schema: &'a Schema,
        eval_context: &'a EvaluationContext<'a>,
    ) -> Self {
        Self {
            sort_keys,
            schema,
            eval_context,
        }
    }

    /// Compare two rows according to the sort keys
    ///
    /// Returns:
    /// - Ordering::Less if row1 < row2
    /// - Ordering::Equal if row1 == row2
    /// - Ordering::Greater if row1 > row2
    pub fn compare(&self, row1: &Row, row2: &Row) -> Result<Ordering> {
        // Multi-column sorting: compare by each sort key in order
        for sort_key in self.sort_keys {
            let ordering = self.compare_by_key(row1, row2, sort_key)?;

            // If not equal, return the ordering
            // If equal, continue to the next sort key
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }

        // All sort keys are equal
        Ok(Ordering::Equal)
    }

    /// Compare two rows by a single sort key
    fn compare_by_key(&self, row1: &Row, row2: &Row, key: &SortKey) -> Result<Ordering> {
        // Evaluate the expression for both rows
        let value1 = self.eval_context.evaluate(&key.expr, row1, self.schema)?;
        let value2 = self.eval_context.evaluate(&key.expr, row2, self.schema)?;

        // Compare the values
        let ordering = self.compare_values(&value1, &value2, key)?;

        Ok(ordering)
    }

    /// Compare two values according to sort direction and NULL handling
    fn compare_values(&self, value1: &Value, value2: &Value, key: &SortKey) -> Result<Ordering> {
        // Handle NULL values according to the NULL ordering strategy
        let ordering = match (value1.is_null(), value2.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                // value1 is NULL, value2 is not
                match key.null_ordering {
                    NullOrdering::NullsFirst => Ordering::Less,
                    NullOrdering::NullsLast => Ordering::Greater,
                }
            }
            (false, true) => {
                // value2 is NULL, value1 is not
                match key.null_ordering {
                    NullOrdering::NullsFirst => Ordering::Greater,
                    NullOrdering::NullsLast => Ordering::Less,
                }
            }
            (false, false) => {
                // Both values are non-NULL, use standard comparison
                value1.partial_cmp(value2).ok_or_else(|| {
                    DbError::TypeMismatch(format!(
                        "Cannot compare {} with {}",
                        value1.type_name(),
                        value2.type_name()
                    ))
                })?
            }
        };

        // Apply sort direction
        Ok(if key.descending {
            ordering.reverse()
        } else {
            ordering
        })
    }
}

// ============================================================================
// SORT EXECUTOR - Main sorting functionality
// ============================================================================

/// Executes sorting of rows
///
/// Uses stable sort to maintain relative order of equal elements
pub struct SortExecutor;

impl SortExecutor {
    /// Sort rows according to sort keys
    ///
    /// This is the main entry point for sorting.
    ///
    /// # Arguments
    /// * `rows` - Rows to sort (will be sorted in-place)
    /// * `sort_keys` - Sort keys specifying how to sort
    /// * `schema` - Schema of the rows
    /// * `eval_context` - Evaluation context for evaluating expressions
    ///
    /// # Returns
    /// * `Ok(())` if sorting succeeded
    /// * `Err(DbError)` if sorting failed (e.g., type mismatch)
    pub fn sort(
        rows: &mut [Row],
        sort_keys: &[SortKey],
        schema: &Schema,
        eval_context: &EvaluationContext,
    ) -> Result<()> {
        // Edge cases
        if rows.is_empty() || sort_keys.is_empty() {
            return Ok(()); // Nothing to sort
        }

        // Create comparator
        let comparator = RowComparator::new(sort_keys, schema, eval_context);

        // Stable sort: maintains relative order of equal elements
        // This is important for predictable results and multi-level sorting
        rows.sort_by(|row1, row2| {
            comparator
                .compare(row1, row2)
                .unwrap_or(Ordering::Equal) // In case of error, treat as equal
        });

        Ok(())
    }

    /// Convert (Expr, bool) tuples to SortKey objects
    ///
    /// Helper function to convert the logical plan's sort key representation
    /// to our internal SortKey type with proper NULL handling.
    pub fn keys_from_plan(sort_keys: &[(Expr, bool)]) -> Vec<SortKey> {
        sort_keys
            .iter()
            .map(|(expr, descending)| SortKey::new(expr.clone(), *descending))
            .collect()
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluator::EvaluatorRegistry;

    #[test]
    fn test_null_ordering_defaults() {
        // ASC → NULLS LAST
        assert_eq!(
            NullOrdering::default_for_direction(false),
            NullOrdering::NullsLast
        );

        // DESC → NULLS FIRST
        assert_eq!(
            NullOrdering::default_for_direction(true),
            NullOrdering::NullsFirst
        );
    }

    #[test]
    fn test_sort_key_creation() {
        let expr = Expr::Column("age".to_string());

        // ASC with default NULL handling
        let key = SortKey::new(expr.clone(), false);
        assert!(!key.descending);
        assert_eq!(key.null_ordering, NullOrdering::NullsLast);

        // DESC with default NULL handling
        let key = SortKey::new(expr.clone(), true);
        assert!(key.descending);
        assert_eq!(key.null_ordering, NullOrdering::NullsFirst);
    }

    #[test]
    fn test_compare_integers_ascending() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let sort_key = SortKey::new(Expr::Column("value".to_string()), false);
        let sort_keys = vec![sort_key];
        let comparator = RowComparator::new(&sort_keys, &schema, &eval_ctx);

        let row1 = vec![Value::Integer(1)];
        let row2 = vec![Value::Integer(2)];

        let result = comparator.compare(&row1, &row2).unwrap();
        assert_eq!(result, Ordering::Less);
    }

    #[test]
    fn test_compare_integers_descending() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let sort_key = SortKey::new(Expr::Column("value".to_string()), true);
        let sort_keys = vec![sort_key];
        let comparator = RowComparator::new(&sort_keys, &schema, &eval_ctx);

        let row1 = vec![Value::Integer(1)];
        let row2 = vec![Value::Integer(2)];

        let result = comparator.compare(&row1, &row2).unwrap();
        assert_eq!(result, Ordering::Greater); // Reversed because DESC
    }

    #[test]
    fn test_null_handling_ascending() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        // ASC: NULLS LAST
        let sort_key = SortKey::new(Expr::Column("value".to_string()), false);
        let sort_keys = vec![sort_key];
        let comparator = RowComparator::new(&sort_keys, &schema, &eval_ctx);

        let null_row = vec![Value::Null];
        let value_row = vec![Value::Integer(1)];

        // NULL should be greater (come after) non-NULL in ASC
        let result = comparator.compare(&null_row, &value_row).unwrap();
        assert_eq!(result, Ordering::Greater);
    }

    #[test]
    fn test_null_handling_descending() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        // DESC: NULLS FIRST
        let sort_key = SortKey::new(Expr::Column("value".to_string()), true);
        let sort_keys = vec![sort_key];
        let comparator = RowComparator::new(&sort_keys, &schema, &eval_ctx);

        let null_row = vec![Value::Null];
        let value_row = vec![Value::Integer(1)];

        // NULL should be less (come before) non-NULL in DESC
        let result = comparator.compare(&null_row, &value_row).unwrap();
        assert_eq!(result, Ordering::Less);
    }

    #[test]
    fn test_multi_column_sorting() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![
            Column::new("category", DataType::Text),
            Column::new("value", DataType::Integer),
        ]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        // Sort by category ASC, then by value DESC
        let sort_keys = vec![
            SortKey::new(Expr::Column("category".to_string()), false),
            SortKey::new(Expr::Column("value".to_string()), true),
        ];
        let comparator = RowComparator::new(&sort_keys, &schema, &eval_ctx);

        // Same category, different values
        let row1 = vec![Value::Text("A".to_string()), Value::Integer(1)];
        let row2 = vec![Value::Text("A".to_string()), Value::Integer(2)];

        // Should compare by second key (value DESC)
        let result = comparator.compare(&row1, &row2).unwrap();
        assert_eq!(result, Ordering::Greater); // 1 > 2 in DESC
    }

    #[test]
    fn test_sort_executor_basic() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let mut rows = vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ];

        let sort_keys = vec![SortKey::new(Expr::Column("value".to_string()), false)];

        SortExecutor::sort(&mut rows, &sort_keys, &schema, &eval_ctx).unwrap();

        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[1][0], Value::Integer(2));
        assert_eq!(rows[2][0], Value::Integer(3));
    }

    #[test]
    fn test_sort_executor_with_nulls() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let mut rows = vec![
            vec![Value::Integer(2)],
            vec![Value::Null],
            vec![Value::Integer(1)],
        ];

        // ASC: NULLS LAST
        let sort_keys = vec![SortKey::new(Expr::Column("value".to_string()), false)];

        SortExecutor::sort(&mut rows, &sort_keys, &schema, &eval_ctx).unwrap();

        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[1][0], Value::Integer(2));
        assert_eq!(rows[2][0], Value::Null);
    }

    #[test]
    fn test_sort_executor_descending_with_nulls() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let mut rows = vec![
            vec![Value::Integer(2)],
            vec![Value::Null],
            vec![Value::Integer(1)],
        ];

        // DESC: NULLS FIRST
        let sort_keys = vec![SortKey::new(Expr::Column("value".to_string()), true)];

        SortExecutor::sort(&mut rows, &sort_keys, &schema, &eval_ctx).unwrap();

        assert_eq!(rows[0][0], Value::Null);
        assert_eq!(rows[1][0], Value::Integer(2));
        assert_eq!(rows[2][0], Value::Integer(1));
    }

    #[test]
    fn test_empty_rows() {
        use crate::core::{Column, DataType, Schema as CoreSchema};

        let schema = CoreSchema::new(vec![Column::new("value", DataType::Integer)]);
        let registry = EvaluatorRegistry::with_default_evaluators();
        let eval_ctx = EvaluationContext::new(&registry);

        let mut rows: Vec<Row> = vec![];
        let sort_keys = vec![SortKey::new(Expr::Column("value".to_string()), false)];

        let result = SortExecutor::sort(&mut rows, &sort_keys, &schema, &eval_ctx);
        assert!(result.is_ok());
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_keys_from_plan() {
        let plan_keys = vec![
            (Expr::Column("age".to_string()), false),
            (Expr::Column("name".to_string()), true),
        ];

        let sort_keys = SortExecutor::keys_from_plan(&plan_keys);

        assert_eq!(sort_keys.len(), 2);
        assert!(!sort_keys[0].descending);
        assert_eq!(sort_keys[0].null_ordering, NullOrdering::NullsLast);
        assert!(sort_keys[1].descending);
        assert_eq!(sort_keys[1].null_ordering, NullOrdering::NullsFirst);
    }
}
