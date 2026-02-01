use crate::core::{Row, Column};

#[derive(Debug, Clone)]
pub struct QueryResult {
    columns: Vec<Column>,
    rows: Vec<Row>,
    affected_rows: Option<usize>, // For UPDATE/DELETE operations
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
        }
    }

    pub fn empty_with_message(_message: String) -> Self {
        // For now, just return empty result
        // In future, could add message field to QueryResult
        Self::empty()
    }

    pub fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            affected_rows: None,
        }
    }

    pub fn deleted(count: usize) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: Some(count),
        }
    }

    pub fn updated(count: usize) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: Some(count),
        }
    }

    pub fn affected_rows(&self) -> Option<usize> {
        self.affected_rows
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    pub fn print(&self) {
        if self.columns.is_empty() {
            println!("Empty result set");
            return;
        }

        let widths = self.calculate_column_widths();

        self.print_header(&widths);
        self.print_separator(&widths);
        self.print_rows(&widths);

        println!("\n{} row(s)", self.rows.len());
    }

    fn calculate_column_widths(&self) -> Vec<usize> {
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.name.len()).collect();

        for row in &self.rows {
            for (i, value) in row.iter().enumerate() {
                if let Some(width) = widths.get_mut(i) {
                    *width = (*width).max(value.to_string().len());
                }
            }
        }

        widths
    }

    fn print_header(&self, widths: &[usize]) {
        let header: Vec<String> = self.columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{:width$}", col.name, width = widths[i]))
            .collect();

        println!("{}", header.join(" | "));
    }

    fn print_separator(&self, widths: &[usize]) {
        let separator: String = widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("-+-");

        println!("{}", separator);
    }

    fn print_rows(&self, widths: &[usize]) {
        for row in &self.rows {
            let row_str: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, val)| format!("{:width$}", val, width = widths[i]))
                .collect();

            println!("{}", row_str.join(" | "));
        }
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DataType;

    #[test]
    fn test_empty_result() {
        let result = QueryResult::empty();
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.columns().len(), 0);
    }

    #[test]
    fn test_new_result() {
        let columns = vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text)
        ];
        let rows = vec![];
        let result = QueryResult::new(columns.clone(), rows);

        assert_eq!(result.columns().len(), 2);
        assert_eq!(result.columns()[0].name, "id");
        assert!(result.is_empty());
    }

    #[test]
    fn test_accessors() {
        let columns = vec![Column::new("col1", DataType::Text)];
        let rows = vec![];
        let result = QueryResult::new(columns, rows);

        assert_eq!(result.columns().len(), 1);
        assert_eq!(result.rows().len(), 0);
    }
}