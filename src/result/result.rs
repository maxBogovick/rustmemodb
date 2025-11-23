use crate::core::{Row, Value};

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn print(&self) {
        if self.columns.is_empty() {
            println!("Empty result set");
            return;
        }

        // Calculate column widths
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();

        for row in &self.rows {
            for (i, value) in row.iter().enumerate() {
                widths[i] = widths[i].max(value.to_string().len());
            }
        }

        // Print header
        let header: Vec<String> = self.columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{:width$}", col, width = widths[i]))
            .collect();

        println!("{}", header.join(" | "));

        let separator: String = widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("-+-");
        println!("{}", separator);

        // Print rows
        for row in &self.rows {
            let row_str: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, val)| format!("{:width$}", val, width = widths[i]))
                .collect();
            println!("{}", row_str.join(" | "));
        }

        println!("\n{} row(s)", self.rows.len());
    }
}