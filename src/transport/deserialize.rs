//! Custom deserializer for transposing column-major data to row-major during deserialization.
//!
//! Exasol WebSocket API returns data in column-major format, where each inner array
//! contains all values for a single column. This module provides a deserializer that
//! transposes to row-major format during deserialization, avoiding a separate pass.
//!
//! # Column-Major (Exasol Wire Format)
//!
//! ```json
//! [[col0_val0, col0_val1, col0_val2], [col1_val0, col1_val1, col1_val2]]
//! ```
//!
//! # Row-Major (After Deserialization)
//!
//! ```rust,ignore
//! vec![
//!     vec![col0_val0, col1_val0],  // row 0
//!     vec![col0_val1, col1_val1],  // row 1
//!     vec![col0_val2, col1_val2],  // row 2
//! ]
//! ```

use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
use serde_json::Value;
use std::fmt;
use std::marker::PhantomData;

/// Deserialize column-major data to row-major format.
///
/// This function is intended to be used with `#[serde(deserialize_with = "to_row_major")]`.
///
/// # Type Parameters
/// * `'de` - Deserializer lifetime
/// * `D` - Deserializer type
///
/// # Returns
/// Row-major data where `result[row_idx][col_idx]` gives the value at that position.
pub fn to_row_major<'de, D>(deserializer: D) -> Result<Vec<Vec<Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(DataVisitor)
}

/// Deserialize optional column-major data to optional row-major format.
///
/// This function handles the `Option<Vec<Vec<Value>>>` case used by `ResultSetData.data`.
pub fn to_row_major_option<'de, D>(deserializer: D) -> Result<Option<Vec<Vec<Value>>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_option(OptionDataVisitor)
}

/// Visitor for deserializing Option<Vec<Vec<Value>>> with transposition.
struct OptionDataVisitor;

impl<'de> Visitor<'de> for OptionDataVisitor {
    type Value = Option<Vec<Vec<Value>>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("null or column-major data array")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        to_row_major(deserializer).map(Some)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }
}

/// Outer visitor that iterates over columns in the column-major data.
///
/// This visitor receives the outer array `[[col0...], [col1...], ...]` and
/// uses `ColumnSeed` to distribute values from each column into rows.
struct DataVisitor;

impl<'de> Visitor<'de> for DataVisitor {
    type Value = Vec<Vec<Value>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("column-major data array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // Start with empty row-major result
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut col_idx = 0;

        // Process each column
        while let Some(()) = seq.next_element_seed(ColumnSeed {
            rows: &mut rows,
            col_idx,
            _marker: PhantomData,
        })? {
            col_idx += 1;
        }

        Ok(rows)
    }
}

/// Seed for deserializing a single column and distributing values to rows.
///
/// This implements `DeserializeSeed` to maintain mutable access to the growing
/// row-major result while processing each column.
struct ColumnSeed<'a, 'de> {
    rows: &'a mut Vec<Vec<Value>>,
    col_idx: usize,
    _marker: PhantomData<&'de ()>,
}

impl<'de, 'a> DeserializeSeed<'de> for ColumnSeed<'a, 'de> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ColumnVisitor {
            rows: self.rows,
            col_idx: self.col_idx,
        })
    }
}

/// Visitor that processes values within a single column.
///
/// For each value in the column, it appends to the corresponding row.
/// If this is the first column (col_idx == 0), new rows are created.
/// Otherwise, values are appended to existing rows.
struct ColumnVisitor<'a> {
    rows: &'a mut Vec<Vec<Value>>,
    col_idx: usize,
}

impl<'de, 'a> Visitor<'de> for ColumnVisitor<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("array of column values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut row_idx = 0;

        while let Some(value) = seq.next_element::<Value>()? {
            if self.col_idx == 0 {
                // First column: create new rows
                self.rows.push(vec![value]);
            } else {
                // Subsequent columns: append to existing rows
                if row_idx < self.rows.len() {
                    self.rows[row_idx].push(value);
                } else {
                    // Column has more values than previous columns - this is an error case
                    // but we handle it gracefully by creating a new row with nulls for
                    // previous columns
                    let mut new_row = vec![Value::Null; self.col_idx];
                    new_row.push(value);
                    self.rows.push(new_row);
                }
            }
            row_idx += 1;
        }

        // If this column has fewer values than expected rows, pad with nulls
        while row_idx < self.rows.len() {
            self.rows[row_idx].push(Value::Null);
            row_idx += 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    /// Test struct that uses the custom deserializer
    #[derive(Debug, Deserialize)]
    struct TestData {
        #[serde(deserialize_with = "to_row_major")]
        data: Vec<Vec<Value>>,
    }

    /// Test struct with optional data field
    #[derive(Debug, Deserialize)]
    struct TestDataOption {
        #[serde(deserialize_with = "to_row_major_option")]
        data: Option<Vec<Vec<Value>>>,
    }

    #[test]
    fn test_empty_data() {
        let json = json!({ "data": [] });
        let result: TestData = serde_json::from_value(json).unwrap();
        assert!(result.data.is_empty());
    }

    #[test]
    fn test_single_column_single_row() {
        // Column-major: [[42]]
        let json = json!({ "data": [[42]] });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Row-major: [[42]]
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].len(), 1);
        assert_eq!(result.data[0][0], json!(42));
    }

    #[test]
    fn test_single_column_multiple_rows() {
        // Column-major: [[1, 2, 3]]
        let json = json!({ "data": [[1, 2, 3]] });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Row-major: [[1], [2], [3]]
        assert_eq!(result.data.len(), 3);
        assert_eq!(result.data[0], vec![json!(1)]);
        assert_eq!(result.data[1], vec![json!(2)]);
        assert_eq!(result.data[2], vec![json!(3)]);
    }

    #[test]
    fn test_multiple_columns_single_row() {
        // Column-major: [[1], ["a"]]
        let json = json!({ "data": [[1], ["a"]] });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Row-major: [[1, "a"]]
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0], vec![json!(1), json!("a")]);
    }

    #[test]
    fn test_multiple_columns_multiple_rows() {
        // Column-major: [[1, 2, 3], ["a", "b", "c"]]
        let json = json!({ "data": [[1, 2, 3], ["a", "b", "c"]] });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Row-major: [[1, "a"], [2, "b"], [3, "c"]]
        assert_eq!(result.data.len(), 3);
        assert_eq!(result.data[0], vec![json!(1), json!("a")]);
        assert_eq!(result.data[1], vec![json!(2), json!("b")]);
        assert_eq!(result.data[2], vec![json!(3), json!("c")]);
    }

    #[test]
    fn test_with_null_values() {
        // Column-major: [[1, null, 3], [null, "b", "c"]]
        let json = json!({ "data": [[1, null, 3], [null, "b", "c"]] });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Row-major: [[1, null], [null, "b"], [3, "c"]]
        assert_eq!(result.data.len(), 3);
        assert_eq!(result.data[0], vec![json!(1), Value::Null]);
        assert_eq!(result.data[1], vec![Value::Null, json!("b")]);
        assert_eq!(result.data[2], vec![json!(3), json!("c")]);
    }

    #[test]
    fn test_mixed_types() {
        // Column-major: [[1, 2], [true, false], ["x", "y"], [1.5, 2.5]]
        let json = json!({ "data": [[1, 2], [true, false], ["x", "y"], [1.5, 2.5]] });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Row-major: [[1, true, "x", 1.5], [2, false, "y", 2.5]]
        assert_eq!(result.data.len(), 2);
        assert_eq!(
            result.data[0],
            vec![json!(1), json!(true), json!("x"), json!(1.5)]
        );
        assert_eq!(
            result.data[1],
            vec![json!(2), json!(false), json!("y"), json!(2.5)]
        );
    }

    #[test]
    fn test_option_none() {
        let json = json!({ "data": null });
        let result: TestDataOption = serde_json::from_value(json).unwrap();
        assert!(result.data.is_none());
    }

    #[test]
    fn test_option_some_empty() {
        let json = json!({ "data": [] });
        let result: TestDataOption = serde_json::from_value(json).unwrap();
        assert_eq!(result.data, Some(vec![]));
    }

    #[test]
    fn test_option_some_with_data() {
        // Column-major: [[1, 2], ["a", "b"]]
        let json = json!({ "data": [[1, 2], ["a", "b"]] });
        let result: TestDataOption = serde_json::from_value(json).unwrap();

        // Row-major: [[1, "a"], [2, "b"]]
        let data = result.data.unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data[0], vec![json!(1), json!("a")]);
        assert_eq!(data[1], vec![json!(2), json!("b")]);
    }

    #[test]
    fn test_large_dataset() {
        // Create a large column-major dataset
        let num_rows = 1000;
        let num_cols = 10;

        let columns: Vec<Vec<i32>> = (0..num_cols)
            .map(|col| {
                (0..num_rows)
                    .map(|row| (row * num_cols + col) as i32)
                    .collect()
            })
            .collect();

        let json = json!({ "data": columns });
        let result: TestData = serde_json::from_value(json).unwrap();

        // Verify dimensions
        assert_eq!(result.data.len(), num_rows);
        for row in &result.data {
            assert_eq!(row.len(), num_cols);
        }

        // Verify some values
        assert_eq!(result.data[0][0], json!(0)); // row 0, col 0
        assert_eq!(result.data[0][1], json!(1)); // row 0, col 1
        assert_eq!(result.data[1][0], json!(10)); // row 1, col 0
        assert_eq!(result.data[999][9], json!(9999)); // last row, last col
    }

    #[test]
    fn test_realistic_exasol_response() {
        // Simulate a realistic Exasol response with different data types
        let json = json!({
            "data": [
                [1, 2, 3],                           // INT column
                ["Alice", "Bob", "Carol"],           // VARCHAR column
                [true, false, true],                 // BOOLEAN column
                ["2024-01-15", "2024-02-20", null],  // DATE column (with null)
                [1234.56, 7890.12, 0.0]              // DECIMAL column
            ]
        });
        let result: TestData = serde_json::from_value(json).unwrap();

        assert_eq!(result.data.len(), 3); // 3 rows

        // Row 0: [1, "Alice", true, "2024-01-15", 1234.56]
        assert_eq!(result.data[0][0], json!(1));
        assert_eq!(result.data[0][1], json!("Alice"));
        assert_eq!(result.data[0][2], json!(true));
        assert_eq!(result.data[0][3], json!("2024-01-15"));
        assert_eq!(result.data[0][4], json!(1234.56));

        // Row 1: [2, "Bob", false, "2024-02-20", 7890.12]
        assert_eq!(result.data[1][0], json!(2));
        assert_eq!(result.data[1][1], json!("Bob"));

        // Row 2: [3, "Carol", true, null, 0.0]
        assert_eq!(result.data[2][0], json!(3));
        assert_eq!(result.data[2][3], Value::Null); // null date
    }
}
