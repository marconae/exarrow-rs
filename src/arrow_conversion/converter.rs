//! Main converter for transforming Exasol WebSocket JSON responses to Arrow RecordBatch.
//!
//! This module provides the core conversion logic that takes row-major JSON data
//! and converts it directly to columnar Arrow format without intermediate representations.

use crate::error::ConversionError;
use crate::transport::messages::{ColumnInfo, ResultData};
use crate::types::{ExasolType, SchemaBuilder};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use serde_json::Value;
use std::sync::Arc;

use super::builders::build_array;

/// Converter for transforming Exasol result data to Arrow RecordBatch.
pub struct ArrowConverter {
    schema: Arc<Schema>,
    column_types: Vec<ExasolType>,
}

impl ArrowConverter {
    /// Create a new Arrow converter from Exasol column metadata.
    ///
    /// # Arguments
    /// * `columns` - Column metadata from Exasol result set
    ///
    /// # Returns
    /// A new `ArrowConverter` instance
    ///
    /// # Errors
    /// Returns `ConversionError` if the schema cannot be built
    pub fn new(columns: &[ColumnInfo]) -> Result<Self, ConversionError> {
        // Extract column metadata and build schema
        let column_metadata: Result<Vec<_>, ConversionError> = columns
            .iter()
            .map(|col| {
                let exasol_type = parse_exasol_type(&col.data_type)?;
                Ok::<(String, ExasolType), ConversionError>((col.name.clone(), exasol_type))
            })
            .collect();

        let column_metadata = column_metadata?;

        // Build Arrow schema
        let mut schema_builder = SchemaBuilder::new();
        let mut column_types = Vec::new();

        for (name, exasol_type) in column_metadata {
            schema_builder = schema_builder.add_column(crate::types::ColumnMetadata {
                name,
                data_type: exasol_type.clone(),
                nullable: true, // Exasol columns are nullable by default
            });
            column_types.push(exasol_type);
        }

        let schema = Arc::new(schema_builder.build()?);

        Ok(Self {
            schema,
            column_types,
        })
    }

    /// Get the Arrow schema for this converter.
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Convert Exasol result data to an Arrow RecordBatch.
    ///
    /// This performs the main conversion from row-major JSON data to columnar Arrow format.
    ///
    /// # Arguments
    /// * `result_data` - The result data from Exasol WebSocket response
    ///
    /// # Returns
    /// An Arrow `RecordBatch` containing the converted data
    ///
    /// # Errors
    /// Returns `ConversionError` if:
    /// - The data doesn't match the schema
    /// - Type conversion fails for any value
    /// - Numeric overflow occurs
    /// - UTF-8 validation fails for strings
    pub fn convert_to_record_batch(
        &self,
        result_data: &ResultData,
    ) -> Result<RecordBatch, ConversionError> {
        // Handle empty result set
        if result_data.rows.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        // Verify row structure matches schema
        let num_columns = self.column_types.len();
        for (row_idx, row) in result_data.rows.iter().enumerate() {
            if row.len() != num_columns {
                return Err(ConversionError::SchemaMismatch(format!(
                    "Row {} has {} columns, expected {}",
                    row_idx,
                    row.len(),
                    num_columns
                )));
            }
        }

        // Transpose row-major data to column-major
        let columns_data = transpose_rows_to_columns(&result_data.rows, num_columns);

        // Build Arrow arrays for each column
        let arrays: Result<Vec<_>, _> = self
            .column_types
            .iter()
            .enumerate()
            .map(|(col_idx, exasol_type)| build_array(exasol_type, &columns_data[col_idx], col_idx))
            .collect();

        let arrays = arrays?;

        // Create RecordBatch
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| ConversionError::ArrowError(e.to_string()))
    }

    /// Convert multiple result chunks to RecordBatches.
    ///
    /// This is useful when fetching large result sets in multiple chunks.
    ///
    /// # Arguments
    /// * `result_data_chunks` - Multiple result data chunks
    ///
    /// # Returns
    /// A vector of `RecordBatch` instances
    pub fn convert_chunks(
        &self,
        result_data_chunks: &[ResultData],
    ) -> Result<Vec<RecordBatch>, ConversionError> {
        result_data_chunks
            .iter()
            .map(|chunk| self.convert_to_record_batch(chunk))
            .collect()
    }
}

/// Parse Exasol DataType from WebSocket message format to ExasolType enum.
fn parse_exasol_type(
    data_type: &crate::transport::messages::DataType,
) -> Result<ExasolType, ConversionError> {
    match data_type.type_name.as_str() {
        "BOOLEAN" => Ok(ExasolType::Boolean),

        "CHAR" => {
            let size = data_type.size.ok_or_else(|| {
                ConversionError::InvalidFormat("CHAR type missing size".to_string())
            })? as usize;
            Ok(ExasolType::Char { size })
        }

        "VARCHAR" => {
            let size = data_type.size.ok_or_else(|| {
                ConversionError::InvalidFormat("VARCHAR type missing size".to_string())
            })? as usize;
            Ok(ExasolType::Varchar { size })
        }

        "DECIMAL" => {
            let precision = data_type.precision.ok_or_else(|| {
                ConversionError::InvalidFormat("DECIMAL type missing precision".to_string())
            })? as u8;
            let scale = data_type.scale.ok_or_else(|| {
                ConversionError::InvalidFormat("DECIMAL type missing scale".to_string())
            })? as i8;
            Ok(ExasolType::Decimal { precision, scale })
        }

        "DOUBLE" => Ok(ExasolType::Double),

        "DATE" => Ok(ExasolType::Date),

        "TIMESTAMP" => {
            let with_local_time_zone = data_type.with_local_time_zone.unwrap_or(false);
            Ok(ExasolType::Timestamp {
                with_local_time_zone,
            })
        }

        "INTERVAL YEAR TO MONTH" => Ok(ExasolType::IntervalYearToMonth),

        "INTERVAL DAY TO SECOND" => {
            let precision = data_type.fraction.unwrap_or(3) as u8;
            Ok(ExasolType::IntervalDayToSecond { precision })
        }

        "GEOMETRY" => Ok(ExasolType::Geometry { srid: None }),

        "HASHTYPE" => {
            let byte_size = data_type.size.unwrap_or(16) as usize;
            Ok(ExasolType::Hashtype { byte_size })
        }

        unknown => Err(ConversionError::UnsupportedType {
            exasol_type: unknown.to_string(),
        }),
    }
}

/// Transpose row-major data to column-major.
///
/// Converts Vec<Vec<Value>> (rows) to Vec<Vec<Value>> (columns).
fn transpose_rows_to_columns(rows: &[Vec<Value>], num_columns: usize) -> Vec<Vec<Value>> {
    let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(rows.len()); num_columns];

    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            columns[col_idx].push(value.clone());
        }
    }

    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::messages::DataType;
    use serde_json::json;

    fn create_test_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType {
                    type_name: "DECIMAL".to_string(),
                    precision: Some(18),
                    scale: Some(0),
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType {
                    type_name: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                    size: Some(100),
                    character_set: Some("UTF8".to_string()),
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
            ColumnInfo {
                name: "active".to_string(),
                data_type: DataType {
                    type_name: "BOOLEAN".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
        ]
    }

    #[test]
    fn test_arrow_converter_creation() {
        let columns = create_test_columns();
        let converter = ArrowConverter::new(&columns).unwrap();

        let schema = converter.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "active");
    }

    #[test]
    fn test_convert_empty_result() {
        let columns = create_test_columns();
        let converter = ArrowConverter::new(&columns).unwrap();

        let result_data = ResultData {
            columns: columns.clone(),
            rows: vec![],
            total_rows: 0,
        };

        let batch = converter.convert_to_record_batch(&result_data).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_convert_simple_result() {
        let columns = create_test_columns();
        let converter = ArrowConverter::new(&columns).unwrap();

        let result_data = ResultData {
            columns: columns.clone(),
            rows: vec![
                vec![json!(1), json!("Alice"), json!(true)],
                vec![json!(2), json!("Bob"), json!(false)],
                vec![json!(3), json!("Charlie"), json!(true)],
            ],
            total_rows: 3,
        };

        let batch = converter.convert_to_record_batch(&result_data).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_convert_with_nulls() {
        let columns = create_test_columns();
        let converter = ArrowConverter::new(&columns).unwrap();

        let result_data = ResultData {
            columns: columns.clone(),
            rows: vec![
                vec![json!(1), json!("Alice"), json!(true)],
                vec![json!(2), json!(null), json!(false)],
                vec![json!(null), json!("Charlie"), json!(null)],
            ],
            total_rows: 3,
        };

        let batch = converter.convert_to_record_batch(&result_data).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);

        // Check null counts
        assert_eq!(batch.column(0).null_count(), 1); // id has 1 null
        assert_eq!(batch.column(1).null_count(), 1); // name has 1 null
        assert_eq!(batch.column(2).null_count(), 1); // active has 1 null
    }

    #[test]
    fn test_convert_multiple_chunks() {
        let columns = create_test_columns();
        let converter = ArrowConverter::new(&columns).unwrap();

        let chunks = vec![
            ResultData {
                columns: columns.clone(),
                rows: vec![
                    vec![json!(1), json!("Alice"), json!(true)],
                    vec![json!(2), json!("Bob"), json!(false)],
                ],
                total_rows: 4,
            },
            ResultData {
                columns: columns.clone(),
                rows: vec![
                    vec![json!(3), json!("Charlie"), json!(true)],
                    vec![json!(4), json!("Dave"), json!(false)],
                ],
                total_rows: 4,
            },
        ];

        let batches = converter.convert_chunks(&chunks).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[test]
    fn test_schema_mismatch_error() {
        let columns = create_test_columns();
        let converter = ArrowConverter::new(&columns).unwrap();

        // Wrong number of columns in data
        let result_data = ResultData {
            columns: columns.clone(),
            rows: vec![vec![json!(1), json!("Alice")]], // Missing 'active' column
            total_rows: 1,
        };

        let result = converter.convert_to_record_batch(&result_data);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConversionError::SchemaMismatch(_)
        ));
    }

    #[test]
    fn test_parse_all_exasol_types() {
        let test_cases = vec![
            (
                "BOOLEAN",
                DataType {
                    type_name: "BOOLEAN".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            ),
            (
                "CHAR",
                DataType {
                    type_name: "CHAR".to_string(),
                    precision: None,
                    scale: None,
                    size: Some(10),
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            ),
            (
                "VARCHAR",
                DataType {
                    type_name: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                    size: Some(100),
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            ),
            (
                "DECIMAL",
                DataType {
                    type_name: "DECIMAL".to_string(),
                    precision: Some(18),
                    scale: Some(2),
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            ),
            (
                "DOUBLE",
                DataType {
                    type_name: "DOUBLE".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            ),
            (
                "DATE",
                DataType {
                    type_name: "DATE".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            ),
            (
                "TIMESTAMP",
                DataType {
                    type_name: "TIMESTAMP".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: Some(false),
                    fraction: None,
                },
            ),
        ];

        for (_name, data_type) in test_cases {
            let result = parse_exasol_type(&data_type);
            assert!(result.is_ok(), "Failed to parse: {:?}", data_type);
        }
    }

    #[test]
    fn test_transpose_rows_to_columns() {
        let rows = vec![
            vec![json!(1), json!("a"), json!(true)],
            vec![json!(2), json!("b"), json!(false)],
            vec![json!(3), json!("c"), json!(true)],
        ];

        let columns = transpose_rows_to_columns(&rows, 3);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].len(), 3); // First column has 3 values
        assert_eq!(columns[1].len(), 3); // Second column has 3 values
        assert_eq!(columns[2].len(), 3); // Third column has 3 values

        // Check values
        assert_eq!(columns[0][0], json!(1));
        assert_eq!(columns[0][1], json!(2));
        assert_eq!(columns[0][2], json!(3));

        assert_eq!(columns[1][0], json!("a"));
        assert_eq!(columns[1][1], json!("b"));
        assert_eq!(columns[1][2], json!("c"));
    }

    #[test]
    fn test_unsupported_type_error() {
        let data_type = DataType {
            type_name: "UNKNOWN_TYPE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = parse_exasol_type(&data_type);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConversionError::UnsupportedType { .. }
        ));
    }

    #[test]
    fn test_all_data_types_conversion() {
        let columns = vec![
            ColumnInfo {
                name: "bool_col".to_string(),
                data_type: DataType {
                    type_name: "BOOLEAN".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
            ColumnInfo {
                name: "decimal_col".to_string(),
                data_type: DataType {
                    type_name: "DECIMAL".to_string(),
                    precision: Some(10),
                    scale: Some(2),
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
            ColumnInfo {
                name: "double_col".to_string(),
                data_type: DataType {
                    type_name: "DOUBLE".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
            ColumnInfo {
                name: "date_col".to_string(),
                data_type: DataType {
                    type_name: "DATE".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            },
        ];

        let converter = ArrowConverter::new(&columns).unwrap();

        let result_data = ResultData {
            columns: columns.clone(),
            rows: vec![
                vec![
                    json!(true),
                    json!("123.45"),
                    json!(3.14159),
                    json!("2024-01-15"),
                ],
                vec![
                    json!(false),
                    json!("678.90"),
                    json!(2.71828),
                    json!("2024-02-20"),
                ],
            ],
            total_rows: 2,
        };

        let batch = converter.convert_to_record_batch(&result_data).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }
}
