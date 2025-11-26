//! Result set handling and iteration.
//!
//! This module provides types for handling query results, including
//! streaming result sets and metadata.

use crate::error::{ConversionError, QueryError};
use crate::transport::messages::{ColumnInfo, ResultData, ResultSetHandle};
use crate::transport::protocol::QueryResult as TransportQueryResult;
use crate::transport::TransportProtocol;
use crate::types::TypeMapper;
use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Metadata about a query execution.
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// Schema of the result set
    pub schema: Arc<Schema>,
    /// Total number of rows (if known)
    pub total_rows: Option<i64>,
    /// Number of columns
    pub column_count: usize,
    /// Execution time in milliseconds (if available)
    pub execution_time_ms: Option<u64>,
}

impl QueryMetadata {
    /// Create metadata from schema and row count.
    pub fn new(schema: Arc<Schema>, total_rows: Option<i64>) -> Self {
        Self {
            column_count: schema.fields().len(),
            schema,
            total_rows,
            execution_time_ms: None,
        }
    }

    /// Set execution time.
    pub fn with_execution_time(mut self, execution_time_ms: u64) -> Self {
        self.execution_time_ms = Some(execution_time_ms);
        self
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect()
    }

    /// Get column types.
    pub fn column_types(&self) -> Vec<&arrow::datatypes::DataType> {
        self.schema.fields().iter().map(|f| f.data_type()).collect()
    }
}

/// Query result set that can be either a row count or streaming data.
pub struct ResultSet {
    /// Result type
    inner: ResultSetInner,
    /// Transport reference for fetching more data
    #[allow(dead_code)]
    transport: Arc<Mutex<dyn TransportProtocol>>,
}

impl std::fmt::Debug for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultSet")
            .field("inner", &self.inner)
            .field("transport", &"<TransportProtocol>")
            .finish()
    }
}

#[derive(Debug)]
enum ResultSetInner {
    /// Row count result (INSERT, UPDATE, DELETE)
    RowCount { count: i64 },
    /// Streaming result set (SELECT)
    Stream {
        /// Result set handle for fetching
        handle: Option<ResultSetHandle>,
        /// Query metadata
        metadata: QueryMetadata,
        /// Buffered batches
        batches: Vec<RecordBatch>,
        /// Whether all data has been fetched
        complete: bool,
    },
}

impl ResultSet {
    /// Create a result set from a transport query result.
    pub(crate) fn from_transport_result(
        result: TransportQueryResult,
        transport: Arc<Mutex<dyn TransportProtocol>>,
    ) -> Result<Self, QueryError> {
        match result {
            TransportQueryResult::RowCount { count } => Ok(Self {
                inner: ResultSetInner::RowCount { count },
                transport,
            }),
            TransportQueryResult::ResultSet { handle, data } => {
                let schema = Self::build_schema(&data.columns)
                    .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

                let metadata = QueryMetadata::new(Arc::clone(&schema), Some(data.total_rows));

                // Convert initial data to RecordBatch
                let batches = if !data.rows.is_empty() {
                    vec![Self::rows_to_record_batch(&data, &schema)
                        .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?]
                } else {
                    Vec::new()
                };

                let complete = data.total_rows == data.rows.len() as i64;

                Ok(Self {
                    inner: ResultSetInner::Stream {
                        handle: Some(handle),
                        metadata,
                        batches,
                        complete,
                    },
                    transport,
                })
            }
        }
    }

    /// Get the row count if this is a row count result.
    pub fn row_count(&self) -> Option<i64> {
        match &self.inner {
            ResultSetInner::RowCount { count } => Some(*count),
            _ => None,
        }
    }

    /// Get the metadata if this is a streaming result.
    pub fn metadata(&self) -> Option<&QueryMetadata> {
        match &self.inner {
            ResultSetInner::Stream { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    /// Check if this is a streaming result set.
    pub fn is_stream(&self) -> bool {
        matches!(&self.inner, ResultSetInner::Stream { .. })
    }

    /// Convert to an iterator over RecordBatches.
    ///
    /// # Errors
    /// Returns `QueryError::NoResultSet` if this is not a streaming result.
    pub fn into_iterator(self) -> Result<ResultSetIterator, QueryError> {
        match self.inner {
            ResultSetInner::Stream {
                handle,
                metadata,
                batches,
                complete,
            } => Ok(ResultSetIterator {
                handle,
                transport: self.transport,
                metadata,
                batches,
                current_index: 0,
                complete,
            }),
            ResultSetInner::RowCount { .. } => Err(QueryError::NoResultSet(
                "Cannot iterate over row count result".to_string(),
            )),
        }
    }

    /// Fetch all remaining batches into memory.
    ///
    /// # Errors
    /// Returns `QueryError` if fetching fails or if this is not a streaming result.
    pub async fn fetch_all(mut self) -> Result<Vec<RecordBatch>, QueryError> {
        match &mut self.inner {
            ResultSetInner::Stream {
                handle,
                metadata,
                batches,
                complete,
            } => {
                if *complete {
                    return Ok(batches.clone());
                }

                let mut all_batches = batches.clone();

                // Fetch remaining data
                if let Some(handle_val) = handle {
                    loop {
                        let mut transport = self.transport.lock().await;
                        let result_data = transport
                            .fetch_results(*handle_val)
                            .await
                            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

                        if result_data.rows.is_empty() {
                            *complete = true;
                            break;
                        }

                        let batch = Self::rows_to_record_batch(&result_data, &metadata.schema)
                            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

                        all_batches.push(batch);

                        // Check if we've fetched everything
                        if result_data.total_rows > 0
                            && all_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                                >= result_data.total_rows as usize
                        {
                            *complete = true;
                            break;
                        }
                    }
                }

                *batches = all_batches.clone();
                Ok(all_batches)
            }
            ResultSetInner::RowCount { .. } => Err(QueryError::NoResultSet(
                "Cannot fetch batches from row count result".to_string(),
            )),
        }
    }

    /// Build Arrow schema from column information.
    fn build_schema(columns: &[ColumnInfo]) -> Result<Arc<Schema>, ConversionError> {
        let fields: Result<Vec<Field>, ConversionError> = columns
            .iter()
            .map(|col| {
                // Parse Exasol type from DataType
                let arrow_type = Self::exasol_datatype_to_arrow(&col.data_type)?;

                // All fields are nullable by default in Exasol
                Ok(Field::new(&col.name, arrow_type, true))
            })
            .collect();

        Ok(Arc::new(Schema::new(fields?)))
    }

    /// Convert Exasol DataType to Arrow DataType.
    fn exasol_datatype_to_arrow(
        data_type: &crate::transport::messages::DataType,
    ) -> Result<arrow::datatypes::DataType, ConversionError> {
        use crate::types::ExasolType;

        let exasol_type = match data_type.type_name.as_str() {
            "BOOLEAN" => ExasolType::Boolean,
            "CHAR" => ExasolType::Char {
                size: data_type.size.unwrap_or(1) as usize,
            },
            "VARCHAR" => ExasolType::Varchar {
                size: data_type.size.unwrap_or(2000000) as usize,
            },
            "DECIMAL" => ExasolType::Decimal {
                precision: data_type.precision.unwrap_or(18) as u8,
                scale: data_type.scale.unwrap_or(0) as i8,
            },
            "DOUBLE" => ExasolType::Double,
            "DATE" => ExasolType::Date,
            "TIMESTAMP" => ExasolType::Timestamp {
                with_local_time_zone: data_type.with_local_time_zone.unwrap_or(false),
            },
            "INTERVAL YEAR TO MONTH" => ExasolType::IntervalYearToMonth,
            "INTERVAL DAY TO SECOND" => ExasolType::IntervalDayToSecond {
                precision: data_type.fraction.unwrap_or(3) as u8,
            },
            "GEOMETRY" => ExasolType::Geometry { srid: None },
            "HASHTYPE" => ExasolType::Hashtype { byte_size: 16 },
            _ => {
                return Err(ConversionError::UnsupportedType {
                    exasol_type: data_type.type_name.clone(),
                })
            }
        };

        TypeMapper::exasol_to_arrow(&exasol_type, true)
    }

    /// Convert result data rows to RecordBatch.
    ///
    /// This is a simplified implementation for Phase 1.
    /// In production, use optimized builders for each data type.
    fn rows_to_record_batch(
        data: &ResultData,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, ConversionError> {
        use arrow::array::*;

        if data.rows.is_empty() {
            // Create empty batch with schema
            let empty_arrays: Vec<Arc<dyn arrow::array::Array>> = schema
                .fields()
                .iter()
                .map(|field| arrow::array::new_empty_array(field.data_type()))
                .collect();

            return RecordBatch::try_new(Arc::clone(schema), empty_arrays)
                .map_err(|e| ConversionError::ArrowError(e.to_string()));
        }

        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            use arrow::datatypes::DataType;

            // Build array based on data type
            let array: Arc<dyn arrow::array::Array> = match field.data_type() {
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else if let Some(b) = value.as_bool() {
                                builder.append_value(b);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::new();
                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else if let Some(i) = value.as_i64() {
                                builder.append_value(i as i32);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else if let Some(i) = value.as_i64() {
                                builder.append_value(i);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else if let Some(f) = value.as_f64() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else if let Some(s) = value.as_str() {
                                builder.append_value(s);
                            } else {
                                // Convert to string
                                builder.append_value(value.to_string());
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Decimal128(precision, scale) => {
                    let mut builder = Decimal128Builder::new()
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|e| ConversionError::ArrowError(e.to_string()))?;

                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else if let Some(i) = value.as_i64() {
                                // Scale the integer value
                                let scaled = i * 10i64.pow(*scale as u32);
                                builder.append_value(scaled as i128);
                            } else if let Some(f) = value.as_f64() {
                                let scaled = (f * 10f64.powi(*scale as i32)) as i128;
                                builder.append_value(scaled);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    // Fallback to string for unsupported types
                    let mut builder = StringBuilder::new();
                    for row in &data.rows {
                        if let Some(value) = row.get(col_idx) {
                            if value.is_null() {
                                builder.append_null();
                            } else {
                                builder.append_value(value.to_string());
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
            };

            arrays.push(array);
        }

        RecordBatch::try_new(Arc::clone(schema), arrays)
            .map_err(|e| ConversionError::ArrowError(e.to_string()))
    }
}

/// Iterator over RecordBatches from a result set.
///
/// Lazily fetches data from the transport as needed.
pub struct ResultSetIterator {
    /// Result set handle
    handle: Option<ResultSetHandle>,
    /// Transport reference
    transport: Arc<Mutex<dyn TransportProtocol>>,
    /// Query metadata
    metadata: QueryMetadata,
    /// Buffered batches
    batches: Vec<RecordBatch>,
    /// Current iteration index
    current_index: usize,
    /// Whether all data has been fetched
    complete: bool,
}

impl ResultSetIterator {
    /// Get the query metadata.
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Fetch the next batch from the transport.
    async fn fetch_next_batch(&mut self) -> Result<Option<RecordBatch>, QueryError> {
        if self.complete {
            return Ok(None);
        }

        let handle = match self.handle {
            Some(h) => h,
            None => {
                self.complete = true;
                return Ok(None);
            }
        };

        let mut transport = self.transport.lock().await;
        let result_data = transport
            .fetch_results(handle)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        if result_data.rows.is_empty() {
            self.complete = true;
            return Ok(None);
        }

        let batch = ResultSet::rows_to_record_batch(&result_data, &self.metadata.schema)
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        Ok(Some(batch))
    }

    /// Get the next batch synchronously (blocking).
    ///
    /// This is useful for implementing sync Iterator trait.
    pub fn next_batch(&mut self) -> Option<Result<RecordBatch, QueryError>> {
        // Return buffered batch if available
        if self.current_index < self.batches.len() {
            let batch = self.batches[self.current_index].clone();
            self.current_index += 1;
            return Some(Ok(batch));
        }

        // Need to fetch more data
        if self.complete {
            return None;
        }

        // Use block_on for async fetch (not ideal, but works for Phase 1)
        let runtime = tokio::runtime::Handle::try_current();
        if let Ok(handle) = runtime {
            let result = handle.block_on(self.fetch_next_batch());
            match result {
                Ok(Some(batch)) => {
                    self.batches.push(batch.clone());
                    self.current_index += 1;
                    Some(Ok(batch))
                }
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        } else {
            // No runtime available
            Some(Err(QueryError::InvalidState(
                "No async runtime available".to_string(),
            )))
        }
    }

    /// Close the result set and release resources.
    pub async fn close(mut self) -> Result<(), QueryError> {
        if let Some(handle) = self.handle.take() {
            let mut transport = self.transport.lock().await;
            transport
                .close_result_set(handle)
                .await
                .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;
        }
        Ok(())
    }
}

// Implement Iterator for ResultSetIterator
impl Iterator for ResultSetIterator {
    type Item = Result<RecordBatch, QueryError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::messages::{ColumnInfo, DataType};
    use crate::transport::protocol::QueryResult as TransportQueryResult;
    use async_trait::async_trait;
    use mockall::mock;

    // Mock transport for testing
    mock! {
        pub Transport {}

        #[async_trait]
        impl TransportProtocol for Transport {
            async fn connect(&mut self, params: &crate::transport::protocol::ConnectionParams) -> Result<(), crate::error::TransportError>;
            async fn authenticate(&mut self, credentials: &crate::transport::protocol::Credentials) -> Result<crate::transport::messages::SessionInfo, crate::error::TransportError>;
            async fn execute_query(&mut self, sql: &str) -> Result<TransportQueryResult, crate::error::TransportError>;
            async fn fetch_results(&mut self, handle: ResultSetHandle) -> Result<ResultData, crate::error::TransportError>;
            async fn close_result_set(&mut self, handle: ResultSetHandle) -> Result<(), crate::error::TransportError>;
            async fn close(&mut self) -> Result<(), crate::error::TransportError>;
            fn is_connected(&self) -> bool;
        }
    }

    #[tokio::test]
    async fn test_result_set_row_count() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let result = TransportQueryResult::RowCount { count: 42 };
        let result_set = ResultSet::from_transport_result(result, transport).unwrap();

        assert_eq!(result_set.row_count(), Some(42));
        assert!(!result_set.is_stream());
        assert!(result_set.metadata().is_none());
    }

    #[tokio::test]
    async fn test_result_set_stream() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let data = ResultData {
            columns: vec![
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
            ],
            rows: vec![
                vec![serde_json::json!(1), serde_json::json!("Alice")],
                vec![serde_json::json!(2), serde_json::json!("Bob")],
            ],
            total_rows: 2,
        };

        let result = TransportQueryResult::ResultSet {
            handle: ResultSetHandle::new(1),
            data,
        };

        let result_set = ResultSet::from_transport_result(result, transport).unwrap();

        assert!(result_set.row_count().is_none());
        assert!(result_set.is_stream());

        let metadata = result_set.metadata().unwrap();
        assert_eq!(metadata.column_count, 2);
        assert_eq!(metadata.total_rows, Some(2));
        assert_eq!(metadata.column_names(), vec!["id", "name"]);
    }

    #[tokio::test]
    async fn test_result_set_to_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Decimal128(18, 0), true),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
            Field::new("active", arrow::datatypes::DataType::Boolean, true),
        ]));

        let data = ResultData {
            columns: vec![],
            rows: vec![
                vec![
                    serde_json::json!(1),
                    serde_json::json!("Alice"),
                    serde_json::json!(true),
                ],
                vec![
                    serde_json::json!(2),
                    serde_json::json!("Bob"),
                    serde_json::json!(false),
                ],
            ],
            total_rows: 2,
        };

        let batch = ResultSet::rows_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_schema_building() {
        let columns = vec![
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
                name: "created_at".to_string(),
                data_type: DataType {
                    type_name: "TIMESTAMP".to_string(),
                    precision: None,
                    scale: None,
                    size: None,
                    character_set: None,
                    with_local_time_zone: Some(false),
                    fraction: None,
                },
            },
        ];

        let schema = ResultSet::build_schema(&columns).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "created_at");
    }

    #[test]
    fn test_query_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));

        let metadata = QueryMetadata::new(Arc::clone(&schema), Some(100)).with_execution_time(250);

        assert_eq!(metadata.column_count, 2);
        assert_eq!(metadata.total_rows, Some(100));
        assert_eq!(metadata.execution_time_ms, Some(250));
        assert_eq!(metadata.column_names(), vec!["id", "name"]);
    }
}
