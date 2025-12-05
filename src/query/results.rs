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
                // Note: data.data is in column-major format (each inner array is a column)
                let batches = if !data.data.is_empty() {
                    vec![Self::column_major_to_record_batch(&data, &schema)
                        .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?]
                } else {
                    Vec::new()
                };

                // Result is complete if all data was in initial response OR no handle provided
                // For column-major data, the number of rows is determined by the length of the first column
                let num_rows_received = if data.data.is_empty() {
                    0
                } else {
                    data.data[0].len() as i64
                };
                let complete = handle.is_none() || data.total_rows == num_rows_received;

                Ok(Self {
                    inner: ResultSetInner::Stream {
                        handle, // Already Option<ResultSetHandle>
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

                        if result_data.data.is_empty() {
                            *complete = true;
                            break;
                        }

                        let batch =
                            Self::column_major_to_record_batch(&result_data, &metadata.schema)
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

    /// Convert row-major result data to RecordBatch.
    ///
    /// Data is expected in row-major format where `data[row_idx][col_idx]` contains
    /// the value at that position.
    ///
    /// Example: For a result with 2 columns (id, name) and 3 rows:
    /// ```rust,ignore
    /// // data[row_idx][col_idx]
    /// data[0] = [1, "a"]    // Row 0
    /// data[1] = [2, "b"]    // Row 1
    /// data[2] = [3, "c"]    // Row 2
    /// ```
    fn column_major_to_record_batch(
        data: &ResultData,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, ConversionError> {
        use arrow::array::*;
        use serde_json::Value;

        if data.data.is_empty() {
            // Create empty batch with schema
            let empty_arrays: Vec<Arc<dyn Array>> = schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect();

            return RecordBatch::try_new(Arc::clone(schema), empty_arrays)
                .map_err(|e| ConversionError::ArrowError(e.to_string()));
        }

        // Extract column values from row-major data
        let num_columns = schema.fields().len();
        let column_values: Vec<Vec<&Value>> = (0..num_columns)
            .map(|col_idx| {
                data.data
                    .iter()
                    .map(|row| row.get(col_idx).unwrap_or(&Value::Null))
                    .collect()
            })
            .collect();

        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            use arrow::datatypes::DataType;

            // Get the column data (all values for this column)
            let col_values = &column_values[col_idx];

            // Build array based on data type
            let array: Arc<dyn Array> = match field.data_type() {
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(b) = value.as_bool() {
                            builder.append_value(b);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(i) = value.as_i64() {
                            builder.append_value(i as i32);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(i) = value.as_i64() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(f) = value.as_f64() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(s) = value.as_str() {
                            builder.append_value(s);
                        } else {
                            // Convert to string
                            builder.append_value(value.to_string());
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Decimal128(precision, scale) => {
                    let mut builder = Decimal128Builder::new()
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|e| ConversionError::ArrowError(e.to_string()))?;

                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(s) = value.as_str() {
                            // Parse string decimal (most common format from Exasol)
                            let scaled = Self::parse_string_to_decimal(s, *scale)?;
                            builder.append_value(scaled);
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
                    }
                    Arc::new(builder.finish())
                }
                DataType::Date32 => {
                    let mut builder = Date32Builder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(s) = value.as_str() {
                            // Parse date string "YYYY-MM-DD" to days since Unix epoch
                            match Self::parse_date_to_days(s) {
                                Ok(days) => builder.append_value(days),
                                Err(_) => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Timestamp(_, _) => {
                    let mut builder = TimestampMicrosecondBuilder::new();
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else if let Some(s) = value.as_str() {
                            // Parse timestamp string to microseconds since Unix epoch
                            match Self::parse_timestamp_to_micros(s) {
                                Ok(micros) => builder.append_value(micros),
                                Err(_) => builder.append_null(),
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
                    for value in col_values {
                        if value.is_null() {
                            builder.append_null();
                        } else {
                            builder.append_value(value.to_string());
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

    /// Parse a date string "YYYY-MM-DD" to days since Unix epoch (1970-01-01).
    fn parse_date_to_days(date_str: &str) -> Result<i32, ()> {
        let parts: Vec<&str> = date_str.split('-').collect();
        if parts.len() != 3 {
            return Err(());
        }

        let year: i32 = parts[0].parse().map_err(|_| ())?;
        let month: u32 = parts[1].parse().map_err(|_| ())?;
        let day: u32 = parts[2].parse().map_err(|_| ())?;

        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return Err(());
        }

        // Calculate days since Unix epoch
        let days_from_year =
            (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;
        let days_from_month = match month {
            1 => 0,
            2 => 31,
            3 => 59,
            4 => 90,
            5 => 120,
            6 => 151,
            7 => 181,
            8 => 212,
            9 => 243,
            10 => 273,
            11 => 304,
            12 => 334,
            _ => return Err(()),
        };

        // Add leap day if after February and leap year
        let is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        let leap_adjustment = if month > 2 && is_leap_year { 1 } else { 0 };

        Ok(days_from_year + days_from_month + day as i32 - 1 + leap_adjustment)
    }

    /// Parse a timestamp string to microseconds since Unix epoch.
    fn parse_timestamp_to_micros(timestamp_str: &str) -> Result<i64, ()> {
        // Split date and time
        let parts: Vec<&str> = timestamp_str.split(' ').collect();
        if parts.is_empty() {
            return Err(());
        }

        // Parse date part
        let days = Self::parse_date_to_days(parts[0])?;
        let mut micros = days as i64 * 86400 * 1_000_000;

        // Parse time part if present
        if parts.len() > 1 {
            let time_parts: Vec<&str> = parts[1].split(':').collect();
            if time_parts.len() >= 2 {
                let hours: i64 = time_parts[0].parse().map_err(|_| ())?;
                let minutes: i64 = time_parts[1].parse().map_err(|_| ())?;

                micros += hours * 3600 * 1_000_000;
                micros += minutes * 60 * 1_000_000;

                if time_parts.len() >= 3 {
                    // Parse seconds and microseconds
                    let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
                    let seconds: i64 = sec_parts[0].parse().map_err(|_| ())?;

                    micros += seconds * 1_000_000;

                    if sec_parts.len() > 1 {
                        // Parse fractional seconds (microseconds)
                        let frac = sec_parts[1];
                        let frac_micros = if frac.len() <= 6 {
                            let padding = 6 - frac.len();
                            let padded = format!("{}{}", frac, "0".repeat(padding));
                            padded.parse::<i64>().unwrap_or(0)
                        } else {
                            frac[..6].parse::<i64>().unwrap_or(0)
                        };
                        micros += frac_micros;
                    }
                }
            }
        }

        Ok(micros)
    }

    /// Parse a decimal string to i128 scaled value.
    ///
    /// Handles formats like "123", "123.45", "-123.45"
    fn parse_string_to_decimal(s: &str, scale: i8) -> Result<i128, ConversionError> {
        // Handle empty string
        if s.is_empty() {
            return Err(ConversionError::InvalidFormat(
                "Empty decimal string".to_string(),
            ));
        }

        // Split on decimal point
        let parts: Vec<&str> = s.split('.').collect();

        let (integer_part, decimal_part) = match parts.len() {
            1 => (parts[0], ""),
            2 => (parts[0], parts[1]),
            _ => {
                return Err(ConversionError::InvalidFormat(format!(
                    "Invalid decimal format: {}",
                    s
                )));
            }
        };

        // Parse the integer part
        let mut result: i128 = integer_part.parse().map_err(|_| {
            ConversionError::InvalidFormat(format!("Invalid integer part: {}", integer_part))
        })?;

        // Scale up by 10^scale
        result = result
            .checked_mul(10_i128.pow(scale as u32))
            .ok_or_else(|| ConversionError::InvalidFormat("Decimal overflow".to_string()))?;

        // Add the decimal part
        if !decimal_part.is_empty() {
            let decimal_digits = decimal_part.len().min(scale as usize);
            let decimal_value: i128 = decimal_part[..decimal_digits].parse().map_err(|_| {
                ConversionError::InvalidFormat(format!("Invalid decimal part: {}", decimal_part))
            })?;

            // Scale the decimal part appropriately
            let scale_diff = scale as usize - decimal_digits;
            let scaled_decimal = decimal_value * 10_i128.pow(scale_diff as u32);

            result = result
                .checked_add(if integer_part.starts_with('-') {
                    -scaled_decimal
                } else {
                    scaled_decimal
                })
                .ok_or_else(|| ConversionError::InvalidFormat("Decimal overflow".to_string()))?;
        }

        Ok(result)
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

        if result_data.data.is_empty() {
            self.complete = true;
            return Ok(None);
        }

        let batch = ResultSet::column_major_to_record_batch(&result_data, &self.metadata.schema)
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
    use crate::transport::protocol::{
        PreparedStatementHandle, QueryResult as TransportQueryResult,
    };
    use arrow::array::Array;
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
            async fn create_prepared_statement(&mut self, sql: &str) -> Result<PreparedStatementHandle, crate::error::TransportError>;
            async fn execute_prepared_statement(&mut self, handle: &PreparedStatementHandle, parameters: Option<Vec<Vec<serde_json::Value>>>) -> Result<TransportQueryResult, crate::error::TransportError>;
            async fn close_prepared_statement(&mut self, handle: &PreparedStatementHandle) -> Result<(), crate::error::TransportError>;
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

        // Row-major data format:
        // Row 0: [1, "Alice"]
        // Row 1: [2, "Bob"]
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
            data: vec![
                vec![serde_json::json!(1), serde_json::json!("Alice")],
                vec![serde_json::json!(2), serde_json::json!("Bob")],
            ],
            total_rows: 2,
        };

        let result = TransportQueryResult::ResultSet {
            handle: Some(ResultSetHandle::new(1)),
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

        // Row-major data format:
        // Row 0: [1, "Alice", true]
        // Row 1: [2, "Bob", false]
        let data = ResultData {
            columns: vec![],
            data: vec![
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

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_single_row_single_column() {
        // Test case for: SELECT 42 AS answer
        let schema = Arc::new(Schema::new(vec![Field::new(
            "answer",
            arrow::datatypes::DataType::Decimal128(18, 0),
            true,
        )]));

        // Row-major: one row with one value
        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(42)], // Row 0: [42]
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 1, "Should have exactly 1 row");
        assert_eq!(batch.num_columns(), 1, "Should have exactly 1 column");
    }

    #[tokio::test]
    async fn test_single_row_two_columns() {
        // Test case for: SELECT 42 AS answer, 'hello' AS greeting
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "answer",
                arrow::datatypes::DataType::Decimal128(18, 0),
                true,
            ),
            Field::new("greeting", arrow::datatypes::DataType::Utf8, true),
        ]));

        // Row-major: one row with two values
        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(42), serde_json::json!("hello")], // Row 0: [42, "hello"]
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 1, "Should have exactly 1 row");
        assert_eq!(batch.num_columns(), 2, "Should have exactly 2 columns");
    }

    #[tokio::test]
    async fn test_ten_rows_two_columns() {
        // Test case for: SELECT LEVEL AS id, 'Row ' || LEVEL AS label FROM DUAL CONNECT BY LEVEL <= 10
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Decimal128(18, 0), true),
            Field::new("label", arrow::datatypes::DataType::Utf8, true),
        ]));

        // Row-major: ten rows, each with two values
        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(1), serde_json::json!("Row 1")],
                vec![serde_json::json!(2), serde_json::json!("Row 2")],
                vec![serde_json::json!(3), serde_json::json!("Row 3")],
                vec![serde_json::json!(4), serde_json::json!("Row 4")],
                vec![serde_json::json!(5), serde_json::json!("Row 5")],
                vec![serde_json::json!(6), serde_json::json!("Row 6")],
                vec![serde_json::json!(7), serde_json::json!("Row 7")],
                vec![serde_json::json!(8), serde_json::json!("Row 8")],
                vec![serde_json::json!(9), serde_json::json!("Row 9")],
                vec![serde_json::json!(10), serde_json::json!("Row 10")],
            ],
            total_rows: 10,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 10, "Should have exactly 10 rows");
        assert_eq!(batch.num_columns(), 2, "Should have exactly 2 columns");
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

    // =========================================================================
    // Tests for QueryMetadata::column_types
    // =========================================================================

    #[test]
    fn test_query_metadata_column_types_returns_correct_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
            Field::new("active", arrow::datatypes::DataType::Boolean, true),
        ]));

        let metadata = QueryMetadata::new(Arc::clone(&schema), Some(100));
        let types = metadata.column_types();

        assert_eq!(types.len(), 3);
        assert_eq!(types[0], &arrow::datatypes::DataType::Int64);
        assert_eq!(types[1], &arrow::datatypes::DataType::Utf8);
        assert_eq!(types[2], &arrow::datatypes::DataType::Boolean);
    }

    #[test]
    fn test_query_metadata_column_types_empty_schema() {
        let fields: Vec<Field> = vec![];
        let schema = Arc::new(Schema::new(fields));

        let metadata = QueryMetadata::new(Arc::clone(&schema), None);
        let types = metadata.column_types();

        assert!(types.is_empty());
    }

    // =========================================================================
    // Tests for ResultSet::parse_date_to_days
    // =========================================================================

    #[test]
    fn test_parse_date_to_days_unix_epoch() {
        let result = ResultSet::parse_date_to_days("1970-01-01").unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_parse_date_to_days_after_epoch() {
        let result = ResultSet::parse_date_to_days("1970-01-02").unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_parse_date_to_days_year_2000() {
        // 2000-01-01 is 10957 days after Unix epoch
        let result = ResultSet::parse_date_to_days("2000-01-01").unwrap();
        assert_eq!(result, 10957);
    }

    #[test]
    fn test_parse_date_to_days_leap_year() {
        // 2000-03-01 should include Feb 29 (leap year)
        let result = ResultSet::parse_date_to_days("2000-03-01").unwrap();
        // 2000-01-01 = 10957, Jan has 31 days, Feb has 29 days (leap year)
        // 10957 + 31 + 29 = 11017
        assert_eq!(result, 11017);
    }

    #[test]
    fn test_parse_date_to_days_non_leap_year() {
        // 2001-03-01 should NOT include Feb 29 (non-leap year)
        let result = ResultSet::parse_date_to_days("2001-03-01").unwrap();
        // 2001-01-01 = 11323, Jan has 31 days, Feb has 28 days
        // 11323 + 31 + 28 = 11382
        assert_eq!(result, 11382);
    }

    #[test]
    fn test_parse_date_to_days_before_epoch() {
        // 1969-12-31 is -1 day before Unix epoch
        let result = ResultSet::parse_date_to_days("1969-12-31").unwrap();
        assert_eq!(result, -1);
    }

    #[test]
    fn test_parse_date_to_days_invalid_format_wrong_separator() {
        let result = ResultSet::parse_date_to_days("2000/01/01");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_format_missing_parts() {
        let result = ResultSet::parse_date_to_days("2000-01");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_month_zero() {
        let result = ResultSet::parse_date_to_days("2000-00-01");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_month_thirteen() {
        let result = ResultSet::parse_date_to_days("2000-13-01");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_day_zero() {
        let result = ResultSet::parse_date_to_days("2000-01-00");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_day_thirty_two() {
        let result = ResultSet::parse_date_to_days("2000-01-32");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_non_numeric() {
        let result = ResultSet::parse_date_to_days("YYYY-MM-DD");
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for ResultSet::parse_timestamp_to_micros
    // =========================================================================

    #[test]
    fn test_parse_timestamp_to_micros_date_only() {
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01").unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_time() {
        // 1970-01-01 01:00:00 = 1 hour = 3600 * 1_000_000 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 01:00:00").unwrap();
        assert_eq!(result, 3600 * 1_000_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_minutes() {
        // 1970-01-01 00:30:00 = 30 minutes = 30 * 60 * 1_000_000 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 00:30:00").unwrap();
        assert_eq!(result, 30 * 60 * 1_000_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_seconds() {
        // 1970-01-01 00:00:45 = 45 seconds = 45 * 1_000_000 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 00:00:45").unwrap();
        assert_eq!(result, 45 * 1_000_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_3_digits() {
        // 1970-01-01 00:00:00.123 = 123000 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 00:00:00.123").unwrap();
        assert_eq!(result, 123000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_6_digits() {
        // 1970-01-01 00:00:00.123456 = 123456 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 00:00:00.123456").unwrap();
        assert_eq!(result, 123456);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_more_than_6_digits() {
        // 1970-01-01 00:00:00.1234567 should truncate to 123456 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 00:00:00.1234567").unwrap();
        assert_eq!(result, 123456);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_1_digit() {
        // 1970-01-01 00:00:00.1 = 100000 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 00:00:00.1").unwrap();
        assert_eq!(result, 100000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_complex_timestamp() {
        // 2000-06-15 12:30:45.500
        // days = 11124 (from previous calculation for 2000-06-15)
        // time = 12*3600 + 30*60 + 45 seconds = 45045 seconds
        // micros from time = 45045 * 1_000_000 + 500000
        let result = ResultSet::parse_timestamp_to_micros("2000-06-15 12:30:45.500").unwrap();

        // Calculate expected value
        let days_micros: i64 =
            ResultSet::parse_date_to_days("2000-06-15").unwrap() as i64 * 86400 * 1_000_000;
        let time_micros: i64 = (12 * 3600 + 30 * 60 + 45) * 1_000_000 + 500000;
        assert_eq!(result, days_micros + time_micros);
    }

    #[test]
    fn test_parse_timestamp_to_micros_empty_string() {
        let result = ResultSet::parse_timestamp_to_micros("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp_to_micros_hours_and_minutes_only() {
        // 1970-01-01 01:30 (without seconds)
        // hours + minutes = 1*3600 + 30*60 = 5400 seconds = 5400 * 1_000_000 microseconds
        let result = ResultSet::parse_timestamp_to_micros("1970-01-01 01:30").unwrap();
        assert_eq!(result, 5400 * 1_000_000);
    }

    // =========================================================================
    // Tests for ResultSet::parse_string_to_decimal
    // =========================================================================

    #[test]
    fn test_parse_string_to_decimal_integer() {
        let result = ResultSet::parse_string_to_decimal("123", 2).unwrap();
        // 123 with scale 2 = 12300
        assert_eq!(result, 12300);
    }

    #[test]
    fn test_parse_string_to_decimal_with_decimal_point() {
        let result = ResultSet::parse_string_to_decimal("123.45", 2).unwrap();
        // 123.45 with scale 2 = 12345
        assert_eq!(result, 12345);
    }

    #[test]
    fn test_parse_string_to_decimal_negative() {
        let result = ResultSet::parse_string_to_decimal("-123.45", 2).unwrap();
        // -123.45 with scale 2 = -12345
        assert_eq!(result, -12345);
    }

    #[test]
    fn test_parse_string_to_decimal_zero_scale() {
        let result = ResultSet::parse_string_to_decimal("123", 0).unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn test_parse_string_to_decimal_high_scale() {
        let result = ResultSet::parse_string_to_decimal("1.5", 6).unwrap();
        // 1.5 with scale 6 = 1500000
        assert_eq!(result, 1500000);
    }

    #[test]
    fn test_parse_string_to_decimal_truncates_extra_decimals() {
        // When decimal part has more digits than scale
        let result = ResultSet::parse_string_to_decimal("1.123456", 3).unwrap();
        // 1.123 with scale 3 = 1123 (truncates extra digits)
        assert_eq!(result, 1123);
    }

    #[test]
    fn test_parse_string_to_decimal_zero() {
        let result = ResultSet::parse_string_to_decimal("0", 2).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_parse_string_to_decimal_negative_zero() {
        let result = ResultSet::parse_string_to_decimal("-0", 2).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_parse_string_to_decimal_empty_string() {
        let result = ResultSet::parse_string_to_decimal("", 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_string_to_decimal_multiple_decimal_points() {
        let result = ResultSet::parse_string_to_decimal("1.2.3", 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_string_to_decimal_non_numeric() {
        let result = ResultSet::parse_string_to_decimal("abc", 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_string_to_decimal_large_value() {
        let result = ResultSet::parse_string_to_decimal("999999999999999999", 0).unwrap();
        assert_eq!(result, 999999999999999999_i128);
    }

    // =========================================================================
    // Tests for ResultSet::exasol_datatype_to_arrow
    // =========================================================================

    #[test]
    fn test_exasol_datatype_to_arrow_boolean() {
        let data_type = DataType {
            type_name: "BOOLEAN".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Boolean);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_char() {
        let data_type = DataType {
            type_name: "CHAR".to_string(),
            precision: None,
            scale: None,
            size: Some(10),
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_char_default_size() {
        let data_type = DataType {
            type_name: "CHAR".to_string(),
            precision: None,
            scale: None,
            size: None, // Should default to 1
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_varchar() {
        let data_type = DataType {
            type_name: "VARCHAR".to_string(),
            precision: None,
            scale: None,
            size: Some(100),
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_varchar_default_size() {
        let data_type = DataType {
            type_name: "VARCHAR".to_string(),
            precision: None,
            scale: None,
            size: None, // Should default to 2000000
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_decimal() {
        let data_type = DataType {
            type_name: "DECIMAL".to_string(),
            precision: Some(18),
            scale: Some(2),
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Decimal128(18, 2));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_decimal_default_precision_scale() {
        let data_type = DataType {
            type_name: "DECIMAL".to_string(),
            precision: None, // Should default to 18
            scale: None,     // Should default to 0
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Decimal128(18, 0));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_double() {
        let data_type = DataType {
            type_name: "DOUBLE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Float64);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_date() {
        let data_type = DataType {
            type_name: "DATE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Date32);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_timestamp_without_tz() {
        let data_type = DataType {
            type_name: "TIMESTAMP".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: Some(false),
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert!(matches!(
            result,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        ));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_timestamp_with_tz() {
        let data_type = DataType {
            type_name: "TIMESTAMP".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: Some(true),
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert!(matches!(
            result,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_))
        ));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_timestamp_default_tz() {
        let data_type = DataType {
            type_name: "TIMESTAMP".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None, // Should default to false
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert!(matches!(
            result,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        ));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_interval_year_to_month() {
        let data_type = DataType {
            type_name: "INTERVAL YEAR TO MONTH".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert!(matches!(
            result,
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
        ));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_interval_day_to_second() {
        let data_type = DataType {
            type_name: "INTERVAL DAY TO SECOND".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: Some(6),
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert!(matches!(
            result,
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
        ));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_interval_day_to_second_default_fraction() {
        let data_type = DataType {
            type_name: "INTERVAL DAY TO SECOND".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None, // Should default to 3
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert!(matches!(
            result,
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
        ));
    }

    #[test]
    fn test_exasol_datatype_to_arrow_geometry() {
        let data_type = DataType {
            type_name: "GEOMETRY".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Binary);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_hashtype() {
        let data_type = DataType {
            type_name: "HASHTYPE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type).unwrap();
        assert_eq!(result, arrow::datatypes::DataType::Binary);
    }

    #[test]
    fn test_exasol_datatype_to_arrow_unsupported_type() {
        let data_type = DataType {
            type_name: "UNKNOWN_TYPE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };

        let result = ResultSet::exasol_datatype_to_arrow(&data_type);
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for ResultSet::column_major_to_record_batch with various types
    // =========================================================================

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(null)],
                vec![serde_json::json!(4)],
            ],
            total_rows: 4,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 1);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(array.value(0), 1);
        assert_eq!(array.value(1), 2);
        assert!(array.is_null(2));
        assert_eq!(array.value(3), 4);
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(9223372036854775807_i64)], // Max i64
                vec![serde_json::json!(-9223372036854775808_i64)], // Min i64
                vec![serde_json::json!(null)],
            ],
            total_rows: 3,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 3);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(array.value(0), 9223372036854775807_i64);
        assert_eq!(array.value(1), -9223372036854775808_i64);
        assert!(array.is_null(2));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Float64,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(3.14159)],
                vec![serde_json::json!(-2.71828)],
                vec![serde_json::json!(null)],
                vec![serde_json::json!(0.0)],
            ],
            total_rows: 4,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 4);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((array.value(0) - 3.14159).abs() < 0.00001);
        assert!((array.value(1) - (-2.71828)).abs() < 0.00001);
        assert!(array.is_null(2));
        assert!((array.value(3) - 0.0).abs() < 0.00001);
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_boolean() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            arrow::datatypes::DataType::Boolean,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(true)],
                vec![serde_json::json!(false)],
                vec![serde_json::json!(null)],
            ],
            total_rows: 3,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 3);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(array.value(0));
        assert!(!array.value(1));
        assert!(array.is_null(2));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_date32() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "date",
            arrow::datatypes::DataType::Date32,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("1970-01-01")],
                vec![serde_json::json!("2000-01-01")],
                vec![serde_json::json!(null)],
            ],
            total_rows: 3,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 3);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(array.value(0), 0); // Unix epoch
        assert_eq!(array.value(1), 10957); // 2000-01-01
        assert!(array.is_null(2));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_timestamp() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("1970-01-01 00:00:00")],
                vec![serde_json::json!("1970-01-01 01:00:00.123456")],
                vec![serde_json::json!(null)],
            ],
            total_rows: 3,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 3);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(array.value(0), 0);
        // 1 hour + 123456 microseconds
        assert_eq!(array.value(1), 3600 * 1_000_000 + 123456);
        assert!(array.is_null(2));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_empty_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, true),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));

        let data = ResultData {
            columns: vec![],
            data: vec![],
            total_rows: 0,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_with_utf8_string() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "text",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("hello")],
                vec![serde_json::json!("world")],
                vec![serde_json::json!(null)],
                vec![serde_json::json!("")], // Empty string
            ],
            total_rows: 4,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 4);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(array.value(0), "hello");
        assert_eq!(array.value(1), "world");
        assert!(array.is_null(2));
        assert_eq!(array.value(3), "");
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_utf8_from_non_string_value() {
        // Test that non-string JSON values are converted to string
        let schema = Arc::new(Schema::new(vec![Field::new(
            "text",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(123)], // Number should be converted to "123"
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(array.value(0), "123");
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_decimal_from_string() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            arrow::datatypes::DataType::Decimal128(18, 2),
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("123.45")],
                vec![serde_json::json!("-67.89")],
                vec![serde_json::json!(null)],
            ],
            total_rows: 3,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 3);

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), 12345); // 123.45 with scale 2
        assert_eq!(array.value(1), -6789); // -67.89 with scale 2
        assert!(array.is_null(2));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_decimal_from_integer() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            arrow::datatypes::DataType::Decimal128(18, 2),
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(100)], // Integer should be scaled
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), 10000); // 100 with scale 2 = 10000
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_decimal_from_float() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            arrow::datatypes::DataType::Decimal128(18, 2),
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!(99.99)], // Float should be scaled
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), 9999); // 99.99 with scale 2 = 9999
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_invalid_date_becomes_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "date",
            arrow::datatypes::DataType::Date32,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("invalid-date")],
                vec![serde_json::json!(123)], // Non-string should become null
            ],
            total_rows: 2,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        // Invalid date formats become null
        assert!(array.is_null(0));
        assert!(array.is_null(1));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_invalid_timestamp_becomes_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("not-a-timestamp")],
                vec![serde_json::json!(12345)], // Non-string should become null
            ],
            total_rows: 2,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert!(array.is_null(0));
        assert!(array.is_null(1));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_boolean_null_from_non_bool() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            arrow::datatypes::DataType::Boolean,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("not-a-bool")], // String is not a bool
                vec![serde_json::json!(123)],          // Number is not a bool
            ],
            total_rows: 2,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        // Non-bool values become null
        assert!(array.is_null(0));
        assert!(array.is_null(1));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_int32_null_from_non_int() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("not-an-int")], // String is not parseable as int
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert!(array.is_null(0));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_float64_null_from_non_float() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Float64,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![
                vec![serde_json::json!("not-a-float")], // String is not parseable as float
            ],
            total_rows: 1,
        };

        let batch = ResultSet::column_major_to_record_batch(&data, &schema).unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(array.is_null(0));
    }

    #[tokio::test]
    async fn test_column_major_to_record_batch_unsupported_type_returns_error() {
        // Test that unsupported types (like LargeUtf8) result in an ArrowError
        // because the fallback creates a StringArray which doesn't match the schema
        let schema = Arc::new(Schema::new(vec![Field::new(
            "large_text",
            arrow::datatypes::DataType::LargeUtf8,
            true,
        )]));

        let data = ResultData {
            columns: vec![],
            data: vec![vec![serde_json::json!("test_data")]],
            total_rows: 1,
        };

        let result = ResultSet::column_major_to_record_batch(&data, &schema);

        // Unsupported types cause an error because the fallback creates a StringArray
        // which doesn't match the expected LargeUtf8 type in the schema
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for ResultSetIterator::metadata
    // =========================================================================

    #[tokio::test]
    async fn test_result_set_iterator_metadata() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let data = ResultData {
            columns: vec![ColumnInfo {
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
            }],
            data: vec![vec![serde_json::json!(1)]],
            total_rows: 1,
        };

        let result = TransportQueryResult::ResultSet {
            handle: Some(ResultSetHandle::new(1)),
            data,
        };

        let result_set = ResultSet::from_transport_result(result, transport).unwrap();
        let iterator = result_set.into_iterator().unwrap();

        let metadata = iterator.metadata();
        assert_eq!(metadata.column_count, 1);
        assert_eq!(metadata.total_rows, Some(1));
        assert_eq!(metadata.column_names(), vec!["id"]);
    }

    #[tokio::test]
    async fn test_result_set_iterator_metadata_multiple_columns() {
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
            ],
            data: vec![vec![
                serde_json::json!(1),
                serde_json::json!("Alice"),
                serde_json::json!(true),
            ]],
            total_rows: 1,
        };

        let result = TransportQueryResult::ResultSet {
            handle: Some(ResultSetHandle::new(1)),
            data,
        };

        let result_set = ResultSet::from_transport_result(result, transport).unwrap();
        let iterator = result_set.into_iterator().unwrap();

        let metadata = iterator.metadata();
        assert_eq!(metadata.column_count, 3);
        assert_eq!(metadata.column_names(), vec!["id", "name", "active"]);

        let types = metadata.column_types();
        assert_eq!(types.len(), 3);
        assert!(matches!(
            types[0],
            arrow::datatypes::DataType::Decimal128(18, 0)
        ));
        assert!(matches!(types[1], arrow::datatypes::DataType::Utf8));
        assert!(matches!(types[2], arrow::datatypes::DataType::Boolean));
    }
}
