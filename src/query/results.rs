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
}
