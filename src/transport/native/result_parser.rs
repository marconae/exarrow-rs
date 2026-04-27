use crate::error::TransportError;

use super::constants::*;

/// Parsed column metadata from a native protocol result set.
#[derive(Debug, Clone)]
pub struct NativeColumnMeta {
    pub name: String,
    pub type_id: u32,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub is_varchar: bool,
    pub max_len: Option<i32>,
}

/// Parsed response from the native protocol.
#[derive(Debug)]
pub enum NativeResponse {
    ResultSet {
        handle: i32,
        columns: Vec<NativeColumnMeta>,
        batch: Option<arrow::record_batch::RecordBatch>,
        total_rows: i64,
        rows_received: i64,
    },
    RowCount(i64),
    Empty,
    StillExecuting,
    MoreRows(Vec<u8>),
    Exception {
        message: String,
        sql_state: String,
    },
}

/// Non-fatal warning returned alongside a terminal result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeWarning {
    pub message: String,
    pub sql_state: String,
}

/// Parsed native response envelope containing warnings and one terminal result.
#[derive(Debug)]
pub struct NativeResponseEnvelope {
    pub warnings: Vec<NativeWarning>,
    pub terminal: NativeResponse,
}

/// Parse a response payload (after the 21-byte header) into a structured response.
pub fn parse_response(data: &[u8]) -> Result<NativeResponseEnvelope, TransportError> {
    if data.is_empty() {
        return Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: NativeResponse::Empty,
        });
    }

    if data.len() == 4 && i32::from_le_bytes([data[0], data[1], data[2], data[3]]) == 0 {
        return Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: NativeResponse::Empty,
        });
    }

    if let Some(envelope) = try_parse_counted_envelope(data)? {
        return Ok(envelope);
    }

    parse_legacy_response(data)
}

fn try_parse_counted_envelope(
    data: &[u8],
) -> Result<Option<NativeResponseEnvelope>, TransportError> {
    if data.len() < 5 {
        return Ok(None);
    }

    let mut probe_offset = 0;
    let result_count = read_i32(data, &mut probe_offset)?;
    if !(1..=64).contains(&result_count) {
        return Ok(None);
    }

    let first_part_type = data[probe_offset] as i8;
    if !is_known_response_type(first_part_type) {
        return Ok(None);
    }

    let mut offset = 0;
    let result_count = read_i32(data, &mut offset)?;

    let mut warnings = Vec::new();
    let mut terminal = None;

    for part_idx in 0..(result_count as usize) {
        let result_type = read_u8(data, &mut offset)? as i8;
        match result_type {
            R_WARNING => warnings.push(parse_warning(data, &mut offset)?),
            R_COLUMN_COUNT => {
                let _ = read_i32(data, &mut offset)?;
            }
            R_RESULT_SET => {
                let response = parse_result_set_at(data, &mut offset)?;
                assign_terminal_result(&mut terminal, response)?;
            }
            R_HANDLE => {
                let response = parse_handle_only_at(data, &mut offset)?;
                assign_terminal_result(&mut terminal, response)?;
            }
            R_ROW_COUNT => {
                let response = parse_row_count_at(data, &mut offset)?;
                assign_terminal_result(&mut terminal, response)?;
            }
            R_EXCEPTION => {
                let response = parse_exception_at(data, &mut offset)?;
                assign_terminal_result(&mut terminal, response)?;
            }
            R_EMPTY => {
                assign_terminal_result(&mut terminal, NativeResponse::Empty)?;
            }
            R_STILL_EXECUTING => {
                assign_terminal_result(&mut terminal, NativeResponse::StillExecuting)?;
            }
            R_MORE_ROWS => {
                if part_idx + 1 != result_count as usize {
                    return Err(TransportError::ProtocolError(
                        "MoreRows must be the final native response part".into(),
                    ));
                }
                let response = NativeResponse::MoreRows(data[offset..].to_vec());
                assign_terminal_result(&mut terminal, response)?;
                offset = data.len();
            }
            _ => {
                let preview_end = data.len().min(64);
                return Err(TransportError::ProtocolError(format!(
                    "Unknown response type: {} at offset {} (data {:02x?})",
                    result_type,
                    offset.saturating_sub(1),
                    &data[..preview_end]
                )));
            }
        }
    }

    if offset != data.len() {
        return Ok(None);
    }

    Ok(Some(NativeResponseEnvelope {
        warnings,
        terminal: terminal.unwrap_or(NativeResponse::Empty),
    }))
}

fn parse_legacy_response(data: &[u8]) -> Result<NativeResponseEnvelope, TransportError> {
    let mut offset = 0;
    let response = parse_legacy_response_at(data, &mut offset)?;
    if offset != data.len() {
        return Err(TransportError::ProtocolError(format!(
            "Trailing bytes after legacy response: remaining {:02x?}",
            &data[offset..]
        )));
    }
    Ok(response)
}

fn parse_legacy_response_at(
    data: &[u8],
    offset: &mut usize,
) -> Result<NativeResponseEnvelope, TransportError> {
    let result_type = read_u8(data, offset)? as i8;
    parse_legacy_response_body(data, offset, result_type)
}

fn parse_legacy_response_body(
    data: &[u8],
    offset: &mut usize,
    result_type: i8,
) -> Result<NativeResponseEnvelope, TransportError> {
    match result_type {
        R_RESULT_SET => Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: parse_result_set_at(data, offset)?,
        }),
        R_HANDLE => Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: parse_handle_only_at(data, offset)?,
        }),
        R_ROW_COUNT => Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: parse_row_count_at(data, offset)?,
        }),
        R_EXCEPTION => Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: parse_exception_at(data, offset)?,
        }),
        R_WARNING => Ok(NativeResponseEnvelope {
            warnings: { vec![parse_warning(data, offset)?] },
            terminal: NativeResponse::Empty,
        }),
        R_COLUMN_COUNT => {
            let _ = read_i32(data, offset)?;
            Ok(NativeResponseEnvelope {
                warnings: Vec::new(),
                terminal: NativeResponse::Empty,
            })
        }
        R_EMPTY => Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: NativeResponse::Empty,
        }),
        R_STILL_EXECUTING => Ok(NativeResponseEnvelope {
            warnings: Vec::new(),
            terminal: {
                consume_status_message(data, offset)?;
                NativeResponse::StillExecuting
            },
        }),
        R_MORE_ROWS => {
            let remaining = data[*offset..].to_vec();
            *offset = data.len();
            Ok(NativeResponseEnvelope {
                warnings: Vec::new(),
                terminal: NativeResponse::MoreRows(remaining),
            })
        }
        _ => Err(TransportError::ProtocolError(format!(
            "Unknown response type: {}",
            result_type
        ))),
    }
}

fn is_known_response_type(result_type: i8) -> bool {
    matches!(
        result_type,
        R_RESULT_SET
            | R_HANDLE
            | R_ROW_COUNT
            | R_COLUMN_COUNT
            | R_WARNING
            | R_STILL_EXECUTING
            | R_MORE_ROWS
            | R_EXCEPTION
            | R_EMPTY
    )
}

fn consume_status_message(data: &[u8], offset: &mut usize) -> Result<(), TransportError> {
    if *offset >= data.len() {
        return Ok(());
    }
    let msg_len = read_i32(data, offset)? as usize;
    if *offset + msg_len > data.len() {
        return Err(TransportError::ProtocolError(
            "StillExecuting status message truncated".into(),
        ));
    }
    *offset += msg_len;
    Ok(())
}

fn assign_terminal_result(
    slot: &mut Option<NativeResponse>,
    response: NativeResponse,
) -> Result<(), TransportError> {
    if slot.is_some() {
        return Err(TransportError::ProtocolError(
            "Multiple terminal results in native response envelope".into(),
        ));
    }
    *slot = Some(response);
    Ok(())
}

/// Parse a handle-only response (R_HANDLE = 2).
///
/// Used for CREATE PREPARED responses containing the statement handle
/// and optional column/parameter metadata as a sub-result.
/// Format: [statement_handle:4 LE] [sub_result_type:1] [sub_result_data...]
fn parse_handle_only_at(data: &[u8], offset: &mut usize) -> Result<NativeResponse, TransportError> {
    let statement_handle = read_i32(data, offset)?;

    // Check for sub-result (parameter/column metadata)
    let mut parameter_description = None;
    let mut result_columns = None;
    if *offset < data.len() {
        while *offset < data.len() {
            let sub_response = parse_legacy_response_at(data, offset)?;
            if let NativeResponse::ResultSet {
                handle: sub_handle,
                columns,
                ..
            } = sub_response.terminal
            {
                if sub_handle == PARAMETER_DESCRIPTION {
                    parameter_description = Some((sub_handle, columns));
                } else {
                    result_columns = Some((sub_handle, columns));
                }
            }
        }
    }

    if let Some((sub_handle, columns)) = parameter_description.or(result_columns) {
        // The sub-result's handle indicates the kind of metadata:
        // - PARAMETER_DESCRIPTION (-5): parameter metadata
        // - SMALL_RESULTSET (-3): result column metadata
        return Ok(NativeResponse::ResultSet {
            handle: statement_handle,
            columns,
            batch: None,
            total_rows: sub_handle as i64,
            rows_received: 0,
        });
    }

    // No parameter metadata
    Ok(NativeResponse::ResultSet {
        handle: statement_handle,
        columns: Vec::new(),
        batch: None,
        total_rows: 0,
        rows_received: 0,
    })
}

#[cfg(test)]
fn parse_result_set(data: &[u8]) -> Result<NativeResponse, TransportError> {
    parse_result_set_at_end(data)
}

#[cfg(test)]
fn parse_result_set_at_end(data: &[u8]) -> Result<NativeResponse, TransportError> {
    let mut offset = 0;
    let response = parse_result_set_at(data, &mut offset)?;
    if offset != data.len() {
        return Err(TransportError::ProtocolError(format!(
            "Trailing bytes after result-set response: remaining {:02x?}",
            &data[offset..]
        )));
    }
    Ok(response)
}

fn parse_result_set_at(data: &[u8], offset: &mut usize) -> Result<NativeResponse, TransportError> {
    let handle = read_i32(data, offset)?;
    let num_columns = read_i32(data, offset)? as usize;
    let total_rows = read_i64(data, offset)?;
    let rows_received = read_i64(data, offset)?;
    if rows_received < 0 {
        return Err(TransportError::ProtocolError(format!(
            "Invalid row count from server: {}",
            rows_received
        )));
    }

    let mut columns = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        columns.push(parse_column_meta(data, offset)?);
    }

    let batch = if num_columns == 0 {
        None
    } else {
        Some(build_batch_from_wire(
            data,
            offset,
            &columns,
            rows_received as usize,
        )?)
    };

    Ok(NativeResponse::ResultSet {
        handle,
        columns,
        batch,
        total_rows,
        rows_received,
    })
}

fn parse_column_meta(data: &[u8], offset: &mut usize) -> Result<NativeColumnMeta, TransportError> {
    let name_len = read_i32(data, offset)? as usize;
    if *offset + name_len > data.len() {
        return Err(TransportError::ProtocolError(
            "Column name truncated".into(),
        ));
    }
    let name = std::str::from_utf8(&data[*offset..*offset + name_len])
        .map_err(|e| TransportError::ProtocolError(format!("Invalid column name UTF-8: {}", e)))?
        .to_owned();
    *offset += name_len;

    let type_id = read_i32(data, offset)? as u32;

    let mut precision = None;
    let mut scale = None;
    let mut is_varchar = false;
    let mut max_len = None;

    match type_id {
        T_DECIMAL | T_SMALLDECIMAL | T_BIGDECIMAL => {
            precision = Some(read_i32(data, offset)?);
            scale = Some(read_i32(data, offset)?);
        }
        T_CHAR | T_GEOMETRY | T_HASHTYPE => {
            // All string-like types: vcFlag(1) + maxLen(4) + octetLen(4)
            if *offset >= data.len() {
                return Err(TransportError::ProtocolError(
                    "CHAR metadata truncated".into(),
                ));
            }
            let vc_flag = data[*offset];
            *offset += 1;
            is_varchar = (vc_flag & IS_VARCHAR) != 0;
            max_len = Some(read_i32(data, offset)?);
            // octet_len
            let _octet_len = read_i32(data, offset)?;
        }
        T_INTERVAL_YEAR => {
            // vcFlag(1) + maxLen(4) + octetLen(4)
            if *offset >= data.len() {
                return Err(TransportError::ProtocolError(
                    "INTERVAL metadata truncated".into(),
                ));
            }
            let vc_flag = data[*offset];
            *offset += 1;
            is_varchar = (vc_flag & IS_VARCHAR) != 0;
            max_len = Some(read_i32(data, offset)?);
            let _octet_len = read_i32(data, offset)?;
            // Protocol v19+: datetimeIntervalPrecision(4)
            let _interval_precision = read_i32(data, offset)?;
        }
        T_INTERVAL_DAY => {
            // vcFlag(1) + maxLen(4) + octetLen(4)
            if *offset >= data.len() {
                return Err(TransportError::ProtocolError(
                    "INTERVAL metadata truncated".into(),
                ));
            }
            let vc_flag = data[*offset];
            *offset += 1;
            is_varchar = (vc_flag & IS_VARCHAR) != 0;
            max_len = Some(read_i32(data, offset)?);
            let _octet_len = read_i32(data, offset)?;
            // Protocol v19+: datetimeIntervalPrecision(4) + datetimeIntervalFraction(4)
            let _interval_precision = read_i32(data, offset)?;
            let _interval_fraction = read_i32(data, offset)?;
        }
        T_TIMESTAMP | T_TIMESTAMP_LOCAL_TZ | T_TIMESTAMP_UTC => {
            // Protocol v19+: precision(4)
            let _ts_precision = read_i32(data, offset)?;
        }
        _ => {}
    }

    Ok(NativeColumnMeta {
        name,
        type_id,
        precision,
        scale,
        is_varchar,
        max_len,
    })
}

fn parse_row_count_at(data: &[u8], offset: &mut usize) -> Result<NativeResponse, TransportError> {
    let count = read_i64(data, offset)?;
    Ok(NativeResponse::RowCount(count))
}

fn parse_exception_at(data: &[u8], offset: &mut usize) -> Result<NativeResponse, TransportError> {
    let warning = parse_message_part(data, offset)?;
    Ok(NativeResponse::Exception {
        message: warning.message,
        sql_state: warning.sql_state,
    })
}

fn parse_warning(data: &[u8], offset: &mut usize) -> Result<NativeWarning, TransportError> {
    parse_message_part(data, offset)
}

fn parse_message_part(data: &[u8], offset: &mut usize) -> Result<NativeWarning, TransportError> {
    let msg_len = read_i32(data, offset)? as usize;
    if *offset + msg_len > data.len() {
        return Err(TransportError::ProtocolError(
            "Exception message truncated".into(),
        ));
    }
    let message = std::str::from_utf8(&data[*offset..*offset + msg_len])
        .map_err(|e| TransportError::ProtocolError(format!("Invalid exception UTF-8: {}", e)))?
        .to_owned();
    *offset += msg_len;

    let sql_state = if *offset + 5 <= data.len() {
        std::str::from_utf8(&data[*offset..*offset + 5])
            .unwrap_or("?????")
            .to_owned()
    } else {
        "?????".to_owned()
    };
    if *offset + 5 <= data.len() {
        *offset += 5;
    }

    Ok(NativeWarning { message, sql_state })
}

/// Build an Arrow `RecordBatch` directly from native wire bytes in a single pass,
/// appending values straight into typed Arrow builders. DATE → Date32,
/// TIMESTAMP → TimestampMicrosecond; no intermediate string allocations.
fn build_batch_from_wire(
    data: &[u8],
    offset: &mut usize,
    column_metas: &[NativeColumnMeta],
    num_rows: usize,
) -> Result<arrow::record_batch::RecordBatch, TransportError> {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::Schema;

    if column_metas.is_empty() {
        let schema = Arc::new(Schema::empty());
        return arrow::record_batch::RecordBatch::try_new_with_options(
            schema,
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .map_err(|e| {
            TransportError::ProtocolError(format!("Failed to create empty RecordBatch: {}", e))
        });
    }

    let mut fields = Vec::with_capacity(column_metas.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_metas.len());

    for meta in column_metas {
        let (field, array) = fill_column_builder(data, offset, meta, num_rows)?;
        fields.push(field);
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    arrow::record_batch::RecordBatch::try_new(schema, arrays)
        .map_err(|e| TransportError::ProtocolError(format!("Failed to create RecordBatch: {}", e)))
}

/// Extract Arrow Decimal128 precision and scale from column metadata, clamping
/// to the valid Arrow range (precision 1..=38, scale in i8) and defaulting
/// precision when absent.
fn decimal_precision_scale(meta: &NativeColumnMeta, default_precision: i32) -> (u8, i8) {
    let raw_precision = meta.precision.unwrap_or(default_precision);
    let precision = raw_precision.clamp(1, 38) as u8;
    let raw_scale = meta.scale.unwrap_or(0);
    let scale = raw_scale.clamp(i8::MIN as i32, i8::MAX as i32) as i8;
    (precision, scale)
}

/// Parse one column worth of wire bytes directly into an Arrow array.
fn fill_column_builder(
    data: &[u8],
    offset: &mut usize,
    meta: &NativeColumnMeta,
    num_rows: usize,
) -> Result<
    (
        arrow::datatypes::Field,
        std::sync::Arc<dyn arrow::array::Array>,
    ),
    TransportError,
> {
    use std::sync::Arc;

    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
        Int32Builder, Int64Builder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit};

    match meta.type_id {
        T_DOUBLE => {
            let mut b = Float64Builder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_f64(data, offset)?);
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Float64, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_REAL => {
            let mut b = Float64Builder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_f32(data, offset)? as f64);
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Float64, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_INTEGER => {
            let mut b = Int64Builder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_i64(data, offset)?);
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Int64, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_SMALLINT => {
            let mut b = Int32Builder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_i32(data, offset)?);
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Int32, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_BOOLEAN => {
            let mut b = BooleanBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_u8(data, offset)? != 0);
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Boolean, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_BINARY => {
            let mut b = BinaryBuilder::with_capacity(num_rows, num_rows * 16);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    let len = read_i32(data, offset)? as usize;
                    if *offset + len > data.len() {
                        return Err(TransportError::ProtocolError(
                            "Binary data truncated".into(),
                        ));
                    }
                    b.append_value(&data[*offset..*offset + len]);
                    *offset += len;
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Binary, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_SMALLDECIMAL => {
            let (precision, scale) = decimal_precision_scale(meta, 9);
            let dt = ArrowDataType::Decimal128(precision, scale);
            let mut b = Decimal128Builder::with_capacity(num_rows).with_data_type(dt.clone());
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_i32(data, offset)? as i128);
                }
            }
            let field = Field::new(&meta.name, dt, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_DECIMAL => {
            let (precision, scale) = decimal_precision_scale(meta, 18);
            let dt = ArrowDataType::Decimal128(precision, scale);
            let mut b = Decimal128Builder::with_capacity(num_rows).with_data_type(dt.clone());
            let use_i32 = meta.precision.unwrap_or(18) <= 9;
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else if use_i32 {
                    b.append_value(read_i32(data, offset)? as i128);
                } else {
                    b.append_value(read_i64(data, offset)? as i128);
                }
            }
            let field = Field::new(&meta.name, dt, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_BIGDECIMAL => {
            let (precision, scale) = decimal_precision_scale(meta, 36);
            let dt = ArrowDataType::Decimal128(precision, scale);
            let mut b = Decimal128Builder::with_capacity(num_rows).with_data_type(dt.clone());
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    b.append_value(read_i128(data, offset)?);
                }
            }
            let field = Field::new(&meta.name, dt, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_DATE => {
            let mut b = Date32Builder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    let packed = read_i32(data, offset)?;
                    let year = packed >> 16;
                    let month = ((packed >> 8) & 0xFF) as u32;
                    let day = (packed & 0xFF) as u32;
                    b.append_value(crate::types::conversion::ymd_to_days(year, month, day));
                }
            }
            let field = Field::new(&meta.name, ArrowDataType::Date32, true);
            Ok((field, Arc::new(b.finish())))
        }
        T_TIMESTAMP | T_TIMESTAMP_LOCAL_TZ | T_TIMESTAMP_UTC => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                if read_u8(data, offset)? == 0 {
                    b.append_null();
                } else {
                    let year = read_i16(data, offset)? as i32;
                    let month = read_u8(data, offset)? as u32;
                    let day = read_u8(data, offset)? as u32;
                    let hour = read_u8(data, offset)? as u64;
                    let minute = read_u8(data, offset)? as u64;
                    let second = read_u8(data, offset)? as u64;
                    let nanos = read_i32(data, offset)?;
                    b.append_value(crate::types::conversion::ymd_hms_nanos_to_micros(
                        year, month, day, hour, minute, second, nanos,
                    ));
                }
            }
            let finished = b.finish();
            let (data_type, array): (ArrowDataType, std::sync::Arc<dyn arrow::array::Array>) =
                if meta.type_id == T_TIMESTAMP_UTC {
                    let with_tz = finished.with_timezone("UTC");
                    (
                        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        Arc::new(with_tz),
                    )
                } else {
                    (
                        ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                        Arc::new(finished),
                    )
                };
            let field = Field::new(&meta.name, data_type, true);
            Ok((field, array))
        }
        T_CHAR | T_GEOMETRY | T_HASHTYPE | T_INTERVAL_YEAR | T_INTERVAL_DAY => {
            fill_string_builder(data, offset, &meta.name, num_rows)
        }
        _ => fill_string_builder(data, offset, &meta.name, num_rows),
    }
}

/// Specialised length-prefixed string path that appends directly to a
/// `StringBuilder` without allocating an owned `String` per value.
fn fill_string_builder(
    data: &[u8],
    offset: &mut usize,
    name: &str,
    num_rows: usize,
) -> Result<
    (
        arrow::datatypes::Field,
        std::sync::Arc<dyn arrow::array::Array>,
    ),
    TransportError,
> {
    use std::sync::Arc;

    use arrow::array::StringBuilder;
    use arrow::datatypes::{DataType as ArrowDataType, Field};

    let mut b = StringBuilder::with_capacity(num_rows, num_rows * 16);
    for _ in 0..num_rows {
        if read_u8(data, offset)? == 0 {
            b.append_null();
        } else {
            let len = read_i32(data, offset)? as usize;
            if *offset + len > data.len() {
                return Err(TransportError::ProtocolError(
                    "String data truncated".into(),
                ));
            }
            let s = std::str::from_utf8(&data[*offset..*offset + len]).map_err(|e| {
                TransportError::ProtocolError(format!("Invalid UTF-8 in string: {}", e))
            })?;
            b.append_value(s);
            *offset += len;
        }
    }
    let field = Field::new(name, ArrowDataType::Utf8, true);
    Ok((field, Arc::new(b.finish())))
}

pub fn parse_fetch_to_record_batch(
    data: &[u8],
    columns: &[NativeColumnMeta],
) -> Result<(i64, arrow::record_batch::RecordBatch), TransportError> {
    let mut offset = 0;
    let rows_received = read_i64(data, &mut offset)?;
    if rows_received < 0 {
        return Err(TransportError::ProtocolError(format!(
            "Invalid row count from server: {}",
            rows_received
        )));
    }
    let batch = build_batch_from_wire(data, &mut offset, columns, rows_received as usize)?;
    Ok((rows_received, batch))
}

// --- Primitive readers ---

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, TransportError> {
    if *offset >= data.len() {
        return Err(TransportError::ProtocolError("Data truncated (u8)".into()));
    }
    let v = data[*offset];
    *offset += 1;
    Ok(v)
}

fn read_i16(data: &[u8], offset: &mut usize) -> Result<i16, TransportError> {
    if *offset + 2 > data.len() {
        return Err(TransportError::ProtocolError("Data truncated (i16)".into()));
    }
    let v = i16::from_le_bytes([data[*offset], data[*offset + 1]]);
    *offset += 2;
    Ok(v)
}

fn read_i32(data: &[u8], offset: &mut usize) -> Result<i32, TransportError> {
    if *offset + 4 > data.len() {
        return Err(TransportError::ProtocolError("Data truncated (i32)".into()));
    }
    let v = i32::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ]);
    *offset += 4;
    Ok(v)
}

fn read_i64(data: &[u8], offset: &mut usize) -> Result<i64, TransportError> {
    if *offset + 8 > data.len() {
        return Err(TransportError::ProtocolError("Data truncated (i64)".into()));
    }
    let v = i64::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    Ok(v)
}

fn read_f64(data: &[u8], offset: &mut usize) -> Result<f64, TransportError> {
    if *offset + 8 > data.len() {
        return Err(TransportError::ProtocolError("Data truncated (f64)".into()));
    }
    let v = f64::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    Ok(v)
}

fn read_f32(data: &[u8], offset: &mut usize) -> Result<f32, TransportError> {
    if *offset + 4 > data.len() {
        return Err(TransportError::ProtocolError("Data truncated (f32)".into()));
    }
    let v = f32::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ]);
    *offset += 4;
    Ok(v)
}

fn read_i128(data: &[u8], offset: &mut usize) -> Result<i128, TransportError> {
    if *offset + 16 > data.len() {
        return Err(TransportError::ProtocolError(
            "Data truncated (i128)".into(),
        ));
    }
    let v = i128::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
        data[*offset + 8],
        data[*offset + 9],
        data[*offset + 10],
        data[*offset + 11],
        data[*offset + 12],
        data[*offset + 13],
        data[*offset + 14],
        data[*offset + 15],
    ]);
    *offset += 16;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_response() {
        let resp = parse_response(&[]).unwrap();
        assert!(matches!(resp.terminal, NativeResponse::Empty));
    }

    #[test]
    fn parse_zero_result_count_response() {
        let resp = parse_response(&0i32.to_le_bytes()).unwrap();
        assert!(matches!(resp.terminal, NativeResponse::Empty));
    }

    #[test]
    fn parse_result_count_with_still_executing() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_le_bytes());
        data.push(R_STILL_EXECUTING as u8);

        let resp = parse_response(&data).unwrap();
        assert!(matches!(resp.terminal, NativeResponse::StillExecuting));
    }

    #[test]
    fn parse_result_count_with_row_count_response() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_le_bytes());
        data.push(R_ROW_COUNT as u8);
        data.extend_from_slice(&42i64.to_le_bytes());
        let resp = parse_response(&data).unwrap();
        match resp.terminal {
            NativeResponse::RowCount(c) => assert_eq!(c, 42),
            _ => panic!("Expected RowCount"),
        }
    }

    #[test]
    fn parse_result_count_with_exception_response() {
        let msg = "test error";
        let state = "42000";
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_le_bytes());
        data.push(R_EXCEPTION as u8);
        data.extend_from_slice(&(msg.len() as i32).to_le_bytes());
        data.extend_from_slice(msg.as_bytes());
        data.extend_from_slice(state.as_bytes());

        let resp = parse_response(&data).unwrap();
        match resp.terminal {
            NativeResponse::Exception { message, sql_state } => {
                assert_eq!(message, "test error");
                assert_eq!(sql_state, "42000");
            }
            _ => panic!("Expected Exception"),
        }
    }

    /// Helper: write column metadata bytes for a given type.
    fn write_col_meta(buf: &mut Vec<u8>, name: &str, type_id: u32, extra: &[u8]) {
        buf.extend_from_slice(&(name.len() as i32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&(type_id as i32).to_le_bytes());
        buf.extend_from_slice(extra);
    }

    fn envelope(parts: &[Vec<u8>]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(parts.len() as i32).to_le_bytes());
        for part in parts {
            data.extend_from_slice(part);
        }
        data
    }

    fn warning_part(message: &str, sql_state: &str) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(R_WARNING as u8);
        data.extend_from_slice(&(message.len() as i32).to_le_bytes());
        data.extend_from_slice(message.as_bytes());
        data.extend_from_slice(sql_state.as_bytes());
        data
    }

    fn row_count_part(count: i64) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(R_ROW_COUNT as u8);
        data.extend_from_slice(&count.to_le_bytes());
        data
    }

    fn empty_part() -> Vec<u8> {
        vec![R_EMPTY as u8]
    }

    fn column_count_part(count: i32) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(R_COLUMN_COUNT as u8);
        data.extend_from_slice(&count.to_le_bytes());
        data
    }

    fn result_set_part(body: Vec<u8>) -> Vec<u8> {
        let mut data = Vec::with_capacity(body.len() + 1);
        data.push(R_RESULT_SET as u8);
        data.extend_from_slice(&body);
        data
    }

    /// Helper: build a minimal result set payload (after the result-type byte).
    /// Contains only column metadata (0 rows) so no column data section.
    fn build_result_set_header(columns: &[(&str, u32, Vec<u8>)]) -> Vec<u8> {
        let mut data = Vec::new();
        // handle
        data.extend_from_slice(&(-3i32).to_le_bytes()); // SMALL_RESULTSET
                                                        // num_columns
        data.extend_from_slice(&(columns.len() as i32).to_le_bytes());
        // total_rows
        data.extend_from_slice(&0i64.to_le_bytes());
        // rows_received
        data.extend_from_slice(&0i64.to_le_bytes());
        // column metadata
        for (name, type_id, extra) in columns {
            write_col_meta(&mut data, name, *type_id, extra);
        }
        data
    }

    #[test]
    fn parse_result_count_with_empty_response() {
        let resp = parse_response(&envelope(&[empty_part()])).unwrap();
        assert!(matches!(resp.terminal, NativeResponse::Empty));
    }

    #[test]
    fn parse_warning_then_row_count_response() {
        let resp = parse_response(&envelope(&[
            warning_part("careful", "01000"),
            row_count_part(7),
        ]))
        .unwrap();

        assert_eq!(resp.warnings.len(), 1);
        assert_eq!(resp.warnings[0].message, "careful");
        assert_eq!(resp.warnings[0].sql_state, "01000");
        assert!(matches!(resp.terminal, NativeResponse::RowCount(7)));
    }

    #[test]
    fn parse_warning_then_result_set_response() {
        let resp = parse_response(&envelope(&[
            warning_part("heads up", "01000"),
            result_set_part(build_result_set_header(&[("ID", T_INTEGER, Vec::new())])),
        ]))
        .unwrap();

        assert_eq!(resp.warnings.len(), 1);
        match resp.terminal {
            NativeResponse::ResultSet { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "ID");
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    #[test]
    fn parse_column_count_then_empty_response() {
        let resp = parse_response(&envelope(&[column_count_part(3), empty_part()])).unwrap();
        assert!(matches!(resp.terminal, NativeResponse::Empty));
    }

    #[test]
    fn parse_column_meta_timestamp_reads_precision() {
        // TIMESTAMP metadata in protocol v19+: type_id(4) + precision(4)
        let mut ts_extra = Vec::new();
        ts_extra.extend_from_slice(&3i32.to_le_bytes()); // precision = 3

        let columns = vec![("created_at", T_TIMESTAMP, ts_extra)];
        let data = build_result_set_header(&columns);
        let result = parse_result_set(&data).unwrap();

        match result {
            NativeResponse::ResultSet { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "created_at");
                assert_eq!(columns[0].type_id, T_TIMESTAMP);
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    #[test]
    fn parse_column_meta_timestamp_utc_reads_precision() {
        let mut ts_extra = Vec::new();
        ts_extra.extend_from_slice(&6i32.to_le_bytes()); // precision = 6

        let columns = vec![("ts_utc", T_TIMESTAMP_UTC, ts_extra)];
        let data = build_result_set_header(&columns);
        let result = parse_result_set(&data).unwrap();

        match result {
            NativeResponse::ResultSet { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "ts_utc");
                assert_eq!(columns[0].type_id, T_TIMESTAMP_UTC);
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    #[test]
    fn parse_column_meta_interval_year_reads_char_and_precision() {
        // INTERVAL YEAR TO MONTH: vcFlag(1) + maxLen(4) + octetLen(4) + intervalPrecision(4)
        let mut extra = Vec::new();
        extra.push(0u8); // vcFlag
        extra.extend_from_slice(&13i32.to_le_bytes()); // maxLen
        extra.extend_from_slice(&13i32.to_le_bytes()); // octetLen
        extra.extend_from_slice(&2i32.to_le_bytes()); // intervalPrecision

        let columns = vec![("interval_col", T_INTERVAL_YEAR, extra)];
        let data = build_result_set_header(&columns);
        let result = parse_result_set(&data).unwrap();

        match result {
            NativeResponse::ResultSet { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "interval_col");
                assert_eq!(columns[0].type_id, T_INTERVAL_YEAR);
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    #[test]
    fn parse_column_meta_interval_day_reads_char_and_two_precisions() {
        // INTERVAL DAY TO SECOND: vcFlag(1) + maxLen(4) + octetLen(4)
        //   + intervalPrecision(4) + intervalFraction(4)
        let mut extra = Vec::new();
        extra.push(0u8); // vcFlag
        extra.extend_from_slice(&29i32.to_le_bytes()); // maxLen
        extra.extend_from_slice(&29i32.to_le_bytes()); // octetLen
        extra.extend_from_slice(&2i32.to_le_bytes()); // intervalPrecision
        extra.extend_from_slice(&3i32.to_le_bytes()); // intervalFraction

        let columns = vec![("interval_day_col", T_INTERVAL_DAY, extra)];
        let data = build_result_set_header(&columns);
        let result = parse_result_set(&data).unwrap();

        match result {
            NativeResponse::ResultSet { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "interval_day_col");
                assert_eq!(columns[0].type_id, T_INTERVAL_DAY);
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    #[test]
    fn parse_column_meta_geometry_reads_char_metadata() {
        // GEOMETRY: vcFlag(1) + maxLen(4) + octetLen(4)
        let mut extra = Vec::new();
        extra.push(0u8); // vcFlag (no varchar, no UTF-8)
        extra.extend_from_slice(&100i32.to_le_bytes()); // maxLen
        extra.extend_from_slice(&100i32.to_le_bytes()); // octetLen

        let columns = vec![("geom_col", T_GEOMETRY, extra)];
        let data = build_result_set_header(&columns);
        let result = parse_result_set(&data).unwrap();

        match result {
            NativeResponse::ResultSet { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "geom_col");
                assert_eq!(columns[0].type_id, T_GEOMETRY);
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    #[test]
    fn parse_multi_column_benchmark_table_metadata() {
        // Simulate the benchmark table:
        // id BIGINT, name VARCHAR(100), email VARCHAR(200), age INTEGER,
        // salary DECIMAL(12,2), created_at TIMESTAMP, is_active BOOLEAN,
        // description VARCHAR(1000)

        let mut columns = Vec::new();

        // BIGINT -> DECIMAL(18,0) on the wire
        let mut decimal_extra = Vec::new();
        decimal_extra.extend_from_slice(&18i32.to_le_bytes()); // precision
        decimal_extra.extend_from_slice(&0i32.to_le_bytes()); // scale
        columns.push(("ID", T_DECIMAL, decimal_extra));

        // VARCHAR(100)
        let mut vc_extra = Vec::new();
        vc_extra.push(IS_VARCHAR | IS_UTF8); // vcFlag
        vc_extra.extend_from_slice(&100i32.to_le_bytes()); // maxLen
        vc_extra.extend_from_slice(&400i32.to_le_bytes()); // octetLen (UTF-8 = 4x)
        columns.push(("NAME", T_CHAR, vc_extra));

        // VARCHAR(200)
        let mut vc_extra2 = Vec::new();
        vc_extra2.push(IS_VARCHAR | IS_UTF8);
        vc_extra2.extend_from_slice(&200i32.to_le_bytes());
        vc_extra2.extend_from_slice(&800i32.to_le_bytes());
        columns.push(("EMAIL", T_CHAR, vc_extra2));

        // INTEGER -> DECIMAL(18,0) on the wire
        let mut int_extra = Vec::new();
        int_extra.extend_from_slice(&18i32.to_le_bytes());
        int_extra.extend_from_slice(&0i32.to_le_bytes());
        columns.push(("AGE", T_DECIMAL, int_extra));

        // DECIMAL(12,2)
        let mut dec_extra = Vec::new();
        dec_extra.extend_from_slice(&12i32.to_le_bytes());
        dec_extra.extend_from_slice(&2i32.to_le_bytes());
        columns.push(("SALARY", T_DECIMAL, dec_extra));

        // TIMESTAMP with precision field (protocol v19+)
        let mut ts_extra = Vec::new();
        ts_extra.extend_from_slice(&3i32.to_le_bytes()); // precision
        columns.push(("CREATED_AT", T_TIMESTAMP, ts_extra));

        // BOOLEAN (no extra metadata)
        columns.push(("IS_ACTIVE", T_BOOLEAN, Vec::new()));

        // VARCHAR(1000)
        let mut vc_extra3 = Vec::new();
        vc_extra3.push(IS_VARCHAR | IS_UTF8);
        vc_extra3.extend_from_slice(&1000i32.to_le_bytes());
        vc_extra3.extend_from_slice(&4000i32.to_le_bytes());
        columns.push(("DESCRIPTION", T_CHAR, vc_extra3));

        let data = build_result_set_header(&columns);
        let result = parse_result_set(&data).unwrap();

        match result {
            NativeResponse::ResultSet {
                columns: cols,
                total_rows,
                rows_received,
                ..
            } => {
                assert_eq!(cols.len(), 8);
                assert_eq!(cols[0].name, "ID");
                assert_eq!(cols[0].type_id, T_DECIMAL);
                assert_eq!(cols[1].name, "NAME");
                assert_eq!(cols[1].type_id, T_CHAR);
                assert!(cols[1].is_varchar);
                assert_eq!(cols[2].name, "EMAIL");
                assert_eq!(cols[3].name, "AGE");
                assert_eq!(cols[4].name, "SALARY");
                assert_eq!(cols[4].precision, Some(12));
                assert_eq!(cols[4].scale, Some(2));
                assert_eq!(cols[5].name, "CREATED_AT");
                assert_eq!(cols[5].type_id, T_TIMESTAMP);
                assert_eq!(cols[6].name, "IS_ACTIVE");
                assert_eq!(cols[6].type_id, T_BOOLEAN);
                assert_eq!(cols[7].name, "DESCRIPTION");
                assert_eq!(cols[7].type_id, T_CHAR);
                assert!(cols[7].is_varchar);
                assert_eq!(total_rows, 0);
                assert_eq!(rows_received, 0);
            }
            _ => panic!("Expected ResultSet"),
        }
    }

    fn meta(
        name: &str,
        type_id: u32,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> NativeColumnMeta {
        NativeColumnMeta {
            name: name.to_string(),
            type_id,
            precision,
            scale,
            is_varchar: false,
            max_len: None,
        }
    }

    #[test]
    fn single_pass_date_column_decodes_to_date32() {
        use arrow::array::{Array, Date32Array};
        let columns = vec![meta("d", T_DATE, None, None)];
        let mut data = Vec::new();
        // Row 0: 2024-01-02 → packed i32
        let packed = (2024i32 << 16) | (1 << 8) | 2;
        data.push(1u8);
        data.extend_from_slice(&packed.to_le_bytes());
        // Row 1: null
        data.push(0u8);

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 2).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);

        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(arr.is_null(1));
        let expected = crate::types::conversion::ymd_to_days(2024, 1, 2);
        assert_eq!(arr.value(0), expected);
        assert_eq!(offset, data.len());
    }

    #[test]
    fn single_pass_timestamp_column_decodes_to_microseconds() {
        use arrow::array::{Array, TimestampMicrosecondArray};

        let columns = vec![meta("ts", T_TIMESTAMP, None, None)];
        let mut data = Vec::new();
        // Row 0: 1970-01-01 00:00:00.123
        data.push(1u8);
        data.extend_from_slice(&1970i16.to_le_bytes());
        data.push(1u8);
        data.push(1u8);
        data.push(0u8);
        data.push(0u8);
        data.push(0u8);
        data.extend_from_slice(&123_000_000i32.to_le_bytes()); // 123 ms → 123_000 micros
                                                               // Row 1: null
        data.push(0u8);

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 2).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), 123_000);
        assert!(arr.is_null(1));
    }

    #[test]
    fn single_pass_timestamp_utc_has_utc_timezone() {
        use arrow::array::Array;
        use arrow::datatypes::{DataType, TimeUnit};

        let columns = vec![meta("ts", T_TIMESTAMP_UTC, None, None)];
        let mut data = Vec::new();
        data.push(1u8);
        data.extend_from_slice(&1970i16.to_le_bytes());
        data.push(1u8);
        data.push(1u8);
        data.push(0u8);
        data.push(0u8);
        data.push(0u8);
        data.extend_from_slice(&0i32.to_le_bytes());

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 1).unwrap();
        let arr = batch.column(0);
        assert_eq!(
            arr.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn single_pass_string_column_decodes_utf8_without_owning_string() {
        use arrow::array::{Array, StringArray};

        let columns = vec![meta("s", T_CHAR, None, None)];
        let mut data = Vec::new();
        // Row 0: "hello"
        data.push(1u8);
        data.extend_from_slice(&5i32.to_le_bytes());
        data.extend_from_slice(b"hello");
        // Row 1: null
        data.push(0u8);

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 2).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello");
        assert!(arr.is_null(1));
    }

    #[test]
    fn single_pass_all_primitive_types_offsets_align() {
        // DOUBLE + BOOLEAN + INTEGER with 2 rows each, mixed nulls.
        let columns = vec![
            meta("d", T_DOUBLE, None, None),
            meta("b", T_BOOLEAN, None, None),
            meta("i", T_INTEGER, None, None),
        ];
        let mut data = Vec::new();
        // DOUBLE column
        data.push(1u8);
        data.extend_from_slice(&1.5f64.to_le_bytes());
        data.push(0u8); // null

        // BOOLEAN column
        data.push(1u8);
        data.push(1u8); // true
        data.push(0u8); // null

        // INTEGER column
        data.push(1u8);
        data.extend_from_slice(&42i64.to_le_bytes());
        data.push(0u8); // null

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 2).unwrap();
        assert_eq!(offset, data.len(), "all bytes should be consumed");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        use arrow::array::{Array, BooleanArray, Float64Array, Int64Array};
        let f = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let i = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(f.value(0), 1.5);
        assert!(f.is_null(1));
        assert!(b.value(0));
        assert!(b.is_null(1));
        assert_eq!(i.value(0), 42);
        assert!(i.is_null(1));
    }

    #[test]
    fn single_pass_empty_rows_produces_typed_empty_batch() {
        let columns = vec![
            meta("i", T_INTEGER, None, None),
            meta("s", T_CHAR, None, None),
        ];
        let data = Vec::new();
        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 0).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        use arrow::datatypes::DataType;
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn single_pass_binary_column_copies_into_builder() {
        use arrow::array::{Array, BinaryArray};
        let columns = vec![meta("bin", T_BINARY, None, None)];
        let mut data = Vec::new();
        data.push(1u8);
        data.extend_from_slice(&3i32.to_le_bytes());
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE]);
        data.push(0u8);

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 2).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(arr.value(0), &[0xDE, 0xAD, 0xBE][..]);
        assert!(arr.is_null(1));
    }

    #[test]
    fn parse_fetch_to_record_batch_reads_row_count_prefix() {
        use arrow::array::{Array, Int64Array};
        let columns = vec![meta("i", T_INTEGER, None, None)];
        let mut data = Vec::new();
        // rows_received prefix
        data.extend_from_slice(&2i64.to_le_bytes());
        // Row 0: 100
        data.push(1u8);
        data.extend_from_slice(&100i64.to_le_bytes());
        // Row 1: null
        data.push(0u8);

        let (rows, batch) = parse_fetch_to_record_batch(&data, &columns).unwrap();
        assert_eq!(rows, 2);
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100);
        assert!(arr.is_null(1));
    }

    #[test]
    fn single_pass_decimal_scale_zero_maps_to_decimal128() {
        use arrow::array::Decimal128Array;
        use arrow::datatypes::DataType;

        let columns = vec![meta("big", T_DECIMAL, Some(18), Some(0))];
        let mut data = Vec::new();
        data.push(1u8);
        data.extend_from_slice(&12345i64.to_le_bytes());

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 1).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(18, 0)
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345i128);
    }

    #[test]
    fn single_pass_decimal_with_scale_maps_to_decimal128_preserving_precision() {
        use arrow::array::Decimal128Array;
        use arrow::datatypes::DataType;

        let columns = vec![meta("s", T_DECIMAL, Some(12), Some(2))];
        let mut data = Vec::new();
        // precision <= 9 would be i32; precision 12 uses i64
        data.push(1u8);
        data.extend_from_slice(&12345i64.to_le_bytes());

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 1).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(12, 2)
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345i128);
        assert_eq!(arr.value_as_string(0), "123.45");
    }

    #[test]
    fn single_pass_smalldecimal_uses_i32_wire_and_decimal128() {
        use arrow::array::{Array, Decimal128Array};
        use arrow::datatypes::DataType;

        let columns = vec![meta("s", T_SMALLDECIMAL, Some(5), Some(2))];
        let mut data = Vec::new();
        data.push(1u8);
        data.extend_from_slice(&12345i32.to_le_bytes());
        data.push(0u8); // null

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 2).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(5, 2)
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345i128);
        assert!(arr.is_null(1));
        assert_eq!(offset, data.len());
    }

    #[test]
    fn single_pass_decimal_precision_le_9_uses_i32_wire() {
        use arrow::array::Decimal128Array;
        use arrow::datatypes::DataType;

        let columns = vec![meta("d", T_DECIMAL, Some(9), Some(3))];
        let mut data = Vec::new();
        data.push(1u8);
        data.extend_from_slice(&1_234_567i32.to_le_bytes());

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 1).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(9, 3)
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1_234_567i128);
        assert_eq!(offset, data.len());
    }

    #[test]
    fn single_pass_bigdecimal_preserves_i128() {
        use arrow::array::Decimal128Array;
        use arrow::datatypes::DataType;

        // i128 value larger than i64::MAX would overflow under the old path.
        let big: i128 = (i64::MAX as i128) + 1;
        let columns = vec![meta("bd", T_BIGDECIMAL, Some(36), Some(0))];
        let mut data = Vec::new();
        data.push(1u8);
        data.extend_from_slice(&big.to_le_bytes());

        let mut offset = 0;
        let batch = build_batch_from_wire(&data, &mut offset, &columns, 1).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(36, 0)
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), big);
        assert_eq!(offset, data.len());
    }
}
