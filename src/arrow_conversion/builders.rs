//! Array builders for converting JSON values to Arrow arrays.
//!
//! This module provides type-specific builders that convert columnar JSON data
//! into Arrow arrays with proper NULL handling.

use crate::error::ConversionError;
use crate::types::ExasolType;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
    IntervalMonthDayNanoBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use serde_json::Value;
use std::sync::Arc;

/// Trait for building Arrow arrays from JSON values.
pub trait ArrayBuilder {
    /// Append a JSON value to the array builder.
    ///
    /// # Arguments
    /// * `value` - The JSON value to append (or null)
    /// * `row` - The row index for error reporting
    /// * `column` - The column index for error reporting
    fn append_value(
        &mut self,
        value: &Value,
        row: usize,
        column: usize,
    ) -> Result<(), ConversionError>;

    /// Finish building and return the Arrow array.
    fn finish(&mut self) -> Result<ArrayRef, ConversionError>;
}

/// Build an Arrow array from JSON values for a specific Exasol type.
///
/// # Arguments
/// * `exasol_type` - The Exasol data type
/// * `values` - Column of JSON values (one per row)
/// * `column` - Column index for error reporting
///
/// # Returns
/// An Arrow ArrayRef containing the converted values
pub fn build_array(
    exasol_type: &ExasolType,
    values: &[Value],
    column: usize,
) -> Result<ArrayRef, ConversionError> {
    match exasol_type {
        ExasolType::Boolean => build_boolean_array(values, column),
        ExasolType::Char { .. } | ExasolType::Varchar { .. } => build_string_array(values, column),
        // Exasol DECIMAL precision is 1-36; always fits in Decimal128
        ExasolType::Decimal { precision, scale } => {
            build_decimal128_array(values, *precision, *scale, column)
        }
        ExasolType::Double => build_double_array(values, column),
        ExasolType::Date => build_date_array(values, column),
        ExasolType::Timestamp {
            with_local_time_zone,
        } => build_timestamp_array(values, *with_local_time_zone, column),
        ExasolType::IntervalYearToMonth => build_interval_year_to_month_array(values, column),
        ExasolType::IntervalDayToSecond { .. } => {
            build_interval_day_to_second_array(values, column)
        }
        ExasolType::Geometry { .. } | ExasolType::Hashtype { .. } => {
            build_binary_array(values, column)
        }
    }
}

/// Build a Boolean array.
fn build_boolean_array(values: &[Value], column: usize) -> Result<ArrayRef, ConversionError> {
    let mut builder = BooleanBuilder::with_capacity(values.len());

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(b) = value.as_bool() {
            builder.append_value(b);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected boolean, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Estimate average string length for capacity pre-allocation.
/// Uses a sample of the first few non-null values.
fn estimate_string_capacity(values: &[Value]) -> usize {
    const SAMPLE_SIZE: usize = 10;
    const DEFAULT_AVG_LEN: usize = 32;

    let mut total_len = 0;
    let mut count = 0;

    for value in values.iter().take(SAMPLE_SIZE) {
        if let Some(s) = value.as_str() {
            total_len += s.len();
            count += 1;
        }
    }

    if count > 0 {
        let avg_len = total_len / count;
        // Add some buffer and multiply by total values
        (avg_len + 8) * values.len()
    } else {
        DEFAULT_AVG_LEN * values.len()
    }
}

/// Build a String array with UTF-8 validation.
fn build_string_array(values: &[Value], column: usize) -> Result<ArrayRef, ConversionError> {
    // Use better capacity estimation based on actual string lengths
    let estimated_bytes = estimate_string_capacity(values);
    let mut builder = StringBuilder::with_capacity(values.len(), estimated_bytes);

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            // UTF-8 validation is automatic in Rust strings
            builder.append_value(s);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected string, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Validate that a decimal string fits within the target precision.
///
/// This function checks that the total number of significant digits in the
/// decimal value does not exceed the specified precision.
///
/// # Arguments
/// * `value_str` - The decimal value as a string
/// * `precision` - The maximum number of significant digits allowed
/// * `_scale` - The scale (reserved for future use)
/// * `row` - The row index for error reporting
/// * `column` - The column index for error reporting
///
/// # Returns
/// `Ok(())` if the value fits within precision, or `ConversionError::NumericOverflow` otherwise.
fn validate_decimal_precision(
    value_str: &str,
    precision: u8,
    _scale: i8,
    row: usize,
    column: usize,
) -> Result<(), ConversionError> {
    // Remove sign and decimal point for digit counting
    let digits: String = value_str.chars().filter(|c| c.is_ascii_digit()).collect();

    let total_digits = digits.len();
    let max_digits = precision as usize;

    // Also strip leading zeros for accurate precision check
    let significant_digits = digits.trim_start_matches('0');
    let significant_count = if significant_digits.is_empty() {
        1 // "0" has 1 significant digit
    } else {
        significant_digits.len()
    };

    if significant_count > max_digits {
        return Err(ConversionError::NumericOverflow { row, column });
    }

    // Also check total raw digit count as a sanity check
    // (precision includes both integer and fractional parts)
    if total_digits > max_digits + 1 {
        // +1 for potential leading zero
        return Err(ConversionError::NumericOverflow { row, column });
    }

    Ok(())
}

/// Build a Decimal128 array.
fn build_decimal128_array(
    values: &[Value],
    precision: u8,
    scale: i8,
    column: usize,
) -> Result<ArrayRef, ConversionError> {
    let mut builder = Decimal128Builder::with_capacity(values.len())
        .with_precision_and_scale(precision, scale)
        .map_err(|e| ConversionError::ArrowError(e.to_string()))?;

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else {
            let decimal_value = parse_decimal_to_i128(value, precision, scale, row, column)?;
            builder.append_value(decimal_value);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a JSON value to i128 for Decimal128.
fn parse_decimal_to_i128(
    value: &Value,
    precision: u8,
    scale: i8,
    row: usize,
    column: usize,
) -> Result<i128, ConversionError> {
    let value_str = if let Some(s) = value.as_str() {
        // Validate precision before conversion
        validate_decimal_precision(s, precision, scale, row, column)?;
        s
    } else if let Some(n) = value.as_i64() {
        return Ok((n as i128) * 10_i128.pow(scale as u32));
    } else if let Some(n) = value.as_f64() {
        // Convert float to decimal by multiplying by 10^scale
        let scaled = (n * 10_f64.powi(scale as i32)).round();
        return scaled
            .to_string()
            .parse::<i128>()
            .map_err(|_| ConversionError::NumericOverflow { row, column });
    } else {
        return Err(ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Expected numeric value, got: {:?}", value),
        });
    };

    // Parse string representation of decimal
    // Format: "123.45" or "123"
    let parts: Vec<&str> = value_str.split('.').collect();

    let (integer_part, decimal_part) = match parts.len() {
        1 => (parts[0], ""),
        2 => (parts[0], parts[1]),
        _ => {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Invalid decimal format: {}", value_str),
            });
        }
    };

    // Parse the integer part
    let mut result: i128 =
        integer_part
            .parse()
            .map_err(|_| ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Invalid integer part: {}", integer_part),
            })?;

    // Scale up by 10^scale
    result = result
        .checked_mul(10_i128.pow(scale as u32))
        .ok_or(ConversionError::NumericOverflow { row, column })?;

    // Add the decimal part
    if !decimal_part.is_empty() {
        let decimal_digits = decimal_part.len().min(scale as usize);
        let decimal_value: i128 = decimal_part[..decimal_digits].parse().map_err(|_| {
            ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Invalid decimal part: {}", decimal_part),
            }
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
            .ok_or(ConversionError::NumericOverflow { row, column })?;
    }

    Ok(result)
}

/// Build a Float64 array.
fn build_double_array(values: &[Value], column: usize) -> Result<ArrayRef, ConversionError> {
    let mut builder = Float64Builder::with_capacity(values.len());

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(f) = value.as_f64() {
            builder.append_value(f);
        } else if let Some(i) = value.as_i64() {
            builder.append_value(i as f64);
        } else if let Some(s) = value.as_str() {
            // Handle special values like "Infinity", "-Infinity", "NaN"
            let f = match s {
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                "NaN" => f64::NAN,
                _ => s
                    .parse::<f64>()
                    .map_err(|_| ConversionError::ValueConversionFailed {
                        row,
                        column,
                        message: format!("Invalid float value: {}", s),
                    })?,
            };
            builder.append_value(f);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected number, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Date32 array (days since Unix epoch).
fn build_date_array(values: &[Value], column: usize) -> Result<ArrayRef, ConversionError> {
    let mut builder = Date32Builder::with_capacity(values.len());

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            // Parse date string in format "YYYY-MM-DD"
            let days = parse_date_to_days(s, row, column)?;
            builder.append_value(days);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected date string, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a date string to days since Unix epoch (1970-01-01).
fn parse_date_to_days(date_str: &str, row: usize, column: usize) -> Result<i32, ConversionError> {
    let parts: Vec<&str> = date_str.split('-').collect();
    if parts.len() != 3 {
        return Err(ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid date format: {}", date_str),
        });
    }

    let year: i32 = parts[0]
        .parse()
        .map_err(|_| ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid year: {}", parts[0]),
        })?;

    let month: u32 = parts[1]
        .parse()
        .map_err(|_| ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid month: {}", parts[1]),
        })?;

    let day: u32 = parts[2]
        .parse()
        .map_err(|_| ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid day: {}", parts[2]),
        })?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid date values: {}", date_str),
        });
    }

    // Calculate days since Unix epoch
    // Simple calculation (doesn't account for all leap years perfectly, but close enough)
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
        _ => unreachable!(),
    };

    // Add leap day if after February and leap year
    let is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let leap_adjustment = if month > 2 && is_leap_year { 1 } else { 0 };

    Ok(days_from_year + days_from_month + day as i32 - 1 + leap_adjustment)
}

/// Build a Timestamp array (microseconds since Unix epoch).
fn build_timestamp_array(
    values: &[Value],
    _with_local_time_zone: bool,
    column: usize,
) -> Result<ArrayRef, ConversionError> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            // Parse timestamp string in format "YYYY-MM-DD HH:MM:SS.ffffff"
            let micros = parse_timestamp_to_micros(s, row, column)?;
            builder.append_value(micros);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected timestamp string, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a timestamp string to microseconds since Unix epoch.
fn parse_timestamp_to_micros(
    timestamp_str: &str,
    row: usize,
    column: usize,
) -> Result<i64, ConversionError> {
    // Split date and time
    let parts: Vec<&str> = timestamp_str.split(' ').collect();
    if parts.is_empty() {
        return Err(ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid timestamp format: {}", timestamp_str),
        });
    }

    // Parse date part
    let days = parse_date_to_days(parts[0], row, column)?;
    let mut micros = days as i64 * 86400 * 1_000_000;

    // Parse time part if present
    if parts.len() > 1 {
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() >= 2 {
            let hours: i64 =
                time_parts[0]
                    .parse()
                    .map_err(|_| ConversionError::ValueConversionFailed {
                        row,
                        column,
                        message: format!("Invalid hour: {}", time_parts[0]),
                    })?;

            let minutes: i64 =
                time_parts[1]
                    .parse()
                    .map_err(|_| ConversionError::ValueConversionFailed {
                        row,
                        column,
                        message: format!("Invalid minute: {}", time_parts[1]),
                    })?;

            micros += hours * 3600 * 1_000_000;
            micros += minutes * 60 * 1_000_000;

            if time_parts.len() >= 3 {
                // Parse seconds and microseconds
                let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
                let seconds: i64 =
                    sec_parts[0]
                        .parse()
                        .map_err(|_| ConversionError::ValueConversionFailed {
                            row,
                            column,
                            message: format!("Invalid second: {}", sec_parts[0]),
                        })?;

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

/// Build an IntervalMonthDayNano array for INTERVAL YEAR TO MONTH.
fn build_interval_year_to_month_array(
    values: &[Value],
    column: usize,
) -> Result<ArrayRef, ConversionError> {
    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(values.len());

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            // Parse format "+YY-MM" or "-YY-MM"
            let months = parse_interval_year_to_month(s, row, column)?;
            // IntervalMonthDayNano stores months as i32, days as i32, nanoseconds as i64
            let interval = arrow::datatypes::IntervalMonthDayNano {
                months,
                days: 0,
                nanoseconds: 0,
            };
            builder.append_value(interval);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected interval string, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse INTERVAL YEAR TO MONTH string to months.
fn parse_interval_year_to_month(
    interval_str: &str,
    row: usize,
    column: usize,
) -> Result<i32, ConversionError> {
    // Format: "+YY-MM" or "-YY-MM"
    let is_negative = interval_str.starts_with('-');
    let parts: Vec<&str> = interval_str
        .trim_start_matches(&['+', '-'][..])
        .split('-')
        .collect();

    if parts.len() != 2 {
        return Err(ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid interval format: {}", interval_str),
        });
    }

    let years: i32 = parts[0]
        .parse()
        .map_err(|_| ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid years: {}", parts[0]),
        })?;

    let months: i32 = parts[1]
        .parse()
        .map_err(|_| ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid months: {}", parts[1]),
        })?;

    let total_months = years * 12 + months;
    Ok(if is_negative {
        -total_months
    } else {
        total_months
    })
}

/// Build an IntervalMonthDayNano array for INTERVAL DAY TO SECOND.
fn build_interval_day_to_second_array(
    values: &[Value],
    column: usize,
) -> Result<ArrayRef, ConversionError> {
    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(values.len());

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            // Parse format "+DD HH:MM:SS.ffffff"
            let (days, nanos) = parse_interval_day_to_second(s, row, column)?;
            // IntervalMonthDayNano stores months as i32, days as i32, nanoseconds as i64
            let interval = arrow::datatypes::IntervalMonthDayNano {
                months: 0,
                days,
                nanoseconds: nanos,
            };
            builder.append_value(interval);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected interval string, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse INTERVAL DAY TO SECOND string to (days, nanoseconds).
fn parse_interval_day_to_second(
    interval_str: &str,
    row: usize,
    column: usize,
) -> Result<(i32, i64), ConversionError> {
    // Format: "+DD HH:MM:SS.ffffff" or "-DD HH:MM:SS.ffffff"
    let is_negative = interval_str.starts_with('-');
    let trimmed = interval_str.trim_start_matches(&['+', '-'][..]);
    let parts: Vec<&str> = trimmed.split(' ').collect();

    if parts.is_empty() {
        return Err(ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid interval format: {}", interval_str),
        });
    }

    let days: i32 = parts[0]
        .parse()
        .map_err(|_| ConversionError::ValueConversionFailed {
            row,
            column,
            message: format!("Invalid days: {}", parts[0]),
        })?;

    let mut nanos: i64 = 0;

    if parts.len() > 1 {
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() >= 2 {
            let hours: i64 =
                time_parts[0]
                    .parse()
                    .map_err(|_| ConversionError::ValueConversionFailed {
                        row,
                        column,
                        message: format!("Invalid hours: {}", time_parts[0]),
                    })?;

            let minutes: i64 =
                time_parts[1]
                    .parse()
                    .map_err(|_| ConversionError::ValueConversionFailed {
                        row,
                        column,
                        message: format!("Invalid minutes: {}", time_parts[1]),
                    })?;

            nanos += hours * 3600 * 1_000_000_000;
            nanos += minutes * 60 * 1_000_000_000;

            if time_parts.len() >= 3 {
                let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
                let seconds: i64 =
                    sec_parts[0]
                        .parse()
                        .map_err(|_| ConversionError::ValueConversionFailed {
                            row,
                            column,
                            message: format!("Invalid seconds: {}", sec_parts[0]),
                        })?;

                nanos += seconds * 1_000_000_000;

                if sec_parts.len() > 1 {
                    // Parse fractional seconds (nanoseconds)
                    let frac = sec_parts[1];
                    let frac_nanos = if frac.len() <= 9 {
                        let padding = 9 - frac.len();
                        let padded = format!("{}{}", frac, "0".repeat(padding));
                        padded.parse::<i64>().unwrap_or(0)
                    } else {
                        frac[..9].parse::<i64>().unwrap_or(0)
                    };
                    nanos += frac_nanos;
                }
            }
        }
    }

    Ok(if is_negative {
        (-days, -nanos)
    } else {
        (days, nanos)
    })
}

/// Build a Binary array for GEOMETRY and HASHTYPE.
fn build_binary_array(values: &[Value], column: usize) -> Result<ArrayRef, ConversionError> {
    // Estimate binary capacity: hex strings are 2 chars per byte
    // Sample up to 10 values to estimate average size
    const SAMPLE_SIZE: usize = 10;
    let sample_count = values.len().min(SAMPLE_SIZE);
    let sample_total: usize = values
        .iter()
        .take(SAMPLE_SIZE)
        .filter_map(|v| v.as_str())
        .map(|s| s.len() / 2)
        .sum();

    let estimated_bytes = if sample_count > 0 {
        let avg_size = (sample_total / sample_count).max(1);
        avg_size * values.len()
    } else {
        values.len() * 32 // Default estimate
    };

    let mut builder = BinaryBuilder::with_capacity(values.len(), estimated_bytes.max(1024));

    for (row, value) in values.iter().enumerate() {
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            // Assume hex-encoded string, decode to bytes
            let bytes = decode_hex(s).map_err(|_| ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Invalid hex string: {}", s),
            })?;
            builder.append_value(&bytes);
        } else {
            return Err(ConversionError::ValueConversionFailed {
                row,
                column,
                message: format!("Expected hex string, got: {:?}", value),
            });
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Decode a hex string to bytes.
fn decode_hex(s: &str) -> Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
        return Err(());
    }

    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_build_boolean_array() {
        let values = vec![json!(true), json!(false), json!(null), json!(true)];
        let array = build_boolean_array(&values, 0).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_string_array() {
        let values = vec![json!("hello"), json!("world"), json!(null), json!("test")];
        let array = build_string_array(&values, 0).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_decimal128_array() {
        let values = vec![json!("123.45"), json!("678.90"), json!(null)];
        let array = build_decimal128_array(&values, 10, 2, 0).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_double_array() {
        let values = vec![json!(1.5), json!(2.7), json!(null), json!("Infinity")];
        let array = build_double_array(&values, 0).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_parse_date_to_days() {
        // 1970-01-01 should be day 0
        let days = parse_date_to_days("1970-01-01", 0, 0).unwrap();
        assert_eq!(days, 0);

        // 1970-01-02 should be day 1
        let days = parse_date_to_days("1970-01-02", 0, 0).unwrap();
        assert_eq!(days, 1);
    }

    #[test]
    fn test_parse_timestamp_to_micros() {
        // 1970-01-01 00:00:00 should be 0 microseconds
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00", 0, 0).unwrap();
        assert_eq!(micros, 0);

        // 1970-01-01 00:00:01 should be 1,000,000 microseconds
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:01", 0, 0).unwrap();
        assert_eq!(micros, 1_000_000);
    }

    #[test]
    fn test_parse_interval_year_to_month() {
        let months = parse_interval_year_to_month("+01-06", 0, 0).unwrap();
        assert_eq!(months, 18); // 1 year + 6 months

        let months = parse_interval_year_to_month("-02-03", 0, 0).unwrap();
        assert_eq!(months, -27); // -(2 years + 3 months)
    }

    #[test]
    fn test_parse_interval_day_to_second() {
        let (days, nanos) = parse_interval_day_to_second("+01 12:30:45.123456789", 0, 0).unwrap();
        assert_eq!(days, 1);
        assert_eq!(
            nanos,
            12 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123_456_789
        );
    }

    #[test]
    fn test_decode_hex() {
        let bytes = decode_hex("48656c6c6f").unwrap();
        assert_eq!(bytes, b"Hello");

        let bytes = decode_hex("").unwrap();
        assert_eq!(bytes, b"");

        assert!(decode_hex("xyz").is_err());
        assert!(decode_hex("1").is_err()); // Odd length
    }

    #[test]
    fn test_parse_decimal_to_i128() {
        // Test integer
        let result = parse_decimal_to_i128(&json!("123"), 10, 2, 0, 0).unwrap();
        assert_eq!(result, 12300); // 123 * 10^2

        // Test decimal
        let result = parse_decimal_to_i128(&json!("123.45"), 10, 2, 0, 0).unwrap();
        assert_eq!(result, 12345); // 123.45 * 10^2

        // Test negative
        let result = parse_decimal_to_i128(&json!("-123.45"), 10, 2, 0, 0).unwrap();
        assert_eq!(result, -12345);

        // Test from number
        let result = parse_decimal_to_i128(&json!(123), 10, 2, 0, 0).unwrap();
        assert_eq!(result, 12300);
    }

    #[test]
    fn test_invalid_conversions() {
        // Invalid boolean
        let values = vec![json!("not a bool")];
        assert!(build_boolean_array(&values, 0).is_err());

        // Invalid date
        let values = vec![json!("2024-13-01")]; // Invalid month
        assert!(build_date_array(&values, 0).is_err());

        // Invalid hex
        let values = vec![json!("zzz")];
        assert!(build_binary_array(&values, 0).is_err());
    }

    #[test]
    fn test_validate_decimal_precision() {
        // Valid: 5 digits fits in precision 10
        assert!(validate_decimal_precision("12345", 10, 2, 0, 0).is_ok());

        // Valid: decimal point doesn't count
        assert!(validate_decimal_precision("123.45", 10, 2, 0, 0).is_ok());

        // Valid: sign doesn't count
        assert!(validate_decimal_precision("-12345", 10, 2, 0, 0).is_ok());

        // Invalid: 6 significant digits in precision 5
        assert!(validate_decimal_precision("123456", 5, 2, 0, 0).is_err());

        // Valid: leading zeros don't count as significant
        assert!(validate_decimal_precision("00123", 5, 2, 0, 0).is_ok());

        // Valid: zero
        assert!(validate_decimal_precision("0", 1, 0, 0, 0).is_ok());

        // Valid: zero with leading zeros
        assert!(validate_decimal_precision("000", 5, 0, 0, 0).is_ok());
    }

    #[test]
    fn test_decimal_precision_overflow() {
        // Test that precision overflow is detected
        let values = vec![json!("12345678901234567890")]; // 20 digits
        let result = build_decimal128_array(&values, 10, 2, 0);
        assert!(result.is_err());

        // Should succeed with sufficient precision
        let values = vec![json!("12345")]; // 5 digits
        let result = build_decimal128_array(&values, 10, 2, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_estimate_string_capacity() {
        let values = vec![json!("hello"), json!("world"), json!("test")];
        let capacity = estimate_string_capacity(&values);
        // Average length is about 5, plus buffer, times 3 values
        assert!(capacity > 0);
        assert!(capacity >= 15); // At least the actual total length
    }
}
