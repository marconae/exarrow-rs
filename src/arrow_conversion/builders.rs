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

    // ==========================================================================
    // Tests for build_array dispatch
    // ==========================================================================

    #[test]
    fn test_build_array_dispatches_to_boolean() {
        let values = vec![json!(true), json!(false)];
        let result = build_array(&ExasolType::Boolean, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_char() {
        let values = vec![json!("abc"), json!("def")];
        let result = build_array(&ExasolType::Char { size: 3 }, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_varchar() {
        let values = vec![json!("hello"), json!("world")];
        let result = build_array(&ExasolType::Varchar { size: 100 }, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_decimal() {
        let values = vec![json!("123.45"), json!("678.90")];
        let result = build_array(
            &ExasolType::Decimal {
                precision: 10,
                scale: 2,
            },
            &values,
            0,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_double() {
        let values = vec![json!(1.5), json!(2.5)];
        let result = build_array(&ExasolType::Double, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_date() {
        let values = vec![json!("2024-01-15"), json!("2024-06-20")];
        let result = build_array(&ExasolType::Date, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_timestamp_without_tz() {
        let values = vec![json!("2024-01-15 10:30:00"), json!("2024-06-20 14:45:30")];
        let result = build_array(
            &ExasolType::Timestamp {
                with_local_time_zone: false,
            },
            &values,
            0,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_timestamp_with_tz() {
        let values = vec![json!("2024-01-15 10:30:00"), json!("2024-06-20 14:45:30")];
        let result = build_array(
            &ExasolType::Timestamp {
                with_local_time_zone: true,
            },
            &values,
            0,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_interval_year_to_month() {
        let values = vec![json!("+01-06"), json!("-02-03")];
        let result = build_array(&ExasolType::IntervalYearToMonth, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_interval_day_to_second() {
        let values = vec![json!("+01 12:30:45"), json!("-02 08:15:30")];
        let result = build_array(
            &ExasolType::IntervalDayToSecond { precision: 3 },
            &values,
            0,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_geometry() {
        let values = vec![json!("48656c6c6f"), json!("576f726c64")];
        let result = build_array(&ExasolType::Geometry { srid: Some(4326) }, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_array_dispatches_to_hashtype() {
        let values = vec![json!("deadbeef"), json!("cafebabe")];
        let result = build_array(&ExasolType::Hashtype { byte_size: 16 }, &values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    // ==========================================================================
    // Tests for build_date_array
    // ==========================================================================

    #[test]
    fn test_build_date_array_with_valid_dates() {
        let values = vec![
            json!("2024-01-15"),
            json!("2023-06-20"),
            json!("1970-01-01"),
        ];
        let result = build_date_array(&values, 0).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_date_array_with_null_values() {
        let values = vec![json!("2024-01-15"), json!(null), json!("2023-06-20")];
        let result = build_date_array(&values, 0).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 1);
    }

    #[test]
    fn test_build_date_array_invalid_format_not_string() {
        let values = vec![json!(12345)];
        let result = build_date_array(&values, 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConversionError::ValueConversionFailed { message, .. } => {
                assert!(message.contains("Expected date string"));
            }
            _ => panic!("Expected ValueConversionFailed"),
        }
    }

    #[test]
    fn test_build_date_array_invalid_date_format() {
        let values = vec![json!("2024/01/15")]; // Wrong separator
        let result = build_date_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_date_array_invalid_month_out_of_range() {
        let values = vec![json!("2024-13-01")]; // Month 13
        let result = build_date_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_date_array_invalid_day_out_of_range() {
        let values = vec![json!("2024-01-32")]; // Day 32
        let result = build_date_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_date_array_invalid_month_zero() {
        let values = vec![json!("2024-00-15")]; // Month 0
        let result = build_date_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_date_array_invalid_day_zero() {
        let values = vec![json!("2024-01-00")]; // Day 0
        let result = build_date_array(&values, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for parse_date_to_days - all months
    // ==========================================================================

    #[test]
    fn test_parse_date_to_days_january() {
        let days = parse_date_to_days("1970-01-15", 0, 0).unwrap();
        assert_eq!(days, 14); // 15th day, 0-indexed
    }

    #[test]
    fn test_parse_date_to_days_february() {
        let days = parse_date_to_days("1970-02-01", 0, 0).unwrap();
        assert_eq!(days, 31); // Jan has 31 days
    }

    #[test]
    fn test_parse_date_to_days_march() {
        let days = parse_date_to_days("1970-03-01", 0, 0).unwrap();
        assert_eq!(days, 59); // Jan(31) + Feb(28)
    }

    #[test]
    fn test_parse_date_to_days_april() {
        let days = parse_date_to_days("1970-04-01", 0, 0).unwrap();
        assert_eq!(days, 90); // Jan(31) + Feb(28) + Mar(31)
    }

    #[test]
    fn test_parse_date_to_days_may() {
        let days = parse_date_to_days("1970-05-01", 0, 0).unwrap();
        assert_eq!(days, 120); // Jan(31) + Feb(28) + Mar(31) + Apr(30)
    }

    #[test]
    fn test_parse_date_to_days_june() {
        let days = parse_date_to_days("1970-06-01", 0, 0).unwrap();
        assert_eq!(days, 151);
    }

    #[test]
    fn test_parse_date_to_days_july() {
        let days = parse_date_to_days("1970-07-01", 0, 0).unwrap();
        assert_eq!(days, 181);
    }

    #[test]
    fn test_parse_date_to_days_august() {
        let days = parse_date_to_days("1970-08-01", 0, 0).unwrap();
        assert_eq!(days, 212);
    }

    #[test]
    fn test_parse_date_to_days_september() {
        let days = parse_date_to_days("1970-09-01", 0, 0).unwrap();
        assert_eq!(days, 243);
    }

    #[test]
    fn test_parse_date_to_days_october() {
        let days = parse_date_to_days("1970-10-01", 0, 0).unwrap();
        assert_eq!(days, 273);
    }

    #[test]
    fn test_parse_date_to_days_november() {
        let days = parse_date_to_days("1970-11-01", 0, 0).unwrap();
        assert_eq!(days, 304);
    }

    #[test]
    fn test_parse_date_to_days_december() {
        let days = parse_date_to_days("1970-12-01", 0, 0).unwrap();
        assert_eq!(days, 334);
    }

    // ==========================================================================
    // Tests for leap year handling
    // ==========================================================================

    #[test]
    fn test_parse_date_to_days_leap_year_2000() {
        // 2000 is a leap year (divisible by 400)
        // March 1, 2000 should include the leap day
        let march_1_2000 = parse_date_to_days("2000-03-01", 0, 0).unwrap();
        let feb_28_2000 = parse_date_to_days("2000-02-28", 0, 0).unwrap();
        assert_eq!(march_1_2000 - feb_28_2000, 2); // Feb 29 exists
    }

    #[test]
    fn test_parse_date_to_days_leap_year_2004() {
        // 2004 is a leap year (divisible by 4, not by 100)
        let march_1_2004 = parse_date_to_days("2004-03-01", 0, 0).unwrap();
        let feb_28_2004 = parse_date_to_days("2004-02-28", 0, 0).unwrap();
        assert_eq!(march_1_2004 - feb_28_2004, 2); // Feb 29 exists
    }

    #[test]
    fn test_parse_date_to_days_non_leap_year_1900() {
        // 1900 is NOT a leap year (divisible by 100, not by 400)
        let march_1_1900 = parse_date_to_days("1900-03-01", 0, 0).unwrap();
        let feb_28_1900 = parse_date_to_days("1900-02-28", 0, 0).unwrap();
        assert_eq!(march_1_1900 - feb_28_1900, 1); // No Feb 29
    }

    #[test]
    fn test_parse_date_to_days_non_leap_year_2023() {
        // 2023 is NOT a leap year
        let march_1_2023 = parse_date_to_days("2023-03-01", 0, 0).unwrap();
        let feb_28_2023 = parse_date_to_days("2023-02-28", 0, 0).unwrap();
        assert_eq!(march_1_2023 - feb_28_2023, 1); // No Feb 29
    }

    #[test]
    fn test_parse_date_to_days_leap_adjustment_only_after_february() {
        // In a leap year, January should not have the leap adjustment
        let jan_31_2000 = parse_date_to_days("2000-01-31", 0, 0).unwrap();
        let jan_30_2000 = parse_date_to_days("2000-01-30", 0, 0).unwrap();
        assert_eq!(jan_31_2000 - jan_30_2000, 1); // Normal day difference
    }

    #[test]
    fn test_parse_date_to_days_before_unix_epoch() {
        // 1969-12-31 should be -1
        let days = parse_date_to_days("1969-12-31", 0, 0).unwrap();
        assert_eq!(days, -1);
    }

    #[test]
    fn test_parse_date_to_days_far_future() {
        // Test a date far in the future
        let days = parse_date_to_days("2100-01-01", 0, 0).unwrap();
        assert!(days > 0);
    }

    // ==========================================================================
    // Tests for parse_date_to_days error cases
    // ==========================================================================

    #[test]
    fn test_parse_date_to_days_invalid_format_missing_parts() {
        let result = parse_date_to_days("2024-01", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_format_too_many_parts() {
        let result = parse_date_to_days("2024-01-15-extra", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_year() {
        let result = parse_date_to_days("XXXX-01-15", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_month() {
        let result = parse_date_to_days("2024-XX-15", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_day() {
        let result = parse_date_to_days("2024-01-XX", 0, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for build_timestamp_array
    // ==========================================================================

    #[test]
    fn test_build_timestamp_array_without_timezone() {
        let values = vec![
            json!("2024-01-15 10:30:00"),
            json!("2024-06-20 14:45:30.123456"),
        ];
        let result = build_timestamp_array(&values, false, 0).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_timestamp_array_with_timezone() {
        let values = vec![
            json!("2024-01-15 10:30:00"),
            json!("2024-06-20 14:45:30.123456"),
        ];
        let result = build_timestamp_array(&values, true, 0).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_timestamp_array_with_null_values() {
        let values = vec![
            json!("2024-01-15 10:30:00"),
            json!(null),
            json!("2024-06-20 14:45:30"),
        ];
        let result = build_timestamp_array(&values, false, 0).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 1);
    }

    #[test]
    fn test_build_timestamp_array_date_only() {
        // Test timestamp with only date part (no time)
        let values = vec![json!("2024-01-15")];
        let result = build_timestamp_array(&values, false, 0).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_build_timestamp_array_invalid_not_string() {
        let values = vec![json!(12345)];
        let result = build_timestamp_array(&values, false, 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConversionError::ValueConversionFailed { message, .. } => {
                assert!(message.contains("Expected timestamp string"));
            }
            _ => panic!("Expected ValueConversionFailed"),
        }
    }

    // ==========================================================================
    // Tests for parse_timestamp_to_micros
    // ==========================================================================

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_3_digits() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.123", 0, 0).unwrap();
        assert_eq!(micros, 123_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_6_digits() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.123456", 0, 0).unwrap();
        assert_eq!(micros, 123_456);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_9_digits() {
        // Only first 6 digits are used for microseconds
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.123456789", 0, 0).unwrap();
        assert_eq!(micros, 123_456);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_1_digit() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.1", 0, 0).unwrap();
        assert_eq!(micros, 100_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds_2_digits() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.12", 0, 0).unwrap();
        assert_eq!(micros, 120_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_hours_minutes_only() {
        let micros = parse_timestamp_to_micros("1970-01-01 12:30", 0, 0).unwrap();
        assert_eq!(micros, 12 * 3600 * 1_000_000 + 30 * 60 * 1_000_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_full_time() {
        let micros = parse_timestamp_to_micros("1970-01-01 12:30:45", 0, 0).unwrap();
        assert_eq!(
            micros,
            12 * 3600 * 1_000_000 + 30 * 60 * 1_000_000 + 45 * 1_000_000
        );
    }

    #[test]
    fn test_parse_timestamp_to_micros_empty_string() {
        let result = parse_timestamp_to_micros("", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp_to_micros_invalid_hour() {
        let result = parse_timestamp_to_micros("1970-01-01 XX:30:00", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp_to_micros_invalid_minute() {
        let result = parse_timestamp_to_micros("1970-01-01 12:XX:00", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp_to_micros_invalid_second() {
        let result = parse_timestamp_to_micros("1970-01-01 12:30:XX", 0, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for build_interval_year_to_month_array
    // ==========================================================================

    #[test]
    fn test_build_interval_year_to_month_array_positive() {
        let values = vec![json!("+01-06"), json!("+00-03")];
        let result = build_interval_year_to_month_array(&values, 0).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_interval_year_to_month_array_negative() {
        let values = vec![json!("-02-03"), json!("-00-11")];
        let result = build_interval_year_to_month_array(&values, 0).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_interval_year_to_month_array_with_nulls() {
        let values = vec![json!("+01-06"), json!(null), json!("-02-03")];
        let result = build_interval_year_to_month_array(&values, 0).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 1);
    }

    #[test]
    fn test_build_interval_year_to_month_array_invalid_not_string() {
        let values = vec![json!(12345)];
        let result = build_interval_year_to_month_array(&values, 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConversionError::ValueConversionFailed { message, .. } => {
                assert!(message.contains("Expected interval string"));
            }
            _ => panic!("Expected ValueConversionFailed"),
        }
    }

    // ==========================================================================
    // Tests for parse_interval_year_to_month
    // ==========================================================================

    #[test]
    fn test_parse_interval_year_to_month_zero_years() {
        let months = parse_interval_year_to_month("+00-06", 0, 0).unwrap();
        assert_eq!(months, 6);
    }

    #[test]
    fn test_parse_interval_year_to_month_zero_months() {
        let months = parse_interval_year_to_month("+05-00", 0, 0).unwrap();
        assert_eq!(months, 60);
    }

    #[test]
    fn test_parse_interval_year_to_month_large_value() {
        let months = parse_interval_year_to_month("+99-11", 0, 0).unwrap();
        assert_eq!(months, 99 * 12 + 11);
    }

    #[test]
    fn test_parse_interval_year_to_month_negative_large_value() {
        let months = parse_interval_year_to_month("-99-11", 0, 0).unwrap();
        assert_eq!(months, -(99 * 12 + 11));
    }

    #[test]
    fn test_parse_interval_year_to_month_invalid_format() {
        let result = parse_interval_year_to_month("01-06", 0, 0); // Missing sign
                                                                  // This should still work since we trim the sign
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_interval_year_to_month_invalid_format_missing_parts() {
        let result = parse_interval_year_to_month("+01", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_year_to_month_invalid_years() {
        let result = parse_interval_year_to_month("+XX-06", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_year_to_month_invalid_months() {
        let result = parse_interval_year_to_month("+01-XX", 0, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for build_interval_day_to_second_array
    // ==========================================================================

    #[test]
    fn test_build_interval_day_to_second_array_with_fractional_seconds() {
        let values = vec![json!("+01 12:30:45.123456789")];
        let result = build_interval_day_to_second_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_interval_day_to_second_array_without_fractional_seconds() {
        let values = vec![json!("+01 12:30:45")];
        let result = build_interval_day_to_second_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_interval_day_to_second_array_negative() {
        let values = vec![json!("-02 08:15:30.500000000")];
        let result = build_interval_day_to_second_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_interval_day_to_second_array_with_nulls() {
        let values = vec![json!("+01 12:30:45"), json!(null), json!("-02 08:15:30")];
        let result = build_interval_day_to_second_array(&values, 0).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 1);
    }

    #[test]
    fn test_build_interval_day_to_second_array_invalid_not_string() {
        let values = vec![json!(12345)];
        let result = build_interval_day_to_second_array(&values, 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConversionError::ValueConversionFailed { message, .. } => {
                assert!(message.contains("Expected interval string"));
            }
            _ => panic!("Expected ValueConversionFailed"),
        }
    }

    // ==========================================================================
    // Tests for parse_interval_day_to_second
    // ==========================================================================

    #[test]
    fn test_parse_interval_day_to_second_days_only() {
        let (days, nanos) = parse_interval_day_to_second("+05", 0, 0).unwrap();
        assert_eq!(days, 5);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_parse_interval_day_to_second_hours_minutes_only() {
        let (days, nanos) = parse_interval_day_to_second("+01 12:30", 0, 0).unwrap();
        assert_eq!(days, 1);
        assert_eq!(nanos, 12 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_day_to_second_negative_values() {
        let (days, nanos) = parse_interval_day_to_second("-03 06:15:30", 0, 0).unwrap();
        assert_eq!(days, -3);
        assert_eq!(
            nanos,
            -(6 * 3600 * 1_000_000_000 + 15 * 60 * 1_000_000_000 + 30 * 1_000_000_000)
        );
    }

    #[test]
    fn test_parse_interval_day_to_second_with_fractional_3_digits() {
        let (days, nanos) = parse_interval_day_to_second("+00 00:00:00.123", 0, 0).unwrap();
        assert_eq!(days, 0);
        assert_eq!(nanos, 123_000_000);
    }

    #[test]
    fn test_parse_interval_day_to_second_with_fractional_6_digits() {
        let (days, nanos) = parse_interval_day_to_second("+00 00:00:00.123456", 0, 0).unwrap();
        assert_eq!(days, 0);
        assert_eq!(nanos, 123_456_000);
    }

    #[test]
    fn test_parse_interval_day_to_second_with_fractional_9_digits() {
        let (days, nanos) = parse_interval_day_to_second("+00 00:00:00.123456789", 0, 0).unwrap();
        assert_eq!(days, 0);
        assert_eq!(nanos, 123_456_789);
    }

    #[test]
    fn test_parse_interval_day_to_second_with_fractional_12_digits() {
        // Only first 9 digits should be used
        let (days, nanos) =
            parse_interval_day_to_second("+00 00:00:00.123456789012", 0, 0).unwrap();
        assert_eq!(days, 0);
        assert_eq!(nanos, 123_456_789);
    }

    #[test]
    fn test_parse_interval_day_to_second_empty_string() {
        let result = parse_interval_day_to_second("", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_day_to_second_invalid_days() {
        let result = parse_interval_day_to_second("+XX 12:30:45", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_day_to_second_invalid_hours() {
        let result = parse_interval_day_to_second("+01 XX:30:45", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_day_to_second_invalid_minutes() {
        let result = parse_interval_day_to_second("+01 12:XX:45", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_day_to_second_invalid_seconds() {
        let result = parse_interval_day_to_second("+01 12:30:XX", 0, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for build_binary_array
    // ==========================================================================

    #[test]
    fn test_build_binary_array_valid_hex() {
        let values = vec![json!("48656c6c6f"), json!("576f726c64")];
        let result = build_binary_array(&values, 0).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_binary_array_with_null_values() {
        let values = vec![json!("48656c6c6f"), json!(null), json!("576f726c64")];
        let result = build_binary_array(&values, 0).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 1);
    }

    #[test]
    fn test_build_binary_array_empty_hex() {
        let values = vec![json!("")];
        let result = build_binary_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_build_binary_array_invalid_not_string() {
        let values = vec![json!(12345)];
        let result = build_binary_array(&values, 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConversionError::ValueConversionFailed { message, .. } => {
                assert!(message.contains("Expected hex string"));
            }
            _ => panic!("Expected ValueConversionFailed"),
        }
    }

    #[test]
    fn test_build_binary_array_invalid_hex_odd_length() {
        let values = vec![json!("abc")]; // Odd length
        let result = build_binary_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_binary_array_invalid_hex_chars() {
        let values = vec![json!("ghij")]; // Invalid hex chars
        let result = build_binary_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_binary_array_capacity_estimation_with_samples() {
        // Create more than 10 values to test sampling
        let values: Vec<Value> = (0..15).map(|_| json!("deadbeef")).collect();
        let result = build_binary_array(&values, 0).unwrap();
        assert_eq!(result.len(), 15);
    }

    #[test]
    fn test_build_binary_array_capacity_estimation_all_nulls() {
        // All null values for capacity estimation
        let values: Vec<Value> = (0..5).map(|_| json!(null)).collect();
        let result = build_binary_array(&values, 0).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result.null_count(), 5);
    }

    // ==========================================================================
    // Additional edge case tests
    // ==========================================================================

    #[test]
    fn test_estimate_string_capacity_empty_values() {
        let values: Vec<Value> = vec![];
        let capacity = estimate_string_capacity(&values);
        // With no values, should return default estimate
        assert_eq!(capacity, 0);
    }

    #[test]
    fn test_estimate_string_capacity_all_nulls() {
        let values = vec![json!(null), json!(null), json!(null)];
        let capacity = estimate_string_capacity(&values);
        // With no string values, should use default average
        assert!(capacity > 0);
    }

    #[test]
    fn test_estimate_string_capacity_mixed_nulls_and_strings() {
        let values = vec![json!("hello"), json!(null), json!("world")];
        let capacity = estimate_string_capacity(&values);
        assert!(capacity > 0);
    }

    #[test]
    fn test_build_double_array_from_integer() {
        let values = vec![json!(42), json!(100)];
        let result = build_double_array(&values, 0).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_build_double_array_from_string_numeric() {
        let values = vec![json!("3.14159")];
        let result = build_double_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_build_double_array_neg_infinity() {
        let values = vec![json!("-Infinity")];
        let result = build_double_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_build_double_array_nan() {
        let values = vec![json!("NaN")];
        let result = build_double_array(&values, 0).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_build_double_array_invalid_string() {
        let values = vec![json!("not a number")];
        let result = build_double_array(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_decimal_to_i128_from_float() {
        let result = parse_decimal_to_i128(&json!(123.45), 10, 2, 0, 0).unwrap();
        assert_eq!(result, 12345);
    }

    #[test]
    fn test_parse_decimal_to_i128_invalid_decimal_format() {
        let result = parse_decimal_to_i128(&json!("1.2.3"), 10, 2, 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_decimal_to_i128_invalid_integer_part() {
        let result = parse_decimal_to_i128(&json!("abc.45"), 10, 2, 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_decimal_to_i128_invalid_decimal_part() {
        let result = parse_decimal_to_i128(&json!("123.xyz"), 10, 2, 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_decimal_to_i128_non_numeric_value() {
        let result = parse_decimal_to_i128(&json!({"key": "value"}), 10, 2, 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_hex_uppercase() {
        let bytes = decode_hex("DEADBEEF").unwrap();
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_decode_hex_mixed_case() {
        let bytes = decode_hex("DeAdBeEf").unwrap();
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }
}
