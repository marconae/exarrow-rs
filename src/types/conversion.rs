//! Shared type conversion utilities.
//!
//! This module provides common parsing functions for date, timestamp, and decimal
//! values used across the codebase. The functions return simple `Result<T, String>`
//! to allow callers to wrap errors into their specific error types.

use arrow::datatypes::{DataType, TimeUnit};

use crate::types::ExasolType;

/// Parses a date string (YYYY-MM-DD) to days since Unix epoch (1970-01-01).
///
/// # Arguments
/// * `date_str` - A date string in YYYY-MM-DD format
///
/// # Returns
/// * `Ok(i32)` - Days since Unix epoch
/// * `Err(String)` - Description of the parsing error
///
pub fn parse_date_to_days(date_str: &str) -> Result<i32, String> {
    let parts: Vec<&str> = date_str.split('-').collect();
    if parts.len() != 3 {
        return Err(format!(
            "Invalid date format: {} (expected YYYY-MM-DD)",
            date_str
        ));
    }

    let year: i32 = parts[0]
        .parse()
        .map_err(|_| format!("Invalid year: {}", parts[0]))?;
    let month: u32 = parts[1]
        .parse()
        .map_err(|_| format!("Invalid month: {}", parts[1]))?;
    let day: u32 = parts[2]
        .parse()
        .map_err(|_| format!("Invalid day: {}", parts[2]))?;

    if !(1..=12).contains(&month) {
        return Err(format!("Month out of range: {}", month));
    }
    if !(1..=31).contains(&day) {
        return Err(format!("Day out of range: {}", day));
    }

    // Calculate days since Unix epoch (1970-01-01)
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

/// Converts a year/month/day triple directly to days since Unix epoch (1970-01-01).
///
/// Uses the same arithmetic as `parse_date_to_days` but skips string parsing
/// and validation; callers that have already decoded the components should
/// prefer this function on hot paths.
pub fn ymd_to_days(year: i32, month: u32, day: u32) -> i32 {
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
        _ => 0,
    };

    let is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let leap_adjustment = if month > 2 && is_leap_year { 1 } else { 0 };

    days_from_year + days_from_month + day as i32 - 1 + leap_adjustment
}

/// Converts a pre-decoded timestamp (y, m, d, h, min, s, nanos) to microseconds
/// since Unix epoch. Nanoseconds are truncated to microsecond precision.
pub fn ymd_hms_nanos_to_micros(
    year: i32,
    month: u32,
    day: u32,
    hour: u64,
    minute: u64,
    second: u64,
    nanos: i32,
) -> i64 {
    let days = ymd_to_days(year, month, day);
    let mut micros = days as i64 * 86_400 * 1_000_000;
    micros += hour as i64 * 3_600 * 1_000_000;
    micros += minute as i64 * 60 * 1_000_000;
    micros += second as i64 * 1_000_000;
    micros += nanos as i64 / 1_000;
    micros
}

/// Parses a timestamp string to microseconds since Unix epoch.
///
/// Supports formats:
/// - "YYYY-MM-DD" (date only, time defaults to 00:00:00)
/// - "YYYY-MM-DD HH:MM:SS" (date and time)
/// - "YYYY-MM-DD HH:MM:SS.ffffff" (date, time, and fractional seconds)
///
/// # Arguments
/// * `timestamp_str` - A timestamp string
///
/// # Returns
/// * `Ok(i64)` - Microseconds since Unix epoch
/// * `Err(String)` - Description of the parsing error
pub fn parse_timestamp_to_micros(timestamp_str: &str) -> Result<i64, String> {
    // Split date and time
    let parts: Vec<&str> = timestamp_str.split(' ').collect();
    if parts.is_empty() {
        return Err(format!("Invalid timestamp format: {}", timestamp_str));
    }

    // Parse date part
    let days = parse_date_to_days(parts[0])?;
    let mut micros = days as i64 * 86400 * 1_000_000;

    // Parse time part if present
    if parts.len() > 1 {
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() >= 2 {
            let hours: i64 = time_parts[0]
                .parse()
                .map_err(|_| format!("Invalid hour: {}", time_parts[0]))?;
            let minutes: i64 = time_parts[1]
                .parse()
                .map_err(|_| format!("Invalid minute: {}", time_parts[1]))?;

            micros += hours * 3600 * 1_000_000;
            micros += minutes * 60 * 1_000_000;

            if time_parts.len() >= 3 {
                // Parse seconds and microseconds
                let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
                let seconds: i64 = sec_parts[0]
                    .parse()
                    .map_err(|_| format!("Invalid second: {}", sec_parts[0]))?;

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

/// Parses a decimal string to i128 with the given scale.
///
/// Supports formats:
/// - "123" (integer)
/// - "123.45" (decimal)
/// - "-123.45" (negative decimal)
///
/// # Arguments
/// * `value_str` - A decimal string
/// * `scale` - The scale (number of decimal places) to use
///
/// # Returns
/// * `Ok(i128)` - The decimal value scaled by 10^scale
/// * `Err(String)` - Description of the parsing error
pub fn parse_decimal_to_i128(value_str: &str, scale: i8) -> Result<i128, String> {
    // Parse string representation of decimal
    // Format: "123.45" or "123" or "-123.45"
    let parts: Vec<&str> = value_str.split('.').collect();

    let (integer_part, decimal_part) = match parts.len() {
        1 => (parts[0], ""),
        2 => (parts[0], parts[1]),
        _ => return Err(format!("Invalid decimal format: {}", value_str)),
    };

    // Parse the integer part
    let mut result: i128 = integer_part
        .parse()
        .map_err(|_| format!("Invalid integer part: {}", integer_part))?;

    // Scale up by 10^scale
    result = result
        .checked_mul(10_i128.pow(scale as u32))
        .ok_or_else(|| format!("Numeric overflow for value: {}", value_str))?;

    // Add the decimal part
    if !decimal_part.is_empty() {
        let decimal_digits = decimal_part.len().min(scale as usize);
        let decimal_value: i128 = decimal_part[..decimal_digits]
            .parse()
            .map_err(|_| format!("Invalid decimal part: {}", decimal_part))?;

        // Scale the decimal part appropriately
        let scale_diff = scale as usize - decimal_digits;
        let scaled_decimal = decimal_value * 10_i128.pow(scale_diff as u32);

        result = if integer_part.starts_with('-') {
            result
                .checked_sub(scaled_decimal)
                .ok_or_else(|| format!("Numeric overflow for value: {}", value_str))?
        } else {
            result
                .checked_add(scaled_decimal)
                .ok_or_else(|| format!("Numeric overflow for value: {}", value_str))?
        };
    }

    Ok(result)
}

/// Maps an Exasol type to an Arrow DataType.
///
/// # Arguments
/// * `exasol_type` - The Exasol type to convert
///
/// # Returns
/// * `Ok(DataType)` - The corresponding Arrow DataType
/// * `Err(String)` - Description if the type is unsupported
pub fn exasol_type_to_arrow(exasol_type: &ExasolType) -> Result<DataType, String> {
    match exasol_type {
        ExasolType::Boolean => Ok(DataType::Boolean),
        ExasolType::Char { .. } | ExasolType::Varchar { .. } => Ok(DataType::Utf8),
        ExasolType::Decimal { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        ExasolType::Double => Ok(DataType::Float64),
        ExasolType::Date => Ok(DataType::Date32),
        ExasolType::Timestamp {
            with_local_time_zone,
        } => {
            if *with_local_time_zone {
                Ok(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                ))
            } else {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
        }
        ExasolType::IntervalYearToMonth => Ok(DataType::Int64), // Months as i64
        ExasolType::IntervalDayToSecond { .. } => Ok(DataType::Int64), // Nanoseconds as i64
        ExasolType::Geometry { .. } | ExasolType::Hashtype { .. } => Ok(DataType::Binary),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for parse_date_to_days

    #[test]
    fn test_parse_date_to_days_unix_epoch() {
        assert_eq!(parse_date_to_days("1970-01-01").unwrap(), 0);
    }

    #[test]
    fn test_parse_date_to_days_positive_date() {
        // 2024-01-01 should be a positive number of days
        let days = parse_date_to_days("2024-01-01").unwrap();
        assert!(days > 0);
    }

    #[test]
    fn test_parse_date_to_days_negative_date() {
        // 1969-01-01 should be negative (before Unix epoch)
        let days = parse_date_to_days("1969-01-01").unwrap();
        assert!(days < 0);
    }

    #[test]
    fn test_parse_date_to_days_leap_year() {
        // Feb 29, 2024 (leap year) should be valid
        let days = parse_date_to_days("2024-02-29").unwrap();
        assert!(days > 0);
    }

    #[test]
    fn test_parse_date_to_days_invalid_format() {
        assert!(parse_date_to_days("2024/01/01").is_err());
        // "01-01-2024" parses as year=1, month=1, day=2024, which fails day validation
        assert!(parse_date_to_days("01-01-2024").is_err());
        assert!(parse_date_to_days("2024-1-1").is_ok()); // Single digits should work
    }

    #[test]
    fn test_parse_date_to_days_invalid_month() {
        assert!(parse_date_to_days("2024-13-01").is_err());
        assert!(parse_date_to_days("2024-00-01").is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_day() {
        assert!(parse_date_to_days("2024-01-32").is_err());
        assert!(parse_date_to_days("2024-01-00").is_err());
    }

    // Tests for parse_timestamp_to_micros

    #[test]
    fn test_parse_timestamp_to_micros_date_only() {
        let micros = parse_timestamp_to_micros("1970-01-01").unwrap();
        assert_eq!(micros, 0);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_time() {
        let micros = parse_timestamp_to_micros("1970-01-01 01:00:00").unwrap();
        assert_eq!(micros, 3600 * 1_000_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fractional_seconds() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.123456").unwrap();
        assert_eq!(micros, 123456);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_short_fraction() {
        // .123 should be interpreted as 123000 microseconds
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.123").unwrap();
        assert_eq!(micros, 123000);
    }

    // Tests for ymd_to_days

    #[test]
    fn test_ymd_to_days_unix_epoch() {
        assert_eq!(ymd_to_days(1970, 1, 1), 0);
    }

    #[test]
    fn test_ymd_to_days_matches_string_parser() {
        for date in &[
            "1970-01-01",
            "1969-12-31",
            "2024-02-29",
            "2000-02-29",
            "1900-03-01",
            "2024-01-01",
            "2100-03-01",
            "2023-12-31",
            "1601-01-01",
        ] {
            let parts: Vec<&str> = date.split('-').collect();
            let y: i32 = parts[0].parse().unwrap();
            let m: u32 = parts[1].parse().unwrap();
            let d: u32 = parts[2].parse().unwrap();
            assert_eq!(
                ymd_to_days(y, m, d),
                parse_date_to_days(date).unwrap(),
                "mismatch for {}",
                date
            );
        }
    }

    #[test]
    fn test_ymd_to_days_leap_year_after_feb() {
        // 2024 is a leap year; March 1 2024 is one day later than a non-leap year would be
        let leap_mar1 = ymd_to_days(2024, 3, 1);
        let non_leap_mar1 = ymd_to_days(2023, 3, 1);
        assert_eq!(leap_mar1 - non_leap_mar1, 366);
    }

    // Tests for ymd_hms_nanos_to_micros

    #[test]
    fn test_ymd_hms_nanos_to_micros_epoch() {
        assert_eq!(ymd_hms_nanos_to_micros(1970, 1, 1, 0, 0, 0, 0), 0);
    }

    #[test]
    fn test_ymd_hms_nanos_to_micros_with_time() {
        let micros = ymd_hms_nanos_to_micros(1970, 1, 1, 1, 0, 0, 0);
        assert_eq!(micros, 3600 * 1_000_000);
    }

    #[test]
    fn test_ymd_hms_nanos_to_micros_sub_second_precision() {
        // 123456789 nanos → 123456 micros (truncated)
        let micros = ymd_hms_nanos_to_micros(1970, 1, 1, 0, 0, 0, 123_456_789);
        assert_eq!(micros, 123_456);
    }

    #[test]
    fn test_ymd_hms_nanos_to_micros_full_timestamp() {
        // 2024-01-02 03:04:05.678 → known value
        let micros = ymd_hms_nanos_to_micros(2024, 1, 2, 3, 4, 5, 678_000_000);
        let days = ymd_to_days(2024, 1, 2) as i64;
        let expected = days * 86_400 * 1_000_000
            + 3 * 3_600 * 1_000_000
            + 4 * 60 * 1_000_000
            + 5 * 1_000_000
            + 678_000;
        assert_eq!(micros, expected);
    }

    // Tests for parse_decimal_to_i128

    #[test]
    fn test_parse_decimal_to_i128_integer() {
        let result = parse_decimal_to_i128("123", 2).unwrap();
        assert_eq!(result, 12300); // 123 * 10^2
    }

    #[test]
    fn test_parse_decimal_to_i128_with_decimal() {
        let result = parse_decimal_to_i128("123.45", 2).unwrap();
        assert_eq!(result, 12345);
    }

    #[test]
    fn test_parse_decimal_to_i128_negative() {
        let result = parse_decimal_to_i128("-123.45", 2).unwrap();
        assert_eq!(result, -12345);
    }

    #[test]
    fn test_parse_decimal_to_i128_scale_larger_than_digits() {
        // "12.3" with scale 4 should give 123000
        let result = parse_decimal_to_i128("12.3", 4).unwrap();
        assert_eq!(result, 123000);
    }

    #[test]
    fn test_parse_decimal_to_i128_invalid_format() {
        assert!(parse_decimal_to_i128("12.34.56", 2).is_err());
        assert!(parse_decimal_to_i128("abc", 2).is_err());
    }

    // Tests for exasol_type_to_arrow

    #[test]
    fn test_exasol_type_to_arrow_boolean() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Boolean).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn test_exasol_type_to_arrow_varchar() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Varchar { size: 100 }).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_exasol_type_to_arrow_decimal() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Decimal {
                precision: 18,
                scale: 2
            })
            .unwrap(),
            DataType::Decimal128(18, 2)
        );
    }

    #[test]
    fn test_exasol_type_to_arrow_timestamp() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Timestamp {
                with_local_time_zone: false
            })
            .unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_exasol_type_to_arrow_timestamp_with_tz() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Timestamp {
                with_local_time_zone: true
            })
            .unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_exasol_type_to_arrow_date() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Date).unwrap(),
            DataType::Date32
        );
    }

    #[test]
    fn test_exasol_type_to_arrow_double() {
        assert_eq!(
            exasol_type_to_arrow(&ExasolType::Double).unwrap(),
            DataType::Float64
        );
    }
}
