//! Type mapping between Exasol and Apache Arrow data types.

use crate::error::ConversionError;
use arrow_schema::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Exasol data type representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "UPPERCASE")]
pub enum ExasolType {
    /// BOOLEAN type
    #[serde(rename = "BOOLEAN")]
    Boolean,

    /// CHAR(n) type
    #[serde(rename = "CHAR")]
    Char { size: usize },

    /// VARCHAR(n) type
    #[serde(rename = "VARCHAR")]
    Varchar { size: usize },

    /// DECIMAL(p, s) type
    #[serde(rename = "DECIMAL")]
    Decimal { precision: u8, scale: i8 },

    /// DOUBLE PRECISION type
    #[serde(rename = "DOUBLE")]
    Double,

    /// DATE type
    #[serde(rename = "DATE")]
    Date,

    /// TIMESTAMP type
    #[serde(rename = "TIMESTAMP")]
    Timestamp { with_local_time_zone: bool },

    /// INTERVAL YEAR TO MONTH
    #[serde(rename = "INTERVAL YEAR TO MONTH")]
    IntervalYearToMonth,

    /// INTERVAL DAY TO SECOND
    #[serde(rename = "INTERVAL DAY TO SECOND")]
    IntervalDayToSecond { precision: u8 },

    /// GEOMETRY type
    #[serde(rename = "GEOMETRY")]
    Geometry { srid: Option<i32> },

    /// HASHTYPE type (for hash values)
    #[serde(rename = "HASHTYPE")]
    Hashtype { byte_size: usize },
}

/// Type mapper for converting between Exasol and Arrow types.
pub struct TypeMapper;

impl TypeMapper {
    /// Convert an Exasol type to an Arrow DataType.
    ///
    /// # Arguments
    /// * `exasol_type` - The Exasol type to convert
    /// * `nullable` - Whether the field is nullable
    ///
    /// # Returns
    /// The corresponding Arrow DataType
    ///
    /// # Errors
    /// Returns `ConversionError::UnsupportedType` if the type cannot be mapped
    pub fn exasol_to_arrow(
        exasol_type: &ExasolType,
        nullable: bool,
    ) -> Result<DataType, ConversionError> {
        let _ = nullable; // Arrow nullability is handled at the Field level

        match exasol_type {
            ExasolType::Boolean => Ok(DataType::Boolean),

            ExasolType::Char { .. } | ExasolType::Varchar { .. } => Ok(DataType::Utf8),

            ExasolType::Decimal { precision, scale } => {
                if *precision <= 38 {
                    Ok(DataType::Decimal128(*precision, *scale))
                } else {
                    Ok(DataType::Decimal256(*precision, *scale))
                }
            }

            ExasolType::Double => Ok(DataType::Float64),

            ExasolType::Date => Ok(DataType::Date32),

            ExasolType::Timestamp {
                with_local_time_zone,
            } => {
                if *with_local_time_zone {
                    // Timestamp with local timezone -> Timestamp with UTC timezone
                    Ok(DataType::Timestamp(
                        arrow_schema::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ))
                } else {
                    // Timestamp without timezone
                    Ok(DataType::Timestamp(
                        arrow_schema::TimeUnit::Microsecond,
                        None,
                    ))
                }
            }

            ExasolType::IntervalYearToMonth => {
                // Map to MonthDayNano interval (only using month component)
                Ok(DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano))
            }

            ExasolType::IntervalDayToSecond { .. } => {
                // Map to MonthDayNano interval (using day and nanosecond components)
                Ok(DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano))
            }

            ExasolType::Geometry { .. } => {
                // Geometry as binary (WKB - Well-Known Binary)
                Ok(DataType::Binary)
            }

            ExasolType::Hashtype { .. } => {
                // Hash values as fixed-size binary
                Ok(DataType::Binary)
            }
        }
    }

    /// Convert an Arrow DataType to an Exasol type.
    ///
    /// This is used for parameter binding and type inference.
    ///
    /// # Arguments
    /// * `arrow_type` - The Arrow type to convert
    ///
    /// # Returns
    /// The corresponding Exasol type
    ///
    /// # Errors
    /// Returns `ConversionError::UnsupportedType` if the type cannot be mapped
    pub fn arrow_to_exasol(arrow_type: &DataType) -> Result<ExasolType, ConversionError> {
        match arrow_type {
            DataType::Boolean => Ok(ExasolType::Boolean),

            DataType::Utf8 | DataType::LargeUtf8 => {
                // Default to VARCHAR(2000000) for string types
                Ok(ExasolType::Varchar { size: 2000000 })
            }

            DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(ExasolType::Decimal {
                precision: 18,
                scale: 0,
            }),

            DataType::Int64 => Ok(ExasolType::Decimal {
                precision: 36,
                scale: 0,
            }),

            DataType::Float32 | DataType::Float64 => Ok(ExasolType::Double),

            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                Ok(ExasolType::Decimal {
                    precision: *precision,
                    scale: *scale,
                })
            }

            DataType::Date32 | DataType::Date64 => Ok(ExasolType::Date),

            DataType::Timestamp(_, tz) => Ok(ExasolType::Timestamp {
                with_local_time_zone: tz.is_some(),
            }),

            DataType::Interval(_) => Ok(ExasolType::IntervalDayToSecond { precision: 3 }),

            DataType::Binary | DataType::LargeBinary => {
                Ok(ExasolType::Varchar { size: 2000000 }) // Store as hex string
            }

            _ => Err(ConversionError::UnsupportedType {
                exasol_type: format!("Arrow type {:?}", arrow_type),
            }),
        }
    }

    /// Create Arrow field metadata to preserve Exasol type information.
    ///
    /// This allows round-tripping type information.
    pub fn create_field_metadata(exasol_type: &ExasolType) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        metadata.insert(
            "exasol:type".to_string(),
            serde_json::to_string(exasol_type).unwrap_or_default(),
        );

        match exasol_type {
            ExasolType::Char { size } => {
                metadata.insert("exasol:size".to_string(), size.to_string());
            }
            ExasolType::Varchar { size } => {
                metadata.insert("exasol:size".to_string(), size.to_string());
            }
            ExasolType::Decimal { precision, scale } => {
                metadata.insert("exasol:precision".to_string(), precision.to_string());
                metadata.insert("exasol:scale".to_string(), scale.to_string());
            }
            ExasolType::Geometry { srid } => {
                if let Some(srid) = srid {
                    metadata.insert("exasol:srid".to_string(), srid.to_string());
                }
            }
            _ => {}
        }

        metadata
    }

    /// Extract Exasol type from Arrow field metadata.
    pub fn from_field_metadata(metadata: &HashMap<String, String>) -> Option<ExasolType> {
        metadata
            .get("exasol:type")
            .and_then(|s| serde_json::from_str(s).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_mapping() {
        let exasol_type = ExasolType::Boolean;
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert_eq!(arrow_type, DataType::Boolean);

        let reverse = TypeMapper::arrow_to_exasol(&arrow_type).unwrap();
        assert_eq!(reverse, ExasolType::Boolean);
    }

    #[test]
    fn test_string_mapping() {
        let exasol_type = ExasolType::Varchar { size: 100 };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert_eq!(arrow_type, DataType::Utf8);
    }

    #[test]
    fn test_decimal_mapping() {
        let exasol_type = ExasolType::Decimal {
            precision: 18,
            scale: 2,
        };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert_eq!(arrow_type, DataType::Decimal128(18, 2));

        let reverse = TypeMapper::arrow_to_exasol(&arrow_type).unwrap();
        assert_eq!(reverse, exasol_type);
    }

    #[test]
    fn test_decimal256_mapping() {
        let exasol_type = ExasolType::Decimal {
            precision: 40,
            scale: 5,
        };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert_eq!(arrow_type, DataType::Decimal256(40, 5));
    }

    #[test]
    fn test_timestamp_mapping() {
        let exasol_type = ExasolType::Timestamp {
            with_local_time_zone: false,
        };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert!(matches!(
            arrow_type,
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        ));
    }

    #[test]
    fn test_timestamp_with_tz_mapping() {
        let exasol_type = ExasolType::Timestamp {
            with_local_time_zone: true,
        };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert!(matches!(
            arrow_type,
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_))
        ));
    }

    #[test]
    fn test_date_mapping() {
        let exasol_type = ExasolType::Date;
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert_eq!(arrow_type, DataType::Date32);
    }

    #[test]
    fn test_metadata_preservation() {
        let exasol_type = ExasolType::Decimal {
            precision: 18,
            scale: 2,
        };

        let metadata = TypeMapper::create_field_metadata(&exasol_type);
        assert!(metadata.contains_key("exasol:type"));
        assert_eq!(metadata.get("exasol:precision"), Some(&"18".to_string()));
        assert_eq!(metadata.get("exasol:scale"), Some(&"2".to_string()));

        let restored = TypeMapper::from_field_metadata(&metadata).unwrap();
        assert_eq!(restored, exasol_type);
    }

    #[test]
    fn test_geometry_mapping() {
        let exasol_type = ExasolType::Geometry { srid: Some(4326) };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert_eq!(arrow_type, DataType::Binary);
    }

    #[test]
    fn test_interval_mapping() {
        let exasol_type = ExasolType::IntervalDayToSecond { precision: 3 };
        let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).unwrap();
        assert!(matches!(
            arrow_type,
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
        ));
    }
}
