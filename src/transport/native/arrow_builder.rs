use super::constants::*;
use super::result_parser::NativeColumnMeta;

/// Map Exasol native type metadata to the corresponding ColumnInfo DataType for compatibility.
pub fn native_meta_to_data_type(meta: &NativeColumnMeta) -> crate::transport::messages::DataType {
    match meta.type_id {
        T_DECIMAL | T_SMALLDECIMAL | T_BIGDECIMAL => crate::transport::messages::DataType {
            type_name: "DECIMAL".to_string(),
            precision: meta.precision,
            scale: meta.scale,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_DOUBLE => crate::transport::messages::DataType::double(),
        T_REAL => crate::transport::messages::DataType {
            type_name: "DOUBLE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_INTEGER => crate::transport::messages::DataType::decimal(18, 0),
        T_SMALLINT => crate::transport::messages::DataType::decimal(9, 0),
        T_BOOLEAN => crate::transport::messages::DataType::boolean(),
        T_CHAR => {
            if meta.is_varchar {
                crate::transport::messages::DataType::varchar(
                    meta.max_len.unwrap_or(2_000_000) as i64
                )
            } else {
                crate::transport::messages::DataType {
                    type_name: "CHAR".to_string(),
                    precision: None,
                    scale: None,
                    size: meta.max_len.map(|l| l as i64),
                    character_set: Some("UTF8".to_string()),
                    with_local_time_zone: None,
                    fraction: None,
                }
            }
        }
        T_DATE => crate::transport::messages::DataType {
            type_name: "DATE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_TIMESTAMP => crate::transport::messages::DataType {
            type_name: "TIMESTAMP".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_TIMESTAMP_LOCAL_TZ => crate::transport::messages::DataType {
            type_name: "TIMESTAMP WITH LOCAL TIME ZONE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: Some(true),
            fraction: None,
        },
        T_TIMESTAMP_UTC => crate::transport::messages::DataType {
            type_name: "TIMESTAMP WITH LOCAL TIME ZONE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: Some(true),
            fraction: None,
        },
        T_BINARY => crate::transport::messages::DataType {
            type_name: "BINARY".to_string(),
            precision: None,
            scale: None,
            size: meta.max_len.map(|l| l as i64),
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_GEOMETRY => crate::transport::messages::DataType {
            type_name: "GEOMETRY".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_HASHTYPE => crate::transport::messages::DataType {
            type_name: "HASHTYPE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_INTERVAL_YEAR => crate::transport::messages::DataType {
            type_name: "INTERVAL YEAR TO MONTH".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        T_INTERVAL_DAY => crate::transport::messages::DataType {
            type_name: "INTERVAL DAY TO SECOND".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        },
        _ => crate::transport::messages::DataType::varchar(2_000_000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_integer_to_decimal_18_0() {
        let meta = NativeColumnMeta {
            name: "x".to_string(),
            type_id: T_INTEGER,
            precision: None,
            scale: None,
            is_varchar: false,
            max_len: None,
        };
        let dt = native_meta_to_data_type(&meta);
        assert_eq!(dt.type_name, "DECIMAL");
        assert_eq!(dt.precision, Some(18));
        assert_eq!(dt.scale, Some(0));
    }

    #[test]
    fn maps_char_with_varchar_flag_to_varchar() {
        let meta = NativeColumnMeta {
            name: "name".to_string(),
            type_id: T_CHAR,
            precision: None,
            scale: None,
            is_varchar: true,
            max_len: Some(100),
        };
        let dt = native_meta_to_data_type(&meta);
        assert_eq!(dt.type_name, "VARCHAR");
        assert_eq!(dt.size, Some(100));
    }

    #[test]
    fn maps_timestamp_utc_with_time_zone_flag() {
        let meta = NativeColumnMeta {
            name: "ts".to_string(),
            type_id: T_TIMESTAMP_UTC,
            precision: None,
            scale: None,
            is_varchar: false,
            max_len: None,
        };
        let dt = native_meta_to_data_type(&meta);
        assert_eq!(dt.type_name, "TIMESTAMP WITH LOCAL TIME ZONE");
        assert_eq!(dt.with_local_time_zone, Some(true));
    }
}
