//! Schema building utilities for converting Exasol result metadata to Arrow schemas.

use crate::error::ConversionError;
use crate::types::{ExasolType, TypeMapper};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

/// Column metadata from Exasol result set.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// Exasol data type
    pub data_type: ExasolType,
    /// Whether the column is nullable
    pub nullable: bool,
}

/// Builder for constructing Arrow schemas from Exasol metadata.
pub struct SchemaBuilder {
    columns: Vec<ColumnMetadata>,
}

impl SchemaBuilder {
    /// Create a new schema builder.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Add a column to the schema.
    pub fn add_column(mut self, column: ColumnMetadata) -> Self {
        self.columns.push(column);
        self
    }

    /// Add multiple columns to the schema.
    pub fn add_columns(mut self, columns: Vec<ColumnMetadata>) -> Self {
        self.columns.extend(columns);
        self
    }

    /// Build the Arrow schema.
    ///
    /// # Returns
    /// An Arrow Schema with all columns converted to Arrow types
    ///
    /// # Errors
    /// Returns `ConversionError` if any column type cannot be mapped
    pub fn build(self) -> Result<Schema, ConversionError> {
        let fields: Result<Vec<Field>, ConversionError> = self
            .columns
            .iter()
            .map(|col| {
                let arrow_type = TypeMapper::exasol_to_arrow(&col.data_type, col.nullable)?;
                let metadata = TypeMapper::create_field_metadata(&col.data_type);

                Ok(Field::new(&col.name, arrow_type, col.nullable).with_metadata(metadata))
            })
            .collect();

        Ok(Schema::new(fields?))
    }

    /// Build the Arrow schema with additional schema-level metadata.
    pub fn build_with_metadata(
        self,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<Schema, ConversionError> {
        let schema = self.build()?;
        Ok(schema.with_metadata(metadata))
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to extract column metadata from Exasol WebSocket response.
///
/// This will be used by the Arrow converter to parse result set metadata.
pub fn extract_column_metadata_from_json(
    columns: &[serde_json::Value],
) -> Result<Vec<ColumnMetadata>, ConversionError> {
    columns
        .iter()
        .map(|col| {
            let name = col
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ConversionError::InvalidFormat("Missing column name".to_string()))?
                .to_string();

            let data_type: ExasolType =
                serde_json::from_value(col.get("dataType").cloned().ok_or_else(|| {
                    ConversionError::InvalidFormat("Missing dataType field".to_string())
                })?)
                .map_err(|e| ConversionError::InvalidFormat(format!("Invalid dataType: {}", e)))?;

            // Exasol columns are nullable by default unless specified otherwise
            let nullable = true;

            Ok(ColumnMetadata {
                name,
                data_type,
                nullable,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let schema = SchemaBuilder::new()
            .add_column(ColumnMetadata {
                name: "id".to_string(),
                data_type: ExasolType::Decimal {
                    precision: 18,
                    scale: 0,
                },
                nullable: false,
            })
            .add_column(ColumnMetadata {
                name: "name".to_string(),
                data_type: ExasolType::Varchar { size: 100 },
                nullable: true,
            })
            .add_column(ColumnMetadata {
                name: "price".to_string(),
                data_type: ExasolType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                nullable: true,
            })
            .build()
            .unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "price");

        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_extract_column_metadata() {
        let json = serde_json::json!([
            {
                "name": "id",
                "dataType": {
                    "type": "DECIMAL",
                    "precision": 18,
                    "scale": 0
                }
            },
            {
                "name": "name",
                "dataType": {
                    "type": "VARCHAR",
                    "size": 100
                }
            }
        ]);

        let columns = extract_column_metadata_from_json(json.as_array().unwrap()).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
    }

    #[test]
    fn test_schema_with_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "exasol".to_string());

        let schema = SchemaBuilder::new()
            .add_column(ColumnMetadata {
                name: "id".to_string(),
                data_type: ExasolType::Decimal {
                    precision: 18,
                    scale: 0,
                },
                nullable: false,
            })
            .build_with_metadata(metadata.clone())
            .unwrap();

        assert_eq!(schema.metadata().get("source"), Some(&"exasol".to_string()));
    }
}
