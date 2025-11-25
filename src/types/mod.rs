//! Type mapping between Exasol and Arrow data types.

mod mapping;
mod schema;

pub use mapping::{ExasolType, TypeMapper};
pub use schema::{ColumnMetadata, SchemaBuilder};
