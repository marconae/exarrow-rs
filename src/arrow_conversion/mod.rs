//! Arrow data format conversion utilities.
//!
//! This module handles conversion between Exasol WebSocket JSON responses
//! and Apache Arrow columnar format.

mod builders;
mod converter;

pub use builders::{build_array, ArrayBuilder};
pub use converter::ArrowConverter;

// TODO: Add tests module
// #[cfg(test)]
// mod tests;
