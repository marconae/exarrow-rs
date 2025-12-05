//! ADBC Statement type - re-exports query::Statement for backward compatibility.
//!
//! Statement is now a pure data container. Execution is performed by Connection.

pub use crate::query::statement::{Parameter, Statement, StatementType};
