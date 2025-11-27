//! Connection management for Exasol database connections.
//!
//! This module provides connection parameter parsing, authentication,
//! and session management functionality.
//!
//! # Example
//!
//! ```no_run
//! # use exarrow_rs::connection::{ConnectionBuilder, ConnectionParams};
//! # use std::str::FromStr;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Using ConnectionBuilder
//! let params = ConnectionBuilder::new()
//!     .host("localhost")
//!     .port(8563)
//!     .username("sys")
//!     .password("exasol")
//!     .schema("MY_SCHEMA")
//!     .connection_timeout(std::time::Duration::from_secs(10))
//!     .build()?;
//!
//! // Or parse from connection string
//! let params = ConnectionParams::from_str(
//!     "exasol://sys:exasol@localhost:8563/MY_SCHEMA?timeout=10"
//! )?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod params;
pub mod session;

pub use auth::{AuthenticationHandler, Credentials};
pub use params::{ConnectionBuilder, ConnectionParams};
pub use session::{Session, SessionConfig};
