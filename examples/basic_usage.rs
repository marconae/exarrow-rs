//! Basic usage example for exarrow-rs ADBC driver.

use exarrow_rs::adbc::{Connection, Driver};
use std::error::Error;

const HOST: &str = "localhost";
const PORT: u16 = 8563;
const USER: &str = "sys";
const PASSWORD: &str = "exasol";
const VALIDATE_CERT: bool = false; // Set to false for Docker/self-signed certs
const SCHEMA: &str = "exarrow";

/// Establishes a connection to the Exasol database.
async fn example_connection() -> Result<Connection, Box<dyn Error>> {
    let driver = Driver::new();
    let conn_string = format!(
        "exasol://{}:{}@{}:{}?tls=1&validateservercertificate={}",
        USER, PASSWORD, HOST, PORT, VALIDATE_CERT as u8
    );
    let database = driver.open(&conn_string)?;
    let connection = database.connect().await?;
    Ok(connection)
}

/// Executes a simple arithmetic query and returns the row count.
async fn example_simple_select(conn: &mut Connection) -> Result<usize, Box<dyn Error>> {
    let results = conn.query("SELECT 1+1").await?;
    let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
    Ok(row_count)
}

/// Demonstrates a full transaction: create schema, table, insert, commit, select, cleanup.
async fn example_transaction(conn: &mut Connection) -> Result<usize, Box<dyn Error>> {
    // Create schema if it doesn't exist (ignore error if already exists)
    let _ = conn
        .execute_update(&format!("CREATE SCHEMA {}", SCHEMA))
        .await;

    conn.execute_update(&format!(
        "CREATE TABLE {}.test_example (id INT, name VARCHAR(100))",
        SCHEMA
    ))
    .await?;

    conn.begin_transaction().await?;
    conn.execute_update(&format!(
        "INSERT INTO {}.test_example VALUES (1, 'Alice')",
        SCHEMA
    ))
    .await?;

    conn.execute_update(&format!(
        "INSERT INTO {}.test_example VALUES (2, 'Bob')",
        SCHEMA
    ))
    .await?;

    conn.execute_update(&format!(
        "INSERT INTO {}.test_example VALUES (3, 'Charlie')",
        SCHEMA
    ))
    .await?;

    conn.commit().await?;

    let results = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_example ORDER BY id",
            SCHEMA
        ))
        .await?;
    let row_count: usize = results.iter().map(|b| b.num_rows()).sum();

    conn.execute_update(&format!("DROP TABLE {}.test_example", SCHEMA))
        .await?;
    conn.execute_update(&format!("DROP SCHEMA {}", SCHEMA))
        .await?;
    Ok(row_count)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut conn = example_connection().await?;
    println!("Connected: session {}", conn.session_id());

    let rows = example_simple_select(&mut conn).await?;
    println!("Simple select: {} row(s)", rows);

    let rows = example_transaction(&mut conn).await?;
    println!("Transaction: {} row(s)", rows);

    conn.close().await?;
    println!("Done");

    Ok(())
}
