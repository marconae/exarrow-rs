//! Benchmark for exarrow-rs import and Polars streaming performance.
//!
//! Measures import performance for CSV and Parquet files,
//! and SELECT streaming to Polars DataFrame.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use exarrow_rs::adbc::{Connection, Driver};
use exarrow_rs::import::{CsvImportOptions, ParquetImportOptions};
use polars::prelude::*;

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum Operation {
    ImportCsv,
    ImportParquet,
    SelectPolars,
}

#[derive(Parser, Debug)]
#[command(name = "benchmark")]
#[command(about = "Benchmark exarrow-rs import and Polars streaming performance")]
struct Args {
    /// Operation to benchmark
    #[arg(long, value_enum)]
    operation: Operation,

    /// Data size (e.g., 100mb, 1gb)
    #[arg(short, long, default_value = "100mb")]
    size: String,

    /// Number of benchmark iterations
    #[arg(short, long, default_value = "5")]
    iterations: usize,

    /// Number of warmup iterations
    #[arg(short, long, default_value = "1")]
    warmup: usize,

    /// Data directory
    #[arg(long, default_value = "benches/data")]
    data_dir: PathBuf,

    /// Output JSON file
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Transport protocol to use (native or websocket)
    #[arg(long, default_value = "native")]
    transport: String,
}

/// Connection configuration from environment
struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    validate_cert: bool,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            host: env::var("EXASOL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("EXASOL_PORT")
                .unwrap_or_else(|_| "8563".to_string())
                .parse()?,
            user: env::var("EXASOL_USER")
                .map_err(|_| "EXASOL_USER environment variable not set")?,
            password: env::var("EXASOL_PASSWORD")
                .map_err(|_| "EXASOL_PASSWORD environment variable not set")?,
            validate_cert: env::var("EXASOL_VALIDATE_CERT")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
        })
    }
}

async fn connect(
    config: &Config,
    transport: &str,
) -> Result<Connection, Box<dyn std::error::Error>> {
    let driver = Driver::new();
    let conn_string = format!(
        "exasol://{}:{}@{}:{}?tls=1&validateservercertificate={}&transport={}",
        config.user,
        config.password,
        config.host,
        config.port,
        config.validate_cert as u8,
        transport
    );
    let database = driver.open(&conn_string)?;
    let connection = database.connect().await?;
    Ok(connection)
}

fn get_schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("shared")
        .join("schema.sql")
}

async fn setup_table(conn: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    let schema_path = get_schema_path();
    let schema_sql = fs::read_to_string(&schema_path).map_err(|e| {
        format!(
            "Failed to read schema file {}: {}",
            schema_path.display(),
            e
        )
    })?;

    // Execute each statement separately (schema.sql has CREATE SCHEMA and CREATE TABLE)
    for statement in schema_sql.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() && !statement.starts_with("--") {
            let _ = conn.execute_update(statement).await;
        }
    }
    Ok(())
}

async fn truncate_table(conn: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_update("TRUNCATE TABLE benchmark.benchmark_data")
        .await?;
    Ok(())
}

async fn import_csv(
    conn: &mut Connection,
    file_path: &Path,
) -> Result<(i64, f64), Box<dyn std::error::Error>> {
    truncate_table(conn).await?;

    let options = CsvImportOptions::default().skip_rows(1); // Skip header

    let start = Instant::now();
    let rows = conn
        .import_csv_from_file("benchmark.benchmark_data", file_path, options)
        .await?;
    let elapsed = start.elapsed().as_secs_f64();

    Ok((rows as i64, elapsed))
}

async fn import_parquet(
    conn: &mut Connection,
    file_path: &Path,
) -> Result<(i64, f64), Box<dyn std::error::Error>> {
    truncate_table(conn).await?;

    let options = ParquetImportOptions::default();

    let start = Instant::now();
    let rows = conn
        .import_from_parquet("benchmark.benchmark_data", file_path, options)
        .await?;
    let elapsed = start.elapsed().as_secs_f64();

    Ok((rows as i64, elapsed))
}

async fn select_to_polars(
    conn: &mut Connection,
) -> Result<(i64, f64, DataFrame), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let all_batches = conn.query("SELECT * FROM benchmark.benchmark_data").await?;

    if all_batches.is_empty() {
        return Err("No data returned".into());
    }

    // Convert Arrow batches to Polars DataFrame via IPC format
    let mut ipc_buffer = Vec::new();
    {
        let schema = all_batches[0].schema();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc_buffer, &schema)?;
        for batch in &all_batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    // Read IPC buffer with Polars
    let cursor = std::io::Cursor::new(ipc_buffer);
    let df = IpcStreamReader::new(cursor).finish()?;

    let elapsed = start.elapsed().as_secs_f64();
    let row_count = df.height() as i64;

    Ok((row_count, elapsed, df))
}

async fn run_import_benchmark(
    conn: &mut Connection,
    operation: &Operation,
    file_path: &Path,
    iterations: usize,
    warmup: usize,
    file_size_mb: f64,
) -> Result<(Vec<f64>, i64), Box<dyn std::error::Error>> {
    // Warmup iterations
    println!("Running {} warmup iteration(s)...", warmup);
    for i in 0..warmup {
        let (rows, elapsed) = match operation {
            Operation::ImportCsv => import_csv(conn, file_path).await?,
            Operation::ImportParquet => import_parquet(conn, file_path).await?,
            _ => unreachable!(),
        };
        println!("  Warmup {}: {} rows in {:.3}s", i + 1, rows, elapsed);
    }

    // Benchmark iterations
    println!("Running {} benchmark iteration(s)...", iterations);
    let mut times = Vec::with_capacity(iterations);
    let mut total_rows: i64 = 0;

    for i in 0..iterations {
        let (rows, elapsed) = match operation {
            Operation::ImportCsv => import_csv(conn, file_path).await?,
            Operation::ImportParquet => import_parquet(conn, file_path).await?,
            _ => unreachable!(),
        };
        times.push(elapsed);
        total_rows = rows;
        println!(
            "  Iteration {}: {} rows in {:.3}s ({:.2} MB/s)",
            i + 1,
            rows,
            elapsed,
            file_size_mb / elapsed
        );
    }

    Ok((times, total_rows))
}

async fn run_select_polars_benchmark(
    conn: &mut Connection,
    iterations: usize,
    warmup: usize,
) -> Result<(Vec<f64>, i64), Box<dyn std::error::Error>> {
    // Warmup iterations
    println!("Running {} warmup iteration(s)...", warmup);
    for i in 0..warmup {
        let (rows, elapsed, _df) = select_to_polars(conn).await?;
        println!("  Warmup {}: {} rows in {:.3}s", i + 1, rows, elapsed);
    }

    // Benchmark iterations
    println!("Running {} benchmark iteration(s)...", iterations);
    let mut times = Vec::with_capacity(iterations);
    let mut total_rows: i64 = 0;

    for i in 0..iterations {
        let (rows, elapsed, _df) = select_to_polars(conn).await?;
        times.push(elapsed);
        total_rows = rows;
        println!(
            "  Iteration {}: {} rows in {:.3}s ({:.0} rows/s)",
            i + 1,
            rows,
            elapsed,
            rows as f64 / elapsed
        );
    }

    Ok((times, total_rows))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load .env from benches directory
    let benches_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches");
    let _ = dotenvy::from_path(benches_dir.join(".env"));

    let config = Config::from_env()?;

    match args.operation {
        Operation::ImportCsv => run_import_benchmark_main(&args, &config, "csv").await,
        Operation::ImportParquet => run_import_benchmark_main(&args, &config, "parquet").await,
        Operation::SelectPolars => run_select_polars_benchmark_main(&args, &config).await,
    }
}

async fn run_import_benchmark_main(
    args: &Args,
    config: &Config,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = args
        .data_dir
        .join(format!("benchmark_{}.{}", args.size, format));
    if !file_path.exists() {
        eprintln!("Data file not found: {}", file_path.display());
        eprintln!(
            "Run 'cargo run --release --features benchmark --bin generate_data -- --size {}' first",
            args.size
        );
        std::process::exit(1);
    }

    let file_size_bytes = fs::metadata(&file_path)?.len();
    let file_size_mb = file_size_bytes as f64 / (1024.0 * 1024.0);

    println!("exarrow-rs Import Benchmark: {} {}", format, args.size);
    println!("  Host: {}:{}", config.host, config.port);
    println!("  File: {} ({:.2} MB)", file_path.display(), file_size_mb);
    println!();

    let mut conn = connect(config, &args.transport).await?;
    setup_table(&mut conn).await?;

    let (times, total_rows) = run_import_benchmark(
        &mut conn,
        &args.operation,
        &file_path,
        args.iterations,
        args.warmup,
        file_size_mb,
    )
    .await?;

    conn.close().await?;

    let avg_time: f64 = times.iter().sum::<f64>() / times.len() as f64;
    let min_time: f64 = times.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_time: f64 = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    println!();
    println!("Results:");
    println!("  Avg time: {:.3}s", avg_time);
    println!("  Throughput: {:.2} MB/s", file_size_mb / avg_time);
    println!("  Rows/sec: {:.0}", total_rows as f64 / avg_time);

    // Build results JSON
    let results = serde_json::json!({
        "library": "exarrow-rs",
        "operation": format!("import-{}", format),
        "format": format,
        "size": args.size,
        "file_path": file_path.to_string_lossy(),
        "file_size_bytes": file_size_bytes,
        "file_size_mb": file_size_mb,
        "total_rows": total_rows,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "times_secs": times,
        "avg_time_secs": avg_time,
        "min_time_secs": min_time,
        "max_time_secs": max_time,
        "throughput_mb_per_sec": file_size_mb / avg_time,
        "rows_per_sec": total_rows as f64 / avg_time,
    });

    if let Some(output_path) = &args.output {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(output_path, serde_json::to_string_pretty(&results)?)?;
        println!("  Saved to: {}", output_path.display());
    }

    Ok(())
}

async fn run_select_polars_benchmark_main(
    args: &Args,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("exarrow-rs -> Polars Streaming Benchmark: {}", args.size);
    println!("  Host: {}:{}", config.host, config.port);
    println!("  Note: Data must already exist from prior import benchmark");
    println!();

    let mut conn = connect(config, &args.transport).await?;

    // Verify data exists
    let result = conn
        .query("SELECT COUNT(*) FROM benchmark.benchmark_data")
        .await;

    let row_count = match result {
        Ok(batches) => {
            if batches.is_empty() || batches[0].num_rows() == 0 {
                0i64
            } else {
                use arrow::array::{Array, Decimal128Array, Int64Array};
                let col = batches[0].column(0);
                // Try Int64 first, then Decimal128 (COUNT(*) may return DECIMAL in Exasol)
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    arr.value(0)
                } else if let Some(arr) = col.as_any().downcast_ref::<Decimal128Array>() {
                    // Decimal128 stores scaled value, but COUNT(*) has scale 0
                    arr.value(0) as i64
                } else {
                    0
                }
            }
        }
        Err(_) => 0,
    };

    if row_count == 0 {
        eprintln!("ERROR: No data in benchmark.benchmark_data table.");
        eprintln!("Run the import benchmark first to populate data:");
        eprintln!(
            "  cargo run --release --features benchmark --bin benchmark -- --operation import-csv --size {}",
            args.size
        );
        std::process::exit(1);
    }

    println!("  Data rows: {}", row_count);
    println!();

    let (times, total_rows) =
        run_select_polars_benchmark(&mut conn, args.iterations, args.warmup).await?;

    conn.close().await?;

    let avg_time: f64 = times.iter().sum::<f64>() / times.len() as f64;
    let min_time: f64 = times.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_time: f64 = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    println!();
    println!("Results:");
    println!("  Avg time: {:.3}s", avg_time);
    println!("  Rows/sec: {:.0}", total_rows as f64 / avg_time);

    // Build results JSON
    let results = serde_json::json!({
        "library": "exarrow-rs+polars",
        "operation": "select-polars",
        "size": args.size,
        "total_rows": total_rows,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "times_secs": times,
        "avg_time_secs": avg_time,
        "min_time_secs": min_time,
        "max_time_secs": max_time,
        "rows_per_sec": total_rows as f64 / avg_time,
    });

    if let Some(output_path) = &args.output {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(output_path, serde_json::to_string_pretty(&results)?)?;
        println!("  Saved to: {}", output_path.display());
    }

    Ok(())
}
