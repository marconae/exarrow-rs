# Feature: Bulk Ingestion

Enables ADBC-standard bulk data ingestion into Exasol tables from Arrow RecordBatches, supporting create, append, replace, and schema-targeting modes via the existing HTTP transport import pipeline.

## Background

Bulk ingestion is triggered through the ADBC Statement interface by setting `IngestTargetTable` and `IngestMode` options before calling `execute()` with bound Arrow data. The driver converts the Arrow RecordBatch to the internal import format and uses the HTTP tunneling transport to load data into Exasol. Table DDL is generated from the Arrow schema when creating tables, with Exasol types derived from the existing Arrow-to-Exasol type mapping. Exasol does not support catalogs (a single synthetic catalog "EXA" is used) or `CREATE TEMPORARY TABLE`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Append to existing table

* *GIVEN* an ADBC statement with `IngestTargetTable` set to an existing table name
* *AND* `IngestMode` is set to `Append` (or not set, as append is the default)
* *AND* Arrow data is bound to the statement
* *WHEN* the statement is executed
* *THEN* the driver SHALL insert the bound data into the existing table via HTTP transport import
* *AND* the driver SHALL return the number of rows ingested

### Scenario: Create new table and ingest

* *GIVEN* an ADBC statement with `IngestTargetTable` set to a table name that does not exist
* *AND* `IngestMode` is set to `Create`
* *AND* Arrow data is bound to the statement
* *WHEN* the statement is executed
* *THEN* the driver SHALL generate a `CREATE TABLE` statement from the Arrow schema
* *AND* the driver SHALL map Arrow types to Exasol types using the existing type mapping
* *AND* the driver SHALL insert the bound data into the newly created table
* *AND* the driver SHALL return the number of rows ingested

### Scenario: Create or append (create if not exists)

* *GIVEN* an ADBC statement with `IngestTargetTable` set to a table name
* *AND* `IngestMode` is set to `CreateAppend`
* *AND* Arrow data is bound to the statement
* *WHEN* the statement is executed
* *THEN* the driver SHALL create the table if it does not exist (using `CREATE TABLE IF NOT EXISTS`)
* *AND* the driver SHALL insert the bound data into the table
* *AND* the driver SHALL return the number of rows ingested

### Scenario: Replace table contents

* *GIVEN* an ADBC statement with `IngestTargetTable` set to an existing table name
* *AND* `IngestMode` is set to `Replace`
* *AND* Arrow data is bound to the statement
* *WHEN* the statement is executed
* *THEN* the driver SHALL drop the existing table
* *AND* the driver SHALL create a new table from the Arrow schema
* *AND* the driver SHALL insert the bound data into the new table
* *AND* the driver SHALL return the number of rows ingested

### Scenario: Target schema specification

* *GIVEN* an ADBC statement with `IngestTargetTable` set to a table name
* *AND* a target schema is specified via `adbc.ingest.target_db_schema` option
* *WHEN* the statement is executed
* *THEN* the driver SHALL create or reference the table in the specified schema
* *AND* the driver SHALL use fully qualified table names (`schema.table`)

### Scenario: NOT NULL propagation from Arrow schema

* *GIVEN* an ADBC statement with `IngestMode` set to `Create` or `CreateAppend` or `Replace`
* *AND* the bound Arrow data has fields marked as non-nullable
* *WHEN* the driver generates `CREATE TABLE` DDL
* *THEN* non-nullable Arrow fields SHALL produce columns with `NOT NULL` constraints
* *AND* nullable Arrow fields SHALL produce columns without `NOT NULL` constraints

### Scenario: Dotted table name without explicit schema

* *GIVEN* an ADBC statement with `IngestTargetTable` set to a dotted name (e.g., `SCHEMA.TABLE`)
* *AND* no target schema is specified via `adbc.ingest.target_db_schema` option
* *WHEN* the statement is executed
* *THEN* the driver SHALL split the table name on the first dot
* *AND* the part before the dot SHALL be used as the schema name
* *AND* the part after the dot SHALL be used as the table name
* *AND* both parts SHALL be individually quoted as identifiers

### Scenario: Temporary table not supported

* *GIVEN* an ADBC statement with the temporary table ingestion option enabled
* *WHEN* the statement is executed
* *THEN* the driver SHALL return an error with status `NotImplemented`
* *AND* the error message SHALL indicate that Exasol does not support temporary tables
<!-- /DELTA:NEW -->
