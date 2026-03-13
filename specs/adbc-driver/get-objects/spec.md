# Feature: GetObjects

Implements the ADBC `Connection::get_objects` method, returning database metadata at varying depths (catalogs, schemas, tables, columns) in the ADBC-standard nested Arrow format.

## Background

Exasol uses a single synthetic catalog named "EXA" since it has no multi-catalog concept. Schema metadata is queried from `SYS.EXA_ALL_SCHEMAS`, table metadata from `SYS.EXA_ALL_OBJECTS` (filtered by `OBJECT_TYPE`), and column metadata from `SYS.EXA_ALL_COLUMNS`. Results are returned as Arrow RecordBatches using the ADBC-defined nested schema with `List` and `Struct` types. The `ObjectDepth` parameter controls how deep the metadata tree is populated. Optional filter parameters (catalog, db_schema, table_name, table_type, column_name) restrict the returned results.

## Scenarios

### Scenario: GetObjects at catalog depth

* *GIVEN* an active ADBC connection to Exasol
* *WHEN* `get_objects` is called with `depth=Catalogs`
* *THEN* the driver SHALL return an Arrow RecordBatch with a single row
* *AND* the `catalog_name` field SHALL be "EXA"
* *AND* the `catalog_db_schemas` field SHALL be null (not populated at this depth)

### Scenario: GetObjects at db_schemas depth

* *GIVEN* an active ADBC connection to Exasol
* *WHEN* `get_objects` is called with `depth=DbSchemas`
* *THEN* the driver SHALL return the "EXA" catalog with a nested list of schemas
* *AND* each schema entry SHALL contain a `db_schema_name` from `SYS.EXA_ALL_SCHEMAS`
* *AND* the `db_schema_tables` field SHALL be null (not populated at this depth)

### Scenario: GetObjects at tables depth

* *GIVEN* an active ADBC connection to Exasol
* *WHEN* `get_objects` is called with `depth=Tables`
* *THEN* the driver SHALL return catalogs, schemas, and a nested list of tables per schema
* *AND* each table entry SHALL contain `table_name` and `table_type` from `SYS.EXA_ALL_OBJECTS`
* *AND* the `table_columns` field SHALL be null (not populated at this depth)

### Scenario: GetObjects at columns depth

* *GIVEN* an active ADBC connection to Exasol
* *WHEN* `get_objects` is called with `depth=All`
* *THEN* the driver SHALL return catalogs, schemas, tables, and a nested list of columns per table
* *AND* each column entry SHALL contain `column_name`, `ordinal_position`, and type metadata from `SYS.EXA_ALL_COLUMNS`
* *AND* the `xdbc_type_name` field SHALL contain the Exasol type name

### Scenario: GetObjects with filters

* *GIVEN* an active ADBC connection to Exasol
* *WHEN* `get_objects` is called with filter parameters (db_schema, table_name, or column_name)
* *THEN* the driver SHALL apply the filters to restrict returned metadata
* *AND* the catalog filter SHALL match against the synthetic "EXA" catalog name
* *AND* schema, table, and column filters SHALL be applied as SQL `LIKE` patterns where supported
