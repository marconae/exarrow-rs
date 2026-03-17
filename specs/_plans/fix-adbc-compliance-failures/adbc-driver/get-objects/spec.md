# Feature: GetObjects

## Background

Exasol uses a single synthetic catalog named "EXA". The ADBC validation tests expect `CurrentCatalog` to return this value so that get_objects results can be matched against expected catalog names.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: GetObjects at catalog depth

* *GIVEN* an active ADBC connection to Exasol
* *WHEN* `get_objects` is called with `depth=Catalogs`
* *THEN* the driver SHALL return an Arrow RecordBatch with a single row
* *AND* the `catalog_name` field SHALL be "EXA"
* *AND* the `CurrentCatalog` connection option SHALL return "EXA" via `get_option_string`
<!-- /DELTA:CHANGED -->
