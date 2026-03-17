# Feature: Transactions (ADBC)

## Background

Exasol supports transactions through the `autocommit` session attribute. When autocommit is toggled via the ADBC `AutoCommit` connection option, the driver must ensure the connection is established before sending `setAttributes` to the server.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Toggle autocommit

* *GIVEN* an ADBC connection with autocommit in any state
* *WHEN* the `AutoCommit` connection option is changed
* *THEN* the driver SHALL ensure the connection is established before applying the change
* *AND* the driver SHALL send a `setAttributes` command to update autocommit on the server
* *AND* if switching from auto to manual mode, the driver SHALL begin a transaction
<!-- /DELTA:CHANGED -->
