# Feature: Transactions (ADBC)

Implements proper ADBC transaction support by controlling autocommit via the Exasol WebSocket `setAttributes` command, ensuring that begin/commit/rollback operations correctly control transaction boundaries at the server level.

## Background

Exasol supports transactions through the `autocommit` session attribute. When autocommit is `true` (the default, set at login time), each statement is immediately committed. When autocommit is `false` (toggled via the `setAttributes` WebSocket command), statements accumulate in an open transaction until an explicit `COMMIT` or `ROLLBACK` is issued. The ADBC specification exposes transactions through `Connection::commit()` and `Connection::rollback()` methods, with autocommit controlled via the `AutoCommit` connection option.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Autocommit enabled by default

* *GIVEN* a new ADBC connection to Exasol
* *WHEN* a DML statement is executed without explicitly disabling autocommit
* *THEN* the session SHALL have `autocommit` enabled at login time via connection attributes
* *AND* the statement's effects SHALL be immediately visible to other sessions

### Scenario: Disable autocommit for explicit transactions

* *GIVEN* an ADBC connection with autocommit set to `false`
* *WHEN* autocommit is disabled
* *THEN* the driver SHALL send a `setAttributes` command to disable autocommit on the server
* *AND* subsequent DML statements SHALL accumulate in an open transaction
* *AND* the statement's effects SHALL NOT be visible to other sessions until commit

### Scenario: Commit transaction

* *GIVEN* an ADBC connection with autocommit disabled
* *AND* one or more DML statements have been executed
* *WHEN* `commit()` is called on the connection
* *THEN* the driver SHALL execute a `COMMIT` statement
* *AND* all pending changes SHALL become visible to other sessions
* *AND* the driver SHALL send a `setAttributes` command to re-enable autocommit on the server

### Scenario: Rollback transaction

* *GIVEN* an ADBC connection with autocommit disabled
* *AND* one or more DML statements have been executed
* *WHEN* `rollback()` is called on the connection
* *THEN* the driver SHALL execute a `ROLLBACK` statement
* *AND* all pending changes SHALL be discarded
* *AND* the driver SHALL send a `setAttributes` command to re-enable autocommit on the server

### Scenario: Toggle autocommit

* *GIVEN* an ADBC connection with autocommit in any state
* *WHEN* the `AutoCommit` connection option is changed
* *THEN* the driver SHALL send a `setAttributes` command to update autocommit on the server
* *AND* if switching from manual to auto mode and a transaction is active, the driver SHALL commit it first
* *AND* if switching from manual to auto mode and no transaction is active, the driver SHALL skip the commit
<!-- /DELTA:CHANGED -->
