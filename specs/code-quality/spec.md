# code-quality Specification

## Purpose

Specifies code quality standards for the exarrow-rs codebase, ensuring zero clippy warnings, clean builds across all
targets and features, and comprehensive test coverage for all changes.

## Requirements

### Requirement: Clean Clippy Builds

The codebase SHALL maintain zero clippy warnings when built with all targets and features.

#### Scenario: Clippy validation

- **WHEN** running `cargo clippy --all-targets --all-features -- -W clippy::all`
- **THEN** it SHALL produce zero warnings
- **AND** it SHALL pass all lint checks

#### Scenario: Test code lint exceptions

- **WHEN** test code uses approximate values for constants (e.g., 3.14 for PI)
- **THEN** it SHALL use `#[allow(clippy::approx_constant)]` attribute
- **AND** the lint exception SHALL be scoped to the test module only

### Requirement: Test Suite Integrity

ALL code changes MUST pass the complete test suite before being considered complete.

#### Scenario: Unit test validation

- **WHEN** clippy fixes are applied
- **THEN** `cargo test` MUST pass with zero failures
- **AND** no existing functionality SHALL be broken

#### Scenario: Integration test validation

- **WHEN** clippy fixes are applied
- **THEN** integration tests against Exasol database MUST pass
- **AND** driver functionality SHALL remain intact

