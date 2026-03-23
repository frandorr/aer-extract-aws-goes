# Testing Strategy

## Current Coverage
- **Unit Testing**: Standard `pytest` for testing components in `test/components/`.
- **Sample Tests**: `test_sample.py` exists as a placeholder for functionality.

## Execution
- **Run Tests**: Use `pytest` on the `test` directory.
- **Tools**: `pytest`, `pytest-cov` for coverage reports.

## Requirements
- **Location**: Mirror component structure (`test/components/aer/extract_aws_goes`).
- **Imports**: Direct component imports for unit tests.

## Future Plans
- **Integration Tests**: Adding tests that verify actual downloads against AWS/GOES.
- **Schema Validation**: Testing that search results meet the required schemas.
