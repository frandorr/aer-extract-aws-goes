# Concerns & Technical Debt

## Known Issues
- **Minimal Testing**: Current testing is very basic and does not cover real-world use cases.
- **Dependency Versioning**: `aer-core` and `aer-search-aws-goes` versions should be closely tracked for compatibility.

## Technical Debt
- **Plugin Registry**: No automated way to verify that entry points are valid and matching the component functions.
- **Error Handling**: The `extract_aws_goes` function depends on `download()` directly; no error handling for failed downloads in the plugin yet.

## Future Plans
- **CI/CD Integration**: Ensuring tests and linting run on every push.
- **Enhanced Logic**: Moving beyond a simple wrapper for `download` if more complex extraction logic is needed.
- **Documentation**: More user-facing documentation for common use cases.
