# Coding Conventions

## Language & Style
- **Python Version**: `>=3.13`
- **Formatting & Style**: [Ruff](https://astral.sh/ruff) (line length: 120).
- **Type Hints**: Mandatory for public APIs. Checked using `Basedpyright`.
- **Unit Testing**: Mirror component paths within `test/components/`.

## Naming & Organization
- **Plugins**: Prefixed with `extract_` for consistency.
- **Components**: Follow Polylith naming conventions (e.g., `aer/extract_aws_goes`).
- **Entry Points**: Consistent registration in `pyproject.toml` (`[project.entry-points."aer.plugins"]`).

## Data Handling
- **Schemas**: Standard `SearchResultSchema` for search results.
- **Data Frames**: `geopandas` for geospatial results.
- **Validation**: [Pandera](https://github.com/unionai-oss/pandera) for run-time validation.
