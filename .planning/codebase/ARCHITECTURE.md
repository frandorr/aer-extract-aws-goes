# Architectural Architecture

## Philosophy
- **Modular Plugin Pattern**: Decoupling extraction logic from the core framework.
- **Polylith Structure**: Using `components` for shared logic and `projects` for package delivery.
- **Functional API**: Using decorators (`@plugin`) to register and discover plugin functionality.

## Core Patterns
- **Plugin Registration**: Entry points in `pyproject.toml` define available plugins.
- **Data Schemas**: `pandera` schemas and `geopandas` types for data validation.
- **Shared Downloads**: Leveraging `aer.download_api` for standardized binary data retrieval.

## System Workflow
1. User requests an extraction.
2. The `aer` system finds the `aws_goes` extraction plugin.
3. The plugin receives a `GeoDataFrame` containing search results.
4. The plugin invokes the download API to fetch files to a destination directory.
