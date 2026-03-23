# Integration Overview

## Internal Integrations
- **aer-core**: The base plugin system for the AER framework.
- **aer-search-aws-goes**: Related search plugin functionality (referenced in main `pyproject.toml` dependencies).

## Third-Party Integrations
- **AWS**: Accessed indirectly through `aer.download_api` for retrieving binary data.
- **GOES (Geostationary Operational Environmental Satellite)**: GOES data processing via `aer-search-aws-goes` and extraction processes.

## External APIs
- **AER Registry**: Plugins are discovered via `entry-points` (`project.entry-points."aer.plugins"`).
