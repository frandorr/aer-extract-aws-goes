# Folder Structure

## Repository Root
- **components/**: The core logic of the plugin system (Polylith bricks).
  - `aer/extract_aws_goes/`: Main plugin component.
- **projects/**: The definitions of individual packages to be published.
  - `aer-extract-aws-goes/`: Main project package.
- **test/**: Test scripts mirroring the structure of `components` or `bases`.
  - `components/aer/extract_aws_goes/`: Unit tests for the main extraction logic.
- **development/**: Local scripts, notebooks, and temporary files safely ignored from git.
  - `local/`: Scripts and notebooks for developer use.
- **.planning/**: GSD-related documentation and state.

## Configuration Files
- `pyproject.toml`: Main workspace configuration (uv, Ruff, Basedpyright, Hatch).
- `workspace.toml`: Polylith workspace details.
- `uv.lock`: Dependency lock file for the workspace.
- `PUBLISH.md`: Documentation for publishing the package.
