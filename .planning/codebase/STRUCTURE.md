# Codebase Structure

**Analysis Date:** 2026-04-23

## Directory Layout

```
aer-extract-aws-goes/
├── .planning/codebase/        # GSD documentation (generated)
├── .agents/                   # Agent scripts
│   └── scripts/release.py
├── components/                # Polylith bricks (source code)
│   └── aer/extract_aws_goes/
│       ├── __init__.py        # Public API exports
│       ├── core.py            # Main extractor plugin (~930 lines)
│       ├── lut.py            # LUT engine (~587 lines)
│       └── generate_luts.py  # LUT generator CLI (~77 lines)
├── projects/                 # Polylith projects (package definitions)
│   └── aer-extract-aws-goes/
│       ├── pyproject.toml
│       └── README.md
├── test/                     # Test files mirroring components
│   └── components/aer/extract_aws_goes/
│       ├── __init__.py
│       ├── test_core.py      # Core plugin tests (~135 lines)
│       ├── test_lut.py
│       └── test_utm_grouping.py
├── development/              # Developer local files
│   └── local/
├── luts/                    # Pre-computed LUTs (gitignored)
├── .env                     # Environment variables
├── pyproject.toml           # Root workspace config
├── workspace.toml           # Polylith workspace config
├── uv.lock                  # Dependency lockfile
└── setup.sh                # Setup script
```

## Directory Purposes

**components/aer/extract_aws_goes/**
- Purpose: Core plugin source code (Polylith brick)
- Contains: Python modules for extraction
- Key files:
  - `core.py`: Main `AwsGoesExtractor` plugin class
  - `lut.py`: LUT generation and extraction engine
  - `generate_luts.py`: CLI for LUT generation
  - `__init__.py`: Public API exports

**projects/aer-extract-aws-goes/**
- Purpose: Package definition for PyPI/publish
- Contains: `pyproject.toml` with build config
- Key files:
  - `pyproject.toml`: Package metadata, entry points, dependencies
  - `README.md`: Package-specific readme

**test/components/aer/extract_aws_goes/**
- Purpose: Unit tests following Polylith/pytest conventions
- Contains: Test modules matching component structure
- Key files:
  - `test_core.py`: Tests for AwsGoesExtractor class
  - `test_lut.py`: Tests for LUT functions
  - `test_utm_grouping.py`: Tests for UTM grouping logic

**development/local/**
- Purpose: Developer scripts, notebooks (gitignored)
- Contains: Temporary development files

## Key File Locations

**Entry Points:**
- `components/aer/extract_aws_goes/core.py`: `AwsGoesExtractor` class (line 86)
- `components/aer/extract_aws_goes/generate_luts.py`: CLI main function (line 27)

**Configuration:**
- `pyproject.toml`: Root workspace (dependencies, dev groups, pytest config)
- `workspace.toml`: Polylith namespace and structure config
- `projects/aer-extract-aws-goes/pyproject.toml`: Package definition with entry point

**Core Logic:**
- `components/aer/extract_aws_goes/core.py`: Main ~930 lines
- `components/aer/extract_aws_goes/lut.py`: LUT engine ~587 lines

**Testing:**
- `test/components/aer/extract_aws_goes/test_core.py`: ~135 lines

## Naming Conventions

**Files:**
- Components: `snake_case.py`
- Tests: `test_<module>.py`
- CLI: `generate_luts.py`

**Directories:**
- Polylith: `components/<namespace>/<brick>/`
- Tests: `test/<mirrored path>/`

**Classes:**
- PascalCase: `AwsGoesExtractor`, `UTMZoneLUT`
- Functions: `snake_case`

**Constants:**
- UPPER_SNAKE_CASE: `L1B_PATTERN`, `SUPPORTED_COLLECTIONS`

## Where to Add New Code

**New Feature/Method:**
- Implementation: `components/aer/extract_aws_goes/core.py`
- Add to `AwsGoesExtractor` class or module-level functions

**New LUT Utility:**
- Implementation: `components/aer/extract_aws_goes/lut.py`
- Add as module-level function

**New Test:**
- Tests: `test/components/aer/extract_aws_goes/test_<feature>.py`
- Follow `test_core.py` patterns

**New Project/Package:**
- Create new: `projects/<project-name>/pyproject.toml`
- Update root `pyproject.toml` `[tool.uv.workspace.members]`

## Special Directories

**luts/**
- Purpose: Pre-computed lookup tables
- Generated: Yes (by `generate_luts.py`)
- Committed: No (gitignored - stored in Hugging Face bucket)

**.agents/scripts/**
- Purpose: Custom agent scripts
- Generated: No
- Committed: Yes

**.planning/codebase/**
- Purpose: GSD codebase analysis documents
- Generated: Yes (/gsd-map-codebase)
- Committed: Yes

---

*Structure analysis: 2026-04-23*