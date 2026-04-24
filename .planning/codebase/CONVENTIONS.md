# Coding Conventions

**Analysis Date:** 2026-04-23

## Language & Runtime

- **Python Version**: `>=3.13`
- **Package Manager**: `uv`

## Formatting & Style

**Tool:** Ruff (with formatter enabled)

**Configuration** (`pyproject.toml`):
- Line length: 120 characters
- Excludes: `.git`, `.github`, `__pycache__`, `.ruff_cache`, `dist`, `.venv`

**Type Checking:**
- BasedPyright
- Extra paths: `components/`, `bases/`

## Naming Patterns

**Files:**
- Modules: `snake_case.py` (e.g., `core.py`, `lut.py`)
- Tests: `test_*.py` (e.g., `test_core.py`)

**Classes:**
- PascalCase (e.g., `AwsGoesExtractor`, `ExtractionTask`)

**Functions:**
- snake_case (e.g., `detect_reader`, `map_channel_ids_to_satpy_names`)

**Variables:**
- snake_case (e.g., `lut_dir_str`, `source_crop`)

**Constants:**
- UPPERCASE_SNAKE_CASE (e.g., `SUPPORTED_COLLECTIONS`, `L1B_PATTERN`)

## Docstring Format

**Style:** Google Style (documented in README line 98)

**Structure:**
```python
def function_name(param: Type) -> ReturnType:
    """Short description.

    Longer description if needed.

    Args:
        param: Description of param.

    Returns:
        Description of return value.

    Raises:
        ExceptionType: When this exception is raised.
    """
```

**Examples from `components/aer/extract_aws_goes/core.py`:**
- Lines 57-71: Multi-line docstring with Args section
- Lines 121-143: Full docstring with Args, Returns sections
- Lines 797-811: Full docstring documenting calibration parameters in Returns

## Type Hints

**Approach:**
- Full type annotations on public APIs
- Uses `typing.cast` for downcasts (line 6, 494, 735)
- Uses `@override` decorator from `typing` for method overriding (lines 112, 117, 172, 895)

**Union Syntax:**
- `Type | None` (Python 3.13+)

**Examples:**
```python
from typing import Any, cast, override

@property
@override
def target_grid_d(self) -> int:
    return self._target_grid_d
```

```python
return cast(GeoDataFrame[ArtifactSchema], validated)
```

## Import Organization

**Order:**
1. Standard library (`import` then `from`)
2. Third-party packages
3. Local application imports

**Example from `core.py` lines 1-22:**
```python
import gc                # stdlib
import hashlib
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Sequence, cast, override

import rioxarray        # third-party

import geopandas as gpd  # third-party
import numpy as np
import pandas as pd
import s3fs
from aer.grid import GridCell  # local application
from aer.interfaces import ExtractionTask, Extractor
```

## Error Handling

**Pattern:**
- Explicit exception raising with descriptive messages
- Logger-based error tracking with structured logging via structlog

**Examples:**
```python
if uri is None:
    raise ValueError(
        "Default prepare_for_extraction requires resolution and uri to be defined"
        "If you want to prepare without resolution or uri, you need to override this method with a custom implementation."
    )
```

```python
except Exception as exc:
    logger.error(
        "cell_extract_failed",
        error=str(exc),
        grid_cell=gc_.id(),
        engine="lut",
    )
    return None
```

## Logging

**Framework:** structlog

**Usage:**
```python
from structlog import get_logger

logger = get_logger()

logger.info("file_downloaded", local_path=str(local_path))
```

---

*Convention analysis: 2026-04-23*