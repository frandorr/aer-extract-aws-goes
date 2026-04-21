---
phase: 08-utm-zone-lookup-table-extraction-engine
plan: 01
subsystem: extraction
tags: [pyresample, zarr, pyproj, netcdf, lut]

# Dependency graph
requires: []
provides:
  - UTMZoneLUT dataclass and lookup table utilities in `lut.py`
  - Offline generation script for pre-computing GOES to UTM grid indices
affects: [08-02-PLAN.md, aws_goes_extraction]

# Tech tracking
tech-stack:
  added: [zarr, pyproj]
  patterns: [Zarr array chunking for fast partial reads, pre-computation of nearest-neighbour indices]

key-files:
  created:
    - components/aer/extract_aws_goes/lut.py
    - components/aer/extract_aws_goes/generate_luts.py
  modified:
    - pyproject.toml

key-decisions:
  - "Extract limits of GOES disk using dense WGS84 point generation to ensure robust auto-detection of intersecting UTM zones."
  - "Robust CRS parsing using `pyproj.CRS` with explicit fallbacks from modern `pyresample` attributes to legacy `proj_dict`."

patterns-established:
  - "Store `pyresample` neighbourhood indices as `int32` and boolean masks using Zarr chunking for optimized memory loading of sub-regions."

requirements-completed: [REQ-08.1, REQ-08.2, REQ-08.4]

# Metrics
duration: 4min
completed: 2026-04-21T00:23:00Z
---

# Phase 08 Plan 01: UTM Zone LUT Generator Module Summary

**Standalone GOES→UTM Zarr LUT generator with auto-zone detection and chunked persistent arrays**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-21T00:19:00Z
- **Completed:** 2026-04-21T00:23:00Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments
- Implemented `lut.py` to auto-detect intersecting UTM zones for GOES data and compute pixel mapping arrays using `pyresample`.
- Created an offline generation CLI tool `generate_luts.py` for computing the persistent lookups in chunks.
- Added `zarr` and `pyproj` backend dependencies necessary for fast lookups.

## Task Commits

1. **Task 1: Create the `lut.py` module** - `f6368e5` (feat)
2. **Task 2: Create offline LUT generation script** - `f0396a9` (feat)
3. **Task 3: Add `zarr` dependency to pyproject.toml** - `4ef1003` (chore)

## Files Created/Modified
- `components/aer/extract_aws_goes/lut.py` - Core utilities for Pyresample Nearest Neighbour extraction bounding box processing and storage
- `components/aer/extract_aws_goes/generate_luts.py` - CLI script driving offline processing capabilities
- `pyproject.toml` - Declared new analytical dependencies (`zarr`, `pyproj`)

## Decisions Made
- Used exact pixel projection limits detection via WGS84 polygon point conversion to make GOES disk bounds detection robust across geometries.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
LUT generator is ready to be integrated into `AwsGoesExtractor.extract` as a primary engine.
