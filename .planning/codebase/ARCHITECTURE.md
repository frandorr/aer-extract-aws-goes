# Architecture

**Analysis Date:** 2026-04-23

## Pattern Overview

**Overall:** Plugin-based Satellite Data Extraction with Dual Processing Engines

**Key Characteristics:**
- `AwsGoesExtractor` plugin extends `Extractor` base class from `aer-core`
- Supports two extraction pipelines: satpy-based (default) and LUT-based (fastest)
- Uses pre-computed lookup tables (LUTs) for zero-reprojection extraction
- Groups grid cells by UTM zone for efficient batch processing

## Layers

**Plugin Layer:**
- Purpose: Main extractor plugin registered as aer plugin
- Location: `components/aer/extract_aws_goes/core.py`
- Contains: `AwsGoesExtractor` class definition (~930 lines)
- Depends on: `aer-core` interfaces (`Extractor`, `ExtractionTask`), `aer.grid.GridCell`
- Used by: aer framework via entry point

**LUT Engine Layer:**
- Purpose: High-performance LUT generation and loading
- Location: `components/aer/extract_aws_goes/lut.py` (~587 lines)
- Contains: UTM zone LUT generation, loading, and cell extraction functions
- Depends on: `pyresample`, `zarr`, `fsspec`, `h5py`
- Used by: `core.py` `_extract_lut` method

**CLI Layer:**
- Purpose: Offline LUT generator CLI
- Location: `components/aer/extract_aws_goes/generate_luts.py` (~77 lines)
- Contains: Command-line entry point for LUT generation
- Depends on: `lut.py` functions

## Data Flow

**Extraction Pipeline:**

1. **Prepare Phase (`prepare_for_extraction`):**
   - Receives search results (GeoDataFrame of assets)
   - Adds resolution based on channel_id via `AerLocalSpectralRepository`
   - Groups assets by granule_id
   - Returns `Sequence[ExtractionTask]` objects

2. **Extract Phase (`extract`):**
   - Dispatches to engine based on `extract_params["engine"]`
   - Default engine (when `engine != "satpy"`): `_extract_lut()` - zero-reprojection
   - Alternative: `_extract_satpy()` - satpy Scene resampling

3. **LUT Extraction Flow (`_extract_lut`):**
   - Download granule from S3 (`s3fs`)
   - Group grid cells by UTM zone (EPSG code)
   - Load UTM zone LUT from Zarr store (remote Hugging Face bucket or local)
   - Compute minimal source crop via `compute_source_crop_slices()` 
   - Extract each cell via `extract_cell_from_lut()` using LUT index arrays
   - Write GeoTIFF with rasterio (deflate compression, 512x512 blocks)
   - Return `GeoDataFrame[ArtifactSchema]`

4. **Satpy Extraction Flow (`_extract_satpy`):**
   - Download granule from S3
   - Build satpy `Scene` with detected reader (`abi_l1b`, `abi_l2_brf_nc`, `abi_l2_nc`)
   - Group grid cells by UTM zone
   - Resample Scene to group AreaDefinition (`nearest` resampler)
   - Extract each cell via array slicing with `compute_cell_slice()`
   - Write GeoTIFF with rasterio
   - Return `GeoDataFrame[ArtifactSchema]`

**State Management:**
- `ExtractionTask` holds: assets, grid_cells, resolution, uri, target_grid settings, task_context
- `ArtifactSchema`-validated GeoDataFrame returned from extraction

## Class Hierarchy

```
Extractor (aer-core abstract base)
    │
    └── AwsGoesExtractor (core.py:86)
            ├── extends Extractor
            ├── plugin_abstract=False
            ├── supported_collections: Sequence[str] = SUPPORTED_COLLECTIONS
            │
            ├── Properties:
            │   ├── repository → AerLocalSpectralRepository
            │   ├── abi_instrument
            │   ├── target_grid_d
            │   └── target_grid_overlap
            │
            └── Methods:
                ├── prepare_for_extraction() → Sequence[ExtractionTask]
                ├── extract() → GeoDataFrame[ArtifactSchema]
                │       ├── _extract_lut() ← primary engine
                │       └── _extract_satpy() ← alternative
                ├── extract_batches() → GeoDataFrame[ArtifactSchema]
                │
                └── Static helpers:
                        ├── _detect_combo() → "goes_east_radf"
                        ├── _detect_subdataset() → "NETCDF:...:Rad"
                        ├── _read_abi_calibration_params()
                        └── _apply_abi_calibration()
```

## Key Abstractions

**AwsGoesExtractor:**
- Purpose: Primary plugin class for GOES ABI data extraction from AWS
- Examples: `components/aer/extract_aws_goes/core.py`
- Pattern: Extends `Extractor` with `plugin_abstract=False`

**ExtractionTask:**
- Purpose: Encapsulates extraction parameters and data
- Used by: Both satpy and LUT extraction engines
- Contains: assets, grid_cells, resolution, uri, target_grid settings

**UTMZoneLUT:**
- Purpose: Metadata container for LUT Zarr stores
- Examples: `lut.py` attrs class (line 108-116)
- Pattern: Frozen attrs class with utm_epsg, resolution, area_extent, width, height, zarr_path

**Supported Collections:**
```
ABI-L1b-RadC, ABI-L1b-RadF, ABI-L1b-RadM
ABI-L2-AODC, ABI-L2-AODF
ABI-L2-BRFC, ABI-L2-BRFF, ABI-L2-BRFM
```

## Entry Points

**Plugin Registration:**
- Location: `projects/aer-extract-aws-goes/pyproject.toml` (line 31-32)
- Entry point: `aer.plugins.extract_aws_goes = "aer.extract_aws_goes.core:AwsGoesExtractor"`
- Also in root `pyproject.toml` (line 28-29) for workspace

**CLI Entry:**
- Module: `aer.extract_aws_goes.generate_luts`
- Usage: `python -m aer.extract_aws_goes.generate_luts --goes-file ... --output-dir ...`

## Error Handling

**Strategy:** Per-cell error isolation with structlog logging

**Patterns:**
- `_extract_one()` (lut.py): Try-catch per grid cell, returns None on failure
- `_extract_cell()` (satpy): Try-catch per grid cell, returns None on failure
- Group-level error handling: Log group failure, continue to next UTM zone
- Batch-level: Collect errors in list, raise RuntimeError if all fail

## Cross-Cutting Concerns

**Logging:** structlog via `get_logger()`

**Validation:** pandera schemas (`ArtifactSchema`, `AssetSchema`) via `geopandas`

**S3 Authentication:** Anonymous access (`s3fs.S3FileSystem(anon=True)`)

**Calibration:** `_read_abi_calibration_params()` / `_apply_abi_calibration()` for radiance, reflectance, brightness_temperature

---

*Architecture analysis: 2026-04-23*