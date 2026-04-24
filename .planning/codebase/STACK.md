# Technology Stack

**Analysis Date:** 2026-04-23

## Languages

**Primary:**
- Python 3.13+ - All code is Python-based for satellite data processing

**Configuration Format:**
- TOML - Used in `pyproject.toml` for project configuration

## Runtime

**Environment:**
- Python 3.13+

**Package Manager:**
- uv (PEP 582) - Modern Python package manager
- Lockfile: Not committed to repository

## Frameworks

**Core Data Processing:**
- satpy (>=0.60.0) - Primary satellite data processing framework
  - Reads GOES ABI NetCDF files via built-in readers
  - Supports multi-channel loading and calibration
  - Used in `core.py:_extract_satpy()` for scene-based extraction
- pyresample (>=1.35.0) - Geospatial resampling and area definitions
  - KD-tree nearest-neighbor interpolation for LUT generation
  - AreaDefinition for source/target grid representation
  - Used in `lut.py:generate_utm_zone_lut()`

**Raster/GIS:**
- rasterio (>=1.5.0) - GeoTIFF I/O and CRS handling
  - GeoTIFF profile creation (deflate compression, tiling)
  - CRS transformation utilities
  - Used in `core.py:_extract_wrapper()` and `lut.py:compute_cell_slice()`
- rioxarray (>=0.22.0) - xarray + rasterio integration
  - Enables clip_box operations on xarray DataArrays
  - CRS writing for GeoTIFF metadata
- geopandas (latest) - GeoDataFrame operations
  - Asset/artifact schema validation
  - Used for GeoDataFrame validation in extraction results
- pyproj (>=3.6.0) - CRS transformations and coordinate operations
  - EPSG code to WKT conversion
  - UTM zone detection and area extent calculation

**Data Formats:**
- xarray (>=2026.2.0) - Multi-dimensional data structures
  - NetCDF reading for calibration parameters
  - Coordinate-aware data manipulation
- netcdf4 (>=1.7.4) - NetCDF4 bindings
- h5netcdf[h5py] (>=1.8.1) - HDF5/NetCDF reading in satpy
- zarr (>=2.18.0) - Chunked array storage for LUTs
  - Zarr group stores for lookup tables
  - Used in `lut.py:load_utm_zone_lut()` for lazy LUT access

**Data Analysis:**
- numpy (latest) - Numerical arrays and operations
- pandas (latest) - DataFrame operations

**Other Libraries:**
- attrs (latest) - Immutable LUT metadata classes
- structlog (latest) - Structured logging
- fsspec (latest) - Filesystem abstraction for remote LUT access (Hugging Face buckets)
- h5py (latest) - Direct HDF5 reading for GOES crop extraction
- pandera (latest) - Schema validation for GeoDataFrames

## Key Dependencies

**Critical:**
- `aer-core` - Main AER framework (plugin_host)
- `satpy>=0.60.0` - GOES ABI data processing
- `rasterio>=1.5.0` - GeoTIFF output
- `zarr>=2.18.0` - LUT storage

**Infrastructure:**
- `gdal==3.10.3` - Geospatial data abstraction layer
- `pyproj>=3.6.0` - CRS and projection handling
- `pyresample>=1.35.0` - Geospatial resampling

## Build System

**Architecture:** Polylith (Bricks-based)
- Package location: `components/aer/extract_aws_goes/`
- Build backend: Hatchling with `hatch-polylith-bricks`
- Entry point: `aer.plugins.extract_aws_goes = aer.extract_aws_goes.core:AwsGoesExtractor`

**Linting & Formatting:**
- Ruff - Code linting
- Basedpyright - Type checking

**Testing:**
- pytest (>=9.0.2,<10.0.0)

## Platform Requirements

**Development:**
- Python >=3.13

**Production:**
- Same as development (Python package deployment)

---

*Stack analysis: 2026-04-23*