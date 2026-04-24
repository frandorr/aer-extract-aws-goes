# External Integrations

**Analysis Date:** 2026-04-23

## Internal Integrations

**aer-core:**
- The base plugin system for the AER framework
- AwsGoesExtractor extends `aer.interfaces.Extractor`
- Uses `aer.grid.GridCell` for grid cell handling
- Uses `aer.repository.AerLocalSpectralRepository` for instrument metadata
- Uses `aer.schemas.ArtifactSchema`, `AssetSchema` for validation
- Uses `aer.eoids.build_eoids_path()` for output path generation

**Plugin Registration:**
- Registered via `project.entry-points."aer.plugins"` in both `pyproject.toml` files
- Entry: `extract_aws_goes = aer.extract_aws_goes.core:AwsGoesExtractor`

## Third-Party Integrations

**AWS S3 (GOES Data):**
- Service: AWS S3 (NOAA GOES Data Archive)
- Purpose: Download GOES ABI satellite granules (L1b, L2 products)
- Connection: s3fs.S3FileSystem(anon=True) - Anonymous access, no credentials
- URI Pattern: `s3://noaa-goes18/...` (or similar bucket paths)
- Used in: `core.py:_extract_wrapper()` lines 309-310, 537-543, `core.py:extract()` lines 304-311

**Hugging Face Buckets (LUT Storage):**
- Service: Hugging Face Hub for LUT storage
- Purpose: Store pre-computed UTM zone lookup tables (Zarr stores)
- Connection: fsspec with filecache protocol
- Default URI: `hf://buckets/frandorr/aer-data`
- Configuration: Via `lut_dir` or `bucket_uri` extract_params
- Used in: `lut.py:load_utm_zone_lut()` lines 296-311

## External APIs

**GOES Satellite Data:**
- NOAA GOES Data Archive (AWS S3)
- Products: ABI-L1b-Rad[C/F/M], ABI-L2-AOD[C/F], ABI-L2-BRF[C/F/M]
- satpy readers: `abi_l1b`, `abi_l2_nc`, `abi_l2_brf_nc`

## Data Storage

**File Storage:**
- Local filesystem - Temporary download storage
- GeoTIFF output format with compression (deflate, predictor=2, zlevel=1)

**LUT Storage:**
- Zarr (>=2.18.0) - Chunked array storage
- Layout: `{bucket_uri}/{combo}/{utm_epsg}/{resolution}m.zarr`
- Example: `hf://buckets/frandorr/aer-data/goes_east_radf/32618/1000m.zarr`
- Local fallback: `~/.cache/aer_cache/` for remote LUT caching

## File Formats

**Input Formats:**
- NetCDF (GOES ABI L1b/L2) - Primary input format
  - ABI-L1b-Rad[C/F/M]: Level 1B radiance data
  - ABI-L2-AOD[C/F]: Aerosol optical depth
  - ABI-L2-BRF[C/F/M]: Basic reflectance factor
- Detection via satpy readers: `abi_l1b`, `abi_l2_nc`, `abi_l2_brf_nc`

**Output Formats:**
- GeoTIFF - Raster output format
  - Profile: GTiff, float32, deflate compression
  - Tiling: 512x512 blocks
  - CRS: UTM zone EPSG codes (e.g., EPSG:32618)
  - Metadata in `core.py`: lines 436-452

**LUT Format:**
- Zarr - Binary lookup table storage
  - Arrays: `valid_input_index`, `valid_output_index`, `index_array`
  - Metadata: area_extent, resolution, source_shape, CRS
  - Generated via pyresample KD-tree in `lut.py:generate_utm_zone_lut()`

## Authentication & Identity

**AWS S3:**
- Method: Anonymous access (no authentication required)
- Env vars: None required
- Note: Public NOAA data is accessible without credentials

**Hugging Face:**
- Method: public bucket access (no auth required for public datasets)
- Env vars: None required

## Monitoring & Observability

**Logging:**
- Framework: structlog
- Logger: Acquired via `structlog.get_logger()` in `core.py`
- Log events: file_downloaded, source_crop_loaded, resampling_group, cell_extract_failed

## Configuration Files

**Project Configuration:**
- `/projects/aer-extract-aws-goes/pyproject.toml` - Workspace config
- `/pyproject.toml` - Root config with uv sources

**Plugin/Component:**
- `/components/aer/extract_aws_goes/areas.yaml` - pyresample area definitions for GOES ABI

---

*Integration audit: 2026-04-23*