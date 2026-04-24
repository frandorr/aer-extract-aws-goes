# Codebase Concerns

**Analysis Date:** 2026-04-23

## Security Considerations

### AWS S3 Anonymous Access

**Issue:** The code uses `s3fs.S3FileSystem(anon=True)` for downloading GOES data from AWS (lines 309 and 537 in `components/aer/extract_aws_goes/core.py`).

**Files:**
- `components/aer/extract_aws_goes/core.py`

**Impact:** Anonymous access to AWS S3 is intentional here (NOAA's GOES public bucket). However, the `.env` file presence indicates environment configuration exists. If other buckets or services are accessed, credentials could inadvertently be exposed through the same mechanism.

**Current mitigation:** Public GOES bucket access is standard and expected behavior for NOAA data.

**Recommendations:**
- Document that this extractor requires public AWS S3 access
- Consider adding a warning if credentials are detected in environment (could accidentally be used for other operations)

---

## Exception Handling Issues

### Broad Exception Catching in Grid Cell Extraction

**Issue:** `_extract_lut` (lines 469-476) catches `Exception` in `_extract_one` and only logs errors, returning `None`. Similarly, `_extract_satpy` (lines 706-712) catches exceptions per cell. Failures silently accumulate.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 469-476, 706-712)

**Impact:** Individual cell failures are logged but not propagated. If all cells fail, line 490 raises `ValueError`, but partial failures (some cells, not all) are silently dropped.

**Pattern observed:**
```python
except Exception as exc:
    logger.error("cell_extract_failed", ...)
    return None
```

**Recommendations:**
- Add a configurable retry mechanism for cell extraction failures
- Track partial failure counts and expose via logging/metrics
- Consider raising on critical failures rather than silently continuing

### Broad Exception in UTM Group Processing

**Issue:** The outer `try-except` blocks (lines 342-487 for LUT, 569-726 for satpy) catch all exceptions per UTM zone group, logging and continuing.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 486-487, 725-726)

**Impact:** A corrupted LUT or malformed grid cell could cause an entire UTM zone to be skipped without stopping the extraction.

**Recommendations:**
- Add a `strict` mode that raises on UTM group failures
- Include utm_epsg/utm_crs in the error context for debugging

### No Exception Handling for S3 Download

**Issue:** `fs.get()` calls (lines 310 and 543) have no exception handling. Network failures, file not found, or access denied will propagate as-is.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 310, 543)

**Impact:** A single failed download causes the entire extraction task to fail. No retry logic.

**Recommendations:**
- Add retry logic with exponential backoff for S3 downloads
- Add specific exception handling for common S3 errors (ConnectionError, FileNotFoundError, etc.)

---

## Resource Management

### File Cleanup Without Context Manager

**Issue:** `Path(local_path).unlink()` is called directly (lines 492, 732) without `try/finally` protection.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 492, 732)

**Example:**
```python
Path(local_path).unlink()
gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))
```

**Impact:** If `GeoDataFrame` creation or validation fails, the downloaded file remains on disk (cleanup leak).

**Recommendations:**
- Use a context manager or `try/finally` to ensure cleanup happens regardless of exceptions
- Consider moving cleanup before the final return statement

### Manual Garbage Collection Calls

**Issue:** `gc.collect()` is called explicitly (line 723) after deleting `group_resampled_da`. This suggests concern about memory, but explicit GC calls can be unreliable and may slow down execution.

**Files:**
- `components/aer/extract_aws_goes/core.py` (line 723)

**Impact:** Forced GC pauses can cause performance dips. The memory pressure that necessitates this call suggests large array handling that could be optimized.

**Recommendations:**
- Rely on Python's implicit garbage collection rather than explicit `gc.collect()` calls
- Consider using memory-mapped arrays or chunked processing if memory is a concern
- Profile with tools like `scalene` to identify actual memory bottlenecks

### Resource Leak in LUT Zarr Access

**Issue:** `zarr.open()` in `load_utm_zone_lut` (lines 306, 311) opens stores but there's no explicit `.close()` or context manager.

**Files:**
- `components/aer/extract_aws_goes/lut.py` (lines 306, 311)

**Impact:** File handles may not be released promptly, especially for remote Zarr stores.

**Recommendations:**
- Use context managers where possible
- For remote stores, ensure proper cleanup of fsspec mappers

---

## Performance Concerns

### Large Memory Footprint from satpy Resampling

**Issue:** `_extract_satpy` resamples the entire scene for each UTM group, creating potentially very large arrays. For CONUS (3000x5000) or Full Disk (21696x21696) data, memory usage could be substantial.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 609-617)

**Impact:** Processing Full Disk data at 500m resolution creates ~2.5 GB arrays per UTM zone group. Could cause OOM in memory-constrained environments.

**Current mitigation:** Groups grid cells by UTM zone and resamples once per zone, not per cell. Line 722-723 attempts to free memory after each group.

**Recommendations:**
- Add memory profiling tests
- Consider streaming/chunked resampling for very large areas
- Add an explicit memory limit check before attempting large resampling operations

### Thread Pool Shared State Risk

**Issue:** `ThreadPoolExecutor` is used in `_extract_lut` (line 479) and `_extract_satpy` (line 715) with `_extract_one`/`_extract_cell` functions that capture `local_path`, `href`, `collection`, and other variables from the enclosing scope.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 478-484, 714-720)

**Impact:** While numpy arrays and standard types are thread-safe in CPython due to the GIL, the shared state could cause subtle bugs if the code evolves. The `group_resampled_da` in satpy extraction is explicitly shared across threads (line 658).

**Current mitigation:** numpy array slicing and reads are GIL-protected; satpy's resampling output is read-only after resampling.

**Recommendations:**
- Document that thread-safe numpy operations are relied upon
- Consider making shared data explicit via partial functions or worker initialization

### Process Pool Executor Wrapper Pattern

**Issue:** `_extract_wrapper` (lines 48-54) imports `AwsGoesExtractor` inside the function for pickling. This imports module on each process spawn.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 48-54, 911-914)

**Impact:** Each worker process pays the import cost. For short tasks, this overhead could be significant. The pattern creates a new `AwsGoesExtractor()` instance per batch, potentially re-initializing repository properties multiple times.

**Recommendations:**
- Consider using `concurrent.futures.ProcessPoolExecutor` with an initializer function to set up worker processes once
- Cache the extractor instance in the worker process to avoid repeated initialization

### No Bounded Worker Count for Nested Thread Pools

**Issue:** When using `extract_batches` with `ProcessPoolExecutor`, each batch then creates its own `ThreadPoolExecutor` with up to 16 workers (default). With N batches and M threads each, total thread count is N×M.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 478, 714)

**Impact:** Thread count could explode with high `max_batch_workers` values. Python's default thread pool overhead applies per process.

**Recommendations:**
- Document the memory/thread implications of high `max_batch_workers` values
- Consider a global thread limit that scales down per-batch workers when batch parallelism is high

---

## External API Dependencies

### Heavy Dependency on satpy

**Issue:** `satpy` is a large dependency (line 22 in `core.py`). Used for Scene-based extraction as the default.

**Files:**
- `components/aer/extract_aws_goes/core.py`
- `pyproject.toml`

**Impact:** satpy has complex internal dependencies (PyTroll stack, dask, etc.). Version compatibility issues could arise. The alternative "gdal" engine is mentioned in comments but not implemented (line 241).

**Recommendations:**
- Document satpy version constraints clearly
- Consider implementing the "gdal" engine mentioned in the docstring

### GDAL Optional Import

**Issue:** `gdal` and `osr` are imported with try/except (lines 24-28). If unavailable, operations that need GDAL will fail later with confusing errors.

**Files:**
- `components/aer/extract_aws_goes/core.py` (lines 24-28)

**Impact:** Code will fail at call time rather than startup time, making debugging harder.

**Recommendations:**
- Validate GDAL availability at extractor initialization time
- Provide clear error messages if GDAL is required but unavailable

### Rasterio/GDAL Version Compatibility

**Issue:** `gdal==3.10.3` is pinned in `pyproject.toml`, but `rasterio>=1.5.0` is not pinned to a specific GDAL version. GDAL version mismatches can cause runtime segfaults.

**Files:**
- `pyproject.toml` (line 20)

**Impact:** Rasterio compiles against a specific GDAL version. Version mismatch can cause `ImportError` or runtime crashes.

**Recommendations:**
- Pin rasterio to a version known to work with GDAL 3.10.3, or document compatible version ranges
- Add a startup check that validates GDAL/rasterio compatibility

### Zarr Remote Store Caching

**Issue:** `load_utm_zone_lut` uses fsspec filecache (line 310) but hardcodes the cache path to `~/.cache/aer_cache` (line 308). No way to customize cache location.

**Files:**
- `components/aer/extract_aws_goes/lut.py` (lines 308-310)

**Impact:** Cache pollution or inability to cache on systems with limited home directory space.

**Recommendations:**
- Use `get_default_lut_dir()` pattern for consistency
- Add an environment variable override for cache location

---

## Maintainability Issues

### Inline Imports Throughout

**Issue:** Imports like `s3fs`, `rasterio`, `xarray` are imported inside functions (lines 279, 424, 645, 813, 535).

**Files:**
- `components/aer/extract_aws_goes/core.py`

**Impact:** Makes it harder to identify all dependencies at module level. Impacts type checking and static analysis.

**Recommendations:**
- Move all imports to module level for clarity and performance
- Use lazy imports only for optional dependencies that need clear error messages

### Magic Numbers

**Issue:** Several magic numbers without named constants: 16 (default workers), 2 (padding pixels in satpy), compression settings (lines 444-449, 680-686).

**Files:**
- `components/aer/extract_aws_goes/core.py`

**Recommendations:**
- Extract to module-level constants: `DEFAULT_MAX_WORKERS`, `RESAMPLING_PADDING_PIXELS`, `TIFF_COMPRESSION_OPTS`

### Inconsistent Error Message Format

**Issue:** Some error messages include context (lines 355-357), others don't (line 490).

**Files:**
- `components/aer/extract_aws_goes/core.py`

**Recommendations:**
- Standardize error message format with consistent fields

### No Type Hints on Some Helper Functions

**Issue:** `_extract_wrapper` (line 48), `_extract_one` (line 374), `_extract_cell` (line 621) lack return type annotations.

**Files:**
- `components/aer/extract_aws_goes/core.py`

**Recommendations:**
- Add type hints for all public/private functions

### Hardcoded Hugging Face Bucket URI

**Issue:** `get_default_bucket_uri()` returns `"hf://buckets/frandorr/aer-data"`. This is a specific external dependency with no fallback mechanism.

**Files:**
- `components/aer/extract_aws_goes/lut.py` (line 22)

**Impact:** If the Hugging Face bucket becomes unavailable or changes, LUT-based extraction fails completely.

**Recommendations:**
- Document bucket ownership and availability expectations
- Consider mirroring LUTs to multiple locations (AWS S3, local filesystem)
- Add health checks for remote bucket availability

---

## Test Coverage Gaps

**Untested areas:**
- End-to-end extraction with real S3 data (only manual test scripts in root: `test_cache*.py`, `test_hf*.py`)
- Retry logic for S3 download failures (not implemented, but assumed future need)
- Concurrent execution with `ProcessPoolExecutor` (no test coverage)
- GDAL/rasterio error paths when files are malformed
- Calibration parameter extraction for corrupted NetCDF files
- Memory usage under large Full Disk resampling scenarios

**Test file concerns:**
- Root-level test scripts (`test_cache.py`, `test_hf*.py`) appear to be manual testing scripts, not proper tests
- No integration tests for the full extraction pipeline
- No mock tests for S3 download behavior

**Files needing tests:**
- `components/aer/extract_aws_goes/core.py` - integration points with S3
- `components/aer/extract_aws_goes/lut.py` - Zarr read paths

---

## Known Fragile Areas

### LUT Source Shape Inference

**File:** `components/aer/extract_aws_goes/lut.py` (lines 39-51)

**Why fragile:** If `source_shape` is not stored in the Zarr attributes, the code infers shape from pixel count. This works for known GOES shapes but falls back to square assumption, which could silently produce incorrect results for edge cases.

### File Path Handling on Windows

**File:** `components/aer/extract_aws_goes/core.py`

**Why fragile:** Uses forward slashes in S3 paths (`href.replace("s3://", "")`). While Path objects handle this, mixing string operations with Path objects could cause issues on Windows.

### GDAL Subdataset Detection

**File:** `components/aer/extract_aws_goes/core.py` (lines 774-795)

**Why fragile:** `_detect_subdataset` uses string parsing of filename to avoid opening the NetCDF file (to prevent HDF5 warnings). This could misidentify products if filenames change format.

---

## Performance Bottlenecks

### LUT Loading Overhead

**Location:** `components/aer/extract_aws_goes/lut.py` (lines 278-332)

**Problem:** Each UTM zone group loads its LUT from remote Zarr on first access. Loading includes reading metadata attributes individually, causing multiple round-trips for remote stores.

**Cause:** Sequential attribute reads (`z.attrs["key"]`) instead of batch reading

**Improvement path:** Read all attributes in one operation, or cache LUT metadata separately

### Cell-by-Cell File I/O

**Location:** `components/aer/extract_aws_goes/core.py` (lines 451-452, 687-688)

**Problem:** Each grid cell writes its own GeoTIFF. For many small cells, this creates many small I/O operations.

**Improvement path:** Consider batch writing multiple cells per TIFF or using a single multi-band output file per granule/zone

---

## Scaling Limits

**Resource: Memory**
- Current capacity: Limited by available RAM; Full Disk at 500m could require ~2.5 GB per UTM zone
- Limit: OOM errors on systems with <8 GB RAM processing Full Disk data
- Scaling path: Chunked resampling, streaming to disk

**Resource: Parallelism**
- Current capacity: Up to 16 threads per extraction + N batch workers (N×16 total threads)
- Limit: Python GIL limits true parallelism for CPU-bound work in threads
- Scaling path: ProcessPoolExecutor already used for batch parallelism; consider moving heavy computation to processes

**Resource: Network**
- Current capacity: Downloads each granule from S3 once, caches LUTs in filesystem
- Limit: Repeated downloads if cache is cleared; no bandwidth limiting
- Scaling path: Implement S3 pagination for large file listings, add rate limiting

---

*Concerns audit: 2026-04-23*