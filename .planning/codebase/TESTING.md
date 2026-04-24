# Testing Patterns

**Analysis Date:** 2026-04-23

## Test Framework

**Runner:**
- pytest (version 9.0.2+, configured in `pyproject.toml` line 74)

**Config** (`pyproject.toml` lines 79-82):
```toml
[tool.pytest.ini_options]
testpaths = ["test"]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
]
```

## Test File Organization

**Location:**
- Tests mirror component paths at `test/components/aer/extract_aws_goes/`
- Test structure mirrors the component structure

**Naming:**
- `test_*.py` for test modules

**Key Test Files:**
- `test/components/aer/extract_aws_goes/test_core.py` - Core extractor tests
- `test/components/aer/extract_aws_goes/test_lut.py` - LUT extraction tests
- `test/components/aer/extract_aws_goes/test_utm_grouping.py` - UTM grouping tests

## Test Structure Patterns

### Basic Function Tests
```python
def test_detect_reader_l1b() -> None:
    assert detect_reader("OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc") == "abi_l1b"
    assert detect_reader("OR_ABI-L1b-RadF-M6C13_G19_s20251520000203.nc") == "abi_l1b"
```

### Class Tests
```python
def test_extractor_is_subclass() -> None:
    """AwsGoesExtractor must be a valid Extractor subclass."""
    assert issubclass(AwsGoesExtractor, Extractor)

def test_extractor_target_grid_d() -> None:
    """target_grid_d must return 100000."""
    extractor = AwsGoesExtractor()
    assert extractor.target_grid_d == 100_000
```

### Helper Factories
```python
def _make_asset_gdf(n: int = 1, granule_id: str = "test_granule.nc") -> gpd.GeoDataFrame:
    """Build a minimal AssetSchema-compliant GeoDataFrame."""
    rows = []
    for i in range(n):
        rows.append(
            {
                "id": f"asset_{i}",
                "collection": "ABI-L1b-RadF",
                "geometry": box(-80, 20, -70, 30),
                "start_time": datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
                "end_time": datetime(2025, 6, 1, 12, 10, 0, tzinfo=timezone.utc),
                "href": f"s3://noaa-goes19/ABI-L1b-RadF/2025/152/12/{granule_id}",
                "granule_id": granule_id,
                "channel_id": "1",
            }
        )
    return gpd.GeoDataFrame(rows, geometry="geometry")
```

### Integration-Style Tests with Mocks
```python
@pytest.mark.slow
@patch("s3fs.S3FileSystem")
@patch("aer.extract_aws_goes.core.Scene")
def test_extract_groups_by_utm(self, mock_scene_cls, mock_s3):
    # Setup mocks
    mock_scene = mock_scene_cls.return_value
    mock_scene.available_dataset_names.return_value = ["C01"]
    
    # Create test data and execute
    ...
    
    # Verify with assertions
    self.assertEqual(mock_scene.resample.call_count, 2)
```

## Test Fixtures

**tmp_path:** Uses pytest's built-in `tmp_path` fixture for temporary directories.

**Example from `test_lut.py`:**
```python
def test_extract_cell_from_lut_simple(tmp_path):
    import zarr
    
    store_path = tmp_path / "test.zarr"
    z = zarr.open(str(store_path), mode="w")
    
    # Create test data and verify
    ...
```

## Mocking

**Framework:** unittest.mock

**Patterns:**
- `unittest.mock.patch` for patching
- `MagicMock` for creating mock objects
- `PropertyMock` for property patching

**Example:**
```python
from unittest.mock import MagicMock, patch, PropertyMock

with patch("s3fs.S3FileSystem"):
    with patch("aer.extract_aws_goes.core.Scene"):
        # Test code here
```

## Test Markers

**slow:** Mark tests that are slow (network-dependent, I/O-heavy)

**Configuration:**
```toml
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
]
```

**Usage:**
```python
@pytest.mark.slow
def test_compute_utm_zone_area_extent():
    # Slow test here
```

## Assertions

**Framework:** pytest assertions (plus unittest.TestCase for some tests)

**Patterns:**
```python
assert detect_reader("file.nc") == "expected_reader"
assert len(tasks) == 2
with pytest.raises(ValueError, match="resolution and uri"):
    extractor.prepare_for_extraction(gdf)
```

## Run Commands

**Run all tests:**
```bash
pytest test/
```

**Run without slow tests:**
```bash
pytest -m "not slow"
```

**Run with coverage:**
```bash
pytest --cov=components/aer/extract_aws_goes test/
```

---

*Testing analysis: 2026-04-23*