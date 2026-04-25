import numpy as np
import pytest

from aer.extract_aws_goes.utils import (
    compute_cell_slice,
    compute_utm_zone_area_extent,
    parse_goes_filename,
    _parse_domain,
    compute_goes_source_area_def,
    detect_goes_utm_zones,
)
from aer.extract_aws_goes.lut import (
    SUPPORTED_RESOLUTIONS,
    extract_cell_from_lut,
    generate_utm_zone_lut,
    load_utm_zone_lut,
)


@pytest.mark.slow
def test_compute_utm_zone_area_extent():
    minx, miny, maxx, maxy, width, height = compute_utm_zone_area_extent(32720, 1000)
    assert width > 0
    assert height > 0
    assert width == int(round((maxx - minx) / 1000))
    assert height == int(round((maxy - miny) / 1000))


def test_compute_cell_slice():
    lut_extent = (200000, 5000000, 800000, 10000000)
    cell_bounds = (300000, 7000000, 400000, 7100000)

    row_slice, col_slice, _ = compute_cell_slice(cell_bounds, lut_extent, 1000)

    assert col_slice == slice(100, 200)
    assert row_slice == slice(2900, 3000)


def _make_mock_grid_cell(area_def, bounds):
    """Create a mock grid cell with a mockable area_def that supports to_yaml()."""
    from unittest.mock import MagicMock

    area_mock = MagicMock()
    area_mock.to_yaml.return_value = "<dummy-yaml>"
    # Delegate actual properties to the real AreaDefinition
    area_mock.height = area_def.height
    area_mock.width = area_def.width
    area_mock.area_extent = area_def.area_extent
    area_mock.get_proj_vectors = area_def.get_proj_vectors

    grid_cell = MagicMock()
    grid_cell.utm_footprint.bounds = bounds
    grid_cell.area_def.return_value = area_mock
    return grid_cell


def test_extract_cell_from_lut_simple(tmp_path):

    store_path = tmp_path / "test.npz"
    row_offsets = np.arange(100) // 10
    col_offsets = np.arange(100) % 10

    # Save dummy LUT
    np.savez(
        str(store_path),
        row_map=row_offsets.reshape(10, 10),
        col_map=col_offsets.reshape(10, 10),
        utm_epsg=32720,
        resolution=1000,
        area_extent=(500000, 5000000, 510000, 5010000),
        width=10,
        height=10,
        crop_slices=(0, 10, 0, 10),
        source_shape=(10, 10),
        satellite="goes_east",
        domain="f",
    )

    source_crop = np.arange(100, dtype=np.float32).reshape(10, 10)

    # Extract cell: rows 2 to 5 (size 3), cols 3 to 7 (size 4)
    from aer.extract_aws_goes.lut import UTMZoneLUT
    from pyresample.geometry import AreaDefinition
    from unittest.mock import patch
    import xarray as xr

    lut = UTMZoneLUT(
        utm_epsg=32720,
        resolution=1000,
        area_extent=(500000, 5000000, 510000, 5010000),
        width=10,
        height=10,
        row_map=row_offsets.reshape(10, 10),
        col_map=col_offsets.reshape(10, 10),
        crop_slices=(0, 10, 0, 10),
        source_shape=(10, 10),
        lut_path=str(store_path),
        satellite="goes_east",
        domain="f",
    )

    # Extract cell: 4km wide, 3km high
    # x: 500000 + 3*1000 to 500000 + 7*1000 -> (503000, 507000)
    # y: 5010000 - 5*1000 to 5010000 - 2*1000 -> (5005000, 5008000)
    cell_area_def = AreaDefinition(
        "test", "test", "test", "EPSG:32720", 4, 3, (503000, 5005000, 507000, 5008000)
    )
    bounds = (503000, 5005000, 507000, 5008000)
    grid_cell = _make_mock_grid_cell(cell_area_def, bounds)

    with patch("aer.extract_aws_goes.lut.load_area_from_string", return_value=cell_area_def):
        result = extract_cell_from_lut(
            source_crop=xr.DataArray(source_crop),
            grid_cell=grid_cell,
            lut=lut,
        )

    assert result.shape == (3, 4)
    # The cell starts at row 2 col 3 -> flat index 23
    assert result[0, 0] == 23


def test_extract_cell_from_lut_partial_overlap(tmp_path):
    """Edge cell extends beyond the LUT boundary — should pad with NaN."""
    from aer.extract_aws_goes.lut import UTMZoneLUT
    from pyresample.geometry import AreaDefinition
    from unittest.mock import patch
    import xarray as xr

    row_offsets = np.arange(100) // 10
    col_offsets = np.arange(100) % 10

    lut = UTMZoneLUT(
        utm_epsg=32720,
        resolution=1000,
        area_extent=(500000, 5000000, 510000, 5010000),
        width=10,
        height=10,
        row_map=row_offsets.reshape(10, 10),
        col_map=col_offsets.reshape(10, 10),
        crop_slices=(0, 10, 0, 10),
        source_shape=(10, 10),
        lut_path=str(tmp_path / "test.npz"),
        satellite="goes_east",
        domain="f",
    )

    source_crop = np.arange(100, dtype=np.float32).reshape(10, 10)

    # Cell that extends 2 pixels beyond the right edge of the LUT
    # LUT covers cols 0-9 (x: 500000-510000).  Cell wants cols 8-12 (x: 508000-512000)
    cell_area_def = AreaDefinition(
        "test", "test", "test", "EPSG:32720", 4, 3, (508000, 5005000, 512000, 5008000)
    )
    bounds = (508000, 5005000, 512000, 5008000)
    grid_cell = _make_mock_grid_cell(cell_area_def, bounds)

    with patch("aer.extract_aws_goes.lut.load_area_from_string", return_value=cell_area_def):
        result = extract_cell_from_lut(
            source_crop=xr.DataArray(source_crop),
            grid_cell=grid_cell,
            lut=lut,
        )

    # Shape should match the area_def, not the clamped slice
    assert result.shape == (3, 4)
    # Right 2 columns should be NaN (outside LUT)
    assert np.all(np.isnan(result.values[:, 2:]))
    # Left 2 columns should have valid data
    assert not np.any(np.isnan(result.values[:, :2]))


def test_extract_cell_from_lut_no_overlap(tmp_path):
    """Cell entirely outside the LUT — should return all NaN."""
    from aer.extract_aws_goes.lut import UTMZoneLUT
    from pyresample.geometry import AreaDefinition
    from unittest.mock import patch
    import xarray as xr

    row_offsets = np.arange(100) // 10
    col_offsets = np.arange(100) % 10

    lut = UTMZoneLUT(
        utm_epsg=32720,
        resolution=1000,
        area_extent=(500000, 5000000, 510000, 5010000),
        width=10,
        height=10,
        row_map=row_offsets.reshape(10, 10),
        col_map=col_offsets.reshape(10, 10),
        crop_slices=(0, 10, 0, 10),
        source_shape=(10, 10),
        lut_path=str(tmp_path / "test.npz"),
        satellite="goes_east",
        domain="f",
    )

    source_crop = np.arange(100, dtype=np.float32).reshape(10, 10)

    # Cell entirely outside the LUT (beyond right edge)
    cell_area_def = AreaDefinition(
        "test", "test", "test", "EPSG:32720", 4, 3, (512000, 5005000, 516000, 5008000)
    )
    bounds = (512000, 5005000, 516000, 5008000)
    grid_cell = _make_mock_grid_cell(cell_area_def, bounds)

    with patch("aer.extract_aws_goes.lut.load_area_from_string", return_value=cell_area_def):
        result = extract_cell_from_lut(
            source_crop=xr.DataArray(source_crop),
            grid_cell=grid_cell,
            lut=lut,
        )

    assert result.shape == (3, 4)
    assert np.all(np.isnan(result.values))


@pytest.mark.slow
def test_generate_and_load_utm_zone_lut(tmp_path):
    try:
        from pyresample.geometry import AreaDefinition
    except ImportError:
        pytest.skip("pyresample not installed")

    # Small synthetic geostationary area definition
    source_area_def = AreaDefinition(
        area_id="test_src",
        description="test",
        proj_id="test",
        projection="EPSG:4326",
        width=100,
        height=100,
        area_extent=(-66, -80, -60, -70),
    )

    # Need a mock file or a real one. Let's create a dummy one.
    goes_path = tmp_path / "OR_ABI-L1b-RadF-M6C01_G16_s20233652345558_e20233652354558_c20233652355432.nc"
    goes_path.touch()

    # We need to mock compute_goes_source_area_def because it will try to open the file with satpy
    from unittest.mock import patch
    with patch("aer.extract_aws_goes.lut.compute_goes_source_area_def", return_value=source_area_def):
        generate_utm_zone_lut(goes_path, 32720, 10000, output_uri=tmp_path)

    loaded_lut = load_utm_zone_lut(tmp_path, 32720, 10000, combo="goes_east_f")

    assert loaded_lut.utm_epsg == 32720
    assert loaded_lut.resolution == 10000
    assert loaded_lut.row_map is not None
    assert loaded_lut.col_map is not None
    assert loaded_lut.satellite == "goes_east"
    assert loaded_lut.domain == "f"


@pytest.mark.slow
def test_detect_goes_utm_zones():
    try:
        from pyresample.geometry import AreaDefinition
    except ImportError:
        pytest.skip("pyresample not installed")

    # Synthetic area over part of South America
    source_area_def = AreaDefinition(
        area_id="test_src",
        description="test",
        proj_id="test",
        projection="EPSG:4326",
        width=100,
        height=100,
        area_extent=(-66, -30, -60, -20),
    )

    zones = detect_goes_utm_zones(source_area_def)
    assert len(zones) > 0
    # Longitude -66 to -60 lies in UTM zones 20 and 21 South
    assert 32720 in zones or 32721 in zones


def test_supported_resolutions():
    assert SUPPORTED_RESOLUTIONS == (500, 1000, 2000)


def test_parse_goes_filename():
    filename = "OR_ABI-L1b-RadF-M6C01_G16_s20233652345558_e20233652354558_c20233652355432.nc"
    info = parse_goes_filename(filename)
    assert info.get("sat_id") == 16
    assert info.get("channel_id") == "1"
    assert info.get("product") == "ABI-L1b-RadF"
    assert "start_time" in info
    assert "end_time" in info

    # Invalid filename
    assert parse_goes_filename("invalid_filename.nc") == {}


def test_parse_domain():
    import pytest

    assert _parse_domain("ABI-L1b-RadF") == "F"
    assert _parse_domain("ABI-L2-CMIPC") == "C"
    assert _parse_domain("ABI-L1b-RadM") == "M"
    assert _parse_domain("GLM-L2-LCFA") == "F"

    with pytest.raises(ValueError):
        _parse_domain("")
    with pytest.raises(ValueError):
        _parse_domain("UNKNOWN_PRODUCT")


@pytest.mark.slow
def test_compute_goes_source_area_def():
    import pytest

    # 1. Explicit arguments
    area_def = compute_goes_source_area_def(sat="east", domain="f", res="500m")
    assert area_def.area_id == "goes_east_abi_f_500m"

    # 2. From filename
    filename = "OR_ABI-L1b-RadC-M6C02_G16_s20233652345558_e20233652354558_c20233652355432.nc"
    area_def2 = compute_goes_source_area_def(goes_file=filename)
    assert area_def2.area_id == "goes_east_abi_c_500m"

    # 3. Override resolution
    area_def3 = compute_goes_source_area_def(goes_file=filename, res="1km")
    assert area_def3.area_id == "goes_east_abi_c_1km"

    # 4. Error on missing info
    with pytest.raises(ValueError):
        compute_goes_source_area_def()

    with pytest.raises(ValueError):
        compute_goes_source_area_def(goes_file="invalid.nc")
