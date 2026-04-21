import numpy as np
import pytest

from aer.extract_aws_goes.lut import (
    SUPPORTED_RESOLUTIONS,
    compute_cell_slice,
    compute_utm_zone_area_extent,
    detect_goes_utm_zones,
    extract_cell_from_lut,
    generate_utm_zone_lut,
    load_utm_zone_lut,
)

def test_compute_utm_zone_area_extent():
    minx, miny, maxx, maxy, width, height = compute_utm_zone_area_extent(32720, 1000)
    assert width > 0
    assert height > 0
    assert width == int(round((maxx - minx) / 1000))
    assert height == int(round((maxy - miny) / 1000))


def test_compute_cell_slice():
    lut_extent = (200000, 5000000, 800000, 10000000)
    cell_bounds = (300000, 7000000, 400000, 7100000)
    
    row_slice, col_slice = compute_cell_slice(cell_bounds, lut_extent, 1000)
    
    assert col_slice == slice(100, 200)
    assert row_slice == slice(2900, 3000)


def test_extract_cell_from_lut_simple(tmp_path):
    import zarr
    
    store_path = tmp_path / "test.zarr"
    z = zarr.open(str(store_path), mode="w")
    
    valid_output = np.ones(100, dtype=bool)
    index_arr = np.arange(100, dtype=np.int32)
    
    z.create_dataset("valid_output_index", data=valid_output, shape=(100,), dtype=bool)
    z.create_dataset("index_array", data=index_arr, shape=(100,), dtype=np.int32)
    
    source_data = np.arange(100, dtype=np.float32)
    
    # Extract cell: rows 2 to 5 (size 3), cols 3 to 7 (size 4)
    result = extract_cell_from_lut(
        source_data=source_data,
        lut_group=z,
        cell_row_slice=slice(2, 5),
        cell_col_slice=slice(3, 7),
        lut_height=10,
        lut_width=10,
    )
    
    assert result.shape == (3, 4)
    # The cell starts at row 2 col 3 -> flat index 23
    assert result[0, 0] == 23


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
    
    generate_utm_zone_lut(source_area_def, 32720, 10000, tmp_path)
    
    lut_meta, lut_group = load_utm_zone_lut(tmp_path, 32720, 10000)
    
    assert lut_meta.utm_epsg == 32720
    assert lut_meta.resolution == 10000
    assert "valid_input_index" in lut_group
    assert "valid_output_index" in lut_group
    assert "index_array" in lut_group


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
