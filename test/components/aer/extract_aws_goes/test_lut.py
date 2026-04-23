import numpy as np
import pytest

from aer.extract_aws_goes.lut import (
    SUPPORTED_RESOLUTIONS,
    compute_cell_slice,
    compute_utm_zone_area_extent,
    _parse_goes_filename,
    _parse_domain,
    compute_goes_source_area_def,
    detect_goes_utm_zones,
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


def test_extract_cell_from_lut_simple(tmp_path):
    import zarr
    
    store_path = tmp_path / "test.zarr"
    z = zarr.open(str(store_path), mode="w")
    
    valid_output = np.ones(100, dtype=bool)
    index_arr = np.arange(100, dtype=np.int32)
    
    z.create_array("valid_output_index", data=valid_output)
    z.create_array("index_array", data=index_arr)
    
    source_crop = np.arange(100, dtype=np.float32).reshape(10, 10)
    row_offsets = np.arange(100) // 10
    col_offsets = np.arange(100) % 10
    
    # Extract cell: rows 2 to 5 (size 3), cols 3 to 7 (size 4)
    result = extract_cell_from_lut(
        source_crop=source_crop,
        row_offsets=row_offsets,
        col_offsets=col_offsets,
        lut_group=z,
        cell_row_slice=slice(2, 5),
        cell_col_slice=slice(3, 7),
        lut_width=10,
    )
    
    assert result.shape == (3, 4)
    # The cell starts at row 2 col 3 -> flat index 23
    assert result[0, 0] == 23


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
    
    generate_utm_zone_lut(source_area_def, 32720, 10000, tmp_path)
    
    lut_meta, lut_group = load_utm_zone_lut(tmp_path, 32720, 10000)
    
    assert lut_meta.utm_epsg == 32720
    assert lut_meta.resolution == 10000
    assert "valid_input_index" in lut_group
    assert "valid_output_index" in lut_group
    assert "index_array" in lut_group


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
    info = _parse_goes_filename(filename)
    assert info.get("sat_id") == 16
    assert info.get("channel_id") == "1"
    assert info.get("product") == "ABI-L1b-RadF"
    assert "start_time" in info
    assert "end_time" in info
    
    # Invalid filename
    assert _parse_goes_filename("invalid_filename.nc") == {}


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
