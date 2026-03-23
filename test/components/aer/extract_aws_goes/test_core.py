from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point

from aer.extract.core import ExtractedResultSchema
from aer.extract_aws_goes.core import (
    detect_reader,
    extract_aws_goes,
    group_files_by_reader,
    map_channel_ids_to_satpy_names,
)


def test_detect_reader() -> None:
    assert detect_reader("OR_ABI-L1b-RadC-M6C01_G16_s20202151301170_e20202151303543_c20202151304033.nc") == "abi_l1b"
    assert detect_reader("OR_ABI-L2-AODC-M6_G16_s20202151301170_e20202151303543_c20202151305215.nc") == "abi_l2_nc"
    assert detect_reader("OR_ABI-L2-BRFC-M6_G16_s20202151301170_e20202151303543_c20202151305215.nc") == "abi_l2_brf_nc"
    assert detect_reader("UNKNOWN.nc") is None


def test_group_files_by_reader() -> None:
    files = [
        Path("OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc"),
        Path("OR_ABI-L1b-RadC-M6C02_G16_s20202151301170.nc"),
        Path("OR_ABI-L2-AODC-M6_G16_s20202151301170.nc"),
    ]
    grouped = group_files_by_reader(files)
    assert len(grouped.get("abi_l1b", [])) == 2
    assert len(grouped.get("abi_l2_nc", [])) == 1


def test_map_channel_ids_to_satpy_names_direct() -> None:
    """When c_id exactly matches satpy name."""
    result = map_channel_ids_to_satpy_names({"C01"}, {"C01", "C02"})
    assert result == ["C01"]


def test_map_channel_ids_to_satpy_names_padded() -> None:
    """When c_id is '1' and satpy has 'C01'."""
    result = map_channel_ids_to_satpy_names({"1", "13"}, {"C01", "C02", "C13"})
    assert sorted(result) == ["C01", "C13"]


def test_map_channel_ids_no_match() -> None:
    """When nothing matches."""
    result = map_channel_ids_to_satpy_names({"99"}, {"C01", "C02"})
    assert result == []


@patch("aer.extract_aws_goes.core.compute_writer_results")
@patch("aer.extract_aws_goes.core.download")
@patch("aer.extract_aws_goes.core.satpy.Scene")
def test_extract_aws_goes_per_row(
    mock_scene_cls: MagicMock,
    mock_download: MagicMock,
    mock_compute: MagicMock,
    tmp_path: Path,
) -> None:
    """Test with per-row channels and spatial extents."""
    local_path_1 = tmp_path / "OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc"
    local_path_2 = tmp_path / "OR_ABI-L1b-RadC-M6C02_G16_s20202151301170.nc"

    # Two different grid cells from two different rows' spatial extents
    mock_grid_cell_a = MagicMock()
    mock_grid_cell_a.area_def.return_value = "area_def_a"
    mock_grid_cell_a.area_name.return_value = "0U_0R"

    mock_grid_cell_b = MagicMock()
    mock_grid_cell_b.area_def.return_value = "area_def_b"
    mock_grid_cell_b.area_name.return_value = "1U_0R"

    mock_extent_1 = MagicMock()
    mock_extent_1.grid_cells = frozenset([mock_grid_cell_a])

    mock_extent_2 = MagicMock()
    mock_extent_2.grid_cells = frozenset([mock_grid_cell_b])

    # Two different channels: c_id "1" and c_id "2"
    mock_channel_1 = MagicMock()
    mock_channel_1.c_id = "1"
    mock_channel_2 = MagicMock()
    mock_channel_2.c_id = "2"

    download_data = {
        "product_name": ["ABI-L1b-RadC", "ABI-L1b-RadC"],
        "granule_id": ["G16_s20202151301170_C01", "G16_s20202151301170_C02"],
        "start_time": [pd.Timestamp("2020-01-01T00:00:00Z")] * 2,
        "end_time": [pd.Timestamp("2020-01-01T00:05:00Z")] * 2,
        "s3_url": ["s3://bucket/file1.nc", "s3://bucket/file2.nc"],
        "https_url": ["https://bucket/file1.nc", "https://bucket/file2.nc"],
        "size_mb": [10.5, 11.0],
        "geometry": [Point(0, 0), Point(1, 1)],
        "overlapping_spatial_extent": [mock_extent_1, mock_extent_2],
        "input_spatial_extent": [None, None],
        "cell_overlap_mode": ["contains", "contains"],
        "channels": [(mock_channel_1,), (mock_channel_2,)],
        "local_path": [str(local_path_1), str(local_path_2)],
        "download_status": ["complete", "complete"],
    }

    downloaded_gdf = GeoDataFrame(pd.DataFrame(download_data), geometry="geometry", crs="EPSG:4326")
    mock_download.return_value = downloaded_gdf

    # satpy scene mock — available names use "C01", "C02" format
    mock_scene = MagicMock()
    mock_scene.available_dataset_names.return_value = {"C01", "C02"}
    mock_scene.keys.return_value = ["C01", "C02"]

    mock_resampled = MagicMock()
    mock_resampled.keys.return_value = ["C01", "C02"]
    mock_resampled.save_datasets.return_value = ["delayed"]
    mock_scene.resample.return_value = mock_resampled
    mock_scene_cls.return_value = mock_scene

    input_gdf = GeoDataFrame(pd.DataFrame(download_data), geometry="geometry", crs="EPSG:4326")

    result = extract_aws_goes(input_gdf, dest_dir=tmp_path, resolution=500.0)

    # Should have mapped "1" → "C01" and "2" → "C02"
    load_call_args = mock_scene.load.call_args[0][0]
    assert sorted(load_call_args) == ["C01", "C02"]

    # Should have resampled to BOTH grid cells (from different rows)
    assert mock_scene.resample.call_count == 2

    # Verify deferred compute was called
    mock_compute.assert_called_once()

    assert "reprojected_path" in result.columns
    assert "resolution" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["resolution"] == 500.0

    ExtractedResultSchema.validate(result)
