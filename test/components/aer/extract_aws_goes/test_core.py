from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point

from aer.extract.core import ExtractedResultSchema
from aer.extract_aws_goes.core import detect_reader, extract_aws_goes, group_files_by_reader


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


@patch("aer.extract_aws_goes.core.compute_writer_results")
@patch("aer.extract_aws_goes.core.download")
@patch("aer.extract_aws_goes.core.satpy.Scene")
def test_extract_aws_goes(
    mock_scene_cls: MagicMock,
    mock_download: MagicMock,
    mock_compute: MagicMock,
    tmp_path: Path,
) -> None:
    # 1. Setup mock downloaded result dataframe conforming to DownloadedResultSchema
    local_path = tmp_path / "OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc"

    mock_grid_cell = MagicMock()
    mock_grid_cell.area_def.return_value = "dummy_area_def"
    mock_grid_cell.area_name.return_value = "0A_0B"

    mock_extent = MagicMock()
    mock_extent.grid_cells = [mock_grid_cell]

    mock_channel = MagicMock()
    mock_channel.c_id = "C01"

    download_data = {
        "product_name": ["ABI-L1b-RadC"],
        "granule_id": ["G16_s20202151301170"],
        "start_time": [pd.Timestamp("2020-01-01T00:00:00Z")],
        "end_time": [pd.Timestamp("2020-01-01T00:05:00Z")],
        "s3_url": ["s3://noaa-goes16/ABI-L1b-RadC/2020/001/00/OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc"],
        "https_url": ["https://noaa-goes16.s3.amazonaws.com/..."],
        "size_mb": [10.5],
        "geometry": [Point(0, 0)],
        "overlapping_spatial_extent": [mock_extent],
        "input_spatial_extent": [None],
        "cell_overlap_mode": ["contains"],
        "channels": [(mock_channel,)],
        "local_path": [str(local_path)],
        "download_status": ["complete"],
    }

    # Needs a GeoDataFrame since download() returns one
    downloaded_gdf = GeoDataFrame(pd.DataFrame(download_data), geometry="geometry", crs="EPSG:4326")
    mock_download.return_value = downloaded_gdf

    # 2. Setup mock satpy Scene
    mock_scene = MagicMock()
    mock_scene.available_dataset_names.return_value = {"C01"}
    mock_scene.keys.return_value = ["C01"]

    # Mock chain properly for resample
    mock_resampled = MagicMock()
    mock_resampled.keys.return_value = ["C01"]
    mock_resampled.save_datasets.return_value = ["delayed_result"]
    mock_scene.resample.return_value = mock_resampled
    mock_scene_cls.return_value = mock_scene

    # 3. Create input dummy search result
    input_gdf = GeoDataFrame(pd.DataFrame(download_data), geometry="geometry", crs="EPSG:4326")

    # 4. Invoke extraction
    result = extract_aws_goes(input_gdf, dest_dir=tmp_path, resolution=500.0)

    # 5. Assertions
    mock_scene.resample.assert_called_once_with(
        destination="dummy_area_def",
        datasets=["C01"],
        resampler="nearest",
    )

    # Verify deferred compute was called
    mock_compute.assert_called_once()

    assert "reprojected_path" in result.columns
    assert "resolution" in result.columns
    assert len(result) == 1
    assert result.iloc[0]["resolution"] == 500.0

    # Check that ExtractedResultSchema validates the dataframe
    ExtractedResultSchema.validate(result)
