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


@patch("aer.extract_aws_goes.core.download")
@patch("aer.extract_aws_goes.core.satpy.Scene")
def test_extract_aws_goes(mock_scene_cls: MagicMock, mock_download: MagicMock, tmp_path: Path) -> None:
    # 1. Setup mock downloaded result dataframe conforming to DownloadedResultSchema
    local_path = tmp_path / "OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc"
    download_data = {
        "product_name": ["ABI-L1b-RadC"],
        "granule_id": ["G16_s20202151301170"],
        "start_time": [pd.Timestamp("2020-01-01T00:00:00Z")],
        "end_time": [pd.Timestamp("2020-01-01T00:05:00Z")],
        "s3_url": ["s3://noaa-goes16/ABI-L1b-RadC/2020/001/00/OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc"],
        "https_url": ["https://noaa-goes16.s3.amazonaws.com/..."],
        "size_mb": [10.5],
        "geometry": [Point(0, 0)],
        "overlapping_spatial_extent": [None],
        "input_spatial_extent": [None],
        "cell_overlap_mode": ["contains"],
        "local_path": [str(local_path)],
        "download_status": ["complete"],
    }

    # Needs a GeoDataFrame since download() returns one
    downloaded_gdf = GeoDataFrame(pd.DataFrame(download_data), geometry="geometry", crs="EPSG:4326")
    mock_download.return_value = downloaded_gdf

    # 2. Setup mock satpy Scene
    mock_scene = MagicMock()
    mock_scene.available_dataset_names.return_value = ["C01"]
    mock_scene.keys.return_value = ["C01"]
    # return `self` from harmonize to mock the pipeline properly
    mock_scene.return_value = mock_scene
    mock_scene_cls.return_value = mock_scene

    # 3. Create input dummy search result
    input_gdf = GeoDataFrame(pd.DataFrame(download_data), geometry="geometry", crs="EPSG:4326")

    # 4. Invoke extraction
    result = extract_aws_goes(input_gdf, dest_dir=tmp_path)

    # 5. Assertions
    assert "reprojected_path" in result.columns
    assert "resolution" in result.columns
    assert len(result) == 1

    # Check that ExtractedResultSchema validates the dataframe
    # pandera schema `.validate()` throws an exception if invalid.
    ExtractedResultSchema.validate(result)
