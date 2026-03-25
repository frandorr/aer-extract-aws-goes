from pathlib import Path
from unittest.mock import MagicMock, patch

from aer.extract import ExtractionTask
from aer.extract.core import ExtractionStatus
from aer.extract_aws_goes.core import detect_reader, extract_aws_goes, map_channel_ids_to_satpy_names


# --- detect_reader tests ---


def test_detect_reader_l1b() -> None:
    assert detect_reader("OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc") == "abi_l1b"
    assert detect_reader("OR_ABI-L1b-RadF-M6C13_G19_s20251520000203.nc") == "abi_l1b"


def test_detect_reader_l2_aod() -> None:
    assert detect_reader("OR_ABI-L2-AODC-M6_G16_s20202151301170.nc") == "abi_l2_nc"


def test_detect_reader_l2_brf() -> None:
    assert detect_reader("OR_ABI-L2-BRFC-M6_G16_s20202151301170.nc") == "abi_l2_brf_nc"


def test_detect_reader_unknown() -> None:
    assert detect_reader("UNKNOWN.nc") is None


# --- map_channel_ids_to_satpy_names tests ---


def test_map_direct_match() -> None:
    assert map_channel_ids_to_satpy_names({"C01"}, {"C01", "C02"}) == ["C01"]


def test_map_numeric_to_padded() -> None:
    result = map_channel_ids_to_satpy_names({"1", "13"}, {"C01", "C02", "C13"})
    assert sorted(result) == ["C01", "C13"]


def test_map_no_match() -> None:
    assert map_channel_ids_to_satpy_names({"99"}, {"C01", "C02"}) == []


def test_map_empty() -> None:
    assert map_channel_ids_to_satpy_names(set(), {"C01"}) == []


# --- extract_aws_goes tests ---


def _make_task(granule_id: str, c_id: str, resolution: int = 2000) -> ExtractionTask:
    """Build a minimal ExtractionTask for testing."""
    mock_channel = MagicMock()
    mock_channel.c_id = c_id
    mock_channel.resolution = resolution

    mock_grid_cell = MagicMock()
    mock_grid_cell.area_def.return_value = MagicMock()
    mock_grid_cell.area_name.return_value = "22U_107L_100km_2000m"

    mock_grid = MagicMock()
    mock_grid.grid_cell = mock_grid_cell

    mock_sr = MagicMock()
    mock_sr.granule_id = granule_id
    mock_sr.channel = mock_channel
    mock_sr.grid = mock_grid

    return ExtractionTask(
        search_result=mock_sr,
        output_dir=Path("/tmp/test_extract"),
    )


@patch("aer.extract_aws_goes.core.compute_writer_results")
@patch("aer.extract_aws_goes.core.download")
@patch("aer.extract_aws_goes.core.satpy.Scene")
@patch("aer.search.core.SearchResult.to_gdf")
def test_extract_success(mock_to_gdf, mock_scene_cls, mock_download, mock_compute, tmp_path):
    mock_scene = MagicMock()
    mock_scene.available_dataset_names.return_value = ["C01"]
    mock_resampled = MagicMock()
    mock_resampled.save_datasets.return_value = ["delayed"]
    mock_scene.resample.return_value = mock_resampled
    mock_scene_cls.return_value = mock_scene

    mock_to_gdf.return_value = MagicMock()

    downloaded_gdf = MagicMock()
    downloaded_gdf.iloc = [{"local_path": str(tmp_path / "test.nc")}]
    mock_download.return_value = downloaded_gdf

    task = _make_task(
        "OR_ABI-L1b-RadF-M6C01_G19_s20251520000203_e20251520009510.nc",
        "1",
    )
    task = ExtractionTask(
        search_result=task.search_result,
        output_dir=tmp_path,
    )
    result = extract_aws_goes(task)

    assert result.status == ExtractionStatus.SUCCESS
    mock_scene.load.assert_called_once_with(["C01"])
    mock_scene.resample.assert_called_once()
    mock_compute.assert_called_once()


def test_extract_unknown_reader():
    task = _make_task("UNKNOWN_FILE.nc", "1")
    result = extract_aws_goes(task)
    assert result.status == ExtractionStatus.FAILED


def test_extract_no_channel():
    mock_sr = MagicMock()
    mock_sr.granule_id = "OR_ABI-L1b-RadF-M6C01_G19_s20251520000203.nc"
    mock_sr.channel = None
    mock_sr.grid = MagicMock()

    task = ExtractionTask(search_result=mock_sr, output_dir=Path("/tmp"))
    result = extract_aws_goes(task)
    assert result.status == ExtractionStatus.FAILED


def test_extract_no_grid():
    mock_sr = MagicMock()
    mock_sr.granule_id = "OR_ABI-L1b-RadF-M6C01_G19_s20251520000203.nc"
    mock_sr.channel = MagicMock()
    mock_sr.grid = None

    task = ExtractionTask(search_result=mock_sr, output_dir=Path("/tmp"))
    result = extract_aws_goes(task)
    assert result.status == ExtractionStatus.FAILED
