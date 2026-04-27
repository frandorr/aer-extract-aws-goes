from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd
import pytest
from aer.extract_aws_goes.core import (
    AwsGoesExtractor,
)
from aer.extract_aws_goes.utils import (
    detect_reader,
    map_channel_ids_to_satpy_names,
)
from aer.interfaces import Extractor
from shapely.geometry import box


# --- detect_reader tests (unchanged) ---


def test_detect_reader_l1b() -> None:
    assert detect_reader("OR_ABI-L1b-RadC-M6C01_G16_s20202151301170.nc") == "abi_l1b"
    assert detect_reader("OR_ABI-L1b-RadF-M6C13_G19_s20251520000203.nc") == "abi_l1b"


def test_detect_reader_l2_aod() -> None:
    assert detect_reader("OR_ABI-L2-AODC-M6_G16_s20202151301170.nc") == "abi_l2_nc"


def test_detect_reader_l2_brf() -> None:
    assert detect_reader("OR_ABI-L2-BRFC-M6_G16_s20202151301170.nc") == "abi_l2_brf_nc"


def test_detect_reader_unknown() -> None:
    assert detect_reader("UNKNOWN.nc") is None


# --- map_channel_ids_to_satpy_names tests (unchanged) ---


def test_map_direct_match() -> None:
    assert map_channel_ids_to_satpy_names({"C01"}, {"C01", "C02"}) == ["C01"]


def test_map_numeric_to_padded() -> None:
    result = map_channel_ids_to_satpy_names({"1", "13"}, {"C01", "C02", "C13"})
    assert sorted(result) == ["C01", "C13"]


def test_map_no_match() -> None:
    assert map_channel_ids_to_satpy_names({"99"}, {"C01", "C02"}) == []


def test_map_empty() -> None:
    assert map_channel_ids_to_satpy_names(set(), {"C01"}) == []


# --- AwsGoesExtractor class tests ---


def test_extractor_is_subclass() -> None:
    """AwsGoesExtractor must be a valid Extractor subclass."""
    assert issubclass(AwsGoesExtractor, Extractor)


def test_extractor_supported_collections() -> None:
    """supported_collections must be a non-empty sequence."""
    extractor = AwsGoesExtractor()
    assert isinstance(extractor.supported_collections, (list, tuple, set))
    assert len(extractor.supported_collections) > 0


def test_extractor_target_grid_d() -> None:
    """target_grid_d must return 100000."""
    extractor = AwsGoesExtractor()
    assert extractor.target_grid_d == 100_000


def test_extractor_target_grid_overlap() -> None:
    """target_grid_overlap must return False."""
    extractor = AwsGoesExtractor()
    assert extractor.target_grid_overlap is False


# --- prepare_for_extraction tests ---


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


def test_prepare_groups_by_granule() -> None:
    """prepare_for_extraction should group assets by granule_id."""
    extractor = AwsGoesExtractor()
    gdf = pd.concat(
        [
            _make_asset_gdf(2, "granule_A.nc"),
            _make_asset_gdf(3, "granule_B.nc"),
        ],
        ignore_index=True,
    )
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry")

    tasks = extractor.prepare_for_extraction(gdf, resolution=2000.0, uri="/tmp/test_output")

    assert len(tasks) > 0
    # Check task_context has granule_id
    granule_ids = {t.task_context["granule_id"] for t in tasks}
    assert granule_ids == {"granule_A.nc", "granule_B.nc"}

    # Every task must carry chunk tracking metadata and pre-computed cells
    for t in tasks:
        assert "chunk_id" in t.task_context
        assert "total_chunks" in t.task_context
        assert "grid_cells" in t.task_context
        assert len(t.task_context["grid_cells"]) > 0


def test_prepare_requires_resolution_and_uri() -> None:
    """prepare_for_extraction should raise ValueError if resolution or uri is None."""
    extractor = AwsGoesExtractor()
    gdf = _make_asset_gdf()

    with pytest.raises(ValueError, match="resolution and uri"):
        extractor.prepare_for_extraction(gdf)


def test_prepare_respects_cells_per_chunk() -> None:
    """Smaller cells_per_chunk should produce at least as many tasks."""
    extractor = AwsGoesExtractor()
    gdf = _make_asset_gdf(1)

    tasks_large = extractor.prepare_for_extraction(
        gdf, resolution=2000.0, uri="/tmp/test", prepare_params={"cells_per_chunk": 100},
    )
    tasks_small = extractor.prepare_for_extraction(
        gdf, resolution=2000.0, uri="/tmp/test", prepare_params={"cells_per_chunk": 1},
    )
    # Fewer cells per chunk → more tasks
    assert len(tasks_small) >= len(tasks_large)
def test_prepare_no_duplicate_cells() -> None:
    """Each grid cell ID must appear in exactly one task (regression for de-dup bug)."""
    extractor = AwsGoesExtractor()
    gdf = _make_asset_gdf(1)

    tasks = extractor.prepare_for_extraction(
        gdf,
        resolution=2000.0,
        uri="/tmp/test",
        prepare_params={"cells_per_chunk": 3},
    )

    seen: set[str] = set()
    for task in tasks:
        cells = task.task_context["grid_cells"]
        for cell in cells:
            cell_id = cell.id()
            assert cell_id not in seen, f"Duplicate cell ID: {cell_id}"
            seen.add(cell_id)



def test_split_aoi_single_chunk() -> None:
    """An AOI smaller than chunk_size should produce exactly one chunk."""
    from aer.extract_aws_goes.utils import split_aoi_into_chunks

    aoi = box(0, 0, 1, 1)
    chunks = split_aoi_into_chunks(aoi, chunk_size=5.0)
    assert len(chunks) == 1
    assert chunks[0].intersects(aoi)


def test_split_aoi_multiple_chunks() -> None:
    """A 10×10 AOI with chunk_size=5 should produce 4 chunks."""
    from aer.extract_aws_goes.utils import split_aoi_into_chunks

    aoi = box(0, 0, 10, 10)
    chunks = split_aoi_into_chunks(aoi, chunk_size=5.0)
    assert len(chunks) == 4
    for c in chunks:
        assert c.intersects(aoi)


def test_split_aoi_irregular_shape() -> None:
    """Chunks outside an L-shaped AOI should be excluded."""
    from aer.extract_aws_goes.utils import split_aoi_into_chunks
    from shapely.geometry import Polygon

    # L-shape within a 10×10 bounding box; top-right quadrant is empty
    # Slightly inset from the (5,5) corner so the top-right chunk doesn't
    # touch via a shared vertex.
    l_shape = Polygon([(0, 0), (10, 0), (10, 4.9), (4.9, 4.9), (4.9, 10), (0, 10)])
    chunks = split_aoi_into_chunks(l_shape, chunk_size=5.0)
    # The top-right quadrant (5-10, 5-10) does NOT intersect the L-shape
    assert len(chunks) == 3
    for c in chunks:
        assert c.intersects(l_shape)


# --- Engine dispatch tests ---


def test_extract_default_engine_calls_odc_cell() -> None:
    """extract() with no engine param should call _extract_odc_cell."""
    from unittest.mock import patch

    extractor = AwsGoesExtractor()
    with (
        patch.object(extractor, "_extract_odc_cell", return_value="odc_result") as mock_odc,
        patch.object(extractor, "_extract_pyresample", return_value="pyresample_result") as mock_pyresample,
    ):
        result = extractor.extract("fake_task")  # type: ignore[arg-type]

    mock_odc.assert_called_once_with("fake_task", {})
    mock_pyresample.assert_not_called()
    assert result == "odc_result"


def test_extract_odc_zone_engine_explicit() -> None:
    """extract() with engine='odc_zone' should call _extract_odc_zone."""
    from unittest.mock import patch

    extractor = AwsGoesExtractor()
    with patch.object(extractor, "_extract_odc_zone", return_value="odc_result") as mock_odc:
        result = extractor.extract("fake_task", {"engine": "odc_zone"})  # type: ignore[arg-type]

    mock_odc.assert_called_once_with("fake_task", {"engine": "odc_zone"})
    assert result == "odc_result"


def test_extract_pyresample_engine() -> None:
    """extract() with engine='pyresample' should call _extract_pyresample."""
    from unittest.mock import patch

    extractor = AwsGoesExtractor()
    with (
        patch.object(extractor, "_extract_odc_zone", return_value="odc_result") as mock_odc,
        patch.object(extractor, "_extract_pyresample", return_value="pyresample_result") as mock_pyresample,
    ):
        result = extractor.extract("fake_task", {"engine": "pyresample"})  # type: ignore[arg-type]

    mock_pyresample.assert_called_once_with("fake_task", {"engine": "pyresample"})
    mock_odc.assert_not_called()
    assert result == "pyresample_result"


def test_extract_odc_cell_engine() -> None:
    """extract() with engine='odc_cell' should call _extract_odc_cell."""
    from unittest.mock import patch

    extractor = AwsGoesExtractor()
    with (
        patch.object(extractor, "_extract_odc_zone", return_value="odc_result") as mock_odc,
        patch.object(extractor, "_extract_odc_cell", return_value="naive_result") as mock_naive,
    ):
        result = extractor.extract("fake_task", {"engine": "odc_cell"})  # type: ignore[arg-type]

    mock_naive.assert_called_once_with("fake_task", {"engine": "odc_cell"})
    mock_odc.assert_not_called()
    assert result == "naive_result"

