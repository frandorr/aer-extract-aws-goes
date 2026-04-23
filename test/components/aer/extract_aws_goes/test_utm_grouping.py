import unittest
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import tempfile
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from aer.extract_aws_goes.core import AwsGoesExtractor
from aer.interfaces import ExtractionTask


class TestUtmGrouping(unittest.TestCase):
    @pytest.mark.slow
    @patch("s3fs.S3FileSystem")
    @patch("aer.extract_aws_goes.core.Scene")
    def test_extract_groups_by_utm(self, mock_scene_cls, mock_s3):
        # Setup mocks
        mock_scene = mock_scene_cls.return_value
        mock_scene.available_dataset_names.return_value = ["C01"]

        # Mock resampled dataset with real numpy data
        mock_da = MagicMock()
        mock_da.values = np.ones((10, 10), dtype=np.float32)
        mock_da.rio.write_crs.return_value = mock_da

        mock_resampled_scene = {"C01": mock_da}
        mock_scene.resample.return_value = mock_resampled_scene

        # Create assets
        assets = gpd.GeoDataFrame(
            {
                "id": ["a1"],
                "collection": ["ABI-L1b-RadC"],
                "geometry": [box(-80, 20, -79, 21)],
                "start_time": [pd.Timestamp(2025, 1, 1)],
                "end_time": [pd.Timestamp(2025, 1, 1)],
                "href": ["s3://bucket/OR_ABI-L1b-RadC-M6C01_G16_s2025001120000.nc"],
                "channel_id": ["1"],
            },
            geometry="geometry",
        )

        # Create grid cells with different UTM CRSs
        def make_mock_gc(utm_crs, footprint, geom, cell_id, area_name):
            gc = MagicMock()
            gc.utm_crs = utm_crs
            gc.utm_footprint = footprint
            gc.geom = geom
            gc.id.return_value = cell_id
            gc.area_name.return_value = area_name
            gc.D = 100000
            gc.area_def.return_value = MagicMock(
                area_extent=(0, 0, 100000, 100000),
                width=50,
                height=50,
                projection=utm_crs,
            )
            return gc

        gc1 = make_mock_gc("EPSG:32610", box(0, 0, 100, 100), box(-80, 20, -79, 21), "cell1", "cell1_area")
        gc2 = make_mock_gc("EPSG:32610", box(100, 100, 200, 200), box(-79, 20, -78, 21), "cell2", "cell2_area")
        gc3 = make_mock_gc("EPSG:32611", box(0, 0, 100, 100), box(-70, 20, -69, 21), "cell3", "cell3_area")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a dummy file so that the post-extraction cleanup (unlink) succeeds
            import os
            dummy_nc = os.path.join(tmpdir, "OR_ABI-L1b-RadC-M6C01_G16_s2025001120000.nc")
            open(dummy_nc, "w").close()

            task = ExtractionTask(
                assets=assets, target_grid_d=100000, target_grid_overlap=False, resolution=2000.0, uri=tmpdir
            )

            # Track rasterio.open calls by wrapping the real function
            import rasterio as _real_rasterio

            rio_open_calls = []
            original_open = _real_rasterio.open

            def tracking_open(*args, **kwargs):
                rio_open_calls.append(args)
                return original_open(*args, **kwargs)

            with patch.object(_real_rasterio, "open", side_effect=tracking_open):
                with patch(
                    "aer.interfaces.core.ExtractionTask.overlapping_grid_cells", new_callable=PropertyMock
                ) as mock_overlap:
                    mock_overlap.return_value = [gc1, gc2, gc3]
                    extractor = AwsGoesExtractor()
                    with patch("aer.extract_aws_goes.core.ArtifactSchema.validate", side_effect=lambda x: x):
                        result = extractor.extract(task, extract_params={"engine": "satpy"})

            # Verify resample calls: should be 2 (one for EPSG:32610, one for EPSG:32611)
            self.assertEqual(mock_scene.resample.call_count, 2)

            # Verify rasterio.open calls: should be 3 (one per grid cell)
            self.assertEqual(len(rio_open_calls), 3)

            # Verify artifact rows
            self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
