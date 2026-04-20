import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from aer.extract_aws_goes.core import AwsGoesExtractor
from aer.interfaces import ExtractionTask


class TestUtmGrouping(unittest.TestCase):
    @patch("s3fs.S3FileSystem")
    @patch("aer.extract_aws_goes.core.Scene")
    def test_extract_groups_by_utm(self, mock_scene_cls, mock_s3):
        # Setup mocks
        mock_scene = mock_scene_cls.return_value
        mock_scene.available_dataset_names.return_value = ["C01"]

        # Mock resampled dataset
        mock_da = MagicMock()
        mock_da.rio.write_crs.return_value = mock_da
        mock_da.rio.clip_box.return_value = mock_da
        mock_da.rio.reproject.return_value = mock_da

        mock_resampled_scene = {"C01": mock_da}  # Simulate dict-like access
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

        task = ExtractionTask(
            assets=assets, target_grid_d=100000, target_grid_overlap=False, resolution=2000.0, uri="/tmp/test_output"
        )

        # Mock Path in core to avoid real FS operations
        with patch("aer.extract_aws_goes.core.Path") as mock_path_cls:

            def path_side_effect(path_str):
                m = MagicMock()
                m.__str__.return_value = str(path_str)
                m.__truediv__.side_effect = lambda x: path_side_effect(f"{path_str}/{x}")
                m.exists.return_value = not str(path_str).endswith(".tif")
                return m

            mock_path_cls.side_effect = path_side_effect

            with patch.object(mock_da.rio, "to_raster") as mock_to_raster:
                with patch(
                    "aer.interfaces.core.ExtractionTask.overlapping_grid_cells", new_callable=PropertyMock
                ) as mock_overlap:
                    mock_overlap.return_value = [gc1, gc2, gc3]
                    extractor = AwsGoesExtractor()
                    # Fix ArtifactSchema validation if it fails due to mocked geometry
                    with patch("aer.extract_aws_goes.core.ArtifactSchema.validate", side_effect=lambda x: x):
                        result = extractor.extract(task)

            # Verify resample calls: should be 2 (one for EPSG:32610, one for EPSG:32611)
            self.assertEqual(mock_scene.resample.call_count, 2)

            # Verify to_raster calls: should be 3
            self.assertEqual(mock_to_raster.call_count, 3)

            # Verify artifact rows
            self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
