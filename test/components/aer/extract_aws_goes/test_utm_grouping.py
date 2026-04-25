import unittest
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import tempfile
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from aer.extract_aws_goes.core import AwsGoesExtractor
from aer.interfaces import ExtractionTask


class TestUtmGrouping(unittest.TestCase):
    @pytest.mark.slow
    @patch("s3fs.S3FileSystem")
    @patch("aer.extract_aws_goes.core.read_goes_crop")
    @patch("aer.extract_aws_goes.lut.load_utm_zone_lut")
    @patch("aer.extract_aws_goes.utils.download_lut_if_needed")
    def test_extract_groups_by_utm(self, mock_download, mock_load_lut, mock_read_crop, mock_s3):
        # Setup mocks
        mock_lut = MagicMock()
        mock_lut.crop_slices = (0, 10, 0, 10)
        mock_load_lut.return_value = mock_lut
        mock_read_crop.return_value = MagicMock()
        mock_da = MagicMock()
        mock_da.rio.to_raster = MagicMock()

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

            with patch("aer.interfaces.core.ExtractionTask.overlapping_grid_cells", new_callable=PropertyMock) as mock_overlap:
                with patch("aer.extract_aws_goes.lut.extract_cell_from_lut") as mock_extract_cell:
                    mock_extract_cell.return_value = mock_da
                    mock_overlap.return_value = [gc1, gc2, gc3]
                    extractor = AwsGoesExtractor()
                    with patch("aer.extract_aws_goes.core.ArtifactSchema.validate", side_effect=lambda x: x):
                        result = extractor.extract(task, extract_params={"engine": "satpy"})

            # Verify read_goes_crop calls: should be 2 (one for EPSG:32610, one for EPSG:32611)
            self.assertEqual(mock_read_crop.call_count, 2)
            
            # Verify extract_cell_from_lut calls: should be 3 (one per grid cell)
            self.assertEqual(mock_extract_cell.call_count, 3)

            # Verify artifact rows
            self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
