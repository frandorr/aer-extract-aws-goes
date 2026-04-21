import os
import shutil
from datetime import datetime, timezone
from unittest.mock import PropertyMock, patch

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from aer.extract_aws_goes.core import AwsGoesExtractor
from aer.grid import GridCell
from aer.interfaces import ExtractionTask
from shapely.geometry import box

NC_FILE = "/root/repos/aer/development/local/cmp_out/OR_ABI-L1b-RadF-M6C02_G19_s20260011000228_e20260011009536_c20260011009582.nc"

@pytest.mark.skipif(not os.path.exists(NC_FILE), reason="Test GOES file not found")
def test_gdal_engine_functional(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Setup mock asset
    asset_row = {
        "id": "test_asset",
        "collection": "ABI-L1b-RadF",
        "geometry": box(-130, 20, -60, 55),
        "start_time": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
        "end_time": datetime(2026, 4, 1, 12, 10, 0, tzinfo=timezone.utc),
        "href": f"s3://noaa-goes19/ABI-L1b-RadF/2026/110/20/{os.path.basename(NC_FILE)}",
        "granule_id": os.path.basename(NC_FILE),
        "channel_id": "2",
    }
    assets = gpd.GeoDataFrame([asset_row], geometry="geometry")
    
    # Create a real GridCell for testing
    gc_geom = box(-101, 39, -100, 40)
    gc = GridCell(d=100000, geom=gc_geom, is_primary=True, cell_id="test_cell")
    
    task = ExtractionTask(
        assets=assets,
        target_grid_d=100000,
        target_grid_overlap=False,
        resolution=2000.0,
        uri=str(output_dir),
        aoi=box(-105, 35, -95, 45),
        task_context={"granule_id": os.path.basename(NC_FILE)}
    )
    
    extractor = AwsGoesExtractor()
    
    # Mock s3fs.S3FileSystem.get to copy our local file instead of downloading
    # Also mock the overlapping_grid_cells property because ExtractionTask is frozen
    with patch("s3fs.S3FileSystem.get") as mock_get, \
         patch("s3fs.S3FileSystem.__init__", return_value=None), \
         patch("aer.interfaces.ExtractionTask.overlapping_grid_cells", new_callable=PropertyMock) as mock_cells:
        
        mock_cells.return_value = [gc]
        mock_get.side_effect = lambda s3, local: shutil.copy(NC_FILE, local)
        
        results = extractor.extract(task, extract_params={"engine": "gdal"})
        
    assert len(results) == 1
    output_file = results.iloc[0]["uri"]
    assert os.path.exists(output_file)
    assert output_file.endswith(".tif")
    
    # Verify the output GeoTIFF
    with rasterio.open(output_file) as src:
        # 100km / 2000m = 50 pixels
        assert src.width == 50
        assert src.height == 50
        assert src.crs.to_epsg() == 32614  # UTM 14N
        data = src.read(1)
        assert not (data == 0).all()
        assert not (data == np.nan).all()
