"""Benchmark: LUT engine vs rasterio engine for GOES extraction.

Usage:
    python development/local/benchmark_lut_engine.py \
        --goes-file development/local/OR_ABI-L1b-RadF-*.nc \
        --lut-dir development/local/luts_zarr/ \
        --resolution 1000 \
        --num-cells 10

Compares:
1. engine="rasterio" (current approach - reproj per cell)
2. engine="lut" (new approach - index slicing)
"""
import argparse
import time
from pathlib import Path

import numpy as np
from aer.grid import GridDefinition, GridCell
from shapely.geometry import Polygon


def benchmark_rasterio_engine(extractor, extraction_task, extract_params):
    """Benchmark the rasterio extraction engine."""
    t0 = time.perf_counter()
    result = extractor._extract_rasterio(extraction_task, extract_params)
    elapsed = time.perf_counter() - t0
    return elapsed, len(result)


def benchmark_lut_engine(extractor, extraction_task, extract_params):
    """Benchmark the LUT extraction engine."""
    t0 = time.perf_counter()
    result = extractor._extract_lut(extraction_task, extract_params)
    elapsed = time.perf_counter() - t0
    return elapsed, len(result)


def main():
    parser = argparse.ArgumentParser(description="Benchmark LUT vs rasterio engine")
    parser.add_argument("--goes-file", required=True)
    parser.add_argument("--lut-dir", required=True)
    parser.add_argument("--resolution", type=int, default=1000)
    parser.add_argument("--num-cells", type=int, default=10)
    args = parser.parse_args()

    # Setup: create grid cells over a test area
    grid = GridDefinition(d=100_000)
    poly = Polygon([(-102, 18), (-98, 18), (-98, 22), (-102, 22), (-102, 18)])
    overlapping_cells = grid.generate_grid_cells(poly)
    grid_cells = sorted(list(overlapping_cells), key=lambda g: g.area_name(args.resolution))[:args.num_cells]

    print(f"Benchmarking {len(grid_cells)} grid cells at {args.resolution}m resolution")
    print(f"GOES file: {args.goes_file}")
    print(f"LUT dir: {args.lut_dir}")
    print()

    from aer.extract_aws_goes.core import AwsGoesExtractor
    import pandas as pd
    from aer.interfaces import ExtractionTask

    extractor = AwsGoesExtractor(target_grid_d=100_000)

    # Build dummy asset
    assets = pd.DataFrame([{
        "id": "bench_item",
        "href": args.goes_file,
        "granule_id": Path(args.goes_file).name,
        "channel_id": "01",
        "collection": "goes-16-abi-l1b-radf",
        "start_time": pd.Timestamp.now(),
        "end_time": pd.Timestamp.now()
    }])

    import geopandas as gpd
    assets_gdf = gpd.GeoDataFrame(assets, geometry=[poly])

    task_dir = Path("development/local/benchmark_tmp").absolute()
    task_dir.mkdir(parents=True, exist_ok=True)
    import shutil
    
    task = ExtractionTask(
        assets=assets_gdf,
        target_grid_d=100_000,
        target_grid_overlap=False,
        resolution=args.resolution,
        uri=str(task_dir),
        aoi=poly,
        task_context={}
    )

    print("=" * 60)
    print("Results:")
    print(f"{'Engine':<15} {'Time (s)':<12} {'Cells':<8} {'ms/cell':<10}")
    print("-" * 60)

    def run_benchmark(engine, label):
        # Setup file to bypass download
        target_file = task_dir / Path(args.goes_file).name
        shutil.copy(args.goes_file, target_file)
        
        params = {"engine": engine, "lut_dir": args.lut_dir} if engine == "lut" else {"engine": engine}
        t0 = time.perf_counter()
        result = extractor.extract(task, params)
        elapsed = time.perf_counter() - t0
        
        cells = len(result)
        ms_per_cell = (elapsed / cells * 1000) if cells > 0 else 0
        print(f"{label:<15} {elapsed:<12.2f} {cells:<8} {ms_per_cell:<10.1f}")

    # run_benchmark("rasterio", "rasterio")
    run_benchmark("lut", "lut")
    print("=" * 60)

if __name__ == "__main__":
    main()
