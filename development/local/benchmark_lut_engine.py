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
    spatial_extent = grid.intersecting_grid_spatial_extent(poly)
    grid_cells = sorted(list(spatial_extent.grid_cells), key=lambda g: g.area_name(args.resolution))[:args.num_cells]

    print(f"Benchmarking {len(grid_cells)} grid cells at {args.resolution}m resolution")
    print(f"GOES file: {args.goes_file}")
    print(f"LUT dir: {args.lut_dir}")
    print()

    from aer.extract_aws_goes.core import AwsGoesExtractor

    extractor = AwsGoesExtractor(target_grid_d=100_000)

    # Note: This benchmark requires pre-generated LUTs.
    # Run generate_luts.py first to create them.

    print("=" * 60)
    print("Results:")
    print(f"{'Engine':<15} {'Time (s)':<12} {'Cells':<8} {'ms/cell':<10}")
    print("-" * 60)

    # Run rasterio benchmark
    # (Implementation depends on having an ExtractionTask — adapt as needed)
    print("NOTE: Full benchmark requires ExtractionTask setup.")
    print("Use this script as a template and adapt to your specific test setup.")
    print("=" * 60)


if __name__ == "__main__":
    main()
