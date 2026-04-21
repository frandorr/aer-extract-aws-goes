"""Offline GOES→UTM zone LUT generator.

Usage:
    python -m aer.extract_aws_goes.generate_luts \
        --goes-file /path/to/OR_ABI-L1b-RadF-M6C01_G19_....nc \
        --output-dir /path/to/luts/ \
        --resolutions 500 1000 2000 \
        [--utm-zones 32720 32721 ...]  # optional: specific zones only
        [--chunk-size 1024]

If --utm-zones is not specified, auto-detects all UTM zones intersecting
the GOES disk footprint.
"""
import argparse
import logging
import time
from pathlib import Path

from aer.extract_aws_goes.core import detect_reader
from aer.extract_aws_goes.lut import (
    SUPPORTED_RESOLUTIONS,
    compute_goes_source_area_def,
    detect_goes_utm_zones,
    generate_utm_zone_lut,
)

def main():
    parser = argparse.ArgumentParser(description="Generate GOES→UTM zone lookup tables")
    parser.add_argument("--goes-file", required=True, help="Path to a GOES NetCDF file")
    parser.add_argument("--output-dir", required=True, help="Output directory for LUT Zarr stores")
    parser.add_argument("--resolutions", nargs="+", type=int, default=list(SUPPORTED_RESOLUTIONS),
                        help="Target resolutions in meters")
    parser.add_argument("--utm-zones", nargs="*", type=int, default=None,
                        help="Specific UTM zone EPSG codes (auto-detect if omitted)")
    parser.add_argument("--chunk-size", type=int, default=1024, help="Zarr chunk size")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # 1. Get GOES source area definition
    source_area_def = compute_goes_source_area_def(args.goes_file)
    logger.info(f"Source area: {source_area_def.area_id}, shape: {source_area_def.shape}")

    # 2. Determine UTM zones
    if args.utm_zones:
        utm_zones = args.utm_zones
    else:
        utm_zones = detect_goes_utm_zones(source_area_def)
    logger.info(f"Processing {len(utm_zones)} UTM zones: {utm_zones[:5]}...")

    # 3. Generate LUTs
    output_dir = Path(args.output_dir)
    total = len(utm_zones) * len(args.resolutions)
    completed = 0
    for utm_epsg in utm_zones:
        for resolution in args.resolutions:
            t0 = time.time()
            try:
                generate_utm_zone_lut(
                    source_area_def=source_area_def,
                    utm_epsg=utm_epsg,
                    resolution=resolution,
                    output_dir=output_dir,
                    chunk_size=args.chunk_size,
                )
                elapsed = time.time() - t0
                completed += 1
                logger.info(f"[{completed}/{total}] EPSG:{utm_epsg} @ {resolution}m — {elapsed:.1f}s")
            except Exception as e:
                logger.error(f"Failed EPSG:{utm_epsg} @ {resolution}m: {e}")

    logger.info(f"Done. Generated {completed}/{total} LUTs in {output_dir}")

if __name__ == "__main__":
    main()
