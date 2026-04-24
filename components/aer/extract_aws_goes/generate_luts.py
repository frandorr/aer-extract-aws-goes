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


from aer.extract_aws_goes.lut import (
    SUPPORTED_RESOLUTIONS,
    generate_utm_zone_lut,
)
from aer.extract_aws_goes.utils import (
    compute_goes_source_area_def,
    detect_goes_utm_zones,
)


def main():
    """Main entry point for the GOES LUT generator CLI."""
    parser = argparse.ArgumentParser(description="Generate GOES→UTM zone lookup tables")
    parser.add_argument("--goes-file", required=True, help="Path to a GOES NetCDF file")
    parser.add_argument("--output-dir", required=True, help="Output directory for LUT .npz files")
    parser.add_argument(
        "--resolutions", nargs="+", type=int, default=list(SUPPORTED_RESOLUTIONS), help="Target resolutions in meters"
    )
    parser.add_argument(
        "--utm-zones", nargs="*", type=int, default=None, help="Specific UTM zone EPSG codes (auto-detect if omitted)"
    )
    # Remove chunk-size as it's no longer used for npz
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
    from aer.extract_aws_goes.lut import save_lut_config, load_lut_config

    output_dir = Path(args.output_dir)
    total = len(utm_zones) * len(args.resolutions)
    completed = 0

    # Load existing config to merge new results
    current_config = load_lut_config(str(output_dir))

    # Detect combo for grouping in config
    from aer.extract_aws_goes.core import AwsGoesExtractor

    combo = AwsGoesExtractor._detect_combo(args.goes_file)

    if combo not in current_config:
        current_config[combo] = {}

    for utm_epsg in utm_zones:
        epsg_str = str(utm_epsg)
        if epsg_str not in current_config[combo]:
            current_config[combo][epsg_str] = {}

        for resolution in args.resolutions:
            t0 = time.time()
            try:
                lut = generate_utm_zone_lut(
                    goes_path=Path(args.goes_file),
                    utm_epsg=utm_epsg,
                    resolution=resolution,
                    output_uri=output_dir,
                )

                # Capture crop slices for config update
                current_config[combo][epsg_str][str(resolution)] = lut.crop_slices

                elapsed = time.time() - t0
                completed += 1
                logger.info(f"[{completed}/{total}] EPSG:{utm_epsg} @ {resolution}m — {elapsed:.1f}s")
            except Exception as e:
                logger.error(f"Failed EPSG:{utm_epsg} @ {resolution}m: {e}")

    # Auto-save the updated configuration
    save_lut_config(current_config, str(output_dir))

    logger.info(f"Done. Generated {completed}/{total} LUTs in {output_dir}")
    logger.info("lut_config.json has been automatically updated.")


if __name__ == "__main__":
    main()
