"""Offline GOES→UTM zone LUT generator.

Usage:
    python -m aer.extract_aws_goes.generate_luts \
        --goes-file /path/to/OR_ABI-L1b-RadF-M6C01_G19_....nc \
        --output-dir /path/to/luts/ \
        --resolutions 500 1000 2000 \
        [--utm-zones 32720 32721 ...]  # optional: specific zones only
        [--max-workers 8]

If --utm-zones is not specified, auto-detects all UTM zones intersecting
the GOES disk footprint.
"""

import argparse
import logging
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing


from aer.extract_aws_goes.lut import (
    SUPPORTED_RESOLUTIONS,
    generate_utm_zone_lut,
)
from aer.extract_aws_goes.utils import (
    compute_goes_source_area_def,
    detect_goes_utm_zones,
    detect_combo,
)


def _generate_lut_worker(goes_file, utm_epsg, resolution, output_dir):
    """Worker function for parallel LUT generation.

    Runs in a subprocess. All heavy objects are explicitly deleted
    and garbage-collected before returning so the child process
    gives memory back to the OS.
    """
    import gc
    try:
        lut = generate_utm_zone_lut(
            goes_path=Path(goes_file),
            utm_epsg=utm_epsg,
            resolution=resolution,
            output_uri=output_dir,
        )
        combo = detect_combo(str(goes_file))
        # Free the large arrays before returning the small result dict
        del lut
        return {
            "combo": combo,
            "epsg": str(utm_epsg),
            "resolution": str(resolution),
            "success": True,
        }
    except Exception as e:
        return {"epsg": str(utm_epsg), "resolution": str(resolution), "success": False, "error": str(e)}
    finally:
        gc.collect()


def _run_task_sequential(goes_file, utm_epsg, resolution, output_dir, logger, idx, total):
    """Run a single LUT task in-process (no pool overhead) with memory cleanup."""
    import gc
    res_data = _generate_lut_worker(goes_file, utm_epsg, resolution, output_dir)
    if res_data["success"]:
        logger.info(f"[{idx}/{total}] {res_data['combo']} EPSG:{res_data['epsg']} @ {res_data['resolution']}m — Success")
    else:
        logger.error(
            f"[{idx}/{total}] EPSG:{res_data['epsg']} @ {res_data['resolution']}m — FAILED: {res_data['error']}"
        )
    gc.collect()
    return res_data


def main():
    """Main entry point for the GOES LUT generator CLI."""
    parser = argparse.ArgumentParser(description="Generate GOES→UTM zone lookup tables")
    parser.add_argument("--goes-file", nargs="+", required=True, help="Path to one or more GOES NetCDF files")
    parser.add_argument("--output-dir", required=True, help="Output directory for LUT .npz files")
    parser.add_argument(
        "--resolutions", nargs="+", type=int, default=list(SUPPORTED_RESOLUTIONS), help="Target resolutions in meters"
    )
    parser.add_argument(
        "--utm-zones", nargs="*", type=int, default=None, help="Specific UTM zone EPSG codes (auto-detect if omitted)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=1, help="Maximum number of parallel workers (default: 1 for memory safety)"
    )

    args = parser.parse_args()

    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    for goes_file in args.goes_file:
        # 1. Get GOES source area definition
        source_area_def = compute_goes_source_area_def(goes_file)
        combo = detect_combo(goes_file)
        logger.info(f"Processing {combo} from {Path(goes_file).name}")

        # 2. Determine UTM zones
        if args.utm_zones:
            utm_zones = args.utm_zones
        else:
            utm_zones = detect_goes_utm_zones(source_area_def)
        logger.info(f"Detected {len(utm_zones)} UTM zones for {combo}")

        # Free heavy area def immediately — not needed after zone detection
        del source_area_def

        for utm_epsg in utm_zones:
            for resolution in args.resolutions:
                tasks.append((goes_file, utm_epsg, resolution))

    # Sort tasks by resolution descending (2000m first, then 1000m, then 500m)
    # This ensures quick, low-memory tasks complete first before the memory-intensive ones.
    tasks.sort(key=lambda x: x[2], reverse=True)

    total = len(tasks)
    failed = 0

    # ── Sequential path (max_workers == 1) ───────────────────────────
    # Avoids ProcessPoolExecutor entirely to skip IPC/serialisation overhead
    # and eliminate the BrokenProcessPool risk from OOM kills.
    if args.max_workers == 1:
        logger.info(f"Starting SEQUENTIAL generation of {total} LUTs (max-workers=1)")
        t_start = time.time()
        for idx, (g_file, epsg, res) in enumerate(tasks, 1):
            res_data = _run_task_sequential(g_file, epsg, res, str(output_dir), logger, idx, total)
            if not res_data["success"]:
                failed += 1
        elapsed = time.time() - t_start
        logger.info(f"Done. {total - failed}/{total} succeeded, {failed} failed in {elapsed:.1f}s")
        logger.info(f"LUTs saved to {output_dir}")
        return

    # ── Parallel path (max_workers > 1) ──────────────────────────────
    # Submit in small batches (== max_workers) so we never hold more
    # futures in memory than we have workers.  Each batch is waited on
    # fully before starting the next, which prevents runaway memory use.
    logger.info(f"Starting BATCHED parallel generation of {total} LUTs using {args.max_workers} workers")
    t_start = time.time()
    completed = 0
    batch_size = args.max_workers

    for batch_start in range(0, total, batch_size):
        batch = tasks[batch_start : batch_start + batch_size]

        with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
            future_to_task = {
                executor.submit(_generate_lut_worker, g_file, epsg, res, str(output_dir)): (g_file, epsg, res)
                for g_file, epsg, res in batch
            }

            for future in as_completed(future_to_task):
                res_data = future.result()
                completed += 1

                if res_data["success"]:
                    logger.info(
                        f"[{completed}/{total}] {res_data['combo']} EPSG:{res_data['epsg']} @ {res_data['resolution']}m — Success"
                    )
                else:
                    failed += 1
                    logger.error(
                        f"[{completed}/{total}] EPSG:{res_data['epsg']} @ {res_data['resolution']}m — FAILED: {res_data['error']}"
                    )

        # Pool is shut down after each batch — child processes are reaped, memory freed
        import gc
        gc.collect()

    elapsed = time.time() - t_start
    logger.info(f"Done. {total - failed}/{total} succeeded, {failed} failed in {elapsed:.1f}s")
    logger.info(f"LUTs saved to {output_dir}")


if __name__ == "__main__":
    main()
