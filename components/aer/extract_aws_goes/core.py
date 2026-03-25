from __future__ import annotations

import re
from pathlib import Path

import attrs
import satpy
from satpy.writers.core.compute import compute_writer_results
from aer.extract import ExtractionTask
from aer.extract.core import ExtractionStatus
from aer.download_api import download
from structlog import get_logger

logger = get_logger()

L1B_PATTERN = re.compile(r"ABI-L1b-Rad[CF]")
L2_AOD_PATTERN = re.compile(r"ABI-L2-AOD[CF]")
L2_BRF_PATTERN = re.compile(r"ABI-L2-BRF[CF]")


def detect_reader(filename: str) -> str | None:
    """Detect the satpy reader based on the GOES filename."""
    if L1B_PATTERN.search(filename):
        return "abi_l1b"
    if L2_BRF_PATTERN.search(filename):
        return "abi_l2_brf_nc"
    if L2_AOD_PATTERN.search(filename):
        return "abi_l2_nc"
    return None


def map_channel_ids_to_satpy_names(channel_ids: set[str], available_names: set[str]) -> list[str]:
    """Map channel IDs to satpy dataset names.

    Handles direct matches ('C01' in available) and numeric IDs
    ('1' -> 'C01', '13' -> 'C13').
    """
    result: list[str] = []
    for cid in channel_ids:
        if cid in available_names:
            result.append(cid)
        elif cid.isdigit():
            padded = f"C{int(cid):02d}"
            if padded in available_names:
                result.append(padded)
    return result


def extract_aws_goes(task: ExtractionTask) -> ExtractionTask:
    """Extract GOES satellite data from AWS using satpy.

    Downloads the file from the search result, creates a satpy Scene,
    maps the channel, resamples to the grid cell area definition,
    and saves as GeoTIFF.

    Args:
        task: Extraction task with search result and output directory.

    Returns:
        The task with status updated to SUCCESS or FAILED.
    """
    sr = task.search_result
    granule_id = sr.granule_id
    channel = sr.channel
    grid = sr.grid

    reader = detect_reader(granule_id)
    if reader is None:
        logger.error("extract_failed", reason=f"Cannot detect reader from {granule_id}")
        return attrs.evolve(task, status=ExtractionStatus.FAILED)

    if channel is None:
        logger.error("extract_failed", reason="SearchResult has no channel")
        return attrs.evolve(task, status=ExtractionStatus.FAILED)

    if grid is None:
        logger.error("extract_failed", reason="SearchResult has no grid")
        return attrs.evolve(task, status=ExtractionStatus.FAILED)

    try:
        # Reconstruct a single-row gdf for the download function
        from aer.search import SearchResult as SR

        gdf = SR.to_gdf([sr])
        downloaded = download(gdf, task.output_dir)
        local_path = Path(downloaded.iloc[0]["local_path"])

        # Build satpy scene
        scene = satpy.Scene(reader=reader, filenames=[str(local_path)])

        # Map channel c_id to satpy name
        available = set(scene.available_dataset_names())
        mapped = map_channel_ids_to_satpy_names({channel.c_id}, available)

        if not mapped:
            logger.error("extract_failed", reason=f"Channel {channel.c_id} not found in available: {available}")
            return attrs.evolve(task, status=ExtractionStatus.FAILED)

        # Load, resample, save
        scene.load(mapped)
        grid_cell = grid.grid_cell
        area_def = grid_cell.area_def(channel.resolution)
        area_name = grid_cell.area_name(channel.resolution)

        resampled = scene.resample(area_def, datasets=mapped, generate=False, unload=True, resampler="nearest")
        tif_name = f"{area_name}.tif"
        result = resampled.save_datasets(
            writer="geotiff",
            base_dir=str(task.output_dir),
            filename=tif_name,
            compute=False,
        )
        compute_writer_results(result)

        logger.info("extract_success", area_name=area_name, channel=channel.c_id)
        return attrs.evolve(task, status=ExtractionStatus.SUCCESS)

    except Exception as exc:
        logger.error("extract_failed", reason=str(exc))
        return attrs.evolve(task, status=ExtractionStatus.FAILED)
