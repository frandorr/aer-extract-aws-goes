from __future__ import annotations

import re
import gc
from pathlib import Path
from typing import Any

import numpy as np
import attrs
import satpy
from aer.extract import ExtractionTask
from aer.extract.core import ExtractionStatus
from aer.download_api import download
from structlog import get_logger
from aer.plugin import plugin

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


def extract_resample_lut(
    source_area: Any,
    target_area: Any,
    radius_of_influence: float | None = None,
) -> dict[str, Any]:
    """Compute a nearest-neighbor resampling lookup table.

    Args:
        source_area: pyresample AreaDefinition for the source grid.
        target_area: pyresample AreaDefinition for the target grid.
        radius_of_influence: Search radius in meters. None lets pyresample
            compute a sensible default.

    Returns:
        Dict with keys: index_array, valid_input_index, valid_output_index,
        distance_array, source_shape, target_shape.
    """
    from pyresample.kd_tree import XArrayResamplerNN

    kwargs: dict[str, Any] = {"neighbours": 1}
    if radius_of_influence is not None:
        kwargs["radius_of_influence"] = radius_of_influence

    resampler = XArrayResamplerNN(source_area, target_area, **kwargs)
    valid_input_index, valid_output_index, index_array, distance_array = resampler.get_neighbour_info()

    # Convert dask arrays to numpy if needed
    def _to_numpy(arr: Any) -> np.ndarray:
        if hasattr(arr, "compute"):
            return np.asarray(arr.compute())
        return np.asarray(arr)

    return {
        "index_array": _to_numpy(index_array),
        "valid_input_index": _to_numpy(valid_input_index),
        "valid_output_index": _to_numpy(valid_output_index),
        "distance_array": _to_numpy(distance_array),
        "source_shape": source_area.shape,
        "target_shape": target_area.shape,
    }


def apply_resample_lut(
    lut: dict[str, Any],
    source_data: np.ndarray,
    fill_value: float = np.nan,
) -> np.ndarray:
    """Apply a precomputed resampling LUT to source data.

    Args:
        lut: Dict returned by extract_resample_lut.
        source_data: 2D array with shape matching lut['source_shape'].
        fill_value: Value for pixels with no valid source neighbor.

    Returns:
        2D numpy array with shape lut['target_shape'].
    """
    index_array = lut["index_array"]
    valid_input_index = lut["valid_input_index"]
    target_shape = lut["target_shape"]

    # Flatten source data
    source_flat = source_data.ravel()

    # For nearest-neighbor with neighbours=1, index_array is (target_rows, target_cols, 1)
    index_2d = index_array[:, :, 0]

    # Create output filled with fill_value
    output = np.full(target_shape, fill_value, dtype=np.float64)

    # Valid mask: index >= 0 means a neighbor was found
    valid_mask = index_2d >= 0

    # Map through valid_input_index to get actual source flat indices
    valid_indices = index_2d[valid_mask]
    actual_source_indices = valid_input_index[valid_indices]

    output[valid_mask] = source_flat[actual_source_indices]
    return output


@plugin("aws_goes", "extract")
def extract_aws_goes(task: ExtractionTask, **kwargs) -> ExtractionTask:
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
        if modifiers := kwargs.get("modifiers"):
            scene.load(mapped, modifiers=modifiers)
        else:
            scene.load(mapped)
        grid_cell = grid.grid_cell
        area_def = grid_cell.area_def(channel.resolution)
        area_name = grid_cell.area_name(channel.resolution)

        resampled = scene.resample(area_def, datasets=mapped, generate=False, unload=True, resampler="nearest")
        ts = sr.start_time.strftime("%Y%m%dT%H%M%S")
        filename = f"{ts}_{sr.product_id}_{mapped[0]}_{grid.name}_{grid.dist}km.nc"
        output_path = Path(task.output_dir) / filename
        resampled.save_dataset(
            dataset_id=mapped[0],
            writer="cf",
            filename=str(output_path),
        )

        # Free memory
        del resampled, scene, gdf, downloaded
        gc.collect()

        logger.info("extract_success", area_name=area_name, channel=channel.c_id)
        return attrs.evolve(task, status=ExtractionStatus.SUCCESS)

    except Exception as exc:
        logger.error("extract_failed", reason=str(exc))
        return attrs.evolve(task, status=ExtractionStatus.FAILED)
