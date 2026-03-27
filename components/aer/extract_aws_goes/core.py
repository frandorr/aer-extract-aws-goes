from __future__ import annotations

import re
import gc
from pathlib import Path
from typing import Any

import cloudpickle
import numpy as np
import xarray as xr
import attrs
import satpy
from aer.extract import ExtractionTask
from aer.extract.core import ExtractionStatus
from aer.download_api import download
from pyresample.kd_tree import get_neighbour_info, get_sample_from_neighbour_info
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


def _parse_granule_id(granule_id: str) -> tuple[str, str]:
    """Extract satellite and scan type from a GOES granule ID.

    Examples
    --------
    >>> _parse_granule_id("ABI-L1b-RadF-M6C01_G19_s20260331200204_e20260331209512_c20260331209563.nc")
    ('G19', 'FD')
    >>> _parse_granule_id("ABI-L1b-RadC-M6C01_G16_s20260331200204_e20260331209512_c20260331209563.nc")
    ('G16', 'Conus')
    """
    sat_match = re.search(r"_(G\d+)_", granule_id)
    scan_match = re.search(r"Rad([CF])", granule_id)
    satellite = sat_match.group(1) if sat_match else "unknown"
    scan_char = scan_match.group(1) if scan_match else "?"
    scan_type = "FD" if scan_char == "F" else "Conus"
    return satellite, scan_type


def _lut_key(satellite: str, scan_type: str, area_name: str) -> str:
    """Build a LUT cache key from satellite, scan type, and area name."""
    return f"{satellite}_{scan_type}_{area_name}"


def save_lookup_table(
    filename: str,
    valid_input_index: np.ndarray,
    valid_output_index: np.ndarray,
    index_array: np.ndarray,
    shape: tuple[int, int],
) -> None:
    """Save a lookup table to a compressed npz file.

    Uses np.packbits for boolean arrays and np.savez_compressed for
    efficient storage.
    """
    packed_input = np.packbits(valid_input_index)
    packed_output = np.packbits(valid_output_index)

    np.savez_compressed(
        filename,
        valid_input_index=packed_input,
        valid_output_index=packed_output,
        index_array=index_array.astype(np.int32),
        valid_input_length=len(valid_input_index),
        valid_output_length=len(valid_output_index),
        shape=np.array(shape),
    )


def load_lookup_table(
    filename: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, tuple[int, int]]:
    """Load a lookup table from a compressed npz file.

    Returns (valid_input_index, valid_output_index, index_array, shape).
    """
    npz = np.load(filename)
    valid_input_index = np.unpackbits(npz["valid_input_index"])[: int(npz["valid_input_length"])]
    valid_output_index = np.unpackbits(npz["valid_output_index"])[: int(npz["valid_output_length"])]
    index_array = npz["index_array"]
    shape = tuple(npz["shape"])

    return (
        valid_input_index.astype(bool),
        valid_output_index.astype(bool),
        index_array,
        shape,
    )


def apply_lookup_table(
    source_arr: np.ndarray,
    valid_input_index: np.ndarray,
    valid_output_index: np.ndarray,
    index_array: np.ndarray,
    target_shape: tuple[int, int],
) -> np.ndarray:
    """Apply a lookup table for fast nearest-neighbour resampling.

    Uses pyresample's get_sample_from_neighbour_info.
    """
    res = get_sample_from_neighbour_info(
        "nn",
        target_shape,
        source_arr,
        valid_input_index,
        valid_output_index,
        index_array,
        fill_value=np.nan,
    )
    return res


def build_and_save_lut(
    scene: satpy.Scene,
    area_def,
    area_name: str,
    lut_key_str: str,
    lut_cache_dir: Path,
    dataset_name: str,
) -> tuple:
    """Build a resampling LUT from scene and area definition, save to disk.

    Returns (valid_input_index, valid_output_index, index_array,
             target_shape, template).
    """
    source_geo_def = scene[dataset_name].attrs["area"]

    # Compute neighbour info
    valid_input_index, valid_output_index, index_array, _ = get_neighbour_info(
        source_geo_def,
        area_def,
        radius_of_influence=10000,
        neighbours=1,
        nprocs=-1,
    )

    target_shape = (area_def.height, area_def.width)

    # Save LUT
    lut_path = lut_cache_dir / f"lut_{lut_key_str}.npz"
    save_lookup_table(
        str(lut_path),
        valid_input_index,
        valid_output_index,
        index_array,
        target_shape,
    )

    # Build and save template via resample once
    resampled_scene = scene.resample(
        area_def,
        datasets=[dataset_name],
        generate=False,
        unload=True,
        resampler="nearest",
    )
    da_template = resampled_scene[dataset_name]
    template = {
        "coords": {k: v.values for k, v in da_template.coords.items()},
        "dims": da_template.dims,
        "attrs": dict(da_template.attrs),
        "name": da_template.name,
    }

    template_path = lut_cache_dir / f"template_{lut_key_str}.pkl"
    with open(template_path, "wb") as f:
        cloudpickle.dump(template, f)

    return valid_input_index, valid_output_index, index_array, target_shape, template


def load_lut(lut_key_str: str, lut_cache_dir: Path) -> tuple:
    """Load a cached LUT and template from disk.

    Returns (valid_input_index, valid_output_index, index_array,
             target_shape, template).
    """
    lut_path = lut_cache_dir / f"lut_{lut_key_str}.npz"
    valid_input_index, valid_output_index, index_array, shape = load_lookup_table(str(lut_path))

    template_path = lut_cache_dir / f"template_{lut_key_str}.pkl"
    with open(template_path, "rb") as f:
        template = cloudpickle.load(f)

    return valid_input_index, valid_output_index, index_array, shape, template


def get_or_build_lut(
    scene: satpy.Scene,
    area_def,
    area_name: str,
    lut_key_str: str,
    lut_cache_dir: Path,
    dataset_name: str,
) -> tuple:
    """Return LUT data, building only if not cached on disk.

    Returns (valid_input_index, valid_output_index, index_array,
             target_shape, template).
    """
    lut_path = lut_cache_dir / f"lut_{lut_key_str}.npz"
    if lut_path.exists():
        logger.info("lut_cache_hit", lut_key=lut_key_str)
        return load_lut(lut_key_str, lut_cache_dir)
    logger.info("lut_cache_miss", lut_key=lut_key_str)
    return build_and_save_lut(scene, area_def, area_name, lut_key_str, lut_cache_dir, dataset_name)


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

        # Parse granule ID for LUT key
        satellite, scan_type = _parse_granule_id(granule_id)
        lut_key_str = _lut_key(satellite, scan_type, area_name)

        # LUT cache directory
        lut_cache_dir = Path(task.output_dir) / "luts"
        lut_cache_dir.mkdir(parents=True, exist_ok=True)

        # LUT-based resampling (fast, cached)
        valid_input_index, valid_output_index, index_array, target_shape, template = get_or_build_lut(
            scene, area_def, area_name, lut_key_str, lut_cache_dir, mapped[0]
        )

        source_data = np.asarray(scene[mapped[0]].data)
        if hasattr(source_data, "compute"):
            source_data = source_data.compute()

        resampled_data = apply_lookup_table(
            source_data, valid_input_index, valid_output_index, index_array, target_shape
        )

        # Wrap in xarray DataArray for save_dataset compatibility
        resampled_da = xr.DataArray(
            resampled_data.reshape(target_shape),
            coords=template["coords"],
            dims=template["dims"],
            attrs=template["attrs"],
            name=template["name"],
        )

        ts = sr.start_time.strftime("%Y%m%dT%H%M%S")
        filename = f"{ts}_{sr.product_id}_{mapped[0]}_{grid.name}_{grid.dist}km.nc"
        output_path = Path(task.output_dir) / filename
        xr.Dataset({mapped[0]: resampled_da}).to_netcdf(str(output_path))

        # Free memory
        del resampled_da, scene, gdf, downloaded
        gc.collect()

        logger.info("extract_success", area_name=area_name, channel=channel.c_id)
        return attrs.evolve(task, status=ExtractionStatus.SUCCESS)

    except Exception as exc:
        logger.error("extract_failed", reason=str(exc))
        return attrs.evolve(task, status=ExtractionStatus.FAILED)
