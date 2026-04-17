from __future__ import annotations

import re
import gc
import hashlib
from pathlib import Path
from collections import defaultdict
from typing import Any, Sequence, cast, override

import threading
import cloudpickle
import numpy as np
import geopandas as gpd
import xarray as xr
import satpy
from concurrent.futures import ThreadPoolExecutor, as_completed
from aer.interfaces import Extractor, ExtractionTask
from aer.schemas import ArtifactSchema, AssetSchema, GridSchema
from aer.grid import GridCell
from pandera.typing.geopandas import GeoDataFrame
from shapely.geometry.base import BaseGeometry
from pyresample.kd_tree import get_neighbour_info, get_sample_from_neighbour_info
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


def group_results(results):
    grouped = defaultdict(list)

    for r in results:
        key = (r.granule_id, r.channel)
        grouped[key].append(r)

    return grouped


_lut_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)

SUPPORTED_COLLECTIONS: Sequence[str] = [
    "ABI-L1b-RadC",
    "ABI-L1b-RadF",
    "ABI-L1b-RadM",
    "ABI-L2-AODC",
    "ABI-L2-AODF",
    "ABI-L2-BRFC",
    "ABI-L2-BRFF",
    "ABI-L2-BRFM",
]


class AwsGoesExtractor(Extractor, plugin_abstract=False):
    """Extractor plugin for GOES ABI satellite data from AWS.

    Downloads NetCDF granules, builds satpy Scenes, resamples to grid cells
    using LUT-cached nearest-neighbour interpolation, and saves as NetCDF.
    """

    supported_collections: Sequence[str] = SUPPORTED_COLLECTIONS

    @property
    @override
    def target_grid_d(self) -> int:
        """Grid cell size in meters (100 km)."""
        return 100_000

    @property
    @override
    def target_grid_overlap(self) -> bool:
        return False

    @override
    def prepare_for_extraction(
        self,
        search_results: GeoDataFrame[AssetSchema],
        target_aoi: BaseGeometry | None = None,
        resolution: float | None = None,
        uri: str | None = None,
        prepare_params: dict[str, Any] | None = None,
    ) -> Sequence[ExtractionTask]:
        """Group assets by granule_id so each granule is downloaded once.

        Each ExtractionTask will contain all asset rows sharing the same
        granule_id. The extract() method will iterate overlapping grid cells
        internally for each granule.
        """
        if resolution is None or uri is None:
            raise ValueError(
                "prepare_for_extraction requires resolution and uri. "
                "Override with custom implementation if you want different behaviour."
            )

        tasks: list[ExtractionTask] = []

        # Group by granule_id if available, otherwise fall back to one-per-row
        if "granule_id" in search_results.columns:
            for _granule_id, group_df in search_results.groupby("granule_id"):
                task = ExtractionTask(
                    assets=cast(GeoDataFrame, group_df),
                    target_grid_d=self.target_grid_d,
                    target_grid_overlap=self.target_grid_overlap,
                    resolution=resolution,
                    uri=uri,
                    aoi=target_aoi,
                    task_context={
                        "prepare_params": prepare_params,
                        "granule_id": str(_granule_id),
                    },
                )
                tasks.append(task)
        else:
            # Fallback: one task per asset row
            for i in range(len(search_results)):
                task = ExtractionTask(
                    assets=search_results.iloc[[i]],
                    target_grid_d=self.target_grid_d,
                    target_grid_overlap=self.target_grid_overlap,
                    resolution=resolution,
                    uri=uri,
                    aoi=target_aoi,
                    task_context={"prepare_params": prepare_params},
                )
                tasks.append(task)

        return tasks

    @override
    def extract(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any] | None = None,
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract GOES data for a batch of assets sharing the same granule.

        Downloads the granule once, builds a satpy Scene, then resamples
        to each overlapping grid cell using LUT-cached nearest-neighbour
        interpolation. Returns one ArtifactSchema row per grid cell.
        """
        extract_params = extract_params or {}
        assets = extraction_task.assets
        resolution = extraction_task.resolution
        uri = extraction_task.uri
        grid_cells = extraction_task.overlapping_grid_cells

        # All rows in this task share the same granule
        first_row = assets.iloc[0]
        href: str = first_row["href"]
        granule_id: str = first_row.get("granule_id", Path(href).name)
        channel_id: str | None = first_row.get("channel_id")
        collection: str = first_row["collection"]
        start_time = first_row["start_time"]
        end_time = first_row["end_time"]
        source_ids = ",".join(assets["id"].astype(str).tolist())

        reader = detect_reader(granule_id)
        if reader is None:
            raise ValueError(f"Cannot detect satpy reader from granule: {granule_id}")

        if channel_id is None:
            raise ValueError(f"No channel_id in asset row for granule: {granule_id}")

        # Download file from S3
        import s3fs

        fs = s3fs.S3FileSystem(anon=True)
        local_dir = Path(uri)
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / Path(href).name
        if not local_path.exists():
            s3_path = href.replace("s3://", "")
            fs.get(s3_path, str(local_path))
        logger.info("file_downloaded", local_path=str(local_path))

        # Build satpy scene
        scene = satpy.Scene(reader=reader, filenames=[str(local_path)])
        available = set(scene.available_dataset_names())
        mapped = map_channel_ids_to_satpy_names({channel_id}, available)

        if not mapped:
            raise ValueError(f"Channel {channel_id} not found in available datasets: {available}")

        dataset_name = mapped[0]
        modifiers = extract_params.get("modifiers")
        if modifiers is not None:
            scene.load([dataset_name], modifiers=modifiers)
        else:
            scene.load([dataset_name])

        source_data = np.asarray(scene[dataset_name].data)
        if hasattr(source_data, "compute"):
            source_data = source_data.compute()

        # Parse satellite/scan info for LUT key
        satellite, scan_type = _parse_granule_id(granule_id)

        # LUT cache directory
        lut_cache_dir = local_dir / "luts"
        lut_cache_dir.mkdir(parents=True, exist_ok=True)

        artifact_rows: list[dict[str, Any]] = []

        def _process_grid_cell(grid_cell: GridCell) -> dict[str, Any] | None:
            try:
                area_def = grid_cell.area_def(int(resolution))
                area_name = grid_cell.area_name(int(resolution))
                ts = start_time.strftime("%Y%m%dT%H%M%S")
                filename = f"{ts}_{collection}_{dataset_name}_{area_name}.nc"
                output_path = local_dir / filename

                if output_path.exists():
                    logger.info("output_exists", path=str(output_path))
                else:
                    lut_key_str = _lut_key(satellite, scan_type, area_name)

                    with _lut_locks[lut_key_str]:
                        valid_input_index, valid_output_index, index_array, target_shape, template = get_or_build_lut(
                            scene, area_def, area_name, lut_key_str, lut_cache_dir, dataset_name
                        )

                    resampled_data = apply_lookup_table(
                        source_data, valid_input_index, valid_output_index, index_array, target_shape
                    )

                    resampled_da = xr.DataArray(
                        resampled_data.reshape(target_shape),
                        coords=template["coords"],
                        dims=template["dims"],
                        attrs=template["attrs"],
                        name=template["name"],
                    )

                    local_scene = scene.copy()
                    local_scene[dataset_name] = resampled_da
                    local_scene.save_dataset(dataset_id=dataset_name, filename=str(output_path))
                    del resampled_da
                    gc.collect()

                # Build artifact row
                artifact_id = hashlib.md5(f"{granule_id}_{area_name}".encode()).hexdigest()
                return {
                    "id": artifact_id,
                    "source_ids": source_ids,
                    "start_time": start_time,
                    "end_time": end_time,
                    "uri": str(output_path),
                    "geometry": grid_cell.geom,
                    "collection": collection,
                    "grid_cell": grid_cell.id(),
                    "grid_dist": grid_cell.D,
                    "cell_geometry": grid_cell.geom,
                    "cell_utm_crs": str(grid_cell.utm_crs),
                    "cell_utm_fooprint": grid_cell.utm_footprint,
                }
            except Exception as exc:
                logger.error("grid_cell_extract_failed", error=str(exc), grid_cell=grid_cell.id())
                return None

        # Process grid cells in parallel
        max_workers = extract_params.get("max_workers", 8)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process_grid_cell, gc_) for gc_ in grid_cells]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    artifact_rows.append(result)

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {granule_id}")

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        validated = ArtifactSchema.validate(gdf)
        return cast(GeoDataFrame[ArtifactSchema], validated)

    @override
    def extract_batches(
        self,
        extraction_task_batch: Sequence[ExtractionTask],
        extract_params: dict[str, Any] | None = None,
    ) -> GeoDataFrame[ArtifactSchema]:
        """Execute extraction over multiple granule batches in parallel.

        Overrides the default sequential implementation to use ThreadPoolExecutor
        for concurrent granule processing. Each batch downloads and processes
        one granule, so parallelism here reduces total wall-clock time.
        """
        extract_params = extract_params or {}
        max_batch_workers = extract_params.get("max_batch_workers", 4)

        results: list[GeoDataFrame[ArtifactSchema]] = []
        errors: list[str] = []

        with ThreadPoolExecutor(max_workers=max_batch_workers) as executor:
            futures = {
                executor.submit(self.extract, batch, extract_params): i for i, batch in enumerate(extraction_task_batch)
            }
            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    df = future.result()
                    results.append(df)
                except Exception as exc:
                    logger.error("batch_extract_failed", batch=batch_idx, error=str(exc))
                    errors.append(str(exc))

        if not results:
            raise RuntimeError(f"All {len(extraction_task_batch)} batches failed. Errors: {errors}")

        import pandas as pd

        concatenated = pd.concat(results, ignore_index=True)
        validated = ArtifactSchema.validate(concatenated)
        return cast(GeoDataFrame[ArtifactSchema], validated)
