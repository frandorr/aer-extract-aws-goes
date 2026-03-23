import re
from pathlib import Path
from typing import Any

import pandas as pd
import satpy
from structlog import get_logger

from aer.download_api import download
from aer.extract.core import ExtractedResultSchema
from aer.plugin import plugin
from aer.search import SearchResultSchema
from aer.settings.core import ENV_SETTINGS
from pandera.typing.geopandas import GeoDataFrame
from satpy.writers.core.compute import compute_writer_results

logger = get_logger()

L1B_PATTERN = re.compile(r"ABI-L1b-Rad[CF]")
L2_AOD_PATTERN = re.compile(r"ABI-L2-AOD[CF]")
L2_BRF_PATTERN = re.compile(r"ABI-L2-BRF[CF]")

# Confirm that satpy config is correctly set
satpy.config.set(config_path=[ENV_SETTINGS.SATPY_CONFIG_PATH])


def detect_reader(filename: str) -> str | None:
    """Detect the satpy reader based on the GOES filename."""
    if L1B_PATTERN.search(filename):
        return "abi_l1b"
    if L2_BRF_PATTERN.search(filename):
        return "abi_l2_brf_nc"
    if L2_AOD_PATTERN.search(filename):
        return "abi_l2_nc"
    return None


def group_files_by_reader(files: list[Path]) -> dict[str, list[Path]]:
    """Group GOES files by their applicable satpy reader."""
    grouped: dict[str, list[Path]] = {}
    for f in files:
        reader = detect_reader(f.name)
        if reader:
            grouped.setdefault(reader, []).append(f)
        else:
            logger.warning("unknown_goes_file", filename=f.name)
    return grouped


def map_channel_ids_to_satpy_names(
    channel_ids: set[str], available_names: set[str]
) -> list[str]:
    """Map aer Channel c_ids to satpy dataset names.

    The search plugin stores c_id as e.g. "1" (stripped int), while satpy
    uses "C01" for ABI L1b.  This function tries direct match first, then
    zero-padded "C{id}" format.

    Args:
        channel_ids: Set of c_id strings from the GDF channels column.
        available_names: Set from ``scene.available_dataset_names()``.

    Returns:
        List of satpy dataset names that matched.
    """
    matched: list[str] = []
    for c_id in channel_ids:
        if c_id in available_names:
            matched.append(c_id)
        else:
            # Try "C01" format: zero-pad the numeric id
            padded = f"C{c_id.zfill(2)}"
            if padded in available_names:
                matched.append(padded)
    return matched


def harmonize_reflectance(scene: satpy.Scene) -> satpy.Scene:
    """Harmonize reflectance and set calibration units if necessary."""
    # Placeholder for actual harmonization from the reference script.
    return scene


def _collect_unique_grid_cells(valid_gdf: pd.DataFrame) -> list[Any]:
    """Collect unique grid cells across all rows' overlapping_spatial_extent."""
    seen_cells: set[Any] = set()
    all_cells: list[Any] = []
    if "overlapping_spatial_extent" not in valid_gdf.columns:
        return all_cells

    for spatial_ext in valid_gdf["overlapping_spatial_extent"].dropna():
        if hasattr(spatial_ext, "grid_cells"):
            for cell in spatial_ext.grid_cells:
                if cell not in seen_cells:
                    seen_cells.add(cell)
                    all_cells.append(cell)
    return all_cells


def _collect_channel_ids(valid_gdf: pd.DataFrame) -> set[str]:
    """Collect all unique channel c_ids from the channels column."""
    ids: set[str] = set()
    if "channels" not in valid_gdf.columns:
        return ids
    for ch_tuple in valid_gdf["channels"].dropna():
        for ch in ch_tuple:
            if hasattr(ch, "c_id"):
                ids.add(ch.c_id)
    return ids


@plugin(name="aws_goes", category="extract")
def extract_aws_goes(
    search_result: GeoDataFrame[SearchResultSchema],
    dest_dir: Path | str,
    resolution: float = 2000.0,
    **options: Any,
) -> GeoDataFrame[ExtractedResultSchema]:
    """Extract AWS GOES data for the given search results using Satpy.

    Supports per-row ``channels`` and ``overlapping_spatial_extent``.
    All unique grid cells across rows are collected, and all requested
    channel IDs are unioned for loading.

    Args:
        search_result: The search results to download and extract.
        dest_dir: Base directory where local files and extracted output will be saved.
        resolution: Target resolution for extraction (default 2000.0).
        **options: Additional options (e.g., output_format).

    Returns:
        A GeoDataFrame conforming to ExtractedResultSchema.
    """
    dest_path = Path(dest_dir)

    # 1. Download
    downloaded = download(gdf=search_result, dest_dir=dest_path)

    # Filter only successfully downloaded files
    valid = downloaded[downloaded["download_status"] == "complete"].copy()
    if valid.empty:
        logger.warning("no_files_downloaded")
        valid["reprojected_path"] = pd.Series(dtype=str)
        valid["resolution"] = pd.Series(dtype=float)
        return valid  # type: ignore

    local_files = [Path(p) for p in valid["local_path"].dropna()]
    grouped = group_files_by_reader(local_files)

    # 2. Collect all requested band IDs and unique grid cells across ALL rows
    requested_channel_ids = _collect_channel_ids(valid)
    all_grid_cells = _collect_unique_grid_cells(valid)

    # 3. Extract using satpy
    output_rows: list[Any] = []
    to_be_computed: list[Any] = []
    output_format = options.get("output_format", "nc")
    res_int = int(resolution)

    for reader, files in grouped.items():
        try:
            scene = satpy.Scene(filenames=[str(f) for f in files], reader=reader)
            available = set(scene.available_dataset_names())

            # Map channel c_ids → satpy dataset names (handles "1" → "C01")
            if requested_channel_ids:
                bands_to_load = map_channel_ids_to_satpy_names(
                    requested_channel_ids, available
                )
            else:
                bands_to_load = list(available)

            if not bands_to_load:
                logger.warning(
                    "no_matching_bands",
                    reader=reader,
                    requested=requested_channel_ids,
                    available=available,
                )
                continue

            scene.load(bands_to_load)
            scene = harmonize_reflectance(scene)

            if all_grid_cells:
                for grid_cell in all_grid_cells:
                    area_def = grid_cell.area_def(res_int)
                    resampled_scn = scene.resample(
                        destination=area_def,
                        datasets=bands_to_load,
                        resampler="nearest",
                    )

                    cell_id = grid_cell.area_name(res_int)
                    out_file = (
                        dest_path / f"extracted_{reader}_{cell_id}.{output_format}"
                    )

                    if output_format in ("nc", "netcdf"):
                        delayed = resampled_scn.save_datasets(
                            filename=str(out_file), writer="cf", compute=False
                        )
                    else:
                        delayed = resampled_scn.save_datasets(
                            filename=str(out_file), writer="geotiff", compute=False
                        )

                    if delayed:
                        to_be_computed.extend(delayed)

                    rep_row = valid.iloc[0].copy()
                    rep_row["reprojected_path"] = str(out_file)
                    rep_row["resolution"] = float(resolution)
                    output_rows.append(rep_row)

                    del resampled_scn
            else:
                # No spatial extent — save raw scene
                out_file = dest_path / f"extracted_{reader}.{output_format}"
                if output_format in ("nc", "netcdf"):
                    delayed = scene.save_datasets(
                        filename=str(out_file), writer="cf", compute=False
                    )
                else:
                    delayed = scene.save_datasets(
                        filename=str(out_file), writer="geotiff", compute=False
                    )
                if delayed:
                    to_be_computed.extend(delayed)

                rep_row = valid.iloc[0].copy()
                rep_row["reprojected_path"] = str(out_file)
                rep_row["resolution"] = float(resolution)
                output_rows.append(rep_row)

            del scene

        except Exception as exc:
            logger.error("extraction_failed", reader=reader, error=str(exc))

    # Batch-compute all deferred writes
    if to_be_computed:
        compute_writer_results(to_be_computed)

    if not output_rows:
        valid["reprojected_path"] = pd.Series(dtype=str)
        valid["resolution"] = pd.Series(dtype=float)
        return valid  # type: ignore

    result_df = pd.DataFrame(output_rows)
    from geopandas import GeoDataFrame as gpd_GeoDataFrame

    return gpd_GeoDataFrame(result_df, geometry="geometry", crs=search_result.crs)
