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


def harmonize_reflectance(scene: satpy.Scene) -> satpy.Scene:
    """Harmonize reflectance and set calibration units if necessary."""
    # Placeholder for actual harmonization from the reference script.
    return scene


@plugin(name="aws_goes", category="extract")
def extract_aws_goes(
    search_result: GeoDataFrame[SearchResultSchema],
    dest_dir: Path | str,
    resolution: float = 2000.0,
    **options: Any,
) -> GeoDataFrame[ExtractedResultSchema]:
    """Extract AWS GOES data for the given search results using Satpy.

    Args:
        search_result: The search results to download and extract.
        dest_dir: Base directory where local files and extracted output will be saved.
        resolution: Target resolution for extraction (default 2000.0).
        **options: Additional options matching the ExtractPlugin protocol
                   (e.g., output_format, bands).

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

    # 2. Extract using satpy
    output_rows: list[Any] = []
    to_be_computed: list[Any] = []
    output_format = options.get("output_format", "nc")
    # Allow caller to restrict which bands to load (default: all available)
    requested_bands: set[str] | None = options.get("bands", None)

    for reader, files in grouped.items():
        try:
            scene = satpy.Scene(filenames=[str(f) for f in files], reader=reader)
            available = set(scene.available_dataset_names())

            # Only load requested bands that are actually available
            if requested_bands:
                bands_to_load = list(available & requested_bands)
            else:
                bands_to_load = list(available)

            if not bands_to_load:
                logger.warning(
                    "no_matching_bands",
                    reader=reader,
                    requested=requested_bands,
                    available=available,
                )
                continue

            scene.load(bands_to_load)
            scene = harmonize_reflectance(scene)

            # Get grid cells from the spatial extent
            spatial_ext = valid.iloc[0].get("overlapping_spatial_extent")
            grid_cells = (
                spatial_ext.grid_cells
                if spatial_ext is not None and hasattr(spatial_ext, "grid_cells")
                else []
            )

            if grid_cells:
                res_int = int(resolution)
                for grid_cell in grid_cells:
                    area_def = grid_cell.area_def(res_int)
                    resampled_scn = scene.resample(
                        destination=area_def,
                        datasets=bands_to_load,
                        resampler="nearest",
                    )

                    cell_id = grid_cell.area_name(res_int)
                    out_file = dest_path / f"extracted_{reader}_{cell_id}.{output_format}"

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

                    # Free resampled scene to avoid OOM
                    del resampled_scn
            else:
                # No spatial extent provided — save raw scene
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

            # Free original scene after processing all grid cells for this reader
            del scene

        except Exception as exc:
            logger.error("extraction_failed", reader=reader, error=str(exc))

    # Batch-compute all deferred writes at once (memory-efficient)
    if to_be_computed:
        compute_writer_results(to_be_computed)

    if not output_rows:
        valid["reprojected_path"] = pd.Series(dtype=str)
        valid["resolution"] = pd.Series(dtype=float)
        return valid  # type: ignore

    result_df = pd.DataFrame(output_rows)
    from geopandas import GeoDataFrame as gpd_GeoDataFrame

    return gpd_GeoDataFrame(result_df, geometry="geometry", crs=search_result.crs)
