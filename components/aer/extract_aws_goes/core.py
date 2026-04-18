import gc
import hashlib
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Sequence, cast, override

import geopandas as gpd
import numpy as np
import pandas as pd
from aer.grid import GridCell
from aer.interfaces import ExtractionTask, Extractor
from aer.repository import AerLocalSpectralRepository
from aer.schemas import ArtifactSchema, AssetSchema
from pandera.typing.geopandas import GeoDataFrame
from pyresample.area_config import load_area_from_string
from satpy.scene import Scene
from shapely.geometry.base import BaseGeometry
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


def _extract_wrapper(args):
    """Top-level wrapper to make the call picklable."""
    instance, batch, extract_params = args
    return instance.extract(batch, extract_params)


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

    def __init__(self, target_grid_d: int = 100_000, target_grid_overlap: bool = False):
        self._target_grid_d = target_grid_d
        self._target_grid_overlap = target_grid_overlap
        self._repository = AerLocalSpectralRepository()
        self._abi_instrument = self._repository.get_instrument("abi")

    @property
    @override
    def target_grid_d(self) -> int:
        return self._target_grid_d

    @property
    @override
    def target_grid_overlap(self) -> bool:
        return self._target_grid_overlap

    def _add_resolution(
        self,
        search_results: GeoDataFrame[AssetSchema],
        resolution: float | None = None,
        prepare_params: dict | None = None,
    ) -> GeoDataFrame[AssetSchema]:
        """
        Add a 'resolution' column to the search results based on the 'channel_id' column.
        If 'channel_id' is missing, use the provided resolution for all rows.
        If prepare_params contains resolution overrides for specific channel_ids, use those as a fallback:
        - Look for keys in prepare_params that match the pattern 'resolution_{channel_id}'.
        - If a resolution override is found for a channel_id, use it for rows with that channel_id where the
            initial resolution lookup returned None.

        Args:
            search_results: GeoDataFrame containing the search results with an optional 'channel_id' column
            resolution: Default resolution to use if 'channel_id' is missing
            prepare_params: Optional dictionary that may contain resolution overrides in the format 'resolution_{channel_id}': value

        Returns:
            GeoDataFrame with an added 'resolution' column based on 'channel_id' lookups and prepare_params overrides

        """
        df = search_results.copy()
        if "channel_id" not in df.columns:
            df["resolution"] = resolution
            return df

        # 1. Normalize channel_id
        channel_ids = pd.to_numeric(df["channel_id"], errors="coerce")
        # 2. Build lookup table (unique values only)
        unique_channels = channel_ids.dropna().astype(int).unique()  # pyright: ignore

        resolution_map = {
            ch: self._repository.get_channel(self._abi_instrument, channel_number=ch).spatial_resolution  # pyright: ignore
            for ch in unique_channels
        }
        # 3. Vectorized mapping
        df["resolution"] = channel_ids.map(resolution_map)  # pyright: ignore
        # 4. Fallback from prepare_params (vectorized)
        if prepare_params:
            fallback_map = {int(k.split("_")[1]): v for k, v in prepare_params.items() if k.startswith("resolution_")}
            fallback_series = channel_ids.map(fallback_map)  # pyright: ignore
            df["resolution"] = df["resolution"].fillna(fallback_series)  # pyright: ignore

        # 5. Final fallback → explicit resolution argument
        if resolution is not None:
            df["resolution"] = df["resolution"].fillna(resolution)
        return df

    @override
    def prepare_for_extraction(
        self,
        search_results: GeoDataFrame[AssetSchema],
        target_aoi: BaseGeometry | None = None,
        resolution: float | None = None,
        uri: str | None = None,
        prepare_params: dict[str, Any] | None = None,
    ) -> Sequence[ExtractionTask]:
        """
        Override the default implementation to set resolution based on GOES product.
        We'll take resolutions from Instruments Repository provided bt aer-core
        """
        if uri is None:
            raise ValueError(
                "Default prepare_for_extraction requires resolution and uri to be defined"
                "If you want to prepare without resolution or uri, you need to override this method with a custom implementation."
            )
        df = self._add_resolution(search_results, resolution, prepare_params)
        tasks = []
        for i in range(len(df)):
            asset_batch = df.iloc[[i]]  # single-row GeoDataFrame
            task = ExtractionTask(
                assets=asset_batch,
                target_grid_d=self.target_grid_d,
                target_grid_overlap=self.target_grid_overlap,
                resolution=asset_batch.resolution.item(),
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

        # This extractor uses one row per granule, so we can take metadata from the 1st row
        first_row = assets.iloc[0]
        href: str = first_row["href"]
        granule_id: str = first_row.get("granule_id", Path(href).name)
        channel_id: str | None = first_row.get("channel_id")
        collection: str = first_row["collection"]
        start_time = first_row["start_time"]
        end_time = first_row["end_time"]
        source_ids = ",".join(assets["id"].astype(str).tolist())

        reader = detect_reader(href)
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
        scene = Scene(reader=reader, filenames=[str(local_path)])
        available = set(scene.available_dataset_names())
        mapped = map_channel_ids_to_satpy_names({channel_id}, available)

        if not mapped:
            raise ValueError(f"Channel {channel_id} not found in available datasets: {available}")

        dataset_name = mapped[0]
        modifiers = extract_params.get("modifiers", "*")
        scene.load([dataset_name], modifiers=modifiers)

        artifact_rows: list[dict[str, Any]] = []

        def _process_grid_cell(grid_cell: GridCell) -> dict[str, Any] | None:
            try:
                area_def_model = grid_cell.area_def(int(resolution))
                area_def = load_area_from_string(area_def_model.to_yaml(), area_def_model.area_id)
                area_name = grid_cell.area_name(int(resolution))
                ts = start_time.strftime("%Y%m%dT%H%M%S")
                filename = f"{ts}_{collection}_{dataset_name}_{area_name}.tif"
                output_path = local_dir / filename

                if output_path.exists():
                    logger.info("output_exists", path=str(output_path))
                else:
                    resampled_da = scene.resample(
                        destination=area_def, datasets=[dataset_name], resampler="nearest", unload=False
                    )
                    # save as NetCDF with xarray to preserve geospatial metadata
                    resampled_da.save_dataset(
                        dataset_name, filename=str(output_path), writer="geotiff", dtype=np.float32, enhance=False
                    )

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

        # rm downloaded file
        Path(local_path).unlink()

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        validated = ArtifactSchema.validate(gdf)
        return cast(GeoDataFrame[ArtifactSchema], validated)

    @override
    def extract_batches(
        self,
        extraction_task_batch: Sequence[ExtractionTask],
        extract_params: dict[str, Any] | None = None,
    ) -> GeoDataFrame[ArtifactSchema]:
        extract_params = extract_params or {}
        max_batch_workers = extract_params.get("max_batch_workers")
        if max_batch_workers is None:
            # run sequential calling super
            return super().extract_batches(extraction_task_batch, extract_params)

        results: list[GeoDataFrame[ArtifactSchema]] = []
        errors: list[str] = []

        # Important: pass explicit args to avoid closure issues
        tasks = [(self, batch, extract_params) for batch in extraction_task_batch]

        with ProcessPoolExecutor(max_workers=max_batch_workers) as executor:
            futures = {executor.submit(_extract_wrapper, t): i for i, t in enumerate(tasks)}

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

        concatenated = pd.concat(results, ignore_index=True)
        validated = ArtifactSchema.validate(concatenated)
        return cast(GeoDataFrame[ArtifactSchema], validated)
