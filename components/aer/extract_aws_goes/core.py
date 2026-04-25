import hashlib
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Sequence, cast, override

import attrs

import rioxarray  # noqa: F401

import geopandas as gpd
import pandas as pd
import s3fs
from aer.grid import GridCell
from aer.interfaces import ExtractionTask, Extractor
from aer.repository import AerLocalSpectralRepository
from aer.schemas import ArtifactSchema, AssetSchema
from pandera.typing.geopandas import GeoDataFrame
from shapely.geometry.base import BaseGeometry
from structlog import get_logger


try:
    from osgeo import gdal, osr
except ImportError:
    gdal = None
    osr = None

from .utils import (
    create_extraction_artifact,
    create_metadata_from_row,
    detect_combo,
    read_goes_crop,
)


logger = get_logger()


def _extract_wrapper(args):
    """Top-level wrapper to make the call picklable."""
    _, batch, extract_params = args
    from aer.extract_aws_goes.core import AwsGoesExtractor

    instance = AwsGoesExtractor()
    return instance.extract(batch, extract_params)


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

    @property
    def repository(self) -> AerLocalSpectralRepository:
        if not hasattr(self, "_repository_inst"):
            self._repository_inst = AerLocalSpectralRepository()
        return self._repository_inst

    @property
    def abi_instrument(self) -> Any:
        if not hasattr(self, "_abi_instrument_inst"):
            self._abi_instrument_inst = self.repository.get_instrument("abi")
        return self._abi_instrument_inst

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
        """Add a 'resolution' column to the search results based on the 'channel_id' column.

        If 'channel_id' is missing, use the provided resolution for all rows.
        If prepare_params contains resolution overrides for specific channel_ids, use those as a fallback:
        - Look for keys in prepare_params that match the pattern 'resolution_{channel_id}'.
        - If a resolution override is found for a channel_id, use it for rows with that channel_id where the
            initial resolution lookup returned None.

        Args:
            search_results: GeoDataFrame containing the search results with an optional 'channel_id' column.
            resolution: Default resolution to use if 'channel_id' is missing.
            prepare_params: Optional dictionary that may contain resolution overrides in the format
                'resolution_{channel_id}': value.

        Returns:
            GeoDataFrame with an added 'resolution' column based on 'channel_id' lookups and
            prepare_params overrides.
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
            ch: self.repository.get_channel(self.abi_instrument, channel_number=ch).spatial_resolution  # pyright: ignore
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
        """Override the default implementation to set resolution based on GOES product.

        We'll take resolutions from Instruments Repository provided bt aer-core.

        Args:
            search_results: GeoDataFrame containing the search results.
            target_aoi: Optional AOI to filter/clip to.
            resolution: Fixed resolution to use if not derived from assets.
            uri: Target URI for extraction artifacts.
            prepare_params: Optional parameters for task preparation.

        Returns:
            Sequence of ExtractionTask objects.
        """
        if uri is None:
            raise ValueError(
                "Default prepare_for_extraction requires resolution and uri to be defined"
                "If you want to prepare without resolution or uri, you need to override this method with a custom implementation."
            )
        df = self._add_resolution(search_results, resolution, prepare_params)

        # Group by granule_id
        # If granule_id is missing, use filename from href
        if "granule_id" not in df.columns:
            df["granule_id"] = df["href"].apply(lambda x: Path(x).name)

        tasks = []
        for granule_id, group in df.groupby("granule_id"):
            # For GOES, all assets in a granule usually have the same resolution
            # (unless it's a multi-resolution product, but we use the fixed resolution from ExtractionTask)
            # Actually, ExtractionTask needs a single resolution.
            # We'll take the resolution from the first asset in the group.
            group_res = group["resolution"].iloc[0]

            task = ExtractionTask(
                assets=group,
                target_grid_d=self.target_grid_d,
                target_grid_overlap=self.target_grid_overlap,
                resolution=group_res,
                uri=uri,
                aoi=target_aoi,
                task_context={
                    "prepare_params": prepare_params,
                    "granule_id": str(granule_id),
                },
            )
            tasks.append(task)
        return tasks

    def extract(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any] | None = None,
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract GOES data for a batch of assets sharing the same granule.

        Downloads the granule once, then dispatches to either the satpy-based
        or rasterio/GDAL-based extraction engine.

        Set ``extract_params["engine"] = "gdal"`` for the fastest performance.
        Other engines: ``"rasterio"``, ``"satpy"`` (default).

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Optional parameters for the extraction engine.

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        extract_params = extract_params or {}
        return self._extract_lut(extraction_task, extract_params)

    # ── LUT-based extraction ───────────────────────────────

    def _extract_lut(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using pre-computed UTM zone lookup tables — zero reprojection.

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Dictionary of parameters.
                Required:
                    lut_dir (str): Path to root LUT directory containing .npz files.
                Optional:
                    calibration (str): 'radiance', 'reflectance', or 'brightness_temperature'
                        (default: 'counts').
                    max_workers (int): Thread pool workers for parallel cell extraction
                        (default: 16).

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        from aer.extract_aws_goes.lut import (
            extract_cell_from_lut,
            get_default_bucket_uri,
            load_utm_zone_lut,
        )

        from aer.extract_aws_goes.utils import download_lut_if_needed

        lut_dir_str = extract_params.get("lut_dir", "/tmp/luts")
        local_lut_dir = Path(lut_dir_str)
        bucket_uri = extract_params.get("bucket_uri", get_default_bucket_uri())

        first_row = extraction_task.assets.iloc[0]
        meta = create_metadata_from_row(first_row, extract_params, extraction_task)

        # Download from S3 (or copy if local)
        meta.local_dir.mkdir(parents=True, exist_ok=True)
        if not meta.local_path.exists():
            if meta.href.startswith("s3://"):
                fs = s3fs.S3FileSystem(anon=True)
                fs.get(meta.href.replace("s3://", ""), str(meta.local_path))
            elif Path(meta.href).exists():
                # If it's a local path and different from target local_path, copy it
                if Path(meta.href).absolute() != meta.local_path.absolute():
                    import shutil

                    shutil.copy(meta.href, meta.local_path)
            else:
                raise FileNotFoundError(f"Source file not found at {meta.href}")

        logger.info("file_downloaded", local_path=str(meta.local_path))

        artifact_rows: list[dict[str, Any]] = []

        # Group grid cells by UTM zone for LUT loading
        from collections import defaultdict

        utm_groups: dict[int, list[GridCell]] = defaultdict(list)
        for gc_ in extraction_task.overlapping_grid_cells:
            epsg = int(str(gc_.utm_crs).replace("EPSG:", "").replace("epsg:", ""))
            utm_groups[epsg].append(gc_)

        def _extract_utm_group(utm_epsg, group_cells):
            try:
                combo = detect_combo(meta.href)

                download_lut_if_needed(
                    combo=combo,
                    utm_epsg=utm_epsg,
                    resolution=meta.resolution,
                    local_dir=local_lut_dir,
                    remote_bucket=bucket_uri,
                )

                lut = load_utm_zone_lut(local_lut_dir, utm_epsg, meta.resolution, combo=combo)

                # Read only the needed crop from the GOES file via Satpy (in read_goes_crop)
                source_crop = read_goes_crop(str(meta.local_path), lut.crop_slices, calibration=meta.calibration)

                logger.info(
                    "source_crop_loaded",
                    utm_epsg=utm_epsg,
                    crop_shape=source_crop.shape,
                    calibration=meta.calibration,
                )

                group_results = []
                for gc_ in group_cells:
                    try:
                        # Extract cell using the LUT
                        cell_data = extract_cell_from_lut(source_crop, gc_, lut)

                        # Save as GeoTIFF
                        area_name = gc_.area_name(meta.resolution)
                        combo_parts = combo.split("_")
                        eoids_sat = f"{combo_parts[0]}_{combo_parts[1]}" if len(combo_parts) >= 2 else "unknown"
                        eoids_prod = meta.collection.split("-")[-1]

                        from aer.eoids import build_eoids_path

                        output_path = build_eoids_path(
                            local_dir=meta.local_dir,
                            cell_id=gc_.id(),
                            start_time=meta.start_time,
                            end_time=meta.end_time,
                            satellite=eoids_sat,
                            product=eoids_prod,
                            band=meta.dataset_name,
                            resolution=meta.resolution,
                        )

                        if not output_path.exists():
                            # Use rioxarray to save the DataArray to GeoTIFF
                            cell_data.rio.to_raster(
                                str(output_path),
                                driver="GTiff",
                                compress="deflate",
                                predictor=2,
                                zlevel=1,
                                tiled=True,
                                blockxsize=512,
                                blockysize=512,
                            )

                        artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
                        artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
                        group_results.append(attrs.asdict(artifact))
                    except Exception as exc:
                        logger.error(
                            "cell_extract_failed",
                            error=str(exc),
                            grid_cell=gc_.id(),
                            engine="lut",
                        )
                return group_results
            except Exception as grp_exc:
                logger.error("utm_group_lut_failed", error=str(grp_exc), utm_epsg=utm_epsg)
                return []

        # Parallelize by UTM zone (usually 1-3 per granule)
        max_workers = extract_params.get("max_workers", 4)
        print("Max workers: ", max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_extract_utm_group, epsg, cells) for epsg, cells in utm_groups.items()]
            for fut in as_completed(futures):
                artifact_rows.extend(fut.result())

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {meta.granule_id}")

        if meta.local_path.exists():
            meta.local_path.unlink()

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

    # ── Helpers ───────────────────────────────

    # @override
    # def extract_batches(
    #     self,
    #     extraction_task_batch: Sequence[ExtractionTask],
    #     extract_params: dict[str, Any] | None = None,
    # ) -> GeoDataFrame[ArtifactSchema]:
    #     extract_params = extract_params or {}
    #     max_batch_workers = extract_params.get("max_batch_workers")
    #     if max_batch_workers is None:
    #         # run sequential calling super
    #         return super().extract_batches(extraction_task_batch, extract_params)

    #     results: list[GeoDataFrame[ArtifactSchema]] = []
    #     errors: list[str] = []

    #     # Important: pass explicit args to avoid closure issues
    #     tasks = [(self, batch, extract_params) for batch in extraction_task_batch]

    #     with ProcessPoolExecutor(max_workers=max_batch_workers) as executor:
    #         futures = {executor.submit(_extract_wrapper, t): i for i, t in enumerate(tasks)}

    #         for future in as_completed(futures):
    #             batch_idx = futures[future]
    #             try:
    #                 df = future.result()
    #                 results.append(df)
    #             except Exception as exc:
    #                 logger.error("batch_extract_failed", batch=batch_idx, error=str(exc))
    #                 errors.append(str(exc))

    #     if not results:
    #         raise RuntimeError(f"All {len(extraction_task_batch)} batches failed. Errors: {errors}")

    #     concatenated = pd.concat(results, ignore_index=True)
    #     validated = ArtifactSchema.validate(concatenated)
    #     return cast(GeoDataFrame[ArtifactSchema], validated)

    @staticmethod
    def _detect_combo(href: str) -> str:
        """Helper to detect combo from href using utils."""
        return detect_combo(href)
