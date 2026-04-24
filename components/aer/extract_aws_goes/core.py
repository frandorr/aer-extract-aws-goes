import gc
import hashlib
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Sequence, cast, override

import rioxarray  # noqa: F401

import geopandas as gpd
import numpy as np
import pandas as pd
import s3fs
from aer.grid import GridCell
from aer.interfaces import ExtractionTask, Extractor
from aer.repository import AerLocalSpectralRepository
from aer.schemas import ArtifactSchema, AssetSchema
from pandera.typing.geopandas import GeoDataFrame
from satpy.scene import Scene
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from structlog import get_logger

try:
    from osgeo import gdal, osr
except ImportError:
    gdal = None
    osr = None

from .utils import (
    detect_reader,
    detect_combo,
    map_channel_ids_to_satpy_names,
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
        if extract_params.get("engine") == "satpy":
            return self._extract_satpy(extraction_task, extract_params)
        else:
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
        from aer.extract_aws_goes.lut import get_default_bucket_uri

        lut_dir_str = extract_params.get("lut_dir")
        if lut_dir_str:
            bucket_uri = str(Path(lut_dir_str))
        else:
            bucket_uri = extract_params.get("bucket_uri", get_default_bucket_uri())

        assets = extraction_task.assets
        resolution = extraction_task.resolution
        uri = extraction_task.uri
        grid_cells = extraction_task.overlapping_grid_cells

        first_row = assets.iloc[0]
        href = first_row["href"]
        granule_id = first_row.get("granule_id", Path(href).name)
        channel_id = first_row.get("channel_id")
        collection = first_row["collection"]
        start_time = first_row["start_time"]
        end_time = first_row["end_time"]
        source_ids = ",".join(assets["id"].astype(str).tolist())

        if channel_id is None:
            raise ValueError(f"No channel_id for granule: {granule_id}")

        # Download from S3
        local_dir = Path(uri).absolute()
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / Path(href).name
        if not local_path.exists():
            fs = s3fs.S3FileSystem(anon=True)
            fs.get(href.replace("s3://", ""), str(local_path))
        logger.info("file_downloaded", local_path=str(local_path))

        # Determine variable name based on product
        src_path = self._detect_subdataset(str(local_path), channel_id)
        var_name = src_path.split(":")[-1] if ":" in src_path else "Rad"

        # Calibration
        calibration = extract_params.get("calibration", "counts")
        cal_params = {}
        if calibration not in ("radiance", "counts"):
            cal_params = self._read_abi_calibration_params(src_path)

        dataset_name = f"C{int(channel_id):02d}" if channel_id.isdigit() else channel_id
        artifact_rows: list[dict[str, Any]] = []

        # Group grid cells by UTM zone for LUT loading
        from collections import defaultdict
        from aer.extract_aws_goes.lut import (
            extract_cell_from_lut,
            load_utm_zone_lut,
        )
        from .utils import (
            compute_cell_slice,
            read_goes_crop,
        )

        utm_groups: dict[int, list[GridCell]] = defaultdict(list)
        for gc_ in grid_cells:
            epsg = int(str(gc_.utm_crs).replace("EPSG:", "").replace("epsg:", ""))
            utm_groups[epsg].append(gc_)

        for utm_epsg, group_cells in utm_groups.items():
            try:
                # Detect combo (e.g. goes_east_radf) for auto-download
                combo = self._detect_combo(href)

                # Load UTM zone LUT metadata and arrays from .npz
                # crop_slices are stored in the .npz and loaded into lut
                lut = load_utm_zone_lut(bucket_uri, utm_epsg, int(resolution), combo=combo)
                lut_extent = lut.area_extent
                crop_slices = lut.crop_slices

                if crop_slices is None:
                    raise ValueError(f"No crop_slices found for EPSG:{utm_epsg} {resolution}m in .npz metadata")

                src_row_sl = slice(crop_slices[0], crop_slices[1])
                src_col_sl = slice(crop_slices[2], crop_slices[3])

                # Read only the needed crop from the GOES file via h5py
                apply_scale_offset = calibration != "counts"
                source_crop = read_goes_crop(
                    local_path, src_row_sl, src_col_sl, variable=var_name, apply_scale_offset=apply_scale_offset
                )

                logger.info(
                    "source_crop_loaded",
                    utm_epsg=utm_epsg,
                    crop_shape=source_crop.shape,
                )

                def _extract_one(gc_):
                    try:
                        bounds = gc_.utm_footprint.bounds  # (minx, miny, maxx, maxy)
                        area_def = gc_.area_def(int(resolution))
                        row_sl, col_sl, eff_extent = compute_cell_slice(
                            bounds,
                            lut_extent,
                            int(resolution),
                            target_width=area_def.width,
                            target_height=area_def.height,
                        )

                        cell_data = extract_cell_from_lut(
                            source_crop,
                            lut,
                            row_sl,
                            col_sl,
                        )

                        # Apply calibration
                        if calibration not in ("radiance", "counts") and cal_params:
                            cell_data = self._apply_abi_calibration(cell_data, calibration, cal_params)

                        # Save as GeoTIFF — use the LUT-grid-aligned extent so
                        # pixel values and geospatial coordinates are consistent.
                        area_name = gc_.area_name(int(resolution))

                        combo_parts = AwsGoesExtractor._detect_combo(href).split("_")
                        eoids_sat = f"{combo_parts[0]}_{combo_parts[1]}" if len(combo_parts) >= 2 else "unknown"
                        eoids_prod = collection.split("-")[-1]

                        from aer.eoids import build_eoids_path

                        output_path = build_eoids_path(
                            local_dir=local_dir,
                            cell_id=gc_.id(),
                            start_time=start_time,
                            end_time=end_time,
                            satellite=eoids_sat,
                            product=eoids_prod,
                            band=dataset_name,
                            resolution=int(resolution),
                        )

                        if not output_path.exists():
                            import rasterio
                            from rasterio.crs import CRS as RioCRS
                            from rasterio.transform import from_bounds

                            dst_crs = str(gc_.utm_crs)
                            if dst_crs.isdigit():
                                dst_crs = f"EPSG:{dst_crs}"
                            minx, miny, maxx, maxy = eff_extent
                            cell_w = col_sl.stop - col_sl.start
                            cell_h = row_sl.stop - row_sl.start
                            dst_transform = from_bounds(minx, miny, maxx, maxy, cell_w, cell_h)

                            profile = {
                                "driver": "GTiff",
                                "dtype": "float32",
                                "width": cell_w,
                                "height": cell_h,
                                "count": 1,
                                "crs": RioCRS.from_user_input(dst_crs),
                                "transform": dst_transform,
                                "compress": "deflate",
                                "predictor": 2,
                                "zlevel": 1,
                                "tiled": True,
                                "blockxsize": 512,
                                "blockysize": 512,
                            }
                            with rasterio.open(str(output_path), "w", **profile) as dst:
                                dst.write(cell_data.astype(np.float32), 1)

                        artifact_id = hashlib.md5(f"{granule_id}_{area_name}".encode()).hexdigest()
                        return {
                            "id": artifact_id,
                            "source_ids": source_ids,
                            "start_time": start_time,
                            "end_time": end_time,
                            "uri": str(output_path),
                            "geometry": gc_.geom,
                            "collection": collection,
                            "grid_cell": gc_.id(),
                            "grid_dist": gc_.D,
                            "cell_geometry": gc_.geom,
                            "cell_utm_crs": str(gc_.utm_crs),
                            "cell_utm_footprint": gc_.utm_footprint,
                        }
                    except Exception as exc:
                        logger.error(
                            "cell_extract_failed",
                            error=str(exc),
                            grid_cell=gc_.id(),
                            engine="lut",
                        )
                        return None

                max_workers = extract_params.get("max_workers", 16)
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [pool.submit(_extract_one, gc_) for gc_ in group_cells]
                    for fut in as_completed(futures):
                        res = fut.result()
                        if res:
                            artifact_rows.append(res)

            except Exception as grp_exc:
                logger.error("utm_group_lut_failed", error=str(grp_exc), utm_epsg=utm_epsg)

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {granule_id}")

        Path(local_path).unlink()
        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

    # ── satpy-based extraction (current default) ──────────────────────────

    def _extract_satpy(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using satpy Scene.resample + rioxarray clip_box.

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Dictionary of parameters for Satpy load and resample.

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
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
        calibration = extract_params.get("calibration", "counts")
        scene.load([dataset_name], modifiers=modifiers, calibration=calibration)

        artifact_rows: list[dict[str, Any]] = []

        # Process grid cells by UTM zone
        from collections import defaultdict

        utm_groups: dict[str, list[GridCell]] = defaultdict(list)
        for gc_ in grid_cells:
            utm_groups[str(gc_.utm_crs)].append(gc_)

        for utm_crs, group_cells in utm_groups.items():
            try:
                # 1. Compute bounding box for the entire UTM group
                all_footprints = [gc_.utm_footprint for gc_ in group_cells]
                union_geom = unary_union(all_footprints)
                minx, miny, maxx, maxy = union_geom.bounds

                # 2. Create AreaDefinition for the group
                # Align extent to resolution to avoid sub-pixel shifts
                # Add 2 pixels padding to avoid out-of-bounds indexing when extracting exactly like LUT
                res = float(resolution)
                minx = np.floor(minx / res) * res - res * 2
                miny = np.floor(miny / res) * res - res * 2
                maxx = np.ceil(maxx / res) * res + res * 2
                maxy = np.ceil(maxy / res) * res + res * 2

                width = int((maxx - minx) / res)
                height = int((maxy - miny) / res)
                area_extent = (minx, miny, maxx, maxy)
                area_id = f"group_{utm_crs}_{granule_id}"

                from pyresample.geometry import AreaDefinition

                group_area_def = AreaDefinition(
                    area_id,
                    f"Group area for {utm_crs}",
                    "area_id",
                    utm_crs,
                    width,
                    height,
                    area_extent,
                )

                # 3. Resample scene once for this group
                logger.info(
                    "resampling_group",
                    utm_crs=utm_crs,
                    num_cells=len(group_cells),
                    width=width,
                    height=height,
                )
                resampled_scene = scene.resample(
                    destination=group_area_def,
                    datasets=[dataset_name],
                    resampler="nearest",
                    unload=False,
                )
                group_resampled_da = resampled_scene[dataset_name]
                # Ensure CRS is set for rioxarray
                group_resampled_da.rio.write_crs(utm_crs, inplace=True)

                # 4. Extract each cell from the group resampled data in parallel
                # Threads are ideal here as they share the large resampled array and the work is mainly I/O
                def _extract_cell(gc_: GridCell) -> dict[str, Any] | None:
                    try:
                        area_name = gc_.area_name(int(resolution))

                        combo_parts = AwsGoesExtractor._detect_combo(href).split("_")
                        eoids_sat = f"{combo_parts[0]}_{combo_parts[1]}" if len(combo_parts) >= 2 else "unknown"
                        eoids_prod = collection.split("-")[-1]

                        from aer.eoids import build_eoids_path

                        output_path = build_eoids_path(
                            local_dir=local_dir,
                            cell_id=gc_.id(),
                            start_time=start_time,
                            end_time=end_time,
                            satellite=eoids_sat,
                            product=eoids_prod,
                            band=dataset_name,
                            resolution=int(resolution),
                        )

                        if output_path.exists():
                            logger.info("output_exists", path=str(output_path))
                        else:
                            import rasterio
                            from rasterio.transform import from_bounds
                            from .utils import compute_cell_slice

                            cell_area_def = gc_.area_def(int(resolution))
                            row_sl, col_sl, eff_extent = compute_cell_slice(
                                gc_.utm_footprint.bounds,
                                group_area_def.area_extent,
                                int(resolution),
                                target_width=cell_area_def.width,
                                target_height=cell_area_def.height,
                            )

                            full_arr = group_resampled_da.values
                            if len(full_arr.shape) == 3 and full_arr.shape[0] == 1:
                                full_arr = full_arr[0]
                            cell_data = full_arr[row_sl, col_sl]

                            dst_crs = str(gc_.utm_crs)
                            if dst_crs.isdigit():
                                dst_crs = f"EPSG:{dst_crs}"

                            minx, miny, maxx, maxy = eff_extent
                            cell_w = col_sl.stop - col_sl.start
                            cell_h = row_sl.stop - row_sl.start
                            dst_transform = from_bounds(minx, miny, maxx, maxy, cell_w, cell_h)

                            profile = {
                                "driver": "GTiff",
                                "dtype": "float32",
                                "width": cell_w,
                                "height": cell_h,
                                "count": 1,
                                "crs": rasterio.crs.CRS.from_user_input(dst_crs),
                                "transform": dst_transform,
                                "compress": "deflate",
                                "predictor": 2,
                                "zlevel": 1,
                                "tiled": True,
                                "blockxsize": 512,
                                "blockysize": 512,
                            }
                            with rasterio.open(str(output_path), "w", **profile) as dst:
                                dst.write(cell_data.astype(np.float32), 1)

                        # Build artifact row
                        artifact_id = hashlib.md5(f"{granule_id}_{area_name}".encode()).hexdigest()
                        return {
                            "id": artifact_id,
                            "source_ids": source_ids,
                            "start_time": start_time,
                            "end_time": end_time,
                            "uri": str(output_path),
                            "geometry": gc_.geom,
                            "collection": collection,
                            "grid_cell": gc_.id(),
                            "grid_dist": gc_.D,
                            "cell_geometry": gc_.geom,
                            "cell_utm_crs": str(gc_.utm_crs),
                            "cell_utm_footprint": gc_.utm_footprint,
                        }
                    except Exception as cell_exc:
                        logger.error(
                            "cell_extract_failed",
                            error=str(cell_exc),
                            grid_cell=gc_.id(),
                        )
                        return None

                max_cell_workers = extract_params.get("max_workers", 16)
                with ThreadPoolExecutor(max_workers=max_cell_workers) as cell_pool:
                    cell_futures = [cell_pool.submit(_extract_cell, gc_) for gc_ in group_cells]
                    for cf in as_completed(cell_futures):
                        res = cf.result()
                        if res:
                            artifact_rows.append(res)

                del group_resampled_da
                gc.collect()

            except Exception as grp_exc:
                logger.error("utm_group_extract_failed", error=str(grp_exc), utm_crs=utm_crs)

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {granule_id}")

        # rm downloaded file
        Path(local_path).unlink()

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        validated = ArtifactSchema.validate(gdf)
        return cast(GeoDataFrame[ArtifactSchema], validated)

    # ── Helpers ───────────────────────────────

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

    @staticmethod
    def _detect_combo(href: str) -> str:
        """Helper to detect combo from href using utils."""
        return detect_combo(href)
