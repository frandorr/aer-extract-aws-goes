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
    _, batch, extract_params = args
    from aer.extract_aws_goes.core import AwsGoesExtractor

    instance = AwsGoesExtractor()
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
                    lut_dir (str): Path to root LUT directory containing Zarr stores.
                Optional:
                    calibration (str): 'radiance', 'reflectance', or 'brightness_temperature'
                        (default: 'counts').
                    max_workers (int): Thread pool workers for parallel cell extraction
                        (default: 16).

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        from aer.extract_aws_goes.lut import get_default_bucket_uri

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
            compute_cell_slice,
            compute_source_crop_slices,
            extract_cell_from_lut,
            load_utm_zone_lut,
            read_goes_crop,
        )

        utm_groups: dict[int, list[GridCell]] = defaultdict(list)
        for gc_ in grid_cells:
            epsg = int(str(gc_.utm_crs).replace("EPSG:", "").replace("epsg:", ""))
            utm_groups[epsg].append(gc_)

        for utm_epsg, group_cells in utm_groups.items():
            try:
                # Detect combo (e.g. goes19_radf) for auto-download
                combo = self._detect_combo(href)

                # Load UTM zone LUT (lazy Zarr group, mapping from bucket)
                lut_info, lut_group = load_utm_zone_lut(
                    bucket_uri, utm_epsg, int(resolution), combo=combo
                )
                lut_extent = lut_info.area_extent
                lut_width = lut_info.width

                # Compute the minimal crop of the GOES source needed for this UTM zone
                valid_input_index = np.asarray(lut_group["valid_input_index"][:])
                source_shape = lut_info.source_shape
                if source_shape is None:
                    raise ValueError(
                        f"LUT for EPSG:{utm_epsg} missing source_shape; "
                        "regenerate with the latest generate_utm_zone_lut"
                    )
                src_row_sl, src_col_sl, row_offsets, col_offsets = compute_source_crop_slices(
                    valid_input_index, source_shape
                )
                # Read only the needed crop from the GOES file via h5py
                apply_scale_offset = calibration != "counts"
                source_crop = read_goes_crop(
                    local_path, src_row_sl, src_col_sl, variable=var_name, apply_scale_offset=apply_scale_offset
                )
                logger.info(
                    "source_crop_loaded",
                    utm_epsg=utm_epsg,
                    crop_shape=source_crop.shape,
                    full_shape=source_shape,
                )

                def _extract_one(gc_):
                    try:
                        bounds = gc_.utm_footprint.bounds  # (minx, miny, maxx, maxy)
                        # Use area_def dimensions as authoritative pixel counts to avoid
                        # fencepost errors from non-aligned UTM footprint bounds.
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
                            row_offsets,
                            col_offsets,
                            lut_group,
                            row_sl,
                            col_sl,
                            lut_width,
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
                            logger.info("cell_extracted", path=str(output_path), engine="lut")

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
                            from aer.extract_aws_goes.lut import compute_cell_slice

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
                            logger.info("cell_extracted", path=str(output_path))

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

    @staticmethod
    def _detect_combo(href: str) -> str:
        """Detect the satellite/product combo from a GOES filename.

        Uses orbital-position-based naming so satellites at the same position
        (and therefore with identical geostationary area definitions) share LUTs:
          - GOES-16 and GOES-19 → ``goes_east``  (75.2 °W)
          - GOES-17 and GOES-18 → ``goes_west``  (137.2 °W)

        Example: ...OR_ABI-L1b-RadF-M6C01_G19... → goes_east_radf
        """
        name = Path(href).name.lower()

        # Map satellite number → orbital position.
        # GOES-16/19 are at the East slot; GOES-17/18 are at the West slot.
        if "g16" in name or "g19" in name:
            sat = "goes_east"
        elif "g17" in name or "g18" in name:
            sat = "goes_west"
        else:
            sat = "unknown"

        # Product/Domain
        if "radf" in name:
            prod = "radf"
        elif "radc" in name:
            prod = "radc"
        elif "radm" in name:
            prod = "radm"
        else:
            prod = "unknown"

        return f"{sat}_{prod}"

    @staticmethod
    def _detect_subdataset(nc_path: str, channel_id: str) -> str:
        """Detect the right GDAL subdataset URI for a GOES NetCDF.

        For L1b products: NETCDF:"file.nc":Rad
        For L2 BRF:       NETCDF:"file.nc":BRF
        For L2 AOD:       NETCDF:"file.nc":AOD
        """
        # Do not use gdal.Open or rasterio.open on the root NetCDF here because
        # it can trigger HDF5 warnings/errors that escalate to segfaults.
        base_name = set(nc_path.split("/")[-1].split("_"))
        if any("L2-AOD" in p for p in base_name):
            return f'NETCDF:"{nc_path}":AOD'
        elif any("L2-BRF" in p for p in base_name):
            return f'NETCDF:"{nc_path}":BRF'
        elif any("L2-SST" in p for p in base_name):
            return f'NETCDF:"{nc_path}":SST'
        elif any("L2-TPW" in p for p in base_name):
            return f'NETCDF:"{nc_path}":TPW'

        # Default for L1b is Rad
        return f'NETCDF:"{nc_path}":Rad'

    @staticmethod
    def _read_abi_calibration_params(nc_path: str) -> dict[str, Any]:
        """Read ABI calibration constants from a GOES NetCDF file.

        Args:
            nc_path: Path to the GOES .nc file.

        Returns:
            Dict with keys needed for VIS reflectance and IR BT conversion:
                - esun: Band solar irradiance (W m^-2 um^-1).
                - esd: Earth-sun distance anomaly (AU).
                - planck_fk1: Planck function constant 1 (IR only).
                - planck_fk2: Planck function constant 2 (IR only).
                - planck_bc1: Planck bias correction 1 (IR only).
                - planck_bc2: Planck bias correction 2 (IR only).
        """
        import xarray as xr

        # Strip the "netcdf:...:Rad" GDAL URI prefix if present
        clean_path = nc_path
        if clean_path.lower().startswith("netcdf:"):
            # Format is typically NETCDF:"/path/to/file.nc":Rad
            parts = clean_path.split(":")
            # If path has no colons inside, parts[1] is the quoted path.
            # actually we can just strip prefix and suffix
            clean_path = clean_path[7:]  # remove NETCDF:
            if ":" in clean_path:
                clean_path = clean_path.rsplit(":", 1)[0]  # remove :Rad
            clean_path = clean_path.strip("\"'")

        ds = xr.open_dataset(clean_path, mask_and_scale=False)
        params: dict[str, Any] = {}
        for key in (
            "esun",
            "earth_sun_distance_anomaly_in_AU",
            "planck_fk1",
            "planck_fk2",
            "planck_bc1",
            "planck_bc2",
        ):
            if key in ds:
                params[key] = float(ds[key].values)
        ds.close()
        return params

    @staticmethod
    def _apply_abi_calibration(
        data: np.ndarray,
        calibration: str,
        cal_params: dict[str, Any],
    ) -> np.ndarray:
        """Apply ABI radiometric calibration to a warped radiance array.

        Args:
            data: Float32 radiance array (already scale+offset applied by GDAL).
            calibration: Type of calibration to apply.
                - 'radiance': No-op, return as-is.
                - 'reflectance': VIS calibration -> TOA reflectance (%).
                - 'brightness_temperature': IR Planck inversion -> BT in Kelvin.
            cal_params: Dict from _read_abi_calibration_params.

        Returns:
            Calibrated array.
        """
        if calibration in ("radiance", "counts"):
            return data

        if calibration == "reflectance":
            esun = cal_params.get("esun")
            esd = cal_params.get("earth_sun_distance_anomaly_in_AU")
            if esun is None or esd is None:
                raise ValueError(
                    "'esun' and 'earth_sun_distance_anomaly_in_AU' must be present in "
                    "the NetCDF for reflectance calibration (VIS channels C01-C06 only)."
                )
            # Satpy formula: refl = (π * esd² / esun) * Rad  →  multiply by 100 for %
            factor = np.float32(np.pi * esd * esd / esun)
            return np.where(np.isnan(data), np.nan, data * factor * 100.0).astype(np.float32)

        if calibration == "brightness_temperature":
            fk1 = cal_params.get("planck_fk1")
            fk2 = cal_params.get("planck_fk2")
            bc1 = cal_params.get("planck_bc1")
            bc2 = cal_params.get("planck_bc2")
            if any(v is None for v in (fk1, fk2, bc1, bc2)):
                raise ValueError(
                    "Planck constants (planck_fk1/fk2/bc1/bc2) must be present in the "
                    "NetCDF for brightness_temperature calibration (IR channels C07-C16 only)."
                )
            # Satpy formula: BT = (fk2 / ln(fk1 / Rad + 1) - bc1) / bc2
            with np.errstate(divide="ignore", invalid="ignore"):
                bt = (fk2 / np.log(np.float32(fk1) / data + 1.0) - np.float32(bc1)) / np.float32(bc2)
            return np.where(np.isnan(data) | (data <= 0), np.nan, bt).astype(np.float32)

        raise ValueError(
            f"Unknown calibration '{calibration}'. Choose from: 'radiance', 'reflectance', 'brightness_temperature'."
        )

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
