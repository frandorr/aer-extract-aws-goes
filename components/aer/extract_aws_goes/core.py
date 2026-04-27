import hashlib
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed  # noqa: F401
from pathlib import Path
from typing import Any, Sequence, cast, override

import attrs

import rioxarray  # noqa: F401

import geopandas as gpd
import pandas as pd
import s3fs
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
    parse_goes_filename,
    detect_reader,
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
    using odc-geo nearest-neighbour interpolation, and saves as GeoTIFF.
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

        Downloads the granule once, then dispatches to the selected engine.

        Engines (set via ``extract_params["engine"]``):

        * ``"odc_cell"`` (default) — per-cell odc-geo reprojection. Simplest and
          fastest for small AOIs or single-UTM-zone extractions.
        * ``"odc_zone"`` — UTM-grouped odc-geo reprojection.  Highly optimized
          for large AOIs spanning multiple UTM zones by grouping cells to
          minimize reproject calls.
        * ``"pyresample"`` — simple per-cell satpy resample; slowest but
          produces the canonical pyresample output as ground-truth reference.

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Optional parameters for the extraction engine. Supports:
                - ``engine`` (str): The extraction engine to use (default: "odc_cell").
                - ``resampling`` (str): The resampling method to use (default: "nearest").
                - ``padding`` (int): Padding to add to the target area (default: 0).

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        extract_params = extract_params or {}
        engine = extract_params.get("engine", "odc_cell")
        if engine == "pyresample":
            return self._extract_pyresample(extraction_task, extract_params)
        if engine == "odc_zone":
            return self._extract_odc_zone(extraction_task, extract_params)
        return self._extract_odc_cell(extraction_task, extract_params)

    # ── ODC-based extraction ───────────────────────────────

    def _extract_odc_zone(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using odc-geo reprojection with UTM-grouped strategy.

        Performance strategy:
        1. Build ONE global crop of the geostationary source covering all cells.
        2. Group grid cells by UTM CRS (typically 5-25 zones per AOI).
        3. For each UTM zone: reproject the crop once to cover the full zone
           extent, then xarray-slice each individual cell from the result.
        4. Write each cell via rioxarray ``rio.to_raster()``.

        This reduces ``odc.reproject`` calls from N (one per cell) to the
        number of unique UTM zones — typically a 2-3× speedup on large AOIs.

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Dictionary of parameters.

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        import odc.geo.xr  # noqa: F401
        from odc.geo.geom import BoundingBox, bbox_intersection
        from odc.geo.geobox import GeoBox
        from shapely.ops import unary_union
        from shapely.geometry import box
        from satpy.scene import Scene
        from aer.eoids import build_eoids_path

        first_row = extraction_task.assets.iloc[0]
        meta = create_metadata_from_row(first_row, extract_params, extraction_task)

        # Download from S3 (or copy if local)
        meta.local_dir.mkdir(parents=True, exist_ok=True)
        if not meta.local_path.exists():
            if meta.href.startswith("s3://"):
                fs = s3fs.S3FileSystem(anon=True)
                fs.get(meta.href.replace("s3://", ""), str(meta.local_path))
            elif Path(meta.href).exists():
                if Path(meta.href).absolute() != meta.local_path.absolute():
                    import shutil

                    shutil.copy(meta.href, meta.local_path)
            else:
                raise FileNotFoundError(f"Source file not found at {meta.href}")

        logger.info("file_downloaded", local_path=str(meta.local_path))

        nc_path = str(meta.local_path)
        reader = detect_reader(nc_path)
        info = parse_goes_filename(nc_path)
        scn = Scene(filenames=[nc_path], reader=reader)
        available_datasets = scn.available_dataset_names()

        channel_id_list = [info["channel_id"]] if info.get("channel_id") else []
        channel_names = map_channel_ids_to_satpy_names(channel_id_list, available_datasets)
        if not channel_names:
            raise ValueError(f"Could not map channel ID {info.get('channel_id')} to a dataset in {nc_path}")

        channel_name = channel_names[0]

        scn.load([channel_name], calibration=meta.calibration)
        ds = scn[channel_name]
        ds = ds.odc.assign_crs(ds.crs.item())

        grid_cells = extraction_task.overlapping_grid_cells

        # ── Build ONE global crop covering all cells ───────────────────────────
        total = box(*unary_union([g.geom for g in grid_cells]).bounds)
        bbox = BoundingBox(*total.bounds[:4], crs="EPSG:4326")
        target_geobox = GeoBox.from_bbox(bbox, resolution=0.014)
        target_poly_src = target_geobox.extent.to_crs(ds.odc.geobox.crs)
        safe_bbox = bbox_intersection([target_poly_src.boundingbox, ds.odc.geobox.extent.boundingbox])
        crop_ds = ds.odc.crop(safe_bbox.polygon).odc.assign_crs(ds.crs.item()).compute()

        combo = self._detect_combo(meta.href)
        combo_parts = combo.split("_")
        eoids_sat = f"{combo_parts[0]}_{combo_parts[1]}" if len(combo_parts) >= 2 else "unknown"
        eoids_prod = meta.collection.split("-")[-1]

        padding: int = int(extract_params.get("padding", 0))
        resampling: str = extract_params.get("resampling", "nearest")

        # ── Group cells by UTM CRS and process zone by zone ────────────────────
        by_zone: dict[int, list[Any]] = defaultdict(list)
        for gc_ in grid_cells:
            by_zone[gc_.utm_crs].append(gc_)

        artifact_rows = []

        for utm_crs, zone_cells in by_zone.items():
            # Compute the encompassing bbox for all cells in this UTM zone
            cell_bboxes = [
                BoundingBox(*gc_.area_def(meta.resolution, padding=padding).area_extent, crs=f"EPSG:{utm_crs}")
                for gc_ in zone_cells
            ]
            zone_utm_bbox = BoundingBox(
                min(b.left for b in cell_bboxes),
                min(b.bottom for b in cell_bboxes),
                max(b.right for b in cell_bboxes),
                max(b.top for b in cell_bboxes),
                crs=f"EPSG:{utm_crs}",
            )
            zone_utm_geobox = GeoBox.from_bbox(zone_utm_bbox, resolution=meta.resolution)

            # One reproject call covers all cells in this zone
            zone_reproj = crop_ds.odc.reproject(how=zone_utm_geobox, resampling=resampling, resolution=meta.resolution)

            res = meta.resolution

            for gc_, cell_bbox in zip(zone_cells, cell_bboxes):
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

                if output_path.exists():
                    area_name = gc_.area_name(meta.resolution)
                    artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
                    artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
                    artifact_rows.append(attrs.asdict(artifact))
                    continue

                # Slice this cell out of the zone-wide reprojected DataArray
                col_off = round((cell_bbox.left - zone_utm_bbox.left) / res)
                row_off = round((zone_utm_bbox.top - cell_bbox.top) / res)
                nrows = round((cell_bbox.top - cell_bbox.bottom) / res)
                ncols = round((cell_bbox.right - cell_bbox.left) / res)
                cell_da = zone_reproj.isel(
                    y=slice(row_off, row_off + nrows),
                    x=slice(col_off, col_off + ncols),
                )

                output_path.parent.mkdir(parents=True, exist_ok=True)
                cell_da.rio.to_raster(
                    str(output_path),
                    driver="GTiff",
                    compress="deflate",
                    predictor=3,
                    zlevel=1,
                )

                area_name = gc_.area_name(meta.resolution)
                artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
                artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
                artifact_rows.append(attrs.asdict(artifact))

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {meta.granule_id}")

        if meta.local_path.exists():
            meta.local_path.unlink()

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

    # ── ODC-cell extraction (per-cell, no UTM grouping) ───────────────

    def _extract_odc_cell(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using odc-geo reprojection — one reproject per grid cell.

        Unlike :meth:`_extract_odc_zone` this does **not** group cells by UTM zone.
        Each cell gets its own ``odc.reproject`` call from the global crop.
        This approach is typically faster for small AOIs or extractions
        contained within a single UTM zone due to reduced overhead.

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Dictionary of parameters.

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        import odc.geo.xr  # noqa: F401
        from odc.geo.geom import BoundingBox, bbox_intersection
        from odc.geo.geobox import GeoBox
        from shapely.ops import unary_union
        from shapely.geometry import box
        from satpy.scene import Scene
        from aer.eoids import build_eoids_path
        from tqdm import tqdm

        first_row = extraction_task.assets.iloc[0]
        meta = create_metadata_from_row(first_row, extract_params, extraction_task)

        # Download from S3 (or copy if local)
        meta.local_dir.mkdir(parents=True, exist_ok=True)
        if not meta.local_path.exists():
            if meta.href.startswith("s3://"):
                fs = s3fs.S3FileSystem(anon=True)
                fs.get(meta.href.replace("s3://", ""), str(meta.local_path))
            elif Path(meta.href).exists():
                if Path(meta.href).absolute() != meta.local_path.absolute():
                    import shutil

                    shutil.copy(meta.href, meta.local_path)
            else:
                raise FileNotFoundError(f"Source file not found at {meta.href}")

        logger.info("file_downloaded", local_path=str(meta.local_path), engine="odc_naive")

        nc_path = str(meta.local_path)
        reader = detect_reader(nc_path)
        info = parse_goes_filename(nc_path)
        scn = Scene(filenames=[nc_path], reader=reader)
        available_datasets = scn.available_dataset_names()

        channel_id_list = [info["channel_id"]] if info.get("channel_id") else []
        channel_names = map_channel_ids_to_satpy_names(channel_id_list, available_datasets)
        if not channel_names:
            raise ValueError(f"Could not map channel ID {info.get('channel_id')} to a dataset in {nc_path}")

        channel_name = channel_names[0]
        scn.load([channel_name], calibration=meta.calibration)
        ds = scn[channel_name]
        ds = ds.odc.assign_crs(ds.crs.item())

        grid_cells = extraction_task.overlapping_grid_cells

        # Build ONE global crop covering all cells
        total = box(*unary_union([g.geom for g in grid_cells]).bounds)
        bbox = BoundingBox(*total.bounds[:4], crs="EPSG:4326")
        target_geobox = GeoBox.from_bbox(bbox, resolution=0.014)
        target_poly_src = target_geobox.extent.to_crs(ds.odc.geobox.crs)
        safe_bbox = bbox_intersection([target_poly_src.boundingbox, ds.odc.geobox.extent.boundingbox])
        crop_ds = ds.odc.crop(safe_bbox.polygon).odc.assign_crs(ds.crs.item()).compute()
        combo = self._detect_combo(meta.href)
        combo_parts = combo.split("_")
        eoids_sat = f"{combo_parts[0]}_{combo_parts[1]}" if len(combo_parts) >= 2 else "unknown"
        eoids_prod = meta.collection.split("-")[-1]

        artifact_rows = []

        padding: int = int(extract_params.get("padding", 0))
        resampling: str = extract_params.get("resampling", "nearest")

        for gc_ in tqdm(grid_cells):
            area_def_obj = gc_.area_def(meta.resolution, padding=padding)
            utm_crs = gc_.utm_crs
            res = meta.resolution

            # Direct reproject from global crop using area_def extent (matches reference script)
            cell_bbox = BoundingBox(*area_def_obj.area_extent, crs=f"EPSG:{utm_crs}")
            target_cell_geobox = GeoBox.from_bbox(cell_bbox, resolution=res)
            cell_reproj = crop_ds.odc.reproject(how=target_cell_geobox, resampling=resampling)

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

            if output_path.exists():
                area_name = gc_.area_name(meta.resolution)
                artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
                artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
                artifact_rows.append(attrs.asdict(artifact))
                continue

            output_path.parent.mkdir(parents=True, exist_ok=True)
            cell_reproj.rio.to_raster(
                str(output_path),
                driver="GTiff",
                compress="deflate",
                predictor=3,
                zlevel=1,
            )

            area_name = gc_.area_name(meta.resolution)
            artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
            artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
            artifact_rows.append(attrs.asdict(artifact))

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {meta.granule_id}")

        if meta.local_path.exists():
            meta.local_path.unlink()

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

    # ── Pyresample-based extraction (reference / ground-truth) ──────────

    def _extract_pyresample(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using satpy Scene.resample() with pyresample AreaDefinitions.

        This is the *simplest* extraction path: for every grid cell we build a
        pyresample ``AreaDefinition`` from the cell's YAML, call
        ``scn.resample(area_def)``, and write the result to GeoTIFF.

        It is **much slower** than :meth:`_extract_odc` (one resample call per
        cell rather than per UTM zone), but produces the canonical pyresample
        nearest-neighbour output that can be used as a ground-truth reference.

        Args:
            extraction_task: The task containing assets and grid cells to extract.
            extract_params: Dictionary of parameters.

        Returns:
            GeoDataFrame containing references to extracted artifacts.
        """
        from pyresample.area_config import load_area_from_string
        from satpy.scene import Scene
        from aer.eoids import build_eoids_path

        first_row = extraction_task.assets.iloc[0]
        meta = create_metadata_from_row(first_row, extract_params, extraction_task)

        # Download from S3 (or copy if local)
        meta.local_dir.mkdir(parents=True, exist_ok=True)
        if not meta.local_path.exists():
            if meta.href.startswith("s3://"):
                fs = s3fs.S3FileSystem(anon=True)
                fs.get(meta.href.replace("s3://", ""), str(meta.local_path))
            elif Path(meta.href).exists():
                if Path(meta.href).absolute() != meta.local_path.absolute():
                    import shutil

                    shutil.copy(meta.href, meta.local_path)
            else:
                raise FileNotFoundError(f"Source file not found at {meta.href}")

        logger.info("file_downloaded", local_path=str(meta.local_path), engine="pyresample")

        nc_path = str(meta.local_path)
        reader = detect_reader(nc_path)
        info = parse_goes_filename(nc_path)
        scn = Scene(filenames=[nc_path], reader=reader)
        available_datasets = scn.available_dataset_names()

        channel_id_list = [info["channel_id"]] if info.get("channel_id") else []
        channel_names = map_channel_ids_to_satpy_names(channel_id_list, available_datasets)
        if not channel_names:
            raise ValueError(f"Could not map channel ID {info.get('channel_id')} to a dataset in {nc_path}")

        channel_name = channel_names[0]
        scn.load([channel_name], calibration=meta.calibration)

        grid_cells = extraction_task.overlapping_grid_cells

        combo = self._detect_combo(meta.href)
        combo_parts = combo.split("_")
        eoids_sat = f"{combo_parts[0]}_{combo_parts[1]}" if len(combo_parts) >= 2 else "unknown"
        eoids_prod = meta.collection.split("-")[-1]

        artifact_rows = []

        padding: int = int(extract_params.get("padding", 0))
        resampling: str = extract_params.get("resampling", "nearest")

        for gc_ in grid_cells:
            area_def_obj = gc_.area_def(meta.resolution, padding=padding)
            area_yaml = area_def_obj.to_yaml()
            target_area = load_area_from_string(area_yaml, area_def_obj.area_id)

            rsampled_scn = scn.resample(
                target_area,
                unload=False,
                generate=False,
                resampler=resampling,
            )
            cell_da = rsampled_scn[channel_name].compute()

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

            if output_path.exists():
                area_name = gc_.area_name(meta.resolution)
                artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
                artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
                artifact_rows.append(attrs.asdict(artifact))
                continue

            output_path.parent.mkdir(parents=True, exist_ok=True)
            cell_da.rio.to_raster(
                str(output_path),
                driver="GTiff",
                compress="deflate",
                predictor=3,
                zlevel=1,
            )

            area_name = gc_.area_name(meta.resolution)
            artifact_id = hashlib.md5(f"{meta.granule_id}_{area_name}".encode()).hexdigest()
            artifact = create_extraction_artifact(artifact_id, meta, output_path, gc_)
            artifact_rows.append(attrs.asdict(artifact))

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {meta.granule_id}")

        if meta.local_path.exists():
            meta.local_path.unlink()

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

    # ── Helpers ───────────────────────────────

    @override
    def extract_batches(
        self,
        extraction_task_batch: Sequence[ExtractionTask],
        extract_params: dict[str, Any] | None = None,
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract multiple granules, optionally in parallel via ProcessPool.

        When ``extract_params["max_batch_workers"]`` is set, granules are
        processed in parallel using ``ProcessPoolExecutor``.  Each worker
        compresses its source crop into a ``blosc2.NDArray`` immediately
        after reading the NetCDF, keeping the per-process memory footprint
        bounded even when many workers run simultaneously.

        If ``max_batch_workers`` is not set, falls back to sequential
        processing via the base class.
        """
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
