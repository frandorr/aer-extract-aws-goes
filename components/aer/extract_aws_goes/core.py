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
                task_context={"prepare_params": prepare_params, "granule_id": str(granule_id)},
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
        """
        extract_params = extract_params or {}
        engine = extract_params.get("engine", "satpy")

        if engine == "gdal":
            return self._extract_gdal(extraction_task, extract_params)
        if engine == "rasterio":
            return self._extract_rasterio(extraction_task, extract_params)
        return self._extract_satpy(extraction_task, extract_params)

    # ── satpy-based extraction (current default) ──────────────────────────

    def _extract_satpy(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using satpy Scene.resample + rioxarray clip_box."""
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
                res = float(resolution)
                minx = np.floor(minx / res) * res
                miny = np.floor(miny / res) * res
                maxx = np.ceil(maxx / res) * res
                maxy = np.ceil(maxy / res) * res

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
                logger.info("resampling_group", utm_crs=utm_crs, num_cells=len(group_cells), width=width, height=height)
                resampled_scene = scene.resample(
                    destination=group_area_def, datasets=[dataset_name], resampler="nearest", unload=False
                )
                group_resampled_da = resampled_scene[dataset_name]
                # Ensure CRS is set for rioxarray
                group_resampled_da.rio.write_crs(utm_crs, inplace=True)

                # 4. Extract each cell from the group resampled data in parallel
                # Threads are ideal here as they share the large resampled array and the work is mainly I/O
                def _extract_cell(gc_: GridCell) -> dict[str, Any] | None:
                    try:
                        area_name = gc_.area_name(int(resolution))
                        ts = start_time.strftime("%Y%m%dT%H%M%S")
                        filename = f"{ts}_{collection}_{dataset_name}_{area_name}.tif"
                        output_path = local_dir / filename

                        if output_path.exists():
                            logger.info("output_exists", path=str(output_path))
                        else:
                            # Use reproject to the cell's area definition to guarantee exact shape (e.g. 20x20)
                            # Since we are in the same CRS and resolution, this is a very fast alignment-correct slice.
                            cell_area_def = gc_.area_def(int(resolution))
                            from rasterio.transform import from_bounds
                            from rasterio.warp import Resampling

                            dst_crs = str(gc_.utm_crs)
                            if dst_crs.isdigit():
                                dst_crs = f"EPSG:{dst_crs}"

                            dst_transform = from_bounds(
                                *cell_area_def.area_extent, cell_area_def.width, cell_area_def.height
                            )
                            cell_da = group_resampled_da.rio.reproject(
                                dst_crs,
                                shape=(cell_area_def.height, cell_area_def.width),
                                transform=dst_transform,
                                resampling=Resampling.nearest,
                            )

                            # Save as GeoTIFF with optimized creation options
                            cell_da.rio.to_raster(
                                str(output_path),
                                dtype=np.float32,
                                compress="DEFLATE",
                                predictor=2,
                                zlevel=1,
                                tiled=True,
                                blockxsize=512,
                                blockysize=512,
                            )
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
                        logger.error("cell_extract_failed", error=str(cell_exc), grid_cell=gc_.id())
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

    # ── rasterio/GDAL warp-based extraction ───────────────────────────────

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

        Returns a dict with keys needed for VIS reflectance and IR BT conversion:
          - ``esun``        : band solar irradiance  (W m⁻² μm⁻¹)
          - ``esd``         : earth-sun distance anomaly (AU)
          - ``planck_fk1``  : Planck function constant 1 (IR only)
          - ``planck_fk2``  : Planck function constant 2 (IR only)
          - ``planck_bc1``  : Planck bias correction 1   (IR only)
          - ``planck_bc2``  : Planck bias correction 2   (IR only)
        """
        import xarray as xr

        # Strip the "netcdf:...:Rad" GDAL URI prefix if present
        clean_path = nc_path
        if clean_path.startswith("netcdf:"):
            clean_path = clean_path.split(":")[1]

        ds = xr.open_dataset(clean_path, mask_and_scale=False)
        params: dict[str, Any] = {}
        for key in ("esun", "earth_sun_distance_anomaly_in_AU", "planck_fk1", "planck_fk2", "planck_bc1", "planck_bc2"):
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

        Parameters
        ----------
        data:
            Float32 radiance array (already scale+offset applied by GDAL).
        calibration:
            ``'radiance'``               – no-op, return as-is.
            ``'reflectance'``            – VIS calibration → TOA reflectance (%)
                                           equivalent to Satpy's default for C01-C06.
            ``'brightness_temperature'`` – IR Planck inversion → BT in Kelvin
                                           equivalent to Satpy's default for C07-C16.
        cal_params:
            Dict from :meth:`_read_abi_calibration_params`.
        """
        if calibration == "radiance":
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

    @staticmethod
    def _get_goes_georef(nc_path: str) -> tuple[str, tuple[float, float, float, float, float, float], int, int]:
        """Read GOES NetCDF georeferencing metadata.

        Returns (crs_wkt, geotransform, width, height) with coordinates scaled to meters.
        """
        import xarray as xr

        clean_path = nc_path
        if clean_path.startswith("netcdf:") or clean_path.startswith("NETCDF:"):
            clean_path = clean_path.split(":")[1].strip('"')
            if ":" in clean_path:
                clean_path = clean_path.split(":")[0]

        ds = xr.open_dataset(clean_path)
        x = ds["x"].values
        y = ds["y"].values
        ds.close()

        h = 35786023.0
        x_m = x * h
        y_m = y * h

        x_res = abs(x_m[1] - x_m[0]) if len(x_m) > 1 else abs(x_m[0])
        y_res = abs(y_m[1] - y_m[0]) if len(y_m) > 1 else abs(y_m[0])

        minx = x_m[0] - x_res / 2
        miny = y_m[-1] - y_res / 2
        maxx = x_m[-1] + x_res / 2
        maxy = y_m[0] + y_res / 2

        gt = (minx, x_res, 0.0, maxy, 0.0, -y_res)
        width = len(x_m)
        height = len(y_m)

        crs_wkt = (
            'PROJCS["GOES ABI Geostationary",'
            'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],'
            'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
            'PROJECTION["Geostationary_Satellite"],'
            'PARAMETER["central_meridian",-75.0],'
            'PARAMETER["satellite_height",35786023.0],'
            'PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],'
            'UNIT["Meter",1.0]]'
        )
        return crs_wkt, gt, width, height

    @staticmethod
    def _warp_cell_gdal(
        src_path: str,
        dst_path: str,
        dst_crs: str,
        bounds: tuple[float, float, float, float],
        width: int,
        height: int,
        calibration: str = "radiance",
        cal_params: dict[str, Any] | None = None,
    ) -> None:
        """Warp a single grid cell using GDAL Warp (osgeo.gdal)."""
        if gdal is None or osr is None:
            raise ImportError("GDAL Python bindings (osgeo) not installed.")

        import rasterio
        from rasterio.crs import CRS as RioCRS

        gdal.UseExceptions()

        # 1. Get source CRS and Transform from NetCDF metadata
        logger.debug("reading_goes_georef", path=src_path)
        src_crs_wkt, src_gt, _, _ = AwsGoesExtractor._get_goes_georef(src_path)

        # 2. Setup GDAL objects
        minx, miny, maxx, maxy = bounds
        if dst_crs.isdigit():
            dst_crs = f"EPSG:{dst_crs}"

        logger.debug("opening_src_gdal", path=src_path)
        src_ds = gdal.Open(src_path, gdal.GA_ReadOnly)
        if src_ds is None:
            raise RuntimeError(f"Could not open {src_path} with GDAL")

        # Create a memory VRT to override projection and geotransform
        vrt_ds = gdal.BuildVRT("", src_ds)
        vrt_ds.SetProjection(src_crs_wkt)
        vrt_ds.SetGeoTransform(src_gt)

        # 3. Perform Warp
        logger.debug("starting_gdal_warp", dst_crs=dst_crs)
        warp_ds = gdal.Warp(
            "",
            vrt_ds,
            format="MEM",
            dstSRS=dst_crs,
            outputBounds=(minx, miny, maxx, maxy),
            width=width,
            height=height,
            resampleAlg=gdal.GRA_NearestNeighbour,
            multithread=False,
        )
        logger.debug("reading_warp_array")
        dst_data = warp_ds.ReadAsArray()

        # 4. Calibration
        if calibration != "radiance" and cal_params:
            dst_data = AwsGoesExtractor._apply_abi_calibration(dst_data, calibration, cal_params)

        # 5. Write final GeoTIFF
        from rasterio.transform import from_bounds

        dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": width,
            "height": height,
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
        with rasterio.open(dst_path, "w", **profile) as dst:
            if dst_data.ndim == 2:
                dst.write(dst_data, 1)
            else:
                dst.write(dst_data)

        # Cleanup GDAL objects
        src_ds = None
        vrt_ds = None
        warp_ds = None

    def _extract_gdal(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using GDAL Warp (osgeo.gdal) — fast multi-threaded warping."""
        assets = extraction_task.assets
        resolution = extraction_task.resolution
        uri = extraction_task.uri
        grid_cells = extraction_task.overlapping_grid_cells

        first_row = assets.iloc[0]
        href: str = first_row["href"]
        granule_id: str = first_row.get("granule_id", Path(href).name)
        channel_id: str | None = first_row.get("channel_id")
        collection: str = first_row["collection"]
        start_time = first_row["start_time"]
        end_time = first_row["end_time"]
        source_ids = ",".join(assets["id"].astype(str).tolist())

        if channel_id is None:
            raise ValueError(f"No channel_id in asset row for granule: {granule_id}")

        # Download file from S3 (only if not cached)
        local_dir = Path(uri).absolute()
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / Path(href).name
        if not local_path.exists():
            fs = s3fs.S3FileSystem(anon=True)
            s3_path = href.replace("s3://", "")
            fs.get(s3_path, str(local_path))
        logger.info("file_downloaded", local_path=str(local_path))

        # Detect subdataset
        src_path = self._detect_subdataset(str(local_path), channel_id)

        # Calibration
        calibration = extract_params.get("calibration", "radiance")
        cal_params: dict[str, Any] = {}
        if calibration != "radiance":
            cal_params = self._read_abi_calibration_params(src_path)

        artifact_rows: list[dict[str, Any]] = []
        dataset_name = f"C{int(channel_id):02d}" if channel_id.isdigit() else channel_id

        def _warp_one(gc_: GridCell) -> dict[str, Any] | None:
            try:
                area_def = gc_.area_def(int(resolution))
                area_name = gc_.area_name(int(resolution))
                ts = start_time.strftime("%Y%m%dT%H%M%S")
                cal_suffix = "" if calibration == "radiance" else f"_{calibration[:3]}"
                filename = f"{ts}_{collection}_{dataset_name}_{area_name}{cal_suffix}.tif"
                output_path = local_dir / filename

                if not output_path.exists():
                    self._warp_cell_gdal(
                        src_path=src_path,
                        dst_path=str(output_path),
                        dst_crs=area_def.projection,
                        bounds=area_def.area_extent,
                        width=area_def.width,
                        height=area_def.height,
                        calibration=calibration,
                        cal_params=cal_params,
                    )
                    logger.info("cell_extracted", path=str(output_path), engine="gdal")

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
                logger.error("cell_extract_failed", error=str(exc), grid_cell=gc_.id(), engine="gdal")
                return None

        max_workers = extract_params.get("max_workers", 8)
        if max_workers == 1:
            for gc_ in grid_cells:
                res = _warp_one(gc_)
                if res:
                    artifact_rows.append(res)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(_warp_one, gc_) for gc_ in grid_cells]
                for fut in as_completed(futures):
                    res = fut.result()
                    if res:
                        artifact_rows.append(res)

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {granule_id}")

        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

    @staticmethod
    def _warp_cell(
        src_path: str,
        dst_path: str,
        dst_crs: str,
        bounds: tuple[float, float, float, float],
        width: int,
        height: int,
        calibration: str = "radiance",
        cal_params: dict[str, Any] | None = None,
    ) -> None:
        """Warp a single grid cell from GOES geostationary to UTM using rasterio."""
        import rasterio
        from rasterio.crs import CRS as RioCRS
        from rasterio.transform import from_bounds
        from rasterio.warp import reproject, Resampling

        if dst_crs.isdigit():
            dst_crs = f"EPSG:{dst_crs}"

        dst_crs_obj = RioCRS.from_user_input(dst_crs)
        minx, miny, maxx, maxy = bounds
        dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)

        # Read georeferencing from NetCDF metadata
        src_crs_wkt, src_gt, src_width, src_height = AwsGoesExtractor._get_goes_georef(src_path)
        src_crs = RioCRS.from_wkt(src_crs_wkt)
        src_transform = rasterio.Affine(*src_gt[:6])

        # Open the raw NetCDF (not the subdataset URI) to read the data array
        clean_path = src_path
        if clean_path.startswith("netcdf:") or clean_path.startswith("NETCDF:"):
            clean_path = clean_path.split(":")[1].strip('"')
            if ":" in clean_path:
                clean_path = clean_path.split(":")[0]

        with rasterio.open(clean_path) as src:
            dst_data = np.empty((1, height, width), dtype=np.float32)

            reproject(
                source=rasterio.band(src, 1),
                destination=dst_data[0],
                src_transform=src_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs_obj,
                resampling=Resampling.nearest,
                num_threads=2,
            )

        # Optional post-warp calibration (VIS reflectance or IR BT)
        if calibration != "radiance" and cal_params:
            dst_data[0] = AwsGoesExtractor._apply_abi_calibration(dst_data[0], calibration, cal_params)

        # Write output GeoTIFF
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": width,
            "height": height,
            "count": 1,
            "crs": dst_crs_obj,
            "transform": dst_transform,
            "compress": "deflate",
            "predictor": 2,
            "zlevel": 1,
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
        }
        with rasterio.open(dst_path, "w", **profile) as dst:
            dst.write(dst_data)

    def _extract_rasterio(
        self,
        extraction_task: ExtractionTask,
        extract_params: dict[str, Any],
    ) -> GeoDataFrame[ArtifactSchema]:
        """Extract using rasterio.warp.reproject (GDAL warp) — one warp per cell."""
        assets = extraction_task.assets
        resolution = extraction_task.resolution
        uri = extraction_task.uri
        grid_cells = extraction_task.overlapping_grid_cells

        first_row = assets.iloc[0]
        href: str = first_row["href"]
        granule_id: str = first_row.get("granule_id", Path(href).name)
        channel_id: str | None = first_row.get("channel_id")
        collection: str = first_row["collection"]
        start_time = first_row["start_time"]
        end_time = first_row["end_time"]
        source_ids = ",".join(assets["id"].astype(str).tolist())

        if channel_id is None:
            raise ValueError(f"No channel_id in asset row for granule: {granule_id}")

        # Download file from S3 (only if not cached)
        local_dir = Path(uri).absolute()
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / Path(href).name
        if not local_path.exists():
            fs = s3fs.S3FileSystem(anon=True)
            s3_path = href.replace("s3://", "")
            fs.get(s3_path, str(local_path))
        logger.info("file_downloaded", local_path=str(local_path))

        # Detect subdataset
        src_path = self._detect_subdataset(str(local_path), channel_id)

        # Calibration
        calibration = extract_params.get("calibration", "radiance")
        cal_params: dict[str, Any] = {}
        if calibration != "radiance":
            cal_params = self._read_abi_calibration_params(src_path)

        artifact_rows: list[dict[str, Any]] = []
        dataset_name = f"C{int(channel_id):02d}" if channel_id.isdigit() else channel_id

        def _warp_one(gc_: GridCell) -> dict[str, Any] | None:
            try:
                area_def = gc_.area_def(int(resolution))
                area_name = gc_.area_name(int(resolution))
                ts = start_time.strftime("%Y%m%dT%H%M%S")
                # Include calibration in filename so different calibrations don't clash
                cal_suffix = "" if calibration == "radiance" else f"_{calibration[:3]}"
                filename = f"{ts}_{collection}_{dataset_name}_{area_name}{cal_suffix}.tif"
                output_path = local_dir / filename

                if not output_path.exists():
                    self._warp_cell(
                        src_path=src_path,
                        dst_path=str(output_path),
                        dst_crs=area_def.projection,
                        bounds=area_def.area_extent,
                        width=area_def.width,
                        height=area_def.height,
                        calibration=calibration,
                        cal_params=cal_params,
                    )
                    logger.info("cell_extracted", path=str(output_path), engine="rasterio")

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
                logger.error("cell_extract_failed", error=str(exc), grid_cell=gc_.id(), engine="rasterio")
                return None

        max_workers = extract_params.get("max_workers", 8)
        if max_workers == 1:
            for gc_ in grid_cells:
                res = _warp_one(gc_)
                if res:
                    artifact_rows.append(res)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(_warp_one, gc_) for gc_ in grid_cells]
                for fut in as_completed(futures):
                    res = fut.result()
                    if res:
                        artifact_rows.append(res)

        if not artifact_rows:
            raise ValueError(f"All grid cells failed for granule: {granule_id}")

        Path(local_path).unlink()
        gdf = gpd.GeoDataFrame(artifact_rows, geometry="geometry")
        return cast(GeoDataFrame[ArtifactSchema], ArtifactSchema.validate(gdf))

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
