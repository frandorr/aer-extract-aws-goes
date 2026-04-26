import re
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast, Sequence

import cattrs
from attrs import field, frozen, validators

import numpy as np
import pyproj
from pyresample.geometry import AreaDefinition
from satpy.scene import Scene
from shapely.geometry.base import BaseGeometry
import xarray as xr

L1B_PATTERN = re.compile(r"ABI-L1b-Rad[CF]")
L2_AOD_PATTERN = re.compile(r"ABI-L2-AOD[CF]")
L2_BRF_PATTERN = re.compile(r"ABI-L2-BRF[CF]")

KNOWN_GOES_SHAPES: dict[int, tuple[int, int]] = {
    # Full Disk
    21696 * 21696: (21696, 21696),  # 0.5 km (C01, C03)
    10848 * 10848: (10848, 10848),  # 1 km   (C02)
    5424 * 5424: (5424, 5424),  # 2 km   (all others)
    # CONUS
    3000 * 5000: (3000, 5000),  # 0.5 km
    1500 * 2500: (1500, 2500),  # 1 km
    750 * 1250: (750, 1250),  # 2 km
}


def validate_channel_id(instance, attribute, value):
    if not value or value == "None":
        raise ValueError("channel_id is required and cannot be empty")


@frozen
class ExtractionArtifact:
    id: str = field(validator=validators.instance_of(str))
    source_ids: str
    start_time: datetime
    end_time: datetime
    uri: str = field(validator=validators.instance_of(str))
    geometry: BaseGeometry
    collection: str
    grid_cell: str
    grid_dist: float
    cell_geometry: BaseGeometry
    cell_utm_crs: str
    cell_utm_footprint: BaseGeometry


@frozen
class GoesExtractionMetadata:
    granule_id: str
    channel_id: str = field(validator=validate_channel_id)
    collection: str
    start_time: datetime
    end_time: datetime
    source_ids: str
    href: str
    resolution: int
    local_path: Path = field(converter=Path)
    local_dir: Path = field(converter=Path)
    calibration: str = field(default="counts")

    @property
    def dataset_name(self) -> str:
        """Derive satpy dataset name (e.g. 'C01') from channel_id."""
        if self.channel_id.isdigit():
            return f"C{int(self.channel_id):02d}"
        return self.channel_id


# Create a converter instance for handling the structure
converter = cattrs.Converter()
converter.register_structure_hook(datetime, lambda v, _: v if isinstance(v, datetime) else datetime.fromisoformat(v))
converter.register_structure_hook(BaseGeometry, lambda v, _: v)


def create_extraction_artifact(
    artifact_id: str,
    meta: GoesExtractionMetadata,
    output_path: Path,
    gc_: Any,
) -> ExtractionArtifact:
    """Helper to create an ExtractionArtifact from metadata and a grid cell."""
    data = {
        "id": artifact_id,
        "source_ids": meta.source_ids,
        "start_time": meta.start_time,
        "end_time": meta.end_time,
        "uri": str(output_path),
        "geometry": gc_.geom,
        "collection": meta.collection,
        "grid_cell": gc_.id(),
        "grid_dist": float(gc_.D),
        "cell_geometry": gc_.geom,
        "cell_utm_crs": str(gc_.utm_crs),
        "cell_utm_footprint": gc_.utm_footprint,
    }
    return converter.structure(data, ExtractionArtifact)


def create_metadata_from_row(row: Any, extra_params: dict, extraction_task: Any) -> GoesExtractionMetadata:
    """Prepare GoesExtractionMetadata from a task asset row and parameters."""
    data = {
        "granule_id": row.get("granule_id", Path(row["href"]).name),
        "channel_id": str(row.get("channel_id")),
        "collection": row["collection"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "source_ids": ",".join(extraction_task.assets["id"].astype(str).tolist()),
        "href": row["href"],
        "resolution": int(extraction_task.resolution),
        "local_path": Path(extraction_task.uri).absolute() / Path(row["href"]).name,
        "local_dir": Path(extraction_task.uri).absolute(),
        "calibration": extra_params.get("calibration", "counts"),
    }

    return converter.structure(data, GoesExtractionMetadata)




def detect_reader(filename: str) -> str | None:
    """Detect the satpy reader based on the GOES filename."""
    if L1B_PATTERN.search(filename):
        return "abi_l1b"
    if L2_BRF_PATTERN.search(filename):
        return "abi_l2_brf_nc"
    if L2_AOD_PATTERN.search(filename):
        return "abi_l2_nc"
    return None


def detect_combo(href: str) -> str:
    """Detect the satellite/domain combo from a GOES filename.

    Uses orbital-position-based naming so satellites at the same position
    (and therefore with identical geostationary area definitions) share LUTs:
      - GOES-16 and GOES-19 → ``goes_east``  (75.2 °W)
      - GOES-17 and GOES-18 → ``goes_west``  (137.2 °W)

    Example: ...OR_ABI-L1b-RadF-M6C01_G19... → goes_east_f
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
        domain = "f"
    elif "radc" in name:
        domain = "c"
    elif "radm" in name:
        domain = "m"
    else:
        domain = "unknown"

    return f"{sat}_{domain}"


def map_channel_ids_to_satpy_names(channel_ids: Sequence[str], available_names: Sequence[str]) -> Sequence[str]:
    """Map channel IDs to satpy dataset names.

    Handles direct matches ('C01' in available) and numeric IDs
    ('1' -> 'C01', '13' -> 'C13').

    Args:
        channel_ids: Sequence of channel IDs to map.
        available_names: Sequence of available dataset names in the satpy scene.

    Returns:
        List of satpy dataset names corresponding to the channel IDs.
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


def infer_source_shape(n_pixels: int) -> tuple[int, int]:
    """Infer the 2D GOES source shape from the flat pixel count.

    Falls back to assuming a square grid if the shape is not in KNOWN_GOES_SHAPES.
    """
    if n_pixels in KNOWN_GOES_SHAPES:
        return KNOWN_GOES_SHAPES[n_pixels]
    side = int(math.isqrt(n_pixels))
    if side * side == n_pixels:
        return (side, side)
    raise ValueError(
        f"Cannot infer source shape from {n_pixels} pixels. Known shapes: {list(KNOWN_GOES_SHAPES.values())}"
    )


def parse_goes_filename(filename: str) -> dict[str, Any]:
    """Parse start/end times and band channel ID from a GOES-R filename.

    Example: OR_ABI-L1b-RadF-M6C01_G16_s202312312345678_e202312312354567_c202312312355432.nc

    The channel ID is extracted from the ``C##`` portion (e.g. ``"1"`` from
    ``C01``, ``"13"`` from ``C13``).
    """
    match = re.search(r"_s(\d{13})\d*_e(\d{13})\d*_c(\d{13})\d*\.nc", filename)
    if not match:
        return {}

    start_str = match.group(1)
    end_str = match.group(2)

    try:
        start_time = datetime.strptime(start_str, "%Y%j%H%M%S").replace(tzinfo=timezone.utc)
        end_time = datetime.strptime(end_str, "%Y%j%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return {}

    # Extract band/channel ID from the filename (e.g. C01 → "1", C13 → "13")
    band_match = re.search(r"-M\d+C(\d+)", filename)
    channel_id = str(int(band_match.group(1))) if band_match else None

    # Extract satellite ID (e.g. G16)
    sat_match = re.search(r"_G(\d+)_", filename)
    sat_id = int(sat_match.group(1)) if sat_match else None

    if sat_id in [16, 19]:
        sat = "goes_east"
    elif sat_id in [17, 18]:
        sat = "goes_west"
    else:
        raise ValueError(f"Unknown satellite ID: {sat_id}")

    # Extract product/collection
    product_match = re.search(r"OR_(.*?)-M\d+C\d+", filename)
    if product_match:
        product = product_match.group(1)
    else:
        raise ValueError(f"Could not parse product from filename: {filename}")

    domain = _parse_domain(product).lower()

    return {
        "start_time": start_time,
        "end_time": end_time,
        "channel_id": channel_id,
        "sat_id": sat_id,
        "sat": sat,
        "product": product,
        "domain": domain,
    }


def _parse_domain(collection: str) -> str:
    """Parse the domain from a GOES product collection name."""
    if not collection:
        raise ValueError("Collection name is empty")
    domain = collection[-1]
    if domain in ["C", "F", "M"]:
        return domain
    if "GLM-L2-LCFA" in collection:
        return "F"
    raise ValueError(f"Unknown GOES domain in collection name: {collection}")


def compute_goes_source_area_def(
    goes_file: str | Path | None = None,
    sat: str | None = None,
    domain: str | None = None,
    res: str | None = None,
) -> AreaDefinition:
    """Compute the GOES source area definition.

    Can be computed either from a GOES filename or by explicitly providing
    the satellite, domain, and resolution.

    Args:
        goes_file: Path to the GOES NetCDF file. If provided, metadata is parsed from filename.
        sat: Optional satellite name ("east" or "west").
        domain: Optional domain ("f", "c", or "p").
        res: Optional resolution ("500m", "1km", or "2km").

    Returns:
        pyresample.geometry.AreaDefinition: The source area definition.
    """
    from pyresample import load_area
    from pathlib import Path

    if not (sat and domain and res):
        if not goes_file:
            raise ValueError("Must provide either goes_file or all of (sat, domain, res)")

        filename = Path(goes_file).name
        info = parse_goes_filename(filename)
        if not (info.get("sat_id") and info.get("product") and info.get("channel_id")):
            raise ValueError(f"Could not parse GOES metadata from filename: {filename}")

        if not sat:
            sat = "east" if info["sat_id"] in (16, 19) else "west"
        if not domain:
            domain_code = _parse_domain(info["product"])
            domain = "f" if domain_code == "F" else ("c" if domain_code == "C" else "p")
        if not res:
            channel_id = int(info["channel_id"])
            if channel_id == 2:
                res = "500m"
            elif channel_id in (1, 3, 5):
                res = "1km"
            else:
                res = "2km"

    if sat == "west" and domain == "c":
        domain = "p"
    sat_id_str = sat.replace("goes_", "")
    area_name = f"goes_{sat_id_str}_abi_{domain}_{res}"
    areas_path = Path(__file__).parent / "areas.yaml"
    return cast(AreaDefinition, load_area(str(areas_path), area_name))


def read_abi_calibration_params(ds: xr.Dataset) -> dict[str, Any]:
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
    return params


def apply_abi_calibration(
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


def read_goes_crop(nc_path: str, crop: tuple[int, int, int, int], calibration="counts", modifiers="*"):
    r0, r1, c0, c1 = crop
    reader = detect_reader(nc_path)
    info = parse_goes_filename(nc_path)
    scn = Scene(filenames=[nc_path], reader=reader)
    available_datasets = scn.available_dataset_names()
    channel_name = map_channel_ids_to_satpy_names([info["channel_id"]], available_datasets)[0]
    scn.load([channel_name], calibration=calibration, modifiers=modifiers)
    return scn[r0:r1, c0:c1][channel_name].compute()


def compute_cell_slice(
    cell_utm_footprint_bounds: tuple[float, float, float, float],
    lut_area_extent: tuple[float, float, float, float],
    resolution: int,
    target_height: int | None = None,
    target_width: int | None = None,
) -> tuple[slice, slice, tuple[float, float, float, float]]:
    """Compute row/col slices within a UTM zone LUT for a grid cell.

    Given the UTM footprint bounds of a grid cell and the area extent
    of the full UTM zone LUT, return (row_slice, col_slice, effective_extent)
    to extract exactly the grid cell's region from the LUT arrays.

    The ``effective_extent`` is the area extent of the returned slice,
    snapped to the LUT's pixel grid. Use this (not the original cell
    bounds) when writing GeoTIFFs to ensure that pixel values and
    geospatial metadata are consistent.

    Args:
        cell_utm_footprint_bounds: Bounds of the grid cell in its UTM CRS (meters).
        lut_area_extent: Full extent of the UTM zone LUT (meters).
        resolution: Pixel size in meters.
        target_height: Optional forced height in pixels.
        target_width: Optional forced width in pixels. Avoids fencepost errors.

    Returns:
        Tuple of (row_slice, col_slice, effective_extent).
            - row_slice, col_slice: Slices into the 2D LUT grid.
            - effective_extent (tuple): (minx, miny, maxx, maxy) of the slice on the LUT grid.
    """
    lut_minx, lut_miny, _, lut_maxy = lut_area_extent
    cell_minx, cell_miny, cell_maxx, cell_maxy = cell_utm_footprint_bounds
    lut_height = int(round((lut_maxy - lut_miny) / resolution))

    if target_width is not None and target_height is not None:
        # Anchor start positions via floor to avoid sub-pixel drift,
        # then derive end = start + target dimension for exact size.
        col_start = int(math.floor((cell_minx - lut_minx) / resolution))
        col_end = col_start + target_width

        row_end_from_bottom = int(math.floor((cell_miny - lut_miny) / resolution))
        row_end = lut_height - row_end_from_bottom
        row_start = row_end - target_height
    else:
        # Legacy path: round both boundaries independently
        col_start = int(round((cell_minx - lut_minx) / resolution))
        col_end = int(round((cell_maxx - lut_minx) / resolution))

        row_start = lut_height - int(round((cell_maxy - lut_miny) / resolution))
        row_end = lut_height - int(round((cell_miny - lut_miny) / resolution))

    # Compute the effective area_extent on the LUT grid
    eff_minx = lut_minx + col_start * resolution
    eff_maxx = lut_minx + col_end * resolution
    eff_maxy = lut_maxy - row_start * resolution
    eff_miny = lut_maxy - row_end * resolution
    effective_extent = (eff_minx, eff_miny, eff_maxx, eff_maxy)

    return slice(row_start, row_end), slice(col_start, col_end), effective_extent


def compute_utm_zone_area_extent(utm_epsg: int, resolution: int) -> tuple[float, float, float, float, int, int]:
    crs = pyproj.CRS.from_epsg(utm_epsg)
    area = crs.area_of_use
    if area is None:
        raise ValueError(f"Could not determine area of use for EPSG:{utm_epsg}")

    west, south, east, north = area.bounds
    
    # Add a 2 degree buffer (~220km) to ensure grid cells that cross UTM
    # boundaries are fully covered by the LUT without getting NaN padded.
    west -= 2.0
    east += 2.0
    south = max(-90.0, south - 2.0)
    north = min(90.0, north + 2.0)
    transformer = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)

    lons = np.concatenate(
        [np.linspace(west, east, 100), np.full(100, east), np.linspace(east, west, 100), np.full(100, west)]
    )
    lats = np.concatenate(
        [np.full(100, south), np.linspace(south, north, 100), np.full(100, north), np.linspace(north, south, 100)]
    )

    xs, ys = transformer.transform(lons, lats)

    minx, miny = float(np.min(xs)), float(np.min(ys))
    maxx, maxy = float(np.max(xs)), float(np.max(ys))

    minx = math.floor(minx / resolution) * resolution
    miny = math.floor(miny / resolution) * resolution
    maxx = math.ceil(maxx / resolution) * resolution
    maxy = math.ceil(maxy / resolution) * resolution

    width = int((maxx - minx) / resolution)
    height = int((maxy - miny) / resolution)

    return minx, miny, maxx, maxy, width, height


def detect_goes_utm_zones(source_area_def: Any) -> list[int]:
    """Detect UTM zones that truly intersect the GOES satellite's visible disk.

    Uses a two-pass approach:
      1. **Coarse filter** — bounding-box overlap in lon/lat (fast, eliminates
         most of the 120 UTM zones).
      2. **Precise validation** — for each surviving candidate, sample a 5×5
         grid of lon/lat points within the UTM zone and project them into
         geostationary space.  Only zones where at least one sample point
         lands inside the ``source_area_def.area_extent`` are kept.

    The second pass is necessary because the geostationary visible disk is
    circular, not rectangular in lon/lat.  Zones at the limb of the disk
    pass the coarse bbox test but have zero actual pixel overlap.
    """
    crswgs84 = pyproj.CRS("EPSG:4326")

    if hasattr(source_area_def, "crs") and source_area_def.crs is not None:
        src_crs = pyproj.CRS(source_area_def.crs)
    elif hasattr(source_area_def, "proj_dict"):
        src_crs = pyproj.CRS(source_area_def.proj_dict)
    else:
        raise ValueError("Cannot determine CRS from source_area_def")

    # ── Pass 1: coarse bounding-box filter in lon/lat ────────────────
    transformer_to_wgs84 = pyproj.Transformer.from_crs(src_crs, crswgs84, always_xy=True)

    goes_minx, goes_miny, goes_maxx, goes_maxy = source_area_def.area_extent
    xs = np.linspace(goes_minx, goes_maxx, 100)
    ys = np.linspace(goes_miny, goes_maxy, 100)
    xv, yv = np.meshgrid(xs, ys)
    lons, lats = transformer_to_wgs84.transform(xv.ravel(), yv.ravel())

    valid = np.isfinite(lons) & np.isfinite(lats)
    valid_lons = lons[valid]
    valid_lats = lats[valid]

    if len(valid_lons) == 0:
        return []

    goes_min_lon = float(np.min(valid_lons)) - 1
    goes_max_lon = float(np.max(valid_lons)) + 1
    goes_min_lat = float(np.min(valid_lats)) - 1
    goes_max_lat = float(np.max(valid_lats)) + 1

    candidates: list[tuple[int, float, float, float, float]] = []

    for hemisphere_offset in (32600, 32700):
        for z in range(1, 61):
            epsg = hemisphere_offset + z
            crs = pyproj.CRS.from_epsg(epsg)
            area = crs.area_of_use
            if not area:
                continue
            west, south, east, north = area.bounds

            # Simple bounding box overlap
            if goes_min_lon <= east and goes_max_lon >= west and goes_min_lat <= north and goes_max_lat >= south:
                candidates.append((epsg, west, south, east, north))

    if not candidates:
        return []

    # ── Pass 2: validate by reverse-projecting into geostationary ────
    # For each candidate, sample a 5×5 lon/lat grid within the zone
    # and project into geostationary coordinates.  If at least one
    # sample falls within the GOES area extent, the zone is kept.
    transformer_to_goes = pyproj.Transformer.from_crs(crswgs84, src_crs, always_xy=True)

    utm_zones: list[int] = []

    for epsg, west, south, east, north in candidates:
        sample_lons = np.linspace(west, east, 5)
        sample_lats = np.linspace(south, north, 5)
        slons, slats = np.meshgrid(sample_lons, sample_lats)

        gx, gy = transformer_to_goes.transform(slons.ravel(), slats.ravel())

        hits = (
            np.isfinite(gx) & np.isfinite(gy)
            & (gx >= goes_minx) & (gx <= goes_maxx)
            & (gy >= goes_miny) & (gy <= goes_maxy)
        )

        if np.any(hits):
            utm_zones.append(epsg)

    return sorted(utm_zones)


def compute_source_crop_slices(
    valid_input_index: np.ndarray,
    source_shape: tuple[int, int],
) -> tuple[slice, slice, np.ndarray, np.ndarray]:
    """Compute the minimal bounding-box crop of the GOES source grid.

    Uses the valid_input_index boolean mask to find participating source pixels,
    then returns slices for the tightest 2D crop containing all of them.

    Args:
        valid_input_index: 1D boolean array of length source_height * source_width.
        source_shape: (height, width) of the source GOES image.

    Returns:
        Tuple containing:
            - row_slice: Slice into source rows.
            - col_slice: Slice into source columns.
            - row_offsets: Row indices of valid pixels relative to crop origin.
            - col_offsets: Column indices of valid pixels relative to crop origin.
    """
    mask_2d = valid_input_index.reshape(source_shape)
    rows_2d, cols_2d = np.where(mask_2d)

    if len(rows_2d) == 0:
        raise ValueError("No valid source pixels found — UTM zone does not overlap the GOES source grid")

    row_min, row_max = int(rows_2d.min()), int(rows_2d.max())
    col_min, col_max = int(cols_2d.min()), int(cols_2d.max())

    row_slice = slice(row_min, row_max + 1)
    col_slice = slice(col_min, col_max + 1)

    # Offsets within the crop
    row_offsets = rows_2d - row_min
    col_offsets = cols_2d - col_min

    return row_slice, col_slice, row_offsets, col_offsets


def download_lut_if_needed(
    combo: str,
    utm_epsg: int,
    resolution: int,
    local_dir: str | Path,
    remote_bucket: str = "hf://buckets/frandorr/aer-data/luts"
) -> Path:
    """Download a UTMZoneLUT file from a remote bucket to a local directory if it does not exist locally.

    Args:
        combo: Satellite and domain combination (e.g., 'goes_east_f').
        utm_epsg: UTM EPSG code.
        resolution: Spatial resolution in meters.
        local_dir: Local directory to store LUTs.
        remote_bucket: Remote bucket URI.

    Returns:
        Path to the local LUT file.

    Raises:
        FileNotFoundError: If the LUT is not available locally and cannot be
            downloaded (remote file missing or download failure).
    """
    import fsspec
    from structlog import get_logger
    logger = get_logger()

    local_dir = Path(local_dir)
    rel_path = f"{combo}/{utm_epsg}/{resolution}m.npz"
    local_path = local_dir / rel_path

    # Guard against 0-byte files left by previous failed downloads
    if local_path.exists() and local_path.stat().st_size == 0:
        logger.warning("removing_empty_lut", path=str(local_path))
        local_path.unlink()

    if not local_path.exists():
        remote_path = f"{remote_bucket.rstrip('/')}/{rel_path}"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fs, rpath = fsspec.core.url_to_fs(remote_path)
            if fs.exists(rpath):
                logger.info("downloading_lut", remote_path=remote_path, local_path=str(local_path))
                fs.get(rpath, str(local_path))
            else:
                raise FileNotFoundError(
                    f"LUT not available at remote path: {remote_path}"
                )
        except FileNotFoundError:
            raise
        except Exception as e:
            # Clean up any partial download
            if local_path.exists():
                local_path.unlink()
            raise FileNotFoundError(
                f"Failed to download LUT from {remote_path}: {e}"
            ) from e

        # Validate the downloaded file is not empty
        if local_path.exists() and local_path.stat().st_size == 0:
            local_path.unlink()
            raise FileNotFoundError(
                f"Downloaded LUT file is empty (0 bytes): {local_path}"
            )

    return local_path

