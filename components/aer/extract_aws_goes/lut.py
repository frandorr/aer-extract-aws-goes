import re
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import attrs
import numpy as np
import pyproj
import fsspec
import os
import zarr
from pyresample.geometry import AreaDefinition
from pyresample.kd_tree import get_neighbour_info

SUPPORTED_RESOLUTIONS = (500, 1000, 2000)
DEFAULT_RADIUS_OF_INFLUENCE = 50_000  # meters, for pyresample kd-tree search


def get_default_bucket_uri() -> str:
    """Return the default Hugging Face bucket URI for LUTs."""
    return "hf://buckets/frandorr/aer-data"


# Known GOES ABI source shapes by flat pixel count.
# Used to reshape valid_input_index when source_shape is not stored in metadata.
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


def _parse_goes_filename(filename: str) -> dict[str, Any]:
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

    # Extract product/collection
    product_match = re.search(r"OR_(.*?)-M\d+C\d+", filename)
    product = product_match.group(1) if product_match else None

    return {
        "start_time": start_time,
        "end_time": end_time,
        "channel_id": channel_id,
        "sat_id": sat_id,
        "product": product,
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


@attrs.frozen
class UTMZoneLUT:
    utm_epsg: int
    resolution: int
    area_extent: tuple[float, float, float, float]
    width: int
    height: int
    zarr_path: str
    source_shape: tuple[int, int] | None = None


def compute_utm_zone_area_extent(utm_epsg: int, resolution: int) -> tuple[float, float, float, float, int, int]:
    crs = pyproj.CRS.from_epsg(utm_epsg)
    area = crs.area_of_use
    if area is None:
        raise ValueError(f"Could not determine area of use for EPSG:{utm_epsg}")

    west, south, east, north = area.bounds
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
        info = _parse_goes_filename(filename)
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
    area_name = f"goes_{sat}_abi_{domain}_{res}"
    areas_path = Path(__file__).parent / "areas.yaml"
    return cast(AreaDefinition, load_area(str(areas_path), area_name))


def generate_utm_zone_lut(
    source_area_def: AreaDefinition, utm_epsg: int, resolution: int, output_dir: Path, chunk_size: int = 1024
) -> None:
    minx, miny, maxx, maxy, width, height = compute_utm_zone_area_extent(utm_epsg, resolution)

    target_area_def = AreaDefinition(
        area_id=f"utm_{utm_epsg}_{resolution}m",
        description=f"UTM Zone {utm_epsg} at {resolution}m",
        proj_id=f"epsg_{utm_epsg}",
        projection=f"EPSG:{utm_epsg}",
        width=width,
        height=height,
        area_extent=(minx, miny, maxx, maxy),
    )

    valid_input_index, valid_output_index, index_array, target_shape = get_neighbour_info(
        source_area_def, target_area_def, radius_of_influence=DEFAULT_RADIUS_OF_INFLUENCE, neighbours=1, nprocs=-1
    )

    store_dir = Path(output_dir) / str(utm_epsg)
    store_dir.mkdir(parents=True, exist_ok=True)
    store_path = store_dir / f"{resolution}m.zarr"

    z = zarr.open(str(store_path), mode="w")

    # Store metadata
    z.attrs["utm_epsg"] = utm_epsg
    z.attrs["resolution"] = resolution
    z.attrs["area_extent"] = [minx, miny, maxx, maxy]
    z.attrs["width"] = width
    z.attrs["height"] = height

    if hasattr(source_area_def, "crs") and source_area_def.crs is not None:
        source_crs_wkt = source_area_def.crs.to_wkt()
    elif hasattr(source_area_def, "proj_dict"):
        source_crs_wkt = pyproj.CRS(source_area_def.proj_dict).to_wkt()
    else:
        source_crs_wkt = ""

    z.attrs["source_crs_wkt"] = source_crs_wkt
    z.attrs["created"] = datetime.now(tz=timezone.utc).isoformat()
    z.attrs["shape"] = [height, width]
    z.attrs["source_shape"] = [source_area_def.height, source_area_def.width]

    # Store arrays with chunking
    z.create_array(  # pyright: ignore[reportAttributeAccessIssue]
        "valid_input_index",
        data=valid_input_index,
        chunks=(chunk_size * chunk_size,),
    )
    z.create_array(  # pyright: ignore[reportAttributeAccessIssue]
        "valid_output_index",
        data=valid_output_index,
        chunks=(chunk_size * chunk_size,),
    )
    z.create_array(  # pyright: ignore[reportAttributeAccessIssue]
        "index_array",
        data=index_array.astype(np.int32),
        chunks=(chunk_size * chunk_size,),
    )


def get_default_lut_dir() -> Path:
    """Return the platform-specific default cache directory for LUTs."""
    import os

    if "XDG_CACHE_HOME" in os.environ:
        base = Path(os.environ["XDG_CACHE_HOME"])
    else:
        base = Path.home() / ".cache"

    return base / "aer" / "extract-aws-goes" / "luts"


def load_utm_zone_lut(
    bucket_uri: str | Path,
    utm_epsg: int,
    resolution: int,
    combo: str | None = None,
) -> tuple[UTMZoneLUT, Any]:
    """Load a UTM zone LUT from a remote Zarr store (e.g. Hugging Face bucket) or local path.

    Args:
        bucket_uri: Base URI (e.g., 'hf://frandorr/aer-data') or local path.
        utm_epsg: UTM EPSG code.
        resolution: Spatial resolution in meters.
        combo: Optional combo name (e.g. 'goes19_radf').

    Returns:
        Tuple of (UTMZoneLUT metadata, Zarr group object).
    """
    bucket_uri_str = str(bucket_uri)
    if combo:
        zarr_path = f"{bucket_uri_str.rstrip('/')}/{combo}/{utm_epsg}/{resolution}m.zarr"
    else:
        # Backward compatibility / simple layout for tests
        zarr_path = f"{bucket_uri_str.rstrip('/')}/{utm_epsg}/{resolution}m.zarr"

    protocol = bucket_uri_str.split("://")[0] if "://" in bucket_uri_str else "file"
    if protocol == "file":
        if zarr_path.startswith("file://"):
            zarr_path = zarr_path[7:]
        z = zarr.open(zarr_path, mode="r")
    else:
        cache_dir = os.path.expanduser("~/.cache/aer_cache")
        os.makedirs(cache_dir, exist_ok=True)
        mapper = fsspec.get_mapper(f"simplecache::{zarr_path}", cache_storage=cache_dir)
        z = zarr.open(mapper, mode="r")

    # Read source_shape if stored, otherwise infer from valid_input_index length
    raw_source_shape = z.attrs.get("source_shape")
    if raw_source_shape is not None:
        source_shape: tuple[int, int] | None = (int(raw_source_shape[0]), int(raw_source_shape[1]))  # pyright: ignore[reportIndexIssue, reportArgumentType]
    else:
        # Infer from valid_input_index array length
        vi_len = z["valid_input_index"].shape[0]  # pyright: ignore[reportAttributeAccessIssue, reportGeneralTypeIssues, reportArgumentType]
        source_shape = infer_source_shape(vi_len)

    lut_meta = UTMZoneLUT(
        utm_epsg=z.attrs["utm_epsg"],  # pyright: ignore[reportAttributeAccessIssue, reportArgumentType]
        resolution=z.attrs["resolution"],  # pyright: ignore[reportAttributeAccessIssue, reportArgumentType]
        area_extent=tuple(z.attrs["area_extent"]),  # pyright: ignore[reportAttributeAccessIssue, reportArgumentType]
        width=z.attrs["width"],  # pyright: ignore[reportAttributeAccessIssue, reportArgumentType]
        height=z.attrs["height"],  # pyright: ignore[reportAttributeAccessIssue, reportArgumentType]
        zarr_path=zarr_path,
        source_shape=source_shape,
    )

    return lut_meta, z


def list_available_utm_zones(lut_dir: Path) -> list[tuple[int, int]]:
    available = []
    base_dir = Path(lut_dir)
    if not base_dir.exists():
        return available

    for epsg_dir in base_dir.iterdir():
        if not epsg_dir.is_dir() or not epsg_dir.name.isdigit():
            continue

        epsg = int(epsg_dir.name)
        for res_dir in epsg_dir.glob("*m.zarr"):
            res_str = res_dir.name.replace("m.zarr", "")
            if res_str.isdigit():
                available.append((epsg, int(res_str)))

    return sorted(available)


def detect_goes_utm_zones(source_area_def: AreaDefinition) -> list[int]:
    crswgs84 = pyproj.CRS("EPSG:4326")

    if hasattr(source_area_def, "crs") and source_area_def.crs is not None:
        src_crs = pyproj.CRS(source_area_def.crs)
    elif hasattr(source_area_def, "proj_dict"):
        src_crs = pyproj.CRS(source_area_def.proj_dict)
    else:
        raise ValueError("Cannot determine CRS from source_area_def")

    transformer = pyproj.Transformer.from_crs(src_crs, crswgs84, always_xy=True)

    minx, miny, maxx, maxy = source_area_def.area_extent
    xs = np.linspace(minx, maxx, 100)
    ys = np.linspace(miny, maxy, 100)
    xv, yv = np.meshgrid(xs, ys)
    lons, lats = transformer.transform(xv.ravel(), yv.ravel())

    valid = np.isfinite(lons) & np.isfinite(lats)
    valid_lons = lons[valid]
    valid_lats = lats[valid]

    if len(valid_lons) == 0:
        return []

    goes_min_lon, goes_max_lon = np.min(valid_lons), np.max(valid_lons)
    goes_min_lat, goes_max_lat = np.min(valid_lats), np.max(valid_lats)

    goes_min_lon -= 1
    goes_max_lon += 1
    goes_min_lat -= 1
    goes_max_lat += 1

    utm_zones = []

    # North
    for z in range(1, 61):
        epsg = 32600 + z
        crs = pyproj.CRS.from_epsg(epsg)
        area = crs.area_of_use
        if area:
            west, south, east, north = area.bounds

            # Simple bounding box overlap logic
            # Be careful with 180/-180 wrap, but for geostationary usually ok if simple
            # Since going to WGS84, handle if GOES extends over antimeridian

            if goes_min_lon <= east and goes_max_lon >= west and goes_min_lat <= north and goes_max_lat >= south:
                utm_zones.append(epsg)

    # South
    for z in range(1, 61):
        epsg = 32700 + z
        crs = pyproj.CRS.from_epsg(epsg)
        area = crs.area_of_use
        if area:
            west, south, east, north = area.bounds
            if goes_min_lon <= east and goes_max_lon >= west and goes_min_lat <= north and goes_max_lat >= south:
                utm_zones.append(epsg)

    return sorted(utm_zones)


def compute_cell_slice(
    cell_utm_footprint_bounds: tuple[float, float, float, float],
    lut_area_extent: tuple[float, float, float, float],
    resolution: int,
    target_width: int | None = None,
    target_height: int | None = None,
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
        target_width: Optional forced width in pixels. Avoids fencepost errors.
        target_height: Optional forced height in pixels.

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

    row_min, row_max = int(rows_2d.min()), int(rows_2d.max())
    col_min, col_max = int(cols_2d.min()), int(cols_2d.max())

    row_slice = slice(row_min, row_max + 1)
    col_slice = slice(col_min, col_max + 1)

    # Offsets within the crop
    row_offsets = rows_2d - row_min
    col_offsets = cols_2d - col_min

    return row_slice, col_slice, row_offsets, col_offsets


def read_goes_crop(
    nc_path: str | Path,
    row_slice: slice,
    col_slice: slice,
    variable: str = "Rad",
    apply_scale_offset: bool = True,
) -> np.ndarray:
    """Read a 2D crop from a GOES NetCDF file using h5py.

    Args:
        nc_path: Path to the GOES .nc file.
        row_slice, col_slice: Slices into the source raster.
        variable: HDF5 variable name.
        apply_scale_offset: If True, apply scale_factor and add_offset attributes.

    Returns:
        2D float32 array of shape (row_slice height, col_slice width).
    """
    import h5py

    with h5py.File(str(nc_path), "r") as f:
        var = f[variable]
        crop = var[row_slice, col_slice]  # type: ignore[index]
        if apply_scale_offset and "scale_factor" in var.attrs:
            crop = crop.astype(np.float32) * var.attrs["scale_factor"][0] + var.attrs["add_offset"][0]  # pyright: ignore[reportAttributeAccessIssue, reportIndexIssue]
        else:
            crop = crop.astype(np.float32)  # pyright: ignore[reportAttributeAccessIssue]
    return cast(np.ndarray, crop)


def extract_cell_from_lut(
    source_crop: np.ndarray,
    row_offsets: np.ndarray,
    col_offsets: np.ndarray,
    lut_group: zarr.Group,
    cell_row_slice: slice,
    cell_col_slice: slice,
    lut_width: int,
) -> np.ndarray:
    """Extract a grid cell's data using pre-computed LUT arrays.

    Args:
        source_crop: 2D cropped source region.
        row_offsets, col_offsets: Coordinate arrays from compute_source_crop_slices.
        lut_group: Zarr group with valid_output_index and index_array.
        cell_row_slice, cell_col_slice: Slices within the full UTM zone LUT grid.
        lut_width: Full dimensions of the UTM zone grid.

    Returns:
        2D float32 array of shape (cell_height, cell_width).
    """
    cell_height = cell_row_slice.stop - cell_row_slice.start
    cell_width = cell_col_slice.stop - cell_col_slice.start

    # Build flat index mask for the cell region within the full LUT
    rows = np.arange(cell_row_slice.start, cell_row_slice.stop)
    cols = np.arange(cell_col_slice.start, cell_col_slice.stop)
    row_grid, col_grid = np.meshgrid(rows, cols, indexing="ij")
    flat_indices = (row_grid * lut_width + col_grid).ravel()

    # Load only the needed LUT chunks from Zarr
    valid_output = np.asarray(lut_group["valid_output_index"][flat_indices]).astype(bool)
    index_arr = np.asarray(lut_group["index_array"][flat_indices])

    # Gather valid input pixels from the crop (compressed valid-input space)
    # source_crop[row_offsets, col_offsets] yields exactly the pixels where
    # valid_input_index is True, in flat order — matching pyresample convention.
    valid_source_pixels = source_crop[row_offsets, col_offsets]

    # Apply the LUT: index_array values index into valid_source_pixels
    result = np.full(cell_height * cell_width, np.nan, dtype=np.float32)
    result[valid_output] = valid_source_pixels[index_arr[valid_output]]

    return result.reshape(cell_height, cell_width)
