import xarray as xr
from rasterio.transform import from_bounds
from pathlib import Path
from typing import Any
import cattrs

import attrs
import numpy as np
import fsspec
import os
from pyresample.geometry import AreaDefinition
from pyresample.kd_tree import get_neighbour_info
from structlog import get_logger
from .utils import (
    compute_utm_zone_area_extent,
    compute_source_crop_slices,
    compute_goes_source_area_def,
    parse_goes_filename,
    compute_cell_slice,
)
from aer.grid import GridCell

logger = get_logger()

SUPPORTED_RESOLUTIONS = (500, 1000, 2000)
DEFAULT_RADIUS_OF_INFLUENCE = 50_000  # meters, for pyresample kd-tree search


def get_default_bucket_uri() -> str:
    """Return the default Hugging Face bucket URI for LUTs."""
    return "hf://datasets/frandorr/aer-data/luts"


def uri_exists(uri: str | Path) -> bool:
    """Check if a URI (local or remote) exists."""
    path_str = str(uri)
    protocol = path_str.split("://")[0] if "://" in path_str else "file"
    if protocol == "file":
        # Handle file:// prefix if present
        local_path = path_str[7:] if path_str.startswith("file://") else path_str
        return Path(local_path).exists()

    try:
        fs, path = fsspec.core.url_to_fs(uri)
        return fs.exists(path)
    except Exception:
        return False


@attrs.frozen
class UTMZoneLUT:
    utm_epsg: int
    resolution: int
    area_extent: tuple[float, float, float, float]
    width: int
    height: int
    row_map: Any
    col_map: Any
    crop_slices: tuple[int, int, int, int]
    source_shape: tuple[int, int]
    lut_path: str
    satellite: str
    domain: str


def generate_utm_zone_lut(goes_path: Path, utm_epsg: int, resolution: int, output_uri: Path | str | None) -> UTMZoneLUT:
    """
    Generate a lookup table (LUT) to map GOES source data to a UTM zone grid.

    The LUT is stored as a NumPy .npz file with two 2D int16 arrays:
    - row_map: (height, width) int16 — row index into the GOES crop for each UTM pixel, or -1 if invalid
    - col_map: (height, width) int16 — col index into the GOES crop for each UTM pixel, or -1 if invalid

    This eliminates the need to pre-load any offset arrays into RAM. Each cell extraction
    requires only two 2D memmapped reads (row_map[row_sl, col_sl] and col_map[row_sl, col_sl]).
    crop_slices provide the zero-S3-read initial GOES file crop.

    Args:
        goes_path: Path to the GOES NetCDF file.
        utm_epsg: The UTM EPSG code.
        resolution: The resolution.
        output_uri: URI to save the LUT to. If None, the LUT will not be saved.
    """
    # if output_uri is not None check if file already exist and return loading the lut
    sat_info = parse_goes_filename(str(goes_path))
    if output_uri is not None:
        lut_path = generate_path(output_uri, sat_info.get("sat", ""), sat_info.get("domain", ""), utm_epsg, resolution)
        if uri_exists(lut_path):
            return load_lut(lut_path)

    source_area_def = compute_goes_source_area_def(goes_path)

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

    valid_input_index, valid_output_index, index_array, _ = get_neighbour_info(
        source_area_def, target_area_def, radius_of_influence=DEFAULT_RADIUS_OF_INFLUENCE, neighbours=1, nprocs=-1
    )

    source_shape = (source_area_def.height, source_area_def.width)
    row_sl, col_sl, row_offsets, col_offsets = compute_source_crop_slices(valid_input_index, source_shape)

    n_valid_in = int(valid_input_index.sum())
    n_target = height * width

    row_map = np.full(n_target, -1, dtype=np.int16)
    col_map = np.full(n_target, -1, dtype=np.int16)

    # Flat indices into the target grid that have a valid neighbor
    valid_out_flat = np.flatnonzero(valid_output_index)

    # Guard against fill values (index == n_valid_in means no valid neighbor)
    valid_ia_mask = index_array < n_valid_in
    valid_out_with_neighbor = valid_out_flat[valid_ia_mask]
    ia_clipped = index_array[valid_ia_mask]

    row_map[valid_out_with_neighbor] = row_offsets[ia_clipped].astype(np.int16)
    col_map[valid_out_with_neighbor] = col_offsets[ia_clipped].astype(np.int16)

    row_map_2d = row_map.reshape(height, width)
    col_map_2d = col_map.reshape(height, width)

    zone_lut_dict = dict(
        utm_epsg=utm_epsg,
        resolution=resolution,
        area_extent=(minx, miny, maxx, maxy),
        width=width,
        height=height,
        crop_slices=(row_sl.start, row_sl.stop, col_sl.start, col_sl.stop),
        source_shape=(source_area_def.height, source_area_def.width),
        row_map=row_map_2d,
        col_map=col_map_2d,
        satellite=sat_info.get("sat", ""),
        domain=sat_info.get("domain", ""),
        lut_path=generate_path(output_uri, sat_info.get("sat", ""), sat_info.get("domain", ""), utm_epsg, resolution),
    )

    utm_zone_lut = cattrs.structure(zone_lut_dict, UTMZoneLUT)

    if output_uri is not None:
        save_utm_zone_lut(utm_zone_lut)

    return utm_zone_lut


def save_lut(path: str | Path, lut: UTMZoneLUT):
    # np.savez is used for speed (uncompressed). Metadata is saved as arrays.
    np.savez_compressed(
        str(path),
        row_map=lut.row_map,
        col_map=lut.col_map,
        utm_epsg=lut.utm_epsg,
        resolution=lut.resolution,
        area_extent=lut.area_extent,
        width=lut.width,
        height=lut.height,
        crop_slices=lut.crop_slices,
        source_shape=lut.source_shape,
        satellite=lut.satellite,
        domain=lut.domain,
    )


def generate_path(uri, sat, domain, utm_epsg, resolution) -> str:
    return f"{str(uri).rstrip('/')}/{sat}_{domain}/{utm_epsg}/{resolution}m.npz"


def save_utm_zone_lut(
    lut: UTMZoneLUT,
):
    lut_path = lut.lut_path
    if not lut_path:
        raise ValueError("UTMZoneLUT does not have a lut_path set")

    # Only create directories for local paths
    protocol = lut_path.split("://")[0] if "://" in lut_path else "file"
    if protocol == "file":
        local_path = Path(lut_path[7:] if lut_path.startswith("file://") else lut_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        save_lut(local_path, lut)
    else:
        save_lut(lut_path, lut)


def load_lut(path: str | Path) -> UTMZoneLUT:
    # np.load with mmap_mode='r' is lazy and only loads metadata initially.
    # It will raise an error if the path doesn't exist.
    # Note: fsspec paths might not support mmap_mode directly,
    # but since we are focusing on local data for speed, this is optimized for local.
    with np.load(str(path), mmap_mode="r") as data:
        # Extract metadata and arrays
        # Scalars are returned as 0-d arrays, so we use .item() or [()]
        lut_dict = {
            "utm_epsg": int(data["utm_epsg"]),
            "resolution": int(data["resolution"]),
            "area_extent": tuple(data["area_extent"]),
            "width": int(data["width"]),
            "height": int(data["height"]),
            "crop_slices": tuple(data["crop_slices"]),
            "source_shape": tuple(data["source_shape"]),
            "satellite": str(data["satellite"]),
            "domain": str(data["domain"]),
            "row_map": data["row_map"],
            "col_map": data["col_map"],
            "lut_path": str(path),
        }

    return cattrs.structure(lut_dict, UTMZoneLUT)


def load_utm_zone_lut(
    uri: str | Path,
    utm_epsg: int,
    resolution: int,
    combo: str | None = None,
) -> UTMZoneLUT:
    """Load a UTM zone LUT from a NumPy .npz file (local path recommended).

    Args:
        uri: Base URI (e.g., '/path/to/luts') or local path.
        utm_epsg: UTM EPSG code.
        resolution: Spatial resolution in meters.
        combo: Optional combo name (e.g. 'goes_east_f').

    Returns:
        UTMZoneLUT metadata.
    """
    bucket_uri_str = str(uri).rstrip("/")

    if combo:
        lut_path = f"{bucket_uri_str}/{combo}/{utm_epsg}/{resolution}m.npz"
    else:
        lut_path = f"{bucket_uri_str}/{utm_epsg}/{resolution}m.npz"

    return load_lut(lut_path)


def apply_lut(
    source_crop: np.ndarray,
    lut: UTMZoneLUT,
    row_slice: slice,
    col_slice: slice,
):
    row = lut.row_map[row_slice, col_slice]
    col = lut.col_map[row_slice, col_slice]

    valid = row >= 0

    out = np.full(row.shape, np.nan, dtype=np.float32)
    out[valid] = source_crop[row[valid], col[valid]]

    return out


def list_available_utm_zones(lut_dir: str | Path) -> list[tuple[int, int]]:
    available = []
    if not uri_exists(lut_dir):
        return available

    fs, base_path = fsspec.core.url_to_fs(str(lut_dir))

    try:
        # Details=False returns a list of strings
        for epsg_path in fs.ls(base_path, detail=False):
            epsg_name = os.path.basename(epsg_path.rstrip("/"))
            if not epsg_name.isdigit():
                continue

            epsg = int(epsg_name)
            # In some fsspec implementations, ls might not be recursive, so we check one level down
            for res_path in fs.ls(epsg_path, detail=False):
                res_name = os.path.basename(res_path.rstrip("/"))
                if res_name.endswith("m.npz"):
                    res_str = res_name.replace("m.npz", "")
                    if res_str.isdigit():
                        available.append((epsg, int(res_str)))
    except Exception as e:
        logger.warning("Failed to list available UTM zones", error=str(e), path=str(lut_dir))

    return sorted(list(set(available)))


from pyresample.area_config import load_area_from_string


def extract_cell_from_lut(
    source_crop: xr.DataArray,
    grid_cell: GridCell,
    lut: UTMZoneLUT,
) -> xr.DataArray:
    utm_bounds = grid_cell.utm_footprint.bounds
    # area_def = grid_cell.area_def(lut.resolution)
    area_def = load_area_from_string(grid_cell.area_def(lut.resolution).to_yaml())
    row_sl, col_sl, eff_bounds = compute_cell_slice(
        utm_bounds, lut.area_extent, lut.resolution, area_def.height, area_def.width
    )
    tile = apply_lut(source_crop.values, lut, row_sl, col_sl)

    minx, miny, maxx, maxy = eff_bounds
    cell_w = col_sl.stop - col_sl.start
    cell_h = row_sl.stop - row_sl.start

    # Coordinates (centers of the pixels, descending y)
    x_coords, y_coords = area_def.get_proj_vectors()

    da = xr.DataArray(
        tile.astype(np.float32),
        coords={"y": y_coords, "x": x_coords},
        dims=("y", "x"),
        name="goes_data",
    )

    # Set CRS and transform using rioxarray conventions
    da.rio.write_crs(f"EPSG:{lut.utm_epsg}", inplace=True)
    dst_transform = from_bounds(*area_def.area_extent, cell_w, cell_h)
    da.rio.write_transform(dst_transform, inplace=True)

    return da
