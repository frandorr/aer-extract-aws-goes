import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import attrs
import numpy as np
import pyproj
import shutil
import urllib.request
import zipfile
import importlib.metadata
import zarr
from pyresample.geometry import AreaDefinition
from pyresample.kd_tree import get_neighbour_info

SUPPORTED_RESOLUTIONS = (500, 1000, 2000)
DEFAULT_RADIUS_OF_INFLUENCE = 50_000  # meters, for pyresample kd-tree search

def get_package_version() -> str:
    """Return the version of the aer-extract-aws-goes package."""
    try:
        # Standard way for installed pip packages
        return importlib.metadata.version("aer-extract-aws-goes")
    except importlib.metadata.PackageNotFoundError:
        # Fallback for development/uninstalled mode
        return "1.1.0"


def get_default_lut_release_url() -> str:
    """Return the GitHub Release URL for the current package version."""
    version = get_package_version()
    # Normalize version: ensure it starts with 'v' if needed, though releases usually use v1.1.0
    v_tag = f"v{version}" if not version.startswith("v") else version
    return f"https://github.com/frandorr/aer-extract-aws-goes/releases/download/{v_tag}-luts/"

@attrs.frozen
class UTMZoneLUT:
    utm_epsg: int
    resolution: int
    area_extent: tuple[float, float, float, float]
    width: int
    height: int
    zarr_path: str


def compute_utm_zone_area_extent(utm_epsg: int, resolution: int) -> tuple[float, float, float, float, int, int]:
    crs = pyproj.CRS.from_epsg(utm_epsg)
    area = crs.area_of_use
    if area is None:
        raise ValueError(f"Could not determine area of use for EPSG:{utm_epsg}")
    
    west, south, east, north = area.bounds
    transformer = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    
    lons = np.concatenate([
        np.linspace(west, east, 100),
        np.full(100, east),
        np.linspace(east, west, 100),
        np.full(100, west)
    ])
    lats = np.concatenate([
        np.full(100, south),
        np.linspace(south, north, 100),
        np.full(100, north),
        np.linspace(north, south, 100)
    ])
    
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


def compute_goes_source_area_def(goes_file: str | Path) -> AreaDefinition:
    from aer.extract_aws_goes.core import AwsGoesExtractor
    
    crs_wkt, gt, width, height = AwsGoesExtractor._get_goes_georef(str(goes_file))
    
    minx = gt[0]
    maxy = gt[3]
    maxx = minx + width * gt[1]
    miny = maxy + height * gt[5]
    
    crs = pyproj.CRS.from_wkt(crs_wkt)
    
    return AreaDefinition(
        area_id="goes_abi",
        description="GOES ABI Geostationary",
        proj_id="goes_abi",
        projection=crs.to_proj4(),
        width=width,
        height=height,
        area_extent=(minx, miny, maxx, maxy)
    )


def generate_utm_zone_lut(
    source_area_def: AreaDefinition,
    utm_epsg: int,
    resolution: int,
    output_dir: Path,
    chunk_size: int = 1024
) -> None:
    minx, miny, maxx, maxy, width, height = compute_utm_zone_area_extent(utm_epsg, resolution)
    
    target_area_def = AreaDefinition(
        area_id=f"utm_{utm_epsg}_{resolution}m",
        description=f"UTM Zone {utm_epsg} at {resolution}m",
        proj_id=f"epsg_{utm_epsg}",
        projection=f"EPSG:{utm_epsg}",
        width=width,
        height=height,
        area_extent=(minx, miny, maxx, maxy)
    )
    
    valid_input_index, valid_output_index, index_array, target_shape = get_neighbour_info(
        source_area_def,
        target_area_def,
        radius_of_influence=DEFAULT_RADIUS_OF_INFLUENCE,
        neighbours=1,
        nprocs=-1
    )
    
    store_dir = Path(output_dir) / str(utm_epsg)
    store_dir.mkdir(parents=True, exist_ok=True)
    store_path = store_dir / f"{resolution}m.zarr"
    
    z = zarr.open(str(store_path), mode='w')
    
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
    
    # Store arrays with chunking
    z.create_dataset(
        "valid_input_index", 
        data=valid_input_index, 
        shape=valid_input_index.shape, 
        dtype=bool, 
        chunks=(chunk_size * chunk_size,)
    )
    z.create_dataset(
        "valid_output_index", 
        data=valid_output_index, 
        shape=valid_output_index.shape, 
        dtype=bool, 
        chunks=(chunk_size * chunk_size,)
    )
    z.create_dataset(
        "index_array", 
        data=index_array.astype(np.int32), 
        shape=index_array.shape, 
        dtype=np.int32, 
        chunks=(chunk_size * chunk_size,)
    )


def get_default_lut_dir() -> Path:
    """Return the platform-specific default cache directory for LUTs."""
    import os

    if "XDG_CACHE_HOME" in os.environ:
        base = Path(os.environ["XDG_CACHE_HOME"])
    else:
        base = Path.home() / ".cache"

    return base / "aer" / "extract-aws-goes" / "luts"


def download_lut(
    lut_dir: Path,
    utm_epsg: int,
    resolution: int,
    combo: str,
    base_url: str | None = None,
) -> Path:
    """Download and extract a LUT from the remote repository release assets.

    Parameters
    ----------
    lut_dir : Path
        Local directory where LUTs are stored.
    utm_epsg : int
        UTM EPSG code (e.g., 32619).
    resolution : int
        Spatial resolution in meters.
    combo : str
        Satellite/product combo name (e.g., 'goes19_radf').
    base_url : str, optional
        Base URL for the release assets. Defaults to get_default_lut_release_url().

    Returns
    -------
    Path
        Path to the extracted .zarr store.
    """
    if base_url is None:
        base_url = get_default_lut_release_url()

    filename = f"{combo}_{utm_epsg}_{resolution}m.zarr.zip"
    url = f"{base_url.rstrip('/')}/{filename}"
    local_zip = lut_dir / filename
    target_dir = lut_dir / combo / str(utm_epsg) / f"{resolution}m.zarr"

    if target_dir.exists():
        return target_dir

    print(f"LUT not found locally ({target_dir}). Downloading from {url}...")
    (lut_dir / combo / str(utm_epsg)).mkdir(parents=True, exist_ok=True)

    try:
        # Use urllib for zero-dependency download
        urllib.request.urlretrieve(url, local_zip)
        with zipfile.ZipFile(local_zip, "r") as zip_ref:
            # We assume the zip contains the '...m.zarr' directory content
            # or the '...m.zarr' directory itself.
            # Best practice is for the zip to contain the directory itself.
            zip_ref.extractall(lut_dir / combo / str(utm_epsg))

        if local_zip.exists():
            local_zip.unlink()
        return target_dir
    except Exception as e:
        if local_zip.exists():
            local_zip.unlink()
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        raise RuntimeError(f"Failed to download LUT from {url}: {e}") from e


def load_utm_zone_lut(
    lut_dir: Path,
    utm_epsg: int,
    resolution: int,
    combo: str | None = None,
    auto_download: bool = True,
) -> tuple[UTMZoneLUT, Any]:
    """Load a UTM zone LUT from a Zarr store, optionally downloading it if missing.

    Parameters
    ----------
    lut_dir : Path
        Root directory for LUTs. If combo is provided, the store is expected
        at lut_dir / combo / epsg / res m.zarr.
    utm_epsg : int
    resolution : int
    combo : str, optional
        Combo name (e.g. 'goes19_radf'). Required if auto_download is True.
    auto_download : bool
        If True and the LUT is missing, attempt to download it.
    """
    if combo:
        store_path = Path(lut_dir) / combo / str(utm_epsg) / f"{resolution}m.zarr"
    else:
        # Backward compatibility / simple layout
        store_path = Path(lut_dir) / str(utm_epsg) / f"{resolution}m.zarr"

    if not store_path.exists():
        if auto_download:
            if not combo:
                raise ValueError("auto_download=True requires 'combo' name")
            download_lut(lut_dir, utm_epsg, resolution, combo)
        else:
            raise FileNotFoundError(f"LUT not found at {store_path}")

    z = zarr.open(str(store_path), mode="r")

    lut_meta = UTMZoneLUT(
        utm_epsg=z.attrs["utm_epsg"],
        resolution=z.attrs["resolution"],
        area_extent=tuple(z.attrs["area_extent"]),
        width=z.attrs["width"],
        height=z.attrs["height"],
        zarr_path=str(store_path),
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
            
            if (goes_min_lon <= east and goes_max_lon >= west and 
                goes_min_lat <= north and goes_max_lat >= south):
                utm_zones.append(epsg)
                
    # South
    for z in range(1, 61):
        epsg = 32700 + z
        crs = pyproj.CRS.from_epsg(epsg)
        area = crs.area_of_use
        if area:
            west, south, east, north = area.bounds
            if (goes_min_lon <= east and goes_max_lon >= west and 
                goes_min_lat <= north and goes_max_lat >= south):
                utm_zones.append(epsg)
                
    return sorted(utm_zones)


def compute_cell_slice(
    cell_utm_footprint_bounds: tuple[float, float, float, float],
    lut_area_extent: tuple[float, float, float, float],
    resolution: int,
) -> tuple[slice, slice]:
    """Compute row/col slices within a UTM zone LUT for a grid cell.

    Given the UTM footprint bounds of a grid cell and the area extent
    of the full UTM zone LUT, return (row_slice, col_slice) to extract
    exactly the grid cell's region from the LUT arrays.

    Parameters
    ----------
    cell_utm_footprint_bounds : (minx, miny, maxx, maxy)
        Bounds of the grid cell in its UTM CRS (meters).
    lut_area_extent : (minx, miny, maxx, maxy)
        Full extent of the UTM zone LUT (meters).
    resolution : int
        Pixel size in meters.

    Returns
    -------
    (row_slice, col_slice) : tuple of slices
        Slices into the 2D (height, width) LUT grid.
    """
    lut_minx, lut_miny, lut_maxx, lut_maxy = lut_area_extent
    cell_minx, cell_miny, cell_maxx, cell_maxy = cell_utm_footprint_bounds

    # Column indices (x-axis, left to right)
    col_start = int(round((cell_minx - lut_minx) / resolution))
    col_end = int(round((cell_maxx - lut_minx) / resolution))

    # Row indices (y-axis, top to bottom — maxy is row 0)
    lut_height = int(round((lut_maxy - lut_miny) / resolution))
    row_start = lut_height - int(round((cell_maxy - lut_miny) / resolution))
    row_end = lut_height - int(round((cell_miny - lut_miny) / resolution))

    return slice(row_start, row_end), slice(col_start, col_end)


def extract_cell_from_lut(
    source_data: np.ndarray,
    lut_group: zarr.Group,
    cell_row_slice: slice,
    cell_col_slice: slice,
    lut_height: int,
    lut_width: int,
) -> np.ndarray:
    """Extract a grid cell's data using pre-computed LUT arrays.

    Loads only the LUT chunks covering the cell region, then applies
    get_sample_from_neighbour_info for the cell sub-grid.

    Parameters
    ----------
    source_data : np.ndarray
        Flattened 1D source data from the GOES file.
    lut_group : zarr.Group
        Opened Zarr group containing valid_input_index, valid_output_index, index_array.
    cell_row_slice, cell_col_slice : slices
        Row and column slices within the full UTM zone LUT grid.
    lut_height, lut_width : int
        Full dimensions of the UTM zone grid.

    Returns
    -------
    np.ndarray
        2D array of shape (cell_height, cell_width) with extracted data.
    """
    # Convert 2D cell slices to 1D flat indices within the full LUT
    cell_height = cell_row_slice.stop - cell_row_slice.start
    cell_width = cell_col_slice.stop - cell_col_slice.start

    # Build flat index mask for the cell region within the full LUT
    rows = np.arange(cell_row_slice.start, cell_row_slice.stop)
    cols = np.arange(cell_col_slice.start, cell_col_slice.stop)
    row_grid, col_grid = np.meshgrid(rows, cols, indexing='ij')
    flat_indices = (row_grid * lut_width + col_grid).ravel()

    # Load only the needed chunks from Zarr
    valid_output = lut_group['valid_output_index'][flat_indices]
    index_arr = lut_group['index_array'][flat_indices]

    # Apply the LUT: for each valid output pixel, fetch source data at index_arr position
    result = np.full(cell_height * cell_width, np.nan, dtype=np.float32)
    valid_mask = valid_output.astype(bool)
    result[valid_mask] = source_data.ravel()[index_arr[valid_mask]]

    return result.reshape(cell_height, cell_width)
