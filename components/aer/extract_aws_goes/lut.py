import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import attrs
import numpy as np
import pyproj
import zarr
from pyresample.geometry import AreaDefinition
from pyresample.kd_tree import get_neighbour_info

SUPPORTED_RESOLUTIONS = (500, 1000, 2000)
DEFAULT_RADIUS_OF_INFLUENCE = 50_000  # meters, for pyresample kd-tree search

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


def load_utm_zone_lut(lut_dir: Path, utm_epsg: int, resolution: int) -> tuple[UTMZoneLUT, Any]:
    store_path = Path(lut_dir) / str(utm_epsg) / f"{resolution}m.zarr"
    z = zarr.open(str(store_path), mode='r')
    
    lut_meta = UTMZoneLUT(
        utm_epsg=z.attrs["utm_epsg"],
        resolution=z.attrs["resolution"],
        area_extent=tuple(z.attrs["area_extent"]),
        width=z.attrs["width"],
        height=z.attrs["height"],
        zarr_path=str(store_path)
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
