import re
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast, Sequence

import cattrs
from attrs import field, frozen, validators

import numpy as np
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
    (and therefore with identical geostationary area definitions) share properties:
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



