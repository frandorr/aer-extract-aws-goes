"""High-performance GOES data extraction using pre-computed LUTs."""

from .core import (
    AwsGoesExtractor,
)
from .utils import (
    detect_reader,
    map_channel_ids_to_satpy_names,
    detect_goes_utm_zones,
)
from .lut import (
    UTMZoneLUT,
    generate_utm_zone_lut,
    load_utm_zone_lut,
    SUPPORTED_RESOLUTIONS,
)

__all__ = [
    "AwsGoesExtractor",
    "detect_reader",
    "map_channel_ids_to_satpy_names",
    "generate_utm_zone_lut",
    "load_utm_zone_lut",
    "detect_goes_utm_zones",
    "SUPPORTED_RESOLUTIONS",
    "UTMZoneLUT",
]
