# 🚀 aer-extract-aws-goes

Welcome to the **aer-extract-aws-goes** plugin! This repository provides an extractor plugin for the `aer` ecosystem to download and process GOES ABI satellite data from AWS.

## ⚡ Overview

This plugin implements the `AwsGoesExtractor` class which inherits from the `aer.interfaces.Extractor` base class. It enables seamless extraction, resampling, and grid-based storage of GOES satellite granules.

### Key Features
- **Automated Download**: Fetches NetCDF granules anonymously from AWS S3.
- **satpy Integration**: Automatically detects and builds `satpy` Scenes (supports ABI-L1b and ABI-L2 collections).
- **Grid Resampling**: Resamples data to predefined overlapping grid cells using LUT-cached nearest-neighbor interpolation with pixel-perfect numerical parity.
- **GeoTIFF / NetCDF Saving**: Outputs standard artifacts following the **EOIDS** (Earth Observation Imaging Data Structure) directory standard (`loc > date > sat`) and filename convention to preserve precise start/end temporal metadata and prevent filesystem performance degradation.
- **Concurrent Batching**: Extracts grid cells in parallel across multiple worker threads and processes.

### Supported Collections
- `ABI-L1b-RadC`, `ABI-L1b-RadF`, `ABI-L1b-RadM`
- `ABI-L2-AODC`, `ABI-L2-AODF`
- `ABI-L2-BRFC`, `ABI-L2-BRFF`, `ABI-L2-BRFM`

---

## 🛠️ Usage

The plugin provides the following entry points for the `aer` ecosystem:

```toml
[project.entry-points."aer.plugins"]
extract_aws_goes = "aer.extract_aws_goes.core:AwsGoesExtractor"
```

The extractor can be instantiated natively via the `aer` plugin system, which handles routing the search results to the extraction task seamlessly.

### Example (via aer core)

```python
from aer.repository import get_extractor

# Initialize the extractor
extractor = get_extractor("extract_aws_goes", target_grid_d=100_000)

# Tasks generated from search results
tasks = extractor.prepare_for_extraction(
    search_results=search_results, 
    uri="/path/to/extracted/items"
)

# Extract artifacts to grid cells
artifacts_df = extractor.extract_batches(tasks, extract_params={"max_workers": 8})
```

### 🚅 High-Performance LUT Engine

This plugin includes a high-performance extraction engine that uses pre-computed **Look-Up Tables (LUTs)** to achieve near-zero projection overhead during extraction.

#### Features
- **Zero Reprojection**: Uses pre-calculated nearest-neighbor indices stored in NumPy .npz format.
- **Lazy Loading**: Only the chunks of the LUT covering your specific Area of Interest (AOI) are loaded.
- **Auto-Download**: If a LUT for a specific UTM Zone and resolution is missing locally, the plugin automatically fetches it from the GitHub Release assets.
- **Scientific Parity**: Achieving identical results to Satpy's nearest-neighbor resampling through precision integer-based coordinate slicing.

#### Usage
To use the LUT engine, set the `engine` parameter to `"lut"`. You can optionally specify a `lut_dir` (defaults to `~/.cache/aer/extract-aws-goes/luts`).

```python
# Extract using the LUT engine
artifacts_df = extractor.extract(
    task, 
    extract_params={
        "engine": "lut",
        "calibration": "reflectance", # 'counts' (raw), 'radiance', 'reflectance', or 'brightness_temperature'
    }
)
```

### 🌍 ODC-Geo Reprojection Engines

For areas where a LUT is not available, or for dynamic reprojection, the plugin provides high-performance engines based on `odc-geo`.

- **`odc_cell` (Default)**: Performs reprojection on a per-grid-cell basis. This is the **fastest** option for small Areas of Interest (AOIs) or when all grid cells fall within a **single UTM zone**, as it avoids the overhead of zone-based grouping.
- **`odc_zone`**: Groups grid cells by UTM zone and performs one large reprojection per zone, then slices individual cells. This is highly optimized for **large AOIs** spanning multiple UTM zones (e.g., 5+ zones), significantly reducing the number of expensive `odc.reproject` calls.
- **`pyresample`**: The canonical reference engine. Slowest, but used to verify scientific parity.

```python
# Extract using the ODC cell engine (default)
artifacts_df = extractor.extract(
    task, 
    extract_params={"engine": "odc_cell"}
)
```

#### LUT Distribution
LUTs are organized as zipped .npz files and published as assets in the GitHub Releases. 

By default, **the plugin handles everything automatically**. When you request an extraction, it dynamically identifies the satellite and product combination (e.g., `goes19_radf`), checks if you have the required LUT locally, and downloads the ZIP from the latest GitHub release if missing. It extracts the files directly to your cache directory (default `~/.cache/aer/extract-aws-goes/luts`).

**Offline Environments / Manual Setup:**
If you are running the extractor in an air-gapped environment or want to pre-seed your cache:
1. Navigate to the [Releases page](https://github.com/frandorr/aer-extract-aws-goes/releases) and download the required ZIP files (e.g., `goes19_radf_32720_1000m.npz.zip`).
2. Unzip the contents into your `lut_dir`. The structure must match: `<lut_dir>/<combo>/<utm_epsg>/<resolution>m.npz`. 
   *(Example: `~/.cache/aer/extract-aws-goes/luts/goes19_radf/32720/1000m.npz`)*
3. Run your extraction. The system will detect the local directory and skip the download.

#### Generating Custom LUTs
If you need to generate LUTs for a specific UTM zone or resolution not available in the official releases:

```bash
# Install the package and run the generator
generate-luts \
    --goes-file /path/to/OR_ABI-L1b-RadF-M6C01_G19_s202312312345678.nc \
    --output-dir ./luts \
    --resolutions 500 1000 2000 \
    --utm-zones 32620 32621
```

The generator will print a JSON snippet at the end. Add this snippet to `components/aer/extract_aws_goes/data/lut_config.json` to enable the "zero-S3-read" optimization for the new LUTs.

---

## 🏗️ Architecture

This project uses a **Polylith** structure. Code is organized into:
- **Components**: The core logic under `components/`.
- **Projects**: Deployable packaging under `projects/`.

## 📚 Documentation

This project adheres to the **Google Docstring style guide**. All public APIs are documented with detailed `Args`, `Returns`, and `Raises` sections to ensure clarity and ease of use for integrated AI coding assistants.

## 📜 License

This plugin is licensed under the [MIT License](LICENSE).