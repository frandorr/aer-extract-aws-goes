# 🚀 aer-extract-aws-goes

Welcome to the **aer-extract-aws-goes** plugin! This repository provides an extractor plugin for the `aer` ecosystem to download and process GOES ABI satellite data from AWS.

## ⚡ Overview

This plugin implements the `AwsGoesExtractor` class which inherits from the `aer.interfaces.Extractor` base class. It enables seamless extraction, resampling, and grid-based storage of GOES satellite granules.

### Key Features
- **Automated Download**: Fetches NetCDF granules anonymously from AWS S3.
- **satpy Integration**: Automatically detects and builds `satpy` Scenes (supports ABI-L1b and ABI-L2 collections).
- **Grid Resampling**: Resamples data to predefined overlapping grid cells using LUT-cached nearest-neighbor interpolation.
- **GeoTIFF / NetCDF Saving**: Outputs standard artifacts that preserve geospatial metadata.
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

---

## 🏗️ Architecture

This project uses a **Polylith** structure. Code is organized into:
- **Components**: The core logic under `components/`.
- **Projects**: Deployable packaging under `projects/`.

## 📜 License

This plugin is licensed under the [MIT License](LICENSE).