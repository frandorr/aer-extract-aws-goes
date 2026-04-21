# Requirements: aer-extract-aws-goes

## Core Features (REQ-01)
- [ ] **REQ-01.1**: The plugin must accept a `GeoDataFrame[SearchResultSchema]` as input.
- [ ] **REQ-01.2**: It must resolve the download logic via `aer.download_api`.
- [ ] **REQ-01.3**: It must save files to a specified `dest_dir`.

## Output Formats (REQ-02)
- [ ] **REQ-02.1**: Support standard binary file extraction.
- [ ] **REQ-02.2**: [NEW] Support Parquet format for metadata or processed results (using `pyarrow`).

## Plugin Registration (REQ-03)
- [ ] **REQ-03.1**: Standard `aer.plugin` category must be `extract`.
- [ ] **REQ-03.2**: Plugin name must be `aws_goes`.
- [ ] **REQ-03.3**: Entry point must point to the plugin function.

## Verification & Testing (REQ-04)
- [ ] **REQ-04.1**: Unit tests for the core extraction logic.
- [ ] **REQ-04.2**: Integration tests for verifying search result processing.

## UTM Zone LUT Extraction (REQ-08)
- [x] **REQ-08.1**: Offline LUT generator that computes GOES→UTM reprojection indices per UTM zone at configurable resolutions (500m, 1000m, 2000m).
- [x] **REQ-08.2**: LUT storage in a chunked, fast-access format (Zarr) that supports lazy loading of specific UTM zones without loading the entire dataset.
- [ ] **REQ-08.3**: New extraction engine (`engine="lut"`) that, given a grid_cell, loads the corresponding UTM zone LUT and extracts data via array index slicing — no runtime reprojection.
- [x] **REQ-08.4**: Multi-resolution support: LUTs must be generated and loadable for 500m, 1000m, and 2000m target resolutions.

