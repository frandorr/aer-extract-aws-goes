# Roadmap: aer-extract-aws-goes (Milestone: v0.1.0)

## Phase 1: Baseline Plugin Functionality (Status: Complete)
- [x] Goal: Establish a fully working extraction plugin for standard AWS GOES results.
- [x] Plans:
  - [x] Initialize `PROJECT.md`, `REQUIREMENTS.md`, `ROADMAP.md` (Done).
  - [x] Verify `components/aer/extract_aws_goes/core.py` registers correctly.
  - [x] Implement robust unit tests for the core logic.
  - [x] Update `pyproject.toml` entry points if needed.

## Phase 2: Parquet Output Support (Status: Complete)
- [x] Goal: Add support for saving extraction metadata or results as Parquet.
- [x] Plans:
  - [x] Integrate `pyarrow` and `pyarrow.parquet`.
  - [x] Design extraction logic that utilizes Parquet for metadata or results.
  - [x] Update plugin tests to verify Parquet output.

## Phase 3: Enhanced Extraction & Error Handling (Status: Complete)
- [x] Goal: Implement more sophisticated extraction logic and error management.
- [x] Plans:
  - [x] Add error handling for failed downloads.
  - [x] Support custom extraction parameters in search results.
  - [x] Performance optimization for bulk extractions.

### Phase 4: Improve LUT extraction performance and compare in bench_performance.py (Status: Complete)

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 3
**Plans:** 0 plans

Plans:
- [x] TBD (run /gsd-plan-phase 4 to break down)

### Phase 5: Refactor extract_aws_goes to use Satpy Scene slicing/subsetting with LUT resampling, benchmarked via local/bench_performance.py (Status: Complete)

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 4
**Plans:** 0 plans

Plans:
- [x] TBD (run /gsd-plan-phase 5 to break down)

### Phase 6: Refactor GOES extractor to Extractor abstract class plugin system (Status: Complete)

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 5
**Plans:** 2/2 plans complete

Plans:
- [x] TBD (run /gsd-plan-phase 6 to break down)