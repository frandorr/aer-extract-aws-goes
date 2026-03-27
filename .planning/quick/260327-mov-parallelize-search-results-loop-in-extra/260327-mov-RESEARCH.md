# Quick Task 260327-mov: Research - Parallelizing GOES Extraction

## Findings: Parallelism in Satpy

### `satpy.Scene.copy()`
- Creating a copy of a `Scene` is lightweight if datasets are already loaded (it copies the dataset container, not necessarily the deep data if it's backed by `dask` or a memory-view).
- In our case, `source_data` is a pre-computed numpy array. `scene_cp[mapped[0]] = resampled_da` *sets* new data.
- If each thread gets a local `scene.copy()`, they can perform `save_dataset` in parallel safely.

### `ThreadPoolExecutor` vs `ProcessPoolExecutor`
- `apply_lookup_table` uses `pyresample`'s nearest-neighbour resampling. This is largely CPU-bound.
- However, the overall task includes I/O (loading LUT, saving data).
- `ThreadPoolExecutor` is what the user asked for.
- `pyresample`'s nearest-neighbour resampling might release the GIL if it's using the right backend.

### Memory Concerns
- If many threads are active, they will all hold `resampled_da` (the same size as the output grid).
- For a 5424x5424 grid (full disk), this is ~117 MB per channel (float32).
- With 4 workers, that's ~470 MB. This is fine on most systems.
- If we had many large grids, we might need a semaphore or a smaller pool.

### `save_dataset` Thread Safety
- `satpy.Scene.save_dataset` uses writers (usually `cf` for netCDF).
- The `cf` writer is generally thread-safe if saving to *different* filenames (which we are).

## Implementation Recommendation
- Define a worker function `_extract_single(sr, source_data, template, scene_cp)`
- Use `scene_cp.copy()` inside the worker to avoid race conditions.
- Collect results via `as_completed`.

## Potential Pitfalls
- `scene_cp` modification: Modifying a shared `Scene` from multiple threads will definitely cause issues.
- `logger` (Structlog): Typically thread-safe.
- Memory: Monitor memory if many threads are used.
- LUT builds: `get_or_build_lut` might build the same LUT simultaneously if two threads hit a cache miss for the same area. We should use a lock or `mkdir(exist_ok=True)` logic carefully.
  - Wait, if it's building the same LUT, one thread might overwrite the other's half-written file.
  - We should probably add a local lock around `get_or_build_lut` per area or just for any miss.

## Summary
- `ThreadPoolExecutor` is viable.
- Use `Scene.copy()` per task.
- Be careful with simultaneous LUT builds (use a lock).
