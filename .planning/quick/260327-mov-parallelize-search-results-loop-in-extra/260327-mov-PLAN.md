# Quick Task 260327-mov: Parallelize search_results loop in extract_aws_goes - Plan

**Status:** Ready for Execution

<must_haves>
- [ ] Concurrent execution of the `search_results` loop using `ThreadPoolExecutor`.
- [ ] Thread-safe handling of `satpy.Scene` (using `copy()` per thread).
- [ ] Thread-safe LUT generation (using a lock to avoid simultaneous file writes).
- [ ] Proper error propagation and status return.
- [ ] Import `concurrent.futures.ThreadPoolExecutor` and `as_completed`.
</must_haves>

<tasks>

### Task 1: Refactor loop into worker function and parallelize
- **Files**: `components/aer/extract_aws_goes/core.py`
- **Action**: 
    1. Import `threading` and `concurrent.futures`.
    2. Define `lut_lock = threading.Lock()`.
    3. Define worker function `_extract_single(sr, source_data, template, scene_cp, lut_cache_dir, mapped, granule_id, first_sr, task)`.
    4. Move the loop body into the worker.
    5. Wrap `get_or_build_lut` in `with lut_lock:`.
    6. Replace the `for` loop with `ThreadPoolExecutor`.
- **Verify**: Code compiles and `extract_aws_goes` still returns `ExtractionTask`.
- **Done**: Worker function exists and is called in parallel.

### Task 2: Implement thread-safe saving
- **Files**: `components/aer/extract_aws_goes/core.py`
- **Action**:
    1. Inside the worker, use `local_scene = scene_cp.copy()` instead of the shared one.
    2. Perform `save_dataset` on `local_scene`.
- **Verify**: `scene_cp` is no longer mutated directly by multiple threads.
- **Done**: Local scene copy used for saving.

### Task 3: Handle error propagation
- **Files**: `components/aer/extract_aws_goes/core.py`
- **Action**:
    1. Update the loop to collect futures.
    2. Check results of all futures. If any fail, return `ExtractionStatus.FAILED`.
- **Verify**: If a single result fails, the entire task reports failure.
- **Done**: Correct status return.

</tasks>
