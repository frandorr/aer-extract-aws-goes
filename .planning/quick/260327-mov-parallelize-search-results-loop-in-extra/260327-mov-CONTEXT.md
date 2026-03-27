# Quick Task 260327-mov: Parallelize search_results loop in extract_aws_goes - Context

**Gathered:** 2026-03-27
**Status:** In Discussion

<domain>
## Task Boundary

The user wants to parallelize the `for sr in search_results` loop in `extract_aws_goes` using `ThreadPoolExecutor`.
The main concern is `scene_cp[mapped[0]] = resampled_da` and `scene_cp.save_dataset`.

</domain>

<decisions>
## Implementation Decisions

### Thread-Safe Saving
- Locked saving on the original `scene_cp` OR personal copy for each thread.
- *Decision*: Each thread's task will create a local copy of `scene_cp` using `.copy()`. This avoids a lock and allows parallel I/O for saving.

### Error Handling
- Return status: If any thread fails, we should return `ExtractionStatus.FAILED`.
- *Decision*: Use `as_completed` and catch exceptions. If one fails, the overall task fails. Better than returning early so others finish (or we could cancel them if needed, but finishing is safer in some cases).

### Number of Workers
- Worker pool size management.
- *Decision*: Default to `None` (let `ThreadPoolExecutor` decide based on CPU) or a reasonable fixed number like 4-8.

</decisions>
