# GnitzDB Test Suite Summary

> **⚠️ IMPORTANT INSTRUCTION FOR LLM AGENTS:**
> The summaries below describe the logic and coverage of the GnitzDB test suite. 
> 
> **Do not attempt to modify, refactor, or debug these files based on these descriptions alone.**
> 
> Most files have been significantly updated to support Z-Set algebraic logic (Weights) and Fragmented LSM (FLSM) features. If you need to work with a specific test, you must explicitly request:
> *"Please serve the content of [filename.py] so I can read it."*
>
> Hallucinating the internal structure of these tests will lead to translation failures in the RPython toolchain.

## 1. Integration & Lifecycle
*   **`translate_test.py`**: The primary end-to-end integration test designed for RPython translation. It simulates the entire database lifecycle:
    *   **Ingestion:** Inserts data into the `MemTable` and flushes multiple overlapping shards to disk.
    *   **Algebraic Resolution:** Verifies that the `Engine` correctly sums weights across `MemTable` and multiple `Spine` shards (Z-Set logic).
    *   **Manifest Management:** Tests atomic manifest updates and versioned snapshots.
    *   **Compaction:** Triggers an automated "Vertical Merge" based on read-amplification heuristics.
    *   **Physical Pruning:** Verifies the **Ghost Property**—ensuring that entities with a net weight of zero are physically removed from the resulting shard.
    *   **Cleanup:** Verifies that the `RefCounter` safely deletes obsolete shard files only after all handles are released.

## 2. Core Data Structures & Memory
*   **`test_types.py`**: Validates `ComponentLayout`. Verifies C-style alignment, padding, and stride calculations for fixed-width component records.
*   **`test_strings.py`**: Tests "German String" (16-byte hybrid) logic. Covers inline short strings, blob-heap long strings, and O(1) prefix-based equality failures.
*   **`test_arena.py`**: Tests the monotonic bump-pointer allocator for memory exhaustion and 8-byte alignment enforcement.
*   **`test_buffer.py`**: Tests `MappedBuffer` bounds-checking for raw pointer access.

## 3. Storage Layer (ECS Shards)
*   **`test_layout.py`**: Validates physical file constants, including Magic Numbers and the Quad-Partition offsets (Regions E, W, C, B).
*   **`test_writer_ecs.py`**: Tests `ECSShardWriter`. Verifies the serialization of Entity IDs, **Algebraic Weights (i64)**, Component Data, and the relocation of string pointers into the Blob Heap.
*   **`test_shard_ecs.py`**: Tests `ECSShardView`. Verifies binary search lookups on `Region E` and raw weight retrieval from `Region W`.

## 4. Manifest & Shard Metadata
*   **`test_manifest_format.py`**: Validates the binary encoding of the Manifest header and entry records.
*   **`test_manifest_io.py`**: Tests `ManifestWriter` and `ManifestReader` for sequential and indexed access to shard metadata.
*   **`test_manifest_versioning.py`**: Tests `ManifestManager` atomic swap-file mechanics (Rename-based atomicity).
*   **`test_spine_manifest.py`**: Verifies that the `Spine` can filter shards by Component ID and correctly identify **all** shards overlapping a specific Entity ID.

## 5. Algebraic Logic & Compaction
*   **`test_compaction_logic.py`**: Tests `MergeAccumulator`. Verifies:
    *   **Annihilation:** $W_{net} = 0$.
    *   **Accumulation:** $W_{net} > 1$.
    *   **LSN Resolution:** Ensuring the value from the highest Log Sequence Number (LSN) wins during a merge.
*   **`test_tournament_tree.py`**: Tests the K-Way Merge Sort foundation. Verifies the `TournamentTree` correctly yields minimal Entity IDs across $N$ sorted streams.
*   **`test_compactor.py`**: Tests the physical `compact_shards` process. Ensures multiple input files are correctly transformed into a single, algebraically-reduced output shard.
*   **`test_compaction_heuristics.py`**: Tests `CompactionPolicy`. Verifies that compaction is triggered automatically when **Read Amplification** (overlap depth) exceeds the configured threshold.

## 6. Engine & Orchestration
*   **`test_engine_summation.py`**: Focused test for `Engine.get_effective_weight`. Specifically validates that weights are summed correctly when an entity exists in both the `MemTable` and multiple persistent shards.
*   **`test_shard_registry.py`**: Verifies the global tracking of shards, read-amplification metrics, and component-level compaction flags.
*   **`test_refcount.py`**: Verifies that shard files are protected from deletion (`unlink`) as long as they are mapped by a `Spine` or `Compactor`.

## 7. Infrastructure
*   **`test_mmap_posix.py`**: Validates the RPython FFI wrappers for `mmap`, `munmap`, and `msync`.
*   **`test_errors.py`**: Validates the custom exception hierarchy (e.g., `CorruptShardError`, `BoundsError`).
