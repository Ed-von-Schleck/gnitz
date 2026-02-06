# GnitzDB Test Suite Summary

> **⚠️ IMPORTANT INSTRUCTION FOR LLM AGENTS:**
> The full source code for the test files listed below has been omitted from the context window to save space.
> 
> **Do not attempt to modify these files based on assumptions.**
> 
> If you need to modify, debug, or analyze a specific test file, you must explicitly request:
> *"Please serve the content of [filename.py] so I can read it."*
>
> Once served, you may proceed with modifications.

## Overview
The GnitzDB test suite covers the storage engine, memory management, algebraic logic (DBSP), and RPython integration. The tests are split into unit tests for specific components and an integration entry point for the RPython translator.

## File Descriptions

### Integration & Translation
*   **`translate_test.py`**: The primary integration test entry point for the RPython toolchain. It runs a phased verification of the entire engine:
    *   **Phase 0:** Core ECS Engine (MemTable -> Spine lookup).
    *   **Phase 1:** Manifest System, Shard Registry, RefCounting, and Tournament Tree.
    *   It simulates a full lifecycle: Insert -> Flush -> Load -> Query -> Compact.

### Core Data Structures
*   **`test_types.py`**: Verifies `ComponentLayout`. Ensures correct calculation of byte offsets, alignment (padding), and strides for primitive types.
*   **`test_strings.py`**: Tests the "German String" implementation (`strings.py`). Verifies the 16-byte struct packing, short-string optimization (inline), long-string heap allocation, and the prefix-based equality check.
*   **`test_arena.py`**: Tests the `Arena` bump-pointer allocator for memory exhaustion and alignment.
*   **`test_buffer.py`**: Tests `MappedBuffer` for memory safety and bounds checking logic.

### Storage & I/O
*   **`test_writer_ecs.py`**: Tests `ECSShardWriter`. Verifies that data is correctly serialized into the Columnar format (Regions E, C, and B), including correct handling of string relocation.
*   **`test_shard_ecs.py`**: Tests `ECSShardView`. Verifies reading headers, binary searching Entity IDs, and retrieving component data from memory-mapped files.
*   **`test_mmap_posix.py`**: Tests the RPython `mmap` wrappers and error handling.
*   **`test_layout.py`**: Verifies physical file constants (Magic Numbers, Header Offsets).

### Ingestion & Memory Table
*   **`test_memtable_ecs.py`**: Tests `MemTableManager` and `MemTable`. Key scenarios include:
    *   SkipList insertion.
    *   **Algebraic Annihilation:** Ensuring records with a net weight of 0 are dropped.
    *   Flushing memory arenas to disk.

### Manifest & Metadata
*   **`test_manifest_io.py`**: Tests `ManifestWriter` and `ManifestReader`. Verifies low-level binary read/write of manifest entries.
*   **`test_manifest_format.py`**: Validates magic numbers, header encoding, and entry truncation.
*   **`test_manifest_versioning.py`**: Tests `ManifestManager`. Verifies atomic updates (swap-file mechanics) and version consistency.
*   **`test_spine_manifest.py`**: Tests the `Spine`. Verifies loading `ShardHandles` from a manifest and filtering shards by Component ID.

### Compaction & Lifecycle
*   **`test_shard_registry.py`**: Tests `ShardRegistry`. Verifies logic for:
    *   Finding overlapping shards.
    *   Calculating **Read Amplification**.
    *   Triggering compaction thresholds.
*   **`test_tournament_tree.py`**: Tests the K-Way Merge Sort logic (`TournamentTree` and `StreamCursor`). Covers:
    *   Merging sorted streams.
    *   Handling identical Entity IDs across multiple shards.
    *   Stream exhaustion.
*   **`test_refcount.py`**: Tests `RefCounter`. Verifies safe deletion of files, ensuring files are only `unlink`ed when reference counts drop to zero.

### Miscellaneous
*   **`test_errors.py`**: Validates the custom exception hierarchy (`StorageError`, `CorruptShardError`, etc.).
