This Markdown file is designed to be provided to an LLM to explain the storage architecture without providing the implementation details.

***

# GnitzDB Storage Subsystem Overview (`gnitz/storage/`)

This document provides a high-level catalog and functional description of the internal storage implementation for GnitzDB. 

## ⚠️ MANDATORY INSTRUCTIONS FOR THE LLM ⚠️
1. **NO ASSUMPTIONS:** Do not assume the internal implementation details, method signatures, or logic flow of the files described below.
2. **NO HALLUCINATION:** Do not invent or "fill in" code for these files based on the descriptions provided.
3. **REQUEST CLARIFICATION:** If your analysis requires the specific implementation, logic, or data structures contained within any of these files to provide an accurate answer, you **MUST explicitly ask the user to provide the content of that specific file.**
4. **TREAT AS BLACK BOX:** Unless the source code is provided in the current context, treat these modules as black-box components that perform the roles described below.

---

## File Catalog

### 1. Core Orchestration
*   **`engine.py`**: The primary coordinator for the storage layer. It manages the interaction between the MemTable, the Spine (persistent shards), the Manifest, and the WAL. It handles ingestion, recovery, and the high-level flush/rotate lifecycle.
*   **`spine.py`**: Manages the "Spine," an in-memory index of all active persistent shards. It provides efficient range-based lookup of shards to resolve Entity IDs across multiple physical files.

### 2. In-Memory & Ingestion
*   **`memtable.py`**: Implements the high-velocity ingestion layer. It uses a row-oriented SkipList backed by Monotonic Arenas to perform in-place algebraic summation of Z-Set weights.
*   **`wal.py` & `wal_format.py`**: Implements the Write-Ahead Log. `wal.py` handles the append-only I/O and durability (fsync) logic, while `wal_format.py` defines the binary serialization of Z-Set batches.

### 3. Persistent Shard Format (Penta-Partition)
*   **`writer_ecs.py`**: The "Unzipping" pipeline. It transforms row-oriented MemTable data into the columnar "Penta-Partition" shard format (Regions E, W, C, B, and Header).
*   **`shard_ecs.py`**: The reader for persistent shards. It utilizes memory-mapping to provide high-performance, random access to the columnar regions and implements German String decoding.
*   **`layout.py`**: Defines the physical constants, magic numbers, and header offsets for the shard binary format.

### 4. Metadata & Consistency
*   **`manifest.py`**: Manages the `MANIFNGT` binary manifest. It ensures atomic, crash-safe updates to the database state using a temporary swap-file and `rename()` mechanics.
*   **`shard_registry.py`**: Tracks active shards for a component and monitors "Read Amplification" metrics to determine when a component requires compaction.
*   **`refcount.py`**: A distributed reference counter that ensures physical files are only unlinked from the disk after all reader processes have released their handles.

### 5. Compaction & Merging
*   **`compactor.py`**: Orchestrates the compaction lifecycle. It selects candidate shards, executes the merge, and updates the Manifest.
*   **`compaction_logic.py`**: Contains the core algebraic merge logic. It sums weights, resolves values via LSN, and implements the "Ghost Property" (annihilating records with a net weight of zero).
*   **`tournament_tree.py`**: Implements an N-way merge-sort primitive used by the compactor to efficiently process multiple sorted shards simultaneously.

### 6. Low-Level Infrastructure
*   **`arena.py`**: Implements a zero-allocation, bump-pointer monotonic allocator for GC-independent memory management.
*   **`buffer.py`**: Provides a safe wrapper around raw memory pointers for bounds-checked reads of primitive types.
*   **`mmap_posix.py`**: Provides RPython-compatible wrappers for POSIX memory-mapping and file synchronization (`mmap`, `munmap`, `fsync`).
*   **`errors.py`**: Defines the exception hierarchy for the storage subsystem (e.g., `CorruptShardError`, `MemTableFullError`).
