# GnitzDB

GnitzDB is a persistent, embedded database engine built using RPython. It implements a **Database Stream Processing (DBSP)** architecture, treating all data changes as operations over an Abelian Group. The system uses **Z-Sets** (Generalized Multisets) as its fundamental primitive, mapping records to integer weights.

This project utilizes the RPython toolchain to generate a high-performance native binary with a Meta-Tracing JIT compiler, optimizing execution paths at runtime.

## Core Design Principles

### Theoretical Framework
GnitzDB deviates from traditional CRUD models. Instead of destructive updates, it uses an append-only algebraic model:
*   **Z-Sets:** Data is represented as `(Key, Payload) -> Weight`.
*   **Algebraic Consistency:** State transitions are additive. Inserting a record adds weight `+1`; deleting a record adds weight `-1`.
*   **The Ghost Property:** Records with a net weight of zero are physically "annihilated." The storage engine guarantees that zero-weight records are bypassed during I/O and do not consume CPU cycles during computation.

### Architecture
*   **Hybrid Layout:**
    *   **Ingestion (MemTable):** In-memory data is stored in a row-oriented (Array of Structures) SkipList for fast random write access.
    *   **Persistence (Storage):** Data is flushed to disk in an N-Partition Columnar format (Structure of Arrays) optimized for scan performance and SIMD vectorization.
*   **Fragmented LSM (FLSM):** Storage is managed via a Log-Structured Merge tree. Unlike traditional LSMs using Last-Write-Wins, GnitzDB performs a "Pure Z-Set Merge," algebraically summing weights of matching records during compaction.
*   **Strict Typing:** Primary keys are restricted to `u64` or `u128` to ensure register-sized comparisons and eliminate pointer chasing in hot loops.
*   **German Strings:** Variable-length strings use a 16-byte inline structure (Length, Prefix, Pointer/Payload). This allows for $O(1)$ equality checks for most non-matching strings without following heap pointers.

## Project Structure

The codebase is organized into core primitives, storage logic, and execution engine components.

### Core Modules
*   **`gnitz/core/types.py`**: Defines the static schema system (`TableSchema`, `ColumnDefinition`) and primitive field types.
*   **`gnitz/core/zset.py`**: The high-level public API (`PersistentTable`).
*   **`gnitz/core/values.py`**: The `TaggedValue` union type used for passing dynamic data through the RPython static type system.
*   **`gnitz/core/strings.py`**: Implementation of German String logic and comparison kernels.
*   **`gnitz/core/checksum.py`**: XXH3-64 implementation for data integrity.

### Storage Engine
*   **`gnitz/storage/engine.py`**: Central orchestrator managing the MemTable, WAL, and Shard Index.
*   **`gnitz/storage/memtable.py`**: The mutable in-memory SkipList. Uses a custom bump-pointer arena allocator.
*   **`gnitz/storage/wal.py` & `wal_format.py`**: Write-Ahead Log implementation. Handles crash recovery via LSN (Log Sequence Number) replay.
*   **`gnitz/storage/shard_table.py`**: Reader for the N-Partition columnar shard format (mmap).
*   **`gnitz/storage/writer_table.py`**: Writer that transmutes row-oriented MemTable data into columnar shards ("Unzipping").
*   **`gnitz/storage/compactor.py`**: Performs N-way merge sort of overlapping shards, summing weights and discarding ghosts.
*   **`gnitz/storage/manifest.py`**: Manages the atomic registry of active shards to ensure a consistent snapshot view.

### Low-Level Primitives
*   **`gnitz/storage/buffer.py`**: Memory management abstractions (`Arena`, `MappedBuffer`).
*   **`gnitz/storage/mmap_posix.py`**: RPython interfaces for POSIX `mmap`, `flock`, and `fsync`.
*   **`gnitz/storage/tournament_tree.py`**: Min-heap implementation for merging sorted runs.

## Building and Running

GnitzDB requires the RPython toolchain (part of the PyPy project) to build. It cannot be run as a standard Python script due to its use of `rpython` specific libraries (`rlib`, `rffi`, `lltype`).

### Prerequisites
1.  Python 2.7 (required for the RPython translation chain).
2.  A checkout of the PyPy source code (to access `rpython`).

### Compilation
To translate the execution entry point (`translate_test.py`) into a native binary:

```bash
# Assuming PYTHONPATH includes the path to the PyPy source
rpython --batch translate_test.py
```

This will produce a binary executable (e.g., `translate_test-c`) linked against the RPython runtime.

### Usage Example

The `translate_test.py` file demonstrates the lifecycle of the engine. A simplified usage pattern follows:

```python
from gnitz.core import zset, types, values

# 1. Define Schema: PK (i64) + Column (String)
layout = types.TableSchema(
    [types.ColumnDefinition(types.TYPE_I64), 
     types.ColumnDefinition(types.TYPE_STRING)],
    pk_index=0
)

# 2. Initialize Database
db = zset.PersistentTable("/path/to/db", "my_table", layout)

# 3. Create Payload
payload = [values.TaggedValue.make_string("data")]

# 4. Insert (Weight +1)
db.insert(100, payload)

# 5. Flush to Disk (Creates Columnar Shard)
db.flush()

# 6. Query Weight
weight = db.get_weight(100, payload)
# weight should be 1
```

## Implementation Status

Based on the design specification, the following components are implemented:

*   **Storage primitives:** Arenas, Buffers, MMap wrappers.
*   **Data Types:** `u64`, `u128` (UUID), Strings (German style), Integers, Floats.
*   **Ingestion:** Row-oriented MemTable with algebraic coalescing.
*   **Durability:** WAL with LSN tracking and checksum validation.
*   **Persistence:** N-Partition Columnar Shard writer/reader.
*   **Compaction:** Table-scoped N-way merge with ghost annihilation.
*   **Manifest:** Atomic versioning of the shard set.

**Pending/Future Work:**
*   **DBSP Virtual Machine:** The register-based VM described in the spec is not yet present.
*   **SQL Compiler:** Integration with Apache Calcite.
*   **Network Distribution:** The replication layer for syncing Z-Sets over the network.

## License

MIT License
