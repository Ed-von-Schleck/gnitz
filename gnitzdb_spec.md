## 1. Introduction and Theoretical Foundation

### 1.1. Theoretical Framework: DBSP and Universal Z-Sets
GnitzDB is built on **Database Stream Processing (DBSP)**, a mathematical framework that treats all computations as operations over an **Abelian Group** $(G, +, 0)$. 
*   **Unified Data Representation:** The system utilizes **Z-Sets** (Generalized Multisets) as the universal primitive for all data. A Z-Set maps records—represented as (Primary Key, Row Payload) pairs—to integer weights $w \in \mathbb{Z}$.
*   **Algebraic Homomorphism:** Every state transition, persistent log entry, and distribution delta is expressed as a Z-Set. By utilizing the linearity of DBSP operators, the engine maintains global consistency through the identity:
    $$ Q(\sum \Delta I) = \sum Q(\Delta I) $$
*   **The Unified Log:** The Write-Ahead Log (WAL), Operator Traces, and Synchronization streams are mathematically equivalent. This algebraic uniformity allows the engine to treat ingestion, maintenance, and distribution as a single Z-Set transformation pipeline.

### 1.2. The Reactive Relational Columnar Model
The system utilizes a **Relational Columnar** architecture optimized for high-velocity state changes and reactive materialized views.
*   **Primary Keys (Clustering Keys):** Restricted to 64-bit (`u64`) or 128-bit (`u128`) unsigned integers. This restriction enables $O(1)$ key comparisons, eliminates pointer chasing during index scans, and ensures 16-byte alignment for native CPU register optimization.
*   **Tables:** Collections of records defined by a `TableSchema`. Each table is a distinct Z-Set stream $(\Delta PK, \Delta Row, \Delta W)$.
*   **Systems:** Materialized views implemented as persistent DBSP circuits. Systems consume deltas from input Tables and produce new output Z-Sets, which can be fed back into the ingestion layer or distributed to remote nodes.

### 1.3. Objective: The Unified Z-Set Engine
The engine is designed for the incremental maintenance and distribution of relational state via a single source of truth.
*   **WAL-Centric Distribution:** The **Z-Set WAL** serves as the unified stream for local crash recovery and remote client synchronization. It persists LSN-prefixed binary batches of Z-Set deltas. 
*   **Idempotent Synchronization:** Remote mirrors maintain state by additively applying Z-Set deltas. The algebraic nature of the stream ensures that any prefix of the WAL can be replayed to reach a consistent state baseline.
*   **Snapshot-to-Stream Continuity:** Point-in-time snapshots are resolved by algebraically summing persistent columnar shards, providing a $w_{net}$ baseline before transitioning to live WAL tailing.

### 1.4. Performance Mandate: JIT and the Ghost Property
Implementation in **RPython** enables a **Meta-Tracing Virtual Machine** to specialize execution at runtime.
*   **Dynamic Layout Promotion:** The `TableSchema` registry calculates physical strides and offsets. The RPython JIT promotes these to constants, specializing assembly for specific table structures.
*   **The Ghost Property:** Algebraic weights are decoupled from row payloads. Records with a net weight of zero (annihilated) are bypassed during DRAM fetches and I/O, ensuring only records with $w_{net} \neq 0$ consume CPU cycles.
*   **German String Optimization:** String data uses a 16-byte inline structure (Length, Prefix, Payload/Offset). This enables $O(1)$ equality failures and elides heap access for the majority of Z-Set comparisons.
*   **128-bit Key Optimization:** Native support for `u128` keys (UUIDs) uses RPython `r_uint128`, mapping to native 128-bit registers in modern C compilers to maintain parity with 64-bit integer performance.

## 2. System Architecture Overview

### 2.1. Ingestion Layer and Registry
The ingestion layer manages the entry of Z-Set updates and validates structural integrity against a dynamic type system.
*   **Table Registry:** A persistent catalog mapping Table IDs to `TableSchema` descriptors. It defines the primary key type (`u64` or `u128`), column types, and field offsets.
*   **Packed Row Ingestion:** Updates are submitted as atomic "Rows" serialized into a row-oriented format (Array of Structs) for write-efficiency.
*   **Staging MemTable:** In-memory ingestion utilizes a row-oriented SkipList within a **Dual-Arena System**. The primary **Monotonic Arena** stores SkipList nodes, inline keys, and fixed-width row data, while a dedicated **Blob Arena** stores variable-length string data. This structure facilitates rapid algebraic coalescing of weights before persistence.

### 2.2. Runtime Layer: Persistent DBSP VM
The runtime executes reactive circuits defined over Table streams.
*   **Vertical Join Execution:** The core operation is the joining of independent Z-Sets on Primary Key. The VM utilizes vectorized zip-join algorithms on sorted arrays to align records efficiently.
*   **Circuit Scheduler:** Manages the dataflow between Z-Set streams. When a source Table receives updates, only dependent circuits are triggered.
*   **State Management:** The VM maintains persistent operator state using the same columnar storage format as base data, ensuring uniformity in storage management.

### 2.3. Storage Layer: Fragmented LSM (FLSM)
Persistence is managed by a **Fragmented Log-Structured Merge (FLSM)** tree, physically organized into independent columnar shards.
*   **Table Shards:** Data is stored in typed, immutable shards sorted by Primary Key.
*   **N-Partition Columnar Format:** Shards utilize a dynamic layout where each column defined in the `TableSchema` is stored in a discrete, contiguous region. A **Column Directory** in the shard header maps region offsets, allowing for schema-flexible columnar access.
*   **Transmutation Pipeline:** A background process transforms row-oriented (AoS) MemTable data into column-oriented (SoA) Table Shards. This involves "unzipping" the packed rows into distinct physical regions and relocating variable-length strings into a consolidated **Shard Blob Heap**.
*   **The Spine:** A lightweight in-memory index tracking the `[MinPK, MaxPK]` ranges for every active shard, enabling correct algebraic summation across overlapping LSN ranges.

### 2.4. Distribution Layer: Reactive Streaming
The distribution layer synchronizes remote client state with server-side views.
*   **Table Subscriptions:** Clients subscribe to specific tables or materialized query results.
*   **Delta Push:** The server streams deltas (additions/removals) serialized via a unified binary protocol derived from the WAL format.
*   **Snapshot Resolution:** For bootstrapping, the distribution layer performs a merge-scan across the FLSM Spine to reconstruct the net state ($w_{net}$) of requested tables at a specific Log Sequence Number (LSN).

## 3. Memory Subsystem and Physical Layout

### 3.1. Dual-Arena Allocation
Memory for ingestion and intermediate state is managed via a **Dual-Arena** system to isolate fixed-width metadata and packed row payloads from variable-length data, bypassing garbage collection overhead.
*   **Staging Arena:** Stores SkipList nodes, Primary Keys, and packed row data (AoS). Allocation follows a monotonic bump-pointer pattern. To support native 128-bit register access for UUID keys, the allocator enforces 16-byte alignment for the Primary Key field within the node.
*   **Blob Arena:** Stores variable-length overflow data for German Strings. By isolating blobs, the engine performs "Ghost-aware" transmutation, only copying strings associated with non-zero net weights during shard finalization.
*   **Memory Safety:** The system utilizes raw pointers (`rffi.CCHARP`) and manual lifecycle management. Bounds-checked access is enforced via `MappedBuffer` abstractions during reads.

### 3.2. Physical Shard Layout (N-Partition)
Each Table Shard is a memory-mapped file organized into dynamic columnar regions. All regions are aligned to 64-byte boundaries to support AVX-512 vectorized loads and cache-line isolation.

1.  **Header (64 bytes):** Contains magic numbers, versioning, row counts, Table ID, and the file offset to the **Column Directory**.
2.  **Column Directory:** A variable-length metadata block immediately following the header. It contains an array of `ColumnMeta` descriptors (Offset, Size, Checksum) for every region in the shard.
3.  **Region PK (Primary Keys):** A dense, sorted vector of 64-bit or 128-bit unsigned integers. Sorting allows for $O(\log N)$ binary search and linear-time merge-joins.
4.  **Region W (Weights):** A vector of 64-bit signed integers mapping 1-to-1 by index to Primary Keys.
5.  **Region $C_1 \dots C_n$ (Columns):** Discrete vectors for each column defined in the `TableSchema`. Numeric types are stored as dense arrays; string types utilize the 16-byte German String structure.
6.  **Region B (Shared Blob Heap):** A consolidated overflow heap for all variable-length string data in the shard, regardless of the originating column.

### 3.3. JIT-Compiled Columnar Access
The engine leverages the RPython Meta-Tracing JIT to optimize columnar access patterns based on the `TableSchema`.
*   **Directory Promotion:** During trace compilation, the JIT promotes the **Column Directory** (offsets and sizes) to immutable constants. 
*   **Access Specialization:** The JIT specializes the address calculation `base + region_offset + (index * type_size)` into machine instructions with immediate offsets, eliminating the overhead of dynamic directory lookups during hot loops.

### 3.4. German String and Shared Blob Heap
String fields utilize a 16-byte inline structure to maximize cache efficiency and minimize heap pressure.
*   **Structure:** `Bytes 0-3`: Length (`u32`); `Bytes 4-7`: Prefix (`char[4]`); `Bytes 8-15`: Payload (Union of 8-byte suffix or **64-bit heap offset**).
*   **Shared Heap:** Unlike row-oriented formats where strings are interleaved, the N-Partition format pools all string overflows into **Region B**. This enables shard-wide string deduplication during transmutation.
*   **Equality Logic:** $O(1)$ equality failure via length and prefix checks. The **Ghost Property** ensures that Region B access is elided for records where $w_{net} = 0$.

### 3.5. Transmutation: AoS to SoA "Unzipping"
The transition from mutable memory (MemTable) to immutable persistence (Shard) involves a columnar demultiplexing process known as **Unzipping**.
*   **Process:** The pipeline performs a linear scan of the SkipList. It separates the row-oriented nodes into discrete columnar buffers ($PK, W, C_1 \dots C_n$).
*   **Pointer Relocation:** For German Strings, long payloads are read from the MemTable's Blob Arena and copied into the shard's **Region B**. The 64-bit offset in the columnar region is recalculated to be relative to the start of the shard's Region B, ensuring physical self-containment.
*   **Algebraic Coalescing:** Only records with a non-zero net weight ($w_{net} \neq 0$) are materialized during transmutation, physically reclaiming space from annihilated state.

## 4. Storage Architecture: Fragmented LSM (FLSM)

### 4.1. The Binary Manifest and Registry Projection
The storage state is anchored by a versioned binary **Manifest** (`MANIFNGT`), serving as the authoritative root of truth for the persistent shard set.
*   **Atomic Updates:** Manifest updates utilize temporary swap-files and atomic `rename()` operations. This ensures that reader processes (Sync Server, Compactor) always observe a consistent, point-in-time snapshot of the global shard set.
*   **Metadata Tracking:** For every shard, the manifest records the Table ID, Primary Key range (`MinPK`, `MaxPK`), and LSN range. The manifest header tracks a `global_max_lsn`, defining the high-water mark of persisted state across all tables.
*   **Transient Registry Projection:** The **ShardRegistry** is an in-memory projection of the Manifest used for range-query optimization. It monitors **Read Amplification**, defined as the number of overlapping shards covering a specific Primary Key range.

### 4.2. Vertical Decomposition and Sorted Alignment
Data is physically partitioned by Table ID into independent columnar shards.
*   **Primary Key Sorting:** All shards are strictly sorted by Primary Key ($u64$ or $u128$) to enable $O(\log N)$ binary search lookups and linear-time Vertical Joins.
*   **Columnar Isolation:** The directory-based layout allows the engine to perform selective I/O. Queries requiring only a subset of columns map only the relevant regions from the shard, minimizing memory-mapped overhead.

### 4.3. Compaction: Pure Z-Set Merge
Compaction merges overlapping shards for a single Table ID into a consolidated "Guard" shard. Unlike traditional LSMs using Last-Write-Wins (LWW) semantics, GnitzDB performs a **Pure Z-Set Merge**.
*   **Tournament Tree N-Way Merge:** The compactor uses a tournament tree to yield the globally minimal Primary Key across all input shards. The tree is specialized for $u64$ or $u128$ comparison to eliminate pointer-chasing overhead.
*   **Semantic Payload Grouping:** For a given Primary Key, the engine groups records by the equality of their entire row payload. This comparison includes all columns defined in the `TableSchema` (excluding the PK). For string columns, the engine performs content-based comparison by following offsets into the shard's **Region B**.
*   **Algebraic Summation:** For each unique payload group, the engine calculates the net algebraic weight $\sum w_i$.
*   **Annihilation (The Ghost Property):** If the net weight sums to zero, the record is annihilated and discarded. Only records with $w_{net} \neq 0$ are written to the output Guard Shard.
*   **Multiset Preservation:** If a Primary Key is associated with multiple distinct row payloads across shards (and their net weights are non-zero), each unique payload is preserved as a distinct entry in the output shard.

### 4.4. Read Amplification and Triggers
The **ShardRegistry** monitors the structural health of the FLSM levels for each Table ID.
*   **Metric:** Read Amplification is tracked at the Primary Key level.
*   **Heuristic Trigger:** When the number of overlapping shards for a Table ID exceeds a configurable threshold (default: 4), the table is flagged for automated compaction to restore lookup performance and reclaim storage from annihilated records.

## 5. Physical Encoding and Serialization

### 5.1. Table Shard Header Specification
Each Table Shard begins with a fixed 64-byte header. All multi-byte integers are encoded in **Little Endian**.

| Byte Offset | Field Name | Type | Description |
| :--- | :--- | :--- | :--- |
| 00 - 07 | Magic Number | `u64` | Constant: `0x31305F5A54494E47` |
| 08 - 15 | Version | `u64` | Constant: `2` |
| 16 - 23 | Row Count | `u64` | Total number of records in this shard |
| 24 - 31 | Directory Offset | `u64` | Byte offset to the Column Directory block |
| 32 - 39 | Table ID | `u64` | Authority ID for the table schema |
| 40 - 63 | Reserved | `u8[24]` | Padded with `0x00` |

### 5.2. Column Directory (Variable Length)
The Column Directory is a contiguous array of `ColumnMeta` entries located at the `Directory Offset`. The number of entries is determined by the `TableSchema` (PK region + Weight region + N column regions + Blob region).

**ColumnMeta Entry (24 bytes):**
*   `[00-07]` **Offset**: Absolute byte offset from file start to the region start.
*   `[08-15]` **Size**: Total byte size of the region.
*   `[16-23]` **Checksum**: XXH3-64 hash of the region's contents.

### 5.3. Region Alignment and Padding
To support vectorized memory access and cache-line alignment, the physical layout enforces 64-byte alignment for all region boundaries.
*   **Region Transitions:** The `TableShardWriter` calculates the necessary padding to ensure `(region_offset % 64 == 0)`.
*   **Padding Bytes:** Inter-region gaps are filled with `0x00`.

### 5.4. Region PK: Primary Key Vector
Contains a dense, sorted sequence of Primary Keys. The entry size is fixed per shard based on the `TableSchema`:
*   **Type u64:** 8-byte unsigned integers.
*   **Type u128:** 16-byte unsigned integers (stored as two contiguous 64-bit segments).
Validation is performed via the corresponding checksum in the Column Directory.

### 5.5. Region W: Weight Vector
Contains a dense sequence of 64-bit signed integers mapping 1-to-1 by index to the keys in Region PK. This region is always required to support Z-Set algebraic summation.

### 5.6. Column Regions ($C_1 \dots C_n$)
Each column in the `TableSchema` (excluding the Primary Key) is assigned its
own discrete region.

*   **Fixed-Stride Integer Primitives:** Integer types (`i8`, `i16`, `i32`,
    `i64`, `u8`, `u16`, `u32`, `u64`) are stored as dense arrays with element
    size equal to the column's declared byte width.

*   **Fixed-Stride Floating-Point Primitives:** `f32` and `f64` are stored as
    dense arrays of IEEE 754 single- or double-precision values.

*   **TYPE_U128 (Non-PK Columns):** Stored as pairs of contiguous 64-bit
    unsigned integers in little-endian order — the low 64 bits at the base
    column offset, the high 64 bits at offset + 8 — giving a total of 16 bytes
    per element. This physical layout is identical to the Primary Key region
    layout for `u128` PKs, which allows the same raw-memory read path
    (`SoAAccessor`, `PackedNodeAccessor`) to serve both cases.

    **Distinction from TYPE_U128 Primary Keys.** Only one column per table may
    be designated the Primary Key. Primary Key values of type `u128` flow
    through the engine as native `r_uint128` throughout the ingestion pipeline,
    SkipList nodes, cursor reads, and shard format, and never enter the
    `TaggedValue` union. Non-PK columns of TYPE_U128 — typically UUID foreign
    keys referencing another table — must transit the `TaggedValue` union
    between the ingestion API and physical storage (see §8.7). The 16-byte
    on-disk representation is identical for both cases; only the in-memory
    handling differs.

*   **German Strings (TYPE_STRING):** Stored as 16-byte structs. The structure
    encodes a 4-byte length, a 4-byte prefix for O(1) equality rejection, and
    an 8-byte payload that is either an inline suffix (strings ≤ 12 bytes) or
    a 64-bit offset into the **Shared Blob Heap (Region B)**.

### 5.7. Region B: Shared Blob Heap
A consolidated overflow region for variable-length string data. It is accessed via the 64-bit offsets stored within any string-type column region. All long string payloads are deduplicated within this region during shard finalization.

### 5.8. Z-Set Write-Ahead Log (WAL) Format
The WAL is an append-only sequence of LSN-prefixed blocks. Unlike shards, the
WAL uses a row-oriented (AoS) format for high-velocity durability.

**WAL Block Header (32 bytes):**

*   `[00-07]` **LSN**: 64-bit monotonic Log Sequence Number.
*   `[08-11]` **Table ID**: 32-bit unsigned integer identifying the table.
*   `[12-15]` **Entry Count**: 32-bit unsigned integer (number of records in
    block).
*   `[16-23]` **Block Size**: 32-bit unsigned integer (total byte length of
    header + body). *(Bytes 20-23 reserved, padded with `0x00`.)*
*   `[24-31]` **Block Checksum**: XXH3-64 hash of the block body.

**WAL Block Body:**

A stream of records, each formatted as:

```
[ Key (8 or 16 bytes) | Weight (8 bytes) | Packed Row Payload (Stride bytes) ]
```

*   **Key:** 8 bytes for `u64` PKs, 16 bytes for `u128` PKs. The key size is
    determined by the `TableSchema`.
*   **Weight:** 64-bit signed integer (`r_int64`).
*   **Packed Row Payload:** AoS representation of all non-PK columns at their
    schema-declared offsets. Column types are serialized as follows:

    | Column Type | WAL Representation |
    | :--- | :--- |
    | Integer (`i8`…`u64`) | Native-width little-endian integer |
    | `f64` / `f32` | IEEE 754 double / single |
    | `TYPE_STRING` | 16-byte German String struct; long-string bodies appended inline after the fixed stride, with the struct's 8-byte payload field holding the absolute byte offset within the WAL block |
    | `TYPE_U128` (non-PK) | Two contiguous 64-bit words: low word at the column's schema offset, high word at offset + 8 |

Long German String overflow bodies are appended immediately after the fixed
stride of each record. The allocated block size accounts for these overflows.
The `WALBlobAllocator` manages the overflow cursor so that each string's
absolute position within the block is stored in its German String struct.

**WAL Block Body:**
A stream of records formatted as: `[Key (8/16 bytes) | Weight (8 bytes) | Packed Row Payload (Stride bytes)]`. 
*   The Key size is determined by the `TableSchema`.
*   The Weight is a 64-bit signed integer.
*   The Packed Row Payload is the AoS representation of all non-PK columns, including 16-byte German String structs.

### 5.9. Data Integrity and The Ghost Property
The storage layer utilizes region-scoped validation to optimize "Ghost" record processing.
*   **Partial Validation:** Readers (e.g., during query execution) validate the PK and Weight regions first. 
*   **Deferred Validation:** Checksums for specific column regions ($C_i$) or the Blob Heap (Region B) are only validated if the records being accessed possess a non-zero net weight ($w_{net} \neq 0$), preventing I/O and CPU stalls for annihilated data.
    
## 6. The MemTable: High-Velocity Z-Set Ingestion

### 6.1. The Ingestion Pipeline and Unified Log
The MemTable is the mutable, in-memory write-head of the engine, optimized for the rapid absorption of row-oriented Z-Set deltas. Ingestion follows a strict, durable pipeline:
1.  **Schema Enforcement:** Incoming deltas are validated against the **Table Registry** to ensure primary key type compatibility ($u64$ vs $u128$) and physical layout compliance.
2.  **LSN Assignment:** Each ingestion batch is assigned a unique, monotonic **Log Sequence Number (LSN)**.
3.  **Z-Set WAL Serialization:** Before application to memory, the delta is serialized to the **Z-Set Write-Ahead Log** in a packed-row format. This serves as the source of truth for crash recovery.

### 6.2. AoS SkipList for Pure Z-Set Coalescing
The MemTable utilizes a SkipList indexed by a composite key consisting of the **Primary Key** and the **Packed Row Payload** to perform immediate, in-place algebraic summation.
*   **Algebraic Accumulation:** The SkipList performs an $O(\log N)$ search. If a node with the exact Primary Key and Row Payload exists, the incoming weight is added to the node's 64-bit weight field.
*   **Multi-Value Support:** If the same Primary Key is submitted with a different row payload, a new node is created. This maintains multiset semantics, allowing a single key to possess multiple distinct states simultaneously.
*   **In-Memory Annihilation:** If updates result in a net weight of zero for a specific payload, the record is marked as an annihilated "Ghost." These nodes are bypassed during the transmutation process to reclaim memory.

### 6.3. Physical Node Layout

Nodes are allocated within the **Dual-Arena** system (Staging Arena and Blob
Arena). To ensure native CPU register performance, the Primary Key field is
16-byte aligned.

**Node Structure:**

*   `[00-07]` **Weight**: 64-bit signed integer (`w ∈ ℤ`).
*   `[08-08]` **Height**: 8-bit unsigned integer (SkipList tower height `h`).
*   `[09-11]` **Padding**: Three bytes ensuring 4-byte alignment for the
    pointer array.
*   `[12 … 12+(h×4)-1]` **Next-Pointer Array**: Height-indexed array of `h`
    32-bit arena offsets, one per SkipList level.
*   `[aligned to 16]` **Primary Key**: 8-byte (`u64`) or 16-byte (`u128`)
    unsigned integer, stored at the first 16-byte-aligned offset after the
    pointer array.
*   `[PK end … PK end + Stride - 1]` **Packed Row Payload**: Fixed-stride
    block containing the serialized values of all non-PK columns at their
    `TableSchema`-declared offsets. Each column is stored as:

    | Column Type | Node Representation |
    | :--- | :--- |
    | Integer (`i8`…`u64`) | Native-width value; the bit pattern is identical for signed and unsigned types, matching the `r_int64` bitcast used in `PayloadRow._lo` |
    | `f64` / `f32` | IEEE 754 double / single |
    | `TYPE_U128` (non-PK) | Two contiguous 64-bit words: low word at the schema offset, high word at offset + 8; reconstructed via `PayloadRow.get_u128()`, which recombines the low word stored in `_lo` and the high word stored in `_hi` |
    | `TYPE_STRING` | 16-byte German String struct; long-string tails stored in the Blob Arena and referenced by their 64-bit blob-arena offset |

When a MemTable node is flushed to a columnar shard, the `TableShardWriter`
reads the `TYPE_U128` payload from raw memory via `PackedNodeAccessor.get_u128`,
which reconstructs the full 128-bit value from the two 64-bit words written at
serialisation time.

### 6.4. Sealing and Transmutation (Unzipping)
The transition from mutable row-oriented memory to immutable columnar persistence is triggered by Arena occupancy.
*   **Atomic Rotation:** The active MemTable is marked as "Sealed" (read-only), and a new SkipList/Arena pair is instantiated.
*   **Columnar Demultiplexing:** The Transmutation Pipeline performs a linear scan of the Sealed SkipList. It "unzips" each node, distributing the Primary Key, Weight, and individual row columns into the discrete regions ($PK, W, C_1 \dots C_n$) of a new **N-Partition Table Shard**.
*   **Directory Generation:** During this process, the pipeline calculates the final offsets and XXH3-64 checksums for each region to populate the shard's **Column Directory**.

### 6.5. Shared Blob Relocation and Deduplication
The transmutation process optimizes variable-length storage by consolidating German String data.
*   **Survivor Relocation:** For surviving records ($w_{net} \neq 0$) containing "Long" German Strings, the payload is read from the MemTable's Blob Arena.
*   **Content-Based Deduplication:** The pipeline utilizes a transient hash map to detect duplicate string payloads across all columns within the same transmutation batch. Duplicate strings are stored only once in the shard's **Region B**.
*   **Offset Swizzling:** The 64-bit heap offsets within the columnar regions are updated to reflect the final, shard-relative offsets within the consolidated **Shared Blob Heap**.

## 7. Compaction: The JIT-Compiled Merge Loop

### 7.1. Table-Scoped Compaction
Compaction operates independently on the shards of a specific Table ID to maintain read performance and reclaim storage.
*   **Trigger Heuristic:** Triggered when the `ShardRegistry` detects that the number of overlapping shards for a specific Primary Key range (Read Amplification) exceeds the defined threshold.
*   **Goal:** To merge multiple overlapping row-oriented MemTables and columnar shards into a single, non-overlapping "Guard" shard.

### 7.2. Type-Aware Tournament Tree Merge
The compaction engine employs a **Tournament Tree** (min-heap) to execute an N-way merge sort of input shards, keyed by Primary Key.
*   **Key Specialization:** The tree is specialized for either $u64$ or $u128$ keys. This enables native CPU register comparisons and eliminates pointer-chasing or secondary key-materialization overhead.
*   **Cursor Management:** Each input shard is accessed via a `StreamCursor` that maps the Region PK and Region W. Cursors utilize the **Column Directory** to locate specific column regions only when payload comparison is required.

### 7.3. Semantic Row Coalescing: Pure Z-Set Merge
For each unique Primary Key yielded by the tournament tree, the engine performs a **Pure Z-Set Merge** to consolidate row state.
*   **Full-Row Semantic Equality:** Contributions from different shards are grouped by the equality of their entire row payload (all non-PK columns). For columns containing German Strings, the engine performs content-based comparison by following offsets into the respective shards' **Region B**, as physical offsets for identical strings vary across shards.
*   **Net Weight Summation:** Within each unique row payload group, the engine performs an algebraic summation of all weights ($\sum w_i$) contributed by the overlapping shards.
*   **Annihilation (The Ghost Property):** If a specific row group's net weight sums to zero, it is identified as annihilated. The record is discarded, and no I/O is performed for its column data or blob payloads, physically reclaiming storage.
*   **Multiset Preservation:** If a Primary Key is associated with multiple distinct row payloads, and those payloads have non-zero net weights, each unique payload is preserved as a distinct record in the output shard.

### 7.4. JIT-Compiled Specialized Merge
The RPython Meta-Tracing JIT optimizes the merge loop to maximize throughput for a specific `TableSchema`.
*   **Schema Specialization:** The JIT specializes the grouping and summation logic for the specific number of columns, their respective strides, and field offsets.
*   **Materialization Barrier:** The JIT enforces a barrier that guards payload access. Column data is only read if the Primary Key and Weight regions indicate a surviving record ($w_{net} \neq 0$). This ensures that annihilated data never occupies CPU cache lines or triggers unnecessary Region B fetches.

### 7.5. Shard Materialization and Directory Finalization
Coalesced rows are streamed into a `TableShardWriter` to generate the new columnar Guard Shard.
*   **Unzipping to N-Partitions:** The writer distributes the consolidated Primary Keys, Weights, and Column Data into discrete, 64-byte aligned regions ($PK, W, C_1 \dots C_n, B$).
*   **Relocation and Deduplication:** Long strings are relocated to the new shard's consolidated **Region B**. String payloads are deduplicated during this pass to minimize the blob heap footprint.
*   **Directory Generation:** Upon finalization, the writer calculates the absolute offsets and XXH3-64 checksums for every region to populate the shard's **Column Directory**.
*   **Atomic Manifest Update:** The Manifest is updated via a swap-and-rename operation, replacing the input shard set with the new Guard Shard in a single atomic transaction.
    
## 8. Execution Model: Persistent DBSP Virtual Machine

### 8.1. Register-Based Z-Set VM

The engine executes reactive circuits via a register-based Virtual Machine
designed for the incremental maintenance of relational views.

*   **Register File:** A collection of specialized registers indexed by the VM
    ISA. The Register File maintains a monomorphic interface to facilitate
    RPython JIT tracing.

*   **Delta Registers ($R_\Delta$):** Transient registers holding a `ZSetBatch`.
    Each batch is a set of three parallel arrays — primary keys (`r_uint128`),
    algebraic weights (`r_int64`), and payload rows (`PayloadRow`) — representing
    the in-memory Z-Set deltas $(\Delta PK, \Delta W, \Delta Row)$ for the
    current LSN epoch. The `PayloadRow` type stores all non-PK column values in a
    struct-of-arrays layout; column types are derived from the `TableSchema` at
    every access site rather than stored per value.

*   **Trace Registers ($R_T$):** Persistent registers holding a `UnifiedCursor`.
    These provide a seekable, merged view across the persistent FLSM shards and
    the active MemTable, serving as the historical state ($S$) required for
    non-linear operations.

### 8.2. Operational Primitives (DBSP-Complete ISA)
The ISA implements the core operators of the DBSP calculus. All operators handle multiple payloads per Primary Key to maintain multiset integrity.
*   **Linear Operators:** 
    *   `FILTER` / `MAP`: Apply user-defined `ScalarFunction` logic to deltas.
    *   `UNION`: Performs algebraic addition of two Z-Set streams.
    *   `NEGATE`: Multiplies weights by $-1$, enabling the calculation of retractions ($f(x_{old})$).
*   **Bilinear Operators (Join):**
    *   `JOIN_DELTA_TRACE`: Implements the incremental join rule $\Delta(A \times B) = \Delta A \times B$. Joins a transient batch against a persistent trace.
    *   `JOIN_DELTA_DELTA`: Joins two transient batches to compute the "delta-delta" term in the bilinear expansion.
*   **Non-Linear Operators:**
    *   `REDUCE`: Performs non-linear aggregation (e.g., `SUM`, `COUNT`, `MIN`, `MAX`) by iterating over a `TraceRegister`.
    *   `DISTINCT`: Normalizes weights to set semantics ($w > 0 \to 1$).
*   **Temporal and Integral Operators:**
    *   `DELAY` ($z^{-1}$): Moves a Z-Set from $R_\Delta$ at tick $T$ to a register available at tick $T+1$, enabling recursive CTEs and fixed-point iteration.
    *   `INTEGRATE`: The terminal sink that flushes a `ZSetBatch` into a `PersistentTable`, updating the global state.

### 8.3. Incremental Join Semantics
Relational joins are lowered into `JOIN_DELTA_TRACE` instructions. Because the engine supports multisets, the join performs an $N \times M$ cross-product for matching Primary Keys ($u64$ or $u128$).
*   **Weight Multiplication:** The resulting weight is the product of input weights ($w_{out} = w_\Delta \times w_T$).
*   **Key-Type Specialization:** The VM selects specialized kernels for 64-bit or 128-bit keys to utilize native CPU comparison instructions.
*   **Indexed Seek:** The operator leverages the `seek()` capability of the `UnifiedCursor` to perform Index-Nested-Loop Joins (INLJ), preventing full table scans for sparse deltas.

### 8.4. Aggregation and Group-By (`REDUCE`)
Aggregations utilize **Full-Row Semantic Equality**.
*   **Coalescing:** The operator advances the `UnifiedCursor` across the Trace, grouping records by Primary Key and the equality of all specified Group-By columns.
*   **German String Contentment:** The VM follows German String offsets into Region B to ensure content-based grouping rather than binary-offset grouping for variable-length data.

### 8.5. Materialization Barriers and the Ghost Property
The VM enforces the **Ghost Property** via materialization barriers that guard access to non-PK columns.
*   **Weight-Gated Execution:** The VM inspects the algebraic weight in Region W before executing scalar logic, row comparisons, or column fetches.
*   **Annihilation Bypass:** If a record's net weight is zero, the VM elides all fetches for column regions ($C_i$) and the shared blob heap (Region B). This ensures that annihilated "Ghost" data never occupies CPU cache lines or triggers unnecessary I/O.

### 8.6. JIT Specialization via VMSchema Registry
The RPython Meta-Tracing JIT optimizes the circuit execution based on the `VMSchema` associated with each register.
*   **Metadata Promotion:** The `VMSchema` registry freezes column offsets, strides, and types as JIT-immutable constants. This allows the JIT to fold address arithmetic into immediate machine instructions.
*   **Scalar Inlining:** User-defined `ScalarFunction` implementations are inlined directly into the trace. The JIT specializes the map/filter logic for the physical layout of the input `ZSetBatch`, eliminating the overhead of dynamic field lookups.
*   **Loop Unrolling:** For small, fixed-width schemas, the JIT unrolls the columnar iteration, enabling vectorized loads and SIMD-optimized comparison kernels.

### 8.7. In-Memory Value Representation: `PayloadRow`

The ingestion layer and VM pass non-PK column values through the `PayloadRow`
type (defined in `gnitz/core/values.py`). `PayloadRow` uses a **struct-of-arrays**
layout: instead of one heap object per column value, all values for a single
row are packed into a small, fixed number of parallel arrays whose element types
are statically known to RPython. Column type dispatch is always performed via
the `TableSchema`, never via a per-value tag.

#### 8.7.1. Layout

A `PayloadRow` contains the following fields, all allocated at row-construction
time:

| Field | Type | Always Present | Contents |
| :--- | :--- | :---: | :--- |
| `_lo` | `List[r_int64]` | Yes | One entry per non-PK column. Integer types: native value bitcast to `r_int64`. Float types: IEEE 754 bit pattern via `float2longlong` (lossless). `TYPE_U128`: low 64 bits, bitcast `r_uint64 → r_int64`. `TYPE_STRING`: `r_int64(0)` (data lives in `_strs`). NULL: `r_int64(0)`. |
| `_hi` | `List[r_uint64]` or `None` | Only when schema contains a non-PK `TYPE_U128` column | High 64 bits of `TYPE_U128` values; `r_uint64(0)` for all other column types at the same index. `None` when the schema has no non-PK `TYPE_U128` columns, avoiding a heap allocation. |
| `_strs` | `List[str]` or `None` | Only when schema contains a `TYPE_STRING` column | Python string value for `TYPE_STRING` columns; `""` for all other column types at the same index. `None` when the schema has no string columns. |
| `_null_word` | `r_uint64` | Yes | Scalar bitfield (no heap allocation). Bit *N* is set when payload column *N* is null. For schemas with no nullable columns, `_has_nullable` is JIT-promoted to `False` and this field is never read or written. |

Three boolean flags — `_has_u128`, `_has_string`, `_has_nullable` — are listed
in `_immutable_fields_`. The JIT promotes them to compile-time constants per
trace, resolving all branches on these flags at trace-compile time and
preventing dead branches from appearing in emitted machine code.

#### 8.7.2. Construction

`PayloadRow` instances must be created exclusively via `make_payload_row(schema)`.
Direct construction is prohibited. `make_payload_row` calls `_analyze_schema` to
derive the allocation flags, then constructs the row with correctly-sized arrays
using `newlist_hint` (see Appendix A §4 for the mr-poisoning hazard this
prevents).

After construction, columns are appended in schema order (skipping the PK
column) using the typed append methods:

| Method | Use for |
| :--- | :--- |
| `append_int(val_i64)` | All integer column types: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64` |
| `append_float(val_f64)` | `TYPE_F32`, `TYPE_F64` (stored via `float2longlong`, not value-cast) |
| `append_string(val_str)` | `TYPE_STRING` |
| `append_u128(lo_u64, hi_u64)` | Non-PK `TYPE_U128` columns |
| `append_null(payload_col_idx)` | Any nullable column; sets the corresponding bit in `_null_word` |

The row is considered immutable once all non-PK columns have been appended. No
mutation API is exposed after that point.

#### 8.7.3. Access

Columns are read back by payload index (0-based, PK excluded) via the typed
accessor methods. Column type dispatch is always performed by the caller using
`schema.columns[i].field_type`, exactly as in the storage-layer comparators:

| Method | Returns | Use for |
| :--- | :--- | :--- |
| `get_int(payload_col_idx)` | `r_uint64` | All integer types (unsigned semantics) |
| `get_int_signed(payload_col_idx)` | `r_int64` | All integer types (signed semantics) |
| `get_float(payload_col_idx)` | `float` | `TYPE_F32`, `TYPE_F64` (bit-level inverse of `float2longlong`) |
| `get_u128(payload_col_idx)` | `r_uint128` | Non-PK `TYPE_U128` columns |
| `get_str(payload_col_idx)` | `str` | `TYPE_STRING` |
| `is_null(payload_col_idx)` | `bool` | Any column; always `False` when `_has_nullable` is `False` |

#### 8.7.4. Reconstruction Identity for Non-PK `TYPE_U128`

The full 128-bit value is reconstructed from the two stored 64-bit words by
`get_u128`:

```python
lo = r_uint128(r_uint64(self._lo[payload_col_idx]))   # undo r_int64 bitcast
hi = r_uint128(self._hi[payload_col_idx])
return (hi << 64) | lo
```

This identity is used identically in `PayloadRowAccessor.get_u128` (comparator
layer) and every serialisation site that writes non-PK `TYPE_U128` values to
raw memory.

#### 8.7.5. Primary Key Boundary

`TYPE_U128` Primary Keys **never** enter the `PayloadRow` representation. They
are handled as native `r_uint128` throughout the entire engine:

*   Ingestion API (`PersistentTable.insert` / `delete`)
*   SkipList node key storage (`memtable_node.py`)
*   Shard PK region reads (`TableShardView.get_pk_u128`)
*   Cursor key comparison (`TournamentTree`, `UnifiedCursor`)

`PayloadRow` applies exclusively to **non-PK** columns — for example, `TYPE_U128`
foreign keys that reference another table's PK space.

#### 8.7.6. Payload Row Comparison

Within the VM layer, `PayloadRow` instances are compared using
`PayloadRowComparator` (defined in `gnitz/core/row_logic.py`). This class holds
a pair of pre-allocated `PayloadRowAccessor` instances and delegates to
`comparator.compare_rows`, which dispatches on column type via the schema and
enforces correct comparison semantics for all types, including unsigned
comparison for integer types and content-based comparison for `TYPE_U128` (via
`get_u128`).

Code in `gnitz/vm` must **never** import `gnitz/storage/comparator` directly.
`PayloadRowComparator` is the designated API proxy that enforces the
`vm → core → storage` dependency boundary.

`ZSetBatch` uses `PayloadRowComparator` in its `_row_cmp` helper, ensuring that
batch ordering is always consistent with the storage comparator and preventing
incorrect join results or missed consolidations for rows whose non-PK `TYPE_U128`
columns differ only in the high word.

## 9. Distribution and Concurrency Model

### 9.1. Multi-Process SWMR Architecture
The system utilizes strict process isolation to bypass the Global Interpreter Lock (GIL) and maximize I/O parallelism. The architecture is defined by a Single-Writer/Multi-Reader (SWMR) pattern across three specialized roles:
*   **The Executor Process:** The authoritative writer. It owns the `MemTableManager` and DBSP VM. It is the exclusive writer for the Z-Set WAL, finalized N-Partition Table Shards, and the binary Manifest. It assigns monotonic LSNs to all incoming ingestion batches.
*   **The Sync Server Process:** A concurrent reader process. It tails the Z-Set WAL for live updates and performs snapshot merges from Table Shards. It synchronizes its internal `ShardRegistry` by monitoring Manifest version shifts.
*   **The Compactor Process:** A background worker that performs N-way tournament-tree merges of overlapping shards. It writes new Guard Shards and submits the new shard set to the Executor for atomic Manifest publication.

### 9.2. SWMR Consistency and Manifest Authority
Concurrency is anchored by the immutability of the N-Partition shard format and the atomic `rename()` mechanics of the binary Manifest.
*   **Manifest Authority:** Readers safely `mmap` shards identified in the Manifest. Because shards are immutable once published, no coordination locks are required during read-scans. The Manifest serves as a point-in-time snapshot of the database state.
*   **Reference Counting:** The system implements a `RefCounter` to prevent the physical deletion of superseded shards (e.g., after compaction) until all reader processes have closed their respective `ShardHandle` instances. This prevents `SIGBUS` errors during background file cleanup.
*   **Atomic State Transitions:** Manifest updates utilize a temporary swap-file and atomic `rename()` to ensure that secondary processes never encounter a partially updated or corrupt shard list.

### 9.3. WAL-Based Inter-Process Communication
The **Z-Set WAL** serves as the primary data conduit between the Executor and the Sync Server.
*   **Tailing Semantics:** The Sync Server treats the WAL as a unified stream of row-oriented deltas. This ensures that distribution is chronologically and algebraically aligned with local ingestion.
*   **Single-Writer Authority:** To maintain monotonic LSN sequences, the WAL is subject to a strict file-system lock. Only the Executor may append to the WAL; secondary processes must treat the WAL as a read-only stream.
*   **LSN Synchronization:** The high-water mark of the database state is defined by the `global_max_lsn` in the Manifest. The Sync Server utilizes this LSN to transition from shard-based snapshot resolution to live WAL tailing.

### 9.4. Primary Key-Based Process Isolation
Process-level operations (Compaction and Joining) are optimized for the restricted Primary Key model ($u64$ or $u128$).
*   **Parallel Compaction:** Because tables are physically independent, multiple Compactor processes can operate on different Table IDs simultaneously without resource contention.
*   **Zero-Copy Handover:** Readers utilize `MAP_SHARED` memory mappings to access Table Shards. This allows multiple processes to share the same physical memory pages for the sorted Primary Key and Column regions, maximizing cache efficiency across the process boundary.

## 10. Client Synchronization Protocol

### 10.1. Handshake and Schema Validation
The session begins with a structural handshake to ensure binary compatibility between the server's Table state and the client's local replica.
*   **Schema Hash Exchange:** Clients and servers exchange cryptographic hashes of the `TableSchema`. This hash includes the Primary Key type (u64 vs u128), column types, and field offsets. Mismatched hashes terminate the session to prevent memory corruption during raw Z-Set application.
*   **LSN Negotiation:** The client provides its `Last_ACK_LSN`. The server utilizes this to determine if the client requires a full **Mode 1 (Snapshot Bootstrap)** or can proceed directly to **Mode 2 (Live Z-Set Tailing)**.

### 10.2. Mode 1: Snapshot Bootstrap
The server generates a baseline state image by resolving the persistent FLSM layer for the requested Table ID.
*   **Algebraic Merge-Scan:** The server performs an N-way merge-scan across all N-Partition Table Shards identified in the Manifest. The tournament tree aligns records by Primary Key ($u64$ or $u128$).
*   **Pure Z-Set Summation:** Contributions are grouped by full-row semantic equality. The server sums weights across these payload groups. Only records with a non-zero net weight ($w_{net} \neq 0$) are materialized.
*   **Serialized Stream:** The result is streamed to the client as a sequence of Z-Set deltas. To minimize bandwidth, the server deduplicates long German Strings within the bootstrap batch.
*   **LSN Transition:** The bootstrap stream terminates with a `SNAPSHOT_END` signal containing the `global_max_lsn` from the Manifest at the time the scan commenced.

### 10.3. Mode 2: Live Z-Set Tailing
Incremental updates are pushed to the client as they are appended to the unified Z-Set WAL.
*   **Filtering:** The Sync Server tails the WAL, filtering for the specific Table ID and applying the client's current LSN cursor. 
*   **Row-Oriented Propagation:** Deltas are transmitted in the WAL's row-oriented (AoS) format: `[Primary Key | Weight | Packed Row Payload]`. 
*   **Idempotent Additive Application:** Clients apply Z-Set deltas additively to their local state. Because the protocol is based on additive multisets, synchronization is natively idempotent; re-applying a previously received delta results in a consistent (though temporarily doubled) state that is corrected once the corresponding negative weights are processed or a local compaction is triggered.
*   **The Ghost Property:** Clients are expected to honor the Ghost Property, eliding processing or display of records where the local accumulated weight is zero.

### 10.4. Differential Subscription
The protocol supports partial synchronization via **Differential Subscriptions**.
*   **Predicate Pushdown:** Clients may provide a linear predicate (e.g., a filter on a specific column). 
*   **Server-Side Filtering:** The Sync Server applies the predicate to the Z-Set stream. Since filters are linear operators in DBSP, the resulting sub-stream maintains algebraic integrity, allowing the client to maintain a consistent partial replica of the table.
*   **Semantic Integrity:** For string-based predicates, the server utilizes the German String prefixes in the WAL blocks to perform $O(1)$ filter rejections before accessing the full payload.

## 11. SQL Frontend and Compiler Interface

### 11.1. Table and Schema Physicalization
The frontend utilizes **Apache Calcite** to translate SQL DDL and DML into the GnitzDB relational columnar model.
*   **Table Physicalization:** `CREATE TABLE` statements are translated into `TableSchema` descriptors. The compiler assigns the Primary Key type (restricted to `u64` or `u128`) and calculates physical offsets for all columns.
*   **Columnar Offsets:** For the persistent layer, the compiler generates instructions to populate the **Column Directory**. For the VM layer, it calculates the fixed strides required for row-oriented delta processing.
*   **Materialized View Registration:** `CREATE VIEW` statements are parsed into logical plans and identified by the Table IDs of their source relations. The compiler ensures that the output of the view is itself a valid Z-Set stream.

### 11.2. Incremental DBSP Lifting
The compiler transforms standard relational algebra into incremental DBSP circuits using the lifting identity $Q_\Delta = D \circ \uparrow Q \circ I$.
*   **Minimal Delta Computation:** The lifted circuit ensures that only the minimal Z-Set change (the difference between state $t$ and $t+1$) is computed and propagated.
*   **Multiset Join Lowering:** Relational joins are lowered into `JOIN_V` VM instructions. Because the engine supports multisets, the compiler generates logic to handle $N \times M$ row payload cross-products for matching Primary Keys ($u64$ or $u128$), ensuring the output weight is the product of input weights.
*   **Aggregation and Reduction:** Aggregations are implemented as `REDUCE` operators that utilize semantic row equality to coalesce weights across the Table's columnar regions.

### 11.3. Register Allocation and JIT Specialization
The execution plan is serialized into the VM ISA, with physical constants injected to facilitate RPython trace specialization.
*   **Immediate Constant Injection:** Physical strides, column offsets from the **Column Directory**, and Schema Hashes are injected as immediate constants into the bytecode.
*   **JIT Trace Specialization:** The RPython JIT promotes these constants to specialize the merge-scan and scalar logic loops. It specifically optimizes the **Semantic Equality Checks** required for German Strings by specializing the offsets into **Region B**.
*   **Materialization Barrier Injection:** The compiler automatically inserts weight-gated barriers before any `MAP`, `FILTER`, or `JOIN` scalar logic. These barriers inspect **Region W** at runtime and elide all column region ($C_i$) and blob heap (Region B) fetches if the net weight of the record is zero, enforcing the **Ghost Property** across the entire reactive pipeline.
*   **Key-Type Optimization:** The compiler selects specialized opcodes for `u64` vs `u128` Primary Keys, allowing the JIT to emit optimal machine code for 128-bit integer comparisons without heap-based overflow checks.

## 12. Implementation Plan

### Phase 0: Core Storage Primitives [COMPLETED]
*   **Zero-Allocation Memory Management:** Implementation of `Arena` bump-pointer allocators and `MappedBuffer` bounds-checked view abstractions.
*   **German String Optimization:** 16-byte inline string format with $O(1)$ prefix-based equality checks and shared heap relocation logic.
*   **Low-Level I/O Infrastructure:** RPython-compatible wrappers for POSIX `mmap`, `msync`, `flock`, and atomic `rename`.
*   **Integrity Layer:** Implementation of **XXH3-64** scalar hashing for region-scoped and block-level checksum validation.

### Phase 1: Ingestion and Durability [COMPLETED]
*   **Row-Oriented MemTable:** Implementation of a monotonic SkipList utilizing composite keys (Primary Key + Row Payload) for algebraic weight coalescing.
*   **Unified Z-Set WAL:** Append-only Write-Ahead Log storing LSN-prefixed Z-Set batches in Array-of-Structs (AoS) format.
*   **Durability Interlock:** Implementation of the strict write-ahead protocol and LSN-based crash recovery to reconstruct MemTable state.
*   **Binary Manifest System:** Authoritative root-of-truth tracking shard sets, `MinPK/MaxPK` ranges, and `global_max_lsn` with atomic swap mechanics.

### Phase 1.5: Relational Columnar Pivot [COMPLETED]
*   **128-bit Key Support:** Integration of `r_uint128` for native Primary Key processing, including 16-byte alignment in Arenas and specialized Tournament Tree comparators.
*   **N-Partition Shard Format:** Transition from Penta-Partition to a directory-based columnar format. Implementation of the **Column Directory** and variable region mapping.
*   **Transmutation Pipeline (Unzipping):** Implementation of the AoS-to-SoA converter that demultiplexes MemTable rows into discrete columnar regions ($PK, W, C_1 \dots C_n$).
*   **Shared Blob Consolidation:** Implementation of the consolidated **Region B** for shard-wide string deduplication and offset swizzling.
*   **Pure Z-Set Compaction:** N-way merge infrastructure utilizing a type-aware `TournamentTree` and full-row semantic equality grouping for algebraic summation.
*   **Shard Registry & RefCounting:** Implementation of Read-Amplification monitoring and reference-counted file lifecycle management for multi-process safety.

### Phase 2: Persistent DBSP Virtual Machine [PENDING]
*   **Register-Based Execution Core:** Implementation of the VM supporting $R_\Delta$ (delta) and $R_T$ (persistent trace) registers.
*   **Multiset Join Logic:** Development of the `JOIN_V` operator to execute $N \times M$ row cross-products with algebraic weight multiplication ($w_A \times w_B$).
*   **Weight-Gated Materialization Barriers:** JIT-specialized instructions that inspect **Region W** to elide column and blob fetches for annihilated records ($w_{net}=0$).
*   **JIT Schema Specialization:** Implementation of trace-constant promotion for `TableSchema` strides, column offsets, and Primary Key types.
*   **Trace Persistence:** Integration of VM operator state (Traces) with the FLSM storage layer, treating history as standard N-Partition Table Shards.

### Phase 3: Distribution and Synchronization [PENDING]
*   **Structural Handshake:** Implementation of the `TableSchema` hash exchange to enforce binary compatibility between nodes.
*   **Snapshot Bootstrap Resolver:** Development of the merge-scan engine to stream the net algebraic sum of persistent shards to new clients.
*   **Live WAL Tailing:** Implementation of the asynchronous propagation engine to filter and push WAL deltas to active subscribers.
*   **Differential Subscriptions:** Development of server-side predicate pushdown to generate algebraically consistent sub-streams of Z-Set deltas.

### Phase 4: SQL Frontend and Tooling [PENDING]
*   **Relational-to-DBSP Compiler:** Integration of **Apache Calcite** to translate SQL DDL and DML into incremental DBSP circuits.
*   **Incremental Lifting Logic:** Implementation of the $Q_\Delta = D \circ \uparrow Q \circ I$ transformation to ensure minimal state propagation.
*   **Bytecode Generation:** Development of the backend to inject physical strides and offsets as immediate constants into the VM ISA for RPython JIT optimization.
*   **Observability Suite:** Development of CLI tools for binary Manifest inspection, WAL-based point-in-time debugging, and Read-Amplification visualization.

### Phase 5: Advanced Optimization [FUTURE]
*   **Key-Range Partitioning:** Horizontal scaling via the sharding of the unified Z-Set WAL based on Primary Key ranges.
*   **Tiered Compaction Heuristics:** Implementation of write-heavy and read-heavy compaction strategies optimized for specific table workloads.
*   **Portable WASM Application:** Development of a WebAssembly library for idempotent Z-Set application in browser-based environments.
    
## 13. Future Work and Edge Cases

### 13.1. Dynamic Columnar Projection
The engine will implement **Dynamic Columnar Projection** to allow clients to subscribe to partial Z-Set deltas containing only specific columns from a `TableSchema`.
*   **Linear Filtering:** Since filters are linear operators in DBSP, the Sync Server can apply predicates and column masks directly to the Z-Set WAL during tailing.
*   **Directory-Aware Snapshotting:** For bootstrap snapshots, the server utilizes the **Column Directory** to elide I/O for non-subscribed regions, transmitting an algebraically consistent sub-stream of deltas that maintains the Ghost Property.

### 13.2. Distributed Primary Key Partitioning
Horizontal scaling will be achieved via **Key-Range Partitioning** of the unified Z-Set WAL based on the $u64$ or $u128$ Primary Key space.
*   **Sharded WALs:** The global LSN sequence remains unified, but the WAL is partitioned based on PK hash-spaces to enable parallel ingestion.
*   **Cross-Node Exchange:** The VM ISA will be extended with an `EXCHANGE` instruction to facilitate cross-node data shuffles during binary join operations, allowing for distributed row reconstruction while maintaining Z-Set invariants.

### 13.3. Portable Z-Set Application (WASM)
To facilitate integration with browser-based environments, a **Portable Z-Set Application** library will be developed in WebAssembly (WASM).
*   **Idempotent Application:** The library will provide a hardened implementation of the additive delta-to-relational logic, allowing clients to apply row-oriented WAL Z-Sets to local storage (e.g., SQLite, DuckDB).
*   **Zero-Copy Handover:** The library will utilize shared memory buffers to apply binary Z-Set segments directly from the network stack to the local mirror without intermediate deserialization.

### 13.4. Fixed-Point Iteration and Feedback Bounding
Future iterations will formalize the **Inter-Instance Feedback** mechanism to support recursive DBSP circuits for graph processing and recursive CTEs.
*   **Stratification:** The compiler will implement stratification logic to detect and manage recursive dependencies in the Reactive Trigger Graph.
*   **Algebraic Convergence:** For recursive circuits, the engine will execute the circuit until the output delta $R_\Delta$ reaches zero (convergence) within a single LSN epoch.

### 13.5. Self-Hosting Metadata Z-Sets
System metadata, including the **Table Registry** and active subscription lists, will be transitioned to internal **Metadata Z-Sets**.
*   **Unified Infrastructure:** This allows the system to manage its own schemas and partitions using the same FLSM, WAL, and N-Partition compaction infrastructure as user data.
*   **Streaming Metadata:** Changes to the database topology (e.g., new tables, dropped columns) will be propagated via the WAL as LSN-sequenced metadata deltas.

### 13.6. Algebraic Differential Privacy
GnitzDB is uniquely positioned to implement **Differential Privacy** at the circuit level by manipulating Z-Set weights.
*   **Noise Injection:** By injecting controlled Laplacian or Gaussian noise into the weight summation ($\sum w_i$) of aggregation operators, the engine can produce WAL deltas that satisfy formal privacy guarantees.
*   **Reactive Privacy:** Privacy-preserving views remain reactive, pushing updated, noisy deltas as the underlying source data changes.

### 13.7. Tiered Compaction for SoA Shards
The FLSM will be extended with **Tiered Compaction Heuristics** optimized for diverse columnar workload profiles.
*   **Write-Heavy Tiers:** Minimizes ingestion stalls by allowing higher overlap depth in upper FLSM levels while maintaining row-oriented WAL segments.
*   **Read-Heavy Guard Shards:** Prioritizes the consolidation of shards into large, non-overlapping N-Partition columnar blocks to maximize binary search performance and minimize the number of `mmap` handles required for snapshot resolution.

# Appendix A: RPython Implementation Reference

**Target:** Systems developers.
**Context:** RPython is a statically analyzable subset of Python 2.7 that translates to C with a Meta-Tracing JIT.

## 1. The Translation Contract
*   **Global Type Inference:** The annotator analyzes all reachable code. Every variable must have a single, static type (Monomorphism) per control-flow merge point.
*   **Entry Point:** Do not use `__main__`. Define a target function:
    ```python
    def entry_point(argv): return 0 # Must return int
    def target(driver, args): return entry_point, None
    ```
*   **Dead Code:** Elimination occurs *after* annotation. Unreachable code that violates type constraints will still cause translation failure.

## 2. Language Subset & Restrictions
*   **Banned:** `eval`, `exec`, dynamic class/function creation, runtime metaprogramming, `**kwargs`, `set` (use `dict` keys), multiple inheritance (mostly), re-binding globals.
*   **Restricted:** Generators (simple iteration only), Recursion (avoid in hot paths), Exceptions (expensive in JIT).
*   **Globals:** Treated as immutable `const` after initialization. Do not mutate global module state; use a Singleton `State` class instance passed via `entry_point`.

## 3. Type System & Containers
*   **Lists:** Strictly homogeneous. `[1, "a"]` is illegal. Use a base class wrapper for heterogeneous data.
    *   **Resizability Poisoning:** If a list is created in a way that RPython considers "fixed-size," the entire listdef is marked as **must-not-resize (mr)**. Any subsequent `.append()` call on *any* instance of that list type will fail with `ListChangeUnallowed`.
    *   **Fixed-Size Triggers:**
        *   Literals: `[]`, `[x, y]`.
        *   Multiplication: `[None] * n`.
        *   Concatenation: `list_a + list_b` (if lengths are known at compile time).
    *   **The Solution:** Always use `rpython.rlib.objectmodel.newlist_hint(n)` to initialize lists destined for mutation or shared storage. 
    *   **Concatenation:** Instead of `a + b`, create a new list via `newlist_hint(len(a) + len(b))` and use explicit loops with `.append()`.
*   **Dicts:** Keys and values must be statically typed.
*   **Signatures:** Avoid `*args` unless the tuple layout is static. Prefer `List[T]` for variable arguments.
*   **None/Null:** Variables can be `Optional[T]`. However, mixing `T` and `None` in a list can confuse the annotator (Union types). Prefer the Null Object pattern over `None` in containers.

## 4. Integer Semantics (Critical)
*   **Semantics:** Python `int`/`long` translate to C `long` (machine word).
*   **Overflow:** Standard math uses C semantics (wraps or overflows).
    *   Use `rpython.rlib.rarithmetic.ovfcheck(x+y)` to catch overflow.
    *   Use `intmask(x)` to force truncation to machine word.
*   **Unsigned:** Use `r_uint` for bitwise ops/checksums.
*   **128-bit:** Natively supported via `r_ulonglonglong`. **Warning:** RPython types (`int`, `long`) are "frozen" descriptors. **Never** cast via `long(x)`; use specialized casting helpers.
*   **List Tracing Bug:** Storing `r_uint128` primitives directly in resizable lists can lead to alignment issues or SIGSEGV in the translated C code.
*   **Best Practice:** Split `u128` values into two `u64` lists (`keys_lo`, `keys_hi`) when storing them in resizable containers. Reconstruct the `u128` only at the point of computation/comparison.
*   **Prebuilt Long Trap:** Do not use literals > `sys.maxint` (e.g., `0xFFFFFF...`). Python 2 creates a `long` object, which cannot be frozen into the binary. Use `r_uint(-1)` or bit-shifts.

## 5. Memory & FFI
*   **Allocations:** Visible to the annotator. Avoid object churn in loops.
*   **FFI:** Use `rpython.rtyper.lltypesystem.rffi`.
    *   **RAII:** Use `with rffi.scoped_str2charp(s) as ptr:` to prevent leaks.
    *   **Pointers:** Always validate `fd < 0` or `ptr` nullability.
    *   **Nullability:** `rffi.CCHARP` is a raw pointer. Always use `lltype.nullptr(rffi.CCHARP.TO)` for nulls. 
*   **I/O:** Avoid `os.read`/`write`. They introduce nullability ambiguity (`SomeString(can_be_None=True)`).
    *   Use `rpython.rlib.rposix.read`: Returns non-nullable string.
    *   Use `rposix_stat`: Returns strictly typed structs (fixed-width `st_ino`/`st_size`), preventing C-signedness warnings.
    *   **I/O:** Use `rpython.rlib.rposix.read` (returns non-nullable strings) rather than `os.read`.

## 6. JIT Optimization Guidelines
*   **Control Flow:** Keep hot loops simple (`while/for`). The JIT traces the interpreter loop; obscure control flow breaks the trace.
*   **Virtualizables:** Use `jit.promote()` on values that are effectively constant within a specific trace (e.g., Schema strides) to compile them as immediates.
*   **Unrolling:** Use `jit.unrolling_iterable` for loops over small, fixed lists (e.g., column schemas).
*   **Immutable Hinting:** Mark arrays/lists as immutable if they don't change after initialization to allow JIT constant folding.

## 7. Common Failure Modes
1.  **Monomorphism Violation:** `x = 1` then `x = "a"`.
2.  **FrozenDesc:** Calling `int()` or `long()` as functions on RPython primitives.
3.  **Prebuilt Long:** Using large integer literals directly.
4.  **Nullability Mismatch:** Passing a potentially `None` string (from `os`) to a function expecting strict `str` (like `rffi`).
5.  **List Mutation:** Calling `.append()` on a list previously hinted as immutable.
6.  **mr-Poisoning:** Using `[]` for a payload list, causing all future `.append` calls to crash.
7.  **u128-List Crashes:** Storing raw 128-bit integers in a resizable list instead of splitting into `lo/hi` pairs.
8.  **Raw-Leak:** Malloc-ing a `dummy_ptr` in an accessor and never freeing it, corrupting the C heap.
9.  **`r_int64`-Long Overflow:** Storing a value in `[2**63, 2**64)` via `r_int64(x)` and later reading it back with `rffi.cast(rffi.ULONGLONG, ...)` causes `OverflowError: integer is out of bounds` in test mode. Use `r_uint64(x)` for the read-back and `rffi.cast(rffi.LONGLONG, r_uint64(x))` for the write-in.

# Appendix B: Coding practices

* Code should be formatted how the tool `black` would do it. If you change a file with bad formatting, fix the formatting carefully.
* Attributes, methods and functions starting with underscore (`_`) are considered private. They should not be used by code outside the class/module, not even by tests. If you encounter a test using a private method, fix it by either making it use a public attribute/method/function, or remove it entirely.
* When fixing a problem, don't leave code comments like `# FIXED: ...` or `# REMOVED: ...` or similar. Code comments are not for documenting change.
* Prefer module-level functions to staticmethods.
