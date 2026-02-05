## 1. Introduction and Theoretical Foundation

### 1.1. Theoretical Framework: DBSP
This system implements **Database Stream Processing (DBSP)**, a mathematical framework that treats both batch and streaming computations as operations over an **Abelian Group** $(G, +, 0)$.
*   **Data Representation:** Datasets are modeled as **Z-Sets** (Generalized Multisets), mapping every record $r$ to an integer weight $w \in \mathbb{Z}$. 
    *   **Insert:** $(r, +1)$
    *   **Delete:** $(r, -1)$
    *   **Update:** $\{(r_{old}, -1), (r_{new}, +1)\}$
*   **Incremental View Maintenance (IVM):** For a relational query $Q$, the engine executes a lifted circuit $Q^\circ$. By utilizing the linearity of DBSP operators, the engine computes the change in the output view $(\Delta O)$ directly from the change in the input stream $(\Delta I)$:
    $$ Q(\sum \Delta I) = \sum Q(\Delta I) $$
    Non-linear operators (Join, Distinct) are handled via the chain rule of DBSP calculus, maintaining exact incremental state.

### 1.2. System Objective: The Reactive Sync Engine
GnitzDB is a reactive database server designed for the incremental maintenance and distribution of materialized views. 
*   **Table Ingestion:** Tables are append-only streams of records governed by strict, static schemas. Data is ingested as atomic batches, each assigned a monotonic **Log Sequence Number (LSN)**.
*   **Persistent Circuits:** The engine maintains a set of persistent DBSP circuits. Each circuit consumes input deltas from one or more tables and produces a stream of output deltas.
*   **Delta Distribution:** Clients subscribe to specific views. The server pushes incremental Z-Set updates (deltas) to clients, allowing them to maintain local synchronized replicas (e.g., in SQLite or DuckDB).
*   **Snapshot Synchronization:** The engine provides a fallback mechanism for lagging or new clients, delivering a full point-in-time snapshot of a Z-Set before resuming delta-based synchronization.

### 1.3. Data Ingestion and the WAL
Data integrity and durability are anchored by a **Write-Ahead Log (WAL)** using the **Cap'n Proto** binary format.
*   **Schema Enforcement:** All incoming records are validated against a pre-defined schema registry prior to ingestion.
*   **Zero-Copy Ingestion:** The engine utilizes Cap'n Proto's segment-based memory model to allow zero-copy transition from the WAL to the in-memory processing structures.
*   **Atomic Batches:** Records within a single batch are processed as a single algebraic unit. The engine ensures that either all records in a batch are applied to the DBSP circuits or none are.

### 1.4. Performance Mandate: The Ghost Property and JIT
The engine is implemented in **RPython**, utilizing a **Meta-Tracing Virtual Machine** to specialize query execution at runtime.
*   **The Ghost Property:** Physical storage decouples algebraic weights from record payloads using a **Structure of Arrays (SoA)** layout. This ensures that records with a net weight of zero (annihilated records) do not trigger DRAM fetches for payload data.
*   **Materialization Barriers:** The RPython JIT is directed to treat payload materialization as an opaque operation (@jit.dont_look_inside). This prevents the compiler from hoisting data-materialization logic above the weight-summation check, enforcing the Ghost Property at the assembly level.
*   **Transmutation:** The engine bridges high-velocity, row-oriented ingestion (Array of Structs) with read-optimized, columnar processing (Structure of Arrays) through a specialized transmutation layer during the persistence and compaction phases.

## 2. System Architecture Overview

The system architecture is organized into four functional layers that manage the lifecycle of data from schema-validated ingestion to incremental delta distribution.

### 2.1. Ingestion Layer and Catalog
The ingestion layer serves as the entry point for all record-based updates. It enforces data integrity through a centralized **Catalog**.
*   **Schema Registry:** Maintains static definitions for all tables and views. Every incoming record is validated against these schemas using the **Cap'n Proto** binary format.
*   **Write-Ahead Log (WAL):** Validated batches are appended to a durable, sequential WAL. Each batch is assigned a monotonic **Log Sequence Number (LSN)**, ensuring atomicity and providing a recovery point for the system state.
*   **AoS MemTable:** In-memory ingestion utilizes a row-oriented (Array of Structs) SkipList within a **Monotonic Arena**. This layout facilitates rapid weight coalescing and random-access key lookups for incoming deltas.

### 2.2. Runtime Layer: Persistent DBSP VM
The runtime layer executes materialized view definitions as long-running, incremental circuits.
*   **DBSP Circuit Scheduler:** Manages the execution of multiple bytecode-defined circuits. Circuits consume deltas from table streams and produce output deltas for their respective views.
*   **Register-Based Interpreter:** An RPython-based virtual machine specializing in Z-Set algebra. It utilizes a **Meta-Tracing JIT** to optimize hot dataflow paths, such as join-multiplication and filter loops.
*   **State Management:** The VM maintains the persistent algebraic state (e.g., join indexes and aggregation totals) required for incremental computation. This state is periodically checkpointed to the storage layer to minimize recovery time.

### 2.3. Storage Layer: Quad-Partition FLSM
Persistence is managed by a **Fragmented Log-Structured Merge (FLSM)** tree, physically optimized to support the **Ghost Property**.
*   **The Quad-Partition Shard:** Immutable storage units divided into four 64-byte aligned regions:
    1.  **Region W (Weights):** Contiguous `int64` vectors.
    2.  **Region O (Offsets):** Contiguous `int32` pairs.
    3.  **Region K (Key Heap):** VarInt-prefixed binary keys.
    4.  **Region V (Value Heap):** VarInt-prefixed binary values.
*   **The SoA Spine:** A versioned global index that maintains parallel vectors of shard metadata. This structure enables $O(\log N)$ point-in-time snapshot resolution for bootstrapping new subscribers.

### 2.4. Distribution Layer: Sync and Subscription
The distribution layer manages the delivery of deltas to remote clients (e.g., SQLite or DuckDB instances).
*   **Subscription Management:** Tracks active clients and their current **Change Offsets** relative to the view logs.
*   **Delta Replay Log:** Maintains a configurable history of recent view deltas. If a client’s offset falls within the retained log, the engine streams the incremental Cap'n Proto updates.
*   **Snapshot Bootstrap:** If a client is new or its offset has been garbage-collected, the distribution layer resolves a full point-in-time snapshot from the FLSM spine, streams the total Z-Set, and then transitions to live delta synchronization.

## 3. Memory Subsystem and Physical Layout

### 3.1. Monotonic Arena Allocation
The engine utilizes **Monotonic Arenas** to manage memory for high-velocity ingestion and intermediate operator state. This bypasses the RPython Garbage Collector and ensures predictable performance.
*   **Bump-Pointer Mechanics:** Memory is allocated in contiguous virtual blocks (default 64MB). Allocation involves a single pointer increment, maintaining $O(1)$ complexity.
*   **8-Byte Alignment:** The allocator enforces strict 8-byte alignment for all blocks. This ensures that `int64` weight vectors and 64-bit pointers are naturally aligned, preventing split-load penalties and hardware exceptions during algebraic summation.
*   **Relocatable 32-bit Offsets:** Internal structures (SkipList nodes, pointer arrays) use 32-bit offsets relative to the Arena base. This reduces memory footprint by 50% compared to absolute 64-bit pointers and enables the relocation of entire Arenas without pointer swizzling.

### 3.2. SkipList Node Topology
MemTable nodes are laid out as contiguous byte sequences within the Arena. The topology is designed for zero-allocation searching and rapid transmutation:
*   `[00-07]` **Weight**: 64-bit signed integer.
*   `[08-08]` **Height**: 8-bit unsigned integer (SkipList tower height).
*   `[09-11]` **Padding**: Ensures the subsequent pointer array is 4-byte aligned.
*   `[12-XX]` **Next-Pointer Array**: Height-indexed array of 32-bit Arena offsets.
*   `[XX-YY]` **Key Payload**: VarInt-prefixed binary segment (Cap'n Proto formatted).
*   `[YY-ZZ]` **Value Payload**: VarInt-prefixed binary segment (Cap'n Proto formatted).

### 3.3. Memory View Safety and JIT Promotion
Direct memory access is mediated by the **MappedBuffer** class, which provides an abstraction over `mmap` regions and raw Arena pointers.
*   **Safety Guards:** The class implements conditional bounds checking. During RPython translation, these checks are preserved as invariants that the Meta-Tracing JIT utilizes to elide redundant guards in unrolled loops.
*   **Constant Promotion:** Fields such as `base_ptr` and `size` are marked as `_immutable_fields_`. The JIT promotes these to constants during trace generation, converting variable memory offsets into fixed machine-instruction displacements.
*   **Zero-Copy Casting:** The subsystem utilizes `rffi.cast` and `rffi.ptradd` to interpret raw bytes as structured types (e.g., `LONGLONG`, `INT`) without moving data into the RPython heap.

### 3.4. Physical Realization of the Ghost Property
The engine enforces the Ghost Property through a strict materialization barrier in the memory access path:
*   **Barrier Decorator:** Methods responsible for materializing record payloads from the heaps are decorated with `@jit.dont_look_inside`.
*   **Hoisting Prevention:** The decorator acts as a compiler barrier, preventing the JIT from hoisting DRAM fetch instructions above the algebraic weight check. 
*   **Pipeline Elision:** If the net weight of a record sums to zero, the instruction pointer never enters the materialization function. This ensures that the hardware prefetcher does not pull payload data into the cache hierarchy for algebraically irrelevant records.

### 3.5. Quad-Partition Alignment and Stride Mechanics
Physical shards utilize a **Structure of Arrays (SoA)** layout with 64-byte alignment to match CPU cache line boundaries and meet SIMD requirements.
*   **Region W (Weight Vector):** Stride is exactly 8 bytes (`int64`). This contiguous layout enables the JIT to generate AVX-512 vectorized summation instructions.
*   **Region O (Offset Spine):** Stride is 8 bytes, containing two 32-bit relative offsets (Key and Value). By separating offsets from weights, the engine prevents the hardware prefetcher from speculatively loading heap addresses during weight-only scans.
*   **Cache Line Isolation:** Each region (W, O, K, V) begins on a new 64-byte boundary. This prevents false sharing and ensures that the prefetcher does not pull metadata into the L1 cache during weight-intensive compute phases.

## 4. Storage Architecture: Fragmented LSM (FLSM)

### 4.1. The Manifest and Versioned Spine
The storage state is anchored by the **Manifest**, a durable, append-only log that records the current set of active shards and their associated **Log Sequence Number (LSN)** ranges.
*   **Versioned Snapshots:** Each update to the Manifest produces a new version of the **Spine**. This versioning provides snapshot isolation for long-running synchronization tasks, ensuring that a client bootstrapping from a specific LSN receives a consistent point-in-time view of the Z-Set.
*   **Metadata Vectors:** The Spine maintains parallel in-memory vectors for `min_keys`, `max_keys`, and `shard_handles`. This enables cache-localized binary search to identify candidate shards for both point queries and range-based snapshot materialization.

### 4.2. Dual-Mode Storage: Delta Logs and Shards
The FLSM maintains two distinct physical representations of data to support different synchronization requirements:
*   **The Delta Log:** A sequential, Cap'n Proto-formatted log of every atomic batch (LSN). It is optimized for low-latency distribution to connected subscribers who only require the most recent incremental changes.
*   **The Shard Spine:** A compacted, columnar representation of the net Z-Set state. It is optimized for high-bandwidth bootstrap operations where the engine must stream the entire accumulated state to a new or lagging subscriber.

### 4.3. Multi-Level Key Resolution
Because the FLSM allows for overlapping "Dirty" shards, key resolution involves a multi-way merge across all shards whose boundaries encompass the target key. 
*   **Algebraic Consolidation:** The engine identifies all candidate shards via the Spine metadata and performs a simultaneous scan of their respective Region W (Weight) vectors. 
*   **Net Weight Summation:** The final value of a record is determined by the algebraic sum of weights across all fragments. If the sum is non-zero, the engine proceeds to materialization; otherwise, the record is treated as non-existent (annihilated).

### 4.4. LSN-Aware Transmutation
The transition from the mutable MemTable to the immutable FLSM is governed by the **Transmutation Pipeline**, which enforces LSN boundaries:
*   **Atomic Flushing:** During a flush, the engine unzips the row-oriented SkipList into a Quad-Partition shard. This shard is tagged with the highest LSN contained within the flushed batch.
*   **Zero-Weight Pruning:** The pipeline performs the first stage of garbage collection by eliding records whose net weight within the MemTable is zero, preventing "Ghost" records from entering the persistent storage layer.

### 4.5. Compaction and Reference Management
Compaction merges overlapping shards to maintain a flat, searchable Spine and reclaim disk space.
*   **Read Amplification Control:** Compaction tasks are prioritized based on the depth of shard overlap for specific key ranges. The goal is to promote "Dirty" shards into non-overlapping "Guard" shards.
*   **Distributed Reference Counting:** To prevent `SIGBUS` errors during concurrent access to `mmap` regions, the system employs reference-counted `ShardHandles`. Physical files are unlinked from the filesystem only after the last active subscriber or background task releases its reference to the shard's `MappedBuffer`.
*   **Retention Policy:** The FLSM manages the lifecycle of both the Delta Log and Shards. Once a set of shards is successfully compacted into a Guard Shard, the older fragments and their corresponding Delta Log entries are scheduled for deletion based on the system's configured recovery and synchronization window.

## 5. Physical Encoding and Serialization

### 5.1. Shard Header Specification
Each Quad-Partition shard begins with a fixed 64-byte header. All multi-byte integers are stored in **Little Endian** format.

| Byte Offset | Field Name | Type | Description |
| :--- | :--- | :--- | :--- |
| 00 - 07 | Magic Number | `u64` | Constant: `0x31305F5A54494E47` ("GNITZ_01") |
| 08 - 15 | Entry Count | `u64` | Total number of records in the shard |
| 16 - 23 | Min LSN | `u64` | The lowest Log Sequence Number contained in this shard |
| 24 - 31 | Max LSN | `u64` | The highest Log Sequence Number contained in this shard |
| 32 - 39 | Offset W | `u64` | Byte offset to Region W (Weights) |
| 40 - 47 | Offset O | `u64` | Byte offset to Region O (Offsets) |
| 48 - 55 | Offset K | `u64` | Byte offset to Region K (Key Heap) |
| 56 - 63 | Offset V | `u64` | Byte offset to Region V (Value Heap) |

### 5.2. Cap'n Proto Wire Format (WAL and Delta Logs)
The engine utilizes the **Cap'n Proto** serialization format for the Write-Ahead Log and the transmission of View Deltas.
*   **Segment-Based Memory:** Messages are organized into one or more segments. The engine utilizes the `capnpy` implementation to ensure that pointers are encoded as relative offsets, enabling zero-copy access from the `MappedBuffer` without swizzling.
*   **Canonical Representation:** Incoming batches are validated against the schema and written to the WAL in canonical form. This allows the engine to perform bitwise comparisons or hash-based deduplication without full message deserialization.
*   **LSN Tagging:** Every Cap'n Proto message in the log is prefixed by an 8-byte LSN. This allows for rapid log seeking by treating the WAL as a sequence of fixed-length length-prefixed blocks.

### 5.3. Base-128 VarInt Heap Encoding
Within the variable-length heaps (Regions K and V), the engine utilizes a **7-bit Base-128 VarInt** encoding for all record length descriptors.
*   **Most Significant Bit (MSB) Logic:** If the MSB is set (1), the subsequent byte continues the integer. If the MSB is clear (0), the current byte is the last.
*   **Decoding Efficiency:** The decoder performs a bitwise OR and shift within the `MappedBuffer` view. Small strings (under 128 bytes) incur only a 1-byte length penalty, minimizing the physical size of the Key Heap.

### 5.4. Region Padding and SIMD Alignment
The `ShardWriter` enforces a **64-byte boundary** for the start of every region through null-byte insertion.
*   **AVX-512 Compatibility:** The alignment ensures that the Weight Vector (Region W) is compatible with vectorized 512-bit load instructions. 
*   **Cache Line Isolation:** Padding prevents the end of one region and the beginning of another from occupying the same CPU cache line. This isolation ensures that linear scans of the weight vector do not speculatively pull data from the Offset Spine or Heaps into the cache.

### 5.5. Binary Manifest Format
The Manifest is an append-only file that serializes the current state of the FLSM. 
*   **Atomic Updates:** Each Manifest entry consists of a version number, a timestamp, and an array of active shard filenames with their associated `[min_key, max_key]` boundaries and LSN ranges. 
*   **Checksumming:** Every entry concludes with a CRC32 checksum. During startup, the engine performs a forward scan of the Manifest; if a checksum failure occurs (indicating a partial write), the engine truncates the file to the last valid state and recovers the missing state from the WAL.
    
## 6. The MemTable: High-Velocity Ingestion Buffer

### 6.1. The Ingestion Pipeline and Schema Validation
The MemTable serves as the mutable write-head of the engine, optimized for the rapid coalescing of Z-Set deltas. Ingestion follows a strict pipeline:
1.  **Schema Enforcement**: Incoming records are validated against the **Schema Registry** using the `capnpy` validator.
2.  **LSN Assignment**: Each batch is assigned a unique, monotonic **Log Sequence Number (LSN)**. All records within a batch share this LSN.
3.  **WAL Serialization**: The batch is serialized to the Cap'n Proto-formatted WAL before being applied to the in-memory structures.

### 6.2. Algebraic Coalescing in the SkipList
The MemTable utilizes the row-oriented (AoS) SkipList to perform in-place algebraic summation of weights at the moment of ingestion.
*   **Key Identification**: The unique key for a record is determined by the schema definition. The SkipList performs an $O(\log N)$ search using the zero-allocation comparison logic to locate existing keys.
*   **Weight Summation**: If a key exists, the incoming weight is added to the existing 64-bit weight field. This immediate coalescing ensures that the MemTable represents the net change of the current ingestion epoch, reducing the volume of data passed to the transmutation and distribution layers.
*   **Atomicity**: Updates are applied to the SkipList only after the entire batch has been successfully committed to the WAL. If a batch fails validation or serialization, the MemTable remains unchanged.

### 6.3. Batch-Level Synchronization
The MemTable tracks the LSN range of its current contents to facilitate synchronization with the DBSP VM and the distribution layer.
*   **LSN High-Water Mark**: The `MemTableManager` maintains the highest LSN successfully applied. This value serves as the input trigger for the persistent DBSP circuits.
*   **Circuit Injection**: As batches are finalized in the MemTable, the resulting deltas are pushed into the input registers of the active DBSP circuits. This ensures that materialized views are updated in near real-time relative to the ingestion stream.

### 6.4. Sealing and Rotation Mechanics
The transition from mutable ingestion to immutable processing is triggered by memory pressure or time-based flush policies.
*   **Atomic Rotation**: When the Arena occupancy reaches the configured threshold, the `MemTableManager` initializes a new Arena and SkipList. The previous instance is marked as "Sealed" and transitioned to a read-only state.
*   **Read-Only Servicing**: During the transmutation phase, the Sealed MemTable remains available for point-queries and snapshot generation. It is only decommissioned and its Arena freed once the resulting Quad-Partition shard has been finalized and recorded in the Spine Manifest.
*   **Annihilation Pruning**: During the iteration of the Sealed MemTable for transmutation, the engine elides any nodes with a net weight of zero. This ensures that records that were inserted and subsequently retracted within the same ingestion epoch do not persist into the FLSM layer.

## 7. Compaction: The JIT-Compiled Merge Loop

### 7.1. Selection Heuristics and Read Amplification
Compaction is triggered by the **Read Amplification** metric, defined as the depth of overlapping shard boundaries for any given key range in the Spine. 
*   **Overlap Density:** The engine identifies "Dirty Shards"—newly flushed fragments with high overlap—and prioritizes them for merging into non-overlapping "Guard Shards."
*   **LSN Progression:** Compaction preserves the temporal ordering of data by merging shards within contiguous or overlapping Log Sequence Number (LSN) windows. The resulting Guard Shard is tagged with the union of the input LSN ranges.

### 7.2. The Tournament Tree Merge Iterator
The core compaction logic is implemented as a multi-way merge sort using a **Tournament Tree** (priority queue) to manage $N$ input shards.
*   **Key Consolidation:** The iterator identifies the minimal key across all shard heads. If multiple shards contain the same key, the engine performs a linear summation of their weights from Region W.
*   **Algebraic Pruning:** If the net weight $\sum w_i$ equals zero, the record is identified as an annihilated "Ghost." The engine advances the heads of all contributing shards without accessing their respective Region V (Value Heap) or Region K (Key Heap) for the payload.
*   **Value Selection:** In cases of conflicting values for the same key across different LSNs (updates), the engine selects the value associated with the highest LSN, consistent with DBSP update semantics.

### 7.3. JIT Specialization of the Merge Loop
The RPython Meta-Tracing JIT optimizes the compaction loop by specializing the trace for the specific number of input shards.
*   **Trace Unrolling:** The Tournament Tree comparisons are unrolled into a linear sequence of machine instructions, eliminating the overhead of generic heap management and function dispatch.
*   **Barrier Enforcement:** The JIT respects the `@jit.dont_look_inside` decorator on materialization methods. This ensures that payload data is only requested from DRAM when the weight summation results in a non-zero value, physically manifesting the Ghost Property during the merge process.

### 7.4. Columnar Re-Partitioning
Surviving records ($W_{net} \neq 0$) are streamed into a new `ShardWriter` to produce a Guard Shard.
*   **Sequential Heap Construction:** Keys and values are appended to the new heaps, and their new relative offsets are recorded in a fresh Offset Spine (Region O).
*   **Vectorized Weight Output:** The consolidated weights are written as a contiguous $i64$ vector in the new Region W, restoring the SoA alignment required for subsequent vectorized scans.

### 7.5. Atomic Version Transition
Upon successful finalization of the Guard Shard, the compaction task submits a **Spine Manifest** update.
*   **Shard Promotion:** The new Guard Shard is atomically swapped into the Spine, and the superseded Dirty Shards are marked for deletion.
*   **Reference Release:** Superseded shards are physically unlinked from the filesystem only after their **Distributed Reference Count** reaches zero, ensuring that active queries or synchronization tasks can complete their operations on the previous snapshot without interruption.
    
## 8. Execution Model: Persistent DBSP Virtual Machine

### 8.1. Persistent Dataflow Circuits
The execution engine maintains long-running **DBSP Circuits** as first-class entities. Unlike traditional query engines that discard state after execution, these circuits are resident in memory and maintain the algebraic traces necessary for incremental view maintenance.
*   **Circuit Registration:** Materialized view definitions are compiled into bytecode and registered in the Catalog. The VM instantiates the circuit, allocating the necessary registers and stateful operator traces.
*   **LSN-Driven Triggering:** The VM operates on a reactive trigger model. When the Ingestion Layer advances the WAL high-water mark, the **Circuit Scheduler** identifies dependent circuits and feeds the new LSN batches into their input nodes.

### 8.2. Operator Trace Management and Checkpointing
Non-linear DBSP operators (Join, Distinct, Aggregate) require internal state—referred to as **Traces**—to process new deltas relative to previous history.
*   **Trace Storage:** Traces are maintained as in-memory Z-Sets within Monotonic Arenas. They utilize the same row-oriented (AoS) format as the MemTable to facilitate rapid insertion and coalescing of new deltas.
*   **State Checkpointing:** To ensure recovery without replaying the entire table history, the VM periodically snapshots operator traces to the FLSM as Quad-Partition shards. These checkpoints are tagged with the LSN of the last processed batch.
*   **Recovery Logic:** Upon restart, the VM loads the latest trace checkpoints and replays only the WAL entries from the checkpoint LSN to the current high-water mark.

### 8.3. Output Serialization: The View Log
The terminal node of every persistent circuit is an `EMIT` operator that serializes output deltas for distribution.
*   **Log Sequencing:** Each output delta is assigned a **View LSN**. These are written sequentially to the **View Log**, which utilizes the Cap'n Proto wire format to enable zero-copy reads for the Distribution Layer.
*   **Algebraic Consistency:** The VM ensures that output deltas are written to the View Log only after the input batch has been fully processed and the operator traces have been updated, maintaining strict consistency between the table state and the view state.

### 8.4. Specialized Dataflow Interpreters
The RPython Virtual Machine utilizes **Meta-Tracing** to collapse the overhead of the bytecode interpreter during high-volume streaming.
*   **Loop Unrolling:** The JIT unrolls the circuit's dataflow graph into a linear sequence of operations. This allows the compiler to optimize the transfer of deltas between operator registers, frequently eliding intermediate allocations.
*   **Predicate Inlining:** Scalar expressions executed by the Micro-Interpreter are inlined directly into the hot traces of the parent operators (e.g., `FILTER`). This enables the JIT to generate branch-free machine code for complex row-level logic.

### 8.5. Multi-Circuit Resource Isolation
To prevent a single complex view from exhausting system resources, the VM enforces isolation between persistent circuits.
*   **Arena Partitioning:** Each circuit is assigned a dedicated set of Monotonic Arenas for its internal traces and intermediate results. This prevents memory fragmentation and allows for bulk deallocation of intermediate state upon circuit termination.
*   **Scheduler Fairness:** The Circuit Scheduler utilizes a round-robin or priority-based mechanism to allocate CPU cycles to active circuits, ensuring that high-velocity input streams do not starve background maintenance tasks or lower-priority views.

## 9. Distribution and Concurrency Model

### 9.1. Multi-Process Architecture
The system utilizes a multi-process model to circumvent the Global Interpreter Lock (GIL) and isolate I/O-intensive distribution and maintenance tasks from the CPU-intensive virtual machine.
*   **The Executor Process**: The primary process owning the `MemTableManager` and the DBSP VM. It serves as the exclusive writer for the WAL, View Logs, and new FLSM shards.
*   **The Sync Server Process**: A dedicated process responsible for managing TCP/Unix socket connections and streaming deltas to remote clients. It operates as a concurrent reader of the View Logs and FLSM shards.
*   **The Compactor Process**: An independent background worker that merges shards and prunes deleted records. It operates as a concurrent reader of older shards and a producer of new Guard Shards.

### 9.2. Single-Writer/Multi-Reader Log Concurrency
Consistency across processes is maintained through a single-writer, multi-reader (SWMR) pattern anchored by the filesystem.
*   **Atomic Log Append**: The Executor appends batches to the WAL and View Logs. Because appends are sequential and LSN-prefixed, the Sync Server can safely tail these files using `inotify` or polling without explicit locking.
*   **Snapshot Consistency**: The Sync Server and Compactor utilize the versioned **Spine Manifest** to obtain a consistent view of the database. Since physical shards and logs are immutable once finalized, readers can operate on a point-in-time snapshot while the Executor continues to ingest new deltas.

### 9.3. Client Session and Cursor Management
The Sync Server maintains the state of all active subscriptions to ensure reliable delta delivery.
*   **The LSN Cursor**: Each client session is associated with a cursor representing the last successfully acknowledged LSN. The Sync Server uses this cursor to determine whether to stream from the Delta Log or trigger a Snapshot Bootstrap.
*   **Backpressure Handling**: The server monitors the delta queue depth for each client. If a client falls behind the garbage-collection window of the logs, the server terminates the incremental stream and signals the client to perform a full resynchronization from the latest Spine snapshot.

### 9.4. Distributed Reference Counting and Reclamation
Physical resource reclamation (unlinking files) is governed by a **Distributed Reference Counter** residing in shared memory or coordinated via IPC.
*   **Handle Acquisition**: Before mapping a shard or log segment, the Sync Server or Compactor increments a global reference counter for that resource.
*   **Deferred Deletion**: When the Executor compacts shards or rotates logs, it marks superseded files for deletion. The `unlink()` syscall is invoked only when the resource's reference count reaches zero. This prevents `SIGBUS` errors and ensures that long-running bootstrap operations can complete using the data that was active at the start of the session.

### 9.5. Inter-Process Communication (IPC)
Coordination between processes is mediated through low-latency IPC channels.
*   **Manifest Propagation**: When the Executor or Compactor updates the Spine, it broadcasts a **Spine Manifest** update signal. The Sync Server receives this signal to update its local view of available Guard Shards for new bootstrap requests.
*   **Lifecycle Signaling**: IPC is used to coordinate the handover of LSN high-water marks from the Ingestion Layer to the VM, and subsequently to the Sync Server once the View Log has been persisted.

## 10. Client Synchronization Protocol

### 10.1. Handshake and Schema Negotiation
The synchronization session begins with a bidirectional handshake to establish protocol compatibility and schema alignment.
*   **Version Verification**: The client provides a protocol version and a unique view identifier. The server validates these against the local **Schema Registry**.
*   **LSN Alignment**: The client submits its `Last_LSN`. If the client is new, it sends a `NULL_LSN` (0), signaling the server to initiate a snapshot-based bootstrap. 
*   **Schema Hash Validation**: To prevent data corruption, the client and server exchange a cryptographic hash of the view's schema definition. If the hashes mismatch, the server terminates the session, requiring the client to update its local table definition before proceeding.

### 10.2. Mode 1: Snapshot Bootstrap
If the client’s `Last_LSN` is 0 or has been purged from the **Delta Replay Log**, the server enters **Snapshot Mode**.
*   **Static Spine Materialization**: The server resolves the current **Spine Manifest** and iterates through the corresponding Quad-Partition shards.
*   **Z-Set Streaming**: The server streams the total accumulated Z-Set state to the client. This stream consists of `(Key, Value, Weight)` tuples serialized in a high-bandwidth Cap'n Proto segment. 
*   **Atomic Finalization**: The bootstrap concludes with a `SNAPSHOT_END` signal containing the specific LSN at which the snapshot was captured. This LSN serves as the anchor point for the transition to live updates.

### 10.3. Mode 2: Live Delta Streaming
Once a client is aligned with a valid LSN, the server transitions to **Live Mode**, providing real-time incremental updates.
*   **Log Tailing**: The Sync Server process reads from the **View Log**, starting at the offset immediately following the client's `Last_LSN`.
*   **Batch Push**: As the DBSP VM finalizes new circuit outputs, the server pushes the resulting deltas to the client. Each push is an atomic Cap'n Proto message containing one or more batches, each tagged with its monotonic LSN.
*   **Client Acknowledgement**: Clients periodically send `ACK_LSN` messages. The server utilizes these acknowledgements to update internal session state and facilitate garbage collection of the Delta Replay Log.

### 10.4. Transition and LSN Convergence
The transition from Snapshot Mode to Live Mode is designed to be seamless and idempotent.
*   **The Catch-up Phase**: If new batches are written to the View Log while a snapshot bootstrap is in progress, the server buffers these deltas in a session-local queue.
*   **Overlap Resolution**: Once the snapshot stream is complete, the server flushes the catch-up queue, starting from the snapshot LSN. Because DBSP operations are additive, any overlap in deltas is algebraically resolved by the client's local storage engine.

### 10.5. Fault Tolerance and Reconnection
The protocol incorporates mechanisms to handle network instability and process restarts.
*   **Heartbeat Mechanism**: Periodic keep-alive signals detect stale connections and prevent firewall-induced timeouts.
*   **Idempotent Resumption**: Upon reconnection, a client re-submits its latest `ACK_LSN`. The server resumes streaming from the subsequent log offset.
*   **Purge Notification**: If a client remains offline longer than the log retention window, the server issues a `RESYNC_REQUIRED` error upon the next handshake, forcing the client to discard its local state and re-initiate a Snapshot Bootstrap.

## 11. The SQL Frontend and Compiler Interface

### 11.1. Schema Registry and DDL Parsing
The frontend serves as the management interface for the **Schema Registry**, utilizing **Apache Calcite** to parse and validate Data Definition Language (DDL) statements.
*   **Table Definitions**: `CREATE TABLE` statements are translated into static schema entries. The compiler maps logical SQL types to specific **Cap'n Proto** primitive types (e.g., `VARCHAR` to `Text`, `BIGINT` to `Int64`), establishing the fixed-width offsets required for the AoS MemTable layout and the Quad-Partition heaps.
*   **Materialized View Definitions**: `CREATE MATERIALIZED VIEW` statements trigger the construction of a persistent DBSP circuit. The compiler validates that all source tables exist in the registry and that the projected types are compatible with the distribution layer's serialization format.
*   **Schema Hashing**: For every registered view, the compiler generates a unique cryptographic hash of the logical schema and its physical mapping. This hash is utilized during the client handshake to ensure structural alignment between the server's View Log and the client's local storage.

### 11.2. Relational-to-DBSP Algebraic Translation
The core of the compiler is the transformation of a logical Relational Algebra tree into a lifted DBSP dataflow graph.
*   **Operator Mapping**: Standard relational operators are converted into their bilinear or linear DBSP equivalents. A SQL `JOIN` is transformed into a weight-multiplication operator ($w_{out} = w_L \times w_R$), while `UNION ALL` is mapped to Z-Set addition. 
*   **Incremental Lifting**: The compiler applies the DBSP lifting transformation, ensuring that the circuit computes only the changes ($\Delta$) to the output view. This involves injecting integral and differential operators to manage the accumulation of state over time.
*   **Sub-query Optimization**: Calcite’s optimizer performs predicate pushdown and join reordering within the logical tree before it is passed to the DBSP transformer, ensuring the generated bytecode minimizes the size of internal operator traces.

### 11.3. Bytecode Emission and Stride Resolution
The compiler serializes the optimized DBSP graph into the engine's custom Instruction Set Architecture (ISA) for the RPython VM.
*   **Register Allocation**: Each edge in the dataflow graph is assigned a VM register. The compiler calculates the lifetime of intermediate deltas to enable register reuse, minimizing the memory footprint of the active circuit.
*   **Scalar Compilation**: Expressions in `WHERE` or `SELECT` clauses are compiled into stack-based bytecode for the **Scalar Micro-Interpreter**. The compiler ensures these instructions are placed behind materialization barriers to maintain the Ghost Property.
*   **Stride Calculation**: For every operator, the compiler pre-calculates the fixed-width strides for Region W and Region O based on the physical schema. These constants are embedded in the bytecode, allowing the VM to perform $O(1)$ coordinate arithmetic during scans.

### 11.4. Dependency Graph and Trigger Logic
The frontend maintains a global **Dependency Graph** that links physical tables to their dependent materialized views.
*   **Trigger Mapping**: When the Ingestion Layer commits a batch to a table, the graph identifies all dependent circuits. The compiler generates the "Wakeup" logic that the **Circuit Scheduler** uses to feed the table's LSN deltas into the appropriate circuit input registers.
*   **Pipeline Coordination**: For complex view-on-view dependencies, the compiler establishes the execution order to ensure that deltas propagate through the hierarchy in a single LSN epoch, maintaining global algebraic consistency across the entire synchronized state.

### 11.5. Checkpoint Planning
During compilation, the frontend determines the optimal checkpointing strategy for stateful operators based on the estimated size of their traces.
*   **Trace Schema Definition**: The compiler defines the physical layout for the Quad-Partition shards used in trace checkpointing. These shards mirror the structure of the primary table shards but are optimized for the specific key-value pairs required by join indexes or aggregation groups.
*   **LSN Anchor Points**: The emitted bytecode includes instructions for the VM to periodically signal the storage layer to finalize trace Arenas and record their LSN offsets in the Manifest, facilitating rapid recovery.

## 12. Implementation Plan

### Phase 0: Physical Foundation [COMPLETED]
*   **Zero-Allocation Memory Management**: Implementation of `Arena` bump-pointer allocators and `MappedBuffer` view abstractions for GC-independent memory access.
*   **Quad-Partition Storage**: Development of `ShardWriter` and `ShardView` to enforce the Structure of Arrays (SoA) layout across Regions W, O, K, and V.
*   **High-Velocity Ingestion Buffer**: Implementation of the row-oriented (AoS) SkipList MemTable with in-place algebraic weight coalescing.
*   **Transmutation Pipeline**: Development of the unzipping logic to bridge the AoS MemTable with the columnar persistent shards.
*   **Basic Key Resolution**: Implementation of the `Spine` and `Engine` point-query coordinators for algebraic weight summation across memory and disk.

### Phase 1: Ingestion and Durability [PENDING]
*   **Cap'n Proto Integration**: Incorporation of the `capnpy` library and definition of the canonical `Batch` and `UpsertBatch` schemas.
*   **Schema Registry**: Development of the validation layer and persistent catalog for table and view definitions.
*   **Write-Ahead Log (WAL)**: Implementation of the durable LSN-prefixed log and the recovery mechanism to reconstruct MemTable state from Cap'n Proto segments.

### Phase 2: Persistent DBSP Virtual Machine [PENDING]
*   **Circuit Runtime**: Development of the register-based interpreter with support for long-running, stateful dataflow graphs.
*   **Operator Trace Management**: Implementation of in-memory traces for Joins and Aggregates using AoS Arenas.
*   **Trace Checkpointing**: Development of the logic to snapshot operator state into Quad-Partition shards for LSN-anchored recovery.
*   **Scalar Micro-Interpreter**: Implementation of the stack-based bytecode evaluator for predicate and projection logic.

### Phase 3: Distribution and Sync Server [PENDING]
*   **Multi-Process Coordination**: Implementation of the IPC signaling layer between the Executor and Sync Server processes.
*   **View Log Management**: Development of the sequential output logs for materialized views using Cap'n Proto.
*   **Sync Protocol**: Implementation of the Handshake, Snapshot Bootstrap, and Live Mode transitions.
*   **Delta Replay Log**: Development of the history retention and garbage collection logic for the distribution tail.

### Phase 4: Compiler and SQL Integration [PENDING]
*   **Relational Translation**: Integration of Apache Calcite and development of the `RelVisitor` to transform SQL trees into lifted DBSP circuits.
*   **Bytecode Emission**: Implementation of the ISA generator, including register allocation and stride constant resolution.
*   **Schema Hashing**: Development of the cryptographic verification logic for client-server structural alignment.

### Phase 5: Compaction and Lifecycle Management [PENDING]
*   **Tournament Tree Compactor**: Development of the independent background process for multi-way shard merging and Ghost record pruning.
*   **Manifest Persistence**: Implementation of the durable Spine Manifest and the atomic swap mechanism for versioned snapshots.
*   **Distributed Reference Counting**: Implementation of the shared-memory coordination layer for safe `mmap` reclamation and deferred file unlinking.
    
## 13. Future Work and Edge Cases

### 13.1. Dynamic View Projection
The current synchronization protocol assumes clients subscribe to an entire materialized view. Future iterations will implement **Dynamic View Projection**, allowing clients to submit a predicate (e.g., `WHERE region_id = 42`) during the handshake.
*   **Server-Side Filtering**: The Sync Server applies the predicate to the View Log deltas before transmission.
*   **Algebraic Integrity**: Since filters are linear operators in DBSP, the server-side projection maintains algebraic consistency, ensuring the client receives exactly the subset of $+1/-1$ deltas required to maintain a partial local replica.

### 13.2. Distributed Z-Set Partitioning
To support horizontal scaling, the engine will implement **Key-Range Partitioning** across multiple physical nodes.
*   **Partitioned View Logs**: The global LSN sequence remains unified, but the View Log is sharded based on a hash or range of the record key.
*   **Exchange Operators**: The VM ISA will be extended with an `EXCHANGE` instruction to facilitate cross-node data shuffles during join operations, maintaining the Z-Set algebraic invariants across a distributed network.

### 13.3. Portable Delta Application (WASM)
To facilitate integration with browser-based environments and mobile applications, a **Portable Delta Application** library will be developed in WebAssembly (WASM).
*   **Standardized Logic**: This library provides a hardened implementation of the delta-to-SQL translation logic, allowing any client with a WASM runtime to apply Cap'n Proto View Logs to a local SQLite or DuckDB instance with minimal integration overhead.
*   **Zero-Copy Handover**: The WASM runtime will utilize shared memory buffers to read Cap'n Proto segments directly from the network stack, minimizing CPU cycles spent on the client-side synchronization hot path.

### 13.4. Metadata-Only DDL Transitions
The Quad-Partition format’s reliance on opaque heaps enables **Lazy Schema Evolution** for certain structural changes.
*   **Non-Breaking Changes**: Operations such as adding a nullable column or renaming a field are handled via the Schema Registry without rewriting physical shards.
*   **Background Re-Transmutation**: For breaking changes (e.g., type conversions), the engine initiates a background maintenance task. This task reads existing shards, applies the transformation during a specialized compaction loop, and atomically updates the Spine Manifest once the transition is complete.

### 13.5. Tiered Storage Compaction Heuristics
While the current FLSM uses a basic read-amplification trigger, future work will introduce **Tiered Compaction Heuristics** optimized for different workload profiles.
*   **Write-Heavy Tiers**: Minimizes write stalls by allowing higher overlap depth in the upper levels of the FLSM.
*   **Read-Optimized Guard Shards**: Prioritizes the consolidation of shards into large, non-overlapping blocks to minimize the number of `mmap` handles and binary search steps required for snapshot generation.

### 13.6. Differential Privacy Integration
As a reactive engine, GnitzDB is uniquely positioned to implement **Differential Privacy** at the circuit level. By injecting controlled noise into the DBSP aggregation operators, the engine can produce View Logs that satisfy formal privacy guarantees, enabling the synchronization of sensitive datasets to lower-trust client environments without exposing individual-level record data.
