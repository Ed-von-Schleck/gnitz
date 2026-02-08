## 1. Introduction and Theoretical Foundation

### 1.1. Theoretical Framework: DBSP and Universal Z-Sets
GnitzDB is built on **Database Stream Processing (DBSP)**, a mathematical framework that treats all computations as operations over an **Abelian Group** $(G, +, 0)$. 
*   **Unified Data Representation:** The system utilizes **Z-Sets** (Generalized Multisets) as the universal primitive for all data in transit and at rest. A Z-Set maps records—represented as Entity-Component pairs—to integer weights $w \in \mathbb{Z}$.
*   **Algebraic Homomorphism:** Every state transition, persistent log entry, and distribution delta is expressed as a Z-Set. By utilizing the linearity of DBSP operators, the engine maintains global consistency through the identity:
    $$ Q(\sum \Delta I) = \sum Q(\Delta I) $$
*   **The Unified Log:** The Write-Ahead Log (WAL), Operator Traces, and Synchronization streams are mathematically equivalent. This algebraic uniformity allows the engine to treat ingestion, maintenance, and distribution as a single, continuous Z-Set transformation pipeline.

### 1.2. The Reactive ECS Data Model
The system replaces relational tables with an **Entity Component System (ECS)** architecture, optimizing for high-velocity state changes and reactive simulations.
*   **Entities:** Unique 64-bit unsigned integers serving as the primary join key across all Z-Set streams.
*   **Components:** User-defined structures associated with entities. Each component type is a distinct Z-Set stream $(\Delta E, \Delta C, \Delta W)$.
*   **Systems:** Materialized views implemented as persistent DBSP circuits. Systems consume deltas from input Z-Sets and produce new output Z-Sets, which can be fed back into the ingestion layer or another GnitzDB instance.

### 1.3. Objective: The Unified Z-Set Engine
The engine is designed for the incremental maintenance and distribution of complex state via a single source of truth.
*   **WAL-Centric Distribution:** The traditional "View Log" is deprecated. The **Z-Set WAL** serves as the unified stream for both local crash recovery and remote client synchronization. Clients subscribe to component views by tailing and filtering the WAL.
*   **Idempotent Synchronization:** Remote mirrors maintain state by additively applying Z-Set deltas. The algebraic nature of the stream ensures that any prefix of the WAL can be replayed to reach a consistent state baseline.
*   **Snapshot-to-Stream Continuity:** Point-in-time snapshots are resolved by algebraically summing persistent columnar shards, providing a $w_{net}$ baseline before transitioning to live WAL tailing.

### 1.4. Performance Mandate: JIT and the Ghost Property
Implementation in **RPython** enables a **Meta-Tracing Virtual Machine** to specialize execution at runtime based on the Z-Set structure.
*   **Dynamic Layout Promotion:** The `ComponentLayout` registry calculates physical strides and offsets. The RPython JIT promotes these to constants, specializing the assembly for specific component structures.
*   **The Ghost Property:** Algebraic weights are decoupled from component payloads. Records with a net weight of zero (annihilated) are bypassed during DRAM fetches and I/O, ensuring that only records with $w_{net} \neq 0$ consume CPU cycles.
*   **German String Optimization:** String data uses a 16-byte inline structure (Length, Prefix, Payload/Offset). This enables $O(1)$ equality failures and elides heap access for the majority of Z-Set comparisons, maximizing cache-line efficiency during vertical joins.

## 2. System Architecture Overview

### 2.1. Ingestion Layer and Registry
The ingestion layer manages the entry of entity updates and validates structural integrity against a dynamic type system.
*   **Component Registry:** A persistent catalog mapping component hashes to physical **ComponentLayout** descriptors. It defines the stride and field offsets for user-defined component structs.
*   **Bundle Ingestion:** Updates are submitted as atomic "Bundles"—groups of components associated with a single Entity ID. These are serialized into a row-oriented format (Array of Structs) for cache-localized writing.
*   **Staging MemTable:** In-memory ingestion utilizes a row-oriented SkipList within a **Monotonic Arena**. This structure sorts incoming bundles by Entity ID, facilitating rapid algebraic coalescing of high-frequency updates before persistence.

### 2.2. Runtime Layer: Persistent DBSP VM
The runtime executes reactive circuits defined over component streams.
*   **Vertical Join Execution:** The core operation is the joining of independent component Z-Sets on Entity ID. The VM utilizes vectorized zip-join algorithms on sorted component arrays to align entities efficiently.
*   **Circuit Scheduler:** Manages the dataflow between component streams. When a specific component type (e.g., `Position`) receives updates, only the dependent circuits are triggered.
*   **State Management:** The VM maintains persistent operator state (e.g., differential dataflow traces) using the same columnar storage format as the base data, ensuring uniformity in storage management.

### 2.3. Storage Layer: Decomposed Columnar FLSM
Persistence is managed by a **Fragmented Log-Structured Merge (FLSM)** tree, physically organized into independent columnar shards.
*   **ECS Shards:** Data is stored in typed, immutable shards. Each shard contains data for exactly one component type, sorted by Entity ID.
*   **Transmutation Pipeline:** A background process transforms the row-oriented (AoS) Staging MemTable into column-oriented (SoA) ECS Shards. This involves "unzipping" the component bundles into distinct physical files.
*   **The Spine:** A lightweight in-memory index tracking the `[MinEntityID, MaxEntityID]` ranges for every active shard. This enables $O(\log N)$ point lookups by identifying candidate shards without disk access.

### 2.4. Distribution Layer: Reactive Streaming
The distribution layer synchronizes remote client state with the server's component views.
*   **Component Subscriptions:** Clients subscribe to specific component types or query results.
*   **Delta Push:** The server streams component-level deltas (additions/removals) serialized via Cap'n Proto.
*   **Snapshot Resolution:** For bootstrapping, the distribution layer performs a merge-scan across the FLSM Spine to reconstruct the net state of requested components at a specific Log Sequence Number (LSN).

## 3. Memory Subsystem and Physical Layout

### 3.1. Monotonic Arena Allocation
Memory for ingestion and intermediate state is managed via **Monotonic Arenas** to bypass garbage collection overhead and ensure deterministic memory locality.
*   **Bump-Pointer Mechanics:** Allocation is $O(1)$ via a pointer increment within pre-allocated contiguous blocks.
*   **Alignment Enforcement:** The allocator enforces 8-byte alignment to support direct casting of raw pointers to primitive types (`int64`, `double`), preventing unaligned access faults during vectorized summation and JIT-optimized fetches.

### 3.2. Physical Shard Layout
Each ECS Shard is a memory-mapped file organized into five contiguous regions (**Penta-Partition**), aligned to 64-byte boundaries for AVX-512 compatibility and cache-line isolation.
*   **Region E (Entities):** A sorted vector of 64-bit unsigned Entity IDs.
*   **Region W (Weights):** A vector of 64-bit signed integers representing algebraic Z-Set weights.
*   **Region C (Components):** A vector of fixed-stride component data, containing inline primitives and 16-byte German String structs.
*   **Region B (Blob Heap):** A variable-length heap storing overflow data for German Strings (length > 12 bytes).

### 3.3. JIT-Compiled Stride Access
The engine leverages the RPython Meta-Tracing JIT to optimize variable-stride access patterns within Region C.
*   **Constant Promotion:** The `ComponentLayout` (stride and field offsets) for a shard is promoted to an immutable constant during trace compilation.
*   **Stride Specialization:** The JIT specializes the offset arithmetic `base + (index * stride)` into machine instructions with immediate constants, eliminating dynamic arithmetic overhead during columnar scans.

### 3.4. German String Optimization
String fields utilize a hybrid 16-byte inline structure to maximize cache efficiency and maintain the **Ghost Property**.
*   **Structure:** `Bytes 0-3`: Length (`u32`); `Bytes 4-7`: Prefix (`char[4]`); `Bytes 8-15`: Payload (Union of 8-byte suffix or 64-bit heap offset).
*   **Equality Logic:** $O(1)$ equality failure is achieved via length and prefix checks before triggering Region B heap access.
*   **Ghost Property Integration:** Records with a net weight of zero are identified via Region W before German String comparison, preventing unnecessary heap pointer chasing for annihilated records.

### 3.5. Transmutation and Relocation
The transition from MemTable to Shard involves an "Unzipping" pipeline that transforms row-oriented buffers into columnar regions.
*   **Weight Serialization:** Algebraic weights accumulated in the MemTable SkipList are serialized directly into Region W.
*   **Pointer Swizzling:** During transmutation, offsets for "Long" German Strings are recalculated from MemTable arena-relative offsets to Shard-relative Region B offsets, ensuring shard immutability and self-containment.

## 4. Storage Architecture: Fragmented LSM (FLSM)

### 4.1. The Binary Manifest
The storage state is anchored by a versioned binary **Manifest** (`MANIFNGT`).
*   **Atomic Updates:** Manifest updates utilize a temporary swap-file and atomic `rename()` mechanics to provide consistent, point-in-time snapshots of the active shard set.
*   **Metadata Tracking:** For every shard, the manifest records the Component ID, Entity ID range (`min`, `max`), and Log Sequence Number range (`min_lsn`, `max_lsn`).

### 4.2. Vertical Decomposition and Sorted Alignment
Data is physically partitioned by Component ID into independent columnar shards.
*   **Entity Sorting:** All shards are strictly sorted by Entity ID to enable $O(\log N)$ binary search lookups and linear-time Vertical Joins.
*   **Component Isolation:** Querying a specific attribute only triggers I/O for relevant component shards, maximizing memory bandwidth efficiency.

### 4.3. Compaction: The Vertical Merge
Compaction reduces read amplification by merging overlapping shards into a consolidated "Guard" shard.
*   **Tournament Tree Merge:** An N-way merge sort processes input shards by Entity ID.
*   **MergeAccumulator Logic:** For each Entity ID, a `MergeAccumulator` calculates the net algebraic weight and tracks the payload pointer associated with the highest LSN.
*   **Algebraic Pruning:** If the net weight sums to zero, the record is discarded (Annihilation). This physically realizes the Ghost Property by reclaiming space from deleted or balanced state.

### 4.4 . Read Amplification and Triggers
The **ShardRegistry** monitors the structural health of the FLSM.
*   **Metrics:** Read Amplification is defined as the number of overlapping shards covering a specific Entity ID.
*   **Heuristic Trigger:** When Read Amplification exceeds a configured threshold (default: 4), the component is flagged for automated compaction to restore $O(\log N)$ lookup performance.

## 5. Physical Encoding and Serialization

### 5.1. ECS Shard Header Specification
Each ECS Shard begins with a fixed 64-byte header. All multi-byte integers are encoded in **Little Endian** format.

| Byte Offset | Field Name | Type | Description |
| :--- | :--- | :--- | :--- |
| 00 - 07 | Magic Number | `u64` | Constant: `0x31305F5A54494E47` |
| 08 - 15 | Entry Count | `u64` | Total number of entities in this shard |
| 16 - 31 | Reserved | `u8[16]` | Reserved for versioning and alignment |
| 32 - 39 | Offset E | `u64` | Byte offset to Region E (Entity IDs) |
| 40 - 47 | Offset C | `u64` | Byte offset to Region C (Component Data) |
| 48 - 55 | Offset B | `u64` | Byte offset to Region B (Blob Heap) |
| 56 - 63 | Offset W | `u64` | Byte offset to Region W (Algebraic Weights) |

### 5.2. Region Alignment and Padding
The physical layout enforces 64-byte alignment for all region boundaries to optimize AVX-512 utilization and prevent cache-line splits.
*   **Region Transitions:** Padding bytes (`0x00`) are inserted between Regions E, W, C, and B. Each region start must satisfy `(offset % 64 == 0)`.
*   **Checksum Placement:** A 64-bit checksum is appended to the end of the Header and each individual Region (E, W, C, B). Checksums are themselves aligned to 8-byte boundaries within the 64-byte padding block.

### 5.3. Region E: Entity ID Vector
Region E contains a dense, sorted sequence of 64-bit unsigned integers.
*   **Vectorization:** Ascending order enables $O(\log N)$ binary search and vectorized SIMD intersection logic during joins.
*   **Integrity:** Validated by a trailing **XXH3** checksum.

### 5.4. Region W: Weight Vector
Region W contains a dense sequence of 64-bit signed integers representing algebraic Z-Set weights.
*   **Mapping:** Weights are mapped 1-to-1 by index to Entity IDs in Region E.
*   **Partial Validation:** The separation of W from C allows the engine to verify entity existence and net weight without reading component payloads.

### 5.5. Region C: Component Data Vector
Region C stores fixed-stride component records defined by the **ComponentLayout**.
*   **Inline Primitives:** Numeric types are stored at offsets calculated during schema registration.
*   **German Strings:** String fields occupy 16-byte slots containing length, a 4-byte prefix, and either an 8-byte inline suffix (for length $\le 12$) or a 64-bit relative offset into Region B.

### 5.6. Region B: The Blob Heap
Region B provides unstructured overflow storage for variable-length string data.
*   **Compaction:** During transmutation, only blobs referenced by records with non-zero net weights are copied, effectively performing garbage collection of annihilated string state.
*   **Addressing:** Accessed exclusively via offsets stored in Region C.

### 5.7. Z-Set Write-Ahead Log (WAL) Format
The WAL is a sequence of append-only blocks, each representing a Z-Set batch associated with a specific **Log Sequence Number (LSN)**.
*   **Block Header:**
    *   `u64` LSN
    *   `u32` Component ID
    *   `u32` Batch Entry Count
    *   `u64` Block Checksum (XXH3)
*   **Block Body:** A contiguous stream of Z-Set records. Each record consists of a 64-bit Entity ID, a 64-bit signed Weight, and the packed Component payload (including German Strings and required inline blobs).
*   **Unified Streaming:** The WAL serves as the primary distribution log; clients synchronize state by tailing the WAL and filtering for specific Component IDs.

### 5.8. Data Integrity and Trailing Checksums
To support the **Ghost Property** at the integrity layer, GnitzDB utilizes **Trailing Region Checksums**.
*   **Partial Validation:** Readers validate the integrity of specific regions (e.g., E and W) independently. If an entity is annihilated ($w_{net} = 0$), the engine avoids I/O and checksum validation for Region C and B.
*   **Algorithm:** **XXH3-64** is used for all regions to maximize performance on modern CPUs while ensuring high collision resistance.
*   **Header Integrity:** The 64-byte header includes its own checksum at bytes 24-31 (using the reserved block) or as a trailing trailer before Region E.
    
## 6. The MemTable: High-Velocity Z-Set Ingestion

### 6.1. The Ingestion Pipeline and Unified Log
The MemTable is the mutable, in-memory write-head of the engine, optimized for the rapid absorption of row-oriented Z-Set deltas. Ingestion follows a strict, durable pipeline:
1.  **Schema Enforcement:** Incoming deltas are validated against the **Component Registry** to ensure physical layout compatibility.
2.  **LSN Assignment:** Each ingestion batch is assigned a unique, monotonic **Log Sequence Number (LSN)**.
3.  **Z-Set WAL Serialization:** Before application to memory, the delta is serialized to the **Z-Set Write-Ahead Log**. This log stores the triple ($\Delta E, \Delta C, \Delta W$), serving as the source of truth for both crash recovery and live distribution.

### 6.2. AoS SkipList for Algebraic Coalescing
The MemTable utilizes a SkipList indexed by Entity ID to perform immediate, in-place algebraic summation of weights at ingestion time.
*   **Algebraic Accumulation:** The SkipList performs an $O(\log N)$ search to locate existing nodes. If found, the incoming weight is added to the node's 64-bit weight field.
*   **In-Memory Annihilation:** If high-frequency updates within the MemTable result in a net weight of zero, the entity is marked as annihilated.

### 6.3. Physical Node Layout
Nodes are laid out as contiguous byte sequences within a **Monotonic Arena** to minimize allocation overhead and ensure cache locality.
*   `[00-07]` **Weight**: 64-bit signed integer ($w \in \mathbb{Z}$).
*   `[08-08]` **Height**: 8-bit unsigned integer (SkipList tower height).
*   `[09-11]` **Padding**: Ensures 4-byte alignment for the pointer array.
*   `[12-XX]` **Next-Pointer Array**: Height-indexed array of 32-bit Arena offsets.
*   `[XX-YY]` **Entity ID**: 64-bit unsigned integer key.
*   `[YY-ZZ]` **Component Bundle**: Fixed-stride raw byte payload matching the `ComponentLayout`.

### 6.4. Sealing and Transmutation (Unzipping)
The transition from mutable memory to immutable persistence is triggered by Arena occupancy or LSN commit policies.
*   **Atomic Rotation:** The active MemTable is marked as "Sealed" (read-only), and a new, empty SkipList/Arena pair is instantiated.
*   **Columnar Demultiplexing:** The Transmutation Pipeline performs a linear scan of the Sealed SkipList. It "unzips" the row-oriented (AoS) nodes, separating the Entity IDs, Weights, and Component fields into the distinct regions (E, W, C, B) of a new **ECS Shard**.

### 6.5. Survivor Blob Compaction
The transmutation process utilizes algebraic weights to minimize physical storage footprint and I/O.
*   **Annihilation Pruning:** Nodes with $w_{net} = 0$ are discarded. The pipeline elides all I/O for these records, preventing "Ghost" data from reaching the disk.
*   **Blob Relocation:** For surviving records with "Long" German Strings, the payload is read from the MemTable's Blob Arena and copied into the shard's **Region B**.
*   **Offset Swizzling:** The 64-bit offset within the German String struct is updated from the temporary Arena offset to the persistent Shard-relative offset. This ensures that Region B contains a compacted, sequential heap of only relevant string data.

### 6.6. Reactive Feedback Integration
The MemTable ingestion layer is designed to accept Z-Set deltas produced by the DBSP VM (Section 8).
*   **Recursive Ingestion:** View deltas generated by internal circuits are fed back into the MemTable as new ingestion batches.
*   **LSN Consistency:** Feedback deltas are processed with the same algebraic guarantees as external updates, allowing for complex, multi-stage state transformations within a unified LSN sequence.

## 7. Compaction: The JIT-Compiled Merge Loop

### 7.1. Component-Scoped Compaction
Compaction operates independently on the shards of a single component type to maintain read performance and reclaim physical storage.
*   **Trigger Heuristic:** Compaction is triggered for a component when the `ShardRegistry` detects that its Read Amplification—the number of overlapping shards for a given Entity ID range—exceeds a configured threshold.
*   **Goal:** To merge multiple overlapping, heterogeneous shards into a single, large, non-overlapping "Guard" shard.

### 7.2. The Tournament Tree Merge
The core compaction logic employs a **Tournament Tree** (a priority queue) to execute an N-way merge sort of shards, keyed by Entity ID.
*   **Cursor Management:** Each shard is accessed via a `StreamCursor` that maintains a pointer to the current Entity ID in Region E.
*   **Minimal ID Extraction:** The tournament tree yields the globally minimal Entity ID across all input shards in each iteration, allowing the engine to align all contributions for that entity.

### 7.3. Algebraic Consolidation and Pruning
For each unique Entity ID, the engine aggregates contributions using a `MergeAccumulator`.
*   **Net Weight Summation:** The engine performs an algebraic summation of all weights ($w \in \mathbb{Z}$) from the contributing shards for the given Entity ID.
*   **Annihilation (The Ghost Property):** If the net weight $\sum w_i$ sums to zero, the entity is identified as annihilated. The record is discarded, and no payload data is materialized from Region C or Region B.
*   **LSN-Aware Value Resolution:** For non-annihilated entities, the engine selects the component payload from the shard with the highest Log Sequence Number (LSN), ensuring Last-Write-Wins semantics.

### 7.4. JIT-Compiled Specialized Merge
The RPython Meta-Tracing JIT optimizes the merge loop to maximize throughput.
*   **Trace Specialization:** The JIT specializes the merge logic for the specific component stride and field offsets of the shard layout.
*   **Materialization Barrier:** The JIT is prevented from hoisting component data fetches above the weight summation check. This ensures that payload data is only read from disk or mapped memory for entities that possess a non-zero net weight, physically realizing the Ghost Property at the I/O level.

### 7.5. Columnar Re-Partitioning
Surviving entities ($W_{net} \neq 0$) are streamed into a new `ECSShardWriter`.
*   **Sequential Writes:** The consolidated entities, weights, component data, and relocated strings are written sequentially into new E, W, C, and B regions, respectively.
*   **Atomic Manifest Update:** Upon successful finalization of the new Guard Shard, the engine updates the Manifest and Registry atomically, replacing the input shards with the new generation.
    
## 8. Execution Model: Persistent DBSP Virtual Machine

### 8.1. Register-Based Z-Set VM
The engine executes reactive circuits via a register-based Virtual Machine optimized for high-velocity Z-Set manipulation. Registers in this ISA hold references to multiset buffers containing aligned Entity, Weight, and Component regions.
*   **Delta Registers ($R_\Delta$):** Transient registers holding incremental Z-Set deltas for the current LSN epoch. 
*   **Trace Registers ($R_T$):** Persistent registers holding the indexed Z-Set state (Traces) required for non-linear operations. Traces are physically stored as immutable Penta-Partition shards in the FLSM.

### 8.2. Operational Primitives and Join Lowering
The ISA implements the core operators of DBSP calculus, treating every input and output as an algebraic Z-Set.
*   **Linear Operators:** `FILTER`, `MAP`, and `UNION` operate via algebraic addition. These require no historical state and process deltas in isolation.
*   **Non-Linear Operators:** `JOIN_V` (Vertical Join) and `JOIN_T` (Trace Join) correlate deltas with Traces.
*   **Sequential Join Topology:** Complex multi-component joins (e.g., $A \Join B \Join C$) are lowered by the compiler into a sequence of binary `JOIN_V` operations. The intermediate Z-Sets are materialized in transient registers before being joined with subsequent components.
*   **Temporal Operators:** `INTEGRATE` ($I$) and `DIFFERENTIATE` ($D$) manage the transformation between Z-Set streams and snapshots.

### 8.3. Vectorized Vertical Zip-Join
Entity reconstruction is performed via the **Vertical Zip-Join**. Because input shards and Traces are strictly sorted by Entity ID, the join is implemented as a cache-localized, vectorized merge-scan.
*   **Algorithm:** The VM advances dual pointers across input Region E vectors. Intersection at a specific Entity ID triggers the multiplication of weights ($w_l \times w_r$) and the materialization of the joined Component payload.
*   **Optimization:** The $O(N+M)$ complexity ensures predictable performance even during massive state updates.

### 8.4. Persistent Trace Management
Stateful operators utilize Traces to store the necessary history for incremental computation. 
*   **Trace Maintenance:** The `TRACE_ADD` instruction appends the current $R_\Delta$ to a persistent Trace. This triggers a background merge-sort within the FLSM, identical to the shard compaction process.
*   **Z-Set Uniformity:** Traces are stored in the Penta-Partition format, allowing them to be queried or compacted using the same logic as base component shards.

### 8.5. Reactive Circuit Scheduling
Execution is driven by an event-driven **Circuit Scheduler** triggered by LSN commits in the WAL.
*   **Reactive Trigger Graph:** Systems are organized in a Directed Acyclic Graph (DAG) based on Component ID subscriptions. 
*   **Topological Dispatch:** Upon an LSN commit, the scheduler identifies all dependent systems and executes them in topological order, ensuring all Z-Sets are resolved within the same LSN epoch.

### 8.6. Materialization Barriers and the Ghost Property
The VM enforces the **Ghost Property** through materialization barriers that guard payload access.
*   **Weight-Gated Execution:** The VM inspects the algebraic weight in Region W before processing scalar logic. 
*   **Annihilation Bypass:** If an entity's net weight is zero, the VM elides all fetches for Region C and Region B. This prevents pipeline stalls and ensures that irrelevant data never occupies CPU cache lines or consumes memory bandwidth.

### 8.7. Unified Output Serialization and Feedback
The terminal `EMIT` instruction handles the propagation of circuit results. 
*   **Unified Z-Set WAL:** There are no dedicated "View Logs." The `EMIT` opcode appends the finalized Z-Set deltas to the unified WAL. 
*   **Feedback Loops:** Circuit outputs can be fed back into the MemTable ingestion layer or transmitted to another GnitzDB instance. This allows for inter-instance Z-Set propagation, where the output delta of one system serves as the input delta for another, maintaining global algebraic consistency across distributed nodes.

## 9. Distribution and Concurrency Model

### 9.1. Multi-Process Architecture
The system employs strict process isolation to maximize I/O parallelism and bypass the Global Interpreter Lock (GIL).
*   **The Executor Process:** Owns the `MemTableManager` and DBSP VM. It is the exclusive writer for the Z-Set WAL, finalized ECS Shards, and the active Manifest.
*   **The Sync Server Process:** A concurrent reader process that manages remote client sessions. It tails the unified Z-Set WAL and performs snapshot merges from persistent shards.
*   **The Compactor Process:** A background worker that merges overlapping shards of a specific component type, producing consolidated Guard Shards and updating the Manifest.

### 9.2. Single-Writer/Multi-Reader (SWMR) Consistency
Concurrency is anchored by the filesystem and the immutability of the Penta-Partition shard format.
*   **Manifest Authority:** The **Manifest** is the authoritative source of truth for the active shard set. Updates are performed via an atomic `rename()` of a temporary manifest file, providing consistent point-in-time views to reader processes.
*   **Immutable Shards:** Once a shard is finalized and its XXH3 checksums are written, it is never modified. Readers safely `mmap` shards without coordination locks.

### 9.3. Distributed Reference Counting and Deferred Deletion
Physical file reclamation is managed via a distributed `RefCounter` to ensure reader safety.
*   **SIGBUS Protection:** Processes increment a reference count upon mapping a shard. If the Compactor supersedes a shard, it marks the file for deletion in the `RefCounter`.
*   **Physical Unlink:** The actual `unlink()` syscall is deferred until the global reference count reaches zero. The `Spine.close_all()` routine ensures handles are released exactly once, triggering the final cleanup of obsolete Penta-Partition files.

### 9.4. WAL-Based Inter-Process Communication
The **Z-Set WAL** serves as the primary data conduit between the Executor and the Sync Server.
*   **Tailing Semantics:** The Sync Server tails the append-only WAL, treating it as a unified Z-Set stream. This eliminates the need for dedicated "View Logs" and ensures that ingestion and distribution are chronologically and algebraically aligned.

## 10. Client Synchronization Protocol

### 10.1. Handshake and Schema Validation
The session begins with a structural handshake to ensure binary compatibility between the server's layout and the client's local state.
*   **Schema Hash Validation:** Clients and servers exchange cryptographic hashes of the `ComponentLayout`. Mismatched hashes terminate the session to prevent memory corruption during raw Z-Set application.
*   **LSN Anchor:** The client submits its `Last_ACK_LSN`. A value of zero initiates a full state bootstrap.

### 10.2. Mode 1: Snapshot Bootstrap
The server generates a baseline state image by algebraically resolving the persistent FLSM layer.
*   **Penta-Partition Merge-Scan:** The server performs an N-way merge-scan across all shards associated with the requested Component ID. 
*   **Algebraic Pruning:** The `MergeAccumulator` sums weights across shards. Only records with a net weight of $+1$ are serialized and streamed to the client.
*   **LSN Transition:** The stream terminates with a `SNAPSHOT_END` signal containing the snapshot's LSN, which serves as the starting cursor for live tailing.

### 10.3. Mode 2: Live Z-Set Tailing
Incremental updates are pushed as they appear in the unified Z-Set WAL.
*   **Filtering and Propagation:** The Sync Server tails the WAL, filtering Z-Set deltas $(\Delta E, \Delta C, \Delta W)$ by the client's subscribed Component IDs.
*   **Idempotent Additive Application:** Because GnitzDB protocol is based on additive Z-Set deltas, synchronization is natively idempotent. Clients can re-apply previously received deltas without violating state consistency, simplifying fault-tolerant recovery and network retransmissions.
*   **Backpressure and Purge:** If a client's LSN cursor falls behind the WAL's retention window, the server issues a `RESYNC_REQUIRED` signal, forcing the client to re-initiate a Snapshot Bootstrap.

## 11. The SQL Frontend and Compiler Interface

### 11.1. Schema Registry and DDL Translation
The frontend serves as the entry point for defining the reactive data model, utilizing **Apache Calcite** for DDL validation and relational algebra construction.
*   **Component Physicalization:** `CREATE COMPONENT` statements are translated into canonical **ComponentLayout** descriptors. The compiler calculates the strides for the Penta-Partition shard format, enforces field alignment, and maps logical types to the 16-byte German String structure.
*   **Schema Hashing:** A unique cryptographic hash is generated for every `ComponentLayout`. This hash is persisted in the **Manifest**-backed registry and serves as the mandatory validation token during client-server structural negotiation.
*   **System Registration:** `CREATE SYSTEM` (Materialized View) definitions are parsed into logical plans. The compiler identifies source Component IDs and prepares the reactive dependencies for the **Circuit Scheduler**.

### 11.2. Incremental DBSP Lifting and Join Lowering
The compiler transforms logical relational algebra into incremental DBSP circuits designed for Z-Set delta processing.
*   **Lifting Identity:** Logical operators $Q$ are lifted into incremental circuits $Q^\circ$ via the transformation $Q_\Delta = D \circ \uparrow Q \circ I$. This ensures the circuit computes only the minimal Z-Set change required to maintain the view.
*   **Sequential Join Planning:** Multi-component joins are lowered into a sequence of binary `JOIN_V` (Vertical Zip-Join) operations. The compiler organizes these into left-deep or bushy trees where intermediate results are materialized in transient **Delta Registers** ($R_\Delta$).
*   **Stateful Operator Allocation:** Operators requiring history (Joins, Aggregates) are lowered into instructions that utilize persistent **Trace Registers** ($R_T$) backed by Penta-Partition shards in the FLSM.

### 11.3. Register Allocation and JIT Specialization
The incremental plan is serialized into the engine's register-based bytecode ISA.
*   **Register Management:** The compiler performs register allocation, distinguishing between **Delta Registers** ($R_\Delta$) for LSN-local intermediate Z-Sets and **Trace Registers** ($R_T$) for operator state persistence.
*   **Immediate Constant Injection:** Physical constants, including component strides, field offsets, and checksum offsets, are injected directly into the bytecode. The RPython JIT promotes these to constants, enabling the emission of specialized assembly for specific memory layouts.
*   **Materialization Barriers:** The compiler inserts weight-gated barriers before any scalar logic (`MAP`, `FILTER`). These instructions inspect **Region W** and elide data materialization if $w_{net} = 0$, enforcing the Ghost Property at the VM level.

### 11.4. Trigger Graph and Unified WAL Integration
The compiler constructs a global **Reactive Trigger Graph** to coordinate execution within a unified LSN epoch.
*   **Subscription Mapping:** Each `LOAD_DELTA` instruction creates a directed dependency from a source Component ID to the System ID.
*   **LSN Dispatch:** Upon a WAL commit, the scheduler performs a topological sort of the graph and executes dependent systems. 
*   **Unified EMIT Opcode:** Circuits terminate with an `EMIT` instruction. Instead of writing to isolated view logs, `EMIT` appends the resulting Z-Set deltas back into the unified **Z-Set WAL**.
*   **Feedback Loops:** This unified logging allows the output of one system to be consumed as the input of another—either within the same instance or across distributed GnitzDB nodes—while maintaining a single, algebraically consistent log sequence.

## 12. Implementation Plan

### Phase 0: Core Foundation and Storage Engine [COMPLETED]
*   **Zero-Allocation Memory Management:** Implemented `Arena` bump-pointer allocators and `MappedBuffer` view abstractions for GC-independent memory access.
*   **Dynamic Type System:** Developed the `FieldType` and `ComponentLayout` registry to calculate physical offsets and strides for user-defined components at runtime.
*   **German String Optimization:** Implemented the 16-byte inline string format with prefix-based equality checks and heap fallback logic.
*   **Penta-Partition Shard Storage:** Created `ECSShardWriter` and `ECSShardView` to persist and read decomposed columnar data across Regions E, W, C, and B.
*   **Algebraic MemTable Ingestion:** Implemented a row-oriented SkipList in memory to perform immediate algebraic weight summation and LSN-based value overwrites.
*   **Transmutation Pipeline:** Developed the "Unzipping" logic to transform row-oriented MemTable nodes into column-oriented physical shards, including string relocation and pointer swizzling.
*   **Binary Manifest System:** Implemented the `ManifestManager` and `ManifestReader/Writer` to track active shards via an atomic, versioned binary format with crash-safe swap mechanics.
*   **Shard Registry:** Developed the `ShardRegistry` to monitor Read Amplification metrics and manage component-level metadata for range queries.
*   **Compaction Engine:** Built the vertical merge infrastructure utilizing a `TournamentTree` for N-way sorting and a `MergeAccumulator` to realize the Ghost Property (algebraic pruning) and LSN resolution.
*   **Lifecycle and Reference Counting:** Implemented the `RefCounter` and integrated it with the `Spine` handle management to support deferred physical deletion of obsolete shards.

### Phase 1: Ingestion, Durability, and Integrity [PENDING]
*   **Unified Z-Set Write-Ahead Log:** Implement the append-only WAL storing LSN-prefixed Z-Set batches ($\Delta E, \Delta C, \Delta W$). Use Cap'n Proto for efficient serialization of component deltas within the log blocks, serving as the source of truth for both recovery and distribution.
*   **Trailing Region Checksums:** Integrate **XXH3-64** checksums into the `ECSShardWriter` and `ECSShardView`. Implement trailing checksums for each of the five regions (E, W, C, B, and Header) to support partial integrity validation and ensure the Ghost Property at the I/O layer.
*   **MemTable-WAL Interlock:** Update the `MemTableManager` to enforce a strict write-before-apply protocol. Ensure every ingestion batch is committed to the Z-Set WAL before it is algebraically coalesced into the in-memory SkipList.
*   **Survivor-Aware Transmutation:** Refine the "Unzipping" pipeline to perform **Blob Compaction**. Ensure that German String relocation into Region B only occurs for entities with a non-zero net weight, physically reclaiming heap space during the transition from mutable memory to immutable shards.
*   **Recovery and Replay Logic:** Develop the crash-recovery routine to scan the Z-Set WAL and replay batches into the MemTable. The routine must align with the last finalized LSN in the Manifest to ensure absolute algebraic consistency after an unclean shutdown.
*   **Integrity-Gated Ingestion:** Implement validation logic that utilizes trailing checksums to verify WAL block integrity during replay, preventing the propagation of partial or corrupt ingestion batches into the storage layer.

### Phase 2: The DBSP Runtime [PENDING]
*   **Register-Based Z-Set VM:** Implement the execution core supporting transient Delta Registers ($R_\Delta$) and persistent Trace Registers ($R_T$). Registers must natively manage Penta-Partition buffers (E, W, C, B) to maintain algebraic consistency across all intermediate computations.
*   **Linear Z-Set Operator Suite:** Develop the `FILTER`, `MAP`, and `UNION` opcodes. Incorporate JIT-specialized **Weight-Gated Barriers** that inspect Region W to elide Region C and Region B materialization for annihilated records ($w_{net} = 0$), enforcing the Ghost Property at the instruction level.
*   **Vertical Join Infrastructure:** Implement the `JOIN_V` (Vertical Zip-Join) operator for $O(N+M)$ entity reconstruction. Support sequential join lowering by materializing intermediate Z-Set results in $R_\Delta$ for multi-component pipelines.
*   **Persistent Trace Management:** Develop the `TRACE_ADD` and `JOIN_T` opcodes. Integrate Trace maintenance with the FLSM compaction pipeline to merge incremental Z-Sets into persistent Penta-Partition history shards, enabling incremental non-linear operations.
*   **Algebraic Temporal Operators:** Implement the `INTEGRATE` ($I$) and `DIFFERENTIATE` ($D$) operators. These manage the calculus of Z-Set streams, facilitating exact incremental state maintenance and the transition between deltas and snapshots.
*   **Reactive Trigger Scheduler:** Build the event-driven `CircuitScheduler` based on a topological Directed Acyclic Graph (DAG). Execution must be triggered by LSN commits in the Z-Set WAL, ensuring that all dependent systems resolve their algebraic state within a single LSN epoch.
*   **Feedback Loop Implementation:** Enable internal Z-Set feedback by allowing `EMIT` operations to inject deltas back into the MemTable ingestion layer. Ensure the scheduler handles recursive triggers while maintaining global LSN order and algebraic convergence.

### Phase 3: Distribution and Synchronization [PENDING]
*   **Structural Handshake and Hash Validation:** Develop the synchronization handshake protocol to enforce binary compatibility. Exchange cryptographic Schema Hashes of the `ComponentLayout` to ensure the client's local Penta-Partition mirrors are structurally aligned with the server's region offsets and German String logic.
*   **Algebraic Snapshot Resolution:** Implement the FLSM merge-scan logic to bootstrap new clients. Resolve the state by performing an N-way algebraic summation of persistent shards at a specific LSN cursor. Serialize only records with a net weight of $+1$ into a baseline Z-Set stream, ensuring clients receive a consistent starting state.
*   **Unified WAL Z-Set Tailing:** Build the "Live Mode" propagation engine by tailing the unified Z-Set WAL. Implement high-performance filtering to extract only the component-specific Z-Set deltas ($\Delta E, \Delta C, \Delta W$) required for active client subscriptions, bypassing the need for dedicated View Log files.
*   **Idempotent Z-Set Propagation:** Utilize the additive properties of DBSP to ensure fault-tolerant synchronization. Design the protocol such that clients apply Z-Set deltas additively to their local state. This ensures that network retransmissions or LSN overlaps are naturally resolved through algebraic summation, providing idempotent state consistency.
*   **Integrity-Aware Sync:** Leverage trailing region checksums during distribution. Allow the Sync Server to validate the integrity of WAL blocks and persistent regions before transmission, ensuring that only verified Z-Set state is propagated to remote mirrors.
*   **Inter-Instance Feedback Plumbing:** Implement the network-layer support for inter-instance feedback loops. Develop the mechanism to allow the output of a remote GnitzDB instance’s WAL tail to be consumed directly as an ingestion batch for a local `MemTable`, facilitating distributed, multi-stage DBSP computation.
*   **LSN Cursor Management:** Implement server-side cursor tracking for all client subscriptions. Manage WAL retention and backpressure logic to ensure clients can resume synchronization from their last acknowledged LSN, with automated triggers for Snapshot Bootstrap if a client falls outside the log retention window.

### Phase 4: SQL Frontend and Tooling [PENDING]
*   **SQL DDL and Component Compilation:** Integrate **Apache Calcite** to parse and validate `CREATE COMPONENT` and `CREATE SYSTEM` statements. Implement the compiler pass that translates logical schema definitions into physical **ComponentLayout** descriptors, calculating the strides and field offsets required for the Penta-Partition shard format.
*   **Incremental Plan Lifting:** Develop the algebraic translator to transform logical relational algebra into incremental DBSP circuits. Implement the $Q_\Delta = D \circ \uparrow Q \circ I$ transformation logic, ensuring the generated circuits compute minimal Z-Set deltas over component streams.
*   **Sequential Join Lowering:** Implement the join optimizer to decompose multi-way relational joins into optimized trees of binary `JOIN_V` (Vertical Zip-Join) operations. Orchestrate the materialization of intermediate Z-Set results in transient registers to minimize memory overhead during complex entity reconstruction.
*   **JIT-Specialized Bytecode Generation:** Build the backend that serializes incremental plans into the register-based VM ISA. Perform register allocation for $R_\Delta$ and $R_T$ buffers and inject immediate physical constants—strides, field offsets, and cryptographic Schema Hashes—directly into the bytecode to enable RPython JIT specialization.
*   **Reactive Dependency Graph Construction:** Implement the generator for the global **Reactive Trigger Graph**. Map Component ID subscriptions from the bytecode `LOAD_DELTA` instructions to reactive system nodes, enabling the **Circuit Scheduler** to perform topological dispatch upon LSN commits in the Z-Set WAL.
*   **System Integrity and Barrier Injection:** Orchestrate the insertion of **Weight-Gated Barriers** into the generated bytecode. Ensure the compiler automatically protects all scalar logic with Region W inspections, enforcing the Ghost Property by eliding payload materialization for annihilated records in every generated system.
*   **CLI and Observability Tooling:** Develop a unified command-line interface for database administration. Implement tools to inspect binary **Manifest** versions, monitor **ShardRegistry** read-amplification metrics, and perform point-in-time Z-Set debugging by tailing the unified WAL for specific LSN ranges.
*   **Inter-Instance Integration Hooks:** Build the DDL and plumbing to support inter-instance Z-Set feedback. Allow SQL definitions to specify remote GnitzDB WALs as source streams, facilitating the construction of distributed, multi-node DBSP pipelines with global algebraic convergence.
    
## 13. Future Work and Edge Cases

### 13.1. Dynamic View Projection
The engine will implement **Dynamic View Projection** to allow clients to subscribe to partial Z-Set deltas based on server-side predicates.
*   **Linear Filtering:** Since filters are linear operators in DBSP, the Sync Server can apply predicates directly to the Z-Set WAL during tailing.
*   **Algebraic Consistency:** The server-side projection produces a sub-stream of deltas that maintains algebraic integrity, ensuring the client receives the exact set of $+1/-1$ updates required to maintain a partial local replica.

### 13.2. Distributed Z-Set Partitioning
Horizontal scaling will be achieved via **Key-Range Partitioning** of the unified Z-Set WAL.
*   **Sharded WALs:** The global LSN sequence remains unified, but the WAL is partitioned based on Entity ID ranges.
*   **Cross-Node Exchange:** The VM ISA will be extended with an `EXCHANGE` instruction to facilitate cross-node data shuffles during binary join operations, allowing for distributed entity reconstruction while maintaining global Z-Set invariants.

### 13.3. Portable Z-Set Application (WASM)
To facilitate integration with browser-based and mobile environments, a **Portable Z-Set Application** library will be developed in WebAssembly (WASM).
*   **Idempotent Application:** The library will provide a hardened implementation of the additive delta-to-SQL logic, allowing any client with a WASM runtime to apply WAL Z-Sets to local storage (e.g., SQLite, DuckDB) with minimal integration overhead.
*   **Zero-Copy Handover:** The library will utilize shared memory buffers to apply Cap'n Proto Z-Set segments directly from the network stack to the local mirror.

### 13.4. Fixed-Point Iteration and Feedback Bounding
Future iterations will formalize the **Inter-Instance Feedback** mechanism to support recursive DBSP circuits.
*   **Stratification:** The compiler will implement stratification logic to detect and manage recursive dependencies in the Reactive Trigger Graph.
*   **Convergence Monitoring:** For recursive circuits, the engine will implement fixed-point iteration logic, executing the circuit until the output delta $R_\Delta$ reaches zero (algebraic convergence) within a single LSN epoch.

### 13.5. Metadata as Z-Sets
System metadata, including the Shard Registry and active subscription lists, will be transitioned to internal **Metadata Z-Sets**.
*   **Unified Infrastructure:** This allows the system to manage its own state using the same FLSM, WAL, and compaction infrastructure as user data.
*   **Streaming Metadata:** Changes to the database topology (e.g., new shards, dropped systems) will be propagated via the WAL as LSN-sequenced metadata deltas.

### 13.6. Algebraic Differential Privacy
GnitzDB is uniquely positioned to implement **Differential Privacy** at the circuit level by manipulating multiset weights.
*   **Noise Injection:** By injecting controlled noise into the weight summation of DBSP aggregation operators, the engine can produce WAL deltas that satisfy formal privacy guarantees.
*   **Reactive Privacy:** Privacy-preserving views remain reactive, pushing updated, noisy deltas as the underlying source components change.

### 13.7. Tiered Compaction Heuristics
The FLSM will be extended with **Tiered Compaction Heuristics** optimized for diverse workload profiles.
*   **Write-Heavy Tiers:** Minimizes ingestion stalls by allowing higher overlap depth in upper FLSM levels.
*   **Penta-Partition Guard Shards:** Prioritizes the consolidation of shards into large, non-overlapping Penta-Partition blocks to minimize the number of binary search steps and `mmap` handles required for snapshot resolution.
