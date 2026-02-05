## 1. Introduction and Theoretical Foundation

### 1.1. Theoretical Framework: DBSP
GnitzDB is built on **Database Stream Processing (DBSP)**, a mathematical framework that treats both batch and streaming computations as operations over an **Abelian Group** $(G, +, 0)$. 
*   **Data Representation:** Datasets are modeled as **Z-Sets** (Generalized Multisets) mapping records to integer weights $w \in \mathbb{Z}$. In this system, records are represented as **Entity-Component** pairs.
*   **Incremental View Maintenance (IVM):** The engine executes lifted circuits $Q^\circ$. By utilizing the linearity of DBSP operators, the engine computes output deltas ($\Delta O$) directly from input component deltas ($\Delta I$):
    $$ Q(\sum \Delta I) = \sum Q(\Delta I) $$
    Non-linear operations such as Joins on Entity IDs utilize the chain rule of DBSP calculus to maintain exact incremental state.

### 1.2. The Reactive ECS Data Model
The system replaces traditional relational tables with an **Entity Component System (ECS)** architecture, optimizing for high-velocity state changes and reactive simulations.
*   **Entities:** Represented as unique 64-bit unsigned integers (`u64`). Entities serve as the primary join key across all component streams.
*   **Components:** User-defined data structures (bundles of primitives) associated with entities. Each component type is treated as a distinct Z-Set stream.
*   **Systems:** Materialized views implemented as persistent DBSP circuits. Systems consume deltas from one or more component streams and produce new component deltas or side effects.

### 1.3. Objective: The Reactive Sync Engine
The engine is designed for the incremental maintenance and distribution of complex, high-bandwidth state.
*   **Vertical Decomposition:** Unlike row-oriented databases, GnitzDB decomposes entity state into independent component columns. This allows DBSP circuits to process only the specific attributes (components) that have changed.
*   **Delta Distribution:** Clients subscribe to component views. The server pushes incremental Z-Set updates, enabling remote mirrors (e.g., in SQLite or game engines) to remain synchronized with the server's state via additive delta application.
*   **Snapshot Synchronization:** The engine resolves point-in-time snapshots by aggregating persistent columnar shards, providing a consistent baseline for new subscribers before transitioning to live delta streaming.

### 1.4. Performance Mandate: JIT and German Strings
Implementation in **RPython** enables the use of a **Meta-Tracing Virtual Machine** to specialize execution at runtime.
*   **Dynamic Layout Promotion:** The engine utilizes a **ComponentLayout** registry that calculates physical byte offsets for user-defined structs. The RPython JIT promotes these layouts to constants, specializing the assembly for specific component strides.
*   **The Ghost Property:** The system decouples algebraic weights from component payloads. Records with a net weight of zero (annihilated) are bypassed during DRAM fetches, preventing irrelevant data from entering the CPU cache.
*   **German String Optimization:** String data is stored as 16-byte inline structures containing the length, a 4-byte prefix, and either the remaining data (Short) or a 64-bit offset to a blob heap (Long). This allows for $O(1)$ equality failures and elides heap access for the majority of comparisons.

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
Memory for ingestion and intermediate state is managed via **Monotonic Arenas** to bypass garbage collection overhead.
*   **Bump-Pointer Mechanics:** Allocation is $O(1)$ via a pointer increment within pre-allocated contiguous blocks.
*   **Alignment Enforcement:** The allocator enforces 8-byte alignment to support direct casting of raw pointers to primitive types (`int64`, `double`), preventing unaligned access faults during vectorized summation.

### 3.2. Physical Shard Layout
Each ECS Shard is a memory-mapped file organized into three contiguous regions, aligned to 64-byte boundaries for AVX-512 compatibility.
*   **Region E (Entities):** A sorted, contiguous vector of 64-bit unsigned Entity IDs.
*   **Region C (Components):** A contiguous vector of fixed-stride component data. The stride is determined by the **ComponentLayout** from the registry. This region contains inline primitives and 16-byte German String structs.
*   **Region B (Blob Heap):** A variable-length heap storing data that exceeds the inline capacity of Region C (e.g., long strings).

### 3.3. JIT-Compiled Stride Access
The engine leverages the RPython Meta-Tracing JIT to optimize variable-stride access patterns.
*   **Constant Promotion:** The `ComponentLayout` for a shard is treated as an immutable constant during trace compilation.
*   **Stride Specialization:** The JIT compiles the generic offset calculation `base + (index * stride)` into specific machine instructions with immediate constants (e.g., `LEA rax, [rbx + rdi*16]`), eliminating dynamic arithmetic overhead for column scans.

### 3.4. German String Optimization
String fields utilize a hybrid storage model to maximize cache efficiency and maintain the **Ghost Property**.
*   **16-Byte Struct:** All strings in Region C occupy exactly 16 bytes:
    *   `Bytes 0-3`: Length (`u32`).
    *   `Bytes 4-7`: Prefix (`char[4]`).
    *   `Bytes 8-15`: Payload (Union).
*   **Short Strings (<= 12 bytes):** The payload contains the suffix bytes. The string is entirely inline, requiring no heap access.
*   **Long Strings (> 12 bytes):** The payload contains a 64-bit offset into Region B.
*   **Prefix-First Equality:** Comparisons check length and the 4-byte prefix before chasing the heap pointer. This ensures that the majority of string comparisons fail fast using only inline data, preventing pipeline stalls.

### 3.5. Transmutation and Relocation
The transition from MemTable to Shard involves pointer relocation for heap-allocated data.
*   **Unzipping:** The row-oriented MemTable payload is iterated and split into separate writers for each component type.
*   **Heap Compaction:** String data stored in the temporary MemTable Blob Arena is copied to the persistent Region B of the new shard.
*   **Pointer Swizzling:** The offset in the German String struct is updated from the MemTable arena offset to the new relative offset within Region B, ensuring the persisted shard is self-contained.

## 4. Storage Architecture: Fragmented LSM (FLSM)

### 4.1. The Manifest and Component Spine
The storage state is anchored by the **Manifest**, a durable log recording the active set of **ECS Shards** and their metadata.
*   **Component-Centric Organization:** Unlike table-based LSM trees, the FLSM is partitioned by Component ID. Each component type has its own independent Spine, tracking the shards relevant to that specific attribute.
*   **Spine Metadata:** The Spine maintains in-memory vectors for `min_entity_id`, `max_entity_id`, and `shard_handles` for each component. This enables $O(\log N)$ binary search to identify candidate shards for a specific Entity ID without disk access.
*   **Versioned Snapshots:** Updates to the Manifest produce immutable Spine versions, providing snapshot isolation for long-running synchronization tasks.

### 4.2. Vertical Decomposition Strategy
Data is physically stored in a **Decomposed Columnar** format, replacing the traditional row-based or hybrid formats.
*   **Typed Shards:** Each physical file contains data for exactly one component type. This isolation ensures that a query accessing `Position` does not pollute the CPU cache with `Velocity` or `Inventory` data.
*   **Entity Alignment:** All shards are strictly sorted by Entity ID. This allows the runtime engine to perform "Vertical Joins" (reconstructing entities from components) using highly efficient, vectorized Zip-Join algorithms rather than expensive Hash Joins.

### 4.3. Multi-Layer Resolution
The engine resolves the current state of a component by querying the memory and storage layers in a strict hierarchy.
*   **Layer 1: MemTable:** The active in-memory buffer is checked first. It utilizes a SkipList index to provide $O(\log N)$ lookups for recent updates.
*   **Layer 2: The Spine:** If the entity is not found in memory, the engine queries the persistent Spine. Since persistent shards are immutable and sorted, lookups are performed via binary search on the `Region E` (Entity ID) vector.
*   **Algebraic Summation:** For complete Z-Set semantics, the engine sums the weights across all layers. An entity exists if and only if its net weight is non-zero.

### 4.4. Compaction: The Vertical Merge
Compaction manages the lifecycle of shards to reduce read amplification and reclaim space.
*   **Merge Sort:** The compactor performs a multi-way merge sort on several overlapping shards of the same component type.
*   **Algebraic Pruning:** Entities with a net weight of zero (annihilated) are dropped during the merge, preventing "Ghost" records from persisting into the new Guard Shard.
*   **Re-Partitioning:** The resulting data is written into new, larger ECS Shards with optimized 64-byte alignment, updating the Manifest to point to the new generation of files.

### 4.5. LSN-Aware Transmutation
The transition from mutable memory to immutable disk involves a layout transformation.
*   **Row-to-Column Transmutation:** The **Staging MemTable** stores components as "Bundles" (rows). During flush, the Transmutation Pipeline "unzips" these bundles, separating each component into its own writer stream.
*   **Heap Relocation:** Variable-length data (e.g., Long German Strings) stored in the MemTable's temporary arena is compacted and copied into the persistent `Region B` (Blob Heap) of the destination shard, with pointer offsets updated to reflect the new location.

## 5. Physical Encoding and Serialization

### 5.1. ECS Shard Header Specification
Each ECS Shard begins with a fixed 64-byte header containing metadata and region offsets. Integers are stored in **Little Endian** format.

| Byte Offset | Field Name | Type | Description |
| :--- | :--- | :--- | :--- |
| 00 - 07 | Magic Number | `u64` | Constant: `0x31305F5A54494E47` |
| 08 - 15 | Entry Count | `u64` | Total number of entities in this shard |
| 16 - 31 | Reserved | `u8[16]` | Reserved for LSN/Version metadata |
| 32 - 39 | Offset E | `u64` | Byte offset to Region E (Entity IDs) |
| 40 - 47 | Offset C | `u64` | Byte offset to Region C (Component Data) |
| 48 - 55 | Offset B | `u64` | Byte offset to Region B (Blob Heap) |

### 5.2. Region Alignment and Padding
To maximize hardware efficiency, the physical layout enforces strict alignment rules.
*   **64-Byte Region Boundaries:** The start of every region (E, C, B) is aligned to a 64-byte boundary via null-byte padding. This prevents false sharing and ensures AVX-512 operations start on cache line boundaries.
*   **Field Alignment:** Within `Region C`, component fields are naturally aligned (e.g., `f64` on 8-byte boundaries) by the **ComponentLayout** logic, allowing for direct memory casting without undefined behavior.

### 5.3. Region E: Entity ID Vector
The Entity Region contains a dense, sorted sequence of 64-bit unsigned integers.
*   **Dense Packing:** Entity IDs are stored contiguously without separators or length prefixes.
*   **Vectorization:** This layout enables the JIT to emit SIMD instructions for operations like "Find all Entities > X" or intersection logic.

### 5.4. Region C: Component Data Vector
The Component Region stores fixed-stride data records.
*   **Stride Calculation:** The stride is determined at runtime by the Schema Registry but promoted to a constant during execution.
*   **Inline Primitives:** Numeric types (`u64`, `f64`, `i32`, etc.) are stored inline.
*   **German Strings:** String fields occupy a fixed 16-byte slot consisting of:
    *   `u32` Length
    *   `u8[4]` Prefix
    *   `u64` Payload (Inline Suffix or Heap Offset)
    This structure ensures that the stride remains constant regardless of string content length.

### 5.5. Region B: The Blob Heap
The Blob Region acts as an unstructured overflow heap for variable-length data.
*   **Offset-Based Access:** Data in Region B is addressed solely via relative offsets stored in `Region C` German String structs.
*   **Compaction:** During the Transmutation and Compaction phases, the heap is rewritten sequentially, effectively garbage collecting any unused blobs referenced by annihilated records.
    
## 6. The MemTable: High-Velocity Ingestion Buffer

### 6.1. The Ingestion Pipeline
The MemTable is the mutable, in-memory write-head of the engine, optimized for rapid, row-oriented (Array of Structs) updates. Ingestion follows a strict pipeline:
1.  **Schema Enforcement**: Incoming bundles are validated against the **Component Registry** to ensure type and layout compatibility.
2.  **LSN Assignment**: Each batch is assigned a unique, monotonic **Log Sequence Number (LSN)**.
3.  **WAL Serialization**: The batch is serialized to the durable Write-Ahead Log before being applied to the in-memory MemTable.

### 6.2. AoS SkipList for Algebraic Coalescing
The MemTable utilizes a SkipList indexed by Entity ID to perform in-place algebraic summation of weights at ingestion time.
*   **Entity ID Lookup**: The SkipList performs an $O(\log N)$ search to locate existing entities.
*   **Weight Summation**: If an entity exists, the incoming weight is added to the node's 64-bit weight field. This immediate coalescing reduces the data volume passed to the persistence layer.
*   **Value Overwrite**: The component bundle associated with the entity is overwritten with the latest version, ensuring last-write-wins semantics for updates within the same ingestion epoch.

### 6.3. Physical Node Layout
Nodes are laid out as contiguous byte sequences within a Monotonic Arena to minimize allocation overhead.
*   `[00-07]` **Weight**: 64-bit signed integer.
*   `[08-08]` **Height**: 8-bit unsigned integer (SkipList tower height).
*   `[09-11]` **Padding**: Ensures 4-byte alignment for the pointer array.
*   `[12-XX]` **Next-Pointer Array**: Height-indexed array of 32-bit Arena offsets.
*   `[XX-YY]` **Entity ID**: 64-bit unsigned integer key.
*   `[YY-ZZ]` **Component Bundle**: Fixed-stride raw byte payload matching the `ComponentLayout`.

### 6.4. Sealing and Transmutation Trigger
The transition from mutable memory to immutable persistence is triggered by Arena occupancy or time-based policies.
*   **Atomic Rotation**: When the Arena is full, the active `MemTable` is marked as "Sealed" (read-only), and a new, empty `MemTable` instance is created for subsequent writes.
*   **Transmutation Pipeline**: The Sealed MemTable is handed off to a background process for transmutation into columnar ECS Shards.

### 6.5. The Transmutation Process (Unzipping)
The pipeline transforms the row-oriented MemTable into the column-oriented shard format.
*   **Row Iteration**: The pipeline performs a linear scan of the Sealed MemTable's SkipList.
*   **Annihilation Pruning**: Nodes with a net weight of zero are discarded and are not processed further, preventing annihilated records from reaching disk.
*   **Columnar Demultiplexing**: For each surviving entity, the component bundle is "unzipped." Each field is written to its corresponding `ECSShardWriter`, effectively demultiplexing one row-oriented stream into multiple column-oriented streams.
*   **String Relocation**: For "Long" German Strings, the payload is read from the MemTable's Blob Arena and copied into the destination shard's Blob Heap. The offset within the 16-byte string struct is updated (swizzled) to point to the new location.

## 7. Compaction: The JIT-Compiled Merge Loop

### 7.1. Component-Scoped Compaction
Compaction is a per-component process, operating independently on the shards of a single component type to maintain read performance.
*   **Trigger Heuristic**: Compaction is triggered for a component when its **Read Amplification** metric (the number of overlapping shards for a given Entity ID range) exceeds a configured threshold.
*   **Goal**: To merge multiple small, overlapping "Dirty" shards into a single, large, non-overlapping "Guard" shard.

### 7.2. The Tournament Tree Merge
The core compaction logic uses a **Tournament Tree** (priority queue) to perform an N-way merge sort of shards, keyed by Entity ID.
*   **Shard Head Management**: The tree maintains the next available entity from each input shard.
*   **Entity ID Consolidation**: In each step, the iterator processes all entries for the globally minimal Entity ID across all shards.

### 7.3. Algebraic Consolidation and Pruning
For each Entity ID, the engine performs an algebraic summation of its weights from all contributing input shards.
*   **The Ghost Property**: If the net weight $\sum w_i$ sums to zero, the entity is identified as annihilated. The engine discards the record and advances the iterators of all contributing shards without materializing the component data from disk.
*   **Space Reclamation**: This process is the primary mechanism for garbage collecting annihilated data and reclaiming physical storage.

### 7.4. Value Resolution via LSN
In cases of conflicting component values for the same entity (i.e., updates), the engine resolves the conflict by selecting the value from the shard with the highest **Log Sequence Number (LSN)**, ensuring "last write wins" semantics.

### 7.5. JIT Specialization of the Merge Loop
The RPython Meta-Tracing JIT heavily optimizes the compaction process.
*   **Trace Unrolling**: The merge logic is unrolled and specialized for the specific number of input shards, eliminating dynamic dispatch overhead.
*   **Materialization Barrier**: The JIT is prevented from hoisting component data fetches above the weight summation check. This ensures that payload data is only read from disk for entities that have a non-zero net weight, physically realizing the Ghost Property at the I/O level.

### 7.6. Columnar Re-Partitioning
Surviving entities ($W_{net} \neq 0$) are streamed into a new `ECSShardWriter`.
*   **Sequential Writes**: The consolidated entities, component data, and relocated strings are written sequentially into new E, C, and B regions, respectively.
*   **Atomic Manifest Update**: Upon successful finalization of the new Guard Shard, the engine atomically updates the Spine Manifest to replace the input shards with the new, compacted shard.
    
## 8. Execution Model: Persistent DBSP Virtual Machine

### 8.1. Systems as Persistent Circuits
The execution engine treats **Systems** as persistent, long-running **DBSP Circuits**. Unlike traditional query engines, these circuits are resident in memory and maintain the algebraic state necessary for incremental computation over component streams.
*   **System Registration:** A "System" is a materialized view definition compiled into bytecode. The VM instantiates the circuit, allocating registers for component Z-Set streams.
*   **LSN-Driven Execution:** The VM operates on a reactive trigger model. As new LSN batches are committed, the **Circuit Scheduler** identifies dependent systems and feeds the corresponding component deltas into their input registers.

### 8.2. The Vertical Join Operator
The fundamental operation for reconstructing entity state is the **Vertical Join** on Entity ID. This DBSP operator combines two or more component streams into a unified Z-Set.
*   **Zip-Join Algorithm:** Because all component shards are sorted by Entity ID, the join is implemented as a highly efficient, cache-friendly merge-sort (or "zip") operation that linearly advances pointers through the input `Region E` vectors.
*   **JIT Specialization:** The RPython Meta-Tracing JIT unrolls the zip-join loop and specializes the trace for the specific component layouts involved. It generates vectorized machine code that operates directly on the primitive arrays in `Region C`, eliding deserialization overhead.

### 8.3. Operator Trace Management
Stateful DBSP operators (e.g., joins, aggregations) require internal state—referred to as **Traces**—to incrementally process new deltas.
*   **Trace Storage:** Traces are maintained as in-memory Z-Sets within Monotonic Arenas, using the same row-oriented (AoS) format as the MemTable for rapid updates.
*   **State Checkpointing:** To ensure fault tolerance, the VM periodically snapshots operator traces to the FLSM as dedicated ECS Shards. These checkpoints are tagged with the LSN of the last processed batch, allowing for rapid recovery by replaying only subsequent WAL entries.

### 8.4. Output Serialization
The terminal node of a system is an `EMIT` operator that serializes the output deltas for distribution or feedback into the storage layer.
*   **Component Deltas:** The output of a system is typically a Z-Set of a specific component type (e.g., a `Velocity` system producing `Position` deltas).
*   **View Log:** These output deltas are written sequentially to a dedicated, append-only **View Log** using the Cap'n Proto wire format. This log serves as the input stream for the Distribution Layer.

## 9. Distribution and Concurrency Model

### 9.1. Multi-Process Architecture
The system isolates I/O-intensive and CPU-intensive tasks into separate processes to bypass the Global Interpreter Lock (GIL) and maximize throughput.
*   **The Executor Process**: The primary process responsible for ingestion, owning the `MemTableManager` and the DBSP VM. It is the exclusive writer for the WAL, View Logs, and new ECS Shards.
*   **The Sync Server Process**: A dedicated process that manages client connections and streams component deltas. It operates as a concurrent reader of View Logs and FLSM shards.
*   **The Compactor Process**: An independent background worker that merges shards for each component type. It reads older shards and produces new "Guard" shards.

### 9.2. Single-Writer/Multi-Reader Concurrency
Consistency across processes is maintained via a single-writer, multi-reader (SWMR) pattern anchored by the filesystem.
*   **Immutable Shards**: Once an ECS Shard is written by the Executor or Compactor, it is never modified. This allows reader processes to safely `mmap` the file without locks.
*   **Versioned Manifest**: The **Spine Manifest** provides a consistent, point-in-time view of the active shards. Readers load the latest complete version of the manifest and operate on that snapshot, while the Executor prepares the next version. Atomic file renames are used to publish manifest updates.

### 9.3. Client Session and Subscription Management
The Sync Server manages the state of all client subscriptions to ensure reliable delivery of component deltas.
*   **Component Subscription**: A client session is defined by a subscription to one or more component types.
*   **LSN Cursor**: Each subscription is associated with an LSN cursor tracking the last successfully acknowledged change. The Sync Server uses this cursor to stream from the correct offset in the component's View Log.
*   **Backpressure Handling**: If a client's LSN cursor falls behind the configured retention window of the View Log, the server signals the client to perform a full resynchronization from the latest Spine snapshot.

### 9.4. Distributed Reference Counting and Reclamation
Physical file deletion is managed by a **Distributed Reference Counter** (implemented in shared memory or via IPC) to prevent `SIGBUS` errors.
*   **Handle Acquisition**: Before a process maps a shard, it increments the global reference counter for that file.
*   **Deferred Deletion**: When the Compactor replaces old shards with a new one, it marks the old files for deletion. The `unlink()` syscall is invoked only after the resource's reference count drops to zero, ensuring that no process is actively reading the file when it is removed.

### 9.5. Inter-Process Communication (IPC)
Low-latency IPC channels are used to coordinate state transitions between processes.
*   **Manifest Propagation**: When the Executor or Compactor updates the Spine, it broadcasts a signal. The Sync Server listens for this signal to update its local view of available shards for new client snapshot requests.
*   **LSN High-Water Mark**: IPC is used to signal the highest LSN committed to the WAL, triggering the DBSP VM to process new batches and, subsequently, the Sync Server to read new entries from the View Logs.

## 10. Client Synchronization Protocol

### 10.1. Handshake and Schema Negotiation
The synchronization session begins with a handshake to establish compatibility and ensure structural alignment for the requested components.
*   **Version Verification**: The client provides the protocol version and a list of requested Component IDs.
*   **Schema Hash Validation**: The server validates the client's local schema against the persistent **Component Registry**. The exchange of a cryptographic hash of the component **ComponentLayout** is mandatory. If hashes mismatch, the session is terminated, preventing local data corruption due to incompatible memory structures.
*   **LSN Alignment**: The client submits its `Last_ACK_LSN`. A `NULL_LSN` (0) initiates a full snapshot bootstrap.

### 10.2. Mode 1: Snapshot Bootstrap
If the client requires a full state image, the server enters **Snapshot Mode**.
*   **Point-in-Time Spine Resolution**: The server resolves the **Spine Manifest** at the highest available LSN, providing a consistent view across all component streams.
*   **Vertical Reconstruction**: The server performs an N-way merge-scan across all required component shards (E, C, and B regions). This is an algebraic aggregation, ensuring only records with a net weight of $+1$ are included.
*   **Z-Set Streaming**: The complete, reconstructed set of entities and their components are streamed to the client, serialized in a high-bandwidth Cap'n Proto format.
*   **Atomic Finalization**: The stream concludes with a `SNAPSHOT_END` signal containing the final LSN of the snapshot, which serves as the starting anchor for live delta synchronization.

### 10.3. Mode 2: Live Delta Streaming
Once the client is aligned, the server transitions to **Live Mode**, providing real-time incremental updates.
*   **View Log Tailing**: The Sync Server tails the **View Log** corresponding to the client's subscription, starting immediately after the client's last acknowledged LSN.
*   **Component Delta Push**: The server pushes atomic Cap'n Proto messages. Each message contains a batch of component deltas ($\Delta \text{EntityID}, \Delta \text{ComponentValue}, \Delta \text{Weight}$).
*   **LSN Acknowledgement**: Clients periodically transmit an `ACK_LSN`. The Sync Server uses these acknowledgements to manage log retention and backpressure.

### 10.4. Fault Tolerance and Reconnection
The protocol is designed for idempotent resumption after network failures.
*   **Idempotent Resumption**: Upon reconnection, the client re-submits its latest `ACK_LSN`. The server resumes streaming from the subsequent log offset. The additive nature of DBSP deltas ensures that the client's local state remains consistent even if it re-applies a few previously acknowledged deltas.
*   **Purge Notification**: If a client falls outside the log retention window, the server issues a `RESYNC_REQUIRED` error, forcing the client to re-initiate a full Snapshot Bootstrap.

## 11. The SQL Frontend and Compiler Interface

### 11.1. Schema Registry and DDL Parsing
The frontend acts as the interface for defining the data model, utilizing **Apache Calcite** for DDL parsing.
*   **Component Definitions**: `CREATE COMPONENT` or equivalent DDL is translated into the canonical **ComponentLayout**. The compiler maps logical SQL types (e.g., `VARCHAR(N)`, `BIGINT`) to the specific **FieldType** primitives, including the fixed 16-byte structure for German Strings.
*   **Materialized View Definitions**: `CREATE SYSTEM` (or `CREATE MATERIALIZED VIEW`) statements define persistent DBSP circuits. The compiler verifies that source components are registered and that the target component definition is valid.
*   **Component Hashing**: A unique cryptographic hash is generated for every logical and physical component definition. This hash is embedded in the schema registry and used for client-server handshake validation.

### 11.2. Relational-to-DBSP Algebraic Translation
The compiler transforms a logical Relational Algebra tree into a lifted DBSP dataflow graph designed to operate over component streams.
*   **Vertical Join Mapping**: A logical `JOIN` operation is mapped onto the highly efficient **Vertical Join** operator, specializing the join key to the 64-bit Entity ID.
*   **Incremental Lifting**: The compiler applies the DBSP transformation to ensure the circuit computes only the $\Delta$ change to the output Z-Set.
*   **Type Promotion**: During translation, the component access logic is specialized to use the statically calculated `field_offsets` from the **ComponentLayout**, allowing the RPython JIT to generate efficient, deserialization-free pointer arithmetic.

### 11.3. Bytecode Emission and Physical Constants
The optimized DBSP graph is serialized into the engine's custom Instruction Set Architecture (ISA) for the RPython VM.
*   **Register Allocation**: VM registers are assigned to intermediate component Z-Set deltas.
*   **Stride Constant Injection**: The fixed `stride` and `field_offsets` for all components accessed in the circuit are injected directly into the bytecode as immediate constants. This enforces the JIT's specialization mandate, allowing the final machine code to operate at maximum efficiency.
*   **Scalar Micro-Interpreter**: Logic within `WHERE` or `SELECT` clauses is compiled into stack-based bytecode, but access to component data is protected by materialization barriers and utilizes the specialized German String comparison logic.

### 11.4. Dependency Graph and Trigger Logic
The frontend maintains a global **Dependency Graph** linking component streams to the **Systems** that consume them.
*   **Trigger Mapping**: When an LSN batch is committed for a source component (e.g., `Velocity`), the graph identifies all dependent downstream systems. The compiler generates the specific trigger logic for the **Circuit Scheduler**.
*   **Pipeline Coordination**: The compiler ensures that systems with complex dependencies (view-on-view) are scheduled for execution in the correct topological order within a single LSN epoch, maintaining global algebraic consistency.

## 12. Implementation Plan

### Phase 0: Physical Foundation [COMPLETED]
*   **Zero-Allocation Memory Management**: Implemented `Arena` bump-pointer allocators and `MappedBuffer` view abstractions for GC-independent memory access.
*   **Dynamic Type System**: Developed the `FieldType` and `ComponentLayout` registry to calculate physical offsets and strides for user-defined components at runtime.
*   **German String Logic**: Implemented the 16-byte inline string format with efficient prefix-based equality checks and heap fallback logic.
*   **ECS Shard Storage**: Created `ECSShardWriter` and `ECSShardView` to persist and read decomposed columnar data (Regions E, C, and B).
*   **Transmutation Bridge**: Refactored `MemTable` to store row-oriented component bundles and "unzip" them into columnar shards during flush, including pointer relocation for German Strings.
*   **Integrated ECS Engine**: Unified the `Spine` and `Engine` to perform Entity-Component lookups across memory and disk.

### Phase 1: Storage Lifecycle and Compaction [PENDING]
*   **Manifest Management**: Implement the durable `Manifest` file to track active shards and their LSN ranges. Add atomic swap logic for versioned snapshots.
*   **Compactor Process**: Develop the background worker that performs the **Vertical Merge** of overlapping shards.
    *   Implement the `TournamentTree` iterator for N-way merging of sorted Entity IDs.
    *   Apply algebraic annihilation (pruning records with net weight 0).
    *   Garbage collect unused blobs from Region B during compaction.
*   **Reference Counting**: Implement the distributed reference counting mechanism to safely unlink superseded shards while readers are active.

### Phase 2: Ingestion and Durability [PENDING]
*   **Write-Ahead Log (WAL)**: Implement the durable, LSN-prefixed WAL using Cap'n Proto for ingestion recovery.
*   **Server Frontend**: Create the network listener (TCP/Unix Socket) that accepts client connections.
*   **Bundle Deserialization**: Implement the logic to parse incoming Cap'n Proto "Bundles" from clients and feed them into the `MemTableManager`.
*   **Crash Recovery**: Develop the startup routine to replay the WAL from the last checkpoint recorded in the Manifest.

### Phase 3: The DBSP Runtime [PENDING]
*   **Circuit Infrastructure**: Implement the `Circuit` and `Scheduler` classes to manage persistent dataflow graphs.
*   **Vertical Join Operator**: Develop the high-performance, JIT-friendly Zip-Join operator for combining multiple `ECSShardView` streams by Entity ID.
*   **Filter/Map Operators**: Implement linear operators that accept `ComponentLayout`-aware bytecode for predicate evaluation and projection.
*   **System Registration**: Add the API to register new Circuits (Systems) and link them to component inputs.

### Phase 4: Distribution and Sync [PENDING]
*   **View Log**: Implement the append-only log for circuit outputs (Deltas).
*   **Sync Protocol**: Develop the client-server handshake, including Schema Hash validation.
*   **Snapshot Bootstrap**: Implement the logic to merge-scan the Spine and stream the net state of a component to new clients.
*   **Live Delta Pushing**: Implement the mechanism to tail the View Log and push incremental updates to subscribed clients.

### Phase 5: SQL Compiler and Tooling [PENDING]
*   **SQL Parser Integration**: Integrate Apache Calcite to parse `CREATE COMPONENT` and `CREATE SYSTEM` statements.
*   **Algebraic Translator**: Implement the logic to convert Relational Algebra trees into the engine's internal DBSP circuit topology.
*   **Bytecode Generator**: Develop the compiler pass that emits RPython bytecode with embedded `ComponentLayout` constants (strides/offsets).
*   **CLI Tool**: Build a command-line interface for managing the registry, querying system status, and manually triggering compaction.
    
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
