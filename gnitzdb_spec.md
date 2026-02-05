## 1. Introduction and Theoretical Foundation

### 1.1. Theoretical Framework: DBSP
This engine implements **Database Stream Processing (DBSP)**, a mathematical framework unifying streaming and static database computations. Unlike traditional stream processors that treat events as ephemeral messages, DBSP defines streams formally as infinite sequences of updates applied to a dataset.

Formally, we operate over an **Abelian Group** $(G, +, 0)$.
*   **Data Representation:** A database table is modeled as a **Z-Set** (Generalized Multiset), where every record $r$ is mapped to a weight $w \in \mathbb{Z}$.
    *   **Insertion:** $(r, +1)$
    *   **Deletion:** $(r, -1)$
    *   **Update:** $\{(r_{old}, -1), (r_{new}, +1)\}$
*   **The Stream:** A stream $S$ is a sequence of Z-Sets: $S = z_0, z_1, z_2, \dots$ representing batches of changes (deltas).
*   **Incremental View Maintenance (IVM):** For a relational query $Q$, the engine executes the lifted circuit $Q^\circ$. By linearity, processing the stream of changes yields the exact change to the output view:
    $$ Q(\sum \Delta I) = \sum Q(\Delta I) $$
    *(Note: Non-linear operators like Join and Distinct are handled via the chain rule of calculus in the DBSP model.)*

### 1.2. The Physical Data Model: The Columnar Mandate
The engine's storage and execution layers are direct physical realizations of the underlying algebra. We do not implement "Insert" or "Delete" as distinct imperative operations; all state mutation is **Algebraic Summation**.

*   **Annihilation:** If the engine receives $(A, +1)$ followed by $(A, -1)$, the weights sum to 0. Records with $w=0$ are physically removed during compaction.
*   **The Ghost Property:** This architecture specifically targets the "Ghost" property: if a set of updates sums to zero weight, the payload data (Key/Value bytes) is irrelevant. 
*   **Structure of Arrays (SoA):** To realize the Ghost Property on modern superscalar hardware, the engine enforces a physical separation of weight vectors from payload heaps. By decoupling the control flow (weights) from the data flow (payloads), the engine prevents hardware prefetchers from incurring DRAM latency for annihilated records.

### 1.3. System Objective: The JIT-Compiled Kernel
Existing DBSP implementations typically utilize static compilation (Rust). This engine utilizes **RPython** to implement a **Meta-Tracing Virtual Machine** that specializes at runtime.

*   **Meta-Tracing JIT:** The RPython toolchain generates a JIT compiler that traces the execution of the interpreter loop, specializing hot paths like merge-join loops into optimized machine code.
*   **Materialization Barriers:** The JIT is configured to recognize opaque boundaries around payload access. By selectively preventing the inlining or hoisting of data-materialization logic, the VM ensures that the instruction pipeline never computes the address of a value unless the algebraic weight computation yields a non-zero result. This ensures the Ghost Property is enforced at the assembly level.

### 1.4. The Transmutation Bridge
The engine acknowledges a fundamental impedance mismatch between data ingestion and data processing. 
*   **Ingestion (AoS):** High-frequency updates require a row-oriented (Array of Structs) in-memory buffer to facilitate rapid weight coalescing and random-access key lookups.
*   **Processing (SoA):** Efficient stream processing and compaction require a columnar (Structure of Arrays) layout to maximize memory bandwidth and enable the Ghost Property.
*   **Transmutation:** The engine implements a "Transmutation" layer that unzips row-oriented memory buffers into columnar "Quad-Partition" shards during the persistence phase, ensuring optimal performance for both the write-head and the read-optimized storage spine.

## 2. System Architecture Overview

The system architecture is stratified into four distinct layers, moving from high-level relational abstractions to low-level virtual memory management.

### 2.1. The Frontend: Compiler and Planner
The entry point is an **Apache Calcite** integration that parses SQL into a logical Relational Algebra tree. A custom adapter transforms this plan into a **DBSP Dataflow Circuit**. This transformation layer is responsible for:
1.  **Algebraic Translation:** Converting Set/Bag semantics into Z-Set operations (e.g., transforming `join` into `weight_multiplication`).
2.  **Bytecode Emission:** Serializing the circuit into the engine's custom Instruction Set Architecture (ISA).
3.  **Layout Resolution:** Resolving types to the specific Quad-Partition offsets required by the storage and execution layers.

### 2.2. The Runtime: RPython Virtual Machine
The core execution unit is a register-based interpreter written in RPython, utilizing a Meta-Tracing JIT.
*   **Registers:** Hold pointers to **Batches** (immutable columnar Z-Sets).
*   **Trace Anchors:** `jit_merge_point` hints are embedded within the inner loops of operators.
*   **Materialization Barriers:** The runtime enforces a strict functional boundary between weight-summing logic and payload-materialization logic. By marking materialization methods as `@jit.dont_look_inside`, the VM prevents the JIT from hoisting payload DRAM fetches into the "Ghost" paths where the net weight is zero.

### 2.3. The Storage Layer: Quad-Partition FLSM
Persistence is managed by a **Fragmented Log-Structured Merge (FLSM)** tree. The storage format is physically optimized to enforce the Ghost Property.
*   **The Quad-Partition Shard:** Each shard is a single `mmap`-backed file divided into four 64-byte aligned regions:
    1.  **Region W (Weights):** A contiguous vector of `int64` weights.
    2.  **Region O (Offsets):** A contiguous vector of `int32` pairs pointing into the heaps.
    3.  **Region K (Key Heap):** A variable-length blob of VarInt-prefixed keys.
    4.  **Region V (Value Heap):** A variable-length blob of VarInt-prefixed values.
*   **The SoA Spine:** The global index is an immutable, versioned **Structure of Arrays (SoA)**. It maintains parallel lists of `min_keys` and `max_keys` for all active shards, enabling $O(\log N)$ binary search across shard boundaries with optimal CPU cache locality.

### 2.4. The Memory Subsystem: Transmutation and Arenas
To bypass the overhead of the Python Garbage Collector and ensure hardware sympathy, the engine implements a custom memory manager.
*   **Monotonic Arenas:** The MemTable utilizes 8-byte aligned, append-only memory blocks. Allocation is a trivial bump-pointer increment, and deallocation is a bulk operation.
*   **Row-Oriented SkipList:** For ingestion, the MemTable utilizes an Array-of-Structs (AoS) SkipList. This layout maximizes locality during the frequent random-access lookups and weight coalescing required by DBSP.
*   **Transmutation Pipeline:** During a flush or compaction, the engine performs "Transmutation"—unzipping the row-oriented SkipList nodes into the columnar Quad-Partition format. This transition matches the impedance between high-velocity ingestion (AoS) and high-efficiency processing (SoA).

## 3. Memory Subsystem and Physical Layout

The memory subsystem is designed to bridge the impedance mismatch between high-velocity ingestion and read-optimized processing while enforcing the Ghost Property through physical data isolation.

### 3.1. Monotonic Arena Allocation (Ingestion Layer)
For the mutable write-head (MemTable), the engine utilizes **Monotonic Arenas** to bypass the Python Garbage Collector and minimize allocation overhead.
*   **Bump-Pointer Allocation:** Memory is managed as contiguous virtual blocks (default 64MB). Allocation is a single pointer increment, ensuring $O(1)$ performance.
*   **8-Byte Alignment:** All allocations are forced to 8-byte boundaries. This ensures that `int64` weights are naturally aligned, preventing split-load penalties and hardware exceptions during algebraic summation.
*   **Relocatable Offsets:** SkipList nodes use 32-bit offsets relative to the Arena base rather than absolute 64-bit pointers. This reduces the memory footprint of the "next" pointer array and facilitates relocatability during background maintenance.

### 3.2. Structured Memory Views (MappedBuffer)
To ensure memory safety without sacrificing performance, raw `mmap` pointers are encapsulated within **MappedBuffer** view objects.
*   **Zero-Copy Access:** Access is mediated via `rffi.ptradd` and `rffi.cast`, allowing the engine to interpret raw bytes as structured types (e.g., `LONGLONG`, `INT`) without copying data into the GC heap.
*   **Conditional Bounds Checking:** Views implement strict bounds checking. In debug mode, every access is validated; in release mode, the RPython JIT utilizes these checks as invariants to elide redundant guards in the hot loop.
*   **Immutability Hints:** Buffer pointers and sizes are marked as `_immutable_fields_`, allowing the JIT to promote these values to constants within a trace, effectively turning variable memory offsets into fixed-offset machine instructions.

### 3.3. The Materialization Barrier (JIT Discipline)
The engine utilizes a "Materialization Barrier" to physically manifest the Ghost Property. This is achieved through a strict separation of concerns in the code path:
*   **The Hot Path:** Weight summation logic is kept transparent to the JIT. The tracer observes the contiguous streaming of the Weight Vector, enabling loop unrolling and potential SIMD vectorization.
*   **The Opaque Path:** Methods responsible for fetching payloads from the Key and Value heaps are decorated with `@jit.dont_look_inside`. 
*   **Hoisting Prevention:** This decorator acts as a compiler barrier, preventing the JIT from hoisting payload DRAM fetches above the algebraic weight check. If the net weight of a record is zero, the instruction pointer never enters the materialization function, and the corresponding DRAM lines are never requested by the hardware.

### 3.4. Quad-Partition Shard Layout (SoA)
The physical file format (The Shard) is structured as a **Structure of Arrays (SoA)** to maximize memory bandwidth during columnar scans. Each shard is 64-byte aligned to match CPU cache line boundaries and support AVX-512 alignment requirements.

| Region | Content | Format | Access Pattern |
| :--- | :--- | :--- | :--- |
| **Header** | Metadata | 64-byte Fixed | Once on Open |
| **Region W** | Weights | Contiguous `i64` | Linear Scan (Always) |
| **Region O** | Offsets | Contiguous `i32` pairs | Linear Scan (Conditional) |
| **Region K** | Key Heap | VarInt + Bytes | Random Access (Conditional) |
| **Region V** | Value Heap | VarInt + Bytes | Random Access (Conditional) |

*   **Region W (Weight Vector):** The primary target for DBSP operators. Its isolation ensures that the engine can determine the relevance of a record by reading only 8 bytes.
*   **Region O (Offset Spine):** Contains the coordinates for the heaps. By separating offsets from weights, the engine prevents the hardware prefetcher from speculatively loading heap addresses during the summation phase.
*   **Variable-Length Heaps:** Keys and values are stored using prefix-length encoding (VarInt). This minimizes disk I/O and ensures that the physical size of the shard is dominated by actual data rather than padding.

## 4. Storage Architecture: Fragmented LSM (FLSM)

The storage engine implements a **Fragmented Log-Structured Merge Tree (FLSM)** designed to minimize read amplification and write stalls. Unlike standard level-based LSMs, the FLSM manages data as a collection of immutable, columnar **Shards** indexed by a versioned metadata structure.

### 4.1. The SoA Spine (Global Index)
The global index, or **The Spine**, is an immutable, versioned snapshot that provides **Snapshot Isolation** for concurrent readers. To maximize CPU cache efficiency during key lookups, the Spine is implemented as a **Structure of Arrays (SoA)** residing in memory.

*   **Parallel Metadata Vectors:** The Spine maintains three parallel arrays: `min_keys[]`, `max_keys[]`, and `shard_handles[]`. 
*   **Cache-Optimized Search:** During a lookup, the engine performs a binary search over the contiguous `min_keys` vector. Because this vector is packed and contains only the boundary strings, the search exhibits high temporal and spatial locality, minimizing TLB misses and DRAM stalls.
*   **Atomic Swapping:** When a MemTable flush or a compaction task completes, a new Spine instance is constructed and swapped into the primary process via an atomic pointer update.

### 4.2. Shard Lifecycle and Reference Counting
Physical storage units (Shards) are strictly immutable and accessed via `mmap`. To ensure memory safety during concurrent operations, the system employs **Distributed Reference Counting**:
*   **Shard Handles:** Each entry in the Spine points to a `ShardHandle`, which encapsulates the `ShardView` and its pre-extracted key boundaries.
*   **Graceful Cleanup:** When a shard is superseded by compaction, its reference count is decremented. The underlying file is only unlinked via the `unlink()` syscall once the last active query or compaction task releases its handle, preventing `SIGBUS` errors.

### 4.3. Two-Tier Search Logic
The engine employs a hierarchical search strategy to resolve point queries across the fragmented storage:
1.  **Global Search (Tier 1):** The Spine's binary search identifies the candidate shard(s) whose range `[min_key, max_key]` covers the target key.
2.  **Local Search (Tier 2):** Within the identified shard, the engine performs a secondary binary search over the **Offset Spine** (Region O). This search materializes keys from the **Key Heap** (Region K) only as needed to resolve the search, maintaining the Ghost Property throughout the lookup process.

### 4.4. The Transmutation Pipeline
The transition from the mutable write-head to the immutable storage layer is handled by the **Transmutation Pipeline**. This process resolves the impedance mismatch between ingestion and processing:
*   **Unzipping Logic:** The pipeline iterates through the row-oriented (AoS) SkipList in sorted order.
*   **Columnar Projection:** As it traverses Level 0, it "unzips" each node, streaming weights into the **Weight Vector** (Region W) and calculating heap offsets for the **Offset Spine** (Region O).
*   **Zero-Weight Pruning:** The Transmuter performs the first stage of garbage collection by eliding any records whose net weight in the MemTable is zero, preventing "Ghost" records from entering the persistent storage layer.

### 4.5. Fragmentation Strategy: Guard vs. Dirty Shards
The FLSM optimizes for read performance by categorizing shards based on their overlap depth:
*   **Guard Shards:** Non-overlapping, sorted shards that allow for $O(\log N)$ single-shard lookups.
*   **Dirty Shards:** Newer, potentially overlapping shards that reside in the upper levels of the FLSM.
*   **Compaction Trigger:** Compaction is triggered based on a heuristic of **Read Amplification** (the number of overlapping shards for a given key range). The goal of compaction is to merge Dirty Shards into the Guard Shards, maintaining a flat, searchable Spine.

## 5. Physical Encoding and Serialization

The physical encoding of Quad-Partition shards is governed by strict alignment and variable-length serialization rules designed to minimize disk footprint while ensuring high-speed random access via `mmap`.

### 5.1. The Shard Header Specification
Each shard begins with a fixed 64-byte header. All multi-byte integers are stored in **Little Endian** format.

| Byte Offset | Field Name | Type | Description |
| :--- | :--- | :--- | :--- |
| 00 - 07 | Magic Number | `u64` | Constant: `0x31305F5A54494E47` ("GNITZ_01") |
| 08 - 15 | Entry Count | `u64` | Total number of records in the shard |
| 16 - 23 | Offset W | `u64` | Byte offset to the start of the Weight Vector |
| 24 - 31 | Offset O | `u64` | Byte offset to the start of the Offset Spine |
| 32 - 39 | Offset K | `u64` | Byte offset to the start of the Key Heap |
| 40 - 47 | Offset V | `u64` | Byte offset to the start of the Value Heap |
| 48 - 63 | Padding | `u8[16]` | Reserved for future bloom filter/checksum metadata |

### 5.2. Base-128 VarInt Encoding
To minimize the metadata overhead in the variable-length heaps (Regions K and V), the engine utilizes a **7-bit Base-128 VarInt** encoding for all length descriptors.
*   **MSB Logic:** The most significant bit of each byte is used as a continuation flag. If the MSB is 1, another byte follows; if 0, the current byte is the last.
*   **Storage Efficiency:** Small strings (under 128 bytes) incur only a 1-byte length penalty. This is critical for the Key Heap, where keys are frequently short identifiers.
*   **Decoding:** The decoder performs a bitwise OR and shift operation within the `MappedBuffer` view, allowing for $O(1)$ length resolution before string materialization.

### 5.3. Alignment and Region Padding
The engine enforces a **64-byte alignment** for the start of every region (W, O, K, V). This alignment is not merely for logical organization but serves two hardware-level purposes:
1.  **Cache Line Isolation:** By ensuring that Region W (Weights) and Region O (Offsets) start on new cache lines, the engine prevents false sharing and ensures that the hardware prefetcher does not accidentally pull Offset data into the L1 cache during a Weight-only scan.
2.  **SIMD Readiness:** The 64-byte boundary satisfies the alignment requirements for AVX-512 instructions, enabling the RPython JIT to generate vectorized instructions for weight summation without incurring unaligned-access penalties.
*   **Padding Implementation:** The `ShardWriter` calculates the necessary padding bytes after each region and inserts null bytes to reach the next `0x40` boundary.

### 5.4. Fixed-Width Stride Mechanics
Regions W and O utilize fixed-stride layouts to enable $O(1)$ coordinate calculation.
*   **Region W (Weight Vector):** Stride is exactly 8 bytes (`int64`). Accessing the weight of record $i$ is a simple `base + (i * 8)` calculation.
*   **Region O (Offset Spine):** Stride is exactly 8 bytes, consisting of two `int32` fields:
    *   `KeyOffset` (4 bytes): Relative to the start of Region K.
    *   `ValOffset` (4 bytes): Relative to the start of Region V.
*   **Pointer Stability:** By using 32-bit relative offsets within the heaps, the engine ensures that a shard file is position-independent and can be mapped to any virtual address without requiring pointer swizzling.
    
## 6. The MemTable: High-Velocity Ingestion Buffer

The MemTable serves as the engine's mutable write-head, optimized for the rapid ingestion and algebraic coalescing of Z-Set deltas. It utilizes a row-oriented (AoS) architecture to maintain strict temporal and spatial locality during the random-access updates required by DBSP.

### 6.1. SkipList Node Topology
To maintain sorted order without the overhead of GC-managed object headers, the MemTable is implemented as a **Flat SkipList** where nodes are laid out as contiguous byte sequences within a Monotonic Arena.

*   **Node Layout:** Each node follows a fixed-prefix, variable-suffix structure:
    *   `[00-07]` Weight (`i64`)
    *   `[08-08]` Height (`u8`)
    *   `[09-11]` Alignment Padding
    *   `[12-XX]` Next-Pointer Array (Height × `u32` offsets)
    *   `[XX-YY]` Key Payload (VarInt Length + Bytes)
    *   `[YY-ZZ]` Value Payload (VarInt Length + Bytes)
*   **Pointer Ambiguity Mitigation:** The allocator reserves the first 8 bytes of the Arena (Offset 0) as a "Null Sentinel." This ensures that a valid node offset is never zero, allowing the SkipList logic to use `0` as a reliable `NULL` pointer equivalent within the `u32` next-pointer array.

### 6.2. Optimized Search and Coalescing
The ingestion logic prioritizes zero-allocation paths to minimize GC pressure and JIT trace bloat.
*   **Static Flyweight Access:** The engine avoids instantiating RPython wrapper objects for nodes during traversal. Instead, it utilizes static helper functions that operate directly on the Arena's base pointer and a node's integer offset.
*   **Zero-Allocation Comparison:** Key comparisons are performed byte-by-byte between the search string and the raw Arena memory. This avoids the creation of intermediate RPython string objects during the $O(\log N)$ search path.
*   **Algebraic Update:** Upon a key hit, the engine performs an in-place atomic addition on the node's weight field. This coalescing happens at the point of ingestion, ensuring the MemTable size reflects the net change rather than the raw event volume.

### 6.3. Memory Pressure and Capacity Management
The lifecycle of the MemTable is governed by the `MemTableManager`, which monitors the physical growth of the Arena.
*   **Integer-Based Thresholds:** To avoid floating-point overhead in the hot path, capacity checks are performed using pre-calculated byte thresholds. The manager signals a "Full" state when the Arena's bump-pointer exceeds a fixed-point percentage of the total capacity.
*   **Update List Reuse:** To further reduce GC churn, the manager maintains a pre-allocated "Update List" (the array of predecessor offsets) which is reused across all `upsert` operations. This eliminates the need to allocate a new list for every insertion.

### 6.4. The Rotation and Flush Cycle
When the capacity threshold is reached, the MemTable transitions from a mutable state to a read-only state.
*   **Atomic Rotation:** The manager initializes a fresh Arena and SkipList for incoming writes while the previous table is prepared for transmutation.
*   **Sorted Traversal:** The flush logic iterates exclusively through Level 0 of the SkipList. This ensures that the resulting Quad-Partition shard is physically sorted by key, a prerequisite for the Spine's binary search and the multi-way merge logic used in compaction.
*   **Annihilation Pruning:** During the flush, any node with a net weight of zero is discarded. This proactive pruning ensures that retracted records do not consume disk I/O or metadata slots in the persistent storage layer.

## 7. Compaction: The JIT-Compiled Merge Loop

Compaction is the engine’s primary mechanism for reclaiming disk space and maintaining the search efficiency of the Spine by merging overlapping "Dirty" shards into non-overlapping "Guard" shards. The process is implemented as a multi-way merge sort that leverages the Quad-Partition layout to minimize DRAM traffic.

### 7.1. The Multi-Way Merge Iterator
The core of the compaction task is a tournament tree (priority queue) that manages $N$ input shards. The merge logic operates in three distinct phases per unique key:
1.  **Key Comparison:** The tournament tree compares the current keys at the head of each input shard's Region K.
2.  **Algebraic Summation:** For all shards sharing the minimal key, the engine performs a linear scan of their respective Weight Vectors (Region W) and computes the net weight: $W_{net} = \sum w_i$.
3.  **Conditional Materialization:** The engine only accesses the Value Heaps (Region V) of the input shards if $W_{net} \neq 0$.

### 7.2. JIT-Enforced Ghost Pruning
The performance of the compaction loop relies on the RPython JIT's ability to specialize the merge for a fixed number of input shards. 
*   **Trace Unrolling:** The JIT unrolls the tournament tree comparisons into a linear sequence of machine instructions, eliminating the overhead of generic priority queue management.
*   **Barrier Enforcement:** By utilizing the "Materialization Barrier" (@jit.dont_look_inside), the compactor ensures that value materialization logic is never speculatively executed or hoisted. If the net weight of a key sums to zero (annihilation), the JIT-generated code bypasses the value-loading block entirely. This ensures that "Ghost" records—those existing only as retractions—are pruned without ever incurring the latency of a DRAM fetch for their associated values.

### 7.3. Columnar Re-Partitioning
The output of the merge process is streamed directly into a new `ShardWriter`. As the compactor identifies surviving records ($W_{net} \neq 0$), it performs a "Columnar Re-Partitioning":
*   The minimal key and its materialized value are appended to the new heaps.
*   The net weight is appended to the new Weight Vector.
*   The new Offset Spine is updated to reflect the coordinates in the newly constructed heaps.
This ensures that the output shard maintains the strict SoA alignment and Quad-Partition invariants required for subsequent high-speed queries.

### 7.4. Compaction Heuristics and Read Amplification
Candidate selection for compaction is driven by the Spine metadata rather than simple file counts. The engine monitors **Read Amplification**, defined as the depth of overlapping key ranges across the Spine. 
*   **Targeting Overlap:** Compaction tasks are prioritized for key ranges where the `min_key` and `max_key` boundaries indicate a high density of Dirty Shards. 
*   **Guard Shard Promotion:** Shards resulting from a compaction that covers a significant, non-overlapping portion of the key space are promoted to "Guard" status in the Spine, signaling to the search logic that a single $O(\log N)$ binary search is sufficient to resolve queries in that range.
    
## 8. Execution Model: The DBSP Virtual Machine

The DBSP Virtual Machine is a register-based, meta-tracing interpreter designed to execute dataflow circuits over immutable batches. It abstracts the physical fragmentation of the FLSM, providing a unified algebraic interface for incremental view maintenance.

### 8.1. Register Architecture and Batch Semantics
VM registers do not store individual records; they hold pointers to **Batches**. A Batch is a logical Z-Set representing a collection of deltas.
*   **Composition:** A Batch is a collection of handles to one or more `ShardViews` or `MemTable` snapshots.
*   **Immutability:** Once a Batch is loaded into a register, it is treated as immutable. Transformations (e.g., `FILTER`) produce new Batches stored in destination registers.
*   **Algebraic State:** Registers facilitate the lifting of relational operators. For example, the `ADD` instruction performs the union of two Batches by logically summing their underlying Z-Sets.

### 8.2. Instruction Set Architecture (ISA)
The ISA consists of coarse-grained operators that manipulate Batches. These instructions are designed to be "trace-friendly," allowing the Meta-Tracing JIT to unroll loops across the constituent shards of a Batch.

*   **`LOAD_BATCH <RegID> <SourceID>`:** Populates a register with a handle to the current Spine snapshot or a specific storage table.
*   **`FILTER <DestReg> <SrcReg> <PredicateID>`:** Produces a new Batch containing only records that satisfy the scalar predicate.
*   **`JOIN <DestReg> <LeftReg> <RightReg>`:** Executes a bilinear join. For matching keys, the output weight is the product of the input weights ($w_{out} = w_L \times w_R$).
*   **`NEGATE <DestReg> <SrcReg>`:** Multiplies all weights in the Batch by $-1$, facilitating the retraction logic required for IVM.

### 8.3. The Scalar Micro-Interpreter
Scalar expressions (e.g., `WHERE price > 100`) are executed by a specialized **Micro-Interpreter** embedded within the VM's hot loops.
*   **Bytecode Inlining:** The Micro-Interpreter is a stack-based evaluator. Because the scalar bytecode is constant for the duration of a query, the Meta-Tracing JIT inlines the evaluation logic directly into the dataflow trace.
*   **Barrier Integration:** To maintain the Ghost Property, the Micro-Interpreter is invoked behind the materialization barrier. It only triggers payload materialization if the weight-based pruning logic determines the record is algebraically relevant.

### 8.4. Pipelined Operator Execution
Operators do not materialize full output sets in a single step. Instead, they operate as streaming transformers:
*   **Intermediate Arenas:** Each operator utilizes a temporary **Monotonic Arena** to buffer its output. As the operator iterates over the input Weight Vectors, it streams surviving deltas into the arena in a row-oriented format.
*   **On-the-Fly Transmutation:** Once an operator completes its pass, the intermediate arena is transmuted into a new columnar Batch. This ensures that the output of one instruction is immediately ready for high-speed columnar processing by the next instruction in the circuit.

### 8.5. Fragmentation Abstraction
The VM hides the complexity of the FLSM's fragmented shards from the query logic. When an instruction like `FILTER` is executed on a Batch composed of multiple shards, the VM's internal loop iterates over each shard sequentially. The JIT observes this nested loop structure and specializes the trace for the specific number of shards currently in the register, effectively collapsing the fragmented storage into a single, high-speed execution stream.

## 9. Concurrency and Process Model

The engine adopts a multi-process architecture to circumvent the Global Interpreter Lock (GIL) inherent in RPython and to isolate heavy background maintenance tasks from the high-velocity query execution path.

### 9.1. Process Separation and Role Definition
System responsibilities are partitioned between two distinct operating system processes, ensuring that I/O-bound compaction does not introduce jitter into the CPU-bound virtual machine execution.
*   **The Primary Process (Executor):** Owns the `MemTableManager` and the active `Engine` instance. It is responsible for stream ingestion, weight coalescing in the Arena, and executing the DBSP bytecode circuit.
*   **The Compactor Process:** An independent background worker dedicated to multi-way merge operations. It operates in a separate address space, preventing compaction-related memory pressure or cache pollution from affecting the Primary Process.

### 9.2. Snapshot Isolation via Immutable Spines
Concurrency control is achieved through **Snapshot Isolation**, anchored by the immutable nature of the Spine and its constituent shards.
*   **Versioned State:** When a query begins, the Primary Process provides it with a reference to the current `Spine` instance. Because the Spine and its `ShardHandles` are immutable, the query operates on a consistent point-in-time snapshot of the database without requiring global read-locks.
*   **Concurrent Read/Write:** New deltas continue to accumulate in the `MemTableManager` while queries are in progress. Since the MemTable is isolated from the persisted shards, read-write contention is eliminated at the architectural level.

### 9.3. The Atomic Spine Swap
The transition between database states—triggered by a MemTable flush or a compaction completion—is handled via an **Atomic Spine Swap**.
1.  **Manifest Generation:** Upon completing a maintenance task, the relevant component (Manager or Compactor) produces a **Spine Manifest**, a serialized descriptor of the new Quad-Partition shard set.
2.  **Pointer Update:** The Primary Process receives the manifest and constructs a new `Spine` object. It then performs an atomic pointer update to replace the global Spine reference.
3.  **Epoch Transition:** New queries are routed to the new Spine, while legacy queries continue to hold references to the previous snapshot, ensuring uninterrupted execution.

### 9.4. Distributed Reference Counting and Cleanup
To maintain memory safety for `mmap`-backed regions across process boundaries, the engine employs a **Distributed Reference Counting** mechanism.
*   **Handle Tracking:** Each `ShardHandle` tracks the number of active queries currently referencing its `ShardView`.
*   **Deferred Deletion:** When a `Spine` is swapped and its shards are superseded, the Primary Process does not immediately invoke `unlink()`. Instead, it marks the shards for deletion.
*   **Resource Reclamation:** The physical file is unlinked from the filesystem only when the associated `ShardHandle` reference count reaches zero. This prevents `SIGBUS` errors that would otherwise occur if a file backing were removed while a query was still accessing its `MappedBuffer` views.

### 9.5. Inter-Process Communication (IPC)
Communication between the Primary and Compactor processes is mediated through a low-latency IPC channel (typically Unix Domain Sockets). The protocol is restricted to the exchange of Spine Manifests and lifecycle signals, keeping the coordination overhead minimal and ensuring that the bulk of the data transfer remains zero-copy via the shared filesystem and `mmap` subsystem.

## 10. The SQL Frontend and Compiler Interface

The compiler interface bridges declarative SQL semantics with the engine's underlying Z-Set algebra and Quad-Partition storage. It utilizes **Apache Calcite** to perform logical optimization before emitting the specialized DBSP bytecode required by the RPython VM.

### 10.1. The DBSP Lifting Pipeline
The compilation process transforms standard Relational Algebra into a lifted DBSP dataflow circuit. 
1.  **Logical Planning:** Calcite optimizes the SQL query into a `RelNode` tree, applying standard optimizations such as predicate pushdown and join reordering.
2.  **Algebraic Translation:** The compiler maps relational operators to their bilinear or linear DBSP equivalents. For instance, a SQL `JOIN` is translated into a weight-multiplication operator ($w_{out} = w_L \times w_R$), and `UNION ALL` is mapped to a Z-Set addition.
3.  **Bytecode Generation:** The optimized circuit is serialized into the VM's ISA. This includes the generation of the stack-based bytecode for the Scalar Micro-Interpreter.

### 10.2. Physical Schema Mapping and Stride Resolution
The compiler is responsible for mapping logical SQL types to the physical constraints of the Quad-Partition format.
*   **Type Atomicity:** Logical types (e.g., `BIGINT`, `TIMESTAMP`) are resolved to fixed-width `i64` slots in the Weight Vector or Offset Spine.
*   **Heap Offloading:** Variable-length types (e.g., `VARCHAR`, `VARBINARY`) are automatically assigned to the Key or Value Heaps. The compiler generates the specific `i32` offset logic required to navigate the Offset Spine (Region O).
*   **Stride Calculation:** The compiler computes the fixed-width strides for the Control Vector based on the query's projection list, ensuring the VM can perform $O(1)$ coordinate arithmetic during scans.

### 10.3. Materialization Strategy and Barrier Emission
A critical task of the compiler is identifying the boundary between control-flow (weights) and data-flow (payloads) to support the Ghost Property.
*   **Barrier Identification:** The compiler analyzes the query to determine which columns are required for predicates and which are merely "pass-through" payloads. 
*   **Conditional Fetch Logic:** It generates bytecode that explicitly gates payload materialization. The emitted instructions ensure the VM performs weight-based pruning before the Micro-Interpreter attempts to access the heaps, maintaining the structural integrity of the JIT's materialization barrier.

### 10.4. Transmutation Planning
For incoming data streams, the compiler generates a **Transmutation Plan**. This plan defines how row-oriented input tuples (AoS) are decomposed into the Quad-Partition (SoA) format.
*   **Key/Value Bifurcation:** The plan specifies which fields constitute the unique DBSP Key and which belong to the Value payload.
*   **Initial Weight Assignment:** The compiler injects logic to assign the initial `+1` or `-1` weights to incoming deltas based on whether the stream represents an insertion, deletion, or update.
*   **Arena Layout Generation:** It defines the exact byte-layout for the MemTable SkipList nodes, ensuring that the in-memory row format is perfectly aligned for the subsequent unzipping process during a flush.

### 10.5. Static Analysis for JIT Specialization
The compiler performs static analysis to provide hints to the RPython JIT. By embedding "constancy hints" in the bytecode, the compiler allows the JIT to promote specific query parameters (like shard counts in a batch or fixed offsets in a schema) to constants. This enables the JIT to collapse generic dataflow operators into specialized machine code tailored to the specific schema of the query.

## 11. Implementation Plan

The implementation strategy follows a tiered approach, prioritizing the validation of the physical "Ghost Property" before scaling to complex relational operators.

### Phase 0: JIT and Ghost Property Validation [COMPLETED]
*   **Objective:** Empirically verify that the RPython JIT elides DRAM fetches for annihilated records using a columnar layout.
*   **Deliverables:**
    *   Micro-benchmark comparing AoS (Array of Structs) vs. SoA (Structure of Arrays).
    *   Validation of `@jit.dont_look_inside` as a materialization barrier.
    *   Hardware counter analysis (`L1-dcache-misses`) proving load elision.

### Phase 1: Storage Core and Quad-Partition Safety [COMPLETED]
*   **Objective:** Build the immutable storage foundation and memory-safe accessors.
*   **Deliverables:**
    *   `mmap_posix` wrappers for low-level VM management.
    *   `MappedBuffer` for zero-copy, bounds-checked memory access.
    *   `ShardWriter` and `ShardView` implementing the Quad-Partition format (Regions W, O, K, V).

### Phase 2: In-Memory Mutation and Transmutation [COMPLETED]
*   **Objective:** Implement the high-velocity write-head and the AoS-to-SoA bridge.
*   **Deliverables:**
    *   `Arena` bump-pointer allocator for GC-independent memory management.
    *   `MemTable` SkipList with zero-allocation search and algebraic weight coalescing.
    *   `Transmutation` logic for unzipping row-oriented nodes into columnar shards.
    *   `MemTableManager` for capacity monitoring and rotation.

### Phase 3: The Spine and Unified Search [COMPLETED]
*   **Objective:** Implement the global index and point-query resolution.
*   **Deliverables:**
    *   `ShardHandle` for boundary metadata extraction.
    *   `Spine` implementing the SoA metadata layout for cache-efficient binary search.
    *   `Engine` coordinator for summing weights across the MemTable and the Spine.

### Phase 4: The Virtual Machine and Bytecode ISA [PENDING]
*   **Objective:** Transition from point queries to full dataflow circuit execution.
*   **Tasks:**
    *   Define the Bytecode ISA for DBSP operators (`LOAD`, `FILTER`, `JOIN`, `NEGATE`).
    *   Implement the primary Interpreter Loop with `jit_merge_point` annotations.
    *   Construct the Scalar Micro-Interpreter for branch-free predicate evaluation.
    *   Implement "On-the-Fly Transmutation" for intermediate operator results.

### Phase 5: Compaction and Concurrency [PENDING]
*   **Objective:** Implement background maintenance and snapshot isolation.
*   **Tasks:**
    *   Develop the multi-process IPC protocol for Spine Manifest exchange.
    *   Implement the Tournament Tree multi-way merge logic in the Compactor process.
    *   Integrate Distributed Reference Counting for safe `mmap` reclamation.
    *   Implement the Atomic Spine Swap for epoch-based state transitions.

### Phase 6: SQL Integration and Relational Mapping [PENDING]
*   **Objective:** Provide a declarative interface for the DBSP engine.
*   **Tasks:**
    *   Implement the Apache Calcite `RelVisitor` for automated bytecode generation.
    *   Map Relational Algebra to bilinear Z-Set operators.
    *   Execute end-to-end TPC-H streaming benchmarks.
    *   Validate algebraic correctness against baseline DBSP implementations (e.g., Feldera).
    
## 12. Future Work and Edge Cases

The completion of the storage and search core (Phases 1-3) provides a stable foundation for addressing advanced systems challenges and performance optimizations.

### 12.1. Persistence and Crash Recovery
The current MemTable implementation resides exclusively in volatile memory. To ensure durability, a **Write-Ahead Log (WAL)** must be integrated into the ingestion path.
*   **Synchronous Logging:** Prior to Arena allocation, the `MemTableManager` will append `(Key, Value, Weight)` tuples to a synchronous, append-only file.
*   **Arena Replay:** On startup, the engine will scan the WAL and replay the `upsert` operations into a fresh Arena to reconstruct the in-memory state.
*   **Log Truncation:** The WAL is truncated atomically only after the Transmutation Pipeline successfully finalizes a Quad-Partition shard and updates the Spine Manifest.

### 12.2. SIMD-Accelerated Weight Summation
The 64-byte alignment of Region W (Weights) is designed to facilitate **SIMD (Single Instruction, Multiple Data)** vectorization.
*   **AVX-512 Integration:** Future iterations of the VM and Compactor will utilize RPython’s vectorization hints to process Weight Vectors. This enables the engine to perform algebraic summation on 8 records per CPU cycle.
*   **Vectorized Pruning:** By combining SIMD summation with mask-based branching, the engine can identify blocks of annihilated records (where a vector of weights sums to zero) and skip entire cache lines of Offset and Heap data in a single operation.

### 12.3. Secondary Indexing as Materialized Views
In the DBSP framework, secondary indices are not distinct storage structures but **Derived Z-Sets**.
*   **Index Shards:** A secondary index is implemented as a separate Spine where the Key Heap contains a composite of the indexed attribute and the primary key.
*   **Automated Maintenance:** The Virtual Machine’s dataflow circuit handles index maintenance automatically. Any delta applied to the primary table is transformed into a corresponding delta for the index Z-Set, ensuring that indices are always consistent with the base data through standard IVM logic.

### 12.4. Adaptive Compaction Heuristics
While the current FLSM supports basic tiered compaction, future work will focus on **Overlap-Aware Compaction**.
*   **Read Amplification Monitoring:** The Spine will track the "Overlap Depth" for specific key ranges. Compaction will be triggered dynamically for ranges where the number of overlapping shards exceeds a configurable threshold, regardless of the total data volume.
*   **Guard Shard Optimization:** The engine will prioritize the creation of large, non-overlapping "Guard Shards" to ensure that the majority of point queries can be resolved with a single $O(\log N)$ binary search.

### 12.5. Distributed Execution and Sharding
The Spine’s metadata-driven architecture facilitates horizontal scaling through key-range partitioning.
*   **Partitioned Spines:** The global key space can be divided across multiple nodes, with each node managing a local Spine and FLSM instance.
*   **Exchange Operators:** The VM ISA will be extended with an `EXCHANGE` instruction to facilitate data shuffles between nodes, enabling distributed joins and aggregations while maintaining the Z-Set algebraic invariants across the network.

### 12.6. Lazy Schema Evolution
The Quad-Partition format’s reliance on opaque heaps allows for zero-downtime schema changes.
*   **Metadata-Only Evolution:** Operations like `RENAME COLUMN` or `ADD COLUMN` (with default values) are handled entirely within the Compiler’s catalog.
*   **Background Re-Transmutation:** For structural changes (e.g., type conversions), the engine can perform a background "Re-Transmutation" task. This task reads existing shards, applies the transformation during the merge loop, and writes new shards with the updated layout, seamlessly updating the Spine once the task completes.
