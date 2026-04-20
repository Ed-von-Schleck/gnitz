



# GnitzDB Vector Search Implementation Blueprint

This document defines the exhaustive technical specification for implementing vector search in GnitzDB. The architecture leverages an Inverted File Index (IVF) as an immutable sidecar within the FLSM shards, combined with AVX-512 SIMD distance computation. Deletions and updates are handled entirely by GnitzDB's existing Z-Set ghost elimination (`get_weight_for_row`), requiring zero tombstone management within the vector index itself.

## 1. Schema and Memory Layout Upgrades

Vectors are fixed-width binary payloads. To support high-dimensional vectors, the internal batch layout limits must be expanded.

### 1.1. Capacity and Alignment Upgrades
1. **Schema Definition:** In `schema.rs`, upgrade the `size` field of `SchemaColumn` from `u8` (max 255 bytes) to `u32`.
2. **Batch Strides:** In `batch.rs`, update `strides` from `[u8; MAX_BATCH_REGIONS]` to `[u32; MAX_BATCH_REGIONS]`.
3. **SIMD Alignment:** In `batch.rs::compute_offsets`, enforce 64-byte alignment for all region boundaries to guarantee safe and optimal AVX-512 memory loads:
   ```rust
   off = (off + 63) & !63;
   offsets[i] = off as u32;
   off += capacity * strides[i] as usize;
   ```

### 1.2. Vector Type Codes
Add the following type codes to `schema::type_code`:
* `TYPE_VECTOR_F32 = 10`: Standard 32-bit floats.
* `TYPE_VECTOR_SQ8 = 11`: 8-bit scalar quantized integers.
* `TYPE_VECTOR_BQ = 12`: Binary quantized bitsets.

### 1.3. Z-Set Total Ordering
In `columnar.rs::compare_rows`, implement strict total ordering for the new types to satisfy Z-Set consolidation invariants:
* **F32:** Cast the raw byte slice to `&[f32]` using `bytemuck` (or `std::slice::from_raw_parts`) and compare lexicographically using `f32::total_cmp`. This ensures `NaN`s group deterministically and cancel out correctly.
* **SQ8 / BQ:** Compare lexicographically using standard `Ord` on `&[u8]`.

## 2. IVF Sidecar Format (Shard Level)

The vector index is an Inverted File Index (IVF) appended to the immutable shard file. It maps centroids to contiguous arrays of row indices (Posting Lists).

### 2.1. Header Expansion
In `layout.rs` and `shard_file.rs`:
* Bump `SHARD_VERSION` to `5`.
* Expand `HEADER_SIZE` from 64 to 128 bytes to provide room for future extensions and the new sidecar pointers.
* Define `OFF_IVF_OFFSET = 56` (u64) and `OFF_IVF_SIZE = 64` (u64).

### 2.2. IVF Payload Layout
The IVF block is written immediately after the XOR8 filter. If a table has multiple vector columns, the block contains a directory.
1. **Header (8 bytes):** `num_vector_cols` (u32), `reserved` (u32).
2. **Column Directory:** For each vector column: `logical_col_idx` (u32), `block_offset` (u32 relative to IVF start), `block_size` (u32).
3. **IVF Block (Per Column):**
   * `num_centroids` (u32)
   * `metric_type` (u8: 0=L2, 1=Cosine, 2=IP, 3=Hamming)
   * `reserved` (11 bytes)
   * **Centroids Array:** `num_centroids * col_size` bytes.
   * **Posting List Offsets:** `[u32; num_centroids]` (byte offsets relative to the start of the Posting Lists Array).
   * **Posting List Lengths:** `[u32; num_centroids]` (number of `u32` row indices).
   * **Posting Lists Array:** Flat array of `u32` containing row indices.

## 3. Index Construction (Write Path)

Index construction is executed synchronously during `compact_shards` and `flush_inner` in background threads.

### 3.1. Builder Integration (`shard_file.rs`)
Inside `build_shard_image` and `write_shard_streaming`, after the column regions and XOR8 filter are finalized:
1. Identify all `SchemaColumn` entries with vector type codes.
2. If `row_count < 1024`, bypass IVF generation (set `OFF_IVF_OFFSET = 0`). Small shards are faster to brute-force.
3. For `row_count >= 1024`, execute deterministic K-Means++ to locate centroids ($K = \lfloor\sqrt{N}\rfloor$).
   * *Determinism Check:* Seed the K-Means PRNG (e.g., `ChaCha8Rng`) using the shard's `min_lsn` to ensure bit-for-bit reproducible index builds.
4. Compute the distance from every `row_idx` ($0 \dots N-1$) to all centroids.
5. Group `row_idx`s by their nearest centroid and serialize the IVF payload layout specified in 2.2.

## 4. Zero-Copy Search (Read Path)

### 4.1. Mapping the Sidecar (`shard_reader.rs`)
In `MappedShard::open`, read `OFF_IVF_OFFSET`. If `> 0`, instantiate an `IvfView` wrapper that holds raw pointers into the memory map for the Centroids Array, Offsets Array, Lengths Array, and Posting Lists Array. Zero heap allocations are permitted during mapping.

### 4.2. Shard-Level Probe API
Implement `MappedShard::search_ivf(&self, col_idx: usize, query: &[u8], nprobe: usize) -> Option<Vec<u32>>`:
1. Compute distances between `query` and all centroids in the `IvfView`.
2. Identify the top `nprobe` centroids.
3. Use the Offsets and Lengths arrays to slice the Posting Lists Array.
4. Return the concatenated `u32` row indices.

## 5. SIMD Distance Kernels

Implement hardware-accelerated kernels in a new `math.rs` module using `std::arch::x86_64`. Distance functions must operate on 64-byte aligned slices.

* **TYPE_VECTOR_F32 (L2 / Cosine):** Unrolled `_mm512_fmadd_ps` loop.
* **TYPE_VECTOR_SQ8 (Int8 IP/L2):** Utilize AVX-512 VNNI (`_mm512_dpbusds_epi32`). Compute 64 dimensions per cycle.
* **TYPE_VECTOR_BQ (Hamming):** Utilize `_mm512_xor_si512` and `_mm512_popcnt_epi64`. A 1536-dim binary vector (192 bytes) must be processed in exactly 3 register operations.

## 6. Query Execution & Z-Set Validation

The top-level search operator executes in `table.rs` and dynamically filters out deleted/updated vectors without tombstone logic.

### 6.1. Search API
Implement `Table::search_vector(&mut self, col_idx: usize, query: &[u8], k: usize, nprobe: usize) -> Vec<(u128, i64)>`.

### 6.2. Execution Flow
1. **Candidate Generation:**
   * Iterate over `self.shard_index.all_shard_arcs()`.
   * For shards with an IVF sidecar, call `search_ivf` to get candidate `row_idx`s.
   * For shards without IVF, and for the `MemTable`, treat all valid row indices as candidates.
2. **Refinement:**
   * Fetch the exact vector bytes for all candidates via `col_ptr_by_logical`.
   * Compute exact distances using the SIMD kernels.
   * Maintain a Min-Max Binary Heap of size `K * 2` (oversampling to account for potential ghosts).
3. **Z-Set Validation (Ghost Elimination):**
   * Iteratively pop the closest candidate `(shard_id, row_idx)` from the heap.
   * Fetch the candidate's PK via `shard.get_pk(row_idx)`.
   * Call `self.get_weight_for_row(pk, shard.as_ref(), row_idx)`.
   * If the returned weight $\le 0$, the vector is a ghost (deleted or superseded). **Discard it.**
   * If the returned weight $> 0$, the vector is live. Append it to the final result set.
   * Terminate when $K$ validated results are collected.

## 7. Edge Cases and Failure Modes

* **Malformed Data Ingestion:** `Batch::append_row_simple` must strictly validate vector lengths against `SchemaColumn::size`. If `f32` vectors contain `NaN`s, reject the transaction at the API layer, as `NaN`s will violate the Min-Max Heap distance invariants during search.
* **K-Means Degeneration:** If K-Means converges to $< K$ unique centroids (e.g., due to highly duplicated datasets), the builder must safely collapse empty posting lists and serialize a smaller $K$ to prevent empty memory accesses.
* **OOM Protection during Compaction:** If `num_centroids * col_size` exceeds 64MB during the `build_shard_image` phase, cap $K$ to enforce memory boundaries. The query engine will safely fall back to scanning larger posting lists.
* **Concurrent DML:** DBSP's epoch-based execution ensures `CursorHandle` and `MappedShard` references are immutable snapshots. Z-Set validation against `get_weight_for_row` is strictly race-free.
