



# GnitzDB Vector Search Implementation Blueprint

This document defines the exhaustive technical specification for implementing vector search in GnitzDB. The architecture leverages an Inverted File Index (IVF) as an immutable sidecar within the FLSM shards, combined with AVX-512 SIMD distance computation. Deletions and updates are handled entirely by GnitzDB's existing Z-Set ghost elimination (`get_weight_for_row`), requiring zero tombstone management within the vector index itself.

## 0. Codebase Reconciliation (verified 2026-05-22)

This blueprint predates the current tree. The corrections below are load-bearing — the original section numbers referenced stale constants and a stale type model.

### 0.1 Pre-alpha status — what it simplifies

GnitzDB has no production users and no persistent data that must survive an upgrade. Therefore:

* **No shard migration / dual-format reader.** Bumping `SHARD_VERSION` is a hard break: the reader may reject any other version outright, and developers wipe data dirs between builds. Do not build a backward-compatible read path for the current format.
* **No header reservation for "future extensions."** Because the on-disk format can be broken again at zero cost, `HEADER_SIZE` only needs to grow enough for the two new fields (§2.1). The 128-byte "room for the future" padding in the original draft is unnecessary speculation — expand the header again if and when a later field needs it.
* **No feature flag or staged rollout.** `SchemaColumn`'s memory layout, the catalog serialization, and the `TypeCode` enum can all change outright.

What pre-alpha does **NOT** simplify: the type-code collision (§1.2), the `size`-derived-from-`type_code` problem (§1.1), and the batch stride widening (§1.1) are in-memory correctness/architecture issues, unaffected by the absence of users. These are the hard parts and remain fully in scope.

### 0.2 File paths and symbol locations

* `schema.rs` → `crates/gnitz-engine/src/schema.rs`; `layout.rs` → `crates/gnitz-engine/src/layout.rs`.
* `batch.rs`, `columnar.rs`, `shard_file.rs`, `shard_reader.rs`, `table.rs`, `compact.rs`, `xor8.rs` → `crates/gnitz-engine/src/storage/`.
* `compact_shards` lives in `storage/compact.rs` (NOT `shard_file.rs`); `flush_inner` and `get_weight_for_row` are in `storage/table.rs`; `col_ptr_by_logical`, `get_pk`, and `MappedShard` are in `storage/shard_reader.rs`.
* `type_code` is **defined** in `crates/gnitz-wire/src/lib.rs` and re-exported as `crate::schema::type_code`. New codes and all `TypeCode` enum changes happen in the **wire crate**, not `schema.rs`.
* `math.rs` is net-new (no SIMD/distance module exists today). Place it at `crates/gnitz-engine/src/math.rs` and register it in the crate module tree.

## 1. Schema and Memory Layout Upgrades

Vectors are fixed-width binary payloads. To support high-dimensional vectors, the internal batch layout limits must be expanded.

### 1.1. Capacity and Alignment Upgrades

1. **Per-column size must become independent of `type_code` (the central change).** Today `SchemaColumn` is `#[repr(C)] { type_code: u8, size: u8, nullable: u8, _pad: u8 }`, `size` is **private**, and it is computed by `SchemaColumn::new(tc, nullable)` as `type_size(tc)` → `gnitz_wire::wire_stride(tc)`. The doc comment is explicit: size is *"never written independently."* This model cannot represent a vector, whose byte width = `dim × element_size` is not derivable from the type code alone. Required work:
   * Widen `size: u8` → `size: u32` and update `SchemaColumn::size() -> u32`. This changes the `#[repr(C)]` layout (currently 4 bytes); audit every site that copies `SchemaColumn` as raw bytes — in particular the **catalog (de)serialization** path — and `SchemaDescriptor`'s `#[derive(PartialEq)]` / manual `eq`.
   * Add an explicit-size constructor (e.g. `SchemaColumn::new_sized(tc, nullable, byte_size)`) for vector columns; scalar columns keep deriving size from `type_code`. DDL must supply the dimension at this point. Note `SchemaDescriptor::new` is a `const fn`, so the new path must also be `const`-compatible.
   * `pk_stride` stays `u8` (asserts ≤ 255): vectors are not PK-eligible (§1.2), so the PK region is unaffected.

2. **Batch payload strides must widen `u8` → `u32`.** In `storage/batch.rs`, `strides` is `[u8; MAX_BATCH_REGIONS]` (`MAX_BATCH_REGIONS = 68`) and `compute_offsets` reads strides as `u8`. A 1536-dim f32 vector is 6144 bytes — far past 255 — so this overflow is real. Widen `strides` to `[u32; MAX_BATCH_REGIONS]` and fix the fan-out: `compute_offsets`, `fill_payload_strides`, `strides_from_schema`, `Batch::empty`, and the `strides`/`offsets` fields on the batch struct. `REG_PK`/`REG_WEIGHT`/`REG_NULL_BMP` fixed regions are unaffected in value but ride along the widened array.

3. **SIMD Alignment:** `compute_offsets` does **not** currently align regions (`offsets[i] = off; off += capacity * strides[i]`). A 64-byte `ALIGNMENT` const already exists in `layout.rs` — reuse it. Enforce alignment per boundary for safe/optimal AVX-512 loads:
   ```rust
   off = (off + ALIGNMENT - 1) & !(ALIGNMENT - 1);
   offsets[i] = off as u32;
   off += capacity * strides[i] as usize;
   ```
   Note `offsets` is `u32`; `capacity × vector_byte_width` can approach 4 GiB for large batches of high-dim vectors — confirm the u32 offset domain is sufficient or widen it too.

### 1.2. Vector Type Codes

The original codes `10/11/12` **collide** with existing types — in `gnitz-wire/src/lib.rs` `F64 = 10`, `STRING = 11`, `U128 = 12`. The highest code in use is `BLOB = 14`, so vector codes take the next free values. Add to the `type_code` module in `crates/gnitz-wire/src/lib.rs`:
* `VECTOR_F32 = 15`: Standard 32-bit floats.
* `VECTOR_SQ8 = 16`: 8-bit scalar quantized integers.
* `VECTOR_BQ  = 17`: Binary quantized bitsets.

Because `type_code` lives in the wire crate, every dependent definition there must be extended in lockstep:
* **`TypeCode` enum** — add `VectorF32 = 15`, `VectorSq8 = 16`, `VectorBq = 17`, and arms in `try_from_u8`.
* **`TypeCode::stride()`** returns a fixed `u8` per variant and is exhaustive; it *cannot* yield a meaningful value for variable-width vectors. Decide the contract: either make the vector arms `unreachable!()`/panic (callers must use `SchemaColumn::size()` instead) or return a 0 sentinel. `SchemaColumn::size()` is the authoritative width for vectors.
* **`wire_stride(tc: u8)`** (the free `const fn`) has the same problem — its `match` returns fixed widths and falls through to `8` for unknown codes. Ensure no caller uses `wire_stride(vector_code)` as the true width; the schema-level `size()` is authoritative.
* **`is_pk_eligible`** is an allow-list, so vectors are automatically PK-ineligible — no change needed, but this is the correct behavior (matches §6/§7's no-float-PK rule).
* **`schema.rs::promote_to_index_key`** matches exhaustively on `TypeCode`; add vector arms (vectors never serve as index keys, so a panic/unreachable arm is acceptable).

### 1.3. Z-Set Total Ordering
In `storage/columnar.rs::compare_rows`, extend the existing per-column `match col.type_code` with arms for the new types to satisfy Z-Set consolidation invariants. Note the scalar `F32`/`F64` arms in this match **already use `total_cmp`** — the vector arms follow the same precedent:
* **VECTOR_F32:** Cast the raw byte slice to `&[f32]` (`bytemuck` or `slice::from_raw_parts`) and compare lexicographically using `f32::total_cmp`. This makes `NaN`s group deterministically and cancel correctly, consistent with the scalar float handling already present.
* **VECTOR_SQ8 / VECTOR_BQ:** Compare lexicographically using standard `Ord` on `&[u8]`.

## 2. IVF Sidecar Format (Shard Level)

The vector index is an Inverted File Index (IVF) appended to the immutable shard file. It maps centroids to contiguous arrays of row indices (Posting Lists).

### 2.1. Header Expansion
In `layout.rs` and `storage/shard_file.rs`. Current state: `SHARD_VERSION = 6`, `HEADER_SIZE = 64`, with the last used fields `OFF_XOR8_OFFSET = 40` and `OFF_XOR8_SIZE = 48` (each `u64`, occupying bytes 40..48 and 48..56). So byte 56 is the first free slot.
* Bump `SHARD_VERSION` to `7` (it is already at 6, not 4). No compatibility shim — see §0.1; the reader rejects any other version.
* Define `OFF_IVF_OFFSET = 56` (u64, bytes 56..64) and `OFF_IVF_SIZE = 64` (u64, bytes 64..72).
* Grow `HEADER_SIZE` to at least 72 to contain `OFF_IVF_SIZE`. Per §0.1 there is no need to pad to 128 for hypothetical future fields; if a multiple of `ALIGNMENT` (64) is desired for body alignment, 128 is the next step, but the body is independently aligned by `compute_offsets`, so a 72-byte (or 64-aligned-to-128 only if measured to matter) header is sufficient.

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

Index construction is executed synchronously during `compact_shards` (in `storage/compact.rs`) and `flush_inner` (in `storage/table.rs`) on background threads. The actual builder body lives in `build_shard_image` / `write_shard_streaming` in `storage/shard_file.rs`, which those two sites call.

### 3.1. Builder Integration (`storage/shard_file.rs`)
Inside `build_shard_image` and `write_shard_streaming`, after the column regions and XOR8 filter are finalized:
1. Identify all `SchemaColumn` entries with vector type codes.
2. If `row_count < 1024`, bypass IVF generation (set `OFF_IVF_OFFSET = 0`). Small shards are faster to brute-force.
3. For `row_count >= 1024`, execute deterministic K-Means++ to locate centroids ($K = \lfloor\sqrt{N}\rfloor$).
   * *Determinism Check:* Seed the K-Means PRNG (e.g., `ChaCha8Rng`) using the shard's `min_lsn` to ensure bit-for-bit reproducible index builds. Under pre-alpha (§0.1) this is a test-stability nicety, not a correctness or migration requirement — any fixed seed works; `min_lsn` is just convenient.
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
