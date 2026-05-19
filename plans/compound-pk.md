# Compound primary keys

Enable `PRIMARY KEY (a, b, ...)` in GnitzDB. Today every table is required
to have exactly one PK column.

Storage and merge already handle compound and wide PK regions end-to-end:
`pk_stride` accepts the full `0..=MAX_PK_BYTES` range, the `Batch` /
`MemBatch` / `MappedShard` PK-bytes accessors (`get_pk_bytes`,
`extend_pk_bytes`, `set_pk_at_bytes`) exist, the column-aware
`compare_pk_bytes` comparator is live (signed- and float-aware), the
in-memory N-way merge / sort+consolidate / weight-fold order and
consolidate compound PKs of any width via `compare_pk_bytes` (narrow
regions on the packed-u128 key, wide regions on the raw PK bytes), and
hash partition routing covers any width through `partition_for_pk_bytes`.

What remains is every boundary that still calls
`SchemaDescriptor::pk_index_single()` — catalog, `compiler.rs`,
`ops/reduce.rs`, `runtime/master`, `gnitz-sql`, `gnitz-core`'s wire
`Schema`, `gnitz-py`, `gnitz-capi` — plus the storage accessors and shard
metadata that still assume a u128-packed PK. Each `pk_index_single()` call
`assert!`s `pk_count == 1` in release; that assertion is the canary that
fires the day a compound PK reaches an unmigrated boundary.

**Design choice — Concat.** The PK region in the physical batch / WAL
block / shard file holds the concatenated raw PK column bytes, one row
after the next. PK columns are *not* duplicated into payload regions —
the PK region is the PK. Ordered comparison goes through the
column-aware `compare_pk_bytes` helper, which iterates `pk_columns()`
with type dispatch, mirroring `compare_rows`'s payload loop. The
single-PK case stays on its u128-keyed fast paths (XOR8 keys,
`HeapNode.key`, shard `pk_min`/`pk_max` as u128); compound PK adds a
parallel slower path with no cost to single-PK.

## Scope

In:

- `PRIMARY KEY (a, b, ...)` in CREATE TABLE, up to 4 columns
  (planner-enforced; the internal `MAX_PK_COLUMNS = 5` slot is
  reserved for secondary-index tables — see Vocabulary).
- PK columns of any currently-allowed PK type (U64, U128, UUID). Mixed
  types within one compound PK allowed. Total `pk_stride ≤ 64` for
  user tables (`≤ MAX_PK_BYTES = 80` once secondary-index tables are
  factored in).
- INSERT, ON CONFLICT, UPDATE/DELETE point lookups extended to bind
  the full PK tuple.
- FOREIGN KEY: a single-column FK referencing one column of a compound
  PK is rejected unless that column carries its own UNIQUE index (see
  Foreign keys below).
- Compound PK on user tables only. System catalog tables stay
  single-PK (their PKs are already pre-packed via `pack_*_pk` helpers
  in `catalog/sys_tables.rs`).

Out:

- Multi-column foreign keys (`FOREIGN KEY (a, b) REFERENCES t (x, y)`).
- Migrating existing single-PK tables to compound PK.
- Compound PK with String/Blob columns (variable-width PK columns
  would force the PK region to vary per row, not just per schema).
- Partial-tuple ON CONFLICT (`ON CONFLICT (a)` when PK is `(a, b)`).

## Vocabulary

- `MAX_PK_COLUMNS = 5`. User-facing `CREATE TABLE` caps PK columns at
  4 (planner-enforced). The internal cap is one higher so secondary
  indexes — modeled as compound-PK tables `(indexed, src_pk_0, …,
  src_pk_{k-1})` in the Operators section — can hold a 4-PK source
  plus the indexed column without overflowing `pk_indices`.
- `pk_stride`: total bytes per row of the PK region, `sum(strides of
  PK columns)`. `u8`-sized, bounded by `MAX_PK_BYTES`.
- `MAX_PK_BYTES = 80` (`MAX_PK_COLUMNS × 16`). Sized for the widest
  secondary-index PK (U64 indexed + 4 × U128 source PK). Used by
  `PkBuf` in shard metadata and the wire-side bytes buffer.
- Two orthogonal axes gate the storage dispatch:
  - **Region width** (`pk_stride <= 16` vs `pk_stride > 16`): governs
    which storage path can keep the PK packed in a u128 word. Order-
    agnostic byte paths — XOR8 build, the `extend_pk(u128)` writer —
    keep the region in a u128 whenever it fits in 16 bytes, regardless
    of column count, because they treat the region as opaque bytes.
  - **Column count** (`pk_count == 1` vs `pk_count >= 2`): governs
    which comparison path is correct. `pk_count == 1` allows a u128
    numerical compare on the PK region. For `pk_count >= 2`, ordering
    requires column-aware `compare_pk_bytes` — a u128 compare over a
    concatenated tuple puts the trailing column in the high bits and
    reverses priority, so even compound `(U64, U64)` (16-byte region)
    must take the column-aware path for ordered comparisons.
- "narrow region" / "wide region" refer to the region-width axis only.
  They never imply anything about ordering. Ordered call sites
  (`find_lower_bound`, shard overlap, heap key compare) gate on
  `pk_count`; opaque-bytes call sites gate on `pk_stride`.

---

## The change, layer by layer

### 1. Storage batch (`storage/batch.rs`)

`pk_stride` already accepts the full `0..=MAX_PK_BYTES` range, the
bytes accessors (`extend_pk_bytes`, `set_pk_at_bytes`, `get_pk_bytes`)
exist, and the narrow-PK `get_pk` / `extend_pk` / `set_pk_at` family
keeps its `match stride { 8 | 16 => …, _ => panic! }` shape (those
panics guard the u128 API against wide-stride misuse; compound-PK code
paths use the bytes accessors instead). The remaining work here:

- `Batch::append_row_from_ptable_found` takes `pk: u128`; change to
  `pk_bytes: &[u8]` using `extend_pk_bytes`. `PartitionedTable`'s
  found-row plumbing (`storage/partitioned_table.rs`) follows: the
  found-row PK is retained as bytes and surfaced via
  `PartitionedTable::found_pk_bytes()` (alongside the existing
  `found_null_word` / `found_col_ptr` / `found_blob_ptr`).
- `Batch::find_lower_bound(key: u128)` stays as the narrow-PK helper.
  Add `find_lower_bound_bytes(&[u8])` for compound, routing through
  `compare_pk_bytes`. The shard-reader and read-cursor equivalents
  (`shard_reader.rs`, `read_cursor.rs`) get the same pair.

### 2. XOR8 (`storage/xor8.rs`, `storage/shard_file.rs`)

XOR8 builds on `&[u128]` keys (`xor8::build(&pks: &[u128])`). Keep
that API. `build_xor8_from_pk_region` today only handles stride 8 / 16;
for wide PK it must hash each row's PK bytes to u128 first:

```rust
fn build_xor8_from_pk_region(pk_ptr, pk_sz, n) -> Option<Filter> {
    let stride = pk_sz / n;
    let pks: Vec<u128> = (0..n).map(|i| {
        let row = unsafe { from_raw_parts(pk_ptr.add(i*stride), stride) };
        if stride <= 16 {
            let mut b = [0u8; 16]; b[..stride].copy_from_slice(row);
            u128::from_le_bytes(b)            // existing behaviour
        } else {
            xxh::checksum128(row)             // collapse to u128 for the filter
        }
    }).collect();
    xor8::build(&pks)
}
```

The probe API takes a u128 key; callers compute it the same way build
does. False-positive rate unchanged.

### 3. Shard `pk_min` / `pk_max` (`storage/shard_index.rs`, `storage/manifest.rs`)

`ShardEntry`, `LevelGuard`, `PendingShard`:

```rust
pub struct PkBuf {
    pub bytes: [u8; MAX_PK_BYTES],   // 80
    pub len:   u8,                   // == owning table's pk_stride
}

struct ShardEntry {
    ...
    pk_min: PkBuf,
    pk_max: PkBuf,
}
```

`PkBuf` is a plain value type — no generics, no trait bounds. `len`
mirrors the table's `pk_stride` so manifest round-trip preserves the
exact key width.

For `pk_count == 1` the existing u128 numerical compare applies (the
PkBuf holds one column's LE bytes, which sort numerically). For
`pk_count >= 2`, range-prune (`pk_min <= key <= pk_max`) and any
shard-overlap test go through
`compare_pk_bytes(schema, &pk_min.bytes[..len], &key[..len])` —
byte-wise lex compare of LE bytes is wrong (e.g. `1` = `[0x01, 0, …]`
vs `256` = `[0x00, 0x01, 0, …]`).

**Manifest on-disk format.** `storage/manifest.rs` stores `pk_min`/
`pk_max` as `u128 LE` at fixed offsets. Replace with `(len: u8,
bytes: [u8; MAX_PK_BYTES])` pairs; `ENTRY_SIZE_V*` widens accordingly.

### 4. WAL block (`runtime/sal.rs`, `storage/wal.rs`)

The WAL block format declares region 0 as "pk", region 1 "weight",
region 2 "null_bmp", regions 3+ payload, last region blob; per-region
size is in the directory. The pk region's per-row stride was implicit
(8 or 16). Encoders read it from the schema; decoders read it from the
directory (`pk_region_size / row_count = pk_stride`). Both generalise
without a format change.

Touch points:

- `runtime/wire.rs` (`schema_wal_block_size`): compute `pk_stride =
  sum(pk col strides)` via `schema.pk_stride()`.
- `runtime/sal.rs`: same `pk_stride` plumbing on encode/decode.
- `gnitz-core/src/protocol/wal_block.rs`: replace the
  `schema.columns[schema.pk_index_single()].type_code.wire_stride()`
  derivation with `schema.pk_stride()`; the `pk_stride == 8` / `16`
  decode arms gain a wide bytes arm.
- `storage/wal.rs`: the `positions` ceiling still suffices (3 fixed +
  68 payload + blob).

The on-disk WAL layout is unchanged conceptually; only region 0's
contents widen.

### 5. Wire control block (`gnitz-wire/src/lib.rs::control`)

`CONTROL_COLS` declares `seek_pk: U128`. For wide PK that's
insufficient. Add `seek_pk_extra: BLOB nullable` carrying bytes 16..
for wide PK; empty for `pk_stride ≤ 16`. The fast path
(`encode_ctrl_block_template_fast`) stays a memcpy of the precomputed
template plus the patch fields; wide PK falls through to
`encode_ctrl_block_ipc`, which already handles a blob region for
`error_msg`. (Keep `seek_pk: U128` rather than widening it to BLOB:
a BLOB `seek_pk` would break the template-copy fast path
`CTRL_BLOCK_TEMPLATE` in `runtime/wire.rs`, which dominates the
error-response cost.)

`gnitz-engine/src/runtime/wire.rs`:

- `encode_ctrl_block_ipc`: add `seek_pk_extra: &[u8]` parameter.
- `CONTROL_SCHEMA_DESC` / `ctrl_region_offset`: pick up the new column
  automatically — its region offset is computed from `CONTROL_COLS`.
  `CTRL_BLOCK_SIZE_NO_BLOB` grows by one region's fixed bytes (0 for a
  blob region, but `align8` may add padding).

### 6. Wire data plane: `ZSetBatch` PK encoding

`gnitz-core::ZSetBatch` carries client-side PK columns as a `PkColumn`
enum (`gnitz-core/src/protocol/types.rs`) with `U64s(Vec<u64>)` and
`U128s(Vec<u128>)` variants. Add a compound variant:

```rust
pub enum PkColumn {
    U64s(Vec<u64>),
    U128s(Vec<u128>),
    Bytes { stride: u8, buf: Vec<u8> },   // new
}
```

`for_type` keeps `U64s`/`U128s` when `pk_count == 1`; compound schemas
select `Bytes { stride: schema.pk_stride(), .. }`. `push`, `get`,
`swap`, `truncate`, `len` get a third arm slicing by `stride`. The
wire encoder forwards the buffer verbatim — the worker decodes the
same `stride` from the table's schema. `ZSetBatch::new` replaces
`pk_index_single()` with `pk_indices()[0]` (single-PK) or selects the
`Bytes` variant; SQL INSERT/SEEK paths in `gnitz-sql/src/dml.rs`
serialize each extracted PK column into LE bytes in `pk_indices()`
order and push to the chosen variant.

### 7. Catalog: persist the PK column list

`TABLE_TAB`'s `pk_col_idx: u64` field (`catalog/sys_tables.rs`) is the
persistence site. Repack it as:

```
bits [0..4)    pk_count       (1..=4)
bits [4..11)   pk_col_idx[0]
bits [11..18)  pk_col_idx[1]
bits [18..25)  pk_col_idx[2]
bits [25..32)  pk_col_idx[3]
bits [32..64)  reserved (zero)
```

`MAX_COLUMNS = 65`, so column indices fit in 7 bits. The user-facing
PK cap is 4 (the internal `MAX_PK_COLUMNS = 5` slot is for index
tables, not persisted via TABLE_TAB). All writes use this layout; no
legacy discriminator.

Touch points:

- `catalog/ddl.rs` (`create_table` signature): `pk_col_idx: u32` →
  `pk_cols: &[u32]`. Validation loops over all PK indices for type and
  nullability; pack into the u64 with the encoding above.
- `catalog/hooks.rs`: decode → unpacking helper in `sys_tables.rs`;
  PK-type validation iterates; schema construction passes the list.
- `catalog/store.rs` (`build_schema_from_col_defs`): accept `&[u32]`
  instead of `u32`.
- `catalog/cache.rs`: `pk_col_of: FxHashMap<i64, u32>` →
  `FxHashMap<i64, ArrayVec<u32, 4>>` (or a packed u32 mirroring the
  on-disk encoding). Cache invalidates when *any* PK column index
  changes.
- `catalog/validation.rs`, `catalog/hooks.rs` (FK type resolution):
  the FK's `referred_columns` clause names exactly one parent column;
  the catalog records that resolved index. Type promotion reads
  `parent_schema.columns[recorded_idx].type_code` — no
  `pk_index_single()`.
- `catalog/utils.rs` (`BatchBuilder::physical_col_idx`): walk the PK
  list to compute "number of PK columns with index < col_idx" and
  subtract. Single-PK is the special case.

System tables stay single-PK; `make_schema(cols, pk_index)` continues
to wrap a single index.

### 8. SQL planner (`gnitz-sql/src/planner.rs`)

CREATE TABLE:

- Table-level PK constraint: drop the `pk_cols.len() != 1` rejection.
  Resolve every named column; enforce no duplicates within the PK
  list; enforce `len <= 4`. Reject mixing inline `PRIMARY KEY` columns
  with a table-level `PRIMARY KEY (...)` (generalise the existing
  multi-PK message).
- Inline single-column PK: unchanged.
- PK type coercion: drop the I*/U*→U64 coercion entirely. Each PK
  column retains its declared scalar type; `compare_pk_bytes` reads
  each column's `type_code` and runs the right signed/unsigned/float
  compare. With the comparator already signed-aware, dropping the
  coercion is what makes native signed PKs (I8/I16/I32/I64) sort in
  signed numerical order end-to-end. Validation still enforces
  `is_pk_eligible(type_code)` and non-nullable for every PK column.
- `client.create_table` call: pass `&pk_cols[..]`. Update
  `gnitz-core/src/client.rs` signature.
- `build_projection` (view PK): replace `pk_index_single()` with a
  `pk_indices()` walk. View output schema carries the source's PK
  column list verbatim.

FK:

- `resolve_fk_target` currently returns `(tid, pk_index, pk_type)` for
  the parent's single PK. For compound-PK parents, resolve the FK's
  `referred_columns` clause:
  - If it names exactly one parent column, and that column either (a)
    has its own UNIQUE index or (b) the parent has `pk_count == 1` and
    the named column is the PK, accept and return `(tid, named_idx,
    named_type)`. Otherwise reject with a clear error.
  - A single-column FK to a compound-PK parent must point at a column
    the parent has otherwise constrained to be unique.

JOIN / GROUP BY reindex: `cb.map_reindex(filtered, single_pk_idx,
prog)` is unchanged — multi-column joins remain rejected at the
planner, so `map_reindex` is still called with a single column.

DML (`gnitz-sql/src/dml.rs`):

- ON CONFLICT validation: allow `Columns(cols)` with `len > 1` iff it
  equals the table's PK column set (any order).
- `extract_pk_value`: return a stack-resident byte buffer of exactly
  `schema.pk_stride()` bytes, populated by serialising each extracted
  literal as LE bytes in `schema.pk_indices()` order (not AST or
  column-name order). The caller splits this into the wire `seek_pk`
  (first 16 bytes) + `seek_pk_extra` (remainder) pair.
- `client_side_filter_do_nothing` / `client_side_merge_do_update`: the
  local-dedup set is `HashSet<u128>`. For compound PK that truncates
  and collides silently. Change to a key carrying the full
  `(stride, [u8; MAX_PK_BYTES])` so the whole compound key
  participates in hash and eq. Single-PK stays correct under the same
  code path.
- `try_extract_pk_seek`: walk the AND chain in any order, bucketing
  each `col = literal` predicate into a per-PK-column slot indexed by
  the PK column's schema position. Reject unless every PK column has
  exactly one binding. Then serialise the literals into the
  `pk_stride`-byte buffer in `schema.pk_indices()` order — the on-disk
  PK byte layout is the only ground truth; AST/binding order is
  irrelevant. Partial (prefix) bindings fall back to the full scan.
- `try_extract_pk_in`: only single-column IN lists. Compound-PK tables
  → no IN-pushdown for now (correct fallback).

### 9. Python / C API surface

- `gnitz-py/src/lib.rs`: `PySchema.pk_index` →
  `PySchema.pk_indices: Vec<u32>`. Existing single-PK code paths read
  `pk_indices[0]` after `assert len == 1`.
- `gnitz-capi/src/lib.rs`: `create_table` C ABI accepts
  `pk_count: u32` and `pk_cols: *const u32`. Existing single-column
  callers pass `pk_count = 1, &single_idx`.

### 10. Operators

#### map_reindex

Unchanged for this plan. `map_reindex`
(`gnitz-core/src/circuit.rs`) still rewrites the PK slot from a single
source column. Multi-column joins are blocked at the planner; a
compound `map_reindex` would be dead until that restriction lifts.

#### Join

`compiler.rs` (`merge_schemas_for_join_impl`): output schema is
`[left_PK_cols..., left_payload..., right_payload...]`. Generalise:

```rust
let mut n = 0;
let mut pk_idx_list = ArrayVec::<u32, 4>::new();
for (_, _, c) in left.pk_columns() {
    cols[n] = *c;
    pk_idx_list.push(n as u32);
    n += 1;
}
for (_, _, c) in left.payload_columns() { cols[n] = *c; n += 1; }
for (_, _, c) in right.payload_columns() { /* nullable propagation */ n += 1; }
SchemaDescriptor::new(&cols[..n], &pk_idx_list[..])
```

For single-PK left this produces exactly today's
`SchemaDescriptor::new(&cols[..n], &[0])`; for compound left PK with
`pk_count = k`, `pk_indices = [0, 1, ..., k-1]`.

`ops/join.rs` (`write_join_row`):
`output.extend_pk(left_batch.get_pk(left_row))` →
`output.extend_pk_bytes(left_batch.get_pk_bytes(left_row))`. Narrow-PK
joins still hit the `extend_pk → u128` path because the bytes form is
a strict superset for `pk_stride ≤ 16`. The `left_npc` null-bit offset
is unchanged — left's payload column count doesn't depend on PK width.

#### Reduce

`ops/reduce.rs`: `in_pki = pk_index_single()` becomes `pk_indices`.
`group_by_pk` detection fires when `sorted(group_by_cols) ==
sorted(input_schema.pk_indices())`; the fast path treats the entire PK
region as the group key.

`compiler.rs` (`build_reduce_output_schema`): the `use_natural_pk`
rule today fires when `group_cols.len() == 1` and that column's type
is `U64`/`U128`/`UUID` (any PK-eligible scalar promotes to the
output's natural PK). Preserve that verbatim. *Additionally* fire when
the group-column set, as an unordered set, equals
`schema.pk_indices()` — then the output's PK region is the group
columns concatenated in `pk_indices()` order, mirroring the source's
PK shape. Both rules can be true at once (single PK, group by it) and
produce the same output. Falling through neither keeps the existing
synthetic U128 PK.

`emit_reduce_row`: `output.extend_pk(group_key)` for synthetic-PK
output is unchanged; for natural-PK output the group_key *is* the
row's PK region bytes →
`output.extend_pk_bytes(source_mb.get_pk_bytes(exemplar_row))`.

#### Distinct, Linear, Scan

- Distinct (`ops/distinct.rs`): element identity is `(PK, payload)`.
  PK comparison goes through the heap key path automatically; compound
  PK works once `compare_rows` is invoked on PK tie (as it already
  is). No code change.
- Linear (`ops/linear.rs`): output schema construction follows
  `merge_schemas_for_join_impl` — copy all PK columns from input, then
  payload, then `pk_indices = [0..pk_count]`.
- Scan (`ops/scan.rs`): no PK assumption in the scan body.

#### Index / GI / AVI (`ops/index.rs`)

Secondary indexes today hand-pack `(indexed_col_value, source_pk)`
into a u128 composite key (`(gc_u64 << 64) | source_pk_lo`, with
`spk_hi` as an I64 payload), relying on the source PK fitting in 64
bits and the indexed column promoting to a 64-bit order-preserving
key. With compound source PKs that pack no longer fits; drop the
hand-packing and make the secondary-index table itself a compound-PK
table.

`make_gi_schema(source)` / `make_avi_schema(source)` return:

- GI: PK columns
  `(promoted_indexed: U64, src_pk_col_0: T0, src_pk_col_1: T1, ...)`
  with `pk_indices = [0, 1, ..., src.pk_count]`. The `spk_hi` payload
  column disappears — the source PK columns are now first-class PK
  columns of the index table.
- AVI: same shape, with the for_max negation applied to the first
  column at insert time.

Ingest at `op_integrate_with_indexes` writes each PK column directly
(`extend_pk_bytes` over the concatenated bytes), and the storage layer
sorts via `compare_pk_bytes`.

**Master index-routing cache.** `extract_col_key`'s PK-col branch and
`PartitionRouter` (`ops/exchange.rs`) — the master-side
unique-secondary-index unicast cache keyed `(tid, col_idx, u128)` —
still funnel the key through `get_pk` and a u128 map key, which
truncates a wide source PK. Generalise the cache key to carry the full
`(stride, bytes)` PK so a compound source PK routes without collision.
This is the only PK-routing site that still assumes a ≤16-byte key.

**Lookup is a prefix scan.** A `WHERE indexed = X` query binds only
the leading PK column — the source PK is what we're finding, not
something the caller knows. `seek_by_index` changes shape:

```rust
// catalog/store.rs
pub fn seek_by_index(&mut self, table_id: i64, col_idx: u32,
                     prefix: &[u8]) -> Result<Option<Batch>, String>;
```

The cursor does `find_lower_bound_bytes(prefix)`, walks forward
collecting every row whose PK region begins with `prefix`, stopping at
the first that doesn't. Each collected index row exposes its trailing
source-PK columns; the call resolves each via `seek_family` and
returns the union as a single Batch. UNIQUE indexes stop after one
match; non-unique yield all.

The wire/SAL `seek_by_index` carries the prefix via the same
`(seek_pk: U128, seek_pk_extra: BLOB)` pair — the indexed column is at
most 16 bytes, so for typical lookups `seek_pk_extra` is empty and the
narrow fast path applies. The control-block response gains a
`FLAG_HAS_DATA` continuation streaming the resolved source rows (same
pattern as `scan`).

Single-PK sources also benefit: the index table becomes
`pk_count == 2` (`(indexed, source_pk)`) instead of the
`(gc_u64 << 64) | spk_lo` u128 trick — cleaner with no extra cost
since `pk_stride == 16` is on the narrow-region fast path.

### 11. Range / scan operations (`storage/read_cursor.rs`)

`seek(key: u128)` stays the narrow-PK entry point; add
`seek_bytes(key: &[u8])` for wide PK. Both call the same
merge-tree-positioning logic, parameterised on the PK comparison.

`current_key: u128` stays for narrow PK. For wide-PK cursors, also
expose `current_pk_bytes(&self) -> &[u8]` reading from the active
source without copying. Most consumers (DAG, VM bindings) call
`current_pk_bytes()` and forward to the next operator; the existing
single-PK call sites (e.g. `reduce` trace_out_cursor seek) stay
narrow.

---

## Foreign keys to compound parent PKs

The current FK design references one parent column and validates type
match. That's fine for a single-column parent PK — the referenced
column *is* the unique constraint. With a compound parent PK, a
single-column FK references a non-unique value, violating SQL
semantics.

Rule for this plan:

- `FOREIGN KEY (child) REFERENCES parent (col)`: accepted iff
  `parent.pk_count == 1 && col == parent_pk[0]`, OR `col` has its own
  declared UNIQUE index in `parent`.
- Multi-column FK (`FOREIGN KEY (a, b) REFERENCES parent (x, y)`): out
  of scope. Error message points at this limitation.

---

## Testing

### Rust unit tests

- `storage/batch.rs`: build a compound-PK batch via `with_schema`;
  insert rows with varying PK tuples; verify sort by `(PK bytes,
  payload)`; verify `get_pk` panics on a wide-region batch.
- `storage/shard_index.rs`: range prune with `PkBuf` `pk_min`/
  `pk_max`, including the LE-bytes regression (`pk_min` numeric >
  `pk_max` numeric in byte-lex order — must not prune).
- `storage/manifest.rs`: round-trip the new format on schemas with
  `pk_stride` covering 8, 16, 24, 80.
- `runtime/sal.rs`: WAL block encode/decode round-trip on a
  compound-PK schema with `pk_stride = 64`.
- `runtime/wire.rs`: CONTROL block encode/decode with `seek_pk_extra`
  non-empty.

### Engine integration tests

- Ingest into a compound-PK table; seek by full PK tuple; scan;
  retract; re-insert. Duplicate compound-PK tuple in the same batch →
  conflict. Same exercise across narrow and wide compound schemas.

### Python E2E (`tests/test_compound_pk.py`)

- `CREATE TABLE ... PRIMARY KEY (a, b)`; `PRIMARY KEY (a, b, c, d)`;
  inline `PRIMARY KEY` mixed with table-level rejected.
- INSERT same compound tuple twice → conflict (with and without ON
  CONFLICT DO NOTHING / DO UPDATE).
- UPDATE/DELETE WHERE matching full tuple → `try_extract_pk_seek` fast
  path. WHERE on a PK prefix falls back to full delta scan, still
  correct.
- JOIN with compound-PK left side. Verify output schema shape and rows.
- GROUP BY all PK columns → `group_by_pk` fast path. GROUP BY a prefix
  → general reduce path.
- FK to a compound-PK parent: (a) wrong column → rejected with the new
  error; (b) a column with its own UNIQUE index → accepted.
- Multi-worker (`GNITZ_WORKERS=4`): partition routing by compound PK
  is consistent across workers (regression test for the
  `hash_row_by_columns` invariant in dev-guide.md).

### Native signed PK

Drop-the-coercion regression: tables with `I8`/`I16`/`I32`/`I64` PK
columns, mixed positive and negative values, scan, verify ascending
order matches signed numerical order (`-128 < -1 < 0 < 127`).
Single-column and compound. Multi-worker — partition routing on a
signed value must hash identically to its `as i64` form regardless of
the producing worker.

---

## Rollout

Order of merging (each step keeps the test suite green):

1. **Catalog encoding.** Pack `pk_count + pk_cols` into TABLE_TAB.
2. **SAL/WAL pk_stride.** Region 0 width follows `schema.pk_stride()`.
3. **Manifest format.** Write `pk_min`/`pk_max` as `(len, bytes)`.
4. **Wire control extension.** Add `seek_pk_extra`.
5. **SQL planner.** Accept `PRIMARY KEY (a, b, ...)`; drop the
   I*/U*→U64 PK coercion. From this commit, CREATE TABLE allows
   compound PK and native signed PK types — the feature becomes
   user-visible here.
6. **DML.** `extract_pk_value` returns a tuple; `try_extract_pk_seek`
   requires full-tuple binding.
7. **Operators.** Schema builders in `compiler.rs` and `reduce`/`join`
   adjustments. Index seek becomes prefix-scan.
8. **FK stricter rule.** Reject single-column FK to a compound-PK
   parent unless the referenced column has its own UNIQUE index.
9. **Python/CAPI.** `pk_index` → `pk_indices`.

Each step is independently mergeable; the feature only becomes
user-visible at step 5.
