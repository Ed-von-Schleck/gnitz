# Compound primary keys

Enable `PRIMARY KEY (a, b, ...)` in GnitzDB. Today every table is required
to have exactly one PK column. The schema descriptor is already shaped
for it: `pk_indices: [u32; MAX_PK_COLUMNS=5]`, `pk_count: u32`,
`payload_mapping` excluding all PK columns, plus `pk_columns()` /
`pk_stride()` accessors. The storage layer, scatter routines, and
partition routing already iterate via `pk_indices()` / `pk_stride()`.
The remaining `pk_index_single()` call sites — catalog,
`compiler.rs`, `ops/reduce.rs`, `runtime/master::format_pk_value`,
`gnitz-sql`, `gnitz-core`'s wire `Schema`, `gnitz-py`, `gnitz-capi` —
are the boundaries this plan generalises. Their `pk_index_single()`
calls each `assert!(pk_count == 1)` in release; the assertion is the
canary that fires the day a compound PK reaches an unmigrated
boundary.

**Design choice — Concat.** The PK region in the physical batch / WAL
block / shard file holds the concatenated raw PK column bytes, one row
after the next. `pk_stride` grows from `{8, 16}` up to `MAX_PK_BYTES`
(see Vocabulary). PK columns are *not* duplicated into payload regions
— the PK region is the PK. Comparison goes through a new column-aware
`compare_pk_bytes` helper that iterates `pk_columns()` with type
dispatch, mirroring `compare_rows`'s payload loop.

The single-PK case stays bit-identical to today: a single PK column has
`pk_stride ∈ {8, 16}`, the PK region is its raw LE bytes, and every
existing u128-keyed fast path (`get_pk -> u128`, `partition_for_key`,
XOR8 keys, `HeapNode.key`, shard `pk_min`/`pk_max` as u128) still
applies. Compound PK adds a parallel slower path; no existing site
becomes slower.

## Why Concat over a digest

The alternative — store an `xxh3(pk_tuple)` digest in the u128 PK slot
and move the real PK column values into payload — keeps the storage
hot path bit-identical but pays for it in three places:

- **Memory.** Real PK column values duplicated (once in the digest's
  source bytes, once in payload). 2× PK overhead for `(U64, U64)`.
- **Seek correctness.** `find_row_index(digest)` returns a candidate
  that must be re-checked against the real PK tuple to handle digest
  collisions. Every seek site needs the tiebreaker.
- **Conceptual debt.** The PK slot is no longer the primary key — it's
  a routing/sort hint. foundations.md §1 already disclaims PK
  uniqueness for *intermediate* batches; the digest design weakens it
  for base tables too.

Concat preserves the existing mental model: "the PK region is the PK".
Sort by PK bytes, then payload. Element identity stays
`(PK, payload)`. No collision tiebreaker. No hidden cost at seek sites.

The cost paid for that clarity is a wider `pk_stride` flowing through
the storage layer. Most of those sites already index by `pk_stride`
(see dev-guide.md's narrow-PK-hash checklist) and just need the value
range expanded.

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
  PK is rejected unless that column carries its own UNIQUE index. (See
  §FK below — this is a correctness wart in the relational model that
  the digest design also has, so we tighten it here regardless.)
- Compound PK on user tables only. System catalog tables stay
  single-PK (their PKs are already pre-packed via `pack_*_pk` helpers
  in `catalog/sys_tables.rs:176-215`; no reason to touch).

Out:

- Multi-column foreign keys (`FOREIGN KEY (a, b) REFERENCES t (x, y)`).
- Migrating existing single-PK tables to compound PK.
- Compound PK with String/Blob columns. (Variable-width PK columns
  would force the PK region to vary per row, not just per schema.
  Out for this plan; revisit if needed.)
- Partial-tuple ON CONFLICT (`ON CONFLICT (a)` when PK is `(a, b)`).

## Vocabulary

- `MAX_PK_COLUMNS = 5`. User-facing `CREATE TABLE` caps PK columns at
  4 (planner-enforced). The internal cap is one higher so secondary
  indexes — modeled as compound-PK tables `(indexed, src_pk_0, …,
  src_pk_{k-1})` in §12 — can hold a 4-PK source plus the indexed
  column without overflowing the schema's `pk_indices` array.
- `pk_stride`: total bytes per row of the PK region. `sum(strides of
  PK columns)`. `u8`-sized, bounded by `MAX_PK_BYTES = 80`.
- `MAX_PK_BYTES = 80`. `MAX_PK_COLUMNS × 16` — sized for the widest
  secondary-index PK (U64 indexed + 4 × U128 source PK). Used by
  `PkBuf` in shard metadata and the wire-side bytes buffer.
- Two orthogonal axes gate the dispatch in the storage layer:
  - **Region width** (`pk_stride <= 16` vs `pk_stride > 16`): governs
    which storage path can keep the PK packed in a u128 word. Order-
    agnostic fast paths — hash-based partition routing, XOR8 build,
    the `extend_pk(u128)` writer — work whenever the region fits in
    16 bytes, regardless of column count, because they treat the
    region as opaque bytes.
  - **Column count** (`pk_count == 1` vs `pk_count >= 2`): governs
    which comparison path is correct. `pk_count == 1` allows a u128
    numerical compare on the PK region (one column's LE bytes sort
    numerically in column order). For `pk_count >= 2`, ordering
    requires column-aware `compare_pk_bytes` — a u128 compare over
    a concatenated tuple puts the trailing column in the high bits
    and reverses priority, so even compound `(U64, U64)` (16-byte
    region) must take the column-aware path for ordered comparisons.
- "narrow region" / "wide region" in the rest of this doc refer to
  the region-width axis only. They never imply anything about
  ordering. Ordered call sites (`find_lower_bound`, shard overlap,
  heap key compare) gate on `pk_count`; opaque-bytes call sites gate
  on `pk_stride`.

---

## The change, layer by layer

### 1. Storage batch (`storage/batch.rs`)

`pk_stride` (already `u8`) widens its value range from `{8, 16}` to
`1..=MAX_PK_BYTES` (80). The type doesn't change. The
`pk_stride == 8 || pk_stride == 16` precondition in `Batch::empty` /
`with_schema` and the analogous gates in `MemBatch` and `MappedShard`
loosen to `1..=MAX_PK_BYTES`. Today's `get_pk`/`extend_pk`/`set_pk_at`
exhaustive `match stride { 8 | 16 => …, _ => panic! }` blocks gain new
arms (or fall through to the bytes variant) so that wider strides do
not panic.

Bytes accessors (`get_pk_bytes` / `extend_pk_bytes` / `set_pk_at_bytes`
on `Batch`; `get_pk_bytes` on `MemBatch` and `MappedShard`) are already
in place from `pk-bytes-accessors.md`. New compound-PK code calls those
directly. Single-PK code keeps calling `get_pk` / `extend_pk` and stays
bit-identical.

`append_row_from_ptable_found` (`storage/batch.rs:953`) currently
takes `pk: u128`. It becomes `pk_bytes: &[u8]` and uses the bytes
extender. `PartitionedTable`'s found-row plumbing
(`storage/partitioned_table.rs`) follows the same shift: the
found-row PK is retained as bytes and surfaced via
`PartitionedTable::found_pk_bytes()`.

`Batch::find_lower_bound(key: u128)` (`storage/batch.rs:827`) stays as a
narrow-PK helper. New `find_lower_bound_bytes(&[u8])` for compound. The
shard-reader equivalent (`shard_reader.rs`) gets the same pair. Both
helpers route through `compare_pk_bytes` from §2.

### 2. PK comparison (`storage/columnar.rs`)

Add:

```rust
pub fn compare_pk_bytes(
    schema: &SchemaDescriptor,
    a: &[u8],
    b: &[u8],
) -> Ordering;
```

Callers pass the contiguous PK-region slices returned by
`Batch::get_pk_bytes` (or any other source that produces concatenated
LE column bytes). The body walks `schema.pk_columns()`, slicing out
each column's `size()` bytes in pk-list order with the same type
dispatch `compare_rows` uses for payload (no null bit — PK columns
are non-nullable). For `pk_count == 1` the function is unused: the
existing u128 path handles single-PK comparison directly.

`compare_pk_bytes` is on the seek-binary-search hot path
(`find_lower_bound_bytes` invokes it log₂(N) times per seek). The
generic loop is acceptable as a fallback but compound PKs cluster
on a few shapes — `(U64, U64)`, `(U64, U32)`, `(U64, U64, U64)`,
`(U64, U64, U64, U64)`. Add a const-generic fast path matched on
those shapes that loads each column as a typed integer and compares
with `<` / `cmp`, mirroring the `scatter_*_<PKS>` strategy in §3b.
Unrecognised shapes fall through to the generic walk.

`compare_rows` itself stays *unchanged*. It still iterates only the
payload columns. The PK comparison happens at the heap level (next
section), and `compare_rows` is invoked on PK tie — same as today.

### 3. Merge heap (`storage/heap.rs`, `storage/merge.rs`)

`HeapNode` stays at 32 B (`key: u128`, `source_idx: usize`, `row:
usize`). The `LoserTree::walk_up` swap cost is on the hot merge path
for every operator; bloating the node would regress *every* query
(single-PK included) to make compound-PK work, which is the wrong
trade. The dispatch handles both narrow and wide PK without changing
the node layout:

- `pk_stride <= 16` (single-PK plus most compound shapes like
  `(U64, U64)`, `(U64, U32, U32)`): the heap fills `node.key` with
  `u128::from_le_bytes(zero_pad_to_16(pk_bytes))`. The full PK is
  inside the node. No indirection.
- `pk_stride > 16` (wide compound, including secondary-index PKs):
  `node.key` is unused as a comparator input. The heap-driver passes
  a `Comparator` capturing `&[SourceCursor]` so the comparison
  closure can pull `source.pk_bytes(row)` directly. Wide PK is rare
  and the indirection cost is dwarfed by the per-row scatter work.

The comparison closure is chosen at the merge entry point from
schema shape:

| `pk_count` | single col type | `pk_stride` | Closure |
|---|---|---|---|
| 1 | unsigned (U64/U128/UUID) | ≤ 16 | `a.key.cmp(&b.key)` — u128 numerical, bit-identical to today |
| 1 | signed (I8/I16/I32/I64) | ≤ 16 | `compare_pk_bytes(schema, ...)` — sign-aware via type dispatch |
| ≥ 2 | mixed | ≤ 16 | `compare_pk_bytes(schema, &a.key.to_le_bytes()[..stride], &b.key.to_le_bytes()[..stride])` |
| ≥ 2 | mixed | > 16 | `compare_pk_bytes(schema, sources[a.source_idx].pk_bytes(a.row), sources[b.source_idx].pk_bytes(b.row))` |

On PK tie, every branch falls through to `compare_rows` as today.

Heap fill: callers compute the u128 packed value (or a fixed
sentinel for wide PK) and pass it through the existing `replace_top`
/ `pop_top` API. The node layout, swap path, and `walk_up`
arithmetic are unchanged.

`pk_cache: [u128; 256]` in `ops/exchange.rs:313` stays a u128 array.
For `pk_stride <= 16` it caches the packed concat; for `pk_stride
> 16` the consumer ignores the cache and fetches bytes from the
source on demand (same indirection as the heap).

### 3b. Scatter routines (`storage/merge.rs`)

`scatter_col_first`, `scatter_multi_source`, and
`scatter_unified_sources_with_weights` dispatch on `writer.pk_stride`
to const-generic helpers (`scatter_col_first_fixed::<PKS>`,
`scatter_mb_pk_wt_nbm::<PKS>`, `scatter_unified_pk_wt_nbm::<PKS>`).
Today's exhaustive `match writer.pk_stride { 8 => ::<8>, 16 => ::<16>,
_ => panic! }` covers the 8/16 strides but panics on anything wider.
Compound PK strides reach 24..=80 — every dispatcher must cover them.

Replace the panicking arm in each match with a `scatter_*_dynamic`
call:

```rust
match writer.pk_stride {
    8  => scatter_*_<8>(...),
    16 => scatter_*_<16>(...),
    s  => scatter_*_dynamic(..., s as usize),
}
```

`scatter_*_dynamic` is a sibling that copies `pk_stride` bytes per
row via `copy_nonoverlapping` against a runtime-sized stride. It
loses the literal-width store optimisation but stays correct. The
dispatcher keeps only the two const arms — single-PK is the
overwhelming majority and benefits from monomorphisation; for
compound strides the runtime-sized memcpy on 8-aligned data is
within noise of a literal-width store and not worth the binary
bloat of six more inner loops.

### 4. Partition routing (`storage/partitioned_table.rs:partition_for_key`)

```rust
pub fn partition_for_key(pk: u128) -> usize;          // narrow
pub fn partition_for_pk_bytes(bytes: &[u8]) -> usize; // wide
```

Narrow path keeps the existing multiplicative hash on the u128 value.
Wide path hashes the bytes via xxh3 → u128, then runs the same
multiplicative hash on the result. Same 256-bucket output; same
hash-route property.

`extract_col_key` and `partition_for_row` in `ops/exchange.rs:30-78`
dispatch on `pk_stride`. The PK-routing fast paths (lines 164-169,
335-381) stay for narrow PK; compound-PK PK-routing repartition goes
through the bytes variant.

### 5. XOR8 (`storage/xor8.rs`, `storage/shard_file.rs:88-110`)

XOR8 today builds on `&[u128]` keys (`xor8::build(&pks: &[u128])`).
Keep that API. For wide PK, the build helper hashes each row's PK
bytes to u128 first:

```rust
// storage/shard_file.rs
fn build_xor8_from_pk_region(pk_ptr, pk_sz, n, stride) -> Option<Filter> {
    let pks: Vec<u128> = (0..n).map(|i| {
        let row = unsafe { from_raw_parts(pk_ptr.add(i*stride), stride) };
        if stride <= 16 {
            // zero-extend (existing behaviour)
            let mut b = [0u8; 16]; b[..stride].copy_from_slice(row);
            u128::from_le_bytes(b)
        } else {
            xxh::checksum128(row)  // collapse to u128 for the filter
        }
    }).collect();
    xor8::build(&pks)
}
```

The probe API takes a u128 key; callers compute it the same way as
build does. False-positive rate unchanged.

### 6. Shard `pk_min` / `pk_max` (`storage/shard_index.rs`, `storage/manifest.rs`)

`ShardEntry`, `LevelGuard`, `PendingShard`:

```rust
pub struct PkBuf {
    pub bytes: [u8; MAX_PK_BYTES],   // 80
    pub len:   u8,                   // == owning table's pk_stride
}

struct ShardEntry {
    ...
    pk_min: PkBuf,   // was: u128
    pk_max: PkBuf,
}
```

`PkBuf` is a plain value type — no generics, no trait bounds. The
`len` field mirrors the table's `pk_stride` so manifest round-trip
preserves the exact key width.

For `pk_count == 1` the existing u128 numerical compare applies (the
PkBuf holds a single column's LE bytes, which sort numerically). For
`pk_count >= 2`, range-prune (`pk_min <= key <= pk_max`) and any
shard-overlap test must go through
`compare_pk_bytes(schema, &pk_min.bytes[..len], &key[..len])` —
byte-wise lex compare of LE bytes is wrong (e.g. `1` = `[0x01, 0, …]`
vs `256` = `[0x00, 0x01, 0, …]`).

Range-disjoint-shard-concat (`plans/6-range-disjoint-shard-concat.md`):
the "is hi_i < lo_j" check uses the same compound-PK comparator. No
design change needed.

**Manifest on-disk format.** `storage/manifest.rs:17-18` stores
`pk_min: u128 LE` + `pk_max: u128 LE` at fixed offsets. Replace with
`pk_min/pk_max` as `(len: u8, bytes: [u8; MAX_PK_BYTES=80])` pairs.
`ENTRY_SIZE_V*` widens accordingly.

### 7. WAL block (`runtime/sal.rs`, `storage/wal.rs`)

The WAL block format declares region 0 as "pk", region 1 as "weight",
region 2 as "null_bmp", regions 3+ as payload, last region is blob.
Per-region size is in the directory.

The "pk" region's per-row stride was implicit (8 or 16). Encoders read
it from the schema. Decoders read it from the directory: `pk_region_size
/ row_count = pk_stride`. Both generalise without a format change.

Touch points:

- `runtime/wire.rs:77, 82-86`: `schema_wal_block_size` computes
  `pk_stride = sum(pk col strides)` via the new `schema.pk_stride()`.
- `runtime/sal.rs:189-200, 458-476`: same `pk_stride` plumbing.
- `storage/wal.rs:30-94`: the `positions[72]` ceiling still suffices
  (3 fixed + 68 payload + blob).

The on-disk WAL block layout is unchanged conceptually; only the
contents of region 0 widen.

### 8. Wire control block (`gnitz-wire/src/lib.rs::control`)

`CONTROL_COLS` (lines 786-796) declares `seek_pk: U128`. For wide PK
that's insufficient. Add `seek_pk_extra: BLOB nullable` carrying
bytes 16.. for wide PK; empty for `pk_stride ≤ 16`. The fast path
(`encode_ctrl_block_template_fast` at the narrow-PK case) stays a
memcpy of the precomputed template plus seven patch fields; wide PK
falls through to `encode_ctrl_block_ipc` which already handles a
blob region for `error_msg`.

The alternative — replacing `seek_pk: U128` with `seek_pk: BLOB` —
is cleaner on paper but breaks the template-copy fast path
(`CTRL_BLOCK_TEMPLATE` in `runtime/wire.rs:284-291`), which dominates
the error-response cost. Reject.

`gnitz-engine/src/runtime/wire.rs`:

- `encode_ctrl_block_ipc` (line 195): add `seek_pk_extra: &[u8]`
  parameter.
- `META_SCHEMA_DESC` (line 44): unchanged.
- `CONTROL_SCHEMA_DESC` (line 52) and `ctrl_region_offset` (line 243):
  pick up the new column automatically — its region offset is computed
  from `CONTROL_COLS`. The `CTRL_BLOCK_SIZE_NO_BLOB` constant grows by
  one region's worth of fixed bytes (0 for a blob region, but `align8`
  may add padding).

### 8b. Wire data plane: `ZSetBatch` PK encoding

`gnitz-core::ZSetBatch` carries client-side PK columns as a `PkColumn`
enum (`gnitz-core/src/protocol/types.rs:104`) with `U64s(Vec<u64>)`
and `U128s(Vec<u128>)` variants. Both encode an ordered single
column. Compound PK extends it:

```rust
pub enum PkColumn {
    U64s(Vec<u64>),
    U128s(Vec<u128>),
    Bytes { stride: u8, buf: Vec<u8> },   // new
}
```

Selection: `for_type` keeps using `U64s`/`U128s` when `pk_count == 1`;
compound schemas select `Bytes { stride: schema.pk_stride(), .. }`.
`push`, `get`, `swap`, `truncate`, `len` get a third arm that slices
by `stride`. The wire encoder forwards the buffer verbatim — the
worker decodes the same `stride` from the table's schema.

`ZSetBatch::new` (`types.rs:189`) replaces `pk_index_single()` with
`pk_indices()[0]` (single-PK) or selects the `Bytes` variant; SQL
INSERT/SEEK paths in `gnitz-sql/src/dml.rs` serialize each extracted
PK column into LE bytes in pk-list order and push to the chosen
variant.

### 9. Catalog: persist the PK column list

`TABLE_TAB`'s `pk_col_idx: u64` field (`catalog/sys_tables.rs:44`) is
the persistence site. Repack it as:

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
tables, which are not persisted via TABLE_TAB — see §12). All writes
use this layout; no legacy discriminator.

Touch points (the catalog inventory):

- `catalog/ddl.rs:119` (`create_table` signature): `pk_col_idx: u32` →
  `pk_cols: &[u32]`. Validation loops over all PK indices for type and
  nullability. Line 182 packs into the u64 with the encoding above.
- `catalog/hooks.rs:110, 130-134, 148, 302`: decode → unpacking helper
  in `sys_tables.rs`; PK-type validation iterates; schema construction
  passes the list.
- `catalog/store.rs:734-745` (`build_schema_from_col_defs`): accept
  `&[u32]` instead of `u32`.
- `catalog/cache.rs:18, 184-209`:
  `pk_col_of: FxHashMap<i64, u32>` → `FxHashMap<i64, ArrayVec<u32, 4>>`
  (or a packed u32 mirroring the on-disk encoding). Cache invalidates
  when *any* PK column index changes.
- `catalog/validation.rs:22`, `catalog/hooks.rs:302` (FK type
  resolution): the FK's `referred_columns` clause names exactly one
  parent column; the catalog records that resolved index. Type
  promotion reads `parent_schema.columns[recorded_idx].type_code` —
  no `pk_index_single()` involved.
- `catalog/utils.rs:97` (`BatchBuilder::physical_col_idx`): walks the
  PK list to compute "number of PK columns with index < col_idx" and
  subtracts. Single-PK is the special case.

System tables stay single-PK; `make_schema(cols, pk_index)` continues
to wrap a single index.

### 10. SQL planner (`gnitz-sql/src/planner.rs`)

CREATE TABLE:

- `:128-145` (table-level PK constraint): drop the `pk_cols.len() != 1`
  rejection. Resolve every named column; enforce no duplicates within
  the PK column list; enforce `len <= 4` (the user-facing cap — the
  internal `MAX_PK_COLUMNS = 5` slot is reserved for index tables in
  §12). Reject mixing inline `PRIMARY KEY` columns with a table-level
  `PRIMARY KEY (...)` (already does for the multi-PK case, generalise
  the message).
- `:147-168` (inline PK): unchanged for single-column.
- `:193-204` (PK type coercion + non-nullable): drop the I*/U*→U64
  coercion entirely. Each PK column retains its declared scalar
  type; `compare_pk_bytes` reads each column's `type_code` and runs
  the right signed/unsigned compare. This also fixes the latent
  negative-sort bug for I8/I16/I32/I64 PKs (which today get
  bit-cast to u64 and sort wrong). Validation still enforces
  `is_pk_eligible(type_code)` and `is_nullable = false` for every
  PK column.
- `:206` (`client.create_table` call): pass `&pk_cols[..]`. Update
  `gnitz-core/src/client.rs:295` signature.
- `:498-510` (`build_projection` view PK): replace
  `pk_index_single()` with `pk_indices()` walk. View output schema
  carries the source's PK column list verbatim.

FK:

- `:78-98` (`resolve_fk_target`): currently returns `(tid, pk_index,
  pk_type)` for the parent's single PK. For compound-PK parents,
  resolve the FK's `referred_columns` clause:
  - If it names exactly one parent column, and that column either (a)
    has its own UNIQUE index or (b) the parent has `pk_count == 1` and
    the named column is the PK, accept and return `(tid, named_idx,
    named_type)`. Otherwise reject with a clear error.
  - This is stricter than today (the digest design's "FK to any PK
    column" is non-standard SQL). Concretely: a single-column FK to a
    compound-PK parent must point at a column the parent has otherwise
    constrained to be unique.

JOIN / GROUP BY reindex:

- `:1270, 1399` (`cb.map_reindex(filtered, single_pk_idx, prog)`):
  unchanged. Multi-column joins remain rejected at planner.rs:779-805,
  so `map_reindex` is still called with a single column.

DML (`gnitz-sql/src/dml.rs`):

- `:60`: ON CONFLICT validation. Allow `Columns(cols)` with `len > 1`
  iff it equals the table's PK column set (any order).
- `:126, :431-454` (`extract_pk_value`): return a stack-resident
  byte buffer of exactly `schema.pk_stride()` bytes, populated by
  serialising each extracted literal as LE bytes in
  `schema.pk_indices()` order (not AST or column-name order). The
  caller splits this buffer into the wire `seek_pk` (first 16 bytes)
  + `seek_pk_extra` (remainder) pair.
- `:262, :307` (`client_side_filter_do_nothing`,
  `client_side_merge_do_update`): the local-dedup set today is
  `HashSet<u128>`. For compound PK this truncates and collides
  silently. Change to `HashSet<ArrayVec<u8, MAX_PK_BYTES>>` (or a
  small wrapper struct keyed on `(stride, [u8; MAX_PK_BYTES])`) so
  the full compound key participates in the hash and eq. Single-PK
  stays correct under the same code path.
- `:579-632, :1212, :1330` (`try_extract_pk_seek`): walk the AND
  chain in any order, bucketing each `col = literal` predicate into
  a per-PK-column slot indexed by the PK column's position within
  the table (not its `pk_indices` ordinal — schema position). Reject
  unless every PK column has exactly one binding. Then serialise the
  literals into the `pk_stride`-byte buffer in `schema.pk_indices()`
  order. AST ordering is irrelevant; binding order is irrelevant;
  the on-disk PK byte layout is the only ground truth. Partial
  bindings (prefix) fall back to the full scan today; a later plan
  can add prefix-seek when leading-prefix range pruning gets
  enabled.
- `:1148-1212` (`try_extract_pk_in`): only single-column IN lists.
  Compound-PK tables → no IN-pushdown for now (correct fallback).

### 11. Python / C API surface

- `gnitz-py/src/lib.rs:153, 282, 353, 403`: `PySchema.pk_index` →
  `PySchema.pk_indices: Vec<u32>`. Existing single-PK code paths read
  `pk_indices[0]` after `assert len == 1`.
- `gnitz-capi/src/lib.rs:246, 466`: `create_table` C ABI accepts
  `pk_count: u32` and `pk_cols: *const u32`. Existing single-column
  callers pass `pk_count = 1, &single_idx`.

### 12. Operators

#### map_reindex

Unchanged for this plan. `map_reindex` (`gnitz-core/src/circuit.rs:230`)
still rewrites the PK slot from a single source column. Multi-column
joins are blocked at the planner (planner.rs:779-805 rejects anything
beyond `col = col`); a compound `map_reindex` would be dead until that
restriction is lifted, so it ships with whatever plan does that.

#### Join

`compiler.rs:598-625` (`merge_schemas_for_join_impl`): output schema
is `[left_PK_cols..., left_payload..., right_payload...]`. Generalise:

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
`SchemaDescriptor::new(&cols[..n], &[0])`. For compound left PK with
`pk_count = k`, it produces `pk_indices = [0, 1, ..., k-1]`.

`ops/join.rs:432-487` (`write_join_row`): currently
`output.extend_pk(left_batch.get_pk(left_row))`. Becomes
`output.extend_pk_bytes(left_batch.get_pk_bytes(left_row))`. Narrow-PK
joins still hit the `extend_pk -> u128` path because `pk_stride <= 16`;
the bytes form is a strict superset. The `left_npc` null-bit offset
calculation is unchanged — left's payload column count doesn't depend
on the PK width.

#### Reduce

`ops/reduce.rs:723, 741-742, 795-810`: `in_pki = pk_index_single()`
becomes `pk_indices`. `group_by_pk` detection fires when
`sorted(group_by_cols) == sorted(input_schema.pk_indices())`. The
fast-path treats the entire PK region as the group key.

`compiler.rs:666-700` (`build_reduce_output_schema`): the
`use_natural_pk` rule today fires when `group_cols.len() == 1` and
that single column's type is `U64`/`U128`/`UUID` (regardless of
whether it is the source's PK — any PK-eligible scalar promotes to
the output's natural PK). Preserve that rule verbatim. *Additionally*
fire when the group-column set, treated as an unordered set, equals
`schema.pk_indices()` — in that case the output's PK region is the
group columns concatenated in `schema.pk_indices()` order, mirroring
the source's PK shape and width. Both rules can be true at once
(single PK column, group by that one column); they produce the same
output. Falling through neither branch keeps the existing synthetic
U128 PK.

`emit_reduce_row` (`reduce.rs:1014-1095`): currently
`output.extend_pk(group_key)`. For natural-PK output the group_key
*is* the row's PK region bytes; for synthetic-PK output it's still
the existing u128 hash. The natural-PK path becomes
`output.extend_pk_bytes(source_mb.get_pk_bytes(exemplar_row))`. Same
narrow-PK fast path applies via the `extend_pk_bytes` impl when
stride ≤ 16.

#### Distinct, Linear, Scan

- Distinct (`ops/distinct.rs`): element identity is `(PK, payload)`.
  PK comparison goes through the heap key path automatically. Compound
  PK works once `compare_rows` is invoked on PK tie (which it already
  is). No code change.
- Linear (`ops/linear.rs:399`): output schema construction follows
  the same pattern as `merge_schemas_for_join_impl` — copy all PK
  columns from input, then payload, then `pk_indices = [0..pk_count]`.
- Scan (`ops/scan.rs`): no PK assumption in the scan body itself.

#### Index / GI / AVI (`ops/index.rs:160-167, 205-217`)

Secondary indexes today hand-pack `(indexed_col_value, source_pk)`
into a u128 composite key — relying on the source PK fitting in 64
bits and the indexed column being promoted to a 64-bit
order-preserving key. With compound source PKs that pack no longer
fits, but the natural answer is to drop the hand-packing entirely
and make the secondary-index table itself a compound-PK table.

`make_gi_schema(source)` and `make_avi_schema(source)` take the
source schema and return:

- GI: PK columns
  `(promoted_indexed: U64, src_pk_col_0: T0, src_pk_col_1: T1, ...)`
  with `pk_indices = [0, 1, ..., src.pk_count]`. The
  `spk_hi` payload column disappears — the source PK columns are
  now first-class PK columns of the index table.
- AVI: same shape, with the for_max negation applied to the first
  column at insert time.

Ingest at `op_integrate_with_indexes` writes each PK column directly
(`extend_pk_bytes` over the concatenated bytes, or per-column
writes), and the storage layer sorts via the compound-PK comparator
from §2.

**Lookup is a prefix scan.** A `WHERE indexed = X` query binds only
the leading PK column — the source PK is what we're trying to find,
not something the caller knows. `seek_by_index` therefore changes
shape:

```rust
// catalog/store.rs
pub fn seek_by_index(&mut self, table_id: i64, col_idx: u32,
                     prefix: &[u8]) -> Result<Option<Batch>, String>;
```

The cursor does `find_lower_bound_bytes(prefix)`, then walks
forward collecting every row whose PK region begins with `prefix`,
stopping at the first row that doesn't. Each collected index row
exposes its trailing source-PK columns; the call then resolves
each via `seek_family` and returns the union as a single Batch.
For UNIQUE indexes the walk stops after one match (uniqueness is
enforced upstream); for non-unique indexes it yields all matches.

The wire/SAL `seek_by_index` carries the prefix bytes via the same
`(seek_pk: U128, seek_pk_extra: BLOB)` pair from §8 — the indexed
column is at most 16 bytes wide, so for typical lookups
`seek_pk_extra` is empty and the narrow fast path applies. The
control-block response gains a `FLAG_HAS_DATA` continuation
streaming the resolved source rows (same pattern as `scan`).

Single-PK sources still benefit: the index table becomes
`pk_count == 2` (`(indexed, source_pk)`) instead of the
`(gc_u64 << 64) | spk_lo` u128 trick — strictly cleaner with no
extra cost since `pk_stride == 16` is on the narrow-region fast
path. The prefix-scan path is the same: prefix = the indexed value
encoded as that column's stride bytes.

### 13. Range / scan operations

`storage/read_cursor.rs:400` (`seek(key: u128)`): keep as the narrow-PK
entry point. Add `seek_bytes(key: &[u8])` for wide PK. Internally
both call into the same merge-tree-positioning logic, parameterised
on the PK comparison.

`storage/read_cursor.rs:313, 459, 520` (`current_key: u128`): keep
this field for narrow PK. For wide-PK cursors, also expose
`current_pk_bytes(&self) -> &[u8]` reading from the active source
without copying. Most consumers (DAG, VM bindings) call
`current_pk_bytes()` and forward to the next operator; only the
existing single-PK call sites (e.g. `reduce.rs:830` trace_out_cursor
seek) still use `current_key` and stay narrow.

---

## Foreign keys to compound parent PKs

The current FK design references one parent column and validates type
match. That's fine when the parent has a single-column PK — the
referenced column *is* the unique constraint. With a compound parent
PK, a single-column FK references a non-unique value, which violates
SQL semantics.

Rule for this plan:

- `FOREIGN KEY (child) REFERENCES parent (col)`: accepted iff
  `parent.pk_count == 1 && col == parent_pk[0]`, OR `col` has its own
  declared UNIQUE index in `parent`.
- Multi-column FK (`FOREIGN KEY (a, b) REFERENCES parent (x, y)`): out
  of scope for this plan. Error message points at this limitation.

This is the same rule both the digest design and concat design would
benefit from; bake it in here.

---

## Testing

### Rust unit tests

- `schema.rs`: `pk_columns()` over schemas with `pk_count` 1..=4,
  including mixed types. Round-trip via `payload_idx` /
  `payload_col_idx`. `pk_stride()` for each.
- `storage/columnar.rs`: `compare_pk_bytes` on every PK type
  combination (U64, U128, UUID, mixed). Including the (U64, U64)
  case — a u128 numerical compare on a (U64, U64) concat sorts by
  the trailing column; this test pins that compound PK goes through
  `compare_pk_bytes`, not the u128 path.
- `storage/batch.rs`: build a compound-PK batch via `with_schema`;
  insert rows with varying PK tuples; verify sort by `(PK bytes,
  payload)`; verify that calling `get_pk` on a wide-region batch
  panics (the `match stride` exhaustive arm).
- `storage/merge.rs`: 3-way N-way merge on a wide-PK schema. Same-PK
  / different-payload rows consolidate correctly. Cover both the
  `pk_count == 1` u128-cast closure and the compound
  `compare_pk_bytes` closure.
- `storage/shard_index.rs`: range prune with `PkBuf` `pk_min`/`pk_max`,
  including the LE-bytes regression (`pk_min` numeric > `pk_max`
  numeric in byte-lex order — should not prune).
- `storage/manifest.rs`: round-trip new format on schemas with
  `pk_stride` covering 8, 16, 24, 80.
- `runtime/sal.rs`: WAL block encode/decode round-trip on a compound-PK
  schema with `pk_stride = 64` (max).
- `runtime/wire.rs`: CONTROL block encode/decode with `seek_pk_extra`
  non-empty.

### Engine integration tests

- `runtime/tests/`: ingest into a compound-PK table; seek by full PK
  tuple; scan; retract; re-insert. Duplicate compound-PK tuple in
  the same batch → conflict.
- Same exercise across narrow and wide compound schemas.

### Python E2E (`tests/test_compound_pk.py`)

- `CREATE TABLE ... PRIMARY KEY (a, b)`; `PRIMARY KEY (a, b, c, d)`;
  inline `PRIMARY KEY` mixed with table-level rejected.
- INSERT same compound tuple twice → conflict (with and without ON
  CONFLICT DO NOTHING / DO UPDATE).
- UPDATE/DELETE WHERE matching full tuple — hits `try_extract_pk_seek`
  fast path. WHERE on a PK prefix falls back to full delta scan, still
  correct.
- JOIN with compound-PK left side. Verify output schema shape; verify
  rows.
- GROUP BY all PK columns → `group_by_pk` fast path. GROUP BY a
  prefix → general reduce path.
- FK to a compound-PK parent on (a) the wrong column → rejected with
  the new error. (b) A column with its own UNIQUE index → accepted.
- Multi-worker (`GNITZ_WORKERS=4`): partition routing by compound PK
  is consistent across workers; same row lands on the same worker
  regardless of which worker received it first. (Regression test for
  the `hash_row_by_columns` invariant in dev-guide.md.)

### Native signed PK

Drop-the-coercion regression: create tables with `I8`/`I16`/`I32`/`I64`
PK columns, insert mixed positive and negative values, scan, verify
ascending order matches signed numerical order (`-128 < -1 < 0 < 127`).
Single-column and compound. Multi-worker — partition routing on a
signed value must hash identically to its `as i64` form regardless
of the worker that produced the row.

---

## Rollout

Order of merging (each step keeps the test suite green; not split
into phases):

1. **`compare_pk_bytes`.** New helper in `columnar.rs`. Unused for
   single-PK schemas.
2. **Widen `pk_stride` domain.** Loosen the `{8, 16}` gate in
   `Batch::empty` / `with_schema`, `MemBatch`, `MappedShard`; extend
   the `match stride` blocks in `get_pk`/`extend_pk`/`set_pk_at` and
   the scatter dispatchers (§3b) to cover wider strides via the
   `scatter_*_dynamic` arm.
3. **Heap dispatch.** `HeapNode` stays 32 B; merge entry points pick
   the comparison closure from `pk_count` / `pk_stride` (table in §3).
   Single-PK path is bit-identical to today; wide-PK path indirects
   via the source cursors only when `pk_stride > 16`.
4. **Catalog encoding.** Pack `pk_count + pk_cols` into TABLE_TAB
   per §9.
5. **SAL/WAL pk_stride.** Region 0 width follows
   `schema.pk_stride()`. For single-PK this is identical to today.
6. **Manifest format.** Write `pk_min`/`pk_max` as `(len, bytes)`
   pairs (§6).
7. **Wire control extension.** Add `seek_pk_extra` (§8).
8. **SQL planner.** Accept `PRIMARY KEY (a, b, ...)`. Drop the
   I*/U*→U64 PK coercion. From this commit onward, CREATE TABLE
   actually allows compound PK and native signed PK types.
9. **DML.** `extract_pk_value` returns a tuple;
   `try_extract_pk_seek` requires full-tuple binding.
10. **Operators.** Schema builders in `compiler.rs` and
    `reduce`/`join` adjustments. Index seek becomes prefix-scan.
11. **FK stricter rule.** Reject single-column FK to a compound-PK
    parent unless the referenced column has its own UNIQUE index.
12. **Python/CAPI.** `pk_index` → `pk_indices`.

Each step is independently mergeable; the feature only becomes
user-visible at step 8.
