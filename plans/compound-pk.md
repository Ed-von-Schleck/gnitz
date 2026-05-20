# Compound primary keys

Enable `PRIMARY KEY (a, b, ...)` in GnitzDB. Today every table is required
to have exactly one PK column.

What remains is every boundary that still calls
`SchemaDescriptor::pk_index_single()` — `ops/index.rs`, the master
index-routing cache + PK-render helper, `gnitz-sql`, `gnitz-core`'s
wire `Schema`, `gnitz-py`, `gnitz-capi`, and the
`catalog/validation.rs` FK type-resolution site. Each
`pk_index_single()` call `assert!`s `pk_count == 1` in release; that
assertion is the canary that fires the day a compound PK reaches an
unmigrated boundary.

**Design choice — Concat.** The PK region in the physical batch / WAL
block / shard file holds the concatenated raw PK column bytes, one row
after the next. PK columns are *not* duplicated into payload regions —
the PK region is the PK. Ordered comparison goes through the
column-aware `compare_pk_bytes` helper, which iterates `pk_columns()`
with type dispatch, mirroring `compare_rows`'s payload loop. The
single-PK case stays on its u128-keyed fast paths (XOR8 keys,
`HeapNode.key`, shard `pk_min`/`pk_max` widened to u128); compound PK
adds a parallel slower path with no cost to single-PK.

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
- Wide-stride (`pk_stride > 16`) compound PK reduce. The reduce
  slow-path cursors (`trace_out`, `trace_in`, AVI/GI) still use
  `ReadCursor::seek(u128)`; lifting them to `seek_bytes` is a
  follow-on. The planner caps reduce/GROUP BY on compound PK at
  `pk_stride ∈ {8, 16}`; point lookups, scans, and DML on wider
  compound PKs are not subject to this cap.

## Vocabulary

- `MAX_PK_COLUMNS = 5`. User-facing `CREATE TABLE` caps PK columns at
  4 (planner-enforced). The internal cap is one higher so secondary
  indexes — modeled as compound-PK tables `(indexed, src_pk_0, …,
  src_pk_{k-1})` in the Operators section — can hold a 4-PK source
  plus the indexed column without overflowing `pk_indices`.
- `pk_stride`: total bytes per row of the PK region, `sum(strides of
  PK columns)`. `u8`-sized, bounded by `MAX_PK_BYTES`.
- `MAX_PK_BYTES = 80` (`MAX_PK_COLUMNS × 16`). Sized for the widest
  secondary-index PK (U64 indexed + 4 × U128 source PK). Bounds the
  `PkBuf` in shard metadata and the wire-side bytes buffer.
- Two orthogonal axes gate the storage dispatch:
  - **Region width** (`pk_stride <= 16` vs `pk_stride > 16`): governs
    which storage path can keep the PK packed in a u128 word. Order-
    agnostic byte paths — e.g. the `extend_pk(u128)` writer — keep the
    region in a u128 whenever it fits in 16 bytes, regardless of column
    count, because they treat the region as opaque bytes.
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

System tables stay single-PK throughout; `make_schema(cols, pk_index)`
continues to wrap a single index. The catalog's in-memory cache and
`BatchBuilder` already carry the full PK column list (`pk_col_of:
FxHashMap<i64, PkColList>`, `BatchBuilder::physical_col_idx` →
`schema.payload_col_idx`). The remaining catalog touchpoints —
`validation.rs:22` and `hooks.rs:297` — are owned by the FK and
Operators/Index sections below.

### 1. SQL planner (`gnitz-sql/src/planner.rs`)

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
  `gnitz-core/src/client.rs` signature (currently
  `pk_col_idx: usize`).
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

Compound-PK admission rule: CREATE TABLE accepts compound PKs at any
storage-supported stride. Reduce/GROUP BY on a compound-PK input is
admitted iff `pk_stride ∈ {8, 16}`; reject wider strides with a clear
error until the reduce slow-path cursors are lifted to `seek_bytes`.
Any GROUP BY shape (full PK set, subset, single PK column, non-PK
columns) is admissible at the supported stride — the reduce slow path
handles compound PK correctly there. Point lookups, scans, and
INSERT/UPDATE/DELETE on compound PKs work at any stride and aren't
subject to this rule.

DML (`gnitz-sql/src/dml.rs`):

- ON CONFLICT validation: allow `Columns(cols)` with `len > 1` iff it
  equals the table's PK column set (any order).
- `extract_pk_value` (currently returns `u128`): return a stack-
  resident byte buffer of exactly `schema.pk_stride()` bytes,
  populated by serialising each extracted literal as LE bytes in
  `schema.pk_indices()` order (not AST or column-name order). The
  caller splits this into the wire `seek_pk` (first 16 bytes) +
  `seek_pk_extra` (remainder) pair.
- `client_side_filter_do_nothing` / `client_side_merge_do_update`: the
  local-dedup set is `HashSet<u128>`. For compound PK that truncates
  and collides silently. Change to a key carrying the full
  `(stride, [u8; MAX_PK_BYTES])` so the whole compound key
  participates in hash and eq. Single-PK stays correct under the same
  code path.
- `try_extract_pk_seek` (currently returns `Option<u128>`): walk the
  AND chain in any order, bucketing each `col = literal` predicate
  into a per-PK-column slot indexed by the PK column's schema
  position. Reject unless every PK column has exactly one binding.
  Then serialise the literals into the `pk_stride`-byte buffer in
  `schema.pk_indices()` order — the on-disk PK byte layout is the
  only ground truth; AST/binding order is irrelevant. Partial
  (prefix) bindings fall back to the full scan.
- `try_extract_pk_in` (currently returns `Option<Vec<u128>>`): only
  single-column IN lists. Compound-PK tables → no IN-pushdown for
  now (correct fallback).

### 2. Python / C API surface

- `gnitz-py/src/lib.rs`: `PySchema.pk_index: usize` →
  `PySchema.pk_indices: Vec<u32>`. Existing single-PK code paths read
  `pk_indices[0]` after `assert len == 1`.
- `gnitz-capi/src/lib.rs`: `gnitz_schema_new(pk_index: u32)` accepts
  `pk_count: u32` and `pk_cols: *const u32`. Existing single-column
  callers pass `pk_count = 1, &single_idx`. Replace the
  `pk_index_single()` reads in the batch/seek helpers with the PK list.

### 3. Operators

#### map_reindex

Unchanged for this plan. `map_reindex`
(`gnitz-core/src/circuit.rs`) still rewrites the PK slot from a single
source column. Multi-column joins are blocked at the planner; a
compound `map_reindex` would be dead until that restriction lifts.

#### Distinct, Linear, Scan

- Distinct (`ops/distinct.rs`): element identity is `(PK, payload)`.
  PK comparison goes through the heap key path automatically; compound
  PK works once `compare_rows` is invoked on PK tie (as it already
  is). No code change.
- Linear (`ops/linear.rs`): already uses `in_schema.pk_indices()`
  when constructing the output schema; no change needed.
- Scan (`ops/scan.rs`): no PK assumption in the scan body.

#### Index / GI / AVI (`ops/index.rs`)

Secondary indexes today hand-pack `(indexed_col_value, source_pk)`
into a u128 composite key (`((gc_u64 as u128) << 64) | source_pk_lo`,
with `spk_hi` as an I64 payload), relying on the source PK fitting in
64 bits and the indexed column promoting to a 64-bit order-preserving
key. With compound source PKs that pack no longer fits; drop the
hand-packing and make the secondary-index table itself a compound-PK
table.

`make_gi_schema` / `make_avi_schema` (today both no-arg, returning a
hardcoded U128-PK + I64-payload shape) return:

- GI: PK columns
  `(promoted_indexed: U64, src_pk_col_0: T0, src_pk_col_1: T1, ...)`
  with `pk_indices = [0, 1, ..., src.pk_count]`. The `spk_hi` payload
  column disappears — the source PK columns are now first-class PK
  columns of the index table. (They take a `source: &SchemaDescriptor`
  argument to copy the source PK columns; today they take none.)
- AVI: same shape, with the for_max negation applied to the first
  column at insert time.

`catalog/utils.rs::make_index_schema` and its caller at
`catalog/hooks.rs:297` (`source_pk_type = owner_schema.columns[
owner_schema.pk_index_single()].type_code`) build the *index table*
schema with the same `(indexed, src_pk)` shape and migrate together —
the index table's PK becomes the concatenated `(indexed, src_pk_cols…)`,
so the constructor takes the source `SchemaDescriptor` directly rather
than a single `source_pk_type: u8`.

Ingest at `op_integrate_with_indexes` writes each PK column directly
(`extend_pk_bytes` over the concatenated bytes), and the storage layer
sorts via `compare_pk_bytes`.

**Master index-routing cache.** `extract_col_key` (returns a `u128`)
and `PartitionRouter` (`ops/exchange.rs`) — the master-side
unique-secondary-index unicast cache keyed
`index_routing: FxHashMap<(u32, u32, u128), u32>` — still funnel the
key through a u128 map key, which truncates a wide source PK. The data
plane already routes any width via `partition_for_pk_bytes`; this cache
is the only PK-routing site that still assumes a ≤16-byte key.
Generalise the cache key to carry the full `(stride, bytes)` PK so a
compound source PK routes without collision.

**PK-render helper.** `format_pk_value(pk: u128, schema)` in
`runtime/master.rs` types-dispatches on the single PK column for the
unique-PK violation message. Compound PK needs a `(pk_bytes,
schema)` variant that walks `pk_columns()` and joins the rendered
values with `, `.

**Lookup is a prefix scan.** A `WHERE indexed = X` query binds only
the leading PK column — the source PK is what we're finding, not
something the caller knows. `seek_by_index` changes shape from
`fn seek_by_index(table_id, col_idx, key: u128) -> ...` to:

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
`((gc_u64 as u128) << 64) | spk_lo` u128 trick — cleaner with no extra
cost since `pk_stride == 16` is on the narrow-region fast path.

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

Catalog touchpoint: `catalog/validation.rs:22` (`validate_fk_column`)
resolves the parent PK type as
`(entry.schema.pk_index_single(), entry.schema.columns[
entry.schema.pk_index_single()].type_code)`. With the stricter rule
above, the resolved column is the FK's recorded `fk_col_idx` (either
the parent's single PK column, or a parent column carrying its own
UNIQUE index); read the type directly from
`parent_schema.columns[fk_col_idx].type_code`.

---

## Testing

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
- GROUP BY all PK columns → `group_by_pk` fast path. GROUP BY a PK
  prefix or a single PK column → reduce slow path, correct at
  `pk_stride ∈ {8, 16}`. `pk_stride > 16` GROUP BY rejected at plan
  build until the cursor-seek lift lands.
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

1. **SQL planner.** Accept `PRIMARY KEY (a, b, ...)`; drop the
   I*/U*→U64 PK coercion; enforce the compound-PK admission rule at
   plan build (reduce/GROUP BY on compound PK requires
   `pk_stride ∈ {8, 16}`). From this commit, CREATE TABLE allows
   compound PK and native signed PK types — the feature becomes
   user-visible here.
2. **DML.** `extract_pk_value` returns a byte tuple;
   `try_extract_pk_seek` requires full-tuple binding.
3. **Operators.** Index/GI/AVI reshape (secondary-index table becomes
   a compound-PK table); index seek becomes prefix-scan; master
   index-routing cache key carries the full PK.
4. **FK stricter rule.** Reject single-column FK to a compound-PK
   parent unless the referenced column has its own UNIQUE index;
   migrate the catalog FK type-resolution sites.
5. **Python/CAPI.** `pk_index` → `pk_indices`.

Each step is independently mergeable; the feature only becomes
user-visible at step 1.
