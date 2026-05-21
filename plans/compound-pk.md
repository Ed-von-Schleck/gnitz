# Compound primary keys

`CREATE TABLE ... PRIMARY KEY (a, b, …)` is now user-visible end to end:
the planner admission rule lands compound PKs, the wire schema codec
encodes the per-column PK position so round-trips preserve declared
order, INSERT/UPDATE/DELETE/SELECT all run, and the master's
PK-existence checks route by raw PK bytes so they hit the same worker
partition as the data writes.

What remains is every **operator and binding surface** that still calls
`SchemaDescriptor::pk_index_single()` — gated behind planner rejections
today, but blocked features tomorrow. Each `pk_index_single()` call
`assert!`s `pk_count == 1` in release; that assertion is the canary that
fires the day a compound PK reaches an unmigrated boundary.

Surfaces still single-PK-only:

- `gnitz-py` / `gnitz-capi`: `PySchema.pk_index: usize` and the four
  internal `pk_index_single()` reads in batch/seek helpers. The bindings
  can't even expose a compound-PK schema to a Python or C caller.
- `ops/index.rs` (GI / AVI) and `catalog/utils.rs::make_index_schema`:
  secondary indexes hand-pack `(indexed, source_pk)` into a u128.
  `client.create_index` rejects compound-PK owners until the index
  table itself becomes a compound-PK table.
- Master index-routing cache (`extract_col_key` →
  `index_routing: FxHashMap<(u32, u32, u128), u32>` in
  `ops/exchange.rs`): the cache key still funnels through a u128, so a
  wide source PK would truncate. The data plane already routes via
  `partition_for_pk_bytes`; this cache is the lone PK-routing site that
  still assumes a ≤16-byte key.
- `catalog/validation.rs:22` (`validate_fk_column`) and
  `catalog/hooks.rs::hook_index_register`'s `source_pk_type` read: both
  resolve "the parent's PK column" assuming there's exactly one.

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

- Compound source PKs of up to 4 columns reach the secondary-index,
  master routing, and FK-validation code paths without truncation or
  assertion.
- FOREIGN KEY: a single-column FK referencing one column of a compound
  PK is accepted iff that column carries its own UNIQUE index (today
  the planner blanket-rejects any FK to a compound-PK parent).
- Python / C bindings expose `pk_indices` instead of a single
  `pk_index`, so callers that build schemas through those bindings can
  declare compound PKs.

Out:

- Multi-column foreign keys (`FOREIGN KEY (a, b) REFERENCES t (x, y)`).
- Migrating existing single-PK tables to compound PK.
- Compound PK with String/Blob columns (variable-width PK columns
  would force the PK region to vary per row, not just per schema).
- Partial-tuple ON CONFLICT (`ON CONFLICT (a)` when PK is `(a, b)`).
- Wide-stride (`pk_stride > 16`) compound PK reduce. The reduce
  slow-path cursors (`trace_out`, `trace_in`, AVI/GI) still use
  `ReadCursor::seek(u128)`; lifting them to `seek_bytes` is a
  separate concern. The planner caps reduce/GROUP BY on compound PK
  at `pk_stride ∈ {8, 16}`; point lookups, scans, and DML on wider
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

## What remains, layer by layer

System tables stay single-PK throughout; `make_schema(cols, pk_index)`
continues to wrap a single index. The catalog's in-memory cache and
`BatchBuilder` already carry the full PK column list.

### Python / C API surface

- `gnitz-py/src/lib.rs`: `PySchema.pk_index: usize` →
  `PySchema.pk_indices: Vec<u32>`. Existing single-PK code paths read
  `pk_indices[0]` after `assert len == 1`.
- `gnitz-capi/src/lib.rs`: `gnitz_schema_new(pk_index: u32)` accepts
  `pk_count: u32` and `pk_cols: *const u32`. Existing single-column
  callers pass `pk_count = 1, &single_idx`. Replace the
  `pk_index_single()` reads in the batch/seek helpers with the PK list.

### Operators

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
`catalog/hooks.rs::hook_index_register` (`source_pk_type = owner_schema
.columns[owner_schema.pk_index_single()].type_code`) build the *index
table* schema with the same `(indexed, src_pk)` shape and migrate
together — the index table's PK becomes the concatenated
`(indexed, src_pk_cols…)`, so the constructor takes the source
`SchemaDescriptor` directly rather than a single `source_pk_type: u8`.

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
  *(Already covered for narrow compounds — `pk_stride ∈ {1,2,4,8,16}`
  — by `tests/test_compound_pk.py`. Wide-stride coverage waits on
  storage-layer reduce-cursor work.)*

### Python E2E (`tests/test_compound_pk.py`)

- FK to a compound-PK parent: (a) wrong column → rejected with the new
  error; (b) a column with its own UNIQUE index → accepted.
- Secondary index on a compound-PK source table: `WHERE indexed = X`
  resolves to the right source rows via the prefix-scan path.

---

## Rollout

Each step is independently mergeable.

1. **Python/CAPI.** `pk_index` → `pk_indices`. Smallest scope; lifts
   the introspection block so downstream tests can read compound-PK
   schemas back. (Extracted to `plans/compound-pk-python-binding.md`.)
2. **Operators.** Index/GI/AVI reshape (secondary-index table becomes
   a compound-PK table); index seek becomes prefix-scan; master
   index-routing cache key carries the full PK.
3. **FK rule.** Accept a single-column FK to a compound-PK parent
   when the referenced column has its own UNIQUE index; migrate
   `catalog/validation.rs:22` and the planner's FK type-resolution
   sites.
