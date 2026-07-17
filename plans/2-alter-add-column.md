# ALTER TABLE ADD COLUMN

Adds the one ALTER operation that widens a table's physical region count. It builds on the
equal-region ALTER machinery already in the engine — `hook_column_alter`,
`DagEngine::swap_table_schema`, the Column-family precheck arm, and the `column_alter`
tick-quiesce — extending each from "descriptor-only swap" to "region-count growth." Because
base-table shards are durable only via checkpoints and the DDL zone forces checkpointing off,
pre-ALTER shards cannot be rewritten at ALTER time; instead every consumer reads a padded view
of the old bytes (missing trailing columns are NULL), materialized once at shard open and
decaying as compaction rewrites shards at full width. One shared batch kernel expresses the
widening; one persisted per-shard region count drives the read-time pad; one ingest-time pad
lifts replayed pre-ALTER pushes. This is a storage-format change: `SHARD_VERSION` is bumped and
old-format shards are invalid (pre-alpha, no migration).

## 1. Semantics

`ALTER TABLE t ADD COLUMN c <type>` appends one **nullable** payload column at
`col_idx = current physical column count` (which **includes** any hidden dropped columns —
count the physical layout, not visible columns). Bundle shape: a single COL_TAB `+1` row for the
new column. Physical effect: the table's schema widens by one trailing payload column; existing
rows read that column as NULL.

Rejected (planner-side, clear errors), on top of the shared ALTER rejects already enforced:

- `ADD COLUMN … NOT NULL` (`is_nullable == 0`) — needs a full-table validation scan; a new
  column over existing rows is unconditionally nullable.
- `ADD COLUMN` with a default, a `column_position` (`FIRST`/`AFTER`), `if_not_exists`, SERIAL,
  or FK metadata.
- ADD COLUMN reaching `MAX_COLUMNS` (`gnitz-wire/src/catalog.rs:262`, `= 65`).
- ADD COLUMN on a table with dependent views (the existing RESTRICT via `dag.get_dep_map()` —
  operator traces would hold re-keyed base rows at the old width). Same guard the equal-region
  column ops use.
- A column name colliding with any **visible** column (collision with a hidden dropped column is
  allowed — hidden names are excluded from client duplicate checks).

## 2. The lift kernel: one operator for null-extension

Schema widening is the lifted linear map "identity ⊕ NULL-extension" — the same operator the
outer join's null-fill already implements. `op_null_extend` (`ops/linear.rs:262-320`,
`op_null_extend(batch: &Batch, in_schema, out_schema) -> Batch`) copies PK/weight regions
verbatim (`:293-294`), copies input payload columns (`:297-302`), leaves appended columns
zero-filled by `Batch::with_schema` (`:304`, `storage/repr/batch.rs:388`), and sets the appended
columns' null bits per row via `merge_null_words(in_null, right_null_bits, in_npc)`
(`:307-312`) — a shift-and-OR `left | (right << left_npc)` (`ops/util.rs:18-24`). Its only
production callers are the outer-join null-fill sites (`plan/view/join.rs:969,1372` via
`CircuitBuilder::null_extend` → `Instr::NullExtend`, `vm/exec.rs:312-317`).

That body is hoisted into

```
storage/repr/lift.rs::lift_batch_to_schema(&Batch, &SchemaDescriptor) -> Batch
```

placed in `storage/repr` beside `merge`/`scatter` because L3 `memtable` cannot call up into
`ops` (verified: nothing under `storage/` imports `crate::ops`, and the layering table forbids
it). `op_null_extend` becomes a thin wrapper over it. The kernel handles the
equal-region-count case as a no-copy fast path (returns the input unchanged). Blob handling needs
no special mode: `share_blob_from` copies the blob bytes (`repr/batch.rs:1357-1365`), so lifted
batches own their blobs regardless of the source's lifetime. Adding a nullable STRING/BLOB column
needs no blob growth for old rows — their value is NULL (null bit set), so the synthesized column
never references the heap. Consumers: the outer join (existing), the ingest pad (§4), and the
memtable / RAM-tier rebuild (§5).

## 3. Storage: reading pre-ALTER bytes

One invariant: **every consumer of a shard row sees the current schema width; missing trailing
columns are NULL.** The heavy read paths get this from the shard's region table and null
representation at open — no per-row branches. The dominant shard consumers bypass per-row
accessors: compaction phase 2 and the cursor drain/materialize path go through `to_unified`
(`shard_reader/access.rs:416-447`), which exposes raw `ColPtr`s; the single-shard scan fast path
bulk-copies regions in `slice_to_owned_batch` (`access.rs:298-399`, `expand_scalar` at
`:335-338`); and the null word is read directly from the region enum by `get_null_word`
(`access.rs:132-137`), which feeds the cursor's per-row output and `compare_rows`' null-first
payload comparator. A design that ORs a mask at one accessor would be bypassed by the others —
pre-ALTER NULLs would materialize as non-NULL zeros, and payload-equality retraction (UPDATE/
DELETE of a padded row against a memtable retraction carrying NULL) would pick the wrong winner
and drive a base-table weight negative.

### 3.1 Padding-aware `MappedShard::open`

The file's payload-region count is **persisted in the shard header**: the reserved zero bytes
`[32,40)` (`storage/repr/layout.rs:13`) record `num_regions` at write time, and `SHARD_VERSION`
(`layout.rs:4`, currently `8`) is bumped to `9` (pre-alpha: old shards without the field are
invalid data). It cannot be derived from geometry — the directory sits *between* the header and
the data regions (`shard_file.rs:533-545`), and 64-byte alignment makes odd/even entry counts
produce identical first-region offsets (`align64(64 + 32N) == 64 + 32(N+1)`). `open`
(`storage/lsm/shard_reader/open.rs:16`) walks the **file's** count for its directory walk,
checksum verification, and region-size validation. File payload regions map to payload columns
`[0, file_npc)`; the file's last region is its blob region regardless of schema width. When
`file_npc < schema_npc` (`schema_npc` from `strides_from_schema`, `repr/batch.rs:189`):

- `col_regions[file_npc..schema_npc]` (`MappedShard.col_regions: Vec<PayloadRegion>`,
  `shard_reader/mod.rs:93`) are synthesized as a **dedicated zero variant** — *not* a bare
  `ScalarRegion::Constant`. A `Constant`'s payload-column arm in `col_ptr_by_logical`
  (`access.rs:203`) dereferences the region's mmap `offset` (`base.add(*offset)`); a synthetic
  region has no file offset, so a bare `Constant` there would read arbitrary mmap bytes as its
  "zero." (The other `Constant` readers — `to_col_ptr` at `access.rs:30-33`, `get_col_ptr` at
  `:156`, `expand_scalar` at `:337` — read the inline `value` and would be fine; only
  `col_ptr_by_logical` forces the issue. Benign either way because the null bit is checked first,
  but not worth the fragility.) The dedicated variant returns a pointer into an **owned, 8-aligned
  16-byte zero buffer** on the `MappedShard`; payload strides are ≤ 16, so one buffer serves every
  synthesized column.
- The null region is replaced by an **owned image** — a `null_image: Option<Box<[u8]>>` field on
  `MappedShard`, populated only when `file_npc < schema_npc` (`count × 8` bytes: the file's null
  words OR'd with the pad mask — bits for payload indices `[file_npc, schema_npc)`, since old
  rows wrote 0 there, which would read as "non-null"). The three null-region readers —
  `get_null_word`, `slice_to_owned_batch`'s `expand_scalar`, `to_unified`'s `to_col_ptr` — branch
  `if let Some(img) = &self.null_image { … } else { … read the mmap region … }`. Three localized
  branches, zero churn to the shared `ScalarRegion` enum, and the image exists only for padded
  shards. (A single-accessor OR mask cannot serve `to_unified`, which hands out a raw pointer, so
  the pre-materialized image is required; making it an owned field rather than an enum variant
  keeps the shared PK/null/payload region enum free of a null-only arm.)

`file_npc > schema_npc` is `InvalidShard` (schemas never narrow). Flush and compaction always
emit full current-schema regions, so padding decays: any rewrite of a padded shard materializes
the NULL column and the pad path stops firing for that data.

A one-shot rewrite of the table's shards at ALTER time was considered and rejected: base tables
are `RecoverySource::SalReplay`, so a mid-generation shard rewrite either violates the
checkpoint-sole-durability contract or pushes O(table) rows through the SAL; compaction publishes
no manifest on the ingest path (`storage/lsm/table/mod.rs:642-654`) and checkpointing is forced
off inside the DDL zone (`master/dispatch.rs:799-801`); and the pad is the fused lazy form of the
same linear operator, materialized incrementally by compaction.

### 3.2 Ingest-time pad

Recovery replays the pre-ALTER SAL tail: those pushes decode self-consistently into old-width
batches (each replayed frame carries its embedded schema block — `ipc::decode_wire` hard-errors on
`FLAG_HAS_DATA` without one, `protocol/wire.rs:1067-1071`), then ingest into a table whose schema is
already the post-ALTER image (the master applies **all** catalog SAL entries pre-fork through
`recover_system_tables_from_sal`, widening the descriptor; workers inherit it via fork and replay
`FLAG_PUSH` entries only, `runtime/bootstrap.rs:217-246`). At the **head** of
`ingest_returning_effective` (`query/dag/ingest.rs:96`) — before `enforce_unique_pk`
(`ingest.rs:295`), which appends rows into an `effective` batch shaped by the table schema
(`ingest.rs:315`), so padding any later is too late — compare the batch's **region count**
(`batch.num_regions`, `repr/batch.rs:319`, always present; robust to `Batch.schema` being an
`Option`, `batch.rs:326`) against the schema's region count. Both `Batch::num_regions` and
`strides_from_schema` (`repr/batch.rs:188`) are `pub(in crate::storage)` and thus invisible from
`query/dag/ingest.rs`; widen them (or add a `pub(crate) fn schema_region_count(&SchemaDescriptor)
-> u8` wrapper in `storage/repr`) so the pad site can read both. Equal → zero cost. Narrower →
`lift_batch_to_schema`. Wider → error. This one site covers every ingest into an ALTER-able table:
such tables are all `unique_pk`, and `ingest_by_ref` (`ingest.rs:72`) delegates through
`ingest_to_family` → `ingest_returning_effective` for `unique_pk` tables (`ingest.rs:78-80`);
non-`unique_pk` relations (system tables, view outputs) bypass this site but are never ADD-COLUMN
targets. Each replayed push is padded independently by its own embedded width, so interleaved
widths (`N`, `N+1`, `N+2` from successive ALTERs in the tail) each lift to the final schema with
no double-pad.

## 4. Engine: grow-path in `hook_column_alter` and `swap_table_schema`

`hook_column_alter` already acts on Column-family pairs (rename / drop-column / drop-not-null) via an
equal-region descriptor swap. ADD COLUMN reaches it as an **unpaired `+1`** on a registered base
table (the shape the equal-region version currently rejects at precheck). Extend it:

- **Shape guard (existing):** the hook still skips any owner whose rows include an unpaired `-1`
  (drop-cascade/replay signature). An unpaired `+1` on a registered base table is now an accepted
  shape (ADD COLUMN append), in addition to pairs.
- **Pair-reassertion / index cascade (existing step 2) is skipped for the unpaired `+1`:** a
  brand-new column_id has no live counterpart row to re-assert pair semantics against and no
  index to cascade-retract, and it is not in the paired-PK set. The step-2 body runs only for the
  pair shapes; do not transcribe its "compare the pair's two rows" logic onto the `+1` (it would
  unwrap a non-existent counterpart and panic).
- **Descriptor rebuild (existing):** `read_column_defs(owner)` →
  `build_schema_from_col_defs(defs, pk, cur.dist_prefix_len()).with_replicated(cur.replicated())`;
  stop if it equals `TableEntry.schema`. For ADD COLUMN the rebuilt descriptor has one more column,
  so `SchemaDescriptor::eq` (`schema.rs:983-992`) differs and the swap proceeds with a **grown**
  region count.
- **Stage all fallible work first (new):** with a grown region count, rebuild every `MemTable` run
  and every `in_memory_l0` RAM-tier run (`storage/lsm/table/mod.rs:241`, a separate overflow tier
  merged into every cursor and point lookup) through `lift_batch_to_schema` into staging vectors, and
  re-open every registered `ShardEntry`'s `MappedShard` under the new schema into a staging set (§3.1;
  handles opened pre-swap have `col_regions`/`null_image` sized at open time and cannot be
  retrofitted — old `Rc`s held by in-flight consumers stay valid and drop naturally). Any failure
  here leaves the old image fully intact, so compensation's descriptor-equality check no-ops instead
  of attempting a forbidden narrowing swap (which would turn a transient I/O error into a fatal
  abort).
- **Then publish infallibly (extend `swap_table_schema`):** `DagEngine::swap_table_schema(tid, desc)`
  updates `TableEntry.schema`, then `StoreHandle::swap_schema(desc)` →
  `PartitionedTable::swap_schema` (its own copy) → each partition `Table::swap_schema`, installing
  the staged `MemTable` runs + schema **together** (no mixed-schema state; per-run PK blooms are
  PK-only and kept), the staged `in_memory_l0` runs, the staged `ShardIndex`/`MappedShard` handles,
  and clearing `cached_full_scan` (`table/mod.rs:234`). The equal-region path (rename/drop-column/
  drop-not-null) is unchanged — it swaps descriptors and returns with no staging. Afterwards
  `debug_assert` that all holders agree on the descriptor including `replicated`/`dist_prefix_len`.

The `column_alter` tick-quiesce already fires for ADD COLUMN (its COL_TAB `+1` targets a registered
table), so no worker applies the swap while an exchange is in flight and no post-ALTER push races the
deferred swap.

## 5. Precheck and client

- **Column precheck arm:** replace the "unpaired `+1` rejected" rule with ADD COLUMN validation. For
  an unpaired `+1` on a registered user table with no dependent views: reject if the column_id is
  already live (closes the concurrent-append race — two connections colliding on the packed
  column_id; the second bundle is rejected). Validate the **prospective** column set — current defs
  from `scan_column_defs` (`registry.rs:28`) plus the new one — through the existing
  `validate_relation_defs` (`catalog/sys_tables.rs:123`: contiguity, count, `MAX_COLUMNS`, PK
  eligibility), then the ADD-specific deltas: `is_nullable == 1`, `is_serial == 0`, `fk_table_id ==
  0`, and the name must not collide with any **visible** column (collision with a hidden column is
  allowed). Retraction payload-equality still applies to any `-1` rows (there are none in an ADD
  bundle).
- **Client method** `alter_add_column(tid, def)` in `gnitz-core/src/client.rs`, next to the other
  `alter_*` methods: builds one COL_TAB `+1` family for the new column at `col_idx = current physical
  count` and ships through `push_ddl`.
- **SQL front end** (`gnitz-sql`): in `plan/alter.rs`, stop rejecting `AlterTableOperation::AddColumn`
  and instead validate it — the column is nullable (reject `NOT NULL`), no `column_position`
  (FIRST/AFTER), no `if_not_exists`, and its type is accepted by the CREATE TABLE type mapper
  `sql_type_to_typecode` (`gnitz-sql/src/types.rs:5`, `DataType -> TypeCode`, which already errors
  on unsupported types) — then call `alter_add_column`. Add `AddColumn`-clause validation next to
  the other per-op guards.

## 6. Tests

Rust (engine):

- `lift_batch_to_schema` round-trip (equal-count no-copy fast path; narrower→padded with correct null
  bits); `op_null_extend` still equivalent after the refactor.
- Padded shard: header region count round-trip; open old-width shard under wider schema; cursor reads
  NULL through `get_null_word`, `to_unified`, and `slice_to_owned_batch`; **UPDATE/DELETE retraction
  of a padded row with NULL new-column payload retracts the right row** (payload-comparator
  regression); compaction of a padded shard with a new-width shard preserves NULLs; `file_npc >
  schema_npc` rejected; a synthetic value column read through `col_ptr_by_logical`/`to_unified`
  returns the owned-zero buffer, not mmap bytes.
- `swap_table_schema` grow-path: memtable runs, `in_memory_l0` runs, `cached_full_scan`,
  `PartitionedTable.schema`, and shard handles all swapped/cleared; agreement assert covers
  `replicated`/`dist_prefix_len`; ADD COLUMN on a **replicated** and on a **CLUSTER BY** table
  preserves routing (dist-bits regression); a fault injected in the staging step leaves the old
  schema image intact and compensates cleanly (stage-then-publish regression).
- Ingest pad keyed on region count with a schemaless batch; interleaved-width replay (`N`/`N+1`)
  each padded independently.

E2E (`crates/gnitz-py/tests/test_alter.py`, `GNITZ_WORKERS=4`):

- ADD COLUMN on a populated multi-worker table that has **overflowed since the last checkpoint**
  (exercises the RAM tier): pre-ALTER rows read NULL, inserts/updates of the new column work,
  UPDATE/DELETE of pre-ALTER rows works, scans and point-seeks merge padded shards with new writes,
  `SELECT *` includes the column.
- ADD COLUMN → kill before checkpoint → recovery replays the pre-ALTER SAL tail through the ingest
  pad; post-restart scans identical.
- Re-ADD of a previously DROP COLUMN'd name works (new physical column, starts NULL; the hidden old
  column is untouched).
- ADD COLUMN of a STRING column: old rows read NULL, new inserts store heap values.
- Concurrent ADD COLUMN from two connections: exactly one succeeds, the loser gets the duplicate-
  column_id rejection.
- ADD COLUMN → concurrent same-table INSERT while a multi-round exchange is in flight on another
  view: no width error, no phantom row after restart (quiesce + ingest-pad regression).
