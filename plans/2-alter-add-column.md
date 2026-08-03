# ALTER TABLE ADD COLUMN

Adds the one ALTER operation that widens a table's physical region count. It extends the
equal-region comparator-swap subsystem — `hook_column_alter`, `DagEngine::swap_table_schema`
(→ `StoreHandle::swap_schema` → `PartitionedTable::swap_schema` → per-partition
`Table::swap_schema`), and the Column-family precheck arm — from "descriptor-only swap" to
"region-count growth."

**Precondition (must land first, described inline here so this plan needs no other document).**
That subsystem is the equal-region comparator swap that DROP COLUMN / DROP NOT NULL introduce; it is
**not yet in the tree** (only ALTER RENAME TABLE/COLUMN has shipped). Its contract, relied on below:
- `hook_column_alter` is a side-effect hook on the `Column` sys-family (added to the
  `SysFamily::Column` dispatch in `catalog/hooks.rs`, beside `apply_col_names_invalidate` /
  `apply_fk_constraints` / `apply_needs_lock` at `hooks.rs:80-83`). Batch-shape-derived (never
  context-derived): per owner tid it acts only on COL `(-1,+1)` **pairs** on a registered base
  table, skips any owner with an unpaired `-1` (drop-cascade/replay signature), rebuilds the
  descriptor from `read_column_defs`, and early-outs via `SchemaDescriptor::eq`.
- `DagEngine::swap_table_schema(tid, desc)` updates `TableEntry.schema` (`query/dag/mod.rs:179`)
  then pushes the descriptor down through `StoreHandle::swap_schema` →
  `PartitionedTable::swap_schema` (its own copy) → each partition `Table::swap_schema`, which
  updates the **three** comparator-schema holders inside one `Table` — `Table.schema`
  (`storage/lsm/table/mod.rs:221`), `Table.memtable.schema` (`storage/lsm/memtable/mod.rs:38`),
  `Table.shard_index.schema` (`storage/lsm/shard_index/mod.rs:169`) — and clears
  `cached_full_scan` (`table/mod.rs:234`). Equal-region ops swap descriptors only (no data motion).
- The Column-family precheck arm (in `precheck_family`, `catalog/write_path.rs:414`, the
  `SysFamily::Column` arm at `:430-433`, running `precheck_retraction_contract`, `:251-346`)
  validates a COL bundle before apply.

Base-table shards are durable only via checkpoints, and a checkpoint never rewrites a pre-existing
shard (§3.1), so pre-ALTER shards cannot be widened at ALTER time; instead every consumer reads a
padded view of the old bytes (missing trailing columns are NULL), computed at shard open and
decaying as compaction rewrites shards at full width. One shared batch kernel expresses the
widening; one persisted per-shard payload-column count drives the read-time pad; one ingest-time pad
lifts replayed pre-ALTER pushes. This is a storage-format change: `SHARD_VERSION` is bumped and
old-format shards are invalid (pre-alpha, no migration).

ADD COLUMN adds two things on top of the subsystem: (a) the precheck arm accepts the unpaired `+1`
shape and `hook_column_alter` accepts it as an append; (b) `swap_table_schema` grows the region
count (lifting resident runs and re-opening shards) instead of only swapping the comparator. It
needs **no** added tick/drive quiesce — §4 shows why the width change is race-free on its own.

## 1. Semantics

`ALTER TABLE t ADD COLUMN c <type>` appends one **nullable** payload column at
`col_idx = current physical column count` (which **includes** any hidden dropped columns —
count the physical layout, not visible columns). Bundle shape: a single COL_TAB `+1` row for the
new column. Physical effect: the table's schema widens by one trailing payload column; existing
rows read that column as NULL.

Rejected (planner-side, clear errors), on top of the shared ALTER rejects the subsystem enforces:

- `ADD COLUMN … NOT NULL` (`is_nullable == 0`) — needs a full-table validation scan; a new
  column over existing rows is unconditionally nullable.
- `ADD COLUMN` with a `DEFAULT`, a SERIAL flag, or FK metadata — gnitz has no column-default
  concept anywhere (`reject_unhonored_column_options`, `gnitz-sql/src/plan/validate.rs:1037-1074`,
  is exhaustive over `ColumnOption` and already rejects `DEFAULT`/SERIAL/etc. for CREATE TABLE);
  reuse it for the ADD COLUMN clause. `column_position` (`FIRST`/`AFTER`) and `if_not_exists` are
  direct fields on sqlparser's `AlterTableOperation::AddColumn` (see §5) and rejected there.
- ADD COLUMN reaching `MAX_COLUMNS` (`gnitz-wire/src/catalog.rs:276`, `= 65`, counting hidden
  columns). Payload columns therefore cap at 64, which is exactly one `u64` null word — the null
  region stays one 8-byte word per row at every width (load-bearing for §2 / §3).
- ADD COLUMN on a table with dependent views (RESTRICT via `dag.get_dep_map()`, `query/dag/meta.rs`)
  — operator traces would hold re-keyed base rows at the old width. Same guard the equal-region
  column ops use. (Secondary indexes are **not** dependent views and do **not** block ADD COLUMN;
  §4 shows they survive it untouched.)
- A column name colliding with any **visible** column (collision with a hidden dropped column is
  allowed — hidden names are excluded from client duplicate checks; the reusable visible-only test
  is `wildcard_name_is_visible`, `gnitz-sql/src/ast_util.rs:701-706`, and the client-side pattern in
  `alter_rename_column`, `gnitz-core/src/client.rs:1371-1379`).

## 2. The lift kernel: one operator for null-extension

Schema widening is the lifted linear map "identity ⊕ NULL-extension" — the same operator the
outer join's null-fill already implements. `op_null_extend` (`ops/linear.rs:260-316`,
`op_null_extend(batch: &Batch, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Batch`)
copies PK/weight regions verbatim (`:291-292`), copies each input payload column by its own stride
via `in_schema.payload_columns()` (`:294-300`), leaves appended columns zero-filled by
`Batch::with_schema` (`storage/repr/batch.rs:388`, which zero-fills unwritten payload — its doc
comment names null-extend as a dependent caller), shares the input blob when
`in_schema.has_german_string()` (`share_blob_from`, `batch.rs:1350-1358`, an owning `Vec` copy),
and sets the appended columns' null bits per row via
`merge_null_words(batch.get_null_word(row), all_payload_null_mask(right_npc), in_npc)`
(`:304-309`) — a shift-and-OR `left | (right << left_npc)`, guarded for `left_npc >= 64`
(`ops/util.rs:18-24`); `all_payload_null_mask(npc)` (`ops/util.rs:31`) returns `u64::MAX` at 64, so
the 64-column boundary is UB-free. Its only production callers are the outer-join null-fill sites
(`gnitz-sql/src/plan/view/join.rs:969,1372` via `CircuitBuilder::null_extend`,
`gnitz-core/src/circuit.rs:464` → `Instr::NullExtend`, executed at `query/vm/exec.rs:301-306`).

That body is hoisted into

```
storage/repr/lift.rs::lift_batch_to_schema(batch: &Batch, out_schema: &SchemaDescriptor) -> Batch
```

declared `pub(super) mod lift;` in `storage/repr/mod.rs` (alphabetically between `heap` and
`merge`), beside `merge`/`scatter` — placed in `storage/repr` because L3 `memtable` cannot call up
into `ops` (verified: nothing under `storage/` imports `crate::ops`; the layering table forbids it).
The two pure null-word bit helpers the kernel uses — `merge_null_words` and `all_payload_null_mask`
(`ops/util.rs:18-31`, no `ops` dependency) — move down into `storage/repr` with the kernel; the
`ops` side (`op_null_extend` and any other caller) imports them from their new home, which is legal
because `ops` depends on `storage`.

**Single-descriptor signature.** The current `op_null_extend` needs `in_schema` only to iterate the
input's payload columns and test `has_german_string()`. In **both** callers the input columns are a
prefix of the output columns — ADD COLUMN appends a trailing column, and the outer-join null-fill's
`out_schema` is literally `in_schema` with the null-fill columns appended (`query/compiler/emit.rs:663-683`;
the RIGHT/FULL `[NULL-A, B]` reorder is a **separate** `map_reindex` instruction *after* null_extend,
`gnitz-sql/src/plan/view/join.rs:960-977,1368-1416`, never inside it) — so `lift_batch_to_schema`
derives the input layout as `out_schema`'s first `batch.num_payload_cols()` payload columns
(`batch.num_payload_cols()`, `batch.rs:494`, `pub`; equals the input width because PK/weight/null
regions never change count on a trailing append). This drops the `in_schema` parameter, is correct
for a **schemaless** batch (`Batch.schema: Option<SchemaDescriptor>`, `batch.rs:326`) — the ingest
pad (§3.2) passes one — and keeps a `debug_assert` that the input is a prefix of `out_schema` for
future callers. The kernel handles the equal-payload-count case as a no-copy fast path (returns the
input unchanged). `op_null_extend` becomes a thin wrapper (`lift_batch_to_schema(batch, out_schema)`),
keeping its three unit tests (`linear.rs:709,793,832`) and the planner-shape test
(`gnitz-sql/tests/planner_join.rs:1113`) valid. Adding a nullable STRING/BLOB column needs no blob
growth for old rows — their value is NULL (null bit set), so the synthesized column never
references the heap.

**Load-bearing invariant.** A nullable ADD COLUMN makes `schema.payload_cmp` become `Generic`
(`compute_payload_cmp` requires *every* payload column non-nullable for the `FixedIntNonnull`
null-skipping fast path, `columnar.rs`), so `SchemaDescriptor::new` in the descriptor rebuild
recomputes it and every comparator (`compare_rows` on the N-way merge, `DirectWriter::write_row`)
takes the null-aware path that reads `get_null_word`. The pad (§3) is therefore consulted on every
path — no fixed-int fast path skips the null word. Consumers of the kernel: the outer join
(existing), the ingest pad (§3.2), and the memtable / RAM-tier rebuild (§4).

## 3. Storage: reading pre-ALTER bytes

One invariant: **every consumer of a shard row sees the current schema width; missing trailing
columns are NULL.** The heavy read paths get this from the shard's stored payload count and a
null-word pad mask computed at open — no per-row branches. There are exactly **three** physical
readers of a shard's null word, and the pad reaches all three:
- `get_null_word` (`shard_reader/access.rs:131-137`) — feeds the cursor's per-row output and
  `compare_rows`' null-first payload comparator.
- `slice_to_owned_batch` (`access.rs:305-406`, the single-shard scan fast path) — bulk-copies the
  null region at `:378`.
- `scatter_unified_pk_wt_nbm` (`storage/repr/scatter.rs:372`, reading the null word at `:387` and
  copying it at `:392`) — the **sole** dereferencer of a `UnifiedSource`'s null field. Compaction
  (`compact/merge.rs:114`) and the cursor drain/materialize path (`read_cursor/output.rs:305`) only
  *build* a shard `UnifiedSource` via `to_unified` (`access.rs:423-454`) and hand it to this shared
  scatter, which is where the raw pointer is read.

A design that fixed up NULLs at one accessor would be bypassed by the others — pre-ALTER NULLs would
materialize as non-NULL zeros; and because compaction then writes the row at full width (§3.1), the
NULL would be **permanently destroyed on disk**, and payload-equality retraction (UPDATE/DELETE of a
padded row against a memtable retraction carrying NULL) would pick the wrong winner and drive a
base-table weight negative.

### 3.1 Padding-aware `MappedShard::open`

`MappedShard::open` (`storage/lsm/shard_reader/open.rs:16`,
`open(path, schema: &SchemaDescriptor, validate_checksums) -> Result<Self, StorageError>`) today
derives **everything** — the region count, the blob-region directory index `nr`, the `.take(nr)`
Raw-size validation, and the `col_regions` walk — from the passed-in `schema`
(`strides_from_schema(schema)`, count = `3 + npc + 1`). After ADD COLUMN the current schema is
**wider** than a pre-ALTER file, so reusing the schema-derived count overruns the file's directory
and mis-maps the blob. The fix records the **file's own** payload-column count and drives the walk
from it.

- **Persist `file_npc` in the shard header.** The header is free constants, not a struct
  (`storage/repr/layout.rs:9-16`): `OFF_MAGIC=0`, `OFF_VERSION=8`, `OFF_ROW_COUNT=16`,
  `OFF_DIR_OFFSET=24`, then reserved bytes `[32,40)`, `OFF_XOR8_OFFSET=40`. Add `OFF_FILE_NPC=32`
  (u64 LE, the writer's `schema.num_payload_cols()`), written in `shard_file.rs` where the header
  is laid out (`:578-584`, currently zero-inits and never touches `[32,40)`). Bump `SHARD_VERSION`
  `8 → 9` (`layout.rs:4`): `[32,40)` is **not** zero-validated on open today (unlike the
  per-directory-entry reserved bytes `[25,32)`, which are), so the version bump — not a zero check —
  is what invalidates old-format files (pre-alpha: no migration). A geometry derivation of the count
  is impossible: the directory sits between the header and the aligned data regions, and `align64`
  rounds an odd `num_regions` up by exactly 32, making `num_regions` and `num_regions+1` produce the
  same first-region offset — so the header field is required, not redundant.

  `open` reads `file_npc`, computes `file_nr = 3 + file_npc` (the file's blob directory index) and
  walks `file_npc + 4` directory entries; validates Raw region sizes against the **file's** strides
  for payload indices `[0, file_npc)`; maps `entries[file_nr]` (the file's last region) as the blob
  region; and sizes `col_regions` to the current `schema_npc = schema.num_payload_cols()`, filling
  `[0, file_npc)` from the file's payload entries and `[file_npc, schema_npc)` synthetically (below).

- **Synthetic value columns** for `col_regions[file_npc..schema_npc]` are a **bare**
  `PayloadRegion::Scalar(ScalarRegion::Constant { value: [0u8; 16] })` (there is no direct `Constant`
  variant on `PayloadRegion` — it nests through `Scalar`; `ScalarRegion` at `mod.rs:30-81`). Their
  value is never semantically read (the null bit is always set), but a valid pointer is handed out.
  **Required one-line fix:** `col_ptr_by_logical` (`access.rs:171-223`, arms at `:194` PK / `:210`
  payload) is the *only* Constant reader that dereferences the file `offset` (`base.add(*offset)`)
  rather than the inline `value`; change its two arms to return `&value` like the other three
  readers already do (`to_col_ptr` `:24-35`, `get_col_ptr` `:140-164`, `expand_scalar` `:342-345`).
  This is provably equivalent for every on-disk Constant (the sole constructor, `open.rs:111-128`,
  copies `data[offset..]` into `value`), and it is **required** for the synthetic column — with the
  old offset read, `offset: 0` would return a pointer to the shard's magic bytes. After the fix
  `ScalarRegion::Constant.offset` has **zero** readers: **drop the field** (aligning `Constant`
  with `WeightRegion::Constant`, `mod.rs:72-74`) and update its construction sites; the one existing
  test that does an 8-aligned `*(ptr as *const i64)` off that pointer (`mod.rs:389`) must switch to
  an unaligned `[u8;8]` read, since `&value` is align-1. `strides ≤ 16` on every payload column, so
  the shared 16-byte inline `value` covers any synthesized column.

- **Null pad via a per-shard mask, not a materialized image.** `MappedShard` gains
  `null_pad_mask: u64`, computed once at open as the payload-index bits `[file_npc, schema_npc)`.
  Build it as `all_payload_null_mask(schema_npc) & !all_payload_null_mask(file_npc)` using the
  helper relocated into `storage/repr` in §2 (it returns `u64::MAX` at 64, so the 64-column cap is
  UB-free — a naive `(1 << schema_npc) - 1` would be UB there). The mask is `0` when `file_npc == schema_npc`
  (every full-width shard). Old rows wrote `0` in those bits (reads as "non-null"); the mask forces
  them to `1` (NULL). The three null readers apply it:
  - `get_null_word(row)` → `raw_word | self.null_pad_mask`.
  - `slice_to_owned_batch` ORs the mask into the copied null region.
  - `to_unified` sets `null_pad_mask` on the returned `UnifiedSource` (add the field at
    `repr/merge.rs:68`; the memtable path `mem_batch_to_unified` sets `0`), and
    `scatter_unified_pk_wt_nbm` reads → ORs the mask → writes the null word instead of a raw copy.
  For a full-width shard the mask is `0`, so every OR is a no-op and normal shards pay nothing. This
  is O(1) per shard: **no** `count × 8`-byte owned buffer is allocated at open — critical because
  §4 re-opens **every** pre-ALTER shard at once, and a materialized null image would spike anonymous
  RAM by 8 bytes × all on-disk rows.

`file_npc > schema_npc` is `InvalidShard` (schemas never narrow — DROP COLUMN hides a column but
keeps the region). Flush and compaction always write the current-schema `file_npc`, so padding
decays: any rewrite of a padded shard materializes the NULL column at full width and the pad stops
firing for that data. `open` is the single shard-open entry point (the sole `Mmap::open_ro` caller,
**synchronous** — no executor yield; matters for §4's atomic swap); its callers are
`compact/merge.rs:35` and `shard_index/mod.rs:77`.

**Why not eager rewrite, and why not compute the pad at read.** A one-shot rewrite of the table's
shards at ALTER time is not merely expensive but impossible in the ingest path: a checkpoint does
**not** rewrite pre-existing shards — the barrier flush writes exactly one new shard from the RAM
tier at the current schema and never touches pre-existing L1+ shards
(`storage/lsm/table/flush.rs`); only threshold-triggered compaction rewrites a shard to full width,
and it publishes **no** manifest on the ingest path (`storage/lsm/table/mod.rs:662-666`, the
checkpoint barrier is the sole manifest publish). Base tables are `RecoverySource::SalReplay`
(`query/dag/mod.rs:123`), so a durable mid-generation rewrite would push O(table) rows through the
SAL. The pad is the fused lazy form of the same linear operator, materialized incrementally by
compaction; with the O(1) mask the ALTER-time reopen is O(shards). Computing the pad at read from
`(file_npc, reader schema)` — making `MappedShard` schema-invariant so it never reopens at ALTER —
is rejected: `get_null_word` is a bare per-row accessor with no schema in hand and ~30 trait
callers, so it would burden the hot path to simplify a rare DDL; baking the O(1) mask at open keeps
the hot path a single unconditional OR.

### 3.2 Ingest-time pad

Recovery replays the pre-ALTER SAL tail: those pushes decode self-consistently into old-width
batches (each replayed frame carries its embedded schema block — `ipc::decode_wire`, which always
passes `schema_hint = None`, hard-errors on `FLAG_HAS_DATA` without one at
`runtime/protocol/wire.rs:1056`), then ingest into a table whose schema is already the post-ALTER
image. The master applies **all** catalog SAL entries pre-fork through
`recover_system_tables_from_sal` (`runtime/bootstrap.rs:427`, before `fork()` at `:531`), widening
the descriptor; workers inherit it via fork and replay `FLAG_PUSH` entries only
(`recover_from_sal`, `bootstrap.rs:207-246`, ingest at `:230`). Because catalog and push entries are
**not** interleaved at replay — every catalog entry is durably applied before any push is replayed
— the schema is already at its **final** width before any push, so a push originally embedded at
width `N`, `N+1`, or `N+2` (successive ALTERs in the tail) each pads exactly once to the final
schema, no double-pad.

Insert the pad at the **head** of `ingest_returning_effective` (`query/dag/ingest.rs:92`), right
after `let schema = entry.schema;` (`:101`) and before the `enforce_unique_pk` branch (`:109`) —
`enforce_unique_pk` (`:314`) appends rows into an `effective` batch shaped by the table schema
(`Batch::with_schema(*schema, …)`, `:334`), so padding any later is too late:

```
if batch.num_payload_cols() < schema.num_payload_cols() {
    batch = lift_batch_to_schema(&batch, &schema);
}
debug_assert!(batch.num_payload_cols() == schema.num_payload_cols());  // never wider (schemas don't narrow)
```

`Batch::num_payload_cols()` (`batch.rs:494`, reads the `num_regions` field, so it is correct even
when `batch.schema == None`) and `SchemaDescriptor::num_payload_cols()` (`schema.rs:503`) are
already `pub` and both types are already in scope in `ingest.rs`, so the pad needs **no** visibility
change and **no** new wrapper. Equal width → no-op (the common path, cost: one comparison).

This one site covers every ingest into an ALTER-able table: such tables are all `unique_pk` (base
tables register `unique_pk = true`, `gnitz-sql/src/plan/ddl.rs:580`), and `ingest_by_ref`
(`ingest.rs:68`) routes `unique_pk` tables through `ingest_to_family → ingest_returning_effective`
(`:75`); live worker push handling (`worker/mod.rs:1122`) and recovery replay (`bootstrap.rs:230`)
both bottom out here. View-output ingests also pass through `ingest_returning_effective`, but view
schemas never grow (`ALTER VIEW … AS` registers a fresh vid and drops the old, never widening a live
view's schema), so the pad's `<` check is a permanent no-op there.

## 4. Engine: grow-path in the swap subsystem

`hook_column_alter` acts on Column-family pairs (rename / drop-column / drop-not-null) via an
equal-region descriptor swap. ADD COLUMN reaches it as an **unpaired `+1`** on a registered base
table. Extend the subsystem:

- **Precheck arm (Column).** The subsystem's arm (`write_path.rs:430-433`) currently lets an
  unpaired `+1` through inertly. Replace that with ADD COLUMN validation for an unpaired `+1` on a
  registered user table with no dependent views: reject if the `column_id` is **already live**
  (`seek_live_sys_row`, the primitive `precheck_retraction_contract` uses at `:292` — closes the
  concurrent-append race, since `pack_col_id = (owner_id << 9) | col_idx`,
  `gnitz-wire/src/catalog.rs:217-229`, is a pure function of the client-read physical count, so two
  connections racing pick the **same** id and the second must lose). Then validate the
  **prospective** column set — current defs from `scan_column_defs` (`catalog/registry.rs:28`; it
  also checks column-index contiguity) plus the new one — through `validate_relation_defs`
  (`catalog/sys_tables.rs:162`: non-empty, PK eligibility, `MAX_COLUMNS`), then the ADD-specific
  deltas: `is_nullable == 1`, `is_serial == 0`, `fk_table_id == 0`, and the name must not collide
  with any **visible** column. Retraction payload-equality still applies to any `-1` rows (none in
  an ADD bundle).

- **`hook_column_alter` shape guard.** The hook still skips any owner with an unpaired `-1`
  (drop-cascade/replay signature). An unpaired `+1` on a registered base table is now an accepted
  shape (ADD COLUMN append). The pair-only step — covering-index cascade / pair re-assertion — is
  **skipped** for the unpaired `+1`: a brand-new `column_id` has no live counterpart to re-assert
  against and no covering index to cascade-retract, and it is not in the paired-PK set; running the
  pair body would unwrap a non-existent counterpart and panic.

- **Descriptor rebuild (grow).** `read_column_defs(owner)` (includes hidden columns) →
  `build_schema_from_col_defs(defs, pk, cur.dist_prefix_len()).with_replicated(cur.replicated())`;
  stop if it equals `TableEntry.schema`. For ADD COLUMN the rebuilt descriptor has one more column,
  so `SchemaDescriptor::eq` (`schema.rs:1047-1054`, compares `num_columns` + `pk_indices` + the
  column array, not `replicated`/`dist_prefix_len`) differs on `num_columns` and the swap proceeds
  with a **grown** region count. Because the added column is nullable, the rebuilt descriptor's
  `payload_cmp` is `Generic` (§2, load-bearing invariant).

- **Grow-path swap (extend `swap_table_schema`).** The swap runs identically on the master (live
  apply) and every worker (`ddl_sync → apply_local`). The master holds no user partitions post-fork,
  so its swap is the descriptor-only `TableEntry.schema` update. On a worker, per partition `Table`:
  1. Lift every `MemTable` run and every `in_memory_l0` RAM-tier run (`storage/lsm/table/mod.rs:241`,
     a separate overflow tier merged into every cursor at `:429-439` and every point lookup at
     `:532-541`) through `lift_batch_to_schema` — pure, infallible compute — into locals.
  2. Re-open every registered `ShardEntry`'s `MappedShard` under the new schema (§3.1) into locals.
     This is the **only** fallible step (a shard-open I/O error). Re-open is required, not
     retrofittable: `MappedShard` has no interior mutability and is shared via `Rc`, so
     `col_regions`/`null_pad_mask` are fixed at open; old `Rc`s held by in-flight consumers stay
     valid and drop naturally. With the §3.1 mask the re-open is O(shards). A re-open error
     `?`-propagates into the worker's existing `DdlSync`-apply **fatal** handler
     (`worker/mod.rs:793-806`) — the same fail-stop every other worker `DdlSync` failure takes,
     and the *correct* response: the catalog SAL is already durably widened when the worker applies,
     so a live worker on the old schema would diverge; `fatal_shutdown` + SalReplay recovery reloads
     the shards under the new schema via the padding-aware `open`. No compensation/rollback is
     possible or wanted here (compensation is a master-side pre-broadcast mechanism; this is
     worker-side post-durable).
  3. Assign atomically: set all **three** comparator-schema holders (`Table.schema`,
     `Table.memtable.schema`, `Table.shard_index.schema`) to the new descriptor, install the lifted
     `MemTable` runs (per-run PK blooms are PK-only, `memtable/runs.rs:62-65`, and kept), the lifted
     `in_memory_l0` runs, and the re-opened `MappedShard` handles, and clear `cached_full_scan`
     (`table/mod.rs:234`). The whole span (steps 1–3) is one **synchronous** stretch (open is
     synchronous, lifting is pure compute) with no `await`, so on the single-threaded worker no push,
     scan, or point-lookup interleaves between the reads and the assignment — no mixed-schema state
     is ever observable. Afterwards `debug_assert` that `TableEntry.schema`, `PartitionedTable.schema`,
     and every partition's three holders agree, including `replicated`/`dist_prefix_len` (which `eq`
     won't check). The equal-region path (rename/drop-column/drop-not-null) is unchanged.

**No added quiesce is needed** (unlike the nullability-flipping ops, which take the tick `Quiesce`
to stop an inline write under the still-`FixedIntNonnull` comparator). ADD COLUMN's
width change is race-free from three shipped properties:
1. The swap is a single synchronous non-yielding span on the single-threaded worker, so it is atomic
   w.r.t. every other operation on that worker.
2. It arrives as an ordinary `DdlSync` applied only at a safe deferral boundary
   (`worker/mod.rs` deferral matrix). While deferred, the worker's `TableEntry.schema` and its store
   are both still old, so an inline old-width push lands in the old-width store and is lifted by the
   later swap; a push after the swap is padded by §3.2. Either way the width is reconciled — and
   because the new column is NULL-valued for every such row (payload-distinct from any real value),
   no old-width write can silently consolidate with a new-width row. There is no comparator-corruption
   path as there is for the nullability flip.
3. Read cursors are per-open CoW snapshots: `create_read_cursor` captures the table's current schema
   **by value** (`Copy`; `ReadCursor.schema`, `read_cursor/mod.rs:104`) alongside `Rc` clones of
   every run and shard, so a cursor opened before the swap reads old schema + old shards
   consistently (the swap installs new `Rc` handles; the old ones stay alive), and one opened after
   reads new + new — no cursor mixes an old schema with new-width shards. The base table is absent
   from every in-flight exchange (dependent views are RESTRICTed), so no old-width base-table batch
   is in flight there.

**Secondary indexes survive ADD COLUMN untouched.** An index's own `SchemaDescriptor`
(`make_index_schema`) folds every index column into `pk_indices`, so it has **zero** payload columns
and no payload comparator to desync; ADD COLUMN appends a trailing column, so existing column
offsets never shift and the index's precomputed `key_spec` span stays valid
(`IndexCircuitEntry.index_schema`/`key_spec`, `query/dag/mod.rs:44,47`). No index schema swap and no
index rebuild are needed. Update the now-stale `key_spec` doc comment ("owner and index schemas are
immutable post-registration — no ALTER exists").

## 5. Client and SQL front end

- **Client method** `alter_add_column(tid, def)` in `gnitz-core/src/client.rs`, next to
  `alter_rename_relation` (`:1297`) / `alter_rename_column` (`:1346`): builds one COL_TAB `+1`
  family for the new column at `col_idx = current physical count` via `append_col_row`
  (`:1885-1906`, which packs `pack_col_id(owner_id, col_idx)` and writes the `gnitz_core::ColumnDef`
  fields — `protocol/types.rs:17-38`) and ships through `push_ddl` (`:768`). The visible-only
  duplicate-name check mirrors `alter_rename_column`'s (`:1371-1379`).
- **SQL front end** (`gnitz-sql`): in `plan/alter.rs`, replace the `AlterTableOperation::AddColumn`
  rejection (`:65-74`) with a dedicated `add_column(...)` handler (mirroring `rename_column`'s
  style): reject `if_not_exists` and `column_position` (direct fields on the sqlparser variant
  `AddColumn { column_keyword, if_not_exists, column_def, column_position }`); run
  `reject_unhonored_column_options` over `column_def.options` (rejects `NOT NULL`, `DEFAULT`,
  SERIAL, FK — nested in `options`, not direct fields); map the type via `sql_type_to_typecode`
  (`gnitz-sql/src/types.rs:5`, which already errors on unsupported types); resolve the target
  through `resolve_base_table` (rejects a view target), run the dependent-view RESTRICT guard, and
  call `alter_add_column`.

## 6. Tests

Rust (engine):

- `lift_batch_to_schema`: equal-payload-count no-copy fast path; narrower→padded with correct null
  bits; **schemaless input batch** padded correctly (prefix-derivation regression); the prefix
  `debug_assert`; `op_null_extend` wrapper still equivalent (its three existing tests unchanged).
- Padded shard: header `file_npc` round-trip; open an old-width shard under a wider schema and assert
  the blob maps from `entries[file_npc+3]` and the synthetic columns land between the file payload
  and the blob; cursor reads NULL through `get_null_word`, `slice_to_owned_batch`, and the
  `to_unified` → `scatter_unified_pk_wt_nbm` path (the scatter-mask regression — the load-bearing
  one); **UPDATE/DELETE retraction of a padded row with NULL new-column payload retracts the right
  row** (payload-comparator regression); compaction of a padded shard with a full-width shard
  preserves NULLs and emits a shard whose `file_npc` equals the new width (pad-decay regression);
  ADD COLUMN reaching the **64th payload column** — the pad mask is
  `u64::MAX & !all_payload_null_mask(file_npc)` with no shift-by-64 UB; `file_npc > schema_npc` rejected; a synthesized value column read through
  `col_ptr_by_logical`/`to_unified` returns the inline zero, not mmap bytes; `null_pad_mask` is `0`
  for a full-width shard (zero-overhead regression); the `mod.rs:389` Constant test reads unaligned.
- `swap_table_schema` grow-path: lifted memtable runs, lifted `in_memory_l0` runs, `cached_full_scan`,
  `PartitionedTable.schema`, all three per-partition schema holders, and shard handles all
  swapped/cleared together; agreement assert covers `replicated`/`dist_prefix_len`; ADD COLUMN on a
  **replicated** and on a **CLUSTER BY** table preserves routing (dist-bits regression); an injected
  shard-reopen error is **fatal** (fail-stop, not silently swallowed), matching the other worker
  `DdlSync` failure sites.
- Secondary index survives ADD COLUMN on its owner: a point/range seek through the index still
  returns the right base rows (now at the wider width) and the new column reads NULL for pre-ALTER
  rows (index-immutability regression).
- Ingest pad keyed on payload-column count with a schemaless batch; interleaved-width replay
  (`N`/`N+1`) all padded to the final schema.

E2E (`crates/gnitz-py/tests/test_alter.py`, `GNITZ_WORKERS=4`):

- ADD COLUMN on a populated multi-worker table that has **overflowed since the last checkpoint**
  (exercises the RAM tier): pre-ALTER rows read NULL, inserts/updates of the new column work,
  UPDATE/DELETE of pre-ALTER rows works, scans and point-seeks merge padded shards with new writes,
  `SELECT *` includes the column.
- ADD COLUMN → kill before checkpoint → recovery replays the pre-ALTER SAL tail through the ingest
  pad; post-restart scans identical.
- ADD COLUMN → force a checkpoint → compaction rewrites padded shards at full width → post-restart
  scans identical and the new column still reads NULL for pre-ALTER rows (pad-decay across restart).
- Re-ADD of a previously DROP COLUMN'd name works (new physical column, starts NULL; the hidden old
  column is untouched).
- ADD COLUMN of a STRING column: old rows read NULL, new inserts store heap values.
- Concurrent ADD COLUMN from two connections: exactly one succeeds, the loser gets the
  duplicate-`column_id` rejection.
- ADD COLUMN → concurrent same-table INSERT while a multi-round exchange is in flight on another
  view, and separately an in-flight ad-hoc query drive on the ALTER-ed table: no width error, no
  phantom row after restart, no stale-schema read (validates the §4 snapshot-isolation + ingest-pad
  safety argument, which stands without any added quiesce).
- ADD COLUMN on a table with a secondary index: the index still seeks correctly and the new column
  is readable.
