# ALTER TABLE / ALTER VIEW

ALTER support built entirely on the ZSet catalog: every ALTER is one `FLAG_DDL_TXN`
bundle of signed sys-table rows flowing through the existing write spine
(`submit → precheck_family → apply_local → fire_hooks → SAL broadcast → worker
ddl_sync → boot replay`). No new wire opcode, no new durability mechanism, no new
quiescing primitive. The engine-side work is: order-independent catalog
application (sign-two-pass folds, reconciling register hooks, retraction
payload-equality), one new Column-family precheck arm and hook, one batch-lift
kernel shared with the outer join, a padding-aware shard open (with a persisted
region count), and id-only relation directories. Everything else is client-side
planning plus small client fixes.

## 1. Supported statements and semantics

| Statement | Bundle shape | Physical effect |
|---|---|---|
| `ALTER TABLE t RENAME TO n` (t may be a table **or** a view) | TABLE_TAB or VIEW_TAB `(-1,+1)` same id, new `name` | none |
| `ALTER TABLE t RENAME COLUMN a TO b` | COL_TAB `(-1,+1)` same column_id, new `name` | none |
| `ALTER TABLE t ADD COLUMN c <type>` (nullable, appended) | COL_TAB `+1` at `col_idx = current count` | schema widens by one trailing payload column |
| `ALTER TABLE t DROP COLUMN c` | COL_TAB `(-1,+1)` same column_id: `is_hidden=1`, `is_nullable=1` (covering secondary indexes are cascade-retracted engine-side) | none (logical drop; layout keeps the column) |
| `ALTER TABLE t ALTER COLUMN c DROP NOT NULL` | COL_TAB `(-1,+1)` same column_id, `is_nullable=1` | none (strides unchanged) |
| `ALTER TABLE t ADD CONSTRAINT [n] UNIQUE (cols)` | maps to the existing CREATE UNIQUE INDEX path (IDX_TAB `+1`, global preflight) | index build |
| `ALTER TABLE t DROP CONSTRAINT n` | maps to the existing DROP INDEX path (IDX_TAB `-1`) | index teardown |
| `ALTER VIEW v AS <query>` | **two sequential DDL zones**: the fully-validated new plan is compiled first, then `drop_view` (old vid + hidden segments), then `create_view_chain` (fresh vid, same name) | new view storage; backfilled by the existing live CREATE-VIEW driver |

`IF EXISTS` is honored. Exactly **one** operation per `ALTER TABLE` statement; a
multi-operation statement (comma-separated ops) is rejected with a clear error.
ALTER inside an open transaction is already rejected by the existing DDL
whitelist in `dispatch.rs::reject_in_transaction` — no change needed.

Postgres accepts `ALTER TABLE <view> RENAME TO`; we do the same because
sqlparser 0.56 has no `ALTER VIEW … RENAME TO` form (its `Statement::AlterView`
is only the `AS <query>` shape). `ALTER VIEW` on a table is rejected;
`ALTER TABLE` column operations on a view are rejected. A rename target with a
schema qualifier naming a different schema than the source's is rejected
(cross-schema moves would break qname bookkeeping and hidden-segment scoping).

`ALTER VIEW` is deliberately **not** atomic across the drop and create. The new
definition is planned and validated client-side *before* the drop is issued, so
the only failures after the drop are engine/I/O errors; on such a failure the
view is gone and the statement errors telling the user to re-issue the CREATE.
Single-bundle atomicity would require in-bundle qname release, mixed
create+drop compensation with per-zone directory tracking, and two-phase
rollback ordering — machinery serving only this statement.

### Rejected operations (planner-side, clear errors)

- PK changes of any kind (add/drop/reorder PK columns). The OPK is the routing,
  sort, filter, and manifest-guard key everywhere; this is a table recreate.
- Column type changes (`SetDataType`) — stride change forces a full rewrite.
- `SET NOT NULL` — needs a full-table validation scan; not built.
- `SET DEFAULT` / `DROP DEFAULT` — column defaults do not exist in gnitz.
- `ADD COLUMN` with `NOT NULL`, a default, or positional placement (`FIRST`/`AFTER`).
- `DROP COLUMN` on a PK column, a SERIAL column, or a column carrying FK
  metadata on either side (`fk_by_child` / `fk_by_parent`); the FK must be
  dropped first.
- `ADD FOREIGN KEY` — needs a child-side validation scan; not built.
- Any ALTER targeting a system relation (`tid < FIRST_USER_TABLE_ID`) or a
  relation in `_system`.
- Any column-set or nullability change (`ADD COLUMN`, `DROP COLUMN`,
  `DROP NOT NULL`) on a table with dependent views → rejected via
  `dag.get_dep_map()`, same RESTRICT style as the existing DROP guard. Renames
  are exempt: views bind sources by id and columns by ordinal, so renames are
  always safe with dependents. (RESTRICTing column changes is what keeps
  operator traces — which hold re-keyed copies of base rows at the old width —
  out of scope: no dependent circuit ever observes a widened base table.)
- `ALTER VIEW` on a view with dependent views → rejected by the existing
  view-dependency drop guard, which the drop zone triggers unchanged. The
  engine has no planner, so it cannot re-plan dependents; RESTRICT is the only
  correct answer.
- Self-referential `ALTER VIEW v AS SELECT … FROM v` → rejected client-side in
  `execute_alter_view` (the old vid appears in the new plan's source set)
  before any zone is issued — otherwise the drop zone would remove the new
  definition's own source.

## 2. Existing machinery this rides on (verified)

- Generic catalog-mutation wire message (`FLAG_DDL_TXN`, `flags.rs:65`) — verb
  semantics are entirely row signs + target families. No wire change.
- Every COL_TAB delta bumps the per-table `schema_version` epoch and drops the
  wire-schema block (`cache.rs:98-111`); every push/scan OK reply stamps the
  version (`worker/reply.rs:134`, `executor.rs:2380`); stale hint-only pushes
  are rejected and the client re-fetches (`connection.rs:577-591`). Client
  staleness after ALTER is handled end to end with zero new code.
- Column caches are rescan-from-ZSet (`fill_column_caches`,
  `registry.rs:82-97`); a rewritten COL_TAB re-derives with no bespoke patching.
- `SysFamily::Column` runs only cache folds today (`hooks.rs:69-73`) and has a
  no-op precheck arm (`write_path.rs:284-292`) — the ALTER hook and precheck
  slot into empty space.
- Boot replay full-scans consolidated sys tables through the same hooks
  (`bootstrap.rs:268-286`); a net `(-1,+1)` survives restart as the new image.
- The DDL zone (catalog write lock + tick gate + committer drain,
  `dispatch.rs:796-801`) is the quiescing window. Worker SAL application is
  FIFO and `DdlSync` is deferred across exchange waits; reply trains are
  materialized `Rc<Batch>`es at enqueue — no lazy cursor crosses the swap.
- Batches, WAL blocks, SAL group frames, and exchange rounds are
  self-describing; the batch pool is schema-agnostic; the committer
  `merge_pool` and master preflight pool already guard with `schema ==` and
  re-allocate on mismatch.
- SAL recovery pass 2 applies only `FLAG_PUSH` entries, all through
  `ingest_returning_effective` (`runtime/bootstrap.rs:217-246`); catalog
  replays before data. Replayed push frames always carry an embedded schema
  block (`ipc::decode_wire` hard-errors on `FLAG_HAS_DATA` without one,
  `protocol/wire.rs:1092-1095`) — asserted at the replay decode site, since
  the ingest pad (§6.3) depends on it. One pad site covers the whole tail.
- xor8 probes, shard-index guard keys, and point-seek routing are PK-region
  only — untouched by payload widening.

## 3. Order-independent catalog application

The batch `fire_hooks` sees is the raw decoded bundle — **not** sorted, and
Stage-A compensation negates weights in place (`write_path.rs:766`), reversing
any client emission order. Same-PK `(-1,+1)` rewrite pairs therefore cannot rely
on row order. The changes below make the apply layer order-independent; the
"edge-triggered hooks are not retract+insert-safe" hazard notes at
`catalog/mod.rs:16-27` and `hooks.rs:14-26` are then **deleted** (the hazard no
longer exists), not patched.

Terminology used throughout: within one family batch, a **pair** is a PK
carrying rows of both signs; an **unpaired** row is one whose PK appears with a
single sign.

### 3.1 Sign-two-pass fold application

`fire_hooks` applies each family's `apply_*` cache folds in two passes over the
batch: all `weight < 0` rows first, then all `weight > 0` rows. This restores
the natural Z-set application order (a retraction always observes the
pre-bundle state for its key) for **every** fold at once, including the
compensation path (the negated pair's `-1` carries the new name, which *is*
the live state at compensation time):

- `apply_entity_caches` (`cache.rs:163-198`): its `-1` arm reads the old name
  from live `entity_by_id` — correct exactly when retractions run first.
- `apply_pk_col_of` (`cache.rs:231-255`) and the set-membership folds
  (`tables_by_schema`, `views_by_schema`): `-1` then `+1` on the same key nets
  to present; the `+1`-first order that nets to absent can no longer occur.

No individual fold changes. (A rename pair spuriously bumps `schema_version`
and clears the lazily-refilled column caches — documented harmless.)

### 3.2 Reconciling register hooks

`hook_table_register` and `hook_view_register` stop being edge-triggered:
storage is applied before hooks fire (`write_path.rs:113-119`), so each hook
gates its side effects on the **net live state** of the row — the reconcile
shape `hook_index_register` already uses (its drop branch acts on what remains
live, `hooks.rs:574-601`). Net liveness is probed with `seek_exact_live`
(`read_cursor/mod.rs:352-363`, the primitive `retract_single_row` uses), which
consolidates and gates on net weight:

- `+1` row: register only if the family store's net row for this PK is live
  **and** `dag.tables` lacks the id (the existing `contains_key` skip at
  `hooks.rs:217`/`:361` is half of this). If the id is already registered,
  verify the live row's immutable fields (schema_id, pk_col_idx, flags) still
  match the registered entry — defense-in-depth against corrupt replayed rows,
  mirroring `hooks.rs:239-248` — and skip.
- `-1` row: run the teardown branch (cascades, `unregister_table`,
  `pending_dir_deletions`, `purge_table_versions`) only if the net row is
  **dead**.

A rename pair nets to "live + registered" for both rows → no side effects fire,
on every path (live apply, worker ddl_sync, boot replay, compensation), in any
row order.

**`hook_cascade_fk` gets the same pair-exclusion.** It fires
`create_fk_indices` for any live `+1` Table row (`hooks.rs:615-624`), and the
FK auto-index name embeds the *current* table name — already updated by the
folds — so a rename pair's `+1` would mint a duplicate `__fk_` index row on
every apply path. Fix twice over: skip PKs that are paired in the batch, and
harden `create_fk_indices` to dedup by `(owner, column list)` via
`indices_by_owner` instead of by name string.

### 3.3 Post-image precheck for Table/View families

The TABLE_TAB/VIEW_TAB arms of `precheck_family` validate the **post-image**
over the touched PKs:

- **Retraction payload-equality** (the Z-set retraction contract: you may only
  retract the element that exists): every `-1` row's full payload must
  byte-equal the current live row for that PK; reject with "catalog changed
  concurrently" otherwise. Without this, a rename composed from a stale client
  snapshot (concurrent renames from two connections) passes the net check yet
  leaves a persistent negative row plus two live head rows — violating base
  positivity. This check applies identically in the Column arm (§3.4).
- Per PK: `net = live_weight + Σ batch weights` must be 0 or 1. (Sys stores
  are not `unique_pk` — `RelationKind::SystemCatalog` — so nothing else
  prevents a duplicate live head row.)
- A PK live both before and after (a pair) may differ from the live sys row
  **only** in `name`. Any other field diff (schema_id, pk_col_idx, flags,
  sql_definition, created_lsn) is rejected.
- Post-image qualified-name injectivity: a new name must not collide with any
  live entity that survives the bundle.
- Rewrites on `tid < FIRST_USER_TABLE_ID` are rejected — closing the
  pre-existing gap that no precheck guards system-range payload ids.
- **The existing drop-integrity guards re-key on net-dead PKs.** Today
  `drop_ids` is collected from raw `weight < 0` rows (`write_path.rs:379-390`)
  and feeds the FK-children guard (`:482-496`) and the view-dependency guard
  (`:507-524`); a rename pair would land its tid in `drop_ids` and be rejected
  as "referenced by FK" / "View dependency". Both guards (and `drop_ids`
  itself) act only on PKs whose bundle **net is dead**, preserving §1's
  rename-with-dependents exemption without weakening DROP protection.

### 3.4 Column-family precheck arm

Replaces the no-op arm. It is skipped when `ctx.in_cascade_drop()` is set;
`cascade_retract_columns` (`hooks.rs:339-351`) is wrapped in
`with_cascade_drop`, like `cascade_retract_indices` already is — without this,
every DROP TABLE/VIEW cascade would trip the new arm. (Worker ddl_sync and
master sys-table SAL recovery bypass precheck entirely — §3.7.)

For submissions where the owner is already registered:

- Owner must be a registered **user table** (not a view, not system-range).
- Owner must have no dependent views (`dag.get_dep_map()`), except when the
  pair is a pure rename.
- Retraction payload-equality for every `-1` row, as in §3.3.
- Unpaired `+1` (ADD COLUMN): reject if the column_id is already live (closes
  the concurrent-append race — two connections colliding on the packed
  column_id; the second bundle is rejected). Validate the **prospective**
  column set — current defs from `scan_column_defs` plus the new one — through
  the existing `validate_relation_defs` (contiguity, count, `MAX_COLUMNS`, PK
  eligibility), then the ALTER-specific deltas: `is_nullable == 1`,
  `is_serial == 0`, `fk_table_id == 0`, and the name must not collide with any
  **visible** column (collision with a hidden column is allowed — hidden names
  are excluded from client duplicate checks).
- Pair: the payload diff vs the live row must be exactly one of (a) `name`
  only — rename; (b) `is_hidden 0→1` plus `is_nullable →1` — drop, and the
  column must not be a PK column, not SERIAL, not FK-involved; (c)
  `is_nullable 0→1` only — DROP NOT NULL, not on a PK column.
- Unpaired `-1` on a registered owner is rejected (physical column removal
  does not exist).

### 3.5 Compensation

`compensate_stage_a` keeps its structure — topo-direction sorts and the binary
drain-vs-discard of `pending_dir_deletions` — with two corrections:

- The homogeneity `debug_assert` (`write_path.rs:740-750`) is relaxed to:
  mixed signs are allowed only as pairs (every negative row's PK also appears
  positively in the same family, and vice versa).
- `is_create` classifies by **unpaired rows only** (`∃` unpaired `+1`). The raw
  any-positive-weight rule would classify a failed DROP COLUMN bundle (COL
  pair + engine-cascaded IDX `-1`) as CREATE: its compensation would then
  DESC-sort (restoring the index before its columns) and, fatally, **drain**
  the forward-queued index-dir deletion — `remove_dir_all` on the directory of
  an index the compensation just restored as live. Under unpaired-row
  classification that bundle is DROP-shaped: ASC restore order and the
  discard path preserve the directory. A pure-pair bundle (rename) classifies
  DROP with an empty queue — a no-op either way.

### 3.6 Pair-aware bundle bookkeeping in `handle_ddl_txn`

`family_pks_by_sign` (`executor.rs:1987`) assumes weight-homogeneous families;
rewrite pairs break it. A view rename would set `view_create`
(`executor.rs:2055-2056`) and **re-backfill a populated exchange/join view** —
Z-set addition, doubling its output weights — and a table rename's `-1` would
enter `dropped_tids` (`executor.rs:2171`), spuriously invalidating unique
filters on a live table. Fix: a pair-aware variant that excludes paired PKs,
used for `new_view_ids` and `dropped_tids`.

Additionally, the post-fsync invalidation lists (`dropped_indices` → unique
filter removal) are derived from the **drained broadcasts** rather than the
client bundle, so engine-cascaded IDX retractions (DROP COLUMN's covering
indexes, §5) release their unique-filter memory instead of leaving dormant
entries.

### 3.7 Master sys-table SAL recovery bypasses precheck

Master system-table SAL recovery currently runs **after** `go_live()`
(`apply_context.rs:29-31`) through `ingest_to_family` → `submit` →
`precheck_family`, with rejections silently swallowed
(`runtime/bootstrap.rs:161` tests only `.is_ok()`). With the new arms this
would re-judge cascaded COL `-1` groups that replay as independent entries
(children-first) while the owner is still registered. Fix: route this recovery
through `apply_local` (no precheck) exactly like worker `ddl_sync` — the rows
are master-validated by definition — and treat a replay `Err` as fatal instead
of uncounted. Hooks (which re-run everywhere) remain the defense-in-depth
layer, per the existing posture (`hooks.rs:508-518`).

## 4. The lift kernel: one operator for null-extension

Schema widening is the lifted linear map "identity ⊕ NULL-extension" — the same
operator the outer join's null-fill already implements: `op_null_extend`
(`ops/linear.rs:262-320`) copies PK/weight regions, copies input payload
columns, leaves appended columns zero-filled by `with_schema`, and ORs the new
columns' null bits per row. That body is hoisted into

```
storage/repr/lift.rs::lift_batch_to_schema(&Batch, &SchemaDescriptor) -> Batch
```

(placed in `storage/repr` beside `merge`/`scatter` because L3 `memtable` cannot
call up into `ops`; `op_null_extend` becomes a thin wrapper over it). The
kernel handles the equal-region-count case as a no-copy fast path. Blob
handling needs no special mode: `share_blob_from` copies the blob bytes
(`repr/batch.rs:1342-1350`), so lifted batches own their blobs regardless of
the source's lifetime. Consumers: the outer join (existing), the ingest pad
(§6.3), and the memtable/RAM-tier rebuild (§5).

## 5. Engine: `hook_column_alter`

New side-effect hook on the `Column` family, run after the cache folds. Its
trigger is **batch-shape-derived, never context-derived** — cascaded
whole-table COL retractions (DROP TABLE/VIEW) reach this hook on workers and at
replay with no cascade flag available, so the shape is the only reliable
signal. Per distinct owner tid in the batch:

1. Skip unless the owner is registered in `dag.tables` **and** is a base table
   (`entry.kind`). Skip any owner whose batch rows include an **unpaired
   `-1`** — that shape is the drop-cascade/replay signature (physical column
   removal does not exist); acting on it would rebuild a schema from zero
   columns and panic. The hook acts only on pairs and unpaired `+1` appends.
   (Live CREATE TABLE bundles apply COL_TAB before TABLE_TAB, so the owner is
   unregistered and skipped; at boot replay Table registers before Column —
   `bootstrap.rs:268-286` — and step 3's equality check makes the hook a
   no-op, the register hook having already scanned the fully-loaded COL_TAB.)
2. Re-assert the §3.4 pair semantics against the live rows for the shapes the
   hook acts on (defense-in-depth on the precheck-skipping paths) — asserting,
   never erroring, on shapes it skips. If the batch flips `is_hidden 0→1` for
   a column, cascade-retract every secondary index covering it —
   `cascade_retract_indices`-style under `with_cascade_drop`. This keeps
   "cascade is the applier's declared reaction" (`write_path.rs:14-20`), works
   for raw capi bundles, is idempotent on worker re-derivation (reads live
   rows; the children-first SAL order means the worker's own cascade finds
   them already retracted), and bumps the client `index_version` epoch via
   `apply_index_by_id` so cached `IndexMeta` re-fetches.
3. Rebuild the descriptor: `read_column_defs(owner)` →
   `build_schema_from_col_defs(defs, pk, cur.dist_prefix_len())
   .with_replicated(cur.replicated())` — the distribution prefix and
   replicated flag live on TABLE_TAB/the registered schema, **not** COL_TAB,
   and `build_schema_from_col_defs` alone would silently drop them (wrong-
   worker routing on CLUSTER BY tables, misclassified replicated stores). If
   the result equals the current `TableEntry.schema`, stop (idempotence under
   replay and re-application). `SchemaDescriptor::eq` compares full column
   bytes including nullability (`schema.rs:982-991`) but **not**
   `replicated`/`dist_prefix_len`, so renames and pure hidden-flag flips end
   here, while `DROP COLUMN` (which sets `is_nullable=1`) and `DROP NOT NULL`
   proceed — with unchanged region count.
4. **Stage all fallible work first**: with a grown region count, rebuild every
   `MemTable` run and every `in_memory_l0` RAM-tier run
   (`table/mod.rs:236-241` — a separate overflow tier merged into every
   cursor and point lookup) through `lift_batch_to_schema` into staging
   vectors, and re-open every registered `ShardEntry`'s `MappedShard` under
   the new schema into a staging set (§6.2; handles opened pre-swap have
   `col_regions` sized at open time and cannot be retrofitted — old `Rc`s held
   by in-flight consumers stay valid and drop naturally). Any failure here
   leaves the old image fully intact, so compensation's step-3 equality check
   no-ops instead of attempting a forbidden narrowing swap (which would turn a
   transient I/O error into a fatal abort).
5. **Then publish infallibly** through one push-down entry point:
   `DagEngine::swap_table_schema(tid, desc)` updates `TableEntry.schema` and
   calls `StoreHandle::swap_schema(desc)` → `PartitionedTable` (updating **its
   own copy** — it routes with `self.schema.partition_for_pk` and slices
   scatter sub-batches with it, `partitioned_table.rs:198-274`) → each
   partition `Table::swap_schema(desc)` — the table's copy, the staged
   `MemTable` runs + schema (replaced together, no mixed-schema state; per-run
   PK blooms are PK-only and kept), the staged `ShardIndex` handles, and
   clearing `cached_full_scan` (`table/mod.rs:234`), which would otherwise
   serve a cached old-width batch. Equal-region-count changes (nullability
   only) swap descriptors and return — strides identical, no data motion.
   Afterwards `debug_assert` that all holders agree on the descriptor
   **including `replicated` and `dist_prefix_len`** (which `eq` won't check).
6. `debug_assert!(dependent views of owner are empty)` — the RESTRICT
   invariant from §1 made load-bearing, and the reason no plan-cache
   invalidation is needed: no compiled circuit scans this table. The
   `vm/exec.rs:94` width assert stays as the backstop.

The hook runs identically on the master (live apply), on every worker
(`ddl_sync` → `apply_local`), and at boot replay (no-op by step 3). Post-fork
the master holds no user partitions; replicated tables get the swap on every
worker via ddl_sync. The DDL zone guarantees no tick or scan is in flight
during the swap.

Why swap-in-place instead of re-registering the storage handle:
`PartitionedTable::new` opens from manifests only; the DDL zone does not
checkpoint user memtables (the committer barrier flushes WAL, not shards), so
re-registration of a populated table would drop every unflushed row. The
`partition_lsn.rs:87,135` handle-replacement sites operate on empty stores by
documented contract and are not a precedent.

## 6. Storage: reading pre-ALTER bytes

One invariant: **every consumer of a batch or shard row sees the current schema
width; missing trailing columns are NULL.** The heavy read paths get this from
the shard's region table and null representation at open — no per-row branches.

### 6.1 Why padding lives in the region table

The dominant shard consumers bypass per-row accessors: compaction phase 2 and
the cursor drain/materialize path go through `to_unified`
(`shard_reader/access.rs:380-440`), which exposes raw `ColPtr`s; the
single-shard scan fast path bulk-copies regions in `slice_to_owned_batch`
(`access.rs:262-363`); and the null word is additionally read directly from
the region enum by `get_null_word` (`access.rs:111-116`), which feeds the
cursor's per-row output and `compare_rows`' null-first payload comparator. A
design that ORs a mask at one accessor would be bypassed by the others —
pre-ALTER NULLs would materialize as non-NULL zeros, and payload-equality
retraction (UPDATE/DELETE of a padded row against a memtable retraction
carrying NULL) would pick the wrong winner.

### 6.2 Padding-aware `MappedShard::open`

The file's payload-region count is **persisted in the shard header**: the
reserved zero bytes `[32,40)` (`layout.rs:13`) record `num_regions` at write
time, and `SHARD_VERSION` is bumped (pre-alpha: old shards without the field
are invalid data, consistent with §7's posture). It cannot be derived from
geometry — the directory sits *between* the header and the data regions
(`shard_file.rs:533-545`), and 64-byte alignment makes odd/even entry counts
produce identical first-region offsets. `open`'s directory walk, checksum
verification, and region-size validation loops all iterate the **file's**
count. File payload regions map to payload columns `[0, file_npc)`; the file's
last region is its blob region regardless of schema width. When
`file_npc < schema_npc`:

- `col_regions[file_npc..schema_npc]` are synthesized constant-zero regions.
  The pointer-returning accessor arm (`col_ptr_by_logical`, `access.rs:182`)
  returns a pointer into an **owned, 8-aligned 16-byte zero buffer** on the
  `MappedShard` — the existing Constant arm points into the mmap at the
  region's file offset, which a synthetic region does not have. Payload
  strides are ≤ 16, so one buffer serves every synthesized column.
- The null region's **representation is replaced**, not repointed: a new
  owned-image variant of the null region type, materialized once at open
  (`count × 8` bytes: the file's null words OR'd with the pad mask — bits for
  payload indices `[file_npc, schema_npc)`, since old rows wrote 0 there,
  which would read as "non-null"). Making it a distinct variant forces every
  consumer — `get_null_word`, `expand_scalar` in `slice_to_owned_batch`,
  `to_unified` — through the image at compile time, with zero per-read cost.

`file_npc > schema_npc` is `InvalidShard` (schemas never narrow). Flush and
compaction always emit full current-schema regions, so padding decays: any
rewrite of a padded shard materializes the NULL column and the pad path stops
firing for that data.

A one-shot rewrite of the table's shards at ALTER time was considered and
rejected: base tables are `RecoverySource::SalReplay`, so a mid-generation
shard rewrite either violates the checkpoint-sole-durability contract or
pushes O(table) rows through the SAL; checkpointing is forced off inside the
DDL zone (`dispatch.rs:799-801`); and the pad is the fused lazy form of the
same linear operator, materialized incrementally by compaction.

### 6.3 Ingest-time pad

Recovery replays the pre-ALTER SAL tail: those pushes decode self-consistently
into old-width batches (each replayed frame carries its embedded schema block —
asserted at the decode site, §2), then ingest into a table whose schema is
already the post-ALTER image. At the **head** of `ingest_returning_effective`
(`dag/ingest.rs:96`) — before `enforce_unique_pk`, which appends rows from the
incoming batch into an `effective` batch shaped by the table schema
(`ingest.rs:315-362`), so padding any later would be too late — compare the
batch's **region count** (`batch.num_regions`, always present; robust to
`Batch.schema` being `Option`) against `strides_from_schema(entry.schema)`.
Equal → zero cost. Narrower → `lift_batch_to_schema`. Wider → error. Both live
pushes and SAL replay flow through this one site; the only other ingest
entrance (`ingest_by_ref`) serves system tables and view backfill, both
un-ALTERable.

## 7. Id-only relation directories

`table_dir` / `view_dir` (`catalog/utils.rs:49-56`) embed the relation **name**
(`{schema}/{name}_{tid}`), recomputed from the live catalog at every
registration — under RENAME the next boot would recompute a new-name path and
orphan the data. Formats change to id-only: `{base}/{schema_name}/t_{tid}`,
`v_{vid}`, index dirs `idx_{index_id}` (nesting unchanged). Renames then never
touch the filesystem. The drop path, orphan GC (`write_path.rs:625-632`), the
`is_table_dir_name` recognizer, and boot open all consume the same recomputed
string and keep working. Pre-alpha: no migration; stale name-format dirs in dev
data directories are invalid data.

The persisted TABLE_TAB `directory` / VIEW_TAB `cache_directory` payload
columns are dead — the engine defines no accessor and always recomputes — and
are **removed** from `gnitz-wire/src/catalog.rs`, the client writers, and the
bootstrap seed rows, rather than left as a lie.

## 8. ALTER VIEW mechanics

Client-side `execute_alter_view`, in strict order:

1. Resolve the old vid by name; honor `IF EXISTS`.
2. Compile the new query exactly as `execute_create_view` does (CTE inlining,
   derived-table chain, shape dispatch), allocating a **fresh vid** and fresh
   hidden-segment vids. Ids are never reused in gnitz; keeping the old vid
   would need a new engine re-register path for zero benefit — every consumer
   resolves name→id per statement, and dependent views (the only id-pinned
   consumers) are RESTRICTed.
3. Reject self-reference: the old vid must not appear in the new plan's source
   set.
4. Zone 1: `drop_view` (old vid + its hidden segments — the hidden-segment
   prefix embeds the owner **vid**, so this is rename-proof; the engine's
   `hook_view_register` drop branch cascades circuit/dep/col rows; the view
   dir — which nests the operator scratch dirs, `emit.rs:64-65` — is queued
   for recursive deletion). The existing view-dependency guard rejects the
   drop if dependents exist, before anything is torn down.
5. Zone 2: `create_view_chain` with the pre-compiled plan under the same name.
   The head view carries `sql_definition` of the re-rendered
   `CREATE VIEW`-shaped statement. `handle_ddl_txn` backfills the new vids
   through `order_by_intra_bundle_deps` + `fan_out_backfill`/inline backfill
   exactly as live CREATE VIEW does.

## 9. SQL front end and client

### 9.1 Dispatch and validation (`gnitz-sql`)

- `dispatch.rs`: arms for `Statement::AlterTable` and `Statement::AlterView`.
  `AlterTable` with `operations.len() != 1` → error. Delegate to
  `plan/alter.rs::execute_alter_table` /
  `plan/view/dispatch.rs::execute_alter_view`.
- `plan/validate.rs`: `reject_unhonored_alter_table_clauses` (rejects `only`,
  `location`, `on_cluster`, and every `AlterTableOperation` variant outside the
  supported set) and `reject_unhonored_alter_view_clauses` (rejects `columns`,
  `with_options`), following the per-verb convention.
- `plan/alter.rs` (new): resolves the target from the catalog snapshot,
  performs all client-visible validation (existence, kind, PK/SERIAL/FK
  guards, visible-name collisions, cross-schema rename rejection,
  supported-type checks for ADD COLUMN), computes the row diffs, and calls the
  new `gnitz-core` client methods. `ADD CONSTRAINT UNIQUE` / `DROP CONSTRAINT`
  re-enter the existing `execute_create_index` / drop-index planner paths.
- Statement-local catalog reads performed **after** composing a write use
  direct `session.scan`, never `scan_catalog` — the per-statement snapshot
  memoizes each sys table once and would return the pre-write image
  (`client.rs:300-326`; `drop_schema`'s cascade at `client.rs:858` is the
  existing pattern).

### 9.2 Client methods (`gnitz-core/src/client.rs`)

Next to `create_table`/`drop_table`: `alter_rename_relation(id, kind,
new_name)`, `alter_rename_column(tid, col_idx, new_name)`,
`alter_add_column(tid, def)`, `alter_drop_column(tid, col_idx)`,
`alter_drop_not_null(tid, col_idx)`. Each builds the family batches from
section 1 (rewrite pairs = two rows, same PK, the live row's exact payload at
`-1` — §3.3's payload-equality rule — and the new payload at `+1`, in one
family batch; no ordering contract needed, §3.1) and ships through the
existing `push_ddl` choke point.

### 9.3 Hidden-column correctness fixes (required before DROP COLUMN ships)

- `dml/insert.rs:186` — the no-column-list arity check counts
  `schema.columns.len() - is_serial`; must exclude hidden columns.
- `dml/insert.rs:481-486` — the explicit-column-list expected set filters only
  `is_serial`; must also filter hidden.
- **Positional remap**: the VALUES loop maps user values by physical column
  index (`row[ci]` / `extract_pk_value` indexing the user row physically,
  `dml/insert.rs:221-234`); once a *middle* column is hidden, every later user
  value would shift into the wrong column. Build an explicit visible-position
  → physical-ci map (skipping hidden and SERIAL) and drive both PK extraction
  and the payload loop from it.
- INSERT row encoding writes NULL (zero value + null bit set) for hidden
  columns — the physical layout still contains them.
- The four "base-table columns are never hidden" invariant comments
  (`protocol/types.rs:35,188`, `plan/validate.rs:17`, `dml/overlay.rs:87`) and
  `bind/resolve.rs:60-62` are rewritten to the new invariant: *base-table
  hidden columns exist only via DROP COLUMN, are always nullable, and are
  excluded from all name-facing surfaces*. The code beneath them
  (`visible_columns()`, `find_unique_column`, wildcard/JOIN combination)
  already skips hidden correctly — verified by the new tests, not changed.
  UPDATE is already safe (`resolve_set_target` → `find_unique_column` skips
  hidden; carry-through preserves hidden values).

### 9.4 Transaction schema pin

An open client transaction buffers batches shaped by the schema at statement
time; a concurrent ALTER makes a later statement's `overlay_batch` walk the new
schema's `payload_columns()` over the old-shaped buffered batch
(`dml/overlay.rs:91-110`) — a misaligned client-side read that no commit-time
check can catch. Reply-derived version tracking cannot close it either: a
buffered INSERT makes **no server round-trip** (`client.rs:387-390`), so the
first reply for the table can arrive already post-ALTER and pin the wrong
version.

Fix: pin the **shape**. `TxnBuffer` records, per table, an owned `Schema`
captured at the transaction's first contact with that table, at **two**
capture/compare sites: (i) the binder's table resolution
(`bind/resolve.rs:170-204`) — pure SELECTs never consult `TxnBuffer`, so the
read side must pin at bind time; (ii) `TxnBuffer::append`
(`client.rs:1464`) — covering capi/Python raw pushes that bypass SQL bind.
Every subsequent capture at either site compares against the pin with
`types_match`; on mismatch the transaction fails with "schema changed
concurrently; transaction rolled back". `types_match` compares column count,
types, and nullability, so renames pass (layout unchanged, names re-resolve per
statement) while width or nullability divergence aborts before any misaligned
read; no width-preserving type change exists in the supported subset. The
commit path needs no extra check: commit ships per-family schema blocks that
the server validates.

### 9.5 Result surface

`SqlResult::Altered { object: String, name: String }` in
`gnitz-sql/src/lib.rs`; matching dict arm in `gnitz-py`'s `execute_sql`. capi
gets ALTER through `gnitz_execute_sql` with no new entry points. Caller-owned
schema handles (capi `GnitzSchema`, Python `Struct._schema`) remain static
declarations backed by the reactive `types_match`/server-validation net; their
docs gain an "ALTER requires rebuilding this" note.

## 10. Tests

Rust (`crates/gnitz-sql/tests/planner_alter.rs`): parse/validate/reject matrix —
every supported statement plans; every rejected operation (incl. multi-op,
system relations, cross-schema rename, self-referential ALTER VIEW) errors with
the documented message; bundle shapes (row signs, families, pair payload diffs,
`-1` payload = live-row payload) asserted against the built batches.

Rust (engine, alongside existing catalog/storage tests):

- Reconciling hooks: a rename pair fires no cascade and no dir deletion, in
  both row orders, on live apply and via `replay_catalog`; a compensated
  rename restores `entity_by_qname`/`entity_by_id`/`pk_col_of`/
  `tables_by_schema` exactly; renaming an FK-child table mints **no** second
  `__fk_` index row (pair-aware `hook_cascade_fk` + `(owner, cols)` dedup).
- Sign-two-pass folds: same-PK pair applied `+1`-first at the batch level
  still yields correct caches.
- Post-image precheck: stale-snapshot rename (`-1` payload ≠ live row) is
  rejected "catalog changed concurrently"; duplicate live head row rejected;
  rename of an FK **parent** and of a table **with a dependent view** both
  pass (net-dead guard re-keying), while genuine drops of each are still
  rejected.
- Column precheck + `hook_column_alter` shape-guard: DROP TABLE and DROP VIEW
  of relations **with columns** work end to end on master, workers, and
  through SAL recovery (regression for the cascade-shape panic); concurrent
  ADD COLUMN duplicate column_id rejected; prospective
  `validate_relation_defs` wiring.
- §3.7: master sys-table SAL recovery applies a DROP-cascade sequence without
  precheck rejections; an injected replay error is fatal, not swallowed.
- Pair-aware `handle_ddl_txn`: a view rename bundle triggers **no** backfill;
  a table rename does not enter `dropped_tids`; a DROP COLUMN's cascaded
  index retraction releases its unique filter (drained-broadcast derivation).
- Compensation: relaxed assert admits pairs; failed CREATE-shaped and
  DROP-shaped bundles compensate exactly; a **failed DROP COLUMN** (fault
  injected after the IDX cascade) restores the index and leaves its directory
  intact (unpaired-row `is_create` regression); a fault injected in
  `hook_column_alter`'s staging step leaves the old schema image intact and
  compensates cleanly (stage-then-publish regression).
- Padded shard: header region count round-trip; open old-width shard under
  wider schema; cursor reads NULL through `get_null_word`, `to_unified`, and
  `slice_to_owned_batch`; **UPDATE/DELETE retraction of a padded row with
  NULL new-column payload retracts the right row** (payload-comparator
  regression); compaction of a padded shard with a new-width shard preserves
  NULLs; `file_npc > schema_npc` rejected.
- `swap_schema` push-down: memtable runs, `in_memory_l0` runs,
  `cached_full_scan`, **`PartitionedTable.schema`**, and shard handles all
  swapped/cleared; agreement assert covers `replicated`/`dist_prefix_len`;
  ADD COLUMN on a **replicated** and on a **CLUSTER BY** table preserves
  routing (dist-bits regression); equal-region-count (DROP NOT NULL) swaps
  descriptor without run rebuild.
- `lift_batch_to_schema` round-trip; `op_null_extend` equivalence after the
  refactor; ingest pad keyed on region count with a schemaless batch.

E2E (`crates/gnitz-py/tests/test_alter.py`, `GNITZ_WORKERS=4`):

- rename table/column/view; old name gone, new name resolves; data intact; a
  second connection sees the rename next statement; renaming a populated
  exchange-join **view** leaves its output weights unchanged; renaming an
  FK-child table and an FK-parent table both work.
- ADD COLUMN on a populated multi-worker table that has **overflowed since the
  last checkpoint** (exercises the RAM tier): pre-ALTER rows read NULL,
  inserts/updates of the new column work, UPDATE/DELETE of pre-ALTER rows
  works, scans and point-seeks merge padded shards with new writes,
  `SELECT *` includes the column.
- ADD COLUMN → kill before checkpoint → recovery replays the pre-ALTER SAL
  tail through the ingest pad; post-restart scans identical.
- DROP COLUMN of a **middle** column: wildcard/arity/name resolution exclude
  it; INSERT values land in the right columns (positional-remap regression);
  re-ADD of the same name works; covering index is cascade-dropped and the
  client's index cache refreshes; explicit INSERT naming it errors.
- DROP NOT NULL: NULL insert succeeds after, fails before.
- ALTER VIEW: definition replaced, backfill correct on populated bases, old
  view storage (incl. scratch dirs) reclaimed, RESTRICT with a dependent view,
  self-reference rejected.
- RESTRICT matrix: every rejected combination from section 1 errors cleanly
  and leaves the catalog unchanged (verified via a follow-up identical CREATE).
- Transaction pin: `BEGIN; INSERT` (buffered, no round-trip) on conn A, ALTER
  on conn B, next statement on A fails with the rollback error.
- Concurrent ADD COLUMN from two connections: exactly one succeeds; concurrent
  RENAME from two connections: exactly one succeeds, the loser gets "catalog
  changed concurrently".
