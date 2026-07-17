# ALTER TABLE / ALTER VIEW

ALTER support built entirely on the ZSet catalog: every ALTER is one `FLAG_DDL_TXN`
bundle of signed sys-table rows flowing through the existing write spine
(`submit → precheck_family → apply_local → fire_hooks → SAL broadcast → worker
ddl_sync → boot replay`). No new wire opcode, no new durability mechanism, no new
storage format. **This plan changes no on-disk shard/WAL format and never widens a
table's physical region count** — every supported op leaves `strides_from_schema`
unchanged. The engine-side work is: order-independent catalog application
(sign-two-pass folds, reconciling register hooks, retraction payload-equality),
one new Column-family precheck arm and side-effect hook, an equal-region descriptor
swap, extending the DDL tick-quiesce to schema-mutating ALTERs, and id-only
relation directories. Everything else is client-side planning plus small client
fixes.

## 1. Supported statements and semantics

| Statement | Bundle shape | Physical effect |
|---|---|---|
| `ALTER TABLE t RENAME TO n` (t may be a table **or** a view) | TABLE_TAB or VIEW_TAB `(-1,+1)` same id, new `name` | none |
| `ALTER TABLE t RENAME COLUMN a TO b` | COL_TAB `(-1,+1)` same column_id, new `name` | none |
| `ALTER TABLE t DROP COLUMN c` | COL_TAB `(-1,+1)` same column_id: `is_hidden=1`, `is_nullable=1` (covering secondary indexes are cascade-retracted engine-side) | none (logical drop; layout keeps the column); the storage descriptor's comparator for that column swaps to the nullable form |
| `ALTER TABLE t ALTER COLUMN c DROP NOT NULL` | COL_TAB `(-1,+1)` same column_id, `is_nullable=1` | none (strides unchanged); descriptor comparator for that column swaps to the nullable form |
| `ALTER TABLE t ADD CONSTRAINT [n] UNIQUE (cols)` | maps to the existing CREATE UNIQUE INDEX path (IDX_TAB `+1`, global preflight) | index build |
| `ALTER TABLE t DROP CONSTRAINT n` | maps to the existing DROP INDEX path (IDX_TAB `-1`) | index teardown |
| `ALTER VIEW v AS <query>` | **two sequential DDL zones**: the fully-validated new plan is compiled first, then `drop_view` (old vid + hidden segments), then `create_view_chain` (fresh vid, same name) | new view storage; backfilled by the existing live CREATE-VIEW driver |

Exactly **one** operation per `ALTER TABLE` statement; a multi-operation statement
(comma-separated ops) is rejected with a clear error. Note sqlparser 0.62's
`DropColumn` operation carries `column_names: Vec<Ident>` — `DROP COLUMN a, b` is a
**single** `AlterTableOperation`, so it slips past the "one operation" guard and needs
its own `column_names.len() != 1` reject. ALTER inside an open transaction is already
rejected by `gnitz-sql/src/dispatch.rs::reject_in_transaction` (`:24-43`, an allowlist
of `Insert/Query/Update/Delete/StartTransaction/Commit/Rollback` — ALTER is rejected
by omission) — no change needed there.

`IF EXISTS` is honored for every form parsed as `ALTER TABLE` (its `AlterTable`
struct carries `if_exists`), including `RENAME TO` on a view. sqlparser's
`Statement::AlterView` has **no** `if_exists` field in any version, so
`ALTER VIEW IF EXISTS v AS …` does not parse; `IF EXISTS` is therefore not offered on
the `ALTER VIEW … AS <query>` form.

Postgres accepts `ALTER TABLE <view> RENAME TO`; we do the same because sqlparser's
`Statement::AlterView` is only the `AS <query>` shape (no `RENAME TO`), and
`parse_alter` branches purely on the `TABLE`/`VIEW` keyword with no catalog awareness,
so `ALTER TABLE <view-name> RENAME TO n` always parses as
`AlterTableOperation::RenameTable`. `ALTER VIEW` on a table is rejected; `ALTER TABLE`
column operations on a view are rejected. A rename target with a schema qualifier
naming a different schema than the source's is rejected (cross-schema moves would break
qname bookkeeping and hidden-segment scoping).

`ALTER VIEW` is deliberately **not** atomic across the drop and create. The new
definition is planned and validated client-side *before* the drop is issued, so the
only failures after the drop are engine/I/O errors; on such a failure the view is gone
and the statement errors telling the user to re-issue the CREATE. Single-bundle
atomicity would require in-bundle qname release, mixed create+drop compensation with
per-zone directory tracking, and two-phase rollback ordering — machinery serving only
this statement.

### Rejected operations (planner-side, clear errors)

- **`ADD COLUMN`** — widens the physical region count
  (`strides_from_schema = 3 + npc + 1`), which requires a lazy shard-padding storage
  subsystem (a persisted per-shard region count, padding-aware shard open, a null
  image, an ingest-time pad, and a shard-format version bump). That is a self-contained
  storage change out of scope for this catalog-only plan; reject `AddColumn` here
  (also reject its `if_not_exists` and any `column_position` FIRST/AFTER).
- PK changes of any kind (add/drop/reorder PK columns). The OPK is the routing, sort,
  filter, and manifest-guard key everywhere; this is a table recreate.
- Column type changes (`SetDataType`) — stride change forces a full rewrite.
- `SET NOT NULL` — needs a full-table validation scan; not built.
- `SET DEFAULT` / `DROP DEFAULT` — column defaults do not exist in gnitz.
- `DROP COLUMN` on a PK column, a SERIAL column, or a column carrying FK metadata on
  either side (`fk_by_child` / `fk_by_parent`); the FK must be dropped first.
- `DROP COLUMN` naming more than one column (`column_names.len() != 1`), or with
  `CASCADE` (`drop_behavior == Some(DropBehavior::Cascade)`).
- `ADD FOREIGN KEY` — needs a child-side validation scan; not built.
- `ADD CONSTRAINT … NOT VALID` (`not_valid == true`), and any `AddConstraint` whose
  constraint is not `UNIQUE`.
- `ALTER TABLE` with `ONLY`, a `HiveSetLocation`, an `ON CLUSTER`, or a non-`None`
  `table_type` (Iceberg/Dynamic/External).
- Any ALTER targeting a system relation (`tid < FIRST_USER_TABLE_ID`) or a relation in
  `_system`.
- Any column-set or nullability change (`DROP COLUMN`, `DROP NOT NULL`) on a table with
  dependent views → rejected via `dag.get_dep_map()`, same RESTRICT style as the
  existing DROP guard. Renames are exempt: views bind sources by id and columns by
  ordinal, so renames are always safe with dependents. (RESTRICTing column/nullability
  changes is what keeps operator traces — which hold re-keyed copies of base rows under
  the old comparator — out of scope: no dependent circuit ever observes a mutated base
  schema.)
- `ALTER VIEW` on a view with dependent views → rejected by the existing view-dependency
  drop guard, which the drop zone triggers unchanged. The engine has no planner, so it
  cannot re-plan dependents; RESTRICT is the only correct answer.
- Self-referential `ALTER VIEW v AS SELECT … FROM v` → rejected client-side in
  `execute_alter_view` (the old vid appears in the new plan's source set) before any
  zone is issued — otherwise the drop zone would remove the new definition's own source.

## 2. Existing machinery this rides on (verified)

- Generic catalog-mutation wire message (`FLAG_DDL_TXN`, `gnitz-wire/src/flags.rs:65`,
  `1 << 57`) — verb semantics are entirely row signs + target families. No wire change.
- Every COL_TAB delta bumps the per-table `schema_version` epoch
  (`apply_col_names_invalidate`, `catalog/cache.rs:258-264`, calling
  `invalidate_col_names`, `:98-106`) and drops the wire-schema block
  (`clear_col_cache_no_bump`, `:90-96`, `schema_wire_cache.remove` at `:95`); every
  push/scan OK reply stamps the version (`worker/reply.rs:134`;
  `orchestration/executor.rs:2386` in `send_ok_response`); stale hint-only pushes are
  rejected by the server (`STATUS_SCHEMA_MISMATCH`) and the client re-fetches
  (`gnitz-core/src/connection.rs:577-591`, in `roundtrip_push`). Client staleness after
  ALTER is handled end to end with zero new code.
- Column caches are rescan-from-ZSet (`fill_column_caches`, `catalog/registry.rs`); a
  rewritten COL_TAB re-derives with no bespoke patching.
- `SysFamily::Column` runs only cache folds today (`catalog/hooks.rs:69-73`:
  `apply_col_names_invalidate`, `apply_fk_constraints`, `apply_needs_lock` — no
  side-effect hook) and has a no-op precheck arm shared with five other families
  (`catalog/write_path.rs:284-292`, `Column | ViewDep | Sequence | CircuitNodes |
  CircuitEdges | CircuitNodeColumns`) — the new Column precheck must split into its own
  arm and preserve the five others' no-op.
- Boot replay full-scans consolidated sys tables through the same hooks
  (`catalog/bootstrap.rs:268-286`, order Schema, Table, View, Column, Index;
  `replay_system_table` calls `full_scan()` then `fire_hooks`); a net `(-1,+1)` survives
  restart as the new image.
- The DDL zone (catalog write lock + committer drain, `orchestration/executor.rs:2058-
  2101`; checkpointing forced off, `master/dispatch.rs:799-801`) plus the tick/exchange
  quiesce (§4) are the quiescing window. Worker SAL application is FIFO and `DdlSync` is
  deferred across exchange waits; reply trains are materialized `Rc<Batch>`es at enqueue
  — no lazy cursor crosses the swap.
- Batches, WAL blocks, SAL group frames, and exchange rounds are self-describing; the
  batch pool is schema-agnostic; the committer `merge_pool` and master preflight pool
  guard with `schema ==` and re-allocate on mismatch. (No batch in this plan changes
  region count, so these guards never fire on an ALTER, but the descriptor swap must
  still leave every holder agreeing — §5.)
- SAL recovery pass 2 applies only `FLAG_PUSH` entries, all through
  `ingest_returning_effective` (`runtime/bootstrap.rs:217-246`); catalog replays before
  data. `ipc::decode_wire` hard-errors on `FLAG_HAS_DATA` without an embedded schema
  block (`protocol/wire.rs:1067-1071`, "FLAG_HAS_DATA without FLAG_HAS_SCHEMA").
- xor8 probes, shard-index guard keys, and point-seek routing are PK-region only —
  untouched by any supported op (none mutate the PK region).

## 3. Order-independent catalog application

The batch `fire_hooks` sees is the raw decoded bundle — **not** sorted, and Stage-A
compensation re-sorts by `catalog_topo_priority` and negates weights in place
(`catalog/write_path.rs:754-766`), so no same-PK `(-1,+1)` rewrite pair can rely on
client emission order. The changes below make the apply layer order-independent; the
"edge-triggered hooks are not retract+insert-safe" hazard notes at `catalog/mod.rs:16-27`
and `catalog/hooks.rs:14-26` — which name ALTER's `(-1,+1)` pair as the exact breaking
case — are then **deleted** (the hazard no longer exists), not patched.

Terminology: within one family batch, a **pair** is a PK carrying rows of both signs;
an **unpaired** row is one whose PK appears with a single sign. The **paired-PK set** is
computed **per family** (not one global PK set — a hand-crafted raw-capi bundle could
carry numerically-colliding PKs across families), **once** in `handle_ddl_txn` (before
apply) and threaded to every consumer (precheck, `hook_cascade_fk`, `family_pks_by_sign`)
— no site re-derives it. It is only available on the master live-apply path (a
`handle_ddl_txn` artifact); worker `ddl_sync` and boot replay do not have it, which is
why the register hooks (§3.2) reconcile from net state rather than from this set.

### 3.1 Sign-two-pass fold application

`fire_hooks` applies each family's `apply_*` cache folds in two passes over the batch:
all `weight < 0` rows first, then all `weight > 0` rows. This restores the natural Z-set
application order (a retraction always observes the pre-bundle state for its key) for
**every** fold at once, including the compensation path (the negated pair's `-1` carries
the new name, which *is* the live state at compensation time):

- `apply_entity_caches` (`catalog/cache.rs:163-198`): its `-1` arm reads the old name
  from live `entity_by_id` (`:179`) — correct exactly when retractions run first.
- `apply_pk_col_of` (`cache.rs:231-255`) and the set-membership folds (`tables_by_schema`,
  `views_by_schema`): `-1` then `+1` on the same key nets to present; the `+1`-first order
  that nets to absent can no longer occur.

No individual fold changes. (A rename pair spuriously bumps `schema_version` and clears
the lazily-refilled column caches — documented harmless.)

### 3.2 Reconciling register hooks

`hook_table_register` and `hook_view_register` stop being edge-triggered: storage is
applied before hooks fire (`write_path.rs:113-119`: `ingest_borrowed_batch` at `:113`,
`fire_hooks` at `:119`), so each hook gates its side effects on the **net live state** of
the row — the reconcile shape `hook_index_register` already uses (its drop branch acts on
what remains live, `hooks.rs:574-601`). Net liveness is probed with `seek_exact_live`
(`storage/lsm/read_cursor/mod.rs:352-363`, gating on net weight through the merged cursor,
the primitive `retract_single_row` uses at `catalog/utils.rs:193`):

- `+1` row: register only if the family store's net row for this PK is live **and**
  `dag.tables` lacks the id (the existing `contains_key` skip at `hooks.rs:217`/`:361` is
  half of this). If the id is already registered, verify the live row's fields that ALTER
  must never mutate (schema_id, pk_col_idx, flags) still match the registered entry —
  defense-in-depth against corrupt replayed rows — and skip.
- `-1` row: run the teardown branch (cascades, `unregister_table`, `pending_dir_deletions`,
  `purge_table_versions`) only if the net row is **dead**.

A rename pair nets to "live + registered" for both rows → no side effects fire, on every
path (live apply, worker ddl_sync, boot replay, compensation), in any row order.

**`hook_cascade_fk` gets pair-exclusion.** It fires `create_fk_indices` for any live `+1`
Table row (`hooks.rs:615-624`, gated on `ctx.is_live()` and `dag.tables.contains_key`), and
the FK auto-index name embeds the *current* table name (`make_fk_index_name` in
`create_fk_indices`, `catalog/ddl.rs:661`, from the current `entity_by_id` lookup at
`:650`) — already updated by the folds — so a rename pair's `+1` would mint a duplicate
`__fk_` index row. Fix: skip PKs in the bundle's paired-PK set. (The `create_fk_indices`
name-string dedup is already correct absent a rename, so it is not re-keyed — the pair-skip
is the whole fix.)

The two hooks use **different** exclusion mechanisms on purpose, and the difference is
load-bearing: `hook_cascade_fk` is gated `ctx.is_live()`, so it runs only on the master
live-apply path where the paired-PK set exists, and can use the cheap set-membership skip.
The register hooks run on **every** path (master live, worker `ddl_sync`, boot replay),
and the paired-PK set is not threaded to the worker/replay paths — so they must reconcile
from net state via `seek_exact_live`, which is correct on all three (storage is applied
before hooks, the DDL zone serializes catalog mutation, and boot replay scans the
already-**consolidated** sys tables where a rename pair has already folded to its net `+1`).
Do not "simplify" the register hooks to the paired-set skip — it would silently break
worker `ddl_sync`.

### 3.3 Post-image precheck for Table/View families

The TABLE_TAB/VIEW_TAB arms of `precheck_family` validate the **post-image** over the
touched PKs:

- **Retraction payload-equality** (the Z-set retraction contract: you may only retract the
  element that exists): every `-1` row's full payload must byte-equal the current live row
  for that PK; reject "catalog changed concurrently" otherwise. Without this, a rename
  composed from a stale client snapshot (concurrent renames from two connections) passes the
  net check yet leaves a persistent negative row plus two live head rows — violating base
  positivity. This is the cheapest correct compare-and-swap (one payload `memcmp`, not
  field-by-field). It applies identically in the Column arm (§3.4).
- Per PK: `net = live_weight + Σ batch weights` must be 0 or 1. (Sys stores are not
  `unique_pk` — `RelationKind::SystemCatalog` — so nothing else prevents a duplicate live
  head row.)
- A PK live both before and after (a pair) may differ from the live sys row **only** in
  `name`. Any other field diff (schema_id, pk_col_idx, flags, sql_definition, created_lsn)
  is rejected.
- Post-image qualified-name injectivity: a new name must not collide with any live entity
  that survives the bundle.
- Rewrites on `tid < FIRST_USER_TABLE_ID` are rejected — closing the pre-existing gap that
  no precheck guards system-range payload ids.
- **The existing drop-integrity guards re-key on net-dead PKs.** Today `drop_ids` is
  collected from raw `weight < 0` rows (`write_path.rs:379-390`) and feeds the FK-children
  guard (`:482-496`) and the view-dependency guard (`:507-524`), both via
  `drop_ids.binary_search`; a rename pair would land its tid in `drop_ids` and be rejected
  as "referenced by FK" / "View dependency". Both guards (and `drop_ids` itself) act only on
  PKs whose bundle **net is dead** (exclude the paired-PK set), preserving §1's
  rename-with-dependents exemption without weakening DROP protection.

### 3.4 Column-family precheck arm

Replaces the shared no-op arm (split Column out of the six-family arm at
`write_path.rs:284-292`, leaving the other five no-op). It is skipped when
`ctx.in_cascade_drop()` is set; `cascade_retract_columns` (`hooks.rs:339-351`) is wrapped
in `with_cascade_drop`, like `cascade_retract_indices` already is — without this, every
DROP TABLE/VIEW cascade would trip the new arm. (Worker ddl_sync and master sys-table SAL
recovery bypass precheck entirely — §3.7.)

For submissions where the owner is already registered:

- Owner must be a registered **user table** (not a view, not system-range).
- Owner must have no dependent views (`dag.get_dep_map()`), except when every touched PK is
  a pure rename.
- Retraction payload-equality for every `-1` row, as in §3.3.
- Pair: the payload diff vs the live row must be exactly one of (a) `name` only — rename;
  (b) `is_hidden 0→1` plus `is_nullable →1` — DROP COLUMN, and the column must not be a PK
  column, not SERIAL, not FK-involved; (c) `is_nullable 0→1` only — DROP NOT NULL, not on a
  PK column.
- Unpaired `-1` on a registered owner is rejected (physical column removal does not exist).
- Unpaired `+1` (an ADD COLUMN append) is rejected here as well — ADD COLUMN is out of scope
  (§1). This also closes the door on a raw-capi bundle attempting a bare column append.

### 3.5 Compensation

`compensate_stage_a` (`write_path.rs:721-788`) keeps its structure — topo-direction sorts
and the binary drain-vs-discard of `pending_dir_deletions` — with two corrections:

- The homogeneity `debug_assert` (`write_path.rs:740-750`) is relaxed to a **per-family**
  predicate: **each family in the rollback list is internally homogeneous (all one sign)
  OR fully paired** (every negative PK also appears positively in that same family, and
  vice versa). A failed DROP COLUMN bundle is `{COL pair} + {engine-cascaded IDX -1}`: the
  COL family is fully paired ✓ and the IDX family is homogeneous-negative ✓. A bundle-wide
  "mixed only as pairs" predicate would reject on the unpaired IDX `-1` and abort debug
  builds — the per-family form is the correct relaxation.
- `is_create` (computed on the **original**, un-negated weights, `write_path.rs:736-738`;
  negation is later at `:766`) classifies by **unpaired rows only** (`∃` unpaired `+1`). The
  raw any-positive-weight rule would classify a failed DROP COLUMN bundle (COL pair +
  cascaded IDX `-1`) as CREATE: its compensation would then DESC-sort (restoring the index
  before its columns) and, fatally, **drain** the forward-queued index-dir deletion —
  `remove_dir_all` on the directory of an index the compensation just restored as live.
  Under unpaired-row classification that bundle is DROP-shaped: ASC restore order and the
  discard path preserve the directory. A pure-pair bundle (rename) classifies DROP with an
  empty queue — a no-op either way.

### 3.6 Pair-aware bundle bookkeeping in `handle_ddl_txn`

`family_pks_by_sign` (`orchestration/executor.rs:1987`, doc `:1983-1986` "weight-homogeneous
per family") assumes weight-homogeneous families; rewrite pairs break it. A view rename would
set `view_create` (`:2055-2056`) and **re-backfill a populated exchange/join view** — Z-set
addition, doubling its output weights — and a table rename's `-1` would enter `dropped_tids`
(`:2171`), spuriously invalidating unique filters on a live table. Fix: a pair-aware variant
that excludes the bundle's paired-PK set, used for `new_view_ids` and `dropped_tids`.

Additionally, the post-fsync invalidation lists (`dropped_indices` → unique filter removal)
are derived from the **drained broadcasts** rather than the client bundle, so engine-cascaded
IDX retractions (DROP COLUMN's covering indexes, §5) release their unique-filter memory
instead of leaving dormant entries.

### 3.7 Master sys-table SAL recovery bypasses precheck

Master system-table SAL recovery runs **after** `go_live()` (`catalog/apply_context.rs:29-31`;
`recover_system_tables_from_sal` at `runtime/bootstrap.rs:427`) through `ingest_to_family →
submit → precheck_family`, with rejections silently swallowed (`runtime/bootstrap.rs:161`
tests only `.is_ok()`). With the new arms this would re-judge cascaded COL `-1` groups that
replay as independent entries (children-first) while the owner is still registered. Fix: route
this recovery through `apply_local` (no precheck) exactly like worker `ddl_sync` — the rows are
master-validated by definition — and treat a replay `Err` as fatal (`gnitz_fatal_abort!`, as
pass 2 already does at `runtime/bootstrap.rs:230-238`) instead of uncounted. Hooks (which re-run
everywhere) remain the defense-in-depth layer.

## 4. Extending the tick/exchange quiesce to schema-mutating ALTERs

The DDL handler engages the tick/exchange **Quiesce** (`orchestration/executor.rs:2074-2085`,
`TickTrigger::Quiesce`) only when `view_create` is set. An ALTER bundle carries no `VIEW_TAB
+1`, so today it would take the `TickGate(None)` branch and **never quiesce**. That is a
correctness hole: the catalog write lock blocks only master-side readers, not in-flight worker
exchanges. A worker blocked in `do_exchange_wait` for an unrelated view **defers** the ALTER's
`DdlSync` (`worker/mod.rs:465-506`, `exchange.deferred`) while applying a **subsequent**
same-table `Push` **inline** (`worker/mod.rs:923` → `ingest_returning_effective`) — so the push
lands in a store whose descriptor has not yet swapped. For DROP COLUMN / DROP NOT NULL that means
a NULL written under the still-`FixedIntNonnull` comparator (`compute_payload_cmp`,
`schema.rs:126-142`) consolidates against a real `0` — a silent weight error. The window is real
at `W ≥ 2` whenever a multi-round exchange (`orchestration/executor.rs:2297-2301`) keeps a worker
in the exchange-wait context across the ALTER.

Fix: compute a `column_alter` flag next to `view_create` (`:2055-2056`) and take the Quiesce
branch when **`view_create || column_alter`**. `column_alter` fires for any bundle carrying a
COL_TAB row (weight > 0) whose owner tid is already registered as a base table — i.e. every column
ALTER (rename / drop-column / drop-not-null):

```
// One bundle-local pass over the COL_TAB family; no schema recompute needed.
// - CREATE TABLE: owner created in-bundle, not yet registered → excluded.
// - DROP TABLE: no COL_TAB family in the client bundle (column cascade is engine-side) → excluded.
// - table/view RENAME: no COL_TAB → excluded.
let column_alter = families.iter().any(|(tid, b)| *tid == COL_TAB_ID
    && (0..b.count).any(|i| b.get_weight(i) > 0
        && owner_registered_base_table(cat_ptr_raw, b.owner_tid(i))));
```

Only drop-not-null/drop-column on a table currently in the `FixedIntNonnull` comparator class
actually *need* the quiesce (that is the only case where a racing push could mis-consolidate a NULL
against a `0`, §5); this trigger also fires on column rename and on already-`Generic` tables, which
is a **benign over-quiesce** on a rare operation — kept deliberately, because a bundle-local
row-scan is far simpler than pre-computing the prospective comparator before the write lock. The
`dag.tables` probe is a lock-free single-threaded read at the handler's top (`:2055`, no intervening
`await`, before the write lock is taken); it can only ever *over*-quiesce, never under-quiesce a
valid ALTER (a succeeding ALTER's owner was already registered when the flag was computed). The
lock-held committer barrier (`:2089-2101`) stays `view_create`-only: it exists to flush straggler
pushes before the CREATE-VIEW **source drain**, which an ALTER has no analogue of.

The Quiesce closes the window because of a hard ordering invariant: `TickTrigger::Quiesce` is
dequeued only **after** the prior `run_tick` returned (`:655-669`), and `run_tick` returns only
after every worker's tick ACK — emitted only once that worker has exited `do_exchange_wait`. So at
`DdlSync`-broadcast time (`emit_zone_to_sal`, `:2242`) no worker is in an exchange-wait: the swap
`DdlSync` lands on the main dispatch path (inline, `worker/mod.rs:409-418`), not deferred. The
write lock blocks all pushes for the whole zone, and SAL FIFO then orders the swap before any
post-ALTER push on every worker; a pre-swap straggler push can only carry **non-null** values (the
client rejects a null bit on a still-NOT-NULL column), which sort identically under either
comparator. The tick loop stays gated on `release` for the whole handler, so no new exchange can
start before the swap. A `view_create && column_alter` bundle takes the superset (Quiesce +
barrier + swap) and is handled.

## 5. Engine: `hook_column_alter` and the equal-region descriptor swap

New side-effect hook on the `Column` family, run after the cache folds (add it to the
`SysFamily::Column` arm at `hooks.rs:69-73`). Its trigger is **batch-shape-derived, never
context-derived** — cascaded whole-table COL retractions (DROP TABLE/VIEW) reach this hook on
workers and at replay with no cascade flag available, so the shape is the only reliable signal.
Per distinct owner tid in the batch:

1. Skip unless the owner is registered in `dag.tables` **and** is a base table (`entry.kind`).
   Skip any owner whose batch rows include an **unpaired `-1`** — that shape is the
   drop-cascade/replay signature (physical column removal does not exist); acting on it would
   rebuild a schema from zero columns and panic. The hook acts only on pairs (rename /
   drop-column / drop-not-null). ("Exactly one operation per ALTER" (§1) guarantees an owner's
   COL rows in one bundle are a single pair — never a pair mixed with an unpaired `-1`.) Live
   CREATE TABLE bundles apply COL_TAB before TABLE_TAB, so the owner is unregistered and skipped;
   at boot replay Table registers before Column (`bootstrap.rs:268-286`) and step 3's equality
   check makes the hook a no-op.
2. Re-assert the §3.4 pair semantics against the live rows (defense-in-depth on the
   precheck-skipping paths) — asserting, never erroring, on shapes it skips. If the batch flips
   `is_hidden 0→1` for a column (DROP COLUMN), cascade-retract **only the secondary indexes that
   cover that column** — enumerate the owner's indexes via `indices_by_owner` and retract those
   whose key columns include the dropped column, under `with_cascade_drop`. (Do **not** reuse
   `cascade_retract_indices` (`hooks.rs:321`) unfiltered — it drops *all* of the owner's indexes,
   which would silently kill indexes on unrelated columns.) This keeps "cascade is the applier's
   declared reaction" (`write_path.rs:14-20`), works for raw capi bundles, is idempotent on worker
   re-derivation (reads live rows; the children-first SAL order means the worker's own cascade
   finds the index already retracted), and bumps the client `index_version` epoch via
   `apply_index_by_id` so cached `IndexMeta` re-fetches.
3. Rebuild the descriptor: `read_column_defs(owner)` (`catalog/registry.rs:71`) →
   `build_schema_from_col_defs(defs, pk, cur.dist_prefix_len())` (`registry.rs:103`, takes
   `dist_prefix_len`) `.with_replicated(cur.replicated())` — the distribution prefix and
   replicated flag live on the registered `TableEntry.schema` (from the TABLE_TAB flags), **not**
   COL_TAB, and `build_schema_from_col_defs` alone would silently drop them (wrong-worker routing
   on CLUSTER BY tables, misclassified replicated stores). `dist_prefix_len()` (`schema.rs:442`) is
   a private `const fn` — callable here since the hook is same-crate. If the result equals the
   current `TableEntry.schema`, stop (idempotence under replay and re-application).
   `SchemaDescriptor::eq` (`schema.rs:983-992`) compares `num_columns`, `pk_indices`, and full
   column bytes (type_code + nullable) but **not** `replicated`/`dist_prefix_len`, so a rename
   (names are not in the descriptor) ends here, while DROP COLUMN and DROP NOT NULL (both flip
   `is_nullable`, so the descriptor differs) proceed to the swap.
4. Publish infallibly through one push-down entry point: `DagEngine::swap_table_schema(tid, desc)`
   updates `TableEntry.schema` and calls `StoreHandle::swap_schema(desc)` →
   `PartitionedTable::swap_schema(desc)` (updating **its own copy** — it routes with
   `self.schema.partition_for_pk` and slices scatter sub-batches with it,
   `storage/lsm/partitioned_table.rs:198,208`) → each partition `Table::swap_schema(desc)`. There
   are **exactly three comparator-schema holders inside one partition `Table`**, and all three must
   be swapped or a NULL will consolidate against a real `0` on a path the primary holder doesn't
   cover:
   - `Table.schema` (`storage/lsm/table/mod.rs:221`) — the read-cursor / `into_consolidated` /
     `compare_rows` comparator.
   - `Table.memtable.schema` (`MemTable`, `table/mod.rs:38`) — the **flush** consolidation
     comparator: `runs_as_sorted` → `as_sorted_mem_batch(&self.schema)` and
     `consolidate_batches(&batches, &self.schema)` (`storage/lsm/memtable/runs.rs:108,128,151`).
   - `Table.shard_index.schema` (`ShardIndex`, `storage/lsm/shard_index/mod.rs:169`) — the
     **compaction** comparator: `compact_shards(&input, &out, &self.schema, …)`
     (`shard_index/index.rs:314`).

   Also clear `cached_full_scan` (`table/mod.rs:234`), which would otherwise serve a cached batch
   compared under the old descriptor. The comparator dispatch (`with_payload_cmp!`,
   `storage/repr/columnar.rs:335`) reads `payload_cmp` from the schema **passed by the caller** at
   each of these sites, never from a batch's embedded schema — so **batch-embedded schemas
   (`Batch.schema`, `in_memory_l0` run batches) are not comparator sources and must not be touched**
   (`InMemRun` is `{ batch, bloom }`, no schema field; `consolidate_runs(runs, schema)` takes the
   comparator as a parameter). **Every supported op leaves the region count unchanged**, so this is
   a pure descriptor replacement: no run rebuild, no shard reopen, no data motion — the run bytes
   are identical (and `FixedIntNonnull` and `Generic` impose the *same* order on non-null values,
   so old rows stay sorted; only a *new* NULL row would sort differently, and the §4 quiesce
   guarantees no such row is written under the old comparator). Afterwards `debug_assert` that all
   holders agree — **`TableEntry.schema`, `PartitionedTable.schema`, and every partition's
   `Table.schema` / `Table.memtable.schema` / `Table.shard_index.schema`** — on the descriptor
   **including `replicated` and `dist_prefix_len`** (which `eq` won't check).
   `swap_schema`/`swap_table_schema` are new; model them on the existing
   rebuild-and-replace idiom at `storage/lsm/partition_lsn.rs:87,135` (which construct a fresh
   `PartitionedTable` and drop it into `entry.handle`) but as an in-place descriptor mutation, since
   there is no data to move.
5. `debug_assert!(dependent views of owner are empty)` — the RESTRICT invariant from §1 made
   load-bearing, and the reason no plan-cache invalidation is needed: no compiled circuit scans this
   table. The `vm/exec.rs:94` width assert (gated on `input_batch.schema.is_some()`) stays as the
   backstop.

The hook runs identically on the master (live apply), on every worker (`ddl_sync` → `apply_local`),
and at boot replay (no-op by step 3). Post-fork the master holds no user partitions; replicated
tables get the swap on every worker via ddl_sync. The DDL zone plus the §4 quiesce guarantee no
tick or scan is in flight during the swap.

Why swap-in-place instead of re-registering the storage handle: `PartitionedTable::new` opens from
manifests only; the DDL zone does not checkpoint user memtables (the committer barrier flushes WAL,
not shards), so re-registration of a populated table would drop every unflushed row. The
`partition_lsn.rs:87,135` handle-replacement sites operate on **empty** stores by documented
contract and are not a precedent for a populated one.

## 6. Id-only relation directories

`table_dir` / `view_dir` (`catalog/utils.rs:49-56`) embed the relation **name**
(`{schema}/{name}_{tid}` and `{schema}/view_{name}_{vid}`), recomputed from the live catalog at
every registration (`hook_table_register`, `hooks.rs:254`; `hook_view_register`, `hooks.rs:382`) —
so under RENAME the next boot would recompute a new-name path and orphan the data. Formats change to
id-only: `{base}/{schema_name}/t_{tid}` and `v_{vid}` (index dirs are **already** id-only —
`index_dir`, `utils.rs:59-61`, emits `{owner_dir}/idx_{idx_id}` with no name — no change there).
Renames then never touch the filesystem.

Consumers keep working unchanged: `is_table_dir_name` (`utils.rs:122-124`) tests only the shape
"ends in `_<digits>`", which `t_5`/`v_10` satisfy; the orphan sweep (`gc_orphan_directories`,
`write_path.rs:623`) reads `TableEntry.directory` (the recomputed **in-memory** field, `:625`), not
any persisted column; child scratch dirs nest under the recomputed `view_dir`
(`query/compiler/emit.rs:64-66`).

(The dead persisted `directory` / `cache_directory` payload columns — TABLE_TAB, VIEW_TAB, **and**
IDX_TAB all carry one — are left untouched by this plan; they are already written empty and never
read for path resolution. Removing them is an independent wire-schema cleanup, not part of ALTER
correctness.)

## 7. ALTER VIEW mechanics

Client-side `execute_alter_view`, in strict order:

1. Resolve the old vid by name (error if not found — the `AS <query>` form has no `IF EXISTS`).
2. Compile the new query exactly as `execute_create_view` does (CTE inlining, derived-table chain,
   shape dispatch), allocating a **fresh vid** and fresh hidden-segment vids. Ids are never reused in
   gnitz (`registry.rs`/`cache.rs` monotonic allocation); keeping the old vid would need a new engine
   re-register path for zero benefit — every consumer resolves name→id per statement, and dependent
   views (the only id-pinned consumers) are RESTRICTed.
3. Reject self-reference: the old vid must not appear in the new plan's source set.
4. Zone 1: `drop_view` (old vid + its hidden segments — the hidden-segment prefix embeds the owner
   **vid**, so this is rename-proof; the engine's `hook_view_register` drop branch cascades
   circuit/dep/col rows; the view dir — which nests the operator scratch dirs — is queued for
   recursive deletion). The existing view-dependency guard (`precheck_family`,
   `write_path.rs:507-524`, re-evaluated under the catalog write lock at drop time) rejects the drop
   if dependents exist, before anything is torn down.
5. Zone 2: `create_view_chain` with the pre-compiled plan under the same name. The head view carries
   `sql_definition` of the re-rendered `CREATE VIEW`-shaped statement. `handle_ddl_txn` backfills the
   new vids through `order_by_intra_bundle_deps` + `fan_out_backfill`/inline backfill exactly as live
   CREATE VIEW does.

## 8. SQL front end and client

### 8.1 Dispatch and validation (`gnitz-sql`)

sqlparser is **0.62**. `Statement::AlterTable` is a tuple variant wrapping a struct
(`AlterTable(AlterTable { name, if_exists, only, operations, location, on_cluster, table_type,
end_token })`); `Statement::AlterView { name, columns, query, with_options }` is unchanged (no
`if_exists`).

- `dispatch.rs`: add arms for `Statement::AlterTable(a)` and `Statement::AlterView { .. }` (the match
  currently has a catch-all at `dispatch.rs:110`). For `AlterTable`, reject when
  `a.operations.len() != 1`, then delegate to `plan/alter.rs::execute_alter_table`. For `AlterView`,
  delegate to `plan/view/dispatch.rs::execute_alter_view`. Neither is added to `reject_in_transaction`'s
  allowlist, so both stay rejected inside an open transaction.
- `plan/validate.rs`: `reject_unhonored_alter_table_clauses` — an **exhaustive destructure** of the
  0.62 `AlterTable` struct (no `..`, matching the `reject_unhonored_create_table_clauses` convention at
  `validate.rs:707`): consume `name`, `operations`, `if_exists`, `end_token`; reject `only == true`,
  `location.is_some()`, `on_cluster.is_some()`, `table_type.is_some()`. And
  `reject_unhonored_alter_view_clauses` — reject `!columns.is_empty()` and `!with_options.is_empty()`.
- The single `AlterTableOperation` is matched in `plan/alter.rs`:
  - `RenameTable { table_name: RenameTableNameKind::To(n) | RenameTableNameKind::As(n) }` → rename
    relation (both `To` and `As` mean rename-to; treat identically).
  - `RenameColumn { old_column_name, new_column_name }` → rename column.
  - `DropColumn { column_names, if_exists, drop_behavior, has_column_keyword: _ }` → reject
    `column_names.len() != 1` and `drop_behavior == Some(DropBehavior::Cascade)`; honor `if_exists`;
    → drop column.
  - `AlterColumn { column_name, op: AlterColumnOperation::DropNotNull }` → drop not null; every other
    `AlterColumnOperation` (`SetNotNull`, `SetDataType`, `SetDefault`, `DropDefault`, `AddGenerated`,
    …) → reject.
  - `AddConstraint { constraint, not_valid }` → reject `not_valid == true`; if `constraint` is a
    `UNIQUE` `TableConstraint`, re-enter the existing `execute_create_index` path; else reject.
  - `DropConstraint { name, if_exists, drop_behavior }` → re-enter the existing drop-index path.
  - `AddColumn { .. }` and every other variant → reject with a clear message.
- `plan/alter.rs` (new): resolves the target from the catalog snapshot, performs all client-visible
  validation (existence + `IF EXISTS`, kind, PK/SERIAL/FK guards, visible-name collisions,
  cross-schema rename rejection, system-relation rejection), computes the row diffs, and calls the new
  `gnitz-core` client methods.
- Statement-local catalog reads performed **after** composing a write use direct `session.scan`, never
  `scan_catalog` — the per-statement snapshot memoizes each sys table once (`client.rs:315-326`,
  populated at `:300-309`, never invalidated by `push_ddl`) and would return the pre-write image;
  `drop_schema`'s cascade (`client.rs:858-861`) is the existing pattern.

### 8.2 Client methods (`gnitz-core/src/client.rs`)

Next to `create_table`/`drop_table` (`client.rs:884-1008`): `alter_rename_relation(id, kind,
new_name)`, `alter_rename_column(tid, col_idx, new_name)`, `alter_drop_column(tid, col_idx)`,
`alter_drop_not_null(tid, col_idx)`. Each builds the family batches from section 1 (rewrite pairs =
two rows, same PK, the live row's exact payload at `-1` — §3.3's payload-equality rule — and the new
payload at `+1`, in one family batch; no ordering contract needed, §3.1) and ships through the existing
`push_ddl` choke point (`client.rs:771-778`, which rejects if a txn is open). The `-1` payload is
reproduced byte-for-byte from the client's `TableRecord`/`ViewRecord`/column decode (the same
mechanism `drop_table` uses at `client.rs:1001`).

### 8.3 Hidden-column correctness fixes (required before DROP COLUMN ships)

The invariant "base-table columns are never hidden" holds today; DROP COLUMN is the first thing to
break it, so the client DML paths that assume it must be fixed:

- `dml/insert.rs:186` — the no-column-list arity check counts `schema.columns.len() - is_serial`; must
  exclude hidden columns.
- `dml/insert.rs:491-496` — the explicit-column-list expected set filters only `is_serial`; must also
  filter hidden.
- **Positional remap**: the VALUES loop maps user values by physical column index (`extract_pk_value`
  at `insert.rs:221`, `row[ci]` at `:229`); once a *middle* column is hidden, every later user value
  would shift into the wrong column. Build an explicit visible-position → physical-ci map (skipping
  hidden and SERIAL) and drive both PK extraction and the payload loop from it.
- INSERT row encoding writes NULL (zero value + null bit set) for hidden columns — the physical layout
  still contains them.
- The invariant comments (`protocol/types.rs:35,188`, `plan/validate.rs:17`, `dml/overlay.rs:87`,
  `bind/resolve.rs:60-62`) are rewritten to: *base-table hidden columns exist only via DROP COLUMN, are
  always nullable, and are excluded from all name-facing surfaces*. The code beneath them
  (`visible_columns()` at `types.rs:191`, `find_unique_column` at `resolve.rs:63`, wildcard/JOIN
  combination) already skips hidden correctly — verified by the new tests, not changed. UPDATE is
  already safe (`resolve_set_target` → `find_unique_column` skips hidden; carry-through preserves hidden
  values).

### 8.4 Transaction schema pin (single site)

An open client transaction buffers batches shaped by the schema at statement time; a concurrent ALTER
from another connection makes a later statement reinterpret an old-shaped buffered batch under the new
schema — a misaligned client-side read no commit-time check catches (a buffered INSERT makes no server
round-trip, `client.rs:387-390`). Every path that reinterprets a buffered batch under a freshly-fetched
schema is **SQL DML only**: `overlay_batch` (`dml/overlay.rs:100,106`, UPDATE/DELETE via `mutate.rs`),
`effective_row` (`overlay.rs:56`, INSERT ON CONFLICT via `insert.rs:384,439`), and the INSERT buffered
reads (`insert.rs:392,443`). Pure SELECT in a transaction reads committed state only (never the
overlay), and capi/Python raw pushes/scans bypass the SQL overlay entirely. Every UPDATE/DELETE/INSERT
resolves its target through the binder's `resolve_base_table` (`bind/resolve.rs:177-212`) **before** any
of those reinterpretations.

Fix — **one** check, at `resolve_base_table`, reusing state that already exists: `TxnBuffer` stores an
owned `Schema` clone per buffered family (`client.rs:1490`). When the transaction already holds a family
for the resolved tid, compare its stored schema against the freshly-resolved schema with `types_match`
(`protocol/types.rs:277-285`: column count + pk_cols + per-column type_code + nullability; ignores name);
on mismatch, fail the transaction with "schema changed concurrently; transaction rolled back". Because
`types_match` ignores names, a rename passes (layout unchanged); DROP COLUMN / DROP NOT NULL change
nullability and abort before any misaligned read. A table the transaction has not buffered has no stored
schema, so a SELECT-only-then-ALTER sequence never spuriously aborts. Raw capi/Python pushes need no
client pin: they never client-reinterpret, and commit ships per-family schema blocks the server
validates. Add a small `TxnBuffer` accessor returning the stored schema for a tid.

### 8.5 Result surface

`SqlResult::Altered { object: String, name: String }` in `gnitz-sql/src/lib.rs` (enum at `:24-53`). Both
FFI surfaces match `SqlResult` exhaustively (no wildcard) and need a new arm: the dict arm in
`gnitz-py`'s `execute_sql` (`gnitz-py/src/lib.rs:1750-1808`) **and** the arm in `gnitz-capi`
(`gnitz-capi/src/lib.rs:1350-1362`). capi gets ALTER through `gnitz_execute_sql` with no new entry
points. Caller-owned schema handles (capi `GnitzSchema`, Python `Struct._schema`) remain static
declarations backed by the reactive `types_match`/server-validation net; their docs gain an "ALTER
requires rebuilding this" note.

## 9. Tests

Rust (`crates/gnitz-sql/tests/planner_alter.rs`): parse/validate/reject matrix — every supported
statement plans; every rejected operation (incl. multi-op, `DROP COLUMN a, b` plural, `ADD COLUMN`,
`SetDataType`, `SET NOT NULL`, system relations, cross-schema rename, self-referential ALTER VIEW,
`ALTER TABLE ONLY`, non-`None` `table_type`) errors with the documented message; bundle shapes (row
signs, families, pair payload diffs, `-1` payload = live-row payload) asserted against the built batches.

Rust (engine, alongside existing catalog/storage tests):

- Reconciling hooks: a rename pair fires no cascade and no dir deletion, in both row orders, on live
  apply and via `replay_catalog`; a compensated rename restores
  `entity_by_qname`/`entity_by_id`/`pk_col_of`/`tables_by_schema` exactly; renaming an FK-child table
  mints **no** second `__fk_` index row (pair-aware `hook_cascade_fk`).
- Sign-two-pass folds: same-PK pair applied `+1`-first at the batch level still yields correct caches.
- Post-image precheck: stale-snapshot rename (`-1` payload ≠ live row) rejected "catalog changed
  concurrently"; duplicate live head row rejected; rename of an FK **parent** and of a table **with a
  dependent view** both pass (net-dead guard re-keying), while genuine drops of each are still rejected.
- Column precheck + `hook_column_alter` shape-guard: DROP TABLE and DROP VIEW of relations **with
  columns** work end to end on master, workers, and through SAL recovery (regression for the
  cascade-shape panic); an unpaired `+1` (ADD COLUMN attempt) is rejected.
- §3.7: master sys-table SAL recovery applies a DROP-cascade sequence without precheck rejections; an
  injected replay error is fatal, not swallowed.
- §4 quiesce: a `column_alter` bundle takes the Quiesce branch (assert `TickTrigger::Quiesce` sent); a
  table/view RENAME and a CREATE TABLE do **not**.
- Pair-aware `handle_ddl_txn`: a view rename bundle triggers **no** backfill; a table rename does not
  enter `dropped_tids`; a DROP COLUMN's cascaded index retraction releases its unique filter
  (drained-broadcast derivation).
- Compensation: relaxed **per-family** assert admits pairs; failed CREATE-shaped and DROP-shaped bundles
  compensate exactly; a **failed DROP COLUMN** (fault injected after the IDX cascade) restores the index
  and leaves its directory intact (unpaired-row `is_create` regression) and does not trip the
  homogeneity assert.
- `swap_table_schema` push-down (equal-region): descriptor swapped on `TableEntry`, `PartitionedTable`,
  and every partition `Table`; `cached_full_scan` cleared; agreement assert covers
  `replicated`/`dist_prefix_len`; DROP NOT NULL on a **replicated** and on a **CLUSTER BY** table
  preserves routing (dist-bits regression); after the swap the column's comparator is the nullable form
  (a written NULL no longer consolidates against a real 0).

E2E (`crates/gnitz-py/tests/test_alter.py`, `GNITZ_WORKERS=4`):

- rename table/column/view; old name gone, new name resolves; data intact; a second connection sees the
  rename next statement; renaming a populated exchange-join **view** leaves its output weights unchanged;
  renaming an FK-child table and an FK-parent table both work.
- DROP COLUMN of a **middle** column: wildcard/arity/name resolution exclude it; INSERT values land in
  the right columns (positional-remap regression); covering index is cascade-dropped and the client's
  index cache refreshes; explicit INSERT naming it errors; a concurrent same-table INSERT during the
  ALTER (multi-round exchange in flight on another view) does not corrupt weights (§4 quiesce
  regression).
- DROP NOT NULL: NULL insert succeeds after, fails before; pre-ALTER non-null rows read back correctly;
  UPDATE/DELETE of a pre-ALTER row retracts the right row.
- ALTER VIEW: definition replaced, backfill correct on populated bases, old view storage (incl. scratch
  dirs) reclaimed, RESTRICT with a dependent view, self-reference rejected.
- RESTRICT matrix: every rejected combination from section 1 errors cleanly and leaves the catalog
  unchanged (verified via a follow-up identical CREATE).
- Transaction pin: `BEGIN; INSERT` (buffered, no round-trip) on conn A, ALTER on conn B, next
  UPDATE/DELETE on A fails with the rollback error; a pure `SELECT` on A of an untouched table does not
  spuriously abort.
- Concurrent RENAME from two connections: exactly one succeeds, the loser gets "catalog changed
  concurrently".
