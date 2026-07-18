# ALTER TABLE DROP COLUMN / ALTER COLUMN DROP NOT NULL

The two ALTER operations that change a base table's **payload comparator** without changing its
physical region count. Both flip a column `is_nullable 0→1`, which moves the table's payload
comparator from the `FixedIntNonnull` fast path to `Generic` (null-aware). DROP COLUMN
additionally hides the column (`is_hidden 0→1`) and cascade-drops the secondary indexes covering
it. This plan owns the **equal-region comparator-swap subsystem**: `hook_column_alter`,
`DagEngine::swap_table_schema`, the Column-family precheck arm, and the `column_alter`
tick/drive quiesce.

**Prerequisite: `plans/1-alter-table-view.md`.** This plan builds directly on plan 1's catalog
spine — the sign-two-pass fold (plan 1 §3.1), the reconciling register hooks (plan 1 §3.2), the
Table/View post-image precheck (plan 1 §3.3), the per-family compensation homogeneity relaxation
(plan 1 §3.4), the pair-aware `family_pks_by_sign` (plan 1 §3.5), id-only relation directories
(plan 1 §4), and the SQL dispatch / client-method / result scaffolding (plan 1 §6). Those are
assumed present and are not restated. Cross-plan references are always written "plan 1 §X"; a bare
"§X" always refers to a section of *this* plan.

**Extended by `plans/2-alter-add-column.md`.** ADD COLUMN reuses this exact subsystem
(`hook_column_alter`, `swap_table_schema`, the Column precheck arm, the `column_alter` quiesce)
and extends each from "equal-region descriptor swap" to "region-count growth."

**No on-disk shard/WAL format change and no region-count change** — every supported op leaves
`strides_from_schema` unchanged. DROP COLUMN is a **logical** drop: the column stays physically in
every row (its bytes are never rewritten and its disk space is never reclaimed — the same as
Postgres `DROP COLUMN`), flagged hidden and nullable. The engine-side work is one Column-family
precheck arm, one side-effect hook (`hook_column_alter`) doing the covering-index cascade plus an
equal-region descriptor swap, extending the DDL quiesce to these ALTERs, and the client-side
hidden-column DML fixes plus a transaction schema pin.

## 1. Supported statements and semantics

| Statement | Bundle shape | Physical effect |
|---|---|---|
| `ALTER TABLE t DROP COLUMN c` | COL_TAB `(-1,+1)` same column_id: `is_hidden=1`, `is_nullable=1`; covering secondary indexes cascade-retracted engine-side | none (logical drop; layout keeps the column, disk never reclaimed); the storage descriptor's comparator for that column swaps to the nullable form |
| `ALTER TABLE t ALTER COLUMN c DROP NOT NULL` | COL_TAB `(-1,+1)` same column_id, `is_nullable=1` | none (strides unchanged); descriptor comparator for that column swaps to the nullable form |

Exactly **one** operation per statement (plan 1's `operations.len() != 1` guard). sqlparser 0.62's
`DropColumn` carries `column_names: Vec<Ident>`, so `DROP COLUMN a, b` is a **single**
`AlterTableOperation` — reject `column_names.len() != 1`. ALTER inside an open transaction is
rejected by plan 1's dispatch (ALTER is absent from `reject_in_transaction`'s allowlist).

### Rejected (planner-side, clear errors), on top of plan 1's shared ALTER rejects

- `DROP COLUMN` on a PK column, a SERIAL column, or a column carrying FK metadata on either side
  (`fk_by_child` / `fk_by_parent`); the FK must be dropped first.
- `DROP COLUMN` naming more than one column (`column_names.len() != 1`), or with `CASCADE`
  (`drop_behavior == Some(DropBehavior::Cascade)`). (Covering indexes are dropped regardless — the
  non-CASCADE form still auto-drops them, matching Postgres; CASCADE is rejected only because
  gnitz has no other dependent-object kind to cascade to.)
- `ALTER COLUMN … SET NOT NULL` — needs a full-table validation scan; not built. Every other
  `AlterColumnOperation` (`SetDataType`, `SetDefault`, `DropDefault`, `AddGenerated`) is rejected
  by plan 1's `AlterColumn` arm.
- **Any column-set or nullability change on a table with dependent views** → rejected via
  `dag.get_dep_map()`, same RESTRICT style as the DROP guard. (This is what keeps operator traces
  — which hold re-keyed copies of base rows under the old comparator — out of scope: no dependent
  circuit ever observes a mutated base schema. It is also what closes the stale-cursor path in §5:
  no view backfill can be scanning an about-to-be-altered table.)

`DROP NOT NULL` on an already-nullable column, and `DROP COLUMN` on an already-nullable column,
are legal but perform **no comparator swap** (§4/§5 no-op via `SchemaDescriptor::eq`); DROP COLUMN
still hides the column and cascades its covering indexes.

## 2. The comparator-swap subsystem it introduces

`SchemaDescriptor` holds, per payload column, `type_code + nullable`; `is_hidden` is a **COL_TAB
catalog flag, not a descriptor field**. The descriptor's payload comparator is a **whole-schema**
property (`compute_payload_cmp`, `schema.rs:143-155`; `PayloadCmpKind`, `schema.rs:132-155`):
`FixedIntNonnull` iff *every* payload column is `nullable == 0 && is_fixed_int`, else `Generic`.
So the only comparator transition any supported op can cause is `FixedIntNonnull → Generic`, and
it happens iff some column flips `nullable 0→1` on a schema that was otherwise all-non-null-fixed-int.
`compare_rows_fixedint_nonnull` (`columnar.rs:266-303`) skips null-bit reads; `Generic`
(`compare_rows`, `columnar.rs:34-70`) reads them, ordering NULL strictly below any non-null value.
The two agree on every non-null value, so existing (never-null) rows stay correctly ordered after
the swap; only a *new* NULL row sorts differently, and §4's quiesce guarantees no such row is
written under the old comparator.

## 3. Catalog-application additions (on plan 1's §3 spine)

### 3.1 Column-family precheck arm

Replaces plan 1's shared no-op arm for Column: split Column out of the six-family arm at
`write_path.rs:284-292` (`Column | ViewDep | Sequence | CircuitNodes | CircuitEdges |
CircuitNodeColumns | None => Ok(())`), leaving the other five no-op. It is skipped when
`ctx.in_cascade_drop()` is set; `cascade_retract_columns` (`hooks.rs:339-351`) is wrapped in
`with_cascade_drop` like `cascade_retract_indices` already is — without this, every DROP TABLE/VIEW
cascade would trip the new arm. (Worker `ddl_sync` and master sys-table SAL recovery bypass
precheck entirely — §3.4.)

For submissions where the owner is already registered:
- Owner must be a registered **user table** (not a view, not system-range).
- Owner must have no dependent views (`dag.get_dep_map()`).
- Retraction payload-equality for every `-1` row (the plan 1 §3.3 CAS: the `-1` payload must
  byte-equal the current live row; the net check alone misses a stale-snapshot ghost).
- Pair: the payload diff vs the live row must be exactly one of (a) `is_hidden 0→1` **plus**
  `is_nullable →1` — DROP COLUMN, and the column must not be a PK column, not SERIAL, not
  FK-involved; (b) `is_nullable 0→1` only — DROP NOT NULL, not on a PK column. (A `name`-only diff
  is a rename — plan 1's concern, not this arm's.)
- Unpaired `-1` on a registered owner is rejected (physical column removal does not exist).
- Unpaired `+1` (an ADD COLUMN append) is rejected here — ADD COLUMN is `plans/2`'s job; this also
  closes a raw-capi bare-append.

### 3.2 Compensation: unpaired-`+1` `is_create` classification

Plan 1 §3.4 relaxes the compensation homogeneity `debug_assert` (`write_path.rs:754-764`) to the
per-family form (each family internally one-sign OR fully paired). This plan additionally corrects
`is_create` (`write_path.rs:750-752`, currently `any(weight > 0)` bundle-wide): a failed DROP COLUMN
bundle is `{COL pair} + {engine-cascaded IDX -1}`. Under the raw any-positive rule the COL `+1`
classifies it CREATE → DESC restore order + `drain_pending_dir_deletions` (`:790`) →
`remove_dir_all` on the directory of the index the compensation is simultaneously restoring.
Reclassify by **unpaired rows only** (`∃` unpaired `+1`): the failed DROP COLUMN bundle has no
unpaired `+1` → DROP-shaped → ASC restore + discard-dir, preserving the index directory. A pure
rename pair (plan 1) has an empty dir queue and is a no-op either way; a genuine CREATE keeps its
unpaired `+1`.

The restored IDX `+1` re-registers an **empty** master index copy — correct, because
`hook_index_register` skips `backfill_index` on the master and in rollback (`hooks.rs:643`),
master index copies are permanently empty by design, and compensation runs **pre-broadcast**
(`executor.rs:2560-2575`), so no worker ever saw the aborted DROP.

### 3.3 Pair-aware `dropped_indices` from drained broadcasts

The post-fsync unique-filter invalidation list (`dropped_indices` → `unique_filter_remove`,
`executor.rs:2608-2614`) is currently derived from the **client bundle** `families`
(`:2514-2527`, `families.iter().find(|(tid,_)| *tid == IDX_TAB_ID)`). A DROP COLUMN's client bundle
carries **only the COL pair** — no IDX family — so as-is the cascaded index's unique filter leaks.
Derive `dropped_indices` from the **drained broadcasts** (`drained`, `executor.rs:2581`) instead,
which do contain the engine-cascaded IDX `-1` (full payload, from `retract_single_row`); the same
derivation still captures the ordinary DROP INDEX case (that row is in both `families` and
`drained`).

### 3.4 Master sys-table SAL recovery bypasses precheck

Master system-table SAL recovery runs **after** `go_live()` (`apply_context.rs:29-31`;
`recover_system_tables_from_sal`, `runtime/bootstrap.rs:427`) through `ingest_to_family → submit →
precheck_family`, with rejections silently swallowed (`bootstrap.rs:161` tests only `.is_ok()`).
With the new Column arm this would re-judge cascaded COL `-1` groups that replay as independent
entries (children-first) while the owner is still registered. Fix: route this recovery through
`apply_local` (no precheck) exactly like worker `ddl_sync` — the rows are master-validated by
definition — and treat a replay `Err` as fatal (`gnitz_fatal_abort!`, as pass 2 already does at
`bootstrap.rs:230-238`) instead of uncounted. Hooks (which re-run everywhere) remain the
defense-in-depth layer.

## 4. Extending the tick/drive quiesce to comparator-changing ALTERs

Plan 1's DDL handler engages the tick **Quiesce** (`TickTrigger::Quiesce`,
`orchestration/executor.rs:2416-2427`) **and** the transient-drive exclusion
(`_drive_excl = shared.drive_rwlock.write().await`, `:2410-2414`) only when `view_create` is set.
A column-ALTER bundle carries no `VIEW_TAB +1`, so it takes the `TickGate(None)` branch (`:2426`)
with no drive lock and **never quiesces** — a correctness hole for the two comparator-changing ops:
the catalog write lock blocks only master-side readers, not in-flight worker exchanges. A worker
blocked in `do_exchange_wait` (`worker/exchange.rs:33-110`, building `DispatchContext::InEval`)
**defers** the ALTER's `DdlSync` into `exchange.deferred` (`worker/mod.rs:661-677`) while applying
a **subsequent** same-table `Push` **inline** (the `(_, Push)` wildcard arm, `worker/mod.rs:768-780`
→ `handle_push` → `ingest_returning_effective`, `:1113`) — so the push lands in a store whose
descriptor has not yet swapped. A NULL written under the still-`FixedIntNonnull` comparator is
byte-identical to a real `0` in the zeroed payload region and consolidates against it — a silent
Z-set weight error. The window is real at `W ≥ 2` whenever a multi-round exchange
(`query/dag/exec.rs:407-464`; the range-join two-round path `:342-346`) keeps a worker in the
exchange-wait context across the ALTER.

**`do_exchange_wait` has exactly one caller** (`WorkerExchangeCtx::do_exchange`,
`worker/mod.rs:210-214`), constructed by exactly two InEval drivers plus one lock-serialized one:
- the **tick loop** (`evaluate_dag`, `worker/mod.rs:1752`) — closed by the Quiesce (`_tick_gate`);
- a **transient / ad-hoc-query drive** (`drive_view_step`/`handle_backfill`, `:1366`;
  `drive_transient` holds `drive_rwlock.read()` across the whole `emit_groups_await_acks` loop,
  `executor.rs:2183`) — closed **only** by `drive_rwlock.write()`, not the Quiesce (RunTransient
  emission drops its `catalog_rwlock.read()` before awaiting worker ACKs);
- live **CREATE VIEW backfill** (`fan_out_backfill`) — already excluded, it runs under
  `catalog_rwlock.write()` inside `handle_ddl_txn`, mutually exclusive with the ALTER's own write
  lock.

The single-threaded worker means *any* concurrent exchanging work, on any view, blocks it in
`do_exchange_wait` while an unrelated table's `DdlSync`+`Push` interleave — so both InEval drivers
must be excluded.

Fix: compute a `column_alter` flag and take **both** `_drive_excl` and the Quiesce when
**`view_create || column_alter`** — the two gates are already adjacent and in the load-bearing
order (`_drive_excl` before the Quiesce send, so a transient parked in its own source drain cannot
sit ahead of the gate). `column_alter` fires **only** for the comparator-changing ops, detected
purely bundle-locally (no live read, no lock) by reusing plan 1's per-family paired-PK set:

```
// A COL pair on a registered base-table owner whose is_nullable flips 0→1 is exactly a
// DROP NOT NULL / DROP COLUMN that moves the column FixedIntNonnull→Generic — the only shape
// whose comparator changes. is_hidden is a COL_TAB flag absent from SchemaDescriptor, so a
// RENAME COLUMN (is_nullable unchanged) and an already-nullable DROP COLUMN (is_nullable 1→1)
// leave the descriptor `eq` and need no swap — both correctly excluded, no over-quiesce.
let column_alter = col_paired_pks.iter().any(|&col_id| {
    owner_registered_base_table(cat_ptr_raw, owner_of(col_id))
        && col_row(col_id, /*weight*/ -1).is_nullable == 0
        && col_row(col_id, /*weight*/ +1).is_nullable == 1
});
```

The probe reads only bundle rows plus the lock-free single-threaded `dag.tables` map at the handler
top (before the write lock, across the committer-barrier `await`); it can only ever *over*-quiesce,
never under-quiesce — a succeeding ALTER's owner was registered when the flag was computed, table
ids are never recycled, and `PayloadCmpKind` being whole-schema means every comparator change flips
`nullable 0→1`. `hook_column_alter` (§5) still runs for every column pair (it early-outs via
`SchemaDescriptor::eq` when the descriptor is unchanged); the `column_alter` flag gates only the
*quiesce*, not the hook. The lock-held committer barrier (`:2431-2443`) stays `view_create`-only.

Why both gates close the window: (1) `TickTrigger::Quiesce` is dequeued only **after** the prior
`run_tick` returned (the serial `tick_loop_async`, `:625-734`, at `:725`), and
`run_tick`→`emit_groups_await_acks` (`:743-817`) awaits every worker's tick ACK (`:813`), emitted
(`worker/mod.rs:969-973`) only after `evaluate_dag` fully returns — after every nested
`do_exchange_wait` exited *and* `dispatch_deferred` applied any staged `DdlSync`. Once a `Quiesce`
enters the loop (`:692-699`) the whole task blocks on `release.await`, so no new tick-driven
exchange starts for the span. (2) `drive_rwlock.write()` completes only after every in-flight
transient read drops and blocks new ones for the handler's duration. So at `DdlSync`-broadcast time
no worker is exchange-waiting: the swap lands on the `TopLevel` dispatch arm (`worker/mod.rs:660`)
and is applied inline (`cat().ddl_sync`, `:889-911`), not deferred. SAL FIFO then orders the swap
before any post-ALTER push; a pre-swap straggler push can carry only **non-null** values —
`ZSetBatch::validate` (`types.rs:813`) rejects a null bit on a still-NOT-NULL column (`:894-911`,
`test_validate_rejects_null_bit_on_not_null_column`) against the client's pre-ALTER schema — which
sort identically under both comparators (§2).

(Cross-plan note: `drive_rwlock` exists only to gate transient drives; the separate plan retiring
the transient executor deletes the lock wholesale, removing this `column_alter` arm of `_drive_excl`
with it. Until then, this plan must take it.)

## 5. Engine: `hook_column_alter` and the equal-region descriptor swap

New side-effect hook on the `Column` family, run after the cache folds (add it to the
`SysFamily::Column` dispatch arm, `hooks.rs` — beside `apply_col_names_invalidate` /
`apply_fk_constraints` / `apply_needs_lock`). Its trigger is **batch-shape-derived, never
context-derived** — cascaded whole-table COL retractions (DROP TABLE/VIEW) reach this hook on
workers and at replay with no cascade flag available, so the shape is the only reliable signal.
Per distinct owner tid in the batch:

1. Skip unless the owner is registered in `dag.tables` **and** is a base table. Skip any owner whose
   batch rows include an **unpaired `-1`** — the drop-cascade/replay signature (physical column
   removal does not exist); acting on it would rebuild a schema from zero columns. The hook acts
   only on pairs. ("Exactly one operation per ALTER" guarantees an owner's COL rows in one bundle
   are a single pair.) Live CREATE TABLE bundles apply COL_TAB before TABLE_TAB, so the owner is
   unregistered and skipped; at boot replay Table registers before Column
   (`bootstrap.rs:266-270`) and step 3's equality check makes the hook a no-op.
2. **Covering-index cascade (DROP COLUMN only).** If the batch flips `is_hidden 0→1` for a column,
   cascade-retract **only the secondary indexes that cover that column** — enumerate the owner's
   indexes via `indices_by_owner` and retract those whose key columns include the dropped column,
   under `with_cascade_drop`. Do **not** reuse `cascade_retract_indices` (`hooks.rs:321`)
   unfiltered — it drops *all* the owner's indexes. Gate the cascade **solely** on the `is_hidden
   0→1` flip: this is direction-safe (a compensation-reversed `1→0` pair, or a DROP NOT NULL pair,
   does not flip is_hidden → no cascade — correct). It cascades via `submit` →
   `pending_broadcasts.push` (children-appended-after-hooks, so the broadcast order is
   `[covering-IDX -1, COL pair]`), the master drains and broadcasts it once, and it is idempotent
   on worker re-derivation: the worker applies the broadcast IDX `-1` first (removing the index from
   `indices_by_owner`, `cache.rs:293-294`), so when it re-runs this hook `retract_single_row`
   returns `count == 0` and the `if batch.count > 0` guard skips a second `submit` (`hooks.rs:419-422`).
   Bumps the client `index_version` epoch via `apply_index_by_id` so cached `IndexMeta` re-fetches.
   **Do not re-assert the §3.1 forward pair shape here** — compensation re-fires this hook on the
   *negated* pair (`is_hidden 1→0`, `is_nullable 1→0`, in `ctx.in_rollback()`), which matches no
   forward shape; a hard shape-assert would panic during a failed-DROP-COLUMN rollback. The cascade
   (gated on `is_hidden 0→1`) and the swap (gated on step-3 `eq`) are already direction-safe on
   their own, so no shape re-assertion is needed.
3. Rebuild the descriptor: `read_column_defs(owner)` (`registry.rs:71`, includes hidden columns) →
   `build_schema_from_col_defs(defs, pk, cur.dist_prefix_len())` (`registry.rs:103`; iterates **all**
   col_defs with no is_hidden filter, `:116-119`, so the hidden column stays in the descriptor and
   region count is preserved) `.with_replicated(cur.replicated())` (`schema.rs:364`) — the
   distribution prefix and replicated flag live on the registered `TableEntry.schema` (from
   TABLE_TAB flags), **not** COL_TAB, and `build_schema_from_col_defs` alone would silently drop
   them. `dist_prefix_len()` (`schema.rs:459`, private `const fn`) and `.replicated()`
   (`schema.rs:470`) are callable here (same crate). If the result equals the current
   `TableEntry.schema`, stop. `SchemaDescriptor::eq` (`schema.rs:1047-1058`) compares
   `num_columns`, `pk_indices`, and the full column array (type_code + nullable) but **not**
   `replicated`/`dist_prefix_len` — so a rename (names absent from the descriptor) and an
   already-nullable drop end here, while DROP COLUMN and DROP NOT NULL that flip `is_nullable`
   proceed.
4. Publish infallibly through one push-down entry point: `DagEngine::swap_table_schema(tid, desc)`
   updates `TableEntry.schema` and calls `StoreHandle::swap_schema(desc)` →
   `PartitionedTable::swap_schema(desc)` (updating its own copy, `partitioned_table.rs:68`; it
   routes with `self.schema.partition_for_pk` and slices scatter sub-batches with it, `:200,210`) →
   each partition `Table::swap_schema(desc)`. There are **exactly three comparator-schema holders
   inside one partition `Table`**, all of which must swap or a NULL consolidates against a real `0`
   on a path the primary holder doesn't cover:
   - `Table.schema` (`storage/lsm/table/mod.rs:221`) — read-cursor / `into_consolidated` /
     `compare_rows` comparator.
   - `Table.memtable.schema` (`MemTable`, `storage/lsm/memtable/mod.rs:38`) — the **flush**
     consolidation comparator: `as_sorted_mem_batch(&self.schema)` /
     `consolidate_batches(&batches, &self.schema)` (`storage/lsm/memtable/runs.rs:108,128`).
   - `Table.shard_index.schema` (`ShardIndex`, `storage/lsm/shard_index/mod.rs:169`) — the
     **compaction** comparator: `compact_shards(.., &self.schema, ..)`
     (`storage/lsm/shard_index/index.rs:320`).

   Also clear `cached_full_scan` (`table/mod.rs:234`), which would otherwise serve a batch compared
   under the old descriptor. The comparator dispatch (`with_payload_cmp!`, `columnar.rs:320`) reads
   `payload_cmp` from the schema **passed by the caller** at every site, never from a batch's
   embedded schema — so batch-embedded schemas (`Batch.schema`, `in_memory_l0` run batches) are not
   comparator sources and must not be touched. Transient `ReadCursor`/`SortedMemBatch` copies are
   always rebuilt from `Table.schema` at open, so they are not persistent holders (verified: no
   fourth holder exists — WAL, bloom, xor8, shard_reader, and compaction all take schema by
   parameter, and compaction runs inline under the same DDL zone, so no captured schema goes stale).
   Every supported op leaves the region count unchanged, so this is a pure descriptor replacement:
   no run rebuild, no shard reopen, no data motion — old rows stay sorted (§2). Afterwards
   `debug_assert` all holders agree — `TableEntry.schema`, `PartitionedTable.schema`, and every
   partition's three holders — on the descriptor **including `replicated` and `dist_prefix_len`**
   (which `eq` won't check). `swap_schema`/`swap_table_schema` are new; model them on the
   handle-replacement idiom at `storage/lsm/partition_lsn.rs:87,135` (which build a fresh
   `PartitionedTable` and drop it into `entry.handle`) but as an in-place descriptor mutation, since
   there is no data to move. Swap-in-place (not re-registration) is mandatory: `PartitionedTable::new`
   opens from manifests only, and the DDL zone does not checkpoint user memtables, so
   re-registration of a populated table would drop every unflushed row; the `partition_lsn.rs` sites
   operate on **empty** stores by documented contract (`:55`, `:131`).
5. `debug_assert!(dependent views of owner are empty)` — the §1 RESTRICT invariant made
   load-bearing, and the reason no plan-cache invalidation is needed: no compiled circuit scans this
   table. The `vm/exec.rs:94` width assert (gated on `input_batch.schema.is_some()`) stays as the
   backstop.

The hook runs identically on the master (live apply), every worker (`ddl_sync` → `apply_local`),
and boot replay (no-op by step 3). Post-fork the master holds no user partitions; replicated tables
get the swap on every worker via ddl_sync. The DDL zone plus the §4 quiesce guarantee no tick,
scan, or transient drive is in flight during the swap.

## 6. Client: hidden-column DML fixes (required before DROP COLUMN ships)

The invariant "base-table columns are never hidden" holds today; DROP COLUMN is the first thing to
break it, so the client DML paths that assume it must be fixed:

- `dml/insert.rs:186` — the no-column-list arity check counts `schema.columns.len() - is_serial`;
  must exclude hidden columns.
- `dml/insert.rs:491-496` — the explicit-column-list expected set filters only `is_serial`; must
  also filter hidden.
- **Positional remap**: the VALUES loop maps user values by physical column index (`extract_pk_value`
  at `insert.rs:221`, `row[ci]` at `:229`); once a *middle* column is hidden, every later user value
  would shift into the wrong column. Build an explicit visible-position → physical-ci map (skipping
  hidden and SERIAL) and drive both PK extraction and the payload loop from it.
- INSERT row encoding writes NULL (zero value + null bit set) for hidden columns — the physical
  layout still contains them.
- **DML-side wildcard resolution leaks the dropped column.** `exec/batch.rs::resolve_projection`
  (the thin-path resolver, `:107-117`) returns **every physical column including hidden** on both its
  bare-`*` fast path (`is_bare_wildcard_projection`) and its `SelectItem::Wildcard(_)` arm — a
  different, unfiltered path from the view compiler's wildcard expansion (which already skips hidden).
  It backs `SELECT * FROM t` via `AccessPath::ScanAll` (`dml/select.rs:188`, the single most common
  query) **and** `INSERT … RETURNING *` (`dml/insert.rs:250`). After DROP COLUMN this returns the
  dropped (hidden, still-physically-populated) column's pre-drop values in `SqlResult::Rows`. Fix:
  filter `!is_hidden` in `resolve_projection`'s bare-`*`/`Wildcard` arms (mirroring
  `visible_columns()`), and in `WildcardRewrite::rewrite_column` (`ast_util.rs:682-691`, which today
  checks only excludes/rename). gnitz-py masks this incidentally at its boundary
  (`make_shared_batch_data(.., include_hidden=false)` → `visible_columns()`, `gnitz-py/src/lib.rs:963-967`),
  but **gnitz-capi's `gnitz_execute_sql_query` (`gnitz-capi/src/lib.rs:1456-1486`) has no downstream
  filter** — the engine-side fix is the real one.
- The invariant comments (`protocol/types.rs:35,188`, `plan/validate.rs:17`, `dml/overlay.rs:87`,
  `bind/resolve.rs:58-60`) are rewritten to: *base-table hidden columns exist only via DROP COLUMN,
  are always nullable, and are excluded from all name-facing surfaces*. The code beneath them
  (`visible_columns()` at `types.rs:191`, `find_unique_column` at `resolve.rs:61/64`, wildcard/JOIN
  combination) already skips hidden correctly — verified by the new tests, not changed. UPDATE is
  already safe (`resolve_set_target` → `find_unique_column` skips hidden; `build_merged_row`
  carry-through preserves hidden values verbatim, `mutate.rs:96-127`). DELETE is PK-only
  (`retraction_batch` + server-side `Table::retract_pk_bytes` reads the true live bytes).

`plan/alter.rs`'s DROP COLUMN / DROP NOT NULL arms (added by plan 1's dispatch, which rejects them
until this plan ships) resolve the target, run the PK/SERIAL/FK and dependent-view guards, compute
the COL pair (the live row's exact `-1` payload plus the new `+1`), and call the new `gnitz-core`
client methods `alter_drop_column(tid, col_idx)` / `alter_drop_not_null(tid, col_idx)` (next to plan
1's `alter_rename_*`), shipping through `push_ddl` (`client.rs:771-778`).

## 7. Transaction schema pin (single site)

An open client transaction buffers batches shaped by the schema at statement time; a concurrent
column-ALTER from another connection makes a later statement reinterpret an old-shaped buffered batch
under the new schema — a misaligned client-side read no commit-time check catches (a buffered INSERT
makes no server round-trip, `client.rs:387-390`). Every path that reinterprets a buffered batch under
a freshly-fetched schema is **SQL DML only**: `overlay_batch` (`dml/overlay.rs:100,106`, UPDATE/DELETE
via `mutate.rs`), `effective_row` (`overlay.rs:56`, INSERT ON CONFLICT via `insert.rs:384,439`), and
the INSERT buffered reads (`insert.rs:392,443`). Every UPDATE/DELETE/INSERT resolves its target
through `resolve_base_table` (`bind/resolve.rs:208-237`) **before** any of those reinterpretations.

Fix — **one** check at `resolve_base_table`, reusing existing state: `TxnBuffer` stores an owned
`Schema` clone per buffered family (`client.rs:1499`). When the transaction already holds a family
for the resolved tid, compare its stored schema against the freshly-resolved schema with `types_match`
(`protocol/types.rs:277-285`: column count + pk_cols + per-column type_code + nullability; ignores
name); on mismatch, fail the transaction with "schema changed concurrently; transaction rolled back".
Because `types_match` ignores names, a plan 1 rename passes (layout unchanged); DROP COLUMN / DROP NOT
NULL change nullability and abort before any misaligned read. A table the transaction has not buffered
has no stored schema, so a SELECT-only-then-ALTER sequence never spuriously aborts. Raw capi/Python
pushes need no pin: they never client-reinterpret, and commit ships per-family schema blocks the
server validates. Add a small `TxnBuffer` accessor returning the stored schema for a tid.

## 8. Tests

Rust (`crates/gnitz-sql/tests/planner_alter.rs`, extending plan 1's matrix): DROP COLUMN and DROP
NOT NULL plan; every drop-side reject (PK/SERIAL/FK column, multi-column `DROP COLUMN a, b`,
`CASCADE`, `SET NOT NULL`, dependent-view RESTRICT) errors with the documented message; bundle shapes
(COL pair, `-1` payload = live-row payload, pair diffs (a)/(b)) asserted against the built batches.

Rust (engine):
- Column precheck arm + `hook_column_alter` shape-guard: DROP TABLE and DROP VIEW of relations **with
  columns** work end to end on master, workers, and through SAL recovery (regression for the
  cascade-shape panic); an unpaired `+1` (ADD COLUMN attempt) is rejected.
- §3.4: master sys-table SAL recovery applies a DROP-cascade sequence without precheck rejections; an
  injected replay error is fatal, not swallowed.
- §4 quiesce: a `column_alter` bundle (is_nullable flip) takes both `_drive_excl` and the Quiesce
  branch (assert `TickTrigger::Quiesce` sent and `drive_rwlock.write()` acquired); a column **rename**
  and an **already-nullable** DROP COLUMN do **not** (bundle-local trigger excludes them).
- Pair-aware `dropped_indices`: a DROP COLUMN's cascaded index retraction releases its unique filter
  (drained-broadcast derivation, not client-bundle).
- Compensation: a **failed DROP COLUMN** (fault injected after the IDX cascade) restores the index and
  leaves its directory intact (unpaired-row `is_create` regression), does not trip the per-family
  homogeneity assert, and **does not panic when `hook_column_alter` re-fires on the negated pair**
  (rollback-tolerance regression); a failed DROP NOT NULL compensates cleanly.
- `swap_table_schema` push-down (equal-region): descriptor swapped on `TableEntry`, `PartitionedTable`,
  and every partition `Table`'s three holders; `cached_full_scan` cleared; agreement assert covers
  `replicated`/`dist_prefix_len`; DROP NOT NULL on a **replicated** and on a **CLUSTER BY** table
  preserves routing (dist-bits regression); after the swap a written NULL no longer consolidates
  against a real 0.
- Boot replay: a DROP COLUMN survives restart (hidden+nullable column re-registers, comparator is the
  nullable form, cascaded index absent, swap is a no-op).
- Concurrent column-ALTER vs ALTER/DROP: exactly one wins; the loser gets "catalog changed
  concurrently" (payload-equality CAS under the serialized write lock).

E2E (`crates/gnitz-py/tests/test_alter_drop.py`, `GNITZ_WORKERS=4`):
- DROP COLUMN of a **middle** column: wildcard/arity/name resolution and `SELECT *`/`RETURNING *`
  exclude it (thin-path leak regression); INSERT values land in the right columns (positional-remap
  regression); covering index is cascade-dropped and the client's index cache refreshes; explicit
  INSERT naming it errors; a concurrent same-table INSERT during the ALTER (multi-round exchange in
  flight on another view, and separately an in-flight ad-hoc query drive) does not corrupt weights
  (§4 quiesce + `_drive_excl` regression).
- DROP NOT NULL: NULL insert succeeds after, fails before; pre-ALTER non-null rows read back
  correctly; UPDATE/DELETE of a pre-ALTER row retracts the right row.
- RESTRICT: DROP COLUMN / DROP NOT NULL on a table with a dependent view errors cleanly and leaves the
  catalog unchanged.
- Transaction pin: `BEGIN; INSERT` (buffered, no round-trip) on conn A, DROP COLUMN on conn B, next
  UPDATE/DELETE on A fails with the rollback error; a pure `SELECT` on A of an untouched table does
  not spuriously abort.
- `make verify` and `make e2e WORKERS=4` green.
