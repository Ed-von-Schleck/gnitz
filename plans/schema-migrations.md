# Schema Migrations

## Status

| Version | Status       | Scope                                                                            |
|---------|--------------|----------------------------------------------------------------------------------|
| V1      | **done**     | `CREATE/DROP TABLE`, `CREATE/DROP INDEX`, direct-apply (no staging)              |
| V1.5    | **partial**  | `CREATE/DROP VIEW` (plain SELECT) done; `__mig_` staging + `mig_gc_task` pending |
| V2      | pending      | Modifies via backfill + FK rewire + size guardrails                              |

V1 shipped as an MVP. V1.5 so far: views land in migrations via a
pre-compiled `CircuitGraph` embedded in the AST; Phase 3 applies the
8 sys-table deltas atomically using the same direct-apply block as
V1. Staging, GC, and complex view shapes remain. See **Deviations**
below for concrete differences between the code and the original
design.


## Context

GnitzDB has no in-place schema evolution today. `CREATE TABLE` /
`CREATE VIEW` / `CREATE INDEX` add objects; `DROP` removes them;
nothing alters columns, changes types, or renames. This plan
introduces git-style declarative migrations: content-addressed
commits, server-computed diffs, linear chain, strict per-version
physical immutability.


## Model

A **migration** is an atomic catalog commit moving the database to a
new declarative desired-state (SQL). Commits form a linear chain:

```
hash = xxh3_128(parent_hash || bincode(desired_state_ast) || author || message)
```

The **canonical form** is the shared AST (`TableDef`, `ColumnDef`,
`ViewDef` including `circuit: CircuitGraph`, `IndexDef`, `ViewDep`)
in `gnitz_wire::migration` + `gnitz_wire::circuit`, serialised with
`bincode::config::legacy()` after sorting top-level Vecs by
fully-qualified name. Whitespace-equivalent *SQL* does not guarantee
byte-equivalent canonical form today because `ViewDef.sql` stores
raw text — V1.5 follow-up should normalise at parse time or persist
a parsed AST.

Object references inside the canonical form are by **name**, not
`table_id` — ids are allocation artefacts that change across
migrations. The `CircuitGraph` inside `ViewDef` is the sole exception:
its `sources`/`dependencies` fields carry `table_id`s, resolved at
compile time against the live catalog or (future) the same-migration
pre-allocation map.

**Stability contract.** Every migration row carries a
`format_version: U64` column (current value: **2** — V1.5 bumped
from 1 to accommodate the new `ViewDef.circuit` field). Changes to
struct layout, field order, or the hash framing require bumping
`format_version` and teaching the decoder to dispatch. V2's
transform/rename annotations will require the next bump. Pre-v2 DBs
surface a clear "wipe the data dir" error at
`apply_migration_row` — acceptable because migrations were only
days old at bump time.

**Physical-immutability invariant** (narrower than "nothing about a
`table_id` ever changes"): a live `table_id`'s *physical layout* —
shard bytes, column offsets / null bitmap / PK index in compiled
circuits, `SchemaDescriptor` — never changes. Metadata pointers (FK
targets, dep rows) are not embedded in compiled artefacts and remain
updatable in place.

Modifying a table therefore allocates a fresh `table_id`, backfills,
recompiles dependent views against the new id, rebinds the logical
name, and drops the old id. Compiled circuits and shards for retired
versions are reaped; `sys_migrations` is the sole historical record.

**Sync-only for V1+V2.** Migrations apply under `sal_writer_excl`:
writes quiesce, the migration runs end-to-end, the lock releases.
Async / online migrations are explicitly deferred.


## Catalog additions

One system table: **`sys_migrations`** (done, `MIGRATIONS_TAB = 8`).

| Column                    | Type      | Notes                                      |
|---------------------------|-----------|--------------------------------------------|
| `hash`                    | U128 (PK) | `xxh3_128(parent || canonical || ...)`     |
| `parent_hash`             | U128      | Zero for genesis                           |
| `author`                  | STRING    |                                            |
| `message`                 | STRING    |                                            |
| `created_lsn`             | I64       | SAL LSN at commit (placeholder in V1)      |
| `desired_state_sql`       | STRING    | Raw input, audit/display only              |
| `desired_state_canonical` | STRING    | **Hex-encoded** bincode AST blob           |
| `format_version`          | U64       | Current: 2 (V1.5 added `ViewDef.circuit`)  |

No per-object snapshot rows. Historical queries hex-decode then
`bincode::serde::decode_from_slice` the canonical bytes on demand
(`executor::read_migration_canonical`).


## Wire and SAL: master-computed batches over standard `FLAG_DDL_SYNC`

The master is the sole source of truth. It computes the diff, builds
all swap batches, and broadcasts each as a standard `FLAG_DDL_SYNC`
under a single `sal_writer_excl` hold + single `fsync()`. Workers are
passive consumers — their existing `ddl_sync` applies each batch via
the per-table hooks (`on_table_delta`, `on_view_delta`,
`on_index_delta`, plus the new `on_column_delta` for V2). No
`on_migrations_delta` hook, no AST shipped to workers, no
determinism-coupling between processes, no `bincode`-stability across
the wire — the wire carries only the ingest batch itself.

**Atomicity.** `handle_system_dml` already holds `sal_writer_excl`
across one synchronous `broadcast_ddl` then submits fsync and
releases the lock before awaiting. The SAL writer mutex guards
synchronous mmap writes only — no `.await` inside. Extend the same
pattern to N calls under one hold:

```
fsync_fut = {
    let _sal_excl = sal_writer_excl.lock().await;
    for (tid, batch) in swap_batches { broadcast_ddl(tid, batch); }
    reactor.fsync(sal_fd)               // SQE submit, no await
};
fsync_fut.await;
```

If master crashes before fsync completes, the SAL is recovered to the
last successful fsync — all N entries lost atomically. If fsync
succeeds, all N are durable. Workers may have applied earlier entries
in memory before crash; on master restart they re-fork from the
recovered (pre-migration) catalog state via the existing bootstrap
path. Worker hooks are idempotent under re-apply.

**Master vs worker ingest paths.** After `fork()`, workers share the
master's sys-table WAL fds (`bootstrap.rs:trim_worker_partitions`
skips sys tables). Master's `ingest_to_family` writes the WAL;
worker's `ddl_sync` uses `ingest_batch_memonly_from_regions` —
multi-writer access to the same WAL is unrecoverable corruption. No
new memonly variants required; the standard paths already do the
right thing per side.

### Worker dispatch

No new wire-flag arms. `FLAG_DDL_SYNC` already defers in
`do_exchange_wait` and drains via `dispatch_deferred`, which calls
`ddl_sync`. The swap DDLs flow through this path unchanged.

### Client cache invalidation

Deferred. No client-side `name_to_id` cache and no
`CONTROL_SCHEMA_DESC` change. If a cache is added later, the standard
`STATUS_ERROR` retry pattern suffices — migrations are rare, so the
one failed request per client after a migration is negligible.

### Client API

```rust
impl Connection {
    pub fn push_migration(
        &self,
        parent_hash: u128,
        desired_state_sql: &str,
        author: &str,
        message: &str,
        canonical: &[u8],     // gnitz_wire::migration::canonicalize(parsed AST)
    ) -> Result<u128, ClientError>;
}
```

The Python wrapper calls `gnitz_sql::migration::parse_desired_state`
+ `canonicalize` to produce `canonical`. Canonical bytes travel the
wire hex-encoded; the hash is computed over **raw** bytes on both
ends.

### Inbound dispatch

`handle_message` routes migration pushes via a branch placed BEFORE
the `migration_in_flight` gate and BEFORE the generic DDL path:

```rust
if target_id == MIGRATIONS_TAB_ID && has_batch && batch_count > 0 {
    apply_migration(shared, fd, client_id, decoded).await;
    return;
}
```

Defence-in-depth: `handle_system_dml` rejects non-empty batches for
`SYS_MIGRATIONS_TAB_ID` with a specific error — migrations MUST go
through `apply_migration`, never a raw catalog insert.


## Apply algorithm: Stage-and-Swap (V2 shape)

Below is the target shape for V2 (backfill) and V1.5-remaining
(`__mig_` staging for views + prep for V2). **V1 + V1.5-as-shipped
collapse this into a single write-lock block with no staging** — see
"V1 MVP direct-apply path". Staging resumes when V2 backfill lands
or a compelling V1.5 use case forces it (e.g. enabling complex view
shapes that read from both live and same-migration sources).

Orchestration lives in the **async executor**. Concurrent migrations
serialise on `shared.migration_in_flight: AtomicBool`. While set,
`handle_message` rejects external DML AND external DDL pushes (the
latter is required so user `CREATE TABLE` etc. cannot race the
orchestrator's write-lock acquisition in Phase 3). Internal system
tasks (committer, relay, tick) use `sal_writer_excl` which is
orthogonal.

```
async fn apply_migration(shared, decoded) {
    // Scope guard: CAS false->true; clears on any exit incl. panic.
    let _guard = MigrationInFlightGuard::acquire(&shared)
        .ok_or("another migration is in flight; retry shortly")?;

    // ---------------- Phase 1: drain + validate + diff ----------------
    // Drain in-flight DML AND the tick loop. Committer barrier mirrors
    // the DDL path; tick drain uses TickTrigger::Drain (same as SCAN).
    // Both are load-bearing: an auto-tick emitted just before the flag
    // is set can produce FLAG_EXCHANGE relays during Phase 1/2/3, and
    // Phase 3's drop of a view whose relay is still in flight makes
    // prepare_relay panic inside guard_panic against the dropped vid —
    // caught and logged, but the exchange result is silently dropped.
    send CommitRequest::Barrier { done } on committer_tx, await done;
    send TickTrigger::Drain  { tids: vec![], done } on tick_tx, await done;

    let diff = {
        let _r = shared.catalog_rwlock.read().await;
        verify row.hash == xxh3_128(row.parent_hash || canonical || author || message)
        verify row.parent_hash == cat.current_migration_hash   // provisional
        parent_ast = decanonicalize(sys_migrations row for parent_hash)
        new_ast    = decanonicalize(row.desired_state_canonical)
        raw_diff   = diff_by_name(parent_ast, new_ast)

        // V2 size guardrail.
        for t in raw_diff.modified:
            if cat.row_count(t.old_id) > V2_MAX_MODIFY_ROWS:
                return Err("table too large for sync modify")

        diff = expand_modified(raw_diff)       // modify -> drop-and-create

        // Transitive drop-validation: closure(diff.dropped, sys_view_deps)
        // ⊆ diff.dropped.

        // Topological sort. Wrong order in Phase 3 -> drop before child
        // (fk_children_of errors) OR table insert before columns
        // (read_column_defs silently skips registration). Either failure
        // aborts the server inside guard_panic. Exhaustive unit tests on
        // the topo sort (cycles, diamonds, view-on-view, FK
        // parent-before-child, mixed drop+create) are in
        // gnitz_wire::migration::tests and MUST be extended when new
        // edge types are added.
        topo_sort_diff(&mut diff)?;
        diff
    };

    // ---------------- Phase 2: universal stage + backfill ----------------
    // NO catalog lock, NO sal_writer_excl. Everything runs through
    // standard DDL + committer paths under __mig_<hash>_<name>
    // temporary names. Pure creates and modifies stage identically;
    // Phase 3 then becomes a pure rename+drop engine.
    //
    // Staged directories keep their __mig_<hash>_<name>_<tid> names
    // permanently (on-disk path doesn't change at commit). The
    // directory-column reads in on_table_delta / on_view_delta (V1
    // prereq, done) preserve the staged path across rename.
    //
    // AST rewrite (before any staging). `diff.affected_views` is the
    // full transitive closure of views reachable through
    // sys_view_deps from any object in diff.created ∪ diff.modified
    // (for V2→V1→T with only T modified, closure includes both V1
    // and V2). All three sets stage under __mig_<hash>_ names.
    //
    // For every object in diff.created ∪ diff.modified ∪
    // diff.affected_views, rewrite name-references so any dependency
    // that is ALSO being staged resolves to its __mig_<hash>_ name.
    // Cascade-staged but otherwise-unmodified views MUST be rewritten
    // too: a staged __mig_V2 whose SQL still names live V1 would
    // evaluate through the pre-migration V1 (which still reads old
    // T), collapsing the staging isolation.
    //
    // TWO classes must be rewritten: (a) view query FROMs and joins,
    // (b) FK REFERENCES clauses on CREATE TABLE columns. Missing (b)
    // binds __mig_Orders.fk_table_id to the OLD Users id (the planner
    // resolves by name at parse time); Phase 3's drop of old Users
    // then trips fk_children_of → guard_panic → server crash.

    for tbl in diff.created ∪ diff.modified:
        standard_create_table("__mig_{hash}_{tbl.name}", rewritten_tbl_ast).await

    for view in diff.created ∪ diff.modified ∪ diff.affected_views:
        standard_create_view("__mig_{hash}_{view.name}", rewritten_view_sql).await

    // Initialise traces of newly-staged join views for their UNMODIFIED
    // sources. `on_view_delta` skips backfill_view when
    // `view_needs_exchange` is true (join views co-shard via
    // FLAG_EXCHANGE). Today's code fills this only at bootstrap.
    // Runtime staging needs the same: without it, a staged join
    // __mig_V that reads an unmodified U sees U's data as ∅ — silent
    // half-data loss.
    //
    // Targeted backfill (no wire changes). `handle_backfill` today
    // ignores the view_id master packs into seek_pk_lo and calls
    // evaluate_dag(source) — fans the scan delta to every dependent
    // of source via dep_map. At runtime this would double the live
    // sibling V's trace. Fix: decode seek_pk_lo as target view_id in
    // handle_backfill, then extend evaluate_dag_multi_worker with
    // `target_view_id: Option<i64>`:
    //   - When Some(v), gate ONLY the initial dep_map fan-out to
    //     vid == v.
    //   - DO NOT gate the downstream cascade push. Staged views can
    //     chain (__mig_V → __mig_W); gating cascade would leave
    //     __mig_W's trace empty. Cascade is naturally sibling-safe —
    //     dep_map[__mig_V] contains only __mig_ deps.
    //   - Bootstrap passes None; Phase 2 passes Some(__mig_V.id).
    for view in diff.created ∪ diff.modified ∪ diff.affected_views:
        if dag.view_needs_exchange(__mig_V.id):
            for src_id in dag.get_source_ids(__mig_V.id):
                if src_id is NOT a migrated object:
                    fan_out_backfill(__mig_V.id, src_id).await

    for tbl in diff.modified:
        old = fan_out_scan_async(tbl.old_id).await?
        new = tbl.transform_plan.apply(old)            // master-local
        commit_push("__mig_{hash}_{tbl.name}", new).await?
        // Normal DAG propagates across FLAG_EXCHANGE into staged views.

    // Create staged indices AFTER modified-table data is populated,
    // so backfill_index's HashSet uniqueness check runs against the
    // full transformed dataset. Creating them on empty __mig_ tables
    // would make the check trivially pass; the DAG-cascade path from
    // commit_push into index_circuits reaches
    // ingest_batch_from_regions without re-validation, so duplicates
    // introduced by a lossy CAST or default-value collision would
    // land in a unique index and swap into the live catalog
    // unchecked.
    //
    // Pure-create tables are empty so order is irrelevant for them;
    // placing the whole index loop here keeps a single code path.
    for idx in diff.created ∪ diff.modified:
        standard_create_index(
            "__mig_{hash}_{idx.name}",
            owner = "__mig_{hash}_{idx.owner_name}",
            ...
        ).await

    // Backfill failure (e.g. VARCHAR->INT cast on a bad row) surfaces
    // here, BEFORE Phase 3 touches the SAL. Clear migration_in_flight,
    // return error; __mig_<hash>_* orphans get GC'd on boot.

    // ---------------- Phase 2→3 relay fence ----------------
    // Phase 2's commit_push and fan_out_backfill produce FLAG_EXCHANGE
    // replies that the relay_loop processes asynchronously under
    // catalog_rwlock.read(). If Phase 3 acquires catalog_rwlock.write()
    // with stale PendingRelay messages still queued, the write lock
    // parks them; when Phase 3 releases, those relays wake against a
    // mutated catalog. For a vid Phase 3 dropped, prepare_relay's
    // tail call to get_schema_desc panics inside guard_panic — caught
    // as a warning, but the result is silently dropped.
    //
    // Fix: before Phase 3's write lock, fence the relay channel via
    // the `PendingRelay::Fence { done }` variant (shipped in V1,
    // exercised but no-op because V1 MVP has no Phase 2 activity).
    // No deadlock window: by Phase 1's barriers + migration_in_flight,
    // no other task is queued for catalog_rwlock.write() when the
    // fence is processed.
    send PendingRelay::Fence { done } on shared.relay_tx, await done;

    // ---------------- Phase 3: master-computed batches ----------------
    // Master alone computes the swap. Workers receive standard DDLs.
    //
    // Capture dropped directories BEFORE the swap — once the
    // SYS_TABLES -1 fires, on_table_delta calls
    // dag.unregister_table(tid) and retracts the persisted row, so
    // the directory column is unreadable post-swap.
    //
    // The read lock scopes only to the snapshot + batch construction;
    // we do NOT hold any catalog lock across the broadcast block
    // (which takes catalog_rwlock.write() itself).
    let (swap, dropped_dirs) = {
        let _r = shared.catalog_rwlock.read().await;
        // TOCTOU re-check before any SAL write. Stale parent_hash →
        // abort; __mig_* orphans reaped by mig_gc_task.
        if row.parent_hash != cat.current_migration_hash {
            return Err("stale parent_hash; rebase");
        }
        let diff = recompute_diff(cat, &row.desired_state_canonical);
        let cascade = cat.collect_cascade(&diff, renames);
        let dirs = cat.collect_dropped_directories(&cascade);
        let swap = cat.build_swap_batches(&cascade, &row);
        (swap, dirs)
    };

    // Apply + broadcast under catalog write lock + sal_writer_excl.
    // sal_writer_excl is held only across the synchronous broadcasts +
    // fsync SQE submit; the fsync future is awaited OUTSIDE the lock
    // (matching handle_system_dml — concurrent benign SAL writers
    // observe the mutex available within microseconds).
    let fsync_fut = {
        let _w = shared.catalog_rwlock.write().await;
        // Apply each batch to the master's local catalog (durable WAL
        // path) before broadcasting, so the master's in-memory state
        // and SAL stay aligned.
        for (tid, batch) in &swap {
            cat.ingest_to_family(*tid, batch)?;
        }
        let _sal_excl = shared.sal_writer_excl.lock().await;
        for (tid, batch) in &swap {
            dispatcher.broadcast_ddl(*tid, batch)
                .unwrap_or_else(|e| gnitz_fatal_abort!("partial swap: {}", e));
        }
        // Append the sys_migrations row last so workers process it
        // after the swap is applied. The fire_hooks arm for
        // MIGRATIONS_TAB_ID updates `current_migration_hash` (no
        // swap derivation, no side effects).
        cat.ingest_to_family(SYS_MIGRATIONS_TAB_ID, &row_batch)?;
        dispatcher.broadcast_ddl(SYS_MIGRATIONS_TAB_ID, &row_batch)
            .unwrap_or_else(|e| gnitz_fatal_abort!("row broadcast: {}", e));
        cat.current_migration_hash = row.hash;
        shared.reactor.fsync(shared.sal_fd)    // SQE submit only
    };
    let fsync_rc = fsync_fut.await;
    if fsync_rc < 0 { gnitz_fatal_abort!("SAL fdatasync failed"); }

    // ---------------- Phase 4: cleanup ----------------
    // V1 MVP does NOT collect per-broadcast ACKs — the fsync is the
    // synchronisation point (matches handle_system_dml). Workers may
    // still hold memtable handles or be mid-compaction on the old
    // table_id directories, but on Linux unlink-while-open is safe
    // (fds keep data readable until closed; new opens fail, which
    // workers handle via the drop branch of on_table_delta).
    //
    // If V1.5/V2 adds explicit ACK collection, it goes HERE:
    //   await collect_all_worker_acks(req_id);
    for dir in dropped_dirs { rmtree(dir); }
    send_ok_response(client)
}
```

### V1 MVP direct-apply path (done; V1.5 views extend it)

Phase 2 is a no-op; Phase 3 builds `build_{create,drop}_{table,view,index}_batches`
with FINAL names, ingests + broadcasts each under one
`sal_writer_excl` + single fsync, then appends the sys_migrations
row. V1.5 plugged view creates/drops into the existing loop; the
block structure is unchanged.

Safe for pure-create/drop because the commit is a single SAL
transaction — pre-fsync crash rolls everything back. V2's backfill
forces the Phase-2 / Phase-3 split: its Phase-2 work is
non-idempotent and can fail, and you want the failure to leave
orphans, not a half-applied catalog.

### V1.5 view create/drop (done)

Client path: PyO3 `push_migration` → `has_create_view(sql)` scan →
`parse_desired_state_with_circuits(sql, client, default_schema)` in
`gnitz-sql/src/migration.rs`, which:
1. Calls `parse_desired_state` (empty circuits).
2. For each `CREATE VIEW`, calls
   `gnitz_sql::planner::compile_view_for_migration(client,
   view_schema, stmt)` — uses the view's own schema as binder
   default so qualified FROMs resolve.
3. Converts `gnitz_core::CircuitGraph` → `gnitz_wire::CircuitGraph`
   via `core_to_wire_circuit`.
4. Populates `state.views[i].circuit`.
5. Client hashes the full AST (circuits included).

Server path: Phase 1 validates every created view's circuit via
`CatalogEngine::validate_view_circuit` BEFORE any SAL touch.
Phase 3 emits view creates + drops via new helpers:

**`build_create_view_batches(v)` emission order (8 batches):**
1. `sys_columns` (+1 per output col; `OWNER_KIND_VIEW`).
2. `sys_view_deps` (+1 per dep_tid).
3. `sys_circuit_nodes` (+1 per node).
4. `sys_circuit_edges` (+1 per edge).
5. `sys_circuit_sources` (+1 per source binding).
6. `sys_circuit_params` (+1 per param; `str_value=NULL`; see
   deviation 14 re: `const_strings`).
7. `sys_circuit_group_cols` (+1 per group col).
8. **`sys_views` (last)** — triggers `on_view_delta` →
   `dag.register_table` + `ensure_compiled`. Ordering is
   load-bearing: the hook calls `read_column_defs(vid)` and
   `dag.get_source_ids(vid)`, both of which read state from batches
   1-5.

**`build_drop_view_batches(schema, name)` retracts in reverse:**
circuit (5 tables via `retract_rows_by_pk_hi`) → view_deps →
columns (via `build_retract_column_records` with `OWNER_KIND_VIEW`)
→ sys_views (via `retract_single_row`). Returns `(vid, batches,
Option<PathBuf>)` so `apply_phase3` can rmtree the directory after
fsync.

Both helpers slot into `apply_phase3`'s existing
`ingest_to_family + broadcast_ddl` loop — no new wire path.

### Building swap batches (`build_swap_batches`)

Master-local helper (`catalog::migration::build_swap_batches`).
Produces N batches in a fixed emission order (see below). Builds
`-1` retractions via pure builders to guarantee byte-exact payload
cancellation (consolidation groups by exact (PK, payload); a
mismatched `-1` accumulates ghost rows):

- `retract_single_row(table, schema, pk_lo, pk_hi)` — seek by PK,
  copy stored payload with weight=-1. Single-row drops + `-1` half
  of rename pairs.
- `retract_rows_by_pk_hi(table, schema, pk_hi)` — scan contiguous
  rows sharing pk_hi, emit -1 per row. SYS_VIEW_DEPS + 5×
  SYS_CIRCUIT_*.
- `build_retract_column_records(owner_id, kind)` — reads
  sys_columns for owner_id, emits -1 for each column without
  ingesting.

All three are `pub(super)` in `catalog/mod.rs` (done).

`+1` rename rows are appended to the SAME `Batch` as the paired `-1`
(preserving the rename fast-path's same-batch invariant) via
`BatchBuilder`. The master cursor-seeks the stored row, reads every
column through typed accessors (`cursor_read_u64` /
`cursor_read_string` / …), then builds both rows: `-1` with the
stored values verbatim, `+1` with the stored values EXCEPT for the
renamed columns:
- SYS_TABLES rename: `name`
- SYS_VIEWS rename: `name` + `sql_definition`
- SYS_INDICES rename: `name`

Every other column — `directory` (tables/views), `cache_directory`
(indices, always `""` per `create_index`), `schema_id`,
`pk_col_idx`, `flags`, `created_lsn`, `owner_id`, `is_unique`,
`source_col_idx` — copied verbatim.

**Blob-safety invariant.** Do NOT clone a `-1` batch and overwrite
its byte buffers directly. STRING cells whose length exceeds
`SHORT_STRING_THRESHOLD` (12 B) store an offset into `Batch.blob`;
overwriting the 16-byte German-String struct in `col_data_mut`
without allocating new blob bytes and recomputing the offset
corrupts the heap on deserialisation.
`BatchBuilder::put_string` allocates into the builder's blob and
emits the correct offset; `copy_cursor_row_with_weight` — the
primitive under `retract_single_row` — relocates via
`relocate_german_string_vec` for the verbatim-copy path.

**Load-bearing.** The physical table directory keeps its
`__mig_<hash>_<name>_<tid>` prefix across the rename (no filesystem
work). Synthesizing `.../{schema}/{name}_{tid}` from new_ast would
make `Table::new` on restart open the wrong path, silently create an
empty table, and lose every post-commit row. `on_table_delta` must
also READ directory from the batch column instead of recomputing —
V1 prereq, done.

Stale `__mig_` in `sql_definition` would NOT crash replay
(view compilation reads `sys_circuit_*`, not `sql_definition`) but
is visible to client reads — keep clean.

### Emission order

Each step is a single `Batch` whose `-1`/`+1` rows land together
(preserving the rename fast-path's same-batch invariant):

1. **SYS_INDICES** — `-1` dropped + rename pair.
2. **SYS_COLUMNS** — `-1` for every column of dropped **tables and
   views** (both register columns via `write_column_records`). In-place
   `-1`/`+1` `fk_table_id` rewires for FK-sibling rows (V2; column
   ownership keyed by `owner_id`).
3. **SYS_TABLES** — `-1` dropped + rename pair. `+1` `directory`
   preserved verbatim.
4. **SYS_VIEWS** — `-1` dropped + rename pair. `+1` `name` +
   `sql_definition` overwritten.
5. **SYS_VIEW_DEPS + 5× SYS_CIRCUIT_*** — `-1` per dropped vid via
   `retract_rows_by_pk_hi`. All six tables pack `view_id` into
   `pk_hi`. Raw SYS_VIEWS `-1` bypasses `drop_view()` — the only
   caller of `retract_circuit_graph` — so without this step orphans
   leak permanently. No `+1` rows: staged `__mig_V`'s dep/circuit
   rows were written during Phase 2 with the correct vid already.
6. **SYS_SEQUENCES** — `-1`/`+1` carrying the master's actual
   `next_table_id` HWM. V1 MVP extends this to also cover
   `SEQ_ID_INDICES` because pure-create indices allocate their
   `idx_id` during Phase 3 and must be durable. `FLAG_ALLOCATE_*_ID`
   bumps `next_*_id` and calls `advance_sequence` locally without
   broadcast or fsync; Phase 2/3 allocations become durable only via
   this batch. The `-1` payload is the cursor-seeked currently-
   stored value; `+1` is `cat.next_*_id - 1`. Master is the sole
   authority — workers never derive these values.

### Lock hygiene

- **Throughout**: `migration_in_flight` atomic is set; gates both
  external DML and external DDL.
- **Phase 1**: `catalog_rwlock.read()` for diff snapshot, released
  before Phase 2.
- **Phase 2**: no catalog lock, no `sal_writer_excl`. `relay_loop`
  acquires `sal_writer_excl` freely for `FLAG_EXCHANGE_RELAY`
  emissions triggered by Phase 2 pushes.
- **Phase 2→3 fence**: no catalog lock (uses the relay channel).
- **Phase 3**: `catalog_rwlock.write()` for the apply + broadcast
  block. `sal_writer_excl` acquired inside, held across all N
  broadcasts + fsync SQE submit, released before fsync `.await`.
- **Phase 4**: no catalog lock.


## Required catalog changes

### Rename fast-path (done; unexercised until staging lands)

`on_table_delta` / `on_view_delta` / `on_index_delta` detect same-PK
`-1`/`+1` pairs via `detect_rename_pks` and apply only the +1's name
remap. Registry mappings updated in memory; `PartitionedTable`,
memtable, compiled plan, directory preserved. Idempotent on replay.
Load-bearing for views/indices (teardown loses view in-memory data;
index rebuild stalls the cluster). V1.5 direct-apply doesn't
exercise it because views get final names directly.

### Ownership-guarded `name_to_id.remove` (done)

Drop hooks only erase a `name_to_id` / `index_by_name` entry if it
still points to the id being dropped. Prevents a same-batch
`-1 id=42` from clobbering a fresh entry inserted moments earlier
by the rename pair's `+1`.

### Directory-column reads (done)

`on_table_delta` reads `TABLETAB_COL_DIRECTORY`; `on_view_delta`
reads `VIEWTAB_COL_CACHE_DIRECTORY`. Load-bearing for staged
`__mig_<hash>_` paths; synthesised fallback preserved for legacy
empty-column rows.

### `on_column_delta` hook (V2)

`fire_hooks` has no arm for `COL_TAB_ID` in V1. V1 doesn't need one:
`on_table_delta`'s drop branch handles FK teardown for dropped
tables, and `wire_fk_constraints` handles fresh creates by reading
just-ingested `sys_columns` rows.

V2's in-place FK rewires need a hook. It is **stateless** (no `-1`/
`+1` pairing): collect unique `owner_id`s from the batch, and for
each:

1. Clear that `owner_id`'s contribution from `fk_constraints` and
   `fk_parent_map`.
2. Call `read_column_defs(owner_id)` against the just-ingested state.
3. Call `wire_fk_constraints(owner_id, &defs)`.

Step 1 is **mandatory**: `wire_fk_constraints` only adds to
`fk_parent_map`, never removes; skipping cleanup leaves ghost entries
under the OLD parent's key, and a subsequent drop of the old parent
calls `fk_children_of`, finds the ghost, returns `"Integrity
violation"` → `guard_panic` aborts the server.

Fires **once per batch** (per the existing per-batch hook contract),
not per row, so a 5-column add re-runs the clear+rewire once. The
emission order pre-creates all tables (Phase 2 has fully created
every `__mig_<hash>_<tbl>` before Phase 3 starts), so when SYS_COLUMNS
deltas arrive at workers they reference table ids already present in
`dag.tables` — no missing-target hazard.

### `handle_backfill` targeting (V2)

Today's `handle_backfill` ignores the view_id master packs into
`seek_pk_lo` and calls `evaluate_dag(source_tid)`, fanning the scan
delta to every dependent of source via `dep_map`. Harmless at
bootstrap (no live views have data) but catastrophic at migration
runtime: `fan_out_backfill(__mig_V, U)` would double the live sibling
`V`'s trace.

Required:

- `worker.rs:handle_backfill` decodes `seek_pk_lo` as the target
  view_id (0 preserves bootstrap's "all dependents" path).
- `dag.rs:evaluate_dag_multi_worker` gains
  `target_view_id: Option<i64>`. When `Some(v)`, gates **only the
  initial fan-out** on `vid == v`. Downstream cascade stays
  unrestricted — staged chains (`__mig_V → __mig_W`) must cascade or
  `__mig_W`'s trace is empty; sibling-safety is natural because
  `dep_map[__mig_V]` only contains `__mig_` dependents.
- `master.rs:fan_out_backfill` already packs `view_id` — no
  master-side change.


## `current_migration_hash` (done)

`CatalogEngine.current_migration_hash: u128` (zero for fresh DB).
Populated by `replay_migrations_head` which scans `sys_migrations`
and picks `max(created_lsn)`. Scan-only — there is no
`on_migrations_delta` hook; the sys_tables / sys_views memtables
already contain post-swap state via `Table::new`.

Maintained live: Phase 3 sets it inside the write-lock block after
applying swap batches, before broadcasting the sys_migrations row.
Worker side: `fire_hooks` arm for `SYS_MIGRATIONS_TAB_ID` updates
the worker's local `current_migration_hash` from the broadcast row.

Exactly one head — no branches. Fresh DB stays zero; first
migration's `parent_hash=0` passes TOCTOU.


## Recovery (done)

N swap batches are made durable atomically by Phase 3's single fsync.
Pre-fsync crashes lose all; post-fsync crashes preserve all.

- **`replay_catalog`** loads flushed sys-table state and fires the
  standard delta hooks. `replay_migrations_head` sets
  `current_migration_hash` from `max(created_lsn)` in sys_migrations.
- **`recover_system_tables_from_sal`** replays unflushed
  `FLAG_DDL_SYNC` entries pre-`fork()` on master only; workers
  inherit recovered state via COW. sys_migrations entry comes last;
  its replay updates `current_migration_hash`.
- **Post-flush / pre-SAL-reset window**: replay may re-apply
  post-swap state. Hooks tolerate via check-and-skip; memtable
  weights accumulate ghosts but registry stays correct.


## Crash recovery & GC

- **Pre-SAL crash**: pre-migration state intact. Any staged objects
  (V1.6+) are orphans → `mig_gc_task`.
- **Crash between fsync and rmtree**: committed state correct; old
  dir is the orphan → startup sweep (done) removes it.
- **Worker mid-replay crash**: SAL replay resumes at `read_cursor`;
  `ddl_sync` is idempotent.

### Startup sweep (done)

`startup_directory_sweep` runs after `replay_catalog` and before
reactor wire-up. Builds a live set from `dag.tables[*].directory` +
`sys_indices` owner_dir/idx_<id>; whitelists `<base>/_system_catalog`;
`rmtree`s everything else. Full-path matching — no filename parsing.
Also catches `__mig_<hash>_` orphans once staging lands (aborted
Phase 2 never registered the directory).

### `mig_gc_task` (V1.5+)

Phase 2 aborts leave `__mig_<hash>_*` objects. `replay_catalog`
re-registers them (they made it into the master's sys_tables
memtable), pinning them indefinitely unless we reap. **No hash check
needed**: a successful Phase 3 renames every `__mig_<hash>_X` away
from its `__mig_` name, so any name in `cat.name_to_id` at any boot
starting with `__mig_` is, by construction, an aborted leftover.

**Safety against user collision**: `validate_user_identifier`
already rejects ANY identifier starting with `_`, including
`__mig_*`. A user cannot manually create a table the GC would reap.

**Cannot run from inside `replay_catalog`.** `replay_catalog` runs
before workers are wired up. Pattern:

1. `new_engine` spawns a one-shot `mig_gc_task` alongside reactor
   tasks.
2. Once the reactor is alive, the task:
   (a) acquires `catalog_rwlock.read()` briefly,
   (b) snapshots `__mig_*` entries from `cat.name_to_id` into a Vec,
   (c) drops the read lock,
   (d) iterates the snapshot and issues standard `drop_table` /
       `drop_view` per entry.

Holding the read lock across the channel await would deadlock:
`handle_system_dml` takes `catalog_rwlock.write()` per drop, and a
pending writer blocks the GC's read waiter.


## Deviations from the original design

Items here are differences between this plan as originally written
and the V1 code that shipped. Read before starting V1.5 / V2.

1. **Hash function: BLAKE3 → xxh3_128.** The original plan
   specified BLAKE3 truncated to 128 bits. V1 switched to
   `xxhash_rust::xxh3::xxh3_128` over a length-prefixed framing —
   xxhash is already a dep of `gnitz-protocol` / `gnitz-engine` for
   row hashing, and migrations are authored by trusted operators
   (drift detection is the requirement, not adversarial collision
   resistance). If V2 deployments need cryptographic collision
   resistance (e.g. for public / untrusted inputs), swap to BLAKE3
   and bump `format_version`.

2. **Canonical-form AST lives in `gnitz-wire`, not `gnitz-sql`.** The
   server needs to `decanonicalize` historical migrations without
   pulling in `sqlparser`. Types (`TableDef`, `ColumnDef`, `ViewDef`,
   `IndexDef`, `ViewDep`, `DesiredState`, `Diff`) plus
   `canonicalize` / `decanonicalize` / `compute_migration_hash` /
   `diff_by_name` / `topo_sort_diff` / `validate_drop_closure` live
   in `gnitz_wire::migration`. The SQL→AST parser stays in
   `gnitz_sql::migration` (re-exports the types).

3. **Canonical bytes travel the wire hex-encoded.** `ColData::Strings`
   holds Rust `String` (UTF-8); bincode output can contain arbitrary
   bytes. Client calls `to_hex(canonicalize(ast))` before push;
   server calls `from_hex` after read. The migration hash is
   computed over **raw** bytes on both ends so both sides agree.
   `gnitz_wire::migration::{to_hex, from_hex}` are the helpers. This
   doubles the storage cost of the canonical column, which is
   negligible (migrations are rare and small).

4. **V1 MVP has no `__mig_` staging.** The plan's Phase 2 is a
   no-op in V1 MVP; Phase 3 builds create + drop batches directly
   with final names and applies them atomically under one
   `sal_writer_excl` + single fsync. V1.5 resumes the full staging
   pattern to add views and prepare for V2 backfill.

5. **Phase 4 has no ACK collection in V1 MVP.** Matches the existing
   `handle_system_dml` pattern: fsync is the synchronisation point.
   Workers consume the SAL asynchronously; `rmtree` after fsync
   relies on Linux unlink-while-open semantics. If V1.5 or V2
   introduces a reason workers must have fully applied the swap
   before `rmtree` (e.g. windows-style file locks in a future port),
   reintroduce per-broadcast `req_ids` and `collect_all_worker_acks`
   before the rmtree loop.

6. **`build_sequence_hwm_batch` covers both SEQ_ID_TABLES and
   SEQ_ID_INDICES.** The original plan's Phase 3 SYS_SEQUENCES batch
   only mentioned SEQ_ID_TABLES. V1 MVP allocates new `idx_id`s for
   pure-create indices during Phase 3, so the index sequence HWM
   must also be durable. V2 adds SEQ_ID_SCHEMAS when migration SQL
   can include `CREATE SCHEMA`.

7. **`CREATE VIEW` in migrations (V1.5, partial).** Shipped via
   route (b) from the V1.5 goals — the AST carries a client-compiled
   `CircuitGraph` inside `ViewDef.circuit`. Plan originally preferred
   route (a) (server-side planner) but the "Z-sets over the wire /
   server doesn't do SQL" tenets ruled it out. The hash covers the
   CircuitGraph (contradicting the plan's original "hash covers AST,
   not compiled bundle" note) — acceptable because client compile is
   deterministic and the simpler canonical form is worth it. Only
   plain SELECT views are supported today; JOIN / GROUP BY / UNION /
   DISTINCT and same-migration view-on-table are follow-ups.

8. **FK column index not resolved.** `ColumnDef.fk_table_id` was the
   old design's resolution handle. The canonical AST uses
   `fk_schema` + `fk_table` (by name). `fk_col_idx` in the AST is
   always 0 — it's resolved to the parent's PK at `build_create_table_batches`
   time. V1.5 should validate that the parent's PK matches the FK's
   declared target column name (today any `REFERENCES parent(col)`
   silently becomes `REFERENCES parent(PK)`).

9. **Empty-diff migrations are rejected.** Plan acceptance criterion
   4 ("empty migration rejected") is enforced in Phase 1 before any
   SAL activity, matching the criterion.

10. **`put_string_bytes` + `migrations_tab_schema` additions.** The
    `BatchBuilder` gained a `put_string_bytes` method for non-UTF-8
    payloads (used internally for canonical storage — but superseded
    by hex encoding; see item 3). The `migrations_tab_schema()`
    builder and `MIGRATIONS_TAB_ID` constant live alongside existing
    sys-table schemas.

11. **Two `CircuitGraph` types (V1.5).** `gnitz-wire::CircuitGraph`
    (serde, Hash, with `output_col_defs`) is the migration type
    embedded in `ViewDef.circuit`. `gnitz-core::CircuitGraph` is the
    imperative type used by `create_view_with_circuit`. Client-side
    `core_to_wire_circuit` is a field-by-field copy at the migration
    boundary. Unification is a future refactor; keeping them separate
    avoids pulling serde into `gnitz-core`'s hot path.

12. **`validate_view_circuit` Phase-1 gate (V1.5).** For every
    created view, Phase 1 calls
    `CatalogEngine::validate_view_circuit(&v.circuit)` which wraps
    `DagEngine::validate_graph_structure`. Rejects empty graphs,
    dangling edges, missing primary-input source, missing INTEGRATE
    sink. Runs BEFORE any SAL touch so bad circuits never reach Phase
    3 where `on_view_delta → ensure_compiled` would panic.

13. **`build_create_view_batches` emission order (V1.5).** sys_columns
    → sys_view_deps → sys_circuit_nodes → sys_circuit_edges →
    sys_circuit_sources → sys_circuit_params →
    sys_circuit_group_cols → **sys_views (last)**. sys_views last is
    load-bearing: `on_view_delta`'s register path calls
    `read_column_defs(vid)` and `dag.get_source_ids(vid)` — both read
    from catalog state the prior batches populated. Reversing the
    order produces "columns not found" warnings and silent
    registration skips. Batches go through the same
    `ingest_to_family + broadcast_ddl` loop in `apply_phase3` that
    tables use, so no new wire path.

14. **`const_strings` gap (pre-V1.5, still present).** Both the
    imperative `write_circuit_graph` and the new
    `build_create_view_batches` iterate `graph.params` and emit
    `(value, NULL str_value)` to sys_circuit_params. Neither writes
    `graph.const_strings` rows. Views whose compile produces string
    literals (e.g. filter-by-const-string) would lose them across
    restart. Not exercised by current client compile paths; fix
    before enabling string-predicate views via migration.

### Relevant file paths (post-V1)

- `gnitz-wire/src/migration.rs` — AST, canonical, hash, diff, topo,
  hex helpers.
- `gnitz-sql/src/migration.rs` — SQL→AST parser (re-exports types).
- `gnitz-engine/src/catalog/migration.rs` — `Cascade`, `Rename`,
  `MigrationRow`, swap-batch construction, drop/create batch
  builders, `collect_dropped_directories`.
- `gnitz-engine/src/catalog/mod.rs` — `sys_migrations` table,
  rename fast-path (`detect_rename_pks`), ownership guards,
  directory-column reads, startup sweep, `current_migration_hash`,
  `replay_migrations_head`, `sys_migrations_cursor`.
- `gnitz-engine/src/executor.rs` — `MigrationInFlightGuard`,
  `apply_migration`, `apply_phase3`, `extract_migration_row`,
  `read_migration_canonical`, dispatch branch.
- `gnitz-engine/src/reactor/exchange.rs` — `PendingRelay` enum
  with `Relay` / `Fence` variants.
- `gnitz-core/src/connection.rs` — `push_migration`.
- `gnitz-core/src/client.rs` — `GnitzClient::push_migration`.
- `gnitz-py/src/lib.rs` — `PyGnitzClient::push_migration` pyo3 binding.
- `gnitz-py/python/gnitz/_client.py` — `Connection.push_migration`.
- `gnitz-py/tests/test_migrations.py` — migration E2E tests.
- `gnitz-wire/src/circuit.rs` (V1.5) — `CircuitGraph` canonical type.
- `gnitz-sql/src/planner.rs` (V1.5) — `compile_view_for_migration`.


## V1 scope (done)

- `CREATE TABLE` / `DROP TABLE` (whole-object).
- `CREATE INDEX` / `DROP INDEX`.
- Declarative desired-state via `push_migration`; content-addressed
  linear chain; single-SAL-transaction atomic cutover (no staging).
- Prerequisites: rename fast-path, directory-column reads, ownership
  guards, `PendingRelay::Fence`, `migration_in_flight`, startup
  sweep.

Rejections: `modified` diff entries, empty diffs.

## V1.5-so-far (done)

- `CREATE/DROP VIEW` in `push_migration` for plain SELECT (WHERE +
  projection + computed columns). See "V1.5 view create/drop" above
  for the flow.
- `format_version` bumped 1 → 2; `ViewDef.circuit` is part of the
  AST + hash.
- Phase-1 structural validation of every created view's circuit.
- Preserves: imperative `create_view_with_circuit` path is unchanged.

Rejections (still present): `modified`, empty diffs, complex view
shapes via migrations (JOIN/GROUP/UNION/DISTINCT — surface a
"use imperative" error), same-migration view-on-table.


## V1.5 scope

V1.5 is the minimum superset of V1 that supports views and resumes
the staging pattern. V2 layers backfill on top of V1.5.

### V1.5 goals

1. **`CREATE/DROP VIEW` in migrations — done (plain SELECT).**
   Route (b) shipped: `gnitz-wire::CircuitGraph` embedded in
   `ViewDef.circuit`; client compiles client-side via
   `gnitz_sql::migration::parse_desired_state_with_circuits` which
   delegates to `gnitz_sql::planner::compile_view_for_migration`;
   server calls `build_create_view_batches` /
   `build_drop_view_batches` in `apply_phase3`.

   Remaining follow-ups (track as V1.5 goal 1b):
   - **Complex view shapes via migrations.** Only plain SELECT is
     supported; JOIN / GROUP BY / UNION / DISTINCT surface a
     "use imperative `create_view_with_circuit`" error from the
     migration compile path. Port the plain-view compile refactor
     pattern (`compile_plain_view_circuit` in
     `gnitz-sql/src/planner.rs`) to each `execute_create_*_view`
     sibling.
   - **Same-migration view-on-table.** `CREATE TABLE t; CREATE VIEW
     v AS SELECT * FROM t` in one migration fails at compile time
     because `t` doesn't yet exist in the live catalog. Fix: client
     pre-allocates tids for same-migration tables via
     `FLAG_ALLOCATE_TABLE_ID` before compile, and compile consults
     a local `(schema, name) → pre_alloc_tid` map before querying
     the server. Burned tids on migration abort are acceptable
     (sequences never reuse; migrations are rare).
   - **`const_strings` persistence.** See deviation 14.

2. **Resume `__mig_<hash>_` staging — pending.** Follow the "Apply
   algorithm" pseudocode above verbatim. V1.5 as shipped still uses
   V1's direct-apply block — views get their final names in Phase 3,
   not staged names. Safe today because views are created empty (no
   backfill path) and the whole migration is one SAL transaction.
   Staging becomes mandatory when V2 backfill lands: non-idempotent
   Phase 2 work needs isolation so a failure leaves orphans instead
   of a half-applied catalog.

   V1's `build_create_table_batches` / `build_create_index_batches`
   / `build_create_view_batches` helpers still have value: V1.5's
   Phase 2 can call them to synthesise CREATE broadcasts with staged
   names. Phase 3 then uses the existing `build_swap_batches` +
   `Cascade.renames` population with `RenameKind::View` (already
   drafted in `catalog/migration.rs` but never exercised).

3. **`mig_gc_task` — pending.** Only relevant once goal 2 lands;
   today's single-fsync direct-apply cannot leave orphans.

4. **Relay fence — inactive, shipped.** `PendingRelay::Fence`
   already ships and is a no-op because V1.5 has no Phase 2 activity
   that emits exchange relays. Becomes load-bearing with goal 2.

5. **ViewDef SQL normalisation — pending.** `ViewDef.sql` stores
   `sqlparser::Statement::CreateView.query.to_string()`, which
   round-trips through sqlparser's `Display` impl but whitespace
   differences in input can still produce different hashes. Consider
   normalising or storing a parsed AST. Low priority — the
   `CircuitGraph` inside `ViewDef` is deterministic from SQL so
   duplicate submissions still match.

### V1.5 non-goals

- **Modifies** (still rejected in Phase 1).
- **Backfill.** View populations in V1.5 are limited to views whose
  sources are all in the diff's `created` set — Phase 2 creates the
  tables empty, then creates the views (also empty). Backfill for
  *unmodified* sources is a V2 concern.
- **Transitive view closure (`diff.affected_views`).** V1.5 has no
  modifies so no closure is needed. Keep the code path in place (it
  appears in the Phase 2 pseudocode) but it's always empty until V2.

### V1.5 acceptance criteria

Rename-fast-path criteria (9-14) and the `mig_gc_task` criterion
(36) are gated on goal 2 (staging) shipping. What's covered today:

- **16a** ✓ drop-cascade retracts view columns (now exercised via
  `build_drop_view_batches`).
- **17** ✓ drop with live view dependency — unchanged; views in
  migrations go through the same `validate_drop_closure`.
- **18** ◐ transitive drop-validation: validator unchanged, V1.5 E2E
  adds a triangle test as a follow-up.
- **19** ◐ view-on-view create ordering: `topo_sort_diff` already
  orders `created_views` after `created_tables` and view-on-view by
  deps; needs E2E once same-migration view-on-table lands.

Remaining for V1.5 (when goal 2 lands):
- **9**: final directory retains `__mig_` prefix + survives reboot.
- **10**: unified-batch rename assertion (exercised).
- **11**: view-rename preserves ephemeral data (V1.5 views do not
  carry data across the same-migration rename because there's no
  backfill — but the rename fast-path still must not tear down the
  trace).
- **12**: index rename preserves data + no cluster stall.
- **13**: table-rename preserves storage.
- **14**: universal-stage pure-create round-trip.
- **36**: pre-SAL crash recovery via `mig_gc_task`.

### Sketch of the V1.5 apply flow

```
1. Validate + diff (same as V1).
2. Reject modifications (V1 scope guard still active).
3. Reject views? NO — V1.5 enables views.
4. Topo-sort.
5. Phase 2: for each created/affected object, issue CREATE with
   __mig_<hash>_<name> via the standard DDL broadcast path.
   Each broadcast takes its own sal_writer_excl + fsync (per the
   plan's "cost accepted" note).
6. Phase 2→3 fence (already shipped).
7. Phase 3: catalog_rwlock.read for diff + cascade + dirs snapshot.
   catalog_rwlock.write for swap ingest + broadcast under one
   sal_writer_excl + single fsync (same structure as V1).
8. Phase 4: rmtree dropped directories.
9. Respond OK.
```


## V2 prerequisites

- **`on_column_delta` hook** for in-place FK rewires (see
  "Required catalog changes" above).
- **`handle_backfill` targeting** (see same section).
- **`fan_out_scan_async`** for master-side full-table scan into one
  `Batch`. Needed for the Phase 2 modify path.
- **`diff.affected_views` computation.** Transitive closure through
  `sys_view_deps` starting from `diff.created ∪ diff.modified`.
- **AST name rewrite helpers** for view queries and FK
  `REFERENCES`.


## V2 size guardrail

V2 holds `migration_in_flight` across Phase 2's scan-transform on
the master; `fan_out_scan_async` collects the full scan into one
`Batch` on the master, so the whole old table must fit in memory.
Phase 1 rejects any `modified` table whose row count exceeds a
deployment-configured threshold (e.g. 1M rows).


## V2 scope

V1.5 plus the diff engine and the full Stage-and-Swap execution path
with backfill. Input stays purely declarative — no imperative `ALTER
TABLE` (would contradict the content-addressed commit model).

- **Diff-inferred column changes.** Column adds/removes detected by
  comparing column names in old/new `CREATE TABLE` ASTs.
- **Declarative renames via annotations.** Renames are ambiguous
  under name-keyed diff (`status → state` is indistinguishable from
  `drop status; add state`). Disambiguated by inline AST annotations
  parsed from special SQL comments: `-- @rename old_name` on a
  column, or on a `CREATE TABLE` / `CREATE VIEW` / `CREATE INDEX`
  statement. The annotation is part of the canonical AST (and thus
  the hash) — omitting it means "drop+add" by construction.
- **Diff-inferred type changes with transform annotations.**
  Trivially safe widenings (e.g. `INT → BIGINT`) apply with an
  implicit identity cast. Non-trivial conversions require an inline
  `-- @transform CAST(col AS INT)` annotation whose SQL expression
  is compiled into the master-local memory transform that Phase 2
  applies during backfill. Without the annotation, Phase 1 rejects
  the diff with a specific error naming the column and types.
- **Stage-and-Swap execution** for modifies.

Annotations live inside the SQL submitted to `push_migration`;
`gnitz-sql` parses them into dedicated AST fields (`rename_from:
Option<String>` on `ColumnDef` / `TableDef` / etc., `transform_expr:
Option<Expr>` on `ColumnDef`). They are part of the canonical AST
and therefore part of the migration hash. Adding these fields
requires bumping `format_version` (document: any AST field change
must bump the version).


## Acceptance criteria

Annotated with the version that owns each criterion. `✓` = covered
in V1; `◐` = partially covered (often: gate exists, full case not
exercised); `V1.5` / `V2` = pending.

### Correctness under concurrency

1. ✓ **Concurrent migration serialisation**: two clients with same
   `parent_hash`; exactly one commits, other gets stale-parent
   error.
2. ◐ **TOCTOU under yield**: A passes Phase 1, yields during Phase
   2; B commits. A's Phase 3 re-check fails cleanly; staged orphans
   GC'd. *V1 MVP has no Phase 2 yield point, so the TOCTOU window
   is vanishingly small; V1.5 reintroduces a real window — add a
   fault-injection test then.*
3. ✓ **Genesis commit**: first migration with `parent_hash=0`
   succeeds.
4. ✓ **Empty migration**: same state as parent — rejected.
5. ✓ **Migration-under-load**: second `push_migration` during first
   returns "in flight" without blocking; DML and DDL gated and
   observably resume after completion.

### Stage-and-Swap integrity

6. V2 **Modify round-trip**: column-type change — after commit,
   reading returns transformed data; no `__mig_` objects left.
6a. V2 **Unique-index violation caught on transformed data**.
7. V2 **Join view follows base** (modify propagation).
8. V1.5 **No relay deadlock during staged backfill**.
9. V1.5 **Final directory retains `__mig_` prefix + survives
   reboot**. *V1 final directory has no `__mig_` prefix because no
   staging — rewrite this assertion when V1.5 lands.*

### Rename fast-path

10. V1.5 **Unified-batch rename assertion**.
11. V1.5 **View-rename preserves ephemeral data**.
12. V1.5 **Index rename preserves data + no cluster stall**.
13. V1.5 **Table-rename preserves storage**.
14. V1.5 **Universal-stage pure-create round-trip**.

### Cascade & leak prevention

15. ✓ **Drop-cascade enumerates indices**: covered by V1
    `collect_cascade`.
16. ✓ **Drop-cascade retracts circuit graph**: V1 `build_swap_batches`
    emits retractions for all 5 circuit tables.
16a. ✓ **Drop-cascade retracts view columns**: V1.5
    `build_drop_view_batches` exercises `build_retract_column_records`
    with `OWNER_KIND_VIEW`.
17. ✓ **Drop with live view dependency**: rejected with specific
    error via `validate_drop_closure`.
18. ◐ **Transitive drop-validation**: triangle `X→W→V` with all
    three dropped. Validator exists; V1.5 needs a dedicated E2E now
    that views work in migrations.

### Robustness

19. ◐ **View-on-view create ordering**: `topo_sort_diff` emits
    `V_A` before `V_B`. E2E needs same-migration view-on-table
    (V1.5 follow-up) to exercise end-to-end.
19a. V2 **Multi-hop cascade AST rewrite**.
20. ✓ **Allocator-race stress**: HWM broadcast covers
    SEQ_ID_TABLES + SEQ_ID_INDICES.
21. ✓ **Sequence HWM under concurrency-then-abort**.
21a. ✓ **SYS_SEQUENCES byte-exact retraction**.
22. ◐ **Worker-ACK-gated rmtree**: V1 MVP does not collect ACKs;
    rmtree runs right after fsync. Linux unlink-while-open keeps
    this safe. Criterion is explicitly deferred — re-evaluate if
    V1.5/V2 adds a reason ACKs are required.
23. ✓ **`fsync` lock released before await**.
24. ✓ **In-flight DML drained**: committer barrier + tick drain.

### FK re-wire (V2)

25. V2 **FK re-wire**: migration that bumps a parent's `table_id`.
26. V2 **FK-parent-map cleanup on re-wire**.
27. V2 **Co-migrated FK target rewrite**.

### Internal state integrity

28. ✓ **`name_to_id` ownership guard**.
29. V2 **Join-view unmodified-source backfill**.
30. V2 **Targeted backfill does not duplicate sibling traces**.
30a. V2 **Cascade ungated through staged chains**.
31. V1.5 **No spurious rename rows in retract-only tables**: assert
    `SYS_VIEW_DEPS`, `SYS_COLUMNS`, and 5× `SYS_CIRCUIT_*` batches
    contain ONLY `-1` rows.
32. V1.5 **Mid-exchange migration commit**: worker parked inside
    `do_exchange_wait` during commit.
33. ✓ **Pre-checkpoint replay applies all N entries**: standard
    `recover_system_tables_from_sal` path; V1 test exists via
    the head-survives-restart E2E.
34. ✓ **Post-checkpoint replay does NOT double-apply**.
35. ✓ **Worker ingest path is memonly**: existing invariant.

### Recovery

36. V1.5 **Pre-SAL crash**: kill mid-Phase-2 → `mig_gc_task`
    reaps. *V1 has no Phase 2 and no `mig_gc_task`; this is the
    headline V1.5 test.*
37. V1.5 **GC without hash check**.
38. ✓ **Current-head rehydrates on boot**.
39. ✓ **Startup-sweep safety**.
40. V2 **Backfill conversion failure**.


## Files touched

### V1 (shipped)

| File                                              | Change                                                                                   |
|---------------------------------------------------|------------------------------------------------------------------------------------------|
| `gnitz-wire/src/migration.rs`                     | AST + canonical + hash + diff + topo + hex helpers (new).                                 |
| `gnitz-wire/src/lib.rs`                           | `MIGRATIONS_TAB = 8`, `pub mod migration`.                                                |
| `gnitz-wire/Cargo.toml`                           | Deps: `serde`, `bincode 2.0`, `xxhash-rust`.                                              |
| `gnitz-sql/src/migration.rs`                      | SQL→AST parser; re-exports from `gnitz-wire`.                                             |
| `gnitz-engine/src/catalog/mod.rs`                 | sys_migrations plumbing; rename fast-path; ownership guards; directory reads; startup sweep; `current_migration_hash`; replay arm; hook arm. |
| `gnitz-engine/src/catalog/sys_tables.rs`          | `MIGRATIONS_TAB_ID`, `migrations_tab_schema`, column constants.                           |
| `gnitz-engine/src/catalog/migration.rs`           | Cascade / Rename / MigrationRow / swap + create/drop batch builders (new).                |
| `gnitz-engine/src/executor.rs`                    | `MigrationInFlightGuard`, `apply_migration`, `apply_phase3`, dispatch branch.             |
| `gnitz-engine/src/reactor/exchange.rs`            | `PendingRelay` → enum with `Fence`.                                                       |
| `gnitz-engine/src/storage/shard_index.rs`         | Document compact-drain invariant (no code change).                                        |
| `gnitz-core/src/connection.rs`                    | `Connection::push_migration`.                                                             |
| `gnitz-core/src/client.rs`                        | `GnitzClient::push_migration`.                                                            |
| `gnitz-core/src/types.rs`                         | `migrations_tab_schema`.                                                                  |
| `gnitz-py/src/lib.rs`                             | pyo3 binding.                                                                             |
| `gnitz-py/python/gnitz/_client.py`                | Python wrapper.                                                                           |
| `gnitz-py/tests/test_migrations.py`               | 10 E2E tests.                                                                             |

### V1.5 (shipped — views in direct-apply)

| File                                              | Change                                                                                             |
|---------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `gnitz-wire/src/circuit.rs`                       | New. `CircuitGraph` (serde + Hash) canonical type for migrations.                                  |
| `gnitz-wire/src/lib.rs`                           | `pub mod circuit; pub use circuit::CircuitGraph;`                                                  |
| `gnitz-wire/src/migration.rs`                     | `ViewDef.circuit: CircuitGraph`.                                                                   |
| `gnitz-engine/src/executor.rs`                    | Reject `format_version != 2`; drop view-reject; Phase-1 `validate_view_circuit` loop; Phase-3 view create/drop loops. |
| `gnitz-engine/src/catalog/migration.rs`           | `build_create_view_batches`, `build_drop_view_batches` (8-batch emission order).                   |
| `gnitz-engine/src/catalog/mod.rs`                 | `CatalogEngine::validate_view_circuit` (Phase-1 gate).                                             |
| `gnitz-sql/src/planner.rs`                        | `compile_plain_view_circuit` extracted from `execute_create_view`; `compile_view_for_migration` dispatches. |
| `gnitz-sql/src/migration.rs`                      | `parse_desired_state_with_circuits(sql, client, default_schema)`.                                  |
| `gnitz-core/src/connection.rs`                    | `push_migration` emits `format_version=2`.                                                         |
| `gnitz-py/src/lib.rs`                             | `has_create_view` scan; PyO3 `push_migration` routes via `parse_desired_state_with_circuits`.      |
| `gnitz-py/tests/test_migrations.py`               | View-create + view-drop-by-omission E2E; old view-rejected test removed.                           |
| `gnitz-engine/src/catalog/tests.rs`               | 5 unit tests: build_create/drop batch shapes, hash stability, circuit-validation rejects.          |

### V1.5 (remaining — staging, GC, complex views)

| File                                              | Change                                                                                              |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `gnitz-engine/src/executor.rs`                    | Phase-2 staging loop (CREATE under `__mig_<hash>_` names); `mig_gc_task` spawn.                     |
| `gnitz-engine/src/catalog/migration.rs`           | `Cascade.renames` population from Phase-2 ids; `build_swap_batches` exercises `RenameKind::View`.   |
| `gnitz-sql/src/planner.rs`                        | Extract compile-only helpers from the remaining `execute_create_*_view` siblings (join / group / set-op / distinct). |
| `gnitz-sql/src/migration.rs`                      | Client-side tid pre-allocation for same-migration tables; resolver consults pre-alloc map.          |
| `gnitz-engine/src/catalog/mod.rs`                 | Persist `circuit.const_strings` to sys_circuit_params (deviation 14) — applies to both imperative and migration paths. |
| `gnitz-py/tests/test_migrations.py`               | Add `__mig_` prefix test, GC-on-boot test, complex-view tests, same-migration view-on-table test.   |

### V2 (expected)

| File                                              | Change                                                                                   |
|---------------------------------------------------|------------------------------------------------------------------------------------------|
| `gnitz-engine/src/catalog/mod.rs`                 | Stateless `on_column_delta` (clear-then-rewire per unique `owner_id`).                    |
| `gnitz-engine/src/dag.rs`                         | `evaluate_dag_multi_worker` gains `target_view_id: Option<i64>`.                          |
| `gnitz-engine/src/worker.rs`                      | `handle_backfill` decodes `seek_pk_lo` as target view_id.                                 |
| `gnitz-engine/src/executor.rs`                    | V2 Phase 2 backfill; modify-path orchestration; size guardrail.                           |
| `gnitz-sql/src/migration.rs`                      | `-- @rename` / `-- @transform` annotation parsing; AST name-rewrite helpers.              |


## Future work

- **Async / online migrations.** Lifts the write-stall during V2's
  backfill window. Requires dual-write or trace-capture discipline,
  a client-visible version handle, and a catch-up / cutover protocol.
  Until it lands, V2 is only practical for small-to-medium tables.
- **Server-pushed cache invalidation.** A lightweight signal on
  every response (e.g. `catalog_epoch` counter or hash) so clients
  can proactively invalidate caches. Useful once a client-side
  `name_to_id` cache exists; not needed for V1+V2.
- **Real subscription API.** A proper server-push stream of catalog
  deltas. Useful beyond migrations.
- **Cryptographic hash mode.** If GnitzDB ever accepts migrations
  from untrusted input, add a BLAKE3 `format_version` and dispatch
  by the column.
