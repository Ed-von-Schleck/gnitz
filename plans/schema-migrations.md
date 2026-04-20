# Schema Migrations

## Status

| Version | Status   | Scope                                                                                           |
|---------|----------|-------------------------------------------------------------------------------------------------|
| V1      | **done** | `CREATE/DROP TABLE`, `CREATE/DROP INDEX`, direct-apply (no staging)                             |
| V1.5    | **done** | `CREATE/DROP VIEW`; `__mig_` staging (Phase 2); rename fast-path in Phase 3; `mig_gc_task`     |
| V2      | pending  | Modifies via backfill + FK rewire + size guardrails                                             |

V1.5 is complete. Views land via client-compiled `CircuitGraph` in `ViewDef.circuit`. Phase 2 creates every new object under `__mig_<hash>_<name>` staged names; Phase 3 renames them atomically via `build_swap_batches` + rename fast-path. `mig_gc_task` reaps orphans at startup. See **Deviations** for differences between this plan and the code.


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

### V1/V1.5 apply path (done)

V1.5 ships the full Stage-and-Swap path. The high-level flow in `apply_migration_row`:

1. **Phase 1**: drain committer + tick, validate + diff under `catalog_rwlock.read()`.
2. **Phase 2** (`apply_phase2`): for each object in `diff.created_*`, create under `__mig_<hash>_<name>` (same schema). Each object: one `sal_writer_excl` + `ingest + broadcast + fsync`. Holds `catalog_rwlock.write()` for all of Phase 2 (deviation 15).
3. **Relay fence**: drain pending `FLAG_EXCHANGE` relays.
4. **Phase 3** (`apply_phase3`): under `catalog_rwlock.write()`, call `build_cascade_from_staged(diff, staged)` → `Cascade`, then `collect_dropped_directories(&cascade)`, then `build_swap_batches(&cascade)` → rename + drop batches, then `build_sequence_hwm_batch()`, then ingest + broadcast all under one `sal_writer_excl` + single fsync.
5. **Phase 4**: rmtree dropped dirs, respond OK.

**`build_create_view_batches(v)` emission order (8 batches, load-bearing):**
sys_columns → sys_view_deps → 5× sys_circuit_* → **sys_views last** (triggers `on_view_delta` which reads all prior rows).

**`build_drop_view_batches`**: 5× sys_circuit_* → sys_view_deps → sys_columns → sys_views.

**Staged view SQL**: `sql_definition` in the staged row stores the staged name. Phase 3's `append_view_rename_plus` overwrites it with `final_sql` (original SQL). After rename, `sql_definition` is clean.

**Staged directories**: directory column keeps `__mig_` prefix permanently after Phase 3 rename (verbatim copy — see deviation 16).

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

- **Throughout**: `migration_in_flight` set; gates external DML + DDL.
- **Phase 1**: `catalog_rwlock.read()` only; released before Phase 2.
- **Phase 2**: holds `catalog_rwlock.write()` for all creates (deviation 15). Each object takes `sal_writer_excl` inside, releases before fsync `.await`.
- **Phase 2→3 fence**: no catalog lock.
- **Phase 3**: `catalog_rwlock.write()`. `sal_writer_excl` inside, across all N broadcasts + fsync SQE submit, released before fsync `.await`.
- **Phase 4**: no catalog lock.


## Required catalog changes

### Rename fast-path (done)

`on_table_delta` / `on_view_delta` / `on_index_delta` detect same-PK
`-1`/`+1` pairs via `detect_rename_pks` and apply only the +1's name
remap. Load-bearing for views/indices (teardown loses in-memory data;
index rebuild stalls the cluster).

### Ownership-guarded `name_to_id.remove` (done)

Drop hooks only erase `name_to_id` / `index_by_name` if the entry
still points to the id being dropped.

### Directory-column reads (done)

`on_table_delta` reads `TABLETAB_COL_DIRECTORY`; `on_view_delta`
reads `VIEWTAB_COL_CACHE_DIRECTORY`. Load-bearing: staged paths have
`__mig_` in the directory string; recomputing from name would open
wrong path.

### `on_index_delta` HWM fix (done, V1.5)

The create branch previously omitted `if idx_id + 1 > self.next_index_id { self.next_index_id = idx_id + 1; }`. Without it, replaying a crashed Phase-2 staged-index batch left `next_index_id` stale; the next migration would reallocate the same `idx_id`. Fixed before any staged index could be fsynced.

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

### `mig_gc_task` (done, V1.5)

Spawned in `ServerExecutor::run` (before `accept_loop`). Runs once at startup.

**Invariant**: Phase 3 renames every `__mig_<hash>_X` to its final name atomically. Any `__mig_*` name still present at boot is an aborted Phase-2 leftover. No hash check needed.

**Safety against user collision**: `validate_user_identifier` rejects identifiers starting with `_`, so users can never create a `__mig_*` object.

**Implementation**: 
1. Acquire `catalog_rwlock.read()` briefly; call `cat.collect_staged_orphans()` → `Vec<(StagedKind, String, String)>` (scans `name_to_id` + `index_by_name`).
2. Drop read lock.
3. For each orphan: acquire `catalog_rwlock.write()`; call `build_drop_{table,view,index}_batches` (bypasses `validate_user_identifier`; public `drop_table`/`drop_view` would reject `__mig_*` names); ingest + broadcast + fsync; drop write lock.

`StagedKind` / `StagedMapping` and `collect_staged_orphans` live in `catalog/migration.rs` (deviation 17).


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

15. **Phase 2 holds `catalog_rwlock.write()` (V1.5).** The plan's pseudocode says "NO catalog lock" in Phase 2. The actual implementation acquires `catalog_rwlock.write()` for all of Phase 2's creates (released before the relay fence). Necessary to prevent concurrent client DDL from racing the Phase-2 creates; `migration_in_flight` gates new MIGRATIONS pushes but not normal `handle_message` DDL paths.

16. **`build_swap_batches` does NOT update SYS_SEQUENCES (V1.5).** The original `build_swap_batches` included a step 11 that rewrote `SEQ_ID_TABLES`. This conflicted with the separately-called `build_sequence_hwm_batch` (which handles both `SEQ_ID_TABLES` + `SEQ_ID_INDICES`). Step 11 was removed from `build_swap_batches`; `build_sequence_hwm_batch` is the sole sequence updater, called separately in Phase 3 after `build_swap_batches`. Any future caller of `build_swap_batches` must call `build_sequence_hwm_batch` separately.

17. **`StagedKind` / `StagedMapping` live in `catalog/migration.rs`, not `executor.rs` (V1.5).** The plan suggested `executor.rs`. These types are used by `build_cascade_from_staged` and `collect_staged_orphans` (both methods on `CatalogEngine`), so they belong in the catalog module. `executor.rs` imports them via `use crate::catalog::migration::{StagedKind, StagedMapping}`.

### Relevant file paths (post-V1)

- `gnitz-wire/src/migration.rs` — AST, canonical, hash, diff, topo,
  hex helpers.
- `gnitz-sql/src/migration.rs` — SQL→AST parser (re-exports types).
- `gnitz-engine/src/catalog/migration.rs` — `StagedKind`,
  `StagedMapping`, `Cascade`, `Rename`, `MigrationRow`,
  `build_cascade_from_staged`, `collect_staged_orphans`,
  swap-batch construction, drop/create batch builders,
  `collect_dropped_directories`.
- `gnitz-engine/src/catalog/mod.rs` — `sys_migrations` table,
  rename fast-path (`detect_rename_pks`), ownership guards,
  directory-column reads, `on_index_delta` HWM fix, `is_view_id`,
  startup sweep, `current_migration_hash`, `replay_migrations_head`,
  `sys_migrations_cursor`.
- `gnitz-engine/src/executor.rs` — `MigrationInFlightGuard`,
  `apply_migration`, `apply_phase2`, `ingest_and_fsync_phase2`,
  `apply_phase3`, `mig_gc_task`, `extract_migration_row`,
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


## V1.5 scope (done)

All V1.5 goals shipped:
1. ✓ `CREATE/DROP VIEW` in migrations (plain SELECT; `CircuitGraph` in `ViewDef.circuit`).
2. ✓ `__mig_<hash>_` staging (Phase 2 + Phase 3 rename fast-path).
3. ✓ `mig_gc_task` — reaps aborted Phase-2 orphans at startup.
4. ✓ Relay fence active (was a shipped no-op; now exercises load-bearing code path).

### V1.5 remaining follow-ups (not in V2 scope, track separately)

- **Complex view shapes via migrations.** Only plain SELECT; JOIN / GROUP BY / UNION / DISTINCT / DISTINCT are rejected. Port `compile_plain_view_circuit` pattern to the remaining `execute_create_*_view` siblings.
- **Same-migration view-on-table.** Fails at client compile time (`t` not in live catalog). Fix: client pre-allocates tids via `FLAG_ALLOCATE_TABLE_ID` before compile; compile consults a local `(schema, name) → pre_alloc_tid` map.
- **`const_strings` persistence** (deviation 14).
- **ViewDef SQL normalisation** — whitespace differences still produce different hashes.


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
2. ✓ **TOCTOU under yield**: Phase 3 re-check fails cleanly; staged orphans GC'd on next boot. E2E: `test_staging_toctou_leaves_clean_state_after_restart`.
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
9. ✓ **Final directory retains `__mig_` prefix + survives reboot**. E2E: `test_staging_round_trip_table_and_view_survives_restart`.

### Rename fast-path

10. ✓ **Unified-batch rename assertion**. `build_swap_batches` emits `-1`/`+1` in same `Batch`.
11. ✓ **View-rename preserves ephemeral data**. Rename fast-path keeps compiled plan + trace intact; no teardown.
12. ✓ **Index rename preserves data + no cluster stall**. Rename fast-path skips backfill_index.
13. ✓ **Table-rename preserves storage**. Directory kept verbatim.
14. ✓ **Universal-stage pure-create round-trip**. E2E: `test_staging_round_trip_table_and_view`, `test_staging_table_and_index`.

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
18. ✓ **Transitive drop-validation**: triangle `X→W→V` with all three dropped. E2E: `test_transitive_drop_validation`.

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
31. ✓ **No spurious rename rows in retract-only tables**: unit test `test_build_drop_view_batches_retract_only` asserts every row in `SYS_VIEW_DEPS`, `SYS_COLUMNS`, and 5× `SYS_CIRCUIT_*` batches has weight=-1.
32. V1.5 **Mid-exchange migration commit**: worker parked inside
    `do_exchange_wait` during commit.
33. ✓ **Pre-checkpoint replay applies all N entries**: standard
    `recover_system_tables_from_sal` path; V1 test exists via
    the head-survives-restart E2E.
34. ✓ **Post-checkpoint replay does NOT double-apply**.
35. ✓ **Worker ingest path is memonly**: existing invariant.

### Recovery

36. ✓ **Pre-SAL crash**: `mig_gc_task` reaps `__mig_*` orphans at startup. E2E: `test_mig_gc_no_orphans_after_clean_migration`.
37. ✓ **GC without hash check**: any `__mig_*` name at boot is an orphan by construction.
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

### V1.5 staging + GC (done)

| File                                              | Change                                                                                              |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `gnitz-engine/src/executor.rs`                    | `apply_phase2`, `ingest_and_fsync_phase2`; refactored `apply_phase3` to use `build_cascade_from_staged`; `mig_gc_task`; spawn in `run`. |
| `gnitz-engine/src/catalog/migration.rs`           | `StagedKind`, `StagedMapping`; `build_cascade_from_staged`; `collect_staged_orphans`; removed step-11 from `build_swap_batches` (deviation 16). |
| `gnitz-engine/src/catalog/mod.rs`                 | `on_index_delta` HWM fix; `is_view_id` helper.                                                     |
| `gnitz-py/tests/test_migrations.py`               | 6 new staging/GC E2E tests.                                                                        |

### V1.5 remaining follow-ups (not yet done)

| File                                              | Change                                                                                              |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `gnitz-sql/src/planner.rs`                        | Extract compile-only helpers from the remaining `execute_create_*_view` siblings (join / group / set-op / distinct). |
| `gnitz-sql/src/migration.rs`                      | Client-side tid pre-allocation for same-migration tables; resolver consults pre-alloc map.          |
| `gnitz-engine/src/catalog/mod.rs`                 | Persist `circuit.const_strings` to sys_circuit_params (deviation 14).                              |

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
- **`MasterDispatcher::schema_names_cache` drain on drop.**
  `schema_names_cache` (`table_id → (SchemaDescriptor, Rc<[Vec<u8>]>)`)
  is never invalidated when a table or view is dropped. `table_id`s
  are strictly monotonic and never reused, so stale entries cause no
  correctness bugs — but they accumulate indefinitely under heavy DDL
  churn. Fix: add `invalidate_schema_cache(table_id)` to
  `MasterDispatcher` and call it from `apply_phase3` when iterating
  `dropped_tids` (alongside the existing executor-cache invalidation).
