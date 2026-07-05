# Slice-local secondary indexes

Unify the secondary-index lifecycle on one shape: **an index table is a
worker-local structure containing exactly the projection of that worker's base
slice, living in a per-rank single-writer subdirectory; the master's copies are
permanently empty.** Today the shape depends on how the index came to exist —
live-created indexes are slice-local, boot-recovered indexes are full copies
replicated on every worker plus a dead full copy on the master — and the stale
foreign fraction of those full copies produces four classes of wrong results
after any restart, plus (W+1)× the index RAM.

This plan also absorbs a related boot defect it depends on: worker rank is set
too late for the worker boot phase, so boot-compiled plans in every worker
carry rank 0 / num_workers 1.

---

## Current mechanics (verified against source)

- An index table is a plain `Table` (`RecoverySource::Rederive`,
  `SYS_TABLE_ARENA`) owned by `IndexCircuitEntry.index_table`
  (`query/dag/mod.rs:48-62`, `UnsafeCell<Box<Table>>`), created by the
  sys_indices hook (`catalog/hooks.rs:595-683`) at
  `index_dir(&owner_dir, idx_id)` = `{owner_dir}/idx_{idx_id}`
  (`catalog/utils.rs:71`), then backfilled by `backfill_index`
  (`catalog/ddl.rs:449`) → `stream_index_projection` (`ddl.rs:485`) over
  `open_store_cursor(owner)` — a `PartitionedTable` cursor over the calling
  process's **active** partitions. Maintained per push by
  `ingest_store_and_indices` (`query/dag/mod.rs:1053-1065`).
- **Boot**: `replay_catalog` runs pre-fork on the master inside
  `CatalogEngine::open` (`catalog/bootstrap.rs:98,388-393`) with all 256
  partitions loaded → the hook backfills a **full** index pre-fork; every
  worker inherits that full copy across `fork()`. Nothing trims it:
  `trim_worker_partitions` / `close_user_table_partitions`
  (`catalog/partition_lsn.rs:31-59`) only touch `entry.handle`
  (`PartitionedTable`), never `entry.index_circuits`. Post-fork each worker
  applies only its own slice's deltas, so the (W−1)/W foreign fraction of its
  index goes monotonically stale as those rows churn on their owning workers.
- **Live CREATE INDEX**: the hook runs on the master (applies DDL first, then
  broadcasts) and on every worker via `ddl_sync`
  (`catalog/store_io.rs:441-451`). The master's backfill scans its post-fork
  base partitions — closed by `close_user_table_partitions`
  (`runtime/bootstrap.rs:583`) — so its copy stays empty; each worker's
  backfill scans its trimmed slice. Live-created = slice-local.
- **Index readers**: `seek_by_index` resolves index hits against the local
  base slice and skips orphans (`catalog/store_io.rs:131-243`;
  `seek_first_positive_with_prefix`,
  `storage/lsm/read_cursor/mod.rs:475-504`). But the distributed unique/FK
  probes do **raw** index probes with no base resolution: worker
  `handle_has_pk` with `HasPkLookup::UniqueIndex`
  (`runtime/orchestration/worker/mod.rs:1399-1460`) answers "present" on any
  positive-weight prefix match; the master broadcasts these as
  `FLAG_HAS_PK` batches from the unique/FK validation pipeline
  (`runtime/orchestration/master/preflight.rs:840-960`) and treats any
  non-empty reply as a hit. The master-side first-line check
  `validate_unique_indices` (`catalog/validation.rs:176-368`, called on every
  push at `runtime/orchestration/executor.rs:874-881`) seeks the **master's
  own** index copy — which is never updated post-fork (the committer maintains
  only `unique_filters`, `runtime/orchestration/master/unique_filter.rs`) —
  and its upsert exemption `entry.handle.has_pk_bytes(...)`
  (`validation.rs:328-330`) can never fire on the master (base partitions
  closed).
- **Rank timing defect**: `set_worker_rank` is called only in
  `WorkerProcess::new` (`worker/mod.rs:346`), which runs **after** the worker
  boot phase (`runtime/bootstrap.rs:478-570`: `set_active_partitions` :478,
  trim :479, rehome :485, `recover_from_sal` :497, boot flush :507-512,
  `invalidate_all_plans` :522, non-exchange `backfill_view` loop :533-552,
  then `WorkerProcess::new` + `run` :569). So plans compiled during boot
  `backfill_view` see `worker_rank() == 0` and `num_workers() == 1` in every
  worker — shared `_w0` scratch directories and identity `PartitionFilter`s —
  and nothing re-invalidates plans after the rank is finally set.
- **Multi-writer directory hazard**: all processes' index Tables point at the
  same `idx_{id}` dir, so worker spills past `INMEM_CEILING` write
  `shard_{tid}_{lsn}.db` with near-lockstep per-table LSN counters —
  same-name `.tmp` interleaving and cross-process renames
  (`write_shard_streaming`, `storage/lsm/shard_file.rs:241-273`); compaction
  outputs `hcomp_{tid}_L{n}_G{g}_{seq}` (`shard_index/index.rs:280`) collide
  the same way via the per-process `compact_seq`.
- DROP INDEX / DROP TABLE queue the **parent** `idx_{id}` dir into
  `pending_dir_deletions`; the master executor deletes it with
  `remove_dir_all` after the drop is durable
  (`catalog/write_path.rs:554-589`), checkpoint-gated against lagging
  workers. `gc_orphan_directories` (`write_path.rs:639-700`) removes dead
  index dirs at boot but never looks inside a live one.

## The bugs this fixes (all verified)

Post-restart, with a unique secondary index or a column FK:

1. **False unique-violation rejections.** Value v held at boot by a row on
   worker w, deleted post-boot: w's index nets it to 0, every other worker's
   full boot copy keeps +1 forever. INSERT of v → `FLAG_HAS_PK` broadcast →
   a stale worker answers positive → rejected.
2. **Orphaned FK children admitted silently.** FK parent-existence uses the
   same raw probe; a deleted parent still "exists" on non-owning workers →
   child rows referencing it are accepted.
3. **False FK RESTRICT rejections.** The child-index probe for retired parent
   values (`preflight.rs:895-905`) hits stale child entries.
4. **Master first-line false rejections.** The master's frozen boot index
   rejects delete-then-reinsert of a boot-era unique value, and insert-mode
   upserts of boot-era rows (the `is_upsert` exemption cannot fire).
5. **(W+1)× RAM** for every boot-recovered index, plus unbounded orphan
   accumulation until the next restart.
6. **Boot-compiled plans with rank 0 / num_workers 1** (the rank timing
   defect above) — shared `_w0` scratch dirs across workers.

---

## Design

### Process role (`foundation/worker_ctx.rs`)

```rust
static ROLE: AtomicU8 = AtomicU8::new(0); // 0 Standalone, 1 Master, 2 Worker

pub(crate) fn set_master_role();          // ROLE ← 1
pub(crate) fn is_master() -> bool;        // ROLE == 1
pub(crate) fn is_worker() -> bool;        // ROLE == 2
```

`set_worker_rank(rank, num_workers)` additionally stores `ROLE ← 2`.
`Standalone` (the default) is what unit tests and any in-process embedding
see; they must never set a role — `cargo test` shares one process across test
threads, and role-dependent behavior is covered by e2e (real fork).

Role semantics for indexes: **the master never backfills** (its index copies
stay permanently empty); **a worker homes its index tables in a per-rank
subdirectory**; **standalone behaves exactly like today** (hook backfills at
the parent dir on live CREATE and on reopen's `replay_catalog`), so the
existing unit-test population path is unchanged.

### Rank at fork (`runtime/bootstrap.rs`, `worker/mod.rs`)

- First statement of `server_main` (before `CatalogEngine::open`):
  `set_master_role()` — the pre-fork replay hooks already see Master.
- In the forked child, immediately after the `getppid` re-check
  (`bootstrap.rs:447`): `set_worker_rank(w as u32, num_workers)`, before ANY
  catalog work. Delete the call in `WorkerProcess::new` (`worker/mod.rs:346`)
  — single owner. This makes every boot-compiled plan rank-correct
  (`invalidate_all_plans` at :522 already cleared the inherited pre-fork
  plans) and gives the boot index rebuild its rank.

### Per-rank index directories (`catalog/utils.rs`, `catalog/hooks.rs`)

```rust
/// The directory holding THIS process's copy of an index table: the index
/// dir itself for master/standalone, `{idx_dir}/w{rank}` for a forked worker
/// (single-writer isolation for spills and compaction). Creates the rank dir
/// (create_dir_all — ensure_dir in Table::new is non-recursive).
pub(crate) fn index_table_dir(idx_dir: &str) -> String;

/// True if `name` is a per-rank index subdir (`w<digits>`).
pub(crate) fn is_index_rank_dir_name(name: &str) -> bool;
```

`hook_index_register` (`hooks.rs:628-650`):
- keeps `idx_dir = index_dir(&owner_dir, idx_id)` and keeps pre-staging
  **`idx_dir`** in `pending_dir_deletions` (parent removal is recursive, so a
  failed create or a DROP reclaims the rank subdirs too);
- `Table::new` targets `index_table_dir(&idx_dir)`;
- the backfill call becomes
  `if !self.ctx.in_rollback() && !worker_ctx::is_master() {
      self.backfill_index(owner_id, cols, is_unique, ptr, &idx_schema,
                          is_unique && self.ctx.is_live())?;
  }`
- the DROP branch is unchanged (parent dir queued via `creating_idx_id`).

Single-writer invariant restored: the master's parent-dir Table is never
populated and never spills; each worker writes only its own `w{rank}` dir.
The cross-process shard/hcomp name collisions above become impossible for
index tables.

### Explicit dup-check flag (`catalog/ddl.rs`)

`stream_index_projection(..., check_dups: bool)` replaces the internal
`let check_dups = is_unique && self.ctx.is_live();` (`ddl.rs:500`); thread the
same trailing parameter through `backfill_index`. Callers: the hook passes
`is_unique && ctx.is_live()` (today's policy); `promote_index_to_unique`
passes `true` (already inside its live guard); the boot rebuild below passes
`false` — boot data was validated at original write time, and the per-worker
`seen` set over a large slice is pure waste (the boot-phase reasoning already
codified at `ddl.rs:472-480`). A slice-local dup check also cannot
false-positive, so this is purely a cost decision.

### Boot rebuild (`catalog/ddl.rs`, `query/dag/mod.rs`, `runtime/bootstrap.rs`)

```rust
/// Worker-boot index rebuild: for every registered index circuit, re-create
/// its Table at this process's `index_table_dir` (replacing the
/// fork-inherited parent-dir table) and backfill it from the trimmed/rehomed
/// base slice. Must run after trim/rehome and BEFORE SAL replay (replay
/// projects the unflushed tail into the index exactly once). Fail-fast: an
/// error aborts worker boot via the startup ACK.
pub fn backfill_all_indexes(&mut self) -> Result<(), String>
```

Implementation: collect a worklist
`(owner_id, cols: PkColList, index_id, is_unique, idx_schema, owner_dir)`
from `dag.tables`; per item create
`Table::new(index_table_dir(&index_dir(&owner_dir, index_id)),
&format!("_idx_{index_id}"), idx_schema, index_id as u32, SYS_TABLE_ARENA,
RecoverySource::Rederive)`, swap it in via a new dag helper, and run
`backfill_index(..., /*check_dups=*/false)`.

```rust
// query/dag/mod.rs, next to add_index_circuit (:353)
/// Replace the index circuit's owned Table (worker-boot re-home to the rank
/// subdir). Returns the new table pointer for the backfill; None if no
/// circuit matches. Dropping the old Box closes the inherited parent-dir
/// table. Sound: pointer consumers (get_index_store_handle, cursors) are
/// fetched per request, and the swap runs before the worker serves anything.
pub fn replace_index_table(&mut self, table_id: i64, col_indices: &[u32],
                           t: Box<Table>) -> Option<*mut Table>
```

Worker boot phase (`bootstrap.rs`, between rehome :485 and the SalReader
construction :493) — the error variable is renamed `boot_err` since it now
covers more than the flush:

```rust
let mut boot_err: Option<String> =
    catalog.backfill_all_indexes().err()
        .map(|e| format!("boot index backfill failed: {e}"));
// existing: SalReader / W2mWriter construction
if boot_err.is_none() {
    recover_from_sal(&sal_reader, catalog);
    // existing per-family boot flush loop (sets boot_err on failure)
}
// existing: invalidate_all_plans; gated non-exchange backfill_view loop;
// WorkerProcess::new + run(boot_err)  → error rides the startup ACK,
// master aborts boot before the SAL sentinel is zeroed.
```

**Ordering is load-bearing in both directions.** Backfill indexes the flushed
base shards; `recover_from_sal` then replays the committed tail through
`replay_ingest` → `ingest_returning_effective` → `ingest_store_and_indices`,
projecting each replayed row into the index exactly once. Backfill *after*
replay would double-count every replayed row. Over-replay of a
partially-flushed family is idempotent for the index too: a re-applied upsert
resolves through `enforce_unique_pk` into retract(old)+insert(new) whose
index projections net to zero against the backfilled entry, and a re-applied
DELETE resolves against an absent row and projects nothing
(`catalog/store_io.rs:464-485` documents the base-table half of this).

In a Standalone process the same call is a legal idempotent
re-create-and-rebuild at the parent dir — unit-testable without touching the
global role.

### Stale rank dirs (`catalog/write_path.rs`)

Restarting with a smaller worker count would strand `w{k}` subdirs of live
index dirs forever (`gc_orphan_directories` treats a live index dir as
terminal; each worker's Rederive erase cleans only its own dir). Extend the
live-index arm of `gc_orphan_directories` (`write_path.rs:675-677`): remove
every `is_index_rank_dir_name` subdirectory before `continue`. It runs once
per boot on the master, pre-fork — no worker exists yet, and each worker
rebuilds its own subdir afterwards. Invariant: **rank copies never survive a
boot.**

### What each reader sees afterwards

> **Invariant.** Worker w's index table contains exactly the projection of
> worker w's local base slice: established at boot by `backfill_all_indexes`
> over the trimmed/rehomed flushed slice, extended by SAL replay and every
> live push through the single shared path `ingest_store_and_indices` (the
> SAL scatter guarantees a worker ingests only rows routed to it; replicated
> tables broadcast whole, making the local slice the full table on every
> worker). Hence an indexed value has a net-positive entry on worker w iff a
> live base row carrying it lives on w, and the master's HAS_PK union over
> workers is exactly global existence — bugs 1–3 die.

- **Replicated owners**: pushes to a replicated table broadcast whole to
  every worker (`master/dispatch.rs:1698-1705`), trim exempts the store and
  rehome gives each worker its own full flushed copy
  (`partition_lsn.rs:50-108`), so boot and live agree on a full per-worker
  index — identical to today's live behavior. Every distributed consumer is
  multiplicity-insensitive: the `FLAG_HAS_PK` verdicts test only non-empty
  results (`preflight.rs:919-938`), the upsert arm is a membership test, and
  `fan_out_seek_by_index_async` collapses W identical holders to one slot
  (`dispatch.rs:961-987`).
- **Master**: `validate_unique_indices` keeps its code unchanged. On the
  master its committed-holder cursor now never hits (empty index) — the
  function reduces to the still-valid intra-batch checks (`seen`, weight>1 on
  non-unique_pk, forged-retraction pairing), and committed collisions are
  fully covered by the distributed pipeline that runs immediately after on
  the same path (`executor.rs:883`). The `unique_filters` machinery never
  read the master's index (warmup fans base-table scans to workers,
  `unique_filter.rs:114-211`), so nothing else changes. In Standalone
  processes the full committed-holder logic stays exercised against a
  populated parent-dir index — the unit tests keep their meaning.
- **CREATE INDEX replayed from the SAL tail** (pre-fork,
  `recover_system_tables_from_sal`, `runtime/bootstrap.rs:144-169` →
  `apply_local` fires hooks, `write_path.rs:104-115`; ctx is already live):
  the master registers the circuit, creates the parent-dir Table, and skips
  the backfill (and with it the redundant live dup re-check — the original
  CREATE passed `validate_unique_index_create_async` before it ever reached
  the SAL). Workers fork after this replay and rebuild slice-local via
  `backfill_all_indexes`.
- **`promote_index_to_unique`** (`ddl.rs:531-544`): unchanged. The promote
  shape still emits an IDX_TAB +1 row with the unique payload bit, so the
  executor's distributed preflight runs for it (`executor.rs:1489-1521`);
  the hook's live re-scan on the master is a no-op over closed partitions and
  on a worker is a within-slice subset check that cannot false-positive.
- **`seek_by_index`**: strictly improves — every live index entry now
  resolves against the local base slice by construction; the orphan-skip in
  `resolve_source_pks` remains as a within-epoch-window guard rather than a
  stale-masking crutch.

## Deletions / simplifications ledger

| Change | Where |
|---|---|
| `set_worker_rank` call in `WorkerProcess::new` deleted (single owner: the fork site) | `worker/mod.rs:346` |
| Internal `check_dups` policy in `stream_index_projection` deleted (hoisted to callers) | `ddl.rs:500` |
| Boot-recovered full-replicated index copies (the entire foreign fraction, its RAM, and its staleness) | design |
| The master's populated index copies (dead weight; readers subsumed by the distributed pipeline) | design |

---

## Tests

Rust (`make test`):

- `catalog/tests/reopen_rebuild_tests.rs`: existing tests unchanged
  (Standalone role keeps the hook backfill on reopen). Add
  `backfill_all_indexes_rebuilds_exactly_once`: create table + rows + index,
  flush, reopen (hook rebuild fires), assert per-key weights via
  `seek_by_index`; call `backfill_all_indexes()` and assert identical results
  (replace-then-rebuild, not additive); call it again (idempotent).
- `catalog/tests/index_tests.rs`: add a variant asserting
  `validate_unique_indices` still rejects a committed duplicate after a
  `backfill_all_indexes` rebuild.
- No unit test sets the process role (thread-shared statics; role paths are
  e2e-covered).

E2E (`make e2e`, `GNITZ_WORKERS=4`, in
`crates/gnitz-py/tests/test_crash_recovery.py` using
`_start_server`/`_restart_server`):

1. `test_unique_reinsert_after_restart`: `t(id PK, u UNIQUE)`; INSERT u=5;
   restart; DELETE the row; INSERT u=5 again → must succeed (today: false
   violation from stale copies — bugs 1 and 4).
2. `test_fk_no_orphan_after_restart`: `P(id PK, u UNIQUE)`,
   `C(id PK, f REFERENCES P(u))`; INSERT P(u=5); restart; DELETE the P row;
   INSERT C(f=5) → must FAIL with an FK violation, and a SELECT proves C is
   empty (today: the orphan is admitted — bug 2).
3. `test_fk_restrict_update_after_restart`: same P/C; INSERT P(u=5), C(f=5);
   DELETE C; restart; `UPDATE P SET u = 6` → must succeed (today: false
   RESTRICT — bug 3).
4. `test_secondary_index_select_after_restart`: non-unique index, rows across
   partitions, restart, indexed SELECT plus post-restart INSERT/DELETE and
   the same SELECT again — asserts exactly-once multiplicity end-to-end
   (guards the backfill-before-replay seam).

---

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`)
after each.

- [ ] 1. **Process roles + rank at fork.** `worker_ctx` ROLE tri-state,
  `set_master_role()` at the top of `server_main`, `set_worker_rank` moved to
  the fork child (before any catalog work), deleted from
  `WorkerProcess::new`. No index semantics change yet (the hook gate is not
  wired to the role): boot-compiled plans get real rank/num_workers and
  per-rank scratch isolation.
- [ ] 2. **Slice-local index unification** (atomic — gating the master
  without the worker rebuild would be strictly worse): `index_table_dir` +
  `is_index_rank_dir_name`; hook gate + rank-dir homing; `check_dups`
  parameter; `backfill_all_indexes` + `replace_index_table`; boot-phase
  insertion with the `boot_err` rename; `gc_orphan_directories` rank-dir
  wipe; the Rust unit tests.
- [ ] 3. **E2E regression proofs**: the four restart tests above (they pass
  only on top of commit 2; keeping them separate makes commit 2's e2e run a
  no-regression check and this commit the bug-fix proof).
