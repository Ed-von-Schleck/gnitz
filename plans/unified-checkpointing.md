# Unified table lifecycle and ephemeral-state checkpointing

Give ephemeral state (views, secondary indexes, DBSP operator traces) a durable
checkpoint so a restart resumes from the last checkpoint plus the SAL tail
instead of rebuilding every view from scratch — and do it by unifying the
durable and ephemeral table lifecycles into one: **no table fsyncs or publishes
a manifest between checkpoints; the checkpoint barrier is the single durability
point for all LSM state; the fsynced SAL alone carries durability in between.**

This plan assumes three properties of the surrounding engine, stated where they
are used: the checkpoint drains pending ticks before flushing and does not clear
`pending_deltas`; the barrier-flush commit path returns directory fds as
`OwnedFd`; and compaction output naming is reboot-stable and collision-free on
both the horizontal and vertical paths (a persisted, strictly-monotonic per-table
compaction sequence plus the destination guard key), which the deferred cleanup
relies on.

Format note: this plan changes the shard-manifest format and the checkpoint
wire protocol. Existing data directories are not readable afterwards (pre-alpha,
no compatibility concerns).

---

## Current mechanics (verified against source)

Storage (`crates/gnitz-engine/src/storage/lsm/`):

- One `Table` type; `persistence: Persistence { Durable, Ephemeral }`
  (`table/mod.rs:42-49,191`). `RelationKind::persistence()`
  (`query/dag/mod.rs:94-99`): base/system → Durable, views → Ephemeral;
  index circuits and compiler child tables are created Ephemeral
  (`dag/mod.rs:1051-1053`, `compiler/emit.rs:86-105`).
- **Durable ingest overflow fsyncs inline.** `upsert_owned_and_maybe_flush`
  (`table/mod.rs:331-359`) → `flush_inner(Durable)` (`flush.rs:232-263`):
  write shard `.tmp` + manifest `.tmp`, then `fdatasync(shard)`,
  `fdatasync(manifest)`, `fsync(dir)` — all synchronous on the worker's
  single-threaded event loop, per ~192 KiB overflow (75 % of the 256 KiB
  per-partition arena, `memtable/runs.rs:78-79`, `partitioned_table.rs:65-68`).
  Measured on the dev machine: ~2.0 ms median per overflow, > 50 % of it
  fixed manifest+dir commit cost — ~11 ms of event-loop time per MiB of hot
  ingest.
- **Ephemeral ingest overflow stays in RAM.** `flush_prepare_with`'s Ephemeral
  branch (`flush.rs:90-99`) pushes the consolidated memtable snapshot onto
  `in_memory_l0: Vec<Rc<Batch>>`, folds past `INMEM_COMPACT_THRESHOLD = 4`
  runs (`flush.rs:294-315`), and spills one consolidated **unsynced** shard
  past `EPHEMERAL_INMEM_CEILING = 4 MiB` (`flush.rs:338-396`,
  `write_shard_streaming(..., durable=false, ...)`), registered with
  `add_shard(path, 0, 0)` (`flush.rs:380`). Ephemeral tables have no manifest
  (`manifest_path = None`) and erase their files at open
  (`erase_stale_shards`, `table/mod.rs:242-244,706-729`).
- The DBSP read path is source-agnostic: `open_cursor`
  (`table/mod.rs:383-393`) concatenates memtable runs + `in_memory_l0` +
  shards into one `ReadCursor`; seeks are binary/gallop searches on every
  source, RAM or disk (`read_cursor/source.rs:75-92`). Join, distinct,
  positive_part, reduce, and the MIN/MAX value index all probe through this
  path only.
- The DML found-row path is **not** RAM-tier-aware: `retract_pk_bytes`
  (`table/mod.rs:514-566`) carries `debug_assert!(in_memory_l0.is_empty())`
  (`:521-524`); `get_weight_for_row_bytes` (`:570-594`) scans memtable +
  shards only. `has_pk_bytes` already scans the RAM tier via
  `scan_inmem_weight` (`:471-496`).
- Manifest V5 (`manifest.rs:33`): header MAGIC/VERSION/COUNT/global-max-LSN;
  per-entry `(table_id, pk_min, pk_max, min_lsn, max_lsn, filename, level,
  guard_key)`. Published atomically (serialize → `.tmp` → fdatasync → rename →
  parent-dir fsync, `manifest.rs:352-373`). At open,
  `current_lsn = shard_index.max_lsn() + 1` (`table/mod.rs:267-276`).
  `current_lsn` bumps only on Durable ingest (`table/mod.rs:343-345`).
- `compact_if_needed` (`table/mod.rs:616-637`) publishes the manifest, fsyncs
  the dir, and drains `pending_deletions` (`shard_index/index.rs:412-426`)
  mid-epoch — the only non-flush manifest publish.

Orchestration (`crates/gnitz-engine/src/runtime/orchestration/`):

- Durability contract: a client ACK for an upsert implies **SAL** fdatasync
  only (`committer.rs`, Phase D at `:525-540`). No code depends on shard
  durability between checkpoints; the master never reads worker shard files
  while live.
- Checkpoint today: committer, when `sal_needs_checkpoint()` (75 % of the
  1 GiB SAL mmap, `sal.rs:746-749,1191-1193`) or a relay-space Barrier
  (`committer.rs:145`), runs `run_checkpoint_phase` (`committer.rs:238-264`)
  holding `sal_writer_excl` across write + ACK-wait + post-ack: FLAG_FLUSH
  broadcast → each worker's `handle_flush_all` (`worker/mod.rs:1459-1511`)
  flushes all **user** tables two-phase (prepare → `uring_batch_fdatasync`,
  chunked at `FD_CHUNK_THRESHOLD = 256` → commit renames → deduped dir
  fsyncs); ephemeral tables return `DoneInline` and produce no files. Then
  `checkpoint_post_ack` (`master/dispatch.rs:1741-1750`):
  `flush_all_system_tables()` → `drain_checkpoint_gated_deletions()` →
  `sal.checkpoint_reset()` (epoch += 1, write_cursor = 0, `sal.rs:1195-1202`).
- View state advances on TICK, not PUSH: `handle_push`
  (`worker/mod.rs:1115-1141`) ingests the base delta and buffers the
  **effective** (post-unique-pk) delta into `pending_deltas[tid]`;
  `handle_tick` (`:1143-1158`) drains it and drives the DAG. Ticks are
  trigger-driven only (`tick_loop_async`, `executor.rs:388-492`;
  `TICK_COALESCE_ROWS = 10_000`, coalesce window `TICK_DEADLINE_MS = 20`);
  triggers are `Auto` (committer `fire_auto_tick`), `Drain{tids, done}`
  (scan barrier, `executor.rs:1190-1221`), `Quiesce{acked, release}`
  (CREATE-VIEW DDL, `executor.rs:443-457,1444-1455`).
- `run_tick` drops `sal_writer_excl` before awaiting worker ACKs
  (`executor.rs:522-535`), and workers process FLAG_FLUSH inline even inside
  an exchange wait (dispatch matrix, `worker/mod.rs:613-627,771-782`) — so a
  checkpoint can interleave with an in-flight tick today.
- Recovery replays only FLAG_DDL_SYNC (master, pre-fork) and FLAG_PUSH
  (worker, post-fork) from the SAL, skipping zones at or below each family's
  flushed watermark (`runtime/bootstrap.rs:86-137`); `replay_ingest`
  (`catalog/store_io.rs:473-483`) computes the effective delta and **discards
  it**. Views/indexes are rebuilt: `backfill_view` per worker
  (`catalog/ddl.rs:381-445`, boot call `runtime/bootstrap.rs:533-552`),
  `backfill_index` on the master during `replay_catalog`
  (`catalog/hooks.rs:602-652`), exchange views via `backfill_exchange_views`
  (`runtime/bootstrap.rs:210-217`). The synchronous per-source drain-tick
  primitive used by CREATE VIEW is `drain_tick_blocking`
  (`master/dispatch.rs:839-848`).
- Circuit identity is restart-stable: node ids are persisted in
  `sys_circuit_nodes/edges` and re-read (`compiler/load.rs:40-148`); operator
  child tables live at deterministic paths
  `{view_dir}/scratch_{child}_w{rank}` with names embedding
  `{view_id}_{node_id}` (`emit.rs:77-105`); they are owned by
  `VmHandle.owned_tables: Vec<Box<Table>>` (`vm/mod.rs:169`) in the per-worker
  plan cache (`dag/mod.rs:284`) and are **not** in `dag.tables`, so no flush
  path reaches them today.
- The only in-memory cross-epoch circuit state outside these tables is the
  Delay (z⁻¹) register batch (`vm/mod.rs:243-277`); no SQL plan emits Delay —
  it is reachable only from the circuit-builder C/Python API.
- `_sequences` (SEQ_TAB_ID = 7) is the KV-shaped system table
  (`seq_id: U64 PK → next_val: U64`, `sys_tables.rs:253-255`); seq_ids 1-3
  are reserved, 4..=15 free, ≥ 16 are user SERIAL. `recover_sequences`
  (`catalog/bootstrap.rs:309-329`) dispatches on seq_id at boot.
  `flush_all_system_tables` (`catalog/bootstrap.rs:425-434`) flushes all
  system tables durably and must succeed **before** `checkpoint_reset`
  (`dispatch.rs:1735-1740`).
- Worker count is a CLI arg (`--workers`), persisted nowhere. `NUM_PARTITIONS
  = 256` is fixed; base-table dirs are keyed by partition number, so base
  tables are layout-stable across worker-count changes; rank-stamped scratch
  dirs and range-homed replicated stores are not.

Measured basis for the constants and shape chosen here (dev machine, btrfs +
LUKS on NVMe): batched barrier for 64 files × 256 KiB–1 MiB = 20–60 ms total;
256 × 4 MiB (1 GiB dirty) ≈ 840 ms of which ≈ 64 % is buffered writing that
mid-epoch spills absorb; fdatasync of an already-clean file ≈ 1 µs (so
re-fsyncing everything referenced by a manifest is effectively free for clean
files); one consolidated shard per checkpoint beats many 256 KiB shards by
~24 % on total durable-write cost.

---

## Design

### One lifecycle for every table

`Persistence` is renamed to what it now actually means:

```rust
// storage/lsm/table/mod.rs
pub enum RecoverySource {
    SalReplay,  // base + system tables: unflushed tail recovered by SAL replay
    Rederive,   // views, indexes, operator traces: tail re-derived through circuits
}
```

Between checkpoints, **all** tables behave like today's Ephemeral tables:

- Ingest overflow takes the in-RAM branch unconditionally: memtable snapshot →
  `in_memory_l0`, fold at 4 runs, unsynced spill past the flat 4 MiB
  per-table ceiling (constant unchanged; renamed `INMEM_CEILING`). The
  aggregate un-spilled RAM across the cluster is bounded by un-checkpointed
  SAL bytes (≤ 75 % of `GNITZ_SAL_BYTES`, default ≈ 768 MiB) because every
  ingested byte flows through the SAL and spill frees RAM — no global memory
  budget exists or is needed. Document this bound where the ceiling is
  defined.
- `flush_inner`'s inline-fsync arm becomes unreachable from ingest. It
  survives only behind `flush_durable()` for system tables (master-side, DDL
  and `flush_all_system_tables`) — the sole synchronous-fsync entry point.
- The `force_ephemeral` parameter of `upsert_owned_and_maybe_flush`
  (`table/mod.rs:331,338-342`) and `ingest_owned_batch_memonly`
  (`:303-304`) are deleted; with one ingest behavior there is nothing to
  override.
- `current_lsn` bumps on **every** ingest (drop the `eff == Durable` guard at
  `table/mod.rs:343-345`). Spills register with real
  `(min_lsn = 0, max_lsn = current_lsn - 1)` instead of `(0, 0)`, and spill
  naming unifies to the durable scheme `shard_{tid}_{lsn}.db` (delete the
  `eph_shard_{tid}_{pid}_{seq}_{lsn}` pid/seq naming, `flush.rs:356-361`).
  Reopen then seeds `current_lsn = max_lsn() + 1` correctly for every table.
- **No manifest is published mid-epoch.** The per-flush manifest prepare +
  rename leaves `flush_prepare`/`flush_commit` (it moves to the barrier), and
  `compact_if_needed` (`table/mod.rs:616-637`) collapses to
  `should_compact()` + `run_compact()`: compaction writes its output files
  unsynced, swaps the in-memory index, and appends superseded files to
  `pending_deletions` (all three compaction paths do — `index.rs:180,307,397`)
  — which is drained only after the next barrier publish. Between checkpoints,
  superseded compaction inputs and their replacements coexist on disk. This
  transient overhead is bounded by one checkpoint interval's spill-plus-compaction
  write volume, itself bounded by the un-checkpointed SAL tail (≤ 75 % of
  `GNITZ_SAL_BYTES`) because on-disk shards arise only from spills and every
  ingested byte flows through the SAL. Cleanup cannot be pulled earlier than the
  barrier publish: mid-epoch, the superseded inputs are still referenced by the
  last-published (active on-disk) manifest, so unlinking them before the new
  manifest supersedes them would strand the cut manifest over deleted files on the
  next boot. A shorter checkpoint interval (lower `GNITZ_CHECKPOINT_BYTES`) trades
  durable-write cost for a tighter transient-disk bound.
- **Mid-epoch compaction defers its cleanup, so it relies on compaction output
  names being reboot-stable and collision-free across *both* the horizontal and
  vertical paths** — an output must never reuse the filename of any live shard
  (input, orphan, or a concurrently-written output), or the deferred cleanup below
  would unlink a live shard and orphan it under the published manifest (ENOENT on
  the next boot). The naming that satisfies this keys every compaction output on a
  persisted, strictly-monotonic per-table compaction sequence (restored from the
  manifest at boot, never reused across the table's lifetime) plus the
  *destination guard key* — so no two outputs, in any call sequence or across any
  restart, share a basename. An LSN-derived tag is insufficient: it is only safe
  on the horizontal path (per-guard `max_lsn` strictly increases per
  re-compaction) and leaves the vertical path collidable, because
  `commit_l0_to_l1` stamps every per-guard L1 output of one `run_compact` with one
  shared `l0_max_lsn`.
- At the barrier, a dirty table's flush is: push memtable snapshot into
  `in_memory_l0` (existing Ephemeral flush), fold to one run, write that run
  as one shard (the spill path, minus the ceiling check), register it, and
  prepare the manifest — then the barrier fsyncs **every file referenced by
  the new manifest** (open-by-path → fdatasync → close; `ShardEntry.filename`
  holds full paths, `shard_index/mod.rs:49`; clean files cost ~1 µs, so the
  barrier re-fsyncs every referenced file rather than tracking per-*file*
  dirtiness), renames the manifest, fsyncs dirs, and drains `pending_deletions`.
  The barrier shard must carry the durable path's **PK-unique tag**: the spill
  path passes `flags = 0` to `write_shard_streaming`, which drops
  `SHARD_FLAG_PK_UNIQUE` — for a base table that would forfeit the read-path
  payload-comparator skip on cross-source PK ties. Compute the flag exactly as
  the durable overflow flush does (`flush.rs:107-117`), gated on
  `can_tag_pk_unique`, over the run being written:

  ```rust
  // storage/lsm/table/flush.rs — barrier shard write flags (was the spill `0`)
  let flush_flags = if self.can_tag_pk_unique {
      let mut checker = PkUniqueChecker::new();
      for i in 0..run.count {
          checker.observe(run.get_pk_bytes(i), run.get_weight(i));
      }
      checker.flags_if(true)
  } else {
      0
  };
  shard_file::write_shard_streaming(
      dirfd, &name_c, self.table_id, run.count as u32, &run.regions(),
      false, flush_flags, // durable=false: the barrier's fsync sweep syncs it
  )?;
  ```

  The XOR8 filter needs no change — `write_shard_streaming` builds it
  unconditionally when `row_count > 0`; its `durable` bool gates only the
  per-file fsync, not filter construction, so the spill path never dropped it.
- **The barrier flush folds the memtable into the RAM tier before it decides
  whether there is anything to publish.** Ingest overflow already leaves a table
  with an *empty memtable and a populated `in_memory_l0`* (the overflow flush
  pushes the memtable snapshot into `in_memory_l0` and resets the memtable) — the
  common post-overflow state, not an edge case. So the barrier `flush_prepare`
  must first consolidate any residual memtable into `in_memory_l0`, and only
  *then* test whether the table is clean. Gating on the memtable alone — as the
  pre-unification `flush_prepare_with` did, with a `memtable.is_empty()` gate and
  a second `snapshot.count == 0` gate (`flush.rs:84,89`), both preceding the
  `in_memory_l0.push` — would drop a populated `in_memory_l0` on the floor at the
  barrier: silent data loss after the following SAL reset.

- **A table needs a barrier publish iff its on-disk manifest does not reflect its
  post-fold in-memory state**: `!in_memory_l0.is_empty() ||
  !shard_index.pending_deletions.is_empty()`, evaluated *after* the memtable is
  folded in. The `pending_deletions` disjunct is the load-bearing addition: once
  publication defers to the barrier, a table that compacts mid-epoch and then goes
  quiet ends the epoch with an empty memtable **and** empty `in_memory_l0` (the
  spill that triggered the compaction cleared it), yet the barrier still must
  publish so it can drain the superseded pre-compaction shards from
  `pending_deletions` — otherwise the last (pre-compaction) manifest survives
  pointing at unlinked files. Every compaction path appends to `pending_deletions`
  (`index.rs:180,307,397`), drained only after publish, so
  `!pending_deletions.is_empty()` is exactly "compacted since last publish"; no
  separate dirty flag is needed.

  There is a single fold-then-gate path (no second memtable-only gate — the
  memtable is folded into `in_memory_l0` up front):

  ```rust
  // storage/lsm/table/flush.rs — barrier Phase-1 flush (unified lifecycle).
  // Ingest overflow no longer reaches here: it takes the in-RAM branch
  // (memtable snapshot → in_memory_l0, fold, spill past the ceiling; no shard,
  // no manifest). This method runs only at the checkpoint barrier.
  pub fn flush_prepare(&mut self) -> Result<FlushOutcome, StorageError> {
      self.found_source = FoundSource::None;

      // 1. Fold the residual memtable into the RAM tier. `in_memory_l0` may
      //    already hold runs from mid-epoch overflow flushes; after this every
      //    live row is in `in_memory_l0` and the memtable is empty. The pushed
      //    run gets a fresh per-run bloom (§DML found-row path).
      if !self.memtable.is_empty() {
          let snapshot = self.memtable.consolidate_for_flush();
          if snapshot.count > 0 {
              self.cached_full_scan = None;
              self.in_memory_l0.push(InMemRun::from_batch(snapshot));
          }
          self.memtable.reset();
      }

      // 2. Clean iff nothing to write AND nothing compacted since last publish.
      if self.in_memory_l0.is_empty() {
          if self.shard_index.has_pending_deletions() {
              // Compaction-only: prepare_manifest (no new shard) → Pending{manifest}.
              let manifest = self.prepare_manifest()?;
              return Ok(FlushOutcome::Pending(Box::new(FlushWork::manifest_only(manifest))));
          }
          return Ok(FlushOutcome::Empty);
      }

      // 3. Fold the RAM tier to one net-state run and write it as one shard via
      //    the one-shot barrier write above (write_shard_streaming, durable=false,
      //    with the PK-unique flush_flags), register it, then prepare the manifest
      //    over the now-updated index. Retry-safe exactly like the spill path
      //    (flush.rs:322-378): a write failure leaves the run in `in_memory_l0`
      //    with nothing on disk; an add_shard failure unlinks the shard and keeps
      //    the run; on success the run clears from `in_memory_l0`.
      self.compact_in_memory();
      let run = Rc::clone(&self.in_memory_l0[0].batch);
      // ... write_shard_streaming(dirfd, &name_c, run, false, flush_flags) →
      //     add_shard(final_full, 0, current_lsn - 1) → in_memory_l0.clear() →
      //     prepare_manifest() → FlushOutcome::Pending(FlushWork { manifest }).
  }
  ```

  Both the shard-writing path and the compaction-only path end in the same
  no-pending `prepare_manifest` (a two-phase sibling of `publish_manifest`,
  `persist.rs:135`: `build_manifest_entries` over the current index →
  `manifest::prepare_file` to a `.tmp` → `PreparedManifest`), returning
  `FlushOutcome::Pending` with only the manifest set (`shard_fd = None`,
  `pending_shard = None`); they differ only in whether a shard was written and
  registered first. `flush_commit` renames that manifest alone — the shard, if
  any, is already at its final name via the one-shot write. The
  fsync-every-referenced-file sweep durably syncs the new shard and the
  already-written, unsynced compaction outputs before the manifest rename. Truly
  clean tables (empty `in_memory_l0`, no pending deletions) return `Empty` exactly
  as today's clean path skips them.

### DML found-row path learns the RAM tier

`enforce_unique_pk` (`dag/mod.rs:1790,1823-1841`) is the one caller that
breaks when base tables hold `in_memory_l0`:

```rust
// storage/lsm/table/mod.rs — FoundSource gains an owning in-RAM arm:
pub enum FoundSource {
    None,
    MemTable,
    InMemRun(Rc<Batch>, usize),          // NEW — Rc so the row outlives the probe
    Shard(Rc<MappedShard>, usize),
}
// found_row(): FoundSource::InMemRun(rc, idx) => FoundRow::Mem(rc.as_ref(), idx)
// — the existing FoundRow::Mem arm accepts any Batch; no new row-access code.
```

- Delete the `debug_assert!(self.in_memory_l0.is_empty())`
  (`table/mod.rs:521-524`).
- Consolidate `scan_inmem_weight` (`:488-496`) into one
  `scan_inmem(pk) -> (i64 net_weight, Vec<(Rc<Batch>, usize)>)` mirroring
  `scan_shards_for_pk_bytes` (`:653-669`); `has_pk_bytes` uses its weight,
  `retract_pk_bytes` its candidates.
- `retract_pk_bytes` arms the winning `FoundSource` by **global** net weight
  across all three tiers: extend `get_weight_for_row_bytes` (`:570-594`) with
  a loop over the RAM runs (mirror `MemTable::find_weight_for_row_bytes`,
  `memtable/lookup.rs:77-95`) and use it as the sole liveness oracle for
  multi-candidate resolution. Invariant note for the implementation: for a
  DML-enforced `unique_pk` table, per-(PK, payload) consolidated weight is
  always 0 or 1 with strict insert/delete alternation, so "memtable-positive
  but globally dead" is unreachable — global-net arming makes tier-local
  netting subtleties moot rather than relying on that argument.
- Each RAM run carries a **bloom filter**, built by a single
  `InMemRun::from_batch(Rc<Batch>)` constructor — one hashing pass keyed exactly
  like the memtable bloom (`pack_pk_be` of the OPK bytes, `memtable/lookup.rs:47-49`,
  `memtable/runs.rs:54-58`), count-sized. The same constructor serves both the
  snapshot push and the fold (the folded run is a fresh merge that cannot inherit
  a bloom anyway). Store as
  `in_memory_l0: Vec<InMemRun { batch: Rc<Batch>, bloom: BloomFilter }>` with an
  accessor (`in_memory_runs`) yielding owned batch handles for cursor gathering,
  so `open_cursor`/`gather_runs` are untouched. `scan_inmem` gates each run's
  binary search on its bloom. No per-run XOR8 and no per-run PK-uniqueness
  certificate: correctness never needs them, and `is_pk_unique = false` for
  Batch cursor sources (`read_cursor/mod.rs:190-194`) is the existing
  behavior whenever a memtable is non-empty.

### Manifest V6 and conditional load for Rederive tables

- Manifest header gains a `generation: u64` field → `VERSION_V6`
  (`manifest.rs:113-117`, written into the reserved header window at offset 40;
  the header stays 64 bytes). The generation is written at barrier publish;
  `SalReplay` tables write it too (uniform format) but never check it.
- **The write-side stamp reaches `manifest_header` without threading a parameter
  through the LSM API.** `committed_generation` is a process-global `AtomicU64`
  alongside `WORKER_RANK`/`NUM_WORKERS` in `foundation::worker_ctx` — workers are
  single-threaded processes, so the existing atomic-static pattern fits (a
  thread-local would not: the value is read deep inside `storage/lsm`, an L3
  module that may depend on the L0 `worker_ctx`). It holds the current committed
  generation at all times: set on the master when it bumps the generation
  (checkpoint step 0), and on each worker from the `FLAG_FLUSH_EPH(gen)` group
  header the instant the ephemeral round arrives, before `handle_flush_all`
  iterates tables. `ShardIndex::manifest_header` (`shard_index/persist.rs:141`)
  reads `worker_ctx::committed_generation()` into the V6 header. The same atomic,
  set to the recovered value at fork, feeds the scratch tables'
  `expected_generation` at compile time (below); a later live ephemeral round
  overwriting it does not disturb an already-captured `expected_generation`.
  Base/`SalReplay` flushes stamp whatever the atomic holds and never read it
  back, so their stamped value is immaterial — no separate base-round generation
  plumbing (e.g. a read from `_sequences` per flush) is needed.
- `Table::new` gains `expected_generation: Option<u64>`: `None` for
  `SalReplay` tables (always load, as today); `Some(g)` for `Rederive`
  tables — load the manifest iff `manifest.generation == g` (then
  `gc_orphans`, seed `current_lsn`), else `erase_stale_shards` and start
  empty. An absent manifest is invalid. `erase_stale_shards`'s prefix set
  drops the `eph_shard_` prefix (naming unified).
- **Worker compile-time scratch tables get `expected_generation` from the
  fork-inherited `committed_generation`**, not by threading a parameter through
  `compile_view`/`build_plan`/`emit_node` (all already wide). `create_child_table`
  (`compiler/emit.rs:105-124`) reads the worker-global `committed_generation` the
  same way it already reads `worker_rank()` for the scratch dir (`emit.rs:101`) —
  workers inherit the registry value across the fork (§Durable checkpoint
  records) — and passes `Some(committed_generation)` to `Table::new`. That value
  is correct in every case: a live CREATE VIEW's scratch has no manifest → erase
  → empty; a valid view's scratch manifest is at `committed_generation` (the
  flush-ordering invariant, §validity) → load; an invalid view's scratch
  mismatches → erase, and step 4 re-derives it anyway. `Persistence::Ephemeral`
  at the current `create_child_table` call becomes `RecoverySource::Rederive`.
- `pub const STATE_FORMAT: u32 = 1;` lives next to the manifest version.
  It must be bumped by any change to operator-state table schemas or
  shard/manifest layout (e.g. reindex payload pruning, value-index layout
  changes, trace-table elision) — a mismatch wipes all Rederive state at
  boot, which is always correct because Rederive state re-derives from base
  tables.

### Durable checkpoint records

Two reserved rows in `_sequences`, with `recover_sequences` match arms:

| seq_id | value |
|---|---|
| 4 `SEQ_ID_CHECKPOINT_GEN` | committed checkpoint generation (monotonic) |
| 5 `SEQ_ID_TOPOLOGY` | `(worker_count as u64) << 32 \| STATE_FORMAT as u64` |

Written via `ingest_to_family(SEQ_TAB_ID, ...)` + `flush_all_system_tables()`
(shard-durable; no SAL zone needed — the row must survive the SAL reset that
follows it, and recovery reads it from shards pre-fork). The registry holds
the recovered values (`committed_generation`, `recorded_topology`); workers
inherit them across the fork.

Checkpoint validity is a **per-relation, all-or-nothing** verdict — never
per-partition. A view's ephemeral state spans its output-store partitions (256
for a hashed store, `W` for a replicated one — see the enumeration below) plus
every worker's operator-trace ("scratch") tables; loading some partitions from a
generation-valid manifest while others start empty would let the whole-view
backfill double-count the loaded partitions, because `backfill_view` re-derives
the view and **adds** (`ingest_to_family(vid, result)`, `catalog/ddl.rs:428`) and
output stores are not `unique_pk`. So a relation is resumed only if **every**
manifest constituting it carries `committed_generation`; any single stale or
absent manifest erases the whole relation and re-derives it (Recovery step 4). No
table is ever loaded and then thrown away.

The verdict is computed once, on the master, from the authoritative output
manifests alone — made sufficient by a **flush-ordering invariant**: the
ephemeral round flushes a relation's trace/scratch tables **before** its
authoritative output store(s) (§checkpoint sequence). Within a worker, an output
manifest at generation `G` therefore implies that worker's traces for the
relation are already durable at `G`; and since the master reads *every*
output-partition manifest of the relation pre-fork (the enumeration below), "every
output partition at `G`" implies every worker completed both its trace and its
output flush — a correct all-or-nothing verdict from one pre-fork scan, with no
worker round-trip:

```
topology_valid  = recorded_topology == (launched_workers << 32 | STATE_FORMAT)
relation_valid  = topology_valid
                  && for every output-partition path P of the relation (below):
                       manifest(P) exists
                       && manifest(P).generation == committed_generation
```

**Enumerating a relation's output-partition paths follows its store shape
(`Routing`), not its read-gather predicate.** The master reads the shape off its
own pre-fork handle: a **hashed** output store (no replicated source) spans all
256 partitions `{dir}/part_0/manifest.bin … part_255/manifest.bin`; a
**replicated** output store (`Routing::Replicated`, built for any relation with a
replicated source, `hooks.rs:194`) is homed one partition per worker at
`part_{partition_range(w, W).0}` (`bootstrap.rs:26-35`: `chunk = 256 / W`,
`start = w · chunk`), so it spans exactly `W` manifests —
`{ part_{partition_range(w, W).0} : w ∈ 0..W }`, **not** 256 and **not**
`part_0..part_{W-1}` (at `W=4` those are `part_0, part_64, part_128, part_192`).
`W` is `launched_workers`, which `topology_valid` has already pinned to the
checkpoint-time worker count, so the range-homed offsets the master enumerates
match exactly what the workers flushed. This distinction is load-bearing in both
directions: reading only `part_0` would miss the **distinct** per-worker slices of
a replicated-*derived* view (partitioned ⋈ replicated: a replicated store shape
but a union read), and enumerating `0..256` would flag `256 − W` non-existent
partitions and force a needless rebuild every boot. The store-shape predicate is
`PartitionedTable::is_replicated()` on the master's handle (or equivalently the
relation's `has_replicated_source` = *any* source replicated), which is distinct
from `relation_output_is_replicated` (= *all* sources replicated; that one gates
single- vs union-read, not on-disk layout, `partition_lsn.rs:117`) — do not
substitute one for the other.

The master evaluates `relation_valid` per view (over its store-shape partition
set) and per master-built index, then broadcasts the invalid set as part of the
step-4 backfill coordination it already drives. The one verdict drives both
sides: workers load a relation's scratch iff its verdict is valid and erase it
otherwise, so output and scratch never disagree.

### The checkpoint sequence

One protocol replaces `run_checkpoint_phase`; there are no checkpoint
"flavors". FLAG_FLUSH gains a sibling wire constant `FLAG_FLUSH_EPH`
(gnitz-wire), dispatched inline in both worker contexts exactly like
FLAG_FLUSH; the group header's `lsn` field carries the generation stamp for
the ephemeral round. `handle_flush_all(mode)` iterates `SalReplay` families
for the base round; for the ephemeral round it flushes, **per relation, the
operator-trace tables before the relation's authoritative output store** — every
compiled plan's `VmHandle.owned_tables` (via a new `DagEngine` iterator over the
plan cache, clearing `owned_cursor_handles` first, per the
`refresh_owned_cursors` pattern, `vm/mod.rs:197-229`; sound because no epoch runs
at the barrier and the worker is single-threaded) first, then the `Rederive`
output families (view output stores, index circuits). This ordering is what makes
"output manifest at `G` ⟹ traces at `G`" hold per worker, so the master's
pre-fork output scan is a correct all-or-nothing verdict (§validity).

Committer sequence (steady state):

```
0. GEN BUMP (before anything else, before taking sal_writer_excl):
   committed_generation += 1  → ingest _sequences row → flush_all_system_tables().
   From this instant every existing Rederive manifest is stale; any crash
   anywhere below rebuilds views instead of silently staleifying them.
1. BASE ROUND (holding sal_writer_excl across write + ACKs + reset):
   write FLAG_FLUSH(base) → signal → all-worker ACKs
   → drain_checkpoint_gated_deletions() → sal.checkpoint_reset().
2. DRAIN (lock released; tick task acquires it per tick):
   snapshot dirty tids from the committer-held tick_rows/tick_tids ledger,
   send TickTrigger::Drain{tids, done}, await done;
   send TickTrigger::Quiesce{acked, release}, await acked.
   The drain runs against a just-reset, nearly empty SAL, so it cannot
   exhaust SAL space by construction. While awaiting `done`, the committer
   services only CommitRequest::Barrier by re-running steps 0-1 (base tables
   are clean then — ticks mutate views, not base — so the re-run is cheap);
   Push requests stay queued.
3. EPHEMERAL ROUND (holding sal_writer_excl across write + ACKs + reset):
   write FLAG_FLUSH_EPH(gen = committed_generation) → signal → all-worker
   ACKs (each worker flushes Rederive state — traces before output stores,
   §validity — manifests stamped gen)
   → sal.checkpoint_reset() → release Quiesce.
```

The committer already holds the tick ledger (`tick_rows`/`tick_tids`,
`committer.rs:82-84`) and a tick-trigger sender (a closure over `tick_tx` like
`fire_auto_tick`, `executor.rs:233-235`). No new trigger variants: `Drain` and
`Quiesce` are reused as two sequential awaits — the tick loop processes `Quiesce`
before ticking
(`executor.rs:450-457`), so drain must complete first as its own round-trip.

The checkpoint decision point in the committer loop (`committer.rs:145`) is
unchanged (`sal_needs_checkpoint() || (has_barriers && !sal_has_relay_space())`);
what runs is the sequence above. `checkpoint_post_ack` is absorbed into
steps 0-1 (its system-table-flush-before-reset ordering is preserved by the
gen bump's flush happening before the reset).

A checkpoint also runs at the end of boot (replacing the boot flush, below)
and on graceful shutdown (before FLAG_SHUTDOWN), so a clean restart recovers
instantly. There is no time-based trigger.

### Recovery

Boot sequence changes (in `runtime/bootstrap.rs` order):

1. Master: catalog open + `replay_catalog` + system-table SAL replay as
   today, now also recovering `SEQ_ID_CHECKPOINT_GEN`/`SEQ_ID_TOPOLOGY`.
   For each view (over its store-shape output-partition set — 256 for a hashed
   store, the `W` range-homed offsets for a replicated one, §validity) and each
   master-built index, the master computes the per-relation `relation_valid`
   verdict (§validity): valid → load every partition; invalid → erase the whole
   relation and mark it for backfill (step 4). No relation is loaded partially. `backfill_index` runs
   only for invalid index state (its existing flushed-shards scan plus the
   per-worker replay projection covers the tail exactly as today).
2. Workers (post-fork): `recover_from_sal` replays FLAG_PUSH into base tables
   as today, and per replayed zone now **keeps** the effective delta —
   `replay_ingest` returns it (it already computes it and discards it,
   `store_io.rs:473-483`) — buffering into the worker's `pending_deltas`,
   exactly like live `handle_push`. Replay is driven in windows interleaved
   with the drain sweep (step 3), not as one exhaustive pass. The boot flush
   (`runtime/bootstrap.rs:507-512`) is deleted.
3. Master, coordinating with the workers: **windowed pre-live tick drive.**
   `pending_deltas` is a per-tid in-RAM `Batch` that does not spill, and the
   uncheckpointed tail is up to 75 % of `GNITZ_SAL_BYTES` (≈ 768 MiB) with an
   effective-delta buffer ~2× that for update-heavy tails (retract + insert per
   unique-pk update) — so draining the *entire* tail in one epoch would spike
   recovery RAM to ~1.5 GiB/worker (worst on low worker counts, where each worker
   owns a larger partition share). Replay instead proceeds in **master-planned,
   byte-balanced LSN windows**, bounding peak `pending_deltas` to one window.

   The master plans the windows **pre-fork, from SAL headers alone — no payload
   decode.** It walks the committed-push tail exactly as `collect_committed_lsns`
   already does (`bootstrap.rs:61-79`: `try_read(offset)` → read `msg.lsn` /
   `msg.flags`, advance by the header's group byte length; `decode_wire` is never
   called), accumulating `FLAG_PUSH` group byte sizes and cutting a window
   boundary each time the accumulated bytes reach `RECOVERY_DRAIN_BYTES` (default
   64 MiB). Boundaries fall on `lsn` values; an atomic zone stamps its groups with
   one non-decreasing `lsn` and those groups are contiguous (`sal.rs`), so a
   boundary between zones never splits a zone. The output is an ordered list of
   LSN cut points covering `[checkpoint_cut, current_lsn]`. (The header carries
   `lsn` at `hdr+8`, `flags` at `hdr+20`, and the group byte length as the commit
   prefix / `advance`, all read before any payload decode — so the master has
   both quantities window-planning needs without decoding pushes it cannot
   interpret pre-fork.)

   Recovery then runs as master-driven rounds, all workers in lockstep (the
   existing backfill/drain lockstep). Round *k*: the master broadcasts window
   `[A_k, B_k)`; each worker resumes its append-ordered SAL scan from the prior
   window's end byte offset and replays only committed `FLAG_PUSH` groups with
   `A_k ≤ lsn < B_k` — ingesting the base delta and buffering the effective delta
   into `pending_deltas`, exactly like live `handle_push` (`replay_ingest` now
   returns the effective delta it already computes and discarded,
   `store_io.rs:473-483`) — then ACKs. The master then drives one
   `drain_tick_blocking` sweep over every base table in source-depth order
   (`dispatch.rs:839-848`; workers whose buffer for a source is empty issue empty
   ticks — required so exchanging views stay in step), draining `pending_deltas`
   and advancing every generation-valid view. The next round follows. Peak
   `pending_deltas` is bounded to one window's effective delta, independent of
   total tail size, while high-churn *within* a window still collapses to net
   rows.

   No worker→master "buffer full" signal is needed (the master fixed the
   boundaries up front) and no `FLAG_TICK` is read from the tail (the checkpoint
   reset cleared them; the tick sweeps are master-injected *between* windows). The
   one new worker mechanism is that `recover_from_sal` becomes **resumable** — it
   stops at the window's upper `lsn` bound and continues from that byte offset on
   the next round, instead of running to completion in a single pass. This brings
   every generation-valid view from its checkpoint cut to the present.
4. Master: for views whose state was invalid, invalidate their plans, erase
   their (tick-drive-polluted) scratch and output state, and run today's
   backfill paths (worker-local `backfill_view` + `backfill_exchange_views`).
   Driving first and erasing the invalid views' partial state afterwards
   costs bounded wasted work on a rare path and needs no per-view gating in
   the DAG driver.
5. Master: write the topology row for the *launched* worker count, then run
   the full checkpoint sequence (synchronous variant, like today's boot
   `reset_sal`: gen bump → FLUSH(base) → collect_acks → reset →
   FLUSH_EPH(gen) → collect_acks → reset; no drain needed — step 3 just
   drained everything and no pushes have been admitted). Freshly backfilled
   views are thereby durably checkpointed before the socket opens.

DDL interactions (all existing machinery): a view created after the last
checkpoint has no manifest at crash → invalid → backfill. A dropped view's
replayed `-1` DDL unregisters it and queues its directory for deletion, so a
leftover manifest is removed with the dir. The Delay (z⁻¹) opcode is purged
server-side: delete `Instr::Delay` (`vm/mod.rs`) and its exec arm (`vm/exec.rs`),
`RegisterKind::DelayState` + the `delay_state` constructor (`vm/mod.rs` — it has
no non-Delay users; register lifecycle keys only on `Delta`/`Trace`, so deleting
the variant needs no conditional rewrite), `add_delay` (`vm/builder.rs`), the
`OpNode::Delay` emit arm and its `dtor_port` match arm (`compiler/emit.rs`), and
the Delay VM tests. The `OpNode::Delay` *wire* variant stays in `gnitz-wire` (a
client can still encode `OPCODE_DELAY`), so the compiler must **reject** it with
a `CompileError` in `emit_node` rather than let it fall through — SQL never emits
Delay, and the circuit-builder API loses it on the server side. This purge is
required by the windowed-recovery correctness argument (no operator may hold
cross-epoch state outside its integral tables).

### Deletions (net-simpler ledger)

| Deleted | Where |
|---|---|
| Inline fsync arm from the ingest path (reachable only via `flush_durable`) | `flush.rs:232-263` callers |
| `force_ephemeral` param + `ingest_owned_batch_memonly` | `table/mod.rs:303-359` |
| Mid-epoch manifest publish + dir fsync + `try_cleanup` in compaction | `table/mod.rs:616-637` |
| Per-flush manifest prepare/rename in overflow flush | `flush.rs:100-174` (moves to barrier) |
| Worker boot flush | `runtime/bootstrap.rs:507-512` |
| Unconditional `erase_stale_shards` at open (survives only as the invalid branch) | `table/mod.rs:242-244` |
| `eph_shard_{pid}_{seq}` naming + `(0,0)` spill LSNs | `flush.rs:356-380` |
| `scan_inmem_weight` (folded into `scan_inmem`) | `table/mod.rs:488-496` |
| Delay opcode server-side (`Instr::Delay`, `RegisterKind::DelayState`/`delay_state`, `add_delay`, `OpNode::Delay` emit + `dtor_port` arms, VM tests; wire variant kept but `emit_node` rejects it with `CompileError`) | `vm`/`emit`/`builder` |

---

## Correctness arguments (implementation-relevant)

**Generation protocol crash windows.** The invariant: *at any SAL reset,
either (a) every push in the destroyed tail is reflected both in flushed base
tables and in every Rederive manifest stamped with the current committed
generation, or (b) the committed generation exceeds every Rederive manifest
stamp.* Step 0-before-step 1 establishes (b) before any base watermark can
advance — this closes the window where a worker's base-manifest rename lands
before the master's gen record (crash there → views stamped old-gen →
rebuild). Step 1's reset satisfies (b). Step 3's reset satisfies (a): no
pushes were admitted since step 1's reset, so the destroyed tail contains
only command groups. A crash during step 3 leaves a relation's manifests
split — some at the current generation, some at the prior one. This is safe
because validity is all-or-nothing per relation (§validity) and traces flush
before output: the master's pre-fork scan of the relation's output manifests
sees at least one partition below `committed_generation` whenever *any* of its
constituent flushes (trace or output, on any worker) did not complete, so the
whole relation erases and re-derives via backfill — no partition is ever loaded
partially. A Barrier-triggered re-run of steps 0-1 during the drain re-bumps the
generation, so step 3 stamps the latest value — read the stamp at step-3
emission time.

**Windowed recovery replay is state-exact.** Driving each source's buffered
delta as one or more consolidated windows, sources sequenced within each
window, preserves single-source-per-epoch; for a bilinear join a source's
window sees the other side's accumulated trace, so each cross pair
`ΔA_i ⋈ ΔB_j` is emitted exactly once across the interleaving (the same
across-epochs argument as the shared-source-branch case). Weight-clamp
operators (distinct, positive_part) and reduce are functions of their
integrals, and integrals telescope over **any** delta decomposition — so the
window boundary is a free choice: it bounds peak buffer RAM (§Recovery step 3)
without changing the fixpoint. Per-epoch *output deltas* differ from the
original schedule; final accumulated state — all recovery needs, since no
client observes pre-live intermediates — does not. Source ordering follows
tick_tids insertion order as live ticks do. This argument requires that no
operator holds cross-epoch state outside its integral tables — which is why
the Delay opcode is removed server-side.

**Deferred compaction cleanup is crash-safe.** The last published manifest's
files are never unlinked before the next publish (drain moves strictly after
barrier publish), so a crash loads the cut manifest over intact cut files;
unsynced mid-epoch compaction outputs are unreferenced orphans, removed by
`gc_orphans` at open. Two properties this rests on: (1) a compaction output
never reuses the filename of any live shard — guaranteed by the reboot-stable,
collision-free compaction naming described above (persisted monotonic
sequence + destination guard key, on both the horizontal and vertical paths),
without which a post-reboot or cross-guard collision would `renameat` over a
live shard or append it to `pending_deletions` and orphan it; (2) a table
dirtied only by compaction still publishes at the barrier before its
`pending_deletions` are drained — guaranteed by the `!has_pending_deletions()`
term in the Empty gate,
without which the pre-compaction manifest would survive while its shards are
unlinked. The barrier's fsync-everything-referenced rule is what makes
publishing a manifest over mid-epoch-written (unsynced) compaction outputs
safe.

**Pushes admitted during the drain would be safe but are not admitted.** A
push committed mid-drain lands in the post-reset SAL and in base memtables
(post-base-flush), and its deltas would be recovered by replay + tick drive
even though step 3's view flush misses them — the view stamp is still valid
because the SAL tail carries them. The committer nevertheless keeps pushes
queued during the sequence for simplicity; only Barrier is serviced.

---

## Tests

Rust (unit/integration, `make test`):

- `retract_pk_bytes`/`get_weight_for_row_bytes` with live rows in
  `in_memory_l0` on a `SalReplay` table: narrow/wide/signed PK, update pairs
  (same PK, two payloads in one run), cross-tier netting (insert → flush →
  delete → re-insert). Twins of the existing durable retract tests
  (`table/mod.rs:1108,1277,1354`) that leave rows unflushed.
- Fold rebuilds run blooms; probe gating equivalence (bloom hit/miss vs
  linear result).
- Manifest V6 round-trip; generation stamp preserved by compaction republish;
  `Table::new` conditional load (match → load, mismatch/absent → erase);
  spill registers real LSNs and reopen seeds `current_lsn`.
- Compaction with deferred cleanup: crash-sim by reopening from the old
  manifest after a compaction that did not republish — cut state intact,
  orphans GC'd. Compact-then-quiet: compact a table, leave its memtable and
  `in_memory_l0` empty, run a barrier, reopen — the manifest reflects the
  compacted index and every referenced shard exists.
- **Barrier flush of an empty-memtable, populated-`in_memory_l0` table** (the
  common post-overflow state): assert the fold-first gate writes the L0 shard and
  the table reloads intact — it must NOT return `Empty` and drop the RAM tier.
  Sibling gate cases in one test matrix: empty memtable + empty L0 +
  `has_pending_deletions()` → the no-shard `prepare_manifest` publish (manifest
  reflects the compaction-swapped index, no new shard); empty on all three →
  `Empty`.
- Barrier shard carries the PK-unique tag: barrier-flush a `unique_pk` base
  table with a live memtable+L0, read the shard header, assert
  `SHARD_FLAG_PK_UNIQUE` is set and the XOR8 filter is present (parity with the
  durable overflow flush, not the spill path's untagged `0`).
- `OpNode::Delay` is rejected: `load_circuit` a client circuit carrying
  `OPCODE_DELAY`, compile it, assert `CompileError` (the wire variant survives;
  the server refuses it).
- `recover_sequences` arms for seq_ids 4/5.

E2E (pytest, `make e2e`, always `GNITZ_WORKERS=4`):

- Same-dir SIGKILL-restart tests (reuse the `_start_server`/`_stop_server`/
  `_crash_and_restart` helpers in `crates/gnitz-py/tests/test_persistence.py:22-70`
  and the two crash-across/after-checkpoint tests as templates):
  1. checkpoint → more pushes → crash → restart: views correct (cut + tail).
  2. crash between checkpoint phases, driven by a debug-only
     `GNITZ_INJECT_CHECKPOINT_PANIC={gen|base|drain|eph}` knob (precedent:
     `GNITZ_INJECT_DDL_PANIC` in `test_crash_recovery.py`): after each
     injection point, restart must produce correct views (rebuild is
     acceptable; wrong weights are not — assert full view == expected).
  3. restart with a different `--workers`: correct views (rebuild path).
  4. CREATE VIEW after a checkpoint → crash → restart: view correct via
     backfill. DROP VIEW after a checkpoint → crash → restart: view gone,
     directory reclaimed.
  5. graceful shutdown → restart: correct views, and assert no backfill ran
     (log marker), demonstrating instant resume.
  6. **Replicated-view resume/rebuild** (the range-homed partition set, §validity):
     a view over replicated base tables (`Routing::Replicated`, homed at
     `part_{w·(256/W)}` per worker) — checkpoint → graceful restart resumes with
     no backfill (asserts the master enumerates the `W` range-homed offsets, not
     `part_0` alone and not `0..256`); then a variant where one worker's
     partition manifest is corrupted/absent before restart must rebuild the whole
     relation (no silent per-partition load). Include a replicated-*derived* view
     (partitioned ⋈ replicated: replicated store shape, union read) so the
     distinct per-worker slices are exercised, not identical copies.
- Large-churn-tail recovery: fill the SAL near its checkpoint threshold with
  updates to a hot base table feeding a view, SIGKILL before checkpoint,
  restart, and assert (a) views correct and (b) peak worker RSS stays near
  `RECOVERY_DRAIN_BYTES` rather than the full tail — proving the windowed
  drain bounds recovery memory.

Benchmark (validation, not a gate): a restart-time benchmark with a large
join trace (two ~1 M-row tables joined many-to-many) comparing time-to-first
correct scan after SIGKILL, before vs after this plan — checkpoints must
convert the O(base-through-circuits) rebuild into O(SAL-tail).

---

## Documentation updates (same commits as the code they describe)

- `CLAUDE.md` GnitzDB sections: the SAL durability contract paragraph
  (checkpoint now the sole shard-durability point, two flush rounds), the
  "views are rebuilt after restart" statements (now: resumed from checkpoint
  when generation-valid, rebuilt otherwise), and the `Persistence` →
  `RecoverySource` rename where mentioned.
- `async-invariants.md`: the checkpoint locking section (lock held per round,
  released across the drain; Quiesce parking; Barrier servicing rule), and
  the stale `next_lsn` "reset on checkpoint" comment (`master/mod.rs:309-313`)
  gets fixed in passing since that section is being rewritten.
- The `GNITZ_CHECKPOINT_BYTES` doc entry: note that deferred compaction cleanup
  keeps superseded compaction inputs on disk until the next barrier, so peak disk
  use carries a transient overhead bounded by one checkpoint interval's
  spill-plus-compaction write volume (≤ the SAL threshold); a smaller
  `GNITZ_CHECKPOINT_BYTES` tightens that bound at the cost of more frequent
  durable writes.

---

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`)
after each. This sequence assumes the engine already drains ticks before the
checkpoint flush (and does not clear `pending_deltas`), returns barrier-flush
directory fds as `OwnedFd`, and names compaction outputs collision-freely
(persisted monotonic sequence + destination guard key) — the three properties the
Design relies on.

- [x] 1. **DML RAM-tier readiness. DONE.** `in_memory_l0` is now `Vec<InMemRun>`
  (per-run PK bloom); `FoundSource::InMemRun` + `scan_inmem` (candidates) + a
  RAM-tier loop in `get_weight_for_row_bytes` make `retract_pk_bytes` arm the
  live row by global net weight across memtable + RAM + shards. The
  `debug_assert` is gone; durable base tables are unaffected until commit 2
  fills their L0.
- [x] 2. **Lifecycle unification. DONE.** Ingest overflow now lands in
  `in_memory_l0` for **every** table (`current_lsn` bumps per ingest, feeding the
  spill/barrier shard-naming floor). `force_ephemeral`/memonly, `flush_durable`,
  `flush_prepare_with`, `flush_inner`, and `flush_seq` are gone; ingest overflow
  routes through `flush_to_ram`. `flush_prepare` branches on `recovery_source`
  (Rederive → `DoneInline`; SalReplay → fold memtable + L0 into one consolidated,
  PK-unique-tagged shard behind a fold-first `in_memory_l0.is_empty()` clean-gate)
  and `flush_commit` clears L0. Spills use the unified `shard_{tid}_{lsn}.db`
  naming with real LSNs and are durable + PK-tagged for SalReplay tables (unsynced
  + untagged for Rederive). `Persistence` is renamed
  `RecoverySource {SalReplay, Rederive}` (open-time behavior unchanged).
  System-table checkpoint flush goes through `flush()`. Base tables are durable
  **only at the barrier** now — commit 3 defers the manifest publish and adds the
  `!has_pending_deletions()` gate + fsync-every-referenced-file sweep (which lets
  SalReplay spills revert to unsynced). E2E green proves recovery handles the
  wider replay window.
- [ ] 3. **No mid-epoch manifest publish.** Strip publish/cleanup from
  `compact_if_needed`; move manifest prepare/rename to the barrier;
  barrier fsyncs every manifest-referenced file by path; `pending_deletions`
  drains post-publish (safe because compaction output naming is reboot-stable and
  collision-free, so deferred cleanup never unlinks a live shard). The barrier
  `flush_prepare` folds the residual memtable into `in_memory_l0` **first**, then
  applies a single clean-gate `in_memory_l0.is_empty() && !has_pending_deletions()`
  — so a table with an empty memtable but a populated L0 (the common post-overflow
  state) writes its L0 shard instead of being swallowed by a memtable-only gate,
  and a compacted-then-quiet table (empty L0, pending deletions) takes the
  no-shard `prepare_manifest` publish path. Barrier shards carry the PK-unique tag
  (`PkUniqueChecker`, gated on `can_tag_pk_unique`), not the spill path's `0`.
  Tests: (a) barrier-flush a table with empty memtable + populated `in_memory_l0`
  and assert the L0 shard is written and reloads intact (not `Empty`); (b)
  crash-sim compact-then-quiet reopen — the manifest reflects the compacted index
  and every referenced shard exists.
- [ ] 4. **Checkpoint records + manifest V6 + three-step sequence.**
  `SEQ_ID_CHECKPOINT_GEN`/`SEQ_ID_TOPOLOGY` + `recover_sequences` arms +
  `STATE_FORMAT`; manifest V6 with generation, stamped via the
  `worker_ctx::committed_generation` process-global atomic (set on the master at
  gen-bump and on each worker from the `FLAG_FLUSH_EPH(gen)` header, read by
  `ShardIndex::manifest_header`); `FLAG_FLUSH_EPH` wire constant +
  `handle_flush_all(mode)` split + `VmHandle.owned_tables` iterator; committer
  sequence becomes gen-bump → base round → drain → ephemeral round; boot and
  graceful shutdown run the sequence. Rederive
  manifests are now written and stamped but still erased at open (no reload
  yet) — state is discarded exactly as today, so behavior is unchanged.
- [ ] 5. **Recovery reload + windowed pre-live tick drive.** Conditional
  manifest load for Rederive tables (master stores from the pre-fork per-view
  verdict, enumerated over the relation's store-shape partition set — 256 for a
  hashed store, the `W` range-homed offsets for a replicated one; worker
  compile-time scratch from fork-inherited `committed_generation`);
  `replay_ingest` keeps effective deltas → worker buffering; delete the boot
  flush; **master-planned byte-balanced LSN windows** (header-only pre-fork walk,
  `RECOVERY_DRAIN_BYTES`) driven as broadcast-window → resumable per-window
  `recover_from_sal` → `drain_tick_blocking` sweep rounds (peak `pending_deltas`
  ≤ one window, not the whole tail); erase-then-backfill for invalid views; Delay
  opcode removed server-side. Crash-window injection knob + e2e restart tests
  1–5; a large-churn-tail recovery test asserting bounded worker RSS; restart
  benchmark; documentation updates.
