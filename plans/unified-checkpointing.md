# Unified table lifecycle and ephemeral-state checkpointing

Give view state (output stores and DBSP operator traces) a durable checkpoint
so a restart resumes from the last checkpoint plus the SAL tail instead of
rebuilding every view through its circuits. The lifecycle half is shipped: no
table fsyncs or publishes a manifest on the ingest path anymore; between
checkpoints the fsynced SAL alone carries durability and every table's
overflow lives in the RAM tier. What remains is making the checkpoint barrier
the single durability point for all LSM state and teaching recovery to reload
generation-valid view state.

**Base-table secondary indexes are out of scope.** Every index table is one
`Table` per process over one *shared* directory `{owner_dir}/idx_{idx_id}`
(`catalog/hooks.rs:628-655`), written live by all W workers; checkpoint-
reloading that state would require per-process directory ownership — an
independent redesign — while its rebuild is already the cheap linear part of
boot: `backfill_index` (`catalog/ddl.rs:449`) is a stateless projection scan
over base shards on the pre-fork master, with no circuit evaluation and no
trace state. Indexes keep today's erase-at-open + rebuild forever under this
plan; only the multi-writer filename collision in their shared directories is
fixed here (commit 1), because the shipped naming unification made it live.

This plan assumes two properties of the surrounding engine, stated where they
are used: the checkpoint drains pending ticks before flushing and does not
clear `pending_deltas`; and the barrier-flush commit path returns directory
fds as `OwnedFd`.

Format note: this plan changes the shard-manifest format and the checkpoint
wire protocol. Existing data directories are not readable afterwards
(pre-alpha, no compatibility concerns).

---

## Current state (verified against source)

Shipped lifecycle (`crates/gnitz-engine/src/storage/lsm/`):

- `RecoverySource { SalReplay, Rederive }` (`table/mod.rs:48`): base + system
  tables are `SalReplay` (manifest loaded at open, SAL tail replayed); views,
  index circuits, and operator scratch are `Rederive` (erased at open via
  `erase_stale_shards`, `table/mod.rs:754`, prefixes `shard_{tid}_` and
  `hcomp_{tid}_`; rebuilt from sources).
- Ingest overflow folds into `in_memory_l0` for **every** table
  (`flush_to_ram`, `flush.rs:100`): memtable snapshot → per-run-bloomed
  `InMemRun`, fold at `INMEM_COMPACT_THRESHOLD = 4` runs, spill past
  `INMEM_CEILING = 4 MiB` (`table/mod.rs:37`). `current_lsn` bumps once per
  ingest. The DML found-row path (`retract_pk_bytes`,
  `get_weight_for_row_bytes`, `scan_inmem`) resolves live rows across
  memtable + RAM tier + shards by global net weight.
- Spill (`spill_in_memory_to_disk`, `flush.rs:366`): folds to one net-state
  run, writes `shard_{tid}_{lsn}.db` (`next_shard_name_and_lsn`,
  `flush.rs:114`) — durable + PK-unique-tagged (`shard_flush_flags`,
  `flush.rs:124`) for `SalReplay`, unsynced + untagged for `Rederive` —
  registers real LSNs, then `compact_if_needed`.
- Barrier/synchronous flush (`flush_prepare`, `flush.rs:152`): `Rederive` →
  fold to RAM, `DoneInline`; `SalReplay` → fold memtable + L0, single
  clean-gate `in_memory_l0.is_empty()`, then one consolidated PK-tagged shard
  `.tmp` + manifest `.tmp` (`prepare_manifest_with_pending`) → `Pending`.
  `flush_commit` (`flush.rs:234`) renames both and clears the RAM tier.
  `flush()` (`flush.rs:32`) is the synchronous wrapper; system tables go
  through it via `flush_all_system_tables` (`catalog/bootstrap.rs:425`).
- `compact_if_needed` (`table/mod.rs:667`) still publishes the manifest,
  fsyncs the dir, and drains cleanup mid-epoch — the only non-barrier publish
  left (removed by commit 2). Compaction outputs are
  `hcomp_{tid}_L{n}_G{guard}_{seq}.db` (`shard_index/index.rs:280`) keyed on
  the per-table `compact_seq`, persisted in the manifest header
  (`manifest.rs:116`) — no code parses any shard filename; gc and erase are
  prefix matchers.

Multi-writer directories (verified — motivates commit 1):

- **Index tables.** The `sys_indices` hook fires on the master (DDL/replay)
  and on every worker (`ddl_sync`); each process opens the same
  `{owner_dir}/idx_{idx_id}` and is fed one projected batch per base-table
  push (`ingest_store_and_indices`, `query/dag/mod.rs:1053`). The master is
  write-idle post-fork (`close_user_table_partitions`,
  `runtime/bootstrap.rs:583-584`; user pushes go to workers; its live
  CREATE-INDEX backfill streams from closed partitions), but all W workers
  write the shared dir concurrently, and their per-table `current_lsn`
  counters advance in near-lockstep (one bump per non-empty push slice). With
  pid-less names, two workers spilling the same index table write the **same**
  `shard_{tid}_{lsn}.db.tmp` and rename over each other
  (`write_shard_streaming`, `shard_file.rs:241`): interleaved `.tmp` writes,
  then each process mmaps whichever file won the rename — cross-process
  corruption, live, needing only a > 4 MiB-per-worker index. The
  `hcomp_*_{seq}` compaction names have the same cross-process collision
  (per-process `compact_seq`, also near-lockstep) — pre-existing.
- **Worker copies of `_sys` tables**, inherited across fork over master-owned
  dirs and fed by `ddl_sync`. Workers never barrier-flush them
  (`handle_flush_all` iterates `iter_user_table_ids()`,
  `worker/mod.rs:1484`, `catalog/registry.rs:11`; `flush_all_system_tables`
  is master-side), but a > 4 MiB spill would write `shard_{tid}_{lsn}.db`
  into the master's directory with a name the master's own barrier can mint.
- Every other directory is single-writer: base-table partition dirs are
  worker-owned; `_sys` barrier writes are master-only; `VmHandle` scratch dirs
  are rank-scoped (`{view_dir}/scratch_{child}_w{rank}`); replicated stores
  are rank-homed (`rehome_single_partition_stores`,
  `catalog/partition_lsn.rs:73`).

Orchestration (`crates/gnitz-engine/src/runtime/`), unchanged and relied on:

- Durability contract: a client ACK for an upsert implies **SAL** fdatasync
  only (committer Phase D). No code depends on shard durability between
  checkpoints; the master never reads worker shard files while live.
- Checkpoint today: the committer, when `sal_needs_checkpoint()` (75 % of the
  SAL mmap) or a relay-space Barrier (`committer.rs:145`), runs
  `run_checkpoint_phase` (`committer.rs:244`) holding `sal_writer_excl`
  across write + ACK-wait + post-ack: FLAG_FLUSH broadcast → each worker's
  `handle_flush_all` (`worker/mod.rs:1484`) flushes all user tables two-phase
  (prepare → `uring_batch_fdatasync` on the `FlushWork` fds → commit renames
  → deduped dir fsyncs); `Rederive` tables return `DoneInline`. Then
  `checkpoint_post_ack` (`master/dispatch.rs:1746`):
  `flush_all_system_tables()` → `drain_checkpoint_gated_deletions()` →
  `sal.checkpoint_reset()`.
- View state advances on TICK, not PUSH: `handle_push` (`worker/mod.rs:1125`)
  ingests the base delta and buffers the effective delta into
  `pending_deltas[tid]`; `handle_tick` (`:1153`) drains it and drives the
  DAG. Tick triggers are `Auto` (committer `fire_auto_tick`),
  `Drain{tids, done}` (scan barrier), `Quiesce{acked, release}` (CREATE-VIEW
  DDL); the tick loop processes `Quiesce` before ticking. `run_tick` drops
  `sal_writer_excl` before awaiting worker ACKs, and workers process
  FLAG_FLUSH inline even inside an exchange wait.
- Recovery replays only FLAG_DDL_SYNC (master, pre-fork) and FLAG_PUSH
  (worker, post-fork), skipping zones at or below each family's flushed
  watermark; `replay_ingest` (`catalog/store_io.rs:475`) computes the
  effective delta and **discards it**. Views are rebuilt: `backfill_view` per
  worker (`catalog/ddl.rs:381`), exchange views via
  `backfill_exchange_views`; indexes via `backfill_index` on the master
  during `replay_catalog`. The synchronous per-source drain-tick primitive is
  `drain_tick_blocking` (`master/dispatch.rs:844`). The worker boot flush
  lives at `runtime/bootstrap.rs:505-516`. The pre-fork committed-LSN walk
  that reads SAL group headers without payload decode is
  `collect_committed_lsns` (`runtime/bootstrap.rs:61`).
- Circuit identity is restart-stable: node ids persist in
  `sys_circuit_nodes/edges`; operator child tables live at deterministic
  rank-scoped paths and are owned by `VmHandle.owned_tables` in the
  per-worker plan cache — no flush path reaches them today. The only
  in-memory cross-epoch circuit state outside these tables is the Delay
  (z⁻¹) register; no SQL plan emits Delay (circuit-builder API only).
- `_sequences` (SEQ_TAB_ID = 7) is the KV-shaped system table; seq_ids 1-3
  reserved, 4..=15 free. `recover_sequences` (`catalog/bootstrap.rs:309`)
  dispatches on seq_id at boot. Worker count is a CLI arg, persisted nowhere;
  base-table dirs are keyed by partition number and layout-stable across
  worker-count changes; rank-stamped scratch dirs and range-homed replicated
  stores are not.

Measured basis (dev machine, btrfs + LUKS on NVMe): batched barrier for 64
files × 256 KiB–1 MiB = 20–60 ms; fdatasync of an already-clean file ≈ 1 µs
(re-fsyncing everything a manifest references is effectively free for clean
files); one consolidated shard per checkpoint beats many 256 KiB shards by
~24 % on total durable-write cost.

---

## Design

### 1. Process-scoped spill and compaction filenames

Spill filenames gain the writing process's pid:
`shard_{tid}_p{pid}_{lsn}.db` (in `next_shard_name_and_lsn`'s spill caller —
split the helper so the barrier keeps the pid-free form). Compaction outputs
gain it too: `hcomp_{tid}_p{pid}_L{n}_G{guard}_{seq}.db`
(`shard_index/index.rs:280` and the vertical/L0→L1 sites). Barrier shards
keep `shard_{tid}_{lsn}.db` — they are written only into single-writer
directories (worker-owned partitions, master-owned `_sys`).

Why this is sufficient and safe:

- No filename is parsed anywhere: `compact_seq` and shard LSNs are manifest
  header/entry fields (`manifest.rs:116`), and `gc_orphans` /
  `erase_stale_shards` match the `shard_{tid}_` / `hcomp_{tid}_` prefixes,
  which the pid segment preserves.
- Concurrent processes sharing a dir (index tables; a worker `_sys` copy vs
  the master) can no longer mint the same name — distinct pids.
- Across reboots, collision-freedom is carried by the persisted per-table
  `compact_seq` and by LSN monotonicity (reopen seeds
  `current_lsn = max_lsn() + 1`), exactly as today; a recycled pid cannot
  reuse a name because the seq/LSN component has moved past every persisted
  entry. Deferred cleanup (design §2) therefore never unlinks a live shard.
- The single-process uniqueness comment at `next_shard_name_and_lsn`
  (`flush.rs:114`) is rewritten: spills are process-scoped; barrier shards
  are single-writer by directory ownership.

### 2. No mid-epoch manifest publish (barrier-only durability)

`compact_if_needed` (`table/mod.rs:667`) collapses to `should_compact()` +
`run_compact()`: compaction writes its outputs unsynced, swaps the in-memory
index, and appends superseded files to `pending_deletions` (all three
compaction paths already do) — drained only after the next barrier publish.
Between checkpoints, superseded compaction inputs and their replacements
coexist on disk; the transient overhead is bounded by one checkpoint
interval's spill-plus-compaction write volume, itself bounded by the
un-checkpointed SAL tail (≤ 75 % of `GNITZ_SAL_BYTES`), because on-disk
shards arise only from spills and every ingested byte flows through the SAL.
Cleanup cannot be pulled earlier: mid-epoch, the superseded inputs are still
referenced by the last-published manifest, so unlinking them before the new
manifest supersedes them would strand the cut manifest over deleted files on
the next boot.

`SalReplay` spills revert to `durable = false` (the barrier's fsync sweep
below covers them; the PK-unique tag stays — it is a read-path property, not
a durability one).

The barrier `flush_prepare` changes in two ways from the shipped code
(`flush.rs:152`):

- **The clean gate gains a `pending_deletions` disjunct.** A table needs a
  barrier publish iff its on-disk manifest does not reflect its post-fold
  in-memory state: `!in_memory_l0.is_empty() ||
  shard_index.has_pending_deletions()`, evaluated after the fold. The
  disjunct is load-bearing: once publication defers to the barrier, a table
  that compacts mid-epoch and then goes quiet ends the epoch with an empty
  RAM tier, yet must still publish so its superseded pre-compaction shards
  can be unlinked — otherwise the last manifest survives pointing at files
  the deferred drain removes. `!pending_deletions.is_empty()` is exactly
  "compacted since last publish"; no separate dirty flag is needed.
- **The shard write becomes one-shot at its final name** (the spill path's
  `write_shard_streaming` with `durable = false` and the existing
  `shard_flush_flags`), replacing the `.tmp` + fd two-phase — the barrier's
  sweep now syncs by path, so no per-flush fd needs to survive to the fsync
  batch. `flush_commit` renames the manifest alone.

```rust
// storage/lsm/table/flush.rs — barrier Phase-1 tail (delta from the shipped
// flush_prepare; fold-first gate, helpers, and retry-safety are already in).
self.fold_memtable_into_l0();
self.compact_in_memory();
if self.in_memory_l0.is_empty() {
    if self.shard_index.has_pending_deletions() {
        // Compaction-only: publish the swapped index, no new shard.
        let manifest = self.prepare_manifest()?;
        return Ok(FlushOutcome::Pending(Box::new(FlushWork::manifest_only(manifest))));
    }
    return Ok(FlushOutcome::Empty);
}
let run = Rc::clone(&self.in_memory_l0[0].batch);
let (shard_name, lsn_max) = self.next_shard_name_and_lsn();
let flush_flags = self.shard_flush_flags(&run);
// write_shard_streaming(dirfd, &name_c, .., durable = false, flush_flags)
//   → add_shard(final_full, 0, lsn_max) → in_memory_l0.clear()
//   → prepare_manifest() → Pending(FlushWork { manifest, .. rest None }).
```

Both paths end in the same no-pending `prepare_manifest` (a two-phase sibling
of `publish_manifest`: build entries over the current index →
`manifest::prepare_file` to a `.tmp`), differing only in whether a shard was
written first. A write/register failure leaves the run in `in_memory_l0` with
nothing live on disk — retry-safe like the spill path.

**The barrier fsyncs every file referenced by the new manifest** before the
manifest rename: open-by-path → fdatasync → close (`ShardEntry.filename`
holds full paths). Clean files cost ~1 µs, so the sweep re-fsyncs everything
rather than tracking per-file dirtiness; this is what makes publishing a
manifest over mid-epoch-written (unsynced) compaction outputs and unsynced
spills safe. The worker's `handle_flush_all` fdatasync batch switches from
`FlushWork` fds to this by-path sweep (chunking at `FD_CHUNK_THRESHOLD`
unchanged), then commit renames, deduped dir fsyncs, then
`drain_pending_deletions`.

### 3. Manifest V6, checkpoint records, and the three-step sequence

- Manifest header gains `generation: u64` → `VERSION_V6` (written into the
  reserved header window; the header stays 64 bytes). Stamped at every
  publish; `SalReplay` tables write it too (uniform format) but never check
  it.
- **The stamp reaches `manifest_header` without threading a parameter through
  the LSM API**: `committed_generation` is a process-global `AtomicU64` in
  `foundation::worker_ctx` (the established atomic-static pattern; workers
  are single-threaded processes, and the value is read deep inside
  `storage/lsm`, an L3 module that may depend on L0). Set on the master at
  gen-bump (step 0 below) and on each worker from the `FLAG_FLUSH_EPH(gen)`
  group header before `handle_flush_all` iterates. Base flushes stamp
  whatever the atomic holds and never read it back.
- Two reserved `_sequences` rows with `recover_sequences` match arms:

  | seq_id | value |
  |---|---|
  | 4 `SEQ_ID_CHECKPOINT_GEN` | committed checkpoint generation (monotonic) |
  | 5 `SEQ_ID_TOPOLOGY` | `(worker_count as u64) << 32 \| STATE_FORMAT as u64` |

  Written via `ingest_to_family(SEQ_TAB_ID, ...)` +
  `flush_all_system_tables()` (shard-durable; the row must survive the SAL
  reset that follows it). The registry holds the recovered values; workers
  inherit them across the fork. `pub const STATE_FORMAT: u32 = 1;` lives next
  to the manifest version and must be bumped by any change to operator-state
  schemas or shard/manifest layout — a mismatch wipes all Rederive state at
  boot, always correct because it re-derives.
- `FLAG_FLUSH` gains a sibling wire constant `FLAG_FLUSH_EPH` (gnitz-wire),
  dispatched inline in both worker contexts exactly like `FLAG_FLUSH`; the
  group header's `lsn` field carries the generation. `handle_flush_all(mode)`:
  the **base round** iterates `SalReplay` user families as today; the
  **ephemeral round** flushes, per view, the operator-trace tables **before**
  the view's output store — every compiled plan's `VmHandle.owned_tables`
  (via a new `DagEngine` iterator over the plan cache, clearing
  `owned_cursor_handles` first per the `refresh_owned_cursors` pattern; sound
  because no epoch runs at the barrier) first, then the `Rederive` view
  output families. **Index tables are not flushed in either round** — they
  stay erase-at-boot (out of scope, goal section).

Committer sequence (steady state), replacing `run_checkpoint_phase` — no
checkpoint "flavors":

```
0. GEN BUMP (before anything else, before taking sal_writer_excl):
   committed_generation += 1 → ingest _sequences row → flush_all_system_tables().
   From this instant every existing Rederive manifest is stale; a crash
   anywhere below rebuilds views instead of silently staleifying them.
1. BASE ROUND (holding sal_writer_excl across write + ACKs + reset):
   write FLAG_FLUSH(base) → signal → all-worker ACKs
   → drain_checkpoint_gated_deletions() → sal.checkpoint_reset().
2. DRAIN (lock released; the tick task acquires it per tick):
   snapshot dirty tids from the committer-held tick_rows/tick_tids ledger,
   send TickTrigger::Drain{tids, done}, await done;
   send TickTrigger::Quiesce{acked, release}, await acked.
   The drain runs against a just-reset, nearly empty SAL, so it cannot
   exhaust SAL space. While awaiting `done`, the committer services only
   CommitRequest::Barrier by re-running steps 0-1 (base tables are clean
   then — ticks mutate views, not base); Push requests stay queued.
3. EPHEMERAL ROUND (holding sal_writer_excl across write + ACKs + reset):
   write FLAG_FLUSH_EPH(gen = committed_generation) → signal → all-worker
   ACKs (each worker flushes traces before output stores, manifests stamped
   gen) → sal.checkpoint_reset() → release Quiesce.
```

The committer already holds the tick ledger and a tick-trigger sender (a
closure over `tick_tx` like `fire_auto_tick`). No new trigger variants:
`Drain` and `Quiesce` are two sequential awaits. The checkpoint decision
point (`committer.rs:145`) is unchanged; `checkpoint_post_ack` is absorbed
into steps 0-1 (its system-table-flush-before-reset ordering is preserved by
the gen bump's flush). A checkpoint also runs at the end of boot (replacing
the boot flush) and on graceful shutdown (before FLAG_SHUTDOWN), so a clean
restart recovers instantly. There is no time-based trigger.

### 4. Conditional reload and windowed recovery

**Conditional load.** `Table::new` gains `expected_generation: Option<u64>`:
`None` for `SalReplay` tables (always load, as today); `Some(g)` for
`Rederive` view tables — load the manifest iff `manifest.generation == g`
(then `gc_orphans`, seed `current_lsn`), else `erase_stale_shards` and start
empty. An absent manifest is invalid. Worker compile-time scratch tables get
`expected_generation` from the fork-inherited `committed_generation`, read by
`create_child_table` the same way it already reads `worker_rank()` — no
parameter threading through `compile_view`/`build_plan`/`emit_node`. Index
tables keep unconditional erase (`None`-with-erase, today's behavior).

**Validity is a per-relation, all-or-nothing verdict — never per-partition.**
A view's state spans its output-store partitions plus every worker's scratch
tables; loading some partitions while others start empty would let the
whole-view backfill double-count (backfill **adds** via `ingest_to_family`,
and output stores are not `unique_pk`). The verdict is computed once, on the
master, from the authoritative output manifests alone — made sufficient by
the **flush-ordering invariant** (traces flush before outputs, §3): an output
manifest at generation `G` implies that worker's traces are durable at `G`,
so "every output partition at `G`" implies every constituent flush completed.

```
topology_valid  = recorded_topology == (launched_workers << 32 | STATE_FORMAT)
relation_valid  = topology_valid
                  && for every output-partition path P of the view:
                       manifest(P) exists
                       && manifest(P).generation == committed_generation
```

**Enumerating a view's output-partition paths follows its store shape
(`Routing`), not its read-gather predicate.** A **hashed** output store spans
all 256 partitions (`part_0..part_255`); a **replicated** store
(`Routing::Replicated`, built for any relation with a replicated source) is
homed one partition per worker at `part_{partition_range(w, W).0}`
(`chunk = 256 / W`, `start = w · chunk`) — exactly `W` manifests, **not** 256
and **not** `part_0..part_{W-1}` (at W=4: `part_0, part_64, part_128,
part_192`). `W` is `launched_workers`, already pinned by `topology_valid`.
This is load-bearing in both directions: reading only `part_0` would miss the
distinct per-worker slices of a replicated-*derived* view (partitioned ⋈
replicated: replicated store shape, union read), and enumerating `0..256`
would flag `256 − W` non-existent partitions and force a needless rebuild
every boot. The store-shape predicate is `PartitionedTable::is_replicated()`
on the master's pre-fork handle — distinct from
`relation_output_is_replicated` (all-sources-replicated, which gates single-
vs union-read, not layout); do not substitute one for the other.

The master evaluates `relation_valid` per view and broadcasts the invalid set
as part of the step-4 backfill coordination it already drives. The one
verdict drives both sides: workers load a view's scratch iff valid and erase
it otherwise, so output and scratch never disagree.

**Boot sequence** (in `runtime/bootstrap.rs` order):

1. Master: catalog open + `replay_catalog` + system-table SAL replay as
   today, now also recovering `SEQ_ID_CHECKPOINT_GEN`/`SEQ_ID_TOPOLOGY`.
   `backfill_index` runs unconditionally during `replay_catalog`, unchanged.
   For each view (over its store-shape partition set) the master computes
   `relation_valid`: valid → load every partition; invalid → erase the whole
   relation and mark it for backfill (step 4). No relation loads partially.
2. Workers (post-fork): `recover_from_sal` replays FLAG_PUSH into base tables
   as today, and per replayed zone now **keeps** the effective delta —
   `replay_ingest` returns it instead of discarding it
   (`catalog/store_io.rs:475`) — buffering into the worker's
   `pending_deltas`, exactly like live `handle_push`. Replay is driven in
   windows interleaved with step 3, not as one exhaustive pass. The boot
   flush (`runtime/bootstrap.rs:505-516`) is deleted.
3. Master: **windowed pre-live tick drive.** `pending_deltas` is an in-RAM
   `Batch` that does not spill, and the uncheckpointed tail is up to 75 % of
   `GNITZ_SAL_BYTES` (≈ 768 MiB) with an effective-delta buffer ~2× that for
   update-heavy tails — draining the entire tail in one epoch would spike
   recovery RAM to ~1.5 GiB/worker. Replay instead proceeds in
   **master-planned, byte-balanced LSN windows** bounding peak
   `pending_deltas` to one window:
   - The master plans the windows pre-fork from SAL headers alone — no
     payload decode — walking the committed-push tail exactly as
     `collect_committed_lsns` does (`runtime/bootstrap.rs:61`; header `lsn`
     at hdr+8, `flags` at hdr+20, group byte length from the commit prefix),
     cutting a boundary each time accumulated FLAG_PUSH bytes reach
     `RECOVERY_DRAIN_BYTES` (default 64 MiB). Boundaries fall on `lsn`
     values; an atomic zone's groups are contiguous with one non-decreasing
     `lsn`, so a boundary never splits a zone.
   - Round *k*: broadcast window `[A_k, B_k)`; each worker resumes its
     append-ordered SAL scan from the prior window's end byte offset,
     replays committed FLAG_PUSH groups with `A_k ≤ lsn < B_k` (ingest base
     delta, buffer effective delta), ACKs. The master then drives one
     `drain_tick_blocking` sweep (`master/dispatch.rs:844`) over every base
     table in source-depth order (workers with an empty buffer for a source
     issue empty ticks — required so exchanging views stay in step), then
     the next round. The one new worker mechanism is that `recover_from_sal`
     becomes **resumable** — it stops at the window's upper bound and
     continues from that byte offset next round.
   - No worker→master "buffer full" signal (boundaries are fixed up front)
     and no FLAG_TICK is read from the tail (the checkpoint reset cleared
     them; tick sweeps are master-injected between windows). This brings
     every generation-valid view from its checkpoint cut to the present.
4. Master: for views whose state was invalid, invalidate their plans, erase
   their (tick-drive-polluted) scratch and output state, and run today's
   backfill paths (`backfill_view` + `backfill_exchange_views`). Driving
   first and erasing afterwards costs bounded wasted work on a rare path and
   needs no per-view gating in the DAG driver.
5. Master: write the topology row for the *launched* worker count, then run
   the full checkpoint sequence synchronously (gen bump → FLUSH(base) → ACKs
   → reset → FLUSH_EPH(gen) → ACKs → reset; no drain — step 3 just drained
   everything and no pushes are admitted yet). Freshly backfilled views are
   durably checkpointed before the socket opens.

DDL interactions (all existing machinery): a view created after the last
checkpoint has no manifest at crash → invalid → backfill. A dropped view's
replayed `-1` DDL unregisters it and queues its directory for deletion.

**The Delay (z⁻¹) opcode is purged server-side**: delete `Instr::Delay` and
its exec arm, `RegisterKind::DelayState` + the `delay_state` constructor
(no non-Delay users; register lifecycle keys only on `Delta`/`Trace`),
`add_delay`, the `OpNode::Delay` emit arm and its `dtor_port` match arm, and
the Delay VM tests. The `OpNode::Delay` *wire* variant stays in `gnitz-wire`,
so `emit_node` must **reject** it with a `CompileError` — SQL never emits
Delay, and the circuit-builder API loses it server-side. Required by the
windowed-replay correctness argument (no operator may hold cross-epoch state
outside its integral tables).

### Deletions (net-simpler ledger)

| Deleted | Where |
|---|---|
| Mid-epoch manifest publish + dir fsync + `try_cleanup` in compaction | `table/mod.rs:667` |
| Barrier `.tmp`-shard + per-flush fd plumbing (one-shot write + by-path sweep replaces it) | `flush.rs:152-232` |
| Worker boot flush | `runtime/bootstrap.rs:505-516` |
| Unconditional `erase_stale_shards` at open for view tables (survives as the invalid branch; index tables keep it) | `table/mod.rs:267-270` |
| Delay opcode server-side (wire variant kept; `emit_node` rejects) | `vm`/`emit`/`builder` |

---

## Correctness arguments (implementation-relevant)

**Generation protocol crash windows.** Invariant: *at any SAL reset, either
(a) every push in the destroyed tail is reflected both in flushed base tables
and in every view manifest stamped with the current committed generation, or
(b) the committed generation exceeds every view manifest stamp.* Step
0-before-step 1 establishes (b) before any base watermark can advance. Step
1's reset satisfies (b). Step 3's reset satisfies (a): no pushes were
admitted since step 1's reset, so the destroyed tail holds only command
groups. A crash during step 3 leaves a view's manifests split across
generations — safe because validity is all-or-nothing per relation and traces
flush before outputs: the master's pre-fork output scan sees at least one
partition below `committed_generation` whenever any constituent flush did not
complete, so the whole relation erases and re-derives. A Barrier-triggered
re-run of steps 0-1 during the drain re-bumps the generation, so step 3
stamps the latest value — read the stamp at step-3 emission time.

**Windowed recovery replay is state-exact.** Driving each source's buffered
delta as consolidated windows, sources sequenced within each window,
preserves single-source-per-epoch; for a bilinear join a source's window sees
the other side's accumulated trace, so each cross pair `ΔA_i ⋈ ΔB_j` is
emitted exactly once (the same across-epochs argument as the shared-source-
branch case). Weight-clamp operators and reduce are functions of their
integrals, and integrals telescope over any delta decomposition — the window
boundary is a free choice bounding peak buffer RAM without changing the
fixpoint. Per-epoch output deltas differ from the original schedule; final
accumulated state — all recovery needs — does not. Source ordering follows
`tick_tids` insertion order as live ticks do. This requires that no operator
holds cross-epoch state outside its integral tables — hence the Delay purge.

**Deferred compaction cleanup is crash-safe.** The last published manifest's
files are never unlinked before the next publish (drain moves strictly after
barrier publish), so a crash loads the cut manifest over intact cut files;
unsynced mid-epoch compaction outputs are unreferenced orphans, removed by
`gc_orphans` at open. Two properties this rests on: (1) a compaction output
never reuses the filename of any live shard — per-table persisted
`compact_seq` + destination guard key within a process, the pid segment
(design §1) across processes, and seq/LSN monotonicity across reboots;
(2) a table dirtied only by compaction still publishes at the barrier before
its `pending_deletions` drain — the `has_pending_deletions()` gate disjunct
(design §2), without which the pre-compaction manifest would survive while
its shards are unlinked.

**Pushes admitted during the drain would be safe but are not admitted.** A
push committed mid-drain lands in the post-reset SAL and in base memtables,
and its deltas would be recovered by replay + tick drive even though step 3's
view flush misses them. The committer nevertheless keeps pushes queued during
the sequence for simplicity; only Barrier is serviced.

---

## Tests

Rust (unit/integration, `make test`):

- Commit 1: spill and compaction output filenames embed `std::process::id()`
  (assert the format on both paths; cross-process uniqueness then holds by
  construction); barrier shard names stay `shard_{tid}_{lsn}.db`;
  `erase_stale_shards` and `gc_orphans` still match pid-segmented names.
- Commit 2: compaction with deferred cleanup — crash-sim by reopening from
  the old manifest after a compaction that did not republish: cut state
  intact, orphans GC'd. Compact-then-quiet: compact, leave memtable and
  `in_memory_l0` empty, run a barrier, reopen — the manifest reflects the
  compacted index and every referenced shard exists (the
  `has_pending_deletions()` gate arm). Barrier gate matrix: empty RAM tier +
  pending deletions → manifest-only publish; empty on all three → `Empty`.
  Unsynced-spill-then-barrier reopen: every manifest-referenced file synced
  by the sweep.
- Commit 3: manifest V6 round-trip; generation stamp preserved by compaction
  republish; `recover_sequences` arms for seq_ids 4/5.
- Commit 4: `Table::new` conditional load (match → load, mismatch/absent →
  erase); `OpNode::Delay` rejected — `load_circuit` a client circuit carrying
  `OPCODE_DELAY`, compile, assert `CompileError`.

E2E (pytest, `make e2e`, always `GNITZ_WORKERS=4`):

- Same-dir SIGKILL-restart tests (reuse `_start_server`/`_stop_server`/
  `_crash_and_restart` in `crates/gnitz-py/tests/test_persistence.py` and the
  two crash-across/after-checkpoint tests as templates):
  1. checkpoint → more pushes → crash → restart: views correct (cut + tail).
  2. crash between checkpoint phases, driven by a debug-only
     `GNITZ_INJECT_CHECKPOINT_PANIC={gen|base|drain|eph}` knob (precedent:
     `GNITZ_INJECT_DDL_PANIC`): after each injection point, restart must
     produce correct views (rebuild acceptable; wrong weights are not).
  3. restart with a different `--workers`: correct views (rebuild path).
  4. CREATE VIEW after a checkpoint → crash → restart: view correct via
     backfill. DROP VIEW after a checkpoint → crash → restart: view gone,
     directory reclaimed.
  5. graceful shutdown → restart: correct views, and assert no backfill ran
     (log marker) — instant resume.
  6. Replicated-view resume/rebuild (the range-homed partition set):
     checkpoint → graceful restart resumes with no backfill (asserts the
     master enumerates the `W` range-homed offsets, not `part_0` alone and
     not `0..256`); a variant with one worker's partition manifest
     corrupted/absent must rebuild the whole relation. Include a
     replicated-derived view (partitioned ⋈ replicated) so the distinct
     per-worker slices are exercised.
- Large secondary index under sustained multi-worker ingest (enough rows that
  every worker's index slice spills past `INMEM_CEILING`), then index-seek
  correctness assertions — the regression surface for the commit-1 filename
  collision.
- Large-churn-tail recovery: fill the SAL near its checkpoint threshold with
  updates to a hot base table feeding a view, SIGKILL before checkpoint,
  restart, assert (a) views correct and (b) peak worker RSS stays near
  `RECOVERY_DRAIN_BYTES` rather than the full tail.

Benchmark (validation, not a gate): restart-time with a large join trace
(two ~1 M-row tables joined many-to-many), time-to-first-correct-scan after
SIGKILL, before vs after — checkpoints must convert the
O(base-through-circuits) rebuild into O(SAL-tail).

---

## Documentation updates (same commits as the code they describe)

- `CLAUDE.md` GnitzDB sections: the SAL durability contract paragraph
  (checkpoint becomes the sole shard-durability point, two flush rounds), and
  the "views are rebuilt after restart" statements (resumed from checkpoint
  when generation-valid, rebuilt otherwise; indexes always rebuilt).
- `async-invariants.md`: the checkpoint locking section (lock held per round,
  released across the drain; Quiesce parking; Barrier servicing rule), and
  the stale `next_lsn` "reset on checkpoint" comment in `master/mod.rs` gets
  fixed in passing.
- `GNITZ_CHECKPOINT_BYTES` doc entry: deferred compaction cleanup keeps
  superseded inputs on disk until the next barrier — peak disk use carries a
  transient overhead bounded by one checkpoint interval's write volume; a
  smaller `GNITZ_CHECKPOINT_BYTES` tightens that bound at the cost of more
  frequent durable writes.

---

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`)
after each.

- [ ] 1. **Process-scoped spill/compaction filenames.** Spills gain
  `_p{pid}_`; compaction outputs gain `_p{pid}_`; barrier shards unchanged;
  rewrite the uniqueness comment at `next_shard_name_and_lsn`; update the
  spill-naming unit tests and add the large-index spill e2e. Fixes the live
  cross-worker collision in shared index directories and the
  worker-`_sys`-copy hazard.
- [ ] 2. **No mid-epoch manifest publish.** Strip publish/cleanup from
  `compact_if_needed`; barrier `flush_prepare` gains the
  `has_pending_deletions()` gate disjunct and the manifest-only path, and
  writes its shard one-shot at the final name; the worker barrier fsyncs
  every manifest-referenced file by path and drains `pending_deletions`
  post-publish; `SalReplay` spills revert to unsynced (tag kept).
- [ ] 3. **Checkpoint records + manifest V6 + three-step sequence.**
  `SEQ_ID_CHECKPOINT_GEN`/`SEQ_ID_TOPOLOGY` + `recover_sequences` arms +
  `STATE_FORMAT`; V6 generation stamped via the
  `worker_ctx::committed_generation` atomic; `FLAG_FLUSH_EPH` +
  `handle_flush_all(mode)` split + the `VmHandle.owned_tables` iterator
  (traces before outputs; indexes excluded); committer sequence gen-bump →
  base round → drain → ephemeral round; boot and graceful shutdown run the
  sequence. View manifests are now written and stamped but still erased at
  open (no reload yet) — behavior unchanged.
- [ ] 4. **Recovery reload + windowed pre-live tick drive.** Conditional
  manifest load for view tables (master stores from the pre-fork per-view
  verdict over the store-shape partition set; worker scratch from
  fork-inherited `committed_generation`); `replay_ingest` keeps effective
  deltas → worker buffering; delete the boot flush; master-planned
  byte-balanced LSN windows (header-only pre-fork walk,
  `RECOVERY_DRAIN_BYTES`) driven as broadcast-window → resumable per-window
  `recover_from_sal` → `drain_tick_blocking` sweep rounds; erase-then-
  backfill for invalid views; Delay opcode removed server-side.
  Crash-window injection knob + e2e restart tests 1–6; large-churn-tail RSS
  test; restart benchmark; documentation updates.
