# Unified table lifecycle and ephemeral-state checkpointing

Give ephemeral state (views, secondary indexes, DBSP operator traces) a durable
checkpoint so a restart resumes from the last checkpoint plus the SAL tail
instead of rebuilding every view from scratch — and do it by unifying the
durable and ephemeral table lifecycles into one: **no table fsyncs or publishes
a manifest between checkpoints; the checkpoint barrier is the single durability
point for all LSM state; the fsynced SAL alone carries durability in between.**
This also fixes a confirmed live-correctness bug (§ Bug) in which a checkpoint
silently discards buffered view deltas.

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

## Bug fixed by this plan

The committer checkpoints **before** committing the pending push batch
(`committer.rs:145-166`), and `handle_flush_all` begins with
`self.pending_deltas.clear()` (`worker/mod.rs:1460`). Any committed-but-unticked
push deltas buffered at that moment are discarded; the base table keeps the
rows (ingested at push time and flushed by the checkpoint), but no later tick
ever sees them, so **views silently diverge from base tables until a restart
rebuilds them**. Reproduced deterministically (W=4,
`GNITZ_CHECKPOINT_BYTES=4096`): push 2000 rows, push 3 more (triggers the
checkpoint first), scan — table 2003 rows, view 3 rows; worker logs show
501+499+501+499 = 2000 buffered delta rows discarded. Restart heals it via
rebuild. The fix — drain ticks before the flush and delete the `clear()` — is
the first commit of this plan.

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
  `pending_deletions` — which is drained only after the next barrier publish.
  Between checkpoints, superseded compaction inputs and their replacements
  coexist on disk (bounded by one checkpoint interval).
- At the barrier, a dirty table's flush is: push memtable snapshot into
  `in_memory_l0` (existing Ephemeral flush), fold to one run, write that run
  as one shard (the spill path, minus the ceiling check), register it, and
  prepare the manifest — then the barrier fsyncs **every file referenced by
  the new manifest** (open-by-path → fdatasync → close; `ShardEntry.filename`
  holds full paths, `shard_index/mod.rs:49`; clean files cost ~1 µs, so no
  dirty-file tracking exists), renames the manifest, fsyncs dirs, and drains
  `pending_deletions`. Clean tables (empty memtable, empty L0, no index
  changes since last publish) are skipped exactly as today's `Empty` path
  skips them.

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
- Each RAM run carries a **bloom filter**: inherited from the memtable at
  snapshot push (the run *is* the memtable content;
  `may_contain_pk`/`bloom_add_batch`, `memtable/lookup.rs:47-49`,
  `memtable/runs.rs:54-58`) and rebuilt during a fold (one hashing pass over
  rows the merge already touches). Store as
  `in_memory_l0: Vec<InMemRun { batch: Rc<Batch>, bloom: BloomFilter }>` with
  an accessor yielding the batches for cursor gathering, so
  `open_cursor`/`gather_runs` are untouched. `scan_inmem` gates each run's
  binary search on its bloom. No per-run XOR8 and no per-run PK-uniqueness
  certificate: correctness never needs them, and `is_pk_unique = false` for
  Batch cursor sources (`read_cursor/mod.rs:190-194`) is the existing
  behavior whenever a memtable is non-empty.

### Manifest V6 and conditional load for Rederive tables

- Manifest header gains a `generation: u64` field → `VERSION_V6`
  (`manifest.rs:33`). The generation is written at barrier publish;
  `SalReplay` tables write it too (uniform format) but never check it.
- `Table::new` gains `expected_generation: Option<u64>`: `None` for
  `SalReplay` tables (always load, as today); `Some(g)` for `Rederive`
  tables — load the manifest iff `manifest.generation == g` (then
  `gc_orphans`, seed `current_lsn`), else `erase_stale_shards` and start
  empty. An absent manifest is invalid. `erase_stale_shards`'s prefix set
  drops the `eph_shard_` prefix (naming unified).
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

Checkpoint validity, evaluated wherever a `Rederive` table is constructed
(master pre-fork for view output stores, workers at compile time for scratch
tables):

```
checkpoint_valid_globally = recorded_topology == (launched_workers << 32 | STATE_FORMAT)
table_valid               = checkpoint_valid_globally
                            && manifest exists
                            && manifest.generation == committed_generation
```

### The checkpoint sequence

One protocol replaces `run_checkpoint_phase`; there are no checkpoint
"flavors". FLAG_FLUSH gains a sibling wire constant `FLAG_FLUSH_EPH`
(gnitz-wire), dispatched inline in both worker contexts exactly like
FLAG_FLUSH; the group header's `lsn` field carries the generation stamp for
the ephemeral round. `handle_flush_all(mode)` iterates `SalReplay` families
for the base round; for the ephemeral round it iterates `Rederive` families
(view output stores, index circuits) **plus** every compiled plan's
`VmHandle.owned_tables` via a new `DagEngine` iterator over the plan cache
(clearing `owned_cursor_handles` first, per the `refresh_owned_cursors`
pattern, `vm/mod.rs:197-229`; sound because no epoch runs at the barrier and
the worker is single-threaded). `pending_deltas.clear()` is deleted.

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
   ACKs (each worker flushes Rederive state, manifests stamped gen)
   → sal.checkpoint_reset() → release Quiesce.
```

The committer already holds the tick ledger (`tick_rows`/`tick_tids`,
`committer.rs:82-84`); it gains a tick-trigger sender the same way it already
holds `fire_auto_tick` (a closure over `tick_tx`, `executor.rs:233-235`). No
new trigger variants: `Drain` and `Quiesce` are reused as two sequential
awaits — the tick loop processes `Quiesce` before ticking
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
   View output stores and master-built index tables are constructed with
   `expected_generation` — valid state loads, invalid state erases.
   `backfill_index` runs only for invalid index state (its existing
   flushed-shards scan plus the per-worker replay projection covers the tail
   exactly as today).
2. Workers (post-fork): `recover_from_sal` replays FLAG_PUSH into base tables
   as today, and **keeps** each zone's effective delta —
   `replay_ingest` returns it (it already computes it and discards it,
   `store_io.rs:473-483`) — buffering into the worker's `pending_deltas`,
   exactly like live `handle_push`. The boot flush
   (`runtime/bootstrap.rs:507-512`) is deleted.
3. Master, after all worker ACKs: **pre-live tick drive** — for every base
   table id in source-depth order, `drain_tick_blocking(src)` (the existing
   CREATE-VIEW primitive, `dispatch.rs:839-848`; empty sources are no-op
   ticks). Each source's entire buffered delta is driven as **one
   consolidated epoch**, so high-churn tails collapse to net rows before
   touching circuits. This brings every generation-valid view from its
   checkpoint cut to the present.
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
leftover manifest is removed with the dir. The Delay (z⁻¹) register is
rejected at compile time for engine-registered circuits (SQL never emits it;
the circuit-builder API loses access to it on the server side — remove the
opcode path rather than gating it).

### Deletions (net-simpler ledger)

| Deleted | Where |
|---|---|
| Inline fsync arm from the ingest path (reachable only via `flush_durable`) | `flush.rs:232-263` callers |
| `force_ephemeral` param + `ingest_owned_batch_memonly` | `table/mod.rs:303-359` |
| Mid-epoch manifest publish + dir fsync + `try_cleanup` in compaction | `table/mod.rs:616-637` |
| Per-flush manifest prepare/rename in overflow flush | `flush.rs:100-174` (moves to barrier) |
| `pending_deltas.clear()` | `worker/mod.rs:1460` |
| Worker boot flush | `runtime/bootstrap.rs:507-512` |
| Unconditional `erase_stale_shards` at open (survives only as the invalid branch) | `table/mod.rs:242-244` |
| `eph_shard_{pid}_{seq}` naming + `(0,0)` spill LSNs | `flush.rs:356-380` |
| `scan_inmem_weight` (folded into `scan_inmem`) | `table/mod.rs:488-496` |
| Delay opcode server-side | `vm`/`emit` Delay paths |

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
only command groups. A crash during step 3 after *some* workers renamed
view manifests is safe per-worker: state is partition-local, every renamed
manifest is stamped with the *current* committed generation and represents
exactly the post-drain cut = flushed base state, and un-renamed workers'
partitions rebuild. A Barrier-triggered re-run of steps 0-1 during the drain
re-bumps the generation, so step 3 stamps the latest value — read the stamp
at step-3 emission time.

**Coalesced recovery replay is state-exact.** Driving each source's summed
delta in one epoch, sources sequenced, preserves single-source-per-epoch;
for a bilinear join the second source's epoch sees the first's updated trace,
so the cross term `ΔA ⋈ ΔB` is emitted exactly once (the same
across-epochs argument as the shared-source-branch case). Weight-clamp
operators (distinct, positive_part) and reduce are functions of their
integrals, and integrals telescope over any delta decomposition. Per-epoch
*output deltas* differ from the original schedule; final accumulated state —
all recovery needs, since no client observes pre-live intermediates — does
not. Source ordering follows tick_tids insertion order as live ticks do.
This argument requires that no operator holds cross-epoch state outside its
integral tables — which is why the Delay opcode is removed server-side.

**`pending_deltas` never clears.** Every buffered entry has a matching
`tick_rows` bump on the same push (`committer.rs:503-511` /
`worker/mod.rs:1133-1137`), so auto-ticks (10k rows / 20 ms) and the
checkpoint drain bound the buffer, including for tables with no dependent
views (their ticks are cheap no-ops). Add a debug assertion coupling the two
paths.

**Deferred compaction cleanup is crash-safe.** The last published manifest's
files are never unlinked before the next publish (drain moves strictly after
barrier publish), so a crash loads the cut manifest over intact cut files;
unsynced mid-epoch compaction outputs are unreferenced orphans, removed by
`gc_orphans` at open. The barrier's fsync-everything-referenced rule is what
makes publishing a manifest over mid-epoch-written (unsynced) compaction
outputs safe.

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
  orphans GC'd.
- `recover_sequences` arms for seq_ids 4/5.

E2E (pytest, `make e2e`, always `GNITZ_WORKERS=4`):

- The delta-loss repro (base 2003 vs view 3 under
  `GNITZ_CHECKPOINT_BYTES=4096`) adapted as a regression test asserting
  view == base across a checkpoint. It fails today and must pass from the
  first commit.
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
- Sustained-ingest test at a small SAL (`GNITZ_SAL_BYTES=16 MiB`) crossing
  many checkpoints with concurrent scans: no wedge, views stay equal to base.

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

---

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`)
after each.

- [ ] 1. **Checkpoint drains ticks; delete `pending_deltas.clear()`.**
  Committer gains the tick-trigger sender; sequence = snapshot dirty tids →
  `Drain` (await done) → `Quiesce` (await acked) → existing single
  FLAG_FLUSH checkpoint → release. Barrier requests arriving mid-drain are
  serviced by running the existing checkpoint (no drain precondition — views
  are not yet persisted, so a mid-drain reset loses nothing durable).
  Delete the `clear()`. Add the delta-loss regression test (now passing)
  and the sustained-ingest test.
- [ ] 2. **DML RAM-tier readiness.** `FoundSource::InMemRun`, `scan_inmem`
  consolidation, global-net-weight arming, extended
  `get_weight_for_row_bytes`, per-run blooms (`InMemRun` struct), delete the
  `debug_assert`. Rust unit tests. No behavior change for durable tables yet
  (their L0 is still empty).
- [ ] 3. **Lifecycle unification.** All ingest overflow → RAM-L0; delete
  `force_ephemeral`/memonly and the ingest-reachable inline-fsync arm;
  `current_lsn` bumps on every ingest; unified spill naming + real LSNs;
  barrier flush (existing `handle_flush_all`) writes memtable+L0 as one
  consolidated shard per dirty table; `Persistence` → `RecoverySource`
  rename. E2E green proves recovery handles the wider replay window.
- [ ] 4. **No mid-epoch manifest publish.** Strip publish/cleanup from
  `compact_if_needed`; move manifest prepare/rename to the barrier;
  barrier fsyncs every manifest-referenced file by path; `pending_deletions`
  drains post-publish. Crash-sim compaction tests.
- [ ] 5. **Checkpoint records + manifest V6 + three-step sequence.**
  `SEQ_ID_CHECKPOINT_GEN`/`SEQ_ID_TOPOLOGY` + `recover_sequences` arms +
  `STATE_FORMAT`; manifest V6 with generation; `FLAG_FLUSH_EPH` wire
  constant + `handle_flush_all(mode)` split + `VmHandle.owned_tables`
  iterator; committer sequence becomes gen-bump → base round → drain →
  ephemeral round; boot and graceful shutdown run the sequence. Rederive
  manifests are now written and stamped but still erased at open (no reload
  yet) — state is discarded exactly as today, so behavior is unchanged.
- [ ] 6. **Recovery reload + pre-live tick drive.** Conditional manifest load
  for Rederive tables (master stores + worker compile-time scratch);
  `replay_ingest` keeps effective deltas → worker buffering; delete the boot
  flush; pre-live `drain_tick_blocking` sweep; erase-then-backfill for
  invalid views; Delay opcode removed server-side. Crash-window injection
  knob + e2e restart tests 1–5; restart benchmark; documentation updates.
