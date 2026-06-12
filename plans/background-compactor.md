# Background Compactor Thread

Move `Table::compact_if_needed` off the worker's request-handling
thread onto a worker-local background compactor thread. Today
compaction runs synchronously inside `create_cursor()`, which means
every tick that touches a trace pays whatever compaction work is
pending at that moment.

The goal is to make tick latency independent of compaction cost.
Workers stay forked OS processes; the new thread lives inside each
worker.

No prerequisites. The change is contained to `Table` /
`PartitionedTable` ownership and a small thread shim in `worker.rs`.

---

## Background

### Where compaction runs today

```rust
// crates/gnitz-engine/src/storage/table.rs:610-625
pub fn compact_if_needed(&mut self) -> Result<(), StorageError> {
    if !self.shard_index.should_compact() { return Ok(()); }
    self.found_source = FoundSource::None;
    self.shard_index.run_compact()?;
    if let Some(ref path) = self.manifest_path {
        self.shard_index.publish_manifest(path)?;
        if unsafe { libc::fsync(self.dirfd) } < 0 {
            return Err(StorageError::Io);
        }
    }
    self.shard_index.try_cleanup();
    Ok(())
}
```

Called from three sites, all on the worker thread, all synchronous:

| Caller | Frame |
|--------|-------|
| `Table::create_cursor` | `table.rs:413` — every cursor materialization |
| `Vm::setup_trace_cursors` | `vm.rs:564` — every tick that reads a trace |
| `dag.rs:build_ext_cursors` | `dag.rs:1197` — every tick that binds an external trace cursor |
| `PartitionedTable::create_cursor` | `partitioned_table.rs:128` — partitioned variant |

Each call:

1. Checks `needs_compaction` (cheap, `bool`).
2. If set, runs N-way merge of L0 (or further levels) via
   `compact::merge_and_route` — `O(rows_in_l0 × log L0)` with a
   tournament tree, plus disk writes for every output shard.
3. Publishes a new manifest file with `fsync` on the table's directory.
4. Cleans up old shard files.

Step 2 is the long pole. For a hot table with 4 full L0 shards this
can take milliseconds to tens of milliseconds; the worker is blocked
end-to-end for the duration, and the master cannot proceed until the
slowest worker acks.

### Why moving it off-thread is safe

The data structures already support it:

- **Shards are immutable.** `MappedShard` is `Rc`-owned and mmap'd
  read-only. A cursor that holds an `Rc<MappedShard>` keeps that
  shard alive even if `ShardIndex` removes the entry.
- **Compaction writes new files atomically.** `write_shard_atomic`
  writes to `.tmp` and renames. Concurrent readers see the old set
  until the manifest swap.
- **Manifest publish is single-writer.** Only the compactor writes
  new manifests for its own table. The query thread reads
  `shard_index` via the same `&mut Table` it already holds.

The only thing missing is a coordination channel between the query
thread (which observes `needs_compaction = true`) and the compactor
thread (which performs the merge and publishes the new index state).

---

## Design

### One thread per worker, not per table

A worker owns N tables. Spawning a thread per table balloons fast.
Instead: **one compactor thread per worker process**, fed by a
single MPSC channel that carries `CompactionRequest` messages keyed
by table ID. The thread drains the channel, runs the requested
compaction, and publishes the result back via a second channel
(`CompactionResult`) that the worker drains opportunistically.

```rust
// New: crates/gnitz-engine/src/storage/compactor.rs

pub struct CompactionRequest {
    pub table_id: u32,
    // Snapshot of the state needed to compact without touching Table:
    pub schema: SchemaDescriptor,
    pub output_dir: String,
    pub manifest_path: Option<String>,
    pub dirfd: i32,
    pub l0_filenames: Vec<String>,
    pub l0_lsn_ranges: Vec<(u64, u64)>,
    pub levels_snapshot: LevelSnapshot,   // existing guards + entries
    pub compact_seq_in: u64,
}

pub struct CompactionResult {
    pub table_id: u32,
    pub compact_seq_out: u64,
    pub new_l1_entries: Vec<(u128, String, u64, u64)>, // gk, path, min_lsn, max_lsn
    pub removed_l0_filenames: Vec<String>,
    pub deeper_level_updates: Vec<LevelUpdate>,        // cascaded L1→L2 etc.
    pub manifest_published: bool,
}

pub struct CompactorHandle {
    tx_req: Sender<CompactionRequest>,
    rx_done: Receiver<CompactionResult>,
    thread: JoinHandle<()>,
    in_flight: HashSet<u32>,   // table_ids with an outstanding request
}
```

The compactor thread runs:

```rust
fn compactor_loop(rx: Receiver<CompactionRequest>, tx: Sender<CompactionResult>) {
    while let Ok(req) = rx.recv() {
        let result = compact_one(&req).unwrap_or_else(|e| {
            log_compactor_error(req.table_id, e);
            CompactionResult::empty(req.table_id, req.compact_seq_in)
        });
        let _ = tx.send(result);
    }
}
```

`compact_one` is a free function that takes the snapshot, calls
`compact::merge_and_route` with the same arguments `run_compact_all`
uses today (`shard_index.rs:441-496`), and writes the new manifest
file. It never touches a `Table` or `ShardIndex` — those live on
the query thread.

### Query-thread side: enqueue and drain

`compact_if_needed` becomes non-blocking:

```rust
pub fn compact_if_needed(&mut self) -> Result<(), StorageError> {
    if !self.shard_index.should_compact() { return Ok(()); }
    if self.compactor.in_flight.contains(&self.table_id) { return Ok(()); }
    let req = self.shard_index.snapshot_for_compaction(self.table_id, ...);
    self.compactor.tx_req.send(req).map_err(|_| StorageError::Io)?;
    self.compactor.in_flight.insert(self.table_id);
    Ok(())
}
```

At a natural boundary (top of each tick, before cursor setup; also
at idle drain), the query thread drains finished results:

```rust
pub fn poll_compactor(tables: &mut HashMap<u32, Table>, handle: &mut CompactorHandle) {
    while let Ok(res) = handle.rx_done.try_recv() {
        if let Some(tbl) = tables.get_mut(&res.table_id) {
            tbl.apply_compaction_result(res);
        }
        handle.in_flight.remove(&res.table_id);
    }
}
```

`Table::apply_compaction_result` is the only non-trivial new piece on
the query side. It atomically:

1. Opens the new L1 entries (each is a freshly-rendered file at a
   real path, written by the compactor thread).
2. Removes the L0 entries listed in `removed_l0_filenames` from
   `shard_index.l0`.
3. Installs the new entries into `levels[0]` and applies any cascaded
   `deeper_level_updates`.
4. Updates `compact_seq` from `compact_seq_out`.
5. Schedules the old shard files for deletion (`pending_deletions`),
   identical to the current synchronous path's tail at
   `run_compact_all` line 484.
6. Clears `needs_compaction`, then re-runs `update_flags()` against
   the post-apply state. If new flushes piled in while compaction
   ran, `needs_compaction` flips back on and the next tick enqueues
   the next request.

This mirrors the existing post-merge cleanup in `run_compact_all`
exactly; only the merge step itself happens elsewhere. The opens at
step 1 may fail (transient FS error) — on failure, log and *leave
the L0 entries in place*. The new L1 files sit as orphans for the
GC pass to clean up. Identical recovery semantics to the current
"all opens succeed before mutating" pattern.

### LSN guard against losing newer flushes

Between `snapshot_for_compaction` and `apply_compaction_result`, the
query thread may flush more L0 shards. The result must not remove
shards the compactor never saw.

`apply_compaction_result` filters `removed_l0_filenames` against the
current `shard_index.l0` (by filename) and removes only matches.
Any L0 entry added after the snapshot is preserved untouched — the
next compaction cycle picks it up.

This is single-writer-safe: only the query thread mutates
`shard_index.l0`, and the compactor only proposes a removal set.

### Backpressure

If compaction can't keep up, `needs_compaction` keeps firing but
`in_flight.contains(&table_id)` keeps the enqueue a no-op. L0 grows
in the meantime. The growth is bounded by how fast the query thread
flushes; in the worst case L0 reaches an arbitrary depth and read
amp climbs.

Two safety nets:

1. **Hard fallback to synchronous compaction.** When L0 size exceeds
   `L0_BLOCKING_THRESHOLD = 16` (4× the trigger), `compact_if_needed`
   ignores `in_flight` and runs `run_compact_all` synchronously on
   the query thread. The user sees a latency spike but never
   unbounded read amp.
2. **Idle drain.** When the worker is idle (no SAL messages, eventfd
   timeout), call `poll_compactor` regardless. This catches results
   that completed during a quiet period.

### Ownership: where does the compactor live?

Two options:

**Option A — Per-Table reference to a shared handle.** Each `Table`
holds a clone of `Rc<CompactorHandle>`. Cheap, no lifetime
gymnastics, but every table call has to bounce through the
worker-global handle.

**Option B — Worker-owned, threaded through tick context.** The
worker owns the handle; `Tick::run` passes `&mut CompactorHandle`
down to wherever `compact_if_needed` is called.

Option A is simpler and incurs no measurable cost (a single `Rc`
clone per table). Use Option A.

### Manifest fsync

`publish_manifest` + `fsync(dirfd)` is the durability fence today
(`table.rs:617-621`). Move it to the compactor thread: after writing
the new manifest, the compactor calls `fsync` on the dirfd it
received in the request. The fence stays before
`CompactionResult` is sent over the channel, so the query thread's
`apply_compaction_result` runs only after durability is established
in the compactor.

`dirfd` ownership: the `Table` opens the dirfd at construction
(`table.rs`, see `close()` at line 631-636). Passing the raw fd by
value into the request is safe — both threads dup it logically, but
since fds are reference-counted by the kernel and the compactor only
calls `fsync`, no `dup` is required as long as the `Table` doesn't
close the fd while a request is in flight (it doesn't: `close` is
called only when the table itself is dropped, which the worker
sequences after compactor drain).

---

## File Changes

### 1. `crates/gnitz-engine/src/storage/compactor.rs` (new, ~250 lines)

- `CompactionRequest`, `CompactionResult`, `LevelSnapshot`,
  `LevelUpdate` structs.
- `CompactorHandle` with `spawn() -> Self`, `enqueue()`, `try_drain()`.
- `compact_one(&CompactionRequest) -> Result<CompactionResult, _>`
  — pure function, calls `compact::merge_and_route` and writes a
  manifest. Identical merge logic to `run_compact_all` today; the
  difference is it operates on the request snapshot, not on a
  `&mut Table`.

### 2. `crates/gnitz-engine/src/storage/shard_index.rs`

- Add `snapshot_for_compaction(table_id, schema, output_dir,
  manifest_path, dirfd) -> CompactionRequest`. Reads the current L0
  + levels and packages them. No mutation.
- Add `apply_compaction_result(&mut self, result: &CompactionResult)
  -> Result<(), StorageError>`. Performs the L0 prune, L1 insert,
  cascaded updates, and `pending_deletions` append. Self-contained;
  does not call `run_compact_*`.
- Keep `run_compact_all` as-is for the synchronous fallback path.
- `update_flags` and `should_compact` unchanged.

### 3. `crates/gnitz-engine/src/storage/table.rs`

- Add `compactor: Rc<CompactorHandle>` field to `Table`.
- Constructor takes a `Rc<CompactorHandle>` argument and stores it.
- Replace `compact_if_needed` body with the async-enqueue version
  above.
- Add `apply_pending_compaction(&mut self) -> Result<(), StorageError>`
  which drains results destined for this table (the worker's
  `poll_compactor` is the bulk drain; this is for unit tests that
  drive one table directly).
- Add the L0_BLOCKING_THRESHOLD fallback at the top of
  `compact_if_needed`.

### 4. `crates/gnitz-engine/src/storage/partitioned_table.rs`

- Constructor takes the same `Rc<CompactorHandle>` and forwards it
  to each per-partition `Table`.
- No other changes — partitioned cursor setup delegates to per-
  partition `Table::compact_if_needed`, which is already async after
  step 3.

### 5. `crates/gnitz-engine/src/runtime/worker.rs`

- Worker startup: `let compactor = Rc::new(CompactorHandle::spawn());`
- Thread through to every `Table::open` call site.
- After SAL dispatch, before parking, call `compactor.poll_results()`
  to drain finished compactions into the live tables. This is the
  "idle drain" guarantee. (`poll_results` is a tiny helper that
  loops `try_recv` and dispatches results to the `tables` map.)
- On worker shutdown: drop `compactor` (sends close to the channel,
  the thread exits cleanly).

### 6. `crates/gnitz-engine/src/vm.rs`, `dag.rs`

No code change. `compact_if_needed` keeps the same signature; the
sites at `vm.rs:564` and `dag.rs:1197` continue to call it.

---

## Edge cases

**Worker shutdown with compaction in flight.** Worker drains the
result channel one last time before dropping tables. Any unfinished
work is abandoned — the compactor thread is told to stop, half-
written `.tmp` files are GC'd by the existing startup-time orphan
scan in `bootstrap.rs`.

**Table dropped while a request is in flight.** Don't allow it: the
worker's table map is the single source of truth, and we only drop
on shutdown after the final drain. Add a debug assert in
`Table::close`: "no in-flight compactions for this table."

**Compactor thread panics.** Use a `catch_unwind` boundary in
`compactor_loop` per request. A panic logs and resets `in_flight`
state (via an `Error` result) so the query thread can fall back to
synchronous mode for that table.

**Cursor holds a reference to a shard the compactor wants to delete.**
Already handled: `Rc<MappedShard>` keeps the mmap alive until the
last cursor drops. The compactor schedules file deletion via
`pending_deletions`, which `try_cleanup` flushes only when no
references remain (this is the current behavior of
`shard_index.rs:try_cleanup`, unchanged).

**Concurrent flush during compaction.** The query thread keeps
flushing into `shard_index.l0` while the compactor merges its
snapshot. `apply_compaction_result`'s filename-based filter (see
"LSN guard" above) preserves the new flushes. Read cursors created
between snapshot and apply see the *current* L0 (snapshot + new
flushes) — strictly correct.

**Manifest write reordering with respect to cursor reads.** No
cross-thread shard-list visibility issue: the query thread is the
sole reader of `shard_index`, and `apply_compaction_result` is the
only place the list changes after the compactor finishes. Cursors
created before `apply_compaction_result` capture the old list (via
`all_shard_arcs`), and `Rc` keeps those shards alive until the
cursor drops.

**Failure to publish manifest.** If `compact_one` succeeds at merging
but fails to write the manifest, the new L1 files exist on disk but
are not committed. The compactor sends a `CompactionResult` with
`manifest_published = false` and an empty `new_l1_entries`; the
query thread treats it as a no-op (`needs_compaction` remains true,
next cycle retries). Orphan `.tmp` and committed-but-unreferenced
files are cleaned by bootstrap GC.

---

## Tests

All new tests go in `crates/gnitz-engine/src/storage/compactor.rs`
as `#[cfg(test)] mod tests`, plus integration tests in
`crates/gnitz-engine/src/storage/table.rs`'s existing tests module.

**`test_compactor_basic`**

Spawn a `CompactorHandle`. Build a `CompactionRequest` from a real
table with 5 L0 shards. Send. Receive a `CompactionResult`. Apply
to a table built from the same starting state. Assert L0 is empty,
L1 has expected entries, file contents match what synchronous
`run_compact_all` would produce against the same input.

**`test_compact_if_needed_async`**

Build a `Table` wired to a real `CompactorHandle`. Flush 5 L0
shards. Call `compact_if_needed`. Assert: the call returns
*immediately* (no blocking — measure wall clock under a tight
upper bound, e.g. 1 ms). Assert: `shard_index.l0.len() == 5` still
(compaction not yet applied). Drain the channel with
`apply_pending_compaction`. Assert: L0 cleared, L1 populated.

**`test_in_flight_dedup`**

Call `compact_if_needed` twice in a row with no drain in between.
Assert: only one request was sent (the second call observes
`in_flight` and is a no-op).

**`test_concurrent_flush_during_compaction`**

Snapshot a `CompactionRequest` from a table with 5 L0 shards. Add
2 more L0 shards (simulating a flush during compaction). Apply the
result. Assert: original 5 are gone, 2 new ones remain in L0,
`needs_compaction` is reset based on the post-apply state.

**`test_blocking_fallback`**

Saturate the compactor (e.g. fill the channel or stall the thread
via a test-only blocking primitive). Keep flushing L0 shards until
the count exceeds `L0_BLOCKING_THRESHOLD`. Call
`compact_if_needed`. Assert: it ran synchronously (L0 cleared on
return).

**`test_panic_recovery`**

Inject a compactor failure for one specific table (e.g. an
unwritable output dir). Send a request. Receive a result with an
error indicator. Assert: `in_flight` cleared for that table. Next
call to `compact_if_needed` re-enqueues (and the test then fixes
the dir to verify recovery).

**Worker-level smoke (`tests/test_workers.py`).**

Add a multi-worker E2E test: push 10 000 rows into a table in
bursts, run a heavy view evaluation that depends on a trace cursor
on that table, assert query latency is bounded (e.g. 95th
percentile within X of the no-compaction baseline). Loose
assertion — primarily a regression guard against blocking
reintroduction.

---

## Implementation order

```
[1] compactor.rs scaffold: CompactorHandle, channels, compactor_loop
    with an inert (no-op merge) compact_one. Unit-test
    send/recv/shutdown plumbing.
[2] compact_one implemented in terms of compact::merge_and_route.
    Mirror run_compact_all exactly but operate on the request
    snapshot. Test against the synchronous path's output on a
    deterministic input.
[3] shard_index: snapshot_for_compaction + apply_compaction_result.
    Test round-trip: synchronous run_compact_all output ==
    apply_compaction_result(compact_one(snapshot)).
[4] table.rs: async compact_if_needed + L0_BLOCKING_THRESHOLD
    fallback. Run full storage test suite.
[5] worker.rs: spawn handle at startup, idle-drain at SAL park.
    Run make e2e.
[6] partitioned_table.rs: thread the handle through. Re-run e2e.
[7] Benchmark via make bench-sweep against pre-change baseline.
    Look for tick-latency tail improvement on write-heavy workloads.
```

---

## What this enables

- **Tick latency decoupled from compaction cost.** Query path is
  enqueue + drain; the merge runs on the compactor thread.
- **Compaction can run during otherwise-idle periods.** The worker's
  query thread no longer needs to wait for a tick that triggers
  `compact_if_needed`; the compactor processes the queue as fast as
  it can.
- **Foundation for further parallelism.** With compaction off the
  query thread, future work (e.g. moving compaction to a separate
  process, splitting per-level cascade across multiple threads)
  changes only the compactor side and leaves the query path stable.

The change preserves the existing crash-recovery semantics:
manifests are written atomically, shard files use `.tmp` + rename,
and the recovery path in `bootstrap.rs` is untouched.
