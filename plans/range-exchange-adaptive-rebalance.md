# Adaptive rebalancing for the range-partitioned exchange

The range-partitioned exchange routes a pure-range INNER join's delta to the
contiguous worker interval its predicate can match, and range-partitions each side's
trace by a shared set of `W-1` OPK split keys (`bounds`). With bounds **sealed once**
at startup, partition balance decays as the live distribution drifts: an append-mostly
range key (a timestamp / sequence) whose hot working set migrates past the top split
piles every new row onto the top interval's worker, serializing the active set on one
worker while the others hold cold history. That is strictly worse than the hash-spread
balance the broadcast path had, so a range exchange without live rebalancing regresses
the most common range-join workload once it drifts.

This plan makes bounds **track the running distribution**: the master recomputes
quantile bounds from a live key sample, and when the physical-trace imbalance crosses a
threshold it runs a one-shot **repartition sub-epoch** that physically re-partitions
both traces to the new bounds before they take effect. The hard parts are (a) doing the
cross-worker trace shuffle without deadlocking the W2M↔SAL rings, and (b) re-deriving
the partitioned traces at all, since the engine has no live way to rebuild a view's
ephemeral state. Both are solved below.

## Assumed starting point

This builds directly on the range exchange and reuses its primitives verbatim:

- Each side's trace is range-partitioned by `bounds: Vec<PkBuf>` (`W-1` OPK split keys
  of width `stride_T`, non-decreasing under `compare_pk_bytes`). `owner_of(opk, bounds)`
  binary-searches it; `compute_bounds(sample_sorted, nw, stride_t)` produces quantile
  splits (empty ⇒ even-width type-domain). These live in `ops/exchange/range_route.rs`.
- Workers hold the live bounds in `Program::range_bounds` and filter the integrate path
  with `op_range_partition_filter`. The master holds a per-view copy for the range
  scatter (`fill_range_indices`).
- `FLAG_SET_RANGE_BOUNDS(view_id, bounds)` is a broadcast control message that
  overwrites a worker's `Program::range_bounds` (no fdatasync — re-derivable).
- `view_range_join_shape(view_id)` is `Some` iff the view is an INNER pure-range join
  (carries a `RangePartitionFilter` node).

Bounds are still **not durable**: on restart they re-seal from a fresh base-data sample
and the ephemeral traces rebuild under them. This plan only adds *live* adaptation
between restarts.

---

## 1. Why a trace shuffle, and why not re-backfill

A view's output sink **and** every `IntegrateTrace` scratch table are
`Persistence::Ephemeral` (`query/dag/mod.rs:95`, `query/compiler/emit.rs:87-95`),
rebuilt only by replaying base data through the circuit. That replay (`backfill_view` /
`fan_out_backfill`) assumes the target starts empty — it relies on the open-time
`erase_stale_shards` in `Table::new` (`storage/lsm/table/mod.rs:228`) and performs **no
clear** itself (`catalog/ddl.rs:382-391`). There is **no live primitive** to clear a
view's stored rows (`clear_view_regfile_deltas` only clears VM delta *registers*;
`unregister_table` is the drop path).

So "rebuild the traces by re-running backfill" is not viable live:

- The sink ingest is **additive** (`ingest_to_family`), so a second backfill **doubles**
  the materialized output.
- Worse, a non-linear operator whose trace was wiped recomputes from a **zero
  integral** and re-emits the entire current state as `+` deltas with no `−`
  retractions to cancel — over-counting, not merely doubling.
- Avoiding that would require a new live primitive to clear the sink **and** every
  trace back to empty, plus output double-buffering so downstream queries don't observe
  a transiently-empty view mid-rebuild.

The repartition does not need the output rebuilt at all — re-partitioning is purely
internal; the join *result* is identical regardless of where each row was computed. So
the committed mechanism is a **direct trace shuffle**: stream each side's existing trace
rows to their new owners under the new bounds, leaving the sink untouched. This moves
`O(|trace_a| + |trace_b|)` bytes once per threshold-breaching checkpoint — cheap
relative to the per-epoch routing it restores.

---

## 2. The deadlock constraint (load-bearing)

The shuffle streams bulk data **worker → master → workers**: workers push trace rows up
the W2M rings, the master re-scatters them down the SAL. Both are finite and both block:

- The W2M ring is 1 GiB per worker, `MAX_W2M_MSG = 256 MiB` (`runtime/protocol/w2m_ring.rs`).
  `W2mWriter::send_encoded` **hard-blocks** (indefinite `FUTEX_WAIT` on `writer_seq`)
  when the ring is full (`runtime/protocol/w2m.rs:40-68`); the worker's single thread
  does nothing else — it cannot read the SAL — until the master drains a slot.
- The SAL is 1 GiB, checkpoint at 75%, and a relay needs ≥1/8 (128 MiB) free
  (`runtime/protocol/sal.rs`, `master/dispatch.rs:500-502`). Reclaiming SAL space runs
  a checkpoint, which needs **every worker to ACK a FLUSH**.

The exact deadlock is already documented in the engine as the reason chunked streaming
is **forbidden from inside an exchange wait** (`worker/mod.rs:234-243`, the
`pending_streams` doc): a worker that blocks in `send_encoded` on a full W2M ring stops
reading the SAL, so the relay it (or the checkpoint) needs sits unread → permanent hang.
Instantiated for the shuffle: workers blast trace chunks up → W2M fills → workers park
in `send_encoded` → stop reading SAL → master's re-scattered chunks (and any checkpoint
FLUSH) sit unread → master can't reclaim SAL → cluster wedges.

The engine's one deadlock-safe bulk-stream — the scan fan-out — avoids this with a
strict discipline (`worker/mod.rs:405-461`): **emit exactly one chunk per `drain_sal`
pass, then keep draining the SAL**, so `send_encoded` only ever backpressures briefly
and the worker always returns to consume the SAL. The master drains every worker's W2M
non-blockingly at the top of each reactor tick (`reactor/runloop.rs:58-83`), freeing
ring slots continuously. The scan path routes worker→master→**client TCP**, never back
to SAL, so SAL-space pressure never arises there. The shuffle's re-scatter **back to
SAL**, coupled with checkpoints needing worker cooperation, is the genuinely novel
pressure no existing path exercises — so the discipline below is mandatory and §8 gates
it with a dedicated stress test.

---

## 3. Committed design

Seven pieces. 1–3 gather the signal; 4–7 are the repartition sub-epoch.

### 3.1 Stats piggybacked on the exchange ACK

In `worker/exchange.rs`, after building the FLAG_EXCHANGE reply, append a small trailing
block: (a) a fixed-size (e.g. 64) evenly-strided OPK sample of the worker's *post-filter*
integrate batch (the rows it owns this epoch), and (b) the worker's current **physical**
trace row counts (`trace_a.count()`, `trace_b.count()`). This rides the existing W2M
exchange message — no extra round trip, no hot-path cost beyond the trailing bytes.

### 3.2 Master reservoir + imbalance gauge

`ExchangeAccumulator` (`runtime/reactor/exchange.rs`) folds the per-worker samples into a
per-view fixed-capacity OPK reservoir and records the latest per-worker physical trace
counts. The imbalance metric is `max_owned / mean_owned` over the **physical trace
counts**, not delta-stream volume: Z-set consolidation makes a hot key contribute many
deltas but ≤1 trace row, so delta volume would false-trigger. Pure in-memory routing
state, rebuilt from scratch on restart.

### 3.3 Reseal trigger at the checkpoint cadence

Tie the reseal to the master's existing checkpoint decision (`dispatch.rs` checkpoint
path). On checkpoint: `compute_bounds` from the reservoir; run a repartition sub-epoch
**iff** the new bounds differ from the live bounds **and** the physical-trace imbalance
exceeds `τ = 2.0`; else keep the live bounds. Checkpoint cadence makes reseal rare and
amortized; steady state between checkpoints carries zero extra coordination. Scheduling
the sub-epoch right after a checkpoint also means the SAL is freshly drained — maximal
headroom for the re-scatter (§3.7).

### 3.4 Repartition sub-epoch — overview

A master-initiated epoch for one view, dispatched between normal epochs. The
per-`(view, source)` epoch serialization gives a clean quiescence point (§4): no user
delta is in flight, the executor is single-threaded per worker, and trace cursors are
rebuilt each epoch, so no probe straddles the boundary swap. It re-derives both traces
under the new bounds with a **double-buffered shuffle** that cannot read its own writes:

1. Master broadcasts `FLAG_SET_RANGE_BOUNDS(view_id, new_bounds)`; workers **stage**
   `new_bounds` (do not yet apply — `Program::range_bounds` still holds the old bounds).
2. Master broadcasts `FLAG_RANGE_REPARTITION(view_id)`. Each worker, for **each** of its
   two range trace tables (resolved by graph traversal, §3.6):
   a. Allocates a **fresh empty sibling** trace `Table` (`Table::new`, ephemeral).
   b. Opens a consolidated read cursor over the **old** trace.
   c. Runs the deadlock-safe streaming loop (§3.7): per `drain_sal` pass, drain one
      chunk from the old-trace cursor and emit it up the W2M reply, **keyed by each
      row's raw OPK PK bytes** for a point re-route; between chunks, keep reading the
      SAL and ingest any inbound re-scattered chunks (§3.6) into the **fresh** trace.
      The old trace is read-only and the fresh trace is write-only, so there is no
      read-write hazard.
3. The master re-scatters each emitted trace chunk by `new_bounds` with a **PK-region
   point-route** (§3.5): read each row's OPK from the chunk's PK region, route it to
   `[owner_of(opk, new_bounds)]` (a single destination — a trace row has one key), and
   write it to that worker's SAL slot under `FLAG_RANGE_TRACE_CHUNK`.
4. When every worker has drained its old traces and the master has flushed all
   re-scattered chunks (lockstep termination, §3.7), each worker **swaps** its VM owned
   trace handle old→new (dropping the old table) and **applies** the staged bounds
   (`range_bounds ← new_bounds`). The master swaps its own bounds copy and resumes
   normal epochs.

The fresh trace is partitioned by `new_bounds` (every row was routed to its
`new_bounds` owner), and routing now uses `new_bounds`, so the next probe is consistent.

### 3.5 Schema-from-frame re-scatter (resolves gap: master lacks the trace schema)

The master has no ephemeral-trace schema in its catalog, so it cannot look one up. It
does not need to: the worker emits each trace chunk as a **self-describing wire batch**
(the schema block travels in the frame, exactly as the FLAG_EXCHANGE payload already
carries `schema.as_ref()`, `worker/exchange.rs:44-63`). The master re-scatters using the
schema **carried in the received chunk**, and routing reads only the PK region:

```rust
// PK-region point-route: one destination per trace row (its new owner).
// `chunk_schema` is the schema decoded from the received frame; `stride_T` is the
// range slot's promoted-type width, resolved once at view setup. No catalog lookup
// of the ephemeral trace schema.
let mb = chunk.as_mem_batch();
let mut worker_indices = vec![Vec::new(); nw];
for i in 0..chunk.count {
    let owner = owner_of(mb.get_pk_bytes(i), new_bounds);   // PK region = OPK _join_pk
    worker_indices[owner].push(i as u32);
}
sal.scatter_wire_group(&chunk, &worker_indices, &chunk_schema, /* … */
                       FLAG_RANGE_TRACE_CHUNK, /* wire_flags carrying view_id+side */ …);
```

This is **not** the §3.5-core payload-column range scatter (which extracts the key from a
base-table payload column): a trace row's key is already the packed `_join_pk` in its PK
region, so feeding trace rows to the payload-column scatter would read garbage. It is a
dedicated point-route that trusts the PK region. `scatter_wire_group` copies whole rows
by index into the destination slots, treating payload columns as opaque.

### 3.6 Worker trace-chunk ingestion (resolves gap: no path routes chunks to the trace)

A new SAL data flag `FLAG_RANGE_TRACE_CHUNK` carries `(view_id, side)` (left/right) plus
the batch. The worker's `dispatch` (`worker/mod.rs`) gets a new arm that does **not**
feed the DAG — it ingests straight into the resolved fresh trace table:

- **Trace-table resolution by graph traversal.** Given `view_id` and `side`, the worker
  loads the compiled circuit (`load_meta_circuit`, cached), finds the `DeltaTraceRange`
  node for that side, follows its `PORT_TRACE` input to the feeding `IntegrateTrace`
  node, and maps that node's output register to its owned `table_idx` via
  `VmHandle::owned_trace_regs`. This keeps the master free of worker-local table
  numbering (assigned during the worker's own emit). During a repartition the resolver
  returns the **fresh** sibling table allocated in §3.4(2a), held in a per-view
  `repartition: Option<RepartitionState>` on the worker.
- **Direct ingest.** `table.upsert(batch)` into the fresh trace — the normal ephemeral
  ingest path, bypassing the VM entirely. No epoch, no probe.

`FLAG_RANGE_TRACE_CHUNK` is processed at the worker's top-level `drain_sal` boundary
between epochs, so it never collides with an in-flight probe (§4). It is re-derivable
control traffic — no fdatasync.

### 3.7 Deadlock-safe streaming discipline + lockstep termination (resolves gap: W2M↔SAL deadlock)

The shuffle obeys the scan-path contract verbatim, plus master-coordinated lockstep
(the backfill template) so SAL pressure stays bounded:

- **One chunk per `drain_sal` pass.** A worker emits at most one trace chunk up the W2M
  per pass through its run loop, then continues `next_sal_message` to **drain the SAL**
  (ingesting inbound `FLAG_RANGE_TRACE_CHUNK`s into its fresh trace). `send_encoded`'s
  backpressure is therefore always transient — the worker never parks on a full W2M
  while holding unread SAL, so the master can always advance.
- **Master-coordinated rounds.** Like the backfill pad/decision loop
  (`worker/mod.rs:1203-1221`, `dispatch.rs:363-440`): each round, every worker emits one
  chunk or a **pad** (old-trace cursor exhausted); the master ANDs the pad bits, and
  once all workers pad **and** the re-scatter queue is flushed, it stamps a STOP
  decision. All workers run the same number of rounds (pad keeps them in lockstep),
  ending together on the STOP round, at which point they swap (§3.4(4)).
- **Bounded in-flight SAL.** The master re-scatters at most one round's worth of trace
  chunks before the next round, and workers ingest each round's chunks before emitting
  the next, so the volume resident in the SAL at any instant is `O(nw × chunk)` — far
  under the 128 MiB relay margin. Running the sub-epoch immediately after a checkpoint
  starts it with a drained SAL.
- **Liveness.** A lost trace chunk or relay wedges the lockstep, so the paths
  `gnitz_fatal_abort!` rather than warn-and-drop, matching the existing relay contract
  (`executor.rs:537-545`) — a loud, recoverable crash (workers self-exit via
  `getppid()`, operator restarts; bounds re-seal cold) beats a silent wedge.

---

## 4. Quiescence and the control sub-epoch

The sub-epoch is a master-driven evaluation with **no user delta**, for which the engine
has direct precedent: `FLAG_TICK` already drives a circuit evaluation with an empty batch
(`dispatch.rs:1490`, `worker/mod.rs:1155`), and `FLAG_BACKFILL` is the master-coordinated
multi-round lockstep template. A worker processes broadcast control messages at its
top-level `drain_sal` boundary, where any prior epoch has fully returned and its cursors
dropped — a clean quiescence point with no in-flight probe.

The one hazard is a control message observed **inside** an exchange wait of an unrelated
in-flight tick (`DispatchContext::InsideExchangeWait`). `FLAG_SET_RANGE_BOUNDS` and
`FLAG_RANGE_REPARTITION` are therefore **deferred-and-replayed** at top level, exactly
like `FLAG_TICK` / `FLAG_DDL_SYNC` already are (`worker/mod.rs:676-713`), so the
repartition only ever begins at quiescence. The master serializes tick batches
one-at-a-time (`run_tick().await`), so it can interleave the sub-epoch between normal
ticks without racing them.

`FLAG_RANGE_REPARTITION` must key its rounds by a `(view_id, source_id)` distinct from any
concurrent steady-state round of the same view, or relays mis-deliver (a fixed
root-cause bug — `worker/mod.rs:654-659`); use a reserved synthetic `source_id` for the
sub-epoch.

---

## 5. Reseal flow, end to end

1. **Steady state**: workers piggyback samples + trace counts on each exchange ACK
   (§3.1); the master folds them into the per-view reservoir + imbalance gauge (§3.2).
2. **Checkpoint**: master computes candidate bounds; if they differ from live **and**
   imbalance > τ, it begins the sub-epoch (§3.3); else does nothing.
3. **Stage**: `FLAG_SET_RANGE_BOUNDS(view_id, new_bounds)` → workers stage (§3.4.1).
4. **Shuffle**: `FLAG_RANGE_REPARTITION(view_id)` → workers double-buffer + stream
   (§3.4.2, §3.7); master point-routes chunks by `new_bounds` (§3.5); destination
   workers ingest into fresh traces (§3.6). Lockstep rounds until all-pad + flushed.
5. **Swap**: workers swap owned trace handles, drop old, apply staged bounds; master
   swaps its copy; normal epochs resume (§3.4.4).

After the swap, both traces are physically partitioned under `new_bounds` and routing
uses `new_bounds`, so the next probe's interval routing finds every match where it lives.

---

## 6. Crash recovery

Both traces and the staged bounds are ephemeral / re-derivable, so a crash mid-sub-epoch
is safe: recovery replays base data to the last checkpoint with freshly-recomputed bounds
(cold-start sample), and the staged bounds and any half-built fresh trace are discarded
at open (`erase_stale_shards`). No half-applied state survives. Because the sub-epoch is
scheduled right after a checkpoint, the replay distance on such a crash is minimal.

---

## 7. Edge cases

- **No drift** — imbalance stays ≤ τ, no sub-epoch ever runs; pure steady-state cost is
  the per-ACK sample bytes (§3.1).
- **Equal adjacent new bounds (heavy ties)** — `owner_of` leaves in-between workers
  empty; the point-route sends their (nonexistent) rows nowhere. Correct, skewed.
- **`W = 1`** — imbalance is trivially 1.0; the sub-epoch never triggers; `owner_of`
  is always 0.
- **Reseal during a backfill** — the reseal trigger is gated to steady-state checkpoints
  (not boot/CREATE backfill, which seals cold from a base sample), so a sub-epoch never
  overlaps a backfill.
- **Empty trace side** — a side with zero rows pads immediately; the other side shuffles
  normally; bounds still swap.
- **Repeated reseals** — each is independent and idempotent w.r.t. correctness; `τ` and
  the differ-from-live guard prevent thrashing on marginal drift.

---

## 8. Tests

### 8.1 Unit
- Imbalance metric: `max/mean` over synthetic trace counts; τ boundary triggers exactly
  at the threshold; delta-volume skew with balanced trace counts does **not** trigger
  (the consolidation false-trigger guard).
- PK-region point-route: a chunk of known OPK keys routes each row to
  `owner_of(opk, new_bounds)` and nowhere else; opaque payload columns are copied
  verbatim.
- Trace-table resolver: a compiled range-join circuit resolves left/right
  `DeltaTraceRange → IntegrateTrace → owned table_idx` to the correct two tables.

### 8.2 Engine — multi-worker repartition (`crates/gnitz-engine` integration test)
- Build a pure-range INNER circuit at `W = 4`; drive enough skewed data to trip `τ`;
  assert the sub-epoch runs, bounds change, both traces are re-partitioned with **no row
  lost or duplicated** (sum of per-worker trace counts is invariant across the swap), and
  post-reseal output equals the broadcast reference. Run several times to rule out a
  shuffle/chunking/lockstep race.
- Double-buffer correctness: assert the old trace is fully drained and dropped, the fresh
  trace holds exactly the re-routed rows, and a concurrent (deferred) tick during the
  sub-epoch replays correctly afterward.

### 8.3 Stress — the novel back-to-SAL coupling (the load-bearing test)
- Load a large trace (≥ several hundred MB across workers) and trigger a reseal; assert
  the shuffle completes without wedging, with W2M backpressure and SAL re-scatter both
  under load, and that an interleaved checkpoint mid-shuffle still drains. This is the
  one path no existing test exercises (§2); fail-fast on a timeout.

### 8.4 E2E (`crates/gnitz-py/tests/`, `GNITZ_WORKERS=4`)
- Append-mostly time-series workload (`ON a.ts < b.ts`, monotonically increasing inserts)
  long enough to cross several checkpoints; assert correctness throughout and that
  per-worker owned-row counts re-balance after each reseal (the regression this plan
  fixes). Include deletes/updates interleaved.

### 8.5 Gate
`make verify` and `make e2e K='range' WORKERS=4`; run §8.2/§8.3 several times.

---

## 9. File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-wire/src/circuit.rs` | Add `FLAG_RANGE_REPARTITION` (control) and `FLAG_RANGE_TRACE_CHUNK` (data) flags |
| `crates/gnitz-engine/src/runtime/protocol/sal.rs` | `SalMessageKind` variants + `classify`/`is_broadcast` arms for the two flags; (de)serialize the ACK stats trailer (sample + trace counts) |
| `crates/gnitz-engine/src/runtime/orchestration/worker/exchange.rs` | Append OPK sample + physical trace counts to the FLAG_EXCHANGE reply |
| `crates/gnitz-engine/src/runtime/reactor/exchange.rs` | Per-view OPK reservoir + per-worker physical-trace gauge in `ExchangeAccumulator` |
| `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs` | Reseal decision at checkpoint (τ + differ-from-live); drive the repartition sub-epoch; PK-region point-route re-scatter via `scatter_wire_group` using the chunk's own schema |
| `crates/gnitz-engine/src/runtime/orchestration/worker/mod.rs` | `RepartitionState` per view; `FLAG_RANGE_REPARTITION` handler (allocate fresh trace, stream loop, swap); `FLAG_RANGE_TRACE_CHUNK` ingest arm; defer-and-replay both control flags inside exchange waits; one-chunk-per-`drain_sal` discipline |
| `crates/gnitz-engine/src/query/vm/{mod}.rs` | Replace-owned-trace-table operation (swap old→new between epochs); stage-vs-apply for `Program::range_bounds` |
| `crates/gnitz-engine/src/query/dag` / `compiler/load.rs` | Worker-side range-trace resolver (`DeltaTraceRange` PORT_TRACE → `IntegrateTrace` → owned `table_idx`) |
| `crates/gnitz-engine/src/storage/lsm/table` | Confirm/construct the fresh sibling ephemeral `Table` reuse (no new clear primitive needed — double-buffer via `Table::new` + swap) |
| `crates/gnitz-engine` tests + `crates/gnitz-py/tests/` | §8 |

No change to the core range-exchange ops, the range probe, or the output exchange — this
plan only adds the live bounds-tracking layer on top.
