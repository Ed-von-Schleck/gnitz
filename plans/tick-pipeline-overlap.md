# Tick Pipeline Overlap: Eliminating Dead Time in the Multi-Worker Loop

---

## Unifying Principle

The multi-worker event loop alternates between three resource consumers: master CPU
(encoding, SAL writes), storage I/O (fdatasync), and worker computation (DAG evaluation,
exchange). Today these are largely serialized: the master writes, then waits for storage,
then waits for workers, then writes again. Each resource idles while another is busy.

Every improvement below derives from the same idea: **overlap resource utilization across
pipeline stages.** Where the current code has sequential phases (write, sync, compute,
collect), restructure so that one stage's work runs concurrently with the next stage's.
The constraint is DBSP correctness: cross-worker exchanges require lockstep evaluation at
the same logical timestamp. Within that constraint, there is significant slack to exploit.

---

## Current Pipeline (sustained load)

```
Master:   [write SAL][signal+fsync][collect push ACKs][wait fsync]─[write TICK][signal]─
Workers:      idle   [filter+ingest+ACK]     idle         idle         idle
Storage:      idle        [fdatasync ────────────────────────────]      idle

Master:   ─[poll_tick_progress ──────────────────]─[write SAL][signal+fsync][collect]...
Workers:   [evaluate_dag + exchange ─────────────]     idle   [filter+ingest+ACK]...
Storage:                  idle                         idle        [fdatasync ──]...
```

Workers idle during push SAL writes + fdatasync. Master and storage idle during tick.
The inter-tick gap (between tick completion and next tick start) is dominated by fdatasync
latency (~1-10 ms), during which all workers sit idle.

---

## Improvement 1: Pre-write pushes during tick

**Gap eliminated:** The inter-tick dead time where workers idle while the master encodes
the next push batch and waits for fdatasync.

**Mechanism:** Split `flush_pending_pushes` into a write-phase (SAL encode + async fdatasync
submit, runs during `TickState::Active`) and a signal-phase (signal workers + collect push
ACKs, runs after tick completes). The fdatasync runs concurrently with worker DAG
evaluation. When the tick completes, fdatasync is typically already done.

**Worker-side requirement:** `do_exchange_impl` must stash `FLAG_PUSH` messages encountered
during exchange (following the existing `FLAG_DDL_SYNC` deferral pattern) and replay them
after the tick ACK. Without this, push groups written to the SAL during the tick would be
silently dropped by the exchange wait loop.

**Pipeline after:**

```
Master:   [write TICK][signal]─[poll_tick(pre-write SAL + fsync)]─[signal][collect ACKs]─
Workers:       idle           [evaluate_dag + exchange ──────────][ingest+ACK]
Storage:       idle                [fdatasync(next push) ────────]   idle
```

Inter-tick gap: ~0.2 ms (signal + ACK collection) instead of ~5-10 ms.

**Complexity:** ~80 LOC across executor.rs, master.rs, worker.rs. Follows existing patterns
(async fsync, deferred DDL). No wire protocol changes. No new abstractions.

---

## Improvement 2: Multi-table tick batching

**Gap eliminated:** Sequential per-table tick barriers. If 3 tables received pushes, the
current code fires 3 separate `fan_out_tick` calls, each with its own signal + exchange
relay + ACK collection barrier. Three round-trips when one could suffice.

**Mechanism:** Write multiple `FLAG_TICK` groups to the SAL in one pass, signal once. Workers
`drain_sal` processes all ticks sequentially in a single pass and sends a single composite
ACK. The master collects one ACK round instead of N.

**Complication:** Exchange relay interleaving. If table A's DAG needs exchange while table
B's doesn't, the worker blocks in `do_exchange_impl` between tables. The master must handle
exchange relays from mixed-table ticks, routing by view_id. The accumulator (`acc`) already
tracks view_id, so demuxing is feasible but adds branching to the relay path.

**Expected impact:** Proportional to the number of tables with pending pushes per tick cycle.
Workloads that push to many tables concurrently (e.g., batch ETL) benefit most.

**Complexity:** Medium. Requires changes to tick firing (executor.rs), tick collection
(master.rs), and worker tick processing (drain multiple ticks before ACK).

---

## Improvement 3: Configurable tick coalescing thresholds

**Gap eliminated:** One-size-fits-all tick timing. `TICK_DEADLINE_MS=20` and
`TICK_COALESCE_ROWS=10K` are hardcoded. A bulk-loading workload wants larger batches
(fewer ticks, less exchange overhead). An OLTP workload wants faster ticks (lower view
staleness).

**Mechanism:** Make thresholds configurable per-table via `CREATE TABLE ... WITH (tick_ms=N,
tick_rows=N)` or a server-level default. Store in catalog metadata.

**Complexity:** Low. The coalescing logic in `should_fire_ticks` already checks per-tid row
counts. Adding a per-tid threshold lookup is straightforward. Schema changes are minor
(one metadata column in the table catalog entry).

---

## What Was Considered and Left Out

**Pre-exchange consolidation.** Consolidating the pre-plan output before sending through
exchange would reduce IPC volume when the pre-plan produces duplicate keys. However, the
pre-plan for most exchange views is `map_reindex` (repartition by group key), which
produces few duplicates in practice. The sort+merge cost would exceed the IPC savings for
typical workloads. Could revisit if profiling shows exchange IPC as a bottleneck.

**Worker `pending_deltas` consolidation.** Consolidating accumulated push deltas before
DAG evaluation would shrink the delta size. However, `pending_deltas` is already consumed
in one shot per tick, and the DAG operators that need consolidation (distinct, reduce)
already consolidate their inputs. The extra sort+merge on the full delta would be redundant
work for most DAG shapes.

**Per-view independent ticks for exchange-free views.** Workers could fire ticks
independently for views without exchange, eliminating the barrier for those views. However,
this breaks the current model where a tick is a table-level event that triggers the full
downstream DAG. Splitting per-view would require per-view delta tracking, per-view tick
scheduling, and careful ordering to prevent a downstream exchange-view from seeing partial
upstream state. High complexity for marginal benefit (exchange-free views are already fast).
