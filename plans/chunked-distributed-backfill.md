# Chunked distributed view backfill

## Goal

Bound the peak memory of distributed (multi-worker) CREATE VIEW backfill from
`O(N)` per worker — and `O(num_workers × N)` at the master for broadcast views
— to `O(chunk)`, by streaming each source partition through the incremental
plan in fixed-size chunks instead of one whole-partition batch. `N` is a
worker's committed partition row count.

## Current state (verified against code)

`handle_backfill` (`runtime/worker.rs:1419`) materializes the worker's entire
source partition into one batch and pushes it through a single `evaluate_dag`:

```rust
fn handle_backfill(&mut self, source_tid: i64, request_id: u64) -> Result<(), String> {
    if !self.cat().has_id(source_tid) { return Ok(()); }
    let local_batch = self.cat().scan_family(source_tid)?;   // whole partition → one Batch
    let owned = Rc::try_unwrap(local_batch).unwrap_or_else(|a| (*a).clone());
    self.evaluate_dag(source_tid, owned, request_id);         // one evaluate_dag = one exchange round
    Ok(())
}
```

`scan_family` → `scan_store` → `open_cursor().cursor.materialize()`
(`catalog/store.rs:785`, `:1648`) has no row cap. For a view whose
`evaluate_dag_multi_worker` performs an exchange, the master then collects one
payload per worker; for the range-join broadcast relay it concatenates all `W`
payloads and clones per worker (`range-join.md` §8), so a whole-partition
backfill batch makes the master relay group `O(num_workers × N)` and can trip
the *"SAL space exhausted during backfill exchange relay"* guard
(`runtime/master.rs:611`). This is view-type-agnostic: every distributed
backfill loads its partition whole today.

Two sibling backfill paths already stream chunk-wise and bound peak memory at
`O(chunk)`, but neither crosses the cross-worker exchange barrier per chunk:

- `backfill_view` (`catalog/ddl.rs:356`) — the **single-node** path: drives
  `execute_epoch` per chunk locally, no IPC.
- `handle_unique_preflight` (`runtime/worker.rs:1446`) — accumulates spans
  locally and sends **one** reply frame at the end, no per-chunk exchange.

The streaming primitives they use already exist: `open_store_cursor(tid) ->
Option<CursorHandle>` (`catalog/store.rs:1664`), `CursorHandle.cursor`
(`storage/read_cursor.rs:1223`, an owned struct holding its sources via `Rc` —
no catalog borrow, safe to hold across chunks), `ReadCursor::drain_chunk(max)
-> Option<Batch>` (consolidated, sorted, never splits a (PK, payload) merge
group across chunks; `read_cursor.rs:982`), and the chunk-size constant
`ddl_scan_chunk_rows` (default `DDL_SCAN_CHUNK_ROWS = 65_536`,
`read_cursor.rs:73`; tests shrink it to force boundaries).

## Why a naive per-chunk `evaluate_dag` is wrong

Looping `drain_chunk` + `evaluate_dag` per chunk **silently corrupts**
exchange-view backfill, because the per-chunk rounds are generated locally by
each worker draining its own partition, while the exchange barrier assumes one
round in lockstep:

- The master exchange accumulator (`runtime/reactor/exchange.rs:24`, `:58`) is
  keyed by `(view_id, source_id)` with **no round/epoch component** and relays
  the moment `count == num_workers` reports arrive
  (`exchange.rs:73`), with each worker's payload stored at `payloads[w]`. Two
  workers with unequal partition sizes desync: when a faster worker sends its
  next round's payload, it **overwrites its own slot** and pushes `count` to
  `num_workers` from two reports by the same worker — the master relays a
  mismatched/empty-peer batch and the earlier payload is lost. Not a clean
  hang: data corruption first, then a wedge.
- The boot driver `collect_acks_and_relay` (`runtime/master.rs:599`) is
  bootstrap-only (runs before the reactor) and marks a worker **done** on its
  first non-`FLAG_EXCHANGE` reply. A worker that interleaves a tick-ACK with
  more chunks would be retired early.
- No per-table cardinality is tracked anywhere in the catalog/store, so workers
  cannot independently derive a common round count "for free".

The one property already on side: `evaluate_dag_multi_worker` runs `do_exchange`
**unconditionally**, including on an empty pre-phase batch (`dag.rs:1216`,
empty-placeholder queueing at `dag.rs:1263`), and `do_exchange_wait` encodes and
sends a 0-row batch as a valid round (`worker.rs:1696`). So an exhausted worker
can **pad** with empty rounds to stay in lockstep.

## Core invariant

For every `(view_id, source_id)` that exchanges, **all workers must issue the
same number of exchange rounds**. Short partitions pad with empty
`evaluate_dag` calls. With equal round counts the accumulator gets exactly
`num_workers` reports per round — no collision, no key change required.

Views where `evaluate_dag_multi_worker` does **not** exchange (the
`skip_exchange` co-partition shortcut, `dag.rs:1206`; the `has_join_shard`
co-partitioned arm, `dag.rs:1242`) have no barrier and may chunk freely with no
coordination. Coordination is needed only for true-exchange views: the
range-join broadcast, GROUP BY / reduce, set-ops, and non-co-partitioned joins.

## Design — agree a round count, then stream with padding (recommended)

Least invasive: the exchange accumulator, the wire format, and the relay/ACK
loop in `collect_acks_and_relay` all stay unchanged; only `handle_backfill` and
the boot driver's preamble change.

1. **Local round bound.** Each worker computes an upper bound on its chunk count
   for `source_tid`. Prefer a cheap bound from shard metadata
   (`ceil(sum_of_shard_entry_counts / chunk_rows)`; over-estimates are harmless
   — surplus rounds are empty). If no cheap raw count is exposed, fall back to a
   drain-and-discard counting pass over a throwaway `open_store_cursor`
   (`O(chunk)` memory, one extra `O(N)` read).
2. **All-reduce `max_rounds`.** Before the existing relay loop, `fan_out_backfill`
   (`master.rs:837`) collects each worker's local bound and broadcasts
   `max_rounds = max_w(bound_w)`. Workers report the count and read back the max.
   (A tiny count round; reuses the boot W2M/broadcast rings.)
3. **Stream exactly `max_rounds` rounds.** Re-open the cursor; per round drain a
   chunk if available, else synthesize an empty batch, and call `evaluate_dag`.
   The existing `collect_acks_and_relay` loop relays each round through the
   accumulator (now exactly `num_workers` reports per round) and retires each
   worker on its single terminal ACK after the loop — unchanged.

```rust
fn handle_backfill(&mut self, source_tid: i64, request_id: u64) -> Result<(), String> {
    let chunk_rows = self.cat().ddl_scan_chunk_rows;
    let has = self.cat().has_id(source_tid);

    // (1) local bound, (2) agree the global round count. A worker missing the
    // table (or a co-partitioned view that skips the exchange) reports 0 and
    // simply pads; the exchange shape determines whether max_rounds > local.
    let local_rounds = if has { self.cat().backfill_round_bound(source_tid, chunk_rows) } else { 0 };
    let max_rounds = self.backfill_allreduce_max_rounds(source_tid, local_rounds);

    // (3) stream max_rounds rounds, padding exhausted workers with empties.
    let schema = self.cat().get_schema_desc(source_tid)
        .ok_or_else(|| format!("backfill: no schema for {source_tid}"))?;
    let mut handle = if has { self.cat().open_store_cursor(source_tid) } else { None };
    for _ in 0..max_rounds {
        let chunk = handle.as_mut()
            .and_then(|h| h.cursor.drain_chunk(chunk_rows))
            .unwrap_or_else(|| Batch::with_schema(schema, 0));
        self.evaluate_dag(source_tid, chunk, request_id);
    }
    Ok(())
}
```

`backfill_round_bound` and `backfill_allreduce_max_rounds` are the only new
units; the cursor loop, schema fallback, and `evaluate_dag` plumbing are
existing patterns (`backfill_view`, the equi exchange arm).

### Alternatives

- **Collective stop-signal loop** (avoids the counting pass / single read):
  loop `drain_chunk`-or-empty → `evaluate_dag`, and stop when a round in which
  **no** worker had data completes. Needs the master — which holds all
  `num_workers` payloads when it assembles a round — to stamp a "more rounds
  remain" bit onto the relay and `do_exchange_wait` to surface it (for a
  broadcast view this is just "the concatenated relay was non-empty"; a scatter
  view needs the bit set explicitly). Cheaper I/O, but touches the relay path
  and `do_exchange_wait`'s return.
- **Master-driven rounds**: persist a `CursorHandle` in `WorkerProcess`
  (precedent: `pending_scan`, `worker.rs:183`), add a `FLAG_BACKFILL_CHUNK`
  per-round command and a per-round exhausted bit, and drive rounds from the
  master until all workers report exhausted. Single read, but requires
  round-numbering the accumulator key (`exchange.rs:24`) and the `FLAG_EXCHANGE`
  wire — the most invasive option; not recommended.

## The change, by file

- `runtime/worker.rs` — rewrite `handle_backfill` (`:1419`) to the chunked
  round loop; add `backfill_allreduce_max_rounds` (report local bound, receive
  the max). Add `backfill_round_bound` on the catalog (`catalog/store.rs`),
  cheap shard-count bound with a drain-and-discard fallback.
- `runtime/master.rs` — `fan_out_backfill` (`:837`): collect per-worker round
  bounds and broadcast `max_rounds` before entering `collect_acks_and_relay`.
  The relay/ACK loop (`:599`) is unchanged.
- No change to `reactor/exchange.rs`, the `FLAG_EXCHANGE` wire, or
  `do_exchange_wait` for the recommended design.

## Correctness

- **Chunk-sum equals whole-batch.** Trace integration is additive and
  incremental evaluation is linear, so summing per-chunk results equals the
  whole-partition result — the same property `backfill_view` relies on.
- **Equal round counts** keep the `(view_id, source_id)` accumulator at exactly
  `num_workers` reports per round, eliminating the slot-overwrite collision.
- **Empty padding rounds** emit valid empty exchange payloads (`dag.rs:1216`,
  `worker.rs:1696`) — the barrier participates without contributing rows.
- **Boot-time snapshot stability.** Backfill runs before the reactor; source
  partitions are static, so the counting pass and the streaming pass observe the
  same rows, and a held/re-opened cursor stays valid across rounds
  (`open_store_cursor` contract, `store.rs:1656`).
- **Completion** is still one terminal ACK per worker after the round loop;
  `collect_acks_and_relay`'s done-detection is untouched.

## Testing

- **Skewed partitions** (all rows hash to one worker): backfill completes,
  result matches a single-node reference, no corruption or hang. Shrink
  `ddl_scan_chunk_rows` (e.g. to 3) to force many rounds and unequal local
  counts across workers.
- **Range-join broadcast backfill** (`GNITZ_WORKERS=4`) over a multi-chunk
  table: result equals the cross-filter reference; master relay group stays
  `O(num_workers × chunk)`, not `O(num_workers × N)`.
- **GROUP BY / reduce, set-op, and non-co-partitioned join** backfill under
  chunking — each exchanges; verify equal-round padding holds.
- **Co-partitioned / `skip_exchange` view**: chunks with no coordination,
  result correct (no barrier to desync).
- **Empty partition on some workers** (local bound 0): still participates via
  padding to `max_rounds`; downstream view consistent.
- **Large table** memory regression: peak worker RSS bounded near `O(chunk)`,
  not `O(N)`.

## Out of scope

- Live (post-boot) re-backfill / incremental view rematerialization — backfill
  here is the boot-time CREATE-over-populated-tables path only.
- Range-partitioned exchange to shrink the broadcast `num_workers ×` factor —
  see `range-join.md` Out of scope.
