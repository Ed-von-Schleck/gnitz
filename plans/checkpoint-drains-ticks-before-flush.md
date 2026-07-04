# Drain ticks before the checkpoint flush

A checkpoint silently discards committed-but-unticked view deltas, so **views
diverge from base tables until a restart rebuilds them**. The committer
checkpoints before the pending push batch is ticked (`committer.rs:145-166`), and
`handle_flush_all` begins with `self.pending_deltas.clear()`
(`worker/mod.rs:1460`), dropping every buffered effective delta. The base table
keeps the rows (ingested at push time, flushed by the checkpoint), but no later
tick ever applies them to dependent views.

Reproduced deterministically (`GNITZ_WORKERS=4`, `GNITZ_CHECKPOINT_BYTES=4096`):
push 2000 rows, push 3 more (which triggers the checkpoint first), scan — the base
table has 2003 rows, the view has 3; worker logs show 501+499+501+499 = 2000
buffered delta rows discarded. A restart heals it via rebuild, which is exactly
why it went unnoticed.

Fix: drive the pending ticks to completion **before** the checkpoint flush, and
delete the `clear()`. This uses only the existing single-`FLAG_FLUSH`
checkpoint — no new checkpoint protocol.

Pre-alpha: no compatibility concern.

---

## Current mechanics (verified against source)

- View state advances on TICK, not PUSH. `handle_push`
  (`worker/mod.rs:1115-1141`) ingests the base delta and buffers the *effective*
  (post-unique-pk) delta into `pending_deltas[tid]`; `handle_tick`
  (`worker/mod.rs:1143-1158`) drains it and drives the DAG. Ticks are
  trigger-driven (`tick_loop_async`, `executor.rs:388-492`).
- The committer's checkpoint decision (`committer.rs:145`,
  `sal_needs_checkpoint() || (has_barriers && !sal_has_relay_space())`) runs
  `run_checkpoint_phase` (`committer.rs:238-264`), which holds `sal_writer_excl`
  across the `FLAG_FLUSH` broadcast → per-worker `handle_flush_all` → ACK-wait →
  `checkpoint_post_ack` (`master/dispatch.rs:1741-1750`) → `checkpoint_reset`.
- `handle_flush_all` (`worker/mod.rs:1459-1511`) begins with
  `self.pending_deltas.clear()` (`:1460`) — the discard.
- The committer already holds the tick ledger (`tick_rows`/`tick_tids`,
  `committer.rs:82-84`) bumped on every buffered push
  (`committer.rs:503-511` / `worker/mod.rs:1133-1137`). Tick triggers `Drain{tids,
  done}` (scan barrier, `executor.rs:1190-1221`) and `Quiesce{acked, release}`
  (CREATE-VIEW DDL, `executor.rs:443-457,1444-1455`) already exist; the tick loop
  processes `Quiesce` before ticking (`executor.rs:450-457`).

## Design

The committer gains a tick-trigger sender the same way it already holds
`fire_auto_tick` (a closure over `tick_tx`, `executor.rs:233-235`). No new trigger
variants — `Drain` and `Quiesce` are reused as two sequential awaits. On a
checkpoint, before broadcasting `FLAG_FLUSH`:

```
snapshot dirty tids from the tick_rows/tick_tids ledger
  → send TickTrigger::Drain{tids, done};  await done       (buffered deltas ticked into views)
  → send TickTrigger::Quiesce{acked, release}; await acked  (tick loop parked)
  → run the existing single-FLAG_FLUSH checkpoint
  → release Quiesce.
```

Because `Drain` must complete before the tick loop will process `Quiesce`
(`executor.rs:450-457`), the drain is its own round-trip. Barrier requests
arriving mid-drain are serviced by running the existing checkpoint directly (no
drain precondition — views are not persisted by today's checkpoint, so a
mid-drain reset loses nothing durable). Push requests stay queued during the
sequence, so `pending_deltas` stays empty from the end of the drain through the
flush.

Then delete `self.pending_deltas.clear()` at `worker/mod.rs:1460`. After the
drain, `pending_deltas` is already empty, so the `clear()` is redundant — and
harmful, because the whole bug is that it discards deltas that a checkpoint
interleaving left buffered. Removing it means no committed push delta is ever
dropped.

## Correctness

**`pending_deltas` never clears, and stays bounded.** Every buffered entry has a
matching `tick_rows` bump on the same push (`committer.rs:503-511` /
`worker/mod.rs:1133-1137`), so auto-ticks (10k rows / 20 ms) and the checkpoint
drain both bound the buffer, including for tables with no dependent views (their
ticks are cheap no-ops). Add a debug assertion coupling the two paths (a buffered
`pending_deltas[tid]` entry implies a `tick_rows[tid]` bump).

**The drain empties the buffer before the flush.** With pushes queued for the
duration of the sequence and `Quiesce` parking the tick loop, no new delta
buffers between the drain's completion and the flush; the flush therefore sees an
empty `pending_deltas` and the deleted `clear()` is a no-op on the happy path,
correctness-preserving on every path (a delta that *was* buffered is ticked, not
discarded).

## Tests

- **Delta-loss regression (E2E, `make e2e`, `GNITZ_WORKERS=4`).** The repro above
  (base 2003 vs view 3 under `GNITZ_CHECKPOINT_BYTES=4096`) adapted to assert
  `view == base` across a checkpoint. Fails today; must pass after this fix.
- **Sustained ingest across many checkpoints (E2E).** Small SAL
  (`GNITZ_SAL_BYTES=16 MiB`) crossing many checkpoints with concurrent scans: no
  wedge, views stay equal to base throughout.

## Sequencing

One commit; the tree is green (`make verify` + `make e2e`) after it.

- [ ] 1. **Checkpoint drains ticks; delete `pending_deltas.clear()`.** Committer
  gains the tick-trigger sender; checkpoint sequence = snapshot dirty tids →
  `Drain` (await done) → `Quiesce` (await acked) → existing single-`FLAG_FLUSH`
  checkpoint → release; Barrier mid-drain runs the existing checkpoint directly.
  Delete the `clear()`; add the coupling debug assertion. Add the delta-loss
  regression test and the sustained-ingest test.
