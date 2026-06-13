# Chunked distributed view backfill

## Goal

Bound the peak memory of distributed (multi-worker) CREATE VIEW backfill, which
streams a source partition of `N` rows through an incremental plan across a
cross-worker exchange. Three resources scale with `N` today, and a complete
design must address all three:

- **Per-worker RAM** — `O(N)` (the whole partition materialized into one
  `Batch`, plus its derived intermediates) → `O(chunk)`. Fixed by draining the
  partition in fixed-size chunks.
- **Master transient relay RAM** — `O(num_workers × N)` (the exchange
  accumulator holds one whole-partition `Batch` per worker, and `prepare_relay`
  scatters/broadcasts them into per-worker dest batches) → `O(num_workers ×
  chunk)`. Fixed by relaying one chunk-sized round at a time.
- **Master SAL footprint** — the cumulative bytes the relay writes to the
  shared append log. **Chunking does NOT reduce this.** The SAL is append-only
  within an epoch and is not reclaimed as workers consume relays; the only reset
  (`checkpoint_reset`, `sal.rs:981`) bumps the epoch and is forbidden
  mid-backfill. `N/chunk` rounds each write `O(num_workers × chunk)` and
  accumulate to the same `O(num_workers × N)` — plus a `GROUP_HEADER_SIZE = 576`
  byte header per round (`sal.rs:26`), so chunking makes the cumulative footprint
  *slightly larger*. Bounding the SAL requires reclaiming space at a round
  barrier, which only the chunked round structure makes safe (see
  [The SAL is the true ceiling](#the-sal-is-the-true-ceiling)).

`N` is a worker's committed partition row count. `chunk` is
`ddl_scan_chunk_rows` (default `DDL_SCAN_CHUNK_ROWS = 65_536`,
`read_cursor.rs:73`).

The SAL is a fixed 1 GiB mmap (`SAL_MMAP_SIZE = 1 << 30`, `sal.rs:27`), so for
relay-heavy views it — not RAM — is the binding constraint on backfill size, and
it is exactly the resource the *"SAL space exhausted during backfill exchange
relay"* guard (`master.rs:625`) protects. A plan that bounds only RAM does not
raise the ceiling on the headline case (the range-join broadcast); it only helps
backfills whose total relay volume already fits the SAL.

## Current state (verified against code)

`handle_backfill` (`runtime/worker.rs:1419`) materializes the worker's entire
source partition into one batch and pushes it through a single `evaluate_dag`:

```rust
fn handle_backfill(&mut self, source_tid: i64, request_id: u64) -> Result<(), String> {
    if !self.cat().has_id(source_tid) { return Ok(()); }
    let local_batch = self.cat().scan_family(source_tid)?;   // whole partition → one Batch
    let owned = Rc::try_unwrap(local_batch).unwrap_or_else(|a| (*a).clone());
    self.evaluate_dag(source_tid, owned, request_id);         // whole partition → one evaluate_dag pass
    Ok(())
}
```

`scan_family` (`catalog/store.rs:785`) → `scan_store` (`store.rs:1799`) →
`open_cursor().cursor.materialize()` (`read_cursor.rs:942`) has no row cap;
`materialize` drains the whole cursor into one `Batch`. The worker run loop's
backfill arm (`worker.rs:901`) calls `handle_backfill` and then sends exactly one
terminal non-exchange ACK (`send_ack`, `worker.rs:905`).

The master broadcasts `FLAG_BACKFILL` and then drives the exchange relay inline
via `collect_acks_and_relay`, which maintains its own `ExchangeAccumulator`
(`master.rs:601`, accumulator constructed at `:605`; the reactor's accumulator
is not yet wired at boot). For a view whose `evaluate_dag_multi_worker` performs
an exchange, the master collects one payload per worker; for the range-join
broadcast it concatenates all `W` payloads and clones per worker
(`prepare_relay` → `op_relay_broadcast`, `master.rs:768`), so a whole-partition
backfill batch makes the master relay group `O(num_workers × N)` and can trip the
SAL guard (`master.rs:625`). This is view-type-agnostic: every distributed
backfill loads its partition whole today.

What each view type actually puts on the wire (the relay volume) differs, and
this determines how badly the SAL binds (`dag.rs`, `evaluate_dag_multi_worker`
at `:1192`):

- **Range-join broadcast** (`dag.rs:1236`) does **two** exchange rounds per
  `evaluate_dag`: (1) an input broadcast of the *raw source delta* to every
  worker at `source_id == src_id` (`do_exchange(view_id, &input_with_schema,
  src_id)`, `dag.rs:1254`) — `O(num_workers × N)`; then (2) an output scatter of
  the post-probe `pre_result` at `source_id == 0` (`dag.rs:1265`). The broadcast
  is the worst case; SAL-bound to very small tables.
- **Non-co-partitioned join** (`has_join_shard`, non-co-part branch at
  `dag.rs:1310`) relays the *raw source delta*, scattered by reindex key at
  `source_id == src_id`: one round, `O(N)`.
- **Binary set-ops** (`has_exchange && view_has_side_b`, `dag.rs:1268`;
  `run_two_sided`) relay one round per *active* side at that side's `source_id`
  (`== src_id`, always `> 0`). The relayed payload is the **pre-phase output** —
  a linear `HashRow` repartition of the side (`do_exchange(view_id, &pre,
  side_src)`, `dag.rs:1271`), **not** the raw delta — but still `O(N)`.
- **GROUP BY / reduce / DISTINCT** (`has_exchange`, `dag.rs:1273`) relays the
  *pre-phase output* (locally aggregated partials, `do_exchange(view_id,
  &pre_result, 0)` at `:1293`), typically `O(distinct groups)`, in **one** round
  at `source_id == 0`. Chunking re-aggregates *per chunk*, so a group spanning
  `K` chunks emits `K` partials — chunking can *increase* a GROUP BY's relay
  volume even as it lowers worker RAM, which makes the SAL reclamation below
  matter *more* for these views, not less.

Two facts here drive the design below. First, a `source_id`-**only**
termination discriminator is insufficient: `source_id == 0` is the range-join's
*output scatter* on a range-join view but the *sole (input-tracking) round* on a
GROUP BY / reduce / DISTINCT view, so telling the two apart needs the view type
(`view_is_range_join(view_id)`, which the master has). The pad bit below sidesteps
the view-type lookup entirely. Second, the relayed payload's emptiness
does **not** track a worker's drain exhaustion for GROUP BY / set-op views,
because the relayed `pre_result` is the **pre-phase output** and the pre-phase
includes the view's `WHERE` filter and reindex (the `ExchangeShard` node splits
pre/post in `compiler.rs`; the stateful reduce is *post*-relay) — a non-empty
chunk that filters out entirely relays an *empty* payload. Only the range-join
exchanges more than once per chunk. Both facts are why termination uses a
**worker-reported pad bit**, not relayed-payload emptiness — see
[Design](#design--master-driven-rounds-with-a-worker-reported-stop-signal-recommended).

Two sibling backfill paths already stream chunk-wise and bound peak RAM at
`O(chunk)`, but neither crosses the cross-worker exchange barrier per chunk:

- `backfill_view` (`catalog/ddl.rs:356`) — the **single-node** path: drives
  `execute_epoch` per chunk locally (loop at `ddl.rs:376`), no IPC. The
  precedent for chunk-sum-equals-whole-batch.
- `handle_unique_preflight` (`runtime/worker.rs:1446`) — accumulates spans
  locally and sends **one** reply frame train at the end, no per-chunk exchange.

The streaming primitives they use already exist:
- `open_store_cursor(table_id) -> Option<CursorHandle>` (`catalog/store.rs:1815`).
- `CursorHandle.cursor` (field at `read_cursor.rs:1237`), an owned struct whose
  `ReadCursor` holds its sources via `Rc` — no catalog borrow, safe to hold
  across chunks while *other* relations are written. (The struct doc comment at
  `read_cursor.rs:1232` says "Arc"; the code uses `Rc`. Trivial doc fix.)
- `ReadCursor::drain_chunk(max) -> Option<Batch>` (`read_cursor.rs:982`):
  consolidated and sorted (`batch.sorted = true; batch.consolidated = true`,
  `:1006`–`1007`), returns **at most** `max` rows, never splits a `(PK, payload)`
  merge group across chunks (each drained entry is one fully-folded group; the
  limit is checked per group, `:1160`), and returns **`None`** — not
  `Some(empty)` — when exhausted (`:985`, `:988`, `:995`). The Some-iff-non-empty
  property is exact: the only `Some` paths return a `count > 0` batch, so the
  `drain_chunk(...).unwrap_or_else(|| empty)` exhaustion-detection idiom is sound.
- The chunk-size field `ddl_scan_chunk_rows` (`catalog/mod.rs:156`); tests shrink
  it (e.g. to `3`, `catalog/tests/ddl_tests.rs:643`) to force boundaries.

## Why a naive per-chunk `evaluate_dag` is wrong

Looping `drain_chunk` + `evaluate_dag` per chunk **deadlocks** exchange-view
backfill, because each worker drains its *own* partition and partitions are
unequal, while the exchange barrier assumes all workers issue the same number of
rounds in lockstep.

`do_exchange_wait` (`worker.rs:1685`) is **blocking**: it sends one
`FLAG_EXCHANGE` for `(view_id, source_id)` up the **W2M ring**
(`w2m_writer.send_encoded`, `:1698`–`1704`) and then loops (`:1710`) on
`sal_reader.wait()` (`:1715`) until the matching `FLAG_EXCHANGE_RELAY` comes back
**down the SAL** and is returned (`:1712`/`:1735`). (The two legs use different
transports: worker→master reports ride W2M, master→worker relays ride the SAL —
which is why the SAL epoch reset below cannot disturb the consumption proof.) A
worker therefore cannot issue its next round for the same key until the current
round has been relayed back. The accumulator
(`reactor/exchange.rs:24`/`:55`/`:73`) is keyed by `(view_id, source_id)` with no
round component and relays the moment `count == num_workers`. Because every round
completes and is removed before any worker is unblocked, no worker ever writes
two reports into one live round — so there is **no slot-overwrite and no data
corruption**. The failure is a clean deadlock:

- 2 workers, `W0` has 2 chunks, `W1` has 1. Round 1: both report → relay → both
  unblock. `W0` drains chunk 2 → sends round 2 → blocks. `W1` is exhausted → its
  `handle_backfill` returns → run loop sends its **terminal ACK** (a non-exchange
  reply).
- `collect_acks_and_relay` marks a worker **done** on its first non-`FLAG_EXCHANGE`
  reply (`collected[w] = true`, `master.rs:639`). So it retires `W1` — while
  `W0`'s round 2 sits at `count == 1`, can never reach `num_workers`, and `W0`
  blocks in `do_exchange_wait` forever. The master then spins in its `wait_for`
  loop (`master.rs:643`–`649`) on the still-uncollected `W0`. Wedge.

So a shorter worker's terminal ACK is mistaken for "this view's exchange is
done," retiring it before a longer worker has finished issuing rounds.

The property already on side: `evaluate_dag_multi_worker` reaches the exchange
arm regardless of `delta.count` (`build_pending` seeds a pending entry for an
empty batch too, `dag.rs:1064`), and `do_exchange_wait` encodes and sends a 0-row
batch as a valid round unconditionally (`worker.rs:1698`–`1704`, no emptiness
check). So an exhausted worker can **pad** with empty rounds to stay in lockstep,
and the master's accumulator still receives exactly `num_workers` reports for
that round.

## Core invariant

For every `(view_id, source_id)` that exchanges, **all workers must issue the
same number of exchange rounds**. Short partitions pad with empty `evaluate_dag`
calls. With equal round counts the accumulator gets exactly `num_workers` reports
per round — no early retirement, no collision.

Views where `evaluate_dag_multi_worker` does **not** exchange have no barrier and
may chunk freely with no coordination:

- the `skip_exchange` co-partition shortcut (`dag.rs:1231`: `is_trivial &&
  is_co_partitioned`),
- the co-partitioned `has_join_shard` arm (`dag.rs:1299`: `plan_source_co_partitioned`),
- the plain single-phase `else` arm (`dag.rs:1313`).

Coordination is needed only for true-exchange views: the range-join broadcast
(two rounds per `evaluate_dag` — input broadcast then output scatter), GROUP BY /
reduce / DISTINCT, set-ops (a binary set-op exchanges once per *active* side),
and non-co-partitioned joins.

## The SAL is the true ceiling

Chunking the relay shrinks each *round's* group and the master's *transient* RAM,
but it does not shrink the **cumulative** SAL footprint, because nothing reclaims
SAL space between rounds during backfill:

- The SAL write cursor is monotonic within an epoch — every relay group advances
  it by the group size (`commit` returns `base + total`, `sal.rs:343`);
  `sal_begin_group`'s fit test is `write_cursor + total > mmap_size`
  (`sal.rs:375`), against the absolute cursor.
- A worker reading a relay reclaims nothing: each worker advances only its own
  private `read_cursor` (`worker.rs` per-worker field); the SAL header has no
  shared consume watermark (unlike the W2M ring, which does).
- `maybe_checkpoint` runs once at the *start* of `fan_out_backfill`
  (`master.rs:853`) and never between rounds; `collect_acks_and_relay` only
  *checks* free space (`sal_relay_space_ok_raw`, `master.rs:694`) and **errors**
  when low rather than checkpointing. This is deliberate (`master.rs:621`): a
  mid-backfill `FLAG_FLUSH` bumps the SAL epoch, and workers reject groups whose
  epoch ≠ their expected epoch — orphaning unconsumed backfill groups and hanging
  boot.

Consequence: `N/chunk` rounds writing `O(num_workers × chunk)` each accumulate to
`O(num_workers × N)` regardless of chunk size, and the guard trips at the same
cumulative volume. Worse, in the band where one whole-partition group fits the
1 GiB mmap but the cumulative crosses the ⅛-reserve threshold (`mmap >> 3`,
`master.rs:695`) — i.e. once cumulative > `896 MiB` (1 GiB − 128 MiB) — naive
chunking *regresses* a backfill that succeeds today: the single group cleared the
start-of-relay check, but the many small groups trip the reserve partway through.
Plus the `576 B`/round header overhead. **Chunking alone is net-negative for the
SAL.**

The fix the chunked structure enables: **reclaim the SAL at a per-round barrier
when space is low.** The actual `checkpoint_reset` (epoch bump + `write_cursor =
0`, `sal.rs:981`) is a *write-side* operation, so the **master** (the sole
`SalWriter`) performs it; workers are `SalReader`s and only advance their own
read-side epoch + reset their private `read_cursor`. The barrier is the
**individual exchange round**, not the chunk — each round is already a full
all-workers handshake (the accumulator relays at `count == num_workers`), so the
reset can be injected before *any* round and needs **no** knowledge of where chunk
boundaries fall (and therefore no `view_is_range_join` / `source_id`
discriminator). The safe ordering:

1. When about to relay round `R` and space is low, the master stamps
   `checkpoint-then-continue` on round `R`'s relay instead of erroring, and marks a
   pending reset.
2. Each worker, **inside `do_exchange_wait` the moment it consumes that relay**,
   advances its expected epoch and resets its `read_cursor` — *before* it issues
   round `R+1`'s `FLAG_EXCHANGE` (W2M). This reuses the existing reader-side
   checkpoint path: `next_sal_message` (`worker.rs:462`) re-reads `expected_epoch`
   every call and **parks** any group whose epoch exceeds the expected one at the
   cursor until the cursor is reset — exactly the FLAG_FLUSH mechanism, driven here
   by the relay's signal instead of a FLAG_FLUSH group.
3. The master holds the actual `checkpoint_reset` until **every** worker's round
   `R+1` report has arrived — which proves all workers consumed round `R`'s relay
   (the last SAL write before the reset) and have already bumped their epoch. It
   then resets and writes round `R+1`'s relay at `write_cursor = 0` in the new
   epoch, which all workers now expect (until then their `next_sal_message` parks
   harmlessly at cursor 0).

The round-`R+1` reports ride W2M, not the SAL, so they survive the epoch reset.
No unconsumed SAL groups straddle the reset — the precise condition the current
whole-batch path cannot establish and therefore forbids checkpointing. Because the
worker bumps its epoch *per consumed relay* (not once per chunk), the range-join's
two within-chunk rounds are handled with no special case: if space goes low on the
broadcast round, the master resets before the scatter round, which the worker has
already re-epoched to accept.

**What this bounds, and what it does not.** Peak resident SAL is the high-water
between two resets. Resetting only *when low* (free `< mmap >> 3` = 128 MiB) lets
each epoch fill to roughly `mmap − reserve ≈ 896 MiB` before recycling, so peak SAL
is **`O(mmap)` — a constant ≈ 896 MiB–1 GiB, independent of `N`** (it is the
*cumulative*-over-`N` growth that reclamation kills, letting backfill scale past
the 1 GiB ceiling). It is *not* `O(num_workers × chunk)`; that bound applies only
to the per-round **transient** (the master's in-RAM accumulator/dest batches plus
the one in-flight relay group). Driving peak down to `O(num_workers × chunk)` would
require resetting after *every* round — needless epoch churn — and is not the goal.

**Precondition.** The trigger fires while ≥ `size(round R)` is still free (it
crosses *below* the 128 MiB reserve), and round `R` is written at the high cursor
*before* the reset, so per-round reclamation works only if **a single round's relay
group fits the reserve** (`mmap >> 3` = 128 MiB). A broadcast round whose
`num_workers × chunk × row_width` exceeds 128 MiB cannot be reclaimed this way and
remains a hard ceiling: shrink `ddl_scan_chunk_rows` (chunk size thus gains an
upper bound for broadcast views) or eliminate the `num_workers ×` factor with a
range-partitioned exchange (see [Out of scope](#out-of-scope)).

The protocol requires the master to *drive* the rounds (so it owns the barrier and
injects the reset), which is why the recommended design is master-driven.

## Design — master-driven rounds with a worker-reported stop signal (recommended)

The master already relays every round inline in `collect_acks_and_relay` and, at
each round's completion, holds all `num_workers` reports — so it can combine a
per-worker fact across the round and stamp a collective decision back. Drive
termination and SAL reclamation from there; equal round counts hold by
construction.

1. **Workers loop drain-or-pad until told to stop.** `handle_backfill` opens a
   cursor once and loops: `drain_chunk` a chunk if available, else synthesize an
   empty `Batch::with_schema(schema, 0)`; record whether this chunk was a **pad**
   (drain returned `None`); call `evaluate_dag`; then read the collective signal
   and continue or stop.

2. **Workers report per-chunk exhaustion; the master ANDs it.** Exhaustion is a
   property of the **source drain**, not of any view: a chunk is the last one
   exactly when every worker's `drain_chunk` returned `None`. Each worker stamps
   its **pad bit** onto the outbound `FLAG_EXCHANGE` of every round the chunk
   issues — a free control field, since `do_exchange_wait` passes a literal `0` in
   `seek_col_idx` today (`worker.rs:1701`) and the accumulator never reads it. The
   accumulator (`process`, `reactor/exchange.rs:55`) already collects all
   `num_workers` reports per `(view_id, source_id)` round, so it **ANDs** the pad
   bits: when every worker is a pad on a round the chunk is the final all-pad round
   → the master stamps **stop** into the relay's `seek_col_idx`; otherwise
   **continue** (or **checkpoint-then-continue**, step 3). Because a worker stamps
   the *same* per-chunk pad bit on every round it issues (the range-join's
   broadcast *and* scatter, a self set-op's two same-key rounds), the master sees
   all-pad on every round of an all-pad chunk and may stamp stop on any of them —
   **no input-vs-output round discriminator is needed**, and the result is
   independent of `source_id`.

   *Why worker-reported, not payload-emptiness.* Inferring "all workers exhausted"
   from "all relayed payloads empty" is **wrong** for views with a row-dropping
   pre-phase. For GROUP BY / reduce / DISTINCT and set-ops the relayed payload is
   the **pre-phase output** (`do_exchange(view_id, &pre_result, 0)` /
   `do_exchange(view_id, &pre, side_src)`), and the pre-plan contains the view's
   `WHERE` filter and reindex — the stateful reduce sits *post*-relay (the
   `ExchangeShard` node is the pre/post boundary in `compiler.rs`). A non-empty
   chunk all of whose rows fail the predicate relays an **empty** payload, so if
   every worker's chunk filters out on the same round, payload-emptiness falsely
   reads "exhausted" and stops backfill early — **truncating the view** (a wrong
   result, not merely a hang). The master cannot recover the raw drain state for
   these views: it only ever sees the post-filter payload. (Payload-emptiness is
   also ambiguous by `source_id`, since a GROUP BY's sole exchange shares
   `source_id == 0` with the range-join's *output* scatter.) The pad bit is
   filter- and view-type-agnostic: the worker knows it drained `None`. See
   [Alternatives](#alternatives) for the rejected payload-emptiness variant.

   The collective signal rides the relay's `seek_col_idx`, also free today —
   `emit_relay` (`master.rs:800`) passes `source_id` in `seek_pk` and a literal
   `0` in `seek_col_idx` (`send_to_workers`, `master.rs:811`–`812`), and the
   worker reads only `seek_pk` off the relay. So **both legs reuse an existing
   free `seek_col_idx`** (worker→master pad bit on `FLAG_EXCHANGE`; master→worker
   decision on `FLAG_EXCHANGE_RELAY`) — no new flag, no wire-format change.
   Termination is one all-pad round beyond the last data round, and every worker
   issues an identical number of rounds.

3. **Reclaim the SAL at the per-round barrier when low.** Before stamping
   continue, if `sal_relay_space_ok_raw()` reports low, encode a third signal value
   (`checkpoint-then-continue`) instead of erroring. The worker handles this signal
   **inline in `do_exchange_wait`, per consumed relay** — it advances its read-side
   epoch and resets its `read_cursor` immediately, before issuing the next round —
   *not* at the chunk boundary. (A once-per-chunk bump would force the master to
   avoid resetting between a range-join's two within-chunk rounds, which would
   reintroduce a chunk-boundary / `view_is_range_join` discriminator; bumping per
   relay needs none.) The **master** performs the actual `checkpoint_reset`
   (write-side: epoch bump + `write_cursor = 0`) once every worker's next-round
   report confirms it consumed the prior relay, then writes the next round in the
   new epoch (full ordering in
   [The SAL is the true ceiling](#the-sal-is-the-true-ceiling)). This is the
   element that lets cumulative relay volume exceed the mmap.

Two signals share the relay's `seek_col_idx` but are consumed differently. The
**checkpoint** is acted on inline (step 3), per relay, in `do_exchange_wait`. The
**stop/continue** decision is recorded into a single per-backfill slot — the same
place relays are stashed by key today (`pending_relays.insert((view_id,
source_id), batch)`, `worker.rs:773`) — and read once after `evaluate_dag`
returns; a `checkpoint-then-continue` relay records `Continue` in that slot after
applying the checkpoint. The slot is **cleared by the read each chunk** (`take`
semantics), so "the slot is `Some` ⇒ this chunk issued an exchange" holds: every
round of a chunk carries the same stop/continue decision (the AND of the same
per-chunk pad bits), so a last-wins read is safe. If the source feeds **no**
exchange view, no relay arrives and the slot stays `None`: the worker
self-terminates on local exhaustion (a non-exchange source has no barrier —
`dag.rs:1231` / `1299` / `1313` — and chunks freely). `do_exchange_wait`'s `Batch`
return is unchanged.

```rust
fn handle_backfill(&mut self, source_tid: i64, request_id: u64) -> Result<(), String> {
    let chunk_rows = self.cat().ddl_scan_chunk_rows;
    let has = self.cat().has_id(source_tid);
    let schema = self.cat().get_schema_desc(source_tid)
        .ok_or_else(|| format!("backfill: no schema for {source_tid}"))?;
    let mut handle = if has { self.cat().open_store_cursor(source_tid) } else { None };

    loop {
        let drained = handle.as_mut().and_then(|h| h.cursor.drain_chunk(chunk_rows));
        let pad = drained.is_none();                 // worker-known exhaustion
        let chunk = drained.unwrap_or_else(|| Batch::with_schema(schema, 0));
        self.exchange.backfill_pad = pad;            // stamped onto each FLAG_EXCHANGE this chunk sends
        // do_exchange_wait applies any CheckpointThenContinue inline, per relay
        // (advances expected_epoch + resets read_cursor), and records only the
        // chunk-level Stop/Continue into the slot below.
        self.evaluate_dag(source_tid, chunk, request_id);
        // take_backfill_signal() is Some iff this chunk issued an exchange (barrier
        // view); the master ANDed the workers' pad bits and stamped the decision.
        match self.exchange.take_backfill_signal() {
            Some(BackfillRound::Stop) => break,
            Some(BackfillRound::Continue) => {}      // includes a round that triggered an inline checkpoint
            None if pad => break,                    // non-exchange source: no barrier, stop locally
            None => {}
        }
    }
    Ok(())
}

// inside do_exchange_wait, after consuming the relay for (view_id, source_id):
//   match decision_from_seek_col_idx(relay) {
//       Stop                 => self.exchange.backfill_signal = Some(BackfillRound::Stop),
//       CheckpointThenContinue => { self.apply_inline_checkpoint();   // bump epoch + reset read_cursor now
//                                   self.exchange.backfill_signal = Some(BackfillRound::Continue); }
//       Continue             => self.exchange.backfill_signal = Some(BackfillRound::Continue),
//   }
```

Correctness obligations to discharge in implementation: (a) the reset is taken
only at a round where every worker has consumed the prior relay (proven by all
workers' next-round reports arriving), so no group is orphaned; (b) every worker
advances its expected epoch on the same round — *inside `do_exchange_wait` at
relay-consume time*, not at the chunk boundary — so no worker rejects a post-reset
group and no range-join within-chunk round needs a discriminator; (c) the pad bit
is stamped on **every** round a chunk issues, so the master's AND is consistent
across the range-join's two rounds and a self set-op's (`a UNION a`) two
same-`(view_id, source_id)` rounds; (d) a source feeding no exchange view
self-terminates on `drain == None` — the worker distinguishes by whether
`evaluate_dag` issued any exchange this chunk (`take_backfill_signal()` returns
`None`, the slot having been cleared by the previous `take`); (e) the outbound pad
bit is `0` for all non-backfill exchanges — gate the stamp on a "backfill active"
flag so `do_exchange_wait` keeps passing its literal `0` for steady-state ticks;
(f) the accumulator (`process`, `reactor/exchange.rs:55`) must be extended to read
and AND the per-worker `seek_col_idx` pad bit it currently ignores; (g) a single
round's relay group must fit the `mmap >> 3` reserve for per-round reclamation to
apply (see the precondition in
[The SAL is the true ceiling](#the-sal-is-the-true-ceiling)).

### Alternatives

- **Master infers exhaustion from relayed-payload emptiness** (rejected). The
  master holds all `num_workers` payloads per round, so "all payloads empty" is
  tempting as the stop condition — no worker-side pad bit. It is **incorrect** on
  two counts. (1) *Filter false-stop:* for GROUP BY / reduce / DISTINCT and
  set-ops the relayed payload is the post-filter pre-phase output, so a non-empty
  chunk that fails the view's `WHERE` relays empty; a round where every worker
  filters out stops backfill early and truncates the view. (2) *`source_id`
  ambiguity:* the round that tracks input exhaustion is view-type-dependent
  (`src_id != 0` broadcast for a range join, but `source_id == 0` for GROUP BY),
  so the master needs `is_input_round = !view_is_range_join(view_id) || source_id
  != 0` and must avoid the range-join scatter (whose emptiness reflects probe
  matches). The worker-reported pad bit avoids both: it is the raw drain state,
  before any pre-phase op, and is uniform across a chunk's rounds.

- **All-reduce a fixed round count, then pad** (no per-round signal): before the
  relay loop, each worker reports a local upper bound on its chunk count and
  reads back `max_rounds = max_w(bound_w)`, then streams exactly `max_rounds`
  rounds. Simpler worker loop (`do_exchange_wait` and the relay path untouched),
  but materially *more* plumbing than the recommended design and it does **not**
  bound the SAL on its own:
  - There is **no cheap, *exact* row count** to derive the bound from. No
    summable per-table/per-shard count is exposed at the catalog/handle level:
    `MappedShard.count` (`shard_reader.rs:100`) is the physical shard-file row
    count and over-counts ghosts/tombstones and cross-source duplicate PKs,
    `manifest::entry_count` (`manifest.rs:479`) counts shard files not rows, and
    the memtable row counter (`memtable.rs:161`) is `#[cfg(test)]` (only the
    boolean `is_empty()` is exposed in production). A ready-made *upper* bound
    does exist — `ReadCursor::estimated_length()` (`read_cursor.rs:845`,
    `Σ states[].count.saturating_sub(position)`, already used by the join
    adaptive-swap heuristic) — and it is a valid bound (surplus rounds pad empty)
    obtainable straight off the open cursor. But it over-counts by the same
    ghosts/dups, so a ghost/dup-heavy table inflates it into many wasted
    full-cluster rounds; an *exact* bound needs a drain-and-discard counting pass
    (`O(N)` extra read). The plan must not assume a free *exact* count exists.
  - The count round-trip needs a **new SAL flag** (e.g. `FLAG_BACKFILL_COUNT`,
    next free bit `1 << 19`). The flag classifier is mutually exclusive and the
    "first non-`FLAG_EXCHANGE` reply == done" rule (`master.rs:639`) forbids
    reusing a bare ACK (up-leg) or `FLAG_BACKFILL` (down-leg). The *value* rides
    free in `seek_pk`/`seek_col_idx` both ways, but a new master-side
    collect-max-then-broadcast bootstrap phase before `fan_out_backfill` is
    required (the single-pass collect loop has no seam for a mid-DAG barrier).
  SAL reclamation, if added, would still have to ride on the per-round relay loop
  exactly as in the recommended design.

- **Persist the cursor in `WorkerProcess` and number the rounds** (precedent:
  `pending_scan`): add a `FLAG_BACKFILL_CHUNK` per-round command and a per-round
  exhausted bit, and round-number the accumulator key (`exchange.rs:24`) and the
  `FLAG_EXCHANGE` wire so multiple in-flight rounds can't collide. Most invasive;
  only worth it if the per-round signal proves insufficient.

## The change, by file

- `runtime/worker.rs` — rewrite `handle_backfill` (`:1419`) to the chunked loop;
  set the per-chunk `backfill_pad` bit (drain returned `None`) before
  `evaluate_dag`, and have `do_exchange_wait` (`:1685`) stamp it into the outbound
  `FLAG_EXCHANGE`'s `seek_col_idx` (replacing the literal `0` at `:1701`) while a
  backfill is active; on consuming a relay (`pending_relays`, `:773`) record the
  stop/continue decision into a single per-backfill slot **and** apply any
  `checkpoint-then-continue` inline (advance `expected_epoch` + reset `read_cursor`,
  reusing the FLAG_FLUSH reader path at `next_sal_message`, `:462`); self-terminate
  on `drain == None` when no exchange was issued.
- `runtime/master.rs` — in `collect_acks_and_relay` (`:601`) / the accumulator
  (`reactor/exchange.rs:55`), AND the workers' pad bits per `(view_id, source_id)`
  round (the accumulator must start reading the `seek_col_idx` it ignores today)
  and stamp continue/stop/checkpoint into the relay's `seek_col_idx` (`emit_relay`,
  `:800`; the `0` it passes there today is the field reused) — **no `source_id`
  discriminator**. Reclaim the SAL at the per-round barrier — the master performs
  the `checkpoint_reset` once all workers' next-round reports confirm consumption —
  when `sal_relay_space_ok_raw` (`:694`) is low instead of erroring.
  `fan_out_backfill` (`:852`) is structurally unchanged: its single
  `collect_acks_and_relay` already loops over rounds and finishes when every
  worker sends its terminal ACK — which now happens only after the stop signal
  ends each worker's loop.
- No new SAL flag and no `FLAG_EXCHANGE` / `FLAG_EXCHANGE_RELAY` wire-format
  change — the pad bit (up-leg) and the stop decision (down-leg) each reuse an
  already-free `seek_col_idx`.

## Correctness

- **Chunk-sum equals whole-batch.** Trace integration is additive and incremental
  evaluation is linear, so summing per-chunk results equals the whole-partition
  result — the property `backfill_view` (`ddl.rs:356`) relies on. (GROUP BY
  re-aggregates per chunk and emits more partial rows, but they consolidate to the
  same net groups downstream.)
- **Equal round counts** hold by construction: all workers loop until the master
  stamps stop on the same all-pad round, so the `(view_id, source_id)` accumulator
  gets exactly `num_workers` reports every round, eliminating the early-retirement
  deadlock.
- **Multi-view fan-out per source.** `handle_backfill(source_tid)` drives
  `evaluate_dag(source_tid)`, which evaluates *every* view dependent on that
  source per chunk, so one backfill session can interleave several views' rounds.
  Every round of a given chunk carries the same per-worker pad bit (the chunk's
  drain outcome), so the master's AND is consistent whichever view's round it
  lands on; the worker reads one stop slot per chunk, not per view, so all
  workers stop in lockstep regardless of fan-out. (A `checkpoint-then-continue` is
  applied per relay inside `do_exchange_wait` rather than from the slot, so it is
  unaffected by which view a round belongs to.)
- **Empty padding rounds** emit valid empty exchange payloads (`do_exchange_wait`
  sends a 0-row batch unconditionally, `worker.rs:1698`–`1704`) — the barrier
  participates without contributing rows.
- **A worker missing the source must still pad, not early-return.** Today's
  `handle_backfill` returns immediately on `!has_id(source_tid)`; the chunked loop
  must instead open no cursor (`handle = None`) and pad every round, because an
  exchange view's accumulator still needs this worker's `num_workers`-th report per
  round — an early return would leave the round stuck at `count < num_workers` and
  wedge the same way the unequal-partition deadlock does. (For a non-exchange
  source it harmlessly pads one round and self-terminates.)
- **Boot-time snapshot stability.** Backfill runs before the reactor with no
  concurrent writers to the source, so the partition is static; a held cursor
  stays valid (`open_store_cursor` contract, `store.rs:1807` — the handle owns its
  sources via `Rc`; the contract requires only that the scanned relation not be
  written mid-loop, which boot satisfies).
- **SAL reclamation safety** — the per-round reset is taken only when all prior
  relays are consumed (proven by next-round reports) and all workers have advanced
  their epoch inside `do_exchange_wait` (obligations above). Peak resident SAL is
  the inter-reset high-water (`O(mmap)`, a constant ≈ 896 MiB–1 GiB), not the
  cumulative-over-`N` volume; the per-round transient is `O(num_workers × chunk)`.
- **Completion** is still one terminal ACK per worker after the loop;
  `collect_acks_and_relay`'s done-detection (`collected[w] = true`,
  `master.rs:639`) is reached only once the stop signal has ended every worker's
  loop, so no worker is retired early.

## Testing

- **Skewed partitions** (all rows hash to one worker): backfill completes, result
  matches a single-node reference, no deadlock. Shrink `ddl_scan_chunk_rows`
  (e.g. to 3) to force many rounds and unequal local counts across workers.
- **Empty partition on some workers** (and the all-empty CREATE-over-empty case):
  short workers pad to the stop round; downstream view consistent.
- **Range-join broadcast backfill** (`GNITZ_WORKERS=4`) over a table whose
  *cumulative* relay volume exceeds the 1 GiB mmap (a per-round relay still well
  under the 128 MiB reserve): result equals the cross-filter reference, **and peak
  resident SAL stays bounded below the mmap** (does not trip the guard) regardless
  of `N`, via per-round reclamation — i.e. the epoch advances more than once over
  the backfill. Also assert a backfill in the 896 MiB–1 GiB cumulative band that
  succeeds today still succeeds (no
  reserve-threshold regression).
- **GROUP BY / reduce / DISTINCT backfill** — exchange exactly once per chunk at
  `source_id == 0`; assert backfill completes and the result matches a single-node
  reference. Two sub-cases the rejected payload-emptiness mechanism gets wrong and
  the pad bit must get right:
  - **Filtered (`WHERE`) GROUP BY** where a contiguous chunk-range fails the
    predicate on *every* worker at once: backfill must **not** stop early — assert
    the full result, not a truncated one. Shrink `ddl_scan_chunk_rows` so an
    all-filtered round is easy to force.
  - Plain GROUP BY with shrunk `ddl_scan_chunk_rows`: completes, no hang (a
    `source_id` discriminator that excluded `source_id == 0` would deadlock here).
- **Set-op and non-co-partitioned join** backfill under chunking — verify
  equal-round padding and correct results. Include a self-referential set-op
  (`a UNION a`), which issues two same-`(view_id, source_id)` rounds per chunk
  carrying the same pad bit.
- **Co-partitioned / `skip_exchange` / non-exchange source** that feeds no
  exchange view: chunks with no coordination (no barrier to desync). The worker
  self-terminates on local drain exhaustion (no relay, no signal) — assert it
  neither hangs waiting for a stop nor under-materializes, and the result is
  correct.
- **Large-table RAM regression**: peak worker RSS bounded near `O(chunk)`, not
  `O(N)`.

## Out of scope

- Live (post-boot) re-backfill / incremental view rematerialization — backfill
  here is the boot-time CREATE-over-populated-tables path only.
- Range-partitioned exchange to shrink the broadcast `num_workers ×` relay factor
  — see `wide-pk-incremental-views.md` §1 (pure-range distribution beyond
  broadcast). (Reduces the SAL constant; orthogonal to the per-round reclamation
  here.)
