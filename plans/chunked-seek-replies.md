# Chunk large index-seek replies instead of asserting on 256 MiB

## Problem

Worker replies for `SalMessageKind::SeekByIndex`, `SeekByIndexRange`, and
`Gather` are sent as a **single** W2M frame via `WorkerProcess::send_response`
(`worker.rs:1034`). `send_response` calls `w2m_writer.send_encoded`, which
hard-asserts the frame fits one ring message:

```rust
// runtime/w2m.rs:31
assert!((sz as u64) <= w2m_ring::MAX_W2M_MSG, …);   // MAX_W2M_MSG = 1 << 28 = 256 MiB
```

A single worker whose partition matches more than ~256 MiB of base rows for one
seek therefore **panics** (debug) / aborts the worker. Point seeks on a
high-cardinality non-unique index already have this ceiling; the ordered range
scan (`0b2af7c`) makes it easy to hit — `WHERE x > 0` can match most of a table.

The `Scan` path does **not** have this problem: it streams. `send_scan_response`
(`worker.rs:1078`) splits a wire-safe batch across multiple `pending_scan`
frames, `emit_pending_scan_chunk` (`worker.rs:407`) emits each with
`FLAG_CONTINUATION` (`FLAG_SCAN_LAST` on the terminal chunk), and the master
drains the per-worker frame train (`parse_train_header` / `drain_index_scan`,
`master.rs:2743` / `master.rs:2786`) until the terminal frame.

Three further defects live on the same paths and are fixed here:

1. **`pending_scan` overwrite — live bug.** `pending_scan: Option<PendingScan>`
   (`worker.rs:192`) holds at most one chunked train. Client connections are
   independent reactor tasks (`executor.rs:268`) and `handle_scan` holds only
   the catalog *read* lock, so two connections can run two large scans
   concurrently. The second `send_scan_response` overwrites the first train
   (`worker.rs:1173`, `self.pending_scan = Some(…)`); the master task draining
   the first train then awaits a continuation frame that is never sent and
   hangs forever (no timeout), its `ScanLease` held.
2. **Merged collect reply exceeds the client frame cap — live bug.**
   `fan_out_index_collect_common` (`master.rs:991`) merges per-worker batches
   (each ≤ 256 MiB) with no total bound, and `send_ok_response`
   (`executor.rs:1405`) encodes the merge as a single TCP frame. The client
   rejects any frame over `MAX_FRAME_PAYLOAD_CLIENT` (256 MiB,
   `gnitz-wire/src/handshake.rs:15`): four workers × 100 MiB each is a protocol
   failure today, with no oversized partition required.
3. **`execute_gather_async` is single-frame-only.** It allocates plain request
   ids and `await_reply`s exactly one frame per worker (`master.rs:1295-1316`).
   If gather replies chunk, the continuation frames are parked forever in
   `parked_replies` (no waiter exists) and the master proceeds with silently
   partial data — FK/UPDATE validation would return wrong verdicts. Chunking
   the worker's `Gather` arm therefore requires this collect to be upgraded in
   the same step.

## Goal

Route oversized index-seek and gather replies through the same chunked train
the `Scan` path uses; bound the master-side merged reply at the client frame
cap with a clean error; fix the worker's single-slot pending-train state.

## Design

### Worker: FIFO queue of pending reply streams

Replace the scalar with a queue:

```rust
// worker.rs
pending_streams: std::collections::VecDeque<PendingScan>,
```

- `run()` skips the SAL wait while `!self.pending_streams.is_empty()`
  (today: `pending_scan.is_none()`, `worker.rs:353`).
- `drain_sal()` emits **one chunk of the front stream** per pass (today's
  pacing; `send_encoded` provides backpressure), then drains SAL messages.
  `emit_pending_scan_chunk` operates on `pending_streams.front_mut()` and
  `pop_front()`s when the terminal chunk is sent.
- `send_scan_response` / `stream_batch_response` `push_back` new trains.

Trains are strictly FIFO — a train fully drains before the next starts. Do
**not** interleave streams round-robin: the master drains one request's train
at a time, so an interleaved second train's frames would sit parked in
`scan_parked` holding un-released ring slots; `consume_cursor` (released in
ring order, `w2m.rs:142`) could then never pass them, the ring fills, the
worker blocks in `send_encoded`, and the cluster deadlocks. FIFO is
deadlock-free: every fan-out writes its group to all workers under
`sal_writer_excl`, so all worker queues share one global request order; each
master task drains workers in ascending index order; the earliest-ordered
awaited train always has its frames at the front of some worker's queue with a
live consumer.

Single-frame replies for *other* requests still go out immediately between
chunks (distinct ring-prefix request ids; the reactor routes per id), as today.

This change alone fixes live bug 1.

### Worker: chunk-aware seek/gather replies

`send_scan_response` cannot simply be reused for seeks: its frames are
forwarded **verbatim to the client** (scan protocol — every frame carries
`FLAG_CONTINUATION`; the client loop stops at the master's flag-free terminal
frame), and it serves the table's cached schema wire block unconditionally.
Seek/gather replies have the opposite constraints:

- `Seek` and unicast `SeekByIndex` slots are forwarded verbatim to the client
  (`executor.rs:828`, `executor.rs:904`), so a single-frame reply must stay
  **byte-identical** to today's `send_response` shape (no train flags,
  `seek_pk` + `request_id` echoed in the payload).
- `Gather` replies carry a **projected** schema. The table-keyed cached block
  (`get_cached_schema_wire_block(tid_key)`) must not serve them — the master
  would decode projected rows with the base table's stride. `send_response`
  already has the mismatch arm (`worker.rs:1046-1049`, gated on
  `schema_matches_table`, `worker.rs:1028`): build a one-off
  `ipc::build_schema_wire_block(s, &[], target_id as u32)` and never cache it.

Factor that mismatch-aware block selection out of `send_response` /
`send_scan_response` into one helper used by all reply paths:

```rust
/// Reply schema wire block: a one-off (never cached) block for a projected or
/// synthetic schema, the table's cached block otherwise. Returns the block and
/// the table's schema version.
fn reply_schema_block(&mut self, tid_key: i64, schema: Option<&SchemaDescriptor>)
    -> (Option<Rc<Vec<u8>>>, u16)
```

Then add the seek/gather entry point:

```rust
/// Reply with `result`, chunking through `pending_streams` when it exceeds one
/// W2M frame. Single-frame replies are byte-identical to `send_response` (no
/// train flags), so slot-forwarding consumers see no wire change; only a real
/// multi-frame train carries FLAG_CONTINUATION / FLAG_SCAN_LAST. The schema is
/// always embedded on the train's first frame (internal consumers decode with
/// it; there is no client cache on these paths).
fn stream_batch_response(
    &mut self,
    target_id: u64,
    result: Option<Rc<Batch>>,
    schema: Option<&SchemaDescriptor>,
    request_id: u64,
    client_id: u64,
    seek_pk: u128,
) -> Option<String> {
    let Some(batch) = result.filter(|b| b.count > 0) else {
        self.send_response(target_id, None, schema, request_id, client_id, seek_pk);
        return None;
    };
    let tid_key = target_id as i64;
    let is_wire_safe = schema.map(schema_wire_safe).unwrap_or(true);
    let (prebuilt_rc, server_version) = self.reply_schema_block(tid_key, schema);
    let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
    let sz = if is_wire_safe {
        ipc::wire_size_range(STATUS_OK, &[], schema, None, &batch, batch.count, prebuilt)
    } else {
        ipc::wire_size(STATUS_OK, &[], schema, None, Some(&*batch), prebuilt, &[])
    };
    if sz <= w2m_ring::MAX_W2M_MSG as usize {
        self.send_response(target_id, Some(&batch), schema, request_id, client_id, seek_pk);
        return None;
    }
    if !is_wire_safe {
        return Some(format!(
            "result wire_size={sz} > MAX_W2M_MSG={}; STRING-column chunking not \
             yet implemented — add a tighter predicate or LIMIT",
            w2m_ring::MAX_W2M_MSG));
    }
    self.pending_streams.push_back(PendingScan {
        batch, next_row: 0, request_id, client_id, target_id,
        prebuilt_schema: prebuilt_rc, server_version,
    });
    None
}
```

`PendingScan` and `emit_pending_scan_chunk` are unchanged (chunk framing —
`FLAG_CONTINUATION` on every chunk, `FLAG_SCAN_LAST` on the terminal one — is
exactly what the master's train drain consumes).

Arm disposition:

- `SeekByIndex`, `SeekByIndexRange`, `Gather` → `stream_batch_response`
  (results are `Option<Batch>` / `Batch`; wrap in `Rc`). An `Err` return maps
  to `DispatchResult::Error` exactly like `send_scan_response` in the `Scan`
  arm today.
- `Seek`, `HasPk` → stay on `send_response`. Both are bounded (one PK family /
  one row) and their slots are forwarded or inspected as single frames; a
  single pathological row over 256 MiB is the variable-width non-goal.
- `Scan` → `send_scan_response`, unchanged apart from the queue and
  `reply_schema_block`.

### Master: terminal-frame contract

A single-frame `send_response` reply carries no train flags, so the drain must
treat "no `FLAG_CONTINUATION`" as terminal (a length-1 train). In
`parse_train_header` (`master.rs:2756`):

```rust
let has_more = ctrl.status == 0
    && (ctrl.flags & FLAG_SCAN_LAST == 0)
    && (ctrl.flags & FLAG_CONTINUATION != 0);
```

Compatible with every existing train producer: worker scan frames and unique
pre-flight frames always set `FLAG_CONTINUATION` on non-terminal frames.

### Master: `drain_index_scan` — early return, schema guard, fallible sink

Rework `drain_index_scan` (`master.rs:2786`):

- **Return on the first error instead of draining every remaining train.** The
  "failing to drain a still-streaming worker would wedge it" claim in the
  current doc comment is obsolete: all callers hold the `ScanLease` from
  `dispatch_scan_fanout`, and when the early `Err` unwinds it, the lease drop
  (`reactor/mod.rs:1761`) frees every parked slot and `route_scan_slot`
  (`reactor/mod.rs:1309`) discards all later frames at the ring boundary,
  advancing `consume_cursor` — the worker cannot wedge. Decoding and parsing
  gigabytes of doomed frames is pure waste. Rewrite the comment accordingly.
- **Guard the schema.** Validate each train's first schema-bearing frame
  against the caller's expected descriptor with `validate_schema_match`
  (hoist from `executor.rs:1464` to `schema.rs`, `pub(crate)`). Worker reply
  schemas can lag the master's during DDL races (`run_tick` releases the
  catalog read lock before awaiting ACKs, `executor.rs:466`; a worker inside
  `do_exchange_wait` defers `DdlSync` but serves seeks inline,
  `worker.rs:624`; seek handlers take no catalog lock at all).
  `Batch::append_batch` / `append_mem_batch_range` do not validate shape
  (documented precondition only, `storage/batch.rs:1006`), so an unguarded
  mismatch is memory-unsafe garbage handed to the client under the master's
  schema block.
- **Fallible sink with frame size.** `on_batch` becomes
  `FnMut(&MemBatch<'_>, usize) -> Result<(), String>`; the `usize` is
  `slot.bytes().len()`. An `Err` aborts the drain (lease drop discards the
  rest). This is what the reply cap below hangs off.

```rust
async fn drain_index_scan(
    slots: Vec<W2mSlot>,
    req_ids: &[u64; crate::runtime::sal::MAX_WORKERS],
    reactor: &crate::runtime::reactor::Reactor,
    what: &str,
    expected: Option<&SchemaDescriptor>,
    mut on_batch: impl FnMut(&crate::storage::MemBatch<'_>, usize) -> Result<(), String>,
) -> Result<(), String> {
    for (w, mut slot) in slots.into_iter().enumerate() {
        let mut saved_schema: Option<(SchemaDescriptor, u16)> = None;
        loop {
            let mut fault = None;
            let (ctrl, has_more) = parse_train_header(&slot, w, what, &mut fault);
            // Fault or corrupt frame: return now. The caller's ScanLease drops
            // on unwind; route_scan_slot then discards every late frame at the
            // ring boundary, so no manual drain is needed to unwedge workers.
            if let Some(e) = fault { return Err(e); }
            let ctrl = ctrl.expect("parse_train_header: healthy frame has ctrl");
            let server_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
            let frame_len = slot.bytes().len();
            let schema_hint = saved_schema.as_ref()
                .map(|(s, v)| SchemaWithVersion { descriptor: s, version: *v });
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(
                slot.bytes(), ctrl.block_size, ctrl, schema_hint,
            ).map_err(|e| scan_decode_err(w, e))?;
            if saved_schema.is_none() {
                if let Some(ref s) = zc.schema {
                    if let Some(exp) = expected {
                        crate::schema::validate_schema_match(s, exp)
                            .map_err(|e| format!("worker {w}: {what}: {e}"))?;
                    }
                    saved_schema = Some((*s, server_version));
                }
            }
            if let Some(ref mb) = zc.data_batch {
                if mb.count > 0 { on_batch(mb, frame_len)?; }
            }
            drop(zc); // borrows slot
            drop(slot);
            if !has_more { break; }
            slot = reactor.await_scan_slot(req_ids[w] as u32).await;
        }
    }
    Ok(())
}
```

Caller adaptations: the unique-filter warmup (`master.rs:2365`) passes the
table schema it already looks up (`master.rs:2328`, currently `_schema`) as
`expected` and wraps its closure body in `Ok(())`. The drain unit tests at
`master.rs:3650+` keep asserting single-poll completion on a fault frame; the
deferred-error assertions become immediate-`Err` assertions.

### Master: collect paths through the drain

`fan_out_index_collect_common` (`master.rs:991`) switches from one-slot-per-
worker to the train drain, gaining the multi-frame merge, the schema guard, a
zero-copy append, and the client-cap from live bug 2:

```rust
async fn fan_out_index_collect_common(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex<()>>,
    target_id: i64, sal_flag: u32, seek_pk: u128,
    seek_col_idx: u64, seek_pk_extra: &[u8], op: &str,
) -> Result<Option<Batch>, String> {
    let expected = unsafe {
        (*(*disp_ptr).catalog).get_schema_desc(target_id)
            .ok_or_else(|| format!("{op}: table {target_id} not found"))?
    };
    let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
        let (schema, col_names) = disp.get_schema_and_names(target_id);
        let lsn = disp.next_lsn();
        disp.write_group_with_req_ids(
            target_id, lsn, sal_flag, 0, &[], &schema, &col_names,
            seek_pk, seek_col_idx, req_ids, -1, 0, None, seek_pk_extra,
        )
    }).await?;

    let mut acc: Option<Batch> = None;
    let mut merged_bytes = 0usize;
    drain_index_scan(slots, &req_ids, reactor, op, Some(&expected), |mb, frame_len| {
        // Σ frame bytes ≥ the merged single-frame encode size (every frame
        // re-counts its header and the first one the schema block), so this
        // cap can never let a reply through that the client would reject
        // (MAX_FRAME_PAYLOAD_CLIENT) — and it bounds the master's merge heap.
        merged_bytes += frame_len;
        if merged_bytes > gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT {
            return Err(format!(
                "{op}: result exceeds the {} MiB reply cap; add a tighter \
                 predicate or LIMIT",
                gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT >> 20));
        }
        let a = acc.get_or_insert_with(|| Batch::with_schema(expected, mb.count));
        a.append_mem_batch_range(mb, 0, mb.count, None);
        Ok(())
    }).await?;
    Ok(acc.filter(|a| a.count > 0))
}
```

`fan_out_seek_by_index_collect_async` / `fan_out_seek_by_index_range_collect_async`
need no signature change. The unicast paths (`single_worker_async`,
`master.rs:3085`, and `fan_out_seek_by_index_async`) stay single-frame: they
serve unique-index lookups (≤ 1 row) and forward slots verbatim.

### Master: `execute_gather_async` through the drain

Switch `execute_gather_async` (`master.rs:1263`) from plain
`alloc_request_id` + `await_reply` to `dispatch_scan_fanout` (scan ids +
`ScanLease`; the existing `scatter_wire_group` call moves into the submit
closure, which receives the per-worker req-id slice) and decode rows inside a
`drain_index_scan` sink — the same per-row loop as today, reading from the
zero-copy `MemBatch` instead of a `DecodedWire` batch. Pass
`Some(&project_schema(&parent_schema, project))` as `expected` so a projected
reply with the wrong shape errors instead of mis-decoding (`project_schema`,
`catalog/store.rs:1823`, is the exact constructor the worker uses for the
reply schema; hoist it to `pub(crate)`).

### Master: `fan_out_scan_async` early return

Apply the same lease-backed simplification to `fan_out_scan_async`
(`master.rs:1085`): on a worker fault, decode error, or client disconnect,
return immediately instead of draining the remaining trains into
`deferred_err` / dropped slots — the lease drop discards them at the ring
boundary. The client sees data frames followed by a `STATUS_ERROR` frame,
which `recv_scan_response` already handles mid-stream. Rewrite the stale
wedge-rationale comment.

## Migration order

Each step keeps the suite green; masters always understand both single-frame
replies (length-1 trains) and chunked trains before any worker produces them.

1. **Master drain rework**: `parse_train_header` terminal contract,
   `drain_index_scan` new signature + early return + schema guard, hoist
   `validate_schema_match`, adapt the warmup caller and drain unit tests.
2. **Master collect paths**: `fan_out_index_collect_common` and
   `execute_gather_async` onto the drain (replies are still single-frame;
   behavior change is only the new cap and guard). `fan_out_scan_async` early
   return.
3. **Worker FIFO queue**: `pending_streams` (standalone fix for live bug 1).
4. **Worker reply helper**: `reply_schema_block`, `stream_batch_response`,
   switch the `SeekByIndex` / `SeekByIndexRange` / `Gather` arms. End-to-end
   chunking is now live.

## Tests

- **Worker unit** (existing direct-`PendingScan` pattern, `worker.rs:2360+`):
  - FIFO: queue two trains, drive `drain_sal` passes; train A's frames
    (terminal `FLAG_SCAN_LAST`) fully precede train B's; B's first chunk
    carries B's schema block.
  - `stream_batch_response`: a fitting result produces a frame byte-identical
    to `send_response` (flags, `seek_pk`/`request_id` echo); an oversized
    wire-safe result enqueues a train; an oversized STRING result returns the
    clean error.
  - Projected (gather) schema: the emitted train's schema block is a one-off
    projected block and the table's cached block is untouched.
- **Master unit** (existing synthetic-train pattern, `master.rs:3650+`):
  multi-frame merge across workers; fault frame → immediate `Err` in a single
  poll; first-frame schema mismatch → `Err`; sink `Err` (cap) aborts the
  drain.
- **E2E** (`make e2e`, `GNITZ_WORKERS=4`):
  - Non-unique value and range scattered across workers equal a
    scan-and-filter reference (multi-worker train merge).
  - Retraction across the result: insert, retract some, streamed merged result
    reflects net weight (mandatory for any index access path).
  - Merged reply just over `MAX_FRAME_PAYLOAD_CLIENT` returns the clean cap
    error, not a client-side frame rejection.
- **Oversized e2e** (slow-marked): one worker's reply > `MAX_W2M_MSG` returns
  the full set with no panic. If a test-size override for `MAX_W2M_MSG` is
  introduced instead of a real 256 MiB fixture, `W2M_REGION_SIZE`
  (`w2m_ring.rs:74`) must shrink with it: the region must stay within
  `[2·cap + W2M_HEADER_SIZE + 16, 64·cap]`, otherwise more than
  `W2M_MAX_IN_FLIGHT = 64` small frames fit the ring, the master parks them
  all while draining another worker, and `InFlightState` overflows
  (`w2m.rs:105`; the 65th `take` silently overwrites in release, the release
  assert / `route_scan_slot` debug_assert fires first in debug). Override both
  together (e.g. region = 4·cap) or use the real-size fixture.

## Scope / non-goals

- **Bounding the worker's peak result `Batch`** — this plan chunks an *already
  materialized* result at reply time. Producing the result incrementally
  during the index walk (emit a chunk per `DDL_SCAN_CHUNK_ROWS` resolved rows)
  changes `seek_by_index[_range]` into a chunk-emit callback; do it when
  worker-side OOM (not the frame cap) becomes binding. It composes with the
  per-chunk PK sort in `plans/index-range-sorted-pk-resolution.md`.
- **Streaming the merged result to the client** — needs the SELECT result to
  become a stream and a client-side continuation loop
  (`Connection::seek_by_index_range` is a single round-trip,
  `gnitz-core/src/connection.rs:191`). Until then the reply cap above turns
  the overflow into a clean error. The eventual shape pipes each worker slot
  to the client fd like `fan_out_scan_async`.
- **Variable-width (STRING) streaming chunker** — the cap stays a clean error
  for non-wire-safe schemas.
- **Sorted source-PK resolution for range scans** — separate optimization,
  `plans/index-range-sorted-pk-resolution.md`.
