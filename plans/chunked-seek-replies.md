# Chunk large index-seek replies instead of asserting on 256 MiB

## Problem

Worker replies for `SalMessageKind::SeekByIndex` (and the planned
`SeekByIndexRange`, and `Gather`) are sent as a **single** W2M frame via
`WorkerProcess::send_response`. `send_response` calls `w2m_writer.send_encoded`,
which hard-asserts the frame fits one ring message:

```rust
// runtime/w2m.rs
assert!((sz as u64) <= w2m_ring::MAX_W2M_MSG, …);   // MAX_W2M_MSG = 1 << 28 = 256 MiB
```

A single worker whose partition matches more than ~256 MiB of base rows for one
seek therefore **panics** (debug) / aborts the worker. The master side compounds
the peak: `fan_out_seek_by_index_collect_async` (`runtime/master.rs:943`) reads
exactly one slot per worker and accumulates all workers' batches into one merged
`Batch`, so master heap peaks at the sum across workers.

Point seeks on a high-cardinality non-unique index already have this ceiling; the
ordered range scan (landed in `0b2af7c`) makes it easy to hit —
`WHERE x > 0` can match most of a table.

The `Scan` path does **not** have this problem: it streams. `send_scan_response`
(`worker.rs:1033`) splits a wire-safe batch across multiple `pending_scan` frames,
`emit_pending_scan_chunk` emits each with `FLAG_CONTINUATION`
(`FLAG_SCAN_LAST` on the terminal chunk), and the master drains the per-worker
frame train (`parse_train_header`, `master.rs:2710`) until `FLAG_SCAN_LAST`.

## Goal

Route oversized index-seek replies through the same chunked train the `Scan` path
uses, so a large seek/range result streams in bounded frames instead of tripping
the `MAX_W2M_MSG` assert.

## Design

### Worker: stream the seek result like a scan

`send_response` is used by `SeekByIndex`, `Seek`, `Gather`, and (planned)
`SeekByIndexRange`. Generalize the reply so that when the encoded result exceeds a
single frame it is handed to the existing chunker.

- Reuse `pending_scan`: after the seek produces its result `Batch`, if the batch's
  single-frame `wire_size` exceeds `MAX_W2M_MSG` (or unconditionally, for
  uniformity), enqueue it as a `PendingScan` keyed by the seek's `request_id` and
  let `emit_pending_scan_chunk` stream it. The first frame carries the schema
  (when the client cache is cold); every frame sets `FLAG_CONTINUATION`; the
  terminal frame clears it / sets `FLAG_SCAN_LAST`. This is exactly what
  `send_scan_response` already does — factor its body into a shared
  `stream_batch_response(target_id, batch, schema, request_id, client_id,
  client_version)` and call it from both the `Scan` handler and the seek handlers.
- **Non-wire-safe (STRING-column) schemas** cannot be linearly chunked by the
  current interpolation in `emit_pending_scan_chunk` (per-row wire size is not
  constant). `send_scan_response` already handles this by sending a single frame
  and returning an error if it exceeds `MAX_W2M_MSG`. Keep that behavior for seeks:
  a STRING-heavy seek result over 256 MiB returns a clean `STATUS_ERROR`
  ("result too large; add a tighter predicate or LIMIT") instead of asserting.
  A true streaming chunker for variable-width schemas is a separate effort.

### Master: drain a per-worker train in the collect path

`fan_out_seek_by_index_collect_async` and its range sibling must drain each
worker's frame **train** to its terminal frame and merge every non-empty
continuation batch, rather than reading one slot. Reuse the train-drain helper
that backs `fan_out_scan` (the `parse_train_header` / `FLAG_SCAN_LAST` loop at
`master.rs:2710`): for each worker, loop receiving slots until a frame without
`FLAG_CONTINUATION` (or with `FLAG_SCAN_LAST`), appending each batch into the
merged accumulator. Hold the `ScanLease` across the whole drain (as `fan_out_scan`
does) so a cancelled drain discards rather than parks late frames.

This keeps the per-frame W2M footprint bounded; the master's merged `Batch` is
still materialized whole (the SELECT result is an unordered set today). Bounding
the master-side peak — streaming the merged result back to the client frame by
frame — is a further follow-on that depends on the SELECT result becoming a
stream rather than a single batch.

## Migration order

1. Factor `stream_batch_response` out of `send_scan_response`; switch the
   `SeekByIndex`/`Seek`/`Gather` worker arms to it. No master change yet — single
   worker reply still fits one frame in the common case, so the train is length 1.
2. Switch `fan_out_seek_by_index_collect_async` (and the range sibling, once it
   lands) to the train-drain merge. Now multi-frame seek replies work end to end.
3. Apply to `SeekByIndexRange` when that handler is added.

## Tests

- **Rust/E2E oversized seek.** Seed a non-unique index whose single value matches
  enough rows that one worker's reply exceeds `MAX_W2M_MSG`; assert the seek
  returns the full set (multi-frame train) with no panic. A reduced
  `MAX_W2M_MSG` (test-only override) keeps the fixture small.
- **Retraction across the train.** Insert range rows, retract some, assert the
  streamed merged result reflects net positive weight (mandatory retraction test
  for any index access path).
- **STRING result over the cap** returns the clean `STATUS_ERROR`, not an assert.
- **Multi-worker merge** (`GNITZ_WORKERS=4`): a value/range scattered across
  workers returns the union of all workers' trains and equals a
  full-scan-and-filter reference.

## Scope / non-goals

- **Bounding the worker's peak result `Batch`** — this plan chunks an *already
  materialized* result `Batch` at reply time, so a worker whose partition matches
  most of a huge table still builds the whole result in RAM before the first frame
  goes out. Bounding the worker peak means producing the result *incrementally
  during the walk* — emit a chunk every `DDL_SCAN_CHUNK_ROWS` (65,536) resolved
  rows and clear the accumulator — which changes `seek_by_index_range` /
  `seek_by_index` from "return one `Batch`" to "drive a chunk-emit callback".
  Separate from the reply-framing fix here; do it when worker-side OOM on a single
  partition (not just the W2M frame cap) becomes the binding constraint.
- **Streaming the merged result to the client** (master-side peak) — separate;
  needs the SELECT result to be a stream. The eventual shape is a
  `fan_out_seek_by_index_range_async` that pipes each worker W2M slot straight to
  the client fd (`reactor.send_slot(fd, slot)`, O(1) master memory) with a
  client-side continuation loop in `GnitzClient::seek_by_index_range` accumulating
  the chunk train — exactly how `fan_out_scan_async` already streams `Scan`.
- **Variable-width (STRING) streaming chunker** — separate; the cap stays a clean
  error for those schemas.
