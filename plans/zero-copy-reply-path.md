# Zero-Copy Worker Reply Path

## Problem

Profiling (commit `997831e`, w4_c1) shows three overlapping hotspots that all
trace to the same architectural flaw: the worker-to-client data path copies
bytes **three times** before a SEEK result reaches the TCP socket.

```
perf top (no-children, cycles:u)
─────────────────────────────────────────────────────────────────────
19.81%  __memmove_avx512_unaligned_erms     (libc)
         └─ 9.47%  encode_response_buffer → encode_wire_into_impl
         └─ 1.67%  append_mem_batch_range  → drain_w2m_for_worker
 7.25%  __memset_avx512_unaligned_erms      (libc, zeroing new Batches)
 5.81%  Reactor::drain_w2m_for_worker       (loop + branch overhead)
 4.37%  core::hash::sip::Hasher::write      (SipHash on u64 keys)
         └─ 2.86%  HashMap::insert → send_buf_inner
         └─ 0.51%  HashMap::insert → route_reply → drain_w2m_for_worker
```

### The three copies for a SEEK

```
Worker: result Batch ──encode_wire_into_ipc──► W2M ring
                                               (correct request_id,
                                                client_id=0, seek_pk=0)

Master drain (ingest_from_slot):
  ring bytes ──decode_wire_ipc_zero_copy──► MemBatch (borrow)
  MemBatch   ──append_mem_batch_range───► owned Batch (heap)   ← copy 1
  drop slot, route_reply → parked_replies HashMap insert (SipHash)

single_worker_async gets DecodedWire via await_reply:
  owned Batch ──extract_single_batch::append_batch──► new Batch  ← copy 2
  (the with_schema + append_batch inside extract_single_batch is
   a second full copy of the already-owned Batch — entirely wasteful)

handle_seek → send_ok_response → encode_response_buffer:
  Batch ──encode_wire_into──► Vec<u8> ──io_uring SEND──► socket  ← copy 3
```

The scan path already does this correctly — it parks the raw `W2mSlot` and
forwards via `send_slot`, resulting in **zero copies** between ring and socket.
The regular (SEEK / SEEK_BY_INDEX) data reply path should mirror it exactly.

### Separate problem: SipHash on u64 keys (4.37%)

`reply_wakers`, `parked_replies`, and `send_fd_for_id` all declare
`HashMap<u64, _>` with no hasher override, so the standard library's
SipHash-1-3 is used. For small-integer keys, SipHash is 5–10× slower than
a trivial multiply-shift hash (`FxHashMap`). These maps live on the reactor's
single-threaded hot path; there is no adversarial input, so SipHash's
DoS-resistance buys nothing.

---

## Root-cause summary

| Symptom | Cause | Fix |
|---|---|---|
| 9.47% memmove in encode_response_buffer | Batch re-encoded to wire on master even though ring already holds the result in wire format | Park W2mSlot, forward via send_slot |
| 1.67% memmove in append_mem_batch_range | Ring bytes decoded into owned Batch unnecessarily | Same — eliminate ingest_from_slot decode for SEEK path |
| Copy 2 in extract_single_batch | append_batch duplicates an already-owned Batch | Eliminated as part of the slot path; the function goes away |
| 4.37% SipHash | std::HashMap on u64 keys | FxHashMap |
| 5.81% drain_w2m_for_worker (loop) | Minor: branch + atomic per empty ring | Secondary; addressed by reducing work per slot |

---

## Solution

Two independent changes, each correct and testable on its own.

### Change A — FxHashMap for all integer-keyed maps

**Files**: `crates/gnitz-engine/src/runtime/reactor/mod.rs`, `crates/gnitz-engine/src/runtime/executor.rs`

Replace every `HashMap<_, _>` / `HashSet<_>` field in `ReactorShared` and `Shared` with `FxHashMap`/`FxHashSet`:

**`ReactorShared` fields** (`runtime/reactor/mod.rs`):

| Field | Current type | New type |
|---|---|---|
| `tasks` | `HashMap<usize, Task>` | `FxHashMap<usize, Task>` |
| `task_wakers` | `HashMap<usize, Waker>` | `FxHashMap<usize, Waker>` |
| `reply_wakers` | `HashMap<u64, Waker>` | `FxHashMap<u64, Waker>` |
| `parked_replies` | `HashMap<u64, DecodedWire>` | `FxHashMap<u64, DecodedWire>` |
| `fsync_wakers` | `HashMap<u64, Waker>` | `FxHashMap<u64, Waker>` |
| `parked_fsync_results` | `HashMap<u64, i32>` | `FxHashMap<u64, i32>` |
| `timer_wakers` | `HashMap<u64, (Waker, Rc<Cell<bool>>)>` | `FxHashMap<u64, (Waker, Rc<Cell<bool>>)>` |
| `conns` | `HashMap<i32, Box<io::Conn>>` | `FxHashMap<i32, Box<io::Conn>>` |
| `recv_waiters` | `HashMap<i32, Waker>` | `FxHashMap<i32, Waker>` |
| `pending_recv` | `HashMap<i32, VecDeque<_>>` | `FxHashMap<i32, VecDeque<_>>` |
| `recv_closed` | `HashMap<i32, bool>` | `FxHashMap<i32, bool>` |
| `send_wakers` | `HashMap<u64, Waker>` | `FxHashMap<u64, Waker>` |
| `send_fd_for_id` | `HashMap<u64, i32>` | `FxHashMap<u64, i32>` |
| `send_buffers_in_flight` | `HashMap<u64, SendAlive>` | `FxHashMap<u64, SendAlive>` |
| `parked_send_results` | `HashMap<u64, i32>` | `FxHashMap<u64, i32>` |
| `scan_wakers` | `HashMap<u32, Waker>` | `FxHashMap<u32, Waker>` |
| `scan_parked` | `HashMap<u32, W2mSlot>` | `FxHashMap<u32, W2mSlot>` |
| `closing_fds` | `HashSet<i32>` | `FxHashSet<i32>` |

**`Shared` fields** (`runtime/executor.rs`):

| Field | Current type | New type |
|---|---|---|
| `tick_rows` | `HashMap<i64, usize>` | `FxHashMap<i64, usize>` |
| `table_locks` | `HashMap<i64, Rc<AsyncMutex<()>>>` | `FxHashMap<i64, Rc<AsyncMutex<()>>>` |

`FxHashMap`/`FxHashSet` are already a dependency (used in `MasterDispatcher`).
No logic changes. Initializers without explicit capacity become `FxHashMap::default()`.
Where an existing initializer uses `HashMap::with_capacity(n)`, preserve the hint:
`FxHashMap::with_capacity_and_hasher(n, Default::default())`.

Expected gain: ~4.37% CPU, zero risk.

---

### Change B — Unified zero-copy slot forwarding for data replies

This makes the non-exchange data reply path identical to the scan path: the
`W2mSlot` is parked by `req_id`, and the task forwards it to the socket with
`send_slot` without any decode or re-encode.

#### B1 — Worker: encode client_id and seek_pk correctly

**File**: `crates/gnitz-engine/src/runtime/worker.rs`

`send_response` currently hard-codes `client_id=0` and `seek_pk=0u128` in its
`encode_wire_into_ipc` call. Both values are available in `dispatch_inner`
(extracted from the inbound request at lines ~574–576). Thread them through:

```rust
// Before
fn send_response(&mut self,
    target_id: u64, result: Option<&Batch>,
    schema: Option<&SchemaDescriptor>, request_id: u64)

// After
fn send_response(&mut self,
    target_id: u64, result: Option<&Batch>,
    schema: Option<&SchemaDescriptor>, request_id: u64,
    client_id: u64, seek_pk: u128)          // ← added
```

Three changes to the body of `send_response`:

1. **Encoder**: replace `encode_wire_into_ipc` with `encode_wire_into`
   (checksum: true). The slot is forwarded directly to the TCP socket, so
   the master's checksummed encoding must be preserved. `encode_wire_into_ipc`
   was correct when the master discarded the IPC bytes and re-encoded; after
   zero-copy forwarding it would send zero checksums to the client.

2. **Schema block**: replace the worker-local `schema_wire_block_cache`
   lookup (which builds the block with an empty col_names slice) with
   `self.cat().get_cached_schema_wire_block` + `self.cat().get_col_names_bytes`,
   mirroring `send_scan_response` exactly. The master's `encode_response_buffer`
   encodes with full column names from the catalog cache; without this change,
   SEEK responses would silently omit column names after zero-copy forwarding.

3. **Arguments**: pass `client_id` and `seek_pk` into the encoder (replacing
   the two `0` literals). All call sites in `dispatch_inner` already have
   both values in scope.

The resulting `send_response` body becomes structurally identical to
`send_scan_response`, minus `FLAG_CONTINUATION`.

#### B2 — Reactor: reuse SCAN_REQ_ID_FLAG for SEEK replies

**File**: `crates/gnitz-engine/src/runtime/reactor/mod.rs`

Do **not** add a `parked_data_slots` map or a `park_data_slot` function. The
existing scan slot mechanism already provides everything needed:

- `alloc_scan_request_id()` allocates a u64 req_id with bit 31 set
  (`SCAN_REQ_ID_FLAG`).
- `drain_w2m_for_worker` intercepts any slot whose `internal_req_id` has
  `SCAN_REQ_ID_FLAG` set **before** inspecting `FLAG_HAS_DATA`:

  ```rust
  if slot.internal_req_id & SCAN_REQ_ID_FLAG != 0 {
      self.route_scan_slot(slot);   // unconditional — handles data, empty, error
      continue;
  }
  // FLAG_HAS_DATA check only reached for non-scan-flagged req_ids
  ```

- Because the intercept is unconditional, all reply types (data, empty result,
  error) are captured correctly. A `FLAG_HAS_DATA`-based intercept would miss
  empty results and errors and leave their awaiters hung permanently.

- `ingest_from_slot` and `park_owned` are **not touched**. They remain the
  decode path for pipeline validation requests, which use regular req_ids
  (no `SCAN_REQ_ID_FLAG`) and `await_reply` → `parked_replies`. Those two
  code paths are fully independent.

No new fields, no new futures, no new types.

#### B3 — Master: single_worker_async and dispatch_fanout return W2mSlot

**File**: `crates/gnitz-engine/src/runtime/master.rs`

Change `single_worker_async` to allocate a scan req_id and await via
`await_scan_slot`, returning the raw slot:

```rust
// Before: returns Result<Option<Batch>, String>
// After:  returns Result<W2mSlot, String>

async fn single_worker_async(...) -> Result<W2mSlot, String> {
    let (req_id, ...) = {
        let _guard = sal_excl.lock().await;
        unsafe {
            let disp = &mut *disp_ptr;
            let req_id = reactor.alloc_scan_request_id();   // ← SCAN_REQ_ID_FLAG set
            // ... write_group unchanged ...
            (req_id, ...)
        }
    };
    let slot = reactor.await_scan_slot(req_id as u32).await;
    // Peek status from the slot's ctrl block; no full decode needed.
    let ctrl = wire::peek_control_block(slot.bytes())
        .expect("W2M ctrl corrupt in single_worker_async");
    if ctrl.status != 0 {
        let msg = String::from_utf8_lossy(&ctrl.error_msg);
        return Err(format!("worker {}: {}: {}", worker, op_name, msg));
    }
    Ok(slot)
}
```

For the broadcast seek_by_index cache-miss path, replace `dispatch_fanout`
with `dispatch_scan_fanout` (already exists, already uses `alloc_scan_request_id`
and `await_scan_slot` internally). Also update the `req_ids` allocation inside
`dispatch_scan_fanout` from a heap `Vec<u64>` to a stack array — `nw` is bounded
by `MAX_WORKERS` (64) and is typically 4–16, so the allocation is avoidable:

```rust
// Before (in dispatch_scan_fanout)
let req_ids: Vec<u64> = (0..nw).map(|_| reactor.alloc_scan_request_id()).collect();

// After
let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
for id in req_ids[..nw].iter_mut() {
    *id = reactor.alloc_scan_request_id();
}
// Pass &req_ids[..nw] to write_group_with_req_ids
```

Apply the same stack-array change to `dispatch_fanout`:

```rust
// dispatch_fanout — after
let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
for id in req_ids[..nw].iter_mut() {
    *id = reactor.alloc_request_id();
}
{
    let _guard = sal_excl.lock().await;
    unsafe {
        let disp = &mut *disp_ptr;
        submit(disp, &req_ids[..nw])?;
        disp.signal_all();
    }
}
Ok(crate::runtime::reactor::join_all(
    req_ids[..nw].iter().map(|&id| reactor.await_reply(id))
).await)
```

`join_all` accepts `IntoIterator` — pass the iterator directly without collecting to an
intermediate `Vec` (same pattern applies to `dispatch_scan_fanout`):

```rust
// dispatch_scan_fanout — join_all after
Ok(crate::runtime::reactor::join_all(
    req_ids[..nw].iter().map(|&id| reactor.await_scan_slot(id as u32))
).await)
```

```rust
// Before: dispatch_fanout → Vec<DecodedWire>
// After:  dispatch_scan_fanout → Vec<W2mSlot>

let slots = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
    disp.write_group_with_req_ids(
        target_id, lsn, FLAG_SEEK_BY_INDEX, &[], &schema, &col_names,
        key, col_idx as u64, req_ids, -1, 0,
    )
}).await?;

let mut data_idx = None;
for (w, slot) in slots.iter().enumerate() {
    let ctrl = wire::peek_control_block(slot.bytes())
        .map_err(|e| format!("seek_by_index: worker {}: {}", w, e))?;
    if ctrl.status != 0 {
        return Err(format!("worker {}: seek_by_index: {}", w, String::from_utf8_lossy(&ctrl.error_msg)));
    }
    if ctrl.flags & FLAG_HAS_DATA != 0 {
        data_idx = Some(w);
    }
}
// Drop empty slots, return the one with data (or slot 0 if all are empty).
Ok(slots.swap_remove(data_idx.unwrap_or(0)))
```

`fan_out_seek_async` and `fan_out_seek_by_index_async` both become
`-> Result<W2mSlot, String>`.

**Add `peek_ctrl_size` helper to `wire.rs`** — consolidates the magic-offset
ctrl-size read used in `validate_all_distributed_async` and keeps the raw
`WAL_OFF_SIZE` reference out of master.rs:

```rust
// wire.rs
#[inline(always)]
pub(crate) fn peek_ctrl_size(data: &[u8]) -> Result<usize, &'static str> {
    if data.len() < WAL_OFF_SIZE + 4 {
        return Err("payload too small for ctrl_size field");
    }
    Ok(u32::from_le_bytes(data[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].try_into().unwrap()) as usize)
}
```

**Update `validate_all_distributed_async` for the Upsert path** (lines ~1368–1384):
The `P2Label::Upsert` branch calls `fan_out_seek_by_index_async` and accesses
`found.get_pk(0)` on the returned `Batch`. After the return type change this is
a compile error. Replace with a zero-copy decode of the slot:

```rust
let slot = Self::fan_out_seek_by_index_async(
    disp_ptr, reactor, sal_excl, target_id, *col_idx,
    crate::util::make_pk(key_lo, key_hi),
).await.map_err(|e| e)?;
let ctrl = wire::peek_control_block(slot.bytes())
    .map_err(|e| e.to_string())?;
if ctrl.flags & FLAG_HAS_DATA != 0 {
    let ctrl_size = wire::peek_ctrl_size(slot.bytes()).map_err(|e| e.to_string())?;
    let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(
        slot.bytes(), ctrl_size, ctrl,
    ).map_err(|e| e.to_string())?;
    if let Some(ref found) = zc.data_batch {
        if found.count > 0 && found.get_pk(0) != pk_i {
            let col_name = unsafe { (*disp_ptr).get_col_name(target_id, *source_col) };
            return Err(format!("Unique index violation on column '{}'", col_name));
        }
    }
}
```

Also add `WAL_OFF_SIZE` to the `wire` import line in `master.rs` (line 27).

Remove `extract_single_batch` — it only existed to re-wrap an already-owned
`Batch` into a second `Batch`, and the slot path makes it obsolete.

#### B4 — Executor: send_slot instead of send_ok_response

**File**: `crates/gnitz-engine/src/runtime/executor.rs`

`handle_seek` and `handle_seek_by_index` currently unify both the user-table
path (`fan_out_seek_async`, returns `Result<Option<Batch>, String>`) and the
system-table path (`(*cat_ptr).seek_family`, also returns
`Result<Option<Batch>, String>`) into a single `let result = if ... else ...`
binding. After Change B3, the user-table path returns `Result<W2mSlot, String>`,
which is a type mismatch — Rust will not compile both in a single binding.

Split the branches so each type is consumed independently:

```rust
// handle_seek — after
if target_id >= FIRST_USER_TABLE_ID {
    match MasterDispatcher::fan_out_seek_async(
        shared.dispatcher, &shared.reactor, &shared.sal_writer_excl,
        target_id, pk,
    ).await {
        Ok(slot) => {
            let rc = shared.reactor.send_slot(fd, slot).await;
            if rc < 0 { shared.reactor.close_fd(fd); }
        }
        Err(e)   => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
} else {
    match unsafe { (*shared.catalog).seek_family(target_id, pk) } {
        Ok(batch) => send_ok_response(shared, fd, target_id, batch.as_ref(),
                                      client_id, &schema, pk as u128).await,
        Err(e)    => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
}
```

Apply the same split to `handle_seek_by_index` (`fan_out_seek_by_index_async`
vs `(*cat_ptr).seek_by_index`). In both branches the `send_slot` return value
must be checked: `if rc < 0 { shared.reactor.close_fd(fd); }`. This mirrors the
existing scan path (executor.rs lines 680–681) and `send_ok_response`
(lines 872/887/906).

`send_slot` already exists and is already used by the scan path (`handle_scan`).

---

## What this does NOT change

- **Exchange path** (`FLAG_EXCHANGE`): multi-worker batches are merged before
  the client sees them. `process_exchange_from_slot` → `exchange_acc` →
  `dispatch_relay` stays as-is. The decode+copy there is necessary.

- **No-data ACK replies** (`park_owned`): these carry no batch and are already
  cheap. No change. They use regular req_ids (no SCAN_REQ_ID_FLAG) and flow
  through `await_reply`.

- **INSERT commit response** (`send_ok_response(None, lsn)`): comes from the
  committer channel, not a W2M slot. Out of scope here.

- **Error replies for SEEK**: after B2/B3, these are intercepted via
  `SCAN_REQ_ID_FLAG` → `route_scan_slot` → `scan_parked`. `single_worker_async`
  awaits via `await_scan_slot`, peeks `ctrl.status`, and returns `Err()` if
  non-zero. Error handling is correct for all reply types.

- **Pipeline validation error replies**: use regular req_ids, flow through
  `park_owned` → `await_reply` → `send_error`. Unchanged.

- **Wire protocol**: clients see identical bytes. After B1, `send_response`
  encodes with `checksum: true`, full column names from the catalog cache,
  and correct `client_id`/`seek_pk` — matching exactly what
  `encode_response_buffer` currently produces. For system-table `handle_seek`,
  B4 passes the actual `pk` instead of the hardcoded `0`; for
  `handle_seek_by_index` system tables the found row's pk is not in scope
  without decoding the returned Batch, so that case is left unchanged.

---

## Expected impact

| Change | Perf overhead eliminated | Notes |
|---|---|---|
| A: FxHashMap | ~4.37% SipHash | Drop-in, no semantic change |
| B: slot forwarding (copy 1) | ~1.67% memmove (append_mem_batch_range) | |
| B: slot forwarding (copy 2) | Hidden in 7.25% memset (Batch::with_schema zeroing) | extract_single_batch gone |
| B: slot forwarding (copy 3) | ~portion of 9.47% memmove (encode_response_buffer) | For SEEK traffic |
| B: drain loop | Reduced work per slot (no decode) | Part of 5.81% loop cost |

Total estimated reduction: **~15% of measured CPU cycles** on the w4_c1
benchmark, concentrated on the SEEK / SEEK_BY_INDEX paths.
`test_pk_seek` and `test_index_seek` should see the sharpest improvement.
The bulk-insert / view-maintenance benchmarks (most of the 80ms per-batch
time) are unaffected; they use the commit+ACK path, which this does not touch.

---

## Files touched

| File | Changes |
|---|---|
| `runtime/reactor/mod.rs` | FxHashMap/FxHashSet for all `ReactorShared` maps |
| `runtime/executor.rs` | FxHashMap for `Shared` maps; `handle_seek` / `handle_seek_by_index` split if/else; call `send_slot` for user-table path; pass `pk` to `send_ok_response` for system-table seek |
| `runtime/worker.rs` | `send_response` gains `client_id` + `seek_pk` params; switches to `encode_wire_into` + catalog-level schema cache with col_names; all call sites updated |
| `runtime/master.rs` | `single_worker_async` returns `W2mSlot`; broadcast path uses `dispatch_scan_fanout`; `fan_out_seek_async` / `fan_out_seek_by_index_async` updated; `validate_all_distributed_async` Upsert branch updated; `extract_single_batch` deleted; `WAL_OFF_SIZE` added to wire import |
| `runtime/wire.rs` | Add `peek_ctrl_size` helper |

No storage, WAL, wire format, or catalog changes. No new dependencies.
