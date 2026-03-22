# IPC Overhaul: Shared Append-Only Log

Prerequisite: Exchange & IPC improvements Phases 0–4 (done).

---

## Problem

256 fsyncs per push (64 partitions × 4 workers), ~11 IPC syscalls per
message (memfd+SCM_RIGHTS), 8× data copies. io_uring incompatible with
SCM_RIGHTS (CVE-2023-52654, kernel 6.7). SEQPACKET limit ~208 KB.

**Target**: 1 fdatasync, ~12 syscalls, 2× copies.

---

## Design Overview

```
External client (Rust/Python)
    │  length-prefixed bytes over SOCK_STREAM (no memfd)
    │  io_uring multishot accept + recv on server [Step 5]
    ▼
  Master process
    │  recv → classify → gather-encode ALL workers' data (partition-sorted)
    │  → append into SINGLE file-backed SAL (mmap)
    │  → fdatasync(sal_fd)           ← 1 fsync, ALL workers, ALL partitions
    │  → signal N workers via eventfd
    ▼
  Worker processes (forked, each owns partition range)
    │  read SAL at current offset → (ptr, len)  ← mmap page-cache hit, 0 syscalls
    │  clone into owned batch        ← 1 memcpy, existing upsert_batch
    │  upsert_batch into memtable    ← existing code, unchanged
    │  advance read_cursor
    │  ACK master

  Exchange (mid-circuit repartition, when needed):
    Worker Wi ──encode──→ exchange arena slot[i]  (shared memory)
    Worker Wi ──signal──→ Master ──barrier──→ all workers
    Worker Wj ──read──→ arena slot[*] for Wj  (clone into owned batch)
    Master never touches exchange data — only coordinates barriers.
```

Key decisions:
- **SAL not ring buffer**: append-only = full recovery, no wrap logic, no
  message size limit. Ring's recovery gap is fatal (~840 pushes overwritten
  before memtable flush).
- **Clone not zero-copy views**: avoids mmap accumulation, blob_arena
  accounting bugs, sort incompatibility. Clone cost (~10 μs) negligible
  vs eliminated 128 ms fsync.
- **memfd not MAP_SHARED|MAP_ANONYMOUS for W2M/exchange**: MADV_DONTNEED
  doesn't release shmem pages; ftruncate(fd,0) does.
- **fallocate + NOCOW**: btrfs NOCOW avoids CoW on every overwrite;
  fallocate avoids block allocation during fdatasync. Sub-1ms p99 vs 8ms
  sparse first-fill. Cursor-reset reuses blocks forever.
- **fdatasync flushes mmap writes on Linux**: unified page cache,
  page_mkwrite sets PAGECACHE_TAG_DIRTY, no msync needed. O(dirty pages).

---

## Step 1 — DONE

FFI primitives: `eventfd_ffi.py`, `sal_ffi.py`, `fdatasync_c` in
`mmap_posix.py`. Tests in `rpython_tests/eventfd_test.py`.

---

## Step 2 — DONE (2a, 2c complete; 2b, 2d deferred)

### 2a: SAL transport — DONE

- `SharedAppendLog(ptr, fd, mmap_size)` with `write_cursor`, `lsn_counter`,
  `epoch` (u32, bumped on checkpoint)
- `W2MRegion` with atomic cursor for worker→master responses
- `write_message_group` / `read_worker_message`: 576-byte group header,
  MAX_WORKERS=64, size prefix written last (atomic release fence)
- `_encode_wire` / `_parse_from_ptr`: factored from memfd transport
- `write_to_w2m` / `read_from_w2m`: W2M response channel
- Master: `_write_group`, `_send_to_workers`, `_send_broadcast`,
  `_sync_and_signal_all`, `_wait_all_workers`, `_collect_acks_and_relay`
- Worker: `_drain_sal` loop with epoch fence, `_dispatch_message`,
  W2M ACK/response, initial ready ACK on startup
- SAL allocation in `main.py`: fallocate + NOCOW, W2M memfd regions,
  eventfds, socketpair crash detection
- Tests: `rpython_tests/ipc_transport_test.py` (11 tests incl. cross-process
  checkpoint)

### 2b: Gather-encode into SAL — DEFERRED

Not yet implemented. Current code uses `repartition_batch` + per-worker
`_encode_wire` (3-copy scatter). Pure optimization — functional behavior
identical. Implement when profiling shows scatter is a bottleneck.

Planned API:
- `encode_batch_gather(dest_ptr, ...)` in `wal_columnar.py`
- `scatter_encode_to_sal(sal, ...)` in `ipc.py`
- `FLAG_PARTITION_SORTED = r_uint64(1 << 53)`
- Worker-side `ingest_batch_presorted(batch, boundaries)`

### 2c: Unified WAL + checkpoint/reset — DONE

- `_has_wal` flag on `EphemeralTable` / `PartitionedTable.set_has_wal()`;
  disabled in multi-worker mode (SAL handles durability)
- Crash recovery: `_recover_from_sal()` scans SAL from offset 0, replays
  FLAG_PUSH groups with `lsn > max_flushed_lsn`, epoch-aware (stops at
  epoch decrease)
- SAL preserved across restarts (no O_TRUNC); conditionalized fallocate
- Epoch-based fencing: group header offset 28 carries epoch (u32); workers
  compare against `_expected_epoch` to skip stale data after cursor reset
- Cooperative checkpoint: master sends FLAG_FLUSH (1<<14), workers flush
  all user-table families, reset `read_cursor=0`, bump `_expected_epoch`,
  ACK with FLAG_CHECKPOINT (1<<13); master resets `write_cursor=0`, bumps
  `sal.epoch`, zeros size prefix at offset 0 (atomic release)
- Proactive trigger: `_maybe_checkpoint()` at start of every `fan_out_*`
  method when `write_cursor >= 75%` of `SAL_MMAP_SIZE`
- SAL space guard in `_relay_exchange()` errors at < 12.5% remaining
- Post-fork synchronization: workers send ready ACK, master collects all
  before resetting SAL and starting backfill

### 2d: Shared Exchange Arena — DEFERRED

Current implementation uses relay-through-master: workers send exchange
data via W2M, master repartitions via `relay_scatter()`, sends result
back via SAL. Partially compensated by preloaded exchange optimization
(`FLAG_PRELOADED_EXCHANGE`).

Implement arena when exchange relay becomes a measured bottleneck. The
relay adds one data copy + master CPU for repartition, but avoids the
complexity of inter-worker shared memory coordination. Preloaded exchange
already eliminates the relay for trivial (direct INPUT→SHARD) views.

---

## Step 3: Length-Prefixed SOCK_STREAM (External IPC)

Drop memfd+SCM_RIGHTS from external client path. Switch to `SOCK_STREAM`
with 4-byte length-prefix framing. No message size limit.

### Current state (what to change)

External client ↔ master uses SOCK_SEQPACKET + SCM_RIGHTS fd passing:
- **Server side**: `ipc_ffi.py` has `gnitz_server_create` (SEQPACKET),
  `gnitz_ipc_send_fd`/`gnitz_ipc_recv_fd`/`gnitz_ipc_recv_fd_nb` (SCM_RIGHTS).
  `ipc.py` has `serialize_to_memfd` → `send_batch` and `receive_payload` /
  `try_receive_payload` (memfd mmap + parse). `executor.py:_drain_client`
  calls `try_receive_payload` for recv, `send_batch` for responses.
- **Rust client**: `rust_client/gnitz-protocol/src/transport.rs` — SEQPACKET
  connect + SCM_RIGHTS send/recv.
- **Python client**: `py_client/gnitz_client/` — wraps the Rust client via
  PyO3 bindings; transport changes are Rust-only.

Key insight: `_encode_wire` / `_parse_from_ptr` (factored in Step 2a)
already produce/consume the WAL-block wire format as raw bytes. Step 3 only
replaces the transport layer (memfd+SCM_RIGHTS → length-prefixed TCP-style
framing). The wire payload format is unchanged.

Internal master ↔ worker IPC (SAL + W2M) is completely unaffected.

### Wire protocol

```
Both directions: [u32 payload_length (LE)][payload bytes]
payload_length = 0: connection close
payload bytes: same WAL-block wire format as today
```

### `ipc_ffi.py` changes

`gnitz_server_create_stream`: AF_UNIX SOCK_STREAM listen socket.
`gnitz_send_framed` / `gnitz_recv_framed`: length-prefixed send/recv with
`send_all` / `recv_all` loops handling partial I/O and EINTR.

### `ipc.py` new functions

`send_framed(sock_fd, target_id, zbatch, ...)`: encode via `_encode_wire` +
framed send.
`recv_framed(sock_fd)`: receive framed → `_parse_from_ptr`.

### `executor.py` changes

`_handle_client`: `recv_framed` / `send_framed`. Multi-worker scans call
`fan_out_scan_stream(client_fd)`.

### Rust client changes

`transport.rs`: SOCK_STREAM + length-prefixed framing. Scan responses: read
framed messages in loop until `FLAG_SCAN_DONE` sentinel.

Breaking change. Version both sides together.

### Files changed

- `gnitz/server/ipc.py`, `gnitz/server/ipc_ffi.py`, `gnitz/server/executor.py`
- `rust_client/gnitz-protocol/src/transport.rs`

### Test

- `make run-server_test-c`
- `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/`

---

## Step 4: `uring_ffi.py` — io_uring FFI

Raw syscalls (no liburing). Syscall numbers 425/426/427 (x86_64 stable ABI).

Follow the FFI pattern in `eventfd_ffi.py` and `ipc_ffi.py`:
`ExternalCompilationInfo` with `separate_module_sources` for inline C,
`rffi.llexternal` for each function. Use `releasegil=True` on blocking calls.

### C API

| Function | Description |
|---|---|
| `gnitz_uring_create(entries)` | `io_uring_setup` + mmap SQ/CQ/SQEs |
| `gnitz_uring_destroy(ring)` | munmap + close |
| `gnitz_uring_prep_accept(ring, fd, user_data)` | Multishot via `IORING_ACCEPT_MULTISHOT` in `sqe->ioprio` |
| `gnitz_uring_prep_recv(ring, fd, buf, len, user_data)` | `IORING_OP_RECV` |
| `gnitz_uring_prep_send(ring, fd, buf, len, user_data)` | `IORING_OP_SEND` |
| `gnitz_uring_submit_and_wait(ring, min_complete)` | `io_uring_enter` with `IORING_ENTER_GETEVENTS` |
| `gnitz_uring_drain(ring, out_arrays, max)` | Read CQEs → output arrays, advance `cq_head` |
| `gnitz_uring_register_buffers(ring, iovecs, count)` | `IORING_REGISTER_BUFFERS` |

All memory barriers and SQE manipulation in C. RPython never touches ring
internals. `releasegil=True` on `submit_and_wait` (blocking).
All io_uring buffers must be raw-allocated (`flavor='raw'`).

### Files

- New: `gnitz/server/uring_ffi.py` (~300 lines C, ~100 lines Python)

### Test

- New: `rpython_tests/uring_test.py` — NOP submit/complete, tempfile
  read/write, Unix socket accept+recv
- `make run-uring_test-c`

---

## Step 5: io_uring Event Loop for External Clients

Replace poll-based `run_socket_server` in `executor.py` with io_uring
event loop. Current loop uses `ipc_ffi.poll()` + non-blocking accept/recv.
Step 3 must land first (SOCK_STREAM framing).

```
io_uring instance (256 SQ entries)
  ├── 1 multishot IORING_OP_ACCEPT (user_data = ACCEPT_TAG)
  ├── per-client IORING_OP_RECV (user_data = client_fd)
  └── per-response IORING_OP_SEND (user_data = SEND_TAG | client_fd)
```

Per-connection state machine for length-prefix parsing (4-byte header →
payload). Pre-allocated recv buffer pool (N × 8 MB, registered).

Fallback to poll + `recv_framed` if `io_uring_setup` fails.

Available io_uring features on kernel 6.18:

| Feature | Kernel | Relevance |
|---|---|---|
| Multishot recv | 6.0 | Eliminates recv re-submission |
| Provided buffer rings + incremental consumption | 6.12 | Variable-size messages |
| Receive bundles | 6.10 | Batches recv completions |
| Ring resizing | 6.13 | Start small, grow under load |

### Files changed

- `gnitz/server/executor.py`: `_run_uring_server` + `_run_poll_server` fallback

### Test

- `make run-server_test-c`
- `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/`

---

## Implementation Order

```
Step 1   DONE
Step 2a  DONE — SAL transport, W2M, master/worker SAL integration
Step 2b  DEFERRED — gather-encode optimization (pure perf, no functional gap)
Step 2c  DONE — unified WAL, checkpoint/reset, crash recovery, epoch fencing
Step 2d  DEFERRED — exchange arena (relay-through-master works, preload covers common case)
Step 3   External IPC (independent of Step 2)
Step 4   uring_ffi.py (independent of everything)
Step 5   io_uring event loop (depends on 3 + 4)
```

---

## Cost Comparison

Per push, 4 workers, 64 partitions/worker:

| | Current | After Step 2 | After Step 3 | After Step 5 |
|---|---|---|---|---|
| Data copies | 8× | 2× | 2× | 2× |
| WAL fdatasync() | 256 | **1** | 1 | 1 |
| Transport syscalls | ~120 | 9 | 4 | ~2 |
| Total syscalls | ~632 | ~32 | ~14 | ~12 |

---

## Lessons Learned (Steps 1–2)

### Epoch fencing beats zeroing

After checkpoint, old data at offset 0 still has non-zero size prefixes.
Zeroing 1 GB is expensive and creates a visibility window. Instead, each
group header carries an epoch (u32 at offset 28). Workers compare against
`_expected_epoch` — stale groups are skipped without any bulk memory
operation. Recovery uses epoch-decrease detection to find the stale
boundary.

### Worker ready ACK is required for startup synchronization

Workers must signal the master after recovery completes (before entering
the main loop). Without this, the master could reset the SAL or start
backfill before workers have finished replaying. The ready ACK uses the
existing W2M + eventfd channel — no new mechanism needed.

### Checkpoint can't happen mid-exchange

`_maybe_checkpoint()` must only trigger at `fan_out_*` entry points,
never inside `_collect_acks_and_relay()`. Workers inside `do_exchange()`
are blocked waiting for a relay response; they can't process FLAG_FLUSH
until the exchange completes. The 75% threshold leaves 256 MB headroom
for one full operation (push + exchange relay).

### SAL space guard in _relay_exchange

A complex DAG with many exchanges can fill the SAL faster than expected.
The relay guard at < 12.5% remaining gives a clear error instead of the
generic "SAL full" crash. Future work: if this fires in practice, consider
mid-operation checkpoint or increasing SAL_MMAP_SIZE.

### Cross-process eventfd tests need two eventfds

Using a single eventfd for bidirectional parent↔child signaling causes
races: either process can consume the initial signal. Use one eventfd per
direction (c2m_efd, m2c_efd) for deterministic synchronization in tests.

### ExprProgram num_regs=0 is valid for pure COPY_COL MAP programs

The per-row `evaluate_map` fallback (used when `reindex_col >= 0`) calls
`eval_expr` which returns `regs[result_reg]`. When the MAP bytecode
contains only COPY_COL instructions, `num_regs=0` and `regs` is empty —
accessing `regs[0]` crashes with "fixed getitem out of bounds". Both
`eval_expr` and `_eval_row_direct` must guard the return with
`if program.num_regs == 0: return sentinel`.

### SAL file must not be truncated on startup

The original `O_TRUNC` flag zeroed the SAL before recovery could read it.
In multi-worker mode, per-partition WAL is disabled (`_has_wal=False`), so
the SAL is the durability layer. Use `O_CREAT` without `O_TRUNC`;
conditionalize `fallocate_c` on `fget_size < SAL_MMAP_SIZE`.

### Worker ordering invariant for checkpoint

Workers must reset `read_cursor=0` and bump `_expected_epoch` BEFORE
sending the FLAG_CHECKPOINT ACK. Master resets `write_cursor=0` AFTER
receiving all ACKs. This prevents a window where master overwrites data
a worker hasn't finished reading.

---

## Open Issues

### 1. Worker-side partition boundary routing

Step 2b encodes partition boundary tables into SAL. Worker must use them
to avoid re-hashing in `PartitionedTable.ingest_batch`. Add
`ingest_batch_presorted(batch, boundaries)`. Deferred.

### 2. W2M page accumulation after large scans

Scan streaming (Step 2a) bounds per-worker data to ~1/N total. Release
via `ftruncate(fd,0)` + `ftruncate(fd,SIZE)` if profiling shows high-water
mark persists. Latent, not correctness.

### 3. xfs first-fill conversion

fallocate creates unwritten extents; first write converts to written.
Optional zero-fill pass at startup (~0.5s for 1 GB). Defer to benchmarking.

### 4. btrfs snapshots

NOCOW reverts to CoW when extents shared via snapshot. Mitigate by
excluding SAL directory from snapshots (subvolume layout). Deployment doc.

---

## Deployment Requirements

- Linux kernel >= 5.19 (Steps 1–3 any kernel; Step 5 needs io_uring)
- Filesystem: xfs or btrfs recommended (ext4 has higher first-fill overhead)
- btrfs: SAL directory on subvolume excluded from snapshots
- 1 GB disk for SAL file (pre-allocated)
- No liburing, no sysctl tuning

---

## What This Does NOT Do

- Does not change WAL-block wire format (adds optional boundary region)
- Does not change storage engine shard read path
- Does not add io_uring for WAL writes or shard I/O
- Does not add TCP/remote transport (AF_UNIX only)
- Does not change single-process WAL/scan paths
- Does not change MemTable, compaction, or storage layer code
