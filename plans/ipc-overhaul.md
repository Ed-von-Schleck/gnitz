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

## Step 1: `eventfd_ffi.py` + `sal_ffi.py` + `fdatasync_c` — DONE

- `gnitz/server/eventfd_ffi.py`: eventfd create/signal/wait/wait_any (C FFI)
- `gnitz/server/sal_ffi.py`: try_set_nocow + fallocate_c (C FFI)
- `gnitz/storage/mmap_posix.py`: added `fdatasync_c(fd)` (raises MMapError)
- `rpython_tests/eventfd_test.py`: 7 tests, all pass
- `Makefile`: test registered as `make run-eventfd_test-c`

### SAL allocation (before fork, in `main.py`) — reference for Step 2

```python
SAL_MMAP_SIZE    = 1 << 30             # 1 GB, pre-allocated via fallocate
W2M_REGION_SIZE  = 1 << 30             # 1 GB virtual per worker, memfd-backed
# Total virtual: 5 GB for 4 workers (0.004% of 128 TB VA space)

sal_path = base_dir + "/wal.sal"
sal_fd = rposix.open(sal_path, O_RDWR | O_CREAT, 0o644)
sal_ffi.try_set_nocow(sal_fd)          # btrfs in-place overwrites; silent on other FS
if intmask(mmap_posix.fget_size(sal_fd)) < SAL_MMAP_SIZE:
    sal_ffi.fallocate_c(sal_fd, SAL_MMAP_SIZE)
sal_ptr = mmap_posix.mmap_file(sal_fd, SAL_MMAP_SIZE,
    prot=PROT_READ | PROT_WRITE, flags=MAP_SHARED)

for w in range(num_workers):
    w2m_fd = mmap_posix.memfd_create_c("w2m_%d" % w)
    mmap_posix.ftruncate_c(w2m_fd, W2M_REGION_SIZE)
    w2m_ptr = mmap_posix.mmap_file(w2m_fd, W2M_REGION_SIZE, ...)
    m2w_efd = eventfd_ffi.eventfd_create()
    w2m_efd = eventfd_ffi.eventfd_create()
```

After fork: child closes eventfds of other workers. Master writes SAL,
workers read.

---

## Step 2: Internal IPC + Unified WAL

Replace memfd+socket in `master.py` / `worker.py` with single file-backed
SAL transport. The SAL serves as both IPC channel and durable WAL.

### 2a: SAL transport (master side)

#### `SharedAppendLog` class

```python
class SharedAppendLog(object):
    def __init__(self, ptr, fd):
        self.ptr = ptr           # mmap base (1 GB)
        self.fd = fd             # backing file fd
        self.write_cursor = 0    # master only
```

Each worker holds its own `read_cursor` (integer).

#### Message group layout

```
[8-byte size prefix]
[group_header: 64 bytes, cache-line aligned]
  u64  total_payload_size
  u64  lsn
  u32  num_workers
  u32  flags                      // FLAG_PUSH, FLAG_DDL_SYNC, etc.
  u32  target_id
  u32  reserved
  u32  worker_offsets[MAX_WORKERS] // byte offset of worker i's data
  u32  worker_sizes[MAX_WORKERS]   // byte size (0 = no data)

[worker_0_data: WAL-block encoded IPC message (control + schema + data)]
[worker_1_data: ...]
...
```

MAX_WORKERS = 64 (compile-time). Header fixed-size regardless of actual count.

#### `ipc.py` new functions

- `write_message_group(sal, target_id, lsn, flags, worker_bufs, num_workers)`:
  append all workers' data as one contiguous group
- `read_worker_message(sal_ptr, read_cursor, worker_id)`:
  extract this worker's data → `(payload, group_advance)` or `(None, 0)`
- `_encode_wire(...)`: factored from `serialize_to_memfd` (control + schema + data blocks)
- `_parse_from_ptr(ptr, length)`: factored from `_recv_and_parse`
- `send_to_sal` / `receive_from_sal`: for W2M anonymous regions

#### `master.py` changes

`fan_out_push`: encode all workers → single message group → fdatasync once →
signal all workers via eventfd → collect ACKs from W2M regions.

`broadcast_ddl`: encode once → message group with identical data for all workers.

#### Streaming scan forwarding

`fan_out_scan_stream(target_id, schema, client_fd)`: raw-forward each
worker's W2M scan result directly to client. O(1) master memory. No decode,
no accumulate, no re-encode. N+1 framed messages (N results + FLAG_SCAN_DONE
sentinel).

#### `worker.py` changes

Main loop: `read_worker_message` from SAL → handle → advance `read_cursor` →
ACK via W2M. No worker-side fsync. Push handler: `ingest_to_family` +
`upsert_batch` (existing code, unchanged).

#### Socket keepalive

SEQPACKET socketpair kept for crash detection only (POLLHUP). No data in
steady state.

#### Back-pressure

Protocol is request/response: master blocks on ACKs before next push.
If `write_cursor` approaches `SAL_MMAP_SIZE`, master forces checkpoint.

### 2b: Gather-encode into SAL (SoA-aware scatter)

Replace 3-copy scatter (gather → encode → memcpy) with single
gather-encode pass writing directly into SAL. Classify by partition
(256-way), group by worker, sort indices by partition within each worker.

#### SAL direct-write API

```python
def sal_get_write_ptr(sal, max_size):
    return rffi.ptradd(sal.ptr, sal.write_cursor + 8)

def sal_commit(sal, actual_size):
    _write_u64(sal.ptr, sal.write_cursor, r_uint64(actual_size))
    sal.write_cursor += 8 + _align8(actual_size)
```

#### `wal_columnar.py` new function

`encode_batch_gather(dest_ptr, max_size, schema, src_batch, indices,
boundaries, lsn, table_id)`: gather-encode selected rows into dest.
Extra region: `u32[num_boundaries]` partition boundary table.

#### `ipc.py` new function

`scatter_encode_to_sal(sal, batch, num_workers, assignment, schema,
target_id, lsn, flags)`: classify by partition → gather per-worker indices
with boundaries → encode → write single message group.

New IPC flag: `FLAG_PARTITION_SORTED = r_uint64(1 << 53)`.

### 2c: Single unified WAL (file-backed SAL)

The SAL file IS the WAL. No separate `WorkerWAL`. No `append_and_map`.
No per-push `mmap()`. No worker-side fsync.

#### Recovery

```python
def recover_from_sal(sal_path, engine, assignment, worker_id):
    # Sequential forward scan from offset 0.
    # Extract this worker's data via group header offsets.
    # Replay blocks with LSN > last_flushed_lsn.
    # After cursor-reset: old-epoch groups (higher offsets, lower LSN) skipped.
    # Cost: O(file_size) header reads. ~1 ms for 100 MB.
```

#### Checkpoint / SAL reset

Worker: after all partitions flushed since last reset, set `FLAG_CHECKPOINT`
on ACK, reset own `read_cursor = 0`.

Master: when ALL workers checkpointed, `sal.write_cursor = 0`. No file
operations. Overwrites from offset 0 (blocks already allocated).

#### `table.py` changes

```python
if self._has_wal:
    self.wal_writer.append_batch(lsn, self.table_id, batch)  # single-process
    self.memtable.upsert_batch(batch)
else:
    self.memtable.upsert_batch(batch)  # multi-worker: SAL handles durability
```

### 2d: Shared Exchange Arena

Per-worker memfd slots (1 GB virtual each, demand-paged). Workers
scatter-encode directly into their slot; other workers read and clone.
Master coordinates barriers only — never touches exchange data.

#### Allocation (before fork)

```python
EXCHANGE_SLOT_SIZE = 1 << 30   # 1 GB virtual per worker
for w in range(num_workers):
    fd = mmap_posix.memfd_create_c("xchg_%d" % w)
    mmap_posix.ftruncate_c(fd, EXCHANGE_SLOT_SIZE)
    exchange_ptrs[w] = mmap_posix.mmap_file(fd, EXCHANGE_SLOT_SIZE, ...)
```

#### Slot layout

```
Slot header (64 bytes):
  u32 sub_batch_count, u32 total_bytes_written
  u32 offsets[MAX_WORKERS], u32 sizes[MAX_WORKERS]
Sub-batch for W0: [WAL block]
Sub-batch for W1: [WAL block]
...
```

#### Protocol flags

```python
FLAG_EXCHANGE_READY    = r_uint64(1 << 54)  # worker → master: slot written
FLAG_EXCHANGE_BARRIER  = r_uint64(1 << 55)  # master → workers: all ready
FLAG_EXCHANGE_CONSUMED = r_uint64(1 << 56)  # worker → master: reads done
```

#### Exchange flow

1. Worker repartitions → encodes sub-batches into own slot
2. Signals `FLAG_EXCHANGE_READY` to master
3. Master collects N ready → broadcasts `FLAG_EXCHANGE_BARRIER`
4. Workers read from all slots, clone into owned batches
5. Workers signal `FLAG_EXCHANGE_CONSUMED`
6. Next exchange: `ftruncate(fd,0)` + `ftruncate(fd,SIZE)` releases pages

Consumed barrier guarantees no readers remain → no double-buffering needed.
SIGBUS safety: ftruncate window is between consumed and next write.

#### `WorkerExchangeHandler` rewrite

`do_exchange`: ftruncate release → local repartition → encode to slot →
signal ready → wait barrier → read all slots → clone → signal consumed.

#### `master.py` changes

`_collect_and_barrier` replaces `_relay_exchange`: barrier coordination
only, no data relay. `_relay_exchange` and `relay_scatter` deleted.

### Files changed (Step 2)

- `gnitz/server/main.py`: SAL + W2M + eventfd + exchange arena before fork
- `gnitz/server/ipc.py`: message group read/write, SAL transport, new flags
- `gnitz/server/master.py`: SAL fan_out, streaming scan, exchange barrier
- `gnitz/server/worker.py`: SAL read loop, exchange arena handler
- `gnitz/storage/wal_columnar.py`: `encode_batch_gather`
- `gnitz/storage/table.py`: `_has_wal` flag for multi-worker mode
- `gnitz/dbsp/ops/exchange.py`: unchanged (repartition_batch reused)

### Test (Step 2)

- `make run-eventfd_test-c`
- `make run-storage_comprehensive_test-c`
- `make run-multicore_test-c` (covers master/worker + exchange)
- `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/`

---

## Step 3: Length-Prefixed SOCK_STREAM (External IPC)

Drop memfd+SCM_RIGHTS from external client path. Switch to `SOCK_STREAM`
with 4-byte length-prefix framing. No message size limit. No `slice_view`,
`FLAG_MORE`, or `_send_chunked`.

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

Replace poll-based `run_socket_server` with io_uring event loop.

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
Step 1   DONE — eventfd_ffi.py, sal_ffi.py, fdatasync_c
Step 2   Internal IPC overhaul (2a → 2b → 2c → 2d)
         2a-2c can land with existing master-relay exchange as fallback
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

## Validated Claims

All claims validated against kernel 6.18/6.19 docs, headers, and man pages.
See git history for the full validation report.

One unverifiable claim: XFS commit `fc0561ce` (fdatasync skips log force for
timestamp-only changes) — not in kernel docs, requires XFS git history.
Behavior is consistent with XFS architecture regardless.

---

## What This Does NOT Do

- Does not change WAL-block wire format (adds optional boundary region)
- Does not change storage engine shard read path
- Does not add io_uring for WAL writes or shard I/O
- Does not add TCP/remote transport (AF_UNIX only)
- Does not change single-process WAL/scan paths
- Does not change MemTable, compaction, or storage layer code
