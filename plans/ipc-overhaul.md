# IPC Overhaul: Shared Append-Only Log

Prerequisite: Exchange & IPC improvements Phases 0–4 (done).

---

## Problem

### Fsync Amplification (the real bottleneck)

Every `PartitionedTable.ingest_batch` fans out to up to 256 live partition
stores (64 per worker with 4 workers). Each `PersistentTable.ingest_batch`
calls `wal_writer.append_batch` → `encode_batch_to_buffer` + `write_all` +
`fsync_c`. For a single client push with 4 workers and uniform row
distribution:

```
Client push → master repartitions by worker → 4 sub-batches
  Worker 0: PartitionedTable.ingest_batch
    → partition 0:  WAL encode + write() + fsync()
    → partition 1:  WAL encode + write() + fsync()
    → ...
    → partition 63: WAL encode + write() + fsync()
  Worker 1–3: same
Total: 256 encode + 256 write() + 256 fsync()
```

Each `fsync` costs 0.1–10ms depending on storage. At 0.5ms average, the 256
fsyncs cost **128ms** per push. On spinning disks, this exceeds 1 second.

### IPC Syscalls

The IPC hot path pays ~11 syscalls per message. Send side: `memfd_create`,
`ftruncate`, `mmap`, `munmap`, `sendmsg` (SCM_RIGHTS), `close` (6). Receive
side: `recvmsg` (SCM_RIGHTS), `fstat`, `mmap` (3), plus deferred `munmap` +
`close` on payload release (2). A poll-based loop adds one `poll` per
iteration.

Two forcing constraints:

1. **io_uring removed SCM_RIGHTS support** (CVE-2023-52656, kernel 6.8+). The
   current memfd+fd-passing design is fundamentally incompatible with io_uring
   for recv. Any io_uring adoption requires dropping memfd from the hot path.

2. **SEQPACKET message size limit** is `sk_sndbuf - 32`. Default ~208 KB.
   Raising it requires `sysctl net.core.wmem_max` + `setsockopt(SO_SNDBUF)`.
   Step 3 eliminates this constraint entirely by switching the external client
   socket to `SOCK_STREAM` with length-prefixed framing.

### Copies

The push hot path copies every byte of column data ~8× between client and
durable storage:

```
Client → memfd                     (1: column encode)
Master repartition → sub-batches   (2: per-column gather)
Master encode → wire_buf           (3: per-column memmove)
Master wire_buf → memfd            (4: bulk memmove)
Worker partition split → 64 subs   (5: per-column gather)
Per-partition WAL encode            (6: per-column memmove)
Per-partition WAL write()           (7: kernel copy to disk)
Per-partition memtable clone        (8: per-column memmove)
```

Copies 2–4 are three separate passes over the same data on the master. Copy 5
re-does the partition classification that the master already computed. Copy 8
exists because the memfd/ring backing must be freed before the memtable flushes.

### Current cost per push (4 workers, 64 partitions/worker, uniform)

| | Count |
|---|---|
| User-space data copies | 8× payload volume |
| IPC syscalls (transport) | ~120 |
| WAL write() syscalls | 256 |
| WAL fsync() syscalls | 256 |
| **Total syscalls** | **~632** |

---

## Key Insight

The current architecture treats IPC transport, WAL durability, memtable
backing, and exchange relay as four separate concerns, each with its own
format, buffer, and lifecycle. The plan unifies the first two and simplifies
the third:

1. A **single shared append-only log** (SAL — one file-backed mmap for all
   workers) replaces both the memfd+SCM_RIGHTS transport AND the per-partition
   WAL files. The master gather-encodes partition-sorted batches for all N
   workers into the SAL as one contiguous append, calls `fdatasync()` once,
   then signals all workers. One file, one fdatasync, one NVMe flush command.

2. The worker **reads from the SAL** (mmap page-cache hit, zero syscalls) and
   **clones into owned memtable batches** via the existing `upsert_batch`
   path. The clone is one sequential memcpy per column (~10 μs for typical
   batches). No per-push `mmap()`/`munmap()`. No long-lived pinning. No
   worker-side fsync. The memtable code is completely unchanged.

3. A **shared exchange arena** (pre-fork shared memory) replaces the
   master-as-relay exchange model. Workers scatter-encode exchange data
   directly into the arena; other workers read and clone with zero master
   involvement. The master coordinates barriers only.

One `fdatasync` (master-owned). Zero per-push `mmap`/`munmap`. Zero
`write()`. Zero cross-process fsync assumptions. The existing memtable and
storage layer are oblivious to the SAL.

### Why not a ring buffer?

An earlier version of this plan proposed an SPSC ring buffer instead of an
append-only log. Investigation revealed three problems with a ring:

1. **The ring solves a problem that doesn't exist.** The protocol is
   request/response: master writes, worker processes, ACKs, master writes
   again. At most 1–3 messages are in flight (preloaded exchange adds a
   small, bounded number of extra messages per push). Circular indexing,
   skip markers, and wrap detection are machinery for a streaming workload
   that never occurs.

2. **Recovery gap.** A ring recycles space as the consumer advances. Once the
   worker clones a message and advances, the master may overwrite that offset
   with the next push. If the memtable hasn't flushed to a shard yet, the
   overwritten data exists only in volatile memory. On crash, it's lost.
   Kernel writeback can flush dirty overwrites to disk nondeterministically,
   making the recovery outcome depend on timing. With 16 MB capacity and
   100 KB pushes, the ring wraps every ~160 pushes — while the memtable may
   not flush for ~1000 pushes. ~840 pushes' WAL data could be irrecoverably
   overwritten. Compare the current per-partition WALs: append-only,
   truncated only after shard flush — zero recovery gap.

3. **Fixed maximum message size.** Ring capacity minus skip-marker waste at
   the wrap point yields ~15.5 MB effective capacity. Exceeding this requires
   either a larger ring (wastes disk), master-side chunking (protocol
   complexity), or client-side splitting (pushes complexity outward).

The SAL eliminates all three: append-only means no overwrites (full recovery
correctness), 1 GB pre-sized sparse file means no message size limit, and
sequential append means no wrap/skip logic. The C code shrinks from ~200 lines
(ring struct, skip markers, wrap detection) to ~20 lines (eventfd helpers).

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

---

## Step 1: `eventfd_ffi.py` — eventfd Helpers + mmap Additions

New file: `gnitz/server/eventfd_ffi.py`

Inline C via `separate_module_sources` (same pattern as `ipc_ffi.py`).

### C API

```c
#include <sys/eventfd.h>
#include <poll.h>
#include <unistd.h>

int gnitz_eventfd_create(void) {
    return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

int gnitz_eventfd_signal(int efd) {
    uint64_t v = 1;
    return (write(efd, &v, 8) == 8) ? 0 : -1;
}

int gnitz_eventfd_wait(int efd, int timeout_ms) {
    struct pollfd pfd = { .fd = efd, .events = POLLIN };
    int r = poll(&pfd, 1, timeout_ms);
    if (r > 0) {
        uint64_t v;
        (void)read(efd, &v, 8);
    }
    return r;
}

int gnitz_eventfd_wait_any(int *efds, int n, int timeout_ms) {
    struct pollfd pfds[64];  /* MAX_WORKERS */
    for (int i = 0; i < n && i < 64; i++) {
        pfds[i].fd = efds[i];
        pfds[i].events = POLLIN;
    }
    return poll(pfds, n, timeout_ms);
}
```

~40 lines of C total.

### RPython wrappers

```python
@jit.dont_look_inside
def eventfd_create():
    fd = _gnitz_eventfd_create()
    if rffi.cast(lltype.Signed, fd) < 0:
        raise errors.StorageError("eventfd_create failed")
    return intmask(fd)

@jit.dont_look_inside
def eventfd_signal(efd):
    _gnitz_eventfd_signal(rffi.cast(rffi.INT, efd))

@jit.dont_look_inside
def eventfd_wait(efd, timeout_ms):
    return int(_gnitz_eventfd_wait(
        rffi.cast(rffi.INT, efd), rffi.cast(rffi.INT, timeout_ms)))

@jit.dont_look_inside
def eventfd_wait_any(efd_list, timeout_ms):
    # Marshal int list to C array, call gnitz_eventfd_wait_any
    ...
```

### `mmap_posix.py` additions

```python
_fdatasync = rffi.llexternal("fdatasync", [rffi.INT], rffi.INT,
                             compilation_info=eci, _nowrapper=True)

@jit.dont_look_inside
def fdatasync_c(fd):
    res = _fdatasync(rffi.cast(rffi.INT, fd))
    if rffi.cast(lltype.Signed, res) < 0:
        raise MMapError()

MAP_ANONYMOUS = 0x20

@jit.dont_look_inside
def mmap_anonymous_shared(size):
    res = _mmap(lltype.nullptr(rffi.CCHARP.TO),
                rffi.cast(rffi.SIZE_T, size),
                rffi.cast(rffi.INT, PROT_READ | PROT_WRITE),
                rffi.cast(rffi.INT, MAP_SHARED | MAP_ANONYMOUS),
                rffi.cast(rffi.INT, -1),
                rffi.cast(rffi.LONGLONG, 0))
    if is_invalid_ptr(res):
        raise MMapError()
    return res

MADV_DONTNEED = 4

_madvise = rffi.llexternal("madvise",
    [rffi.VOIDP, rffi.SIZE_T, rffi.INT], rffi.INT,
    compilation_info=eci, _nowrapper=True)

@jit.dont_look_inside
def madvise_dontneed(ptr, size):
    """Release physical pages. Virtual mapping preserved, reads return zeroes."""
    res = _madvise(rffi.cast(rffi.VOIDP, ptr),
                   rffi.cast(rffi.SIZE_T, size),
                   rffi.cast(rffi.INT, MADV_DONTNEED))
    if rffi.cast(lltype.Signed, res) < 0:
        raise MMapError()
```

### SAL allocation (before fork, in `main.py`)

One file-backed SAL (master→all workers) + per-worker anonymous shared
regions (worker→master).

**Master→worker SAL** (file-backed, single file): one `ftruncate` + one
`mmap`. The file is extended to the full mmap size (`SAL_MMAP_SIZE`, 1 GB)
as a sparse file — `ftruncate` only modifies inode metadata (~1 μs on any
POSIX filesystem), physical blocks are allocated on first write (demand
paging by the filesystem). The master writes all workers' data into this
single file and calls `fdatasync` once — no cross-process fsync assumption.

**Worker→master regions** (anonymous, per worker): one
`mmap(MAP_SHARED|MAP_ANONYMOUS)` each. These carry ACKs, scan results,
exchange signals, and error responses.

```python
SAL_MMAP_SIZE    = 1 << 30             # 1 GB virtual + file size (sparse)
W2M_REGION_SIZE  = 1 << 30             # 1 GB virtual per worker, anonymous (demand-paged)
# Total virtual: SAL_MMAP_SIZE + W2M_REGION_SIZE * num_workers
# = 5 GB for 4 workers (0.004% of 128 TB x86-64 VA space)
# Physical at rest: 0 (SAL sparse, W2M demand-paged)
```

Create eventfds (one per direction, per worker) before fork.

### SAL setup

```python
sal_path = base_dir + "/wal.sal"
sal_fd = rposix.open(sal_path, O_RDWR | O_CREAT, 0o644)
mmap_posix.ftruncate_c(sal_fd, SAL_MMAP_SIZE)   # sparse — ~1 μs, any filesystem
sal_ptr = mmap_posix.mmap_file(
    sal_fd, SAL_MMAP_SIZE,
    prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
    flags=mmap_posix.MAP_SHARED,
)

for w in range(num_workers):
    w2m_ptr = mmap_posix.mmap_anonymous_shared(W2M_REGION_SIZE)
    m2w_efd = eventfd_ffi.eventfd_create()
    w2m_efd = eventfd_ffi.eventfd_create()
    ...
```

After fork: child closes eventfds of other workers. All processes (master +
workers) inherit the SAL mmap — master writes, workers read.

### Files changed
- New: `gnitz/server/eventfd_ffi.py` (~40 lines C, ~40 lines Python)
- `gnitz/storage/mmap_posix.py`: add `mmap_anonymous_shared(size)`,
  `fdatasync_c(fd)`, `madvise_dontneed(ptr, size)`

### Test
- New: `rpython_tests/eventfd_test.py` — eventfd create/signal/wait, wait_any
  with multiple fds, timeout behavior
- Build: `make prove-eventfd`

---

## Step 2: Internal IPC + Unified WAL

Replace the memfd+socket path in `master.py` and `worker.py` with the
single file-backed SAL transport. The SAL serves as both the IPC channel
and the durable WAL for all workers.

### 2a: SAL transport (master side)

#### `main.py` changes

SAL + eventfd allocation before fork (see Step 1 setup code). Pass SAL
pointer, fd, and per-worker eventfds to `WorkerProcess` and
`MasterDispatcher`.

#### SAL state

```python
class SharedAppendLog(object):
    """Single shared append-only log for all workers.

    Master appends message groups (containing all N workers' data) at
    write_cursor. Each worker reads at its own read_cursor.
    Cursors are in-process variables (not stored in the file).
    """
    def __init__(self, ptr, fd):
        self.ptr = ptr                  # mmap base (1 GB virtual = file size)
        self.fd = fd                    # backing file fd
        self.write_cursor = 0           # master only
```

Each worker holds its own `read_cursor` (an integer, not a shared object).
Read cursors advance by exactly the bytes the master wrote per message group.

#### `ipc.py` changes

New transport functions alongside existing `send_batch`/`receive_payload`.

**Message group layout** — the master writes one group per push, containing
all N workers' data in a single contiguous append:

```
[group_header: 64 bytes, cache-line aligned]
  u64  total_payload_size         // size of entire group (excl. 8-byte prefix)
  u64  lsn
  u32  num_workers
  u32  flags                      // FLAG_PUSH, FLAG_DDL_SYNC, etc.
  u32  target_id
  u32  reserved
  u32  worker_offsets[MAX_WORKERS] // byte offset of worker i's data within group
  u32  worker_sizes[MAX_WORKERS]   // byte size of worker i's data (0 = no data)

[worker_0_data: WAL-block encoded IPC message (control + schema + data)]
[worker_1_data: ...]
[worker_2_data: ...]
[worker_3_data: ...]
```

MAX_WORKERS is a compile-time constant (e.g., 64). The header is fixed-size
regardless of actual worker count to keep offset arithmetic simple.

```python
GROUP_HEADER_SIZE = 64 + 4 * MAX_WORKERS * 2   # offsets + sizes

@jit.dont_look_inside
def write_message_group(sal, target_id, lsn, flags, worker_bufs, num_workers):
    """Append a message group for all N workers into the SAL.

    worker_bufs[w] is a (base_ptr, size) pair or (NULL, 0) for empty.
    Master calls fdatasync after this returns, before signaling workers.
    """
    payload_size = GROUP_HEADER_SIZE
    for w in range(num_workers):
        payload_size += _align8(worker_bufs[w][1])

    total = 8 + _align8(payload_size)   # 8-byte size prefix + aligned payload

    # No growth check needed: file is ftruncate'd to SAL_MMAP_SIZE at
    # startup (sparse — blocks allocated on write).  If write_cursor
    # approaches SAL_MMAP_SIZE, master forces a coordinated checkpoint.

    # Write size prefix
    _write_u64(sal.ptr, sal.write_cursor, r_uint64(payload_size))

    # Write group header
    hdr_ptr = rffi.ptradd(sal.ptr, sal.write_cursor + 8)
    _write_group_header(hdr_ptr, lsn, flags, target_id, num_workers, worker_bufs)

    # Write per-worker data
    data_offset = GROUP_HEADER_SIZE
    for w in range(num_workers):
        wptr, wsz = worker_bufs[w]
        if wsz > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(hdr_ptr, data_offset)),
                rffi.cast(rffi.VOIDP, wptr),
                rffi.cast(rffi.SIZE_T, wsz),
            )
        data_offset += _align8(wsz)

    sal.write_cursor += total
```

`_encode_wire` is the encoding logic factored out of `serialize_to_memfd`:
control block + optional schema block + optional data block. Same WAL-block
wire format, no memfd.

`_parse_from_ptr(ptr, length)` is the decoding logic factored out of
`_recv_and_parse`: takes a raw pointer + length instead of an fd.

```python
@jit.dont_look_inside
def read_worker_message(sal_ptr, read_cursor, worker_id):
    """Read this worker's data from the next message group in the SAL.

    Returns (payload, group_advance) or (None, 0) if no message ready.
    The returned payload holds non-owning views into SAL memory.
    Caller advances read_cursor by group_advance when done.
    """
    msg_size = _read_u64(sal_ptr, read_cursor)
    if msg_size == 0:
        return None, 0
    hdr_ptr = rffi.ptradd(sal_ptr, read_cursor + 8)
    my_offset = _read_worker_offset(hdr_ptr, worker_id)
    my_size = _read_worker_size(hdr_ptr, worker_id)
    group_advance = 8 + _align8(intmask(msg_size))
    if my_size == 0:
        return None, group_advance
    my_ptr = rffi.ptradd(hdr_ptr, my_offset)
    payload = _parse_from_ptr(my_ptr, my_size)
    return payload, group_advance
```

#### `master.py` changes

`MasterDispatcher.__init__` gains `sal` (single SharedAppendLog),
`w2m_sals` (per-worker anonymous regions), and per-worker eventfds.

All fan_out methods switch from socket to SAL. The critical change: the
master encodes all workers' data into a single message group, fdatasyncs
once, then signals all workers:

```python
def fan_out_push(self, target_id, batch, schema):
    # 1. Classify + encode per-worker data into buffers
    worker_bufs = self._encode_per_worker(batch, schema, target_id)

    # 2. Write message group to SAL (one contiguous append)
    ipc.write_message_group(self.sal, target_id, self._next_lsn(),
                            ipc.FLAG_PUSH, worker_bufs, self.num_workers)
    for wb in worker_bufs:
        wb.free()

    # 3. Durability — one fdatasync, one NVMe flush command
    mmap_posix.fdatasync_c(self.sal.fd)

    # 4. Signal all workers (data is durable, safe to read)
    for w in range(self.num_workers):
        eventfd_ffi.eventfd_signal(self.m2w_efds[w])

    # 5. Collect ACKs (+ exchange relay)
    self._collect_acks_and_relay(schema)
```

Poll+receive loops use per-worker anonymous w2m regions:
```python
for w in range(self.num_workers):
    if w not in acked:
        payload = ipc.receive_from_sal(self.w2m_sals[w])
        if payload is not None:
            # process ...
            payload.close()   # advances w2m read cursor
if len(acked) < self.num_workers:
    eventfd_ffi.eventfd_wait_any(self.w2m_efds, timeout_ms=10)
```

`broadcast_ddl` encodes once into a message group with identical data for
all workers, fdatasyncs, then signals.

#### Streaming scan forwarding

`fan_out_scan` is replaced by `fan_out_scan_stream(target_id, schema,
client_fd)` which raw-forwards each worker's scan result directly to the
client without decoding, accumulating, or re-encoding:

```python
def fan_out_scan_stream(self, target_id, schema, client_fd):
    """Stream scan results from workers directly to client.

    Each worker's W2M output is already WAL-block encoded — the same
    wire format the client expects. The master reads raw bytes from
    each worker's W2M region and forwards them as separate framed
    messages. No decode, no ArenaZSetBatch allocation, no re-encode.
    """
    # 1. Send scan request to all workers via SAL
    ipc.write_message_group(self.sal, target_id, self._next_lsn(),
                            ipc.FLAG_SCAN, [None] * self.num_workers,
                            self.num_workers)
    mmap_posix.fdatasync_c(self.sal.fd)
    for w in range(self.num_workers):
        eventfd_ffi.eventfd_signal(self.m2w_efds[w])

    # 2. Forward each worker's raw result directly to client
    remaining = self.num_workers
    while remaining > 0:
        for w in range(self.num_workers):
            raw_ptr, raw_size = ipc.read_raw_from_sal(self.w2m_sals[w])
            if raw_ptr:
                # Raw forward: W2M bytes → client socket. Zero-copy.
                ipc_ffi.send_framed(client_fd, raw_ptr, raw_size)
                remaining -= 1

    # 3. Send sentinel so client knows scan is complete
    ipc.send_framed(client_fd, target_id, None, flags=ipc.FLAG_SCAN_DONE)
```

The master handles scan results in **O(1) memory** regardless of result
size. No `ArenaZSetBatch` allocation. No `append_batch`. No re-encode.

Per-worker scan results are ~1/N of the total (hash partitioning ≈ uniform).
For 10M total rows with 4 workers: ~260 MB per worker, well within the 1 GB
W2M region. The W2M region sizing concern (original Open Issue 2) is
eliminated: the per-worker share is always bounded, and the master never
accumulates.

Copy path comparison for a 10M-row scan:

| | Current (accumulate) | Streaming raw-forward |
|---|---|---|
| Master decode | 4 × 260 MB | **0** |
| Master accumulate | 1 × 1 GB | **0** |
| Master re-encode | 1 × 1 GB | **0** |
| Master peak memory | ~2 GB | **~0** |

`fan_out_scan` (returning `ArenaZSetBatch`) is kept for internal use but
is not called for client-facing scans when `dispatcher is not None`.

#### `worker.py` changes

`WorkerProcess.__init__` gains `sal_ptr` (shared mmap, read-only access),
`read_cursor` (integer), `w2m_sal`, and per-worker eventfds.

Main poll loop in `run()`:
```python
while True:
    payload, advance = ipc.read_worker_message(
        self.sal_ptr, self.read_cursor, self.worker_id)
    if payload is not None:
        should_exit = self._handle_request_from_sal(payload)
        self.read_cursor += advance
        if should_exit:
            return
    else:
        eventfd_ffi.eventfd_wait(self.m2w_efd, timeout_ms=1000)
```

Response sending via `ipc.send_to_sal(self.w2m_sal, ...)`.

#### Worker push hot path

The master has already called `fdatasync()` before signaling. The data is
durable when the worker reads it. The push handler just clones into the
existing memtable:

```python
def _handle_push_from_sal(self, payload, target_id, batch):
    """Ingest from SAL. Data is already durable — just clone and ingest."""
    # No fdatasync here. The master fsynced before signaling us.
    # The SAL data is durable on disk and also in the page cache (fast read).

    # Ingest — clone from SAL mmap into owned memtable batch.
    # upsert_batch calls to_sorted() which creates a new owned batch,
    # copying column data and relocating blob arena strings. After this,
    # the memtable has no references to the SAL.
    family = self.engine.registry.get_by_id(target_id)
    effective = ingest_to_family(family, batch)
    family.store.flush()

    # read_cursor is advanced by the caller after this returns.

    # ... rest of push handling (pending_deltas, evaluate_dag) unchanged ...
```

There is no `slice_view`, no shared blob_arena, no per-push `mmap()`, no
mmap region tracking, and no worker-side fsync. The existing `MemTable`,
`PersistentTable`, and storage layer are completely unchanged.

#### Why clone is the right trade-off

The plan originally proposed zero-copy memtable views into WAL-backed mmap
regions. Investigation revealed three problems with that approach:

1. **mmap accumulation**: each push creates a new `mmap()` region. These
   accumulate until all 64 partitions flush past a WAL offset, risking
   `vm.max_map_count` exhaustion (default 65530).

2. **Shared blob_arena accounting**: `slice_view` shares the parent's blob
   arena across 64 partition slices. `_total_bytes()` counts blob bytes 64×,
   causing premature `MemTableFullError`.

3. **Sort compatibility**: memtable runs require PK-sorted data for binary
   search (`_lower_bound`). WAL entries arrive partition-sorted (not globally
   PK-sorted). Creating sorted views without a copy is not possible; the
   existing `to_sorted()` already produces an owned copy.

The clone approach trades one sequential memcpy (~10 μs for 1K rows, ~1 ms for
100K rows) for zero mmap syscalls, zero accounting bugs, and zero changes to
the storage layer. The file-backed SAL already eliminated `write()` and reduced
fsyncs from 256 to 1 — the remaining clone is in L1/L2 cache and is negligible
compared to the disk fsync.

#### Back-pressure

The SAL file is pre-sized to `SAL_MMAP_SIZE` (1 GB sparse). Unbounded
cursor growth is prevented by protocol structure: the master sends one push
(plus optional preloaded exchange messages) per fan_out_push, then blocks
waiting for ACKs. Workers process and advance before the master sends the
next push. Between checkpoints, physical block usage grows by the total
volume of unflushed data — the same total as the current 64 per-partition
WALs, just consolidated into one file.

If `write_cursor` approaches `SAL_MMAP_SIZE` (which implies ~1 GB of
unflushed WAL data — a pathological case), the master forces a checkpoint
when all workers have flushed: reset cursor to 0 (see 2c).

#### Socket keepalive

The SEQPACKET socketpair is kept alive for `check_workers()` in master
(crash detection via `os.waitpid`) and for worker-side master disconnect
detection (POLLHUP on a separate monitoring fd). No data flows over the
socketpair in steady state.

### 2b: Gather-encode into SAL (SoA-aware scatter)

The basic SAL transport (2a) still copies column data three times on the
scatter path: (1) gather from source batch into N intermediate sub-batches,
(2) encode each sub-batch into a `wire_buf`, (3) memcpy `wire_buf` into SAL.

Replace with a single gather-encode pass that writes directly into the SAL.
During the classify pass, classify by exact partition (256-way) instead of by
worker (N-way). Group partitions by owning worker. Within each worker's index
list, sort indices by partition so the encoded data arrives partition-sorted
with a boundary table:

```
Source batch (SoA, M rows)
    │
    ├─ classify rows by partition ─── O(M), builds partition index lists
    │
    ├─ sal_get_write_ptr(w0, max_size)
    │   └── gather_encode ─── ONE COPY:
    │        size prefix (8 bytes)
    │        control block + schema block
    │        partition boundary table (extra WAL region)
    │        for each column:
    │          gather src[indices[0]], src[indices[1]], ... → SAL region
    │        copy blob data for string columns
    │        sal_commit(actual_size)
    │
    ├─ sal_get_write_ptr(w1) + gather_encode ...
    └─ ...
```

#### SAL direct-write API

```python
def sal_get_write_ptr(sal, max_size):
    """Get raw pointer into SAL at write cursor."""
    # No growth check: file is ftruncate'd to SAL_MMAP_SIZE at startup.
    # Return pointer past the 8-byte size prefix (caller writes payload)
    return rffi.ptradd(sal.ptr, sal.write_cursor + 8)

def sal_commit(sal, actual_size):
    """Finalize a direct write. Backfills size prefix, advances cursor.
    Does NOT signal or fsync — caller does that after committing all data."""
    _write_u64(sal.ptr, sal.write_cursor, r_uint64(actual_size))
    sal.write_cursor += 8 + _align8(actual_size)
```

This is the SAL equivalent of the ring's `reserve`/`commit` — without wrap
detection, skip markers, or contiguous-space checks. The SAL is append-only;
space is always available (the file is pre-sized to `SAL_MMAP_SIZE`). The
master calls `fdatasync_c` after `sal_commit`, then signals workers via
eventfd.

#### `wal_columnar.py` new function

```python
def encode_batch_gather(dest_ptr, max_size, schema, src_batch, indices,
                        boundaries, lsn, table_id):
    """Gather-encode selected rows from src_batch into dest_ptr.

    Same WAL block layout as encode_batch_append. Instead of sequential
    column copies, reads src[indices[r]] for each row r and writes
    sequentially into dest. An extra region at the end holds the
    partition boundary table: u32[len(boundaries)].

    Returns actual encoded size, or -1 if max_size exceeded.
    """
```

#### `ipc.py` / `exchange.py` new function

```python
def scatter_encode_to_sal(sal, batch, num_workers, assignment, schema,
                          target_id, lsn, flags):
    """Classify by partition + gather-encode all workers into single SAL."""
    n = batch.length()

    # Pass 1: classify rows by partition (256-way)
    part_lists = [newlist_hint(0) for _ in range(256)]
    for i in range(n):
        p = _partition_for_key(batch._read_pk_lo(i), batch._read_pk_hi(i))
        part_lists[p].append(i)

    # Pass 2: encode per-worker data into temporary buffers
    worker_bufs = []
    for w in range(num_workers):
        indices = newlist_hint(n // num_workers)
        boundaries = newlist_hint(65)  # up to 64 partitions + sentinel
        for p in range(assignment.start(w), assignment.end(w)):
            boundaries.append(len(indices))
            for idx in part_lists[p]:
                indices.append(idx)
        boundaries.append(len(indices))  # sentinel

        if len(indices) == 0:
            worker_bufs.append((lltype.nullptr(rffi.CCHARP.TO), 0))
            continue

        buf = _encode_worker_data(schema, batch, indices, boundaries,
                                  target_id, flags)
        worker_bufs.append((buf.base_ptr, buf.offset))

    # Pass 3: write single message group into SAL
    ipc.write_message_group(sal, target_id, lsn, flags,
                            worker_bufs, num_workers)

    # Pass 4: free per-worker encode buffers
    for wb in worker_bufs:
        if wb[0]:
            _free_buf(wb[0])
```

`_estimate_encoded_size`: for fixed-width columns, exact. For string columns,
add `batch.blob_arena.offset` as upper bound.

New IPC flag: `FLAG_PARTITION_SORTED = r_uint64(1 << 53)`. When set, the data
WAL block contains an extra region: `u32[num_boundaries]` — row-index boundaries
for partition-sorted data.

#### `master.py` changes

`fan_out_push` and `fan_out_ingest` call `scatter_encode_to_sal` instead of
`repartition_batch` + N × `send_batch`. After the encode, master calls
`fdatasync_c(sal.fd)` once, then signals all workers via eventfd.

### 2c: Single unified WAL (file-backed SAL)

The single SAL file IS the WAL for all workers. The master appends
gather-encoded message groups into the file-backed mmap and calls
`fdatasync()` before signaling workers. On crash, the SAL file contains all
message groups since the last checkpoint.

No separate `WorkerWAL` class. No `append_and_map`. No per-push `mmap()`.
No worker-side fsync. The SAL and its backing file are the entire WAL
implementation.

#### `mmap_posix.py` changes

`fdatasync_c`, `mmap_anonymous_shared`, `madvise_dontneed` (see Step 1).

#### `worker.py` changes

No SAL fd stored — the worker doesn't fsync. It only reads from the
inherited mmap.

#### Recovery

On worker startup (or crash recovery), each worker scans the shared SAL
file and extracts its own data from message group headers:

```python
def recover_from_sal(sal_path, engine, assignment, worker_id):
    """Replay a file-backed SAL after crash.

    The SAL is an append-only sequence of message groups. Each group has an
    8-byte size prefix followed by a group header (with per-worker offsets)
    and per-worker WAL-block encoded payloads. Scan forward from offset 0,
    extracting this worker's data and replaying blocks with LSN > last flushed.

    After a cursor-reset checkpoint, old-epoch groups may exist at higher
    offsets. These are skipped by the LSN filter (all old-epoch LSNs <=
    last_flushed_lsn because checkpoint requires all partitions flushed).
    """
    fd = rposix.open(sal_path, os.O_RDONLY, 0)
    try:
        file_size = intmask(mmap_posix.fget_size(fd))
        if file_size == 0:
            return
        ptr = mmap_posix.mmap_file(fd, file_size,
                                    prot=mmap_posix.PROT_READ,
                                    flags=mmap_posix.MAP_SHARED)
        try:
            _scan_and_replay(ptr, file_size, engine, assignment, worker_id)
        finally:
            mmap_posix.munmap_file(ptr, file_size)
    finally:
        rposix.close(fd)


def _scan_and_replay(ptr, file_size, engine, assignment, worker_id):
    """Sequential scan of append-only SAL. O(file_size) header reads."""
    offset = 0
    flushed_lsn = _last_flushed_lsn(engine, assignment, worker_id)
    while offset + 8 <= file_size:
        msg_size = _read_u64(ptr, offset)
        if msg_size == 0:
            break  # zero padding or end of valid data
        if offset + 8 + intmask(msg_size) > file_size:
            break  # truncated group — crash during write

        group_ptr = rffi.ptradd(ptr, offset + 8)

        # Extract this worker's data from the message group
        my_offset = _read_worker_offset(group_ptr, worker_id)
        my_size = _read_worker_size(group_ptr, worker_id)

        if my_size > 0:
            my_ptr = rffi.ptradd(group_ptr, my_offset)
            valid, lsn = _try_parse_and_validate(my_ptr, my_size)
            if not valid:
                break  # corrupt block — crash during write, stop here
            if lsn > flushed_lsn:
                _replay_block(my_ptr, my_size, engine,
                              assignment, worker_id)
            # else: old-epoch data (post cursor-reset) — skip

        offset += 8 + _align8(intmask(msg_size))
```

Key properties:
- Forward-only sequential scan. No circular scanning.
- Each worker extracts its own data via group header offsets.
- LSN filter handles cursor-reset epochs: after checkpoint, old groups
  have LSN <= flushed_lsn and are skipped. New groups are replayed.
- Scan cost: O(file_size) header reads. For 100 MB file: ~1 ms.

#### Checkpoint / SAL reset

The SAL can be reset when ALL workers have flushed ALL partition memtables
to shards. Checkpoint is cursor-reset only — no file operations.

**Worker side**: after each flush cycle, the worker checks whether all its
partitions have flushed since the last cursor reset. If so, it sets
`FLAG_CHECKPOINT` on its next ACK:

```python
# In worker ACK path:
ack_flags = 0
if self._all_partitions_flushed_since_reset():
    ack_flags |= ipc.FLAG_CHECKPOINT
    self.read_cursor = 0       # reset own read cursor
    self._mark_checkpoint()    # record epoch boundary
ipc.send_to_sal(self.w2m_sal, target_id, None, flags=ack_flags)
```

**Master side**: in `_collect_acks_and_relay`, the master tracks which
workers have checkpointed. When ALL N workers have sent `FLAG_CHECKPOINT`,
the master resets its write cursor:

```python
# In _collect_acks_and_relay:
if payload.flags & ipc.FLAG_CHECKPOINT:
    checkpoint_count += 1
# After all ACKs collected:
if checkpoint_count == self.num_workers:
    self.sal.write_cursor = 0    # reset — one integer assignment
```

**What this does NOT do:**
- No `ftruncate`. No `fallocate`. No `punch_hole`. The file stays at
  `SAL_MMAP_SIZE` (1 GB sparse). After cursor reset, the master overwrites
  from offset 0.
- No separate checkpoint message. The flag piggybacks on the existing ACK.
- No coordination race. The master processes the ACK (with checkpoint flag)
  before writing the next message group. By the time the master writes at
  offset 0, all workers have already reset their read cursors to 0.

**Disk space**: the SAL file is always `SAL_MMAP_SIZE` apparent size (sparse).
Physical block usage equals the high-water mark of written data. For typical
workloads: ~100 MB of physical blocks, matching the current 64 per-partition
WALs. After cursor reset, subsequent writes reuse already-allocated blocks.

**Recovery correctness after cursor reset**: new-epoch groups (written after
reset at lower offsets) have LSN > flushed_lsn → replayed. Old-epoch groups
(at higher offsets, not yet overwritten) have LSN <= flushed_lsn → skipped
by LSN filter. No sentinel or epoch marker needed — the LSN is already in
every WAL block header.

#### `table.py` changes

`PersistentTable` no longer creates per-partition WAL files in multi-worker
mode. The WAL writer is only active in single-process mode:

```python
if self._has_wal:
    # Single-process: per-partition WAL (existing path)
    self.wal_writer.append_batch(lsn, self.table_id, batch)
    self.memtable.upsert_batch(batch)
else:
    # Multi-worker: file-backed SAL handles durability externally
    self.memtable.upsert_batch(batch)
```

### 2d: Shared Exchange Arena

The SAL channels carry push data, scan results, ACKs, and DDL sync — all
bounded-size, request/response messages. Mid-circuit exchange data is
fundamentally different: unbounded, potentially large (join fan-out over
millions of rows), and N-to-N (every worker sends to every other worker).

In the current architecture, the master acts as both barrier coordinator and
data relay: it receives N exchange batches, repartitions the merged data N
ways, and sends N relay responses. This creates three problems:

1. **Capacity**: a single worker's exchange output can exceed SAL capacity.
2. **Throughput**: sequential repartition on the master while N workers idle.
3. **Allocation**: N temporary batches created, encoded, and freed on master.

The shared exchange arena eliminates the master from the exchange data path
entirely. Workers scatter-encode directly into pre-fork shared memory. Other
workers read and clone into owned batches. The master only coordinates barriers.

#### Allocation (before fork, in `main.py`)

```python
EXCHANGE_SLOT_SIZE = 1 << 30   # 1 GB virtual per worker slot
# Single arena — no double-buffering needed (see "Why single arena is safe")
exchange_arena = mmap_posix.mmap_anonymous_shared(
    EXCHANGE_SLOT_SIZE * num_workers
)
# Total virtual: EXCHANGE_SLOT_SIZE * num_workers
# = 4 GB for 4 workers (0.003% of 128 TB x86-64 VA space)
# Physical at rest: 0 (demand-paged — pages allocated on first write)
# Physical during exchange: proportional to actual data written
```

N slots per arena, one per worker. Worker Wi writes to slot i; all workers
read from all slots. The barrier between write and read phases provides
memory ordering.

#### Why 1 GB slots and demand paging

On 64-bit Linux, virtual address space (128 TB) is nearly free. Physical
pages are allocated only when first written (demand paging). A 1 GB
mapping that holds 10 MB of exchange data consumes 10 MB of physical RAM,
not 1 GB. This eliminates overflow as a concern: 1 GB per slot holds ~10M
rows of a 10-column I64 schema. Any exchange delta exceeding 1 GB implies
the system is memory-bound regardless.

Page table overhead: for 10 MB of touched pages (2560 × 4 KB pages), the
three-level PTE hierarchy consumes ~20 KB. Negligible.

#### Why single arena is safe (no double-buffering)

The consumed barrier guarantees all workers have finished reading before
any worker starts the next exchange. Each worker calls
`madvise(MADV_DONTNEED)` on its own slot at the START of the next exchange
(before writing new data). On `MAP_SHARED|MAP_ANONYMOUS` memory,
`MADV_DONTNEED` releases physical pages and zeroes them — a single fast
syscall with no I/O.

```
Exchange N:    write → barrier → read+clone → consumed
Exchange N+1:  MADV_DONTNEED(my_slot) → write → barrier → read+clone → consumed
```

Between step N's "consumed" and step N+1's `MADV_DONTNEED`, no worker
touches the arena. The consumed barrier from the previous exchange
guarantees no readers remain. No double-buffering needed.

Physical memory follows a sawtooth pattern: rises during exchange
(proportional to actual data), drops to zero after `MADV_DONTNEED`.
No accumulation across exchanges, even with varying exchange sizes.

#### Arena slot layout

Each worker's slot contains N sub-batches (one per destination worker),
encoded as standard WAL blocks:

```
Slot header (64 bytes, cache-line aligned):
  uint32  sub_batch_count        // N (num_workers)
  uint32  total_bytes_written    // actual data size in slot
  uint32  offsets[MAX_WORKERS]   // byte offset of sub-batch for dest Wj
  uint32  sizes[MAX_WORKERS]     // byte size of sub-batch for dest Wj

Sub-batch for W0:  [WAL block: header + directory + column data]
Sub-batch for W1:  [WAL block: ...]
...
Sub-batch for W(N-1): [WAL block: ...]
```

MAX_WORKERS is a compile-time constant (e.g., 64). The header is fixed-size
regardless of actual worker count to keep offset arithmetic simple.

Readers compute `arena_base + Wi * SLOT_SIZE + offsets[my_id]` and call
`decode_batch_from_ptr(ptr, sizes[my_id], schema)` — one pointer add, one
non-owning decode, followed by `clone()` into an owned batch.

#### Protocol flags

```python
FLAG_EXCHANGE_READY    = r_uint64(1 << 54)  # worker → master: slot written
FLAG_EXCHANGE_BARRIER  = r_uint64(1 << 55)  # master → workers: all slots ready
FLAG_EXCHANGE_CONSUMED = r_uint64(1 << 56)  # worker → master: reads complete
```

These are tiny control-only messages on the SAL channels (no data, no schema).
Each is a single CONTROL_SCHEMA row (~100 bytes encoded).

#### Exchange flow

```
Worker Wi hits exchange operator:
  1. Resolve shard columns locally:
       shard_cols = program_cache.get_join_shard_cols(view_id, source_id)
       if not shard_cols:
           shard_cols = program_cache.get_shard_cols(view_id)
  2. Repartition exchange_batch into N sub-batches:
       sub_batches = repartition_batch(batch, shard_cols, num_workers, assignment)
  3. For each dest Wj:
       encode_batch_append(slot_buf, schema, 0, view_id, sub_batches[j])
       Record offset/size in slot header
     Free sub_batches
  4. Signal master: FLAG_EXCHANGE_READY (tiny msg on w2m SAL, carries view_id)

Master (_collect_and_barrier):
  5. Collect N ready signals from w2m SALs
  6. Broadcast FLAG_EXCHANGE_BARRIER to all m2w SALs

Worker Wj after barrier:
  7. For each source Wi:
       ptr = arena_base + Wi * SLOT_SIZE + offsets_in_slot_i[Wj]
       sub = decode_batch_from_ptr(ptr, sizes_in_slot_i[Wj], schema)
       result.append_batch(sub)  // copy into owned batch
       sub.free()                // non-owning: just nulls pointers
  8. Signal master: FLAG_EXCHANGE_CONSUMED (tiny msg on w2m SAL)
  9. Continue DAG evaluation with result

Master:
  10. Collect N consumed signals
```

#### `WorkerExchangeHandler` changes

```python
def do_exchange(self, view_id, batch, source_id=0):
    # Preloaded case: unchanged
    if view_id in self._stash:
        result = self._stash[view_id]
        del self._stash[view_id]
        return result

    schema = batch._schema
    shard_cols = self._program_cache.get_join_shard_cols(view_id, source_id)
    if len(shard_cols) == 0:
        shard_cols = self._program_cache.get_shard_cols(view_id)

    # 0. Release stale pages from previous exchange (single syscall, no I/O).
    #    Safe: consumed barrier from the previous exchange guarantees no
    #    readers remain on our slot.
    mmap_posix.madvise_dontneed(self._my_slot_ptr, EXCHANGE_SLOT_SIZE)

    # 1. Repartition locally → N sub-batches
    sub_batches = repartition_batch(
        batch, shard_cols, self._num_workers, self._assignment
    )

    # 2. Encode each sub-batch into our arena slot
    _write_exchange_slot(self._my_slot_ptr, sub_batches, schema,
                         self._num_workers, view_id)
    for sb in sub_batches:
        if sb is not None:
            sb.free()

    # 3. Signal ready, wait for barrier
    ipc.send_to_sal(self._w2m_sal, view_id, None,
                    flags=FLAG_EXCHANGE_READY)
    eventfd_ffi.eventfd_wait(self._m2w_sal.efd, timeout_ms=30000)
    barrier = ipc.receive_from_sal(self._m2w_sal)
    barrier.close()

    # 4. Read our partition from all workers' slots.
    #    Pre-size result to exact capacity (one scan for row counts,
    #    one allocation, zero reallocs).
    total_rows = 0
    for src_w in range(self._num_workers):
        total_rows += _slot_row_count(self._arena_base, src_w,
                                      self._worker_id)
    result = ArenaZSetBatch(schema, initial_capacity=max(total_rows, 8))
    for src_w in range(self._num_workers):
        sub = _read_exchange_sub(self._arena_base, src_w,
                                 self._worker_id, schema)
        if sub is not None and sub.length() > 0:
            result.append_batch(sub)
            sub.free()   # non-owning: just nulls pointers

    # 5. Signal consumed
    ipc.send_to_sal(self._w2m_sal, view_id, None,
                    flags=FLAG_EXCHANGE_CONSUMED)

    return result
```

#### `master.py` changes

`_collect_acks_and_relay` is replaced by `_collect_and_barrier`:

```python
def _collect_and_barrier(self):
    """Coordinate exchange barriers. No data relay."""
    exchange_ready = {}    # view_id -> count
    while len(poll_fds) > 0:
        # ... poll w2m SALs ...
        if payload.flags & FLAG_EXCHANGE_READY:
            vid = payload.target_id
            exchange_ready[vid] = exchange_ready.get(vid, 0) + 1
            payload.close()
            if exchange_ready[vid] == self.num_workers:
                # All workers ready — broadcast barrier
                for w in range(self.num_workers):
                    ipc.send_to_sal(self.m2w_sals[w], vid, None,
                                    flags=FLAG_EXCHANGE_BARRIER)
                del exchange_ready[vid]
                # Wait for consumed signals
                consumed = 0
                while consumed < self.num_workers:
                    for w in range(self.num_workers):
                        p = ipc.receive_from_sal(self.w2m_sals[w])
                        if p and p.flags & FLAG_EXCHANGE_CONSUMED:
                            consumed += 1
                            p.close()
                # No arena toggle needed — workers call MADV_DONTNEED
                # on their own slots at the start of the next exchange.
        elif not (payload.flags & FLAG_EXCHANGE):
            # ACK — remove from poll set
            payload.close()
            ...
```

The existing `_relay_exchange` method and `relay_scatter` import are deleted.

#### Why arena reuse is safe

After step 8, each worker has consumed its exchange input. The `append_batch`
calls in step 7 deep-copy the data into owned batches. By the time the next
exchange hits (step 1 again), all views from the previous exchange are freed.

Each worker calls `madvise(MADV_DONTNEED)` on its own slot at the start of
step 1, releasing physical pages and zeroing the memory. No double-buffering
is needed — the consumed barrier guarantees no readers remain.

#### Cost comparison: exchange path

Per exchange barrier, 4 workers, 10-column I64 schema, ~25 MB total:

| | Master relay (current) | Shared arena |
|---|---|---|
| Master data copies | 3× (read + repartition + write) | **0** |
| Worker encode | 1× (send to master) | 1× (scatter-encode to arena) |
| Worker decode | 1× clone from relay response | 1× clone from arena |
| SAL/transport messages | 8 large (4 exchange + 4 relay) | **12 signals** (~1.2 KB total) |
| Master CPU during exchange | O(total_rows) repartition | **~0** (eventfd signals only) |
| Worker idle time during relay | 100% (blocked on master) | **~0** (parallel scatter/gather) |
| Capacity constraint | message must fit in SAL | **none** (1 GB demand-paged slot) |
| Temporary batch allocs on master | N batches | **0** |
| Physical memory at rest | 256 MB (fixed alloc) | **0** (demand-paged + MADV_DONTNEED) |

### Files changed (Step 2, all sub-steps)
- `gnitz/server/main.py`: single SAL + per-worker w2m + eventfd + exchange
  arena allocation before fork, pass to workers
- `gnitz/server/ipc.py`: `write_message_group`, `read_worker_message`,
  `_parse_from_ptr`, `_encode_wire`, `scatter_encode_to_sal`,
  `_encode_worker_data`, `_estimate_encoded_size`, `FLAG_PARTITION_SORTED`,
  `FLAG_EXCHANGE_READY`, `FLAG_EXCHANGE_BARRIER`, `FLAG_EXCHANGE_CONSUMED`,
  `FLAG_CHECKPOINT`, `FLAG_SCAN_DONE`, `read_raw_from_sal`;
  `SharedAppendLog` class (single file)
- `gnitz/server/master.py`: fan_out/push methods use single SAL with
  `scatter_encode_to_sal` + `fdatasync_c` + eventfd signal;
  `fan_out_scan_stream` raw-forwards per-worker scan results to client;
  `_collect_acks_and_relay` handles `FLAG_CHECKPOINT` (cursor reset when all
  workers checkpointed); `_collect_and_barrier` replaces `_relay_exchange`
  (barrier coordination only, no data relay); `_relay_exchange` deleted
- `gnitz/server/worker.py`: main loop reads from shared SAL mmap; push path
  uses existing `ingest_to_family` / `upsert_batch` (no worker-side fsync);
  `WorkerExchangeHandler` rewritten to use shared arena (local repartition +
  scatter-encode + barrier + clone-from-arena);
  checkpoint: set `FLAG_CHECKPOINT` on ACK when all partitions flushed
- `gnitz/server/eventfd_ffi.py`: `eventfd_wait_any`
- `gnitz/storage/mmap_posix.py`: `mmap_anonymous_shared`,
  `fdatasync_c`, `madvise_dontneed`
- `gnitz/storage/wal_columnar.py`: `encode_batch_gather`
- `gnitz/storage/table.py`: `PersistentTable` no longer creates per-partition
  WAL in multi-worker mode (WAL writer only active in single-process mode)
- `gnitz/dbsp/ops/exchange.py`: no changes (repartition_batch reused by workers)

### Test
- `make prove-eventfd` — eventfd unit tests
- `make prove-storage_comprehensive` — unchanged memtable produces identical
  query results
- `make prove-master_worker` — exercises fan_out_push, arena exchange, scan,
  seek with 4 workers
- `make prove-exchange` — arena exchange with varying batch sizes,
  multi-exchange DAGs, skewed partitions
- `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/` — E2E

---

## Step 3: Length-Prefixed SOCK_STREAM (External IPC)

Drop memfd+SCM_RIGHTS from the external client path. Switch the external
client socket from `SOCK_SEQPACKET` to `SOCK_STREAM` with a 4-byte
length-prefix framing protocol.

### Why SOCK_STREAM instead of SEQPACKET

SEQPACKET preserves message boundaries but imposes a per-message size limit
of `sk_sndbuf - 32` (~208 KB default, ~8 MB with `SO_SNDBUF=4MB`). Scan
results can exceed any fixed limit. Chunking a single result into multiple
SEQPACKET messages requires `slice_view` (which doesn't exist and has
shared blob_arena hazards), `FLAG_MORE` reassembly on the client, and
per-chunk re-encoding — substantial complexity for what is fundamentally a
"send big blob in pieces" problem.

SOCK_STREAM has **no message size limit**. Adding a trivial 4-byte length
prefix provides the same framing guarantee as SEQPACKET. The protocol is
strictly serial per connection. For scan results in multi-worker mode, the
master streams N per-worker results as separate framed messages followed by
a `FLAG_SCAN_DONE` sentinel (see Step 2a "Streaming scan forwarding"). The
client reads framed messages in a loop until it sees the sentinel.

This eliminates `slice_view`, `FLAG_MORE`, `_send_chunked`, per-chunk
encoding, and client-side reassembly logic — replaced by 8 lines of C.

### Wire protocol

```
Both directions (client ↔ server):
  [u32 payload_length (little-endian)][payload bytes]

payload_length = 0 is reserved (connection close)
payload bytes  = control WAL block + optional schema WAL block + optional data WAL block
                 (same format as today's memfd contents — wire format unchanged)
```

### `ipc_ffi.py` changes

Switch from `SOCK_SEQPACKET` to `SOCK_STREAM` for the listen socket:

```c
int gnitz_server_create_stream(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    unlink(path);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    socklen_t len = offsetof(struct sockaddr_un, sun_path) + strlen(path) + 1;
    if (bind(fd, (struct sockaddr *)&addr, len) < 0) { close(fd); return -1; }
    if (listen(fd, 128) < 0) { close(fd); return -1; }
    return fd;
}
```

New framed send/recv functions:

```c
/* Send a length-prefixed message over SOCK_STREAM.
   Handles partial writes internally. */
int gnitz_send_framed(int fd, const void *buf, uint32_t len) {
    uint32_t hdr = len;  /* native little-endian on x86 */
    if (gnitz_send_all(fd, &hdr, 4) < 0) return -1;
    if (len > 0 && gnitz_send_all(fd, buf, len) < 0) return -1;
    return 0;
}

/* Receive a length-prefixed message. Allocates buffer, caller frees.
   Returns payload size, or -1 on error, 0 on clean close. */
ssize_t gnitz_recv_framed(int fd, char **out_buf) {
    uint32_t hdr;
    ssize_t n = gnitz_recv_all(fd, &hdr, 4);
    if (n <= 0) { *out_buf = NULL; return n; }
    if (hdr == 0) { *out_buf = NULL; return 0; }
    char *buf = (char *)malloc(hdr);
    if (!buf) return -1;
    n = gnitz_recv_all(fd, buf, hdr);
    if (n < (ssize_t)hdr) { free(buf); *out_buf = NULL; return -1; }
    *out_buf = buf;
    return (ssize_t)hdr;
}

/* Internal helpers: loop on partial reads/writes */
static ssize_t gnitz_send_all(int fd, const void *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, (const char *)buf + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return (ssize_t)sent;
}

static ssize_t gnitz_recv_all(int fd, void *buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(fd, (char *)buf + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return (ssize_t)got;  /* peer closed */
        got += (size_t)n;
    }
    return (ssize_t)got;
}
```

### `ipc.py` new functions

```python
@jit.dont_look_inside
def send_framed(sock_fd, target_id, zbatch, status=0, error_msg="",
                client_id=0, schema=None, flags=0,
                seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
    """Encode + send as length-prefixed message over SOCK_STREAM. No memfd."""
    wire_buf = _encode_wire(target_id, client_id, zbatch,
                            schema if schema is not None else (
                                zbatch._schema if zbatch is not None else None),
                            flags, seek_pk_lo, seek_pk_hi, seek_col_idx,
                            status, error_msg)
    try:
        ret = ipc_ffi.send_framed(sock_fd, wire_buf.base_ptr, wire_buf.offset)
        if ret < 0:
            raise errors.ClientDisconnectedError("send_framed failed")
    finally:
        wire_buf.free()

@jit.dont_look_inside
def recv_framed(sock_fd):
    """Receive a length-prefixed message from SOCK_STREAM. Returns IPCPayload."""
    ptr, length = ipc_ffi.recv_framed(sock_fd)
    if length <= 0:
        raise errors.ClientDisconnectedError("recv_framed: connection closed")
    payload = _parse_from_ptr(ptr, length)
    payload._recv_buf = ptr   # raw alloc, freed in payload.close()
    return payload
```

`_encode_wire` is the encoding logic factored out of `serialize_to_memfd`:
builds the wire_buf (control block + optional schema block + optional data
block). Same WAL-block wire format, no memfd. Used by both `send_framed`
(external clients) and `send_to_sal` (internal SAL transport from Step 2).

### `executor.py` changes

`ServerExecutor._handle_client` switches from `ipc.receive_payload(client_fd)`
+ `ipc.send_batch(client_fd, ...)` to `ipc.recv_framed(client_fd)` +
`ipc.send_framed(client_fd, ...)`.

No `_send_chunked`. No `FLAG_MORE`. Every response is a single encode +
single framed send, regardless of result size.

### Rust client changes

`rust_client/gnitz-protocol/src/transport.rs`:
- `connect`: switch from `SOCK_SEQPACKET` to `SOCK_STREAM`.
- `send_memfd` → `send_framed`: encode to buffer, write 4-byte length prefix
  + payload bytes. No `memfd_create`, no `mmap`, no `sendmsg(SCM_RIGHTS)`.
- `recv_memfd` → `recv_framed`: read 4-byte length prefix, allocate buffer,
  `recv_all` payload bytes. No `recvmsg(SCM_RIGHTS)`, no `fstat`, no `mmap`.

```rust
pub fn send_framed(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    let len = (data.len() as u32).to_le_bytes();
    send_all(sock_fd, &len)?;
    if !data.is_empty() {
        send_all(sock_fd, data)?;
    }
    Ok(())
}

pub fn recv_framed(sock_fd: RawFd) -> Result<Vec<u8>, ProtocolError> {
    let mut len_buf = [0u8; 4];
    recv_all(sock_fd, &mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut buf = vec![0u8; len];
    recv_all(sock_fd, &mut buf)?;
    Ok(buf)
}
```

### Backward compatibility

Breaking change for the Rust/Python client. Version both sides together. No
mixed-version support needed (gnitz is not yet released).

### Files changed
- `gnitz/server/ipc.py`: `send_framed`, `recv_framed`, `_encode_wire` (factored
  from `serialize_to_memfd`)
- `gnitz/server/ipc_ffi.py`: `gnitz_server_create_stream`, `gnitz_send_framed`,
  `gnitz_recv_framed`, `gnitz_send_all`, `gnitz_recv_all`
- `gnitz/server/executor.py`: client handling switched to `send_framed`/
  `recv_framed`; multi-worker scans call `fan_out_scan_stream(client_fd)`
  instead of `fan_out_scan` + `send_batch`
- `rust_client/gnitz-protocol/src/transport.rs`: drop memfd, switch to
  `SOCK_STREAM` + length-prefixed framing; scan responses: read framed
  messages in loop until `FLAG_SCAN_DONE` sentinel

### Deleted (vs. original plan)
- `slice_view` in `batch.py` — not needed
- `_send_chunked` — not needed
- `FLAG_MORE` — not needed
- `setsockopt(SO_SNDBUF)` / `sysctl wmem_max` — not needed (STREAM has no
  per-message size limit)
- `gnitz_set_sndbuf` in `ipc_ffi.py` — not needed

### Test
- `make prove-ipc_comprehensive` — round-trip encode/decode for all message types
- `make prove-server` — single-worker server + client interaction
- `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/` — E2E

---

## Step 4: `uring_ffi.py` — io_uring FFI

New file: `gnitz/server/uring_ffi.py`

Inline C via `separate_module_sources`. Raw syscalls (no liburing dependency).
Syscall numbers: `__NR_io_uring_setup` (425), `__NR_io_uring_enter` (426),
`__NR_io_uring_register` (427) on x86_64.

### C data structure

```c
struct gnitz_uring {
    int ring_fd;

    // SQ ring (kernel-shared mmap)
    unsigned *sq_head;      // kernel-updated
    unsigned *sq_tail;      // user-updated
    unsigned *sq_ring_mask;
    unsigned *sq_array;     // indirection array
    struct io_uring_sqe *sqes;  // separate mmap
    unsigned  sq_entries;

    // CQ ring (kernel-shared mmap)
    unsigned *cq_head;      // user-updated
    unsigned *cq_tail;      // kernel-updated
    unsigned *cq_ring_mask;
    struct io_uring_cqe *cqes;
    unsigned  cq_entries;
};
```

### C API

| Function | Description |
|---|---|
| `gnitz_uring_create(entries)` | `io_uring_setup` + mmap SQ/CQ/SQEs. Returns `gnitz_uring*`. |
| `gnitz_uring_destroy(ring)` | munmap + close. |
| `gnitz_uring_prep_accept(ring, fd, user_data)` | Prep `IORING_OP_ACCEPT` SQE. Multishot via `IOSQE_ACCEPT_MULTISHOT` flag. |
| `gnitz_uring_prep_recv(ring, fd, buf, len, user_data)` | Prep `IORING_OP_RECV` SQE. |
| `gnitz_uring_prep_recvmsg(ring, fd, msghdr, user_data)` | Prep `IORING_OP_RECVMSG` SQE. |
| `gnitz_uring_prep_send(ring, fd, buf, len, user_data)` | Prep `IORING_OP_SEND` SQE. |
| `gnitz_uring_submit(ring)` | `io_uring_enter(ring_fd, to_submit, 0, 0)`. Returns count. |
| `gnitz_uring_submit_and_wait(ring, min_complete)` | `io_uring_enter(ring_fd, to_submit, min_complete, IORING_ENTER_GETEVENTS)`. |
| `gnitz_uring_drain(ring, user_data_out, res_out, flags_out, max)` | Read CQEs, write to output arrays, advance `cq_head`. Returns count. |
| `gnitz_uring_register_buffers(ring, iovecs, count)` | `io_uring_register(IORING_REGISTER_BUFFERS)`. |

All memory barriers in C. All SQE struct manipulation in C. RPython never
touches ring internals.

### RPython wrappers

```python
@jit.dont_look_inside
def uring_submit_and_wait(ring_ptr, min_complete):
    # releasegil=True: blocks in kernel waiting for completions
    return int(_gnitz_uring_submit_and_wait(ring_ptr, rffi.cast(rffi.INT, min_complete)))

@jit.dont_look_inside
def uring_drain(ring_ptr, max_cqes):
    """Returns list of (user_data, res, flags) tuples."""
    ...
```

`releasegil=True` on `_gnitz_uring_submit_and_wait` (blocking call).
`_nowrapper=True` on non-blocking prep/submit calls.

### Buffer management

All buffers passed to io_uring SQEs must be raw-allocated
(`lltype.malloc(..., flavor='raw')`). RPython's moving GC can relocate objects
while the kernel has them in flight.

The existing `Buffer` class already uses raw allocation. For io_uring recv
buffers: pre-allocate a pool of fixed-size buffers (e.g., 8 MB each, one per
client slot) and register them via `IORING_REGISTER_BUFFERS`.

### Files changed
- New: `gnitz/server/uring_ffi.py` (~300 lines C, ~100 lines Python)

### Test
- New: `rpython_tests/uring_test.py` — basic submit/complete with IORING_OP_NOP;
  read/write to a tempfile; accept+recv on a Unix socket
- Build: `make prove-uring`

---

## Step 5: io_uring Event Loop for External Clients

Replace the poll-based `ServerExecutor.run_socket_server` with an io_uring
event loop.

### Architecture

```
io_uring instance (256 SQ entries)
  ├── 1 multishot IORING_OP_ACCEPT on listen_fd (user_data = ACCEPT_TAG)
  ├── per-client IORING_OP_RECV (user_data = client_fd)
  └── per-response IORING_OP_SEND (user_data = SEND_TAG | client_fd)
```

Step 3 uses SOCK_STREAM with length-prefixed framing. For io_uring, this
means the recv path needs a per-connection state machine to parse the 4-byte
length prefix before reading the payload. Each connection tracks:

```c
struct client_state {
    int      fd;
    uint32_t expected_len;   // 0 = waiting for header, >0 = waiting for payload
    uint32_t bytes_received;
    char    *recv_buf;       // pre-allocated from registered buffer pool
};
```

On startup:
1. `gnitz_uring_create(256)`
2. Pre-allocate N recv buffers (N = max clients, e.g., 64)
3. Submit multishot accept
4. Enter main loop

Main loop:
```python
while True:
    uring_ffi.uring_submit_and_wait(ring, 1)  # block until >=1 CQE
    completions = uring_ffi.uring_drain(ring, 64)
    for user_data, res, flags in completions:
        if user_data == ACCEPT_TAG:
            _on_accept(res)           # res = new client fd
        elif user_data & SEND_TAG:
            _on_send_complete(...)    # free send buffer
        else:
            _on_recv(user_data, res)  # user_data = client fd, res = bytes received
            # state machine: if header complete, allocate payload buffer;
            # if payload complete, dispatch request
```

### Fallback

If `io_uring_setup` fails (kernel too old, seccomp restriction), fall back to
the existing `poll` + `recv_framed` path:

```python
try:
    ring = uring_ffi.uring_create(256)
    _run_uring_server(ring, listen_fd)
except OSError:
    _run_poll_server(listen_fd)   # existing code path
```

### Files changed
- `gnitz/server/executor.py`: `run_socket_server` refactored to
  `_run_uring_server` + `_run_poll_server`

### Test
- `make prove-server` — single-worker server interaction (exercises both paths)
- `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/` — E2E

---

## Implementation Order

```
Step 1   eventfd_ffi.py: eventfd helpers + mmap additions
         New: gnitz/server/eventfd_ffi.py
         Changed: gnitz/storage/mmap_posix.py
         Test: make prove-eventfd

Step 2   Internal IPC overhaul (single SAL + unified WAL + exchange arena)
  2a     SAL transport: single file, master-owned fdatasync, eventfd signals
  2b     Gather-encode into SAL (SoA-aware scatter, partition-sorted)
  2c     Unified WAL (recovery with LSN filter, cursor-reset checkpoint)
  2d     Shared exchange arena: demand-paged 1 GB slots, MADV_DONTNEED
         Changed: main.py, ipc.py, master.py, worker.py, eventfd_ffi.py
         Changed: mmap_posix.py, wal_columnar.py, table.py
         Test: make prove-eventfd
               make prove-storage_comprehensive
               make prove-master_worker
               make prove-exchange
               GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/

Step 3   External IPC: SOCK_STREAM + length-prefixed framing
         Changed: ipc.py, ipc_ffi.py, executor.py
                  rust_client/gnitz-protocol/src/transport.rs
         Test: make prove-ipc_comprehensive
               make prove-server
               GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/

Step 4   uring_ffi.py: io_uring C FFI
         New: gnitz/server/uring_ffi.py
         Test: make prove-uring

Step 5   External event loop: io_uring accept + recv + send
         Changed: executor.py
         Test: make prove-server
               GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/
```

Step 1 → 2 is the critical sequence. 2a-2c can land first with the existing
master-relay exchange as a temporary fallback; 2d replaces it with the arena.
Step 3 is independent of Step 2. Step 4 is independent of everything. Step 5
depends on 3 and 4.

---

## Cost Comparison

Per `fan_out_push`, 10-column I64 schema, M=1000 rows, N=4 workers,
64 partitions per worker (PersistentTable):

### User-space data copies

| Step | Master | Worker | Total |
|---|---|---|---|
| Current | gather + encode + memmove-to-memfd (3×) | partition-split + WAL-encode + WAL-write + memtable-clone (4×) | 8× (see note) |
| After Step 2a | gather + encode + SAL-write (3×) | clone into memtable (1×) | 4× |
| After Step 2b | gather-encode into SAL (1×) | clone into memtable (1×) | **2×** |

After Step 2 complete: **2 user-space copies**: the fused gather-encode into the
SAL (master), and the clone from SAL mmap into owned memtable batch (worker).

Note: current worker 4× = partition re-split (1×, re-hashing + per-column
gather) + WAL encode (1×, encode_batch_to_buffer) + WAL write (1×, kernel
copy) + memtable clone (1×, to_sorted or explicit clone in upsert_batch).
Current master 3× = repartition gather (1×) + IPC encode into wire_buf (1×)
+ memmove wire_buf→memfd (1×). Client-side encode adds 1× on the client.

### Syscalls

Per round-trip (4 workers, no exchange):

| | Current | After Step 2 | After Step 3 | After Step 5 |
|---|---|---|---|---|
| Master→Worker transport | ~128 (send: 4×11, recv: 4×5, ×2 dirs) | 9 (4×eventfd_signal + 1×wait_any + 4×eventfd_signal) | 9 | 9 |
| Client↔Server transport | 22 (2×11) | 22 (unchanged) | 4 (send+recv×2) | ~2 (io_uring) |
| WAL write() | 256 | **0** (mmap writes) | 0 | 0 |
| WAL fdatasync() | 256 | **1** (master, single file) | 1 | 1 |
| **Total** | **~662** | **~32** | **~14** | **~12** |

### Fsync reduction detail

| | Current | After Step 2 |
|---|---|---|
| Fsyncs per push | 256 (64 per worker × 4 workers) | **1** (master, single SAL file) |
| NVMe flush commands per push | 256 | **1** |
| Wall-clock @ 0.5ms/fsync | 128ms (serial within each worker) | **0.5ms** |

---

## Trade-offs

### Worker-side clone cost

The clone from SAL mmap into owned memtable batch is a sequential memcpy
per column buffer plus string relocation for blob arenas. For typical push
sizes (100–10K rows), this is 10 μs – 1 ms. For the largest pushes (100K+
rows), it can reach a few milliseconds. This is negligible compared to the
eliminated 256× fsync cost (128 ms → 0.5 ms).

The clone also includes sorting (the batch arrives partition-sorted, not
globally PK-sorted; `upsert_batch` calls `to_sorted()`). For N rows, this is
O(N log N). For 1000 rows, ~10K comparisons — sub-millisecond.

### Single-process mode

Single-process mode (no workers) continues to use the existing per-partition
WAL path. The file-backed SAL only applies when `num_workers > 1`.
`PersistentTable.ingest_batch` checks a flag:

```python
if self._has_wal:
    # Single-process: per-partition WAL (existing path)
    self.wal_writer.append_batch(lsn, self.table_id, batch)
    self.memtable.upsert_batch(batch)
else:
    # Multi-worker: file-backed SAL handles durability externally
    self.memtable.upsert_batch(batch)
```

### Recovery complexity

SAL recovery scans one file (shared by all workers). Each worker reads
message group headers, extracts its own data via `worker_offsets[my_id]`,
and replays blocks with LSN > last_flushed_lsn. After a cursor-reset
checkpoint, old-epoch groups at higher offsets are skipped by the LSN filter
(all old-epoch LSNs <= flushed_lsn because checkpoint requires all
partitions flushed). No circular scanning, no sentinel markers.

Scan cost: O(file_size) header reads. For a 100 MB file: ~1 ms (reading
~48 bytes per group header, jumping by total_size). Acceptable as a
one-time startup cost.

### Checkpoint latency

The SAL can be reset when ALL workers have flushed ALL partition memtables.
Unlike per-partition WALs (which truncate independently), this is
all-or-nothing. A slow/idle partition holds the SAL from being reset.

Mitigation: with hash-based partitioning, all partitions receive roughly
equal data and flush at roughly the same cadence. Physical block usage in the
SAL equals the total unflushed data volume — the same total as the current 64
per-partition WALs per worker, just consolidated into one file. Checkpoint
resets the cursor to 0 (no file operations).

### SAL file size

The SAL file is created at `SAL_MMAP_SIZE` (1 GB) via `ftruncate` — a sparse
file where only written blocks consume physical disk space. `ls -l` shows
1 GB; `du -h` shows actual usage (~100 MB for typical workloads). On
checkpoint, the cursor resets to 0 and the same blocks are reused. No
dynamic growth, no file operations, no size tracking.

Physical block usage between checkpoints matches the current per-partition
WALs (4 workers × 64 partitions × ~0.4 MB = ~102 MB), just consolidated
into one file. After cursor reset, subsequent writes reuse already-allocated
blocks — no new allocation overhead on fdatasync.

If `write_cursor` approaches `SAL_MMAP_SIZE` (pathological: 1 GB of
unflushed data), the master forces a coordinated checkpoint (signal all
workers to flush, wait for `FLAG_CHECKPOINT` from all, reset cursor).

---

## Resolved Issues

### 1. Exchange arena overflow — RESOLVED

**Original problem**: fixed 32 MB slots could overflow on high-cardinality
exchange deltas.

**Solution**: 1 GB demand-paged virtual mappings (`MAP_SHARED|MAP_ANONYMOUS`).
Physical pages allocated only on write (demand paging). Physical pages
released between exchanges via `madvise(MADV_DONTNEED)`. Single arena
(no double-buffering needed — consumed barrier + MADV_DONTNEED is sufficient).

- **No overflow**: 1 GB per slot holds ~10M rows of a 10-column I64 schema.
  Any delta exceeding this implies the system is memory-bound regardless.
- **No wasted physical memory**: demand paging allocates on write,
  MADV_DONTNEED releases after consumed barrier. Physical footprint = 0
  between exchanges.
- **No fallback paths**: one code path for all sizes.
- **Cost**: 1 `madvise` syscall per worker per exchange (~2 μs). 4 GB
  virtual address space for 4 workers (0.003% of 128 TB x86-64 VA).

See Step 2d for full design.

### 2. `slice_view` for scan chunking — RESOLVED

**Original problem**: SEQPACKET has a per-message size limit (~8 MB). Scan
results can exceed this, requiring `slice_view` + `FLAG_MORE` + per-chunk
re-encoding + client reassembly.

**Solution**: switch the external client socket from `SOCK_SEQPACKET` to
`SOCK_STREAM` with 4-byte length-prefixed framing. STREAM has no message
size limit. Every response is a single encode + single framed send,
regardless of size.

- **Deleted**: `slice_view`, `_send_chunked`, `FLAG_MORE`, `sysctl wmem_max`,
  `setsockopt(SO_SNDBUF)`, chunk reassembly in Rust client.
- **Added**: 8 lines of C (`gnitz_send_framed`, `gnitz_recv_framed`).
- **Cost**: 4 bytes of wire overhead per message (length prefix).

See Step 3 for full design.

### 3. WAL maximum message size — RESOLVED

**Original problem**: a fixed-size ring buffer had ~15.5 MB effective capacity
(16 MB minus skip-marker waste at the wrap point). Large pushes could not fit.

**Solution**: the SAL file is `ftruncate`'d to `SAL_MMAP_SIZE` (1 GB) at
startup as a sparse file. The mmap covers the full file. No growth path, no
dynamic `ftruncate`, no size tracking. No message size limit. No skip
markers. No wrap-around. If `write_cursor` approaches `SAL_MMAP_SIZE`, the
master forces a coordinated checkpoint (reset cursor to 0).

See "Why not a ring buffer?" in Key Insight.

### 4. Recovery ordering with concurrent writes — RESOLVED

**Original problem**: a ring buffer recycles space after the consumer advances.
If the memtable hasn't flushed to a shard, the overwritten WAL data is the
only durable copy. On crash, that data is irrecoverable. Kernel writeback
can flush dirty overwrites nondeterministically, making recovery outcomes
timing-dependent.

**Solution**: the SAL is append-only. Data is never overwritten until explicit
checkpoint (which only occurs after all partitions have flushed to shards).
All fsynced data is preserved and recoverable. Recovery scans forward from
offset 0 and stops at the first invalid block — no circular scanning, no
ambiguity about which data is current.

### 5. Recovery checksum strength for circular scanning — RESOLVED

**Original problem**: circular ring recovery scans the entire capacity for
valid blocks in overwritten regions, relying on XXH64 to reject coincidental
matches. While statistically safe (~2×10^-14 false-positive rate), the scan
is complex and the argument is non-obvious.

**Solution**: SAL recovery is sequential, forward-only. Blocks are in append
order. The scan stops at the first invalid block (zero size prefix, bad
checksum, or truncated). No scanning of overwritten regions. The checksum
only needs to validate the tail block (which may be partially written due to
crash). False positives in overwritten regions are impossible because there
are no overwritten regions.

### 6. fdatasync cross-process ordering — RESOLVED

**Original problem**: the initial design had workers calling `fdatasync` on
the SAL to flush pages dirtied by the master (via the shared MAP_SHARED
mmap). This relies on Linux page cache semantics (fdatasync on any fd for the
same inode flushes all dirty pages for that inode) which is standard Linux
behavior but not POSIX-guaranteed. The proposed startup self-test (kill -9 +
restart) was also flawed — it tests persistence of the file, not cross-process
flush ordering.

**Solution**: single SAL file with master-owned fdatasync. The master writes
all worker messages into one SAL file and calls `fdatasync` once — on its own
writes, which is POSIX-guaranteed. Workers only read the SAL (via MAP_SHARED).
The eventfd signal from master to worker provides the memory ordering guarantee
(eventfd_read is a full memory barrier).

This eliminates the cross-process fsync assumption entirely. It also reduces
NVMe flush commands from N (one per worker) to 1 (one fdatasync on one file),
and provides natural group commit — all workers' data is durable after one
flush.

### 7. Checkpoint coordination SIGBUS race — RESOLVED

**Original problem**: the checkpoint sequence required the worker to truncate
the SAL file, then signal the master to reset its write cursor. If the master
sent a new message between the worker's truncation and receiving the
checkpoint signal, the master would write beyond the file size into a
truncated MAP_SHARED mapping → SIGBUS.

**Solution**: cursor-reset only (no file operations). The SAL file is never
truncated. On checkpoint:

1. Worker detects all its partitions have flushed since the last cursor reset.
2. Worker piggybacks `FLAG_CHECKPOINT` on its normal ACK (not a separate
   message or phase).
3. Master collects ACKs. When all N workers have sent `FLAG_CHECKPOINT`,
   master resets `sal.write_cursor = 0`.
4. Old-epoch data at higher offsets is harmless — recovery skips it via LSN
   filtering (all old-epoch LSNs ≤ `last_flushed_lsn` because checkpoint
   requires all partitions flushed).

Zero file operations. Zero SIGBUS risk. Zero additional syscalls. The
request/response structure provides natural coordination: when the worker
sends its ACK, the master has not yet written the next message, so there is
no race.

### 8. SAL file pre-allocation on non-ext4/xfs — RESOLVED

**Original problem**: `posix_fallocate` may fall back to a slow write-zeroes
loop on filesystems that don't support `fallocate(2)` natively (e.g., ZFS,
NFS). The proposed mitigation — "check return code for `EOPNOTSUPP`" — was
wrong: `posix_fallocate` is a libc wrapper that silently falls back and
returns 0 regardless of mechanism.

**Solution**: eliminate `fallocate` entirely. The SAL file is `ftruncate`'d
to `SAL_MMAP_SIZE` (1 GB) at startup as a sparse file. `ftruncate` is
POSIX.1-2001 — works on every filesystem, takes ~1 μs regardless of file
size (only modifies inode metadata). Physical blocks are allocated on first
write by the filesystem (demand paging). Block allocation cost at fdatasync
time is ~1-2 μs per 4 KB block (~50 μs for a 100 KB push) — negligible
compared to the NVMe flush command (100-10,000 μs). After one cursor-reset
cycle, all blocks below the high-water mark are already allocated, so
subsequent writes never allocate.

- **Deleted**: `fallocate_c` in `mmap_posix.py` (not needed anywhere)
- **Deleted**: `INITIAL_SAL_SIZE` constant
- **Deleted**: `SharedAppendLog.file_size` field
- **Deleted**: growth conditional in `write_message_group`
- **Deleted**: `_next_power_of_2` helper
- **Works on ALL POSIX filesystems**: no `fallocate(2)` dependency

### 9. W2M region sizing for large scan results — RESOLVED

**Original problem**: the W2M region (1 GB demand-paged) might be exceeded
by large scan results. With 10-column I64 schema and 10M rows: "~800 MB
encoded."

**The premise was wrong.** The W2M region carries ONE worker's result, not
all workers combined. With hash partitioning and N workers, each worker's
share is ~total/N. For 10M total rows with 4 workers: ~2.5M rows per worker
= ~260 MB encoded — well within 1 GB. For the W2M region to overflow, a
single worker would need >9.6M rows, implying >38M total rows (~4 GB of raw
data). At that scale the system is memory-bound regardless.

**The real constraint was the master's memory.** The master accumulated all
workers' results into a combined `ArenaZSetBatch` (~1 GB) and re-encoded it
into a `wire_buf` (~1 GB). Peak master memory for a 10M-row scan: ~2 GB.
Every byte was decoded, copied, and re-encoded — 3 unnecessary passes over
data the master never inspects.

**Solution**: streaming scan forwarding (see Step 2a). The master
raw-forwards each worker's W2M output directly to the client as a separate
framed message. No decode, no accumulate, no re-encode. Master peak memory:
O(1). The client receives N framed messages + 1 `FLAG_SCAN_DONE` sentinel
and concatenates locally.

- **W2M region stays at 1 GB**: sufficient for any in-memory workload
- **Master memory for scans**: O(1) regardless of result size
- **Copy reduction**: eliminates ~3 GB of unnecessary master-side copies for
  a 10M-row scan (decode + accumulate + re-encode → 0)
- **Client protocol change**: scan responses become N+1 framed messages
  (per-worker result × N + sentinel), handled as part of Step 3

See Step 2a "Streaming scan forwarding" for design.

---

## Open Issues

### 1. Worker-side partition boundary routing

Step 2b describes encoding partition boundary tables into the SAL message so
workers can avoid re-hashing. The master's `scatter_encode_to_sal` pre-sorts
rows by partition and writes per-worker sub-batches with a boundary region:

```
[boundary_table: N_partitions × 8 bytes]
  u32 partition_id
  u32 row_offset
```

However, the worker-side code path is underspecified. Currently, worker
`_handle_push` calls `ingest_to_family` → `PartitionedTable.ingest_batch`,
which re-hashes every row to determine its partition. For the SAL's 2× copy
claim to hold, the worker must use the boundary table to route rows directly
to partition sub-stores without re-hashing.

**Option A (recommended): Boundary-aware ingest method.** Add
`PartitionedTable.ingest_batch_presorted(batch, boundaries)` that reads the
boundary table and slices the batch into partition ranges:

```python
def ingest_batch_presorted(self, batch, boundaries):
    for i in range(len(boundaries)):
        part_id, start = boundaries[i]
        end = boundaries[i+1][1] if i+1 < len(boundaries) else batch.length()
        self.partitions[part_id].ingest_batch_range(batch, start, end)
```

This avoids re-hashing but requires a new code path in PartitionedTable.
The boundary table is small (64 entries × 8 bytes = 512 bytes) and already
computed during master-side scatter-encode.

**Option B (rejected): Single-partition messages.** Encoding one SAL message
per worker per partition (64 messages per worker instead of 1) defeats the
SAL's "one group per push" design. It inflates the message group header
(64× more offset/size entries), increases SAL scan cost on recovery, and
adds 64 header parses per worker per push. The complexity gain from
eliminating boundary tables is not worth the throughput regression.

Decision: Option A. Implementation deferred.

---

## Deployment Requirements

- Linux kernel >= 6.0 (for io_uring multishot accept). Current target: 6.18.
  Steps 1–3 work on any Linux kernel; only Step 5 requires io_uring.
- Filesystem: ext4, xfs, or btrfs recommended (for sparse file and fdatasync
  semantics). `ftruncate` is POSIX — no filesystem-specific dependency.
- No external library dependencies (no liburing).
- No `sysctl` tuning required (Step 3 uses SOCK_STREAM, which has no
  per-message size limit; the old `wmem_max` requirement is eliminated).

---

## What This Does NOT Do

- Does not change the WAL-block wire format (only adds optional partition
  boundary region behind a flag)
- Does not change the storage engine's mmap-based shard read path
- Does not add io_uring for WAL writes or shard file I/O (separate concern)
- Does not add TCP/remote transport (SOCK_STREAM is AF_UNIX only)
- Does not change the single-process WAL path or single-process scan path
- Does not change MemTable, compaction, or any storage layer code
- Scan streaming changes the client protocol for multi-worker scans only
  (N+1 framed messages instead of 1); single-process scans are unchanged
