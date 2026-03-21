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

1. A **shared append-only log** (SAL — mmap of a per-worker file) replaces
   both the memfd+SCM_RIGHTS transport AND the per-partition WAL files. The
   master gather-encodes partition-sorted batches directly into the SAL. The
   worker calls `fdatasync()` on the backing file — one syscall makes all
   partitions durable.

2. The worker **clones from the SAL** into owned memtable batches via the
   existing `upsert_batch` path. The clone is one sequential memcpy per column
   (~10 μs for typical batches). After the clone, the SAL offset advances.
   No per-push `mmap()`/`munmap()`. No long-lived pinning. The memtable code
   is completely unchanged.

3. A **shared exchange arena** (pre-fork shared memory) replaces the
   master-as-relay exchange model. Workers scatter-encode exchange data
   directly into the arena; other workers read and clone with zero master
   involvement. The master coordinates barriers only.

One `fdatasync`. Zero per-push `mmap`/`munmap`. Zero `write()`. The existing
memtable and storage layer are oblivious to the SAL.

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
correctness), file growth via `ftruncate` means no message size limit, and
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
    │  recv → classify → gather-encode (partition-sorted)
    │  → append into per-worker file-backed SAL (mmap)
    │  → signal via eventfd
    ▼
  Worker processes (forked, each owns partition range)
    │  read SAL at current offset → (ptr, len)
    │  fdatasync(wal_fd)             ← 1 fsync, ALL partitions
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

_fallocate = rffi.llexternal("posix_fallocate",
    [rffi.INT, rffi.LONGLONG, rffi.LONGLONG], rffi.INT,
    compilation_info=eci, _nowrapper=True)

@jit.dont_look_inside
def fallocate_c(fd, size):
    res = _fallocate(rffi.cast(rffi.INT, fd),
                     rffi.cast(rffi.LONGLONG, 0),
                     rffi.cast(rffi.LONGLONG, size))
    if rffi.cast(lltype.Signed, res) != 0:
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

Per worker: 1 file-backed SAL (master→worker) + 1 anonymous shared region
(worker→master).

**Master→worker SAL** (file-backed): one `fallocate` + one `mmap` of a
pre-allocated file. The mmap is created at 1 GB virtual (far exceeding the
initial file size) to allow growth without remapping. On Linux, `ftruncate`
to grow a file makes new pages accessible through an existing MAP_SHARED
mapping without remapping — the kernel checks file size at page-fault time.
LMDB and SQLite WAL mode use the same technique.

**Worker→master region** (anonymous): one `mmap(MAP_SHARED|MAP_ANONYMOUS)`.
This carries ACKs, scan results, exchange signals, and error responses.

```python
INITIAL_SAL_SIZE = 16 * 1024 * 1024    # 16 MB, file-backed
SAL_MMAP_SIZE    = 1 << 30             # 1 GB virtual (demand-paged)
W2M_REGION_SIZE  = 1 << 30             # 1 GB virtual, anonymous (demand-paged)
# Total virtual: (SAL_MMAP_SIZE + W2M_REGION_SIZE) * num_workers
# = 8 GB for 4 workers (0.006% of 128 TB x86-64 VA space)
# Physical at rest: 16 MB * num_workers (SAL fallocate) + 0 (anonymous demand-paged)
# = 64 MB for 4 workers
```

Create eventfds (one per direction, per worker) before fork.

### SAL setup

```python
for w in range(num_workers):
    wal_path = base_dir + "/worker_" + str(w) + ".wal"
    wal_fd = rposix.open(wal_path, O_RDWR | O_CREAT, 0o644)
    fallocate_c(wal_fd, INITIAL_SAL_SIZE)
    m2w_ptr = mmap_posix.mmap_file(
        wal_fd, SAL_MMAP_SIZE,
        prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        flags=mmap_posix.MAP_SHARED,
    )
    m2w_efd = eventfd_ffi.eventfd_create()

    w2m_ptr = mmap_posix.mmap_anonymous_shared(W2M_REGION_SIZE)
    w2m_efd = eventfd_ffi.eventfd_create()
    ...
```

After fork: child closes eventfds of other workers.

### Files changed
- New: `gnitz/server/eventfd_ffi.py` (~40 lines C, ~40 lines Python)
- `gnitz/storage/mmap_posix.py`: add `mmap_anonymous_shared(size)`,
  `fallocate_c(fd, size)`, `fdatasync_c(fd)`, `madvise_dontneed(ptr, size)`

### Test
- New: `rpython_tests/eventfd_test.py` — eventfd create/signal/wait, wait_any
  with multiple fds, timeout behavior
- Build: `make prove-eventfd`

---

## Step 2: Internal IPC + Per-Worker Unified WAL

Replace the memfd+socket path in `master.py` and `worker.py` with the
file-backed SAL transport. The SAL serves as both the IPC channel and
the durable WAL.

### 2a: SAL transport (master side)

#### `main.py` changes

SAL + eventfd allocation before fork (see Step 1 setup code). Pass SAL
pointers, fds, eventfds, and file sizes to `WorkerProcess` and
`MasterDispatcher`.

#### SAL state

```python
class SharedAppendLog(object):
    """Per-worker shared append-only log.

    Master appends messages at write_cursor. Worker reads at read_cursor.
    Both cursors are in-process variables (not stored in the file) — they
    stay in sync because the protocol is request/response.
    """
    def __init__(self, ptr, fd, efd, file_size):
        self.ptr = ptr                  # mmap base (1 GB virtual)
        self.fd = fd                    # backing file fd
        self.efd = efd                  # eventfd for signaling
        self.file_size = file_size      # current file size (grows on demand)
        self.cursor = 0                 # write_cursor (master) or read_cursor (worker)
```

The master and worker each hold a `SharedAppendLog` for the same underlying
file+mmap. The master uses `cursor` as the write position; the worker uses
`cursor` as the read position. These stay in sync because the worker advances
by exactly the bytes the master wrote.

#### `ipc.py` changes

New transport functions alongside existing `send_batch`/`receive_payload`:

```python
@jit.dont_look_inside
def send_to_sal(sal, target_id, zbatch, schema, flags,
                seek_pk_lo, seek_pk_hi, seek_col_idx,
                status, error_msg):
    """Encode WAL blocks into wire_buf, append to SAL."""
    wire_buf = _encode_wire(target_id, zbatch, schema, flags,
                            seek_pk_lo, seek_pk_hi, seek_col_idx,
                            status, error_msg)
    try:
        msg_size = wire_buf.offset
        total = 8 + _align8(msg_size)      # 8-byte size prefix + aligned payload

        # Grow file on demand (rare — only for large pushes).
        # ftruncate on MAP_SHARED: new pages accessible without remapping.
        if sal.cursor + total > sal.file_size:
            new_size = _next_power_of_2(sal.cursor + total)
            rposix.ftruncate(sal.fd, new_size)
            sal.file_size = new_size

        # Write size prefix
        _write_u64(sal.ptr, sal.cursor, r_uint64(msg_size))
        # Write payload
        buffer_ops.c_memmove(
            rffi.cast(rffi.VOIDP, rffi.ptradd(sal.ptr, sal.cursor + 8)),
            rffi.cast(rffi.VOIDP, wire_buf.base_ptr),
            rffi.cast(rffi.SIZE_T, msg_size),
        )
        sal.cursor += total
        eventfd_ffi.eventfd_signal(sal.efd)
    finally:
        wire_buf.free()
```

`_encode_wire` is the encoding logic factored out of `serialize_to_memfd`:
control block + optional schema block + optional data block. Same WAL-block
wire format, no memfd.

`_parse_from_ptr(ptr, length)` is the decoding logic factored out of
`_recv_and_parse`: takes a raw pointer + length instead of an fd.

```python
@jit.dont_look_inside
def receive_from_sal(sal):
    """Read next message from SAL, decode into IPCPayload.

    Returns None if no message ready (read_cursor == write position).
    The returned payload holds non-owning views into SAL memory.
    Caller must call payload.close() when done, which advances
    the SAL read cursor.
    """
    msg_size = _read_u64(sal.ptr, sal.cursor)
    if msg_size == 0:
        return None
    msg_ptr = rffi.ptradd(sal.ptr, sal.cursor + 8)
    payload = _parse_from_ptr(msg_ptr, intmask(msg_size))
    payload._pending_sal = sal
    payload._pending_sal_advance = 8 + _align8(intmask(msg_size))
    return payload
```

In `IPCPayload.close()`:

```python
def close(self):
    if self._pending_sal is not None:
        self._pending_sal.cursor += self._pending_sal_advance
        self._pending_sal = None
    if self.ptr:
        mmap_posix.munmap_file(self.ptr, intmask(self.total_size))
        self.ptr = lltype.nullptr(rffi.CCHARP.TO)
    if self.fd >= 0:
        os.close(intmask(self.fd))
        self.fd = -1
```

#### `master.py` changes

`MasterDispatcher.__init__` gains `m2w_sals` and `w2m_sals` arrays
(SharedAppendLog per worker, per direction).

All fan_out methods switch from socket to SAL:
```python
# Before:
ipc.send_batch(self.worker_fds[w], target_id, sb, schema=schema, flags=ipc.FLAG_PUSH)
# After:
ipc.send_to_sal(self.m2w_sals[w], target_id, sb, schema=schema, flags=ipc.FLAG_PUSH)
```

Poll+receive loops switch to SAL reads:
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

`broadcast_ddl` encodes once, writes to N SALs:
```python
def broadcast_ddl(self, target_id, batch, schema):
    wire_buf = ipc._encode_wire(target_id, batch, schema, flags=ipc.FLAG_DDL_SYNC)
    for w in range(self.num_workers):
        _sal_write_raw(self.m2w_sals[w], wire_buf.base_ptr, wire_buf.offset)
        eventfd_ffi.eventfd_signal(self.m2w_sals[w].efd)
    wire_buf.free()
```

#### `worker.py` changes

`WorkerProcess.__init__` gains `m2w_sal` and `w2m_sal`.

Main poll loop in `run()`:
```python
while True:
    payload = ipc.receive_from_sal(self.m2w_sal)
    if payload is not None:
        should_exit = self._handle_request_from_sal(payload)
        # payload.close() called inside _handle_request_from_sal
        if should_exit:
            return
    else:
        eventfd_ffi.eventfd_wait(self.m2w_sal.efd, timeout_ms=1000)
```

Response sending via `ipc.send_to_sal(self.w2m_sal, ...)`.

#### Worker push hot path

The push handler calls `fdatasync()` on the SAL's backing fd to make the
data durable, then clones the batch into the existing memtable via
`upsert_batch`:

```python
def _handle_push_from_sal(self, payload, target_id, batch):
    """Ingest SAL data via unified WAL. One fdatasync, one clone."""
    # Step 1: Durability — the SAL data is in the page cache (master wrote
    # via mmap). fdatasync flushes dirty pages to disk.
    mmap_posix.fdatasync_c(self.wal_sal_fd)

    # Step 2: Ingest — clone from SAL mmap into owned memtable batch.
    # upsert_batch calls to_sorted() which creates a new owned batch,
    # copying column data and relocating blob arena strings. After this,
    # the memtable has no references to the SAL.
    family = self.engine.registry.get_by_id(target_id)
    effective = ingest_to_family(family, batch)
    family.store.flush()

    # Step 3: Close payload — advances read cursor.
    # Safe: the memtable holds only owned data; SAL memory is unreferenced.
    payload.close()

    # ... rest of push handling (pending_deltas, evaluate_dag) unchanged ...
```

There is no `slice_view`, no shared blob_arena, no per-push `mmap()`, and no
mmap region tracking. The existing `MemTable`, `PersistentTable`, and storage
layer are completely unchanged.

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
fsyncs from 256 to 4 — the remaining clone is in L1/L2 cache and is negligible
compared to the disk fsync.

#### Back-pressure

The SAL grows on demand via `ftruncate`, so it cannot "fill up" in the ring
sense. However, unbounded growth is prevented by protocol structure: the
master sends one push (plus optional preloaded exchange messages) per
fan_out_push, then blocks waiting for ACKs. The worker processes and advances
before the master sends the next push. Between checkpoints, the SAL file
grows by the total volume of unflushed data — the same total as the current
64 per-partition WALs, just consolidated into one file.

If SAL growth approaches the 1 GB mmap limit (which implies ~1 GB of
unflushed WAL data — a pathological case), the worker forces a checkpoint:
flush all memtables to shards, then truncate+reset the SAL.

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
    """Get raw pointer into SAL at write cursor. Grows file if needed."""
    total = 8 + max_size   # size prefix + payload
    if sal.cursor + total > sal.file_size:
        new_size = _next_power_of_2(sal.cursor + total)
        rposix.ftruncate(sal.fd, new_size)
        sal.file_size = new_size
    # Return pointer past the 8-byte size prefix (caller writes payload)
    return rffi.ptradd(sal.ptr, sal.cursor + 8)

def sal_commit(sal, actual_size):
    """Finalize a direct write. Backfills size prefix, advances cursor."""
    _write_u64(sal.ptr, sal.cursor, r_uint64(actual_size))
    sal.cursor += 8 + _align8(actual_size)
    eventfd_ffi.eventfd_signal(sal.efd)
```

This is the SAL equivalent of the ring's `reserve`/`commit` — without wrap
detection, skip markers, or contiguous-space checks. The SAL is append-only;
space is always available (the file grows if needed).

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
def scatter_to_sals(batch, sals, num_workers, assignment, schema,
                    target_id, flags):
    """Classify by partition + gather-encode directly into N SALs."""
    n = batch.length()

    # Pass 1: classify rows by partition (256-way)
    part_lists = [newlist_hint(0) for _ in range(256)]
    for i in range(n):
        p = _partition_for_key(batch._read_pk_lo(i), batch._read_pk_hi(i))
        part_lists[p].append(i)

    # Pass 2: gather-encode per worker (partition-sorted with boundary table)
    for w in range(num_workers):
        indices = newlist_hint(n // num_workers)
        boundaries = newlist_hint(65)  # up to 64 partitions + sentinel
        for p in range(assignment.start(w), assignment.end(w)):
            boundaries.append(len(indices))
            for idx in part_lists[p]:
                indices.append(idx)
        boundaries.append(len(indices))  # sentinel

        if len(indices) == 0:
            ipc.send_to_sal(sals[w], target_id, None, schema=schema, flags=flags)
            continue

        max_size = _estimate_encoded_size(schema, len(indices), batch)
        dest_ptr = sal_get_write_ptr(sals[w], max_size)
        actual = _encode_full_message(dest_ptr, max_size, target_id, schema,
                                      batch, indices, boundaries, flags)
        sal_commit(sals[w], actual)
```

`_estimate_encoded_size`: for fixed-width columns, exact. For string columns,
add `batch.blob_arena.offset` as upper bound.

New IPC flag: `FLAG_PARTITION_SORTED = r_uint64(1 << 53)`. When set, the data
WAL block contains an extra region: `u32[num_boundaries]` — row-index boundaries
for partition-sorted data.

#### `master.py` changes

`fan_out_push` and `fan_out_ingest` call `scatter_to_sals` instead of
`repartition_batch` + N × `send_to_sal`.

### 2c: Per-worker unified WAL (file-backed SAL)

The SAL file IS the WAL. The master appends gather-encoded data into the
file-backed mmap. The worker calls `fdatasync()` to flush dirty pages. On
crash, the SAL file contains all messages since the last checkpoint.

No separate `WorkerWAL` class. No `append_and_map`. No per-push `mmap()`.
The SAL and its backing file are the entire WAL implementation.

#### `mmap_posix.py` changes

`fdatasync_c`, `fallocate_c`, `mmap_anonymous_shared`, `madvise_dontneed`
(see Step 1).

#### `worker.py` changes

`WorkerProcess.__init__` stores the SAL's backing fd:

```python
self.wal_sal_fd = wal_sal_fd  # for fdatasync
```

The hot path in `_handle_push` calls `fdatasync_c` then uses the existing
`ingest_to_family` + `upsert_batch` path (see 2a worker push hot path above).

#### Recovery

On worker startup (or crash recovery):

```python
def recover_from_sal(wal_path, engine, assignment, worker_id):
    """Replay a file-backed SAL after crash.

    The SAL is an append-only sequence of messages. Each message has an
    8-byte size prefix followed by WAL-block encoded payload. Scan forward
    from offset 0, replaying valid blocks with LSN > last flushed.
    """
    fd = rposix.open(wal_path, os.O_RDONLY, 0)
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
    """Sequential scan of append-only SAL. O(data), not O(capacity)."""
    offset = 0
    while offset + 8 <= file_size:
        msg_size = _read_u64(ptr, offset)
        if msg_size == 0:
            break  # zero padding — end of valid data
        if offset + 8 + intmask(msg_size) > file_size:
            break  # truncated message — crash during write
        data_ptr = rffi.ptradd(ptr, offset + 8)
        valid, lsn = _try_parse_and_validate(data_ptr, intmask(msg_size))
        if not valid:
            break  # corrupt block — crash during write, stop here
        if lsn > _last_flushed_lsn(engine, assignment, worker_id):
            _replay_block(data_ptr, intmask(msg_size), engine,
                          assignment, worker_id)
        offset += 8 + _align8(intmask(msg_size))
```

Key difference from ring recovery: the scan is sequential, forward-only, and
stops at the first invalid block. No circular scanning, no handling of
partially overwritten regions, no sorting by LSN. The append-only invariant
means all valid blocks are in order.

#### Checkpoint / SAL reset

The SAL can be reset when all partition memtables have flushed to shards.
The worker tracks this via partition manifest LSNs:

```python
def checkpoint_if_possible(self):
    """Reset SAL when all partitions are flushed past all SAL entries."""
    if self._all_partitions_flushed():
        # Truncate to 0, then re-allocate initial size.
        # Both master and worker reset their cursors to 0.
        rposix.ftruncate(self.wal_sal_fd, 0)
        mmap_posix.fallocate_c(self.wal_sal_fd, INITIAL_SAL_SIZE)
        self.m2w_sal.cursor = 0
        # Signal master to reset its write cursor for this worker.
        ipc.send_to_sal(self.w2m_sal, 0, None,
                        flags=ipc.FLAG_CHECKPOINT)
```

On the master side, receiving `FLAG_CHECKPOINT` resets the corresponding
`m2w_sal.cursor = 0` and `m2w_sal.file_size = INITIAL_SAL_SIZE`.

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
- `gnitz/server/main.py`: SAL + eventfd + exchange arena allocation before
  fork, pass to workers
- `gnitz/server/ipc.py`: `send_to_sal`, `receive_from_sal`, `_parse_from_ptr`,
  `_encode_wire`, `scatter_to_sals`, `_encode_full_message`, `_estimate_encoded_size`,
  `FLAG_PARTITION_SORTED`, `FLAG_EXCHANGE_READY`, `FLAG_EXCHANGE_BARRIER`,
  `FLAG_EXCHANGE_CONSUMED`, `FLAG_CHECKPOINT`; `IPCPayload._pending_sal` field +
  cursor advance in `close()`; `SharedAppendLog` class
- `gnitz/server/master.py`: fan_out/push methods use SAL send/recv;
  `scatter_to_sals` for push; `_collect_acks_and_relay` replaced by
  `_collect_and_barrier` (barrier coordination only, no data relay);
  `_relay_exchange` deleted
- `gnitz/server/worker.py`: main loop uses SAL; push path uses `fdatasync_c`
  then existing `ingest_to_family` / `upsert_batch`;
  `WorkerExchangeHandler` rewritten to use shared arena (local repartition +
  scatter-encode + barrier + clone-from-arena);
  checkpoint logic (reset SAL on full flush)
- `gnitz/server/eventfd_ffi.py`: `eventfd_wait_any`
- `gnitz/storage/mmap_posix.py`: `mmap_anonymous_shared`, `fallocate_c`,
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
prefix provides the same framing guarantee as SEQPACKET. The request/
response protocol is strictly serial (one request, one response, no
pipelining), so STREAM's lack of message boundaries is harmless.

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
- `gnitz/server/executor.py`: client handling switched to `send_framed`/`recv_framed`
- `rust_client/gnitz-protocol/src/transport.rs`: drop memfd, switch to
  `SOCK_STREAM` + length-prefixed framing

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

Step 2   Internal IPC overhaul (SAL transport + file-backed WAL + exchange arena)
  2a     SAL transport: master ↔ worker via shared append-only log + fdatasync
  2b     Gather-encode into SAL (SoA-aware scatter, partition-sorted)
  2c     File-backed SAL (recovery, checkpoint, table.py changes)
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
| Current | gather + encode + memmove-to-memfd (3×) | partition-split + WAL-encode + memtable-clone (3×) | 6× |
| After Step 2a | gather + encode + SAL-write (3×) | fdatasync + clone into memtable (1×) | 4× |
| After Step 2b | gather-encode into SAL (1×) | fdatasync + clone into memtable (1×) | **2×** |

After Step 2 complete: **2 user-space copies**: the fused gather-encode into the
SAL (master), and the clone from SAL mmap into owned memtable batch (worker).

### Syscalls

Per round-trip (4 workers, no exchange):

| | Current | After Step 2 | After Step 3 | After Step 5 |
|---|---|---|---|---|
| Master→Worker transport | 96 (4×11×2) | 8 (4×eventfd×2) | 8 | 8 |
| Client↔Server transport | 22 (2×11) | 22 (unchanged) | 4 (send+recv×2) | ~2 (io_uring) |
| WAL write() | 256 | **0** (mmap writes) | 0 | 0 |
| WAL fdatasync() | 256 | **4** | 4 | 4 |
| **Total** | **~630** | **~34** | **~16** | **~14** |

### Fsync reduction detail

| | Current | After Step 2 |
|---|---|---|
| Fsyncs per worker per push | 64 (one per partition) | **1** (one per worker) |
| Total fsyncs (4 workers) | 256 | **4** |
| Wall-clock @ 0.5ms/fsync | 128ms (serial within worker) | **2ms** |

---

## Trade-offs

### Worker-side clone cost

The clone from SAL mmap into owned memtable batch is a sequential memcpy
per column buffer plus string relocation for blob arenas. For typical push
sizes (100–10K rows), this is 10 μs – 1 ms. For the largest pushes (100K+
rows), it can reach a few milliseconds. This is negligible compared to the
eliminated 64× fsync cost (128 ms → 2 ms).

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

SAL recovery scans one append-only file per worker. The scan is sequential
from offset 0: read the 8-byte size prefix, validate the WAL block checksum,
replay if LSN > last flushed. Stop at the first zero or invalid block.
O(data), not O(capacity). No circular scanning, no handling of partially
overwritten regions. A 100 MB SAL with 10 MB of valid data takes ~1 ms.

### Checkpoint latency

The SAL can be reset when all partitions have flushed past all SAL entries.
Unlike per-partition WALs (which truncate independently), this is
all-or-nothing. A slow/idle partition holds the SAL from being reset.

Mitigation: with hash-based partitioning, all 64 partitions receive roughly
equal data and flush at roughly the same cadence. The SAL file grows by the
total unflushed data volume — the same total as the current 64 per-partition
WALs, just consolidated into one file. Checkpoint resets it to the initial
16 MB.

### SAL file growth

The SAL file grows between checkpoints as messages accumulate. With 1000
pushes of 100 KB each: 100 MB per worker. This matches the current 64
per-partition WALs (64 × ~1.6 MB = ~102 MB). On checkpoint (all partitions
flushed), the file is truncated and re-allocated to 16 MB.

The 1 GB mmap virtual mapping accommodates growth without remapping. If
growth exceeds 1 GB (pathological: 1 GB of unflushed data per worker), the
worker forces a checkpoint (flush all memtables, truncate SAL).

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

**Solution**: the SAL is append-only with file growth on demand via
`ftruncate`. The 1 GB virtual mmap covers growth without remapping (Linux
`ftruncate` on a MAP_SHARED file makes new pages accessible at fault time —
standard LMDB/SQLite technique). No message size limit. No skip markers.
No wrap-around.

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

---

## Open Issues

### 1. `posix_fallocate` on all filesystems

`posix_fallocate` may fall back to a slow write-zeroes loop on filesystems that
don't support `fallocate(2)` natively (e.g., ZFS, some NFS mounts). The SAL
file is created once at startup and grown rarely (only for oversized pushes),
so this is not a hot-path concern, but it could cause a surprising
multi-second delay on first server start.

Mitigation: detect the fallback (check return code for `EOPNOTSUPP` from the
underlying `fallocate(2)` syscall) and fall back to `ftruncate` + explicit
`mmap(MAP_POPULATE)` to fault in pages eagerly. Or simply document that
ext4/xfs/btrfs are the supported filesystems.

### 2. Master fdatasync ordering guarantee

The claim that `fdatasync(fd)` on the worker flushes pages dirtied by the
master (via the shared MAP_SHARED mmap) relies on Linux page cache semantics:
MAP_SHARED pages are in the page cache, and `fdatasync` on any fd referring to
the same inode flushes all dirty pages for that inode.

This is standard Linux behavior (used by LMDB, SQLite WAL mode, etc.) but is
not POSIX-guaranteed. The implementation should include a startup self-test:
master writes a known pattern to the SAL, worker fsyncs, both processes verify
the pattern survives a simulated crash (kill -9 + restart).

### 3. Checkpoint coordination protocol

The checkpoint sequence (worker detects all partitions flushed → truncates SAL
→ signals master to reset write cursor) requires both sides to agree on the
reset atomically. If the master sends a new message between the worker's
truncation and the master receiving the checkpoint signal, the master writes
into a truncated file. The protocol must ensure no in-flight writes exist
when the SAL resets.

The request/response structure provides a natural coordination point: the
worker only checkpoints between request processing (after ACK, before the
next receive). At that moment, the master is blocked waiting for the ACK
and has not yet written the next message. The checkpoint signal travels via
the w2m SAL, which the master reads before writing the next m2w message.
So the ordering is:

```
Worker: process push → ACK → detect flush → truncate SAL → send CHECKPOINT
Master: receive ACK → receive CHECKPOINT → reset write cursor → send next push
```

This is safe because the master processes the CHECKPOINT before writing the
next message. But the implementation must ensure the CHECKPOINT flag is
checked in the ACK-receive loop (not a separate poll phase) so it's
processed atomically with the ACK sequence.

### 4. W2M region sizing for large scan results

The worker→master region uses anonymous shared memory (1 GB demand-paged).
Large scan results (full table scan with millions of rows) are written as
a single message. With 10-column I64 schema and 10M rows: ~800 MB encoded.
This fits within 1 GB but leaves little headroom.

For most schemas and workloads, 1 GB is generous. If a scan result exceeds
the mapping: the worker could chunk the result into multiple messages (with a
continuation flag), or the mapping could be sized larger (2 GB virtual is
still negligible). The simplest approach is to document the limit and size
the mapping to 2 GB.

---

## Deployment Requirements

- Linux kernel >= 6.0 (for io_uring multishot accept). Current target: 6.18.
  Steps 1–3 work on any Linux kernel; only Step 5 requires io_uring.
- Filesystem: ext4, xfs, or btrfs recommended (for native `fallocate` support).
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
- Does not change the Rust client's synchronous request/response model
- Does not change the single-process WAL path
- Does not change MemTable, compaction, or any storage layer code
