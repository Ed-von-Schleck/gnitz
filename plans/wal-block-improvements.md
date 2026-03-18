# WAL-Block IPC: Improvements

Prerequisite: `ipc-unification.md` — `_parse_wal_block` / `decode_batch_from_ptr` must
exist and IPC messages must be WAL blocks.

## Exchange relay: streaming scatter

The exchange relay in `_relay_exchange` currently does:
- receive N exchange payloads → `payload.batch.clone()` (N allocs + N copies) → `payload.close()` → merge N clones → `repartition_batch` → encode × N → send × N

The clone exists solely because `payload.close()` frees the mmap, so the batch must
survive it. With WAL-block IPC, `payload.batch` buffers are non-owning views into the
mmap. `_direct_append_row` does a full copy of each row (including string relocation into
the destination blob arena) before returning, so scattering rows into destination batches
and then calling `payload.close()` is safe.

Replace `exchange_buffers[vid][w] = payload.batch.clone()` with direct scatter into N
pre-initialized destination batches:

```python
# On first exchange message for this vid, initialize dest_batches
if vid not in exchange_dest:
    exchange_dest[vid] = [ArenaZSetBatch(schema)] * num_workers

# Scatter rows from mmap view directly — no clone
if payload.batch is not None:
    repartition_into(payload.batch, shard_cols, exchange_dest[vid])
payload.close()   # mmap freed; rows already in dest_batches

# After all N workers:
if exchange_counts[vid] == num_workers:
    for w in range(num_workers):
        ipc.send_batch(worker_fds[w], vid, exchange_dest[vid][w], schema=schema)
```

Peak memory: 1 mmap view + N dest batches = N+1 batches simultaneously, down from N source
clones + 1 merged + N dest batches = 2N+1. For N=4: 5 vs 9 batches. The merge step
disappears entirely.

## Parallel `fan_out_scan`

`fan_out_scan` currently receives worker responses in fixed order (w=0, w=1, ...). If
worker 3 finishes in 10ms and worker 0 takes 100ms, the master blocks on worker 0. Replace
with a poll loop (identical to the one already used in `fan_out_push`):

```python
while workers_remaining > 0:
    revents = ipc_ffi.poll(unfinished_fds, POLLIN, timeout)
    for each ready fd:
        payload = receive_payload(fd)
        result.append_batch(payload.batch)   # full copy
        payload.close()                      # mmap freed; rows safely in result
        workers_remaining -= 1
```

Scan latency becomes O(max worker scan time) instead of O(sum). `payload.close()` is safe
immediately after `append_batch` because both string and non-string column data is fully
copied by the append.

## DDL sync = WAL fan-out

`broadcast_ddl` / `_handle_ddl_sync` / `FLAG_DDL_SYNC` exist to propagate system-table
writes from master to workers. The master encodes a batch to a WAL file, then immediately
re-encodes the same batch for IPC.

`WALWriter.append_batch` gains a `subscriber_fds` list. After `encode_batch_to_buffer`
writes to `block_buf`, it copies the block to each subscriber fd (encode once, N+1
destinations: file + N workers). `broadcast_ddl`, `_handle_ddl_sync`, and `FLAG_DDL_SYNC`
are deleted. Workers apply incoming system-table WAL blocks via `ingest_batch_memonly` +
hook dispatch — the same code path they already have. The ACK barrier is a trivial one-byte
ping, independent of batch encoding.

## Zero-copy WAL recovery

`WALReader.read_next_block()` currently does: 2 `rposix.read` syscalls + 1 `lltype.malloc`
+ 2 byte-by-byte copies + 1 `lltype.free` per block. Switch WALReader to mmap:

```python
ptr = mmap_posix.mmap_file(fd, file_size, PROT_READ, MAP_SHARED)
off = 0
while off < file_size:
    block_size = wal_layout.WALBlockHeaderView(rffi.ptradd(ptr, off)).get_total_size()
    batch = wal_columnar.decode_batch_from_ptr(rffi.ptradd(ptr, off), block_size, schema)
    yield batch        # non-owning views into mmap
    batch.free()       # no-op on memory
    off += block_size
mmap_posix.munmap_file(ptr, file_size)
```

Zero allocations, zero copies. `decode_batch_from_ptr` handles both IPC receive and WAL
recovery. The OS manages page faults on demand.

## Shared slot arena

`decode_batch_from_ptr(ptr, total_size, schema)` is pointer-agnostic — it does not care
whether the pointer is a memfd mmap or a pre-forked shared memory region. This is the
key primitive for eliminating memfd round-trips from **all** hot IPC paths.

### Arena layout

Before forking workers, master allocates one `MAP_SHARED | MAP_ANONYMOUS` region:

```
m2w_push[N]   — master writes push sub-batches, workers read        (N × slot_size)
w2m_exch[N]   — workers write exchange batches, master reads        (N × slot_size)
m2w_exch[N]   — master writes repartitioned exchange, workers read  (N × slot_size)
```

All N workers inherit the mapping through `fork()` at zero extra cost. Total memory:
3N × slot_size (e.g., 3 × 4 × 64 MB = 768 MB for N=4). Both `slot_size` and N are
configurable.

The socket fd survives but its role changes: it carries **only 1-byte signals**. `send_fd`
/ `recv_fd` (SCM_RIGHTS) are eliminated from the hot path entirely.

### Push fan-out (critical path: every user-table write)

**Master** splits the batch by PK as today, then for each worker W:
1. `encode_batch_to_buffer(Buffer.from_external_ptr(m2w_push[w], slot_size), ...)` —
   encodes directly into shared memory; no intermediate buffer, no memmove.
2. `write(worker_socket[w], b'\x01', 1)` — signal "push slot ready".

**Worker** on signal:
1. `total_size = WALBlockHeaderView(m2w_push[w]).get_total_size()` — zero syscalls.
2. `decode_batch_from_ptr(m2w_push[w], total_size, schema)` — non-owning views into the
   slot; identical call to the memfd path.
3. Process batch, then `write(master_socket, b'\x00', 1)` — ACK, and slot-free signal.

The ACK doubles as slot-free notification. Master can reuse `m2w_push[w]` as soon as the
ACK arrives. The synchronous push protocol (one outstanding push per worker) already
guarantees no slot collision.

**Syscall count per `fan_out_push`, N=4**:
- memfd path: 4 workers × (`memfd_create` + `ftruncate` + `mmap` + `munmap` + `sendmsg` +
  `recvmsg` + `mmap` + `munmap`) = **40 syscalls**
- Shared slots: 4 workers × (`write` + `read`) = **8 syscalls**

For a write that also triggers one exchange barrier: 120 → 24 syscalls end-to-end.

### Exchange (same mechanism, same arena)

**Worker sends exchange batch**:
1. Compute WAL block size from `batch.length()` + `batch.blob_arena.offset` (deterministic).
2. `encode_batch_to_buffer(Buffer.from_external_ptr(w2m_exch[w], slot_size), ...)`.
3. `write(master_socket, b'\x01', 1)`.

**Master** on signal:
1. `total_size = WALBlockHeaderView(w2m_exch[w]).get_total_size()`.
2. `decode_batch_from_ptr(w2m_exch[w], total_size, schema)` — scatter rows directly.
3. `write(worker_socket[w], b'\x01', 1)` (repartitioned data now in `m2w_exch[w]`).

The synchronous exchange protocol (worker blocks on master's response) guarantees one
in-flight exchange per worker per direction.

### Fallback for oversized batches

If the WAL block would exceed `slot_size`, fall back to the memfd path. The fallback is
signalled by a different byte value (e.g., `b'\xff'`). `decode_batch_from_ptr` is the same
call regardless — pointer just differs. For push sub-batches, oversized is unlikely: after
splitting across N workers, each sub-batch is at most 1/N of the original.

### What does not benefit from shared slots

- **Scans**: result size is unbounded (full table scan); slot sizing is impractical.
  The parallel `fan_out_scan` improvement (poll loop, O(max) latency) is independent.
- **DDL**: WAL subscriber fan-out (encode once, N memmoves to subscriber fds) is the right
  model; DDL messages are small and infrequent.
- **Allocations**: tiny, rare, not worth optimising.

## Exchange fault tolerance

Before sending an exchange batch to master, a worker writes it to a local WAL. After
receiving the master's repartitioned response, it truncates the WAL. If the worker crashes
before ACK, it replays the WAL on restart and resends. Exactly-once exchange semantics
emerge from the format identity — no new infrastructure needed.
