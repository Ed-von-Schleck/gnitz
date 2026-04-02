# Rust IPC Layer for GnitzDB

## Goal

Replace the inline C FFI code in gnitz's server I/O layer with a Rust cdylib,
and restructure the per-connection state machine to enable client pipelining.

## Boundary Decision: Rust as I/O Library (Not Rust as Main Binary)

Verified: "Rust cdylib called by RPython" is the correct boundary. The inverse
("Rust binary, RPython .so") is wrong for gnitz due to GC root tracking,
GIL serialisation, fork safety, and no RPython-as-.so precedent. See git
history for the full analysis.

## Architecture

### Rust Internal Structure: Ring Trait + Transport

```
┌─────────────────────────────────────────────────────┐
│  libgnitz_transport.so  (Rust cdylib, extern "C")   │
│                                                     │
│  ┌─ Ring trait (I/O mechanism) ──────────────────┐  │
│  │  IoUringRing: prep_recv, prep_send,           │  │
│  │    prep_accept, submit_and_wait_timeout,       │  │
│  │    drain_cqes                                  │  │
│  │  SQ auto-flush: prep_* are infallible         │  │
│  │  (backed by io-uring crate, ~100-line adapter) │  │
│  └────────────────────────────────────────────────┘  │
│                        ▲                             │
│                        │ generic: Transport<R: Ring> │
│  ┌─ Transport (state machine) ───────────────────┐  │
│  │  conns: HashMap<fd, Box<Conn>>                │  │
│  │    Conn { recv_state, send_queue, closing }   │  │
│  │  dirty_fds: Vec<i32> (pending-send tracker)   │  │
│  │  pending_delivery: Vec<i32> (overflow recv)   │  │
│  │  Inner drain loop (exact-length recv)         │  │
│  │  Deferred-close lifecycle                     │  │
│  │  Double-buffered send coalescing              │  │
│  └────────────────────────────────────────────────┘  │
│                                                     │
│  C-ABI exports (extern "C"):                        │
│    transport_create/destroy/accept/poll/send/        │
│    free_recv/close_fd                                │
│                                                     │
│  Also (thin syscall wrappers, if phases 2-3):       │
│    eventfd_create/signal/wait/wait_any              │
│    mmap_file/munmap_file/memfd_create/fdatasync     │
│    sal_write_group/sal_read_worker_msg              │
│    w2m_write/w2m_read                               │
└─────────────────────────────────────────────────────┘
```

Wire protocol, recv model, SQ capacity management, RecvState, SendQueue,
deferred-close lifecycle, transport_poll inner drain loop, CQE buffer sizing,
pipelining analysis, and read barrier pattern — see git history for full
design documentation.

## Phase 1: Rust Transport Core — DONE

### What shipped

- **Rust crate** `crates/gnitz-transport/` (10 source files, cdylib):
  Ring trait, IoUringRing (wraps `io-uring` 0.7), RecvState, SendQueue,
  Conn (with Drop), Transport<R: Ring> with full transport_poll, MockRing,
  extern "C" FFI exports. 42 unit tests.

- **RPython bindings** `gnitz/server/rust_ffi.py`: 7 `rffi.llexternal`
  wrappers following the `uring_ffi.py` ECI pattern.

- **Executor refactor** `gnitz/server/executor.py`: Deleted 9-dict state
  machine + 4 connection registries + 6 methods (~300 lines). Replaced with
  `transport_poll`-based event loop (~80 lines). Read barriers before
  seeks/scans. `_fire_pending_ticks()` before index seeks (fixes pre-existing
  staleness bug).

- **Build integration** `Makefile`: `rust-transport` target, `server` /
  `release-server` / `release-server-nojit` depend on it. `GNITZ_TRANSPORT_LIB`
  env var for library path + rpath.

- **Verification**: `cargo test` 42/42, `run-server_test-c` PASS,
  `GNITZ_WORKERS=4 make e2e` 360/360 pass.

### Lessons learned (apply to future phases)

1. **HashMap + inline buffers = use-after-move.** `Conn` contains an inline
   `hdr_buf: [u8; 4]`. When the HashMap resizes, all entries move, invalidating
   pointers held by io_uring SQEs. Fix: `HashMap<i32, Box<Conn>>` — the Conn
   lives on the heap and doesn't move. This applies to ANY struct with inline
   buffers stored in a resizable container where external code holds pointers
   into the buffer.

2. **`rffi.VOIDP` vs `rffi.CCHARP` at FFI boundary.** `transport_poll`
   returns `void**` output arrays. `ipc._parse_from_ptr` expects `CCHARP`.
   Must cast: `rffi.cast(rffi.CCHARP, ptr)` before passing. RPython's
   annotator fails at translation time if types don't unify — easy to catch.

3. **`rffi.VOIDPP` exists** (`CArrayPtr(VOIDP)`) — no need for manual type
   construction.

4. **`io-uring` crate 0.7 API**: Use `submitter().submit_with_args(want, &args)`
   with `SubmitArgs::new().timespec(&ts)` for timeout-based wait. There is no
   `submit_and_wait_with_timeout` method. Handle `ETIME` and `EINTR` as Ok(0).

5. **`_flush_pending_pushes` needs `transport` arg.** All send paths flow
   through `_send_response` which needs the transport handle. Thread it
   through every method that sends responses, including flush and error paths.

6. **Read barrier clears lists in-place** (`del p_fds[:]`). The caller in
   `_run_server` holds the same list objects, so `pending_row_count` may
   accumulate past the flushed batch. Harmless — the threshold check calls
   flush on empty lists (no-op) and resets the counter.

### Deferrable cleanups (independent of future phases)

1. **PendingBatch extraction** — refactor inline 5-parallel-list batching
   into a class. Code clarity only.

2. **Socketpair → `os.getppid()` migration** — workers poll `crash_fd` for
   POLLHUP to detect master death (`worker.py:106-108`). Replace with
   `os.getppid() != master_pid`. Eliminates 2×N socketpair fds,
   `ipc_ffi.poll()`, `ipc_ffi.create_socketpair()`.

3. **Per-table read barriers** — current implementation flushes ALL pending
   pushes on any read op. Per-table selective flush (only flush pushes for
   the target table) improves cross-table pipelining. The sort-merge in
   `_flush_pending_pushes` already groups by tid — partial flush extracts
   and processes only the matching run.

4. **E2E test additions** called for by this plan:
   - Index seek after push (validates tick-before-index-seek fix)
   - Cross-table pipelining (validates per-table barrier behaviour)

5. **Dead code cleanup** — `uring_ffi.py` is still compiled into the binary
   (inline C). `uring_prep_read`/`uring_prep_write` are dead code (never
   called outside the file). Can be removed once verified no transitive
   reference exists.

## ~~Phase 2: eventfd + mmap/memfd~~ (deferred indefinitely)

**Value assessment:** These are stable, thin syscall wrappers. Moving them
to Rust adds build complexity for every FFI callsite, not just the transport.
If the cdylib linkage has any issues during RPython translation, it affects
storage I/O too — blast radius grows for no correctness benefit.

**Recommendation:** Defer unless a concrete need arises (e.g., adding
Rust-side storage I/O that needs mmap).

If proceeding:
- Move `eventfd_ffi.py` functions (create, signal, wait, wait_any) into cdylib
- Move `mmap_posix.py` functions (mmap, munmap, fdatasync, memfd_create,
  ftruncate, raise_fd_limit, try_set_nocow) into cdylib
- Update `rust_ffi.py` with new bindings
- **Test**: `run-multicore_test-c`, E2E with `GNITZ_WORKERS=4`
- **Lesson from Phase 1**: workers call eventfd/mmap post-fork — safe because
  these are stateless syscall wrappers with no shared mutable state

## ~~Phase 3: SAL + W2M Transport~~ (deferred indefinitely)

Same value assessment as Phase 2. These are stable binary-layout functions.

If proceeding:
- Move `ipc.write_message_group`, `ipc.read_worker_message`,
  `ipc.write_to_w2m`, `ipc.read_from_w2m` into Rust
- Rust implements message group header as `#[repr(C)]` struct with
  `AtomicU64` for size prefix
- RPython continues to call `ipc._parse_from_ptr` on data payloads
- **Test**: `run-multicore_test-c`, E2E with `GNITZ_WORKERS=4`

## ~~Phase 4: Wire Codec (Control Block)~~ (deferred indefinitely)

Moving control-block encode/decode to Rust means any wire format change
requires coordinated Rust + RPython modifications. The parsing overhead is
negligible vs. engine processing. Only makes sense as a stepping stone
toward a Rust-native server, which this plan rejects.

## Pipelining (enabled by Phase 1, no additional server changes needed)

Phase 1's transport already enables pipelining:
1. Rust Transport decouples recv/send per fd
2. Inner drain loop exhausts pipelined data per poll
3. RPython event loop processes batches of messages per poll
4. Send coalescing batches multiple responses per fd into one SEND SQE

Remaining work is client-side (async Rust client, Python asyncio wrapper).
The GIL serialises engine calls, so pipelining reduces latency and syscall
count, not throughput.

## Verified Assumptions

| # | Claim | Status |
|---|-------|--------|
| 1 | Batch data is raw malloc, not GC heap | ✅ |
| 2 | ArenaZSetBatch object is GC-managed | ✅ |
| 3 | Schema/TableFamily/EntityRegistry are GC heap | ✅ |
| 4 | IPC-received batches are non-owning views | ✅ |
| 5 | Wire format matches between Rust and RPython | ✅ |
| 6 | JIT is unaffected by replacing C FFI with Rust | ✅ |
| 7 | ExternalCompilationInfo can link Rust cdylib | ✅ |
| 8 | Fork is safe with a stateless Rust cdylib | ✅ |
| 9 | Rust panics at FFI boundary abort (since 1.71) | ✅ |
| 10 | RPython dispatch logic requires GC objects | ✅ |
| 11 | Buffered pushes clone batch data | ✅ |
| 12 | fd_to_client/client_to_fd are cleanup-only | ✅ |
| 13 | Current code has exactly 1 SQE per fd | ✅ |
| 14 | io_uring ring created after fork, master only | ✅ |
| 15 | Current recv is exact-length, not streaming | ✅ |
| 16 | drain_cqes is no-syscall (memory-mapped) | ✅ |
| 17 | submit_and_wait_timeout(0,0) is submit-only | ✅ |
| 18 | `_queue_async_send` is unsafe for multi-send per fd | ✅ |
| 19 | Crash detection: master uses waitpid, workers use socketpair POLLHUP | ✅ |
| 20 | Current code caps drain at sq_entries (256) | ✅ |
| 21 | active_fds holds server_fd (safe to delete) | ✅ |
| 22 | Index seeks don't fire ticks (staleness bug, now fixed) | ✅ |
| 23 | ECI external library linking is production-proven | ✅ |

## Build System

```makefile
rust-transport:
	cd crates && cargo build --release -p gnitz-transport

server: rust-transport
	GNITZ_TRANSPORT_LIB=$(PWD)/crates/target/release \
	$(RPYTHON) $(RPYFLAGS) --output=gnitz-server-c gnitz/server/main.py
```

`GNITZ_TRANSPORT_LIB` provides both `-L` (via `library_dirs`) and `-rpath`
(via `link_extra`) in `gnitz/server/rust_ffi.py`.
