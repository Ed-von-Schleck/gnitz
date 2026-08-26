# 3 — Two async clients over one spine, and no I/O thread

The connection is a sans-io state machine — `submit` queues a request,
`step(ready)` does the non-blocking I/O the driver says the fd will accept,
returns the slots that completed, and returns only once nothing buffered can
advance another one, `interest()` reports which directions to watch, and
`close()` abandons every pending slot, returns them, and refuses further work.
Nothing in it waits. Two async clients are added over it: a Rust one for any
runtime, and an asyncio one that replaces `gnitz.aio`'s background I/O thread.

Measured, 400 pushes of 4 rows × 10 columns on one connection, `strace -f -c`,
interpreter startup subtracted:

| | total | writev | recvfrom | futex | epoll_wait | sendto |
|---|---|---|---|---|---|---|
| sync | 1203 | 401 | 802 | ~13 | 0 | 0 |
| `await` in a loop | ~6258 | 404 | 1602 | 3047 | 808 | 400 |
| `asyncio.gather` | ~880 | 5 | 808 | 57 | 15 | 4 |

Syscall counts are the acceptance measure. Wall-clock on this box is
contaminated — the same workload spans 75.6 → 409.0 µs/op across the build and
worker-count configurations the repo produces — so no absolute timing is a
threshold.

The await-in-a-loop excess is the thread: one `sendto` per operation on the
loop's self-pipe, 808 `epoll_wait`, 3047 futex. Per connection it also costs one
OS thread and 480 KiB of RSS before a byte is sent — `sync_channel(4096)`
eagerly allocates `4096 × size_of::<Slot<IoRequest>>()` = 491,520 B.

The 808 `recvfrom` on the `gather` row is two syscalls per reply frame, because
the current reader takes the 4-byte header and the payload separately. The
connection's reader over-reads into a 64 KiB scratch, so a run of pipelined push
ACKs — 4 + `CTRL_BLOCK_SIZE_NO_BLOB` = 260 bytes each — drains ~252 per read.

Acceptance: `await`-in-a-loop reaches 4 syscalls per operation; `gather` falls
below 1; futex and `sendto` reach zero on both. Neither has an assertion today.

## The executor contract

**The spine is the maximal shared unit, and it is shared by all three clients.**
It owns request encoding, the schema LRU, warm/cold push packing, positional
correlation, reply-train reassembly, the retry below, the 4096-request in-flight
cap, and `STATUS_*`→`ClientError` classification. An executor owns exactly two
things: how it waits, and what primitive it resolves.

That line is where it is because **each runtime owns its own reactor**. Only one
thing can be blocked on a fd set; when asyncio is running, its loop *is* the
selector. Sharing across the line means bridging two reactors, which costs a
thread, a cross-thread wakeup and a GIL acquisition from a foreign thread —
which is the current architecture and the 3047 futex calls above.

**`Readiness` and `Connection<R>` are therefore Rust-only and are not on the
Python path.** They exist to turn "step this until done" into a `Future` for a
Rust runtime to poll. asyncio needs no such conversion: it hands out a callback
on readability, and that callback calls `step` directly. Routing Python through
`Connection<R>` reintroduces the thread this plan deletes, which is also why
`pyo3-async-runtimes` is not used.

`close` lives on the spine for the same reason: all three clients need "abandon
everything, refuse further work, resolve each abandoned slot with my own error
value", and three copies of that policy drift. `ClientError` is not `Clone`
(`ProtocolError` carries a `std::io::Error`), so `close` returns the slot ids
and the executor holds the cause.

## Pushes pack warm, and the schema retry quiesces

`PyAsyncTransport::push` encodes with `Some((schema, batch))` unconditionally —
bare `FLAG_PUSH`, no version stamp; `gnitz-py` has no reference to
`schema_version`, `schema_cache` or `wire_flags`. Measured off the wire, async
ships 1428 bytes/push against sync's 780 for 4 rows × 10 columns; the 648-byte
delta is exactly the schema block (88 fixed + 56 per column × 10), 45% of the
frame. Packing on the spine against the connection's own cache removes it, and
both async clients get it by construction.

That makes `STATUS_SCHEMA_MISMATCH` reachable on an async path for the first
time: the server's `decode_push_frame` raises it only for
`has_data && !has_schema`, and every async push sets `FLAG_HAS_SCHEMA` today. So
a retry is required.

A mismatched push **commits nothing** — `decode_push_frame` returns before the
commit path — so a retry re-orders the relation's history only if something for
that relation committed *after* it. That is the one condition to watch, and it
is directly observable while draining.

**Quiesce, retry a contiguous run, resume.** On the first
`STATUS_SCHEMA_MISMATCH` for a relation: evict the cache entry immediately, stop
flushing, and drain the outstanding replies, collecting every further mismatch
for that relation. Then re-send every mismatched push cold, at the *front* of
the outbound queue and in submission order, and only then resume flushing what
queued behind them.

The eviction comes first because it bounds the recovery at one round: a cold
frame sets `FLAG_HAS_SCHEMA` and cannot re-trigger, and any push for that
relation submitted while the drain runs finds no cache entry and is encoded cold
too. Evicting after the drain would let a push submitted during it inherit the
stale stamp, mismatch in turn, and start a second round.

**If any push for that relation ACKs `STATUS_OK` after the first mismatch, the
mismatched slots fail with `ClientError::SchemaMismatch` rather than being
retried.** They ordinarily cannot — every in-flight push for a relation carries
the same stamp, because `submit` encodes at submit time and the cache's only
writer is reply processing. But they can when a reply processed *between* two
submits refreshes that stamp: `scan(T)` and `push(T, v=10)` submitted together,
a concurrent `ALTER TABLE T` upstream, the scan's reply (which arrives first,
positionally) installing the new version, then `push(T, v=20)` encoded warm at
the new version and committed. Retrying the v=10 push behind it leaves the row
at 10. Failing there is what the async path does for every mismatch today, so it
takes nothing away.

Quiescing is what a contiguous run needs, and a tail retry without it corrupts.
`WireConflictMode::Update = 0` is retract-and-insert, last-write-wins. Pipeline
`A(pk=1, v=10)` and `B(pk=1, v=20)`, both warm, both mismatching; between
observing A's mismatch and B's, the caller submits `C(pk=1, v=30)`. A tail retry
appends `A'`, then `C`, then `B'`, and the row ends at 20 though 30 was
submitted last. Retrying "in place" is not a third option: on a stream socket a
retry frame is at the tail by construction.

The re-send needs no re-encode of the rows. `encode_parts` builds the data block
as `encode_wal_block(schema, target_id as u32, batch)`, reading no flags, and
the cold and warm routes produce identical `seek_pk` / `seek_col_idx` / extra —
`PkTuple::EMPTY` splits to `(0, &[])` and `encode_message_noschema_parts` passes
`0, 0, &[]` — so the two frames carry a byte-identical data block and differ
only in `ctrl` and the schema block. `encode_control_block` is `pub(crate)`, so
rebuilding the control block needs no new API. An empty warm push is not a
hazard: `encode_parts` filters an empty batch out of `data`, so `has_data` is
false and the server never reaches the mismatch branch.

**The retry payload is the submitter's.** The push variant of `Request` carries
`Option<Arc<Schema>>` and the slot retains
`(MessageParts, Option<Arc<Schema>>, client_id, base_flags)`. Both async clients
already own an `Arc<Schema>` — `PyZSetBatch.schema` is one and
`AsyncClient::push` takes one — so they pay nothing. **The blocking client
passes `None`**, keeps its own in-place retry, and arms none of this: it has one
operation outstanding and nothing to order against. Threading the retry
uniformly would make `GnitzClient::push`, whose signature takes `&Schema`, pay
`Arc::new(schema.clone())` — an `Arc`, a `Vec` and one `String` per column — on
every call to arm machinery it can never use. A slot carrying `None` that draws
a mismatch simply completes with `ClientError::SchemaMismatch`.

The cached block is not a substitute for the caller's schema: the warm path is
gated on `types_match`, which ignores column names, so the cache can hold a
type-compatible schema with different names than the one the caller encoded
against.

## `Readiness`, and where the tokio impl lives

```rust
/// Waits for any of `interest` on the connection's fd, and reports which
/// directions became ready — the set the caller then hands to `step`.
///
/// The caller guarantees it drives every direction this returns to a drained
/// fd before polling again — an implementation may therefore clear a cached
/// readiness flag eagerly.
pub trait Readiness {
    fn poll_ready(&mut self, cx: &mut Context<'_>, interest: Interest)
        -> Poll<io::Result<Interest>>;
}
```

Returning the ready set rather than `()` is what lets `Connection<R>` call
`step(ready)` instead of `step(READ | WRITE)`: a direction that did not fire
would otherwise cost a syscall per wakeup to discover, which is the same
speculative read the blocking client avoids. `AsyncFd` exposes
`poll_read_ready` and `poll_write_ready` separately, so the union is what the
impl already has to compute.

The drained-before-repolling contract is not a convenience; it is what makes a
correct tokio impl possible. `AsyncFd` caches readiness with edge-triggered
semantics: once epoll reports the fd readable, `poll_read_ready` returns `Ready`
immediately until an `AsyncFdReadyGuard` clears the flag. Poll, drop the guard,
do the I/O yourself, hit `EAGAIN`, poll again → `Ready` again → a busy loop at
100% CPU. tokio's own guidance is the same rule stated from the other side:
attempt the I/O first and poll for readiness only when it fails with
`WouldBlock`.

Clearing eagerly inside `poll_ready` is correct *only* under that contract.
`step(READ)` returns only with the source drained — either an explicit `EAGAIN`
or the short read that proves the queue is empty — and with nothing buffered
that could advance a slot, so the flag genuinely no longer describes the fd and
the executor never has cause to skip a park. A short
read is as good as an `EAGAIN` here for the same reason it is on the blocking
path: it leaves the kernel queue empty, so the next arrival is a fresh edge.
Clearing eagerly without the contract is worse than a busy loop: it waits for an
edge that may never come while data sits in the buffer. The trait therefore
needs no `clear_ready` method, and `async-io`'s `Async<T>`, whose
`poll_readable` re-registers on every call, implements it in ten lines with
nothing to clear.

`AsyncFd` requires the fd to be non-blocking, which the connection's own
`mark_established` guarantees, and it does not close the fd — the inner
`AsRawFd` type does. So the impl wraps the raw fd in a newtype with no `Drop`;
the transport owns the fd and closes it.

**The tokio impl ships as `gnitz-tokio`, a new workspace member, not as a
`gnitz-core` feature.** `gnitz-core` gets no async-runtime dependency, ever.
This is not tidiness: `gnitz-py` depends on `gnitz-core`, and the workspace is
`resolver = "2"`, which splits feature unification for build-dependencies,
proc-macros and inactive target-dependencies but **not** for a normal dependency
shared by two workspace members in one build. A `gnitz-core/tokio` feature
enabled anywhere in `cargo clippy --workspace --all-targets` — which includes
`gnitz-py` — unifies tokio into the pyo3 extension's `gnitz-core`, while
`maturin develop` (run from `crates/gnitz-py`) resolves without it. Two builds
of one crate with different feature sets. A separate leaf crate makes that
unrepresentable, and its tests then run under a plain `cargo test --workspace`
with no Makefile flag.

`CLAUDE.md` gains a crate-table row:

```
| `gnitz-tokio` | The `Readiness` impl that puts the async client on tokio's reactor | `core` |
```

and `crates/Cargo.toml` a `"gnitz-tokio"` member.

## The Rust async client

```rust
/// Owns the connection and the request channel; drains, steps, resolves.
pub struct Connection<R: Readiness> { /* … */ }
impl<R: Readiness> Future for Connection<R> { type Output = Result<(), ClientError>; }

/// A channel sender and a client id. Send + Sync + Clone; owns no connection.
#[derive(Clone)]
pub struct AsyncClient { /* … */ }

impl AsyncClient {
    /// Blocking: the TCP connect and TLS handshake run on the calling thread,
    /// for up to `CONNECT_TIMEOUT` per resolved address. `mk` builds the
    /// readiness source from the connected fd — it cannot be passed in already
    /// built, because the fd does not exist until connect returns. The source
    /// registers the fd and must never close it.
    pub fn connect<R: Readiness>(
        target: &str,
        mk: impl FnOnce(RawFd) -> io::Result<R>,
    ) -> Result<(Self, Connection<R>), ClientError>;

    pub async fn push(&self, tid: u64, schema: Arc<Schema>, batch: ZSetBatch)
        -> Result<u64, ClientError>;
    pub async fn scan(&self, tid: u64) -> Result<ScanReply, ClientError>;
    pub async fn seek(&self, tid: u64, pk: PkTuple) -> Result<ScanReply, ClientError>;
    pub async fn scan_many(&self, tids: &[u64]) -> Result<Vec<ScanReply>, ClientError>;
    pub async fn resolve(&self, schema_name: &str, name: &str)
        -> Result<Option<Arc<RelDescriptor>>, ClientError>;
    pub fn client_id(&self) -> u64;
}
```

`connect` is a plain `fn` because it blocks; an `async fn` that blocks its
executor says the opposite in its type.

**`resolve` is on the surface** because without it the handle cannot address a
relation at all — every other verb takes a `tid` — and the alternative is
opening a second, blocking connection just to look one up. It is a single
control-frame round trip whose only client-side effect is installing the reply's
schema block under the live tid, which the spine's cache absorption already
does; the descriptor is then built from the reply by the same pure construction
`GnitzClient::fetch_descriptor` performs, which reads no client state. It is
**always** a round trip: `GnitzClient::resolve` first consults the
statement-scoped `CatalogSnapshot`, and an async handle has no statement bracket
to scope one to. `PkTuple` is `Copy`, so `seek` takes it by value across the
channel.

**`AsyncClient` is `Clone`.** It is a channel sender; every method takes
`&self`; cloning is what a shared handle should cost. It has no `&mut self`
method to protect precisely because of what is left off it.

**Not on the async surface: `execute_sql`, `transaction`, DDL and id
allocation.** A statement is a plan followed by an interleaved
read/compute/write sequence on a blocking client; exposing it means blocking the
driver task or making SQL execution resumable. A gnitz transaction is the same
shape — `TxnBuffer` buffers every write client-side, the SQL overlay reads
through it, and the OCC basis advances from each ACK — so it is a sequence over
`GnitzClient` state, not a wire verb. `create_table` and friends are likewise
allocate-then-canonicalize-then-write sequences. All stay on the blocking
client. A `Mirror` also stays there: it is `!Send`, so `AsyncClient` will never
feed one.

The request channel is bounded at **256**. The spine's 4096-request cap is the
real in-flight bound and dominates memory; the channel only hands work across,
and a suspended `push` costs nothing — no thread, no allocation — so the depth
need only be enough that a burst rarely round-trips through the scheduler
mid-flight. `Connection` drains the channel into `submit` while the spine is
under its cap, so `push` suspends on a full channel rather than seeing the cap's
error.

`Connection` erroring calls `close` and resolves every returned slot with
that error, then completes. Dropping every `AsyncClient` closes the channel and
ends `Connection`.

## The asyncio executor

It replaces `PyAsyncTransport` entirely: the OS thread, the `sync_channel`,
`IO_CHANNEL_DEPTH`, `IO_BATCH_MAX`, `IoOp`, `IoRequest`, `async_io_loop`,
`fail_all`, `dispatch`, `Resolutions`, `LoopResult`, `RecvKind`, `Pending`,
`classify_recv_err` and `loop_result_to_py` — the contiguous ~500-line block in
`gnitz-py/src/lib.rs` plus its `add_class` registration.

**That is the last consumer of the blocking reply path, which goes with it.**
`Session::send_batch`, `recv_push_ack` and `recv_scan` are deleted;
`pack_scan`, `pack_seek` and `pack_scan_multi` drop to crate visibility, having
been `pub` only for this transport, and become the spine's own encoders. The
private path beneath them — `drain_reply_train`, `recv_cached`, `recv_checked`,
`recv_message` — has no other caller once the spine's accumulator serves every
`GnitzClient` verb, and goes too. So do `ClientTransport::waker`,
`Session::waker`, `TransportWaker` and its two re-exports (`protocol/mod.rs`'s
`pub use transport::{…}` and `lib.rs`'s prelude), whose only consumer is this
transport's teardown; `transport/mod.rs`'s
`test_transport_waker_unblocks_parked_recv` goes with them, and `client.rs`'s
Send-assertion comment loses its "the async transport moves a bare `Session`
into its I/O thread" clause.

`not_null_bit_rejection.rs` drove `send_batch` + `recv_push_ack` from a plain
`#[test]`, to skip `push_with_mode`'s client-side `ZSetBatch::validate` and ship
a null bit under a `NOT NULL` column. It moves to the raw-wire surface
`tls_client.rs` and `slow_client_eviction.rs` already use —
`ClientTransport::connect`, `hello_handshake`, `encode_message_parts`,
`send_framed_iov(&parts.segments())`, `ClientTransport::recv_framed`,
`parse_response` — all public and all staying, with a literal client id as
`tls_client.rs` uses. That leaves one raw-wire test surface rather than two, and
no production entry point gated on a test feature.

One pyo3 crossing per readiness event:

```rust
let done = py.detach(|| conn.step(ready));  // read or flush, decode, advance
// reacquire: for each completed slot, convert its Reply and resolve the
// asyncio.Future this executor holds for it.
```

`ready` is `READ` from the `add_reader` callback and `WRITE` from
`add_writer` — the loop already knows which fired, so neither callback spends a
syscall discovering that the other direction has nothing for it.

Anything per-operation must stay out of Python. That is the argument the deleted
`dispatch` already makes for `call_soon_threadsafe` — CPython allocates a
handle, takes the loop lock and writes the self-pipe per call — one level down:
one GIL acquisition per readable event is independent of N, where a per-op poll
or resolve callback on a `gather` of 1000 is 1000 of them. The
`SlotId -> Py<PyAny>` map lives in `gnitz-py`, not the spine.

Because the reader callback runs **on** the loop thread, futures resolve
directly. `aio.py`'s `_resolve_batch`, the `resolve_batch_fn` constructor
argument, the bound `call_soon_threadsafe` and the four positionally-aligned
lists all go; the constructor becomes `AsyncTransport(socket_path, event_loop)`.

`interest()` arms `loop.add_reader` and `loop.add_writer`, and **disarms each
the moment its bit drops** — a permanently-armed writer callback spins the loop
at 100% CPU on an always-writable fd. Submissions arm at most one
`loop.call_soon` per idle→pending transition, not one per submit, so a `gather`
of N still leaves in one `writev`.

`close()` disarms both callbacks and fails every slot the spine's `close`
returns; further submits are refused with
`ClientError::ServerError("connection closed")` — the text
`test_enqueue_after_close_raises` matches. There is no thread to join,
so `Drop` does the same and the "do NOT join from GC" comment goes with it.

Deleting the thread removes a shutdown crash without a finalizer: a release
build today dies with `Fatal Python error: gilstate_tss_set: failed to set
current tstate` when an exception escapes `asyncio.run` with work in flight —
the I/O thread calling `Python::attach` after finalization began.

`aio.py`'s "All I/O runs on a background Rust thread" becomes false and is
replaced. Two limitations stay and are documented as choices: connect is
synchronous and blocks the loop for up to `CONNECT_TIMEOUT` per resolved
address, and `AsyncConnection` stays bound to its constructing loop, because
`add_reader` is a property of that loop. A third is documented for the first
time: abandoning an operation is not cancellation — `asyncio.wait_for(conn.push(...), t)`
still writes the frame, and the server still commits it.

`client_id` stays exposed on the connection object: `test_distinct_client_ids`
is the only coverage of the shared-generator invariant that stops one process
minting the same client id twice.

## Tests

**Deleted**: `test_drop_without_close_releases_thread` and
`test_explicit_close_no_deadlock` in `test_async.py`, and with them the file's
`gc` and `time` imports and `_os_thread_count`;
`test_transport_waker_unblocks_parked_recv` in `transport/mod.rs`.

**Moved**: `not_null_bit_rejection.rs` to the raw-`ClientTransport` surface.

**Rewritten**: `test_connection_loss_resolves_every_queued_request`, whose
docstring is entirely about `IO_BATCH_MAX` and the request channel. Its 3000
submissions stay — below the 4096 cap — and its assertion is unchanged.

**Must keep passing**: the rest of `test_async.py`, in particular
`test_pipeline_mixes_operation_kinds` (push, scan, seek and `scan_many` gathered
as one in-flight group, run at `GNITZ_WORKERS=4` so replies leave the workers
out of order), `test_pipeline_empty_push_interleaved`,
`test_scan_many_malformed_list_does_not_desync`, `test_close_idempotent` and
`test_enqueue_after_close_raises`.

**New**:
- Two pipelined pushes to one PK both draw `STATUS_SCHEMA_MISMATCH`, and a third
  to the same PK is submitted between their replies: the committed row is the
  third value. Fails against a tail retry that does not quiesce.
- Every warm push in flight at a stale version is retried, in order, and each
  future resolves to its own ACK — not only the first to mismatch.
- A push for the relation that ACKs OK after a mismatch fails the mismatched
  slots instead of retrying behind it; the committed row is the one that
  succeeded.
- A warm async push ships 780 bytes, not 1428, counted off the wire.
- `Readiness` under tokio: a connection idle after a completed operation
  consumes no CPU over a one-second window, measured as process CPU time. This
  is the busy-loop guard and it is the reason the eager-clear contract exists.
- The actor: N operations on cloned `AsyncClient` handles across tasks complete
  correctly and leave in one `writev`; dropping every handle ends `Connection`;
  `Connection` erroring fails every outstanding operation rather than hanging
  one; `resolve` on the async handle returns the same descriptor the blocking
  client does.
- Python, release build: every shutdown shape exits 0, in particular an
  exception escaping `asyncio.run` with work in flight.
- An idle `AsyncConnection` with one operation outstanding consumes no CPU —
  the `add_writer` disarm guard, which a spinning loop would fail.
- `await`-in-a-loop costs 4 syscalls per operation and `gather` fewer than 1,
  both with futex and `sendto` at zero, counted with `strace -f -c`.

## Sequencing

- [ ] Warm push packing on the spine, the push `Request` variant's
      `Option<Arc<Schema>>`, and the quiescing retry with its
      committed-after-mismatch guard. The blocking client passes `None` and is
      unchanged.
- [ ] `Readiness` added to `gnitz-core`, returning the ready set, with the
      drive-before-repolling contract on the trait.
- [ ] `gnitz-tokio`: the workspace member, the non-owning fd newtype, the
      eager-clearing `AsyncFd` impl, the idle-CPU test, and the `CLAUDE.md`
      crate-table row.
- [ ] `Connection<R>`, `AsyncClient`, the 256-deep channel, and the actor tests.
- [ ] The asyncio executor over one `step` crossing per readable event, with the
      interest arm/disarm and the single `call_soon` per idle→pending
      transition; `PyAsyncTransport` and its ~500-line block deleted;
      `_resolve_batch` and `call_soon_threadsafe` gone from `aio.py`;
      `client_id` re-exposed; the three documented limitations written down.
- [ ] The blocking reply path deleted with its last consumer: `send_batch` /
      `recv_push_ack` / `recv_scan`, the `pack_*` visibility drop,
      `drain_reply_train` / `recv_cached` / `recv_checked` / `recv_message`,
      `TransportWaker` with both `waker()` methods and its two re-exports, and
      `not_null_bit_rejection.rs` moved to the raw-wire surface.
