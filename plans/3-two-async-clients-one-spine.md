# 3 — Two async clients over one spine, and no I/O thread

`Session` is a sans-io state machine — `submit` encodes against the
connection's schema cache and registers a slot, `step(ready)` does the I/O the
driver says the fd will accept and returns the slots that completed, returning
only once nothing buffered can advance another one, `interest()` reports which
directions to watch, `close()` abandons every pending slot, returns them, and
refuses further work. Nothing in it waits. Two async clients are added over it:
a Rust one for any runtime, and an asyncio one that replaces `gnitz.aio`'s
background I/O thread.

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
the thread reads through `recv_framed`, which takes the 4-byte header and the
payload separately. The spine's reader over-reads into a 64 KiB scratch, so a
run of pipelined push ACKs — 4 + `CTRL_BLOCK_SIZE_NO_BLOB` = 260 bytes each —
drains ~252 per read.

**Acceptance.** `await`-in-a-loop costs 5 syscalls per operation: `writev`,
`recvfrom`, one blocking `epoll_wait`, and two zero-timeout `epoll_wait`s that
are asyncio's own turn cost — `_run_once` selects with `timeout = 0` whenever a
handle is ready, and each operation schedules two handles: the deferred flush,
and the task wake-up `Future.set_result` posts. They cannot be folded: a handle
scheduled during an iteration runs in the next one. `gather` costs fewer than 1
per operation. Futex and `sendto` reach zero on both. Neither has an assertion
today; the blocking client has one (`three_syscalls_per_push_*` in
`spine_driver.rs`), and its `strace -f -c` harness is the pattern.

## The executor contract

**The spine is the maximal shared unit, and it is shared by all three clients.**
It owns request encoding, the schema LRU, warm/cold push packing, positional
correlation, reply-train reassembly, the schema-mismatch resend below, the
`MAX_IN_FLIGHT` cap, and `STATUS_*`→`ClientError` classification. An executor
owns exactly two things: how it waits, and what primitive it resolves.

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

Three spine surfaces the blocking verbs keep private today become `pub`, because
both async clients need them and each would otherwise re-derive them:

- `Session::scan_reply(tid, ReplyTrain) -> ScanReply` (today's `narrow_train`):
  the schema the train carried else the cached one, its rows, the terminal
  watermark. `ReplyTrain.schema` is only the in-frame block, so a warm scan
  reply is unusable without this fallback.
- `Request::scan(tid)` and `Request::seek(tid, &PkTuple)` constructors, so a
  driver never spells `FLAG_SEEK` and `split_wire` itself; `seek_roundtrip`
  uses them too.
- RESOLVE split into `Session::resolve_request(RelTarget) -> Vec<u8>` (the
  frame; submitted as `Request::Uncorrelated`) and
  `Session::resolve_reply(ReplyTrain) -> Option<(u64, Arc<Schema>, RelDescriptorBlob)>`,
  which merges the descriptor's FKs into the schema and installs it under the
  **live** tid. The slot is uncorrelated, so `feed` absorbs nothing for it —
  the install is this method's, not the accumulator's. `RelTarget` becomes
  `pub`. `RelDescriptor::from_resolve(tid, schema, blob)` is the pure
  construction `GnitzClient::fetch_descriptor` performs today, moved onto the
  type so the async handle builds the same descriptor.

## Pushes pack warm, and a mismatch resends

`PyAsyncTransport::push` encodes with `Some((schema, batch))` unconditionally —
bare `FLAG_PUSH`, no version stamp; `gnitz-py` has no reference to
`schema_version`, `schema_cache` or `wire_flags`. Measured off the wire, async
ships 1428 bytes/push against sync's 780 for 4 rows × 10 columns; the 648-byte
delta is exactly the schema block (88 fixed + 56 per column × 10), 45% of the
frame. `submit`'s push arm already packs warm against the connection's own
cache, so both async clients get it by construction.

That makes `STATUS_SCHEMA_MISMATCH` reachable on an async path: the server's
`decode_push_frame` raises it only for `has_data && !has_schema`, and every
async push sets `FLAG_HAS_SCHEMA` today. A mismatched push **commits nothing**
— `decode_push_frame` returns before the commit path — and a cold frame cannot
draw it. So the spine resends cold, and the only thing to get right is order:
a relation's pushes must commit in submission order.

**Every push at the stale stamp is resent at once, in order, at the tail.**
A push slot records the version it was stamped with (`0` for cold). On a
`STATUS_SCHEMA_MISMATCH` reply to a push slot for relation `T` at stamp `v`:

1. Evict `T`'s cache entry if it still holds `v`. A scan reply may already have
   installed a newer block; that one stays.
2. Let `S` be every pending push slot for `T` at stamp `v`, in queue order —
   the replying head first, then sent-but-unanswered, then still-queued.
   Every one of them will draw the same reply: the server's version is not `v`,
   `submit` encodes at submit time, and the cache's only writer is reply
   processing.
3. If a push for `T` that is **not** at stamp `v` — a different stamp, or cold —
   is pending behind the first member of `S`, nothing is resent: the head
   completes with `ClientError::SchemaMismatch`, and the rest of `S` complete as
   their own replies say. Resending `S` behind that push would reorder the
   relation's history. It arises when a reply processed *between* two submits
   refreshed the stamp: `scan(T)` and `push(T, v=10)` submitted together, a
   concurrent `ALTER TABLE T` upstream, the scan's reply (first, positionally)
   installing the new version, then `push(T, v=20)` encoded warm at it. Failing
   is what the async path does for every mismatch today, so it takes nothing
   away.
4. Otherwise, for each member of `S` in order: enqueue its cold frame at the
   tail and register a new pending entry carrying the **same** `SlotId`; mark
   the original superseded. The head's own entry is first, and it is popped now
   and reported to nobody.
5. A superseded entry's reply is consumed and discarded, whatever its status —
   it keeps positional correlation and nothing else. The one status it cannot
   carry is `STATUS_OK`: the server's version counter only moves forward
   (`invalidate_col_names` increments, wrapping 65535 → 1), so a stamp it has
   refused is not accepted again short of 65,534 further ALTERs on one
   relation inside one server boot.

Nothing pauses, and there is no drain phase: the resends are appended in one
pass, so a push submitted afterwards queues behind all of them, and it is
encoded cold because the eviction came first. Submitting `A(pk=1, v=10)` and
`B(pk=1, v=20)` warm at a stale stamp, then `C(pk=1, v=30)` after `A`'s
mismatch, commits `A'`, `B'`, `C` — the row ends at 30. A retry that resends
each push as its own mismatch arrives commits `A'`, `C`, `B'`. That the
still-queued members of `S` go out warm and mismatch before their cold copies
is bytes, not history: they commit nothing.

A resend counts in `requests_sent` and in `pending.len()` — so the cap is
transiently tighter by the number of superseded entries still awaiting their
reply — and `close()` reports each `SlotId` once, skipping superseded entries.

**The retained payload is the submitter's, and it is shared, not copied.**
`MessageParts.data` becomes `Arc<Vec<u8>>` — the encoded data block is the one
allocation a push makes that the cold frame reuses byte-for-byte:
`encode_parts` builds it as `encode_wal_block(schema, target_id as u32, batch)`,
reading no flags, and the cold and warm routes produce identical
`seek_pk` / `seek_col_idx` / extra, so the two frames differ only in `ctrl` and
the schema block. The push slot retains `(Arc<Vec<u8>>, Arc<Schema>, base
flags)`; the cold `ctrl` is rebuilt from the base flags through
`encode_control_block`, which is `pub(crate)` already, and the schema block
from the `Arc<Schema>` — which is **not** the cached block: the warm gate is
`types_match`, which ignores column names, so the cache can hold a
type-compatible schema with different names than the one the rows were encoded
against. A control-only frame's `data` is an empty `Arc<Vec<u8>>` — one small
allocation, no buffer. An empty batch is not a hazard: `encode_parts` filters it
out of `data`, so `has_data` is false and the server never reaches the mismatch
branch.

The push variant of `Request` carries its schema as

```rust
pub enum PushSchema<'a> {
    Borrowed(&'a Schema),
    Shared(Arc<Schema>),
}
```

`Shared` is what makes a slot resendable. Both async clients already own an
`Arc<Schema>` — `PyZSetBatch.schema` is one and `AsyncClient::push` takes one —
so they pay a refcount bump. **The blocking client passes `Borrowed`** and keeps
its own retry in `roundtrip_push`: it has one live operation and nothing to
order against, and threading `Shared` through `GnitzClient::push`, whose
signature takes `&Schema`, would make every call pay `Arc::new(schema.clone())`
— an `Arc`, a `Vec` and one `String` per column — to arm machinery it can never
use. A `Borrowed` slot that draws a mismatch completes with
`ClientError::SchemaMismatch`, which is what `roundtrip_push` matches on. Its
retry then needs no `cold` flag: it evicts the entry and resubmits, and a
relation with no cache entry encodes cold. The `cold` field on `Request::Push`
goes.

## `Readiness`

```rust
/// A runtime's readiness source for the connection's fd.
///
/// Waits for any direction in `interest`. Once at least one is ready, runs
/// `io` with the ready set — the set the caller hands to `step` — and `io`
/// returns, with its result, the directions it exhausted: proven drained, or
/// refused with `WouldBlock`. An implementation that caches readiness forgets
/// exactly those directions, as observed *before* `io` ran.
pub trait Readiness {
    fn poll_io<T>(
        &mut self,
        cx: &mut Context<'_>,
        interest: Interest,
        io: impl FnOnce(Interest) -> (T, Interest),
    ) -> Poll<io::Result<T>>;
}
```

Passing the ready set rather than `()` is what lets `Connection<R>` call
`step(ready)` instead of `step(READ | WRITE)`: a direction that did not fire
would otherwise cost a syscall per wakeup to discover, which is the same
speculative read the blocking client avoids.

The closure shape, rather than a `poll_ready` that returns the set, is forced
by tokio's `AsyncFd`, which caches readiness with edge-triggered semantics: once
epoll reports the fd readable, `poll_read_ready` returns `Ready` until an
`AsyncFdReadyGuard` clears the flag. Which directions may be cleared, and when,
differ:

- **Read is exhausted by every step.** `step(READ)` returns only with the
  source drained — an explicit `EAGAIN`, or the short read that proves the
  queue was empty — and nothing buffered that could advance a slot. A short
  read is as good as an `EAGAIN` here: `recv` on a `SOCK_STREAM` copies until
  the buffer is full or the receive queue is empty. Measured against a queue
  quiesced on `FIONREAD` with a probe buffer strictly larger than it: no short
  read left a byte behind, on AF_UNIX or TCP, at every queued size from 260 B
  to 1 MiB, across 300 multi-burst trials on each transport. Data arriving
  after the read returns is a fresh edge — `sk_data_ready` fires on every
  arrival — which is what the cleared flag then waits for.
- **Write is exhausted only by `WouldBlock`**, which after a step is exactly
  `interest().write` still set: `flush` loops until the queue is empty or the
  fd refuses. Clearing it after a flush that *succeeded* hangs on TCP. The
  kernel's own comment (`net/core/stream.c`, `sk_stream_wait_memory`): "When
  TCP receives ACK packets that make room, tcp_check_space() only calls
  tcp_new_space() if SOCK_NOSPACE is set" — and `SOCK_NOSPACE` is set by the
  refusal, so a socket that never refused never produces an `EPOLLOUT` edge.
  What does re-set the flag is the next *read* event — epoll reports an item's
  whole current mask (`ep_item_poll`, masked by the interest set) on every
  wakeup — so a submit issued while a reply is outstanding would wait for that
  reply before it could flush: pipelining destroyed rather than a hang, and
  invisible to a test that submits everything before the first flush. AF_UNIX
  edges on every consumed skb (`unix_write_space`), which only hides it.

The guard's tick makes the before/after distinction safe: `clear_readiness`
runs `set_readiness(Tick::Clear(event.tick), …)`, which returns without clearing
when the driver has since set readiness under a newer tick. So an edge that
arrives between the poll and the clear is kept, and a clear of the guard
obtained *before* `io` never discards an event `io` did not see. The tokio impl
is therefore: poll `poll_read_ready` / `poll_write_ready` for the directions in
`interest`; if neither is `Ready`, `Pending`; else run `io` with the union, and
`clear_ready` on each guard whose direction `io` reports exhausted. The two
guards borrow the `AsyncFd` immutably and coexist. `Connection`'s `io` is
`step(ready)`, reporting `read: ready.read, write: ready.write &&
interest().write`.

`async-io`'s `Async<T>`, whose `poll_readable` re-registers on every call,
implements the trait with nothing to clear: poll, run `io`, return. It is the
witness that the trait is runtime-neutral; it is not shipped.

`AsyncFd` requires the fd to be non-blocking, which the connection's own
`mark_established` guarantees, and it does not close the fd — the inner
`AsRawFd` type does. So the impl wraps the raw fd in a newtype with no `Drop`;
the transport owns the fd and closes it. `AsyncFd::new` must run inside a tokio
runtime context, so `connect` must be called from one.

## Where it lives: `gnitz-async`

**One new workspace member, `gnitz-async`, holds `Readiness`, `Connection<R>`,
`AsyncClient`, and the tokio impl behind a `tokio` feature that is on by
default.** `gnitz-core` gets no async dependency, ever, and nothing else gains a
`tokio` feature: `gnitz-py` depends on `gnitz-core`, and the workspace is
`resolver = "2"`, which splits feature unification for build-dependencies,
proc-macros and inactive target-dependencies but **not** for a normal
dependency shared by two workspace members in one build. A `gnitz-core/tokio`
feature enabled anywhere in `cargo clippy --workspace --all-targets` — which
includes `gnitz-py` — would unify tokio into the pyo3 extension's `gnitz-core`,
while `maturin develop` (run from `crates/gnitz-py`) resolves without it: two
builds of one crate with different feature sets. Nothing depends on
`gnitz-async`, so a feature on it unifies into nothing; a runtime other than
tokio takes it with `default-features = false` and implements the ten-line
trait. Its tests run under a plain `cargo test --workspace`; the tokio-bound
ones are `#[cfg(feature = "tokio")]`.

Dependencies: `gnitz-core`, `futures-channel` (the bounded mpsc and the
oneshot) and `futures-core` (the `Stream` trait `Receiver::poll_next` needs),
`tokio` with `net` + `rt` under the feature, `tokio`'s `rt-multi-thread` +
`macros` and `gnitz-test-harness` as dev-dependencies. `std::future::poll_fn`
is the only other async primitive used. No `futures-util`.

`CLAUDE.md` gains a crate-table row:

```
| `gnitz-async` | The Rust async client: a `Connection` future over any runtime's `Readiness`, tokio's impl by default | `core` |
```

and `crates/Cargo.toml` a `"gnitz-async"` member.

## The Rust async client

```rust
/// Owns the connection and the request channel; drains, steps, resolves.
pub struct Connection<R: Readiness> { /* … */ }
impl<R: Readiness + Unpin> Future for Connection<R> { type Output = Result<(), ClientError>; }

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

/// `#[cfg(feature = "tokio")]`: `connect` with the `AsyncFd` source.
pub fn connect(target: &str) -> Result<(AsyncClient, Connection<TokioReadiness>), ClientError>;
```

`connect` is a plain `fn` because it blocks; an `async fn` that blocks its
executor says the opposite in its type.

What crosses the channel is owned — `Request<'_>` borrows, and the borrow ends
at `submit`:

```rust
enum Op {
    Push { tid: u64, schema: Arc<Schema>, batch: ZSetBatch },
    Scan(u64),
    Seek(u64, PkTuple),
    ScanMany(Vec<u64>),
    Resolve(String),          // the canonical "schema.name"
}
struct Submission { op: Op, reply: oneshot::Sender<Result<Reply, ClientError>> }
```

`Connection` encodes on its own task at `submit` — the push arm submits
`PushSchema::Shared(schema)` and validates the batch first, as
`push_with_mode` does — and keeps `SlotId → oneshot::Sender` until the slot
completes. Each verb's `async fn` sends its `Submission`, awaits the receiver,
and narrows the `Reply`: `terminal.seek_pk as u64` for a push (conflict mode
`Update`, as the asyncio push), `scan_reply`
for a scan or seek, `scan_reply` per tid for `scan_many`, `resolve_reply` then
`RelDescriptor::from_resolve` for a resolve. The narrowing needs the session,
so it runs on the driver before the oneshot is fired; the oneshot carries the
narrowed value.

**`resolve` is on the surface** because without it the handle cannot address a
relation at all — every other verb takes a `tid` — and the alternative is
opening a second, blocking connection just to look one up. It is **always** a
round trip: `GnitzClient::resolve` first consults the statement-scoped
`CatalogSnapshot`, and an async handle has no statement bracket to scope one
to. `PkTuple` is `Copy`, so `seek` takes it by value.

**`AsyncClient` is `Clone`.** It is a channel sender; every method takes
`&self`; cloning is what a shared handle should cost. It has no `&mut self`
method to protect precisely because of what is left off it. Each verb clones
the sender for its own send — `Sender::poll_ready` takes `&mut self` — which is
an atomic increment and decrement, and lends the channel one extra slot for
the call's duration.

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

The request channel is `futures_channel::mpsc::channel(256)`. The spine's
`MAX_IN_FLIGHT` is the real in-flight bound and dominates memory; the channel
only hands work across, and a suspended `push` costs nothing — no thread, no
allocation — so the depth need only be enough that a burst rarely round-trips
through the scheduler mid-flight. `Connection` drains the channel into `submit`
only while `pending.len() < MAX_IN_FLIGHT`, so `push` suspends on a full
channel rather than seeing the cap's error.

`Connection::poll` is one loop: drain the channel while under the cap; if
`interest()` is empty, `Pending` on the channel alone; else `poll_io` with
`step`, resolve every completion, and go round again — it returns `Pending`
only when both the channel and the readiness source are. A `step` error calls
`close`, resolves every returned slot with that error, and completes with it.
When every `AsyncClient` is dropped the channel closes and `Connection`
completes with `Ok(())`; a `push` future that was still alive holds a borrow of
its handle, so no listener can be left behind. Dropping a verb's future after
its submission was sent is not cancellation: the frame is written and the
server commits it; the oneshot's receiver is gone and the driver drops the
result.

## The asyncio executor

It replaces `PyAsyncTransport` entirely: the OS thread, the `sync_channel`,
`IO_CHANNEL_DEPTH`, `IO_BATCH_MAX`, `IoOp`, `IoRequest`, `async_io_loop`,
`fail_all`, `dispatch`, `Resolutions`, `LoopResult`, `RecvKind`, `Pending`,
`classify_recv_err` and `loop_result_to_py` — the contiguous ~500-line block in
`gnitz-py/src/lib.rs` plus its `add_class` registration.

**Rust owns the session, the slot map and resolution; Python owns the loop.**
The `#[pyclass] AsyncTransport` holds the `Session`, `create_future` bound
once, and `SlotId → (future, include_hidden)`. It exposes `fileno()`, the four
verbs, `close()`, `client_id`, and two step entry points:

```rust
/// The loop's reader callback: one pyo3 crossing per readable event.
fn on_readable(&mut self, py) -> PyResult<bool>   // returns interest().write
/// The loop's writer callback, and the deferred flush after submits.
fn on_writable(&mut self, py) -> PyResult<bool>   // returns interest().write
```

Each runs `let done = py.detach(|| session.step(ready))`, then, attached, for
every completed slot converts its `Reply` with the existing `triple_to_lazy` /
`classified_err` and calls `set_result` / `set_exception` on the slot's future
(skipping one already `done()`: a cancelled future refuses a result). The
`bool` is `interest().write` after the step, and it is read on **every** step,
not only writes: on TLS a read can queue an alert or key update that
`wants_write` then reports. A `scan_many` slot keeps its tid list beside the
future, as the blocking `scan_multi` keeps it in its frame, so each train
narrows under its own relation. A `step` error — the peer's EOF or reset, an
out-of-order reply — closes the transport exactly as `Connection` does: the
spine's `close`, every returned slot failed with that error, both callbacks
removed. A callback the loop had already queued before the removal finds the
session closed and `step` returns nothing.

`aio.py` arms and disarms from those booleans. The reader is armed once, at
construction, and removed at close: an armed reader on a quiet socket costs
nothing, while arming per operation would cost two `epoll_ctl` per operation.
The writer is armed only while the step reported `write` — a permanently-armed
writer callback spins the loop at 100% CPU on an always-writable fd — and
removed the moment a step reports it clear. A submit schedules
`loop.call_soon(on_writable)` once per idle→pending transition, not once per
submit: a Python-side flag is raised by the submit that schedules the handle
and lowered by the first step that reports `write` clear — so while the flush
handle is pending, or the writer is armed after a `WouldBlock`, a further
submit schedules nothing and rides the flush already coming. A `gather` of N
therefore leaves in one `writev`.

The `SlotId → future` map lives in `gnitz-py`, not the spine, and anything
per-operation stays out of Python: the reader callback is one GIL crossing per
readable event, independent of N, where a per-op poll or resolve callback on a
`gather` of 1000 is 1000 of them. Because it runs **on** the loop thread,
futures resolve directly. `aio.py`'s `_resolve_batch`, the `resolve_batch_fn`
constructor argument, the bound `call_soon_threadsafe` and the four
positionally-aligned lists all go; the constructor becomes
`AsyncTransport(socket_path, event_loop)`.

`close()` removes both callbacks, then runs the spine's `close` and fails every
slot it returns; further submits are refused with
`ClientError::ServerError("connection closed")` — the text
`test_enqueue_after_close_raises` matches. Removal precedes the fd's close so
the selector never holds a dead fd. There is no thread to join, and the "do NOT
join from GC" comment goes.

**An `AsyncConnection` that is never closed lives as long as its loop.**
`add_reader` holds a bound method of the transport, so the loop keeps the object
alive until `close()` removes the callback or the loop itself closes, and only
then does `Drop` run — closing the fd through the `Session`. That is the
lifetime asyncio's own selector transports have, and it is documented as such
rather than papered over with a weak reference.

Deleting the thread removes a shutdown crash without a finalizer: a release
build today dies with `Fatal Python error: gilstate_tss_set: failed to set
current tstate` when an exception escapes `asyncio.run` with work in flight —
the I/O thread calling `Python::attach` after finalization began.

`aio.py`'s "All I/O runs on a background Rust thread" becomes false and is
replaced. Three limitations are documented as choices: connect is synchronous
and blocks the loop for up to `CONNECT_TIMEOUT` per resolved address;
`AsyncConnection` is bound to its constructing loop, because `add_reader` is a
property of that loop; and abandoning an operation is not cancellation —
`asyncio.wait_for(conn.push(...), t)` still writes the frame, and the server
still commits it.

`client_id` stays exposed on the connection object: `test_distinct_client_ids`
is the only coverage of the shared-generator invariant that stops one process
minting the same client id twice.

## The blocking reply path goes with its last consumer

`Session::send_batch`, `recv_push_ack`, `recv_scan`, `pack_scan`, `pack_seek`
and `pack_scan_multi` are deleted — `submit` encodes every one of those shapes
already, through `encode_control_frame` / `versioned_flags` /
`encode_scan_multi_frame`, so nothing "becomes the spine's encoder"; the
`pack_*` are dead. So are `drain_reply_train` and `recv_cached`. `recv_message`
goes too; the `message.rs` tests that call it read
`parse_response(&t.recv_framed()?, hint)` instead, which is what it is. So do
`ClientTransport::waker`, `Session::waker`, `TransportWaker` and its two
re-exports (`protocol/mod.rs`'s `pub use transport::{…}` and `lib.rs`'s
prelude), whose only consumer is the deleted transport's teardown, and both
waker tests in `transport/tests.rs`,
`test_transport_waker_unblocks_parked_recv` and
`test_transport_waker_unblocks_parked_poll`.

Four comments describe the thread and are rewritten, not trimmed: the
`connection.rs` module header (the "blocking reply path the gnitz-py
background I/O transport still calls" paragraph), `Session`'s doc ("the
gnitz-py async I/O thread holds its own"), `MAX_IN_FLIGHT`'s doc ("the async
transport's channel"), and the Send-assertion comments in `client.rs` and
`transport/mod.rs`. The `Send` requirement itself stays and gains its real
reason: the asyncio executor steps the `Session` inside `Python::detach`, whose
`Ungil` bound is `Send` on the closure.

`ClientTransport` gains `bytes_sent()` beside `frames_sent()`, counted where
frames are: in `enqueue`. It is what the warm-bytes test reads.

`not_null_bit_rejection.rs` drove `send_batch` + `recv_push_ack` from a plain
`#[test]`, to skip `push_with_mode`'s client-side `ZSetBatch::validate` and ship
a null bit under a `NOT NULL` column. `submit` runs no validator either, so it
moves onto the spine: `Session::connect`, `submit(Request::Push { schema:
PushSchema::Borrowed(..), .. })`, then `step` / `poll` / `step` as
`spine_driver.rs`'s `drive_all` does. No raw-wire surface, no second copy of
the handshake.

## Tests

**Deleted**: `test_drop_without_close_releases_thread` and
`test_explicit_close_no_deadlock` in `test_async.py`, and with them the file's
`gc` and `time` imports and `_os_thread_count`; both waker tests in
`transport/tests.rs`.

**Moved**: `not_null_bit_rejection.rs` onto the spine.

**Rewritten**: `test_connection_loss_resolves_every_queued_request`, whose
docstring is entirely about `IO_BATCH_MAX` and the request channel. Its 3000
submissions stay — below `MAX_IN_FLIGHT` — and its assertion is unchanged:
the flush hits the reset or the reader reads the EOF, that step's error
closes, and every slot fails.

**Must keep passing**: the rest of `test_async.py`, in particular
`test_pipeline_mixes_operation_kinds` (push, scan, seek and `scan_many` gathered
as one in-flight group, run at `GNITZ_WORKERS=4` so replies leave the workers
out of order), `test_pipeline_empty_push_interleaved`,
`test_scan_many_malformed_list_does_not_desync`, `test_close_idempotent` and
`test_enqueue_after_close_raises`; `push_retries_cold_on_schema_mismatch` in
`spine_tests.rs`, whose `requests_sent() == 3` holds unchanged.

**New — the resend, over the scripted peer of `spine_tests.rs`**, where the
policy lives and a mismatch is one line to script:
- Two warm pushes to one PK at a stale stamp and a third submitted between
  the first mismatch and the second reply: the peer sees `A'`, `B'`, `C` in
  that order, each slot completes exactly once with its cold copy's ACK, and
  the superseded replies complete nothing. Fails against a retry that resends
  per mismatch.
- A push at a different stamp pending behind a stale one: the stale slots
  complete with `SchemaMismatch`, the other with its ACK, and the peer sees no
  resend.
- A `Borrowed` slot draws `SchemaMismatch` and nothing is resent.
- `close()` after a resend reports each `SlotId` once.

**New — end to end**, in `test_async.py`: warm the cache, `ALTER TABLE …
RENAME COLUMN` through the sync client — it bumps the version and changes no
type, and the server's cold gate compares count, types and nullability, never
names — then gather pushes encoded against the old schema: every future
resolves to its own ACK and the rows are committed. And a warm async push
ships 780 bytes, not 1428, read off `bytes_sent`.

**New — `gnitz-async`**:
- A connection idle after a completed operation consumes no CPU over a
  one-second window, measured as process CPU time: the busy-loop guard.
- A submit issued while a reply is outstanding flushes before that reply
  arrives — hold the server's reply with a scripted peer, submit a second
  operation, and assert the peer receives its frame. This is the test the
  write-exhaustion rule exists for, and eager clearing fails it on TCP.
- N operations on cloned `AsyncClient` handles across tasks complete
  correctly, each to its own result; a burst of N from one task on a
  `current_thread` runtime leaves in one `writev`, counted as
  `count_syscalls` counts — across tasks on a multi-thread runtime the driver
  may be polled between two sends, so the count is only bounded there;
  dropping every handle ends `Connection`;
  `Connection` erroring fails every outstanding operation rather than hanging
  one; `resolve` on the async handle returns the same descriptor the blocking
  client does.

**New — Python**:
- Every shutdown shape exits with the exception's status and never an abort,
  in particular an exception escaping `asyncio.run` with work in flight, run in
  a subprocess.
- An idle `AsyncConnection` with one operation outstanding consumes no CPU —
  the writer-disarm guard, which a spinning loop would fail.
- `await`-in-a-loop costs 5 syscalls per operation and `gather` fewer than 1,
  both with futex and `sendto` at zero, counted with `strace -f -c` over a
  subprocess, skipped when `strace` is absent, as `count_syscalls` does.

## Sequencing

- [ ] `PushSchema`, the retained payload with `MessageParts.data: Arc<Vec<u8>>`,
      the stamp on the push slot, the resend with its ordering guard, the
      `cold` field removed and `roundtrip_push` on evict-and-resubmit; the
      scripted-peer tests.
- [ ] The spine surfaces: `scan_reply`, `Request::scan` / `Request::seek`, the
      RESOLVE split with `RelTarget` public and `RelDescriptor::from_resolve`,
      `bytes_sent`.
- [ ] `gnitz-async`: the workspace member and `CLAUDE.md` row, `Readiness`,
      the tokio impl over the non-owning fd newtype, `Connection<R>`,
      `AsyncClient`, `connect`, and its tests including the idle-CPU and
      flush-while-outstanding guards.
- [ ] The asyncio executor: the `AsyncTransport` pyclass over one `step`
      crossing per event, `aio.py`'s arming from the returned booleans and the
      one `call_soon` per idle→pending transition, `PyAsyncTransport` and its
      block deleted, `_resolve_batch` and `call_soon_threadsafe` gone,
      `client_id` re-exposed, the three limitations and the lifetime written
      down; the Python tests above.
- [ ] The blocking reply path deleted: `send_batch` / `recv_push_ack` /
      `recv_scan` / the three `pack_*` / `drain_reply_train` / `recv_cached` /
      `recv_message`, `TransportWaker` with both `waker()` methods, both
      re-exports and both tests, the four comments rewritten, and
      `not_null_bit_rejection.rs` moved onto the spine.
