# 3 — Two async clients over one spine, and no I/O thread

`Session` is a sans-io state machine — `submit` encodes against the
connection's schema cache and registers a slot, `step(ready)` does the I/O the
driver says the fd will accept and returns the slots that completed, returning
only once nothing buffered can advance another one, `interest()` reports which
directions to watch, `close()` abandons every pending slot, returns them, and
refuses further work. Nothing in it waits. Two async clients are added over it:
a Rust one on tokio, and an asyncio one that replaces `gnitz.aio`'s
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
correlation, reply-train reassembly, the schema-mismatch eviction below, the
`MAX_IN_FLIGHT` cap, and `STATUS_*`→`ClientError` classification. An executor
owns exactly two things: how it waits, and what primitive it resolves.

That line is where it is because **each runtime owns its own reactor**. Only one
thing can be blocked on a fd set; when asyncio is running, its loop *is* the
selector. Sharing across the line means bridging two reactors, which costs a
thread, a cross-thread wakeup and a GIL acquisition from a foreign thread —
which is the current architecture and the 3047 futex calls above.

**`Connection` is therefore Rust-only and is not on the Python path.** It
exists to turn "step this until done" into a `Future` for tokio to poll.
asyncio needs no such conversion: it hands out a callback on readability, and
that callback calls `step` directly. Routing Python through `Connection`
reintroduces the thread this plan deletes, which is also why
`pyo3-async-runtimes` is not used.

`close` lives on the spine for the same reason: all three clients need "abandon
everything, refuse further work, resolve each abandoned slot with my own error
value", and three copies of that policy drift. `ClientError` is not `Clone`
(`ProtocolError` carries a `std::io::Error`), so `close` returns the slot ids
and the executor holds the cause.

Four spine surfaces the blocking verbs keep private today become `pub`, because
both async clients need them and each would otherwise re-derive them:

- `Session::scan_reply(tid, ReplyTrain) -> ScanReply` (today's `narrow_train`):
  the schema the train carried else the cached one, its rows, the terminal
  watermark. `ReplyTrain.schema` is only the in-frame block, so a warm scan
  reply is unusable without this fallback.
- `Reply::Multi` carries `Vec<ScanReply>`, narrowed in `feed` under each
  train's own tid — the spine holds `SlotKind::Multi { tids }` until the slot
  completes, so neither executor has to keep the list beside its future to
  narrow with. The blocking `scan_multi` returns it as is.
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

## Pushes pack warm, and a mismatch evicts

`PyAsyncTransport::push` encodes with `Some((schema, batch))` unconditionally —
bare `FLAG_PUSH`, no version stamp; `gnitz-py` has no reference to
`schema_version`, `schema_cache` or `wire_flags`. Measured off the wire, async
ships 1428 bytes/push against sync's 780 for 4 rows × 10 columns; the 648-byte
delta is exactly the schema block (88 fixed + 56 per column × 10), 45% of the
frame. `submit`'s push arm already packs warm against the connection's own
cache, so both async clients get it by construction — and the cache is warm
from the first push: a RESOLVE reply installs the block under the live tid, and
a cold push's ACK carries it (`send_ok_response` includes the block whenever
the request's stamp was `0` or stale).

That makes `STATUS_SCHEMA_MISMATCH` newly reachable on an async path: the
server's `decode_push_frame` raises it only for `has_data && !has_schema`, and
every async push sets `FLAG_HAS_SCHEMA` today, so no async caller has ever seen
it. A mismatched push **commits nothing** — `decode_push_frame` returns before
the commit path.

**What a cold resend can and cannot rescue** bounds what is worth building. The
stamp moves only on a COL_TAB delta for the relation (`invalidate_col_names`
has one caller, `apply_col_names_invalidate`), and the only DDL that writes one
for an existing table is `ALTER TABLE … ADD / DROP / RENAME COLUMN` — `CREATE
INDEX` does not bump it, and `SET NOT NULL` / `SET DATA TYPE` are refused. A
cold frame is checked by `validate_schema_match`, which compares column count,
PK indices, types and nullability and **never names**. Measured against a live
server: after `RENAME COLUMN` a stale warm push bounces and its cold retry
commits (one row of two columns: 364-byte warm frame, 564-byte cold frame,
one ACK); after `ADD COLUMN` the cold retry — and a fresh connection's cold
push — is refused with `Schema mismatch: expected 3 columns, got 2`. So a
resend rescues exactly the rename-class bump. After any other column DDL the
caller must re-resolve the relation and re-encode against the new schema,
whatever the client does — so every caller that can meet a mismatch already
needs that recovery path.

Given that, the spine does not resend. **A mismatch evicts, and fails its
slot.** In `feed`, when `check_response` yields `ClientError::SchemaMismatch`
for a `Correlated` slot, the slot's tid is popped from `schema_cache` before
the slot completes with the error — unconditionally: a scan reply may have
installed a newer block meanwhile, and evicting it costs one cold frame whose
ACK reinstalls it. The eviction has to be the spine's because nothing else can
refresh the cache: the mismatch reply is control-only (`send_control_only`)
and carries no block, so a client that only pushes would otherwise encode at
the stale stamp forever. From the eviction on, every submit for that relation
is cold until a reply carries the new block, which the first cold ACK does.

**The blocking client keeps its retry, unchanged in shape.** `roundtrip_push`
still re-submits once on `SchemaMismatch` — it holds `&Schema` and
`&ZSetBatch` across the round trip, and with one live operation nothing can be
ordered against it. Its own `schema_cache.pop` goes, since `feed` did it, and
the `cold` field on `Request::Push` goes with it: after the eviction the warm
gate finds no entry and encodes cold on its own — the same route a fresh
connection's first push takes. `push_retries_cold_on_schema_mismatch` keeps
passing unchanged, with `requests_sent() == 3`. A slot an aborted park
abandoned behaves as today: its mismatch evicts and is reported to nobody, and
the caller's own push is retried once.

**The async clients surface the error.** Every push submitted at the stale
stamp fails with `SchemaMismatch` — all of them, since the server's version is
not that stamp and `submit` encoded at submit time — and the caller re-issues
them in its own order: the same batches after a rename, re-encoded against a
re-resolved schema after a layout change (the asyncio surface has no
`resolve`; the sync client's is the one to use, as today). This is the same
contract every other per-operation error on a pipeline already has
(`test_error_among_concurrent_pushes`): a failed push committed nothing, a
push behind it may have. The one hazard is the caller's, and it is the same
under any client-side policy: a push encoded at the *new* stamp between two
stale ones (a scan reply installed it in between) commits ahead of the
re-issued stale one.

## Readiness on tokio

`Connection` waits on the fd through tokio's `AsyncFd`: it polls
`poll_read_ready` / `poll_write_ready` for the directions in `interest()`, and
when at least one is `Ready` runs `step` with the union as the ready set —
`step(ready)`, never `step(READ | WRITE)`: a direction that did not fire would
otherwise cost a syscall per wakeup to discover, which is the same speculative
read the blocking client avoids.

What makes this more than a poll loop is that `AsyncFd` caches readiness with
edge-triggered semantics: once epoll reports the fd readable,
`poll_read_ready` returns `Ready` until an `AsyncFdReadyGuard` clears the
flag. Which directions may be cleared, and when, differ:

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
arrives between the poll and the clear is kept, and a clear of a guard obtained
*before* the step never discards an event the step did not see. The poll is
therefore: obtain the guards for the directions in `interest()`; if neither is
`Ready`, `Pending`; else `step` with the union, then `clear_ready` on the read
guard whenever `ready.read`, and on the write guard only when `ready.write &&
interest().write` still holds after the step. The two guards borrow the
`AsyncFd` immutably and coexist.

`AsyncFd` requires the fd to be non-blocking, which the connection's own
`mark_established` guarantees, and it does not close the fd — the inner
`AsRawFd` type does. So it wraps the raw fd in a newtype with no `Drop`; the
transport owns the fd and closes it. `AsyncFd::new` must run inside a tokio
runtime context, so `connect` must be called from one.

## Where it lives: `gnitz-tokio`

**One new workspace member**, `gnitz-tokio`, holding `Connection`,
`AsyncClient` and `connect`. It depends on `gnitz-core` and `tokio` with
`net`, `rt` and `sync` — `AsyncFd`, the runtime context it needs, and the
bounded mpsc and oneshot the handle and driver share; `std::future::poll_fn`
is the only other async primitive used. `tokio`'s `rt-multi-thread` +
`macros` and `gnitz-test-harness` are dev-dependencies. It is a leaf: nothing
depends on it. Its server-backed tests sit behind an `integration` feature
exactly as `gnitz-core`'s and `gnitz-sql`'s do, and the Makefile's `test` and
`clippy` lines gain `--features gnitz-tokio/integration` beside the two they
pass today; the scripted-peer tests need no server and no feature.

`gnitz-core` gets no async dependency, ever, and no crate on the Python path
gets a `tokio` feature: `gnitz-py` depends on `gnitz-core`, and the workspace
is `resolver = "2"`, which splits feature unification for build-dependencies,
proc-macros and inactive target-dependencies but **not** for a normal
dependency shared by two workspace members in one build. A `gnitz-core/tokio`
feature enabled anywhere in `cargo clippy --workspace --all-targets` — which
includes `gnitz-py` — would unify tokio into the pyo3 extension's `gnitz-core`,
while `maturin develop` (run from `crates/gnitz-py`) resolves without it: two
builds of one crate with different feature sets. A separate leaf crate makes
that unrepresentable.

`CLAUDE.md` gains one crate-table row:

```
| `gnitz-tokio` | The Rust async client: a `Connection` future over tokio's reactor, and the `AsyncClient` handle | `core` |
```

and `crates/Cargo.toml` the member.

## The Rust async client

```rust
/// Owns the connection, its `AsyncFd` and the request channel; drains,
/// steps, resolves.
pub struct Connection { /* … */ }
impl Future for Connection { type Output = Result<(), ClientError>; }

/// A channel sender and a client id. Send + Sync + Clone; owns no connection.
#[derive(Clone)]
pub struct AsyncClient { /* … */ }

/// Blocking: the TCP connect and TLS handshake run on the calling thread,
/// for up to `CONNECT_TIMEOUT` per resolved address. Must be called inside a
/// tokio runtime context, which `AsyncFd::new` requires.
pub fn connect(target: &str) -> Result<(AsyncClient, Connection), ClientError>;

impl AsyncClient {
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

`Connection` encodes on its own task at `submit` — the push arm validates
the batch first, as `push_with_mode` does — and keeps `SlotId →
oneshot::Sender` until the slot completes. Each verb's `async fn` sends its
`Submission`, awaits the receiver,
and narrows the `Reply`: `terminal.seek_pk as u64` for a push (conflict mode
`Update`, as the asyncio push), `scan_reply` for a scan or seek, the
`Reply::Multi` payload as is for `scan_many`, `resolve_reply` then
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
`&self` — `tokio::sync::mpsc::Sender::send` does too, so a verb sends on the
handle's own sender with no clone; cloning is what a shared handle should
cost. It has no `&mut self` method to protect precisely because of what is
left off it.

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

The request channel is `tokio::sync::mpsc::channel(256)`. The spine's
`MAX_IN_FLIGHT` is the real in-flight bound and dominates memory; the channel
only hands work across, and a suspended `push` costs nothing — no thread, no
allocation — so the depth need only be enough that a burst rarely round-trips
through the scheduler mid-flight. `Connection` drains the channel into `submit`
only while `pending.len() < MAX_IN_FLIGHT`, so `push` suspends on a full
channel rather than seeing the cap's error.

`Connection::poll` is one loop: drain the channel while under the cap; if
`interest()` is empty, `Pending` on the channel alone; else poll the `AsyncFd`
guards and `step` as above, resolve every completion, and go round again — it
returns `Pending` only when both the channel and the readiness source are. A
`step` error calls `close`, resolves every returned slot with that error, and
completes with it.
When every `AsyncClient` is dropped the channel closes; `Connection` keeps
stepping until `interest()` is empty — the queue flushed and every pending
slot answered — and then completes with `Ok(())`. A `push` future that was
still alive holds a borrow of its handle, so no listener can be left behind.
Dropping a verb's future after its submission was sent is not cancellation:
the frame is written and the server commits it — which is what the drain
above guarantees even when the handle goes with the future; the oneshot's
receiver is gone and the driver drops the result.

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
/// One pyo3 crossing per readable event: `step(READ)`.
fn on_readable(&mut self, py) -> PyResult<Option<bool>>
/// The writer callback, and the deferred flush after submits: `step(WRITE)`.
fn on_writable(&mut self, py) -> PyResult<Option<bool>>
```

Each runs `let done = py.detach(|| session.step(ready))`, then, attached, for
every completed slot converts its `Reply` with the existing `triple_to_lazy` /
`classified_err` and calls `set_result` / `set_exception` on the slot's future
(skipping one already `done()`: a cancelled future refuses a result). The
return is `Some(interest().write)` after the step, read on **every** step, not
only writes: on TLS a read can queue an alert or key update that `wants_write`
then reports. A `step` error — the peer's EOF or reset, an out-of-order reply
— closes the transport exactly as `Connection` does: the spine's `close`,
every returned slot failed with that error, and the return is `None`, which
is how the loop learns to deregister — a Rust method cannot reach the loop's
`remove_reader`, and raising out of a loop callback only feeds asyncio's
exception handler. A callback the loop had already queued before the removal
finds the session closed and `step` returns nothing.

**The callbacks registered with the loop are `AsyncConnection` methods**, not
the Rust entry points: `add_reader` and `add_writer` discard a callback's
return value, so a Python wrapper is what reads it and arms from it. Each is
one Python frame around one pyo3 crossing per event, independent of N. The
reader is armed once, at construction, and removed at close: an armed reader
on a quiet socket costs nothing, while arming per operation would cost two
`epoll_ctl` per operation. The writer is armed only while the step reported
`write` — a permanently-armed writer callback spins the loop at 100% CPU on an
always-writable fd — and removed the moment a step reports it clear; `None`
removes both. A submit schedules `loop.call_soon` of the writer wrapper once
per idle→pending transition, not once per submit: a Python-side flag is raised
by the submit that schedules the handle and lowered by the first step that
reports `write` clear — so while the flush handle is pending, or the writer is
armed after a `WouldBlock`, a further submit schedules nothing and rides the
flush already coming. A `gather` of N therefore leaves in one `writev`.

A verb past `MAX_IN_FLIGHT` raises the spine's cap error synchronously, where
today's raises "transport queue full" at the same depth — nothing suspends,
because a Python verb returns its future at call time. And `push` runs
`ZSetBatch::validate` before `submit`, as `Session::push_with_mode` and the
Rust actor do; today's async push is the one path that skips it and lets the
server reject the frame instead.

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
`add_reader` holds a bound method of the connection, which holds the
transport, so the loop keeps both alive until `close()` removes the callback
or the loop itself closes, and only then does `Drop` run — closing the fd
through the `Session`, never while the loop still selects on it. That is the
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
moves onto the spine: `Session::connect`, `submit(Request::Push { .. })`,
then `step` / `poll` / `step` as
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

**New — the eviction, over the scripted peer of `spine_tests.rs`**, where a
mismatch is one line to script:
- Two warm pushes to one relation at a stale stamp, both sent, then the peer
  answers both with `STATUS_SCHEMA_MISMATCH`: each slot completes once with
  `SchemaMismatch`, the connection stays open, and a push submitted after the
  first mismatch goes out cold — its frame is longer than the warm ones by the
  schema block — and completes with its ACK; that ACK, carrying the block at
  the new version, puts the next push back on the warm path.
- The blocking `push_with_mode` over the same peer: one cold retry, the ACK's
  LSN returned, `requests_sent() == 3` — the existing
  `push_retries_cold_on_schema_mismatch`, unchanged.

**New — end to end**, in `test_async.py`: warm the cache, `ALTER TABLE …
RENAME COLUMN` through the sync client, then gather pushes encoded against the
old schema: every future fails with `GnitzError` matching `schema version
mismatch`, none of their rows is committed, and a push issued afterwards
succeeds and its rows are read back — the same connection, no reconnect. The
sync client on the same table, pushing the same stale batch, succeeds
transparently. And a warm async push ships 780 bytes, not 1428, read off
`bytes_sent`.

**New — `gnitz-tokio`**, over the actor:
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

- [ ] The mismatch eviction in `feed`, `roundtrip_push`'s own eviction and
      the `cold` field removed; the scripted-peer test.
- [ ] The spine surfaces: `scan_reply`, `Reply::Multi` narrowed in `feed`,
      `Request::scan` / `Request::seek`, the
      RESOLVE split with `RelTarget` public and `RelDescriptor::from_resolve`,
      `bytes_sent`.
- [ ] `gnitz-tokio`: the workspace member, its `integration` feature on the
      Makefile's `test` and `clippy` lines, and the `CLAUDE.md` row;
      `Connection` over the non-owning fd newtype with the clear-ready rules,
      `AsyncClient`, `connect`; the tests including the idle-CPU and
      flush-while-outstanding guards.
- [ ] The asyncio executor: the `AsyncTransport` pyclass over one `step`
      crossing per event, `aio.py`'s arming from the returned `Option<bool>`
      and the one `call_soon` per idle→pending transition, `PyAsyncTransport`
      and its block deleted, `_resolve_batch` and `call_soon_threadsafe` gone,
      `client_id` re-exposed, the three limitations and the lifetime written
      down; the Python tests above.
- [ ] The blocking reply path deleted: `send_batch` / `recv_push_ack` /
      `recv_scan` / the three `pack_*` / `drain_reply_train` / `recv_cached` /
      `recv_message`, `TransportWaker` with both `waker()` methods, both
      re-exports and both tests, the four comments rewritten, and
      `not_null_bit_rejection.rs` moved onto the spine.
