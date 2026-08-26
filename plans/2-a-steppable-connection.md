# 2 — A steppable connection, and two drivers for it

The client protocol becomes a sans-io connection state machine: `submit` queues
a request, `step` does the non-blocking I/O the driver says the fd is ready for
and reports what completed, and nothing in it ever waits. Waiting belongs to
whoever drives it. Two drivers land with it — the blocking client, which is the
whole existing product, and a test driver that runs many operations at once,
because the blocking client cannot exercise a single one of the state machine's
distinguishing parts.

`rustls::ClientConnection` and `StreamOwned` are the same split, in a crate this
one already depends on.

What lands here on its own: the second outbound path (`send_framed_batch`, its
trait and its thread-local scratch) is deleted; a long blocking call becomes
Ctrl-C-interruptible; and the `scan_multi` reply-desync that today's error path
avoids only through the shape of a `?` becomes a written-down rule with a test
on it.

Acceptance: the entire existing suite green, and **3 syscalls per operation on
both transports**. AF_UNIX is 3 today (`writev`, two `recvfrom`) and 3
afterwards (`writev`, `poll`, one read that takes header and payload together).
TLS is **2 today** — a blocking read is both the wait and the read, and the
whole small frame comes out of one record — and 3 afterwards, because a
non-blocking read splits the wait from the read and the TLS arm has no second
read to merge away in exchange. The extra `poll` is a per-*wakeup* cost, not a
per-operation one: it is the same one syscall whether the wakeup delivers one
reply frame or the ~252 a pipelined run drains, which is the trade this whole
plan exists to make. A frame larger than the receive buffer is the one place
that cost is per-byte instead: it pays one `poll` per refill where the blocking
read paid none.

## The fd goes non-blocking, and the blocking API emulates it

`O_NONBLOCK` is per open file description, so this is not per-direction: every
read and every write changes shape. Nothing sets it today, and the transport's
only blocking knob is `connect_tls`'s `SO_RCVTIMEO`, cleared by
`mark_established`.

The switch happens in `mark_established`, which already runs at exactly the
right moment: the TLS handshake and `hello_handshake` complete blocking before
it, so `SO_RCVTIMEO = CONNECT_TIMEOUT` still bounds connect exactly as today,
and `mark_established` keeps its `set_read_timeout(None)`.

**`ClientTransport::connect`, `send_framed`, `send_framed_iov`, `send_control`
and `recv_framed` stay public and keep their blocking contract.** Once
`Session`'s bodies convert, their callers are `hello_handshake` and the two test
files that drive the wire deliberately, `gnitz-core/tests/tls_client.rs` and
`gnitz-sql/tests/slow_client_eviction.rs`.
They are also the migration bridge: they are what lets the fd go non-blocking in
one commit while `Session`'s bodies and `gnitz-py`'s background I/O transport
are still blocking, so no commit here leaves the suite red.

**The park is a wrapper, never a primitive.** Each direction gets one
non-blocking core — write what the fd accepts and report what is left; read what
is there and report whether the source is drained — and each blocking method is
that core inside a park-and-retry loop. `flush()` and `step` below call the
core; `send_framed`, `send_framed_iov` and `recv_framed` call the loop. The
inverse layering — a blocking `writev_all` with `flush()` expressed over it —
puts a park inside the state machine, which is the one thing it must not
contain.

- `writev_all` today retries only `Interrupted` and returns `EAGAIN` as
  `ProtocolError::IoError`. It splits: the core writes what the fd accepts and
  reports the byte count, and `writev_all` becomes the `POLLOUT` wait around it.
- `flush_tls` loops `write_tls` and propagates `WouldBlock` verbatim;
  `write_all_vectored` treats `n == 0` as "rustls buffer full" and calls
  `flush_tls`, which then errors. They split the same way: a core that ships
  what the socket takes and reports whether `wants_write` still holds, and the
  wait-and-retry loop above it.
- `recv_framed` parks on `POLLIN` **on `EAGAIN`**, never ahead of the read —
  parking first would cost a syscall the read does not need — and parks
  untimed. The read side needs no `SO_RCVTIMEO` emulation to match the send
  side's: the only `SO_RCVTIMEO` ever set on a `ClientTransport` is
  `connect_tls`'s, and the same `mark_established` that makes the fd
  non-blocking clears it, so no read on a non-blocking fd has one to honour.
  (`tls_client.rs` does set read timeouts, but on raw `TcpStream`s it built
  itself.) `SO_SNDTIMEO` is different precisely because a test sets it on a
  transport *after* establishment.
- Every `poll(2)` in these wrappers retries on `EINTR`, as the reads already do.
  The blocking client's own park is the deliberate exception, for the reason the
  Ctrl-C section below gives.

**Emulating blocking includes emulating `SO_SNDTIMEO`.** The `POLLOUT` wait
reads the socket's own send timeout with `getsockopt` and passes it as the
`poll(2)` timeout — the option's `0` meaning "no timeout" becoming `-1` —
returning `WouldBlock` on expiry. `eviction_observed_within` in `tls_client.rs`
sets `SO_SNDTIMEO = 1 s` precisely so "a full send buffer surfaces as WouldBlock
(keep probing) instead of hanging", and its `WouldBlock => continue` arm is
reached whenever the server is alive but not draining. An untimed `POLLOUT` wait
parks there past the helper's own 8 s and 20 s deadlines and the test dies on its
120 s watchdog. The `getsockopt` costs one syscall per park, and a park only
happens under backpressure.

Without the wait-and-retry, four tests break at the first `EAGAIN`:
`pipelined_pushes_ahead_of_scan_do_not_deadlock` (30 frames of ~1 MB with no
reads under 128 KiB socket buffers), `inbound_cap_breach_closes_stalled_connection`
(140 × ~1 MB, breaking on the first `is_err()`, which would then never breach
the 64 MiB cap), and the two that reach `eviction_observed_within`.

On the read side the Unix arm must distinguish `EAGAIN` ("no data yet") from
`0` ("EOF"). It has no `EAGAIN` arm at all today and maps `n == 0` to
`UnexpectedEof`, so conflating them turns an idle connection into an error. The
`Interrupted` retry stays: Python signal handlers run on the main thread during
I/O.

## Framing

`FrameReader` becomes a field of `ClientTransport`, and the transport also takes
over the negotiated payload ceiling — it starts at `MAX_FRAME_PAYLOAD_CLIENT`
for the HELLO exchange and `mark_established` narrows it to the ACK's value,
which it takes as an argument: `hello_handshake` already computes
`min(ack.limit_bytes, MAX_FRAME_PAYLOAD_CLIENT)`, and that clamp moves to the
call rather than staying a value the caller threads onward.
The ceiling is what sizes the reader's allocation, so it belongs beside the
reader rather than threaded through every recv. `Session::max_payload_len` goes,
`recv_framed` loses its parameter, and `parse_frame_len` stays the one place the
bound is enforced. The four `recv_framed` call sites in `tls_client.rs` pass
`MAX_FRAME_PAYLOAD_CLIENT` today and tighten to the negotiated ceiling: the
server advertises `MAX_FRAME_PAYLOAD_SERVER` in its HELLO ACK and refuses to
emit a frame above it, so the bound they end up with is exactly the largest
frame that can arrive.

Two properties pull against each other:

- A driver must get every buffered frame from one readable event, or it burns a
  syscall per frame and re-arms per frame.
- A payload must not be copied out of a scratch buffer. `recv_framed` today
  allocates `Vec::with_capacity(payload_len)` and receives into its spare
  capacity, so the returned `Vec` *is* the frame and `RawBlock { frame, block }`
  hands out ranges into it with no copy — the point of the raw path, and
  `gnitz-mirror`'s steady-state ingest. A reply frame is sized by the worker's
  `reply_frame_budget`, which defaults to the whole 64 MB
  `MAX_FRAME_PAYLOAD_SERVER`: a worker ships its entire scan result as one frame
  whenever it fits, and only chunks past that. `big_push_and_multiframe_scan`'s
  700,000 rows × 40 wire bytes = ~28 MB come back as one such frame per worker —
  ~7 MB each at `GNITZ_WORKERS=4`, the whole 28 MB at one worker.

A `BufReader` over the transport meets the first property and, through its
large-read bypass, most of the second — but it reports neither what its last
read asked for nor what it got, which is the drained proof below, and it cannot
be interposed at rustls's `read_tls`, which is where that proof has to be taken.

So the **header** is read into a 64 KiB scratch that may over-read, surplus kept
in `carry`. Once `payload_len` is known the payload is allocated at exactly that
size, whatever sits in `carry` moves across, and the remainder is read
**directly into the payload buffer**. The copy is bounded by one read's surplus,
never by the frame.

64 KiB is sized by the smallest complete reply frame: a control-only frame is
4 + `CTRL_BLOCK_SIZE_NO_BLOB` = 260 bytes, and a push ACK is exactly one, so a
pipelined run of ACKs drains ~252 per read. It is also the ceiling on the one
memcpy `carry` can force. One scratch per connection.

### Drained, and what `step` leaves behind

A read that returns **less than it asked for** proves the source is drained.
`recv` on a `SOCK_STREAM` copies until the buffer is full or the receive queue
is empty, so a short return means the queue is empty. That inference is what
keeps an operation at one read syscall rather than two. The one thing that
would break it — AF_UNIX stopping a stream read at an skb carrying ancillary
data — needs `SCM_RIGHTS`, which nothing on this connection sends, and no path
here uses `MSG_PEEK`.

**It holds through rustls too, at the ciphertext layer.** `read_tls` performs
exactly one `rd.read(&mut buf[used..])` on the transport it is handed
(`ConnectionCommon::read_tls` → `DeframerVecBuffer::read`), so wrapping the
`TcpStream` in a non-owning `Read` newtype that records `(asked, got)` gives the
TLS arm the same proof for the same zero syscalls. What proves nothing is the
*plaintext* read — `reader().read()` is short whenever a record boundary falls
there, for reasons unrelated to the fd. Two `read_tls` returns are not
observations at all and must not be taken as proof: `Ok(0)` after a
`close_notify`, and the received-plaintext-buffer-full error, both of which
return before touching the transport.

**EOF is deferred behind whatever is already buffered.** A read returning `0`
records the end of stream; it is raised only once the reader can no longer
produce a frame from `carry`. Raising it eagerly loses the last reply whenever
the server answers and closes and the answering read happened to fill the
scratch exactly — the shape `wire_version_mismatch_hello_gets_status_error`
sits on, where the whole point is to read the error frame the server sent just
before it hung up. A `0` arriving mid-payload is different and stays immediate:
that frame is truncated and `UnexpectedEof` is the answer. rustls draws the same
line one layer down — `reader()` reports `UnexpectedEof` only after
`received_plaintext` is drained.

**`step` returns only once nothing buffered can advance a slot.** That is its
postcondition, not a rule drivers must obey, so a driver may park
unconditionally after any `step`. The condition is not "`carry` is empty": on
the TLS arm rustls holds decrypted plaintext of its own, so one record carrying
two frames leaves `carry` empty with the second frame already in memory.
Draining that plaintext costs no syscall, so a `READ` step interleaves — pump
ciphertext, drain plaintext into the reader, consume every frame that completes
— and returns only when the source is proven drained and no complete frame
remains in `carry` or in rustls's plaintext buffer. A step in any other
direction still consumes what is already buffered, so the postcondition holds
for all of them. Publishing the predicate instead and asking every driver, in
every runtime, to consult it before parking would leave the failure silent — the
stream stays byte-aligned and the connection simply stops — where one loop
condition makes it unrepresentable.

`parse_frame_len`'s two rejections move in unchanged. The raw read needs
`self.inner` and `self.reader` borrowed disjointly —
`self.read_into(self.reader.spare())` is E0499, and a local field destructure is
enough. Both transport arms meet at one `read_into`, which is what keeps the
reader itself transport-blind.

The TLS arm reproduces what `StreamOwned`'s read path does, in three parts:
flush `wants_write` before reading (`process_new_packets` queues outbound TLS on
the read path — a TLS 1.3 `KeyUpdate` obliges a reply, a decrypt failure queues
a fatal alert); `reader().read()` returning `WouldBlock` means "pump another
record"; and `reader()`'s two end-of-stream signals, `Ok(0)` for a clean
`close_notify` and `Err(UnexpectedEof)` for an unclean TCP EOF. `read_tls`
returning `Ok(0)` is neither of those but must still stop the pump: it is both
what *sets* the unclean-EOF state `reader()` then reports, and what a connection
already past `close_notify` returns without touching the transport, so treating
it as a short read to retry spins.

That first part is why `step(READ)` can write: rustls's ciphertext flush is a
protocol obligation on the read path, not the outbound queue's `WRITE`
direction, and the two must not be conflated. On a non-blocking fd it may only
partially drain, and that is not an error — the remainder stays in
`sendable_tls`, `interest()` reports `WRITE` for it, and the read proceeds.

Once that lands nothing calls `StreamOwned`'s `Read` or `Write` — the send path
already reaches `s.conn.writer()` and `s.sock` directly — so `Inner::Tls` holds
the transport's own `{ conn, sock }` pair instead. Leaving `StreamOwned` there
would leave a `.read()` in scope that bypasses `FrameReader` and desyncs the
stream, which is the kind of mistake that shows up as a hang, not a
compile error.

### The loopback TLS server

The TLS framing tests need **no test-only hook in production code**: mint a
self-signed cert with `rcgen`, run a `rustls::ServerConnection` over a loopback
`TcpStream` on a helper thread, and drive a real
`ClientTransport::connect("tls://127.0.0.1:{port}?insecure")`. `?insecure`
against a loopback host is already permitted without `GNITZ_TLS_INSECURE`, so
the test exercises `connect_tls`, the handshake and the framing through the
fully public path. The helper also answers the HELLO with
`gnitz_wire::encode_hello_ack`, because `hello_handshake` is what calls
`mark_established` — without the ACK the fd never goes non-blocking and the test
would prove the framing only on a blocking one.

It is not redundant with `tls_client.rs`. The properties that matter here are
*record* boundaries against *frame* boundaries — one record carrying two frames,
one frame split across two records — and nothing about the real server is
steerable to produce either. Separately, `ServerHandle::start*` returns `None`
when no server binary is present, so every test in `tls_client.rs` silently
skips under a bare `cargo test`; these do not.

rustls 0.23 has no client/server feature split, so `ServerConnection` is
available under the `ring` + `std` features already selected. `rcgen` is not a
`gnitz-core` dependency today (it belongs to `gnitz-server` and
`gnitz-test-harness`, whose minting helper is private and mints a client CA), so
add it to `gnitz-core`'s dev-dependencies with the same
`default-features = false, features = ["crypto", "pem", "ring"]` those two use —
the lock file already carries `rcgen 0.14.8`.

There is deliberately **no `#[cfg(test)]` `Inner` variant over a pipe**. It
would force new arms in `as_raw_fd` and `mark_established`, and — because a pipe
has no honest single fd — would turn `as_raw_fd` into `Option<RawFd>` for every
production caller and every driver, to serve a test a loopback socket serves
better and through the same code path production uses.

## Outbound

The transport gains an **owned-`MessageParts` queue and a partial-write
cursor**, beside the reader and for the same reason: both are per-connection
byte-stream state, and putting them together lets `Session::send_batch` reach
the queue one commit before the spine exists.

It is a queue of owned parts, not a flat `Vec<u8>`: `send_framed_iov` builds an
`iovec` array over borrowed segments and `writev_all`s them, so there is no
payload copy today, and a flat buffer would memcpy every outbound byte — 28 MB
of it on `big_push_and_multiframe_scan`'s single push frame (700,000 rows × 40
wire bytes; the test's own "~16 MB" comment counts only the payload columns, not
the PK, weight and null regions).

Each queue entry owns its `MessageParts` and its own 4-byte length prefix, and
the transport holds one reusable `Vec<libc::iovec>` beside the queue. Those are
the two halves of the `SCRATCH` thread-local, given owners: the prefix bytes no
longer have to outlive a `writev` through a raw pointer, and the iovec array is
per-connection rather than per-thread, so its high-water mark is the depth that
connection's own caller chose rather than a retention heuristic. The array is
rebuilt from the queue cursor on every flush, so the queue's cursor is the only
cursor and no in-place iovec mutation has to survive between calls.
`flush() -> Result<bool, ProtocolError>` writes what the fd accepts from the
cursor and reports whether anything remains. It does not park: it drives the
non-blocking core directly.

**The TLS arm needs the same cursor.** rustls's `sendable_plaintext` is bounded
at `DEFAULT_BUFFER_LIMIT` = 64 KiB, so `writer().write_vectored` short-writes
and then returns `0` once that buffer is full — the plaintext cannot simply be
handed over wholesale. The cursor advances across
`write_vectored → ship whatever ciphertext the socket takes → write_vectored`
exactly as it advances across `EAGAIN` on the Unix arm — the non-blocking core,
not `flush_tls`, which is now the park-and-retry loop above it.

Raising the limit instead is the tempting shortcut and it is wrong:
`set_buffer_limit(None)` unbounds `sendable_plaintext` and `sendable_tls`
together, so rustls would accept the whole frame in one `write_vectored` and
hold a plaintext copy of it — 28 MB of copy on the push frame above, which is
the memcpy the owned queue exists to avoid, plus the same again as ciphertext.

**`send_framed_batch` and `FrameSegments` are deleted.** The cursor-driven
`writev` is the same construction — one 4-byte prefix iovec plus one per
non-empty segment, chunked by `IOV_MAX` — with resumability added, so keeping
both leaves a second, non-resumable copy of the multi-frame outbound path. The
`SCRATCH` thread-local, `shrink_to_cap`, `SCRATCH_LENS_CAP` and
`SCRATCH_IOVS_CAP` go with them, which also disposes of two comments that size
themselves by `IO_BATCH_MAX` in another crate. `MessageParts::segments()` stays —
`send_framed_iov` and the raw-wire tests use it.

`send_framed_iov` is **not** a third copy and stays. It and `flush()` sit on
the same non-blocking core — the Unix arm's single `writev`, the TLS arm's
`write_vectored` + ship-what-the-socket-takes — differing only in who owns the
cursor and whether a park wraps it; `send_framed_iov` is five lines of stack
iovec over the one-frame borrowed case, and the stack array is why it keeps
costing zero allocations. What made `send_framed_batch` redundant was that it
duplicated the *multi-frame* prefix-and-iovec construction the cursor now owns.

In-flight work is capped at **4096 requests**, and `submit` raises past it. 4096
is today's number in the same role: the async transport's request channel is
`sync_channel(4096)` and `enqueue` raises `"transport queue full"` when it is
full. The cap must sit above the largest concurrent submission anything makes —
`test_connection_loss_resolves_every_queued_request` submits 3000 — and holding
today's number keeps that margin without inventing one.

No byte axis and no `GNITZ_OUTBOUND_BYTES`. Nothing in the tree submits deep:
the blocking client holds one operation, and the async transport's `IO_BATCH_MAX`
holds 1024 encoded frames in a `Vec` and ships them in one `send_batch` today, so
the queue's byte high-water mark is the one the current code already reaches. Past
that the depth is whatever a caller's own `gather` makes it, in bytes a caller's
own batches size — which no cap can bound anyway, since a byte cap would need a
single-frame exemption for the 28 MB push, and that exemption is the whole
worst case back again.

## The spine

```rust
impl Session {
    /// Encode against this connection's schema cache, enqueue, register a slot.
    /// Raises past the in-flight cap.
    fn submit(&mut self, req: Request) -> Result<SlotId, ClientError>;

    /// Do the I/O `ready` says the fd will accept — flush the cursor on
    /// `WRITE`, read until the source is drained on `READ`, neither on an empty
    /// set — advancing the reply train at the head of the pending queue as
    /// bytes arrive. Returns every slot that completed, and returns only once
    /// nothing buffered can advance another one, so a driver may park on
    /// `interest()` immediately afterwards.
    fn step(&mut self, ready: Interest) -> Result<Completions, ClientError>;

    /// `READ` while any slot is outstanding; `WRITE` while bytes remain queued
    /// or rustls has ciphertext to ship.
    fn interest(&self) -> Interest;

    /// Abandon every pending slot, return them, and refuse further work: a
    /// later `submit` returns `ClientError::ServerError("connection closed")`.
    fn close(&mut self) -> Vec<SlotId>;
}
```

**`step` takes the readiness it was given.** An argument-free `step` has to
attempt both directions every call, so a driver that has just submitted pays a
read that can only return `EAGAIN` — one syscall per operation on the blocking
client, which is the difference between the 3 above and 4. The driver already
knows: it just polled, and the concurrent driver below hands back exactly the
directions `poll(2)` reported rather than probing the other one. An empty set is
defined (advance from what is buffered, touch no fd) but nothing has cause to
pass one: `step` never returns with a frame left to advance.

**Abandoning and closing are one operation.** Pending trains are on the wire and
the head-of-queue accumulator is the only thing that can consume them in order;
a connection that abandoned its slots and kept accepting submits would decode
the next reply against the wrong slot. So there is no `fail_all` beside `close`
— they had the same postcondition. `ClientError` is not `Clone`
(`ProtocolError` carries a `std::io::Error`), so `close` returns the slot ids
and the driver, which holds the cause, builds one value per slot.

**Interest is two booleans on the connection, not a per-slot union.** Read while
anything is outstanding — replies arrive in request order, so there is never a
pending slot whose bytes we do not want. Write while the queue is non-empty, or
while rustls has ciphertext queued from the read path. A last-writer-wins
interest cell would let a push awaiting writability overwrite a scan's read
interest; the driver would then arm only the write side, never drain replies,
and the server's per-frame send deadline would evict the client — the shape
`stalled_scan_client_is_evicted_by_send_deadline` pins. Deriving both flags from
the queue and the pending count makes that unrepresentable rather than guarded.

**A dead peer needs no special readiness handling, and a driver must not
invent any.** On Linux a connected stream socket whose peer closed, half-closed
or reset reports `POLLHUP` / `POLLERR` *alongside* the requested bit — measured
on AF_UNIX and TCP, for FIN, RST and a local `shutdown(SHUT_RDWR)`, registering
`POLLIN` alone, `POLLOUT` alone and both. So `interest()` (which arms `READ`
while any slot is outstanding) is already enough: the step that follows reads,
gets EOF or `ECONNRESET`, and surfaces it — which is what
`restart_same_port_fails_fast_then_reconnects` requires to be immediate. What a
driver must not do is derive an *empty* ready set from a poll that returned;
`POLLHUP` / `POLLERR` arrive whether or not they were requested, so a mapping
that recognises only `POLLIN` / `POLLOUT` must fold them into `READ` rather than
step on nothing and re-park on a descriptor that is ready again at once.

**The spine grows on `Session`; there is no new type.** `Session` already owns
the transport, the client id, the schema LRU, and the whole warm/cold packing,
continuation-reassembly, cache-absorption and status→error policy. The queue,
the slots and the reply accumulator join that; a `Conn` beside it would give the
schema cache two owners and the reassembly policy two copies. `Session` also
gains an `as_raw_fd` delegating to the transport's: a driver has to have
something to poll, and that is the one thing the spine will not do for it.

So `Session` ends up carrying two layers, and "nothing in it ever waits" is a
property of the **five spine methods**, not of the type. Above them sit
`Session`'s own blocking verb bodies, which park; beneath them, until the
background I/O transport goes, sits the blocking reply path that transport still
calls. The verbs cannot move up to `GnitzClient` to make the split a type
boundary: `roundtrip_push` evicts a cache entry on a mismatch and every pack
stamps a cached version, so moving them would hand the schema LRU the second
owner the paragraph above refuses it.

`frames_sent` is bumped **as a request is enqueued**, not once per `writev`.
`gnitz-sql/tests/relation_resolve.rs` and `gnitz-mirror/tests/mirror.rs` assert
exact per-statement counts through `requests_sent` ("a delegated describe costs
one RESOLVE", "must issue no request"); counting writevs would let batching move
a number those tests pin. The transport ends up with exactly two ways to put a
frame on the wire — the queue's enqueue and `send_framed_iov` — and one bump in
each, so the counter is still one-per-frame with no request path able to forget
it; `frames_sent`'s doc comment names both instead of one.

**`step` returns completions and holds no completion policy.** It knows nothing
of wakers, channels or Python objects: each driver keeps its own
`SlotId -> its own primitive` map and drains what `step` hands back.

### What goes in, and what comes out

`Request` covers every verb, and its variants are cut where the encoding or the
decoding differs, not where the verb names do: a correlated control frame (SCAN,
SEEK, SEEK_BY_INDEX), an uncorrelated one (the four id allocations, RESOLVE, the
two pre-encoded transaction frames), PUSH — correlated like the first, but the
only variant that chooses warm against cold — SCAN_SPEC carrying the caller's
reply schema and a raw flag, and SCAN_MULTI carrying the tid list. `submit`
therefore reads the slot's three facts straight off the variant it was handed.
It takes the verb's inputs rather than bytes because the frame cannot exist
before the connection stamps it: the cached schema version rides the flag word,
and the warm/cold choice reads the same cache, which only the session owns.

Not nine typed `submit_*` methods, one per verb: five shapes are fewer than
nine, and cutting by shape is what makes the decode facts a property of the
variant rather than three arguments each entry point sets for itself — a verb
added later has to land in an existing variant or state why it is a sixth,
where a tenth method would simply carry its own defaults.

```rust
/// One reassembled train. The terminal frame is kept whole because it is what
/// several verbs' answers live in — `target_id` for an id allocation,
/// `seek_pk` for a push ACK's LSN and a scan's watermark, `seek_pk_extra` for
/// a RESOLVE's descriptor blob — and its own data block is folded into `data`,
/// not left on it.
///
/// `schema` is the block the train physically carried and never a cache
/// lookup: `schema_or_cached`'s fallback stays at the caller, because a slot
/// that is off the cache — `scan_spec`, `scan_spec_raw` — must not be handed a
/// cached schema for the tid it happens to name.
pub struct ReplyTrain {
    pub terminal: Message,
    pub schema: Option<Arc<Schema>>,
    pub data: Option<ZSetBatch>,
}

pub enum Reply {
    Train(ReplyTrain),
    /// The same train with each frame's data block left undecoded in its own
    /// frame buffer: `scan_spec_raw`, and the mirror's copy-free ingest. It
    /// carries no schema — the server sends no block back for a SCAN_SPEC.
    Raw { blocks: Vec<RawBlock>, terminal: Message },
    /// `scan_multi`: N trains in request order.
    Multi(Vec<ReplyTrain>),
}

pub type Completions = Vec<(SlotId, Result<Reply, ClientError>)>;
```

Three flat variants rather than a train with a decoded-or-raw body, because
those are exactly the three reachable combinations: only `scan_spec_raw` is raw
and only `scan_multi` is many, so a raw multi is not a case anyone has to think
about.

Keeping the terminal `Message` is what folds the bare-`Message` verbs into the
train: today `roundtrip`, `seek_roundtrip` and `resolve` hand back a `Message`
and `scan` hands back a train, and the only reason they were different shapes is
that nothing carried both.

`SlotId` is a monotonic `u64`, never an index into a recycled table: a driver
that holds one across an abandonment must not be able to match a later slot.
The `Vec` costs nothing on the steps that complete nothing — an empty `Vec`
does not allocate — so the blocking client pays one allocation per operation,
not per step.

**A per-slot `Err` and `step`'s own `Err` are different failures.** Every
`STATUS_*` the server can name — `STATUS_ERROR`, `STATUS_SCHEMA_MISMATCH`,
`STATUS_TXN_CONFLICT`, `STATUS_NO_INDEX`, `STATUS_DELTA_EXPIRED` — completes its
slot with an `Err` and leaves the connection usable. One of them carries a
value and not just a classification: `STATUS_TXN_CONFLICT` is a control-only
frame whose `seek_pk` is the server's fresh OCC basis, which
`check_response` lifts into `ClientError::TxnConflict { fresh_basis }` and
`GnitzClient::advance_basis` adopts. An accumulator that dropped the frame on a
non-OK status before classifying it would lose that, and the autocommit RMW
retry would re-read at the same stale basis and conflict again.

Only a transport or
protocol failure — `ClientError::Protocol`, which is every `ProtocolError` plus
the out-of-order `target_id` check — comes out of `step` itself, and it poisons
the connection: the byte stream's framing is no longer trustworthy, so the
driver's only move is `close`. That is exactly the line `gnitz-py`'s
`classify_recv_err` draws today (`ClientError::Protocol` stops the loop,
everything else fails one future), and it moves onto the spine so every driver
gets it from one place.

**The blocking client therefore calls `close` on a `step` error and reports the
connection closed thereafter**, where today it returns the error and lets the
next call proceed on a stream whose framing it can no longer trust. That is the
one behaviour change in this plan that a caller can observe, and it is confined
to `ClientError::Protocol`: every rejection any test asserts on — a planner
rejection, a missing relation, a schema mismatch, an OCC conflict, the hostile
push in `not_null_bit_rejection.rs` — is a server status and completes its slot
without touching the connection.

### Reply trains

`drain_reply_train` and `drain_reply_train_raw` are blocking loops accumulating
a schema, a concatenated `ZSetBatch` (or `Vec<RawBlock>`) and a `u128`
watermark across N `FLAG_CONTINUATION` frames; `scan_multi` reads **N such
trains for one request**. Under `step` all of that becomes resumable state, and
it is larger than the rest of the spine combined.

**There is exactly one accumulator, and it belongs to the head of the pending
queue.** The server's `connection_loop` handles one message to completion before
receiving the next, and every client-peer write happens inside that awaited
chain, so replies leave a connection in request order unconditionally — the
invariant is stated and enforced there, and spawning `handle_message` to overlap
requests is what would break it. Every byte that arrives therefore belongs to
the head slot until that slot's last train terminates. A per-slot accumulator
would be N times the state and would leave `step` with no way to choose one: a
frame's `target_id` does not name a slot, and two pending slots routinely share
a tid.
The accumulator holds the train-in-progress partial, its variant, the terminal
frame and `trains_remaining` for `scan_multi`; when the head completes it is
reset for the next slot.

**For `scan_multi` the relation advances with the train.** All three things the
slot's tid drives — the decode hint handed to `recv_message`, the out-of-order
`target_id` check, and the key cache absorption files the block under — are
`tids[i]` for train `i`, not the slot's first tid. That is what today's
`for &tid in tids { recv_scan(tid) }` does one relation at a time; an
accumulator that held a single tid across all N trains would hint the wrong
schema at relation 2 and decode a reply under another relation's column types.
So the request's tid list rides the accumulator beside `trains_remaining`, and
the index into it is the state.

**Status is classified on every frame, before the continuation test.** A
`STATUS_ERROR` fault frame carries flags `0`, structurally identical to the
master's terminal frame, so an accumulator that tested `FLAG_CONTINUATION`
first and only classified what it took to be the terminal would end the train
and report it empty and successful — the failure `drain_reply_train`'s
per-frame `check_response` exists to stop. `check_response` moves onto the
accumulator whole, and it runs first.

**A non-OK frame ends the whole request, not one train.** A `scan_multi`
rejection or a mid-stream worker fault sends **one** error frame — possibly
after k of the N trains already streamed — and `handle_scan_multi` then sends
nothing more ("the client discards any partial results it read"). So the
accumulator zeroes `trains_remaining` on it. Today's `Session::scan_multi` gets
this right by accident, because `recv_scan(tid)?` returns on the first error and
there is nothing left to read; an accumulator that kept counting would wait
forever for trains the server will never send.

A single-frame reply — a seek, an ACK, an id allocation — is a train of length
one, and folding them in rather than keeping a second single-frame path also
closes a hole: today `seek_roundtrip` returns its frame without looking at
`FLAG_CONTINUATION`, so a continuation the server did send would desync the
connection.

**The slot says how its reply decodes, because the bytes do not.** Three facts
travel with it, each per-call today and none inferable:

- *Correlated or not.* `recv_cached`'s out-of-order `target_id` check — which
  becomes the accumulator's, still raising `ClientError::Protocol` — is right
  for scan / seek / push ACK and wrong for the replies whose *answer is* a
  target id: `alloc_table_id` / `alloc_schema_id` / `alloc_index_id` /
  `alloc_serial_range`, `resolve`, `push_ddl_txn` and `push_txn` all receive
  uncorrelated today (`recv_message(.., None, ..)`) for exactly that reason.
  **The same fact gates cache absorption**, which `recv_cached` keys by the
  *requested* tid: an uncorrelated slot absorbs nothing. `resolve` is why —
  its requested tid is `0` for a by-name lookup, so absorbing there would file
  the relation's schema under `0` and leave the following `scan`/`push` cold,
  which is the opposite of what the resolve was for. It installs the block
  under the reply's own `target_id` itself, off the terminal `Message`.
- *Decoded or raw.* `scan_spec_raw` keeps undecoded blocks — that is what makes
  the mirror's ingest copy-free; everything else builds a `ZSetBatch`.
- *Which schema decodes it.* `scan_spec` and `scan_spec_raw` carry the caller's
  reply schema and stay **off** the schema cache in both directions, because a
  per-query projected schema keyed under the table id would corrupt a later
  plain scan.

`submit` builds its frame with the encoders that exist. `pack_scan`, `pack_seek`
and `pack_scan_multi` already stamp the cached schema version and already reject
a malformed `scan_multi` tid list before any frame is written — the empty list
being the one that matters, since a `count=0` frame draws one server error frame
that an N=0 read loop never consumes, shifting every later reply by one. They
keep their current visibility here — `gnitz-py`'s background I/O transport is
still a caller.

### Dropped work

The connection owns the pending queue independently of any caller. Abandoning a
slot does not skip its reply: the train is read to its terminal frame and
discarded, because the head-of-queue accumulator cannot step over it.
Abandonment is **not cancellation** — the frame is on the wire, or its unwritten
remainder is still in the queue ahead of everything behind it, and either way
the server commits it. `close` is the bulk case; the single case is Ctrl-C
below, and the cost of it lands on the next call, which drives the abandoned
train to its terminal before its own reply can start.

## The blocking client

`GnitzClient`'s public signatures do not change and it keeps delegating to
`Session`; the drive loop is **one private `Session` method**, not nine copies
of a loop:

```rust
/// Submit, then drive to completion. The one place the blocking client waits.
fn round_trip(&mut self, req: Request) -> Result<Reply, ClientError> {
    let slot = self.submit(req)?;
    let mut ready = Interest::WRITE;         // queued work, nothing readable yet
    loop {
        match self.step(ready) {
            Ok(mut done) => {
                if let Some(i) = done.iter().position(|(s, _)| *s == slot) {
                    return done.swap_remove(i).1;
                }
            }
            Err(e) => { self.close(); return Err(e); }
        }
        // hook, then poll(2); revents → Interest
        ready = self.park(self.interest())?;
    }
}
```

`Session`'s nine send-then-recv bodies — `send_txn_frame`, `scan`, `scan_multi`,
`seek_roundtrip`, `resolve`, `scan_spec`, `scan_spec_raw`, `roundtrip`,
`roundtrip_push` — each become "build the `Request`, call `round_trip`, narrow
the `Reply`", and that narrowing is all they keep of their own. Nine copies of
the loop is what a per-body conversion would produce, and each copy is another
place to get the `close`-on-`Protocol` rule or the park wrong.
`hello_handshake` is a tenth round trip that stays blocking, before the
non-blocking switch.

A driver holding one live slot never needs to index `Completions`: the scan is
over at most one element. A driver that pipelines keeps its own
`SlotId -> primitive` map instead.

Three syscalls: the `writev` from `step(WRITE)`, the `poll`, and the one read
from `step(READ)` that takes header and payload together. The park is
unconditional because `step` returns nothing buffered.

`roundtrip_push`'s schema retry is unchanged in substance: on
`ClientError::SchemaMismatch`, evict the cache entry, submit the cold frame,
drive again. It stays correct for the reason it is correct today — the blocking
client has exactly one *live* operation, so nothing can be ordered against the
retry. A slot abandoned by the Ctrl-C path below can sit ahead of it and does
not change that: the outbound queue is FIFO, so whatever of that slot's frame is
still unwritten goes out ahead of the retry, and its train is drained before the
retry's reply can start.

`drain_reply_train_raw` is deleted with `scan_spec_raw`'s conversion, since
`scan_spec_raw` is its only caller.

**Six `Session` members stay public through this plan**, because `gnitz-py`'s
background I/O transport still calls all six and must keep building and passing
its suite: `send_batch`, `recv_push_ack`, `recv_scan`, `pack_scan`, `pack_seek`
and `pack_scan_multi`. The blocking reply path beneath the first three —
`drain_reply_train`, `recv_cached`, `recv_checked`, `recv_message` — stays as
their implementation and nothing else's; it keeps working because
`ClientTransport::recv_framed` keeps its blocking contract above. That leaves
the correlate-then-absorb policy written twice, on the accumulator and in
`recv_cached`, for as long as that transport exists — which is what pays for
every commit here landing green, and it ends when the transport does.
`ClientTransport::waker`, `Session::waker` and `TransportWaker` stay for the
same reason: that transport's teardown is their only consumer. The park they
unblock moves from the kernel `recv` to `poll(2)`, and `shutdown(SHUT_RDWR)`
wakes a parked `poll` on the same open file description just as it wakes a
parked `recv` — measured: it reports `POLLIN | POLLHUP` on AF_UNIX and TCP
alike. `test_transport_waker_unblocks_parked_recv` survives the switch
unchanged, though it does not cover that: its transport comes from
`from_unix_fd` and never runs the handshake, so its fd stays blocking and its
park stays a kernel `recv`. The `poll` wake is what the background I/O
transport's teardown rests on, and `test_async.py`'s two shutdown tests are
where it is exercised.

`Session::send_batch` is the one that changes shape, because
`send_framed_batch` is what it called: it becomes "enqueue every part, then
flush until the queue is empty". It registers no slot, so a connection driven
that way leaves the pending queue and the accumulator empty and uses the
outbound queue as a flush-to-completion buffer — which is also why the two
outbound paths cannot interleave on one connection during the middle of the
sequence, when everything else still writes through `send_framed_iov`: the queue
is empty whenever `send_batch` returns. That is what lets the second outbound
path be deleted here rather than a plan later.

**Ctrl-C.** Today an `EINTR` inside a `py.detach`ed blocking `recv` is retried
in Rust and CPython never runs `PyErr_CheckSignals`, so a long scan is
uninterruptible. The park point makes a fix possible, and the hook hangs off
**the park, not off `step`**: `step` is the state machine, and a callback on a
function whose whole claim is that it does not wait would contradict the design.
`round_trip` is the one thing that waits, so `Session` holds
`Option<Box<dyn FnMut() -> Result<(), ClientError> + Send + Sync>>`, calls it
before each park, and lets its `Err` abort the operation. `GnitzClient` exposes
the installer and holds no hook of its own — a second copy on the client would
be a field the parking code cannot see.

**The park is therefore the one `poll(2)` that does not retry `EINTR`.**
CPython installs its handlers without `SA_RESTART`, so the signal surfaces as
`EINTR` — and a park that restarted the poll in place would swallow it, leaving
the hook unrun until bytes arrived and the long scan exactly as uninterruptible
as it is today. `EINTR` returns to the top of the park's own hook-then-poll
loop instead. The transport's blocking wrappers keep retrying `EINTR`, because
no hook hangs off them.

The bounds are not decoration, and they land on the type that most needs them:
`client.rs` asserts `Send` for `Session` because the background I/O transport
moves a bare one into its thread, and for `GnitzClient` because `py.detach`'s
`Ungil` resolves to it. `Sync` costs the installer nothing — a closure that
reacquires the GIL captures nothing. `GnitzClient`, `Session` and
`ClientTransport` are all `Send + Sync` at HEAD, so a `Send`-only bound on the
field would silently narrow all three and could disturb a downstream bound
(pyo3's `#[pyclass]`) a crate away. `gnitz-py` installs a hook that runs
`PyErr_CheckSignals` under a reacquired GIL — `gnitz-core` has no pyo3
dependency and cannot name it — and the Rust client installs none.

An aborted operation leaves its slot pending, so the next call drains and
discards that train first. Whether the frame reached the server does not have to
be known: a park can happen mid-flush, and the queue retains the unwritten
remainder for the next drive, so the server sees exactly one well-formed frame
either way — and commits it, because abandonment is not cancellation.

## The concurrent test driver

The blocking client exercises none of the spine's distinguishing parts. With one
operation outstanding the outbound queue never holds two frames, `flush()` never
returns `true`, `interest()` never has both bits set, the head accumulator never
hands off to a second slot, and the cap is never approached. A green suite would
be false confidence, and worse, an implementation written to what the blocking
client needs collapses honestly to no queue, no slot map, no cap and no
accumulator hand-off — passing everything and generalizing to nothing.

So a second driver lands in `gnitz-core/tests`, with no dependencies beyond the
ones already there: submit N requests, then loop — `step(ready)`, drain the
completions into a `SlotId -> result` map, and park in `poll(2)` on `interest()`
— until every slot is done. Roughly twenty lines, because `step`'s
postcondition means the loop has no park predicate to consult. It is gated
`integration` like every other `gnitz-core` test that needs a server. It drives
N operations concurrently on one connection against a real server on both
transports, and it is what proves the queue, the cursor across steps, multi-slot
hand-off, the interest pair and the cap.

It carries **no futures, no `Waker` and no executor**. Nothing in this plan's
surface is a `Future`, so an executor here would exist only to have something to
poll, and every property listed above falls out of the plain loop. Because it
lives in `tests/`, the spine is `pub`: the four methods above, `as_raw_fd`,
`Interest`, `SlotId`, `Request`, `Reply` and `Completions`.

**The frame-level tests are crate-internal, and drive a scripted peer.** A train
arriving one frame per `step`, a `STATUS_ERROR` terminal in the middle of a
`scan_multi`, one TLS record carrying two frames — a real server produces none
of these on demand, and `tests/` cannot reach a socketpair anyway: `Session`'s
only constructor takes an address, and `make_transport_pair` is `pub(crate)`.
So those live in `#[cfg(test)]` modules inside `src`, beside the ones in
`transport/mod.rs` and `message.rs` that already build reply frames by hand and
pre-stage them on the far end of a socketpair, with a crate-internal
`Session::from_transport` to skip the handshake. The split is by what the test
needs to steer, not by convenience: the driver proves the spine under a real
server, the unit tests prove the reassembly under bytes only a test can
arrange.

## Tests

**Moved**: the twelve `recv_framed` call sites in `transport/mod.rs`'s test
module (ten free-fn, two method) go to the `ClientTransport` method, since the
free function is subsumed by `FrameReader`. With the free function gone, half of
every `make_socketpair` pair becomes a transport that owns its fd, so
`from_unix_fd` takes an `OwnedFd` instead of a `RawFd` and `make_socketpair`
returns a pair of them; the trailing `libc::close` calls go with the change.

Five of the seven `send_framed_batch` tests move onto the outbound queue:
`test_send_framed_batch_forces_multiple_writev` (more iovecs than `IOV_MAX`,
every frame intact and in order) and
`test_send_framed_batch_partial_writes_small_sndbuf` (500 frames through a
4 KiB `SO_SNDBUF`) cover properties the cursor inherits and nothing else
asserts — the second becomes the direct test of resumable flushing across
`EAGAIN` — plus the single-frame and many-frames-in-order pair, plus
`test_send_framed_batch_empty_input_ok_no_syscall`, whose property is "an empty
batch touches no fd" and which becomes `flush()` on an empty queue returning
`Ok(false)` without a syscall.

The remaining two become direct unit tests of `frame_len_prefix`, which is
already the single enforcement point they were reaching through an `fd = -1`
trick — the prefix is now computed at enqueue, so both rejections happen before
any queue entry exists, and the "no writev" half of
`test_send_framed_batch_rejects_empty_frame_no_writev` becomes structural.
`test_send_framed_batch_rejects_oversized_frame` could not move onto the queue
in any case: it fabricates a `&[u8]` whose length exceeds `u32::MAX` from a
dangling pointer, and an owned-`MessageParts` queue has no way to hold one.

**Must keep passing**: `tls_client.rs` in full, in particular
`eviction_observed_within`, `inbound_cap_breach_closes_stalled_connection`,
`stalled_scan_client_is_evicted_by_send_deadline`,
`pipelined_pushes_ahead_of_scan_do_not_deadlock` and
`big_push_and_multiframe_scan`; `slow_client_eviction.rs`;
`gnitz-mirror/tests/mirror.rs` and `test_mirror.py` for the raw ingest path;
the whole `make e2e` suite at `GNITZ_WORKERS=4`. `gnitz-mirror`'s tests are
ungated; `gnitz-core`'s and every `gnitz-sql` test file needing a server are
gated `--features …/integration`, which `make test` passes. `make e2e-tls`
covers the rewritten TLS read and write paths. `not_null_bit_rejection.rs` and
`test_async.py` both keep passing unchanged, on the six `Session` members that
stay.

**New**:
- `FrameReader` fed a two-frame stream split at every offset; over-read
  retained; a frame served wholly from `carry` with no read.
- One `step` consumes both frames of a single TLS record and completes both
  slots, leaving nothing buffered in `carry` or in rustls's plaintext buffer —
  the postcondition every parking driver rests on.
- A payload larger than the scratch lands in one allocation of exactly
  `payload_len`, asserted on the `Vec`'s capacity and on the pointer the raw
  reply hands out.
- A reply train split across N continuation frames arriving one frame per `step`
  completes exactly once; `scan_multi`'s N trains complete in request order.
- A `scan_multi` over two relations with *different* schemas, each replying
  warm, decodes every train under its own relation's schema — the shape a single
  tid held across all N trains gets wrong.
- A `scan_multi` whose k-th train is replaced by a `STATUS_ERROR` terminal fails
  that one slot and leaves the connection usable for the next request.
- TLS framing over a loopback `rustls::ServerConnection` through
  `ClientTransport::connect`; a frame split across two records.
- An established TLS connection idle past `CONNECT_TIMEOUT` still reads.
- `writev_all` and `flush_tls` make progress across `EAGAIN` under a shrunk
  socket buffer, and return `WouldBlock` at `SO_SNDTIMEO` rather than parking.
- A non-blocking read distinguishes `EAGAIN` from EOF.
- A peer that writes a whole frame and closes in one breath: the frame is
  delivered and the EOF surfaces on the next read, not instead of it. Driven
  with a frame sized to fill the scratch exactly, so the reader is obliged to
  read again and sees the `0`.
- Two reply frames arriving in two back-to-back TLS records come out of one
  `step` at one `read` syscall — the ciphertext-level drained inference.
- A `ClientError::Protocol` closes the blocking client: the next call reports
  the connection closed rather than submitting onto a desynced stream.
- A server that closes while an operation is parked completes it with an error
  on the next step rather than hanging.
- Under the concurrent driver: a pending write and a pending read arm both
  interests; the cap raises rather than hanging; an abandoned slot does not
  desync the next reply; `close` returns every pending slot exactly once and a
  later `submit` reports the connection closed.
- The blocking client and the concurrent driver agree on the same operation
  sequence against one server.
- The blocking client costs 3 syscalls per push over AF_UNIX and 3 over TLS,
  counted with `strace -f -c`.
- Ctrl-C interrupts a blocking call parked on the fd: a signal delivered during
  the park runs the hook rather than restarting the `poll`, and a hook that
  errors aborts the operation while leaving the connection usable for the next
  call, which drains the abandoned train first.

## Sequencing

Every commit leaves the suite green; the blocking-emulation wrappers are what
make that true across the middle of the sequence.

- [ ] Each direction splits into a non-blocking core and a park-and-retry
      wrapper: `writev_all` and `flush_tls` become the wrappers, waiting on
      `POLLOUT` under `SO_SNDTIMEO`; `recv_framed` parks on `POLLIN` after
      `EAGAIN`; the Unix read arm distinguishes `EAGAIN` from EOF.
      Behaviour-neutral until the fd goes non-blocking.
- [ ] `FrameReader` with `carry`, the 64 KiB scratch, exact-sized payload
      allocation and the short-read drained inference; `recv_framed` re-expressed
      as the reader driven to one frame, so the connection has exactly one
      reader; the payload ceiling moved onto the transport; the twelve test call
      sites moved; `from_unix_fd` takes an `OwnedFd`. Both arms meet at
      `read_into`, whose TLS side is still `StreamOwned::read` here, so the
      drained inference is honest on the Unix arm only — nothing depends on it
      yet, because `recv_framed` reads until its frame is complete either way.
- [ ] The TLS read arm rebuilt over `read_tls` / `reader()`, with the counting
      `Read` newtype that carries the drained inference into `read_tls`;
      `Inner::Tls` drops `StreamOwned` for the transport's own `{ conn, sock }`
      pair, nothing needing its `Read`/`Write` any more; the loopback
      `ServerConnection` test;
      `rcgen` added to `gnitz-core`'s dev-dependencies.
- [ ] The fd goes non-blocking in `mark_established`. The one commit where the
      switch is observable.
- [ ] The owned-`MessageParts` outbound queue, its cursor and the
      per-connection iovec buffer; `Session::send_batch` re-expressed as
      enqueue-then-flush-to-empty over the park wrapper;
      `send_framed_batch`, `FrameSegments` and the `SCRATCH` thread-local with
      its two caps deleted, five of their tests moved onto the queue and two
      onto `frame_len_prefix`.
- [ ] The spine: `submit` / `step` / `interest` / `close`, the
      head-of-queue accumulator, the pending queue, the 4096-request cap, the
      interest pair, `frames_sent` counted on enqueue,
      `Request` / `Reply` / `Completions`, and the per-slot-versus-connection
      error split. `Session::from_transport` and the scripted-peer unit tests
      for reassembly land with it.
- [ ] `Session::round_trip` — the park loop, its `close` on a
      `ClientError::Protocol`, and the signal hook — with the nine bodies
      reduced to building a `Request` and narrowing a `Reply`;
      `drain_reply_train_raw` deleted. The six members the background I/O
      transport calls stay public and blocking.
- [ ] The concurrent test driver, and the spine tests that only it can reach.
