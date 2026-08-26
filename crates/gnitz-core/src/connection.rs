//! The protocol session: a sans-io connection state machine and the blocking
//! verbs written over it.
//!
//! The **spine** is five methods — [`Session::submit`], [`Session::step`],
//! [`Session::interest`], [`Session::close`] and [`Session::as_raw_fd`] — and
//! "nothing in it ever waits" is a property of those five, not of the type.
//! `submit` encodes a request against the connection's schema cache and
//! registers a slot; `step` does the I/O the driver says the fd is ready for
//! and reports which slots completed; waiting belongs to whoever drives it.
//! Above the spine sit `Session`'s own blocking verb bodies, which park in
//! [`Session::round_trip`]; beneath it, the blocking reply path the gnitz-py
//! background I/O transport still calls (`send_batch`, `recv_push_ack`,
//! `recv_scan`), which reads through `ClientTransport::recv_framed`.
//!
//! Replies leave the server in request order, so there is exactly one reply
//! accumulator and it belongs to the head of the pending queue: every byte
//! that arrives is the head slot's until its last train terminates.

use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::ClientError;
use crate::protocol::codec::encode_schema_block;
use crate::protocol::message::{encode_message_noschema_parts, encode_message_parts, MessageParts};
use crate::protocol::transport::{poll_fd, Next};
use crate::protocol::{
    encode_control_frame, encode_ddl_txn, encode_push_txn, encode_scan_multi, hello_handshake, parse_response,
    parse_response_frame, recv_message, wire_flags_get_schema_version, wire_flags_set_conflict_mode,
    wire_flags_set_schema_version, ClientTransport, Message, PkTuple, ProtocolError, Schema, WireConflictMode,
    ZSetBatch, FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE, FLAG_ALLOCATE_TABLE_ID,
    FLAG_CONTINUATION, FLAG_PUSH, FLAG_RESOLVE, FLAG_SCAN_SPEC, FLAG_SEEK, FLAG_SEEK_BY_INDEX, STATUS_DELTA_EXPIRED,
    STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};
use gnitz_wire::RelDescriptorBlob;
use lru::LruCache;

pub use gnitz_wire::{
    COL_TAB, FIRST_USER_SCHEMA_ID, FIRST_USER_TABLE_ID, IDX_TAB, SCHEMA_TAB, SEQ_TAB, TABLE_TAB, VIEW_TAB,
};

/// Per-connection schema LRU capacity. Sized to comfortably hold a session's
/// working set of tables/views without unbounded growth.
const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();

/// Requests one connection may hold in flight; `submit` raises past it — a
/// bound on the memory a driver that never waits can pin, not a throughput
/// knob: 4× the deepest batch anything hands a connection.
pub const MAX_IN_FLIGHT: usize = 4096;

/// One relation's reply to a `scan`/`seek`/`seek_by_index`: the (cached)
/// `Schema`, the materialised `ZSetBatch` if any rows came back, and the server
/// LSN at which the read was served.
pub type ScanReply = (Option<Arc<Schema>>, Option<ZSetBatch>, u64);

/// The single-relation read result.
pub type ScanResult = Result<ScanReply, ClientError>;

/// One reply frame's data block, kept undecoded: the owned frame buffer and the
/// block's extent within it. `block()` is the block itself.
pub struct RawBlock {
    frame: Vec<u8>,
    block: std::ops::Range<usize>,
}

impl RawBlock {
    /// The data block's bytes, ready to decode against the reply schema.
    pub fn block(&self) -> &[u8] {
        &self.frame[self.block.clone()]
    }
}

/// The N per-relation results of a `scan_multi`, in request order. Every
/// relation was snapshotted at the same server-side SAL cut, so an atomic
/// multi-table commit is never torn across the result set.
pub type MultiScanResult = Result<Vec<ScanReply>, ClientError>;

/// Generate a session-unique client ID.
///
/// Combines PID (top 32 bits) with a per-process monotonic sequence (bottom 32 bits).
/// This guarantees uniqueness across all connections from the same process, and makes
/// cross-process collisions practically impossible even with PID reuse.
fn new_client_id() -> u64 {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed) as u64;
    (std::process::id() as u64) << 32 | seq
}

fn check_response(msg: Message) -> Result<Message, ClientError> {
    if msg.status == STATUS_SCHEMA_MISMATCH {
        return Err(ClientError::SchemaMismatch);
    }
    if msg.status == STATUS_NO_INDEX {
        return Err(ClientError::ServerError("no index on requested column".into()));
    }
    if msg.status == STATUS_DELTA_EXPIRED {
        // The cursor named rounds the refusing worker's capacity sweep dropped.
        // Structured, not a string, so the subscriber can react by re-reading at
        // `after_tick = 0` — the read it made on its first day — rather than by
        // matching on text.
        return Err(ClientError::DeltaExpired);
    }
    if msg.status == STATUS_TXN_CONFLICT {
        // Control-only frame: the fresh basis rides in `seek_pk`. Left as a
        // structured error so the SQL layer can retry (autocommit) or surface
        // it (BEGIN/COMMIT); the human-readable text is synthesized upstream.
        return Err(ClientError::TxnConflict {
            fresh_basis: msg.seek_pk as u64,
        });
    }
    if msg.status == STATUS_ERROR {
        // Fall back to the default text on an empty string, not only on None:
        // a STATUS_ERROR with Some("") would otherwise surface as a blank
        // ServerError. This matters because the warm-push guard converts
        // silent corruption into a surfaced error, which must be legible.
        let text = msg
            .error_text
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown server error".into());
        return Err(ClientError::ServerError(text));
    }
    if msg.status != STATUS_OK {
        // A status this build does not know is a failure, not a success: falling
        // through would hand the caller an empty reply and no error, so a read
        // refused for a reason we cannot name would read as "no rows". The
        // worker→master leg carries whatever status a fault names, so the set is
        // not fixed by this crate.
        return Err(ClientError::ServerError(format!(
            "server returned unrecognized status {}",
            msg.status
        )));
    }
    Ok(msg)
}

fn closed_error() -> ClientError {
    ClientError::ServerError("connection closed".into())
}

/// Which relation a RESOLVE request describes. The wire carries an id field and
/// a name blob and lets the name win, but exactly one is ever meaningful — this
/// says which, so no caller has to encode that as a `0` / `""` sentinel pair.
#[derive(Copy, Clone, Debug)]
pub(crate) enum RelTarget<'a> {
    /// The canonical `"schema_name.relation_name"`.
    Name(&'a str),
    Id(u64),
}

// ── The spine's vocabulary ───────────────────────────────────────────────────

/// What a driver should wait for before its next `step`, and what it hands
/// back: two booleans on the connection, never a per-slot union.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Interest {
    pub read: bool,
    pub write: bool,
}

impl Interest {
    pub const NONE: Interest = Interest {
        read: false,
        write: false,
    };
    pub const READ: Interest = Interest {
        read: true,
        write: false,
    };
    pub const WRITE: Interest = Interest {
        read: false,
        write: true,
    };

    pub fn is_empty(self) -> bool {
        self == Interest::NONE
    }

    /// The `poll(2)` event mask to wait for.
    pub fn poll_events(self) -> libc::c_short {
        (if self.read { libc::POLLIN } else { 0 }) | (if self.write { libc::POLLOUT } else { 0 })
    }

    /// What a `poll(2)` wakeup says is ready, as the set to hand `step`.
    /// `POLLHUP` / `POLLERR` fold into `read` — they arrive whether requested
    /// or not, and the step that follows reads the EOF or reset and surfaces
    /// it; a driver that ignored them would park forever on a dead peer.
    pub fn from_revents(revents: libc::c_short) -> Interest {
        Interest {
            read: revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0,
            write: revents & libc::POLLOUT != 0,
        }
    }
}

/// A pending request. Monotonic, never an index into a recycled table: a
/// driver holding one across an abandonment cannot match a later slot.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(u64);

/// One request, cut where the encoding or the decoding differs rather than
/// where the verb names do. It borrows its inputs; the borrow ends at
/// `submit`, which encodes there and then.
pub enum Request<'a> {
    /// A correlated control frame — SCAN, SEEK, SEEK_BY_INDEX: the reply's
    /// `target_id` must be this one, and any schema block it carries is
    /// absorbed into the cache under it.
    Control {
        target_id: u64,
        flags: u64,
        seek_pk: u128,
        seek_col_idx: u64,
        seek_pk_extra: &'a [u8],
    },
    /// A pre-encoded, uncorrelated frame whose *answer* may be a target id:
    /// the id allocations, RESOLVE, and the two transaction frames. Nothing is
    /// absorbed into the cache. Owned, because it is the one variant whose
    /// encoding already happened — a transaction frame can be megabytes, and
    /// every producer holds the `Vec` this becomes the queue entry of.
    Uncorrelated(Vec<u8>),
    /// PUSH — correlated like `Control`, and the one variant that chooses the
    /// warm (schema-less) encoding against the cold one. `cold` forces the
    /// schema block, which is what the mismatch retry sends.
    Push {
        target_id: u64,
        schema: &'a Schema,
        batch: &'a ZSetBatch,
        mode: WireConflictMode,
        cold: bool,
    },
    /// SCAN_SPEC, carrying the caller's reply schema — the decode hint for
    /// every frame of the train, since the server sends none back — and
    /// whether to keep the data blocks raw. Uncorrelated and off the cache in
    /// both directions.
    ScanSpec {
        target_id: u64,
        spec: &'a [u8],
        reply_schema: &'a Schema,
        raw: bool,
    },
    /// SCAN_MULTI: N trains in request order, each decoded under its own
    /// relation.
    ScanMulti(&'a [u64]),
}

/// One reassembled train. The terminal frame is kept whole because it is what
/// several verbs' answers live in — `target_id` for an id allocation,
/// `seek_pk` for a push ACK's LSN and a scan's watermark, `seek_pk_extra` for
/// a RESOLVE's descriptor blob — and its own data block and schema are folded
/// into `data` / `schema`, not left on it.
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
    Raw {
        blocks: Vec<RawBlock>,
        terminal: Message,
    },
    /// `scan_multi`: N trains in request order.
    Multi(Vec<ReplyTrain>),
}

pub type Completions = Vec<(SlotId, Result<Reply, ClientError>)>;

/// The three decode facts a slot carries, read off the `Request` variant.
enum SlotKind {
    /// Correlate on `tid` and absorb schema blocks under it.
    Correlated {
        tid: u64,
    },
    Uncorrelated,
    ScanSpec {
        reply_schema: Arc<Schema>,
        raw: bool,
    },
    Multi {
        tids: Vec<u64>,
    },
}

struct Slot {
    id: SlotId,
    kind: SlotKind,
}

/// The train-in-progress of the head slot.
#[derive(Default)]
struct Accumulator {
    schema: Option<Arc<Schema>>,
    data: Option<ZSetBatch>,
    blocks: Vec<RawBlock>,
    /// Finished trains of a `scan_multi`; its index is `trains.len()`.
    trains: Vec<ReplyTrain>,
}

/// A protocol session: the transport plus all per-connection protocol state
/// (client id, the schema LRU, the pending queue and reply accumulator, and
/// the warm/cold packing, continuation reassembly, cache absorption, and
/// status→error policy that read/write them). Exactly one owner of that
/// state — the sync [`crate::GnitzClient`] holds one; the gnitz-py async I/O
/// thread holds its own. Because the session owns the cache, no `LruCache` is
/// threaded as a parameter and no cache lock is shared across threads.
pub struct Session {
    transport: ClientTransport,
    pub client_id: u64,
    schema_cache: LruCache<u64, (Arc<Schema>, u16)>,
    pending: VecDeque<Slot>,
    next_slot: u64,
    accum: Accumulator,
    closed: bool,
    /// Run before every park of the blocking client; its `Err` aborts the
    /// operation. `Send + Sync` so the field cannot silently narrow the
    /// auto-traits of the public types that hold a session.
    park_hook: Option<ParkHook>,
}

/// The blocking client's pre-park hook — how a host runtime gets a turn (the
/// Python binding runs `PyErr_CheckSignals` here, which is what makes a long
/// call Ctrl-C-interruptible).
pub type ParkHook = Box<dyn FnMut() -> Result<(), ClientError> + Send + Sync>;

impl Session {
    /// `target` is an AF_UNIX socket path or a `tls://HOST:PORT[?PARAM]`
    /// address (see `ClientTransport::connect`). Returns the session paired with
    /// the server durability watermark from the HELLO ACK, which
    /// `GnitzClient::connect` adopts as the seed for its OCC basis.
    pub fn connect(target: &str) -> Result<(Self, u64), ClientError> {
        let mut transport = ClientTransport::connect(target)?;
        // Run the HELLO handshake before any data flows. The server
        // accepts the first frame at an 8-byte limit, so this must
        // happen before a control block would be emitted.
        let published_lsn = hello_handshake(&mut transport)?;
        Ok((Self::over(transport), published_lsn))
    }

    fn over(transport: ClientTransport) -> Self {
        Session {
            transport,
            client_id: new_client_id(),
            schema_cache: LruCache::new(SCHEMA_CACHE_CAP),
            pending: VecDeque::new(),
            next_slot: 1,
            accum: Accumulator::default(),
            closed: false,
            park_hook: None,
        }
    }

    /// A session over a transport that never ran the handshake, marked
    /// established under the client ceiling. For the scripted-peer tests.
    #[cfg(test)]
    pub(crate) fn from_transport(mut transport: ClientTransport) -> Self {
        transport.mark_established(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT);
        Self::over(transport)
    }

    /// Request frames this session has written since it connected. Counted in
    /// the transport as a request is enqueued, so it covers every verb without
    /// a per-path bump and batching cannot move it. Tests assert on it to pin
    /// the per-statement round-trip count.
    pub fn requests_sent(&self) -> u64 {
        self.transport.frames_sent()
    }

    /// Handle that unblocks a blocking recv parked in another thread (the
    /// async I/O loop's teardown wake). Delegates to the transport.
    pub fn waker(&self) -> Result<crate::protocol::TransportWaker, ProtocolError> {
        self.transport.waker()
    }

    /// The fd a driver polls. The one thing the spine will not do for it.
    pub fn as_raw_fd(&self) -> RawFd {
        self.transport.as_raw_fd()
    }

    #[cfg(test)]
    pub(crate) fn queue_is_empty(&self) -> bool {
        self.transport.queue_is_empty()
    }

    /// Install (or clear) the hook `round_trip` runs before each park.
    pub fn set_park_hook(&mut self, hook: Option<ParkHook>) {
        self.park_hook = hook;
    }

    // ── The spine ──────────────────────────────────────────────────────────

    /// Encode against this connection's schema cache, enqueue, register a slot.
    /// Raises past the in-flight cap. `Request` borrows its inputs; the borrow
    /// ends here, because encoding is what `submit` does.
    pub fn submit(&mut self, req: Request<'_>) -> Result<SlotId, ClientError> {
        if self.closed {
            return Err(closed_error());
        }
        if self.pending.len() >= MAX_IN_FLIGHT {
            return Err(ClientError::ServerError(format!(
                "connection has {MAX_IN_FLIGHT} requests in flight"
            )));
        }
        let client_id = self.client_id;
        let (parts, kind) = match req {
            Request::Control {
                target_id,
                flags,
                seek_pk,
                seek_col_idx,
                seek_pk_extra,
            } => {
                let flags = self.versioned_flags(target_id, flags);
                let ctrl = encode_control_frame(target_id, client_id, flags, seek_pk, seek_col_idx, seek_pk_extra);
                (control_parts(ctrl), SlotKind::Correlated { tid: target_id })
            }
            Request::Uncorrelated(frame) => (control_parts(frame), SlotKind::Uncorrelated),
            Request::Push {
                target_id,
                schema,
                batch,
                mode,
                cold,
            } => {
                // FLAG_PUSH marks the frame as a push independent of data
                // presence, so an empty batch (a legitimate empty Z-set delta)
                // is ACKed as a no-op push instead of being mistaken for a scan.
                let base_flags = wire_flags_set_conflict_mode(FLAG_PUSH, mode);
                // The warm path is gated on `types_match`, not the version
                // alone: a version proves the catalog has not changed, not
                // that the caller encoded under the same column types, and a
                // schema-less frame under mismatched types is reinterpreted
                // silently at rest.
                let warm_version = match self.schema_cache.peek(&target_id) {
                    Some((cached, v)) if !cold && *v != 0 && schema.types_match(cached.as_ref()) => Some(*v),
                    _ => None,
                };
                let parts = match warm_version {
                    Some(v) => {
                        let flags = wire_flags_set_schema_version(base_flags, v);
                        encode_message_noschema_parts(target_id, client_id, flags, schema, batch)
                    }
                    None => encode_message_parts(
                        target_id,
                        client_id,
                        base_flags,
                        &PkTuple::EMPTY,
                        0,
                        Some((schema, batch)),
                    ),
                };
                (parts, SlotKind::Correlated { tid: target_id })
            }
            Request::ScanSpec {
                target_id,
                spec,
                reply_schema,
                raw,
            } => {
                // The reply schema rides the request blob (the master forwards it
                // verbatim) and stays with the slot as the decode hint.
                let block = encode_schema_block(reply_schema, target_id as u32);
                let extra = gnitz_wire::pack_scan_spec_extra(spec, &block);
                let ctrl = encode_control_frame(target_id, client_id, FLAG_SCAN_SPEC, 0, 0, &extra);
                (
                    control_parts(ctrl),
                    SlotKind::ScanSpec {
                        reply_schema: Arc::new(reply_schema.clone()),
                        raw,
                    },
                )
            }
            Request::ScanMulti(tids) => (
                control_parts(self.encode_scan_multi_frame(tids)?),
                SlotKind::Multi { tids: tids.to_vec() },
            ),
        };
        self.transport.enqueue(parts)?;
        let id = SlotId(self.next_slot);
        self.next_slot += 1;
        self.pending.push_back(Slot { id, kind });
        Ok(id)
    }

    /// Do the I/O `ready` says the fd will accept — flush the cursor on
    /// `WRITE`, read until the source is drained on `READ`, neither on an empty
    /// set — advancing the reply train at the head of the pending queue as
    /// bytes arrive. Returns every slot that completed, and returns only once
    /// nothing buffered can advance another one, so a driver may park on
    /// `interest()` immediately afterwards.
    ///
    /// A per-slot `Err` (any `STATUS_*` the server names) leaves the connection
    /// usable. An `Err` from `step` itself is a transport or protocol failure:
    /// the byte stream's framing is no longer trustworthy, and the driver's
    /// only move is `close`.
    pub fn step(&mut self, ready: Interest) -> Result<Completions, ClientError> {
        let mut done: Completions = Vec::new();
        if self.closed {
            return Ok(done);
        }
        if ready.write {
            self.transport.flush()?;
        }
        if ready.read {
            self.transport.begin_read();
        }
        while let Next::Frame(buf) = self.transport.next_frame(ready.read)? {
            self.feed(buf, &mut done)?;
        }
        Ok(done)
    }

    /// `READ` while any slot is outstanding; `WRITE` while bytes remain queued
    /// or rustls has ciphertext to ship.
    pub fn interest(&self) -> Interest {
        Interest {
            read: !self.pending.is_empty(),
            write: self.transport.wants_write(),
        }
    }

    /// Abandon every pending slot, return them, and refuse further work: a
    /// later `submit` returns `ClientError::ServerError("connection closed")`.
    /// Drops the outbound queue and resets the accumulator with them, so
    /// `interest()` reports nothing afterwards and a driver still in its loop
    /// finds no readiness to wait on rather than spinning on a `WRITE` bit
    /// nothing will ever clear.
    pub fn close(&mut self) -> Vec<SlotId> {
        self.closed = true;
        self.transport.clear_queue();
        self.accum = Accumulator::default();
        self.pending.drain(..).map(|s| s.id).collect()
    }

    /// One reply frame for the head slot. Status is classified on every
    /// frame before the continuation test: a `STATUS_ERROR` fault frame has
    /// flags 0, structurally identical to a terminal frame, and a non-OK frame
    /// ends the whole request — a `scan_multi` rejection after k trains sends
    /// one error frame and nothing more.
    fn feed(&mut self, buf: Vec<u8>, done: &mut Completions) -> Result<(), ClientError> {
        let Some(head) = self.pending.front() else {
            return Err(ClientError::Protocol(ProtocolError::DecodeError(
                "reply frame with no request pending".into(),
            )));
        };
        // The relation a `scan_multi` train decodes under advances with the
        // train: tids[i] for train i, never the slot's first tid. The decode
        // hint is the slot's own reply schema when it is off the cache, else
        // the cached schema for the correlated relation.
        let (correlate_tid, raw, multi_len, hint_owned) = match &head.kind {
            SlotKind::Correlated { tid } => (Some(*tid), false, None, self.cached_hint(*tid)),
            SlotKind::Uncorrelated => (None, false, None, None),
            SlotKind::ScanSpec { reply_schema, raw } => (None, *raw, None, Some((Arc::clone(reply_schema), 0))),
            SlotKind::Multi { tids } => {
                let tid = tids[self.accum.trains.len()];
                (Some(tid), false, Some(tids.len()), self.cached_hint(tid))
            }
        };
        let hint = hint_owned.as_ref().map(|(s, v)| (s.as_ref(), *v));

        let (msg, block) = if raw {
            let parsed = parse_response_frame(&buf, hint)?;
            (parsed.message, parsed.data_block)
        } else {
            (parse_response(&buf, hint)?, None)
        };
        let mut msg = match check_response(msg) {
            Ok(m) => m,
            Err(e) => {
                self.complete_head(Err(e), done);
                return Ok(());
            }
        };
        if let Some(tid) = correlate_tid {
            // Replies arrive in request order and the hint is keyed by
            // `target_id`, so an out-of-order frame would decode under the
            // wrong schema silently; make it loud, before any absorb.
            if msg.target_id != tid {
                return Err(ClientError::Protocol(ProtocolError::DecodeError(format!(
                    "reply out of order: expected target {tid}, got {}",
                    msg.target_id
                ))));
            }
            if let Some(s) = msg.schema.as_ref() {
                self.schema_cache
                    .put(tid, (Arc::clone(s), wire_flags_get_schema_version(msg.flags)));
            }
        }

        let schema = msg.schema.take();
        let data_batch = msg.data_batch.take();
        let terminal = msg;
        let acc = &mut self.accum;
        acc.schema = acc.schema.take().or(schema);
        if let Some(range) = block {
            acc.blocks.push(RawBlock {
                frame: buf,
                block: range,
            });
        }
        if let Some(batch) = data_batch {
            match acc.data.as_mut() {
                Some(a) => a.extend_from_owned(batch),
                None => acc.data = Some(batch),
            }
        }
        if terminal.flags & FLAG_CONTINUATION != 0 {
            return Ok(());
        }

        // The train terminated.
        let train = ReplyTrain {
            terminal,
            schema: acc.schema.take(),
            data: acc.data.take(),
        };
        let reply = match multi_len {
            Some(n) => {
                acc.trains.push(train);
                if acc.trains.len() < n {
                    return Ok(());
                }
                Reply::Multi(std::mem::take(&mut acc.trains))
            }
            None if raw => Reply::Raw {
                blocks: std::mem::take(&mut acc.blocks),
                terminal: train.terminal,
            },
            None => Reply::Train(train),
        };
        self.complete_head(Ok(reply), done);
        Ok(())
    }

    /// The cached `(schema, version)` for `tid` as an owned decode hint.
    /// `get` (not `peek`) so a relation in use refreshes its LRU recency.
    fn cached_hint(&mut self, tid: u64) -> Option<(Arc<Schema>, u16)> {
        self.schema_cache.get(&tid).map(|(s, v)| (Arc::clone(s), *v))
    }

    /// The head slot is done: report it and hand the accumulator to the next.
    fn complete_head(&mut self, result: Result<Reply, ClientError>, done: &mut Completions) {
        let slot = self.pending.pop_front().expect("a frame was fed to a pending head");
        self.accum = Accumulator::default();
        done.push((slot.id, result));
    }

    // ── The blocking client ────────────────────────────────────────────────

    /// Submit, then drive to completion. The one place the blocking client
    /// waits. A `step` error closes the connection: its framing can no longer
    /// be trusted, and the next call reports it closed rather than submitting
    /// onto a desynced stream. A slot abandoned by an aborting park stays
    /// pending, and the next call drains its train before its own can start.
    fn round_trip(&mut self, req: Request<'_>) -> Result<Reply, ClientError> {
        let slot = self.submit(req)?;
        let mut ready = Interest::WRITE;
        loop {
            match self.step(ready) {
                Ok(mut done) => {
                    if let Some(i) = done.iter().position(|(s, _)| *s == slot) {
                        return done.swap_remove(i).1;
                    }
                }
                Err(e) => {
                    self.close();
                    return Err(e);
                }
            }
            ready = self.park(self.interest())?;
        }
    }

    /// `poll(2)`, returning the readiness as an `Interest`. The one poll that
    /// does not retry `EINTR` in place: CPython installs its handlers without
    /// `SA_RESTART`, so a signal lands here as `EINTR`, and that is when the
    /// hook runs — a signal arriving anywhere else is delivered when the call
    /// returns to its host, which is where the hot path stays free of the
    /// hook's cost (a GIL acquisition, for the Python binding).
    fn park(&mut self, interest: Interest) -> Result<Interest, ClientError> {
        if interest.is_empty() {
            return Err(closed_error());
        }
        loop {
            match poll_fd(self.transport.as_raw_fd(), interest.poll_events(), None, false) {
                Ok(revents) => return Ok(Interest::from_revents(revents)),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    if let Some(hook) = self.park_hook.as_mut() {
                        hook()?;
                    }
                }
                Err(e) => return Err(ClientError::Protocol(ProtocolError::IoError(e))),
            }
        }
    }

    /// `round_trip` narrowed to a single train.
    fn round_trip_train(&mut self, req: Request<'_>) -> Result<ReplyTrain, ClientError> {
        match self.round_trip(req)? {
            Reply::Train(t) => Ok(t),
            _ => unreachable!("a single-train request completes as Reply::Train"),
        }
    }

    /// An uncorrelated control-only round trip: one control frame, one train.
    fn uncorrelated(
        &mut self,
        target_id: u64,
        flags: u64,
        seek_col_idx: u64,
        extra: &[u8],
    ) -> Result<ReplyTrain, ClientError> {
        let frame = encode_control_frame(target_id, self.client_id, flags, 0, seek_col_idx, extra);
        self.round_trip_train(Request::Uncorrelated(frame))
    }

    /// An id allocation: the answer rides the terminal frame's `target_id`.
    fn alloc(&mut self, target_id: u64, flag: u64, seek_col_idx: u64) -> Result<u64, ClientError> {
        Ok(self
            .uncorrelated(target_id, flag, seek_col_idx, &[])?
            .terminal
            .target_id)
    }

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_TABLE_ID, 0)
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_SCHEMA_ID, 0)
    }

    pub fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_INDEX_ID, 0)
    }

    /// Reserve a contiguous range of `count` SERIAL ids for the sequence keyed
    /// by `seq_table_id`. Returns the range base; the caller owns
    /// `[base, base + count)`. The range `count` rides in `seek_col_idx`, and
    /// `target_id = seq_table_id ≠ 0` steers the master to the durable
    /// range-advance branch.
    pub fn alloc_serial_range(&mut self, seq_table_id: u64, count: u64) -> Result<u64, ClientError> {
        self.alloc(seq_table_id, FLAG_ALLOCATE_SERIAL_RANGE, count)
    }

    pub fn push_with_mode(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        batch.validate(schema).map_err(ClientError::ServerError)?;
        let msg = self.roundtrip_push(target_id, schema, batch, mode)?;
        Ok(msg.seek_pk as u64)
    }

    /// Send an atomic DDL transaction: a bundle of system-table family batches
    /// (`FLAG_DDL_TXN`) that the server ingests under one durable SAL zone. Used
    /// by every catalog write — a `CREATE`'s N families or a
    /// `DROP`/`CREATE INDEX`/`CREATE SCHEMA`'s single family. Returns the zone
    /// LSN (echoed in the ACK's `seek_pk`, as `push` does).
    ///
    /// No schema cache interaction: system-table schemas are compile-time-fixed
    /// and known to both sides, so the frame carries a schema block per family
    /// (via `encode_wal_block`) and the server resolves each family's schema from
    /// its own catalog.
    pub fn push_ddl_txn(&mut self, families: &[(u64, ZSetBatch)]) -> Result<u64, ClientError> {
        for (tid, batch) in families {
            batch
                .validate(crate::types::sys_schema(*tid))
                .map_err(ClientError::ServerError)?;
        }
        let payload = encode_ddl_txn(self.client_id, families);
        self.send_txn_frame(payload)
    }

    /// Send a pre-encoded transaction frame and receive its uncorrelated
    /// zone-LSN ACK (`seek_pk`). Shared by `push_ddl_txn` and `push_txn`.
    fn send_txn_frame(&mut self, payload: Vec<u8>) -> Result<u64, ClientError> {
        let train = self.round_trip_train(Request::Uncorrelated(payload))?;
        Ok(train.terminal.seek_pk as u64)
    }

    /// Send an atomic **user-table** push transaction (`FLAG_PUSH_TXN`): a bundle
    /// of user-table families — each carrying its conflict mode and its schema
    /// block — that the server validates as a unit under the union of the
    /// involved table locks and commits under one durable SAL zone. Returns the
    /// zone LSN (echoed in the ACK's `seek_pk`, as `push` does).
    ///
    /// The reply is received uncorrelated, exactly as `push_ddl_txn` does. Each
    /// family's batch is validated client-side before encoding, and the encoded
    /// frame is bounds-checked against the server ingress cap so an oversized
    /// bundle fails locally rather than being truncated on the wire.
    ///
    /// `preconditions` carries the OCC `(tid, basis)` assertions ("`tid` not
    /// written since `basis`"); the server rejects the whole transaction with
    /// `ClientError::TxnConflict` if any fails. Every precondition tid must be a
    /// family tid (the engine rejects otherwise). Pass an empty slice for none.
    pub fn push_txn(
        &mut self,
        families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)],
        preconditions: &[(u64, u64)],
    ) -> Result<u64, ClientError> {
        for (_, schema, batch, _) in families {
            batch.validate(schema).map_err(ClientError::ServerError)?;
        }
        let payload = encode_push_txn(self.client_id, families, preconditions);
        if payload.len() > gnitz_wire::MAX_FRAME_PAYLOAD_SERVER {
            return Err(ClientError::ServerError(format!(
                "transaction frame is {} bytes, exceeding the {}-byte server ingress cap; split the transaction",
                payload.len(),
                gnitz_wire::MAX_FRAME_PAYLOAD_SERVER
            )));
        }
        self.send_txn_frame(payload)
    }

    /// A correlated control-only round trip narrowed to a `ScanReply`: the
    /// schema the train carried else the cached one, its rows, and the
    /// terminal watermark as a plain LSN.
    fn control_scan(
        &mut self,
        target_id: u64,
        flags: u64,
        seek_pk: u128,
        seek_col_idx: u64,
        extra: &[u8],
    ) -> ScanResult {
        let train = self.round_trip_train(Request::Control {
            target_id,
            flags,
            seek_pk,
            seek_col_idx,
            seek_pk_extra: extra,
        })?;
        Ok(self.narrow_train(target_id, train))
    }

    fn narrow_train(&mut self, target_id: u64, train: ReplyTrain) -> ScanReply {
        (
            self.schema_or_cached(target_id, train.schema),
            train.data,
            train.terminal.seek_pk as u64,
        )
    }

    pub fn scan(&mut self, target_id: u64) -> ScanResult {
        self.control_scan(target_id, 0, 0, 0, &[])
    }

    /// Consistent multi-relation scan: snapshot every relation in `tids` at one
    /// server-side SAL cut and return their results in request order. An atomic
    /// multi-table commit (a `push_txn`) is either visible in every result or in
    /// none — never torn across the set. `scan_multi(&[t]) == scan(t)`.
    ///
    /// Sends one SCAN_MULTI frame stamped with each relation's cached schema
    /// version (so warm relations omit their schema block), then reads the N
    /// reply trains positionally, each decoded and cache-absorbed under its own
    /// relation. Like `scan`, it does not advance any commit watermark. A
    /// duplicate tid or a list outside `1..=SCAN_MULTI_MAX_RELATIONS` is
    /// rejected locally by the shared frame encoder (the same check the server
    /// runs) before the frame is sent; other shape/tid errors surface from the
    /// server as `ClientError::ServerError`.
    pub fn scan_multi(&mut self, tids: &[u64]) -> MultiScanResult {
        let trains = match self.round_trip(Request::ScanMulti(tids))? {
            Reply::Multi(t) => t,
            _ => unreachable!("a scan_multi completes as Reply::Multi"),
        };
        Ok(tids
            .iter()
            .zip(trains)
            .map(|(&tid, train)| self.narrow_train(tid, train))
            .collect())
    }

    /// One single-frame seek round trip. `base_flag` picks the seek kind; the
    /// cached schema version is embedded so the server can omit the schema block
    /// on a warm-cache hit (matching push/scan).
    fn seek_roundtrip(&mut self, target_id: u64, base_flag: u64, pk: &PkTuple, seek_col_idx: u64) -> ScanResult {
        let (seek_pk, seek_pk_extra) = pk.split_wire();
        self.control_scan(target_id, base_flag, seek_pk, seek_col_idx, seek_pk_extra)
    }

    pub fn seek(&mut self, target_id: u64, pk: &PkTuple) -> ScanResult {
        self.seek_roundtrip(target_id, FLAG_SEEK, pk, 0)
    }

    pub fn seek_by_index(&mut self, table_id: u64, col_indices: &[u32], key_vals: &[u128]) -> ScanResult {
        // `seek_roundtrip`'s `split_wire` routes slot 0 → seek_pk and slots 1..K →
        // seek_pk_extra, where the worker reassembles them with
        // `unpack_index_key_slots`. Arity is validated upstream in
        // `GnitzClient::seek_by_index` (the one choke point for every binding).
        let (buf, len) = gnitz_wire::pack_index_key_slots(key_vals);
        let pk = PkTuple::from_bytes(&buf[..len]);
        let seek_col_idx = gnitz_wire::pack_pk_cols(col_indices);
        self.seek_roundtrip(table_id, FLAG_SEEK_BY_INDEX, &pk, seek_col_idx)
    }

    /// Describe one relation in a single round trip: `(live tid, schema,
    /// descriptor)`, or `None` when no such relation exists — a successful
    /// answer the caller renders in its own wording.
    ///
    /// The descriptor's foreign keys are merged into the schema here, while the
    /// `Arc` is still unique, so the block installed in `schema_cache` is the
    /// same FK-complete schema the caller gets rather than a second copy of it.
    ///
    /// The reply is received uncorrelated: it never carries data, and a
    /// correlated slot would absorb the block under the *requested* tid, which
    /// for a by-name resolve is 0. The block is instead installed under the
    /// **live** tid the reply carries, which is what keeps a following
    /// `scan`/`push` on its warm path.
    pub(crate) fn resolve(
        &mut self,
        target: RelTarget<'_>,
    ) -> Result<Option<(u64, Arc<Schema>, RelDescriptorBlob)>, ClientError> {
        // The one place the request's "name wins, else id" encoding is spelled.
        let (target_id, qname) = match target {
            RelTarget::Name(q) => (0, q),
            RelTarget::Id(tid) => (tid, ""),
        };
        // The name rides an explicit extra blob rather than a `PkTuple`, whose
        // `split_wire` would silently truncate it past `MAX_PK_BYTES`.
        let train = self.uncorrelated(target_id, FLAG_RESOLVE, 0, qname.as_bytes())?;
        let msg = train.terminal;
        let ncols = train.schema.as_ref().map_or(0, |s| s.columns.len());
        let Some(desc) = RelDescriptorBlob::decode(&msg.seek_pk_extra, ncols).map_err(ClientError::ServerError)? else {
            return Ok(None);
        };
        let mut schema = train
            .schema
            .ok_or_else(|| ClientError::ServerError("resolve reply carried no schema block".to_string()))?;
        // `batch_to_schema` rebuilds every column-layout fact but leaves the FK
        // fields at 0 — a reference to *another* relation rides the descriptor.
        // `decode` bounded every `col_idx` against this schema's column count.
        if !desc.fks.is_empty() {
            let cols = &mut Arc::make_mut(&mut schema).columns;
            for fk in &desc.fks {
                cols[fk.col_idx as usize].fk_table_id = fk.fk_table_id;
                cols[fk.col_idx as usize].fk_col_idx = fk.fk_col_idx as u64;
            }
        }
        self.schema_cache.put(
            msg.target_id,
            (Arc::clone(&schema), wire_flags_get_schema_version(msg.flags)),
        );
        Ok(Some((msg.target_id, schema, desc)))
    }

    /// [`Self::scan_spec`] keeping the reply's raw data blocks instead of a
    /// decoded batch; the caller decodes them itself.
    pub fn scan_spec_raw(
        &mut self,
        target_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<(Vec<RawBlock>, u128), ClientError> {
        match self.round_trip(Request::ScanSpec {
            target_id,
            spec,
            reply_schema,
            raw: true,
        })? {
            Reply::Raw { blocks, terminal } => Ok((blocks, terminal.seek_pk)),
            _ => unreachable!("a raw scan_spec completes as Reply::Raw"),
        }
    }

    /// Ship a parameterized bounded read (`ReadSpec`) and reassemble its result.
    /// `spec` is the encoded `ReadSpec`; `reply_schema` is the schema the caller
    /// built for the result — it is encoded into the request blob (bundled with
    /// `spec` by [`gnitz_wire::pack_scan_spec_extra`], which the master forwards
    /// verbatim) and is the decode hint for every reply frame, since the server
    /// sends no schema block back. Returns one concatenated batch. Like `scan`, it
    /// advances no commit watermark and — critically — never touches the schema
    /// cache: a per-query projected schema keyed under the table id would corrupt
    /// a later plain scan of the same relation.
    /// The terminal frame's whole watermark word comes back with the rows: a
    /// delta read needs both halves of it, and every other caller drops it.
    pub fn scan_spec(
        &mut self,
        target_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<(Option<ZSetBatch>, u128), ClientError> {
        let train = self.round_trip_train(Request::ScanSpec {
            target_id,
            spec,
            reply_schema,
            raw: false,
        })?;
        Ok((train.data, train.terminal.seek_pk))
    }

    /// Push path: packs `WireConflictMode` into bits 16-23 of `wire_flags`.
    /// When the schema cache holds a valid version for `target_id` *and* the
    /// caller's `schema` type-matches the cached one, omits the schema block
    /// from the wire frame (warm path). On STATUS_SCHEMA_MISMATCH the cache
    /// entry is evicted and the push is retried with the full schema. The
    /// retry is safe because the blocking client has exactly one *live*
    /// operation, so nothing can be ordered against it.
    fn roundtrip_push(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<Message, ClientError> {
        let push = |cold| Request::Push {
            target_id,
            schema,
            batch,
            mode,
            cold,
        };
        let ack = match self.round_trip_train(push(false)) {
            Err(ClientError::SchemaMismatch) => {
                // Stale cache: evict and retry with full schema.
                self.schema_cache.pop(&target_id);
                self.round_trip_train(push(true))?
            }
            Ok(train) => train,
            Err(e) => return Err(e),
        };
        // The cache is written by the accumulator alone. Whenever the server
        // changes the schema version it also ships the schema block in the ACK
        // (`wire_should_include_schema`), so the cached entry carries the
        // server's real column names paired with the matching version. Writing
        // the caller's copy here would clobber both.
        Ok(ack.terminal)
    }

    // ── Async-shared protocol surface ──────────────────────────────────────
    //
    // Build/receive helpers the gnitz-py async I/O loop drives directly: it
    // packs a batch of requests, ships them with one `send_batch`, then reads
    // the responses back through the blocking reply path below. `send_batch`
    // takes pre-packed parts and carries none of the decode facts a slot
    // needs, so it registers none: the pending queue and the accumulator
    // stay empty on a connection driven this way, and the correlate-then-
    // absorb policy is written a second time in `recv_cached` for as long as
    // that transport exists.

    /// Pack a scan request (control-only) with the cached schema version, so
    /// the server may omit the schema block on a warm hit. The matching
    /// [`Self::recv_scan`] resolves the schema from the cache the session
    /// owns, so no hint is threaded back.
    pub fn pack_scan(&self, target_id: u64) -> MessageParts {
        let flags = self.versioned_flags(target_id, 0);
        encode_message_parts(target_id, self.client_id, flags, &PkTuple::EMPTY, 0, None)
    }

    /// Pack a point-seek request with the cached schema version.
    pub fn pack_seek(&self, target_id: u64, pk: &PkTuple) -> MessageParts {
        let flags = self.versioned_flags(target_id, FLAG_SEEK);
        encode_message_parts(target_id, self.client_id, flags, pk, 0, None)
    }

    /// Pack a SCAN_MULTI request (control-only), stamping each relation with its
    /// cached schema version. The whole self-contained frame body rides the
    /// `ctrl` segment; the matching receiver reads N `recv_scan` trains in
    /// request order. Rejects a list outside `1..=SCAN_MULTI_MAX_RELATIONS` or
    /// with a duplicate tid through the shared `encode_scan_multi_frame`, so an
    /// async driver needs no pre-check of its own.
    pub fn pack_scan_multi(&self, tids: &[u64]) -> Result<MessageParts, ClientError> {
        Ok(control_parts(self.encode_scan_multi_frame(tids)?))
    }

    /// Ship many pre-encoded frames: enqueue every part, then flush until the
    /// queue is empty. Drains the caller's vector — each `MessageParts` moves
    /// (three `Vec` headers, no payload byte copied) and the caller keeps its
    /// capacity as scratch.
    pub fn send_batch(&mut self, parts: &mut Vec<MessageParts>) -> Result<(), ProtocolError> {
        for p in parts.drain(..) {
            self.transport.enqueue(p)?;
        }
        self.transport.flush_blocking()
    }

    /// Reassemble one reply train: receive frames via `recv_one` until the
    /// terminal (non-continuation) frame, keeping the first in-frame schema and
    /// concatenating data batches. Status is checked on **every** frame — a
    /// `STATUS_ERROR` fault frame has flags 0, structurally identical to the
    /// master's terminal frame, so a flags-only check would silently drop it.
    ///
    /// The concatenated batch is the Z-set **sum** of the replies, unconsolidated:
    /// `extend_from_owned` never folds two entries sharing a `(PK, payload)`, and a
    /// non-injective server-side projection can collide two within one reply — so
    /// consumers must not assume unique `(PK, payload)`. The sum is correct because
    /// the replies partition the relation: each entry lives in one worker's store,
    /// and a replicated relation's read goes to worker 0 alone.
    fn drain_reply_train(&mut self, mut recv_one: impl FnMut(&mut Self) -> Result<Message, ClientError>) -> ScanResult {
        let mut schema: Option<Arc<Schema>> = None;
        let mut data: Option<ZSetBatch> = None;
        let watermark = loop {
            let msg = check_response(recv_one(self)?)?;
            let is_continuation = (msg.flags & FLAG_CONTINUATION) != 0;
            schema = schema.or(msg.schema);
            if let Some(batch) = msg.data_batch {
                match data.as_mut() {
                    Some(acc) => acc.extend_from_owned(batch),
                    None => data = Some(batch),
                }
            }
            if !is_continuation {
                break msg.seek_pk;
            }
        };
        Ok((schema, data, watermark as u64))
    }

    /// Receive a streaming scan/seek response: reassemble continuation frames,
    /// absorb any schema block into the cache, and recover the schema from the
    /// cache if the response was schema-less. Same policy as the sync `scan`.
    pub fn recv_scan(&mut self, target_id: u64) -> ScanResult {
        let (schema, data, watermark) = self.drain_reply_train(|s| s.recv_cached(target_id))?;
        Ok((self.schema_or_cached(target_id, schema), data, watermark))
    }

    /// Receive a single push ACK and return its ingest LSN, absorbing any schema
    /// block it carries into the cache (matching the sync push path). The status
    /// runs through the session's own `check_response`, so a pipelined push
    /// classifies server failures exactly as the sync push does rather than
    /// leaving each caller to re-derive the policy.
    pub fn recv_push_ack(&mut self, target_id: u64) -> Result<u64, ClientError> {
        let msg = self.recv_cached(target_id)?;
        check_response(msg).map(|m| m.seek_pk as u64)
    }

    /// The client's cached schema version for `tid` (`0` = no cached schema, so
    /// the server sends the block). `peek` leaves LRU recency untouched — a
    /// version probe is not an access.
    fn cached_schema_version(&self, tid: u64) -> u16 {
        self.schema_cache.peek(&tid).map(|(_, v)| *v).unwrap_or(0)
    }

    /// Build a SCAN_MULTI request frame, stamping each tid with its cached schema
    /// version. The one choke point both encode routes funnel through — `submit`
    /// and the async `pack_scan_multi` — so the wire-shape contract is enforced
    /// here, in every build profile, for every caller. The case that matters is
    /// the empty list: it would encode a count=0 frame whose lone server error
    /// frame an N=0 accumulator never consumes, permanently shifting every later
    /// read on this connection by one frame.
    fn encode_scan_multi_frame(&self, tids: &[u64]) -> Result<Vec<u8>, ClientError> {
        gnitz_wire::validate_scan_multi_tids(tids).map_err(ClientError::ServerError)?;
        let relations: Vec<(u64, u16)> = tids.iter().map(|&tid| (tid, self.cached_schema_version(tid))).collect();
        Ok(encode_scan_multi(self.client_id, &relations))
    }

    /// The cached schema version for `target_id` OR'd into the flag word, so a
    /// warm-cache request lets the server omit the schema block.
    fn versioned_flags(&self, target_id: u64, base: u64) -> u64 {
        wire_flags_set_schema_version(base, self.cached_schema_version(target_id))
    }

    /// The schema for a reply: the one the frame carried, else the cached one —
    /// a warm-cache response omits the block, so the LRU is the only source.
    fn schema_or_cached(&mut self, target_id: u64, in_frame: Option<Arc<Schema>>) -> Option<Arc<Schema>> {
        in_frame.or_else(|| self.schema_cache.get(&target_id).map(|(s, _)| Arc::clone(s)))
    }

    /// Receive one framed message *correlated to `target_id`*, using the LRU
    /// cache to decode continuation frames that arrive without a schema block,
    /// and caching any schema block the frame does carry.
    ///
    /// Every recv reaching here — scan, seek, push ACK — awaits a reply for one
    /// known relation, and the server replies to a connection strictly in request
    /// order, so the frame's own `target_id` must be that relation. Checking it
    /// makes a violation loud: the schema hint is keyed by `target_id`, so an
    /// out-of-order frame would otherwise decode under the *wrong schema* and
    /// return a wrong answer silently. The check precedes the cache absorb, so a
    /// mis-correlated block is not installed either.
    ///
    /// `STATUS_OK` only — an error frame names no relation (`target_id = 0`).
    fn recv_cached(&mut self, target_id: u64) -> Result<Message, ClientError> {
        let msg = {
            // `get` (not `peek`) so a frequently-accessed schema refreshes its
            // LRU recency and isn't evicted under memory pressure.
            let hint = self.schema_cache.get(&target_id).map(|(s, v)| (s.as_ref(), *v));
            recv_message(&mut self.transport, hint)?
        };
        if msg.status == STATUS_OK && msg.target_id != target_id {
            return Err(ClientError::Protocol(ProtocolError::DecodeError(format!(
                "reply out of order: expected target {target_id}, got {}",
                msg.target_id
            ))));
        }
        // `msg.schema` is `Some` exactly when the schema block was physically
        // in the frame. Absorb it as an `Arc` clone (refcount bump, no deep
        // copy) — this is the authoritative schema with the server's real
        // column names.
        if let Some(s) = msg.schema.as_ref() {
            let version = wire_flags_get_schema_version(msg.flags);
            self.schema_cache.put(target_id, (Arc::clone(s), version));
        }
        Ok(msg)
    }
}

/// A control-only frame as queue parts.
fn control_parts(ctrl: Vec<u8>) -> MessageParts {
    MessageParts {
        ctrl,
        schema: None,
        data: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_msg(error_text: Option<String>) -> Message {
        Message {
            status: STATUS_ERROR,
            target_id: 0,
            flags: 0,
            seek_pk: 0,
            schema: None,
            data_batch: None,
            error_text,
            seek_pk_extra: Vec::new(),
        }
    }

    // `Message` does not implement Debug, so match the Result rather than
    // calling unwrap_err (which would require the Ok variant to be Debug).
    fn server_error_text(msg: Message) -> String {
        match check_response(msg) {
            Err(ClientError::ServerError(s)) => s,
            Err(other) => panic!("expected ServerError, got {other:?}"),
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn check_response_empty_error_text_falls_back_to_default() {
        // A STATUS_ERROR with Some("") must surface the default text, not a
        // blank ServerError — the warm-push guard's rejection must be legible.
        assert_eq!(
            server_error_text(error_msg(Some(String::new()))),
            "unknown server error"
        );
    }

    #[test]
    fn check_response_none_error_text_falls_back_to_default() {
        assert_eq!(server_error_text(error_msg(None)), "unknown server error");
    }

    #[test]
    fn check_response_nonempty_error_text_preserved() {
        assert_eq!(server_error_text(error_msg(Some("real error".into()))), "real error");
    }
}

#[cfg(test)]
mod spine_tests;
