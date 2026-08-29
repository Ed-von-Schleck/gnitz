//! The protocol session: a sans-io connection state machine and the blocking
//! verbs written over it.
//!
//! The **spine** is [`Session::submit`] and [`Session::step`], plus what a
//! driver reads between them: [`Session::interest`], [`Session::close`], and
//! the fd through [`Session::as_raw_fd`] or [`Session::try_clone_fd`].
//! "Nothing in it ever waits" is a property of those methods, not of the type.
//! `submit` encodes a request against the connection's schema cache and
//! registers a slot; `step` does the I/O the driver says the fd is ready for
//! and reports which slots completed; waiting belongs to whoever drives it.
//!
//! Above the spine sit `Session`'s own blocking verb bodies, which park in
//! [`Session::round_trip`]; the tokio driver and the asyncio executor drive the
//! same methods and do their own waiting. There is no second reply path.
//!
//! Replies leave the server in request order, so there is exactly one reply
//! accumulator and it belongs to the head of the pending queue: every byte
//! that arrives is the head slot's until its last train terminates.

use std::collections::VecDeque;
use std::os::fd::{OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::ClientError;
use crate::protocol::codec::encode_schema_block;
use crate::protocol::message::{encode_message_noschema_parts, encode_message_parts, MessageParts};
use crate::protocol::transport::{poll_fd, Next};
use crate::protocol::{
    encode_control_frame, encode_ddl_txn, encode_push_txn, encode_scan_multi, hello_handshake, parse_response,
    parse_response_frame, wire_flags_get_schema_version, wire_flags_set_conflict_mode, wire_flags_set_schema_version,
    ClientTransport, Message, PkTuple, ProtocolError, Schema, WireConflictMode, ZSetBatch, FLAG_ALLOCATE_INDEX_ID,
    FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE, FLAG_ALLOCATE_TABLE_ID, FLAG_CONTINUATION, FLAG_PUSH,
    FLAG_RESOLVE, FLAG_SCAN_SPEC, FLAG_SEEK, FLAG_SEEK_BY_INDEX, STATUS_DELTA_EXPIRED, STATUS_ERROR, STATUS_NO_INDEX,
    STATUS_OK, STATUS_SAL_FULL, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};
use gnitz_wire::RelDescriptorBlob;
use lru::LruCache;

pub use gnitz_wire::{
    COL_TAB, FIRST_USER_SCHEMA_ID, FIRST_USER_TABLE_ID, IDX_TAB, SCHEMA_TAB, SEQ_TAB, TABLE_TAB, VIEW_TAB,
};

/// Per-connection schema LRU capacity. Sized to comfortably hold a session's
/// working set of tables/views without unbounded growth.
const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();

/// Requests one connection may hold in flight; `submit` raises past it. A bound
/// on the memory a driver that never waits can pin, not a throughput knob.
pub const MAX_IN_FLIGHT: usize = 4096;

/// Unwritten frame bytes one connection may hold; `submit` raises past it. An
/// encoded push is a full copy of its batch, so the count above bounds no
/// memory on its own. Checked before queueing, so one frame of any size always
/// goes through — what it bounds is a driver that submits without flushing.
pub const MAX_QUEUED_BYTES: usize = 64 << 20;

/// One relation's reply to a `scan`/`seek`/`seek_by_index`: the (cached)
/// `Schema`, the materialised `ZSetBatch` if any rows came back, and the server
/// LSN at which the read was served.
pub type ScanReply = (Option<Arc<Schema>>, Option<ZSetBatch>, u64);

/// The single-relation read result.
pub type ScanResult = Result<ScanReply, ClientError>;

/// A [`ScanReply`] whose served LSN may be absent: a mirrored copy answers at a
/// feed round rather than a server-side counter, so a locally answered read
/// carries none. What the `_local_first` reads hand back.
pub type LocalScanReply = (Option<Arc<Schema>>, Option<ZSetBatch>, Option<u64>);

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

    /// A block that owns its whole buffer, for a producer that is not a reply
    /// frame. [`MirrorStore::ingest`](crate::MirrorStore::ingest) takes these, so
    /// a store's own tests need a way to build one.
    pub fn from_block(block: Vec<u8>) -> RawBlock {
        RawBlock {
            block: 0..block.len(),
            frame: block,
        }
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
    if msg.status == STATUS_SAL_FULL {
        // Transient by construction: a reclaim frees the log within one watchdog
        // tick, so a caller can retry. Structured for exactly that reason — the
        // text is a message, not a contract.
        return Err(ClientError::SalFull(msg.error_text.unwrap_or_default()));
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
pub enum RelTarget<'a> {
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
    /// A correlated read — SCAN, SEEK, SEEK_BY_INDEX: the reply's `target_id`
    /// must be this one, any schema block it carries is absorbed into the cache
    /// under it, and it completes as [`Reply::Scan`].
    Read {
        target_id: u64,
        flags: u64,
        seek_pk: u128,
        seek_col_idx: u64,
        seek_pk_extra: &'a [u8],
    },
    /// A frame its producer already encoded — the id allocations and the two
    /// transaction frames. Uncorrelated: nothing is absorbed into the cache,
    /// and it completes as [`Reply::Train`].
    Uncorrelated(Vec<u8>),
    /// RESOLVE — describe one relation. Uncorrelated on the wire, because a
    /// by-name resolve names no id, so nothing is absorbed under the *requested*
    /// target; the reply installs the schema under the live id it carries.
    Resolve(RelTarget<'a>),
    /// PUSH — correlated like `Read`, and the one variant that chooses the
    /// warm (schema-less) encoding against the cold one. The choice is the
    /// cache's alone: a mismatch evicts the entry in `feed`, so a re-submit
    /// finds nothing warm and encodes cold with no flag to carry it.
    Push {
        target_id: u64,
        schema: &'a Schema,
        batch: &'a ZSetBatch,
        mode: WireConflictMode,
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

impl<'a> Request<'a> {
    /// A full-relation SCAN.
    pub fn scan(target_id: u64) -> Request<'static> {
        Request::Read {
            target_id,
            flags: 0,
            seek_pk: 0,
            seek_col_idx: 0,
            seek_pk_extra: &[],
        }
    }

    /// A point SEEK by primary key. `split_wire` routes the key's low bytes to
    /// `seek_pk` and any overflow to `seek_pk_extra`, so no driver spells the
    /// split itself.
    pub fn seek(target_id: u64, pk: &'a PkTuple) -> Request<'a> {
        let (seek_pk, seek_pk_extra) = pk.split_wire();
        Request::Read {
            target_id,
            flags: FLAG_SEEK,
            seek_pk,
            seek_col_idx: 0,
            seek_pk_extra,
        }
    }
}

/// One reassembled train: every frame's rows concatenated into `data`, the
/// schema they decoded under, and the terminal frame whole — `target_id` and
/// `seek_pk` are where the answers of [`Reply::Train`]'s verbs live.
pub struct ReplyTrain {
    pub terminal: Message,
    /// The block the train carried, else the one it decoded under — a warm
    /// reply omits the block.
    pub schema: Option<Arc<Schema>>,
    pub data: Option<ZSetBatch>,
}

impl ReplyTrain {
    /// The train as one relation's read result: its schema, its rows, and the
    /// terminal watermark as a plain LSN.
    fn into_scan(self) -> ScanReply {
        (self.schema, self.data, self.terminal.seek_pk as u64)
    }
}

/// What a slot's verb asked for. The spine resolves a reply against the request
/// that opened its slot, so no driver re-attaches a relation id.
pub enum Reply {
    /// SCAN / SEEK / SEEK_BY_INDEX.
    Scan(ScanReply),
    /// `scan_multi`: N per-relation results in request order.
    Multi(Vec<ScanReply>),
    /// A PUSH ACK's ingest LSN.
    Lsn(u64),
    /// A RESOLVE: the live id, the FK-complete schema and the descriptor, or
    /// `None` when no such relation exists.
    Resolve(Option<(u64, Arc<Schema>, RelDescriptorBlob)>),
    /// The train itself, for the verbs whose answer is a field of the terminal
    /// frame: the id allocations, the transaction ACKs, and `scan_spec`.
    Train(ReplyTrain),
    /// The same train with each frame's data block left undecoded in its own
    /// frame buffer: `scan_spec_raw`, and the mirror's copy-free ingest. It
    /// carries no schema — the server sends no block back for a SCAN_SPEC.
    Raw { blocks: Vec<RawBlock>, terminal: Message },
}

pub type Completions = Vec<(SlotId, Result<Reply, ClientError>)>;

/// How a slot decodes its reply and what that reply becomes, read off the
/// `Request` variant at `submit`.
enum SlotKind {
    /// A correlated read: frames decode under `tid`, and schema blocks are
    /// absorbed under it.
    Read {
        tid: u64,
    },
    /// A push: correlated the same way.
    Push {
        tid: u64,
    },
    Uncorrelated,
    Resolve,
    ScanSpec {
        reply_schema: Arc<Schema>,
        raw: bool,
    },
    Multi {
        tids: Vec<u64>,
    },
}

/// Which [`Reply`] the head slot's terminating train becomes, projected from
/// the slot's kind. `feed` reads the kind once and carries this instead.
enum ReplyShape {
    Scan,
    PushAck,
    Resolve,
    Train,
    /// `scan_spec_raw`: the data blocks stay undecoded.
    Raw,
    /// `scan_multi`, whose slot wants this many trains in all.
    Multi(usize),
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
    /// Narrowed results of a `scan_multi`; the next train's index is
    /// `replies.len()`.
    replies: Vec<ScanReply>,
}

/// A protocol session: the transport plus all per-connection protocol state
/// (client id, the schema LRU, the pending queue and reply accumulator, and
/// the warm/cold packing, continuation reassembly, cache absorption, and
/// status→error policy that read/write them). Exactly one owner of that
/// state — the sync [`crate::GnitzClient`] holds one, and so does each async
/// executor. Because the session owns the cache, no `LruCache` is threaded as
/// a parameter and no cache lock is shared across threads.
pub struct Session {
    transport: ClientTransport,
    pub client_id: u64,
    schema_cache: LruCache<u64, (Arc<Schema>, u16)>,
    pending: VecDeque<Slot>,
    next_slot: u64,
    accum: Accumulator,
    closed: bool,
    /// Run before every park of the blocking client; its `Err` aborts the
    /// operation. `Send + Sync` so the field cannot silently narrow `Session`'s
    /// own auto-traits.
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

    /// The fd a driver polls. Borrowed: it lives exactly as long as the
    /// session, so a reactor that outlives one call wants
    /// [`Self::try_clone_fd`] instead.
    pub fn as_raw_fd(&self) -> RawFd {
        self.transport.as_raw_fd()
    }

    /// An owned `dup` of the connection's socket, for a reactor to register and
    /// drop on its own schedule. It shares the open file description, so it
    /// reports the same readiness, and closing it leaves the connection open.
    pub fn try_clone_fd(&self) -> Result<OwnedFd, ClientError> {
        Ok(self.transport.try_clone_fd()?)
    }

    #[cfg(test)]
    pub(crate) fn queue_is_empty(&self) -> bool {
        self.transport.queue_is_empty()
    }

    /// Install (or clear) the hook `round_trip` runs before each park.
    pub fn set_park_hook(&mut self, hook: Option<ParkHook>) {
        self.park_hook = hook;
    }

    /// Take the hook out, leaving none. What
    /// [`GnitzClient::reconnect`](crate::GnitzClient::reconnect) moves onto the
    /// session it replaces this one with — the hook is the host's, and outlives
    /// the connection it was installed on.
    pub fn take_park_hook(&mut self) -> Option<ParkHook> {
        self.park_hook.take()
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
        let queued = self.transport.queued_bytes();
        if queued >= MAX_QUEUED_BYTES {
            return Err(ClientError::ServerError(format!(
                "connection has {queued} unwritten bytes queued, at the {MAX_QUEUED_BYTES}-byte cap"
            )));
        }
        let client_id = self.client_id;
        let (parts, kind) = match req {
            Request::Read {
                target_id,
                flags,
                seek_pk,
                seek_col_idx,
                seek_pk_extra,
            } => {
                let flags = self.versioned_flags(target_id, flags);
                let ctrl = encode_control_frame(target_id, client_id, flags, seek_pk, seek_col_idx, seek_pk_extra);
                (control_parts(ctrl), SlotKind::Read { tid: target_id })
            }
            Request::Uncorrelated(frame) => (control_parts(frame), SlotKind::Uncorrelated),
            Request::Resolve(target) => (control_parts(self.resolve_request(target)), SlotKind::Resolve),
            Request::Push {
                target_id,
                schema,
                batch,
                mode,
            } => {
                // In-process, so a convenience and never a trust boundary; the
                // server checks the same things. Here so no driver has to
                // remember to.
                batch.validate(schema).map_err(ClientError::ServerError)?;
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
                    Some((cached, v)) if *v != 0 && schema.types_match(cached.as_ref()) => Some(*v),
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
                (parts, SlotKind::Push { tid: target_id })
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
        if ready.read {
            self.transport.begin_read();
        }
        while let Next::Frame(buf) = self.transport.next_frame(ready.read)? {
            self.feed(buf, &mut done)?;
        }
        // Last, so that bytes still queued when this returns are ones the fd
        // refused — a read of its own can queue ciphertext, and flushing before
        // it would leave that behind and make the two indistinguishable.
        if ready.write {
            self.transport.flush()?;
        }
        Ok(done)
    }

    /// Frame bytes queued and not yet written, against [`MAX_QUEUED_BYTES`].
    pub fn queued_bytes(&self) -> usize {
        self.transport.queued_bytes()
    }

    /// `READ` while any slot is outstanding; `WRITE` while bytes remain queued
    /// or rustls has ciphertext to ship.
    pub fn interest(&self) -> Interest {
        Interest {
            read: !self.pending.is_empty(),
            write: self.transport.wants_write(),
        }
    }

    /// Abandon every pending slot and refuse further work; the outbound queue
    /// and the accumulator go with them, so `interest()` reports nothing
    /// afterwards. A driver fails its own outstanding futures — it registered
    /// them, so it already knows which they are.
    pub fn close(&mut self) {
        self.closed = true;
        self.transport.clear_queue();
        self.accum = Accumulator::default();
        self.pending.clear();
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
        // Read the head slot's kind once; everything below works off these.
        // A `scan_multi` slot advances with its train: tids[i] for train i,
        // never the slot's first tid.
        let (correlate_tid, hint_owned, shape) = match &head.kind {
            SlotKind::Read { tid } => (Some(*tid), self.cached_hint(*tid), ReplyShape::Scan),
            SlotKind::Push { tid } => (Some(*tid), self.cached_hint(*tid), ReplyShape::PushAck),
            SlotKind::Uncorrelated => (None, None, ReplyShape::Train),
            SlotKind::Resolve => (None, None, ReplyShape::Resolve),
            SlotKind::ScanSpec { reply_schema, raw } => (
                None,
                Some((Arc::clone(reply_schema), 0)),
                if *raw { ReplyShape::Raw } else { ReplyShape::Train },
            ),
            SlotKind::Multi { tids } => {
                let (tid, n) = (tids[self.accum.replies.len()], tids.len());
                (Some(tid), self.cached_hint(tid), ReplyShape::Multi(n))
            }
        };
        let hint = hint_owned.as_ref().map(|(s, v)| (s.as_ref(), *v));

        let (msg, block) = if matches!(shape, ReplyShape::Raw) {
            let parsed = parse_response_frame(&buf, hint)?;
            (parsed.message, parsed.data_block)
        } else {
            (parse_response(&buf, hint)?, None)
        };
        let mut msg = match check_response(msg) {
            Ok(m) => m,
            Err(e) => {
                // A rejected warm stamp, and the mismatch reply carries no
                // block to refresh it with: evict, so the next push is cold.
                if let (ClientError::SchemaMismatch, ReplyShape::PushAck, Some(tid)) = (&e, &shape, correlate_tid) {
                    self.schema_cache.pop(&tid);
                }
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
            schema: acc.schema.take().or_else(|| hint_owned.map(|(s, _)| s)),
            data: acc.data.take(),
        };
        let reply = match shape {
            ReplyShape::Scan => Ok(Reply::Scan(train.into_scan())),
            ReplyShape::PushAck => Ok(Reply::Lsn(train.terminal.seek_pk as u64)),
            ReplyShape::Resolve => self.resolve_reply(train).map(Reply::Resolve),
            ReplyShape::Train => Ok(Reply::Train(train)),
            ReplyShape::Raw => Ok(Reply::Raw {
                blocks: std::mem::take(&mut self.accum.blocks),
                terminal: train.terminal,
            }),
            ReplyShape::Multi(n) => {
                self.accum.replies.push(train.into_scan());
                if self.accum.replies.len() < n {
                    return Ok(());
                }
                Ok(Reply::Multi(std::mem::take(&mut self.accum.replies)))
            }
        };
        self.complete_head(reply, done);
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

    /// Submit, then park on `interest()` until that slot completes, running the
    /// park hook and absorbing `EINTR`. The blocking way to drive the spine.
    ///
    /// A `step` error closes the connection: its framing can no longer be
    /// trusted. A slot abandoned by an aborting park stays pending, and the
    /// next call drains its train before its own can start.
    pub fn round_trip(&mut self, req: Request<'_>) -> Result<Reply, ClientError> {
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

    /// `round_trip` narrowed to a single relation's read result.
    fn round_trip_scan(&mut self, req: Request<'_>) -> ScanResult {
        match self.round_trip(req)? {
            Reply::Scan(r) => Ok(r),
            _ => unreachable!("a correlated read completes as Reply::Scan"),
        }
    }

    /// An id allocation: one uncorrelated control frame, and the answer rides
    /// the terminal frame's `target_id`.
    fn alloc(&mut self, target_id: u64, flag: u64, seek_col_idx: u64) -> Result<u64, ClientError> {
        let frame = encode_control_frame(target_id, self.client_id, flag, 0, seek_col_idx, &[]);
        Ok(self.round_trip_train(Request::Uncorrelated(frame))?.terminal.target_id)
    }

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.alloc_table_ids(1)
    }

    /// Reserve a contiguous run of `count` relation ids — one round trip and one
    /// durable sequence advance for the whole run, which is how a multi-segment
    /// view chain draws its ids. The caller owns `[base, base + count)`. The count
    /// rides in `seek_col_idx`, the field `alloc_serial_range` already uses for
    /// it, so the frame is unchanged.
    pub fn alloc_table_ids(&mut self, count: u64) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_TABLE_ID, count)
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_SCHEMA_ID, 0)
    }

    pub fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        self.alloc_index_ids(1)
    }

    /// [`Self::alloc_table_ids`] for index ids — what a `CREATE TABLE` with `n`
    /// inline `UNIQUE` constraints draws in one round trip.
    pub fn alloc_index_ids(&mut self, count: u64) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_INDEX_ID, count)
    }

    /// Reserve a contiguous range of `count` SERIAL ids for the sequence keyed
    /// by `seq_table_id`. Returns the range base; the caller owns
    /// `[base, base + count)`. The range `count` rides in `seek_col_idx`, and
    /// `target_id = seq_table_id ≠ 0` steers the master to the durable
    /// range-advance branch.
    pub fn alloc_serial_range(&mut self, seq_table_id: u64, count: u64) -> Result<u64, ClientError> {
        self.alloc(seq_table_id, FLAG_ALLOCATE_SERIAL_RANGE, count)
    }

    /// One push, retried once on a mismatch: `feed` evicted the cache entry, so
    /// the re-submit finds nothing warm and encodes cold. Retrying is safe only
    /// because the blocking client has exactly one *live* operation, so nothing
    /// can be ordered against it.
    pub fn push_with_mode(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        let push = || Request::Push {
            target_id,
            schema,
            batch,
            mode,
        };
        let reply = match self.round_trip(push()) {
            Err(ClientError::SchemaMismatch) => self.round_trip(push())?,
            other => other?,
        };
        match reply {
            Reply::Lsn(lsn) => Ok(lsn),
            _ => unreachable!("a push completes as Reply::Lsn"),
        }
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
    ///
    /// The bounds check is here rather than at either caller because the caps are
    /// asymmetric — 256 MB inbound, 64 MB outbound — so a bundle derived from a
    /// reply the client legitimately accepted can still exceed what the server
    /// will take. Without it that is an ingress rejection and a dropped
    /// connection rather than an error the caller can act on.
    fn send_txn_frame(&mut self, payload: Vec<u8>) -> Result<u64, ClientError> {
        if payload.len() > gnitz_wire::MAX_FRAME_PAYLOAD_SERVER {
            return Err(ClientError::ServerError(format!(
                "transaction frame is {} bytes, exceeding the {}-byte server ingress cap; split the transaction",
                payload.len(),
                gnitz_wire::MAX_FRAME_PAYLOAD_SERVER
            )));
        }
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
    /// family's batch is validated client-side before encoding, and
    /// [`Self::send_txn_frame`] bounds-checks the frame against the server ingress
    /// cap so an oversized bundle fails locally.
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
        self.send_txn_frame(payload)
    }

    pub fn scan(&mut self, target_id: u64) -> ScanResult {
        self.round_trip_scan(Request::scan(target_id))
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
        match self.round_trip(Request::ScanMulti(tids))? {
            Reply::Multi(replies) => Ok(replies),
            _ => unreachable!("a scan_multi completes as Reply::Multi"),
        }
    }

    pub fn seek(&mut self, target_id: u64, pk: &PkTuple) -> ScanResult {
        self.round_trip_scan(Request::seek(target_id, pk))
    }

    pub fn seek_by_index(&mut self, table_id: u64, col_indices: &[u32], key_vals: &[u128]) -> ScanResult {
        // `split_wire` routes slot 0 → seek_pk and slots 1..K → seek_pk_extra,
        // where the worker reassembles them with `unpack_index_key_slots`.
        // Arity is validated upstream in `GnitzClient::seek_by_index`.
        let (buf, len) = gnitz_wire::pack_index_key_slots(key_vals);
        let key = PkTuple::from_bytes(&buf[..len]);
        let (seek_pk, seek_pk_extra) = key.split_wire();
        self.round_trip_scan(Request::Read {
            target_id: table_id,
            flags: FLAG_SEEK_BY_INDEX,
            seek_pk,
            seek_col_idx: gnitz_wire::pack_pk_cols(col_indices),
            seek_pk_extra,
        })
    }

    /// Describe one relation in a single round trip: `(live tid, schema,
    /// descriptor)`, or `None` when no such relation exists — a successful
    /// answer the caller renders in its own wording.
    ///
    /// The descriptor's foreign keys are merged into the schema by
    /// [`Self::resolve_reply`], while the `Arc` is still unique, so the block
    /// installed in `schema_cache` is the same FK-complete schema the caller
    /// gets rather than a second copy of it.
    ///
    /// The reply is received uncorrelated: it never carries data, and a
    /// correlated slot would absorb the block under the *requested* tid, which
    /// for a by-name resolve is 0.
    pub(crate) fn resolve(
        &mut self,
        target: RelTarget<'_>,
    ) -> Result<Option<(u64, Arc<Schema>, RelDescriptorBlob)>, ClientError> {
        match self.round_trip(Request::Resolve(target))? {
            Reply::Resolve(d) => Ok(d),
            _ => unreachable!("a resolve completes as Reply::Resolve"),
        }
    }

    /// The RESOLVE request frame for `target`. The one place the wire's "name
    /// wins, else id" encoding is spelled: the name rides an explicit extra
    /// blob rather than a `PkTuple`, whose `split_wire` would silently truncate
    /// it past `MAX_PK_BYTES`.
    fn resolve_request(&self, target: RelTarget<'_>) -> Vec<u8> {
        let (target_id, qname) = match target {
            RelTarget::Name(q) => (0, q),
            RelTarget::Id(tid) => (tid, ""),
        };
        encode_control_frame(target_id, self.client_id, FLAG_RESOLVE, 0, 0, qname.as_bytes())
    }

    /// The matching reply: `(live tid, schema, descriptor)`, or `None` when no
    /// such relation exists. The slot is uncorrelated, so installing the block
    /// under the **live** tid — which is what puts a following `scan`/`push` on
    /// its warm path — happens here rather than in `feed`.
    fn resolve_reply(
        &mut self,
        train: ReplyTrain,
    ) -> Result<Option<(u64, Arc<Schema>, RelDescriptorBlob)>, ClientError> {
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

    /// The client's cached schema version for `tid` (`0` = no cached schema, so
    /// the server sends the block). `peek` leaves LRU recency untouched — a
    /// version probe is not an access.
    fn cached_schema_version(&self, tid: u64) -> u16 {
        self.schema_cache.peek(&tid).map(|(_, v)| *v).unwrap_or(0)
    }

    /// Build a SCAN_MULTI request frame, stamping each tid with its cached schema
    /// version. Rejects the list here, in every build profile, before a frame
    /// exists. The case that matters is the empty list: it would encode a
    /// count=0 frame whose lone server error frame an N=0 accumulator never
    /// consumes, permanently shifting every later read on this connection by
    /// one frame.
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
#[path = "tests/connection.rs"]
mod tests;
