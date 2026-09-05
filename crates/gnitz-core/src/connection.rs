//! The protocol session: a sans-io connection state machine and the blocking
//! verbs written over it.
//!
//! The **spine** is [`Session::submit`] and [`Session::step`], plus what a
//! driver reads between them: [`Session::interest`], [`Session::close`], and
//! the fd through [`Session::as_raw_fd`] or [`Session::try_clone_fd`]. It is
//! the whole of the type's public surface, and nothing in it ever waits:
//! `submit` encodes a request against the connection's schema cache and
//! registers a slot; `step` does the I/O the driver says the fd is ready for
//! and reports which slots completed; waiting belongs to whoever drives it.
//!
//! Above the spine sit `Session`'s own blocking verb bodies, which park in
//! `round_trip`. They are crate-internal, so a reactor thread handed a
//! `&mut Session` cannot block in `poll(2)` on one; the host-facing blocking
//! verbs live on [`crate::GnitzClient`]. There is no second reply path.
//!
//! Replies leave the server in request order, so there is exactly one reply
//! accumulator and it belongs to the head of the pending queue: every byte
//! that arrives is the head slot's until its last train terminates.

use std::collections::VecDeque;
use std::os::fd::{OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::ClientError;
use crate::protocol::message::{encode_message_noschema_parts, encode_message_parts, MessageParts};
use crate::protocol::transport::{poll_fd, Next};
use crate::protocol::wal_block::decode_wal_block_into;
use crate::protocol::ReplySchema;
use crate::protocol::{
    encode_control_frame, encode_ddl_txn, encode_push_txn, encode_scan_multi, hello_handshake, parse_response_frame,
    wire_flags_get_schema_version, wire_flags_set_conflict_mode, wire_flags_set_schema_version, ClientTransport,
    Message, PkTuple, ProtocolError, Schema, WireConflictMode, ZSetBatch, FLAG_ALLOCATE_INDEX_ID,
    FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE, FLAG_ALLOCATE_TABLE_ID, FLAG_CONTINUATION, FLAG_PUSH,
    FLAG_RESOLVE, FLAG_SCAN_SPEC, FLAG_SEEK, FLAG_SEEK_BY_INDEX, STATUS_DELTA_EXPIRED, STATUS_ERROR, STATUS_NO_INDEX,
    STATUS_OK, STATUS_SAL_FULL, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};
use gnitz_wire::RelDescriptorBlob;
use lru::LruCache;

/// Per-connection schema LRU capacity. Sized to comfortably hold a session's
/// working set of tables/views without unbounded growth.
const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();

/// Requests one connection may hold in flight; `submit` raises past it. A bound
/// on the memory a driver that never waits can pin, not a throughput knob.
pub const MAX_IN_FLIGHT: usize = 4096;

/// Unwritten frame bytes one connection may hold; `submit` raises past it. An
/// encoded push is a full copy of its batch, so the count above bounds no
/// memory on its own. Checked before queueing, so one frame up to the peer's
/// egress limit always goes through — what it bounds is a driver that submits
/// without flushing.
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
#[derive(Debug)]
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
        RawBlock { block: 0..block.len(), frame: block }
    }
}

/// The N per-relation results of a `scan_multi`, in request order. Every
/// relation was snapshotted at the same server-side SAL cut, so an atomic
/// multi-table commit is never torn across the result set.
pub type MultiScanResult = Result<Vec<ScanReply>, ClientError>;

/// Generate a session-unique client ID.
///
/// Combines PID (top 32 bits) with a per-process monotonic sequence (bottom 32 bits).
/// The sequence is a wrapping `AtomicU32`, so ids are unique across the first
/// 2^32 connections a process opens and repeat only after it wraps; the PID half
/// makes cross-process collisions practically impossible even with PID reuse.
fn new_client_id() -> u64 {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed) as u64;
    (std::process::id() as u64) << 32 | seq
}

/// Classify a reply frame's status. Every status but `STATUS_OK` is an error,
/// including one this build does not know: the worker→master leg carries
/// whatever status a fault names, and falling through would render a refused
/// read as "no rows".
fn check_response(msg: &mut Message) -> Result<(), ClientError> {
    match msg.status {
        STATUS_OK => Ok(()),
        STATUS_SCHEMA_MISMATCH => Err(ClientError::SchemaMismatch),
        STATUS_NO_INDEX => Err(ClientError::ServerError("no index on requested column".into())),
        STATUS_DELTA_EXPIRED => Err(ClientError::DeltaExpired),
        STATUS_SAL_FULL => Err(ClientError::SalFull(msg.error_text.take().unwrap_or_default())),
        // Control-only frame: the fresh basis rides in `seek_pk`.
        STATUS_TXN_CONFLICT => Err(ClientError::TxnConflict { fresh_basis: msg.seek_pk as u64 }),
        // Fall back to the default text on an empty string, not only on None:
        // a STATUS_ERROR with Some("") would otherwise surface as a blank
        // ServerError. This matters because the warm-push guard converts
        // silent corruption into a surfaced error, which must be legible.
        STATUS_ERROR => Err(ClientError::ServerError(
            msg.error_text
                .take()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "unknown server error".into()),
        )),
        other => Err(ClientError::ServerError(format!(
            "server returned unrecognized status {other}"
        ))),
    }
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
    pub const NONE: Interest = Interest { read: false, write: false };
    pub const READ: Interest = Interest { read: true, write: false };
    pub const WRITE: Interest = Interest { read: false, write: true };
    pub const BOTH: Interest = Interest { read: true, write: true };

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
    /// An id allocation: `flag` names the sequence, `count` the run length, and
    /// the answer rides the terminal frame's `target_id`. Uncorrelated: nothing
    /// is absorbed into the cache, and it completes as [`Reply::Train`].
    Alloc { target_id: u64, flag: u64, count: u64 },
    /// An atomic DDL transaction (`FLAG_DDL_TXN`): system-table family batches
    /// the server ingests under one durable SAL zone, each named by its system
    /// table id alone. Uncorrelated.
    DdlTxn(&'a [(u64, ZSetBatch)]),
    /// An atomic user-table push transaction (`FLAG_PUSH_TXN`): families with
    /// their conflict modes, plus the OCC `(tid, basis)` preconditions the
    /// server asserts before committing any of them. Uncorrelated.
    PushTxn {
        families: &'a [(u64, &'a Schema, &'a ZSetBatch, WireConflictMode)],
        preconditions: &'a [(u64, u64)],
    },
    /// A frame the caller encoded itself, for the scripted-peer tests that
    /// drive bytes the library would never build. Test-only, so no production
    /// path reaches an encoder behind `submit`'s back. Uncorrelated.
    #[cfg(test)]
    RawFrame(Vec<u8>),
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
        reply_schema: &'a ReplySchema,
        raw: bool,
    },
    /// SEEK_BY_INDEX — a point read of a secondary index by its FULL declared
    /// column list, with native key values (fewer than columns for a
    /// leading-prefix seek). Correlated like [`Self::Read`]; its own variant
    /// because packing the key asserts contracts `submit` must check first.
    SeekByIndex {
        table_id: u64,
        col_indices: &'a [u32],
        key_vals: &'a [u128],
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
#[derive(Debug)]
pub struct ReplyTrain {
    pub terminal: Message,
    /// The block the train carried, else the one it decoded under — a warm
    /// reply omits the block.
    pub schema: Option<Arc<Schema>>,
    pub data: Option<ZSetBatch>,
}

impl ReplyTrain {
    /// The train as one relation's read result: its schema, its rows, and the
    /// terminal watermark as a plain LSN. Distinct from [`Reply::into_scan`],
    /// which projects an already-built one out of the enum.
    fn into_scan_reply(self) -> ScanReply {
        (self.schema, self.data, self.terminal.seek_pk as u64)
    }
}

/// What a slot's verb asked for. The spine resolves a reply against the request
/// that opened its slot, so no driver re-attaches a relation id.
#[derive(Debug)]
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

impl Reply {
    /// The variant's name, for [`wrong_shape`]. Not `Debug`: `Reply::Scan`
    /// reaches a whole `ZSetBatch`.
    fn kind(&self) -> &'static str {
        match self {
            Reply::Scan(_) => "Scan",
            Reply::Multi(_) => "Multi",
            Reply::Lsn(_) => "Lsn",
            Reply::Resolve(_) => "Resolve",
            Reply::Train(_) => "Train",
            Reply::Raw { .. } => "Raw",
        }
    }

    /// A PUSH ACK's ingest LSN.
    #[inline]
    #[track_caller]
    pub fn into_lsn(self) -> u64 {
        match self {
            Reply::Lsn(lsn) => lsn,
            other => wrong_shape(other.kind(), "Lsn"),
        }
    }

    /// One relation's read result: SCAN, SEEK, SEEK_BY_INDEX.
    #[inline]
    #[track_caller]
    pub fn into_scan(self) -> ScanReply {
        match self {
            Reply::Scan(r) => r,
            other => wrong_shape(other.kind(), "Scan"),
        }
    }

    /// A `scan_multi`'s N per-relation results, in request order.
    #[inline]
    #[track_caller]
    pub fn into_multi(self) -> Vec<ScanReply> {
        match self {
            Reply::Multi(r) => r,
            other => wrong_shape(other.kind(), "Multi"),
        }
    }

    /// A RESOLVE's `(live tid, schema, descriptor)`, or `None` when no such
    /// relation exists. The triple, not a `RelDescriptor`: that lives above
    /// this module.
    #[inline]
    #[track_caller]
    pub fn into_resolve(self) -> Option<(u64, Arc<Schema>, RelDescriptorBlob)> {
        match self {
            Reply::Resolve(d) => d,
            other => wrong_shape(other.kind(), "Resolve"),
        }
    }

    /// The reassembled train, for the verbs whose answer is a field of the
    /// terminal frame.
    #[inline]
    #[track_caller]
    pub fn into_train(self) -> ReplyTrain {
        match self {
            Reply::Train(t) => t,
            other => wrong_shape(other.kind(), "Train"),
        }
    }

    /// A raw `scan_spec`'s undecoded blocks and its terminal frame.
    #[inline]
    #[track_caller]
    pub fn into_raw(self) -> (Vec<RawBlock>, Message) {
        match self {
            Reply::Raw { blocks, terminal } => (blocks, terminal),
            other => wrong_shape(other.kind(), "Raw"),
        }
    }
}

#[cold]
#[inline(never)]
#[track_caller]
fn wrong_shape(got: &'static str, want: &'static str) -> ! {
    panic!(
        "the spine resolves a reply against the request that opened its slot; \
         wanted Reply::{want}, got Reply::{got}"
    )
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

impl SlotKind {
    /// Whether a schema block this slot's reply carries belongs in the cache.
    /// False for SCAN_SPEC alone: its reply schema is the caller's own
    /// projection, and keying that under the table id would corrupt a later
    /// plain scan of the same relation.
    fn absorbs_schema(&self) -> bool {
        !matches!(self, SlotKind::ScanSpec { .. })
    }

    /// Whether this slot's data blocks stay undecoded in their frame buffers.
    fn keeps_blocks_raw(&self) -> bool {
        matches!(self, SlotKind::ScanSpec { raw: true, .. })
    }
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

    /// A session over an already-established transport, for the scripted-peer
    /// tests. Keeps whatever frame ceiling the transport negotiated.
    #[cfg(test)]
    pub(crate) fn from_transport(transport: ClientTransport) -> Self {
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

    /// Install (or clear) the hook `round_trip` runs before each park.
    pub(crate) fn set_park_hook(&mut self, hook: Option<ParkHook>) {
        self.park_hook = hook;
    }

    /// Take the hook out, leaving none. What
    /// [`GnitzClient::reconnect`](crate::GnitzClient::reconnect) moves onto the
    /// session it replaces this one with — the hook is the host's, and outlives
    /// the connection it was installed on.
    pub(crate) fn take_park_hook(&mut self) -> Option<ParkHook> {
        self.park_hook.take()
    }

    // ── The spine ──────────────────────────────────────────────────────────

    /// Encode against this connection's schema cache, enqueue, register a slot.
    /// Raises past the in-flight cap. `Request` borrows its inputs; the borrow
    /// ends here, because encoding is what `submit` does.
    pub fn submit(&mut self, req: Request<'_>) -> Result<SlotId, ClientError> {
        if self.closed {
            return Err(ClientError::Closed);
        }
        // The predicate a driver reads for back-pressure, so the two can never
        // disagree; only the message re-derives which cap was hit.
        if self.at_capacity() {
            let queued = self.queued_bytes();
            return Err(ClientError::ServerError(if self.pending.len() >= MAX_IN_FLIGHT {
                format!("connection has {MAX_IN_FLIGHT} requests in flight")
            } else {
                format!("connection has {queued} unwritten bytes queued, at the {MAX_QUEUED_BYTES}-byte cap")
            }));
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
            Request::Alloc { target_id, flag, count } => {
                // The run length rides in `seek_col_idx`, the field
                // `alloc_serial_range` already uses for it.
                let ctrl = encode_control_frame(target_id, client_id, flag, 0, count, &[]);
                (control_parts(ctrl), SlotKind::Uncorrelated)
            }
            Request::DdlTxn(families) => {
                for (tid, batch) in families {
                    batch.validate(crate::types::sys_schema(*tid))?;
                }
                (
                    control_parts(encode_ddl_txn(client_id, families)),
                    SlotKind::Uncorrelated,
                )
            }
            Request::PushTxn { families, preconditions } => {
                for (_, schema, batch, _) in families {
                    batch.validate(schema)?;
                }
                (
                    control_parts(encode_push_txn(client_id, families, preconditions)),
                    SlotKind::Uncorrelated,
                )
            }
            #[cfg(test)]
            Request::RawFrame(frame) => (control_parts(frame), SlotKind::Uncorrelated),
            Request::Resolve(target) => (control_parts(self.resolve_request(target)), SlotKind::Resolve),
            Request::Push { target_id, schema, batch, mode } => {
                // In-process, so a convenience and never a trust boundary; the
                // server checks the same things. Here so no driver has to
                // remember to.
                batch.validate(schema)?;
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
            Request::ScanSpec { target_id, spec, reply_schema, raw } => {
                // The reply schema rides the request blob (the master forwards it
                // verbatim) and stays with the slot as the decode hint.
                let extra = gnitz_wire::pack_scan_spec_extra(spec, reply_schema.block());
                let ctrl = encode_control_frame(target_id, client_id, FLAG_SCAN_SPEC, 0, 0, &extra);
                (
                    control_parts(ctrl),
                    SlotKind::ScanSpec { reply_schema: reply_schema.schema(), raw },
                )
            }
            Request::SeekByIndex { table_id, col_indices, key_vals } => {
                // Both packers below assert their contracts, so the list and
                // the arity are admitted here, in every build profile.
                gnitz_wire::validate_pk_col_list(col_indices)
                    .map_err(|e| ClientError::ServerError(format!("seek_by_index: {e}")))?;
                // K rides as the wire byte count, so an empty `key_vals` reads
                // at the worker as one value `0`.
                if key_vals.is_empty() || key_vals.len() > col_indices.len() {
                    return Err(ClientError::ServerError(format!(
                        "seek_by_index: key value count {} must be in 1..={}",
                        key_vals.len(),
                        col_indices.len()
                    )));
                }
                // `split_wire` routes slot 0 → seek_pk and the rest to
                // seek_pk_extra; `unpack_index_key_slots` reassembles them.
                let (kbuf, klen) = gnitz_wire::pack_index_key_slots(key_vals);
                let key = PkTuple::from_bytes(&kbuf[..klen]);
                let (seek_pk, seek_pk_extra) = key.split_wire();
                let flags = self.versioned_flags(table_id, FLAG_SEEK_BY_INDEX);
                let ctrl = encode_control_frame(
                    table_id,
                    client_id,
                    flags,
                    seek_pk,
                    gnitz_wire::pack_pk_cols(col_indices),
                    seek_pk_extra,
                );
                (control_parts(ctrl), SlotKind::Read { tid: table_id })
            }
            Request::ScanMulti(tids) => (
                control_parts(self.encode_scan_multi_frame(tids)?),
                SlotKind::Multi { tids: tids.to_vec() },
            ),
        };
        // The caps are asymmetric — 256 MB inbound, 64 MB outbound — so a
        // bundle built from a reply this client accepted can still exceed what
        // the peer will take. Refused here, it is an error the caller can act
        // on rather than an ingress rejection and a dropped connection.
        let total: usize = parts.segments().iter().map(|s| s.len()).sum();
        let limit = self.transport.egress_limit();
        if total > limit {
            return Err(ClientError::ServerError(format!(
                "request frame is {total} bytes, exceeding the {limit}-byte server ingress cap; \
                 split the request"
            )));
        }
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

    /// Either of the caps [`Self::submit`] refuses on — in-flight slots or
    /// queued bytes — reached. A driver that can wait leaves the request where
    /// it is, which is what turns a cap into back-pressure rather than an error.
    /// Each cap implies its own interest bit, so gating on it loses no wakeup.
    pub fn at_capacity(&self) -> bool {
        self.pending.len() >= MAX_IN_FLIGHT || self.queued_bytes() >= MAX_QUEUED_BYTES
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
        // Destructured so the head slot stays borrowed for the whole function
        // while the cache and the accumulator are independent `&mut`s.
        let Session { pending, schema_cache, accum, .. } = self;
        let Some(head) = pending.front() else {
            return Err(ClientError::Protocol(ProtocolError::DecodeError(
                "reply frame with no request pending".into(),
            )));
        };
        // A `scan_multi` slot advances with its train: tids[i] for train i,
        // never the slot's first tid.
        let (correlate_tid, hint_owned) = match &head.kind {
            SlotKind::Read { tid } | SlotKind::Push { tid } => (Some(*tid), cached_hint(schema_cache, *tid)),
            SlotKind::Uncorrelated | SlotKind::Resolve => (None, None),
            SlotKind::ScanSpec { reply_schema, .. } => (None, Some((Arc::clone(reply_schema), 0))),
            SlotKind::Multi { tids } => {
                let tid = tids[accum.replies.len()];
                (Some(tid), cached_hint(schema_cache, tid))
            }
        };
        let hint = hint_owned.as_ref().map(|(s, v)| (s.as_ref(), *v));

        let mut parsed = parse_response_frame(&buf, hint)?;
        if let Err(e) = check_response(&mut parsed.message) {
            // A rejected warm stamp, and the mismatch reply carries no block to
            // refresh it with: evict, so the next push is cold.
            if let (ClientError::SchemaMismatch, SlotKind::Push { tid }) = (&e, &head.kind) {
                schema_cache.pop(tid);
            }
            complete_head(pending, accum, Err(e), done);
            return Ok(());
        }
        if let Some(tid) = correlate_tid {
            // Replies arrive in request order and the hint is keyed by
            // `target_id`, so an out-of-order frame would decode under the
            // wrong schema silently; make it loud, before any absorb.
            if parsed.message.target_id != tid {
                return Err(ClientError::Protocol(ProtocolError::DecodeError(format!(
                    "reply out of order: expected target {tid}, got {}",
                    parsed.message.target_id
                ))));
            }
        }
        // Keyed on the tid the *frame* carries, which a correlated slot just
        // asserted is its own and a by-name RESOLVE (requested id 0) reports as
        // the live one.
        if head.kind.absorbs_schema() {
            if let Some(sch) = parsed.message.schema.as_ref() {
                let version = wire_flags_get_schema_version(parsed.message.flags);
                schema_cache.put(parsed.message.target_id, (Arc::clone(sch), version));
            }
        }

        // Decoded straight into the accumulator: a train carries one data frame
        // per worker, and a per-frame batch would be copied in and dropped.
        match parsed.data_block.take() {
            Some(r) if head.kind.keeps_blocks_raw() => accum.blocks.push(RawBlock { frame: buf, block: r }),
            Some(r) => {
                let eff = parsed.effective(hint.map(|(sch, _)| sch)).ok_or_else(|| {
                    ClientError::Protocol(ProtocolError::DecodeError("no schema for data block".into()))
                })?;
                let sink = accum.data.get_or_insert_with(|| ZSetBatch::new(eff));
                decode_wal_block_into(sink, &buf[r], eff)?;
            }
            None => {}
        }

        let mut msg = parsed.message;
        let schema = msg.schema.take();
        let terminal = msg;
        accum.schema = accum.schema.take().or(schema);
        if terminal.flags & FLAG_CONTINUATION != 0 {
            return Ok(());
        }

        // The train terminated.
        let train = ReplyTrain {
            terminal,
            schema: accum.schema.take().or_else(|| hint_owned.map(|(sch, _)| sch)),
            data: accum.data.take(),
        };
        let reply = match &head.kind {
            SlotKind::Read { .. } => Ok(Reply::Scan(train.into_scan_reply())),
            SlotKind::Push { .. } => Ok(Reply::Lsn(train.terminal.seek_pk as u64)),
            SlotKind::Resolve => resolve_descriptor(train).map(Reply::Resolve),
            SlotKind::Uncorrelated => Ok(Reply::Train(train)),
            SlotKind::ScanSpec { raw: true, .. } => Ok(Reply::Raw {
                blocks: std::mem::take(&mut accum.blocks),
                terminal: train.terminal,
            }),
            SlotKind::ScanSpec { .. } => Ok(Reply::Train(train)),
            SlotKind::Multi { tids } => {
                accum.replies.push(train.into_scan_reply());
                if accum.replies.len() < tids.len() {
                    return Ok(());
                }
                Ok(Reply::Multi(std::mem::take(&mut accum.replies)))
            }
        };
        complete_head(pending, accum, reply, done);
        Ok(())
    }

    // ── The blocking client ────────────────────────────────────────────────

    /// Submit, then park on `interest()` until that slot completes, running the
    /// park hook and absorbing `EINTR`. The blocking way to drive the spine.
    ///
    /// A `step` error closes the connection: its framing can no longer be
    /// trusted. A slot abandoned by an aborting park stays pending, and the
    /// next call drains its train before its own can start.
    pub(crate) fn round_trip(&mut self, req: Request<'_>) -> Result<Reply, ClientError> {
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
            return Err(ClientError::Closed);
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
    pub(crate) fn round_trip_train(&mut self, req: Request<'_>) -> Result<ReplyTrain, ClientError> {
        self.round_trip(req).map(|r| r.into_train())
    }

    /// `round_trip` narrowed to a single relation's read result.
    pub(crate) fn round_trip_scan(&mut self, req: Request<'_>) -> ScanResult {
        self.round_trip(req).map(|r| r.into_scan())
    }

    /// An id allocation: one uncorrelated control frame, and the answer rides
    /// the terminal frame's `target_id`.
    fn alloc(&mut self, target_id: u64, flag: u64, count: u64) -> Result<u64, ClientError> {
        Ok(self
            .round_trip_train(Request::Alloc { target_id, flag, count })?
            .terminal
            .target_id)
    }

    pub(crate) fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.alloc_table_ids(1)
    }

    /// Reserve a contiguous run of `count` relation ids — one round trip and one
    /// durable sequence advance for the whole run, which is how a multi-segment
    /// view chain draws its ids. The caller owns `[base, base + count)`. The count
    /// rides in `seek_col_idx`, the field `alloc_serial_range` already uses for
    /// it, so the frame is unchanged.
    pub(crate) fn alloc_table_ids(&mut self, count: u64) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_TABLE_ID, count)
    }

    pub(crate) fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_SCHEMA_ID, 0)
    }

    pub(crate) fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        self.alloc_index_ids(1)
    }

    /// [`Self::alloc_table_ids`] for index ids — what a `CREATE TABLE` with `n`
    /// inline `UNIQUE` constraints draws in one round trip.
    pub(crate) fn alloc_index_ids(&mut self, count: u64) -> Result<u64, ClientError> {
        self.alloc(0, FLAG_ALLOCATE_INDEX_ID, count)
    }

    /// Reserve a contiguous range of `count` SERIAL ids for the sequence keyed
    /// by `seq_table_id`. Returns the range base; the caller owns
    /// `[base, base + count)`. The range `count` rides in `seek_col_idx`, and
    /// `target_id = seq_table_id ≠ 0` steers the master to the durable
    /// range-advance branch.
    pub(crate) fn alloc_serial_range(&mut self, seq_table_id: u64, count: u64) -> Result<u64, ClientError> {
        self.alloc(seq_table_id, FLAG_ALLOCATE_SERIAL_RANGE, count)
    }

    /// One push, retried once on a mismatch: `feed` evicted the cache entry, so
    /// the re-submit finds nothing warm and encodes cold. Retrying is safe only
    /// because the blocking client has exactly one *live* operation, so nothing
    /// can be ordered against it.
    pub(crate) fn push_with_mode(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        let push = || Request::Push { target_id, schema, batch, mode };
        let reply = match self.round_trip(push()) {
            Err(ClientError::SchemaMismatch) => self.round_trip(push())?,
            other => other?,
        };
        Ok(reply.into_lsn())
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
    pub(crate) fn push_ddl_txn(&mut self, families: &[(u64, ZSetBatch)]) -> Result<u64, ClientError> {
        Ok(self.round_trip_train(Request::DdlTxn(families))?.terminal.seek_pk as u64)
    }

    /// Send an atomic **user-table** push transaction (`FLAG_PUSH_TXN`): a bundle
    /// of user-table families — each carrying its conflict mode and its schema
    /// block — that the server validates as a unit under the union of the
    /// involved table locks and commits under one durable SAL zone. Returns the
    /// zone LSN (echoed in the ACK's `seek_pk`, as `push` does).
    ///
    /// The reply is received uncorrelated, exactly as `push_ddl_txn` does. Each
    /// family's batch is validated in `submit` before encoding, and `submit`
    /// bounds the frame against the server ingress cap so an oversized bundle
    /// fails locally.
    ///
    /// `preconditions` carries the OCC `(tid, basis)` assertions ("`tid` not
    /// written since `basis`"); the server rejects the whole transaction with
    /// `ClientError::TxnConflict` if any fails. Every precondition tid must be a
    /// family tid (the engine rejects otherwise). Pass an empty slice for none.
    pub(crate) fn push_txn(
        &mut self,
        families: &[(u64, &Schema, &ZSetBatch, WireConflictMode)],
        preconditions: &[(u64, u64)],
    ) -> Result<u64, ClientError> {
        Ok(self
            .round_trip_train(Request::PushTxn { families, preconditions })?
            .terminal
            .seek_pk as u64)
    }

    pub(crate) fn scan(&mut self, target_id: u64) -> ScanResult {
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
    pub(crate) fn scan_multi(&mut self, tids: &[u64]) -> MultiScanResult {
        self.round_trip(Request::ScanMulti(tids)).map(|r| r.into_multi())
    }

    pub(crate) fn seek(&mut self, target_id: u64, pk: &PkTuple) -> ScanResult {
        self.round_trip_scan(Request::seek(target_id, pk))
    }

    pub(crate) fn seek_by_index(&mut self, table_id: u64, col_indices: &[u32], key_vals: &[u128]) -> ScanResult {
        self.round_trip_scan(Request::SeekByIndex { table_id, col_indices, key_vals })
    }

    /// Describe one relation in a single round trip: `(live tid, schema,
    /// descriptor)`, or `None` when no such relation exists — a successful
    /// answer the caller renders in its own wording.
    ///
    /// The reply is received uncorrelated: a by-name resolve names no id, so
    /// the requested tid is 0 and only the frame's own `target_id` says which
    /// relation answered. `feed` absorbs the block under that, so what the cache
    /// holds is the wire schema; the foreign keys the descriptor carries are
    /// merged into the copy the caller gets. A schema block carries no FK fields
    /// at all, so every schema the server sends is FK-less already and a cold
    /// scan would overwrite a merged one anyway.
    pub(crate) fn resolve(
        &mut self,
        target: RelTarget<'_>,
    ) -> Result<Option<(u64, Arc<Schema>, RelDescriptorBlob)>, ClientError> {
        self.round_trip(Request::Resolve(target)).map(|r| r.into_resolve())
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

    /// [`Self::scan_spec`] keeping the reply's raw data blocks instead of a
    /// decoded batch; the caller decodes them itself.
    pub(crate) fn scan_spec_raw(
        &mut self,
        target_id: u64,
        spec: &[u8],
        reply_schema: &ReplySchema,
    ) -> Result<(Vec<RawBlock>, u128), ClientError> {
        let (blocks, terminal) = self
            .round_trip(Request::ScanSpec { target_id, spec, reply_schema, raw: true })?
            .into_raw();
        Ok((blocks, terminal.seek_pk))
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
    pub(crate) fn scan_spec(
        &mut self,
        target_id: u64,
        spec: &[u8],
        reply_schema: &ReplySchema,
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
        gnitz_wire::validate_scan_multi_tids(tids)?;
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
    MessageParts { ctrl, schema: None, data: Vec::new() }
}

/// The cached `(schema, version)` for `tid` as an owned decode hint.
/// `get` (not `peek`) so a relation in use refreshes its LRU recency.
fn cached_hint(cache: &mut LruCache<u64, (Arc<Schema>, u16)>, tid: u64) -> Option<(Arc<Schema>, u16)> {
    cache.get(&tid).map(|(s, v)| (Arc::clone(s), *v))
}

/// The head slot is done: report it and hand the accumulator to the next.
/// Resetting the accumulator is what an omission at either call site would get
/// wrong, and it would corrupt the *next* slot's train rather than this one's.
fn complete_head(
    pending: &mut VecDeque<Slot>,
    accum: &mut Accumulator,
    result: Result<Reply, ClientError>,
    done: &mut Completions,
) {
    let slot = pending.pop_front().expect("a frame was fed to a pending head");
    *accum = Accumulator::default();
    done.push((slot.id, result));
}

/// A RESOLVE train as `(live tid, schema, descriptor)`, or `None` when no such
/// relation exists. The descriptor's foreign keys are merged into the schema
/// here: `batch_to_schema` rebuilds every column-layout fact but leaves the FK
/// fields at 0, because a reference to *another* relation rides the descriptor.
/// `decode` bounded every `col_idx` against this schema's column count.
fn resolve_descriptor(train: ReplyTrain) -> Result<Option<(u64, Arc<Schema>, RelDescriptorBlob)>, ClientError> {
    let msg = train.terminal;
    let ncols = train.schema.as_ref().map_or(0, |s| s.columns.len());
    let Some(desc) = RelDescriptorBlob::decode(&msg.seek_pk_extra, ncols)? else {
        return Ok(None);
    };
    let mut schema = train
        .schema
        .ok_or_else(|| ClientError::ServerError("resolve reply carried no schema block".to_string()))?;
    if !desc.fks.is_empty() {
        let cols = &mut Arc::make_mut(&mut schema).columns;
        for fk in &desc.fks {
            cols[fk.col_idx as usize].fk_table_id = fk.fk_table_id;
            cols[fk.col_idx as usize].fk_col_idx = fk.fk_col_idx as u64;
        }
    }
    Ok(Some((msg.target_id, schema, desc)))
}

#[cfg(test)]
#[path = "tests/connection.rs"]
mod tests;
