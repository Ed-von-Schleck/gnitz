//! The protocol session: a sans-io connection state machine. Nothing in it
//! waits; whoever drives [`Session::step`] does.
//!
//! Replies leave the server in request order, so there is exactly one reply
//! accumulator and it belongs to the head of the pending queue: every byte
//! that arrives is the head slot's until its last train terminates.

use std::collections::VecDeque;
use std::os::fd::{OwnedFd, RawFd};
use std::sync::Arc;
use std::time::Instant;

use crate::client::DeltaCursor;
use crate::error::ClientError;
use crate::protocol::message::MessageParts;
use crate::protocol::transport::{Next, CONNECT_TIMEOUT};
use crate::protocol::wal_block::decode_wal_block_into;
use crate::protocol::ReplySchema;
use crate::protocol::{
    encode_ddl_txn, encode_frame, encode_push_txn, hello_handshake, parse_response_frame, ClientTransport, ClientVerb,
    FkTarget, Message, ProtocolError, Schema, WireConflictMode, WireFlags, WireStatus, ZSetBatch,
};
use gnitz_wire::control::ControlHeader;
use gnitz_wire::txn_frame;
use gnitz_wire::{RelClass, RelDescriptorBlob};
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

/// One relation's read result. `lsn` is the server LSN the read was served
/// at; a read answered off a mirrored copy has none.
#[derive(Debug)]
pub struct ScanReply {
    pub schema: Arc<Schema>,
    pub batch: ZSetBatch,
    pub lsn: Option<u64>,
}

/// The single-relation read result.
pub type ScanResult = Result<ScanReply, ClientError>;

/// One relation, as a RESOLVE describes it.
#[derive(Debug)]
pub struct RelDescriptor {
    pub tid: u64,
    pub class: RelClass,
    pub replicated: bool,
    pub schema: Arc<Schema>,
    pub indexes: Arc<Vec<gnitz_wire::RelIndex>>,
}

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

/// Classify a reply frame's status. Every status but `Ok` is an error; one this
/// build does not know was already refused by the control-header decode.
fn check_response(msg: &mut Message) -> Result<(), ClientError> {
    match msg.hdr.status {
        WireStatus::Ok => Ok(()),
        WireStatus::SchemaMismatch => Err(ClientError::SchemaMismatch),
        WireStatus::DeltaExpired => Err(ClientError::DeltaExpired),
        // The frame's `target_id` is the relation the request named, so the id is
        // the whole of what there is to say.
        WireStatus::NotFound => Err(ClientError::NotFound {
            noun: "relation",
            name: msg.hdr.target_id.to_string(),
        }),
        WireStatus::SalFull => Err(ClientError::SalFull(msg.error_text.take().unwrap_or_default())),
        WireStatus::TxnConflict => Err(ClientError::TxnConflict { fresh_basis: msg.hdr.arg0 }),
        // Fall back to the default text on an empty string, not only on None:
        // an `Error` with Some("") would otherwise surface as a blank
        // ServerError. This matters because the warm-push guard converts
        // silent corruption into a surfaced error, which must be legible.
        WireStatus::Error => Err(ClientError::ServerError(
            msg.error_text
                .take()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "unknown server error".into()),
        )),
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
    /// A correlated read — SCAN, SEEK: the reply's `target_id`
    /// must be this one, any schema block it carries is absorbed into the cache
    /// under it, and it completes as [`Reply::Scan`].
    Read {
        target_id: u64,
        /// The SEEK key, packed native-LE PK columns; `None` for a SCAN.
        seek: Option<&'a [u8]>,
    },
    /// An id allocation. Completes as [`Reply::Id`].
    Alloc(IdRun),
    /// An atomic DDL transaction: system-table batches, each named by its table
    /// id, under one durable SAL zone. Completes as [`Reply::Lsn`].
    DdlTxn(&'a [(u64, ZSetBatch)]),
    /// An atomic user-table push transaction, and the OCC `(tid, basis)`
    /// preconditions the server asserts first. Completes as [`Reply::Lsn`].
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
    /// every frame of the train, since the server sends none back. Uncorrelated
    /// and off the cache in both directions.
    ScanSpec {
        target_id: u64,
        spec: &'a gnitz_wire::ReadSpec,
        reply_schema: &'a ReplySchema,
    },
    /// SCAN_MULTI: N trains in request order, each decoded under its own
    /// relation.
    ScanMulti(&'a [u64]),
}

impl<'a> Request<'a> {
    /// A full-relation SCAN.
    pub fn scan(target_id: u64) -> Request<'static> {
        Request::Read { target_id, seek: None }
    }

    /// A point SEEK by primary key: `key` is the packed native-LE PK columns.
    pub fn seek(target_id: u64, key: &'a [u8]) -> Request<'a> {
        Request::Read { target_id, seek: Some(key) }
    }
}

/// A run of ids from one server-side sequence.
#[derive(Clone, Copy, Debug)]
pub enum IdRun {
    /// A run of catalog object ids.
    Ids(u64),
    /// The SERIAL sequence of `table_id`.
    Serial { table_id: u64, count: u64 },
}

/// What a slot's verb asked for. The spine resolves a reply against the request
/// that opened its slot, so no driver re-attaches a relation id.
#[derive(Debug)]
pub enum Reply {
    /// SCAN / SEEK.
    Scan(ScanReply),
    /// `scan_multi`: N per-relation results in request order.
    Multi(Vec<ScanReply>),
    /// A PUSH or transaction ACK's LSN.
    Lsn(u64),
    /// A RESOLVE: the descriptor, its schema FK-complete, or `None` when no such
    /// relation exists.
    Resolve(Option<Arc<RelDescriptor>>),
    /// An id allocation's base id.
    Id(u64),
    /// A decoded `scan_spec`: its rows under the slot's reply schema.
    Rows(ZSetBatch),
    /// A delta poll: the slot is done, and every view's blocks went to the
    /// poll's own listener as that view's terminal arrived.
    Polled,
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
            Reply::Id(_) => "Id",
            Reply::Rows(_) => "Rows",
            Reply::Polled => "Polled",
        }
    }

    /// A PUSH or transaction ACK's LSN.
    #[inline]
    #[track_caller]
    pub fn into_lsn(self) -> u64 {
        match self {
            Reply::Lsn(lsn) => lsn,
            other => wrong_shape(other.kind(), "Lsn"),
        }
    }

    /// One relation's read result: SCAN, SEEK.
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

    /// A RESOLVE's descriptor, or `None` when no such relation exists.
    #[inline]
    #[track_caller]
    pub fn into_resolve(self) -> Option<Arc<RelDescriptor>> {
        match self {
            Reply::Resolve(d) => d,
            other => wrong_shape(other.kind(), "Resolve"),
        }
    }

    /// An id allocation's base id.
    #[inline]
    #[track_caller]
    pub fn into_id(self) -> u64 {
        match self {
            Reply::Id(id) => id,
            other => wrong_shape(other.kind(), "Id"),
        }
    }

    /// A decoded `scan_spec`'s rows.
    #[inline]
    #[track_caller]
    pub fn into_rows(self) -> ZSetBatch {
        match self {
            Reply::Rows(batch) => batch,
            other => wrong_shape(other.kind(), "Rows"),
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

/// The cached `(schema, version)` a request was stamped with.
type Hint = (Arc<Schema>, u16);

/// How a slot decodes its reply and what that reply becomes, read off the
/// `Request` variant at `submit`.
enum SlotKind {
    /// A correlated read: frames name `tid`, schema blocks are absorbed under
    /// it, and a hint-only frame decodes under `hint` — the cached `(schema,
    /// version)` the request was stamped with, captured at submit so a later
    /// eviction cannot take it away.
    Read {
        tid: u64,
        hint: Option<Hint>,
    },
    /// A push: correlated the same way.
    Push {
        tid: u64,
    },
    Alloc,
    /// A DDL or push transaction ACK.
    Commit,
    Resolve,
    ScanSpec {
        reply_schema: Arc<Schema>,
    },
    /// One position per relation, each with the hint its stamp was taken from.
    Multi {
        tids: Vec<(u64, Option<Hint>)>,
    },
    /// One position per view, in request order. Its blocks stay raw, so no
    /// reply schema is needed here; the ids are what correlates each terminal —
    /// views differing only in a `WHERE` share a schema, so a misdirected block
    /// would decode cleanly.
    DeltaPoll {
        views: Vec<u64>,
    },
}

impl SlotKind {
    /// Whether a schema block this slot's reply carries belongs in the cache.
    /// False for the two client-authored verbs: their reply schema is the
    /// caller's own projection, and keying that under the table id would
    /// corrupt a later plain scan of the same relation.
    fn absorbs_schema(&self) -> bool {
        !matches!(self, SlotKind::ScanSpec { .. } | SlotKind::DeltaPoll { .. })
    }

    /// Whether this slot's data blocks stay undecoded in their frame buffers.
    fn keeps_blocks_raw(&self) -> bool {
        matches!(self, SlotKind::DeltaPoll { .. })
    }
}

struct Slot {
    id: SlotId,
    kind: SlotKind,
}

/// The train-in-progress of the head slot.
#[derive(Default)]
struct Accumulator {
    /// The latest schema block the current train carried, with its version.
    schema: Option<Hint>,
    data: Option<ZSetBatch>,
    blocks: Vec<RawBlock>,
    /// Narrowed results of a `scan_multi`.
    replies: Vec<ScanReply>,
    /// The position of a multi-position slot (`Multi`, `DeltaPoll`) this train
    /// belongs to; `0` for every other kind.
    at: usize,
}

/// One view's result, handed over as its terminal arrives — so a poll over M
/// views holds one train, not M. Positions are filled in request order, and a
/// terminal naming another view is refused, so the slot's next unanswered
/// position is the one this belongs to.
pub(crate) type PolledView = Result<(Vec<RawBlock>, DeltaCursor), ClientError>;

/// The listener a delta poll's results go to, addressed by the slot that asked
/// — so a train left behind by an abandoned poll is recognised rather than
/// matched onto a live view of the same id.
pub(crate) type PollSink<'a> = dyn FnMut(SlotId, PolledView) + 'a;

/// A protocol session: the transport plus all per-connection protocol state
/// (the schema LRU, the pending queue and reply accumulator, and
/// the warm/cold packing, continuation reassembly, cache absorption, and
/// status→error policy that read/write them). Exactly one owner of that
/// state — the sync [`crate::GnitzClient`] holds one, and so does each async
/// executor. Because the session owns the cache, no `LruCache` is threaded as
/// a parameter and no cache lock is shared across threads.
pub struct Session {
    transport: ClientTransport,
    schema_cache: LruCache<u64, (Arc<Schema>, u16)>,
    pending: VecDeque<Slot>,
    next_slot: u64,
    accum: Accumulator,
    closed: bool,
}

impl Session {
    /// `target` is an AF_UNIX socket path or a `tls://` target (see
    /// `ClientTransport::connect`). Returns the session paired with
    /// the server durability watermark from the HELLO ACK, which
    /// `GnitzClient::connect` adopts as the seed for its OCC basis.
    pub fn connect(target: &str) -> Result<(Self, u64), ClientError> {
        let until = Some(Instant::now() + CONNECT_TIMEOUT);
        let mut transport = ClientTransport::connect(target, until)?;
        // Run the HELLO handshake before any data flows. The server
        // accepts the first frame at an 8-byte limit, so this must
        // happen before a control block would be emitted.
        let published_lsn = hello_handshake(&mut transport, until)?;
        Ok((Self::over(transport), published_lsn))
    }

    fn over(transport: ClientTransport) -> Self {
        Session {
            transport,
            schema_cache: LruCache::new(SCHEMA_CACHE_CAP),
            pending: VecDeque::new(),
            next_slot: 1,
            accum: Accumulator::default(),
            closed: false,
        }
    }

    /// A session over an already-established transport, for the scripted-peer
    /// tests. Keeps whatever frame ceiling the transport negotiated.
    #[cfg(test)]
    pub(crate) fn from_transport(transport: ClientTransport) -> Self {
        Self::over(transport)
    }

    /// Requests submitted since this session connected — one per `submit`,
    /// however the bytes were batched. HELLO predates the session, uncounted.
    pub fn requests_sent(&self) -> u64 {
        self.next_slot - 1
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
        let (parts, kind) = match req {
            Request::Read { target_id, seek } => {
                let hint = self.cached_hint(target_id);
                let flags = WireFlags {
                    verb: if seek.is_some() {
                        ClientVerb::Seek
                    } else {
                        ClientVerb::Scan
                    },
                    schema_version: hint.as_ref().map_or(0, |h| h.1),
                    ..Default::default()
                };
                let hdr = ControlHeader { flags, target_id, ..Default::default() };
                let parts = encode_frame(hdr, seek.unwrap_or(&[]), None, None);
                (parts, SlotKind::Read { tid: target_id, hint })
            }
            Request::Alloc(run) => {
                let (target_id, verb, count) = match run {
                    IdRun::Ids(n) => (0, ClientVerb::AllocIds, n),
                    IdRun::Serial { table_id, count } => (table_id, ClientVerb::AllocSerialRange, count),
                };
                let hdr = ControlHeader {
                    flags: WireFlags { verb, ..Default::default() },
                    target_id,
                    arg1: count,
                    ..Default::default()
                };
                (encode_frame(hdr, &[], None, None), SlotKind::Alloc)
            }
            Request::DdlTxn(families) => {
                for (tid, batch) in families {
                    batch.validate(crate::types::sys_schema(*tid))?;
                }
                (MessageParts::single(encode_ddl_txn(families)), SlotKind::Commit)
            }
            Request::PushTxn { families, preconditions } => {
                for (_, schema, batch, _) in families {
                    batch.validate(schema)?;
                }
                (
                    MessageParts::single(encode_push_txn(families, preconditions)),
                    SlotKind::Commit,
                )
            }
            #[cfg(test)]
            Request::RawFrame(frame) => (MessageParts::single(frame), SlotKind::Commit),
            Request::Resolve(target) => (self.resolve_request(target), SlotKind::Resolve),
            Request::Push { target_id, schema, batch, mode } => {
                // In-process, so a convenience and never a trust boundary; the
                // server checks the same things. Here so no driver has to
                // remember to.
                batch.validate(schema)?;
                // The push verb marks the frame as a push independent of data
                // presence, so an empty batch (a legitimate empty Z-set delta)
                // is ACKed as a no-op push instead of being mistaken for a scan.
                let base_flags = WireFlags {
                    verb: ClientVerb::Push,
                    conflict_mode: mode,
                    ..Default::default()
                };
                // The warm path is gated on `types_match`, not the version
                // alone: a version proves the catalog has not changed, not
                // that the caller encoded under the same column types, and a
                // schema-less frame under mismatched types is reinterpreted
                // silently at rest.
                let warm_version = match self.cached_hint(target_id) {
                    Some((cached, v)) if v != 0 && schema.types_match(&cached) => Some(v),
                    _ => None,
                };
                let parts = match warm_version {
                    Some(v) => {
                        let flags = WireFlags { schema_version: v, ..base_flags };
                        let hdr = ControlHeader { flags, target_id, ..Default::default() };
                        encode_frame(hdr, &[], None, Some(batch))
                    }
                    None => {
                        let hdr = ControlHeader {
                            flags: base_flags,
                            target_id,
                            ..Default::default()
                        };
                        encode_frame(hdr, &[], Some(schema), Some(batch))
                    }
                };
                (parts, SlotKind::Push { tid: target_id })
            }
            Request::ScanSpec { target_id, spec, reply_schema } => {
                // The reply schema rides the request blob and stays with the slot
                // as the decode hint.
                let blob = spec.encode(reply_schema.block());
                let hdr = ControlHeader {
                    flags: WireFlags {
                        verb: ClientVerb::ScanSpec,
                        ..Default::default()
                    },
                    target_id,
                    ..Default::default()
                };
                let parts = encode_frame(hdr, &blob, None, None);
                (parts, SlotKind::ScanSpec { reply_schema: reply_schema.schema() })
            }
            Request::ScanMulti(tids) => {
                // Rejected here, in every build profile, before a frame exists.
                // The case that matters is the empty list: it would encode a
                // count=0 frame whose lone server error frame an N=0 accumulator
                // never consumes, permanently shifting every later read on this
                // connection by one frame.
                txn_frame::validate_item_ids("SCAN_MULTI", tids, |&tid| tid)?;
                let tids: Vec<(u64, Option<Hint>)> = tids.iter().map(|&tid| (tid, self.cached_hint(tid))).collect();
                (
                    MessageParts::single(self.encode_scan_multi_frame(&tids)),
                    SlotKind::Multi { tids },
                )
            }
        };
        self.enqueue_slot(parts, kind)
    }

    /// DELTA_POLL: one train per item, in order, delivered to the [`PollSink`] of a
    /// [`Self::step_polling`] drain — without which they are dropped, hence no [`Request`].
    pub(crate) fn submit_delta_poll(&mut self, views: &[txn_frame::DeltaPollItem<'_>]) -> Result<SlotId, ClientError> {
        txn_frame::validate_item_ids("DELTA_POLL", views, |v| v.view_id)?;
        let parts = MessageParts::single(txn_frame::encode_delta_poll(views));
        self.enqueue_slot(
            parts,
            SlotKind::DeltaPoll {
                views: views.iter().map(|v| v.view_id).collect(),
            },
        )
    }

    /// Queue an encoded request and open its slot. The caps are asymmetric —
    /// 256 MB inbound, 64 MB outbound — so a bundle built from a reply this
    /// client accepted can still exceed what the peer will take. Refused here,
    /// it is an error the caller can act on rather than an ingress rejection and
    /// a dropped connection.
    fn enqueue_slot(&mut self, parts: MessageParts, kind: SlotKind) -> Result<SlotId, ClientError> {
        let total = parts.byte_len();
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

    /// Do the I/O `ready` allows and return every slot that completed. Afterwards
    /// a driver may park on `interest()`: nothing buffered can advance, and
    /// bytes still queued are ones the fd refused.
    ///
    /// An `Err` from `step` itself means the framing is lost; `close`.
    pub fn step(&mut self, ready: Interest) -> Result<Completions, ClientError> {
        self.step_polling(ready, None)
    }

    /// [`Self::step`] for a drain that takes a delta poll's per-view results.
    pub(crate) fn step_polling(
        &mut self,
        ready: Interest,
        mut sink: Option<&mut PollSink<'_>>,
    ) -> Result<Completions, ClientError> {
        let mut done: Completions = Vec::new();
        if self.closed {
            return Ok(done);
        }
        if ready.read {
            self.transport.begin_read();
        }
        while let Next::Frame(buf) = self.transport.next_frame(ready.read)? {
            // The sink runs after `feed` has returned, so an unwind out of the
            // caller's code finds the session consistent. No sink is an
            // abandoned poll — an interrupt, or an unwind past its driver — and
            // the position is dropped, which is what its caller being gone wants.
            if let Some((slot, result)) = self.feed(buf, &mut done)? {
                if let Some(f) = sink.as_deref_mut() {
                    f(slot, result);
                }
            }
        }
        // Last, so the ciphertext a read queues goes out with this flush.
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

    /// One reply frame for the head slot. A non-OK frame ends the whole request, so
    /// status is classified before the continuation test.
    ///
    /// Returns the delta-poll position this frame filled, if it filled one.
    fn feed(&mut self, buf: Vec<u8>, done: &mut Completions) -> Result<Option<(SlotId, PolledView)>, ClientError> {
        // Destructured so the head slot stays borrowed for the whole function
        // while the cache and the accumulator are independent `&mut`s.
        let Session { pending, schema_cache, accum, .. } = self;
        let Some(head) = pending.front() else {
            return Err(ClientError::Protocol(ProtocolError::DecodeError(
                "reply frame with no request pending".into(),
            )));
        };
        // The view a DELTA_POLL slot is on and how many positions it answers,
        // else `None` — what tells the paths below to fill one position rather
        // than end the request.
        let poll = match &head.kind {
            SlotKind::DeltaPoll { views } => Some((views[accum.at], views.len())),
            _ => None,
        };
        // The relation this frame must name.
        let correlate_tid = match &head.kind {
            SlotKind::Read { tid, .. } | SlotKind::Push { tid } => Some(*tid),
            SlotKind::Multi { tids } => Some(tids[accum.at].0),
            SlotKind::DeltaPoll { .. } => poll.map(|(view, _)| view),
            SlotKind::Alloc | SlotKind::Commit | SlotKind::Resolve | SlotKind::ScanSpec { .. } => None,
        };
        // What a data block decodes under and the version a hint-only frame must carry.
        let (slot_schema, slot_version) = match &head.kind {
            SlotKind::Read { hint, .. } => hint.clone().unzip(),
            SlotKind::Multi { tids } => tids[accum.at].1.clone().unzip(),
            // Client-authored: the server sends no block and stamps 0.
            SlotKind::ScanSpec { reply_schema, .. } => (Some(Arc::clone(reply_schema)), Some(0)),
            SlotKind::DeltaPoll { .. } => (None, Some(0)),
            SlotKind::Push { .. } | SlotKind::Alloc | SlotKind::Commit | SlotKind::Resolve => (None, None),
        };
        // A block earlier in this train (the master's prelim frame) governs the
        // hint-only worker frames after it.
        let (hint_schema, version) = match &accum.schema {
            Some((s, v)) => (Some(Arc::clone(s)), Some(*v)),
            None => (slot_schema, slot_version),
        };

        let mut parsed = parse_response_frame(&buf, version)?;
        if let Err(e) = check_response(&mut parsed.message) {
            // A DELTA_POLL failure that names a view ends that view's position
            // alone; only one naming no relation fails the request.
            if let Some((view, positions)) = poll {
                if parsed.message.hdr.target_id != 0 {
                    if parsed.message.hdr.target_id != view {
                        return Err(out_of_order(view, parsed.message.hdr.target_id));
                    }
                    return Ok(Some(fill_poll_position(pending, accum, done, positions, Err(e))));
                }
            }
            // A rejected warm stamp, and the mismatch reply carries no block to
            // refresh it with: evict, so the next push is cold.
            if let (ClientError::SchemaMismatch, SlotKind::Push { tid }) = (&e, &head.kind) {
                schema_cache.pop(tid);
            }
            complete_head(pending, accum, Err(e), done);
            return Ok(None);
        }
        if let Some(tid) = correlate_tid {
            // Replies arrive in request order and the hint is keyed by
            // `target_id`, so an out-of-order frame would decode under the
            // wrong schema silently; make it loud, before any absorb.
            if parsed.message.hdr.target_id != tid {
                return Err(out_of_order(tid, parsed.message.hdr.target_id));
            }
        }
        // Keyed on the tid the *frame* carries, which a correlated slot just
        // asserted is its own and a by-name RESOLVE (requested id 0) reports as
        // the live one.
        if let Some(sch) = parsed.message.schema.as_ref() {
            let version = parsed.message.hdr.flags.schema_version;
            if head.kind.absorbs_schema() {
                schema_cache.put(parsed.message.hdr.target_id, (Arc::clone(sch), version));
            }
            accum.schema = Some((Arc::clone(sch), version));
        }

        // Decoded straight into the accumulator: a train carries one data frame
        // per worker, and a per-frame batch would be copied in and dropped.
        match parsed.data_block.take() {
            Some(r) if head.kind.keeps_blocks_raw() => accum.blocks.push(RawBlock { frame: buf, block: r }),
            Some(r) => {
                let eff = parsed.effective(hint_schema.as_deref()).ok_or_else(|| {
                    ClientError::Protocol(ProtocolError::DecodeError("no schema for data block".into()))
                })?;
                let sink = accum.data.get_or_insert_with(|| ZSetBatch::new(eff));
                decode_wal_block_into(sink, &buf[r], eff)?;
            }
            None => {}
        }

        let terminal = parsed.message;
        if terminal.hdr.flags.continuation {
            return Ok(None);
        }

        // The train terminated. These `take()`s also clear the accumulator
        // between SCAN_MULTI positions.
        let schema = accum.schema.take().map(|h| h.0).or(hint_schema);
        let data = accum.data.take();
        if let Some((_, positions)) = poll {
            let blocks = std::mem::take(&mut accum.blocks);
            let cursor = DeltaCursor {
                tag: terminal.hdr.arg1,
                tick: terminal.hdr.arg0,
            };
            let filled = Ok((blocks, cursor));
            return Ok(Some(fill_poll_position(pending, accum, done, positions, filled)));
        }
        let scan_reply = |schema: Option<Arc<Schema>>, data: Option<ZSetBatch>, terminal: &Message| {
            let schema = schema.ok_or_else(no_schema)?;
            Ok::<_, ClientError>(ScanReply {
                batch: data.unwrap_or_else(|| ZSetBatch::new(&schema)),
                schema,
                lsn: Some(terminal.hdr.arg0),
            })
        };
        let reply = match &head.kind {
            SlotKind::Read { .. } => Ok(Reply::Scan(scan_reply(schema, data, &terminal)?)),
            SlotKind::Push { .. } => Ok(Reply::Lsn(terminal.hdr.arg0)),
            SlotKind::Resolve => resolve_descriptor(terminal, schema).map(Reply::Resolve),
            SlotKind::Alloc => Ok(Reply::Id(terminal.hdr.target_id)),
            SlotKind::Commit => Ok(Reply::Lsn(terminal.hdr.arg0)),
            SlotKind::ScanSpec { reply_schema } => {
                Ok(Reply::Rows(data.unwrap_or_else(|| ZSetBatch::new(reply_schema))))
            }
            SlotKind::Multi { tids } => {
                accum.replies.push(scan_reply(schema, data, &terminal)?);
                accum.at += 1;
                if accum.at < tids.len() {
                    return Ok(None);
                }
                Ok(Reply::Multi(std::mem::take(&mut accum.replies)))
            }
            // `poll` is `Some` for exactly this kind, and that path returned.
            SlotKind::DeltaPoll { .. } => unreachable!("a DELTA_POLL position is filled before this match"),
        };
        complete_head(pending, accum, reply, done);
        Ok(None)
    }

    /// The RESOLVE request frame for `target`. The one place the wire's "name
    /// wins, else id" encoding is spelled: the name rides the blob.
    fn resolve_request(&self, target: RelTarget<'_>) -> MessageParts {
        let (target_id, qname) = match target {
            RelTarget::Name(q) => (0, q),
            RelTarget::Id(tid) => (tid, ""),
        };
        let hdr = ControlHeader {
            flags: WireFlags {
                verb: ClientVerb::Resolve,
                ..Default::default()
            },
            target_id,
            ..Default::default()
        };
        encode_frame(hdr, qname.as_bytes(), None, None)
    }

    /// The cached `(schema, version)` for `tid`. `get`, so a relation in use
    /// keeps its LRU recency; the version is what the request stamps.
    fn cached_hint(&mut self, tid: u64) -> Option<Hint> {
        self.schema_cache.get(&tid).map(|(s, v)| (Arc::clone(s), *v))
    }

    /// A SCAN_MULTI request frame, each tid stamped with the version of the
    /// hint beside it.
    fn encode_scan_multi_frame(&self, tids: &[(u64, Option<Hint>)]) -> Vec<u8> {
        let relations: Vec<(u64, u16)> = tids
            .iter()
            .map(|(tid, h)| (*tid, h.as_ref().map_or(0, |h| h.1)))
            .collect();
        txn_frame::encode_scan_multi(&relations)
    }
}

/// A read train that terminated with neither a schema block nor a hint to
/// decode under: the framing can no longer be trusted.
fn no_schema() -> ClientError {
    ClientError::Protocol(ProtocolError::DecodeError("read reply carries no schema".into()))
}

/// A reply frame naming a relation the head slot's position does not expect.
fn out_of_order(want: u64, got: u64) -> ClientError {
    ClientError::Protocol(ProtocolError::DecodeError(format!(
        "reply out of order: expected target {want}, got {got}"
    )))
}

/// Fill the delta-poll position the head slot is on, and complete the slot once
/// that was its last. Returns what the caller's sink is owed.
fn fill_poll_position(
    pending: &mut VecDeque<Slot>,
    accum: &mut Accumulator,
    done: &mut Completions,
    positions: usize,
    result: PolledView,
) -> (SlotId, PolledView) {
    let slot = pending.front().expect("a frame was fed to a pending head").id;
    // This position's train is over; the next one starts clean.
    *accum = Accumulator { at: accum.at + 1, ..Default::default() };
    if accum.at == positions {
        complete_head(pending, accum, Ok(Reply::Polled), done);
    }
    (slot, result)
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

/// A RESOLVE train, as its terminal frame and the schema block it carried, as
/// the descriptor, or `None` when no such relation exists.
/// The descriptor's foreign keys are merged into the schema here:
/// `batch_to_schema` rebuilds every column-layout fact but leaves the FK fields
/// at 0, because a reference to *another* relation rides the descriptor.
/// `decode` bounded every `col_idx` against this schema's column count.
fn resolve_descriptor(msg: Message, schema: Option<Arc<Schema>>) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
    let ncols = schema.as_ref().map_or(0, |s| s.columns.len());
    let Some(desc) = RelDescriptorBlob::decode(&msg.blob, ncols)? else {
        return Ok(None);
    };
    let mut schema =
        schema.ok_or_else(|| ClientError::ServerError("resolve reply carried no schema block".to_string()))?;
    if !desc.fks.is_empty() {
        let cols = &mut Arc::make_mut(&mut schema).columns;
        for fk in &desc.fks {
            cols[fk.col_idx as usize].fk = Some(FkTarget::Table { id: fk.fk_table_id, col: fk.fk_col_idx });
        }
    }
    Ok(Some(Arc::new(RelDescriptor {
        tid: msg.hdr.target_id,
        class: desc.class,
        replicated: desc.replicated,
        schema,
        indexes: Arc::new(desc.indexes),
    })))
}

#[cfg(test)]
#[path = "tests/connection.rs"]
mod tests;
