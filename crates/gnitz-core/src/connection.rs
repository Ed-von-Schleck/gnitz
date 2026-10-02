//! The protocol session: a sans-io connection state machine. Nothing in it
//! waits; whoever drives [`Session::step`] does.
//!
//! Replies leave the server in request order, so there is exactly one reply
//! accumulator and it belongs to the head of the pending queue: every byte
//! that arrives is the head slot's until its last train terminates.

use gnitz_expr::SchemaFacts;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::os::fd::{OwnedFd, RawFd};
use std::sync::Arc;
use std::time::Instant;

use crate::error::ClientError;
use crate::protocol::wal_block::decode_wal_block_into;
use crate::{
    encode_ddl_txn, encode_frame, encode_push_txn, sys_schema, ClientTransport, ProtocolError, PushFamily, Schema,
    ZSetBatch,
};
use gnitz_wire::control::{peek_control_block, ControlHeader, DecodedControl};
use gnitz_wire::txn_frame;
use gnitz_wire::CONNECT_TIMEOUT;
use gnitz_wire::{ClientVerb, WireConflictMode, WireFlags};
use gnitz_wire::{RelClass, RelDescriptorBlob, RelIndex, WireFault, WireStatus};

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

/// One relation, as a RESOLVE describes it.
#[derive(Debug)]
pub struct RelDescriptor {
    pub tid: u64,
    pub class: RelClass,
    /// A stream, or a view whose planner set [`gnitz_wire::sys_rows::ViewTabRow::pk_repeats`].
    pub pk_repeats: bool,
    /// [`gnitz_wire::TableProps::serial`].
    pub serial: bool,
    pub schema: Arc<Schema>,
    pub indexes: Vec<RelIndex>,
}

/// One reply frame's data block, kept undecoded: the owned frame buffer and the
/// block's extent within it. `block()` is the block itself.
#[derive(Debug)]
pub(crate) struct RawBlock {
    frame: Vec<u8>,
    block: std::ops::Range<usize>,
}

impl RawBlock {
    /// The data block's bytes, ready to decode against the reply schema.
    pub(crate) fn block(&self) -> &[u8] {
        &self.frame[self.block.clone()]
    }
}

/// A delta-feed position, held by the client alone: the boot and relation its
/// rounds belong to (`tag`), and the last round it covers (`tick`).
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct DeltaCursor {
    pub tag: u64,
    pub tick: NonZeroU64,
}

impl DeltaCursor {
    /// The cursor a flat `(tag, tick)` pair spells; tick 0 spells none.
    pub fn from_pair(tag: u64, tick: u64) -> Option<DeltaCursor> {
        NonZeroU64::new(tick).map(|tick| DeltaCursor { tag, tick })
    }

    /// This cursor as a flat `(tag, tick)` pair.
    pub fn pair(self) -> (u64, u64) {
        (self.tag, self.tick.get())
    }

    /// `next` as this cursor's successor, or a `DeltaExpired` refusal when it
    /// names another boot or relation.
    pub(crate) fn advanced_to(self, next: DeltaCursor) -> Result<DeltaCursor, ClientError> {
        (self.tag == next.tag)
            .then_some(next)
            .ok_or_else(|| delta_expired("delta cursor's tag names a different boot or relation; bootstrap"))
    }
}

/// A `WireStatus::DeltaExpired` refusal raised on this side.
fn delta_expired(text: &str) -> ClientError {
    ClientError::Refused(WireFault {
        status: WireStatus::DeltaExpired,
        text: text.into(),
    })
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
/// [`Request::encode`].
pub enum Request<'a> {
    /// An id allocation. Completes as [`Reply::Id`].
    Alloc(IdRun),
    /// An atomic DDL transaction: system-table batches, each named by its table
    /// id, under one durable SAL zone. Completes as [`Reply::Lsn`].
    DdlTxn(&'a [(u64, ZSetBatch)]),
    /// An atomic user-table push transaction: the server refuses it if some
    /// family's relation was written after that family's `basis`. Completes as
    /// [`Reply::Lsn`].
    PushTxn { families: &'a [PushFamily<'a>] },
    /// RESOLVE — describe the relation named by the canonical
    /// `"schema_name.relation_name"`. Uncorrelated on the wire, because the
    /// request names no id.
    Resolve(&'a str),
    /// PUSH. The frame always carries `schema`'s record.
    Push {
        target_id: u64,
        schema: &'a Schema,
        batch: &'a ZSetBatch,
        mode: WireConflictMode,
    },
    /// SCAN_SPEC, replied in `reply_schema`'s layout.
    ScanSpec {
        target_id: u64,
        spec: &'a gnitz_wire::ReadSpec,
        reply_schema: &'a Arc<Schema>,
    },
    /// SCAN_MULTI: every row of N relations at one cut, each replied in the
    /// layout of the schema paired with it.
    ScanMulti(Vec<(u64, Arc<Schema>)>),
}

/// A [`Request`] validated and encoded, with how its reply decodes. Built
/// without a session, so a driver whose session lives on another task encodes
/// where the request's borrows do.
pub struct Encoded {
    frame: Vec<u8>,
    kind: SlotKind,
}

impl Encoded {
    /// A frame past the ceiling is refused here rather than by the server's
    /// ingress cap, which would drop the connection.
    fn new(frame: Vec<u8>, kind: SlotKind) -> Result<Self, ClientError> {
        let total = frame.len();
        let limit = gnitz_wire::MAX_FRAME_PAYLOAD;
        if total > limit {
            return Err(ClientError::from(format!(
                "request frame is {total} bytes, exceeding the {limit}-byte server ingress cap; \
                 split the request"
            )));
        }
        Ok(Encoded { frame, kind })
    }
}

impl Request<'_> {
    /// Validate and encode, for [`Session::enqueue`].
    pub fn encode(self) -> Result<Encoded, ClientError> {
        let (frame, kind) = match self {
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
                    if gnitz_wire::sys_family_index(*tid).is_none() {
                        return Err(ClientError::from(format!("DDL family {tid} is not a system table")));
                    }
                    batch.validate(sys_schema(*tid))?;
                }
                (encode_ddl_txn(families), SlotKind::Commit)
            }
            Request::PushTxn { families } => {
                for f in families {
                    f.batch.validate(f.schema)?;
                }
                (encode_push_txn(families), SlotKind::Commit)
            }
            Request::Resolve(qname) => {
                let hdr = ControlHeader {
                    flags: WireFlags {
                        verb: ClientVerb::Resolve,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                (encode_frame(hdr, qname.as_bytes(), None, None), SlotKind::Resolve)
            }
            Request::Push { target_id, schema, batch, mode } => {
                // In-process, so a convenience and never a trust boundary; the
                // server checks the same things. Here so no driver has to
                // remember to.
                batch.validate(schema)?;
                // The push verb marks the frame as a push independent of data
                // presence, so an empty batch (a legitimate empty Z-set delta)
                // is ACKed as a no-op push instead of being mistaken for a scan.
                let flags = WireFlags {
                    verb: ClientVerb::Push,
                    conflict_mode: mode,
                    ..Default::default()
                };
                let hdr = ControlHeader { flags, target_id, ..Default::default() };
                (
                    encode_frame(hdr, &[], Some(&schema.to_block()), Some(batch)),
                    SlotKind::Push { tid: target_id },
                )
            }
            Request::ScanSpec { target_id, spec, reply_schema } => {
                let hdr = ControlHeader {
                    flags: WireFlags {
                        verb: ClientVerb::ScanSpec,
                        ..Default::default()
                    },
                    target_id,
                    arg0: reply_schema.layout_digest(),
                    ..Default::default()
                };
                (
                    encode_frame(hdr, &spec.encode(), None, None),
                    SlotKind::ScanSpec {
                        tid: target_id,
                        reply_schema: Arc::clone(reply_schema),
                    },
                )
            }
            Request::ScanMulti(rels) => {
                let items: Vec<txn_frame::ScanMultiItem> = rels
                    .iter()
                    .map(|(tid, schema)| txn_frame::ScanMultiItem {
                        tid: *tid,
                        reply_layout: schema.layout_digest(),
                    })
                    .collect();
                (txn_frame::encode_scan_multi(&items), SlotKind::Multi { rels })
            }
        };
        Encoded::new(frame, kind)
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
    /// SCAN_SPEC.
    Scan(ScanReply),
    /// `scan_multi`: N per-relation results in request order.
    Multi(Vec<ScanReply>),
    /// A PUSH or transaction ACK's LSN.
    Lsn(u64),
    /// A RESOLVE: the descriptor, or `None` when no such relation exists.
    Resolve(Option<Arc<RelDescriptor>>),
    /// An id allocation's base id.
    Id(u64),
    /// A delta poll: the slot is done, and every view's blocks and terminal
    /// went to the poll's own listener as their frames arrived.
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

    /// One relation's read result.
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
/// `Request` variant at [`Request::encode`].
enum SlotKind {
    Push {
        tid: u64,
    },
    Alloc,
    /// A DDL or push transaction ACK.
    Commit,
    Resolve,
    ScanSpec {
        tid: u64,
        reply_schema: Arc<Schema>,
    },
    /// One position per relation, each with the schema its train decodes under.
    Multi {
        rels: Vec<(u64, Arc<Schema>)>,
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
    /// The schema the data blocks of position `at` decode under: the one the
    /// request named. `None` for a slot whose reply carries no rows.
    fn reply_schema(&self, at: usize) -> Option<&Arc<Schema>> {
        match self {
            SlotKind::ScanSpec { reply_schema, .. } => Some(reply_schema),
            SlotKind::Multi { rels } => rels.get(at).map(|r| &r.1),
            SlotKind::Push { .. }
            | SlotKind::Alloc
            | SlotKind::Commit
            | SlotKind::Resolve
            | SlotKind::DeltaPoll { .. } => None,
        }
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
    data: Option<ZSetBatch>,
    /// Narrowed results of a `scan_multi`.
    replies: Vec<ScanReply>,
    /// The position of a multi-position slot (`Multi`, `DeltaPoll`) this train
    /// belongs to; `0` for every other kind.
    at: usize,
}

/// How one view's train ends: the cursor its terminal carries, or the fault
/// that ended its position.
pub(crate) type PollEnd = Result<DeltaCursor, ClientError>;

/// What a delta poll hands its listener as each frame arrives.
pub(crate) enum Polled {
    /// One data block of the view's train.
    Block(RawBlock),
    /// The view's terminal; the position is answered.
    End(PollEnd),
}

/// The listener a delta poll's results go to, addressed by the slot that asked
/// — so a train left behind by an abandoned poll is recognised rather than
/// matched onto a live view of the same id.
pub(crate) type PollSink<'a> = dyn FnMut(SlotId, Polled) + 'a;

/// Why a session refuses work.
enum Ended {
    Closed,
    Lost(ProtocolError),
}

impl Ended {
    fn error(&self) -> ClientError {
        match self {
            Ended::Closed => ClientError::Closed,
            Ended::Lost(cause) => ClientError::ConnectionLost(cause.clone()),
        }
    }
}

/// A protocol session: the transport plus all per-connection protocol state
/// (the pending queue and reply accumulator, and the continuation reassembly
/// and status→error policy that read/write them). Exactly one owner of that
/// state — the sync [`crate::GnitzClient`] holds one, and so does each async
/// executor.
pub struct Session {
    transport: ClientTransport,
    pending: VecDeque<Slot>,
    next_slot: u64,
    accum: Accumulator,
    /// Why this session refuses work; `None` while it is open.
    ended: Option<Ended>,
}

impl Session {
    /// `target` is an AF_UNIX socket path or a `tls://` target (see
    /// `ClientTransport::connect`).
    pub fn connect(target: &str) -> Result<Self, ClientError> {
        Ok(Self::over(ClientTransport::connect(
            target,
            Instant::now() + CONNECT_TIMEOUT,
        )?))
    }

    /// A session over a connected transport.
    pub(crate) fn over(transport: ClientTransport) -> Self {
        Session {
            transport,
            pending: VecDeque::new(),
            next_slot: 1,
            accum: Accumulator::default(),
            ended: None,
        }
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

    /// Encode, enqueue, register a slot. `Request` borrows its inputs; the
    /// borrow ends here.
    pub fn submit(&mut self, req: Request<'_>) -> Result<SlotId, ClientError> {
        self.enqueue(req.encode()?)
    }

    /// Queue an encoded request and open its slot. Raises at either cap.
    pub fn enqueue(&mut self, req: Encoded) -> Result<SlotId, ClientError> {
        self.check_open()?;
        // The predicate a driver reads for back-pressure, so the two can never
        // disagree; only the message re-derives which cap was hit.
        if self.at_capacity() {
            let queued = self.queued_bytes();
            return Err(ClientError::from(if self.pending.len() >= MAX_IN_FLIGHT {
                format!("connection has {MAX_IN_FLIGHT} requests in flight")
            } else {
                format!("connection has {queued} unwritten bytes queued, at the {MAX_QUEUED_BYTES}-byte cap")
            }));
        }
        Ok(self.open_slot(req))
    }

    /// DELTA_POLL: one train per item, in order, delivered to the [`PollSink`] of a
    /// [`Self::step_polling`] drain — without which they are dropped, hence no [`Request`].
    pub(crate) fn submit_delta_poll(&mut self, views: &[txn_frame::DeltaPollItem]) -> Result<SlotId, ClientError> {
        self.check_open()?;
        let kind = SlotKind::DeltaPoll {
            views: views.iter().map(|v| v.view_id).collect(),
        };
        Ok(self.open_slot(Encoded::new(txn_frame::encode_delta_poll(views), kind)?))
    }

    fn check_open(&self) -> Result<(), ClientError> {
        self.ended.as_ref().map_or(Ok(()), |e| Err(e.error()))
    }

    fn open_slot(&mut self, Encoded { frame, kind }: Encoded) -> SlotId {
        self.transport.enqueue(frame);
        let id = SlotId(self.next_slot);
        self.next_slot += 1;
        self.pending.push_back(Slot { id, kind });
        id
    }

    /// Do the I/O `ready` allows and return every slot that completed. Afterwards
    /// a driver may park on `interest()`: nothing buffered can advance, and
    /// bytes still queued are ones the fd refused.
    ///
    /// A failure ends the session: every slot still pending comes back
    /// [`ClientError::ConnectionLost`], after those that completed.
    pub fn step(&mut self, ready: Interest) -> Completions {
        self.step_polling(ready, None)
    }

    /// [`Self::step`] for a drain that takes a delta poll's per-view results.
    pub(crate) fn step_polling(&mut self, ready: Interest, mut sink: Option<&mut PollSink<'_>>) -> Completions {
        let mut done: Completions = Vec::new();
        if ready.read {
            self.read_frames(sink.as_deref_mut(), &mut done);
        }
        // Last, so the ciphertext a read queues goes out with this flush.
        if ready.write && self.ended.is_none() {
            if let Err(e) = self.transport.flush() {
                // A peer gone after answering is still readable.
                self.read_frames(sink, &mut done);
                self.end(Ended::Lost(e), &mut done);
            }
        }
        done
    }

    /// Feed the frames of every read the fd allows, until one proves it drained
    /// or fails — which ends the session.
    fn read_frames(&mut self, mut sink: Option<&mut PollSink<'_>>, done: &mut Completions) {
        let mut polled = Vec::new();
        while self.ended.is_none() {
            let Session { transport, pending, accum, .. } = self;
            let read = transport.read(|buf| feed(pending, accum, buf, done, &mut polled));
            let more = match read {
                Ok(more) => more,
                Err(e) => {
                    self.end(Ended::Lost(e), done);
                    false
                }
            };
            match sink.as_deref_mut() {
                Some(sink) => polled.drain(..).for_each(|(slot, p)| sink(slot, p)),
                // An abandoned poll: nobody is left to take what its frames owed.
                None => polled.clear(),
            }
            if !more {
                return;
            }
        }
    }

    /// Frame bytes queued and not yet written, against [`MAX_QUEUED_BYTES`].
    fn queued_bytes(&self) -> usize {
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
    /// or rustls has ciphertext to ship. Nothing once the session has ended.
    pub fn interest(&self) -> Interest {
        if self.ended.is_some() {
            return Interest::NONE;
        }
        Interest {
            read: !self.pending.is_empty(),
            write: self.transport.wants_write(),
        }
    }

    /// Whether this session refuses work: closed by its owner, or lost.
    pub fn is_closed(&self) -> bool {
        self.ended.is_some()
    }

    /// Fail every pending slot with `how`'s error and refuse further work. The
    /// shutdown shows the server EOF now rather than when the session drops.
    fn end(&mut self, how: Ended, done: &mut Completions) {
        if self.ended.is_some() {
            return;
        }
        self.transport.close();
        self.accum = Accumulator::default();
        done.extend(self.pending.drain(..).map(|s| (s.id, Err(how.error()))));
        self.ended = Some(how);
    }

    /// Close the session; returns every slot it abandoned, failed `Closed`.
    #[must_use]
    pub fn close(&mut self) -> Completions {
        let mut done = Vec::new();
        self.end(Ended::Closed, &mut done);
        done
    }

    /// Fail the session with `cause`, met outside `step`; returns every slot it
    /// abandoned, failed `ConnectionLost(cause)`.
    #[must_use]
    pub fn abort(&mut self, cause: ProtocolError) -> Completions {
        let mut done = Vec::new();
        self.end(Ended::Lost(cause), &mut done);
        done
    }
}

/// Feed one reply frame to the head slot: the slots it completes go onto `done`,
/// what it owes a delta poll's listener onto `polled`.
fn feed(
    pending: &mut VecDeque<Slot>,
    accum: &mut Accumulator,
    mut buf: Cow<'_, [u8]>,
    done: &mut Completions,
    polled: &mut Vec<(SlotId, Polled)>,
) -> Result<(), ProtocolError> {
    let Some(head) = pending.front() else {
        return Err(ProtocolError::DecodeError("reply frame with no request pending".into()));
    };
    // The view a DELTA_POLL slot is on and how many positions it answers,
    // else `None` — what tells the paths below to fill one position rather
    // than end the request.
    let poll = match &head.kind {
        SlotKind::DeltaPoll { views } => views.get(accum.at).map(|&v| (v, views.len())),
        _ => None,
    };
    // The relation this frame must name.
    let correlate_tid = match &head.kind {
        SlotKind::Push { tid } | SlotKind::ScanSpec { tid, .. } => Some(*tid),
        SlotKind::Multi { rels } => rels.get(accum.at).map(|r| r.0),
        SlotKind::DeltaPoll { .. } => poll.map(|(view, _)| view),
        SlotKind::Alloc | SlotKind::Commit | SlotKind::Resolve => None,
    };

    let ctrl = peek_control_block(&buf).map_err(ProtocolError::DecodeError)?;
    if let Some(fault) = ctrl.fault(&buf) {
        // A DELTA_POLL failure that names a view ends that view's position
        // alone; only one naming no relation fails the request.
        if let Some((view, positions)) = poll {
            if ctrl.hdr.target_id != 0 {
                if ctrl.hdr.target_id != view {
                    return Err(out_of_order(view, ctrl.hdr.target_id));
                }
                let slot = end_poll_position(pending, accum, done, positions);
                polled.push((slot, Polled::End(Err(ClientError::Refused(fault)))));
                return Ok(());
            }
        }
        complete_head(pending, accum, Err(ClientError::Refused(fault)), done);
        return Ok(());
    }
    if let Some(tid) = correlate_tid {
        // Replies arrive in request order, so a frame naming another
        // relation would decode under the wrong schema silently; make it loud.
        if ctrl.hdr.target_id != tid {
            return Err(out_of_order(tid, ctrl.hdr.target_id));
        }
    }
    // Only a RESOLVE is answered in the server's schema; every read decodes
    // under the schema its request named.
    let frame_schema = match ctrl.schema.clone() {
        None => None,
        Some(r) if matches!(head.kind, SlotKind::Resolve) => Some(Arc::new(
            Schema::from_block(&buf[r]).map_err(ProtocolError::DecodeError)?,
        )),
        Some(_) => {
            return Err(ProtocolError::DecodeError(
                "a schema block on a reply whose request named its schema".into(),
            ))
        }
    };

    match ctrl.data.clone() {
        Some(r) if head.kind.keeps_blocks_raw() => polled.push((
            head.id,
            Polled::Block(RawBlock {
                frame: std::mem::take(&mut buf).into_owned(),
                block: r,
            }),
        )),
        // Decoded straight into the accumulator: a train carries one data frame
        // per worker, and a per-frame batch would be copied in and dropped.
        Some(r) => {
            let eff = head
                .kind
                .reply_schema(accum.at)
                .ok_or_else(|| ProtocolError::DecodeError("a data block on a reply that carries no rows".into()))?;
            let sink = accum.data.get_or_insert_with(|| ZSetBatch::new(eff));
            decode_wal_block_into(sink, &buf[r], eff)?;
        }
        None => {}
    }

    if ctrl.hdr.flags.continuation {
        return Ok(());
    }

    // The train terminated.
    let data = accum.data.take();
    if let Some((_, positions)) = poll {
        let cursor = DeltaCursor::from_pair(ctrl.hdr.arg1, ctrl.hdr.arg0)
            .ok_or_else(|| ProtocolError::DecodeError("a delta-poll terminal at round 0".into()))?;
        let slot = end_poll_position(pending, accum, done, positions);
        polled.push((slot, Polled::End(Ok(cursor))));
        return Ok(());
    }
    let scan_reply = |schema: &Arc<Schema>, data: Option<ZSetBatch>, lsn: u64| ScanReply {
        batch: data.unwrap_or_else(|| ZSetBatch::new(schema)),
        schema: Arc::clone(schema),
        lsn: Some(lsn),
    };
    let reply = match &head.kind {
        SlotKind::Push { .. } => Ok(Reply::Lsn(ctrl.hdr.arg0)),
        SlotKind::Resolve => resolve_descriptor(&ctrl, &buf, frame_schema).map(Reply::Resolve),
        SlotKind::Alloc => Ok(Reply::Id(ctrl.hdr.target_id)),
        SlotKind::Commit => Ok(Reply::Lsn(ctrl.hdr.arg0)),
        SlotKind::ScanSpec { reply_schema, .. } => Ok(Reply::Scan(scan_reply(reply_schema, data, ctrl.hdr.arg0))),
        SlotKind::Multi { rels } => {
            accum.replies.push(scan_reply(&rels[accum.at].1, data, ctrl.hdr.arg0));
            // This position's train is over; the next one starts clean.
            *accum = Accumulator {
                at: accum.at + 1,
                replies: std::mem::take(&mut accum.replies),
                ..Default::default()
            };
            if accum.at < rels.len() {
                return Ok(());
            }
            Ok(Reply::Multi(std::mem::take(&mut accum.replies)))
        }
        // `poll` is `Some` for exactly this kind, and that path returned.
        SlotKind::DeltaPoll { .. } => unreachable!("a DELTA_POLL position is filled before this match"),
    };
    complete_head(pending, accum, reply, done);
    Ok(())
}

/// A reply frame naming a relation the head slot's position does not expect.
fn out_of_order(want: u64, got: u64) -> ProtocolError {
    ProtocolError::DecodeError(format!("reply out of order: expected target {want}, got {got}"))
}

/// End the delta-poll position the head slot is on, and complete the slot once
/// that was its last. Returns the slot the position belonged to.
fn end_poll_position(
    pending: &mut VecDeque<Slot>,
    accum: &mut Accumulator,
    done: &mut Completions,
    positions: usize,
) -> SlotId {
    let slot = pending.front().expect("a frame was fed to a pending head").id;
    // This position's train is over; the next one starts clean.
    *accum = Accumulator { at: accum.at + 1, ..Default::default() };
    if accum.at == positions {
        complete_head(pending, accum, Ok(Reply::Polled), done);
    }
    slot
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

/// A RESOLVE train as its descriptor; `None` when the reply names no relation.
fn resolve_descriptor(
    ctrl: &DecodedControl,
    frame: &[u8],
    schema: Option<Arc<Schema>>,
) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
    if ctrl.hdr.target_id == 0 {
        return Ok(None);
    }
    let schema = schema.ok_or_else(|| ProtocolError::DecodeError("RESOLVE reply carries no schema".into()))?;
    let desc = RelDescriptorBlob::decode(&frame[ctrl.blob.clone()]).map_err(ProtocolError::DecodeError)?;
    Ok(Some(Arc::new(RelDescriptor {
        tid: ctrl.hdr.target_id,
        class: desc.class,
        pk_repeats: desc.pk_repeats,
        serial: desc.serial,
        schema,
        indexes: desc.indexes,
    })))
}

#[cfg(test)]
#[path = "tests/connection.rs"]
mod tests;
