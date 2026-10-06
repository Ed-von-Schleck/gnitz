//! The protocol session: a sans-io connection state machine. Nothing in it
//! waits; whoever drives [`Session::step`] does.
//!
//! Replies leave the server in request order, so only the head of the pending
//! queue is ever being answered: every byte that arrives is the head slot's
//! until its last train terminates.

use gnitz_expr::SchemaFacts;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::os::fd::{BorrowedFd, RawFd};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
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
    /// The RESOLVE answer's descriptor token: what a request built from this
    /// descriptor carries, for the server to refuse it once the relation
    /// resolves differently.
    pub token: u64,
}

pub use gnitz_wire::control::Target;

impl From<&RelDescriptor> for Target {
    fn from(rel: &RelDescriptor) -> Self {
        Target { tid: rel.tid, token: rel.token }
    }
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

/// One request, cut where the encoding or the decoding differs rather than
/// where the verb names do. It borrows its inputs; the borrow ends at
/// [`Request::encode`].
pub enum Request<'a> {
    /// A run of this many catalog object ids. Completes as [`Reply::Ack`], the
    /// run's base id.
    AllocIds(u64),
    /// A run of `count` ids from the SERIAL sequence of `table`. Completes as
    /// [`Reply::Ack`], the run's base id.
    AllocSerial { table: Target, count: u64 },
    /// An atomic DDL transaction: system-table batches, each named by its table
    /// id, under one durable SAL zone. Completes as [`Reply::Ack`], its LSN.
    DdlTxn(&'a [(u64, ZSetBatch)]),
    /// An atomic user-table push transaction: the server refuses it if some
    /// family's relation was written after that family's `basis`. Completes as
    /// [`Reply::Ack`], its LSN.
    PushTxn { families: &'a [PushFamily<'a>] },
    /// RESOLVE — describe the relation named by the canonical
    /// `"schema_name.relation_name"`. Uncorrelated on the wire, because the
    /// request names no id.
    Resolve(&'a str),
    /// PUSH. The frame always carries `schema`'s record. Completes as
    /// [`Reply::Ack`], its LSN.
    Push {
        target: Target,
        schema: &'a Schema,
        batch: &'a ZSetBatch,
        mode: WireConflictMode,
    },
    /// SCAN_SPEC, replied in `reply_schema`'s layout.
    ScanSpec {
        target: Target,
        spec: &'a gnitz_wire::ReadSpec,
        reply_schema: &'a Arc<Schema>,
    },
    /// SCAN_MULTI: every row of N relations at one cut, each replied in the
    /// layout of the schema paired with it.
    ScanMulti(Vec<(u64, Arc<Schema>)>),
}

/// A request validated and encoded, with how its reply decodes.
pub(crate) struct Encoded {
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

    /// DELTA_POLL: one train per view, in order, queued on `feed` under the
    /// view's index in its poll — `first` for the first of `views`. Not a
    /// [`Request`], because its results are a stream and not one reply.
    fn delta_poll(views: &[txn_frame::DeltaPollItem], first: usize, feed: &Arc<PollFeed>) -> Result<Self, ClientError> {
        if views.is_empty() {
            return Err(ClientError::from("a delta poll names no view".to_string()));
        }
        let kind = SlotKind::DeltaPoll {
            views: views.iter().map(|v| v.view_id).collect(),
            at: 0,
            first,
            feed: Arc::downgrade(feed),
        };
        Encoded::new(txn_frame::encode_delta_poll(views), kind)
    }
}

impl Request<'_> {
    /// Validate and encode.
    fn encode(self) -> Result<Encoded, ClientError> {
        let (frame, kind) = match self {
            Request::AllocIds(n) => {
                let hdr = ControlHeader {
                    flags: WireFlags {
                        verb: ClientVerb::AllocIds,
                        ..Default::default()
                    },
                    arg0: n,
                    ..Default::default()
                };
                (encode_frame(hdr, &[], None, None), SlotKind::Ack { tid: 0 })
            }
            Request::AllocSerial { table, count } => {
                let hdr = ControlHeader::naming(ClientVerb::AllocSerialRange, table, count);
                (encode_frame(hdr, &[], None, None), SlotKind::Ack { tid: table.tid })
            }
            Request::DdlTxn(families) => {
                for (tid, batch) in families {
                    if gnitz_wire::sys_family_index(*tid).is_none() {
                        return Err(ClientError::from(format!("DDL family {tid} is not a system table")));
                    }
                    batch.validate(sys_schema(*tid))?;
                }
                (encode_ddl_txn(families), SlotKind::Ack { tid: 0 })
            }
            Request::PushTxn { families } => {
                for f in families {
                    f.batch.validate(f.schema)?;
                }
                (encode_push_txn(families), SlotKind::Ack { tid: 0 })
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
            Request::Push { target, schema, batch, mode } => {
                // In-process, so a convenience and never a trust boundary; the
                // server checks the same things. Here so no driver has to
                // remember to.
                batch.validate(schema)?;
                // The push verb marks the frame as a push independent of data
                // presence, so an empty batch (a legitimate empty Z-set delta)
                // is ACKed as a no-op push instead of being mistaken for a scan.
                let mut hdr = ControlHeader::naming(ClientVerb::Push, target, 0);
                hdr.flags.conflict_mode = mode;
                (
                    encode_frame(hdr, &[], Some(&schema.to_block()), Some(batch)),
                    SlotKind::Ack { tid: target.tid },
                )
            }
            Request::ScanSpec { target, spec, reply_schema } => {
                let hdr = ControlHeader::naming(ClientVerb::ScanSpec, target, reply_schema.layout_digest());
                (
                    encode_frame(hdr, &spec.encode(), None, None),
                    SlotKind::Scan {
                        tid: target.tid,
                        reply_schema: Arc::clone(reply_schema),
                        data: None,
                    },
                )
            }
            Request::ScanMulti(rels) => {
                if rels.is_empty() {
                    return Err(ClientError::from("a multi-read names no relation".to_string()));
                }
                let items: Vec<txn_frame::ScanMultiItem> = rels
                    .iter()
                    .map(|(tid, schema)| txn_frame::ScanMultiItem {
                        tid: *tid,
                        reply_layout: schema.layout_digest(),
                    })
                    .collect();
                let (replies, data) = (Vec::with_capacity(rels.len()), None);
                (
                    txn_frame::encode_scan_multi(&items),
                    SlotKind::Multi { rels, replies, data },
                )
            }
        };
        Encoded::new(frame, kind)
    }
}

/// What a slot's verb asked for. The spine resolves a reply against the request
/// that opened its slot, so no driver re-attaches a relation id.
#[derive(Debug)]
pub enum Reply {
    /// SCAN_SPEC.
    Scan(ScanReply),
    /// `scan_multi`: N per-relation results in request order.
    Multi(Vec<ScanReply>),
    /// A control-only ACK's value: a PUSH's or transaction's LSN, an id
    /// allocation's base id.
    Ack(u64),
    /// A RESOLVE: the descriptor, or `None` when no such relation exists.
    Resolve(Option<Arc<RelDescriptor>>),
    /// A delta poll: the slot is done, and every view's blocks and end went to
    /// the poll's own feed.
    Polled,
}

impl Reply {
    /// The variant's name, for [`wrong_shape`]. Not `Debug`: `Reply::Scan`
    /// reaches a whole `ZSetBatch`.
    fn kind(&self) -> &'static str {
        match self {
            Reply::Scan(_) => "Scan",
            Reply::Multi(_) => "Multi",
            Reply::Ack(_) => "Ack",
            Reply::Resolve(_) => "Resolve",
            Reply::Polled => "Polled",
        }
    }

    /// A control-only ACK's value.
    #[inline]
    #[track_caller]
    pub fn into_ack(self) -> u64 {
        match self {
            Reply::Ack(value) => value,
            other => wrong_shape(other.kind(), "Ack"),
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

/// How a slot decodes its reply, what that reply becomes, and the reply read so
/// far. A slot is answered only while it heads the queue, so its state is the
/// one train in progress, and it goes when the slot does.
enum SlotKind {
    /// A control-only ACK naming `tid`, its value in `arg0`.
    Ack {
        tid: u64,
    },
    Resolve,
    Scan {
        tid: u64,
        reply_schema: Arc<Schema>,
        /// The train's rows so far.
        data: Option<ZSetBatch>,
    },
    /// One train per relation, each decoded under the schema paired with it.
    /// The train in progress is `rels[replies.len()]`.
    Multi {
        rels: Vec<(u64, Arc<Schema>)>,
        replies: Vec<ScanReply>,
        data: Option<ZSetBatch>,
    },
    /// One train per view, its blocks kept raw. The train in progress is
    /// `views[at]`. The ids are what correlates each
    /// terminal: views differing only in a `WHERE` share a schema, so a
    /// misdirected block would decode cleanly.
    DeltaPoll {
        views: Vec<u64>,
        at: usize,
        /// The index of `views[0]` in the poll this request is a chunk of.
        first: usize,
        /// Dead once the poll's reader is gone, which drops what is left of
        /// its trains.
        feed: Weak<PollFeed>,
    },
}

struct Slot {
    kind: SlotKind,
    /// Where the reply goes. A delta poll has none: its results go to its feed.
    cell: Option<Arc<SentCell>>,
}

/// Where a request's reply is, between its request and its reader.
#[derive(Default)]
enum Awaited {
    #[default]
    Outstanding,
    Arrived(Result<Reply, ClientError>),
    /// Handed on as it arrives.
    Routed(Box<dyn FnOnce(Result<Reply, ClientError>) + Send>),
}

#[derive(Default)]
struct SentCell(Mutex<Awaited>);

impl SentCell {
    /// A panic under this lock leaves what it guards whole.
    fn lock(&self) -> MutexGuard<'_, Awaited> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn arrive(&self, reply: Result<Reply, ClientError>) {
        let mut state = self.lock();
        match std::mem::take(&mut *state) {
            Awaited::Routed(route) => {
                drop(state);
                route(reply)
            }
            _ => *state = Awaited::Arrived(reply),
        }
    }

    fn take(&self) -> Option<Result<Reply, ClientError>> {
        let mut state = self.lock();
        match std::mem::take(&mut *state) {
            Awaited::Arrived(reply) => Some(reply),
            other => {
                *state = other;
                None
            }
        }
    }
}

/// The reply to a request already submitted, borrowing nothing. Stepping the
/// session is what makes it arrive:
/// [`GnitzClient::wait`](crate::GnitzClient::wait) steps until it has.
pub struct Sent<T>(SentState<T>);

enum SentState<T> {
    /// Answered without a request.
    Ready(Option<Result<T, ClientError>>),
    Waiting(Arc<SentCell>, fn(Reply) -> T),
}

impl<T> Sent<T> {
    pub fn ready(value: Result<T, ClientError>) -> Self {
        Sent(SentState::Ready(Some(value)))
    }

    /// The reply, if it has arrived. It is handed out once.
    pub fn try_take(&mut self) -> Option<Result<T, ClientError>> {
        match &mut self.0 {
            SentState::Ready(value) => value.take(),
            SentState::Waiting(cell, narrow) => cell.take().map(|r| r.map(*narrow)),
        }
    }

    /// Hand the reply to `route` — now if it has arrived, else from inside the
    /// step that reads it.
    pub fn then(self, route: impl FnOnce(Result<T, ClientError>) + Send + 'static)
    where
        T: 'static,
    {
        let (cell, narrow) = match self.0 {
            SentState::Ready(value) => return route(value.expect("a reply is handed out once")),
            SentState::Waiting(cell, narrow) => (cell, narrow),
        };
        let mut state = cell.lock();
        match std::mem::take(&mut *state) {
            Awaited::Arrived(reply) => {
                drop(state);
                route(reply.map(narrow))
            }
            _ => *state = Awaited::Routed(Box::new(move |reply| route(reply.map(narrow)))),
        }
    }
}

/// How one view's train ends: the cursor its terminal carries, or the fault
/// that ended its position.
pub(crate) type PollEnd = Result<DeltaCursor, ClientError>;

/// What a delta poll hands its reader as each frame arrives.
pub(crate) enum Polled {
    /// One data block of the view's train.
    Block(RawBlock),
    /// The view's terminal; the position is answered.
    End(PollEnd),
}

/// Where a delta poll's results queue for its reader, each under its view's
/// index in the poll. Each view gets exactly one [`Polled::End`], in request
/// order, however the poll ends.
pub(crate) type PollFeed = Mutex<VecDeque<(usize, Polled)>>;

/// Queue `polled` for the reader of `feed`, if it still has one.
fn hand(feed: &Weak<PollFeed>, item: usize, polled: Polled) {
    if let Some(feed) = feed.upgrade() {
        feed.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((item, polled));
    }
}

/// A protocol session: the transport plus all per-connection protocol state
/// (the pending queue, and the continuation reassembly and status→error policy
/// that read/write it). Exactly one owner of that state: a
/// [`crate::GnitzClient`], whose host decides how it waits.
pub struct Session {
    transport: ClientTransport,
    pending: VecDeque<Slot>,
    submitted: u64,
    /// The error every request gets once the session has ended — `Closed` or
    /// `ConnectionLost`; `None` while it is open.
    ended: Option<ClientError>,
    /// The last read stopped with more waiting, to hand a delta poll's blocks
    /// over before reading on.
    unread: bool,
}

/// Every reply still owed resolves `Closed`, so nothing waits on a session
/// that is gone.
impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
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
            submitted: 0,
            ended: None,
            unread: false,
        }
    }

    /// Requests submitted since this session connected — one per `submit`,
    /// however the bytes were batched. HELLO predates the session, uncounted.
    pub fn requests_sent(&self) -> u64 {
        self.submitted
    }

    /// The connection's socket, as a number: valid while this session lives.
    pub fn as_raw_fd(&self) -> RawFd {
        self.transport.as_raw_fd()
    }

    /// The connection's socket, for a [`Host`](crate::Host) to register.
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.transport.as_fd()
    }

    // ── The spine ──────────────────────────────────────────────────────────

    /// Encode and queue `req`; its reply arrives as `narrow` makes of it.
    /// `Request` borrows its inputs; the borrow ends here. Raises at either cap.
    pub fn submit<T>(&mut self, req: Request<'_>, narrow: fn(Reply) -> T) -> Result<Sent<T>, ClientError> {
        let cell = Arc::new(SentCell::default());
        self.enqueue(req.encode()?, Some(Arc::clone(&cell)))?;
        Ok(Sent(SentState::Waiting(cell, narrow)))
    }

    /// Queue a delta poll of `views`, the items of a poll from its `first` on.
    /// Their results queue on `feed` as the steps that read them run.
    pub(crate) fn submit_delta_poll(
        &mut self,
        views: &[txn_frame::DeltaPollItem],
        first: usize,
        feed: &Arc<PollFeed>,
    ) -> Result<(), ClientError> {
        self.enqueue(Encoded::delta_poll(views, first, feed)?, None)
    }

    fn enqueue(&mut self, Encoded { frame, kind }: Encoded, cell: Option<Arc<SentCell>>) -> Result<(), ClientError> {
        if let Some(why) = &self.ended {
            return Err(why.clone());
        }
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
        self.transport.enqueue(frame);
        self.submitted += 1;
        self.pending.push_back(Slot { kind, cell });
        Ok(())
    }

    /// Do the I/O `ready` allows; every reply it completes arrives. Afterwards
    /// a driver may park on `interest()`: nothing buffered can advance, and
    /// bytes still queued are ones the fd refused.
    ///
    /// A failure ends the session: every reply still owed arrives as
    /// [`ClientError::ConnectionLost`], after those that completed.
    pub fn step(&mut self, ready: Interest) {
        if ready.read {
            self.read_frames();
        }
        // Last, so the ciphertext a read queues goes out with this flush.
        if ready.write && self.ended.is_none() {
            if let Err(e) = self.transport.flush() {
                // A peer gone after answering is still readable.
                self.read_frames();
                self.end(ClientError::ConnectionLost(e));
            }
        }
    }

    /// Feed the frames of every read the fd allows, until one proves it drained
    /// or fails — which ends the session.
    ///
    /// Stops at the first read that queued a delta poll's blocks for its
    /// reader, so the blocks held at once are one read's.
    fn read_frames(&mut self) {
        self.unread = false;
        while self.ended.is_none() {
            let Session { transport, pending, .. } = self;
            let mut handed = false;
            let more = match transport.read(|buf| feed(pending, buf, &mut handed)) {
                Ok(more) => more,
                Err(e) => {
                    self.end(ClientError::ConnectionLost(e));
                    false
                }
            };
            if !more {
                return;
            }
            if handed {
                self.unread = true;
                return;
            }
        }
    }

    /// The last step left a read unfinished: step `READ` again without waiting
    /// for the socket, which has no new edge to report.
    pub(crate) fn unread(&self) -> bool {
        self.unread
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

    /// Fail every pending slot with `why` and refuse further work. The shutdown
    /// shows the server EOF now rather than when the session drops.
    fn end(&mut self, why: ClientError) {
        if self.ended.is_some() {
            return;
        }
        self.transport.close();
        for slot in self.pending.drain(..) {
            slot.complete(Err(why.clone()));
        }
        self.ended = Some(why);
    }

    /// Close the session: every reply still owed arrives as `Closed`.
    pub fn close(&mut self) {
        self.end(ClientError::Closed);
    }
}

/// Feed one reply frame to the head slot, whose reply arrives if that completes
/// it. `handed` is set by a block queued for a delta poll's reader.
fn feed(pending: &mut VecDeque<Slot>, buf: Cow<'_, [u8]>, handed: &mut bool) -> Result<(), ProtocolError> {
    let Some(head) = pending.front_mut() else {
        return Err(ProtocolError::DecodeError("reply frame with no request pending".into()));
    };
    if let Some(result) = head.feed(buf, handed)? {
        let slot = pending.pop_front().expect("the head was just fed");
        slot.complete(result);
    }
    Ok(())
}

impl Slot {
    /// Take one reply frame. `Some` once the slot is answered: its reply, or the
    /// refusal that ended the request. `Err` is a frame this slot cannot have
    /// been sent, which ends the session.
    fn feed(
        &mut self,
        mut buf: Cow<'_, [u8]>,
        handed: &mut bool,
    ) -> Result<Option<Result<Reply, ClientError>>, ProtocolError> {
        let ctrl = peek_control_block(&buf).map_err(ProtocolError::DecodeError)?;
        let named = ctrl.hdr.target_id;
        // The relation this frame must name. Replies arrive in request order,
        // so a frame naming another would decode under the wrong schema
        // silently; make it loud.
        let want = match &self.kind {
            SlotKind::Ack { tid } | SlotKind::Scan { tid, .. } => Some(*tid),
            SlotKind::Multi { rels, replies, .. } => Some(rels[replies.len()].0),
            SlotKind::DeltaPoll { views, at, .. } => Some(views[*at]),
            SlotKind::Resolve => None,
        };
        if let Some(fault) = ctrl.fault(&buf) {
            let refused = ClientError::Refused(fault);
            // A DELTA_POLL fault naming a view ends that view's position alone;
            // every other fault ends the request.
            return match (&mut self.kind, want) {
                (SlotKind::DeltaPoll { views, at, first, feed }, Some(view)) if named != 0 => {
                    if named != view {
                        return Err(out_of_order(view, named));
                    }
                    hand(feed, *first + *at, Polled::End(Err(refused)));
                    *at += 1;
                    Ok((*at == views.len()).then_some(Ok(Reply::Polled)))
                }
                _ => Ok(Some(Err(refused))),
            };
        }
        if let Some(want) = want.filter(|&want| want != named) {
            return Err(out_of_order(want, named));
        }
        // Only a RESOLVE is answered in the server's schema; every read decodes
        // under the schema its request named.
        let frame_schema = match ctrl.schema.clone() {
            None => None,
            Some(r) if matches!(self.kind, SlotKind::Resolve) => Some(Arc::new(
                Schema::from_block(&buf[r]).map_err(ProtocolError::DecodeError)?,
            )),
            Some(_) => {
                return Err(ProtocolError::DecodeError(
                    "a schema block on a reply whose request named its schema".into(),
                ))
            }
        };

        // Rows are decoded straight into the slot: a train carries one data
        // frame per worker, and a per-frame batch would be copied in and dropped.
        let decode = |data: &mut Option<ZSetBatch>, schema: &Arc<Schema>, block: &[u8]| {
            decode_wal_block_into(data.get_or_insert_with(|| ZSetBatch::new(schema)), block, schema)
        };
        match (&mut self.kind, ctrl.data.clone()) {
            (_, None) => {}
            // An abandoned poll's block is dropped uncopied.
            (SlotKind::DeltaPoll { at, first, feed, .. }, Some(block)) if feed.strong_count() > 0 => {
                let frame = std::mem::take(&mut buf).into_owned();
                hand(feed, *first + *at, Polled::Block(RawBlock { frame, block }));
                *handed = true;
            }
            (SlotKind::DeltaPoll { .. }, Some(_)) => {}
            (SlotKind::Scan { reply_schema, data, .. }, Some(r)) => decode(data, reply_schema, &buf[r])?,
            (SlotKind::Multi { rels, replies, data }, Some(r)) => decode(data, &rels[replies.len()].1, &buf[r])?,
            (SlotKind::Ack { .. } | SlotKind::Resolve, Some(_)) => {
                return Err(ProtocolError::DecodeError(
                    "a data block on a reply that carries no rows".into(),
                ))
            }
        }

        if ctrl.hdr.flags.continuation {
            return Ok(None);
        }

        // The train terminated.
        let scan_reply = |schema: &Arc<Schema>, data: &mut Option<ZSetBatch>| ScanReply {
            batch: data.take().unwrap_or_else(|| ZSetBatch::new(schema)),
            schema: Arc::clone(schema),
            lsn: Some(ctrl.hdr.arg0),
        };
        Ok(match &mut self.kind {
            SlotKind::Ack { .. } => Some(Ok(Reply::Ack(ctrl.hdr.arg0))),
            SlotKind::Resolve => Some(Ok(Reply::Resolve(resolve_descriptor(&ctrl, &buf, frame_schema)?))),
            SlotKind::Scan { reply_schema, data, .. } => Some(Ok(Reply::Scan(scan_reply(reply_schema, data)))),
            SlotKind::Multi { rels, replies, data } => {
                let reply = scan_reply(&rels[replies.len()].1, data);
                replies.push(reply);
                (replies.len() == rels.len()).then(|| Ok(Reply::Multi(std::mem::take(replies))))
            }
            SlotKind::DeltaPoll { views, at, first, feed } => {
                let cursor = DeltaCursor::from_pair(ctrl.hdr.arg1, ctrl.hdr.arg0)
                    .ok_or_else(|| ProtocolError::DecodeError("a delta-poll terminal at round 0".into()))?;
                hand(feed, *first + *at, Polled::End(Ok(cursor)));
                *at += 1;
                (*at == views.len()).then_some(Ok(Reply::Polled))
            }
        })
    }

    /// The slot is answered: its reply arrives. A delta poll that failed whole
    /// ends each view it had yet to answer with that failure.
    fn complete(self, result: Result<Reply, ClientError>) {
        if let (SlotKind::DeltaPoll { views, at, first, feed }, Err(why)) = (&self.kind, &result) {
            for item in first + at..first + views.len() {
                hand(feed, item, Polled::End(Err(why.clone())));
            }
        }
        if let Some(cell) = self.cell {
            cell.arrive(result);
        }
    }
}

/// A reply frame naming a relation the head slot's position does not expect.
fn out_of_order(want: u64, got: u64) -> ProtocolError {
    ProtocolError::DecodeError(format!("reply out of order: expected target {want}, got {got}"))
}

/// A RESOLVE train as its descriptor; `None` when the reply names no relation.
fn resolve_descriptor(
    ctrl: &DecodedControl,
    frame: &[u8],
    schema: Option<Arc<Schema>>,
) -> Result<Option<Arc<RelDescriptor>>, ProtocolError> {
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
        token: ctrl.hdr.arg0,
    })))
}

#[cfg(test)]
#[path = "tests/connection.rs"]
mod tests;
