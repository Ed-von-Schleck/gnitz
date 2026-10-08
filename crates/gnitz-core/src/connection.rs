//! The protocol session: a sans-io connection state machine. Nothing in it
//! waits; whoever drives [`Session::step`] does.
//!
//! Replies leave the server in request order, so only the head of the pending
//! queue is ever being answered: every frame that arrives is the head slot's
//! until its last train terminates — but for a pushed train, which opens with
//! a frame marked as one, arrives whole between two replies and is set aside
//! for whoever holds the subscription it is of.

use gnitz_expr::SchemaFacts;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::num::NonZeroU64;
use std::ops::Range;
use std::os::fd::{BorrowedFd, RawFd};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::error::ClientError;
use crate::protocol::wal_block::decode_wal_block_into;
use crate::{
    encode_ddl_txn, encode_frame, encode_push_txn, sys_schema, ClientTransport, ProtocolError, PushFamily, Schema,
    ZSetBatch,
};
use gnitz_wire::control::{peek_control_block, ControlHeader, DecodedControl};
use gnitz_wire::txn_frame;
use gnitz_wire::CONNECT_TIMEOUT;
use gnitz_wire::{ClientVerb, WireConflictMode};
use gnitz_wire::{RelClass, RelDescriptorBlob, RelIndex};

/// Requests one connection may hold in flight; `submit` raises past it. A bound
/// on the memory a driver that never waits can pin, not a throughput knob.
pub const MAX_IN_FLIGHT: usize = 4096;

/// Unwritten frame bytes one connection may hold; `submit` raises past it. An
/// encoded push is a full copy of its batch, so the count above bounds no
/// memory on its own. Checked before queueing, so one frame up to the peer's
/// egress limit always goes through — what it bounds is a driver that submits
/// without flushing.
pub const MAX_QUEUED_BYTES: usize = 64 << 20;

/// One relation's read result.
#[derive(Debug)]
pub struct ScanReply {
    pub schema: Arc<Schema>,
    pub batch: ZSetBatch,
    /// The server LSN the read was served at. `None` for rows no server read
    /// produced: an answer off a mirrored copy, a constant, `EXPLAIN` or
    /// `RETURNING`.
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

    /// The flat pair [`Self::from_pair`] reads `cursor` back from.
    pub fn flat(cursor: Option<DeltaCursor>) -> (u64, u64) {
        cursor.map_or((0, 0), DeltaCursor::pair)
    }

    /// The item of a delta read of `view` after `from` — the view whole with
    /// none — under `spec`, replied in `reply_schema`'s layout.
    pub(crate) fn item<'a>(
        from: Option<DeltaCursor>,
        view: Target,
        reply_schema: &Schema,
        spec: &'a [u8],
    ) -> txn_frame::DeltaPollItem<'a> {
        let (tag, after_tick) = DeltaCursor::flat(from);
        txn_frame::DeltaPollItem {
            view,
            tag,
            after_tick,
            reply_layout: reply_schema.layout_digest(),
            spec,
        }
    }
}

// ── The request vocabulary ───────────────────────────────────────────────────

/// What a driver should wait for before its next `step`, and what it hands
/// back: two booleans on the connection.
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

/// A request answered by one value — an LSN, or the base id of a run. It
/// borrows its inputs; the borrow ends at [`Session::submit`].
pub enum Request<'a> {
    /// A run of this many catalog object ids.
    AllocIds(u64),
    /// A run of `count` ids from the SERIAL sequence of `table`.
    AllocSerial { table: Target, count: u64 },
    /// An atomic DDL transaction: system-table batches, each named by its table
    /// id, under one durable SAL zone.
    DdlTxn(&'a [(u64, ZSetBatch)]),
    /// An atomic user-table push transaction: the server refuses it if some
    /// family's relation was written after that family's `basis`.
    PushTxn { families: &'a [PushFamily] },
    /// PUSH. The frame always carries `schema`'s record.
    Push {
        target: Target,
        schema: &'a Schema,
        batch: &'a ZSetBatch,
        mode: WireConflictMode,
    },
}

impl Request<'_> {
    /// Validate and encode: the frame, and the relation its ACK names.
    fn encode(self) -> Result<(Vec<u8>, u64), ClientError> {
        Ok(match self {
            Request::AllocIds(n) => {
                let hdr = ControlHeader::naming(ClientVerb::AllocIds, Target::from(0), n);
                (encode_frame(hdr, &[], None, None), 0)
            }
            Request::AllocSerial { table, count } => {
                let hdr = ControlHeader::naming(ClientVerb::AllocSerialRange, table, count);
                (encode_frame(hdr, &[], None, None), table.tid)
            }
            Request::DdlTxn(families) => {
                for (tid, batch) in families {
                    if gnitz_wire::sys_family_index(*tid).is_none() {
                        return Err(ClientError::from(format!("DDL family {tid} is not a system table")));
                    }
                    batch.validate(sys_schema(*tid))?;
                }
                (encode_ddl_txn(families), 0)
            }
            Request::PushTxn { families } => {
                for f in families {
                    f.batch.validate(&f.schema)?;
                }
                (encode_push_txn(families), 0)
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
                    target.tid,
                )
            }
        })
    }
}

/// One request in flight: how its reply decodes, the reply read so far, and
/// where it goes. A slot is answered only while it heads the queue, so its
/// state is the one train in progress, and it goes when the slot does.
enum Slot {
    /// A control-only ACK naming `tid`, its value in `arg0`.
    Ack { tid: u64, to: Promise<u64> },
    /// RESOLVE: the descriptor, or `None` when no such relation exists.
    /// Uncorrelated on the wire, because the request names no id.
    Resolve { to: Promise<Option<Arc<RelDescriptor>>> },
    Scan {
        tid: u64,
        reply_schema: Arc<Schema>,
        /// The train's rows so far.
        data: Option<ZSetBatch>,
        to: Promise<ScanReply>,
    },
    /// One view's delta read: a scan's train, whose terminal carries the cursor
    /// to read from next.
    Delta {
        tid: u64,
        reply_schema: Arc<Schema>,
        data: Option<ZSetBatch>,
        to: Promise<(ScanReply, DeltaCursor)>,
    },
    /// One train per relation, each decoded under the schema paired with it.
    /// The train in progress is `rels[replies.len()]`.
    Multi {
        rels: Vec<(u64, Arc<Schema>)>,
        replies: Vec<ScanReply>,
        data: Option<ZSetBatch>,
        to: Promise<Vec<ScanReply>>,
    },
    /// One train per view, its blocks kept raw. The train in progress is
    /// `views[at]`. The ids are what correlates each
    /// terminal: views differing only in a `WHERE` share a schema, so a
    /// misdirected block would decode cleanly.
    DeltaPoll {
        views: Vec<u64>,
        at: usize,
        /// The poll this request is; see [`Polls::live`].
        poll: u64,
    },
    /// A SUBSCRIBE asking for the subscriptions `ids`. Its ACK says only that
    /// the frame was read, so nothing waits for it: one that fails ends each
    /// of them, as a fault train does.
    Subscribe { ids: Range<u64> },
    /// A SYNC_PUSHED, sent when the subscriptions up to `asked` were asked for.
    Sync { asked: u64, to: Promise<()> },
    /// A pushed train of subscription `sub` to `tid`, which no request is
    /// answered by: it never queues, and is fed ahead of the head while open.
    Pushed { tid: u64, sub: u64, blocks: Vec<RawBlock> },
}

/// Where a reply is, between its writer and its reader.
#[derive(Default)]
enum Awaited<T> {
    #[default]
    Outstanding,
    Arrived(Result<T, ClientError>),
    /// Handed on as it arrives.
    Routed(Box<dyn FnOnce(Result<T, ClientError>) + Send>),
    /// Awaited by a task its arrival wakes.
    Watched(Waker),
}

struct Cell<T>(Mutex<Awaited<T>>);

impl<T> Cell<T> {
    /// A panic under this lock leaves what it guards whole.
    fn lock(&self) -> MutexGuard<'_, Awaited<T>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A reply on its way, borrowing nothing. One to a request arrives as the
/// session is stepped: [`GnitzClient::wait`](crate::GnitzClient::wait) steps
/// until it has, and awaiting it directly leaves the stepping to someone else.
pub struct Sent<T>(Arc<Cell<T>>);

/// The writer of a [`Sent`]. Dropped unfulfilled, the reply is `Closed`.
pub struct Promise<T>(Option<Arc<Cell<T>>>);

/// A reply yet to arrive, and its writer.
pub fn promise<T>() -> (Promise<T>, Sent<T>) {
    let cell = Arc::new(Cell(Mutex::new(Awaited::Outstanding)));
    (Promise(Some(Arc::clone(&cell))), Sent(cell))
}

impl<T> Promise<T> {
    /// The reply arrives; a second one goes nowhere.
    pub fn fulfil(&mut self, reply: Result<T, ClientError>) {
        let Some(cell) = self.0.take() else {
            return;
        };
        let mut state = cell.lock();
        match std::mem::take(&mut *state) {
            Awaited::Routed(route) => {
                drop(state);
                route(reply)
            }
            Awaited::Watched(waiter) => {
                *state = Awaited::Arrived(reply);
                drop(state);
                waiter.wake()
            }
            Awaited::Outstanding | Awaited::Arrived(_) => *state = Awaited::Arrived(reply),
        }
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        self.fulfil(Err(ClientError::Closed));
    }
}

impl<T> Sent<T> {
    /// A reply that needed no request.
    pub fn ready(value: Result<T, ClientError>) -> Self {
        Sent(Arc::new(Cell(Mutex::new(Awaited::Arrived(value)))))
    }

    /// The reply, if it has arrived. It is handed out once.
    pub fn try_take(&mut self) -> Option<Result<T, ClientError>> {
        let mut state = self.0.lock();
        match std::mem::take(&mut *state) {
            Awaited::Arrived(reply) => Some(reply),
            other => {
                *state = other;
                None
            }
        }
    }

    /// Hand the reply to `route` — now if it has arrived, else from wherever
    /// it does: for a request's, inside the step that reads it.
    pub fn then(self, route: impl FnOnce(Result<T, ClientError>) + Send + 'static) {
        let mut state = self.0.lock();
        match std::mem::take(&mut *state) {
            Awaited::Arrived(reply) => {
                drop(state);
                route(reply)
            }
            _ => *state = Awaited::Routed(Box::new(route)),
        }
    }
}

impl<T> Future for Sent<T> {
    type Output = Result<T, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.0.lock();
        match std::mem::take(&mut *state) {
            Awaited::Arrived(reply) => Poll::Ready(reply),
            _ => {
                *state = Awaited::Watched(cx.waker().clone());
                Poll::Pending
            }
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

/// A delta poll's results, queued for its reader in item order: each view's
/// blocks, then its one [`Polled::End`], however the poll ends. And what was
/// pushed for each subscription the session holds, kept until its owner takes
/// it.
#[derive(Default)]
struct Polls {
    /// The poll whose reader is live. A slot of any other drops what it is fed.
    live: u64,
    queue: VecDeque<Polled>,
    /// By subscription id. An id with no entry is one nobody holds.
    subs: HashMap<u64, Parked>,
    /// The last subscription id handed out.
    asked: u64,
}

/// The blocks pushed for one subscription and not yet taken, and the cursor
/// past everything pushed — or the fault that ended the subscription.
type Parked = Result<(Vec<RawBlock>, DeltaCursor), ClientError>;

impl Polls {
    fn hand(&mut self, poll: u64, polled: Polled) {
        if poll == self.live {
            self.queue.push_back(polled);
        }
    }

    /// One train of `sub` arrived whole: its blocks and the cursor past them,
    /// or the fault that ends the subscription. One nobody holds is dropped.
    fn park(&mut self, sub: u64, train: Result<(Vec<RawBlock>, DeltaCursor), ClientError>) {
        let Some(parked) = self.subs.get_mut(&sub) else {
            return;
        };
        match (parked.as_mut(), train) {
            (Ok((blocks, end)), Ok((more, cursor))) => {
                blocks.extend(more);
                *end = cursor;
            }
            (Ok(_), Err(why)) => *parked = Err(why),
            (Err(_), _) => {}
        }
    }

    /// Whether a subscription holds a train or an end nobody took yet.
    fn untaken(&self) -> bool {
        let quiet = |parked: &Parked| matches!(parked, Ok((blocks, _)) if blocks.is_empty());
        self.subs.values().any(|parked| !quiet(parked))
    }

    /// Move the subscriptions up to `asked` to `round`, the answer of their
    /// sync. Round 0 is that of a connection the server holds none for.
    fn sync_through(&mut self, asked: u64, round: u64) {
        let Some(round) = NonZeroU64::new(round) else {
            return;
        };
        for (_, parked) in self.subs.iter_mut().filter(|(id, _)| **id <= asked) {
            if let Ok((_, cursor)) = parked {
                cursor.tick = cursor.tick.max(round);
            }
        }
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
    /// The error every request gets once the session has ended: what ended
    /// it. `None` while it is open.
    ended: Option<ClientError>,
    /// The last read filled its window, so more may be waiting.
    unread: bool,
    polls: Polls,
    /// The pushed train being read.
    pushed: Option<Slot>,
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
            polls: Polls::default(),
            pushed: None,
        }
    }

    /// Requests submitted since this session connected — one per `submit`,
    /// however the bytes were batched. HELLO predates the session, uncounted.
    pub fn requests_sent(&self) -> u64 {
        self.submitted
    }

    /// Blocks parked for a subscription and not yet taken.
    #[cfg(test)]
    pub(crate) fn parked_blocks(&self) -> usize {
        let held = self.polls.subs.values().filter_map(|p| p.as_ref().ok());
        held.map(|(blocks, _)| blocks.len()).sum()
    }

    /// The connection's socket, as a number: valid while this session lives.
    pub fn as_raw_fd(&self) -> RawFd {
        self.transport.as_raw_fd()
    }

    /// The connection's socket, for a [`Host`](crate::Host) to register.
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.transport.as_fd()
    }

    // ── Submitting and stepping ────────────────────────────────────────────

    /// Encode and queue `req`. Raises at either cap, as every submit does.
    pub fn submit(&mut self, req: Request<'_>) -> Result<Sent<u64>, ClientError> {
        let (frame, tid) = req.encode()?;
        let (to, sent) = promise();
        self.enqueue(frame, Slot::Ack { tid, to })?;
        Ok(sent)
    }

    /// RESOLVE — describe the relation named by the canonical
    /// `"schema_name.relation_name"`; `None` when there is none.
    pub fn submit_resolve(&mut self, qname: &str) -> Result<Sent<Option<Arc<RelDescriptor>>>, ClientError> {
        let hdr = ControlHeader::naming(ClientVerb::Resolve, Target::from(0), 0);
        let (to, sent) = promise();
        self.enqueue(encode_frame(hdr, qname.as_bytes(), None, None), Slot::Resolve { to })?;
        Ok(sent)
    }

    /// SCAN_SPEC, replied in `reply_schema`'s layout.
    pub fn submit_scan(
        &mut self,
        target: Target,
        spec: &gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<Sent<ScanReply>, ClientError> {
        let hdr = ControlHeader::naming(ClientVerb::ScanSpec, target, reply_schema.layout_digest());
        let (to, sent) = promise();
        let slot = Slot::Scan {
            tid: target.tid,
            reply_schema: Arc::clone(reply_schema),
            data: None,
            to,
        };
        self.enqueue(encode_frame(hdr, &spec.encode(), None, None), slot)?;
        Ok(sent)
    }

    /// SCAN_MULTI: every row of N relations at one cut, each replied in the
    /// layout of the schema paired with it, in request order.
    pub fn submit_scan_multi(&mut self, rels: Vec<(u64, Arc<Schema>)>) -> Result<Sent<Vec<ScanReply>>, ClientError> {
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
        let (to, sent) = promise();
        let slot = Slot::Multi {
            replies: Vec::with_capacity(rels.len()),
            rels,
            data: None,
            to,
        };
        self.enqueue(txn_frame::encode_scan_multi(&items), slot)?;
        Ok(sent)
    }

    /// DELTA_POLL of one view, replied in `reply_schema`'s layout.
    pub fn submit_delta_read(
        &mut self,
        item: txn_frame::DeltaPollItem<'_>,
        reply_schema: &Arc<Schema>,
    ) -> Result<Sent<(ScanReply, DeltaCursor)>, ClientError> {
        let (to, sent) = promise();
        let slot = Slot::Delta {
            tid: item.view.tid,
            reply_schema: Arc::clone(reply_schema),
            data: None,
            to,
        };
        self.enqueue(txn_frame::encode_delta_poll(&[item]), slot)?;
        Ok(sent)
    }

    /// SUBSCRIBE to each item's view from its cursor on, and return their
    /// ids, in order. Nothing waits for the answer: a refused item ends its
    /// subscription as a pushed train does.
    pub(crate) fn subscribe(&mut self, items: &[txn_frame::DeltaPollItem]) -> Result<Range<u64>, ClientError> {
        let from = |item: &txn_frame::DeltaPollItem| {
            DeltaCursor::from_pair(item.tag, item.after_tick).ok_or_else(|| {
                ClientError::from("a subscription continues a cursor; read the view whole first".to_string())
            })
        };
        let cursors: Vec<DeltaCursor> = items.iter().map(from).collect::<Result<_, _>>()?;
        let first = self.polls.asked + 1;
        let ids = first..first + items.len() as u64;
        self.enqueue(
            txn_frame::encode_subscribe(items, first),
            Slot::Subscribe { ids: ids.clone() },
        )?;
        self.polls.asked = ids.end - 1;
        let parked = ids
            .clone()
            .zip(cursors)
            .map(|(id, cursor)| (id, Ok((Vec::new(), cursor))));
        self.polls.subs.extend(parked);
        Ok(ids)
    }

    /// End each subscription of `ids` this session holds. Nothing waits for
    /// the answer: a train of one still on its way is dropped, and one the
    /// session refuses to end is ended with its connection.
    pub(crate) fn unsubscribe(&mut self, ids: impl IntoIterator<Item = u64>) {
        for id in ids {
            if self.polls.subs.remove(&id).is_some() {
                let hdr = ControlHeader::naming(ClientVerb::Unsubscribe, Target::from(0), id);
                let (to, _) = promise();
                let _ = self.enqueue(encode_frame(hdr, &[], None, None), Slot::Ack { tid: 0, to });
            }
        }
        // What the socket takes now; the rest rides with the next request.
        self.step(Interest::WRITE);
    }

    /// SYNC_PUSHED: bring this connection's subscriptions up to date, held up
    /// to `wait` while none has anything new.
    pub(crate) fn submit_sync(&mut self, wait: Duration) -> Sent<()> {
        if self.polls.subs.is_empty() {
            return Sent::ready(Ok(()));
        }
        let wait = if self.polls.untaken() { Duration::ZERO } else { wait };
        let hdr = ControlHeader::naming(ClientVerb::SyncPushed, Target::from(0), wait_ms(wait));
        let (to, sent) = promise();
        let slot = Slot::Sync { asked: self.polls.asked, to };
        match self.enqueue(encode_frame(hdr, &[], None, None), slot) {
            Ok(()) => sent,
            Err(why) => Sent::ready(Err(why)),
        }
    }

    /// What was pushed for subscription `id` since it was last taken: the
    /// blocks in arrival order, and the cursor past everything pushed.
    ///
    /// `Err` is the end of the subscription, which the session then holds no
    /// more: the server ended it, or this connection never held it.
    pub(crate) fn take_pushed(&mut self, id: u64) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        let Entry::Occupied(mut parked) = self.polls.subs.entry(id) else {
            return Err(ClientError::from(format!(
                "subscription {id} is not held on this connection; read from its cursor and subscribe again"
            )));
        };
        match parked.get_mut() {
            Ok((blocks, cursor)) => Ok((std::mem::take(blocks), *cursor)),
            Err(_) => parked.remove(),
        }
    }

    /// This session, handing out no subscription id `old` did.
    pub(crate) fn replacing(mut self, old: &Session) -> Self {
        self.polls.asked = old.polls.asked;
        self
    }

    /// DELTA_POLL: one train per view of `views`, in order — the items of the
    /// live poll. Their results queue for [`Self::next_polled`] as the steps that read
    /// them run; a request the session refuses ends each view with the refusal.
    /// No view is no request.
    pub(crate) fn submit_delta_poll(&mut self, views: &[txn_frame::DeltaPollItem]) {
        if views.is_empty() {
            return;
        }
        let slot = Slot::DeltaPoll {
            views: views.iter().map(|v| v.view.tid).collect(),
            at: 0,
            poll: self.polls.live,
        };
        if let Err(why) = self.enqueue(txn_frame::encode_delta_poll(views), slot) {
            let ends = views.iter().map(|_| Polled::End(Err(why.clone())));
            self.polls.queue.extend(ends);
        }
    }

    fn enqueue(&mut self, frame: Vec<u8>, slot: Slot) -> Result<(), ClientError> {
        // A frame past the ceiling is refused here rather than by the server's
        // ingress cap, which would drop the connection.
        let (total, limit) = (frame.len(), gnitz_wire::MAX_FRAME_PAYLOAD);
        if total > limit {
            return Err(ClientError::from(format!(
                "request frame is {total} bytes, exceeding the {limit}-byte server ingress cap; \
                 split the request"
            )));
        }
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
        self.pending.push_back(slot);
        Ok(())
    }

    /// Do the I/O `ready` allows — one read, then one flush; every reply it
    /// completes arrives. Afterwards a driver may park on `interest()` unless
    /// [`Self::unread`]: nothing buffered can advance, and bytes still queued
    /// are ones the fd refused.
    ///
    /// A failure ends the session: every reply still owed arrives as
    /// [`ClientError::ConnectionLost`], after those that completed.
    pub fn step(&mut self, ready: Interest) {
        if ready.read {
            self.read();
        }
        // Last, so the ciphertext a read queues goes out with this flush.
        if ready.write && self.ended.is_none() {
            if let Err(e) = self.transport.flush() {
                // A peer gone after answering is still readable.
                self.read();
                while self.unread {
                    self.read();
                }
                self.end(ClientError::ConnectionLost(e));
            }
        }
    }

    /// One read of the socket, each frame it completes fed to the head slot. A
    /// failure ends the session.
    fn read(&mut self) {
        self.unread = false;
        if self.ended.is_some() {
            return;
        }
        let Session { transport, pending, polls, pushed, .. } = self;
        match transport.read(|buf| feed(pending, pushed, buf, polls)) {
            Ok(more) => self.unread = more,
            Err(e) => self.end(ClientError::ConnectionLost(e)),
        }
    }

    /// The live delta poll's next result.
    pub(crate) fn next_polled(&mut self) -> Option<Polled> {
        self.polls.queue.pop_front()
    }

    /// Everything the last step queued has been handed out.
    pub(crate) fn polled_out(&self) -> bool {
        self.polls.queue.is_empty()
    }

    /// The live poll's reader is gone: drop what it was handed, and what its
    /// slots are still to be fed.
    pub(crate) fn abandon_poll(&mut self) {
        self.polls.live += 1;
        self.polls.queue.clear();
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

    /// Whether this session has ended, and refuses work.
    pub fn is_closed(&self) -> bool {
        self.ended.is_some()
    }

    /// Fail every pending slot with `why` and refuse further work. The shutdown
    /// shows the server EOF now rather than when the session drops.
    pub(crate) fn end(&mut self, why: ClientError) {
        if self.ended.is_some() {
            return;
        }
        self.transport.close();
        let Session { pending, polls, .. } = self;
        for slot in pending.drain(..) {
            slot.fail(why.clone(), polls);
        }
        self.ended = Some(why);
    }
}

/// `wait` in the wire's milliseconds, rounded up: a wait shorter than one is
/// still a wait.
fn wait_ms(wait: Duration) -> u64 {
    u64::try_from(wait.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX)
}

/// Feed one frame to the slot it is of — the pushed train it opens or the one
/// open, else the head — whose reply arrives if that completes it.
fn feed(
    pending: &mut VecDeque<Slot>,
    pushed: &mut Option<Slot>,
    buf: Cow<'_, [u8]>,
    polls: &mut Polls,
) -> Result<(), ProtocolError> {
    let ctrl = peek_control_block(&buf).map_err(ProtocolError::DecodeError)?;
    if ctrl.hdr.flags.pushed {
        let train = Slot::Pushed {
            tid: ctrl.hdr.target_id,
            sub: ctrl.hdr.arg0,
            blocks: Vec::new(),
        };
        return match pushed.replace(train) {
            None => Ok(()),
            Some(_) => Err(ProtocolError::DecodeError(
                "a pushed train opened inside another".into(),
            )),
        };
    }
    let in_train = pushed.is_some();
    let Some(slot) = pushed.as_mut().or(pending.front_mut()) else {
        return Err(ProtocolError::DecodeError("reply frame with no request pending".into()));
    };
    if let Some(answered) = slot.feed(ctrl, buf, polls)? {
        let slot = if in_train { pushed.take() } else { pending.pop_front() };
        if let Err(why) = answered {
            slot.expect("the slot was just fed").fail(why, polls);
        }
    }
    Ok(())
}

impl Slot {
    /// Take one reply frame. `Some` once the slot is answered: its reply has
    /// arrived, or this refusal ended the request. `Err` is a frame this slot
    /// cannot have been sent, which ends the session.
    fn feed(
        &mut self,
        ctrl: DecodedControl,
        mut buf: Cow<'_, [u8]>,
        polls: &mut Polls,
    ) -> Result<Option<Result<(), ClientError>>, ProtocolError> {
        let named = ctrl.hdr.target_id;
        // The relation this frame must name. Replies arrive in request order,
        // so a frame naming another would decode under the wrong schema
        // silently; make it loud.
        let want = match &*self {
            Slot::Ack { tid, .. } | Slot::Scan { tid, .. } | Slot::Delta { tid, .. } | Slot::Pushed { tid, .. } => {
                Some(*tid)
            }
            Slot::Subscribe { .. } | Slot::Sync { .. } => Some(0),
            Slot::Multi { rels, replies, .. } => Some(rels[replies.len()].0),
            Slot::DeltaPoll { views, at, .. } => Some(views[*at]),
            Slot::Resolve { .. } => None,
        };
        if let Some(fault) = ctrl.fault(&buf) {
            let refused = ClientError::Refused(fault);
            // A DELTA_POLL fault naming a view ends that view's position alone;
            // every other fault ends the request.
            return match (&mut *self, want) {
                (Slot::DeltaPoll { views, at, poll }, Some(view)) if named != 0 => {
                    if named != view {
                        return Err(out_of_order(view, named));
                    }
                    polls.hand(*poll, Polled::End(Err(refused)));
                    *at += 1;
                    Ok((*at == views.len()).then_some(Ok(())))
                }
                _ => Ok(Some(Err(refused))),
            };
        }
        if let Some(want) = want.filter(|&want| want != named) {
            return Err(out_of_order(want, named));
        }
        // Only a RESOLVE is answered in the server's schema; every read decodes
        // under the schema its request named.
        if ctrl.schema.is_some() && !matches!(self, Slot::Resolve { .. }) {
            return Err(ProtocolError::DecodeError(
                "a schema block on a reply whose request named its schema".into(),
            ));
        }

        // Rows are decoded straight into the slot: a train carries one data
        // frame per worker, and a per-frame batch would be copied in and dropped.
        let decode = |data: &mut Option<ZSetBatch>, schema: &Arc<Schema>, block: &[u8]| {
            decode_wal_block_into(data.get_or_insert_with(|| ZSetBatch::new(schema)), block, schema)
        };
        match (&mut *self, ctrl.data.clone()) {
            (_, None) => {}
            // An abandoned poll's block is dropped uncopied.
            (Slot::DeltaPoll { poll, .. }, Some(block)) if *poll == polls.live => {
                let frame = std::mem::take(&mut buf).into_owned();
                polls.hand(*poll, Polled::Block(RawBlock { frame, block }));
            }
            (Slot::DeltaPoll { .. }, Some(_)) => {}
            (Slot::Pushed { blocks, .. }, Some(block)) => {
                let frame = std::mem::take(&mut buf).into_owned();
                blocks.push(RawBlock { frame, block });
            }
            (Slot::Scan { reply_schema, data, .. } | Slot::Delta { reply_schema, data, .. }, Some(r)) => {
                decode(data, reply_schema, &buf[r])?
            }
            (Slot::Multi { rels, replies, data, .. }, Some(r)) => decode(data, &rels[replies.len()].1, &buf[r])?,
            (Slot::Ack { .. } | Slot::Resolve { .. } | Slot::Subscribe { .. } | Slot::Sync { .. }, Some(_)) => {
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
        let cursor = || {
            DeltaCursor::from_pair(ctrl.hdr.arg1, ctrl.hdr.arg0)
                .ok_or_else(|| ProtocolError::DecodeError("a delta-poll terminal at round 0".into()))
        };
        match self {
            Slot::Ack { to, .. } => to.fulfil(Ok(ctrl.hdr.arg0)),
            Slot::Resolve { to } => to.fulfil(Ok(resolve_descriptor(&ctrl, &buf)?)),
            Slot::Scan { reply_schema, data, to, .. } => to.fulfil(Ok(scan_reply(reply_schema, data))),
            Slot::Delta { reply_schema, data, to, .. } => {
                let reply = ScanReply {
                    lsn: None,
                    ..scan_reply(reply_schema, data)
                };
                to.fulfil(Ok((reply, cursor()?)))
            }
            Slot::Multi { rels, replies, data, to } => {
                let reply = scan_reply(&rels[replies.len()].1, data);
                replies.push(reply);
                if replies.len() < rels.len() {
                    return Ok(None);
                }
                to.fulfil(Ok(std::mem::take(replies)))
            }
            Slot::DeltaPoll { views, at, poll } => {
                polls.hand(*poll, Polled::End(Ok(cursor()?)));
                *at += 1;
                if *at < views.len() {
                    return Ok(None);
                }
            }
            Slot::Subscribe { .. } => {}
            Slot::Sync { asked, to } => {
                polls.sync_through(*asked, ctrl.hdr.arg0);
                to.fulfil(Ok(()))
            }
            Slot::Pushed { sub, blocks, .. } => polls.park(*sub, Ok((std::mem::take(blocks), cursor()?))),
        }
        Ok(Some(Ok(())))
    }

    /// The request ended unanswered: `why` is its reply, the end of each view
    /// a delta poll had yet to answer, and the end of each subscription a
    /// SUBSCRIBE asked for or a pushed train is of.
    fn fail(self, why: ClientError, polls: &mut Polls) {
        match self {
            Slot::Ack { mut to, .. } => to.fulfil(Err(why)),
            Slot::Resolve { mut to } => to.fulfil(Err(why)),
            Slot::Scan { mut to, .. } => to.fulfil(Err(why)),
            Slot::Delta { mut to, .. } => to.fulfil(Err(why)),
            Slot::Multi { mut to, .. } => to.fulfil(Err(why)),
            Slot::DeltaPoll { views, at, poll } => {
                for _ in at..views.len() {
                    polls.hand(poll, Polled::End(Err(why.clone())));
                }
            }
            Slot::Subscribe { ids } => ids.for_each(|id| polls.park(id, Err(why.clone()))),
            Slot::Sync { mut to, .. } => to.fulfil(Err(why)),
            Slot::Pushed { sub, .. } => polls.park(sub, Err(why)),
        }
    }
}

/// A reply frame naming a relation the head slot's position does not expect.
fn out_of_order(want: u64, got: u64) -> ProtocolError {
    ProtocolError::DecodeError(format!("reply out of order: expected target {want}, got {got}"))
}

/// A RESOLVE train as its descriptor; `None` when the reply names no relation.
fn resolve_descriptor(ctrl: &DecodedControl, frame: &[u8]) -> Result<Option<Arc<RelDescriptor>>, ProtocolError> {
    if ctrl.hdr.target_id == 0 {
        return Ok(None);
    }
    let schema = ctrl
        .schema
        .clone()
        .ok_or_else(|| ProtocolError::DecodeError("RESOLVE reply carries no schema".into()))?;
    let schema = Arc::new(Schema::from_block(&frame[schema]).map_err(ProtocolError::DecodeError)?);
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

#[cfg(test)]
#[path = "benches/connection.rs"]
mod bench;
