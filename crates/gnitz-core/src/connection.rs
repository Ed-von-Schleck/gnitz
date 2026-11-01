//! The protocol session: a connection state machine. [`Session::step`] does
//! its I/O without waiting; [`Session::turn`] waits on the client's [`Host`]
//! only when the socket must report readiness first.
//!
//! A frame is of what its [`WireLane`] names. Replies arrive in request order,
//! each the head slot's until its last train terminates; a pushed train and
//! the answer to a sync arrive whole between two replies.

use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};

use crate::batch::{decode_wal_blocks_into, parse_wal_block};
use crate::error::ClientError;
use crate::host::{promise, Host, Promise, Sent};
use crate::transport::Interest;
use crate::{sys_schema, ClientTransport, ProtocolError, Schema, ZSetBatch};
use gnitz_wire::txn_frame::{self, encode_items, kept_as, FrameItem};
use gnitz_wire::{
    append_frame, peek_control_block, ClientVerb, ControlHeader, DecodedControl, PkBuf, RelClass, RelDescriptorBlob,
    RelIndex, WireConflictMode, WireLane, WireStatus, CONNECT_TIMEOUT,
};

/// Requests one connection may hold in flight; `submit` refuses past it. A bound
/// on the memory a driver that never waits can pin, not a throughput knob.
pub const MAX_IN_FLIGHT: usize = 4096;

/// Unwritten frame bytes one connection may hold; `submit` refuses past it. An
/// encoded push is a full copy of its batch, so the count above bounds no
/// memory on its own. Checked before queueing, so one frame up to
/// [`gnitz_wire::MAX_FRAME_PAYLOAD`] always goes through — what it bounds is a driver that submits
/// without flushing.
pub const MAX_QUEUED_BYTES: usize = 64 << 20;

/// One relation's read result.
#[derive(Debug)]
pub struct ScanReply {
    pub schema: Arc<Schema>,
    pub batch: ZSetBatch,
    /// The server LSN the read was served at. `None` where the rows are no
    /// point read of the log: a delta read or a pushed train, whose position is
    /// its cursor, an answer off a mirrored copy, a constant, `EXPLAIN` or
    /// `RETURNING`.
    pub lsn: Option<u64>,
}

/// One page of a snapshot read.
#[derive(Debug)]
pub(crate) struct SnapshotPage {
    /// Each train's rows in key order, one train after another.
    pub(crate) batch: ZSetBatch,
    /// Where each train's rows end in `batch`.
    pub(crate) worker_ends: Vec<usize>,
}

/// One relation, as a RESOLVE describes it.
#[derive(Debug)]
pub struct RelDescriptor {
    pub tid: u64,
    pub class: RelClass,
    /// A stream, or a view whose planner set [`gnitz_wire::ViewTabRow::pk_repeats`].
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

pub use gnitz_wire::Target;

impl From<&RelDescriptor> for Target {
    fn from(rel: &RelDescriptor) -> Self {
        Target { tid: rel.tid, token: rel.token }
    }
}

/// One reply frame's data block, kept undecoded: the owned frame buffer and the
/// block's extent within it.
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

pub use gnitz_wire::txn_frame::{DeltaCursor, SubId};

// ── The request vocabulary ───────────────────────────────────────────────────

/// A request answered by one value — an LSN, or the base id of a run. It
/// borrows its inputs; the borrow ends at [`Session::submit`].
pub(crate) enum Request<'a> {
    /// A run of this many catalog object ids.
    AllocIds(u64),
    /// A run of `count` ids from the SERIAL sequence of `table`.
    AllocSerial { table: Target, count: u64 },
    /// An atomic DDL transaction: system-table batches, each named by its table
    /// id, under one durable SAL zone.
    DdlTxn(&'a [(u64, ZSetBatch)]),
    /// An atomic user-table push transaction: the server refuses it if some
    /// family's relation was written after that family's `basis`.
    PushTxn(&'a [PushFamily]),
    /// PUSH. The frame always carries `schema`'s record.
    Push {
        target: Target,
        schema: &'a Schema,
        batch: &'a ZSetBatch,
        mode: WireConflictMode,
    },
    /// SNAPSHOT_OPEN; the ACK names the snapshot.
    SnapshotOpen,
    /// SNAPSHOT_CLOSE of this snapshot.
    SnapshotClose(u64),
    /// BACKUP into the backup directory's entry of this name.
    Backup(&'a str),
}

impl Request<'_> {
    /// Validate: the relation its ACK names.
    fn validate(&self) -> Result<u64, ClientError> {
        match *self {
            Request::AllocSerial { table, .. } => Ok(table.tid),
            Request::DdlTxn(families) => {
                for (tid, batch) in families {
                    if gnitz_wire::sys_family_index(*tid).is_none() {
                        return Err(ClientError::from(format!("DDL family {tid} is not a system table")));
                    }
                    batch.validate(sys_schema(*tid))?;
                }
                Ok(0)
            }
            Request::PushTxn(families) => {
                for f in families {
                    f.batch.validate(&f.schema)?;
                }
                Ok(0)
            }
            Request::Push { target, schema, batch, .. } => {
                // In-process, so a convenience and never a trust boundary; the
                // server checks the same things. Here so no driver has to
                // remember to.
                batch.validate(schema)?;
                Ok(target.tid)
            }
            Request::AllocIds(_) | Request::SnapshotOpen | Request::SnapshotClose(_) | Request::Backup(_) => Ok(0),
        }
    }

    /// Append the frame payload to `out`.
    fn append(self, out: &mut Vec<u8>) {
        // A control-only request: its verb, the relation it names, `arg0`, and its blob.
        let (verb, target, arg0, blob): (ClientVerb, Target, u64, &[u8]) = match self {
            Request::AllocIds(n) => (ClientVerb::AllocIds, Target::from(0), n, &[]),
            Request::AllocSerial { table, count } => (ClientVerb::AllocSerialRange, table, count, &[]),
            Request::SnapshotOpen => (ClientVerb::SnapshotOpen, Target::from(0), 0, &[]),
            Request::SnapshotClose(id) => (ClientVerb::SnapshotClose, Target::from(0), id, &[]),
            Request::Backup(name) => (ClientVerb::Backup, Target::from(0), 0, name.as_bytes()),
            Request::DdlTxn(families) => return append_ddl_txn(out, families),
            Request::PushTxn(families) => return append_push_txn(out, families),
            Request::Push { target, schema, batch, mode } => {
                let mut hdr = ControlHeader::naming(ClientVerb::Push, target, 0);
                hdr.flags.conflict_mode = mode;
                return append_payload(out, hdr, &[], Some(&schema.to_block()), Some(batch));
            }
        };
        append_payload(out, ControlHeader::naming(verb, target, arg0), blob, None, None)
    }
}

/// Append one frame payload, without the 4-byte length prefix, to `out`. An
/// empty batch ships no data block, and still ships the schema record.
pub(crate) fn append_payload(
    out: &mut Vec<u8>,
    hdr: ControlHeader,
    blob: &[u8],
    schema: Option<&[u8]>,
    data: Option<&ZSetBatch>,
) {
    append_frame(
        out,
        &hdr,
        blob,
        schema,
        data.filter(|b| !b.is_empty()).map(|b| b.wire_regions()).as_deref(),
    );
}

/// One family of a user-table push transaction: a maximal run of same-mode
/// writes into `target`.
pub struct PushFamily {
    /// The relation, under the descriptor token of the first of the family's
    /// batches built from a descriptor; `0` while none was.
    pub target: Target,
    pub schema: Arc<Schema>,
    /// May still be shared with the caller that pushed it, so it is never
    /// written in place.
    pub batch: Arc<ZSetBatch>,
    pub mode: WireConflictMode,
    /// The commit fails if `target` was written after `basis`, the oldest
    /// watermark among the reads this family's writes were built from; `BLIND`
    /// when none was.
    pub basis: u64,
}

/// Append an atomic user-table push transaction frame (`ClientVerb::PushTxn`) to
/// `out` (without the 4-byte frame header). Every family carries its schema
/// record, which the master validates it against.
pub(crate) fn append_push_txn(out: &mut Vec<u8>, families: &[PushFamily]) {
    let schemas: Vec<Vec<u8>> = families.iter().map(|f| f.schema.to_block()).collect();
    let items: Vec<FrameItem> = families
        .iter()
        .zip(&schemas)
        .map(|(f, schema)| FrameItem {
            target: f.target,
            conflict_mode: f.mode,
            basis: f.basis,
            schema: Some(schema),
            data: f.batch.wire_regions(),
        })
        .collect();
    encode_items(out, ClientVerb::PushTxn, &items)
}

/// Encode an atomic DDL transaction frame (`ClientVerb::DdlTxn`) into wire bytes
/// (without the 4-byte frame header). Each family is named by its system table id
/// alone; its batch carries its own layout, which `Session::submit` validates
/// against that id's system schema.
pub fn encode_ddl_txn(families: &[(u64, ZSetBatch)]) -> Vec<u8> {
    let mut out = Vec::new();
    append_ddl_txn(&mut out, families);
    out
}

/// Append the frame [`encode_ddl_txn`] encodes to `out`.
fn append_ddl_txn(out: &mut Vec<u8>, families: &[(u64, ZSetBatch)]) {
    let items: Vec<FrameItem> = families
        .iter()
        .map(|(tid, b)| FrameItem {
            target: Target::from(*tid),
            conflict_mode: WireConflictMode::default(),
            basis: 0,
            schema: None,
            data: b.wire_regions(),
        })
        .collect();
    encode_items(out, ClientVerb::DdlTxn, &items)
}

/// One request in flight, or a pushed train: how its reply decodes, the reply
/// read so far, and where it goes. A slot is answered only while it heads its
/// queue, so its state is the one train in progress, and it goes when the slot
/// does.
enum Slot {
    /// A control-only ACK naming `tid`, its value in `arg0`.
    Ack { tid: u64, to: Promise<u64> },
    /// RESOLVE: the descriptor, or `None` when no such relation exists.
    /// Uncorrelated on the wire, because the request names no id.
    Resolve { to: Promise<Option<Arc<RelDescriptor>>> },
    /// SCAN_SPEC or SCAN_MULTI: one train per relation, in request order.
    Scan(Scan),
    /// One view's delta read: a scan's train, whose terminal carries the cursor
    /// to read from next.
    Delta {
        train: Train,
        /// The subscription id the view is kept as from that cursor.
        keep: Option<SubId>,
        to: Promise<(ScanReply, DeltaCursor)>,
    },
    /// SNAPSHOT_PAGE: a scan's trains, with where each worker's ends.
    Page {
        train: Train,
        /// The row count at the end of each worker's train so far.
        worker_ends: Vec<usize>,
        to: Promise<SnapshotPage>,
    },
    /// One train per view, its blocks kept raw; the train in progress is
    /// `views[at]`, handed to the poll's reader while it is live.
    DeltaPoll {
        views: Vec<u64>,
        at: usize,
        /// The poll this request is, see [`Polls::live`].
        poll: u64,
        /// The subscription id `views[0]` is kept as, the rest counting up.
        first: SubId,
    },
    /// A train the server pushed for subscription `sub`, its blocks kept for
    /// whoever holds it.
    Pushed {
        tid: u64,
        sub: SubId,
        blocks: Vec<RawBlock>,
    },
}

/// One relation's train, decoded under the schema its request named when it
/// terminates.
struct Train {
    tid: u64,
    schema: Arc<Schema>,
    /// The continuation frames' blocks, undecoded until the train terminates
    /// so its batch is sized once for every worker's rows.
    held: Vec<RawBlock>,
    /// The rows of every block taken so far, held or terminal.
    rows: usize,
}

impl Train {
    fn new(tid: u64, schema: &Arc<Schema>) -> Self {
        Train {
            tid,
            schema: Arc::clone(schema),
            held: Vec::new(),
            rows: 0,
        }
    }

    /// Take the data block at `block` of `frame`, its header checked at once.
    /// A continuation frame's is held, the frame owned, since a borrowed one
    /// lives in the transport window; the terminal frame's is decoded in place
    /// by [`Self::finish`], behind the held ones.
    fn take(
        &mut self,
        frame: &mut Cow<'_, [u8]>,
        block: std::ops::Range<usize>,
        more: bool,
    ) -> Result<(), ProtocolError> {
        self.rows += parse_wal_block(&frame[block.clone()], &self.schema)?.rows;
        if more {
            let frame = std::mem::take(frame).into_owned();
            self.held.push(RawBlock { frame, block });
        }
        Ok(())
    }

    /// The train's rows: the held blocks, then the terminal frame's block
    /// `last`, decoded in place.
    fn finish(&mut self, last: Option<&[u8]>) -> Result<ZSetBatch, ProtocolError> {
        let mut batch = ZSetBatch::new(&self.schema);
        let held = std::mem::take(&mut self.held);
        decode_wal_blocks_into(&mut batch, held.iter().map(RawBlock::block).chain(last), &self.schema)?;
        Ok(batch)
    }

    fn reply(&mut self, last: Option<&[u8]>, lsn: Option<u64>) -> Result<ScanReply, ProtocolError> {
        Ok(ScanReply {
            batch: self.finish(last)?,
            schema: Arc::clone(&self.schema),
            lsn,
        })
    }
}

/// A scan's trains and where their replies go: SCAN_SPEC answers with its one
/// reply, SCAN_MULTI with every relation's, gathered as each train terminates.
enum Scan {
    One {
        train: Train,
        to: Promise<ScanReply>,
    },
    /// The train in progress is `trains[replies.len()]`.
    All {
        trains: Vec<Train>,
        replies: Vec<ScanReply>,
        to: Promise<Vec<ScanReply>>,
    },
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

/// Where the trains of a delta poll go.
#[derive(Default)]
struct Polls {
    /// The poll whose reader is live.
    live: u64,
    /// The live poll's results, in item order: each view's blocks, then its
    /// one [`Polled::End`], however the poll ends.
    queue: VecDeque<Polled>,
    /// What was pushed for each subscription the session holds, by its id,
    /// kept until its owner takes it. An id with no entry is one nobody holds.
    subs: HashMap<SubId, Parked>,
    /// The server may hold a subscription of this connection: a read it was
    /// asked to keep was answered with a cursor since the last sync, or that
    /// sync named one.
    server_holds: bool,
}

/// The blocks pushed for one subscription and not yet taken, and the cursor
/// past everything pushed — or the fault that ended the subscription.
type Parked = Result<(Vec<RawBlock>, DeltaCursor), ClientError>;

impl Polls {
    /// Hold subscription `id` at `cursor` before the reader takes the reply:
    /// a train pushed for the id may be right behind it.
    fn hold(&mut self, id: SubId, cursor: DeltaCursor) {
        self.server_holds = true;
        self.subs.insert(id, Ok((Vec::new(), cursor)));
    }

    /// One view of the request poll `poll`, kept as `kept`, ended with `end`.
    fn reader_end(&mut self, poll: u64, kept: SubId, end: PollEnd) {
        // A cursor is what the server keeps a subscription from, live reader or not.
        self.server_holds |= end.is_ok();
        if poll != self.live {
            return;
        }
        if let Ok(cursor) = &end {
            self.hold(kept, *cursor);
        }
        self.queue.push_back(Polled::End(end));
    }

    /// A train pushed for `sub` ended with `end`, after `train`.
    fn holder_end(&mut self, sub: SubId, train: Vec<RawBlock>, end: PollEnd) {
        self.server_holds |= end.is_ok();
        match (self.subs.get_mut(&sub), end) {
            (Some(Ok((held, at))), Ok(cursor)) => {
                held.extend(train);
                *at = cursor;
            }
            (Some(parked @ Ok(_)), Err(why)) => *parked = Err(why),
            _ => {}
        }
    }

    /// Whether a subscription holds a train or an end nobody took yet.
    fn untaken(&self) -> bool {
        let quiet = |parked: &Parked| matches!(parked, Ok((blocks, _)) if blocks.is_empty());
        self.subs.values().any(|parked| !quiet(parked))
    }
}

/// What inbound frames are fed to: the reply and sync lanes, the pushed train
/// being read, and the delta-poll/subscription routing.
#[derive(Default)]
struct Inbox {
    pending: VecDeque<Slot>,
    /// The syncs sent and not yet answered, oldest first.
    syncing: VecDeque<Promise<()>>,
    /// The pushed train being read.
    pushed: Option<Slot>,
    polls: Polls,
}

impl Inbox {
    /// Feed one frame to the slot it is of, whose reply arrives if that completes it.
    fn feed(&mut self, buf: Cow<'_, [u8]>) -> Result<(), ProtocolError> {
        let ctrl = peek_control_block(&buf).map_err(ProtocolError::DecodeError)?;
        let in_train = self.pushed.is_some();
        match ctrl.hdr.flags.lane {
            WireLane::Reply => {}
            _ if in_train => {
                return Err(ProtocolError::DecodeError(
                    "a pushed train split by a frame that is not of it".into(),
                ))
            }
            WireLane::PushedTrain => {
                let h = &ctrl.hdr;
                if h.flags.continuation
                    || h.status != WireStatus::Ok
                    || ctrl.data.is_some()
                    || ctrl.schema.is_some()
                    || !ctrl.blob.is_empty()
                    || !ctrl.body.is_empty()
                {
                    return Err(ProtocolError::DecodeError(
                        "a pushed train opens with a bare head".into(),
                    ));
                }
                let Some(sub) = SubId::new(h.arg0) else {
                    return Err(ProtocolError::DecodeError(
                        "a pushed train names no subscription".into(),
                    ));
                };
                self.pushed = Some(Slot::Pushed {
                    tid: h.target_id,
                    sub,
                    blocks: Vec::new(),
                });
                return Ok(());
            }
            WireLane::SyncAnswer => {
                let h = &ctrl.hdr;
                if h.target_id != 0 || h.flags.continuation || ctrl.data.is_some() || ctrl.schema.is_some() {
                    return Err(ProtocolError::DecodeError(
                        "a sync answer is one empty frame naming 0".into(),
                    ));
                }
                let Some(mut to) = self.syncing.pop_front() else {
                    return Err(ProtocolError::DecodeError("reply frame with no request pending".into()));
                };
                to.fulfil(ctrl.fault(&buf).map_or(Ok(()), |f| Err(ClientError::Refused(f))));
                return Ok(());
            }
        }
        let Some(slot) = self.pushed.as_mut().or(self.pending.front_mut()) else {
            return Err(ProtocolError::DecodeError("reply frame with no request pending".into()));
        };
        if slot.feed(ctrl, buf, &mut self.polls)? {
            match in_train {
                true => self.pushed = None,
                false => drop(self.pending.pop_front()),
            }
        }
        Ok(())
    }

    /// Requests sent and not yet answered.
    fn in_flight(&self) -> usize {
        self.pending.len() + self.syncing.len()
    }

    /// Fail every unanswered request with `why`.
    fn fail_all(&mut self, why: &ClientError) {
        for mut slot in self.pending.drain(..) {
            slot.fail(why.clone(), &mut self.polls);
        }
        for mut to in self.syncing.drain(..) {
            to.fulfil(Err(why.clone()));
        }
    }
}

/// A protocol session: the transport plus the `Inbox` its frames are fed to
/// (reply, sync and pushed lanes, delta-poll/subscription routing). Exactly one
/// owner of that state: a [`crate::GnitzClient`], whose host decides how it
/// waits.
pub(crate) struct Session {
    transport: ClientTransport,
    inbox: Inbox,
    submitted: u64,
    /// The error every request gets once the session has ended: what ended
    /// it. `None` while it is open.
    ended: Option<ClientError>,
    /// The last read filled its window, so more may be waiting.
    unread: bool,
    /// The last flush left bytes the socket would not take, so the next one
    /// waits for it to be writable.
    refused: bool,
}

/// The connection's socket, for a [`Host`] to register.
impl AsFd for Session {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.transport.as_fd()
    }
}

impl Session {
    /// `target` is an AF_UNIX socket path or a `tls://` target (see
    /// `ClientTransport::connect`).
    pub(crate) fn connect(target: &str) -> Result<Self, ClientError> {
        Ok(Self::over(ClientTransport::connect(
            target,
            Instant::now() + CONNECT_TIMEOUT,
        )?))
    }

    /// A session over a connected transport.
    pub(crate) fn over(transport: ClientTransport) -> Self {
        Session {
            transport,
            inbox: Inbox::default(),
            submitted: 0,
            ended: None,
            unread: false,
            refused: false,
        }
    }

    /// Requests submitted since this session connected — one per `submit`,
    /// however the bytes were batched. HELLO predates the session, uncounted.
    pub(crate) fn requests_sent(&self) -> u64 {
        self.submitted
    }

    // ── Submitting and stepping ────────────────────────────────────────────

    /// Encode and queue `req`. A request this session refuses — one that
    /// does not validate, or at either cap — is answered with the refusal, as
    /// every submit's is.
    pub(crate) fn submit(&mut self, req: Request<'_>) -> Sent<u64> {
        let tid = match req.validate() {
            Ok(tid) => tid,
            Err(why) => return Sent::ready(Err(why)),
        };
        let (to, sent) = promise();
        self.enqueue(|out| req.append(out), Slot::Ack { tid, to });
        sent
    }

    /// RESOLVE — describe the relation named by the canonical
    /// `"schema_name.relation_name"`; `None` when there is none.
    pub(crate) fn submit_resolve(&mut self, qname: &str) -> Sent<Option<Arc<RelDescriptor>>> {
        let hdr = ControlHeader::naming(ClientVerb::Resolve, Target::from(0), 0);
        let (to, sent) = promise();
        self.enqueue(
            |out| append_payload(out, hdr, qname.as_bytes(), None, None),
            Slot::Resolve { to },
        );
        sent
    }

    /// SCAN_SPEC, replied in `reply_schema`'s layout.
    pub(crate) fn submit_scan(
        &mut self,
        target: Target,
        spec: &gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Sent<ScanReply> {
        let hdr = ControlHeader::naming(ClientVerb::ScanSpec, target, reply_schema.layout().layout_digest());
        let (to, sent) = promise();
        let slot = Slot::Scan(Scan::One {
            train: Train::new(target.tid, reply_schema),
            to,
        });
        let spec = spec.encode();
        self.enqueue(|out| append_payload(out, hdr, &spec, None, None), slot);
        sent
    }

    /// SNAPSHOT_PAGE: up to `limit` rows of `tid` as snapshot `snapshot` holds
    /// it, each worker's after its key in `after` — every worker's first rows
    /// with `after` empty — replied in `reply_schema`'s layout.
    pub(crate) fn submit_snapshot_page(
        &mut self,
        snapshot: u64,
        tid: u64,
        after: &[Option<PkBuf>],
        limit: u64,
        reply_schema: &Arc<Schema>,
    ) -> Sent<SnapshotPage> {
        let hdr = ControlHeader::naming(ClientVerb::SnapshotPage, Target::from(tid), snapshot);
        let request = gnitz_wire::snapshot::PageRequest {
            layout: reply_schema.layout().layout_digest(),
            limit,
            after: after.to_vec(),
        };
        let (to, sent) = promise();
        let slot = Slot::Page {
            train: Train::new(tid, reply_schema),
            worker_ends: Vec::new(),
            to,
        };
        let request = request.encode();
        self.enqueue(|out| append_payload(out, hdr, &request, None, None), slot);
        sent
    }

    /// SCAN_MULTI: N [`Self::submit_scan`]s as one request, replied in order.
    pub(crate) fn submit_scan_multi(
        &mut self,
        scans: &[(Target, &gnitz_wire::ReadSpec, &Arc<Schema>)],
    ) -> Sent<Vec<ScanReply>> {
        if scans.is_empty() {
            return Sent::ready(Err(ClientError::from("a multi-read names no relation")));
        }
        let specs: Vec<Vec<u8>> = scans.iter().map(|(_, spec, _)| spec.encode()).collect();
        let items: Vec<txn_frame::ScanItem> = scans
            .iter()
            .zip(&specs)
            .map(|(&(target, _, schema), spec)| txn_frame::ScanItem {
                target,
                reply_layout: schema.layout().layout_digest(),
                spec,
            })
            .collect();
        let (to, sent) = promise();
        let slot = Slot::Scan(Scan::All {
            trains: scans
                .iter()
                .map(|&(target, _, schema)| Train::new(target.tid, schema))
                .collect(),
            replies: Vec::with_capacity(scans.len()),
            to,
        });
        self.enqueue(|out| txn_frame::encode_scan_multi(out, &items), slot);
        sent
    }

    /// DELTA_POLL of one view after `from` — the view whole with none — under
    /// `spec`, replied in `reply_schema`'s layout. With `keep`, a view answered
    /// with a cursor stays subscribed from it under that id.
    pub(crate) fn submit_delta_read(
        &mut self,
        view: Target,
        from: Option<DeltaCursor>,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
        keep: Option<SubId>,
    ) -> Sent<(ScanReply, DeltaCursor)> {
        let (to, sent) = promise();
        let slot = Slot::Delta {
            train: Train::new(view.tid, reply_schema),
            keep,
            to,
        };
        let item = txn_frame::DeltaPollItem {
            view,
            from,
            reply_layout: reply_schema.layout().layout_digest(),
            spec,
        };
        self.enqueue(|out| txn_frame::encode_delta_poll(out, &[item], keep), slot);
        sent
    }

    /// SYNC_PUSHED: bring the subscriptions `held` names up to date, held up
    /// to `wait` while none has anything new, and end every other one of this
    /// connection.
    pub(crate) fn submit_sync(&mut self, held: &[SubId], wait: Duration) -> Sent<()> {
        let polls = &mut self.inbox.polls;
        polls.subs.retain(|id, _| held.contains(id));
        if !polls.server_holds {
            return Sent::ready(Ok(()));
        }
        // An unanswered sync may bring trains nobody is left to take. The wait goes in
        // the wire's milliseconds, rounded up, so a wait shorter than one is still a wait.
        let wait_ms = if polls.untaken() || !self.inbox.syncing.is_empty() {
            0
        } else {
            u64::try_from(wait.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX)
        };
        let hdr = ControlHeader::naming(ClientVerb::SyncPushed, Target::from(0), wait_ms);
        let named = txn_frame::encode_held(polls.subs.keys().copied());
        let (mut to, sent) = promise();
        match self.admit(|out| append_payload(out, hdr, &named, None, None)) {
            Ok(()) => {
                self.inbox.syncing.push_back(to);
                self.inbox.polls.server_holds = !self.inbox.polls.subs.is_empty();
            }
            Err(why) => to.fulfil(Err(why)),
        }
        sent
    }

    /// What was pushed for subscription `id` since it was last taken: the
    /// blocks in arrival order, and the cursor past everything pushed.
    ///
    /// `Err` is the end of the subscription, which the session then holds no
    /// more: the server ended it, or this connection never held it.
    pub(crate) fn take_pushed(&mut self, id: SubId) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        let Entry::Occupied(mut parked) = self.inbox.polls.subs.entry(id) else {
            return Err(ClientError::from(format!(
                "subscription {id} is not held on this connection; read from its cursor and subscribe again"
            )));
        };
        match parked.get_mut() {
            Ok((blocks, cursor)) => Ok((std::mem::take(blocks), *cursor)),
            Err(_) => parked.remove(),
        }
    }

    /// Stop holding subscription `id`: what is pushed for it from now on is
    /// dropped unparked. The next sync still tells the server.
    pub(crate) fn release(&mut self, id: SubId) {
        self.inbox.polls.subs.remove(&id);
    }

    /// DELTA_POLL of `views`, the items of the live poll: each one's train
    /// queues for [`Self::next_polled`], in order, and a request the session
    /// refuses ends each with the refusal. A view answered with a cursor stays
    /// subscribed from it, item `i` as [`kept_as`]`(first, i)`.
    pub(crate) fn submit_delta_poll(&mut self, first: SubId, views: &[txn_frame::DeltaPollItem]) {
        if views.is_empty() {
            return;
        }
        let slot = Slot::DeltaPoll {
            views: views.iter().map(|v| v.view.tid).collect(),
            at: 0,
            poll: self.inbox.polls.live,
            first,
        };
        self.enqueue(|out| txn_frame::encode_delta_poll(out, views, Some(first)), slot);
    }

    /// Queue the frame `encode` appends, whose reply `slot` reads. A request
    /// this session refuses is answered with the refusal.
    fn enqueue(&mut self, encode: impl FnOnce(&mut Vec<u8>), mut slot: Slot) {
        match self.admit(encode) {
            Ok(()) => self.inbox.pending.push_back(slot),
            Err(why) => slot.fail(why, &mut self.inbox.polls),
        }
    }

    /// Queue the frame `encode` appends on the transport, or the refusal this
    /// session answers it with.
    fn admit(&mut self, encode: impl FnOnce(&mut Vec<u8>)) -> Result<(), ClientError> {
        if let Some(why) = &self.ended {
            return Err(why.clone());
        }
        if let Some(why) = self.cap_refusal() {
            return Err(why);
        }
        // A frame past the ceiling is refused here rather than by the server's
        // ingress cap, which would drop the connection.
        self.transport.frame(encode).map_err(|total| {
            ClientError::from(format!(
                "request frame is {total} bytes, exceeding the {}-byte server ingress cap; split the request",
                gnitz_wire::MAX_FRAME_PAYLOAD
            ))
        })?;
        self.submitted += 1;
        Ok(())
    }

    /// Do the I/O `ready` allows — one read, then one flush; every reply it
    /// completes arrives. `turn` decides when to step again without
    /// waiting and when to park on `interest()`.
    ///
    /// A failure ends the session: every reply still owed arrives as
    /// [`ClientError::ConnectionLost`], after those that completed.
    pub(crate) fn step(&mut self, ready: Interest) {
        if ready.read {
            self.read();
        }
        // Last, so the ciphertext a read queues goes out with this flush.
        if ready.write && self.ended.is_none() {
            match self.transport.flush() {
                Ok(()) => self.refused = self.transport.wants_write(),
                Err(e) => {
                    // A peer gone after answering is still readable.
                    self.read();
                    while self.unread {
                        self.read();
                    }
                    self.end(ClientError::ConnectionLost(e));
                }
            }
        }
    }

    /// Step once, waiting on `host` only when the socket must report an edge
    /// first. A read the last step left unfinished is stepped again without
    /// waiting, since the socket has no new edge to report; so is a write
    /// while the last flush left no bytes the socket refused, since a socket
    /// takes bytes until it refuses them, and asking the host first would cost
    /// a wait per request. Otherwise it parks on `interest()`.
    ///
    /// `Closed` when the session will answer nothing more: it ended, or the
    /// reply awaited belongs to a session since replaced.
    pub(crate) fn turn(&mut self, host: &mut dyn Host, cx: &mut Context<'_>) -> Poll<Result<(), ClientError>> {
        if self.unread {
            self.step(Interest::READ);
            return Poll::Ready(Ok(()));
        }
        let want = self.interest();
        if want.is_empty() {
            return Poll::Ready(Err(ClientError::Closed));
        }
        if want.write && !self.refused {
            self.step(Interest::WRITE);
            return Poll::Ready(Ok(()));
        }
        let waited = ready!(host.poll_io(want, cx, &mut |ready| {
            self.step(ready);
            self.interest().write
        }));
        Poll::Ready(match waited {
            // A wait that failed is the connection's end, which every reply owed
            // arrives as; an interrupt leaves the session as it stood.
            Err(e) if !matches!(e, ClientError::Interrupted(_)) => {
                self.end(match e {
                    ClientError::Protocol(lost) => ClientError::ConnectionLost(lost),
                    e => e,
                });
                Ok(())
            }
            waited => waited,
        })
    }

    /// One read of the socket, each frame it completes fed to the head slot. A
    /// failure ends the session.
    fn read(&mut self) {
        self.unread = false;
        if self.ended.is_some() {
            return;
        }
        match self.transport.read(|buf| self.inbox.feed(buf)) {
            Ok(more) => self.unread = more,
            Err(e) => self.end(ClientError::ConnectionLost(e)),
        }
    }

    /// The live delta poll's next result.
    pub(crate) fn next_polled(&mut self) -> Option<Polled> {
        self.inbox.polls.queue.pop_front()
    }

    /// Everything the last step queued has been handed out.
    pub(crate) fn polled_out(&self) -> bool {
        self.inbox.polls.queue.is_empty()
    }

    /// The live poll's reader is gone: drop what it was handed and what its
    /// slots are still to be fed.
    pub(crate) fn abandon_poll(&mut self) {
        self.inbox.polls.live += 1;
        self.inbox.polls.queue.clear();
    }

    /// Whether a cap [`Self::submit`] refuses on is reached. A driver that can
    /// wait leaves the request where it is, which is what turns a cap into
    /// back-pressure rather than an error. Each cap implies its own interest
    /// bit, so gating on it loses no wakeup.
    pub(crate) fn at_capacity(&self) -> bool {
        self.inbox.in_flight() >= MAX_IN_FLIGHT || self.transport.queued_bytes() >= MAX_QUEUED_BYTES
    }

    /// The refusal [`Self::submit`] answers at a reached cap, if one is.
    fn cap_refusal(&self) -> Option<ClientError> {
        if !self.at_capacity() {
            return None;
        }
        let why = match self.inbox.in_flight() >= MAX_IN_FLIGHT {
            true => format!("connection has {MAX_IN_FLIGHT} requests in flight"),
            false => format!(
                "connection has {} unwritten bytes queued, at the {MAX_QUEUED_BYTES}-byte cap",
                self.transport.queued_bytes()
            ),
        };
        Some(ClientError::from(why))
    }

    /// `READ` while any request is unanswered; `WRITE` while bytes remain queued
    /// or rustls has ciphertext to ship. Nothing once the session has ended.
    pub(crate) fn interest(&self) -> Interest {
        if self.ended.is_some() {
            return Interest::NONE;
        }
        Interest {
            read: self.inbox.in_flight() > 0,
            write: self.transport.wants_write(),
        }
    }

    /// Whether this session has ended, and refuses work.
    pub(crate) fn is_closed(&self) -> bool {
        self.ended.is_some()
    }

    /// Fail every unanswered request with `why` and refuse further work. The shutdown
    /// shows the server EOF now rather than when the session drops.
    pub(crate) fn end(&mut self, why: ClientError) {
        if self.ended.is_some() {
            return;
        }
        self.transport.close();
        self.inbox.fail_all(&why);
        self.ended = Some(why);
    }
}

impl Slot {
    /// Take one reply frame; whether that answered the slot, with its reply
    /// or with the refusal this frame is. `Err` is a frame this slot cannot
    /// have been sent, which ends the session.
    fn feed(&mut self, ctrl: DecodedControl, mut buf: Cow<'_, [u8]>, polls: &mut Polls) -> Result<bool, ProtocolError> {
        let named = ctrl.hdr.target_id;
        // The relation this frame must name. Replies arrive in request order,
        // so a frame naming another would decode under the wrong schema
        // silently; make it loud.
        let want = match self {
            Slot::Ack { tid, .. } | Slot::Pushed { tid, .. } => Some(*tid),
            Slot::DeltaPoll { views, at, .. } => Some(views[*at]),
            Slot::Resolve { .. } => None,
            _ => self.train().map(|t| t.tid),
        };
        if let Some(fault) = ctrl.fault(&buf) {
            let refused = ClientError::Refused(fault);
            // A DELTA_POLL fault naming a view ends that view's position alone;
            // every other fault ends the request.
            return match (&mut *self, want) {
                (Slot::DeltaPoll { views, at, poll, first }, Some(view)) if named != 0 => {
                    if named != view {
                        return Err(out_of_order(view, named));
                    }
                    polls.reader_end(*poll, kept_as(*first, *at), Err(refused));
                    *at += 1;
                    Ok(*at == views.len())
                }
                (Slot::Pushed { .. }, Some(view)) if named != 0 && named != view => Err(out_of_order(view, named)),
                _ => {
                    self.fail(refused, polls);
                    Ok(true)
                }
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

        match (&mut *self, ctrl.data.clone()) {
            (_, None) => {}
            (Slot::DeltaPoll { poll, .. }, Some(block)) => {
                if *poll == polls.live {
                    let frame = std::mem::take(&mut buf).into_owned();
                    polls.queue.push_back(Polled::Block(RawBlock { frame, block }));
                }
            }
            (Slot::Pushed { sub, blocks, .. }, Some(block)) => {
                if polls.subs.contains_key(sub) {
                    let frame = std::mem::take(&mut buf).into_owned();
                    blocks.push(RawBlock { frame, block });
                }
            }
            (slot, Some(r)) => match slot.train() {
                Some(train) => train.take(&mut buf, r, ctrl.hdr.flags.continuation)?,
                None => {
                    return Err(ProtocolError::DecodeError(
                        "a data block on a reply that carries no rows".into(),
                    ))
                }
            },
        }

        if let Slot::Page { train, worker_ends, .. } = &mut *self {
            if ctrl.hdr.flags.scan_last {
                worker_ends.push(train.rows);
            }
        }
        if ctrl.hdr.flags.continuation {
            return Ok(false);
        }

        // The train terminated; a train slot's last block is still in `buf`.
        let last = || ctrl.data.clone().map(|block| &buf[block]);
        let cursor = || {
            DeltaCursor::from_pair(ctrl.hdr.arg1, ctrl.hdr.arg0)
                .ok_or_else(|| ProtocolError::DecodeError("a delta-poll terminal at round 0".into()))
        };
        match self {
            Slot::Ack { to, .. } => to.fulfil(Ok(ctrl.hdr.arg0)),
            Slot::Resolve { to } => to.fulfil(Ok(resolve_descriptor(&ctrl, &buf)?)),
            Slot::Scan(Scan::One { train, to }) => to.fulfil(Ok(train.reply(last(), Some(ctrl.hdr.arg0))?)),
            Slot::Scan(Scan::All { trains, replies, to }) => {
                let reply = trains[replies.len()].reply(last(), Some(ctrl.hdr.arg0))?;
                replies.push(reply);
                if replies.len() < trains.len() {
                    return Ok(false);
                }
                to.fulfil(Ok(std::mem::take(replies)))
            }
            Slot::Page { train, worker_ends, to } => {
                let batch = train.finish(last())?;
                // The next page's rows are shared over the stores this one
                // names, so a page naming none is no reply to a page request.
                if worker_ends.is_empty() {
                    return Err(ProtocolError::DecodeError("a snapshot page naming no worker".into()));
                }
                let worker_ends = std::mem::take(worker_ends);
                to.fulfil(Ok(SnapshotPage { batch, worker_ends }))
            }
            Slot::Delta { train, keep, to } => {
                let cursor = cursor()?;
                let reply = train.reply(last(), None)?;
                if let Some(id) = *keep {
                    polls.hold(id, cursor);
                }
                to.fulfil(Ok((reply, cursor)))
            }
            Slot::DeltaPoll { views, at, poll, first } => {
                polls.reader_end(*poll, kept_as(*first, *at), Ok(cursor()?));
                *at += 1;
                if *at < views.len() {
                    return Ok(false);
                }
            }
            Slot::Pushed { sub, blocks, .. } => polls.holder_end(*sub, std::mem::take(blocks), Ok(cursor()?)),
        }
        Ok(true)
    }

    /// The train in progress of a slot that reads rows; `None` for one that
    /// carries none, or keeps its blocks raw.
    fn train(&mut self) -> Option<&mut Train> {
        match self {
            Slot::Scan(Scan::One { train, .. }) | Slot::Delta { train, .. } | Slot::Page { train, .. } => Some(train),
            Slot::Scan(Scan::All { trains, replies, .. }) => trains.get_mut(replies.len()),
            _ => None,
        }
    }

    /// The request ended unanswered: `why` is its reply, and the end of each
    /// view a delta poll had yet to answer.
    fn fail(&mut self, why: ClientError, polls: &mut Polls) {
        match self {
            Slot::Ack { to, .. } => to.fulfil(Err(why)),
            Slot::Resolve { to } => to.fulfil(Err(why)),
            Slot::Scan(Scan::One { to, .. }) => to.fulfil(Err(why)),
            Slot::Scan(Scan::All { to, .. }) => to.fulfil(Err(why)),
            Slot::Page { to, .. } => to.fulfil(Err(why)),
            Slot::Delta { to, .. } => to.fulfil(Err(why)),
            Slot::DeltaPoll { views, at, poll, first } => {
                for at in *at..views.len() {
                    polls.reader_end(*poll, kept_as(*first, at), Err(why.clone()));
                }
            }
            Slot::Pushed { sub, blocks, .. } => polls.holder_end(*sub, std::mem::take(blocks), Err(why)),
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
#[path = "fuzz/connection.rs"]
mod fuzz;

#[cfg(test)]
#[path = "benches/connection.rs"]
mod bench;
