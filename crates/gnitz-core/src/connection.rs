//! The protocol session: a connection state machine. [`Session::step`] does
//! its I/O without waiting; [`Session::turn`] waits on the client's [`Host`]
//! only when the socket must report readiness first.
//!
//! A frame is of the [`ServerLane`] its header names. Replies arrive in request order,
//! each the head slot's until its last train terminates; a pushed train and
//! the answer to a sync arrive whole between two replies.

use gnitz_wire::FrameBytes;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use crate::batch::{FrameBlock, decode_wal_blocks};
use crate::error::ClientError;
use crate::host::{Host, Promise, Sent, promise};
use crate::transport::Interest;
use crate::write_frames::{SpanningWrite, WriteFamily};
use crate::{ClientTransport, ProtocolError, Schema, ZSetBatch};
use gnitz_wire::Target;
use gnitz_wire::multi_frame::{self, DeltaCursor, SubId, kept_as};
use gnitz_wire::{
    CONNECT_TIMEOUT, ClientVerb, ControlHeader, DecodedControl, PkBuf, RelClass, RelDescriptorBlob, RelIndex,
    ServerLane, SysFamily, WireConflictMode, append_frame, peek_control_block,
};
use smallvec::SmallVec;

/// Requests one connection may hold in flight; `submit` refuses past it. A bound
/// on the memory a driver that never waits can pin, not a throughput knob.
pub const MAX_IN_FLIGHT: usize = 4096;

/// Unwritten frame bytes one connection may hold; `submit` refuses past it. An
/// encoded push is a full copy of its batch, so the count above bounds no
/// memory on its own. Checked before queueing, so one request always goes
/// through — what it bounds is a driver that submits without flushing. A write
/// queued behind others, or past the frame ceiling, is held as its batches and
/// counts only its frames encoded.
pub const MAX_QUEUED_BYTES: usize = 64 << 20;

/// One relation's read result.
#[derive(Debug)]
pub struct ScanReply {
    /// The rows, under the schema they are presented in.
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

impl From<&RelDescriptor> for Target {
    fn from(rel: &RelDescriptor) -> Self {
        Target { tid: rel.tid, token: rel.token }
    }
}

/// One reply frame's data block, kept undecoded: the owned frame buffer and the
/// block's extent within it.
pub(crate) type RawBlock = FrameBlock<'static>;

/// Own `frame`'s buffer, taking it out of `frame` (a borrowed frame lives in
/// the transport window), with its data block at `block`.
fn own_block(frame: &mut FrameBytes<'_>, block: std::ops::Range<usize>) -> RawBlock {
    FrameBlock {
        frame: FrameBytes::Owned(std::mem::take(frame).into_owned()),
        block,
    }
}

// ── The request vocabulary ───────────────────────────────────────────────────

/// A request answered by one value — an LSN, or the base id of a run. It
/// borrows its inputs; the borrow ends at [`Session::submit`].
pub(crate) enum Request<'a> {
    /// A run of this many catalog object ids.
    AllocIds(u64),
    /// A run of `count` ids from the SERIAL sequence of `table`.
    AllocSerial { table: Target, count: u64 },
    /// An atomic DDL transaction: system-family batches under one durable SAL
    /// zone.
    DdlTxn(&'a [(SysFamily, &'a ZSetBatch)]),
    /// An atomic user-table push transaction: the server refuses it if some
    /// family's relation was written after that family's `basis`.
    PushTxn(&'a [PushFamily]),
    /// PUSH. Its blob is always the batch's schema's record. Held, not
    /// copied, by a write spanning several frames until its last is encoded.
    Push {
        target: Target,
        batch: &'a Arc<ZSetBatch>,
        mode: WireConflictMode,
    },
    /// SNAPSHOT_OPEN; the ACK names the snapshot.
    SnapshotOpen,
    /// SNAPSHOT_CLOSE of this snapshot.
    SnapshotClose(u64),
    /// BACKUP into the backup directory's entry of this name.
    Backup(&'a str),
}

impl<'a> Request<'a> {
    /// The relation its ACK names.
    fn acked_tid(&self) -> u64 {
        match *self {
            Request::AllocSerial { table, .. } | Request::Push { target: table, .. } => table.tid,
            Request::AllocIds(_)
            | Request::DdlTxn(_)
            | Request::PushTxn(_)
            | Request::SnapshotOpen
            | Request::SnapshotClose(_)
            | Request::Backup(_) => 0,
        }
    }

    /// A write's families, as frames carry them: a PUSH's one, a PUSH_TXN's
    /// every one; `None` for a request that writes no user rows.
    fn write_families(&self) -> Option<(ClientVerb, SmallVec<[WriteFamily; 1]>)> {
        match *self {
            Request::Push { target, batch, mode } => {
                let family = WriteFamily {
                    hdr: ControlHeader::item(ClientVerb::Push, target, 0, mode),
                    batch: Arc::clone(batch),
                };
                Some((ClientVerb::Push, smallvec::smallvec![family]))
            }
            Request::PushTxn(families) => {
                let families = (families.iter()).map(|f| WriteFamily {
                    hdr: f.head(),
                    batch: Arc::clone(&f.batch),
                });
                Some((ClientVerb::PushTxn, families.collect()))
            }
            _ => None,
        }
    }

    /// Append the frame payload of a request that writes no user rows to
    /// `out`; a write goes as [`SpanningWrite`] encodes it.
    fn append(self, out: &mut Vec<u8>) {
        // A control-only request: its verb, the relation it names, `arg0`, and its blob.
        let (verb, target, arg0, blob): (ClientVerb, Target, u64, &[u8]) = match self {
            Request::AllocIds(n) => (ClientVerb::AllocIds, Target::from(0), n, &[]),
            Request::AllocSerial { table, count } => (ClientVerb::AllocSerialRange, table, count, &[]),
            Request::SnapshotOpen => (ClientVerb::SnapshotOpen, Target::from(0), 0, &[]),
            Request::SnapshotClose(id) => (ClientVerb::SnapshotClose, Target::from(0), id, &[]),
            Request::Backup(name) => (ClientVerb::Backup, Target::from(0), 0, name.as_bytes()),
            Request::DdlTxn(families) => {
                let blocks = families.iter().map(|(family, b)| (*family, b.regions()));
                return multi_frame::encode_ddl_txn(out, blocks);
            }
            Request::PushTxn(_) | Request::Push { .. } => unreachable!("a write goes as SpanningWrite encodes it"),
        };
        append_payload(out, ControlHeader::naming(verb, target, arg0), blob, None)
    }
}

/// Append one frame payload, without the 4-byte length prefix, to `out`. An
/// empty batch ships no data block, and still ships the blob.
pub(crate) fn append_payload(out: &mut Vec<u8>, hdr: ControlHeader, blob: &[u8], data: Option<&ZSetBatch>) {
    append_frame(
        out,
        &hdr,
        blob,
        data.filter(|b| !b.is_empty()).map(|b| b.regions()).as_deref(),
    );
}

/// The refusal of a request frame of `total` bytes, past the ceiling.
fn oversize(total: usize) -> ClientError {
    ClientError::from(format!(
        "request frame is {total} bytes, exceeding the {}-byte server ingress cap; split the request",
        gnitz_wire::MAX_FRAME_PAYLOAD
    ))
}

/// One family of a user-table push transaction: a maximal run of same-mode
/// writes into `target`.
pub(crate) struct PushFamily {
    /// The relation, under the descriptor token of the first of the family's
    /// batches built from a descriptor; `0` while none was.
    pub(crate) target: Target,
    /// May still be shared with the caller that pushed it, so it is never
    /// written in place.
    pub(crate) batch: Arc<ZSetBatch>,
    pub(crate) mode: WireConflictMode,
    /// The commit fails if `target` was written after `basis`, the oldest
    /// watermark among the reads this family's writes were built from; `BLIND`
    /// when none was.
    pub(crate) basis: u64,
}

impl PushFamily {
    /// The item header its chunks carry.
    fn head(&self) -> ControlHeader {
        ControlHeader::item(ClientVerb::PushTxn, self.target, self.basis, self.mode)
    }
}

/// One request in flight, or a pushed train: how its reply decodes, the reply
/// read so far, and where it goes. A request's slot is answered only while it
/// heads its queue, and a pushed train's while it is the one being read, so its
/// state is the one train in progress, and it goes when the slot does.
enum Slot {
    /// A control-only ACK naming `tid`, its value in `arg0`.
    Ack { tid: u64, to: Promise<u64> },
    /// RESOLVE: the descriptor, or `None` when no such relation exists.
    /// Uncorrelated on the wire, because the request names no id.
    Resolve { to: Promise<Option<Arc<RelDescriptor>>> },
    /// A reply of rows, one relation's train at a time: `train` is the one in
    /// progress, and `to` what its terminal answers.
    Rows { train: Train, to: RowsTo },
    /// One train per view, its blocks kept raw; the train in progress is
    /// `views[at]`, parked under the id it is kept as while that id is read.
    DeltaPoll {
        views: Vec<u64>,
        at: usize,
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

/// What a [`Slot::Rows`] train's terminal answers.
enum RowsTo {
    /// A one-relation SCAN_MULTI, at the LSN its terminal names.
    Scan(Promise<ScanReply>),
    /// SCAN_MULTI of several, replied in request order: the trains after the
    /// one in progress, last first, and the replies before it.
    Scans {
        next: Vec<Train>,
        replies: Vec<ScanReply>,
        to: Promise<Vec<ScanReply>>,
    },
    /// One view's delta read, whose terminal carries the cursor to read from
    /// next; with `keep`, the subscription it is kept as.
    Delta {
        to: Promise<(ScanReply, DeltaCursor)>,
        keep: Option<SubId>,
    },
    /// SNAPSHOT_PAGE: every worker's rows as one train, with the row count at
    /// the end of each worker's so far.
    Page {
        worker_ends: Vec<usize>,
        to: Promise<SnapshotPage>,
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
    /// lives in the transport window; the terminal frame's is decoded by
    /// [`Self::finish`], behind the held ones.
    fn take(
        &mut self,
        frame: &mut FrameBytes<'_>,
        block: std::ops::Range<usize>,
        more: bool,
    ) -> Result<(), ProtocolError> {
        self.rows += gnitz_wire::wal::parse_block(&frame[block.clone()], self.schema.layout().row_width())
            .map_err(|e| ProtocolError::DecodeError(format!("WAL {e}")))?
            .rows;
        if more {
            self.held.push(own_block(frame, block));
        }
        Ok(())
    }

    /// The train's rows: the held blocks, then the terminal frame's block
    /// `last`.
    fn finish(&mut self, last: Option<FrameBlock<'_>>) -> Result<ZSetBatch, ProtocolError> {
        decode_wal_blocks(std::mem::take(&mut self.held), last, &self.schema)
    }

    fn reply(&mut self, last: Option<FrameBlock<'_>>, lsn: Option<u64>) -> Result<ScanReply, ProtocolError> {
        Ok(ScanReply { batch: self.finish(last)?, lsn })
    }
}

/// How one view's train ends: the cursor its terminal carries, or the fault
/// that ended its position.
pub(crate) type PollEnd = Result<DeltaCursor, ClientError>;

/// Where the trains of delta polls and pushed trains go.
#[derive(Default)]
struct Polls {
    /// The blocks so far of each delta poll item still being answered, by
    /// the id it is kept as. An item that ends moves to [`Self::subs`].
    reading: HashMap<SubId, Vec<RawBlock>>,
    /// What was read or pushed for each subscription the session holds, by
    /// its id, kept until its owner takes it. An id with no entry in either
    /// map is one nobody holds: its frames are dropped.
    subs: HashMap<SubId, Held>,
    /// The server may hold a subscription of this connection: a read it was
    /// asked to keep was answered with a cursor since the last sync, or that
    /// sync named one.
    server_holds: bool,
}

/// What is kept for one subscription until its owner takes it: the blocks
/// read or pushed and not yet taken, with the cursor past them, or the fault
/// that ended it.
pub(crate) type Held = Result<(Vec<RawBlock>, DeltaCursor), ClientError>;

/// Whether `held` keeps a train or an end its owner has not taken.
pub(crate) fn untaken(held: &Held) -> bool {
    held.as_ref().map_or(true, |(blocks, _)| !blocks.is_empty())
}

/// Merge `taken` into `held`: its blocks after those kept, under its cursor,
/// or its end in their place. An ended `held` stays as it is.
pub(crate) fn park(held: &mut Held, taken: Held) {
    match (held, taken) {
        (Ok((blocks, at)), Ok((train, cursor))) => {
            blocks.extend(train);
            *at = cursor;
        }
        (held @ Ok(_), end @ Err(_)) => *held = end,
        (Err(_), _) => {}
    }
}

impl Polls {
    /// The delta poll's item kept as `id` ended with `end`: its train is held
    /// at the cursor, or the item ended. An id no longer read is left as it is.
    fn poll_end(&mut self, id: SubId, end: PollEnd) {
        match self.reading.remove(&id) {
            Some(blocks) => self.hold(id, blocks, end),
            // A cursor is what the server keeps a subscription from, held here or not.
            None => self.server_holds |= end.is_ok(),
        }
    }

    /// Hold `id` with `blocks` read for it, at the cursor `end` names, or
    /// ended.
    fn hold(&mut self, id: SubId, blocks: Vec<RawBlock>, end: PollEnd) {
        self.server_holds |= end.is_ok();
        self.subs.insert(id, end.map(|cursor| (blocks, cursor)));
    }

    /// A train pushed for `sub` ended with `end`, after `train`.
    fn holder_end(&mut self, sub: SubId, train: Vec<RawBlock>, end: PollEnd) {
        self.server_holds |= end.is_ok();
        if let Some(held) = self.subs.get_mut(&sub) {
            park(held, end.map(|cursor| (train, cursor)));
        }
    }

    /// Whether a subscription holds a train or an end nobody took yet.
    fn untaken(&self) -> bool {
        self.subs.values().any(untaken)
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
    fn feed(&mut self, buf: FrameBytes<'_>) -> Result<(), ProtocolError> {
        let ctrl = peek_control_block(&buf).map_err(ProtocolError::DecodeError)?;
        let in_train = self.pushed.is_some();
        match ctrl.server_lane() {
            Ok(ServerLane::Reply) => {}
            _ if in_train => {
                return Err(ProtocolError::DecodeError(
                    "a pushed train split by a frame that is not of it".into(),
                ));
            }
            Err(e) => return Err(ProtocolError::DecodeError(e.into())),
            Ok(ServerLane::PushedTrain { view, sub }) => {
                self.pushed = Some(Slot::Pushed { tid: view, sub, blocks: Vec::new() });
                return Ok(());
            }
            Ok(ServerLane::SyncAnswer) => {
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
    /// The payload bytes past which a write is cut into several frames
    /// (`GNITZ_WRITE_FRAME_BYTES`, at most and by default
    /// [`gnitz_wire::MAX_FRAME_PAYLOAD`]).
    write_frame_bytes: usize,
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
            write_frame_bytes: gnitz_foundation::env::env_num_in(
                "GNITZ_WRITE_FRAME_BYTES",
                gnitz_wire::MAX_FRAME_PAYLOAD,
                1..=gnitz_wire::MAX_FRAME_PAYLOAD,
            ),
        }
    }

    /// Cut writes into frames of at most `bytes`.
    #[cfg(test)]
    pub(crate) fn set_write_frame_bytes(&mut self, bytes: usize) {
        self.write_frame_bytes = bytes;
    }

    /// Requests submitted since this session connected — one per `submit`,
    /// however the bytes were batched. HELLO predates the session, uncounted.
    pub(crate) fn requests_sent(&self) -> u64 {
        self.submitted
    }

    // ── Submitting and stepping ────────────────────────────────────────────

    /// Encode and queue `req`. A request this session refuses — at either cap,
    /// or past the frame ceiling — is answered with the refusal, as every
    /// submit's is; the server certifies a pushed batch's shape record, and
    /// every row's NULL bits and cells, against the relation's own schema.
    ///
    /// A write goes as [`SpanningWrite`] cuts it at [`Self::write_frame_bytes`]:
    /// past it as several frames, which the server commits as one write.
    pub(crate) fn submit(&mut self, req: Request<'_>) -> Sent<u64> {
        let (to, sent) = promise();
        let mut slot = Slot::Ack { tid: req.acked_tid(), to };
        let Some((verb, families)) = req.write_families() else {
            self.enqueue(|out| req.append(out), slot);
            return sent;
        };
        let ceiling = self.write_frame_bytes;
        let queued = self.admit_with(|t| {
            t.write(SpanningWrite::cut(verb, families, ceiling).map_err(oversize)?);
            Ok(())
        });
        match queued {
            Ok(()) => self.inbox.pending.push_back(slot),
            Err(why) => slot.fail(why, &mut self.inbox.polls),
        }
        sent
    }

    /// RESOLVE — describe the relation named by the canonical
    /// `"schema_name.relation_name"`; `None` when there is none.
    pub(crate) fn submit_resolve(&mut self, qname: &str) -> Sent<Option<Arc<RelDescriptor>>> {
        let hdr = ControlHeader::naming(ClientVerb::Resolve, Target::from(0), 0);
        let (to, sent) = promise();
        self.enqueue(
            |out| append_payload(out, hdr, qname.as_bytes(), None),
            Slot::Resolve { to },
        );
        sent
    }

    /// A one-item SCAN_MULTI, replied in `reply_schema`'s layout.
    pub(crate) fn submit_scan(
        &mut self,
        target: Target,
        spec: &gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Sent<ScanReply> {
        let spec = spec.encode();
        let item = scan_item(target, reply_schema, &spec);
        let (to, sent) = promise();
        let slot = Slot::Rows {
            train: Train::new(target.tid, reply_schema),
            to: RowsTo::Scan(to),
        };
        self.enqueue(|out| multi_frame::encode_scan_multi(out, &[item]), slot);
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
        let slot = Slot::Rows {
            train: Train::new(tid, reply_schema),
            to: RowsTo::Page { worker_ends: Vec::new(), to },
        };
        let request = request.encode();
        self.enqueue(|out| append_payload(out, hdr, &request, None), slot);
        sent
    }

    /// SCAN_MULTI: a read of N relations as one request, replied in order, each
    /// in its schema's layout.
    pub(crate) fn submit_scan_multi(
        &mut self,
        scans: &[(Target, &gnitz_wire::ReadSpec, &Arc<Schema>)],
    ) -> Sent<Vec<ScanReply>> {
        let specs: Vec<Vec<u8>> = scans.iter().map(|(_, spec, _)| spec.encode()).collect();
        let items: Vec<multi_frame::ScanItem> = scans
            .iter()
            .zip(&specs)
            .map(|(&(target, _, schema), spec)| scan_item(target, schema, spec))
            .collect();
        let mut next: Vec<Train> = (scans.iter().rev())
            .map(|&(target, _, schema)| Train::new(target.tid, schema))
            .collect();
        let Some(train) = next.pop() else {
            return Sent::ready(Err(ClientError::from("a multi-read names no relation")));
        };
        let (to, sent) = promise();
        let slot = Slot::Rows {
            train,
            to: RowsTo::Scans {
                next,
                replies: Vec::with_capacity(scans.len()),
                to,
            },
        };
        self.enqueue(|out| multi_frame::encode_scan_multi(out, &items), slot);
        sent
    }

    /// DELTA_POLL of one view after `from` — the view whole with none — under
    /// `spec`, replied in `reply_schema`'s layout. With `keep`, it stays
    /// subscribed as that id from the cursor it answers with, held for
    /// [`Self::take_pushed`] until a sync no longer names it.
    pub(crate) fn submit_delta_read(
        &mut self,
        view: Target,
        from: Option<DeltaCursor>,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
        keep: Option<SubId>,
    ) -> Sent<(ScanReply, DeltaCursor)> {
        let (to, sent) = promise();
        let slot = Slot::Rows {
            train: Train::new(view.tid, reply_schema),
            to: RowsTo::Delta { to, keep },
        };
        let item = multi_frame::DeltaPollItem {
            view,
            from,
            reply_layout: reply_schema.layout().layout_digest(),
            spec,
        };
        self.enqueue(|out| multi_frame::encode_delta_poll(out, &[item], keep), slot);
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
        let named = multi_frame::encode_held(polls.subs.keys().copied());
        let (mut to, sent) = promise();
        match self.admit(|out| append_payload(out, hdr, &named, None)) {
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
    pub(crate) fn take_pushed(&mut self, id: SubId) -> Held {
        let Entry::Occupied(mut held) = self.inbox.polls.subs.entry(id) else {
            return Err(ClientError::from(format!(
                "subscription {id} is not held on this connection; read from its cursor and subscribe again"
            )));
        };
        match held.get_mut() {
            Ok((blocks, cursor)) => Ok((std::mem::take(blocks), *cursor)),
            Err(_) => held.remove(),
        }
    }

    /// The blocks read for the delta poll's item kept as `id` since they were
    /// last taken, while its train is read; `None` once it ended, its train
    /// and end then taken by [`Self::take_pushed`].
    pub(crate) fn take_reading(&mut self, id: SubId) -> Option<Vec<RawBlock>> {
        self.inbox.polls.reading.get_mut(&id).map(std::mem::take)
    }

    /// Stop holding subscription `id`: what is pushed for it from now on is
    /// dropped unparked. The next sync still tells the server.
    pub(crate) fn release(&mut self, id: SubId) {
        self.inbox.polls.reading.remove(&id);
        self.inbox.polls.subs.remove(&id);
    }

    /// DELTA_POLL of `views`, item `i` kept as [`kept_as`]`(first, i)`: its
    /// train's blocks for [`Self::take_reading`] as they arrive, then for
    /// [`Self::take_pushed`] the cursor it is held at, or the fault that ended
    /// it. A request the session
    /// refuses ends each with the refusal. An id released before its item
    /// ends drops what is left of its train.
    pub(crate) fn submit_delta_poll(&mut self, first: SubId, views: &[multi_frame::DeltaPollItem]) {
        if views.is_empty() {
            return;
        }
        // Before the request is queued, so a refusal finds every item to end.
        let reading = (0..views.len()).map(|i| (kept_as(first, i), Vec::new()));
        self.inbox.polls.reading.extend(reading);
        let slot = Slot::DeltaPoll {
            views: views.iter().map(|v| v.view.tid).collect(),
            at: 0,
            first,
        };
        self.enqueue(|out| multi_frame::encode_delta_poll(out, views, Some(first)), slot);
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
        // A frame past the ceiling is refused here rather than by the server's
        // ingress cap, which would drop the connection.
        self.admit_with(|t| t.frame(encode).map_err(oversize))
    }

    /// Queue one request's frames, which `queue` puts on the transport, or the
    /// refusal this session answers it with.
    fn admit_with(
        &mut self,
        queue: impl FnOnce(&mut ClientTransport) -> Result<(), ClientError>,
    ) -> Result<(), ClientError> {
        if let Some(why) = &self.ended {
            return Err(why.clone());
        }
        if let Some(why) = self.cap_refusal() {
            return Err(why);
        }
        queue(&mut self.transport)?;
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
    /// a wait per request. Otherwise it parks on `interest()`. A step that
    /// waits on nothing first asks the host's [`Host::poll_proceed`].
    ///
    /// `Closed` when the session will answer nothing more: it ended, or the
    /// reply awaited belongs to a session since replaced.
    pub(crate) fn turn(&mut self, host: &mut dyn Host, cx: &mut Context<'_>) -> Poll<Result<(), ClientError>> {
        let want = self.interest();
        let ready = if self.unread || (want.write && !self.refused) {
            ready!(host.poll_proceed(cx));
            match self.unread {
                true => Interest::READ,
                false => Interest::WRITE,
            }
        } else if want.is_empty() {
            return Poll::Ready(Err(ClientError::Closed));
        } else {
            match ready!(host.poll_io(want, cx)) {
                Ok(ready) => ready,
                // An interrupt leaves the session as it stood.
                Err(e @ ClientError::Interrupted(_)) => return Poll::Ready(Err(e)),
                // A wait that failed is the connection's end, which every reply
                // owed arrives as.
                Err(e) => {
                    self.end(match e {
                        ClientError::Protocol(lost) => ClientError::ConnectionLost(lost),
                        e => e,
                    });
                    return Poll::Ready(Ok(()));
                }
            }
        };
        self.step(ready);
        Poll::Ready(Ok(()))
    }

    /// Step until `ready` yields, asking it before each step: a step with
    /// nothing outstanding is `Closed`, so an answer already in hand is taken
    /// first.
    pub(crate) fn poll_until<R>(
        &mut self,
        host: &mut dyn Host,
        cx: &mut Context<'_>,
        mut ready: impl FnMut(&mut Session) -> Option<R>,
    ) -> Poll<Result<R, ClientError>> {
        loop {
            if let Some(r) = ready(self) {
                return Poll::Ready(Ok(r));
            }
            ready!(self.turn(host, cx))?;
        }
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
        Some(ClientError::from(if self.inbox.in_flight() >= MAX_IN_FLIGHT {
            format!("connection has {MAX_IN_FLIGHT} requests in flight")
        } else {
            format!(
                "connection has {} unwritten bytes queued, at the {MAX_QUEUED_BYTES}-byte cap",
                self.transport.queued_bytes()
            )
        }))
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
    fn feed(
        &mut self,
        ctrl: DecodedControl,
        mut buf: FrameBytes<'_>,
        polls: &mut Polls,
    ) -> Result<bool, ProtocolError> {
        let named = ctrl.hdr.target_id;
        // The relation this frame must name. Replies arrive in request order,
        // so a frame naming another would decode under the wrong schema
        // silently; make it loud.
        let want = match self {
            Slot::Ack { tid, .. } | Slot::Pushed { tid, .. } => Some(*tid),
            Slot::DeltaPoll { views, at, .. } => Some(views[*at]),
            Slot::Rows { train, .. } => Some(train.tid),
            Slot::Resolve { .. } => None,
        };
        if let Some(fault) = ctrl.fault(&buf) {
            let refused = ClientError::Refused(fault);
            // A DELTA_POLL fault naming a view ends that view's position alone;
            // every other fault ends the request.
            return match (&mut *self, want) {
                (Slot::DeltaPoll { views, at, first }, Some(view)) if named != 0 => {
                    if named != view {
                        return Err(out_of_order(view, named));
                    }
                    polls.poll_end(kept_as(*first, *at), Err(refused));
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
        match (&mut *self, ctrl.data.clone()) {
            (_, None) => {}
            (Slot::DeltaPoll { at, first, .. }, Some(block)) => {
                if let Some(blocks) = polls.reading.get_mut(&kept_as(*first, *at)) {
                    blocks.push(own_block(&mut buf, block));
                }
            }
            (Slot::Pushed { sub, blocks, .. }, Some(block)) => {
                if matches!(polls.subs.get(sub), Some(Ok(_))) {
                    blocks.push(own_block(&mut buf, block));
                }
            }
            (Slot::Rows { train, .. }, Some(r)) => train.take(&mut buf, r, ctrl.hdr.flags.continuation)?,
            (Slot::Ack { .. } | Slot::Resolve { .. }, Some(_)) => {
                return Err(ProtocolError::DecodeError(
                    "a data block on a reply that carries no rows".into(),
                ));
            }
        }

        if let Slot::Rows {
            train,
            to: RowsTo::Page { worker_ends, .. },
        } = &mut *self
            && ctrl.hdr.flags.scan_last
        {
            worker_ends.push(train.rows);
        }
        if ctrl.hdr.flags.continuation {
            return Ok(false);
        }

        // The train terminated; a train slot's last block is still in `buf`.
        let cursor = || {
            ctrl.hdr
                .delta_cursor()
                .ok_or_else(|| ProtocolError::DecodeError("a delta-poll terminal at round 0".into()))
        };
        match self {
            Slot::Ack { to, .. } => to.fulfil(Ok(ctrl.hdr.arg0)),
            Slot::Resolve { to } => to.fulfil(Ok(resolve_descriptor(&ctrl, &buf)?)),
            Slot::Rows { train, to } => {
                // The frame is taken out of `buf`, so a frame the transport handed over whole
                // becomes the batch.
                let last = (ctrl.data.clone()).map(|block| FrameBlock { frame: std::mem::take(&mut buf), block });
                match to {
                    RowsTo::Scan(to) => to.fulfil(Ok(train.reply(last, Some(ctrl.hdr.arg0))?)),
                    RowsTo::Scans { next, replies, to } => {
                        replies.push(train.reply(last, Some(ctrl.hdr.arg0))?);
                        if let Some(following) = next.pop() {
                            *train = following;
                            return Ok(false);
                        }
                        to.fulfil(Ok(std::mem::take(replies)))
                    }
                    RowsTo::Page { worker_ends, to } => {
                        let batch = train.finish(last)?;
                        // The next page's rows are shared over the stores this one
                        // names, so a page naming none is no reply to a page request.
                        if worker_ends.is_empty() {
                            return Err(ProtocolError::DecodeError("a snapshot page naming no worker".into()));
                        }
                        let worker_ends = std::mem::take(worker_ends);
                        to.fulfil(Ok(SnapshotPage { batch, worker_ends }))
                    }
                    RowsTo::Delta { to, keep } => {
                        let cursor = cursor()?;
                        let reply = train.reply(last, None)?;
                        if let Some(id) = *keep {
                            polls.hold(id, Vec::new(), Ok(cursor));
                        }
                        to.fulfil(Ok((reply, cursor)))
                    }
                }
            }
            Slot::DeltaPoll { views, at, first } => {
                polls.poll_end(kept_as(*first, *at), Ok(cursor()?));
                *at += 1;
                if *at < views.len() {
                    return Ok(false);
                }
            }
            Slot::Pushed { sub, blocks, .. } => polls.holder_end(*sub, std::mem::take(blocks), Ok(cursor()?)),
        }
        Ok(true)
    }

    /// The request ended unanswered: `why` is its reply, and the end of each
    /// view a delta poll had yet to answer.
    fn fail(&mut self, why: ClientError, polls: &mut Polls) {
        match self {
            Slot::Ack { to, .. } => to.fulfil(Err(why)),
            Slot::Resolve { to } => to.fulfil(Err(why)),
            Slot::Rows { to, .. } => match to {
                RowsTo::Scan(to) => to.fulfil(Err(why)),
                RowsTo::Scans { to, .. } => to.fulfil(Err(why)),
                RowsTo::Delta { to, .. } => to.fulfil(Err(why)),
                RowsTo::Page { to, .. } => to.fulfil(Err(why)),
            },
            Slot::DeltaPoll { views, at, first } => {
                for at in *at..views.len() {
                    polls.poll_end(kept_as(*first, at), Err(why.clone()));
                }
            }
            Slot::Pushed { sub, blocks, .. } => polls.holder_end(*sub, std::mem::take(blocks), Err(why)),
        }
    }
}

/// One SCAN_MULTI item: `target` read under `spec`, replied in `schema`'s layout.
fn scan_item<'a>(target: Target, schema: &Schema, spec: &'a [u8]) -> multi_frame::ScanItem<'a> {
    multi_frame::ScanItem {
        target,
        reply_layout: schema.layout().layout_digest(),
        spec,
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
    let desc = RelDescriptorBlob::decode(&frame[ctrl.blob.clone()]).map_err(ProtocolError::DecodeError)?;
    let schema = Arc::new(Schema::from_record(desc.schema).map_err(ProtocolError::DecodeError)?);
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

/// `n` fresh subscription ids, of which the first is returned: `next_sub`
/// moves past them.
pub(crate) fn draw_subs(next_sub: &mut SubId, n: usize) -> SubId {
    let first = *next_sub;
    *next_sub = kept_as(first, n);
    first
}

/// A delta poll in flight: `items` in one request, item `i` parked in the
/// session as [`Self::id`]`(i)` and taken in order. Dropping it releases
/// every item not yet taken whole, which drops what is left of its train. A
/// subscription it kept is its caller's only once recorded where the next
/// sync names it from.
pub(crate) struct DeltaPoll<'s> {
    pub(crate) session: &'s mut Session,
    first: SubId,
    /// The item being answered: every one before it was taken whole.
    pub(crate) at: usize,
    items: usize,
}

impl Drop for DeltaPoll<'_> {
    fn drop(&mut self) {
        for i in self.at..self.items {
            self.session.release(self.id(i));
        }
    }
}

impl<'s> DeltaPoll<'s> {
    /// Poll `items`, each kept as a subscription whose id is drawn from
    /// `next_sub`, which moves past them.
    pub(crate) fn start(session: &'s mut Session, next_sub: &mut SubId, items: &[multi_frame::DeltaPollItem]) -> Self {
        let first = draw_subs(next_sub, items.len());
        session.submit_delta_poll(first, items);
        DeltaPoll {
            session,
            first,
            at: 0,
            items: items.len(),
        }
    }

    /// The subscription item `i` is kept as.
    pub(crate) fn id(&self, i: usize) -> SubId {
        kept_as(self.first, i)
    }
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
