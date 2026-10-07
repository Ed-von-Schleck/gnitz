use crate::connection::{
    promise, DeltaCursor, Interest, Polled, RelDescriptor, Request, ScanReply, Sent, Session, Target,
};
use crate::error::ClientError;
use crate::protocol::transport::poll_fd;
use crate::{sys_schema, BatchAppender, PkColumn, ProtocolError, PushFamily, RelName, Schema, ZSetBatch};
use gnitz_expr::{ColumnTable, SchemaFacts};
use gnitz_wire::{ColumnDef, PkBuf, PkKeys, WireConflictMode};
use gnitz_wire::{WireFault, WireStatus};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};
use std::time::Duration;

use gnitz_expr::{LogicalProgram, RowFilter};
use gnitz_wire::sys_rows::{
    CircuitRow, ColTabRow, ColTabSlot, FkRef, IdxTabRow, SchemaTabRow, SchemaTabSlot, SysRow, TableTabRow, ViewTabRow,
};
use gnitz_wire::txn_frame::{DeltaPollItem, BLIND};
use gnitz_wire::{payload_bytes, payload_str, payload_u64};
use gnitz_wire::{Circuit, ComputeMap, Cut, KeyRange, ReadBound, ReadSink, ReadSpec};
use gnitz_wire::{
    PkColList, PkListRole, TableProps, ViewProps, CIRCUIT_TAB, COL_TAB, IDX_TAB, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB,
};

// --- Module-private helpers ---

/// The absence a catalog lookup by name reports.
pub fn not_found(noun: &'static str, name: &RelName) -> ClientError {
    absent(format!("{noun} '{name}' not found"))
}

/// A `WireStatus::NotFound` refusal raised on this side, worded by the caller.
fn absent(text: String) -> ClientError {
    ClientError::Refused(WireFault { status: WireStatus::NotFound, text })
}

/// Build the `-1` retraction batch for `pks`: the server's unique-PK rule
/// retracts by PK alone, so the payload columns are inert filler. Built directly rather
/// than through `BatchAppender`, which has no way to take a whole `PkColumn`.
pub fn retraction_batch(schema: &Schema, pks: PkColumn) -> ZSetBatch {
    let count = pks.len();
    ZSetBatch {
        pks,
        weights: vec![-1; count],
        nulls: vec![0; count],
        payload: ZSetBatch::filler_columns(schema, count),
        blob: vec![],
    }
}

/// The keys-only read of `schema`: a reply of its PK columns alone, and a rows sink
/// whose zero-instruction map fills no payload slot.
pub fn key_reply(schema: &Schema) -> (Arc<Schema>, ReadSink) {
    let reply = Schema {
        columns: schema.hidden_key_columns().collect(),
        pk_cols: (0..schema.pk_cols.len() as u32).collect(),
    };
    let program = LogicalProgram::copy_cols(&[]).to_blob_bytes();
    let map = ComputeMap { program, out_cols: Vec::new() };
    (Arc::new(reply), ReadSink { map: Some(map), ..ReadSink::all_rows() })
}

/// Autocommit attempts [`GnitzClient::read_modify_write`] makes before surfacing
/// the conflict for the caller's own retry. Each attempt re-reads; there is no
/// backoff.
pub const RMW_MAX_ATTEMPTS: usize = 4;

// --- GnitzClient ---

/// One inline `UNIQUE` constraint of a `CREATE TABLE`. `name` is the catalog
/// index name `DROP INDEX` matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineUniqueIndex {
    pub cols: PkColList,
    pub name: String,
}

/// One live secondary index, as its IDX_TAB row states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRow {
    /// The id of the table it indexes.
    pub owner: u64,
    pub name: String,
    pub cols: PkColList,
    pub is_unique: bool,
}

/// The column a FOREIGN KEY column references. `SelfTable` names the table
/// being created, which has no id yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FkTarget {
    Table(FkRef),
    SelfTable { col: u32 },
}

impl FkTarget {
    /// The referenced column, once the table being created has the id `own_id`.
    fn resolve(self, own_id: u64) -> FkRef {
        match self {
            FkTarget::Table(fk) => fk,
            FkTarget::SelfTable { col } => FkRef { table_id: own_id, col },
        }
    }
}

/// The ids one SERIAL reservation takes when the statement asks for fewer. Each
/// reservation is a durable advance on the master.
const SERIAL_RANGE_SIZE: u64 = 64;

/// The rows of one DDL transaction: one batch per system family it writes.
#[derive(Default)]
struct DdlBundle(Vec<(u64, ZSetBatch)>);

impl DdlBundle {
    /// `family`'s batch, opened on first use — so a family nothing wrote to has
    /// no entry, and no family has two.
    fn batch(&mut self, family: u64) -> &mut ZSetBatch {
        let at = match self.0.iter().position(|(f, _)| *f == family) {
            Some(at) => at,
            None => {
                self.0.push((family, ZSetBatch::new(sys_schema(family))));
                self.0.len() - 1
            }
        };
        &mut self.0[at].1
    }

    /// `row` at `weight`, in its own family's batch.
    fn put<R: SysRow>(&mut self, row: &R, weight: i64) {
        row.write(&mut BatchAppender::new(self.batch(R::FAMILY)), weight);
    }

    /// Whether `other` holds the same live rows, family for family.
    fn same_rows(&self, other: &DdlBundle) -> bool {
        // A German cell's offset is relative to the arena of the batch it sits
        // in, so each side is copied onto an arena laid out in key order.
        fn in_key_order(family: u64, rows: &ZSetBatch) -> ZSetBatch {
            let mut live: Vec<usize> = rows.live_rows().collect();
            live.sort_unstable_by_key(|&i| rows.pks.get_bytes(i));
            let mut out = ZSetBatch::new(sys_schema(family));
            for i in live {
                out.copy_row_at(rows, i, rows.weights[i]);
            }
            out
        }
        self.0.len() == other.0.len()
            && self.0.iter().all(|(family, rows)| {
                other
                    .0
                    .iter()
                    .any(|(f, theirs)| f == family && in_key_order(*family, rows) == in_key_order(*family, theirs))
            })
    }
}

/// Upper bound on the segments in one atomic view chain — a bound on the DDL
/// bundle, not on planning: every segment is already built and in RAM by the time
/// `create_view_chain` counts them.
pub const MAX_CHAIN_SEGMENTS: usize = 64;

/// How an internal chain segment is named, from its own allocated view id.
///
/// Unique because vids are, and unspellable at every SQL surface because
/// [`gnitz_wire::validate_user_identifier`] rejects a leading `_`. Ownership is the
/// `owner_view_id` column, not the name.
fn segment_name(vid: u64) -> String {
    format!("_seg{vid}")
}

/// The symbolic id by which a [`ViewBundle`] circuit's `ScanDelta` names
/// `segments[j]`. No durable relation id reaches
/// [`gnitz_wire::CATALOG_ID_CEILING`], so it is no relation's own.
pub fn segment_id(j: u64) -> u64 {
    gnitz_wire::CATALOG_ID_CEILING + j
}

/// The relation `source` names in a chain whose first member stands at `base`.
/// A [`segment_id`] past the chain's end becomes an id no lower than the user-named
/// view's own, which the engine refuses.
fn source_at(source: u64, base: u64) -> u64 {
    match source.checked_sub(gnitz_wire::CATALOG_ID_CEILING) {
        Some(j) => base + j,
        None => source,
    }
}

/// One view in a [`ViewBundle`].
pub struct PlannedView {
    pub circuit: Circuit,
    pub schema: Arc<Schema>,
    /// [`ViewTabRow::pk_repeats`], stated by the emitter that minted the key.
    pub pk_repeats: bool,
}

/// A [`GnitzClient::create_view_chain`] bundle: the user-named view and the
/// hidden segments it owns, in dependency order. [`segment_id`]`(j)` names
/// `segments[j]`.
pub struct ViewBundle {
    pub segments: Vec<PlannedView>,
    pub view: PlannedView,
}

impl ViewBundle {
    /// The chain's catalog rows at `+1`, its first member standing at `base`:
    /// each segment named by [`segment_name`] and owned by the user-named view,
    /// which stands last under `view_name` and `props`. Every schema has passed
    /// [`Schema::validate`].
    fn put_rows(&self, b: &mut DdlBundle, view_name: &str, props: ViewProps, schema_id: u64, base: u64) {
        let owner_vid = base + self.segments.len() as u64;
        for (pv, vid) in self.segments.iter().chain([&self.view]).zip(base..) {
            let mut circuit = pv.circuit.clone();
            for src in circuit.sources_mut() {
                *src = source_at(*src, base);
            }
            let (name, owner_view_id, props) = if vid == owner_vid {
                (view_name.to_string(), 0, props)
            } else {
                (segment_name(vid), owner_vid, ViewProps::default())
            };

            // A foreign key constrains a base table, not a view.
            append_col_rows(b, vid, &pv.schema.columns, &[]);
            let circuit = circuit.encode();
            b.put(&CircuitRow { view_id: vid, circuit: &circuit }, 1);
            let (capacity_bytes, delta_bytes) = props.row_words();
            // The VIEW_TAB register hook triggers server-side compilation.
            b.put(
                &ViewTabRow {
                    view_id: vid,
                    schema_id,
                    name: &name,
                    pk_col_idx: PkColList::from_slice(&pv.schema.pk_cols).pack(),
                    capacity_bytes,
                    delta_bytes,
                    owner_view_id,
                    pk_repeats: pv.pk_repeats as u64,
                },
                1,
            );
        }
    }
}

impl From<PlannedView> for ViewBundle {
    fn from(view: PlannedView) -> Self {
        ViewBundle { segments: Vec::new(), view }
    }
}

/// Run when a signal interrupts a blocking wait; an `Err` aborts the call.
/// The Python binding checks for Ctrl-C here.
pub type ParkHook = Box<dyn FnMut() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send>;

/// Work a [`Host`] runs where blocking is allowed.
pub type Job = Box<dyn FnOnce() + Send>;

/// How a client waits: for its socket, and for work that blocks.
pub trait Host: Send {
    /// The client's socket is `fd` from here on, until the next `attach` or
    /// this host's drop — either of which comes before the socket closes.
    fn attach(&mut self, fd: BorrowedFd<'_>) -> std::io::Result<()>;

    /// Once the socket is ready for any of `want`, run `io` on what is ready.
    /// `io` reads until the socket has no more, and answers what the client
    /// still waits for: write, if the socket refused bytes.
    fn poll_io(
        &mut self,
        want: Interest,
        cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> Interest,
    ) -> Poll<Result<(), ClientError>>;

    /// Run `job` where it may block. A host that cannot run it drops it.
    fn spawn(&mut self, job: Job);
}

/// Waits in `poll(2)` and runs jobs in place, so a future over it never pends:
/// [`block_on`] drives one to completion.
#[derive(Default)]
pub struct BlockingHost {
    fd: Option<RawFd>,
    hook: Option<ParkHook>,
    /// The socket refused bytes, so the next write waits for it.
    refused: bool,
}

impl BlockingHost {
    /// A host that runs `hook` whenever a signal interrupts its wait.
    pub fn with_hook(hook: ParkHook) -> Self {
        BlockingHost { hook: Some(hook), ..Default::default() }
    }
}

impl Host for BlockingHost {
    fn attach(&mut self, fd: BorrowedFd<'_>) -> std::io::Result<()> {
        self.fd = Some(fd.as_raw_fd());
        self.refused = false;
        Ok(())
    }

    fn poll_io(
        &mut self,
        want: Interest,
        _cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> Interest,
    ) -> Poll<Result<(), ClientError>> {
        let fd = self.fd.expect("a client attaches its host before it waits");
        // A socket takes bytes until it says otherwise, so asking first would
        // be a syscall per request.
        if want.write && !self.refused {
            self.refused = io(Interest::WRITE).write;
            return Poll::Ready(Ok(()));
        }
        loop {
            match poll_fd(fd, want.poll_events(), None) {
                Ok(revents) => {
                    self.refused = io(Interest::from_revents(revents)).write;
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    if let Some(hook) = self.hook.as_mut() {
                        if let Err(e) = hook() {
                            return Poll::Ready(Err(ClientError::Interrupted(e.into())));
                        }
                    }
                }
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }
    }

    fn spawn(&mut self, job: Job) {
        job()
    }
}

/// Drive a future of a client whose host never pends — a [`BlockingHost`]'s.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("block_on drove a client whose host pends; that client belongs to an event loop"),
    }
}

/// Wait for the session's socket and step it on what is ready.
fn poll_turn(session: &mut Session, host: &mut dyn Host, cx: &mut Context<'_>) -> Poll<Result<(), ClientError>> {
    if session.unread() {
        session.step(Interest::READ);
        return Poll::Ready(Ok(()));
    }
    let want = session.interest();
    if want.is_empty() {
        // Nothing this session will ever answer: it ended, or the reply
        // awaited belongs to a session since replaced.
        return Poll::Ready(Err(ClientError::Closed));
    }
    host.poll_io(want, cx, &mut |ready| {
        session.step(ready);
        session.interest()
    })
}

/// Run `job` on `host`, where it may block, and wait for what it returns. A
/// panic in it resumes here; a job the host dropped is `Closed`.
pub(crate) async fn offload<R: Send + 'static>(
    host: &mut dyn Host,
    job: impl FnOnce() -> R + Send + 'static,
) -> Result<R, ClientError> {
    let (mut ended, job_end) = promise();
    host.spawn(Box::new(move || {
        ended.fulfil(Ok(std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))))
    }));
    job_end.await?.map_err(|panic| std::panic::resume_unwind(panic))
}

/// A verb that is one request: awaited, it is that round trip;
/// [detached](Self::detach), the request is on its way and the client is free
/// for the next, which is what lets several share a round trip.
#[must_use = "a verb does nothing until awaited or detached"]
pub struct Pending<'a, T> {
    client: &'a mut GnitzClient,
    sent: Sent<T>,
}

impl<T> Unpin for Pending<'_, T> {}

impl<'a, T> Pending<'a, T> {
    /// A refusal that sent nothing is the reply.
    fn submitted(client: &'a mut GnitzClient, sent: Result<Sent<T>, ClientError>) -> Self {
        let sent = sent.unwrap_or_else(|e| Sent::ready(Err(e)));
        Pending { client, sent }
    }

    /// Let go of the client, keeping the reply.
    pub fn detach(self) -> Sent<T> {
        self.sent
    }
}

impl<T> Future for Pending<'_, T> {
    type Output = Result<T, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            if let Some(reply) = this.sent.try_take() {
                return Poll::Ready(reply);
            }
            let GnitzClient { session, host, .. } = &mut *this.client;
            ready!(poll_turn(session, &mut **host, cx))?;
        }
    }
}

/// One queued call of a [`serve`] loop: it has the client to itself until its
/// future completes.
pub type Op = Box<dyn for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, ()> + Send>;

/// A boxed future a host can move between threads.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Run the ops `next` yields on `client`, in order and one at a time. Steps
/// the session while none is ready, and after `next` yields `None` until
/// nothing is outstanding; then drops the client.
pub async fn serve(mut client: GnitzClient, mut next: impl FnMut(&mut Context<'_>) -> Poll<Option<Op>>) {
    let mut more = true;
    loop {
        let op = poll_fn(|cx| loop {
            if more {
                match next(cx) {
                    Poll::Ready(Some(op)) => return Poll::Ready(Some(op)),
                    Poll::Ready(None) => more = false,
                    Poll::Pending => {}
                }
            }
            let GnitzClient { session, host, .. } = &mut client;
            if session.interest().is_empty() && !session.unread() {
                return match more {
                    true => Poll::Pending,
                    false => Poll::Ready(None),
                };
            }
            match poll_turn(session, &mut **host, cx) {
                Poll::Ready(Ok(())) => {}
                // The replies outstanding resolve as the session's end.
                Poll::Ready(Err(_)) => session.close(),
                Poll::Pending => return Poll::Pending,
            }
        })
        .await;
        let Some(op) = op else { return };
        // A connection that is gone refuses the op's own requests.
        let _ = client.make_room().await;
        op(&mut client).await;
    }
}

pub struct GnitzClient {
    /// Before the session, so it lets go of the socket before that closes.
    pub(crate) host: Box<dyn Host>,
    pub(crate) session: Session,
    /// Per table, the SERIAL ids a reservation drew and no INSERT has taken. A
    /// disconnect discards them (an intentional, PostgreSQL-style gap).
    serial_cache: HashMap<u64, std::ops::Range<u64>>,
    /// Open transaction, if any; `None` is autocommit. Every user-table write
    /// buffers here while it is open, and dropping it is ROLLBACK.
    txn: Option<TxnBuffer>,
    /// The local copy this client reads through, if a host attached one. Boxed,
    /// so a client that never mirrors pays one `None` and no allocation.
    pub(crate) mirror: Option<Box<crate::mirror::MirrorState>>,
    /// Qualified name → the descriptor its last RESOLVE answered, for
    /// [`Self::kept_desc`]. This client's own DDL empties it.
    kept: HashMap<RelName, Arc<RelDescriptor>>,
}

// `gnitz-py` runs these with the GIL released and `gnitz-tokio` spawns them,
// and both need `Send`. Asserted here, a breach names the field that caused it.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn assert_send_value<T: Send>(_: T) {}
    assert_send::<GnitzClient>();
    assert_send::<Sent<ScanReply>>();
    let _ = |mut c: GnitzClient, schema: &Arc<Schema>, batch: &ZSetBatch| {
        assert_send_value(c.poll_mirror(Duration::ZERO));
        assert_send_value(c.create_schema(""));
        assert_send_value(c.push(0, schema, batch, WireConflictMode::Update));
        assert_send_value(serve(c, |_| Poll::Ready(None)));
    };
    assert_send::<ZSetBatch>();
    assert_send::<ClientError>();
    // Handed back as `Arc<Schema>` by the scan path, and `Arc<T>: Send` requires
    // `T: Send + Sync`, so this one is the stricter bound.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Schema>();
};

impl GnitzClient {
    /// A blocking client: its verbs' futures complete under [`block_on`].
    pub fn connect(target: &str) -> Result<Self, ClientError> {
        block_on(Self::connect_with(target, Box::new(BlockingHost::default())))
    }

    /// A client that waits the way `host` does, connected where `host` lets
    /// work block.
    pub async fn connect_with(target: &str, mut host: Box<dyn Host>) -> Result<Self, ClientError> {
        let target = target.to_owned();
        let session = offload(&mut *host, move || Session::connect(&target)).await??;
        Self::over(session, host)
    }

    /// A blocking client over an already-connected session.
    #[cfg(test)]
    pub(crate) fn from_session(session: Session) -> GnitzClient {
        Self::over(session, Box::new(BlockingHost::default())).expect("a blocking host attaches to any socket")
    }

    /// A client over an already-connected session.
    pub fn over(session: Session, mut host: Box<dyn Host>) -> Result<GnitzClient, ClientError> {
        host.attach(session.as_fd())?;
        Ok(GnitzClient {
            host,
            session,
            serial_cache: HashMap::new(),
            txn: None,
            mirror: None,
            kept: HashMap::new(),
        })
    }

    /// Requests this connection has submitted. Exposed for the
    /// round-trip-count assertions; see [`Session::requests_sent`].
    pub fn requests_sent(&self) -> u64 {
        self.session.requests_sent()
    }

    // ── Waiting ────────────────────────────────────────────────────────────

    fn ack(&mut self, req: Request<'_>) -> Pending<'_, u64> {
        let sent = self.session.submit(req);
        Pending::submitted(self, sent)
    }

    /// Step this client's session until `sent` is answered. Replies arrive in
    /// request order, so every reply detached before it is answered by then.
    pub fn wait<T>(&mut self, sent: Sent<T>) -> Pending<'_, T> {
        Pending { client: self, sent }
    }

    /// Wait for the socket and step once. `Closed` when nothing is outstanding.
    pub async fn turn(&mut self) -> Result<(), ClientError> {
        let GnitzClient { session, host, .. } = self;
        poll_fn(|cx| poll_turn(session, &mut **host, cx)).await
    }

    /// Step until the connection is under its in-flight caps, at which a
    /// request is refused.
    pub async fn make_room(&mut self) -> Result<(), ClientError> {
        while self.session.at_capacity() {
            self.turn().await?;
        }
        Ok(())
    }

    /// Run `job` where this client's host lets work block.
    pub async fn offload<R: Send + 'static>(
        &mut self,
        job: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, ClientError> {
        offload(&mut *self.host, job).await
    }

    /// Reserve `count` contiguous SERIAL ids for `table` and return the first,
    /// so an INSERT that knows its row count pays one fsynced durable advance
    /// rather than `ceil(count / SERIAL_RANGE_SIZE)`. An abandoned tail — the old
    /// range's, or this reservation's — is the intentional PostgreSQL-style gap.
    pub async fn reserve_serial_ids(&mut self, table: &RelDescriptor, count: u64) -> Result<u64, ClientError> {
        match self.serial_cache.get_mut(&table.tid) {
            Some(r) if r.end - r.start >= count => {
                let base = r.start;
                r.start += count;
                Ok(base)
            }
            // Refill, abandoning whatever tail the old range still held.
            _ => {
                let want = count.max(SERIAL_RANGE_SIZE);
                let base = self
                    .ack(Request::AllocSerial { table: table.into(), count: want })
                    .await?;
                self.serial_cache.insert(table.tid, base + count..base + want);
                Ok(base)
            }
        }
    }

    // --- Raw ops ---

    /// Allocate a run of `n` catalog object ids, returning its first.
    async fn alloc_ids(&mut self, n: u64) -> Result<u64, ClientError> {
        self.ack(Request::AllocIds(n)).await
    }

    /// Allocate one catalog object id (schema, relation or index).
    pub async fn alloc_id(&mut self) -> Result<u64, ClientError> {
        self.alloc_ids(1).await
    }

    /// Push `batch` into `target` under `mode`. SQL `INSERT` uses `Error` to get
    /// SQL-standard rejection semantics; every other caller passes `Update`.
    ///
    /// Inside an open transaction the batch is buffered instead of sent, and the
    /// returned LSN is `0` — nothing is durable until `txn_commit`, which returns
    /// the one zone LSN covering the whole bundle. A batch passed by value moves
    /// into the buffer; a borrowed one is cloned into it.
    pub fn push<'a>(
        &mut self,
        target: impl Into<Target>,
        schema: &Arc<Schema>,
        batch: impl Into<Cow<'a, ZSetBatch>>,
        mode: WireConflictMode,
    ) -> Pending<'_, u64> {
        let (target, batch) = (target.into(), batch.into());
        if let Some(txn) = &mut self.txn {
            let buffered = txn.push(target, schema, batch.into_owned(), mode, BLIND);
            let sent = Sent::ready(buffered.map(|()| 0));
            return Pending { client: self, sent };
        }
        let batch = &*batch;
        self.ack(Request::Push { target, schema, batch, mode })
    }

    /// Run a parameterized bounded read, replied in `reply_schema`'s layout.
    pub fn scan_spec(
        &mut self,
        target: impl Into<Target>,
        spec: &ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Pending<'_, ScanReply> {
        let target = target.into();
        let sent = self.session.submit_scan(target, spec, reply_schema);
        Pending::submitted(self, sent)
    }

    // ── The read seam ──────────────────────────────────────────────────────
    //
    // `resolve` and `scan_spec` are the connection; `mirrored_desc` and
    // `scan_spec_local_first` below consult the copy first, and `kept_desc`
    // the last RESOLVE's answer, so every call site
    // declares which freshness it is asking for. **The gate is what the copy
    // holds, never whether a store is attached**, so a client with one reads
    // exactly like a client without for every relation the copy does not hold.

    /// The descriptor of the mirrored view `name`, while its copy answers
    /// reads. As stale as the copy.
    pub fn mirrored_desc(&self, name: &RelName) -> Option<Arc<RelDescriptor>> {
        let view = self.mirror.as_deref()?.answering(name)?;
        Some(Arc::clone(&view.desc))
    }

    /// What the last RESOLVE answered for `name`. Only a request
    /// carrying its token finds out whether it is stale; a planning error or an
    /// answer that needs no request is the caller's to repeat from
    /// [`Self::resolve`].
    pub fn kept_desc(&self, name: &RelName) -> Option<Arc<RelDescriptor>> {
        self.kept.get(name).cloned()
    }

    /// [`Self::scan_spec`], answered off the copy when it holds `target`, with
    /// no served LSN: a copy's freshness is [`Self::cursor_of`].
    pub fn scan_spec_local_first(
        &mut self,
        target: impl Into<Target>,
        spec: ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Pending<'_, ScanReply> {
        let target = target.into();
        let Some(store) = self
            .mirror
            .as_deref()
            .filter(|m| m.cursor_of(target.tid).is_some())
            .map(|m| &m.store)
        else {
            return self.scan_spec(target, &spec, reply_schema);
        };
        let reply = store
            .get()
            .scan_spec(target.tid, spec, reply_schema)
            .map(|batch| ScanReply {
                batch,
                schema: Arc::clone(reply_schema),
                lsn: None,
            });
        let sent = Sent::ready(reply.map_err(ClientError::from));
        Pending { client: self, sent }
    }

    /// Replace the connection and keep the copies — what a host does after a
    /// server restart, which kills the socket while the copies survive it.
    ///
    /// Refused while a transaction is open. A failed connect, or a host that
    /// refuses the new socket, leaves this client exactly as it was.
    ///
    /// Every copy keeps its cursor and answers no read until the next poll: the
    /// new connection may be a different server. A poisoned store crosses
    /// unchanged.
    pub async fn reconnect(&mut self, target: &str) -> Result<(), ClientError> {
        if self.txn_active() {
            return Err(ClientError::from(
                "reconnect inside a transaction; commit or roll back first".to_string(),
            ));
        }
        let target = target.to_owned();
        let fresh = self.offload(move || Session::connect(&target)).await??;
        // Every field, so one added later is decided here too.
        let GnitzClient {
            host,
            session,
            serial_cache,
            txn: _,
            mirror,
            kept,
        } = self;
        host.attach(fresh.as_fd())?;
        *session = fresh;
        serial_cache.clear();
        kept.clear();
        if let Some(m) = mirror.as_deref_mut() {
            m.connection_replaced();
        }
        Ok(())
    }

    /// Bootstrap a view's delta feed: the view's whole current value at its true
    /// net weights, and the cursor to poll from. It replaces a copy's state; it
    /// does not add to it.
    ///
    /// `spec` is an encoded `ReadSpec` forwarding rows with no cut: the reply is
    /// that spec applied to the view, in `reply_schema`, which is the view's own
    /// under `ReadSpec::all_rows` of no bound. Every later poll of the copy
    /// carries the same `spec`.
    pub async fn delta_bootstrap(
        &mut self,
        view: impl Into<Target>,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
    ) -> Result<(ScanReply, DeltaCursor), ClientError> {
        self.delta_read(view.into(), None, Duration::ZERO, reply_schema, spec)
            .await
    }

    /// Poll a view's delta feed: every delta it emitted in `(cursor.tick, T]`,
    /// under `spec`, with the cursor to poll from next. The reply comes back in
    /// `reply_schema`, weights and all. Apply what comes back and store the new
    /// cursor; there is nothing to reconcile.
    ///
    /// A cursor the feed does not continue — its rounds dropped, or handed out
    /// by another boot, relation or `spec` — is refused as `DeltaExpired`:
    /// discard the copy and [`delta_bootstrap`](Self::delta_bootstrap) again.
    ///
    /// It carries every push acknowledged before it. With nothing to report, the
    /// server holds the reply until a round leaves the view a row `spec` keeps,
    /// or `wait` passes.
    pub async fn delta_poll(
        &mut self,
        view: impl Into<Target>,
        cursor: DeltaCursor,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
        wait: Duration,
    ) -> Result<(ScanReply, DeltaCursor), ClientError> {
        self.delta_read(view.into(), Some(cursor), wait, reply_schema, spec)
            .await
    }

    /// One view's delta read after `from` — the view whole with none —
    /// decoded under `reply_schema`, with the terminal frame's `(tag, T)` pair
    /// as a cursor.
    async fn delta_read(
        &mut self,
        view: Target,
        from: Option<DeltaCursor>,
        wait: Duration,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
    ) -> Result<(ScanReply, DeltaCursor), ClientError> {
        let (tag, after_tick) = DeltaCursor::flat(from);
        let item = DeltaPollItem {
            view,
            tag,
            after_tick,
            reply_layout: reply_schema.layout_digest(),
            spec,
        };
        let schema = Arc::clone(reply_schema);
        let mut batch = ZSetBatch::new(&schema);
        let GnitzClient { session, host, .. } = self;
        let mut poll = DeltaPoll::start(session, &[item], wait);
        let mut end = None;
        while let Some((_, polled)) = poll.next(&mut **host).await? {
            match polled {
                Polled::Block(b) => crate::protocol::wal_block::decode_wal_block_into(&mut batch, b.block(), &schema)?,
                Polled::End(e) => end = Some(e),
            }
        }
        let cursor = end.expect("one item ends once")?;
        Ok((ScanReply { schema, batch, lsn: None }, cursor))
    }

    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table `txn_commit` is never observed torn
    /// across the result set.
    /// Each relation is replied in the layout of the schema paired with it.
    pub fn scan_many(&mut self, relations: Vec<(u64, Arc<Schema>)>) -> Pending<'_, Vec<ScanReply>> {
        let sent = self.session.submit_scan_multi(relations);
        Pending::submitted(self, sent)
    }

    /// Index `cols` of relation `owner_id`, in that order, under the catalog name
    /// `index_name`.
    pub async fn create_index(
        &mut self,
        owner_id: u64,
        cols: PkColList,
        index_name: &str,
        is_unique: bool,
    ) -> Result<u64, ClientError> {
        let index_name = gnitz_wire::canonical_identifier(index_name)?;
        let index_id = self.alloc_id().await?;
        let mut b = DdlBundle::default();
        b.put(
            &IdxTabRow {
                index_id,
                owner_id,
                source_col_idx: cols.pack(),
                name: &index_name,
                is_unique: is_unique as u64,
            },
            1,
        );
        self.commit_ddl(b).await?;
        Ok(index_id)
    }

    /// Drop indexes by name as **one** DDL zone: the whole set retires or none of
    /// it does, and a name repeated in `index_names` retires once.
    pub async fn drop_indexes_by_name(&mut self, index_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(index_names, "index", if_exists, |_| true).await
    }

    /// `ALTER TABLE … DROP CONSTRAINT`: the UNIQUE index `name` of table `tid`.
    /// The `-1` is the stored row, so the engine's CAS re-proves owner and
    /// uniqueness against the live row.
    pub async fn drop_unique_constraint(&mut self, tid: u64, name: &str, if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(&[name], "constraint", if_exists, |r| r.owner == tid && r.is_unique)
            .await
    }

    /// Retract the live IDX_TAB rows named in `names` that pass `matches`.
    async fn drop_index_rows(
        &mut self,
        names: &[&str],
        noun: &'static str,
        if_exists: bool,
        matches: impl Fn(&IndexRow) -> bool,
    ) -> Result<(), ClientError> {
        let scanned = self.sys_rows(IDX_TAB, ReadBound::None).await?;
        let rows = idx_rows(&scanned)?;
        let mut b = DdlBundle::default();
        let mut retired: Vec<usize> = Vec::with_capacity(names.len());
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            match rows.iter().find(|(_, r)| r.name == name && matches(r)) {
                Some(&(i, _)) => {
                    // A repeated name retires once: the row is already in the batch.
                    if !retired.contains(&i) {
                        retired.push(i);
                        b.batch(IDX_TAB).copy_row_at(&scanned, i, -1);
                    }
                }
                None if if_exists => {}
                None => return Err(absent(format!("{noun} '{name}' not found"))),
            }
        }
        self.commit_ddl(b).await
    }

    /// Every live secondary index. Names come back canonical (lowercase): every
    /// writer folds them at store time.
    pub async fn index_rows(&mut self) -> Result<Vec<IndexRow>, ClientError> {
        let scanned = self.sys_rows(IDX_TAB, ReadBound::None).await?;
        Ok(idx_rows(&scanned)?.into_iter().map(|(_, r)| r).collect())
    }

    // --- Transactions (BEGIN / COMMIT / ROLLBACK) ---
    //
    // One transaction per client: `txn_begin` opens the buffer every write path
    // then routes into, `txn_commit` ships it as one atomic frame, `txn_rollback`
    // discards it. All state-machine errors are raised here; the SQL dispatch
    // arms and the Python context manager only translate them.

    /// True while a transaction is open.
    pub fn txn_active(&self) -> bool {
        self.txn.is_some()
    }

    /// Open a transaction; errors if one is already open. Each family it buffers
    /// carries the basis of the read it was built from, which COMMIT checks.
    pub fn txn_begin(&mut self) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::from("transaction already open".to_string()));
        }
        self.txn = Some(TxnBuffer::default());
        Ok(())
    }

    /// Discard the open transaction (ROLLBACK): drop the buffer, sending
    /// nothing. Errors if no transaction is open.
    pub fn txn_rollback(&mut self) -> Result<(), ClientError> {
        self.txn.take().map(|_| ()).ok_or_else(no_transaction)
    }

    /// Commit the open transaction atomically and return its durable zone LSN, `0`
    /// when it wrote nothing. Errors if no transaction is open; the transaction is
    /// closed even when the commit fails.
    ///
    /// A `StaleCatalog` refusal is reported as `TxnConflict`: a relation the
    /// transaction wrote has been altered since this client resolved it — which
    /// may have been before BEGIN, a write buffered under a kept descriptor
    /// sending no request of its own. The recovery is a conflict's: nothing was
    /// written, and the transaction run again resolves the relations it wrote
    /// afresh.
    pub async fn txn_commit(&mut self) -> Result<u64, ClientError> {
        let buf = self.txn.take().ok_or_else(no_transaction)?;
        self.push_txn(&buf).await.map_err(|e| match e {
            ClientError::Refused(WireFault { status: WireStatus::StaleCatalog, text }) => {
                self.kept.retain(|_, rel| !buf.families_of.contains_key(&rel.tid));
                ClientError::Refused(WireFault {
                    status: WireStatus::TxnConflict,
                    text: format!("transaction conflict: {text}; retry"),
                })
            }
            e => e,
        })
    }

    /// Ship `buf` as one `PUSH_TXN` frame, whose families land together under one
    /// zone LSN or not at all; an empty buffer sends nothing.
    async fn push_txn(&mut self, buf: &TxnBuffer) -> Result<u64, ClientError> {
        if buf.families.is_empty() {
            return Ok(0);
        }
        self.ack(Request::PushTxn { families: &buf.families }).await
    }

    /// Read `target`'s rows under `bound` and `predicate` — only their keys when
    /// `keys` — as the open transaction sees them, hand them to `build`, and write
    /// what it returns on the condition that `target` was not written after the
    /// read. Returns the written row count.
    ///
    /// Inside a transaction COMMIT checks the condition. In autocommit the statement
    /// is a transaction of its own, and a conflict re-reads and rebuilds, up to
    /// [`RMW_MAX_ATTEMPTS`] times.
    pub async fn read_modify_write<E: From<ClientError>>(
        &mut self,
        target: &RelDescriptor,
        bound: ReadBound,
        predicate: Vec<u8>,
        keys: bool,
        mut build: impl FnMut(ZSetBatch) -> Result<ZSetBatch, E>,
    ) -> Result<usize, E> {
        let (tid, schema) = (target.tid, &target.schema);
        let (reply, sink) = match keys {
            true => key_reply(schema),
            false => (Arc::clone(schema), ReadSink::all_rows()),
        };
        let spec = ReadSpec { bound, predicate, sink };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let rest = match (self.txn.as_mut(), &spec.bound) {
                (Some(txn), ReadBound::PkSet(set)) => txn.unwritten(tid, set),
                _ => None,
            };
            let (batch, basis) = match rest {
                // Every key is the transaction's own: nothing is read, so the write
                // depends on no committed state.
                Some(rest) if rest.is_empty() => (ZSetBatch::new(&reply), BLIND),
                rest => {
                    let narrowed = rest.map(|rest| ReadSpec {
                        bound: ReadBound::PkSet(rest),
                        predicate: spec.predicate.clone(),
                        sink: spec.sink.clone(),
                    });
                    let ScanReply { batch, lsn, .. } = self
                        .scan_spec(target, narrowed.as_ref().unwrap_or(&spec), &reply)
                        .await?;
                    (batch, lsn.expect("a server read carries its watermark"))
                }
            };
            let mut own = TxnBuffer::default();
            let txn = self.txn.as_mut().unwrap_or(&mut own);
            let batch = build(txn.overlay(tid, schema, &spec, keys, batch)?)?;
            let count = batch.len();
            txn.push(target, schema, batch, WireConflictMode::Update, basis)?;
            let pushed = match self.txn {
                Some(_) => Ok(0),
                None => self.push_txn(&own).await,
            };
            match pushed {
                Err(ClientError::Refused(WireFault { status: WireStatus::TxnConflict, .. }))
                    if attempt < RMW_MAX_ATTEMPTS => {}
                r => {
                    r?;
                    return Ok(count);
                }
            }
        }
    }

    // --- DDL ---

    /// Commit `bundle` as one DDL zone. One that holds no row opens no zone.
    async fn commit_ddl(&mut self, bundle: DdlBundle) -> Result<(), ClientError> {
        if bundle.0.is_empty() {
            return Ok(());
        }
        if self.txn.is_some() {
            return Err(ClientError::from("DDL is not allowed inside a transaction".to_string()));
        }
        self.ack(Request::DdlTxn(&bundle.0)).await?;
        self.kept.clear();
        self.after_ddl_commit(&bundle.0).await;
        Ok(())
    }

    /// Drop the copy of every view the bundle retracted, except a mirrored view
    /// the bundle also wrote back — a rename — which is rebound under its new
    /// name.
    async fn after_ddl_commit(&mut self, families: &[(u64, ZSetBatch)]) {
        let Some(m) = self.mirror.as_deref() else {
            return;
        };
        let Some((_, b)) = families.iter().find(|(family, _)| *family == VIEW_TAB) else {
            return;
        };
        let vid = |i| b.pks.get(i) as u64;
        let mut dropped: Vec<u64> = Vec::new();
        let mut renamed = Vec::new();
        for v in (0..b.len()).filter(|&i| b.weights[i] < 0).map(vid) {
            match (m.views.get(&v), b.live_rows().find(|&j| vid(j) == v)) {
                (Some(view), Some(j)) => renamed.push(
                    view.renamed(
                        payload_str(b, j, RELTAB_PAY_NAME)
                            .ok()
                            .and_then(|new| view.name.sibling(new).ok())
                            .expect("a bundle this client built names its views"),
                    ),
                ),
                _ => dropped.push(v),
            }
        }
        for v in dropped {
            let _ = self.forget_view(v).await;
        }
        for entry in renamed {
            let tid = entry.desc.tid;
            if self.bind(entry).await.is_err() {
                let _ = self.forget_view(tid).await;
            }
        }
    }

    pub async fn create_schema(&mut self, name: &str) -> Result<u64, ClientError> {
        // Refuses the empty string, a leading `_` (the reserved system prefix)
        // and illegal characters.
        let name = gnitz_wire::canonical_identifier(name)?;
        let schema_id = self.alloc_id().await?;
        let mut b = DdlBundle::default();
        b.put(&SchemaTabRow { schema_id, name: &name }, 1);
        self.commit_ddl(b).await?;
        Ok(schema_id)
    }

    /// Drop a schema and every table and view it contains — PostgreSQL
    /// `DROP SCHEMA … CASCADE` semantics — as **one** atomic DDL bundle, so a
    /// schema of any size costs one `fdatasync` and one worker broadcast.
    ///
    /// All-negative, which is what makes the engine apply it VIEW → TABLE →
    /// SCHEMA: each view is retired before the tables it reads, and the schema row
    /// last, by which time its member-count guard sees an empty schema.
    ///
    /// Atomic in both directions: an external dependent (a cross-schema FK child
    /// or view-on-view) or a rename landing between the scans and the push fails
    /// the whole bundle and drops nothing.
    pub async fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
        let name = gnitz_wire::canonical_identifier(name)?;
        let (schemas, at) = self.lookup_schema(&name).await?;
        let schema_id = schemas.pks.get(at) as u64;

        // Hidden segments need no separate pass: each is an ordinary VIEW_TAB row
        // carrying this `schema_id`, so the whole matching set is already complete
        // — and the engine's co-drop carve-out admits it, every dependent being in
        // the same drop set.
        let mut b = DdlBundle::default();
        for family in [VIEW_TAB, TABLE_TAB] {
            let scanned = self.sys_rows(family, ReadBound::None).await?;
            for i in scanned.live_rows() {
                if payload_u64(&scanned, i, RELTAB_PAY_SCHEMA_ID) == schema_id {
                    b.batch(family).copy_row_at(&scanned, i, -1);
                }
            }
        }
        b.batch(SCHEMA_TAB).copy_row_at(&schemas, at, -1);
        self.commit_ddl(b).await
    }

    /// Register a table, its FOREIGN KEY columns and its inline UNIQUE indexes as
    /// one DDL bundle. `fks[i]` is column `i`'s target; an empty `fks` gives no
    /// column one.
    pub async fn create_table(
        &mut self,
        table: &RelName,
        schema: &Schema,
        fks: &[Option<FkTarget>],
        props: TableProps,
        unique_indexes: &[InlineUniqueIndex],
    ) -> Result<u64, ClientError> {
        let index_names: Vec<String> = unique_indexes
            .iter()
            .map(|spec| gnitz_wire::canonical_identifier(&spec.name))
            .collect::<Result<Vec<_>, String>>()?;
        // `PkColList::from_slice` panics on a schema this refuses.
        schema
            .validate()
            .map_err(|e| ClientError::from(format!("create_table: {e}")))?;
        let pk = PkColList::from_slice(&schema.pk_cols);
        if !fks.is_empty() && fks.len() != schema.columns.len() {
            return Err(ClientError::from(format!(
                "create_table: {} foreign-key slots for {} columns",
                fks.len(),
                schema.columns.len()
            )));
        }

        let (schemas, at) = self.lookup_schema(table.schema()).await?;
        let schema_id = schemas.pks.get(at) as u64;
        // The table's id, then one per inline UNIQUE index.
        let new_tid = self.alloc_ids(1 + unique_indexes.len() as u64).await?;

        let mut b = DdlBundle::default();
        append_col_rows(&mut b, new_tid, &schema.columns, fks);
        b.put(
            &TableTabRow {
                table_id: new_tid,
                schema_id,
                name: table.name(),
                pk_col_idx: pk.pack(),
                flags: props.pack(),
            },
            1,
        );
        for (k, spec) in unique_indexes.iter().enumerate() {
            b.put(
                &IdxTabRow {
                    index_id: new_tid + 1 + k as u64,
                    owner_id: new_tid,
                    source_col_idx: spec.cols.pack(),
                    name: &index_names[k],
                    is_unique: 1,
                },
                1,
            );
        }
        self.commit_ddl(b).await?;

        Ok(new_tid)
    }

    /// Drop tables as one DDL zone; the engine cascades each one's indexes off
    /// their owner. See [`Self::drop_relations`] for the batch rules.
    pub async fn drop_table(&mut self, tables: &[RelName], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(TABLE_TAB, "table", tables, if_exists).await
    }

    /// Create a passthrough view over `source` and return its id.
    pub async fn create_view(
        &mut self,
        view: &RelName,
        source: &RelDescriptor,
        props: ViewProps,
    ) -> Result<u64, ClientError> {
        // A minimal SCAN_DELTA → INTEGRATE_SINK circuit, built through the typed
        // builder so the row materialisation matches the stored layout exactly.
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(source.tid, ReadBound::None);
        circuit.sink(scan);

        // A passthrough's layout must equal its source's, so the whole output
        // schema and its PK-repeat flag are the source's own.
        let planned = PlannedView {
            circuit,
            schema: Arc::clone(&source.schema),
            pk_repeats: source.pk_repeats,
        };
        self.create_view_chain(view, planned.into(), props, None).await
    }

    /// Create `bundle` as `view` in one atomic `DDL_TXN` and return its id.
    /// `replace` is the id of the view it supersedes in the same zone. One that
    /// already is `bundle` stays under its id, and nothing is written.
    pub async fn create_view_chain(
        &mut self,
        view: &RelName,
        bundle: ViewBundle,
        props: ViewProps,
        replace: Option<u64>,
    ) -> Result<u64, ClientError> {
        let view_name = view.name();
        let n_views = bundle.segments.len() + 1;
        if n_views > MAX_CHAIN_SEGMENTS {
            return Err(ClientError::from(format!(
                "view chain has {n_views} segments, exceeding the {MAX_CHAIN_SEGMENTS}-segment limit",
            )));
        }
        // Before any allocation, so a bad schema leaves no residue and never
        // reaches `PkColList::from_slice`, which panics on one.
        for (k, pv) in bundle.segments.iter().chain([&bundle.view]).enumerate() {
            pv.schema
                .validate()
                .map_err(|e| ClientError::from(format!("View '{view_name}' segment {k}: {e}")))?;
        }
        let n_segments = bundle.segments.len() as u64;

        let mut b = DdlBundle::default();
        let schema_id = match replace {
            Some(vid) => {
                // Where `bundle` stands if it is the chain under `vid`: one
                // allocation holds a chain, the user-named view last.
                let base = vid.saturating_sub(n_segments);
                let members = ReadSpec::all_rows(ReadBound::Range(KeyRange::new(
                    PkColList::from_slice(&[0]),
                    &[],
                    Cut::before(base as u128),
                    Cut::after(vid as u128),
                )));
                // One round trip: every family's read is sent before the first is awaited.
                let reads = [VIEW_TAB, COL_TAB, CIRCUIT_TAB]
                    .map(|family| (family, self.scan_spec(family, &members, sys_schema(family)).detach()));
                let mut standing = DdlBundle::default();
                for (family, read) in reads {
                    standing.0.push((family, self.wait(read).await?.batch));
                }
                let views = &standing.0[0].1;
                let i = views
                    .live_rows()
                    .find(|&i| views.pks.get(i) as u64 == vid)
                    .ok_or_else(|| not_found("view", view))?;
                let schema_id = payload_u64(views, i, RELTAB_PAY_SCHEMA_ID);

                let mut want = DdlBundle::default();
                bundle.put_rows(&mut want, view_name, props, schema_id, base);
                if want.same_rows(&standing) {
                    return Ok(vid);
                }
                // Only the user-named view, ahead of the new chain's `+1`s: the
                // engine cascades its segments.
                b.batch(VIEW_TAB).copy_row_at(views, i, -1);
                schema_id
            }
            None => {
                let (schemas, at) = self.lookup_schema(view.schema()).await?;
                schemas.pks.get(at) as u64
            }
        };

        let base = self.alloc_ids(n_views as u64).await?;
        bundle.put_rows(&mut b, view_name, props, schema_id, base);
        self.commit_ddl(b).await?;
        Ok(base + n_segments)
    }

    /// Drop views as one DDL zone; the engine cascades each one's hidden segments
    /// off `owner_view_id`, so the client never names a segment. See
    /// [`Self::drop_relations`] for the batch rules.
    pub async fn drop_view(&mut self, views: &[RelName], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(VIEW_TAB, "view", views, if_exists).await
    }

    /// Retire every named relation of `family` in **one** DDL zone: the whole set
    /// goes or none does, and a name repeated in `names` retires once.
    ///
    /// `if_exists` answers a name that does not resolve, and nothing else: a name
    /// that resolves to another family, a dependent view and an FK child outside
    /// the batch all still fail the statement.
    async fn drop_relations(
        &mut self,
        family: u64,
        noun: &'static str,
        names: &[RelName],
        if_exists: bool,
    ) -> Result<(), ClientError> {
        let mut b = DdlBundle::default();
        let mut retired: Vec<u64> = Vec::with_capacity(names.len());
        for name in names {
            let Some((scanned, i)) = self.relation_retraction(family, noun, name).await? else {
                if if_exists {
                    continue;
                }
                return Err(not_found(noun, name));
            };
            let id = scanned.pks.get(i) as u64;
            if !retired.contains(&id) {
                retired.push(id);
                b.batch(family).copy_row_at(&scanned, i, -1);
            }
        }
        self.commit_ddl(b).await
    }

    /// The live `family` row of `name`, by one master-local seek. `Ok(None)` is the
    /// *resolve* miss alone; a name that resolves but holds no row in `family` —
    /// `DROP TABLE <view>` — is the hard `not_found`.
    async fn relation_retraction(
        &mut self,
        family: u64,
        noun: &'static str,
        name: &RelName,
    ) -> Result<Option<(ZSetBatch, usize)>, ClientError> {
        let Some(desc) = self.resolve(name).await? else {
            return Ok(None);
        };
        let (scanned, i) = self
            .seek_sys_row(family, &[desc.tid as u128], || not_found(noun, name))
            .await?;
        // Renamed since the resolve: the name no longer denotes this relation.
        if payload_bytes(&scanned, i, RELTAB_PAY_NAME) != name.name().as_bytes() {
            return Ok(None);
        }
        Ok(Some((scanned, i)))
    }

    /// Rename `rel`: a `(-1, +1)` rewrite pair of its TABLE_TAB / VIEW_TAB row.
    pub async fn alter_rename_relation(&mut self, rel: &RelDescriptor, new_name: &str) -> Result<(), ClientError> {
        let new_name = gnitz_wire::canonical_identifier(new_name)?;
        let tid = rel.tid;
        let family = if rel.class.is_view() { VIEW_TAB } else { TABLE_TAB };
        self.rewrite_sys_row(
            family,
            &[tid as u128],
            || absent(format!("relation {tid} not found")),
            |b, row| b.set_string_cell(row, RELTAB_PAY_NAME, &new_name),
        )
        .await
    }

    pub async fn alter_rename_column(&mut self, tid: u64, col_idx: usize, new_col: &str) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| {
            b.set_string_cell(row, ColTabSlot::name as usize, new_col)
        })
        .await
    }

    /// `ALTER TABLE … DROP COLUMN`: the column stays physically present, so the
    /// table keeps its layout and comparator.
    pub async fn alter_drop_column(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| {
            b.set_u64_cell(row, ColTabSlot::is_hidden as usize, 1)
        })
        .await
    }

    /// `ALTER TABLE … ALTER COLUMN … DROP NOT NULL`: a `(-1, +1)` COL_TAB rewrite
    /// pair, only `is_nullable` flipped to true at `+1`. Once the catalog reports
    /// the column nullable, `ZSetBatch::validate` permits a null bit there.
    pub async fn alter_drop_not_null(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| {
            b.set_u64_cell(row, ColTabSlot::is_nullable as usize, 1)
        })
        .await
    }

    /// `ALTER TABLE … ADD COLUMN`: `def` appended to `rel` after every physical
    /// column, dropped ones included.
    pub async fn alter_add_column(&mut self, rel: &RelDescriptor, def: &ColumnDef) -> Result<(), ClientError> {
        let (tid, col_idx) = (rel.tid, rel.schema.num_columns());

        let mut b = DdlBundle::default();
        b.put(&ColTabRow::of(tid, col_idx as u64, def, None), 1);
        self.commit_ddl(b).await
    }

    /// [`Self::rewrite_sys_row`] on column `col_idx` of relation `tid`.
    async fn alter_col_pair(
        &mut self,
        tid: u64,
        col_idx: usize,
        patch: impl FnOnce(&mut ZSetBatch, usize),
    ) -> Result<(), ClientError> {
        self.rewrite_sys_row(
            COL_TAB,
            &[tid as u128, col_idx as u128],
            || absent(format!("column index {col_idx} not found on table {tid}")),
            patch,
        )
        .await
    }

    /// Push a `(-1, +1)` rewrite pair on the live `family` row keyed `key`: the
    /// stored row at `-1`, and a copy of it at `+1` that `patch` edits in place.
    async fn rewrite_sys_row(
        &mut self,
        family: u64,
        key: &[u128],
        missing: impl FnOnce() -> ClientError,
        patch: impl FnOnce(&mut ZSetBatch, usize),
    ) -> Result<(), ClientError> {
        let (scanned, i) = self.seek_sys_row(family, key, missing).await?;
        let mut b = DdlBundle::default();
        let rows = b.batch(family);
        rows.copy_row_at(&scanned, i, -1);
        rows.copy_row_at(&scanned, i, 1);
        patch(rows, 1);
        self.commit_ddl(b).await
    }

    /// [`Self::resolve`], with a missing relation an error.
    pub async fn resolve_relation(&mut self, name: &RelName) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(name).await?.ok_or_else(|| not_found("relation", name))
    }

    // --- Relation resolution ---

    /// The descriptor for `name` as the server answers it now, or `None` when
    /// no such relation exists; `Err` is a missing schema or a decode error.
    pub async fn resolve(&mut self, name: &RelName) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let sent = self.session.submit_resolve(name.key());
        let found = Pending::submitted(self, sent).await?;
        match &found {
            Some(desc) => self.kept.insert(name.clone(), Arc::clone(desc)),
            None => self.kept.remove(name),
        };
        Ok(found)
    }

    // --- Private catalog-lookup helpers ---

    /// The live SCHEMA_TAB row named `schema_name` (already canonical): the scanned
    /// batch and the row's index in it.
    async fn lookup_schema(&mut self, schema_name: &str) -> Result<(ZSetBatch, usize), ClientError> {
        let batch = self.sys_rows(SCHEMA_TAB, ReadBound::None).await?;
        let i = batch
            .live_rows()
            .find(|&i| payload_bytes(&batch, i, SchemaTabSlot::name as usize) == schema_name.as_bytes())
            .ok_or_else(|| absent(format!("schema '{schema_name}' not found")))?;
        Ok((batch, i))
    }

    /// System family `family`'s rows under `bound`, decoded under its own schema;
    /// the server checks the reply against that schema's layout.
    async fn sys_rows(&mut self, family: u64, bound: ReadBound) -> Result<ZSetBatch, ClientError> {
        Ok(self
            .scan_spec(family, &ReadSpec::all_rows(bound), sys_schema(family))
            .await?
            .batch)
    }

    /// The live `family` row keyed `key` — its PK columns' native values in
    /// PK-list order — by one master-local keyed read: the
    /// reply batch and the row's index in it, for a caller to copy the stored row
    /// out of. `PkSet` is exact, so a live row in the reply is that key's; none is
    /// `missing()`.
    async fn seek_sys_row(
        &mut self,
        family: u64,
        key: &[u128],
        missing: impl FnOnce() -> ClientError,
    ) -> Result<(ZSetBatch, usize), ClientError> {
        let mut keys = PkColumn::empty_for_schema(sys_schema(family));
        keys.push_natives(key);
        let batch = self.sys_rows(family, ReadBound::PkSet(keys.keys())).await?;
        let i = batch.live_rows().next().ok_or_else(missing)?;
        Ok((batch, i))
    }
}

/// A delta poll in flight: `items` in one request, handed out as each item's
/// blocks and then its one end, the items in order. Dropped unfinished, the
/// session drops what is left of its trains.
pub(crate) struct DeltaPoll<'s> {
    session: &'s mut Session,
    total: usize,
    /// Ends handed out so far: the index of the item being answered.
    answered: usize,
}

impl Drop for DeltaPoll<'_> {
    fn drop(&mut self) {
        self.session.abandon_poll();
    }
}

impl<'s> DeltaPoll<'s> {
    pub(crate) fn start(session: &'s mut Session, items: &[DeltaPollItem], wait: Duration) -> Self {
        session.submit_delta_poll(items, wait);
        DeltaPoll { session, total: items.len(), answered: 0 }
    }

    /// Everything the last step queued has been handed out.
    pub(crate) fn drained(&self) -> bool {
        self.session.polled_out()
    }

    /// The next block or end, with its item's index; `None` once every item
    /// has ended. `Err` is the host's alone — an interrupt.
    pub(crate) async fn next(&mut self, host: &mut dyn Host) -> Result<Option<(usize, Polled)>, ClientError> {
        loop {
            if let Some(next) = self.session.next_polled() {
                let item = self.answered;
                self.answered += usize::from(matches!(next, Polled::End(_)));
                return Ok(Some((item, next)));
            }
            if self.answered == self.total {
                return Ok(None);
            }
            poll_fn(|cx| poll_turn(self.session, host, cx)).await?;
        }
    }
}

// --- TxnBuffer: the locally-buffered atomic write-batch transaction ---

fn no_transaction() -> ClientError {
    ClientError::from("no transaction open".to_string())
}

/// A transaction's buffered writes, which [`GnitzClient::push_txn`] ships as one
/// `PUSH_TXN` frame; dropping it sends nothing. Per tid, the families are the
/// maximal same-mode runs of the writes in call order, the order the engine
/// validates and applies them in.
#[derive(Default)]
struct TxnBuffer {
    /// Creation order; per tid, maximal same-mode runs of the caller's op
    /// sequence.
    families: Vec<PushFamily>,
    /// Per family, how many of its batch's rows are already folded into
    /// `last_op_of`. A batch only ever extends, so this is a watermark.
    indexed: Vec<usize>,

    /// tid → its family indices in creation order. Only the last one can still
    /// grow (a matching-mode append extends it), which is what makes indexing a
    /// tid's families in this order the same as indexing in append order.
    families_of: HashMap<u64, Vec<usize>>,
    /// tid → PK → `(family index, row index)` of the LAST op buffered on that
    /// PK. Row indices are stable: `push` only ever extends a family batch or
    /// pushes a new one. Weight-0 rows are not indexed — they are inert, exactly
    /// as the engine's fold treats them.
    last_op_of: HashMap<u64, HashMap<PkBuf, (usize, usize)>>,
}

impl TxnBuffer {
    /// The buffer's one write entry point: append `batch` to `tid`'s current run,
    /// or open a new family when the mode differs (or `tid` has no family yet).
    /// Empty batches contribute nothing and open no family.
    /// `basis`: the watermark of the read `batch` was built from, or `BLIND`. A
    /// family keeps the oldest basis of the batches it holds.
    ///
    /// `Error` mode rejects the whole transaction if any of these rows' PKs
    /// already exist, checked cumulatively in frame order against committed state
    /// and earlier families. A delete is a batch of `-1` rows in `Update` mode, so
    /// "delete k; insert k" emits an Update family `[D(k)]` then an Error family
    /// `[I(k)]`, in that order.
    ///
    /// Refused when `batch` is not in the layout `tid` already holds.
    fn push(
        &mut self,
        target: impl Into<Target>,
        schema: &Arc<Schema>,
        batch: ZSetBatch,
        mode: WireConflictMode,
        basis: u64,
    ) -> Result<(), ClientError> {
        let target = target.into();
        let tid = target.tid;
        batch
            .layout_matches(schema)
            .map_err(|e| ClientError::from(format!("relation {tid}: the batch is not in its schema's layout: {e}")))?;
        if batch.is_empty() {
            return Ok(());
        }
        // A batch in this very schema is in the layout the first family holds.
        self.check_layout(tid, |held| match Arc::ptr_eq(&held.schema, schema) {
            true => Ok(()),
            false => batch.layout_matches(&held.schema),
        })?;
        // Copied out, so no borrow of `families_of` spans the `families` read.
        let last = self.families_of.get(&tid).and_then(|v| v.last().copied());
        match last.filter(|&i| self.families[i].mode == mode) {
            Some(i) => {
                let f = &mut self.families[i];
                f.batch.extend_from_owned(batch);
                f.basis = f.basis.min(basis);
                if f.target.token == 0 {
                    f.target = target;
                }
            }
            None => {
                self.families_of.entry(tid).or_default().push(self.families.len());
                self.families.push(PushFamily {
                    target,
                    schema: Arc::clone(schema),
                    batch,
                    mode,
                    basis,
                });
                self.indexed.push(0);
            }
        }
        Ok(())
    }

    /// `matches` over `tid`'s first family; every later one shares its layout.
    fn check_layout(
        &self,
        tid: u64,
        matches: impl FnOnce(&PushFamily) -> Result<(), String>,
    ) -> Result<(), ClientError> {
        let Some(&first) = self.families_of.get(&tid).and_then(|v| v.first()) else {
            return Ok(());
        };
        matches(&self.families[first])
            .map_err(|e| ClientError::from(format!("relation {tid} changed layout during this transaction: {e}")))
    }

    /// Fold every row `tid` has buffered since the last catch-up into
    /// `last_op_of`. Each row is folded at most once, so the whole index costs
    /// O(rows buffered) across the transaction however often it is read — and
    /// nothing at all for a transaction that never reads.
    fn index_tid(&mut self, tid: u64) {
        let Some(own) = self.families_of.get(&tid) else {
            return;
        };
        let index = self.last_op_of.entry(tid).or_default();
        for &fam in own {
            let batch = &self.families[fam].batch;
            for row in std::mem::replace(&mut self.indexed[fam], batch.len())..batch.len() {
                if batch.weights[row] != 0 {
                    index.insert(PkBuf::from_bytes(batch.pks.get_bytes(row)), (fam, row));
                }
            }
        }
    }

    /// The keys of `set` this transaction has not written — what a read of
    /// `set` still has to ask the server. `None` when it wrote none of them.
    fn unwritten(&mut self, tid: u64, set: &PkKeys) -> Option<PkKeys> {
        self.index_tid(tid);
        let index = self.last_op_of.get(&tid)?;
        let first = set.iter().position(|k| index.contains_key(k))?;
        let stride = set.stride();
        let mut rest = Vec::with_capacity(set.as_bytes().len() - stride);
        rest.extend_from_slice(&set.as_bytes()[..first * stride]);
        for k in set.iter().skip(first + 1).filter(|k| !index.contains_key(*k)) {
            rest.extend_from_slice(k);
        }
        Some(PkKeys::from_sorted(stride, rest))
    }

    /// `committed`, a server read of `tid` under `spec`, as this transaction sees
    /// it: less every PK the transaction wrote, plus its live rows (last op per PK,
    /// weight > 0) that `spec` keeps, each at weight 1. With `keys`, `committed` is in the
    /// [`key_reply`] layout, and so are the added rows.
    fn overlay(
        &mut self,
        tid: u64,
        schema: &Schema,
        spec: &ReadSpec,
        keys: bool,
        committed: ZSetBatch,
    ) -> Result<ZSetBatch, ClientError> {
        self.check_layout(tid, |held| held.batch.layout_matches(schema))?;
        self.index_tid(tid);
        let Some(index) = self.last_op_of.get(&tid) else {
            return Ok(committed);
        };
        let keep: Vec<(usize, i64)> = (0..committed.len())
            .filter(|&i| !index.contains_key(committed.pks.get_bytes(i)))
            .map(|i| (i, committed.weights[i]))
            .collect();
        // `gather` compacts the string arena the dropped rows carried.
        let mut out = committed.gather(&keep);

        let mut live = ZSetBatch::new(schema);
        let families = &self.families;
        let take = |&(fam, row): &(usize, usize)| {
            let b = &families[fam].batch;
            if b.weights[row] > 0 {
                live.copy_row_at(b, row, 1);
            }
        };
        match &spec.bound {
            ReadBound::PkSet(set) => set.iter().filter_map(|k| index.get(k)).for_each(take),
            _ => index.values().for_each(take),
        }
        if live.is_empty() {
            return Ok(out);
        }
        let mut ranges = Vec::new();
        RowFilter::for_read(&spec.predicate, &spec.bound, schema)
            .map_err(|e| ClientError::from(e.to_string()))?
            .ranges(&live, &mut ranges);
        if keys {
            // The predicate has read the payload; the key reply carries none.
            live.payload.clear();
            live.blob.clear();
            live.retain_ranges(&ranges);
            live.nulls.fill(0);
        } else {
            let kept: Vec<(usize, i64)> = ranges
                .iter()
                .flat_map(|&(s, e)| s..e)
                .map(|r| (r, live.weights[r]))
                .collect();
            live = live.gather(&kept);
        }
        out.extend_from_owned(live);
        Ok(out)
    }
}

/// The live rows of `batch`, a read of IDX_TAB, each with its index in `batch`.
fn idx_rows(batch: &ZSetBatch) -> Result<Vec<(usize, IndexRow)>, ClientError> {
    batch
        .live_rows()
        .map(|i| {
            let r = IdxTabRow::read(batch, i).map_err(|e| ProtocolError::DecodeError(format!("index row {i}: {e}")))?;
            let name = r.name.to_owned();
            let cols = PkColList::unpack(r.source_col_idx).map_err(|rule| {
                ProtocolError::DecodeError(format!("index '{name}': {}", rule.for_role(PkListRole::ColumnList)))
            })?;
            let is_unique = gnitz_wire::bool_word(r.is_unique)
                .map_err(|e| ProtocolError::DecodeError(format!("index '{name}': {e}")))?;
            let owner = r.owner_id;
            Ok((i, IndexRow { owner, name, cols, is_unique }))
        })
        .collect()
}

/// Append one `COL_TAB` row per column of `owner_id`, at `+1`. `fks[i]` is
/// column `i`'s target; a column past its end has none.
fn append_col_rows(b: &mut DdlBundle, owner_id: u64, columns: &[ColumnDef], fks: &[Option<FkTarget>]) {
    for (i, cd) in columns.iter().enumerate() {
        let fk = fks.get(i).copied().flatten().map(|t| t.resolve(owner_id));
        b.put(&ColTabRow::of(owner_id, i as u64, cd, fk), 1);
    }
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/client.rs"]
mod bench;
