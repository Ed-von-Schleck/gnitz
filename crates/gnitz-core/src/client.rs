use crate::catalog::{idx_rows, Catalog, IndexRow, CATALOG_FAMILIES};
use crate::connection::{promise, DeltaCursor, Interest, RelDescriptor, Request, ScanReply, Sent, Session, Target};
use crate::error::ClientError;
use crate::protocol::transport::poll_fd;
use crate::{sys_schema, BatchAppender, PkColumn, PushFamily, RelName, Schema, ViewDefinition, ZSetBatch};
use gnitz_wire::{
    payload_bytes, payload_u64, Circuit, CircuitRow, ColTabRow, ColTabSlot, ColumnDef, ComputeMap, FkAction, FkRef,
    IdxTabRow, IdxTabSlot, KeyRange, PkBuf, PkColList, PkKeys, ReadBound, ReadSink, ReadSpec, SchemaTabRow,
    SchemaTabSlot, SysRow, TableProps, TableTabRow, ViewProps, ViewTabRow, WireConflictMode, WireFault, WireStatus,
    CIRCUIT_TAB, COL_TAB, IDX_TAB, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, SCHEMA_TAB, TABLE_TAB, VIEW_TAB,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::{poll_fn, Future};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};
use std::time::Duration;

use gnitz_expr::{LogicalProgram, RowFilter};
use gnitz_wire::txn_frame::BLIND;

/// The absence a catalog lookup by name reports.
pub fn not_found(noun: &'static str, name: impl std::fmt::Display) -> ClientError {
    absent(format!("{noun} '{name}' not found"))
}

/// A `WireStatus::NotFound` refusal raised on this side, worded by the caller.
fn absent(text: String) -> ClientError {
    ClientError::Refused(WireFault { status: WireStatus::NotFound, text })
}

/// The index in `schemas`, SCHEMA_TAB's rows, of the live row named
/// `schema_name` (already canonical).
fn schema_row(schemas: &ZSetBatch, schema_name: &str) -> Option<usize> {
    schemas
        .live_rows()
        .find(|&i| payload_bytes(schemas, i, SchemaTabSlot::name as usize) == schema_name.as_bytes())
}

/// The keys-only read of `schema`: a reply of its PK columns alone, and a rows sink
/// whose zero-instruction map fills no payload slot.
pub fn key_reply(schema: &Schema) -> (Arc<Schema>, ReadSink) {
    let pk: Vec<u32> = (0..schema.pk_cols().len() as u32).collect();
    let reply = Schema::from_parts(schema.hidden_key_columns().collect(), &pk)
        .expect("a schema's own PK columns are an admissible schema");
    let program = LogicalProgram::copy_cols(&[]).to_blob_bytes();
    let map = ComputeMap { program, out_cols: Vec::new() };
    (Arc::new(reply), ReadSink { map: Some(map), ..ReadSink::all_rows() })
}

/// Autocommit attempts [`GnitzClient::read_modify_write`] makes before surfacing
/// the conflict for the caller's own retry. Each attempt re-reads; there is no
/// backoff.
pub const RMW_MAX_ATTEMPTS: usize = 4;

/// One inline `UNIQUE` constraint of a `CREATE TABLE`. `name` is the catalog
/// index name `DROP INDEX` matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineUniqueIndex {
    pub cols: PkColList,
    pub name: String,
}

/// A `CREATE TABLE`: the table, its FOREIGN KEY columns and its inline UNIQUE
/// indexes, which [`GnitzClient::create_table`] writes as one DDL bundle.
pub struct TableDef {
    pub name: RelName,
    pub schema: Schema,
    /// `fks[i]` is column `i`'s target; an empty `fks` gives no column one.
    pub fks: Vec<Option<FkTarget>>,
    pub props: TableProps,
    /// Each inline UNIQUE constraint's columns and the catalog name its index
    /// takes.
    pub unique_indexes: Vec<InlineUniqueIndex>,
}

impl TableDef {
    /// `name` over `schema`, with no foreign key, unique index or option.
    pub fn new(name: RelName, schema: Schema) -> Self {
        TableDef {
            name,
            schema,
            fks: Vec::new(),
            props: TableProps::default(),
            unique_indexes: Vec::new(),
        }
    }
}

/// The column a FOREIGN KEY column references. `SelfTable` names the table
/// being created, which has no id yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FkTarget {
    Table(FkRef),
    SelfTable { col: u32, on_delete: FkAction },
}

/// The ids one SERIAL reservation takes when the statement asks for fewer. Each
/// reservation is a round trip to the master.
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

    /// `scanned`'s row `i`, a stored `family` row, at `-1`.
    fn retract(&mut self, family: u64, scanned: &ZSetBatch, i: usize) {
        self.batch(family).copy_row_at(scanned, i, -1);
    }

    /// `row` at `weight`, in its own family's batch.
    fn put<R: SysRow>(&mut self, row: &R, weight: i64) {
        row.write(&mut BatchAppender::new(self.batch(R::FAMILY)), weight);
    }

    /// The relations whose descriptor the bundle changes: every row names its
    /// relation in its leading key column, an index row in `owner_id`.
    fn relations_written(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();
        for (family, rows) in &self.0 {
            for i in 0..rows.len() {
                ids.insert(match *family {
                    IDX_TAB => payload_u64(rows, i, IdxTabSlot::owner_id as usize),
                    _ => gnitz_wire::sys_leading_id(rows.pks.get_bytes(i)),
                });
            }
        }
        ids
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

/// The symbolic id by which a [`ViewBundle`] circuit's `ScanDelta` names
/// `segments[j]`. No durable relation id reaches
/// [`gnitz_wire::CATALOG_ID_CEILING`], so it is no relation's own.
pub fn segment_id(j: u64) -> u64 {
    gnitz_wire::CATALOG_ID_CEILING + j
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
    /// The user-named view's name and options.
    pub name: RelName,
    pub props: ViewProps,
    /// The id of the view the chain supersedes in the same zone.
    pub replacing: Option<u64>,
    pub segments: Vec<PlannedView>,
    pub view: PlannedView,
    /// What the user-named view's row records of its body.
    pub definition: ViewDefinition,
}

impl ViewBundle {
    /// The chain's catalog rows at `+1`, its first member standing at `base`:
    /// each segment named `_seg{vid}` and owned by the user-named view,
    /// which stands last under its own name and props, recording `definition`.
    fn put_rows(&self, b: &mut DdlBundle, definition: &[u8], schema_id: u64, base: u64) {
        let owner_vid = base + self.segments.len() as u64;
        for (pv, vid) in self.segments.iter().chain([&self.view]).zip(base..) {
            let mut circuit = pv.circuit.clone();
            // A `segment_id` past the chain's end becomes an id no lower than the
            // user-named view's own, which the engine refuses.
            for src in circuit.sources_mut() {
                if let Some(j) = src.checked_sub(gnitz_wire::CATALOG_ID_CEILING) {
                    *src = base + j;
                }
            }
            // A segment's name is unique because vids are, and no SQL surface can
            // spell it because gnitz_wire::validate_user_identifier rejects a leading
            // `_`; the owner_view_id column records its ownership.
            let (name, owner_view_id, props, definition) = if vid == owner_vid {
                (self.name.name().to_string(), 0, self.props, definition)
            } else {
                (format!("_seg{vid}"), owner_vid, ViewProps::default(), &[][..])
            };

            // A foreign key constrains a base table, not a view.
            append_col_rows(b, vid, pv.schema.columns(), &[]);
            let circuit = circuit.encode();
            b.put(
                &CircuitRow {
                    view_id: vid,
                    circuit: circuit.as_slice().into(),
                },
                1,
            );
            let (capacity_bytes, delta_bytes) = props.budget.row_words();
            // The VIEW_TAB register hook triggers server-side compilation.
            b.put(
                &ViewTabRow {
                    view_id: vid,
                    schema_id,
                    name: name.as_str().into(),
                    pk_col_idx: PkColList::from_slice(pv.schema.pk_cols()),
                    capacity_bytes,
                    delta_bytes,
                    owner_view_id,
                    pk_repeats: pv.pk_repeats,
                    compress: props.compress,
                    definition: definition.into(),
                },
                1,
            );
        }
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

    /// Once the socket is ready for any of `want`, step the client on what is
    /// ready by calling `io`. It answers whether bytes are still waiting to be
    /// written: after a write, ones the socket refused. Readiness is spent by
    /// the call: before it asks again, the client reads on by itself until the
    /// socket has no more, and writes by itself until the socket refuses bytes.
    fn poll_io(
        &mut self,
        want: Interest,
        cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> bool,
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
        Ok(())
    }

    fn poll_io(
        &mut self,
        want: Interest,
        _cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> bool,
    ) -> Poll<Result<(), ClientError>> {
        let fd = self.fd.expect("a client attaches its host before it waits");
        loop {
            match poll_fd(fd, want.poll_events(), None) {
                Ok(revents) => {
                    io(Interest::from_revents(revents));
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
pub(crate) fn poll_turn(
    session: &mut Session,
    host: &mut dyn Host,
    cx: &mut Context<'_>,
) -> Poll<Result<(), ClientError>> {
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
    // Asking the host first would cost a wait per request.
    if want.write && !session.refused() {
        session.step(Interest::WRITE);
        return Poll::Ready(Ok(()));
    }
    let waited = ready!(host.poll_io(want, cx, &mut |ready| {
        session.step(ready);
        session.interest().write
    }));
    Poll::Ready(match waited {
        // A wait that failed is the connection's end, which every reply owed
        // arrives as; an interrupt leaves the session as it stood.
        Err(e) if !matches!(e, ClientError::Interrupted(_)) => {
            session.end(e);
            Ok(())
        }
        waited => waited,
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

impl<T> Pending<'_, T> {
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
            match poll_turn(session, &mut **host, cx) {
                Poll::Ready(Ok(())) => {}
                // Nothing outstanding and nothing buffered.
                Poll::Ready(Err(ClientError::Closed)) => {
                    return match more {
                        true => Poll::Pending,
                        false => Poll::Ready(None),
                    }
                }
                // No call is waiting to hand an interrupt to.
                Poll::Ready(Err(e)) => session.end(e),
                Poll::Pending => return Poll::Pending,
            }
        })
        .await;
        let Some(op) = op else { return };
        // A connection that is gone refuses the op's own requests, as what
        // ended it.
        if let Err(e) = client.make_room().await {
            client.session.end(e);
        }
        op(&mut client).await;
    }
}

pub struct GnitzClient {
    /// Before the session, so it lets go of the socket before that closes.
    pub(crate) host: Box<dyn Host>,
    pub(crate) session: Session,
    /// Per table, the SERIAL ids a reservation drew and no INSERT has taken. A
    /// disconnect discards them (an intentional, PostgreSQL-style gap): a server
    /// that crashed may hand the ids no committed row took to another client.
    serial_cache: HashMap<u64, std::ops::Range<u64>>,
    /// Open transaction, if any; `None` is autocommit. Every user-table write
    /// buffers here while it is open, and dropping it is ROLLBACK.
    txn: Option<TxnBuffer>,
    /// The local copy this client reads through, if a host attached one. Boxed,
    /// so a client that never mirrors pays one `None` and no allocation.
    pub(crate) mirror: Option<Box<crate::mirror::MirrorState>>,
    /// A reader's subscriptions, each with the layout its trains decode
    /// under, in the order they were made. A mirror keeps its own with the
    /// views they feed.
    pub(crate) readers: std::collections::BTreeMap<u64, Arc<Schema>>,
    /// Qualified name → the descriptor its last RESOLVE answered, for
    /// [`Self::kept_desc`]. This client's own DDL drops the relations it wrote.
    kept: HashMap<RelName, Arc<RelDescriptor>>,
    /// The next subscription id this client asks for. One id names one
    /// subscription for the client's whole life: a reader still holding an id
    /// of a connection since replaced is told that one ended, never handed
    /// another's trains.
    pub(crate) next_sub: u64,
}

// `gnitz-py` runs these with the GIL released and `gnitz-tokio` spawns them,
// and both need `Send`. Asserted here, a breach names the field that caused it.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn assert_send_value<T: Send>(_: T) {}
    assert_send::<GnitzClient>();
    assert_send::<Sent<ScanReply>>();
    let _ = |mut c: GnitzClient, schema: &Arc<Schema>, batch: &ZSetBatch| {
        assert_send_value(c.sync(Duration::ZERO));
        assert_send_value(c.create_schema("", false));
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
            readers: Default::default(),
            kept: HashMap::new(),
            next_sub: 1,
        })
    }

    /// Requests this connection has submitted. Exposed for the
    /// round-trip-count assertions; see [`Session::requests_sent`].
    pub fn requests_sent(&self) -> u64 {
        self.session.requests_sent()
    }

    /// Whether the connection has ended, so no request on it can be answered.
    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    // ── Waiting ────────────────────────────────────────────────────────────

    pub(crate) fn ack(&mut self, req: Request<'_>) -> Pending<'_, u64> {
        let sent = self.session.submit(req);
        self.wait(sent)
    }

    /// Step this client's session until `sent` is answered. Replies arrive in
    /// request order, so every reply detached before it is answered by then;
    /// a sync's answer is no reply, and arrives in the order of syncs alone.
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
    /// so an INSERT that knows its row count pays one round trip rather than
    /// `ceil(count / SERIAL_RANGE_SIZE)`. An abandoned tail — the old
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

    /// Reserve exactly `count` SERIAL ids of `table` and return the first,
    /// leaving this client's own reservation untouched: what puts a restored
    /// table's sequence where its dump left it.
    pub async fn reserve_serial_exact(&mut self, table: &RelDescriptor, count: u64) -> Result<u64, ClientError> {
        self.ack(Request::AllocSerial { table: table.into(), count }).await
    }

    // --- Raw ops ---

    /// Allocate one catalog object id (schema, relation or index).
    pub async fn alloc_id(&mut self) -> Result<u64, ClientError> {
        self.ack(Request::AllocIds(1)).await
    }

    /// Push `batch` into `target` under `mode`. SQL `INSERT` uses `Error` to get
    /// SQL-standard rejection semantics; every other caller passes `Update`.
    ///
    /// Inside an open transaction the batch is buffered instead of sent, and the
    /// returned LSN is `0` — nothing is durable until `txn_commit`, which returns
    /// the one zone LSN covering the whole bundle. A batch passed by value moves
    /// into the buffer; a borrowed one is copied into it once.
    pub fn push<'a>(
        &mut self,
        target: impl Into<Target>,
        schema: &Arc<Schema>,
        batch: impl Into<Cow<'a, ZSetBatch>>,
        mode: WireConflictMode,
    ) -> Pending<'_, u64> {
        let (target, batch) = (target.into(), batch.into());
        if let Some(txn) = &mut self.txn {
            let buffered = txn.push(target, schema, batch, mode, BLIND);
            return self.wait(Sent::ready(buffered.map(|()| 0)));
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
        self.wait(sent)
    }

    // ── Reads, by the freshness asked for ──────────────────────────────────
    //
    // `resolve` and `scan_spec` ask the server; `mirrored_desc` and
    // `scan_spec_local_first` below consult the copy first, and `kept_desc`
    // the last RESOLVE's answer. **The gate is what the copy holds, never
    // whether a store is attached**, so a client with one reads exactly like
    // a client without for every relation the copy does not hold.

    /// The descriptor of the mirrored view `name`, while its copy answers
    /// reads. As stale as the copy.
    pub fn mirrored_desc(&self, name: &RelName) -> Option<Arc<RelDescriptor>> {
        self.mirror.as_deref()?.answering(name)
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
        if let Some(m) = self.mirror.as_deref() {
            // One guard covers the gate and the scan.
            let mut store = m.store.get();
            if m.views.get(&target.tid).and_then(|v| v.answers_at(&*store)).is_some() {
                let reply = store.scan_spec(target.tid, spec, reply_schema).map(|batch| ScanReply {
                    batch,
                    schema: Arc::clone(reply_schema),
                    lsn: None,
                });
                drop(store);
                return self.wait(Sent::ready(reply.map_err(ClientError::from)));
            }
        }
        self.scan_spec(target, &spec, reply_schema)
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
            readers: _,
            kept,
            next_sub: _,
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

    /// Read a view's delta feed under `spec`, with the cursor to poll from
    /// next. The reply comes back in `reply_schema`, weights and all.
    ///
    /// With no `from` it bootstraps: the view's whole current value at its true
    /// net weights, which replaces a copy's state rather than adding to it.
    /// After `from` it is every delta the view emitted since: apply it and store
    /// the new cursor; there is nothing to reconcile.
    ///
    /// `spec` is an encoded `ReadSpec` forwarding rows with no cut: the reply is
    /// that spec applied to the view, in `reply_schema`, which is the view's own
    /// under `ReadSpec::all_rows` of no bound. Every later poll of the copy
    /// carries the same `spec`.
    ///
    /// A cursor the feed does not continue — its rounds dropped, or handed out
    /// by another boot, relation or `spec` — is refused as `DeltaExpired`:
    /// discard the copy and bootstrap again.
    ///
    /// It carries every push acknowledged before it, and is answered at once:
    /// only a sync waits for a view to change.
    pub fn delta_read(
        &mut self,
        view: impl Into<Target>,
        from: Option<DeltaCursor>,
        reply_schema: &Arc<Schema>,
        spec: &[u8],
    ) -> Pending<'_, (ScanReply, DeltaCursor)> {
        let sent = self
            .session
            .submit_delta_read(view.into(), from, reply_schema, spec, None);
        self.wait(sent)
    }

    /// N [`Self::scan_spec`]s as one request, answered in request order: no
    /// commit is in some of the replies and not in others.
    pub fn scan_many(&mut self, scans: &[(Target, &ReadSpec, &Arc<Schema>)]) -> Pending<'_, Vec<ScanReply>> {
        let sent = self.session.submit_scan_multi(scans);
        self.wait(sent)
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
                source_col_idx: cols,
                name: index_name.as_str().into(),
                is_unique,
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
        let mut retracted = Vec::new();
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            match rows.iter().find(|(_, r)| r.name == name && matches(r)) {
                Some(&(i, _)) if retracted.contains(&i) => {}
                Some(&(i, _)) => {
                    retracted.push(i);
                    b.retract(IDX_TAB, &scanned, i);
                }
                None if if_exists => {}
                None => return Err(not_found(noun, name)),
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

    /// The whole catalog, read at one cut.
    pub async fn catalog(&mut self) -> Result<Catalog, ClientError> {
        let all = ReadSpec::all_rows(ReadBound::None);
        let families = self.sys_cut(CATALOG_FAMILIES, &all).await?;
        Ok(Catalog::decode(&families)?)
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

    /// Commit `bundle` as one DDL zone; refused inside a transaction. One that
    /// holds no row opens no zone.
    async fn commit_ddl(&mut self, bundle: DdlBundle) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::from("DDL is not allowed inside a transaction".to_string()));
        }
        if bundle.0.is_empty() {
            return Ok(());
        }
        self.ack(Request::DdlTxn(&bundle.0)).await?;
        let written = bundle.relations_written();
        self.kept.retain(|_, rel| !written.contains(&rel.tid));
        self.ddl_committed(&bundle.0).await;
        Ok(())
    }

    /// `CREATE SCHEMA [IF NOT EXISTS]`. With `if_not_exists`, an existing
    /// schema's id is returned, and the id fetched alongside the probe goes
    /// unused: a gap in the id sequence and nothing more.
    pub async fn create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<u64, ClientError> {
        // Refuses the empty string, a leading `_` (the reserved system prefix)
        // and illegal characters.
        let name = gnitz_wire::canonical_identifier(name)?;
        let schema_id = if if_not_exists {
            let all = ReadSpec::all_rows(ReadBound::None);
            let schemas = self
                .session
                .submit_scan(SCHEMA_TAB.into(), &all, sys_schema(SCHEMA_TAB));
            let ids = self.session.submit(Request::AllocIds(1));
            let schemas = self.wait(schemas).await?.batch;
            if let Some(at) = schema_row(&schemas, &name) {
                return Ok(schemas.pks.get(at) as u64);
            }
            self.wait(ids).await?
        } else {
            self.alloc_id().await?
        };
        let mut b = DdlBundle::default();
        b.put(&SchemaTabRow { schema_id, name: name.as_str().into() }, 1);
        self.commit_ddl(b).await?;
        Ok(schema_id)
    }

    /// `DROP SCHEMA … CASCADE`: the schema and every table and view in it, as
    /// one DDL bundle. A relation outside the schema that depends on one inside
    /// fails the bundle, and nothing is dropped.
    pub async fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
        let name = gnitz_wire::canonical_identifier(name)?;
        let all = ReadSpec::all_rows(ReadBound::None);
        let [schemas, views, tables] = self.sys_cut([SCHEMA_TAB, VIEW_TAB, TABLE_TAB], &all).await?;
        let at = schema_row(&schemas, &name).ok_or_else(|| not_found("schema", &name))?;
        let schema_id = schemas.pks.get(at) as u64;

        let mut b = DdlBundle::default();
        for (family, scanned) in [(VIEW_TAB, views), (TABLE_TAB, tables)] {
            for i in scanned.live_rows() {
                if payload_u64(&scanned, i, RELTAB_PAY_SCHEMA_ID) == schema_id {
                    b.retract(family, &scanned, i);
                }
            }
        }
        b.retract(SCHEMA_TAB, &schemas, at);
        self.commit_ddl(b).await
    }

    /// Register `def` as one DDL bundle and return the table's id.
    pub async fn create_table(&mut self, def: &TableDef) -> Result<u64, ClientError> {
        let TableDef {
            name: table,
            schema,
            fks,
            props,
            unique_indexes,
        } = def;
        let index_names: Vec<String> = unique_indexes
            .iter()
            .map(|spec| gnitz_wire::canonical_identifier(&spec.name))
            .collect::<Result<Vec<_>, String>>()?;
        let pk = PkColList::from_slice(schema.pk_cols());
        if !fks.is_empty() && fks.len() != schema.columns().len() {
            return Err(ClientError::from(format!(
                "create_table: {} foreign-key slots for {} columns",
                fks.len(),
                schema.columns().len()
            )));
        }

        // The table's id, then one per inline UNIQUE index.
        let (schema_id, new_tid) = self
            .schema_and_ids(table.schema(), 1 + unique_indexes.len() as u64)
            .await?;

        let mut b = DdlBundle::default();
        append_col_rows(&mut b, new_tid, schema.columns(), fks);
        b.put(
            &TableTabRow {
                table_id: new_tid,
                schema_id,
                name: table.name().into(),
                pk_col_idx: pk,
                flags: props.pack(),
            },
            1,
        );
        for (k, spec) in unique_indexes.iter().enumerate() {
            b.put(
                &IdxTabRow {
                    index_id: new_tid + 1 + k as u64,
                    owner_id: new_tid,
                    source_col_idx: spec.cols,
                    name: index_names[k].as_str().into(),
                    is_unique: true,
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

    /// Create `bundle` in one atomic `DDL_TXN` and return its id. A view it
    /// replaces that already is `bundle` stays under its id, and nothing is
    /// written.
    pub async fn create_view_chain(&mut self, bundle: ViewBundle) -> Result<u64, ClientError> {
        let view = &bundle.name;
        let n_views = bundle.segments.len() + 1;
        let n_segments = bundle.segments.len() as u64;

        let mut b = DdlBundle::default();
        let (schema_id, base) = match bundle.replacing {
            Some(vid) => {
                // Where `bundle` stands if it is the chain under `vid`: one
                // allocation holds a chain, the user-named view last.
                let base = vid.saturating_sub(n_segments);
                let members = ReadSpec::all_rows(ReadBound::Range(KeyRange::new(
                    PkColList::from_slice(&[0]),
                    &[],
                    base as u128,
                    vid as u128,
                )));
                let families = [VIEW_TAB, COL_TAB, CIRCUIT_TAB];
                let rows = self.sys_cut(families, &members).await?;
                let standing = DdlBundle(families.into_iter().zip(rows).collect());
                let views = &standing.0[0].1;
                let i = views
                    .live_rows()
                    .find(|&i| views.pks.get(i) as u64 == vid)
                    .ok_or_else(|| not_found("view", view))?;
                let schema_id = payload_u64(views, i, RELTAB_PAY_SCHEMA_ID);

                // The definition says how the body was spelled, which is no
                // part of what the view is: another spelling of the same body
                // is the same view, and keeps the standing definition.
                let standing_definition = ViewTabRow::read(views, i)
                    .map_err(|e| ClientError::from(format!("view {vid}: {e}")))?
                    .definition;
                let mut want = DdlBundle::default();
                bundle.put_rows(&mut want, &standing_definition, schema_id, base);
                if want.same_rows(&standing) {
                    return Ok(vid);
                }
                // Only the user-named view, ahead of the new chain's `+1`s: the
                // engine cascades its segments.
                b.retract(VIEW_TAB, views, i);
                (schema_id, self.ack(Request::AllocIds(n_views as u64)).await?)
            }
            None => self.schema_and_ids(view.schema(), n_views as u64).await?,
        };

        let definition = bundle.definition.encode();
        bundle.put_rows(&mut b, &definition, schema_id, base);
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
        for (k, name) in names.iter().enumerate() {
            // RelName compares by its folded `schema.name`, so a repeat spelled
            // differently is still a repeat.
            if names[..k].contains(name) {
                continue;
            }
            match self.relation_retraction(family, noun, name).await? {
                Some((scanned, i)) => b.retract(family, &scanned, i),
                None if if_exists => {}
                None => return Err(not_found(noun, name)),
            }
        }
        self.commit_ddl(b).await
    }

    /// The live `family` row of `name`, by one master-local seek. `Ok(None)` is the
    /// *resolve* miss alone; a name held by the other kind — `DROP TABLE <view>` —
    /// is refused as such, never as a `not_found` a caller may read as "already gone".
    async fn relation_retraction(
        &mut self,
        family: u64,
        noun: &'static str,
        name: &RelName,
    ) -> Result<Option<(ZSetBatch, usize)>, ClientError> {
        let Some(desc) = self.resolve(name).await? else {
            return Ok(None);
        };
        if desc.class.is_view() != (family == VIEW_TAB) {
            return Err(format!(
                "'{name}' is a {}; DROP {} requires a {noun}",
                desc.class.describe(),
                noun.to_uppercase()
            )
            .into());
        }
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
        let (tid, col_idx) = (rel.tid, rel.schema.layout().num_columns());

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
        b.retract(family, &scanned, i);
        let rows = b.batch(family);
        rows.copy_row_at(&scanned, i, 1);
        patch(rows, 1);
        self.commit_ddl(b).await
    }

    // --- Relation resolution ---

    /// The descriptor for `name` as the server answers it now, or `None` when
    /// no such relation exists; `Err` is a missing schema or a decode error.
    pub async fn resolve(&mut self, name: &RelName) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let sent = self.session.submit_resolve(name.key());
        let found = self.wait(sent).await?;
        match &found {
            Some(desc) => self.kept.insert(name.clone(), Arc::clone(desc)),
            None => self.kept.remove(name),
        };
        Ok(found)
    }

    /// [`Self::resolve`], with a missing relation an error.
    pub async fn resolve_relation(&mut self, name: &RelName) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(name).await?.ok_or_else(|| not_found("relation", name))
    }

    // --- Catalog lookups ---

    /// The id of the live schema `schema_name` (already canonical) and the first
    /// of `n` fresh catalog ids, asked for in one round trip. The ids a missing
    /// schema leaves untaken are a gap in the id sequence and nothing more.
    async fn schema_and_ids(&mut self, schema_name: &str, n: u64) -> Result<(u64, u64), ClientError> {
        let all = ReadSpec::all_rows(ReadBound::None);
        let schemas = self
            .session
            .submit_scan(SCHEMA_TAB.into(), &all, sys_schema(SCHEMA_TAB));
        let ids = self.session.submit(Request::AllocIds(n));
        let schemas = self.wait(schemas).await?.batch;
        let at = schema_row(&schemas, schema_name).ok_or_else(|| not_found("schema", schema_name))?;
        Ok((schemas.pks.get(at) as u64, self.wait(ids).await?))
    }

    /// `families`' rows under `spec`, with no DDL between them.
    async fn sys_cut<const N: usize>(
        &mut self,
        families: [u64; N],
        spec: &ReadSpec,
    ) -> Result<[ZSetBatch; N], ClientError> {
        let scans = families.map(|family| (Target::from(family), spec, sys_schema(family)));
        let mut replies = self.scan_many(&scans).await?.into_iter();
        Ok(std::array::from_fn(|_| {
            replies.next().expect("one reply per scan").batch
        }))
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
    fn push<'b>(
        &mut self,
        target: impl Into<Target>,
        schema: &Arc<Schema>,
        batch: impl Into<Cow<'b, ZSetBatch>>,
        mode: WireConflictMode,
        basis: u64,
    ) -> Result<(), ClientError> {
        let (target, batch) = (target.into(), batch.into());
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
                f.batch.extend_from(&batch);
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
                    batch: batch.into_owned(),
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
        if !set.iter().any(|k| index.contains_key(k)) {
            return None;
        }
        let mut rest = set.clone();
        rest.retain(|k| !index.contains_key(k));
        Some(rest)
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
        let keep: Vec<u32> = (0..committed.len())
            .filter(|&i| !index.contains_key(committed.pks.get_bytes(i)))
            .map(|i| i as u32)
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
        RowFilter::for_read(&spec.predicate, spec.bound.range(), schema.layout())
            .map_err(|e| ClientError::from(e.to_string()))?
            .ranges(&live, &mut ranges);
        if keys {
            // The predicate has read the payload; the key reply carries none.
            live.payload.clear();
            live.blob.clear();
            live.retain_ranges(&ranges);
            live.nulls.fill(0);
        } else {
            let kept: Vec<u32> = ranges.iter().flat_map(|&(s, e)| s as u32..e as u32).collect();
            live = live.gather(&kept);
        }
        out.extend_from_owned(live);
        Ok(out)
    }
}

/// Append one `COL_TAB` row per column of `owner_id`, at `+1`. `fks[i]` is
/// column `i`'s target; a column past its end has none.
fn append_col_rows(b: &mut DdlBundle, owner_id: u64, columns: &[ColumnDef], fks: &[Option<FkTarget>]) {
    for (i, cd) in columns.iter().enumerate() {
        let fk = fks.get(i).copied().flatten().map(|t| match t {
            FkTarget::Table(fk) => fk,
            FkTarget::SelfTable { col, on_delete } => FkRef { table_id: owner_id, col, on_delete },
        });
        b.put(&ColTabRow::of(owner_id, i as u64, cd, fk), 1);
    }
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/client.rs"]
mod bench;
