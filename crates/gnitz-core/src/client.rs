use crate::connection::{
    IdRun, Interest, MultiScanResult, PollSink, PolledView, RawBlock, RelDescriptor, RelTarget, Reply, Request,
    ScanReply, ScanResult, Session, SlotId,
};
use crate::error::ClientError;
use crate::protocol::transport::poll_fd;
use crate::protocol::{
    BatchAppender, ColumnDef, PkBuf, PkColumn, ProtocolError, ReplySchema, Schema, TypeCode, WireConflictMode,
    ZSetBatch,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::mirror::{MirrorState, MirrorStore, MirroredView};
use crate::types::sys_schema;
use gnitz_expr::SchemaFacts;
use gnitz_wire::sys_rows::{IdxTabRow, TableTabRow, ViewTabRow};
use gnitz_wire::txn_frame::DeltaPollItem;
use gnitz_wire::Circuit;
use gnitz_wire::{
    RelClass, TableProps, ViewProps, CIRCUIT_NODES_TAB, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME,
    COL_TAB, IDXTAB_COL_FLAGS, IDXTAB_COL_NAME, IDXTAB_COL_OWNER_ID, IDXTAB_COL_SOURCE_COLS, IDX_TAB, OWNER_KIND_TABLE,
    OWNER_KIND_VIEW, RELTAB_COL_NAME, RELTAB_COL_SCHEMA_ID, SCHEMATAB_COL_NAME, SCHEMA_TAB, TABLE_TAB, VIEW_TAB,
};

// --- Module-private helpers ---

fn col_u64(batch: &ZSetBatch, schema: &Schema, ci: usize, i: usize) -> u64 {
    gnitz_wire::read_u64_le(SchemaFacts::locate(schema, ci).bytes(batch, i), 0)
}

/// Row `i` of a system-table STRING column. Every such column is declared
/// non-nullable, so a NULL is a malformed reply, not a case — this is the trust
/// boundary that says so rather than substituting `""`. UTF-8 is validated here
/// too: the region carries bytes.
fn col_str<'a>(batch: &'a ZSetBatch, schema: &Schema, ci: usize, i: usize) -> Result<&'a str, ClientError> {
    let loc = SchemaFacts::locate(schema, ci);
    if loc.is_null(batch, i) {
        return Err(ClientError::ServerError(format!(
            "col_str: NULL in a non-nullable system column at row {i}"
        )));
    }
    std::str::from_utf8(gnitz_wire::german_string_content(loc.bytes(batch, i), &batch.blob))
        .map_err(|e| ClientError::ServerError(format!("col_str: invalid UTF-8 at row {i}: {e}")))
}

/// [`gnitz_wire::qualified_key`] from names that may still be raw user text.
/// The fold lives on this side only — see that function for why.
pub fn qualified_name(schema_name: &str, name: &str) -> String {
    let mut q = gnitz_wire::qualified_key(schema_name, name);
    q.make_ascii_lowercase();
    q
}

/// The classified absence every schema-qualified catalog lookup reports, rather
/// than a spelling of one message per call site.
fn not_found(noun: &'static str, schema_name: &str, name: &str) -> ClientError {
    ClientError::NotFound {
        noun,
        name: qualified_name(schema_name, name),
    }
}

/// Build the `-1` retraction batch for `pks`: the server's `retract_pk` matches
/// by PK alone, so the payload columns are inert filler. Built directly rather
/// than through `BatchAppender`, which has no way to take a whole `PkColumn`.
/// Shared by `GnitzClient::delete` and the SQL layer's DELETE RMW retry closure
/// (which needs the batch without an immediate push).
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

// --- GnitzClient ---

/// A secondary-index descriptor: the declared column list (the unique key — the
/// circuit list is deduped by column list) and the system's operative
/// uniqueness truth. This is the wire type verbatim, so a resolved descriptor's
/// index list moves into the client.
pub use gnitz_wire::RelIndex as IndexMeta;

/// One inline `UNIQUE` constraint to fold into a `CREATE TABLE`'s atomic DDL
/// bundle. `col_indices` are the constrained columns (a 1-element list for a
/// single-column UNIQUE); `name` is the resolved catalog index name that
/// `DROP INDEX` will match. Column types are derived from the table's columns,
/// so a UNIQUE+FK column's parent-rewritten (integer) type is picked up
/// automatically.
#[derive(Clone, Copy, Debug)]
pub struct InlineUniqueIndex<'a> {
    pub col_indices: &'a [u32],
    pub name: &'a str,
}

/// A cached half-open range `[next, end)` of unissued SERIAL ids for one table,
/// drawn from the master by `reserve_serial_ids`. Cache-loss on disconnect
/// discards the unissued tail (an intentional, PostgreSQL-style gap).
struct SerialRange {
    next: u64,
    end: u64,
}

/// Number of SERIAL ids reserved per master round-trip. Each reservation is a
/// `catalog_rwlock`-serialized, fsync'd durable advance on the master, so the
/// range cache amortizes that one fsync across `SERIAL_RANGE_SIZE` inserts.
const SERIAL_RANGE_SIZE: u64 = 64;

/// Upper bound on the segments in one atomic view chain — a bound on the DDL
/// bundle, not on planning: every segment is already built and in RAM by the time
/// `create_view_chain` counts them.
pub const MAX_CHAIN_SEGMENTS: usize = 64;

/// How an internal chain segment is named, from its own allocated view id.
///
/// Unique because vids are, and unspellable at every user surface because
/// [`crate::validate_user_identifier`] rejects a leading `_`. Ownership is the
/// `owner_view_id` column, not the name.
fn segment_name(vid: u64) -> String {
    format!("_seg{vid}")
}

/// A subscriber's whole state: one word, held on the client.
///
/// `tag` names what the cursor is a cursor *into* — a `(boot, relation)`
/// identity. A round number alone identifies nothing: it is meaningless across a
/// restart, because the counter starts over, and meaningless across a
/// `DROP VIEW v; CREATE VIEW v …`, because the recreated `v` takes a fresh id
/// whose rounds are numbered from the same global counter as the old one's — so a
/// stale cursor would read as "nothing changed" and the client would sit on the
/// previous view's rows. A tag the server does not echo back means: discard the
/// copy and bootstrap again.
///
/// The cursor lives here and nowhere else, so there is nothing to lose on a
/// disconnect, nothing to forge, and no liveness for the server to detect.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub struct DeltaCursor {
    /// Identifies the boot and the relation this cursor belongs to.
    pub tag: u64,
    /// The last tick round it covers; the next poll asks for everything after it.
    pub tick: u64,
}

impl DeltaCursor {
    /// The tick a poll from this cursor reads after.
    ///
    /// Tick `0` is **refused**: it names no copy to protect and no tag to match,
    /// so it could only mean a bootstrap — and a bootstrap walks the view's own
    /// store and comes back in the view's schema, not the [`delta_reply_schema`]
    /// shape a poll takes.
    pub(crate) fn poll_after(self) -> Result<u64, ClientError> {
        (self.tick != 0).then_some(self.tick).ok_or(ClientError::DeltaExpired)
    }

    /// `next` as this cursor's successor, or [`ClientError::DeltaExpired`].
    ///
    /// A tag the server did not echo back names a different boot or a different
    /// relation, and the rows such a read draws are unsafe to apply: they are the
    /// *other* relation's recent deltas, and a recreated view's backfill never
    /// enters a delta store at all. The recovery is the one a cursor that fell out
    /// of the retention window gets — discard the copy and bootstrap.
    pub(crate) fn advanced_to(self, next: DeltaCursor) -> Result<DeltaCursor, ClientError> {
        (self.tag == next.tag).then_some(next).ok_or(ClientError::DeltaExpired)
    }
}

/// The reply schema of an incremental delta read: a `_tick` U64 key column, then
/// `view`'s PK columns in PK order, then its payload columns in schema order.
///
/// The order is `gnitz_wire::delta_schema_order`, which the engine's
/// `make_delta_schema` applies to build the delta store's own descriptor — the
/// same permutation, not a second statement of it. The worker's identity-rows
/// path demands the reply schema match that layout exactly.
///
/// A bootstrap read is **not** in this shape — it walks the view's own store and
/// comes back in the view's own schema.
pub fn delta_reply_schema(view: &Schema) -> Result<Schema, ClientError> {
    // The one limit that can bind: the stamp is one column more than the view,
    // which is what the engine's `make_delta_schema` answers `None` for. Not
    // `Schema::from_parts` — its PK-arity cap is the *persisted* PK-list codec's,
    // and a stamped key is one column past it by construction.
    if view.num_columns() >= gnitz_wire::MAX_COLUMNS {
        return Err(ClientError::ServerError(format!(
            "a view with {} columns cannot carry a delta feed: the `_tick` stamp would exceed the \
             {}-column limit",
            view.num_columns(),
            gnitz_wire::MAX_COLUMNS
        )));
    }
    let mut columns = Vec::with_capacity(view.num_columns() + 1);
    let mut pk_cols = Vec::with_capacity(view.pk_count() + 1);
    for c in gnitz_wire::delta_schema_order(&view.pk_cols, view.num_columns()) {
        let (cd, is_key) = match c {
            gnitz_wire::DeltaCol::Tick => (ColumnDef::new("_tick", TypeCode::U64, false).hidden(), true),
            gnitz_wire::DeltaCol::Key(i) => (view.columns[i].clone(), true),
            gnitz_wire::DeltaCol::Payload(i) => (view.columns[i].clone(), false),
        };
        if is_key {
            pk_cols.push(columns.len() as u32);
        }
        columns.push(cd);
    }
    Ok(Schema { columns, pk_cols })
}

/// The symbolic id naming element `j` of a view bundle from a later element's
/// `ScanDelta`. Symbolic ids start at [`gnitz_wire::CATALOG_ID_CEILING`], which
/// no durable relation id reaches, so `create_view_chain` tells them apart from
/// real relation ids and substitutes the id it allocated — a bundle reaches no
/// server while it is built.
pub fn segment_id(j: u64) -> u64 {
    gnitz_wire::CATALOG_ID_CEILING + j
}

/// One view in a [`GnitzClient::create_view_chain`] bundle.
pub struct PlannedView {
    pub circuit: Circuit,
    pub output_columns: Vec<ColumnDef>,
    pub pk_cols: Vec<u32>,
}

/// What one statement has already read, dropped whole at `end_statement`.
///
/// One entry per resolved canonical `"schema.name"`, absent verdicts included,
/// so a two-probe error ladder costs one round trip rather than two.
/// [`Self::get`] and [`Self::insert`] build the key through [`qualified_name`]
/// themselves, so a case-varying reference cannot split into two entries.
///
/// `BTreeMap` rather than `HashMap` because `BTreeMap::new()` is `const`, which
/// [`EMPTY_CATALOG`] needs to be a `static`.
#[derive(Default)]
pub struct CatalogSnapshot {
    relations: BTreeMap<String, Option<Arc<RelDescriptor>>>,
}

/// What a planning pass sees outside a statement bracket: nothing, because every
/// read issued between statements hits the wire.
static EMPTY_CATALOG: CatalogSnapshot = CatalogSnapshot { relations: BTreeMap::new() };

impl CatalogSnapshot {
    /// This statement's verdict for `schema_name.name`: `None` if the statement
    /// has not resolved that name at all, `Some(None)` for a recorded absence.
    pub fn get(&self, schema_name: &str, name: &str) -> Option<Option<Arc<RelDescriptor>>> {
        self.get_qname(&qualified_name(schema_name, name))
    }

    /// [`Self::get`] for a caller that already built the key.
    pub(crate) fn get_qname(&self, qname: &str) -> Option<Option<Arc<RelDescriptor>>> {
        self.relations.get(qname).cloned()
    }

    /// Record a verdict, replacing any entry already under the same key.
    pub fn insert(&mut self, schema_name: &str, name: &str, desc: Option<Arc<RelDescriptor>>) {
        self.insert_qname(qualified_name(schema_name, name), desc);
    }

    /// [`Self::insert`] for a caller that already built the key.
    pub(crate) fn insert_qname(&mut self, qname: String, desc: Option<Arc<RelDescriptor>>) {
        self.relations.insert(qname, desc);
    }

    /// The descriptor this statement resolved for `tid`. A linear scan with no
    /// wire fallback; a statement resolves a handful of relations.
    pub(crate) fn by_tid(&self, tid: u64) -> Option<Arc<RelDescriptor>> {
        self.relations.values().flatten().find(|d| d.tid == tid).map(Arc::clone)
    }
}

/// Run when a signal interrupts a blocking call's wait; an `Err` aborts the call.
/// The Python binding checks for Ctrl-C here.
pub type ParkHook = Box<dyn FnMut() -> Result<(), ClientError> + Send>;

pub struct GnitzClient {
    pub(crate) session: Session,
    pub(crate) park_hook: Option<ParkHook>,
    serial_cache: HashMap<u64, SerialRange>,
    /// What the current statement has resolved and scanned; `None` outside a
    /// statement, so a read issued between statements always hits the wire.
    scope: Option<CatalogSnapshot>,
    /// Open transaction, if any. `Some` between `txn_begin` and its
    /// `txn_commit`/`txn_rollback`: **every** user-table write on this client
    /// (`push`, `push_with_mode`, `delete` — and so every SQL DML statement, C
    /// and Python binary push alike) buffers here instead of going to the wire,
    /// and the SQL overlay consults it for read-your-own-writes. `None` is
    /// autocommit. The buffer keys families by resolved tid, so a `schema_name`
    /// change between `execute_sql` calls cannot corrupt it. Dropping the client
    /// with an open transaction discards `txn` by plain `Drop` — identical to
    /// ROLLBACK, nothing was ever sent.
    ///
    /// Catalog writes never route here: DDL has its own atomic commit and is
    /// rejected inside a transaction at the client's `push_ddl_txn` choke point (and,
    /// earlier and friendlier, by the SQL front end).
    txn: Option<TxnBuffer>,
    /// The client's OCC basis: the running maximum over server-issued watermarks
    /// the connection has observed — seeded from the HELLO ACK at connect, then
    /// advanced by every push / `txn_commit` ACK and every `WireStatus::TxnConflict`
    /// fresh basis. Always `≤ published()` at receipt, so it is a sound
    /// (conservative) basis: underusing it costs at most a self-healing retry,
    /// never a false pass. Autocommit RMW statements read it as their basis;
    /// `BEGIN` snapshots it once for the whole transaction.
    last_seen_lsn: u64,
    /// The local copy this client reads through, if a host attached one. Boxed,
    /// so a client that never mirrors pays one `None` and no allocation.
    pub(crate) mirror: Option<Box<crate::mirror::MirrorState>>,
}

// The client-facing types must stay `Send`: `gnitz-py` drops the GIL inside
// `Python::detach`, whose `Ungil` bound is `Send`, and `gnitz-tokio` spawns a
// `Session`-owning future onto a multi-thread runtime. An `Rc` anywhere in
// their reachable set otherwise breaks the build a crate away, naming a pyo3
// trait rather than the field.
//
// A `GnitzClient` is deliberately *not* `Sync`: an attached store is a live
// engine. `Session` is, because `gnitz-py` exposes a bare one as a `#[pyclass]`
// and pyo3 demands `Sync` of every pyclass it holds by value; `ClientTransport`
// follows, because a `Session` holds one.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send::<GnitzClient>();
    assert_send_sync::<Session>();
    assert_send_sync::<crate::protocol::ClientTransport>();
    assert_send::<ZSetBatch>();
    assert_send::<ClientError>();
    // Handed back as `Arc<Schema>` by the scan path, and `Arc<T>: Send` requires
    // `T: Send + Sync`, so this one is the stricter bound.
    assert_send_sync::<Schema>();
};

impl GnitzClient {
    pub fn connect(target: &str) -> Result<Self, ClientError> {
        // Seed the OCC basis from the HELLO ACK watermark. A restart yields a
        // fresh GnitzClient re-seeded from the new ACK, so a basis never spans it.
        let (session, last_seen_lsn) = Session::connect(target)?;
        Ok(GnitzClient {
            session,
            park_hook: None,
            serial_cache: HashMap::new(),
            scope: None,
            txn: None,
            last_seen_lsn,
            mirror: None,
        })
    }

    /// A client over an already-connected session, for the scripted-peer tests.
    /// The twin of [`Session::from_transport`], in the crate that owns both
    /// types.
    #[cfg(test)]
    pub(crate) fn from_session(session: Session) -> GnitzClient {
        GnitzClient {
            session,
            park_hook: None,
            serial_cache: HashMap::new(),
            scope: None,
            txn: None,
            last_seen_lsn: 0,
            mirror: None,
        }
    }

    /// Requests this connection has submitted. Exposed for the
    /// round-trip-count assertions; see [`Session::requests_sent`].
    pub fn requests_sent(&self) -> u64 {
        self.session.requests_sent()
    }

    pub fn set_park_hook(&mut self, hook: Option<ParkHook>) {
        self.park_hook = hook;
    }

    /// The client's current OCC basis (running max of observed watermarks).
    /// Read by the SQL layer as its autocommit/BEGIN basis.
    pub fn last_seen_lsn(&self) -> u64 {
        self.last_seen_lsn
    }

    // ── The blocking driver ────────────────────────────────────────────────

    pub(crate) fn round_trip(&mut self, req: Request<'_>) -> Result<Reply, ClientError> {
        let slot = self.session.submit(req)?;
        self.await_slot(slot, None)
    }

    /// Step and park until `slot` completes, handing `sink` every delta-poll
    /// position the steps fill.
    fn await_slot(&mut self, slot: SlotId, mut sink: Option<&mut PollSink<'_>>) -> Result<Reply, ClientError> {
        let mut ready = Interest::WRITE;
        loop {
            match self.session.step_polling(ready, sink.as_deref_mut()) {
                Ok(mut done) => {
                    if let Some(i) = done.iter().position(|(s, _)| *s == slot) {
                        return done.swap_remove(i).1;
                    }
                }
                Err(e) => {
                    // The framing is lost.
                    self.session.close();
                    return Err(e);
                }
            }
            ready = park(&self.session, &mut self.park_hook)?;
        }
    }

    /// Advance `last_seen_lsn` from a commit reply: `Ok(lsn)` and a
    /// `TxnConflict { fresh_basis }` both carry a server watermark `≤ published()`.
    /// Threaded through every push / txn_commit / commit_rmw return so the basis
    /// tracks the freshest value the connection has seen. Returns the result
    /// unchanged.
    fn track_lsn(&mut self, r: Result<u64, ClientError>) -> Result<u64, ClientError> {
        match &r {
            Ok(lsn) => self.last_seen_lsn = self.last_seen_lsn.max(*lsn),
            Err(ClientError::TxnConflict { fresh_basis }) => self.last_seen_lsn = self.last_seen_lsn.max(*fresh_basis),
            _ => {}
        }
        r
    }

    /// Open a statement scope: every relation this statement resolves is
    /// remembered in it and dropped at [`Self::end_statement`], so nothing
    /// survives a catalog write and there is no cross-statement state to
    /// invalidate. The SQL planner brackets each statement with begin/end.
    pub fn begin_statement(&mut self) {
        self.scope = Some(CatalogSnapshot::default());
    }

    /// Close the statement scope. The next statement opens a fresh one, so a DDL
    /// write in this statement is visible to the next.
    pub fn end_statement(&mut self) {
        self.scope = None;
    }

    /// What this statement has resolved: the whole catalog input a pure planning
    /// pass reads. Empty outside a statement bracket.
    pub fn catalog(&self) -> &CatalogSnapshot {
        self.scope.as_ref().unwrap_or(&EMPTY_CATALOG)
    }

    /// Record a resolve this client performed itself, which
    /// [`Self::resolve_local_first`] must do: the planner's resolve loop reads
    /// the statement snapshot back rather than the return value, and that is its
    /// termination proof. A no-op outside a statement bracket.
    fn record_relation(&mut self, qname: String, desc: Option<Arc<RelDescriptor>>) {
        if let Some(scope) = &mut self.scope {
            scope.insert_qname(qname, desc);
        }
    }

    /// Reserve `count` contiguous SERIAL ids for `table_id` and return the first,
    /// so an INSERT that knows its row count pays one fsynced durable advance
    /// rather than `ceil(count / SERIAL_RANGE_SIZE)`. An abandoned tail — the old
    /// range's, or this reservation's — is the intentional PostgreSQL-style gap.
    pub fn reserve_serial_ids(&mut self, table_id: u64, count: u64) -> Result<u64, ClientError> {
        match self.serial_cache.get_mut(&table_id) {
            Some(r) if r.end.saturating_sub(r.next) >= count => {
                let base = r.next;
                r.next += count;
                Ok(base)
            }
            // Refill, abandoning whatever tail the old range still held.
            _ => {
                let want = count.max(SERIAL_RANGE_SIZE);
                let base = self.alloc(IdRun::Serial { table_id, count: want })?;
                self.serial_cache
                    .insert(table_id, SerialRange { next: base + count, end: base + want });
                Ok(base)
            }
        }
    }

    // --- Raw ops ---

    /// Allocate `run`, returning its first id.
    fn alloc(&mut self, run: IdRun) -> Result<u64, ClientError> {
        self.round_trip(Request::Alloc(run)).map(Reply::into_id)
    }

    /// Allocate one catalog object id (schema, relation or index).
    pub fn alloc_id(&mut self) -> Result<u64, ClientError> {
        self.alloc(IdRun::Ids(1))
    }

    pub fn push(&mut self, table_id: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, ClientError> {
        self.push_with_mode(table_id, schema, batch, WireConflictMode::Update)
    }

    /// Push with an explicit `WireConflictMode`. SQL `INSERT` uses `Error` to get
    /// SQL-standard rejection semantics; all other callers pass `Update` (or use
    /// the plain `push`, which defaults to `Update`).
    ///
    /// Inside an open transaction the batch is buffered instead of sent, and the
    /// returned LSN is `0` — nothing is durable until `txn_commit`, which returns
    /// the one zone LSN covering the whole bundle.
    pub fn push_with_mode(
        &mut self,
        table_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        if let Some(txn) = &mut self.txn {
            txn.push(table_id, schema, batch.clone(), mode);
            return Ok(0);
        }
        let r = self.send_push(table_id, schema, batch, mode);
        self.track_lsn(r)
    }

    /// [`Self::push_with_mode`] for a caller that owns the batch and drops it:
    /// inside a transaction the rows move into the buffer instead of being deep
    /// cloned. The borrowing form stays for callers that cannot move (the Python
    /// driver holds a `PyRef`).
    pub fn push_owned(
        &mut self,
        table_id: u64,
        schema: &Schema,
        batch: ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        if let Some(txn) = &mut self.txn {
            txn.push(table_id, schema, batch, mode);
            return Ok(0);
        }
        let r = self.send_push(table_id, schema, &batch, mode);
        self.track_lsn(r)
    }

    /// A push, retried once on a schema mismatch — which evicted the stale cache
    /// entry, so the retry goes out cold.
    fn send_push(
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

    /// Autocommit read-modify-write commit: ship a one-family, one-precondition
    /// `PUSH_TXN` frame asserting `table_id` has not been written since
    /// `basis`. Returns the zone LSN on success; a `ClientError::TxnConflict`
    /// (whose `fresh_basis` the caller adopts and re-reads with) means the table
    /// was written since `basis`. Autocommit only — the SQL driver calls this
    /// only when no transaction is open. Both outcomes advance `last_seen_lsn`.
    pub fn commit_rmw(
        &mut self,
        table_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        basis: u64,
    ) -> Result<u64, ClientError> {
        let r = self
            .round_trip(Request::PushTxn {
                families: &[(table_id, schema, batch, WireConflictMode::Update)],
                preconditions: &[(table_id, basis)],
            })
            .map(Reply::into_lsn);
        self.track_lsn(r)
    }

    /// Buffer an RMW write into the open transaction AND record `table_id` in the
    /// transaction's read-set, so `COMMIT` ships an OCC precondition for it. Used
    /// by UPDATE / DELETE / INSERT ... ON CONFLICT, which read `table_id` before
    /// writing. A blind INSERT uses plain `push_with_mode` and records nothing (a
    /// blind write cannot lose an update). No-op outside a transaction.
    pub fn txn_push_rmw(&mut self, table_id: u64, schema: &Schema, batch: ZSetBatch) {
        if let Some(txn) = &mut self.txn {
            txn.push(table_id, schema, batch, WireConflictMode::Update);
            txn.record_read(table_id);
        }
    }

    pub fn scan(&mut self, table_id: u64) -> ScanResult {
        self.round_trip(Request::scan(table_id)).map(Reply::into_scan)
    }

    /// Run a parameterized bounded read — the ad-hoc SELECT access path — decoding
    /// every reply frame under `reply_schema`, since the server sends no schema block.
    pub fn scan_spec(
        &mut self,
        table_id: u64,
        spec: &gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<ZSetBatch, ClientError> {
        let reply_schema = ReplySchema::new(Arc::clone(reply_schema), table_id);
        self.round_trip(Request::ScanSpec {
            target_id: table_id,
            spec,
            reply_schema: &reply_schema,
        })
        .map(Reply::into_rows)
    }

    // ── The read seam ──────────────────────────────────────────────────────
    //
    // `resolve`, `scan` and `scan_spec` are the connection; the `_local_first`
    // trio below consults the copy and falls through to them, so every call site
    // declares which freshness it is asking for. **The gate is what the copy
    // holds, never whether a store is attached**, so a client with one reads
    // exactly like a client without for every relation the copy does not hold.

    /// [`Self::resolve`], answered off a mirrored registration when there is one
    /// — which is what keeps a mirrored `SELECT` round-trip-free, since the
    /// statement scope is dropped whole and delegating would cost one RESOLVE per
    /// statement.
    ///
    /// The trade is a name → id binding as stale as the copy itself; the feed
    /// detects it (the next poll's tag stops continuing) and the recovery
    /// re-resolves before it reseeds. A name binds locally exactly when its copy
    /// answers locally: the cursor is the one gate, for names and reads alike.
    /// The stored names are canonical, so part-wise case-insensitive equality is
    /// the same test as folding the joined name, without the allocation.
    pub fn resolve_local_first(
        &mut self,
        schema_name: &str,
        name: &str,
    ) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let local = self.mirror.as_deref().and_then(|m| {
            m.views
                .iter()
                // The two string compares first: they are what discriminates,
                // and the store probe is the more expensive of the three.
                .find(|(&t, v)| {
                    v.schema_name.eq_ignore_ascii_case(schema_name)
                        && v.name.eq_ignore_ascii_case(name)
                        && m.store.cursor_of(t).is_some()
                })
                .map(|(_, v)| Arc::clone(&v.desc))
        });
        match local {
            Some(desc) => {
                self.record_relation(qualified_name(schema_name, name), Some(Arc::clone(&desc)));
                Ok(Some(desc))
            }
            None => self.resolve(schema_name, name),
        }
    }

    /// The registration and the store behind a local read of `tid` — the gate
    /// [`Self::mirrors`] tests, returning what it proved. `None` when the copy
    /// does not hold `tid` and the read is the caller's to delegate.
    pub(crate) fn local_read(&mut self, tid: u64) -> Option<(&MirroredView, &mut dyn MirrorStore)> {
        let m = self.mirror.as_deref_mut()?;
        m.store.cursor_of(tid)?;
        let MirrorState { store, views, .. } = m; // disjoint fields, so two borrows
        Some((views.get(&tid)?, store.as_mut()))
    }

    /// Every row of `table_id` off the copy, or `None` when the copy does not
    /// hold it and the read is the caller's to delegate — which an async handle
    /// does on its own connection rather than this one, and
    /// [`Self::scan_local_first`] does here.
    pub fn scan_local(&mut self, table_id: u64) -> Result<Option<ScanReply>, ClientError> {
        let Some((view, store)) = self.local_read(table_id) else {
            return Ok(None);
        };
        let schema = Arc::clone(&view.desc.schema);
        let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None);
        let batch = store.scan_spec(table_id, spec, &schema)?;
        Ok(Some(ScanReply { schema, batch, lsn: None }))
    }

    /// [`Self::scan`], answered off the copy when it holds `table_id`.
    ///
    /// The served LSN is `None` for a local answer: it is a server-side counter,
    /// and a copy's freshness is a feed round — [`Self::cursor_of`] is where a
    /// host reads it.
    pub fn scan_local_first(&mut self, table_id: u64) -> Result<ScanReply, ClientError> {
        match self.scan_local(table_id)? {
            Some(r) => Ok(r),
            None => self.scan(table_id),
        }
    }

    /// [`Self::scan_spec`], answered off the copy when it holds `table_id`.
    ///
    /// A held answer is the answer, empty or not; only "not held" falls through.
    pub fn scan_spec_local_first(
        &mut self,
        table_id: u64,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<ZSetBatch, ClientError> {
        if let Some((_, store)) = self.local_read(table_id) {
            return Ok(store.scan_spec(table_id, spec, reply_schema)?);
        }
        self.scan_spec(table_id, &spec, reply_schema)
    }

    /// Replace the connection and keep the copies — what a host does after a
    /// server restart, which kills the socket while the copies survive it.
    ///
    /// Refused while a transaction is open. Otherwise the client is rebuilt
    /// through [`Self::connect`] rather than reset field by field, which keeps
    /// the statement scope, the transaction slot, the SERIAL cache and the OCC
    /// basis from being enumerated here and drifting. Nothing is taken out of
    /// `self` until the new session exists, so a failed connect leaves this
    /// client exactly as it was.
    ///
    /// A copy rides along with every cursor dropped: the new connection may be a
    /// different server, where the same name is a different id, so the next
    /// poll re-resolves every view by name. A poisoned store crosses unchanged.
    pub fn reconnect(&mut self, target: &str) -> Result<(), ClientError> {
        if self.txn_active() {
            return Err(ClientError::ServerError(
                "reconnect inside a transaction; commit or roll back first".to_string(),
            ));
        }
        let mut fresh = GnitzClient::connect(target)?;
        // The hook is what keeps a blocking call Ctrl-C-interruptible.
        fresh.park_hook = self.park_hook.take();
        fresh.mirror = self.mirror.take();
        if let Some(m) = fresh.mirror.as_deref_mut() {
            m.store.clear_cursors();
        }
        *self = fresh;
        Ok(())
    }

    /// Bootstrap a view's delta feed: the view's whole current value at its true
    /// net weights, in the view's own schema, and the cursor to poll from. It
    /// replaces a copy's state; it does not add to it.
    pub fn delta_bootstrap(
        &mut self,
        view_id: u64,
        view_schema: &Arc<Schema>,
    ) -> Result<(ZSetBatch, DeltaCursor), ClientError> {
        self.delta_read(view_id, 0, &ReplySchema::new(Arc::clone(view_schema), view_id))
    }

    /// Poll a view's delta feed: every delta it emitted in `(cursor.tick, T]`,
    /// in [`delta_reply_schema`]'s shape, with the cursor to poll from next.
    /// Apply what comes back and store the new cursor; there is nothing to
    /// filter and nothing to reconcile.
    ///
    /// Both refusals a poll can answer with — a cursor at tick `0`, and a reply
    /// whose tag does not continue the cursor — are `DeltaCursor::poll_after`
    /// and `DeltaCursor::advanced_to`, which state the rule once for every
    /// caller. Both surface as [`ClientError::DeltaExpired`], whose recovery is
    /// to discard the copy and [`delta_bootstrap`](Self::delta_bootstrap) again.
    ///
    /// A poll does **not** drive a tick: a delta read answers "what has
    /// happened", not "what is current", so a push the tick loop has not run yet
    /// is a round the next poll will carry.
    pub fn delta_poll(
        &mut self,
        view_id: u64,
        cursor: DeltaCursor,
        reply_schema: &Arc<Schema>,
    ) -> Result<(ZSetBatch, DeltaCursor), ClientError> {
        let rs = ReplySchema::new(Arc::clone(reply_schema), view_id);
        let (data, next) = self.delta_read(view_id, cursor.poll_after()?, &rs)?;
        Ok((data, cursor.advanced_to(next)?))
    }

    /// [`Self::delta_bootstrap`] handing back the reply's *undecoded* blocks —
    /// what the mirror state machine feeds a store.
    ///
    /// Decoding to a `ZSetBatch` walks every OPK key back to a native value, and
    /// re-encoding it costs the walk again plus a second full region copy — to
    /// reconstruct the block the socket already delivered.
    pub(crate) fn delta_bootstrap_raw(
        &mut self,
        view_id: u64,
        view_schema: &ReplySchema,
    ) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        self.delta_read_raw(view_id, 0, view_schema)
    }

    /// One view's delta read, decoded under `reply_schema`, with the terminal
    /// frame's `(tag, T)` pair as a cursor.
    fn delta_read(
        &mut self,
        view_id: u64,
        after_tick: u64,
        reply_schema: &ReplySchema,
    ) -> Result<(ZSetBatch, DeltaCursor), ClientError> {
        let (blocks, cursor) = self.delta_read_raw(view_id, after_tick, reply_schema)?;
        let schema = reply_schema.schema();
        let mut data = ZSetBatch::new(&schema);
        for b in &blocks {
            crate::protocol::wal_block::decode_wal_block_into(&mut data, b.block(), &schema)?;
        }
        Ok((data, cursor))
    }

    /// [`Self::delta_read`] keeping the reply's raw blocks. The cursor comes back
    /// **unchecked** against a previous one — the tag rule belongs to the caller
    /// that holds it.
    pub(crate) fn delta_read_raw(
        &mut self,
        view_id: u64,
        after_tick: u64,
        reply_schema: &ReplySchema,
    ) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        let item = DeltaPollItem {
            view_id,
            after_tick,
            reply_block: reply_schema.block(),
        };
        let slot = self.session.submit_delta_poll(&[item])?;
        let mut got: Option<PolledView> = None;
        let mut sink = |s: SlotId, view: PolledView| {
            if s == slot {
                got = Some(view);
            }
        };
        // A per-view fault fills the position and completes the slot `Ok`; a
        // frame-level rejection completes it `Err` with no position filled.
        self.await_slot(slot, Some(&mut sink))?;
        got.expect("a one-view poll completes by filling its position")
    }

    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table `txn_commit` is never observed torn
    /// across the result set. Like `scan`, it leaves `last_seen_lsn` untouched.
    pub fn scan_many(&mut self, table_ids: &[u64]) -> MultiScanResult {
        self.round_trip(Request::ScanMulti(table_ids)).map(Reply::into_multi)
    }

    /// A point SEEK by primary key: `key` is the packed PK columns in the wire's
    /// **native** little-endian key space. The engine OPK-encodes it against the
    /// relation's schema, so no caller here needs one.
    pub fn seek(&mut self, table_id: u64, key: &[u8]) -> ScanResult {
        self.round_trip(Request::seek(table_id, key)).map(Reply::into_scan)
    }

    /// The statement's descriptor for `tid` — the by-id twin of [`Self::resolve`].
    /// A tid usually comes from resolving the same relation by name earlier in the
    /// statement, which makes this a scope hit; a tid from anywhere else falls back
    /// to a by-id round trip rather than reporting an empty index list.
    pub fn describe_by_id(&mut self, tid: u64) -> Result<Arc<RelDescriptor>, ClientError> {
        if let Some(d) = self.catalog().by_tid(tid) {
            return Ok(d);
        }
        self.round_trip(Request::Resolve(RelTarget::Id(tid)))
            .map(Reply::into_resolve)?
            .ok_or_else(|| ClientError::NotFound { noun: "relation", name: tid.to_string() })
    }

    /// Persist a secondary-index catalog row over an already-resolved base table.
    ///
    /// Resolution and view-rejection are the caller's responsibility: an index
    /// may only back a base table — a view is a read-only derived relation whose
    /// store is maintained solely by its circuit, so indexing a snapshot of
    /// derived data has no defined semantics — and the SQL layer rejects a view
    /// target with a precise error before reaching here. `col_indices`/`col_types`
    /// identify the indexed columns within the resolved table, in declared order;
    /// `index_name` is the final catalog name (auto-generated or user-supplied)
    /// that `DROP INDEX` resolves against. A 1-element list is the single-column
    /// case; the persisted `source_cols` slot always carries `pack_pk_cols`.
    pub fn create_index(
        &mut self,
        table_id: u64,
        col_indices: &[u32],
        col_types: &[TypeCode],
        index_name: &str,
        is_unique: bool,
    ) -> Result<u64, ClientError> {
        let index_name = gnitz_wire::canonical_identifier(index_name)?;
        // Arity, 7-bit column range, duplicates — the Err form of the
        // pack_pk_cols contract, so the pack below can never panic.
        gnitz_wire::validate_pk_col_list(col_indices, gnitz_wire::PK_LIST_COL_LIMIT)
            .map_err(|e| ClientError::ServerError(format!("create_index: {e}")))?;
        if col_types.len() != col_indices.len() {
            return Err(ClientError::ServerError(
                "create_index: col_indices and col_types length mismatch".to_string(),
            ));
        }
        // The engine's own promotion rule, so this gateway admits exactly what
        // `make_index_schema` will. The singular: the plural's arity/stride caps
        // need the source relation's PK, which this raw-binary entry point would
        // have to fetch and which `precheck_index_family` enforces anyway.
        for &ct in col_types {
            gnitz_wire::index_key_type(ct as u8)?;
        }

        // No client-side name probe: the engine rejects a duplicate against both
        // the persisted `index_by_name` and the rest of this bundle. A rejected
        // bundle burns this index_id, which costs nothing — ids are never reused.
        let index_id = self.alloc_id()?;

        let idx_schema = sys_schema(IDX_TAB);
        let mut batch = ZSetBatch::new(idx_schema);
        gnitz_wire::sys_rows::write_idx_tab_row(
            &mut BatchAppender::new(&mut batch, idx_schema),
            &IdxTabRow {
                index_id,
                owner_id: table_id,
                source_col_idx: gnitz_wire::pack_pk_cols(col_indices),
                name: &index_name,
                flags: gnitz_wire::IndexProps { is_unique }.pack(),
            },
            1,
        );

        self.push_ddl_txn(&[(IDX_TAB, batch)])?;
        Ok(index_id)
    }

    /// Drop indexes by name as **one** DDL zone: the whole set retires or none of
    /// it does, and a name repeated in `index_names` retires once.
    pub fn drop_indexes_by_name(&mut self, index_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(index_names, "index", if_exists, |_, _| true)
    }

    /// `ALTER TABLE … DROP CONSTRAINT`: the UNIQUE index `name` of table `tid`.
    /// The `-1` is the stored row, so the engine's CAS re-proves owner and
    /// uniqueness against the live row.
    pub fn drop_unique_constraint(&mut self, tid: u64, name: &str, if_exists: bool) -> Result<(), ClientError> {
        let s = sys_schema(IDX_TAB);
        self.drop_index_rows(&[name], "constraint", if_exists, |b, i| {
            col_u64(b, s, IDXTAB_COL_OWNER_ID, i) == tid
                && gnitz_wire::IndexProps::from_flags(col_u64(b, s, IDXTAB_COL_FLAGS, i)).is_unique
        })
    }

    /// Retract the live IDX_TAB rows named in `names` that pass `matches`.
    ///
    /// `if_exists` is answered at this verb's own not-found path, never by a
    /// pre-check: a pre-check leaves a window a concurrent DROP can land in and
    /// resurface the "not found" it must suppress.
    fn drop_index_rows(
        &mut self,
        names: &[&str],
        noun: &'static str,
        if_exists: bool,
        matches: impl Fn(&ZSetBatch, usize) -> bool,
    ) -> Result<(), ClientError> {
        let scanned = checked_sys_rows(IDX_TAB, self.scan(IDX_TAB)?)?;
        let idx_schema = sys_schema(IDX_TAB);
        let mut batch = ZSetBatch::new(idx_schema);
        let mut retired: Vec<usize> = Vec::with_capacity(names.len());
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            let mut hit = None;
            for i in scanned.live_rows() {
                if col_str(&scanned, idx_schema, IDXTAB_COL_NAME, i)? == name && matches(&scanned, i) {
                    hit = Some(i);
                    break;
                }
            }
            match hit {
                Some(i) => {
                    // A repeated name retires once: the row is already in the batch.
                    if !retired.contains(&i) {
                        retired.push(i);
                        batch.copy_row_at(&scanned, i, -1);
                    }
                }
                None if if_exists => {}
                None => return Err(ClientError::NotFound { noun, name }),
            }
        }
        // Every name skipped: no zone, so no barrier and no fdatasync.
        if batch.is_empty() {
            return Ok(());
        }
        self.push_ddl_txn(&[(IDX_TAB, batch)])
    }

    /// `(id, name, indexed columns)` of every live secondary index. Names come back
    /// canonical (lowercase): every writer folds them at store time.
    pub fn index_rows(&mut self) -> Result<Vec<(u64, String, gnitz_wire::PkColList)>, ClientError> {
        let idx_batch = checked_sys_rows(IDX_TAB, self.scan(IDX_TAB)?)?;
        let mut out = Vec::new();
        for i in idx_batch.live_rows() {
            let name = col_str(&idx_batch, sys_schema(IDX_TAB), IDXTAB_COL_NAME, i)?.to_string();
            let cols = gnitz_wire::unpack_pk_cols(col_u64(&idx_batch, sys_schema(IDX_TAB), IDXTAB_COL_SOURCE_COLS, i))
                .map_err(|rule| ClientError::ServerError(format!("index '{name}': {rule}")))?;
            out.push((idx_batch.pks.get(sys_schema(IDX_TAB), i) as u64, name, cols));
        }
        Ok(out)
    }

    /// Delete `pks` from `table_id` (retraction rows). Buffered like any other
    /// write while a transaction is open, through the by-value entry point:
    /// this call builds the batch and drops it, so nothing is cloned.
    pub fn delete(&mut self, table_id: u64, schema: &Schema, pks: PkColumn) -> Result<(), ClientError> {
        if pks.is_empty() {
            return Ok(());
        }
        let batch = retraction_batch(schema, pks);
        self.push_owned(table_id, schema, batch, WireConflictMode::Update)?;
        Ok(())
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

    /// Open a transaction. Errors if one is already open. Snapshots the current
    /// `last_seen_lsn` as the transaction-wide OCC basis — one value for every
    /// read-set table (sound because `last_seen ≤ published() ≤` any later read's
    /// snapshot; strictly conservative, and it avoids the unsound footgun of
    /// raising a table's basis on a later scan).
    pub fn txn_begin(&mut self) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::ServerError("transaction already open".into()));
        }
        self.txn = Some(TxnBuffer {
            basis: self.last_seen_lsn,
            ..Default::default()
        });
        Ok(())
    }

    /// Discard the open transaction (ROLLBACK): drop the buffer, sending
    /// nothing. Errors if no transaction is open.
    pub fn txn_rollback(&mut self) -> Result<(), ClientError> {
        self.txn
            .take()
            .map(|_| ())
            .ok_or_else(|| ClientError::ServerError("no transaction open".into()))
    }

    /// Commit the open transaction as one atomic `PUSH_TXN` frame — all
    /// families land together under one durable zone LSN, or none do. Returns
    /// that LSN (0 for an empty transaction, which needs no wire roundtrip).
    /// Errors if no transaction is open.
    ///
    /// The buffer is taken OUT before the fallible send, so an engine-side
    /// failure leaves the transaction already closed ("COMMIT consumes the
    /// transaction") — the SQL planner's opened-this-call rollback then finds
    /// nothing open, so there is no double path.
    pub fn txn_commit(&mut self) -> Result<u64, ClientError> {
        let buf = self
            .txn
            .take()
            .ok_or_else(|| ClientError::ServerError("no transaction open".into()))?;
        if buf.families.is_empty() {
            return Ok(0);
        }
        let fam_refs: Vec<(u64, &Schema, &ZSetBatch, WireConflictMode)> = buf
            .families
            .iter()
            .map(|f| (f.tid, &f.schema, &f.batch, f.mode))
            .collect();
        // One precondition per read-set tid at the transaction-wide basis. The
        // read-set is deduped and a subset of the buffered-write (family) tids, so
        // every precondition tid is a family tid — the engine's `preconditions ⊆
        // families` rule holds by construction.
        let preconditions: Vec<(u64, u64)> = buf.read_set.iter().map(|&t| (t, buf.basis)).collect();
        let r = self
            .round_trip(Request::PushTxn {
                families: &fam_refs,
                preconditions: &preconditions,
            })
            .map(Reply::into_lsn);
        self.track_lsn(r)
    }

    /// The open transaction's buffered ops on `tid`. `None` in autocommit.
    pub fn txn_reads(&mut self, tid: u64) -> Option<TxnReads<'_>> {
        Some(self.txn.as_mut()?.reads(tid))
    }

    // --- DDL ---

    /// Catalog write choke point. DDL commits atomically on its own and never
    /// joins an open transaction's buffered user-table writes — a batch buffered
    /// under the old schema would guarantee a commit-time mismatch. Reject it here
    /// at the state owner, mirroring `push_with_mode`'s single branch for data
    /// writes; the SQL front end's own rejection is then a friendlier early error,
    /// not the sole enforcement.
    /// The zone LSN is dropped rather than returned: `record_commit_lsn` is
    /// reached only from the push and user-transaction handlers, never from the
    /// DDL one, so a DDL never raises a user table's commit LSN and there is no
    /// basis here for `track_lsn` to advance.
    ///
    /// Being the choke point is also what lets it retire this client's own
    /// copies, in [`Self::forget_retired_views`], rather than each DDL verb
    /// carrying a teardown of its own.
    pub fn push_ddl_txn(&mut self, families: &[(u64, ZSetBatch)]) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::ServerError(
                "DDL is not allowed inside a transaction".into(),
            ));
        }
        self.round_trip(Request::DdlTxn(families))?;
        self.forget_retired_views(families);
        Ok(())
    }

    /// Stop mirroring every view this committed bundle retired: a VIEW_TAB pk at
    /// negative weight and **not** at positive weight. A pk at both is a rewrite
    /// pair, which for VIEW_TAB is only a rename — handled at
    /// [`Self::alter_rename_relation`], where the new name is in hand.
    ///
    /// No DDL retires a view its own batch does not name: `DROP TABLE` and the
    /// `ALTER TABLE` column ops are RESTRICT, `DROP SCHEMA` emits a `-1` per
    /// member, and a replaced view's lone `-1` rides the replacement's batch. The
    /// engine's cascade reaches only hidden chain segments, which
    /// `canonical_identifier` refuses to name and so cannot be mirrored.
    fn forget_retired_views(&mut self, families: &[(u64, ZSetBatch)]) {
        if self.mirror.is_none() {
            return;
        }
        let Some((_, b)) = families.iter().find(|(family, _)| *family == VIEW_TAB) else {
            return;
        };
        // A create-only bundle allocates nothing.
        if !b.weights.iter().any(|&w| w < 0) {
            return;
        }
        let s = sys_schema(VIEW_TAB);
        let vid = |i| b.pks.get(s, i) as u64;
        let rewritten: Vec<u64> = b.live_rows().map(vid).collect();
        for i in 0..b.len() {
            if b.weights[i] < 0 && !rewritten.contains(&vid(i)) {
                self.invalidate_own_copy(vid(i));
            }
        }
    }

    pub fn create_schema(&mut self, name: &str) -> Result<u64, ClientError> {
        // Reject the empty string, a leading `_` (reserved system prefix), and
        // illegal characters. The SQL planner has no CREATE SCHEMA surface, so
        // this client entry point is the sole enforcement for schema names.
        let name = gnitz_wire::canonical_identifier(name)?;
        let new_sid = self.alloc_id()?;
        let schema = sys_schema(SCHEMA_TAB);
        let mut batch = ZSetBatch::new(schema);
        gnitz_wire::sys_rows::write_schema_tab_row(
            &mut BatchAppender::new(&mut batch, schema),
            &gnitz_wire::sys_rows::SchemaTabRow { schema_id: new_sid, name: &name },
            1,
        );
        self.push_ddl_txn(&[(SCHEMA_TAB, batch)])?;
        Ok(new_sid)
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
    /// the whole bundle and drops nothing. A schema whose view text exceeds the
    /// frame cap must be drained with individual `DROP VIEW`s.
    pub fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
        let name = gnitz_wire::canonical_identifier(name)?;
        let schema_id = self.lookup_schema_id(&name)?;

        // Hidden segments need no separate pass: each is an ordinary VIEW_TAB row
        // carrying this `schema_id`, so the whole matching set is already complete
        // — and the engine's co-drop carve-out admits it, every dependent being in
        // the same drop set.
        let schema_s = sys_schema(SCHEMA_TAB);

        let vb = self.schema_retractions(VIEW_TAB, schema_id)?;
        let tb = self.schema_retractions(TABLE_TAB, schema_id)?;

        // `create_schema`'s own writer at `-1`: both values are in hand, so this
        // family keeps exactly one writer.
        let mut sb = ZSetBatch::new(schema_s);
        gnitz_wire::sys_rows::write_schema_tab_row(
            &mut BatchAppender::new(&mut sb, schema_s),
            &gnitz_wire::sys_rows::SchemaTabRow { schema_id, name: &name },
            -1,
        );

        let mut families: Vec<(u64, ZSetBatch)> = Vec::with_capacity(3);
        if !vb.is_empty() {
            families.push((VIEW_TAB, vb));
        }
        if !tb.is_empty() {
            families.push((TABLE_TAB, tb));
        }
        families.push((SCHEMA_TAB, sb));
        self.push_ddl_txn(&families)
    }

    /// `unique_indexes` are the table's inline `UNIQUE` constraints, folded into
    /// the same atomic DDL bundle as `[COL_TAB, TABLE_TAB, IDX_TAB]` so a failure
    /// rolls the whole `CREATE` back — never a table left missing its unique
    /// constraint. Pass an empty slice for a table with no inline UNIQUE. A
    /// stream owns no index: an IDX_TAB row naming one is refused by the engine's
    /// index owner check, and the whole bundle with it.
    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        pk_cols: &[u32],
        props: TableProps,
        unique_indexes: &[InlineUniqueIndex],
    ) -> Result<u64, ClientError> {
        let table_name = gnitz_wire::canonical_identifier(table_name)?;
        let index_names: Vec<String> = unique_indexes
            .iter()
            .map(|spec| gnitz_wire::canonical_identifier(spec.name).map_err(ClientError::ServerError))
            .collect::<Result<_, _>>()?;
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        // Full schema-admissibility rule set (column cap + PK rules), applied
        // here so a caller that skipped the planner gets a clean error before
        // any id allocation instead of relying on the server-side reject (and
        // `pack_pk_cols` below can never panic).
        Schema::validate_parts(pk_cols, columns).map_err(|e| ClientError::ServerError(format!("create_table: {e}")))?;
        // The rule the flags packing cannot represent, shared with the planner
        // and the engine's own decoder.
        props
            .validate()
            .map_err(|e| ClientError::ServerError(format!("create_table: {e}")))?;
        // `dist_prefix_len` is a leading-PK-prefix length (0 = default = full PK).
        // Shared with the engine's own TABLE_TAB decoder, which re-checks it: this
        // one catches the caller's mistake before any id is allocated.
        props
            .validate_against_pk(pk_cols.len())
            .map_err(|e| ClientError::ServerError(format!("create_table: {e}")))?;

        // Column types come from `columns`, so a UNIQUE+FK column's
        // parent-rewritten type is used.
        for spec in unique_indexes {
            // Structural rules only (arity, in-range, no duplicates) — unlike a
            // PK, an indexed column may be nullable. In-range against the actual
            // column list also keeps the `columns[c]` read panic-free.
            gnitz_wire::validate_pk_col_list(spec.col_indices, columns.len()).map_err(|msg| {
                ClientError::ServerError(format!("create_table: unique index '{}': {msg}", spec.name))
            })?;
            for &c in spec.col_indices {
                gnitz_wire::index_key_type(columns[c as usize].type_code as u8)?;
            }
        }

        // The table's id, then one per inline UNIQUE index.
        let new_tid = self.alloc(IdRun::Ids(1 + unique_indexes.len() as u64))?;
        let schema_id = self.lookup_schema_id(&schema_name)?;

        // Encode the PK list using the shared wire packer so the engine
        // catalog decodes it identically. Single-PK callers still flow
        // through the same packer; there is no second form of the word.
        let pk_packed = gnitz_wire::pack_pk_cols(pk_cols);

        // COL_TAB family — the server sorts families by topo priority, so it
        // ingests columns before the TABLE_TAB register hook that reads them.
        let col_s = sys_schema(COL_TAB);
        let mut col_batch = ZSetBatch::new(col_s);
        append_col_rows(
            &mut BatchAppender::new(&mut col_batch, col_s),
            new_tid,
            OWNER_KIND_TABLE,
            columns,
        );

        // TABLE_TAB family.
        let tbl_schema = sys_schema(TABLE_TAB);
        let mut tb = ZSetBatch::new(tbl_schema);
        gnitz_wire::sys_rows::write_table_tab_row(
            &mut BatchAppender::new(&mut tb, tbl_schema),
            &TableTabRow {
                table_id: new_tid,
                schema_id,
                name: &table_name,
                pk_col_idx: pk_packed,
                flags: props.pack(),
            },
            1,
        );

        let mut families: Vec<(u64, ZSetBatch)> = vec![(COL_TAB, col_batch), (TABLE_TAB, tb)];
        if !unique_indexes.is_empty() {
            let idx_schema = sys_schema(IDX_TAB);
            let mut idx_batch = ZSetBatch::new(idx_schema);
            {
                let mut a = BatchAppender::new(&mut idx_batch, idx_schema);
                for (k, spec) in unique_indexes.iter().enumerate() {
                    gnitz_wire::sys_rows::write_idx_tab_row(
                        &mut a,
                        &IdxTabRow {
                            index_id: new_tid + 1 + k as u64,
                            owner_id: new_tid,
                            source_col_idx: gnitz_wire::pack_pk_cols(spec.col_indices),
                            name: &index_names[k],
                            flags: gnitz_wire::IndexProps { is_unique: true }.pack(),
                        },
                        1,
                    );
                }
            }
            families.push((IDX_TAB, idx_batch));
        }
        self.push_ddl_txn(&families)?;

        Ok(new_tid)
    }

    /// Drop tables as one DDL zone; the engine cascades each one's indexes off
    /// their owner. See [`Self::drop_relations`] for the batch rules.
    pub fn drop_table(&mut self, schema_name: &str, table_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(TABLE_TAB, "table", schema_name, table_names, if_exists)
    }

    pub fn create_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        source_table_id: u64,
        output_columns: &[ColumnDef],
    ) -> Result<u64, ClientError> {
        // A minimal SCAN_DELTA → INTEGRATE_SINK circuit, built through the typed
        // builder so the row materialisation matches the stored layout exactly.
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(source_table_id, gnitz_wire::ReadBound::None);
        circuit.sink(scan);

        let vids = self.create_view_chain(
            schema_name,
            view_name,
            vec![PlannedView {
                circuit,
                output_columns: output_columns.to_vec(),
                // Minimal SCAN→SINK passthrough: single output PK at slot 0.
                pk_cols: vec![0],
            }],
            ViewProps::default(),
            false,
        )?;
        Ok(vids[0])
    }

    /// Create every view in `views` in one atomic `DDL_TXN`. Row order carries no
    /// meaning — the engine orders registration and backfill by the dependencies it
    /// derives from the `ScanDelta` nodes of the circuit rows. Returns the vids in
    /// input order.
    ///
    /// **One `BatchAppender` per family spans all views** — the engine *rejects* a
    /// bundle carrying a second block for a family it has already seen, so a chain
    /// must merge every view's COL/circuit rows into a single batch per family and
    /// one all-`+1` VIEW_TAB batch in input order. A single `push_ddl_txn` then commits — or, via the engine's
    /// per-family precheck/compensate loop, rolls back — the whole chain.
    ///
    /// `pk_cols` for each view is its physical PK column list — the leading `k`
    /// output slots (`[0]` for a synthetic-PK view, `0..k` for a compound-PK
    /// passthrough).
    ///
    /// **`views.last()` is the user-named view: it takes `view_name` and `props`**;
    /// every earlier element is an internal segment it owns — see [`segment_name`].
    /// [`segment_id`]`(j)` inside `views[k]` names `views[j]`, which the engine
    /// requires to precede it.
    ///
    /// `replace` supersedes the view already holding this name: `false` leaves a
    /// name collision to the engine, `true` makes a missing view an error. Its
    /// `-1` rides the same VIEW_TAB batch, so a rejection anywhere in the zone
    /// leaves the old view exactly as it was.
    pub fn create_view_chain(
        &mut self,
        schema_name: &str,
        view_name: &str,
        views: Vec<PlannedView>,
        props: ViewProps,
        replace: bool,
    ) -> Result<Vec<u64>, ClientError> {
        let view_name = gnitz_wire::canonical_identifier(view_name)?;
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        // Reject a malformed chain before any allocation. An empty bundle names
        // no view to create and would return no vid for the caller to use.
        if views.is_empty() {
            return Err(ClientError::ServerError("view chain has no segments".into()));
        }
        if views.len() > MAX_CHAIN_SEGMENTS {
            return Err(ClientError::ServerError(format!(
                "view chain has {} segments, exceeding the {MAX_CHAIN_SEGMENTS}-segment limit",
                views.len(),
            )));
        }

        // Per-view pre-flight validation up front, before any id allocation, so a
        // bad schema surfaces with no residue: `pack_pk_cols` below asserts its
        // contract, so an out-of-range PK list would panic this client rather than
        // reach the wire.
        for (k, pv) in views.iter().enumerate() {
            Schema::validate_parts(&pv.pk_cols, &pv.output_columns)
                .map_err(|e| ClientError::ServerError(format!("View '{view_name}' segment {k}: {e}")))?;
        }

        let schema_id = self.lookup_schema_id(&schema_name)?;

        // The outgoing view's row, resolved before any id is allocated so a
        // missing view surfaces with no residue. Only the user-named view: the
        // engine cascades its segments off `owner_view_id`.
        let replaced = replace
            .then(|| {
                self.relation_retraction(VIEW_TAB, "view", &schema_name, &view_name)?
                    .ok_or_else(|| not_found("view", &schema_name, &view_name))
            })
            .transpose()?;

        // The whole bundle is assigned in one allocation before any substitution
        // runs, because a downstream segment's `ScanDelta` names an upstream
        // segment by its position.
        let base = self.alloc(IdRun::Ids(views.len() as u64))?;
        let vids: Vec<u64> = (0..views.len() as u64).map(|k| base + k).collect();
        // The user-named view is the bundle's last element, and every segment
        // names it as owner.
        let owner_vid = *vids.last().expect("a non-empty bundle");

        // One batch per family, spanning all views. COL_TAB and VIEW_TAB are
        // always non-empty; the circuit family is included only if some view
        // contributed rows.
        let col_s = sys_schema(COL_TAB);
        let nodes_s = sys_schema(CIRCUIT_NODES_TAB);
        let view_s = sys_schema(VIEW_TAB);

        let mut col_batch = ZSetBatch::new(col_s);
        let mut nodes_batch = ZSetBatch::new(nodes_s);
        let mut view_batch = ZSetBatch::new(view_s);

        // 0. The replaced view's retraction, ahead of the new chain's `+1`s.
        if let Some((scanned, i)) = &replaced {
            view_batch.copy_row_at(scanned, *i, -1);
        }

        {
            let mut col_a = BatchAppender::new(&mut col_batch, col_s);
            let mut nodes_a = BatchAppender::new(&mut nodes_batch, nodes_s);
            let mut view_a = BatchAppender::new(&mut view_batch, view_s);

            let last = views.len() - 1;
            for (k, (mut pv, vid)) in views.into_iter().zip(vids.iter().copied()).enumerate() {
                // 0.5. Substitute the bundle's symbolic ids. `base` is below the
                // ceiling, so this cannot overflow; a forward or out-of-range tag
                // becomes an id no lower than the view's own, which the engine
                // refuses.
                for src in pv.circuit.sources_mut() {
                    if *src >= gnitz_wire::CATALOG_ID_CEILING {
                        *src = base + (*src - gnitz_wire::CATALOG_ID_CEILING);
                    }
                }
                let (name, owner_view_id, row_props) = if k == last {
                    (view_name.clone(), 0, props)
                } else {
                    (segment_name(vid), owner_vid, ViewProps::default())
                };

                // 1. Column records.
                append_col_rows(&mut col_a, vid, OWNER_KIND_VIEW, &pv.output_columns);

                // 2. Circuit node rows.
                gnitz_wire::sys_rows::write_circuit_rows(&mut nodes_a, vid, &pv.circuit);

                // 3. View row — the VIEW_TAB register hook triggers server-side
                // compilation. Encode the view PK with the shared wire packer so the
                // engine catalog decodes it identically to a TABLE_TAB PK.
                gnitz_wire::sys_rows::write_view_tab_row(
                    &mut view_a,
                    &ViewTabRow {
                        view_id: vid,
                        schema_id,
                        name: &name,
                        pk_col_idx: gnitz_wire::pack_pk_cols(&pv.pk_cols),
                        props: row_props,
                        owner_view_id,
                    },
                    1,
                );
            }
        }

        // One families entry per tid (mandatory: the engine's derived lists read
        // only the first block per family).
        let mut families: Vec<(u64, ZSetBatch)> = Vec::new();
        families.push((COL_TAB, col_batch));
        if !nodes_batch.is_empty() {
            families.push((CIRCUIT_NODES_TAB, nodes_batch));
        }
        families.push((VIEW_TAB, view_batch));

        self.push_ddl_txn(&families)?;
        Ok(vids)
    }

    /// Drop views as one DDL zone; the engine cascades each one's hidden segments
    /// off `owner_view_id`, so the client never names a segment. See
    /// [`Self::drop_relations`] for the batch rules.
    pub fn drop_view(&mut self, schema_name: &str, view_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(VIEW_TAB, "view", schema_name, view_names, if_exists)
    }

    /// Retire every named relation of `family` in **one** DDL zone: the whole set
    /// goes or none does, and a name repeated in `names` retires once.
    ///
    /// `if_exists` answers a name that does not resolve, and nothing else: a name
    /// that resolves to another family, a dependent view and an FK child outside
    /// the batch all still fail the statement.
    fn drop_relations(
        &mut self,
        family: u64,
        noun: &'static str,
        schema_name: &str,
        names: &[&str],
        if_exists: bool,
    ) -> Result<(), ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        let s = sys_schema(family);
        let mut batch = ZSetBatch::new(s);
        let mut retired: Vec<u64> = Vec::with_capacity(names.len());
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            let Some((scanned, i)) = self.relation_retraction(family, noun, &schema_name, &name)? else {
                if if_exists {
                    continue;
                }
                return Err(not_found(noun, &schema_name, &name));
            };
            let id = scanned.pks.get(s, i) as u64;
            if !retired.contains(&id) {
                retired.push(id);
                batch.copy_row_at(&scanned, i, -1);
            }
        }
        // Every name skipped: no zone, so no barrier and no fdatasync.
        if batch.is_empty() {
            return Ok(());
        }
        self.push_ddl_txn(&[(family, batch)])
    }

    /// The `-1` batch retiring every live row of system family `family` whose
    /// `schema_id` matches — the members `DROP SCHEMA` retracts. Each `-1` is the
    /// scanned row copied verbatim. One code path over TABLE_TAB and VIEW_TAB.
    fn schema_retractions(&mut self, family: u64, schema_id: u64) -> Result<ZSetBatch, ClientError> {
        let s = sys_schema(family);
        let mut out = ZSetBatch::new(s);
        let scanned = checked_sys_rows(family, self.scan(family)?)?;
        for i in scanned.live_rows() {
            if col_u64(&scanned, s, RELTAB_COL_SCHEMA_ID, i) == schema_id {
                out.copy_row_at(&scanned, i, -1);
            }
        }
        Ok(out)
    }

    /// The live `family` row of `name`, by one master-local seek. `Ok(None)` is the
    /// *resolve* miss alone; a name that resolves but holds no row in `family` —
    /// `DROP TABLE <view>` — is the hard `not_found`.
    fn relation_retraction(
        &mut self,
        family: u64,
        noun: &'static str,
        schema_name: &str,
        name: &str,
    ) -> Result<Option<(ZSetBatch, usize)>, ClientError> {
        let Some(desc) = self.resolve(schema_name, name)? else {
            return Ok(None);
        };
        self.seek_sys_row(family, desc.tid as u128, || not_found(noun, schema_name, name))
            .map(Some)
    }

    /// Rename a table or view: a `(-1, +1)` rewrite pair on TABLE_TAB / VIEW_TAB,
    /// same id, the live row at `-1` and the same row with a new `name` at `+1`.
    /// The resolved descriptor picks the family, so the kind cannot disagree with
    /// the id. A rename changes no comparator or layout, so the DDL zone's write
    /// lock is the whole quiesce it needs.
    pub fn alter_rename_relation(
        &mut self,
        schema_name: &str,
        current_name: &str,
        new_name: &str,
    ) -> Result<(), ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        let current_name = gnitz_wire::canonical_identifier(current_name)?;
        let new_name = gnitz_wire::canonical_identifier(new_name)?;
        let missing = || not_found("relation", &schema_name, &current_name);
        // Key the family lookup on the resolved id — the family's own PK — rather
        // than on `(schema_id, name)`, which would need a SCHEMA_TAB probe first.
        let desc = self.resolve(&schema_name, &current_name)?.ok_or_else(missing)?;

        let family = if desc.class.is_view() { VIEW_TAB } else { TABLE_TAB };
        let name_pi = sys_schema(family).payload_idx(RELTAB_COL_NAME);
        self.rewrite_sys_row(family, desc.tid as u128, missing, |b, row| {
            b.set_string_cell(row, name_pi, &new_name)
        })?;
        if desc.class.is_view() {
            // The id, the layout and the rows are unchanged, so the copy is
            // renamed rather than destroyed. Both probes resolve before either
            // arm runs, because the arms need `&mut self`.
            let (claimed, held) = self.mirror.as_deref().map_or((false, false), |m| {
                (m.views.contains_key(&desc.tid), m.store.cursor_of(desc.tid).is_some())
            });
            if claimed {
                // Post-commit, so this must not fail the rename. The only way it
                // refuses is a poisoned store, which then refuses every read of
                // the copy too and drops this binding with itself at
                // `close_mirror` — so the stale name it leaves cannot be acted on.
                let _ = self.bind(&schema_name, &new_name, Arc::clone(&desc));
            } else if held {
                // A previous session's copy, replayed by the store's open and
                // never claimed here. Binding it would mirror a view the host did
                // not ask for; leaving it would leave the store's record naming
                // the freed name, for a later registration's scan to match.
                self.invalidate_own_copy(desc.tid);
            }
        }
        Ok(())
    }

    pub fn alter_rename_column(&mut self, tid: u64, col_idx: usize, new_col: &str) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| b.set_string_cell(row, COLTAB_PAY_NAME, new_col))
    }

    /// `ALTER TABLE … DROP COLUMN`: the column stays physically present, so the
    /// table keeps its layout and comparator.
    pub fn alter_drop_column(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| b.set_u64_cell(row, COLTAB_PAY_IS_HIDDEN, 1))
    }

    /// `ALTER TABLE … ALTER COLUMN … DROP NOT NULL`: a `(-1, +1)` COL_TAB rewrite
    /// pair on the same packed column id, only `is_nullable` flipped to true at
    /// `+1`. Once the catalog reports the column nullable, `ZSetBatch::validate`
    /// permits a null bit there and the engine swaps the table comparator
    /// `FixedIntNonnull → Generic` (if the table was all-non-null-fixed-int).
    pub fn alter_drop_not_null(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| b.set_u64_cell(row, COLTAB_PAY_IS_NULLABLE, 1))
    }

    /// `ALTER TABLE … ADD COLUMN`: `def` appended after every physical column,
    /// dropped ones included.
    pub fn alter_add_column(&mut self, tid: u64, def: &ColumnDef) -> Result<(), ClientError> {
        let col_idx = self.describe_by_id(tid)?.schema.num_columns();

        let col_s = sys_schema(COL_TAB);
        let mut cb = ZSetBatch::new(col_s);
        {
            let mut a = BatchAppender::new(&mut cb, col_s);
            gnitz_wire::sys_rows::write_col_tab_row(&mut a, &def.col_tab_row(tid, OWNER_KIND_TABLE, col_idx), 1);
        }
        self.push_ddl_txn(&[(COL_TAB, cb)])?;
        Ok(())
    }

    /// [`Self::rewrite_sys_row`] on column `col_idx` of relation `tid`.
    fn alter_col_pair(
        &mut self,
        tid: u64,
        col_idx: usize,
        patch: impl FnOnce(&mut ZSetBatch, usize),
    ) -> Result<(), ClientError> {
        self.rewrite_sys_row(
            COL_TAB,
            tid as u128 | (col_idx as u128) << 64,
            || ClientError::ServerError(format!("column index {col_idx} not found on table {tid}")),
            patch,
        )
    }

    /// Push a `(-1, +1)` rewrite pair on the live `family` row keyed `key`: the
    /// stored row at `-1`, and a copy of it at `+1` that `patch` edits in place.
    fn rewrite_sys_row(
        &mut self,
        family: u64,
        key: u128,
        missing: impl FnOnce() -> ClientError,
        patch: impl FnOnce(&mut ZSetBatch, usize),
    ) -> Result<(), ClientError> {
        let (scanned, i) = self.seek_sys_row(family, key, missing)?;
        let mut b = ZSetBatch::new(sys_schema(family));
        b.copy_row_at(&scanned, i, -1);
        b.copy_row_at(&scanned, i, 1);
        patch(&mut b, 1);
        self.push_ddl_txn(&[(family, b)])
    }

    /// Resolve `table_name` under `schema_name` to its id and schema. Anything that
    /// is not a base table is reported as absent — callers that need to tell the
    /// classes apart use [`Self::resolve_relation`], which returns the class.
    pub fn resolve_table_id(&mut self, schema_name: &str, table_name: &str) -> Result<(u64, Arc<Schema>), ClientError> {
        let d = self.resolve(schema_name, table_name)?;
        match d.filter(|d| d.class == RelClass::Table) {
            Some(d) => Ok((d.tid, Arc::clone(&d.schema))),
            None => Err(not_found("table", schema_name, table_name)),
        }
    }

    /// Resolve `name` under `schema_name`, rejecting a missing relation. The
    /// erroring form of [`Self::resolve`], for the callers whose next step needs
    /// the relation to exist.
    pub fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(schema_name, name)?
            .ok_or_else(|| not_found("table or view", schema_name, name))
    }

    pub fn resolve_table_or_view_id(
        &mut self,
        schema_name: &str,
        name: &str,
    ) -> Result<(u64, Arc<Schema>), ClientError> {
        let d = self.resolve_relation(schema_name, name)?;
        Ok((d.tid, Arc::clone(&d.schema)))
    }

    // --- Relation resolution ---

    /// The statement's descriptor for `schema_name.name`, or `None` when no such
    /// relation exists. Every relation lookup routes through here: the binder's
    /// writable-target check, ALTER's target resolution, the planner's resolve
    /// loop. One round trip per relation per statement — the scope holds the
    /// absent verdict too, so a two-probe error ladder does not pay twice. `Err`
    /// is a missing schema or a decode error, not a miss.
    pub fn resolve(&mut self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        // Built once and reused as the memo key, the wire target and the memo
        // write.
        let qname = qualified_name(schema_name, name);
        if let Some(hit) = self.catalog().get_qname(&qname) {
            return Ok(hit);
        }
        let found = self
            .round_trip(Request::Resolve(RelTarget::Name(&qname)))
            .map(Reply::into_resolve)?;
        self.record_relation(qname, found.clone());
        Ok(found)
    }

    // --- Private catalog-lookup helpers ---

    /// Resolve `schema_name` (already canonicalized) to its SCHEMA_TAB id. A
    /// missing row — or an entirely empty SCHEMA_TAB — is a
    /// [`ClientError::NotFound`], like every other catalog absence.
    pub(crate) fn lookup_schema_id(&mut self, schema_name: &str) -> Result<u64, ClientError> {
        let batch = checked_sys_rows(SCHEMA_TAB, self.scan(SCHEMA_TAB)?)?;
        find_schema_id(&batch, schema_name)?.ok_or_else(|| ClientError::NotFound {
            noun: "schema",
            name: schema_name.to_string(),
        })
    }

    /// The live system-catalog row with PK `key`, by one master-local SEEK: the
    /// reply batch and the row's index in it, for a caller to copy the stored row
    /// out of. No live row is `missing()`.
    fn seek_sys_row(
        &mut self,
        family: u64,
        key: u128,
        missing: impl FnOnce() -> ClientError,
    ) -> Result<(ZSetBatch, usize), ClientError> {
        let reply = checked_sys_rows(family, self.seek(family, &key.to_le_bytes())?)?;
        let i = reply.live_row_with_pk(sys_schema(family), key).ok_or_else(missing)?;
        Ok((reply, i))
    }
}

/// Wait for the session's interest, running `hook` on every `EINTR`.
pub(crate) fn park(session: &Session, hook: &mut Option<ParkHook>) -> Result<Interest, ClientError> {
    let interest = session.interest();
    if interest.is_empty() {
        return Err(ClientError::Closed);
    }
    loop {
        match poll_fd(session.as_raw_fd(), interest.poll_events(), None, false) {
            Ok(revents) => return Ok(Interest::from_revents(revents)),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                if let Some(hook) = hook.as_mut() {
                    hook()?;
                }
            }
            Err(e) => return Err(ClientError::Protocol(ProtocolError::IoError(e))),
        }
    }
}

// --- TxnBuffer: the locally-buffered atomic write-batch transaction ---

/// One buffered family: a maximal run of same-mode ops on one relation.
struct BufferedFamily {
    tid: u64,
    schema: Schema,
    batch: ZSetBatch,
    mode: WireConflictMode,
    /// How many of `batch`'s rows are already folded into `last_op_of`. A batch
    /// only ever extends, so this is a watermark, not a dirty flag.
    indexed: usize,
}

/// The write side of an open transaction. `push` — its one write entry point —
/// appends to the target
/// tid's **last** family when its conflict mode matches, else opens a new family
/// (run-splitting), so per-table op order is preserved end to end: buffer call
/// order = family frame order = the engine's validation-fold and
/// worker-application order. Cross-tid interleaving is unconstrained (FK
/// validation is post-transaction, order-free).
///
/// It also indexes buffered rows by PK (`last_op_of`) so the SQL overlay's
/// read-your-own-writes lookups are O(1) point reads rather than a re-fold of
/// the whole buffer per statement. The index is built **on read**, from a
/// per-family watermark: each row is folded in at most once across the whole
/// transaction, and a transaction that never reads its own writes — a blind bulk
/// INSERT — builds none of it.
///
/// Owned by [`GnitzClient::txn`]; every client write path routes into it while
/// it is open, and `txn_commit` ships it as one `PUSH_TXN` frame. Dropping
/// it is rollback — nothing was ever sent.
#[derive(Default)]
pub struct TxnBuffer {
    /// Creation order; per tid, maximal same-mode runs of the caller's op
    /// sequence.
    families: Vec<BufferedFamily>,

    /// tid → its family indices in creation order. Only the last one can still
    /// grow (a matching-mode append extends it), which is what makes indexing a
    /// tid's families in this order the same as indexing in append order.
    families_of: HashMap<u64, Vec<usize>>,
    /// tid → PK → `(family index, row index)` of the LAST op buffered on that
    /// PK. Row indices are stable: `push` only ever extends a family batch or
    /// pushes a new one. Weight-0 rows are not indexed — they are inert, exactly
    /// as the engine's fold treats them.
    last_op_of: HashMap<u64, HashMap<PkBuf, (usize, usize)>>,
    /// The transaction-wide OCC basis, snapshotted from `last_seen_lsn` at BEGIN.
    /// `COMMIT` ships one precondition `(tid, basis)` per read-set tid.
    basis: u64,
    /// The tids whose rows a buffered statement read before writing (UPDATE /
    /// DELETE / INSERT ... ON CONFLICT that resolved `count > 0`). Deduped, and a
    /// subset of the buffered-write (family) tids by construction — see
    /// `record_read`. SELECT-only and blind-INSERT tids are deliberately absent.
    read_set: Vec<u64>,
}

impl TxnBuffer {
    /// Record `tid` in the read-set (deduped). Called only when an RMW statement
    /// buffered a write for `tid`, so the read-set stays a subset of the family
    /// tids. The linear `contains` is cheap — the read-set holds one entry per
    /// distinct table the transaction RMW'd, not per statement.
    fn record_read(&mut self, tid: u64) {
        if !self.read_set.contains(&tid) {
            self.read_set.push(tid);
        }
    }

    /// The buffer's one write entry point: append `batch` to `tid`'s current run,
    /// or open a new family when the mode differs (or `tid` has no family yet).
    /// Empty batches contribute nothing and open no family.
    ///
    /// `Error` mode rejects the whole transaction if any of these rows' PKs
    /// already exist, checked cumulatively in frame order against committed state
    /// and earlier families. A delete is a batch of `-1` rows in `Update` mode, so
    /// "delete k; insert k" emits an Update family `[D(k)]` then an Error family
    /// `[I(k)]`, in that order.
    pub fn push(&mut self, tid: u64, schema: &Schema, batch: ZSetBatch, mode: WireConflictMode) {
        if batch.is_empty() {
            return;
        }
        // Copied out, so no borrow of `families_of` spans the `families` read.
        let last = self.families_of.get(&tid).and_then(|v| v.last().copied());
        match last.filter(|&i| self.families[i].mode == mode) {
            Some(i) => {
                self.families[i].batch.extend_from_owned(batch);
            }
            None => {
                self.families_of.entry(tid).or_default().push(self.families.len());
                self.families.push(BufferedFamily {
                    tid,
                    schema: schema.clone(),
                    batch,
                    mode,
                    indexed: 0,
                });
            }
        }
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
            let f = &mut self.families[fam];
            for row in f.indexed..f.batch.len() {
                if f.batch.weights[row] != 0 {
                    index.insert(f.batch.pks.get_tuple(row), (fam, row));
                }
            }
            f.indexed = f.batch.len();
        }
    }

    /// How many buffered rows are currently folded into the read index — `0`
    /// for a transaction that has never read its own writes.
    #[cfg(test)]
    pub(crate) fn indexed_rows(&self) -> usize {
        self.families.iter().map(|f| f.indexed).sum()
    }

    /// This buffer's ops on `tid`, with that relation's index caught up first.
    pub fn reads(&mut self, tid: u64) -> TxnReads<'_> {
        self.index_tid(tid);
        TxnReads { buf: self, tid }
    }
}

/// The buffered ops on **one** relation — the only way to reach them, and the
/// only way to construct it runs the index catch-up. So a read against a stale
/// index, or against a relation the caller did not catch up, cannot be written.
pub struct TxnReads<'a> {
    buf: &'a TxnBuffer,
    tid: u64,
}

impl<'a> TxnReads<'a> {
    /// The last op buffered on `pk`, as `(batch, row)` — or `None` if the
    /// transaction has not touched that PK. The row's **weight sign** is the net
    /// effect (positive: live row with that payload; negative: deleted),
    /// mirroring the engine's own per-table fold.
    pub fn last_op(&self, pk: &[u8]) -> Option<(&'a ZSetBatch, usize)> {
        let &(fam, row) = self.buf.last_op_of.get(&self.tid)?.get(pk)?;
        Some((&self.buf.families[fam].batch, row))
    }

    /// Every PK the transaction has touched, with its last op.
    pub fn last_ops(&self) -> impl Iterator<Item = (PkBuf, &'a ZSetBatch, usize)> + '_ {
        let buf = self.buf;
        buf.last_op_of
            .get(&self.tid)
            .into_iter()
            .flatten()
            .map(move |(pk, &(fam, row))| (*pk, &buf.families[fam].batch, row))
    }
}

/// `Ok(None)` = name absent (a legitimate miss); `Err` = a decode error on a
/// corrupt catalog batch.
fn find_schema_id(batch: &ZSetBatch, name: &str) -> Result<Option<u64>, ClientError> {
    for i in batch.live_rows() {
        if col_str(batch, sys_schema(SCHEMA_TAB), SCHEMATAB_COL_NAME, i)? == name {
            return Ok(Some(batch.pks.get(sys_schema(SCHEMA_TAB), i) as u64));
        }
    }
    Ok(None)
}

/// A system family's rows, with the reply's schema checked against
/// `sys_schema(family)`. The row readers above index a reply positionally, and a
/// reply is decoded against the server's block or this connection's cache — never
/// against `sys_schema` — so the check is what makes that indexing a precondition
/// rather than a cross-crate assumption.
fn checked_sys_rows(family: u64, reply: ScanReply) -> Result<ZSetBatch, ClientError> {
    if !reply.schema.types_match(sys_schema(family)) {
        return Err(ClientError::ServerError(format!(
            "system family {family} answered in a schema that is not its own"
        )));
    }
    Ok(reply.batch)
}

/// Append one `COL_TAB` row per column of `owner_id`, at `+1`.
fn append_col_rows(a: &mut BatchAppender<'_>, owner_id: u64, owner_kind: u64, columns: &[ColumnDef]) {
    for (i, cd) in columns.iter().enumerate() {
        gnitz_wire::sys_rows::write_col_tab_row(a, &cd.col_tab_row(owner_id, owner_kind, i), 1);
    }
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;
