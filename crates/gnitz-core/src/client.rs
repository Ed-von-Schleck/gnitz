use crate::connection::{
    MultiScanResult, RawBlock, RelTarget, ScanResult, Session, COL_TAB, IDX_TAB, SCHEMA_TAB, TABLE_TAB, VIEW_TAB,
};
use crate::error::ClientError;
use crate::protocol::{
    BatchAppender, ColData, ColumnDef, PkColumn, PkTuple, Schema, TypeCode, WireConflictMode, ZSetBatch,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::circuit::{is_segment_id, substitute_seg_id, Circuit};
use crate::types::sys_schema;
use gnitz_wire::sys_rows::{IdxTabRow, TableTabRow, ViewTabRow};
use gnitz_wire::{
    RelClass, RelDescriptorBlob, TableProps, CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB,
    IDXTAB_COL_IS_UNIQUE, IDXTAB_COL_NAME, IDXTAB_COL_OWNER_ID, IDXTAB_COL_SOURCE_COLS, OWNER_KIND_TABLE,
    OWNER_KIND_VIEW, SCHEMATAB_COL_NAME, TABTAB_COL_FLAGS, TABTAB_COL_NAME, TABTAB_COL_PK_COL_IDX,
    TABTAB_COL_SCHEMA_ID, VIEWTAB_COL_CAPACITY, VIEWTAB_COL_DELTA, VIEWTAB_COL_NAME, VIEWTAB_COL_PK_COL_IDX,
    VIEWTAB_COL_SCHEMA_ID, VIEWTAB_COL_SQL,
};

// --- Module-private helpers ---

fn col_u64(col: &ColData, i: usize) -> Result<u64, ClientError> {
    let cell = col
        .cell(i, 8)
        .ok_or_else(|| ClientError::ServerError(format!("col_u64: no 8-byte cell at row {i}")))?;
    Ok(gnitz_wire::read_u64_le(cell, 0))
}

fn col_str(col: &ColData, i: usize) -> Result<Option<&str>, ClientError> {
    match col {
        ColData::Strings(v) => v
            .get(i)
            .map(Option::as_deref)
            .ok_or_else(|| ClientError::ServerError(format!("col_str: row {i} out of bounds (len {})", v.len()))),
        _ => Err(ClientError::ServerError("col_str: expected Strings column".into())),
    }
}

/// The canonical `"schema.relation"` key — the engine's `entity_by_qname` key
/// and the statement memo's. Built in one allocation, not three.
pub fn qualified_name(schema_name: &str, name: &str) -> String {
    let mut q = String::with_capacity(schema_name.len() + 1 + name.len());
    q.push_str(schema_name);
    q.push('.');
    q.push_str(name);
    q.make_ascii_lowercase();
    q
}

/// Build the `-1` retraction batch for `pks`: the server's `retract_pk` matches
/// by PK alone, so the payload columns are inert filler (built directly, not via
/// `BatchAppender`, whose `add_row` takes a single scalar PK). Shared by
/// `GnitzClient::delete`, `TxnBuffer::delete`, and the SQL layer's DELETE RMW
/// retry closure (which needs the batch without an immediate push).
pub fn retraction_batch(schema: &Schema, pks: PkColumn) -> ZSetBatch {
    let count = pks.len();
    ZSetBatch {
        pks,
        weights: vec![-1; count],
        nulls: vec![0; count],
        columns: ZSetBatch::filler_columns(schema, count),
    }
}

// --- GnitzClient ---

/// A secondary-index descriptor: the declared column list (the unique key — the
/// circuit list is deduped by column list) and the system's operative
/// uniqueness truth. This is the wire type verbatim, so a resolved descriptor's
/// index list moves into the client instead of being re-collected.
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
/// drawn from the master by `alloc_serial_range`. Cache-loss on disconnect
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

/// The name of the `idx`-th hidden segment view owned by the user view with id
/// `owner_vid`. Ownership is name-encoded: `drop_view` cascades over
/// [`hidden_view_prefix`], so producer (planner) and consumer (drop) must share
/// this one definition.
pub fn hidden_view_name(owner_vid: u64, idx: usize) -> String {
    format!("{}{idx}", hidden_view_prefix(owner_vid))
}

/// A bundle's view name in its canonical stored form, resolving a hidden
/// segment's owner through the bundle's symbolic-id substitution. A segment id
/// the substitution cannot resolve is an error rather than a name: `drop_view`
/// cascades by name prefix, so a segment named after a tag would be undroppable.
/// A hidden name is system-generated and skips the fold, which would reject its
/// leading `_`.
fn resolve_view_name(name: &ViewName, seg_ids: &HashMap<u64, u64>) -> Result<String, ClientError> {
    match name {
        ViewName::Named(n) => gnitz_wire::canonical_identifier(n).map_err(ClientError::ServerError),
        ViewName::Hidden { owner, idx } => Ok(hidden_view_name(substitute_seg_id(*owner, seg_ids)?, *idx)),
    }
}

/// Reject a COL_TAB write against anything but a user base table. Every such row
/// carries `owner_kind = OWNER_KIND_TABLE`, so against a stored view row a `-1`
/// would fail as an opaque CAS conflict and a `+1` as the engine's own owner-kind
/// rejection; naming the relation's kind here is the same verdict, readable. The
/// SQL layer rejects a view earlier — this is the backstop for the binary
/// front ends.
fn reject_non_base_table(desc: &RelDescriptor, op: &str) -> Result<(), ClientError> {
    if desc.class != RelClass::Table {
        return Err(ClientError::ServerError(format!(
            "relation {} is a {}; {op} requires a base table",
            desc.tid,
            desc.class.noun()
        )));
    }
    Ok(())
}

/// Whether a `PlannedView`'s declared `circuit.view_id` needs an id minted for
/// it: `0` is "mint one", a [`segment_id`](crate::segment_id) is "mint one and
/// substitute it through the bundle", anything else is an id the caller holds.
/// The one spelling, so the run `create_view_chain` reserves and the loop that
/// draws from it cannot count differently.
fn needs_fresh_vid(declared: u64) -> bool {
    declared == 0 || is_segment_id(declared)
}

/// The name prefix every hidden segment of `owner_vid` carries. The trailing
/// separator keeps `__h5_` from matching `__h51_0`.
fn hidden_view_prefix(owner_vid: u64) -> String {
    format!("{}{owner_vid}_", gnitz_wire::HIDDEN_VIEW_PREFIX)
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
    /// Split a terminal frame's watermark word: tag in the high half, round in
    /// the low half.
    fn from_watermark(w: u128) -> Self {
        let (tag, tick) = gnitz_wire::unpack_delta_watermark(w);
        DeltaCursor { tag, tick }
    }

    /// The tick a poll from this cursor reads after.
    ///
    /// Tick `0` is **refused**: it names no copy to protect and no tag to match,
    /// so it could only mean a bootstrap — and a bootstrap walks the view's own
    /// store and comes back in the view's schema, not the [`delta_reply_schema`]
    /// shape a poll takes.
    fn poll_after(self) -> Result<u64, ClientError> {
        (self.tick != 0).then_some(self.tick).ok_or(ClientError::DeltaExpired)
    }

    /// `next` as this cursor's successor, or [`ClientError::DeltaExpired`].
    ///
    /// A tag the server did not echo back names a different boot or a different
    /// relation, and the rows such a read draws are unsafe to apply: they are the
    /// *other* relation's recent deltas, and a recreated view's backfill never
    /// enters a delta store at all. The recovery is the one a cursor that fell out
    /// of the retention window gets — discard the copy and bootstrap.
    fn advanced_to(self, next: DeltaCursor) -> Result<DeltaCursor, ClientError> {
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
    for c in gnitz_wire::delta_schema_order(view.pk_indices(), view.num_columns()) {
        let (cd, is_key) = match c {
            gnitz_wire::DeltaCol::Tick => (ColumnDef::new("_tick", TypeCode::U64, false).hidden(), true),
            gnitz_wire::DeltaCol::Key(i) => (view.columns[i].clone(), true),
            gnitz_wire::DeltaCol::Payload(i) => (view.columns[i].clone(), false),
        };
        if is_key {
            pk_cols.push(columns.len());
        }
        columns.push(cd);
    }
    Ok(Schema { columns, pk_cols })
}

/// What a bundle's view is called. A hidden segment's name embeds its owner's
/// real id, which a planner minting symbolic ids does not have, so it names the
/// owner and [`GnitzClient::create_view_chain`] mints the string once that id is
/// assigned. Naming the owner also keeps the name off the segment's row position
/// in the bundle.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ViewName {
    Named(String),
    /// [`hidden_view_name(owner, idx)`](hidden_view_name), minted once `owner`'s
    /// real id exists.
    Hidden {
        owner: u64,
        idx: usize,
    },
}

impl std::fmt::Display for ViewName {
    /// The user-visible name, or what a hidden segment is. Never a `__h…` string
    /// built from a symbolic owner, which would name a view no catalog holds.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewName::Named(n) => f.write_str(n),
            ViewName::Hidden { idx, .. } => write!(f, "hidden segment {idx}"),
        }
    }
}

/// One view in a [`GnitzClient::create_view_chain`] bundle. The `circuit`'s
/// `view_id` is the view's own id: zero for one `create_view_chain` should
/// allocate, a [`segment_id`](crate::segment_id) for one it should allocate *and*
/// substitute through the whole bundle, else an id the caller already holds.
pub struct PlannedView {
    pub name: ViewName,
    pub sql_text: String,
    pub circuit: Circuit,
    pub output_columns: Vec<ColumnDef>,
    pub pk_cols: Vec<u32>,
    /// `WITH (capacity = …)` in bytes, set on the chain's **final** segment only;
    /// `None` is unbounded. Hidden chain segments never carry one.
    pub capacity_bytes: Option<u64>,
    /// `WITH (delta = …)` in bytes, set on the chain's **final** segment only —
    /// the one the client names and the only one whose store it can read; the
    /// hidden segments tick in the same round without carrying feeds of their own.
    /// `None` is no feed.
    pub delta_bytes: Option<u64>,
}

/// Everything a statement needs to know about one relation. Statement-scoped:
/// built by a resolve, dropped at `end_statement`, never carried across. That is
/// what makes a stale name → id binding unreachable rather than merely checked —
/// there is nothing retained to go stale, so nothing to invalidate.
#[derive(Debug)]
pub struct RelDescriptor {
    pub tid: u64,
    pub class: RelClass,
    pub replicated: bool,
    /// The view keeps a delta feed, so `ReadBound::Delta` against it is answerable.
    pub delta: bool,
    pub schema: Arc<Schema>,
    pub indexes: Arc<Vec<IndexMeta>>,
}

impl RelDescriptor {
    /// The descriptor a RESOLVE reply describes, from the three values
    /// [`Reply::Resolve`](crate::Reply::Resolve) carries.
    pub fn from_resolve(tid: u64, schema: Arc<Schema>, blob: RelDescriptorBlob) -> RelDescriptor {
        RelDescriptor {
            tid,
            class: blob.class,
            replicated: blob.replicated,
            delta: blob.delta,
            schema,
            indexes: Arc::new(blob.indexes),
        }
    }
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
static EMPTY_CATALOG: CatalogSnapshot = CatalogSnapshot {
    relations: BTreeMap::new(),
};

impl CatalogSnapshot {
    /// This statement's verdict for `schema_name.name`: `None` if the statement
    /// has not resolved that name at all, `Some(None)` for a recorded absence.
    pub fn get(&self, schema_name: &str, name: &str) -> Option<Option<Arc<RelDescriptor>>> {
        self.relations.get(&qualified_name(schema_name, name)).cloned()
    }

    /// Record a verdict, replacing any entry already under the same key.
    pub fn insert(&mut self, schema_name: &str, name: &str, desc: Option<Arc<RelDescriptor>>) {
        self.relations.insert(qualified_name(schema_name, name), desc);
    }

    /// The descriptor this statement resolved for `tid`. A linear scan with no
    /// wire fallback; a statement resolves a handful of relations.
    pub fn by_tid(&self, tid: u64) -> Option<Arc<RelDescriptor>> {
        self.relations.values().flatten().find(|d| d.tid == tid).map(Arc::clone)
    }
}

pub struct GnitzClient {
    session: Session,
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
    /// rejected inside a transaction at the client's `push_ddl` choke point (and,
    /// earlier and friendlier, by the SQL front end).
    txn: Option<TxnBuffer>,
    /// The client's OCC basis: the running maximum over server-issued watermarks
    /// the connection has observed — seeded from the HELLO ACK at connect, then
    /// advanced by every push / `txn_commit` ACK and every `STATUS_TXN_CONFLICT`
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
    pub fn connect(socket_path: &str) -> Result<Self, ClientError> {
        // Seed the OCC basis from the HELLO ACK watermark. A restart yields a
        // fresh GnitzClient re-seeded from the new ACK, so a basis never spans it.
        let (session, last_seen_lsn) = Session::connect(socket_path)?;
        Ok(GnitzClient {
            session,
            serial_cache: HashMap::new(),
            scope: None,
            txn: None,
            last_seen_lsn,
            mirror: None,
        })
    }

    /// Request frames this connection has written. Exposed for the
    /// round-trip-count assertions; see [`Session::requests_sent`].
    pub fn requests_sent(&self) -> u64 {
        self.session.requests_sent()
    }

    /// Install (or clear) the hook the blocking client runs before each park
    /// on the fd; its `Err` aborts the operation in progress and leaves the
    /// connection usable for the next call. The client holds no hook of its
    /// own — the parking code lives in the session.
    pub fn set_park_hook(&mut self, hook: Option<crate::connection::ParkHook>) {
        self.session.set_park_hook(hook);
    }

    /// The client's current OCC basis (running max of observed watermarks).
    /// Exposed read-only for tests and the SQL layer's autocommit/BEGIN basis.
    pub fn last_seen_lsn(&self) -> u64 {
        self.last_seen_lsn
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
    fn record_relation(&mut self, schema_name: &str, name: &str, desc: Option<Arc<RelDescriptor>>) {
        if let Some(scope) = &mut self.scope {
            scope.insert(schema_name, name, desc);
        }
    }

    /// Reserve at least `count` SERIAL ids for `table_id` in **one** master round
    /// trip, so a multi-row INSERT that knows its row count up front pays one
    /// fsynced durable advance instead of `ceil(count / SERIAL_RANGE_SIZE)` of
    /// them. Any tail left in the previous range is abandoned — the same
    /// intentional, PostgreSQL-style gap a refill already leaves.
    pub fn reserve_serial_ids(&mut self, table_id: u64, count: u64) -> Result<(), ClientError> {
        let held = self
            .serial_cache
            .get(&table_id)
            .map_or(0, |r| r.end.saturating_sub(r.next));
        if held >= count {
            return Ok(());
        }
        let want = count.max(SERIAL_RANGE_SIZE);
        let base = self.session.alloc_serial_range(table_id, want)?;
        self.serial_cache.insert(
            table_id,
            SerialRange {
                next: base,
                end: base + want,
            },
        );
        Ok(())
    }

    /// Draw the next SERIAL id for `table_id` from the per-connection range
    /// cache, refilling from the master's durable sequence when the range is
    /// exhausted. Ids are contiguous within a range; a refill may leave a gap
    /// if a prior range's tail was never issued (intentional, PostgreSQL-style).
    pub fn next_serial_id(&mut self, table_id: u64) -> Result<u64, ClientError> {
        if let Some(r) = self.serial_cache.get_mut(&table_id) {
            if r.next < r.end {
                let id = r.next;
                r.next += 1;
                return Ok(id);
            }
        }
        let base = self.session.alloc_serial_range(table_id, SERIAL_RANGE_SIZE)?;
        self.serial_cache.insert(
            table_id,
            SerialRange {
                next: base + 1,
                end: base + SERIAL_RANGE_SIZE,
            },
        );
        Ok(base)
    }

    // --- Raw ops ---

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.session.alloc_table_id()
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
        let r = self.session.push_with_mode(table_id, schema, batch, mode);
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
        let r = self.session.push_with_mode(table_id, schema, &batch, mode);
        self.track_lsn(r)
    }

    /// Autocommit read-modify-write commit: ship a one-family, one-precondition
    /// `FLAG_PUSH_TXN` frame asserting `table_id` has not been written since
    /// `basis`. Returns the zone LSN on success; a `ClientError::TxnConflict`
    /// (whose `fresh_basis` the caller adopts and re-reads with) means the table
    /// was written since `basis`. Autocommit only — the SQL driver calls this
    /// only when no transaction is open. Both outcomes advance `last_seen_lsn`.
    pub fn commit_rmw(
        &mut self,
        table_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
        basis: u64,
    ) -> Result<u64, ClientError> {
        let r = self
            .session
            .push_txn(&[(table_id, schema, batch, mode)], &[(table_id, basis)]);
        self.track_lsn(r)
    }

    /// Buffer an RMW write into the open transaction AND record `table_id` in the
    /// transaction's read-set, so `COMMIT` ships an OCC precondition for it. Used
    /// by UPDATE / DELETE / INSERT ... ON CONFLICT, which read `table_id` before
    /// writing. A blind INSERT uses plain `push_with_mode` and records nothing (a
    /// blind write cannot lose an update). No-op outside a transaction.
    pub fn txn_push_rmw(&mut self, table_id: u64, schema: &Schema, batch: ZSetBatch, mode: WireConflictMode) {
        if let Some(txn) = &mut self.txn {
            txn.push(table_id, schema, batch, mode);
            txn.record_read(table_id);
        }
    }

    pub fn scan(&mut self, table_id: u64) -> ScanResult {
        self.session.scan(table_id)
    }

    /// Run a parameterized bounded read (`ReadSpec`) — the ad-hoc SELECT access
    /// path. `spec` is the encoded `ReadSpec`; `reply_schema` is the projected
    /// reply schema, shipped with the request and used to decode every reply
    /// frame (the server sends none back). Returns one batch; the SQL layer
    /// applies the client-side ORDER BY / LIMIT window. Bypasses the schema
    /// cache, so it never poisons a later plain `scan` of the same table.
    pub fn scan_spec(
        &mut self,
        table_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<Option<ZSetBatch>, ClientError> {
        // An ad-hoc SELECT holds no cursor across reads, so the terminal
        // watermark is dropped here rather than pushed through every caller.
        self.session.scan_spec(table_id, spec, reply_schema).map(|(b, _)| b)
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
    /// re-resolves before it reseeds. Answered off the registration rather than
    /// the readability gate, so a name whose copy is not valid still binds
    /// locally and only the read it feeds goes upstream.
    pub fn resolve_local_first(
        &mut self,
        schema_name: &str,
        name: &str,
    ) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let local = self.mirror.as_deref().and_then(|m| {
            m.by_qname
                .get(&qualified_name(schema_name, name))
                .and_then(|tid| m.views.get(tid))
                .map(|v| Arc::clone(&v.desc))
        });
        match local {
            Some(desc) => {
                self.record_relation(schema_name, name, Some(Arc::clone(&desc)));
                Ok(Some(desc))
            }
            None => self.resolve(schema_name, name),
        }
    }

    /// Every row of `table_id` off the copy, or `None` when the copy does not
    /// hold it and the read is the caller's to delegate.
    ///
    /// **The store answers whether it holds the relation**, so nothing outside it
    /// spells that gate a second time. For a caller that delegates on its own
    /// terms — an async handle, which delegates on its own connection rather than
    /// this one; [`Self::scan_local_first`] delegates here.
    pub fn scan_local(&mut self, table_id: u64) -> Result<Option<(Arc<Schema>, ZSetBatch)>, ClientError> {
        match self.mirror.as_deref_mut() {
            Some(m) => Ok(m.store.scan(table_id)?),
            None => Ok(None),
        }
    }

    /// [`Self::scan`], answered off the copy when it holds `table_id`.
    ///
    /// The served LSN is `None` for a local answer: it is a server-side counter,
    /// and a copy's freshness is a feed round — [`Self::cursor_of`] is where a
    /// host reads it.
    pub fn scan_local_first(&mut self, table_id: u64) -> Result<crate::connection::LocalScanReply, ClientError> {
        if let Some((schema, batch)) = self.scan_local(table_id)? {
            return Ok((Some(schema), Some(batch), None));
        }
        let (schema, data, lsn) = self.scan(table_id)?;
        Ok((schema, data, Some(lsn)))
    }

    /// [`Self::scan_spec`], answered off the copy when it holds `table_id`.
    ///
    /// A held answer is the answer, empty or not; only "not held" falls through.
    pub fn scan_spec_local_first(
        &mut self,
        table_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<Option<ZSetBatch>, ClientError> {
        if let Some(m) = self.mirror.as_deref_mut() {
            if let crate::mirror::StoreRead::Held(batch) = m.store.scan_spec(table_id, spec, reply_schema)? {
                return Ok(batch);
            }
        }
        self.scan_spec(table_id, spec, reply_schema)
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
    /// A copy rides along and is repointed by `rebind_to_new_server`, whose doc
    /// carries that rule. A poisoned store crosses unchanged, because poison is a
    /// statement about the copy and not about the connection.
    pub fn reconnect(&mut self, target: &str) -> Result<(), ClientError> {
        if self.txn_active() {
            return Err(ClientError::ServerError(
                "reconnect inside a transaction; commit or roll back first".to_string(),
            ));
        }
        let mut fresh = GnitzClient::connect(target)?;
        // The hook lives on the session a reconnect replaces, and it is what
        // keeps a blocking call Ctrl-C-interruptible.
        fresh.session.set_park_hook(self.session.take_park_hook());
        fresh.mirror = self.mirror.take();
        if let Some(m) = fresh.mirror.as_deref_mut() {
            m.rebind_to_new_server();
        }
        *self = fresh;
        Ok(())
    }

    /// Bootstrap a view's delta feed: the view's whole current value, in the
    /// view's own schema, together with the cursor to poll from.
    ///
    /// `Delta { after_tick: 0 }` is *the sum of every delta after round 0* —
    /// the view's entire history, which is precisely what its output store
    /// holds — so this costs exactly what a scan of the view costs, because it
    /// is one. That is the price of joining, and it is paid once per subscriber
    /// rather than once per poll.
    ///
    /// The reply carries true net weights: a bag-valued view's weight-3 row
    /// arrives as weight 3, not as a presence bit. Apply it to a fresh copy —
    /// this replaces state, it does not add to it.
    pub fn delta_bootstrap(
        &mut self,
        view_id: u64,
        view_schema: &Schema,
    ) -> Result<(Option<ZSetBatch>, DeltaCursor), ClientError> {
        self.delta_read(view_id, 0, view_schema)
    }

    /// Poll a view's delta feed: every delta it emitted in `(cursor.tick, T]`,
    /// in [`delta_reply_schema`]'s shape, with the cursor to poll from next.
    /// Apply what comes back and store the new cursor; there is nothing to
    /// filter and nothing to reconcile.
    ///
    /// Both refusals a poll can answer with — a cursor at tick `0`, and a reply
    /// whose tag does not continue the cursor — are
    /// [`DeltaCursor::poll_after`] and [`DeltaCursor::advanced_to`], which state
    /// the rule once for this call and for
    /// [`delta_poll_raw`](Self::delta_poll_raw). Both surface as
    /// [`ClientError::DeltaExpired`], whose recovery is to discard the copy and
    /// [`delta_bootstrap`](Self::delta_bootstrap) again.
    ///
    /// A poll does **not** drive a tick: a delta read answers "what has
    /// happened", not "what is current", so a push the tick loop has not run yet
    /// is a round the next poll will carry.
    pub fn delta_poll(
        &mut self,
        view_id: u64,
        cursor: DeltaCursor,
        reply_schema: &Schema,
    ) -> Result<(Option<ZSetBatch>, DeltaCursor), ClientError> {
        let (data, next) = self.delta_read(view_id, cursor.poll_after()?, reply_schema)?;
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
        view_schema: &Schema,
    ) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        self.delta_read_raw(view_id, 0, view_schema)
    }

    /// [`Self::delta_poll`] handing back the reply's *undecoded* blocks, under
    /// the same two cursor rules.
    pub(crate) fn delta_poll_raw(
        &mut self,
        view_id: u64,
        cursor: DeltaCursor,
        reply_schema: &Schema,
    ) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        let (blocks, next) = self.delta_read_raw(view_id, cursor.poll_after()?, reply_schema)?;
        Ok((blocks, cursor.advanced_to(next)?))
    }

    /// The one request every delta call makes: a `ScanSpec` with a
    /// `ReadBound::Delta` and an identity sink.
    fn delta_spec(after_tick: u64) -> Vec<u8> {
        gnitz_wire::ReadSpec::encode_parts(
            &gnitz_wire::ReadBound::Delta { after_tick },
            &[],
            &gnitz_wire::ReadSink::all_rows(),
        )
    }

    /// [`Self::delta_spec`] shipped, returning the rows and the terminal frame's
    /// `(tag, T)` pair.
    fn delta_read(
        &mut self,
        view_id: u64,
        after_tick: u64,
        reply_schema: &Schema,
    ) -> Result<(Option<ZSetBatch>, DeltaCursor), ClientError> {
        let spec = Self::delta_spec(after_tick);
        let (data, watermark) = self.session.scan_spec(view_id, &spec, reply_schema)?;
        Ok((data, DeltaCursor::from_watermark(watermark)))
    }

    /// [`Self::delta_read`] keeping the reply's raw blocks.
    fn delta_read_raw(
        &mut self,
        view_id: u64,
        after_tick: u64,
        reply_schema: &Schema,
    ) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        let spec = Self::delta_spec(after_tick);
        let (blocks, watermark) = self.session.scan_spec_raw(view_id, &spec, reply_schema)?;
        Ok((blocks, DeltaCursor::from_watermark(watermark)))
    }

    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table `push_txn` is never observed torn
    /// across the result set. Like `scan`, it leaves `last_seen_lsn` untouched.
    pub fn scan_many(&mut self, table_ids: &[u64]) -> MultiScanResult {
        self.session.scan_multi(table_ids)
    }

    pub fn seek(&mut self, table_id: u64, pk: &PkTuple) -> ScanResult {
        self.session.seek(table_id, pk)
    }

    /// Seek a secondary index by `col_indices` (the index's FULL declared column
    /// list — the server matches the circuit by exact list) supplying `key_vals`
    /// native key values. `key_vals.len()` may be `< col_indices.len()` for a
    /// leading-prefix seek. The two arity guards below prevent a `pack_pk_cols`
    /// panic and a silently-misread frame (the worker derives the value count from
    /// the wire byte length). The SQL planner does not come through here — it
    /// ships a `ReadBound::IndexRange` through `scan_spec`.
    pub fn seek_by_index(&mut self, table_id: u64, col_indices: &[u32], key_vals: &[u128]) -> ScanResult {
        // pack_pk_cols asserts its contract — reject here, never panic.
        gnitz_wire::validate_pk_col_list(col_indices)
            .map_err(|e| ClientError::ServerError(format!("seek_by_index: {e}")))?;
        // K rides as the wire byte count (K = 1 + seek_pk_extra.len()/16 ≥ 1), so
        // an empty key_vals would be misread by the worker as one value `0`. More
        // values than columns is rejected by the worker too; fail it here for a
        // clean local error.
        if key_vals.is_empty() || key_vals.len() > col_indices.len() {
            return Err(ClientError::ServerError(format!(
                "seek_by_index: key value count {} must be in 1..={}",
                key_vals.len(),
                col_indices.len()
            )));
        }
        self.session.seek_by_index(table_id, col_indices, key_vals)
    }

    /// The secondary-index descriptor for `col_idx` of `table_id`, read off the
    /// statement's resolved descriptor. The column list is unique per entry (the
    /// server dedups circuits by list), so a plain `find` is exact.
    ///
    /// The reported uniqueness matches the server's authoritative pre-create FK
    /// gate `validate_fk_column` exactly — both read `is_unique` off the same
    /// `index_circuits` — so this check can never accept an FK the server would
    /// reject, nor reject one it would accept.
    pub fn index_for_column(&mut self, table_id: u64, col_idx: usize) -> Result<Option<IndexMeta>, ClientError> {
        let list = Arc::clone(&self.describe_by_id(table_id)?.indexes);
        // Exact single-element match: a composite index does NOT answer a
        // single-column FK/uniqueness query (a `(a, b)` index does not guarantee
        // uniqueness of `a` alone).
        Ok(list.iter().find(|m| m.cols.as_slice() == [col_idx as u32]).copied())
    }

    /// The statement's descriptor for `tid` — the by-id twin of [`Self::resolve`].
    /// A tid usually comes from resolving the same relation by name earlier in the
    /// statement, which makes this a scope hit; a tid from anywhere else falls back
    /// to a by-id round trip rather than reporting an empty index list.
    pub fn describe_by_id(&mut self, tid: u64) -> Result<Arc<RelDescriptor>, ClientError> {
        if let Some(d) = self.catalog().by_tid(tid) {
            return Ok(d);
        }
        self.fetch_descriptor(RelTarget::Id(tid))?
            .ok_or_else(|| ClientError::ServerError(format!("relation {tid} not found")))
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
        let index_name = gnitz_wire::canonical_identifier(index_name).map_err(ClientError::ServerError)?;
        // Arity, 7-bit column range, duplicates — the Err form of the
        // pack_pk_cols contract, so the pack below can never panic.
        gnitz_wire::validate_pk_col_list(col_indices)
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
            gnitz_wire::index_key_type(ct as u8).map_err(ClientError::ServerError)?;
        }

        // No client-side name probe: the engine rejects a duplicate against both
        // the persisted `index_by_name` and the rest of this bundle. A rejected
        // bundle burns this index_id, which costs nothing — ids are never reused.
        let index_id = self.session.alloc_index_id()?;

        let idx_schema = sys_schema(IDX_TAB);
        let mut batch = ZSetBatch::new(idx_schema);
        gnitz_wire::sys_rows::write_idx_tab_row(
            &mut BatchAppender::new(&mut batch, idx_schema),
            &IdxTabRow {
                index_id,
                owner_id: table_id,
                source_col_idx: gnitz_wire::pack_pk_cols(col_indices),
                name: &index_name,
                is_unique: is_unique as u64,
            },
            1,
        );

        self.push_ddl(&[(IDX_TAB, batch)])?;
        Ok(index_id)
    }

    /// Drop an index by name. `if_exists` swallows a missing index (returns
    /// `Ok(())`) — honored at the primitive's own not-found path, NOT via a
    /// client-side existence pre-check, which would be a TOCTOU (a concurrent
    /// DROP landing in the gap resurfaces the very "not found" `IF EXISTS` must
    /// suppress). `ALTER TABLE ... DROP CONSTRAINT IF EXISTS` sets it; the plain
    /// `DROP INDEX` statement passes `false` (drops loudly, like DROP TABLE/VIEW).
    /// The swallow covers only the scan below finding no such index: a concurrent
    /// DROP landing between that scan and the push surfaces the engine's
    /// retraction-contract rejection, `if_exists` or not.
    pub fn drop_index_by_name(&mut self, index_name: &str, if_exists: bool) -> Result<(), ClientError> {
        let index_name = gnitz_wire::canonical_identifier(index_name).map_err(ClientError::ServerError)?;
        let not_found = || -> Result<(), ClientError> {
            if if_exists {
                Ok(())
            } else {
                Err(ClientError::ServerError(format!("index '{index_name}' not found")))
            }
        };
        let Some(idx_batch) = checked_sys_rows(IDX_TAB, self.session.scan(IDX_TAB)?)? else {
            return not_found();
        };
        for i in idx_batch.live_rows() {
            if col_str(&idx_batch.columns[IDXTAB_COL_NAME], i)? != Some(index_name.as_str()) {
                continue;
            }
            let idx_schema = sys_schema(IDX_TAB);
            let mut batch = ZSetBatch::new(idx_schema);
            gnitz_wire::sys_rows::write_idx_tab_row(
                &mut BatchAppender::new(&mut batch, idx_schema),
                &read_idx_tab_row(&idx_batch, i)?,
                -1,
            );
            self.push_ddl(&[(IDX_TAB, batch)])?;
            return Ok(());
        }
        not_found()
    }

    /// `(name, indexed columns)` of every live secondary-index IDX_TAB row (name in
    /// canonical lowercase, since `create_index`/`create_table` canonicalize at
    /// store time). The planner uses it to reject a re-index of an identical column
    /// set under the auto base name and to disambiguate an auto-generated name
    /// against the taken set. Reads the same slots `create_index` writes and
    /// `drop_index_by_name` reads.
    pub fn index_name_cols(&mut self) -> Result<Vec<(String, gnitz_wire::PkColList)>, ClientError> {
        let Some(idx_batch) = checked_sys_rows(IDX_TAB, self.session.scan(IDX_TAB)?)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for i in idx_batch.live_rows() {
            let Some(name) = col_str(&idx_batch.columns[IDXTAB_COL_NAME], i)? else {
                continue;
            };
            let cols = gnitz_wire::unpack_pk_cols(col_u64(&idx_batch.columns[IDXTAB_COL_SOURCE_COLS], i)?);
            out.push((name.to_string(), cols));
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

    /// Commit the open transaction as one atomic `FLAG_PUSH_TXN` frame — all
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
        let r = self.session.push_txn(&fam_refs, &preconditions);
        self.track_lsn(r)
    }

    /// The open transaction's buffer, for the SQL overlay's read-your-own-writes
    /// lookups. `None` in autocommit.
    pub fn txn_buffer(&self) -> Option<&TxnBuffer> {
        self.txn.as_ref()
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
    fn push_ddl(&mut self, families: &[(u64, ZSetBatch)]) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::ServerError(
                "DDL is not allowed inside a transaction".into(),
            ));
        }
        self.session.push_ddl_txn(families).map(|_| ())
    }

    pub fn create_schema(&mut self, name: &str) -> Result<u64, ClientError> {
        // Reject the empty string, a leading `_` (reserved system prefix), and
        // illegal characters. The SQL planner has no CREATE SCHEMA surface, so
        // this client entry point is the sole enforcement for schema names.
        let name = gnitz_wire::canonical_identifier(name).map_err(ClientError::ServerError)?;
        let new_sid = self.session.alloc_schema_id()?;
        let schema = sys_schema(SCHEMA_TAB);
        let mut batch = ZSetBatch::new(schema);
        gnitz_wire::sys_rows::write_schema_tab_row(
            &mut BatchAppender::new(&mut batch, schema),
            &gnitz_wire::sys_rows::SchemaTabRow {
                schema_id: new_sid,
                name: &name,
            },
            1,
        );
        self.push_ddl(&[(SCHEMA_TAB, batch)])?;
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
        let name = gnitz_wire::canonical_identifier(name).map_err(ClientError::ServerError)?;
        let schema_id = self.lookup_schema_id(&name)?;

        // Hidden segments need no separate pass: each is an ordinary VIEW_TAB row
        // carrying this `schema_id`, so the whole matching set is already complete
        // — and the engine's co-drop carve-out admits it, every dependent being in
        // the same drop set.
        let view_s = sys_schema(VIEW_TAB);
        let tbl_s = sys_schema(TABLE_TAB);
        let schema_s = sys_schema(SCHEMA_TAB);

        let views = self.schema_members(VIEW_TAB, schema_id)?;
        let tables = self.schema_members(TABLE_TAB, schema_id)?;

        let mut vb = ZSetBatch::new(view_s);
        views.append_view_tab(&mut BatchAppender::new(&mut vb, view_s))?;
        let mut tb = ZSetBatch::new(tbl_s);
        tables.append_table_tab(&mut BatchAppender::new(&mut tb, tbl_s))?;

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
        self.push_ddl(&families)?;

        // The VIEW_TAB `-1`s go out directly rather than through `drop_view`, so
        // each vid is invalidated here — without it a `SELECT` after a
        // `DROP SCHEMA` on this client answers off a dropped view.
        for vid in views.ids() {
            self.invalidate_own_copy(vid)?;
        }
        Ok(())
    }

    /// `unique_indexes` are the table's inline `UNIQUE` constraints, folded into
    /// the same atomic DDL bundle as `[COL_TAB, TABLE_TAB, IDX_TAB]` so a failure
    /// rolls the whole `CREATE` back — never a table left missing its unique
    /// constraint. Pass an empty slice for a table with no inline UNIQUE — and always
    /// for a stream, which only a base table's index owner check would admit.
    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        pk_cols: &[u32],
        props: TableProps,
        unique_indexes: &[InlineUniqueIndex],
    ) -> Result<u64, ClientError> {
        let table_name = gnitz_wire::canonical_identifier(table_name).map_err(ClientError::ServerError)?;
        let index_names: Vec<String> = unique_indexes
            .iter()
            .map(|spec| gnitz_wire::canonical_identifier(spec.name).map_err(ClientError::ServerError))
            .collect::<Result<_, _>>()?;
        // Column names take the reserved-infix half of the rule only: an index
        // name is interpolated from them, so a `__fk_` column would back an
        // undroppable index. This is the only enforcement point — the SQL planner
        // validates relation names, never column ones.
        for c in columns {
            gnitz_wire::reject_reserved_infix(&c.name).map_err(ClientError::ServerError)?;
        }
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        // Full schema-admissibility rule set (column cap + PK rules), applied
        // here so a caller that skipped the planner gets a clean error before
        // any id allocation instead of relying on the server-side reject (and
        // `pack_pk_cols` below can never panic).
        Schema::validate_parts(pk_cols, columns).map_err(|e| ClientError::ServerError(format!("create_table: {e}")))?;
        // `dist_prefix_len` is a leading-PK-prefix length (0 = default = full PK);
        // a value past the PK count is meaningless and the engine would silently
        // clamp it, so reject it here to catch the caller's mistake.
        if props.dist_prefix_len > pk_cols.len() {
            return Err(ClientError::ServerError(format!(
                "create_table: distribution prefix length {} exceeds PK column count {}",
                props.dist_prefix_len,
                pk_cols.len()
            )));
        }

        let new_tid = self.session.alloc_table_id()?;
        let schema_id = self.lookup_schema_id(&schema_name)?;

        // Encode the PK list using the shared wire packer so the engine
        // catalog decodes it identically. Single-PK callers still flow
        // through the same packer; the packed form's flag bit is what
        // distinguishes it from a bare scalar index.
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
        )?;

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

        // IDX_TAB family — every inline UNIQUE index as one multi-row batch, its
        // ids drawn in one allocation. Column types come from `columns`, so a
        // UNIQUE+FK column's parent-rewritten type is used.
        for spec in unique_indexes {
            // Structural rules only (arity, in-range, no duplicates) — unlike a
            // PK, an indexed column may be nullable. In-range against the actual
            // column list also keeps the `columns[c]` read below panic-free.
            gnitz_wire::validate_pk_indices(spec.col_indices, columns.len())
                .map_err(|e| ClientError::ServerError(format!("create_table: unique index '{}': {e}", spec.name)))?;
            for &c in spec.col_indices {
                gnitz_wire::index_key_type(columns[c as usize].type_code as u8).map_err(ClientError::ServerError)?;
            }
        }
        let mut families: Vec<(u64, ZSetBatch)> = vec![(COL_TAB, col_batch), (TABLE_TAB, tb)];
        if !unique_indexes.is_empty() {
            let first_index_id = self.session.alloc_index_ids(unique_indexes.len() as u64)?;
            let idx_schema = sys_schema(IDX_TAB);
            let mut idx_batch = ZSetBatch::new(idx_schema);
            {
                let mut a = BatchAppender::new(&mut idx_batch, idx_schema);
                for (k, spec) in unique_indexes.iter().enumerate() {
                    gnitz_wire::sys_rows::write_idx_tab_row(
                        &mut a,
                        &IdxTabRow {
                            index_id: first_index_id + k as u64,
                            owner_id: new_tid,
                            source_col_idx: gnitz_wire::pack_pk_cols(spec.col_indices),
                            name: &index_names[k],
                            is_unique: 1,
                        },
                        1,
                    );
                }
            }
            families.push((IDX_TAB, idx_batch));
        }
        self.push_ddl(&families)?;

        Ok(new_tid)
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Result<(), ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        let table_name = gnitz_wire::canonical_identifier(table_name).map_err(ClientError::ServerError)?;
        let not_found = || ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found"));
        let tid = self.resolve(&schema_name, &table_name)?.ok_or_else(not_found)?.tid;
        let batch = self.seek_sys_row(TABLE_TAB, tid)?.ok_or_else(not_found)?;
        let row = find_table_tab_row(&batch, tid)?.ok_or_else(not_found)?;

        let tbl_schema = sys_schema(TABLE_TAB);
        let mut tb = ZSetBatch::new(tbl_schema);
        gnitz_wire::sys_rows::write_table_tab_row(&mut BatchAppender::new(&mut tb, tbl_schema), &row, -1);
        self.push_ddl(&[(TABLE_TAB, tb)])?;

        Ok(())
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
        let vid = self.session.alloc_table_id()?;

        let mut cb = crate::circuit::CircuitBuilder::new(vid, source_table_id);
        let scan = cb.input_delta();
        cb.sink(scan);

        let vids = self.create_view_chain(
            schema_name,
            vec![PlannedView {
                name: ViewName::Named(view_name.to_string()),
                sql_text: String::new(),
                circuit: cb.build(),
                output_columns: output_columns.to_vec(),
                // Minimal SCAN→SINK passthrough: single output PK at slot 0.
                pk_cols: vec![0],
                // The circuit-builder API has no options surface at all.
                capacity_bytes: None,
                delta_bytes: None,
            }],
            None,
        )?;
        Ok(vids[0])
    }

    /// Create every view in `views` in one atomic `DDL_TXN`. Row order carries no
    /// meaning — the engine orders registration and backfill by the dependencies it
    /// derives from the `ScanDelta` nodes of the circuit rows. Returns the vids in
    /// input order.
    ///
    /// **One `BatchAppender` per family spans all views** — the engine's derived
    /// per-family lists (`family_pks_by_sign`, `families.find`) read only the
    /// first block per tid, so a chain must merge every view's COL/circuit rows
    /// into a single batch per family and one all-`+1` VIEW_TAB batch in
    /// input order. A single `push_ddl_txn` then commits — or, via the engine's
    /// per-family precheck/compensate loop, rolls back — the whole chain.
    ///
    /// `pk_cols` for each view is its physical PK column list — the leading `k`
    /// output slots (`[0]` for a synthetic-PK view, `0..k` for a compound-PK
    /// passthrough).
    ///
    /// `replaces` names an existing view this chain supersedes — an ALTER VIEW.
    /// Its `-1` rows (and every hidden segment it owns) join the same VIEW_TAB
    /// batch ahead of the new chain's `+1`s, making the replacement one DDL zone:
    /// a rejection anywhere in it leaves the old view exactly as it was. The
    /// engine runs the retractions first and then registers the new chain in
    /// dependency order, and its qname-collision check admits the incumbent
    /// because this bundle retires it.
    pub fn create_view_chain(
        &mut self,
        schema_name: &str,
        views: Vec<PlannedView>,
        replaces: Option<&str>,
    ) -> Result<Vec<u64>, ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        // Reject an over-long chain before any allocation.
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
        for pv in &views {
            // Hidden segment names are system-generated (`__h…` — the leading
            // `_` is exactly what marks them non-user), so only user-visible
            // names are validated.
            if let ViewName::Named(n) = &pv.name {
                crate::validate_user_identifier(n).map_err(ClientError::ServerError)?;
            }
            Schema::validate_parts(&pv.pk_cols, &pv.output_columns)
                .map_err(|e| ClientError::ServerError(format!("View '{}': {e}", pv.name)))?;
        }

        let schema_id = self.lookup_schema_id(&schema_name)?;

        // The outgoing view's rows, resolved before any id is allocated so a
        // missing view surfaces with no residue.
        let replaced = match replaces {
            Some(old) => {
                let old = gnitz_wire::canonical_identifier(old).map_err(ClientError::ServerError)?;
                Some(self.view_drop_rows(&schema_name, &old)?)
            }
            None => None,
        };

        // The whole bundle is assigned before any substitution runs, because a
        // downstream segment's `ScanDelta` names an upstream segment's id, so
        // every id is drawn in one allocation. `seg_ids` holds only the symbolic
        // ones, so a bundle of real preset ids gets an empty map and an identity
        // substitution pass.
        let fresh = views.iter().filter(|pv| needs_fresh_vid(pv.circuit.view_id)).count();
        let mut next_vid = if fresh > 0 {
            self.session.alloc_table_ids(fresh as u64)?
        } else {
            0
        };
        let mut vids: Vec<u64> = Vec::with_capacity(views.len());
        let mut seg_ids: HashMap<u64, u64> = HashMap::new();
        for pv in &views {
            let declared = pv.circuit.view_id;
            let vid = if needs_fresh_vid(declared) {
                let vid = next_vid;
                next_vid += 1;
                if is_segment_id(declared) {
                    seg_ids.insert(declared, vid);
                }
                vid
            } else {
                declared
            };
            vids.push(vid);
        }

        // One batch per family, spanning all views. COL_TAB and VIEW_TAB are
        // always non-empty; the circuit families are included only if some view
        // contributed rows.
        let col_s = sys_schema(COL_TAB);
        let nodes_s = sys_schema(CIRCUIT_NODES_TAB);
        let edges_s = sys_schema(CIRCUIT_EDGES_TAB);
        let ncol_s = sys_schema(CIRCUIT_NODE_COLUMNS_TAB);
        let view_s = sys_schema(VIEW_TAB);

        let mut col_batch = ZSetBatch::new(col_s);
        let mut nodes_batch = ZSetBatch::new(nodes_s);
        let mut edges_batch = ZSetBatch::new(edges_s);
        let mut ncol_batch = ZSetBatch::new(ncol_s);
        let mut view_batch = ZSetBatch::new(view_s);

        {
            let mut col_a = BatchAppender::new(&mut col_batch, col_s);
            let mut nodes_a = BatchAppender::new(&mut nodes_batch, nodes_s);
            let mut edges_a = BatchAppender::new(&mut edges_batch, edges_s);
            let mut ncol_a = BatchAppender::new(&mut ncol_batch, ncol_s);
            let mut view_a = BatchAppender::new(&mut view_batch, view_s);

            // 0. The replaced view's retractions, ahead of the new chain's `+1`s.
            if let Some(old) = &replaced {
                old.append_view_tab(&mut view_a)?;
            }

            for (mut pv, vid) in views.into_iter().zip(vids.iter().copied()) {
                // 0.5. Substitute the bundle's symbolic ids. Unconditional, so no
                // path reaches a catalog write without the surviving-tag check.
                pv.circuit.resolve_seg_ids(&seg_ids)?;
                let name = resolve_view_name(&pv.name, &seg_ids)?;

                // 1. Column records.
                append_col_rows(&mut col_a, vid, OWNER_KIND_VIEW, &pv.output_columns)?;

                // 2–4. Materialise the typed circuit into the three-table bundle.
                let rows = pv.circuit.into_rows();
                append_circuit_rows(&mut nodes_a, &mut edges_a, &mut ncol_a, vid, &rows)?;

                // 5. View row — the VIEW_TAB register hook triggers server-side
                // compilation. Encode the view PK with the shared wire packer so the
                // engine catalog decodes it identically to a TABLE_TAB PK.
                gnitz_wire::sys_rows::write_view_tab_row(
                    &mut view_a,
                    &ViewTabRow {
                        view_id: vid,
                        schema_id,
                        name: &name,
                        sql_definition: &pv.sql_text,
                        pk_col_idx: gnitz_wire::pack_pk_cols(&pv.pk_cols),
                        capacity_bytes: pv.capacity_bytes.unwrap_or(0),
                        delta_bytes: pv.delta_bytes.unwrap_or(0),
                    },
                    1,
                );
            }
        }

        // One families entry per tid (mandatory: the engine's derived lists read
        // only the first block per family), in dependency order — the server
        // re-sorts by topo priority anyway.
        let mut families: Vec<(u64, ZSetBatch)> = Vec::new();
        families.push((COL_TAB, col_batch));
        if !nodes_batch.is_empty() {
            families.push((CIRCUIT_NODES_TAB, nodes_batch));
        }
        if !edges_batch.is_empty() {
            families.push((CIRCUIT_EDGES_TAB, edges_batch));
        }
        if !ncol_batch.is_empty() {
            families.push((CIRCUIT_NODE_COLUMNS_TAB, ncol_batch));
        }
        families.push((VIEW_TAB, view_batch));

        self.push_ddl(&families)?;

        // Reached only by a host calling this directly: the SQL front end refuses
        // to retarget a view carrying a delta feed, and a feed is what makes a
        // view mirrorable.
        if let Some(old) = &replaced {
            for vid in old.ids() {
                self.invalidate_own_copy(vid)?;
            }
        }

        Ok(vids)
    }

    /// Drop a view and, cascading, every hidden segment view it owns
    /// (`__h{vid}_…`). The user view's `-1` and each hidden member's `-1` share
    /// one VIEW_TAB batch / one `push_ddl_txn`, so the engine's co-drop carve-out
    /// admits the bundle (every dependent is present in the same batch's drop set)
    /// and the whole chain retires atomically. A user view with no hidden members
    /// contributes a single `-1`.
    pub fn drop_view(&mut self, schema_name: &str, view_name: &str) -> Result<(), ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        let view_name = gnitz_wire::canonical_identifier(view_name).map_err(ClientError::ServerError)?;
        let retracted = self.view_drop_rows(&schema_name, &view_name)?;

        // One VIEW_TAB batch: the user view's `-1` plus every hidden member's.
        let view_s = sys_schema(VIEW_TAB);
        let mut vb = ZSetBatch::new(view_s);
        retracted.append_view_tab(&mut BatchAppender::new(&mut vb, view_s))?;
        self.push_ddl(&[(VIEW_TAB, vb)])?;

        // Without this a `SELECT` after a `DROP VIEW` on this same client would
        // answer rows off a view it just dropped.
        for vid in retracted.ids() {
            self.invalidate_own_copy(vid)?;
        }

        Ok(())
    }

    /// Every live row of system family `family` whose `schema_id` matches — the
    /// members `DROP SCHEMA` retracts. One code path over TABLE_TAB and VIEW_TAB,
    /// which the static assert beside their column constants licenses: the two
    /// agree on where `schema_id` sits.
    fn schema_members(&mut self, family: u64, schema_id: u64) -> Result<Retractions, ClientError> {
        let Some(scanned) = checked_sys_rows(family, self.session.scan(family)?)? else {
            return Ok(Retractions {
                scanned: ZSetBatch::new(sys_schema(family)),
                rows: Vec::new(),
            });
        };
        let mut rows = Vec::new();
        for i in scanned.live_rows() {
            if col_u64(&scanned.columns[TABTAB_COL_SCHEMA_ID], i)? == schema_id {
                rows.push(i);
            }
        }
        Ok(Retractions { scanned, rows })
    }

    /// What retiring `view_name` retracts: the user view followed by every hidden
    /// segment it owns, which the shared `hidden_view_prefix` identifies (hidden
    /// views are never shared across user views). DROP VIEW and the replacing
    /// half of ALTER VIEW retract exactly this set.
    ///
    /// A full VIEW_TAB scan, not a seek: the segments are a name prefix group,
    /// and no point lookup answers a prefix.
    fn view_drop_rows(&mut self, schema_name: &str, view_name: &str) -> Result<Retractions, ClientError> {
        let not_found = || ClientError::ServerError(format!("View '{schema_name}.{view_name}' not found"));
        let vid = self.resolve(schema_name, view_name)?.ok_or_else(not_found)?.tid;
        let scanned = checked_sys_rows(VIEW_TAB, self.session.scan(VIEW_TAB)?)?.ok_or_else(not_found)?;
        let owner = scanned.live_row_with_pk(vid).ok_or_else(not_found)?;
        let schema_id = col_u64(&scanned.columns[VIEWTAB_COL_SCHEMA_ID], owner)?;

        let prefix = hidden_view_prefix(vid);
        let mut rows = vec![owner];
        rows.extend(collect_view_rows_with_prefix(&scanned, schema_id, &prefix)?);
        Ok(Retractions { scanned, rows })
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
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        let current_name = gnitz_wire::canonical_identifier(current_name).map_err(ClientError::ServerError)?;
        let new_name = gnitz_wire::canonical_identifier(new_name).map_err(ClientError::ServerError)?;
        let not_found = || ClientError::ServerError(format!("Relation '{schema_name}.{current_name}' not found"));
        // Key the family lookup on the resolved id — the family's own PK — rather
        // than on `(schema_id, name)`, which would need a SCHEMA_TAB probe first.
        let desc = self.resolve(&schema_name, &current_name)?.ok_or_else(not_found)?;

        if desc.class.is_view() {
            let view_batch = self.seek_sys_row(VIEW_TAB, desc.tid)?.ok_or_else(not_found)?;
            let vr = find_view_tab_row(&view_batch, desc.tid)?.ok_or_else(not_found)?;
            let view_s = sys_schema(VIEW_TAB);
            let mut vb = ZSetBatch::new(view_s);
            {
                let mut a = BatchAppender::new(&mut vb, view_s);
                gnitz_wire::sys_rows::write_view_tab_row(&mut a, &vr, -1);
                gnitz_wire::sys_rows::write_view_tab_row(&mut a, &ViewTabRow { name: &new_name, ..vr }, 1);
            }
            self.push_ddl(&[(VIEW_TAB, vb)])?;
            // The whole registration, though a rename keeps the id and so costs a
            // re-bootstrap: the store's `VIEW_TAB` row carries the *old* name,
            // which is the state a re-registration reads to detect a
            // drop-and-recreate.
            self.invalidate_own_copy(desc.tid)?;
        } else {
            let tbl_batch = self.seek_sys_row(TABLE_TAB, desc.tid)?.ok_or_else(not_found)?;
            let tr = find_table_tab_row(&tbl_batch, desc.tid)?.ok_or_else(not_found)?;
            let tbl_s = sys_schema(TABLE_TAB);
            let mut tb = ZSetBatch::new(tbl_s);
            {
                let mut a = BatchAppender::new(&mut tb, tbl_s);
                gnitz_wire::sys_rows::write_table_tab_row(&mut a, &tr, -1);
                gnitz_wire::sys_rows::write_table_tab_row(&mut a, &TableTabRow { name: &new_name, ..tr }, 1);
            }
            self.push_ddl(&[(TABLE_TAB, tb)])?;
        }
        Ok(())
    }

    /// Rename a column: a `(-1, +1)` COL_TAB rewrite pair, same packed column id,
    /// the live column's exact payload at `-1` and only the `name` changed at
    /// `+1`. Column names preserve case (unlike relation names), so `old_col` is
    /// matched case-insensitively but the `-1` reproduces the STORED name, which
    /// is what the engine's retraction CAS compares it against. Rejects an unknown
    /// `old_col` and a collision with an existing visible column.
    pub fn alter_rename_column(
        &mut self,
        schema_name: &str,
        table_name: &str,
        old_col: &str,
        new_col: &str,
    ) -> Result<(), ClientError> {
        gnitz_wire::reject_reserved_infix(new_col).map_err(ClientError::ServerError)?;
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        let table_name = gnitz_wire::canonical_identifier(table_name).map_err(ClientError::ServerError)?;
        // One resolve yields the tid and the schema together. `alter_col_pair`
        // rejects a view.
        let desc = self
            .resolve(&schema_name, &table_name)?
            .ok_or_else(|| ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found")))?;
        let col_idx = desc.schema.visible_column_named(old_col).ok_or_else(|| {
            ClientError::ServerError(format!("column '{old_col}' not found in '{schema_name}.{table_name}'"))
        })?;
        if desc.schema.visible_column_named(new_col).is_some_and(|i| i != col_idx) {
            return Err(ClientError::ServerError(format!(
                "column '{new_col}' already exists in '{schema_name}.{table_name}'"
            )));
        }
        self.alter_col_pair(desc.tid, col_idx, |cd| cd.name = new_col.to_string())
    }

    /// `ALTER TABLE … DROP COLUMN` (logical): a `(-1, +1)` COL_TAB rewrite pair on
    /// the same packed column id — the live row's exact payload at `-1`, only
    /// `is_hidden` flipped to true at `+1`. The column stays physically present
    /// (`is_nullable`, `type_code`, position untouched), so the base table keeps
    /// its comparator. One atomic `push_ddl`; the engine precheck arm validates
    /// the drop shape and the dependent-view RESTRICT.
    pub fn alter_drop_column(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |cd| cd.is_hidden = true)
    }

    /// `ALTER TABLE … ALTER COLUMN … DROP NOT NULL`: a `(-1, +1)` COL_TAB rewrite
    /// pair on the same packed column id, only `is_nullable` flipped to true at
    /// `+1`. Once the catalog reports the column nullable, `ZSetBatch::validate`
    /// permits a null bit there and the engine swaps the table comparator
    /// `FixedIntNonnull → Generic` (if the table was all-non-null-fixed-int).
    pub fn alter_drop_not_null(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |cd| cd.is_nullable = true)
    }

    /// `ALTER TABLE … ADD COLUMN`: a single COL_TAB `+1` row appending `def` at
    /// `col_idx = <current physical column count>` — the physical layout, which
    /// **includes** columns hidden by a previous DROP COLUMN. Existing rows read
    /// the new column as NULL, so it must be nullable; the engine precheck
    /// enforces that along with the trailing position, the dependent-view
    /// RESTRICT and the `MAX_COLUMNS` bound.
    ///
    /// Deliberately not through [`Self::alter_col_pair`], whose whole job is
    /// reproducing a live row at `-1`; an append has no live row.
    ///
    /// The visible-name collision check lives here rather than in the SQL layer
    /// because `gnitz-core` is also the C and Python entry point, and the engine
    /// precheck does not scan COL_TAB for names — a duplicate would otherwise
    /// reach storage and only surface later as "column reference is ambiguous".
    pub fn alter_add_column(&mut self, tid: u64, def: &ColumnDef) -> Result<(), ClientError> {
        gnitz_wire::reject_reserved_infix(&def.name).map_err(ClientError::ServerError)?;
        let desc = self.describe_by_id(tid)?;
        reject_non_base_table(&desc, "ADD COLUMN")?;
        if desc.schema.visible_column_named(&def.name).is_some() {
            return Err(ClientError::ServerError(format!(
                "column '{}' already exists on table {tid}",
                def.name
            )));
        }
        let col_idx = desc.schema.num_columns();

        let col_s = sys_schema(COL_TAB);
        let mut cb = ZSetBatch::new(col_s);
        {
            let mut a = BatchAppender::new(&mut cb, col_s);
            gnitz_wire::sys_rows::write_col_tab_row(&mut a, &def.col_tab_row(tid, OWNER_KIND_TABLE, col_idx), 1)
                .map_err(ClientError::ServerError)?;
        }
        self.push_ddl(&[(COL_TAB, cb)])?;
        Ok(())
    }

    /// Build and push a COL_TAB `(-1, +1)` rewrite pair for column `col_idx` of
    /// base table `tid`: the live row goes in verbatim at `-1` and the same row
    /// with `flip` applied at `+1`. The single client-side pipeline behind
    /// RENAME COLUMN, DROP COLUMN, and DROP NOT NULL.
    ///
    /// The `-1` must be byte-equal to the stored row or the engine's CAS rejects
    /// the batch, which is why the columns come from this statement's own
    /// resolve of `tid` rather than from a caller-supplied `Schema`. That is
    /// fail-safe either way: the `-1`'s PK is `pack_col_id(tid, col_idx)`, so a
    /// wrong payload can only be rejected, never retract a different row.
    fn alter_col_pair(
        &mut self,
        tid: u64,
        col_idx: usize,
        flip: impl FnOnce(&mut ColumnDef),
    ) -> Result<(), ClientError> {
        let desc = self.describe_by_id(tid)?;
        reject_non_base_table(&desc, "ALTER COLUMN")?;
        let cd = desc
            .schema
            .columns
            .get(col_idx)
            .filter(|c| !c.is_hidden)
            .ok_or_else(|| ClientError::ServerError(format!("column index {col_idx} not found on table {tid}")))?;
        let mut new_cd = cd.clone();
        flip(&mut new_cd);

        let col_s = sys_schema(COL_TAB);
        let mut cb = ZSetBatch::new(col_s);
        {
            let mut a = BatchAppender::new(&mut cb, col_s);
            let old_row = cd.col_tab_row(tid, OWNER_KIND_TABLE, col_idx);
            gnitz_wire::sys_rows::write_col_tab_row(&mut a, &old_row, -1).map_err(ClientError::ServerError)?;
            let new_row = new_cd.col_tab_row(tid, OWNER_KIND_TABLE, col_idx);
            gnitz_wire::sys_rows::write_col_tab_row(&mut a, &new_row, 1).map_err(ClientError::ServerError)?;
        }
        self.push_ddl(&[(COL_TAB, cb)])?;
        Ok(())
    }

    /// Resolve `table_name` under `schema_name` to its id and schema. Anything that
    /// is not a base table is reported as absent — callers that need to tell the
    /// classes apart use [`Self::resolve_relation`], which returns the class.
    pub fn resolve_table_id(&mut self, schema_name: &str, table_name: &str) -> Result<(u64, Arc<Schema>), ClientError> {
        let d = self.resolve(schema_name, table_name)?;
        match d.filter(|d| d.class == RelClass::Table) {
            Some(d) => Ok((d.tid, Arc::clone(&d.schema))),
            None => Err(ClientError::ServerError(format!(
                "Table '{}' not found",
                qualified_name(schema_name, table_name)
            ))),
        }
    }

    /// Resolve `name` under `schema_name`, rejecting a missing relation. The
    /// erroring form of [`Self::resolve`], for the callers whose next step needs
    /// the relation to exist.
    pub fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(schema_name, name)?.ok_or_else(|| {
            ClientError::ServerError(format!(
                "Table or view '{}' not found",
                qualified_name(schema_name, name)
            ))
        })
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
        if let Some(hit) = self.catalog().get(schema_name, name) {
            return Ok(hit);
        }
        let qname = qualified_name(schema_name, name);
        let found = self.fetch_descriptor(RelTarget::Name(&qname))?;
        self.record_relation(schema_name, name, found.clone());
        Ok(found)
    }

    /// One RESOLVE round trip. `Ok(None)` is a relation-absent verdict.
    fn fetch_descriptor(&mut self, target: RelTarget<'_>) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let Some((tid, schema, blob)) = self.session.resolve(target)? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(RelDescriptor::from_resolve(tid, schema, blob))))
    }

    // --- Private catalog-lookup helpers ---

    /// Resolve `schema_name` (already canonicalized) to its SCHEMA_TAB id. A
    /// missing row — or an entirely empty SCHEMA_TAB — is the one
    /// schema-qualified "not found" error every DDL/resolve path reports.
    pub(crate) fn lookup_schema_id(&mut self, schema_name: &str) -> Result<u64, ClientError> {
        let batch = checked_sys_rows(SCHEMA_TAB, self.session.scan(SCHEMA_TAB)?)?;
        match &batch {
            Some(b) => find_schema_id(b, schema_name)?,
            None => None,
        }
        .ok_or_else(|| ClientError::ServerError(format!("Schema '{schema_name}' not found")))
    }

    /// The live system-catalog row with PK `id`, as the one-row batch it came back
    /// in — one master-local SEEK, not a transfer of the whole family. The batch
    /// rather than a decoded row, because a caller reads the row back out of it to
    /// write the matching `-1`.
    fn seek_sys_row(&mut self, family: u64, id: u64) -> Result<Option<ZSetBatch>, ClientError> {
        let stride = sys_schema(family).pk_stride();
        let reply = self
            .session
            .seek(family, &PkTuple::from_u128(stride as u8, id as u128))?;
        checked_sys_rows(family, reply)
    }
}

// --- TxnBuffer: the locally-buffered atomic write-batch transaction ---

/// One buffered family: a maximal run of same-mode ops on one relation.
struct BufferedFamily {
    tid: u64,
    schema: Schema,
    batch: ZSetBatch,
    mode: WireConflictMode,
}

/// The write side of an open transaction. `push`/`delete` append to the target
/// tid's **last** family when its conflict mode matches, else open a new family
/// (run-splitting), so per-table op order is preserved end to end: buffer call
/// order = family frame order = the engine's validation-fold and
/// worker-application order. Cross-tid interleaving is unconstrained (FK
/// validation is post-transaction, order-free).
///
/// It also indexes each buffered row by PK (`last_op_of`) as it arrives, so the
/// SQL overlay's read-your-own-writes lookups are O(1) point reads rather than a
/// re-fold of the whole buffer per statement.
///
/// Owned by [`GnitzClient::txn`]; every client write path routes into it while
/// it is open, and `txn_commit` ships it as one `FLAG_PUSH_TXN` frame. Dropping
/// it is rollback — nothing was ever sent.
#[derive(Default)]
pub struct TxnBuffer {
    /// Creation order; per tid, maximal same-mode runs of the caller's op
    /// sequence.
    families: Vec<BufferedFamily>,

    /// tid → index in `families` of that tid's most recently opened family, so
    /// a matching-mode append extends it rather than opening a new family.
    last_family_of: HashMap<u64, usize>,
    /// tid → PK → `(family index, row index)` of the LAST op buffered on that
    /// PK. Row indices are stable: `append` only ever extends a family batch or
    /// pushes a new one. Weight-0 rows are not indexed — they are inert, exactly
    /// as the engine's fold treats them.
    last_op_of: HashMap<u64, HashMap<PkTuple, (usize, usize)>>,
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
        let extend = self
            .last_family_of
            .get(&tid)
            .is_some_and(|&idx| self.families[idx].mode == mode);
        let (fam, base) = if extend {
            let idx = self.last_family_of[&tid];
            (idx, self.families[idx].batch.len())
        } else {
            (self.families.len(), 0)
        };

        let index = self.last_op_of.entry(tid).or_default();
        for i in 0..batch.len() {
            if batch.weights[i] != 0 {
                index.insert(batch.pks.get_tuple(i), (fam, base + i));
            }
        }

        if extend {
            self.families[fam].batch.extend_from_owned(batch);
        } else {
            self.families.push(BufferedFamily {
                tid,
                schema: schema.clone(),
                batch,
                mode,
            });
            self.last_family_of.insert(tid, fam);
        }
    }

    /// The last op buffered on `pk` in `tid`, as `(batch, row)` — or `None` if
    /// the transaction has not touched that PK. The row's **weight sign** is the
    /// net effect (positive: live row with that payload; negative: deleted),
    /// mirroring the engine's `fold_family`.
    pub fn last_op(&self, tid: u64, pk: &PkTuple) -> Option<(&ZSetBatch, usize)> {
        let &(fam, row) = self.last_op_of.get(&tid)?.get(pk)?;
        Some((&self.families[fam].batch, row))
    }

    /// Every PK the transaction has touched in `tid`, with its last op.
    pub fn last_ops(&self, tid: u64) -> impl Iterator<Item = (PkTuple, &ZSetBatch, usize)> + '_ {
        self.last_op_of
            .get(&tid)
            .into_iter()
            .flatten()
            .map(move |(pk, &(fam, row))| (*pk, &self.families[fam].batch, row))
    }
}

/// `Ok(None)` = name absent (a legitimate miss); `Err` = a decode error on a
/// corrupt catalog batch.
fn find_schema_id(batch: &ZSetBatch, name: &str) -> Result<Option<u64>, ClientError> {
    for i in batch.live_rows() {
        if col_str(&batch.columns[SCHEMATAB_COL_NAME], i)? == Some(name) {
            return Ok(Some(batch.pks.get(i) as u64));
        }
    }
    Ok(None)
}

/// Read row `i` of a `TABLE_TAB` batch as the wire row struct, borrowed out of
/// the batch. This crate's one reading of the layout, and its write side is
/// `write_table_tab_row` — so a `-1` built from one of these reproduces the live
/// row by construction, not by two encoders agreeing.
fn read_table_tab_row(b: &ZSetBatch, i: usize) -> Result<TableTabRow<'_>, ClientError> {
    Ok(TableTabRow {
        table_id: b.pks.get(i) as u64,
        schema_id: col_u64(&b.columns[TABTAB_COL_SCHEMA_ID], i)?,
        name: col_str(&b.columns[TABTAB_COL_NAME], i)?.unwrap_or(""),
        pk_col_idx: col_u64(&b.columns[TABTAB_COL_PK_COL_IDX], i)?,
        flags: col_u64(&b.columns[TABTAB_COL_FLAGS], i)?,
    })
}

/// [`read_table_tab_row`]'s VIEW_TAB peer.
fn read_view_tab_row(b: &ZSetBatch, i: usize) -> Result<ViewTabRow<'_>, ClientError> {
    Ok(ViewTabRow {
        view_id: b.pks.get(i) as u64,
        schema_id: col_u64(&b.columns[VIEWTAB_COL_SCHEMA_ID], i)?,
        name: col_str(&b.columns[VIEWTAB_COL_NAME], i)?.unwrap_or(""),
        sql_definition: col_str(&b.columns[VIEWTAB_COL_SQL], i)?.unwrap_or(""),
        pk_col_idx: col_u64(&b.columns[VIEWTAB_COL_PK_COL_IDX], i)?,
        capacity_bytes: col_u64(&b.columns[VIEWTAB_COL_CAPACITY], i)?,
        delta_bytes: col_u64(&b.columns[VIEWTAB_COL_DELTA], i)?,
    })
}

/// [`read_table_tab_row`]'s IDX_TAB peer. `is_unique` stays the stored word
/// rather than a `bool`, so a DROP echoes back exactly what it read.
fn read_idx_tab_row(b: &ZSetBatch, i: usize) -> Result<IdxTabRow<'_>, ClientError> {
    Ok(IdxTabRow {
        index_id: b.pks.get(i) as u64,
        owner_id: col_u64(&b.columns[IDXTAB_COL_OWNER_ID], i)?,
        source_col_idx: col_u64(&b.columns[IDXTAB_COL_SOURCE_COLS], i)?,
        name: col_str(&b.columns[IDXTAB_COL_NAME], i)?.unwrap_or(""),
        is_unique: col_u64(&b.columns[IDXTAB_COL_IS_UNIQUE], i)?,
    })
}

/// The live `TABLE_TAB` row with PK `tid`. `Ok(None)` = absent (a legitimate
/// miss); `Err` = a decode error on a corrupt batch, which must surface rather
/// than be masked as a miss.
fn find_table_tab_row(b: &ZSetBatch, tid: u64) -> Result<Option<TableTabRow<'_>>, ClientError> {
    b.live_row_with_pk(tid).map(|i| read_table_tab_row(b, i)).transpose()
}

/// [`find_table_tab_row`]'s VIEW_TAB peer.
fn find_view_tab_row(b: &ZSetBatch, vid: u64) -> Result<Option<ViewTabRow<'_>>, ClientError> {
    b.live_row_with_pk(vid).map(|i| read_view_tab_row(b, i)).transpose()
}

/// The row indices of every live `VIEW_TAB` row whose `schema_id` matches and
/// whose name starts with `prefix`. Drives the cascading DROP of a user view's
/// synthesized hidden segment views (`__h{vid}_…`); the caller reads each row out
/// of the same batch to write its `-1`.
fn collect_view_rows_with_prefix(b: &ZSetBatch, schema_id: u64, prefix: &str) -> Result<Vec<usize>, ClientError> {
    let mut out = Vec::new();
    for i in b.live_rows() {
        if col_u64(&b.columns[VIEWTAB_COL_SCHEMA_ID], i)? != schema_id {
            continue;
        }
        if matches!(col_str(&b.columns[VIEWTAB_COL_NAME], i)?, Some(n) if n.starts_with(prefix)) {
            out.push(i);
        }
    }
    Ok(out)
}

/// The rows of a scanned system-catalog batch that a DDL is about to retract —
/// the batch included, because a `-1` reproduces the live row by being read back
/// out of the batch it was scanned from.
struct Retractions {
    scanned: ZSetBatch,
    rows: Vec<usize>,
}

impl Retractions {
    /// The relation id of each selected row.
    fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.rows.iter().map(|&i| self.scanned.pks.get(i) as u64)
    }

    /// Append every selected VIEW_TAB row at `-1`, through the family's one
    /// writer — so a retraction and its create cannot diverge.
    fn append_view_tab(&self, a: &mut BatchAppender<'_>) -> Result<(), ClientError> {
        for &i in &self.rows {
            gnitz_wire::sys_rows::write_view_tab_row(a, &read_view_tab_row(&self.scanned, i)?, -1);
        }
        Ok(())
    }

    /// [`Self::append_view_tab`]'s TABLE_TAB peer.
    fn append_table_tab(&self, a: &mut BatchAppender<'_>) -> Result<(), ClientError> {
        for &i in &self.rows {
            gnitz_wire::sys_rows::write_table_tab_row(a, &read_table_tab_row(&self.scanned, i)?, -1);
        }
        Ok(())
    }
}

/// A system family's rows, with the reply's schema checked against
/// `sys_schema(family)`. The row readers above index a reply positionally, and a
/// reply is decoded against the server's block or this connection's cache — never
/// against `sys_schema` — so the check is what makes that indexing a precondition
/// rather than a cross-crate assumption.
fn checked_sys_rows(family: u64, reply: crate::connection::ScanReply) -> Result<Option<ZSetBatch>, ClientError> {
    let (schema, batch, _) = reply;
    if batch.is_some() && !schema.as_deref().is_some_and(|s| s.types_match(sys_schema(family))) {
        return Err(ClientError::ServerError(format!(
            "system family {family} answered in a schema that is not its own"
        )));
    }
    Ok(batch)
}

/// Append one `COL_TAB` row per column of `owner_id`, at `+1`.
fn append_col_rows(
    a: &mut BatchAppender<'_>,
    owner_id: u64,
    owner_kind: u64,
    columns: &[ColumnDef],
) -> Result<(), ClientError> {
    for (i, cd) in columns.iter().enumerate() {
        gnitz_wire::sys_rows::write_col_tab_row(a, &cd.col_tab_row(owner_id, owner_kind, i), 1)
            .map_err(ClientError::ServerError)?;
    }
    Ok(())
}

/// Append a circuit's node / edge / node-column rows to the three circuit-family
/// batch appenders under `vid` (the compound `(view_id, sub)` PK prefix). The
/// single home for the circuit-family wire layout — the PK packings and the
/// nullable `source_table` / `expr_program` writers — used by CREATE VIEW
/// (`create_view_chain`).
fn append_circuit_rows(
    nodes_a: &mut BatchAppender<'_>,
    edges_a: &mut BatchAppender<'_>,
    ncol_a: &mut BatchAppender<'_>,
    vid: u64,
    rows: &crate::circuit::CircuitRows,
) -> Result<(), ClientError> {
    use gnitz_wire::sys_rows::{
        write_circuit_edge_row, write_circuit_node_column_row, write_circuit_node_row, CircuitEdgeRow,
        CircuitNodeColumnRow, CircuitNodeRow,
    };
    for (node_id, opcode, src_tab, expr_blob) in &rows.nodes {
        write_circuit_node_row(
            nodes_a,
            &CircuitNodeRow {
                view_id: vid,
                node_id: *node_id,
                opcode: *opcode,
                source_table: *src_tab,
                expr_program: expr_blob.as_deref(),
            },
            1,
        )
        .map_err(ClientError::ServerError)?;
    }
    for (dst_node, dst_port, src_node) in &rows.edges {
        write_circuit_edge_row(
            edges_a,
            &CircuitEdgeRow {
                view_id: vid,
                dst_node: *dst_node,
                dst_port: *dst_port as u64,
                src_node: *src_node,
            },
            1,
        )
        .map_err(ClientError::ServerError)?;
    }
    for (node_id, kind, position, v1, v2) in &rows.node_columns {
        write_circuit_node_column_row(
            ncol_a,
            &CircuitNodeColumnRow {
                view_id: vid,
                node_id: *node_id,
                kind: *kind,
                position: *position as u64,
                value1: *v1,
                value2: *v2,
            },
            1,
        )
        .map_err(ClientError::ServerError)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;
