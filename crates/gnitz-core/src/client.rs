use crate::connection::{
    DeltaCursor, IdRun, Interest, PollEnd, PollSink, Polled, PolledView, RawBlock, RelDescriptor, RelTarget, Reply,
    Request, ScanReply, ScanResult, Session, SlotId,
};
use crate::error::ClientError;
use crate::protocol::transport::poll_fd;
use crate::{sys_schema, BatchAppender, PkColumn, ProtocolError, PushFamily, Schema, ZSetBatch};
use gnitz_expr::{ColumnTable, SchemaFacts};
use gnitz_wire::{ColumnDef, PkBuf, WireConflictMode};
use gnitz_wire::{WireFault, WireStatus};
use std::collections::HashMap;
use std::sync::Arc;

use gnitz_expr::{payload_bytes, payload_is_null, payload_u64, LogicalProgram, RowFilter};
use gnitz_wire::sys_rows::{ColTabRow, FkRef, IdxTabRow, TableTabRow, ViewTabRow};
use gnitz_wire::txn_frame::{DeltaPollItem, BLIND};
use gnitz_wire::{Circuit, ComputeMap, ReadBound, ReadSink, ReadSpec};
use gnitz_wire::{
    RelClass, TableProps, ViewProps, CIRCUIT_NODES_TAB, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME,
    COL_TAB, IDXTAB_PAY_FLAGS, IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, IDX_TAB, RELTAB_PAY_NAME,
    RELTAB_PAY_SCHEMA_ID, SCHEMATAB_PAY_NAME, SCHEMA_TAB, TABLE_TAB, VIEW_TAB,
};

// --- Module-private helpers ---

/// Row `i` of a system-table STRING payload column. Every such column is declared
/// non-nullable, so a NULL is a malformed reply, not a case — this is the trust
/// boundary that says so rather than substituting `""`. UTF-8 is validated here
/// too: the region carries bytes.
fn col_str(batch: &ZSetBatch, pi: usize, i: usize) -> Result<&str, ClientError> {
    if payload_is_null(batch, i, pi) {
        return Err(
            ProtocolError::DecodeError(format!("col_str: NULL in a non-nullable system column at row {i}")).into(),
        );
    }
    std::str::from_utf8(payload_bytes(batch, i, pi))
        .map_err(|e| ProtocolError::DecodeError(format!("col_str: invalid UTF-8 at row {i}: {e}")).into())
}

/// [`gnitz_wire::qualified_key`] from names that may still be raw user text.
/// The fold lives on this side only — see that function for why.
pub fn qualified_name(schema_name: &str, name: &str) -> String {
    let mut q = gnitz_wire::qualified_key(schema_name, name);
    q.make_ascii_lowercase();
    q
}

/// The classified absence every schema-qualified catalog lookup reports, rather
/// than a spelling of one message per call site. `name` reaches the message only,
/// never a key, so it is reported as the caller spelled it: someone who wrote
/// `MyTab` is told about `MyTab`.
pub fn not_found(noun: &'static str, schema_name: &str, name: &str) -> ClientError {
    absent(format!("{noun} '{schema_name}.{name}' not found"))
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
    pub col_indices: Vec<u32>,
    pub name: String,
}

/// One live secondary index, as its IDX_TAB row states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRow {
    /// The id of the table it indexes.
    pub owner: u64,
    pub name: String,
    pub cols: gnitz_wire::PkColList,
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

/// One FOREIGN KEY column of a `CREATE TABLE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InlineForeignKey {
    pub col_idx: u32,
    pub target: FkTarget,
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
/// Unique because vids are, and unspellable at every SQL surface because
/// [`gnitz_wire::validate_user_identifier`] rejects a leading `_`. Ownership is the
/// `owner_view_id` column, not the name.
fn segment_name(vid: u64) -> String {
    format!("_seg{vid}")
}

/// The symbolic id by which a [`ViewBundle`] circuit's `ScanDelta` names
/// `segments[j]`. Symbolic ids start at [`gnitz_wire::CATALOG_ID_CEILING`], which
/// no durable relation id reaches, so `create_view_chain` tells them apart from
/// real relation ids and substitutes the id it allocated — a bundle reaches no
/// server while it is built.
pub fn segment_id(j: u64) -> u64 {
    gnitz_wire::CATALOG_ID_CEILING + j
}

/// One view in a [`ViewBundle`].
pub struct PlannedView {
    pub circuit: Circuit,
    pub schema: Arc<Schema>,
    /// [`gnitz_wire::ViewFlags::pk_repeats`], stated by the emitter that minted
    /// the key.
    pub pk_repeats: bool,
}

/// A [`GnitzClient::create_view_chain`] bundle: the user-named view and the
/// hidden segments it owns, in dependency order. [`segment_id`]`(j)` names
/// `segments[j]`.
pub struct ViewBundle {
    pub segments: Vec<PlannedView>,
    pub view: PlannedView,
}

impl From<PlannedView> for ViewBundle {
    fn from(view: PlannedView) -> Self {
        ViewBundle { segments: Vec::new(), view }
    }
}

/// Run when a signal interrupts a blocking call's wait; an `Err` aborts the call.
/// The Python binding checks for Ctrl-C here.
pub type ParkHook = Box<dyn FnMut() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send>;

pub struct GnitzClient {
    pub(crate) session: Session,
    pub(crate) park_hook: Option<ParkHook>,
    serial_cache: HashMap<u64, SerialRange>,
    /// Open transaction, if any; `None` is autocommit. Every user-table write
    /// buffers here while it is open, and dropping it is ROLLBACK.
    txn: Option<TxnBuffer>,
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
    assert_send_sync::<crate::ClientTransport>();
    assert_send::<ZSetBatch>();
    assert_send::<ClientError>();
    // Handed back as `Arc<Schema>` by the scan path, and `Arc<T>: Send` requires
    // `T: Send + Sync`, so this one is the stricter bound.
    assert_send_sync::<Schema>();
};

impl GnitzClient {
    pub fn connect(target: &str) -> Result<Self, ClientError> {
        Session::connect(target).map(Self::from_session)
    }

    /// A client over an already-connected session.
    pub(crate) fn from_session(session: Session) -> GnitzClient {
        GnitzClient {
            session,
            park_hook: None,
            serial_cache: HashMap::new(),
            txn: None,
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

    // ── The blocking driver ────────────────────────────────────────────────

    pub(crate) fn round_trip(&mut self, req: Request<'_>) -> Result<Reply, ClientError> {
        let slot = self.session.submit(req)?;
        await_slot(&mut self.session, &mut self.park_hook, slot, None)
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

    /// Push `batch` under `mode`. SQL `INSERT` uses `Error` to get SQL-standard
    /// rejection semantics; every other caller passes `Update`.
    ///
    /// Inside an open transaction the batch is buffered instead of sent, and the
    /// returned LSN is `0` — nothing is durable until `txn_commit`, which returns
    /// the one zone LSN covering the whole bundle.
    pub fn push(
        &mut self,
        table_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        if let Some(txn) = &mut self.txn {
            txn.push(table_id, schema, batch.clone(), mode, BLIND)?;
            return Ok(0);
        }
        self.send_push(table_id, schema, batch, mode)
    }

    /// [`Self::push`] for a caller that owns the batch and drops it:
    /// inside a transaction the rows move into the buffer instead of being deep
    /// cloned. The borrowing form stays for callers that keep the batch.
    pub fn push_owned(
        &mut self,
        table_id: u64,
        schema: &Schema,
        batch: ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        if let Some(txn) = &mut self.txn {
            txn.push(table_id, schema, batch, mode, BLIND)?;
            return Ok(0);
        }
        self.send_push(table_id, schema, &batch, mode)
    }

    fn send_push(
        &mut self,
        target_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        Ok(self
            .round_trip(Request::Push { target_id, schema, batch, mode })?
            .into_lsn())
    }

    /// Run a parameterized bounded read, replied in `reply_schema`'s layout.
    pub fn scan_spec(&mut self, table_id: u64, spec: &ReadSpec, reply_schema: &Arc<Schema>) -> ScanResult {
        self.round_trip(Request::ScanSpec { target_id: table_id, spec, reply_schema })
            .map(Reply::into_scan)
    }

    // ── The read seam ──────────────────────────────────────────────────────
    //
    // `resolve` and `scan_spec` are the connection; the `_local_first` pair
    // below consults the copy and falls through to them, so every call site
    // declares which freshness it is asking for. **The gate is what the copy
    // holds, never whether a store is attached**, so a client with one reads
    // exactly like a client without for every relation the copy does not hold.

    /// [`Self::resolve`], answered off a mirrored registration when there is one
    /// — which is what keeps a mirrored `SELECT` round-trip-free.
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
            Some(desc) => Ok(Some(desc)),
            None => self.resolve(schema_name, name),
        }
    }

    /// [`Self::scan_spec`], answered off the copy when it holds `table_id`, with
    /// no served LSN: a copy's freshness is [`Self::cursor_of`].
    pub fn scan_spec_local_first(&mut self, table_id: u64, spec: ReadSpec, reply_schema: &Arc<Schema>) -> ScanResult {
        match self.mirror.as_deref_mut() {
            Some(m) if m.cursor_of(table_id).is_some() => Ok(ScanReply {
                batch: m.store.scan_spec(table_id, spec, reply_schema)?,
                schema: Arc::clone(reply_schema),
                lsn: None,
            }),
            _ => self.scan_spec(table_id, &spec, reply_schema),
        }
    }

    /// Replace the connection and keep the copies — what a host does after a
    /// server restart, which kills the socket while the copies survive it.
    ///
    /// Refused while a transaction is open. Otherwise the client is rebuilt
    /// through [`Self::connect`] rather than reset field by field, which keeps
    /// the transaction slot and the SERIAL cache from being enumerated here and
    /// drifting. Nothing is taken out of
    /// `self` until the new session exists, so a failed connect leaves this
    /// client exactly as it was.
    ///
    /// A copy rides along with every cursor dropped: the new connection may be a
    /// different server, where the same name is a different id, so the next
    /// poll re-resolves every view by name. A poisoned store crosses unchanged.
    pub fn reconnect(&mut self, target: &str) -> Result<(), ClientError> {
        if self.txn_active() {
            return Err(ClientError::from(
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
    ) -> Result<(ScanReply, DeltaCursor), ClientError> {
        self.delta_read(view_id, 0, view_schema)
    }

    /// Poll a view's delta feed: every delta it emitted in `(cursor.tick, T]`,
    /// with the cursor to poll from next. The reply comes back in the view's
    /// schema, weights and all. Apply what comes back and store the new cursor;
    /// there is nothing to filter and nothing to reconcile.
    ///
    /// A reply whose tag does not continue the cursor is refused as
    /// `DeltaExpired`: discard the copy and
    /// [`delta_bootstrap`](Self::delta_bootstrap) again.
    ///
    /// A poll does **not** drive a tick: a delta read answers "what has
    /// happened", not "what is current", so a push the tick loop has not run yet
    /// is a round the next poll will carry.
    pub fn delta_poll(
        &mut self,
        view_id: u64,
        cursor: DeltaCursor,
        view_schema: &Arc<Schema>,
    ) -> Result<(ScanReply, DeltaCursor), ClientError> {
        let (data, next) = self.delta_read(view_id, cursor.tick.get(), view_schema)?;
        Ok((data, cursor.advanced_to(next)?))
    }

    /// One view's delta read, decoded under `reply_schema`, with the terminal
    /// frame's `(tag, T)` pair as a cursor.
    fn delta_read(
        &mut self,
        view_id: u64,
        after_tick: u64,
        reply_schema: &Arc<Schema>,
    ) -> Result<(ScanReply, DeltaCursor), ClientError> {
        let item = DeltaPollItem {
            view_id,
            after_tick,
            reply_layout: reply_schema.layout_digest(),
        };
        let schema = Arc::clone(reply_schema);
        let mut batch = ZSetBatch::new(&schema);
        let cursor = delta_read_blocks(&mut self.session, &mut self.park_hook, item, |b| {
            Ok(crate::protocol::wal_block::decode_wal_block_into(
                &mut batch,
                b.block(),
                &schema,
            )?)
        })?;
        Ok((ScanReply { schema, batch, lsn: None }, cursor))
    }

    /// [`Self::delta_read`] keeping the reply's raw blocks, the whole train in
    /// hand. The cursor comes back unchecked against a previous one.
    pub(crate) fn delta_read_raw(&mut self, item: DeltaPollItem) -> PolledView {
        let mut blocks = Vec::new();
        let cursor = delta_read_blocks(&mut self.session, &mut self.park_hook, item, |b| {
            blocks.push(b);
            Ok(())
        })?;
        Ok((blocks, cursor))
    }

    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table `txn_commit` is never observed torn
    /// across the result set.
    /// Each relation is replied in the layout of the schema paired with it.
    pub fn scan_many(&mut self, relations: Vec<(u64, Arc<Schema>)>) -> Result<Vec<ScanReply>, ClientError> {
        self.round_trip(Request::ScanMulti(relations)).map(Reply::into_multi)
    }

    /// The descriptor for `tid` — the by-id twin of [`Self::resolve`], one round
    /// trip.
    pub fn describe_by_id(&mut self, tid: u64) -> Result<Arc<RelDescriptor>, ClientError> {
        self.round_trip(Request::Resolve(RelTarget::Id(tid)))
            .map(Reply::into_resolve)?
            .ok_or_else(|| absent(format!("relation {tid} not found")))
    }

    /// Index `col_indices` of table `table_id`, in that order, under the catalog
    /// name `index_name`.
    pub fn create_index(
        &mut self,
        table_id: u64,
        col_indices: &[u32],
        index_name: &str,
        is_unique: bool,
    ) -> Result<u64, ClientError> {
        let index_name = gnitz_wire::canonical_identifier(index_name)?;
        // Arity, 7-bit column range, duplicates — the Err form of the
        // pack_pk_cols contract, so the pack below can never panic.
        gnitz_wire::validate_pk_col_list(col_indices, gnitz_wire::PK_LIST_COL_LIMIT)
            .map_err(|e| ClientError::from(format!("create_index: {e}")))?;

        let index_id = self.alloc_id()?;

        let idx_schema = sys_schema(IDX_TAB);
        let mut batch = ZSetBatch::new(idx_schema);
        gnitz_wire::sys_rows::write_idx_tab_row(
            &mut BatchAppender::new(&mut batch),
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
        self.drop_index_rows(&[name], "constraint", if_exists, |b, i| {
            payload_u64(b, i, IDXTAB_PAY_OWNER_ID) == tid
                && gnitz_wire::IndexProps::from_flags(payload_u64(b, i, IDXTAB_PAY_FLAGS)).is_ok_and(|p| p.is_unique)
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
        let scanned = self.sys_rows(IDX_TAB, ReadBound::None)?;
        let idx_schema = sys_schema(IDX_TAB);
        let mut batch = ZSetBatch::new(idx_schema);
        let mut retired: Vec<usize> = Vec::with_capacity(names.len());
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            let mut hit = None;
            for i in scanned.live_rows() {
                if col_str(&scanned, IDXTAB_PAY_NAME, i)? == name && matches(&scanned, i) {
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
                None => return Err(absent(format!("{noun} '{name}' not found"))),
            }
        }
        // Every name skipped: no zone, so no barrier and no fdatasync.
        if batch.is_empty() {
            return Ok(());
        }
        self.push_ddl_txn(&[(IDX_TAB, batch)])
    }

    /// Every live secondary index. Names come back canonical (lowercase): every
    /// writer folds them at store time.
    pub fn index_rows(&mut self) -> Result<Vec<IndexRow>, ClientError> {
        let idx_batch = self.sys_rows(IDX_TAB, ReadBound::None)?;
        let mut out = Vec::new();
        for i in idx_batch.live_rows() {
            let name = col_str(&idx_batch, IDXTAB_PAY_NAME, i)?.to_string();
            let cols =
                gnitz_wire::unpack_pk_cols(payload_u64(&idx_batch, i, IDXTAB_PAY_SOURCE_COLS)).map_err(|rule| {
                    ProtocolError::DecodeError(format!(
                        "index '{name}': {}",
                        rule.for_role(gnitz_wire::PkListRole::ColumnList)
                    ))
                })?;
            let flags = payload_u64(&idx_batch, i, IDXTAB_PAY_FLAGS);
            let is_unique = gnitz_wire::IndexProps::from_flags(flags)
                .map_err(|e| ProtocolError::DecodeError(format!("index '{name}': {e}")))?
                .is_unique;
            out.push(IndexRow {
                owner: payload_u64(&idx_batch, i, IDXTAB_PAY_OWNER_ID),
                name,
                cols,
                is_unique,
            });
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
    pub fn txn_commit(&mut self) -> Result<u64, ClientError> {
        let buf = self.txn.take().ok_or_else(no_transaction)?;
        self.push_txn(&buf)
    }

    /// Ship `buf` as one `PUSH_TXN` frame, whose families land together under one
    /// zone LSN or not at all; an empty buffer sends nothing.
    fn push_txn(&mut self, buf: &TxnBuffer) -> Result<u64, ClientError> {
        if buf.families.is_empty() {
            return Ok(0);
        }
        let families: Vec<PushFamily<'_>> = buf
            .families
            .iter()
            .map(|f| PushFamily {
                tid: f.tid,
                schema: &f.schema,
                batch: &f.batch,
                mode: f.mode,
                basis: f.basis,
            })
            .collect();
        self.round_trip(Request::PushTxn { families: &families })
            .map(Reply::into_lsn)
    }

    /// Read `target`'s rows under `bound` and `predicate` — only their keys when
    /// `keys` — as the open transaction sees them, hand them to `build`, and write
    /// what it returns on the condition that `target` was not written after the
    /// read. Returns the written row count.
    ///
    /// Inside a transaction COMMIT checks the condition. In autocommit the statement
    /// is a transaction of its own, and a conflict re-reads and rebuilds, up to
    /// [`RMW_MAX_ATTEMPTS`] times.
    pub fn read_modify_write<E: From<ClientError>>(
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
            let ScanReply { batch, lsn, .. } = self.scan_spec(tid, &spec, &reply)?;
            let basis = lsn.expect("a server read carries its watermark");
            let mut own = TxnBuffer::default();
            let txn = self.txn.as_mut().unwrap_or(&mut own);
            let batch = build(txn.overlay(tid, schema, &spec, keys, batch)?)?;
            let count = batch.len();
            txn.push(tid, schema, batch, WireConflictMode::Update, basis)?;
            let pushed = match self.txn {
                Some(_) => Ok(0),
                None => self.push_txn(&own),
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

    /// Every catalog write goes through here, so this is where DDL is refused
    /// inside a transaction and where the copies learn what it committed.
    pub fn push_ddl_txn(&mut self, families: &[(u64, ZSetBatch)]) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::from("DDL is not allowed inside a transaction".to_string()));
        }
        self.round_trip(Request::DdlTxn(families))?;
        self.after_ddl_commit(families);
        Ok(())
    }

    /// Drop the copy of every view the bundle retracted, except a mirrored view
    /// the bundle also wrote back — a rename — which is rebound under its new
    /// name.
    fn after_ddl_commit(&mut self, families: &[(u64, ZSetBatch)]) {
        let Some(m) = self.mirror.as_deref() else {
            return;
        };
        let Some((_, b)) = families.iter().find(|(family, _)| *family == VIEW_TAB) else {
            return;
        };
        let vid = |i| b.pks.get(i) as u64;
        let mut dropped: Vec<u64> = Vec::new();
        let mut renamed: Vec<(String, String, Arc<RelDescriptor>)> = Vec::new();
        for v in (0..b.len()).filter(|&i| b.weights[i] < 0).map(vid) {
            match (m.views.get(&v), b.live_rows().find(|&j| vid(j) == v)) {
                (Some(view), Some(j)) => renamed.push((
                    view.schema_name.clone(),
                    gnitz_expr::payload_str(b, j, RELTAB_PAY_NAME).to_owned(),
                    Arc::clone(&view.desc),
                )),
                _ => dropped.push(v),
            }
        }
        for v in dropped {
            self.invalidate_own_copy(v);
        }
        for (schema_name, name, desc) in renamed {
            let tid = desc.tid;
            if self.bind(&schema_name, &name, desc).is_err() {
                self.invalidate_own_copy(tid);
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
            &mut BatchAppender::new(&mut batch),
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
            &mut BatchAppender::new(&mut sb),
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

    /// Register a table, its FOREIGN KEY columns and its inline UNIQUE indexes as
    /// one DDL bundle.
    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        fks: &[InlineForeignKey],
        props: TableProps,
        unique_indexes: &[InlineUniqueIndex],
    ) -> Result<u64, ClientError> {
        let table_name = gnitz_wire::canonical_identifier(table_name)?;
        let index_names: Vec<String> = unique_indexes
            .iter()
            .map(|spec| gnitz_wire::canonical_identifier(&spec.name))
            .collect::<Result<Vec<_>, String>>()?;
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        // Full schema-admissibility rule set (column cap + PK rules), applied
        // here so a caller that skipped the planner gets a clean error before
        // any id allocation instead of relying on the server-side reject (and
        // `pack_pk_cols` below can never panic).
        schema
            .validate()
            .map_err(|e| ClientError::from(format!("create_table: {e}")))?;
        // Before any id is allocated.
        let pk_cols = schema.pk_cols.iter().map(|&c| &schema.columns[c as usize]);
        props
            .validate(schema.pk_cols.len())
            .and_then(|()| match props.serial {
                true => gnitz_wire::validate_serial_key(pk_cols.map(|cd| (cd.name.as_str(), cd.ty))),
                false => Ok(()),
            })
            .map_err(|e| ClientError::from(format!("create_table: {e}")))?;

        for (j, fk) in fks.iter().enumerate() {
            let ci = fk.col_idx;
            if ci as usize >= schema.columns.len() {
                return Err(ClientError::from(format!(
                    "create_table: foreign key names column {ci}, past the table's {} columns",
                    schema.columns.len()
                )));
            }
            if fks[..j].iter().any(|prev| prev.col_idx == ci) {
                return Err(ClientError::from(format!(
                    "create_table: column {ci} carries more than one foreign key"
                )));
            }
        }

        for spec in unique_indexes {
            // Structural rules only (arity, in-range, no duplicates) — unlike a
            // PK, an indexed column may be nullable. They are `pack_pk_cols`'s
            // precondition.
            gnitz_wire::validate_pk_col_list(&spec.col_indices, schema.columns.len())
                .map_err(|msg| ClientError::from(format!("create_table: unique index '{}': {msg}", spec.name)))?;
        }

        let schema_id = self.lookup_schema_id(&schema_name)?;
        // The table's id, then one per inline UNIQUE index.
        let new_tid = self.alloc(IdRun::Ids(1 + unique_indexes.len() as u64))?;

        // Encode the PK list using the shared wire packer so the engine
        // catalog decodes it identically. Single-PK callers still flow
        // through the same packer; there is no second form of the word.
        let pk_packed = gnitz_wire::pack_pk_cols(&schema.pk_cols);

        // COL_TAB family — the server sorts families by topo priority, so it
        // ingests columns before the TABLE_TAB register hook that reads them.
        let col_s = sys_schema(COL_TAB);
        let mut col_batch = ZSetBatch::new(col_s);
        append_col_rows(&mut BatchAppender::new(&mut col_batch), new_tid, &schema.columns, fks);

        // TABLE_TAB family.
        let tbl_schema = sys_schema(TABLE_TAB);
        let mut tb = ZSetBatch::new(tbl_schema);
        gnitz_wire::sys_rows::write_table_tab_row(
            &mut BatchAppender::new(&mut tb),
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
                let mut a = BatchAppender::new(&mut idx_batch);
                for (k, spec) in unique_indexes.iter().enumerate() {
                    gnitz_wire::sys_rows::write_idx_tab_row(
                        &mut a,
                        &IdxTabRow {
                            index_id: new_tid + 1 + k as u64,
                            owner_id: new_tid,
                            source_col_idx: gnitz_wire::pack_pk_cols(&spec.col_indices),
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

    /// Create a passthrough view over `source_table_id` and return its id.
    pub fn create_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        source_table_id: u64,
        props: ViewProps,
    ) -> Result<u64, ClientError> {
        // A passthrough's layout must equal its source's, so the whole output
        // schema and its PK-repeat flag are the source's own.
        let src = self.describe_by_id(source_table_id)?;
        // A minimal SCAN_DELTA → INTEGRATE_SINK circuit, built through the typed
        // builder so the row materialisation matches the stored layout exactly.
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(source_table_id, ReadBound::None);
        circuit.sink(scan);

        let view = PlannedView {
            circuit,
            schema: Arc::clone(&src.schema),
            pk_repeats: src.pk_repeats,
        };
        self.create_view_chain(schema_name, view_name, view.into(), props, None)
    }

    /// Create `bundle` in one atomic `DDL_TXN` and return the user-named view's id.
    /// `bundle.view` takes `view_name` and `props`; each segment is named by
    /// [`segment_name`] and owned by it.
    ///
    /// `replace` is the id of the view this one supersedes in the same zone.
    pub fn create_view_chain(
        &mut self,
        schema_name: &str,
        view_name: &str,
        bundle: ViewBundle,
        props: ViewProps,
        replace: Option<u64>,
    ) -> Result<u64, ClientError> {
        let view_name = gnitz_wire::canonical_identifier(view_name)?;
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        // Reject a malformed chain before any allocation.
        let n_views = bundle.segments.len() + 1;
        if n_views > MAX_CHAIN_SEGMENTS {
            return Err(ClientError::from(format!(
                "view chain has {n_views} segments, exceeding the {MAX_CHAIN_SEGMENTS}-segment limit",
            )));
        }

        // Before any allocation, so a bad schema leaves no residue and never
        // reaches `pack_pk_cols`, which asserts on one.
        for (k, pv) in bundle.segments.iter().chain([&bundle.view]).enumerate() {
            pv.schema
                .validate()
                .map_err(|e| ClientError::from(format!("View '{view_name}' segment {k}: {e}")))?;
        }

        // Only the user-named view: the engine cascades its segments.
        let replaced = match replace {
            Some(vid) => {
                Some(self.seek_sys_row(VIEW_TAB, &[vid as u128], || not_found("view", &schema_name, &view_name))?)
            }
            None => None,
        };
        let schema_id = match &replaced {
            Some((row, i)) => payload_u64(row, *i, RELTAB_PAY_SCHEMA_ID),
            None => self.lookup_schema_id(&schema_name)?,
        };

        // The whole bundle is assigned in one allocation before any substitution
        // runs, because a downstream segment's `ScanDelta` names an upstream
        // segment by its position.
        let base = self.alloc(IdRun::Ids(n_views as u64))?;
        // The user-named view takes the id after every segment's, and every
        // segment names it as owner.
        let owner_vid = base + bundle.segments.len() as u64;

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
            let mut col_a = BatchAppender::new(&mut col_batch);
            let mut nodes_a = BatchAppender::new(&mut nodes_batch);
            let mut view_a = BatchAppender::new(&mut view_batch);

            let ViewBundle { segments, view } = bundle;
            let views = segments.into_iter().map(|pv| (pv, false)).chain([(view, true)]);
            for ((mut pv, is_view), vid) in views.zip(base..) {
                // 0.5. Substitute the bundle's symbolic ids. `base` is below the
                // ceiling, so this cannot overflow; a forward or out-of-range tag
                // becomes an id no lower than the view's own, which the engine
                // refuses.
                for src in pv.circuit.sources_mut() {
                    if *src >= gnitz_wire::CATALOG_ID_CEILING {
                        *src = base + (*src - gnitz_wire::CATALOG_ID_CEILING);
                    }
                }
                let (name, owner_view_id, row_props) = if is_view {
                    (view_name.clone(), 0, props)
                } else {
                    (segment_name(vid), owner_vid, ViewProps::default())
                };

                // 1. Column records. A foreign key constrains a base table, not a view.
                append_col_rows(&mut col_a, vid, &pv.schema.columns, &[]);

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
                        pk_col_idx: gnitz_wire::pack_pk_cols(&pv.schema.pk_cols),
                        props: row_props,
                        owner_view_id,
                        pk_repeats: pv.pk_repeats,
                    },
                    1,
                );
            }
        }

        // One families entry per tid: the engine refuses a second block per
        // family.
        let mut families: Vec<(u64, ZSetBatch)> = Vec::new();
        families.push((COL_TAB, col_batch));
        if !nodes_batch.is_empty() {
            families.push((CIRCUIT_NODES_TAB, nodes_batch));
        }
        families.push((VIEW_TAB, view_batch));

        self.push_ddl_txn(&families)?;
        Ok(owner_vid)
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
        for &raw in names {
            let name = gnitz_wire::canonical_identifier(raw)?;
            let Some((scanned, i)) = self.relation_retraction(family, noun, &schema_name, &name)? else {
                if if_exists {
                    continue;
                }
                return Err(not_found(noun, &schema_name, raw));
            };
            let id = scanned.pks.get(i) as u64;
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
        let scanned = self.sys_rows(family, ReadBound::None)?;
        for i in scanned.live_rows() {
            if payload_u64(&scanned, i, RELTAB_PAY_SCHEMA_ID) == schema_id {
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
        self.seek_sys_row(family, &[desc.tid as u128], || not_found(noun, schema_name, name))
            .map(Some)
    }

    /// Rename `rel`: a `(-1, +1)` rewrite pair of its TABLE_TAB / VIEW_TAB row.
    pub fn alter_rename_relation(&mut self, rel: &RelDescriptor, new_name: &str) -> Result<(), ClientError> {
        let new_name = gnitz_wire::canonical_identifier(new_name)?;
        let tid = rel.tid;
        let family = if rel.class.is_view() { VIEW_TAB } else { TABLE_TAB };
        self.rewrite_sys_row(
            family,
            &[tid as u128],
            || absent(format!("relation {tid} not found")),
            |b, row| b.set_string_cell(row, RELTAB_PAY_NAME, &new_name),
        )
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

    /// `ALTER TABLE … ADD COLUMN`: `def` appended to `rel` after every physical
    /// column, dropped ones included.
    pub fn alter_add_column(&mut self, rel: &RelDescriptor, def: &ColumnDef) -> Result<(), ClientError> {
        let (tid, col_idx) = (rel.tid, rel.schema.num_columns());

        let col_s = sys_schema(COL_TAB);
        let mut cb = ZSetBatch::new(col_s);
        {
            let mut a = BatchAppender::new(&mut cb);
            let row = ColTabRow {
                owner_id: tid,
                col_idx: col_idx as u64,
                col: def,
                fk: None,
            };
            gnitz_wire::sys_rows::write_col_tab_row(&mut a, &row, 1);
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
            &[tid as u128, col_idx as u128],
            || absent(format!("column index {col_idx} not found on table {tid}")),
            patch,
        )
    }

    /// Push a `(-1, +1)` rewrite pair on the live `family` row keyed `key`: the
    /// stored row at `-1`, and a copy of it at `+1` that `patch` edits in place.
    fn rewrite_sys_row(
        &mut self,
        family: u64,
        key: &[u128],
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

    /// Resolve `name` under `schema_name`, rejecting a missing relation. The
    /// erroring form of [`Self::resolve`], for the callers whose next step needs
    /// the relation to exist.
    pub fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(schema_name, name)?
            .ok_or_else(|| not_found("relation", schema_name, name))
    }

    // --- Relation resolution ---

    /// The descriptor for `schema_name.name`, or `None` when no such relation
    /// exists; `Err` is a missing schema or a decode error. Inside a transaction a
    /// table's name resolves once, so every statement writes it under one layout.
    pub fn resolve(&mut self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let qname = qualified_name(schema_name, name);
        if let Some(bound) = self.txn.as_ref().and_then(|t| t.bound.get(&qname)) {
            return Ok(Some(Arc::clone(bound)));
        }
        let found = self
            .round_trip(Request::Resolve(RelTarget::Name(&qname)))
            .map(Reply::into_resolve)?;
        if let (Some(txn), Some(desc)) = (self.txn.as_mut(), &found) {
            if desc.class == RelClass::Table {
                txn.bound.insert(qname, Arc::clone(desc));
            }
        }
        Ok(found)
    }

    // --- Private catalog-lookup helpers ---

    /// Resolve `schema_name` (already canonicalized) to its SCHEMA_TAB id. A
    /// missing row — or an entirely empty SCHEMA_TAB — is a
    /// `NotFound` refusal, like every other catalog absence.
    fn lookup_schema_id(&mut self, schema_name: &str) -> Result<u64, ClientError> {
        let batch = self.sys_rows(SCHEMA_TAB, ReadBound::None)?;
        find_schema_id(&batch, schema_name)?.ok_or_else(|| absent(format!("schema '{schema_name}' not found")))
    }

    /// System family `family`'s rows under `bound`, decoded under its own schema;
    /// the server checks the reply against that schema's layout.
    fn sys_rows(&mut self, family: u64, bound: ReadBound) -> Result<ZSetBatch, ClientError> {
        Ok(self
            .scan_spec(family, &ReadSpec::all_rows(bound), sys_schema(family))?
            .batch)
    }

    /// The live `family` row keyed `key` — its PK columns' native values in
    /// PK-list order — by one master-local keyed read: the
    /// reply batch and the row's index in it, for a caller to copy the stored row
    /// out of. `PkSet` is exact, so a live row in the reply is that key's; none is
    /// `missing()`.
    fn seek_sys_row(
        &mut self,
        family: u64,
        key: &[u128],
        missing: impl FnOnce() -> ClientError,
    ) -> Result<(ZSetBatch, usize), ClientError> {
        let mut keys = PkColumn::empty_for_schema(sys_schema(family));
        keys.push_natives(key);
        let batch = self.sys_rows(family, ReadBound::PkSet(keys.keys()))?;
        let i = batch.live_rows().next().ok_or_else(missing)?;
        Ok((batch, i))
    }
}

/// Step and park until `slot` completes, handing `sink` what the steps read of
/// a delta poll.
fn await_slot(
    session: &mut Session,
    hook: &mut Option<ParkHook>,
    slot: SlotId,
    mut sink: Option<&mut PollSink<'_>>,
) -> Result<Reply, ClientError> {
    let mut ready = Interest::WRITE;
    loop {
        let mut done = session.step_polling(ready, sink.as_deref_mut());
        if let Some(i) = done.iter().position(|(s, _)| *s == slot) {
            return done.swap_remove(i).1;
        }
        ready = park(session, hook)?;
    }
}

/// One view's delta read, handing `on_block` each block as its frame arrives.
/// Returns the terminal's `(tag, T)` as a cursor, unchecked against a previous
/// one, or the first error `on_block` returned.
pub(crate) fn delta_read_blocks(
    session: &mut Session,
    hook: &mut Option<ParkHook>,
    item: DeltaPollItem,
    mut on_block: impl FnMut(RawBlock) -> Result<(), ClientError>,
) -> Result<DeltaCursor, ClientError> {
    let slot = session.submit_delta_poll(&[item])?;
    let mut end: Option<PollEnd> = None;
    let mut sink = |s: SlotId, polled: Polled| {
        let refused = matches!(end, Some(Err(_)));
        if s != slot || refused {
            return;
        }
        end = match polled {
            Polled::Block(b) => on_block(b).err().map(Err),
            Polled::End(e) => Some(e),
        };
    };
    // A per-view fault ends the position and completes the slot `Ok`; a
    // frame-level rejection completes it `Err` with no position ended.
    await_slot(session, hook, slot, Some(&mut sink))?;
    end.expect("a one-view poll completes by ending its position")
}

/// Wait for the session's interest, running `hook` on every `EINTR`.
pub(crate) fn park(session: &Session, hook: &mut Option<ParkHook>) -> Result<Interest, ClientError> {
    let interest = session.interest();
    assert!(!interest.is_empty(), "park with nothing outstanding");
    loop {
        match poll_fd(session.as_raw_fd(), interest.poll_events(), None) {
            Ok(revents) => return Ok(Interest::from_revents(revents)),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                if let Some(hook) = hook.as_mut() {
                    hook().map_err(|e| ClientError::Interrupted(e.into()))?;
                }
            }
            Err(e) => return Err(e.into()),
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
    /// The oldest watermark among the reads this family's writes were built
    /// from; `BLIND` when none was.
    basis: u64,
    /// How many of `batch`'s rows are already folded into `last_op_of`. A batch
    /// only ever extends, so this is a watermark, not a dirty flag.
    indexed: usize,
}

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
    /// Qualified name → the descriptor [`GnitzClient::resolve`] found for it.
    bound: HashMap<String, Arc<RelDescriptor>>,
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
        tid: u64,
        schema: &Schema,
        batch: ZSetBatch,
        mode: WireConflictMode,
        basis: u64,
    ) -> Result<(), ClientError> {
        batch
            .layout_matches(schema)
            .map_err(|e| ClientError::from(format!("relation {tid}: the batch is not in its schema's layout: {e}")))?;
        if batch.is_empty() {
            return Ok(());
        }
        self.check_layout(tid, |held| batch.layout_matches(&held.schema))?;
        // Copied out, so no borrow of `families_of` spans the `families` read.
        let last = self.families_of.get(&tid).and_then(|v| v.last().copied());
        match last.filter(|&i| self.families[i].mode == mode) {
            Some(i) => {
                let f = &mut self.families[i];
                f.batch.extend_from_owned(batch);
                f.basis = f.basis.min(basis);
            }
            None => {
                self.families_of.entry(tid).or_default().push(self.families.len());
                self.families.push(BufferedFamily {
                    tid,
                    schema: schema.clone(),
                    batch,
                    mode,
                    basis,
                    indexed: 0,
                });
            }
        }
        Ok(())
    }

    /// `matches` over `tid`'s first family; every later one shares its layout.
    fn check_layout(
        &self,
        tid: u64,
        matches: impl FnOnce(&BufferedFamily) -> Result<(), String>,
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
            let f = &mut self.families[fam];
            for row in f.indexed..f.batch.len() {
                if f.batch.weights[row] != 0 {
                    index.insert(PkBuf::from_bytes(f.batch.pks.get_bytes(row)), (fam, row));
                }
            }
            f.indexed = f.batch.len();
        }
    }

    /// `committed`, a server read of `tid` under `spec`, as this transaction sees
    /// it: less every PK the transaction wrote, plus its live rows (last op per PK,
    /// weight > 0) that `spec` keeps. With `keys`, `committed` is in the
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
                live.copy_row_at(b, row, b.weights[row]);
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

/// `Ok(None)` = name absent (a legitimate miss); `Err` = a decode error on a
/// corrupt catalog batch.
fn find_schema_id(batch: &ZSetBatch, name: &str) -> Result<Option<u64>, ClientError> {
    for i in batch.live_rows() {
        if col_str(batch, SCHEMATAB_PAY_NAME, i)? == name {
            return Ok(Some(batch.pks.get(i) as u64));
        }
    }
    Ok(None)
}

/// Append one `COL_TAB` row per column of `owner_id`, at `+1`.
fn append_col_rows(a: &mut BatchAppender<'_>, owner_id: u64, columns: &[ColumnDef], fks: &[InlineForeignKey]) {
    for (i, cd) in columns.iter().enumerate() {
        let row = ColTabRow {
            owner_id,
            col_idx: i as u64,
            col: cd,
            fk: fks
                .iter()
                .find(|fk| fk.col_idx as usize == i)
                .map(|fk| fk.target.resolve(owner_id)),
        };
        gnitz_wire::sys_rows::write_col_tab_row(a, &row, 1);
    }
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;
