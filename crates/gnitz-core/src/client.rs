use crate::connection::{
    DeltaCursor, Encoded, Interest, Polled, RawBlock, RelDescriptor, Reply, Request, ScanReply, Session, SlotId, Target,
};
use crate::error::ClientError;
use crate::protocol::transport::poll_fd;
use crate::{sys_schema, BatchAppender, PkColumn, ProtocolError, PushFamily, Schema, ZSetBatch};
use gnitz_expr::{ColumnTable, SchemaFacts};
use gnitz_wire::{ColumnDef, PkBuf, PkKeys, WireConflictMode};
use gnitz_wire::{WireFault, WireStatus};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use gnitz_expr::{LogicalProgram, RowFilter};
use gnitz_wire::sys_rows::{
    CircuitRow, ColTabRow, ColTabSlot, FkRef, IdxTabRow, SchemaTabRow, SchemaTabSlot, SysRow, TableTabRow, ViewTabRow,
};
use gnitz_wire::txn_frame::{DeltaPollItem, BLIND, DELTA_POLL_MAX_VIEWS};
use gnitz_wire::{payload_bytes, payload_str, payload_u64};
use gnitz_wire::{Circuit, ComputeMap, ReadBound, ReadSink, ReadSpec};
use gnitz_wire::{
    PkColList, PkListRole, TableProps, ViewProps, COL_TAB, IDX_TAB, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, SCHEMA_TAB,
    TABLE_TAB, VIEW_TAB,
};

// --- Module-private helpers ---

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

impl From<PlannedView> for ViewBundle {
    fn from(view: PlannedView) -> Self {
        ViewBundle { segments: Vec::new(), view }
    }
}

/// Where [`GnitzClient::held`] found a descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Held {
    /// A mirrored registration's, while its copy answers reads: as stale as the
    /// copy, and checked by nothing.
    Copy,
    /// What the last RESOLVE answered. Only a request carrying its token finds
    /// out whether it is stale; a planning error or an answer that needs no
    /// request is the caller's to repeat from [`GnitzClient::resolve`].
    Kept,
}

/// Run when a signal interrupts a blocking call's wait; an `Err` aborts the call.
/// The Python binding checks for Ctrl-C here.
pub type ParkHook = Box<dyn FnMut() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send>;

pub struct GnitzClient {
    pub(crate) session: Session,
    pub(crate) park_hook: Option<ParkHook>,
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
    /// [`Self::held`]. This client's own DDL empties it.
    kept: HashMap<String, Arc<RelDescriptor>>,
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
            kept: HashMap::new(),
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
        await_slot(&mut self.session, &mut self.park_hook, slot)
    }

    /// Reserve `count` contiguous SERIAL ids for `table` and return the first,
    /// so an INSERT that knows its row count pays one fsynced durable advance
    /// rather than `ceil(count / SERIAL_RANGE_SIZE)`. An abandoned tail — the old
    /// range's, or this reservation's — is the intentional PostgreSQL-style gap.
    pub fn reserve_serial_ids(&mut self, table: &RelDescriptor, count: u64) -> Result<u64, ClientError> {
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
                    .round_trip(Request::AllocSerial { table: table.into(), count: want })?
                    .into_ack();
                self.serial_cache.insert(table.tid, base + count..base + want);
                Ok(base)
            }
        }
    }

    // --- Raw ops ---

    /// Allocate a run of `n` catalog object ids, returning its first.
    fn alloc_ids(&mut self, n: u64) -> Result<u64, ClientError> {
        self.round_trip(Request::AllocIds(n)).map(Reply::into_ack)
    }

    /// Allocate one catalog object id (schema, relation or index).
    pub fn alloc_id(&mut self) -> Result<u64, ClientError> {
        self.alloc_ids(1)
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
    ) -> Result<u64, ClientError> {
        let (target, batch) = (target.into(), batch.into());
        if let Some(txn) = &mut self.txn {
            txn.push(target, schema, batch.into_owned(), mode, BLIND)?;
            return Ok(0);
        }
        let batch = &*batch;
        Ok(self
            .round_trip(Request::Push { target, schema, batch, mode })?
            .into_ack())
    }

    /// Run a parameterized bounded read, replied in `reply_schema`'s layout.
    pub fn scan_spec(
        &mut self,
        target: impl Into<Target>,
        spec: &ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<ScanReply, ClientError> {
        let target = target.into();
        self.round_trip(Request::ScanSpec { target, spec, reply_schema })
            .map(Reply::into_scan)
    }

    // ── The read seam ──────────────────────────────────────────────────────
    //
    // `resolve` and `scan_spec` are the connection; `held` and
    // `scan_spec_local_first` below consult the copy first, so every call site
    // declares which freshness it is asking for. **The gate is what the copy
    // holds, never whether a store is attached**, so a client with one reads
    // exactly like a client without for every relation the copy does not hold.

    /// The descriptor this client holds for `schema_name.name`. A `read` is
    /// answered off a mirrored registration while its copy answers reads — the
    /// cursor is the one gate, for names and reads alike — and anything else off
    /// what the last RESOLVE answered.
    pub fn held(&self, schema_name: &str, name: &str, read: bool) -> Option<(Arc<RelDescriptor>, Held)> {
        let copy = self.mirror.as_deref().filter(|_| read).and_then(|m| {
            m.views
                .iter()
                // The stored names are canonical, so comparing the parts is the
                // fold of the joined name without its allocation.
                .find(|(&t, v)| {
                    v.schema_name.eq_ignore_ascii_case(schema_name)
                        && v.name.eq_ignore_ascii_case(name)
                        && m.store.cursor_of(t).is_some()
                })
                .map(|(_, v)| (Arc::clone(&v.desc), Held::Copy))
        });
        copy.or_else(|| Some((self.kept.get(&qualified_name(schema_name, name))?.clone(), Held::Kept)))
    }

    /// [`Self::scan_spec`], answered off the copy when it holds `target`, with
    /// no served LSN: a copy's freshness is [`Self::cursor_of`].
    pub fn scan_spec_local_first(
        &mut self,
        target: impl Into<Target>,
        spec: ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<ScanReply, ClientError> {
        let target = target.into();
        match self.mirror.as_deref_mut() {
            Some(m) if m.cursor_of(target.tid).is_some() => Ok(ScanReply {
                batch: m.store.scan_spec(target.tid, spec, reply_schema)?,
                schema: Arc::clone(reply_schema),
                lsn: None,
            }),
            _ => self.scan_spec(target, &spec, reply_schema),
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

    /// Consistent snapshot of N relations at one server-side SAL cut, returned
    /// in request order. An atomic multi-table `txn_commit` is never observed torn
    /// across the result set.
    /// Each relation is replied in the layout of the schema paired with it.
    pub fn scan_many(&mut self, relations: Vec<(u64, Arc<Schema>)>) -> Result<Vec<ScanReply>, ClientError> {
        self.round_trip(Request::ScanMulti(relations)).map(Reply::into_multi)
    }

    /// Index `cols` of relation `owner_id`, in that order, under the catalog name
    /// `index_name`.
    pub fn create_index(
        &mut self,
        owner_id: u64,
        cols: PkColList,
        index_name: &str,
        is_unique: bool,
    ) -> Result<u64, ClientError> {
        let index_name = gnitz_wire::canonical_identifier(index_name)?;
        let index_id = self.alloc_id()?;
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
        self.commit_ddl(b)?;
        Ok(index_id)
    }

    /// Drop indexes by name as **one** DDL zone: the whole set retires or none of
    /// it does, and a name repeated in `index_names` retires once.
    pub fn drop_indexes_by_name(&mut self, index_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(index_names, "index", if_exists, |_| true)
    }

    /// `ALTER TABLE … DROP CONSTRAINT`: the UNIQUE index `name` of table `tid`.
    /// The `-1` is the stored row, so the engine's CAS re-proves owner and
    /// uniqueness against the live row.
    pub fn drop_unique_constraint(&mut self, tid: u64, name: &str, if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(&[name], "constraint", if_exists, |r| r.owner == tid && r.is_unique)
    }

    /// Retract the live IDX_TAB rows named in `names` that pass `matches`.
    fn drop_index_rows(
        &mut self,
        names: &[&str],
        noun: &'static str,
        if_exists: bool,
        matches: impl Fn(&IndexRow) -> bool,
    ) -> Result<(), ClientError> {
        let scanned = self.sys_rows(IDX_TAB, ReadBound::None)?;
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
        self.commit_ddl(b)
    }

    /// Every live secondary index. Names come back canonical (lowercase): every
    /// writer folds them at store time.
    pub fn index_rows(&mut self) -> Result<Vec<IndexRow>, ClientError> {
        let scanned = self.sys_rows(IDX_TAB, ReadBound::None)?;
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
    pub fn txn_commit(&mut self) -> Result<u64, ClientError> {
        let buf = self.txn.take().ok_or_else(no_transaction)?;
        self.push_txn(&buf).map_err(|e| match e {
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
    fn push_txn(&mut self, buf: &TxnBuffer) -> Result<u64, ClientError> {
        if buf.families.is_empty() {
            return Ok(0);
        }
        let families: Vec<PushFamily<'_>> = buf
            .families
            .iter()
            .map(|f| PushFamily {
                target: f.target,
                schema: &f.schema,
                batch: &f.batch,
                mode: f.mode,
                basis: f.basis,
            })
            .collect();
        self.round_trip(Request::PushTxn { families: &families })
            .map(Reply::into_ack)
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
                    let ScanReply { batch, lsn, .. } =
                        self.scan_spec(target, narrowed.as_ref().unwrap_or(&spec), &reply)?;
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

    /// Commit `bundle` as one DDL zone. One that holds no row opens no zone.
    fn commit_ddl(&mut self, bundle: DdlBundle) -> Result<(), ClientError> {
        if bundle.0.is_empty() {
            return Ok(());
        }
        if self.txn.is_some() {
            return Err(ClientError::from("DDL is not allowed inside a transaction".to_string()));
        }
        self.round_trip(Request::DdlTxn(&bundle.0))?;
        self.kept.clear();
        self.after_ddl_commit(&bundle.0);
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
                    payload_str(b, j, RELTAB_PAY_NAME)
                        .expect("a bundle this client built names its views in UTF-8")
                        .to_owned(),
                    Arc::clone(&view.desc),
                )),
                _ => dropped.push(v),
            }
        }
        for v in dropped {
            let _ = self.forget_view(v);
        }
        for (schema_name, name, desc) in renamed {
            let tid = desc.tid;
            if self.bind(&schema_name, &name, desc).is_err() {
                let _ = self.forget_view(tid);
            }
        }
    }

    pub fn create_schema(&mut self, name: &str) -> Result<u64, ClientError> {
        // Refuses the empty string, a leading `_` (the reserved system prefix)
        // and illegal characters.
        let name = gnitz_wire::canonical_identifier(name)?;
        let schema_id = self.alloc_id()?;
        let mut b = DdlBundle::default();
        b.put(&SchemaTabRow { schema_id, name: &name }, 1);
        self.commit_ddl(b)?;
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
    pub fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
        let name = gnitz_wire::canonical_identifier(name)?;
        let (schemas, at) = self.lookup_schema(&name)?;
        let schema_id = schemas.pks.get(at) as u64;

        // Hidden segments need no separate pass: each is an ordinary VIEW_TAB row
        // carrying this `schema_id`, so the whole matching set is already complete
        // — and the engine's co-drop carve-out admits it, every dependent being in
        // the same drop set.
        let mut b = DdlBundle::default();
        for family in [VIEW_TAB, TABLE_TAB] {
            let scanned = self.sys_rows(family, ReadBound::None)?;
            for i in scanned.live_rows() {
                if payload_u64(&scanned, i, RELTAB_PAY_SCHEMA_ID) == schema_id {
                    b.batch(family).copy_row_at(&scanned, i, -1);
                }
            }
        }
        b.batch(SCHEMA_TAB).copy_row_at(&schemas, at, -1);
        self.commit_ddl(b)
    }

    /// Register a table, its FOREIGN KEY columns and its inline UNIQUE indexes as
    /// one DDL bundle. `fks[i]` is column `i`'s target; an empty `fks` gives no
    /// column one.
    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        fks: &[Option<FkTarget>],
        props: TableProps,
        unique_indexes: &[InlineUniqueIndex],
    ) -> Result<u64, ClientError> {
        let table_name = gnitz_wire::canonical_identifier(table_name)?;
        let index_names: Vec<String> = unique_indexes
            .iter()
            .map(|spec| gnitz_wire::canonical_identifier(&spec.name))
            .collect::<Result<Vec<_>, String>>()?;
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
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

        let (schemas, at) = self.lookup_schema(&schema_name)?;
        let schema_id = schemas.pks.get(at) as u64;
        // The table's id, then one per inline UNIQUE index.
        let new_tid = self.alloc_ids(1 + unique_indexes.len() as u64)?;

        let mut b = DdlBundle::default();
        append_col_rows(&mut b, new_tid, &schema.columns, fks);
        b.put(
            &TableTabRow {
                table_id: new_tid,
                schema_id,
                name: &table_name,
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
        self.commit_ddl(b)?;

        Ok(new_tid)
    }

    /// Drop tables as one DDL zone; the engine cascades each one's indexes off
    /// their owner. See [`Self::drop_relations`] for the batch rules.
    pub fn drop_table(&mut self, schema_name: &str, table_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(TABLE_TAB, "table", schema_name, table_names, if_exists)
    }

    /// Create a passthrough view over `source` and return its id.
    pub fn create_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
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
        let view = PlannedView {
            circuit,
            schema: Arc::clone(&source.schema),
            pk_repeats: source.pk_repeats,
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
        // reaches `PkColList::from_slice`, which panics on one.
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
            None => {
                let (schemas, at) = self.lookup_schema(&schema_name)?;
                schemas.pks.get(at) as u64
            }
        };

        // The whole bundle is assigned in one allocation before any substitution
        // runs, because a downstream segment's `ScanDelta` names an upstream
        // segment by its position.
        let base = self.alloc_ids(n_views as u64)?;
        // The user-named view takes the id after every segment's, and every
        // segment names it as owner.
        let owner_vid = base + bundle.segments.len() as u64;

        let mut b = DdlBundle::default();
        // The replaced view's retraction, ahead of the new chain's `+1`s.
        if let Some((scanned, i)) = &replaced {
            b.batch(VIEW_TAB).copy_row_at(scanned, *i, -1);
        }

        let ViewBundle { segments, view } = bundle;
        let views = segments.into_iter().map(|pv| (pv, false)).chain([(view, true)]);
        for ((mut pv, is_view), vid) in views.zip(base..) {
            // Substitute the bundle's symbolic ids. `base` is below the ceiling,
            // so this cannot overflow; a forward or out-of-range tag becomes an
            // id no lower than the view's own, which the engine refuses.
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

            // A foreign key constrains a base table, not a view.
            append_col_rows(&mut b, vid, &pv.schema.columns, &[]);
            let circuit = pv.circuit.encode();
            b.put(&CircuitRow { view_id: vid, circuit: &circuit }, 1);
            let (capacity_bytes, delta_bytes) = row_props.row_words();
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

        self.commit_ddl(b)?;
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
        let mut b = DdlBundle::default();
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
                b.batch(family).copy_row_at(&scanned, i, -1);
            }
        }
        self.commit_ddl(b)
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
        let (scanned, i) = self.seek_sys_row(family, &[desc.tid as u128], || not_found(noun, schema_name, name))?;
        // Renamed since the resolve: the name no longer denotes this relation.
        if payload_bytes(&scanned, i, RELTAB_PAY_NAME) != name.as_bytes() {
            return Ok(None);
        }
        Ok(Some((scanned, i)))
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
        self.alter_col_pair(tid, col_idx, |b, row| {
            b.set_string_cell(row, ColTabSlot::name as usize, new_col)
        })
    }

    /// `ALTER TABLE … DROP COLUMN`: the column stays physically present, so the
    /// table keeps its layout and comparator.
    pub fn alter_drop_column(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| {
            b.set_u64_cell(row, ColTabSlot::is_hidden as usize, 1)
        })
    }

    /// `ALTER TABLE … ALTER COLUMN … DROP NOT NULL`: a `(-1, +1)` COL_TAB rewrite
    /// pair, only `is_nullable` flipped to true at `+1`. Once the catalog reports
    /// the column nullable, `ZSetBatch::validate` permits a null bit there.
    pub fn alter_drop_not_null(&mut self, tid: u64, col_idx: usize) -> Result<(), ClientError> {
        self.alter_col_pair(tid, col_idx, |b, row| {
            b.set_u64_cell(row, ColTabSlot::is_nullable as usize, 1)
        })
    }

    /// `ALTER TABLE … ADD COLUMN`: `def` appended to `rel` after every physical
    /// column, dropped ones included.
    pub fn alter_add_column(&mut self, rel: &RelDescriptor, def: &ColumnDef) -> Result<(), ClientError> {
        let (tid, col_idx) = (rel.tid, rel.schema.num_columns());

        let mut b = DdlBundle::default();
        b.put(&ColTabRow::of(tid, col_idx as u64, def, None), 1);
        self.commit_ddl(b)
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
        let mut b = DdlBundle::default();
        let rows = b.batch(family);
        rows.copy_row_at(&scanned, i, -1);
        rows.copy_row_at(&scanned, i, 1);
        patch(rows, 1);
        self.commit_ddl(b)
    }

    /// Resolve `name` under `schema_name`, rejecting a missing relation. The
    /// erroring form of [`Self::resolve`], for the callers whose next step needs
    /// the relation to exist.
    pub fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<Arc<RelDescriptor>, ClientError> {
        self.resolve(schema_name, name)?
            .ok_or_else(|| not_found("relation", schema_name, name))
    }

    // --- Relation resolution ---

    /// The descriptor for `schema_name.name` as the server answers it now, or
    /// `None` when no such relation exists; `Err` is a missing schema or a decode
    /// error.
    pub fn resolve(&mut self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let qname = qualified_name(schema_name, name);
        let found = self.round_trip(Request::Resolve(&qname)).map(Reply::into_resolve)?;
        match &found {
            Some(desc) => self.kept.insert(qname, Arc::clone(desc)),
            None => self.kept.remove(&qname),
        };
        Ok(found)
    }

    // --- Private catalog-lookup helpers ---

    /// The live SCHEMA_TAB row named `schema_name` (already canonical): the scanned
    /// batch and the row's index in it.
    fn lookup_schema(&mut self, schema_name: &str) -> Result<(ZSetBatch, usize), ClientError> {
        let batch = self.sys_rows(SCHEMA_TAB, ReadBound::None)?;
        let i = batch
            .live_rows()
            .find(|&i| payload_bytes(&batch, i, SchemaTabSlot::name as usize) == schema_name.as_bytes())
            .ok_or_else(|| absent(format!("schema '{schema_name}' not found")))?;
        Ok((batch, i))
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

/// Step and park until `slot` completes.
pub(crate) fn await_slot(
    session: &mut Session,
    hook: &mut Option<ParkHook>,
    slot: SlotId,
) -> Result<Reply, ClientError> {
    let mut ready = Interest::WRITE;
    loop {
        let mut done = session.step(ready);
        if let Some(i) = done.iter().position(|(s, _)| *s == slot) {
            return done.swap_remove(i).1;
        }
        ready = park(session, hook)?;
    }
}

/// Poll `items`, one request per `DELTA_POLL_MAX_VIEWS`, handing `on` each
/// item's blocks and then its one end, the items in order. `Err` is an
/// interrupt alone.
pub(crate) fn poll_deltas(
    session: &mut Session,
    hook: &mut Option<ParkHook>,
    items: &[DeltaPollItem],
    mut on: impl FnMut(usize, Polled),
) -> Result<(), ClientError> {
    let mut slots = Vec::new();
    let mut unsent = None;
    for chunk in items.chunks(DELTA_POLL_MAX_VIEWS) {
        match Encoded::delta_poll(chunk).and_then(|poll| session.enqueue(poll)) {
            Ok(slot) => slots.push(slot),
            // Neither of `enqueue`'s refusals clears without a step, so no
            // later chunk is tried.
            Err(e) => {
                unsent = Some(e);
                break;
            }
        }
    }
    let sent = items.len().min(slots.len() * DELTA_POLL_MAX_VIEWS);
    // The session answers slots in submit order and a slot's views in request
    // order, each exactly once: the ends counted so far name the item.
    let mut answered = 0;
    let mut ready = Interest::WRITE;
    while answered < sent {
        let mut sink = |slot: SlotId, polled: Polled| {
            // By slot, so a train an abandoned call left behind is not taken
            // for one of this call's.
            if slots.contains(&slot) {
                let item = answered;
                answered += usize::from(matches!(polled, Polled::End(_)));
                on(item, polled);
            }
        };
        session.step_polling(ready, Some(&mut sink));
        if answered < sent {
            ready = park(session, hook)?;
        }
    }
    if let Some(e) = unsent {
        (sent..items.len()).for_each(|item| on(item, Polled::End(Err(e.clone()))));
    }
    Ok(())
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
    let mut refused = None;
    let mut end = None;
    poll_deltas(session, hook, &[item], |_, polled| match polled {
        Polled::Block(b) if refused.is_none() => refused = on_block(b).err(),
        Polled::Block(_) => {}
        Polled::End(e) => end = Some(e),
    })?;
    refused.map_or(end.expect("one item ends once"), Err)
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
    /// The relation, under the descriptor token of the first of the family's
    /// batches built from a descriptor; `0` while none was.
    target: Target,
    schema: Arc<Schema>,
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
                self.families.push(BufferedFamily {
                    target,
                    schema: Arc::clone(schema),
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
