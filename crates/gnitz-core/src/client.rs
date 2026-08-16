use crate::connection::{
    MultiScanResult, RelTarget, ScanResult, Session, COL_TAB, IDX_TAB, SCHEMA_TAB, TABLE_TAB, VIEW_TAB,
};
use crate::error::ClientError;
use crate::protocol::{
    BatchAppender, ColData, ColumnDef, PkColumn, PkTuple, Schema, TypeCode, WireConflictMode, ZSetBatch,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::circuit::Circuit;
use crate::types::sys_schema;
use gnitz_wire::sys_rows::{ColTabRow, IdxTabRow, TableTabRow, ViewTabRow};
use gnitz_wire::{
    CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB, IDXTAB_COL_IS_UNIQUE, IDXTAB_COL_NAME,
    IDXTAB_COL_OWNER_ID, IDXTAB_COL_SOURCE_COLS, OWNER_KIND_TABLE, OWNER_KIND_VIEW, SCHEMATAB_COL_NAME,
    TABTAB_COL_FLAGS, TABTAB_COL_NAME, TABTAB_COL_PK_COL_IDX, TABTAB_COL_SCHEMA_ID, VIEWTAB_COL_NAME,
    VIEWTAB_COL_PK_COL_IDX, VIEWTAB_COL_SCHEMA_ID, VIEWTAB_COL_SQL,
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

/// Canonical stored form of a user relation/schema/index name. Names are ASCII
/// `[A-Za-z0-9_]` (enforced by `validate_user_identifier` at this client's
/// create entry points, so no front end can bypass it), so an ASCII lowercase
/// is a total, collision-free fold and the single definition of catalog-name
/// case-insensitivity. Folded at the two boundaries every catalog name flows
/// through — this client (the catalog gateway) and the binder cache — so the
/// engine only ever sees already-canonical names and needs no production change.
fn canon_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// The canonical `"schema.relation"` key — the engine's `entity_by_qname` key
/// and the statement memo's. Built in one allocation, not three.
fn qualified_name(schema_name: &str, name: &str) -> String {
    let mut q = String::with_capacity(schema_name.len() + 1 + name.len());
    q.push_str(schema_name);
    q.push('.');
    q.push_str(name);
    q.make_ascii_lowercase();
    q
}

/// A secondary index maps the indexed column to the PK of the index table, so
/// the indexed column must be PK-eligible. Defer to the canonical
/// `is_pk_eligible` allow-list (the exact set the server's `get_index_key_type`
/// accepts) rather than a deny-list: any future `TypeCode` is index-ineligible
/// until explicitly vetted, instead of silently slipping through.
fn validate_index_col_type(tc: TypeCode) -> Result<(), ClientError> {
    if !tc.is_pk_eligible() {
        return Err(ClientError::ServerError(
            "index on this column type is not supported (must be an integer scalar)".to_string(),
        ));
    }
    Ok(())
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

// --- Internal record types ---

struct TableRecord {
    tid: u64,
    schema_id: u64,
    pk_col_idx: u64,
    flags: u64,
}

struct ViewRecord {
    vid: u64,
    schema_id: u64,
    name: String,
    sql_definition: String,
    pk_col_idx: u64,
}

/// `is_unique` stays the stored word rather than a `bool`, so a DROP echoes
/// back exactly what it read.
struct IndexRecord {
    index_id: u64,
    owner_id: u64,
    source_cols: u64,
    name: String,
    is_unique: u64,
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

/// Number of SERIAL ids reserved per master round-trip. The range cache is
/// load-bearing, not a micro-optimization: each reservation is a
/// `catalog_rwlock`-serialized, fsync'd durable advance on the master, so
/// caching amortizes that one fsync across `SERIAL_RANGE_SIZE` inserts.
const SERIAL_RANGE_SIZE: u64 = 64;

/// Upper bound on the number of segments in one atomic view chain. Bounds the
/// pathological self-referential-CTE blow-up (`WITH b AS (SELECT * FROM a JOIN a)
/// …` doubling per level) with a clean planner error rather than an unbounded
/// bundle. Enforced by `create_view_chain`.
pub const MAX_CHAIN_SEGMENTS: usize = 64;

/// The name of the `idx`-th hidden segment view owned by the user view with id
/// `owner_vid`. Ownership is name-encoded: `drop_view` cascades over
/// [`hidden_view_prefix`], so producer (planner) and consumer (drop) must share
/// this one definition.
pub fn hidden_view_name(owner_vid: u64, idx: usize) -> String {
    format!("{}{idx}", hidden_view_prefix(owner_vid))
}

/// The name prefix every hidden segment of `owner_vid` carries. The trailing
/// separator keeps `__h5_` from matching `__h51_0`.
fn hidden_view_prefix(owner_vid: u64) -> String {
    format!("__h{owner_vid}_")
}

/// Whether `name` is a synthesized hidden segment view (any owner). Sound
/// because user identifiers cannot start with `_` (`validate_user_identifier`).
fn is_hidden_view_name(name: &str) -> bool {
    name.starts_with("__h")
}

/// One view in a [`GnitzClient::create_view_chain`] bundle. The `circuit`'s
/// `view_id` names the view's allocated id when non-zero — a chain pre-allocates
/// every segment's id so a downstream circuit can `ScanDelta` an upstream hidden
/// view; a zero id is allocated by `create_view_chain`.
pub struct PlannedView {
    pub name: String,
    pub sql_text: String,
    pub circuit: Circuit,
    pub output_columns: Vec<ColumnDef>,
    pub pk_cols: Vec<u32>,
}

/// Everything a statement needs to know about one relation. Statement-scoped:
/// built by a resolve, dropped at `end_statement`, never carried across. That is
/// what makes a stale name → id binding unreachable rather than merely checked —
/// there is nothing retained to go stale, so nothing to invalidate.
struct RelDescriptor {
    tid: u64,
    is_view: bool,
    replicated: bool,
    schema: Arc<Schema>,
    indexes: Arc<Vec<IndexMeta>>,
}

/// What one statement has already read, dropped whole at `end_statement`.
///
/// `relations` holds one entry per resolved canonical `"schema.name"`, absent
/// verdicts included, so a two-probe error ladder costs one round trip rather
/// than two. `catalog` holds one scan batch per system table for the reads that
/// still go through the wire (`lookup_schema_id`, the DDL `-1` payload reads);
/// a family this statement also *writes* must bypass it, or the write's own
/// retraction reads back as still live.
#[derive(Default)]
struct StatementScope {
    relations: HashMap<String, Option<Arc<RelDescriptor>>>,
    catalog: HashMap<u64, Option<Arc<ZSetBatch>>>,
}

pub struct GnitzClient {
    session: Session,
    serial_cache: HashMap<u64, SerialRange>,
    /// What the current statement has resolved and scanned; `None` outside a
    /// statement, so a read issued between statements always hits the wire.
    scope: Option<StatementScope>,
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
}

// The client-facing types must stay `Send`, so nothing reachable from them may
// hold an `Rc`. The requirement comes from the Python binding: every blocking
// call runs inside `Python::detach`, whose `Ungil` bound resolves to `Send` on
// both the closure (capturing `&mut GnitzClient`) and its return value — and the
// async transport moves a bare `Session` into its I/O thread. An `Rc` anywhere in
// that reachable set breaks the build in `gnitz-py`, a crate away, with an error
// naming a pyo3 trait rather than the field. Asserted here so the failure lands
// on the line that caused it.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send::<GnitzClient>();
    assert_send::<Session>();
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
        })
    }

    /// Request frames this connection has written. Exposed for the
    /// round-trip-count assertions; see [`Session::requests_sent`].
    pub fn requests_sent(&self) -> u64 {
        self.session.requests_sent()
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

    /// Open a statement scope: everything this statement resolves or scans is
    /// remembered in it and dropped at [`Self::end_statement`], so nothing
    /// survives a catalog write and there is no cross-statement state to
    /// invalidate. The SQL planner brackets each statement with begin/end.
    pub fn begin_statement(&mut self) {
        self.scope = Some(StatementScope::default());
    }

    /// Close the statement scope. The next statement opens a fresh one, so a DDL
    /// write in this statement is visible to the next.
    pub fn end_statement(&mut self) {
        self.scope = None;
    }

    /// Scan a system table, served from the statement scope when one is active
    /// (caching the batch on the first read). The batch is shared via `Arc`, so
    /// a scope hit is a refcount bump, never a batch copy.
    fn scan_catalog(&mut self, tab: u64) -> Result<Option<Arc<ZSetBatch>>, ClientError> {
        if let Some(cached) = self.scope.as_ref().and_then(|s| s.catalog.get(&tab)) {
            return Ok(cached.clone());
        }
        let (_, batch, _) = self.session.scan(tab)?;
        let batch = batch.map(Arc::new);
        if let Some(s) = &mut self.scope {
            s.catalog.insert(tab, batch.clone());
        }
        Ok(batch)
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

    /// Close the connection. Takes `self` by value, so the session — and with it
    /// the transport — is dropped here; the C ABI calls this to close explicitly.
    pub fn close(self) {}

    // --- Raw ops ---

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.session.alloc_table_id()
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        self.session.alloc_schema_id()
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
            txn.push_with_mode(table_id, schema, batch, mode);
            return Ok(0);
        }
        let r = self.session.push_with_mode(table_id, schema, batch, mode);
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
    pub fn txn_push_rmw(&mut self, table_id: u64, schema: &Schema, batch: &ZSetBatch, mode: WireConflictMode) {
        if let Some(txn) = &mut self.txn {
            txn.push_with_mode(table_id, schema, batch, mode);
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
        self.session.scan_spec(table_id, spec, reply_schema)
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
    /// leading-prefix seek. This is the single choke point shared by the SQL
    /// planner and every binding; the two arity guards below are load-bearing,
    /// not cosmetic — they prevent a `pack_pk_cols` panic and a silently-misread
    /// frame (the worker derives the value count from the wire byte length).
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
        let list = self.table_indexes(table_id)?;
        // Exact single-element match: a composite index does NOT answer a
        // single-column FK/uniqueness query (a `(a, b)` index does not guarantee
        // uniqueness of `a` alone).
        Ok(list.iter().find(|m| m.cols.as_slice() == [col_idx as u32]).copied())
    }

    /// The full secondary-index list for `table_id`. Used by the SQL
    /// point-lookup planner, which must see every index's full declared column
    /// list to plan a leading-prefix seek.
    pub fn table_indexes(&mut self, table_id: u64) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
        Ok(Arc::clone(&self.descriptor_by_tid(table_id)?.indexes))
    }

    /// True iff relation `tid` is REPLICATED. The SQL planner consults this when
    /// building an aggregate directly over a source: a reduce over a replicated
    /// relation must be the shard-free `reduce_multi_local` (every worker holds
    /// the full copy, so a sharded reduce would N-fold-multiply the aggregate —
    /// see the replicated-tables design).
    pub fn table_replicated(&mut self, tid: u64) -> Result<bool, ClientError> {
        Ok(self.descriptor_by_tid(tid)?.replicated)
    }

    /// The statement's descriptor for `tid`. A tid normally comes from resolving
    /// that same relation by name earlier in the statement, so this is a scope
    /// hit; the by-id round trip keeps the answer correct for a caller holding a
    /// tid from anywhere else, rather than handing it an empty index list and a
    /// `false` placement. The statement resolves a handful of relations, so the
    /// linear search costs less than a second map to keep in step.
    fn descriptor_by_tid(&mut self, tid: u64) -> Result<Arc<RelDescriptor>, ClientError> {
        let hit = self
            .scope
            .as_ref()
            .and_then(|s| s.relations.values().flatten().find(|d| d.tid == tid));
        if let Some(d) = hit {
            return Ok(Arc::clone(d));
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
        // Gateway backstop for non-SQL front ends (capi): enforce the ASCII
        // `[A-Za-z0-9_]` charset `canon_name` relies on. The SQL planner
        // validates earlier with better error types.
        crate::validate_user_identifier(index_name).map_err(ClientError::ServerError)?;
        let index_name = canon_name(index_name);
        // Arity, 7-bit column range, duplicates — the Err form of the
        // pack_pk_cols contract, so the pack below can never panic.
        gnitz_wire::validate_pk_col_list(col_indices)
            .map_err(|e| ClientError::ServerError(format!("create_index: {e}")))?;
        if col_types.len() != col_indices.len() {
            return Err(ClientError::ServerError(
                "create_index: col_indices and col_types length mismatch".to_string(),
            ));
        }
        for &ct in col_types {
            validate_index_col_type(ct)?;
        }

        // No client-side duplicate-name probe: the engine rejects one
        // authoritatively. `handle_ddl_txn` prechecks every bundle family before
        // applying any of it, and the IDX_TAB arm fails the whole bundle on a name
        // already in `index_by_name`, so a second row under one name is never
        // written and the "undroppable orphan" a client probe would guard against
        // is unreachable. A rejected bundle burns this index_id; ids are monotonic
        // and never reused, so that costs nothing.
        let index_id = self.session.alloc_index_id()?;

        let idx_schema = sys_schema(IDX_TAB);
        let mut batch = ZSetBatch::new(idx_schema);
        append_idx_tab_row(
            &mut BatchAppender::new(&mut batch, idx_schema),
            1,
            &IndexRecord {
                index_id,
                owner_id: table_id,
                source_cols: gnitz_wire::pack_pk_cols(col_indices),
                name: index_name,
                is_unique: is_unique as u64,
            },
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
        let index_name = canon_name(index_name);
        let not_found = |name: &str| -> Result<(), ClientError> {
            if if_exists {
                Ok(())
            } else {
                Err(ClientError::ServerError(format!("index '{name}' not found")))
            }
        };
        let (_, idx_batch, _) = self.session.scan(IDX_TAB)?;
        let Some(idx_batch) = idx_batch else {
            return not_found(&index_name);
        };
        for i in idx_batch.live_rows() {
            if col_str(&idx_batch.columns[IDXTAB_COL_NAME], i)? != Some(index_name.as_str()) {
                continue;
            }
            let rec = decode_index_record(&idx_batch, i)?;

            let idx_schema = sys_schema(IDX_TAB);
            let mut batch = ZSetBatch::new(idx_schema);
            append_idx_tab_row(&mut BatchAppender::new(&mut batch, idx_schema), -1, &rec);
            self.push_ddl(&[(IDX_TAB, batch)])?;
            return Ok(());
        }
        not_found(&index_name)
    }

    /// `(name, indexed columns)` of every live secondary-index IDX_TAB row (name in
    /// canonical lowercase, since `create_index`/`create_table` canonicalize at
    /// store time). The planner uses it to reject a re-index of an identical column
    /// set under the auto base name and to disambiguate an auto-generated name
    /// against the taken set. Reads the same slots `create_index` writes and
    /// `drop_index_by_name` reads.
    pub fn index_name_cols(&mut self) -> Result<Vec<(String, gnitz_wire::PkColList)>, ClientError> {
        let (_, idx_batch, _) = self.session.scan(IDX_TAB)?;
        let Some(idx_batch) = idx_batch else {
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
    /// write while a transaction is open.
    pub fn delete(&mut self, table_id: u64, schema: &Schema, pks: PkColumn) -> Result<(), ClientError> {
        if pks.is_empty() {
            return Ok(());
        }
        let batch = retraction_batch(schema, pks);
        self.push_with_mode(table_id, schema, &batch, WireConflictMode::Update)?;
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
    fn push_ddl(&mut self, families: &[(u64, ZSetBatch)]) -> Result<u64, ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::ServerError(
                "DDL is not allowed inside a transaction".into(),
            ));
        }
        self.session.push_ddl_txn(families)
    }

    pub fn create_schema(&mut self, name: &str) -> Result<u64, ClientError> {
        // Reject the empty string, a leading `_` (reserved system prefix), and
        // illegal characters. The SQL planner has no CREATE SCHEMA surface, so
        // this client entry point is the sole enforcement for schema names.
        crate::validate_user_identifier(name).map_err(ClientError::ServerError)?;
        let name = canon_name(name);
        let new_sid = self.session.alloc_schema_id()?;
        let schema = sys_schema(SCHEMA_TAB);
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(new_sid as u128, 1)
            .str_val(&name);
        self.push_ddl(&[(SCHEMA_TAB, batch)])?;
        Ok(new_sid)
    }

    /// Retry-until-stable drain: each pass attempts every remaining target,
    /// requeues any that fail, and stops when the queue empties (success) or a
    /// full pass makes no progress (return the last error). The client-side
    /// analog of the engine's `drain_drop_targets`. It requeues on **any** `Err`,
    /// not just a dependency error: `ClientError` collapses every engine precheck
    /// rejection into `ServerError(String)` with no structured dependency
    /// variant, so progress — not error-string matching — is the robust
    /// termination signal. Convergence holds because every target is being
    /// dropped: leaves succeed first and unblock their parents.
    fn drain_drops<F>(&mut self, targets: Vec<String>, mut drop_one: F) -> Result<(), ClientError>
    where
        F: FnMut(&mut Self, &str) -> Result<(), ClientError>,
    {
        let mut pending = targets;
        while !pending.is_empty() {
            let before = pending.len();
            let mut retry = Vec::new();
            let mut last_err = None;
            for name in std::mem::take(&mut pending) {
                if let Err(e) = drop_one(self, &name) {
                    last_err = Some(e);
                    retry.push(name);
                }
            }
            if retry.len() == before {
                return Err(last_err.expect("a non-empty no-progress pass recorded an error"));
            }
            pending = retry;
        }
        Ok(())
    }

    /// Drop a schema and every table and view it contains, then retire the schema
    /// row — PostgreSQL `DROP SCHEMA ... CASCADE` semantics. Members are dropped
    /// first (views before tables, since a view may read a member table), each as
    /// an ordinary `drop_view`/`drop_table` RPC whose member `±1` delta fires the
    /// per-member engine hooks (columns / indices / circuit + dir teardown).
    /// Every `conn.push` is synchronous and fully committed before the next, so
    /// each member retraction is reflected in master's caches before the final
    /// `SCHEMA_TAB -1` — which the engine's member-count guard (`precheck_sys_ingest`)
    /// then accepts because the schema is empty.
    ///
    /// RESTRICT on external dependents: a member referenced from *outside* the
    /// schema (a cross-schema FK child or view-on-view) stays blocked by the
    /// engine precheck; the drain makes no progress and returns that error.
    /// Already-dropped leaves stay dropped, but the still-referenced member and
    /// the `SCHEMA_TAB` row are never retracted, so no orphan results.
    pub fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
        let name = canon_name(name);
        let schema_id = self.lookup_schema_id(&name)?;

        // Views first — a view may read a member table; the drain retries to
        // resolve intra-schema view-on-view chains across passes. Synthesized
        // hidden members (`__h…`) are excluded: each is removed exclusively by
        // its owning user view's drop cascade, so draining them directly would
        // only burn RESTRICTed round-trips (owner still live) or target an
        // already-cascaded name.
        let (_, vdata, _) = self.session.scan(VIEW_TAB)?;
        let mut views = vdata.map_or(Ok(Vec::new()), |b| collect_schema_member_names(&b, schema_id))?;
        views.retain(|n| !is_hidden_view_name(n));
        self.drain_drops(views, |c, m| c.drop_view(&name, m))?;

        // Then tables — the drain retries to resolve intra-schema FK chains.
        let (_, tdata, _) = self.session.scan(TABLE_TAB)?;
        let tables = tdata.map_or(Ok(Vec::new()), |b| collect_schema_member_names(&b, schema_id))?;
        self.drain_drops(tables, |c, m| c.drop_table(&name, m))?;

        // Schema now empty; the engine member-count guard accepts this row.
        let schema = sys_schema(SCHEMA_TAB);
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(schema_id as u128, -1)
            .str_val(&name);
        self.push_ddl(&[(SCHEMA_TAB, batch)])?;
        Ok(())
    }

    /// `dist_prefix_len` is the hash-distribution prefix length `k`: rows are
    /// partitioned by the first `k` PK columns (`CLUSTER BY` the PK's leading
    /// prefix). `0` means the default — distribute by the full PK, byte-identical
    /// to the pre-distribution-key behavior. The SQL planner validates `k` against
    /// the PK before calling this; the single-PK Python/test surfaces pass `0`.
    ///
    /// `unique_indexes` are the table's inline `UNIQUE` constraints, folded into
    /// the same atomic DDL bundle as `[COL_TAB, TABLE_TAB, IDX_TAB]` so a failure
    /// rolls the whole `CREATE` back — never a table left missing its unique
    /// constraint. Pass an empty slice for a table with no inline UNIQUE.
    #[allow(clippy::too_many_arguments)]
    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        pk_cols: &[u32],
        replicated: bool,
        dist_prefix_len: usize,
        unique_indexes: &[InlineUniqueIndex],
    ) -> Result<u64, ClientError> {
        // Gateway backstop for non-SQL front ends (capi): enforce the ASCII
        // `[A-Za-z0-9_]` charset `canon_name` relies on for the names this call
        // stores. The SQL planner validates earlier with better error types.
        crate::validate_user_identifier(table_name).map_err(ClientError::ServerError)?;
        for spec in unique_indexes {
            crate::validate_user_identifier(spec.name).map_err(ClientError::ServerError)?;
        }
        let schema_name = canon_name(schema_name);
        let table_name = canon_name(table_name);
        // Full schema-admissibility rule set (column cap + PK rules), applied
        // here so a non-SQL (capi) caller gets a clean error before any id
        // allocation instead of relying on the server-side reject (and
        // `pack_pk_cols` below can never panic).
        let pk_indices: Vec<usize> = pk_cols.iter().map(|&c| c as usize).collect();
        Schema::validate_parts(&pk_indices, columns)
            .map_err(|e| ClientError::ServerError(format!("create_table: {e}")))?;
        // `dist_prefix_len` is a leading-PK-prefix length (0 = default = full PK);
        // a value past the PK count is meaningless and the engine would silently
        // clamp it, so reject it here to catch the caller's mistake.
        if dist_prefix_len > pk_cols.len() {
            return Err(ClientError::ServerError(format!(
                "create_table: distribution prefix length {dist_prefix_len} exceeds PK column count {}",
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
        let col_family = build_col_tab_batch(new_tid, OWNER_KIND_TABLE, columns)?;

        // TABLE_TAB family.
        let tbl_schema = sys_schema(TABLE_TAB);
        let mut tb = ZSetBatch::new(tbl_schema);
        append_table_tab_row(
            &mut BatchAppender::new(&mut tb, tbl_schema),
            1,
            &TableRecord {
                tid: new_tid,
                schema_id,
                pk_col_idx: pk_packed,
                flags: gnitz_wire::pack_table_flags(replicated, dist_prefix_len),
            },
            &table_name,
        );

        // IDX_TAB family — every inline UNIQUE index as one multi-row batch
        // (`hook_index_register` loops over rows). Allocate ids and validate up
        // front so the batch is assembled without interleaving RPCs. Column types
        // come from `columns`, so a UNIQUE+FK column's parent-rewritten type is
        // used. Empty ⇒ the bundle is just `[COL_TAB, TABLE_TAB]`.
        let mut index_ids: Vec<u64> = Vec::with_capacity(unique_indexes.len());
        for spec in unique_indexes {
            // Structural rules only (arity, in-range, no duplicates) — unlike a
            // PK, an indexed column may be nullable. In-range against the actual
            // column list also keeps the `columns[c]` read below panic-free.
            let idx_indices: Vec<usize> = spec.col_indices.iter().map(|&c| c as usize).collect();
            Schema::validate_pk_cols(&idx_indices, columns.len())
                .map_err(|e| ClientError::ServerError(format!("create_table: unique index '{}': {e}", spec.name)))?;
            for &c in spec.col_indices {
                validate_index_col_type(columns[c as usize].type_code)?;
            }
            index_ids.push(self.session.alloc_index_id()?);
        }
        let mut families: Vec<(u64, ZSetBatch)> = vec![col_family, (TABLE_TAB, tb)];
        if !unique_indexes.is_empty() {
            let idx_schema = sys_schema(IDX_TAB);
            let mut idx_batch = ZSetBatch::new(idx_schema);
            {
                let mut a = BatchAppender::new(&mut idx_batch, idx_schema);
                for (spec, &index_id) in unique_indexes.iter().zip(&index_ids) {
                    append_idx_tab_row(
                        &mut a,
                        1,
                        &IndexRecord {
                            index_id,
                            owner_id: new_tid,
                            source_cols: gnitz_wire::pack_pk_cols(spec.col_indices),
                            name: canon_name(spec.name),
                            is_unique: 1,
                        },
                    );
                }
            }
            families.push((IDX_TAB, idx_batch));
        }
        self.push_ddl(&families)?;

        Ok(new_tid)
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Result<(), ClientError> {
        let schema_name = canon_name(schema_name);
        let table_name = canon_name(table_name);
        let record = self.table_record(&schema_name, &table_name)?;

        let tbl_schema = sys_schema(TABLE_TAB);
        let mut tb = ZSetBatch::new(tbl_schema);
        append_table_tab_row(&mut BatchAppender::new(&mut tb, tbl_schema), -1, &record, &table_name);
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
        // Construct a minimal SCAN_DELTA → INTEGRATE_SINK circuit using the
        // typed builder so the row materialisation matches the new layout
        // bit-for-bit (no separate CircuitSources row, no PARAM_TABLE_ID,
        // single dependency entry).
        let vid = self.session.alloc_table_id()?;

        let mut cb = crate::circuit::CircuitBuilder::new(vid, source_table_id);
        let scan = cb.input_delta();
        cb.sink(scan);
        let circuit = cb.build();

        // Minimal SCAN→SINK passthrough: single output PK at slot 0.
        self.create_view_with_circuit(schema_name, view_name, "", circuit, output_columns, &[0])
    }

    pub fn create_view_with_circuit(
        &mut self,
        schema_name: &str,
        view_name: &str,
        sql_text: &str,
        circuit: Circuit,
        output_columns: &[ColumnDef],
        pk_cols: &[u32],
    ) -> Result<u64, ClientError> {
        let vids = self.create_view_chain(
            schema_name,
            vec![PlannedView {
                name: view_name.to_string(),
                sql_text: sql_text.to_string(),
                circuit,
                output_columns: output_columns.to_vec(),
                pk_cols: pk_cols.to_vec(),
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
    /// engine's `view_row_order` runs the retractions first and then registers
    /// the new chain in dependency order, and its qname-collision check admits
    /// the incumbent because this bundle retires it.
    pub fn create_view_chain(
        &mut self,
        schema_name: &str,
        views: Vec<PlannedView>,
        replaces: Option<&str>,
    ) -> Result<Vec<u64>, ClientError> {
        let schema_name = canon_name(schema_name);
        // Reject an over-long chain before any allocation (the self-referential
        // CTE blow-up guard).
        if views.len() > MAX_CHAIN_SEGMENTS {
            return Err(ClientError::ServerError(format!(
                "view chain has {} segments, exceeding the {MAX_CHAIN_SEGMENTS}-segment limit",
                views.len(),
            )));
        }

        // Per-view pre-flight validation up front, before any id allocation, so a
        // bad schema surfaces with no residue. The VIEW_TAB register path has no
        // server-side schema precheck, so a malformed spec would panic the client
        // at `pack_pk_cols`, trip the engine's build_schema_from_col_defs assert,
        // or abort the master in `SchemaDescriptor::new`; reject it all here.
        for pv in &views {
            // Gateway backstop for non-SQL front ends (capi): enforce the ASCII
            // `[A-Za-z0-9_]` charset `canon_name` relies on. Hidden segment names
            // are system-generated (`__h…` — the leading `_` is exactly what marks
            // them non-user), so only user-visible names are validated.
            if !is_hidden_view_name(&pv.name) {
                crate::validate_user_identifier(&pv.name).map_err(ClientError::ServerError)?;
            }
            let pk_indices: Vec<usize> = pv.pk_cols.iter().map(|&c| c as usize).collect();
            Schema::validate_parts(&pk_indices, &pv.output_columns)
                .map_err(|e| ClientError::ServerError(format!("View '{}': {e}", pv.name)))?;
        }

        let schema_id = self.lookup_schema_id(&schema_name)?;

        // The outgoing view's records, resolved before any id is allocated so a
        // missing view surfaces with no residue.
        let retracted: Vec<ViewRecord> = match replaces {
            Some(old) => self.view_drop_records(&schema_name, &canon_name(old))?,
            None => Vec::new(),
        };

        // Each view's vid: its circuit's pre-allocated id, or a fresh one. A chain
        // pre-sets every id (downstream circuits reference upstream hidden views),
        // so no alloc happens on that path.
        let mut vids: Vec<u64> = Vec::with_capacity(views.len());
        for pv in &views {
            let vid = if pv.circuit.view_id == 0 {
                self.session.alloc_table_id()?
            } else {
                pv.circuit.view_id
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

            // 0. The replaced view's retractions, full payload per record (the
            // engine's §3.3 CAS requires each `-1` to byte-equal the live row).
            for rec in &retracted {
                append_view_row(&mut view_a, -1, rec);
            }

            for (pv, vid) in views.into_iter().zip(vids.iter().copied()) {
                // 1. Column records.
                append_col_rows(&mut col_a, vid, OWNER_KIND_VIEW, &pv.output_columns)?;

                // 2–4. Materialise the typed circuit into the three-table bundle.
                //
                // Every circuit PK in `append_circuit_rows` is `(view_id, sub)`
                // with view_id in the LOW u128 half: the PK region OPK-encodes
                // each 8-byte column independently, low bytes first, so view_id
                // lands in the leading at-rest bytes. The engine prefix-seeks a view's rows
                // on `view_id.to_be_bytes()`; packing `(vid << 64) | sub`
                // instead puts `sub` there and breaks every view load.
                let rows = pv.circuit.into_rows();
                append_circuit_rows(&mut nodes_a, &mut edges_a, &mut ncol_a, vid, &rows);

                // 5. View record — the VIEW_TAB register hook triggers server-side
                // compilation. Encode the view PK with the shared wire packer so the
                // engine catalog decodes it identically to a TABLE_TAB PK.
                let pk_packed = gnitz_wire::pack_pk_cols(&pv.pk_cols);
                append_view_row(
                    &mut view_a,
                    1,
                    &ViewRecord {
                        vid,
                        schema_id,
                        name: canon_name(&pv.name),
                        sql_definition: pv.sql_text,
                        pk_col_idx: pk_packed,
                    },
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

        Ok(vids)
    }

    /// Drop a view and, cascading, every hidden segment view it owns
    /// (`__h{vid}_…`). The user view's `-1` and each hidden member's `-1` share
    /// one VIEW_TAB batch / one `push_ddl_txn`, so the engine's co-drop carve-out
    /// admits the bundle (every dependent is present in the same batch's drop set)
    /// and the whole chain retires atomically. A user view with no hidden members
    /// (every view created before this feature) drops exactly as before.
    pub fn drop_view(&mut self, schema_name: &str, view_name: &str) -> Result<(), ClientError> {
        let schema_name = canon_name(schema_name);
        let view_name = canon_name(view_name);
        let records = self.view_drop_records(&schema_name, &view_name)?;

        // One VIEW_TAB batch: the user view's `-1` plus every hidden member's
        // `-1`, full payload reproduced per record.
        let view_s = sys_schema(VIEW_TAB);
        let mut vb = ZSetBatch::new(view_s);
        {
            let mut a = BatchAppender::new(&mut vb, view_s);
            for rec in &records {
                append_view_row(&mut a, -1, rec);
            }
        }
        self.push_ddl(&[(VIEW_TAB, vb)])?;

        Ok(())
    }

    /// The live VIEW_TAB records that retiring `view_name` retracts: the user
    /// view followed by every hidden segment it owns. Ownership is name-encoded
    /// (hidden views are never shared across user views); the shared
    /// `hidden_view_prefix` keeps the producer (planner) and this consumer on one
    /// definition. Each record carries the row's full payload, which is what the
    /// engine's §3.3 CAS compares a `-1` against. DROP VIEW and the replacing
    /// half of ALTER VIEW retract exactly this set.
    fn view_drop_records(&mut self, schema_name: &str, view_name: &str) -> Result<Vec<ViewRecord>, ClientError> {
        let not_found = || ClientError::ServerError(format!("View '{schema_name}.{view_name}' not found"));
        let vid = self.resolve(schema_name, view_name)?.ok_or_else(not_found)?.tid;
        let (_, view_batch, _) = self.session.scan(VIEW_TAB)?;
        let view_batch = view_batch.ok_or_else(not_found)?;
        let vr = find_view_record_by_id(&view_batch, vid)?.ok_or_else(not_found)?;

        let prefix = hidden_view_prefix(vr.vid);
        let schema_id = vr.schema_id;
        let mut records = vec![vr];
        records.extend(collect_view_records_with_prefix(&view_batch, schema_id, &prefix)?);
        Ok(records)
    }

    /// Rename a table or view: a `(-1, +1)` rewrite pair on TABLE_TAB / VIEW_TAB,
    /// same id, the live record's exact payload at `-1` (the engine's §3.3 CAS
    /// requires byte-equality) with only the `name` changed at `+1`. The resolved
    /// descriptor picks the family, so the kind cannot disagree with the id.
    /// One atomic `push_ddl`; the catalog write lock quiesces pushes for the whole
    /// zone, and a rename changes no comparator or layout, so no tick/exchange
    /// quiesce is needed.
    pub fn alter_rename_relation(
        &mut self,
        schema_name: &str,
        current_name: &str,
        new_name: &str,
    ) -> Result<(), ClientError> {
        let schema_name = canon_name(schema_name);
        let current_name = canon_name(current_name);
        let new_name = canon_name(new_name);
        let not_found = || ClientError::ServerError(format!("Relation '{schema_name}.{current_name}' not found"));
        // Key the family lookup on the resolved id — the family's own PK — rather
        // than on `(schema_id, name)`, which would need a SCHEMA_TAB probe first.
        let desc = self.resolve(&schema_name, &current_name)?.ok_or_else(not_found)?;

        if desc.is_view {
            let view_batch = self.scan_catalog(VIEW_TAB)?.ok_or_else(not_found)?;
            let vr = find_view_record_by_id(&view_batch, desc.tid)?.ok_or_else(not_found)?;
            let view_s = sys_schema(VIEW_TAB);
            let mut vb = ZSetBatch::new(view_s);
            {
                let mut a = BatchAppender::new(&mut vb, view_s);
                append_view_row(&mut a, -1, &vr);
                let renamed = ViewRecord { name: new_name, ..vr };
                append_view_row(&mut a, 1, &renamed);
            }
            self.push_ddl(&[(VIEW_TAB, vb)])?;
        } else {
            let tbl_batch = self.scan_catalog(TABLE_TAB)?.ok_or_else(not_found)?;
            let record = find_table_record_by_id(&tbl_batch, desc.tid)?.ok_or_else(not_found)?;
            let tbl_s = sys_schema(TABLE_TAB);
            let mut tb = ZSetBatch::new(tbl_s);
            {
                let mut a = BatchAppender::new(&mut tb, tbl_s);
                append_table_tab_row(&mut a, -1, &record, &current_name);
                append_table_tab_row(&mut a, 1, &record, &new_name);
            }
            self.push_ddl(&[(TABLE_TAB, tb)])?;
        }
        Ok(())
    }

    /// Rename a column: a `(-1, +1)` COL_TAB rewrite pair, same packed column id,
    /// the live column's exact payload at `-1` and only the `name` changed at
    /// `+1`. Column names preserve case (unlike relation names), so `old_col` is
    /// matched case-insensitively but the `-1` reproduces the STORED name so the
    /// engine's §3.3 CAS accepts it. Rejects an unknown `old_col` and a collision
    /// with an existing visible column.
    pub fn alter_rename_column(
        &mut self,
        schema_name: &str,
        table_name: &str,
        old_col: &str,
        new_col: &str,
    ) -> Result<(), ClientError> {
        let schema_name = canon_name(schema_name);
        let table_name = canon_name(table_name);
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
        let desc = self.descriptor_by_tid(tid)?;
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
            append_col_row(&mut a, tid, OWNER_KIND_TABLE, col_idx, &def.name, def, 1)?;
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
        let desc = self.descriptor_by_tid(tid)?;
        // The rows written below carry `owner_kind = OWNER_KIND_TABLE`, so
        // against a stored view row the `-1` would fail as an opaque CAS
        // conflict. Reject here, where the assumption is made, rather than in
        // each of the three entry points. (The SQL layer rejects a view earlier,
        // with a better error; this is the backstop for non-SQL front ends.)
        if desc.is_view {
            return Err(ClientError::ServerError(format!(
                "relation {tid} is a view; ALTER COLUMN requires a base table"
            )));
        }
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
            append_col_row(&mut a, tid, OWNER_KIND_TABLE, col_idx, &cd.name, cd, -1)?;
            append_col_row(&mut a, tid, OWNER_KIND_TABLE, col_idx, &new_cd.name, &new_cd, 1)?;
        }
        self.push_ddl(&[(COL_TAB, cb)])?;
        Ok(())
    }

    /// Resolve `table_name` under `schema_name` to its id and schema. A view is
    /// reported as absent — callers that need to tell the two apart use
    /// [`Self::resolve_relation`], which returns the kind.
    pub fn resolve_table_id(&mut self, schema_name: &str, table_name: &str) -> Result<(u64, Arc<Schema>), ClientError> {
        let d = self.resolve(schema_name, table_name)?;
        match d.filter(|d| !d.is_view) {
            Some(d) => Ok((d.tid, Arc::clone(&d.schema))),
            None => Err(ClientError::ServerError(format!(
                "Table '{}' not found",
                qualified_name(schema_name, table_name)
            ))),
        }
    }

    /// Resolve `name` under `schema_name` to `(id, schema, is_view)`; the kind
    /// comes from the same descriptor as the id, so the two can never disagree.
    pub fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<(u64, Arc<Schema>, bool), ClientError> {
        let d = self.resolve(schema_name, name)?.ok_or_else(|| {
            ClientError::ServerError(format!(
                "Table or view '{}' not found",
                qualified_name(schema_name, name)
            ))
        })?;
        Ok((d.tid, Arc::clone(&d.schema), d.is_view))
    }

    pub fn resolve_table_or_view_id(
        &mut self,
        schema_name: &str,
        name: &str,
    ) -> Result<(u64, Arc<Schema>), ClientError> {
        let (id, schema, _) = self.resolve_relation(schema_name, name)?;
        Ok((id, schema))
    }

    /// Resolve `name` under `schema_name` to `(id, is_view)` — the kind probe.
    /// Every table-vs-view disambiguation (the binder's writable-target check,
    /// ALTER's target resolution) routes through here rather than re-spelling
    /// it. `Ok(None)` = no such relation; `Err` = a missing schema or a decode
    /// error, which must surface rather than be masked as a miss.
    pub fn resolve_relation_kind(&mut self, schema_name: &str, name: &str) -> Result<Option<(u64, bool)>, ClientError> {
        Ok(self.resolve(schema_name, name)?.map(|d| (d.tid, d.is_view)))
    }

    // --- Relation resolution ---

    /// The statement's descriptor for `schema_name.name`, or `None` when no such
    /// relation exists. One round trip per relation per statement: the scope
    /// holds the absent verdict too, so a two-probe error ladder does not pay
    /// twice.
    fn resolve(&mut self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let qname = qualified_name(schema_name, name);
        if let Some(hit) = self.scope.as_ref().and_then(|s| s.relations.get(&qname)) {
            return Ok(hit.clone());
        }
        let found = self.fetch_descriptor(RelTarget::Name(&qname))?;
        if let Some(s) = &mut self.scope {
            s.relations.insert(qname, found.clone());
        }
        Ok(found)
    }

    /// One RESOLVE round trip. `Ok(None)` is a relation-absent verdict.
    fn fetch_descriptor(&mut self, target: RelTarget<'_>) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let Some((tid, schema, blob)) = self.session.resolve(target)? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(RelDescriptor {
            tid,
            is_view: blob.is_view,
            replicated: blob.replicated,
            schema,
            indexes: Arc::new(blob.indexes),
        })))
    }

    // --- Private catalog-lookup helpers ---

    /// Resolve `schema_name` (already canonicalized) to its SCHEMA_TAB id. A
    /// missing row — or an entirely empty SCHEMA_TAB — is the one
    /// schema-qualified "not found" error every DDL/resolve path reports.
    fn lookup_schema_id(&mut self, schema_name: &str) -> Result<u64, ClientError> {
        let batch = self.scan_catalog(SCHEMA_TAB)?;
        match &batch {
            Some(b) => find_schema_id(b, schema_name)?,
            None => None,
        }
        .ok_or_else(|| ClientError::ServerError(format!("Schema '{schema_name}' not found")))
    }

    /// The live TABLE_TAB record for `schema_name.table_name` (both already
    /// canonicalized), keyed on the resolved id — the family's own PK — so no
    /// SCHEMA_TAB probe is needed to search by `(schema_id, name)`. The full
    /// payload is what a `-1` must reproduce byte-for-byte.
    fn table_record(&mut self, schema_name: &str, table_name: &str) -> Result<TableRecord, ClientError> {
        let not_found = || ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found"));
        let tid = self.resolve(schema_name, table_name)?.ok_or_else(not_found)?.tid;
        let batch = self.scan_catalog(TABLE_TAB)?.ok_or_else(not_found)?;
        find_table_record_by_id(&batch, tid)?.ok_or_else(not_found)
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
    /// Buffer an upsert (`WireConflictMode::Update`) of `batch` into `tid`.
    pub fn push(&mut self, tid: u64, schema: &Schema, batch: &ZSetBatch) {
        self.push_with_mode(tid, schema, batch, WireConflictMode::Update);
    }

    /// Buffer a push with an explicit conflict mode. `Error` mode rejects the
    /// whole transaction if any of these rows' PKs already exist (checked
    /// cumulatively in frame order against committed state and earlier families).
    pub fn push_with_mode(&mut self, tid: u64, schema: &Schema, batch: &ZSetBatch, mode: WireConflictMode) {
        self.append(tid, schema, batch.clone(), mode);
    }

    /// Buffer a delete of `pks` from `tid` — `-1` rows with inert filler
    /// payload, exactly as `GnitzClient::delete` builds them. Deletes buffer
    /// into the tid's Update family (mode `Update`), so the "delete k; insert k"
    /// replace idiom emits an Update family `[D(k)]` then an Error family
    /// `[I(k)]` in order.
    pub fn delete(&mut self, tid: u64, schema: &Schema, pks: PkColumn) {
        if pks.is_empty() {
            return;
        }
        let batch = retraction_batch(schema, pks);
        self.append(tid, schema, batch, WireConflictMode::Update);
    }

    /// Record `tid` in the read-set (deduped). Called only when an RMW statement
    /// buffered a write for `tid`, so the read-set stays a subset of the family
    /// tids. The linear `contains` is cheap — the read-set holds one entry per
    /// distinct table the transaction RMW'd, not per statement.
    fn record_read(&mut self, tid: u64) {
        if !self.read_set.contains(&tid) {
            self.read_set.push(tid);
        }
    }

    /// Append one op's `batch` to `tid`'s current run, or open a new family when
    /// the mode differs (or `tid` has no family yet). Empty batches contribute
    /// nothing and open no family.
    fn append(&mut self, tid: u64, schema: &Schema, batch: ZSetBatch, mode: WireConflictMode) {
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

/// Decode row `i` of a `TABLE_TAB` batch into a `TableRecord` — this crate's one
/// reading of the TABLE_TAB layout; both finders below build on it. The write
/// side is `gnitz_wire::sys_rows::write_table_tab_row`, which the whole tree
/// shares. `name` is not decoded here — `append_table_tab_row` takes it
/// separately, since a DROP or RENAME supplies it from elsewhere.
fn decode_table_record(batch: &ZSetBatch, i: usize) -> Result<TableRecord, ClientError> {
    Ok(TableRecord {
        tid: batch.pks.get(i) as u64,
        schema_id: col_u64(&batch.columns[TABTAB_COL_SCHEMA_ID], i)?,
        pk_col_idx: col_u64(&batch.columns[TABTAB_COL_PK_COL_IDX], i)?,
        flags: col_u64(&batch.columns[TABTAB_COL_FLAGS], i)?,
    })
}

/// The TABLE_TAB record with PK `tid`. `Ok(None)` = absent (a legitimate miss);
/// `Err` = a decode error on a corrupt batch, which must surface rather than be
/// masked as a miss.
fn find_table_record_by_id(batch: &ZSetBatch, tid: u64) -> Result<Option<TableRecord>, ClientError> {
    batch
        .live_row_with_pk(tid)
        .map(|i| decode_table_record(batch, i))
        .transpose()
}

/// Decode row `i` of a `VIEW_TAB` batch into a `ViewRecord` — this crate's one
/// reading of the VIEW_TAB layout; every row filter builds on it. The write side
/// is `gnitz_wire::sys_rows::write_view_tab_row`.
fn decode_view_record(batch: &ZSetBatch, i: usize) -> Result<ViewRecord, ClientError> {
    Ok(ViewRecord {
        vid: batch.pks.get(i) as u64,
        schema_id: col_u64(&batch.columns[VIEWTAB_COL_SCHEMA_ID], i)?,
        name: col_str(&batch.columns[VIEWTAB_COL_NAME], i)?.unwrap_or("").to_string(),
        sql_definition: col_str(&batch.columns[VIEWTAB_COL_SQL], i)?.unwrap_or("").to_string(),
        pk_col_idx: col_u64(&batch.columns[VIEWTAB_COL_PK_COL_IDX], i)?,
    })
}

/// Append a circuit's node / edge / node-column rows to the three circuit-family
/// batch appenders under `vid` (the compound `(view_id, sub)` PK prefix). The
/// single home for the circuit-family wire layout — the PK packings and the
/// nullable `source_table` / `expr_program` writers — used by CREATE VIEW
/// (`create_view_chain`). See the convergence invariant in `create_view_chain`
/// for why `view_id` rides the LOW u128 half.
fn append_circuit_rows(
    nodes_a: &mut BatchAppender<'_>,
    edges_a: &mut BatchAppender<'_>,
    ncol_a: &mut BatchAppender<'_>,
    vid: u64,
    rows: &crate::circuit::CircuitRows,
) {
    for (node_id, opcode, src_tab, expr_blob) in &rows.nodes {
        // Compound PK (view_id, sub=node_id): low 8 bytes = view_id.
        let pk = (vid as u128) | ((*node_id as u128) << 64);
        nodes_a.add_row(pk, 1).u64_val(*node_id).u64_val(*opcode);
        // source_table and expr_program are nullable; the `*_null` writers set
        // the row bitmap themselves.
        match src_tab {
            Some(t) => nodes_a.u64_val(*t),
            None => nodes_a.null(),
        };
        match expr_blob {
            Some(b) => nodes_a.bytes_val(b),
            None => nodes_a.null(),
        };
    }
    for (dst_node, dst_port, src_node) in &rows.edges {
        debug_assert!(*dst_node < (1u64 << 40), "dst_node {dst_node} exceeds 40-bit cap");
        // Compound PK (view_id, sub): sub packs (dst_node, dst_port).
        let sub = ((*dst_node as u128) << 8) | (*dst_port as u128);
        let pk = (vid as u128) | (sub << 64);
        edges_a
            .add_row(pk, 1)
            .u64_val(*dst_node)
            .u64_val(*dst_port as u64)
            .u64_val(*src_node);
    }
    for (node_id, kind, position, v1, v2) in &rows.node_columns {
        debug_assert!((*position as u64) <= 0xFFFF);
        debug_assert!((*kind) <= 0xFF);
        debug_assert!((*node_id) <= 0x00FF_FFFF_FFFF);
        // Compound PK (view_id, sub): sub packs (node_id, kind, position).
        let sub = ((*node_id as u128) << 24) | ((*kind as u128) << 16) | (*position as u128);
        let pk = (vid as u128) | (sub << 64);
        ncol_a
            .add_row(pk, 1)
            .u64_val(*node_id)
            .u64_val(*kind)
            .u64_val(*position as u64)
            .u64_val(*v1)
            .u64_val(*v2);
    }
}

/// Append one `VIEW_TAB` row through the shared codec. A drop's `-1` row must
/// reproduce the `+1`'s payload byte-for-byte to cancel in the Z-set, so the
/// create and drop-cascade paths both write through here.
fn append_view_row(a: &mut BatchAppender<'_>, weight: i64, rec: &ViewRecord) {
    gnitz_wire::sys_rows::write_view_tab_row(
        a,
        &ViewTabRow {
            view_id: rec.vid,
            schema_id: rec.schema_id,
            name: &rec.name,
            sql_definition: &rec.sql_definition,
            pk_col_idx: rec.pk_col_idx,
        },
        weight,
    );
}

/// Append one `TABLE_TAB` row — the write-side mirror of `decode_table_record`
/// (which reads every payload column except the `name`). The `name`
/// is passed separately so a DROP or RENAME reproduces the live payload
/// byte-for-byte (§3.3 CAS); the create/drop/rename paths all write through here.
fn append_table_tab_row(a: &mut BatchAppender<'_>, weight: i64, rec: &TableRecord, name: &str) {
    gnitz_wire::sys_rows::write_table_tab_row(
        a,
        &TableTabRow {
            table_id: rec.tid,
            schema_id: rec.schema_id,
            name,
            pk_col_idx: rec.pk_col_idx,
            flags: rec.flags,
        },
        weight,
    );
}

/// Append one `IDX_TAB` row through the shared codec, read back by
/// [`decode_index_record`]. The engine rejects a `-1` whose payload differs
/// from the live row, and only byte-equal rows cancel, so a DROP must
/// reproduce its CREATE exactly.
fn append_idx_tab_row(a: &mut BatchAppender<'_>, weight: i64, rec: &IndexRecord) {
    gnitz_wire::sys_rows::write_idx_tab_row(
        a,
        &IdxTabRow {
            index_id: rec.index_id,
            owner_id: rec.owner_id,
            source_col_idx: rec.source_cols,
            name: &rec.name,
            is_unique: rec.is_unique,
        },
        weight,
    );
}

/// Read-side mirror of [`append_idx_tab_row`].
fn decode_index_record(batch: &ZSetBatch, i: usize) -> Result<IndexRecord, ClientError> {
    Ok(IndexRecord {
        index_id: batch.pks.get(i) as u64,
        owner_id: col_u64(&batch.columns[IDXTAB_COL_OWNER_ID], i)?,
        source_cols: col_u64(&batch.columns[IDXTAB_COL_SOURCE_COLS], i)?,
        name: col_str(&batch.columns[IDXTAB_COL_NAME], i)?.unwrap_or("").to_string(),
        is_unique: col_u64(&batch.columns[IDXTAB_COL_IS_UNIQUE], i)?,
    })
}

/// The VIEW_TAB record with PK `vid` — the VIEW_TAB peer of
/// [`find_table_record_by_id`].
fn find_view_record_by_id(batch: &ZSetBatch, vid: u64) -> Result<Option<ViewRecord>, ClientError> {
    batch
        .live_row_with_pk(vid)
        .map(|i| decode_view_record(batch, i))
        .transpose()
}

/// Collect the full `ViewRecord`s of every live `VIEW_TAB` row whose `schema_id`
/// matches and whose name starts with `prefix`. Drives the cascading DROP of a
/// user view's synthesized hidden segment views (`__h{vid}_…`) — each `-1` row
/// needs the full payload reproduced, so names alone (`collect_schema_member_names`)
/// don't suffice.
fn collect_view_records_with_prefix(
    batch: &ZSetBatch,
    schema_id: u64,
    prefix: &str,
) -> Result<Vec<ViewRecord>, ClientError> {
    let mut out = Vec::new();
    for i in batch.live_rows() {
        if col_u64(&batch.columns[VIEWTAB_COL_SCHEMA_ID], i)? != schema_id {
            continue;
        }
        if !matches!(col_str(&batch.columns[VIEWTAB_COL_NAME], i)?, Some(n) if n.starts_with(prefix)) {
            continue;
        }
        out.push(decode_view_record(batch, i)?);
    }
    Ok(out)
}

/// Collect the entity names of every live row in a `TABLE_TAB`/`VIEW_TAB` batch
/// whose `schema_id` column matches. One code path over both families, which the
/// static assert beside their column constants licenses: the two agree on where
/// `schema_id` and the entity name sit. Keeps the whole matching set rather than
/// one row. Drives the `drop_schema` member cascade.
fn collect_schema_member_names(batch: &ZSetBatch, schema_id: u64) -> Result<Vec<String>, ClientError> {
    let mut out = Vec::new();
    for i in batch.live_rows() {
        if col_u64(&batch.columns[TABTAB_COL_SCHEMA_ID], i)? != schema_id {
            continue;
        }
        if let Some(name) = col_str(&batch.columns[TABTAB_COL_NAME], i)? {
            out.push(name.to_string());
        }
    }
    Ok(out)
}

/// Append one `COL_TAB` row for column `col_idx` of `owner_id` at `weight`, with
/// `name` (column names are case-preserved) and the rest of the payload from
/// `cd`. The create path (`append_col_rows`) and a RENAME COLUMN's `-1`/`+1`
/// pair both write through here — and through the shared codec below it — so a
/// rename's `-1` reproduces the live row byte-for-byte (§3.3 CAS).
///
/// The FK fields are resolved against `owner_id`/`owner_kind` instead of taken
/// from `cd`: `SELF_FK_TABLE_ID` becomes `owner_id` (the id the planner could
/// not name, since the same request creates the table), and a non-table owner
/// writes no FK at all — a view's defs are clones of the projected source
/// columns, and the constraint belongs to the base table, not to a view over
/// it. A rename's `cd` comes from the catalog with a real id, so the
/// byte-for-byte reproduction is unaffected.
fn append_col_row(
    a: &mut BatchAppender<'_>,
    owner_id: u64,
    owner_kind: u64,
    col_idx: usize,
    name: &str,
    cd: &ColumnDef,
    weight: i64,
) -> Result<(), ClientError> {
    let (fk_table_id, fk_col_idx) = if owner_kind != OWNER_KIND_TABLE {
        (0, 0)
    } else if cd.fk_table_id == ColumnDef::SELF_FK_TABLE_ID {
        (owner_id, cd.fk_col_idx)
    } else {
        (cd.fk_table_id, cd.fk_col_idx)
    };
    gnitz_wire::sys_rows::write_col_tab_row(
        a,
        &ColTabRow {
            owner_id,
            owner_kind,
            col_idx: col_idx as u64,
            name,
            type_code: cd.type_code as u64,
            is_nullable: cd.is_nullable,
            fk_table_id,
            fk_col_idx,
            is_serial: cd.is_serial,
            is_hidden: cd.is_hidden,
        },
        weight,
    )
    .map_err(ClientError::ServerError)
}

fn append_col_rows(
    a: &mut BatchAppender<'_>,
    owner_id: u64,
    owner_kind: u64,
    columns: &[ColumnDef],
) -> Result<(), ClientError> {
    for (i, col) in columns.iter().enumerate() {
        append_col_row(a, owner_id, owner_kind, i, &col.name, col, 1)?;
    }
    Ok(())
}

/// Build the `COL_TAB` family batch for a table/view's column records — a pure
/// batch builder returning `(target_id, batch)` for `push_ddl_txn` to bundle.
/// Touches no connection state; the DDL's whole family set is ingested
/// atomically server-side, so column records no longer need a standalone RPC.
fn build_col_tab_batch(owner_id: u64, owner_kind: u64, columns: &[ColumnDef]) -> Result<(u64, ZSetBatch), ClientError> {
    let schema = sys_schema(COL_TAB);
    let mut batch = ZSetBatch::new(schema);
    append_col_rows(
        &mut BatchAppender::new(&mut batch, schema),
        owner_id,
        owner_kind,
        columns,
    )?;
    Ok((COL_TAB, batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn kv_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        }
    }

    fn ins(schema: &Schema, pk: u64, val: i64) -> ZSetBatch {
        let mut b = ZSetBatch::new(schema);
        BatchAppender::new(&mut b, schema).add_row(pk as u128, 1).i64_val(val);
        b
    }

    #[test]
    fn empty_push_opens_no_family() {
        let s = kv_schema();
        let mut buf = TxnBuffer::default();
        assert!(buf.families.is_empty());
        buf.push(16, &s, &ZSetBatch::new(&s));
        assert!(buf.families.is_empty(), "an empty push opens no family");
    }

    #[test]
    fn run_splitting_merges_same_mode_and_splits_on_mode_change() {
        let s = kv_schema();
        let mut buf = TxnBuffer::default();
        let tid = 16u64;
        buf.push(tid, &s, &ins(&s, 1, 10));
        buf.push(tid, &s, &ins(&s, 2, 20));
        assert_eq!(buf.families.len(), 1, "same-mode pushes coalesce");
        assert_eq!(buf.families[0].batch.len(), 2);
        assert_eq!(buf.families[0].mode, WireConflictMode::Update);
        buf.push_with_mode(tid, &s, &ins(&s, 3, 30), WireConflictMode::Error);
        assert_eq!(buf.families.len(), 2, "mode change opens a new family");
        assert_eq!(buf.families[1].mode, WireConflictMode::Error);
        buf.push(tid, &s, &ins(&s, 4, 40));
        assert_eq!(
            buf.families.len(),
            3,
            "back to Update opens a third family in call order"
        );
        assert_eq!(buf.families[2].mode, WireConflictMode::Update);
    }

    #[test]
    fn delete_buffers_into_update_family_before_error_reinsert() {
        // "delete k; insert_error k" → Update family [D(k)] then Error family [I(k)].
        let s = kv_schema();
        let mut buf = TxnBuffer::default();
        let tid = 16u64;
        buf.delete(tid, &s, PkColumn::from_u128s(8, [7]));
        buf.push_with_mode(tid, &s, &ins(&s, 7, 70), WireConflictMode::Error);
        assert_eq!(buf.families.len(), 2);
        assert_eq!(buf.families[0].mode, WireConflictMode::Update);
        assert_eq!(buf.families[0].batch.weights, vec![-1]);
        assert_eq!(buf.families[1].mode, WireConflictMode::Error);
        assert_eq!(buf.families[1].batch.weights, vec![1]);
    }

    #[test]
    fn cross_tid_ops_coalesce_per_tid() {
        // push(A), push(B), push(A) → A one family (2 rows), B one family.
        let s = kv_schema();
        let mut buf = TxnBuffer::default();
        buf.push(16, &s, &ins(&s, 1, 1));
        buf.push(17, &s, &ins(&s, 1, 1));
        buf.push(16, &s, &ins(&s, 2, 2));
        assert_eq!(buf.families.len(), 2);
        assert_eq!(buf.families[0].tid, 16);
        assert_eq!(buf.families[0].batch.len(), 2);
        assert_eq!(buf.families[1].tid, 17);
    }

    #[test]
    fn last_op_indexes_rows_across_family_extension_and_split() {
        // The PK index must survive both append shapes: extending an existing
        // family (row index = base + i) and opening a new one (base = 0).
        let s = kv_schema();
        let mut buf = TxnBuffer::default();
        let tid = 16u64;
        buf.push(tid, &s, &ins(&s, 1, 10)); // new family 0, row 0
        buf.push(tid, &s, &ins(&s, 2, 20)); // extends family 0, row 1
        buf.push_with_mode(tid, &s, &ins(&s, 3, 30), WireConflictMode::Error); // family 1, row 0
        buf.delete(tid, &s, PkColumn::from_u128s(8, [1])); // family 2, row 0 — supersedes pk=1

        let val = |pk: u64| {
            let (b, row) = buf.last_op(tid, &PkTuple::from_u128(8, pk as u128)).unwrap();
            (b.weights[row], row)
        };
        assert_eq!(val(1), (-1, 0), "pk=1's last op is the delete");
        assert_eq!(val(2), (1, 1), "pk=2 is row 1 of the extended family");
        assert_eq!(val(3), (1, 0), "pk=3 is row 0 of the mode-split family");
        assert!(buf.last_op(tid, &PkTuple::from_u128(8, 9)).is_none(), "untouched PK");
        assert!(buf.last_op(17, &PkTuple::from_u128(8, 1)).is_none(), "other tid");
        assert_eq!(buf.last_ops(tid).count(), 3);
    }

    #[test]
    fn validate_index_col_type_rejects_non_pk_eligible() {
        // Deny-list misses these no longer: float/string/blob are all rejected
        // client-side, matching the server's get_index_key_type allow-list.
        for tc in [TypeCode::F32, TypeCode::F64, TypeCode::String, TypeCode::Blob] {
            assert!(validate_index_col_type(tc).is_err(), "{tc:?} must be rejected");
        }
        // Integer scalars (+ U128/UUID) remain index-eligible.
        for tc in [
            TypeCode::U64,
            TypeCode::I64,
            TypeCode::U32,
            TypeCode::I8,
            TypeCode::U128,
            TypeCode::UUID,
        ] {
            assert!(validate_index_col_type(tc).is_ok(), "{tc:?} must be accepted");
        }
    }

    #[test]
    fn find_table_record_surfaces_decode_error_not_miss() {
        let schema = sys_schema(TABLE_TAB);
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(7, 1)
            .u64_val(1) // schema_id
            .str_val("t") // name
            .u64_val(0) // pk_col_idx
            .u64_val(0); // flags

        // Truncate the schema_id column (Fixed) so `col_u64` on the live row is
        // out of bounds — a real decode error, which must surface as Err rather
        // than be masked as an absent-name miss.
        let ColData::Fixed(bytes) = &mut batch.columns[TABTAB_COL_SCHEMA_ID] else {
            panic!("expected Fixed column");
        };
        bytes.clear();
        match find_table_record_by_id(&batch, 7) {
            Err(ClientError::ServerError(s)) => assert!(s.contains("no 8-byte cell"), "got: {s}"),
            _ => panic!("expected decode ServerError, got a non-error result"),
        }
    }

    /// Round-trips `append_table_tab_row` back through `find_table_record_by_id`,
    /// so transposing two slots in either one fails here. Every field holds a
    /// different value, which a fixture writing `0` everywhere would not catch.
    #[test]
    fn table_record_round_trips_through_append_and_find() {
        let schema = sys_schema(TABLE_TAB);
        let rec = TableRecord {
            tid: 7,
            schema_id: 3,
            pk_col_idx: gnitz_wire::pack_pk_cols(&[1, 0]),
            flags: gnitz_wire::pack_table_flags(true, 0),
        };
        let mut batch = ZSetBatch::new(schema);
        append_table_tab_row(&mut BatchAppender::new(&mut batch, schema), 1, &rec, "t");

        assert!(find_table_record_by_id(&batch, 8).unwrap().is_none());

        let back = find_table_record_by_id(&batch, 7).unwrap().expect("row present");
        assert_eq!(back.tid, rec.tid);
        assert_eq!(back.schema_id, rec.schema_id);
        assert_eq!(back.pk_col_idx, rec.pk_col_idx);
        assert_eq!(back.flags, rec.flags);
    }

    /// The VIEW_TAB counterpart, likewise with every field distinct — the write
    /// side (`append_view_row`) round-trips back through the read side.
    #[test]
    fn view_record_round_trips_through_append_and_decode() {
        let schema = sys_schema(VIEW_TAB);
        let rec = ViewRecord {
            vid: 9,
            schema_id: 4,
            name: "v".into(),
            sql_definition: "SELECT id FROM t".into(),
            pk_col_idx: gnitz_wire::pack_pk_cols(&[2]),
        };
        let mut batch = ZSetBatch::new(schema);
        append_view_row(&mut BatchAppender::new(&mut batch, schema), 1, &rec);

        let back = decode_view_record(&batch, 0).unwrap();
        assert_eq!(back.vid, rec.vid);
        assert_eq!(back.schema_id, rec.schema_id);
        assert_eq!(back.name, rec.name);
        assert_eq!(back.sql_definition, rec.sql_definition);
        assert_eq!(back.pk_col_idx, rec.pk_col_idx);
    }

    #[test]
    fn find_schema_id_miss_vs_hit() {
        let schema = sys_schema(SCHEMA_TAB);
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema).add_row(3, 1).str_val("foo");
        assert_eq!(find_schema_id(&batch, "foo").unwrap(), Some(3));
        assert!(find_schema_id(&batch, "bar").unwrap().is_none());
    }
}
