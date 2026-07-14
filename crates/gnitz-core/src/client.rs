use crate::connection::{ScanResult, Session, COL_TAB, DEP_TAB, IDX_TAB, SCHEMA_TAB, TABLE_TAB, VIEW_TAB};
use crate::error::ClientError;
use crate::protocol::types::type_code_from_u64;
use crate::protocol::{
    BatchAppender, ColData, ColumnDef, PkColumn, PkTuple, Schema, TypeCode, WireConflictMode, ZSetBatch,
};
use gnitz_wire::RangeDescriptor;
use lru::LruCache;
use std::collections::HashMap;
use std::sync::Arc;

const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();
use crate::circuit::Circuit;
use crate::types::{
    circuit_edges_schema, circuit_node_columns_schema, circuit_nodes_schema, col_tab_schema, dep_tab_schema,
    idx_tab_schema, schema_tab_schema, table_tab_schema, view_tab_schema,
};
use gnitz_wire::{CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB, OWNER_KIND_TABLE, OWNER_KIND_VIEW};

// --- Module-private helpers ---

fn col_u64(col: &ColData, i: usize) -> Result<u64, ClientError> {
    match col {
        ColData::Fixed(bytes) => {
            let off = i * 8;
            if off + 8 > bytes.len() {
                return Err(ClientError::ServerError(format!(
                    "col_u64: row {} out of bounds (len {})",
                    i,
                    bytes.len()
                )));
            }
            Ok(u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap()))
        }
        _ => Err(ClientError::ServerError("col_u64: expected Fixed column".into())),
    }
}

fn col_str(col: &ColData, i: usize) -> Result<Option<&str>, ClientError> {
    match col {
        ColData::Strings(v) => {
            if i >= v.len() {
                return Err(ClientError::ServerError(format!(
                    "col_str: row {} out of bounds (len {})",
                    i,
                    v.len()
                )));
            }
            Ok(v[i].as_deref())
        }
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

/// Unpack a persisted column-list `u64` (`TABLE_TAB.pk_col_idx`,
/// `IDX_TAB.source_col_idx`). The shared `gnitz_wire::unpack_pk_cols` handles
/// both the bare-scalar (pre-compound) row form and the packed compound form.
/// Rejects a malformed count (`is_well_formed`) — an empty or
/// over-`PK_LIST_MAX_COLS` payload the server would have rejected too;
/// silently truncating here would hide the divergence from the client.
fn unpack_well_formed(packed: u64) -> Result<gnitz_wire::PkColList, ClientError> {
    let pkl = gnitz_wire::unpack_pk_cols(packed);
    if !pkl.is_well_formed() {
        return Err(ClientError::ServerError(format!(
            "packed column-list count {} out of range 1..={}",
            pkl.decoded_count(),
            gnitz_wire::PK_LIST_MAX_COLS
        )));
    }
    Ok(pkl)
}

/// Decode the persisted PK-list `u64` into a `Vec<usize>` for the `Schema` type.
fn decode_pk_cols(packed: u64) -> Result<Vec<usize>, ClientError> {
    Ok(unpack_well_formed(packed)?
        .as_slice()
        .iter()
        .map(|&c| c as usize)
        .collect())
}

fn pack_col_id(owner_id: u64, col_idx: usize) -> Result<u64, ClientError> {
    gnitz_wire::pack_col_id(owner_id, col_idx as u64).map_err(ClientError::ServerError)
}

/// Build the `-1` retraction batch for `pks`: the server's `retract_pk` matches
/// by PK alone, so the payload columns are inert filler (built directly, not via
/// `BatchAppender`, whose `add_row` takes a single scalar PK). Shared by
/// `GnitzClient::delete` and `TxnBuffer::delete`.
fn retraction_batch(schema: &Schema, pks: PkColumn) -> ZSetBatch {
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
    directory: String,
    pk_col_idx: u64,
    created_lsn: u64,
    flags: u64,
}

struct ViewRecord {
    vid: u64,
    schema_id: u64,
    name: String,
    sql_definition: String,
    cache_directory: String,
    created_lsn: u64,
    pk_col_idx: u64,
}

// --- GnitzClient ---

/// A tiny owned secondary-index descriptor. `index_id` has no consumer, so it
/// is omitted; `cols` is the index's declared column list (the unique key — the
/// circuit list is deduped by column list) and `is_unique` is the system's
/// operative uniqueness truth. `PkColList` is `Copy`, so `IndexMeta` stays
/// `Copy` (no `.cloned()` churn at its consumers).
#[derive(Clone, Copy, Debug)]
pub struct IndexMeta {
    pub cols: gnitz_wire::PkColList,
    pub is_unique: bool,
}

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

/// The name prefix every hidden segment of `owner_vid` carries. The separator
/// terminator is load-bearing: `__h5_` must not match `__h51_0`.
pub fn hidden_view_prefix(owner_vid: u64) -> String {
    format!("__h{owner_vid}_")
}

/// Whether `name` is a synthesized hidden segment view (any owner). Sound
/// because user identifiers cannot start with `_` (`validate_user_identifier`).
pub fn is_hidden_view_name(name: &str) -> bool {
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

pub struct GnitzClient {
    session: Session,
    index_cache: LruCache<u64, (Arc<Vec<IndexMeta>>, u8)>,
    serial_cache: HashMap<u64, SerialRange>,
    /// Statement-scoped catalog snapshot: while `Some`, the first read of each
    /// system table caches its scan batch here and later reads within the same
    /// statement reuse it (see `begin_catalog_snapshot`). `None` outside a
    /// statement — reads scan the wire directly.
    catalog_snapshot: Option<HashMap<u64, Option<ZSetBatch>>>,
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
}

impl GnitzClient {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError> {
        Ok(GnitzClient {
            session: Session::connect(socket_path)?,
            index_cache: LruCache::new(SCHEMA_CACHE_CAP),
            serial_cache: HashMap::new(),
            catalog_snapshot: None,
            txn: None,
        })
    }

    /// Begin a statement-scoped catalog snapshot: subsequent catalog reads
    /// (`lookup_schema_id`, `lookup_table_record`, `load_owner_schema`, the
    /// view-branch `VIEW_TAB` scan, `table_replicated`) scan each system table
    /// over the wire at most once and reuse it for the rest of the statement.
    /// The SQL planner brackets each statement with begin/end; nothing else
    /// should hold a snapshot across a catalog write.
    pub fn begin_catalog_snapshot(&mut self) {
        self.catalog_snapshot = Some(HashMap::new());
    }

    /// End the current catalog snapshot (drop the cached batches). The next
    /// statement begins a fresh one, so a DDL write in this statement is
    /// visible to the next — there is no cross-statement state to invalidate.
    pub fn end_catalog_snapshot(&mut self) {
        self.catalog_snapshot = None;
    }

    /// Scan a system table, served from the statement snapshot when one is
    /// active (caching the batch on the first read). The clone on a snapshot
    /// hit is a local memcpy — cheap against the wire scan + server work it
    /// replaces.
    fn scan_catalog(&mut self, tab: u64) -> Result<Option<ZSetBatch>, ClientError> {
        if let Some(snap) = &self.catalog_snapshot {
            if let Some(cached) = snap.get(&tab) {
                return Ok(cached.clone());
            }
        }
        let (_, batch, _) = self.session.scan(tab)?;
        if let Some(snap) = &mut self.catalog_snapshot {
            snap.insert(tab, batch.clone());
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

    pub fn close(self) {
        self.session.close();
    }

    // --- Raw ops ---

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.session.alloc_table_id()
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        self.session.alloc_schema_id()
    }

    pub fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        self.session.alloc_index_id()
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
        self.session.push_with_mode(table_id, schema, batch, mode)
    }

    pub fn scan(&mut self, table_id: u64) -> ScanResult {
        self.session.scan(table_id)
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

    /// Ordered range scan over a secondary index: the leading
    /// `desc.eq_vals().len()` columns are equality-pinned, and the next index
    /// column is bounded by the descriptor's half-open cut interval
    /// `[start, end)`. The guards below fail a malformed request fast, before
    /// any round trip; the engine method self-guards the same arity rule, so
    /// a binding that bypasses this entry point still cannot drive it out of
    /// bounds.
    ///
    /// `Before(type_min)‥After(type_max)` is a legitimate full index scan over
    /// the range column's non-NULL rows — the SQL planner sends exactly that
    /// for an unconstrained or saturated side (e.g. `x < 3e9` on an I32
    /// column), and a column-NULL row is absent from the index, matching SQL
    /// range NULL-exclusion. A zero-width or inverted interval is equally
    /// legitimate: the engine detects it byte-wise and returns nothing.
    pub fn seek_by_index_range(&mut self, table_id: u64, col_indices: &[u32], desc: &RangeDescriptor) -> ScanResult {
        gnitz_wire::validate_pk_col_list(col_indices)
            .map_err(|e| ClientError::ServerError(format!("seek_by_index_range: {e}")))?;
        // The range column sits right after the equality prefix, so the prefix
        // must be strictly shorter than the index column list. (Written `>=`,
        // never a `+ 1` that could overflow on an adversarial length.)
        if desc.eq_vals().len() >= col_indices.len() {
            return Err(ClientError::ServerError(format!(
                "seek_by_index_range: {} equality values leave no range column \
                 within index arity {}",
                desc.eq_vals().len(),
                col_indices.len()
            )));
        }
        self.session.seek_by_index_range(table_id, col_indices, desc)
    }

    /// The secondary-index descriptor for `col_idx` of `table_id`, served from a
    /// durable, epoch-validated cache instead of an `IDX_TAB` scan. The cache
    /// holds the projected `(cols, is_unique)` circuit list at the server's
    /// index epoch; a GET_INDICES round-trip either confirms it (unchanged) or
    /// replaces it. The column list is unique per row (the server dedups
    /// circuits by list), so a plain `find` is exact.
    ///
    /// The reported uniqueness matches the server's authoritative pre-create FK
    /// gate `validate_fk_column` exactly — both read `is_unique` off the same
    /// `index_circuits` — so the cached client check can never accept an FK the
    /// server would reject, nor reject one it would accept. (The old IDX_TAB
    /// scan could diverge by reporting a phantom unique index masked by the
    /// circuit dedup; the circuit list does not.)
    pub fn index_for_column(&mut self, table_id: u64, col_idx: usize) -> Result<Option<IndexMeta>, ClientError> {
        let list = self.refresh_indices(table_id)?;
        // Exact single-element match: a composite index does NOT answer a
        // single-column FK/uniqueness query (a `(a, b)` index does not guarantee
        // uniqueness of `a` alone).
        Ok(list.iter().find(|m| m.cols.as_slice() == [col_idx as u32]).copied())
    }

    /// The full secondary-index list for `table_id`, served from the same
    /// durable, epoch-validated cache `index_for_column` uses (one GET_INDICES
    /// round-trip, no extra wire traffic). Used by the SQL point-lookup planner,
    /// which must see every index's full declared column list to plan a
    /// leading-prefix seek.
    pub fn table_indexes(&mut self, table_id: u64) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
        self.refresh_indices(table_id)
    }

    /// GET_INDICES refresh-and-cache: send the cached index epoch; on an
    /// unchanged epoch reuse the cached `Arc<Vec<IndexMeta>>`, otherwise decode
    /// the fresh descriptor list and replace the cache entry. The PK column of
    /// each descriptor carries `pack_pk_cols(col_list)`, decoded via the shared
    /// `unpack_pk_cols`; a malformed decode is rejected by the same
    /// `is_well_formed` guard `decode_pk_cols` uses (reading `pks.get(i) as u32`
    /// would truncate the packed `u64`'s high columns and drop the
    /// `PK_LIST_PACKED_FLAG` at bit 63).
    fn refresh_indices(&mut self, table_id: u64) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
        let cached_epoch = self.index_cache.peek(&table_id).map(|(_, e)| *e).unwrap_or(0);
        let (batch, epoch) = self.session.fetch_indices(table_id, cached_epoch)?;
        if epoch == cached_epoch && cached_epoch != 0 {
            // Unchanged: the server only sends the no-data reply with a returned
            // epoch equal to a non-zero cached epoch, so the entry is present.
            return Ok(self.index_cache.get(&table_id).map(|(l, _)| Arc::clone(l)).unwrap());
        }
        let mut fresh = Vec::new();
        if let Some(b) = batch {
            // None ⇒ changed-to-empty list
            for i in b.live_rows() {
                fresh.push(IndexMeta {
                    cols: unpack_well_formed(b.pks.get(i) as u64)?,
                    is_unique: col_u64(&b.columns[1], i)? != 0,
                });
            }
        }
        let rc = Arc::new(fresh);
        self.index_cache.put(table_id, (Arc::clone(&rc), epoch));
        Ok(rc)
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

        // Reject a duplicate name before allocating an index_id: a duplicate would
        // create an undroppable orphan in IDX_TAB (`drop_index_by_name` returns
        // after the first match, leaving the second row with a catalog entry but no
        // circuit). Mirrors the engine DDL path's `index_by_name` check, which the
        // client push otherwise bypasses.
        if self.index_name_cols()?.iter().any(|(name, _)| *name == index_name) {
            return Err(ClientError::ServerError(format!("index '{index_name}' already exists")));
        }

        let index_id = self.alloc_index_id()?;

        let idx_schema = idx_tab_schema();
        let mut batch = ZSetBatch::new(idx_schema);
        BatchAppender::new(&mut batch, idx_schema)
            .add_row(index_id as u128, 1)
            .u64_val(table_id)
            .u64_val(0)
            .u64_val(gnitz_wire::pack_pk_cols(col_indices))
            .str_val(&index_name)
            .u64_val(is_unique as u64)
            .str_val("");

        self.push_ddl(&[(IDX_TAB, idx_schema, batch)])?;
        Ok(index_id)
    }

    pub fn drop_index_by_name(&mut self, index_name: &str) -> Result<(), ClientError> {
        let index_name = canon_name(index_name);
        let (_, idx_batch, _) = self.session.scan(IDX_TAB)?;
        let idx_batch = idx_batch.ok_or_else(|| ClientError::ServerError(format!("index '{index_name}' not found")))?;
        for i in idx_batch.live_rows() {
            let name = col_str(&idx_batch.columns[4], i)?.unwrap_or("");
            if name != index_name {
                continue;
            }

            let index_id = idx_batch.pks.get(i) as u64;
            let owner_id = col_u64(&idx_batch.columns[1], i)?;
            let owner_kind = col_u64(&idx_batch.columns[2], i)?;
            let src_col = col_u64(&idx_batch.columns[3], i)?;
            let is_unique = col_u64(&idx_batch.columns[5], i)?;
            let cache_dir = col_str(&idx_batch.columns[6], i)?.unwrap_or("").to_string();

            let idx_schema = idx_tab_schema();
            let mut batch = ZSetBatch::new(idx_schema);
            BatchAppender::new(&mut batch, idx_schema)
                .add_row(index_id as u128, -1)
                .u64_val(owner_id)
                .u64_val(owner_kind)
                .u64_val(src_col)
                .str_val(&index_name)
                .u64_val(is_unique)
                .str_val(&cache_dir);
            self.push_ddl(&[(IDX_TAB, idx_schema, batch)])?;
            return Ok(());
        }
        Err(ClientError::ServerError(format!("index '{index_name}' not found")))
    }

    /// `(name, indexed columns)` of every live secondary-index IDX_TAB row (name in
    /// canonical lowercase, since `create_index`/`create_table` canonicalize at
    /// store time). `create_index` checks it for a duplicate name; the planner uses
    /// it to reject a re-index of an identical column set under the auto base name
    /// and to disambiguate an auto-generated name against the taken set. Name is
    /// column 4, the packed source columns are column 3 — the same slots
    /// `create_index` writes and `drop_index_by_name` reads.
    pub fn index_name_cols(&mut self) -> Result<Vec<(String, gnitz_wire::PkColList)>, ClientError> {
        let (_, idx_batch, _) = self.session.scan(IDX_TAB)?;
        let Some(idx_batch) = idx_batch else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for i in idx_batch.live_rows() {
            let Some(name) = col_str(&idx_batch.columns[4], i)? else {
                continue;
            };
            let cols = gnitz_wire::unpack_pk_cols(col_u64(&idx_batch.columns[3], i)?);
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

    /// Open a transaction. Errors if one is already open.
    pub fn txn_begin(&mut self) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::ServerError("transaction already open".into()));
        }
        self.txn = Some(TxnBuffer::default());
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
            .map(|(tid, schema, batch, mode)| (*tid, schema, batch, *mode))
            .collect();
        self.session.push_txn(&fam_refs)
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
    fn push_ddl(&mut self, families: &[(u64, &Schema, ZSetBatch)]) -> Result<u64, ClientError> {
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
        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(new_sid as u128, 1)
            .str_val(&name);
        self.push_ddl(&[(SCHEMA_TAB, schema, batch)])?;
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
        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(schema_id as u128, -1)
            .str_val(&name);
        self.push_ddl(&[(SCHEMA_TAB, schema, batch)])?;
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
        unique_pk: bool,
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
        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(tbl_schema);
        BatchAppender::new(&mut tb, tbl_schema)
            .add_row(new_tid as u128, 1)
            .u64_val(schema_id)
            .str_val(&table_name)
            .str_val("")
            .u64_val(pk_packed)
            .u64_val(0)
            .u64_val(gnitz_wire::pack_table_flags(unique_pk, replicated, dist_prefix_len));

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
        let mut families: Vec<(u64, &Schema, ZSetBatch)> = vec![col_family, (TABLE_TAB, tbl_schema, tb)];
        if !unique_indexes.is_empty() {
            let idx_schema = idx_tab_schema();
            let mut idx_batch = ZSetBatch::new(idx_schema);
            {
                let mut a = BatchAppender::new(&mut idx_batch, idx_schema);
                for (spec, &index_id) in unique_indexes.iter().zip(&index_ids) {
                    a.add_row(index_id as u128, 1)
                        .u64_val(new_tid) // owner_id
                        .u64_val(0) // owner_kind = table
                        .u64_val(gnitz_wire::pack_pk_cols(spec.col_indices)) // source_col_idx
                        .str_val(&canon_name(spec.name))
                        .u64_val(1) // is_unique
                        .str_val(""); // cache_directory
                }
            }
            families.push((IDX_TAB, idx_schema, idx_batch));
        }
        self.push_ddl(&families)?;

        Ok(new_tid)
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Result<(), ClientError> {
        let schema_name = canon_name(schema_name);
        let table_name = canon_name(table_name);
        let schema_id = self.lookup_schema_id(&schema_name)?;
        let record = self
            .lookup_table_record(schema_id, &table_name)?
            .ok_or_else(|| ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found")))?;

        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(tbl_schema);
        BatchAppender::new(&mut tb, tbl_schema)
            .add_row(record.tid as u128, -1)
            .u64_val(record.schema_id)
            .str_val(&table_name)
            .str_val(&record.directory)
            .u64_val(record.pk_col_idx)
            .u64_val(record.created_lsn)
            .u64_val(record.flags);
        self.push_ddl(&[(TABLE_TAB, tbl_schema, tb)])?;

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
        )?;
        Ok(vids[0])
    }

    /// Create every view in `views` in one atomic `DDL_TXN`. `views` must be in
    /// dependency (topological) order — VIEW_TAB row order sets each view's
    /// registration `depth`; the distributed backfill tail re-derives
    /// materialization order from the dep-map. Returns the vids in input order.
    ///
    /// **One `BatchAppender` per family spans all views** — the engine's derived
    /// per-family lists (`family_pks_by_sign`, `families.find`) read only the
    /// first block per tid, so a chain must merge every view's COL/DEP/circuit
    /// rows into a single batch per family and one all-`+1` VIEW_TAB batch in
    /// input order. A single `push_ddl_txn` then commits — or, via the engine's
    /// per-family precheck/compensate loop, rolls back — the whole chain.
    ///
    /// `pk_cols` for each view is its physical PK column list — the leading `k`
    /// output slots (`[0]` for a synthetic-PK view, `0..k` for a compound-PK
    /// passthrough).
    pub fn create_view_chain(&mut self, schema_name: &str, views: Vec<PlannedView>) -> Result<Vec<u64>, ClientError> {
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
        let col_s = col_tab_schema();
        let dep_s = dep_tab_schema();
        let nodes_s = circuit_nodes_schema();
        let edges_s = circuit_edges_schema();
        let ncol_s = circuit_node_columns_schema();
        let view_s = view_tab_schema();

        let mut col_batch = ZSetBatch::new(col_s);
        let mut dep_batch = ZSetBatch::new(dep_s);
        let mut nodes_batch = ZSetBatch::new(nodes_s);
        let mut edges_batch = ZSetBatch::new(edges_s);
        let mut ncol_batch = ZSetBatch::new(ncol_s);
        let mut view_batch = ZSetBatch::new(view_s);

        {
            let mut col_a = BatchAppender::new(&mut col_batch, col_s);
            let mut dep_a = BatchAppender::new(&mut dep_batch, dep_s);
            let mut nodes_a = BatchAppender::new(&mut nodes_batch, nodes_s);
            let mut edges_a = BatchAppender::new(&mut edges_batch, edges_s);
            let mut ncol_a = BatchAppender::new(&mut ncol_batch, ncol_s);
            let mut view_a = BatchAppender::new(&mut view_batch, view_s);

            for (pv, vid) in views.into_iter().zip(vids.iter().copied()) {
                // 1. Column records.
                append_col_rows(&mut col_a, vid, OWNER_KIND_VIEW, &pv.output_columns)?;

                // 2. Dependency records — every ScanDelta source_table.
                //
                // CONVERGENCE INVARIANT (all circuit-PK packings below too): the
                // client packs view_id in the LOW u128 half while the engine's
                // `pack_view_pk` packs it in the HIGH half. These produce the
                // IDENTICAL view_id-major big-endian at-rest OPK image only because
                // the client's multi-column `PkColumn::Bytes` arm OPK-encodes each
                // 8-byte PK column independently (low u128 bytes → first column)
                // while the engine writes the whole u128 big-endian — byte-order
                // duals that agree only because every circuit-PK column is exactly
                // 8 bytes and unsigned. `load_circuit` / `retract_rows_by_view`
                // prefix-seek on `view_id.to_be_bytes()` and depend on this. Do NOT
                // "align" the two packings: flipping the client to `(vid << 64) |
                // sub` moves `sub` into the leading at-rest bytes and breaks every
                // view-load prefix seek.
                for dep_tid in pv.circuit.dependencies() {
                    // Compound PK (view_id, dep_table_id): low 8 bytes = view_id.
                    let pk = (vid as u128) | ((dep_tid as u128) << 64);
                    dep_a.add_row(pk, 1).u64_val(0); // dep_view_id
                }

                // 3–5. Materialise the typed circuit into the three-table bundle.
                let rows = pv.circuit.into_rows();
                for (node_id, opcode, src_tab, expr_blob) in &rows.nodes {
                    // Compound PK (view_id, sub=node_id): low 8 bytes = view_id.
                    let pk = (vid as u128) | ((*node_id as u128) << 64);
                    nodes_a.add_row(pk, 1).u64_val(*node_id).u64_val(*opcode);
                    // source_table and expr_program are nullable; the `*_null`
                    // writers set the row bitmap themselves.
                    match src_tab {
                        Some(t) => nodes_a.u64_val(*t),
                        None => nodes_a.u64_null(),
                    };
                    match expr_blob {
                        Some(b) => nodes_a.bytes_val(b),
                        None => nodes_a.bytes_null(),
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

                // 6. View record — the VIEW_TAB register hook triggers server-side
                // compilation. Rows are appended in input (topological) order so
                // each view's registration `depth` derives from its already-present
                // sources. Encode the view PK with the shared wire packer so the
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
                        cache_directory: String::new(),
                        created_lsn: 0,
                        pk_col_idx: pk_packed,
                    },
                );
            }
        }

        // One families entry per tid (mandatory: the engine's derived lists read
        // only the first block per family), in dependency order — the server
        // re-sorts by topo priority anyway.
        let mut families: Vec<(u64, &Schema, ZSetBatch)> = Vec::new();
        families.push((COL_TAB, col_s, col_batch));
        if !dep_batch.is_empty() {
            families.push((DEP_TAB, dep_s, dep_batch));
        }
        if !nodes_batch.is_empty() {
            families.push((CIRCUIT_NODES_TAB, nodes_s, nodes_batch));
        }
        if !edges_batch.is_empty() {
            families.push((CIRCUIT_EDGES_TAB, edges_s, edges_batch));
        }
        if !ncol_batch.is_empty() {
            families.push((CIRCUIT_NODE_COLUMNS_TAB, ncol_s, ncol_batch));
        }
        families.push((VIEW_TAB, view_s, view_batch));

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
        let schema_id = self.lookup_schema_id(&schema_name)?;

        let (_, view_batch, _) = self.session.scan(VIEW_TAB)?;
        let view_batch = view_batch
            .ok_or_else(|| ClientError::ServerError(format!("View '{schema_name}.{view_name}' not found")))?;
        let vr = find_view_record(&view_batch, schema_id, &view_name)?
            .ok_or_else(|| ClientError::ServerError(format!("View '{schema_name}.{view_name}' not found")))?;

        // Hidden segment members this view owns. Ownership is name-encoded
        // (hidden views are never shared across user views); the shared
        // `hidden_view_prefix` keeps the producer (planner) and this consumer on
        // one definition.
        let prefix = hidden_view_prefix(vr.vid);
        let members = collect_view_records_with_prefix(&view_batch, schema_id, &prefix)?;

        // One VIEW_TAB batch: the user view's `-1` plus every hidden member's
        // `-1`, full payload reproduced per record.
        let view_s = view_tab_schema();
        let mut vb = ZSetBatch::new(view_s);
        {
            let mut a = BatchAppender::new(&mut vb, view_s);
            for rec in std::iter::once(&vr).chain(members.iter()) {
                append_view_row(&mut a, -1, rec);
            }
        }
        self.push_ddl(&[(VIEW_TAB, view_s, vb)])?;

        Ok(())
    }

    pub fn resolve_table_id(&mut self, schema_name: &str, table_name: &str) -> Result<(u64, Schema), ClientError> {
        let schema_name = canon_name(schema_name);
        let table_name = canon_name(table_name);
        let schema_id = self.lookup_schema_id(&schema_name)?;
        let record = self
            .lookup_table_record(schema_id, &table_name)?
            .ok_or_else(|| ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found")))?;
        let schema = self.load_owner_schema(record.tid, OWNER_KIND_TABLE, record.pk_col_idx)?;
        Ok((record.tid, schema))
    }

    pub fn resolve_table_or_view_id(&mut self, schema_name: &str, name: &str) -> Result<(u64, Schema), ClientError> {
        let schema_name = canon_name(schema_name);
        let name = canon_name(name);
        let schema_id = self.lookup_schema_id(&schema_name)?;

        // Try TABLE_TAB first (most common path). A decode error there is real
        // catalog corruption and must surface — only a genuine miss falls
        // through to the view branch.
        if let Some(record) = self.lookup_table_record(schema_id, &name)? {
            let schema = self.load_owner_schema(record.tid, OWNER_KIND_TABLE, record.pk_col_idx)?;
            return Ok((record.tid, schema));
        }

        let view_batch = self.scan_catalog(VIEW_TAB)?;
        let view_batch = view_batch
            .ok_or_else(|| ClientError::ServerError(format!("Table or view '{schema_name}.{name}' not found")))?;
        let record = find_view_record(&view_batch, schema_id, &name)?
            .ok_or_else(|| ClientError::ServerError(format!("Table or view '{schema_name}.{name}' not found")))?;
        // The view PK is the persisted leading-k column list: a single synthetic
        // hash column for join/set-op/distinct views, or the source PK passed
        // through (0..k) for a plain projection over a compound-PK table.
        let schema = self.load_owner_schema(record.vid, OWNER_KIND_VIEW, record.pk_col_idx)?;
        Ok((record.vid, schema))
    }

    /// True iff base table `tid` is REPLICATED (decoded from `TABLE_TAB.flags`).
    /// Scans `TABLE_TAB` and matches by PK; a `tid` that is not a base table
    /// (e.g. a view id, absent from `TABLE_TAB`) yields `false`. The SQL planner
    /// consults this when building an aggregate directly over a source: a reduce
    /// over a replicated relation must be the shard-free `reduce_multi_local`
    /// (every worker holds the full copy, so a sharded reduce would
    /// N-fold-multiply the aggregate — see the replicated-tables design).
    pub fn table_replicated(&mut self, tid: u64) -> Result<bool, ClientError> {
        let tbl_batch = self.scan_catalog(TABLE_TAB)?;
        let Some(tbl_batch) = tbl_batch else {
            return Ok(false);
        };
        for i in tbl_batch.live_rows() {
            if tbl_batch.pks.get(i) as u64 != tid {
                continue;
            }
            return Ok(gnitz_wire::table_flags_replicated(col_u64(&tbl_batch.columns[6], i)?));
        }
        Ok(false)
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

    /// Find `table_name`'s TABLE_TAB record under `schema_id`. `Ok(None)` = the
    /// name is absent (an empty TABLE_TAB folds into the same miss); `Err` = a
    /// decode error on a corrupt batch, which must surface rather than be
    /// masked as a miss.
    fn lookup_table_record(&mut self, schema_id: u64, table_name: &str) -> Result<Option<TableRecord>, ClientError> {
        let batch = self.scan_catalog(TABLE_TAB)?;
        match &batch {
            Some(b) => find_table_record(b, schema_id, table_name),
            None => Ok(None),
        }
    }

    /// Load an owner's (table's or view's) columns from COL_TAB and assemble
    /// its validated `Schema`. Called only on a resolved owner, so miss paths
    /// never pay the COL_TAB scan.
    fn load_owner_schema(&mut self, owner_id: u64, owner_kind: u64, pk_col_idx: u64) -> Result<Schema, ClientError> {
        let col_batch = self.scan_catalog(COL_TAB)?;
        let col_batch = col_batch.ok_or_else(|| ClientError::ServerError("COL_TAB is empty".to_string()))?;
        let columns = extract_col_entries(&col_batch, owner_id, owner_kind)?;
        assemble_schema(columns, pk_col_idx)
    }
}

// --- TxnBuffer: the locally-buffered atomic write-batch transaction ---

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
    /// (tid, schema, batch, mode) in creation order; per tid, maximal same-mode
    /// runs of the caller's op sequence.
    families: Vec<(u64, Schema, ZSetBatch, WireConflictMode)>,
    /// tid → index in `families` of that tid's most recently opened family, so
    /// a matching-mode append extends it rather than opening a new family.
    last_family_of: HashMap<u64, usize>,
    /// tid → PK → `(family index, row index)` of the LAST op buffered on that
    /// PK. Row indices are stable: `append` only ever extends a family batch or
    /// pushes a new one. Weight-0 rows are not indexed — they are inert, exactly
    /// as the engine's fold treats them.
    last_op_of: HashMap<u64, HashMap<PkTuple, (usize, usize)>>,
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

    /// Append one op's `batch` to `tid`'s current run, or open a new family when
    /// the mode differs (or `tid` has no family yet). Empty batches contribute
    /// nothing and open no family.
    fn append(&mut self, tid: u64, schema: &Schema, batch: ZSetBatch, mode: WireConflictMode) {
        if batch.is_empty() {
            return;
        }
        let stride = schema.pk_stride() as u8;
        let extend = self
            .last_family_of
            .get(&tid)
            .is_some_and(|&idx| self.families[idx].3 == mode);
        let (fam, base) = if extend {
            let idx = self.last_family_of[&tid];
            (idx, self.families[idx].2.len())
        } else {
            (self.families.len(), 0)
        };

        let index = self.last_op_of.entry(tid).or_default();
        for i in 0..batch.len() {
            if batch.weights[i] != 0 {
                index.insert(batch.pks.get_tuple(i, stride), (fam, base + i));
            }
        }

        if extend {
            self.families[fam].2.extend_from_owned(batch);
        } else {
            self.families.push((tid, schema.clone(), batch, mode));
            self.last_family_of.insert(tid, fam);
        }
    }

    /// The last op buffered on `pk` in `tid`, as `(batch, row)` — or `None` if
    /// the transaction has not touched that PK. The row's **weight sign** is the
    /// net effect (positive: live row with that payload; negative: deleted),
    /// mirroring the engine's `fold_family`.
    pub fn last_op(&self, tid: u64, pk: &PkTuple) -> Option<(&ZSetBatch, usize)> {
        let &(fam, row) = self.last_op_of.get(&tid)?.get(pk)?;
        Some((&self.families[fam].2, row))
    }

    /// Every PK the transaction has touched in `tid`, with its last op.
    pub fn last_ops(&self, tid: u64) -> impl Iterator<Item = (PkTuple, &ZSetBatch, usize)> + '_ {
        self.last_op_of
            .get(&tid)
            .into_iter()
            .flatten()
            .map(move |(pk, &(fam, row))| (*pk, &self.families[fam].2, row))
    }
}

fn extract_col_entries(col_batch: &ZSetBatch, owner_id: u64, owner_kind: u64) -> Result<Vec<ColumnDef>, ClientError> {
    // Keyed by COL_TAB `col_idx` so the returned columns are in schema order
    // regardless of storage/scan order.
    let mut col_entries: Vec<(u64, ColumnDef)> = Vec::new();
    for i in col_batch.live_rows() {
        let row_owner_id = col_u64(&col_batch.columns[1], i)?;
        let row_owner_kind = col_u64(&col_batch.columns[2], i)?;
        if row_owner_id != owner_id || row_owner_kind != owner_kind {
            continue;
        }

        let col_idx = col_u64(&col_batch.columns[3], i)?;
        let name = col_str(&col_batch.columns[4], i)?.unwrap_or("").to_string();
        let tc_val = col_u64(&col_batch.columns[5], i)?;
        let type_code = type_code_from_u64(tc_val).map_err(ClientError::Protocol)?;
        col_entries.push((
            col_idx,
            ColumnDef {
                name,
                type_code,
                is_nullable: col_u64(&col_batch.columns[6], i)? != 0,
                fk_table_id: col_u64(&col_batch.columns[7], i)?,
                fk_col_idx: col_u64(&col_batch.columns[8], i)?,
                is_serial: col_u64(&col_batch.columns[9], i)? != 0,
                is_hidden: col_u64(&col_batch.columns[10], i)? != 0,
            },
        ));
    }
    col_entries.sort_by_key(|e| e.0);
    // The physical `col_idx` values must be exactly `0..n`. A gap or duplicate
    // (`[0, 2]`) would silently condense to contiguous positions, shifting every
    // column's schema position relative to the server's physical layout — a
    // silent type/offset corruption. Surface it as an error instead.
    for (expected, (actual, _)) in col_entries.iter().enumerate() {
        if *actual != expected as u64 {
            return Err(ClientError::ServerError(format!(
                "COL_TAB for owner {owner_id} (kind {owner_kind}): column index gap or \
                 duplicate — expected {expected}, got {actual}"
            )));
        }
    }
    Ok(col_entries.into_iter().map(|(_, cd)| cd).collect())
}

/// Assemble and validate a `Schema` from decoded catalog parts.
/// `Schema::from_parts` (shared with the wire schema-block decoder) checks the
/// PK indices — bounds, duplicates, non-nullable, PK-eligible type — so a
/// corrupt `pk_col_idx` or COL_TAB row surfaces as a clean error here rather
/// than a `columns[ci]` panic in `pk_stride`/sort-merge downstream.
fn assemble_schema(columns: Vec<ColumnDef>, pk_col_idx: u64) -> Result<Schema, ClientError> {
    let pk_cols = decode_pk_cols(pk_col_idx)?;
    Schema::from_parts(columns, pk_cols)
        .map_err(|e| ClientError::ServerError(format!("catalog schema is invalid: {e}")))
}

/// `Ok(None)` = name absent (a legitimate miss); `Err` = a decode error on a
/// corrupt catalog batch.
fn find_schema_id(batch: &ZSetBatch, name: &str) -> Result<Option<u64>, ClientError> {
    for i in batch.live_rows() {
        if col_str(&batch.columns[1], i)? == Some(name) {
            return Ok(Some(batch.pks.get(i) as u64));
        }
    }
    Ok(None)
}

/// `Ok(None)` = name absent (a legitimate miss); `Err` = a decode error on a
/// corrupt `TABLE_TAB` batch.
fn find_table_record(batch: &ZSetBatch, schema_id: u64, table_name: &str) -> Result<Option<TableRecord>, ClientError> {
    for i in batch.live_rows() {
        if col_u64(&batch.columns[1], i)? != schema_id {
            continue;
        }
        if col_str(&batch.columns[2], i)? != Some(table_name) {
            continue;
        }
        return Ok(Some(TableRecord {
            tid: batch.pks.get(i) as u64,
            schema_id,
            directory: col_str(&batch.columns[3], i)?.unwrap_or("").to_string(),
            pk_col_idx: col_u64(&batch.columns[4], i)?,
            created_lsn: col_u64(&batch.columns[5], i)?,
            flags: col_u64(&batch.columns[6], i)?,
        }));
    }
    Ok(None)
}

/// Decode row `i` of a `VIEW_TAB` batch into a `ViewRecord` — the single home
/// for the VIEW_TAB column layout on the read side (`append_view_row` is the
/// write side); every row filter builds on it.
fn decode_view_record(batch: &ZSetBatch, i: usize) -> Result<ViewRecord, ClientError> {
    Ok(ViewRecord {
        vid: batch.pks.get(i) as u64,
        schema_id: col_u64(&batch.columns[1], i)?,
        name: col_str(&batch.columns[2], i)?.unwrap_or("").to_string(),
        sql_definition: col_str(&batch.columns[3], i)?.unwrap_or("").to_string(),
        cache_directory: col_str(&batch.columns[4], i)?.unwrap_or("").to_string(),
        created_lsn: col_u64(&batch.columns[5], i)?,
        pk_col_idx: col_u64(&batch.columns[6], i)?,
    })
}

/// Append one `VIEW_TAB` row — the single home for the VIEW_TAB payload layout
/// on the write side, mirroring `decode_view_record`. A drop's `-1` row must
/// reproduce the `+1`'s payload byte-for-byte to cancel in the Z-set, so the
/// create and drop-cascade paths both write through here.
fn append_view_row(a: &mut BatchAppender<'_>, weight: i64, rec: &ViewRecord) {
    a.add_row(rec.vid as u128, weight)
        .u64_val(rec.schema_id)
        .str_val(&rec.name)
        .str_val(&rec.sql_definition)
        .str_val(&rec.cache_directory)
        .u64_val(rec.created_lsn)
        .u64_val(rec.pk_col_idx);
}

/// `Ok(None)` = absent (a legitimate miss); `Err` = a decode error on a corrupt
/// `VIEW_TAB` batch.
fn find_view_record(batch: &ZSetBatch, schema_id: u64, view_name: &str) -> Result<Option<ViewRecord>, ClientError> {
    for i in batch.live_rows() {
        if col_u64(&batch.columns[1], i)? != schema_id {
            continue;
        }
        if col_str(&batch.columns[2], i)? != Some(view_name) {
            continue;
        }
        return Ok(Some(decode_view_record(batch, i)?));
    }
    Ok(None)
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
        if col_u64(&batch.columns[1], i)? != schema_id {
            continue;
        }
        if !matches!(col_str(&batch.columns[2], i)?, Some(n) if n.starts_with(prefix)) {
            continue;
        }
        out.push(decode_view_record(batch, i)?);
    }
    Ok(out)
}

/// Collect the entity names of every live row in a `TABLE_TAB`/`VIEW_TAB` batch
/// whose `schema_id` column matches. Both families carry `schema_id` at column 1
/// and the entity name at column 2, so this mirrors `find_table_record` /
/// `find_view_record`'s scan minus the name filter — keeping the whole set rather
/// than one row. Drives the `drop_schema` member cascade.
fn collect_schema_member_names(batch: &ZSetBatch, schema_id: u64) -> Result<Vec<String>, ClientError> {
    let mut out = Vec::new();
    for i in batch.live_rows() {
        if col_u64(&batch.columns[1], i)? != schema_id {
            continue;
        }
        if let Some(name) = col_str(&batch.columns[2], i)? {
            out.push(name.to_string());
        }
    }
    Ok(out)
}

/// Append one owner's `COL_TAB` column records to `a` — the single home for the
/// COL_TAB row layout, shared by the table path (`build_col_tab_batch`) and the
/// view-chain path (which merges every view's rows into one family batch).
fn append_col_rows(
    a: &mut BatchAppender<'_>,
    owner_id: u64,
    owner_kind: u64,
    columns: &[ColumnDef],
) -> Result<(), ClientError> {
    for (i, col) in columns.iter().enumerate() {
        a.add_row(pack_col_id(owner_id, i)? as u128, 1)
            .u64_val(owner_id)
            .u64_val(owner_kind)
            .u64_val(i as u64)
            .str_val(&col.name)
            .u64_val(col.type_code as u64)
            .u64_val(if col.is_nullable { 1 } else { 0 })
            .u64_val(col.fk_table_id)
            .u64_val(col.fk_col_idx)
            .u64_val(if col.is_serial { 1 } else { 0 })
            .u64_val(if col.is_hidden { 1 } else { 0 });
    }
    Ok(())
}

/// Build the `COL_TAB` family batch for a table/view's column records — a pure
/// batch builder returning `(target_id, schema, batch)` for `push_ddl_txn` to
/// bundle. Touches no connection state; the DDL's whole family set is ingested
/// atomically server-side, so column records no longer need a standalone RPC.
fn build_col_tab_batch(
    owner_id: u64,
    owner_kind: u64,
    columns: &[ColumnDef],
) -> Result<(u64, &'static Schema, ZSetBatch), ClientError> {
    let schema = col_tab_schema();
    let mut batch = ZSetBatch::new(schema);
    append_col_rows(
        &mut BatchAppender::new(&mut batch, schema),
        owner_id,
        owner_kind,
        columns,
    )?;
    Ok((COL_TAB, schema, batch))
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
        assert_eq!(buf.families[0].2.len(), 2);
        assert_eq!(buf.families[0].3, WireConflictMode::Update);
        buf.push_with_mode(tid, &s, &ins(&s, 3, 30), WireConflictMode::Error);
        assert_eq!(buf.families.len(), 2, "mode change opens a new family");
        assert_eq!(buf.families[1].3, WireConflictMode::Error);
        buf.push(tid, &s, &ins(&s, 4, 40));
        assert_eq!(
            buf.families.len(),
            3,
            "back to Update opens a third family in call order"
        );
        assert_eq!(buf.families[2].3, WireConflictMode::Update);
    }

    #[test]
    fn delete_buffers_into_update_family_before_error_reinsert() {
        // "delete k; insert_error k" → Update family [D(k)] then Error family [I(k)].
        let s = kv_schema();
        let mut buf = TxnBuffer::default();
        let tid = 16u64;
        buf.delete(tid, &s, PkColumn::U64s(vec![7]));
        buf.push_with_mode(tid, &s, &ins(&s, 7, 70), WireConflictMode::Error);
        assert_eq!(buf.families.len(), 2);
        assert_eq!(buf.families[0].3, WireConflictMode::Update);
        assert_eq!(buf.families[0].2.weights, vec![-1]);
        assert_eq!(buf.families[1].3, WireConflictMode::Error);
        assert_eq!(buf.families[1].2.weights, vec![1]);
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
        assert_eq!(buf.families[0].0, 16);
        assert_eq!(buf.families[0].2.len(), 2);
        assert_eq!(buf.families[1].0, 17);
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
        buf.delete(tid, &s, PkColumn::U64s(vec![1])); // family 2, row 0 — supersedes pk=1

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
    fn pack_col_id_rejects_col_idx_too_large() {
        assert!(pack_col_id(1, 511).is_ok());
        assert!(pack_col_id(1, 512).is_err());
    }

    #[test]
    fn pack_col_id_rejects_owner_id_too_large() {
        let max_valid = u64::MAX >> 9;
        assert!(pack_col_id(max_valid, 0).is_ok());
        assert!(pack_col_id(max_valid + 1, 0).is_err());
        assert!(pack_col_id(u64::MAX, 0).is_err());
    }

    #[test]
    fn pack_col_id_roundtrip() {
        let id = pack_col_id(12345, 7).unwrap();
        assert_eq!(id >> 9, 12345);
        assert_eq!(id & 0x1FF, 7);
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
    fn decode_pk_cols_bare_single() {
        let v = decode_pk_cols(3).unwrap();
        assert_eq!(v, vec![3]);
    }

    #[test]
    fn decode_pk_cols_packed_compound() {
        let packed = gnitz_wire::pack_pk_cols(&[2, 5, 7, 1]);
        let v = decode_pk_cols(packed).unwrap();
        assert_eq!(v, vec![2, 5, 7, 1]);
    }

    #[test]
    fn decode_pk_cols_rejects_overflow_count() {
        // Craft a count one past the cap (still inside the 4-bit count field;
        // the layout guard keeps PK_LIST_MAX_COLS ≤ 8). `as_slice()` clamps to
        // PK_LIST_MAX_COLS, so the decoded_count/len divergence always fires and
        // the client must surface it as an error instead of silently truncating.
        let bad_count = gnitz_wire::PK_LIST_MAX_COLS + 1;
        let bad = gnitz_wire::PK_LIST_PACKED_FLAG | bad_count as u64;
        let err = decode_pk_cols(bad).expect_err(&format!("expected error on count={bad_count}"));
        match err {
            ClientError::ServerError(s) => assert!(
                s.contains(&bad_count.to_string()),
                "expected '{bad_count}' in message, got: {s}"
            ),
            other => panic!("expected ServerError, got {other:?}"),
        }
    }

    #[test]
    fn find_table_record_surfaces_decode_error_not_miss() {
        let schema = table_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(7, 1)
            .u64_val(1) // schema_id
            .str_val("t") // name
            .str_val("") // directory
            .u64_val(0) // pk_col_idx
            .u64_val(0) // created_lsn
            .u64_val(0); // flags

        // Truncate columns[1] (schema_id, Fixed) so `col_u64` on the live row is
        // out of bounds — a real decode error, which must surface as Err rather
        // than be masked as an absent-name miss.
        let ColData::Fixed(bytes) = &mut batch.columns[1] else {
            panic!("expected Fixed column");
        };
        bytes.clear();
        match find_table_record(&batch, 1, "t") {
            Err(ClientError::ServerError(s)) => assert!(s.contains("out of bounds"), "got: {s}"),
            _ => panic!("expected decode ServerError, got a non-error result"),
        }
    }

    #[test]
    fn find_table_record_miss_is_none() {
        let schema = table_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(7, 1)
            .u64_val(1)
            .str_val("t")
            .str_val("")
            .u64_val(0)
            .u64_val(0)
            .u64_val(0);
        assert!(find_table_record(&batch, 1, "absent").unwrap().is_none());
        assert!(find_table_record(&batch, 1, "t").unwrap().is_some());
    }

    #[test]
    fn find_schema_id_miss_vs_hit() {
        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema).add_row(3, 1).str_val("foo");
        assert_eq!(find_schema_id(&batch, "foo").unwrap(), Some(3));
        assert!(find_schema_id(&batch, "bar").unwrap().is_none());
    }

    #[test]
    fn assemble_schema_rejects_out_of_range_pk() {
        let cols = vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ];
        // pk_col_idx 5 decodes to the single-column PK `[5]`, out of range for a
        // 2-column table — must surface here, not panic downstream.
        match assemble_schema(cols.clone(), 5).expect_err("expected invalid schema") {
            ClientError::ServerError(s) => assert!(s.contains("catalog schema is invalid"), "got: {s}"),
            other => panic!("expected ServerError, got {other:?}"),
        }
        // A valid `(columns, pk_col_idx)` returns the Schema.
        let assembled = assemble_schema(cols, 0).unwrap();
        assert_eq!(assembled.pk_cols, vec![0]);
        assert_eq!(assembled.columns.len(), 2);
    }

    fn push_col_row(a: &mut BatchAppender<'_>, owner_id: u64, col_idx: u64, name: &str) {
        a.add_row(pack_col_id(owner_id, col_idx as usize).unwrap() as u128, 1)
            .u64_val(owner_id) // owner_id
            .u64_val(OWNER_KIND_TABLE) // owner_kind
            .u64_val(col_idx) // col_idx
            .str_val(name) // name
            .u64_val(TypeCode::U64 as u64) // type_code
            .u64_val(0) // is_nullable
            .u64_val(0) // fk_table_id
            .u64_val(0) // fk_col_idx
            .u64_val(0) // is_serial
            .u64_val(0); // is_hidden
    }

    #[test]
    fn extract_col_entries_rejects_index_gap() {
        let schema = col_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        {
            let mut a = BatchAppender::new(&mut batch, schema);
            push_col_row(&mut a, 1, 0, "a");
            push_col_row(&mut a, 1, 2, "c"); // gap: col_idx 1 is missing
        }
        match extract_col_entries(&batch, 1, OWNER_KIND_TABLE).expect_err("expected gap error") {
            ClientError::ServerError(s) => assert!(s.contains("column index gap or duplicate"), "got: {s}"),
            other => panic!("expected ServerError, got {other:?}"),
        }
    }

    #[test]
    fn extract_col_entries_contiguous_ok() {
        let schema = col_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        {
            let mut a = BatchAppender::new(&mut batch, schema);
            push_col_row(&mut a, 1, 1, "b"); // out of storage order
            push_col_row(&mut a, 1, 0, "a");
        }
        let cols = extract_col_entries(&batch, 1, OWNER_KIND_TABLE).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "a");
        assert_eq!(cols[1].name, "b");
    }
}
