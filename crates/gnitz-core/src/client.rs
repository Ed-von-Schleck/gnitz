use std::sync::Arc;
use lru::LruCache;
use gnitz_wire::RangeDescriptor;
use crate::protocol::{Schema, ColumnDef, TypeCode, ZSetBatch, ColData, BatchAppender, PkColumn, PkTuple, WireConflictMode};
use crate::protocol::types::type_code_from_u64;
use crate::connection::{Connection, ScanResult, SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, IDX_TAB};
use crate::error::ClientError;

const SCHEMA_CACHE_CAP: std::num::NonZeroUsize = std::num::NonZeroUsize::new(64).unwrap();
use crate::types::{
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_NODE_COLUMNS_TAB,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    schema_tab_schema, table_tab_schema, col_tab_schema, view_tab_schema,
    dep_tab_schema, idx_tab_schema, circuit_nodes_schema, circuit_edges_schema,
    circuit_node_columns_schema,
};
use crate::circuit::Circuit;

// --- Module-private helpers ---

fn col_u64(col: &ColData, i: usize) -> Result<u64, ClientError> {
    match col {
        ColData::Fixed(bytes) => {
            let off = i * 8;
            if off + 8 > bytes.len() {
                return Err(ClientError::ServerError(
                    format!("col_u64: row {} out of bounds (len {})", i, bytes.len())));
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
                return Err(ClientError::ServerError(
                    format!("col_str: row {} out of bounds (len {})", i, v.len())));
            }
            Ok(v[i].as_deref())
        }
        _ => Err(ClientError::ServerError("col_str: expected Strings column".into())),
    }
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

/// Decode the persisted PK-list `u64` (from `TABLE_TAB.pk_col_idx`) into a
/// `Vec<usize>` for the `Schema` type. The shared `gnitz_wire::unpack_pk_cols`
/// handles both the bare-scalar (pre-compound) row form and the packed
/// compound form. Rejects a malformed count (`is_well_formed`) — an empty or
/// over-`PK_LIST_MAX_COLS` payload the server's `validate_pk_cols` would have
/// rejected too; silently truncating here would hide the divergence from the
/// client.
fn decode_pk_cols(packed: u64) -> Result<Vec<usize>, ClientError> {
    let pkl = gnitz_wire::unpack_pk_cols(packed);
    if !pkl.is_well_formed() {
        return Err(ClientError::ServerError(format!(
            "PK column count {} out of range 1..={}",
            pkl.decoded_count(), gnitz_wire::PK_LIST_MAX_COLS
        )));
    }
    Ok(pkl.as_slice().iter().map(|&c| c as usize).collect())
}

fn pack_col_id(owner_id: u64, col_idx: usize) -> Result<u64, ClientError> {
    if col_idx >= 512 {
        return Err(ClientError::ServerError(
            format!("column index {col_idx} exceeds maximum 511")));
    }
    if owner_id > (u64::MAX >> 9) {
        return Err(ClientError::ServerError(
            format!("owner_id {owner_id} exceeds 55-bit maximum for column ID packing")));
    }
    Ok((owner_id << 9) | col_idx as u64)
}

// --- Internal record types ---

struct TableRecord {
    tid:         u64,
    schema_id:   u64,
    name:        String,
    directory:   String,
    pk_col_idx:  u64,
    created_lsn: u64,
    flags:       u64,
}

struct ViewRecord {
    vid:             u64,
    schema_id:       u64,
    name:            String,
    sql_definition:  String,
    cache_directory: String,
    created_lsn:     u64,
    pk_col_idx:      u64,
}

// --- GnitzClient ---

/// A tiny owned secondary-index descriptor. `index_id` has no consumer, so it
/// is omitted; `cols` is the index's declared column list (the unique key — the
/// circuit list is deduped by column list) and `is_unique` is the system's
/// operative uniqueness truth. `PkColList` is `Copy`, so `IndexMeta` stays
/// `Copy` (no `.cloned()` churn at its consumers).
#[derive(Clone, Copy, Debug)]
pub struct IndexMeta { pub cols: gnitz_wire::PkColList, pub is_unique: bool }

pub struct GnitzClient {
    conn:         Connection,
    schema_cache: LruCache<u64, (Arc<Schema>, u16)>,
    index_cache:  LruCache<u64, (Arc<Vec<IndexMeta>>, u8)>,
}

impl GnitzClient {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError> {
        Ok(GnitzClient {
            conn:         Connection::connect(socket_path)?,
            schema_cache: LruCache::new(SCHEMA_CACHE_CAP),
            index_cache:  LruCache::new(SCHEMA_CACHE_CAP),
        })
    }

    pub fn close(self) {
        self.conn.close();
    }

    // --- Raw ops ---

    pub fn alloc_table_id(&mut self) -> Result<u64, ClientError> {
        self.conn.alloc_table_id()
    }

    pub fn alloc_schema_id(&mut self) -> Result<u64, ClientError> {
        self.conn.alloc_schema_id()
    }

    pub fn alloc_index_id(&mut self) -> Result<u64, ClientError> {
        self.conn.alloc_index_id()
    }

    pub fn push(&mut self, table_id: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, ClientError> {
        self.conn.push(table_id, schema, batch, &mut self.schema_cache)
    }

    /// Push with an explicit `WireConflictMode`. SQL `INSERT` uses
    /// `Error` to get SQL-standard rejection semantics; all other
    /// callers pass `Update` (or use the plain `push` which defaults
    /// to `Update` for backward compatibility).
    pub fn push_with_mode(
        &mut self, table_id: u64, schema: &Schema, batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        self.conn.push_with_mode(table_id, schema, batch, mode, &mut self.schema_cache)
    }

    pub fn scan(&mut self, table_id: u64) -> ScanResult {
        self.conn.scan(table_id, &mut self.schema_cache)
    }

    pub fn seek(
        &mut self,
        table_id: u64,
        pk:       &PkTuple,
    ) -> ScanResult {
        self.conn.seek(table_id, pk, &mut self.schema_cache)
    }

    /// Seek a secondary index by `col_indices` (the index's FULL declared column
    /// list — the server matches the circuit by exact list) supplying `key_vals`
    /// native key values. `key_vals.len()` may be `< col_indices.len()` for a
    /// leading-prefix seek. This is the single choke point shared by the SQL
    /// planner and every binding; the two arity guards below are load-bearing,
    /// not cosmetic — they prevent a `pack_pk_cols` panic and a silently-misread
    /// frame (the worker derives the value count from the wire byte length).
    pub fn seek_by_index(
        &mut self, table_id: u64, col_indices: &[u32], key_vals: &[u128],
    ) -> ScanResult {
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
                key_vals.len(), col_indices.len())));
        }
        self.conn.seek_by_index(table_id, col_indices, key_vals, &mut self.schema_cache)
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
    pub fn seek_by_index_range(
        &mut self,
        table_id:    u64,
        col_indices: &[u32],
        desc:        &RangeDescriptor,
    ) -> ScanResult {
        gnitz_wire::validate_pk_col_list(col_indices)
            .map_err(|e| ClientError::ServerError(format!("seek_by_index_range: {e}")))?;
        // The range column sits right after the equality prefix, so the prefix
        // must be strictly shorter than the index column list. (Written `>=`,
        // never a `+ 1` that could overflow on an adversarial length.)
        if desc.eq_vals().len() >= col_indices.len() {
            return Err(ClientError::ServerError(format!(
                "seek_by_index_range: {} equality values leave no range column \
                 within index arity {}", desc.eq_vals().len(), col_indices.len())));
        }
        self.conn.seek_by_index_range(table_id, col_indices, desc, &mut self.schema_cache)
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
    pub fn index_for_column(
        &mut self, table_id: u64, col_idx: usize,
    ) -> Result<Option<IndexMeta>, ClientError> {
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
    pub fn table_indexes(
        &mut self, table_id: u64,
    ) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
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
    fn refresh_indices(
        &mut self, table_id: u64,
    ) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
        let cached_epoch = self.index_cache.peek(&table_id).map(|(_, e)| *e).unwrap_or(0);
        let (batch, epoch) = self.conn.fetch_indices(table_id, cached_epoch)?;
        if epoch == cached_epoch && cached_epoch != 0 {
            // Unchanged: the server only sends the no-data reply with a returned
            // epoch equal to a non-zero cached epoch, so the entry is present.
            return Ok(self.index_cache.get(&table_id).map(|(l, _)| Arc::clone(l)).unwrap());
        }
        let mut fresh = Vec::new();
        if let Some(b) = batch {                 // None ⇒ changed-to-empty list
            for i in 0..b.len() {
                if b.weights[i] <= 0 { continue; }
                let cols = gnitz_wire::unpack_pk_cols(b.pks.get(i) as u64);
                if !cols.is_well_formed() {
                    return Err(ClientError::ServerError(format!(
                        "index column count {} out of range 1..={}",
                        cols.decoded_count(), gnitz_wire::PK_LIST_MAX_COLS)));
                }
                fresh.push(IndexMeta {
                    cols,
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
        &mut self, table_id: u64, col_indices: &[u32], col_types: &[TypeCode],
        index_name: &str, is_unique: bool,
    ) -> Result<u64, ClientError> {
        // Arity, 7-bit column range, duplicates — the Err form of the
        // pack_pk_cols contract, so the pack below can never panic.
        gnitz_wire::validate_pk_col_list(col_indices)
            .map_err(|e| ClientError::ServerError(format!("create_index: {e}")))?;
        if col_types.len() != col_indices.len() {
            return Err(ClientError::ServerError(
                "create_index: col_indices and col_types length mismatch".to_string()));
        }
        for &ct in col_types {
            validate_index_col_type(ct)?;
        }

        // Reject a duplicate name before allocating an index_id: a duplicate would
        // create an undroppable orphan in IDX_TAB (`drop_index_by_name` returns
        // after the first match, leaving the second row with a catalog entry but no
        // circuit). Mirrors the engine DDL path's `index_by_name` check, which the
        // client push otherwise bypasses.
        let (_, existing, _) = self.conn.scan(IDX_TAB, &mut self.schema_cache)?;
        if let Some(idx_batch) = existing {
            for i in 0..idx_batch.len() {
                if idx_batch.weights[i] <= 0 { continue; }
                let name = col_str(&idx_batch.columns[4], i)?.unwrap_or("");
                if name == index_name {
                    return Err(ClientError::ServerError(format!(
                        "index '{index_name}' already exists"
                    )));
                }
            }
        }

        let index_id = self.alloc_index_id()?;

        let idx_schema = idx_tab_schema();
        let mut batch = ZSetBatch::new(idx_schema);
        BatchAppender::new(&mut batch, idx_schema)
            .add_row(index_id as u128, 1)
            .u64_val(table_id)
            .u64_val(0)
            .u64_val(gnitz_wire::pack_pk_cols(col_indices))
            .str_val(index_name)
            .u64_val(is_unique as u64)
            .str_val("");

        self.push(IDX_TAB, idx_schema, &batch)?;
        Ok(index_id)
    }

    pub fn drop_index_by_name(&mut self, index_name: &str) -> Result<(), ClientError> {
        let (_, idx_batch, _) = self.conn.scan(IDX_TAB, &mut self.schema_cache)?;
        let idx_batch = idx_batch.ok_or_else(|| {
            ClientError::ServerError(format!("index '{index_name}' not found"))
        })?;
        for i in 0..idx_batch.len() {
            if idx_batch.weights[i] <= 0 { continue; }
            let name = col_str(&idx_batch.columns[4], i)?.unwrap_or("");
            if name != index_name { continue; }

            let index_id   = idx_batch.pks.get(i) as u64;
            let owner_id   = col_u64(&idx_batch.columns[1], i)?;
            let owner_kind = col_u64(&idx_batch.columns[2], i)?;
            let src_col    = col_u64(&idx_batch.columns[3], i)?;
            let is_unique  = col_u64(&idx_batch.columns[5], i)?;
            let cache_dir  = col_str(&idx_batch.columns[6], i)?.unwrap_or("").to_string();

            let idx_schema = idx_tab_schema();
            let mut batch = ZSetBatch::new(idx_schema);
            BatchAppender::new(&mut batch, idx_schema)
                .add_row(index_id as u128, -1)
                .u64_val(owner_id)
                .u64_val(owner_kind)
                .u64_val(src_col)
                .str_val(index_name)
                .u64_val(is_unique)
                .str_val(&cache_dir);
            self.push(IDX_TAB, idx_schema, &batch)?;
            return Ok(());
        }
        Err(ClientError::ServerError(format!("index '{index_name}' not found")))
    }

    pub fn delete(
        &mut self,
        table_id: u64,
        schema:   &Schema,
        pks:      PkColumn,
    ) -> Result<(), ClientError> {
        let count = pks.len();
        if count == 0 { return Ok(()); }
        // Bulk-fill payload columns directly. BatchAppender is single-PK only
        // (its cursor mapping calls `pk_index_single`), so the delete path
        // bypasses it. The server's `retract_pk` matches by PK alone, so the
        // payload bytes are inert filler.
        let columns: Vec<ColData> = schema.columns.iter().enumerate().map(|(ci, col)| {
            if schema.is_pk_col(ci) {
                ColData::Fixed(vec![])
            } else {
                match col.type_code {
                    TypeCode::String => {
                        if col.is_nullable {
                            ColData::Strings(vec![None; count])
                        } else {
                            ColData::Strings(vec![Some(String::new()); count])
                        }
                    }
                    TypeCode::Blob => {
                        if col.is_nullable {
                            ColData::Bytes(vec![None; count])
                        } else {
                            ColData::Bytes(vec![Some(Vec::new()); count])
                        }
                    }
                    TypeCode::U128 | TypeCode::UUID => ColData::U128s(vec![0u128; count]),
                    _ => ColData::Fixed(vec![0u8; count * col.type_code.wire_stride()]),
                }
            }
        }).collect();

        let batch = ZSetBatch {
            pks,
            weights: vec![-1; count],
            nulls:   vec![0;  count],
            columns,
        };
        self.conn.push(table_id, schema, &batch, &mut self.schema_cache)?;
        Ok(())
    }

    // --- DDL ---

    pub fn create_schema(&mut self, name: &str) -> Result<u64, ClientError> {
        let new_sid = self.conn.alloc_schema_id()?;
        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(new_sid as u128, 1)
            .str_val(name);
        self.conn.push(SCHEMA_TAB, schema, &batch, &mut self.schema_cache)?;
        Ok(new_sid)
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
        let (_, data, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let data = data.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{name}' not found"))
        })?;
        let schema_id = find_schema_id(&data, name)?;

        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema)
            .add_row(schema_id as u128, -1)
            .str_val(name);
        self.conn.push(SCHEMA_TAB, schema, &batch, &mut self.schema_cache)?;
        Ok(())
    }

    /// `dist_prefix_len` is the hash-distribution prefix length `k`: rows are
    /// partitioned by the first `k` PK columns (`CLUSTER BY` the PK's leading
    /// prefix). `0` means the default — distribute by the full PK, byte-identical
    /// to the pre-distribution-key behavior. The SQL planner validates `k` against
    /// the PK before calling this; the single-PK Python/test surfaces pass `0`.
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
    ) -> Result<u64, ClientError> {
        if !(1..=gnitz_wire::PK_LIST_MAX_COLS).contains(&pk_cols.len()) {
            return Err(ClientError::ServerError(format!(
                "create_table: PK column count {} out of range 1..={}",
                pk_cols.len(), gnitz_wire::PK_LIST_MAX_COLS,
            )));
        }

        let new_tid = self.conn.alloc_table_id()?;

        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{schema_name}' not found"))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        // Encode the PK list using the shared wire packer so the engine
        // catalog decodes it identically. Single-PK callers still flow
        // through the same packer; the packed form's flag bit is what
        // distinguishes it from a bare scalar index.
        let pk_packed = gnitz_wire::pack_pk_cols(pk_cols);

        // COL_TAB first — server hook fires on TABLE_TAB insert and reads COL_TAB
        self.push_col_tab_records(new_tid, OWNER_KIND_TABLE, columns)?;

        // TABLE_TAB last
        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(tbl_schema);
        BatchAppender::new(&mut tb, tbl_schema)
            .add_row(new_tid as u128, 1)
            .u64_val(schema_id)
            .str_val(table_name)
            .str_val("")
            .u64_val(pk_packed)
            .u64_val(0)
            .u64_val(gnitz_wire::pack_table_flags(unique_pk, replicated, dist_prefix_len));
        self.conn.push(TABLE_TAB, tbl_schema, &tb, &mut self.schema_cache)?;

        Ok(new_tid)
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Result<(), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{schema_name}' not found"))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB, &mut self.schema_cache)?;
        let tbl_batch = tbl_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found"))
        })?;
        let record = find_table_record(&tbl_batch, schema_id, table_name)?;

        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(tbl_schema);
        BatchAppender::new(&mut tb, tbl_schema)
            .add_row(record.tid as u128, -1)
            .u64_val(record.schema_id)
            .str_val(&record.name)
            .str_val(&record.directory)
            .u64_val(record.pk_col_idx)
            .u64_val(record.created_lsn)
            .u64_val(record.flags);
        self.conn.push(TABLE_TAB, tbl_schema, &tb, &mut self.schema_cache)?;

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
        let vid = self.conn.alloc_table_id()?;

        let mut cb = crate::circuit::CircuitBuilder::new(vid, source_table_id);
        let scan = cb.input_delta();
        cb.sink(scan);
        let circuit = cb.build();

        // Minimal SCAN→SINK passthrough: single output PK at slot 0.
        self.write_circuit_rows(schema_name, view_name, "", vid, circuit, output_columns, &[0])
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
        let vid = if circuit.view_id == 0 {
            self.conn.alloc_table_id()?
        } else {
            circuit.view_id
        };
        self.write_circuit_rows(schema_name, view_name, sql_text, vid, circuit, output_columns, pk_cols)
    }

    /// Shared serialisation path for `create_view` / `create_view_with_circuit`.
    /// Writes columns, dependencies, the three circuit tables, and the view
    /// record (which must come last — it triggers the server-side hook).
    ///
    /// `pk_cols` is the view's physical PK column list — the leading `k` output
    /// slots. It is `[0]` for every synthetic-PK view (join / set-op / distinct /
    /// minimal passthrough) and `0..k` for a plain projection that passes a
    /// compound source PK through.
    #[allow(clippy::too_many_arguments)]
    fn write_circuit_rows(
        &mut self,
        schema_name: &str,
        view_name: &str,
        sql_text: &str,
        vid: u64,
        circuit: Circuit,
        output_columns: &[ColumnDef],
        pk_cols: &[u32],
    ) -> Result<u64, ClientError> {
        // The view's PK region is the leading `k` columns (pk_cols == 0..k); it
        // has no null bitmap, so any nullable PK column is internally
        // inconsistent. Validate every PK slot, not just the first.
        if output_columns.is_empty() {
            return Err(ClientError::ServerError(
                "View must have at least one output column".into()
            ));
        }
        // Single choke point every view kind passes through: surface an over-wide
        // output schema as a clean planner error before the DDL round-trip, rather
        // than tripping the engine's build_schema_from_col_defs assert. (The JOIN
        // path already rejects combined_cols > MAX_COLUMNS ahead of alloc_table_id;
        // this covers plain projection, GROUP BY, set-op, and DISTINCT uniformly.)
        if output_columns.len() > gnitz_wire::MAX_COLUMNS {
            return Err(ClientError::ServerError(format!(
                "View has {} output columns, exceeding the {}-column limit",
                output_columns.len(), gnitz_wire::MAX_COLUMNS
            )));
        }
        for &p in pk_cols {
            match output_columns.get(p as usize) {
                Some(c) if !c.is_nullable => {}
                Some(_) => return Err(ClientError::ServerError(
                    "View Primary Key column must not be nullable".into()
                )),
                None => return Err(ClientError::ServerError(format!(
                    "View PK column index {} out of range ({} output columns)",
                    p, output_columns.len()
                ))),
            }
        }

        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{schema_name}' not found"))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        // 1. Column records
        self.push_col_tab_records(vid, OWNER_KIND_VIEW, output_columns)?;

        // 2. Dependency records — every ScanDelta source_table.
        let deps = circuit.dependencies();
        if !deps.is_empty() {
            let dep_s = dep_tab_schema();
            let mut dep = ZSetBatch::new(dep_s);
            {
                let mut a = BatchAppender::new(&mut dep, dep_s);
                for &dep_tid in &deps {
                    // Compound PK (view_id, dep_table_id): low 8 bytes = view_id,
                    // high 8 bytes = dep_table_id. dep_view_id is the only payload.
                    //
                    // CONVERGENCE INVARIANT (all circuit-PK packings below too):
                    // the client packs view_id in the LOW u128 half while the
                    // engine's `pack_view_pk` packs it in the HIGH half. These
                    // produce the IDENTICAL view_id-major big-endian at-rest OPK
                    // image only because the client's multi-column `PkColumn::Bytes`
                    // arm OPK-encodes each 8-byte PK column independently (low u128
                    // bytes → first column) while the engine writes the whole u128
                    // big-endian — byte-order duals that agree only because every
                    // circuit-PK column is exactly 8 bytes and unsigned.
                    // `load_circuit` / `retract_rows_by_view` prefix-seek on
                    // `view_id.to_be_bytes()` and depend on this. Do NOT "align"
                    // the two packings: flipping the client to `(vid << 64) | sub`
                    // moves `sub` into the leading at-rest bytes and breaks every
                    // view-load prefix seek.
                    let pk = (vid as u128) | ((dep_tid as u128) << 64);
                    a.add_row(pk, 1)
                        .u64_val(0); // dep_view_id
                }
            }
            self.conn.push(DEP_TAB, dep_s, &dep, &mut self.schema_cache)?;
        }

        // Materialise the typed circuit into the three-table row bundle.
        let rows = circuit.into_rows();

        // 3. CircuitNodes
        if !rows.nodes.is_empty() {
            let nodes_s = circuit_nodes_schema();
            let mut nodes = ZSetBatch::new(nodes_s);
            {
                let mut a = BatchAppender::new(&mut nodes, nodes_s);
                for (node_id, opcode, src_tab, reindex, expr_blob) in &rows.nodes {
                    // Compound PK (view_id, sub=node_id): low 8 bytes = view_id,
                    // high 8 bytes = node_id.
                    let pk = (vid as u128) | ((*node_id as u128) << 64);
                    let mut null_mask: u64 = 0;
                    // Payload columns: node_id=0, opcode=1, source_table=2,
                    // reindex_col=3, expr_program=4.
                    if src_tab.is_none()  { null_mask |= 1u64 << 2; }
                    if reindex.is_none()  { null_mask |= 1u64 << 3; }
                    if expr_blob.is_none() { null_mask |= 1u64 << 4; }
                    a.add_row(pk, 1)
                        .null_mask(null_mask)
                        .u64_val(*node_id)
                        .u64_val(*opcode);
                    match src_tab { Some(t) => { a.u64_val(*t); }, None => { a.u64_val(0); } }
                    match reindex { Some(r) => { a.u64_val(*r as u64); }, None => { a.u64_val(0); } }
                    match expr_blob {
                        Some(b) => { a.bytes_val(b); }
                        None    => { a.bytes_null(); }
                    }
                }
            }
            self.conn.push(CIRCUIT_NODES_TAB, nodes_s, &nodes, &mut self.schema_cache)?;
        }

        // 4. CircuitEdges (natural-key PK — no synthetic edge_id).
        if !rows.edges.is_empty() {
            let edges_s = circuit_edges_schema();
            let mut edges = ZSetBatch::new(edges_s);
            {
                let mut a = BatchAppender::new(&mut edges, edges_s);
                for (dst_node, dst_port, src_node) in &rows.edges {
                    debug_assert!(*dst_node < (1u64 << 40), "dst_node {dst_node} exceeds 40-bit cap");
                    // Compound PK (view_id, sub): sub packs (dst_node, dst_port).
                    let sub = ((*dst_node as u128) << 8) | (*dst_port as u128);
                    let pk = (vid as u128) | (sub << 64);
                    a.add_row(pk, 1)
                        .u64_val(*dst_node)
                        .u64_val(*dst_port as u64)
                        .u64_val(*src_node);
                }
            }
            self.conn.push(CIRCUIT_EDGES_TAB, edges_s, &edges, &mut self.schema_cache)?;
        }

        // 5. CircuitNodeColumns (replaces group_cols + the four PARAM_BASE ranges).
        if !rows.node_columns.is_empty() {
            let s = circuit_node_columns_schema();
            let mut nc = ZSetBatch::new(s);
            {
                let mut a = BatchAppender::new(&mut nc, s);
                for (node_id, kind, position, v1, v2) in &rows.node_columns {
                    debug_assert!((*position as u64) <= 0xFFFF);
                    debug_assert!((*kind) <= 0xFF);
                    debug_assert!((*node_id) <= 0x00FF_FFFF_FFFF);
                    // Compound PK (view_id, sub): sub packs (node_id, kind, position).
                    let sub = ((*node_id as u128) << 24)
                        | ((*kind as u128) << 16)
                        | (*position as u128);
                    let pk = (vid as u128) | (sub << 64);
                    a.add_row(pk, 1)
                        .u64_val(*node_id)
                        .u64_val(*kind)
                        .u64_val(*position as u64)
                        .u64_val(*v1)
                        .u64_val(*v2);
                }
            }
            self.conn.push(CIRCUIT_NODE_COLUMNS_TAB, s, &nc, &mut self.schema_cache)?;
        }

        // 6. View record — must be last (triggers server-side hook + circuit
        // compilation). Encode the view PK with the shared wire packer so the
        // engine catalog decodes it identically to a TABLE_TAB PK.
        let pk_packed = gnitz_wire::pack_pk_cols(pk_cols);
        self.push_view_record(vid, schema_id, view_name, sql_text, pk_packed)?;

        Ok(vid)
    }

    pub fn drop_view(&mut self, schema_name: &str, view_name: &str) -> Result<(), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{schema_name}' not found"))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        let (_, view_batch, _) = self.conn.scan(VIEW_TAB, &mut self.schema_cache)?;
        let view_batch = view_batch.ok_or_else(|| {
            ClientError::ServerError(format!("View '{schema_name}.{view_name}' not found"))
        })?;
        let vr = find_view_record(&view_batch, schema_id, view_name)?;

        let view_s = view_tab_schema();
        let mut vb = ZSetBatch::new(view_s);
        BatchAppender::new(&mut vb, view_s)
            .add_row(vr.vid as u128, -1)
            .u64_val(vr.schema_id)
            .str_val(&vr.name)
            .str_val(&vr.sql_definition)
            .str_val(&vr.cache_directory)
            .u64_val(vr.created_lsn)
            .u64_val(vr.pk_col_idx);
        self.conn.push(VIEW_TAB, view_s, &vb, &mut self.schema_cache)?;

        Ok(())
    }

    pub fn resolve_table_id(
        &mut self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(u64, Schema), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{schema_name}' not found"))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB, &mut self.schema_cache)?;
        let tbl_batch = tbl_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Table '{schema_name}.{table_name}' not found"))
        })?;
        let record = find_table_record(&tbl_batch, schema_id, table_name)?;

        let (_, col_batch, _) = self.conn.scan(COL_TAB, &mut self.schema_cache)?;
        let col_batch = col_batch.ok_or_else(|| {
            ClientError::ServerError("COL_TAB is empty".to_string())
        })?;

        let columns = extract_col_entries(&col_batch, record.tid, OWNER_KIND_TABLE)?;
        Ok((record.tid, Schema { columns, pk_cols: decode_pk_cols(record.pk_col_idx)? }))
    }

    pub fn resolve_table_or_view_id(
        &mut self,
        schema_name: &str,
        name: &str,
    ) -> Result<(u64, Schema), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{schema_name}' not found"))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        // Scan COL_TAB once — shared by both the table and view branches below
        let (_, col_batch, _) = self.conn.scan(COL_TAB, &mut self.schema_cache)?;
        let col_batch = col_batch.ok_or_else(|| {
            ClientError::ServerError("COL_TAB is empty".to_string())
        })?;

        // Try TABLE_TAB first (most common path)
        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB, &mut self.schema_cache)?;
        if let Some(ref tbl_batch) = tbl_batch {
            if let Ok(record) = find_table_record(tbl_batch, schema_id, name) {
                let columns = extract_col_entries(&col_batch, record.tid, OWNER_KIND_TABLE)?;
                return Ok((record.tid, Schema { columns, pk_cols: decode_pk_cols(record.pk_col_idx)? }));
            }
        }

        // Fall back to VIEW_TAB
        let (_, view_batch, _) = self.conn.scan(VIEW_TAB, &mut self.schema_cache)?;
        let view_batch = view_batch.ok_or_else(|| {
            ClientError::ServerError(
                format!("Table or view '{schema_name}.{name}' not found")
            )
        })?;
        let record = find_view_record(&view_batch, schema_id, name)?;
        let columns = extract_col_entries(&col_batch, record.vid, OWNER_KIND_VIEW)?;
        // The view PK is the persisted leading-k column list: a single synthetic
        // hash column for join/set-op/distinct views, or the source PK passed
        // through (0..k) for a plain projection over a compound-PK table.
        Ok((record.vid, Schema { columns, pk_cols: decode_pk_cols(record.pk_col_idx)? }))
    }

    /// True iff base table `tid` is REPLICATED (decoded from `TABLE_TAB.flags`).
    /// Scans `TABLE_TAB` and matches by PK; a `tid` that is not a base table
    /// (e.g. a view id, absent from `TABLE_TAB`) yields `false`. The SQL planner
    /// consults this when building an aggregate directly over a source: a reduce
    /// over a replicated relation must be the shard-free `reduce_multi_local`
    /// (every worker holds the full copy, so a sharded reduce would
    /// N-fold-multiply the aggregate — see the replicated-tables design).
    pub fn table_replicated(&mut self, tid: u64) -> Result<bool, ClientError> {
        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB, &mut self.schema_cache)?;
        let Some(tbl_batch) = tbl_batch else { return Ok(false); };
        for i in 0..tbl_batch.len() {
            if tbl_batch.weights[i] <= 0 { continue; }
            if tbl_batch.pks.get(i) as u64 != tid { continue; }
            return Ok(gnitz_wire::table_flags_replicated(col_u64(&tbl_batch.columns[6], i)?));
        }
        Ok(false)
    }

    // --- Private helpers (delegating to module-level functions) ---
}

fn extract_col_entries(
    col_batch:  &ZSetBatch,
    owner_id:   u64,
    owner_kind: u64,
) -> Result<Vec<ColumnDef>, ClientError> {
    let mut col_entries: Vec<(u64, String, TypeCode, bool, u64, u64)> = Vec::new();
    for i in 0..col_batch.len() {
        if col_batch.weights[i] <= 0 { continue; }
        let row_owner_id   = col_u64(&col_batch.columns[1], i)?;
        let row_owner_kind = col_u64(&col_batch.columns[2], i)?;
        if row_owner_id != owner_id || row_owner_kind != owner_kind { continue; }

        let col_idx     = col_u64(&col_batch.columns[3], i)?;
        let name        = col_str(&col_batch.columns[4], i)?.unwrap_or("").to_string();
        let tc_val      = col_u64(&col_batch.columns[5], i)?;
        let type_code   = type_code_from_u64(tc_val).map_err(ClientError::Protocol)?;
        let is_nullable = col_u64(&col_batch.columns[6], i)? != 0;
        let fk_table_id = col_u64(&col_batch.columns[7], i)?;
        let fk_col_idx  = col_u64(&col_batch.columns[8], i)?;
        col_entries.push((col_idx, name, type_code, is_nullable, fk_table_id, fk_col_idx));
    }
    col_entries.sort_by_key(|e| e.0);
    Ok(col_entries.into_iter().map(|(_, name, type_code, is_nullable, fk_table_id, fk_col_idx)| {
        ColumnDef { name, type_code, is_nullable, fk_table_id, fk_col_idx }
    }).collect())
}

fn find_schema_id(batch: &ZSetBatch, name: &str) -> Result<u64, ClientError> {
    for i in 0..batch.len() {
        if batch.weights[i] <= 0 { continue; }
        if col_str(&batch.columns[1], i)? == Some(name) {
            return Ok(batch.pks.get(i) as u64);
        }
    }
    Err(ClientError::ServerError(format!("Schema '{name}' not found")))
}

fn find_table_record(
    batch: &ZSetBatch,
    schema_id: u64,
    table_name: &str,
) -> Result<TableRecord, ClientError> {
    for i in 0..batch.len() {
        if batch.weights[i] <= 0 { continue; }
        if col_u64(&batch.columns[1], i)? != schema_id { continue; }
        if col_str(&batch.columns[2], i)? != Some(table_name) { continue; }
        return Ok(TableRecord {
            tid:         batch.pks.get(i) as u64,
            schema_id,
            name:        table_name.to_string(),
            directory:   col_str(&batch.columns[3], i)?.unwrap_or("").to_string(),
            pk_col_idx:  col_u64(&batch.columns[4], i)?,
            created_lsn: col_u64(&batch.columns[5], i)?,
            flags:       col_u64(&batch.columns[6], i)?,
        });
    }
    Err(ClientError::ServerError(format!("Table '{table_name}' not found")))
}

fn find_view_record(
    batch: &ZSetBatch,
    schema_id: u64,
    view_name: &str,
) -> Result<ViewRecord, ClientError> {
    for i in 0..batch.len() {
        if batch.weights[i] <= 0 { continue; }
        if col_u64(&batch.columns[1], i)? != schema_id { continue; }
        if col_str(&batch.columns[2], i)? != Some(view_name) { continue; }
        return Ok(ViewRecord {
            vid:             batch.pks.get(i) as u64,
            schema_id,
            name:            view_name.to_string(),
            sql_definition:  col_str(&batch.columns[3], i)?.unwrap_or("").to_string(),
            cache_directory: col_str(&batch.columns[4], i)?.unwrap_or("").to_string(),
            created_lsn:     col_u64(&batch.columns[5], i)?,
            pk_col_idx:      col_u64(&batch.columns[6], i)?,
        });
    }
    Err(ClientError::ServerError(format!("View '{view_name}' not found")))
}

impl GnitzClient {
    fn push_col_tab_records(
        &mut self,
        owner_id: u64,
        owner_kind: u64,
        columns: &[ColumnDef],
    ) -> Result<(), ClientError> {
        let schema = col_tab_schema();
        let mut batch = ZSetBatch::new(schema);
        {
            let mut a = BatchAppender::new(&mut batch, schema);
            for (i, col) in columns.iter().enumerate() {
                a.add_row(pack_col_id(owner_id, i)? as u128, 1)
                    .u64_val(owner_id)
                    .u64_val(owner_kind)
                    .u64_val(i as u64)
                    .str_val(&col.name)
                    .u64_val(col.type_code as u64)
                    .u64_val(if col.is_nullable { 1 } else { 0 })
                    .u64_val(col.fk_table_id)
                    .u64_val(col.fk_col_idx);
            }
        }
        self.conn.push(COL_TAB, schema, &batch, &mut self.schema_cache)?;
        Ok(())
    }

    fn push_view_record(
        &mut self, vid: u64, schema_id: u64, view_name: &str, sql_text: &str, pk_packed: u64,
    ) -> Result<(), ClientError> {
        let view_s = view_tab_schema();
        let mut vb = ZSetBatch::new(view_s);
        BatchAppender::new(&mut vb, view_s)
            .add_row(vid as u128, 1)
            .u64_val(schema_id)
            .str_val(view_name)
            .str_val(sql_text)
            .str_val("")
            .u64_val(0)
            .u64_val(pk_packed);
        self.conn.push(VIEW_TAB, view_s, &vb, &mut self.schema_cache)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        for tc in [TypeCode::U64, TypeCode::I64, TypeCode::U32, TypeCode::I8,
                   TypeCode::U128, TypeCode::UUID] {
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
        let err = decode_pk_cols(bad)
            .expect_err(&format!("expected error on count={bad_count}"));
        match err {
            ClientError::ServerError(s) => assert!(
                s.contains(&bad_count.to_string()),
                "expected '{bad_count}' in message, got: {s}"),
            other => panic!("expected ServerError, got {other:?}"),
        }
    }
}
