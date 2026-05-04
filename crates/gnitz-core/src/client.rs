use crate::protocol::{Schema, ColumnDef, TypeCode, ZSetBatch, ColData, BatchAppender, WireConflictMode};
use crate::protocol::types::type_code_from_u64;
use crate::connection::{Connection, SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, IDX_TAB};
use crate::error::ClientError;
use crate::types::{
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_NODE_COLUMNS_TAB,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    schema_tab_schema, table_tab_schema, col_tab_schema, view_tab_schema,
    dep_tab_schema, circuit_nodes_schema, circuit_edges_schema,
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

fn validate_index_col_type(tc: TypeCode) -> Result<(), ClientError> {
    match tc {
        TypeCode::F32 | TypeCode::F64 | TypeCode::String => Err(ClientError::ServerError(
            "index on float/string column not supported".to_string()
        )),
        _ => Ok(()),
    }
}

fn idx_tab_schema() -> &'static Schema {
    use std::sync::OnceLock;
    use crate::protocol::ColumnDef;
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef { name: "index_id".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "owner_id".into(),        type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "owner_kind".into(),      type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "source_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(),            type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "is_unique".into(),       type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "cache_directory".into(), type_code: TypeCode::String, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    })
}

fn pack_col_id(owner_id: u64, col_idx: usize) -> Result<u64, ClientError> {
    if col_idx >= 512 {
        return Err(ClientError::ServerError(
            format!("column index {} exceeds maximum 511", col_idx)));
    }
    if owner_id > (u64::MAX >> 9) {
        return Err(ClientError::ServerError(
            format!("owner_id {} exceeds 55-bit maximum for column ID packing", owner_id)));
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
}

// --- GnitzClient ---

pub struct GnitzClient {
    conn: Connection,
}

impl GnitzClient {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError> {
        Ok(GnitzClient { conn: Connection::connect(socket_path)? })
    }

    pub fn close(self) {
        self.conn.close();
    }

    // --- Raw ops ---

    pub fn alloc_table_id(&self) -> Result<u64, ClientError> {
        self.conn.alloc_table_id()
    }

    pub fn alloc_schema_id(&self) -> Result<u64, ClientError> {
        self.conn.alloc_schema_id()
    }

    pub fn alloc_index_id(&self) -> Result<u64, ClientError> {
        self.conn.alloc_index_id()
    }

    pub fn push(&self, table_id: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, ClientError> {
        self.conn.push(table_id, schema, batch)
    }

    /// Push with an explicit `WireConflictMode`. SQL `INSERT` uses
    /// `Error` to get SQL-standard rejection semantics; all other
    /// callers pass `Update` (or use the plain `push` which defaults
    /// to `Update` for backward compatibility).
    pub fn push_with_mode(
        &self, table_id: u64, schema: &Schema, batch: &ZSetBatch,
        mode: WireConflictMode,
    ) -> Result<u64, ClientError> {
        self.conn.push_with_mode(table_id, schema, batch, mode)
    }

    pub fn scan(&self, table_id: u64) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        self.conn.scan(table_id)
    }

    pub fn seek(
        &self,
        table_id: u64,
        pk:       u128,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        self.conn.seek(table_id, pk)
    }

    pub fn seek_by_index(
        &self, table_id: u64, col_idx: u64, key: u128,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        self.conn.seek_by_index(table_id, col_idx, key)
    }

    pub fn find_index_for_column(
        &self, table_id: u64, col_idx: usize,
    ) -> Result<Option<(u64, bool)>, ClientError> {
        let (_, idx_batch, _) = self.conn.scan(IDX_TAB)?;
        let idx_batch = match idx_batch { None => return Ok(None), Some(b) => b };
        for i in 0..idx_batch.len() {
            if idx_batch.weights[i] <= 0 { continue; }
            let owner_id       = col_u64(&idx_batch.columns[1], i)?;
            let source_col_idx = col_u64(&idx_batch.columns[3], i)?;
            if owner_id == table_id && source_col_idx == col_idx as u64 {
                let is_unique = col_u64(&idx_batch.columns[5], i)? != 0;
                return Ok(Some((idx_batch.pks.get(i) as u64, is_unique)));
            }
        }
        Ok(None)
    }

    pub fn create_index(
        &self, schema_name: &str, table_name: &str, col_name: &str, is_unique: bool,
    ) -> Result<u64, ClientError> {
        let (table_id, schema) = self.resolve_table_or_view_id(schema_name, table_name)?;
        let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| ClientError::ServerError(
                format!("column '{}' not found in table '{}'", col_name, table_name)
            ))?;
        validate_index_col_type(schema.columns[col_idx].type_code)?;

        let index_name = format!("{}__{}__idx_{}", schema_name, table_name, col_name);
        let index_id = self.alloc_index_id()?;

        let idx_schema = idx_tab_schema();
        let mut batch = ZSetBatch::new(&idx_schema);
        BatchAppender::new(&mut batch, &idx_schema)
            .add_row(index_id as u128, 1)
            .u64_val(table_id)
            .u64_val(0)
            .u64_val(col_idx as u64)
            .str_val(&index_name)
            .u64_val(is_unique as u64)
            .str_val("");

        self.push(IDX_TAB, &idx_schema, &batch)?;
        Ok(index_id)
    }

    pub fn drop_index_by_name(&self, index_name: &str) -> Result<(), ClientError> {
        let (_, idx_batch, _) = self.conn.scan(IDX_TAB)?;
        let idx_batch = idx_batch.ok_or_else(|| {
            ClientError::ServerError(format!("index '{}' not found", index_name))
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
            let mut batch = ZSetBatch::new(&idx_schema);
            BatchAppender::new(&mut batch, &idx_schema)
                .add_row(index_id as u128, -1)
                .u64_val(owner_id)
                .u64_val(owner_kind)
                .u64_val(src_col)
                .str_val(index_name)
                .u64_val(is_unique)
                .str_val(&cache_dir);
            self.push(IDX_TAB, &idx_schema, &batch)?;
            return Ok(());
        }
        Err(ClientError::ServerError(format!("index '{}' not found", index_name)))
    }

    pub fn delete(&self, table_id: u64, schema: &Schema, pks: &[u128]) -> Result<(), ClientError> {
        let mut batch = ZSetBatch::new(schema);
        {
            let mut a = BatchAppender::new(&mut batch, schema);
            let non_pk_count = schema.columns.len() - 1;
            for &pk in pks {
                a.add_row(pk, -1);
                for _ in 0..non_pk_count {
                    a.zero_val();
                }
            }
        }
        self.conn.push(table_id, schema, &batch)?;
        Ok(())
    }

    // --- DDL ---

    pub fn create_schema(&self, name: &str) -> Result<u64, ClientError> {
        let new_sid = self.conn.alloc_schema_id()?;
        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(new_sid as u128, 1)
            .str_val(name);
        self.conn.push(SCHEMA_TAB, &schema, &batch)?;
        Ok(new_sid)
    }

    pub fn drop_schema(&self, name: &str) -> Result<(), ClientError> {
        let (_, data, _) = self.conn.scan(SCHEMA_TAB)?;
        let data = data.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", name))
        })?;
        let schema_id = find_schema_id(&data, name)?;

        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(schema_id as u128, -1)
            .str_val(name);
        self.conn.push(SCHEMA_TAB, &schema, &batch)?;
        Ok(())
    }

    pub fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        pk_col_idx: usize,
        unique_pk: bool,
    ) -> Result<u64, ClientError> {
        let new_tid = self.conn.alloc_table_id()?;

        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        // COL_TAB first — server hook fires on TABLE_TAB insert and reads COL_TAB
        self.push_col_tab_records(new_tid, OWNER_KIND_TABLE, columns)?;

        // TABLE_TAB last
        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(&tbl_schema);
        BatchAppender::new(&mut tb, &tbl_schema)
            .add_row(new_tid as u128, 1)
            .u64_val(schema_id)
            .str_val(table_name)
            .str_val("")
            .u64_val(pk_col_idx as u64)
            .u64_val(0)
            .u64_val(unique_pk as u64);
        self.conn.push(TABLE_TAB, &tbl_schema, &tb)?;

        Ok(new_tid)
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> Result<(), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB)?;
        let tbl_batch = tbl_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Table '{}.{}' not found", schema_name, table_name))
        })?;
        let record = find_table_record(&tbl_batch, schema_id, table_name)?;

        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(&tbl_schema);
        BatchAppender::new(&mut tb, &tbl_schema)
            .add_row(record.tid as u128, -1)
            .u64_val(record.schema_id)
            .str_val(&record.name)
            .str_val(&record.directory)
            .u64_val(record.pk_col_idx)
            .u64_val(record.created_lsn)
            .u64_val(record.flags);
        self.conn.push(TABLE_TAB, &tbl_schema, &tb)?;

        Ok(())
    }

    pub fn create_view(
        &self,
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

        self.write_circuit_rows(schema_name, view_name, "", vid, circuit, output_columns)
    }

    pub fn create_view_with_circuit(
        &self,
        schema_name: &str,
        view_name: &str,
        sql_text: &str,
        circuit: Circuit,
        output_columns: &[ColumnDef],
    ) -> Result<u64, ClientError> {
        let vid = if circuit.view_id == 0 {
            self.conn.alloc_table_id()?
        } else {
            circuit.view_id
        };
        self.write_circuit_rows(schema_name, view_name, sql_text, vid, circuit, output_columns)
    }

    /// Shared serialisation path for `create_view` / `create_view_with_circuit`.
    /// Writes columns, dependencies, the three circuit tables, and the view
    /// record (which must come last — it triggers the server-side hook).
    fn write_circuit_rows(
        &self,
        schema_name: &str,
        view_name: &str,
        sql_text: &str,
        vid: u64,
        circuit: Circuit,
        output_columns: &[ColumnDef],
    ) -> Result<u64, ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        // 1. Column records
        self.push_col_tab_records(vid, OWNER_KIND_VIEW, output_columns)?;

        // 2. Dependency records — every ScanDelta source_table.
        let deps = circuit.dependencies();
        if !deps.is_empty() {
            let dep_s = dep_tab_schema();
            let mut dep = ZSetBatch::new(&dep_s);
            {
                let mut a = BatchAppender::new(&mut dep, &dep_s);
                for &dep_tid in &deps {
                    a.add_row(dep_tid as u128 | (vid as u128) << 64, 1)
                        .u64_val(vid)
                        .u64_val(0)
                        .u64_val(dep_tid);
                }
            }
            self.conn.push(DEP_TAB, &dep_s, &dep)?;
        }

        // Materialise the typed circuit into the three-table row bundle.
        let rows = circuit.into_rows();

        // 3. CircuitNodes
        if !rows.nodes.is_empty() {
            let nodes_s = circuit_nodes_schema();
            let mut nodes = ZSetBatch::new(&nodes_s);
            {
                let mut a = BatchAppender::new(&mut nodes, &nodes_s);
                for (node_id, opcode, src_tab, reindex, expr_blob) in &rows.nodes {
                    let pk = *node_id as u128 | (vid as u128) << 64;
                    let mut null_mask: u64 = 0;
                    // Payload column null-bit positions: source_table=3, reindex_col=4, expr_program=5.
                    if src_tab.is_none()  { null_mask |= 1u64 << 3; }
                    if reindex.is_none()  { null_mask |= 1u64 << 4; }
                    if expr_blob.is_none() { null_mask |= 1u64 << 5; }
                    a.add_row(pk, 1)
                        .null_mask(null_mask)
                        .u64_val(vid)
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
            self.conn.push(CIRCUIT_NODES_TAB, &nodes_s, &nodes)?;
        }

        // 4. CircuitEdges (natural-key PK — no synthetic edge_id).
        if !rows.edges.is_empty() {
            let edges_s = circuit_edges_schema();
            let mut edges = ZSetBatch::new(&edges_s);
            {
                let mut a = BatchAppender::new(&mut edges, &edges_s);
                for (dst_node, dst_port, src_node) in &rows.edges {
                    debug_assert!(*dst_node < (1u64 << 40), "dst_node {} exceeds 40-bit cap", dst_node);
                    let pk_lo = ((*dst_node as u128) << 8) | (*dst_port as u128);
                    let pk = pk_lo | (vid as u128) << 64;
                    a.add_row(pk, 1)
                        .u64_val(vid)
                        .u64_val(*dst_node)
                        .u64_val(*dst_port as u64)
                        .u64_val(*src_node);
                }
            }
            self.conn.push(CIRCUIT_EDGES_TAB, &edges_s, &edges)?;
        }

        // 5. CircuitNodeColumns (replaces group_cols + the four PARAM_BASE ranges).
        if !rows.node_columns.is_empty() {
            let s = circuit_node_columns_schema();
            let mut nc = ZSetBatch::new(&s);
            {
                let mut a = BatchAppender::new(&mut nc, &s);
                for (node_id, kind, position, v1, v2) in &rows.node_columns {
                    debug_assert!((*position as u64) <= 0xFFFF);
                    debug_assert!((*kind) <= 0xFF);
                    debug_assert!((*node_id) <= 0xFFFF_FFFF_FF);
                    let pk_lo = ((*node_id as u128) << 24)
                        | ((*kind as u128) << 16)
                        | (*position as u128);
                    let pk = pk_lo | (vid as u128) << 64;
                    a.add_row(pk, 1)
                        .u64_val(vid)
                        .u64_val(*node_id)
                        .u64_val(*kind)
                        .u64_val(*position as u64)
                        .u64_val(*v1)
                        .u64_val(*v2);
                }
            }
            self.conn.push(CIRCUIT_NODE_COLUMNS_TAB, &s, &nc)?;
        }

        // 6. View record — must be last (triggers server-side hook + circuit compilation).
        self.push_view_record(vid, schema_id, view_name, sql_text)?;

        Ok(vid)
    }

    pub fn drop_view(&self, schema_name: &str, view_name: &str) -> Result<(), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        let (_, view_batch, _) = self.conn.scan(VIEW_TAB)?;
        let view_batch = view_batch.ok_or_else(|| {
            ClientError::ServerError(format!("View '{}.{}' not found", schema_name, view_name))
        })?;
        let vr = find_view_record(&view_batch, schema_id, view_name)?;

        let view_s = view_tab_schema();
        let mut vb = ZSetBatch::new(&view_s);
        BatchAppender::new(&mut vb, &view_s)
            .add_row(vr.vid as u128, -1)
            .u64_val(vr.schema_id)
            .str_val(&vr.name)
            .str_val(&vr.sql_definition)
            .str_val(&vr.cache_directory)
            .u64_val(vr.created_lsn);
        self.conn.push(VIEW_TAB, &view_s, &vb)?;

        Ok(())
    }

    pub fn resolve_table_id(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(u64, Schema), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB)?;
        let tbl_batch = tbl_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Table '{}.{}' not found", schema_name, table_name))
        })?;
        let record = find_table_record(&tbl_batch, schema_id, table_name)?;

        let (_, col_batch, _) = self.conn.scan(COL_TAB)?;
        let col_batch = col_batch.ok_or_else(|| {
            ClientError::ServerError("COL_TAB is empty".to_string())
        })?;

        let columns = extract_col_entries(&col_batch, record.tid, OWNER_KIND_TABLE)?;
        Ok((record.tid, Schema { columns, pk_index: record.pk_col_idx as usize }))
    }

    pub fn resolve_table_or_view_id(
        &self,
        schema_name: &str,
        name: &str,
    ) -> Result<(u64, Schema), ClientError> {
        let (_, schema_batch, _) = self.conn.scan(SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = find_schema_id(&schema_batch, schema_name)?;

        // Scan COL_TAB once — shared by both the table and view branches below
        let (_, col_batch, _) = self.conn.scan(COL_TAB)?;
        let col_batch = col_batch.ok_or_else(|| {
            ClientError::ServerError("COL_TAB is empty".to_string())
        })?;

        // Try TABLE_TAB first (most common path)
        let (_, tbl_batch, _) = self.conn.scan(TABLE_TAB)?;
        if let Some(ref tbl_batch) = tbl_batch {
            if let Ok(record) = find_table_record(tbl_batch, schema_id, name) {
                let columns = extract_col_entries(&col_batch, record.tid, OWNER_KIND_TABLE)?;
                return Ok((record.tid, Schema { columns, pk_index: record.pk_col_idx as usize }));
            }
        }

        // Fall back to VIEW_TAB
        let (_, view_batch, _) = self.conn.scan(VIEW_TAB)?;
        let view_batch = view_batch.ok_or_else(|| {
            ClientError::ServerError(
                format!("Table or view '{}.{}' not found", schema_name, name)
            )
        })?;
        let record = find_view_record(&view_batch, schema_id, name)?;
        let columns = extract_col_entries(&col_batch, record.vid, OWNER_KIND_VIEW)?;
        // View PK is always the U128 hash column at index 0
        Ok((record.vid, Schema { columns, pk_index: 0 }))
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
    Err(ClientError::ServerError(format!("Schema '{}' not found", name)))
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
    Err(ClientError::ServerError(format!("Table '{}' not found", table_name)))
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
        });
    }
    Err(ClientError::ServerError(format!("View '{}' not found", view_name)))
}

impl GnitzClient {
    fn push_col_tab_records(
        &self,
        owner_id: u64,
        owner_kind: u64,
        columns: &[ColumnDef],
    ) -> Result<(), ClientError> {
        let schema = col_tab_schema();
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            for i in 0..columns.len() {
                a.add_row(pack_col_id(owner_id, i)? as u128, 1)
                    .u64_val(owner_id)
                    .u64_val(owner_kind)
                    .u64_val(i as u64)
                    .str_val(&columns[i].name)
                    .u64_val(columns[i].type_code as u64)
                    .u64_val(if columns[i].is_nullable { 1 } else { 0 })
                    .u64_val(columns[i].fk_table_id)
                    .u64_val(columns[i].fk_col_idx);
            }
        }
        self.conn.push(COL_TAB, &schema, &batch)?;
        Ok(())
    }

    fn push_view_record(&self, vid: u64, schema_id: u64, view_name: &str, sql_text: &str) -> Result<(), ClientError> {
        let view_s = view_tab_schema();
        let mut vb = ZSetBatch::new(&view_s);
        BatchAppender::new(&mut vb, &view_s)
            .add_row(vid as u128, 1)
            .u64_val(schema_id)
            .str_val(view_name)
            .str_val(sql_text)
            .str_val("")
            .u64_val(0);
        self.conn.push(VIEW_TAB, &view_s, &vb)?;
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
}
