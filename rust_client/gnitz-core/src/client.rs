use gnitz_protocol::{Schema, ColumnDef, TypeCode, ZSetBatch, ColData};
use crate::connection::Connection;
use crate::error::ClientError;
use crate::ops::{
    self, SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, IDX_TAB,
};
use crate::types::{
    CircuitGraph,
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_SOURCES_TAB,
    CIRCUIT_PARAMS_TAB, CIRCUIT_GROUP_COLS_TAB,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    OPCODE_SCAN_TRACE, OPCODE_INTEGRATE,
    PORT_IN,
    schema_tab_schema, table_tab_schema, col_tab_schema, view_tab_schema,
    dep_tab_schema, circuit_nodes_schema, circuit_edges_schema,
    circuit_sources_schema, circuit_params_schema, circuit_group_cols_schema,
};

// --- Module-private helpers ---

fn col_u64(col: &ColData, i: usize) -> u64 {
    match col {
        ColData::Fixed(bytes) => {
            let offset = i * 8;
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
        }
        _ => panic!("col_u64: expected Fixed column"),
    }
}

fn col_str(col: &ColData, i: usize) -> Option<&str> {
    match col {
        ColData::Strings(v) => v[i].as_deref(),
        _ => panic!("col_str: expected Strings column"),
    }
}

fn push_u64_bytes(buf: &mut Vec<u8>, val: u64) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn push_str(col: &mut ColData, s: &str) {
    if let ColData::Strings(v) = col { v.push(Some(s.to_string())); }
}

fn validate_index_col_type(tc: TypeCode) -> Result<(), ClientError> {
    match tc {
        TypeCode::F32 | TypeCode::F64 | TypeCode::String => Err(ClientError::ServerError(
            "index on float/string column not supported".to_string()
        )),
        _ => Ok(()),
    }
}

fn idx_tab_schema() -> Schema {
    use gnitz_protocol::ColumnDef;
    Schema {
        columns: vec![
            ColumnDef { name: "index_id".into(),       type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "owner_id".into(),        type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "owner_kind".into(),      type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "source_col_idx".into(),  type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(),            type_code: TypeCode::String, is_nullable: false },
            ColumnDef { name: "is_unique".into(),       type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "cache_directory".into(), type_code: TypeCode::String, is_nullable: false },
        ],
        pk_index: 0,
    }
}

fn copy_col_row(src: &ColData, row: usize, dst: &mut ColData, stride: usize) {
    match (src, dst) {
        (ColData::Fixed(s), ColData::Fixed(d)) => {
            let offset = row * stride;
            d.extend_from_slice(&s[offset..offset + stride]);
        }
        (ColData::Strings(s), ColData::Strings(d)) => {
            d.push(s[row].clone());
        }
        (ColData::U128s(s), ColData::U128s(d)) => {
            d.push(s[row]);
        }
        _ => panic!("copy_col_row: mismatched ColData variants"),
    }
}

fn pack_col_id(owner_id: u64, col_idx: usize) -> u64 {
    (owner_id << 9) | col_idx as u64
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
        ops::alloc_table_id(&self.conn)
    }

    pub fn alloc_schema_id(&self) -> Result<u64, ClientError> {
        ops::alloc_schema_id(&self.conn)
    }

    pub fn alloc_index_id(&self) -> Result<u64, ClientError> {
        ops::alloc_index_id(&self.conn)
    }

    pub fn push(&self, table_id: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, ClientError> {
        ops::push(&self.conn, table_id, schema, batch)
    }

    pub fn scan(&self, table_id: u64) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        ops::scan(&self.conn, table_id)
    }

    pub fn seek(
        &self,
        table_id: u64,
        pk_lo:    u64,
        pk_hi:    u64,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        ops::seek(&self.conn, table_id, pk_lo, pk_hi)
    }

    pub fn seek_by_index(
        &self, table_id: u64, col_idx: u64, key_lo: u64, key_hi: u64,
    ) -> Result<(Option<Schema>, Option<ZSetBatch>, u64), ClientError> {
        ops::seek_by_index(&self.conn, table_id, col_idx, key_lo, key_hi)
    }

    pub fn find_index_for_column(
        &self, table_id: u64, col_idx: usize,
    ) -> Result<Option<(u64, bool)>, ClientError> {
        let (_, idx_batch, _) = ops::scan(&self.conn, IDX_TAB)?;
        let idx_batch = match idx_batch { None => return Ok(None), Some(b) => b };
        for i in 0..idx_batch.len() {
            if idx_batch.weights[i] <= 0 { continue; }
            let owner_id       = col_u64(&idx_batch.columns[1], i);
            let source_col_idx = col_u64(&idx_batch.columns[3], i);
            if owner_id == table_id && source_col_idx == col_idx as u64 {
                let is_unique = col_u64(&idx_batch.columns[5], i) != 0;
                return Ok(Some((idx_batch.pk_lo[i], is_unique)));
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
        batch.pk_lo.push(index_id);
        batch.pk_hi.push(0);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] { push_u64_bytes(buf, table_id); }
        if let ColData::Fixed(buf) = &mut batch.columns[2] { push_u64_bytes(buf, 0u64); }
        if let ColData::Fixed(buf) = &mut batch.columns[3] { push_u64_bytes(buf, col_idx as u64); }
        push_str(&mut batch.columns[4], &index_name);
        if let ColData::Fixed(buf) = &mut batch.columns[5] { push_u64_bytes(buf, is_unique as u64); }
        push_str(&mut batch.columns[6], "");

        self.push(IDX_TAB, &idx_schema, &batch)?;
        Ok(index_id)
    }

    pub fn drop_index_by_name(&self, index_name: &str) -> Result<(), ClientError> {
        let (_, idx_batch, _) = ops::scan(&self.conn, IDX_TAB)?;
        let idx_batch = idx_batch.ok_or_else(|| {
            ClientError::ServerError(format!("index '{}' not found", index_name))
        })?;
        for i in 0..idx_batch.len() {
            if idx_batch.weights[i] <= 0 { continue; }
            let name = col_str(&idx_batch.columns[4], i).unwrap_or("");
            if name != index_name { continue; }

            let index_id   = idx_batch.pk_lo[i];
            let owner_id   = col_u64(&idx_batch.columns[1], i);
            let owner_kind = col_u64(&idx_batch.columns[2], i);
            let src_col    = col_u64(&idx_batch.columns[3], i);
            let is_unique  = col_u64(&idx_batch.columns[5], i);
            let cache_dir  = col_str(&idx_batch.columns[6], i).unwrap_or("").to_string();

            let idx_schema = idx_tab_schema();
            let mut batch = ZSetBatch::new(&idx_schema);
            batch.pk_lo.push(index_id);
            batch.pk_hi.push(0);
            batch.weights.push(-1);
            batch.nulls.push(0);
            if let ColData::Fixed(buf) = &mut batch.columns[1] { push_u64_bytes(buf, owner_id); }
            if let ColData::Fixed(buf) = &mut batch.columns[2] { push_u64_bytes(buf, owner_kind); }
            if let ColData::Fixed(buf) = &mut batch.columns[3] { push_u64_bytes(buf, src_col); }
            push_str(&mut batch.columns[4], index_name);
            if let ColData::Fixed(buf) = &mut batch.columns[5] { push_u64_bytes(buf, is_unique); }
            push_str(&mut batch.columns[6], &cache_dir);
            self.push(IDX_TAB, &idx_schema, &batch)?;
            return Ok(());
        }
        Err(ClientError::ServerError(format!("index '{}' not found", index_name)))
    }

    pub fn delete(&self, table_id: u64, schema: &Schema, pks: &[(u64, u64)]) -> Result<(), ClientError> {
        let n = pks.len();
        let mut batch = ZSetBatch::new(schema);
        for &(pk_lo, pk_hi) in pks {
            batch.pk_lo.push(pk_lo);
            batch.pk_hi.push(pk_hi);
            batch.weights.push(-1);
            batch.nulls.push(0);
        }
        for (ci, col_def) in schema.columns.iter().enumerate() {
            if ci == schema.pk_index { continue; }
            match &mut batch.columns[ci] {
                ColData::Fixed(buf) => {
                    let stride = col_def.type_code.wire_stride();
                    buf.extend(std::iter::repeat(0u8).take(n * stride));
                }
                ColData::Strings(v) => {
                    for _ in 0..n { v.push(Some(String::new())); }
                }
                ColData::U128s(u) => {
                    for _ in 0..n { u.push(0u128); }
                }
            }
        }
        ops::push(&self.conn, table_id, schema, &batch)?;
        Ok(())
    }

    // --- DDL ---

    pub fn create_schema(&self, name: &str) -> Result<u64, ClientError> {
        let new_sid = ops::alloc_schema_id(&self.conn)?;
        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(&schema);
        batch.pk_lo.push(new_sid);
        batch.pk_hi.push(0);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Strings(v) = &mut batch.columns[1] {
            v.push(Some(name.to_string()));
        }
        ops::push(&self.conn, SCHEMA_TAB, &schema, &batch)?;
        Ok(new_sid)
    }

    pub fn drop_schema(&self, name: &str) -> Result<(), ClientError> {
        let (_, data, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let data = data.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", name))
        })?;
        let schema_id = self.find_schema_id(&data, name)?;

        let schema = schema_tab_schema();
        let mut batch = ZSetBatch::new(&schema);
        batch.pk_lo.push(schema_id);
        batch.pk_hi.push(0);
        batch.weights.push(-1);
        batch.nulls.push(0);
        if let ColData::Strings(v) = &mut batch.columns[1] {
            v.push(Some(name.to_string()));
        }
        ops::push(&self.conn, SCHEMA_TAB, &schema, &batch)?;
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
        let new_tid = ops::alloc_table_id(&self.conn)?;

        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        // COL_TAB first — server hook fires on TABLE_TAB insert and reads COL_TAB
        self.push_col_tab_records(new_tid, OWNER_KIND_TABLE, columns)?;

        // TABLE_TAB last
        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(&tbl_schema);
        tb.pk_lo.push(new_tid);
        tb.pk_hi.push(0);
        tb.weights.push(1);
        tb.nulls.push(0);
        if let ColData::Fixed(buf) = &mut tb.columns[1] { push_u64_bytes(buf, schema_id); }
        if let ColData::Strings(v) = &mut tb.columns[2] { v.push(Some(table_name.to_string())); }
        if let ColData::Strings(v) = &mut tb.columns[3] { v.push(Some(String::new())); }
        if let ColData::Fixed(buf) = &mut tb.columns[4] { push_u64_bytes(buf, pk_col_idx as u64); }
        if let ColData::Fixed(buf) = &mut tb.columns[5] { push_u64_bytes(buf, 0); }
        if let ColData::Fixed(buf) = &mut tb.columns[6] { push_u64_bytes(buf, unique_pk as u64); }
        ops::push(&self.conn, TABLE_TAB, &tbl_schema, &tb)?;

        Ok(new_tid)
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> Result<(), ClientError> {
        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        let (_, tbl_batch, _) = ops::scan(&self.conn, TABLE_TAB)?;
        let tbl_batch = tbl_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Table '{}.{}' not found", schema_name, table_name))
        })?;
        let record = self.find_table_record(&tbl_batch, schema_id, table_name)?;

        let (_, col_batch, _) = ops::scan(&self.conn, COL_TAB)?;
        if let Some(col_batch) = col_batch {
            self.retract_col_tab_records(&col_batch, record.tid, OWNER_KIND_TABLE)?;
        }

        let tbl_schema = table_tab_schema();
        let mut tb = ZSetBatch::new(&tbl_schema);
        tb.pk_lo.push(record.tid);
        tb.pk_hi.push(0);
        tb.weights.push(-1);
        tb.nulls.push(0);
        if let ColData::Fixed(buf) = &mut tb.columns[1] { push_u64_bytes(buf, record.schema_id); }
        if let ColData::Strings(v) = &mut tb.columns[2] { v.push(Some(record.name.clone())); }
        if let ColData::Strings(v) = &mut tb.columns[3] { v.push(Some(record.directory.clone())); }
        if let ColData::Fixed(buf) = &mut tb.columns[4] { push_u64_bytes(buf, record.pk_col_idx); }
        if let ColData::Fixed(buf) = &mut tb.columns[5] { push_u64_bytes(buf, record.created_lsn); }
        if let ColData::Fixed(buf) = &mut tb.columns[6] { push_u64_bytes(buf, record.flags); }
        ops::push(&self.conn, TABLE_TAB, &tbl_schema, &tb)?;

        Ok(())
    }

    pub fn create_view(
        &self,
        schema_name: &str,
        view_name: &str,
        source_table_id: u64,
        output_columns: &[ColumnDef],
    ) -> Result<u64, ClientError> {
        let vid = ops::alloc_table_id(&self.conn)?;

        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        // 1. Column records
        self.push_col_tab_records(vid, OWNER_KIND_VIEW, output_columns)?;

        // 2. Dependency record (U128 PK: pk_hi=vid, pk_lo=source_table_id)
        {
            let dep_s = dep_tab_schema();
            let mut dep = ZSetBatch::new(&dep_s);
            dep.pk_lo.push(source_table_id);
            dep.pk_hi.push(vid);
            dep.weights.push(1);
            dep.nulls.push(0);
            if let ColData::Fixed(buf) = &mut dep.columns[1] { push_u64_bytes(buf, vid); }
            if let ColData::Fixed(buf) = &mut dep.columns[2] { push_u64_bytes(buf, 0); }
            if let ColData::Fixed(buf) = &mut dep.columns[3] { push_u64_bytes(buf, source_table_id); }
            ops::push(&self.conn, DEP_TAB, &dep_s, &dep)?;
        }

        // 3. Circuit nodes: SCAN_TRACE (node 0) + INTEGRATE (node 1)
        {
            let nodes_s = circuit_nodes_schema();
            let mut nodes = ZSetBatch::new(&nodes_s);
            for (node_id, _) in [(0u64, ()), (1u64, ())] {
                nodes.pk_lo.push(node_id);
                nodes.pk_hi.push(vid);
                nodes.weights.push(1);
                nodes.nulls.push(0);
            }
            if let ColData::Fixed(buf) = &mut nodes.columns[1] {
                push_u64_bytes(buf, OPCODE_SCAN_TRACE);
                push_u64_bytes(buf, OPCODE_INTEGRATE);
            }
            ops::push(&self.conn, CIRCUIT_NODES_TAB, &nodes_s, &nodes)?;
        }

        // 4. Circuit edge: node 0 -> node 1, port PORT_IN
        {
            let edges_s = circuit_edges_schema();
            let mut edges = ZSetBatch::new(&edges_s);
            edges.pk_lo.push(0);
            edges.pk_hi.push(vid);
            edges.weights.push(1);
            edges.nulls.push(0);
            if let ColData::Fixed(buf) = &mut edges.columns[1] { push_u64_bytes(buf, 0); }
            if let ColData::Fixed(buf) = &mut edges.columns[2] { push_u64_bytes(buf, 1); }
            if let ColData::Fixed(buf) = &mut edges.columns[3] { push_u64_bytes(buf, PORT_IN); }
            ops::push(&self.conn, CIRCUIT_EDGES_TAB, &edges_s, &edges)?;
        }

        // 5. Circuit source: node 0, table_id=0 (primary input resolved from deps)
        {
            let sources_s = circuit_sources_schema();
            let mut sources = ZSetBatch::new(&sources_s);
            sources.pk_lo.push(0);
            sources.pk_hi.push(vid);
            sources.weights.push(1);
            sources.nulls.push(0);
            if let ColData::Fixed(buf) = &mut sources.columns[1] { push_u64_bytes(buf, 0); }
            ops::push(&self.conn, CIRCUIT_SOURCES_TAB, &sources_s, &sources)?;
        }

        // 6. View record — must be last (triggers server-side hook + circuit compilation)
        self.push_view_record(vid, schema_id, view_name, "")?;

        Ok(vid)
    }

    pub fn create_view_with_circuit(
        &self,
        schema_name: &str,
        view_name: &str,
        sql_text: &str,
        circuit: CircuitGraph,
        output_columns: &[ColumnDef],
    ) -> Result<u64, ClientError> {
        let vid = if circuit.view_id == 0 {
            ops::alloc_table_id(&self.conn)?
        } else {
            circuit.view_id
        };

        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        // 1. Column records
        self.push_col_tab_records(vid, OWNER_KIND_VIEW, output_columns)?;

        // 2. Dependency records
        if !circuit.dependencies.is_empty() {
            let dep_s = dep_tab_schema();
            let mut dep = ZSetBatch::new(&dep_s);
            for &dep_tid in &circuit.dependencies {
                dep.pk_lo.push(dep_tid);
                dep.pk_hi.push(vid);
                dep.weights.push(1);
                dep.nulls.push(0);
                if let ColData::Fixed(buf) = &mut dep.columns[1] { push_u64_bytes(buf, vid); }
                if let ColData::Fixed(buf) = &mut dep.columns[2] { push_u64_bytes(buf, 0); }
                if let ColData::Fixed(buf) = &mut dep.columns[3] { push_u64_bytes(buf, dep_tid); }
            }
            ops::push(&self.conn, DEP_TAB, &dep_s, &dep)?;
        }

        // 3. Circuit nodes
        if !circuit.nodes.is_empty() {
            let nodes_s = circuit_nodes_schema();
            let mut nodes = ZSetBatch::new(&nodes_s);
            for &(node_id, opcode) in &circuit.nodes {
                nodes.pk_lo.push(node_id);
                nodes.pk_hi.push(vid);
                nodes.weights.push(1);
                nodes.nulls.push(0);
                if let ColData::Fixed(buf) = &mut nodes.columns[1] { push_u64_bytes(buf, opcode); }
            }
            ops::push(&self.conn, CIRCUIT_NODES_TAB, &nodes_s, &nodes)?;
        }

        // 4. Circuit edges
        if !circuit.edges.is_empty() {
            let edges_s = circuit_edges_schema();
            let mut edges = ZSetBatch::new(&edges_s);
            for &(edge_id, src, dst, port) in &circuit.edges {
                edges.pk_lo.push(edge_id);
                edges.pk_hi.push(vid);
                edges.weights.push(1);
                edges.nulls.push(0);
                if let ColData::Fixed(buf) = &mut edges.columns[1] { push_u64_bytes(buf, src); }
                if let ColData::Fixed(buf) = &mut edges.columns[2] { push_u64_bytes(buf, dst); }
                if let ColData::Fixed(buf) = &mut edges.columns[3] { push_u64_bytes(buf, port); }
            }
            ops::push(&self.conn, CIRCUIT_EDGES_TAB, &edges_s, &edges)?;
        }

        // 5. Circuit sources
        if !circuit.sources.is_empty() {
            let sources_s = circuit_sources_schema();
            let mut sources = ZSetBatch::new(&sources_s);
            for &(node_id, table_id) in &circuit.sources {
                sources.pk_lo.push(node_id);
                sources.pk_hi.push(vid);
                sources.weights.push(1);
                sources.nulls.push(0);
                if let ColData::Fixed(buf) = &mut sources.columns[1] { push_u64_bytes(buf, table_id); }
            }
            ops::push(&self.conn, CIRCUIT_SOURCES_TAB, &sources_s, &sources)?;
        }

        // 6. Circuit params (3-column schema: param_pk, value, str_value)
        if !circuit.params.is_empty() || !circuit.const_strings.is_empty() {
            let params_s = circuit_params_schema();
            let mut params = ZSetBatch::new(&params_s);

            // Numeric params: str_value = NULL
            for &(node_id, slot, value) in &circuit.params {
                let param_lo = (node_id << 8) | slot;
                params.pk_lo.push(param_lo);
                params.pk_hi.push(vid);
                params.weights.push(1);
                params.nulls.push(1 << 1); // bit 1 = str_value (payload col idx 1) is null
                if let ColData::Fixed(buf) = &mut params.columns[1] { push_u64_bytes(buf, value); }
                if let ColData::Strings(v) = &mut params.columns[2] { v.push(None); }
            }

            // String constant params: str_value = actual string
            for (node_id, slot, ref value) in &circuit.const_strings {
                let param_lo = (node_id << 8) | slot;
                params.pk_lo.push(param_lo);
                params.pk_hi.push(vid);
                params.weights.push(1);
                params.nulls.push(0); // no nulls
                if let ColData::Fixed(buf) = &mut params.columns[1] { push_u64_bytes(buf, 0); }
                if let ColData::Strings(v) = &mut params.columns[2] { v.push(Some(value.clone())); }
            }

            ops::push(&self.conn, CIRCUIT_PARAMS_TAB, &params_s, &params)?;
        }

        // 7. Circuit group cols
        if !circuit.group_cols.is_empty() {
            let gcols_s = circuit_group_cols_schema();
            let mut gcols = ZSetBatch::new(&gcols_s);
            for &(node_id, col_idx) in &circuit.group_cols {
                let gcol_lo = (node_id << 16) | col_idx;
                gcols.pk_lo.push(gcol_lo);
                gcols.pk_hi.push(vid);
                gcols.weights.push(1);
                gcols.nulls.push(0);
                if let ColData::Fixed(buf) = &mut gcols.columns[1] { push_u64_bytes(buf, col_idx); }
            }
            ops::push(&self.conn, CIRCUIT_GROUP_COLS_TAB, &gcols_s, &gcols)?;
        }

        // 8. View record — must be last
        self.push_view_record(vid, schema_id, view_name, sql_text)?;

        Ok(vid)
    }

    pub fn drop_view(&self, schema_name: &str, view_name: &str) -> Result<(), ClientError> {
        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        let (_, view_batch, _) = ops::scan(&self.conn, VIEW_TAB)?;
        let view_batch = view_batch.ok_or_else(|| {
            ClientError::ServerError(format!("View '{}.{}' not found", schema_name, view_name))
        })?;
        let vr = self.find_view_record(&view_batch, schema_id, view_name)?;

        // Retract view record first — triggers teardown
        {
            let view_s = view_tab_schema();
            let mut vb = ZSetBatch::new(&view_s);
            vb.pk_lo.push(vr.vid);
            vb.pk_hi.push(0);
            vb.weights.push(-1);
            vb.nulls.push(0);
            if let ColData::Fixed(buf) = &mut vb.columns[1] { push_u64_bytes(buf, vr.schema_id); }
            if let ColData::Strings(v) = &mut vb.columns[2] { v.push(Some(vr.name.clone())); }
            if let ColData::Strings(v) = &mut vb.columns[3] { v.push(Some(vr.sql_definition.clone())); }
            if let ColData::Strings(v) = &mut vb.columns[4] { v.push(Some(vr.cache_directory.clone())); }
            if let ColData::Fixed(buf) = &mut vb.columns[5] { push_u64_bytes(buf, vr.created_lsn); }
            ops::push(&self.conn, VIEW_TAB, &view_s, &vb)?;
        }

        self.retract_circuit_graph(vr.vid)?;
        self.retract_deps_for_view(vr.vid)?;

        let (_, col_batch, _) = ops::scan(&self.conn, COL_TAB)?;
        if let Some(col_batch) = col_batch {
            self.retract_col_tab_records(&col_batch, vr.vid, OWNER_KIND_VIEW)?;
        }

        Ok(())
    }

    pub fn resolve_table_id(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(u64, Schema), ClientError> {
        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        let (_, tbl_batch, _) = ops::scan(&self.conn, TABLE_TAB)?;
        let tbl_batch = tbl_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Table '{}.{}' not found", schema_name, table_name))
        })?;
        let record = self.find_table_record(&tbl_batch, schema_id, table_name)?;

        let (_, col_batch, _) = ops::scan(&self.conn, COL_TAB)?;
        let col_batch = col_batch.ok_or_else(|| {
            ClientError::ServerError("COL_TAB is empty".to_string())
        })?;

        let columns = self.extract_col_entries(&col_batch, record.tid, OWNER_KIND_TABLE)?;
        Ok((record.tid, Schema { columns, pk_index: record.pk_col_idx as usize }))
    }

    pub fn resolve_table_or_view_id(
        &self,
        schema_name: &str,
        name: &str,
    ) -> Result<(u64, Schema), ClientError> {
        let (_, schema_batch, _) = ops::scan(&self.conn, SCHEMA_TAB)?;
        let schema_batch = schema_batch.ok_or_else(|| {
            ClientError::ServerError(format!("Schema '{}' not found", schema_name))
        })?;
        let schema_id = self.find_schema_id(&schema_batch, schema_name)?;

        // Scan COL_TAB once — shared by both the table and view branches below
        let (_, col_batch, _) = ops::scan(&self.conn, COL_TAB)?;
        let col_batch = col_batch.ok_or_else(|| {
            ClientError::ServerError("COL_TAB is empty".to_string())
        })?;

        // Try TABLE_TAB first (most common path)
        let (_, tbl_batch, _) = ops::scan(&self.conn, TABLE_TAB)?;
        if let Some(ref tbl_batch) = tbl_batch {
            if let Ok(record) = self.find_table_record(tbl_batch, schema_id, name) {
                let columns = self.extract_col_entries(&col_batch, record.tid, OWNER_KIND_TABLE)?;
                return Ok((record.tid, Schema { columns, pk_index: record.pk_col_idx as usize }));
            }
        }

        // Fall back to VIEW_TAB
        let (_, view_batch, _) = ops::scan(&self.conn, VIEW_TAB)?;
        let view_batch = view_batch.ok_or_else(|| {
            ClientError::ServerError(
                format!("Table or view '{}.{}' not found", schema_name, name)
            )
        })?;
        let record = self.find_view_record(&view_batch, schema_id, name)?;
        let columns = self.extract_col_entries(&col_batch, record.vid, OWNER_KIND_VIEW)?;
        // View PK is always the U128 hash column at index 0
        Ok((record.vid, Schema { columns, pk_index: 0 }))
    }

    // --- Private helpers ---

    fn extract_col_entries(
        &self,
        col_batch:  &ZSetBatch,
        owner_id:   u64,
        owner_kind: u64,
    ) -> Result<Vec<ColumnDef>, ClientError> {
        let mut col_entries: Vec<(u64, String, TypeCode, bool)> = Vec::new();
        for i in 0..col_batch.len() {
            if col_batch.weights[i] <= 0 { continue; }
            let row_owner_id   = col_u64(&col_batch.columns[1], i);
            let row_owner_kind = col_u64(&col_batch.columns[2], i);
            if row_owner_id != owner_id || row_owner_kind != owner_kind { continue; }

            let col_idx     = col_u64(&col_batch.columns[3], i);
            let name        = col_str(&col_batch.columns[4], i).unwrap_or("").to_string();
            let tc_val      = col_u64(&col_batch.columns[5], i);
            let type_code   = TypeCode::try_from_u64(tc_val).map_err(ClientError::Protocol)?;
            let is_nullable = col_u64(&col_batch.columns[6], i) != 0;
            col_entries.push((col_idx, name, type_code, is_nullable));
        }
        col_entries.sort_by_key(|e| e.0);
        Ok(col_entries.into_iter().map(|(_, name, type_code, is_nullable)| {
            ColumnDef { name, type_code, is_nullable }
        }).collect())
    }

    fn find_schema_id(&self, batch: &ZSetBatch, name: &str) -> Result<u64, ClientError> {
        for i in 0..batch.len() {
            if batch.weights[i] <= 0 { continue; }
            if col_str(&batch.columns[1], i) == Some(name) {
                return Ok(batch.pk_lo[i]);
            }
        }
        Err(ClientError::ServerError(format!("Schema '{}' not found", name)))
    }

    fn find_table_record(
        &self,
        batch: &ZSetBatch,
        schema_id: u64,
        table_name: &str,
    ) -> Result<TableRecord, ClientError> {
        for i in 0..batch.len() {
            if batch.weights[i] <= 0 { continue; }
            if col_u64(&batch.columns[1], i) != schema_id { continue; }
            if col_str(&batch.columns[2], i) != Some(table_name) { continue; }
            return Ok(TableRecord {
                tid:         batch.pk_lo[i],
                schema_id,
                name:        table_name.to_string(),
                directory:   col_str(&batch.columns[3], i).unwrap_or("").to_string(),
                pk_col_idx:  col_u64(&batch.columns[4], i),
                created_lsn: col_u64(&batch.columns[5], i),
                flags:       col_u64(&batch.columns[6], i),
            });
        }
        Err(ClientError::ServerError(format!("Table '{}' not found", table_name)))
    }

    fn find_view_record(
        &self,
        batch: &ZSetBatch,
        schema_id: u64,
        view_name: &str,
    ) -> Result<ViewRecord, ClientError> {
        for i in 0..batch.len() {
            if batch.weights[i] <= 0 { continue; }
            if col_u64(&batch.columns[1], i) != schema_id { continue; }
            if col_str(&batch.columns[2], i) != Some(view_name) { continue; }
            return Ok(ViewRecord {
                vid:             batch.pk_lo[i],
                schema_id,
                name:            view_name.to_string(),
                sql_definition:  col_str(&batch.columns[3], i).unwrap_or("").to_string(),
                cache_directory: col_str(&batch.columns[4], i).unwrap_or("").to_string(),
                created_lsn:     col_u64(&batch.columns[5], i),
            });
        }
        Err(ClientError::ServerError(format!("View '{}' not found", view_name)))
    }

    fn push_col_tab_records(
        &self,
        owner_id: u64,
        owner_kind: u64,
        columns: &[ColumnDef],
    ) -> Result<(), ClientError> {
        let schema = col_tab_schema();
        let mut batch = ZSetBatch::new(&schema);
        for i in 0..columns.len() {
            batch.pk_lo.push(pack_col_id(owner_id, i));
            batch.pk_hi.push(0);
            batch.weights.push(1);
            batch.nulls.push(0);
            if let ColData::Fixed(buf) = &mut batch.columns[1] { push_u64_bytes(buf, owner_id); }
            if let ColData::Fixed(buf) = &mut batch.columns[2] { push_u64_bytes(buf, owner_kind); }
            if let ColData::Fixed(buf) = &mut batch.columns[3] { push_u64_bytes(buf, i as u64); }
            if let ColData::Strings(v) = &mut batch.columns[4] { v.push(Some(columns[i].name.clone())); }
            if let ColData::Fixed(buf) = &mut batch.columns[5] { push_u64_bytes(buf, columns[i].type_code as u64); }
            if let ColData::Fixed(buf) = &mut batch.columns[6] { push_u64_bytes(buf, if columns[i].is_nullable { 1 } else { 0 }); }
            if let ColData::Fixed(buf) = &mut batch.columns[7] { push_u64_bytes(buf, 0); }
            if let ColData::Fixed(buf) = &mut batch.columns[8] { push_u64_bytes(buf, 0); }
        }
        ops::push(&self.conn, COL_TAB, &schema, &batch)?;
        Ok(())
    }

    fn retract_col_tab_records(
        &self,
        col_batch: &ZSetBatch,
        owner_id: u64,
        owner_kind: u64,
    ) -> Result<(), ClientError> {
        let schema = col_tab_schema();
        let mut retract = ZSetBatch::new(&schema);
        for i in 0..col_batch.len() {
            if col_batch.weights[i] <= 0 { continue; }
            if col_u64(&col_batch.columns[1], i) != owner_id { continue; }
            if col_u64(&col_batch.columns[2], i) != owner_kind { continue; }
            retract.pk_lo.push(col_batch.pk_lo[i]);
            retract.pk_hi.push(0);
            retract.weights.push(-1);
            retract.nulls.push(0);
            for ci in 0..schema.columns.len() {
                if ci == schema.pk_index { continue; }
                let stride = schema.columns[ci].type_code.wire_stride();
                copy_col_row(&col_batch.columns[ci], i, &mut retract.columns[ci], stride);
            }
        }
        if retract.is_empty() { return Ok(()); }
        ops::push(&self.conn, COL_TAB, &schema, &retract)?;
        Ok(())
    }

    fn retract_circuit_graph(&self, vid: u64) -> Result<(), ClientError> {
        let circuit_tables: &[(u64, fn() -> Schema)] = &[
            (CIRCUIT_NODES_TAB,      circuit_nodes_schema      as fn() -> Schema),
            (CIRCUIT_EDGES_TAB,      circuit_edges_schema      as fn() -> Schema),
            (CIRCUIT_SOURCES_TAB,    circuit_sources_schema    as fn() -> Schema),
            (CIRCUIT_PARAMS_TAB,     circuit_params_schema     as fn() -> Schema),
            (CIRCUIT_GROUP_COLS_TAB, circuit_group_cols_schema as fn() -> Schema),
        ];
        for &(tab_id, schema_fn) in circuit_tables {
            let schema = schema_fn();
            let (_, batch, _) = ops::scan(&self.conn, tab_id)?;
            if let Some(batch) = batch {
                self.retract_records_for_view_u128(tab_id, &schema, &batch, vid)?;
            }
        }
        Ok(())
    }

    fn retract_records_for_view_u128(
        &self,
        table_id: u64,
        schema: &Schema,
        batch: &ZSetBatch,
        vid: u64,
    ) -> Result<(), ClientError> {
        let mut retract = ZSetBatch::new(schema);
        for i in 0..batch.len() {
            if batch.weights[i] <= 0 { continue; }
            if batch.pk_hi[i] != vid { continue; }
            retract.pk_lo.push(batch.pk_lo[i]);
            retract.pk_hi.push(batch.pk_hi[i]);
            retract.weights.push(-1);
            retract.nulls.push(0);
            for ci in 0..schema.columns.len() {
                if ci == schema.pk_index { continue; }
                let stride = schema.columns[ci].type_code.wire_stride();
                copy_col_row(&batch.columns[ci], i, &mut retract.columns[ci], stride);
            }
        }
        if retract.is_empty() { return Ok(()); }
        ops::push(&self.conn, table_id, schema, &retract)?;
        Ok(())
    }

    fn retract_deps_for_view(&self, vid: u64) -> Result<(), ClientError> {
        let (_, dep_batch, _) = ops::scan(&self.conn, DEP_TAB)?;
        let dep_batch = match dep_batch { Some(b) => b, None => return Ok(()) };

        let dep_s = dep_tab_schema();
        let mut retract = ZSetBatch::new(&dep_s);
        for i in 0..dep_batch.len() {
            if dep_batch.weights[i] <= 0 { continue; }
            // columns[1] = view_id (the owning view)
            if col_u64(&dep_batch.columns[1], i) != vid { continue; }
            retract.pk_lo.push(dep_batch.pk_lo[i]);
            retract.pk_hi.push(dep_batch.pk_hi[i]);
            retract.weights.push(-1);
            retract.nulls.push(0);
            for ci in 0..dep_s.columns.len() {
                if ci == dep_s.pk_index { continue; }
                let stride = dep_s.columns[ci].type_code.wire_stride();
                copy_col_row(&dep_batch.columns[ci], i, &mut retract.columns[ci], stride);
            }
        }
        if retract.is_empty() { return Ok(()); }
        ops::push(&self.conn, DEP_TAB, &dep_s, &retract)?;
        Ok(())
    }

    fn push_view_record(&self, vid: u64, schema_id: u64, view_name: &str, sql_text: &str) -> Result<(), ClientError> {
        let view_s = view_tab_schema();
        let mut vb = ZSetBatch::new(&view_s);
        vb.pk_lo.push(vid);
        vb.pk_hi.push(0);
        vb.weights.push(1);
        vb.nulls.push(0);
        if let ColData::Fixed(buf) = &mut vb.columns[1] { push_u64_bytes(buf, schema_id); }
        if let ColData::Strings(v) = &mut vb.columns[2] { v.push(Some(view_name.to_string())); }
        if let ColData::Strings(v) = &mut vb.columns[3] { v.push(Some(sql_text.to_string())); }
        if let ColData::Strings(v) = &mut vb.columns[4] { v.push(Some(String::new())); }
        if let ColData::Fixed(buf) = &mut vb.columns[5] { push_u64_bytes(buf, 0); }
        ops::push(&self.conn, VIEW_TAB, &view_s, &vb)?;
        Ok(())
    }
}
