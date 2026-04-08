//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! runs annotation + optimization passes, and emits VM instructions.
//!
//! Replaces `gnitz/catalog/program_cache.py::compile_from_graph` (~1400 lines
//! of Python) with a single entry point.

use std::collections::{HashMap, HashSet};

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::expr::{ExprProgram, EXPR_COPY_COL};
use crate::ops::AggDescriptor;
use crate::storage::{CursorHandle, Table, ReadCursor};
use crate::scalar_func::{Plan, ScalarFuncKind};
use crate::vm::{ProgramBuilder, VmHandle};

gnitz_wire::cast_consts! { i32;
    OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_DELTA,
    OPCODE_INTEGRATE, OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT,
    OPCODE_SCAN_TRACE, OPCODE_SEEK_TRACE, OPCODE_CLEAR_DELTAS,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_ANTI_JOIN_DELTA_DELTA,
    OPCODE_SEMI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_DELTA,
    OPCODE_EXCHANGE_SHARD, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_NULL_EXTEND, OPCODE_GATHER_REDUCE,
    PORT_IN, PORT_IN_A, PORT_IN_B, PORT_TRACE,
    PARAM_AGG_FUNC_ID, PARAM_TABLE_ID, PARAM_AGG_COL_IDX,
    PARAM_EXPR_NUM_REGS, PARAM_EXPR_RESULT_REG,
    PARAM_REINDEX_COL, PARAM_JOIN_SOURCE_TABLE,
    PARAM_AGG_COUNT, PARAM_AGG_SPEC_BASE,
    PARAM_KEY_ONLY, PARAM_NULL_EXTEND_COUNT, PARAM_NULL_EXTEND_COL_BASE,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_SHARD_COL_BASE, PARAM_CONST_STR_BASE,
}
gnitz_wire::cast_consts! { u8; AGG_COUNT, AGG_SUM, AGG_MIN, AGG_MAX, AGG_COUNT_NON_NULL }

// Engine-only constants
const PARAM_CHUNK_LIMIT: i32 = 2;
const PORT_DELTA: i32       = 0;
const PORT_IN_REDUCE: i32   = 0;
const PORT_TRACE_IN: i32    = 1;
const PORT_IN_DISTINCT: i32 = 0;
const PORT_EXCHANGE_IN: i32 = 0;

// System table column indices (after PK column)
const NODES_COL_OPCODE: usize = 1;
const EDGES_COL_SRC: usize = 1;
const EDGES_COL_DST: usize = 2;
const EDGES_COL_PORT: usize = 3;
const SOURCES_COL_TABLE_ID: usize = 1;
const PARAMS_COL_VALUE: usize = 1;
const PARAMS_COL_STR_VALUE: usize = 2;
const DEP_COL_DEP_TABLE_ID: usize = 3;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A node in the circuit graph.
#[derive(Clone, Copy)]
struct Node {
    id: i32,
    opcode: i32,
}

/// An edge in the circuit graph.
#[derive(Clone, Copy)]
struct Edge {
    _id: i32,
    src: i32,
    dst: i32,
    port: i32,
}

/// Loaded + topologically sorted circuit graph.
struct CircuitGraph {
    view_id: u64,
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    sources: HashMap<i32, i64>,       // node_id → table_id
    params: HashMap<i32, HashMap<i32, i64>>,  // node_id → {slot: value}
    str_params: HashMap<i32, HashMap<i32, Vec<u8>>>, // node_id → {slot: bytes}
    group_cols: HashMap<i32, Vec<i32>>, // node_id → [col_idx]
    out_schema: SchemaDescriptor,

    // Derived by topo_sort
    opcode_of: HashMap<i32, i32>,
    ordered: Vec<i32>,
    outgoing: HashMap<i32, Vec<(i32, i32)>>, // node_id → [(dst, port)]
    incoming: HashMap<i32, Vec<i32>>,        // node_id → [src_ids]
    consumers: HashMap<i32, Vec<i32>>,       // node_id → [consumer_ids]
}

/// Annotation results from pre-passes.
struct Annotation {
    trace_side_sources: HashSet<i32>,
    in_schema: SchemaDescriptor,
    join_shard_map: HashMap<i64, Vec<i32>>,    // source_tid → [reindex_col]
    co_partitioned: HashSet<i64>,               // source_tid set
    is_distinct_at: HashSet<i32>,               // node_id set
}

/// Optimization rewrite decisions.
struct Rewrites {
    skip_nodes: HashSet<i32>,
    fold_finalize: HashMap<i32, usize>,  // reduce_nid → index into owned_expr_progs
    folded_maps: HashMap<i32, i32>,      // map_nid → reduce_nid
}

/// External table handle + schema.
pub struct ExternalTable {
    pub table_id: i64,
    pub handle: *mut Table,
    pub schema: SchemaDescriptor,
}

/// Result of compilation, returned as a flat i64 buffer.
///
/// All values are stored as i64 for alignment simplicity. Pointers are
/// cast to i64 (safe on 64-bit). Layout:
///
/// ```text
/// [0]  pre_vm         (pointer as i64)
/// [1]  post_vm        (pointer as i64, 0 if no exchange)
/// [2]  pre_num_regs
/// [3]  post_num_regs
/// [4]  pre_in_reg
/// [5]  pre_out_reg
/// [6]  post_in_reg
/// [7]  post_out_reg
/// [8]  skip_exchange   (0 or 1)
/// [9]  num_shard_cols
/// [10..18] shard_cols  (8 slots)
/// [18] num_source_regs
/// [19..35] source_reg_tids  (16 slots)
/// [35..51] source_reg_ids   (16 slots, as i64)
/// [51] num_join_shard_entries
/// [52..68] join_shard_tids  (16 slots)
/// [68..84] join_shard_cols  (16 slots, as i64)
/// [84] num_co_partitioned
/// [85..101] co_partitioned_tids (16 slots)
/// [101] num_ext_trace_regs
/// [102..165] ext_trace_reg_ids (63 slots, as i64)
/// [165..228] ext_trace_table_ids (63 slots)
/// [228] pre_etr_count (how many ext_trace_regs belong to the pre-plan)
/// [229] (reserved)
/// [230] error_code
/// [231] exchange_schema_num_cols (0 if no exchange)
/// [232] exchange_schema_pk_index
/// [233..297] exchange col descriptors: (type_code << 16 | size << 8 | nullable) per col, 64 slots
/// Total: 297 i64 values
/// ```
pub const COMPILE_RESULT_SLOTS: usize = 297;

pub struct CompileResult {
    pub buf: [i64; COMPILE_RESULT_SLOTS],
}

// Slot offsets
const CR_PRE_VM: usize = 0;
const CR_POST_VM: usize = 1;
const CR_PRE_NUM_REGS: usize = 2;
const CR_POST_NUM_REGS: usize = 3;
const CR_PRE_IN_REG: usize = 4;
const CR_PRE_OUT_REG: usize = 5;
const CR_POST_IN_REG: usize = 6;
const CR_POST_OUT_REG: usize = 7;
const CR_SKIP_EXCHANGE: usize = 8;
const CR_NUM_SHARD_COLS: usize = 9;
const CR_SHARD_COLS: usize = 10;       // 8 slots
const CR_NUM_SOURCE_REGS: usize = 18;
const CR_SOURCE_REG_TIDS: usize = 19;  // 16 slots
const CR_SOURCE_REG_IDS: usize = 35;   // 16 slots
const CR_NUM_JSM: usize = 51;
const CR_JSM_TIDS: usize = 52;         // 16 slots
const CR_JSM_COLS: usize = 68;         // 16 slots
const CR_NUM_COP: usize = 84;
const CR_COP_TIDS: usize = 85;         // 16 slots
const CR_NUM_ETR: usize = 101;
const CR_ETR_REG_IDS: usize = 102;     // 63 slots
const CR_ETR_TABLE_IDS: usize = 165;   // 63 slots
const CR_PRE_ETR_COUNT: usize = 228;   // how many ext_trace_regs belong to pre-plan
const CR_ERROR_CODE: usize = 230;

impl CompileResult {
    pub fn error(code: i32) -> Self {
        let mut r = Self::empty();
        r.buf[CR_ERROR_CODE] = code as i64;
        r
    }

    fn empty() -> Self {
        CompileResult {
            buf: [0i64; 297],
        }
    }

    fn set_pre_vm(&mut self, vm: Box<VmHandle>) {
        self.buf[CR_PRE_VM] = Box::into_raw(vm) as i64;
    }
    fn set_post_vm(&mut self, vm: Box<VmHandle>) {
        self.buf[CR_POST_VM] = Box::into_raw(vm) as i64;
    }
}

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result for internal Rust consumers
// ---------------------------------------------------------------------------

/// Rich, typed output from `compile_view`.  Used directly by DagEngine to
/// build `CachedPlan` without going through the flat i64 buffer.
///
/// The existing FFI path calls `to_result_buffer()` to produce the legacy
/// `CompileResult` for FFI callers.
pub struct CompileOutput {
    pub pre_vm: Box<VmHandle>,
    pub post_vm: Option<Box<VmHandle>>,
    pub out_schema: SchemaDescriptor,
    pub exchange_in_schema: Option<SchemaDescriptor>,
    pub pre_num_regs: u32,
    pub pre_in_reg: u16,
    pub pre_out_reg: u16,
    pub post_num_regs: u32,
    pub post_in_reg: u16,
    pub post_out_reg: u16,
    pub skip_exchange: bool,
    pub shard_cols: Vec<i32>,
    pub source_reg_map: HashMap<i64, u16>,
    pub join_shard_map: HashMap<i64, Vec<i32>>,
    pub co_partitioned: HashSet<i64>,
    pub pre_ext_trace_regs: Vec<(u16, i64)>,
    pub post_ext_trace_regs: Vec<(u16, i64)>,
}

impl CompileOutput {
    /// Convert to the flat i64[297] buffer expected by the existing FFI path.
    /// **Consumes** self: VmHandle ownership transfers into the buffer as raw
    /// pointers (freed via `gnitz_vm_program_free`).
    pub fn to_result_buffer(self) -> CompileResult {
        let mut result = CompileResult::empty();

        result.set_pre_vm(self.pre_vm);
        result.buf[CR_PRE_NUM_REGS] = self.pre_num_regs as i64;
        result.buf[CR_PRE_IN_REG] = self.pre_in_reg as i64;
        result.buf[CR_PRE_OUT_REG] = self.pre_out_reg as i64;

        if let Some(post_vm) = self.post_vm {
            result.set_post_vm(post_vm);
            result.buf[CR_POST_NUM_REGS] = self.post_num_regs as i64;
            result.buf[CR_POST_IN_REG] = self.post_in_reg as i64;
            result.buf[CR_POST_OUT_REG] = self.post_out_reg as i64;
        }

        result.buf[CR_SKIP_EXCHANGE] = if self.skip_exchange { 1 } else { 0 };

        // Shard cols
        let nsc = self.shard_cols.len().min(8);
        result.buf[CR_NUM_SHARD_COLS] = nsc as i64;
        for (i, &sc) in self.shard_cols.iter().take(8).enumerate() {
            result.buf[CR_SHARD_COLS + i] = sc as i64;
        }

        // Source reg map
        let mut si = 0;
        for (&tid, &reg) in &self.source_reg_map {
            if si < 16 {
                result.buf[CR_SOURCE_REG_TIDS + si] = tid;
                result.buf[CR_SOURCE_REG_IDS + si] = reg as i64;
                si += 1;
            }
        }
        result.buf[CR_NUM_SOURCE_REGS] = si as i64;

        // Join shard map
        let mut ji = 0;
        for (&tid, cols) in &self.join_shard_map {
            if ji < 16 && !cols.is_empty() {
                result.buf[CR_JSM_TIDS + ji] = tid;
                result.buf[CR_JSM_COLS + ji] = cols[0] as i64;
                ji += 1;
            }
        }
        result.buf[CR_NUM_JSM] = ji as i64;

        // Co-partitioned
        let mut ci = 0;
        for &tid in self.co_partitioned.iter() {
            if ci < 16 {
                result.buf[CR_COP_TIDS + ci] = tid;
                ci += 1;
            }
        }
        result.buf[CR_NUM_COP] = ci as i64;

        // Ext trace regs
        let pre_etr_count = self.pre_ext_trace_regs.len();
        populate_ext_trace_regs(&mut result, &self.pre_ext_trace_regs, &self.post_ext_trace_regs);
        result.buf[CR_PRE_ETR_COUNT] = pre_etr_count as i64;

        // Exchange schema
        if let Some(ref ex_schema) = self.exchange_in_schema {
            result.buf[231] = ex_schema.num_columns as i64;
            result.buf[232] = ex_schema.pk_index as i64;
            for i in 0..ex_schema.num_columns as usize {
                let c = &ex_schema.columns[i];
                result.buf[233 + i] = ((c.type_code as i64) << 16) | ((c.size as i64) << 8) | (c.nullable as i64);
            }
        }

        result
    }
}

// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

/// Read an i64 value from a cursor's current row at the given column index.
pub fn cursor_read_i64(cursor: &ReadCursor, col_idx: usize, schema: &SchemaDescriptor) -> i64 {
    let col_size = schema.columns[col_idx].size as usize;
    let ptr = cursor.col_ptr(col_idx, col_size);
    if ptr.is_null() {
        return 0;
    }
    match col_size {
        8 => i64::from_le_bytes(unsafe { *(ptr as *const [u8; 8]) }),
        4 => (unsafe { *(ptr as *const i32) }) as i64,
        2 => (unsafe { *(ptr as *const i16) }) as i64,
        1 => (unsafe { *ptr }) as i8 as i64,
        _ => 0,
    }
}

/// Read a German String from cursor column. Returns bytes (may be empty).
fn cursor_read_string(cursor: &ReadCursor, col_idx: usize, _schema: &SchemaDescriptor) -> Vec<u8> {
    use crate::schema::SHORT_STRING_THRESHOLD;

    let ptr = cursor.col_ptr(col_idx, 16);
    if ptr.is_null() {
        return Vec::new();
    }

    // German string layout: [length:u32][prefix:4bytes][payload:8bytes]
    // If length <= 12: inline (prefix + first 8 bytes of payload)
    // If length > 12: prefix + offset(u32) into blob arena
    let length = unsafe { *(ptr as *const u32) } as usize;
    if length == 0 {
        return Vec::new();
    }

    if length <= SHORT_STRING_THRESHOLD {
        // Inline: bytes are at ptr+4
        let data_ptr = unsafe { ptr.add(4) };
        let mut buf = vec![0u8; length];
        unsafe { std::ptr::copy_nonoverlapping(data_ptr, buf.as_mut_ptr(), length) };
        buf
    } else {
        // Long string: 4 bytes prefix at ptr+4, then blob offset (u64) at ptr+8
        let blob_offset = unsafe { *(ptr.add(8) as *const u64) } as usize;
        let blob_ptr = cursor.blob_ptr();
        let blob_len = cursor.blob_len();
        if blob_ptr.is_null() || blob_offset + length > blob_len {
            return Vec::new();
        }
        let mut buf = vec![0u8; length];
        unsafe {
            std::ptr::copy_nonoverlapping(blob_ptr.add(blob_offset), buf.as_mut_ptr(), length);
        }
        buf
    }
}

/// Check if a column is NULL in the current cursor row.
fn cursor_is_null(cursor: &ReadCursor, col_idx: usize, schema: &SchemaDescriptor) -> bool {
    if col_idx == schema.pk_index as usize {
        return false; // PK is never null
    }
    // Compute payload index
    let payload_idx = if col_idx < schema.pk_index as usize {
        col_idx
    } else {
        col_idx - 1
    };
    (cursor.current_null_word >> payload_idx) & 1 != 0
}

/// Create a cursor for a system table, seeked to view_id range.
/// Returns None if the table handle is null.
fn open_system_cursor(
    table: *mut Table,
    view_id: u64,
) -> Option<CursorHandle<'static>> {
    if table.is_null() {
        return None;
    }
    let t = unsafe { &mut *table };
    let mut ch = t.create_cursor().ok()?;
    ch.cursor.seek(crate::util::make_pk(0, view_id));
    Some(ch)
}

/// Load circuit nodes from system table.
fn load_nodes(table: *mut Table, view_id: u64, schema: &SchemaDescriptor) -> Vec<Node> {
    let mut result = Vec::new();
    let mut ch = match open_system_cursor(table, view_id) {
        Some(c) => c,
        None => return result,
    };
    let end_hi = view_id + 1;
    while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
        if ch.cursor.current_weight > 0 {
            let node_id = ch.cursor.current_key_lo as i32;
            let opcode = cursor_read_i64(&ch.cursor, NODES_COL_OPCODE, schema) as i32;
            result.push(Node { id: node_id, opcode });
        }
        ch.cursor.advance();
        if ch.cursor.valid && ch.cursor.current_key_hi >= end_hi {
            break;
        }
    }
    result
}

/// Load circuit edges from system table.
fn load_edges(table: *mut Table, view_id: u64, schema: &SchemaDescriptor) -> Vec<Edge> {
    let mut result = Vec::new();
    let mut ch = match open_system_cursor(table, view_id) {
        Some(c) => c,
        None => return result,
    };
    let end_hi = view_id + 1;
    while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
        if ch.cursor.current_weight > 0 {
            let edge_id = ch.cursor.current_key_lo as i32;
            let src = cursor_read_i64(&ch.cursor, EDGES_COL_SRC, schema) as i32;
            let dst = cursor_read_i64(&ch.cursor, EDGES_COL_DST, schema) as i32;
            let port = cursor_read_i64(&ch.cursor, EDGES_COL_PORT, schema) as i32;
            result.push(Edge { _id: edge_id, src, dst, port });
        }
        ch.cursor.advance();
        if ch.cursor.valid && ch.cursor.current_key_hi >= end_hi {
            break;
        }
    }
    result
}

/// Load circuit sources from system table.
fn load_sources(table: *mut Table, view_id: u64, schema: &SchemaDescriptor) -> HashMap<i32, i64> {
    let mut result = HashMap::new();
    let mut ch = match open_system_cursor(table, view_id) {
        Some(c) => c,
        None => return result,
    };
    let end_hi = view_id + 1;
    while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
        if ch.cursor.current_weight > 0 {
            let node_id = ch.cursor.current_key_lo as i32;
            let table_id = cursor_read_i64(&ch.cursor, SOURCES_COL_TABLE_ID, schema);
            result.insert(node_id, table_id);
        }
        ch.cursor.advance();
        if ch.cursor.valid && ch.cursor.current_key_hi >= end_hi {
            break;
        }
    }
    result
}

/// Load circuit params (int + string) from system table.
fn load_params(
    table: *mut Table,
    view_id: u64,
    schema: &SchemaDescriptor,
) -> (HashMap<i32, HashMap<i32, i64>>, HashMap<i32, HashMap<i32, Vec<u8>>>) {
    let mut int_params: HashMap<i32, HashMap<i32, i64>> = HashMap::new();
    let mut str_params: HashMap<i32, HashMap<i32, Vec<u8>>> = HashMap::new();
    let mut ch = match open_system_cursor(table, view_id) {
        Some(c) => c,
        None => return (int_params, str_params),
    };
    let end_hi = view_id + 1;
    while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
        if ch.cursor.current_weight > 0 {
            let lo64 = ch.cursor.current_key_lo;
            let node_id = (lo64 >> 8) as i32;
            let slot = (lo64 & 0xFF) as i32;
            let value = cursor_read_i64(&ch.cursor, PARAMS_COL_VALUE, schema);
            int_params.entry(node_id).or_default().insert(slot, value);

            if !cursor_is_null(&ch.cursor, PARAMS_COL_STR_VALUE, schema) {
                let sv = cursor_read_string(&ch.cursor, PARAMS_COL_STR_VALUE, schema);
                if !sv.is_empty() {
                    str_params.entry(node_id).or_default().insert(slot, sv);
                }
            }
        }
        ch.cursor.advance();
        if ch.cursor.valid && ch.cursor.current_key_hi >= end_hi {
            break;
        }
    }
    (int_params, str_params)
}

/// Load circuit group columns from system table.
fn load_group_cols(table: *mut Table, view_id: u64, _schema: &SchemaDescriptor) -> HashMap<i32, Vec<i32>> {
    let mut result: HashMap<i32, Vec<i32>> = HashMap::new();
    let mut ch = match open_system_cursor(table, view_id) {
        Some(c) => c,
        None => return result,
    };
    let end_hi = view_id + 1;
    while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
        if ch.cursor.current_weight > 0 {
            let lo64 = ch.cursor.current_key_lo;
            let node_id = (lo64 >> 16) as i32;
            let col_idx = (lo64 & 0xFFFF) as i32;
            result.entry(node_id).or_default().push(col_idx);
        }
        ch.cursor.advance();
        if ch.cursor.valid && ch.cursor.current_key_hi >= end_hi {
            break;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

fn topo_sort(graph: &mut CircuitGraph) -> Result<(), i32> {
    let mut in_degree: HashMap<i32, i32> = HashMap::new();
    for n in &graph.nodes {
        graph.opcode_of.insert(n.id, n.opcode);
        in_degree.insert(n.id, 0);
        graph.outgoing.insert(n.id, Vec::new());
        graph.incoming.insert(n.id, Vec::new());
        graph.consumers.insert(n.id, Vec::new());
    }

    for e in &graph.edges {
        graph.outgoing.entry(e.src).or_default().push((e.dst, e.port));
        graph.incoming.entry(e.dst).or_default().push(e.src);
        graph.consumers.entry(e.src).or_default().push(e.dst);
        *in_degree.entry(e.dst).or_insert(0) += 1;
    }

    let mut queue: Vec<i32> = Vec::new();
    for n in &graph.nodes {
        if *in_degree.get(&n.id).unwrap_or(&0) == 0 {
            queue.push(n.id);
        }
    }

    let mut ordered = Vec::with_capacity(graph.nodes.len());
    while let Some(nid) = queue.first().copied() {
        queue.remove(0);
        ordered.push(nid);
        if let Some(outs) = graph.outgoing.get(&nid) {
            for &(dst, _) in outs {
                let deg = in_degree.get_mut(&dst).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push(dst);
                }
            }
        }
    }

    if ordered.len() != graph.nodes.len() {
        return Err(-1); // cycle detected
    }
    graph.ordered = ordered;
    Ok(())
}

// ---------------------------------------------------------------------------
// Annotation passes
// ---------------------------------------------------------------------------

fn compute_trace_sides(graph: &CircuitGraph) -> HashSet<i32> {
    let mut trace_side_sources = HashSet::new();
    for e in &graph.edges {
        if graph.opcode_of.get(&e.src) == Some(&OPCODE_SCAN_TRACE) && e.port == PORT_TRACE {
            let dst_op = graph.opcode_of.get(&e.dst).copied().unwrap_or(-1);
            if dst_op == OPCODE_JOIN_DELTA_TRACE
                || dst_op == OPCODE_JOIN_DELTA_TRACE_OUTER
                || dst_op == OPCODE_ANTI_JOIN_DELTA_TRACE
                || dst_op == OPCODE_SEMI_JOIN_DELTA_TRACE
                || dst_op == OPCODE_SEEK_TRACE
            {
                trace_side_sources.insert(e.src);
            }
        }
    }
    trace_side_sources
}

fn resolve_primary_input_schema(
    dep_table: *mut Table,
    dep_schema: &SchemaDescriptor,
    view_id: u64,
    sources: &HashMap<i32, i64>,
    trace_side_sources: &HashSet<i32>,
    ext_tables: &[ExternalTable],
    fallback: &SchemaDescriptor,
) -> SchemaDescriptor {
    // Collect table IDs that feed trace ports
    let mut trace_table_ids: HashSet<i64> = HashSet::new();
    for &nid in trace_side_sources.iter() {
        if let Some(&tid) = sources.get(&nid) {
            if tid > 0 {
                trace_table_ids.insert(tid);
            }
        }
    }

    let mut ch = match open_system_cursor(dep_table, view_id) {
        Some(c) => c,
        None => return *fallback,
    };
    let end_hi = view_id + 1;
    let mut any_dep_schema: Option<SchemaDescriptor> = None;
    while ch.cursor.valid && ch.cursor.current_key_hi == view_id {
        if ch.cursor.current_weight > 0 {
            let dep_tid = cursor_read_i64(&ch.cursor, DEP_COL_DEP_TABLE_ID, dep_schema);
            if dep_tid > 0 {
                if let Some(ext) = ext_tables.iter().find(|t| t.table_id == dep_tid) {
                    if any_dep_schema.is_none() {
                        any_dep_schema = Some(ext.schema);
                    }
                    if !trace_table_ids.contains(&dep_tid) {
                        return ext.schema;
                    }
                }
            }
        }
        ch.cursor.advance();
        if ch.cursor.valid && ch.cursor.current_key_hi >= end_hi {
            break;
        }
    }

    any_dep_schema.unwrap_or(*fallback)
}

fn compute_join_shard_map(
    graph: &CircuitGraph,
    _trace_side_sources: &HashSet<i32>,
) -> HashMap<i64, Vec<i32>> {
    let mut join_shard_map = HashMap::new();
    for e in &graph.edges {
        if graph.opcode_of.get(&e.src) == Some(&OPCODE_SCAN_TRACE) {
            let src_params = graph.params.get(&e.src);
            let source_tid = src_params.and_then(|p| p.get(&PARAM_JOIN_SOURCE_TABLE)).copied().unwrap_or(0);
            if source_tid > 0 && graph.opcode_of.get(&e.dst) == Some(&OPCODE_MAP) {
                let dst_params = graph.params.get(&e.dst);
                let reindex_col = dst_params.and_then(|p| p.get(&PARAM_REINDEX_COL)).copied().unwrap_or(-1);
                if reindex_col >= 0 {
                    join_shard_map.insert(source_tid, vec![reindex_col as i32]);
                }
            }
        }
    }
    join_shard_map
}

fn compute_co_partitioned(
    join_shard_map: &HashMap<i64, Vec<i32>>,
    ext_tables: &[ExternalTable],
) -> HashSet<i64> {
    let mut co_partitioned = HashSet::new();
    for (&tid, cols) in join_shard_map {
        if cols.len() == 1 {
            if let Some(ext) = ext_tables.iter().find(|t| t.table_id == tid) {
                if cols[0] == ext.schema.pk_index as i32 {
                    co_partitioned.insert(tid);
                }
            }
        }
    }
    co_partitioned
}

fn propagate_distinct(graph: &CircuitGraph, ann: &mut Annotation) {
    for &nid in &graph.ordered {
        let op = graph.opcode_of.get(&nid).copied().unwrap_or(-1);
        if op == OPCODE_REDUCE || op == OPCODE_DISTINCT {
            ann.is_distinct_at.insert(nid);
        } else if op == OPCODE_FILTER {
            let in_nids = graph.incoming.get(&nid).cloned().unwrap_or_default();
            if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0]) {
                ann.is_distinct_at.insert(nid);
            }
        } else if op == OPCODE_MAP {
            let node_params = graph.params.get(&nid);
            let reindex = node_params.and_then(|p| p.get(&PARAM_REINDEX_COL)).copied().unwrap_or(-1);
            if reindex < 0 {
                let in_nids = graph.incoming.get(&nid).cloned().unwrap_or_default();
                if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0]) {
                    ann.is_distinct_at.insert(nid);
                }
            }
        }
    }
}

fn annotate(
    graph: &CircuitGraph,
    dep_table: *mut Table,
    dep_schema: &SchemaDescriptor,
    ext_tables: &[ExternalTable],
) -> Annotation {
    let trace_side_sources = compute_trace_sides(graph);
    let in_schema = resolve_primary_input_schema(
        dep_table, dep_schema, graph.view_id,
        &graph.sources, &trace_side_sources, ext_tables, &graph.out_schema,
    );
    let join_shard_map = compute_join_shard_map(graph, &trace_side_sources);
    let co_partitioned = compute_co_partitioned(&join_shard_map, ext_tables);
    let mut ann = Annotation {
        trace_side_sources,
        in_schema,
        join_shard_map,
        co_partitioned,
        is_distinct_at: HashSet::new(),
    };
    propagate_distinct(graph, &mut ann);
    ann
}

// ---------------------------------------------------------------------------
// Optimization passes
// ---------------------------------------------------------------------------

fn extract_map_code(node_params: &HashMap<i32, i64>) -> Vec<i64> {
    let mut code = Vec::new();
    let mut idx = 0;
    while node_params.contains_key(&(PARAM_EXPR_BASE + idx)) {
        code.push(node_params[&(PARAM_EXPR_BASE + idx)]);
        idx += 1;
    }
    code
}

fn all_copy_col_sequential(code: &[i64]) -> bool {
    let n = code.len();
    if n == 0 || n % 4 != 0 {
        return false;
    }
    let mut expected_src: i64 = 1;
    let mut i = 0;
    while i < n {
        if code[i] != EXPR_COPY_COL {
            return false;
        }
        if code[i + 2] != expected_src {
            return false;
        }
        expected_src += 1;
        i += 4;
    }
    true
}

fn schemas_physically_identical(a: &SchemaDescriptor, b: &SchemaDescriptor) -> bool {
    if a.num_columns != b.num_columns || a.pk_index != b.pk_index {
        return false;
    }
    for i in 0..a.num_columns as usize {
        if a.columns[i].type_code != b.columns[i].type_code {
            return false;
        }
    }
    true
}

/// Split fold programs and rewrite state between pre- and post-exchange plans.
///
/// `opt_fold_reduce_map` records programs for ALL REDUCE nodes in the graph.
/// When the graph is split at EXCHANGE_SHARD, only the programs for REDUCE
/// nodes in each plan half are valid for that half. This function partitions
/// `progs` and re-indexes `fold_finalize` so each plan half gets zero-based
/// indices into its own program slice.
fn split_fold_programs(
    rw: Rewrites,
    progs: Vec<Box<ExprProgram>>,
    pre_nids: &HashSet<i32>,
) -> (Rewrites, Vec<Box<ExprProgram>>, Rewrites, Vec<Box<ExprProgram>>) {
    // Invert fold_finalize: old program index → reduce_nid
    let mut idx_to_nid: HashMap<usize, i32> = HashMap::new();
    for (&nid, &idx) in &rw.fold_finalize {
        idx_to_nid.insert(idx, nid);
    }

    let mut pre_progs: Vec<Box<ExprProgram>> = Vec::new();
    let mut post_progs: Vec<Box<ExprProgram>> = Vec::new();
    let mut pre_fold: HashMap<i32, usize> = HashMap::new();
    let mut post_fold: HashMap<i32, usize> = HashMap::new();

    for (old_idx, prog) in progs.into_iter().enumerate() {
        if let Some(&nid) = idx_to_nid.get(&old_idx) {
            if pre_nids.contains(&nid) {
                let new_idx = pre_progs.len();
                pre_progs.push(prog);
                pre_fold.insert(nid, new_idx);
            } else {
                let new_idx = post_progs.len();
                post_progs.push(prog);
                post_fold.insert(nid, new_idx);
            }
        }
    }

    let rw_pre = Rewrites {
        skip_nodes: rw.skip_nodes.clone(),
        fold_finalize: pre_fold,
        folded_maps: rw.folded_maps.clone(),
    };
    let rw_post = Rewrites {
        skip_nodes: rw.skip_nodes,
        fold_finalize: post_fold,
        folded_maps: rw.folded_maps,
    };

    (rw_pre, pre_progs, rw_post, post_progs)
}

fn opt_distinct(graph: &CircuitGraph, ann: &Annotation, rw: &mut Rewrites) {
    for n in &graph.nodes {
        if n.opcode == OPCODE_DISTINCT {
            let in_nids = graph.incoming.get(&n.id).cloned().unwrap_or_default();
            if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0]) {
                rw.skip_nodes.insert(n.id);
            }
        }
    }
}

fn opt_fold_reduce_map(
    graph: &CircuitGraph,
    _ann: &Annotation,
    rw: &mut Rewrites,
    owned_expr_progs: &mut Vec<Box<ExprProgram>>,
) {
    for n in &graph.nodes {
        if n.opcode != OPCODE_MAP {
            continue;
        }
        let in_nids = graph.incoming.get(&n.id).cloned().unwrap_or_default();
        if in_nids.len() != 1 {
            continue;
        }
        let reduce_nid = in_nids[0];
        if graph.opcode_of.get(&reduce_nid) != Some(&OPCODE_REDUCE) {
            continue;
        }
        let consumers = graph.consumers.get(&reduce_nid).cloned().unwrap_or_default();
        if consumers.len() != 1 {
            continue;
        }
        let node_params = match graph.params.get(&n.id) {
            Some(p) => p,
            None => continue,
        };
        let num_regs = node_params.get(&PARAM_EXPR_NUM_REGS).copied().unwrap_or(0);
        let has_code = num_regs > 0 || node_params.contains_key(&PARAM_EXPR_BASE);
        if !has_code {
            continue;
        }
        let code = extract_map_code(node_params);
        if all_copy_col_sequential(&code) {
            continue; // identity MAP handled inline
        }
        if code.len() <= 63 {
            let prog = ExprProgram::new(code, num_regs as u32, 0, Vec::new());
            let idx = owned_expr_progs.len();
            owned_expr_progs.push(Box::new(prog));
            rw.fold_finalize.insert(reduce_nid, idx);
            rw.folded_maps.insert(n.id, reduce_nid);
        }
    }
}

// ---------------------------------------------------------------------------
// Schema construction helpers
// ---------------------------------------------------------------------------

pub fn empty_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        num_columns: 0,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    }
}

fn col(tc: u8, sz: u8, nullable: bool) -> SchemaColumn {
    SchemaColumn { type_code: tc, size: sz, nullable: if nullable { 1 } else { 0 }, _pad: 0 }
}



fn merge_schemas_for_join(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
    let mut out = empty_schema();
    let mut ci: usize = 0;
    // PK from left
    out.columns[ci] = left.columns[left.pk_index as usize];
    ci += 1;
    // Left payloads
    for i in 0..left.num_columns as usize {
        if i != left.pk_index as usize {
            out.columns[ci] = left.columns[i];
            ci += 1;
        }
    }
    // Right payloads (skip right PK)
    for i in 0..right.num_columns as usize {
        if i != right.pk_index as usize {
            out.columns[ci] = right.columns[i];
            ci += 1;
        }
    }
    out.num_columns = ci as u32;
    out.pk_index = 0;
    out
}

fn merge_schemas_for_join_outer(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
    let mut out = empty_schema();
    let mut ci: usize = 0;
    out.columns[ci] = left.columns[left.pk_index as usize];
    ci += 1;
    for i in 0..left.num_columns as usize {
        if i != left.pk_index as usize {
            out.columns[ci] = left.columns[i];
            ci += 1;
        }
    }
    for i in 0..right.num_columns as usize {
        if i != right.pk_index as usize {
            let mut c = right.columns[i];
            c.nullable = 1; // right side nullable in outer join
            out.columns[ci] = c;
            ci += 1;
        }
    }
    out.num_columns = ci as u32;
    out.pk_index = 0;
    out
}

fn build_map_output_schema(input: &SchemaDescriptor, src_indices: &[i32]) -> SchemaDescriptor {
    let mut out = empty_schema();
    let mut ci: usize = 0;
    out.columns[ci] = input.columns[input.pk_index as usize];
    ci += 1;
    for &idx in src_indices {
        let i = idx as usize;
        if i != input.pk_index as usize {
            out.columns[ci] = input.columns[i];
            ci += 1;
        }
    }
    out.num_columns = ci as u32;
    out.pk_index = 0;
    out
}

/// Determine the output type for an aggregate function.
/// Determine the output type for an aggregate function.
/// Must match `UniversalAccumulator.output_column_type()`:
///   COUNT, COUNT_NON_NULL → I64
///   SUM/MIN/MAX on float → F64
///   everything else → I64  (including MIN/MAX on STRING, I32, etc.)
fn agg_output_type(agg_op: u8, col_type_code: u8) -> (u8, u8) {
    match agg_op {
        AGG_COUNT | AGG_COUNT_NON_NULL => (type_code::I64, 8),
        AGG_SUM | AGG_MIN | AGG_MAX => {
            if col_type_code == type_code::F32 || col_type_code == type_code::F64 {
                (type_code::F64, 8)
            } else {
                (type_code::I64, 8)
            }
        }
        _ => (type_code::I64, 8),
    }
}

fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[i32],
    agg_descs: &[AggDescriptor],
) -> SchemaDescriptor {
    let mut out = empty_schema();
    let mut ci: usize = 0;

    let use_natural_pk = if group_cols.len() == 1 {
        let gc_type = input.columns[group_cols[0] as usize].type_code;
        gc_type == type_code::U64 || gc_type == type_code::U128
    } else {
        false
    };

    if use_natural_pk {
        // Col 0: group column (PK)
        out.columns[ci] = input.columns[group_cols[0] as usize];
        ci += 1;
        // Aggregate results
        for ad in agg_descs {
            let (tc, sz) = agg_output_type(ad.agg_op, ad.col_type_code);
            out.columns[ci] = col(tc, sz, false);
            ci += 1;
        }
    } else {
        // Synthetic U128 PK
        out.columns[ci] = col(type_code::U128, 16, false);
        ci += 1;
        // Group columns
        for &gc in group_cols {
            out.columns[ci] = input.columns[gc as usize];
            ci += 1;
        }
        // Aggregate results
        for ad in agg_descs {
            let (tc, sz) = agg_output_type(ad.agg_op, ad.col_type_code);
            out.columns[ci] = col(tc, sz, false);
            ci += 1;
        }
    }
    out.num_columns = ci as u32;
    out.pk_index = 0;
    out
}

fn make_group_idx_schema() -> SchemaDescriptor {
    let mut s = empty_schema();
    s.columns[0] = col(type_code::U128, 16, false); // ck (PK)
    s.columns[1] = col(type_code::I64, 8, false);   // spk_hi
    s.num_columns = 2;
    s.pk_index = 0;
    s
}

fn make_agg_value_idx_schema() -> SchemaDescriptor {
    let mut s = empty_schema();
    s.columns[0] = col(type_code::U128, 16, false); // ck (PK only)
    s.num_columns = 1;
    s.pk_index = 0;
    s
}

fn is_linear_agg(agg_op: u8) -> bool {
    agg_op == AGG_COUNT || agg_op == AGG_SUM || agg_op == AGG_COUNT_NON_NULL
}

fn agg_value_idx_eligible(col_type_code: u8) -> bool {
    col_type_code != type_code::U128 && col_type_code != type_code::STRING
}

// ---------------------------------------------------------------------------
// Expression + scalar function construction helpers
// ---------------------------------------------------------------------------

fn extract_const_strings(str_params: &HashMap<i32, HashMap<i32, Vec<u8>>>, nid: i32) -> Vec<Vec<u8>> {
    let mut consts = Vec::new();
    if let Some(node_str) = str_params.get(&nid) {
        let mut sidx = 0;
        while node_str.contains_key(&(PARAM_CONST_STR_BASE + sidx)) {
            consts.push(node_str[&(PARAM_CONST_STR_BASE + sidx)].clone());
            sidx += 1;
        }
    }
    consts
}

fn create_expr_predicate(
    code: Vec<i64>,
    num_regs: i64,
    result_reg: i64,
    const_strings: Vec<Vec<u8>>,
    _owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let prog = ExprProgram::new(code, num_regs as u32, result_reg as u32, const_strings);
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_predicate(prog)));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

fn create_expr_map(
    code: Vec<i64>,
    num_regs: i64,
    const_strings: Vec<Vec<u8>>,
    pk_index: u32,
    _owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let prog = ExprProgram::new(code, num_regs as u32, 0, const_strings);
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_map(prog, pk_index)));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

fn create_universal_projection(
    src_indices: &[i32],
    src_types: &[u8],
    pk_index: u32,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let indices: Vec<u32> = src_indices.iter().map(|&i| i as u32).collect();
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_projection(
        &indices, src_types, pk_index,
    )));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

fn null_func_ptr() -> *const ScalarFuncKind {
    std::ptr::null()
}

/// Create a child table in a subdirectory of the view's directory.
fn create_child_table(
    view_dir: &str,
    child_name: &str,
    schema: SchemaDescriptor,
    table_id: u32,
) -> Result<Table, i32> {
    let child_dir = format!("{}/scratch_{}", view_dir, child_name);
    Table::new(&child_dir, child_name, schema, table_id, 256 * 1024, false)
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

struct EmitState {
    next_extra_reg: i32,
    sink_reg_id: i32,
    input_delta_reg_id: i32,
}

/// Emit instructions for a single circuit node.
/// Returns true if this is an EXCHANGE_SHARD node (special handling needed).
#[allow(clippy::too_many_arguments)]
fn emit_node(
    graph: &CircuitGraph,
    ann: &Annotation,
    rw: &Rewrites,
    nid: i32,
    op: i32,
    reg_id: i32,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_schemas: &mut Vec<SchemaDescriptor>,
    reg_kinds: &mut Vec<u8>,
    // Owned resources
    owned_tables: &mut Vec<Box<Table>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
    owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    // External tables
    ext_tables: &[ExternalTable],
    ext_trace_regs: &mut Vec<(u16, i64)>,
    source_reg_map: &mut HashMap<i64, i32>,
    // View info
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let node_params = graph.params.get(&nid).cloned().unwrap_or_default();
    let in_regs = compute_in_regs(graph, nid, out_reg_of);

    match op {
        OPCODE_SCAN_TRACE => {
            let table_id = graph.sources.get(&nid).copied().unwrap_or(0);
            let chunk_limit = node_params.get(&PARAM_CHUNK_LIMIT).copied().unwrap_or(0) as i32;

            if table_id == 0 {
                let join_source_table = node_params.get(&PARAM_JOIN_SOURCE_TABLE).copied().unwrap_or(0);
                if join_source_table > 0 {
                    if let Some(ext) = ext_tables.iter().find(|t| t.table_id == join_source_table) {
                        reg_schemas[reg_id as usize] = ext.schema;
                        reg_kinds[reg_id as usize] = 0;
                        source_reg_map.insert(join_source_table, reg_id);
                    } else {
                    }
                    return;
                }
                // Primary input
                reg_schemas[reg_id as usize] = ann.in_schema;
                reg_kinds[reg_id as usize] = 0;
                state.input_delta_reg_id = reg_id;
                return;
            } else if ann.trace_side_sources.contains(&nid) {
                // Trace side (join probe)
                if let Some(ext) = ext_tables.iter().find(|t| t.table_id == table_id) {
                    reg_schemas[reg_id as usize] = ext.schema;
                    reg_kinds[reg_id as usize] = 1;
                    ext_trace_regs.push((reg_id as u16, table_id));
                }
                return;
            } else {
                // Non-trace SCAN_TRACE (e.g., view reading from a base table)
                if let Some(ext) = ext_tables.iter().find(|t| t.table_id == table_id) {
                    reg_schemas[reg_id as usize] = ext.schema;
                    reg_kinds[reg_id as usize] = 1;
                    ext_trace_regs.push((reg_id as u16, table_id));
                    let out_delta_id = state.next_extra_reg;
                    state.next_extra_reg += 1;
                    reg_schemas[out_delta_id as usize] = ext.schema;
                    reg_kinds[out_delta_id as usize] = 0;
                    out_reg_of.insert(nid, out_delta_id);
                    builder.add_scan_trace(reg_id as u16, out_delta_id as u16, chunk_limit);
                }
                return;
            }
        }

        OPCODE_FILTER => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_schemas[in_reg as usize];
            reg_schemas[reg_id as usize] = in_schema;
            reg_kinds[reg_id as usize] = 0;

            let num_regs = node_params.get(&PARAM_EXPR_NUM_REGS).copied().unwrap_or(0);
            let func_ptr = if num_regs > 0 {
                let result_reg = node_params.get(&PARAM_EXPR_RESULT_REG).copied().unwrap_or(0);
                let code = extract_map_code(&node_params);
                let const_strings = extract_const_strings(&graph.str_params, nid);
                create_expr_predicate(code, num_regs, result_reg, const_strings, owned_expr_progs, owned_funcs)
            } else {
                null_func_ptr()
            };
            builder.add_filter(in_reg as u16, reg_id as u16, func_ptr);
        }

        OPCODE_MAP => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            let reindex_col_check = node_params.get(&PARAM_REINDEX_COL).copied().unwrap_or(-1);
            let num_regs = node_params.get(&PARAM_EXPR_NUM_REGS).copied().unwrap_or(0);
            let has_expr_code = num_regs > 0 || node_params.contains_key(&PARAM_EXPR_BASE);

            // Identity MAP optimization: sequential COPY_COL with matching schemas
            if reindex_col_check < 0 && has_expr_code {
                if schemas_physically_identical(&in_reg_schema, &graph.out_schema) {
                    let code = extract_map_code(&node_params);
                    if all_copy_col_sequential(&code) {
                        out_reg_of.insert(nid, in_reg);
                        return;
                    }
                }
            }

            // Folded MAP (into upstream REDUCE)
            if let Some(&reduce_nid) = rw.folded_maps.get(&nid) {
                out_reg_of.insert(nid, *out_reg_of.get(&reduce_nid).unwrap_or(&0));
                return;
            }

            let (func_ptr, node_schema) = if has_expr_code {
                let code = extract_map_code(&node_params);
                let const_strings = extract_const_strings(&graph.str_params, nid);
                let fp = create_expr_map(code, num_regs, const_strings, in_reg_schema.pk_index, owned_expr_progs, owned_funcs);
                let schema = if reindex_col_check >= 0 {
                    // Reindex MAP: output has synthetic PK + all input cols
                    let mut s = empty_schema();
                    s.columns[0] = col(type_code::U128, 16, false); // __pk
                    for i in 0..in_reg_schema.num_columns as usize {
                        s.columns[i + 1] = in_reg_schema.columns[i];
                    }
                    s.num_columns = in_reg_schema.num_columns + 1;
                    s.pk_index = 0;
                    s
                } else {
                    graph.out_schema
                };
                (fp, schema)
            } else if node_params.contains_key(&PARAM_PROJ_BASE) {
                let mut src_indices = Vec::new();
                let mut src_types = Vec::new();
                let mut idx = 0;
                while node_params.contains_key(&(PARAM_PROJ_BASE + idx)) {
                    let src_col = node_params[&(PARAM_PROJ_BASE + idx)] as i32;
                    src_indices.push(src_col);
                    src_types.push(in_reg_schema.columns[src_col as usize].type_code);
                    idx += 1;
                }
                let fp = create_universal_projection(&src_indices, &src_types, in_reg_schema.pk_index, owned_funcs);
                let schema = build_map_output_schema(&in_reg_schema, &src_indices);
                (fp, schema)
            } else if node_params.get(&PARAM_KEY_ONLY).copied().unwrap_or(0) != 0 {
                // Key-only projection: strip all payload, keep only PK.
                let fp = create_universal_projection(&[], &[], in_reg_schema.pk_index, owned_funcs);
                let mut s = empty_schema();
                s.columns[0] = in_reg_schema.columns[in_reg_schema.pk_index as usize];
                s.num_columns = 1;
                s.pk_index = 0;
                (fp, s)
            } else {
                (null_func_ptr(), in_reg_schema)
            };

            let reindex_col = node_params.get(&PARAM_REINDEX_COL).copied().unwrap_or(-1) as i32;
            reg_schemas[reg_id as usize] = node_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_map(in_reg as u16, reg_id as u16, func_ptr, node_schema, reindex_col);
        }

        OPCODE_NEGATE => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_negate(in_reg as u16, reg_id as u16);
        }

        OPCODE_UNION => {
            let in_a = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_a as usize];
            reg_kinds[reg_id as usize] = 0;
            let has_b = in_regs.contains_key(&PORT_IN_B);
            let in_b = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            builder.add_union(in_a as u16, in_b as u16, has_b, reg_id as u16);
        }

        OPCODE_JOIN_DELTA_TRACE => {
            let delta_reg = in_regs.get(&PORT_DELTA).copied().unwrap_or(0);
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let delta_schema = reg_schemas[delta_reg as usize];
            let trace_schema = reg_schemas[trace_reg as usize];
            let join_schema = merge_schemas_for_join(&delta_schema, &trace_schema);
            reg_schemas[reg_id as usize] = join_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_join_dt(delta_reg as u16, trace_reg as u16, reg_id as u16, trace_schema);
        }

        OPCODE_JOIN_DELTA_TRACE_OUTER => {
            let delta_reg = in_regs.get(&PORT_DELTA).copied().unwrap_or(0);
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let delta_schema = reg_schemas[delta_reg as usize];
            let trace_schema = reg_schemas[trace_reg as usize];
            let outer_schema = merge_schemas_for_join_outer(&delta_schema, &trace_schema);
            reg_schemas[reg_id as usize] = outer_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_join_dt_outer(delta_reg as u16, trace_reg as u16, reg_id as u16, trace_schema);
        }

        OPCODE_NULL_EXTEND => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_schemas[in_reg as usize];
            let n_cols = node_params.get(&PARAM_NULL_EXTEND_COUNT).copied().unwrap_or(0) as usize;
            // Build right schema from type codes stored in params
            let mut right = empty_schema();
            right.columns[0] = col(type_code::U128, 16, false); // dummy PK
            for i in 0..n_cols {
                let tc = node_params.get(&(PARAM_NULL_EXTEND_COL_BASE + i as i32)).copied().unwrap_or(type_code::I64 as i64) as u8;
                let sz = crate::schema::type_size(tc);
                right.columns[i + 1] = col(tc, sz, true);
            }
            right.num_columns = (n_cols + 1) as u32;
            right.pk_index = 0;

            let out_schema = merge_schemas_for_join_outer(&in_schema, &right);
            reg_schemas[reg_id as usize] = out_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_null_extend(in_reg as u16, reg_id as u16, right);
        }

        OPCODE_JOIN_DELTA_DELTA => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            let a_schema = reg_schemas[a_reg as usize];
            let b_schema = reg_schemas[b_reg as usize];
            let join_schema = merge_schemas_for_join(&a_schema, &b_schema);
            reg_schemas[reg_id as usize] = join_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_join_dd(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
        }

        OPCODE_DISTINCT => {
            let in_reg = in_regs.get(&PORT_IN_DISTINCT).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];

            if rw.skip_nodes.contains(&nid) {
                out_reg_of.insert(nid, in_reg);
                return;
            }

            // Create history table as child of view dir
            let child_name = format!("_hist_{}_{}", view_id, nid);
            let hist_table = match create_child_table(view_dir, &child_name, in_reg_schema, view_table_id) {
                Ok(t) => t,
                Err(_) => return,
            };
            let table_idx = owned_tables.len();
            owned_tables.push(Box::new(hist_table));
            let hist_table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;

            reg_schemas[reg_id as usize] = in_reg_schema;
            reg_kinds[reg_id as usize] = 1;
            owned_trace_regs.push((reg_id as u16, table_idx));

            let out_delta_id = state.next_extra_reg;
            state.next_extra_reg += 1;
            reg_schemas[out_delta_id as usize] = in_reg_schema;
            reg_kinds[out_delta_id as usize] = 0;
            out_reg_of.insert(nid, out_delta_id);

            builder.add_distinct(in_reg as u16, reg_id as u16, out_delta_id as u16, hist_table_ptr);
        }

        OPCODE_REDUCE => {
            emit_reduce(
                graph, ann, rw, nid, reg_id, &node_params, &in_regs,
                builder, state, out_reg_of, reg_schemas, reg_kinds,
                owned_tables, owned_funcs, owned_expr_progs, owned_trace_regs,
                view_dir, view_table_id, view_id,
            );
        }

        OPCODE_INTEGRATE => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            let int_table_id = node_params.get(&PARAM_TABLE_ID).copied().unwrap_or(0);

            if int_table_id > 0 && int_table_id != view_id as i64 {
                let child_name = format!("_int_{}_{}", view_id, nid);
                if let Ok(t) = create_child_table(view_dir, &child_name, in_reg_schema, view_table_id) {
                    let table_idx = owned_tables.len();
                    owned_tables.push(Box::new(t));
                    let table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;
                    reg_schemas[reg_id as usize] = in_reg_schema;
                    reg_kinds[reg_id as usize] = 1;
                    owned_trace_regs.push((reg_id as u16, table_idx));
                    emit_simple_integrate(builder, in_reg as u16, table_ptr);
                }
                return;
            }

            state.sink_reg_id = in_reg;
            emit_simple_integrate(builder, in_reg as u16, std::ptr::null_mut());
        }

        OPCODE_DELAY => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let state_reg = state.next_extra_reg;
            state.next_extra_reg += 1;
            let in_schema = reg_schemas[in_reg as usize];
            reg_schemas[state_reg as usize] = in_schema;
            reg_kinds[state_reg as usize] = 2;
            reg_schemas[reg_id as usize] = in_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_delay(in_reg as u16, state_reg as u16, reg_id as u16);
        }

        OPCODE_ANTI_JOIN_DELTA_TRACE => {
            let delta_reg = in_regs.get(&PORT_DELTA).copied().unwrap_or(0);
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[delta_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_anti_join_dt(delta_reg as u16, trace_reg as u16, reg_id as u16);
        }

        OPCODE_ANTI_JOIN_DELTA_DELTA => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[a_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_anti_join_dd(a_reg as u16, b_reg as u16, reg_id as u16);
        }

        OPCODE_SEMI_JOIN_DELTA_TRACE => {
            let delta_reg = in_regs.get(&PORT_DELTA).copied().unwrap_or(0);
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[delta_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_semi_join_dt(delta_reg as u16, trace_reg as u16, reg_id as u16);
        }

        OPCODE_SEMI_JOIN_DELTA_DELTA => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[a_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_semi_join_dd(a_reg as u16, b_reg as u16, reg_id as u16);
        }

        OPCODE_SEEK_TRACE => {
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let key_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            builder.add_seek_trace(trace_reg as u16, key_reg as u16);
        }

        OPCODE_CLEAR_DELTAS => {
            builder.add_clear_deltas();
        }

        OPCODE_GATHER_REDUCE => {
            emit_gather_reduce(
                graph, nid, reg_id, &node_params, &in_regs,
                builder, state, out_reg_of, reg_schemas, reg_kinds,
                owned_tables, owned_trace_regs,
                view_dir, view_table_id, view_id,
            );
        }

        _ => {
            // Unknown opcode: pass-through
            if let Some(&in_reg) = in_regs.get(&PORT_IN) {
                reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
                reg_kinds[reg_id as usize] = reg_kinds[in_reg as usize];
            }
        }
    }
}

fn compute_in_regs(graph: &CircuitGraph, nid: i32, out_reg_of: &HashMap<i32, i32>) -> HashMap<i32, i32> {
    let mut in_regs = HashMap::new();
    for e in &graph.edges {
        if e.dst == nid {
            if let Some(&reg) = out_reg_of.get(&e.src) {
                in_regs.insert(e.port, reg);
            }
        }
    }
    in_regs
}

fn emit_simple_integrate(builder: &mut ProgramBuilder, in_reg: u16, table_ptr: *mut Table) {
    builder.add_integrate(
        in_reg,
        table_ptr,
        std::ptr::null_mut(), 0, 0, // no GI
        std::ptr::null_mut(), false, 0, &[], std::ptr::null(), 0, // no AVI
    );
}

// ---------------------------------------------------------------------------
// REDUCE emission (complex enough to warrant its own function)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn emit_reduce(
    graph: &CircuitGraph,
    _ann: &Annotation,
    rw: &Rewrites,
    nid: i32,
    reg_id: i32,
    node_params: &HashMap<i32, i64>,
    in_regs: &HashMap<i32, i32>,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_schemas: &mut Vec<SchemaDescriptor>,
    reg_kinds: &mut Vec<u8>,
    owned_tables: &mut Vec<Box<Table>>,
    _owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
    owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let in_reg_id = in_regs.get(&PORT_IN_REDUCE).copied().unwrap_or(0);
    let in_reg_schema = reg_schemas[in_reg_id as usize];

    // Parse aggregate descriptors
    let agg_count = node_params.get(&PARAM_AGG_COUNT).copied().unwrap_or(0);
    let mut agg_descs: Vec<AggDescriptor> = Vec::new();
    let mut agg_func_id: u8 = 0;
    let mut agg_col_idx: u32 = 0;

    if agg_count > 0 {
        for ai in 0..agg_count as i32 {
            let packed = node_params.get(&(PARAM_AGG_SPEC_BASE + ai)).copied().unwrap_or(0);
            let func_id = (packed >> 32) as u8;
            let col_idx = (packed & 0xFFFFFFFF) as u32;
            let col_type_code = in_reg_schema.columns[col_idx as usize].type_code;
            agg_descs.push(AggDescriptor { col_idx, agg_op: func_id, col_type_code, _pad: [0; 2] });
        }
        if agg_descs.len() == 1 {
            agg_func_id = agg_descs[0].agg_op;
            agg_col_idx = agg_descs[0].col_idx;
        }
    } else {
        agg_func_id = node_params.get(&PARAM_AGG_FUNC_ID).copied().unwrap_or(0) as u8;
        agg_col_idx = node_params.get(&PARAM_AGG_COL_IDX).copied().unwrap_or(0) as u32;
        if agg_func_id > 0 {
            let col_type_code = in_reg_schema.columns[agg_col_idx as usize].type_code;
            agg_descs.push(AggDescriptor { col_idx: agg_col_idx, agg_op: agg_func_id, col_type_code, _pad: [0; 2] });
        } else {
            // NullAggregate
            agg_descs.push(AggDescriptor { col_idx: 0, agg_op: 0, col_type_code: 0, _pad: [0; 2] });
        }
    }

    let gcols: Vec<i32> = graph.group_cols.get(&nid).cloned().unwrap_or_default();
    let gcols_u32: Vec<u32> = gcols.iter().map(|&c| c as u32).collect();

    let reduce_out_schema = build_reduce_output_schema(&in_reg_schema, &gcols, &agg_descs);

    // Create trace table (output history)
    let trace_table = match create_child_table(view_dir, &format!("_reduce_{}_{}", view_id, nid), reduce_out_schema, view_table_id) {
        Ok(t) => t,
        Err(_) => return,
    };
    let trace_table_idx = owned_tables.len();
    owned_tables.push(Box::new(trace_table));
    let trace_table_ptr = &*owned_tables[trace_table_idx] as *const Table as *mut Table;

    reg_schemas[reg_id as usize] = reduce_out_schema;
    reg_kinds[reg_id as usize] = 1;
    owned_trace_regs.push((reg_id as u16, trace_table_idx));

    let raw_delta_id = state.next_extra_reg;
    state.next_extra_reg += 1;
    reg_schemas[raw_delta_id as usize] = reduce_out_schema;
    reg_kinds[raw_delta_id as usize] = 0;

    // Check for folded finalize MAP
    let finalize_prog_idx = rw.fold_finalize.get(&nid).copied();
    let mut fin_delta_id: i32 = -1;
    if finalize_prog_idx.is_some() {
        fin_delta_id = state.next_extra_reg;
        state.next_extra_reg += 1;
        reg_schemas[fin_delta_id as usize] = graph.out_schema;
        reg_kinds[fin_delta_id as usize] = 0;
        out_reg_of.insert(nid, fin_delta_id);
    } else {
        out_reg_of.insert(nid, raw_delta_id);
    }

    // trace_in table (for non-linear aggregates)
    let all_linear = agg_descs.iter().all(|a| is_linear_agg(a.agg_op));
    let will_use_avi = agg_descs.len() == 1
        && (agg_func_id == AGG_MIN || agg_func_id == AGG_MAX)
        && agg_value_idx_eligible(in_reg_schema.columns[agg_col_idx as usize].type_code);

    let mut tr_in_reg_id: i32 = -1;
    let mut tr_in_table_ptr: *mut Table = std::ptr::null_mut();
    let mut tr_in_table_idx: Option<usize> = None;

    if let Some(&existing) = in_regs.get(&PORT_TRACE_IN) {
        tr_in_reg_id = existing;
    } else if !all_linear && !will_use_avi {
        let tr_in = match create_child_table(
            view_dir, &format!("_reduce_in_{}_{}", view_id, nid), in_reg_schema, view_table_id,
        ) {
            Ok(t) => t,
            Err(_) => return,
        };
        let idx = owned_tables.len();
        owned_tables.push(Box::new(tr_in));
        tr_in_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
        tr_in_table_idx = Some(idx);

        tr_in_reg_id = state.next_extra_reg;
        state.next_extra_reg += 1;
        reg_schemas[tr_in_reg_id as usize] = in_reg_schema;
        reg_kinds[tr_in_reg_id as usize] = 1;
        owned_trace_regs.push((tr_in_reg_id as u16, idx));
    }

    // GI table
    let mut gi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut gi_col_idx: u32 = 0;
    let mut gi_col_type_code: u8 = 0;

    if tr_in_table_idx.is_some() && gcols.len() == 1 {
        let gc_col_idx = gcols[0] as usize;
        let gc_type = in_reg_schema.columns[gc_col_idx].type_code;
        if gc_type != type_code::U128
            && gc_type != type_code::STRING
            && gc_type != type_code::F32
            && gc_type != type_code::F64
        {
            let gi_dir = format!(
                "{}/scratch__reduce_in_{}_{}/_gidx",
                view_dir, view_id, nid
            );
            if let Ok(gi_table) = Table::new(&gi_dir, "_gidx", make_group_idx_schema(), 0, 1024 * 1024, false) {
                let idx = owned_tables.len();
                owned_tables.push(Box::new(gi_table));
                gi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
                gi_col_idx = gc_col_idx as u32;
                gi_col_type_code = gc_type;
            }
        }
    }

    // AVI table
    let mut avi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut avi_for_max = false;
    let mut avi_agg_col_type_code: u8 = 0;
    let mut avi_group_cols: Vec<u32> = Vec::new();
    let mut avi_agg_col_idx: u32 = 0;

    if will_use_avi {
        avi_for_max = agg_func_id == AGG_MAX;
        avi_agg_col_type_code = in_reg_schema.columns[agg_col_idx as usize].type_code;
        avi_agg_col_idx = agg_col_idx;
        avi_group_cols = gcols_u32.clone();
        let avi_child = format!("_avidx_{}_{}", view_id, nid);
        if let Ok(av_table) = create_child_table(view_dir, &avi_child, make_agg_value_idx_schema(), view_table_id) {
            let idx = owned_tables.len();
            owned_tables.push(Box::new(av_table));
            avi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
        }
    }

    // AVI INTEGRATE must precede REDUCE
    if !avi_table_ptr.is_null() {
        builder.add_integrate(
            in_reg_id as u16,
            std::ptr::null_mut(), // no target table (AVI-only)
            std::ptr::null_mut(), 0, 0, // no GI
            avi_table_ptr, avi_for_max, avi_agg_col_type_code,
            &avi_group_cols, &in_reg_schema as *const SchemaDescriptor, avi_agg_col_idx,
        );
    }

    // Finalize program
    let fin_prog_ptr: *const ExprProgram = if let Some(idx) = finalize_prog_idx {
        &*owned_expr_progs[idx] as *const ExprProgram
    } else {
        std::ptr::null()
    };
    let fin_schema_ptr: *const SchemaDescriptor = if finalize_prog_idx.is_some() {
        &graph.out_schema as *const SchemaDescriptor
    } else {
        std::ptr::null()
    };

    // Emit REDUCE
    builder.add_reduce(
        in_reg_id as u16,
        tr_in_reg_id as i16,
        reg_id as u16,       // trace_out_reg
        raw_delta_id as u16, // out_reg
        fin_delta_id as i16,
        &agg_descs,
        &gcols_u32,
        reduce_out_schema,
        avi_table_ptr, avi_for_max, avi_agg_col_type_code,
        &avi_group_cols,
        if !avi_table_ptr.is_null() { &in_reg_schema as *const SchemaDescriptor } else { std::ptr::null() },
        avi_agg_col_idx,
        gi_table_ptr, gi_col_idx, gi_col_type_code,
        fin_prog_ptr,
        fin_schema_ptr,
    );

    // INTEGRATE input → trace_in (after REDUCE)
    if !tr_in_table_ptr.is_null() {
        builder.add_integrate(
            in_reg_id as u16,
            tr_in_table_ptr,
            gi_table_ptr, gi_col_idx, gi_col_type_code,
            avi_table_ptr, avi_for_max, avi_agg_col_type_code,
            &avi_group_cols,
            if !avi_table_ptr.is_null() { &in_reg_schema as *const SchemaDescriptor } else { std::ptr::null() },
            avi_agg_col_idx,
        );
    }

    // INTEGRATE raw_delta → trace_out (always, for trace_out history)
    emit_simple_integrate(builder, raw_delta_id as u16, trace_table_ptr);
}

// ---------------------------------------------------------------------------
// GATHER_REDUCE emission
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn emit_gather_reduce(
    _graph: &CircuitGraph,
    nid: i32,
    reg_id: i32,
    node_params: &HashMap<i32, i64>,
    in_regs: &HashMap<i32, i32>,
    builder: &mut ProgramBuilder,
    state: &mut EmitState,
    out_reg_of: &mut HashMap<i32, i32>,
    reg_schemas: &mut Vec<SchemaDescriptor>,
    reg_kinds: &mut Vec<u8>,
    owned_tables: &mut Vec<Box<Table>>,
    owned_trace_regs: &mut Vec<(u16, usize)>,
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
) {
    let in_reg_id = in_regs.get(&PORT_IN_REDUCE).copied().unwrap_or(0);
    let partial_schema = reg_schemas[in_reg_id as usize];
    let num_out_cols = partial_schema.num_columns as usize;

    let agg_count = node_params.get(&PARAM_AGG_COUNT).copied().unwrap_or(0);
    let mut agg_descs: Vec<AggDescriptor> = Vec::new();

    if agg_count > 0 {
        assert!(
            num_out_cols >= agg_count as usize,
            "GATHER_REDUCE node {}: agg_count ({}) exceeds partial schema column count ({})",
            nid,
            agg_count,
            num_out_cols
        );
        for ai in 0..agg_count as i32 {
            let packed = node_params.get(&(PARAM_AGG_SPEC_BASE + ai)).copied().unwrap_or(0);
            let func_id = (packed >> 32) as u8;
            let agg_col_in_partial = num_out_cols - agg_count as usize + ai as usize;
            let col_type = partial_schema.columns[agg_col_in_partial].type_code;
            agg_descs.push(AggDescriptor { col_idx: 0, agg_op: func_id, col_type_code: col_type, _pad: [0; 2] });
        }
    } else {
        let agg_func_id = node_params.get(&PARAM_AGG_FUNC_ID).copied().unwrap_or(0) as u8;
        if agg_func_id > 0 {
            let agg_col_in_partial = num_out_cols - 1;
            let col_type = partial_schema.columns[agg_col_in_partial].type_code;
            agg_descs.push(AggDescriptor { col_idx: 0, agg_op: agg_func_id, col_type_code: col_type, _pad: [0; 2] });
        } else {
            agg_descs.push(AggDescriptor { col_idx: 0, agg_op: 0, col_type_code: 0, _pad: [0; 2] });
        }
    }

    let trace_table = match create_child_table(
        view_dir, &format!("_gather_{}_{}", view_id, nid), partial_schema, view_table_id,
    ) {
        Ok(t) => t,
        Err(_) => return,
    };
    let table_idx = owned_tables.len();
    owned_tables.push(Box::new(trace_table));
    let trace_table_ptr = &*owned_tables[table_idx] as *const Table as *mut Table;

    reg_schemas[reg_id as usize] = partial_schema;
    reg_kinds[reg_id as usize] = 1;
    owned_trace_regs.push((reg_id as u16, table_idx));

    let raw_delta_id = state.next_extra_reg;
    state.next_extra_reg += 1;
    reg_schemas[raw_delta_id as usize] = partial_schema;
    reg_kinds[raw_delta_id as usize] = 0;
    out_reg_of.insert(nid, raw_delta_id);

    builder.add_gather_reduce(in_reg_id as u16, reg_id as u16, raw_delta_id as u16, &agg_descs);
    emit_simple_integrate(builder, raw_delta_id as u16, trace_table_ptr);
}

// ---------------------------------------------------------------------------
// Build a single plan (pre or post exchange)
// ---------------------------------------------------------------------------

struct PlanBuildResult {
    vm: Box<VmHandle>,
    num_regs: u32,
    in_reg: i32,
    out_reg: i32,
    ext_trace_regs: Vec<(u16, i64)>,
    source_reg_map: HashMap<i64, i32>,
}

#[allow(clippy::too_many_arguments)]
fn build_plan(
    graph: &CircuitGraph,
    ann: &Annotation,
    rw: &Rewrites,
    ordered: &[i32],
    ext_tables: &[ExternalTable],
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
    _in_schema_override: &SchemaDescriptor,
    // For pre-exchange plans: the node whose output register is the plan output
    output_node_id: Option<i32>,
    // For post-exchange plans: (exchange_nid, exchange_schema) — allocates an
    // extra input register and maps the exchange node to it.
    exchange_input: Option<(i32, SchemaDescriptor)>,
    // Pre-built expression programs (from optimization passes like fold_reduce_map)
    pre_built_expr_progs: Vec<Box<ExprProgram>>,
) -> Option<PlanBuildResult> {
    // Register assignment
    let mut out_reg_of: HashMap<i32, i32> = HashMap::new();
    let mut next_reg: i32 = 0;
    for &nid in ordered {
        out_reg_of.insert(nid, next_reg);
        next_reg += 1;
    }

    let mut extra_regs = 0;
    for &nid in ordered {
        let op = graph.opcode_of.get(&nid).copied().unwrap_or(-1);
        if op == OPCODE_DISTINCT && !rw.skip_nodes.contains(&nid) {
            extra_regs += 1;
        } else if op == OPCODE_REDUCE {
            extra_regs += 2; // raw_delta + tr_in (may not be used, but safe)
            if rw.fold_finalize.contains_key(&nid) {
                extra_regs += 1;
            }
        } else if op == OPCODE_GATHER_REDUCE {
            extra_regs += 1;
        } else if op == OPCODE_SCAN_TRACE {
            let table_id = graph.sources.get(&nid).copied().unwrap_or(0);
            if table_id > 0 && !ann.trace_side_sources.contains(&nid) {
                extra_regs += 1;
            }
        } else if op == OPCODE_DELAY {
            extra_regs += 1;
        }
    }

    // For post-exchange plans: add an extra input register for the exchange output
    let mut exchange_input_reg_id: i32 = -1;
    if exchange_input.is_some() {
        exchange_input_reg_id = next_reg;
        next_reg += 1;
        extra_regs += 0; // already counted in next_reg
    }

    let num_regs = (next_reg + extra_regs) as usize;
    let mut reg_schemas = vec![empty_schema(); num_regs];
    let mut reg_kinds = vec![0u8; num_regs];

    // Map exchange node to the input register
    if let Some((ex_nid, ex_schema)) = exchange_input {
        out_reg_of.insert(ex_nid, exchange_input_reg_id);
        reg_schemas[exchange_input_reg_id as usize] = ex_schema;
        reg_kinds[exchange_input_reg_id as usize] = 0;
    }

    let mut owned_tables: Vec<Box<Table>> = Vec::new();
    let mut owned_funcs: Vec<Box<ScalarFuncKind>> = Vec::new();
    // Start with any expression programs from the optimization passes (fold_reduce_map)
    let mut owned_expr_progs: Vec<Box<ExprProgram>> = pre_built_expr_progs;
    let mut owned_trace_regs: Vec<(u16, usize)> = Vec::new();
    let mut ext_trace_regs: Vec<(u16, i64)> = Vec::new();
    let mut source_reg_map: HashMap<i64, i32> = HashMap::new();

    let mut builder = ProgramBuilder::new(num_regs as u16);
    let mut state = EmitState {
        next_extra_reg: next_reg,
        sink_reg_id: -1,
        input_delta_reg_id: if exchange_input_reg_id >= 0 { exchange_input_reg_id } else { -1 },
    };

    for &nid in ordered {
        let op = graph.opcode_of.get(&nid).copied().unwrap_or(-1);
        if op == OPCODE_EXCHANGE_SHARD {
            continue;
        }
        let reg_id = *out_reg_of.get(&nid).unwrap();

        emit_node(
            graph, ann, rw, nid, op, reg_id,
            &mut builder, &mut state, &mut out_reg_of,
            &mut reg_schemas, &mut reg_kinds,
            &mut owned_tables, &mut owned_funcs, &mut owned_expr_progs, &mut owned_trace_regs,
            ext_tables, &mut ext_trace_regs, &mut source_reg_map,
            view_dir, view_table_id, view_id,
        );
    }

    builder.add_halt();

    // Determine input reg
    let mut input_delta_reg_id = state.input_delta_reg_id;
    if input_delta_reg_id == -1 && !source_reg_map.is_empty() {
        input_delta_reg_id = *source_reg_map.values().next().unwrap();
    }

    // For pre-exchange plans: use the register assigned to the output node
    let mut sink_reg = state.sink_reg_id;
    if sink_reg == -1 {
        if let Some(out_nid) = output_node_id {
            if let Some(&reg) = out_reg_of.get(&out_nid) {
                sink_reg = reg;
            }
        }
    }

    if input_delta_reg_id == -1 {
        return None;
    }
    if sink_reg == -1 {
        return None;
    }

    // Schema mismatch validation (only for plans with explicit sink, not pre-exchange)
    if output_node_id.is_none() && sink_reg >= 0 {
        let sink_schema = &reg_schemas[sink_reg as usize];
        let out_schema = &graph.out_schema;
        if sink_schema.num_columns > 0 && sink_schema.num_columns != out_schema.num_columns {
            return None;
        }
    }

    let vm = builder.build_with_owned(
        &reg_schemas, &reg_kinds,
        owned_tables, owned_funcs, owned_expr_progs, owned_trace_regs,
    );

    Some(PlanBuildResult {
        vm,
        num_regs: num_regs as u32,
        in_reg: input_delta_reg_id,
        out_reg: sink_reg,
        ext_trace_regs,
        source_reg_map,
    })
}

// ---------------------------------------------------------------------------
// Top-level compile_view entry point
// ---------------------------------------------------------------------------

/// Compile a circuit for a single view.  Returns a rich `CompileOutput`
/// that DagEngine can consume directly, or that the FFI path can convert
/// to a flat buffer via `to_result_buffer()`.
///
/// # Safety
/// All table handles must be valid pointers or null.
pub unsafe fn compile_view(
    view_id: u64,
    // System table handles (NULL if absent)
    sys_nodes: *mut Table,
    sys_edges: *mut Table,
    sys_sources: *mut Table,
    sys_params: *mut Table,
    sys_gcols: *mut Table,
    sys_dep: *mut Table,
    // System table schemas (for column reading)
    sys_nodes_schema: &SchemaDescriptor,
    sys_edges_schema: &SchemaDescriptor,
    sys_sources_schema: &SchemaDescriptor,
    sys_params_schema: &SchemaDescriptor,
    sys_gcols_schema: &SchemaDescriptor,
    sys_dep_schema: &SchemaDescriptor,
    // View family directory (for creating child tables) and schema
    view_dir: &str,
    view_table_id: u32,
    view_schema: &SchemaDescriptor,
    // External table registry
    ext_tables: &[ExternalTable],
) -> Result<CompileOutput, i32> {
    // 1. Load circuit data from system tables
    let nodes = load_nodes(sys_nodes, view_id, sys_nodes_schema);
    if nodes.is_empty() {
        return Err(-1);
    }
    let edges = load_edges(sys_edges, view_id, sys_edges_schema);
    let sources = load_sources(sys_sources, view_id, sys_sources_schema);
    let (params, str_params) = load_params(sys_params, view_id, sys_params_schema);
    let group_cols = load_group_cols(sys_gcols, view_id, sys_gcols_schema);

    // 2. Build circuit graph + topological sort
    let mut graph = CircuitGraph {
        view_id,
        nodes,
        edges,
        sources,
        params,
        str_params,
        group_cols,
        out_schema: *view_schema,
        opcode_of: HashMap::new(),
        ordered: Vec::new(),
        outgoing: HashMap::new(),
        incoming: HashMap::new(),
        consumers: HashMap::new(),
    };
    if topo_sort(&mut graph).is_err() {
        return Err(-2); // cycle
    }
    // 3. Annotate
    let ann = annotate(&graph, sys_dep, sys_dep_schema, ext_tables);

    // 4. Optimization passes
    let mut owned_expr_progs_for_rw: Vec<Box<ExprProgram>> = Vec::new();
    let mut rw = Rewrites {
        skip_nodes: HashSet::new(),
        fold_finalize: HashMap::new(),
        folded_maps: HashMap::new(),
    };
    opt_distinct(&graph, &ann, &mut rw);
    opt_fold_reduce_map(&graph, &ann, &mut rw, &mut owned_expr_progs_for_rw);

    // 5. Check for EXCHANGE_SHARD
    let mut exchange_nid: Option<i32> = None;
    for &nid in &graph.ordered {
        let op = graph.opcode_of.get(&nid).copied().unwrap_or(-1);
        if op == OPCODE_EXCHANGE_SHARD {
            exchange_nid = Some(nid);
            break;
        }
    }

    if let Some(ex_nid) = exchange_nid {
        // Split into pre + post plans
        let mut pre_ordered = Vec::new();
        let mut post_ordered = Vec::new();
        let mut found_exchange = false;
        for &nid in &graph.ordered {
            if nid == ex_nid {
                found_exchange = true;
                continue;
            }
            if found_exchange {
                post_ordered.push(nid);
            } else {
                pre_ordered.push(nid);
            }
        }

        // Extract shard columns
        let ex_params = graph.params.get(&ex_nid).cloned().unwrap_or_default();
        let mut shard_cols = Vec::new();
        let mut idx = 0;
        while ex_params.contains_key(&(PARAM_SHARD_COL_BASE + idx)) {
            shard_cols.push(ex_params[&(PARAM_SHARD_COL_BASE + idx)] as i32);
            idx += 1;
        }

        // Find the exchange input: the node feeding PORT_EXCHANGE_IN of ex_nid
        let mut exchange_input_nid: i32 = -1;
        for e in &graph.edges {
            if e.dst == ex_nid && e.port == PORT_EXCHANGE_IN {
                exchange_input_nid = e.src;
                break;
            }
        }

        // Split fold programs between pre and post plans so each half gets
        // only the ExprPrograms for REDUCE nodes in its own ordered slice.
        let pre_nids: HashSet<i32> = pre_ordered.iter().copied().collect();
        let (rw_pre, pre_progs, rw_post, post_progs) =
            split_fold_programs(rw, owned_expr_progs_for_rw, &pre_nids);

        // Build pre-plan (output is the exchange input node's register)
        let pre = build_plan(
            &graph, &ann, &rw_pre, &pre_ordered, ext_tables,
            view_dir, view_table_id, view_id, &ann.in_schema,
            if exchange_input_nid >= 0 { Some(exchange_input_nid) } else { None },
            None,
            pre_progs,
        ).ok_or(-3)?;

        // Exchange schema = pre-plan output schema
        if pre.out_reg < 0 || pre.out_reg as usize >= pre.vm.program.reg_meta.len() {
            return Err(-3);
        }
        let exchange_schema = pre.vm.program.reg_meta[pre.out_reg as usize].schema;

        // Build post-plan
        let post = build_plan(
            &graph, &ann, &rw_post, &post_ordered, ext_tables,
            view_dir, view_table_id, view_id, &exchange_schema,
            None,
            Some((ex_nid, exchange_schema)),
            post_progs,
        ).ok_or(-4)?;

        // Determine skip_exchange
        let is_trivial = pre.in_reg == pre.out_reg
            || graph.incoming.get(&ex_nid).map(|v| v.len()).unwrap_or(0) == 1;
        let skip_exchange = is_trivial
            && shard_cols.len() == 1
            && shard_cols[0] == ann.in_schema.pk_index as i32;

        let source_reg_map = pre.source_reg_map.iter()
            .map(|(&tid, &reg)| (tid, reg as u16)).collect();

        Ok(CompileOutput {
            pre_vm: pre.vm,
            post_vm: Some(post.vm),
            out_schema: graph.out_schema,
            exchange_in_schema: Some(exchange_schema),
            pre_num_regs: pre.num_regs,
            pre_in_reg: pre.in_reg as u16,
            pre_out_reg: pre.out_reg as u16,
            post_num_regs: post.num_regs,
            post_in_reg: post.in_reg as u16,
            post_out_reg: post.out_reg as u16,
            skip_exchange,
            shard_cols,
            source_reg_map,
            join_shard_map: ann.join_shard_map,
            co_partitioned: ann.co_partitioned,
            pre_ext_trace_regs: pre.ext_trace_regs,
            post_ext_trace_regs: post.ext_trace_regs,
        })
    } else {
        // Single plan (no exchange)
        let plan = build_plan(
            &graph, &ann, &rw, &graph.ordered.clone(), ext_tables,
            view_dir, view_table_id, view_id, &ann.in_schema,
            None, None,
            owned_expr_progs_for_rw,
        ).ok_or(-5)?;

        let source_reg_map = plan.source_reg_map.iter()
            .map(|(&tid, &reg)| (tid, reg as u16)).collect();

        Ok(CompileOutput {
            pre_vm: plan.vm,
            post_vm: None,
            out_schema: graph.out_schema,
            exchange_in_schema: None,
            pre_num_regs: plan.num_regs,
            pre_in_reg: plan.in_reg as u16,
            pre_out_reg: plan.out_reg as u16,
            post_num_regs: 0,
            post_in_reg: 0,
            post_out_reg: 0,
            skip_exchange: false,
            shard_cols: Vec::new(),
            source_reg_map,
            join_shard_map: ann.join_shard_map,
            co_partitioned: ann.co_partitioned,
            pre_ext_trace_regs: plan.ext_trace_regs,
            post_ext_trace_regs: Vec::new(),
        })
    }
}

fn populate_ext_trace_regs(result: &mut CompileResult, pre: &[(u16, i64)], post: &[(u16, i64)]) {
    let mut i = 0;
    for &(reg_id, tid) in pre.iter().chain(post.iter()) {
        if i < 63 {
            result.buf[CR_ETR_REG_IDS + i] = reg_id as i64;
            result.buf[CR_ETR_TABLE_IDS + i] = tid;
            i += 1;
        }
    }
    result.buf[CR_NUM_ETR] = i as i64;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topo_sort_simple() {
        let mut graph = CircuitGraph {
            view_id: 1,
            nodes: vec![
                Node { id: 0, opcode: OPCODE_SCAN_TRACE },
                Node { id: 1, opcode: OPCODE_FILTER },
                Node { id: 2, opcode: OPCODE_INTEGRATE },
            ],
            edges: vec![
                Edge { _id: 0, src: 0, dst: 1, port: 0 },
                Edge { _id: 1, src: 1, dst: 2, port: 0 },
            ],
            sources: HashMap::new(),
            params: HashMap::new(),
            str_params: HashMap::new(),
            group_cols: HashMap::new(),
            out_schema: empty_schema(),
            opcode_of: HashMap::new(),
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
        };
        assert!(topo_sort(&mut graph).is_ok());
        assert_eq!(graph.ordered, vec![0, 1, 2]);
    }

    #[test]
    fn test_topo_sort_cycle() {
        let mut graph = CircuitGraph {
            view_id: 1,
            nodes: vec![
                Node { id: 0, opcode: OPCODE_FILTER },
                Node { id: 1, opcode: OPCODE_FILTER },
            ],
            edges: vec![
                Edge { _id: 0, src: 0, dst: 1, port: 0 },
                Edge { _id: 1, src: 1, dst: 0, port: 0 },
            ],
            sources: HashMap::new(),
            params: HashMap::new(),
            str_params: HashMap::new(),
            group_cols: HashMap::new(),
            out_schema: empty_schema(),
            opcode_of: HashMap::new(),
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
        };
        assert!(topo_sort(&mut graph).is_err());
    }

    #[test]
    fn test_merge_schemas_for_join() {
        let mut left = empty_schema();
        left.columns[0] = col(type_code::U128, 16, false);
        left.columns[1] = col(type_code::I64, 8, false);
        left.num_columns = 2;
        left.pk_index = 0;

        let mut right = empty_schema();
        right.columns[0] = col(type_code::U128, 16, false);
        right.columns[1] = col(type_code::STRING, 16, false);
        right.num_columns = 2;
        right.pk_index = 0;

        let joined = merge_schemas_for_join(&left, &right);
        assert_eq!(joined.num_columns, 3); // PK + left_I64 + right_STRING
        assert_eq!(joined.columns[0].type_code, type_code::U128);
        assert_eq!(joined.columns[1].type_code, type_code::I64);
        assert_eq!(joined.columns[2].type_code, type_code::STRING);
    }

    #[test]
    fn test_merge_schemas_for_join_outer() {
        let mut left = empty_schema();
        left.columns[0] = col(type_code::U128, 16, false);
        left.columns[1] = col(type_code::I64, 8, false);
        left.num_columns = 2;
        left.pk_index = 0;

        let mut right = empty_schema();
        right.columns[0] = col(type_code::U128, 16, false);
        right.columns[1] = col(type_code::I64, 8, false);
        right.num_columns = 2;
        right.pk_index = 0;

        let joined = merge_schemas_for_join_outer(&left, &right);
        assert_eq!(joined.num_columns, 3);
        assert_eq!(joined.columns[2].nullable, 1); // right side nullable
    }

    #[test]
    fn test_agg_output_type() {
        assert_eq!(agg_output_type(AGG_COUNT, type_code::I64), (type_code::I64, 8));
        assert_eq!(agg_output_type(AGG_SUM, type_code::F64), (type_code::F64, 8));
        assert_eq!(agg_output_type(AGG_SUM, type_code::I32), (type_code::I64, 8));
        assert_eq!(agg_output_type(AGG_MIN, type_code::I32), (type_code::I64, 8));
        assert_eq!(agg_output_type(AGG_MAX, type_code::F32), (type_code::F64, 8));
        assert_eq!(agg_output_type(AGG_MAX, type_code::STRING), (type_code::I64, 8));
    }

    #[test]
    fn test_all_copy_col_sequential() {
        // COPY_COL has opcode 34
        assert!(all_copy_col_sequential(&[34, 9, 1, 0, 34, 9, 2, 1]));
        assert!(!all_copy_col_sequential(&[34, 9, 2, 0, 34, 9, 1, 1])); // wrong order
        assert!(!all_copy_col_sequential(&[34, 9, 1, 0, 35, 9, 2, 1])); // wrong opcode
        assert!(!all_copy_col_sequential(&[]));
        assert!(!all_copy_col_sequential(&[34, 9, 1])); // not multiple of 4
    }

    #[test]
    fn test_identity_map_detection() {
        let mut a = empty_schema();
        a.columns[0] = col(type_code::U128, 16, false);
        a.columns[1] = col(type_code::I64, 8, false);
        a.num_columns = 2;
        a.pk_index = 0;

        let mut b = empty_schema();
        b.columns[0] = col(type_code::U128, 16, false);
        b.columns[1] = col(type_code::I64, 8, false);
        b.num_columns = 2;
        b.pk_index = 0;

        assert!(schemas_physically_identical(&a, &b));

        b.columns[1].type_code = type_code::STRING;
        assert!(!schemas_physically_identical(&a, &b));
    }

    #[test]
    fn test_build_reduce_output_schema_natural_pk() {
        let mut input = empty_schema();
        input.columns[0] = col(type_code::U128, 16, false);
        input.columns[1] = col(type_code::U64, 8, false); // group col
        input.columns[2] = col(type_code::I64, 8, false); // agg col
        input.num_columns = 3;
        input.pk_index = 0;

        let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AGG_SUM, col_type_code: type_code::I64, _pad: [0; 2] }];
        let out = build_reduce_output_schema(&input, &[1], &aggs);
        // Natural PK (single U64 group col) → [U64_PK, I64_agg]
        assert_eq!(out.num_columns, 2);
        assert_eq!(out.columns[0].type_code, type_code::U64);
        assert_eq!(out.columns[1].type_code, type_code::I64);
    }

    #[test]
    fn test_build_reduce_output_schema_synthetic_pk() {
        let mut input = empty_schema();
        input.columns[0] = col(type_code::U128, 16, false);
        input.columns[1] = col(type_code::STRING, 16, false); // group col
        input.columns[2] = col(type_code::I64, 8, false);     // agg col
        input.num_columns = 3;
        input.pk_index = 0;

        let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AGG_COUNT, col_type_code: type_code::I64, _pad: [0; 2] }];
        let out = build_reduce_output_schema(&input, &[1], &aggs);
        // Synthetic PK (STRING group col) → [U128_hash, STRING_group, I64_count]
        assert_eq!(out.num_columns, 3);
        assert_eq!(out.columns[0].type_code, type_code::U128);
        assert_eq!(out.columns[1].type_code, type_code::STRING);
        assert_eq!(out.columns[2].type_code, type_code::I64);
    }

    /// Verify that split_fold_programs correctly routes each ExprProgram to the
    /// plan half that owns its REDUCE node, and re-indexes fold_finalize to
    /// zero-based indices within each half.
    ///
    /// Graph: SCAN(0) → REDUCE(1) → MAP(2) → EXCHANGE_SHARD(3) → GATHER_REDUCE(4)
    /// REDUCE(1) is in pre_ordered; MAP(2) is folded into REDUCE(1).
    /// After split: pre_progs has 1 program (for REDUCE 1), post_progs is empty.
    #[test]
    fn test_split_fold_programs_routes_to_pre() {
        let code = vec![0i64; 4]; // minimal non-identity code
        let prog = ExprProgram::new(code, 1, 0, Vec::new());
        let progs: Vec<Box<ExprProgram>> = vec![Box::new(prog)];

        let mut fold_finalize = HashMap::new();
        fold_finalize.insert(1i32, 0usize); // REDUCE node 1 → program index 0

        let mut folded_maps = HashMap::new();
        folded_maps.insert(2i32, 1i32); // MAP node 2 folded into REDUCE node 1

        let rw = Rewrites {
            skip_nodes: HashSet::new(),
            fold_finalize,
            folded_maps,
        };

        // pre_ordered contains only the REDUCE node (1)
        let mut pre_nids = HashSet::new();
        pre_nids.insert(1i32);

        let (rw_pre, pre_progs, rw_post, post_progs) =
            split_fold_programs(rw, progs, &pre_nids);

        // The program must land in pre_progs with re-indexed entry 0
        assert_eq!(pre_progs.len(), 1);
        assert_eq!(post_progs.len(), 0);
        assert_eq!(rw_pre.fold_finalize.get(&1), Some(&0usize));
        assert!(!rw_post.fold_finalize.contains_key(&1));

        // folded_maps and skip_nodes are cloned to both halves
        assert!(rw_pre.folded_maps.contains_key(&2));
        assert!(rw_post.folded_maps.contains_key(&2));
    }
}
