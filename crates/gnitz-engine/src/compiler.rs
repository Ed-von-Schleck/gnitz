//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! runs annotation + optimization passes, and emits VM instructions.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::expr::{ExprProgram, Plan, ScalarFuncKind};
use crate::ops::{AggDescriptor, AggOp};
use crate::storage::{CursorHandle, Table, ReadCursor};
use crate::vm::{ProgramBuilder, VmHandle};

// Engine-only port aliases (all equal to wire constants).
const PORT_IN:    i32 = gnitz_wire::PORT_IN    as i32;
const PORT_IN_A:  i32 = gnitz_wire::PORT_IN_A  as i32;
const PORT_IN_B:  i32 = gnitz_wire::PORT_IN_B  as i32;
const PORT_TRACE: i32 = gnitz_wire::PORT_TRACE as i32;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Typed circuit graph with OpNode payloads. `edges` is the raw edge list
/// before topological sort; the sorted helpers are populated by `topo_sort`.
pub(crate) struct LoadedCircuit {
    out_schema: SchemaDescriptor,
    pub(crate) nodes: HashMap<i32, gnitz_wire::OpNode>,
    /// Raw (src, dst, port) tuples read from CircuitEdges — input to topo_sort.
    pub(crate) edges: Vec<(i32, i32, i32)>,
    // Populated by topo_sort:
    ordered: Vec<i32>,
    pub(crate) outgoing: HashMap<i32, Vec<(i32, i32)>>,
    incoming: HashMap<i32, Vec<(i32, i32)>>,
    consumers: HashMap<i32, Vec<i32>>,
    /// Raw node-column rows for GatherReduce nodes only (stays on legacy path
    /// until OpNode::GatherReduce gains an `agg: AggKind` field).
    gather_reduce_cols: HashMap<i32, Vec<(u64, u16, u64, u64)>>,
}

impl LoadedCircuit {
    pub(crate) fn empty() -> Self {
        LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: HashMap::new(),
            edges: Vec::new(),
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        }
    }
}

/// Annotation results from pre-passes.
struct Annotation {
    co_partitioned: HashSet<i64>,
    is_distinct_at: HashSet<i32>,
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
    pub schema: SchemaDescriptor,
}

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// Output from `compile_view`, consumed directly by DagEngine to build `CachedPlan`.
pub struct CompileOutput {
    pub pre_vm: Box<VmHandle>,
    pub post_vm: Option<Box<VmHandle>>,
    pub exchange_in_schema: Option<SchemaDescriptor>,
    pub pre_num_regs: u32,
    pub pre_in_reg: u16,
    pub pre_out_reg: u16,
    pub post_num_regs: u32,
    pub post_in_reg: u16,
    pub post_out_reg: u16,
    pub source_reg_map: HashMap<i64, u16>,
    pub co_partitioned: HashSet<i64>,
    pub pre_ext_trace_regs: Vec<(u16, i64)>,
    pub post_ext_trace_regs: Vec<(u16, i64)>,
}


// ---------------------------------------------------------------------------
// System table reading
// ---------------------------------------------------------------------------

/// Read an i64 value from a cursor's current row at the given column index.
pub fn cursor_read_i64(cursor: &ReadCursor, col_idx: usize, schema: &SchemaDescriptor) -> i64 {
    let col_size = schema.columns[col_idx].size() as usize;
    let ptr = cursor.col_ptr(col_idx, col_size);
    if ptr.is_null() {
        return 0;
    }
    match col_size {
        8 => i64::from_le_bytes(unsafe { *(ptr as *const [u8; 8]) }),
        4 => i32::from_le_bytes(unsafe { *(ptr as *const [u8; 4]) }) as i64,
        2 => i16::from_le_bytes(unsafe { *(ptr as *const [u8; 2]) }) as i64,
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
    // If length > 12: prefix + offset(u64) into blob arena
    let length = u32::from_le_bytes(unsafe { *(ptr as *const [u8; 4]) }) as usize;
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
        let blob_offset = u64::from_le_bytes(unsafe { *(ptr.add(8) as *const [u8; 8]) }) as usize;
        let blob_ptr = cursor.blob_ptr();
        let blob_len = cursor.blob_len();
        if blob_ptr.is_null() || blob_offset.checked_add(length).map_or(true, |end| end > blob_len) {
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
pub(crate) fn cursor_is_null(cursor: &ReadCursor, col_idx: usize, schema: &SchemaDescriptor) -> bool {
    if schema.is_pk_col(col_idx) {
        return false; // PK is never null
    }
    // Compute payload index
    let payload_idx = schema.payload_idx(col_idx);
    (cursor.current_null_word >> payload_idx) & 1 != 0
}

/// Create a cursor for a system table, seeked to view_id range.
/// Returns None if the table handle is null.
fn open_system_cursor(
    table: *mut Table,
    view_id: u64,
) -> Option<CursorHandle> {
    if table.is_null() {
        return None;
    }
    let t = unsafe { &mut *table };
    let mut ch = t.create_cursor().ok()?;
    ch.cursor.seek(crate::util::make_pk(0, view_id));
    Some(ch)
}

/// Decoded `ExprProgram` blob (inline copy of the gnitz-core wire shape so the
/// engine doesn't take a dependency on the client crate).
struct DecodedExprProgram {
    num_regs: u32,
    result_reg: u32,
    code: Vec<u32>,
    const_strings: Vec<Vec<u8>>,
}

const EXPR_BLOB_MAGIC: u32   = 0x5258_5045; // "EXPR" little-endian
const EXPR_BLOB_VERSION: u8  = 1;
const EXPR_BLOB_HEADER_SIZE: usize = 16;

fn decode_expr_blob(blob: &[u8]) -> Option<DecodedExprProgram> {
    if blob.len() < EXPR_BLOB_HEADER_SIZE { return None; }
    if u32::from_le_bytes(blob[0..4].try_into().unwrap()) != EXPR_BLOB_MAGIC { return None; }
    if blob[4] != EXPR_BLOB_VERSION { return None; }
    if blob[5] != 0 || blob[10] != 0 || blob[11] != 0 { return None; }
    let num_regs   = u16::from_le_bytes(blob[6..8].try_into().unwrap()) as u32;
    let result_reg = u16::from_le_bytes(blob[8..10].try_into().unwrap()) as u32;
    let n          = u32::from_le_bytes(blob[12..16].try_into().unwrap());
    if n % 4 != 0 { return None; }
    let code_bytes = (n as usize) * 4;
    let code_end   = EXPR_BLOB_HEADER_SIZE + code_bytes;
    if blob.len() < code_end + 4 { return None; }
    let mut code = Vec::with_capacity(n as usize);
    for i in 0..n as usize {
        let off = EXPR_BLOB_HEADER_SIZE + i * 4;
        code.push(u32::from_le_bytes(blob[off..off + 4].try_into().unwrap()));
    }
    let s_count = u32::from_le_bytes(blob[code_end..code_end + 4].try_into().unwrap());
    let mut cur = code_end + 4;
    let mut const_strings = Vec::with_capacity(s_count as usize);
    for _ in 0..s_count {
        if blob.len() < cur + 4 { return None; }
        let l = u32::from_le_bytes(blob[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if blob.len() < cur + l { return None; }
        const_strings.push(blob[cur..cur + l].to_vec());
        cur += l;
    }
    Some(DecodedExprProgram { num_regs, result_reg, code, const_strings })
}

// New CircuitNodes columns (PK is col 0; view_id at col 1 is denormalised
// and not read here — the cursor is already seeked by view_id range).
const NODES_COL_NODE_ID: usize      = 2;
const NODES_COL_OPCODE_NEW: usize   = 3;
const NODES_COL_SOURCE_TABLE: usize = 4;
const NODES_COL_REINDEX_COL: usize  = 5;
const NODES_COL_EXPR_PROGRAM: usize = 6;

// New CircuitEdges columns (PK is col 0).
const EDGES_COL_DST_NODE: usize = 2;
const EDGES_COL_DST_PORT: usize = 3;
const EDGES_COL_SRC_NODE: usize = 4;

// New CircuitNodeColumns columns (PK is col 0).
const NODECOL_COL_NODE_ID:  usize = 2;
const NODECOL_COL_KIND:     usize = 3;
const NODECOL_COL_POSITION: usize = 4;
const NODECOL_COL_VALUE1:   usize = 5;
const NODECOL_COL_VALUE2:   usize = 6;

pub(crate) fn load_circuit(
    sys_nodes: *mut Table, sys_nodes_schema: &SchemaDescriptor,
    sys_edges: *mut Table, sys_edges_schema: &SchemaDescriptor,
    sys_node_cols: *mut Table, sys_node_cols_schema: &SchemaDescriptor,
    view_id: u64,
    out_schema: SchemaDescriptor,
) -> Option<LoadedCircuit> {
    let mut nodes: HashMap<i32, gnitz_wire::OpNode> = HashMap::new();
    let mut edges: Vec<(i32, i32, i32)> = Vec::new();
    let mut gather_reduce_cols: HashMap<i32, Vec<(u64, u16, u64, u64)>> = HashMap::new();

    // Phase 1: read CircuitNodeColumns, sorted by (kind, position) per node.
    let mut cols_by_node: HashMap<i32, Vec<(u64, u16, u64, u64)>> = HashMap::new();
    {
        let mut ch = open_system_cursor(sys_node_cols, view_id)?;
        let end_hi = view_id + 1;
        while ch.cursor.valid && (ch.cursor.current_key >> 64) as u64 == view_id {
            if ch.cursor.current_weight > 0 {
                let node_id  = cursor_read_i64(&ch.cursor, NODECOL_COL_NODE_ID,  sys_node_cols_schema) as i32;
                let kind     = cursor_read_i64(&ch.cursor, NODECOL_COL_KIND,     sys_node_cols_schema) as u64;
                let position = cursor_read_i64(&ch.cursor, NODECOL_COL_POSITION, sys_node_cols_schema) as u16;
                let v1       = cursor_read_i64(&ch.cursor, NODECOL_COL_VALUE1,   sys_node_cols_schema) as u64;
                let v2       = cursor_read_i64(&ch.cursor, NODECOL_COL_VALUE2,   sys_node_cols_schema) as u64;
                cols_by_node.entry(node_id).or_default().push((kind, position, v1, v2));
            }
            ch.cursor.advance();
            if ch.cursor.valid && (ch.cursor.current_key >> 64) as u64 >= end_hi {
                break;
            }
        }
    }
    // Sort each node's cols by (kind, position) so decode_op_node sees ordered slices.
    for v in cols_by_node.values_mut() {
        v.sort_by_key(|&(kind, pos, _, _)| (kind, pos));
    }

    // Phase 2: read CircuitNodes; call decode_op_node for each.
    {
        let mut ch = open_system_cursor(sys_nodes, view_id)?;
        let end_hi = view_id + 1;
        while ch.cursor.valid && (ch.cursor.current_key >> 64) as u64 == view_id {
            if ch.cursor.current_weight > 0 {
                let node_id = cursor_read_i64(&ch.cursor, NODES_COL_NODE_ID, sys_nodes_schema) as i32;
                let opcode  = cursor_read_i64(&ch.cursor, NODES_COL_OPCODE_NEW, sys_nodes_schema) as u64;

                let src_tab: Option<u64> = if cursor_is_null(&ch.cursor, NODES_COL_SOURCE_TABLE, sys_nodes_schema) {
                    None
                } else {
                    Some(cursor_read_i64(&ch.cursor, NODES_COL_SOURCE_TABLE, sys_nodes_schema) as u64)
                };
                let reindex: Option<u16> = if cursor_is_null(&ch.cursor, NODES_COL_REINDEX_COL, sys_nodes_schema) {
                    None
                } else {
                    Some(cursor_read_i64(&ch.cursor, NODES_COL_REINDEX_COL, sys_nodes_schema) as u16)
                };
                let expr_blob: Option<Vec<u8>> = if cursor_is_null(&ch.cursor, NODES_COL_EXPR_PROGRAM, sys_nodes_schema) {
                    None
                } else {
                    let b = cursor_read_string(&ch.cursor, NODES_COL_EXPR_PROGRAM, sys_nodes_schema);
                    if b.is_empty() { None } else { Some(b) }
                };

                let cols = cols_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[]);
                match gnitz_wire::decode_op_node(opcode, src_tab, reindex, expr_blob, cols) {
                    Ok(op) => {
                        if matches!(op, gnitz_wire::OpNode::GatherReduce) {
                            if let Some(c) = cols_by_node.get(&node_id) {
                                gather_reduce_cols.insert(node_id, c.clone());
                            }
                        }
                        nodes.insert(node_id, op);
                    }
                    Err(_) => {} // unknown opcode: skip
                }
            }
            ch.cursor.advance();
            if ch.cursor.valid && (ch.cursor.current_key >> 64) as u64 >= end_hi {
                break;
            }
        }
    }

    // Phase 3: read CircuitEdges.
    {
        let mut ch = open_system_cursor(sys_edges, view_id)?;
        let end_hi = view_id + 1;
        while ch.cursor.valid && (ch.cursor.current_key >> 64) as u64 == view_id {
            if ch.cursor.current_weight > 0 {
                let dst  = cursor_read_i64(&ch.cursor, EDGES_COL_DST_NODE, sys_edges_schema) as i32;
                let port = cursor_read_i64(&ch.cursor, EDGES_COL_DST_PORT, sys_edges_schema) as i32;
                let src  = cursor_read_i64(&ch.cursor, EDGES_COL_SRC_NODE, sys_edges_schema) as i32;
                edges.push((src, dst, port));
            }
            ch.cursor.advance();
            if ch.cursor.valid && (ch.cursor.current_key >> 64) as u64 >= end_hi {
                break;
            }
        }
    }

    Some(LoadedCircuit {
        out_schema,
        nodes,
        edges,
        gather_reduce_cols,
        ..LoadedCircuit::empty()
    })
}

// ---------------------------------------------------------------------------
// Topological sort (Kahn's algorithm)
// ---------------------------------------------------------------------------

fn topo_sort(loaded: &mut LoadedCircuit) -> Result<(), i32> {
    let mut in_degree: HashMap<i32, i32> = HashMap::new();
    for &nid in loaded.nodes.keys() {
        in_degree.insert(nid, 0);
        loaded.outgoing.entry(nid).or_default();
        loaded.incoming.entry(nid).or_default();
        loaded.consumers.entry(nid).or_default();
    }

    for &(src, dst, port) in &loaded.edges {
        loaded.outgoing.entry(src).or_default().push((dst, port));
        loaded.incoming.entry(dst).or_default().push((src, port));
        loaded.consumers.entry(src).or_default().push(dst);
        *in_degree.entry(dst).or_insert(0) += 1;
    }

    let mut init: Vec<i32> = loaded.nodes.keys()
        .filter(|&&nid| *in_degree.get(&nid).unwrap_or(&0) == 0)
        .copied()
        .collect();
    init.sort_unstable(); // deterministic order for tied sources
    let mut queue: VecDeque<i32> = init.into();

    let mut ordered = Vec::with_capacity(loaded.nodes.len());
    while let Some(nid) = queue.pop_front() {
        ordered.push(nid);
        if let Some(outs) = loaded.outgoing.get(&nid) {
            let mut next_batch: Vec<i32> = Vec::new();
            for &(dst, _) in outs {
                let deg = in_degree.get_mut(&dst).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    next_batch.push(dst);
                }
            }
            next_batch.sort_unstable();
            queue.extend(next_batch);
        }
    }

    if ordered.len() != loaded.nodes.len() {
        return Err(-1); // cycle detected
    }
    loaded.ordered = ordered;
    Ok(())
}

// ---------------------------------------------------------------------------
// Annotation passes
// ---------------------------------------------------------------------------

/// Build the join-shard map by inspecting OpNode variants directly.
/// Maps source_tid → [reindex_col] for ScanDelta → Map(Expression{reindex_col})
/// chains (equijoin pre-indexing).
fn compute_join_shard_map(loaded: &LoadedCircuit) -> HashMap<i64, Vec<i32>> {
    let mut join_shard_map = HashMap::new();
    for &(src, dst, _port) in &loaded.edges {
        if let Some(gnitz_wire::OpNode::ScanDelta(tid)) = loaded.nodes.get(&src) {
            if let Some(gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                reindex_col: Some(rc), ..
            })) = loaded.nodes.get(&dst) {
                join_shard_map.insert(*tid as i64, vec![*rc as i32]);
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
                if ext.schema.is_pk_col(cols[0] as usize) {
                    co_partitioned.insert(tid);
                }
            }
        }
    }
    co_partitioned
}

fn propagate_distinct(loaded: &LoadedCircuit, ann: &mut Annotation) {
    for &nid in &loaded.ordered {
        match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::Reduce { .. }) | Some(gnitz_wire::OpNode::Distinct) => {
                ann.is_distinct_at.insert(nid);
            }
            Some(gnitz_wire::OpNode::Filter(_)) => {
                let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
                if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0].0) {
                    ann.is_distinct_at.insert(nid);
                }
            }
            Some(gnitz_wire::OpNode::Map(mk)) => {
                let has_reindex = matches!(mk,
                    gnitz_wire::MapKind::Expression { reindex_col: Some(_), .. });
                if !has_reindex {
                    let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
                    if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0].0) {
                        ann.is_distinct_at.insert(nid);
                    }
                }
            }
            _ => {}
        }
    }
}

fn annotate(loaded: &LoadedCircuit, ext_tables: &[ExternalTable]) -> Annotation {
    let join_shard_map = compute_join_shard_map(loaded);
    let co_partitioned = compute_co_partitioned(&join_shard_map, ext_tables);
    let mut ann = Annotation { co_partitioned, is_distinct_at: HashSet::new() };
    propagate_distinct(loaded, &mut ann);
    ann
}

// ---------------------------------------------------------------------------
// Optimization passes
// ---------------------------------------------------------------------------

fn schemas_physically_identical(a: &SchemaDescriptor, b: &SchemaDescriptor) -> bool {
    if a.num_columns() != b.num_columns() || a.pk_indices() != b.pk_indices() {
        return false;
    }
    for i in 0..a.num_columns() {
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
#[allow(clippy::vec_box)]
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

fn opt_distinct(loaded: &LoadedCircuit, ann: &Annotation, rw: &mut Rewrites) {
    for (&nid, op) in &loaded.nodes {
        if matches!(op, gnitz_wire::OpNode::Distinct) {
            let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
            if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0].0) {
                rw.skip_nodes.insert(nid);
            }
        }
    }
}

#[allow(clippy::vec_box, clippy::ptr_arg)]
fn opt_fold_reduce_map(
    loaded: &LoadedCircuit,
    rw: &mut Rewrites,
    owned_expr_progs: &mut Vec<Box<ExprProgram>>,
) {
    for (&nid, op) in &loaded.nodes {
        let blob = match op {
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression { program, reindex_col: None }) => program,
            _ => continue,
        };
        let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
        if in_nids.len() != 1 {
            continue;
        }
        let reduce_nid = in_nids[0].0;
        if !matches!(loaded.nodes.get(&reduce_nid), Some(gnitz_wire::OpNode::Reduce { .. })) {
            continue;
        }
        let consumers = loaded.consumers.get(&reduce_nid).cloned().unwrap_or_default();
        if consumers.len() != 1 {
            continue;
        }
        let dep = match decode_expr_blob(blob) {
            Some(d) => d,
            None => continue,
        };
        let code: Vec<i64> = dep.code.iter().map(|&w| w as i64).collect();
        let prog = ExprProgram::new(code, dep.num_regs, 0, Vec::new());
        if prog.is_sequential_copy_projection() {
            continue;
        }
        if prog.code.len() <= 63 {
            let idx = owned_expr_progs.len();
            owned_expr_progs.push(Box::new(prog));
            rw.fold_finalize.insert(reduce_nid, idx);
            rw.folded_maps.insert(nid, reduce_nid);
        }
    }
}

// ---------------------------------------------------------------------------
// Schema construction helpers
// ---------------------------------------------------------------------------

fn merge_schemas_for_join_impl(
    left: &SchemaDescriptor,
    right: &SchemaDescriptor,
    right_nullable: bool,
) -> SchemaDescriptor {
    let total = left.num_columns() + right.num_payload_cols();
    assert!(total <= crate::schema::MAX_COLUMNS,
        "join output schema exceeds {}-column limit: {} + payload({}) = {}",
        crate::schema::MAX_COLUMNS, left.num_columns(), right.num_payload_cols(), total);
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut n = 0;
    cols[n] = left.columns[left.pk_index_single() as usize];
    n += 1;
    for (_, _, c) in left.payload_columns() {
        cols[n] = *c;
        n += 1;
    }
    for (_, _, c) in right.payload_columns() {
        let mut c = *c;
        if right_nullable { c.nullable = 1; }
        cols[n] = c;
        n += 1;
    }
    SchemaDescriptor::new(&cols[..n], &[0])
}

fn merge_schemas_for_join(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
    merge_schemas_for_join_impl(left, right, false)
}

fn merge_schemas_for_join_outer(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
    merge_schemas_for_join_impl(left, right, true)
}

fn build_map_output_schema(input: &SchemaDescriptor, src_indices: &[i32]) -> SchemaDescriptor {
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut n = 0;
    cols[n] = input.columns[input.pk_index_single() as usize];
    n += 1;
    for &idx in src_indices {
        let i = idx as usize;
        if !input.is_pk_col(i) {
            cols[n] = input.columns[i];
            n += 1;
        }
    }
    SchemaDescriptor::new(&cols[..n], &[0])
}

/// Determine the output type for an aggregate function.
/// Must match `UniversalAccumulator.output_column_type()`:
///   COUNT, COUNT_NON_NULL → I64
///   SUM/MIN/MAX on float → F64
///   everything else → I64  (including MIN/MAX on STRING, I32, etc.)
const fn agg_output_type(agg_op: AggOp, col_type_code: TypeCode) -> u8 {
    match agg_op {
        AggOp::Count | AggOp::CountNonNull => type_code::I64,
        AggOp::Sum | AggOp::Min | AggOp::Max => {
            if col_type_code.is_float() {
                type_code::F64
            } else {
                type_code::I64
            }
        }
        AggOp::Null => type_code::I64,
    }
}

fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[i32],
    agg_descs: &[AggDescriptor],
) -> SchemaDescriptor {
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut n = 0;

    let use_natural_pk = if group_cols.len() == 1 {
        let gc_tc = TypeCode::from_validated_u8(input.columns[group_cols[0] as usize].type_code);
        matches!(gc_tc, TypeCode::U64 | TypeCode::U128 | TypeCode::UUID)
    } else {
        false
    };

    if use_natural_pk {
        // Col 0: group column (PK)
        cols[n] = input.columns[group_cols[0] as usize];
        n += 1;
    } else {
        // Synthetic U128 PK
        cols[n] = SchemaColumn::new(type_code::U128, 0);
        n += 1;
        // Group columns
        for &gc in group_cols {
            cols[n] = input.columns[gc as usize];
            n += 1;
        }
    }
    // Aggregate results (same for both branches)
    for ad in agg_descs {
        cols[n] = SchemaColumn::new(agg_output_type(ad.agg_op, ad.col_type_code), 0);
        n += 1;
    }
    SchemaDescriptor::new(&cols[..n], &[0])
}

fn agg_value_idx_eligible(tc: TypeCode) -> bool {
    !matches!(tc, TypeCode::U128 | TypeCode::UUID | TypeCode::String)
}

// ---------------------------------------------------------------------------
// Expression + scalar function construction helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
fn create_expr_predicate(
    code: Vec<i64>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
    schema: &SchemaDescriptor,
    _owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let mut prog = ExprProgram::new(code, num_regs, result_reg, const_strings);
    prog.set_payload_col_info(schema);
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_predicate(prog, schema)));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

#[allow(clippy::vec_box, clippy::ptr_arg)]
fn create_expr_map(
    code: Vec<i64>,
    num_regs: u32,
    const_strings: Vec<Vec<u8>>,
    schema: &SchemaDescriptor,
    _owned_expr_progs: &mut Vec<Box<ExprProgram>>,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let mut prog = ExprProgram::new(code, num_regs, 0, const_strings);
    prog.set_payload_col_info(schema);
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_map(prog, schema)));
    let ptr = &*func as *const ScalarFuncKind;
    owned_funcs.push(func);
    ptr
}

#[allow(clippy::vec_box, clippy::ptr_arg)]
fn create_universal_projection(
    src_indices: &[i32],
    src_types: &[u8],
    schema: &SchemaDescriptor,
    owned_funcs: &mut Vec<Box<ScalarFuncKind>>,
) -> *const ScalarFuncKind {
    let indices: Vec<u32> = src_indices.iter().map(|&i| i as u32).collect();
    let func = Box::new(ScalarFuncKind::Plan(Plan::from_projection(
        &indices, src_types, schema,
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
) -> Result<Table, crate::storage::StorageError> {
    let child_dir = format!("{}/scratch_{}", view_dir, child_name);
    Table::new(&child_dir, child_name, schema, table_id, 256 * 1024, false)
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

fn is_join_trace_side(loaded: &LoadedCircuit, nid: i32) -> bool {
    loaded.outgoing.get(&nid).map(|outs| {
        outs.iter().any(|&(dst, port)| {
            port == PORT_TRACE
                && matches!(
                    loaded.nodes.get(&dst),
                    Some(gnitz_wire::OpNode::Join(_))
                    | Some(gnitz_wire::OpNode::AntiJoin(_))
                    | Some(gnitz_wire::OpNode::SemiJoin(_))
                    | Some(gnitz_wire::OpNode::SeekTrace)
                )
        })
    }).unwrap_or(false)
}

struct EmitState {
    next_extra_reg: i32,
    sink_reg_id: i32,
    input_delta_reg_id: i32,
    emit_failed: bool,
}

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
fn emit_node(
    loaded: &LoadedCircuit,
    rw: &Rewrites,
    nid: i32,
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
    let in_regs = compute_in_regs(loaded, nid, out_reg_of);
    let op = match loaded.nodes.get(&nid) {
        Some(op) => op,
        None => return,
    };

    match op {
        gnitz_wire::OpNode::ScanDelta(tid) => {
            if let Some(ext) = ext_tables.iter().find(|t| t.table_id == *tid as i64) {
                reg_schemas[reg_id as usize] = ext.schema;
                reg_kinds[reg_id as usize] = 0;
                source_reg_map.insert(*tid as i64, reg_id);
            }
        }

        gnitz_wire::OpNode::ScanTrace(tid) => {
            if let Some(ext) = ext_tables.iter().find(|t| t.table_id == *tid as i64) {
                reg_schemas[reg_id as usize] = ext.schema;
                reg_kinds[reg_id as usize] = 1;
                ext_trace_regs.push((reg_id as u16, *tid as i64));

                if !is_join_trace_side(loaded, nid) {
                    let out_delta_id = state.next_extra_reg;
                    state.next_extra_reg += 1;
                    reg_schemas[out_delta_id as usize] = ext.schema;
                    reg_kinds[out_delta_id as usize] = 0;
                    out_reg_of.insert(nid, out_delta_id);
                    builder.add_scan_trace(reg_id as u16, out_delta_id as u16, 0);
                }
            }
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_schemas[in_reg as usize];
            reg_schemas[reg_id as usize] = in_schema;
            reg_kinds[reg_id as usize] = 0;
            let func_ptr = if let Some(blob) = blob {
                if let Some(dep) = decode_expr_blob(blob) {
                    let code: Vec<i64> = dep.code.iter().map(|&w| w as i64).collect();
                    create_expr_predicate(code, dep.num_regs, dep.result_reg, dep.const_strings,
                        &in_schema, owned_expr_progs, owned_funcs)
                } else {
                    null_func_ptr()
                }
            } else {
                null_func_ptr()
            };
            builder.add_filter(in_reg as u16, reg_id as u16, func_ptr);
        }

        gnitz_wire::OpNode::Map(mk) => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            match mk {
                gnitz_wire::MapKind::Expression { program, reindex_col } => {
                    let dep = decode_expr_blob(program);
                    // Identity MAP: if no reindex and schemas match, skip if sequential copy.
                    if reindex_col.is_none()
                        && schemas_physically_identical(&in_reg_schema, &loaded.out_schema)
                    {
                        if let Some(ref d) = dep {
                            let code: Vec<i64> = d.code.iter().map(|&w| w as i64).collect();
                            let prog = ExprProgram::new(code, d.num_regs, 0, Vec::new());
                            if prog.is_sequential_copy_projection() {
                                out_reg_of.insert(nid, in_reg);
                                return;
                            }
                        }
                    }
                    // Folded MAP (absorbed into upstream REDUCE's finalize program).
                    if let Some(&reduce_nid) = rw.folded_maps.get(&nid) {
                        out_reg_of.insert(nid, *out_reg_of.get(&reduce_nid).unwrap_or(&0));
                        return;
                    }
                    if let Some(dep) = dep {
                        let code: Vec<i64> = dep.code.iter().map(|&w| w as i64).collect();
                        let fp = create_expr_map(code, dep.num_regs, dep.const_strings,
                            &in_reg_schema, owned_expr_progs, owned_funcs);
                        let node_schema = if reindex_col.is_some() {
                            let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
                            cols[0] = SchemaColumn::new(type_code::U128, 0);
                            let n = in_reg_schema.num_columns();
                            for i in 0..n {
                                cols[i + 1] = in_reg_schema.columns[i];
                            }
                            SchemaDescriptor::new(&cols[..n + 1], &[0])
                        } else {
                            loaded.out_schema
                        };
                        reg_schemas[reg_id as usize] = node_schema;
                        reg_kinds[reg_id as usize] = 0;
                        let rc = reindex_col.map(|c| c as i32).unwrap_or(-1);
                        builder.add_map(in_reg as u16, reg_id as u16, fp, node_schema, rc);
                    }
                }

                gnitz_wire::MapKind::Projection(cols) => {
                    let src_indices: Vec<i32> = cols.iter().map(|&c| c as i32).collect();
                    let src_types: Vec<u8> = src_indices.iter()
                        .map(|&i| in_reg_schema.columns[i as usize].type_code)
                        .collect();
                    let fp = create_universal_projection(
                        &src_indices, &src_types, &in_reg_schema, owned_funcs,
                    );
                    let schema = build_map_output_schema(&in_reg_schema, &src_indices);
                    reg_schemas[reg_id as usize] = schema;
                    reg_kinds[reg_id as usize] = 0;
                    builder.add_map(in_reg as u16, reg_id as u16, fp, schema, -1);
                }

                gnitz_wire::MapKind::KeyOnly => {
                    let fp = create_universal_projection(
                        &[], &[], &in_reg_schema, owned_funcs,
                    );
                    let s = SchemaDescriptor::new(
                        &[in_reg_schema.columns[in_reg_schema.pk_index_single() as usize]],
                        &[0],
                    );
                    reg_schemas[reg_id as usize] = s;
                    reg_kinds[reg_id as usize] = 0;
                    builder.add_map(in_reg as u16, reg_id as u16, fp, s, -1);
                }
            }
        }

        gnitz_wire::OpNode::Negate => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            builder.add_negate(in_reg as u16, reg_id as u16);
        }

        gnitz_wire::OpNode::Union => {
            let in_a = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[in_a as usize];
            reg_kinds[reg_id as usize] = 0;
            let has_b = in_regs.contains_key(&PORT_IN_B);
            let in_b = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            builder.add_union(in_a as u16, in_b as u16, has_b, reg_id as u16);
        }

        gnitz_wire::OpNode::Delay => {
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

        gnitz_wire::OpNode::Distinct => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
            if rw.skip_nodes.contains(&nid) {
                out_reg_of.insert(nid, in_reg);
                return;
            }
            let child_name = format!("_hist_{}_{}", view_id, nid);
            let hist_table = match create_child_table(
                view_dir, &child_name, in_reg_schema, view_table_id,
            ) {
                Ok(t) => t,
                Err(_) => { state.emit_failed = true; return; }
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

        gnitz_wire::OpNode::Reduce { group_cols, agg } => {
            emit_reduce(
                loaded, rw, nid, reg_id, group_cols, agg, &in_regs,
                builder, state, out_reg_of, reg_schemas, reg_kinds,
                owned_tables, owned_funcs, owned_expr_progs, owned_trace_regs,
                view_dir, view_table_id, view_id,
            );
        }

        gnitz_wire::OpNode::Join(kind) => {
            // PORT_IN_A == 0 (delta side); PORT_TRACE == PORT_IN_B == 1 (trace/right side).
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let a_schema = reg_schemas[a_reg as usize];
            let b_schema = reg_schemas[b_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace => {
                    reg_schemas[reg_id as usize] = merge_schemas_for_join(&a_schema, &b_schema);
                    builder.add_join_dt(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
                gnitz_wire::JoinKind::DeltaTraceOuter => {
                    reg_schemas[reg_id as usize] = merge_schemas_for_join_outer(&a_schema, &b_schema);
                    builder.add_join_dt_outer(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
                gnitz_wire::JoinKind::DeltaDelta => {
                    reg_schemas[reg_id as usize] = merge_schemas_for_join(&a_schema, &b_schema);
                    builder.add_join_dd(a_reg as u16, b_reg as u16, reg_id as u16, b_schema);
                }
            }
        }

        gnitz_wire::OpNode::AntiJoin(kind) => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[a_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace | gnitz_wire::JoinKind::DeltaTraceOuter => {
                    builder.add_anti_join_dt(a_reg as u16, b_reg as u16, reg_id as u16);
                }
                gnitz_wire::JoinKind::DeltaDelta => {
                    builder.add_anti_join_dd(a_reg as u16, b_reg as u16, reg_id as u16);
                }
            }
        }

        gnitz_wire::OpNode::SemiJoin(kind) => {
            let a_reg = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
            let b_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            reg_schemas[reg_id as usize] = reg_schemas[a_reg as usize];
            reg_kinds[reg_id as usize] = 0;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace | gnitz_wire::JoinKind::DeltaTraceOuter => {
                    builder.add_semi_join_dt(a_reg as u16, b_reg as u16, reg_id as u16);
                }
                gnitz_wire::JoinKind::DeltaDelta => {
                    builder.add_semi_join_dd(a_reg as u16, b_reg as u16, reg_id as u16);
                }
            }
        }

        gnitz_wire::OpNode::IntegrateSink => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            state.sink_reg_id = in_reg;
            emit_simple_integrate(builder, in_reg as u16, std::ptr::null_mut());
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_reg_schema = reg_schemas[in_reg as usize];
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
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {}

        gnitz_wire::OpNode::ExchangeGather => {
            if let Some(&in_reg) = in_regs.get(&PORT_IN) {
                reg_schemas[reg_id as usize] = reg_schemas[in_reg as usize];
                reg_kinds[reg_id as usize] = reg_kinds[in_reg as usize];
                // ExchangeGather is a logical passthrough: the exchange mechanism
                // injects gathered data directly into the exchange-input register.
                // Redirect downstream reads to that register; reg_id is never written.
                out_reg_of.insert(nid, in_reg);
            }
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            let in_schema = reg_schemas[in_reg as usize];
            assert!(
                type_codes.len() < crate::schema::MAX_COLUMNS,
                "NULL_EXTEND n_cols={} would overflow schema array (max {})",
                type_codes.len(),
                crate::schema::MAX_COLUMNS - 1,
            );
            let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
            cols[0] = SchemaColumn::new(type_code::U128, 0); // dummy PK
            for (i, &tc) in type_codes.iter().enumerate() {
                cols[i + 1] = SchemaColumn::new(tc, 1);
            }
            let right = SchemaDescriptor::new(&cols[..type_codes.len() + 1], &[0]);
            let out_schema = merge_schemas_for_join_outer(&in_schema, &right);
            reg_schemas[reg_id as usize] = out_schema;
            reg_kinds[reg_id as usize] = 0;
            builder.add_null_extend(in_reg as u16, reg_id as u16, right);
        }

        gnitz_wire::OpNode::GatherReduce => {
            let raw_cols = loaded.gather_reduce_cols.get(&nid).map(|v| v.as_slice()).unwrap_or(&[]);
            emit_gather_reduce(
                raw_cols, nid, reg_id, &in_regs,
                builder, state, out_reg_of, reg_schemas, reg_kinds,
                owned_tables, owned_trace_regs,
                view_dir, view_table_id, view_id,
            );
        }

        gnitz_wire::OpNode::SeekTrace => {
            let trace_reg = in_regs.get(&PORT_TRACE).copied().unwrap_or(0);
            let key_reg = in_regs.get(&PORT_IN).copied().unwrap_or(0);
            builder.add_seek_trace(trace_reg as u16, key_reg as u16);
        }

        gnitz_wire::OpNode::ClearDeltas => {
            builder.add_clear_deltas();
        }
    }
}

fn compute_in_regs(loaded: &LoadedCircuit, nid: i32, out_reg_of: &HashMap<i32, i32>) -> HashMap<i32, i32> {
    let mut in_regs = HashMap::new();
    if let Some(in_edges) = loaded.incoming.get(&nid) {
        for &(src, port) in in_edges {
            if let Some(&reg) = out_reg_of.get(&src) {
                in_regs.insert(port, reg);
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
// REDUCE emission
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
fn emit_reduce(
    loaded: &LoadedCircuit,
    rw: &Rewrites,
    nid: i32,
    reg_id: i32,
    group_cols: &[u16],
    agg: &gnitz_wire::AggKind,
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
    let in_reg_id = in_regs.get(&PORT_IN).copied().unwrap_or(0);
    let in_reg_schema = reg_schemas[in_reg_id as usize];

    let gcols: Vec<i32> = group_cols.iter().map(|&c| c as i32).collect();
    let gcols_u32: Vec<u32> = group_cols.iter().map(|&c| c as u32).collect();

    let mut agg_descs: Vec<AggDescriptor> = Vec::new();
    let mut agg_func_id: AggOp = AggOp::Null;
    let mut agg_col_idx: u32 = 0;

    match agg {
        gnitz_wire::AggKind::Null => {
            agg_descs.push(AggDescriptor {
                col_idx: 0, agg_op: AggOp::Null, col_type_code: TypeCode::U64, _pad: [0; 2],
            });
        }
        gnitz_wire::AggKind::Specs(specs) => {
            for &(ref func, col_idx) in specs {
                let agg_op = AggOp::from(*func);
                let col_type_code = TypeCode::from_validated_u8(
                    in_reg_schema.columns[col_idx as usize].type_code,
                );
                agg_descs.push(AggDescriptor {
                    col_idx: col_idx as u32, agg_op, col_type_code, _pad: [0; 2],
                });
            }
            if agg_descs.len() == 1 {
                agg_func_id = agg_descs[0].agg_op;
                agg_col_idx = agg_descs[0].col_idx;
            }
        }
    }

    let reduce_out_schema = build_reduce_output_schema(&in_reg_schema, &gcols, &agg_descs);

    let trace_table = match create_child_table(
        view_dir, &format!("_reduce_{}_{}", view_id, nid), reduce_out_schema, view_table_id,
    ) {
        Ok(t) => t,
        Err(_) => { state.emit_failed = true; return; }
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

    let finalize_prog_idx = rw.fold_finalize.get(&nid).copied();
    let mut fin_delta_id: i32 = -1;
    if finalize_prog_idx.is_some() {
        fin_delta_id = state.next_extra_reg;
        state.next_extra_reg += 1;
        reg_schemas[fin_delta_id as usize] = loaded.out_schema;
        reg_kinds[fin_delta_id as usize] = 0;
        out_reg_of.insert(nid, fin_delta_id);
    } else {
        out_reg_of.insert(nid, raw_delta_id);
    }

    let all_linear = agg_descs.iter().all(|a| a.agg_op.is_linear());
    let will_use_avi = agg_descs.len() == 1
        && matches!(agg_func_id, AggOp::Min | AggOp::Max)
        && agg_value_idx_eligible(
            TypeCode::from_validated_u8(in_reg_schema.columns[agg_col_idx as usize].type_code),
        );

    let mut tr_in_reg_id: i32 = -1;
    let mut tr_in_table_ptr: *mut Table = std::ptr::null_mut();
    let mut tr_in_table_idx: Option<usize> = None;

    if let Some(&existing) = in_regs.get(&PORT_TRACE) {
        tr_in_reg_id = existing;
    } else if !all_linear && !will_use_avi {
        let tr_in = match create_child_table(
            view_dir, &format!("_reduce_in_{}_{}", view_id, nid), in_reg_schema, view_table_id,
        ) {
            Ok(t) => t,
            Err(_) => { state.emit_failed = true; return; }
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

    let mut gi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut gi_col_idx: u32 = 0;
    let mut gi_col_type_code: u8 = 0;

    if tr_in_table_idx.is_some() && gcols.len() == 1 {
        let gc_col_idx = gcols[0] as usize;
        let gc_raw = in_reg_schema.columns[gc_col_idx].type_code;
        let gc_tc = TypeCode::from_validated_u8(gc_raw);
        if matches!(gc_tc,
            TypeCode::U8  | TypeCode::I8  | TypeCode::U16 | TypeCode::I16 |
            TypeCode::U32 | TypeCode::I32 | TypeCode::U64 | TypeCode::I64
        ) {
            let gi_dir = format!(
                "{}/scratch__reduce_in_{}_{}/_gidx", view_dir, view_id, nid,
            );
            if let Ok(gi_table) = Table::new(
                &gi_dir, "_gidx", crate::ops::index::make_gi_schema(), 0, 1024 * 1024, false,
            ) {
                let idx = owned_tables.len();
                owned_tables.push(Box::new(gi_table));
                gi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
                gi_col_idx = gc_col_idx as u32;
                gi_col_type_code = gc_raw;
            }
        }
    }

    let mut avi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut avi_for_max = false;
    let mut avi_agg_col_type_code: u8 = 0;
    let mut avi_group_cols: Vec<u32> = Vec::new();
    let mut avi_agg_col_idx: u32 = 0;

    if will_use_avi {
        avi_for_max = agg_func_id == AggOp::Max;
        avi_agg_col_type_code = in_reg_schema.columns[agg_col_idx as usize].type_code;
        avi_agg_col_idx = agg_col_idx;
        avi_group_cols = gcols_u32.clone();
        let avi_child = format!("_avidx_{}_{}", view_id, nid);
        if let Ok(av_table) = create_child_table(
            view_dir, &avi_child, crate::ops::index::make_avi_schema(), view_table_id,
        ) {
            let idx = owned_tables.len();
            owned_tables.push(Box::new(av_table));
            avi_table_ptr = &*owned_tables[idx] as *const Table as *mut Table;
        }
    }

    if !avi_table_ptr.is_null() {
        builder.add_integrate(
            in_reg_id as u16,
            std::ptr::null_mut(),
            std::ptr::null_mut(), 0, 0,
            avi_table_ptr, avi_for_max, avi_agg_col_type_code,
            &avi_group_cols, &in_reg_schema as *const SchemaDescriptor, avi_agg_col_idx,
        );
    }

    let fin_prog_ptr: *const ExprProgram = if let Some(idx) = finalize_prog_idx {
        // Finalize prog reads from the raw reduce output, so resolve its
        // column operands against reduce_out_schema. Idempotent.
        owned_expr_progs[idx].resolve_column_indices(&reduce_out_schema);
        &*owned_expr_progs[idx] as *const ExprProgram
    } else {
        std::ptr::null()
    };
    let fin_schema_ptr: *const SchemaDescriptor = if finalize_prog_idx.is_some() {
        &loaded.out_schema as *const SchemaDescriptor
    } else {
        std::ptr::null()
    };

    builder.add_reduce(
        in_reg_id as u16,
        tr_in_reg_id as i16,
        reg_id as u16,
        raw_delta_id as u16,
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

    emit_simple_integrate(builder, raw_delta_id as u16, trace_table_ptr);
}

// ---------------------------------------------------------------------------
// GATHER_REDUCE emission
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments, clippy::vec_box, clippy::ptr_arg)]
fn emit_gather_reduce(
    raw_cols: &[(u64, u16, u64, u64)],
    nid: i32,
    reg_id: i32,
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
    let in_reg_id = in_regs.get(&PORT_IN).copied().unwrap_or(0);
    let partial_schema = reg_schemas[in_reg_id as usize];
    let num_out_cols = partial_schema.num_columns();

    let agg_specs: Vec<(u64, u64)> = raw_cols.iter()
        .filter(|(k, _, _, _)| *k == gnitz_wire::NODE_COL_KIND_AGG_SPEC)
        .map(|(_, _, v1, v2)| (*v1, *v2))
        .collect();
    let agg_count = agg_specs.len();
    let mut agg_descs: Vec<AggDescriptor> = Vec::new();

    if agg_count > 0 {
        assert!(
            num_out_cols >= agg_count,
            "GATHER_REDUCE node {}: agg_count ({}) exceeds partial schema column count ({})",
            nid, agg_count, num_out_cols,
        );
        for (ai, &(func_id, _)) in agg_specs.iter().enumerate() {
            let agg_op = AggOp::try_from(func_id as u8)
                .unwrap_or_else(|v| panic!("invalid agg_op {v} from wire protocol"));
            let agg_col_in_partial = num_out_cols - agg_count + ai;
            let col_type = TypeCode::from_validated_u8(
                partial_schema.columns[agg_col_in_partial].type_code,
            );
            agg_descs.push(AggDescriptor {
                col_idx: 0, agg_op, col_type_code: col_type, _pad: [0; 2],
            });
        }
    } else {
        agg_descs.push(AggDescriptor {
            col_idx: 0, agg_op: AggOp::Null, col_type_code: TypeCode::U64, _pad: [0; 2],
        });
    }

    let trace_table = match create_child_table(
        view_dir, &format!("_gather_{}_{}", view_id, nid), partial_schema, view_table_id,
    ) {
        Ok(t) => t,
        Err(_) => { state.emit_failed = true; return; }
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

#[allow(clippy::too_many_arguments, clippy::vec_box)]
fn build_plan(
    loaded: &LoadedCircuit,
    rw: &Rewrites,
    ordered: &[i32],
    ext_tables: &[ExternalTable],
    view_dir: &str,
    view_table_id: u32,
    view_id: u64,
    output_node_id: Option<i32>,
    exchange_input: Option<(i32, SchemaDescriptor)>,
    pre_built_expr_progs: Vec<Box<ExprProgram>>,
) -> Option<PlanBuildResult> {
    let mut out_reg_of: HashMap<i32, i32> = HashMap::new();
    let mut next_reg: i32 = 0;
    for &nid in ordered {
        out_reg_of.insert(nid, next_reg);
        next_reg += 1;
    }

    let mut extra_regs = 0;
    for &nid in ordered {
        let op = loaded.nodes.get(&nid);
        if matches!(op, Some(gnitz_wire::OpNode::Distinct)) && !rw.skip_nodes.contains(&nid) {
            extra_regs += 1;
        } else if matches!(op, Some(gnitz_wire::OpNode::Reduce { .. })) {
            extra_regs += 2; // raw_delta + optional tr_in (safe to over-allocate)
            if rw.fold_finalize.contains_key(&nid) {
                extra_regs += 1;
            }
        } else if matches!(op, Some(gnitz_wire::OpNode::GatherReduce)) {
            extra_regs += 1;
        } else if matches!(op, Some(gnitz_wire::OpNode::ScanTrace(_))) {
            if !is_join_trace_side(loaded, nid) {
                extra_regs += 1;
            }
        } else if matches!(op, Some(gnitz_wire::OpNode::Delay)) {
            extra_regs += 1;
        }
    }

    let mut exchange_input_reg_id: i32 = -1;
    if exchange_input.is_some() {
        exchange_input_reg_id = next_reg;
        next_reg += 1;
    }

    let num_regs = (next_reg + extra_regs) as usize;
    let mut reg_schemas = vec![SchemaDescriptor::default(); num_regs];
    let mut reg_kinds = vec![0u8; num_regs];

    if let Some((ex_nid, ex_schema)) = exchange_input {
        out_reg_of.insert(ex_nid, exchange_input_reg_id);
        reg_schemas[exchange_input_reg_id as usize] = ex_schema;
        reg_kinds[exchange_input_reg_id as usize] = 0;
    }

    let mut owned_tables: Vec<Box<Table>> = Vec::new();
    let mut owned_funcs: Vec<Box<ScalarFuncKind>> = Vec::new();
    let mut owned_expr_progs: Vec<Box<ExprProgram>> = pre_built_expr_progs;
    let mut owned_trace_regs: Vec<(u16, usize)> = Vec::new();
    let mut ext_trace_regs: Vec<(u16, i64)> = Vec::new();
    let mut source_reg_map: HashMap<i64, i32> = HashMap::new();

    let mut builder = ProgramBuilder::new(num_regs as u16);
    let mut state = EmitState {
        next_extra_reg: next_reg,
        sink_reg_id: -1,
        input_delta_reg_id: if exchange_input_reg_id >= 0 { exchange_input_reg_id } else { -1 },
        emit_failed: false,
    };

    for &nid in ordered {
        if matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ExchangeShard { .. })) {
            continue;
        }
        let reg_id = *out_reg_of.get(&nid).unwrap();
        emit_node(
            loaded, rw, nid, reg_id,
            &mut builder, &mut state, &mut out_reg_of,
            &mut reg_schemas, &mut reg_kinds,
            &mut owned_tables, &mut owned_funcs, &mut owned_expr_progs, &mut owned_trace_regs,
            ext_tables, &mut ext_trace_regs, &mut source_reg_map,
            view_dir, view_table_id, view_id,
        );
    }

    if state.emit_failed {
        return None;
    }

    builder.add_halt();

    let mut input_delta_reg_id = state.input_delta_reg_id;
    if input_delta_reg_id == -1 && !source_reg_map.is_empty() {
        input_delta_reg_id = *source_reg_map.values().next().unwrap();
    }

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

    if output_node_id.is_none() && sink_reg >= 0 {
        let sink_schema = &reg_schemas[sink_reg as usize];
        let out_schema = &loaded.out_schema;
        if sink_schema.num_columns() > 0 && sink_schema.num_columns() != out_schema.num_columns() {
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

/// Compile a circuit for a single view.
///
/// # Safety
/// All table handles must be valid pointers or null.
#[allow(clippy::too_many_arguments)]
pub unsafe fn compile_view(
    view_id: u64,
    sys_nodes: *mut Table,
    sys_edges: *mut Table,
    sys_node_cols: *mut Table,
    sys_nodes_schema: &SchemaDescriptor,
    sys_edges_schema: &SchemaDescriptor,
    sys_node_cols_schema: &SchemaDescriptor,
    view_dir: &str,
    view_table_id: u32,
    view_schema: &SchemaDescriptor,
    ext_tables: &[ExternalTable],
) -> Result<CompileOutput, i32> {
    let mut loaded = load_circuit(
        sys_nodes, sys_nodes_schema,
        sys_edges, sys_edges_schema,
        sys_node_cols, sys_node_cols_schema,
        view_id, *view_schema,
    ).ok_or(-1)?;
    if loaded.nodes.is_empty() {
        return Err(-1);
    }
    if topo_sort(&mut loaded).is_err() {
        return Err(-2);
    }

    let ann = annotate(&loaded, ext_tables);

    let mut owned_expr_progs_for_rw: Vec<Box<ExprProgram>> = Vec::new();
    let mut rw = Rewrites {
        skip_nodes: HashSet::new(),
        fold_finalize: HashMap::new(),
        folded_maps: HashMap::new(),
    };
    opt_distinct(&loaded, &ann, &mut rw);
    opt_fold_reduce_map(&loaded, &mut rw, &mut owned_expr_progs_for_rw);

    let mut exchange_nid: Option<i32> = None;
    for &nid in &loaded.ordered {
        if matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ExchangeShard { .. })) {
            exchange_nid = Some(nid);
            break;
        }
    }

    if let Some(ex_nid) = exchange_nid {
        let mut pre_ordered = Vec::new();
        let mut post_ordered = Vec::new();
        let mut found_exchange = false;
        for &nid in &loaded.ordered {
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

        let mut exchange_input_nid: i32 = -1;
        for &(src, dst, port) in &loaded.edges {
            if dst == ex_nid && port == PORT_IN {
                exchange_input_nid = src;
                break;
            }
        }

        let pre_nids: HashSet<i32> = pre_ordered.iter().copied().collect();
        let (rw_pre, pre_progs, rw_post, post_progs) =
            split_fold_programs(rw, owned_expr_progs_for_rw, &pre_nids);

        let pre = build_plan(
            &loaded, &rw_pre, &pre_ordered, ext_tables,
            view_dir, view_table_id, view_id,
            if exchange_input_nid >= 0 { Some(exchange_input_nid) } else { None },
            None,
            pre_progs,
        ).ok_or(-3)?;

        if pre.out_reg < 0 || pre.out_reg as usize >= pre.vm.program.reg_meta.len() {
            return Err(-3);
        }
        let exchange_schema = pre.vm.program.reg_meta[pre.out_reg as usize].schema;

        let post = build_plan(
            &loaded, &rw_post, &post_ordered, ext_tables,
            view_dir, view_table_id, view_id,
            None,
            Some((ex_nid, exchange_schema)),
            post_progs,
        ).ok_or(-4)?;

        let source_reg_map = pre.source_reg_map.iter()
            .map(|(&tid, &reg)| (tid, reg as u16)).collect();

        Ok(CompileOutput {
            pre_vm: pre.vm,
            post_vm: Some(post.vm),
            exchange_in_schema: Some(exchange_schema),
            pre_num_regs: pre.num_regs,
            pre_in_reg: pre.in_reg as u16,
            pre_out_reg: pre.out_reg as u16,
            post_num_regs: post.num_regs,
            post_in_reg: post.in_reg as u16,
            post_out_reg: post.out_reg as u16,
            source_reg_map,
            co_partitioned: ann.co_partitioned,
            pre_ext_trace_regs: pre.ext_trace_regs,
            post_ext_trace_regs: post.ext_trace_regs,
        })
    } else {
        let ordered = loaded.ordered.clone();
        let plan = build_plan(
            &loaded, &rw, &ordered, ext_tables,
            view_dir, view_table_id, view_id,
            None, None,
            owned_expr_progs_for_rw,
        ).ok_or(-5)?;

        let source_reg_map = plan.source_reg_map.iter()
            .map(|(&tid, &reg)| (tid, reg as u16)).collect();

        Ok(CompileOutput {
            pre_vm: plan.vm,
            post_vm: None,
            exchange_in_schema: None,
            pre_num_regs: plan.num_regs,
            pre_in_reg: plan.in_reg as u16,
            pre_out_reg: plan.out_reg as u16,
            post_num_regs: 0,
            post_in_reg: 0,
            post_out_reg: 0,
            source_reg_map,
            co_partitioned: ann.co_partitioned,
            pre_ext_trace_regs: plan.ext_trace_regs,
            post_ext_trace_regs: Vec::new(),
        })
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topo_sort_simple() {
        let mut loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: {
                let mut m = HashMap::new();
                m.insert(0, gnitz_wire::OpNode::ScanDelta(0));
                m.insert(1, gnitz_wire::OpNode::Filter(None));
                m.insert(2, gnitz_wire::OpNode::IntegrateSink);
                m
            },
            edges: vec![(0, 1, 0), (1, 2, 0)],
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        };
        assert!(topo_sort(&mut loaded).is_ok());
        assert_eq!(loaded.ordered, vec![0, 1, 2]);
    }

    #[test]
    fn test_topo_sort_cycle() {
        let mut loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: {
                let mut m = HashMap::new();
                m.insert(0, gnitz_wire::OpNode::Filter(None));
                m.insert(1, gnitz_wire::OpNode::Filter(None));
                m
            },
            edges: vec![(0, 1, 0), (1, 0, 0)],
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        };
        assert!(topo_sort(&mut loaded).is_err());
    }

    #[test]
    fn test_merge_schemas_for_join() {
        let left = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let right = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        let joined = merge_schemas_for_join(&left, &right);
        assert_eq!(joined.num_columns(), 3); // PK + left_I64 + right_STRING
        assert_eq!(joined.columns[0].type_code, type_code::U128);
        assert_eq!(joined.columns[1].type_code, type_code::I64);
        assert_eq!(joined.columns[2].type_code, type_code::STRING);
    }

    #[test]
    fn test_merge_schemas_for_join_outer() {
        let left = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let right = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let joined = merge_schemas_for_join_outer(&left, &right);
        assert_eq!(joined.num_columns(), 3);
        assert_eq!(joined.columns[2].nullable, 1); // right side nullable
    }

    #[test]
    fn test_agg_output_type() {
        assert_eq!(agg_output_type(AggOp::Count, TypeCode::I64), type_code::I64);
        assert_eq!(agg_output_type(AggOp::Sum, TypeCode::F64), type_code::F64);
        assert_eq!(agg_output_type(AggOp::Sum, TypeCode::I32), type_code::I64);
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::I32), type_code::I64);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::F32), type_code::F64);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::String), type_code::I64);
    }

    #[test]
    fn test_sequential_copy_projection() {
        use crate::expr::ExprProgram;
        let make = |code: Vec<i64>| ExprProgram::new(code, 4, 0, vec![]);
        // COPY_COL has opcode 34
        assert!(make(vec![34, 9, 1, 0, 34, 9, 2, 1]).is_sequential_copy_projection());
        assert!(!make(vec![34, 9, 2, 0, 34, 9, 1, 1]).is_sequential_copy_projection()); // wrong order
        assert!(!make(vec![34, 9, 1, 0, 35, 9, 2, 1]).is_sequential_copy_projection()); // wrong opcode
        assert!(!make(vec![]).is_sequential_copy_projection()); // empty
    }

    #[test]
    fn test_identity_map_detection() {
        let a = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let b = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        assert!(schemas_physically_identical(&a, &b));

        let c = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        assert!(!schemas_physically_identical(&a, &c));
    }

    #[test]
    fn test_build_reduce_output_schema_natural_pk() {
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::U64, 0), // group col
                SchemaColumn::new(type_code::I64, 0), // agg col
            ],
            &[0],
        );
        let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2] }];
        let out = build_reduce_output_schema(&input, &[1], &aggs);
        // Natural PK (single U64 group col) → [U64_PK, I64_agg]
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.columns[0].type_code, type_code::U64);
        assert_eq!(out.columns[1].type_code, type_code::I64);
    }

    #[test]
    fn test_build_reduce_output_schema_synthetic_pk() {
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0), // group col
                SchemaColumn::new(type_code::I64, 0),    // agg col
            ],
            &[0],
        );
        let aggs = vec![AggDescriptor { col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2] }];
        let out = build_reduce_output_schema(&input, &[1], &aggs);
        // Synthetic PK (STRING group col) → [U128_hash, STRING_group, I64_count]
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.columns[0].type_code, type_code::U128);
        assert_eq!(out.columns[1].type_code, type_code::STRING);
        assert_eq!(out.columns[2].type_code, type_code::I64);
    }

    #[test]
    fn test_split_fold_programs_routes_to_pre() {
        let code = vec![0i64; 4];
        let prog = ExprProgram::new(code, 1, 0, Vec::new());
        let progs: Vec<Box<ExprProgram>> = vec![Box::new(prog)];

        let mut fold_finalize = HashMap::new();
        fold_finalize.insert(1i32, 0usize);

        let mut folded_maps = HashMap::new();
        folded_maps.insert(2i32, 1i32);

        let rw = Rewrites {
            skip_nodes: HashSet::new(),
            fold_finalize,
            folded_maps,
        };

        let mut pre_nids = HashSet::new();
        pre_nids.insert(1i32);

        let (rw_pre, pre_progs, rw_post, post_progs) =
            split_fold_programs(rw, progs, &pre_nids);

        assert_eq!(pre_progs.len(), 1);
        assert_eq!(post_progs.len(), 0);
        assert_eq!(rw_pre.fold_finalize.get(&1), Some(&0usize));
        assert!(!rw_post.fold_finalize.contains_key(&1));
        assert!(rw_pre.folded_maps.contains_key(&2));
        assert!(rw_post.folded_maps.contains_key(&2));
    }

    #[test]
    fn test_build_plan_returns_none_when_child_table_fails() {
        // Circuit: SCAN(0) → DISTINCT(1) → INTEGRATE(2)
        // An invalid view_dir forces create_child_table to fail inside emit_node.
        let mut loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: {
                let mut m = HashMap::new();
                m.insert(0, gnitz_wire::OpNode::ScanDelta(99));
                m.insert(1, gnitz_wire::OpNode::Distinct);
                m.insert(2, gnitz_wire::OpNode::IntegrateSink);
                m
            },
            edges: vec![(0, 1, 0), (1, 2, 0)],
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        };
        topo_sort(&mut loaded).unwrap();

        // Provide an external table so ScanDelta finds its schema and sets source_reg_map.
        let in_schema = SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::U64, 0)],
            &[0],
        );
        let ext_tables = [ExternalTable { table_id: 99, schema: in_schema }];
        let rw = Rewrites {
            skip_nodes:    HashSet::new(),
            fold_finalize: HashMap::new(),
            folded_maps:   HashMap::new(),
        };

        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded, &rw, &ordered, &ext_tables,
            "/nonexistent_gnitz_test_path_xyz_abc",
            0, 99, None, None, vec![],
        );
        assert!(result.is_none(), "build_plan must return None when child table creation fails");
    }

    #[test]
    #[should_panic(expected = "join output schema exceeds")]
    fn test_merge_schemas_for_join_column_overflow() {
        use crate::schema::MAX_COLUMNS;
        let half = MAX_COLUMNS / 2 + 2;
        let make = |n: usize| {
            let mut cols = [SchemaColumn::new(0, 0); MAX_COLUMNS];
            cols[0] = SchemaColumn::new(type_code::U128, 0);
            for i in 1..n {
                cols[i] = SchemaColumn::new(type_code::I64, 0);
            }
            SchemaDescriptor::new(&cols[..n], &[0])
        };
        merge_schemas_for_join(&make(half), &make(half));
    }

    // ── helpers shared by join tests ─────────────────────────────────────

    fn two_col_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        )
    }

    fn make_loaded(
        nodes: HashMap<i32, gnitz_wire::OpNode>,
        edges: Vec<(i32, i32, i32)>,
    ) -> LoadedCircuit {
        let mut lc = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes,
            edges,
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        };
        topo_sort(&mut lc).expect("test circuit must be acyclic");
        lc
    }

    fn empty_rw() -> Rewrites {
        Rewrites {
            skip_nodes:    HashSet::new(),
            fold_finalize: HashMap::new(),
            folded_maps:   HashMap::new(),
        }
    }

    // ── ScanTrace join-trace-side: no add_scan_trace when feeding port=1 ──

    /// A ScanTrace node feeding a Join via PORT_TRACE must not emit add_scan_trace
    /// or allocate an extra delta register.
    #[test]
    fn test_scan_trace_join_trace_side_no_extra_reg() {
        // Circuit: ScanDelta(10) --port0--> Join(DT)(2)
        //          ScanTrace(20) --port1--> Join(DT)(2)
        //          Join(2) --port0--> IntegrateSink(3)
        let schema = two_col_schema();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN_A),  // delta side
            (1, 2, PORT_TRACE), // trace side — must NOT emit add_scan_trace
            (2, 3, PORT_IN),
        ];
        let loaded = make_loaded(nodes, edges);

        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded, &empty_rw(), &ordered, &ext,
            "", 0, 1,
            Some(2), // bypass out_schema mismatch check; sink_reg already set by IntegrateSink
            None, vec![],
        );
        let plan = result.expect("build_plan must succeed for this circuit");

        // The trace-side ScanTrace (node 1) uses reg_id as the trace register;
        // no extra delta register is allocated for it.  The minimum register
        // count is: one per node (4) + zero extras from ScanTrace on trace side.
        // (There are no Distinct/Reduce/Delay nodes adding extras.)
        assert!(
            plan.num_regs == 4,
            "expected exactly 4 regs (one per node, no extra for trace-side ScanTrace), got {}",
            plan.num_regs
        );
    }

    /// A ScanTrace node that does NOT feed a join's TRACE port must still emit
    /// add_scan_trace and allocate an extra delta register.
    #[test]
    fn test_scan_trace_non_join_side_emits_scan_trace() {
        // Circuit: ScanDelta(10) --port0--> Union(2)
        //          ScanTrace(20) --port1--> Union(2)   [port=1 but Union ≠ Join → NOT join-trace-side]
        //          Union(2) --port0--> IntegrateSink(3)
        //
        // ScanDelta provides input_delta_reg_id via source_reg_map.
        // ScanTrace feeds Union on PORT_IN_B (=1), but Union is not in
        // {Join, AntiJoin, SemiJoin, SeekTrace}, so is_join_trace_side = false →
        // add_scan_trace is emitted and one extra delta register is allocated.
        let schema = two_col_schema();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Union);
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN_A),  // ScanDelta → Union left
            (1, 2, PORT_IN_B),  // ScanTrace → Union right (port=1, not a join)
            (2, 3, PORT_IN),
        ];

        let loaded = make_loaded(nodes, edges);
        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded, &empty_rw(), &ordered, &ext,
            "", 0, 1,
            Some(2), // bypass out_schema mismatch check
            None, vec![],
        );
        let plan = result.expect("build_plan must succeed");

        // 4 nodes → base regs 0-3, plus 1 extra delta reg for ScanTrace.
        assert!(
            plan.num_regs == 5,
            "expected 5 regs (4 base + 1 extra delta for non-join-side ScanTrace), got {}",
            plan.num_regs
        );
    }

    // ── compute_join_shard_map covers ScanDelta (SQL-planner join pattern) ──

    /// compute_join_shard_map must find ScanDelta → Map(reindex) chains, not
    /// just ScanTrace sources.
    #[test]
    fn test_compute_join_shard_map_scan_delta() {
        use gnitz_wire::{MapKind, OpNode};

        // Minimal two-sided SQL join circuit skeleton:
        //   ScanDelta(left_tid=10) → Map(reindex_col=1) → Join → IntegrateSink
        //   ScanDelta(right_tid=20) → Map(reindex_col=0) → Join
        let dummy_blob: Vec<u8> = vec![
            0x47, 0x4e, 0x49, 0x54, // magic
            0x01,                   // version
            0, 0, 0, 0, 0,          // reserved
            0, 0, 0, 0,             // code_len = 0
            0,                      // nconst = 0
        ];
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(10));
        nodes.insert(1, OpNode::Map(MapKind::Expression {
            program: dummy_blob.clone(),
            reindex_col: Some(1),
        }));
        nodes.insert(2, OpNode::ScanDelta(20));
        nodes.insert(3, OpNode::Map(MapKind::Expression {
            program: dummy_blob,
            reindex_col: Some(0),
        }));
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = make_loaded(nodes, edges);

        let map = compute_join_shard_map(&loaded);

        assert_eq!(
            map.get(&10).cloned().unwrap_or_default(),
            vec![1],
            "left side (source 10) must map to reindex_col=1"
        );
        assert_eq!(
            map.get(&20).cloned().unwrap_or_default(),
            vec![0],
            "right side (source 20) must map to reindex_col=0"
        );
    }

    /// Pure ScanTrace sources (Python-API joins) must also appear in the map.
    #[test]
    fn test_compute_join_shard_map_scan_trace_unchanged() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob: Vec<u8> = vec![
            0x47, 0x4e, 0x49, 0x54, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(10));
        nodes.insert(1, OpNode::ScanTrace(20));
        nodes.insert(2, OpNode::Map(MapKind::Expression {
            program: dummy_blob,
            reindex_col: Some(2),
        }));
        nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(4, OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN),     // ScanDelta → reindex Map
            (1, 3, PORT_TRACE),  // ScanTrace → join trace port (no reindex)
            (2, 3, PORT_IN_A),
            (3, 4, PORT_IN),
        ];
        let loaded = make_loaded(nodes, edges);

        let map = compute_join_shard_map(&loaded);

        // ScanDelta(10) → Map(reindex_col=2) must be found.
        assert_eq!(
            map.get(&10).cloned().unwrap_or_default(),
            vec![2],
            "ScanDelta source must be in join_shard_map"
        );
        // ScanTrace(20) has no downstream reindex Map — must NOT appear.
        assert!(
            !map.contains_key(&20),
            "ScanTrace-only source with no reindex Map must not be in join_shard_map"
        );
    }

    // ── Finding 1: ExchangeGather must forward its output to exchange-input reg ──

    /// ExchangeGather is a logical passthrough: at runtime the exchange mechanism
    /// injects gathered data into the exchange-input register (`plan.in_reg`).
    /// GatherReduce reads from ExchangeGather's output, so ExchangeGather's output
    /// register must alias the exchange-input register — not a separate unwritten one.
    #[test]
    fn test_exchange_gather_routes_to_exchange_input_register() {
        let schema = two_col_schema();
        let dir = tempfile::tempdir().unwrap();

        // Post-exchange circuit: ExchangeShard(0) → ExchangeGather(1) → GatherReduce(2).
        // We build only the post-plan: post_ordered = [1, 2], exchange_input = Some((0, schema)).
        let loaded = {
            let mut nodes = HashMap::new();
            nodes.insert(0, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![] });
            nodes.insert(1, gnitz_wire::OpNode::ExchangeGather);
            nodes.insert(2, gnitz_wire::OpNode::GatherReduce);
            make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)])
        };

        let post_ordered = vec![1, 2];
        let plan = build_plan(
            &loaded, &empty_rw(), &post_ordered, &[],
            dir.path().to_str().unwrap(), 0, 1,
            Some(2), // GatherReduce(2) is the output node; skips schema mismatch check
            Some((0, schema)),
            vec![],
        ).expect("post-plan must compile");

        let gather_in_reg = plan.vm.program.instructions.iter().find_map(|instr| {
            if let crate::vm::Instr::GatherReduce { in_reg, .. } = instr {
                Some(*in_reg)
            } else {
                None
            }
        }).expect("post-plan must contain a GatherReduce instruction");

        assert_eq!(
            gather_in_reg as i32, plan.in_reg,
            "GatherReduce reads from register {} but exchange data arrives at register {}: \
             ExchangeGather did not forward its output to the exchange-input register",
            gather_in_reg, plan.in_reg
        );
    }

    // ── Finding 2: load_circuit must return None for null system-table pointers ──

    /// Null system-table pointers are a programming error; the engine always supplies
    /// valid handles.  load_circuit must return None so callers get an explicit failure
    /// rather than silently reading an incomplete circuit and producing wrong results.
    #[test]
    fn test_load_circuit_returns_none_for_null_system_tables() {
        let result = load_circuit(
            std::ptr::null_mut(), &SchemaDescriptor::default(),
            std::ptr::null_mut(), &SchemaDescriptor::default(),
            std::ptr::null_mut(), &SchemaDescriptor::default(),
            0,
            SchemaDescriptor::default(),
        );
        assert!(
            result.is_none(),
            "null system-table pointers must return None, not a silently empty circuit"
        );
    }
}
