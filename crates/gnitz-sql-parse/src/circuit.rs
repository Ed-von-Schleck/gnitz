//! Circuit graph + fluent builder. Pure data structure — no client
//! coupling — so `gnitz-engine` can construct circuits during
//! server-side view compile and embed them directly into the format-v2
//! migration row.
//!
//! Mirrors `gnitz_core::circuit` almost verbatim; the shape is extended
//! with `output_col_defs` so a compiled view circuit is self-describing
//! (the server needs the view's output schema to register it as a
//! table-like object). `CircuitGraph` derives serde so it can be
//! bincoded straight into `MigrationRow.compiled_circuits`.

use crate::expr_program::ExprProgram;

use gnitz_wire::{
    OPCODE_SCAN_TRACE, OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT, OPCODE_INTEGRATE,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_TRACE,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER, OPCODE_NULL_EXTEND,
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
    PARAM_FUNC_ID, PARAM_AGG_FUNC_ID, PARAM_AGG_COL_IDX,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_EXPR_NUM_REGS, PARAM_EXPR_RESULT_REG,
    PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
    PARAM_REINDEX_COL, PARAM_JOIN_SOURCE_TABLE, PARAM_CONST_STR_BASE, PARAM_TABLE_ID,
    PARAM_AGG_COUNT, PARAM_AGG_SPEC_BASE,
    PARAM_KEY_ONLY, PARAM_NULL_EXTEND_COUNT, PARAM_NULL_EXTEND_COL_BASE,
};

pub type NodeId = u64;

/// Serializable description of a DBSP circuit graph, enriched with the
/// declared output schema so the recipient (server-side migration
/// apply) has everything needed to materialise the view.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CircuitGraph {
    pub view_id:           u64,
    pub primary_source_id: u64,
    pub nodes:         Vec<(u64, u64)>,           // (node_id, opcode)
    pub edges:         Vec<(u64, u64, u64, u64)>, // (edge_id, src, dst, port)
    pub sources:       Vec<(u64, u64)>,           // (node_id, table_id)
    pub params:        Vec<(u64, u64, u64)>,      // (node_id, slot, value)
    pub group_cols:    Vec<(u64, u64)>,           // (node_id, col_idx)
    pub const_strings: Vec<(u64, u64, String)>,   // (node_id, slot, string)
    pub output_col_defs: Vec<(String, u8)>,       // (col_name, type_code)
    pub dependencies:    Vec<u64>,                // dep table_ids
}

pub struct CircuitBuilder {
    view_id:           u64,
    primary_source_id: u64,
    next_node_id:      u64,
    next_edge_id:      u64,
    nodes:             Vec<(u64, u64)>,
    edges:             Vec<(u64, u64, u64, u64)>,
    sources:           Vec<(u64, u64)>,
    params:            Vec<(u64, u64, u64)>,
    group_cols:        Vec<(u64, u64)>,
    const_strings:     Vec<(u64, u64, String)>,
}

impl CircuitBuilder {
    pub fn new(view_id: u64, primary_source_id: u64) -> Self {
        CircuitBuilder {
            view_id, primary_source_id,
            next_node_id: 1, next_edge_id: 1,
            nodes: Vec::new(), edges: Vec::new(), sources: Vec::new(),
            params: Vec::new(), group_cols: Vec::new(), const_strings: Vec::new(),
        }
    }

    fn alloc_node(&mut self, opcode: u64) -> NodeId {
        let nid = self.next_node_id;
        self.next_node_id += 1;
        self.nodes.push((nid, opcode));
        nid
    }

    fn connect(&mut self, src: NodeId, dst: NodeId, port: u64) {
        let eid = self.next_edge_id;
        self.next_edge_id += 1;
        self.edges.push((eid, src, dst, port));
    }

    pub fn input_delta(&mut self) -> NodeId {
        let nid = self.alloc_node(OPCODE_SCAN_TRACE);
        self.sources.push((nid, 0));
        nid
    }

    pub fn trace_scan(&mut self, table_id: u64) -> NodeId {
        let nid = self.alloc_node(OPCODE_SCAN_TRACE);
        self.sources.push((nid, table_id));
        nid
    }

    pub fn filter(&mut self, input: NodeId, expr: Option<ExprProgram>) -> NodeId {
        let nid = self.alloc_node(OPCODE_FILTER);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_FUNC_ID, 0));
        if let Some(e) = expr {
            self.params.push((nid, PARAM_EXPR_NUM_REGS,   e.num_regs   as u64));
            self.params.push((nid, PARAM_EXPR_RESULT_REG, e.result_reg as u64));
            for (i, &word) in e.code.iter().enumerate() {
                self.params.push((nid, PARAM_EXPR_BASE + i as u64, word as u64));
            }
            for (i, s) in e.const_strings.iter().enumerate() {
                self.const_strings.push((nid, PARAM_CONST_STR_BASE + i as u64, s.clone()));
            }
        }
        nid
    }

    pub fn map_expr(&mut self, input: NodeId, program: ExprProgram) -> NodeId {
        let nid = self.alloc_node(OPCODE_MAP);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_FUNC_ID, 0));
        self.params.push((nid, PARAM_EXPR_NUM_REGS, program.num_regs as u64));
        for (i, &word) in program.code.iter().enumerate() {
            self.params.push((nid, PARAM_EXPR_BASE + i as u64, word as u64));
        }
        for (i, s) in program.const_strings.iter().enumerate() {
            self.const_strings.push((nid, PARAM_CONST_STR_BASE + i as u64, s.clone()));
        }
        nid
    }

    pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId {
        let nid = self.alloc_node(OPCODE_MAP);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_FUNC_ID, 0));
        for (i, &col) in projection.iter().enumerate() {
            self.params.push((nid, PARAM_PROJ_BASE + i as u64, col as u64));
        }
        nid
    }

    pub fn negate(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_NEGATE);
        self.connect(input, nid, PORT_IN);
        nid
    }

    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_UNION);
        self.connect(a, nid, PORT_IN_A);
        self.connect(b, nid, PORT_IN_B);
        nid
    }

    pub fn delay(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_DELAY);
        self.connect(input, nid, PORT_IN);
        nid
    }

    pub fn distinct(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_DISTINCT);
        self.connect(input, nid, PORT_IN);
        nid
    }

    fn binary_join(&mut self, opcode: u64, delta: NodeId, trace_node: NodeId) -> NodeId {
        let nid = self.alloc_node(opcode);
        self.connect(delta, nid, PORT_IN_A);
        self.connect(trace_node, nid, PORT_TRACE);
        nid
    }

    fn binary_join_scan(&mut self, opcode: u64, delta: NodeId, trace_table_id: u64) -> NodeId {
        let trace = self.trace_scan(trace_table_id);
        self.binary_join(opcode, delta, trace)
    }

    pub fn join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OPCODE_JOIN_DELTA_TRACE, delta, trace_table_id)
    }

    pub fn anti_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OPCODE_ANTI_JOIN_DELTA_TRACE, delta, trace_table_id)
    }

    pub fn semi_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OPCODE_SEMI_JOIN_DELTA_TRACE, delta, trace_table_id)
    }

    pub fn reduce(
        &mut self, input: NodeId, group_cols: &[usize],
        agg_func_id: u64, agg_col_idx: usize,
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        let nid = self.alloc_node(OPCODE_REDUCE);
        self.connect(sharded, nid, PORT_IN);
        self.params.push((nid, PARAM_AGG_FUNC_ID, agg_func_id));
        self.params.push((nid, PARAM_AGG_COL_IDX, agg_col_idx as u64));
        for &col in group_cols {
            self.group_cols.push((nid, col as u64));
        }
        nid
    }

    pub fn reduce_multi(
        &mut self, input: NodeId, group_cols: &[usize],
        agg_specs: &[(u64, usize)],
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        let nid = self.alloc_node(OPCODE_REDUCE);
        self.connect(sharded, nid, PORT_IN);
        self.params.push((nid, PARAM_AGG_COUNT, agg_specs.len() as u64));
        for (i, &(func_id, col_idx)) in agg_specs.iter().enumerate() {
            let packed = (func_id << 32) | (col_idx as u64);
            self.params.push((nid, PARAM_AGG_SPEC_BASE + i as u64, packed));
        }
        for &col in group_cols {
            self.group_cols.push((nid, col as u64));
        }
        nid
    }

    pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId {
        let nid = self.alloc_node(OPCODE_EXCHANGE_SHARD);
        self.connect(input, nid, PORT_IN);
        for (i, &col) in shard_cols.iter().enumerate() {
            self.params.push((nid, PARAM_SHARD_COL_BASE + i as u64, col as u64));
        }
        nid
    }

    pub fn gather(&mut self, input: NodeId, worker_id: u64) -> NodeId {
        let nid = self.alloc_node(OPCODE_EXCHANGE_GATHER);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_GATHER_WORKER, worker_id));
        nid
    }

    pub fn input_delta_tagged(&mut self, source_table_id: u64) -> NodeId {
        let nid = self.alloc_node(OPCODE_SCAN_TRACE);
        self.sources.push((nid, 0));
        self.params.push((nid, PARAM_JOIN_SOURCE_TABLE, source_table_id));
        nid
    }

    pub fn map_reindex(
        &mut self, input: NodeId, reindex_col: usize, program: ExprProgram,
    ) -> NodeId {
        let nid = self.alloc_node(OPCODE_MAP);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_FUNC_ID, 0));
        self.params.push((nid, PARAM_REINDEX_COL, reindex_col as u64));
        self.params.push((nid, PARAM_EXPR_NUM_REGS, program.num_regs as u64));
        for (i, &word) in program.code.iter().enumerate() {
            self.params.push((nid, PARAM_EXPR_BASE + i as u64, word as u64));
        }
        for (i, s) in program.const_strings.iter().enumerate() {
            self.const_strings.push((nid, PARAM_CONST_STR_BASE + i as u64, s.clone()));
        }
        nid
    }

    pub fn integrate_trace(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_INTEGRATE);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_TABLE_ID, 1));
        nid
    }

    pub fn join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OPCODE_JOIN_DELTA_TRACE, delta, trace_node)
    }

    pub fn anti_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OPCODE_ANTI_JOIN_DELTA_TRACE, delta, trace_node)
    }

    pub fn left_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OPCODE_JOIN_DELTA_TRACE_OUTER, delta, trace_table_id)
    }

    pub fn left_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OPCODE_JOIN_DELTA_TRACE_OUTER, delta, trace_node)
    }

    pub fn semi_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OPCODE_SEMI_JOIN_DELTA_TRACE, delta, trace_node)
    }

    pub fn map_key_only(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_MAP);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_FUNC_ID, 0));
        self.params.push((nid, PARAM_KEY_ONLY, 1));
        nid
    }

    pub fn null_extend(&mut self, input: NodeId, right_col_type_codes: &[u64]) -> NodeId {
        let nid = self.alloc_node(OPCODE_NULL_EXTEND);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_NULL_EXTEND_COUNT, right_col_type_codes.len() as u64));
        for (i, &tc) in right_col_type_codes.iter().enumerate() {
            self.params.push((nid, PARAM_NULL_EXTEND_COL_BASE + i as u64, tc));
        }
        nid
    }

    pub fn add_const_string(&mut self, node_id: NodeId, index: u32, value: String) {
        let slot = PARAM_CONST_STR_BASE + index as u64;
        self.const_strings.push((node_id, slot, value));
    }

    pub fn sink(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_INTEGRATE);
        self.connect(input, nid, PORT_IN);
        nid
    }

    /// Finalise the circuit. `output_col_defs` describes the view's
    /// declared output schema, used by the server to register the view
    /// as a readable table-like object.
    pub fn build(self, output_col_defs: Vec<(String, u8)>) -> CircuitGraph {
        let mut deps: Vec<u64> = Vec::new();
        if self.primary_source_id > 0 {
            deps.push(self.primary_source_id);
        }
        for &(_, slot, value) in &self.params {
            if slot == PARAM_JOIN_SOURCE_TABLE && value > 0 && !deps.contains(&value) {
                deps.push(value);
            }
        }
        CircuitGraph {
            view_id:           self.view_id,
            primary_source_id: self.primary_source_id,
            nodes:             self.nodes,
            edges:             self.edges,
            sources:           self.sources,
            params:            self.params,
            group_cols:        self.group_cols,
            const_strings:     self.const_strings,
            output_col_defs,
            dependencies:      deps,
        }
    }
}
