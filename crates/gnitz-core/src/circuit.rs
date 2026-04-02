use crate::types::{
    CircuitGraph,
    OPCODE_SCAN_TRACE, OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT, OPCODE_INTEGRATE,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_TRACE,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
    PARAM_FUNC_ID, PARAM_AGG_FUNC_ID, PARAM_AGG_COL_IDX,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_EXPR_NUM_REGS, PARAM_EXPR_RESULT_REG,
    PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
    PARAM_REINDEX_COL, PARAM_JOIN_SOURCE_TABLE, PARAM_CONST_STR_BASE, PARAM_TABLE_ID,
    PARAM_AGG_COUNT, PARAM_AGG_SPEC_BASE,
};
use crate::expr::ExprProgram;

pub type NodeId = u64;

/// Fluent builder for DBSP circuit graphs, producing a `CircuitGraph` for
/// `GnitzClient::create_view_with_circuit`.
pub struct CircuitBuilder {
    view_id:           u64,
    primary_source_id: u64,
    next_node_id:      u64,
    next_edge_id:      u64,
    nodes:      Vec<(u64, u64)>,
    edges:      Vec<(u64, u64, u64, u64)>,
    sources:    Vec<(u64, u64)>,
    params:     Vec<(u64, u64, u64)>,
    group_cols: Vec<(u64, u64)>,
    const_strings: Vec<(u64, u64, String)>,
}

impl CircuitBuilder {
    pub fn new(view_id: u64, primary_source_id: u64) -> Self {
        CircuitBuilder {
            view_id,
            primary_source_id,
            next_node_id: 1,
            next_edge_id: 1,
            nodes:      Vec::new(),
            edges:      Vec::new(),
            sources:    Vec::new(),
            params:     Vec::new(),
            group_cols: Vec::new(),
            const_strings: Vec::new(),
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

    /// Primary input delta node (SCAN_TRACE; source table resolved from dep).
    pub fn input_delta(&mut self) -> NodeId {
        let nid = self.alloc_node(OPCODE_SCAN_TRACE);
        self.sources.push((nid, 0));
        nid
    }

    /// Secondary trace-side source (SCAN_TRACE; reads from `table_id`).
    pub fn trace_scan(&mut self, table_id: u64) -> NodeId {
        let nid = self.alloc_node(OPCODE_SCAN_TRACE);
        self.sources.push((nid, table_id));
        nid
    }

    /// Filter operator. Always emits `PARAM_FUNC_ID=0`.
    /// If `expr` is `Some`, also embeds the bytecode program as params.
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

    /// Map with expression program (Phase 4: computed projections).
    /// Discriminator: `PARAM_EXPR_NUM_REGS > 0` on a MAP node = expr-map mode.
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

    /// Map/projection operator. Emits `PARAM_FUNC_ID=0` and one param per column.
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

    /// Binary union: A→port 0, B→port 1.
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

    /// Binary join primitive: delta→port A, trace_node→port TRACE.
    fn binary_join(&mut self, opcode: u64, delta: NodeId, trace_node: NodeId) -> NodeId {
        let nid = self.alloc_node(opcode);
        self.connect(delta, nid, PORT_IN_A);
        self.connect(trace_node, nid, PORT_TRACE);
        nid
    }

    /// Binary join with internal trace_scan(trace_table_id).
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

    /// Reduce with automatic shard insertion (required for multi-worker correctness).
    pub fn reduce(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_func_id: u64,
        agg_col_idx: usize,
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

    /// Multi-aggregate reduce with automatic shard insertion.
    /// `agg_specs`: list of (agg_func_id, agg_col_idx) tuples.
    pub fn reduce_multi(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
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

    /// Exchange shard: routes rows to workers by hashing the given columns.
    pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId {
        let nid = self.alloc_node(OPCODE_EXCHANGE_SHARD);
        self.connect(input, nid, PORT_IN);
        for (i, &col) in shard_cols.iter().enumerate() {
            self.params.push((nid, PARAM_SHARD_COL_BASE + i as u64, col as u64));
        }
        nid
    }

    /// Exchange gather: collects results from all workers to `worker_id`.
    pub fn gather(&mut self, input: NodeId, worker_id: u64) -> NodeId {
        let nid = self.alloc_node(OPCODE_EXCHANGE_GATHER);
        self.connect(input, nid, PORT_IN);
        self.params.push((nid, PARAM_GATHER_WORKER, worker_id));
        nid
    }

    /// Tagged primary input for join views.
    /// Creates a SCAN_TRACE with source=0 and PARAM_JOIN_SOURCE_TABLE.
    pub fn input_delta_tagged(&mut self, source_table_id: u64) -> NodeId {
        let nid = self.alloc_node(OPCODE_SCAN_TRACE);
        self.sources.push((nid, 0));
        self.params.push((nid, PARAM_JOIN_SOURCE_TABLE, source_table_id));
        nid
    }

    /// Map with PK reindexing (equijoin pre-indexing).
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

    /// Intermediate trace integration (equijoin accumulator).
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

    /// Add a string constant associated with a node (for expr programs).
    pub fn add_const_string(&mut self, node_id: NodeId, index: u32, value: String) {
        let slot = PARAM_CONST_STR_BASE + index as u64;
        self.const_strings.push((node_id, slot, value));
    }

    /// Integrate (sink) operator. Does NOT emit PARAM_TABLE_ID — evaluate_dag
    /// is the sole write path on the server side.
    pub fn sink(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OPCODE_INTEGRATE);
        self.connect(input, nid, PORT_IN);
        nid
    }

    /// Finalises the circuit and collects table dependencies.
    pub fn build(self) -> CircuitGraph {
        let mut deps: Vec<u64> = Vec::new();
        if self.primary_source_id > 0 {
            deps.push(self.primary_source_id);
        }
        // Note: self.sources entries with table_id > 0 are trace-only SCAN_TRACE nodes
        // (created via trace_scan()).  They act as read-only lookup tables and have no
        // delta-input path in the circuit, so they must NOT appear in deps.  Changes to
        // those tables do not trigger view recalculation, and backfilling them as if they
        // were delta sources would bind their rows to the primary-input register, producing
        // spurious output rows (e.g. a_val=0 when only B and C pre-existed in a chained join).
        // True secondary delta sources are registered via input_delta_tagged(), which sets
        // PARAM_JOIN_SOURCE_TABLE and appears in the params loop below.

        // Include secondary delta-input sources (equijoin, multi-input circuits).
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
            dependencies:      deps,
        }
    }
}
