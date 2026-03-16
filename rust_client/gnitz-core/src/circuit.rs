use crate::types::{
    CircuitGraph,
    OPCODE_SCAN_TRACE, OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT, OPCODE_INTEGRATE,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_TRACE,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
    PARAM_FUNC_ID, PARAM_AGG_FUNC_ID, PARAM_AGG_COL_IDX,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_EXPR_NUM_REGS, PARAM_EXPR_RESULT_REG,
    PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
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

    /// Join: delta→port 0, internal trace_scan(trace_table_id)→port 1.
    pub fn join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        let trace = self.trace_scan(trace_table_id);
        let nid = self.alloc_node(OPCODE_JOIN_DELTA_TRACE);
        self.connect(delta, nid, PORT_IN_A);
        self.connect(trace, nid, PORT_TRACE);
        nid
    }

    /// Anti-join: delta→port 0, internal trace_scan(trace_table_id)→port 1.
    pub fn anti_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        let trace = self.trace_scan(trace_table_id);
        let nid = self.alloc_node(OPCODE_ANTI_JOIN_DELTA_TRACE);
        self.connect(delta, nid, PORT_IN_A);
        self.connect(trace, nid, PORT_TRACE);
        nid
    }

    /// Semi-join: delta→port 0, internal trace_scan(trace_table_id)→port 1.
    pub fn semi_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        let trace = self.trace_scan(trace_table_id);
        let nid = self.alloc_node(OPCODE_SEMI_JOIN_DELTA_TRACE);
        self.connect(delta, nid, PORT_IN_A);
        self.connect(trace, nid, PORT_TRACE);
        nid
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

    /// Integrate (sink) operator. Does NOT emit PARAM_TABLE_ID — evaluate_dag
    /// is the sole write path on the server side.
    pub fn sink(&mut self, input: NodeId, _view_id: u64) -> NodeId {
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
        for &(_, table_id) in &self.sources {
            if table_id > 0 && !deps.contains(&table_id) {
                deps.push(table_id);
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
            dependencies:      deps,
        }
    }
}
