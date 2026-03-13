"""Client-side circuit graph builder for constructing DBSP circuits."""

from gnitz_client.types import (
    OPCODE_SCAN_TRACE, OPCODE_FILTER, OPCODE_MAP, OPCODE_INTEGRATE,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    PORT_IN, PORT_TRACE, PORT_EXCHANGE_IN,
    PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
)

# Opcodes used by CircuitBuilder (not all are in types.py)
OPCODE_NEGATE = 3
OPCODE_UNION = 4
OPCODE_JOIN_DELTA_TRACE = 5
OPCODE_JOIN_DELTA_DELTA = 6
OPCODE_DISTINCT = 10
OPCODE_REDUCE = 9
OPCODE_DELAY = 8
OPCODE_ANTI_JOIN_DELTA_TRACE = 16
OPCODE_ANTI_JOIN_DELTA_DELTA = 17
OPCODE_SEMI_JOIN_DELTA_TRACE = 18
OPCODE_SEMI_JOIN_DELTA_DELTA = 19

PORT_IN_A = 0
PORT_IN_B = 1
PORT_DELTA = 0
PORT_IN_DISTINCT = 0
PORT_IN_REDUCE = 0
PORT_TRACE_IN = 1
PARAM_FUNC_ID = 0
PARAM_AGG_FUNC_ID = 1
PARAM_AGG_COL_IDX = 6
PARAM_PROJ_BASE = 32
PARAM_EXPR_BASE = 64
PARAM_EXPR_NUM_REGS = 7
PARAM_EXPR_RESULT_REG = 8


class NodeHandle:
    def __init__(self, node_id: int):
        self.node_id = node_id


class CircuitGraph:
    """Holds the complete graph data ready for persistence via the client."""
    def __init__(
        self,
        view_id: int,
        primary_source_id: int,
        nodes: list,
        edges: list,
        sources: list,
        params: list,
        group_cols: list,
        dependencies: list,
    ):
        self.view_id = view_id
        self.primary_source_id = primary_source_id
        self.nodes = nodes
        self.edges = edges
        self.sources = sources
        self.params = params
        self.group_cols = group_cols
        self.dependencies = dependencies


class CircuitBuilder:
    """Fluent API for constructing DBSP circuit graphs on the client side."""

    def __init__(self, view_id: int, primary_source_id: int):
        self.view_id = view_id
        self.primary_source_id = primary_source_id
        self._next_node_id = 1
        self._next_edge_id = 1
        self._nodes: list[tuple[int, int]] = []
        self._edges: list[tuple[int, int, int, int]] = []
        self._sources: list[tuple[int, int]] = []
        self._params: list[tuple[int, int, int]] = []
        self._group_cols: list[tuple[int, int]] = []

    def _alloc_node(self, opcode: int) -> NodeHandle:
        nid = self._next_node_id
        self._next_node_id += 1
        self._nodes.append((nid, opcode))
        return NodeHandle(nid)

    def _connect(self, src: NodeHandle, dst: NodeHandle, port: int):
        eid = self._next_edge_id
        self._next_edge_id += 1
        self._edges.append((eid, src.node_id, dst.node_id, port))

    def input_delta(self) -> NodeHandle:
        handle = self._alloc_node(OPCODE_SCAN_TRACE)
        self._sources.append((handle.node_id, 0))
        return handle

    def trace_scan(self, table_id: int) -> NodeHandle:
        handle = self._alloc_node(OPCODE_SCAN_TRACE)
        self._sources.append((handle.node_id, table_id))
        return handle

    def filter(self, input_handle: NodeHandle, func_id: int = 0,
               expr: dict | None = None) -> NodeHandle:
        handle = self._alloc_node(OPCODE_FILTER)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_FUNC_ID, func_id))
        if expr is not None:
            self._params.append((handle.node_id, PARAM_EXPR_NUM_REGS, expr['num_regs']))
            self._params.append((handle.node_id, PARAM_EXPR_RESULT_REG, expr['result_reg']))
            for i, word in enumerate(expr['code']):
                self._params.append((handle.node_id, PARAM_EXPR_BASE + i, word))
        return handle

    def map(self, input_handle: NodeHandle, func_id: int = 0, projection: list[int] | None = None) -> NodeHandle:
        handle = self._alloc_node(OPCODE_MAP)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_FUNC_ID, func_id))
        if projection is not None:
            for i, col in enumerate(projection):
                self._params.append((handle.node_id, PARAM_PROJ_BASE + i, col))
        return handle

    def negate(self, input_handle: NodeHandle) -> NodeHandle:
        handle = self._alloc_node(OPCODE_NEGATE)
        self._connect(input_handle, handle, PORT_IN)
        return handle

    def union(self, handle_a: NodeHandle, handle_b: NodeHandle) -> NodeHandle:
        handle = self._alloc_node(OPCODE_UNION)
        self._connect(handle_a, handle, PORT_IN_A)
        self._connect(handle_b, handle, PORT_IN_B)
        return handle

    def delay(self, input_handle: NodeHandle) -> NodeHandle:
        handle = self._alloc_node(OPCODE_DELAY)
        self._connect(input_handle, handle, PORT_IN)
        return handle

    def join(self, delta_handle: NodeHandle, trace_table_id: int) -> NodeHandle:
        trace_src = self.trace_scan(trace_table_id)
        handle = self._alloc_node(OPCODE_JOIN_DELTA_TRACE)
        self._connect(delta_handle, handle, PORT_DELTA)
        self._connect(trace_src, handle, PORT_TRACE)
        return handle

    def reduce(self, input_handle: NodeHandle, group_by_cols: list[int],
               agg_func_id: int = 0, agg_col_idx: int = 0) -> NodeHandle:
        # Auto-insert exchange shard by group columns for multi-worker correctness
        sharded = self.shard(input_handle, group_by_cols)
        handle = self._alloc_node(OPCODE_REDUCE)
        self._connect(sharded, handle, PORT_IN_REDUCE)
        self._params.append((handle.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
        self._params.append((handle.node_id, PARAM_AGG_COL_IDX, agg_col_idx))
        for col in group_by_cols:
            self._group_cols.append((handle.node_id, col))
        return handle

    def anti_join(self, delta_handle: NodeHandle, trace_table_id: int) -> NodeHandle:
        trace_src = self.trace_scan(trace_table_id)
        handle = self._alloc_node(OPCODE_ANTI_JOIN_DELTA_TRACE)
        self._connect(delta_handle, handle, PORT_DELTA)
        self._connect(trace_src, handle, PORT_TRACE)
        return handle

    def semi_join(self, delta_handle: NodeHandle, trace_table_id: int) -> NodeHandle:
        trace_src = self.trace_scan(trace_table_id)
        handle = self._alloc_node(OPCODE_SEMI_JOIN_DELTA_TRACE)
        self._connect(delta_handle, handle, PORT_DELTA)
        self._connect(trace_src, handle, PORT_TRACE)
        return handle

    def distinct(self, input_handle: NodeHandle) -> NodeHandle:
        handle = self._alloc_node(OPCODE_DISTINCT)
        self._connect(input_handle, handle, PORT_IN_DISTINCT)
        return handle

    def shard(self, input_handle: NodeHandle, shard_columns: list[int]) -> NodeHandle:
        handle = self._alloc_node(OPCODE_EXCHANGE_SHARD)
        self._connect(input_handle, handle, PORT_EXCHANGE_IN)
        for i, col in enumerate(shard_columns):
            self._params.append((handle.node_id, PARAM_SHARD_COL_BASE + i, col))
        return handle

    def gather(self, input_handle: NodeHandle, worker_id: int = 0) -> NodeHandle:
        handle = self._alloc_node(OPCODE_EXCHANGE_GATHER)
        self._connect(input_handle, handle, PORT_EXCHANGE_IN)
        self._params.append((handle.node_id, PARAM_GATHER_WORKER, worker_id))
        return handle

    def sink(self, input_handle: NodeHandle, target_table_id: int) -> NodeHandle:
        handle = self._alloc_node(OPCODE_INTEGRATE)
        self._connect(input_handle, handle, PORT_IN)
        # PARAM_TABLE_ID intentionally not emitted — evaluate_dag is the sole write path
        return handle

    def build(self) -> CircuitGraph:
        deps = []
        if self.primary_source_id > 0 and self.primary_source_id not in deps:
            deps.append(self.primary_source_id)
        for node_id, table_id in self._sources:
            if table_id > 0 and table_id not in deps:
                deps.append(table_id)

        return CircuitGraph(
            view_id=self.view_id,
            primary_source_id=self.primary_source_id,
            nodes=list(self._nodes),
            edges=list(self._edges),
            sources=list(self._sources),
            params=list(self._params),
            group_cols=list(self._group_cols),
            dependencies=deps,
        )
