# rpython_tests/helpers/circuit_builder.py

from gnitz.core.opcodes import *

class NodeHandle(object):
    def __init__(self, node_id):
        self.node_id = node_id

class CircuitGraph(object):
    def __init__(self, view_id, primary_source_id, nodes, edges, sources, params, group_cols,
                 output_col_defs, dependencies):
        self.view_id = view_id
        self.primary_source_id = primary_source_id
        self.nodes = nodes
        self.edges = edges
        self.sources = sources
        self.params = params
        self.group_cols = group_cols
        self.output_col_defs = output_col_defs
        self.dependencies = dependencies

class CircuitBuilder(object):
    """
    Improved Fluent API for constructing DBSP circuit graphs.
    Enforces primary source registration and explicit stream intent.
    """

    def __init__(self, view_id, primary_source_id):
        self.view_id = view_id
        self.primary_source_id = primary_source_id
        self._next_node_id = 1
        self._next_edge_id = 1
        self._nodes = []
        self._edges = []
        self._sources = []
        self._params = []
        self._group_cols = []
        self._sink_node = None

    def _alloc_node(self, opcode):
        nid = self._next_node_id
        self._next_node_id += 1
        self._nodes.append((nid, opcode))
        return NodeHandle(nid)

    def _connect(self, src_handle, dst_handle, dst_port):
        eid = self._next_edge_id
        self._next_edge_id += 1
        self._edges.append((eid, src_handle.node_id, dst_handle.node_id, dst_port))

    def input_delta(self):
        """Declares the primary reactive delta stream (table_id=0)."""
        handle = self._alloc_node(OPCODE_SCAN_TRACE)
        self._sources.append((handle.node_id, 0))
        return handle

    def trace_scan(self, table_id):
        """Declares a stateful read from a persistent table."""
        if table_id <= 0:
            from gnitz.core.errors import LayoutError
            raise LayoutError("trace_scan requires a valid table_id > 0")
        handle = self._alloc_node(OPCODE_SCAN_TRACE)
        self._sources.append((handle.node_id, table_id))
        return handle

    def filter(self, input_handle, func_id=0, expr_code=None, expr_num_regs=0, expr_result_reg=0):
        handle = self._alloc_node(OPCODE_FILTER)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_FUNC_ID, func_id))
        if expr_code is not None:
            self._params.append((handle.node_id, PARAM_EXPR_NUM_REGS, expr_num_regs))
            self._params.append((handle.node_id, PARAM_EXPR_RESULT_REG, expr_result_reg))
            for i in range(len(expr_code)):
                self._params.append((handle.node_id, PARAM_EXPR_BASE + i, expr_code[i]))
        return handle

    def map(self, input_handle, func_id=0, projection=None):
        handle = self._alloc_node(OPCODE_MAP)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_FUNC_ID, func_id))
        if projection is not None:
            for i in range(len(projection)):
                self._params.append((handle.node_id, PARAM_PROJ_BASE + i, projection[i]))
        return handle

    def negate(self, input_handle):
        handle = self._alloc_node(OPCODE_NEGATE)
        self._connect(input_handle, handle, PORT_IN)
        return handle

    def union(self, handle_a, handle_b):
        handle = self._alloc_node(OPCODE_UNION)
        self._connect(handle_a, handle, PORT_IN_A)
        self._connect(handle_b, handle, PORT_IN_B)
        return handle

    def delay(self, input_handle):
        handle = self._alloc_node(OPCODE_DELAY)
        self._connect(input_handle, handle, PORT_IN)
        return handle

    def join(self, delta_handle, trace_table_id):
        trace_src = self.trace_scan(trace_table_id)
        handle = self._alloc_node(OPCODE_JOIN_DELTA_TRACE)
        self._connect(delta_handle, handle, PORT_DELTA)
        self._connect(trace_src, handle, PORT_TRACE)
        return handle

    def join_delta_delta(self, handle_a, handle_b):
        handle = self._alloc_node(OPCODE_JOIN_DELTA_DELTA)
        self._connect(handle_a, handle, PORT_IN_A)
        self._connect(handle_b, handle, PORT_IN_B)
        return handle

    def distinct(self, input_handle):
        handle = self._alloc_node(OPCODE_DISTINCT)
        self._connect(input_handle, handle, PORT_IN_DISTINCT)
        return handle

    def reduce(self, input_handle, agg_func_id, group_by_cols, agg_col_idx=0):
        handle = self._alloc_node(OPCODE_REDUCE)
        self._connect(input_handle, handle, PORT_IN_REDUCE)
        self._params.append((handle.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
        self._params.append((handle.node_id, PARAM_AGG_COL_IDX, agg_col_idx))
        for col_idx in group_by_cols:
            self._group_cols.append((handle.node_id, col_idx))
        return handle

    def anti_join(self, delta_handle, trace_table_id):
        trace_src = self.trace_scan(trace_table_id)
        handle = self._alloc_node(OPCODE_ANTI_JOIN_DELTA_TRACE)
        self._connect(delta_handle, handle, PORT_DELTA)
        self._connect(trace_src, handle, PORT_TRACE)
        return handle

    def anti_join_delta_delta(self, handle_a, handle_b):
        handle = self._alloc_node(OPCODE_ANTI_JOIN_DELTA_DELTA)
        self._connect(handle_a, handle, PORT_IN_A)
        self._connect(handle_b, handle, PORT_IN_B)
        return handle

    def semi_join(self, delta_handle, trace_table_id):
        trace_src = self.trace_scan(trace_table_id)
        handle = self._alloc_node(OPCODE_SEMI_JOIN_DELTA_TRACE)
        self._connect(delta_handle, handle, PORT_DELTA)
        self._connect(trace_src, handle, PORT_TRACE)
        return handle

    def semi_join_delta_delta(self, handle_a, handle_b):
        handle = self._alloc_node(OPCODE_SEMI_JOIN_DELTA_DELTA)
        self._connect(handle_a, handle, PORT_IN_A)
        self._connect(handle_b, handle, PORT_IN_B)
        return handle

    def shard(self, input_handle, shard_columns):
        handle = self._alloc_node(OPCODE_EXCHANGE_SHARD)
        self._connect(input_handle, handle, PORT_EXCHANGE_IN)
        for i in range(len(shard_columns)):
            self._params.append((handle.node_id, PARAM_SHARD_COL_BASE + i, shard_columns[i]))
        return handle

    def gather(self, input_handle, worker_id=0):
        handle = self._alloc_node(OPCODE_EXCHANGE_GATHER)
        self._connect(input_handle, handle, PORT_EXCHANGE_IN)
        self._params.append((handle.node_id, PARAM_GATHER_WORKER, worker_id))
        return handle

    def seek(self, trace_handle, key_handle):
        handle = self._alloc_node(OPCODE_SEEK_TRACE)
        self._connect(trace_handle, handle, PORT_TRACE)
        self._connect(key_handle, handle, PORT_IN)
        return handle

    def sink(self, input_handle, target_table_id):
        handle = self._alloc_node(OPCODE_INTEGRATE)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_TABLE_ID, target_table_id))
        self._sink_node = handle
        return handle

    def build(self, output_col_defs):
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
            output_col_defs=output_col_defs,
            dependencies=deps,
        )
