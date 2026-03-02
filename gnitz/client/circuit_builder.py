# gnitz/client/circuit_builder.py
#
# Client-side circuit graph construction.
# Produces raw data rows for the five _circuit_* system tables.
# Imports nothing from gnitz.vm, gnitz.storage, or gnitz.catalog.

from gnitz.core.opcodes import *


class NodeHandle(object):
    """
    Opaque reference to a node in the circuit graph.
    The only user-visible token returned by CircuitBuilder methods.
    """
    def __init__(self, node_id):
        self.node_id = node_id


class CircuitGraph(object):
    """
    Immutable description of a circuit graph, ready for serialization.
    Returned by CircuitBuilder.build().
    """
    def __init__(self, view_id, nodes, edges, sources, params, group_cols,
                 output_col_defs, dependencies):
        self.view_id = view_id
        # nodes: list of (node_id, opcode)
        self.nodes = nodes
        # edges: list of (edge_id, src_node, dst_node, dst_port)
        self.edges = edges
        # sources: list of (node_id, table_id)
        self.sources = sources
        # params: list of (node_id, slot, value)
        self.params = params
        # group_cols: list of (node_id, col_idx)
        self.group_cols = group_cols
        # output_col_defs: list of (name, type_code) for the view's output schema
        self.output_col_defs = output_col_defs
        # dependencies: list of table_ids this view reads from (for _view_deps)
        self.dependencies = dependencies


class CircuitBuilder(object):
    """
    Fluent API for constructing DBSP circuit graphs.

    Returns NodeHandle objects from each operator method. NodeHandles are
    passed as arguments to connect operators. No register IDs, no
    current_reg_idx threading, no VM objects.

    Example:
        b = CircuitBuilder(view_id=42)
        orders = b.source(orders_table_id)
        filtered = b.filter(orders, func_id=AMOUNT_PRED)
        joined = b.join(filtered, customers_table_id)
        b.sink(joined, output_table_id)
        graph = b.build(output_col_defs=[...])
    """

    def __init__(self, view_id):
        self.view_id = view_id
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

    def source(self, table_id=0):
        """
        A leaf node representing an input stream.
        table_id=0: the view's incoming delta batch.
        table_id>0: a persistent table read as a trace.
        """
        handle = self._alloc_node(OPCODE_SCAN_TRACE)
        self._sources.append((handle.node_id, table_id))
        return handle

    def filter(self, input_handle, func_id):
        handle = self._alloc_node(OPCODE_FILTER)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_FUNC_ID, func_id))
        return handle

    def map(self, input_handle, func_id):
        handle = self._alloc_node(OPCODE_MAP)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_FUNC_ID, func_id))
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
        """
        Delta-Trace join. Creates an implicit source node for the trace side.
        """
        trace_src = self.source(trace_table_id)
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

    def reduce(self, input_handle, agg_func_id, group_by_cols):
        handle = self._alloc_node(OPCODE_REDUCE)
        self._connect(input_handle, handle, PORT_IN_REDUCE)
        self._params.append((handle.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
        for col_idx in group_by_cols:
            self._group_cols.append((handle.node_id, col_idx))
        return handle

    def scan(self, table_id, chunk_limit=1000):
        """Explicit scan node with chunk limit (for chunked reads)."""
        handle = self._alloc_node(OPCODE_SCAN_TRACE)
        self._sources.append((handle.node_id, table_id))
        self._params.append((handle.node_id, PARAM_CHUNK_LIMIT, chunk_limit))
        return handle

    def seek(self, trace_handle, key_handle):
        handle = self._alloc_node(OPCODE_SEEK_TRACE)
        self._connect(trace_handle, handle, PORT_TRACE)
        self._connect(key_handle, handle, PORT_IN)
        return handle

    def sink(self, input_handle, target_table_id):
        """
        Terminal integration node. Records the sink for dependency tracking.
        """
        handle = self._alloc_node(OPCODE_INTEGRATE)
        self._connect(input_handle, handle, PORT_IN)
        self._params.append((handle.node_id, PARAM_TABLE_ID, target_table_id))
        self._sink_node = handle
        return handle

    def build(self, output_col_defs):
        """
        Finalizes the graph and returns an immutable CircuitGraph.

        output_col_defs: list of (column_name: str, type_code: int) tuples
            describing the output schema. The caller derives this from the
            chain of operator schemas (type-propagation), which the client
            is responsible for since it knows the table schemas.

        Returns a CircuitGraph ready for serialization to ZSetBatch rows.
        """
        deps = []
        for node_id, table_id in self._sources:
            if table_id > 0 and table_id not in deps:
                deps.append(table_id)

        return CircuitGraph(
            view_id=self.view_id,
            nodes=list(self._nodes),
            edges=list(self._edges),
            sources=list(self._sources),
            params=list(self._params),
            group_cols=list(self._group_cols),
            output_col_defs=output_col_defs,
            dependencies=deps,
        )
