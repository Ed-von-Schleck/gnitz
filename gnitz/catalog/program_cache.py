# gnitz/catalog/program_cache.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask, r_int64

from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import opcodes
from gnitz.core.types import (
    merge_schemas_for_join, merge_schemas_for_join_outer,
    _build_map_output_schema, _build_reduce_output_schema,
    ColumnDefinition, TableSchema,
    TYPE_U128, TYPE_I64, TYPE_STRING, TYPE_F32, TYPE_F64,
)
from gnitz.core.errors import LayoutError
from gnitz.catalog import system_tables as sys
from gnitz.vm import runtime
from gnitz.dbsp import functions
from gnitz.storage import engine_ffi
from gnitz.storage.ephemeral_table import EphemeralTable

NULL_PREDICATE = functions.NullPredicate()
NULL_AGGREGATE = functions.NullAggregate()

_NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)
SCHEMA_DESC_SIZE = engine_ffi.SCHEMA_DESC_SIZE


def _make_group_idx_schema():
    """Schema for the reduce group secondary index: (ck: U128 [PK], spk_hi: I64)."""
    cols = newlist_hint(2)
    cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="ck"))
    cols.append(ColumnDefinition(TYPE_I64, is_nullable=False, name="spk_hi"))
    return TableSchema(cols, pk_index=0)


def _make_agg_value_idx_schema():
    """Schema for AggValueIndex: U128 PK only, no payload."""
    cols = newlist_hint(1)
    cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="ck"))
    return TableSchema(cols, pk_index=0)


def _pack_agg_descs(agg_funcs):
    """Pack agg_funcs into an 8-byte-per-entry C buffer matching Rust AggDescriptor.
    Returns (buf_ptr, count). Caller must free buf_ptr."""
    num_aggs = len(agg_funcs)
    buf_size = max(num_aggs * 8, 8)
    buf = lltype.malloc(rffi.CCHARP.TO, buf_size, flavor="raw")
    for ai in range(num_aggs):
        base = ai * 8
        v = agg_funcs[ai].col_idx
        buf[base] = chr(v & 0xFF)
        buf[base + 1] = chr((v >> 8) & 0xFF)
        buf[base + 2] = chr((v >> 16) & 0xFF)
        buf[base + 3] = chr((v >> 24) & 0xFF)
        buf[base + 4] = chr(agg_funcs[ai].agg_op & 0xFF)
        buf[base + 5] = chr(agg_funcs[ai].col_type_code & 0xFF)
        buf[base + 6] = chr(0)
        buf[base + 7] = chr(0)
    return buf, num_aggs


def _emit_simple_integrate(builder, in_reg_id, table_handle):
    """Emit an INTEGRATE with no GI or AVI."""
    engine_ffi._program_builder_add_integrate(
        builder,
        rffi.cast(rffi.USHORT, in_reg_id),
        table_handle,
        _NULL_HANDLE, rffi.cast(rffi.UINT, 0), rffi.cast(rffi.UCHAR, 0),
        _NULL_HANDLE, rffi.cast(rffi.INT, 0), rffi.cast(rffi.UCHAR, 0),
        lltype.nullptr(rffi.UINTP.TO), rffi.cast(rffi.UINT, 0),
        _NULL_HANDLE, rffi.cast(rffi.UINT, 0),
    )


# Indices into the mutable state list passed to _emit_node
_ST_NEXT_EXTRA_REG = 0
_ST_SINK_REG_ID = 1
_ST_INPUT_DELTA_REG_ID = 2


def _agg_value_idx_eligible(col_type):
    """True if the agg column type can be stored in AggValueIndex."""
    return (col_type.code != TYPE_U128.code
            and col_type.code != TYPE_STRING.code)


def _will_use_agg_value_idx(agg_func_id, agg_col_idx, schema):
    """True if this single-agg reduce will use AggValueIndex (Opt 2: skip tr_in_table)."""
    if agg_func_id != functions.AGG_MIN and agg_func_id != functions.AGG_MAX:
        return False
    agg_col_type = schema.columns[agg_col_idx].field_type
    return _agg_value_idx_eligible(agg_col_type)


def _open_system_scan(registry, table_id, view_id):
    """Returns (cursor, end_key) with cursor pre-seeked, or (None, 0) if table absent."""
    if not registry.has_id(table_id):
        return None, r_uint128(0)
    family = registry.get_by_id(table_id)
    cursor = family.store.create_cursor()
    end_key = r_uint128(r_uint64(view_id + 1)) << 64
    cursor.seek(r_uint64(0), r_uint64(intmask(r_uint64(view_id))))
    return cursor, end_key

def _topo_sort(nodes, edges):
    """Kahn's algorithm. Returns (ordered, outgoing, incoming, consumers) or raises LayoutError on cycle."""
    node_ids = newlist_hint(len(nodes))
    outgoing = {}
    incoming = {}
    consumers = {}
    in_degree = {}
    for nid, _ in nodes:
        node_ids.append(nid)
        outgoing[nid] = []
        incoming[nid] = []
        consumers[nid] = []
        in_degree[nid] = 0

    for _, src, dst, port in edges:
        outgoing[src].append((dst, port))
        incoming[dst].append(src)
        consumers[src].append(dst)
        in_degree[dst] += 1

    queue = []
    for nid in node_ids:
        if in_degree[nid] == 0:
            queue.append(nid)

    ordered = newlist_hint(len(node_ids))
    while queue:
        nid = queue.pop(0)
        ordered.append(nid)
        for dst, _ in outgoing[nid]:
            in_degree[dst] -= 1
            if in_degree[dst] == 0:
                queue.append(dst)

    if len(ordered) != len(node_ids):
        raise LayoutError("View graph contains cycles (not a DAG)")

    return ordered, outgoing, incoming, consumers


class CircuitGraph(object):
    """Wraps circuit graph data with derived topology for compiler passes."""
    def __init__(self, view_id, nodes, edges, sources, params,
                 str_params, group_cols, out_schema):
        self.view_id = view_id
        self.nodes = nodes           # [(nid, opcode), ...]
        self.edges = edges           # [(eid, src, dst, port), ...]
        self.sources = sources       # {nid: table_id}
        self.params = params         # {nid: {slot: int_value}}
        self.str_params = str_params # {nid: {slot: str_value}}
        self.group_cols = group_cols # {nid: [col_idx]}
        self.out_schema = out_schema
        self.opcode_of = {}
        self.ordered = []
        self.outgoing = {}
        self.incoming = {}
        self.consumers = {}


def _build_circuit_graph(view_id, nodes, edges, sources, params,
                         str_params, group_cols, out_schema):
    graph = CircuitGraph(view_id, nodes, edges, sources, params,
                         str_params, group_cols, out_schema)
    for nid, op in nodes:
        graph.opcode_of[nid] = op
    (graph.ordered, graph.outgoing,
     graph.incoming, graph.consumers) = _topo_sort(nodes, edges)
    return graph


class CircuitAnnotation(object):
    """Consolidates all pre-pass results for a circuit graph."""
    def __init__(self):
        self.trace_side_sources = {}  # {nid: True}
        self.in_schema = None
        self.join_shard_map = {}
        self.co_partitioned = {}
        self.is_distinct_at = {}      # {nid: True}


class Rewrites(object):
    """Collects optimization decisions before instruction emission."""
    def __init__(self):
        self.skip_nodes = {}     # {nid: True} — emit nothing, alias output reg to input
        self.fold_finalize = {}  # {reduce_nid: ExprProgram}
        self.folded_maps = {}    # {map_nid: reduce_nid}


def _compute_trace_sides(graph):
    """Identify SCAN_TRACE nodes that feed trace ports of join/seek ops."""
    trace_side_sources = {}
    for _, src, dst, port in graph.edges:
        if graph.opcode_of.get(src, -1) == opcodes.OPCODE_SCAN_TRACE:
            if port == opcodes.PORT_TRACE:
                dst_op = graph.opcode_of.get(dst, -1)
                if (dst_op == opcodes.OPCODE_JOIN_DELTA_TRACE or
                    dst_op == opcodes.OPCODE_JOIN_DELTA_TRACE_OUTER or
                    dst_op == opcodes.OPCODE_ANTI_JOIN_DELTA_TRACE or
                    dst_op == opcodes.OPCODE_SEMI_JOIN_DELTA_TRACE or
                    dst_op == opcodes.OPCODE_SEEK_TRACE):
                    trace_side_sources[src] = True
    return trace_side_sources


def _compute_join_shard_map(graph, trace_side_sources):
    """Build source_table_id → [reindex_col] for multi-input join circuits."""
    join_shard_map = {}
    for _, src, dst, _port in graph.edges:
        if graph.opcode_of.get(src, -1) == opcodes.OPCODE_SCAN_TRACE:
            src_params = graph.params.get(src, {})
            source_tid = src_params.get(opcodes.PARAM_JOIN_SOURCE_TABLE, 0)
            if source_tid > 0 and graph.opcode_of.get(dst, -1) == opcodes.OPCODE_MAP:
                dst_params = graph.params.get(dst, {})
                reindex_col = dst_params.get(opcodes.PARAM_REINDEX_COL, -1)
                if reindex_col >= 0:
                    join_shard_map[source_tid] = [reindex_col]
    return join_shard_map


def _compute_co_partitioned(graph, registry, join_shard_map):
    """Build source_table_id → True when join shard col == source PK."""
    co_partitioned = {}
    for source_tid in join_shard_map:
        cols = join_shard_map[source_tid]
        if registry.has_id(source_tid):
            src_pk = registry.get_by_id(source_tid).schema.pk_index
            if len(cols) == 1 and cols[0] == src_pk:
                co_partitioned[source_tid] = True
    return co_partitioned


def _produces_distinct(op):
    """True if this operator always outputs at-most-one-row-per-key."""
    return op == opcodes.OPCODE_REDUCE or op == opcodes.OPCODE_DISTINCT


def _preserves_distinct(op, node_params):
    """True if this operator outputs a distinct batch when its single input is distinct."""
    if op == opcodes.OPCODE_FILTER:
        return True
    if op == opcodes.OPCODE_MAP:
        return node_params.get(opcodes.PARAM_REINDEX_COL, -1) < 0
    return False


def _propagate_distinct(graph, ann):
    """Forward pass: mark nodes whose output is at-most-one-row-per-key."""
    for nid in graph.ordered:
        op = graph.opcode_of.get(nid, -1)
        if _produces_distinct(op):
            ann.is_distinct_at[nid] = True
        elif _preserves_distinct(op, graph.params.get(nid, {})):
            in_nids = graph.incoming.get(nid, [])
            if in_nids and ann.is_distinct_at.get(in_nids[0], False):
                ann.is_distinct_at[nid] = True


def _resolve_primary_input_schema(registry, program_id, fallback,
                                  circuit_sources, trace_side_sources):
    """Resolve the schema of the primary input (delta) source for a view.

    Uses CircuitSources + trace_side_sources to identify the primary
    input table.  The primary input node has table_id==0 (sentinel),
    so we find its actual table from DepTab — specifically, the
    dependency that is NOT a trace-side source table.
    """
    # Collect table IDs that feed trace ports
    trace_table_ids = {}
    for nid in trace_side_sources:
        tid = circuit_sources.get(nid, 0)
        if tid > 0:
            trace_table_ids[tid] = True

    # DepTab uses (view_id << 64 | dep_tid) PK — range scan by view_id
    cursor, end_key = _open_system_scan(registry, sys.DepTab.ID, program_id)
    if cursor is None:
        if fallback is not None:
            return fallback
        raise LayoutError("Cannot resolve primary input schema for view %d: "
                          "no DepTab" % program_id)
    any_dep_schema = None
    try:
        while cursor.is_valid() and cursor.key() < end_key:
            if cursor.weight() > r_int64(0):
                acc = cursor.get_accessor()
                dep_tid = intmask(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID))
                if dep_tid > 0 and registry.has_id(dep_tid):
                    dep_schema = registry.get_by_id(dep_tid).schema
                    if any_dep_schema is None:
                        any_dep_schema = dep_schema
                    if dep_tid not in trace_table_ids:
                        return dep_schema
            cursor.advance()
    finally:
        cursor.close()

    # Self-join fallback: primary and trace are the same table,
    # so all deps are in trace_table_ids. Any dep works.
    if any_dep_schema is not None:
        return any_dep_schema
    if fallback is not None:
        return fallback
    raise LayoutError("Cannot resolve primary input schema for view %d: "
                      "no source found" % program_id)


def _annotate(graph, registry):
    """Run all pre-passes and return a CircuitAnnotation."""
    ann = CircuitAnnotation()
    ann.trace_side_sources = _compute_trace_sides(graph)
    ann.in_schema = _resolve_primary_input_schema(
        registry, graph.view_id, graph.out_schema,
        graph.sources, ann.trace_side_sources)
    ann.join_shard_map = _compute_join_shard_map(graph, ann.trace_side_sources)
    ann.co_partitioned = _compute_co_partitioned(graph, registry, ann.join_shard_map)
    _propagate_distinct(graph, ann)
    return ann


def _extract_map_code(node_params):
    """Extract ExprProgram bytecode words from node params."""
    code = []
    idx = 0
    while (opcodes.PARAM_EXPR_BASE + idx) in node_params:
        code.append(r_int64(node_params[opcodes.PARAM_EXPR_BASE + idx]))
        idx += 1
    return code


def _all_copy_col_sequential(code):
    """True iff every instruction is COPY_COL with a1 = 1, 2, 3, ... in order.

    COPY_COL layout: [op, dst=type_code, a1=src_col_idx, a2=payload_col_idx]
    op_map inherits the PK from its input via commit_row(get_pk(i), ...);
    the ExprProgram only writes payload columns. a1 must start at 1
    (skipping PK at col 0) and increment by 1 per instruction.
    """
    from gnitz.dbsp.expr import EXPR_COPY_COL
    n = len(code)
    if n == 0 or n % 4 != 0:
        return False
    expected_src = 1
    i = 0
    while i < n:
        if intmask(code[i]) != EXPR_COPY_COL:
            return False
        if intmask(code[i + 2]) != expected_src:
            return False
        expected_src += 1
        i += 4
    return True


def _schemas_physically_identical(a, b):
    """True if two schemas have the same column count, PK index, and column types."""
    if len(a.columns) != len(b.columns):
        return False
    if a.pk_index != b.pk_index:
        return False
    for i in range(len(a.columns)):
        if a.columns[i].field_type.code != b.columns[i].field_type.code:
            return False
    return True


def _opt_distinct(graph, ann, rw):
    """Mark DISTINCT nodes that can be skipped because input is already distinct."""
    for nid, op in graph.nodes:
        if op == opcodes.OPCODE_DISTINCT:
            in_nids = graph.incoming.get(nid, [])
            if in_nids and ann.is_distinct_at.get(in_nids[0], False):
                rw.skip_nodes[nid] = True


def _opt_fold_reduce_map(graph, ann, rw):
    """Fold computed MAP nodes into their upstream REDUCE as a finalize ExprProgram."""
    for nid, op in graph.nodes:
        if op != opcodes.OPCODE_MAP:
            continue
        in_nids = graph.incoming.get(nid, [])
        if len(in_nids) != 1:
            continue
        reduce_nid = in_nids[0]
        if graph.opcode_of.get(reduce_nid, -1) != opcodes.OPCODE_REDUCE:
            continue
        if len(graph.consumers.get(reduce_nid, [])) != 1:
            continue
        node_params = graph.params.get(nid, {})
        num_regs = node_params.get(opcodes.PARAM_EXPR_NUM_REGS, 0)
        has_code = num_regs > 0 or opcodes.PARAM_EXPR_BASE in node_params
        if not has_code:
            continue
        code = _extract_map_code(node_params)
        if _all_copy_col_sequential(code):
            continue   # identity MAP handled inline in _emit_node
        if len(code) <= 63:
            from gnitz.dbsp.expr import ExprProgram
            prog = ExprProgram(code, num_regs, 0)
            rw.fold_finalize[reduce_nid] = prog
            rw.folded_maps[nid] = reduce_nid


def run_optimization_passes(graph, ann):
    """Run all optimization passes. Returns a Rewrites object."""
    rw = Rewrites()
    _opt_distinct(graph, ann, rw)
    _opt_fold_reduce_map(graph, ann, rw)
    return rw


class ProgramCache(object):
    """
    Caches execution plans for Reactive Views.
    """

    _immutable_fields_ = ["registry"]

    def __init__(self, registry):
        self.registry = registry
        self._cache = {}
        self._shard_cols_cache = {}
        self._exchange_info_cache = {}
        self._dep_map = {}
        self._source_map = {}
        self._dep_map_valid = False

    def invalidate(self, program_id):
        if program_id in self._cache:
            self._cache[program_id].close()
            del self._cache[program_id]
        if program_id in self._shard_cols_cache:
            del self._shard_cols_cache[program_id]
        if program_id in self._exchange_info_cache:
            del self._exchange_info_cache[program_id]
        self._dep_map_valid = False

    def invalidate_all(self):
        for plan in self._cache.values():
            plan.close()
        self._cache.clear()
        self._shard_cols_cache.clear()
        self._exchange_info_cache.clear()
        self._dep_map_valid = False

    def invalidate_dep_map(self):
        self._dep_map_valid = False

    def get_dep_map(self, registry):
        if self._dep_map_valid:
            return self._dep_map
        dep_map = {}
        source_map = {}
        if registry.has_id(sys.DepTab.ID):
            deps_family = registry.get_by_id(sys.DepTab.ID)
            cursor = deps_family.store.create_cursor()
            try:
                while cursor.is_valid():
                    if cursor.weight() > r_int64(0):
                        acc = cursor.get_accessor()
                        v_id    = intmask(r_uint64(acc.get_int(sys.DepTab.COL_VIEW_ID)))
                        dep_tid = intmask(r_uint64(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID)))
                        if dep_tid > 0:
                            if dep_tid not in dep_map:
                                dep_map[dep_tid] = []
                            if v_id not in dep_map[dep_tid]:
                                dep_map[dep_tid].append(v_id)
                            # inverse: view -> its sources
                            if v_id not in source_map:
                                source_map[v_id] = []
                            if dep_tid not in source_map[v_id]:
                                source_map[v_id].append(dep_tid)
                    cursor.advance()
            finally:
                cursor.close()
        self._dep_map = dep_map
        self._source_map = source_map
        self._dep_map_valid = True
        return dep_map

    def get_source_ids(self, view_id):
        """Return all direct source (table or view) IDs for this view."""
        self.get_dep_map(self.registry)
        if view_id in self._source_map:
            return self._source_map[view_id]
        return []

    def get_program(self, program_id):
        if program_id in self._cache:
            return self._cache[program_id]

        plan = self.compile_from_graph(program_id)
        if plan is not None:
            self._cache[program_id] = plan
        return plan

    def validate_graph_structure(self, graph):
        """
        Performs dry-run validation of a CircuitGraph before persistence.
        Ensures Kahn's topological sort succeeds and a sink exists.
        """
        if not graph.nodes:
            raise LayoutError("View graph contains no nodes")

        # 1. Check for Primary Input
        has_input = False
        for _, table_id in graph.sources:
            if table_id == 0:
                has_input = True
                break
        if not has_input:
            raise LayoutError("View graph missing primary input (table_id=0)")

        # 2. Check for Sink
        has_sink = False
        for _, opcode in graph.nodes:
            if opcode == opcodes.OPCODE_INTEGRATE:
                has_sink = True
                break
        if not has_sink:
            raise LayoutError("View graph missing sink (INTEGRATE node)")

        # 3. Cycle Detection (Topological Sort)
        _topo_sort(graph.nodes, graph.edges)

    def _load_nodes(self, view_id):
        cursor, end_key = _open_system_scan(self.registry, sys.CircuitNodesTab.ID, view_id)
        if cursor is None:
            return []
        result = newlist_hint(8)
        try:
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    node_id = intmask(r_uint64(pk))
                    opcode = intmask(acc.get_int(sys.CircuitNodesTab.COL_OPCODE))
                    result.append((node_id, opcode))
                cursor.advance()
        finally:
            cursor.close()
        return result

    def _load_edges(self, view_id):
        cursor, end_key = _open_system_scan(self.registry, sys.CircuitEdgesTab.ID, view_id)
        if cursor is None:
            return []
        result = newlist_hint(8)
        try:
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    edge_id = intmask(r_uint64(pk))
                    src = intmask(acc.get_int(sys.CircuitEdgesTab.COL_SRC_NODE))
                    dst = intmask(acc.get_int(sys.CircuitEdgesTab.COL_DST_NODE))
                    port = intmask(acc.get_int(sys.CircuitEdgesTab.COL_DST_PORT))
                    result.append((edge_id, src, dst, port))
                cursor.advance()
        finally:
            cursor.close()
        return result

    def _load_sources(self, view_id):
        cursor, end_key = _open_system_scan(self.registry, sys.CircuitSourcesTab.ID, view_id)
        if cursor is None:
            return {}
        result = {}
        try:
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    node_id = intmask(r_uint64(pk))
                    table_id = intmask(acc.get_int(sys.CircuitSourcesTab.COL_TABLE_ID))
                    result[node_id] = table_id
                cursor.advance()
        finally:
            cursor.close()
        return result

    def _load_params(self, view_id):
        cursor, end_key = _open_system_scan(self.registry, sys.CircuitParamsTab.ID, view_id)
        if cursor is None:
            return {}, {}
        result = {}
        str_params = {}
        try:
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    lo64 = r_uint64(intmask(pk))
                    node_id = intmask(lo64 >> 8)
                    slot = intmask(lo64 & r_uint64(0xFF))
                    value = intmask(acc.get_int(sys.CircuitParamsTab.COL_VALUE))
                    if node_id not in result: result[node_id] = {}
                    result[node_id][slot] = value
                    if not acc.is_null(sys.CircuitParamsTab.COL_STR_VALUE):
                        sv = sys.read_string(acc, sys.CircuitParamsTab.COL_STR_VALUE)
                        if node_id not in str_params: str_params[node_id] = {}
                        str_params[node_id][slot] = sv
                cursor.advance()
        finally:
            cursor.close()
        return result, str_params

    def _load_group_cols(self, view_id):
        cursor, end_key = _open_system_scan(self.registry, sys.CircuitGroupColsTab.ID, view_id)
        if cursor is None:
            return {}
        result = {}
        try:
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    lo64 = r_uint64(intmask(pk))
                    node_id = intmask(lo64 >> 16)
                    col_idx = intmask(lo64 & r_uint64(0xFFFF))
                    if node_id not in result: result[node_id] = newlist_hint(4)
                    result[node_id].append(col_idx)
                cursor.advance()
        finally:
            cursor.close()
        return result

    def get_shard_cols(self, view_id):
        """Extract shard columns for a view without full compilation.
        Reads only system tables — safe for master process."""
        if view_id in self._shard_cols_cache:
            return self._shard_cols_cache[view_id]
        nodes = self._load_nodes(view_id)
        params, _str_params = self._load_params(view_id)
        for nid, op in nodes:
            if op == opcodes.OPCODE_EXCHANGE_SHARD:
                node_params = params.get(nid, {})
                shard_cols = []
                idx = 0
                while (opcodes.PARAM_SHARD_COL_BASE + idx) in node_params:
                    shard_cols.append(node_params[opcodes.PARAM_SHARD_COL_BASE + idx])
                    idx += 1
                self._shard_cols_cache[view_id] = shard_cols
                return shard_cols
        self._shard_cols_cache[view_id] = []
        return []

    def get_join_shard_cols(self, view_id, source_id):
        """Get the shard column index for a join view's specific source.

        Scans the circuit graph for SCAN_TRACE(PARAM_JOIN_SOURCE_TABLE=source_id)
        → MAP(PARAM_REINDEX_COL=X) and returns [X].
        Returns [] if this view is not a join or source_id is not found.
        """
        nodes = self._load_nodes(view_id)
        params_dict, _str = self._load_params(view_id)
        edges = self._load_edges(view_id)

        # Find the SCAN_TRACE node for this source_id
        scan_nid = -1
        for nid, op in nodes:
            if op == opcodes.OPCODE_SCAN_TRACE:
                np = params_dict.get(nid, {})
                if np.get(opcodes.PARAM_JOIN_SOURCE_TABLE, 0) == source_id:
                    scan_nid = nid
                    break
        if scan_nid < 0:
            return []

        # Find the MAP node that this SCAN_TRACE feeds into
        for _eid, src, dst, _port in edges:
            if src == scan_nid:
                dst_params = params_dict.get(dst, {})
                reindex_col = dst_params.get(opcodes.PARAM_REINDEX_COL, -1)
                if reindex_col >= 0:
                    return [reindex_col]
        return []

    def get_exchange_info(self, view_id):
        """Returns (shard_cols, is_trivial, is_co_partitioned) for a view.

        is_trivial: SHARD's direct input is the primary input (INPUT → SHARD).
        is_co_partitioned: SHARD col == PK of the source table.
        """
        if view_id in self._exchange_info_cache:
            return self._exchange_info_cache[view_id]
        nodes = self._load_nodes(view_id)
        edges = self._load_edges(view_id)
        params, _str_params = self._load_params(view_id)
        sources = self._load_sources(view_id)

        # Compute in-degrees for each node in this view's circuit
        in_deg = {}
        for nid, _ in nodes:
            in_deg[nid] = 0
        for _eid, src, dst, _port in edges:
            in_deg[dst] = in_deg.get(dst, 0) + 1

        shard_cols = newlist_hint(2)
        is_trivial = False
        is_co_partitioned = False

        for nid, op in nodes:
            if op == opcodes.OPCODE_EXCHANGE_SHARD:
                node_params = params.get(nid, {})
                idx = 0
                while (opcodes.PARAM_SHARD_COL_BASE + idx) in node_params:
                    shard_cols.append(node_params[opcodes.PARAM_SHARD_COL_BASE + idx])
                    idx += 1

                # is_trivial: SHARD has exactly 1 incoming edge, and that source
                # node has in-degree 0 (INPUT → SHARD, no transforms).
                incoming_srcs = newlist_hint(2)
                for _eid2, src2, dst2, _port2 in edges:
                    if dst2 == nid:
                        incoming_srcs.append(src2)
                if len(incoming_srcs) == 1:
                    src_nid = incoming_srcs[0]
                    if in_deg.get(src_nid, 0) == 0:
                        is_trivial = True
                        src_tid = sources.get(src_nid, 0)
                        if (src_tid > 0 and len(shard_cols) == 1
                                and self.registry.has_id(src_tid)):
                            family = self.registry.get_by_id(src_tid)
                            if shard_cols[0] == family.schema.pk_index:
                                is_co_partitioned = True
                break  # only one EXCHANGE_SHARD per view

        result = (shard_cols, is_trivial, is_co_partitioned)
        self._exchange_info_cache[view_id] = result
        # Keep _shard_cols_cache coherent
        self._shard_cols_cache[view_id] = shard_cols
        return result

    def get_preloadable_views(self, source_table_id):
        """Return [(view_id, shard_cols)] for trivial, non-co-partitioned views."""
        dep_map = self.get_dep_map(self.registry)
        result = newlist_hint(4)
        for view_id in dep_map.get(source_table_id, []):
            shard_cols, is_trivial, is_co_partitioned = self.get_exchange_info(view_id)
            if is_trivial and not is_co_partitioned and len(shard_cols) > 0:
                result.append((view_id, shard_cols))
        return result

    def _emit_node(self, nid, op, reg_id, node_params, in_regs,
                   reg_schemas, reg_kinds, trace_reg_tables,
                   out_reg_of, sources, trace_side_sources,
                   group_cols, view_family, view_id, out_schema, in_schema,
                   builder, state, str_params, rw, source_reg_map=None):
        """Emit a single operator node via ProgramBuilder.

        Adds instructions directly to builder and populates reg_schemas/reg_kinds.
        state: mutable list [next_extra_reg, sink_reg_id, input_delta_reg_id].
        """
        if op == opcodes.OPCODE_SCAN_TRACE:
            table_id = sources.get(nid, 0)
            chunk_limit = node_params.get(opcodes.PARAM_CHUNK_LIMIT, 0)

            if table_id == 0:
                join_source_table = node_params.get(opcodes.PARAM_JOIN_SOURCE_TABLE, 0)
                if join_source_table > 0:
                    src_family = self.registry.get_by_id(join_source_table)
                    reg_schemas[reg_id] = src_family.schema
                    reg_kinds[reg_id] = 0
                    if source_reg_map is not None:
                        source_reg_map[join_source_table] = reg_id
                    return
                reg_schemas[reg_id] = in_schema
                reg_kinds[reg_id] = 0
                state[_ST_INPUT_DELTA_REG_ID] = reg_id
                return
            elif nid in trace_side_sources:
                family = self.registry.get_by_id(table_id)
                reg_schemas[reg_id] = family.schema
                reg_kinds[reg_id] = 1
                trace_reg_tables.append((reg_id, family.store))
                return
            else:
                family = self.registry.get_by_id(table_id)
                reg_schemas[reg_id] = family.schema
                reg_kinds[reg_id] = 1
                trace_reg_tables.append((reg_id, family.store))
                out_delta_id = state[_ST_NEXT_EXTRA_REG]
                state[_ST_NEXT_EXTRA_REG] += 1
                reg_schemas[out_delta_id] = family.schema
                reg_kinds[out_delta_id] = 0
                out_reg_of[nid] = out_delta_id
                engine_ffi._program_builder_add_scan_trace(
                    builder,
                    rffi.cast(rffi.USHORT, reg_id),
                    rffi.cast(rffi.USHORT, out_delta_id),
                    rffi.cast(rffi.INT, chunk_limit),
                )
                return

        elif op == opcodes.OPCODE_FILTER:
            in_schema_f = reg_schemas[in_regs[opcodes.PORT_IN]]
            reg_schemas[reg_id] = in_schema_f
            reg_kinds[reg_id] = 0
            num_regs = node_params.get(opcodes.PARAM_EXPR_NUM_REGS, 0)
            if num_regs > 0:
                result_reg = node_params.get(opcodes.PARAM_EXPR_RESULT_REG, 0)
                code = []
                idx = 0
                while (opcodes.PARAM_EXPR_BASE + idx) in node_params:
                    code.append(r_int64(node_params[opcodes.PARAM_EXPR_BASE + idx]))
                    idx += 1
                const_strings = []
                if nid in str_params:
                    node_str = str_params[nid]
                    sidx = 0
                    while (opcodes.PARAM_CONST_STR_BASE + sidx) in node_str:
                        const_strings.append(node_str[opcodes.PARAM_CONST_STR_BASE + sidx])
                        sidx += 1
                from gnitz.dbsp.expr import ExprProgram, ExprPredicate
                prog = ExprProgram(code, num_regs, result_reg, const_strings if const_strings else None)
                func = ExprPredicate(prog)
            else:
                func = NULL_PREDICATE
            engine_ffi._program_builder_add_filter(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN]),
                rffi.cast(rffi.USHORT, reg_id),
                func.get_func_handle(),
            )
            return

        elif op == opcodes.OPCODE_MAP:
            in_reg_schema = reg_schemas[in_regs[opcodes.PORT_IN]]
            reindex_col_check = node_params.get(opcodes.PARAM_REINDEX_COL, -1)
            num_regs = node_params.get(opcodes.PARAM_EXPR_NUM_REGS, 0)
            has_expr_code = num_regs > 0 or opcodes.PARAM_EXPR_BASE in node_params

            if reindex_col_check < 0 and has_expr_code:
                if _schemas_physically_identical(in_reg_schema, view_family.schema):
                    code = _extract_map_code(node_params)
                    if _all_copy_col_sequential(code):
                        out_reg_of[nid] = in_regs[opcodes.PORT_IN]
                        return

            if nid in rw.folded_maps:
                reduce_nid = rw.folded_maps[nid]
                out_reg_of[nid] = out_reg_of[reduce_nid]
                return

            if has_expr_code:
                code = []
                idx = 0
                while (opcodes.PARAM_EXPR_BASE + idx) in node_params:
                    code.append(r_int64(node_params[opcodes.PARAM_EXPR_BASE + idx]))
                    idx += 1
                const_strings = []
                if nid in str_params:
                    node_str = str_params[nid]
                    sidx = 0
                    while (opcodes.PARAM_CONST_STR_BASE + sidx) in node_str:
                        const_strings.append(node_str[opcodes.PARAM_CONST_STR_BASE + sidx])
                        sidx += 1
                from gnitz.dbsp.expr import ExprProgram, ExprMapFunction
                prog = ExprProgram(code, num_regs, 0, const_strings if const_strings else None)
                func = ExprMapFunction(prog)
                reindex_check = node_params.get(opcodes.PARAM_REINDEX_COL, -1)
                if reindex_check >= 0:
                    in_cols = in_reg_schema.columns
                    reindex_cols = newlist_hint(len(in_cols) + 1)
                    reindex_cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="__pk"))
                    for ci in range(len(in_cols)):
                        reindex_cols.append(in_cols[ci])
                    node_schema = TableSchema(reindex_cols, pk_index=0)
                else:
                    node_schema = out_schema
            elif opcodes.PARAM_PROJ_BASE in node_params:
                src_indices = []
                src_types = []
                idx = 0
                while (opcodes.PARAM_PROJ_BASE + idx) in node_params:
                    src_col = node_params[opcodes.PARAM_PROJ_BASE + idx]
                    src_indices.append(src_col)
                    src_types.append(in_reg_schema.columns[src_col].field_type.code)
                    idx += 1
                func = functions.UniversalProjection(src_indices, src_types)
                node_schema = _build_map_output_schema(in_reg_schema, src_indices)
            else:
                func = NULL_PREDICATE
                node_schema = in_reg_schema
            reindex_col = node_params.get(opcodes.PARAM_REINDEX_COL, -1)
            reg_schemas[reg_id] = node_schema
            reg_kinds[reg_id] = 0
            packed_out = engine_ffi.pack_schema(node_schema)
            engine_ffi._program_builder_add_map(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN]),
                rffi.cast(rffi.USHORT, reg_id),
                func.get_func_handle(),
                rffi.cast(rffi.VOIDP, packed_out),
                rffi.cast(rffi.INT, reindex_col),
            )
            lltype.free(packed_out, flavor="raw")
            return

        elif op == opcodes.OPCODE_NEGATE:
            in_reg_schema = reg_schemas[in_regs[opcodes.PORT_IN]]
            reg_schemas[reg_id] = in_reg_schema
            reg_kinds[reg_id] = 0
            engine_ffi._program_builder_add_negate(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN]),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_UNION:
            in_a_schema = reg_schemas[in_regs[opcodes.PORT_IN_A]]
            reg_schemas[reg_id] = in_a_schema
            reg_kinds[reg_id] = 0
            has_b = 1 if opcodes.PORT_IN_B in in_regs else 0
            in_b_id = in_regs.get(opcodes.PORT_IN_B, 0)
            engine_ffi._program_builder_add_union(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_A]),
                rffi.cast(rffi.USHORT, in_b_id),
                rffi.cast(rffi.INT, has_b),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_JOIN_DELTA_TRACE:
            delta_schema = reg_schemas[in_regs[opcodes.PORT_DELTA]]
            trace_schema = reg_schemas[in_regs[opcodes.PORT_TRACE]]
            join_schema = merge_schemas_for_join(delta_schema, trace_schema)
            reg_schemas[reg_id] = join_schema
            reg_kinds[reg_id] = 0
            packed_right = engine_ffi.pack_schema(trace_schema)
            engine_ffi._program_builder_add_join_dt(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_DELTA]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_TRACE]),
                rffi.cast(rffi.USHORT, reg_id),
                rffi.cast(rffi.VOIDP, packed_right),
            )
            lltype.free(packed_right, flavor="raw")
            return

        elif op == opcodes.OPCODE_JOIN_DELTA_TRACE_OUTER:
            delta_schema = reg_schemas[in_regs[opcodes.PORT_DELTA]]
            trace_schema = reg_schemas[in_regs[opcodes.PORT_TRACE]]
            outer_schema = merge_schemas_for_join_outer(delta_schema, trace_schema)
            reg_schemas[reg_id] = outer_schema
            reg_kinds[reg_id] = 0
            packed_right = engine_ffi.pack_schema(trace_schema)
            engine_ffi._program_builder_add_join_dt_outer(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_DELTA]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_TRACE]),
                rffi.cast(rffi.USHORT, reg_id),
                rffi.cast(rffi.VOIDP, packed_right),
            )
            lltype.free(packed_right, flavor="raw")
            return

        elif op == opcodes.OPCODE_JOIN_DELTA_DELTA:
            a_schema = reg_schemas[in_regs[opcodes.PORT_IN_A]]
            b_schema = reg_schemas[in_regs[opcodes.PORT_IN_B]]
            join_schema = merge_schemas_for_join(a_schema, b_schema)
            reg_schemas[reg_id] = join_schema
            reg_kinds[reg_id] = 0
            packed_right = engine_ffi.pack_schema(b_schema)
            engine_ffi._program_builder_add_join_dd(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_A]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_B]),
                rffi.cast(rffi.USHORT, reg_id),
                rffi.cast(rffi.VOIDP, packed_right),
            )
            lltype.free(packed_right, flavor="raw")
            return

        elif op == opcodes.OPCODE_DISTINCT:
            in_reg_schema = reg_schemas[in_regs[opcodes.PORT_IN_DISTINCT]]

            if nid in rw.skip_nodes:
                out_reg_of[nid] = in_regs[opcodes.PORT_IN_DISTINCT]
                return

            hist_schema = in_reg_schema
            history_table = view_family.store.create_child("_hist_%d_%d" % (view_id, nid), hist_schema)
            reg_schemas[reg_id] = hist_schema
            reg_kinds[reg_id] = 1
            trace_reg_tables.append((reg_id, history_table))
            out_delta_id = state[_ST_NEXT_EXTRA_REG]
            state[_ST_NEXT_EXTRA_REG] += 1
            reg_schemas[out_delta_id] = in_reg_schema
            reg_kinds[out_delta_id] = 0
            out_reg_of[nid] = out_delta_id
            engine_ffi._program_builder_add_distinct(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_DISTINCT]),
                rffi.cast(rffi.USHORT, reg_id),
                rffi.cast(rffi.USHORT, out_delta_id),
                history_table._handle,
            )
            return

        elif op == opcodes.OPCODE_REDUCE:
            in_reg_id = in_regs[opcodes.PORT_IN_REDUCE]
            in_reg_schema = reg_schemas[in_reg_id]
            agg_func_id = 0
            agg_col_idx = 0
            agg_count = node_params.get(opcodes.PARAM_AGG_COUNT, 0)
            if agg_count > 0:
                agg_funcs = newlist_hint(agg_count)
                for ai in range(agg_count):
                    packed = node_params.get(opcodes.PARAM_AGG_SPEC_BASE + ai, 0)
                    func_id = packed >> 32
                    col_idx = packed & 0xFFFFFFFF
                    col_type = in_reg_schema.columns[col_idx].field_type
                    agg_funcs.append(functions.UniversalAccumulator(col_idx, func_id, col_type))
            else:
                agg_func_id = node_params.get(opcodes.PARAM_AGG_FUNC_ID, 0)
                agg_col_idx = node_params.get(opcodes.PARAM_AGG_COL_IDX, 0)
                if agg_func_id > 0:
                    col_type = in_reg_schema.columns[agg_col_idx].field_type
                    agg_funcs = newlist_hint(1)
                    agg_funcs.append(functions.UniversalAccumulator(agg_col_idx, agg_func_id, col_type))
                else:
                    agg_funcs = newlist_hint(1)
                    agg_funcs.append(NULL_AGGREGATE)
            gcols = group_cols.get(nid, [])
            reduce_out_schema = _build_reduce_output_schema(in_reg_schema, gcols, agg_funcs)
            trace_table = view_family.store.create_child("_reduce_%d_%d" % (view_id, nid), reduce_out_schema)
            reg_schemas[reg_id] = reduce_out_schema
            reg_kinds[reg_id] = 1
            trace_reg_tables.append((reg_id, trace_table))
            raw_delta_id = state[_ST_NEXT_EXTRA_REG]
            state[_ST_NEXT_EXTRA_REG] += 1
            reg_schemas[raw_delta_id] = reduce_out_schema
            reg_kinds[raw_delta_id] = 0
            finalize_prog = rw.fold_finalize.get(nid, None)
            fin_delta_id = -1
            if finalize_prog is not None:
                fin_delta_id = state[_ST_NEXT_EXTRA_REG]
                state[_ST_NEXT_EXTRA_REG] += 1
                reg_schemas[fin_delta_id] = view_family.schema
                reg_kinds[fin_delta_id] = 0
                out_reg_of[nid] = fin_delta_id
            else:
                out_reg_of[nid] = raw_delta_id

            tr_in_reg_id = -1
            tr_in_table = None
            all_linear = True
            for af in agg_funcs:
                if not af.is_linear():
                    all_linear = False
                    break
            if opcodes.PORT_TRACE_IN in in_regs:
                tr_in_reg_id = in_regs[opcodes.PORT_TRACE_IN]
            elif not all_linear and not _will_use_agg_value_idx(
                    agg_func_id, agg_col_idx, in_reg_schema):
                tr_in_table = view_family.store.create_child(
                    "_reduce_in_%d_%d" % (view_id, nid), in_reg_schema
                )
                tr_in_reg_id = state[_ST_NEXT_EXTRA_REG]
                state[_ST_NEXT_EXTRA_REG] += 1
                reg_schemas[tr_in_reg_id] = in_reg_schema
                reg_kinds[tr_in_reg_id] = 1
                trace_reg_tables.append((tr_in_reg_id, tr_in_table))

            gi_table_handle = _NULL_HANDLE
            gi_col_idx = 0
            gi_col_type_code = 0
            if tr_in_table is not None and len(gcols) == 1:
                gc_col_idx = gcols[0]
                gc_type = in_reg_schema.columns[gc_col_idx].field_type
                if (gc_type.code != TYPE_U128.code
                        and gc_type.code != TYPE_STRING.code
                        and gc_type.code != TYPE_F32.code
                        and gc_type.code != TYPE_F64.code):
                    gi_table = EphemeralTable(
                        tr_in_table.directory + "_gidx",
                        "_gidx",
                        _make_group_idx_schema(),
                        table_id=0,
                    )
                    gi_table_handle = gi_table._handle
                    gi_col_idx = gc_col_idx
                    gi_col_type_code = gc_type.code

            avi_table_handle = _NULL_HANDLE
            avi_for_max = 0
            avi_agg_col_type_code = 0
            avi_input_schema_packed = _NULL_HANDLE
            avi_agg_col_idx = 0
            avi_group_cols_buf = lltype.nullptr(rffi.UINTP.TO)
            avi_num_group_cols = 0
            if agg_func_id == functions.AGG_MIN or agg_func_id == functions.AGG_MAX:
                agg_col_type = in_reg_schema.columns[agg_col_idx].field_type
                if _agg_value_idx_eligible(agg_col_type):
                    for_max = (agg_func_id == functions.AGG_MAX)
                    av_table = view_family.store.create_child(
                        "_avidx_%d_%d" % (view_id, nid),
                        _make_agg_value_idx_schema(),
                    )
                    avi_table_handle = av_table._handle
                    avi_for_max = 1 if for_max else 0
                    avi_agg_col_type_code = agg_col_type.code
                    avi_agg_col_idx = agg_col_idx
                    avi_num_group_cols = len(gcols)
                    avi_group_cols_buf = lltype.malloc(rffi.UINTP.TO, max(len(gcols), 1), flavor="raw")
                    for gi in range(len(gcols)):
                        avi_group_cols_buf[gi] = rffi.cast(rffi.UINT, gcols[gi])
                    avi_input_schema_packed = rffi.cast(rffi.VOIDP, engine_ffi.pack_schema(in_reg_schema))

            # AVI INTEGRATE must precede REDUCE
            if avi_table_handle != _NULL_HANDLE:
                engine_ffi._program_builder_add_integrate(
                    builder,
                    rffi.cast(rffi.USHORT, in_reg_id),
                    _NULL_HANDLE,  # no target table (AVI-only integrate)
                    _NULL_HANDLE, rffi.cast(rffi.UINT, 0), rffi.cast(rffi.UCHAR, 0),
                    avi_table_handle,
                    rffi.cast(rffi.INT, avi_for_max),
                    rffi.cast(rffi.UCHAR, avi_agg_col_type_code),
                    avi_group_cols_buf,
                    rffi.cast(rffi.UINT, avi_num_group_cols),
                    avi_input_schema_packed,
                    rffi.cast(rffi.UINT, avi_agg_col_idx),
                )

            agg_buf, num_aggs = _pack_agg_descs(agg_funcs)

            gc_buf = lltype.malloc(rffi.UINTP.TO, max(len(gcols), 1), flavor="raw")
            for gi in range(len(gcols)):
                gc_buf[gi] = rffi.cast(rffi.UINT, gcols[gi])

            packed_out_schema = engine_ffi.pack_schema(reduce_out_schema)

            fin_prog_handle = _NULL_HANDLE
            fin_schema_packed = _NULL_HANDLE
            if finalize_prog is not None:
                fin_prog_handle = finalize_prog._rust_handle
                fin_schema_packed = rffi.cast(rffi.VOIDP, engine_ffi.pack_schema(view_family.schema))

            engine_ffi._program_builder_add_reduce(
                builder,
                rffi.cast(rffi.USHORT, in_reg_id),
                rffi.cast(rffi.SHORT, tr_in_reg_id),
                rffi.cast(rffi.USHORT, reg_id),
                rffi.cast(rffi.USHORT, raw_delta_id),
                rffi.cast(rffi.SHORT, fin_delta_id),
                rffi.cast(rffi.VOIDP, agg_buf), rffi.cast(rffi.UINT, num_aggs),
                gc_buf, rffi.cast(rffi.UINT, len(gcols)),
                rffi.cast(rffi.VOIDP, packed_out_schema),
                avi_table_handle,
                rffi.cast(rffi.INT, avi_for_max),
                rffi.cast(rffi.UCHAR, avi_agg_col_type_code),
                avi_group_cols_buf if avi_table_handle != _NULL_HANDLE else lltype.nullptr(rffi.UINTP.TO),
                rffi.cast(rffi.UINT, avi_num_group_cols),
                avi_input_schema_packed,
                rffi.cast(rffi.UINT, avi_agg_col_idx),
                gi_table_handle,
                rffi.cast(rffi.UINT, gi_col_idx),
                rffi.cast(rffi.UCHAR, gi_col_type_code),
                fin_prog_handle,
                fin_schema_packed,
            )

            # INTEGRATE input → trace_in (after REDUCE)
            if tr_in_table is not None:
                engine_ffi._program_builder_add_integrate(
                    builder,
                    rffi.cast(rffi.USHORT, in_reg_id),
                    tr_in_table._handle,
                    gi_table_handle,
                    rffi.cast(rffi.UINT, gi_col_idx),
                    rffi.cast(rffi.UCHAR, gi_col_type_code),
                    avi_table_handle,
                    rffi.cast(rffi.INT, avi_for_max),
                    rffi.cast(rffi.UCHAR, avi_agg_col_type_code),
                    avi_group_cols_buf if avi_table_handle != _NULL_HANDLE else lltype.nullptr(rffi.UINTP.TO),
                    rffi.cast(rffi.UINT, avi_num_group_cols),
                    avi_input_schema_packed,
                    rffi.cast(rffi.UINT, avi_agg_col_idx),
                )

            # Cleanup temp buffers
            lltype.free(agg_buf, flavor="raw")
            lltype.free(gc_buf, flavor="raw")
            lltype.free(packed_out_schema, flavor="raw")
            if avi_group_cols_buf != lltype.nullptr(rffi.UINTP.TO):
                lltype.free(avi_group_cols_buf, flavor="raw")
            if avi_input_schema_packed != _NULL_HANDLE:
                lltype.free(rffi.cast(rffi.CCHARP, avi_input_schema_packed), flavor="raw")
            if fin_schema_packed != _NULL_HANDLE:
                lltype.free(rffi.cast(rffi.CCHARP, fin_schema_packed), flavor="raw")

            _emit_simple_integrate(builder, raw_delta_id, trace_table._handle)
            return

        elif op == opcodes.OPCODE_INTEGRATE:
            in_reg_id = in_regs[opcodes.PORT_IN]
            in_reg_schema = reg_schemas[in_reg_id]
            int_table_id = node_params.get(opcodes.PARAM_TABLE_ID, 0)
            if int_table_id > 0 and int_table_id != view_id:
                int_table = view_family.store.create_child(
                    "_int_%d_%d" % (view_id, nid), in_reg_schema
                )
                reg_schemas[reg_id] = in_reg_schema
                reg_kinds[reg_id] = 1
                trace_reg_tables.append((reg_id, int_table))
                _emit_simple_integrate(builder, in_reg_id, int_table._handle)
                return
            state[_ST_SINK_REG_ID] = in_reg_id
            _emit_simple_integrate(builder, in_reg_id, _NULL_HANDLE)
            return

        elif op == opcodes.OPCODE_DELAY:
            in_reg_schema = reg_schemas[in_regs[opcodes.PORT_IN]]
            reg_schemas[reg_id] = in_reg_schema
            reg_kinds[reg_id] = 0
            engine_ffi._program_builder_add_delay(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN]),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_ANTI_JOIN_DELTA_TRACE:
            delta_schema = reg_schemas[in_regs[opcodes.PORT_DELTA]]
            reg_schemas[reg_id] = delta_schema
            reg_kinds[reg_id] = 0
            engine_ffi._program_builder_add_anti_join_dt(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_DELTA]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_TRACE]),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_ANTI_JOIN_DELTA_DELTA:
            a_schema = reg_schemas[in_regs[opcodes.PORT_IN_A]]
            reg_schemas[reg_id] = a_schema
            reg_kinds[reg_id] = 0
            engine_ffi._program_builder_add_anti_join_dd(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_A]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_B]),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_SEMI_JOIN_DELTA_TRACE:
            delta_schema = reg_schemas[in_regs[opcodes.PORT_DELTA]]
            reg_schemas[reg_id] = delta_schema
            reg_kinds[reg_id] = 0
            engine_ffi._program_builder_add_semi_join_dt(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_DELTA]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_TRACE]),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_SEMI_JOIN_DELTA_DELTA:
            a_schema = reg_schemas[in_regs[opcodes.PORT_IN_A]]
            reg_schemas[reg_id] = a_schema
            reg_kinds[reg_id] = 0
            engine_ffi._program_builder_add_semi_join_dd(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_A]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN_B]),
                rffi.cast(rffi.USHORT, reg_id),
            )
            return

        elif op == opcodes.OPCODE_SEEK_TRACE:
            engine_ffi._program_builder_add_seek_trace(
                builder,
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_TRACE]),
                rffi.cast(rffi.USHORT, in_regs[opcodes.PORT_IN]),
            )
            return

        elif op == opcodes.OPCODE_CLEAR_DELTAS:
            engine_ffi._program_builder_add_clear_deltas(builder)
            return

        elif op == opcodes.OPCODE_GATHER_REDUCE:
            in_reg_id = in_regs[opcodes.PORT_IN_REDUCE]
            partial_schema = reg_schemas[in_reg_id]
            num_out_cols = len(partial_schema.columns)

            agg_count = node_params.get(opcodes.PARAM_AGG_COUNT, 0)
            if agg_count > 0:
                agg_funcs = newlist_hint(agg_count)
                for ai in range(agg_count):
                    packed = node_params.get(opcodes.PARAM_AGG_SPEC_BASE + ai, 0)
                    func_id = packed >> 32
                    agg_col_in_partial = num_out_cols - agg_count + ai
                    col_type = partial_schema.columns[agg_col_in_partial].field_type
                    agg_funcs.append(functions.UniversalAccumulator(0, func_id, col_type))
            else:
                agg_func_id = node_params.get(opcodes.PARAM_AGG_FUNC_ID, 0)
                if agg_func_id > 0:
                    agg_col_in_partial = num_out_cols - 1
                    col_type = partial_schema.columns[agg_col_in_partial].field_type
                    agg_funcs = newlist_hint(1)
                    agg_funcs.append(functions.UniversalAccumulator(0, agg_func_id, col_type))
                else:
                    agg_funcs = newlist_hint(1)
                    agg_funcs.append(NULL_AGGREGATE)

            trace_table = view_family.store.create_child(
                "_gather_%d_%d" % (view_id, nid), partial_schema
            )
            reg_schemas[reg_id] = partial_schema
            reg_kinds[reg_id] = 1
            trace_reg_tables.append((reg_id, trace_table))
            raw_delta_id = state[_ST_NEXT_EXTRA_REG]
            state[_ST_NEXT_EXTRA_REG] += 1
            reg_schemas[raw_delta_id] = partial_schema
            reg_kinds[raw_delta_id] = 0
            out_reg_of[nid] = raw_delta_id

            num_aggs = len(agg_funcs)
            agg_buf, num_aggs = _pack_agg_descs(agg_funcs)

            engine_ffi._program_builder_add_gather_reduce(
                builder,
                rffi.cast(rffi.USHORT, in_reg_id),
                rffi.cast(rffi.USHORT, reg_id),
                rffi.cast(rffi.USHORT, raw_delta_id),
                rffi.cast(rffi.VOIDP, agg_buf),
                rffi.cast(rffi.UINT, num_aggs),
            )
            lltype.free(agg_buf, flavor="raw")

            _emit_simple_integrate(builder, raw_delta_id, trace_table._handle)
            return

        # Unknown opcode: pass-through
        in_reg_id_unk = in_regs.get(opcodes.PORT_IN, -1)
        if in_reg_id_unk >= 0:
            reg_schemas[reg_id] = reg_schemas[in_reg_id_unk]
            reg_kinds[reg_id] = reg_kinds[in_reg_id_unk]
        return

    def _build_vm_handle(self, builder, num_regs, reg_schemas, reg_kinds):
        """Pack register metadata and call ProgramBuilder.build(). Returns VmHandle."""
        from gnitz import log
        reg_schemas_buf = lltype.malloc(rffi.CCHARP.TO, max(num_regs * SCHEMA_DESC_SIZE, 1), flavor="raw")
        reg_kinds_buf = lltype.malloc(rffi.CCHARP.TO, max(num_regs, 1), flavor="raw")
        for ri in range(num_regs):
            schema = reg_schemas[ri]
            if schema is not None:
                tmp = engine_ffi.pack_schema(schema)
                offset = ri * SCHEMA_DESC_SIZE
                for bi in range(SCHEMA_DESC_SIZE):
                    reg_schemas_buf[offset + bi] = tmp[bi]
                lltype.free(tmp, flavor="raw")
            else:
                offset = ri * SCHEMA_DESC_SIZE
                for bi in range(SCHEMA_DESC_SIZE):
                    reg_schemas_buf[offset + bi] = '\x00'
            reg_kinds_buf[ri] = chr(reg_kinds[ri])
        handle = engine_ffi._program_builder_build(
            builder,
            rffi.cast(rffi.VOIDP, reg_schemas_buf),
            reg_kinds_buf,
            rffi.cast(rffi.UINT, num_regs),
        )
        lltype.free(reg_schemas_buf, flavor="raw")
        lltype.free(reg_kinds_buf, flavor="raw")
        if not handle:
            log.debug("program_builder_build returned NULL")
        return handle

    def compile_from_graph(self, view_id):
        from gnitz import log
        nodes = self._load_nodes(view_id)
        if not nodes: return None
        if not self.registry.has_id(view_id): return None

        edges = self._load_edges(view_id)
        sources = self._load_sources(view_id)
        params, str_params = self._load_params(view_id)
        group_cols = self._load_group_cols(view_id)

        view_family = self.registry.get_by_id(view_id)
        out_schema = view_family.schema

        # 1. Build circuit graph + topological sort
        try:
            graph = _build_circuit_graph(view_id, nodes, edges, sources, params,
                                         str_params, group_cols, out_schema)
        except LayoutError:
            return None

        opcode_of = graph.opcode_of
        ordered = graph.ordered

        # 2. Annotate (trace sides, schemas, distinctness)
        ann = _annotate(graph, self.registry)
        in_schema = ann.in_schema
        trace_side_sources = ann.trace_side_sources
        join_shard_map = ann.join_shard_map
        co_partitioned_join_sources = ann.co_partitioned

        # 3. Optimization passes
        rw = run_optimization_passes(graph, ann)

        # 4. Register assignment
        out_reg_of = {}
        next_reg = 0
        for nid in ordered:
            out_reg_of[nid] = next_reg
            next_reg += 1

        extra_regs = 0
        for nid, op in nodes:
            if op == opcodes.OPCODE_DISTINCT:
                if nid not in rw.skip_nodes:
                    extra_regs += 1
            elif op == opcodes.OPCODE_REDUCE:
                extra_regs += 2
                if nid in rw.fold_finalize:
                    extra_regs += 1
            elif op == opcodes.OPCODE_GATHER_REDUCE:
                extra_regs += 1
            elif op == opcodes.OPCODE_SCAN_TRACE:
                table_id = sources.get(nid, 0)
                if table_id > 0 and nid not in trace_side_sources:
                    extra_regs += 1

        num_regs = next_reg + extra_regs
        reg_schemas = [None] * num_regs
        reg_kinds = [0] * num_regs
        trace_reg_tables = []  # [(reg_id, ZSetStore)]

        builder = engine_ffi._program_builder_new(rffi.cast(rffi.USHORT, num_regs))

        # 5. Instruction emission
        state = [next_reg, -1, -1]
        source_reg_map = {}

        for nid in ordered:
            op = opcode_of[nid]
            reg_id = out_reg_of[nid]
            node_params = params.get(nid, {})

            in_regs = {}
            for _, src, dst, port in edges:
                if dst == nid:
                    in_regs[port] = out_reg_of[src]

            if op == opcodes.OPCODE_EXCHANGE_SHARD:
                in_reg_id_for_exchange = in_regs[opcodes.PORT_EXCHANGE_IN]

                shard_cols = []
                idx = 0
                while (opcodes.PARAM_SHARD_COL_BASE + idx) in node_params:
                    shard_cols.append(node_params[opcodes.PARAM_SHARD_COL_BASE + idx])
                    idx += 1

                exchange_in_schema = reg_schemas[in_reg_id_for_exchange]

                # Build post-plan
                post_ordered = []
                found_exchange = False
                for post_nid in ordered:
                    if post_nid == nid:
                        found_exchange = True
                        continue
                    if found_exchange:
                        post_ordered.append(post_nid)

                post_next_reg = 0
                post_out_reg_of = {}
                for pnid in post_ordered:
                    post_out_reg_of[pnid] = post_next_reg
                    post_next_reg += 1

                post_extra_regs = 0
                for pnid in post_ordered:
                    pop = opcode_of[pnid]
                    if pop == opcodes.OPCODE_DISTINCT:
                        if pnid not in rw.skip_nodes:
                            post_extra_regs += 1
                    elif pop == opcodes.OPCODE_REDUCE:
                        post_extra_regs += 2
                        if pnid in rw.fold_finalize:
                            post_extra_regs += 1
                    elif pop == opcodes.OPCODE_GATHER_REDUCE:
                        post_extra_regs += 1
                    elif pop == opcodes.OPCODE_SCAN_TRACE:
                        ptid = sources.get(pnid, 0)
                        if ptid > 0 and pnid not in trace_side_sources:
                            post_extra_regs += 1

                post_input_reg_id = post_next_reg
                post_next_reg += 1

                post_num_regs = post_next_reg + post_extra_regs
                post_reg_schemas = [None] * post_num_regs
                post_reg_kinds = [0] * post_num_regs
                post_trace_reg_tables = []

                post_reg_schemas[post_input_reg_id] = exchange_in_schema
                post_reg_kinds[post_input_reg_id] = 0
                post_out_reg_of[nid] = post_input_reg_id

                post_builder = engine_ffi._program_builder_new(
                    rffi.cast(rffi.USHORT, post_num_regs))
                post_state = [post_next_reg, -1, -1]

                for pnid in post_ordered:
                    pop = opcode_of[pnid]
                    preg_id = post_out_reg_of[pnid]
                    pnode_params = params.get(pnid, {})

                    pin_regs = {}
                    for _, src, dst, port in edges:
                        if dst == pnid:
                            if src in post_out_reg_of:
                                pin_regs[port] = post_out_reg_of[src]

                    self._emit_node(
                        pnid, pop, preg_id, pnode_params, pin_regs,
                        post_reg_schemas, post_reg_kinds,
                        post_trace_reg_tables,
                        post_out_reg_of, sources,
                        trace_side_sources, group_cols, view_family,
                        view_id, out_schema, exchange_in_schema,
                        post_builder, post_state, str_params, rw,
                        source_reg_map=None,
                    )

                engine_ffi._program_builder_add_halt(post_builder)
                post_sink_reg_id = post_state[_ST_SINK_REG_ID]
                if post_sink_reg_id == -1:
                    engine_ffi._program_builder_free(post_builder)
                    engine_ffi._program_builder_free(builder)
                    return None

                post_vm_handle = self._build_vm_handle(
                    post_builder, post_num_regs,
                    post_reg_schemas, post_reg_kinds)

                post_plan = runtime.ExecutablePlan(
                    post_vm_handle, post_num_regs, out_schema,
                    in_reg_idx=post_input_reg_id,
                    out_reg_idx=post_sink_reg_id,
                    trace_reg_tables=post_trace_reg_tables,
                )

                # Finalize pre-plan
                engine_ffi._program_builder_add_halt(builder)
                is_trivial = (in_reg_id_for_exchange == state[_ST_INPUT_DELTA_REG_ID])
                skip_exchange = (
                    is_trivial
                    and len(shard_cols) == 1
                    and shard_cols[0] == in_schema.pk_index
                )
                pre_vm_handle = self._build_vm_handle(
                    builder, num_regs, reg_schemas, reg_kinds)

                pre_plan = runtime.ExecutablePlan(
                    pre_vm_handle, num_regs, exchange_in_schema,
                    in_reg_idx=state[_ST_INPUT_DELTA_REG_ID],
                    out_reg_idx=in_reg_id_for_exchange,
                    trace_reg_tables=trace_reg_tables,
                    exchange_post_plan=post_plan,
                    skip_exchange=skip_exchange,
                )
                return pre_plan

            self._emit_node(
                nid, op, reg_id, node_params, in_regs,
                reg_schemas, reg_kinds, trace_reg_tables,
                out_reg_of, sources, trace_side_sources,
                group_cols, view_family, view_id, out_schema,
                in_schema, builder, state, str_params, rw,
                source_reg_map=source_reg_map,
            )

        engine_ffi._program_builder_add_halt(builder)
        input_delta_reg_id = state[_ST_INPUT_DELTA_REG_ID]
        sink_reg_id = state[_ST_SINK_REG_ID]
        if input_delta_reg_id == -1 and source_reg_map:
            for _k in source_reg_map:
                input_delta_reg_id = source_reg_map[_k]
                break
        if input_delta_reg_id == -1 or sink_reg_id == -1:
            engine_ffi._program_builder_free(builder)
            return None

        # Fail-fast: verify the circuit's output schema matches the view family schema.
        sink_schema = reg_schemas[sink_reg_id]
        if sink_schema is not None and len(sink_schema.columns) != len(out_schema.columns):
            engine_ffi._program_builder_free(builder)
            raise LayoutError(
                "view %d circuit output has %d columns but family schema has %d "
                "(missing or wrong final projection node in circuit graph)"
                % (view_id, len(sink_schema.columns), len(out_schema.columns))
            )
        if sink_schema is not None:
            for i in range(len(out_schema.columns)):
                if sink_schema.columns[i].field_type.code != out_schema.columns[i].field_type.code:
                    engine_ffi._program_builder_free(builder)
                    raise LayoutError(
                        "view %d circuit output col %d type=%d, family schema expects type=%d"
                        % (view_id, i, sink_schema.columns[i].field_type.code,
                           out_schema.columns[i].field_type.code)
                    )

        vm_handle = self._build_vm_handle(builder, num_regs, reg_schemas, reg_kinds)

        return runtime.ExecutablePlan(
            vm_handle, num_regs, out_schema,
            in_reg_idx=input_delta_reg_id, out_reg_idx=sink_reg_id,
            trace_reg_tables=trace_reg_tables,
            source_reg_map=source_reg_map if source_reg_map else None,
            join_shard_map=join_shard_map if join_shard_map else None,
            co_partitioned_join_sources=(
                co_partitioned_join_sources if co_partitioned_join_sources else None
            ),
        )
