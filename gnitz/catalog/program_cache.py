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
from gnitz.storage import engine_ffi
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz import log

_NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)
SCHEMA_DESC_SIZE = engine_ffi.SCHEMA_DESC_SIZE


class ExecutablePlan(object):
    """
    Stateful, pre-compiled execution context for a Reactive View.
    Thin wrapper around a Rust VmHandle created by ProgramBuilder.
    """
    _immutable_fields_ = [
        "_rust_vm", "_num_regs", "out_schema", "in_reg_idx", "out_reg_idx",
        "exchange_post_plan", "source_reg_map",
        "join_shard_map",
        "skip_exchange", "co_partitioned_join_sources",
    ]

    def __init__(self, rust_vm_handle, num_regs, out_schema,
                 in_reg_idx=0, out_reg_idx=1,
                 trace_reg_tables=None,
                 exchange_post_plan=None,
                 source_reg_map=None, join_shard_map=None,
                 skip_exchange=False, co_partitioned_join_sources=None):
        self._rust_vm = rust_vm_handle
        self._num_regs = num_regs
        self.out_schema = out_schema
        self.in_reg_idx = in_reg_idx
        self.out_reg_idx = out_reg_idx
        self._ext_trace_regs = trace_reg_tables if trace_reg_tables is not None else []
        self._cursors = {}  # {reg_id: RustUnifiedCursor}
        self.exchange_post_plan = exchange_post_plan
        self.source_reg_map = source_reg_map
        self.join_shard_map = join_shard_map
        self.skip_exchange = skip_exchange
        self.co_partitioned_join_sources = co_partitioned_join_sources
        if self._rust_vm:
            log.debug("vm: plan created, %d regs" % num_regs)
        else:
            log.debug("vm: plan creation returned NULL handle")

    def close(self):
        if self._rust_vm:
            engine_ffi._vm_program_free(self._rust_vm)
            self._rust_vm = _NULL_HANDLE
        for reg_id in self._cursors:
            c = self._cursors[reg_id]
            if c is not None:
                c.close()
        self._cursors = {}

    def execute_epoch(self, input_delta, source_id=0):
        if not self._rust_vm:
            raise Exception("Rust VM program creation failed")
        return self._execute_epoch_rust(input_delta, source_id)

    def _execute_epoch_rust(self, input_delta, source_id):
        from gnitz.core.batch import ArenaZSetBatch
        # 1. Refresh external trace cursors (z⁻¹(I(X)) — before current delta)
        #    Owned trace cursors are refreshed by Rust in VmHandle::refresh_owned_cursors.
        for i in range(len(self._ext_trace_regs)):
            reg_id = self._ext_trace_regs[i][0]
            store = self._ext_trace_regs[i][1]
            old = self._cursors.get(reg_id, None)
            if old is not None:
                old.close()
            store.compact_if_needed()
            self._cursors[reg_id] = store.create_cursor()

        # 2. Consolidate input
        sealed = input_delta.to_consolidated()

        target_reg_idx = self.in_reg_idx
        if self.source_reg_map is not None and source_id in self.source_reg_map:
            target_reg_idx = self.source_reg_map[source_id]

        # Clone: the VM takes ownership (frees it)
        rust_input = engine_ffi._batch_clone(sealed._handle)

        # 3. Collect cursor handles
        num_regs = self._num_regs
        cursor_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_regs, 1), flavor="raw")
        for ri in range(num_regs):
            if ri in self._cursors and self._cursors[ri] is not None:
                cursor_ptrs[ri] = self._cursors[ri]._handle
            else:
                cursor_ptrs[ri] = _NULL_HANDLE

        out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
        try:
            rc = engine_ffi._vm_execute_epoch(
                self._rust_vm,
                rust_input,
                rffi.cast(rffi.USHORT, target_reg_idx),
                rffi.cast(rffi.USHORT, self.out_reg_idx),
                cursor_ptrs, rffi.cast(rffi.UINT, num_regs),
                out_result,
            )
            if intmask(rc) < 0:
                raise Exception("vm_execute_epoch failed: %d" % intmask(rc))
            handle = out_result[0]
            if handle:
                result = ArenaZSetBatch._wrap_handle(self.out_schema, handle, False, False)
            else:
                result = None
        finally:
            lltype.free(out_result, flavor="raw")
            lltype.free(cursor_ptrs, flavor="raw")
            if sealed is not input_delta:
                sealed.free()
        return result


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


def _field_type_from_code(code, size):
    """Look up the canonical FieldType singleton for a type code."""
    from gnitz.core.types import (
        TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32,
        TYPE_F32, TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128,
    )
    _TYPE_BY_CODE = {
        1: TYPE_U8, 2: TYPE_I8, 3: TYPE_U16, 4: TYPE_I16,
        5: TYPE_U32, 6: TYPE_I32, 7: TYPE_F32, 8: TYPE_U64,
        9: TYPE_I64, 10: TYPE_F64, 11: TYPE_STRING, 12: TYPE_U128,
    }
    if code in _TYPE_BY_CODE:
        return _TYPE_BY_CODE[code]
    from gnitz.core.types import FieldType
    return FieldType(code, size, size)


def _get_store_child_dir(store):
    """Get the directory under which child tables should be created.
    Works for both EphemeralTable (has .directory) and PartitionedTable."""
    from gnitz.storage.partitioned_table import PartitionedTable
    if isinstance(store, PartitionedTable):
        return store.get_child_base_dir()
    return store.directory


def _get_sys_handle(registry, tab_id):
    """Get the Rust table handle for a system table, or NULL if absent."""
    if registry.has_id(tab_id):
        return registry.get_by_id(tab_id).store._handle
    return _NULL_HANDLE


def _get_sys_schema(registry, tab_id):
    """Pack the schema for a system table into a C buffer, or return zeroed buffer."""
    if registry.has_id(tab_id):
        return engine_ffi.pack_schema(registry.get_by_id(tab_id).schema)
    buf = lltype.malloc(rffi.CCHARP.TO, SCHEMA_DESC_SIZE, flavor="raw")
    i = 0
    while i < SCHEMA_DESC_SIZE:
        buf[i] = '\x00'
        i += 1
    return buf


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

    def compile_from_graph(self, view_id):
        from gnitz import log
        if not self.registry.has_id(view_id):
            return None

        view_family = self.registry.get_by_id(view_id)
        out_schema = view_family.schema

        # ── Gather system table handles ─────────────────────────────────
        h_nodes = _get_sys_handle(self.registry, sys.CircuitNodesTab.ID)
        h_edges = _get_sys_handle(self.registry, sys.CircuitEdgesTab.ID)
        h_sources = _get_sys_handle(self.registry, sys.CircuitSourcesTab.ID)
        h_params = _get_sys_handle(self.registry, sys.CircuitParamsTab.ID)
        h_gcols = _get_sys_handle(self.registry, sys.CircuitGroupColsTab.ID)
        h_dep = _get_sys_handle(self.registry, sys.DepTab.ID)

        s_nodes = _get_sys_schema(self.registry, sys.CircuitNodesTab.ID)
        s_edges = _get_sys_schema(self.registry, sys.CircuitEdgesTab.ID)
        s_sources = _get_sys_schema(self.registry, sys.CircuitSourcesTab.ID)
        s_params = _get_sys_schema(self.registry, sys.CircuitParamsTab.ID)
        s_gcols = _get_sys_schema(self.registry, sys.CircuitGroupColsTab.ID)
        s_dep = _get_sys_schema(self.registry, sys.DepTab.ID)

        # ── Build external table registry ────────────────────────────────
        reg_entries = []
        for fam in self.registry.iter_families():
            tid = fam.table_id
            if tid <= 15:
                continue  # skip system tables
            reg_entries.append((tid, fam.store._handle, fam.schema))

        reg_count = len(reg_entries)
        reg_tids = lltype.malloc(rffi.LONGLONGP.TO, max(reg_count, 1), flavor="raw")
        reg_handles = lltype.malloc(rffi.VOIDPP.TO, max(reg_count, 1), flavor="raw")
        reg_schemas_buf = lltype.malloc(rffi.CCHARP.TO, max(reg_count * SCHEMA_DESC_SIZE, 1), flavor="raw")
        for ri in range(reg_count):
            tid, handle, schema = reg_entries[ri]
            reg_tids[ri] = rffi.cast(rffi.LONGLONG, tid)
            reg_handles[ri] = handle
            packed = engine_ffi.pack_schema(schema)
            offset = ri * SCHEMA_DESC_SIZE
            for bi in range(SCHEMA_DESC_SIZE):
                reg_schemas_buf[offset + bi] = packed[bi]
            lltype.free(packed, flavor="raw")

        # ── Pack view schema ─────────────────────────────────────────────
        view_schema_packed = engine_ffi.pack_schema(out_schema)

        # ── Allocate result buffer (flat i64 array) ─────────────────────
        result_buf = lltype.malloc(rffi.LONGLONGP.TO,
                                   engine_ffi.COMPILE_RESULT_SLOTS, flavor="raw")
        i = 0
        while i < engine_ffi.COMPILE_RESULT_SLOTS:
            result_buf[i] = rffi.cast(rffi.LONGLONG, 0)
            i += 1

        # ── Call Rust compiler ───────────────────────────────────────────
        try:
            view_child_dir = _get_store_child_dir(view_family.store)
            with rffi.scoped_str2charp(view_child_dir) as view_dir_c:
                rc = engine_ffi._compile_view(
                    rffi.cast(rffi.ULONGLONG, view_id),
                    h_nodes, h_edges, h_sources, h_params, h_gcols, h_dep,
                    rffi.cast(rffi.VOIDP, s_nodes),
                    rffi.cast(rffi.VOIDP, s_edges),
                    rffi.cast(rffi.VOIDP, s_sources),
                    rffi.cast(rffi.VOIDP, s_params),
                    rffi.cast(rffi.VOIDP, s_gcols),
                    rffi.cast(rffi.VOIDP, s_dep),
                    view_dir_c,
                    rffi.cast(rffi.UINT, view_family.table_id),
                    rffi.cast(rffi.VOIDP, view_schema_packed),
                    reg_tids,
                    reg_handles,
                    rffi.cast(rffi.VOIDP, reg_schemas_buf),
                    rffi.cast(rffi.UINT, reg_count),
                    result_buf,
                )
        finally:
            lltype.free(s_nodes, flavor="raw")
            lltype.free(s_edges, flavor="raw")
            lltype.free(s_sources, flavor="raw")
            lltype.free(s_params, flavor="raw")
            lltype.free(s_gcols, flavor="raw")
            lltype.free(s_dep, flavor="raw")
            lltype.free(view_schema_packed, flavor="raw")
            lltype.free(reg_schemas_buf, flavor="raw")
            lltype.free(reg_tids, flavor="raw")
            lltype.free(reg_handles, flavor="raw")

        if intmask(rc) < 0:
            log.warn("compile_view returned error: %d for view %d" % (intmask(rc), view_id))
            lltype.free(result_buf, flavor="raw")
            return None

        # ── Unpack flat i64 result buffer ────────────────────────────────
        # Slot layout matches compiler.rs CR_* constants
        error_code = intmask(result_buf[230])
        if error_code != 0:
            log.warn("compile_view: error_code=%d for view %d" % (error_code, view_id))
            lltype.free(result_buf, flavor="raw")
            return None
        # Cast i64 → void* via SIZE_T to avoid sign issues
        pre_vm = rffi.cast(rffi.VOIDP, rffi.cast(rffi.SIZE_T, result_buf[0]))
        post_vm = rffi.cast(rffi.VOIDP, rffi.cast(rffi.SIZE_T, result_buf[1]))
        pre_num_regs = intmask(result_buf[2])
        post_num_regs = intmask(result_buf[3])
        pre_in_reg = intmask(result_buf[4])
        pre_out_reg = intmask(result_buf[5])
        post_in_reg = intmask(result_buf[6])
        post_out_reg = intmask(result_buf[7])
        skip_exchange = intmask(result_buf[8])

        num_shard_cols = intmask(result_buf[9])
        shard_cols = []
        for si in range(num_shard_cols):
            shard_cols.append(intmask(result_buf[10 + si]))

        num_source_regs = intmask(result_buf[18])
        source_reg_map = {}
        for si in range(num_source_regs):
            tid = intmask(result_buf[19 + si])
            reg = intmask(result_buf[35 + si])
            source_reg_map[tid] = reg

        num_jsm = intmask(result_buf[51])
        join_shard_map = {}
        for ji in range(num_jsm):
            tid = intmask(result_buf[52 + ji])
            col_val = intmask(result_buf[68 + ji])
            join_shard_map[tid] = [col_val]

        num_cop = intmask(result_buf[84])
        co_partitioned = {}
        for ci in range(num_cop):
            tid = intmask(result_buf[85 + ci])
            co_partitioned[tid] = True

        num_etr = intmask(result_buf[101])
        pre_etr_count = intmask(result_buf[228])  # CR_PRE_ETR_COUNT
        pre_ext_trace_regs = []
        post_ext_trace_regs = []
        for ei in range(num_etr):
            reg = intmask(result_buf[102 + ei])
            tid = intmask(result_buf[165 + ei])
            if self.registry.has_id(tid):
                entry = (reg, self.registry.get_by_id(tid).store)
                if ei < pre_etr_count:
                    pre_ext_trace_regs.append(entry)
                else:
                    post_ext_trace_regs.append(entry)

        # NOTE: result_buf freed after exchange schema reading below

        # ── Build ExecutablePlan(s) ──────────────────────────────────────
        log.debug("compile_view: pre_vm=%d post_vm=%d pre_in=%d pre_out=%d post_in=%d post_out=%d pre_nregs=%d post_nregs=%d src_map_n=%d etr_n=%d"
                  % (intmask(rffi.cast(rffi.LONG, pre_vm)),
                     intmask(rffi.cast(rffi.LONG, post_vm)),
                     pre_in_reg, pre_out_reg, post_in_reg, post_out_reg,
                     pre_num_regs, post_num_regs,
                     num_source_regs, num_etr))

        if post_vm:
            # Read exchange schema (the pre-plan's output schema, != view out_schema)
            ex_num_cols = intmask(result_buf[231])
            ex_pk_index = intmask(result_buf[232])
            if ex_num_cols > 0:
                ex_cols = []
                for ci in range(ex_num_cols):
                    packed = intmask(result_buf[233 + ci])
                    tc = (packed >> 16) & 0xFF
                    sz = (packed >> 8) & 0xFF
                    nul = packed & 0xFF
                    ft = _field_type_from_code(tc, sz)
                    ex_cols.append(ColumnDefinition(ft, is_nullable=(nul != 0)))
                exchange_in_schema = TableSchema(ex_cols, pk_index=ex_pk_index)
            else:
                exchange_in_schema = out_schema

            lltype.free(result_buf, flavor="raw")

            post_plan = ExecutablePlan(
                post_vm, post_num_regs, out_schema,
                in_reg_idx=post_in_reg,
                out_reg_idx=post_out_reg,
                trace_reg_tables=post_ext_trace_regs,
            )
            pre_plan = ExecutablePlan(
                pre_vm, pre_num_regs, exchange_in_schema,
                in_reg_idx=pre_in_reg,
                out_reg_idx=pre_out_reg,
                trace_reg_tables=pre_ext_trace_regs,
                exchange_post_plan=post_plan,
                skip_exchange=(skip_exchange != 0),
                source_reg_map=source_reg_map if source_reg_map else None,
                join_shard_map=join_shard_map if join_shard_map else None,
                co_partitioned_join_sources=(
                    co_partitioned if co_partitioned else None
                ),
            )
            return pre_plan
        else:
            lltype.free(result_buf, flavor="raw")
            return ExecutablePlan(
                pre_vm, pre_num_regs, out_schema,
                in_reg_idx=pre_in_reg,
                out_reg_idx=pre_out_reg,
                trace_reg_tables=pre_ext_trace_regs,
                source_reg_map=source_reg_map if source_reg_map else None,
                join_shard_map=join_shard_map if join_shard_map else None,
                co_partitioned_join_sources=(
                    co_partitioned if co_partitioned else None
                ),
            )
