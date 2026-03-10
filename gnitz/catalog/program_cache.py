# gnitz/catalog/program_cache.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask, r_int64

from gnitz.core import opcodes
from gnitz.core.types import merge_schemas_for_join, _build_reduce_output_schema
from gnitz.core.errors import LayoutError
from gnitz.catalog import system_tables as sys
from gnitz.vm import instructions, runtime
from gnitz.dbsp import functions

NULL_PREDICATE = functions.NullPredicate()
NULL_AGGREGATE = functions.NullAggregate()


def _open_system_scan(registry, table_id, view_id):
    """Returns (cursor, end_key) with cursor pre-seeked, or (None, 0) if table absent."""
    if not registry.has_id(table_id):
        return None, r_uint128(0)
    family = registry.get_by_id(table_id)
    cursor = family.store.create_cursor()
    start_key = r_uint128(r_uint64(view_id)) << 64
    end_key = r_uint128(r_uint64(view_id + 1)) << 64
    cursor.seek(start_key)
    return cursor, end_key

def _topo_sort(nodes, edges):
    """Kahn's algorithm. Returns (ordered, outgoing, incoming) or raises LayoutError on cycle."""
    node_ids = newlist_hint(len(nodes))
    outgoing = {}
    incoming = {}
    in_degree = {}
    for nid, _ in nodes:
        node_ids.append(nid)
        outgoing[nid] = []
        incoming[nid] = []
        in_degree[nid] = 0

    for _, src, dst, port in edges:
        outgoing[src].append((dst, port))
        incoming[dst].append(src)
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

    return ordered, outgoing, incoming


class ProgramCache(object):
    """
    Caches execution plans for Reactive Views.
    """

    _immutable_fields_ = ["registry"]

    def __init__(self, registry):
        self.registry = registry
        self._cache = {}

    def invalidate(self, program_id):
        if program_id in self._cache:
            del self._cache[program_id]

    def invalidate_all(self):
        self._cache.clear()

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
            return {}
        result = {}
        try:
            while cursor.is_valid() and cursor.key() < end_key:
                if cursor.weight() > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    lo64 = r_uint64(pk)
                    node_id = intmask(lo64 >> 8)
                    slot = intmask(lo64 & r_uint64(0xFF))
                    value = intmask(acc.get_int(sys.CircuitParamsTab.COL_VALUE))
                    if node_id not in result: result[node_id] = {}
                    result[node_id][slot] = value
                cursor.advance()
        finally:
            cursor.close()
        return result

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
                    lo64 = r_uint64(pk)
                    node_id = intmask(lo64 >> 16)
                    col_idx = intmask(lo64 & r_uint64(0xFFFF))
                    if node_id not in result: result[node_id] = newlist_hint(4)
                    result[node_id].append(col_idx)
                cursor.advance()
        finally:
            cursor.close()
        return result

    def _resolve_primary_input_schema(self, program_id, fallback):
        if not self.registry.has_id(sys.DepTab.ID): return fallback
        deps_family = self.registry.get_by_id(sys.DepTab.ID)
        cursor = deps_family.store.create_cursor()
        result = None
        try:
            while cursor.is_valid():
                if cursor.weight() <= r_int64(0):
                    cursor.advance()
                    continue
                acc = cursor.get_accessor()
                v_id = intmask(acc.get_int(sys.DepTab.COL_VIEW_ID))
                if v_id == program_id:
                    dep_table_id = intmask(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID))
                    dep_view_id = intmask(acc.get_int(sys.DepTab.COL_DEP_VIEW_ID))
                    source_id = dep_table_id if dep_table_id > 0 else dep_view_id
                    if source_id > 0 and self.registry.has_id(source_id):
                        result = self.registry.get_by_id(source_id).schema
                        break
                cursor.advance()
        finally:
            cursor.close()
        if result is not None:
            return result
        if fallback is not None:
            return fallback
        raise LayoutError("Cannot resolve primary input schema for view %d: "
                          "no dependency record found" % program_id)

    def compile_from_graph(self, view_id):
        nodes = self._load_nodes(view_id)
        if not nodes: return None
        if not self.registry.has_id(view_id): return None

        edges = self._load_edges(view_id)
        sources = self._load_sources(view_id)
        params = self._load_params(view_id)
        group_cols = self._load_group_cols(view_id)

        view_family = self.registry.get_by_id(view_id)
        out_schema = view_family.schema
        in_schema = self._resolve_primary_input_schema(view_id, out_schema)

        # 1. Topological sort
        opcode_of = {}
        for nid, op in nodes:
            opcode_of[nid] = op

        try:
            ordered, outgoing, incoming = _topo_sort(nodes, edges)
        except LayoutError:
            return None

        # 2. Register assignment
        out_reg_of = {}
        next_reg = 0
        for nid in ordered:
            out_reg_of[nid] = next_reg
            next_reg += 1

        trace_side_sources = {}
        for _, src, dst, port in edges:
            if opcode_of.get(src, -1) == opcodes.OPCODE_SCAN_TRACE:
                if port == opcodes.PORT_TRACE:
                    # Only treat as trace-side if the destination actually
                    # consumes a cursor (join/anti_join/semi_join delta_trace,
                    # seek_trace).  PORT_TRACE == PORT_IN_B == 1, so without
                    # this check, UNION's B input would be misidentified.
                    dst_op = opcode_of.get(dst, -1)
                    if (dst_op == opcodes.OPCODE_JOIN_DELTA_TRACE or
                        dst_op == opcodes.OPCODE_ANTI_JOIN_DELTA_TRACE or
                        dst_op == opcodes.OPCODE_SEMI_JOIN_DELTA_TRACE or
                        dst_op == opcodes.OPCODE_SEEK_TRACE):
                        trace_side_sources[src] = True

        extra_regs = 0
        for nid, op in nodes:
            if op == opcodes.OPCODE_DISTINCT or op == opcodes.OPCODE_REDUCE:
                extra_regs += 1
            elif op == opcodes.OPCODE_SCAN_TRACE:
                table_id = sources.get(nid, 0)
                if table_id > 0 and nid not in trace_side_sources:
                    extra_regs += 1

        reg_file = runtime.RegisterFile(next_reg + extra_regs)
        next_extra_reg = next_reg

        # 3. Instruction emission
        program = newlist_hint(len(ordered) + 1)
        input_delta_reg_id = -1
        sink_reg_id = -1

        for nid in ordered:
            op = opcode_of[nid]
            reg_id = out_reg_of[nid]
            node_params = params.get(nid, {})

            in_regs = {}
            for _, src, dst, port in edges:
                if dst == nid:
                    in_regs[port] = out_reg_of[src]

            instr = None

            if op == opcodes.OPCODE_SCAN_TRACE:
                table_id = sources.get(nid, 0)
                chunk_limit = node_params.get(opcodes.PARAM_CHUNK_LIMIT, 0)

                if table_id == 0:
                    reg = runtime.DeltaRegister(reg_id, in_schema)
                    reg_file.registers[reg_id] = reg
                    input_delta_reg_id = reg_id
                elif nid in trace_side_sources:
                    family = self.registry.get_by_id(table_id)
                    reg = runtime.TraceRegister(reg_id, family.schema, family.store.create_cursor(), family.store)
                    reg_file.registers[reg_id] = reg
                else:
                    family = self.registry.get_by_id(table_id)
                    trace_reg = runtime.TraceRegister(reg_id, family.schema, family.store.create_cursor(), family.store)
                    reg_file.registers[reg_id] = trace_reg
                    out_delta_id = next_extra_reg
                    next_extra_reg += 1
                    out_delta_reg = runtime.DeltaRegister(out_delta_id, family.schema)
                    reg_file.registers[out_delta_id] = out_delta_reg
                    out_reg_of[nid] = out_delta_id
                    instr = instructions.scan_trace_op(trace_reg, out_delta_reg, chunk_limit)

            elif op == opcodes.OPCODE_FILTER:
                in_reg = reg_file.registers[in_regs[opcodes.PORT_IN]]
                out_reg = runtime.DeltaRegister(reg_id, in_reg.table_schema)
                reg_file.registers[reg_id] = out_reg
                num_regs = node_params.get(opcodes.PARAM_EXPR_NUM_REGS, 0)
                if num_regs > 0:
                    result_reg = node_params.get(opcodes.PARAM_EXPR_RESULT_REG, 0)
                    code = []
                    idx = 0
                    while (opcodes.PARAM_EXPR_BASE + idx) in node_params:
                        code.append(r_int64(node_params[opcodes.PARAM_EXPR_BASE + idx]))
                        idx += 1
                    from gnitz.dbsp.expr import ExprProgram, ExprPredicate
                    prog = ExprProgram(code, num_regs, result_reg)
                    func = ExprPredicate(prog)
                else:
                    func = NULL_PREDICATE
                instr = instructions.filter_op(in_reg, out_reg, func)

            elif op == opcodes.OPCODE_MAP:
                in_reg = reg_file.registers[in_regs[opcodes.PORT_IN]]
                if opcodes.PARAM_PROJ_BASE in node_params:
                    src_indices = []
                    src_types = []
                    idx = 0
                    while (opcodes.PARAM_PROJ_BASE + idx) in node_params:
                        src_col = node_params[opcodes.PARAM_PROJ_BASE + idx]
                        src_indices.append(src_col)
                        src_types.append(in_reg.table_schema.columns[src_col].field_type.code)
                        idx += 1
                    func = functions.UniversalProjection(src_indices, src_types)
                else:
                    func = NULL_PREDICATE
                out_reg = runtime.DeltaRegister(reg_id, out_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.map_op(in_reg, out_reg, func)

            elif op == opcodes.OPCODE_NEGATE:
                in_reg = reg_file.registers[in_regs[opcodes.PORT_IN]]
                out_reg = runtime.DeltaRegister(reg_id, in_reg.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.negate_op(in_reg, out_reg)

            elif op == opcodes.OPCODE_UNION:
                in_a = reg_file.registers[in_regs[opcodes.PORT_IN_A]]
                in_b = reg_file.registers[in_regs[opcodes.PORT_IN_B]]
                out_reg = runtime.DeltaRegister(reg_id, in_a.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.union_op(in_a, in_b, out_reg)

            elif op == opcodes.OPCODE_JOIN_DELTA_TRACE:
                delta_reg = reg_file.registers[in_regs[opcodes.PORT_DELTA]]
                trace_reg = reg_file.registers[in_regs[opcodes.PORT_TRACE]]
                join_schema = merge_schemas_for_join(delta_reg.table_schema, trace_reg.table_schema)
                out_reg = runtime.DeltaRegister(reg_id, join_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.join_delta_trace_op(delta_reg, trace_reg, out_reg)

            elif op == opcodes.OPCODE_JOIN_DELTA_DELTA:
                reg_a = reg_file.registers[in_regs[opcodes.PORT_IN_A]]
                reg_b = reg_file.registers[in_regs[opcodes.PORT_IN_B]]
                join_schema = merge_schemas_for_join(reg_a.table_schema, reg_b.table_schema)
                out_reg = runtime.DeltaRegister(reg_id, join_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.join_delta_delta_op(reg_a, reg_b, out_reg)

            elif op == opcodes.OPCODE_DISTINCT:
                in_reg = reg_file.registers[in_regs[opcodes.PORT_IN_DISTINCT]]
                hist_schema = in_reg.table_schema
                history_table = view_family.store.create_child("_hist_%d_%d" % (view_id, nid), hist_schema)
                hist_reg = runtime.TraceRegister(reg_id, hist_schema, history_table.create_cursor(), history_table)
                reg_file.registers[reg_id] = hist_reg
                out_delta_id = next_extra_reg
                next_extra_reg += 1
                out_delta_reg = runtime.DeltaRegister(out_delta_id, in_reg.table_schema)
                reg_file.registers[out_delta_id] = out_delta_reg
                out_reg_of[nid] = out_delta_id
                instr = instructions.distinct_op(in_reg, hist_reg, out_delta_reg)

            elif op == opcodes.OPCODE_REDUCE:
                in_reg = reg_file.registers[in_regs[opcodes.PORT_IN_REDUCE]]
                agg_func_id = node_params.get(opcodes.PARAM_AGG_FUNC_ID, 0)
                agg_col_idx = node_params.get(opcodes.PARAM_AGG_COL_IDX, 0)
                if agg_func_id > 0:
                    col_type = in_reg.table_schema.columns[agg_col_idx].field_type
                    agg_func = functions.UniversalAccumulator(agg_col_idx, agg_func_id, col_type)
                else:
                    agg_func = NULL_AGGREGATE
                gcols = group_cols.get(nid, [])
                reduce_out_schema = _build_reduce_output_schema(in_reg.table_schema, gcols, agg_func)
                trace_table = view_family.store.create_child("_reduce_%d_%d" % (view_id, nid), reduce_out_schema)
                tr_out_reg = runtime.TraceRegister(reg_id, reduce_out_schema, trace_table.create_cursor(), trace_table)
                reg_file.registers[reg_id] = tr_out_reg
                out_delta_id = next_extra_reg
                next_extra_reg += 1
                out_delta_reg = runtime.DeltaRegister(out_delta_id, reduce_out_schema)
                reg_file.registers[out_delta_id] = out_delta_reg
                out_reg_of[nid] = out_delta_id
                tr_in_reg = None
                if opcodes.PORT_TRACE_IN in in_regs: tr_in_reg = reg_file.registers[in_regs[opcodes.PORT_TRACE_IN]]
                reduce_instr = instructions.reduce_op(in_reg, tr_in_reg, tr_out_reg, out_delta_reg, gcols, agg_func, reduce_out_schema)
                program.append(reduce_instr)
                instr = instructions.integrate_op(out_delta_reg, trace_table)

            elif op == opcodes.OPCODE_INTEGRATE:
                in_reg_id = in_regs[opcodes.PORT_IN]
                in_reg = reg_file.registers[in_reg_id]
                target_table_id = node_params.get(opcodes.PARAM_TABLE_ID, 0)
                target = None
                if target_table_id > 0 and self.registry.has_id(target_table_id):
                    target = self.registry.get_by_id(target_table_id)
                sink_reg_id = in_reg_id
                # Final views integrate into target.store
                instr = instructions.integrate_op(in_reg, target.store if target else None)

            elif op == opcodes.OPCODE_DELAY:
                in_reg = reg_file.registers[in_regs[opcodes.PORT_IN]]
                out_reg = runtime.DeltaRegister(reg_id, in_reg.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.delay_op(in_reg, out_reg)

            elif op == opcodes.OPCODE_ANTI_JOIN_DELTA_TRACE:
                delta_reg = reg_file.registers[in_regs[opcodes.PORT_DELTA]]
                trace_reg = reg_file.registers[in_regs[opcodes.PORT_TRACE]]
                out_reg = runtime.DeltaRegister(reg_id, delta_reg.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.anti_join_delta_trace_op(delta_reg, trace_reg, out_reg)

            elif op == opcodes.OPCODE_ANTI_JOIN_DELTA_DELTA:
                reg_a = reg_file.registers[in_regs[opcodes.PORT_IN_A]]
                reg_b = reg_file.registers[in_regs[opcodes.PORT_IN_B]]
                out_reg = runtime.DeltaRegister(reg_id, reg_a.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.anti_join_delta_delta_op(reg_a, reg_b, out_reg)

            elif op == opcodes.OPCODE_SEMI_JOIN_DELTA_TRACE:
                delta_reg = reg_file.registers[in_regs[opcodes.PORT_DELTA]]
                trace_reg = reg_file.registers[in_regs[opcodes.PORT_TRACE]]
                out_reg = runtime.DeltaRegister(reg_id, delta_reg.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.semi_join_delta_trace_op(delta_reg, trace_reg, out_reg)

            elif op == opcodes.OPCODE_SEMI_JOIN_DELTA_DELTA:
                reg_a = reg_file.registers[in_regs[opcodes.PORT_IN_A]]
                reg_b = reg_file.registers[in_regs[opcodes.PORT_IN_B]]
                out_reg = runtime.DeltaRegister(reg_id, reg_a.table_schema)
                reg_file.registers[reg_id] = out_reg
                instr = instructions.semi_join_delta_delta_op(reg_a, reg_b, out_reg)

            elif op == opcodes.OPCODE_SEEK_TRACE:
                trace_reg = reg_file.registers[in_regs[opcodes.PORT_TRACE]]
                key_reg = reg_file.registers[in_regs[opcodes.PORT_IN]]
                instr = instructions.seek_trace_op(trace_reg, key_reg)

            if instr is not None: program.append(instr)

        program.append(instructions.halt_op())
        if input_delta_reg_id == -1 or sink_reg_id == -1: return None

        return runtime.ExecutablePlan(program, reg_file, out_schema, in_reg_idx=input_delta_reg_id, out_reg_idx=sink_reg_id)
