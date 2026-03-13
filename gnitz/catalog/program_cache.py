# gnitz/catalog/program_cache.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask, r_int64

from gnitz.core import opcodes
from gnitz.core.types import merge_schemas_for_join, _build_map_output_schema, _build_reduce_output_schema
from gnitz.core.errors import LayoutError
from gnitz.catalog import system_tables as sys
from gnitz.vm import instructions, runtime
from gnitz.dbsp import functions

NULL_PREDICATE = functions.NullPredicate()
NULL_AGGREGATE = functions.NullAggregate()

# Indices into the mutable state list passed to _emit_node
_ST_NEXT_EXTRA_REG = 0
_ST_SINK_REG_ID = 1
_ST_INPUT_DELTA_REG_ID = 2


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
        self._shard_cols_cache = {}

    def invalidate(self, program_id):
        if program_id in self._cache:
            del self._cache[program_id]
        if program_id in self._shard_cols_cache:
            del self._shard_cols_cache[program_id]

    def invalidate_all(self):
        self._cache.clear()
        self._shard_cols_cache.clear()

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
                    lo64 = r_uint64(intmask(pk))
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
        params = self._load_params(view_id)
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

    def _resolve_primary_input_schema(self, program_id, fallback,
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
        cursor, end_key = _open_system_scan(self.registry, sys.DepTab.ID, program_id)
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
                    if dep_tid > 0 and self.registry.has_id(dep_tid):
                        dep_schema = self.registry.get_by_id(dep_tid).schema
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

    def _emit_node(self, nid, op, reg_id, node_params, in_regs,
                   cur_reg_file, out_reg_of, sources, trace_side_sources,
                   group_cols, view_family, view_id, out_schema, in_schema,
                   program, state):
        """Emit a single operator node. Returns instruction or None.

        state: mutable list [next_extra_reg, sink_reg_id, input_delta_reg_id].
        Mutates cur_reg_file, out_reg_of, and state in-place.
        REDUCE appends its reduce pre-instruction to program directly.
        """
        if op == opcodes.OPCODE_SCAN_TRACE:
            table_id = sources.get(nid, 0)
            chunk_limit = node_params.get(opcodes.PARAM_CHUNK_LIMIT, 0)

            if table_id == 0:
                # Primary input (delta): bind external batch at execution time
                reg = runtime.DeltaRegister(reg_id, in_schema)
                cur_reg_file.registers[reg_id] = reg
                state[_ST_INPUT_DELTA_REG_ID] = reg_id
                return None
            elif nid in trace_side_sources:
                family = self.registry.get_by_id(table_id)
                reg = runtime.TraceRegister(reg_id, family.schema, family.store.create_cursor(), family.store)
                cur_reg_file.registers[reg_id] = reg
                return None
            else:
                family = self.registry.get_by_id(table_id)
                trace_reg = runtime.TraceRegister(reg_id, family.schema, family.store.create_cursor(), family.store)
                cur_reg_file.registers[reg_id] = trace_reg
                out_delta_id = state[_ST_NEXT_EXTRA_REG]
                state[_ST_NEXT_EXTRA_REG] += 1
                out_delta_reg = runtime.DeltaRegister(out_delta_id, family.schema)
                cur_reg_file.registers[out_delta_id] = out_delta_reg
                out_reg_of[nid] = out_delta_id
                return instructions.scan_trace_op(trace_reg, out_delta_reg, chunk_limit)

        elif op == opcodes.OPCODE_FILTER:
            in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN]]
            out_reg = runtime.DeltaRegister(reg_id, in_reg.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
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
            return instructions.filter_op(in_reg, out_reg, func)

        elif op == opcodes.OPCODE_MAP:
            in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN]]
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
                node_schema = _build_map_output_schema(in_reg.table_schema, src_indices)
            else:
                func = NULL_PREDICATE
                node_schema = in_reg.table_schema
            out_reg = runtime.DeltaRegister(reg_id, node_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.map_op(in_reg, out_reg, func)

        elif op == opcodes.OPCODE_NEGATE:
            in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN]]
            out_reg = runtime.DeltaRegister(reg_id, in_reg.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.negate_op(in_reg, out_reg)

        elif op == opcodes.OPCODE_UNION:
            in_a = cur_reg_file.registers[in_regs[opcodes.PORT_IN_A]]
            in_b = cur_reg_file.registers[in_regs[opcodes.PORT_IN_B]]
            out_reg = runtime.DeltaRegister(reg_id, in_a.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.union_op(in_a, in_b, out_reg)

        elif op == opcodes.OPCODE_JOIN_DELTA_TRACE:
            delta_reg = cur_reg_file.registers[in_regs[opcodes.PORT_DELTA]]
            trace_reg = cur_reg_file.registers[in_regs[opcodes.PORT_TRACE]]
            join_schema = merge_schemas_for_join(delta_reg.table_schema, trace_reg.table_schema)
            out_reg = runtime.DeltaRegister(reg_id, join_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.join_delta_trace_op(delta_reg, trace_reg, out_reg)

        elif op == opcodes.OPCODE_JOIN_DELTA_DELTA:
            reg_a = cur_reg_file.registers[in_regs[opcodes.PORT_IN_A]]
            reg_b = cur_reg_file.registers[in_regs[opcodes.PORT_IN_B]]
            join_schema = merge_schemas_for_join(reg_a.table_schema, reg_b.table_schema)
            out_reg = runtime.DeltaRegister(reg_id, join_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.join_delta_delta_op(reg_a, reg_b, out_reg)

        elif op == opcodes.OPCODE_DISTINCT:
            in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN_DISTINCT]]
            hist_schema = in_reg.table_schema
            history_table = view_family.store.create_child("_hist_%d_%d" % (view_id, nid), hist_schema)
            hist_reg = runtime.TraceRegister(reg_id, hist_schema, history_table.create_cursor(), history_table)
            cur_reg_file.registers[reg_id] = hist_reg
            out_delta_id = state[_ST_NEXT_EXTRA_REG]
            state[_ST_NEXT_EXTRA_REG] += 1
            out_delta_reg = runtime.DeltaRegister(out_delta_id, in_reg.table_schema)
            cur_reg_file.registers[out_delta_id] = out_delta_reg
            out_reg_of[nid] = out_delta_id
            return instructions.distinct_op(in_reg, hist_reg, out_delta_reg)

        elif op == opcodes.OPCODE_REDUCE:
            in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN_REDUCE]]
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
            cur_reg_file.registers[reg_id] = tr_out_reg
            out_delta_id = state[_ST_NEXT_EXTRA_REG]
            state[_ST_NEXT_EXTRA_REG] += 1
            out_delta_reg = runtime.DeltaRegister(out_delta_id, reduce_out_schema)
            cur_reg_file.registers[out_delta_id] = out_delta_reg
            out_reg_of[nid] = out_delta_id
            tr_in_reg = None
            tr_in_table = None
            if opcodes.PORT_TRACE_IN in in_regs:
                tr_in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_TRACE_IN]]
            elif not agg_func.is_linear():
                # Auto-create intermediate trace for reduce's input history.
                # Non-linear aggregates (MIN/MAX) replay this trace on retraction.
                tr_in_table = view_family.store.create_child(
                    "_reduce_in_%d_%d" % (view_id, nid), in_reg.table_schema
                )
                tr_in_reg_id = state[_ST_NEXT_EXTRA_REG]
                state[_ST_NEXT_EXTRA_REG] += 1
                tr_in_reg = runtime.TraceRegister(
                    tr_in_reg_id, in_reg.table_schema,
                    tr_in_table.create_cursor(), tr_in_table
                )
                cur_reg_file.registers[tr_in_reg_id] = tr_in_reg
            reduce_instr = instructions.reduce_op(in_reg, tr_in_reg, tr_out_reg, out_delta_reg, gcols, agg_func, reduce_out_schema)
            program.append(reduce_instr)
            if tr_in_table is not None:
                program.append(instructions.integrate_op(in_reg, tr_in_table))
            return instructions.integrate_op(out_delta_reg, trace_table)

        elif op == opcodes.OPCODE_INTEGRATE:
            in_reg_id = in_regs[opcodes.PORT_IN]
            in_reg = cur_reg_file.registers[in_reg_id]
            state[_ST_SINK_REG_ID] = in_reg_id
            return instructions.integrate_op(in_reg, None)

        elif op == opcodes.OPCODE_DELAY:
            in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN]]
            out_reg = runtime.DeltaRegister(reg_id, in_reg.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.delay_op(in_reg, out_reg)

        elif op == opcodes.OPCODE_ANTI_JOIN_DELTA_TRACE:
            delta_reg = cur_reg_file.registers[in_regs[opcodes.PORT_DELTA]]
            trace_reg = cur_reg_file.registers[in_regs[opcodes.PORT_TRACE]]
            out_reg = runtime.DeltaRegister(reg_id, delta_reg.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.anti_join_delta_trace_op(delta_reg, trace_reg, out_reg)

        elif op == opcodes.OPCODE_ANTI_JOIN_DELTA_DELTA:
            reg_a = cur_reg_file.registers[in_regs[opcodes.PORT_IN_A]]
            reg_b = cur_reg_file.registers[in_regs[opcodes.PORT_IN_B]]
            out_reg = runtime.DeltaRegister(reg_id, reg_a.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.anti_join_delta_delta_op(reg_a, reg_b, out_reg)

        elif op == opcodes.OPCODE_SEMI_JOIN_DELTA_TRACE:
            delta_reg = cur_reg_file.registers[in_regs[opcodes.PORT_DELTA]]
            trace_reg = cur_reg_file.registers[in_regs[opcodes.PORT_TRACE]]
            out_reg = runtime.DeltaRegister(reg_id, delta_reg.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.semi_join_delta_trace_op(delta_reg, trace_reg, out_reg)

        elif op == opcodes.OPCODE_SEMI_JOIN_DELTA_DELTA:
            reg_a = cur_reg_file.registers[in_regs[opcodes.PORT_IN_A]]
            reg_b = cur_reg_file.registers[in_regs[opcodes.PORT_IN_B]]
            out_reg = runtime.DeltaRegister(reg_id, reg_a.table_schema)
            cur_reg_file.registers[reg_id] = out_reg
            return instructions.semi_join_delta_delta_op(reg_a, reg_b, out_reg)

        elif op == opcodes.OPCODE_SEEK_TRACE:
            trace_reg = cur_reg_file.registers[in_regs[opcodes.PORT_TRACE]]
            key_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN]]
            return instructions.seek_trace_op(trace_reg, key_reg)

        return None

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

        # 1. Topological sort
        opcode_of = {}
        for nid, op in nodes:
            opcode_of[nid] = op

        try:
            ordered, outgoing, incoming = _topo_sort(nodes, edges)
        except LayoutError:
            return None

        # 2. Identify trace-side source nodes (must be before schema resolution)
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

        in_schema = self._resolve_primary_input_schema(view_id, out_schema,
                                                       sources,
                                                       trace_side_sources)

        # 3. Register assignment
        out_reg_of = {}
        next_reg = 0
        for nid in ordered:
            out_reg_of[nid] = next_reg
            next_reg += 1

        extra_regs = 0
        for nid, op in nodes:
            if op == opcodes.OPCODE_DISTINCT:
                extra_regs += 1
            elif op == opcodes.OPCODE_REDUCE:
                extra_regs += 2  # out_delta + auto trace_in
            elif op == opcodes.OPCODE_SCAN_TRACE:
                table_id = sources.get(nid, 0)
                if table_id > 0 and nid not in trace_side_sources:
                    extra_regs += 1

        reg_file = runtime.RegisterFile(next_reg + extra_regs)

        # 3. Instruction emission
        program = newlist_hint(len(ordered) + 1)
        # state: [next_extra_reg, sink_reg_id, input_delta_reg_id]
        state = [next_reg, -1, -1]

        for nid in ordered:
            op = opcode_of[nid]
            reg_id = out_reg_of[nid]
            node_params = params.get(nid, {})

            in_regs = {}
            for _, src, dst, port in edges:
                if dst == nid:
                    in_regs[port] = out_reg_of[src]

            if op == opcodes.OPCODE_EXCHANGE_SHARD or op == opcodes.OPCODE_EXCHANGE_GATHER:
                # Exchange marker: split the plan into pre and post.
                in_reg_id_for_exchange = in_regs[opcodes.PORT_EXCHANGE_IN]

                # Extract shard column indices from params
                shard_cols = []
                if op == opcodes.OPCODE_EXCHANGE_SHARD:
                    idx = 0
                    while (opcodes.PARAM_SHARD_COL_BASE + idx) in node_params:
                        shard_cols.append(node_params[opcodes.PARAM_SHARD_COL_BASE + idx])
                        idx += 1
                # For GATHER, shard_cols stays empty (sentinel for "gather to worker 0")

                # The pre-plan output schema is the schema at the exchange boundary
                exchange_in_schema = reg_file.registers[in_reg_id_for_exchange].table_schema

                # Finalize the pre-plan
                program.append(instructions.halt_op())
                pre_plan = runtime.ExecutablePlan(
                    program, reg_file, exchange_in_schema,
                    in_reg_idx=state[_ST_INPUT_DELTA_REG_ID],
                    out_reg_idx=in_reg_id_for_exchange,
                )

                # Build the post-plan: fresh register file, continue with remaining nodes
                post_ordered = []
                found_exchange = False
                for post_nid in ordered:
                    if post_nid == nid:
                        found_exchange = True
                        continue
                    if found_exchange:
                        post_ordered.append(post_nid)

                # Count registers needed for post-plan
                post_next_reg = 0
                post_out_reg_of = {}
                for pnid in post_ordered:
                    post_out_reg_of[pnid] = post_next_reg
                    post_next_reg += 1

                # Count extra registers for distinct/reduce/scan_trace in post-plan
                post_extra_regs = 0
                for pnid in post_ordered:
                    pop = opcode_of[pnid]
                    if pop == opcodes.OPCODE_DISTINCT:
                        post_extra_regs += 1
                    elif pop == opcodes.OPCODE_REDUCE:
                        post_extra_regs += 2  # out_delta + auto trace_in
                    elif pop == opcodes.OPCODE_SCAN_TRACE:
                        ptid = sources.get(pnid, 0)
                        if ptid > 0 and pnid not in trace_side_sources:
                            post_extra_regs += 1

                # +1 for the post-plan input register (exchanged data)
                post_input_reg_id = post_next_reg
                post_next_reg += 1

                post_reg_file = runtime.RegisterFile(post_next_reg + post_extra_regs)

                # Create the input delta register for exchanged data
                post_in_reg = runtime.DeltaRegister(post_input_reg_id, exchange_in_schema)
                post_reg_file.registers[post_input_reg_id] = post_in_reg

                # Map the exchange node's output to the post input register
                post_out_reg_of[nid] = post_input_reg_id

                post_program = newlist_hint(len(post_ordered) + 1)
                # state: [next_extra_reg, sink_reg_id, input_delta_reg_id]
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

                    pinstr = self._emit_node(
                        pnid, pop, preg_id, pnode_params, pin_regs,
                        post_reg_file, post_out_reg_of, sources,
                        trace_side_sources, group_cols, view_family,
                        view_id, out_schema, exchange_in_schema,
                        post_program, post_state,
                    )
                    if pinstr is not None:
                        post_program.append(pinstr)

                post_program.append(instructions.halt_op())
                post_sink_reg_id = post_state[_ST_SINK_REG_ID]
                if post_sink_reg_id == -1:
                    return None

                post_plan = runtime.ExecutablePlan(
                    post_program, post_reg_file, out_schema,
                    in_reg_idx=post_input_reg_id,
                    out_reg_idx=post_sink_reg_id,
                )
                pre_plan.exchange_post_plan = post_plan
                pre_plan.exchange_shard_cols = shard_cols
                return pre_plan

            instr = self._emit_node(
                nid, op, reg_id, node_params, in_regs,
                reg_file, out_reg_of, sources, trace_side_sources,
                group_cols, view_family, view_id, out_schema,
                in_schema, program, state,
            )
            if instr is not None: program.append(instr)

        program.append(instructions.halt_op())
        input_delta_reg_id = state[_ST_INPUT_DELTA_REG_ID]
        sink_reg_id = state[_ST_SINK_REG_ID]
        if input_delta_reg_id == -1 or sink_reg_id == -1: return None

        return runtime.ExecutablePlan(program, reg_file, out_schema, in_reg_idx=input_delta_reg_id, out_reg_idx=sink_reg_id)
