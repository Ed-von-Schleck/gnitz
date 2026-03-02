# gnitz/catalog/program_cache.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask, r_int64

from gnitz.catalog import system_tables as sys
from gnitz.vm import instructions, runtime
from gnitz.dbsp import functions


# Singletons to prevent allocation during VM setup
NULL_PREDICATE = functions.NullPredicate()
NULL_AGGREGATE = functions.NullAggregate()


def _get_scalar_func(func_id):
    # Logic to look up scalar function in registry would go here.
    return NULL_PREDICATE


def _get_agg_func(agg_func_id):
    # Logic to look up aggregate function in registry would go here.
    return NULL_AGGREGATE


class ProgramCache(object):
    """
    Caches execution plans for Reactive Views.
    Translates rows from `_system._instructions` into `runtime.ExecutablePlan`
    objects containing monomorphic `Instruction` lists and pre-allocated
    `RegisterFile`s.
    """

    _immutable_fields_ = ["registry", "_cache"]

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

        plan = self._load_program(program_id)
        if plan is not None:
            self._cache[program_id] = plan
        return plan

    def _parse_group_by_cols(self, s):
        if not s:
            return newlist_hint(0)
        parts = s.split(",")
        res = newlist_hint(len(parts))
        for p in parts:
            if p:
                res.append(int(p))
        return res

    def _resolve_primary_input_schema(self, program_id, fallback):
        """
        Queries _system._view_deps to find the schema of the primary upstream
        source for this view (the table or view whose delta arrives in register 0).
        """
        if not self.registry.has_id(sys.DepTab.ID):
            return fallback

        deps_family = self.registry.get_by_id(sys.DepTab.ID)
        cursor = deps_family.create_cursor()
        result = fallback
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

                    # Prefer a concrete base table over a derived view dependency.
                    source_id = dep_table_id if dep_table_id > 0 else dep_view_id
                    if source_id > 0 and self.registry.has_id(source_id):
                        result = self.registry.get_by_id(source_id).schema
                        break
                cursor.advance()
        finally:
            cursor.close()
        return result

    def _load_program(self, program_id):
        if not self.registry.has_id(sys.InstrTab.ID):
            return None

        if not self.registry.has_id(program_id):
            return None

        view_family = self.registry.get_by_id(program_id)
        out_schema = view_family.schema

        # Resolve input schema (Register 0) by checking the dependency graph.
        in_schema = self._resolve_primary_input_schema(program_id, out_schema)

        sys_instr = self.registry.get_by_id(sys.InstrTab.ID)
        cursor = sys_instr.create_cursor()

        program = [instructions.Instruction(0)]
        program.pop()
        reg_file = runtime.RegisterFile(16)

        try:
            pid_hi = r_uint64(program_id)
            pid_lo = r_uint64(0)
            start_key = (r_uint128(pid_hi) << 64) | r_uint128(pid_lo)
            cursor.seek(start_key)

            while cursor.is_valid():
                key = cursor.key()
                current_prog_id = intmask(r_uint64(key >> 64))
                if current_prog_id != program_id:
                    break

                if cursor.weight() <= r_int64(0):
                    cursor.advance()
                    continue

                acc = cursor.get_accessor()
                # Instructions schema (Payload indices start after Col 0 PK):
                # acc(0) -> Opcode, acc(1) -> reg_in, acc(2) -> reg_out, etc.
                opcode = intmask(acc.get_int(1))
                instr = None

                if opcode == instructions.Instruction.FILTER:
                    rid_in = intmask(acc.get_int(2))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in

                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out

                    instr = instructions.FilterOp(
                        r_in, r_out, _get_scalar_func(intmask(acc.get_int(15)))
                    )

                elif opcode == instructions.Instruction.MAP:
                    rid_in = intmask(acc.get_int(2))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_in] = r_in

                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out

                    instr = instructions.MapOp(
                        r_in, r_out, _get_scalar_func(intmask(acc.get_int(15)))
                    )

                elif opcode == instructions.Instruction.NEGATE:
                    rid_in = intmask(acc.get_int(2))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.NegateOp(r_in, r_out)

                elif opcode == instructions.Instruction.UNION:
                    rid_in_a = intmask(acc.get_int(4))
                    rid_in_b = intmask(acc.get_int(5))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in_a = reg_file.registers[rid_in_a]
                    if r_in_a is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_in_a = runtime.DeltaRegister(rid_in_a, runtime.VMSchema(sch))
                        reg_file.registers[rid_in_a] = r_in_a
                    r_in_b = reg_file.registers[rid_in_b]
                    if r_in_b is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_in_b = runtime.DeltaRegister(rid_in_b, runtime.VMSchema(sch))
                        reg_file.registers[rid_in_b] = r_in_b
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.UnionOp(r_in_a, r_in_b, r_out)

                elif opcode == instructions.Instruction.JOIN_DELTA_TRACE:
                    rid_delta = intmask(acc.get_int(9))
                    rid_trace = intmask(acc.get_int(6))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_delta = reg_file.registers[rid_delta]
                    if r_delta is None:
                        r_delta = runtime.DeltaRegister(rid_delta, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_delta] = r_delta

                    r_trace = reg_file.registers[rid_trace]
                    if r_trace is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_trace = runtime.TraceRegister(rid_trace, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_trace] = r_trace

                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.JoinDeltaTraceOp(r_delta, r_trace, r_out)

                elif opcode == instructions.Instruction.JOIN_DELTA_DELTA:
                    rid_a = intmask(acc.get_int(11))
                    rid_b = intmask(acc.get_int(12))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_a = reg_file.registers[rid_a]
                    if r_a is None:
                        r_a = runtime.DeltaRegister(rid_a, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_a] = r_a
                    r_b = reg_file.registers[rid_b]
                    if r_b is None:
                        r_b = runtime.DeltaRegister(rid_b, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_b] = r_b
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.JoinDeltaDeltaOp(r_a, r_b, r_out)

                elif opcode == instructions.Instruction.INTEGRATE:
                    rid_in = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(14))
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                    target = self.registry.get_by_id(tid) if self.registry.has_id(tid) else None
                    instr = instructions.IntegrateOp(r_in, target)

                elif opcode == instructions.Instruction.DELAY:
                    rid_in = intmask(acc.get_int(2))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else in_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.DelayOp(r_in, r_out)

                elif opcode == instructions.Instruction.REDUCE:
                    rid_in = intmask(acc.get_int(2))
                    rid_tr_in = intmask(acc.get_int(7))
                    rid_tr_out = intmask(acc.get_int(8))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_in] = r_in

                    r_tr_in = reg_file.registers[rid_tr_in]
                    if r_tr_in is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_tr_in = runtime.TraceRegister(rid_tr_in, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_tr_in] = r_tr_in

                    r_tr_out = reg_file.registers[rid_tr_out]
                    if r_tr_out is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_tr_out = runtime.TraceRegister(rid_tr_out, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_tr_out] = r_tr_out

                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_out] = r_out

                    instr = instructions.ReduceOp(
                        r_in, r_tr_in, r_tr_out, r_out,
                        self._parse_group_by_cols(sys.read_string(acc, 17)),
                        _get_agg_func(intmask(acc.get_int(16))),
                        out_schema,
                    )

                elif opcode == instructions.Instruction.DISTINCT:
                    rid_in = intmask(acc.get_int(2))
                    rid_hist = intmask(acc.get_int(10))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))

                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_in] = r_in
                    r_hist = reg_file.registers[rid_hist]
                    if r_hist is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_hist = runtime.TraceRegister(rid_hist, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_hist] = r_hist
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.DistinctOp(r_in, r_hist, r_out)

                elif opcode == instructions.Instruction.SCAN_TRACE:
                    rid_tr = intmask(acc.get_int(6))
                    rid_out = intmask(acc.get_int(3))
                    tid = intmask(acc.get_int(14))
                    r_tr = reg_file.registers[rid_tr]
                    if r_tr is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_tr = runtime.TraceRegister(rid_tr, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_tr] = r_tr
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.ScanTraceOp(r_tr, r_out, intmask(acc.get_int(18)))

                elif opcode == instructions.Instruction.SEEK_TRACE:
                    rid_tr = intmask(acc.get_int(6))
                    rid_key = intmask(acc.get_int(13))
                    tid = intmask(acc.get_int(14))
                    r_tr = reg_file.registers[rid_tr]
                    if r_tr is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_tr = runtime.TraceRegister(rid_tr, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_tr] = r_tr
                    r_key = reg_file.registers[rid_key]
                    if r_key is None:
                        r_key = runtime.DeltaRegister(rid_key, runtime.VMSchema(in_schema))
                        reg_file.registers[rid_key] = r_key
                    instr = instructions.SeekTraceOp(r_tr, r_key)

                elif opcode == instructions.Instruction.YIELD:
                    instr = instructions.YieldOp(intmask(acc.get_int(20)))
                elif opcode == instructions.Instruction.JUMP:
                    instr = instructions.JumpOp(intmask(acc.get_int(19)))
                elif opcode == instructions.Instruction.CLEAR_DELTAS:
                    instr = instructions.ClearDeltasOp()
                elif opcode == instructions.Instruction.HALT:
                    instr = instructions.HaltOp()

                if instr is not None:
                    program.append(instr)
                cursor.advance()

        finally:
            cursor.close()

        if len(program) == 0:
            return None

        out_reg_idx = 1
        for scan_instr in program:
            if scan_instr.opcode == instructions.Instruction.INTEGRATE:
                if scan_instr.reg_in is not None:
                    out_reg_idx = scan_instr.reg_in.reg_id
                break

        return runtime.ExecutablePlan(program, reg_file, out_schema, 0, out_reg_idx)
