# gnitz/catalog/program_cache.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask, r_int64
from rpython.rtyper.lltypesystem import rffi

from gnitz.core import strings
from gnitz.catalog.system_tables import SYS_TABLE_INSTRUCTIONS
from gnitz.vm import instructions, runtime
from gnitz.dbsp import functions


# Singletons to prevent allocation during VM setup
NULL_PREDICATE = functions.NullPredicate()
NULL_AGGREGATE = functions.NullAggregate()

def _get_scalar_func(func_id):
    # Logic to look up func in registry...
    # if not found:
    return NULL_PREDICATE

def _get_agg_func(agg_func_id):
    # Logic to look up agg in registry...
    # if not found:
    return NULL_AGGREGATE


class ExecutablePlan(object):
    """
    An immutable, pre-compiled execution context.
    The VM Executor directly overlays incoming Deltas into `reg_file.registers[0]` 
    and runs the DAG without any allocation overhead.
    """
    _immutable_fields_ = ["program", "reg_file", "in_schema", "out_schema"]

    def __init__(self, program, reg_file, in_schema, out_schema):
        self.program = program
        self.reg_file = reg_file
        self.in_schema = in_schema
        self.out_schema = out_schema


class ProgramCache(object):
    """
    Caches execution plans for Reactive Views.
    Translates rows from `_system._instructions` into `ExecutablePlan` objects
    containing monomorphic `Instruction` lists and pre-allocated `RegisterFile`s.
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

    def _read_string(self, acc, col_idx):
        length, prefix, struct_ptr, heap_ptr, py_string = acc.get_str_struct(col_idx)
        if py_string is not None:
            return py_string
        if rffi.cast(rffi.SIZE_T, struct_ptr) == 0:
            return ""
        return strings.unpack_string(struct_ptr, heap_ptr)

    def _parse_group_by_cols(self, s):
        if not s:
            return newlist_hint(0)
        parts = s.split(",")
        res = newlist_hint(len(parts))
        for p in parts:
            if p:
                res.append(int(p))
        return res

    def _load_program(self, program_id):
        if not self.registry.has_id(SYS_TABLE_INSTRUCTIONS):
            return None

        if not self.registry.has_id(program_id):
            return None
            
        view_family = self.registry.get_by_id(program_id)
        out_schema = view_family.schema
        in_schema = out_schema 

        sys_instr = self.registry.get_by_id(SYS_TABLE_INSTRUCTIONS)
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
                opcode = intmask(acc.get_int(0))
                
                # --- Helper logic inlined to avoid RPython Closures ---
                
                instr = None

                if opcode == instructions.Instruction.FILTER:
                    rid_in = intmask(acc.get_int(1))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    # Manual Register Acquisition
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                        
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                        
                    instr = instructions.FilterOp(r_in, r_out, _get_scalar_func(intmask(acc.get_int(14))))

                elif opcode == instructions.Instruction.MAP:
                    rid_in = intmask(acc.get_int(1))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                        
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    
                    instr = instructions.MapOp(r_in, r_out, _get_scalar_func(intmask(acc.get_int(14))))

                elif opcode == instructions.Instruction.NEGATE:
                    rid_in = intmask(acc.get_int(1))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.NegateOp(r_in, r_out)

                elif opcode == instructions.Instruction.UNION:
                    rid_in_a = intmask(acc.get_int(3))
                    rid_in_b = intmask(acc.get_int(4))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_in_a = reg_file.registers[rid_in_a]
                    if r_in_a is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in_a = runtime.DeltaRegister(rid_in_a, runtime.VMSchema(sch))
                        reg_file.registers[rid_in_a] = r_in_a
                    r_in_b = reg_file.registers[rid_in_b]
                    if r_in_b is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in_b = runtime.DeltaRegister(rid_in_b, runtime.VMSchema(sch))
                        reg_file.registers[rid_in_b] = r_in_b
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.UnionOp(r_in_a, r_in_b, r_out)

                elif opcode == instructions.Instruction.JOIN_DELTA_TRACE:
                    rid_delta = intmask(acc.get_int(8))
                    rid_trace = intmask(acc.get_int(5))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_delta = reg_file.registers[rid_delta]
                    if r_delta is None:
                        r_delta = runtime.DeltaRegister(rid_delta, runtime.VMSchema(out_schema))
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
                    rid_a = intmask(acc.get_int(10))
                    rid_b = intmask(acc.get_int(11))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_a = reg_file.registers[rid_a]
                    if r_a is None:
                        r_a = runtime.DeltaRegister(rid_a, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_a] = r_a
                    r_b = reg_file.registers[rid_b]
                    if r_b is None:
                        r_b = runtime.DeltaRegister(rid_b, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_b] = r_b
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.JoinDeltaDeltaOp(r_a, r_b, r_out)

                elif opcode == instructions.Instruction.INTEGRATE:
                    rid_in = intmask(acc.get_int(1))
                    tid = intmask(acc.get_int(13))
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                    target = self.registry.get_by_id(tid) if self.registry.has_id(tid) else None
                    instr = instructions.IntegrateOp(r_in, target)

                elif opcode == instructions.Instruction.DELAY:
                    rid_in = intmask(acc.get_int(1))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
                        reg_file.registers[rid_in] = r_in
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(sch))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.DelayOp(r_in, r_out)

                elif opcode == instructions.Instruction.REDUCE:
                    rid_in = intmask(acc.get_int(1))
                    rid_tr_in = intmask(acc.get_int(6))
                    rid_tr_out = intmask(acc.get_int(7))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        sch = self.registry.get_by_id(tid).schema if tid > 0 else out_schema
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(sch))
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
                        
                    instr = instructions.ReduceOp(r_in, r_tr_in, r_tr_out, r_out, 
                                                 self._parse_group_by_cols(self._read_string(acc, 16)), 
                                                 _get_agg_func(intmask(acc.get_int(15))), out_schema)

                elif opcode == instructions.Instruction.DISTINCT:
                    rid_in = intmask(acc.get_int(1))
                    rid_hist = intmask(acc.get_int(9))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
                    r_in = reg_file.registers[rid_in]
                    if r_in is None:
                        r_in = runtime.DeltaRegister(rid_in, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_in] = r_in
                    r_hist = reg_file.registers[rid_hist]
                    if r_hist is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_hist = runtime.TraceRegister(rid_hist, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_hist] = r_hist
                    r_out = reg_file.registers[rid_out]
                    if r_out is None:
                        r_out = runtime.DeltaRegister(rid_out, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_out] = r_out
                    instr = instructions.DistinctOp(r_in, r_hist, r_out)

                elif opcode == instructions.Instruction.SCAN_TRACE:
                    rid_tr = intmask(acc.get_int(5))
                    rid_out = intmask(acc.get_int(2))
                    tid = intmask(acc.get_int(13))
                    
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
                    instr = instructions.ScanTraceOp(r_tr, r_out, intmask(acc.get_int(17)))

                elif opcode == instructions.Instruction.SEEK_TRACE:
                    rid_tr = intmask(acc.get_int(5))
                    rid_key = intmask(acc.get_int(12))
                    tid = intmask(acc.get_int(13))
                    r_tr = reg_file.registers[rid_tr]
                    if r_tr is None and tid > 0:
                        f = self.registry.get_by_id(tid)
                        r_tr = runtime.TraceRegister(rid_tr, runtime.VMSchema(f.schema), f.create_cursor(), f)
                        reg_file.registers[rid_tr] = r_tr
                    r_key = reg_file.registers[rid_key]
                    if r_key is None:
                        r_key = runtime.DeltaRegister(rid_key, runtime.VMSchema(out_schema))
                        reg_file.registers[rid_key] = r_key
                    instr = instructions.SeekTraceOp(r_tr, r_key)

                elif opcode == instructions.Instruction.YIELD:
                    instr = instructions.YieldOp(intmask(acc.get_int(19)))
                elif opcode == instructions.Instruction.JUMP:
                    instr = instructions.JumpOp(intmask(acc.get_int(18)))
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
        return ExecutablePlan(program, reg_file, in_schema, out_schema)
