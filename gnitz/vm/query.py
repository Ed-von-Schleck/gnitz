# gnitz/vm/query.py

from gnitz.core import types
from gnitz.vm import instructions, runtime, interpreter


class QueryError(Exception):
    pass


class View(object):
    """
    A compiled execution handle for a DBSP circuit.
    Wraps an ExecutablePlan to provide an interactive or push-based API.
    """

    def __init__(self, plan, cursors):
        self.plan = plan
        self.cursors = cursors

    def run(self):
        """Runs or resumes the circuit until a YIELD or HALT."""
        # Use the module-level function and the plan's internal state
        interpreter.run_vm(self.plan.program, self.plan.reg_file, self.plan.context)
        return self.plan.context.status

    def process(self, delta_batch):
        """
        Processes a single batch of updates through the circuit.
        Utilizes the zero-copy bind mechanism in ExecutablePlan.
        """
        # execute_epoch handles prepare_for_tick, binding, running, and cloning the result.
        return self.plan.execute_epoch(delta_batch)

    def close(self):
        """Closes all persistent cursors and frees transient batches."""
        for cursor in self.cursors:
            cursor.close()
        
        # Free all transient arenas owned by DeltaRegisters
        for reg in self.plan.reg_file.registers:
            if reg is not None and reg.is_delta():
                # We access the internal batch specifically to ensure the 
                # physical memory arena is released.
                if hasattr(reg, '_internal_batch'):
                    reg._internal_batch.free()
                elif reg.batch is not None:
                    reg.batch.free()


class QueryBuilder(object):
    """
    Fluent API for constructing incremental DBSP circuits.
    """

    def __init__(self, source_table):
        self._source_table = source_table
        self.instructions = []
        self.registers = []
        self.cursors = []
        self._built = False

        # Register 0 is conventionally the input Delta stream
        vm_schema_0 = runtime.VMSchema(source_table.get_schema())
        reg_0 = runtime.DeltaRegister(0, vm_schema_0)
        self.registers.append(reg_0)
        self.current_reg_idx = 0

    def _add_register(self, table_schema, is_trace=False, cursor=None, table=None):
        idx = len(self.registers)
        vm_schema = runtime.VMSchema(table_schema)
        if is_trace:
            reg = runtime.TraceRegister(idx, vm_schema, cursor, table)
        else:
            reg = runtime.DeltaRegister(idx, vm_schema)
        self.registers.append(reg)
        return idx, reg

    # ── Control Flow & State Management ───────────────────────────────

    def label(self):
        return len(self.instructions)

    def yield_(self, reason=runtime.YIELD_REASON_USER):
        self.instructions.append(instructions.YieldOp(reason))
        return self

    def jump(self, target_idx):
        self.instructions.append(instructions.JumpOp(target_idx))
        return self

    def clear_deltas(self):
        self.instructions.append(instructions.ClearDeltasOp())
        return self

    # ── Source & Leaf Operators ───────────────────────────────────────

    def scan(self, table, chunk_limit=1000):
        schema = table.get_schema()
        cursor = table.create_cursor()
        self.cursors.append(cursor)

        tr_idx, tr_reg = self._add_register(schema, is_trace=True, cursor=cursor, table=table)
        return self.scan_trace(tr_idx, chunk_limit)

    def scan_trace(self, trace_reg_idx, chunk_limit=1000):
        tr_reg = self.registers[trace_reg_idx]
        if not tr_reg.is_trace():
            raise QueryError("scan_trace requires a Trace register")

        out_idx, out_reg = self._add_register(tr_reg.vm_schema.table_schema)
        self.instructions.append(instructions.ScanTraceOp(tr_reg, out_reg, chunk_limit))
        self.current_reg_idx = out_idx
        return self

    def seek_trace(self, trace_reg_idx, key_reg_idx):
        t_reg = self.registers[trace_reg_idx]
        k_reg = self.registers[key_reg_idx]
        self.instructions.append(instructions.SeekTraceOp(t_reg, k_reg))
        return self

    def sink(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        self.instructions.append(instructions.IntegrateOp(prev_reg, table))
        return self

    # ── Transformation Operators ──────────────────────────────────────

    def filter(self, predicate):
        prev_reg = self.registers[self.current_reg_idx]
        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        self.instructions.append(instructions.FilterOp(prev_reg, new_reg, predicate))
        self.current_reg_idx = idx
        return self

    def map(self, mapper, output_schema):
        prev_reg = self.registers[self.current_reg_idx]
        idx, new_reg = self._add_register(output_schema)
        self.instructions.append(instructions.MapOp(prev_reg, new_reg, mapper))
        self.current_reg_idx = idx
        return self

    def negate(self):
        prev_reg = self.registers[self.current_reg_idx]
        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        self.instructions.append(instructions.NegateOp(prev_reg, new_reg))
        self.current_reg_idx = idx
        return self

    def union(self, other_builder=None):
        prev_reg = self.registers[self.current_reg_idx]
        other_reg = None
        if other_builder is not None:
            other_reg = other_builder.registers[other_builder.current_reg_idx]

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        self.instructions.append(instructions.UnionOp(prev_reg, other_reg, new_reg))
        self.current_reg_idx = idx
        return self

    def distinct(self):
        prev_reg = self.registers[self.current_reg_idx]
        schema = prev_reg.vm_schema.table_schema
        table_name = "_internal_distinct_%d" % len(self.instructions)
        history_table = self._source_table.create_child(table_name, schema)
        cursor = history_table.create_cursor()
        self.cursors.append(cursor)

        trace_idx, trace_reg = self._add_register(
            schema, is_trace=True, cursor=cursor, table=history_table
        )
        out_idx, out_reg = self._add_register(schema)
        self.instructions.append(instructions.DistinctOp(prev_reg, trace_reg, out_reg))
        self.current_reg_idx = out_idx
        return self

    def reduce(self, group_by_cols, agg_func, reg_trace_in_idx=-1):
        prev_reg = self.registers[self.current_reg_idx]
        input_schema = prev_reg.vm_schema.table_schema

        if not agg_func.is_linear() and reg_trace_in_idx == -1:
            raise QueryError("Non-linear aggregate requires reg_trace_in (input history)")

        out_schema = types._build_reduce_output_schema(input_schema, group_by_cols, agg_func)
        out_idx, reg_out = self._add_register(out_schema)

        trace_name = "_internal_reduce_trace_%d" % len(self.instructions)
        trace_table = self._source_table.create_child(trace_name, out_schema)
        cursor = trace_table.create_cursor()
        self.cursors.append(cursor)

        tr_out_idx, reg_tr_out = self._add_register(
            out_schema, is_trace=True, cursor=cursor, table=trace_table
        )

        reg_trace_in = None
        if reg_trace_in_idx != -1:
            reg_trace_in = self.registers[reg_trace_in_idx]
            if not reg_trace_in.is_trace():
                raise QueryError("reg_trace_in must be a Trace register")

        self.instructions.append(instructions.ReduceOp(
            prev_reg, reg_trace_in, reg_tr_out, reg_out,
            group_by_cols, agg_func, out_schema
        ))

        # Integration of internal reduce state
        self.instructions.append(instructions.IntegrateOp(reg_out, trace_table))

        self.current_reg_idx = out_idx
        return self

    def join_persistent(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        cursor = table.create_cursor()
        self.cursors.append(cursor)
        trace_idx, trace_reg = self._add_register(
            table.get_schema(), is_trace=True, cursor=cursor, table=table
        )

        out_schema = types.merge_schemas_for_join(
            prev_reg.vm_schema.table_schema, table.get_schema()
        )
        out_idx, out_reg = self._add_register(out_schema)

        self.instructions.append(instructions.JoinDeltaTraceOp(prev_reg, trace_reg, out_reg))
        self.current_reg_idx = out_idx
        return self

    # ── Finalization ──────────────────────────────────────────────────

    def build(self):
        if not self._built:
            self.instructions.append(instructions.HaltOp())
            self._built = True

        # Convert to fixed-size list for RPython
        program = [None] * len(self.instructions)
        for i in range(len(self.instructions)):
            program[i] = self.instructions[i]

        reg_file = runtime.RegisterFile(len(self.registers))
        for i in range(len(self.registers)):
            reg_file.registers[i] = self.registers[i]

        # The last register defined is the output register.
        # Register 0 is conventionally the input.
        out_reg = self.registers[self.current_reg_idx]
        
        plan = runtime.ExecutablePlan(
            program, 
            reg_file, 
            out_reg.vm_schema.table_schema,
            in_reg_idx=0,
            out_reg_idx=self.current_reg_idx
        )

        return View(plan, self.cursors)
