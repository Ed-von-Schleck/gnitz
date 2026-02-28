# gnitz/vm/query.py

from gnitz.core import types
from gnitz.vm import instructions, runtime, interpreter

class QueryError(Exception):
    pass

class View(object):
    """
    A compiled execution handle for a DBSP circuit.
    Holds the ExecutionContext, allowing the query to be paused and resumed.
    """
    def __init__(self, interp, context, input_reg_id, output_reg_id, cursors):
        self.interpreter = interp
        self.context = context
        self.input_reg_id = input_reg_id
        self.output_reg_id = output_reg_id
        self.cursors = cursors

    def run(self):
        """Runs or resumes the circuit until a YIELD or HALT."""
        self.interpreter.resume(self.context)
        return self.context.status

    def process(self, delta_batch):
        """
        Processes a single batch of updates through the circuit.
        Used for standard push-based incremental computation.
        """
        # 1. Reset context and clear transient state
        self.context.reset(pc=0)
        self.context.reg_file.clear_all_deltas()

        # 2. Ingest data into the input register
        reg0 = self.context.reg_file.get_register(self.input_reg_id)
        assert isinstance(reg0, runtime.DeltaRegister)
        
        for i in range(delta_batch.length()):
            reg0.batch.append_from_accessor(
                delta_batch.get_pk(i),
                delta_batch.get_weight(i),
                delta_batch.get_accessor(i),
            )

        # 3. Execute
        self.run()

        # 4. Return result
        out_reg = self.context.reg_file.get_register(self.output_reg_id)
        if not out_reg.is_delta():
            raise QueryError("Output register is not a Delta register")
        return out_reg.batch

    def close(self):
        """Closes all persistent cursors and frees transient batches."""
        for cursor in self.cursors:
            cursor.close()
        for reg in self.context.reg_file.registers:
            if reg is not None and reg.is_delta():
                if reg.batch is not None:
                    reg.batch.free()


class QueryBuilder(object):
    """
    Fluent API for constructing incremental DBSP circuits.
    Supports chunked execution, explicit control flow, and standard operators.
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

    # ------------------------------------------------------------------
    # Control Flow & State Management
    # ------------------------------------------------------------------

    def label(self):
        """Returns the index of the next instruction (for JUMP targets)."""
        return len(self.instructions)

    def yield_(self, reason=runtime.YIELD_REASON_USER):
        op = instructions.YieldOp(reason)
        self.instructions.append(op)
        return self

    def jump(self, target_idx):
        op = instructions.JumpOp(target_idx)
        self.instructions.append(op)
        return self

    def clear_deltas(self):
        op = instructions.ClearDeltasOp()
        self.instructions.append(op)
        return self

    # ------------------------------------------------------------------
    # Source & Leaf Operators
    # ------------------------------------------------------------------

    def scan(self, table, chunk_limit=1000):
        """Creates a fresh cursor and scans from it."""
        schema = table.get_schema()
        cursor = table.create_cursor()
        self.cursors.append(cursor)
        
        tr_idx, tr_reg = self._add_register(schema, is_trace=True, cursor=cursor, table=table)
        return self.scan_trace(tr_idx, chunk_limit)

    def scan_trace(self, trace_reg_idx, chunk_limit=1000):
        """Scans from an existing TraceRegister (cursor)."""
        tr_reg = self.registers[trace_reg_idx]
        if not tr_reg.is_trace():
            raise QueryError("scan_trace requires a Trace register")
        
        out_idx, out_reg = self._add_register(tr_reg.vm_schema.table_schema)
        
        op = instructions.ScanTraceOp(tr_reg, out_reg, chunk_limit)
        self.instructions.append(op)
        self.current_reg_idx = out_idx
        return self

    def seek_trace(self, trace_reg_idx, key_reg_idx):
        t_reg = self.registers[trace_reg_idx]
        k_reg = self.registers[key_reg_idx]
        op = instructions.SeekTraceOp(t_reg, k_reg)
        self.instructions.append(op)
        return self

    def sink(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        op = instructions.IntegrateOp(prev_reg, table)
        self.instructions.append(op)
        return self

    # ------------------------------------------------------------------
    # Transformation Operators
    # ------------------------------------------------------------------

    def filter(self, predicate):
        prev_reg = self.registers[self.current_reg_idx]
        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.FilterOp(prev_reg, new_reg, predicate)
        self.instructions.append(op)
        self.current_reg_idx = idx
        return self

    def map(self, mapper, output_schema):
        prev_reg = self.registers[self.current_reg_idx]
        idx, new_reg = self._add_register(output_schema)
        op = instructions.MapOp(prev_reg, new_reg, mapper)
        self.instructions.append(op)
        self.current_reg_idx = idx
        return self

    def negate(self):
        prev_reg = self.registers[self.current_reg_idx]
        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.NegateOp(prev_reg, new_reg)
        self.instructions.append(op)
        self.current_reg_idx = idx
        return self

    def union(self, other_builder=None):
        prev_reg = self.registers[self.current_reg_idx]
        other_reg = None
        if other_builder is not None:
            other_reg = other_builder.registers[other_builder.current_reg_idx]
        
        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.UnionOp(prev_reg, other_reg, new_reg)
        self.instructions.append(op)
        self.current_reg_idx = idx
        return self

    def distinct(self):
        prev_reg = self.registers[self.current_reg_idx]
        schema = prev_reg.vm_schema.table_schema
        table_name = "_internal_distinct_%d" % len(self.instructions)
        history_table = self._source_table.create_child(table_name, schema)
        cursor = history_table.create_cursor()
        self.cursors.append(cursor)

        trace_idx, trace_reg = self._add_register(schema, is_trace=True, cursor=cursor, table=history_table)
        out_idx, out_reg = self._add_register(schema)
        op = instructions.DistinctOp(prev_reg, trace_reg, out_reg)
        self.instructions.append(op)
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

        tr_out_idx, reg_tr_out = self._add_register(out_schema, is_trace=True, cursor=cursor, table=trace_table)

        reg_trace_in = None
        if reg_trace_in_idx != -1:
            reg_trace_in = self.registers[reg_trace_in_idx]
            if not reg_trace_in.is_trace():
                raise QueryError("reg_trace_in must be a Trace register")

        op = instructions.ReduceOp(
            prev_reg, reg_trace_in, reg_tr_out, reg_out,
            group_by_cols, agg_func, out_schema
        )
        self.instructions.append(op)
        
        # Integration of internal reduce state
        sink_op = instructions.IntegrateOp(reg_out, trace_table)
        self.instructions.append(sink_op)

        self.current_reg_idx = out_idx
        return self

    def join_persistent(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        cursor = table.create_cursor()
        self.cursors.append(cursor)
        trace_idx, trace_reg = self._add_register(table.get_schema(), is_trace=True, cursor=cursor)

        out_schema = types.merge_schemas_for_join(prev_reg.vm_schema.table_schema, table.get_schema())
        out_idx, out_reg = self._add_register(out_schema)

        op = instructions.JoinDeltaTraceOp(prev_reg, trace_reg, out_reg)
        self.instructions.append(op)
        self.current_reg_idx = out_idx
        return self

    # ------------------------------------------------------------------
    # Finalization
    # ------------------------------------------------------------------

    def build(self):
        if not self._built:
            self.instructions.append(instructions.HaltOp())
            self._built = True

        program = [None] * len(self.instructions)
        for i in range(len(self.instructions)):
            program[i] = self.instructions[i]

        reg_file = runtime.RegisterFile(len(self.registers))
        for i in range(len(self.registers)):
            reg_file.registers[i] = self.registers[i]

        context = runtime.ExecutionContext(reg_file)
        interp = interpreter.DBSPInterpreter(program)
        
        return View(interp, context, 0, self.current_reg_idx, self.cursors)
