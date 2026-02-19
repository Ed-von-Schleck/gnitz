# gnitz/vm/query.py

from gnitz.core import types
from gnitz.backend.table import AbstractTable
from gnitz.vm import instructions, runtime, interpreter, batch, functions


class QueryError(Exception):
    pass


class View(object):
    """
    A compiled execution handle for a DBSP circuit.
    """
    def __init__(self, interp, input_reg_id, output_reg_id, cursors):
        self.interpreter = interp
        self.input_reg_id = input_reg_id
        self.output_reg_id = output_reg_id
        self.cursors = cursors

    def process(self, delta_batch):
        """Processes a single batch of updates through the circuit."""
        self.interpreter.execute(delta_batch)
        out_reg = self.interpreter.register_file.get_register(self.output_reg_id)
        if not out_reg.is_delta():
            raise QueryError("Output register is not a Delta register")

        assert isinstance(out_reg, runtime.DeltaRegister)
        return out_reg.batch

    def close(self):
        """Closes all persistent cursors associated with the view."""
        for cursor in self.cursors:
            cursor.close()


class QueryBuilder(object):
    """
    Fluent API for constructing incremental DBSP circuits.
    """
    def __init__(self, source_table, source_schema):
        self._source_table = source_table
        self.instructions = []
        self.registers = []  # List[BaseRegister]
        self.cursors = []    # List[UnifiedCursor]
        self._built = False

        vm_schema_0 = runtime.VMSchema(source_schema)
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

    def filter(self, predicate):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Filter input must be a Delta stream")

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.FilterOp(prev_reg, new_reg, predicate)
        self.instructions.append(op)

        self.current_reg_idx = idx
        return self

    def map(self, mapper, output_schema):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Map input must be a Delta stream")

        idx, new_reg = self._add_register(output_schema)
        op = instructions.MapOp(prev_reg, new_reg, mapper)
        self.instructions.append(op)

        self.current_reg_idx = idx
        return self

    def negate(self):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Negate input must be a Delta stream")

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.NegateOp(prev_reg, new_reg)
        self.instructions.append(op)

        self.current_reg_idx = idx
        return self

    def distinct(self):
        prev_reg = self.registers[self.current_reg_idx]
        schema = prev_reg.vm_schema.table_schema
        
        # Allocate internal state for Distinct
        table_name = "_internal_distinct_%d" % len(self.instructions)
        history_table = self._source_table.create_scratch_table(table_name, schema)
        cursor = history_table.create_cursor()
        self.cursors.append(cursor)
        
        trace_idx, trace_reg = self._add_register(schema, is_trace=True, cursor=cursor, table=history_table)
        out_idx, out_reg = self._add_register(schema)
        
        op = instructions.DistinctOp(prev_reg, trace_reg, out_reg)
        self.instructions.append(op)

        self.current_reg_idx = out_idx
        return self

    def reduce(self, group_by_cols, agg_func, reg_trace_in_idx=-1):
        """
        Groups the stream and applies an aggregate function.
        
        Args:
            group_by_cols: List of column indices to group by.
            agg_func: An AggregateFunction instance.
            reg_trace_in_idx: (Optional) Register index for input trace. 
                              Required for non-linear aggs (MIN/MAX).
        """
        prev_reg = self.registers[self.current_reg_idx]
        input_schema = prev_reg.vm_schema.table_schema
        
        if not agg_func.is_linear() and reg_trace_in_idx == -1:
            raise QueryError("Non-linear aggregate requires reg_trace_in (input history)")

        # 1. Define Output Schema
        out_schema = types._build_reduce_output_schema(input_schema, group_by_cols, agg_func)
        
        # 2. Allocate Output Delta Register
        out_idx, reg_out = self._add_register(out_schema)
        
        # 3. Allocate Output Trace (stores the current aggregates for retractions)
        trace_name = "_internal_reduce_trace_%d" % len(self.instructions)
        trace_table = self._source_table.create_scratch_table(trace_name, out_schema)
        cursor = trace_table.create_cursor()
        self.cursors.append(cursor)
        
        tr_out_idx, reg_tr_out = self._add_register(out_schema, is_trace=True, cursor=cursor, table=trace_table)
        
        # 4. Resolve Input Trace (if provided, e.g. for MIN/MAX)
        reg_trace_in = None
        if reg_trace_in_idx != -1:
            reg_trace_in = self.registers[reg_trace_in_idx]
            if not reg_trace_in.is_trace():
                raise QueryError("reg_trace_in must be a Trace register")

        # 5. Emit Reduce Instruction with all required fields for RPython immutability
        op = instructions.ReduceOp(
            prev_reg, 
            reg_trace_in, 
            reg_tr_out, 
            reg_out, 
            group_by_cols, 
            agg_func, 
            out_schema
        )
        self.instructions.append(op)
        
        # 6. Emit Integration: update the internal trace table with the results of this batch
        sink_op = instructions.IntegrateOp(reg_out, trace_table)
        self.instructions.append(sink_op)

        self.current_reg_idx = out_idx
        return self

    def join_persistent(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        cursor = table.create_cursor()
        self.cursors.append(cursor)
        trace_idx, trace_reg = self._add_register(table.schema, is_trace=True, cursor=cursor)

        out_schema = types.merge_schemas_for_join(prev_reg.vm_schema.table_schema, table.schema)
        out_idx, out_reg = self._add_register(out_schema)
        
        op = instructions.JoinDeltaTraceOp(prev_reg, trace_reg, out_reg)
        self.instructions.append(op)

        self.current_reg_idx = out_idx
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

    def sink(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        op = instructions.IntegrateOp(prev_reg, table)
        self.instructions.append(op)
        return self

    def build(self):
        """Finalizes the instruction sequence and returns a View."""
        if not self._built:
            self.instructions.append(instructions.HaltOp())
            self._built = True

        # Copy instructions to a fixed-size list for RPython
        final_program = [None] * len(self.instructions)
        for i in range(len(self.instructions)):
            final_program[i] = self.instructions[i]

        reg_file = runtime.RegisterFile(len(self.registers))
        for i in range(len(self.registers)):
            reg_file.registers[i] = self.registers[i]

        interp = interpreter.DBSPInterpreter(reg_file, final_program)
        return View(interp, 0, self.current_reg_idx, self.cursors)
