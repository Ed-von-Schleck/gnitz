# gnitz/core/query.py

from gnitz.core import types, zset, scalar
from gnitz.vm import instructions, runtime, interpreter, batch


class QueryError(Exception):
    pass


def merge_schemas_for_join(schema_left, schema_right):
    """
    Constructs the resulting schema for a Join operation.
    Output Schema = [PK] + [Left Non-PK Columns] + [Right Non-PK Columns].
    """
    pk_left = schema_left.get_pk_column()
    pk_right = schema_right.get_pk_column()

    if pk_left.field_type.code != pk_right.field_type.code:
        raise QueryError(
            "Join PK Type Mismatch: Left=%d Right=%d"
            % (pk_left.field_type.code, pk_right.field_type.code)
        )

    new_columns = []
    new_columns.append(pk_left)

    for i in range(len(schema_left.columns)):
        if i == schema_left.pk_index:
            continue
        new_columns.append(schema_left.columns[i])

    for i in range(len(schema_right.columns)):
        if i == schema_right.pk_index:
            continue
        new_columns.append(schema_right.columns[i])

    return types.TableSchema(new_columns, pk_index=0)


class View(object):
    def __init__(self, interp, input_reg_id, output_reg_id, cursors):
        self.interpreter = interp
        self.input_reg_id = input_reg_id
        self.output_reg_id = output_reg_id
        self.cursors = cursors

    def process(self, delta_batch):
        self.interpreter.execute(delta_batch)
        out_reg = self.interpreter.register_file.get_register(self.output_reg_id)
        if not out_reg.is_delta():
            raise QueryError("Output register is not a Delta register")

        assert isinstance(out_reg, runtime.DeltaRegister)
        return out_reg.batch

    def close(self):
        pass


class QueryBuilder(object):
    def __init__(self, engine, source_schema):
        self.engine = engine
        self.program = []
        self.registers = []  # List[BaseRegister]
        self.cursors = []  # List[UnifiedCursor]

        vm_schema_0 = runtime.VMSchema(source_schema)
        reg_0 = runtime.DeltaRegister(0, vm_schema_0)
        self.registers.append(reg_0)

        self.current_reg_idx = 0

    def _add_register(self, table_schema, is_trace=False, cursor=None, table=None):
        idx = len(self.registers)
        vm_schema = runtime.VMSchema(table_schema)

        if is_trace:
            # Pass the table reference to the TraceRegister
            reg = runtime.TraceRegister(idx, vm_schema, cursor, table)
        else:
            reg = runtime.DeltaRegister(idx, vm_schema)

        self.registers.append(reg)
        return idx, reg

    def filter(self, predicate):
        if not isinstance(predicate, scalar.ScalarFunction):
            raise QueryError("Predicate must be a ScalarFunction")

        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Filter input must be a Delta stream")

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.FilterOp(prev_reg, new_reg, predicate)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def map(self, mapper, output_schema):
        if not isinstance(mapper, scalar.ScalarFunction):
            raise QueryError("Mapper must be a ScalarFunction")

        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Map input must be a Delta stream")

        idx, new_reg = self._add_register(output_schema)
        op = instructions.MapOp(prev_reg, new_reg, mapper)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def negate(self):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Negate input must be a Delta stream")

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.NegateOp(prev_reg, new_reg)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def distinct(self):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Distinct input must be a Delta stream")

        schema = prev_reg.vm_schema.table_schema
        table_name = "_internal_distinct_%d" % len(self.program)
        history_table = zset.PersistentTable("vm_internal_state", table_name, schema)
        
        cursor = history_table.create_cursor()
        self.cursors.append(cursor)
        
        # Pass history_table into _add_register
        trace_idx, trace_reg = self._add_register(schema, is_trace=True, cursor=cursor, table=history_table)
        out_idx, out_reg = self._add_register(schema)
        
        op = instructions.DistinctOp(prev_reg, trace_reg, out_reg)
        self.program.append(op)

        self.current_reg_idx = out_idx
        return self

    def delay(self):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Delay input must be a Delta stream")

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.DelayOp(prev_reg, new_reg)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def join_persistent(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Join input must be a Delta stream")

        cursor = table.create_cursor()
        self.cursors.append(cursor)
        trace_idx, trace_reg = self._add_register(
            table.schema, is_trace=True, cursor=cursor
        )

        out_schema = merge_schemas_for_join(
            prev_reg.vm_schema.table_schema, table.schema
        )

        out_idx, out_reg = self._add_register(out_schema)
        op = instructions.JoinDeltaTraceOp(prev_reg, trace_reg, out_reg)
        self.program.append(op)

        self.current_reg_idx = out_idx
        return self
        
    def union(self, other_builder=None):
        """
        Algebraically sums two Z-Set streams.
        If other_builder is None, it acts as a pass-through to the next operator.
        """
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Union primary input must be a Delta stream")

        other_reg = None
        if other_builder is not None:
            if not isinstance(other_builder, QueryBuilder):
                raise QueryError("Union argument must be a QueryBuilder instance")
            
            other_reg = other_builder.registers[other_builder.current_reg_idx]
            if not other_reg.is_delta():
                raise QueryError("Union secondary input must be a Delta stream")
            
            # Structural Integrity: In DBSP, Union requires identical schemas.
            # We verify the memory layout (stride) matches before building the op.
            if prev_reg.vm_schema.table_schema.stride != other_reg.vm_schema.table_schema.stride:
                raise QueryError("Union schema mismatch: physical strides do not align")

        # Create output register inheriting the primary input's schema
        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        
        op = instructions.UnionOp(prev_reg, other_reg, new_reg)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def build(self):
        self.program.append(instructions.HaltOp())

        num_ops = len(self.program)
        final_program = [None] * num_ops
        for i in range(num_ops):
            final_program[i] = self.program[i]

        reg_file = runtime.RegisterFile(len(self.registers))
        for i in range(len(self.registers)):
            reg_file.registers[i] = self.registers[i]

        interp = interpreter.DBSPInterpreter(self.engine, reg_file, final_program)
        return View(interp, 0, self.current_reg_idx, self.cursors)

    def sink(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Sink input must be a Delta stream")

        op = instructions.IntegrateOp(prev_reg, table.engine)
        self.program.append(op)

        return self
