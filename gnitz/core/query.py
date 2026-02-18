# gnitz/core/query.py

from gnitz.core import types, zset, scalar
from gnitz.vm import instructions, runtime, interpreter, batch


class QueryError(Exception):
    pass


def merge_schemas_for_join(schema_left, schema_right):
    """
    Constructs the resulting schema for a Join operation.
    Output Schema = [PK] + [Left Non-PK Columns] + [Right Non-PK Columns].
    The PK type matches the input PKs (which must match).
    The new PK index is normalized to 0.
    """
    pk_left = schema_left.get_pk_column()
    pk_right = schema_right.get_pk_column()

    # Strict Type Check
    if pk_left.field_type.code != pk_right.field_type.code:
        raise QueryError(
            "Join PK Type Mismatch: Left=%d Right=%d"
            % (pk_left.field_type.code, pk_right.field_type.code)
        )

    new_columns = []

    # 1. Add PK (Always at index 0 in result for simplicity)
    new_columns.append(pk_left)

    # 2. Add Non-PK columns from Left
    for i in range(len(schema_left.columns)):
        if i == schema_left.pk_index:
            continue
        new_columns.append(schema_left.columns[i])

    # 3. Add Non-PK columns from Right
    for i in range(len(schema_right.columns)):
        if i == schema_right.pk_index:
            continue
        new_columns.append(schema_right.columns[i])

    return types.TableSchema(new_columns, pk_index=0)


class View(object):
    """
    Represents a compiled DBSP circuit.

    Attributes:
        interpreter (DBSPInterpreter): The VM instance.
        input_reg_id (int): Register ID for input injection.
        output_reg_id (int): Register ID for result extraction.
        cursors (list): Persistent cursors kept alive for the view.
    """

    def __init__(self, interp, input_reg_id, output_reg_id, cursors):
        self.interpreter = interp
        self.input_reg_id = input_reg_id
        self.output_reg_id = output_reg_id
        self.cursors = cursors

    def process(self, delta_batch):
        """
        Pushes a delta batch through the circuit.
        Returns the resulting ZSetBatch from the output register.
        """
        # Execute the circuit
        # The interpreter handles loading delta_batch into input_reg_id (Reg 0)
        self.interpreter.execute(delta_batch)

        # Retrieve result
        out_reg = self.interpreter.register_file.get_register(self.output_reg_id)
        if not out_reg.is_delta():
            raise QueryError("Output register is not a Delta register")

        # In RPython, we cast to DeltaRegister to access the batch
        assert isinstance(out_reg, runtime.DeltaRegister)
        return out_reg.batch

    def close(self):
        """Releases persistent resources (cursors)."""
        # Cursors are typically managed by the Engine/RefCounter
        pass


class QueryBuilder(object):
    """
    Fluent API to construct VM circuits.
    """

    def __init__(self, engine, source_schema):
        self.engine = engine
        self.program = []
        self.registers = []  # List[BaseRegister]
        self.cursors = []  # List[UnifiedCursor]

        # Initialize Register 0 (Input)
        vm_schema_0 = runtime.VMSchema(source_schema)
        reg_0 = runtime.DeltaRegister(0, vm_schema_0)
        self.registers.append(reg_0)

        self.current_reg_idx = 0

    def _add_register(self, table_schema, is_trace=False, cursor=None):
        idx = len(self.registers)
        vm_schema = runtime.VMSchema(table_schema)

        if is_trace:
            reg = runtime.TraceRegister(idx, vm_schema, cursor)
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

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.DistinctOp(prev_reg, new_reg)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def delay(self):
        """
        Implements the z^-1 (unit delay) operator.
        The content of the current register at tick T becomes available at tick T+1.
        Essential for recursive circuits and fixed-point iteration.
        """
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

    def join_delta(self, other_delta_reg, output_schema):
        """
        Joins the current delta stream with another delta register
        (JOIN_DELTA_DELTA). Used for bilinear join expansion when both
        inputs are transient batches rather than a batch and a persistent trace.
        """
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("JoinDeltaDelta left input must be a Delta stream")

        out_idx, out_reg = self._add_register(output_schema)
        op = instructions.JoinDeltaDeltaOp(prev_reg, other_delta_reg, out_reg)
        self.program.append(op)

        self.current_reg_idx = out_idx
        return self

    def union(self, other_reg=None):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Union input must be a Delta stream")

        idx, new_reg = self._add_register(prev_reg.vm_schema.table_schema)
        op = instructions.UnionOp(prev_reg, other_reg, new_reg)
        self.program.append(op)

        self.current_reg_idx = idx
        return self

    def build(self):
        """Finalizes the circuit and returns a View."""
        # 1. Add HALT instruction
        self.program.append(instructions.HaltOp())

        # 2. Convert resizable list to fixed-size list for VM immutable fields
        num_ops = len(self.program)
        final_program = [None] * num_ops
        for i in range(num_ops):
            final_program[i] = self.program[i]

        # 3. Construct RegisterFile
        reg_file = runtime.RegisterFile(len(self.registers))
        for i in range(len(self.registers)):
            reg_file.registers[i] = self.registers[i]

        # 4. Create Interpreter
        interp = interpreter.DBSPInterpreter(self.engine, reg_file, final_program)

        return View(interp, 0, self.current_reg_idx, self.cursors)

    def sink(self, table):
        prev_reg = self.registers[self.current_reg_idx]
        if not prev_reg.is_delta():
            raise QueryError("Sink input must be a Delta stream")

        op = instructions.IntegrateOp(prev_reg, table.engine)
        self.program.append(op)

        return self
