# gnitz/vm/interpreter.py

from rpython.rlib import jit

from gnitz.dbsp import ops
from gnitz.vm import instructions, runtime


def get_printable_location(pc, program, self):
    """
    Provides a trace label for the RPython JIT log.
    Identifies the instruction being compiled or executed.
    """
    instr = program[pc]
    return "PC: %d, Opcode: %d" % (pc, instr.opcode)


jitdriver = jit.JitDriver(
    greens=["pc", "program", "self"],
    reds=["reg_file"],
    get_printable_location=get_printable_location,
)


class DBSPInterpreter(object):
    """
    Incremental DBSP Execution Engine.

    Executes a static program of Z-Set transformations.  Each call to
    execute() corresponds to one LSN epoch (tick).

    This class is responsible solely for dispatch: it unpacks each
    instruction's register references into concrete batches, cursors, tables,
    and schemas, then delegates to the stateless operator functions in
    gnitz.dbsp.ops.  No operator logic lives here.
    """

    _immutable_fields_ = ["register_file", "program[*]"]

    def __init__(self, register_file, program):
        self.register_file = register_file
        self.program = program

    def execute(self, input_batch):
        """
        Executes the circuit for a single batch of input updates.

        1. Clears all transient delta registers for the new epoch.
        2. Ingests the raw updates into Register 0 (the circuit entry point).
        3. Iterates through the program instructions until HALT.
        """
        self.register_file.clear_all_deltas()

        reg0 = self.register_file.get_register(0)
        assert isinstance(reg0, runtime.DeltaRegister)

        for i in range(input_batch.length()):
            reg0.batch.append_from_accessor(
                input_batch.get_pk(i),
                input_batch.get_weight(i),
                input_batch.get_accessor(i),
            )

        program = self.program
        reg_file = self.register_file
        pc = 0

        while pc < len(program):
            jitdriver.jit_merge_point(
                pc=pc, program=program, self=self, reg_file=reg_file
            )

            instr = program[pc]
            opcode = instr.opcode

            if opcode == instructions.Instruction.HALT:
                break

            elif opcode == instructions.Instruction.FILTER:
                assert isinstance(instr, instructions.FilterOp)
                ops.op_filter(
                    instr.reg_in.batch,
                    instr.reg_out.batch,
                    instr.func,
                )

            elif opcode == instructions.Instruction.MAP:
                assert isinstance(instr, instructions.MapOp)
                ops.op_map(
                    instr.reg_in.batch,
                    instr.reg_out.batch,
                    instr.func,
                    instr.reg_out.vm_schema.table_schema,
                )

            elif opcode == instructions.Instruction.NEGATE:
                assert isinstance(instr, instructions.NegateOp)
                reg_in = instr.reg_in
                reg_out = instr.reg_out
                assert reg_in is not None and reg_out is not None
                assert reg_in.batch is not None and reg_out.batch is not None
                ops.op_negate(reg_in.batch, reg_out.batch)

            elif opcode == instructions.Instruction.UNION:
                assert isinstance(instr, instructions.UnionOp)
                b_batch = None
                if instr.reg_in_b is not None:
                    b_batch = instr.reg_in_b.batch
                ops.op_union(
                    instr.reg_in_a.batch,
                    b_batch,
                    instr.reg_out.batch,
                )

            elif opcode == instructions.Instruction.DISTINCT:
                assert isinstance(instr, instructions.DistinctOp)
                ops.op_distinct(
                    instr.reg_in.batch,
                    instr.reg_history.table,
                    instr.reg_out.batch,
                )

            elif opcode == instructions.Instruction.JOIN_DELTA_TRACE:
                assert isinstance(instr, instructions.JoinDeltaTraceOp)
                ops.op_join_delta_trace(
                    instr.reg_delta.batch,
                    instr.reg_trace.cursor,
                    instr.reg_out.batch,
                    instr.reg_delta.vm_schema.table_schema,
                    instr.reg_trace.vm_schema.table_schema,
                )

            elif opcode == instructions.Instruction.JOIN_DELTA_DELTA:
                assert isinstance(instr, instructions.JoinDeltaDeltaOp)
                reg_a = instr.reg_a
                reg_b = instr.reg_b
                reg_out = instr.reg_out
                assert reg_a is not None and reg_b is not None and reg_out is not None
                assert reg_a.batch is not None and reg_b.batch is not None and reg_out.batch is not None
                ops.op_join_delta_delta(
                    reg_a.batch, reg_b.batch, reg_out.batch,
                    reg_a.vm_schema.table_schema, reg_b.vm_schema.table_schema
                )

            elif opcode == instructions.Instruction.DELAY:
                assert isinstance(instr, instructions.DelayOp)
                reg_in = instr.reg_in
                reg_out = instr.reg_out
                assert reg_in is not None and reg_out is not None
                assert reg_in.batch is not None and reg_out.batch is not None
                ops.op_delay(reg_in.batch, reg_out.batch)

            elif opcode == instructions.Instruction.INTEGRATE:
                assert isinstance(instr, instructions.IntegrateOp)
                ops.op_integrate(
                    instr.reg_in.batch,
                    instr.target_table,
                )

            elif opcode == instructions.Instruction.REDUCE:
                assert isinstance(instr, instructions.ReduceOp)
                trace_in_cursor = None
                if instr.reg_trace_in is not None:
                    trace_in_cursor = instr.reg_trace_in.cursor
                ops.op_reduce(
                    instr.reg_in.batch,
                    instr.reg_in.vm_schema.table_schema,
                    trace_in_cursor,
                    instr.reg_trace_out.cursor,
                    instr.reg_out.batch,
                    instr.group_by_cols,
                    instr.agg_func,
                    instr.output_schema,
                )

            pc += 1
