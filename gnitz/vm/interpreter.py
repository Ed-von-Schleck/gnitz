# gnitz/vm/interpreter.py

from rpython.rlib import jit
from gnitz.vm import instructions, ops, runtime


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

    Executes a static program of Z-Set transformations. Each execution
    corresponds to one LSN epoch (tick).
    """

    _immutable_fields_ = ["register_file", "program[*]"]

    def __init__(self, register_file, program):
        self.register_file = register_file
        self.program = program

    def execute(self, input_batch):
        """
        Executes the circuit for a single batch of input updates.

        1. Clears transient delta registers.
        2. Ingests raw updates into the circuit entry point (Register 0).
        3. Iterates through the program instructions until HALT.
        """
        # Reset state for a new epoch
        self.register_file.clear_all_deltas()

        # Entry point: Register 0 must be a DeltaRegister
        reg0 = self.register_file.get_register(0)
        assert isinstance(reg0, runtime.DeltaRegister)

        # Move input data into the circuit
        for i in range(input_batch.length()):
            reg0.batch.append(
                input_batch.get_pk(i),
                input_batch.get_weight(i),
                input_batch.get_row(i),
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

            # Marshalling logic: Unpack instructions and pass components to ops.
            # This maintains a pure functional boundary for the operator layer.

            if opcode == instructions.Instruction.HALT:
                break

            elif opcode == instructions.Instruction.FILTER:
                assert isinstance(instr, instructions.FilterOp)
                ops.op_filter(instr.reg_in, instr.reg_out, instr.func)

            elif opcode == instructions.Instruction.MAP:
                assert isinstance(instr, instructions.MapOp)
                ops.op_map(instr.reg_in, instr.reg_out, instr.func)

            elif opcode == instructions.Instruction.NEGATE:
                assert isinstance(instr, instructions.NegateOp)
                ops.op_negate(instr.reg_in, instr.reg_out)

            elif opcode == instructions.Instruction.UNION:
                assert isinstance(instr, instructions.UnionOp)
                ops.op_union(instr.reg_in_a, instr.reg_in_b, instr.reg_out)

            elif opcode == instructions.Instruction.DISTINCT:
                assert isinstance(instr, instructions.DistinctOp)
                # Note: instr.history_batch is no longer used; ops.op_distinct 
                # now uses reg_in.batch for buffered history updates.
                ops.op_distinct(instr.reg_in, instr.reg_history, instr.reg_out)

            elif opcode == instructions.Instruction.JOIN_DELTA_TRACE:
                assert isinstance(instr, instructions.JoinDeltaTraceOp)
                ops.op_join_delta_trace(instr.reg_delta, instr.reg_trace, instr.reg_out)

            elif opcode == instructions.Instruction.JOIN_DELTA_DELTA:
                assert isinstance(instr, instructions.JoinDeltaDeltaOp)
                ops.op_join_delta_delta(instr.reg_a, instr.reg_b, instr.reg_out)

            elif opcode == instructions.Instruction.DELAY:
                assert isinstance(instr, instructions.DelayOp)
                ops.op_delay(instr.reg_in, instr.reg_out)

            elif opcode == instructions.Instruction.INTEGRATE:
                assert isinstance(instr, instructions.IntegrateOp)
                ops.op_integrate(instr.reg_in, instr.target_table)

            elif opcode == instructions.Instruction.REDUCE:
                assert isinstance(instr, instructions.ReduceOp)
                # Fully decomposed signature to keep ops.op_reduce uniform.
                ops.op_reduce(
                    instr.reg_in,
                    instr.reg_trace_in,
                    instr.reg_trace_out,
                    instr.reg_out,
                    instr.group_by_cols,
                    instr.agg_func,
                    instr.output_schema
                )

            pc += 1
