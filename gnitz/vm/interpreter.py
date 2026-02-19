from rpython.rlib import jit
from gnitz.vm import instructions, ops, runtime

def get_printable_location(pc, program, self):
    instr = program[pc]
    # In RPython, the JIT calls this to label traces in the jitlog.
    # It now correctly receives the 'self' (DBSPInterpreter) instance as well.
    return "PC: %d, Op: %d" % (pc, instr.opcode)

jitdriver = jit.JitDriver(
    greens=['pc', 'program', 'self'],
    reds=['reg_file'],
    get_printable_location=get_printable_location
)

class DBSPInterpreter(object):
    _immutable_fields_ = ['engine', 'register_file', 'program[*]']

    def __init__(self, engine, register_file, program):
        self.engine = engine
        self.register_file = register_file
        self.program = program

    def execute(self, input_batch):
        self.register_file.clear_all_deltas()

        reg0 = self.register_file.get_register(0)
        assert isinstance(reg0, runtime.DeltaRegister)
        
        for i in range(input_batch.row_count()):
            reg0.batch.append(
                input_batch.get_key(i),
                input_batch.get_weight(i),
                input_batch.get_payload(i)
            )

        program = self.program
        reg_file = self.register_file
        pc = 0
        
        while pc < len(program):
            jitdriver.jit_merge_point(pc=pc, program=program, self=self, reg_file=reg_file)
            instr = program[pc]
            opcode = instr.opcode

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
                ops.op_integrate(instr.reg_in, instr.target_engine)
            
            pc += 1
