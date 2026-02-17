# gnitz/vm/interpreter.py

from rpython.rlib import jit
from gnitz.vm import instructions, ops
from gnitz.vm.runtime import DeltaRegister

# -----------------------------------------------------------------------------
# JIT Driver Configuration
# -----------------------------------------------------------------------------

def get_location(pc, program, engine):
    """JIT Debug Helper: Returns string representation of current op."""
    if pc < 0 or pc >= len(program):
        return "HALT"
    instr = program[pc]
    return "PC %d OP %d" % (pc, instr.opcode)

# The JitDriver is the heart of the optimization.
# greens: Variables that are constant during the loop for a specific trace.
# reds: Variables that change (Runtime state).
jitdriver = jit.JitDriver(
    greens=['pc', 'program', 'engine'],
    reds=['self'],
    get_printable_location=get_location
)

class DBSPInterpreter(object):
    """
    The Virtual Machine Execution Loop.
    Executes a reactive circuit defined by 'program' over the 'register_file'.
    """
    _immutable_fields_ = ['program[*]', 'register_file', 'engine']
    
    def __init__(self, engine, register_file, program):
        self.engine = engine
        self.register_file = register_file
        self.program = program # List of Instruction objects
        
    def execute(self, input_batch):
        """
        Main Entry Point.
        1. Loads input_batch into Register 0 (Convention).
        2. Executes the circuit.
        """
        # Load input into Register 0 (Must be a DeltaRegister)
        r0 = self.register_file.get_register(0)
        assert isinstance(r0, DeltaRegister)
        
        # Efficient Copy: input_batch is likely transient, but we copy to be safe 
        # and own the memory in the register.
        r0.batch.clear()
        count = input_batch.row_count()
        for i in range(count):
            r0.batch.append(
                input_batch.keys[i],
                input_batch.weights[i],
                input_batch.payloads[i]
            )
            
        self._execute_loop()
        
    def _execute_loop(self):
        pc = 0
        program = self.program
        engine = self.engine
        
        while pc < len(program):
            # Hint to JIT: At this point, specialize code for this PC/Program
            jitdriver.jit_merge_point(
                pc=pc, program=program, engine=engine, self=self
            )
            
            instr = program[pc]
            opcode = instr.opcode
            
            # -----------------------------------------------------------------
            # Instruction Dispatch
            # -----------------------------------------------------------------
            
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
                
            elif opcode == instructions.Instruction.JOIN_DELTA_TRACE:
                assert isinstance(instr, instructions.JoinDeltaTraceOp)
                ops.op_join_delta_trace(instr.reg_delta, instr.reg_trace, instr.reg_out)
                
            elif opcode == instructions.Instruction.JOIN_DELTA_DELTA:
                assert isinstance(instr, instructions.JoinDeltaDeltaOp)
                ops.op_join_delta_delta(instr.reg_delta_a, instr.reg_delta_b, instr.reg_out)
                
            elif opcode == instructions.Instruction.INTEGRATE:
                assert isinstance(instr, instructions.IntegrateOp)
                ops.op_integrate(instr.reg_in, instr.target_engine)

            elif opcode == instructions.Instruction.DELAY:
                assert isinstance(instr, instructions.DelayOp)
                ops.op_delay(instr.reg_in, instr.reg_out)
                
            elif opcode == instructions.Instruction.DISTINCT:
                assert isinstance(instr, instructions.DistinctOp)
                ops.op_distinct(instr.reg_in, instr.reg_out)

            elif opcode == instructions.Instruction.REDUCE:
                # Placeholder for future implementation
                pass

            pc += 1
