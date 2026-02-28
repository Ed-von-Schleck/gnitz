# gnitz/vm/interpreter.py

from rpython.rlib import jit
from gnitz.dbsp import ops
from gnitz.vm import instructions, runtime

def get_printable_location(pc, program, self):
    """Provides a trace label for the RPython JIT log."""
    if pc < len(program):
        instr = program[pc]
        return "PC: %d, Opcode: %d" % (pc, instr.opcode)
    return "PC: %d, OUT_OF_BOUNDS" % pc

jitdriver = jit.JitDriver(
    greens=["pc", "program", "self"],
    reds=["context", "reg_file"],
    get_printable_location=get_printable_location,
)

class DBSPInterpreter(object):
    """
    Stateful DBSP Virtual Machine.
    Executes incremental circuits with support for chunking, yielding, 
    and non-linear control flow.
    """
    _immutable_fields_ = ["program[*]"]

    def __init__(self, program):
        self.program = program

    def resume(self, context):
        """
        Resumes execution of the program from the state stored in context.
        """
        program = self.program
        reg_file = context.reg_file
        pc = context.pc
        context.status = runtime.STATUS_RUNNING

        while pc < len(program):
            jitdriver.jit_merge_point(
                pc=pc, program=program, self=self, 
                context=context, reg_file=reg_file
            )

            instr = program[pc]
            opcode = instr.opcode

            if opcode == instructions.Instruction.HALT:
                context.pc = pc
                context.status = runtime.STATUS_HALTED
                return

            elif opcode == instructions.Instruction.YIELD:
                assert isinstance(instr, instructions.YieldOp)
                context.pc = pc + 1
                context.status = runtime.STATUS_YIELDED
                context.yield_reason = instr.yield_reason
                return

            elif opcode == instructions.Instruction.JUMP:
                assert isinstance(instr, instructions.JumpOp)
                pc = instr.jump_target
                continue

            elif opcode == instructions.Instruction.SCAN_TRACE:
                assert isinstance(instr, instructions.ScanTraceOp)
                reg_t = instr.reg_trace
                reg_o = instr.reg_out
                if reg_t is not None and reg_o is not None:
                    ops.op_scan_trace(reg_t.cursor, reg_o.batch, instr.chunk_limit)

            elif opcode == instructions.Instruction.SEEK_TRACE:
                assert isinstance(instr, instructions.SeekTraceOp)
                reg_t = instr.reg_trace
                reg_k = instr.reg_key
                if reg_t is not None and reg_k is not None:
                    if reg_k.batch.length() > 0:
                        key = reg_k.batch.get_pk(0)
                        ops.op_seek_trace(reg_t.cursor, key)

            elif opcode == instructions.Instruction.CLEAR_DELTAS:
                ops.op_clear_deltas(reg_file)

            elif opcode == instructions.Instruction.FILTER:
                assert isinstance(instr, instructions.FilterOp)
                reg_in = instr.reg_in
                reg_out = instr.reg_out
                if reg_in is not None and reg_out is not None:
                    ops.op_filter(reg_in.batch, reg_out.batch, instr.func)

            elif opcode == instructions.Instruction.MAP:
                assert isinstance(instr, instructions.MapOp)
                reg_in = instr.reg_in
                reg_out = instr.reg_out
                if reg_in is not None and reg_out is not None:
                    ops.op_map(
                        reg_in.batch, 
                        reg_out.batch, 
                        instr.func, 
                        reg_out.vm_schema.table_schema
                    )

            elif opcode == instructions.Instruction.NEGATE:
                assert isinstance(instr, instructions.NegateOp)
                reg_in = instr.reg_in
                reg_out = instr.reg_out
                if reg_in is not None and reg_out is not None:
                    ops.op_negate(reg_in.batch, reg_out.batch)

            elif opcode == instructions.Instruction.UNION:
                assert isinstance(instr, instructions.UnionOp)
                reg_in_a = instr.reg_in_a
                reg_in_b = instr.reg_in_b
                reg_out = instr.reg_out
                if reg_in_a is not None and reg_out is not None:
                    b_batch = None
                    if reg_in_b is not None:
                        b_batch = reg_in_b.batch
                    ops.op_union(reg_in_a.batch, b_batch, reg_out.batch)

            elif opcode == instructions.Instruction.DISTINCT:
                assert isinstance(instr, instructions.DistinctOp)
                reg_in = instr.reg_in
                reg_history = instr.reg_history
                reg_out = instr.reg_out
                if reg_in is not None and reg_history is not None and reg_out is not None:
                    ops.op_distinct(
                        reg_in.batch, 
                        reg_history.table, 
                        reg_out.batch
                    )

            elif opcode == instructions.Instruction.JOIN_DELTA_TRACE:
                assert isinstance(instr, instructions.JoinDeltaTraceOp)
                reg_delta = instr.reg_delta
                reg_trace = instr.reg_trace
                reg_out = instr.reg_out
                if reg_delta is not None and reg_trace is not None and reg_out is not None:
                    ops.op_join_delta_trace(
                        reg_delta.batch, 
                        reg_trace.cursor, 
                        reg_out.batch, 
                        reg_delta.vm_schema.table_schema, 
                        reg_trace.vm_schema.table_schema
                    )

            elif opcode == instructions.Instruction.JOIN_DELTA_DELTA:
                assert isinstance(instr, instructions.JoinDeltaDeltaOp)
                reg_a = instr.reg_a
                reg_b = instr.reg_b
                reg_out = instr.reg_out
                if reg_a is not None and reg_b is not None and reg_out is not None:
                    ops.op_join_delta_delta(
                        reg_a.batch, reg_b.batch, reg_out.batch,
                        reg_a.vm_schema.table_schema, reg_b.vm_schema.table_schema
                    )

            elif opcode == instructions.Instruction.DELAY:
                assert isinstance(instr, instructions.DelayOp)
                reg_in = instr.reg_in
                reg_out = instr.reg_out
                if reg_in is not None and reg_out is not None:
                    ops.op_delay(reg_in.batch, reg_out.batch)

            elif opcode == instructions.Instruction.INTEGRATE:
                assert isinstance(instr, instructions.IntegrateOp)
                reg_in = instr.reg_in
                if reg_in is not None:
                    ops.op_integrate(reg_in.batch, instr.target_table)

            elif opcode == instructions.Instruction.REDUCE:
                assert isinstance(instr, instructions.ReduceOp)
                reg_in = instr.reg_in
                reg_trace_out = instr.reg_trace_out
                reg_out = instr.reg_out
                if reg_in is not None and reg_trace_out is not None and reg_out is not None:
                    trace_in_cursor = None
                    if instr.reg_trace_in is not None:
                        trace_in_cursor = instr.reg_trace_in.cursor
                    ops.op_reduce(
                        reg_in.batch,
                        reg_in.vm_schema.table_schema,
                        trace_in_cursor,
                        reg_trace_out.cursor,
                        reg_out.batch,
                        instr.group_by_cols,
                        instr.agg_func,
                        instr.output_schema,
                    )

            pc += 1
        
        context.pc = pc
        context.status = runtime.STATUS_HALTED
