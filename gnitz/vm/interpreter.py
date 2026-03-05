# gnitz/vm/interpreter.py

from rpython.rlib import jit
from gnitz.core import batch
from gnitz.core import opcodes as op
from gnitz.dbsp import ops
from gnitz.vm import runtime

def get_printable_location(pc, program):
    """Provides a trace label for the RPython JIT log."""
    if pc < len(program):
        instr = program[pc]
        return "PC: %d, Opcode: %d" % (pc, instr.opcode)
    return "PC: %d, OUT_OF_BOUNDS" % pc

jitdriver = jit.JitDriver(
    greens=["pc", "program"],
    reds=["context", "reg_file"],
    get_printable_location=get_printable_location,
)

def run_vm(program, reg_file, context):
    """
    Main execution loop. Executes the program from the current context PC.
    Returns when the program HALTs or YIELDs.
    """
    pc = context.pc
    context.status = runtime.STATUS_RUNNING

    while pc < len(program):
        # 'pc' and 'program' are green, allowing the JIT to specialize
        # machine code for this specific circuit path.
        jitdriver.jit_merge_point(
            pc=pc, program=program,
            context=context, reg_file=reg_file
        )

        instr = program[pc]
        opcode = instr.opcode

        # ── Control Flow Opcodes ────────────────────────────────────────

        if opcode == op.OPCODE_HALT:
            context.pc = pc
            context.status = runtime.STATUS_HALTED
            return

        elif opcode == op.OPCODE_YIELD:
            context.pc = pc + 1
            context.status = runtime.STATUS_YIELDED
            context.yield_reason = instr.yield_reason
            return

        elif opcode == op.OPCODE_JUMP:
            pc = instr.jump_target
            continue

        elif opcode == op.OPCODE_CLEAR_DELTAS:
            ops.op_clear_deltas(reg_file)

        # ── Stateful Cursor Opcodes ─────────────────────────────────────

        elif opcode == op.OPCODE_SCAN_TRACE:
            reg_t = instr.reg_trace
            reg_o = instr.reg_out
            assert reg_t is not None and reg_o is not None
            # Note: For chunked scanning, the VM logic assumes the register 
            # is cleared before the first scan of a sequence.
            writer = batch.BatchWriter(reg_o.batch)
            ops.op_scan_trace(reg_t.cursor, writer, instr.chunk_limit)

        elif opcode == op.OPCODE_SEEK_TRACE:
            reg_t = instr.reg_trace
            reg_k = instr.reg_key
            assert reg_t is not None and reg_k is not None
            if reg_k.batch.length() > 0:
                key = reg_k.batch.get_pk(0)
                ops.op_seek_trace(reg_t.cursor, key)

        # ── DBSP Algebraic Opcodes (Stateless) ──────────────────────────

        elif opcode == op.OPCODE_FILTER:
            reg_in = instr.reg_in
            reg_out = instr.reg_out
            assert reg_in is not None and reg_out is not None
            ops.op_filter(
                reg_in.batch, 
                batch.BatchWriter(reg_out.batch), 
                instr.func
            )

        elif opcode == op.OPCODE_MAP:
            reg_in = instr.reg_in
            reg_out = instr.reg_out
            assert reg_in is not None and reg_out is not None
            ops.op_map(
                reg_in.batch,
                reg_out.batch,
                instr.func,
                reg_out.vm_schema.table_schema
            )

        elif opcode == op.OPCODE_NEGATE:
            reg_in = instr.reg_in
            reg_out = instr.reg_out
            assert reg_in is not None and reg_out is not None
            ops.op_negate(
                reg_in.batch, 
                batch.BatchWriter(reg_out.batch)
            )

        elif opcode == op.OPCODE_UNION:
            reg_in_a = instr.reg_in_a
            reg_in_b = instr.reg_in_b
            reg_out = instr.reg_out
            assert reg_in_a is not None and reg_out is not None
            b_batch = reg_in_b.batch if reg_in_b is not None else None
            ops.op_union(
                reg_in_a.batch, 
                b_batch, 
                batch.BatchWriter(reg_out.batch)
            )

        # ── Non-Linear & Bilinear Opcodes (Stateful Logic) ──────────────

        elif opcode == op.OPCODE_DISTINCT:
            reg_in = instr.reg_in
            reg_history = instr.reg_history
            reg_out = instr.reg_out
            assert reg_in is not None and reg_history is not None and reg_out is not None
            ops.op_distinct(
                reg_in.batch,
                reg_history.table,
                batch.BatchWriter(reg_out.batch)
            )

        elif opcode == op.OPCODE_JOIN_DELTA_TRACE:
            reg_delta = instr.reg_delta
            reg_trace = instr.reg_trace
            reg_out = instr.reg_out
            assert reg_delta is not None and reg_trace is not None and reg_out is not None
            ops.op_join_delta_trace(
                reg_delta.batch,
                reg_trace.cursor,
                batch.BatchWriter(reg_out.batch),
                reg_delta.vm_schema.table_schema,
                reg_trace.vm_schema.table_schema
            )

        elif opcode == op.OPCODE_JOIN_DELTA_DELTA:
            reg_a = instr.reg_a
            reg_b = instr.reg_b
            reg_out = instr.reg_out
            assert reg_a is not None and reg_b is not None and reg_out is not None
            ops.op_join_delta_delta(
                reg_a.batch, 
                reg_b.batch, 
                batch.BatchWriter(reg_out.batch),
                reg_a.vm_schema.table_schema, 
                reg_b.vm_schema.table_schema
            )

        elif opcode == op.OPCODE_DELAY:
            reg_in = instr.reg_in
            reg_out = instr.reg_out
            assert reg_in is not None and reg_out is not None
            ops.op_delay(
                reg_in.batch, 
                batch.BatchWriter(reg_out.batch)
            )

        elif opcode == op.OPCODE_INTEGRATE:
            reg_in = instr.reg_in
            assert reg_in is not None
            ops.op_integrate(reg_in.batch, instr.target_table)

        elif opcode == op.OPCODE_REDUCE:
            reg_in = instr.reg_in
            reg_trace_out = instr.reg_trace_out
            reg_out = instr.reg_out
            assert reg_in is not None and reg_trace_out is not None and reg_out is not None
            trace_in_cursor = instr.reg_trace_in.cursor if instr.reg_trace_in else None
            ops.op_reduce(
                reg_in.batch,
                reg_in.vm_schema.table_schema,
                trace_in_cursor,
                reg_trace_out.cursor,
                batch.BatchWriter(reg_out.batch),
                instr.group_by_cols,
                instr.agg_func,
                instr.output_schema,
            )

        pc += 1

    # Program reached the end without a HALT
    context.pc = pc
    context.status = runtime.STATUS_HALTED
