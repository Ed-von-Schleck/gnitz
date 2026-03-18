# gnitz/vm/interpreter.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.core import batch, errors
from gnitz.core import opcodes as op
from gnitz.dbsp import ops
from gnitz.dbsp.ops.group_index import (
    GroupIdxAccessor as _GroupIdxAccessor,
    promote_group_col_to_u64 as _gi_promote,
)
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
    Returns when the program HALTs.
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
                batch.BatchWriter(reg_out.batch),
                instr.func,
                reg_out.table_schema,
                reindex_col=instr.reindex_col
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
                reg_history.cursor,
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
                reg_delta.table_schema,
                reg_trace.table_schema
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
                reg_a.table_schema, 
                reg_b.table_schema
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
            if instr.group_idx is not None:
                gi = instr.group_idx
                b = reg_in.batch
                n = b.length()
                if n > 0:
                    gi_acc = _GroupIdxAccessor()
                    acc = b.get_accessor(0)
                    for idx in range(n):
                        b.bind_accessor(idx, acc)
                        if acc.is_null(gi.col_idx):
                            continue
                        gc_u64 = _gi_promote(
                            acc, gi.col_idx, gi.col_type,
                        )
                        source_pk = b.get_pk(idx)
                        ck = ((r_uint128(gc_u64) << 64)
                              | r_uint128(r_uint64(intmask(source_pk))))
                        gi_acc.spk_hi = r_int64(intmask(source_pk >> 64))
                        weight = b.get_weight(idx)
                        try:
                            gi.table.memtable.upsert_single(ck, weight, gi_acc)
                        except errors.MemTableFullError:
                            gi.table.flush()
                            gi.table.memtable.upsert_single(ck, weight, gi_acc)

        elif opcode == op.OPCODE_ANTI_JOIN_DELTA_TRACE:
            reg_delta = instr.reg_delta
            reg_trace = instr.reg_trace
            reg_out = instr.reg_out
            assert reg_delta is not None and reg_trace is not None and reg_out is not None
            ops.op_anti_join_delta_trace(
                reg_delta.batch,
                reg_trace.cursor,
                batch.BatchWriter(reg_out.batch),
                reg_delta.table_schema
            )

        elif opcode == op.OPCODE_ANTI_JOIN_DELTA_DELTA:
            reg_a = instr.reg_a
            reg_b = instr.reg_b
            reg_out = instr.reg_out
            assert reg_a is not None and reg_b is not None and reg_out is not None
            ops.op_anti_join_delta_delta(
                reg_a.batch,
                reg_b.batch,
                batch.BatchWriter(reg_out.batch),
                reg_a.table_schema
            )

        elif opcode == op.OPCODE_SEMI_JOIN_DELTA_TRACE:
            reg_delta = instr.reg_delta
            reg_trace = instr.reg_trace
            reg_out = instr.reg_out
            assert reg_delta is not None and reg_trace is not None and reg_out is not None
            ops.op_semi_join_delta_trace(
                reg_delta.batch,
                reg_trace.cursor,
                batch.BatchWriter(reg_out.batch),
                reg_delta.table_schema
            )

        elif opcode == op.OPCODE_SEMI_JOIN_DELTA_DELTA:
            reg_a = instr.reg_a
            reg_b = instr.reg_b
            reg_out = instr.reg_out
            assert reg_a is not None and reg_b is not None and reg_out is not None
            ops.op_semi_join_delta_delta(
                reg_a.batch,
                reg_b.batch,
                batch.BatchWriter(reg_out.batch),
                reg_a.table_schema
            )

        elif opcode == op.OPCODE_REDUCE:
            reg_in = instr.reg_in
            reg_trace_out = instr.reg_trace_out
            reg_out = instr.reg_out
            assert reg_in is not None and reg_trace_out is not None and reg_out is not None
            trace_in_cursor = instr.reg_trace_in.cursor if instr.reg_trace_in else None
            ops.op_reduce(
                reg_in.batch,
                reg_in.table_schema,
                trace_in_cursor,
                reg_trace_out.cursor,
                batch.BatchWriter(reg_out.batch),
                instr.group_by_cols,
                instr.agg_funcs,
                instr.output_schema,
                instr.trace_in_group_idx,
            )

        pc += 1

    # Program reached the end without a HALT
    context.pc = pc
    context.status = runtime.STATUS_HALTED
