# gnitz/vm/instructions.py

import gnitz.core.opcodes as op


class Instruction(object):
    """
    Base class for all VM instructions.

    Standardized layout ensures the RPython annotator can resolve attributes
    during dispatch loops without type unions or attribute errors.
    Every field used by any instruction is defined here.
    """

    _immutable_fields_ = [
        "opcode",
        "reg_in",
        "reg_out",
        "func",
        "reg_in_a",
        "reg_in_b",
        "reg_history",
        "reg_delta",
        "reg_trace",
        "target_table",
        "reg_a",
        "reg_b",
        "reg_trace_in",
        "reg_trace_out",
        "group_by_cols",
        "agg_func",
        "output_schema",
        "chunk_limit",
        "reg_key",
    ]

    def __init__(self, opcode):
        self.opcode = opcode
        self.reg_in = None
        self.reg_out = None
        self.func = None
        self.reg_in_a = None
        self.reg_in_b = None
        self.reg_history = None
        self.reg_delta = None
        self.reg_trace = None
        self.target_table = None
        self.reg_a = None
        self.reg_b = None
        self.reg_trace_in = None
        self.reg_trace_out = None
        self.group_by_cols = None
        self.agg_func = None
        self.output_schema = None
        self.chunk_limit = 0
        self.reg_key = None


# ── DBSP Algebraic Instructions ───────────────────────────────────────────


def filter_op(reg_in, reg_out, func):
    i = Instruction(op.OPCODE_FILTER)
    i.reg_in = reg_in
    i.reg_out = reg_out
    i.func = func
    return i


def map_op(reg_in, reg_out, func):
    i = Instruction(op.OPCODE_MAP)
    i.reg_in = reg_in
    i.reg_out = reg_out
    i.func = func
    return i


def negate_op(reg_in, reg_out):
    i = Instruction(op.OPCODE_NEGATE)
    i.reg_in = reg_in
    i.reg_out = reg_out
    return i


def union_op(reg_in_a, reg_in_b, reg_out):
    i = Instruction(op.OPCODE_UNION)
    i.reg_in_a = reg_in_a
    i.reg_in_b = reg_in_b
    i.reg_out = reg_out
    return i


def distinct_op(reg_in, reg_history, reg_out):
    i = Instruction(op.OPCODE_DISTINCT)
    i.reg_in = reg_in
    i.reg_history = reg_history
    i.reg_out = reg_out
    return i


def join_delta_trace_op(reg_delta, reg_trace, reg_out):
    i = Instruction(op.OPCODE_JOIN_DELTA_TRACE)
    i.reg_delta = reg_delta
    i.reg_trace = reg_trace
    i.reg_out = reg_out
    return i


def integrate_op(reg_in, target_table):
    i = Instruction(op.OPCODE_INTEGRATE)
    i.reg_in = reg_in
    i.target_table = target_table
    return i


def delay_op(reg_in, reg_out):
    i = Instruction(op.OPCODE_DELAY)
    i.reg_in = reg_in
    i.reg_out = reg_out
    return i


def join_delta_delta_op(reg_a, reg_b, reg_out):
    i = Instruction(op.OPCODE_JOIN_DELTA_DELTA)
    i.reg_a = reg_a
    i.reg_b = reg_b
    i.reg_out = reg_out
    return i


def reduce_op(
    reg_in, reg_trace_in, reg_trace_out, reg_out,
    group_by_cols, agg_func, output_schema,
):
    i = Instruction(op.OPCODE_REDUCE)
    i.reg_in = reg_in
    i.reg_trace_in = reg_trace_in
    i.reg_trace_out = reg_trace_out
    i.reg_out = reg_out
    i.group_by_cols = group_by_cols
    i.agg_func = agg_func
    i.output_schema = output_schema
    return i


# ── Control Flow & Cursor Management ──────────────────────────────────────


def halt_op():
    return Instruction(op.OPCODE_HALT)


def scan_trace_op(reg_trace, reg_out, chunk_limit):
    i = Instruction(op.OPCODE_SCAN_TRACE)
    i.reg_trace = reg_trace
    i.reg_out = reg_out
    i.chunk_limit = chunk_limit
    return i


def seek_trace_op(reg_trace, reg_key):
    i = Instruction(op.OPCODE_SEEK_TRACE)
    i.reg_trace = reg_trace
    i.reg_key = reg_key
    return i


def anti_join_delta_trace_op(reg_delta, reg_trace, reg_out):
    i = Instruction(op.OPCODE_ANTI_JOIN_DELTA_TRACE)
    i.reg_delta = reg_delta
    i.reg_trace = reg_trace
    i.reg_out = reg_out
    return i


def anti_join_delta_delta_op(reg_a, reg_b, reg_out):
    i = Instruction(op.OPCODE_ANTI_JOIN_DELTA_DELTA)
    i.reg_a = reg_a
    i.reg_b = reg_b
    i.reg_out = reg_out
    return i


def semi_join_delta_trace_op(reg_delta, reg_trace, reg_out):
    i = Instruction(op.OPCODE_SEMI_JOIN_DELTA_TRACE)
    i.reg_delta = reg_delta
    i.reg_trace = reg_trace
    i.reg_out = reg_out
    return i


def semi_join_delta_delta_op(reg_a, reg_b, reg_out):
    i = Instruction(op.OPCODE_SEMI_JOIN_DELTA_DELTA)
    i.reg_a = reg_a
    i.reg_b = reg_b
    i.reg_out = reg_out
    return i


def clear_deltas_op():
    return Instruction(op.OPCODE_CLEAR_DELTAS)
