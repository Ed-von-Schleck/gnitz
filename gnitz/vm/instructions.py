# gnitz/vm/instructions.py

import gnitz.core.opcodes as op


class Instruction(object):
    """
    Base class for all VM instructions.

    Standardized layout ensures the RPython annotator can resolve attributes
    during dispatch loops without type unions or attribute errors.
    Every field used by any subclass is defined here.
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
        "jump_target",
        "yield_reason",
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
        self.jump_target = 0
        self.yield_reason = 0
        self.reg_key = None


# ── DBSP Algebraic Instructions ───────────────────────────────────────────


class FilterOp(Instruction):
    def __init__(self, reg_in, reg_out, func):
        Instruction.__init__(self, op.OPCODE_FILTER)
        self.reg_in = reg_in
        self.reg_out = reg_out
        self.func = func


class MapOp(Instruction):
    def __init__(self, reg_in, reg_out, func):
        Instruction.__init__(self, op.OPCODE_MAP)
        self.reg_in = reg_in
        self.reg_out = reg_out
        self.func = func


class NegateOp(Instruction):
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, op.OPCODE_NEGATE)
        self.reg_in = reg_in
        self.reg_out = reg_out


class UnionOp(Instruction):
    def __init__(self, reg_in_a, reg_in_b, reg_out):
        Instruction.__init__(self, op.OPCODE_UNION)
        self.reg_in_a = reg_in_a
        self.reg_in_b = reg_in_b
        self.reg_out = reg_out


class DistinctOp(Instruction):
    def __init__(self, reg_in, reg_history, reg_out):
        Instruction.__init__(self, op.OPCODE_DISTINCT)
        self.reg_in = reg_in
        self.reg_history = reg_history
        self.reg_out = reg_out


class JoinDeltaTraceOp(Instruction):
    def __init__(self, reg_delta, reg_trace, reg_out):
        Instruction.__init__(self, op.OPCODE_JOIN_DELTA_TRACE)
        self.reg_delta = reg_delta
        self.reg_trace = reg_trace
        self.reg_out = reg_out


class IntegrateOp(Instruction):
    def __init__(self, reg_in, target_table):
        Instruction.__init__(self, op.OPCODE_INTEGRATE)
        self.reg_in = reg_in
        self.target_table = target_table


class DelayOp(Instruction):
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, op.OPCODE_DELAY)
        self.reg_in = reg_in
        self.reg_out = reg_out


class JoinDeltaDeltaOp(Instruction):
    def __init__(self, reg_a, reg_b, reg_out):
        Instruction.__init__(self, op.OPCODE_JOIN_DELTA_DELTA)
        self.reg_a = reg_a
        self.reg_b = reg_b
        self.reg_out = reg_out


class ReduceOp(Instruction):
    def __init__(
        self, reg_in, reg_trace_in, reg_trace_out, reg_out,
        group_by_cols, agg_func, output_schema,
    ):
        Instruction.__init__(self, op.OPCODE_REDUCE)
        self.reg_in = reg_in
        self.reg_trace_in = reg_trace_in
        self.reg_trace_out = reg_trace_out
        self.reg_out = reg_out
        self.group_by_cols = group_by_cols
        self.agg_func = agg_func
        self.output_schema = output_schema


# ── Control Flow & Cursor Management ──────────────────────────────────────


class HaltOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, op.OPCODE_HALT)


class ScanTraceOp(Instruction):
    def __init__(self, reg_trace, reg_out, chunk_limit):
        Instruction.__init__(self, op.OPCODE_SCAN_TRACE)
        self.reg_trace = reg_trace
        self.reg_out = reg_out
        self.chunk_limit = chunk_limit


class SeekTraceOp(Instruction):
    def __init__(self, reg_trace, reg_key):
        Instruction.__init__(self, op.OPCODE_SEEK_TRACE)
        self.reg_trace = reg_trace
        self.reg_key = reg_key


class YieldOp(Instruction):
    def __init__(self, reason):
        Instruction.__init__(self, op.OPCODE_YIELD)
        self.yield_reason = reason


class JumpOp(Instruction):
    def __init__(self, target_idx):
        Instruction.__init__(self, op.OPCODE_JUMP)
        self.jump_target = target_idx


class ClearDeltasOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, op.OPCODE_CLEAR_DELTAS)
