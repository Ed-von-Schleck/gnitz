# gnitz/vm/instructions.py

from rpython.rlib import jit

class Instruction(object):
    """Base class for all VM instructions."""
    _immutable_fields_ = ['opcode']
    
    HALT             = 0
    FILTER           = 1
    MAP              = 2
    NEGATE           = 3
    UNION            = 4
    JOIN_DELTA_TRACE = 5
    JOIN_DELTA_DELTA = 6
    INTEGRATE        = 7  # Terminal Sink
    DELAY            = 8  # z^-1 (Feedback/Recursion)
    REDUCE           = 9  # Aggregation (SUM, MIN, MAX)
    DISTINCT         = 10 # Set-semantics normalization

    def __init__(self, opcode):
        self.opcode = opcode

class FilterOp(Instruction):
    _immutable_fields_ = ['reg_in', 'reg_out', 'func']
    def __init__(self, reg_in, reg_out, func):
        Instruction.__init__(self, self.FILTER)
        self.reg_in = reg_in
        self.reg_out = reg_out
        self.func = func

class MapOp(Instruction):
    _immutable_fields_ = ['reg_in', 'reg_out', 'func']
    def __init__(self, reg_in, reg_out, func):
        Instruction.__init__(self, self.MAP)
        self.reg_in = reg_in
        self.reg_out = reg_out
        self.func = func

class NegateOp(Instruction):
    _immutable_fields_ = ['reg_in', 'reg_out']
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, self.NEGATE)
        self.reg_in = reg_in
        self.reg_out = reg_out

class UnionOp(Instruction):
    _immutable_fields_ = ['reg_in_a', 'reg_in_b', 'reg_out']
    def __init__(self, reg_in_a, reg_in_b, reg_out):
        Instruction.__init__(self, self.UNION)
        self.reg_in_a = reg_in_a
        self.reg_in_b = reg_in_b
        self.reg_out = reg_out

class JoinDeltaTraceOp(Instruction):
    _immutable_fields_ = ['reg_delta', 'reg_trace', 'reg_out']
    def __init__(self, reg_delta, reg_trace, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_TRACE)
        self.reg_delta = reg_delta    # Delta Register (Batch)
        self.reg_trace = reg_trace    # Trace Register (Cursor)
        self.reg_out = reg_out

class JoinDeltaDeltaOp(Instruction):
    _immutable_fields_ = ['reg_delta_a', 'reg_delta_b', 'reg_out']
    def __init__(self, reg_delta_a, reg_delta_b, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_DELTA)
        self.reg_delta_a = reg_delta_a
        self.reg_delta_b = reg_delta_b
        self.reg_out = reg_out

class IntegrateOp(Instruction):
    """Sinks a ZSetBatch into a specific persistent table engine."""
    _immutable_fields_ = ['reg_in', 'target_engine']
    def __init__(self, reg_in, target_engine):
        Instruction.__init__(self, self.INTEGRATE)
        self.reg_in = reg_in
        self.target_engine = target_engine

class HaltOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, self.HALT)

class DelayOp(Instruction):
    """
    Implements y = z^-1(x).
    Moves the content of reg_in (t) to reg_out (t+1).
    Essential for recursive circuits and Fixed-Point iteration.
    """
    _immutable_fields_ = ['reg_in', 'reg_out']
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, self.DELAY)
        self.reg_in = reg_in
        self.reg_out = reg_out

class ReduceOp(Instruction):
    """
    Implements non-linear aggregation.
    Iterates over a Trace (full state) or Batch to produce 
    aggregated Z-Set (e.g., SUM(salary) GROUP BY dept).
    """
    _immutable_fields_ = ['reg_trace', 'reg_out', 'agg_func']
    def __init__(self, reg_trace, reg_out, agg_func):
        Instruction.__init__(self, self.REDUCE)
        self.reg_trace = reg_trace
        self.reg_out = reg_out
        self.agg_func = agg_func

class DistinctOp(Instruction):
    """
    Incremental Set-semantics normalization.
    Compares incoming deltas (reg_in) against the historical state (reg_history)
    to compute the change in distinct set membership.
    """
    _immutable_fields_ = ['reg_in', 'reg_history', 'reg_out']
    def __init__(self, reg_in, reg_history, reg_out):
        Instruction.__init__(self, self.DISTINCT)
        self.reg_in = reg_in           # DeltaRegister (Incoming Batch)
        self.reg_history = reg_history # TraceRegister (Historical State)
        self.reg_out = reg_out         # DeltaRegister (Incremental result)
