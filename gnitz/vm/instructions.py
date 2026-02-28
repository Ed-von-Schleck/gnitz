# gnitz/vm/instructions.py

class Instruction(object):
    """
    Base class for all VM instructions.
    Standardized layout ensures the RPython annotator can resolve attributes
    during dispatch loops without type unions or attribute errors.
    """
    _immutable_fields_ = [
        'opcode', 'reg_in', 'reg_out', 'func', 'reg_in_a', 'reg_in_b',
        'reg_history', 'reg_delta', 'reg_trace', 'target_table',
        'reg_a', 'reg_b', 'reg_trace_in', 'reg_trace_out',
        'group_by_cols', 'agg_func', 'output_schema',
        # --- New Fields for Phase A ---
        'chunk_limit', 'jump_target', 'yield_reason', 'reg_key'
    ]
    
    HALT             = 0
    FILTER           = 1
    MAP              = 2
    NEGATE           = 3
    UNION            = 4
    JOIN_DELTA_TRACE = 5
    JOIN_DELTA_DELTA = 6
    INTEGRATE        = 7
    DELAY            = 8
    REDUCE           = 9
    DISTINCT         = 10
    # --- New Opcodes ---
    SCAN_TRACE       = 11
    SEEK_TRACE       = 12
    YIELD            = 13
    JUMP             = 14
    CLEAR_DELTAS     = 15

    def __init__(self, opcode):
        self.opcode = opcode
        # Standardized attribute file initialized to None/0
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
        # --- New Fields ---
        self.chunk_limit = 0
        self.jump_target = 0
        self.yield_reason = 0
        self.reg_key = None

class FilterOp(Instruction):
    def __init__(self, reg_in, reg_out, func):
        Instruction.__init__(self, self.FILTER)
        self.reg_in = reg_in
        self.reg_out = reg_out
        self.func = func

class MapOp(Instruction):
    def __init__(self, reg_in, reg_out, func):
        Instruction.__init__(self, self.MAP)
        self.reg_in = reg_in
        self.reg_out = reg_out
        self.func = func

class NegateOp(Instruction):
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, self.NEGATE)
        self.reg_in = reg_in
        self.reg_out = reg_out

class UnionOp(Instruction):
    def __init__(self, reg_in_a, reg_in_b, reg_out):
        Instruction.__init__(self, self.UNION)
        self.reg_in_a = reg_in_a
        self.reg_in_b = reg_in_b
        self.reg_out = reg_out

class DistinctOp(Instruction):
    def __init__(self, reg_in, reg_history, reg_out):
        Instruction.__init__(self, self.DISTINCT)
        self.reg_in = reg_in
        self.reg_history = reg_history
        self.reg_out = reg_out

class JoinDeltaTraceOp(Instruction):
    def __init__(self, reg_delta, reg_trace, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_TRACE)
        self.reg_delta = reg_delta
        self.reg_trace = reg_trace
        self.reg_out = reg_out

class IntegrateOp(Instruction):
    def __init__(self, reg_in, target_table):
        Instruction.__init__(self, self.INTEGRATE)
        self.reg_in = reg_in
        self.target_table = target_table

class DelayOp(Instruction):
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, self.DELAY)
        self.reg_in = reg_in
        self.reg_out = reg_out

class HaltOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, self.HALT)

class JoinDeltaDeltaOp(Instruction):
    def __init__(self, reg_a, reg_b, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_DELTA)
        self.reg_a = reg_a
        self.reg_b = reg_b
        self.reg_out = reg_out
        
class ReduceOp(Instruction):
    def __init__(self, reg_in, reg_trace_in, reg_trace_out, reg_out, 
                 group_by_cols, agg_func, output_schema):
        Instruction.__init__(self, self.REDUCE)
        self.reg_in = reg_in
        self.reg_trace_in = reg_trace_in
        self.reg_trace_out = reg_trace_out
        self.reg_out = reg_out
        self.group_by_cols = group_by_cols
        self.agg_func = agg_func
        self.output_schema = output_schema

# --- New Op Classes for Phase A ---

class ScanTraceOp(Instruction):
    def __init__(self, reg_trace, reg_out, chunk_limit):
        Instruction.__init__(self, self.SCAN_TRACE)
        self.reg_trace = reg_trace
        self.reg_out = reg_out
        self.chunk_limit = chunk_limit

class SeekTraceOp(Instruction):
    def __init__(self, reg_trace, reg_key):
        Instruction.__init__(self, self.SEEK_TRACE)
        self.reg_trace = reg_trace
        self.reg_key = reg_key

class YieldOp(Instruction):
    def __init__(self, reason):
        Instruction.__init__(self, self.YIELD)
        self.yield_reason = reason

class JumpOp(Instruction):
    def __init__(self, target_idx):
        Instruction.__init__(self, self.JUMP)
        self.jump_target = target_idx

class ClearDeltasOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, self.CLEAR_DELTAS)
