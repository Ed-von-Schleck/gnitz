# gnitz/vm/instructions.py

from rpython.rlib import jit

class Instruction(object):
    """
    Base class for VM Bytecode.
    
    Instructions are unpacked by the interpreter and passed into the 
    gnitz.vm.ops layer as individual arguments.
    """
    _immutable_fields_ = ['opcode']
    
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

    def __init__(self, opcode):
        self.opcode = opcode

class HaltOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, self.HALT)

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

class DistinctOp(Instruction):
    """
    Identity: delta_out = set_step(history + delta_in) - set_step(history).
    The history table is updated with the consolidated input batch.
    """
    _immutable_fields_ = ['reg_in', 'reg_history', 'reg_out']
    def __init__(self, reg_in, reg_history, reg_out):
        Instruction.__init__(self, self.DISTINCT)
        self.reg_in = reg_in
        self.reg_history = reg_history
        self.reg_out = reg_out

class JoinDeltaTraceOp(Instruction):
    _immutable_fields_ = ['reg_delta', 'reg_trace', 'reg_out']
    def __init__(self, reg_delta, reg_trace, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_TRACE)
        self.reg_delta = reg_delta
        self.reg_trace = reg_trace
        self.reg_out = reg_out

class JoinDeltaDeltaOp(Instruction):
    _immutable_fields_ = ['reg_a', 'reg_b', 'reg_out']
    def __init__(self, reg_a, reg_b, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_DELTA)
        self.reg_a = reg_a
        self.reg_b = reg_b
        self.reg_out = reg_out

class ReduceOp(Instruction):
    """
    DBSP Aggregation. Requires both input/output delta registers and 
    traces for historical state and retractions.
    """
    _immutable_fields_ = [
        'reg_in', 'reg_trace_in', 'reg_trace_out', 
        'reg_out', 'group_by_cols[*]', 'agg_func', 'output_schema'
    ]
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

class IntegrateOp(Instruction):
    """Terminal sink that persists a delta register to a Table."""
    _immutable_fields_ = ['reg_in', 'target_table']
    def __init__(self, reg_in, target_table):
        Instruction.__init__(self, self.INTEGRATE)
        self.reg_in = reg_in
        self.target_table = target_table

class DelayOp(Instruction):
    """The z^-1 operator for recursive CTEs and temporal state."""
    _immutable_fields_ = ['reg_in', 'reg_out']
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, self.DELAY)
        self.reg_in = reg_in
        self.reg_out = reg_out
