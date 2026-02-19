from rpython.rlib import jit

class Instruction(object):
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

class IntegrateOp(Instruction):
    _immutable_fields_ = ['reg_in', 'target_engine']
    def __init__(self, reg_in, target_engine):
        Instruction.__init__(self, self.INTEGRATE)
        self.reg_in = reg_in
        self.target_engine = target_engine

class DelayOp(Instruction):
    _immutable_fields_ = ['reg_in', 'reg_out']
    def __init__(self, reg_in, reg_out):
        Instruction.__init__(self, self.DELAY)
        self.reg_in = reg_in
        self.reg_out = reg_out

class HaltOp(Instruction):
    def __init__(self):
        Instruction.__init__(self, self.HALT)

class JoinDeltaDeltaOp(Instruction):
    _immutable_fields_ = ['reg_a', 'reg_b', 'reg_out']
    def __init__(self, reg_a, reg_b, reg_out):
        Instruction.__init__(self, self.JOIN_DELTA_DELTA)
        self.reg_a = reg_a
        self.reg_b = reg_b
        self.reg_out = reg_out
