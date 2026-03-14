# gnitz/dbsp/expr.py
#
# Expression bytecode VM for SQL scalar functions.
# Runs @jit.unroll_safe inside operator row loops — the JIT inlines the
# entire expression evaluation per row, producing native code equivalent
# to hand-written comparisons.

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rlib.objectmodel import newlist_hint

from gnitz.dbsp.functions import ScalarFunction

# ---------------------------------------------------------------------------
# Expression Opcodes (separate namespace from DBSP VM opcodes)
# ---------------------------------------------------------------------------

# Column loads
EXPR_LOAD_COL_INT   = 1   # dst = accessor.get_int_signed(arg1)
EXPR_LOAD_COL_FLOAT = 2   # dst = float2longlong(accessor.get_float(arg1))
EXPR_LOAD_CONST     = 3   # dst = arg1 (immediate i64 value)

# Integer arithmetic
EXPR_INT_ADD = 4
EXPR_INT_SUB = 5
EXPR_INT_MUL = 6
EXPR_INT_DIV = 7
EXPR_INT_MOD = 8
EXPR_INT_NEG = 9

# Float arithmetic (registers hold float bits via float2longlong)
EXPR_FLOAT_ADD = 10
EXPR_FLOAT_SUB = 11
EXPR_FLOAT_MUL = 12
EXPR_FLOAT_DIV = 13
EXPR_FLOAT_NEG = 14

# Integer comparison (signed i64)
EXPR_CMP_EQ = 15
EXPR_CMP_NE = 16
EXPR_CMP_GT = 17
EXPR_CMP_GE = 18
EXPR_CMP_LT = 19
EXPR_CMP_LE = 20

# Float comparison (interprets register bits as f64)
EXPR_FCMP_EQ = 21
EXPR_FCMP_NE = 22
EXPR_FCMP_GT = 23
EXPR_FCMP_GE = 24
EXPR_FCMP_LT = 25
EXPR_FCMP_LE = 26

# Boolean logic
EXPR_BOOL_AND = 27
EXPR_BOOL_OR  = 28
EXPR_BOOL_NOT = 29

# Null checks
EXPR_IS_NULL     = 30   # dst = accessor.is_null(arg1) ? 1 : 0
EXPR_IS_NOT_NULL = 31


# ---------------------------------------------------------------------------
# ExprProgram — immutable bytecode for JIT
# ---------------------------------------------------------------------------

class ExprProgram(object):
    _immutable_fields_ = ['code[*]', 'num_regs', 'result_reg', 'num_instrs']

    def __init__(self, code, num_regs, result_reg):
        self.code = code[:]         # copy: [*] requires a never-resized list
        self.num_regs = num_regs
        self.result_reg = result_reg
        self.num_instrs = len(code) // 4

    def code_as_ints(self):
        result = []
        for w in self.code:
            result.append(intmask(w))
        return result


# ---------------------------------------------------------------------------
# eval_expr — the core interpreter (unrolled by JIT)
# ---------------------------------------------------------------------------

@jit.unroll_safe
def eval_expr(program, accessor):
    """Evaluate expression program against a row. Returns regs[result_reg]."""
    program = jit.promote(program)
    code = program.code
    regs = [r_int64(0)] * program.num_regs

    i = 0
    while i < program.num_instrs:
        base = i * 4
        op  = intmask(code[base])
        dst = intmask(code[base + 1])
        a1  = intmask(code[base + 2])
        a2  = intmask(code[base + 3])

        if op == EXPR_LOAD_COL_INT:
            regs[dst] = accessor.get_int_signed(a1)
        elif op == EXPR_LOAD_COL_FLOAT:
            regs[dst] = float2longlong(accessor.get_float(a1))
        elif op == EXPR_LOAD_CONST:
            regs[dst] = r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))

        elif op == EXPR_INT_ADD:
            regs[dst] = r_int64(intmask(regs[a1] + regs[a2]))
        elif op == EXPR_INT_SUB:
            regs[dst] = r_int64(intmask(regs[a1] - regs[a2]))
        elif op == EXPR_INT_MUL:
            regs[dst] = r_int64(intmask(regs[a1] * regs[a2]))
        elif op == EXPR_INT_DIV:
            d = regs[a2]
            regs[dst] = r_int64(intmask(regs[a1] / d)) if d != r_int64(0) else r_int64(0)
        elif op == EXPR_INT_MOD:
            d = regs[a2]
            regs[dst] = r_int64(intmask(regs[a1] % d)) if d != r_int64(0) else r_int64(0)
        elif op == EXPR_INT_NEG:
            regs[dst] = r_int64(intmask(-regs[a1]))

        elif op == EXPR_FLOAT_ADD:
            regs[dst] = float2longlong(longlong2float(regs[a1]) + longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_SUB:
            regs[dst] = float2longlong(longlong2float(regs[a1]) - longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_MUL:
            regs[dst] = float2longlong(longlong2float(regs[a1]) * longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_DIV:
            rhs = longlong2float(regs[a2])
            if rhs != 0.0:
                regs[dst] = float2longlong(longlong2float(regs[a1]) / rhs)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_FLOAT_NEG:
            regs[dst] = float2longlong(-longlong2float(regs[a1]))

        elif op == EXPR_CMP_EQ:
            regs[dst] = r_int64(1) if regs[a1] == regs[a2] else r_int64(0)
        elif op == EXPR_CMP_NE:
            regs[dst] = r_int64(1) if regs[a1] != regs[a2] else r_int64(0)
        elif op == EXPR_CMP_GT:
            regs[dst] = r_int64(1) if regs[a1] > regs[a2] else r_int64(0)
        elif op == EXPR_CMP_GE:
            regs[dst] = r_int64(1) if regs[a1] >= regs[a2] else r_int64(0)
        elif op == EXPR_CMP_LT:
            regs[dst] = r_int64(1) if regs[a1] < regs[a2] else r_int64(0)
        elif op == EXPR_CMP_LE:
            regs[dst] = r_int64(1) if regs[a1] <= regs[a2] else r_int64(0)

        elif op == EXPR_FCMP_EQ:
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) == longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_NE:
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) != longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_GT:
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) > longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_GE:
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) >= longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_LT:
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) < longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_LE:
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) <= longlong2float(regs[a2]) else r_int64(0)

        elif op == EXPR_BOOL_AND:
            regs[dst] = r_int64(1) if (regs[a1] != r_int64(0) and regs[a2] != r_int64(0)) else r_int64(0)
        elif op == EXPR_BOOL_OR:
            regs[dst] = r_int64(1) if (regs[a1] != r_int64(0) or regs[a2] != r_int64(0)) else r_int64(0)
        elif op == EXPR_BOOL_NOT:
            regs[dst] = r_int64(1) if regs[a1] == r_int64(0) else r_int64(0)

        elif op == EXPR_IS_NULL:
            regs[dst] = r_int64(1) if accessor.is_null(a1) else r_int64(0)
        elif op == EXPR_IS_NOT_NULL:
            regs[dst] = r_int64(0) if accessor.is_null(a1) else r_int64(1)

        i += 1

    return regs[program.result_reg]


# ---------------------------------------------------------------------------
# ExprPredicate — plugs into op_filter via ScalarFunction interface
# ---------------------------------------------------------------------------

class ExprPredicate(ScalarFunction):
    _immutable_fields_ = ['program']

    def __init__(self, program):
        self.program = program

    def evaluate_predicate(self, row_accessor):
        return eval_expr(self.program, row_accessor) != r_int64(0)


# ---------------------------------------------------------------------------
# ExprBuilder — ergonomic API for constructing expression programs
# ---------------------------------------------------------------------------

class ExprBuilder(object):
    def __init__(self):
        self._code = newlist_hint(64)
        self._next_reg = 0

    def _alloc_reg(self):
        r = self._next_reg
        self._next_reg += 1
        return r

    def _emit(self, op, dst, a1, a2):
        self._code.append(r_int64(op))
        self._code.append(r_int64(dst))
        self._code.append(r_int64(a1))
        self._code.append(r_int64(a2))

    # --- Column loads ---

    def load_col_int(self, col_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_LOAD_COL_INT, dst, col_idx, 0)
        return dst

    def load_col_float(self, col_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_LOAD_COL_FLOAT, dst, col_idx, 0)
        return dst

    def load_const_int(self, value):
        dst = self._alloc_reg()
        v = r_int64(value)
        lo = intmask(v)
        hi = intmask(v >> 32)
        self._emit(EXPR_LOAD_CONST, dst, lo, hi)
        return dst

    def load_const_float(self, value):
        dst = self._alloc_reg()
        bits = float2longlong(value)
        lo = intmask(bits)
        hi = intmask(bits >> 32)
        self._emit(EXPR_LOAD_CONST, dst, lo, hi)
        return dst

    # --- Integer arithmetic ---

    def int_add(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_ADD, dst, src1, src2)
        return dst

    def int_sub(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_SUB, dst, src1, src2)
        return dst

    def int_mul(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_MUL, dst, src1, src2)
        return dst

    def int_div(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_DIV, dst, src1, src2)
        return dst

    def int_mod(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_MOD, dst, src1, src2)
        return dst

    def int_neg(self, src):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_NEG, dst, src, 0)
        return dst

    # --- Float arithmetic ---

    def float_add(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FLOAT_ADD, dst, src1, src2)
        return dst

    def float_sub(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FLOAT_SUB, dst, src1, src2)
        return dst

    def float_mul(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FLOAT_MUL, dst, src1, src2)
        return dst

    def float_div(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FLOAT_DIV, dst, src1, src2)
        return dst

    def float_neg(self, src):
        dst = self._alloc_reg()
        self._emit(EXPR_FLOAT_NEG, dst, src, 0)
        return dst

    # --- Integer comparison ---

    def cmp_eq(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_EQ, dst, src1, src2)
        return dst

    def cmp_ne(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_NE, dst, src1, src2)
        return dst

    def cmp_gt(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_GT, dst, src1, src2)
        return dst

    def cmp_ge(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_GE, dst, src1, src2)
        return dst

    def cmp_lt(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_LT, dst, src1, src2)
        return dst

    def cmp_le(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_LE, dst, src1, src2)
        return dst

    # --- Float comparison ---

    def fcmp_eq(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FCMP_EQ, dst, src1, src2)
        return dst

    def fcmp_ne(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FCMP_NE, dst, src1, src2)
        return dst

    def fcmp_gt(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FCMP_GT, dst, src1, src2)
        return dst

    def fcmp_ge(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FCMP_GE, dst, src1, src2)
        return dst

    def fcmp_lt(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FCMP_LT, dst, src1, src2)
        return dst

    def fcmp_le(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_FCMP_LE, dst, src1, src2)
        return dst

    # --- Boolean logic ---

    def bool_and(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_BOOL_AND, dst, src1, src2)
        return dst

    def bool_or(self, src1, src2):
        dst = self._alloc_reg()
        self._emit(EXPR_BOOL_OR, dst, src1, src2)
        return dst

    def bool_not(self, src):
        dst = self._alloc_reg()
        self._emit(EXPR_BOOL_NOT, dst, src, 0)
        return dst

    # --- Null checks ---

    def is_null(self, col_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_IS_NULL, dst, col_idx, 0)
        return dst

    def is_not_null(self, col_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_IS_NOT_NULL, dst, col_idx, 0)
        return dst

    # --- Build ---

    def build(self, result_reg):
        return ExprProgram(self._code, self._next_reg, result_reg)

    def build_predicate(self, result_reg):
        return ExprPredicate(self.build(result_reg))
