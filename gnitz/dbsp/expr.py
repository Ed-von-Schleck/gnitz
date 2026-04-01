# gnitz/dbsp/expr.py
#
# Expression bytecode definitions and Rust FFI wrappers for SQL scalar functions.

from rpython.rlib.rarithmetic import r_int64, intmask
from rpython.rlib.longlong2float import float2longlong
from rpython.rlib.objectmodel import newlist_hint

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

# Output opcodes (Phase 4: computed projections)
EXPR_EMIT          = 32   # builder.append_int(regs[a1]) or append_null(a2)
EXPR_INT_TO_FLOAT  = 33   # regs[dst] = float2longlong(float(intmask(regs[a1])))
EXPR_COPY_COL      = 34   # type-dispatched direct copy: input col -> output col

# Fused string comparison opcodes (Phase 5: string predicates)
EXPR_STR_COL_EQ_CONST = 40   # dst = (col[a1] == const_strings[a2]) ? 1 : 0
EXPR_STR_COL_LT_CONST = 41   # dst = (col[a1] <  const_strings[a2]) ? 1 : 0
EXPR_STR_COL_LE_CONST = 42   # dst = (col[a1] <= const_strings[a2]) ? 1 : 0
EXPR_STR_COL_EQ_COL   = 43   # dst = (col[a1] == col[a2]) ? 1 : 0
EXPR_STR_COL_LT_COL   = 44   # dst = (col[a1] <  col[a2]) ? 1 : 0
EXPR_STR_COL_LE_COL   = 45   # dst = (col[a1] <= col[a2]) ? 1 : 0

# Null emission (Phase 6: LEFT JOIN null-fill)
EXPR_EMIT_NULL = 46   # builder.append_null(a1)  -- a1 = payload_col_idx


# ---------------------------------------------------------------------------
# ExprProgram -- immutable bytecode for JIT
# ---------------------------------------------------------------------------

class ExprProgram(object):
    _immutable_fields_ = ['code[*]', 'num_regs', 'result_reg', 'num_instrs',
                          'const_strings[*]', 'const_prefixes[*]',
                          'const_lengths[*]']

    def __init__(self, code, num_regs, result_reg, const_strings=None):
        from gnitz.core import strings as _strings
        self.code = code[:]         # copy: [*] requires a never-resized list
        self.num_regs = num_regs
        self.result_reg = result_reg
        self.num_instrs = len(code) // 4
        if const_strings is not None:
            self.const_strings = const_strings[:]
            prefixes = [0] * len(const_strings)
            lengths = [0] * len(const_strings)
            for _i in range(len(const_strings)):
                prefixes[_i] = _strings.compute_prefix(const_strings[_i])
                lengths[_i] = len(const_strings[_i])
            self.const_prefixes = prefixes
            self.const_lengths = lengths
        else:
            # RPython annotation: must be same type as the 'not None' branch
            self.const_strings = [""][0:0]
            self.const_prefixes = [0][0:0]
            self.const_lengths = [0][0:0]

    def code_as_ints(self):
        result = []
        for w in self.code:
            result.append(intmask(w))
        return result


# ---------------------------------------------------------------------------
# ExprBuilder -- ergonomic API for constructing expression programs
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

    # --- Type conversion ---

    def int_to_float(self, src):
        dst = self._alloc_reg()
        self._emit(EXPR_INT_TO_FLOAT, dst, src, 0)
        return dst

    # --- Output opcodes ---

    def emit_col(self, src_reg, payload_col_idx):
        self._emit(EXPR_EMIT, 0, src_reg, payload_col_idx)

    def copy_col(self, type_code, src_col_idx, payload_col_idx):
        self._emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx)

    def emit_null(self, payload_col_idx):
        self._emit(EXPR_EMIT_NULL, 0, payload_col_idx, 0)

    # --- String comparison ---

    def str_col_eq_const(self, col_idx, const_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_STR_COL_EQ_CONST, dst, col_idx, const_idx)
        return dst

    def str_col_lt_const(self, col_idx, const_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_STR_COL_LT_CONST, dst, col_idx, const_idx)
        return dst

    def str_col_le_const(self, col_idx, const_idx):
        dst = self._alloc_reg()
        self._emit(EXPR_STR_COL_LE_CONST, dst, col_idx, const_idx)
        return dst

    def str_col_eq_col(self, col_a, col_b):
        dst = self._alloc_reg()
        self._emit(EXPR_STR_COL_EQ_COL, dst, col_a, col_b)
        return dst

    def str_col_lt_col(self, col_a, col_b):
        dst = self._alloc_reg()
        self._emit(EXPR_STR_COL_LT_COL, dst, col_a, col_b)
        return dst

    def str_col_le_col(self, col_a, col_b):
        dst = self._alloc_reg()
        self._emit(EXPR_STR_COL_LE_COL, dst, col_a, col_b)
        return dst

    # --- Build ---

    def build(self, result_reg, const_strings=None):
        return ExprProgram(self._code, self._next_reg, result_reg,
                           const_strings=const_strings)
