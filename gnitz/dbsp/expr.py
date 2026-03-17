# gnitz/dbsp/expr.py
#
# Expression bytecode VM for SQL scalar functions.
# Runs @jit.unroll_safe inside operator row loops — the JIT inlines the
# entire expression evaluation per row, producing native code equivalent
# to hand-written comparisons.

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi

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

# Output opcodes (Phase 4: computed projections)
EXPR_EMIT          = 32   # builder.append_int(regs[a1]) or append_null(a2)
EXPR_INT_TO_FLOAT  = 33   # regs[dst] = float2longlong(float(intmask(regs[a1])))
EXPR_COPY_COL      = 34   # type-dispatched direct copy: input col → output col

# Fused string comparison opcodes (Phase 5: string predicates)
EXPR_STR_COL_EQ_CONST = 40   # dst = (col[a1] == const_strings[a2]) ? 1 : 0
EXPR_STR_COL_LT_CONST = 41   # dst = (col[a1] <  const_strings[a2]) ? 1 : 0
EXPR_STR_COL_LE_CONST = 42   # dst = (col[a1] <= const_strings[a2]) ? 1 : 0
EXPR_STR_COL_EQ_COL   = 43   # dst = (col[a1] == col[a2]) ? 1 : 0
EXPR_STR_COL_LT_COL   = 44   # dst = (col[a1] <  col[a2]) ? 1 : 0
EXPR_STR_COL_LE_COL   = 45   # dst = (col[a1] <= col[a2]) ? 1 : 0

# Null emission (Phase 6: LEFT JOIN null-fill)
EXPR_EMIT_NULL = 46   # builder.append_null(a1)  — a1 = payload_col_idx


# ---------------------------------------------------------------------------
# ExprProgram — immutable bytecode for JIT
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
# eval_expr — the core interpreter (unrolled by JIT)
# ---------------------------------------------------------------------------

@jit.unroll_safe
def eval_expr(program, accessor):
    """Evaluate expression program against a row. Returns (value, is_null)."""
    program = jit.promote(program)
    code = program.code
    regs = [r_int64(0)] * program.num_regs
    null = [False] * program.num_regs

    i = 0
    while i < program.num_instrs:
        base = i * 4
        op  = intmask(code[base])
        dst = intmask(code[base + 1])
        a1  = intmask(code[base + 2])
        a2  = intmask(code[base + 3])

        if op == EXPR_LOAD_COL_INT:
            null[dst] = accessor.is_null(a1)
            regs[dst] = accessor.get_int_signed(a1)
        elif op == EXPR_LOAD_COL_FLOAT:
            null[dst] = accessor.is_null(a1)
            regs[dst] = float2longlong(accessor.get_float(a1))
        elif op == EXPR_LOAD_CONST:
            null[dst] = False
            regs[dst] = r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))

        elif op == EXPR_INT_ADD:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(intmask(regs[a1] + regs[a2]))
        elif op == EXPR_INT_SUB:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(intmask(regs[a1] - regs[a2]))
        elif op == EXPR_INT_MUL:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(intmask(regs[a1] * regs[a2]))
        elif op == EXPR_INT_DIV:
            null[dst] = null[a1] or null[a2]
            d = regs[a2]
            regs[dst] = r_int64(intmask(regs[a1] / d)) if d != r_int64(0) else r_int64(0)
        elif op == EXPR_INT_MOD:
            null[dst] = null[a1] or null[a2]
            d = regs[a2]
            regs[dst] = r_int64(intmask(regs[a1] % d)) if d != r_int64(0) else r_int64(0)
        elif op == EXPR_INT_NEG:
            null[dst] = null[a1]
            regs[dst] = r_int64(intmask(-regs[a1]))

        elif op == EXPR_FLOAT_ADD:
            null[dst] = null[a1] or null[a2]
            regs[dst] = float2longlong(longlong2float(regs[a1]) + longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_SUB:
            null[dst] = null[a1] or null[a2]
            regs[dst] = float2longlong(longlong2float(regs[a1]) - longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_MUL:
            null[dst] = null[a1] or null[a2]
            regs[dst] = float2longlong(longlong2float(regs[a1]) * longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_DIV:
            null[dst] = null[a1] or null[a2]
            rhs = longlong2float(regs[a2])
            if rhs != 0.0:
                regs[dst] = float2longlong(longlong2float(regs[a1]) / rhs)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_FLOAT_NEG:
            null[dst] = null[a1]
            regs[dst] = float2longlong(-longlong2float(regs[a1]))

        elif op == EXPR_CMP_EQ:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] == regs[a2] else r_int64(0)
        elif op == EXPR_CMP_NE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] != regs[a2] else r_int64(0)
        elif op == EXPR_CMP_GT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] > regs[a2] else r_int64(0)
        elif op == EXPR_CMP_GE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] >= regs[a2] else r_int64(0)
        elif op == EXPR_CMP_LT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] < regs[a2] else r_int64(0)
        elif op == EXPR_CMP_LE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] <= regs[a2] else r_int64(0)

        elif op == EXPR_FCMP_EQ:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) == longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_NE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) != longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_GT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) > longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_GE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) >= longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_LT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) < longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_LE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) <= longlong2float(regs[a2]) else r_int64(0)

        elif op == EXPR_BOOL_AND:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if (regs[a1] != r_int64(0) and regs[a2] != r_int64(0)) else r_int64(0)
        elif op == EXPR_BOOL_OR:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if (regs[a1] != r_int64(0) or regs[a2] != r_int64(0)) else r_int64(0)
        elif op == EXPR_BOOL_NOT:
            null[dst] = null[a1]
            regs[dst] = r_int64(1) if regs[a1] == r_int64(0) else r_int64(0)

        elif op == EXPR_IS_NULL:
            null[dst] = False
            regs[dst] = r_int64(1) if accessor.is_null(a1) else r_int64(0)
        elif op == EXPR_IS_NOT_NULL:
            null[dst] = False
            regs[dst] = r_int64(0) if accessor.is_null(a1) else r_int64(1)

        elif op == EXPR_STR_COL_EQ_CONST:
            from gnitz.core import strings
            null[dst] = accessor.is_null(a1)
            if not null[dst]:
                _len, _pref, ptr, heap, pystr = accessor.get_str_struct(a1)
                const_str = program.const_strings[a2]
                regs[dst] = r_int64(1) if strings.string_equals(
                    ptr, heap, const_str, program.const_lengths[a2],
                    r_uint64(program.const_prefixes[a2])
                ) else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LT_CONST:
            from gnitz.core import strings
            null[dst] = accessor.is_null(a1)
            if not null[dst]:
                l_len, l_pref, l_ptr, l_heap, l_str = accessor.get_str_struct(a1)
                const_str = program.const_strings[a2]
                cmp = strings.compare_structures(
                    l_len, l_pref, l_ptr, l_heap, l_str,
                    program.const_lengths[a2],
                    rffi.cast(rffi.LONGLONG, r_uint64(program.const_prefixes[a2])),
                    strings.NULL_PTR, strings.NULL_PTR, const_str
                )
                regs[dst] = r_int64(1) if cmp < 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LE_CONST:
            from gnitz.core import strings
            null[dst] = accessor.is_null(a1)
            if not null[dst]:
                l_len, l_pref, l_ptr, l_heap, l_str = accessor.get_str_struct(a1)
                const_str = program.const_strings[a2]
                cmp = strings.compare_structures(
                    l_len, l_pref, l_ptr, l_heap, l_str,
                    program.const_lengths[a2],
                    rffi.cast(rffi.LONGLONG, r_uint64(program.const_prefixes[a2])),
                    strings.NULL_PTR, strings.NULL_PTR, const_str
                )
                regs[dst] = r_int64(1) if cmp <= 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_EQ_COL:
            from gnitz.core import strings
            null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
                l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
                regs[dst] = r_int64(1) if strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) == 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LT_COL:
            from gnitz.core import strings
            null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
                l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
                regs[dst] = r_int64(1) if strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) < 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LE_COL:
            from gnitz.core import strings
            null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
                l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
                regs[dst] = r_int64(1) if strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) <= 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)

        i += 1

    return regs[program.result_reg], null[program.result_reg]


# ---------------------------------------------------------------------------
# ExprPredicate — plugs into op_filter via ScalarFunction interface
# ---------------------------------------------------------------------------

class ExprPredicate(ScalarFunction):
    _immutable_fields_ = ['program']

    def __init__(self, program):
        self.program = program

    def evaluate_predicate(self, row_accessor):
        result, is_null = eval_expr(self.program, row_accessor)
        return (not is_null) and (result != r_int64(0))


# ---------------------------------------------------------------------------
# eval_expr_map — expression VM for computed projections (Phase 4)
# ---------------------------------------------------------------------------

@jit.unroll_safe
def eval_expr_map(program, accessor, builder):
    """Evaluate expression map program: computes columns and writes to builder."""
    from gnitz.core import types, strings
    from rpython.rlib.rarithmetic import r_uint64

    program = jit.promote(program)
    code = program.code
    regs = [r_int64(0)] * program.num_regs
    null = [False] * program.num_regs

    i = 0
    while i < program.num_instrs:
        base = i * 4
        op  = intmask(code[base])
        dst = intmask(code[base + 1])
        a1  = intmask(code[base + 2])
        a2  = intmask(code[base + 3])

        if op == EXPR_LOAD_COL_INT:
            null[dst] = accessor.is_null(a1)
            regs[dst] = accessor.get_int_signed(a1)
        elif op == EXPR_LOAD_COL_FLOAT:
            null[dst] = accessor.is_null(a1)
            regs[dst] = float2longlong(accessor.get_float(a1))
        elif op == EXPR_LOAD_CONST:
            null[dst] = False
            regs[dst] = r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))

        elif op == EXPR_INT_ADD:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(intmask(regs[a1] + regs[a2]))
        elif op == EXPR_INT_SUB:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(intmask(regs[a1] - regs[a2]))
        elif op == EXPR_INT_MUL:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(intmask(regs[a1] * regs[a2]))
        elif op == EXPR_INT_DIV:
            null[dst] = null[a1] or null[a2]
            d = regs[a2]
            regs[dst] = r_int64(intmask(regs[a1] / d)) if d != r_int64(0) else r_int64(0)
        elif op == EXPR_INT_MOD:
            null[dst] = null[a1] or null[a2]
            d = regs[a2]
            regs[dst] = r_int64(intmask(regs[a1] % d)) if d != r_int64(0) else r_int64(0)
        elif op == EXPR_INT_NEG:
            null[dst] = null[a1]
            regs[dst] = r_int64(intmask(-regs[a1]))

        elif op == EXPR_FLOAT_ADD:
            null[dst] = null[a1] or null[a2]
            regs[dst] = float2longlong(longlong2float(regs[a1]) + longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_SUB:
            null[dst] = null[a1] or null[a2]
            regs[dst] = float2longlong(longlong2float(regs[a1]) - longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_MUL:
            null[dst] = null[a1] or null[a2]
            regs[dst] = float2longlong(longlong2float(regs[a1]) * longlong2float(regs[a2]))
        elif op == EXPR_FLOAT_DIV:
            null[dst] = null[a1] or null[a2]
            rhs = longlong2float(regs[a2])
            if rhs != 0.0:
                regs[dst] = float2longlong(longlong2float(regs[a1]) / rhs)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_FLOAT_NEG:
            null[dst] = null[a1]
            regs[dst] = float2longlong(-longlong2float(regs[a1]))

        elif op == EXPR_CMP_EQ:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] == regs[a2] else r_int64(0)
        elif op == EXPR_CMP_NE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] != regs[a2] else r_int64(0)
        elif op == EXPR_CMP_GT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] > regs[a2] else r_int64(0)
        elif op == EXPR_CMP_GE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] >= regs[a2] else r_int64(0)
        elif op == EXPR_CMP_LT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] < regs[a2] else r_int64(0)
        elif op == EXPR_CMP_LE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if regs[a1] <= regs[a2] else r_int64(0)

        elif op == EXPR_FCMP_EQ:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) == longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_NE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) != longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_GT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) > longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_GE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) >= longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_LT:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) < longlong2float(regs[a2]) else r_int64(0)
        elif op == EXPR_FCMP_LE:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if longlong2float(regs[a1]) <= longlong2float(regs[a2]) else r_int64(0)

        elif op == EXPR_BOOL_AND:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if (regs[a1] != r_int64(0) and regs[a2] != r_int64(0)) else r_int64(0)
        elif op == EXPR_BOOL_OR:
            null[dst] = null[a1] or null[a2]
            regs[dst] = r_int64(1) if (regs[a1] != r_int64(0) or regs[a2] != r_int64(0)) else r_int64(0)
        elif op == EXPR_BOOL_NOT:
            null[dst] = null[a1]
            regs[dst] = r_int64(1) if regs[a1] == r_int64(0) else r_int64(0)

        elif op == EXPR_IS_NULL:
            null[dst] = False
            regs[dst] = r_int64(1) if accessor.is_null(a1) else r_int64(0)
        elif op == EXPR_IS_NOT_NULL:
            null[dst] = False
            regs[dst] = r_int64(0) if accessor.is_null(a1) else r_int64(1)

        elif op == EXPR_INT_TO_FLOAT:
            null[dst] = null[a1]
            regs[dst] = float2longlong(float(intmask(regs[a1])))

        elif op == EXPR_EMIT:
            # a1 = src_reg, a2 = payload_col_idx
            if null[a1]:
                builder.append_null(a2)
            else:
                builder.append_int(regs[a1])

        elif op == EXPR_COPY_COL:
            # dst = type_code, a1 = src_col_idx, a2 = payload_col_idx
            tc = dst
            if accessor.is_null(a1):
                builder.append_null(a2)
            elif tc == types.TYPE_STRING.code:
                res = accessor.get_str_struct(a1)
                s = strings.resolve_string(res[2], res[3], res[4])
                builder.append_string(s)
            elif tc == types.TYPE_F64.code or tc == types.TYPE_F32.code:
                builder.append_float(accessor.get_float(a1))
            elif tc == types.TYPE_U128.code:
                val = accessor.get_u128(a1)
                builder.append_u128(r_uint64(intmask(val)), r_uint64(intmask(val >> 64)))
            else:
                builder.append_int(accessor.get_int_signed(a1))

        elif op == EXPR_EMIT_NULL:
            # a1 = payload_col_idx
            builder.append_null(a1)

        elif op == EXPR_STR_COL_EQ_CONST:
            null[dst] = accessor.is_null(a1)
            if not null[dst]:
                _len, _pref, ptr, heap, pystr = accessor.get_str_struct(a1)
                const_str = program.const_strings[a2]
                regs[dst] = r_int64(1) if strings.string_equals(
                    ptr, heap, const_str, program.const_lengths[a2],
                    r_uint64(program.const_prefixes[a2])
                ) else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LT_CONST:
            null[dst] = accessor.is_null(a1)
            if not null[dst]:
                l_len, l_pref, l_ptr, l_heap, l_str = accessor.get_str_struct(a1)
                const_str = program.const_strings[a2]
                cmp = strings.compare_structures(
                    l_len, l_pref, l_ptr, l_heap, l_str,
                    program.const_lengths[a2],
                    rffi.cast(rffi.LONGLONG, r_uint64(program.const_prefixes[a2])),
                    strings.NULL_PTR, strings.NULL_PTR, const_str
                )
                regs[dst] = r_int64(1) if cmp < 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LE_CONST:
            null[dst] = accessor.is_null(a1)
            if not null[dst]:
                l_len, l_pref, l_ptr, l_heap, l_str = accessor.get_str_struct(a1)
                const_str = program.const_strings[a2]
                cmp = strings.compare_structures(
                    l_len, l_pref, l_ptr, l_heap, l_str,
                    program.const_lengths[a2],
                    rffi.cast(rffi.LONGLONG, r_uint64(program.const_prefixes[a2])),
                    strings.NULL_PTR, strings.NULL_PTR, const_str
                )
                regs[dst] = r_int64(1) if cmp <= 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_EQ_COL:
            null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
                l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
                regs[dst] = r_int64(1) if strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) == 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LT_COL:
            null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
                l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
                regs[dst] = r_int64(1) if strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) < 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LE_COL:
            null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
                l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
                regs[dst] = r_int64(1) if strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) <= 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)

        i += 1


class ExprMapFunction(ScalarFunction):
    _immutable_fields_ = ['program']

    def __init__(self, program):
        self.program = program

    def evaluate_map(self, row_accessor, output_row):
        eval_expr_map(self.program, row_accessor, output_row)


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

    def build_predicate(self, result_reg, const_strings=None):
        return ExprPredicate(self.build(result_reg, const_strings=const_strings))
