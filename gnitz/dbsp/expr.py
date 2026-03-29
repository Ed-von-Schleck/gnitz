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
from rpython.rtyper.lltypesystem import rffi, lltype

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
        from gnitz.storage import engine_ffi
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
        # Build Rust ExprProgram handle
        n = len(self.code)
        code_arr = lltype.malloc(rffi.LONGLONGP.TO, n, flavor="raw")
        for _i in range(n):
            code_arr[_i] = rffi.cast(rffi.LONGLONG, self.code[_i])
        n_consts = len(self.const_strings)
        if n_consts > 0:
            total_len = 0
            for _i in range(n_consts):
                total_len += len(self.const_strings[_i])
            const_data = lltype.malloc(rffi.CCHARP.TO, max(total_len, 1), flavor="raw")
            offsets_arr = lltype.malloc(rffi.UINTP.TO, n_consts, flavor="raw")
            lengths_arr = lltype.malloc(rffi.UINTP.TO, n_consts, flavor="raw")
            off = 0
            for _i in range(n_consts):
                s = self.const_strings[_i]
                offsets_arr[_i] = rffi.cast(rffi.UINT, off)
                lengths_arr[_i] = rffi.cast(rffi.UINT, len(s))
                for _j in range(len(s)):
                    const_data[off + _j] = s[_j]
                off += len(s)
            self._rust_handle = engine_ffi._expr_program_create(
                code_arr, rffi.cast(rffi.UINT, n),
                rffi.cast(rffi.UINT, num_regs), rffi.cast(rffi.UINT, result_reg),
                const_data, offsets_arr, lengths_arr,
                rffi.cast(rffi.UINT, n_consts),
            )
            lltype.free(const_data, flavor="raw")
            lltype.free(offsets_arr, flavor="raw")
            lltype.free(lengths_arr, flavor="raw")
        else:
            self._rust_handle = engine_ffi._expr_program_create(
                code_arr, rffi.cast(rffi.UINT, n),
                rffi.cast(rffi.UINT, num_regs), rffi.cast(rffi.UINT, result_reg),
                lltype.nullptr(rffi.CCHARP.TO),
                lltype.nullptr(rffi.UINTP.TO),
                lltype.nullptr(rffi.UINTP.TO),
                rffi.cast(rffi.UINT, 0),
            )
        lltype.free(code_arr, flavor="raw")

    def close(self):
        from gnitz.storage import engine_ffi
        if self._rust_handle:
            engine_ffi._expr_program_free(self._rust_handle)
            self._rust_handle = lltype.nullptr(rffi.VOIDP.TO)

    def code_as_ints(self):
        result = []
        for w in self.code:
            result.append(intmask(w))
        return result


def _clone_program_handle(program):
    """Create a fresh Rust ExprProgram handle from RPython ExprProgram data.
    The handle is intended to be consumed by scalar func creation (which takes ownership)."""
    from gnitz.storage import engine_ffi
    n = len(program.code)
    code_arr = lltype.malloc(rffi.LONGLONGP.TO, n, flavor="raw")
    for _i in range(n):
        code_arr[_i] = rffi.cast(rffi.LONGLONG, program.code[_i])
    n_consts = len(program.const_strings)
    if n_consts > 0:
        total_len = 0
        for _i in range(n_consts):
            total_len += len(program.const_strings[_i])
        const_data = lltype.malloc(rffi.CCHARP.TO, max(total_len, 1), flavor="raw")
        offsets_arr = lltype.malloc(rffi.UINTP.TO, n_consts, flavor="raw")
        lengths_arr = lltype.malloc(rffi.UINTP.TO, n_consts, flavor="raw")
        off = 0
        for _i in range(n_consts):
            s = program.const_strings[_i]
            offsets_arr[_i] = rffi.cast(rffi.UINT, off)
            lengths_arr[_i] = rffi.cast(rffi.UINT, len(s))
            for _j in range(len(s)):
                const_data[off + _j] = s[_j]
            off += len(s)
        handle = engine_ffi._expr_program_create(
            code_arr, rffi.cast(rffi.UINT, n),
            rffi.cast(rffi.UINT, program.num_regs),
            rffi.cast(rffi.UINT, program.result_reg),
            const_data, offsets_arr, lengths_arr,
            rffi.cast(rffi.UINT, n_consts),
        )
        lltype.free(const_data, flavor="raw")
        lltype.free(offsets_arr, flavor="raw")
        lltype.free(lengths_arr, flavor="raw")
    else:
        handle = engine_ffi._expr_program_create(
            code_arr, rffi.cast(rffi.UINT, n),
            rffi.cast(rffi.UINT, program.num_regs),
            rffi.cast(rffi.UINT, program.result_reg),
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.UINTP.TO),
            lltype.nullptr(rffi.UINTP.TO),
            rffi.cast(rffi.UINT, 0),
        )
    lltype.free(code_arr, flavor="raw")
    return handle


# ---------------------------------------------------------------------------
# eval_expr — the core interpreter (unrolled by JIT)
# ---------------------------------------------------------------------------

@jit.unroll_safe
def eval_expr(program, accessor, builder=None):
    """Evaluate expression program against a row. Returns (value, is_null).
    When builder is not None, also handles EMIT/COPY_COL/EMIT_NULL opcodes."""
    from gnitz.core import types, strings

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
            if builder is not None:
                if null[a1]:
                    builder.append_null(a2)
                else:
                    builder.append_int(regs[a1])

        elif op == EXPR_COPY_COL:
            if builder is not None:
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
                    builder.append_u128(accessor.get_u128_lo(a1), accessor.get_u128_hi(a1))
                else:
                    builder.append_int(accessor.get_int_signed(a1))

        elif op == EXPR_EMIT_NULL:
            if builder is not None:
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

    if program.num_regs == 0:
        return r_int64(0), True
    return regs[program.result_reg], null[program.result_reg]


# ---------------------------------------------------------------------------
# ExprPredicate — plugs into op_filter via ScalarFunction interface
# ---------------------------------------------------------------------------

class ExprPredicate(ScalarFunction):
    _immutable_fields_ = ['program']

    def __init__(self, program):
        from gnitz.storage import engine_ffi
        self.program = program
        prog_handle = _clone_program_handle(program)
        self._func_handle = engine_ffi._scalar_func_create_expr_predicate(
            prog_handle)

    def get_func_handle(self):
        return self._func_handle

    def close(self):
        from gnitz.storage import engine_ffi
        if self._func_handle:
            engine_ffi._scalar_func_free(self._func_handle)
            self._func_handle = lltype.nullptr(rffi.VOIDP.TO)

    def evaluate_predicate(self, row_accessor):
        result, is_null = eval_expr(self.program, row_accessor)
        return (not is_null) and (result != r_int64(0))

    def evaluate_predicate_direct(self, in_batch, row_idx):
        row_null_word = in_batch._read_null_word(row_idx)
        result, is_null, _ = _eval_row_direct(
            self.program, in_batch, row_idx, row_null_word,
            _NO_EMIT_BASES, _NO_EMIT_STRIDES, _NO_EMIT_PAYLOADS)
        return (not is_null) and (result != r_int64(0))


# ---------------------------------------------------------------------------
# Empty sentinels for _eval_row_direct when used as predicate
# ---------------------------------------------------------------------------

_NO_EMIT_BASES = [lltype.nullptr(rffi.CCHARP.TO)][0:0]
_NO_EMIT_STRIDES = [0][0:0]
_NO_EMIT_PAYLOADS = [0][0:0]


# ---------------------------------------------------------------------------
# _eval_row_direct — batch-path per-row compute (no RowBuilder)
# ---------------------------------------------------------------------------

@jit.unroll_safe
def _eval_row_direct(program, in_batch, row_idx, row_null_word,
                              emit_bases, emit_strides, emit_payloads):
    """Evaluate compute instructions for one row, writing EMIT results
    directly to pre-allocated output column buffers.
    COPY_COL and EMIT_NULL are noops (handled at batch level).
    Returns null mask for EMIT columns."""
    from gnitz.core import types as _types
    from gnitz.core import strings as _strings

    program = jit.promote(program)
    code = program.code
    pk_idx = in_batch._schema.pk_index
    regs = [r_int64(0)] * program.num_regs
    null = [False] * program.num_regs
    emit_null_mask = r_uint64(0)
    emit_idx = 0

    i = 0
    while i < program.num_instrs:
        base = i * 4
        op  = intmask(code[base])
        dst = intmask(code[base + 1])
        a1  = intmask(code[base + 2])
        a2  = intmask(code[base + 3])

        if op == EXPR_COPY_COL or op == EXPR_EMIT_NULL:
            pass  # handled at batch level

        elif op == EXPR_LOAD_COL_INT:
            if a1 == pk_idx:
                null[dst] = False
                regs[dst] = rffi.cast(rffi.LONGLONG, in_batch._read_pk_lo(row_idx))
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = row_null_word
                null[dst] = bool(nw & (r_uint64(1) << pi))
                regs[dst] = rffi.cast(rffi.LONGLONG, in_batch._read_col_int(row_idx, a1))

        elif op == EXPR_LOAD_COL_FLOAT:
            if a1 == pk_idx:
                null[dst] = False
                regs[dst] = float2longlong(0.0)
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = row_null_word
                null[dst] = bool(nw & (r_uint64(1) << pi))
                regs[dst] = float2longlong(in_batch._read_col_float(row_idx, a1))

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
            if a1 == pk_idx:
                regs[dst] = r_int64(0)
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = in_batch._read_null_word(row_idx)
                regs[dst] = r_int64(1) if bool(nw & (r_uint64(1) << pi)) else r_int64(0)
        elif op == EXPR_IS_NOT_NULL:
            null[dst] = False
            if a1 == pk_idx:
                regs[dst] = r_int64(1)
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = in_batch._read_null_word(row_idx)
                regs[dst] = r_int64(0) if bool(nw & (r_uint64(1) << pi)) else r_int64(1)

        elif op == EXPR_INT_TO_FLOAT:
            null[dst] = null[a1]
            regs[dst] = float2longlong(float(intmask(regs[a1])))

        elif op == EXPR_EMIT:
            base_ptr = emit_bases[emit_idx]
            stride = emit_strides[emit_idx]
            payload = emit_payloads[emit_idx]
            ptr = rffi.ptradd(base_ptr, row_idx * stride)
            if null[a1]:
                rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, 0)
                emit_null_mask |= r_uint64(1) << payload
            else:
                rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, regs[a1])
            emit_idx += 1

        elif op == EXPR_STR_COL_EQ_CONST:
            if a1 == pk_idx:
                null[dst] = False
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = in_batch._read_null_word(row_idx)
                null[dst] = bool(nw & (r_uint64(1) << pi))
            if not null[dst]:
                _len, _pref, ptr, heap, pystr = in_batch._read_col_str_struct(row_idx, a1)
                const_str = program.const_strings[a2]
                regs[dst] = r_int64(1) if _strings.string_equals(
                    ptr, heap, const_str, program.const_lengths[a2],
                    r_uint64(program.const_prefixes[a2])
                ) else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LT_CONST:
            if a1 == pk_idx:
                null[dst] = False
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = in_batch._read_null_word(row_idx)
                null[dst] = bool(nw & (r_uint64(1) << pi))
            if not null[dst]:
                l_len, l_pref, l_ptr, l_heap, l_str = in_batch._read_col_str_struct(row_idx, a1)
                const_str = program.const_strings[a2]
                cmp = _strings.compare_structures(
                    l_len, l_pref, l_ptr, l_heap, l_str,
                    program.const_lengths[a2],
                    rffi.cast(rffi.LONGLONG, r_uint64(program.const_prefixes[a2])),
                    _strings.NULL_PTR, _strings.NULL_PTR, const_str
                )
                regs[dst] = r_int64(1) if cmp < 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LE_CONST:
            if a1 == pk_idx:
                null[dst] = False
            else:
                pi = a1 if a1 < pk_idx else a1 - 1
                nw = in_batch._read_null_word(row_idx)
                null[dst] = bool(nw & (r_uint64(1) << pi))
            if not null[dst]:
                l_len, l_pref, l_ptr, l_heap, l_str = in_batch._read_col_str_struct(row_idx, a1)
                const_str = program.const_strings[a2]
                cmp = _strings.compare_structures(
                    l_len, l_pref, l_ptr, l_heap, l_str,
                    program.const_lengths[a2],
                    rffi.cast(rffi.LONGLONG, r_uint64(program.const_prefixes[a2])),
                    _strings.NULL_PTR, _strings.NULL_PTR, const_str
                )
                regs[dst] = r_int64(1) if cmp <= 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_EQ_COL:
            nw = in_batch._read_null_word(row_idx)
            n_a1 = False if a1 == pk_idx else bool(nw & (r_uint64(1) << (a1 if a1 < pk_idx else a1 - 1)))
            n_a2 = False if a2 == pk_idx else bool(nw & (r_uint64(1) << (a2 if a2 < pk_idx else a2 - 1)))
            null[dst] = n_a1 or n_a2
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = in_batch._read_col_str_struct(row_idx, a1)
                l2, p2, ptr2, h2, s2 = in_batch._read_col_str_struct(row_idx, a2)
                regs[dst] = r_int64(1) if _strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) == 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LT_COL:
            nw = in_batch._read_null_word(row_idx)
            n_a1 = False if a1 == pk_idx else bool(nw & (r_uint64(1) << (a1 if a1 < pk_idx else a1 - 1)))
            n_a2 = False if a2 == pk_idx else bool(nw & (r_uint64(1) << (a2 if a2 < pk_idx else a2 - 1)))
            null[dst] = n_a1 or n_a2
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = in_batch._read_col_str_struct(row_idx, a1)
                l2, p2, ptr2, h2, s2 = in_batch._read_col_str_struct(row_idx, a2)
                regs[dst] = r_int64(1) if _strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) < 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)
        elif op == EXPR_STR_COL_LE_COL:
            nw = in_batch._read_null_word(row_idx)
            n_a1 = False if a1 == pk_idx else bool(nw & (r_uint64(1) << (a1 if a1 < pk_idx else a1 - 1)))
            n_a2 = False if a2 == pk_idx else bool(nw & (r_uint64(1) << (a2 if a2 < pk_idx else a2 - 1)))
            null[dst] = n_a1 or n_a2
            if not null[dst]:
                l1, p1, ptr1, h1, s1 = in_batch._read_col_str_struct(row_idx, a1)
                l2, p2, ptr2, h2, s2 = in_batch._read_col_str_struct(row_idx, a2)
                regs[dst] = r_int64(1) if _strings.compare_structures(
                    l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
                ) <= 0 else r_int64(0)
            else:
                regs[dst] = r_int64(0)

        i += 1

    if program.num_regs == 0:
        return r_int64(0), True, emit_null_mask
    return regs[program.result_reg], null[program.result_reg], emit_null_mask


# ---------------------------------------------------------------------------
# ExprMapFunction — plugs into op_map via ScalarFunction interface
# ---------------------------------------------------------------------------


class ExprMapFunction(ScalarFunction):
    _immutable_fields_ = ['program',
                          '_copy_src_cols[*]', '_copy_out_payloads[*]',
                          '_copy_type_codes[*]', '_null_payloads[*]',
                          '_emit_out_payloads[*]', '_has_compute']

    def __init__(self, program):
        from gnitz.storage import engine_ffi
        self.program = program
        self._analyze()
        prog_handle = _clone_program_handle(program)
        self._func_handle = engine_ffi._scalar_func_create_expr_map(prog_handle)

    def get_func_handle(self):
        return self._func_handle

    def close(self):
        from gnitz.storage import engine_ffi
        if self._func_handle:
            engine_ffi._scalar_func_free(self._func_handle)
            self._func_handle = lltype.nullptr(rffi.VOIDP.TO)

    def _analyze(self):
        code = self.program.code
        copy_src = []
        copy_out = []
        copy_tc = []
        null_pl = []
        emit_out = []
        has_compute = False

        i = 0
        while i < self.program.num_instrs:
            base = i * 4
            op = intmask(code[base])
            dst = intmask(code[base + 1])
            a1 = intmask(code[base + 2])
            a2 = intmask(code[base + 3])

            if op == EXPR_COPY_COL:
                copy_src.append(a1)
                copy_out.append(a2)
                copy_tc.append(dst)
            elif op == EXPR_EMIT_NULL:
                null_pl.append(a1)
            else:
                has_compute = True
                if op == EXPR_EMIT:
                    emit_out.append(a2)
            i += 1

        self._copy_src_cols = copy_src[:]
        self._copy_out_payloads = copy_out[:]
        self._copy_type_codes = copy_tc[:]
        self._null_payloads = null_pl[:]
        self._emit_out_payloads = emit_out[:]
        self._has_compute = has_compute

    def evaluate_map(self, row_accessor, output_row):
        eval_expr(self.program, row_accessor, output_row)

    @jit.unroll_safe
    def evaluate_map_batch(self, in_batch, out_batch, out_schema):
        from gnitz.core import types as _types
        from gnitz.core import strings as _strings
        from gnitz.storage import engine_ffi, buffer as _buf
        from rpython.rtyper.lltypesystem import lltype

        n = in_batch.length()
        if n == 0:
            return True

        in_schema = in_batch._schema

        # Phase 1: allocate system columns and bulk memcpy pk_lo, pk_hi, weight
        engine_ffi._batch_alloc_system(out_batch._handle, rffi.cast(rffi.UINT, n))
        out_count = out_batch.length()
        sys_off = (out_count - n) * 8

        for ri in range(3):  # 0=pk_lo, 1=pk_hi, 2=weight
            src_ptr = engine_ffi._batch_region_ptr(
                in_batch._handle, rffi.cast(rffi.UINT, ri))
            dst_ptr = engine_ffi._batch_region_ptr(
                out_batch._handle, rffi.cast(rffi.UINT, ri))
            _buf.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(dst_ptr, sys_off)),
                rffi.cast(rffi.VOIDP, src_ptr),
                rffi.cast(rffi.SIZE_T, n * 8),
            )

        # Phase 2: batch-level COPY_COL columns
        copy_src = self._copy_src_cols
        copy_out = self._copy_out_payloads
        copy_tc = self._copy_type_codes

        for k in range(len(copy_src)):
            src_ci = copy_src[k]
            out_payload = copy_out[k]
            tc = copy_tc[k]

            if src_ci == in_schema.pk_index:
                stride = out_schema.columns[
                    out_payload if out_payload < out_schema.pk_index else out_payload + 1
                ].field_type.size
                dest_base = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_payload),
                    rffi.cast(rffi.UINT, n * stride),
                )
                dest_arr = rffi.cast(rffi.ULONGLONGP, dest_base)
                for row in range(n):
                    dest_arr[row] = rffi.cast(
                        rffi.ULONGLONG, in_batch._read_pk_lo(row))
            elif tc == _types.TYPE_STRING.code:
                in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
                stride = in_schema.columns[src_ci].field_type.size
                n_bytes = n * stride
                dest_block = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_payload),
                    rffi.cast(rffi.UINT, n_bytes),
                )
                src_block = engine_ffi._batch_col_ptr(
                    in_batch._handle,
                    rffi.cast(rffi.UINT, 0),
                    rffi.cast(rffi.UINT, in_pi),
                    rffi.cast(rffi.UINT, stride),
                )
                _buf.c_memmove(
                    rffi.cast(rffi.VOIDP, dest_block),
                    rffi.cast(rffi.VOIDP, src_block),
                    rffi.cast(rffi.SIZE_T, n_bytes),
                )
                src_blob_base = engine_ffi._batch_blob_ptr(in_batch._handle)
                for row in range(n):
                    src_ptr = rffi.ptradd(src_block, row * stride)
                    length = rffi.cast(
                        lltype.Signed, rffi.cast(rffi.UINTP, src_ptr)[0])
                    if length > _strings.SHORT_STRING_THRESHOLD:
                        dest_ptr = rffi.ptradd(dest_block, row * stride)
                        old_offset = rffi.cast(
                            rffi.ULONGLONGP, rffi.ptradd(src_ptr, 8))[0]
                        src_data_ptr = rffi.ptradd(
                            src_blob_base,
                            rffi.cast(lltype.Signed, old_offset))
                        new_blob_off = engine_ffi._batch_blob_extend(
                            out_batch._handle,
                            rffi.cast(rffi.UINT, length),
                        )
                        out_blob_ptr = engine_ffi._batch_blob_ptr(out_batch._handle)
                        _buf.c_memmove(
                            rffi.cast(rffi.VOIDP, rffi.ptradd(
                                out_blob_ptr,
                                rffi.cast(lltype.Signed, new_blob_off))),
                            rffi.cast(rffi.VOIDP, src_data_ptr),
                            rffi.cast(rffi.SIZE_T, length),
                        )
                        rffi.cast(
                            rffi.ULONGLONGP,
                            rffi.ptradd(dest_ptr, 8)
                        )[0] = rffi.cast(rffi.ULONGLONG, new_blob_off)
            else:
                in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
                stride = in_schema.columns[src_ci].field_type.size
                dest_block = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_payload),
                    rffi.cast(rffi.UINT, n * stride),
                )
                src_block = engine_ffi._batch_col_ptr(
                    in_batch._handle,
                    rffi.cast(rffi.UINT, 0),
                    rffi.cast(rffi.UINT, in_pi),
                    rffi.cast(rffi.UINT, stride),
                )
                _buf.c_memmove(
                    rffi.cast(rffi.VOIDP, dest_block),
                    rffi.cast(rffi.VOIDP, src_block),
                    rffi.cast(rffi.SIZE_T, n * stride),
                )

        # Phase 3: EMIT_NULL columns — alloc + zero-fill
        null_pls = self._null_payloads
        for k in range(len(null_pls)):
            out_payload = null_pls[k]
            stride = out_schema.columns[
                out_payload if out_payload < out_schema.pk_index else out_payload + 1
            ].field_type.size
            dest_base = engine_ffi._batch_col_extend(
                out_batch._handle,
                rffi.cast(rffi.UINT, out_payload),
                rffi.cast(rffi.UINT, n * stride),
            )
            _buf.c_memset(
                rffi.cast(rffi.VOIDP, dest_base),
                rffi.cast(rffi.INT, 0),
                rffi.cast(rffi.SIZE_T, n * stride),
            )

        # Phase 4: EMIT columns + null bitmap
        emit_pls = self._emit_out_payloads
        emit_count = len(emit_pls)

        # Pre-allocate EMIT target columns
        emit_bases = [lltype.nullptr(rffi.CCHARP.TO)] * emit_count
        emit_strides = [0] * emit_count
        if self._has_compute:
            for k in range(emit_count):
                out_payload = emit_pls[k]
                stride = out_schema.columns[
                    out_payload if out_payload < out_schema.pk_index else out_payload + 1
                ].field_type.size
                emit_bases[k] = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_payload),
                    rffi.cast(rffi.UINT, n * stride),
                )
                emit_strides[k] = stride

        # Write null bitmap via region 3
        null_dst = rffi.ptradd(
            engine_ffi._batch_region_ptr(
                out_batch._handle, rffi.cast(rffi.UINT, 3)),
            sys_off,
        )
        null_arr = rffi.cast(rffi.ULONGLONGP, null_dst)

        # Per-row null bitmap + compute
        for row in range(n):
            in_null = in_batch._read_null_word(row)
            out_null = r_uint64(0)

            # COPY_COL null bits — shuffle from input to output positions
            for k in range(len(copy_src)):
                src_ci = copy_src[k]
                if src_ci == in_schema.pk_index:
                    continue
                in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
                out_null |= ((in_null >> in_pi) & r_uint64(1)) << copy_out[k]

            # EMIT_NULL bits
            for k in range(len(null_pls)):
                out_null |= r_uint64(1) << null_pls[k]

            # Execute compute
            if self._has_compute:
                _, _, mask = _eval_row_direct(
                    self.program, in_batch, row, in_null,
                    emit_bases, emit_strides, emit_pls)
                out_null |= mask

            null_arr[row] = rffi.cast(rffi.ULONGLONG, out_null)

        out_batch._invalidate_cache()
        return True


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
