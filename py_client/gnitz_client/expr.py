"""Client-side expression bytecode helper for filter predicates.

Mirrors the server-side ExprProgram 4-word encoding: (opcode, dst, arg1, arg2).
Produces dicts with 'num_regs', 'result_reg', 'code' ready for packing
into PARAM_EXPR_BASE slots on CircuitBuilder filter nodes.
"""

# Expression opcodes (must match gnitz/dbsp/expr.py)
EXPR_LOAD_COL_INT = 1
EXPR_LOAD_COL_FLOAT = 2
EXPR_LOAD_CONST = 3
EXPR_INT_ADD = 4
EXPR_INT_SUB = 5
EXPR_CMP_EQ = 15
EXPR_CMP_NE = 16
EXPR_CMP_GT = 17
EXPR_CMP_GE = 18
EXPR_CMP_LT = 19
EXPR_CMP_LE = 20
EXPR_BOOL_AND = 27
EXPR_BOOL_OR = 28
EXPR_BOOL_NOT = 29
EXPR_IS_NULL = 30
EXPR_IS_NOT_NULL = 31


class ExprBuilder:
    """Ergonomic builder for expression bytecode (client-side)."""

    def __init__(self):
        self._code: list[int] = []
        self._next_reg = 0

    def _alloc_reg(self) -> int:
        r = self._next_reg
        self._next_reg += 1
        return r

    def _emit(self, op: int, dst: int, a1: int, a2: int):
        self._code.extend([op, dst, a1, a2])

    def load_col_int(self, col_idx: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_LOAD_COL_INT, dst, col_idx, 0)
        return dst

    def load_const(self, value: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_LOAD_CONST, dst, value, 0)
        return dst

    def cmp_gt(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_GT, dst, a, b)
        return dst

    def cmp_eq(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_EQ, dst, a, b)
        return dst

    def cmp_lt(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_LT, dst, a, b)
        return dst

    def cmp_ge(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_GE, dst, a, b)
        return dst

    def cmp_le(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_LE, dst, a, b)
        return dst

    def cmp_ne(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_CMP_NE, dst, a, b)
        return dst

    def bool_and(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_BOOL_AND, dst, a, b)
        return dst

    def bool_or(self, a: int, b: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_BOOL_OR, dst, a, b)
        return dst

    def bool_not(self, a: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_BOOL_NOT, dst, a, 0)
        return dst

    def is_not_null(self, col_idx: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_IS_NOT_NULL, dst, col_idx, 0)
        return dst

    def is_null(self, col_idx: int) -> int:
        dst = self._alloc_reg()
        self._emit(EXPR_IS_NULL, dst, col_idx, 0)
        return dst

    def build(self, result_reg: int) -> dict:
        return {
            'num_regs': self._next_reg,
            'result_reg': result_reg,
            'code': list(self._code),
        }
