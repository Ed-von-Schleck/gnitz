# gnitz/dbsp/ops/join.py

from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, intmask

from gnitz.core.comparator import RowAccessor, NullAccessor
from gnitz.core.batch import ArenaZSetBatch, ConsolidatedScope, BatchWriter, pk_lt, pk_eq

"""
Bilinear Join Operators for the DBSP algebra.

Implements the incremental bilinear expansion:
    Δ(A ⋈ B) = ΔA ⋈ I(B) + I(A) ⋈ ΔB

The VM compiles joins into one or two instructions depending on whether
the operand is a persistent trace (JoinDeltaTrace) or another in-flight
delta (JoinDeltaDelta).
"""


class CompositeAccessor(RowAccessor):
    """
    Virtual accessor that concatenates two RowAccessors.
    Used to implement zero-allocation Joins.

    Column mapping follows TableSchema.merge_schemas_for_join:
      col 0        : PK (from left authority)
      col 1..N     : left non-PK payload columns
      col N+1..M   : right non-PK payload columns
    """

    _immutable_fields_ = [
        "left_schema",
        "right_schema",
        "mapping_is_left[*]",
        "mapping_idx[*]",
    ]

    def __init__(self, left_schema, right_schema):
        self.left_schema = left_schema
        self.right_schema = right_schema
        self.left_acc = None
        self.right_acc = None

        len_l = len(left_schema.columns)
        len_r = len(right_schema.columns)
        total = 1 + (len_l - 1) + (len_r - 1)

        mapping_is_left = [False] * total
        mapping_idx = [0] * total

        # Index 0: PK authority comes from left
        mapping_is_left[0] = True
        mapping_idx[0] = left_schema.pk_index

        curr = 1
        for i in range(len_l):
            if i != left_schema.pk_index:
                mapping_is_left[curr] = True
                mapping_idx[curr] = i
                curr += 1

        for i in range(len_r):
            if i != right_schema.pk_index:
                mapping_is_left[curr] = False
                mapping_idx[curr] = i
                curr += 1

        self.mapping_is_left = mapping_is_left
        self.mapping_idx = mapping_idx

    def set_accessors(self, left, right):
        self.left_acc = left
        self.right_acc = right

    @jit.unroll_safe
    def is_null(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.is_null(self.mapping_idx[col_idx])
        return self.right_acc.is_null(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_int(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_int(self.mapping_idx[col_idx])
        return self.right_acc.get_int(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_int_signed(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_int_signed(self.mapping_idx[col_idx])
        return self.right_acc.get_int_signed(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_float(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_float(self.mapping_idx[col_idx])
        return self.right_acc.get_float(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_u128_lo(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_u128_lo(self.mapping_idx[col_idx])
        return self.right_acc.get_u128_lo(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_u128_hi(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_u128_hi(self.mapping_idx[col_idx])
        return self.right_acc.get_u128_hi(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_str_struct(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_str_struct(self.mapping_idx[col_idx])
        return self.right_acc.get_str_struct(self.mapping_idx[col_idx])

    @jit.unroll_safe
    def get_col_ptr(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_col_ptr(self.mapping_idx[col_idx])
        return self.right_acc.get_col_ptr(self.mapping_idx[col_idx])


CONSOLIDATE_INTERVAL_DD = 16384   # output rows written before each consolidation (delta-delta)

# ---------------------------------------------------------------------------
# Join operator implementations
# ---------------------------------------------------------------------------


def op_join_delta_trace(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    """
    Delta-Trace Join: ΔA ⋈ I(B). Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    out_schema = out_writer.get_schema()
    left_buf = engine_ffi.pack_schema(d_schema)
    right_buf = engine_ffi.pack_schema(t_schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_join_dt(
            delta_batch._handle,
            trace_cursor._handle,
            rffi.cast(rffi.VOIDP, left_buf),
            rffi.cast(rffi.VOIDP, right_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_join_dt failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(out_schema, result_handle, True, False)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(left_buf, flavor="raw")
        lltype.free(right_buf, flavor="raw")


def op_join_delta_trace_outer(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    """
    Delta-Trace Left Outer Join: ΔA LEFT⋈ I(B). Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    out_schema = out_writer.get_schema()
    left_buf = engine_ffi.pack_schema(d_schema)
    right_buf = engine_ffi.pack_schema(t_schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_join_dt_outer(
            delta_batch._handle,
            trace_cursor._handle,
            rffi.cast(rffi.VOIDP, left_buf),
            rffi.cast(rffi.VOIDP, right_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_join_dt_outer failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(out_schema, result_handle, True, False)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(left_buf, flavor="raw")
        lltype.free(right_buf, flavor="raw")


def op_join_delta_delta(batch_a, batch_b, out_writer, schema_a, schema_b):
    """
    Delta-Delta Join (Sort-Merge): ΔA ⋈ ΔB.

    batch_a:    ArenaZSetBatch  — left delta (ΔA)
    batch_b:    ArenaZSetBatch  — right delta (ΔB)
    out_writer: BatchWriter     — strictly write-only destination
    schema_a:   TableSchema     — schema of batch_a
    schema_b:   TableSchema     — schema of batch_b
    """
    with ConsolidatedScope(batch_a) as b_a:
        with ConsolidatedScope(batch_b) as b_b:
            composite_acc = CompositeAccessor(schema_a, schema_b)

            idx_a = 0
            idx_b = 0
            n_a = b_a.length()
            n_b = b_b.length()

            rows_since_consolidation = 0
            while idx_a < n_a and idx_b < n_b:
                key_a_lo, key_a_hi = b_a.get_pk_lo(idx_a), b_a.get_pk_hi(idx_a)
                key_b_lo, key_b_hi = b_b.get_pk_lo(idx_b), b_b.get_pk_hi(idx_b)

                if pk_lt(key_a_lo, key_a_hi, key_b_lo, key_b_hi):
                    idx_a += 1
                elif pk_lt(key_b_lo, key_b_hi, key_a_lo, key_a_hi):
                    idx_b += 1
                else:
                    match_lo, match_hi = key_a_lo, key_a_hi

                    start_a = idx_a
                    idx_a += 1  # b_a[start_a] already known to match
                    while idx_a < n_a and pk_eq(b_a.get_pk_lo(idx_a), b_a.get_pk_hi(idx_a), match_lo, match_hi):
                        idx_a += 1

                    start_b = idx_b
                    idx_b += 1  # b_b[start_b] already known to match
                    while idx_b < n_b and pk_eq(b_b.get_pk_lo(idx_b), b_b.get_pk_hi(idx_b), match_lo, match_hi):
                        idx_b += 1

                    for i in range(start_a, idx_a):
                        wa = b_a.get_weight(i)
                        if wa == r_int64(0):
                            continue
                        composite_acc.left_acc = b_a.get_accessor(i)
                        for j in range(start_b, idx_b):
                            wb = b_b.get_weight(j)
                            w_out = r_int64(intmask(wa * wb))
                            if w_out != r_int64(0):
                                composite_acc.right_acc = b_b.get_accessor(j)
                                out_writer.append_from_accessor(
                                    match_lo, match_hi, w_out, composite_acc
                                )
                                rows_since_consolidation += 1
                                if rows_since_consolidation >= CONSOLIDATE_INTERVAL_DD:
                                    out_writer.consolidate()
                                    rows_since_consolidation = 0
    out_writer.mark_sorted(True)
