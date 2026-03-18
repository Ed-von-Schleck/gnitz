# gnitz/dbsp/ops/join.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, intmask

from gnitz.core.comparator import RowAccessor
from gnitz.core.batch import ConsolidatedScope, BatchWriter

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
    def get_u128(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_u128(self.mapping_idx[col_idx])
        return self.right_acc.get_u128(self.mapping_idx[col_idx])

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


CONSOLIDATE_INTERVAL = 8192       # output rows written before each consolidation (delta-trace)
CONSOLIDATE_INTERVAL_DD = 16384   # output rows written before each consolidation (delta-delta)

# ---------------------------------------------------------------------------
# Join operator implementations
# ---------------------------------------------------------------------------


def op_join_delta_trace(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    """
    Delta-Trace Join (Index-Nested-Loop): ΔA ⋈ I(B).

    delta_batch:   ArenaZSetBatch  — the in-flight delta (ΔA)
    trace_cursor:  AbstractCursor  — seekable cursor over the persistent trace (I(B))
    out_writer:    BatchWriter     — strictly write-only destination
    d_schema:      TableSchema     — schema of delta_batch
    t_schema:      TableSchema     — schema of the trace
    """
    composite_acc = CompositeAccessor(d_schema, t_schema)

    count = delta_batch.length()
    rows_since_consolidation = 0
    for i in range(count):
        w_delta = delta_batch.get_weight(i)
        if w_delta == r_int64(0):
            continue

        key = delta_batch.get_pk(i)
        trace_cursor.seek(key)

        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break

            w_trace = trace_cursor.weight()
            # weight multiplication with RPython machine-word truncation
            w_out = r_int64(intmask(w_delta * w_trace))

            if w_out != r_int64(0):
                composite_acc.set_accessors(
                    delta_batch.get_accessor(i),
                    trace_cursor.get_accessor(),
                )
                out_writer.append_from_accessor(key, w_out, composite_acc)
                rows_since_consolidation += 1
                if rows_since_consolidation >= CONSOLIDATE_INTERVAL:
                    out_writer.consolidate()
                    rows_since_consolidation = 0

            trace_cursor.advance()
    out_writer.mark_sorted(delta_batch._sorted)


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
                key_a = b_a.get_pk(idx_a)
                key_b = b_b.get_pk(idx_b)

                if key_a < key_b:
                    idx_a += 1
                elif key_b < key_a:
                    idx_b += 1
                else:
                    match_key = key_a

                    start_a = idx_a
                    while idx_a < n_a and b_a.get_pk(idx_a) == match_key:
                        idx_a += 1

                    start_b = idx_b
                    while idx_b < n_b and b_b.get_pk(idx_b) == match_key:
                        idx_b += 1

                    for i in range(start_a, idx_a):
                        wa = b_a.get_weight(i)
                        if wa == r_int64(0):
                            continue
                        for j in range(start_b, idx_b):
                            wb = b_b.get_weight(j)
                            w_out = r_int64(intmask(wa * wb))
                            if w_out != r_int64(0):
                                composite_acc.set_accessors(
                                    b_a.get_accessor(i),
                                    b_b.get_accessor(j),
                                )
                                out_writer.append_from_accessor(
                                    match_key, w_out, composite_acc
                                )
                                rows_since_consolidation += 1
                                if rows_since_consolidation >= CONSOLIDATE_INTERVAL_DD:
                                    out_writer.consolidate()
                                    rows_since_consolidation = 0
    out_writer.mark_sorted(True)
