# gnitz/vm/join.py
#
# Sort-merge equijoin over two ZSetBatch objects.
#
# Migration note (PayloadRow)
# ----------------------------
# Row construction for the join output changes from:
#
#   Before:
#       out_row = make_payload_row(n_output_cols)
#       for val in left_row:          # List[TaggedValue]
#           out_row.append(val)       # appending TaggedValue directly
#       for val in right_row:
#           out_row.append(val)
#
#   After:
#       out_row = make_payload_row(output_schema)
#       join_fn.evaluate_map(left_accessor, right_accessor, out_row)
#
# where ``evaluate_map`` fills in the output row via typed ``append_*`` calls,
# reading from schema-dispatched accessors rather than from ``TaggedValue.tag``.
#
# JoinFunction.evaluate_map signature
# ------------------------------------
#   Before:  evaluate_map(left_accessor, right_accessor, output_row_list)
#   After:   evaluate_map(left_accessor, right_accessor, output_row)
#
# The function appends exactly one value per non-PK output column by calling
# ``output_row.append_*(...)`` based on the schema type of each output column.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask

from gnitz.core import types
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.core.values import make_payload_row, PayloadRow
from gnitz.vm.batch import ZSetBatch, make_empty_batch


# ---------------------------------------------------------------------------
# JoinFunction — defines what to emit for each matched pair
# ---------------------------------------------------------------------------

class JoinFunction(object):
    """
    Abstract base for functions that combine one left row and one right row
    into one output row.  Used by ``MergeJoinBatch``.

    Analogous to ``ScalarFunction`` but takes two input accessors.

    Before: method received two ``List[TaggedValue]`` objects and appended
            ``TaggedValue.make_*(...)`` to the output list.
    After:  method receives two ``RowAccessor`` objects and calls
            ``output_row.append_*(...)`` on the output ``PayloadRow``.
    """

    def evaluate_map(self, left_accessor, right_accessor, output_row):
        """
        Fill in ``output_row`` by reading from ``left_accessor`` and
        ``right_accessor``.

        Parameters
        ----------
        left_accessor : RowAccessor
            Positioned on the current left record.
        right_accessor : RowAccessor
            Positioned on the current right record.
        output_row : PayloadRow
            Fresh row pre-allocated by the caller via
            ``make_payload_row(output_schema)``.  The function appends exactly
            one value per non-PK output column.
        """
        raise NotImplementedError


class ConcatJoinFunction(JoinFunction):
    """
    Concatenate all non-PK columns from the left row followed by all non-PK
    columns from the right row.

    The output schema must have exactly
    ``(n_left_payload_cols + n_right_payload_cols)`` non-PK columns, laid out
    in that order.

    Before: iterated over left and right ``List[TaggedValue]`` in order,
            dispatching on ``val.tag`` to call the appropriate make_* factory.
    After:  iterates over schema columns of each side and calls the
            typed ``append_*`` method corresponding to each column's type.
    """

    _immutable_fields_ = ['_left_schema', '_right_schema']

    def __init__(self, left_schema, right_schema):
        self._left_schema  = left_schema
        self._right_schema = right_schema

    def evaluate_map(self, left_accessor, right_accessor, output_row):
        _copy_all_payload_cols(self._left_schema,  left_accessor,  output_row)
        _copy_all_payload_cols(self._right_schema, right_accessor, output_row)


def _copy_all_payload_cols(schema, accessor, output_row):
    """
    Append every non-PK column of ``schema`` from ``accessor`` to
    ``output_row``, in schema column order.

    Helper shared by ConcatJoinFunction and similar operators.

    Before: callers iterated over ``List[TaggedValue]``, reading ``val.tag``
            to decide which field to copy and which ``make_*`` to call.
    After:  dispatches on ``schema.columns[i].field_type.code`` — the schema
            type is already known, no per-value tag is needed.
    """
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        col_type = schema.columns[i].field_type

        if col_type.code == types.TYPE_STRING.code:
            length, prefix, struct_ptr, heap_ptr, s = \
                accessor.get_str_struct(i)
            output_row.append_string(s)

        elif col_type.code == types.TYPE_F64.code:
            output_row.append_float(accessor.get_float(i))

        elif col_type.code == types.TYPE_U128.code:
            v  = accessor.get_u128(i)
            lo = r_uint64(v)
            hi = r_uint64(v >> 64)
            output_row.append_u128(lo, hi)

        else:
            raw = accessor.get_int(i)
            output_row.append_int(r_int64(intmask(raw)))


# ---------------------------------------------------------------------------
# MergeJoinBatch — O(n+m) sort-merge equijoin on PK
# ---------------------------------------------------------------------------

class MergeJoinBatch(object):
    """
    Sort-merge equijoin between two ``ZSetBatch`` objects that share the same
    PK domain (i.e. the join key IS the primary key of both sides).

    Complexity: O((n + m) log(n + m)) including the sort, O(n + m) for the
    merge step alone if both inputs are pre-sorted.

    Output weight semantics (DBSP product)
    ---------------------------------------
    For each matching pair (left record with weight w_l, right record with
    weight w_r), the output record has weight w_l * w_r.  This implements
    the DBSP bilinear product of Z-sets.

    Inputs must be sorted by PK before calling ``apply``.  Call
    ``batch.sort()`` if needed.
    """

    _immutable_fields_ = ['_left_schema', '_right_schema', '_output_schema',
                          '_join_fn']

    def __init__(self, left_schema, right_schema, output_schema, join_fn):
        """
        Parameters
        ----------
        left_schema : TableSchema
        right_schema : TableSchema
        output_schema : TableSchema
            Schema of the joined output.
        join_fn : JoinFunction
        """
        self._left_schema   = left_schema
        self._right_schema  = right_schema
        self._output_schema = output_schema
        self._join_fn       = join_fn
        self._left_acc  = PayloadRowAccessor(left_schema)
        self._right_acc = PayloadRowAccessor(right_schema)

    def apply(self, left_batch, right_batch):
        """
        Perform the sort-merge join.

        Parameters
        ----------
        left_batch : ZSetBatch
            Must be sorted by PK.
        right_batch : ZSetBatch
            Must be sorted by PK.

        Returns
        -------
        ZSetBatch
            Joined output.
        """
        output_schema = self._output_schema
        output_batch  = make_empty_batch(output_schema)
        join_fn       = self._join_fn
        left_acc      = self._left_acc
        right_acc     = self._right_acc

        n_left  = left_batch.length()
        n_right = right_batch.length()

        i = 0
        j = 0

        while i < n_left and j < n_right:
            lpk = left_batch.get_pk(i)
            rpk = right_batch.get_pk(j)

            if lpk < rpk:
                i += 1
            elif rpk < lpk:
                j += 1
            else:
                # PK match: emit the Cartesian product of all left and right
                # records with this PK.  In the normal database case (unique
                # PK) this is exactly one left × one right.
                pk = lpk

                # Find the extent of the left group.
                i_end = i + 1
                while i_end < n_left and left_batch.get_pk(i_end) == pk:
                    i_end += 1

                # Find the extent of the right group.
                j_end = j + 1
                while j_end < n_right and right_batch.get_pk(j_end) == pk:
                    j_end += 1

                # Emit Cartesian product.
                for li in range(i, i_end):
                    for rj in range(j, j_end):
                        l_weight = left_batch.get_weight(li)
                        r_weight = right_batch.get_weight(rj)
                        # DBSP bilinear product: out_weight = l_weight * r_weight.
                        out_weight = l_weight * r_weight

                        l_row = left_batch.get_row(li)
                        r_row = right_batch.get_row(rj)

                        left_acc.set_row(l_row)
                        right_acc.set_row(r_row)

                        # Allocate fresh output row for this matched pair.
                        out_row = make_payload_row(output_schema)
                        join_fn.evaluate_map(left_acc, right_acc, out_row)

                        output_batch.append(pk, out_weight, out_row)

                i = i_end
                j = j_end

        return output_batch


# ---------------------------------------------------------------------------
# LeftOuterJoinBatch
# ---------------------------------------------------------------------------

class LeftOuterJoinBatch(object):
    """
    Left outer join: emits all left records, with null-filled right columns
    where no right record matches.

    For matched pairs, delegates to a ``JoinFunction`` for output row
    construction.  For unmatched left records, appends the left columns via
    ``_copy_all_payload_cols`` and then calls ``_append_null_right`` to fill
    the right columns with null values.

    Requires both inputs to be sorted by PK.
    """

    _immutable_fields_ = ['_left_schema', '_right_schema', '_output_schema',
                          '_join_fn']

    def __init__(self, left_schema, right_schema, output_schema, join_fn):
        self._left_schema   = left_schema
        self._right_schema  = right_schema
        self._output_schema = output_schema
        self._join_fn       = join_fn
        self._left_acc  = PayloadRowAccessor(left_schema)
        self._right_acc = PayloadRowAccessor(right_schema)

    def apply(self, left_batch, right_batch):
        output_schema = self._output_schema
        output_batch  = make_empty_batch(output_schema)
        join_fn       = self._join_fn
        left_acc      = self._left_acc
        right_acc     = self._right_acc
        right_schema  = self._right_schema

        n_left  = left_batch.length()
        n_right = right_batch.length()

        i = 0
        j = 0

        while i < n_left:
            lpk = left_batch.get_pk(i)

            # Advance right pointer to first entry >= lpk.
            while j < n_right and right_batch.get_pk(j) < lpk:
                j += 1

            l_weight = left_batch.get_weight(i)
            l_row    = left_batch.get_row(i)
            left_acc.set_row(l_row)

            if j < n_right and right_batch.get_pk(j) == lpk:
                # Match: emit joined records.
                j2 = j
                while j2 < n_right and right_batch.get_pk(j2) == lpk:
                    r_weight = right_batch.get_weight(j2)
                    r_row    = right_batch.get_row(j2)
                    right_acc.set_row(r_row)

                    out_weight = l_weight * r_weight
                    out_row    = make_payload_row(output_schema)
                    join_fn.evaluate_map(left_acc, right_acc, out_row)
                    output_batch.append(lpk, out_weight, out_row)
                    j2 += 1
            else:
                # No match: emit left with null-filled right columns.
                out_row = make_payload_row(output_schema)
                _copy_all_payload_cols(self._left_schema, left_acc, out_row)
                _append_null_right_cols(right_schema, out_row)
                output_batch.append(lpk, l_weight, out_row)

            i += 1

        return output_batch


def _append_null_right_cols(right_schema, output_row):
    """
    Append one null slot per non-PK column in ``right_schema`` to
    ``output_row``.

    Used by left outer join for unmatched left records.  The output row's
    ``_has_nullable`` must be True (enforced by the output schema).
    """
    payload_col = 0
    for i in range(len(right_schema.columns)):
        if i == right_schema.pk_index:
            continue
        output_row.append_null(payload_col)
        payload_col += 1
