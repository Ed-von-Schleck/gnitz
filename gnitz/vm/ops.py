# gnitz/vm/operators.py
#
# Base classes and core VM operators for the GnitzDB DBSP circuit.
#
# Migration note (PayloadRow)
# ----------------------------
# The key change in this file is the ``evaluate_map`` signature:
#
#   Before:  evaluate_map(self, input_accessor, output_row_list)
#            where output_row_list: List[TaggedValue] being built by the caller
#            and the function calls output_row_list.append(TaggedValue.make_*(v))
#
#   After:   evaluate_map(self, input_accessor, output_row)
#            where output_row: PayloadRow pre-allocated by the caller via
#            make_payload_row(output_schema), and the function calls
#            output_row.append_*(v) for each output column.
#
# The caller's loop pattern also changes:
#
#   Before:
#       output_row = make_payload_row(n_output_cols)
#       scalar_fn.evaluate_map(input_accessor, output_row)
#       output_batch.append(pk, weight, output_row)
#
#   After:
#       output_row = make_payload_row(output_schema)
#       scalar_fn.evaluate_map(input_accessor, output_row)
#       output_batch.append(pk, weight, output_row)
#
# The row is still pre-allocated by the caller (one allocation per output
# record); the only difference is that the schema drives allocation rather
# than a raw column count.  The caller must NOT reuse the same PayloadRow
# object across iterations — each call to make_payload_row produces a fresh
# object with empty arrays.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rlib.longlong2float import longlong2float
from rpython.rtyper.lltypesystem import rffi

from gnitz.core import types
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.core.values import make_payload_row, PayloadRow
from gnitz.vm.batch import ZSetBatch, make_empty_batch


# ---------------------------------------------------------------------------
# ScalarFunction — abstract base for row-to-row transformations
# ---------------------------------------------------------------------------

class ScalarFunction(object):
    """
    Abstract base for functions that transform one input row into one output
    row.  Used by ``MapBatch`` and the VM's general MAP operator.

    A ``ScalarFunction`` is stateless with respect to the batch it processes —
    each call to ``evaluate_map`` is independent.

    Subclasses must override ``evaluate_map``.
    """

    def evaluate_map(self, input_accessor, output_row):
        """
        Fill in ``output_row`` by reading from ``input_accessor``.

        Parameters
        ----------
        input_accessor : RowAccessor
            Accessor positioned on the current input record.  The accessor's
            ``get_int``, ``get_float``, ``get_u128``, and ``get_str_struct``
            methods return values by **schema column index** (0-based,
            including the PK column position).

        output_row : PayloadRow
            A fresh ``PayloadRow`` pre-allocated by the caller via
            ``make_payload_row(output_schema)``.  The function appends exactly
            one value per non-PK column in the output schema, in column-index
            order, by calling ``output_row.append_int``, ``append_float``,
            ``append_string``, ``append_u128``, or ``append_null``.

        The function must not read from ``output_row`` (it is write-only
        during construction).  It must not store a reference to ``output_row``
        beyond the duration of this call — the caller takes ownership
        immediately after the call returns.
        """
        raise NotImplementedError


class Predicate(object):
    """
    Abstract base for boolean predicates on rows.  Used by ``FilterBatch``.
    """

    def evaluate(self, pk, input_accessor):
        """
        Evaluate the predicate on the current record.

        Parameters
        ----------
        pk : r_uint128
            The primary key of the record.
        input_accessor : RowAccessor
            Positioned on the current record.

        Returns
        -------
        bool
            True if the record should be kept.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Concrete ScalarFunction implementations
# ---------------------------------------------------------------------------

class IdentityFunction(ScalarFunction):
    """
    Pass-through: copies all non-PK input columns to the output unchanged.

    The input schema and output schema must have identical non-PK column
    layouts.  Used to re-emit a batch with a different weight or PK without
    changing the payload.

    Before: appended ``TaggedValue`` objects read from ``val.i64``, ``val.f64``,
            ``val.str_val``, dispatching on ``val.tag``.
    After:  reads via schema dispatch on ``col_type.code`` and calls the
            appropriate ``append_*`` on the output ``PayloadRow``.
    """

    _immutable_fields_ = ['_schema']

    def __init__(self, schema):
        self._schema = schema

    def evaluate_map(self, input_accessor, output_row):
        schema = self._schema
        payload_col = 0
        for i in range(len(schema.columns)):
            if i == schema.pk_index:
                continue
            col_type = schema.columns[i].field_type

            if col_type.code == types.TYPE_STRING.code:
                length, prefix, struct_ptr, heap_ptr, s = \
                    input_accessor.get_str_struct(i)
                output_row.append_string(s)

            elif col_type.code == types.TYPE_F64.code:
                output_row.append_float(input_accessor.get_float(i))

            elif col_type.code == types.TYPE_U128.code:
                v = input_accessor.get_u128(i)
                lo = r_uint64(v)
                hi = r_uint64(v >> 64)
                output_row.append_u128(lo, hi)

            else:
                # All integer types: store as r_int64 via bitcast.
                raw = input_accessor.get_int(i)          # r_uint64
                output_row.append_int(r_int64(intmask(raw)))

            payload_col += 1


class ProjectionFunction(ScalarFunction):
    """
    Emit a subset of input columns as the output row.

    ``col_indices`` is a list of **schema** column indices (0-based, including
    the PK position) specifying which columns to emit, in output order.  The
    PK column index must not appear in ``col_indices`` (the PK is handled by
    the operator, not the projection function).

    ``output_schema`` describes the layout of the projected output.  It must
    have one non-PK column for each entry in ``col_indices``.

    Before: iterated over ``List[TaggedValue]`` picking elements by index,
            reading ``val.tag`` to dispatch.
    After:  reads each source column via the input accessor using the source
            column's schema-known type, and writes to the output PayloadRow.
    """

    _immutable_fields_ = ['_input_schema', '_output_schema']

    def __init__(self, input_schema, output_schema, col_indices):
        """
        Parameters
        ----------
        input_schema : TableSchema
            Schema of the source batch.
        output_schema : TableSchema
            Schema of the projected output batch.
        col_indices : List[int]
            Source schema column indices to project, in output column order.
            Must exclude the PK column from the source schema.
        """
        self._input_schema  = input_schema
        self._output_schema = output_schema
        self._col_indices   = col_indices   # List[int]

    def evaluate_map(self, input_accessor, output_row):
        input_schema = self._input_schema
        col_indices  = self._col_indices

        for ci in range(len(col_indices)):
            src_col = col_indices[ci]
            col_type = input_schema.columns[src_col].field_type

            if col_type.code == types.TYPE_STRING.code:
                length, prefix, struct_ptr, heap_ptr, s = \
                    input_accessor.get_str_struct(src_col)
                output_row.append_string(s)

            elif col_type.code == types.TYPE_F64.code:
                output_row.append_float(input_accessor.get_float(src_col))

            elif col_type.code == types.TYPE_U128.code:
                v = input_accessor.get_u128(src_col)
                lo = r_uint64(v)
                hi = r_uint64(v >> 64)
                output_row.append_u128(lo, hi)

            else:
                raw = input_accessor.get_int(src_col)
                output_row.append_int(r_int64(intmask(raw)))


# ---------------------------------------------------------------------------
# MapBatch — apply a ScalarFunction to each record in a ZSetBatch
# ---------------------------------------------------------------------------

class MapBatch(object):
    """
    Applies a ``ScalarFunction`` to each record in a ``ZSetBatch``, producing
    a new ``ZSetBatch`` with the same PKs and weights but transformed payloads.

    This is the DBSP MAP operator specialised for batch-at-a-time execution.

    The output schema may differ from the input schema (e.g. after projection
    or type-widening computations).

    Before: ``scalar_fn.evaluate_map(input_accessor, output_row_list)`` where
            ``output_row_list`` was a ``List[TaggedValue]`` being constructed
            by appending ``TaggedValue.make_*(...)`` objects.
    After:  ``scalar_fn.evaluate_map(input_accessor, output_row)`` where
            ``output_row`` is a fresh ``PayloadRow`` filled in via
            ``append_*`` calls.

    The output_row is allocated fresh for each input record via
    ``make_payload_row(output_schema)``.  This is deliberate: the allocator
    fast-path in RPython's incminimark GC makes fresh small objects cheap, and
    PayloadRow is far smaller than the former List[TaggedValue] equivalent.
    """

    _immutable_fields_ = ['_input_schema', '_output_schema', '_scalar_fn']

    def __init__(self, input_schema, output_schema, scalar_fn):
        """
        Parameters
        ----------
        input_schema : TableSchema
        output_schema : TableSchema
        scalar_fn : ScalarFunction
        """
        self._input_schema  = input_schema
        self._output_schema = output_schema
        self._scalar_fn     = scalar_fn
        # Pre-allocate a reusable accessor for the input row.
        # The accessor is reset via set_row() on each iteration.
        self._accessor = PayloadRowAccessor(input_schema)

    def apply(self, input_batch):
        """
        Apply the scalar function to every record in ``input_batch``.

        Parameters
        ----------
        input_batch : ZSetBatch

        Returns
        -------
        ZSetBatch
            New batch with the same PKs and weights, transformed payloads.
        """
        output_schema = self._output_schema
        output_batch  = make_empty_batch(output_schema)
        accessor      = self._accessor
        scalar_fn     = self._scalar_fn
        n             = input_batch.length()

        for i in range(n):
            pk     = input_batch.get_pk(i)
            weight = input_batch.get_weight(i)
            row    = input_batch.get_row(i)

            accessor.set_row(row)

            # Allocate a fresh output row for each input record.
            # MUST NOT reuse across iterations — each PayloadRow is mutable
            # during construction and immutable once passed to append().
            output_row = make_payload_row(output_schema)
            scalar_fn.evaluate_map(accessor, output_row)
            output_batch.append(pk, weight, output_row)

        return output_batch


# ---------------------------------------------------------------------------
# FilterBatch — keep only records satisfying a predicate
# ---------------------------------------------------------------------------

class FilterBatch(object):
    """
    Filters a ``ZSetBatch`` by a ``Predicate``, retaining only records for
    which the predicate returns ``True``.

    The output schema is identical to the input schema.  Weights are
    preserved unchanged.

    The predicate receives the record's PK and an input ``RowAccessor``
    positioned on the current row.  It must not store the accessor reference.
    """

    _immutable_fields_ = ['_schema', '_predicate']

    def __init__(self, schema, predicate):
        self._schema    = schema
        self._predicate = predicate
        self._accessor  = PayloadRowAccessor(schema)

    def apply(self, input_batch):
        """
        Parameters
        ----------
        input_batch : ZSetBatch

        Returns
        -------
        ZSetBatch
            New batch containing only the records for which the predicate
            returned True.
        """
        output_batch = make_empty_batch(self._schema)
        accessor     = self._accessor
        predicate    = self._predicate
        n            = input_batch.length()

        for i in range(n):
            pk     = input_batch.get_pk(i)
            weight = input_batch.get_weight(i)
            row    = input_batch.get_row(i)

            accessor.set_row(row)
            if predicate.evaluate(pk, accessor):
                output_batch.append(pk, weight, row)

        return output_batch


# ---------------------------------------------------------------------------
# NegBatch — negate all weights (DBSP negation)
# ---------------------------------------------------------------------------

class NegBatch(object):
    """
    Negate every weight in a batch.

    In DBSP, the algebraic negation of a Z-set maps each record's weight to
    its arithmetic negative.  Used to implement logical deletion and in the
    construction of the differentiation operator.
    """

    _immutable_fields_ = ['_schema']

    def __init__(self, schema):
        self._schema = schema

    def apply(self, input_batch):
        """
        Parameters
        ----------
        input_batch : ZSetBatch

        Returns
        -------
        ZSetBatch
            New batch with all weights negated.
        """
        output_batch = make_empty_batch(self._schema)
        n = input_batch.length()
        for i in range(n):
            pk     = input_batch.get_pk(i)
            weight = input_batch.get_weight(i)
            row    = input_batch.get_row(i)
            output_batch.append(pk, r_int64(-intmask(weight)), row)
        return output_batch


# ---------------------------------------------------------------------------
# AddBatches — DBSP addition (union of two Z-sets)
# ---------------------------------------------------------------------------

def add_batches(schema, left_batch, right_batch):
    """
    Compute the DBSP sum of two ``ZSetBatch`` objects with the same schema.

    The result is a new batch containing all entries from both inputs.
    Callers should call ``sort()`` followed by ``consolidate()`` on the result
    if they need a canonical form.

    Parameters
    ----------
    schema : TableSchema
    left_batch : ZSetBatch
    right_batch : ZSetBatch

    Returns
    -------
    ZSetBatch
    """
    output_batch = make_empty_batch(schema)
    output_batch.extend(left_batch)
    output_batch.extend(right_batch)
    return output_batch
