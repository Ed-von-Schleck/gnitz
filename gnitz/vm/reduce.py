# gnitz/vm/reduce.py
#
# REDUCE operator and aggregate function implementations for the DBSP VM.
#
# Migration note (PayloadRow — REDUCE accumulators)
# --------------------------------------------------
# The critical change in this file is the elimination of ``TaggedValue``
# scalar accumulators.  Every aggregate function that previously held a
# ``TaggedValue`` as its running total now holds a concrete typed field
# instead.
#
# Pattern change (from plan §11):
#
#   Before:
#       class SumAggregateFunction(AggregateFunction):
#           def __init__(self):
#               self._acc = TaggedValue.make_i64(r_int64(0))
#           def step(self, row_accessor, weight):
#               val = row_accessor.get_int(self._col_idx)
#               self._acc = TaggedValue(TAG_INT,
#                                       self._acc.i64 + r_int64(intmask(val)),
#                                       r_uint64(0), 0.0, "")
#           def emit(self, output_row):
#               output_row.append(self._acc)
#
#   After:
#       class SumIntAggregate(AggregateFunction):
#           def __init__(self, col_idx):
#               self._acc = r_int64(0)   # concrete field, no heap object
#           def step(self, row_accessor, weight):
#               val = r_int64(intmask(row_accessor.get_int(self._col_idx)))
#               self._acc = self._acc + val * weight
#           def emit(self, output_row):
#               output_row.append_int(self._acc)
#
# The heap object formerly needed for ``TaggedValue`` (16-byte GC header +
# 40 bytes of fields = 56 bytes per accumulator reset) is eliminated.  A
# single ``r_int64`` field in the aggregate function object costs 8 bytes and
# zero heap allocations.
#
# emit() signature change
# -----------------------
# Before: ``emit(self, output_row_list)`` where output_row_list was
#         ``List[TaggedValue]`` receiving a new ``TaggedValue``.
# After:  ``emit(self, output_row)`` where output_row is a ``PayloadRow``
#         receiving a typed value via the appropriate ``append_*`` call.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask, MININT
from rpython.rlib.longlong2float import longlong2float

from gnitz.core import types
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.core.values import make_payload_row, PayloadRow
from gnitz.vm.batch import ZSetBatch, make_empty_batch, get_null_row


# ---------------------------------------------------------------------------
# AggregateFunction — abstract base for stateful aggregators
# ---------------------------------------------------------------------------

class AggregateFunction(object):
    """
    Abstract base for incremental aggregate functions used by
    ``GroupByReduceOperator``.

    Lifecycle per group
    -------------------
    1. ``reset()`` — called at the start of each new group.
    2. ``step(row_accessor, weight)`` — called once per record in the group.
    3. ``emit(output_row)`` — called once at the end of the group to append
       the aggregate result to the output ``PayloadRow``.

    The aggregate function object is created once and reused across all groups
    in the batch (via reset/step/emit cycles).  It must carry no group-specific
    state across calls to ``reset()``.
    """

    def reset(self):
        """Reset the accumulator to the identity element for this aggregate."""
        raise NotImplementedError

    def step(self, row_accessor, weight):
        """
        Incorporate one record into the running accumulator.

        Parameters
        ----------
        row_accessor : RowAccessor
            Positioned on the current input record.
        weight : r_int64
            Algebraic weight of this record.  Negative weights represent
            deletions and must be handled correctly by the step function.
        """
        raise NotImplementedError

    def emit(self, output_row):
        """
        Append the aggregate result as a new column in ``output_row``.

        Called exactly once per group, after all ``step`` calls for that group.
        The function appends exactly one value to ``output_row`` via the
        appropriate ``append_*`` method.

        Before: appended a ``TaggedValue`` object to a ``List[TaggedValue]``.
        After:  calls ``output_row.append_int``, ``append_float``, etc.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# COUNT(*)
# ---------------------------------------------------------------------------

class CountAggregate(AggregateFunction):
    """
    COUNT(*): counts the sum of weights over all records in the group.

    The accumulator is a plain ``r_int64``.  In the DBSP model, counting
    sums the algebraic weights (so a record with weight +2 contributes 2 to
    the count, and a record with weight -1 subtracts 1).

    Before: ``self._acc = TaggedValue.make_i64(r_int64(0))``
    After:  ``self._acc = r_int64(0)``
    """

    def __init__(self):
        self._acc = r_int64(0)    # concrete r_int64 field, no heap object

    def reset(self):
        self._acc = r_int64(0)

    def step(self, row_accessor, weight):
        self._acc = self._acc + weight

    def emit(self, output_row):
        output_row.append_int(self._acc)


# ---------------------------------------------------------------------------
# SUM over integer columns
# ---------------------------------------------------------------------------

class SumIntAggregate(AggregateFunction):
    """
    SUM(col) for integer column types (i8/i16/i32/i64/u8/u16/u32/u64).

    The accumulator is a plain ``r_int64``.

    For unsigned input types, the unsigned bit pattern is reinterpreted as
    signed via ``intmask`` before accumulation.  If the underlying column is
    logically unsigned, callers should use ``SumUintAggregate`` instead.

    Before: ``self._acc = TaggedValue.make_i64(r_int64(0))``
    After:  ``self._acc = r_int64(0)``
    """

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        """
        Parameters
        ----------
        col_idx : int
            Schema column index of the column to sum.
        """
        self._col_idx = col_idx
        self._acc = r_int64(0)

    def reset(self):
        self._acc = r_int64(0)

    def step(self, row_accessor, weight):
        raw = row_accessor.get_int(self._col_idx)       # r_uint64
        val = r_int64(intmask(raw))                     # reinterpret as signed
        self._acc = self._acc + val * weight

    def emit(self, output_row):
        output_row.append_int(self._acc)


class SumUintAggregate(AggregateFunction):
    """
    SUM(col) for unsigned integer columns where the sum should also be
    unsigned (accumulates in ``r_uint64``).

    Before: ``self._acc = TaggedValue.make_i64(r_int64(0))``
    After:  ``self._acc = r_uint64(0)``

    Note: weight is ``r_int64`` (can be negative), so the accumulation step
    casts carefully.
    """

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        self._col_idx = col_idx
        self._acc = r_uint64(0)

    def reset(self):
        self._acc = r_uint64(0)

    def step(self, row_accessor, weight):
        val = row_accessor.get_int(self._col_idx)   # r_uint64
        if weight > r_int64(0):
            self._acc = self._acc + val * r_uint64(intmask(weight))
        else:
            self._acc = self._acc - val * r_uint64(intmask(-intmask(weight)))

    def emit(self, output_row):
        # Store unsigned value as r_int64 via intmask (lossless bit reinterpret).
        output_row.append_int(r_int64(intmask(self._acc)))


# ---------------------------------------------------------------------------
# SUM over float columns
# ---------------------------------------------------------------------------

class SumFloatAggregate(AggregateFunction):
    """
    SUM(col) for TYPE_F64 columns.

    The accumulator is a plain Python ``float``.

    Before: ``self._acc = TaggedValue.make_float(0.0)``
    After:  ``self._acc = 0.0``
    """

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        self._col_idx = col_idx
        self._acc = 0.0   # concrete float field, no heap object

    def reset(self):
        self._acc = 0.0

    def step(self, row_accessor, weight):
        val = row_accessor.get_float(self._col_idx)
        self._acc = self._acc + val * float(intmask(weight))

    def emit(self, output_row):
        output_row.append_float(self._acc)


# ---------------------------------------------------------------------------
# MIN / MAX over integer columns
# ---------------------------------------------------------------------------

_INT64_MAX = r_int64(9223372036854775807)    # 2^63 - 1
_INT64_MIN = r_int64(-9223372036854775808)   # -2^63  (rpython: intmask of 0x8000...0)


class MinIntAggregate(AggregateFunction):
    """
    MIN(col) for signed integer column types.

    Accumulator identity: largest possible signed integer (no record seen yet).

    Before: ``self._acc = TaggedValue.make_i64(INT64_MAX)``
    After:  ``self._acc = _INT64_MAX``
    """

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        self._col_idx = col_idx
        self._acc = _INT64_MAX

    def reset(self):
        self._acc = _INT64_MAX

    def step(self, row_accessor, weight):
        # In DBSP, MIN with negative weights is semantically complex.
        # For the standard incremental MIN over insertions (weight >= 0),
        # we take the minimum of the value repeated |weight| times, which
        # is just the value itself if weight > 0.
        if weight > r_int64(0):
            raw = row_accessor.get_int(self._col_idx)
            val = r_int64(intmask(raw))
            if val < self._acc:
                self._acc = val

    def emit(self, output_row):
        output_row.append_int(self._acc)


class MaxIntAggregate(AggregateFunction):
    """
    MAX(col) for signed integer column types.

    Accumulator identity: smallest possible signed integer.

    Before: ``self._acc = TaggedValue.make_i64(INT64_MIN)``
    After:  ``self._acc = _INT64_MIN``
    """

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        self._col_idx = col_idx
        self._acc = _INT64_MIN

    def reset(self):
        self._acc = _INT64_MIN

    def step(self, row_accessor, weight):
        if weight > r_int64(0):
            raw = row_accessor.get_int(self._col_idx)
            val = r_int64(intmask(raw))
            if val > self._acc:
                self._acc = val

    def emit(self, output_row):
        output_row.append_int(self._acc)


class MinFloatAggregate(AggregateFunction):
    """MIN(col) for TYPE_F64 columns."""

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        self._col_idx = col_idx
        self._acc = 1e308 * 1e308   # +inf identity

    def reset(self):
        self._acc = 1e308 * 1e308

    def step(self, row_accessor, weight):
        if weight > r_int64(0):
            val = row_accessor.get_float(self._col_idx)
            if val < self._acc:
                self._acc = val

    def emit(self, output_row):
        output_row.append_float(self._acc)


class MaxFloatAggregate(AggregateFunction):
    """MAX(col) for TYPE_F64 columns."""

    _immutable_fields_ = ['_col_idx']

    def __init__(self, col_idx):
        self._col_idx = col_idx
        self._acc = -(1e308 * 1e308)   # -inf identity

    def reset(self):
        self._acc = -(1e308 * 1e308)

    def step(self, row_accessor, weight):
        if weight > r_int64(0):
            val = row_accessor.get_float(self._col_idx)
            if val > self._acc:
                self._acc = val

    def emit(self, output_row):
        output_row.append_float(self._acc)


# ---------------------------------------------------------------------------
# GroupByReduceOperator
# ---------------------------------------------------------------------------

class GroupByReduceOperator(object):
    """
    DBSP REDUCE operator: groups input records by a key (the PK in the
    simplest case) and applies one or more aggregate functions to each group.

    Assumptions
    -----------
    1. The input batch is sorted by PK (the group-by key) before this
       operator is called.  Call ``input_batch.sort()`` if needed.
    2. The output schema has:
         - The same PK column as the input (the group key is re-emitted as PK).
         - One output column per aggregate function in ``agg_fns``, in order.

    Aggregate functions
    -------------------
    Each entry in ``agg_fns`` is an ``AggregateFunction`` instance.  The
    operator calls ``reset()``, ``step()`` (once per record in the group),
    and ``emit()`` (once per group, to append the aggregate result to the
    output ``PayloadRow``).

    The output ``PayloadRow`` is constructed fresh for each group via
    ``make_payload_row(output_schema)``.  The aggregate functions fill it
    in by calling ``output_row.append_*()`` from their ``emit()`` methods.

    TaggedValue elimination
    -----------------------
    Before: each ``AggregateFunction.emit`` called
            ``output_row.append(TaggedValue.make_i64(self._acc.i64))``
    After:  each ``AggregateFunction.emit`` calls
            ``output_row.append_int(self._acc)``
    where ``self._acc`` is a concrete ``r_int64`` (or ``float``) field.
    """

    _immutable_fields_ = ['_input_schema', '_output_schema']

    def __init__(self, input_schema, output_schema, agg_fns):
        """
        Parameters
        ----------
        input_schema : TableSchema
        output_schema : TableSchema
            Must have the same PK as input_schema, plus one non-PK column per
            aggregate function.
        agg_fns : List[AggregateFunction]
            One aggregate function per non-PK output column, in column order.
        """
        self._input_schema  = input_schema
        self._output_schema = output_schema
        self._agg_fns       = agg_fns   # List[AggregateFunction]
        self._accessor      = PayloadRowAccessor(input_schema)

    def apply(self, input_batch):
        """
        Apply the GROUP BY + aggregation to a sorted ``ZSetBatch``.

        Parameters
        ----------
        input_batch : ZSetBatch
            Must be sorted by PK.

        Returns
        -------
        ZSetBatch
            One output record per distinct PK in the input, carrying the
            aggregate results.  Output weight is always +1 (the group exists).
        """
        n = input_batch.length()
        if n == 0:
            return make_empty_batch(self._output_schema)

        output_schema = self._output_schema
        output_batch  = make_empty_batch(output_schema)
        accessor      = self._accessor
        agg_fns       = self._agg_fns

        # Reset all aggregates before the first group.
        for fn in agg_fns:
            fn.reset()

        cur_pk = input_batch.get_pk(0)

        for i in range(n):
            pk     = input_batch.get_pk(i)
            weight = input_batch.get_weight(i)
            row    = input_batch.get_row(i)

            if pk != cur_pk:
                # Group boundary: emit the previous group.
                output_row = make_payload_row(output_schema)
                for fn in agg_fns:
                    fn.emit(output_row)
                # The group's output weight is +1 (the group is present).
                output_batch.append(cur_pk, r_int64(1), output_row)

                # Start the new group.
                cur_pk = pk
                for fn in agg_fns:
                    fn.reset()

            # Step each aggregate with the current record.
            accessor.set_row(row)
            for fn in agg_fns:
                fn.step(accessor, weight)

        # Emit the final group.
        output_row = make_payload_row(output_schema)
        for fn in agg_fns:
            fn.emit(output_row)
        output_batch.append(cur_pk, r_int64(1), output_row)

        return output_batch
