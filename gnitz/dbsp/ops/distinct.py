# gnitz/dbsp/ops/distinct.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, intmask
from gnitz.core.batch import ConsolidatedScope, BatchWriter, pk_eq

"""
Distinct Operator for the DBSP algebra.

Converts multiset semantics to set semantics: a record's output weight is 1
if its net accumulated weight is positive, -1 if negative, and 0 otherwise.
In practice GnitzDB only accumulates non-negative weights from the ingestion
layer, so the -1 case does not arise for base tables — but it can arise in
intermediate circuit nodes, and the implementation handles it correctly.

The algebraic identity being implemented is:

    δ_out = set_step(I(δ_in)) - set_step(z^{-1}(I(δ_in)))
          = set_step(history + δ_in) - set_step(history)

where set_step(w) = sign(w) clamped to {-1, 0, 1}.

State (the running integral I(δ_in)) is maintained by the caller in
history_table, which is an AbstractTable acting as the operator's trace.
"""


def op_distinct(delta_batch, hist_cursor, hist_table, out_writer):
    """
    DBSP Distinct: converts multiset deltas into set-membership deltas.

    delta_batch: ArenaZSetBatch  — consolidated input delta for this tick
    hist_cursor: AbstractCursor  — seekable cursor over the persistent history
                                   (I(δ_in) snapped at the start of this tick)
    hist_table:  ZSetStore       — persistent trace holding the running sum
                                   I(δ_in) accumulated over all past ticks
    out_writer:  BatchWriter     — strictly write-only destination

    The delta batch is processed in PK order; hist_cursor is advanced
    monotonically via seek(), giving O((|Δ|+|H|)·log K) amortised cost
    instead of O(|Δ|·K) independent per-key probes.
    """
    with ConsolidatedScope(delta_batch) as b:
        n = b.length()
        if n == 0:
            return

        # Pass 1: pure weight logic — only touches structural columns (pk, weight).
        # No payload access.  Collect emitting row indices and their output weights.
        emit_indices = newlist_hint(n)
        emit_weights = newlist_hint(n)

        for i in range(n):
            key_lo, key_hi = b.get_pk_lo(i), b.get_pk_hi(i)
            w_delta = b.get_weight(i)

            hist_cursor.seek(key_lo, key_hi)
            w_old = r_int64(0)
            if hist_cursor.is_valid() and pk_eq(hist_cursor.key_lo(), hist_cursor.key_hi(), key_lo, key_hi):
                w_old = hist_cursor.weight()

            # DBSP distinct converts a multiset to a set.
            # An element is in the set (weight 1) if its accumulated weight is > 0, else 0.
            # In intermediate nodes, weights can be negative, so we use sign logic.
            s_old = 0
            if w_old > r_int64(0):
                s_old = 1
            elif w_old < r_int64(0):
                s_old = -1

            # Algebraic summation with RPython machine-word truncation
            w_new = r_int64(intmask(w_old + w_delta))

            s_new = 0
            if w_new > r_int64(0):
                s_new = 1
            elif w_new < r_int64(0):
                s_new = -1

            out_w = s_new - s_old
            if out_w != 0:
                emit_indices.append(i)
                emit_weights.append(r_int64(out_w))

        # Pass 2: column-major payload copy for the emitting rows.
        # One alloc_n per column; inner loop stays within one source column buffer.
        # The output is a PK-sorted, duplicate-free subset of a consolidated input,
        # so it is itself consolidated.
        out_writer.copy_rows_indexed(b, emit_indices, emit_weights)
        out_writer.mark_consolidated(True)

        # Update the history with the consolidated delta before the scope expires.
        # This reflects hist_table = I(δ_in)
        hist_table.ingest_batch(b)
