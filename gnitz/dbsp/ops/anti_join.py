# gnitz/dbsp/ops/anti_join.py

from rpython.rlib.rarithmetic import r_int64, intmask

from gnitz.core.batch import ConsolidatedScope

"""
Anti-Join and Semi-Join Operators for the DBSP algebra.

Anti-join: antijoin(A, B) = A - semijoin(A, distinct(B))
    Emits rows from A whose join key has NO match in B.

Semi-join: semijoin(A, B)
    Emits rows from A whose join key DOES have a match in B.

Both operators output using the LEFT schema only (no column merge,
no CompositeAccessor needed).

The incremental expansion of antijoin uses these operators:
    Δ(antijoin(A, B)) = anti_dt(ΔA, I(D))
                       - semi_dt(ΔD, I(A))   [output from I(A)]
                       - semi_dd(ΔA, ΔD)
where D = distinct(B).
"""


def op_anti_join_delta_trace(delta_batch, trace_cursor, out_writer, left_schema):
    """
    Delta-Trace Anti-Join: emit ΔA rows with NO positive-weight match in I(B).

    delta_batch:   ArenaZSetBatch  — the in-flight delta (ΔA)
    trace_cursor:  AbstractCursor  — seekable cursor over the persistent trace (I(B))
    out_writer:    BatchWriter     — strictly write-only destination
    left_schema:   TableSchema     — schema of delta_batch (= output schema)
    """
    count = delta_batch.length()
    for i in range(count):
        w = delta_batch.get_weight(i)
        if w == r_int64(0):
            continue

        key = delta_batch.get_pk(i)
        trace_cursor.seek(key)

        has_match = False
        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break
            if trace_cursor.weight() > r_int64(0):
                has_match = True
                break
            trace_cursor.advance()

        if not has_match:
            out_writer.direct_append_row(delta_batch, i, w)
    out_writer.mark_sorted(delta_batch._sorted)


def op_anti_join_delta_delta(batch_a, batch_b, out_writer, left_schema):
    """
    Delta-Delta Anti-Join (Sort-Merge): emit ΔA rows whose keys have
    NO positive-weight match in ΔB.

    batch_a:    ArenaZSetBatch  — left delta (ΔA)
    batch_b:    ArenaZSetBatch  — right delta (ΔB)
    out_writer: BatchWriter     — strictly write-only destination
    left_schema: TableSchema    — schema of batch_a (= output schema)
    """
    with ConsolidatedScope(batch_a) as b_a:
        with ConsolidatedScope(batch_b) as b_b:
            idx_a = 0
            idx_b = 0
            n_a = b_a.length()
            n_b = b_b.length()

            while idx_a < n_a:
                key_a = b_a.get_pk(idx_a)

                # Advance b pointer to first key >= key_a
                while idx_b < n_b and b_b.get_pk(idx_b) < key_a:
                    idx_b += 1

                # Check if any b record at key_a has positive weight
                has_match = False
                if idx_b < n_b and b_b.get_pk(idx_b) == key_a:
                    scan_b = idx_b
                    while scan_b < n_b and b_b.get_pk(scan_b) == key_a:
                        if b_b.get_weight(scan_b) > r_int64(0):
                            has_match = True
                            break
                        scan_b += 1

                # Find end of key group in b_a
                start_a = idx_a
                while idx_a < n_a and b_a.get_pk(idx_a) == key_a:
                    idx_a += 1

                if not has_match:
                    out_writer.append_batch(b_a, start_a, idx_a)
            out_writer.mark_consolidated(True)


def op_semi_join_delta_trace(delta_batch, trace_cursor, out_writer, left_schema):
    """
    Delta-Trace Semi-Join: emit ΔA rows whose key HAS a positive-weight
    match in I(B). Weight is preserved from the delta row.

    This is the complement of op_anti_join_delta_trace.

    delta_batch:   ArenaZSetBatch  — the in-flight delta
    trace_cursor:  AbstractCursor  — seekable cursor over the persistent trace
    out_writer:    BatchWriter     — strictly write-only destination
    left_schema:   TableSchema     — schema of delta_batch (= output schema)
    """
    count = delta_batch.length()
    for i in range(count):
        w = delta_batch.get_weight(i)
        if w == r_int64(0):
            continue

        key = delta_batch.get_pk(i)
        trace_cursor.seek(key)

        has_match = False
        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break
            if trace_cursor.weight() > r_int64(0):
                has_match = True
                break
            trace_cursor.advance()

        if has_match:
            out_writer.direct_append_row(delta_batch, i, w)
    out_writer.mark_sorted(delta_batch._sorted)


def op_semi_join_delta_delta(batch_a, batch_b, out_writer, left_schema):
    """
    Delta-Delta Semi-Join (Sort-Merge): emit ΔA rows whose keys DO have
    a positive-weight match in ΔB. Weight is preserved from batch_a.

    This is the complement of op_anti_join_delta_delta.

    batch_a:    ArenaZSetBatch  — left delta (ΔA)
    batch_b:    ArenaZSetBatch  — right delta (ΔB)
    out_writer: BatchWriter     — strictly write-only destination
    left_schema: TableSchema    — schema of batch_a (= output schema)
    """
    with ConsolidatedScope(batch_a) as b_a:
        with ConsolidatedScope(batch_b) as b_b:
            idx_a = 0
            idx_b = 0
            n_a = b_a.length()
            n_b = b_b.length()

            while idx_a < n_a:
                key_a = b_a.get_pk(idx_a)

                # Advance b pointer to first key >= key_a
                while idx_b < n_b and b_b.get_pk(idx_b) < key_a:
                    idx_b += 1

                # Check if any b record at key_a has positive weight
                has_match = False
                if idx_b < n_b and b_b.get_pk(idx_b) == key_a:
                    scan_b = idx_b
                    while scan_b < n_b and b_b.get_pk(scan_b) == key_a:
                        if b_b.get_weight(scan_b) > r_int64(0):
                            has_match = True
                            break
                        scan_b += 1

                # Find end of key group in b_a
                start_a = idx_a
                while idx_a < n_a and b_a.get_pk(idx_a) == key_a:
                    idx_a += 1

                if has_match:
                    out_writer.append_batch(b_a, start_a, idx_a)
            out_writer.mark_consolidated(True)
