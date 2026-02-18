# gnitz/vm/ops.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, row_logic, strings
from gnitz.core.row_logic import make_payload_row

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


@jit.unroll_safe
def materialize_row(accessor, schema):
    """
    Extracts a full row from a RowAccessor and boxes it into a list of TaggedValues.
    """
    n_payload_cols = len(schema.columns) - 1
    result = make_payload_row(n_payload_cols)

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_def = schema.columns[i]
        type_code = col_def.field_type.code

        if (
            type_code == types.TYPE_I64.code
            or type_code == types.TYPE_U64.code
            or type_code == types.TYPE_I32.code
            or type_code == types.TYPE_U32.code
            or type_code == types.TYPE_I16.code
            or type_code == types.TYPE_U16.code
            or type_code == types.TYPE_I8.code
            or type_code == types.TYPE_U8.code
        ):
            val = accessor.get_int(i)
            result.append(values.TaggedValue.make_int(val))

        elif type_code == types.TYPE_F64.code or type_code == types.TYPE_F32.code:
            val = accessor.get_float(i)
            result.append(values.TaggedValue.make_float(val))

        elif type_code == types.TYPE_STRING.code:
            meta = accessor.get_str_struct(i)
            s_val = meta[4]
            if s_val is None:
                s_val = strings.unpack_string(meta[2], meta[3])
            result.append(values.TaggedValue.make_string(s_val))

        elif type_code == types.TYPE_U128.code:
            val_u128 = accessor.get_u128(i)
            lo = r_uint64(val_u128)
            hi = r_uint64(val_u128 >> 64)
            result.append(values.TaggedValue.make_u128(lo, hi))

        else:
            result.append(values.TaggedValue.make_null())

    return result


def _concat_payloads(left, right):
    """
    Concatenates two payload rows into a fresh resizable list.
    """
    n = len(left) + len(right)
    result = make_payload_row(n)
    for k in range(len(left)):
        result.append(left[k])
    for k in range(len(right)):
        result.append(right[k])
    return result


# -----------------------------------------------------------------------------
# Linear Operators
# -----------------------------------------------------------------------------


def op_filter(reg_in, reg_out, func):
    """Out = { (k, v, w) | (k, v, w) in In AND func(v) }"""
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    count = input_batch.row_count()
    accessor = input_batch.left_acc

    for i in range(count):
        accessor.set_row(input_batch, i)
        if func.evaluate_predicate(accessor):
            output_batch.append(
                input_batch.get_key(i),
                input_batch.get_weight(i),
                input_batch.get_payload(i),
            )


def op_map(reg_in, reg_out, func):
    """Out = { (k, func(v), w) | (k, v, w) in In }"""
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    count = input_batch.row_count()
    accessor = input_batch.left_acc
    n_out_cols = len(reg_out.batch.schema.columns) - 1

    for i in range(count):
        accessor.set_row(input_batch, i)
        new_payload = make_payload_row(n_out_cols)
        func.evaluate_map(accessor, new_payload)

        output_batch.append(
            input_batch.get_key(i),
            input_batch.get_weight(i),
            new_payload,
        )


def op_negate(reg_in, reg_out):
    """Out = { (k, v, -w) | (k, v, w) in In }"""
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    count = input_batch.row_count()
    for i in range(count):
        output_batch.append(
            input_batch.get_key(i),
            -input_batch.get_weight(i),
            input_batch.get_payload(i),
        )


def op_union(reg_in_a, reg_in_b, reg_out):
    """Out = InA + InB"""
    output_batch = reg_out.batch
    output_batch.clear()

    batch_a = reg_in_a.batch
    count_a = batch_a.row_count()
    for i in range(count_a):
        output_batch.append(
            batch_a.get_key(i),
            batch_a.get_weight(i),
            batch_a.get_payload(i),
        )

    if reg_in_b is not None:
        batch_b = reg_in_b.batch
        count_b = batch_b.row_count()
        for i in range(count_b):
            output_batch.append(
                batch_b.get_key(i),
                batch_b.get_weight(i),
                batch_b.get_payload(i),
            )


def op_delay(reg_in, reg_out):
    """Moves content from reg_in to reg_out."""
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    count = input_batch.row_count()
    for i in range(count):
        output_batch.append(
            input_batch.get_key(i),
            input_batch.get_weight(i),
            input_batch.get_payload(i),
        )


# -----------------------------------------------------------------------------
# Bilinear Operators (Joins)
# -----------------------------------------------------------------------------


def op_join_delta_trace(reg_delta, reg_trace, reg_out):
    """Index-Nested-Loop Join (INLJ): DeltaBatch (x) TraceCursor"""
    delta_batch = reg_delta.batch
    trace_cursor = reg_trace.cursor
    output_batch = reg_out.batch
    output_batch.clear()

    delta_batch.sort()
    count = delta_batch.row_count()
    if count == 0:
        return

    for i in range(count):
        d_key = delta_batch.get_key(i)
        d_weight = delta_batch.get_weight(i)

        if d_weight == 0:
            continue

        trace_cursor.seek(d_key)
        while trace_cursor.is_valid():
            t_key = trace_cursor.key()
            if t_key != d_key:
                break
            t_weight = trace_cursor.weight()
            final_weight = d_weight * t_weight

            if final_weight != 0:
                accessor = trace_cursor.get_accessor()
                t_payload = materialize_row(accessor, reg_trace.vm_schema.table_schema)
                d_payload = delta_batch.get_payload(i)
                final_payload = _concat_payloads(d_payload, t_payload)
                output_batch.append(d_key, final_weight, final_payload)

            trace_cursor.advance()


def op_join_delta_delta(reg_a, reg_b, reg_out):
    """Sort-Merge Join: DeltaBatchA (x) DeltaBatchB"""
    batch_a = reg_a.batch
    batch_b = reg_b.batch
    output_batch = reg_out.batch
    output_batch.clear()

    batch_a.sort()
    batch_b.sort()

    count_a = batch_a.row_count()
    count_b = batch_b.row_count()

    idx_a = 0
    idx_b = 0

    while idx_a < count_a and idx_b < count_b:
        key_a = batch_a.get_key(idx_a)
        key_b = batch_b.get_key(idx_b)

        if key_a < key_b:
            idx_a += 1
        elif key_b < key_a:
            idx_b += 1
        else:
            match_key = key_a
            start_a = idx_a
            while idx_a < count_a and batch_a.get_key(idx_a) == match_key:
                idx_a += 1
            end_a = idx_a

            start_b = idx_b
            while idx_b < count_b and batch_b.get_key(idx_b) == match_key:
                idx_b += 1
            end_b = idx_b

            for i in range(start_a, end_a):
                w_a = batch_a.get_weight(i)
                if w_a == 0:
                    continue

                for j in range(start_b, end_b):
                    w_b = batch_b.get_weight(j)
                    final_weight = w_a * w_b

                    if final_weight != 0:
                        final_payload = _concat_payloads(
                            batch_a.get_payload(i), batch_b.get_payload(j)
                        )
                        output_batch.append(match_key, final_weight, final_payload)


# -----------------------------------------------------------------------------
# Integral / Sink Operators
# -----------------------------------------------------------------------------


def op_integrate(reg_in, target_engine):
    """Sinks a batch into a persistent storage engine."""
    input_batch = reg_in.batch
    count = input_batch.row_count()

    for i in range(count):
        weight = input_batch.get_weight(i)
        if weight == 0:
            continue

        key = input_batch.get_key(i)
        payload = input_batch.get_payload(i)
        target_engine.ingest(key, weight, payload)


def op_distinct(reg_in, reg_out):
    """Normalizes weights for set semantics."""
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    input_batch.sort()
    input_batch.consolidate()

    count = input_batch.row_count()
    for i in range(count):
        w = input_batch.get_weight(i)
        if w > 0:
            output_batch.append(
                input_batch.get_key(i),
                r_int64(1),
                input_batch.get_payload(i),
            )
