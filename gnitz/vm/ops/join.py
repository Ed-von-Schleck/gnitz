# gnitz/vm/ops/join.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi

from gnitz.core import types
from gnitz.core.values import make_payload_row
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.vm import runtime


@jit.unroll_safe
def _copy_payload_cols(schema, accessor, output_row):
    """
    Typed-copy of all non-PK columns from an accessor to a PayloadRow.
    Uses schema-based dispatch to eliminate TaggedValue overhead.
    """
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
            
        col_type = schema.columns[i].field_type
        code = col_type.code

        if code == types.TYPE_STRING.code:
            # RowAccessor.get_str_struct returns (length, prefix, struct_p, heap_p, s)
            # We only need the Python string 's' to append to a PayloadRow.
            res = accessor.get_str_struct(i)
            output_row.append_string(res[4])

        elif code == types.TYPE_F64.code or code == types.TYPE_F32.code:
            output_row.append_float(accessor.get_float(i))

        elif code == types.TYPE_U128.code:
            val = accessor.get_u128(i)
            # Split into low/high words for the PayloadRow storage identity
            lo = r_uint64(val)
            hi = r_uint64(val >> 64)
            output_row.append_u128(lo, hi)

        else:
            # Integer primitives (i8...u64)
            # RowAccessor.get_int returns r_uint64; PayloadRow expects r_int64 bitcast.
            raw = accessor.get_int(i)
            output_row.append_int(rffi.cast(rffi.LONGLONG, raw))


def op_join_delta_trace(reg_delta, reg_trace, reg_out):
    """
    Implements Delta-Trace Join (Index-Nested-Loop Join).
    Formula: ΔA ⋈ B
    """
    assert isinstance(reg_delta, runtime.DeltaRegister)
    assert isinstance(reg_trace, runtime.TraceRegister)
    assert isinstance(reg_out, runtime.DeltaRegister)

    delta_batch = reg_delta.batch
    trace_cursor = reg_trace.cursor
    out_batch = reg_out.batch
    out_batch.clear()

    # Schemas for the materialization barrier
    d_schema = reg_delta.vm_schema.table_schema
    t_schema = reg_trace.vm_schema.table_schema
    out_schema = out_batch._schema

    # Temporary accessors
    d_accessor = PayloadRowAccessor(d_schema)

    count = delta_batch.length()
    for i in range(count):
        key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)
        
        # Ghost Property: skip if delta weight is 0
        if w_delta == 0:
            continue

        # Seek the historical trace
        trace_cursor.seek(key)
        
        # Match loop (Handles multiset semantics: one key may have N payloads)
        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break
                
            w_trace = trace_cursor.weight()
            w_out = w_delta * w_trace
            
            # Algebraic Annihilation: skip if net product is 0
            if w_out != 0:
                d_accessor.set_row(delta_batch.get_row(i))
                t_accessor = trace_cursor.get_accessor()
                
                # Construct concatenated PayloadRow
                out_row = make_payload_row(out_schema)
                _copy_payload_cols(d_schema, d_accessor, out_row)
                _copy_payload_cols(t_schema, t_accessor, out_row)
                
                out_batch.append(key, w_out, out_row)
            
            trace_cursor.advance()


def op_join_delta_delta(reg_a, reg_b, reg_out):
    """
    Implements Delta-Delta Join (Sort-Merge Join).
    Formula: ΔA ⋈ ΔB
    """
    assert isinstance(reg_a, runtime.DeltaRegister)
    assert isinstance(reg_b, runtime.DeltaRegister)
    assert isinstance(reg_out, runtime.DeltaRegister)

    batch_a = reg_a.batch
    batch_b = reg_b.batch
    out_batch = reg_out.batch
    out_batch.clear()

    # DBSP Join requires sorted inputs for efficient merge
    batch_a.sort()
    batch_b.sort()

    schema_a = reg_a.vm_schema.table_schema
    schema_b = reg_b.vm_schema.table_schema
    out_schema = out_batch._schema

    acc_a = PayloadRowAccessor(schema_a)
    acc_b = PayloadRowAccessor(schema_b)

    idx_a = 0
    idx_b = 0
    n_a = batch_a.length()
    n_b = batch_b.length()

    while idx_a < n_a and idx_b < n_b:
        key_a = batch_a.get_pk(idx_a)
        key_b = batch_b.get_pk(idx_b)

        if key_a < key_b:
            idx_a += 1
        elif key_b < key_a:
            idx_b += 1
        else:
            # Key Match: compute Cartesian product of blocks with this key
            match_key = key_a
            
            # Identify block boundaries for Multiset support
            start_a = idx_a
            while idx_a < n_a and batch_a.get_pk(idx_a) == match_key:
                idx_a += 1
            
            start_b = idx_b
            while idx_b < n_b and batch_b.get_pk(idx_b) == match_key:
                idx_b += 1

            # Nested-loop over matching blocks
            for i in range(start_a, idx_a):
                wa = batch_a.get_weight(i)
                if wa == 0: continue
                
                for j in range(start_b, idx_b):
                    wb = batch_b.get_weight(j)
                    w_out = wa * wb
                    
                    if w_out != 0:
                        acc_a.set_row(batch_a.get_row(i))
                        acc_b.set_row(batch_b.get_row(j))
                        
                        out_row = make_payload_row(out_schema)
                        _copy_payload_cols(schema_a, acc_a, out_row)
                        _copy_payload_cols(schema_b, acc_b, out_row)
                        
                        out_batch.append(match_key, w_out, out_row)
