# gnitz/vm/ops/join.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64
from gnitz.core.comparator import RowAccessor
from gnitz.vm import runtime

class CompositeAccessor(RowAccessor):
    """
    Virtual accessor that concatenates two RowAccessors.
    Used to implement zero-allocation Joins.
    
    Mapping logic follows TableSchema.merge_schemas_for_join:
    - Col 0: PK (from left)
    - Col 1..N: Left Payloads
    - Col N+1..M: Right Payloads
    """
    _immutable_fields_ = [
        'left_schema', 'right_schema', 
        'mapping_is_left[*]', 'mapping_idx[*]'
    ]

    def __init__(self, left_schema, right_schema):
        self.left_schema = left_schema
        self.right_schema = right_schema
        self.left_acc = None
        self.right_acc = None

        # Build static index mapping
        len_l = len(left_schema.columns)
        len_r = len(right_schema.columns)
        total = 1 + (len_l - 1) + (len_r - 1)
        
        mapping_is_left = [False] * total
        mapping_idx = [0] * total

        # Index 0 is the PK (authority comes from left)
        mapping_is_left[0] = True
        mapping_idx[0] = left_schema.pk_index

        curr = 1
        # Add Left non-PK columns
        for i in range(len_l):
            if i != left_schema.pk_index:
                mapping_is_left[curr] = True
                mapping_idx[curr] = i
                curr += 1
        
        # Add Right non-PK columns
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
    def get_int_signed(self, col_idx):
        if self.mapping_is_left[col_idx]:
            return self.left_acc.get_int_signed(self.mapping_idx[col_idx])
        return self.right_acc.get_int_signed(self.mapping_idx[col_idx])


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

    d_schema = reg_delta.vm_schema.table_schema
    t_schema = reg_trace.vm_schema.table_schema
    
    # Pre-allocate the virtual composite accessor
    composite_acc = CompositeAccessor(d_schema, t_schema)

    count = delta_batch.length()
    for i in range(count):
        key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)

        if w_delta == 0:
            continue

        trace_cursor.seek(key)

        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break

            w_trace = trace_cursor.weight()
            w_out = w_delta * w_trace

            if w_out != 0:
                # Link raw source accessors to the virtual composite
                composite_acc.set_accessors(
                    delta_batch.get_accessor(i),
                    trace_cursor.get_accessor()
                )

                # Zero-copy: serialize directly into the output batch arena
                out_batch.append_from_accessor(key, w_out, composite_acc)

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

    batch_a.sort()
    batch_b.sort()

    schema_a = reg_a.vm_schema.table_schema
    schema_b = reg_b.vm_schema.table_schema
    
    composite_acc = CompositeAccessor(schema_a, schema_b)

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
            match_key = key_a
            start_a = idx_a
            while idx_a < n_a and batch_a.get_pk(idx_a) == match_key:
                idx_a += 1

            start_b = idx_b
            while idx_b < n_b and batch_b.get_pk(idx_b) == match_key:
                idx_b += 1

            for i in range(start_a, idx_a):
                wa = batch_a.get_weight(i)
                if wa == 0:
                    continue

                for j in range(start_b, idx_b):
                    wb = batch_b.get_weight(j)
                    w_out = wa * wb

                    if w_out != 0:
                        composite_acc.set_accessors(
                            batch_a.get_accessor(i),
                            batch_b.get_accessor(j)
                        )
                        out_batch.append_from_accessor(match_key, w_out, composite_acc)
