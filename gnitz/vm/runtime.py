# gnitz/vm/runtime.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64
from gnitz.core import types
from gnitz.vm.batch import ZSetBatch

class VMSchema(object):
    """
    Metadata cache for the VM. 
    Promoted to constants during JIT tracing to fold address arithmetic.
    """
    _immutable_fields_ = [
        'table_schema', 'stride', 'column_offsets[*]', 
        'column_types[*]', 'pk_index', 'pk_type'
    ]

    def __init__(self, table_schema):
        self.table_schema = table_schema
        self.stride = table_schema.stride
        self.pk_index = table_schema.pk_index
        self.pk_type = table_schema.get_pk_column().field_type.code
        
        num_cols = len(table_schema.columns)
        self.column_offsets = [0] * num_cols
        self.column_types = [0] * num_cols
        
        for i in range(num_cols):
            self.column_offsets[i] = table_schema.get_column_offset(i)
            self.column_types[i] = table_schema.columns[i].field_type.code

    @jit.elidable
    def get_offset(self, col_idx):
        assert col_idx >= 0 and col_idx < len(self.column_offsets)
        return self.column_offsets[col_idx]

    @jit.elidable
    def get_type(self, col_idx):
        assert col_idx >= 0 and col_idx < len(self.column_types)
        return self.column_types[col_idx]

    def is_u128_pk(self):
        return self.pk_type == types.TYPE_U128.code


class BaseRegister(object):
    """
    Base class for all VM registers.
    
    RPython Requirement: Fields accessed via a base-class reference (like in 
    the Interpreter or Ops) must be defined and marked immutable at the 
    base-class level to avoid ImmutableConflictError.
    """
    _immutable_fields_ = ['reg_id', 'vm_schema', 'batch', 'cursor', 'table']

    def __init__(self, reg_id, vm_schema):
        self.reg_id = reg_id
        self.vm_schema = vm_schema
        # Initialize all potential subclass fields to None
        self.batch = None
        self.cursor = None
        self.table = None

    def is_delta(self): return False
    def is_trace(self): return False


class DeltaRegister(BaseRegister):
    """R_Delta: Holds transient Z-Set batches for the current epoch."""
    _immutable_fields_ = []

    def __init__(self, reg_id, vm_schema):
        BaseRegister.__init__(self, reg_id, vm_schema)
        # Initialize the batch field defined in BaseRegister
        self.batch = ZSetBatch(vm_schema.table_schema)

    def is_delta(self): return True
    
    def clear(self):
        self.batch.clear()


class TraceRegister(BaseRegister):
    """R_Trace: Holds persistent cursors (Traces) or stateful Table references."""
    _immutable_fields_ = []

    def __init__(self, reg_id, vm_schema, cursor, table=None):
        BaseRegister.__init__(self, reg_id, vm_schema)
        # Initialize fields defined in BaseRegister
        self.cursor = cursor
        self.table = table

    def is_trace(self): return True

    def get_weight(self, key, payload):
        """Looks up the current algebraic weight for a specific record."""
        if self.table is None:
            return r_int64(0)
        return self.table.get_weight(key, payload)

    def update_weight(self, key, delta, payload):
        """Updates the stateful trace with an incoming delta."""
        if self.table is not None:
            # Performs an immediate in-memory ingestion into the trace's 
            # backing table to maintain consistency for the current batch.
            self.table.engine.ingest(key, delta, payload)


class RegisterFile(object):
    """Collection of registers indexed by the VM ISA."""
    _immutable_fields_ = ['registers']

    def __init__(self, num_registers):
        self.registers = [None] * num_registers

    @jit.elidable
    def get_register(self, reg_id):
        reg = self.registers[reg_id]
        assert reg is not None
        return reg

    def clear_all_deltas(self):
        """Resets all transient Delta registers before a new epoch."""
        for reg in self.registers:
            if reg is not None and reg.is_delta():
                reg.clear()
