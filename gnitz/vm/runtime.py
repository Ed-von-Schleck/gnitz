# gnitz/vm/runtime.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64
from gnitz.core import types
from gnitz.core.batch import ZSetBatch

# VM Execution Status Codes
STATUS_INIT    = 0
STATUS_RUNNING = 1
STATUS_YIELDED = 2
STATUS_HALTED  = 3
STATUS_ERROR   = 4

# Yield Reason Codes
YIELD_REASON_NONE        = 0
YIELD_REASON_BUFFER_FULL = 1
YIELD_REASON_ROW_LIMIT   = 2
YIELD_REASON_USER        = 3


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
    """
    _immutable_fields_ = ['reg_id', 'vm_schema', 'batch', 'cursor', 'table']

    def __init__(self, reg_id, vm_schema):
        self.reg_id = reg_id
        self.vm_schema = vm_schema
        # Subclass fields initialized to None for annotator monomorphism
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
        self.batch = ZSetBatch(vm_schema.table_schema)

    def is_delta(self): return True
    
    def clear(self):
        self.batch.clear()


class TraceRegister(BaseRegister):
    """R_Trace: Holds persistent cursors (Traces) or stateful Table references."""
    _immutable_fields_ = []

    def __init__(self, reg_id, vm_schema, cursor, table=None):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self.cursor = cursor
        self.table = table

    def is_trace(self): return True

    def get_weight(self, key, payload):
        """Looks up the current algebraic weight for a specific record."""
        if self.table is None:
            return r_int64(0)
        # TableFamily or EphemeralTable (ZSetStore interface)
        return self.table.get_weight(key, payload)

    def update_batch(self, batch):
        """
        Updates the stateful trace with an incoming ZSetBatch.
        """
        if self.table is not None:
            # TableFamily or EphemeralTable (ZSetStore interface)
            self.table.ingest_batch(batch)


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
        """Resets all transient Delta registers."""
        for reg in self.registers:
            if reg is not None and reg.is_delta():
                reg.clear()


class ExecutionContext(object):
    """
    Maintains the state of a VM execution across multiple chunks/ticks.
    """
    _immutable_fields_ = ['reg_file']

    def __init__(self, reg_file):
        self.reg_file = reg_file
        self.pc = 0
        self.status = STATUS_INIT
        self.yield_reason = YIELD_REASON_NONE

    def reset(self, pc=0):
        self.pc = pc
        self.status = STATUS_RUNNING
        self.yield_reason = YIELD_REASON_NONE
