from rpython.rlib import jit
from gnitz.core import types
from gnitz.vm.batch import ZSetBatch

# Note: UnifiedCursor is assumed to be implemented in gnitz/storage/cursor.py 
# as per Phase 2, Step 1.
from gnitz.storage.cursor import UnifiedCursor

class VMSchema(object):
    """
    A JIT-optimized schema descriptor for the VM.
    By pinning the stride and offsets as immutable, the JIT can
    specialize assembly for specific table structures.
    """
    _immutable_fields_ = ['table_schema', 'stride', 'column_offsets[*]']

    def __init__(self, table_schema):
        self.table_schema = table_schema
        self.stride = table_schema.stride
        # Copying to a list for faster JIT-indexed access
        self.column_offsets = table_schema.column_offsets

    def get_offset(self, col_idx):
        return self.column_offsets[col_idx]


class BaseRegister(object):
    """
    Base class for all VM registers. 
    Allows the RegisterFile to be a monomorphic list.
    """
    _immutable_fields_ = ['reg_id', 'vm_schema']

    def __init__(self, reg_id, vm_schema):
        self.reg_id = reg_id
        self.vm_schema = vm_schema

    def is_delta(self):
        return False

    def is_trace(self):
        return False


class DeltaRegister(BaseRegister):
    """
    A register holding a transient Z-Set Batch (R_Delta).
    Used for incremental updates within a single LSN tick.
    """
    def __init__(self, reg_id, vm_schema):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self.batch = ZSetBatch(vm_schema.table_schema)

    def is_delta(self):
        return True

    def get_batch(self):
        return self.batch

    def clear(self):
        """Reset the batch for the next LSN epoch or operator pass."""
        self.batch.clear()


class TraceRegister(BaseRegister):
    """
    A register holding a persistent UnifiedCursor (R_Trace).
    Provides access to the historical state required for Joins and Reductions.
    """
    def __init__(self, reg_id, vm_schema, cursor):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self.cursor = cursor

    def is_trace(self):
        return True

    def get_cursor(self):
        return self.cursor


class RegisterFile(object):
    """
    The collection of registers for a specific DBSP circuit.
    This object is passed as a 'Red' (variable) variable to the JIT.
    """
    def __init__(self, num_registers):
        # Initialized as None; the QueryBuilder/Compiler will fill these.
        self.registers = [None] * num_registers

    @jit.elidable
    def get_register(self, reg_id):
        reg = self.registers[reg_id]
        assert reg is not None
        return reg

    def set_delta_reg(self, reg_id, schema):
        self.registers[reg_id] = DeltaRegister(reg_id, VMSchema(schema))

    def set_trace_reg(self, reg_id, schema, cursor):
        self.registers[reg_id] = TraceRegister(reg_id, VMSchema(schema), cursor)

    def clear_all_deltas(self):
        """
        Called at the end of a DBSP tick to prepare 
        Delta registers for the next batch of input.
        """
        for reg in self.registers:
            if reg is not None and reg.is_delta():
                # Cast needed for RPython annotator to see clear()
                assert isinstance(reg, DeltaRegister)
                reg.clear()
