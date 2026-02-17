from rpython.rlib import jit
from gnitz.core import types
from gnitz.vm.batch import ZSetBatch

# UnifiedCursor is the Trace Reader defined in Step 1
from gnitz.storage.cursor import UnifiedCursor

class VMSchema(object):
    """
    Step 3.2: The VM-specialized Schema.
    Stores physical layout metadata in a JIT-immutable format.
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
        
        # Pull offsets and types into immutable lists for JIT optimization
        num_cols = len(table_schema.columns)
        self.column_offsets = [0] * num_cols
        self.column_types = [0] * num_cols
        
        for i in range(num_cols):
            self.column_offsets[i] = table_schema.get_column_offset(i)
            self.column_types[i] = table_schema.columns[i].field_type.code

    @jit.elidable
    def get_offset(self, col_idx):
        """Returns byte offset for a specific column."""
        assert col_idx >= 0 and col_idx < len(self.column_offsets)
        return self.column_offsets[col_idx]

    @jit.elidable
    def get_type(self, col_idx):
        """Returns the TYPE_XXX code for a specific column."""
        assert col_idx >= 0 and col_idx < len(self.column_types)
        return self.column_types[col_idx]

    def is_u128_pk(self):
        """Helper for selecting optimized join kernels."""
        return self.pk_type == types.TYPE_U128.code


class SchemaRegistry(object):
    """
    Step 3.2: The Schema Registry.
    Tracks and validates schemas used across a DBSP circuit.
    """
    def __init__(self):
        self.schemas = [] # List[VMSchema]

    def register(self, table_schema):
        """Adds a schema to the registry and returns its index (SchemaID)."""
        # Avoid duplicate registration
        for i in range(len(self.schemas)):
            if self.schemas[i].table_schema == table_schema:
                return i
        
        vm_schema = VMSchema(table_schema)
        schema_id = len(self.schemas)
        self.schemas.append(vm_schema)
        return schema_id

    @jit.elidable
    def get_schema(self, schema_id):
        assert schema_id >= 0 and schema_id < len(self.schemas)
        return self.schemas[schema_id]


class BaseRegister(object):
    """Refined Register Base (from Step 3.1)"""
    _immutable_fields_ = ['reg_id', 'vm_schema']

    def __init__(self, reg_id, vm_schema):
        self.reg_id = reg_id
        self.vm_schema = vm_schema

    def is_delta(self): return False
    def is_trace(self): return False


class DeltaRegister(BaseRegister):
    """R_Delta: Holds transient batches of multisets."""
    def __init__(self, reg_id, vm_schema):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self.batch = ZSetBatch(vm_schema.table_schema)

    def is_delta(self): return True
    
    def clear(self):
        self.batch.clear()


class TraceRegister(BaseRegister):
    """R_Trace: Holds persistent cursors for joins."""
    def __init__(self, reg_id, vm_schema, cursor):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self.cursor = cursor

    def is_trace(self): return True


class RegisterFile(object):
    """
    Container for all registers in the VM.
    The JIT uses this to find the data buffers for operators.
    """
    def __init__(self, num_registers):
        self.registers = [None] * num_registers

    @jit.elidable
    def get_register(self, reg_id):
        reg = self.registers[reg_id]
        assert reg is not None
        return reg

    def clear_all_deltas(self):
        for reg in self.registers:
            if isinstance(reg, DeltaRegister):
                reg.clear()
