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


class BaseRegister(object):
    """Base class for all VM registers."""
    _immutable_fields_ = ['reg_id', 'vm_schema']

    def __init__(self, reg_id, vm_schema):
        self.reg_id = reg_id
        self.vm_schema = vm_schema
        self.batch = None
        self.cursor = None
        self.table = None

    def is_delta(self): return False
    def is_trace(self): return False


class DeltaRegister(BaseRegister):
    """R_Delta: Holds transient Z-Set batches."""
    def __init__(self, reg_id, vm_schema):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self._internal_batch = ZSetBatch(vm_schema.table_schema)
        self.batch = self._internal_batch

    def is_delta(self): return True

    def clear(self):
        """Clears the internal arena."""
        self._internal_batch.clear()

    def bind(self, external_batch):
        """Temporarily borrows an external batch (e.g., for Register 0)."""
        self.batch = external_batch

    def unbind(self):
        """Restores the internal transient batch."""
        self.batch = self._internal_batch


class TraceRegister(BaseRegister):
    """R_Trace: Holds persistent cursors or stateful Table references."""
    def __init__(self, reg_id, vm_schema, cursor, table=None):
        BaseRegister.__init__(self, reg_id, vm_schema)
        self.cursor = cursor
        self.table = table

    def is_trace(self): return True

    def refresh(self):
        if self.table is None:
            return
        if self.cursor is not None:
            self.cursor.close()
        self.cursor = self.table.create_cursor()


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

    def prepare_for_tick(self):
        """Unified lifecycle hook called at the start of every epoch."""
        for reg in self.registers:
            if reg is None:
                continue
            if reg.is_delta():
                reg.clear()
            elif reg.is_trace():
                reg.refresh()
                
    def clear_deltas(self):
        """Only clears transient data; does NOT refresh cursors."""
        for reg in self.registers:
            if reg is not None and reg.is_delta():
                reg.clear()


class ExecutionContext(object):
    """Maintains the state of a VM execution (PC and status)."""
    def __init__(self):
        self.pc = 0
        self.status = STATUS_INIT
        self.yield_reason = YIELD_REASON_NONE

    def reset(self):
        self.pc = 0
        self.status = STATUS_RUNNING
        self.yield_reason = YIELD_REASON_NONE


class ExecutablePlan(object):
    """
    Stateful, pre-compiled execution context for a Reactive View.
    Acts as the primary API boundary between the Executor and the VM.
    """
    _immutable_fields_ = ["program", "reg_file", "out_schema", "in_reg_idx", "out_reg_idx"]

    def __init__(self, program, reg_file, out_schema, in_reg_idx=0, out_reg_idx=1):
        self.program = program
        self.reg_file = reg_file
        self.out_schema = out_schema
        self.in_reg_idx = in_reg_idx
        self.out_reg_idx = out_reg_idx
        self.context = ExecutionContext()

    def execute_epoch(self, input_delta):
        """
        Executes the plan for one epoch/tick.
        Returns a cloned output ZSetBatch if changes were produced, otherwise None.
        """
        from gnitz.vm.interpreter import run_vm

        # 1. Lifecycle: Clear transient state and refresh cursors
        self.reg_file.prepare_for_tick()
        self.context.reset()

        # 2. Binding: Borrow the input batch into the designated register
        in_reg = self.reg_file.get_register(self.in_reg_idx)
        assert in_reg.is_delta()
        in_reg.bind(input_delta)

        # 3. Execution: Run the interpreter logic
        run_vm(self.program, self.reg_file, self.context)

        # 4. Result Extraction: Capture output if the VM halted normally
        result = None
        if self.context.status == STATUS_HALTED:
            out_reg = self.reg_file.get_register(self.out_reg_idx)
            assert out_reg.is_delta()
            if out_reg.batch.length() > 0:
                result = out_reg.batch.clone()

        # 5. Cleanup: Release borrowed reference
        in_reg.unbind()

        return result
