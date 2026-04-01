# gnitz/vm/runtime.py

from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import engine_ffi
from gnitz import log

_NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)


class ExecutablePlan(object):
    """
    Stateful, pre-compiled execution context for a Reactive View.
    Thin wrapper around a Rust VmHandle created by ProgramBuilder.
    """
    _immutable_fields_ = [
        "_rust_vm", "_num_regs", "out_schema", "in_reg_idx", "out_reg_idx",
        "exchange_post_plan", "source_reg_map",
        "join_shard_map",
        "skip_exchange", "co_partitioned_join_sources",
    ]

    def __init__(self, rust_vm_handle, num_regs, out_schema,
                 in_reg_idx=0, out_reg_idx=1,
                 trace_reg_tables=None,
                 exchange_post_plan=None,
                 source_reg_map=None, join_shard_map=None,
                 skip_exchange=False, co_partitioned_join_sources=None):
        self._rust_vm = rust_vm_handle
        self._num_regs = num_regs
        self.out_schema = out_schema
        self.in_reg_idx = in_reg_idx
        self.out_reg_idx = out_reg_idx
        self._trace_regs = trace_reg_tables if trace_reg_tables is not None else []
        self._cursors = {}  # {reg_id: RustUnifiedCursor}
        self.exchange_post_plan = exchange_post_plan
        self.source_reg_map = source_reg_map
        self.join_shard_map = join_shard_map
        self.skip_exchange = skip_exchange
        self.co_partitioned_join_sources = co_partitioned_join_sources
        if self._rust_vm:
            log.debug("vm: plan created, %d regs" % num_regs)
        else:
            log.debug("vm: plan creation returned NULL handle")

    def close(self):
        if self._rust_vm:
            engine_ffi._vm_program_free(self._rust_vm)
            self._rust_vm = _NULL_HANDLE
        for reg_id in self._cursors:
            c = self._cursors[reg_id]
            if c is not None:
                c.close()
        self._cursors = {}

    def execute_epoch(self, input_delta, source_id=0):
        if not self._rust_vm:
            raise Exception("Rust VM program creation failed")
        return self._execute_epoch_rust(input_delta, source_id)

    def _execute_epoch_rust(self, input_delta, source_id):
        # 1. Refresh trace cursors (z⁻¹(I(X)) — before current delta)
        for i in range(len(self._trace_regs)):
            reg_id = self._trace_regs[i][0]
            store = self._trace_regs[i][1]
            old = self._cursors.get(reg_id, None)
            if old is not None:
                old.close()
            store.compact_if_needed()
            self._cursors[reg_id] = store.create_cursor()

        # 2. Consolidate input
        sealed = input_delta.to_consolidated()

        target_reg_idx = self.in_reg_idx
        if self.source_reg_map is not None and source_id in self.source_reg_map:
            target_reg_idx = self.source_reg_map[source_id]

        # Clone: the VM takes ownership (frees it)
        rust_input = engine_ffi._batch_clone(sealed._handle)

        # 3. Collect cursor handles
        num_regs = self._num_regs
        cursor_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_regs, 1), flavor="raw")
        for ri in range(num_regs):
            if ri in self._cursors and self._cursors[ri] is not None:
                cursor_ptrs[ri] = self._cursors[ri]._handle
            else:
                cursor_ptrs[ri] = _NULL_HANDLE

        out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
        try:
            rc = engine_ffi._vm_execute_epoch(
                self._rust_vm,
                rust_input,
                rffi.cast(rffi.USHORT, target_reg_idx),
                rffi.cast(rffi.USHORT, self.out_reg_idx),
                cursor_ptrs, rffi.cast(rffi.UINT, num_regs),
                out_result,
            )
            if intmask(rc) < 0:
                raise Exception("vm_execute_epoch failed: %d" % intmask(rc))
            handle = out_result[0]
            if handle:
                result = ArenaZSetBatch._wrap_handle(self.out_schema, handle, False, False)
            else:
                result = None
        finally:
            lltype.free(out_result, flavor="raw")
            lltype.free(cursor_ptrs, flavor="raw")
            if sealed is not input_delta:
                sealed.free()
        return result
