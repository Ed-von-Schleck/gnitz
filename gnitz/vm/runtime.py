# gnitz/vm/runtime.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, opcodes as op
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import engine_ffi
from gnitz import log

# VM Execution Status Codes
STATUS_INIT    = 0
STATUS_RUNNING = 1
STATUS_HALTED  = 3
STATUS_ERROR   = 4


class BaseRegister(object):
    """Base class for all VM registers."""
    _immutable_fields_ = ['reg_id', 'table_schema']

    def __init__(self, reg_id, table_schema):
        self.reg_id = reg_id
        self.table_schema = table_schema
        self.batch = None
        self.cursor = None
        self.table = None

    def is_delta(self): return False
    def is_trace(self): return False


class DeltaRegister(BaseRegister):
    """R_Delta: Holds transient Z-Set batches. Runtime-only; no compile-time
    annotation fields permitted (see CircuitAnnotation for compile-time data)."""
    def __init__(self, reg_id, table_schema):
        BaseRegister.__init__(self, reg_id, table_schema)
        self._internal_batch = ArenaZSetBatch(table_schema)
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
    def __init__(self, reg_id, table_schema, cursor, table=None):
        BaseRegister.__init__(self, reg_id, table_schema)
        self.cursor = cursor
        self.table = table

    def is_trace(self): return True

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None

    def refresh(self):
        if self.table is None:
            return
        if self.cursor is not None:
            self.cursor.close()
        self.table.compact_if_needed()
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

    def reset(self):
        self.pc = 0
        self.status = STATUS_RUNNING


# ---------------------------------------------------------------------------
# Serialization helpers for the Rust VM
# ---------------------------------------------------------------------------

_NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)
SCHEMA_DESC_SIZE = 264  # Must match engine_ffi.SCHEMA_DESC_SIZE


def _write_u8(buf, pos, val):
    buf[pos] = chr(val & 0xFF)
    return pos + 1


def _write_u16(buf, pos, val):
    buf[pos] = chr(val & 0xFF)
    buf[pos + 1] = chr((val >> 8) & 0xFF)
    return pos + 2


def _write_i16(buf, pos, val):
    v = val & 0xFFFF
    buf[pos] = chr(v & 0xFF)
    buf[pos + 1] = chr((v >> 8) & 0xFF)
    return pos + 2


def _write_u32(buf, pos, val):
    buf[pos] = chr(val & 0xFF)
    buf[pos + 1] = chr((val >> 8) & 0xFF)
    buf[pos + 2] = chr((val >> 16) & 0xFF)
    buf[pos + 3] = chr((val >> 24) & 0xFF)
    return pos + 4


def _write_i32(buf, pos, val):
    v = val & 0xFFFFFFFF
    buf[pos] = chr(v & 0xFF)
    buf[pos + 1] = chr((v >> 8) & 0xFF)
    buf[pos + 2] = chr((v >> 16) & 0xFF)
    buf[pos + 3] = chr((v >> 24) & 0xFF)
    return pos + 4


def _reg_id(reg):
    """Get reg_id from a register or return 0xFFFF for None."""
    if reg is None:
        return 0xFFFF
    return reg.reg_id


class _ResourceCollector(object):
    """Collects unique resource handles and assigns indices for serialization."""
    def __init__(self):
        self.func_handles = []
        self.table_handles = []
        self.expr_handles = []
        self.schema_list = []
        # Agg descs stored as 3 parallel arrays (RPython-friendly)
        self.agg_col_idxs = []
        self.agg_ops = []
        self.agg_type_codes = []
        self.group_cols_flat = []

    def func_idx(self, func_obj):
        if func_obj is None:
            return 0xFFFF
        # Linear scan for dedup (small N)
        h = func_obj.get_func_handle()
        for i in range(len(self.func_handles)):
            if self.func_handles[i] == h:
                return i
        idx = len(self.func_handles)
        self.func_handles.append(h)
        return idx

    def table_idx(self, table_obj):
        if table_obj is None:
            return -1
        h = table_obj._handle
        for i in range(len(self.table_handles)):
            if self.table_handles[i] == h:
                return i
        idx = len(self.table_handles)
        self.table_handles.append(h)
        return idx

    def expr_idx(self, expr_obj):
        if expr_obj is None:
            return -1
        h = expr_obj._rust_handle
        for i in range(len(self.expr_handles)):
            if self.expr_handles[i] == h:
                return i
        idx = len(self.expr_handles)
        self.expr_handles.append(h)
        return idx

    def schema_idx(self, schema_obj):
        if schema_obj is None:
            return -1
        # Schemas are small, dedup by identity
        for i in range(len(self.schema_list)):
            if self.schema_list[i] is schema_obj:
                return i
        idx = len(self.schema_list)
        self.schema_list.append(schema_obj)
        return idx

    def add_agg_descs(self, agg_funcs):
        if agg_funcs is None:
            return (0, 0)
        offset = len(self.agg_col_idxs)
        for af in agg_funcs:
            self.agg_col_idxs.append(af.col_idx)
            self.agg_ops.append(af.agg_op)
            self.agg_type_codes.append(af.col_type_code)
        return (offset, len(agg_funcs))

    def add_group_cols(self, cols):
        if cols is None:
            return (0, 0)
        offset = len(self.group_cols_flat)
        for c in cols:
            self.group_cols_flat.append(c)
        return (offset, len(cols))


def _serialize_program_to_rust(program, reg_file):
    """
    Serialize a compiled program + register file into a Rust VM handle.
    Called once at view creation time. Returns opaque Rust handle (void*).
    """
    rc = _ResourceCollector()

    max_inline = 64  # conservative max per instruction

    num_instrs = len(program)
    log.debug("vm_ser: %d instructions" % num_instrs)
    # Header: 1 (opcode) + 12 (6 slots) + 4 (inline_len) = 17 bytes per instr + inline
    buf_size = num_instrs * (17 + max_inline)
    instr_buf = lltype.malloc(rffi.CCHARP.TO, buf_size, flavor="raw")
    pos = 0

    for idx in range(num_instrs):
        instr = program[idx]
        opcode = instr.opcode

        # Write opcode
        pos = _write_u8(instr_buf, pos, opcode)

        # Write 6 slots (default 0xFFFF)
        s0 = 0xFFFF
        s1 = 0xFFFF
        s2 = 0xFFFF
        s3 = 0xFFFF

        # Inline data buffer (we'll write into instr_buf after the slot header)
        # Reserve space for slots + inline_len, fill inline data, then backpatch inline_len
        slot_start = pos
        pos += 12  # 6 x u16
        inline_len_pos = pos
        pos += 4   # u32 inline_len
        inline_start = pos

        if opcode == op.OPCODE_HALT or opcode == op.OPCODE_CLEAR_DELTAS:
            pass

        elif opcode == op.OPCODE_DELAY:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_out)

        elif opcode == op.OPCODE_SCAN_TRACE:
            s0 = _reg_id(instr.reg_trace)
            s1 = _reg_id(instr.reg_out)
            pos = _write_i32(instr_buf, pos, instr.chunk_limit)

        elif opcode == op.OPCODE_SEEK_TRACE:
            s0 = _reg_id(instr.reg_trace)
            s1 = _reg_id(instr.reg_key)

        elif opcode == op.OPCODE_FILTER:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_out)
            s2 = rc.func_idx(instr.func)

        elif opcode == op.OPCODE_MAP:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_out)
            s2 = rc.func_idx(instr.func)
            out_reg = instr.reg_out
            s3 = rc.schema_idx(out_reg.table_schema) if out_reg is not None else 0xFFFF
            pos = _write_i32(instr_buf, pos, instr.reindex_col)

        elif opcode == op.OPCODE_NEGATE:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_out)

        elif opcode == op.OPCODE_UNION:
            s0 = _reg_id(instr.reg_in_a)
            s1 = _reg_id(instr.reg_in_b)
            s2 = _reg_id(instr.reg_out)
            has_b = 1 if instr.reg_in_b is not None else 0
            pos = _write_u8(instr_buf, pos, has_b)

        elif opcode == op.OPCODE_DISTINCT:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_history)
            s2 = _reg_id(instr.reg_out)
            # History table for ingesting consolidated delta
            hist_reg = instr.reg_history
            if hist_reg is not None and hist_reg.table is not None:
                pos = _write_i16(instr_buf, pos, rc.table_idx(hist_reg.table))
            else:
                pos = _write_i16(instr_buf, pos, -1)

        elif opcode == op.OPCODE_JOIN_DELTA_TRACE:
            s0 = _reg_id(instr.reg_delta)
            s1 = _reg_id(instr.reg_trace)
            s2 = _reg_id(instr.reg_out)
            trace_reg = instr.reg_trace
            s3 = rc.schema_idx(trace_reg.table_schema) if trace_reg is not None else 0xFFFF

        elif opcode == op.OPCODE_JOIN_DELTA_DELTA:
            s0 = _reg_id(instr.reg_a)
            s1 = _reg_id(instr.reg_b)
            s2 = _reg_id(instr.reg_out)
            b_reg = instr.reg_b
            s3 = rc.schema_idx(b_reg.table_schema) if b_reg is not None else 0xFFFF

        elif opcode == op.OPCODE_JOIN_DELTA_TRACE_OUTER:
            s0 = _reg_id(instr.reg_delta)
            s1 = _reg_id(instr.reg_trace)
            s2 = _reg_id(instr.reg_out)
            trace_reg = instr.reg_trace
            s3 = rc.schema_idx(trace_reg.table_schema) if trace_reg is not None else 0xFFFF

        elif opcode == op.OPCODE_ANTI_JOIN_DELTA_TRACE:
            s0 = _reg_id(instr.reg_delta)
            s1 = _reg_id(instr.reg_trace)
            s2 = _reg_id(instr.reg_out)

        elif opcode == op.OPCODE_ANTI_JOIN_DELTA_DELTA:
            s0 = _reg_id(instr.reg_a)
            s1 = _reg_id(instr.reg_b)
            s2 = _reg_id(instr.reg_out)

        elif opcode == op.OPCODE_SEMI_JOIN_DELTA_TRACE:
            s0 = _reg_id(instr.reg_delta)
            s1 = _reg_id(instr.reg_trace)
            s2 = _reg_id(instr.reg_out)

        elif opcode == op.OPCODE_SEMI_JOIN_DELTA_DELTA:
            s0 = _reg_id(instr.reg_a)
            s1 = _reg_id(instr.reg_b)
            s2 = _reg_id(instr.reg_out)

        elif opcode == op.OPCODE_INTEGRATE:
            s0 = _reg_id(instr.reg_in)
            # Inline: table_idx, has_gi, has_avi, [gi fields], [avi fields]
            tidx = rc.table_idx(instr.target_table)
            pos = _write_i32(instr_buf, pos, tidx)
            has_gi = 1 if instr.group_idx is not None else 0
            has_avi = 1 if instr.agg_value_idx is not None else 0
            pos = _write_u8(instr_buf, pos, has_gi)
            pos = _write_u8(instr_buf, pos, has_avi)
            if instr.group_idx is not None:
                gi = instr.group_idx
                gi_tidx = rc.table_idx(gi.table)
                pos = _write_u16(instr_buf, pos, gi_tidx & 0xFFFF)
                pos = _write_u32(instr_buf, pos, gi.col_idx)
                pos = _write_u8(instr_buf, pos, gi.col_type.code)
            if instr.agg_value_idx is not None:
                avi = instr.agg_value_idx
                avi_tidx = rc.table_idx(avi.table)
                pos = _write_u16(instr_buf, pos, avi_tidx & 0xFFFF)
                pos = _write_u8(instr_buf, pos, 1 if avi.for_max else 0)
                pos = _write_u8(instr_buf, pos, avi.agg_col_type.code)
                avi_gc_off, avi_gc_cnt = rc.add_group_cols(avi.group_by_cols)
                pos = _write_u32(instr_buf, pos, avi_gc_off)
                pos = _write_u16(instr_buf, pos, avi_gc_cnt)
                avi_schema_idx = rc.schema_idx(avi.input_schema)
                pos = _write_u16(instr_buf, pos, avi_schema_idx & 0xFFFF)
                pos = _write_u32(instr_buf, pos, avi.agg_col_idx)

        elif opcode == op.OPCODE_REDUCE:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_trace_out)
            s2 = _reg_id(instr.reg_out)
            s3 = rc.schema_idx(instr.output_schema)
            # Inline
            ti_reg = _reg_id(instr.reg_trace_in)
            pos = _write_i16(instr_buf, pos, ti_reg if ti_reg != 0xFFFF else -1)
            fin_reg = _reg_id(instr.fin_reg_out)
            pos = _write_i16(instr_buf, pos, fin_reg if fin_reg != 0xFFFF else -1)
            agg_off, agg_cnt = rc.add_agg_descs(instr.agg_funcs)
            pos = _write_u32(instr_buf, pos, agg_off)
            pos = _write_u16(instr_buf, pos, agg_cnt)
            gc_off, gc_cnt = rc.add_group_cols(instr.group_by_cols)
            pos = _write_u32(instr_buf, pos, gc_off)
            pos = _write_u16(instr_buf, pos, gc_cnt)
            # AVI — cursor created fresh from table by Rust VM
            avi = instr.agg_value_idx
            if avi is not None:
                avi_tidx = rc.table_idx(avi.table)
                pos = _write_i16(instr_buf, pos, avi_tidx)  # avi_table_idx
                pos = _write_u8(instr_buf, pos, 1 if avi.for_max else 0)
                pos = _write_u8(instr_buf, pos, avi.agg_col_type.code)
                avi_gc_off, avi_gc_cnt = rc.add_group_cols(avi.group_by_cols)
                pos = _write_u32(instr_buf, pos, avi_gc_off)
                pos = _write_u16(instr_buf, pos, avi_gc_cnt)
                avi_schema_idx = rc.schema_idx(avi.input_schema)
                pos = _write_i16(instr_buf, pos, avi_schema_idx)
                pos = _write_u32(instr_buf, pos, avi.agg_col_idx)
            else:
                pos = _write_i16(instr_buf, pos, -1)  # avi_table_idx
                pos = _write_u8(instr_buf, pos, 0)    # avi_for_max
                pos = _write_u8(instr_buf, pos, 0)    # avi_agg_col_type_code
                pos = _write_u32(instr_buf, pos, 0)   # avi_group_cols_offset
                pos = _write_u16(instr_buf, pos, 0)   # avi_group_cols_count
                pos = _write_i16(instr_buf, pos, -1)  # avi_input_schema_idx
                pos = _write_u32(instr_buf, pos, 0)   # avi_agg_col_idx
            # GI — cursor created fresh from table by Rust VM
            gi = instr.trace_in_group_idx
            if gi is not None:
                gi_tidx = rc.table_idx(gi.table)
                pos = _write_i16(instr_buf, pos, gi_tidx)  # gi_table_idx
                pos = _write_u32(instr_buf, pos, gi.col_idx)
                pos = _write_u8(instr_buf, pos, gi.col_type.code)
            else:
                pos = _write_i16(instr_buf, pos, -1)  # gi_table_idx
                pos = _write_u32(instr_buf, pos, 0)   # gi_col_idx
                pos = _write_u8(instr_buf, pos, 0)    # gi_col_type_code
            # Finalize
            pos = _write_i16(instr_buf, pos, rc.expr_idx(instr.finalize_prog))
            fin_schema_idx = -1
            if instr.fin_reg_out is not None:
                fin_schema_idx = rc.schema_idx(instr.fin_reg_out.table_schema)
            pos = _write_i16(instr_buf, pos, fin_schema_idx)

        elif opcode == op.OPCODE_GATHER_REDUCE:
            s0 = _reg_id(instr.reg_in)
            s1 = _reg_id(instr.reg_trace_out)
            s2 = _reg_id(instr.reg_out)
            agg_off, agg_cnt = rc.add_agg_descs(instr.agg_funcs)
            pos = _write_u32(instr_buf, pos, agg_off)
            pos = _write_u16(instr_buf, pos, agg_cnt)

        # Backpatch slots and inline_len
        inline_len = pos - inline_start
        sp = slot_start
        sp = _write_u16(instr_buf, sp, s0)
        sp = _write_u16(instr_buf, sp, s1)
        sp = _write_u16(instr_buf, sp, s2)
        sp = _write_u16(instr_buf, sp, s3)
        sp = _write_u16(instr_buf, sp, 0xFFFF)  # s4
        sp = _write_u16(instr_buf, sp, 0xFFFF)  # s5
        _write_u32(instr_buf, inline_len_pos, inline_len)

    instr_data_len = pos

    # Pack register metadata (schemas + kinds only; cursors passed per-epoch)
    num_regs = len(reg_file.registers)
    reg_schemas_buf = lltype.malloc(rffi.CCHARP.TO, max(num_regs * SCHEMA_DESC_SIZE, 1), flavor="raw")
    reg_kinds_buf = lltype.malloc(rffi.CCHARP.TO, max(num_regs, 1), flavor="raw")

    for ri in range(num_regs):
        reg = reg_file.registers[ri]
        if reg is not None:
            tmp_schema = engine_ffi.pack_schema(reg.table_schema)
            offset = ri * SCHEMA_DESC_SIZE
            for bi in range(SCHEMA_DESC_SIZE):
                reg_schemas_buf[offset + bi] = tmp_schema[bi]
            lltype.free(tmp_schema, flavor="raw")
            reg_kinds_buf[ri] = chr(1) if reg.is_trace() else chr(0)
        else:
            offset = ri * SCHEMA_DESC_SIZE
            for bi in range(SCHEMA_DESC_SIZE):
                reg_schemas_buf[offset + bi] = '\x00'
            reg_kinds_buf[ri] = chr(0)

    log.debug("vm_ser: instrs serialized, %d bytes, %d funcs, %d tables, %d schemas" % (
        instr_data_len, len(rc.func_handles), len(rc.table_handles), len(rc.schema_list)))

    # Pack func handles
    num_funcs = len(rc.func_handles)
    func_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_funcs, 1), flavor="raw")
    for fi in range(num_funcs):
        func_ptrs[fi] = rc.func_handles[fi]

    # Pack table handles
    num_tables = len(rc.table_handles)
    table_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_tables, 1), flavor="raw")
    for ti in range(num_tables):
        table_ptrs[ti] = rc.table_handles[ti]

    # Pack expr handles
    num_exprs = len(rc.expr_handles)
    expr_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_exprs, 1), flavor="raw")
    for ei in range(num_exprs):
        expr_ptrs[ei] = rc.expr_handles[ei]

    # Pack agg_descs (flat array of 8-byte structs: u32 col_idx, u8 agg_op, u8 col_type_code, 2 pad)
    num_agg = len(rc.agg_col_idxs)
    agg_buf_size = num_agg * 8
    if agg_buf_size < 8:
        agg_buf_size = 8
    agg_buf = lltype.malloc(rffi.CCHARP.TO, agg_buf_size, flavor="raw")
    for ai in range(num_agg):
        base = ai * 8
        _write_u32(agg_buf, base, rc.agg_col_idxs[ai])
        agg_buf[base + 4] = chr(rc.agg_ops[ai] & 0xFF)
        agg_buf[base + 5] = chr(rc.agg_type_codes[ai] & 0xFF)
        agg_buf[base + 6] = chr(0)
        agg_buf[base + 7] = chr(0)

    # Pack group_cols
    num_gc = len(rc.group_cols_flat)
    gc_buf = lltype.malloc(rffi.UINTP.TO, max(num_gc, 1), flavor="raw")
    for gi in range(num_gc):
        gc_buf[gi] = rffi.cast(rffi.UINT, rc.group_cols_flat[gi])

    # Pack extra schemas
    num_schemas = len(rc.schema_list)
    schemas_buf = lltype.malloc(rffi.CCHARP.TO, max(num_schemas * SCHEMA_DESC_SIZE, SCHEMA_DESC_SIZE), flavor="raw")
    for si in range(num_schemas):
        tmp_schema = engine_ffi.pack_schema(rc.schema_list[si])
        offset = si * SCHEMA_DESC_SIZE
        for bi in range(SCHEMA_DESC_SIZE):
            schemas_buf[offset + bi] = tmp_schema[bi]
        lltype.free(tmp_schema, flavor="raw")

    log.debug("vm_ser: packed regs=%d funcs=%d tables=%d exprs=%d aggs=%d gcols=%d schemas=%d" % (
        num_regs, num_funcs, num_tables, num_exprs, num_agg, num_gc, num_schemas))

    # Call Rust
    handle = engine_ffi._vm_program_create(
        instr_buf, rffi.cast(rffi.UINT, instr_data_len),
        rffi.cast(rffi.VOIDP, reg_schemas_buf), reg_kinds_buf,
        rffi.cast(rffi.UINT, num_regs),
        func_ptrs, rffi.cast(rffi.UINT, num_funcs),
        table_ptrs, rffi.cast(rffi.UINT, num_tables),
        expr_ptrs, rffi.cast(rffi.UINT, num_exprs),
        rffi.cast(rffi.VOIDP, agg_buf), rffi.cast(rffi.UINT, num_agg),
        gc_buf, rffi.cast(rffi.UINT, num_gc),
        rffi.cast(rffi.VOIDP, schemas_buf), rffi.cast(rffi.UINT, num_schemas),
    )

    # Cleanup temp buffers
    lltype.free(instr_buf, flavor="raw")
    lltype.free(reg_schemas_buf, flavor="raw")
    lltype.free(reg_kinds_buf, flavor="raw")
    lltype.free(func_ptrs, flavor="raw")
    lltype.free(table_ptrs, flavor="raw")
    lltype.free(expr_ptrs, flavor="raw")
    lltype.free(agg_buf, flavor="raw")
    lltype.free(gc_buf, flavor="raw")
    lltype.free(schemas_buf, flavor="raw")

    return handle


class ExecutablePlan(object):
    """
    Stateful, pre-compiled execution context for a Reactive View.
    Acts as the primary API boundary between the Executor and the VM.
    """
    _immutable_fields_ = [
        "program", "reg_file", "out_schema", "in_reg_idx", "out_reg_idx",
        "exchange_post_plan", "source_reg_map",
        "join_shard_map",
        "skip_exchange", "co_partitioned_join_sources",
    ]

    def __init__(self, program, reg_file, out_schema, in_reg_idx=0, out_reg_idx=1,
                 exchange_post_plan=None,
                 source_reg_map=None, join_shard_map=None,
                 skip_exchange=False, co_partitioned_join_sources=None):
        self.program = program
        self.reg_file = reg_file
        self.out_schema = out_schema
        self.in_reg_idx = in_reg_idx
        self.out_reg_idx = out_reg_idx
        self.context = ExecutionContext()
        self.exchange_post_plan = exchange_post_plan
        self.source_reg_map = source_reg_map   # dict {source_table_id: reg_id} or None
        self.join_shard_map = join_shard_map    # dict {source_table_id: [col_idx]} or None
        self.skip_exchange = skip_exchange      # True if exchange round-trip can be skipped
        self.co_partitioned_join_sources = co_partitioned_join_sources  # dict or None
        # Serialize program to Rust VM
        log.debug("vm: serializing program with %d instrs, %d regs" % (
            len(program), len(reg_file.registers)))
        self._rust_vm = _serialize_program_to_rust(program, reg_file)
        if self._rust_vm:
            log.debug("vm: serialized OK")
        else:
            log.debug("vm: serialization returned NULL, falling back to RPython")

    def close(self):
        if self._rust_vm:
            engine_ffi._vm_program_free(self._rust_vm)
            self._rust_vm = _NULL_HANDLE
        for reg in self.reg_file.registers:
            if reg is not None and reg.is_trace():
                reg.close()

    def execute_epoch(self, input_delta, source_id=0):
        """
        Executes the plan for one epoch/tick.
        Uses the Rust VM if available, otherwise falls back to RPython interpreter.
        """
        if self._rust_vm:
            return self._execute_epoch_rust(input_delta, source_id)
        return self._execute_epoch_rpython(input_delta, source_id)

    def _execute_epoch_rust(self, input_delta, source_id):
        """Execute via the Rust VM (single FFI call per epoch)."""
        # 1. RPython lifecycle: clear deltas, refresh cursors
        self.reg_file.prepare_for_tick()

        sealed = input_delta.to_consolidated()

        target_reg_idx = self.in_reg_idx
        if self.source_reg_map is not None and source_id in self.source_reg_map:
            target_reg_idx = self.source_reg_map[source_id]

        # Clone: the VM takes ownership (frees it)
        rust_input = engine_ffi._batch_clone(sealed._handle)

        # 2. Collect cursor handles from trace registers
        num_regs = len(self.reg_file.registers)
        cursor_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_regs, 1), flavor="raw")
        for ri in range(num_regs):
            reg = self.reg_file.registers[ri]
            if reg is not None and reg.is_trace() and reg.cursor is not None:
                cursor_ptrs[ri] = reg.cursor._handle
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

    def _execute_epoch_rpython(self, input_delta, source_id):
        """Execute via the RPython interpreter (fallback path)."""
        from gnitz.vm.interpreter import run_vm

        self.reg_file.prepare_for_tick()
        self.context.reset()

        sealed = input_delta.to_consolidated()

        target_reg_idx = self.in_reg_idx
        if self.source_reg_map is not None and source_id in self.source_reg_map:
            target_reg_idx = self.source_reg_map[source_id]
        in_reg = self.reg_file.get_register(target_reg_idx)
        assert in_reg.is_delta()
        in_reg.bind(sealed)

        run_vm(self.program, self.reg_file, self.context)

        result = None
        if self.context.status == STATUS_HALTED:
            out_reg = self.reg_file.get_register(self.out_reg_idx)
            assert out_reg.is_delta()
            if out_reg.batch is out_reg._internal_batch:
                if out_reg.batch.length() > 0:
                    result = out_reg._internal_batch
                    out_reg._internal_batch = ArenaZSetBatch(
                        out_reg.table_schema, initial_capacity=0
                    )
                    out_reg.batch = out_reg._internal_batch
            else:
                if out_reg.batch.length() > 0:
                    result = out_reg.batch.clone()
                out_reg.unbind()

        in_reg.unbind()
        if sealed is not input_delta:
            sealed.free()

        return result
