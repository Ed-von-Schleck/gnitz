//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! One FFI call per epoch instead of ~20 (one per opcode).

use crate::schema::SchemaDescriptor;
use crate::storage::{OwnedBatch, CursorHandle, Table};
use crate::storage::ReadCursor;
use crate::ops::{self, AggDescriptor, GiDesc, AviDesc};
use crate::scalar_func::ScalarFuncKind;


// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// One VM instruction with all operator-specific data pre-resolved.
pub enum Instr {
    Halt,
    ClearDeltas,
    Delay { src: u16, state_reg: u16, dst: u16 },
    ScanTrace { trace_reg: u16, out_reg: u16, chunk_limit: i32 },
    SeekTrace { trace_reg: u16, key_reg: u16 },
    Filter { in_reg: u16, out_reg: u16, func_idx: u16 },
    Map { in_reg: u16, out_reg: u16, func_idx: u16, out_schema_idx: u16, reindex_col: i32 },
    Negate { in_reg: u16, out_reg: u16 },
    Union { in_a: u16, in_b: u16, has_b: bool, out_reg: u16 },
    Distinct { in_reg: u16, hist_reg: u16, out_reg: u16, hist_table_idx: i16 },
    JoinDT { delta_reg: u16, trace_reg: u16, out_reg: u16, right_schema_idx: u16 },
    JoinDD { a_reg: u16, b_reg: u16, out_reg: u16, right_schema_idx: u16 },
    JoinDTOuter { delta_reg: u16, trace_reg: u16, out_reg: u16, right_schema_idx: u16 },
    AntiJoinDT { delta_reg: u16, trace_reg: u16, out_reg: u16 },
    AntiJoinDD { a_reg: u16, b_reg: u16, out_reg: u16 },
    SemiJoinDT { delta_reg: u16, trace_reg: u16, out_reg: u16 },
    SemiJoinDD { a_reg: u16, b_reg: u16, out_reg: u16 },
    Integrate {
        in_reg: u16,
        table_idx: i32,       // index into Program::tables, -1 = no target (sink)
        gi: Option<IntegrateGi>,
        avi: Option<IntegrateAvi>,
    },
    Reduce {
        in_reg: u16,
        trace_in_reg: i16,    // -1 = no trace_in
        trace_out_reg: u16,
        out_reg: u16,
        fin_out_reg: i16,     // -1 = no finalize output
        agg_descs_offset: u32,
        agg_descs_count: u16,
        group_cols_offset: u32,
        group_cols_count: u16,
        output_schema_idx: u16,
        // AVI params — AVI cursor is created fresh from the AVI table each tick
        avi_table_idx: i16,   // -1 = no AVI; index into Program::tables
        avi_for_max: bool,
        avi_agg_col_type_code: u8,
        avi_group_cols_offset: u32,
        avi_group_cols_count: u16,
        avi_input_schema_idx: i16,  // -1 = use input schema
        avi_agg_col_idx: u32,
        // GI params — GI cursor is created fresh from the GI table each tick
        gi_table_idx: i16,    // -1 = no GI; index into Program::tables
        gi_col_idx: u32,
        gi_col_type_code: u8,
        // Finalize
        finalize_func_idx: i16,  // -1 = no finalize program
        finalize_schema_idx: i16,  // -1 = no finalize schema
    },
    GatherReduce {
        in_reg: u16,
        trace_out_reg: u16,
        out_reg: u16,
        agg_descs_offset: u32,
        agg_descs_count: u16,
    },
}

/// GI descriptor embedded in an Integrate instruction.
pub struct IntegrateGi {
    pub table_idx: u16,
    pub col_idx: u32,
    pub col_type_code: u8,
}

/// AVI descriptor embedded in an Integrate instruction.
pub struct IntegrateAvi {
    pub table_idx: u16,
    pub for_max: bool,
    pub agg_col_type_code: u8,
    pub group_cols_offset: u32,
    pub group_cols_count: u16,
    pub input_schema_idx: u16,
    pub agg_col_idx: u32,
}

// ---------------------------------------------------------------------------
// ProgramBuilder — constructs a Program via incremental add_*() calls.
// Serialization bridge (replaces instructions.py + runtime.py).
// ---------------------------------------------------------------------------

pub struct ProgramBuilder {
    num_registers: u16,
    instructions: Vec<Instr>,
    funcs: Vec<*const ScalarFuncKind>,
    tables: Vec<*mut Table>,
    schemas: Vec<SchemaDescriptor>,
    agg_descs: Vec<AggDescriptor>,
    group_cols: Vec<u32>,
    expr_progs: Vec<*const crate::expr::ExprProgram>,
}

// SAFETY: Same justification as Program — single-thread access, stable pointers.
unsafe impl Send for ProgramBuilder {}

impl ProgramBuilder {
    pub fn new(num_registers: u16) -> Self {
        ProgramBuilder {
            num_registers,
            instructions: Vec::with_capacity(16),
            funcs: Vec::new(),
            tables: Vec::new(),
            schemas: Vec::new(),
            agg_descs: Vec::new(),
            group_cols: Vec::new(),
            expr_progs: Vec::new(),
        }
    }

    // ── Resource dedup (linear scan, small N) ────────────────────────────

    fn func_idx(&mut self, ptr: *const ScalarFuncKind) -> u16 {
        for (i, &f) in self.funcs.iter().enumerate() {
            if f == ptr { return i as u16; }
        }
        let idx = self.funcs.len() as u16;
        self.funcs.push(ptr);
        idx
    }

    fn table_idx(&mut self, ptr: *mut Table) -> i32 {
        if ptr.is_null() { return -1; }
        for (i, &t) in self.tables.iter().enumerate() {
            if t == ptr { return i as i32; }
        }
        let idx = self.tables.len() as i32;
        self.tables.push(ptr);
        idx
    }

    fn schema_idx(&mut self, desc: SchemaDescriptor) -> u16 {
        // Dedup by byte equality (SchemaDescriptor is Copy + repr(C))
        for (i, s) in self.schemas.iter().enumerate() {
            if s.num_columns == desc.num_columns && s.pk_index == desc.pk_index {
                let mut same = true;
                for c in 0..desc.num_columns as usize {
                    if s.columns[c].type_code != desc.columns[c].type_code
                        || s.columns[c].size != desc.columns[c].size
                        || s.columns[c].nullable != desc.columns[c].nullable
                    {
                        same = false;
                        break;
                    }
                }
                if same { return i as u16; }
            }
        }
        let idx = self.schemas.len() as u16;
        self.schemas.push(desc);
        idx
    }

    fn expr_idx(&mut self, ptr: *const crate::expr::ExprProgram) -> i16 {
        if ptr.is_null() { return -1; }
        for (i, &e) in self.expr_progs.iter().enumerate() {
            if e == ptr { return i as i16; }
        }
        let idx = self.expr_progs.len() as i16;
        self.expr_progs.push(ptr);
        idx
    }

    fn add_agg_descs(&mut self, descs: &[AggDescriptor]) -> (u32, u16) {
        let offset = self.agg_descs.len() as u32;
        self.agg_descs.extend_from_slice(descs);
        (offset, descs.len() as u16)
    }

    fn add_group_cols(&mut self, cols: &[u32]) -> (u32, u16) {
        let offset = self.group_cols.len() as u32;
        self.group_cols.extend_from_slice(cols);
        (offset, cols.len() as u16)
    }

    // ── Instruction methods ──────────────────────────────────────────────

    pub fn add_halt(&mut self) {
        self.instructions.push(Instr::Halt);
    }

    pub fn add_clear_deltas(&mut self) {
        self.instructions.push(Instr::ClearDeltas);
    }

    pub fn add_delay(&mut self, src: u16, state_reg: u16, dst: u16) {
        self.instructions.push(Instr::Delay { src, state_reg, dst });
    }

    pub fn add_scan_trace(&mut self, trace_reg: u16, out_reg: u16, chunk_limit: i32) {
        self.instructions.push(Instr::ScanTrace { trace_reg, out_reg, chunk_limit });
    }

    pub fn add_seek_trace(&mut self, trace_reg: u16, key_reg: u16) {
        self.instructions.push(Instr::SeekTrace { trace_reg, key_reg });
    }

    pub fn add_filter(&mut self, in_reg: u16, out_reg: u16, func_ptr: *const ScalarFuncKind) {
        let func_idx = self.func_idx(func_ptr);
        self.instructions.push(Instr::Filter { in_reg, out_reg, func_idx });
    }

    pub fn add_map(
        &mut self, in_reg: u16, out_reg: u16,
        func_ptr: *const ScalarFuncKind, out_schema: SchemaDescriptor, reindex_col: i32,
    ) {
        let func_idx = self.func_idx(func_ptr);
        let out_schema_idx = self.schema_idx(out_schema);
        self.instructions.push(Instr::Map { in_reg, out_reg, func_idx, out_schema_idx, reindex_col });
    }

    pub fn add_negate(&mut self, in_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::Negate { in_reg, out_reg });
    }

    pub fn add_union(&mut self, in_a: u16, in_b: u16, has_b: bool, out_reg: u16) {
        self.instructions.push(Instr::Union { in_a, in_b, has_b, out_reg });
    }

    pub fn add_distinct(&mut self, in_reg: u16, hist_reg: u16, out_reg: u16, hist_table: *mut Table) {
        let hist_table_idx = self.table_idx(hist_table) as i16;
        self.instructions.push(Instr::Distinct { in_reg, hist_reg, out_reg, hist_table_idx });
    }

    pub fn add_join_dt(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDT { delta_reg, trace_reg, out_reg, right_schema_idx });
    }

    pub fn add_join_dd(&mut self, a_reg: u16, b_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDD { a_reg, b_reg, out_reg, right_schema_idx });
    }

    pub fn add_join_dt_outer(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDTOuter { delta_reg, trace_reg, out_reg, right_schema_idx });
    }

    pub fn add_anti_join_dt(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::AntiJoinDT { delta_reg, trace_reg, out_reg });
    }

    pub fn add_anti_join_dd(&mut self, a_reg: u16, b_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::AntiJoinDD { a_reg, b_reg, out_reg });
    }

    pub fn add_semi_join_dt(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::SemiJoinDT { delta_reg, trace_reg, out_reg });
    }

    pub fn add_semi_join_dd(&mut self, a_reg: u16, b_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::SemiJoinDD { a_reg, b_reg, out_reg });
    }

    pub fn add_integrate(
        &mut self,
        in_reg: u16,
        target_table: *mut Table,
        // GI params (all NULL/0 if no GI)
        gi_table: *mut Table,
        gi_col_idx: u32,
        gi_col_type_code: u8,
        // AVI params (all NULL/0 if no AVI)
        avi_table: *mut Table,
        avi_for_max: bool,
        avi_agg_col_type_code: u8,
        avi_group_cols: &[u32],
        avi_input_schema: *const SchemaDescriptor,
        avi_agg_col_idx: u32,
    ) {
        let table_idx = self.table_idx(target_table);
        let gi = if !gi_table.is_null() {
            Some(IntegrateGi {
                table_idx: self.table_idx(gi_table) as u16,
                col_idx: gi_col_idx,
                col_type_code: gi_col_type_code,
            })
        } else {
            None
        };
        let avi = if !avi_table.is_null() {
            let (gc_off, gc_cnt) = self.add_group_cols(avi_group_cols);
            let avi_schema = unsafe { *avi_input_schema };
            let schema_idx = self.schema_idx(avi_schema);
            Some(IntegrateAvi {
                table_idx: self.table_idx(avi_table) as u16,
                for_max: avi_for_max,
                agg_col_type_code: avi_agg_col_type_code,
                group_cols_offset: gc_off,
                group_cols_count: gc_cnt,
                input_schema_idx: schema_idx,
                agg_col_idx: avi_agg_col_idx,
            })
        } else {
            None
        };
        self.instructions.push(Instr::Integrate { in_reg, table_idx, gi, avi });
    }

    pub fn add_reduce(
        &mut self,
        in_reg: u16,
        trace_in_reg: i16,
        trace_out_reg: u16,
        out_reg: u16,
        fin_out_reg: i16,
        agg_descs: &[AggDescriptor],
        group_cols: &[u32],
        output_schema: SchemaDescriptor,
        // AVI params
        avi_table: *mut Table,
        avi_for_max: bool,
        avi_agg_col_type_code: u8,
        avi_group_cols: &[u32],
        avi_input_schema: *const SchemaDescriptor,
        avi_agg_col_idx: u32,
        // GI params
        gi_table: *mut Table,
        gi_col_idx: u32,
        gi_col_type_code: u8,
        // Finalize
        finalize_prog: *const crate::expr::ExprProgram,
        finalize_schema: *const SchemaDescriptor,
    ) {
        let (agg_off, agg_cnt) = self.add_agg_descs(agg_descs);
        let (gc_off, gc_cnt) = self.add_group_cols(group_cols);
        let output_schema_idx = self.schema_idx(output_schema);

        let avi_table_idx = self.table_idx(avi_table) as i16;
        let (avi_gc_off, avi_gc_cnt) = if !avi_table.is_null() {
            self.add_group_cols(avi_group_cols)
        } else {
            (0, 0)
        };
        let avi_input_schema_idx = if !avi_table.is_null() && !avi_input_schema.is_null() {
            self.schema_idx(unsafe { *avi_input_schema }) as i16
        } else {
            -1
        };

        let gi_table_idx = self.table_idx(gi_table) as i16;

        let finalize_func_idx = self.expr_idx(finalize_prog);
        let finalize_schema_idx = if !finalize_schema.is_null() {
            self.schema_idx(unsafe { *finalize_schema }) as i16
        } else {
            -1
        };

        self.instructions.push(Instr::Reduce {
            in_reg,
            trace_in_reg,
            trace_out_reg,
            out_reg,
            fin_out_reg,
            agg_descs_offset: agg_off,
            agg_descs_count: agg_cnt,
            group_cols_offset: gc_off,
            group_cols_count: gc_cnt,
            output_schema_idx,
            avi_table_idx,
            avi_for_max,
            avi_agg_col_type_code,
            avi_group_cols_offset: avi_gc_off,
            avi_group_cols_count: avi_gc_cnt,
            avi_input_schema_idx,
            avi_agg_col_idx,
            gi_table_idx,
            gi_col_idx,
            gi_col_type_code,
            finalize_func_idx,
            finalize_schema_idx,
        });
    }

    pub fn add_gather_reduce(
        &mut self,
        in_reg: u16,
        trace_out_reg: u16,
        out_reg: u16,
        agg_descs: &[AggDescriptor],
    ) {
        let (agg_off, agg_cnt) = self.add_agg_descs(agg_descs);
        self.instructions.push(Instr::GatherReduce {
            in_reg,
            trace_out_reg,
            out_reg,
            agg_descs_offset: agg_off,
            agg_descs_count: agg_cnt,
        });
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder, producing a boxed VmHandle (Program + RegisterFile).
    /// `reg_schemas` and `reg_kinds` are parallel arrays of length `num_registers`.
    pub fn build(self, reg_schemas: &[SchemaDescriptor], reg_kinds: &[u8]) -> Box<VmHandle> {
        assert_eq!(reg_schemas.len(), self.num_registers as usize);
        assert_eq!(reg_kinds.len(), self.num_registers as usize);

        let mut reg_meta = Vec::with_capacity(self.num_registers as usize);
        for i in 0..self.num_registers as usize {
            reg_meta.push(RegisterMeta {
                schema: reg_schemas[i],
                kind: match reg_kinds[i] {
                    1 => RegisterKind::Trace,
                    2 => RegisterKind::DelayState,
                    _ => RegisterKind::Delta,
                },
            });
        }

        let regfile = RegisterFile::new(&reg_meta);

        let program = Program {
            instructions: self.instructions,
            reg_meta,
            funcs: self.funcs,
            tables: self.tables,
            schemas: self.schemas,
            agg_descs: self.agg_descs,
            group_cols: self.group_cols,
            expr_progs: self.expr_progs,
        };

        Box::new(VmHandle {
            program,
            regfile,
            owned_tables: Vec::new(),
            owned_funcs: Vec::new(),
            owned_expr_progs: Vec::new(),
            owned_trace_regs: Vec::new(),
            owned_cursor_handles: Vec::new(),
        })
    }

    /// Consume the builder, producing a VmHandle that owns child tables,
    /// scalar functions, and expression programs created by the compiler.
    pub fn build_with_owned(
        self,
        reg_schemas: &[SchemaDescriptor],
        reg_kinds: &[u8],
        owned_tables: Vec<Box<Table>>,
        owned_funcs: Vec<Box<ScalarFuncKind>>,
        owned_expr_progs: Vec<Box<crate::expr::ExprProgram>>,
        owned_trace_regs: Vec<(u16, usize)>,
    ) -> Box<VmHandle> {
        assert_eq!(reg_schemas.len(), self.num_registers as usize);
        assert_eq!(reg_kinds.len(), self.num_registers as usize);

        let mut reg_meta = Vec::with_capacity(self.num_registers as usize);
        for i in 0..self.num_registers as usize {
            reg_meta.push(RegisterMeta {
                schema: reg_schemas[i],
                kind: match reg_kinds[i] {
                    1 => RegisterKind::Trace,
                    2 => RegisterKind::DelayState,
                    _ => RegisterKind::Delta,
                },
            });
        }

        let regfile = RegisterFile::new(&reg_meta);

        let program = Program {
            instructions: self.instructions,
            reg_meta,
            funcs: self.funcs,
            tables: self.tables,
            schemas: self.schemas,
            agg_descs: self.agg_descs,
            group_cols: self.group_cols,
            expr_progs: self.expr_progs,
        };

        let num_owned = owned_trace_regs.len();
        Box::new(VmHandle {
            program,
            regfile,
            owned_tables,
            owned_funcs,
            owned_expr_progs,
            owned_trace_regs,
            owned_cursor_handles: Vec::with_capacity(num_owned),
        })
    }
}

/// Opaque handle owning a compiled program and its register file.
///
/// When produced by the Rust compiler (`compile_view`), `owned_tables`,
/// `owned_funcs`, and `owned_expr_progs` hold heap resources that
/// `program.tables` / `program.funcs` / `program.expr_progs` borrow via
/// raw pointers.  Rust drop order (declaration order) ensures `program`
/// drops before the owned vecs, so dangling pointers are never chased.
pub struct VmHandle {
    pub program: Program,
    pub regfile: RegisterFile,
    /// Child tables created during compilation (history, reduce-in, GI, AVI).
    /// `program.tables` may point into these.  Dropped AFTER `program`.
    pub owned_tables: Vec<Box<Table>>,
    /// Scalar functions created during compilation.
    /// `program.funcs` may point into these.  Dropped AFTER `program`.
    pub owned_funcs: Vec<Box<ScalarFuncKind>>,
    /// Expression programs created during compilation.
    /// `program.expr_progs` may point into these.  Dropped AFTER `program`.
    pub owned_expr_progs: Vec<Box<crate::expr::ExprProgram>>,
    /// Trace registers backed by owned tables: `(reg_id, index into owned_tables)`.
    /// `execute_epoch` creates cursors from these before dispatch.
    pub owned_trace_regs: Vec<(u16, usize)>,
    /// Cursor handles for owned trace registers, kept alive across the epoch.
    /// Indexed in parallel with `owned_trace_regs`.
    owned_cursor_handles: Vec<Option<Box<CursorHandle<'static>>>>,
}

impl VmHandle {
    /// Compact owned tables and create fresh cursors for owned trace registers.
    /// Must be called before `execute_epoch` for compiler-produced plans.
    /// The cursor handles are stored in `owned_cursor_handles` and their
    /// raw pointers bound into the register file.
    pub fn refresh_owned_cursors(&mut self) {
        gnitz_debug!("vm: refresh_owned_cursors, {} owned trace regs", self.owned_trace_regs.len());
        // Resize storage if needed
        if self.owned_cursor_handles.len() < self.owned_trace_regs.len() {
            self.owned_cursor_handles.resize_with(self.owned_trace_regs.len(), || None);
        }
        for (slot, &(reg_id, table_idx)) in self.owned_trace_regs.iter().enumerate() {
            // Drop previous cursor before creating new one (releases shard refs etc.)
            self.owned_cursor_handles[slot] = None;

            // SAFETY: table_idx is valid (set during compilation). We need &mut
            // to the table, but we also hold &self.owned_trace_regs. This is safe
            // because owned_trace_regs is not modified here, and the table is
            // accessed through owned_tables which is a separate field.
            let table: &mut Table = unsafe { &mut *(&mut *self.owned_tables[table_idx] as *mut Table) };
            let _ = table.compact_if_needed();
            match table.create_cursor() {
                Ok(ch) => {
                    let mut boxed = Box::new(ch);
                    let ptr = &mut *boxed as *mut CursorHandle<'static>;
                    self.regfile.registers[reg_id as usize].cursor_ptr = ptr;
                    self.owned_cursor_handles[slot] = Some(boxed);
                }
                Err(_) => {
                    self.regfile.registers[reg_id as usize].cursor_ptr = std::ptr::null_mut();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

/// Register kind: delta (transient), trace (persistent cursor + table),
/// or delay_state (persistent batch for z⁻¹ cross-epoch storage).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RegisterKind {
    Delta,
    Trace,
    DelayState,
}

/// Per-register metadata.
#[derive(Clone)]
pub struct RegisterMeta {
    pub schema: SchemaDescriptor,
    pub kind: RegisterKind,
}

/// A compiled DBSP program ready for execution.
pub struct Program {
    pub instructions: Vec<Instr>,
    pub reg_meta: Vec<RegisterMeta>,
    /// Shared resource arrays — referenced by index from instructions.
    pub funcs: Vec<*const ScalarFuncKind>,
    pub tables: Vec<*mut Table>,
    pub schemas: Vec<SchemaDescriptor>,
    pub agg_descs: Vec<AggDescriptor>,
    pub group_cols: Vec<u32>,
    pub expr_progs: Vec<*const crate::expr::ExprProgram>,
}

// SAFETY: Program is only accessed from a single thread (the worker thread
// that owns the plan).  Raw pointers into ScalarFuncKind,
// Table, and ExprProgram are stable for the lifetime of the plan.
unsafe impl Send for Program {}

// ---------------------------------------------------------------------------
// Register file (runtime state)
// ---------------------------------------------------------------------------

/// Runtime state for one register.
pub struct Register {
    pub kind: RegisterKind,
    pub schema: SchemaDescriptor,
    /// Delta: current batch.  Trace: unused (empty).
    pub batch: OwnedBatch,
    /// Trace: current cursor.  Borrowed each epoch.  Delta: None.
    pub cursor_ptr: *mut CursorHandle<'static>,
}

/// Collection of registers for a single plan execution.
pub struct RegisterFile {
    pub registers: Vec<Register>,
}

impl RegisterFile {
    /// Create from register metadata.  Allocates empty batches and null cursors.
    pub fn new(metas: &[RegisterMeta]) -> Self {
        let mut registers = Vec::with_capacity(metas.len());
        for m in metas {
            let batch = if m.schema.num_columns > 0 {
                OwnedBatch::with_schema(m.schema, if m.kind == RegisterKind::Delta { 16 } else { 0 })
            } else {
                OwnedBatch::empty(0)
            };
            registers.push(Register {
                kind: m.kind,
                schema: m.schema,
                batch,
                cursor_ptr: std::ptr::null_mut(),
            });
        }
        RegisterFile { registers }
    }

    /// Clear all delta batches.  Cursor refresh is done by the caller before
    /// entering the Rust VM (it knows about Table vs PartitionedTable).
    pub fn clear_delta_batches(&mut self) {
        self.clear_deltas();
    }

    /// Bind cursor handles into trace registers.
    /// Each non-null handle is borrowed for the duration of the epoch.
    /// Owned trace registers (listed in `owned_trace_reg_ids`, already set by
    /// `refresh_owned_cursors`) are skipped entirely. External trace registers
    /// are always written from `handles` — stale pointers from prior epochs are
    /// never preserved.
    pub fn bind_cursors(
        &mut self,
        handles: &[*mut libc::c_void],
        owned_trace_reg_ids: &[(u16, usize)],
    ) {
        for (i, reg) in self.registers.iter_mut().enumerate() {
            if reg.kind == RegisterKind::Trace {
                let is_owned = owned_trace_reg_ids.iter().any(|&(rid, _)| rid as usize == i);
                if is_owned {
                    // Cursor was set by refresh_owned_cursors; leave it alone.
                } else if i < handles.len() && !handles[i].is_null() {
                    reg.cursor_ptr = handles[i] as *mut CursorHandle<'static>;
                } else {
                    reg.cursor_ptr = std::ptr::null_mut();
                }
            } else {
                reg.cursor_ptr = std::ptr::null_mut();
            }
        }
    }

    /// Clear delta batches without refreshing cursors.
    pub fn clear_deltas(&mut self) {
        for reg in &mut self.registers {
            if reg.kind == RegisterKind::Delta && reg.schema.num_columns > 0 {
                reg.batch.count = 0;
                reg.batch.pk_lo.clear();
                reg.batch.pk_hi.clear();
                reg.batch.weight.clear();
                reg.batch.null_bmp.clear();
                for col in &mut reg.batch.col_data {
                    col.clear();
                }
                reg.batch.blob.clear();
                reg.batch.sorted = true;
                reg.batch.consolidated = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute one epoch of a compiled program.
///
/// The input_batch is moved into `input_reg`.  After execution, the output
/// batch (if any) is extracted from `output_reg` and returned.
///
/// Returns `Ok(Some(batch))` if output was produced, `Ok(None)` if empty,
/// or `Err(rc)` on error.
pub fn execute_epoch(
    program: &Program,
    regfile: &mut RegisterFile,
    input_batch: OwnedBatch,
    input_reg: u16,
    output_reg: u16,
    cursor_handles: &[*mut libc::c_void],
    owned_trace_reg_ids: &[(u16, usize)],
) -> Result<Option<OwnedBatch>, i32> {
    gnitz_debug!("vm: execute_epoch input_count={} input_reg={} output_reg={} instrs={}",
        input_batch.count, input_reg, output_reg, program.instructions.len());

    // 1. Clear delta batches (cursor refresh already done by caller)
    regfile.clear_delta_batches();

    // 2. Bind cursors and input batch
    regfile.bind_cursors(cursor_handles, owned_trace_reg_ids);
    regfile.registers[input_reg as usize].batch = input_batch;

    // Raw pointer to the register array.  All instructions access distinct
    // registers (guaranteed by topological sort), so aliased &/&mut access
    // through different indices is safe.
    let regs = regfile.registers.as_mut_ptr();
    let nregs = regfile.registers.len();

    // Helper macros for safe indexed access via raw pointer.
    macro_rules! reg {
        ($i:expr) => {{ assert!(($i as usize) < nregs, "register index {} out of bounds (nregs={})", $i, nregs); unsafe { &*regs.add($i as usize) } }};
    }
    macro_rules! reg_mut {
        ($i:expr) => {{ assert!(($i as usize) < nregs, "register index {} out of bounds (nregs={})", $i, nregs); unsafe { &mut *regs.add($i as usize) } }};
    }
    macro_rules! cursor_mut {
        ($i:expr) => {{
            let r = reg_mut!($i);
            if r.cursor_ptr.is_null() {
                None
            } else {
                Some(unsafe { &mut *r.cursor_ptr }.cursor_mut())
            }
        }};
    }

    // 3. Dispatch loop
    for instr in &program.instructions {
        match instr {
            Instr::Halt => break,

            Instr::ClearDeltas => {
                regfile.clear_deltas();
            }

            Instr::Delay { src, state_reg, dst } => {
                let npc = reg!(*src).batch.col_data.len();
                let curr = std::mem::replace(&mut reg_mut!(*src).batch, OwnedBatch::empty(npc));
                let prev = std::mem::replace(&mut reg_mut!(*state_reg).batch, curr);
                reg_mut!(*dst).batch = prev;
            }

            Instr::ScanTrace { trace_reg, out_reg, chunk_limit } => {
                let schema = reg!(*trace_reg).schema;
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_scan_trace(cursor, &schema, *chunk_limit);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::SeekTrace { trace_reg, key_reg } => {
                let kb = &reg!(*key_reg).batch;
                if kb.count > 0 {
                    let key_lo = u64::from_le_bytes(kb.pk_lo[..8].try_into().unwrap());
                    let key_hi = u64::from_le_bytes(kb.pk_hi[..8].try_into().unwrap());
                    if let Some(cursor) = cursor_mut!(*trace_reg) {
                        cursor.seek(crate::util::make_pk(key_lo, key_hi));
                    }
                }
            }

            Instr::Filter { in_reg, out_reg, func_idx } => {
                let func_ptr = program.funcs[*func_idx as usize];
                let in_batch = &reg!(*in_reg).batch;
                let schema = reg!(*in_reg).schema;
                let result = if func_ptr.is_null() {
                    // NullPredicate: pass all rows unchanged
                    let mut out = in_batch.clone_batch();
                    out.schema = Some(schema);
                    out.sorted = in_batch.sorted;
                    out.consolidated = in_batch.consolidated;
                    out
                } else {
                    let func = unsafe { &*func_ptr };
                    ops::op_filter(in_batch, func, &schema)
                };
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Map { in_reg, out_reg, func_idx, out_schema_idx, reindex_col } => {
                let func_ptr = program.funcs[*func_idx as usize];
                let in_schema = reg!(*in_reg).schema;
                let out_schema = &program.schemas[*out_schema_idx as usize];
                let result = if func_ptr.is_null() {
                    // Identity MAP: no transformation — copy batch with updated schema.
                    let mut out = reg!(*in_reg).batch.clone_batch();
                    out.schema = Some(*out_schema);
                    out
                } else {
                    let func = unsafe { &*func_ptr };
                    ops::op_map(&reg!(*in_reg).batch, func, &in_schema, out_schema, *reindex_col)
                };
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Negate { in_reg, out_reg } => {
                let result = ops::op_negate(&reg!(*in_reg).batch);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Union { in_a, in_b, has_b, out_reg } => {
                let schema = reg!(*in_a).schema;
                let batch_b = if *has_b { Some(&reg!(*in_b).batch) } else { None };
                let result = ops::op_union(&reg!(*in_a).batch, batch_b, &schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Distinct { in_reg, hist_reg, out_reg, hist_table_idx } => {
                let schema = reg!(*in_reg).schema;
                if let Some(cursor) = cursor_mut!(*hist_reg) {
                    let (output, consolidated) = ops::op_distinct(&reg!(*in_reg).batch, cursor, &schema);
                    reg_mut!(*out_reg).batch = output;
                    // Ingest consolidated delta into history table
                    if *hist_table_idx >= 0 {
                        let ptr = program.tables[*hist_table_idx as usize];
                        let table = unsafe { &mut *ptr };
                        let _ = table.ingest_owned_batch(consolidated);
                    }
                }
            }

            Instr::JoinDT { delta_reg, trace_reg, out_reg, right_schema_idx } => {
                let left_schema = reg!(*delta_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_join_delta_trace(&reg!(*delta_reg).batch, cursor, &left_schema, right_schema);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::JoinDD { a_reg, b_reg, out_reg, right_schema_idx } => {
                let left_schema = reg!(*a_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                let result = ops::op_join_delta_delta(&reg!(*a_reg).batch, &reg!(*b_reg).batch, &left_schema, right_schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::JoinDTOuter { delta_reg, trace_reg, out_reg, right_schema_idx } => {
                let left_schema = reg!(*delta_reg).schema;
                let right_schema = &program.schemas[*right_schema_idx as usize];
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_join_delta_trace_outer(&reg!(*delta_reg).batch, cursor, &left_schema, right_schema);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::AntiJoinDT { delta_reg, trace_reg, out_reg } => {
                let schema = reg!(*delta_reg).schema;
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_anti_join_delta_trace(&reg!(*delta_reg).batch, cursor, &schema);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::AntiJoinDD { a_reg, b_reg, out_reg } => {
                let schema = reg!(*a_reg).schema;
                let result = ops::op_anti_join_delta_delta(&reg!(*a_reg).batch, &reg!(*b_reg).batch, &schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::SemiJoinDT { delta_reg, trace_reg, out_reg } => {
                let schema = reg!(*delta_reg).schema;
                if let Some(cursor) = cursor_mut!(*trace_reg) {
                    let result = ops::op_semi_join_delta_trace(&reg!(*delta_reg).batch, cursor, &schema);
                    reg_mut!(*out_reg).batch = result;
                }
            }

            Instr::SemiJoinDD { a_reg, b_reg, out_reg } => {
                let schema = reg!(*a_reg).schema;
                let result = ops::op_semi_join_delta_delta(&reg!(*a_reg).batch, &reg!(*b_reg).batch, &schema);
                reg_mut!(*out_reg).batch = result;
            }

            Instr::Integrate { in_reg, table_idx, gi, avi } => {
                let schema = reg!(*in_reg).schema;

                let target_ptr = if *table_idx >= 0 { program.tables[*table_idx as usize] } else { std::ptr::null_mut() };
                let target = if !target_ptr.is_null() {
                    Some(unsafe { &mut *target_ptr })
                } else {
                    None
                };

                let gi_desc = gi.as_ref().map(|g| GiDesc {
                    table: program.tables[g.table_idx as usize],
                    col_idx: g.col_idx,
                    col_type_code: g.col_type_code,
                });

                let avi_desc = avi.as_ref().map(|a| {
                    let gcols = &program.group_cols[a.group_cols_offset as usize
                        ..(a.group_cols_offset as usize + a.group_cols_count as usize)];
                    AviDesc {
                        table: program.tables[a.table_idx as usize],
                        for_max: a.for_max,
                        agg_col_type_code: a.agg_col_type_code,
                        group_by_cols: gcols.to_vec(),
                        input_schema: program.schemas[a.input_schema_idx as usize],
                        agg_col_idx: a.agg_col_idx,
                    }
                });

                gnitz_debug!("vm: INTEGRATE in_count={} target={} gi={} avi={}",
                    reg!(*in_reg).batch.count, target.is_some(), gi_desc.is_some(), avi_desc.is_some());
                ops::op_integrate_with_indexes(
                    &reg!(*in_reg).batch, target, &schema,
                    gi_desc.as_ref(), avi_desc.as_ref(),
                );
            }

            Instr::Reduce {
                in_reg, trace_in_reg, trace_out_reg, out_reg, fin_out_reg,
                agg_descs_offset, agg_descs_count,
                group_cols_offset, group_cols_count,
                output_schema_idx,
                avi_table_idx, avi_for_max, avi_agg_col_type_code,
                avi_group_cols_offset, avi_group_cols_count,
                avi_input_schema_idx, avi_agg_col_idx: _,
                gi_table_idx, gi_col_idx, gi_col_type_code,
                finalize_func_idx, finalize_schema_idx,
            } => {
                let in_schema = reg!(*in_reg).schema;
                let out_schema = &program.schemas[*output_schema_idx as usize];
                let aggs = &program.agg_descs[*agg_descs_offset as usize
                    ..(*agg_descs_offset as usize + *agg_descs_count as usize)];
                let gcols = &program.group_cols[*group_cols_offset as usize
                    ..(*group_cols_offset as usize + *group_cols_count as usize)];

                // trace_in cursor (from register file)
                let ti_cursor_ptr: *mut ReadCursor = if *trace_in_reg >= 0 {
                    cursor_mut!(*trace_in_reg).map(|c| c as *mut ReadCursor).unwrap_or(std::ptr::null_mut())
                } else {
                    std::ptr::null_mut()
                };

                // trace_out cursor (from register file)
                let to_cursor_ptr: *mut ReadCursor = cursor_mut!(*trace_out_reg)
                    .map(|c| c as *mut ReadCursor)
                    .unwrap_or(std::ptr::null_mut());
                if to_cursor_ptr.is_null() {
                    return Err(-10);
                }

                // AVI cursor — created fresh from the AVI table (not a register).
                // Must be created AFTER INTEGRATE populates the AVI table.
                let mut avi_cursor_handle: Option<Box<CursorHandle<'static>>> = if *avi_table_idx >= 0 {
                    let avi_ptr = program.tables[*avi_table_idx as usize];
                    let avi_table = unsafe { &mut *avi_ptr };
                    avi_table.create_cursor().ok().map(|ch| Box::new(ch))
                } else {
                    None
                };

                // GI cursor — created fresh from the GI table (not a register)
                let mut gi_cursor_handle: Option<Box<CursorHandle<'static>>> = if *gi_table_idx >= 0 {
                    let gi_ptr = program.tables[*gi_table_idx as usize];
                    let gi_table = unsafe { &mut *gi_ptr };
                    gi_table.create_cursor().ok().map(|ch| Box::new(ch))
                } else {
                    None
                };

                gnitz_debug!("vm: REDUCE in_count={} trace_in={} trace_out=ok avi={} gi={} aggs={}",
                    reg!(*in_reg).batch.count,
                    !ti_cursor_ptr.is_null(),
                    avi_cursor_handle.is_some(),
                    gi_cursor_handle.is_some(),
                    aggs.len());

                let avi_gcols = if *avi_group_cols_count > 0 {
                    &program.group_cols[*avi_group_cols_offset as usize
                        ..(*avi_group_cols_offset as usize + *avi_group_cols_count as usize)]
                } else {
                    &[] as &[u32]
                };
                let avi_in_schema = if *avi_input_schema_idx >= 0 {
                    Some(&program.schemas[*avi_input_schema_idx as usize])
                } else {
                    None
                };
                let fin_prog = if *finalize_func_idx >= 0 {
                    Some(unsafe { &*program.expr_progs[*finalize_func_idx as usize] })
                } else {
                    None
                };
                let fin_schema = if *finalize_schema_idx >= 0 {
                    Some(&program.schemas[*finalize_schema_idx as usize])
                } else {
                    None
                };

                let mut ti_opt: Option<&mut ReadCursor> = if !ti_cursor_ptr.is_null() {
                    Some(unsafe { &mut *ti_cursor_ptr })
                } else {
                    None
                };
                let mut avi_opt: Option<&mut ReadCursor> = avi_cursor_handle.as_deref_mut()
                    .map(|ch| ch.cursor_mut());
                let mut gi_opt: Option<&mut ReadCursor> = gi_cursor_handle.as_deref_mut()
                    .map(|ch| ch.cursor_mut());

                let (raw_out, fin_out) = ops::op_reduce(
                    &reg!(*in_reg).batch,
                    ti_opt.as_deref_mut(),
                    unsafe { &mut *to_cursor_ptr },
                    &in_schema,
                    out_schema,
                    gcols,
                    aggs,
                    avi_opt.as_deref_mut(),
                    *avi_for_max,
                    *avi_agg_col_type_code,
                    avi_gcols,
                    avi_in_schema,
                    gi_opt.as_deref_mut(),
                    *gi_col_idx,
                    *gi_col_type_code,
                    fin_prog,
                    fin_schema,
                );

                // Drop temporary cursor handles (returned to pool)
                drop(avi_cursor_handle);
                drop(gi_cursor_handle);

                reg_mut!(*out_reg).batch = raw_out;
                if let Some(fin_batch) = fin_out {
                    if *fin_out_reg >= 0 {
                        reg_mut!(*fin_out_reg).batch = fin_batch;
                    }
                }
            }

            Instr::GatherReduce { in_reg, trace_out_reg, out_reg, agg_descs_offset, agg_descs_count } => {
                let schema = reg!(*in_reg).schema;
                let aggs = &program.agg_descs[*agg_descs_offset as usize
                    ..(*agg_descs_offset as usize + *agg_descs_count as usize)];
                if let Some(cursor) = cursor_mut!(*trace_out_reg) {
                    let result = ops::op_gather_reduce(&reg!(*in_reg).batch, cursor, &schema, aggs);
                    reg_mut!(*out_reg).batch = result;
                }
            }
        }
    }

    gnitz_debug!("vm: dispatch done");

    // 4. Extract output
    let out = &mut regfile.registers[output_reg as usize];
    if out.batch.count > 0 {
        let npc = out.batch.col_data.len();
        let result = std::mem::replace(&mut out.batch, OwnedBatch::empty(npc));
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::ops::AggDescriptor;

    // ── Test helpers ─────────────────────────────────────────────────────

    fn make_schema(col_types: &[(u8, u8)]) -> SchemaDescriptor {
        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        // Column 0 is always the U128 PK
        columns[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        for (i, &(tc, sz)) in col_types.iter().enumerate() {
            columns[i + 1] = SchemaColumn { type_code: tc, size: sz, nullable: 0, _pad: 0 };
        }
        SchemaDescriptor {
            num_columns: (col_types.len() + 1) as u32,
            pk_index: 0,
            columns,
        }
    }

    /// Create a schema with one I64 payload column.
    fn schema_1i64() -> SchemaDescriptor {
        make_schema(&[(type_code::I64, 8)])
    }

    /// Create a batch from (pk_lo, pk_hi, weight, col0_i64) tuples.
    fn make_batch(schema: SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(schema, n);
        for &(pk_lo, pk_hi, w, c0) in rows {
            b.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
            b.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&c0.to_le_bytes());
        }
        b.count = n;
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Create a batch with two I64 payload columns.
    fn make_batch_2col(schema: SchemaDescriptor, rows: &[(u64, u64, i64, i64, i64)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(schema, n);
        for &(pk_lo, pk_hi, w, c0, c1) in rows {
            b.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
            b.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&c0.to_le_bytes());
            b.col_data[1].extend_from_slice(&c1.to_le_bytes());
        }
        b.count = n;
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Extract rows from a batch as (pk_lo, weight, col0_i64) tuples.
    fn extract_rows(b: &OwnedBatch) -> Vec<(u64, i64, i64)> {
        let mut rows = Vec::new();
        for i in 0..b.count {
            let pk_lo = u64::from_le_bytes(b.pk_lo[i*8..(i+1)*8].try_into().unwrap());
            let w = i64::from_le_bytes(b.weight[i*8..(i+1)*8].try_into().unwrap());
            let c0 = i64::from_le_bytes(b.col_data[0][i*8..(i+1)*8].try_into().unwrap());
            rows.push((pk_lo, w, c0));
        }
        rows
    }

    // ── Tests ────────────────────────────────────────────────────────────

    #[test]
    fn test_filter_negate_pipeline() {
        // Filter rows where col0 > 0, then negate weights.
        // Input: [(1,0,1,10), (2,0,1,-5), (3,0,1,20)]
        // Filter keeps pk=1 (col0=10>0) and pk=3 (col0=20>0)
        // Negate flips weights: pk=1 w=-1, pk=3 w=-1
        let schema = schema_1i64();

        // CompareConst filter uses schema column index (1 for the I64 payload col)
        let func = Box::new(ScalarFuncKind::Plan(
            crate::scalar_func::Plan::from_compare(1, 2, 0, false),
        ));
        let func_ptr = Box::into_raw(func) as *const ScalarFuncKind;

        let mut builder = ProgramBuilder::new(3);
        builder.add_filter(0, 1, func_ptr);
        builder.add_negate(1, 2);
        builder.add_halt();

        let input = make_batch(schema, &[
            (1, 0, 1, 10),
            (2, 0, 1, -5),
            (3, 0, 1, 20),
        ]);

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8; 3]; // all delta
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 3];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, -1, 10));
        assert_eq!(rows[1], (3, -1, 20));

        // Cleanup
        unsafe { drop(Box::from_raw(func_ptr as *mut ScalarFuncKind)); }
    }

    #[test]
    fn test_ghost_property() {
        // Input with opposing weights for same PK: consolidated to zero → None.
        // We pre-consolidate manually (input is consolidated before sending to VM).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        // Consolidated empty batch (ghost elimination already applied)
        let input = make_batch(schema, &[]);

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_empty_input() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        let input = make_batch(schema, &[]);

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap();

        assert!(result.is_none());
    }

    // ── Delay (z⁻¹) tests ───────────────────────────────────────────────
    //
    // Register layout for all single-delay tests:
    //   reg 0: Delta     (src / input_reg)
    //   reg 1: DelayState (state_reg)
    //   reg 2: Delta     (dst / output_reg)
    // reg_kinds = [0u8, 2u8, 0u8]

    #[test]
    fn test_delay_correct_temporal_semantics() {
        // At tick t, output must be the input from tick t-1 (z⁻¹ semantics).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(3);
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8, 2u8, 0u8];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 3];

        // Tick 0: z⁻¹[0] = 0 → None
        let input0 = make_batch(schema, &[(1, 0, 1, 10)]);
        let r0 = execute_epoch(&vm.program, &mut vm.regfile, input0, 0, 2, &cursors, &[]).unwrap();
        assert!(r0.is_none(), "tick 0 must return None (z⁻¹[0] = 0)");

        // Tick 1: output = tick 0's input
        let input1 = make_batch(schema, &[(2, 0, 1, 20)]);
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2, &cursors, &[]).unwrap().unwrap();
        let rows1 = extract_rows(&r1);
        assert_eq!(rows1, vec![(1, 1, 10)]);

        // Tick 2: output = tick 1's input
        let input2 = make_batch(schema, &[(3, 0, 1, 30)]);
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 2, &cursors, &[]).unwrap().unwrap();
        let rows2 = extract_rows(&r2);
        assert_eq!(rows2, vec![(2, 1, 20)]);
    }

    #[test]
    fn test_delay_empty_first_tick() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(3);
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8, 2u8, 0u8];
        let vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 3];

        let input = make_batch(schema, &[(1, 0, 1, 99)]);
        let r = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 2, &cursors, &[]).unwrap();
        assert!(r.is_none(), "tick 0 must always return None regardless of input");
    }

    #[test]
    fn test_delay_empty_intermediate_tick() {
        // An empty input is stored and replayed correctly.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(3);
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8, 2u8, 0u8];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 3];

        // Tick 0: non-empty input → None
        let r0 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(1, 0, 1, 10)]), 0, 2, &cursors, &[]).unwrap();
        assert!(r0.is_none());

        // Tick 1: empty input → tick 0's input replayed
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[]), 0, 2, &cursors, &[]).unwrap().unwrap();
        assert_eq!(extract_rows(&r1), vec![(1, 1, 10)]);

        // Tick 2: non-empty input → empty (tick 1's empty was stored)
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(2, 0, 1, 20)]), 0, 2, &cursors, &[]).unwrap();
        assert!(r2.is_none(), "tick 2 output should be empty (tick 1 input was empty)");

        // Tick 3: empty input → tick 2's input replayed
        let r3 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[]), 0, 2, &cursors, &[]).unwrap().unwrap();
        assert_eq!(extract_rows(&r3), vec![(2, 1, 20)]);
    }

    #[test]
    fn test_delay_preserves_negative_weights() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(3);
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8, 2u8, 0u8];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 3];

        // Tick 0: insertion → None
        let r0 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(1, 0, 1, 10)]), 0, 2, &cursors, &[]).unwrap();
        assert!(r0.is_none());

        // Tick 1: retraction → tick 0's insertion replayed
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(1, 0, -1, 10)]), 0, 2, &cursors, &[]).unwrap().unwrap();
        assert_eq!(extract_rows(&r1), vec![(1, 1, 10)]);

        // Tick 2: empty → tick 1's retraction replayed
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[]), 0, 2, &cursors, &[]).unwrap().unwrap();
        assert_eq!(extract_rows(&r2), vec![(1, -1, 10)]);
    }

    #[test]
    fn test_delay_chain() {
        // Two delays in series: input → Delay1(state1, mid) → Delay2(state2, out)
        // implements a 2-tick delay.
        //
        // Register layout:
        //   reg 0: Delta      (input)
        //   reg 1: DelayState (state1)
        //   reg 2: Delta      (mid, output of delay1 / input of delay2)
        //   reg 3: DelayState (state2)
        //   reg 4: Delta      (output)
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(5);
        builder.add_delay(0, 1, 2);
        builder.add_delay(2, 3, 4);
        builder.add_halt();

        let reg_schemas = [schema; 5];
        let reg_kinds = [0u8, 2u8, 0u8, 2u8, 0u8];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 5];

        // Tick 0: no output yet
        let r0 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(1, 0, 1, 10)]), 0, 4, &cursors, &[]).unwrap();
        assert!(r0.is_none());

        // Tick 1: still no output (need 2 ticks of delay)
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(2, 0, 1, 20)]), 0, 4, &cursors, &[]).unwrap();
        assert!(r1.is_none());

        // Tick 2: tick 0's input arrives
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[(3, 0, 1, 30)]), 0, 4, &cursors, &[]).unwrap().unwrap();
        assert_eq!(extract_rows(&r2), vec![(1, 1, 10)]);

        // Tick 3: empty input → tick 1's input arrives
        let r3 = execute_epoch(&vm.program, &mut vm.regfile, make_batch(schema, &[]), 0, 4, &cursors, &[]).unwrap().unwrap();
        assert_eq!(extract_rows(&r3), vec![(2, 1, 20)]);
    }

    #[test]
    fn test_delay_state_not_cleared_between_ticks() {
        // Directly verify that DelayState persists: after tick 0, state_reg holds
        // the input; after tick 1, it holds the new input.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(3);
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8, 2u8, 0u8];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 3];

        let input0 = make_batch(schema, &[(1, 0, 1, 10)]);
        execute_epoch(&vm.program, &mut vm.regfile, input0, 0, 2, &cursors, &[]).unwrap();
        assert_eq!(vm.regfile.registers[1].batch.count, 1, "state_reg must hold tick 0's input");

        let input1 = make_batch(schema, &[(2, 0, 1, 20), (3, 0, 1, 30)]);
        execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2, &cursors, &[]).unwrap();
        assert_eq!(vm.regfile.registers[1].batch.count, 2, "state_reg must hold tick 1's input");
    }

    #[test]
    fn test_union_operator() {
        // Union combines two batches. Use a pipeline: filter input into reg 1,
        // then delay input into reg 0 copy, then union reg 0 + reg 1.
        // Simpler: just union reg 0 with has_b=false (single input mode).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        // Union with has_b=false: effectively a pass-through (copies batch_a only)
        builder.add_union(0, 0, false, 1);
        builder.add_halt();

        let input = make_batch(schema, &[(1, 0, 1, 10), (2, 0, 1, 20)]);

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap().unwrap();

        assert_eq!(result.count, 2);
    }

    #[test]
    fn test_input_already_consolidated() {
        // Pre-consolidated input should not cause extra work.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        let mut input = make_batch(schema, &[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
        ]);
        input.consolidated = true;

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, 1, 10));
        assert_eq!(rows[1], (2, 1, 20));
    }

    #[test]
    fn test_delta_isolation_across_ticks() {
        // Two execute_epoch calls: second tick should not see first tick's data.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];

        // Tick 1
        let input1 = make_batch(schema, &[(1, 0, 1, 10)]);
        let r1 = execute_epoch(
            &vm.program, &mut vm.regfile, input1, 0, 1, &cursors, &[],
        ).unwrap().unwrap();
        assert_eq!(r1.count, 1);

        // Tick 2 with different data
        let input2 = make_batch(schema, &[(2, 0, 1, 20), (3, 0, 1, 30)]);
        let r2 = execute_epoch(
            &vm.program, &mut vm.regfile, input2, 0, 1, &cursors, &[],
        ).unwrap().unwrap();
        // Should have exactly 2 rows from tick 2, not 3 (no bleed from tick 1)
        assert_eq!(r2.count, 2);
        let rows = extract_rows(&r2);
        assert_eq!(rows[0].0, 2);
        assert_eq!(rows[1].0, 3);
    }

    #[test]
    fn test_map_operator() {
        // MAP with Plan projection: reorder/select columns.
        let in_schema = make_schema(&[
            (type_code::I64, 8),
            (type_code::I64, 8),
        ]);
        let out_schema = make_schema(&[(type_code::I64, 8)]);

        // MAP with Plan projection: reorder/select columns.
        let func = Box::new(ScalarFuncKind::Plan(
            crate::scalar_func::Plan::from_projection(&[2], &[type_code::I64], in_schema.pk_index),
        ));
        let func_ptr = Box::into_raw(func) as *const ScalarFuncKind;

        let mut builder = ProgramBuilder::new(2);
        builder.add_map(0, 1, func_ptr, out_schema, -1);
        builder.add_halt();

        let input = make_batch_2col(in_schema, &[
            (1, 0, 1, 10, 100),
            (2, 0, 1, 20, 200),
        ]);

        let reg_schemas = [in_schema, out_schema];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        // Projected column should be the second payload col (100, 200)
        assert_eq!(rows[0].2, 100);
        assert_eq!(rows[1].2, 200);

        unsafe { drop(Box::from_raw(func_ptr as *mut ScalarFuncKind)); }
    }

    #[test]
    fn test_distinct_multi_tick() {
        // DISTINCT clamps weights: +3 → +1, -1 → 0 (stays positive → no retraction).
        // Uses a real Table for history.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("dist_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "hist", schema, 0, 1 << 20, false,
        ).unwrap();

        let table_ptr = &mut table as *mut Table;

        let mut builder = ProgramBuilder::new(3);
        // reg 0 = input delta, reg 1 = history trace, reg 2 = output delta
        builder.add_distinct(0, 1, 2, table_ptr);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0, 1, 0]; // 0=delta, 1=trace, 0=delta

        let mut vm = *builder.build(&reg_schemas, &reg_kinds);

        // Tick 1: insert pk=1 with weight +3 → distinct output should be +1
        let input1 = make_batch(schema, &[(1, 0, 3, 42)]);
        // Need to create a cursor for the history register
        let cursor1 = table.create_cursor().unwrap();
        let ch1 = Box::into_raw(Box::new(cursor1)) as *mut libc::c_void;
        let cursors1 = vec![std::ptr::null_mut(), ch1, std::ptr::null_mut()];

        let r1 = execute_epoch(
            &vm.program, &mut vm.regfile, input1, 0, 2, &cursors1, &[],
        ).unwrap().unwrap();

        let rows1 = extract_rows(&r1);
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0], (1, 1, 42)); // clamped to +1

        unsafe { drop(Box::from_raw(ch1 as *mut CursorHandle)); }

        // Tick 2: delta w=-1, integral before tick = +3, after = +2 (still positive).
        // No boundary crossing → output should be empty.
        let cursor2 = table.create_cursor().unwrap();
        let ch2 = Box::into_raw(Box::new(cursor2)) as *mut libc::c_void;
        let cursors2 = vec![std::ptr::null_mut(), ch2, std::ptr::null_mut()];
        let input2 = make_batch(schema, &[(1, 0, -1, 42)]);
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 2, &cursors2, &[]).unwrap();
        assert!(r2.is_none(), "no boundary crossing: output should be empty");
        unsafe { drop(Box::from_raw(ch2 as *mut CursorHandle)); }

        // Tick 3: delta w=-2, integral before tick = +2, after = 0 (non-positive).
        // Positive→non-positive boundary crossed → retraction: output pk=1 w=-1.
        let cursor3 = table.create_cursor().unwrap();
        let ch3 = Box::into_raw(Box::new(cursor3)) as *mut libc::c_void;
        let cursors3 = vec![std::ptr::null_mut(), ch3, std::ptr::null_mut()];
        let input3 = make_batch(schema, &[(1, 0, -2, 42)]);
        let r3 = execute_epoch(&vm.program, &mut vm.regfile, input3, 0, 2, &cursors3, &[])
            .unwrap()
            .unwrap();
        let rows3 = extract_rows(&r3);
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0], (1, -1, 42)); // retraction
        unsafe { drop(Box::from_raw(ch3 as *mut CursorHandle)); }

        table.close();
    }

    #[test]
    fn test_join_delta_trace() {
        // JoinDT: join input delta against a trace cursor.
        // Input: pk=10 w=1 col0=100
        // Trace: pk=10 w=1 col0=200
        // Output: pk=10 w=1*1=1, merged payload
        let left_schema = schema_1i64();
        let right_schema = schema_1i64();
        let join_schema = make_schema(&[
            (type_code::I64, 8),
            (type_code::I64, 8),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("join_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "trace", right_schema, 0, 1 << 20, false,
        ).unwrap();

        // Ingest trace data
        let trace_batch = make_batch(right_schema, &[(10, 0, 1, 200)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.create_cursor().unwrap();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new(3);
        // reg 0 = left delta, reg 1 = right trace, reg 2 = output
        builder.add_join_dt(0, 1, 2, right_schema);
        builder.add_halt();

        let reg_schemas = [left_schema, right_schema, join_schema];
        let reg_kinds = [0, 1, 0];

        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        let input = make_batch(left_schema, &[(10, 0, 1, 100)]);
        let result = execute_epoch(
            &vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        assert_eq!(result.count, 1);
        // Weight should be product: 1*1 = 1
        let w = i64::from_le_bytes(result.weight[0..8].try_into().unwrap());
        assert_eq!(w, 1);
        // First payload col should be from left (100), second from right (200)
        let c0 = i64::from_le_bytes(result.col_data[0][0..8].try_into().unwrap());
        let c1 = i64::from_le_bytes(result.col_data[1][0..8].try_into().unwrap());
        assert_eq!(c0, 100);
        assert_eq!(c1, 200);

        unsafe { drop(Box::from_raw(ch as *mut CursorHandle)); }
        table.close();
    }

    #[test]
    fn test_join_delta_trace_multi_match() {
        // JoinDT: 1 delta row × N trace rows → N output rows.
        let left_schema = schema_1i64();
        let right_schema = schema_1i64();
        let join_schema = make_schema(&[
            (type_code::I64, 8),
            (type_code::I64, 8),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("join_multi_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "trace", right_schema, 0, 1 << 20, false,
        ).unwrap();

        // Ingest 3 trace rows with same PK but different payloads
        let trace_batch = make_batch(right_schema, &[
            (10, 0, 1, 100),
            (10, 0, 1, 200),
            (10, 0, 1, 300),
        ]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.create_cursor().unwrap();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new(3);
        builder.add_join_dt(0, 1, 2, right_schema);
        builder.add_halt();

        let reg_schemas = [left_schema, right_schema, join_schema];
        let reg_kinds = [0, 1, 0];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        let input = make_batch(left_schema, &[(10, 0, 2, 50)]); // weight=2
        let result = execute_epoch(
            &vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        // Should produce 3 output rows (1 delta × 3 trace)
        assert_eq!(result.count, 3);
        // Each output weight = 2 * 1 = 2
        for i in 0..3 {
            let w = i64::from_le_bytes(result.weight[i*8..(i+1)*8].try_into().unwrap());
            assert_eq!(w, 2);
        }

        unsafe { drop(Box::from_raw(ch as *mut CursorHandle)); }
        table.close();
    }

    #[test]
    fn test_cursor_lifecycle() {
        // Table ingest + cursor creation + read.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cursor_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "test", schema, 0, 1 << 20, false,
        ).unwrap();

        let batch = make_batch(schema, &[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
            (3, 0, 1, 30),
        ]);
        table.ingest_owned_batch(batch).unwrap();

        let mut ch = table.create_cursor().unwrap();
        let cursor = ch.cursor_mut();

        // Verify cursor iteration
        let mut count = 0;
        while cursor.valid {
            count += 1;
            cursor.advance();
        }
        assert_eq!(count, 3);

        table.close();
    }

    #[test]
    fn test_input_consolidation() {
        // Pre-consolidated input with summed weights passes through correctly.
        // Input is consolidated before sending to the VM, so we simulate that here.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        // Already-consolidated input: weights were summed before VM entry
        let input = make_batch(schema, &[(1, 0, 5, 42)]);

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (1, 5, 42));
    }

    #[test]
    fn test_anti_join_dt() {
        // AntiJoinDT: keep delta rows that have NO match in trace.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("antijoin_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "trace", schema, 0, 1 << 20, false,
        ).unwrap();

        // Trace has pk=1 and pk=3
        let trace_batch = make_batch(schema, &[
            (1, 0, 1, 100),
            (3, 0, 1, 300),
        ]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.create_cursor().unwrap();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new(3);
        builder.add_anti_join_dt(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0, 1, 0];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        // Delta has pk=1, pk=2, pk=3. Only pk=2 should survive anti-join.
        let input = make_batch(schema, &[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
            (3, 0, 1, 30),
        ]);
        let result = execute_epoch(
            &vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2); // only pk=2 survives

        unsafe { drop(Box::from_raw(ch as *mut CursorHandle)); }
        table.close();
    }

    #[test]
    fn test_semi_join_dt() {
        // SemiJoinDT: keep delta rows that HAVE a match in trace.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("semijoin_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "trace", schema, 0, 1 << 20, false,
        ).unwrap();

        let trace_batch = make_batch(schema, &[
            (1, 0, 1, 100),
            (3, 0, 1, 300),
        ]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.create_cursor().unwrap();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new(3);
        builder.add_semi_join_dt(0, 1, 2);
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0, 1, 0];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        let input = make_batch(schema, &[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
            (3, 0, 1, 30),
        ]);
        let result = execute_epoch(
            &vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1); // pk=1 matches trace
        assert_eq!(rows[1].0, 3); // pk=3 matches trace

        unsafe { drop(Box::from_raw(ch as *mut CursorHandle)); }
        table.close();
    }

    #[test]
    fn test_reduce_sum_multi_tick() {
        // REDUCE with SUM aggregation over a group column.
        // Uses real tables for trace_out and trace_in.
        let in_schema = make_schema(&[
            (type_code::I64, 8),  // group col (payload col 0)
            (type_code::I64, 8),  // agg col (payload col 1)
        ]);

        // Output schema: [U128 PK, I64 group_col, I64 sum_col]
        let out_schema = make_schema(&[
            (type_code::I64, 8),
            (type_code::I64, 8),
        ]);

        let dir = tempfile::tempdir().unwrap();

        // trace_out table
        let tout_dir = dir.path().join("tr_out");
        let mut trace_out_table = Table::new(
            tout_dir.to_str().unwrap(), "tr_out", out_schema, 0, 1 << 20, false,
        ).unwrap();

        let trace_out_ptr = &mut trace_out_table as *mut Table;

        // trace_in table (for non-linear agg, but SUM is linear — still test the path)
        let tin_dir = dir.path().join("tr_in");
        let mut trace_in_table = Table::new(
            tin_dir.to_str().unwrap(), "tr_in", in_schema, 0, 1 << 20, false,
        ).unwrap();

        let trace_in_ptr = &mut trace_in_table as *mut Table;

        // Agg descriptors: SUM of payload col 1 (schema col 2)
        // AGG_SUM = 1 (from functions.py)
        let agg_descs = [AggDescriptor {
            col_idx: 2,  // schema col index
            agg_op: 1,   // AGG_SUM
            col_type_code: type_code::I64,
            _pad: [0; 2],
        }];
        let group_cols = [1u32]; // schema col 1 = payload col 0 (group key)

        let mut builder = ProgramBuilder::new(5);
        // reg 0 = input delta
        // reg 1 = trace_out (trace register for output)
        // reg 2 = raw_delta output
        // reg 3 = trace_in (trace register for input history)
        // reg 4 unused

        // REDUCE: reads from reg 0, trace_in=reg 3, trace_out=reg 1, output=reg 2
        builder.add_reduce(
            0,            // in_reg
            3,            // trace_in_reg
            1,            // trace_out_reg
            2,            // out_reg
            -1,           // fin_out_reg (no finalize)
            &agg_descs,
            &group_cols,
            out_schema,
            std::ptr::null_mut(), // avi_table
            false, 0, &[], std::ptr::null(), 0,
            std::ptr::null_mut(), // gi_table
            0, 0,
            std::ptr::null(),     // finalize_prog
            std::ptr::null(),     // finalize_schema
        );

        // INTEGRATE raw_delta → trace_out
        builder.add_integrate(
            2,                    // in_reg (raw_delta)
            trace_out_ptr,        // target table
            std::ptr::null_mut(), 0, 0,   // no GI
            std::ptr::null_mut(), false, 0, &[], std::ptr::null(), 0, // no AVI
        );

        // INTEGRATE input → trace_in
        builder.add_integrate(
            0,                    // in_reg
            trace_in_ptr,         // target table
            std::ptr::null_mut(), 0, 0,
            std::ptr::null_mut(), false, 0, &[], std::ptr::null(), 0,
        );

        builder.add_halt();

        let reg_schemas = [in_schema, out_schema, out_schema, in_schema, in_schema];
        let reg_kinds = [0, 1, 0, 1, 0]; // delta, trace, delta, trace, delta

        let mut vm = *builder.build(&reg_schemas, &reg_kinds);

        // Tick 1: Insert group=1 with values 10, 20
        let input1 = make_batch_2col(in_schema, &[
            (1, 0, 1, 1, 10),  // pk=1, group=1, val=10
            (2, 0, 1, 1, 20),  // pk=2, group=1, val=20
        ]);

        // Create cursors for trace registers
        let tr_out_cursor = trace_out_table.create_cursor().unwrap();
        let tr_out_ch = Box::into_raw(Box::new(tr_out_cursor)) as *mut libc::c_void;
        let tr_in_cursor = trace_in_table.create_cursor().unwrap();
        let tr_in_ch = Box::into_raw(Box::new(tr_in_cursor)) as *mut libc::c_void;

        let cursors1 = vec![
            std::ptr::null_mut(), // reg 0: delta
            tr_out_ch,            // reg 1: trace_out
            std::ptr::null_mut(), // reg 2: delta
            tr_in_ch,             // reg 3: trace_in
            std::ptr::null_mut(), // reg 4
        ];

        let r1 = execute_epoch(
            &vm.program, &mut vm.regfile, input1, 0, 2, &cursors1, &[],
        ).unwrap().unwrap();

        // SUM of group=1: 10+20 = 30. Output should be one row with sum=30.
        assert!(r1.count >= 1);

        // Cleanup
        unsafe { drop(Box::from_raw(tr_out_ch as *mut CursorHandle)); }
        unsafe { drop(Box::from_raw(tr_in_ch as *mut CursorHandle)); }

        trace_out_table.close();
        trace_in_table.close();
    }

    #[test]
    fn test_seek_trace_point_lookup() {
        // SeekTrace positions a cursor at a given key, then ScanTrace reads from it.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("seek_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "trace", schema, 0, 1 << 20, false,
        ).unwrap();

        let trace_batch = make_batch(schema, &[
            (1, 0, 1, 10),
            (5, 0, 1, 50),
            (10, 0, 1, 100),
        ]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.create_cursor().unwrap();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        // Build: reg0=key input, reg1=trace, reg2=scan output
        let mut builder = ProgramBuilder::new(3);
        builder.add_seek_trace(1, 0);       // Seek trace to key from reg 0
        builder.add_scan_trace(1, 2, 100);  // Scan from trace into reg 2
        builder.add_halt();

        let reg_schemas = [schema, schema, schema];
        let reg_kinds = [0, 1, 0];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        // Input: a batch with pk=5 (used as seek key)
        let input = make_batch(schema, &[(5, 0, 1, 0)]);
        let result = execute_epoch(
            &vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        // After seeking to pk=5, scan should find pk=5 and pk=10 (2 rows from pk=5 onwards)
        assert!(result.count >= 2);
        let pk0 = u64::from_le_bytes(result.pk_lo[0..8].try_into().unwrap());
        assert_eq!(pk0, 5);

        unsafe { drop(Box::from_raw(ch as *mut CursorHandle)); }
        table.close();
    }

    #[test]
    fn test_join_dd() {
        // JoinDD: join two delta batches. Use filter(null) to copy input into
        // both reg 1 and reg 2, then join them (self-join on same PK).
        let schema = schema_1i64();
        let join_schema = make_schema(&[
            (type_code::I64, 8),
            (type_code::I64, 8),
        ]);

        let mut builder = ProgramBuilder::new(4);
        // Copy input (reg 0) into reg 1 and reg 2 via null filters
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_filter(0, 2, std::ptr::null());
        // Self-join: reg 1 × reg 2
        builder.add_join_dd(1, 2, 3, schema);
        builder.add_halt();

        let input = make_batch(schema, &[(10, 0, 2, 100)]);

        let reg_schemas = [schema, schema, schema, join_schema];
        let reg_kinds = [0u8; 4];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 4];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 3, &cursors, &[],
        ).unwrap().unwrap();

        assert_eq!(result.count, 1);
        let w = i64::from_le_bytes(result.weight[0..8].try_into().unwrap());
        assert_eq!(w, 4); // 2 * 2 (self-join weight multiplication)
        let c0 = i64::from_le_bytes(result.col_data[0][0..8].try_into().unwrap());
        let c1 = i64::from_le_bytes(result.col_data[1][0..8].try_into().unwrap());
        assert_eq!(c0, 100);
        assert_eq!(c1, 100);
    }

    #[test]
    fn test_program_builder_resource_dedup() {
        // Verify that adding the same func/table pointer twice reuses the same index.
        let schema = schema_1i64();

        let func = Box::new(ScalarFuncKind::Plan(
            crate::scalar_func::Plan::from_compare(1, 2, 0, false),
        ));
        let func_ptr = Box::into_raw(func) as *const ScalarFuncKind;

        let mut builder = ProgramBuilder::new(4);
        builder.add_filter(0, 1, func_ptr);
        builder.add_filter(1, 2, func_ptr); // same func — should reuse index
        builder.add_halt();

        let reg_schemas = [schema; 4];
        let reg_kinds = [0u8; 4];
        let vm = builder.build(&reg_schemas, &reg_kinds);

        // Should have only 1 func, not 2
        assert_eq!(vm.program.funcs.len(), 1);
        // Both filter instructions should reference func_idx=0
        match &vm.program.instructions[0] {
            Instr::Filter { func_idx, .. } => assert_eq!(*func_idx, 0),
            _ => panic!("expected Filter"),
        }
        match &vm.program.instructions[1] {
            Instr::Filter { func_idx, .. } => assert_eq!(*func_idx, 0),
            _ => panic!("expected Filter"),
        }

        unsafe { drop(Box::from_raw(func_ptr as *mut ScalarFuncKind)); }
    }

    /// Ports compile_graph_test.py::test_anti_semi_complement.
    /// Anti-join + semi-join should partition the input: |anti| + |semi| == |input|.
    #[test]
    fn test_anti_semi_complement() {
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("complement_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "trace", schema, 0, 1 << 20, false,
        ).unwrap();

        // Trace has pk=1, pk=3, pk=5
        let trace_batch = make_batch(schema, &[
            (1, 0, 1, 100),
            (3, 0, 1, 300),
            (5, 0, 1, 500),
        ]);
        table.ingest_owned_batch(trace_batch).unwrap();

        // Anti-join pipeline: reg 0 -> anti_join(reg 0, reg 1) -> reg 2
        let cursor_aj = table.create_cursor().unwrap();
        let ch_aj = Box::into_raw(Box::new(cursor_aj)) as *mut libc::c_void;

        let mut builder_aj = ProgramBuilder::new(3);
        builder_aj.add_anti_join_dt(0, 1, 2);
        builder_aj.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0, 1, 0];
        let mut vm_aj = *builder_aj.build(&reg_schemas, &reg_kinds);
        let cursors_aj = vec![std::ptr::null_mut(), ch_aj, std::ptr::null_mut()];

        // Delta has pk=1,2,3,4,5
        let input_aj = make_batch(schema, &[
            (1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30),
            (4, 0, 1, 40), (5, 0, 1, 50),
        ]);
        let r_aj = execute_epoch(
            &vm_aj.program, &mut vm_aj.regfile, input_aj, 0, 2, &cursors_aj, &[],
        ).unwrap().unwrap();

        // Semi-join pipeline
        let cursor_sj = table.create_cursor().unwrap();
        let ch_sj = Box::into_raw(Box::new(cursor_sj)) as *mut libc::c_void;

        let mut builder_sj = ProgramBuilder::new(3);
        builder_sj.add_semi_join_dt(0, 1, 2);
        builder_sj.add_halt();

        let mut vm_sj = *builder_sj.build(&reg_schemas, &reg_kinds);
        let cursors_sj = vec![std::ptr::null_mut(), ch_sj, std::ptr::null_mut()];

        let input_sj = make_batch(schema, &[
            (1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 1, 30),
            (4, 0, 1, 40), (5, 0, 1, 50),
        ]);
        let r_sj = execute_epoch(
            &vm_sj.program, &mut vm_sj.regfile, input_sj, 0, 2, &cursors_sj, &[],
        ).unwrap().unwrap();

        // Anti should get pk=2,4 (no match); semi should get pk=1,3,5 (match)
        assert_eq!(r_aj.count, 2, "anti-join should keep 2 non-matching rows");
        assert_eq!(r_sj.count, 3, "semi-join should keep 3 matching rows");
        assert_eq!(r_aj.count + r_sj.count, 5,
                   "anti + semi must equal input count (complement property)");

        unsafe { drop(Box::from_raw(ch_aj as *mut CursorHandle)); }
        unsafe { drop(Box::from_raw(ch_sj as *mut CursorHandle)); }
        table.close();
    }

    /// Ports compile_graph_test.py::test_filter_with_expr.
    /// Filter with expression bytecode: col1 > 25 keeps rows with val 30, 40, 50.
    #[test]
    fn test_filter_with_expr() {
        use crate::expr::{ExprProgram, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_CMP_GT};

        let schema = schema_1i64();

        // Build expression: col1 > 25
        // col1 is schema column index 1 (the I64 payload column)
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,  // r0 = col[1]
            EXPR_LOAD_CONST, 1, 25, 0,   // r1 = 25
            EXPR_CMP_GT, 2, 0, 1,        // r2 = (r0 > r1)
        ];
        let prog = ExprProgram {
            code,
            num_regs: 3,
            result_reg: 2,
            num_instrs: 3,
            const_strings: vec![],
            const_prefixes: vec![],
            const_lengths: vec![],
        };

        let func = Box::new(ScalarFuncKind::Plan(
            crate::scalar_func::Plan::from_predicate(prog),
        ));
        let func_ptr = Box::into_raw(func) as *const ScalarFuncKind;

        let mut builder = ProgramBuilder::new(2);
        builder.add_filter(0, 1, func_ptr);
        builder.add_halt();

        let input = make_batch(schema, &[
            (1, 0, 1, 10),
            (2, 0, 1, 20),
            (3, 0, 1, 30),
            (4, 0, 1, 40),
            (5, 0, 1, 50),
        ]);

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        ).unwrap().unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 3, "filter col1>25 should keep 3 rows (30,40,50)");
        assert_eq!(rows[0].0, 3); // pk=3, val=30
        assert_eq!(rows[1].0, 4); // pk=4, val=40
        assert_eq!(rows[2].0, 5); // pk=5, val=50

        unsafe { drop(Box::from_raw(func_ptr as *mut ScalarFuncKind)); }
    }

    /// Ports compile_graph_test.py::test_reduce_multi_agg.
    /// Multi-agg reduce: COUNT + SUM on same column in one pass.
    #[test]
    fn test_reduce_multi_agg() {
        // Input: pk(U64), val(I64). All rows in same group (pk=1).
        // COUNT + SUM of val column.
        let in_schema = schema_1i64();

        // Output: pk(U64), count(I64), sum(I64) — GROUP BY pk → natural PK
        let out_schema = make_schema(&[
            (type_code::I64, 8),  // count
            (type_code::I64, 8),  // sum
        ]);

        let dir = tempfile::tempdir().unwrap();

        let tout_dir = dir.path().join("ma_tr_out");
        let mut trace_out_table = Table::new(
            tout_dir.to_str().unwrap(), "ma_tr_out", out_schema, 0, 1 << 20, false,
        ).unwrap();
        let trace_out_ptr = &mut trace_out_table as *mut Table;

        let tin_dir = dir.path().join("ma_tr_in");
        let mut trace_in_table = Table::new(
            tin_dir.to_str().unwrap(), "ma_tr_in", in_schema, 0, 1 << 20, false,
        ).unwrap();
        let trace_in_ptr = &mut trace_in_table as *mut Table;

        // Two agg descriptors: COUNT(col=1) and SUM(col=1)
        // AGG_COUNT = 1, AGG_SUM = 2 (matching opcodes.py and ops.rs)
        let agg_descs = [
            AggDescriptor {
                col_idx: 1,           // schema col index for the val column
                agg_op: 1,            // AGG_COUNT
                col_type_code: type_code::I64,
                _pad: [0; 2],
            },
            AggDescriptor {
                col_idx: 1,           // schema col index for the val column
                agg_op: 2,            // AGG_SUM
                col_type_code: type_code::I64,
                _pad: [0; 2],
            },
        ];
        // GROUP BY col 0 (= pk, schema col index 0)
        let group_cols = [0u32];

        let mut builder = ProgramBuilder::new(5);
        // reg 0 = input delta, reg 1 = trace_out, reg 2 = output,
        // reg 3 = trace_in, reg 4 = unused
        builder.add_reduce(
            0, 3, 1, 2, -1,
            &agg_descs, &group_cols, out_schema,
            std::ptr::null_mut(), false, 0, &[], std::ptr::null(), 0,
            std::ptr::null_mut(), 0, 0,
            std::ptr::null(), std::ptr::null(),
        );
        builder.add_integrate(
            2, trace_out_ptr,
            std::ptr::null_mut(), 0, 0,
            std::ptr::null_mut(), false, 0, &[], std::ptr::null(), 0,
        );
        builder.add_integrate(
            0, trace_in_ptr,
            std::ptr::null_mut(), 0, 0,
            std::ptr::null_mut(), false, 0, &[], std::ptr::null(), 0,
        );
        builder.add_halt();

        let reg_schemas = [in_schema, out_schema, out_schema, in_schema, in_schema];
        let reg_kinds = [0, 1, 0, 1, 0];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);

        // Input: 3 rows all with pk=1, vals 10, 20, 30
        let input = make_batch(in_schema, &[
            (1, 0, 1, 10),
            (1, 0, 1, 20),
            (1, 0, 1, 30),
        ]);

        let tr_out_cursor = trace_out_table.create_cursor().unwrap();
        let tr_out_ch = Box::into_raw(Box::new(tr_out_cursor)) as *mut libc::c_void;
        let tr_in_cursor = trace_in_table.create_cursor().unwrap();
        let tr_in_ch = Box::into_raw(Box::new(tr_in_cursor)) as *mut libc::c_void;

        let cursors = vec![
            std::ptr::null_mut(), tr_out_ch, std::ptr::null_mut(),
            tr_in_ch, std::ptr::null_mut(),
        ];

        let result = execute_epoch(
            &vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[],
        ).unwrap().unwrap();

        // Should produce 1 row: pk=1, count=3, sum=60
        assert_eq!(result.count, 1, "multi-agg should produce 1 group");
        let count_val = i64::from_le_bytes(
            result.col_data[0][0..8].try_into().unwrap());
        let sum_val = i64::from_le_bytes(
            result.col_data[1][0..8].try_into().unwrap());
        assert_eq!(count_val, 3, "COUNT should be 3");
        assert_eq!(sum_val, 60, "SUM should be 60");

        unsafe { drop(Box::from_raw(tr_out_ch as *mut CursorHandle)); }
        unsafe { drop(Box::from_raw(tr_in_ch as *mut CursorHandle)); }
        trace_out_table.close();
        trace_in_table.close();
    }

    // ── Fix regression: identity MAP (null func_ptr) ─────────────────────

    #[test]
    fn test_map_identity_null_func_ptr() {
        // When the compiler emits a pass-through MAP (null func_ptr), the VM
        // must clone the batch without dereferencing the null pointer.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new(2);
        builder.add_map(0, 1, std::ptr::null(), schema, -1);
        builder.add_halt();

        let input = make_batch(schema, &[(1, 0, 1, 10), (2, 0, 1, 20)]);

        let reg_schemas = [schema; 2];
        let reg_kinds = [0u8; 2];
        let vm = builder.build(&reg_schemas, &reg_kinds);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(
            &vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[],
        )
        .unwrap()
        .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, 1, 10));
        assert_eq!(rows[1], (2, 1, 20));
    }

    // ── Fix regression: stale external cursor in bind_cursors ─────────────

    #[test]
    fn test_bind_cursors_clears_stale_external_cursor() {
        // An external trace register that supplies a cursor in epoch N must NOT
        // retain a dangling pointer in epoch N+1 if the caller passes null.
        // bind_cursors must unconditionally null out external trace registers
        // when no handle is provided.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("stale_cursor_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(), "ext", schema, 0, 1 << 20, false,
        )
        .unwrap();

        // reg 0 = input delta (kind 0), reg 1 = external trace (kind 1), reg 2 = output delta
        let mut builder = ProgramBuilder::new(3);
        builder.add_distinct(0, 1, 2, std::ptr::null_mut()); // hist_table_idx = -1 (no ingest)
        builder.add_halt();

        let reg_schemas = [schema; 3];
        let reg_kinds = [0u8, 1u8, 0u8];
        let mut vm = *builder.build(&reg_schemas, &reg_kinds);

        // Epoch 1: supply a real cursor for reg 1.
        let cursor1 = table.create_cursor().unwrap();
        let ch1 = Box::into_raw(Box::new(cursor1)) as *mut libc::c_void;
        let cursors1 = vec![std::ptr::null_mut(), ch1, std::ptr::null_mut()];
        execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[]),
            0,
            2,
            &cursors1,
            &[],
        )
        .unwrap();
        unsafe { drop(Box::from_raw(ch1 as *mut CursorHandle)) };
        // ch1 is now freed; reg 1's cursor_ptr would be dangling if not cleared.

        // Epoch 2: pass null for the external trace register.
        // bind_cursors must write null, not preserve the freed pointer.
        let cursors2 = vec![std::ptr::null_mut(); 3];
        execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[]),
            0,
            2,
            &cursors2,
            &[],
        )
        .unwrap();

        // If the stale pointer were preserved, reading reg 1's cursor_ptr inside
        // execute_epoch would be UB / crash. Reaching here means it was correctly
        // cleared to null.
        assert!(
            vm.regfile.registers[1].cursor_ptr.is_null(),
            "external trace register must be null after epoch with null handle"
        );

        table.close();
    }
}
