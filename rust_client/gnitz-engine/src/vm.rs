//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Replaces the RPython interpreter (interpreter.py) dispatch loop.
//! One FFI call per epoch instead of ~20 (one per opcode).

use crate::compact::SchemaDescriptor;
use crate::memtable::OwnedBatch;
use crate::ops::{self, AggDescriptor, GiDesc, AviDesc};
use crate::read_cursor::{self, CursorHandle, ReadCursor};
use crate::scalar_func::ScalarFuncKind;
use crate::table::Table;

use std::sync::Arc;

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// One VM instruction with all operator-specific data pre-resolved.
pub enum Instr {
    Halt,
    ClearDeltas,
    Delay { src: u16, dst: u16 },
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
// Program
// ---------------------------------------------------------------------------

/// Register kind: delta (transient) or trace (persistent cursor + table).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RegisterKind {
    Delta,
    Trace,
}

/// Per-register metadata.
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
// that owns the plan).  Raw pointers into RPython-managed ScalarFuncKind,
// Table, and ExprProgram are stable for the lifetime of the plan.
unsafe impl Send for Program {}
unsafe impl Sync for Program {}

// ---------------------------------------------------------------------------
// Register file (runtime state)
// ---------------------------------------------------------------------------

/// Runtime state for one register.
pub struct Register {
    pub kind: RegisterKind,
    pub schema: SchemaDescriptor,
    /// Delta: current batch.  Trace: unused (empty).
    pub batch: OwnedBatch,
    /// Trace: current cursor.  Borrowed from RPython each epoch.  Delta: None.
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

    /// Clear delta batches.  Cursor refresh is done by RPython before entering
    /// the Rust VM (it knows about Table vs PartitionedTable).
    pub fn clear_delta_batches(&mut self) {
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

    /// Bind cursor handles from RPython into trace registers.
    /// Each non-null handle is borrowed for the duration of the epoch.
    pub fn bind_cursors(&mut self, handles: &[*mut libc::c_void]) {
        for (i, reg) in self.registers.iter_mut().enumerate() {
            if reg.kind == RegisterKind::Trace && i < handles.len() && !handles[i].is_null() {
                reg.cursor_ptr = handles[i] as *mut CursorHandle<'static>;
            } else {
                reg.cursor_ptr = std::ptr::null_mut();
            }
        }
    }

    /// Mid-program clear: only clears delta batches, does NOT refresh cursors.
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
) -> Result<Option<OwnedBatch>, i32> {
    gnitz_debug!("vm: execute_epoch input_count={} input_reg={} output_reg={} instrs={}",
        input_batch.count, input_reg, output_reg, program.instructions.len());

    // 1. Clear delta batches (cursor refresh already done by RPython)
    regfile.clear_delta_batches();

    // 2. Bind cursors from RPython and input batch
    regfile.bind_cursors(cursor_handles);
    regfile.registers[input_reg as usize].batch = input_batch;

    // Raw pointer to the register array.  All instructions access distinct
    // registers (guaranteed by topological sort), so aliased &/&mut access
    // through different indices is safe.
    let regs = regfile.registers.as_mut_ptr();
    let nregs = regfile.registers.len();

    // Helper macros for safe indexed access via raw pointer.
    macro_rules! reg {
        ($i:expr) => {{ debug_assert!(($i as usize) < nregs); unsafe { &*regs.add($i as usize) } }};
    }
    macro_rules! reg_mut {
        ($i:expr) => {{ debug_assert!(($i as usize) < nregs); unsafe { &mut *regs.add($i as usize) } }};
    }
    macro_rules! cursor_mut {
        ($i:expr) => {{
            let r = reg_mut!($i);
            if r.cursor_ptr.is_null() {
                None
            } else {
                Some(&mut unsafe { &mut *r.cursor_ptr }.cursor)
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

            Instr::Delay { src, dst } => {
                let npc = reg!(*src).batch.col_data.len();
                let taken = std::mem::replace(&mut reg_mut!(*src).batch, OwnedBatch::empty(npc));
                reg_mut!(*dst).batch = taken;
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
                        cursor.seek(key_lo, key_hi);
                    }
                }
            }

            Instr::Filter { in_reg, out_reg, func_idx } => {
                let func_ptr = program.funcs[*func_idx as usize];
                let in_batch = &reg!(*in_reg).batch;
                let schema = reg!(*in_reg).schema;
                let result = if func_ptr.is_null() {
                    // NullPredicate: pass all rows unchanged
                    let mut out = OwnedBatch::empty(in_batch.col_data.len());
                    out.schema = Some(schema);
                    out.append_batch(in_batch, 0, in_batch.count);
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
                let func = unsafe { &*program.funcs[*func_idx as usize] };
                let in_schema = reg!(*in_reg).schema;
                let out_schema = &program.schemas[*out_schema_idx as usize];
                let result = ops::op_map(&reg!(*in_reg).batch, func, &in_schema, out_schema, *reindex_col);
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

                // trace_in cursor (from register file — passed by RPython)
                let ti_cursor_ptr: *mut ReadCursor = if *trace_in_reg >= 0 {
                    cursor_mut!(*trace_in_reg).map(|c| c as *mut ReadCursor).unwrap_or(std::ptr::null_mut())
                } else {
                    std::ptr::null_mut()
                };

                // trace_out cursor (from register file — passed by RPython)
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
                    .map(|ch| &mut ch.cursor);
                let mut gi_opt: Option<&mut ReadCursor> = gi_cursor_handle.as_deref_mut()
                    .map(|ch| &mut ch.cursor);

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
