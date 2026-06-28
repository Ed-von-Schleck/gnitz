//! DBSP VM: executes compiled circuit programs entirely in Rust.

use crate::expr::ScalarFunc;
use crate::ops::AggDescriptor;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, CursorHandle, Table};

mod builder;
mod exec;

pub(crate) use builder::ProgramBuilder;
pub(crate) use exec::{execute_epoch, execute_epoch_multi};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// One VM instruction with all operator-specific data pre-resolved.
pub(crate) enum Instr {
    Halt,
    ClearDeltas,
    Delay {
        src: u16,
        state_reg: u16,
        dst: u16,
    },
    ScanTrace {
        trace_reg: u16,
        out_reg: u16,
        chunk_limit: i32,
    },
    SeekTrace {
        trace_reg: u16,
        key_reg: u16,
    },
    Filter {
        in_reg: u16,
        out_reg: u16,
        func_idx: u16,
    },
    Map {
        in_reg: u16,
        out_reg: u16,
        func_idx: u16,
        out_schema_idx: u16,
        reindex_off: u32,
        reindex_cnt: u16,
        reindex_hash: bool,
        branch_id: u8,
    },
    Negate {
        in_reg: u16,
        out_reg: u16,
    },
    Union {
        in_a: u16,
        in_b: u16,
        has_b: bool,
        out_reg: u16,
    },
    Distinct {
        in_reg: u16,
        hist_reg: u16,
        out_reg: u16,
        hist_table_idx: i16,
    },
    JoinDT {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
        right_schema_idx: u16,
    },
    JoinDD {
        a_reg: u16,
        b_reg: u16,
        out_reg: u16,
        right_schema_idx: u16,
    },
    JoinDTOuter {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
        right_schema_idx: u16,
    },
    JoinDTRange {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
        right_schema_idx: u16,
        n_eq: u8,
        rel: gnitz_wire::RangeRel,
    },
    PartitionFilter {
        in_reg: u16,
        out_reg: u16,
        worker_id: u32,
        num_workers: u32,
    },
    AntiJoinDT {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
    },
    AntiJoinDD {
        a_reg: u16,
        b_reg: u16,
        out_reg: u16,
    },
    SemiJoinDT {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
    },
    SemiJoinDD {
        a_reg: u16,
        b_reg: u16,
        out_reg: u16,
    },
    NullExtend {
        in_reg: u16,
        out_reg: u16,
        right_schema_idx: u16,
    },
    Integrate {
        in_reg: u16,
        table_idx: i32, // index into Program::tables, -1 = no target (sink)
        gi: Option<Gi>,
        avi: Option<IntegrateAvi>,
    },
    Reduce {
        in_reg: u16,
        trace_in_reg: Option<u16>,
        trace_out_reg: u16,
        out_reg: u16,
        fin_out_reg: Option<u16>,
        agg_descs_offset: u32,
        agg_descs_count: u16,
        group_cols_offset: u32,
        group_cols_count: u16,
        output_schema_idx: u16,
        // GI/AVI cursors are created fresh from their tables each tick; `None`
        // means the operator has no GI/AVI index.
        gi: Option<Gi>,
        avi: Option<ReduceAvi>,
        finalize_func_idx: Option<u16>,
        finalize_schema_idx: Option<u16>,
        // Global-aggregate ground machinery (see `op_reduce`). `global_ground` is
        // the planner's SQL-intent discriminator; `i_am_owner` is baked per-worker
        // at emit time (`replicated || worker_rank() == owner(V₀)`). Both default
        // off for every grouped reduce.
        global_ground: bool,
        i_am_owner: bool,
    },
    GatherReduce {
        in_reg: u16,
        trace_out_reg: u16,
        out_reg: u16,
        agg_descs_offset: u32,
        agg_descs_count: u16,
    },
}

/// GI (group-index) reference: a GI-table slot + the indexed column. Shared by the
/// Integrate and Reduce instructions (the descriptor is byte-identical in both).
pub(crate) struct Gi {
    pub table_idx: u16,
    pub col_idx: u32,
}

/// AVI descriptor embedded in an Integrate instruction.
pub(crate) struct IntegrateAvi {
    pub table_idx: u16,
    pub for_max: bool,
    pub agg_col_type_code: u8,
    pub group_cols_offset: u32,
    pub group_cols_count: u16,
    pub agg_col_idx: u32,
}

/// AVI descriptor embedded in a Reduce instruction. Deliberately narrower than
/// [`IntegrateAvi`]: the Reduce path uses the reduce's own group_cols and has no
/// agg_col_idx, so those three `IntegrateAvi` fields are omitted.
pub(crate) struct ReduceAvi {
    pub table_idx: u16,
    pub for_max: bool,
    pub agg_col_type_code: u8,
}

/// Opaque handle owning a compiled program and its register file.
///
/// When produced by the Rust compiler (`compile_view`), `owned_tables`,
/// `owned_funcs`, and `owned_expr_progs` hold heap resources that
/// `program.tables` / `program.funcs` / `program.expr_progs` borrow via
/// raw pointers.  Rust drop order (declaration order) ensures `program`
/// drops before the owned vecs, so dangling pointers are never chased.
///
/// The compile-time assertions below are machine-checked: reordering any
/// owned vec before `program` produces a compile error, not a use-after-free.
#[allow(clippy::vec_box)]
pub(crate) struct VmHandle {
    pub program: Program,
    pub regfile: RegisterFile,
    /// Cursor handles for owned trace registers, kept alive across the epoch.
    /// Indexed in parallel with `owned_trace_regs`. Cursor destructors
    /// dereference the `Table` they were opened against, so this MUST drop
    /// before `owned_tables`.
    owned_cursor_handles: Vec<Option<Box<CursorHandle>>>,
    /// Child tables created during compilation (history, reduce-in, GI, AVI).
    /// `program.tables` may point into these.  Dropped AFTER `program`.
    pub owned_tables: Vec<Box<Table>>,
    /// Scalar functions created during compilation.
    /// `program.funcs` may point into these.  Dropped AFTER `program`.
    #[allow(dead_code)]
    pub owned_funcs: Vec<Box<ScalarFunc>>,
    /// Expression programs created during compilation.
    /// `program.expr_progs` may point into these.  Dropped AFTER `program`.
    #[allow(dead_code)]
    pub owned_expr_progs: Vec<Box<crate::expr::ResolvedProgram>>,
    /// Trace registers backed by owned tables: `(reg_id, index into owned_tables)`.
    /// `execute_epoch` creates cursors from these before dispatch.
    pub owned_trace_regs: Vec<(u16, usize)>,
}

// Compile-time proof that `program` precedes all owned resource vecs, so
// Rust's declaration-order drop visits `program` before any vec it borrows.
const _: () = assert!(std::mem::offset_of!(VmHandle, program) < std::mem::offset_of!(VmHandle, owned_tables));
const _: () = assert!(std::mem::offset_of!(VmHandle, program) < std::mem::offset_of!(VmHandle, owned_funcs));
const _: () = assert!(std::mem::offset_of!(VmHandle, program) < std::mem::offset_of!(VmHandle, owned_expr_progs));
// Cursor destructors dereference their owning `Table`; cursors MUST drop first.
const _: () =
    assert!(std::mem::offset_of!(VmHandle, owned_cursor_handles) < std::mem::offset_of!(VmHandle, owned_tables));

impl VmHandle {
    /// Compact owned tables and create fresh cursors for owned trace registers.
    /// Must be called before `execute_epoch` for compiler-produced plans.
    /// The cursor handles are stored in `owned_cursor_handles` and their
    /// raw pointers bound into the register file.
    pub fn refresh_owned_cursors(&mut self) {
        gnitz_debug!(
            "vm: refresh_owned_cursors, {} owned trace regs",
            self.owned_trace_regs.len()
        );
        // Resize storage if needed
        if self.owned_cursor_handles.len() < self.owned_trace_regs.len() {
            self.owned_cursor_handles
                .resize_with(self.owned_trace_regs.len(), || None);
        }
        for (slot, &(reg_id, table_idx)) in self.owned_trace_regs.iter().enumerate() {
            // Drop previous cursor before creating new one (releases shard refs etc.)
            self.owned_cursor_handles[slot] = None;

            // SAFETY: table_idx is valid (set during compilation). We need &mut
            // to the table, but we also hold &self.owned_trace_regs. This is safe
            // because owned_trace_regs is not modified here, and the table is
            // accessed through owned_tables which is a separate field.
            let table: &mut Table = unsafe { &mut *(&mut *self.owned_tables[table_idx] as *mut Table) };
            // Operator-state read path: keep compacting so L0 on owned trace
            // tables stays bounded (no background compactor yet). Not a
            // validator, so a compaction Err safely degrades to a null cursor
            // pointer.
            #[allow(clippy::disallowed_methods)] // explicit maintenance: owned-trace operator state
            match table.create_cursor_compacting() {
                Ok(ch) => {
                    // Store the Box into its slot first, then derive the
                    // pointer from the slot — taking the pointer before the
                    // move raises Stacked Borrows aliasing questions even
                    // though the heap address is stable across the move.
                    self.owned_cursor_handles[slot] = Some(Box::new(ch));
                    let ptr = self.owned_cursor_handles[slot].as_mut().unwrap().as_mut() as *mut CursorHandle;
                    self.regfile.registers[reg_id as usize].cursor_ptr = ptr;
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
pub(crate) enum RegisterKind {
    Delta,
    Trace,
    DelayState,
}

/// Per-register metadata.
#[derive(Clone, Copy)]
pub(crate) struct RegisterMeta {
    pub schema: SchemaDescriptor,
    pub kind: RegisterKind,
}

impl RegisterMeta {
    pub const fn delta(schema: SchemaDescriptor) -> Self {
        Self {
            schema,
            kind: RegisterKind::Delta,
        }
    }
    pub const fn trace(schema: SchemaDescriptor) -> Self {
        Self {
            schema,
            kind: RegisterKind::Trace,
        }
    }
    pub const fn delay_state(schema: SchemaDescriptor) -> Self {
        Self {
            schema,
            kind: RegisterKind::DelayState,
        }
    }
}

/// A compiled DBSP program ready for execution.
pub(crate) struct Program {
    pub instructions: Vec<Instr>,
    pub reg_meta: Vec<RegisterMeta>,
    /// Shared resource arrays — referenced by index from instructions.
    pub funcs: Vec<*const ScalarFunc>,
    pub tables: Vec<*mut Table>,
    pub schemas: Vec<SchemaDescriptor>,
    pub agg_descs: Vec<AggDescriptor>,
    pub group_cols: Vec<u32>,
    pub reindex_cols: Vec<u32>,
    /// Parallel to `reindex_cols` (same offsets): per-column carried promotion
    /// target tc (`0` = derive from source).
    pub reindex_target_tcs: Vec<u8>,
    pub expr_progs: Vec<*const crate::expr::ResolvedProgram>,
}

// SAFETY: Program is only accessed from a single thread (the worker thread
// that owns the plan).  Raw pointers into ScalarFunc,
// Table, and ExprProgram are stable for the lifetime of the plan.
unsafe impl Send for Program {}

// ---------------------------------------------------------------------------
// Register file (runtime state)
// ---------------------------------------------------------------------------

/// Runtime state for one register.
pub(crate) struct Register {
    pub kind: RegisterKind,
    pub schema: SchemaDescriptor,
    /// Delta: current batch.  Trace: unused (empty).
    pub batch: Batch,
    /// Trace: current cursor.  Borrowed each epoch.  Delta: None.
    pub cursor_ptr: *mut CursorHandle,
}

/// Collection of registers for a single plan execution.
pub(crate) struct RegisterFile {
    pub registers: Vec<Register>,
}

impl RegisterFile {
    /// Create from register metadata.  Allocates empty batches and null cursors.
    pub fn new(metas: &[RegisterMeta]) -> Self {
        let mut registers = Vec::with_capacity(metas.len());
        for m in metas {
            let batch = if m.schema.num_columns() > 0 {
                Batch::with_schema(m.schema, if m.kind == RegisterKind::Delta { 16 } else { 0 })
            } else {
                Batch::empty(0, 16)
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

    /// Bind cursor handles into trace registers.
    /// Each non-null handle is borrowed for the duration of the epoch.
    /// Owned trace registers (listed in `owned_trace_reg_ids`, already set by
    /// `refresh_owned_cursors`) are skipped entirely. External trace registers
    /// are always written from `handles` — stale pointers from prior epochs are
    /// never preserved.
    pub fn bind_cursors(&mut self, handles: &[*mut libc::c_void], owned_trace_reg_ids: &[(u16, usize)]) {
        for (i, reg) in self.registers.iter_mut().enumerate() {
            if reg.kind == RegisterKind::Trace {
                let is_owned = owned_trace_reg_ids.iter().any(|&(rid, _)| rid as usize == i);
                if is_owned {
                    // Cursor was set by refresh_owned_cursors; leave it alone.
                } else if i < handles.len() && !handles[i].is_null() {
                    reg.cursor_ptr = handles[i] as *mut CursorHandle;
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
            if reg.kind == RegisterKind::Delta && reg.schema.num_columns() > 0 {
                reg.batch.clear();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{AggDescriptor, AggOp};
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, TypeCode};

    #[test]
    fn test_clear_deltas_clears_only_delta_registers() {
        // clear_deltas (used by DagEngine::clear_view_regfile_deltas
        // after backfill) must empty Delta registers while leaving Trace
        // registers untouched.
        let schema = schema_1i64();
        let mut delta_batch = Batch::with_schema(schema, 1);
        delta_batch.extend_pk(1u128);
        delta_batch.extend_weight(&1i64.to_le_bytes());
        delta_batch.extend_null_bmp(&0u64.to_le_bytes());
        delta_batch.extend_col(0, &10i64.to_le_bytes());
        delta_batch.count += 1;

        let mut trace_batch = Batch::with_schema(schema, 1);
        trace_batch.extend_pk(2u128);
        trace_batch.extend_weight(&1i64.to_le_bytes());
        trace_batch.extend_null_bmp(&0u64.to_le_bytes());
        trace_batch.extend_col(0, &20i64.to_le_bytes());
        trace_batch.count += 1;

        let mut rf = RegisterFile {
            registers: vec![
                Register {
                    kind: RegisterKind::Delta,
                    schema,
                    batch: delta_batch,
                    cursor_ptr: std::ptr::null_mut(),
                },
                Register {
                    kind: RegisterKind::Trace,
                    schema,
                    batch: trace_batch,
                    cursor_ptr: std::ptr::null_mut(),
                },
            ],
        };
        assert_eq!(rf.registers[0].batch.count, 1);
        rf.clear_deltas();
        assert_eq!(rf.registers[0].batch.count, 0, "Delta register must be cleared");
        assert_eq!(rf.registers[1].batch.count, 1, "Trace register must be preserved");
    }

    // ── Test helpers ─────────────────────────────────────────────────────

    fn make_schema(col_types: &[u8]) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
        // Column 0 is always the U128 PK
        columns[0] = SchemaColumn::new(type_code::U128, 0);
        for (i, &tc) in col_types.iter().enumerate() {
            columns[i + 1] = SchemaColumn::new(tc, 0);
        }
        let n = col_types.len() + 1;
        SchemaDescriptor::new(&columns[..n], &[0])
    }

    /// Create a schema with one I64 payload column.
    fn schema_1i64() -> SchemaDescriptor {
        make_schema(&[type_code::I64])
    }

    /// Create a batch from (pk, weight, col0_i64) tuples.
    fn make_batch(schema: SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(schema, n);
        for &(pk, w, c0) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c0.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Create a batch with two I64 payload columns.
    fn make_batch_2col(schema: SchemaDescriptor, rows: &[(u128, i64, i64, i64)]) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(schema, n);
        for &(pk, w, c0, c1) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c0.to_le_bytes());
            b.extend_col(1, &c1.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Extract rows from a batch as (pk, weight, col0_i64) tuples.
    fn extract_rows(b: &Batch) -> Vec<(u64, i64, i64)> {
        let mut rows = Vec::new();
        for i in 0..b.count {
            let pk = b.get_pk(i) as u64;
            let w = i64::from_le_bytes(b.weight_data()[i * 8..(i + 1) * 8].try_into().unwrap());
            let c0 = i64::from_le_bytes(b.col_data(0)[i * 8..(i + 1) * 8].try_into().unwrap());
            rows.push((pk, w, c0));
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

        // Predicate: col[1] > 0  (col[1] is the I64 payload at logical index 1)
        let pred_instrs = vec![
            crate::expr::LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col[1]
            crate::expr::LogicalInstr::LoadConst { dst: 1, val: 0 },  // r1 = 0
            crate::expr::LogicalInstr::Cmp {
                op: crate::expr::CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            }, // r2 = r0 > r1
        ];
        let pred_prog = crate::expr::LogicalProgram::new(pred_instrs, 3, 2, vec![]);
        let func = Box::new(crate::expr::ScalarFunc::from_predicate(pred_prog, &schema));
        let func_ptr = Box::into_raw(func) as *const ScalarFunc;

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, func_ptr);
        builder.add_negate(1, 2);
        builder.add_halt();

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, -5), (3u128, 1, 20)]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, -1, 10));
        assert_eq!(rows[1], (3, -1, 20));

        // Cleanup
        unsafe {
            drop(Box::from_raw(func_ptr as *mut ScalarFunc));
        }
    }

    #[test]
    fn test_ghost_property() {
        // Input with opposing weights for same PK: consolidated to zero → None.
        // We pre-consolidate manually (input is consolidated before sending to VM).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        // Consolidated empty batch (ghost elimination already applied)
        let input = make_batch(schema, &[]);

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[]).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_empty_input() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        let input = make_batch(schema, &[]);

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[]).unwrap();

        assert!(result.is_none());
    }

    // ── Delay (z⁻¹) tests ───────────────────────────────────────────────
    //
    // Register layout for all single-delay tests:
    //   reg 0: Delta     (src / input_reg)
    //   reg 1: DelayState (state_reg)
    //   reg 2: Delta     (dst / output_reg)

    #[test]
    fn test_delay_correct_temporal_semantics() {
        // At tick t, output must be the input from tick t-1 (z⁻¹ semantics).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];

        // Tick 0: z⁻¹[0] = 0 → None
        let input0 = make_batch(schema, &[(1u128, 1, 10)]);
        let r0 = execute_epoch(&vm.program, &mut vm.regfile, input0, 0, 2, &cursors, &[]).unwrap();
        assert!(r0.is_none(), "tick 0 must return None (z⁻¹[0] = 0)");

        // Tick 1: output = tick 0's input
        let input1 = make_batch(schema, &[(2u128, 1, 20)]);
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();
        let rows1 = extract_rows(&r1);
        assert_eq!(rows1, vec![(1, 1, 10)]);

        // Tick 2: output = tick 1's input
        let input2 = make_batch(schema, &[(3u128, 1, 30)]);
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();
        let rows2 = extract_rows(&r2);
        assert_eq!(rows2, vec![(2, 1, 20)]);
    }

    #[test]
    fn test_delay_empty_first_tick() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
        ];
        let vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];

        let input = make_batch(schema, &[(1u128, 1, 99)]);
        let r = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 2, &cursors, &[]).unwrap();
        assert!(r.is_none(), "tick 0 must always return None regardless of input");
    }

    #[test]
    fn test_delay_empty_intermediate_tick() {
        // An empty input is stored and replayed correctly.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];

        // Tick 0: non-empty input → None
        let r0 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(1u128, 1, 10)]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap();
        assert!(r0.is_none());

        // Tick 1: empty input → tick 0's input replayed
        let r1 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(extract_rows(&r1), vec![(1, 1, 10)]);

        // Tick 2: non-empty input → empty (tick 1's empty was stored)
        let r2 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(2u128, 1, 20)]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap();
        assert!(r2.is_none(), "tick 2 output should be empty (tick 1 input was empty)");

        // Tick 3: empty input → tick 2's input replayed
        let r3 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(extract_rows(&r3), vec![(2, 1, 20)]);
    }

    #[test]
    fn test_delay_preserves_negative_weights() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];

        // Tick 0: insertion → None
        let r0 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(1u128, 1, 10)]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap();
        assert!(r0.is_none());

        // Tick 1: retraction → tick 0's insertion replayed
        let r1 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(1u128, -1, 10)]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(extract_rows(&r1), vec![(1, 1, 10)]);

        // Tick 2: empty → tick 1's retraction replayed
        let r2 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[]),
            0,
            2,
            &cursors,
            &[],
        )
        .unwrap()
        .unwrap();
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

        let mut builder = ProgramBuilder::new();
        builder.add_delay(0, 1, 2);
        builder.add_delay(2, 3, 4);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 5];

        // Tick 0: no output yet
        let r0 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(1u128, 1, 10)]),
            0,
            4,
            &cursors,
            &[],
        )
        .unwrap();
        assert!(r0.is_none());

        // Tick 1: still no output (need 2 ticks of delay)
        let r1 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(2u128, 1, 20)]),
            0,
            4,
            &cursors,
            &[],
        )
        .unwrap();
        assert!(r1.is_none());

        // Tick 2: tick 0's input arrives
        let r2 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[(3u128, 1, 30)]),
            0,
            4,
            &cursors,
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(extract_rows(&r2), vec![(1, 1, 10)]);

        // Tick 3: empty input → tick 1's input arrives
        let r3 = execute_epoch(
            &vm.program,
            &mut vm.regfile,
            make_batch(schema, &[]),
            0,
            4,
            &cursors,
            &[],
        )
        .unwrap()
        .unwrap();
        assert_eq!(extract_rows(&r3), vec![(2, 1, 20)]);
    }

    #[test]
    fn test_delay_state_not_cleared_between_ticks() {
        // Directly verify that DelayState persists: after tick 0, state_reg holds
        // the input; after tick 1, it holds the new input.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_delay(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delay_state(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];

        let input0 = make_batch(schema, &[(1u128, 1, 10)]);
        execute_epoch(&vm.program, &mut vm.regfile, input0, 0, 2, &cursors, &[]).unwrap();
        assert_eq!(
            vm.regfile.registers[1].batch.count, 1,
            "state_reg must hold tick 0's input"
        );

        let input1 = make_batch(schema, &[(2u128, 1, 20), (3u128, 1, 30)]);
        execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2, &cursors, &[]).unwrap();
        assert_eq!(
            vm.regfile.registers[1].batch.count, 2,
            "state_reg must hold tick 1's input"
        );
    }

    #[test]
    fn test_union_operator() {
        // Union combines two batches. Use a pipeline: filter input into reg 1,
        // then delay input into reg 0 copy, then union reg 0 + reg 1.
        // Simpler: just union reg 0 with has_b=false (single input mode).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        // Union with has_b=false: effectively a pass-through (copies batch_a only)
        builder.add_union(0, 0, false, 1);
        builder.add_halt();

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20)]);

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();

        assert_eq!(result.count, 2);
    }

    // Item 7: Union with in_a == in_b is Z + Z and must double every weight.
    // The naive by-value path moves batch_a out then reads an emptied batch_b,
    // producing +1 instead of +2.
    #[test]
    fn test_self_union_doubles_weights() {
        let schema = schema_1i64();
        let mut builder = ProgramBuilder::new();
        builder.add_union(0, 0, true, 1);
        builder.add_halt();
        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 3, 20)]);
        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();
        let rows = extract_rows(&result);
        assert_eq!(
            rows,
            vec![(1, 2, 10), (2, 6, 20)],
            "self-union (Z + Z) must double every weight",
        );
    }

    // Item 9a: JoinDTOuter with an absent trace cursor must consolidate the
    // delta before null-extending; downstream operators assert consolidated
    // input.
    #[test]
    fn test_join_dt_outer_absent_trace_consolidates() {
        let schema = schema_1i64();
        let mut input = make_batch(schema, &[(2u128, 1, 20), (2u128, 1, 20)]);
        input.sorted = false;
        input.consolidated = false;
        let mut builder = ProgramBuilder::new();
        builder.add_join_dt_outer(0, 1, 2, schema);
        builder.add_halt();
        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();
        assert!(
            result.consolidated,
            "absent-trace outer-join output must be consolidated",
        );
    }

    // Item 9b: AntiJoinDT with an absent trace cursor must consolidate the
    // pass-through delta, not forward it raw.
    #[test]
    fn test_anti_join_dt_absent_trace_consolidates() {
        let schema = schema_1i64();
        let mut input = make_batch(schema, &[(2u128, 1, 20), (2u128, 1, 20)]);
        input.sorted = false;
        input.consolidated = false;
        let mut builder = ProgramBuilder::new();
        builder.add_anti_join_dt(0, 1, 2);
        builder.add_halt();
        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 3];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();
        assert!(
            result.consolidated,
            "absent-trace anti-join output must be consolidated",
        );
    }

    #[test]
    fn test_input_already_consolidated() {
        // Pre-consolidated input should not cause extra work.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20)]);

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, 1, 10));
        assert_eq!(rows[1], (2, 1, 20));
    }

    #[test]
    fn test_delta_isolation_across_ticks() {
        // Two execute_epoch calls: second tick should not see first tick's data.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];

        // Tick 1
        let input1 = make_batch(schema, &[(1u128, 1, 10)]);
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();
        assert_eq!(r1.count, 1);

        // Tick 2 with different data
        let input2 = make_batch(schema, &[(2u128, 1, 20), (3u128, 1, 30)]);
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();
        // Should have exactly 2 rows from tick 2, not 3 (no bleed from tick 1)
        assert_eq!(r2.count, 2);
        let rows = extract_rows(&r2);
        assert_eq!(rows[0].0, 2);
        assert_eq!(rows[1].0, 3);
    }

    #[test]
    fn test_map_operator() {
        // MAP with ScalarFunc projection: reorder/select columns.
        let in_schema = make_schema(&[type_code::I64, type_code::I64]);
        let out_schema = make_schema(&[type_code::I64]);

        // MAP with ScalarFunc projection: reorder/select columns.
        let func = Box::new(crate::expr::ScalarFunc::from_projection(
            &[2],
            &[type_code::I64],
            &in_schema,
            &out_schema,
        ));
        let func_ptr = Box::into_raw(func) as *const ScalarFunc;

        let mut builder = ProgramBuilder::new();
        builder.add_map(0, 1, func_ptr, out_schema, &[], &[], false, 0);
        builder.add_halt();

        let input = make_batch_2col(in_schema, &[(1u128, 1, 10, 100), (2u128, 1, 20, 200)]);

        let reg_meta = [RegisterMeta::delta(in_schema), RegisterMeta::delta(out_schema)];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        // Projected column should be the second payload col (100, 200)
        assert_eq!(rows[0].2, 100);
        assert_eq!(rows[1].2, 200);

        unsafe {
            drop(Box::from_raw(func_ptr as *mut ScalarFunc));
        }
    }

    #[test]
    fn test_distinct_multi_tick() {
        // DISTINCT clamps weights: +3 → +1, -1 → 0 (stays positive → no retraction).
        // Uses a real Table for history.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("dist_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "hist",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        let table_ptr = &mut table as *mut Table;

        let mut builder = ProgramBuilder::new();
        // reg 0 = input delta, reg 1 = history trace, reg 2 = output delta
        builder.add_distinct(0, 1, 2, table_ptr);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];

        let mut vm = *builder.build(&reg_meta);

        // Tick 1: insert pk=1 with weight +3 → distinct output should be +1
        let input1 = make_batch(schema, &[(1u128, 3, 42)]);
        // Need to create a cursor for the history register
        let cursor1 = table.open_cursor();
        let ch1 = Box::into_raw(Box::new(cursor1)) as *mut libc::c_void;
        let cursors1 = vec![std::ptr::null_mut(), ch1, std::ptr::null_mut()];

        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2, &cursors1, &[])
            .unwrap()
            .unwrap();

        let rows1 = extract_rows(&r1);
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0], (1, 1, 42)); // clamped to +1

        unsafe {
            drop(Box::from_raw(ch1 as *mut CursorHandle));
        }

        // Tick 2: delta w=-1, integral before tick = +3, after = +2 (still positive).
        // No boundary crossing → output should be empty.
        let cursor2 = table.open_cursor();
        let ch2 = Box::into_raw(Box::new(cursor2)) as *mut libc::c_void;
        let cursors2 = vec![std::ptr::null_mut(), ch2, std::ptr::null_mut()];
        let input2 = make_batch(schema, &[(1u128, -1, 42)]);
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 2, &cursors2, &[]).unwrap();
        assert!(r2.is_none(), "no boundary crossing: output should be empty");
        unsafe {
            drop(Box::from_raw(ch2 as *mut CursorHandle));
        }

        // Tick 3: delta w=-2, integral before tick = +2, after = 0 (non-positive).
        // Positive→non-positive boundary crossed → retraction: output pk=1 w=-1.
        let cursor3 = table.open_cursor();
        let ch3 = Box::into_raw(Box::new(cursor3)) as *mut libc::c_void;
        let cursors3 = vec![std::ptr::null_mut(), ch3, std::ptr::null_mut()];
        let input3 = make_batch(schema, &[(1u128, -2, 42)]);
        let r3 = execute_epoch(&vm.program, &mut vm.regfile, input3, 0, 2, &cursors3, &[])
            .unwrap()
            .unwrap();
        let rows3 = extract_rows(&r3);
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0], (1, -1, 42)); // retraction
        unsafe {
            drop(Box::from_raw(ch3 as *mut CursorHandle));
        }

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
        let join_schema = make_schema(&[type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("join_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "trace",
            right_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        // Ingest trace data
        let trace_batch = make_batch(right_schema, &[(10u128, 1, 200)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.open_cursor();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new();
        // reg 0 = left delta, reg 1 = right trace, reg 2 = output
        builder.add_join_dt(0, 1, 2, right_schema);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(left_schema),
            RegisterMeta::trace(right_schema),
            RegisterMeta::delta(join_schema),
        ];

        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        let input = make_batch(left_schema, &[(10u128, 1, 100)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        assert_eq!(result.count, 1);
        // Weight should be product: 1*1 = 1
        let w = i64::from_le_bytes(result.weight_data()[0..8].try_into().unwrap());
        assert_eq!(w, 1);
        // First payload col should be from left (100), second from right (200)
        let c0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        let c1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(c0, 100);
        assert_eq!(c1, 200);

        unsafe {
            drop(Box::from_raw(ch as *mut CursorHandle));
        }
        table.close();
    }

    #[test]
    fn test_join_delta_trace_multi_match() {
        // JoinDT: 1 delta row × N trace rows → N output rows.
        let left_schema = schema_1i64();
        let right_schema = schema_1i64();
        let join_schema = make_schema(&[type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("join_multi_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "trace",
            right_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        // Ingest 3 trace rows with same PK but different payloads
        let trace_batch = make_batch(right_schema, &[(10u128, 1, 100), (10u128, 1, 200), (10u128, 1, 300)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.open_cursor();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new();
        builder.add_join_dt(0, 1, 2, right_schema);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(left_schema),
            RegisterMeta::trace(right_schema),
            RegisterMeta::delta(join_schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        let input = make_batch(left_schema, &[(10u128, 2, 50)]); // weight=2
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        // Should produce 3 output rows (1 delta × 3 trace)
        assert_eq!(result.count, 3);
        // Each output weight = 2 * 1 = 2
        for i in 0..3 {
            let w = i64::from_le_bytes(result.weight_data()[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(w, 2);
        }

        unsafe {
            drop(Box::from_raw(ch as *mut CursorHandle));
        }
        table.close();
    }

    #[test]
    fn test_cursor_lifecycle() {
        // Table ingest + cursor creation + read.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cursor_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "test",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        let batch = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20), (3u128, 1, 30)]);
        table.ingest_owned_batch(batch).unwrap();

        let mut ch = table.open_cursor();
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

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_halt();

        // Already-consolidated input: weights were summed before VM entry
        let input = make_batch(schema, &[(1u128, 5, 42)]);

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();

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
            tdir.to_str().unwrap(),
            "trace",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        // Trace has pk=1 and pk=3
        let trace_batch = make_batch(schema, &[(1u128, 1, 100), (3u128, 1, 300)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.open_cursor();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new();
        builder.add_anti_join_dt(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        // Delta has pk=1, pk=2, pk=3. Only pk=2 should survive anti-join.
        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20), (3u128, 1, 30)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2); // only pk=2 survives

        unsafe {
            drop(Box::from_raw(ch as *mut CursorHandle));
        }
        table.close();
    }

    #[test]
    fn test_semi_join_dt() {
        // SemiJoinDT: keep delta rows that HAVE a match in trace.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("semijoin_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "trace",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        let trace_batch = make_batch(schema, &[(1u128, 1, 100), (3u128, 1, 300)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.open_cursor();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        let mut builder = ProgramBuilder::new();
        builder.add_semi_join_dt(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20), (3u128, 1, 30)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1); // pk=1 matches trace
        assert_eq!(rows[1].0, 3); // pk=3 matches trace

        unsafe {
            drop(Box::from_raw(ch as *mut CursorHandle));
        }
        table.close();
    }

    #[test]
    fn test_reduce_sum_multi_tick() {
        // REDUCE with SUM aggregation over a group column.
        // Uses real tables for trace_out and trace_in.
        let in_schema = make_schema(&[
            type_code::I64, // group col (payload col 0)
            type_code::I64, // agg col (payload col 1)
        ]);

        // Output schema: [U128 PK, I64 group_col, I64 sum_col, I64 count companion]
        let out_schema = make_schema(&[type_code::I64, type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();

        // trace_out table
        let tout_dir = dir.path().join("tr_out");
        let mut trace_out_table = Table::new(
            tout_dir.to_str().unwrap(),
            "tr_out",
            out_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        let trace_out_ptr = &mut trace_out_table as *mut Table;

        // trace_in table (for non-linear agg, but SUM is linear — still test the path)
        let tin_dir = dir.path().join("tr_in");
        let mut trace_in_table = Table::new(
            tin_dir.to_str().unwrap(),
            "tr_in",
            in_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        let trace_in_ptr = &mut trace_in_table as *mut Table;

        // Agg descriptors: SUM of payload col 1 (schema col 2), plus the trailing
        // Count cardinality companion every all-linear reduce carries.
        let agg_descs = [
            AggDescriptor {
                col_idx: 2,
                agg_op: AggOp::Sum,
                col_type_code: TypeCode::I64,
                _pad: [0; 2],
            },
            AggDescriptor {
                col_idx: 0,
                agg_op: AggOp::Count,
                col_type_code: TypeCode::I64,
                _pad: [0; 2],
            },
        ];
        let group_cols = [1u32]; // schema col 1 = payload col 0 (group key)

        let mut builder = ProgramBuilder::new();
        // reg 0 = input delta
        // reg 1 = trace_out (trace register for output)
        // reg 2 = raw_delta output
        // reg 3 = trace_in (trace register for input history)
        // reg 4 unused

        // REDUCE: reads from reg 0, trace_in=reg 3, trace_out=reg 1, output=reg 2
        builder.add_reduce(
            0,       // in_reg
            Some(3), // trace_in_reg
            1,       // trace_out_reg
            2,       // out_reg
            None,    // fin_out_reg (no finalize)
            &agg_descs,
            &group_cols,
            out_schema,
            std::ptr::null_mut(), // avi_table
            false,
            0,
            std::ptr::null_mut(), // gi_table
            0,
            std::ptr::null(), // finalize_prog
            std::ptr::null(), // finalize_schema
            false,
            false,
        );

        // INTEGRATE raw_delta → trace_out
        builder.add_integrate(
            2,             // in_reg (raw_delta)
            trace_out_ptr, // target table
            std::ptr::null_mut(),
            0, // no GI
            std::ptr::null_mut(),
            false,
            0,
            &[],
            0, // no AVI
        );

        // INTEGRATE input → trace_in
        builder.add_integrate(
            0,            // in_reg
            trace_in_ptr, // target table
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            false,
            0,
            &[],
            0,
        );

        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema),
            RegisterMeta::delta(out_schema),
            RegisterMeta::trace(in_schema),
            RegisterMeta::delta(in_schema),
        ];

        let mut vm = *builder.build(&reg_meta);

        // Tick 1: Insert group=1 with values 10, 20
        let input1 = make_batch_2col(
            in_schema,
            &[
                (1u128, 1, 1, 10), // pk=1, group=1, val=10
                (2u128, 1, 1, 20), // pk=2, group=1, val=20
            ],
        );

        // Create cursors for trace registers
        let tr_out_cursor = trace_out_table.open_cursor();
        let tr_out_ch = Box::into_raw(Box::new(tr_out_cursor)) as *mut libc::c_void;
        let tr_in_cursor = trace_in_table.open_cursor();
        let tr_in_ch = Box::into_raw(Box::new(tr_in_cursor)) as *mut libc::c_void;

        let cursors1 = vec![
            std::ptr::null_mut(), // reg 0: delta
            tr_out_ch,            // reg 1: trace_out
            std::ptr::null_mut(), // reg 2: delta
            tr_in_ch,             // reg 3: trace_in
            std::ptr::null_mut(), // reg 4
        ];

        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2, &cursors1, &[])
            .unwrap()
            .unwrap();

        // SUM of group=1: 10+20 = 30. Output should be one row with sum=30.
        assert_eq!(r1.count, 1, "one group → one output row");
        let sum_val = i64::from_le_bytes(r1.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(sum_val, 30, "SUM(10+20) must be 30");

        // Cleanup
        unsafe {
            drop(Box::from_raw(tr_out_ch as *mut CursorHandle));
        }
        unsafe {
            drop(Box::from_raw(tr_in_ch as *mut CursorHandle));
        }

        trace_out_table.close();
        trace_in_table.close();
    }

    // Item 41: a non-linear aggregate (MIN) requires trace_in to recompute on
    // retraction. If the trace_in cursor cannot be opened (null) while
    // trace_in_reg >= 0, the VM must abort with Err(-11) rather than silently
    // producing wrong aggregates.
    #[test]
    fn test_reduce_trace_in_null_cursor_aborts() {
        let in_schema = make_schema(&[type_code::I64, type_code::I64]);
        let out_schema = make_schema(&[type_code::I64, type_code::I64]);
        let agg_descs = [AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        }];
        let group_cols = [1u32];

        let mut builder = ProgramBuilder::new();
        builder.add_reduce(
            0,       // in_reg
            Some(3), // trace_in_reg (set, but its cursor will be null)
            1,       // trace_out_reg
            2,       // out_reg
            None,    // fin_out_reg
            &agg_descs,
            &group_cols,
            out_schema,
            std::ptr::null_mut(),
            false,
            0,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            false,
            false,
        );
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema),
            RegisterMeta::delta(out_schema),
            RegisterMeta::trace(in_schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        let input = make_batch_2col(in_schema, &[(1u128, 1, 1, 10)]);
        // All cursors null ⟹ trace_in cursor is null with trace_in_reg >= 0.
        let cursors = vec![std::ptr::null_mut(); 4];
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[]);
        assert!(
            matches!(result, Err(-11)),
            "null trace_in cursor must abort Reduce with Err(-11)",
        );
    }

    // Item 41 (companion): trace_out_reg always checked; if its cursor is null
    // the VM must return Err(-10). Disable trace_in (pass -1) so Err(-11) is
    // not reached first.
    #[test]
    fn test_reduce_trace_out_null_cursor_aborts() {
        let in_schema = make_schema(&[type_code::I64, type_code::I64]);
        let out_schema = make_schema(&[type_code::I64, type_code::I64]);
        let agg_descs = [AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        }];
        let group_cols = [1u32];

        let mut builder = ProgramBuilder::new();
        builder.add_reduce(
            0,    // in_reg
            None, // trace_in_reg disabled — avoids Err(-11) check
            1,    // trace_out_reg (cursor will be null → Err(-10))
            2,    // out_reg
            None, // fin_out_reg
            &agg_descs,
            &group_cols,
            out_schema,
            std::ptr::null_mut(),
            false,
            0,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            false,
            false,
        );
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema),
            RegisterMeta::delta(out_schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        let input = make_batch_2col(in_schema, &[(1u128, 1, 1, 10)]);
        let cursors = vec![std::ptr::null_mut(); 3];
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[]);
        assert!(
            matches!(result, Err(-10)),
            "null trace_out cursor must abort Reduce with Err(-10)",
        );
    }

    #[test]
    fn test_seek_trace_point_lookup() {
        // SeekTrace positions a cursor at a given key, then ScanTrace reads from it.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("seek_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "trace",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        let trace_batch = make_batch(schema, &[(1u128, 1, 10), (5u128, 1, 50), (10u128, 1, 100)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let cursor = table.open_cursor();
        let ch = Box::into_raw(Box::new(cursor)) as *mut libc::c_void;

        // Build: reg0=key input, reg1=trace, reg2=scan output
        let mut builder = ProgramBuilder::new();
        builder.add_seek_trace(1, 0); // Seek trace to key from reg 0
        builder.add_scan_trace(1, 2, 100); // Scan from trace into reg 2
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(), ch, std::ptr::null_mut()];

        // Input: a batch with pk=5 (used as seek key)
        let input = make_batch(schema, &[(5u128, 1, 0)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        // After seeking to pk=5, scan should find pk=5 and pk=10 (2 rows from pk=5 onwards)
        assert!(result.count >= 2);
        let pk0 = result.get_pk(0) as u64;
        assert_eq!(pk0, 5);

        unsafe {
            drop(Box::from_raw(ch as *mut CursorHandle));
        }
        table.close();
    }

    #[test]
    fn test_join_dd() {
        // JoinDD: join two delta batches. Use filter(null) to copy input into
        // both reg 1 and reg 2, then join them (self-join on same PK).
        let schema = schema_1i64();
        let join_schema = make_schema(&[type_code::I64, type_code::I64]);

        let mut builder = ProgramBuilder::new();
        // Copy input (reg 0) into reg 1 and reg 2 via null filters
        builder.add_filter(0, 1, std::ptr::null());
        builder.add_filter(0, 2, std::ptr::null());
        // Self-join: reg 1 × reg 2
        builder.add_join_dd(1, 2, 3, schema);
        builder.add_halt();

        let input = make_batch(schema, &[(10u128, 2, 100)]);

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::delta(schema),
            RegisterMeta::delta(schema),
            RegisterMeta::delta(join_schema),
        ];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 4];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 3, &cursors, &[])
            .unwrap()
            .unwrap();

        assert_eq!(result.count, 1);
        let w = i64::from_le_bytes(result.weight_data()[0..8].try_into().unwrap());
        assert_eq!(w, 4); // 2 * 2 (self-join weight multiplication)
        let c0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        let c1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(c0, 100);
        assert_eq!(c1, 100);
    }

    #[test]
    fn test_program_builder_resource_dedup() {
        // Verify that adding the same func/table pointer twice reuses the same index.
        let schema = schema_1i64();

        let pred_instrs = vec![
            crate::expr::LogicalInstr::LoadColInt { dst: 0, col: 1 },
            crate::expr::LogicalInstr::LoadConst { dst: 1, val: 0 },
            crate::expr::LogicalInstr::Cmp {
                op: crate::expr::CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            },
        ];
        let pred_prog = crate::expr::LogicalProgram::new(pred_instrs, 3, 2, vec![]);
        let func = Box::new(crate::expr::ScalarFunc::from_predicate(pred_prog, &schema));
        let func_ptr = Box::into_raw(func) as *const ScalarFunc;

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, func_ptr);
        builder.add_filter(1, 2, func_ptr); // same func — should reuse index
        builder.add_halt();

        let reg_meta = [RegisterMeta::delta(schema); 4];
        let vm = builder.build(&reg_meta);

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

        unsafe {
            drop(Box::from_raw(func_ptr as *mut ScalarFunc));
        }
    }

    /// Anti-join + semi-join should partition the input: |anti| + |semi| == |input|.
    #[test]
    fn test_anti_semi_complement() {
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("complement_test");
        let mut table = Table::new(
            tdir.to_str().unwrap(),
            "trace",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        // Trace has pk=1, pk=3, pk=5
        let trace_batch = make_batch(schema, &[(1u128, 1, 100), (3u128, 1, 300), (5u128, 1, 500)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        // Anti-join pipeline: reg 0 -> anti_join(reg 0, reg 1) -> reg 2
        let cursor_aj = table.open_cursor();
        let ch_aj = Box::into_raw(Box::new(cursor_aj)) as *mut libc::c_void;

        let mut builder_aj = ProgramBuilder::new();
        builder_aj.add_anti_join_dt(0, 1, 2);
        builder_aj.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm_aj = *builder_aj.build(&reg_meta);
        let cursors_aj = vec![std::ptr::null_mut(), ch_aj, std::ptr::null_mut()];

        // Delta has pk=1,2,3,4,5
        let input_aj = make_batch(
            schema,
            &[
                (1u128, 1, 10),
                (2u128, 1, 20),
                (3u128, 1, 30),
                (4u128, 1, 40),
                (5u128, 1, 50),
            ],
        );
        let r_aj = execute_epoch(&vm_aj.program, &mut vm_aj.regfile, input_aj, 0, 2, &cursors_aj, &[])
            .unwrap()
            .unwrap();

        // Semi-join pipeline
        let cursor_sj = table.open_cursor();
        let ch_sj = Box::into_raw(Box::new(cursor_sj)) as *mut libc::c_void;

        let mut builder_sj = ProgramBuilder::new();
        builder_sj.add_semi_join_dt(0, 1, 2);
        builder_sj.add_halt();

        let mut vm_sj = *builder_sj.build(&reg_meta);
        let cursors_sj = vec![std::ptr::null_mut(), ch_sj, std::ptr::null_mut()];

        let input_sj = make_batch(
            schema,
            &[
                (1u128, 1, 10),
                (2u128, 1, 20),
                (3u128, 1, 30),
                (4u128, 1, 40),
                (5u128, 1, 50),
            ],
        );
        let r_sj = execute_epoch(&vm_sj.program, &mut vm_sj.regfile, input_sj, 0, 2, &cursors_sj, &[])
            .unwrap()
            .unwrap();

        // Anti should get pk=2,4 (no match); semi should get pk=1,3,5 (match)
        assert_eq!(r_aj.count, 2, "anti-join should keep 2 non-matching rows");
        assert_eq!(r_sj.count, 3, "semi-join should keep 3 matching rows");
        assert_eq!(
            r_aj.count + r_sj.count,
            5,
            "anti + semi must equal input count (complement property)"
        );

        unsafe {
            drop(Box::from_raw(ch_aj as *mut CursorHandle));
        }
        unsafe {
            drop(Box::from_raw(ch_sj as *mut CursorHandle));
        }
        table.close();
    }

    /// Filter with expression bytecode: col1 > 25 keeps rows with val 30, 40, 50.
    #[test]
    fn test_filter_with_expr() {
        use crate::expr::{CmpOp, LogicalInstr, LogicalProgram};

        let schema = schema_1i64();

        // Build expression: col1 > 25
        // col1 is schema column index 1 (the I64 payload column)
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col[1]
            LogicalInstr::LoadConst { dst: 1, val: 25 }, // r1 = 25
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            }, // r2 = (r0 > r1)
        ];
        let prog = LogicalProgram::new(instrs, 3, 2, vec![]);

        let func = Box::new(crate::expr::ScalarFunc::from_predicate(prog, &schema));
        let func_ptr = Box::into_raw(func) as *const ScalarFunc;

        let mut builder = ProgramBuilder::new();
        builder.add_filter(0, 1, func_ptr);
        builder.add_halt();

        let input = make_batch(
            schema,
            &[
                (1u128, 1, 10),
                (2u128, 1, 20),
                (3u128, 1, 30),
                (4u128, 1, 40),
                (5u128, 1, 50),
            ],
        );

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 3, "filter col1>25 should keep 3 rows (30,40,50)");
        assert_eq!(rows[0].0, 3); // pk=3, val=30
        assert_eq!(rows[1].0, 4); // pk=4, val=40
        assert_eq!(rows[2].0, 5); // pk=5, val=50

        unsafe {
            drop(Box::from_raw(func_ptr as *mut ScalarFunc));
        }
    }

    /// Multi-agg reduce: COUNT + SUM on same column in one pass.
    #[test]
    fn test_reduce_multi_agg() {
        // Input: pk(U64), val(I64). All rows in same group (pk=1).
        // COUNT + SUM of val column.
        let in_schema = schema_1i64();

        // Output: pk(U64), count(I64), sum(I64) — GROUP BY pk → natural PK
        let out_schema = make_schema(&[
            type_code::I64, // count
            type_code::I64, // sum
        ]);

        let dir = tempfile::tempdir().unwrap();

        let tout_dir = dir.path().join("ma_tr_out");
        let mut trace_out_table = Table::new(
            tout_dir.to_str().unwrap(),
            "ma_tr_out",
            out_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();
        let trace_out_ptr = &mut trace_out_table as *mut Table;

        let tin_dir = dir.path().join("ma_tr_in");
        let mut trace_in_table = Table::new(
            tin_dir.to_str().unwrap(),
            "ma_tr_in",
            in_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();
        let trace_in_ptr = &mut trace_in_table as *mut Table;

        // Two agg descriptors: COUNT(col=1) and SUM(col=1)
        let agg_descs = [
            AggDescriptor {
                col_idx: 1, // schema col index for the val column
                agg_op: AggOp::Count,
                col_type_code: TypeCode::I64,
                _pad: [0; 2],
            },
            AggDescriptor {
                col_idx: 1, // schema col index for the val column
                agg_op: AggOp::Sum,
                col_type_code: TypeCode::I64,
                _pad: [0; 2],
            },
        ];
        // GROUP BY col 0 (= pk, schema col index 0)
        let group_cols = [0u32];

        let mut builder = ProgramBuilder::new();
        // reg 0 = input delta, reg 1 = trace_out, reg 2 = output,
        // reg 3 = trace_in, reg 4 = unused
        builder.add_reduce(
            0,
            Some(3),
            1,
            2,
            None,
            &agg_descs,
            &group_cols,
            out_schema,
            std::ptr::null_mut(),
            false,
            0,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            false,
            false,
        );
        builder.add_integrate(
            2,
            trace_out_ptr,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            false,
            0,
            &[],
            0,
        );
        builder.add_integrate(
            0,
            trace_in_ptr,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            false,
            0,
            &[],
            0,
        );
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema),
            RegisterMeta::delta(out_schema),
            RegisterMeta::trace(in_schema),
            RegisterMeta::delta(in_schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        // Input: 3 rows all with pk=1, vals 10, 20, 30
        let input = make_batch(in_schema, &[(1u128, 1, 10), (1u128, 1, 20), (1u128, 1, 30)]);

        let tr_out_cursor = trace_out_table.open_cursor();
        let tr_out_ch = Box::into_raw(Box::new(tr_out_cursor)) as *mut libc::c_void;
        let tr_in_cursor = trace_in_table.open_cursor();
        let tr_in_ch = Box::into_raw(Box::new(tr_in_cursor)) as *mut libc::c_void;

        let cursors = vec![
            std::ptr::null_mut(),
            tr_out_ch,
            std::ptr::null_mut(),
            tr_in_ch,
            std::ptr::null_mut(),
        ];

        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[])
            .unwrap()
            .unwrap();

        // Should produce 1 row: pk=1, count=3, sum=60
        assert_eq!(result.count, 1, "multi-agg should produce 1 group");
        let count_val = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        let sum_val = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(count_val, 3, "COUNT should be 3");
        assert_eq!(sum_val, 60, "SUM should be 60");

        unsafe {
            drop(Box::from_raw(tr_out_ch as *mut CursorHandle));
        }
        unsafe {
            drop(Box::from_raw(tr_in_ch as *mut CursorHandle));
        }
        trace_out_table.close();
        trace_in_table.close();
    }

    // ── Fix regression: identity MAP (null func_ptr) ─────────────────────

    #[test]
    fn test_map_identity_null_func_ptr() {
        // When the compiler emits a pass-through MAP (null func_ptr), the VM
        // must clone the batch without dereferencing the null pointer.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.add_map(0, 1, std::ptr::null(), schema, &[], &[], false, 0);
        builder.add_halt();

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20)]);

        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(&reg_meta);
        let cursors = vec![std::ptr::null_mut(); 2];
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1, &cursors, &[])
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, 1, 10));
        assert_eq!(rows[1], (2, 1, 20));
    }

    // ── Regression: AntiJoinDT with null trace cursor ────────────────────

    /// AntiJoinDT semantics: if the trace is absent (null cursor), nothing is
    /// excluded → all delta rows should pass through unchanged.
    /// Bug: current code silently returns empty instead.
    #[test]
    fn test_anti_join_dt_null_cursor() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        // reg 0 = delta, reg 1 = trace (null cursor), reg 2 = output
        builder.add_anti_join_dt(0, 1, 2);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        // Pass null for the trace register — simulates cursor-create failure or
        // an empty table that could not produce a cursor handle.
        let cursors = vec![std::ptr::null_mut(); 3];

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20), (3u128, 1, 30)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[]).unwrap();

        // With no trace to exclude against, all 3 delta rows must survive.
        let result = result.expect("anti-join with null trace must pass all delta rows");
        assert_eq!(
            result.count, 3,
            "all delta rows should survive when trace cursor is null"
        );
    }

    /// JoinDTOuter semantics: if the trace is absent (null cursor), every delta
    /// row has no right-side match → all should be null-extended.
    /// Bug: current code silently returns empty instead.
    #[test]
    fn test_join_dt_outer_null_cursor() {
        let left_schema = schema_1i64();
        // right schema: same shape (one I64 payload col)
        let right_schema = schema_1i64();
        // outer join output: left col + right col (right cols will be null)
        let out_schema = make_schema(&[type_code::I64, type_code::I64]);

        let mut builder = ProgramBuilder::new();
        // reg 0 = delta, reg 1 = trace (null cursor), reg 2 = output
        builder.add_join_dt_outer(0, 1, 2, right_schema);
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(left_schema),
            RegisterMeta::trace(right_schema),
            RegisterMeta::delta(out_schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        let cursors = vec![std::ptr::null_mut(); 3];

        let input = make_batch(left_schema, &[(1u128, 1, 10), (2u128, 1, 20)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2, &cursors, &[]).unwrap();

        // With no trace, outer join must null-extend all 2 delta rows.
        let result = result.expect("outer join with null trace must null-extend all delta rows");
        assert_eq!(
            result.count, 2,
            "all delta rows should be null-extended when trace cursor is null"
        );
    }

    /// Proper SUM test: use agg_op=2 (AGG_SUM) and verify the actual aggregate
    /// value, not just that output is non-empty.
    #[test]
    fn test_reduce_sum_value() {
        let in_schema = make_schema(&[type_code::I64]);
        // [U128 PK, I64 sum, I64 count companion]
        let out_schema = make_schema(&[type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();

        let tout_dir = dir.path().join("sv_tr_out");
        let mut trace_out_table = Table::new(
            tout_dir.to_str().unwrap(),
            "sv_tr_out",
            out_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();
        let trace_out_ptr = &mut trace_out_table as *mut Table;

        let tin_dir = dir.path().join("sv_tr_in");
        let mut trace_in_table = Table::new(
            tin_dir.to_str().unwrap(),
            "sv_tr_in",
            in_schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();
        let trace_in_ptr = &mut trace_in_table as *mut Table;

        // SUM of col 1 (agg_op=2 = AGG_SUM, not AGG_COUNT) plus the trailing Count
        // cardinality companion every all-linear reduce carries.
        let agg_descs = [
            AggDescriptor {
                col_idx: 1,
                agg_op: AggOp::Sum,
                col_type_code: TypeCode::I64,
                _pad: [0; 2],
            },
            AggDescriptor {
                col_idx: 0,
                agg_op: AggOp::Count,
                col_type_code: TypeCode::I64,
                _pad: [0; 2],
            },
        ];
        // GROUP BY col 0 (pk)
        let group_cols = [0u32];

        let mut builder = ProgramBuilder::new();
        builder.add_reduce(
            0,
            Some(2),
            1,
            3,
            None,
            &agg_descs,
            &group_cols,
            out_schema,
            std::ptr::null_mut(),
            false,
            0,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            false,
            false,
        );
        builder.add_integrate(
            3,
            trace_out_ptr,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            false,
            0,
            &[],
            0,
        );
        builder.add_integrate(
            0,
            trace_in_ptr,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            false,
            0,
            &[],
            0,
        );
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema),
            RegisterMeta::trace(in_schema),
            RegisterMeta::delta(out_schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        // Three rows all in the same group (same pk), values 10, 20, 30 → SUM=60
        let input = make_batch(in_schema, &[(1u128, 1, 10), (1u128, 1, 20), (1u128, 1, 30)]);

        let tr_out_ch = Box::into_raw(Box::new(trace_out_table.open_cursor())) as *mut libc::c_void;
        let tr_in_ch = Box::into_raw(Box::new(trace_in_table.open_cursor())) as *mut libc::c_void;
        let cursors = vec![std::ptr::null_mut(), tr_out_ch, tr_in_ch, std::ptr::null_mut()];

        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 3, &cursors, &[])
            .unwrap()
            .expect("SUM reduce must produce output");

        assert_eq!(result.count, 1, "one group → one output row");
        let sum_val = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(sum_val, 60, "SUM(10+20+30) must be 60");

        unsafe {
            drop(Box::from_raw(tr_out_ch as *mut CursorHandle));
        }
        unsafe {
            drop(Box::from_raw(tr_in_ch as *mut CursorHandle));
        }
        trace_out_table.close();
        trace_in_table.close();
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
            tdir.to_str().unwrap(),
            "ext",
            schema,
            0,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();

        // reg 0 = input delta (kind 0), reg 1 = external trace (kind 1), reg 2 = output delta
        let mut builder = ProgramBuilder::new();
        builder.add_distinct(0, 1, 2, std::ptr::null_mut()); // hist_table_idx = -1 (no ingest)
        builder.add_halt();

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema),
            RegisterMeta::delta(schema),
        ];
        let mut vm = *builder.build(&reg_meta);

        // Epoch 1: supply a real cursor for reg 1.
        let cursor1 = table.open_cursor();
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
