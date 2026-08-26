//! DBSP VM: executes compiled circuit programs entirely in Rust.

use std::cell::UnsafeCell;

use crate::expr::ScalarFunc;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor, Table};

mod builder;
mod exec;

pub(crate) use builder::ProgramBuilder;
#[cfg(test)]
pub(crate) use exec::execute_epoch;
pub(crate) use exec::execute_epoch_from;
pub(crate) use exec::execute_epoch_multi;

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// The resource pools a `Program` owns are unrelated index spaces, all naturally
/// `u16`. `IntegrateAvi` builds two of them on adjacent lines, where a swap would
/// mis-dispatch into a live table with no panic — so each gets its own type and
/// the compiler does the checking.
macro_rules! resource_idx {
    ($($name:ident => $pool:literal;)*) => {$(
        #[doc = concat!("Index into `Program::", $pool, "`.")]
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        pub(crate) struct $name(pub u16);

        impl $name {
            #[inline]
            fn at(self) -> usize {
                self.0 as usize
            }
        }
    )*};
}

resource_idx! {
    TableIdx => "tables";
    FuncIdx => "funcs";
    PackerIdx => "reindex_packers";
    PlanIdx => "reduce_plans";
    BakeIdx => "avi_bakes";
}

/// One VM instruction with all operator-specific data pre-resolved.
pub(crate) enum Instr {
    Halt,
    Filter {
        in_reg: u16,
        out_reg: u16,
        func_idx: FuncIdx,
    },
    Map {
        in_reg: u16,
        out_reg: u16,
        func_idx: FuncIdx,
        reindex: ReindexOperand,
    },
    Negate {
        in_reg: u16,
        out_reg: u16,
    },
    Union {
        in_a: u16,
        in_b: u16,
        out_reg: u16,
        /// Take `in_a`'s batch rather than clone it — set when `build_plan` proved
        /// this is `in_a`'s last read. Never set for a self-union: that arm doubles
        /// the weights in place and never reaches `op_union`, which is the operator
        /// that would consume the operand.
        consume: bool,
    },
    /// Shared instruction for `distinct` and `positive_part`: per consolidated
    /// (PK, payload), emit `clamp(w_new, lo, hi) − clamp(w_old, lo, hi)`. Bounds
    /// `(-1, 1)` ⇒ `distinct` (set membership); `(0, i64::MAX)` ⇒ `positive_part`
    /// (bag multiplicity).
    WeightClamp {
        in_reg: u16,
        hist_reg: u16,
        out_reg: u16,
        hist_table_idx: TableIdx,
        lo: i64,
        hi: i64,
        /// Take `in_reg`'s batch rather than clone it — see `Instr::Union`.
        consume: bool,
    },
    /// The delta-trace inner join, equi and range alike: the probe is baked by
    /// the compiler from the wire's `JoinKind`, so the wire's relation spelling
    /// never reaches the instruction set.
    JoinDT {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
        probe: crate::ops::JoinProbe,
    },
    WorkerFilter {
        in_reg: u16,
        out_reg: u16,
        worker_id: u32,
        num_workers: u32,
    },
    NullExtend {
        in_reg: u16,
        out_reg: u16,
    },
    Integrate {
        in_reg: u16,
        target: IntegrateTarget,
    },
    Reduce {
        in_reg: u16,
        trace_out_reg: u16,
        out_reg: u16,
        /// The baked `ops::ReducePlan` carrying the schemas, group columns,
        /// aggregate descriptors, and every derived gate (linearity, key kind,
        /// emission roles, ground flags).
        plan_idx: PlanIdx,
        /// The MIN/MAX history. The cursor is created fresh from the table each
        /// tick; `None` means every aggregate is linear.
        avi: Option<ReduceAvi>,
    },
}

/// Stored form of [`crate::ops::ReindexSpec`] — the `Instr::Map` PK-restamp
/// operand. `Pack` indexes `Program::reindex_packers` rather than inlining the
/// packer: a `ReindexPacker` is an order of magnitude wider than an `Instr`, and
/// inlining it would grow every instruction and wreck the dispatch loop's
/// locality. The exec dispatch resolves the index to the borrowed ops enum.
#[derive(Clone, Copy)]
pub(crate) enum ReindexOperand {
    None,
    HashRow { branch_id: u8 },
    Pack { packer_idx: PackerIdx },
}

/// True iff `instr` reads the batch of register `r`. The instruction set's own
/// read-set knowledge, consumed by `build_plan`'s liveness pass, which decides
/// whether a `Union`/`WeightClamp` may take its input in place. Trace registers
/// are cursor-driven — their batches are never read — so trace ports don't count.
pub(crate) fn reads_reg(instr: &Instr, r: u16) -> bool {
    match instr {
        Instr::Filter { in_reg, .. }
        | Instr::Map { in_reg, .. }
        | Instr::Negate { in_reg, .. }
        | Instr::WorkerFilter { in_reg, .. }
        | Instr::NullExtend { in_reg, .. }
        | Instr::WeightClamp { in_reg, .. }
        | Instr::Integrate { in_reg, .. }
        | Instr::Reduce { in_reg, .. } => *in_reg == r,
        Instr::Union { in_a, in_b, .. } => *in_a == r || *in_b == r,
        Instr::JoinDT { delta_reg, .. } => *delta_reg == r,
        Instr::Halt => false,
    }
}

/// True iff executing `instr` writes operator state (a table). The instruction
/// set's own write-set knowledge, matched exhaustively so a new state-writing
/// opcode is a compile error here rather than something a read-only replay
/// silently accepts. Consumed by the bounded-view hydration gate, which must not
/// mutate the state it reads.
pub(crate) fn writes_state(instr: &Instr) -> bool {
    match instr {
        // `Integrate` writes its trace, `WeightClamp` its history table, `Reduce`
        // its output trace and optional value index.
        Instr::Integrate { .. } | Instr::WeightClamp { .. } | Instr::Reduce { .. } => true,
        Instr::Filter { .. }
        | Instr::Map { .. }
        | Instr::Negate { .. }
        | Instr::WorkerFilter { .. }
        | Instr::NullExtend { .. }
        | Instr::Union { .. }
        | Instr::JoinDT { .. }
        | Instr::Halt => false,
    }
}

/// Combined-AVI descriptor embedded in an Integrate instruction. One table
/// serves every MIN/MAX aggregate of the reduce; the baked resources — the
/// composite index schema, the group-key gatherer, and the value-indexed
/// subset of the reduce's descriptors (ordinal = position) with their resolved
/// column locators — live in `Program::avi_bakes`. The population derives
/// `for_max`/type from each baked descriptor, matching the read side.
#[derive(Clone, Copy)]
pub(crate) struct IntegrateAvi {
    pub table_idx: TableIdx,
    pub bake_idx: BakeIdx,
}

/// The value index an `Instr::Reduce` reads: the table to open a cursor against
/// and the bake whose `key_packer` spells the seek prefix. The two travel
/// together so the operator's history argument cannot be half-formed — a cursor
/// with no packer is a state the emitter cannot express.
#[derive(Clone, Copy)]
pub(crate) struct ReduceAvi {
    pub table_idx: TableIdx,
    pub bake_idx: BakeIdx,
}

/// What an `Instr::Integrate` accumulates into. The two are exclusive by
/// construction, so an enum is what the emitter can actually express, where a
/// pair of `Option`s would admit two states nothing produces.
#[derive(Clone, Copy)]
pub(crate) enum IntegrateTarget {
    /// A trace table.
    Trace(TableIdx),
    /// The combined aggregate value index; the delta lands nowhere else.
    Avi(IntegrateAvi),
}

/// Opaque handle owning a compiled program and its register file.
pub(crate) struct VmHandle {
    /// Cursor handles for the trace registers, kept alive across the epoch.
    /// Indexed in parallel with `trace_regs`. Cursor destructors dereference the
    /// `Table` they were opened against, and those tables live in `program`, so
    /// this MUST drop first — the assertion below is the machine check.
    owned_cursor_handles: Vec<Option<Box<ReadCursor>>>,
    pub program: Program,
    pub regfile: RegisterFile,
    /// `(reg_id, backing table)` for every trace register, derived
    /// once from `program.reg_meta` so the per-epoch refresh walks only the
    /// trace registers instead of the whole register file.
    trace_regs: Vec<(u16, TableIdx)>,
}

const _: () = assert!(std::mem::offset_of!(VmHandle, owned_cursor_handles) < std::mem::offset_of!(VmHandle, program));

impl VmHandle {
    /// Compact every owned trace table, keeping its L0 fan-in bounded — there is
    /// no background compactor. The epoch path's job, not a read's: a compaction
    /// mutates shard state. An `Err` leaves the shard index unchanged, so a
    /// cursor opened afterwards still sees a consistent snapshot.
    pub(super) fn compact_owned_traces(&mut self) {
        for slot in 0..self.trace_regs.len() {
            let _ = self.program.table_mut(self.trace_regs[slot].1).compact_if_needed();
        }
    }

    /// Create fresh cursors for the trace registers. Must be called before
    /// `execute_epoch`: the dispatch loop dereferences every trace register's
    /// cursor without a null check. The cursor handles are stored in
    /// `owned_cursor_handles` and their raw pointers bound into the register file.
    pub(super) fn bind_trace_cursors(&mut self) {
        gnitz_debug!("vm: bind_trace_cursors, {} trace regs", self.trace_regs.len());
        // Drop previous cursors before creating new ones (releases shard refs
        // etc.), then size the slot storage back to full length.
        self.null_owned_cursors();
        self.owned_cursor_handles.resize_with(self.trace_regs.len(), || None);
        for slot in 0..self.trace_regs.len() {
            let (reg_id, table_idx) = self.trace_regs[slot];
            let cursor = self.program.table_mut(table_idx).open_cursor();
            // Store the Box into its slot first, then derive the
            // pointer from the slot — taking the pointer before the
            // move raises Stacked Borrows aliasing questions even
            // though the heap address is stable across the move.
            self.owned_cursor_handles[slot] = Some(Box::new(cursor));
            let ptr = self.owned_cursor_handles[slot].as_mut().unwrap().as_mut() as *mut ReadCursor;
            self.regfile.registers[reg_id as usize].cursor_ptr = ptr;
        }
    }

    /// Clear the register file's delta batches (a disjoint-field borrow of the
    /// program's reg_meta and the regfile, packaged for external callers).
    pub(super) fn clear_deltas(&mut self) {
        self.regfile.clear_deltas(&self.program.reg_meta);
    }

    /// Drop every owned-trace cursor and null its register `cursor_ptr` — the
    /// first half of `bind_trace_cursors`, minus the cursor
    /// re-creation. Called before the ephemeral checkpoint round folds each owned
    /// trace table's RAM tier into a shard, so no live cursor holds a stale
    /// snapshot of it. Defensive tidiness, not a safety requirement: held cursors
    /// keep their own `Rc<Batch>` / shard `Arc` clones (a fold produces a
    /// stale-not-dangling snapshot) and the next epoch calls
    /// `bind_trace_cursors` before any deref — but nulling here keeps the
    /// flush's safety local and obvious. Only `trace_regs` are handled
    /// (`_int_`/`_hist_`/`_reduce_`, all cross-epoch); the epoch-local
    /// `_avidx_` cursor is created and dropped inside the `Reduce` instruction.
    pub(super) fn null_owned_cursors(&mut self) {
        self.owned_cursor_handles.clear(); // drops every held cursor
        for &(reg_id, _table_idx) in &self.trace_regs {
            self.regfile.registers[reg_id as usize].cursor_ptr = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

/// Per-register metadata.
#[derive(Clone, Copy)]
pub(crate) struct RegisterMeta {
    pub schema: SchemaDescriptor,
    /// A trace register's backing table; `None` for a delta register. Naming the
    /// table here rather than in a side list is what lets `bind_trace_cursors`
    /// guarantee every trace register holds a live cursor at dispatch.
    pub owned_table: Option<TableIdx>,
}

impl RegisterMeta {
    pub(super) const fn delta(schema: SchemaDescriptor) -> Self {
        Self {
            schema,
            owned_table: None,
        }
    }
    pub(super) const fn trace(schema: SchemaDescriptor, owned_table: TableIdx) -> Self {
        Self {
            schema,
            owned_table: Some(owned_table),
        }
    }
}

/// A compiled DBSP program ready for execution.
///
/// Owns every resource its instructions name, in the index space those
/// instructions use — so an operand is a position in one vector and there is no
/// second numbering to keep in step.
#[allow(clippy::vec_box)]
pub(crate) struct Program {
    pub instructions: Vec<Instr>,
    pub reg_meta: Vec<RegisterMeta>,
    /// Scalar functions — filters, maps, projections, post-reduce finalizes.
    pub funcs: Vec<Box<ScalarFunc>>,
    /// Child tables created during compilation (integrate, history, reduce, AVI).
    /// `UnsafeCell` because an operator mutates its table while the dispatch
    /// still holds `&program` for the instruction stream and `reg_meta`.
    pub tables: Vec<UnsafeCell<Box<Table>>>,
    /// Baked per-`Instr::Map` reindex packers (see `ReindexOperand::Pack`).
    pub reindex_packers: Vec<crate::ops::ReindexPacker>,
    /// Baked per-`Instr::Reduce` plans (see `ops::ReducePlan`).
    pub reduce_plans: Vec<crate::ops::ReducePlan>,
    /// Baked AVI write-side resources, indexed by `IntegrateAvi::bake_idx`.
    pub avi_bakes: Vec<crate::ops::AviBake>,
}

// SAFETY: Program is only accessed from a single thread (the worker thread
// that owns the plan), which is also what makes the `UnsafeCell` tables sound.
unsafe impl Send for Program {}

impl Program {
    /// The table at `idx`, for the operator about to write it. Takes `&self`
    /// because the dispatch loop is iterating `self.instructions` at the time;
    /// single-threaded execution is what keeps the two borrows apart. Callers
    /// must not hold a second `&mut` into the same table (each instruction names
    /// one) — the same contract `IndexCircuitEntry::table_mut` carries.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn table_mut(&self, idx: TableIdx) -> &mut Table {
        // SAFETY: one worker thread owns the plan, and each instruction names
        // one table, so no second `&mut` to it is live.
        unsafe { &mut *self.tables[idx.at()].get() }
    }

    /// Every owned table, for the checkpoint flush sweep.
    pub(crate) fn table_indices(&self) -> impl Iterator<Item = TableIdx> {
        (0..self.tables.len() as u16).map(TableIdx)
    }
}

// ---------------------------------------------------------------------------
// Register file (runtime state)
// ---------------------------------------------------------------------------

/// Runtime state for one register. The schema and kind live in the program's
/// immutable `reg_meta` (one source of truth); only the batch and cursor are
/// per-epoch state.
pub(crate) struct Register {
    /// Delta: current batch.  Trace: unused (empty).
    pub batch: Batch,
    /// Trace: current cursor.  Borrowed each epoch.  Delta: None.
    pub cursor_ptr: *mut ReadCursor,
}

/// Collection of registers for a single plan execution.
pub(crate) struct RegisterFile {
    pub registers: Vec<Register>,
}

impl RegisterFile {
    /// Create from register metadata: zero-allocation empty batches (an input
    /// seed replaces a register's batch wholesale, so pre-sizing buys nothing —
    /// it only pinned a pooled buffer per register for the cached plan's
    /// lifetime) and null cursors.
    pub(super) fn new(metas: &[RegisterMeta]) -> Self {
        let registers = metas
            .iter()
            .map(|m| Register {
                batch: if m.schema.num_columns() > 0 {
                    Batch::empty_with_schema(&m.schema)
                } else {
                    Batch::placeholder()
                },
                cursor_ptr: std::ptr::null_mut(),
            })
            .collect();
        RegisterFile { registers }
    }

    /// Clear delta batches without refreshing cursors.
    pub(super) fn clear_deltas(&mut self, metas: &[RegisterMeta]) {
        for (reg, meta) in self.registers.iter_mut().zip(metas) {
            if meta.owned_table.is_none() && meta.schema.num_columns() > 0 {
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
    use crate::ops::AggDescriptor;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Layout;
    use gnitz_wire::AggFunc;

    #[test]
    fn test_clear_deltas_clears_only_delta_registers() {
        // clear_deltas (used by DagEngine::clear_view_regfile_deltas
        // after backfill) must empty Delta registers while leaving Trace
        // registers untouched.
        let schema = schema_1i64();
        let mut delta_batch = Batch::with_capacity(schema, 1);
        delta_batch.extend_pk(1u128);
        delta_batch.extend_weight(&1i64.to_le_bytes());
        delta_batch.extend_null_bmp(&0u64.to_le_bytes());
        delta_batch.extend_col(0, &10i64.to_le_bytes());
        delta_batch.count += 1;

        let mut trace_batch = Batch::with_capacity(schema, 1);
        trace_batch.extend_pk(2u128);
        trace_batch.extend_weight(&1i64.to_le_bytes());
        trace_batch.extend_null_bmp(&0u64.to_le_bytes());
        trace_batch.extend_col(0, &20i64.to_le_bytes());
        trace_batch.count += 1;

        let metas = [RegisterMeta::delta(schema), RegisterMeta::trace(schema, TableIdx(0))];
        let mut rf = RegisterFile {
            registers: vec![
                Register {
                    batch: delta_batch,
                    cursor_ptr: std::ptr::null_mut(),
                },
                Register {
                    batch: trace_batch,
                    cursor_ptr: std::ptr::null_mut(),
                },
            ],
        };
        assert_eq!(rf.registers[0].batch.count, 1);
        rf.clear_deltas(&metas);
        assert_eq!(rf.registers[0].batch.count, 0, "Delta register must be cleared");
        assert_eq!(rf.registers[1].batch.count, 1, "Trace register must be preserved");
    }

    // ── Test helpers ─────────────────────────────────────────────────────

    /// A table under `dir` for a trace register's backing store; the caller
    /// hands it to `ProgramBuilder::push_table`, which owns it from then on.
    fn owned_table(dir: &std::path::Path, name: &str, schema: SchemaDescriptor) -> Table {
        Table::with_arena(
            dir.join(name).to_str().unwrap(),
            schema,
            0,
            1 << 20,
            crate::storage::RecoverySource::Rederive { resume_at: None },
        )
        .unwrap()
    }

    /// A plain integrate of `in_reg` into `table` (no AVI) — the shape every
    /// test integrate uses.
    fn push_integrate(b: &mut ProgramBuilder, in_reg: u16, table_idx: TableIdx) {
        b.push(Instr::Integrate {
            in_reg,
            target: IntegrateTarget::Trace(table_idx),
        });
    }

    /// A reduce with no AVI — the shape every test reduce uses.
    #[allow(clippy::too_many_arguments)]
    fn push_reduce(
        b: &mut ProgramBuilder,
        in_reg: u16,
        trace_out_reg: u16,
        out_reg: u16,
        aggs: &[AggDescriptor],
        gcols: &[u32],
        in_schema: SchemaDescriptor,
        out_schema: SchemaDescriptor,
        out_key: crate::schema::ReduceOutKey,
    ) {
        let plan_idx = b.add_reduce_plan(crate::ops::ReducePlan::new(
            &in_schema,
            &out_schema,
            gcols,
            aggs,
            out_key,
            false,
            false,
            false,
        ));
        b.push(Instr::Reduce {
            in_reg,
            trace_out_reg,
            out_reg,
            plan_idx,
            avi: None,
        });
    }

    fn make_schema(col_types: &[u8]) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
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
        let mut b = Batch::with_capacity(schema, n);
        for &(pk, w, c0) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c0.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, &schema);
        b
    }

    /// Create a batch with two I64 payload columns.
    fn make_batch_2col(schema: SchemaDescriptor, rows: &[(u128, i64, i64, i64)]) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_capacity(schema, n);
        for &(pk, w, c0, c1) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c0.to_le_bytes());
            b.extend_col(1, &c1.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, &schema);
        b
    }

    /// Extract rows from a batch as (pk, weight, col0_i64) tuples.
    fn extract_rows(b: &Batch) -> Vec<(u64, i64, i64)> {
        let mut rows = Vec::new();
        for i in 0..b.count {
            let pk = b.get_pk(i) as u64;
            let w = b.get_weight(i);
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
            gnitz_expr::LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col[1]
            gnitz_expr::LogicalInstr::LoadConst { dst: 1, val: 0 },  // r1 = 0
            gnitz_expr::LogicalInstr::Cmp {
                op: gnitz_expr::CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            }, // r2 = r0 > r1
        ];
        let pred_prog = gnitz_expr::LogicalProgram::new(pred_instrs, 3, 2, vec![]);
        let mut builder = ProgramBuilder::new();
        let func_idx = builder.push_func(crate::expr::ScalarFunc::from_predicate(pred_prog, &schema).unwrap());
        builder.push(Instr::Filter {
            in_reg: 0,
            out_reg: 1,
            func_idx,
        });
        builder.push(Instr::Negate { in_reg: 1, out_reg: 2 });
        builder.push(Instr::Halt);

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, -5), (3u128, 1, 20)]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 2)
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, -1, 10));
        assert_eq!(rows[1], (3, -1, 20));
    }

    #[test]
    fn test_ghost_property() {
        // Input with opposing weights for same PK: consolidated to zero → None.
        // We pre-consolidate manually (input is consolidated before sending to VM).
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        // pass-through (a union with an empty register is the identity op)
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        // Consolidated empty batch (ghost elimination already applied)
        let input = make_batch(schema, &[]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1);

        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_empty_input() {
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        // pass-through (a union with an empty register is the identity op)
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        let input = make_batch(schema, &[]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1);

        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_union_operator() {
        // Union both input registers: reg 0 carries the seeded batch, reg 2 is
        // seeded with a second batch.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20)]);
        let input_b = make_batch(schema, &[(3u128, 1, 30)]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch_multi(&vm.program, &mut { vm.regfile }, [(0u16, input), (2u16, input_b)], 1)
            .unwrap()
            .unwrap();

        assert_eq!(result.count, 3);
    }

    /// Schemas for a UNION whose sides disagree on the payload column's
    /// nullability: `(left NOT NULL, right nullable, merged output)`. The merged
    /// one is the OR the emit layer's `union_nullability_merge` produces, which
    /// for this pair is the right side's.
    fn union_nullability_schemas() -> (SchemaDescriptor, SchemaDescriptor, SchemaDescriptor) {
        let pk = SchemaColumn::new(type_code::U128, 0);
        let not_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 0)], &[0]);
        let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 1)], &[0]);
        (not_null, nullable, nullable)
    }

    /// One row under `schema`: PK 1, weight +1, payload -3, no null bit.
    fn one_negative_row(schema: SchemaDescriptor) -> Batch {
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &(-3i64).to_le_bytes());
        b.count += 1;
        b
    }

    /// A UNION whose sides disagree on a payload column's nullability must run
    /// under the OUTPUT register's schema. Only the merged schema selects the
    /// null-aware comparator, which sorts a NULL cell below -3; the left input's
    /// null-blind one reads the cell's zero bytes as the integer 0 and sorts it
    /// above. `op_union_merge` certifies whichever order it produced `Sorted`,
    /// so the next `into_consolidated` trusts it rather than re-sorting.
    #[test]
    fn test_union_runs_under_the_merged_output_schema() {
        let (schema_a, schema_b, merged) = union_nullability_schemas();

        // Both rows on the same PK, so the merge resolves them against each
        // other: left is a negative value, right a canonical zero-filled NULL.
        let mut left = one_negative_row(schema_a);
        left.certify_layout(Layout::Sorted, &schema_a);

        let mut right = Batch::with_capacity(schema_b, 1);
        right.extend_pk(1u128);
        right.extend_weight(&1i64.to_le_bytes());
        right.extend_null_bmp(&1u64.to_le_bytes());
        right.fill_col_zero(0, 8);
        right.count += 1;
        right.certify_layout(Layout::Sorted, &schema_b);

        let mut builder = ProgramBuilder::new();
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);
        let reg_meta = [
            RegisterMeta::delta(schema_a),
            RegisterMeta::delta(merged),
            RegisterMeta::delta(schema_b),
        ];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch_multi(&vm.program, &mut { vm.regfile }, [(0u16, left), (2u16, right)], 1)
            .unwrap()
            .unwrap();

        assert_eq!(result.count, 2, "Z-Set + keeps both rows");
        assert!(
            gnitz_wire::null_word_get(result.get_null_word(0), 0),
            "the merged schema's null-aware comparator sorts NULL below -3; the left \
             input's null-blind one reads the null cell as 0 and sorts it above",
        );
    }

    /// `op_union`'s O(1) identity path returns the left operand verbatim, label
    /// included, so a batch leaves the VM carrying the left input's schema unless
    /// the epilogue stamps the output register's own. The exchange wire carries
    /// that label to a master that cannot re-derive it.
    #[test]
    fn test_union_identity_path_output_carries_out_register_schema() {
        let (schema_a, schema_b, merged) = union_nullability_schemas();

        // in_b is never seeded, so `op_union` takes the b.count == 0 identity
        // path and hands `batch_a` straight back with `schema_a` still on it.
        let left = one_negative_row(schema_a);

        let mut builder = ProgramBuilder::new();
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);
        let reg_meta = [
            RegisterMeta::delta(schema_a),
            RegisterMeta::delta(merged),
            RegisterMeta::delta(schema_b),
        ];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch_multi(&vm.program, &mut { vm.regfile }, [(0u16, left)], 1)
            .unwrap()
            .unwrap();
        assert_eq!(result.count, 1);
        assert_eq!(
            result.schema,
            Some(merged),
            "a batch leaving the VM carries its output register's schema, not the operand's",
        );
    }

    // Item 7: Union with in_a == in_b is Z + Z and must double every weight.
    // The naive by-value path moves batch_a out then reads an emptied batch_b,
    // producing +1 instead of +2.
    #[test]
    fn test_self_union_doubles_weights() {
        let schema = schema_1i64();
        let mut builder = ProgramBuilder::new();
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 0,
            out_reg: 1,
            // The in_a == in_b arm doubles in place and never reaches `op_union`,
            // so it takes the register whatever this says; `build_plan` emits
            // `false` for that shape rather than claim a take it does not perform.
            consume: false,
        });
        builder.push(Instr::Halt);
        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 3, 20)]);
        let reg_meta = [RegisterMeta::delta(schema); 2];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1)
            .unwrap()
            .unwrap();
        let rows = extract_rows(&result);
        assert_eq!(
            rows,
            vec![(1, 2, 10), (2, 6, 20)],
            "self-union (Z + Z) must double every weight",
        );
    }

    /// A non-consuming `Union` leaves its left operand in place, so an
    /// instruction after it still sees the batch. Without the flag such a plan
    /// could not be compiled at all — the register had to have no later reader.
    #[test]
    fn a_non_consuming_union_leaves_its_operand_readable() {
        let schema = schema_1i64();
        let mut builder = ProgramBuilder::new();
        // reg1 = -reg0; reg2 = reg0 ∪ reg1 (non-consuming); reg3 = -reg0 again.
        builder.push(Instr::Negate { in_reg: 0, out_reg: 1 });
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 1,
            out_reg: 2,
            consume: false,
        });
        builder.push(Instr::Negate { in_reg: 0, out_reg: 3 });
        builder.push(Instr::Halt);

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 3, 20)]);
        let reg_meta = [RegisterMeta::delta(schema); 4];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 3)
            .unwrap()
            .expect("the trailing reader must still see the union's operand");
        assert_eq!(extract_rows(&result), vec![(1, -1, 10), (2, -3, 20)]);
    }

    #[test]
    fn test_input_already_consolidated() {
        // Pre-consolidated input should not cause extra work.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        // pass-through (a union with an empty register is the identity op)
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        let input = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20)]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1)
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
        // pass-through (a union with an empty register is the identity op)
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let mut vm = *builder.build(reg_meta.to_vec());

        // Tick 1
        let input1 = make_batch(schema, &[(1u128, 1, 10)]);
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 1)
            .unwrap()
            .unwrap();
        assert_eq!(r1.count, 1);

        // Tick 2 with different data
        let input2 = make_batch(schema, &[(2u128, 1, 20), (3u128, 1, 30)]);
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 1)
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
        let mut builder = ProgramBuilder::new();
        let func_idx = builder.push_func(
            crate::expr::ScalarFunc::from_map(gnitz_expr::LogicalProgram::copy_cols(&[2]), &in_schema, &out_schema)
                .unwrap(),
        );
        builder.push(Instr::Map {
            in_reg: 0,
            out_reg: 1,
            func_idx,
            reindex: ReindexOperand::None,
        });
        builder.push(Instr::Halt);

        let input = make_batch_2col(in_schema, &[(1u128, 1, 10, 100), (2u128, 1, 20, 200)]);

        let reg_meta = [RegisterMeta::delta(in_schema), RegisterMeta::delta(out_schema)];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1)
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 2);
        // Projected column should be the second payload col (100, 200)
        assert_eq!(rows[0].2, 100);
        assert_eq!(rows[1].2, 200);
    }

    #[test]
    fn test_distinct_multi_tick() {
        // DISTINCT clamps weights: +3 → +1, -1 → 0 (stays positive → no retraction).
        // Uses a real Table for history.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let table = owned_table(dir.path(), "dist_test", schema);
        let mut builder = ProgramBuilder::new();
        // reg 0 = input delta, reg 1 = history trace, reg 2 = output delta
        let hist_table_idx = builder.push_table(table);
        builder.push(Instr::WeightClamp {
            in_reg: 0,
            hist_reg: 1,
            out_reg: 2,
            hist_table_idx,
            lo: -1,
            hi: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        let reg_meta = [
            RegisterMeta::delta(schema),
            RegisterMeta::trace(schema, TableIdx(0)),
            RegisterMeta::delta(schema),
        ];

        // The history register is backed by the plan's owned table, so each tick
        // opens its cursor through `bind_trace_cursors` — the production path.
        let mut vm = *builder.build(reg_meta.to_vec());

        // Tick 1: insert pk=1 with weight +3 → distinct output should be +1
        let input1 = make_batch(schema, &[(1u128, 3, 42)]);
        vm.bind_trace_cursors();
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2)
            .unwrap()
            .unwrap();

        let rows1 = extract_rows(&r1);
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0], (1, 1, 42)); // clamped to +1

        // Tick 2: delta w=-1, integral before tick = +3, after = +2 (still positive).
        // No boundary crossing → output should be empty.
        let input2 = make_batch(schema, &[(1u128, -1, 42)]);
        vm.bind_trace_cursors();
        let r2 = execute_epoch(&vm.program, &mut vm.regfile, input2, 0, 2);
        assert!(r2.unwrap().is_none(), "no boundary crossing: output should be empty");

        // Tick 3: delta w=-2, integral before tick = +2, after = 0 (non-positive).
        // Positive→non-positive boundary crossed → retraction: output pk=1 w=-1.
        let input3 = make_batch(schema, &[(1u128, -2, 42)]);
        vm.bind_trace_cursors();
        let r3 = execute_epoch(&vm.program, &mut vm.regfile, input3, 0, 2)
            .unwrap()
            .unwrap();
        let rows3 = extract_rows(&r3);
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0], (1, -1, 42)); // retraction
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
        let mut table = owned_table(dir.path(), "join_test", right_schema);
        // Ingest trace data
        let trace_batch = make_batch(right_schema, &[(10u128, 1, 200)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let mut builder = ProgramBuilder::new();
        builder.push_table(table);
        // reg 0 = left delta, reg 1 = right trace, reg 2 = output
        builder.push(Instr::JoinDT {
            probe: crate::ops::JoinProbe::Equi,
            delta_reg: 0,
            trace_reg: 1,
            out_reg: 2,
        });
        builder.push(Instr::Halt);

        let reg_meta = [
            RegisterMeta::delta(left_schema),
            RegisterMeta::trace(right_schema, TableIdx(0)),
            RegisterMeta::delta(join_schema),
        ];

        let mut vm = *builder.build(reg_meta.to_vec());
        vm.bind_trace_cursors();

        let input = make_batch(left_schema, &[(10u128, 1, 100)]);
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2)
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
    }

    #[test]
    fn test_join_delta_trace_multi_match() {
        // JoinDT: 1 delta row × N trace rows → N output rows.
        let left_schema = schema_1i64();
        let right_schema = schema_1i64();
        let join_schema = make_schema(&[type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();
        let mut table = owned_table(dir.path(), "join_multi_test", right_schema);
        // Ingest 3 trace rows with same PK but different payloads
        let trace_batch = make_batch(right_schema, &[(10u128, 1, 100), (10u128, 1, 200), (10u128, 1, 300)]);
        table.ingest_owned_batch(trace_batch).unwrap();

        let mut builder = ProgramBuilder::new();
        builder.push_table(table);
        builder.push(Instr::JoinDT {
            probe: crate::ops::JoinProbe::Equi,
            delta_reg: 0,
            trace_reg: 1,
            out_reg: 2,
        });
        builder.push(Instr::Halt);

        let reg_meta = [
            RegisterMeta::delta(left_schema),
            RegisterMeta::trace(right_schema, TableIdx(0)),
            RegisterMeta::delta(join_schema),
        ];
        let mut vm = *builder.build(reg_meta.to_vec());
        vm.bind_trace_cursors();

        let input = make_batch(left_schema, &[(10u128, 2, 50)]); // weight=2
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2)
            .unwrap()
            .unwrap();

        // Should produce 3 output rows (1 delta × 3 trace)
        assert_eq!(result.count, 3);
        // Each output weight = 2 * 1 = 2
        for i in 0..3 {
            let w = i64::from_le_bytes(result.weight_data()[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(w, 2);
        }
    }

    #[test]
    fn test_cursor_lifecycle() {
        // Table ingest + cursor creation + read.
        let schema = schema_1i64();

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cursor_test");
        let mut table = Table::with_arena(
            tdir.to_str().unwrap(),
            schema,
            0,
            1 << 20,
            crate::storage::RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();

        let batch = make_batch(schema, &[(1u128, 1, 10), (2u128, 1, 20), (3u128, 1, 30)]);
        table.ingest_owned_batch(batch).unwrap();

        let mut ch = table.open_cursor();
        let cursor = &mut ch;

        // Verify cursor iteration
        let mut count = 0;
        while cursor.valid {
            count += 1;
            cursor.advance();
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_input_consolidation() {
        // Pre-consolidated input with summed weights passes through correctly.
        // Input is consolidated before sending to the VM, so we simulate that here.
        let schema = schema_1i64();

        let mut builder = ProgramBuilder::new();
        // pass-through (a union with an empty register is the identity op)
        builder.push(Instr::Union {
            in_a: 0,
            in_b: 2,
            out_reg: 1,
            consume: true,
        });
        builder.push(Instr::Halt);

        // Already-consolidated input: weights were summed before VM entry
        let input = make_batch(schema, &[(1u128, 5, 42)]);

        let reg_meta = [RegisterMeta::delta(schema); 3];
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1)
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (1, 5, 42));
    }

    #[test]
    fn test_reduce_sum_multi_tick() {
        // REDUCE with SUM aggregation over a group column, over a real trace_out
        // table.
        let in_schema = make_schema(&[
            type_code::I64, // group col (payload col 0)
            type_code::I64, // agg col (payload col 1)
        ]);

        // Output schema: [U128 PK, I64 group_col, I64 sum_col, I64 count companion]
        let out_schema = make_schema(&[type_code::I64, type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();

        // trace_out table
        let trace_out_table = owned_table(dir.path(), "tr_out", out_schema);
        // Agg descriptors: SUM of payload col 1 (schema col 2), plus the trailing
        // Count cardinality companion every all-linear reduce carries.
        let agg_descs = [
            AggDescriptor {
                col_idx: 2,
                agg_op: AggFunc::Sum,
            },
            AggDescriptor {
                col_idx: 0,
                agg_op: AggFunc::Count,
            },
        ];
        let group_cols = [1u32]; // schema col 1 = payload col 0 (group key)

        let mut builder = ProgramBuilder::new();
        let trace_out_idx = builder.push_table(trace_out_table);
        // reg 0 = input delta
        // reg 1 = trace_out (trace register for output)
        // reg 2 = raw_delta output

        // REDUCE: reads from reg 0, trace_out=reg 1, output=reg 2
        push_reduce(
            &mut builder,
            0,
            1,
            2,
            &agg_descs,
            &group_cols,
            in_schema,
            out_schema,
            in_schema.reduce_out_key(&group_cols),
        );

        // INTEGRATE raw_delta → trace_out
        push_integrate(&mut builder, 2, trace_out_idx);

        builder.push(Instr::Halt);

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema, TableIdx(0)),
            RegisterMeta::delta(out_schema),
        ];

        // reg 1 = trace_out (owned table 0)
        let mut vm = *builder.build(reg_meta.to_vec());

        // Tick 1: Insert group=1 with values 10, 20
        let input1 = make_batch_2col(
            in_schema,
            &[
                (1u128, 1, 1, 10), // pk=1, group=1, val=10
                (2u128, 1, 1, 20), // pk=2, group=1, val=20
            ],
        );

        vm.bind_trace_cursors();
        let r1 = execute_epoch(&vm.program, &mut vm.regfile, input1, 0, 2)
            .unwrap()
            .unwrap();

        // SUM of group=1: 10+20 = 30. Output should be one row with sum=30.
        assert_eq!(r1.count, 1, "one group → one output row");
        let sum_val = i64::from_le_bytes(r1.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(sum_val, 30, "SUM(10+20) must be 30");
    }

    /// Filter with expression bytecode: col1 > 25 keeps rows with val 30, 40, 50.
    #[test]
    fn test_filter_with_expr() {
        use gnitz_expr::{CmpOp, LogicalInstr, LogicalProgram};

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

        let mut builder = ProgramBuilder::new();
        let func_idx = builder.push_func(crate::expr::ScalarFunc::from_predicate(prog, &schema).unwrap());
        builder.push(Instr::Filter {
            in_reg: 0,
            out_reg: 1,
            func_idx,
        });
        builder.push(Instr::Halt);

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
        let vm = builder.build(reg_meta.to_vec());
        let result = execute_epoch(&vm.program, &mut { vm.regfile }, input, 0, 1)
            .unwrap()
            .unwrap();

        let rows = extract_rows(&result);
        assert_eq!(rows.len(), 3, "filter col1>25 should keep 3 rows (30,40,50)");
        assert_eq!(rows[0].0, 3); // pk=3, val=30
        assert_eq!(rows[1].0, 4); // pk=4, val=40
        assert_eq!(rows[2].0, 5); // pk=5, val=50
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
        let trace_out_table = owned_table(dir.path(), "ma_tr_out", out_schema);
        // Two agg descriptors: COUNT(col=1) and SUM(col=1)
        let agg_descs = [
            AggDescriptor {
                col_idx: 1, // schema col index for the val column
                agg_op: AggFunc::Count,
            },
            AggDescriptor {
                col_idx: 1, // schema col index for the val column
                agg_op: AggFunc::Sum,
            },
        ];
        // GROUP BY col 0 (= pk, schema col index 0)
        let group_cols = [0u32];

        let mut builder = ProgramBuilder::new();
        let trace_out_idx = builder.push_table(trace_out_table);
        // reg 0 = input delta, reg 1 = trace_out, reg 2 = output
        push_reduce(
            &mut builder,
            0,
            1,
            2,
            &agg_descs,
            &group_cols,
            in_schema,
            out_schema,
            in_schema.reduce_out_key(&group_cols),
        );
        push_integrate(&mut builder, 2, trace_out_idx);
        builder.push(Instr::Halt);

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema, TableIdx(0)),
            RegisterMeta::delta(out_schema),
        ];
        let mut vm = *builder.build(reg_meta.to_vec());

        // Input: 3 rows all with pk=1, vals 10, 20, 30
        let input = make_batch(in_schema, &[(1u128, 1, 10), (1u128, 1, 20), (1u128, 1, 30)]);

        vm.bind_trace_cursors();
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 2)
            .unwrap()
            .unwrap();

        // Should produce 1 row: pk=1, count=3, sum=60
        assert_eq!(result.count, 1, "multi-agg should produce 1 group");
        let count_val = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        let sum_val = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(count_val, 3, "COUNT should be 3");
        assert_eq!(sum_val, 60, "SUM should be 60");
    }

    /// Proper SUM test: use agg_op=2 (AGG_SUM) and verify the actual aggregate
    /// value, not just that output is non-empty.
    #[test]
    fn test_reduce_sum_value() {
        let in_schema = make_schema(&[type_code::I64]);
        // [U128 PK, I64 sum, I64 count companion]
        let out_schema = make_schema(&[type_code::I64, type_code::I64]);

        let dir = tempfile::tempdir().unwrap();
        let trace_out_table = owned_table(dir.path(), "sv_tr_out", out_schema);
        // SUM of col 1 (agg_op=2 = AGG_SUM, not AGG_COUNT) plus the trailing Count
        // cardinality companion every all-linear reduce carries.
        let agg_descs = [
            AggDescriptor {
                col_idx: 1,
                agg_op: AggFunc::Sum,
            },
            AggDescriptor {
                col_idx: 0,
                agg_op: AggFunc::Count,
            },
        ];
        // GROUP BY col 0 (pk)
        let group_cols = [0u32];

        let mut builder = ProgramBuilder::new();
        let trace_out_idx = builder.push_table(trace_out_table);
        push_reduce(
            &mut builder,
            0,
            1,
            3,
            &agg_descs,
            &group_cols,
            in_schema,
            out_schema,
            in_schema.reduce_out_key(&group_cols),
        );
        push_integrate(&mut builder, 3, trace_out_idx);
        builder.push(Instr::Halt);

        let reg_meta = [
            RegisterMeta::delta(in_schema),
            RegisterMeta::trace(out_schema, TableIdx(0)),
            RegisterMeta::delta(out_schema),
            RegisterMeta::delta(out_schema),
        ];
        // reg 1 = trace_out (owned table 0)
        let mut vm = *builder.build(reg_meta.to_vec());

        // Three rows all in the same group (same pk), values 10, 20, 30 → SUM=60
        let input = make_batch(in_schema, &[(1u128, 1, 10), (1u128, 1, 20), (1u128, 1, 30)]);

        vm.bind_trace_cursors();
        let result = execute_epoch(&vm.program, &mut vm.regfile, input, 0, 3)
            .unwrap()
            .expect("SUM reduce must produce output");

        assert_eq!(result.count, 1, "one group → one output row");
        let sum_val = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(sum_val, 60, "SUM(10+20+30) must be 60");
    }
}
