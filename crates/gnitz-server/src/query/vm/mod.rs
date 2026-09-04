//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::expr::MapPlan;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::{Batch, ReadCursor, Table};

mod builder;
mod exec;

pub(in crate::query) use builder::ProgramBuilder;
pub(in crate::query) use exec::{execute_epoch_multi, Replay};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// The resource pools a `Program` owns are unrelated index spaces, all naturally
/// `u16`. An instruction can name two of them on adjacent lines, where a swap
/// would mis-dispatch into a live table with no panic — so each gets its own type
/// and the compiler does the checking.
macro_rules! resource_idx {
    ($($name:ident => $pool:literal;)*) => {$(
        #[doc = concat!("Index into `", $pool, "`.")]
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        pub(in crate::query) struct $name(pub u16);

        impl $name {
            #[inline]
            fn at(self) -> usize {
                self.0 as usize
            }
        }
    )*};
}

resource_idx! {
    TableIdx => "VmHandle::tables";
    PredIdx => "Program::predicates";
    MapIdx => "Program::maps";
    PlanIdx => "Program::reduce_plans";
}

/// One VM instruction with all operator-specific data pre-resolved. No variant
/// names a `TableIdx`: a table is reached through the register that owns it
/// ([`Program::trace_table_idx`]).
pub(in crate::query) enum Instr {
    Filter {
        in_reg: u16,
        out_reg: u16,
        pred_idx: PredIdx,
    },
    Map {
        in_reg: u16,
        out_reg: u16,
        map_idx: MapIdx,
    },
    Negate {
        in_reg: u16,
        out_reg: u16,
    },
    Union {
        in_a: u16,
        in_b: u16,
        out_reg: u16,
    },
    /// Shared instruction for `distinct` and `positive_part`: per consolidated
    /// (PK, payload), emit `clamp(w_new, lo, hi) − clamp(w_old, lo, hi)`. Bounds
    /// `(-1, 1)` ⇒ `distinct` (set membership); `(0, i64::MAX)` ⇒ `positive_part`
    /// (bag multiplicity).
    WeightClamp {
        in_reg: u16,
        /// The history trace: read through its cursor, and written through the
        /// table it owns.
        hist_reg: u16,
        out_reg: u16,
        lo: i64,
        hi: i64,
    },
    /// The delta-trace inner join, equi and range alike: the probe is baked by
    /// the compiler from the wire's `JoinKind`, so the wire's relation spelling
    /// never reaches the instruction set.
    JoinDT {
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
        probe: gnitz_store::ops::JoinProbe,
    },
    WorkerFilter {
        in_reg: u16,
        out_reg: u16,
        worker_id: u32,
        num_workers: u32,
    },
    /// Widen every row with NULL-filled trailing payload columns — the LEFT JOIN
    /// null-fill's unmatched preserved rows. The appended column count is the
    /// difference between the two registers' schemas, which the compiler built.
    NullExtend {
        in_reg: u16,
        out_reg: u16,
    },
    Integrate {
        in_reg: u16,
        /// The trace this delta accumulates into, named by its register.
        trace_reg: u16,
    },
    Reduce {
        in_reg: u16,
        trace_out_reg: u16,
        out_reg: u16,
        /// The baked [`BakedReduce`] carrying the schemas, group columns,
        /// aggregate descriptors, every derived gate (linearity, key kind,
        /// emission roles, ground flags), and the value-index table.
        plan_idx: PlanIdx,
    },
}

/// The registers whose **batch** `instr` reads — a trace port names a cursor and
/// a table, never a batch, so it is not one. Every field of every variant is
/// spelled out, no `..`: a future opcode's second delta input must be a compile
/// error here, not a register the liveness pass then lets someone take mid-read.
pub(in crate::query) fn reads(instr: &Instr) -> [Option<u16>; 2] {
    match instr {
        Instr::Filter { in_reg, out_reg: _, pred_idx: _ } => [Some(*in_reg), None],
        Instr::Map { in_reg, out_reg: _, map_idx: _ } => [Some(*in_reg), None],
        Instr::Negate { in_reg, out_reg: _ } => [Some(*in_reg), None],
        Instr::Union { in_a, in_b, out_reg: _ } => [Some(*in_a), Some(*in_b)],
        Instr::WeightClamp {
            in_reg,
            hist_reg: _,
            out_reg: _,
            lo: _,
            hi: _,
        } => [Some(*in_reg), None],
        Instr::JoinDT {
            delta_reg,
            trace_reg: _,
            out_reg: _,
            probe: _,
        } => [Some(*delta_reg), None],
        Instr::WorkerFilter {
            in_reg,
            out_reg: _,
            worker_id: _,
            num_workers: _,
        } => [Some(*in_reg), None],
        Instr::NullExtend { in_reg, out_reg: _ } => [Some(*in_reg), None],
        Instr::Integrate { in_reg, trace_reg: _ } => [Some(*in_reg), None],
        Instr::Reduce {
            in_reg,
            trace_out_reg: _,
            out_reg: _,
            plan_idx: _,
        } => [Some(*in_reg), None],
    }
}

/// True iff `instr` writes operator state on the bounded-view hydration replay,
/// which must not mutate the state it reads. `Integrate` is false because that
/// replay skips it — stated here, beside the dispatch, so the two cannot drift.
pub(in crate::query) fn writes_state_during_replay(instr: &Instr) -> bool {
    match instr {
        // `WeightClamp` writes its history table, `Reduce` its value index. A
        // `Reduce`'s output trace is written by the `Integrate` the emitter puts
        // behind it, not here.
        Instr::WeightClamp { .. } | Instr::Reduce { .. } => true,
        Instr::Integrate { .. }
        | Instr::Filter { .. }
        | Instr::Map { .. }
        | Instr::Negate { .. }
        | Instr::WorkerFilter { .. }
        | Instr::NullExtend { .. }
        | Instr::Union { .. }
        | Instr::JoinDT { .. } => false,
    }
}

/// Opaque handle owning a compiled program, its register file and its tables.
pub(in crate::query) struct VmHandle {
    pub(in crate::query) program: Program,
    pub(in crate::query) regfile: RegisterFile,
    /// Child tables created during compilation (integrate, history, reduce, AVI),
    /// in the index space `TableIdx` names. Mutable state, so it lives here and
    /// not on the immutable `Program`: the dispatch destructures the handle and
    /// holds `&Program` and `&mut [Table]` as the disjoint borrows they are.
    pub(in crate::query) tables: Vec<Table>,
    /// `(reg_id, backing table)` for every trace register, derived once from
    /// `program.reg_meta` so the per-epoch cursor bind walks only the trace
    /// registers instead of the whole register file — whose stride is a
    /// `RegisterMeta`, i.e. a whole `SchemaDescriptor`.
    trace_regs: Vec<(u16, TableIdx)>,
    /// The program carries a global-ground `Reduce` this worker owns whose ground
    /// row has not been minted yet — the one reason an empty epoch is worth
    /// dispatching.
    pub(super) pending_ground_row: bool,
}

// SAFETY: a VmHandle is only accessed from the single worker thread that owns
// the plan. Its tables and bound cursors hold `Rc`s into the thread's own batch
// and shard allocations, none of which is shared with another thread.
unsafe impl Send for VmHandle {}

impl VmHandle {
    /// The table at `idx`, for a caller that only reads it (a cursor open).
    pub(in crate::query) fn table(&self, idx: TableIdx) -> &Table {
        &self.tables[idx.at()]
    }

    /// Compact every owned trace table, keeping its L0 fan-in bounded — there is
    /// no background compactor. The epoch path's job, not a read's: a compaction
    /// mutates shard state. An `Err` leaves the shard index unchanged, so a
    /// cursor opened afterwards still sees a consistent snapshot.
    fn compact_owned_traces(&mut self) {
        let VmHandle { tables, trace_regs, .. } = self;
        for &(_reg, idx) in trace_regs.iter() {
            let _ = tables[idx.at()].compact_if_needed();
        }
    }

    /// Open a fresh cursor on every trace register's backing table. Eager rather
    /// than per-instruction, so an operator sees `z⁻¹(I(X))` — the integral
    /// before this tick's delta — by construction.
    fn bind_trace_cursors(&mut self) {
        gnitz_debug!("vm: bind_trace_cursors, {} trace regs", self.trace_regs.len());
        let VmHandle { regfile, tables, trace_regs, .. } = self;
        for &(reg_id, table_idx) in trace_regs.iter() {
            let cursor = tables[table_idx.at()].open_cursor();
            match &mut regfile.cursors[reg_id as usize] {
                Some(held) => **held = cursor,
                slot => *slot = Some(Box::new(cursor)),
            }
        }
    }

    /// Drop every bound cursor. Before a checkpoint fold that is tidiness — a
    /// held cursor owns `Rc` clones of what it reads, so a fold leaves it stale,
    /// not dangling — and after a backfill it is those clones, whole RAM-tier
    /// batches and mmaps, actually released.
    pub(super) fn reset_trace_cursors(&mut self) {
        self.regfile.cursors.fill_with(|| None);
    }

    /// Free what the register file holds — every batch's buffer, every cursor with
    /// the runs it pins. For the end of a backfill, whose last chunk would
    /// otherwise stay resident for the cached plan's lifetime.
    pub(super) fn release(&mut self) {
        self.regfile.release();
        self.reset_trace_cursors();
    }
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

/// Per-register metadata.
#[derive(Clone, Copy)]
pub(in crate::query) struct RegisterMeta {
    pub(in crate::query) schema: SchemaDescriptor,
    /// A trace register's backing table; `None` for a delta register. Naming the
    /// table here rather than in a side list is what lets `bind_trace_cursors`
    /// guarantee every trace register holds a live cursor at dispatch, and what
    /// keeps an instruction from naming a table its register disagrees with.
    pub(in crate::query) owned_table: Option<TableIdx>,
}

impl RegisterMeta {
    pub(super) const fn delta(schema: SchemaDescriptor) -> Self {
        Self { schema, owned_table: None }
    }
    pub(super) const fn trace(schema: SchemaDescriptor, owned_table: TableIdx) -> Self {
        Self { schema, owned_table: Some(owned_table) }
    }
}

/// One `Instr::Reduce`'s baked operator data: the plan and the table its combined
/// value index lives in. Paired at construction, so the dispatch never reconciles
/// "the plan carries a bake" against "the instruction names a table".
pub(in crate::query) struct BakedReduce {
    pub(in crate::query) plan: gnitz_store::ops::ReducePlan,
    pub(in crate::query) avi_table: Option<TableIdx>,
}

/// A compiled DBSP program: immutable once built, owning every resource its
/// instructions name except the mutable tables ([`VmHandle`]) — each in the index
/// space its own operand is a position in, so nothing is numbered twice.
pub(in crate::query) struct Program {
    pub(in crate::query) instructions: Vec<Instr>,
    pub(in crate::query) reg_meta: Vec<RegisterMeta>,
    /// Filter predicates.
    pub(in crate::query) predicates: Vec<gnitz_expr::Evaluator>,
    /// Map plans — maps, projections, post-reduce finalizes.
    pub(in crate::query) maps: Vec<MapPlan>,
    /// Baked per-`Instr::Reduce` plans, the reduce's value-index bake included.
    pub(in crate::query) reduce_plans: Vec<BakedReduce>,
    /// For each register, the pc of the last instruction that reads it — so an
    /// instruction may empty a register in place exactly when it is that reader.
    /// `u32::MAX` marks a register no instruction may take: the sink (read by the
    /// epoch epilogue, which no instruction spells) and any register nothing reads.
    pub(in crate::query) last_read: Vec<u32>,
    /// The register the epoch's output is extracted from. Here rather than passed
    /// in, because `last_read` bakes "the sink is never takeable" against it: two
    /// spellings could disagree and nothing would catch it.
    pub(in crate::query) out_reg: u16,
}

impl Program {
    /// The schema of the register this program's output leaves in.
    pub(in crate::query) fn out_schema(&self) -> SchemaDescriptor {
        self.reg_meta[self.out_reg as usize].schema
    }

    /// The table backing trace register `reg` — how a state-writing instruction
    /// reaches the table it writes, having named only the register that owns it.
    pub(in crate::query) fn trace_table_idx(&self, reg: u16) -> TableIdx {
        self.reg_meta[reg as usize]
            .owned_table
            .expect("a state-writing instruction names a register `push_trace_reg` allocated")
    }
}

// ---------------------------------------------------------------------------
// Register file (runtime state)
// ---------------------------------------------------------------------------

/// Runtime state for one plan execution: two arrays, both indexed by register id,
/// so a register's batch and its cursor are the same subscript. The schema and
/// kind live in the program's immutable `reg_meta` (one source of truth).
pub(in crate::query) struct RegisterFile {
    /// Per-register delta batch. A trace register's stays empty.
    pub(in crate::query) batches: Vec<Batch>,
    /// Trace cursors, indexed by register id; `None` for a delta register. Boxed:
    /// a `ReadCursor` is ~570 bytes, and inlining it here would put that on every
    /// delta register too, resident for the cached plan's lifetime.
    pub(in crate::query) cursors: Vec<Option<Box<ReadCursor>>>,
}

impl RegisterFile {
    /// Create from register metadata: zero-allocation empty batches (an input
    /// seed replaces a register's batch wholesale, so pre-sizing would only pin a
    /// pooled buffer per register for the cached plan's lifetime) and no cursors
    /// until `bind_trace_cursors` opens them.
    pub(super) fn new(metas: &[RegisterMeta]) -> Self {
        RegisterFile {
            batches: metas.iter().map(|m| Batch::empty_with_schema(&m.schema)).collect(),
            cursors: (0..metas.len()).map(|_| None).collect(),
        }
    }

    /// Clear every register's batch, keeping its buffer: the next epoch either
    /// seeds the register or writes it whole. A trace register is reached through
    /// its cursor, so its batch is already empty (`test_join_delta_trace`).
    pub(super) fn clear(&mut self) {
        for batch in &mut self.batches {
            batch.clear();
        }
    }

    /// [`Self::clear`], releasing the buffers too.
    pub(super) fn release(&mut self) {
        for batch in &mut self.batches {
            drop(batch.take());
        }
    }
}
