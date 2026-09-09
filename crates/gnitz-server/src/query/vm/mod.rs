//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::expr::MapPlan;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, StateIdx};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::{Batch, ReadCursor};

mod builder;
mod exec;

pub(in crate::query) use builder::ProgramBuilder;
pub(in crate::query) use exec::{execute_epoch_multi, Replay};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A `u16` instruction operand. The resource pools a `Program` owns and its
/// register file are unrelated index spaces; an instruction names two of them on
/// adjacent lines, where a swap would mis-dispatch with no panic — so each gets
/// its own type and the compiler does the checking.
macro_rules! operand_idx {
    ($($(#[$doc:meta])* $name:ident;)*) => {$(
        $(#[$doc])*
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

operand_idx! {
    /// Index into `Program::predicates`.
    PredIdx;
    /// Index into `Program::maps`.
    MapIdx;
    /// Index into `Program::reduce_plans`.
    PlanIdx;
    /// Index into `Program::topn_plans`.
    TopNIdx;
    /// A register whose **batch** an instruction reads or writes. Minted only by
    /// `push_delta_reg`.
    ///
    /// A trace register's batch is never written and is cleared each epoch, so
    /// reading one as a delta reads permanent emptiness — silently, with no
    /// panic. Keeping the kinds apart is what makes that unrepresentable.
    DeltaReg;
    /// A register that owns a trace table, read through the cursor
    /// `bind_trace_cursors` opens on it each epoch. Minted only by
    /// `push_trace_reg`, which always creates the table.
    TraceReg;
}

/// Only the two register kinds convert back: `Program::schema_of` reads either.
impl From<DeltaReg> for u16 {
    fn from(r: DeltaReg) -> u16 {
        r.0
    }
}

impl From<TraceReg> for u16 {
    fn from(r: TraceReg) -> u16 {
        r.0
    }
}

/// One VM instruction with all operator-specific data pre-resolved. No variant
/// names a [`StateIdx`]: a child store is reached through the register that owns
/// it ([`Program::trace_table_idx`]).
pub(in crate::query) enum Instr {
    Filter {
        in_reg: DeltaReg,
        out_reg: DeltaReg,
        pred_idx: PredIdx,
    },
    Map {
        in_reg: DeltaReg,
        out_reg: DeltaReg,
        map_idx: MapIdx,
    },
    Negate {
        in_reg: DeltaReg,
        out_reg: DeltaReg,
    },
    Union {
        in_a: DeltaReg,
        in_b: DeltaReg,
        out_reg: DeltaReg,
    },
    /// Shared instruction for `distinct` and `positive_part`: per consolidated
    /// (PK, payload), emit `clamp(w_new) − clamp(w_old)` at `preset`'s bounds.
    WeightClamp {
        in_reg: DeltaReg,
        /// The history trace: read through its cursor, and written through the
        /// table it owns.
        hist_reg: TraceReg,
        out_reg: DeltaReg,
        preset: ops::ClampPreset,
    },
    /// The delta-trace inner join, equi and range alike: the probe is baked by
    /// the compiler from the wire's `JoinKind`, so the wire's relation spelling
    /// never reaches the instruction set.
    JoinDT {
        delta_reg: DeltaReg,
        trace_reg: TraceReg,
        out_reg: DeltaReg,
        probe: gnitz_store::ops::JoinProbe,
    },
    WorkerFilter {
        in_reg: DeltaReg,
        out_reg: DeltaReg,
        worker_id: u32,
        num_workers: u32,
    },
    /// Widen every row with NULL-filled trailing payload columns — the LEFT JOIN
    /// null-fill's unmatched preserved rows. The appended column count is the
    /// difference between the two registers' schemas, which the compiler built.
    NullExtend {
        in_reg: DeltaReg,
        out_reg: DeltaReg,
    },
    Integrate {
        in_reg: DeltaReg,
        /// The trace this delta accumulates into, named by its register.
        trace_reg: TraceReg,
    },
    Reduce {
        in_reg: DeltaReg,
        trace_out_reg: TraceReg,
        out_reg: DeltaReg,
        /// The baked [`BakedReduce`] carrying the schemas, group columns,
        /// aggregate descriptors, every derived gate (linearity, key kind,
        /// emission roles, ground flags), and the value-index table.
        plan_idx: PlanIdx,
    },
    /// Per-group top-N: the ordered index of every input row is populated with
    /// the delta before the walk, and the output integrates into `trace_out_reg`
    /// through the `Integrate` the emitter puts behind it.
    TopN {
        in_reg: DeltaReg,
        trace_out_reg: TraceReg,
        out_reg: DeltaReg,
        plan_idx: TopNIdx,
    },
}

/// The registers whose **batch** `instr` reads — a trace port names a cursor and
/// a table, never a batch, so it is not one. Every field of every variant is
/// spelled out, no `..`: a future opcode's second delta input must be a compile
/// error here, not a register the liveness pass then lets someone take mid-read.
pub(in crate::query) fn reads(instr: &Instr) -> [Option<DeltaReg>; 2] {
    match instr {
        Instr::Filter { in_reg, out_reg: _, pred_idx: _ } => [Some(*in_reg), None],
        Instr::Map { in_reg, out_reg: _, map_idx: _ } => [Some(*in_reg), None],
        Instr::Negate { in_reg, out_reg: _ } => [Some(*in_reg), None],
        Instr::Union { in_a, in_b, out_reg: _ } => [Some(*in_a), Some(*in_b)],
        Instr::WeightClamp {
            in_reg,
            hist_reg: _,
            out_reg: _,
            preset: _,
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
        Instr::TopN {
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
        // `WeightClamp` writes its history table, `Reduce` its value index,
        // `TopN` its ordered index. A `Reduce`'s or `TopN`'s output trace is
        // written by the `Integrate` the emitter puts behind it, not here.
        Instr::WeightClamp { .. } | Instr::Reduce { .. } | Instr::TopN { .. } => true,
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
    /// The child stores created during compilation, in the index space
    /// [`StateIdx`] names. Here and not on the immutable `Program` so the
    /// dispatch can hold `&Program` and `&mut CircuitState` at once.
    pub(in crate::query) state: CircuitState,
    /// `(reg_id, backing table)` for every trace register, derived once from
    /// `program.reg_meta` so the per-epoch cursor bind walks only the trace
    /// registers instead of the whole register file — whose stride is a
    /// `RegisterMeta`, i.e. a whole `SchemaDescriptor`.
    trace_regs: Vec<(TraceReg, StateIdx)>,
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
    /// Open a fresh cursor on every trace register's backing store. Eager rather
    /// than per-instruction, so an operator sees `z⁻¹(I(X))` — the integral
    /// before this tick's delta — by construction.
    fn bind_trace_cursors(&mut self) {
        gnitz_debug!("vm: bind_trace_cursors, {} trace regs", self.trace_regs.len());
        let VmHandle { regfile, state, trace_regs, .. } = self;
        for &(reg_id, table_idx) in trace_regs.iter() {
            let cursor = state.cursor(table_idx);
            match &mut regfile.cursors[reg_id.at()] {
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
        self.regfile.clear();
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
    pub(in crate::query) owned_table: Option<StateIdx>,
}

impl RegisterMeta {
    pub(super) const fn delta(schema: SchemaDescriptor) -> Self {
        Self { schema, owned_table: None }
    }
    pub(super) const fn trace(schema: SchemaDescriptor, owned_table: StateIdx) -> Self {
        Self { schema, owned_table: Some(owned_table) }
    }
}

/// One `Instr::Reduce`'s baked operator data: the plan and the table its combined
/// value index lives in. Paired at construction, so the dispatch never reconciles
/// "the plan carries a bake" against "the instruction names a table".
pub(in crate::query) struct BakedReduce {
    pub(in crate::query) plan: gnitz_store::ops::ReducePlan,
    pub(in crate::query) avi_table: Option<StateIdx>,
}

/// One `Instr::TopN`'s baked operator data: the plan and the table its ordered
/// index lives in.
pub(in crate::query) struct BakedTopN {
    pub(in crate::query) plan: gnitz_store::ops::TopNPlan,
    pub(in crate::query) index_table: StateIdx,
}

/// A compiled DBSP program: immutable once built, owning every resource its
/// instructions name except the mutable child stores ([`VmHandle`]) — each in the index
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
    /// Baked per-`Instr::TopN` plans, each with its index table.
    pub(in crate::query) topn_plans: Vec<BakedTopN>,
    /// For each register, the pc of the last instruction that reads it — so an
    /// instruction may empty a register in place exactly when it is that reader.
    /// `u32::MAX` marks a register no instruction may take: the sink (read by the
    /// epoch epilogue, which no instruction spells) and any register nothing reads.
    pub(in crate::query) last_read: Vec<u32>,
    /// The register the epoch's output is extracted from. Here rather than passed
    /// in, because `last_read` bakes "the sink is never takeable" against it: two
    /// spellings could disagree and nothing would catch it.
    pub(in crate::query) out_reg: DeltaReg,
}

impl Program {
    /// The schema register `reg` is labelled with. By reference: the dispatch
    /// loop hands it straight to kernels, and a `SchemaDescriptor` is 360 bytes.
    pub(in crate::query) fn schema_of(&self, reg: impl Into<u16>) -> &SchemaDescriptor {
        &self.reg_meta[reg.into() as usize].schema
    }

    /// The pc of the first instruction reading `reg`, or the program length when
    /// none does. Each register has exactly one writer, strictly before this —
    /// `push_delta_reg` mints a fresh one per emitting node — so a replay that
    /// seeds `reg` may enter here and find its value intact.
    pub(in crate::query) fn first_read(&self, reg: DeltaReg) -> usize {
        self.instructions
            .iter()
            .position(|i| reads(i).into_iter().flatten().any(|r| r == reg))
            .unwrap_or(self.instructions.len())
    }

    /// The schema of the register this program's output leaves in.
    pub(in crate::query) fn out_schema(&self) -> SchemaDescriptor {
        *self.schema_of(self.out_reg)
    }

    /// The table backing trace register `reg` — how a state-writing instruction
    /// reaches the table it writes, having named only the register that owns it.
    /// A [`TraceReg`] proves the kind, not that it indexes *this* program.
    pub(in crate::query) fn trace_table_idx(&self, reg: TraceReg) -> StateIdx {
        self.reg_meta[reg.at()]
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

    /// Empty every register and return its buffers to the pool. Keeping them
    /// buys nothing — every dispatch arm assigns `batches[out] = result`, so a
    /// retained capacity is dropped at the next write anyway.
    pub(super) fn clear(&mut self) {
        for batch in &mut self.batches {
            batch.release_buffers();
        }
    }
}
