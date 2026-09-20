//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::expr::MapPlan;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, StateIdx};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;

mod builder;
mod exec;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(in crate::query) mod fixtures;

pub(in crate::query) use builder::build;
pub(in crate::query) use exec::{execute_epoch_multi, replay_chunk};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A register whose batch an instruction reads or writes — the one index space
/// a `Program` has, a trace being named by the [`StateIdx`] of its store.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) struct DeltaReg(pub u16);

impl DeltaReg {
    #[inline]
    fn at(self) -> usize {
        self.0 as usize
    }
}

/// One VM instruction: the delta it reads, the delta it writes, and the
/// operator between them, with all operator-specific data pre-resolved.
pub(in crate::query) struct Instr {
    pub(in crate::query) in_reg: DeltaReg,
    pub(in crate::query) out_reg: DeltaReg,
    pub(in crate::query) op: Op,
}

/// The operators, each boxing whatever the emitter baked for it.
pub(in crate::query) enum Op {
    Filter(Box<gnitz_expr::Evaluator>),
    Map(Box<MapPlan>),
    Negate,
    /// The one operator with a second delta operand.
    Union {
        in_b: DeltaReg,
    },
    /// Shared instruction for `distinct` and `positive_part`: per consolidated
    /// (PK, payload), emit `clamp(w_new) − clamp(w_old)` at `preset`'s bounds.
    WeightClamp {
        /// The history trace: read through a cursor, then written.
        hist: StateIdx,
        preset: ops::ClampPreset,
    },
    /// The delta-trace inner join, equi, range and cross alike: the probe is
    /// baked by the compiler from the wire's `JoinKind` and side flag, so neither
    /// spelling reaches the instruction set.
    JoinDT {
        trace: StateIdx,
        probe: ops::JoinProbe,
    },
    WorkerFilter {
        worker_id: u32,
        num_workers: u32,
    },
    /// Widen every row with NULL-filled payload columns — the LEFT JOIN
    /// null-fill's unmatched preserved rows — on the side `nulls_first` names.
    /// The column count is the difference between the two registers' schemas.
    NullExtend {
        nulls_first: bool,
    },
    Reduce {
        out_trace: StateIdx,
        plan: Box<BakedReduce>,
    },
    /// Per-group top-N: the ordered index of every input row is populated with
    /// the delta before the walk.
    TopN {
        out_trace: StateIdx,
        plan: Box<BakedTopN>,
    },
}

/// One `Op::Reduce`'s baked operator data: the plan and the table its combined
/// value index lives in.
pub(in crate::query) struct BakedReduce {
    pub(in crate::query) plan: gnitz_store::ops::ReducePlan,
    pub(in crate::query) avi_table: Option<StateIdx>,
}

/// One `Op::TopN`'s baked operator data: the plan and the table its ordered
/// index lives in.
pub(in crate::query) struct BakedTopN {
    pub(in crate::query) plan: gnitz_store::ops::TopNPlan,
    pub(in crate::query) index_table: StateIdx,
}

/// Everything the VM's passes need to know about one operator, classified once.
struct OpFacts {
    /// The second delta this operator reads.
    second_in: Option<DeltaReg>,
    /// It reads its input at net weights, so the VM folds that register first.
    consolidates_in: bool,
    /// It writes operator state — a history table, a value index, an ordered
    /// index. Not the output trace, which a `Program::integrates` entry writes.
    writes_state: bool,
    /// On an empty input delta it produces an empty output and touches no trace,
    /// so the dispatch loop can skip it whole.
    inert_on_empty: bool,
}

/// Each arm states its difference from `linear`, and matches its variant with
/// no `..`: an opcode that gains a second delta operand must break this, not
/// become a register the liveness pass lets someone take mid-read.
#[inline]
fn facts(op: &Op) -> OpFacts {
    let linear = OpFacts {
        second_in: None,
        consolidates_in: false,
        writes_state: false,
        inert_on_empty: false,
    };
    match op {
        Op::Filter(_)
        | Op::Map(_)
        | Op::Negate
        | Op::WorkerFilter { worker_id: _, num_workers: _ }
        | Op::NullExtend { nulls_first: _ } => linear,
        Op::Union { in_b } => OpFacts { second_in: Some(*in_b), ..linear },
        Op::WeightClamp { hist: _, preset: _ } => OpFacts {
            consolidates_in: true,
            writes_state: true,
            inert_on_empty: true,
            ..linear
        },
        Op::JoinDT { trace: _, probe: _ } => OpFacts {
            consolidates_in: true,
            inert_on_empty: true,
            ..linear
        },
        Op::Reduce { out_trace: _, plan } => OpFacts {
            consolidates_in: plan.plan.consolidates_input(),
            writes_state: true,
            inert_on_empty: !plan.plan.seeds_ground,
            ..linear
        },
        Op::TopN { out_trace: _, plan: _ } => OpFacts {
            writes_state: true,
            inert_on_empty: true,
            ..linear
        },
    }
}

impl Instr {
    /// The registers whose batch this instruction reads, in operand order.
    #[inline]
    fn reads(&self) -> [Option<DeltaReg>; 2] {
        [Some(self.in_reg), facts(&self.op).second_in]
    }
}

/// Opaque handle owning a compiled program, its registers and its tables.
pub(in crate::query) struct VmHandle {
    pub(in crate::query) program: Program,
    /// One batch per delta register.
    batches: Vec<Batch>,
    /// The child stores created during compilation, in the index space
    /// [`StateIdx`] names. Here and not on the immutable `Program` so the
    /// dispatch can hold `&Program` and `&mut CircuitState` at once.
    pub(in crate::query) state: CircuitState,
    /// The program carries a global-ground `Reduce` this worker owns whose ground
    /// row has not been minted yet — the one reason an empty epoch is worth
    /// dispatching.
    pending_ground_row: bool,
}

impl VmHandle {
    /// Free every register's buffer. For the end of a backfill, whose last chunk
    /// would otherwise stay resident for the cached plan's lifetime.
    pub(super) fn release(&mut self) {
        for batch in &mut self.batches {
            batch.release_buffers();
        }
    }
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

/// A compiled DBSP program: immutable once built, owning every resource its
/// instructions name except the mutable child stores ([`VmHandle`]).
pub(in crate::query) struct Program {
    instructions: Vec<Instr>,
    /// `(delta, trace)`, run after the whole instruction range — and not at all
    /// by a replay, which is what makes a replay read-only.
    integrates: Vec<(DeltaReg, StateIdx)>,
    delta_schemas: Vec<SchemaDescriptor>,
    /// For each register, the pc of the last instruction that reads it — so an
    /// instruction may empty a register in place exactly when it is that reader.
    /// An integrate's pc continues past the instruction range. `u32::MAX` marks a
    /// register no instruction may take: the sink (read by the epoch epilogue,
    /// which no instruction spells) and any register nothing reads.
    last_read: Vec<u32>,
    /// For each register some instruction consolidates, the pc of its **first**
    /// reader, which folds it in place. `u32::MAX` elsewhere.
    consolidate_at: Vec<u32>,
    /// The register the epoch's output is extracted from. Here rather than passed
    /// in, because `last_read` bakes "the sink is never takeable" against it: two
    /// spellings could disagree and nothing would catch it.
    out_reg: DeltaReg,
}

/// The pc of the first instruction reading `reg`, or `instructions.len()` when
/// none does.
fn first_read_of(instructions: &[Instr], reg: DeltaReg) -> usize {
    instructions
        .iter()
        .position(|i| i.reads().into_iter().flatten().any(|r| r == reg))
        .unwrap_or(instructions.len())
}

impl Program {
    /// The schema register `reg` is labelled with. By reference: the dispatch
    /// loop hands it straight to kernels, and a `SchemaDescriptor` is not cheap
    /// to copy.
    pub(in crate::query) fn schema_of(&self, reg: DeltaReg) -> &SchemaDescriptor {
        &self.delta_schemas[reg.at()]
    }

    /// The pc of the first instruction reading `reg`, or the program length when
    /// none does. Each register has exactly one writer, strictly before this —
    /// `push_delta_reg` mints a fresh one per emitting node — so a replay that
    /// seeds `reg` may enter here and find its value intact.
    ///
    /// Only meaningful for a register some instruction reads: an integrate is
    /// past the range this counts over.
    pub(in crate::query) fn first_read(&self, reg: DeltaReg) -> usize {
        first_read_of(&self.instructions, reg)
    }

    /// The schema of the register this program's output leaves in.
    pub(in crate::query) fn out_schema(&self) -> &SchemaDescriptor {
        self.schema_of(self.out_reg)
    }

    /// The register the epoch's output is extracted from.
    #[cfg(test)]
    pub(in crate::query) fn out_reg(&self) -> DeltaReg {
        self.out_reg
    }

    /// The operators, in program order.
    #[cfg(test)]
    pub(in crate::query) fn ops(&self) -> impl Iterator<Item = &Op> + '_ {
        self.instructions.iter().map(|i| &i.op)
    }

    /// Every instruction with, per operand, whether it is that operand's last
    /// reader — the take verdict the dispatch acts on.
    #[cfg(test)]
    pub(in crate::query) fn take_verdicts(&self) -> impl Iterator<Item = (&Op, [Option<bool>; 2])> + '_ {
        self.instructions.iter().enumerate().map(|(pc, instr)| {
            let takes = instr.reads().map(|r| r.map(|r| self.last_read[r.at()] == pc as u32));
            (&instr.op, takes)
        })
    }

    /// True iff some instruction trims the delta to this worker's own rows, so
    /// the result is a slice rather than a copy of what every worker computes.
    pub(in crate::query) fn trims_per_worker(&self) -> bool {
        self.instructions
            .iter()
            .any(|i| matches!(i.op, Op::WorkerFilter { .. }))
    }

    /// True iff any instruction from `start_pc` on writes operator state.
    pub(in crate::query) fn writes_state_from(&self, start_pc: usize) -> bool {
        self.instructions[start_pc..].iter().any(|i| facts(&i.op).writes_state)
    }
}
