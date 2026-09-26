//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::expr::MapPlan;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, StateIdx};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::{Batch, Slot};

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

/// One VM instruction: the delta it reads, the delta it writes, the operator
/// between them with all operator-specific data pre-resolved, and the verdicts
/// [`build`] baked for it.
pub(in crate::query) struct Instr {
    pub(in crate::query) in_reg: DeltaReg,
    pub(in crate::query) out_reg: DeltaReg,
    pub(in crate::query) op: Op,
    /// Per [`Instr::reads`] operand: no later reader, so this one may take it.
    takes: [bool; 2],
    /// Per [`Instr::reads`] operand: the first reader of a register some
    /// instruction needs at net weights, so this one folds it.
    folds: [bool; 2],
    /// [`OpFacts::inert_on_empty`], baked.
    inert_on_empty: bool,
}

/// The operators, each boxing whatever the emitter baked for it.
pub(in crate::query) enum Op {
    Filter(Box<gnitz_expr::RowFilter>),
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
        slot: Slot,
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
    pub(in crate::query) plan: ops::ReducePlan,
    avi_table: Option<StateIdx>,
}

impl BakedReduce {
    pub(in crate::query) fn new(plan: ops::ReducePlan, avi_table: Option<StateIdx>) -> BakedReduce {
        assert_eq!(
            plan.avi.is_some(),
            avi_table.is_some(),
            "a reduce's value-index bake and its table are set together",
        );
        BakedReduce { plan, avi_table }
    }

    /// The combined value index: its table and the bake that projects into it.
    fn avi(&self) -> Option<(StateIdx, &ops::AviBake)> {
        self.avi_table.zip(self.plan.avi.as_ref())
    }
}

/// One `Op::TopN`'s baked operator data: the plan and the table its ordered
/// index lives in.
pub(in crate::query) struct BakedTopN {
    pub(in crate::query) plan: gnitz_store::ops::TopNPlan,
    pub(in crate::query) index_table: StateIdx,
}

/// Everything the VM's passes need to know about one operator, classified once.
struct OpFacts {
    /// It reads its input at net weights, so the VM folds that register first.
    consolidates_in: bool,
    /// It writes operator state — a history table, a value index, an ordered
    /// index. Not the output trace, which a `Program::integrates` entry writes.
    writes_state: bool,
    /// With every delta operand empty its kernel returns
    /// `empty_with_schema(out_reg)` and touches no trace.
    inert_on_empty: bool,
}

fn facts(op: &Op) -> OpFacts {
    let linear = OpFacts {
        consolidates_in: false,
        writes_state: false,
        inert_on_empty: true,
    };
    match op {
        Op::Filter(_)
        | Op::Map(_)
        | Op::Negate
        | Op::Union { .. }
        | Op::WorkerFilter { .. }
        | Op::NullExtend { .. } => linear,
        Op::WeightClamp { .. } => OpFacts {
            consolidates_in: true,
            writes_state: true,
            ..linear
        },
        Op::JoinDT { .. } => OpFacts { consolidates_in: true, ..linear },
        Op::Reduce { plan, .. } => OpFacts {
            consolidates_in: !plan.plan.is_exact_linear(),
            writes_state: true,
            // A global-ground reduce mints V₀ from an empty delta.
            inert_on_empty: !plan.plan.seeds_ground,
        },
        Op::TopN { .. } => OpFacts { writes_state: true, ..linear },
    }
}

impl Instr {
    /// The verdicts are [`build`]'s to fill.
    pub(in crate::query) fn new(in_reg: DeltaReg, out_reg: DeltaReg, op: Op) -> Instr {
        Instr {
            in_reg,
            out_reg,
            op,
            takes: [false; 2],
            folds: [false; 2],
            inert_on_empty: false,
        }
    }

    /// The registers whose batch this instruction reads, in operand order. No
    /// `..` in the match: an opcode that gains a delta operand must fail to
    /// compile here.
    #[inline]
    fn reads(&self) -> [Option<DeltaReg>; 2] {
        let second = match &self.op {
            Op::Union { in_b } => Some(*in_b),
            Op::Filter(_)
            | Op::Map(_)
            | Op::Negate
            | Op::WeightClamp { hist: _, preset: _ }
            | Op::JoinDT { trace: _, probe: _ }
            | Op::WorkerFilter { slot: _ }
            | Op::NullExtend { nulls_first: _ }
            | Op::Reduce { out_trace: _, plan: _ }
            | Op::TopN { out_trace: _, plan: _ } => None,
        };
        [Some(self.in_reg), second]
    }

    /// Each operand [`Instr::reads`] has, with its index into `takes`/`folds`.
    #[inline]
    fn operands(&self) -> impl Iterator<Item = (usize, DeltaReg)> {
        self.reads()
            .into_iter()
            .enumerate()
            .filter_map(|(i, r)| r.map(|r| (i, r)))
    }
}

/// A compiled program with the registers it runs over.
pub(in crate::query) struct VmHandle {
    pub(in crate::query) program: Program,
    /// One batch per delta register.
    batches: Vec<Batch>,
    /// No epoch has been dispatched yet and the program carries a global-ground
    /// `Reduce` this worker owns — the one reason an all-empty epoch is worth
    /// dispatching.
    pub(in crate::query) pending_ground_row: bool,
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

/// A compiled DBSP program, owning every resource its instructions name except
/// the child stores ([`CircuitState`]).
pub(in crate::query) struct Program {
    instructions: Vec<Instr>,
    /// Run after the whole instruction range — and not at all by a replay, which
    /// is what makes a replay read-only.
    integrates: Vec<Integrate>,
    delta_schemas: Vec<SchemaDescriptor>,
    /// The register the epoch's output is extracted from. Here rather than passed
    /// in, because `build` bakes "the sink is never takeable" against it: two
    /// spellings could disagree and nothing would catch it.
    out_reg: DeltaReg,
}

/// One tick's accumulation of a register into its trace.
struct Integrate {
    reg: DeltaReg,
    trace: StateIdx,
    /// Nothing reads `reg` after this, so the ingest may take its batch.
    take: bool,
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
        self.instructions.iter().map(|instr| {
            let reads = instr.reads();
            let takes = [reads[0].map(|_| instr.takes[0]), reads[1].map(|_| instr.takes[1])];
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
