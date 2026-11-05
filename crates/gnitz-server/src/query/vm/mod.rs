//! DBSP VM: executes compiled circuit programs.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::rc::Rc;

use gnitz_store::relation::{CircuitState, StateIdx, StateLayout};
use gnitz_wire::ClampKind;
use gnitz_zset::algebra::{MapPlan, ScatterPlan};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use gnitz_zset::stream;

mod builder;
mod exec;

pub(in crate::query) use builder::ProgramBuilder;
pub(in crate::query) use exec::{mint_ground_rows, replay_chunk, run, Epoch, Ran, Stores};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A register whose batch an instruction reads or writes — the one index space
/// a [`Vm`] has, a trace being named by its [`Integral`]. Minted only by
/// [`ProgramBuilder`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) struct DeltaReg(usize);

impl DeltaReg {
    #[inline]
    fn at(self) -> usize {
        self.0
    }
}

/// One VM instruction: the delta it reads, the delta it writes, and the
/// operator between them with all operator-specific data pre-resolved.
struct Instr {
    in_reg: DeltaReg,
    out_reg: DeltaReg,
    op: Op,
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
    /// (PK, payload), emit `clamp(w_new) − clamp(w_old)` at `kind`'s bounds.
    WeightClamp {
        hist: ClampHistory,
        kind: ClampKind,
    },
    /// One term of a join, delta against trace — equi, range and cross alike:
    /// the plan is baked by the compiler from the wire's `JoinKind` and the
    /// term's side, so neither reaches the instruction set.
    JoinDT {
        trace: Integral,
        plan: Box<stream::JoinPlan>,
    },
    /// Keep the rows this worker owns under the plan: its share of a delta every
    /// worker holds whole.
    Share(Rc<ScatterPlan>),
    /// One exchange round under the plan: every worker's delta in, this worker's
    /// share of them all out. `seeds`: the seeded registers whose delta reaches
    /// it. An epoch runs the round iff it seeds one of them, whatever the delta
    /// holds, so every worker runs the same rounds.
    Round {
        plan: Rc<ScatterPlan>,
        seeds: Box<[DeltaReg]>,
    },
    /// Widen every row with NULL-filled payload columns — the LEFT JOIN
    /// null-fill's unmatched preserved rows — on the side `nulls_first` names.
    /// The column count is the difference between the two registers' schemas.
    NullExtend {
        nulls_first: bool,
    },
    /// `out_trace`: the integral of the output register — the view's own store
    /// where that register is the view's output.
    /// `index`: the table the value index of its MIN/MAX aggregates lives in.
    Reduce {
        out_trace: Integral,
        index: Option<StateIdx>,
        plan: Box<stream::ReducePlan>,
    },
    /// A top-N or a window: per group, over `index`, the ordered index of every
    /// input row.
    Ordered {
        index: StateIdx,
        plan: Box<stream::OrderedPlan>,
    },
}

/// Where a clamp reads `I(in)`, its input's history.
pub(in crate::query) enum ClampHistory {
    /// A child store the input is integrated into.
    Own(StateIdx),
    /// The stores of the base tables the input is a signed sum of. Nothing is
    /// integrated.
    Sources(Box<[HistoryTerm]>),
}

/// One base table's term of a [`ClampHistory::Sources`].
pub(in crate::query) struct HistoryTerm {
    pub(in crate::query) relation: u64,
    /// The filters and maps, by position, that turn a row of the relation into
    /// one of the clamp's input.
    pub(in crate::query) steps: Box<[usize]>,
    pub(in crate::query) negated: bool,
}

/// The integral of a delta, as its readers find it: a join's probe, a reduce's
/// output trace (`Op::Reduce::out_trace`), and a bounded view's replay, which
/// seeds from it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) enum Integral {
    /// A child store the circuit integrates a register into.
    Own(StateIdx),
    /// A relation's store as of its last seal, which its own ingest keeps: the
    /// table a delta is scanned from, read as the view last absorbed it, or the
    /// view the plan computes. Nothing is integrated.
    Relation(u64),
}

/// What the VM's passes ask of one operator.
#[derive(Clone, Copy)]
struct OpFacts {
    /// It reads its input at net weights, so the VM folds that register when it
    /// is written.
    consolidates_in: bool,
    /// Its output depends on more than its operands — its input's history, a
    /// value index, an ordered index, the other workers' rows — so a replay
    /// past it would read that against a seed it never saw.
    stateful: bool,
}

fn facts(op: &Op) -> OpFacts {
    let linear = OpFacts { consolidates_in: false, stateful: false };
    match op {
        Op::Filter(_) | Op::Map(_) | Op::Negate | Op::Union { .. } | Op::Share(_) | Op::NullExtend { .. } => linear,
        Op::Round { .. } => OpFacts { stateful: true, ..linear },
        Op::WeightClamp { .. } => OpFacts { consolidates_in: true, stateful: true },
        Op::JoinDT { .. } => OpFacts { consolidates_in: true, ..linear },
        Op::Reduce { plan, .. } => OpFacts {
            consolidates_in: !plan.is_exact_linear(),
            stateful: true,
        },
        Op::Ordered { .. } => OpFacts { stateful: true, ..linear },
    }
}

impl Instr {
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
            | Op::WeightClamp { hist: _, kind: _ }
            | Op::JoinDT { trace: _, plan: _ }
            | Op::Share(_)
            | Op::Round { plan: _, seeds: _ }
            | Op::NullExtend { nulls_first: _ }
            | Op::Reduce { out_trace: _, index: _, plan: _ }
            | Op::Ordered { index: _, plan: _ } => None,
        };
        [Some(self.in_reg), second]
    }
}

// ---------------------------------------------------------------------------
// Vm
// ---------------------------------------------------------------------------

/// A compiled DBSP program and the registers it runs over. It owns every
/// resource its instructions name, and the layout of the child stores they
/// address; the stores themselves are the caller's: the view's own children
/// ([`CircuitState`]) and the relations an [`Integral::Relation`] reads.
pub(in crate::query) struct Vm {
    instructions: Box<[Instr]>,
    /// Each tick's accumulation of a register into its trace. Run after the
    /// whole instruction range — and not at all by a replay, which is what makes
    /// a replay read-only.
    integrates: Box<[(DeltaReg, StateIdx)]>,
    regs: Box<[Reg]>,
    /// One batch per register.
    batches: Box<[Batch]>,
    /// The register the epoch's output is extracted from.
    out_reg: DeltaReg,
    /// The child stores the instructions and integrates address.
    layout: StateLayout,
}

/// Who last reads a register: an instruction, an integrate — which run after
/// every instruction — or nobody: unread, or the output register, which the
/// epoch extracts after both.
#[derive(Clone, Copy, PartialEq, Debug)]
enum LastRead {
    Nobody,
    Instr(usize),
    Integrate(usize),
}

/// What [`ProgramBuilder::finish`] derived about one register.
struct Reg {
    schema: SchemaDescriptor,
    /// Some instruction reads it at net weights, so it is folded when written.
    fold: bool,
    /// Its last reader, which may take its batch.
    last_read: LastRead,
    /// An epoch seeding it runs an exchange round.
    feeds_round: bool,
}

/// Where a read-only replay enters a program: the register it seeds and the
/// first instruction reading it.
#[derive(Clone, Copy)]
pub(in crate::query) struct ReplayEntry {
    reg: DeltaReg,
    pc: usize,
}

impl Vm {
    /// The schema register `reg` is labelled with.
    pub(in crate::query) fn schema_of(&self, reg: DeltaReg) -> &SchemaDescriptor {
        &self.regs[reg.at()].schema
    }

    /// The schema of the register the epoch's output leaves in.
    pub(in crate::query) fn out_schema(&self) -> &SchemaDescriptor {
        self.schema_of(self.out_reg)
    }

    /// The child stores this program addresses, to be opened as its
    /// [`CircuitState`].
    pub(in crate::query) fn layout(&self) -> &StateLayout {
        &self.layout
    }

    /// True iff a reduce reads the view's own store as its output trace.
    pub(in crate::query) fn reads_view_store(&self) -> bool {
        self.instructions
            .iter()
            .any(|i| matches!(i.op, Op::Reduce { out_trace: Integral::Relation(..), .. }))
    }

    /// Some instruction reads `reg` at net weights, so its batch is folded when
    /// it stands.
    pub(in crate::query) fn folds(&self, reg: DeltaReg) -> bool {
        self.regs[reg.at()].fold
    }

    /// An epoch seeding `reg` with an empty delta does nothing: it runs no
    /// exchange round.
    pub(in crate::query) fn idles_on_empty(&self, reg: DeltaReg) -> bool {
        !self.regs[reg.at()].feeds_round
    }

    /// Enter a read-only replay at `reg`'s first reader. Refused where the seed
    /// would reach a stateful operator.
    pub(in crate::query) fn replay_entry(&self, reg: DeltaReg) -> Result<ReplayEntry, String> {
        let pc = self
            .instructions
            .iter()
            .position(|i| i.reads().into_iter().flatten().any(|r| r == reg))
            .unwrap_or(self.instructions.len());
        let mut seeded = vec![false; self.regs.len()];
        seeded[reg.at()] = true;
        for instr in &self.instructions[pc..] {
            let reads_seed = instr.reads().into_iter().flatten().any(|r| seeded[r.at()]);
            if facts(&instr.op).stateful && reads_seed {
                return Err("replay: the seed reaches a stateful operator".into());
            }
            seeded[instr.out_reg.at()] |= reads_seed;
        }
        Ok(ReplayEntry { reg, pc })
    }
}
