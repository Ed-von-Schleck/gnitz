//! DBSP VM: executes compiled circuit programs.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::rc::Rc;

use gnitz_store::relation::{CircuitState, StateIdx, StateLayout};
use gnitz_trace::stream;
use gnitz_wire::ClampKind;
use gnitz_zset::algebra::{MapPlan, ScatterPlan};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

mod builder;
mod exec;

pub(in crate::query) use builder::ProgramBuilder;
pub(in crate::query) use exec::{Epoch, Ran, Stores, mint_ground_rows, replay_keys, resume, start};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A register whose batch an instruction reads or writes — the one index space
/// a [`Vm`] has, a trace being named by its [`Integral`]. Minted only by
/// [`ProgramBuilder`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) struct DeltaReg(usize);

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
    /// share of them all out. An epoch runs the round iff the round is on its
    /// seed's run ([`Reg::run`]), whatever the delta holds, so every worker runs
    /// the same rounds.
    Round(Rc<ScatterPlan>),
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
    /// The filters and maps, in order, that turn a row of the relation into one
    /// of the clamp's input.
    pub(in crate::query) steps: Box<[HistoryStep]>,
    pub(in crate::query) negated: bool,
    /// The relation's payload columns, as payload bits, its scan's readers
    /// read: every one the steps do.
    pub(in crate::query) read: u64,
}

/// A step of a [`HistoryTerm`]: a copy of the operator of the instruction it
/// stands for.
pub(in crate::query) enum HistoryStep {
    Filter(Box<gnitz_expr::RowFilter>),
    Map(Box<MapPlan>),
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
            | Op::Round(_)
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
/// resource its instructions name; the stores they address are the caller's:
/// the view's own children ([`CircuitState`]), whose layout its view is
/// registered with, and the relations an [`Integral::Relation`] reads.
pub(in crate::query) struct Vm {
    instructions: Box<[Instr]>,
    /// Each tick's accumulation of a register into its trace. Run after the
    /// whole instruction range — and not at all by a replay, which is what makes
    /// a replay read-only.
    integrates: Box<[(DeltaReg, StateIdx)]>,
    regs: Box<[Reg]>,
    /// The registers of the last run to end, each empty, for the next to start
    /// on — so a tick's epoch does not label one batch per register afresh.
    /// None is here before the first run, nor after one stood at a round or
    /// failed.
    spare: Box<[Batch]>,
    /// The register the epoch's output is extracted from.
    out_reg: DeltaReg,
}

/// What [`ProgramBuilder::finish`] derived about one register.
struct Reg {
    schema: SchemaDescriptor,
    /// Some instruction reads it at net weights, so it is folded when written.
    fold: bool,
    /// Where its last reader stands on the timeline — an instruction at its
    /// index, then the integrates in order, then the output's extraction at
    /// `usize::MAX` — so that reader may take its batch. 0 for a register nothing
    /// reads, about which no reader asks.
    last_read: usize,
    /// The positions on the timeline a run seeding it steps through, ascending:
    /// each instruction that reads a register its delta reaches, then each
    /// integrate of one. Every other register stays empty through the run.
    run: Box<[usize]>,
}

/// A bounded view's read-only replay: the register it seeds, the integral the
/// seed is gathered from, the clamp the gathered weights are read through — a
/// clamp's history holds its input, whose clamp is its output — and the payload
/// columns, as payload bits, it reads of a relation's rows. Made only by
/// [`Vm::replay`], which refuses a register a replay cannot seed.
#[derive(Clone, Copy)]
pub(in crate::query) struct Replay {
    reg: DeltaReg,
    seed: Integral,
    clamp: Option<ClampKind>,
    read: u64,
}

impl Vm {
    /// The schema register `reg` is labelled with.
    pub(in crate::query) fn schema_of(&self, reg: DeltaReg) -> &SchemaDescriptor {
        &self.regs[reg.0].schema
    }

    /// The schema of the register the epoch's output leaves in.
    pub(in crate::query) fn out_schema(&self) -> &SchemaDescriptor {
        self.schema_of(self.out_reg)
    }

    /// True iff a reduce reads the view's own store as its output trace.
    pub(in crate::query) fn reads_view_store(&self) -> bool {
        self.instructions
            .iter()
            .any(|i| matches!(i.op, Op::Reduce { out_trace: Integral::Relation(..), .. }))
    }

    /// True iff a reduce owes a row over an empty input: a global aggregate's.
    pub(in crate::query) fn mints_ground_rows(&self) -> bool {
        self.instructions
            .iter()
            .any(|i| matches!(&i.op, Op::Reduce { plan, .. } if plan.seeds_ground()))
    }

    /// Some instruction reads `reg` at net weights, so its batch is folded when
    /// it stands.
    pub(in crate::query) fn folds(&self, reg: DeltaReg) -> bool {
        self.regs[reg.0].fold
    }

    /// Label `batch` with `reg`'s schema: rows that enter the program at `reg`.
    pub(in crate::query) fn label(&self, reg: DeltaReg, batch: &mut Batch) {
        let want = &self.regs[reg.0].schema;
        // Rows under the wrong layout would scramble every downstream read
        // silently.
        assert!(
            batch.schema().same_region_types(want),
            "VM register {} schema/batch region mismatch",
            reg.0,
        );
        // A producer's label may be narrower in nullability. Every kernel labels
        // what it writes with its register's schema, so from here on every
        // register's batch carries its own.
        batch.set_schema(want);
    }

    /// An epoch seeding `reg` with an empty delta does nothing: it runs no
    /// exchange round.
    pub(in crate::query) fn idles_on_empty(&self, reg: DeltaReg) -> bool {
        let round = |&at: &usize| matches!(self.instructions.get(at), Some(Instr { op: Op::Round(_), .. }));
        !self.regs[reg.0].run.iter().any(round)
    }

    /// The replay seeding `reg` with what `seed` holds, read through `clamp`
    /// and, of a relation's rows, at the payload columns `read`; refused where
    /// the seed would reach a stateful operator.
    pub(in crate::query) fn replay(
        &self,
        reg: DeltaReg,
        seed: Integral,
        clamp: Option<ClampKind>,
        read: u64,
    ) -> Result<Replay, String> {
        // A replay runs no integrate, the positions past the instructions.
        let steps = self.regs[reg.0].run.iter().map_while(|&at| self.instructions.get(at));
        for instr in steps {
            // Its output depends on more than its operands — its input's
            // history, a value index, an ordered index, the other workers' rows
            // — so a replay past it would read that against a seed it never saw.
            let stateful = match instr.op {
                Op::Round(_) | Op::WeightClamp { .. } | Op::Reduce { .. } | Op::Ordered { .. } => true,
                Op::Filter(_)
                | Op::Map(_)
                | Op::Negate
                | Op::Union { .. }
                | Op::JoinDT { .. }
                | Op::Share(_)
                | Op::NullExtend { .. } => false,
            };
            if stateful {
                return Err("replay: the seed reaches a stateful operator".into());
            }
        }
        Ok(Replay { reg, seed, clamp, read })
    }
}
