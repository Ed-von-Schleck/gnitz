//! DBSP VM: executes compiled circuit programs.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::relation::{CircuitState, StateIdx};
use gnitz_wire::ClampKind;
use gnitz_zset::algebra::MapPlan;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use gnitz_zset::stream;

mod builder;
mod exec;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

pub(in crate::query) use builder::ProgramBuilder;
pub(in crate::query) use exec::{execute_epoch, replay_chunk, Stores};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A register whose batch an instruction reads or writes — the one index space
/// a [`Vm`] has, a trace being named by its [`Integral`]. Minted only by
/// [`ProgramBuilder`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) struct DeltaReg(u16);

impl DeltaReg {
    #[inline]
    fn at(self) -> usize {
        self.0 as usize
    }
}

/// One VM instruction: the delta it reads, the delta it writes, and the
/// operator between them with all operator-specific data pre-resolved.
struct Instr {
    in_reg: DeltaReg,
    out_reg: DeltaReg,
    op: Op,
    /// `op`'s, classified once when the instruction is pushed.
    facts: OpFacts,
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
        /// The history trace `I(in)`, which `finish` integrates.
        hist: StateIdx,
        kind: ClampKind,
    },
    /// The delta-trace inner join, equi, range and cross alike: the probe is
    /// baked by the compiler from the wire's `JoinKind` and side flag, so neither
    /// spelling reaches the instruction set.
    JoinDT {
        trace: Integral,
        probe: stream::JoinProbe,
    },
    WorkerFilter,
    /// Widen every row with NULL-filled payload columns — the LEFT JOIN
    /// null-fill's unmatched preserved rows — on the side `nulls_first` names.
    /// The column count is the difference between the two registers' schemas.
    NullExtend {
        nulls_first: bool,
    },
    /// `out_trace`: the integral of the output register, or `None` where that
    /// register is the view's output and so the view's own store is its integral.
    /// `index`: the table the value index of its MIN/MAX aggregates lives in.
    Reduce {
        out_trace: Option<StateIdx>,
        index: Option<StateIdx>,
        plan: Box<stream::ReducePlan>,
    },
    /// Per-group top-N: `index`, the ordered index of every input row, is
    /// populated with the delta before the walk. `out_trace` as [`Op::Reduce`]'s.
    TopN {
        out_trace: Option<StateIdx>,
        index: StateIdx,
        plan: Box<stream::TopNPlan>,
    },
}

/// The integral of a delta, as its readers find it: a join's probe, and a
/// bounded view's replay, which seeds from it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) enum Integral {
    /// A child store the circuit integrates a register into.
    Own(StateIdx),
    /// The store of the relation the delta is scanned from, read as it stood
    /// when the view last absorbed that relation: nothing is integrated, because
    /// the relation's own ingest already holds every row a trace would.
    Source(u64),
}

/// What the VM's passes ask of one operator.
#[derive(Clone, Copy)]
struct OpFacts {
    /// It reads its input at net weights, so the VM folds that register when it
    /// is written.
    consolidates_in: bool,
    /// Its output depends on state it owns — its input's history, a value index,
    /// an ordered index — so a replay past it would read that state against a
    /// seed it never saw.
    stateful: bool,
    /// With every delta operand empty its kernel returns
    /// `empty_with_schema(out_reg)` and touches no trace.
    inert_on_empty: bool,
}

fn facts(op: &Op) -> OpFacts {
    let linear = OpFacts {
        consolidates_in: false,
        stateful: false,
        inert_on_empty: true,
    };
    match op {
        Op::Filter(_) | Op::Map(_) | Op::Negate | Op::Union { .. } | Op::WorkerFilter | Op::NullExtend { .. } => linear,
        Op::WeightClamp { .. } => OpFacts {
            consolidates_in: true,
            stateful: true,
            ..linear
        },
        Op::JoinDT { .. } => OpFacts { consolidates_in: true, ..linear },
        Op::Reduce { plan, .. } => OpFacts {
            consolidates_in: !plan.is_exact_linear(),
            stateful: true,
            // A global-ground reduce mints V₀ from an empty delta.
            inert_on_empty: !plan.seeds_ground,
        },
        Op::TopN { .. } => OpFacts { stateful: true, ..linear },
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
            | Op::JoinDT { trace: _, probe: _ }
            | Op::WorkerFilter
            | Op::NullExtend { nulls_first: _ }
            | Op::Reduce { out_trace: _, index: _, plan: _ }
            | Op::TopN { out_trace: _, index: _, plan: _ } => None,
        };
        [Some(self.in_reg), second]
    }

    /// The trace this instruction's op owns as the integral of one of its own
    /// registers — the clamp's input history, a reduce's or top-N's output — which
    /// `finish` schedules as an integrate. No `_` arm, as in [`Self::reads`]: a new
    /// opcode must say whether it owns one.
    fn own_integral(&self) -> Option<(DeltaReg, StateIdx)> {
        match &self.op {
            Op::WeightClamp { hist, .. } => Some((self.in_reg, *hist)),
            Op::Reduce { out_trace, .. } | Op::TopN { out_trace, .. } => out_trace.map(|trace| (self.out_reg, trace)),
            Op::Filter(_)
            | Op::Map(_)
            | Op::Negate
            | Op::Union { .. }
            | Op::JoinDT { .. }
            | Op::WorkerFilter
            | Op::NullExtend { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Vm
// ---------------------------------------------------------------------------

/// A compiled DBSP program and the registers it runs over. It owns every
/// resource its instructions name except the stores: the view's own children
/// ([`CircuitState`]) and the relations an [`Integral::Source`] reads.
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
    /// No epoch has been dispatched yet and some instruction is not inert on an
    /// empty delta (a global-ground `Reduce` this worker owns) — the one reason
    /// an all-empty epoch is worth dispatching.
    pub(in crate::query) pending_ground_row: bool,
}

/// Who last reads a register: an instruction, an integrate — which run after
/// every instruction — or nobody: unread, or the sink, which the epoch extracts
/// after both.
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

    /// A batch written to `reg` is folded.
    pub(in crate::query) fn folds(&self, reg: DeltaReg) -> bool {
        self.regs[reg.at()].fold
    }

    /// How many instructions run an op `is` holds of.
    #[cfg(test)]
    pub(in crate::query) fn count_ops(&self, is: impl Fn(&Op) -> bool) -> usize {
        self.instructions.iter().filter(|i| is(&i.op)).count()
    }

    /// True iff a reduce or top-N reads the view's own store as its output trace.
    pub(in crate::query) fn reads_view_store(&self) -> bool {
        self.instructions.iter().any(|i| {
            matches!(
                i.op,
                Op::Reduce { out_trace: None, .. } | Op::TopN { out_trace: None, .. }
            )
        })
    }

    /// True iff some instruction trims the delta to this worker's own rows, so
    /// the result is a slice rather than a copy of what every worker computes.
    pub(in crate::query) fn trims_per_worker(&self) -> bool {
        self.instructions.iter().any(|i| matches!(i.op, Op::WorkerFilter))
    }

    /// Enter a read-only replay at `reg`'s first reader. Refused where the seed
    /// would reach a stateful operator, or where one runs without it: every
    /// other register is empty in a replay, so an operator the seed does not
    /// reach is skipped exactly when it is inert on an empty delta.
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
            if instr.facts.stateful && (reads_seed || !instr.facts.inert_on_empty) {
                return Err("replay: the seed reaches a stateful operator".into());
            }
            seeded[instr.out_reg.at()] |= reads_seed;
        }
        Ok(ReplayEntry { reg, pc })
    }
}
