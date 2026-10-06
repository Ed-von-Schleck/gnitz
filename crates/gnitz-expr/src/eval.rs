//! The evaluator: a resolved program and its register file ([`Evaluator`]),
//! wrapped in one public type per consumer — [`RowFilter`], [`MapEval`],
//! [`ScalarEval`] — each exposing only its own read-back.
//!
//! Row ranges are half-open everywhere in this file: `end` is EXCLUSIVE.

use gnitz_wire::ReadBound;

use crate::batch::{eval_batch, scan_filter_bits, with_str_bufs, EvalScratch, MorselOut, MORSEL};
use crate::program::{ColCopy, MapSinks, ReadAs};
use crate::range::RangeMembership;
use crate::{BatchView, ExprValidateErr, LogicalProgram, MapTarget, ResolvedProgram, SchemaFacts, Sink};

/// One program's result for every row of a batch, in the shape its result
/// register's class fixes.
pub enum ExprResults {
    /// Every row's integer value, `None` for SQL NULL. A float result is its
    /// `f64` bit pattern.
    Int(Vec<Option<i128>>),
    /// Every row's bytes concatenated, addressed by `(offset, length)`; `None`
    /// for SQL NULL, which contributes no bytes.
    Str {
        bytes: Vec<u8>,
        spans: Vec<Option<(usize, usize)>>,
    },
}

/// A resolved program and the register file it evaluates into.
pub(crate) struct Evaluator {
    pub(crate) prog: ResolvedProgram,
    scratch: EvalScratch,
}

/// The only way to obtain an evaluator: each constructor resolves the program
/// against the schemas it will run on, which is what checks it against them.
impl LogicalProgram {
    /// A filter predicate; its result must not be a string.
    pub fn resolve_filter(self, schema: &dyn SchemaFacts) -> Result<RowFilter, ExprValidateErr> {
        let [Sink::Reg(r)] = self.sinks[..] else {
            return Err(ExprValidateErr::OutputRoleMismatch);
        };
        if self.is_str(r) {
            return Err(ExprValidateErr::RegClassMismatch { reg: r.0 });
        }
        let prog = self.resolve_program(schema, ReadAs::Bool)?;
        let pred = FilterEval {
            ev: Evaluator::new(prog),
            result_reg: r.0 as usize,
        };
        Ok(RowFilter {
            pred: Some(pred),
            walk: None,
            words: Vec::new(),
        })
    }

    /// A scalar expression, one value per row.
    pub fn resolve_scalar(self, schema: &dyn SchemaFacts) -> Result<ScalarEval, ExprValidateErr> {
        let [Sink::Reg(r)] = self.sinks[..] else {
            return Err(ExprValidateErr::OutputRoleMismatch);
        };
        let prog = self.resolve_program(schema, ReadAs::Value)?;
        let class = match (self.is_str(r), (prog.reg_u64 >> r.0) & 1 != 0) {
            (true, _) => ResultClass::Str,
            (false, true) => ResultClass::Unsigned,
            (false, false) => ResultClass::Signed,
        };
        Ok(ScalarEval {
            ev: Evaluator::new(prog),
            result_reg: r.0 as usize,
            class,
        })
    }

    /// A map from `in_schema` to `out_schema`, covering every output payload slot
    /// exactly once.
    pub fn resolve_map(
        self,
        in_schema: &dyn SchemaFacts,
        out_schema: &dyn SchemaFacts,
    ) -> Result<MapEval, ExprValidateErr> {
        let prog = self.resolve_program(in_schema, ReadAs::Value)?;
        let sinks = self.map_sinks(in_schema, out_schema)?;
        let is_identity = self.is_identity_map(in_schema, out_schema);
        Ok(MapEval {
            ev: Evaluator::new(prog),
            sinks,
            is_identity,
        })
    }
}

impl Evaluator {
    fn new(prog: ResolvedProgram) -> Self {
        let scratch = EvalScratch::new(&prog);
        Evaluator { prog, scratch }
    }

    /// Evaluate rows `start..start + n` a morsel at a time, handing
    /// `f(rel_start, out)` each morsel's registers; `rel_start` counts from `start`.
    pub(crate) fn eval_morsels(
        &mut self,
        mb: &dyn BatchView,
        start: usize,
        n: usize,
        mut f: impl FnMut(usize, &MorselOut<'_>),
    ) {
        debug_assert!(
            start + n <= mb.row_count(),
            "eval window {start}..{} is past the batch's end",
            start + n,
        );
        let Evaluator { prog, scratch } = self;
        with_str_bufs(prog, mb, |bufs| {
            for rel_start in (0..n).step_by(MORSEL) {
                let m = MORSEL.min(n - rel_start);
                eval_batch(prog, mb, bufs, start + rel_start, m, scratch);
                f(rel_start, &scratch.morsel_out(prog, bufs, m));
            }
        });
    }

    /// Test hook: run this program on the `no_nulls` arm or the nullable one.
    /// Only a program that resolved `no_nulls` may be put back on it.
    #[cfg(test)]
    pub(crate) fn set_no_nulls(&mut self, no_nulls: bool) {
        self.prog.no_nulls = no_nulls;
        self.scratch = EvalScratch::new(&self.prog);
    }
}

// ---------------------------------------------------------------------------
// RowFilter
// ---------------------------------------------------------------------------

/// A predicate's evaluator and the register its verdict lands in.
pub(crate) struct FilterEval {
    ev: Evaluator,
    result_reg: usize,
}

/// Which rows of a batch survive a predicate and a range walk, either optional.
pub struct RowFilter {
    pred: Option<FilterEval>,
    walk: Option<RangeMembership>,
    /// One bit per row of the batch being filtered.
    words: Vec<u64>,
}

impl RowFilter {
    /// A read's filter: its wire predicate (empty for none), and the part of its
    /// bound the source did not apply.
    pub fn for_read(
        predicate: &[u8],
        unapplied: &ReadBound,
        schema: &dyn SchemaFacts,
    ) -> Result<Self, ExprValidateErr> {
        let mut f = match predicate.is_empty() {
            true => RowFilter {
                pred: None,
                walk: None,
                words: Vec::new(),
            },
            false => LogicalProgram::from_blob(predicate)?.resolve_filter(schema)?,
        };
        f.walk = match unapplied {
            ReadBound::Range(r) => Some(RangeMembership::new(r, schema)?),
            ReadBound::None | ReadBound::PkSet(_) => None,
        };
        Ok(f)
    }

    pub(crate) fn keeps_every_row(&self) -> bool {
        self.pred.is_none() && self.walk.is_none()
    }

    /// The surviving rows of `mb` as maximal runs into `out` (cleared first).
    pub fn ranges(&mut self, mb: &dyn BatchView, out: &mut Vec<(usize, usize)>) {
        out.clear();
        let n = mb.row_count();
        if self.keeps_every_row() {
            if n > 0 {
                out.push((0, n));
            }
            return;
        }
        self.words.resize(n.div_ceil(64), 0);
        let words = &mut self.words[..];
        match &mut self.pred {
            Some(FilterEval { ev, result_reg }) => ev.eval_morsels(mb, 0, n, |rel_start, out| {
                let first = rel_start / 64;
                out.filter_words(*result_reg, &mut words[first..first + out.rows().div_ceil(64)]);
            }),
            None => {
                words.fill(u64::MAX);
                if !n.is_multiple_of(64) {
                    words[n / 64] = gnitz_wire::low_bits_mask(n % 64);
                }
            }
        }
        if let Some(w) = &self.walk {
            w.and_into(mb, words);
        }
        scan_filter_bits(words, n, out);
    }
}

// ---------------------------------------------------------------------------
// ScalarEval
// ---------------------------------------------------------------------------

/// How a scalar's result register reads back.
#[derive(Clone, Copy)]
enum ResultClass {
    Signed,
    Unsigned,
    Str,
}

/// A scalar expression: one value per row.
pub struct ScalarEval {
    ev: Evaluator,
    result_reg: usize,
    class: ResultClass,
}

impl ScalarEval {
    pub fn eval_all(&mut self, mb: &dyn BatchView) -> ExprResults {
        let unsigned = match self.class {
            ResultClass::Str => return self.eval_all_str(mb),
            ResultClass::Signed => false,
            ResultClass::Unsigned => true,
        };
        let r = self.result_reg;
        let mut vals = Vec::with_capacity(mb.row_count());
        self.ev.eval_morsels(mb, 0, mb.row_count(), |_, out| {
            let first = vals.len();
            vals.extend(out.reg_values(r).iter().map(|&x| match unsigned {
                true => Some(i128::from(x as u64)),
                false => Some(i128::from(x)),
            }));
            out.for_each_null_row(r, |i| vals[first + i] = None);
        });
        ExprResults::Int(vals)
    }

    fn eval_all_str(&mut self, mb: &dyn BatchView) -> ExprResults {
        let r = self.result_reg;
        let (mut bytes, mut spans) = (Vec::new(), Vec::with_capacity(mb.row_count()));
        self.ev.eval_morsels(mb, 0, mb.row_count(), |_, out| {
            let first = spans.len();
            for i in 0..out.rows() {
                let b = out.str_bytes(r, i);
                spans.push(Some((bytes.len(), b.len())));
                bytes.extend_from_slice(b);
            }
            out.for_each_null_row(r, |i| spans[first + i] = None);
        });
        ExprResults::Str { bytes, spans }
    }

    /// Test hook: the register [`Self::eval_all`] reads back.
    #[cfg(test)]
    pub(crate) fn result_reg(&self) -> usize {
        self.result_reg
    }
}

// ---------------------------------------------------------------------------
// MapEval
// ---------------------------------------------------------------------------

/// A map: column moves the caller runs, and the computed columns
/// [`Self::write_computed`] writes.
pub struct MapEval {
    ev: Evaluator,
    sinks: MapSinks,
    is_identity: bool,
}

impl MapEval {
    pub fn copies(&self) -> &[ColCopy] {
        &self.sinks.copies
    }

    /// True iff some output slot is computed rather than copied.
    pub fn emits_anything(&self) -> bool {
        !self.sinks.scalar_emits.is_empty() || !self.sinks.str_emits.is_empty()
    }

    /// True iff the map reproduces its input, given the input's PK carried through.
    pub fn is_identity(&self) -> bool {
        self.is_identity
    }

    /// Write the null words and computed columns of source rows
    /// `src_start..src_start + n` into `dst` rows from `dst_start`.
    pub fn write_computed(
        &mut self,
        src: &dyn BatchView,
        src_start: usize,
        n: usize,
        dst: &mut dyn MapTarget,
        dst_start: usize,
    ) {
        let emits = self.emits_anything();
        let MapEval { ev, sinks, .. } = self;
        sinks
            .null_perm
            .write_rows(src.null_bmp(), src_start, dst.null_bmp_mut(), dst_start, n);
        if !emits {
            return;
        }
        ev.eval_morsels(src, src_start, n, |rel_start, out| {
            let row0 = dst_start + rel_start;
            for e in &sinks.scalar_emits {
                out.emit_scalar(e, dst.slot_mut(e.slot), row0);
            }
            for e in &sinks.str_emits {
                out.emit_str(e, dst.slot_mut(e.slot), row0);
            }
        });
    }
}

/// The evaluator behind each public type, for tests that read its resolved
/// program or switch its arm. A filter without a predicate has neither.
#[cfg(test)]
pub(crate) trait Resolved {
    fn ev(&self) -> Option<&Evaluator>;
    fn ev_mut(&mut self) -> Option<&mut Evaluator>;

    fn prog(&self) -> &ResolvedProgram {
        &self.ev().expect("an evaluated program").prog
    }

    fn no_nulls(&self) -> bool {
        self.ev().is_some_and(|ev| ev.prog.no_nulls)
    }

    fn set_no_nulls(&mut self, no_nulls: bool) {
        if let Some(ev) = self.ev_mut() {
            ev.set_no_nulls(no_nulls)
        }
    }

    fn eval_morsels(&mut self, mb: &dyn BatchView, start: usize, n: usize, f: impl FnMut(usize, &MorselOut<'_>)) {
        self.ev_mut()
            .expect("an evaluated program")
            .eval_morsels(mb, start, n, f)
    }
}

#[cfg(test)]
impl Resolved for RowFilter {
    fn ev(&self) -> Option<&Evaluator> {
        self.pred.as_ref().map(|p| &p.ev)
    }
    fn ev_mut(&mut self) -> Option<&mut Evaluator> {
        self.pred.as_mut().map(|p| &mut p.ev)
    }
}

#[cfg(test)]
impl Resolved for ScalarEval {
    fn ev(&self) -> Option<&Evaluator> {
        Some(&self.ev)
    }
    fn ev_mut(&mut self) -> Option<&mut Evaluator> {
        Some(&mut self.ev)
    }
}

#[cfg(test)]
impl Resolved for MapEval {
    fn ev(&self) -> Option<&Evaluator> {
        Some(&self.ev)
    }
    fn ev_mut(&mut self) -> Option<&mut Evaluator> {
        Some(&mut self.ev)
    }
}

#[cfg(test)]
#[path = "tests/eval.rs"]
mod tests;
