//! The evaluator ([`Evaluator`]) wrapped for a filter ([`RowFilter`]) and for a
//! scalar ([`ScalarEval`]), each exposing only its own read-back. A map's
//! wrapper, [`crate::MapEval`], is in `map.rs`.
//!
//! Row ranges are half-open everywhere in this file: `end` is EXCLUSIVE.

use gnitz_wire::KeyRange;

use crate::batch::{Evaluator, MorselOut};
use crate::program::ReadAs;
use crate::range::RangeMembership;
#[cfg(test)]
use crate::simd::Level;
#[cfg(test)]
use crate::ResolvedProgram;
use crate::{BatchView, ExprValidateErr, LogicalProgram, SchemaDescriptor, Sink};

/// One row's result of a [`ScalarEval`], in the shape its result register's
/// class fixes; `None` for SQL NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarCell<'a> {
    /// An integer; a float result is its `f64` bit pattern.
    Int(Option<i128>),
    Str(Option<&'a [u8]>),
}

/// The only way to obtain an evaluator: each constructor resolves the program
/// against the schemas it will run on, which is what checks it against them.
impl LogicalProgram {
    /// A filter predicate; its result must not be a string.
    pub fn resolve_filter(&self, schema: &SchemaDescriptor) -> Result<RowFilter, ExprValidateErr> {
        let [Sink::Reg(r)] = self.sinks()[..] else {
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
            runs: Vec::new(),
        })
    }

    /// A scalar expression, one value per row.
    pub fn resolve_scalar(&self, schema: &SchemaDescriptor) -> Result<ScalarEval, ExprValidateErr> {
        let [Sink::Reg(r)] = self.sinks()[..] else {
            return Err(ExprValidateErr::OutputRoleMismatch);
        };
        let prog = self.resolve_program(schema, ReadAs::Value)?;
        let class = match (self.is_str(r), prog.is_u64(r.0)) {
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
}

// ---------------------------------------------------------------------------
// RowFilter
// ---------------------------------------------------------------------------

/// A predicate's evaluator and the register its verdict lands in.
struct FilterEval {
    ev: Evaluator,
    result_reg: usize,
}

/// Which rows of a batch survive a predicate and a range walk, either optional.
pub struct RowFilter {
    pred: Option<FilterEval>,
    walk: Option<RangeMembership>,
    /// One bit per row of the batch being filtered.
    words: Vec<u64>,
    /// The runs [`Self::ranges`] returns.
    runs: Vec<(usize, usize)>,
}

/// The run capacity a filter keeps between calls: a buffer grown past it is
/// dropped at the next call rather than held for the filter's lifetime.
const RETAIN_RUNS: usize = 4096;

impl RowFilter {
    /// A read's filter: its wire predicate (empty for none), and the walk its
    /// source did not apply.
    pub fn for_read(
        predicate: &[u8],
        walk: Option<&KeyRange>,
        schema: &SchemaDescriptor,
    ) -> Result<Self, ExprValidateErr> {
        let mut f = match predicate.is_empty() {
            true => RowFilter {
                pred: None,
                walk: None,
                words: Vec::new(),
                runs: Vec::new(),
            },
            false => LogicalProgram::from_blob(predicate)?.resolve_filter(schema)?,
        };
        f.walk = walk.map(|r| RangeMembership::new(r, schema)).transpose()?;
        Ok(f)
    }

    /// The surviving rows of `mb` as maximal runs, held by the filter until its
    /// next call; a caller may rewrite them in place.
    pub fn ranges(&mut self, mb: &dyn BatchView) -> &mut [(usize, usize)] {
        let n = mb.row_count();
        let RowFilter { pred, walk, words, runs } = self;
        if runs.capacity() > RETAIN_RUNS {
            *runs = Vec::new();
        }
        runs.clear();
        if pred.is_none() && walk.is_none() {
            if n > 0 {
                runs.push((0, n));
            }
            return runs;
        }
        words.resize(n.div_ceil(64), 0);
        match pred {
            Some(FilterEval { ev, result_reg }) => ev.eval_morsels(mb, 0, n, |rel_start, mo| {
                let first = rel_start / 64;
                mo.filter_words(*result_reg, &mut words[first..first + mo.rows().div_ceil(64)]);
            }),
            None => words.fill(u64::MAX),
        }
        if let Some(w) = walk {
            w.and_into(mb, words);
        }
        gnitz_wire::set_bit_runs(words, n, runs);
        runs
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
    /// `f(row, value)` for every row of `mb` in order. Returns the first `Err`
    /// of `f`, after which `f` is not called again.
    pub fn for_each<E>(
        &mut self,
        mb: &dyn BatchView,
        mut f: impl FnMut(usize, ScalarCell<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let r = self.result_reg;
        let unsigned = match self.class {
            ResultClass::Str => {
                return self.try_rows(mb, |out, i, row| {
                    f(row, ScalarCell::Str((!out.is_null(r, i)).then(|| out.str_bytes(r, i))))
                })
            }
            ResultClass::Signed => false,
            ResultClass::Unsigned => true,
        };
        self.try_rows(mb, |out, i, row| {
            let v = (!out.is_null(r, i)).then(|| {
                let x = out.reg_values(r)[i];
                match unsigned {
                    true => i128::from(x as u64),
                    false => i128::from(x),
                }
            });
            f(row, ScalarCell::Int(v))
        })
    }

    /// `f(morsel, i, row)` for row `i` of each morsel, `row` its index in `mb`, up
    /// to the first `Err`.
    fn try_rows<E>(
        &mut self,
        mb: &dyn BatchView,
        mut f: impl FnMut(&MorselOut<'_>, usize, usize) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut res = Ok(());
        self.ev.eval_morsels(mb, 0, mb.row_count(), |rel_start, out| {
            if res.is_ok() {
                res = (0..out.rows()).try_for_each(|i| f(out, i, rel_start + i));
            }
        });
        res
    }
}

/// The evaluator behind each public type, for tests that read its resolved
/// program or switch its arm and level. A filter without a predicate has no
/// evaluator.
#[cfg(test)]
pub(crate) trait Resolved {
    fn ev(&self) -> Option<&Evaluator>;
    fn ev_mut(&mut self) -> Option<&mut Evaluator>;

    fn prog(&self) -> &ResolvedProgram {
        self.ev().expect("an evaluated program").prog()
    }

    fn null_regs(&self) -> u64 {
        self.ev().map_or(0, |ev| ev.prog().null_regs)
    }

    fn set_arm(&mut self, null_regs: u64, level: Level) {
        if let Some(ev) = self.ev_mut() {
            ev.set_arm(null_regs, level)
        }
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
    fn set_arm(&mut self, null_regs: u64, level: Level) {
        if let Some(p) = &mut self.pred {
            p.ev.set_arm(null_regs, level);
        }
        if let Some(w) = &mut self.walk {
            w.level = level;
        }
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
#[path = "tests/eval.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/eval.rs"]
mod bench;
