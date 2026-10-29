//! The evaluator ([`Evaluator`]) wrapped for a filter ([`RowFilter`]) and for a
//! scalar ([`ScalarEval`]), each exposing only its own read-back. A map's
//! wrapper, [`crate::MapEval`], is in `map.rs`.
//!
//! Row ranges are half-open everywhere in this file: `end` is EXCLUSIVE.

use gnitz_wire::KeyRange;

use crate::batch::Evaluator;
#[cfg(test)]
use crate::batch::MorselOut;
use crate::program::ReadAs;
use crate::range::RangeMembership;
#[cfg(test)]
use crate::simd::Level;
#[cfg(test)]
use crate::ResolvedProgram;
use crate::{BatchView, ExprValidateErr, LogicalProgram, SchemaDescriptor, Sink};

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
}

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
            },
            false => LogicalProgram::from_blob(predicate)?.resolve_filter(schema)?,
        };
        f.walk = walk.map(|r| RangeMembership::new(r, schema)).transpose()?;
        Ok(f)
    }

    /// The surviving rows of `mb` as maximal runs into `out` (cleared first).
    pub fn ranges(&mut self, mb: &dyn BatchView, out: &mut Vec<(usize, usize)>) {
        out.clear();
        let n = mb.row_count();
        if self.pred.is_none() && self.walk.is_none() {
            if n > 0 {
                out.push((0, n));
            }
        } else {
            self.filter_bits(mb, n, out);
        }
    }

    /// [`Self::ranges`] of the predicate and the walk, at least one of them present.
    fn filter_bits(&mut self, mb: &dyn BatchView, n: usize, out: &mut Vec<(usize, usize)>) {
        self.words.resize(n.div_ceil(64), 0);
        let words = &mut self.words[..];
        match &mut self.pred {
            Some(FilterEval { ev, result_reg }) => ev.eval_morsels(mb, 0, n, |rel_start, mo| {
                let first = rel_start / 64;
                mo.filter_words(*result_reg, &mut words[first..first + mo.rows().div_ceil(64)]);
            }),
            None => words.fill(u64::MAX),
        }
        if let Some(w) = &self.walk {
            w.and_into(mb, words);
        }
        gnitz_wire::set_bit_runs(words, n, out);
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
        self.ev().map_or(0, |ev| ev.prog().null_regs())
    }

    fn set_arm(&mut self, null_regs: u64, level: Level) {
        if let Some(ev) = self.ev_mut() {
            ev.set_arm(null_regs, level)
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
