//! The evaluator ([`Evaluator`]) wrapped for a filter ([`RowFilter`]) and for a
//! scalar ([`ScalarEval`]), each exposing only its own read-back. A map's
//! wrapper, [`crate::MapEval`], is in `map.rs`.
//!
//! Row ranges are half-open everywhere in this file: `end` is EXCLUSIVE.

use std::ops::Range;

use gnitz_wire::{FixedInt, KeyRange, TypeCode};

use crate::map::{Emit, EmitStore};
use crate::morsel::Evaluator;
use crate::program::ReadAs;
use crate::range::RangeMembership;
#[cfg(test)]
use crate::simd::Level;
use crate::{BatchView, ExprValidateErr, LogicalProgram, MapTarget, SchemaDescriptor, Sink};

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
        Ok(RowFilter { pred: Some(pred), ..Default::default() })
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
#[derive(Clone)]
struct FilterEval {
    ev: Evaluator,
    result_reg: usize,
}

/// Which rows of a batch survive a predicate and a range walk, either optional.
#[derive(Clone, Default)]
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
            true => RowFilter::default(),
            false => LogicalProgram::from_blob(predicate)?.resolve_filter(schema)?,
        };
        f.walk = walk.map(|r| RangeMembership::new(r, schema)).transpose()?;
        Ok(f)
    }

    /// Whether every row survives: no predicate and no walk.
    pub fn keeps_all(&self) -> bool {
        self.pred.is_none() && self.walk.is_none()
    }

    /// The surviving rows of `mb` as maximal runs, held by the filter until its
    /// next call; a caller may rewrite them in place.
    pub fn ranges(&mut self, mb: &dyn BatchView) -> &mut [(usize, usize)] {
        self.ranges_in(mb, 0..mb.row_count())
    }

    /// [`Self::ranges`] over the rows `rows` of `mb` alone, each run in `mb`'s
    /// row numbers.
    pub fn ranges_in(&mut self, mb: &dyn BatchView, rows: Range<usize>) -> &mut [(usize, usize)] {
        if self.runs.capacity() > RETAIN_RUNS {
            self.runs = Vec::new();
        }
        self.runs.clear();
        let (start, n) = (rows.start, rows.len());
        if self.keeps_all() {
            if n > 0 {
                self.runs.push((start, rows.end));
            }
            return &mut self.runs;
        }
        self.mark(mb, rows);
        gnitz_wire::set_bit_runs(self.words.iter().copied(), n, &mut self.runs);
        for run in &mut self.runs {
            *run = (run.0 + start, run.1 + start);
        }
        &mut self.runs
    }

    /// One bit per row of `rows` of `mb`, bit `i` set when row `rows.start + i`
    /// survives; the bits of the last word at or past `rows.len()` are unspecified.
    pub fn mark(&mut self, mb: &dyn BatchView, rows: Range<usize>) -> &[u64] {
        let RowFilter { pred, walk, words, .. } = self;
        let n = rows.len();
        words.resize(n.div_ceil(64), 0);
        match pred {
            Some(FilterEval { ev, result_reg }) => ev.eval_morsels(mb, rows.start, n, |rel_start, mo| {
                let first = rel_start / 64;
                mo.filter_words(*result_reg, &mut words[first..first + mo.rows().div_ceil(64)]);
            }),
            None => words.fill(u64::MAX),
        }
        if let Some(w) = walk {
            w.and_into(mb, rows, words);
        }
        words
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
    /// `f(value)` for every row of `mb`, in row order.
    pub fn for_each(&mut self, mb: &dyn BatchView, mut f: impl FnMut(ScalarCell<'_>)) {
        let (r, class) = (self.result_reg, self.class);
        self.ev.eval_morsels(mb, 0, mb.row_count(), |_, out| {
            let rows = 0..out.rows();
            let some = |i| !out.is_null(r, i);
            match class {
                ResultClass::Str => rows.for_each(|i| f(ScalarCell::Str(some(i).then(|| out.str_bytes(r, i))))),
                class => {
                    let vals = out.reg_values(r);
                    let unsigned = matches!(class, ResultClass::Unsigned);
                    rows.for_each(|i| {
                        let x = vals[i];
                        f(ScalarCell::Int(some(i).then(|| match unsigned {
                            true => i128::from(x as u64),
                            false => i128::from(x),
                        })))
                    })
                }
            }
        });
    }

    /// Write each row of `mb`'s value into payload slot `slot` of `dst`, whose
    /// region already holds that many cells of `tc`: a string as a German-string
    /// cell, an integer as the low bytes of the integer type `tc` is stored as. A
    /// NULL row's cell is zeroed and its null bit set; no bit is cleared. Refused
    /// with the first non-NULL integer outside that type's range, the slot then
    /// partly written.
    pub fn write_slot(
        &mut self,
        mb: &dyn BatchView,
        dst: &mut dyn MapTarget,
        slot: usize,
        tc: TypeCode,
    ) -> Result<(), i128> {
        let reg = self.result_reg;
        let (store, range) = match self.class {
            ResultClass::Str => (EmitStore::Str, None),
            class => {
                let fi = FixedInt::from_type_code(tc).expect("an integer is stored only into an integer-stored slot");
                (EmitStore::int(fi), Some((fi, matches!(class, ResultClass::Unsigned))))
            }
        };
        let e = Emit { reg, slot, store };
        let mut res = Ok(());
        self.ev.eval_morsels(mb, 0, mb.row_count(), |row0, out| {
            if res.is_err() {
                return;
            }
            if let Some((fi, unsigned)) = range
                && let Some(v) = out.first_outside(reg, fi, unsigned)
            {
                res = Err(v);
                return;
            }
            out.emit(&e, dst.slot_mut(slot), row0);
        });
        res
    }
}

/// The evaluator behind each public type, for tests that read its nullable
/// registers or switch its arm and level. A filter without a predicate has no
/// evaluator.
#[cfg(test)]
pub(crate) trait Resolved {
    fn ev(&self) -> Option<&Evaluator>;
    fn ev_mut(&mut self) -> Option<&mut Evaluator>;

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
