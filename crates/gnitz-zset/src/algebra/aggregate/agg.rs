//! Aggregate accumulator state.

use std::cmp::Ordering;
use std::ops::Range;

use crate::repr::{Batch, MemBatch};
use crate::schema::{ColumnLocator, TypeCode};
use gnitz_wire::RowSource;
use gnitz_wire::{for_each_fixed_int, AggFunc, FixedInt, ScalarKind};

use crate::algebra::order_image::{
    ieee_order_bits, ieee_order_bits_f32, order_bits, scalar_image, scalar_native_of_image, wide_native,
    wide_native_of_image, ImageKind, WideKind,
};

/// The value-index parameters of one MIN/MAX aggregate: the column the index
/// reads, how to encode it, and which end of the order the index puts first.
#[derive(Clone, Copy)]
pub(crate) struct ExtremeSpec {
    pub loc: ColumnLocator,
    pub kind: ImageKind,
    pub max: bool,
}

/// A stepped accumulator's value: a register image, or a wide extreme's native
/// bytes.
pub(crate) enum AggValue<'a> {
    Bits(u64),
    Wide(WideKind, &'a [u8]),
}

/// One aggregate's running value. What a row does to it is resolved in `new`.
#[derive(Clone)]
pub(crate) struct Accumulator {
    acc: i64,
    /// A wide extreme's native bytes.
    wide: Box<[u8]>,
    has_extreme: bool,
    /// The aggregated column in an input row.
    src: ColumnLocator,
    /// This aggregate's column in an output row.
    out: ColumnLocator,
    /// What an input row's `src` does to the slot.
    kind: StepKind,
    /// What an output row's `out` does to the slot.
    merge: StepKind,
}

#[derive(Clone, Copy)]
enum StepKind {
    Count,
    CountNonNull,
    /// A float sum holds `f64` bits whatever the source width.
    Sum(ScalarKind),
    /// A scalar extreme is held as the value index holds it, complemented for
    /// MAX, so the smaller image always wins.
    Extreme {
        max: bool,
        kind: ImageKind,
    },
}

impl StepKind {
    /// `None` iff `op` sums and `tc` has no scalar register image.
    fn of(op: AggFunc, tc: TypeCode) -> Option<Self> {
        Some(match op {
            AggFunc::Count => StepKind::Count,
            AggFunc::CountNonNull => StepKind::CountNonNull,
            AggFunc::Sum => StepKind::Sum(ScalarKind::from_type_code(tc)?),
            AggFunc::Min | AggFunc::Max => StepKind::Extreme {
                max: op == AggFunc::Max,
                kind: ImageKind::of(tc),
            },
        })
    }
}

/// A reduce holds one per aggregate, and a fold of one of the kinds no column
/// kernel steps one per group.
const _: () = assert!(std::mem::size_of::<Accumulator>() <= 40);

impl Accumulator {
    /// `op` over input column `src`, emitting into output column `out`.
    pub(crate) fn new(op: AggFunc, src: ColumnLocator, out: ColumnLocator) -> Self {
        let step = |op: AggFunc, loc: ColumnLocator| {
            StepKind::of(op, loc.type_code()).expect("agg_output_type admits a sum only over a scalar register image")
        };
        Accumulator {
            acc: 0,
            wide: Box::default(),
            has_extreme: false,
            src,
            out,
            kind: step(op, src),
            merge: step(op.merge_op(), out),
        }
    }

    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        self.acc = 0;
        self.has_extreme = false;
    }

    #[inline(always)]
    pub(crate) fn is_linear(&self) -> bool {
        !matches!(self.kind, StepKind::Extreme { .. })
    }

    /// A count or an integer sum: exact whatever the row order.
    pub(crate) fn is_exact_linear(&self) -> bool {
        matches!(
            self.kind,
            StepKind::Count | StepKind::CountNonNull | StepKind::Sum(ScalarKind::Int(_))
        )
    }

    /// This aggregate's value-index parameters, or `None` for a linear one.
    #[inline(always)]
    pub(crate) fn extreme_index_spec(&self) -> Option<ExtremeSpec> {
        match self.kind {
            StepKind::Extreme { max, kind } => Some(ExtremeSpec { loc: self.src, kind, max }),
            _ => None,
        }
    }

    /// The net row count a COUNT, or a combine's sum of partial COUNTs, holds.
    #[inline(always)]
    pub(crate) fn count_value(&self) -> i64 {
        debug_assert!(matches!(
            self.kind,
            StepKind::Count | StepKind::CountNonNull | StepKind::Sum(ScalarKind::Int(FixedInt::I64))
        ));
        self.acc
    }

    /// The emitted value; `None` renders NULL.
    pub(crate) fn value(&self) -> Option<AggValue<'_>> {
        Some(match self.kind {
            StepKind::Extreme { .. } if !self.has_extreme => return None,
            StepKind::Extreme { kind: ImageKind::Wide(kind), .. } => AggValue::Wide(kind, &self.wide),
            StepKind::Extreme { kind: ImageKind::Scalar(kind), max } => {
                AggValue::Bits(scalar_native_of_image(kind, max, self.acc as u64))
            }
            _ => AggValue::Bits(self.acc as u64),
        })
    }

    /// Seed a MIN/MAX with the extreme image the value index holds for it.
    pub(crate) fn seed_from_index(&mut self, image: &[u8]) {
        let StepKind::Extreme { max, kind } = self.kind else {
            unreachable!("only an extreme is seeded from the value index")
        };
        match kind {
            ImageKind::Scalar(_) => {
                self.acc = u64::from_be_bytes(image[..8].try_into().unwrap()) as i64;
                self.has_extreme = true;
            }
            ImageKind::Wide(kind) => self.store_wide(&wide_native_of_image(kind, max, image)),
        }
    }

    #[inline(always)]
    fn store_wide(&mut self, v: &[u8]) {
        if self.wide.len() == v.len() {
            self.wide.copy_from_slice(v);
        } else {
            self.wide = v.into();
        }
        self.has_extreme = true;
    }

    #[inline(always)]
    fn apply(&mut self, kind: StepKind, loc: ColumnLocator, rows: &impl RowSource, row: usize, weight: i64) {
        if !matches!(kind, StepKind::Count) && loc.is_null(rows, row) {
            return;
        }
        match kind {
            StepKind::Count | StepKind::CountNonNull => self.acc = self.acc.wrapping_add(weight),
            StepKind::Sum(ScalarKind::Int(fi)) => {
                self.acc = self
                    .acc
                    .wrapping_add(loc.decode_i64(rows, row, fi).wrapping_mul(weight));
            }
            StepKind::Sum(ScalarKind::F32) => self.add_float(
                f32::from_le_bytes(loc.bytes(rows, row).try_into().unwrap()) as f64,
                weight,
            ),
            StepKind::Sum(ScalarKind::F64) => {
                self.add_float(f64::from_le_bytes(loc.bytes(rows, row).try_into().unwrap()), weight)
            }
            StepKind::Extreme { max, kind } => {
                debug_assert!(weight > 0, "an extreme cannot retract: step it with positive rows only");
                match kind {
                    ImageKind::Scalar(kind) => {
                        let image = scalar_image(&loc, kind, max, rows, row);
                        if !self.has_extreme || image < self.acc as u64 {
                            self.acc = image as i64;
                            self.has_extreme = true;
                        }
                    }
                    ImageKind::Wide(kind) => {
                        let mut scratch = [0u8; 16];
                        let v = wide_native(&loc, kind, rows, row, &mut scratch);
                        let wins = if max { Ordering::Greater } else { Ordering::Less };
                        if !self.has_extreme || kind.cmp_native(v, &self.wide) == wins {
                            self.store_wide(v);
                        }
                    }
                }
            }
        }
    }

    /// Step the linear accumulators of `accs` over `rows`, and under `extremes`
    /// the others too, each by its column kernel where one exists.
    pub(crate) fn fold_rows(accs: &mut [Self], mb: &MemBatch, rows: Range<usize>, extremes: bool) {
        for acc in accs.iter_mut().filter(|a| extremes || a.is_linear()) {
            let step = acc.bulk_step();
            step(acc, mb, rows.clone());
        }
    }

    /// This accumulator's [`BulkStep`]: a column kernel where one exists, else
    /// [`Self::step_each`].
    fn bulk_step(&self) -> BulkStep {
        let payload = matches!(self.src, ColumnLocator::Payload { .. });
        match self.kind {
            // Reads no column.
            StepKind::Count => Self::count_rows,
            StepKind::CountNonNull if payload => Self::count_non_null_rows,
            StepKind::Sum(ScalarKind::Int(fi)) if payload => for_each_fixed_int!(fi, |FI| {
                |acc: &mut Self, mb: &MemBatch, rows: Range<usize>| {
                    acc.acc = fold_col::<{ FI.width() }, _>(mb, acc.src, rows, acc.acc, |a, v, w, null| {
                        a.wrapping_add(if null { 0 } else { FI.decode_le_i64(&v).wrapping_mul(w) })
                    });
                }
            }),
            StepKind::Sum(ScalarKind::F32) if payload => Self::sum_float_rows::<4>,
            StepKind::Sum(ScalarKind::F64) if payload => Self::sum_float_rows::<8>,
            _ => Self::step_each,
        }
    }

    fn step_each(&mut self, mb: &MemBatch, rows: Range<usize>) {
        for row in rows {
            self.apply(self.kind, self.src, mb, row, mb.get_weight(row));
        }
    }

    fn count_rows(&mut self, mb: &MemBatch, rows: Range<usize>) {
        self.acc = self.acc.wrapping_add(mb.sum_weights(rows.start, rows.end));
    }

    fn count_non_null_rows(&mut self, mb: &MemBatch, rows: Range<usize>) {
        self.acc = fold_col::<0, _>(mb, self.src, rows, self.acc, |a, _, w, null| {
            a.wrapping_add(if null { 0 } else { w })
        });
    }

    /// SUM over an `f32` (`N = 4`) or `f64` (`N = 8`) column; the slot holds
    /// `f64` bits either way.
    fn sum_float_rows<const N: usize>(&mut self, mb: &MemBatch, rows: Range<usize>) {
        let sum = fold_col::<N, _>(mb, self.src, rows, f64::from_bits(self.acc as u64), |a, v, w, null| {
            let v = match N {
                4 => f32::from_le_bytes(v[..4].try_into().unwrap()) as f64,
                _ => f64::from_le_bytes(v[..8].try_into().unwrap()),
            };
            if null {
                a
            } else {
                a + v * w as f64
            }
        });
        self.acc = sum.to_bits() as i64;
    }

    /// This aggregate's state for a fold over many groups at once, holding none.
    pub(crate) fn grouped(&self) -> GroupedState {
        let payload = matches!(self.src, ColumnLocator::Payload { .. });
        match self.kind {
            StepKind::Count | StepKind::CountNonNull => GroupedState::Bits(Vec::new()),
            StepKind::Sum(_) if payload => GroupedState::Bits(Vec::new()),
            StepKind::Extreme { kind: ImageKind::Scalar(_), .. } => GroupedState::Extreme(Vec::new(), Vec::new()),
            _ => GroupedState::Each(Vec::new()),
        }
    }

    /// Step this aggregate over rows `ranges` of `mb`, the `i`-th of which
    /// belongs to group `ord[i]`: one pass over its column, in row order, into
    /// `state` — this aggregate's own, sized to the group count. A row at a
    /// non-positive weight steps no extreme.
    pub(crate) fn fold_grouped(&self, mb: &MemBatch, ranges: &[(usize, usize)], ord: &[u32], state: &mut GroupedState) {
        let (kind, src) = (self.kind, self.src);
        match state {
            GroupedState::Bits(out) => {
                let add_float = |slot: &mut i64, v: f64, w: i64| {
                    *slot = (f64::from_bits(*slot as u64) + v * w as f64).to_bits() as i64;
                };
                match kind {
                    // A PK column is never NULL, so its non-NULL count is the count.
                    StepKind::Count | StepKind::CountNonNull => {
                        let counted = match kind {
                            StepKind::Count => None,
                            _ => payload_slot(src),
                        };
                        grouped_rows::<0>(mb, counted, ranges, ord, |g, _, w, null| {
                            let slot = &mut out[g];
                            *slot = slot.wrapping_add(if null { 0 } else { w });
                        })
                    }
                    StepKind::Sum(ScalarKind::Int(fi)) => for_each_fixed_int!(fi, |FI| {
                        grouped_col::<{ FI.width() }>(mb, src, ranges, ord, |g, v, w, null| {
                            let slot = &mut out[g];
                            *slot = slot.wrapping_add(if null { 0 } else { FI.decode_le_i64(&v).wrapping_mul(w) });
                        })
                    }),
                    StepKind::Sum(ScalarKind::F32) => grouped_col::<4>(mb, src, ranges, ord, |g, v, w, null| {
                        if !null {
                            add_float(&mut out[g], f32::from_le_bytes(v) as f64, w);
                        }
                    }),
                    StepKind::Sum(ScalarKind::F64) => grouped_col::<8>(mb, src, ranges, ord, |g, v, w, null| {
                        if !null {
                            add_float(&mut out[g], f64::from_le_bytes(v), w);
                        }
                    }),
                    StepKind::Extreme { .. } => unreachable!("an extreme holds no register sum"),
                }
            }
            GroupedState::Extreme(out, has) => {
                let StepKind::Extreme { max, kind: ImageKind::Scalar(scalar) } = kind else {
                    unreachable!("a scalar extreme's state")
                };
                // `bits` is the value's order image; a MAX keeps the smallest
                // complement, as the value index does.
                let mut step = |g: usize, bits: u64| {
                    let image = if max { !bits } else { bits };
                    if !has[g] || image < out[g] as u64 {
                        out[g] = image as i64;
                        has[g] = true;
                    }
                };
                match (payload_slot(src), scalar) {
                    (Some(_), ScalarKind::Int(fi)) => for_each_fixed_int!(fi, |FI| {
                        grouped_col::<{ FI.width() }>(mb, src, ranges, ord, |g, v, w, null| {
                            if w > 0 && !null {
                                step(g, FI.decode_le_i64(&v) as u64 ^ (FI.is_signed() as u64) << 63);
                            }
                        })
                    }),
                    (Some(_), ScalarKind::F32) => grouped_col::<4>(mb, src, ranges, ord, |g, v, w, null| {
                        if w > 0 && !null {
                            step(g, ieee_order_bits_f32(u32::from_le_bytes(v)));
                        }
                    }),
                    (Some(_), ScalarKind::F64) => grouped_col::<8>(mb, src, ranges, ord, |g, v, w, null| {
                        if w > 0 && !null {
                            step(g, ieee_order_bits(u64::from_le_bytes(v)));
                        }
                    }),
                    // A PK column: OPK bytes, decoded a row at a time.
                    (None, _) => {
                        for (row, &g) in ranges.iter().flat_map(|&(s, e)| s..e).zip(ord) {
                            if mb.get_weight(row) > 0 {
                                step(g as usize, order_bits(&src, mb, row, scalar));
                            }
                        }
                    }
                }
            }
            GroupedState::Each(accs) => {
                let linear = self.is_linear();
                for (row, &g) in ranges.iter().flat_map(|&(s, e)| s..e).zip(ord) {
                    let w = mb.get_weight(row);
                    if linear || w > 0 {
                        accs[g as usize].apply(kind, src, mb, row, w);
                    }
                }
            }
        }
    }

    /// Fold in this aggregate's value from a previously-emitted output row.
    pub(crate) fn fold_stored(&mut self, out_row: &impl RowSource, row: usize) {
        self.apply(self.merge, self.out, out_row, row, 1);
    }

    /// Write this aggregate into its column of the row `out` has open; `None` from
    /// [`Self::value`] renders NULL.
    #[inline]
    pub(crate) fn emit(&self, out: &mut Batch) {
        let ColumnLocator::Payload { slot, size, .. } = self.out else {
            unreachable!("an aggregate is a payload column")
        };
        let pi = slot as usize;
        match self.value() {
            None => out.put_null(pi),
            Some(AggValue::Bits(bits)) => out.extend_col(pi, &bits.to_le_bytes()[..size as usize]),
            Some(AggValue::Wide(WideKind::Bytes, v)) => out.extend_col_blob(pi, v),
            Some(AggValue::Wide(WideKind::Fixed(_), v)) => out.extend_col(pi, v),
        }
    }

    #[inline(always)]
    fn add_float(&mut self, v: f64, weight: i64) {
        let cur = f64::from_bits(self.acc as u64);
        self.acc = f64::to_bits(cur + v * weight as f64) as i64;
    }
}

/// One aggregate's running value for every group of a fold: what
/// [`Accumulator::fold_grouped`] steps and [`Self::take`] hands back to an
/// accumulator, group by group.
pub(crate) enum GroupedState {
    /// A count's or a sum's register image.
    Bits(Vec<i64>),
    /// A scalar extreme's image, and whether the group has one.
    Extreme(Vec<i64>, Vec<bool>),
    /// One accumulator per group: the kinds no column kernel steps.
    Each(Vec<Accumulator>),
}

impl GroupedState {
    /// Grow to `groups` groups, each new one empty. `template` is the
    /// accumulator this state belongs to, in its empty state.
    pub(crate) fn resize(&mut self, groups: usize, template: &Accumulator) {
        match self {
            GroupedState::Bits(vals) => vals.resize(groups, 0),
            GroupedState::Extreme(vals, has) => {
                vals.resize(groups, 0);
                has.resize(groups, false);
            }
            GroupedState::Each(accs) => accs.resize(groups, template.clone()),
        }
    }

    /// Move group `g`'s value into `acc`, replacing what it held. The group's
    /// own value is left unspecified: a group is taken once.
    #[inline]
    pub(crate) fn take(&mut self, g: usize, acc: &mut Accumulator) {
        match self {
            GroupedState::Bits(vals) => {
                acc.acc = vals[g];
                acc.has_extreme = false;
            }
            GroupedState::Extreme(vals, has) => {
                acc.acc = vals[g];
                acc.has_extreme = has[g];
            }
            GroupedState::Each(accs) => std::mem::swap(acc, &mut accs[g]),
        }
    }
}

/// The payload slot `src` names, or `None` for a PK column.
fn payload_slot(src: ColumnLocator) -> Option<usize> {
    match src {
        ColumnLocator::Payload { slot, .. } => Some(slot as usize),
        ColumnLocator::Pk { .. } => None,
    }
}

/// [`grouped_rows`] over payload column `src`.
#[inline(always)]
fn grouped_col<const N: usize>(
    mb: &MemBatch,
    src: ColumnLocator,
    ranges: &[(usize, usize)],
    ord: &[u32],
    f: impl FnMut(usize, [u8; N], i64, bool),
) {
    let slot = payload_slot(src).expect("a grouped kernel folds a payload column only");
    grouped_rows(mb, Some(slot), ranges, ord, f)
}

/// Rows `ranges` of `N`-byte payload column `slot` as `(group, value, weight,
/// is NULL)`, in row order; the `i`-th row's group is `ord[i]`. `N = 0` reads no
/// value, and no `slot` no NULL either.
#[inline(always)]
fn grouped_rows<const N: usize>(
    mb: &MemBatch,
    slot: Option<usize>,
    ranges: &[(usize, usize)],
    ord: &[u32],
    mut f: impl FnMut(usize, [u8; N], i64, bool),
) {
    let weights = mb.weight().as_chunks::<8>().0;
    let nulls = mb.null_bmp().as_chunks::<8>().0;
    let vals = match (N, slot) {
        (0, _) | (_, None) => &[][..],
        (_, Some(slot)) => mb.col_data(slot, N).as_chunks::<N>().0,
    };
    let mut at = 0;
    for &(s, e) in ranges {
        let ord = &ord[at..at + (e - s)];
        at += e - s;
        let rows = ord
            .iter()
            .zip(weights[s..e].iter().zip(&nulls[s..e]))
            .map(|(&g, (w, nw))| {
                let null = slot.is_some_and(|slot| gnitz_wire::null_word_get(u64::from_le_bytes(*nw), slot));
                (g as usize, i64::from_le_bytes(*w), null)
            });
        if N == 0 {
            rows.for_each(|(g, w, null)| f(g, [0; N], w, null));
        } else {
            vals[s..e]
                .iter()
                .zip(rows)
                .for_each(|(v, (g, w, null))| f(g, *v, w, null));
        }
    }
}

/// One accumulator's step over a range of a batch's rows, each at its own
/// weight.
type BulkStep = fn(&mut Accumulator, &MemBatch, Range<usize>);

/// Fold rows `rows` of `N`-byte payload column `src` as `(value, weight, is
/// NULL)`, in row order. `N = 0` reads no value.
#[inline(always)]
fn fold_col<const N: usize, T>(
    mb: &MemBatch,
    src: ColumnLocator,
    rows: Range<usize>,
    init: T,
    mut f: impl FnMut(T, [u8; N], i64, bool) -> T,
) -> T {
    let ColumnLocator::Payload { slot, .. } = src else {
        unreachable!("bulk_step folds a payload column only")
    };
    let (slot, Range { start, end }) = (slot as usize, rows);
    let weights = mb.weight()[start * 8..end * 8].as_chunks::<8>().0;
    let nulls = mb.null_bmp()[start * 8..end * 8].as_chunks::<8>().0;
    let rows = weights.iter().zip(nulls).map(|(w, nw)| {
        (
            i64::from_le_bytes(*w),
            gnitz_wire::null_word_get(u64::from_le_bytes(*nw), slot),
        )
    });
    if N == 0 {
        return rows.fold(init, |a, (w, null)| f(a, [0; N], w, null));
    }
    let vals = mb.col_data(slot, N)[start * N..end * N].as_chunks::<N>().0;
    vals.iter().zip(rows).fold(init, |a, (v, (w, null))| f(a, *v, w, null))
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
