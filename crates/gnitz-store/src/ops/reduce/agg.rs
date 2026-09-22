//! Aggregate descriptors and accumulator state.

use std::cmp::Ordering;
use std::ops::Range;

use crate::schema::{ColumnLocator, TypeCode};
use crate::storage::MemBatch;
use gnitz_expr::RowSource;
use gnitz_wire::{AggFunc, FixedInt, ImageKind, ScalarKind, WideKind};

use super::super::order_image::{scalar_image, scalar_native_of_image, wide_native, wide_native_of_image};

/// The value-index parameters of one MIN/MAX aggregate: the column the index
/// reads, how to encode it, and which end of the order the index puts first.
#[derive(Clone, Copy)]
pub(super) struct ExtremeSpec {
    pub loc: ColumnLocator,
    pub kind: ImageKind,
    pub max: bool,
}

/// A stepped accumulator's value: a register image, or a wide extreme's native
/// bytes.
pub(super) enum AggValue<'a> {
    Bits(u64),
    Wide(WideKind, &'a [u8]),
}

/// One aggregate's running value. What a row does to it is resolved in `new`.
#[derive(Clone)]
pub(super) struct Accumulator {
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

/// `AdhocFold` holds `groups × aggregates` of these, with `groups` bounded by
/// the registry's `adhoc_group_cap`.
const _: () = assert!(std::mem::size_of::<Accumulator>() <= 40);

impl Accumulator {
    /// `op` over input column `src`, emitting into output column `out`.
    pub(super) fn new(op: AggFunc, src: ColumnLocator, out: ColumnLocator) -> Self {
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
    pub(super) fn reset(&mut self) {
        self.acc = 0;
        self.has_extreme = false;
    }

    #[inline(always)]
    pub(super) fn is_linear(&self) -> bool {
        !matches!(self.kind, StepKind::Extreme { .. })
    }

    pub(super) fn sums_float(&self) -> bool {
        matches!(self.kind, StepKind::Sum(ScalarKind::F32 | ScalarKind::F64))
    }

    /// Width of this aggregate's output column — the emitted value's truncation.
    #[inline(always)]
    pub(super) fn out_size(&self) -> usize {
        self.out.size()
    }

    /// This aggregate's value-index parameters, or `None` for a linear one.
    #[inline(always)]
    pub(super) fn extreme_index_spec(&self) -> Option<ExtremeSpec> {
        match self.kind {
            StepKind::Extreme { max, kind } => Some(ExtremeSpec { loc: self.src, kind, max }),
            _ => None,
        }
    }

    /// The net row count a COUNT, or a combine's sum of partial COUNTs, holds.
    #[inline(always)]
    pub(super) fn count_value(&self) -> i64 {
        debug_assert!(matches!(
            self.kind,
            StepKind::Count | StepKind::CountNonNull | StepKind::Sum(ScalarKind::Int(FixedInt::I64))
        ));
        self.acc
    }

    /// The emitted value; `None` renders NULL.
    pub(super) fn value(&self) -> Option<AggValue<'_>> {
        Some(match self.kind {
            StepKind::Extreme { .. } if !self.has_extreme => return None,
            StepKind::Extreme { kind: ImageKind::Wide(kind), .. } => AggValue::Wide(kind, &self.wide),
            StepKind::Extreme { kind: ImageKind::Scalar(kind), max } => {
                AggValue::Bits(scalar_native_of_image(kind, max, self.acc as u64))
            }
            _ => AggValue::Bits(self.acc as u64),
        })
    }

    /// [`Self::value`]'s scalar half.
    #[cfg(test)]
    pub(super) fn value_bits(&self) -> u64 {
        let Some(AggValue::Bits(b)) = self.value() else {
            panic!("value_bits over a NULL or wide accumulator")
        };
        b
    }

    /// Seed a MIN/MAX with the extreme image the value index holds for it.
    pub(super) fn seed_from_index(&mut self, image: &[u8]) {
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

    /// This accumulator's [`BulkStep`]. A linear aggregate over a payload column
    /// folds the column's value, weight and null regions in one loop; an
    /// extreme, or a PK column, steps each row as [`Self::step_from_batch`] does.
    /// Rows fold in row order, so a float sum matches stepping each row.
    pub(super) fn bulk_step(&self) -> BulkStep {
        let ColumnLocator::Payload { .. } = self.src else {
            return Self::step_each;
        };
        match self.kind {
            StepKind::Count => Self::count_rows,
            StepKind::CountNonNull => Self::count_non_null_rows,
            StepKind::Sum(ScalarKind::Int(fi)) => match fi {
                FixedInt::U8 => Self::sum_int_rows::<1, false>,
                FixedInt::I8 => Self::sum_int_rows::<1, true>,
                FixedInt::U16 => Self::sum_int_rows::<2, false>,
                FixedInt::I16 => Self::sum_int_rows::<2, true>,
                FixedInt::U32 => Self::sum_int_rows::<4, false>,
                FixedInt::I32 => Self::sum_int_rows::<4, true>,
                // A wrapping 64-bit sum is the same bits signed or unsigned.
                FixedInt::U64 | FixedInt::I64 => Self::sum_int_rows::<8, true>,
            },
            StepKind::Sum(ScalarKind::F32) => Self::sum_float_rows::<4>,
            StepKind::Sum(ScalarKind::F64) => Self::sum_float_rows::<8>,
            StepKind::Extreme { .. } => Self::step_each,
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

    /// SUM over an `N`-byte integer column, sign-extended when `SIGNED`.
    fn sum_int_rows<const N: usize, const SIGNED: bool>(&mut self, mb: &MemBatch, rows: Range<usize>) {
        let shift = 64 - 8 * N as u32;
        self.acc = fold_col::<N, _>(mb, self.src, rows, self.acc, |a, v, w, null| {
            let mut le = [0u8; 8];
            le[..N].copy_from_slice(&v);
            let bits = u64::from_le_bytes(le) << shift;
            let v = if SIGNED {
                (bits as i64) >> shift
            } else {
                (bits >> shift) as i64
            };
            a.wrapping_add(if null { 0 } else { v.wrapping_mul(w) })
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

    #[inline(always)]
    pub(super) fn step_from_batch(&mut self, mb: &impl RowSource, row: usize, weight: i64) {
        self.apply(self.kind, self.src, mb, row, weight);
    }

    /// Fold in this aggregate's value from a previously-emitted output row.
    pub(super) fn fold_stored(&mut self, out_row: &impl RowSource, row: usize) {
        self.apply(self.merge, self.out, out_row, row, 1);
    }

    #[inline(always)]
    fn add_float(&mut self, v: f64, weight: i64) {
        let cur = f64::from_bits(self.acc as u64);
        self.acc = f64::to_bits(cur + v * weight as f64) as i64;
    }
}

/// One accumulator's step over a range of a batch's rows, each at its own
/// weight. [`Accumulator::bulk_step`] picks it once per fold.
pub(super) type BulkStep = fn(&mut Accumulator, &MemBatch, Range<usize>);

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
    let rows = weights
        .iter()
        .zip(nulls)
        .map(|(w, nw)| (i64::from_le_bytes(*w), (u64::from_le_bytes(*nw) >> slot) & 1 == 1));
    if N == 0 {
        return rows.fold(init, |a, (w, null)| f(a, [0; N], w, null));
    }
    let vals = mb.col_data(slot, N)[start * N..end * N].as_chunks::<N>().0;
    vals.iter().zip(rows).fold(init, |a, (v, (w, null))| f(a, *v, w, null))
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
