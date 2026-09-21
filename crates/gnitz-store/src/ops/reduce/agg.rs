//! Aggregate descriptors and accumulator state.

use std::cmp::Ordering;

use crate::schema::{ColumnLocator, TypeCode};
use gnitz_expr::RowSource;
use gnitz_wire::{AggFunc, ImageKind, ScalarKind, WideKind};

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
    has_value: bool,
    /// [`AggFunc::empty_renders_zero`]: an untouched slot renders `acc`'s `0`.
    renders_zero: bool,
    /// The aggregated column in an input row.
    src: ColumnLocator,
    /// This aggregate's column in an output row.
    out: ColumnLocator,
    /// What an input row's `src` does to the slot.
    kind: StepKind,
    /// What an output row's `out` does to the slot: [`AggFunc::merge_func`].
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
            AggFunc::Sum | AggFunc::SumZero => StepKind::Sum(ScalarKind::from_type_code(tc)?),
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
            StepKind::of(op, TypeCode::from_validated_u8(loc.type_code()))
                .expect("agg_output_type admits a sum only over a scalar register image")
        };
        Accumulator {
            acc: 0,
            wide: Box::default(),
            has_value: false,
            renders_zero: op.empty_renders_zero(),
            src,
            out,
            kind: step(op, src),
            merge: step(op.merge_func(), out),
        }
    }

    #[inline(always)]
    pub(super) fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    #[inline(always)]
    pub(super) fn is_linear(&self) -> bool {
        !matches!(self.kind, StepKind::Extreme { .. })
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

    /// A COUNT accumulator's net row count.
    #[inline(always)]
    pub(super) fn count_value(&self) -> i64 {
        debug_assert!(matches!(self.kind, StepKind::Count | StepKind::CountNonNull));
        self.acc
    }

    /// The emitted value; `None` renders NULL.
    pub(super) fn value(&self) -> Option<AggValue<'_>> {
        if !self.has_value && !self.renders_zero {
            return None;
        }
        Some(match self.kind {
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
        match self.value() {
            Some(AggValue::Bits(b)) => b,
            v => panic!(
                "value_bits over {}",
                if v.is_none() {
                    "an untouched accumulator"
                } else {
                    "a wide extreme"
                }
            ),
        }
    }

    /// Seed a MIN/MAX with the extreme image the value index holds for it.
    pub(super) fn seed_from_index(&mut self, image: &[u8]) {
        let StepKind::Extreme { max, kind } = self.kind else {
            unreachable!("only an extreme is seeded from the value index")
        };
        match kind {
            ImageKind::Scalar(_) => {
                self.acc = u64::from_be_bytes(image[..8].try_into().unwrap()) as i64;
                self.has_value = true;
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
        self.has_value = true;
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
                self.has_value = true;
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
                        if !self.has_value || image < self.acc as u64 {
                            self.acc = image as i64;
                            self.has_value = true;
                        }
                    }
                    ImageKind::Wide(kind) => {
                        let mut scratch = [0u8; 16];
                        let v = wide_native(&loc, kind, rows, row, &mut scratch);
                        let wins = if max { Ordering::Greater } else { Ordering::Less };
                        if !self.has_value || kind.cmp_native(v, &self.wide) == wins {
                            self.store_wide(v);
                        }
                    }
                }
            }
        }
    }

    #[inline]
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
        self.has_value = true;
    }
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
