//! Aggregate descriptors and accumulator state.

use crate::schema::{ColumnLocator, TypeCode};
use gnitz_expr::RowSource;
use gnitz_wire::{AggFunc, ScalarKind, WideKind};

use super::super::order_image::{wide_native, ImageKind};

/// The value-index parameters of one MIN/MAX aggregate: the column the index
/// reads, how to encode it, and which end of the order the index puts first.
#[derive(Clone, Copy)]
pub(crate) struct ExtremeSpec {
    pub loc: ColumnLocator,
    pub kind: ImageKind,
    pub for_max: bool,
}

/// A stepped accumulator's value: a register image, or a wide extreme's native
/// bytes.
pub(super) enum AggValue<'a> {
    Bits(u64),
    Wide(WideKind, &'a [u8]),
}

/// Accumulator: internal state for one aggregate column. Cloned per epoch (and,
/// in the ad-hoc fold, per group) off the plan's template, then stepped once per
/// input row per aggregate — so everything the step body needs beyond the row
/// itself is resolved in `new`, once per plan.
#[derive(Clone)]
pub struct Accumulator {
    acc: i64,
    /// A wide extreme's native bytes, under the same `has_value` as `acc`.
    wide: Box<[u8]>,
    has_value: bool,
    /// [`AggFunc::is_linear`], hoisted in `new`: the group walk reads it once
    /// per row per aggregate, and `AggFunc::empty_renders_zero` is a
    /// cross-crate `const fn` that would be a real call at `opt-level=0`.
    linear: bool,
    /// [`AggFunc::empty_renders_zero`], hoisted for the same reason — read once
    /// per emitted aggregate column.
    renders_zero: bool,
    /// The aggregated column in an input row.
    src: ColumnLocator,
    /// This aggregate's own column in a reduce **output** row — where
    /// [`Self::fold_stored`] reads its previously-emitted value and
    /// `emit_agg_col` writes the new one.
    out: ColumnLocator,
    kind: StepKind,
}

/// What one row does to the slot, resolved once at construction so the per-row
/// body dispatches on a discriminant instead of re-deriving the answer from an
/// `AggFunc` and a `TypeCode`.
#[derive(Clone, Copy)]
enum StepKind {
    /// Value-independent: count the row before any column read, so a wide
    /// (>8-byte) source column never reaches the value path.
    Count,
    /// Count the row after the NULL gate, still without reading its value.
    CountNonNull,
    /// Add `value * weight` into the slot, reading the value as `ScalarKind`.
    Sum(ScalarKind),
    /// [`Self::Sum`]'s fold under Count's `0` empty-value; see
    /// [`AggFunc::empty_renders_zero`].
    SumZero(ScalarKind),
    /// Keep the extreme: a scalar as its MIN-oriented order image in `acc`, a
    /// wide value as its native bytes in `wide`. `max` picks the direction.
    Extreme { max: bool, kind: ImageKind },
}

/// `AdhocFold` holds `groups × aggregates` of these, with `groups` bounded by
/// the registry's `adhoc_group_cap`.
const _: () = assert!(std::mem::size_of::<Accumulator>() <= 40);

impl Accumulator {
    /// Build the accumulator for `agg_op` over the input column at `src`, emitting
    /// into the output column at `out`. What one row does to the slot is resolved
    /// here, so the per-row body dispatches on neither `agg_op` nor `TypeCode`.
    ///
    /// `None` iff the aggregate sums its argument and the source type has no
    /// scalar register image — the whole aggregate-eligibility rule: the COUNT
    /// family reads no value, and MIN/MAX select a row of any type.
    pub(crate) fn new(agg_op: AggFunc, src: ColumnLocator, out: ColumnLocator) -> Option<Self> {
        let tc = TypeCode::from_validated_u8(src.type_code());
        let scalar = || ScalarKind::from_type_code(tc);
        // Exhaustive over `AggFunc`: a new opcode cannot reach the row path
        // unclassified.
        let kind = match agg_op {
            AggFunc::Count => StepKind::Count,
            AggFunc::CountNonNull => StepKind::CountNonNull,
            AggFunc::Sum => StepKind::Sum(scalar()?),
            AggFunc::SumZero => StepKind::SumZero(scalar()?),
            AggFunc::Min | AggFunc::Max => StepKind::Extreme {
                max: agg_op == AggFunc::Max,
                kind: ImageKind::of(tc)?,
            },
        };
        let linear = agg_op.is_linear();
        // `fold_stored`'s linear arms read the whole slot as one `u64`, which
        // `agg_output_type` guarantees: I64 for the count family and SumZero,
        // the 8-byte register image for SUM.
        debug_assert!(
            !linear || out.size() == 8,
            "a linear aggregate's output column is 8 bytes"
        );
        Some(Accumulator {
            acc: 0,
            wide: Box::default(),
            has_value: false,
            linear,
            renders_zero: agg_op.empty_renders_zero(),
            src,
            out,
            kind,
        })
    }

    #[inline(always)]
    pub(super) fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    #[inline(always)]
    pub(super) fn is_linear(&self) -> bool {
        self.linear
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
            StepKind::Extreme { max, kind } => Some(ExtremeSpec { loc: self.src, kind, for_max: max }),
            _ => None,
        }
    }

    /// [`AggFunc::empty_renders_zero`] for this accumulator's opcode.
    #[inline(always)]
    pub(super) fn empty_renders_zero(&self) -> bool {
        self.renders_zero
    }

    /// True iff the accumulator was never stepped (`has_value` is false) — "no
    /// row contributed," not "the value equals zero." `emit_agg_col` reads this to
    /// pick the empty-render (NULL, or `0` per `empty_renders_zero`).
    #[inline(always)]
    pub(super) fn is_untouched(&self) -> bool {
        !self.has_value
    }

    /// Row count held by a COUNT/COUNT_NON_NULL accumulator — a group's net
    /// cardinality, for the emission gate.
    #[inline(always)]
    pub(super) fn count_value(&self) -> i64 {
        debug_assert!(matches!(self.kind, StepKind::Count | StepKind::CountNonNull));
        self.acc
    }

    /// The emitted value, or `None` for an accumulator no row contributed to —
    /// which renders its empty form, not a value. MIN/MAX invert the order image
    /// they hold; an `F32` extreme also promotes to the `F64` output column
    /// `agg_output_type` declares for it.
    pub(super) fn value(&self) -> Option<AggValue<'_>> {
        if !self.has_value {
            return None;
        }
        Some(match self.kind {
            StepKind::Extreme { kind: ImageKind::Wide(kind), .. } => AggValue::Wide(kind, &self.wide),
            StepKind::Extreme { kind: ImageKind::Scalar(kind), .. } => {
                let bits = kind.order_inverse(self.acc as u64);
                AggValue::Bits(match kind {
                    ScalarKind::F32 => f64::to_bits(f32::from_bits(bits as u32) as f64),
                    _ => bits,
                })
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

    /// Seed a MIN/MAX accumulator with a MIN-oriented order image — what the
    /// AVI probe reads out of the index, never a raw value.
    pub(super) fn seed_encoded_extreme(&mut self, enc: u64) {
        debug_assert!(matches!(
            self.kind,
            StepKind::Extreme { kind: ImageKind::Scalar(_), .. }
        ));
        self.acc = enc as i64;
        self.has_value = true;
    }

    /// Does the MIN-oriented image `enc` beat the current extreme? MAX keeps the
    /// larger image, MIN the smaller. Reads `acc` only, so callers gate on their
    /// own first/has_value state.
    #[inline(always)]
    fn extreme_replaces(&self, max: bool, enc: u64) -> bool {
        if max {
            enc > self.acc as u64
        } else {
            enc < self.acc as u64
        }
    }

    /// Take the extreme of `enc` and what the accumulator already holds.
    #[inline(always)]
    fn fold_extreme(&mut self, max: bool, enc: u64) {
        if !self.has_value || self.extreme_replaces(max, enc) {
            self.acc = enc as i64;
            self.has_value = true;
        }
    }

    /// Seed a wide MIN/MAX accumulator with the native bytes the AVI probe
    /// decoded out of the index.
    pub(super) fn seed_wide(&mut self, v: &[u8]) {
        debug_assert!(matches!(self.kind, StepKind::Extreme { kind: ImageKind::Wide(_), .. }));
        self.store_wide(v);
    }

    /// Take `v` as the held wide extreme, reusing the current allocation at an
    /// unchanged width — which every 16-byte kind, and any equal-length string,
    /// has. Without it a MIN over a descending column mallocs once per row.
    #[inline(always)]
    fn store_wide(&mut self, v: &[u8]) {
        if self.wide.len() == v.len() {
            self.wide.copy_from_slice(v);
        } else {
            self.wide = v.into();
        }
        self.has_value = true;
    }

    /// [`Self::fold_extreme`] over native bytes. An equal value keeps the held
    /// one, so a tie costs no copy.
    #[inline(always)]
    fn fold_wide(&mut self, max: bool, kind: WideKind, v: &[u8]) {
        let wanted = if max {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
        if !self.has_value || kind.cmp_native(v, &self.wide) == wanted {
            self.store_wide(v);
        }
    }

    /// Fold the extreme held in `rows[row]`'s column `loc` — the one body both
    /// the input-row step and the stored-output fold reach, so the scalar and
    /// wide arms have one spelling.
    #[inline(always)]
    fn fold_extreme_at(&mut self, loc: ColumnLocator, rows: &impl RowSource, row: usize, max: bool, kind: ImageKind) {
        match kind {
            ImageKind::Scalar(kind) => self.fold_extreme(max, loc.order_bits(rows, row, kind)),
            ImageKind::Wide(kind) => {
                let mut scratch = [0u8; 16];
                self.fold_wide(max, kind, wide_native(&loc, kind, rows, row, &mut scratch));
            }
        }
    }

    /// Step: incorporate one input row into the accumulator.
    ///
    /// Runs once per input row per aggregate, so it dispatches on the
    /// pre-resolved [`StepKind`] — no `AggFunc` compare, no `TypeCode` match.
    #[inline]
    pub(super) fn step_from_batch(&mut self, mb: &impl RowSource, row: usize, weight: i64) {
        // COUNT is the only kind that counts a NULL row, so it is the only
        // exemption from the gate — and counting before the read is what keeps a
        // wide (>8-byte) source column off the value path entirely.
        if !matches!(self.kind, StepKind::Count) && self.src.is_null(mb, row) {
            return;
        }
        match self.kind {
            StepKind::Count | StepKind::CountNonNull => {
                self.acc = self.acc.wrapping_add(weight);
                self.has_value = true;
            }
            StepKind::Sum(ScalarKind::Int(fi)) | StepKind::SumZero(ScalarKind::Int(fi)) => {
                self.acc = self
                    .acc
                    .wrapping_add(self.src.decode_i64(mb, row, fi).wrapping_mul(weight));
                self.has_value = true;
            }
            StepKind::Sum(ScalarKind::F32) | StepKind::SumZero(ScalarKind::F32) => {
                let bits = self.src.bytes(mb, row);
                self.add_float(
                    f32::from_bits(u32::from_le_bytes(bits.try_into().unwrap())) as f64,
                    weight,
                );
            }
            StepKind::Sum(ScalarKind::F64) | StepKind::SumZero(ScalarKind::F64) => {
                let bits = self.src.bytes(mb, row);
                self.add_float(f64::from_bits(u64::from_le_bytes(bits.try_into().unwrap())), weight);
            }
            StepKind::Extreme { max, kind } => {
                // The arm ignores `weight` beyond its sign: an extreme is a
                // property of which rows are present, and a retraction makes it
                // recede in a way no compare can express — so both callers walk
                // only positive rows and hand a receding extreme to the AVI.
                debug_assert!(weight > 0, "an extreme accumulator must only see positive weights");
                self.fold_extreme_at(self.src, mb, row, max, kind);
            }
        }
    }

    /// Fold this aggregate's previously-emitted output value back in — the
    /// `new = old + Σdelta` seed on a linear aggregate, and the `combine(old,
    /// pos)` fold on the AVI probe-skip path. A NULL old value contributes
    /// nothing: folding its zero bytes would decode NULL as 0.
    pub(super) fn fold_stored(&mut self, out_row: &impl RowSource, row: usize) {
        if self.out.is_null(out_row, row) {
            return;
        }
        match self.kind {
            // Branch on the *source* type, not the output column's: a float
            // SumZero's column is labelled I64 while the accumulator holds `f64`
            // bits, and both ends of this round trip read the source.
            StepKind::Sum(k) | StepKind::SumZero(k) if k.is_float() => {
                let cur = f64::from_bits(self.acc as u64);
                self.acc = f64::to_bits(cur + f64::from_bits(self.stored_u64(out_row, row))) as i64;
                self.has_value = true;
            }
            StepKind::Count | StepKind::CountNonNull | StepKind::Sum(_) | StepKind::SumZero(_) => {
                self.acc = self.acc.wrapping_add(self.stored_u64(out_row, row) as i64);
                self.has_value = true;
            }
            StepKind::Extreme { max, kind } => {
                // `agg_output_type(Min|Max, src) == src` for every non-float
                // source, so the stored column is in the accumulator's own
                // domain and re-encoding it is exact. A float source's output
                // widens to F64 and always probes instead.
                debug_assert!(
                    !matches!(kind, ImageKind::Scalar(k) if k.is_float()),
                    "a float extreme is never folded from its output column"
                );
                self.fold_extreme_at(self.out, out_row, row, max, kind);
            }
        }
    }

    /// The stored output value as one 8-byte slot. Total for the linear kinds:
    /// `Accumulator::new` asserts their output column's width.
    #[inline]
    fn stored_u64(&self, out_row: &impl RowSource, row: usize) -> u64 {
        u64::from_le_bytes(self.out.bytes(out_row, row).try_into().unwrap())
    }

    /// Accumulate `v * weight` into the float slot (the accumulator holds
    /// `f64::to_bits` for a float aggregate).
    #[inline]
    fn add_float(&mut self, v: f64, weight: i64) {
        let cur = f64::from_bits(self.acc as u64);
        self.acc = f64::to_bits(cur + v * weight as f64) as i64;
        self.has_value = true;
    }
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
