//! Aggregate descriptors and accumulator state.

use crate::schema::{ColumnLocator, TypeCode};
use gnitz_expr::RowSource;
use gnitz_wire::{AggFunc, ScalarKind};

/// Descriptor for one aggregate function — exactly the `(AggFunc, u16)` spec the
/// wire ships. It carries no column *type*: that is `schema.columns[col_idx]`,
/// which every consumer already holds, resolved or not.
#[derive(Clone, Copy)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: AggFunc,
}

/// The value-index parameters of one MIN/MAX aggregate: the column the index
/// reads, how to encode it, and which end of the order the index puts first.
/// Named fields rather than a positional triple — two of the three are scalars
/// the compiler could not tell apart.
#[derive(Clone, Copy)]
pub(crate) struct ExtremeSpec {
    pub loc: ColumnLocator,
    pub kind: ScalarKind,
    pub for_max: bool,
}

/// Accumulator: internal state for one aggregate column. Cloned per epoch (and,
/// in the ad-hoc fold, per group) off the plan's template, then stepped once per
/// input row per aggregate — so everything the step body needs beyond the row
/// itself is resolved in `new`, once per plan.
#[derive(Clone)]
pub struct Accumulator {
    acc: i64,
    has_value: bool,
    /// [`AggFunc::is_linear`] as `kind` spells it, resolved in `new`: the group
    /// walk reads it once per row per aggregate.
    linear: bool,
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
    /// Keep the extreme as its MIN-oriented order image; `max` picks the
    /// direction (the image itself is always MIN-oriented).
    Extreme { max: bool, kind: ScalarKind },
}

/// `AdhocFold` holds `groups × aggregates` of these, with `groups` bounded by
/// the registry's `adhoc_group_cap`.
const _: () = assert!(std::mem::size_of::<Accumulator>() <= 24);

impl Accumulator {
    /// Build the accumulator for `agg_op` over the input column at `src`, emitting
    /// into the output column at `out`. What one row does to the slot is resolved
    /// here, so the per-row body dispatches on neither `agg_op` nor `TypeCode`.
    ///
    /// `None` iff the aggregate reads its argument's value and the source type has
    /// no scalar register image — the whole aggregate-eligibility rule, the COUNT
    /// family reading no value and taking any type.
    pub(crate) fn new(agg_op: AggFunc, src: ColumnLocator, out: ColumnLocator) -> Option<Self> {
        let scalar = || ScalarKind::from_type_code(TypeCode::from_validated_u8(src.type_code()));
        // Exhaustive over `AggFunc`: a new opcode cannot reach the row path
        // unclassified.
        let kind = match agg_op {
            AggFunc::Count => StepKind::Count,
            AggFunc::CountNonNull => StepKind::CountNonNull,
            AggFunc::Sum => StepKind::Sum(scalar()?),
            AggFunc::SumZero => StepKind::SumZero(scalar()?),
            AggFunc::Min | AggFunc::Max => StepKind::Extreme {
                max: agg_op == AggFunc::Max,
                kind: scalar()?,
            },
        };
        let linear = !matches!(kind, StepKind::Extreme { .. });
        // `fold_stored`'s linear arms read the whole slot as one `u64`, which
        // `agg_output_type` guarantees: I64 for the count family and SumZero,
        // the 8-byte register image for SUM.
        debug_assert!(
            !linear || out.size() == 8,
            "a linear aggregate's output column is 8 bytes"
        );
        Some(Accumulator {
            acc: 0,
            has_value: false,
            linear,
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
            StepKind::Extreme { max, kind } => Some(ExtremeSpec {
                loc: self.src,
                kind,
                for_max: max,
            }),
            _ => None,
        }
    }

    /// The zero-identity family, off the resolved kind — the same partition
    /// [`AggFunc::empty_renders_zero`] names, and `tests/agg.rs` holds the two
    /// answers equal for every opcode the wire can name.
    pub(super) fn empty_renders_zero(&self) -> bool {
        matches!(
            self.kind,
            StepKind::Count | StepKind::CountNonNull | StepKind::SumZero(_)
        )
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

    /// The emitted value's bits, truncated to the output column's width by the
    /// caller. MIN/MAX invert the order image they hold; an `F32` extreme also
    /// promotes to the `F64` output column `agg_output_type` declares for it.
    pub(super) fn get_value_bits(&self) -> u64 {
        match self.kind {
            StepKind::Extreme { kind, .. } => {
                let bits = kind.order_inverse(self.acc as u64);
                match kind {
                    ScalarKind::F32 => f64::to_bits(f32::from_bits(bits as u32) as f64),
                    _ => bits,
                }
            }
            _ => self.acc as u64,
        }
    }

    /// Seed a MIN/MAX accumulator with a MIN-oriented order image — what the
    /// AVI probe reads out of the index, never a raw value.
    pub(super) fn seed_encoded_extreme(&mut self, enc: u64) {
        debug_assert!(matches!(self.kind, StepKind::Extreme { .. }));
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
                self.fold_extreme(max, self.src.order_bits(mb, row, kind));
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
                // `agg_output_type(Min|Max, src) == src` for every integer
                // source, so the stored column is in the accumulator's own
                // domain and re-encoding it is exact. A float source's output
                // widens to F64 and always probes instead.
                debug_assert!(
                    !kind.is_float(),
                    "a float extreme is never folded from its output column"
                );
                self.fold_extreme(max, self.out.order_bits(out_row, row, kind));
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
