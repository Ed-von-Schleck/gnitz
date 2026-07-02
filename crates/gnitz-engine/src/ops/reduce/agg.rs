//! Aggregate opcodes, descriptors, accumulator state, and AVI lookup.

use crate::schema::{ColumnLocator, SchemaDescriptor, TypeCode};
use crate::storage::{MemBatch, ReadCursor};

// ---------------------------------------------------------------------------
// Aggregate opcodes
// ---------------------------------------------------------------------------

/// Aggregate function selector.
///
/// `#[repr(u8)]` keeps `AggDescriptor` at its required 8-byte C layout, and
/// lets the compiler enforce exhaustive matching instead of the previous bare
/// `u8` with private named constants.  `Null` (0) is the NullAggregate
/// sentinel emitted by the compiler when no aggregation is configured.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AggOp {
    Null = 0,
    Count = 1,
    Sum = 2,
    Min = 3,
    Max = 4,
    CountNonNull = 5,
    /// `Sum`'s fold with `Count`'s `0` identity: sums values like `Sum` but
    /// grounds/renders an untouched accumulator to `0` instead of NULL. The
    /// two-phase global-aggregate combine sums per-worker partial count columns
    /// with this (a COUNT's empty value is `0`, not NULL).
    SumZero = 6,
}

impl TryFrom<u8> for AggOp {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(AggOp::Null),
            1 => Ok(AggOp::Count),
            2 => Ok(AggOp::Sum),
            3 => Ok(AggOp::Min),
            4 => Ok(AggOp::Max),
            5 => Ok(AggOp::CountNonNull),
            6 => Ok(AggOp::SumZero),
            other => Err(other),
        }
    }
}

impl From<gnitz_wire::AggFunc> for AggOp {
    fn from(f: gnitz_wire::AggFunc) -> Self {
        match f {
            gnitz_wire::AggFunc::Count => AggOp::Count,
            gnitz_wire::AggFunc::Sum => AggOp::Sum,
            gnitz_wire::AggFunc::Min => AggOp::Min,
            gnitz_wire::AggFunc::Max => AggOp::Max,
            gnitz_wire::AggFunc::CountNonNull => AggOp::CountNonNull,
            gnitz_wire::AggFunc::SumZero => AggOp::SumZero,
        }
    }
}

impl AggOp {
    pub fn is_linear(self) -> bool {
        matches!(self, AggOp::Count | AggOp::Sum | AggOp::CountNonNull | AggOp::SumZero)
    }

    /// True iff an untouched accumulator renders `0`, not NULL — the
    /// zero-identity family. COUNT / COUNT_NON_NULL count rows (empty = 0);
    /// SumZero is Sum's fold under Count's 0 identity (the two-phase
    /// partial-count combine). SUM / MIN / MAX have a NULL empty value.
    pub fn empty_renders_zero(self) -> bool {
        matches!(self, AggOp::Count | AggOp::CountNonNull | AggOp::SumZero)
    }

    /// True iff this aggregate is maintained through the combined AggValueIndex —
    /// the order-encodable extremes MIN/MAX. This is the single source of truth
    /// for "which aggregates the value index serves, and in what ordinal order":
    /// the index write side (`op_integrate_with_indexes`) and the reduce read
    /// side (`op_reduce`) both select and order their entries by this predicate
    /// over `agg_descs`, so the two agree by construction. Note it is *not* the
    /// negation of `is_linear`: `Null` is neither linear nor value-indexed.
    pub fn uses_value_index(self) -> bool {
        matches!(self, AggOp::Min | AggOp::Max)
    }
}

/// Descriptor for one aggregate function.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: AggOp,
    pub col_type_code: TypeCode,
    pub _pad: [u8; 2],
}

const _: () = assert!(std::mem::size_of::<AggOp>() == 1);
const _: () = assert!(std::mem::size_of::<TypeCode>() == 1);
const _: () = assert!(std::mem::size_of::<AggDescriptor>() == 8);
const _: () = assert!(std::mem::align_of::<AggDescriptor>() == 4);

/// Accumulator: internal state for one aggregate column.
///
/// Field order: largest alignment first to minimise padding.
pub(super) struct Accumulator {
    acc: i64,
    col_type_code: TypeCode,
    agg_op: AggOp,
    /// Where the aggregated column's value lives in a row (PK byte offset or
    /// dense payload slot). Resolved once; the per-row read goes through it.
    loc: ColumnLocator,
    has_value: bool,
}

impl Accumulator {
    pub(super) fn new(desc: &AggDescriptor, schema: &SchemaDescriptor) -> Self {
        Accumulator {
            acc: 0,
            has_value: false,
            agg_op: desc.agg_op,
            col_type_code: desc.col_type_code,
            loc: schema.locate(desc.col_idx as usize),
        }
    }

    pub(super) fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    pub(super) fn is_linear(&self) -> bool {
        self.agg_op.is_linear()
    }

    /// Delegates to [`AggOp::empty_renders_zero`] (the agg-op rationale lives
    /// there); `emit_agg_col` reads it to render an untouched accumulator as `0`
    /// rather than NULL without a per-op switch.
    pub(super) fn empty_renders_zero(&self) -> bool {
        self.agg_op.empty_renders_zero()
    }

    /// True iff the accumulator was never stepped (`has_value` is false) — "no
    /// row contributed," not "the value equals zero." `emit_agg_col` reads this to
    /// pick the empty-render (NULL, or `0` per `empty_renders_zero`).
    pub(super) fn is_untouched(&self) -> bool {
        !self.has_value
    }

    /// Row count held by a COUNT/COUNT_NON_NULL accumulator — a group's net
    /// cardinality, for the emission gate. Meaningful only for the count family.
    pub(super) fn count_value(&self) -> i64 {
        self.acc
    }

    pub(super) fn get_value_bits(&self) -> u64 {
        // MIN/MAX hold the MIN-oriented order-preserving encoding; decode back to
        // native value bits for emit. Linear aggregates store the value verbatim.
        if self.agg_op.uses_value_index() {
            super::super::util::decode_ordered(self.acc as u64, self.col_type_code, false)
        } else {
            self.acc as u64
        }
    }

    pub(super) fn seed_from_raw_bits(&mut self, bits: u64) {
        self.acc = bits as i64;
        self.has_value = true;
    }

    fn is_float(&self) -> bool {
        self.col_type_code.is_float()
    }

    /// Step: incorporate one input row into the accumulator.
    pub(super) fn step_from_batch(&mut self, mb: &MemBatch, row: usize, weight: i64) {
        // COUNT is value-independent: count the row and return before any column
        // read, so a wide PK column (cs = 16) never reaches the ≤8-byte value path.
        if self.agg_op == AggOp::Count {
            self.acc = self.acc.wrapping_add(weight);
            self.has_value = true;
            return;
        }

        // Null gate for nullable payload columns. PK columns are never null
        // (catalog rule), so `is_null` returns false for them.
        if self.loc.is_null(mb, row) {
            return;
        }

        // COUNT_NON_NULL: presence established (PK, or non-null payload above).
        // Count the row without reading its value.
        if self.agg_op == AggOp::CountNonNull {
            self.acc = self.acc.wrapping_add(weight);
            self.has_value = true;
            return;
        }

        let tc = self.col_type_code;
        let cs = self.loc.size();
        // SUM accumulates into an i64/u64 slot via decode_signed/decode_float;
        // MIN/MAX order-encode via encode_ordered, which handles only the
        // order-encodable ≤8-byte int/float types — a non-encodable MIN/MAX source
        // (STRING, U128/UUID/BLOB) is rejected when the reduce circuit is compiled
        // (`compiler::emit_reduce`) and never reaches here. STRING stays exempt from
        // this assert for SUM: decode_signed handles a 16-byte STRING slot via an
        // 8-byte prefix compare, so an engine-level SUM caller can reach here with
        // one. Every other column addressed here is ≤ 8 bytes.
        debug_assert!(
            cs <= 8 || tc == TypeCode::String,
            "SUM/MIN/MAX over a >8-byte non-STRING column must be rejected by the planner",
        );

        // `native_le_bytes` OPK-decodes a PK-source column back to native
        // little-endian before aggregating; a payload column reads verbatim.
        let mut pk_scratch = [0u8; 16];
        let bytes = self.loc.native_le_bytes(mb, row, &mut pk_scratch);

        let first = !self.has_value;
        self.has_value = true;
        let is_f = self.is_float();

        match self.agg_op {
            // SumZero folds identically to Sum (it differs only in its identity /
            // empty-render, handled by the seed and `emit_agg_col`).
            AggOp::Sum | AggOp::SumZero => {
                if is_f {
                    let val_f = decode_float(bytes, tc);
                    let cur_f = f64::from_bits(self.acc as u64);
                    self.acc = f64::to_bits(cur_f + val_f * weight as f64) as i64;
                } else {
                    let val = decode_signed(bytes, tc);
                    self.acc = self.acc.wrapping_add(val.wrapping_mul(weight));
                }
            }
            // MIN/MAX hold the AVI's MIN-oriented order-preserving encoding
            // (`encode_ordered`, `for_max=false`), so the extreme test is one
            // unsigned `u64` compare — the U64-unsigned and float-total-order
            // rules live solely in the codec, never duplicated here. The encoded
            // extreme is decoded back to native bits at emit (`get_value_bits`).
            // Only order-encodable types reach here: a non-encodable MIN/MAX
            // source (STRING, U128/UUID/BLOB) is rejected when the reduce circuit
            // is compiled (`compiler::emit_reduce`), never executed, so
            // `encode_ordered`'s unreachable arm is genuinely unreachable.
            AggOp::Min | AggOp::Max => {
                let enc = super::super::util::encode_ordered(bytes, tc as u8, false);
                let cur = self.acc as u64;
                let replaces = if self.agg_op == AggOp::Max {
                    enc > cur
                } else {
                    enc < cur
                };
                if first || replaces {
                    self.acc = enc as i64;
                }
            }
            // Count and CountNonNull return early above; Null is a no-op sentinel.
            AggOp::Null => {}
            AggOp::Count | AggOp::CountNonNull => unreachable!("handled by early return above"),
        }
    }

    /// Fold a stored linear aggregate value (read back from `trace_out`) into
    /// this accumulator at weight +1 — the `new = old + Σdelta` seed on
    /// `op_reduce`'s linear paths. Only ever reached for the linear COUNT/SUM
    /// family: `fold_old_aggs` skips the non-linear MIN/MAX (whose extreme the
    /// AVI owns and overwrites), so those arms are unreachable. COUNT must
    /// integer-fold even over a float source column, so the COUNT-vs-SUM split is
    /// load-bearing — it cannot collapse to a single `is_float` branch.
    pub(super) fn merge_accumulated(&mut self, value_bits: u64) {
        match self.agg_op {
            AggOp::Count | AggOp::CountNonNull => {
                self.acc = self.acc.wrapping_add(value_bits as i64);
                self.has_value = true;
            }
            AggOp::Sum | AggOp::SumZero => {
                if self.is_float() {
                    let cur_f = f64::from_bits(self.acc as u64);
                    self.acc = f64::to_bits(cur_f + f64::from_bits(value_bits)) as i64;
                } else {
                    self.acc = self.acc.wrapping_add(value_bits as i64);
                }
                self.has_value = true;
            }
            AggOp::Min | AggOp::Max | AggOp::Null => {
                unreachable!("fold_old_aggs folds only linear aggregates")
            }
        }
    }
}

/// Decode a column's bytes into an `i64`-shaped accumulator slot.
///
/// For sub-64-bit unsigned types (`U8`/`U16`/`U32`) the value is
/// zero-extended into the i64, so signed comparison still orders
/// correctly. Signed types are sign-extended as expected.
///
/// **U64 caveat:** the return value is the U64 bit pattern reinterpreted
/// as `i64` — *not* a sign-extended signed value. Callers comparing the
/// result for MIN/MAX ordering on a `U64` column must cast back to `u64`
/// before comparing, otherwise values with the high bit set order
/// incorrectly. SUM treats the slot as a bit container; wrap-around is
/// unaffected by signedness.
///
/// STRING columns use the first 8 bytes of the 16-byte German String
/// struct as the compare key (caller passes the full 16-byte slice for
/// the row). This produces wrong orderings — the binder must reject
/// MIN/MAX on String before the operator sees it.
#[inline]
fn decode_signed(bytes: &[u8], tc: TypeCode) -> i64 {
    match tc {
        TypeCode::I8 => bytes[0] as i8 as i64,
        TypeCode::U8 => bytes[0] as i64,
        TypeCode::I16 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        TypeCode::U16 => u16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        TypeCode::I32 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        TypeCode::U32 => u32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        TypeCode::I64 | TypeCode::U64 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        TypeCode::String => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        TypeCode::F32 | TypeCode::F64 | TypeCode::U128 | TypeCode::UUID | TypeCode::Blob | TypeCode::I128 => {
            unreachable!("decode_signed: non-integer/string type")
        }
    }
}

/// Decode a column's bytes as f64, with proper F32→F64 promotion.
#[inline]
fn decode_float(bytes: &[u8], tc: TypeCode) -> f64 {
    match tc {
        TypeCode::F32 => f32::from_bits(u32::from_le_bytes(bytes[..4].try_into().unwrap())) as f64,
        TypeCode::F64 => f64::from_bits(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        TypeCode::U8
        | TypeCode::I8
        | TypeCode::U16
        | TypeCode::I16
        | TypeCode::U32
        | TypeCode::I32
        | TypeCode::U64
        | TypeCode::I64
        | TypeCode::U128
        | TypeCode::UUID
        | TypeCode::String
        | TypeCode::Blob
        | TypeCode::I128 => unreachable!("decode_float: non-float type"),
    }
}

/// Reconstruct the 8-byte `i64` accumulator bits from an emitted agg column.
///
/// `bytes.len()` is the *output* column width. An 8-byte column (SUM, COUNT,
/// `I64`/`U64`/`F64` — including a float MIN/MAX, which widens to `F64` and
/// stores the `f64::to_bits` value even for an `F32` source) holds the raw
/// accumulator bits verbatim. A narrow (<8-byte) column is only ever a
/// narrow-integer MIN/MAX value; `decode_signed` sign/zero-extends it back into
/// the slot exactly as the load path produced it.
///
/// Width-gating (not a source-type dispatch) is load-bearing: a float MIN/MAX
/// stores `F64` bits under an `F32` source type, and COUNT can carry a
/// non-integer source `col_type_code` (e.g. `COUNT(uuid_col)` → `UUID`) — either
/// would mis-decode or hit `decode_signed`'s `unreachable!()` if dispatched on
/// the source type. The narrow branch is reached only for narrow integers, where
/// `decode_signed` is exactly right.
pub(super) fn readback_agg_bits(bytes: &[u8], src_tc: TypeCode) -> u64 {
    if bytes.len() == 8 {
        u64::from_le_bytes(bytes.try_into().unwrap())
    } else {
        decode_signed(bytes, src_tc) as u64
    }
}

/// Fold the old aggregate values stored in `cursor`'s current row into `accs`
/// at weight +1 — the `new = old + delta` step on `op_reduce`'s linear fast path,
/// reused in the combined-index path to fold a mixed reduce's linear companions.
///
/// A NULL old aggregate contributes nothing: folding its zero bytes would
/// saturate `has_value`, decoding NULL as 0. The aggregates are the trailing
/// output columns (`build_reduce_output_schema` appends them last), so `cbase`
/// (first agg column index) and `pbase` (first agg payload slot) address each
/// value and its null bit at any PK arity.
///
/// Non-linear (MIN/MAX) accumulators are skipped: the combined AVI owns each
/// extreme and `apply_agg_from_value_index` *overwrites* the accumulator after
/// this fold (or resets it on an empty seek), so folding the old extreme here
/// would be discarded regardless. The skip just avoids a wasted `trace_out`
/// read + fold per non-linear aggregate, and documents that the AVI —
/// never the fold — owns the non-linear value. The all-linear caller never
/// skips (every accumulator is linear).
pub(super) fn fold_old_aggs(
    accs: &mut [Accumulator],
    cursor: &ReadCursor,
    agg_descs: &[AggDescriptor],
    agg_col_widths: &[usize],
    cbase: usize,
    pbase: usize,
) {
    for (k, acc) in accs.iter_mut().enumerate() {
        if !agg_descs[k].agg_op.is_linear() {
            continue;
        }
        if (cursor.current_null_word >> (pbase + k)) & 1 != 0 {
            continue;
        }
        let cw = agg_col_widths[k];
        let ptr = cursor.col_ptr(cbase + k, cw);
        if !ptr.is_null() {
            let bytes = unsafe { std::slice::from_raw_parts(ptr, cw) };
            acc.merge_accumulated(readback_agg_bits(bytes, agg_descs[k].col_type_code));
        }
    }
}

/// True iff `group_cols` is a single column whose type permits using
/// the source column directly as the output PK (vs. a synthetic U128).
/// Shared between `query::compiler::build_reduce_output_schema` and
/// `op_reduce` to keep schema construction and execution in lockstep.
pub(crate) fn is_single_col_natural_pk(schema: &SchemaDescriptor, group_cols: &[u32]) -> bool {
    if group_cols.len() != 1 {
        return false;
    }
    let col = &schema.columns[group_cols[0] as usize];
    col.nullable == 0
        && matches!(
            TypeCode::from_validated_u8(col.type_code),
            TypeCode::U64 | TypeCode::U128 | TypeCode::UUID,
        )
}

// ---------------------------------------------------------------------------
// AVI lookup
// ---------------------------------------------------------------------------

/// Seek the AVI cursor to a group via its full byte-form seek prefix and seed the
/// accumulator with the group's MIN-oriented order-encoded extreme (decoded back
/// to a native value later, at emit, in `get_value_bits`).
///
/// `group_key` is the opaque seek prefix `group_cols ‖ ordinal` — the caller
/// appends the one-byte per-aggregate ordinal so a single combined index can
/// serve several MIN/MAX of the same group without collision. The full AVI PK is
/// `group_cols ‖ ordinal ‖ av_encoded(8)`, ordered group-major, then by ordinal,
/// then by value, so the first positive-weight entry whose key starts with
/// `group_key` is the extremal value for that group+aggregate, and the prefix
/// match confirms the row belongs to this exact group (no hash, so no collision).
pub(super) fn apply_agg_from_value_index(
    avi_cursor: &mut ReadCursor,
    group_key: &[u8],
    for_max: bool,
    acc: &mut Accumulator,
) -> bool {
    use super::super::util::AVI_AV_BYTES;
    if avi_cursor.seek_first_positive_with_prefix(group_key) {
        let k = avi_cursor.current_pk_bytes();
        // current_pk_bytes() is the full AVI PK region; make_avi_schema lays it
        // out as `group ‖ ordinal ‖ av_encoded`, so the trailing av bytes are
        // always in bounds for a seek prefix `group_key` of length
        // `group_stride + 1` (group plus the ordinal byte).
        debug_assert_eq!(
            k.len(),
            group_key.len() + AVI_AV_BYTES,
            "AVI key = seek prefix (group ‖ ordinal) + AVI_AV_BYTES",
        );
        let av_start = group_key.len();
        let av = u64::from_be_bytes(k[av_start..av_start + AVI_AV_BYTES].try_into().unwrap());
        // The AVI stores `encode_ordered(v, for_max)`; the accumulator holds the
        // MIN-oriented encoding, so undo the MAX inversion (no value decode — that
        // happens once, at emit, in `get_value_bits`).
        acc.seed_from_raw_bits(if for_max { !av } else { av });
        return true;
    }
    acc.reset();
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Batch;

    // Item 5: a nullable single group column must NOT be promoted to the
    // natural PK; the PK region has no null bitmap.
    #[test]
    fn nullable_group_col_is_not_natural_pk() {
        let nullable = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 1),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[1],
        );
        assert!(
            !is_single_col_natural_pk(&nullable, &[0]),
            "nullable U64 group column must not be a natural PK",
        );

        let non_nullable = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        assert!(
            is_single_col_natural_pk(&non_nullable, &[0]),
            "non-nullable U64 group column should be a natural PK",
        );
    }

    /// Single-row batch with a U64 PK and one F64 payload column carrying `val`.
    fn f64_batch(val: f64) -> Batch {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0],
        );
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
        b
    }

    fn f64_acc(agg_op: AggOp) -> Accumulator {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0],
        );
        let desc = AggDescriptor {
            col_idx: 1,
            agg_op,
            col_type_code: TypeCode::F64,
            _pad: [0; 2],
        };
        Accumulator::new(&desc, &schema)
    }

    // Item 19: a NaN seen first must not poison MIN. A subsequent finite value
    // is smaller under the total order and must replace it.
    #[test]
    fn min_nan_first_does_not_poison() {
        let nan = f64_batch(f64::NAN);
        let finite = f64_batch(5.0);
        let mut acc = f64_acc(AggOp::Min);
        acc.step_from_batch(&nan.as_mem_batch(), 0, 1);
        acc.step_from_batch(&finite.as_mem_batch(), 0, 1);
        let got = f64::from_bits(acc.get_value_bits());
        assert_eq!(got, 5.0, "finite value must displace a leading NaN in MIN");
    }

    // Item 19 mirror: MAX must use the total order consistently. Under
    // total_cmp a quiet (positive) NaN is the greatest value, so once seen it
    // is retained as the max regardless of arrival order.
    #[test]
    fn max_uses_total_order_for_nan() {
        let finite = f64_batch(5.0);
        let nan = f64_batch(f64::NAN);
        let mut acc = f64_acc(AggOp::Max);
        acc.step_from_batch(&finite.as_mem_batch(), 0, 1);
        acc.step_from_batch(&nan.as_mem_batch(), 0, 1);
        let got = f64::from_bits(acc.get_value_bits());
        assert!(got.is_nan(), "MAX must adopt NaN as the greatest under total order");
    }
}
