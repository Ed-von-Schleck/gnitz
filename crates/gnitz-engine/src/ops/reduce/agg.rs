//! Aggregate opcodes, descriptors, accumulator state, and AVI lookup.

use crate::schema::{SchemaDescriptor, TypeCode};
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
    Null         = 0,
    Count        = 1,
    Sum          = 2,
    Min          = 3,
    Max          = 4,
    CountNonNull = 5,
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
            other => Err(other),
        }
    }
}

impl From<gnitz_wire::AggFunc> for AggOp {
    fn from(f: gnitz_wire::AggFunc) -> Self {
        match f {
            gnitz_wire::AggFunc::Count        => AggOp::Count,
            gnitz_wire::AggFunc::Sum          => AggOp::Sum,
            gnitz_wire::AggFunc::Min          => AggOp::Min,
            gnitz_wire::AggFunc::Max          => AggOp::Max,
            gnitz_wire::AggFunc::CountNonNull => AggOp::CountNonNull,
        }
    }
}

impl AggOp {
    pub fn is_linear(self) -> bool {
        matches!(self, AggOp::Count | AggOp::Sum | AggOp::CountNonNull)
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
    /// Payload index when `!is_pk_col`; byte offset within the PK region
    /// when `is_pk_col`. The fields are disjoint by `is_pk_col`, so a
    /// single byte slot covers both.
    pi_or_pk_off: u8,
    col_size: u8,
    is_pk_col: bool,
    has_value: bool,
}

impl Accumulator {
    pub(super) fn new(desc: &AggDescriptor, schema: &SchemaDescriptor) -> Self {
        let col_idx = desc.col_idx as usize;
        let is_pk_col = schema.is_pk_col(col_idx);
        let tc = desc.col_type_code;
        let pi_or_pk_off = if is_pk_col {
            schema.pk_byte_offset(col_idx)
        } else {
            schema.payload_idx(col_idx) as u8
        };
        Accumulator {
            acc: 0,
            has_value: false,
            agg_op: desc.agg_op,
            col_type_code: tc,
            pi_or_pk_off,
            col_size: tc.stride(),
            is_pk_col,
        }
    }

    pub(super) fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    pub(super) fn is_linear(&self) -> bool {
        self.agg_op.is_linear()
    }

    pub(super) fn is_zero(&self) -> bool {
        !self.has_value
    }

    pub(super) fn get_value_bits(&self) -> u64 {
        self.acc as u64
    }

    pub(super) fn seed_from_raw_bits(&mut self, bits: u64) {
        self.acc = bits as i64;
        self.has_value = true;
    }

    fn is_float(&self) -> bool {
        self.col_type_code.is_float()
    }

    /// Step: incorporate one input row into the accumulator.
    pub(super) fn step_from_batch(
        &mut self,
        mb: &MemBatch,
        row: usize,
        weight: i64,
    ) {
        let tc = self.col_type_code;
        let cs = self.col_size as usize;

        // PK columns are never null, so the null check is gated on
        // !is_pk_col. COUNT skips the null check entirely because it
        // counts rows regardless of value-column nullness.
        let slot = self.pi_or_pk_off as usize;
        let bytes: &[u8] = if self.is_pk_col {
            &mb.get_pk_bytes(row)[slot..slot + cs]
        } else {
            if self.agg_op != AggOp::Count {
                let null_word = mb.get_null_word(row);
                if (null_word >> slot) & 1 != 0 {
                    return;
                }
            }
            mb.get_col_ptr(row, slot, cs)
        };

        let first = !self.has_value;
        self.has_value = true;
        let is_f = self.is_float();

        match self.agg_op {
            AggOp::Count => {
                self.acc = self.acc.wrapping_add(weight);
            }
            AggOp::CountNonNull => {
                self.acc = self.acc.wrapping_add(weight);
            }
            AggOp::Sum => {
                if is_f {
                    let val_f = decode_float(bytes, tc);
                    let cur_f = f64::from_bits(self.acc as u64);
                    let w_f = weight as f64;
                    self.acc = f64::to_bits(cur_f + val_f * w_f) as i64;
                } else {
                    let val = decode_signed(bytes, tc);
                    self.acc = self.acc.wrapping_add(val.wrapping_mul(weight));
                }
            }
            AggOp::Min => {
                if is_f {
                    let v = decode_float(bytes, tc);
                    if first || v < f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(v) as i64;
                    }
                } else {
                    let v = decode_signed(bytes, tc);
                    // U64 comparison must be unsigned: `decode_signed`
                    // returns the bit pattern verbatim, so high-bit-set
                    // values look negative under signed `<`.
                    let replaces = if tc == TypeCode::U64 {
                        (v as u64) < (self.acc as u64)
                    } else {
                        v < self.acc
                    };
                    if first || replaces {
                        self.acc = v;
                    }
                }
            }
            AggOp::Max => {
                if is_f {
                    let v = decode_float(bytes, tc);
                    if first || v > f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(v) as i64;
                    }
                } else {
                    let v = decode_signed(bytes, tc);
                    let replaces = if tc == TypeCode::U64 {
                        (v as u64) > (self.acc as u64)
                    } else {
                        v > self.acc
                    };
                    if first || replaces {
                        self.acc = v;
                    }
                }
            }
            AggOp::Null => {}
        }
    }

    /// Merge an already-accumulated value (from trace_out) into this accumulator.
    /// For linear aggregates (COUNT, SUM): applies weight arithmetic.
    /// For MIN/MAX with positive weight: delegates to combine().
    /// For MIN/MAX with negative weight: no-op (cannot un-MIN/un-MAX).
    pub(super) fn merge_accumulated(&mut self, value_bits: u64, weight: i64) {
        let is_f = self.is_float();
        match self.agg_op {
            AggOp::Count | AggOp::CountNonNull => {
                let prev = value_bits as i64;
                self.acc = self.acc.wrapping_add(prev.wrapping_mul(weight));
                self.has_value = true;
            }
            AggOp::Sum => {
                if is_f {
                    let prev_f = f64::from_bits(value_bits);
                    let w_f = weight as f64;
                    let cur_f = f64::from_bits(self.acc as u64);
                    self.acc = f64::to_bits(cur_f + prev_f * w_f) as i64;
                } else {
                    let prev = value_bits as i64;
                    self.acc = self.acc.wrapping_add(prev.wrapping_mul(weight));
                }
                self.has_value = true;
            }
            AggOp::Min | AggOp::Max => {
                if weight > 0 {
                    self.combine(value_bits);
                }
                // Negative weight: algebraically unsound for MIN/MAX — skip.
            }
            AggOp::Null => {}
        }
    }

    /// Combine a partial aggregate from another shard.
    pub(super) fn combine(&mut self, other_bits: u64) {
        let is_f = self.is_float();
        match self.agg_op {
            AggOp::Count | AggOp::CountNonNull => {
                let prev = other_bits as i64;
                self.acc = self.acc.wrapping_add(prev);
                self.has_value = true;
            }
            AggOp::Sum => {
                if is_f {
                    let prev_f = f64::from_bits(other_bits);
                    let cur_f = f64::from_bits(self.acc as u64);
                    self.acc = f64::to_bits(cur_f + prev_f) as i64;
                } else {
                    let prev = other_bits as i64;
                    self.acc = self.acc.wrapping_add(prev);
                }
                self.has_value = true;
            }
            AggOp::Min => {
                let first = !self.has_value;
                self.has_value = true;
                if is_f {
                    let other_f = f64::from_bits(other_bits);
                    if first || other_f < f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(other_f) as i64;
                    }
                } else {
                    let other_v = other_bits as i64;
                    let replaces = if self.col_type_code == TypeCode::U64 {
                        (other_v as u64) < (self.acc as u64)
                    } else {
                        other_v < self.acc
                    };
                    if first || replaces {
                        self.acc = other_v;
                    }
                }
            }
            AggOp::Max => {
                let first = !self.has_value;
                self.has_value = true;
                if is_f {
                    let other_f = f64::from_bits(other_bits);
                    if first || other_f > f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(other_f) as i64;
                    }
                } else {
                    let other_v = other_bits as i64;
                    let replaces = if self.col_type_code == TypeCode::U64 {
                        (other_v as u64) > (self.acc as u64)
                    } else {
                        other_v > self.acc
                    };
                    if first || replaces {
                        self.acc = other_v;
                    }
                }
            }
            AggOp::Null => {}
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
        TypeCode::I8  => bytes[0] as i8 as i64,
        TypeCode::U8  => bytes[0] as i64,
        TypeCode::I16 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        TypeCode::U16 => u16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        TypeCode::I32 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        TypeCode::U32 => u32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        TypeCode::I64 | TypeCode::U64 =>
            i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        TypeCode::String =>
            i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        TypeCode::F32 | TypeCode::F64 | TypeCode::U128 | TypeCode::UUID | TypeCode::Blob =>
            unreachable!("decode_signed: non-integer/string type"),
    }
}

/// Decode a column's bytes as f64, with proper F32→F64 promotion.
#[inline]
fn decode_float(bytes: &[u8], tc: TypeCode) -> f64 {
    match tc {
        TypeCode::F32 =>
            f32::from_bits(u32::from_le_bytes(bytes[..4].try_into().unwrap())) as f64,
        TypeCode::F64 =>
            f64::from_bits(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        TypeCode::U8 | TypeCode::I8 | TypeCode::U16 | TypeCode::I16 |
        TypeCode::U32 | TypeCode::I32 | TypeCode::U64 | TypeCode::I64 |
        TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob =>
            unreachable!("decode_float: non-float type"),
    }
}

/// True iff `group_cols` is a single column whose type permits using
/// the source column directly as the output PK (vs. a synthetic U128).
/// Shared between `compiler::build_reduce_output_schema` and
/// `op_reduce` to keep schema construction and execution in lockstep.
pub(crate) fn is_single_col_natural_pk(schema: &SchemaDescriptor, group_cols: &[u32]) -> bool {
    group_cols.len() == 1
        && matches!(
            TypeCode::from_validated_u8(schema.columns[group_cols[0] as usize].type_code),
            TypeCode::U64 | TypeCode::U128 | TypeCode::UUID,
        )
}

// ---------------------------------------------------------------------------
// AVI lookup
// ---------------------------------------------------------------------------

/// Seek the AVI cursor to a group via its full byte-form group key and apply
/// the decoded MIN/MAX to the accumulator.
///
/// The AVI PK is `group_key_bytes ++ av_encoded(8)`, ordered group-major then
/// aggregate-minor. The first positive-weight entry whose key starts with the
/// full group key is therefore the extremal value, and the prefix match
/// confirms the row belongs to this exact group (no hash, so no collision).
pub(super) fn apply_agg_from_value_index(
    avi_cursor: &mut ReadCursor,
    group_key: &[u8],
    for_max: bool,
    agg_col_type_code: TypeCode,
    acc: &mut Accumulator,
) -> bool {
    use super::super::util::AVI_AV_BYTES;
    if avi_cursor.seek_first_positive_with_prefix(group_key) {
        let k = avi_cursor.current_pk_bytes();
        // current_pk_bytes() is the full AVI PK region; make_avi_schema lays it
        // out as group_stride + AVI_AV_BYTES (av_encoded), so the trailing bytes
        // are always in bounds for a group_key of length group_stride.
        debug_assert_eq!(
            k.len(), group_key.len() + AVI_AV_BYTES,
            "AVI key = group_stride + AVI_AV_BYTES",
        );
        let av_start = group_key.len();
        let av = u64::from_le_bytes(k[av_start..av_start + AVI_AV_BYTES].try_into().unwrap());
        acc.seed_from_raw_bits(super::super::util::decode_ordered(av, agg_col_type_code, for_max));
        return true;
    }
    acc.reset();
    false
}
