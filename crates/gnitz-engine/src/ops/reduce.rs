//! Reduce operator: accumulator, group key, argsort, AVI, op_reduce, op_gather_reduce.

use std::cmp::Ordering;

use crate::schema::{SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{
    Batch, ConsolidatedBatch, DrainGuard, MemBatch, ReadCursor,
    scatter_copy, write_to_batch,
    compare_rows, compare_rows_int_nonnull, schema_is_int_nonnull,
};

/// `clear()` does not re-zero the data buffer, so the scatter variants used
/// here must be the unconditional-copy ones — a nullable-skip would leak stale
/// bytes through.
fn fill_cleared_batch(
    batch: &mut Batch,
    trace_cursor: Option<&ReadCursor>,
    trace_rows: &[(u32, u32, i64)],
    delta_mb: &MemBatch,
    delta_indices: &[u32],
) {
    let needed = trace_rows.len() + delta_indices.len();
    if needed == 0 {
        return;
    }
    batch.reserve_rows(needed);
    {
        let mut writer = batch.capacity_writer();
        if let Some(cursor) = trace_cursor {
            cursor.scatter_drained_into(trace_rows, &mut writer);
        }
        // Delta rows arrive already filtered to non-zero weights, so the
        // empty `weights` arg routes through `scatter_col_first`.
        scatter_copy(delta_mb, delta_indices, &[], &mut writer);
    }
    batch.count = needed;
}

use super::util::{
    write_string_from_batch,
    extract_group_key, extract_gc_u64, cmp_typed_le,
};

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
struct Accumulator {
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
    fn new(desc: &AggDescriptor, schema: &SchemaDescriptor) -> Self {
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

    fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    fn is_linear(&self) -> bool {
        self.agg_op.is_linear()
    }

    fn is_zero(&self) -> bool {
        !self.has_value
    }

    fn get_value_bits(&self) -> u64 {
        self.acc as u64
    }

    fn seed_from_raw_bits(&mut self, bits: u64) {
        self.acc = bits as i64;
        self.has_value = true;
    }

    fn is_float(&self) -> bool {
        self.col_type_code.is_float()
    }

    /// Step: incorporate one input row into the accumulator.
    fn step_from_batch(
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
                    if first || v < self.acc {
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
                    if first || v > self.acc {
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
    fn merge_accumulated(&mut self, value_bits: u64, weight: i64) {
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
    fn combine(&mut self, other_bits: u64) {
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
                    if first || other_v < self.acc {
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
                    if first || other_v > self.acc {
                        self.acc = other_v;
                    }
                }
            }
            AggOp::Null => {}
        }
    }
}

/// Decode a column's bytes as a sign-extended i64. STRING columns use the
/// first 8 bytes of the 16-byte German String struct as the compare key
/// (caller passes the full 16-byte slice for the row).
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

/// Write the row's PK to `output`. For arity-1 PKs the synthetic u128
/// `group_key` is sufficient; compound PKs must copy the source row's
/// PK bytes verbatim because the u128 cannot carry a >16-byte PK region
/// nor reorder columns to match the output's pk_indices layout.
#[inline]
fn emit_pk(
    output: &mut Batch,
    output_schema: &SchemaDescriptor,
    src_pk_bytes: &[u8],
    group_key: u128,
) {
    if output_schema.pk_indices().len() > 1 {
        output.extend_pk_bytes(src_pk_bytes);
    } else {
        output.extend_pk(group_key);
    }
}

// ---------------------------------------------------------------------------
// Group key extraction
// ---------------------------------------------------------------------------

/// Reverse IEEE order-preserving encoding.
fn ieee_order_bits_reverse(encoded: u64) -> u64 {
    if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    }
}

/// IEEE 754 order-preserving encoding for 32-bit floats, returning u64.
/// Checks the F32 sign bit (bit 31), not bit 63.
#[cfg(test)]
fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 { !raw_bits } else { raw_bits ^ (1u32 << 31) }) as u64
}

/// Reverse of ieee_order_bits_f32.
fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 { e ^ (1u32 << 31) } else { !e }
}

// ---------------------------------------------------------------------------
// Argsort
// ---------------------------------------------------------------------------

/// Pre-computed per-column sort descriptor to avoid repeated schema lookups.
///
/// Five bytes per entry, no padding (all fields are u8-sized).
/// A stack array of MAX_COLUMNS = 65 entries is 325 bytes — well within five cache lines.
#[derive(Clone, Copy)]
struct SortDesc {
    pi: u8,
    cs: u8,
    tc: TypeCode,
    c_idx: u8,
    /// Byte offset of the addressed PK column within the row's PK region.
    /// Meaningful only when `pi == PAYLOAD_MAPPING_PK_SENTINEL`; zero otherwise.
    pk_off: u8,
}

/// Compare two rows by group columns using pre-computed SortDesc array.
fn compare_by_group_cols(
    mb: &MemBatch,
    row_a: usize,
    row_b: usize,
    descs: &[SortDesc],
) -> std::cmp::Ordering {
    let a_null_word = mb.get_null_word(row_a);
    let b_null_word = mb.get_null_word(row_b);

    for desc in descs {
        if desc.pi == PAYLOAD_MAPPING_PK_SENTINEL {
            // Isolate the addressed PK column's byte window. Comparing the
            // whole PK region (the previous `mb.get_pk(row)` widen) splits
            // compound-PK groups that share the addressed column but differ
            // in other PK columns.
            let off = desc.pk_off as usize;
            let cs = desc.cs as usize;
            let a = &mb.get_pk_bytes(row_a)[off..off + cs];
            let b = &mb.get_pk_bytes(row_b)[off..off + cs];
            let ord = cmp_typed_le(a, b, desc.tc);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
            continue;
        }

        let pi = desc.pi as usize;

        // NULL is never set on non-nullable columns, so the bit is always 0
        // there and this branch is harmless. NULLs sort before non-NULLs
        // (NULLS FIRST), so all NULLs are adjacent and form a single group.
        let a_is_null = (a_null_word >> pi) & 1 != 0;
        let b_is_null = (b_null_word >> pi) & 1 != 0;
        match (a_is_null, b_is_null) {
            (true, true) => continue,
            (true, false) => return std::cmp::Ordering::Less,
            (false, true) => return std::cmp::Ordering::Greater,
            (false, false) => {}
        }

        let ord = if desc.tc == TypeCode::String {
            let a_bytes = mb.get_col_ptr(row_a, pi, 16);
            let b_bytes = mb.get_col_ptr(row_b, pi, 16);
            crate::schema::compare_german_strings(a_bytes, mb.blob, b_bytes, mb.blob)
        } else if desc.tc == TypeCode::Blob {
            unreachable!("BLOB columns are not valid group-by keys")
        } else {
            let cs = desc.cs as usize;
            let a = mb.get_col_ptr(row_a, pi, cs);
            let b = mb.get_col_ptr(row_b, pi, cs);
            cmp_typed_le(a, b, desc.tc)
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Build the SortDesc array for a given schema and group_by_cols slice.
/// Returns (array, length); only `&array[..length]` is valid.
fn build_sort_descs(
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> ([SortDesc; crate::schema::MAX_COLUMNS], usize) {
    let mut arr = [SortDesc {
        pi: 0, cs: 0, tc: TypeCode::U64, c_idx: 0, pk_off: 0,
    }; crate::schema::MAX_COLUMNS];
    for (i, &c_idx_u32) in group_by_cols.iter().enumerate() {
        let c_idx = c_idx_u32 as usize;
        let tc = TypeCode::from_validated_u8(schema.columns[c_idx].type_code);
        let pi = schema.payload_mapping_byte(c_idx);
        let pk_off = if pi == PAYLOAD_MAPPING_PK_SENTINEL {
            schema.pk_byte_offset(c_idx)
        } else {
            0
        };
        arr[i] = SortDesc { pi, cs: tc.stride(), tc, c_idx: c_idx as u8, pk_off };
    }
    (arr, group_by_cols.len())
}

/// Argsort delta batch by group columns.
fn argsort_delta(
    batch: &Batch,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> Vec<u32> {
    let mb = batch.as_mem_batch();
    let n = batch.count;
    if n <= 1 {
        return (0..n as u32).collect();
    }

    // packed_sort fast path: non-nullable single non-PK col of a packable
    // int type. Reads via `payload_idx` (PK-invalid) and stores keys in a
    // dense Vec so the comparator skips the per-call TypeCode dispatch.
    // NULL stores as zero bytes — would interleave with integer 0, so
    // nullable falls through to compare_by_group_cols below.
    if group_by_cols.len() == 1
        && !schema.is_pk_col(group_by_cols[0] as usize)
        && schema.columns[group_by_cols[0] as usize].nullable == 0
    {
        let ci = group_by_cols[0] as usize;
        let tc = TypeCode::from_validated_u8(schema.columns[ci].type_code);
        let pi = schema.payload_idx(ci);
        macro_rules! packed_sort {
            ($T:ty, $stride:expr) => {{
                let keys: Vec<$T> = (0..n)
                    .map(|i| {
                        let ptr = mb.get_col_ptr(i, pi, $stride);
                        <$T>::from_le_bytes(ptr.try_into().unwrap())
                    })
                    .collect();
                let mut indices: Vec<u32> = (0..n as u32).collect();
                indices.sort_unstable_by_key(|&i| keys[i as usize]);
                return indices;
            }};
        }
        match tc {
            TypeCode::I64 => packed_sort!(i64, 8),
            TypeCode::U64 => packed_sort!(u64, 8),
            TypeCode::I32 => packed_sort!(i32, 4),
            TypeCode::U32 => packed_sort!(u32, 4),
            _ => {}
        }
    }

    let mut indices: Vec<u32> = (0..n as u32).collect();
    let (sort_descs, len) = build_sort_descs(schema, group_by_cols);
    let descs = &sort_descs[..len];
    indices.sort_unstable_by(|&a, &b| {
        compare_by_group_cols(&mb, a as usize, b as usize, descs)
    });
    indices
}

/// Sort batch by (PK, payload) without consolidation.
/// Used by op_gather_reduce where we need to see each partial separately.
fn sort_owned(batch: &Batch, schema: &SchemaDescriptor) -> Batch {
    let n = batch.count;
    if n <= 1 || batch.sorted {
        return batch.clone_batch();
    }

    let mb = batch.as_mem_batch();

    let pks: Vec<u128> = (0..n).map(|i| mb.get_pk(i)).collect();
    let mut indices: Vec<u32> = (0..n as u32).collect();

    // Standard-library sort is extremely sensitive to comparator inlining;
    // the fast-path branch is hoisted out of the per-comparison closure.
    if schema_is_int_nonnull(schema) {
        indices.sort_unstable_by(|&a, &b| match pks[a as usize].cmp(&pks[b as usize]) {
            Ordering::Equal => compare_rows_int_nonnull(schema, &mb, a as usize, &mb, b as usize),
            ord => ord,
        });
    } else {
        indices.sort_unstable_by(|&a, &b| match pks[a as usize].cmp(&pks[b as usize]) {
            Ordering::Equal => compare_rows(schema, &mb, a as usize, &mb, b as usize),
            ord => ord,
        });
    }

    let blob_cap = mb.blob.len().max(1);
    let mut output = write_to_batch(schema, n, blob_cap, |writer| {
        scatter_copy(&mb, &indices, &[], writer);
    });
    output.sorted = true;
    output
}

// ---------------------------------------------------------------------------
// AVI lookup
// ---------------------------------------------------------------------------

/// Seek AVI cursor to group, apply decoded min/max to accumulator.
fn apply_agg_from_value_index(
    avi_cursor: &mut ReadCursor,
    gc_u64: u64,
    for_max: bool,
    agg_col_type_code: TypeCode,
    acc: &mut Accumulator,
) -> bool {
    avi_cursor.seek(crate::util::make_pk(0, gc_u64));
    gnitz_debug!("avi_lookup: seek(0, {:#x}) valid={}", gc_u64, avi_cursor.valid);
    while avi_cursor.valid {
        let k_hi = (avi_cursor.current_key >> 64) as u64;
        let k_lo = avi_cursor.current_key as u64;
        let w = avi_cursor.current_weight;
        gnitz_debug!("avi_lookup: at pk_lo={:#x} pk_hi={:#x} w={}", k_lo, k_hi, w);
        if k_hi != gc_u64 {
            break;
        }
        if avi_cursor.current_weight > 0 {
            let mut encoded = avi_cursor.current_key as u64;
            if for_max {
                encoded = !encoded;
            }
            match agg_col_type_code {
                TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
                    encoded = (encoded as i64).wrapping_sub(1i64 << 63) as u64;
                }
                TypeCode::F64 => {
                    encoded = ieee_order_bits_reverse(encoded);
                }
                TypeCode::F32 => {
                    encoded = ieee_order_bits_f32_reverse(encoded) as u64;
                }
                TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 |
                TypeCode::U128 | TypeCode::UUID | TypeCode::String => {}
                TypeCode::Blob => unreachable!("BLOB columns are not valid aggregate inputs"),
            }
            acc.seed_from_raw_bits(encoded);
            return true;
        }
        avi_cursor.advance();
    }
    acc.reset();
    false
}

// ---------------------------------------------------------------------------
// op_reduce
// ---------------------------------------------------------------------------

/// Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
#[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
pub fn op_reduce(
    delta: &Batch,
    trace_in_cursor: Option<&mut ReadCursor>,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    avi_for_max: bool,
    avi_agg_col_type_code: TypeCode,
    avi_group_by_cols: &[u32],
    avi_input_schema: Option<&SchemaDescriptor>,
    gi_cursor: Option<&mut ReadCursor>,
    gi_col_idx: u32,
    _gi_col_type_code: TypeCode,
    finalize_prog: Option<&crate::expr::ExprProgram>,
    finalize_out_schema: Option<&SchemaDescriptor>,
) -> (Batch, Option<Batch>) {
    let num_aggs = agg_descs.len();
    let num_out_cols = output_schema.num_columns();

    // Linearity check
    let all_linear = agg_descs.iter().all(|d| d.agg_op.is_linear());

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    // Fast path (linear or already consolidated): borrow delta directly — no allocation.
    let cs = if all_linear { None } else { Batch::consolidate_if_needed(delta, input_schema) };
    let working: &Batch = cs.as_deref().unwrap_or(delta);

    let n = working.count;
    if n == 0 {
        let empty_fin = finalize_out_schema.map(Batch::empty_with_schema);
        return (Batch::empty_with_schema(output_schema), empty_fin);
    }

    // group_set_eq_pk: GROUP BY is a permutation of the source PK columns.
    // group_by_pk additionally requires stride ∈ {8, 16} because the fast
    // path widens PK regions via widen_pk_le on each row; wider compound
    // PKs cannot use the u128 comparator and must take the slow path.
    // Skipping the sort under group_by_pk is load-bearing for output
    // ordering — the input is already PK-sorted from consolidation, and
    // the output's pk_indices is in source pk-list order regardless of
    // group_by_cols permutation.
    let group_set_eq_pk = input_schema.group_cols_eq_pk(group_by_cols);
    let group_by_pk = group_set_eq_pk
        && matches!(input_schema.pk_stride(), 8 | 16);

    // Pre-compute sort descriptors for group comparisons (non-pk path).
    let (sort_descs, sort_descs_len) = if group_by_pk {
        ([SortDesc { pi: 0, cs: 0, tc: TypeCode::U64, c_idx: 0, pk_off: 0 };
          crate::schema::MAX_COLUMNS], 0)
    } else {
        build_sort_descs(input_schema, group_by_cols)
    };
    let group_descs = &sort_descs[..sort_descs_len];

    // Argsort
    let sorted_indices = if group_by_pk {
        (0..n as u32).collect()
    } else {
        argsort_delta(working, input_schema, group_by_cols)
    };

    let mb = working.as_mem_batch();

    // Output mapping: matches build_reduce_output_schema's logic — must
    // stay in sync. Independent of the stride-gated fast-path eligibility
    // above (compound natural-PK at stride > 16 still emits natural PKs).
    let use_natural_pk = group_set_eq_pk
        || is_single_col_natural_pk(input_schema, group_by_cols);

    let mut raw_output = Batch::with_schema(*output_schema, 32);
    let mut fin_output = finalize_out_schema.map(|fs| {
        Batch::with_schema(*fs, 32)
    });
    // One FinalizeContext per finalize program: hoists EvalScratch sizing,
    // out_cols classification, EMIT→register map, and the no_nulls flag out
    // of the (potentially 100k+ iteration) group loop.
    let mut fin_ctx = finalize_prog.map(|p| crate::expr::FinalizeContext::new(p, output_schema));

    let mut accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, input_schema)).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    // We need mutable access to optional cursors. Take ownership via Option::take pattern.
    // The caller passes these as Option<&mut ReadCursor>, but we need to use them
    // multiple times in the loop. They're already &mut so we can use them directly.
    let mut trace_in = trace_in_cursor;
    let mut avi = avi_cursor;
    let mut gi = gi_cursor;

    // Hoist replay batch outside the group loop: reuse the allocation across groups
    // rather than allocating and dropping once per group (can be 100k+ times per epoch).
    let mut replay = (!all_linear && avi.is_none())
        .then(|| Batch::with_schema(*input_schema, 32));

    let mut idx = 0usize;
    let mut num_groups = 0usize;
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = sorted_indices[group_start_pos] as usize;

        let group_key: u128 = if group_by_pk {
            mb.get_pk(group_start_idx)
        } else {
            extract_group_key(&mb, group_start_idx, input_schema, group_by_cols)
        };

        // Step linear accumulators over delta rows in this group
        for acc in accs.iter_mut() {
            acc.reset();
        }
        while idx < n {
            let curr_idx = sorted_indices[idx] as usize;
            if group_by_pk {
                let curr_pk = mb.get_pk(curr_idx);
                if curr_pk != group_key {
                    break;
                }
            } else {
                if compare_by_group_cols(&mb, curr_idx, group_start_idx, group_descs)
                    != std::cmp::Ordering::Equal
                {
                    break;
                }
            }

            let w = mb.get_weight(curr_idx);
            for acc in accs.iter_mut() {
                if acc.is_linear() {
                    acc.step_from_batch(&mb, curr_idx, w);
                }
            }
            idx += 1;
        }

        // Retraction: read old value from trace_out
        trace_out_cursor.seek(group_key);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key == group_key;

        if has_old {
            // Read old agg values from trace_out
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let ptr = trace_out_cursor.col_ptr(agg_col_idx, 8);
                if !ptr.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, 8) };
                    old_vals[k] = u64::from_le_bytes(bytes.try_into().unwrap());
                } else {
                    old_vals[k] = 0;
                }
            }
            // Emit retraction row (weight=-1)
            emit_reduce_row(
                &mut raw_output, &mb, group_start_idx,
                group_key, -1,
                &old_vals, true, // use_old_val=true
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out), Some(ctx)) =
                (finalize_prog, finalize_out_schema, &mut fin_output, fin_ctx.as_mut())
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key, -1,
                    prog, output_schema, fin_schema, ctx,
                );
            }
        }

        // New value calculation
        if all_linear && has_old {
            for k in 0..num_aggs {
                accs[k].merge_accumulated(old_vals[k], 1);
            }
        } else if !all_linear {
            if let Some(ref mut avi_c) = avi {
                // AVI path
                let avi_schema = avi_input_schema.unwrap_or(input_schema);
                let gc_u64 = extract_gc_u64(&mb, group_start_idx, avi_schema, avi_group_by_cols);
                let found = apply_agg_from_value_index(
                    avi_c, gc_u64, avi_for_max, avi_agg_col_type_code,
                    &mut accs[0],
                );
                gnitz_debug!("reduce: AVI lookup gc={:#x} found={}", gc_u64, found);
            } else {
                let replay = replay.as_mut().unwrap();
                replay.clear();

                let delta_indices: &[u32] = &sorted_indices[group_start_pos..idx];

                let mut trace_rows = DrainGuard::new();
                trace_rows.clear();

                if let Some(ti_cursor) = trace_in.as_deref_mut() {
                    if group_by_pk {
                        ti_cursor.seek(group_key);
                        while ti_cursor.valid
                            && ti_cursor.current_key == group_key
                        {
                            ti_cursor.push_current_row(&mut trace_rows);
                            ti_cursor.advance();
                        }
                    } else if let Some(gi_c) = gi.as_deref_mut() {
                        let gi_ci = gi_col_idx as usize;
                        let gc_u64_val = if input_schema.is_pk_col(gi_ci) {
                            mb.get_pk(group_start_idx) as u64
                        } else {
                            let cs = input_schema.columns[gi_ci].size() as usize;
                            let pi = input_schema.payload_idx(gi_ci);
                            let ptr = mb.get_col_ptr(group_start_idx, pi, cs);
                            let mut buf = [0u8; 8];
                            buf[..cs].copy_from_slice(ptr);
                            u64::from_le_bytes(buf)
                        };
                        gi_c.seek(crate::util::make_pk(0, gc_u64_val));
                        while gi_c.valid {
                            let gk_hi = (gi_c.current_key >> 64) as u64;
                            if gk_hi != gc_u64_val {
                                break;
                            }
                            if gi_c.current_weight > 0 {
                                let spk_lo = gi_c.current_key as u64;
                                // spk_hi is in payload col 1 (first payload col = col index 1)
                                let spk_hi_ptr = gi_c.col_ptr(1, 8);
                                let spk_hi = if !spk_hi_ptr.is_null() {
                                    let bytes = unsafe { std::slice::from_raw_parts(spk_hi_ptr, 8) };
                                    u64::from_le_bytes(bytes.try_into().unwrap())
                                } else {
                                    0
                                };
                                let trace_key = crate::util::make_pk(spk_lo, spk_hi);
                                ti_cursor.seek(trace_key);
                                while ti_cursor.valid && ti_cursor.current_key == trace_key {
                                    ti_cursor.push_current_row(&mut trace_rows);
                                    ti_cursor.advance();
                                }
                            }
                            gi_c.advance();
                        }
                    } else {
                        // Fallback: full trace scan, predicate-filtered.
                        ti_cursor.seek(0u128);
                        let ti_mb_exemplar_row = group_start_idx;
                        while ti_cursor.valid {
                            if cursor_matches_group(
                                ti_cursor, &mb, ti_mb_exemplar_row,
                                group_descs,
                            ) {
                                ti_cursor.push_current_row(&mut trace_rows);
                            }
                            ti_cursor.advance();
                        }
                    }
                }

                fill_cleared_batch(
                    replay, trace_in.as_deref(), &trace_rows, &mb, delta_indices,
                );

                // Consolidate replay and step all accumulators (borrow replay; don't consume it)
                let merged_cs = Batch::consolidate_if_needed(replay, input_schema);
                let merged: &Batch = merged_cs.as_deref().unwrap_or(&*replay);
                for acc in accs.iter_mut() {
                    acc.reset();
                }
                let merged_mb = merged.as_mem_batch();
                for m in 0..merged.count {
                    let w = merged_mb.get_weight(m);
                    if w > 0 {
                        for acc in accs.iter_mut() {
                            acc.step_from_batch(&merged_mb, m, w);
                        }
                    }
                }
            }
        }

        // Emission: +1 for new value
        let any_nonzero = accs.iter().any(|a| !a.is_zero());
        if any_nonzero {
            emit_reduce_row(
                &mut raw_output, &mb, group_start_idx,
                group_key, 1,
                &old_vals, false, // use_old_val=false
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out), Some(ctx)) =
                (finalize_prog, finalize_out_schema, &mut fin_output, fin_ctx.as_mut())
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key, 1,
                    prog, output_schema, fin_schema, ctx,
                );
            }
        }

        num_groups += 1;
    }

    gnitz_debug!(
        "op_reduce: in={} groups={} out={} fin={}",
        n, num_groups, raw_output.count,
        fin_output.as_ref().map_or(0, |b| b.count)
    );

    if group_by_pk {
        // PK-grouped output: one row per unique PK, sorted and weight-folded.
        raw_output.sorted = true;
        raw_output.consolidated = true;
        debug_assert!(ConsolidatedBatch::from_batch_ref(&raw_output).is_some());
    }
    (raw_output, fin_output)
}

/// Emit one reduce output row.
#[allow(clippy::too_many_arguments)]
fn emit_reduce_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key: u128,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    use_natural_pk: bool,
    num_aggs: usize,
) {
    let num_out_cols = output_schema.num_columns();

    emit_pk(output, output_schema, input_mb.get_pk_bytes(exemplar_row), group_key);
    output.extend_weight(&weight.to_le_bytes());

    // Build null word and payload columns
    let mut null_word: u64 = 0;

    for (out_pi, ci, col) in output_schema.payload_columns() {
        let cs = col.size() as usize;

        // Determine if this is an agg column or a group exemplar column
        let agg_base = num_out_cols - num_aggs;
        if ci >= agg_base {
            // Aggregate column
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            // Null if accumulator is zero (no value) and not using old val
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else {
                output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);
            }
        } else if use_natural_pk {
            // use_natural_pk: no group exemplar columns in output (PK IS the group)
            // This shouldn't happen — with use_natural_pk, non-agg non-PK cols don't exist
            output.fill_col_zero(out_pi, cs);
        } else {
            // Group exemplar column: ci=1..N maps to group_by_cols[ci-1]
            let grp_idx = ci - 1; // skip PK at 0
            if grp_idx < group_by_cols.len() {
                let src_ci = group_by_cols[grp_idx] as usize;
                if input_schema.is_pk_col(src_ci) {
                    // PK is never null and lives outside the payload region.
                    let pk = input_mb.get_pk(exemplar_row);
                    output.extend_col(out_pi, &pk.to_le_bytes()[..cs]);
                } else {
                    let src_pi = input_schema.payload_idx(src_ci);
                    // Check null from input
                    let in_null = input_mb.get_null_word(exemplar_row);
                    if (in_null >> src_pi) & 1 != 0 {
                        null_word |= 1u64 << out_pi;
                        output.fill_col_zero(out_pi, cs);
                    } else if col.type_code == TypeCode::String as u8 {
                        write_string_from_batch(
                            output, out_pi,
                            input_mb, exemplar_row, src_pi,
                        );
                    } else {
                        let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                        output.extend_col(out_pi, src);
                    }
                }
            } else {
                output.fill_col_zero(out_pi, cs);
            }
        }
    }

    output.extend_null_bmp(&null_word.to_le_bytes());
    output.count += 1;
}

/// Emit a finalized row by evaluating the finalize ExprProgram on the raw output.
///
/// Handles COPY_COL (copy column from raw→finalized), EMIT (computed value),
/// and EMIT_NULL (null column) instructions. Per-row state (scratch register
/// file, classification, EMIT→register map, no_nulls) lives in `ctx`, which
/// the caller builds once before the group loop.
#[allow(clippy::too_many_arguments)]
fn emit_finalized_row(
    fin_output: &mut Batch,
    raw_output: &Batch,
    raw_row: usize,
    group_key: u128,
    weight: i64,
    prog: &crate::expr::ExprProgram,
    raw_schema: &SchemaDescriptor,
    fin_schema: &SchemaDescriptor,
    ctx: &mut crate::expr::FinalizeContext,
) {
    use crate::expr::OutputColKind;

    let raw_mb = raw_output.as_mem_batch();
    let null_word = raw_mb.get_null_word(raw_row);

    ctx.eval_row(prog, &raw_mb, raw_row);
    let out_cols = ctx.out_cols();

    emit_pk(fin_output, fin_schema, raw_mb.get_pk_bytes(raw_row), group_key);
    fin_output.extend_weight(&weight.to_le_bytes());

    let mut fin_null_mask: u64 = 0;

    for (fpi, _ci, col) in fin_schema.payload_columns() {
        let cs = col.size() as usize;

        if fpi < out_cols.len() {
            match out_cols[fpi] {
                OutputColKind::CopyCol(src_pi_byte) => {
                    // After resolve_column_indices, COPY_COL.a1 carries the
                    // resolved payload byte: SENTINEL for the PK column,
                    // otherwise the dense payload index.
                    if src_pi_byte == PAYLOAD_MAPPING_PK_SENTINEL as usize {
                        // Source is the PK column — slice from the full 16-byte
                        // little-endian PK so 8- and 16-byte column sizes both work.
                        let pk = raw_mb.get_pk(raw_row);
                        fin_output.extend_col(fpi, &pk.to_le_bytes()[..cs]);
                    } else {
                        let src_pi = src_pi_byte;
                        let src_ci = raw_schema.payload_col_idx(src_pi);
                        if (null_word >> src_pi) & 1 != 0 {
                            fin_null_mask |= 1u64 << fpi;
                            fin_output.fill_col_zero(fpi, cs);
                        } else if raw_schema.columns[src_ci].type_code == TypeCode::String as u8 {
                            write_string_from_batch(
                                fin_output, fpi,
                                &raw_mb, raw_row, src_pi,
                            );
                        } else {
                            let src = raw_mb.get_col_ptr(raw_row, src_pi, cs);
                            fin_output.extend_col(fpi, src);
                        }
                    }
                }
                OutputColKind::Emit(eidx) => {
                    let (val, is_null) = ctx.read_emit(eidx);
                    if is_null {
                        fin_null_mask |= 1u64 << fpi;
                        fin_output.fill_col_zero(fpi, cs);
                    } else {
                        fin_output.extend_col(fpi, &val.to_le_bytes()[..cs]);
                    }
                }
                OutputColKind::EmitNull => {
                    fin_null_mask |= 1u64 << fpi;
                    fin_output.fill_col_zero(fpi, cs);
                }
            }
        } else {
            fin_output.fill_col_zero(fpi, cs);
        }
    }

    fin_output.extend_null_bmp(&fin_null_mask.to_le_bytes());
    fin_output.count += 1;
}


/// Check if a cursor's current row matches the group columns of an exemplar row.
fn cursor_matches_group(
    cursor: &ReadCursor,
    exemplar_mb: &MemBatch,
    exemplar_row: usize,
    descs: &[SortDesc],
) -> bool {
    let cursor_null_word = cursor.current_null_word;
    let exemplar_null_word = exemplar_mb.get_null_word(exemplar_row);

    for desc in descs {
        if desc.pi == PAYLOAD_MAPPING_PK_SENTINEL {
            // Raw byte equality suffices here (no order, no signed/float
            // dispatch): rows with identical bytes at this PK-column window
            // are equal regardless of type.
            let off = desc.pk_off as usize;
            let cs = desc.cs as usize;
            let cursor_bytes = &cursor.current_pk_bytes()[off..off + cs];
            let exemplar_bytes =
                &exemplar_mb.get_pk_bytes(exemplar_row)[off..off + cs];
            if cursor_bytes != exemplar_bytes {
                return false;
            }
            continue;
        }

        let pi = desc.pi as usize;
        let cs = desc.cs as usize;

        let cursor_is_null = (cursor_null_word >> pi) & 1 != 0;
        let exemplar_is_null = (exemplar_null_word >> pi) & 1 != 0;
        if cursor_is_null != exemplar_is_null {
            return false;
        }
        if cursor_is_null {
            continue;
        }

        let cursor_ptr = cursor.col_ptr(desc.c_idx as usize, cs);
        if cursor_ptr.is_null() {
            return false;
        }
        let cursor_bytes = unsafe { std::slice::from_raw_parts(cursor_ptr, cs) };
        let exemplar_bytes = exemplar_mb.get_col_ptr(exemplar_row, pi, cs);

        if desc.tc == TypeCode::String {
            let cursor_blob = cursor.blob_ptr();
            let cmp = crate::schema::compare_german_strings(
                cursor_bytes, if cursor_blob.is_null() { &[] } else { unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) } },
                exemplar_bytes, exemplar_mb.blob,
            );
            if cmp != std::cmp::Ordering::Equal {
                return false;
            }
        } else if cursor_bytes != exemplar_bytes {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// op_gather_reduce
// ---------------------------------------------------------------------------

/// Gather-reduce: merge partial aggregate deltas from workers.
#[allow(clippy::needless_range_loop)]
pub fn op_gather_reduce(
    partial_batch: &Batch,
    trace_out_cursor: &mut ReadCursor,
    partial_schema: &SchemaDescriptor,
    agg_descs: &[AggDescriptor],
) -> Batch {
    let num_aggs = agg_descs.len();
    let num_out_cols = partial_schema.num_columns();

    // Sort without consolidation
    let sorted = sort_owned(partial_batch, partial_schema);
    let n = sorted.count;
    if n == 0 {
        return Batch::empty_with_schema(partial_schema);
    }

    let smb = sorted.as_mem_batch();

    // Derive group_indices layout
    let num_group_cols = num_out_cols - 1 - num_aggs; // -1 for PK
    let _use_natural_pk_gather = num_group_cols == 0;

    let mut output = Batch::with_schema(*partial_schema, n);
    let mut accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, partial_schema)).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    let mut idx = 0usize;
    while idx < n {
        let exemplar_row = idx;
        // Borrows of `smb` are all immutable here — `get_pk_bytes` returns a
        // slice into `smb.data` and coexists freely with `get_pk_bytes(idx)`
        // and `get_col_ptr(idx, ..)` inside the inner loop.
        let group_pk_bytes = smb.get_pk_bytes(idx);

        for acc in accs.iter_mut() {
            acc.reset();
        }

        // Accumulate all partial deltas for this group
        while idx < n && smb.get_pk_bytes(idx) == group_pk_bytes
        {
            let w = smb.get_weight(idx);
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let pi = agg_col_idx - 1; // -1 for PK (pk_index=0 always in output schema)
                let ptr = smb.get_col_ptr(idx, pi, 8);
                let bits = u64::from_le_bytes(ptr.try_into().unwrap());
                if w > 0 {
                    accs[k].combine(bits);
                } else if w < 0 && accs[k].is_linear() {
                    accs[k].merge_accumulated(bits, -1);
                }
            }
            idx += 1;
        }

        // `trace_out_cursor.seek` still takes a u128 — wide-PK cursor lift
        // is separate work. `get_pk` panics for pk_stride ∉ {8, 16}; today's
        // call sites can only reach this with stride 8 or 16.
        let group_pk: u128 = smb.get_pk(exemplar_row);

        // Read old global from trace_out
        trace_out_cursor.seek(group_pk);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key == group_pk;

        if has_old {
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let ptr = trace_out_cursor.col_ptr(agg_col_idx, 8);
                if !ptr.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, 8) };
                    old_vals[k] = u64::from_le_bytes(bytes.try_into().unwrap());
                } else {
                    old_vals[k] = 0;
                }
            }

            // Emit retraction
            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_pk, -1,
                &old_vals, true,
                &accs, partial_schema, num_aggs,
            );

            // Fold old global into accumulators
            for k in 0..num_aggs {
                accs[k].merge_accumulated(old_vals[k], 1);
            }

        }

        // Emit new global if non-zero
        let any_nonzero = accs.iter().any(|a| !a.is_zero());
        if any_nonzero {
            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_pk, 1,
                &old_vals, false,
                &accs, partial_schema, num_aggs,
            );
        }
    }

    output.sorted = true;
    gnitz_debug!("op_gather_reduce: in={} out={}", n, output.count);
    output
}

/// Emit one gather-reduce output row.
#[allow(clippy::too_many_arguments)]
fn emit_gather_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key: u128,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    schema: &SchemaDescriptor,
    num_aggs: usize,
) {
    let num_cols = schema.num_columns();
    let agg_base = num_cols - num_aggs;

    emit_pk(output, schema, input_mb.get_pk_bytes(exemplar_row), group_key);
    output.extend_weight(&weight.to_le_bytes());

    let mut null_word: u64 = 0;
    let in_null = input_mb.get_null_word(exemplar_row);

    for (out_pi, ci, col) in schema.payload_columns() {
        let cs = col.size() as usize;

        if ci >= agg_base {
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else {
                output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);
            }
        } else {
            // Group exemplar column: copy from input
            let src_pi = out_pi; // same position
            if (in_null >> src_pi) & 1 != 0 {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else if col.type_code == TypeCode::String as u8 {
                write_string_from_batch(
                    output, out_pi,
                    input_mb, exemplar_row, src_pi,
                );
            } else {
                let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                output.extend_col(out_pi, src);
            }
        }
    }

    output.extend_null_bmp(&null_word.to_le_bytes());
    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode, type_code, SHORT_STRING_THRESHOLD};
    use crate::storage::{Batch, ConsolidatedBatch};

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_schema_u64_f32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::F32, 0),
            ],
            &[0],
        )
    }

    fn make_batch_f32(schema: &SchemaDescriptor, rows: &[(u64, i64, f32)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_bits().to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// 3-column source schema: U64 pk (pk_index=0), I64 grp, STRING val (nullable).
    fn make_schema_3col_grp_str() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::STRING, 1),
            ],
            &[0],
        )
    }

    /// 3-column reduce output schema: U128 pk (pk_index=0), I64 grp, I64 agg (nullable).
    fn make_reduce_str_out_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        )
    }

    /// Build a 3-column Batch (U64 pk, I64 grp, STRING val) from tuples.
    /// All strings must be <= 12 bytes (inline, no blob needed).
    fn make_batch_3col_grp_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64, &str)],
    ) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, grp, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &grp.to_le_bytes());
            let bytes = val.as_bytes();
            assert!(bytes.len() <= SHORT_STRING_THRESHOLD, "use inline strings only");
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
            gs[4..4 + bytes.len()].copy_from_slice(bytes);
            b.extend_col(1, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Build a GI Batch (U128 pk: ck_lo=source_pk_lo, ck_hi=gc_u64; I64 payload: spk_hi).
    fn make_gi_batch(rows: &[(u64, u64, i64)]) -> ConsolidatedBatch {
        let gi_schema = crate::ops::index::make_gi_schema();
        let n = rows.len();
        let mut b = Batch::with_schema(gi_schema, n.max(1));

        for &(ck_lo, gc_u64, spk_hi) in rows {
            b.extend_pk(((gc_u64 as u128) << 64) | (ck_lo as u128));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &spk_hi.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_reduce_sum_retraction() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        use crate::schema::{SchemaColumn, type_code};

        // Input: pk(U64), grp(I64), val(I64)
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );

        // Output: pk(U128), grp(I64), sum(I64)
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        // Empty trace_out
        let empty_out = Rc::new(Batch::empty(2, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // Tick 1: insert 3 rows in group 10: val=100, val=200, val=300
        let delta1 = {
            let mut b = Batch::with_schema(in_schema, 3);
            for (pk, val) in [(1u64, 100i64), (2, 200), (3, 300)] {
                b.extend_pk(pk as u128);
                b.extend_weight(&1i64.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &10i64.to_le_bytes()); // grp=10
                b.extend_col(1, &val.to_le_bytes());
                b.count += 1;
            }
            b.sorted = true;
            b.consolidated = true;
            ConsolidatedBatch::new_unchecked(b)
        };

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        let (out1, _) = op_reduce(
            &delta1, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, TypeCode::U64, None, None,
        );
        // SUM of (100+200+300) = 600
        assert_eq!(out1.count, 1);
        let sum1 = crate::util::read_i64_le(out1.col_data(1), 0);
        assert_eq!(sum1, 600);

        // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
        // Need trace_out with previous aggregate
        let prev_out = Rc::new(out1);
        let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);

        let delta2 = {
            let mut b = Batch::with_schema(in_schema, 1);
            b.extend_pk(2u128);
            b.extend_weight(&(-1i64).to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &10i64.to_le_bytes());
            b.extend_col(1, &200i64.to_le_bytes());
            b.count += 1;
            b.sorted = true;
            b.consolidated = true;
            ConsolidatedBatch::new_unchecked(b)
        };

        let (out2, _) = op_reduce(
            &delta2, None, to_ch2.cursor_mut(),
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, TypeCode::U64, None, None,
        );
        // Output: retract old sum (600, w=-1) + insert new sum (400, w=+1) = 2 rows
        assert_eq!(out2.count, 2);
    }

    #[test]
    fn test_reduce_count() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        use crate::schema::type_code;

        // Input: pk(U64), val(I64)
        let in_schema = make_schema_u64_i64();

        // Output: pk(U128), count(I64)
        let out_schema = SchemaDescriptor::new(
            &[
                crate::schema::SchemaColumn::new(type_code::U128, 0),
                crate::schema::SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // 3 rows: pk=1,2,3 all GROUP BY pk (single group using pk as group)
        let delta = make_batch(&in_schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let agg = AggDescriptor {
            col_idx: 0, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        // GROUP BY pk → each row is its own group
        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, TypeCode::U64, None, None,
        );
        // Each pk forms its own group, COUNT=1 for each
        assert_eq!(out.count, 3);
        for i in 0..3 {
            let count = crate::util::read_i64_le(out.col_data(0), i * 8);
            assert_eq!(count, 1, "each single-row group has count=1");
        }
    }

    /// GI path bug: same PK, two different string payloads — the `if` must be `while`.
    #[test]
    fn test_reduce_gi_same_pk_multiple_payloads() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let input_schema = make_schema_3col_grp_str();
        let output_schema = make_reduce_str_out_schema();
        let gi_schema = crate::ops::index::make_gi_schema();

        // trace_in: apple and zebra both at PK=1 (apple sorts first by payload)
        let ti_batch = Rc::new(make_batch_3col_grp_str(
            &input_schema,
            &[(1, 1, 1, "apple"), (1, 1, 1, "zebra")],
        ).into_inner());

        // GI: only PK=1 → group gc_u64=1
        let gi_batch = Rc::new(make_gi_batch(&[(1, 1, 0)]).into_inner());

        // trace_out: empty (no previous aggregate, no retraction emitted)
        let to_batch = Rc::new(Batch::empty(output_schema.num_payload_cols(), 16));

        // delta: retract apple at PK=1
        let delta = make_batch_3col_grp_str(&input_schema, &[(1, -1, 1, "apple")]);

        let mut ti_handle = CursorHandle::from_owned(&[ti_batch], input_schema);
        let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
        let mut to_handle = CursorHandle::from_owned(&[to_batch], output_schema);

        // MAX on STRING agg col (col_idx=2, type=STRING); no AVI
        let agg_desc = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::String,
            _pad: [0; 2],
        };

        let (out, _fin) = op_reduce(
            &delta,
            Some(ti_handle.cursor_mut()),
            to_handle.cursor_mut(),
            &input_schema,
            &output_schema,
            &[1u32],            // group_by_cols: col 1 (grp)
            &[agg_desc],
            None,               // avi_cursor
            false,              // avi_for_max
            TypeCode::String,   // avi_agg_col_type_code (unused; no AVI)
            &[1u32],            // avi_group_by_cols (unused)
            None,               // avi_input_schema
            Some(gi_handle.cursor_mut()), // gi_cursor
            1u32,               // gi_col_idx: grp column
            TypeCode::I64,      // gi_col_type_code
            None,               // finalize_prog
            None,               // finalize_out_schema
        );

        // The accumulator stores the first 8 bytes of the German string as i64.
        // "zebra" first 8 bytes: [len=5, 'z'=122, 'e'=101, 'b'=98, 'r'=114]
        // "apple" first 8 bytes: [len=5, 'a'=97,  'p'=112, 'p'=112, 'l'=108]
        let zebra_ck = i64::from_le_bytes([5, 0, 0, 0, 122, 101, 98, 114]);
        let apple_ck = i64::from_le_bytes([5, 0, 0, 0, 97, 112, 112, 108]);
        assert!(zebra_ck > apple_ck, "test invariant: zebra_ck > apple_ck");

        // With fix: replay = {apple+1, zebra+1, apple−1} → {zebra+1}; one output row.
        // With bug: replay = {apple+1, apple−1} → {}; no output row.
        assert_eq!(out.count, 1,
            "GI loop must be `while` to include zebra after apple is retracted; \
             `if` leaves replay empty → no output");

        // Output payload layout: col_data[0]=grp(I64), col_data[1]=agg(I64)
        let agg = crate::util::read_i64_le(out.col_data(1), 0);
        assert_eq!(agg, zebra_ck,
            "MAX of {{zebra+1}} must be zebra_ck; got {agg} (apple_ck={apple_ck})");
    }

    // -----------------------------------------------------------------------
    // op_gather_reduce
    // -----------------------------------------------------------------------

    #[test]
    fn test_gather_reduce_retraction() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        use crate::schema::type_code;

        // Schema: pk(U128), count(I64) — same as partial/output schema
        let schema = SchemaDescriptor::new(
            &[
                crate::schema::SchemaColumn::new(type_code::U128, 0),
                crate::schema::SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        // Tick 1: two partial COUNT=2 from different workers → global COUNT=4
        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);

        let mut partial1 = Batch::with_schema(schema, 2);

        // Two entries for same group key (pk=1), count=2 each
        for count in [2i64, 2] {
            partial1.extend_pk(1u128);
            partial1.extend_weight(&1i64.to_le_bytes());
            partial1.extend_null_bmp(&0u64.to_le_bytes());
            partial1.extend_col(0, &count.to_le_bytes());
            partial1.count += 1;
        }

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
        assert_eq!(out1.count, 1);
        let global_count = crate::util::read_i64_le(out1.col_data(0), 0);
        assert_eq!(global_count, 4);

        // Tick 2: retract 1 from each worker → partial counts are -1 each → global delta = -2
        let prev_out = Rc::new(out1);
        let mut to_ch2 = CursorHandle::from_owned(&[prev_out], schema);

        let mut partial2 = Batch::with_schema(schema, 2);

        for count in [-1i64, -1] {
            partial2.extend_pk(1u128);
            partial2.extend_weight(&1i64.to_le_bytes());
            partial2.extend_null_bmp(&0u64.to_le_bytes());
            partial2.extend_col(0, &count.to_le_bytes());
            partial2.count += 1;
        }

        let out2 = op_gather_reduce(&partial2, to_ch2.cursor_mut(), &schema, &[agg]);
        // Should have 2 rows: retract old (4, w=-1) + insert new (2, w=+1)
        assert_eq!(out2.count, 2);
    }

    #[test]
    fn test_argsort_delta_f32_group() {
        let schema = make_schema_u64_f32();
        let batch = make_batch_f32(&schema, &[
            (1, 1, 2.0f32),
            (2, 1, -1.0f32),
            (3, 1, 0.5f32),
        ]);
        let indices = argsort_delta(&batch, &schema, &[1]);
        // Sorted order by F32: -1.0 < 0.5 < 2.0
        assert_eq!(indices.len(), 3);
        let mb = batch.as_mem_batch();
        let vals: Vec<f32> = indices.iter().map(|&i| {
            let ptr = mb.get_col_ptr(i as usize, 0, 4);
            f32::from_bits(u32::from_le_bytes(ptr.try_into().unwrap()))
        }).collect();
        assert_eq!(vals, vec![-1.0f32, 0.5f32, 2.0f32]);
    }

    #[test]
    fn test_compare_by_group_cols_f32_negative() {
        let schema = make_schema_u64_f32();
        let batch = make_batch_f32(&schema, &[
            (1, 1, -5.0f32),
            (2, 1, 3.0f32),
        ]);
        let mb = batch.as_mem_batch();
        let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
        let descs = &descs_arr[..descs_len];
        let ord = compare_by_group_cols(&mb, 0, 1, descs);
        assert_eq!(ord, std::cmp::Ordering::Less);
        let ord2 = compare_by_group_cols(&mb, 1, 0, descs);
        assert_eq!(ord2, std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_promote_agg_col_f32_ordering() {
        let schema = make_schema_u64_f32();
        let vals = [-2.0f32, -1.0f32, 0.0f32, 1.0f32, 2.0f32];
        let batch = make_batch_f32(
            &schema,
            &vals.iter().enumerate().map(|(i, &v)| (i as u64 + 1, 1, v)).collect::<Vec<_>>(),
        );
        let mb = batch.as_mem_batch();
        let encoded: Vec<u64> = (0..vals.len()).map(|row| {
            let pi = schema.payload_idx(1); // col_idx=1, pk_index=0
            let ptr = mb.get_col_ptr(row, pi, 4);
            let raw32 = u32::from_le_bytes(ptr.try_into().unwrap());
            // Replicate promote_agg_col_to_u64_ordered for F32 without for_max
            ieee_order_bits_f32(raw32)
        }).collect();
        // Encoded values must be strictly ascending (order-preserving)
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "order-preserving invariant violated: {} >= {}", w[0], w[1]);
        }
        // Round-trip invariant
        for &v in &vals {
            let bits = v.to_bits();
            assert_eq!(
                ieee_order_bits_f32_reverse(ieee_order_bits_f32(bits)),
                bits,
                "round-trip failed for {:?}",
                v
            );
        }
    }

    #[test]
    fn test_extract_group_key_f32() {
        let schema = make_schema_u64_f32();
        let batch = make_batch_f32(&schema, &[
            (1, 1, 1.5f32),
            (2, 1, 2.5f32),
        ]);
        let mb = batch.as_mem_batch();
        let key0 = extract_group_key(&mb, 0, &schema, &[1]);
        let key1 = extract_group_key(&mb, 1, &schema, &[1]);
        assert_ne!(key0, key1, "different F32 values must produce different group keys");
    }

    // -----------------------------------------------------------------------
    // Fix 1: Schema-agnostic reads for sub-8-byte columns
    // -----------------------------------------------------------------------

    fn make_schema_with_type(tc: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(tc, 0),
            ],
            &[0],
        )
    }

    fn make_batch_typed_i32(schema: &SchemaDescriptor, rows: &[(u64, i64, i32)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_batch_typed_i16(schema: &SchemaDescriptor, rows: &[(u64, i64, i16)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_reduce_sum_i32() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_schema_with_type(type_code::I32);

        // Output: pk(U128), sum(I64)
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // 3 rows with I32 values, group by PK
        let delta = make_batch_typed_i32(&in_schema, &[
            (1, 1, 100i32), (2, 1, 200i32), (3, 1, -50i32),
        ]);

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I32, _pad: [0; 2],
        };

        // GROUP BY pk → each row is its own group
        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, TypeCode::U64, None, None,
        );
        assert_eq!(out.count, 3);
        // Check values: row offsets depend on PK order (group_by_pk path)
        let sum0 = crate::util::read_i64_le(out.col_data(0), 0);
        let sum1 = crate::util::read_i64_le(out.col_data(0), 8);
        let sum2 = crate::util::read_i64_le(out.col_data(0), 16);
        assert_eq!(sum0, 100, "SUM of I32 100");
        assert_eq!(sum1, 200, "SUM of I32 200");
        assert_eq!(sum2, -50, "SUM of I32 -50 (sign extension)");
    }

    #[test]
    fn test_reduce_min_f32() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_schema_with_type(type_code::F32);

        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // Use a 2-col input schema: pk(U64), val(F32), GROUP BY pk
        let delta = make_batch_f32(&in_schema, &[
            (1, 1, 3.5f32), (1, 1, -1.0f32), (1, 1, 7.0f32),
        ]);

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::F32, _pad: [0; 2],
        };

        // GROUP BY pk → all 3 rows in same group
        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, TypeCode::U64, None, None,
        );
        assert_eq!(out.count, 1);
        // MIN should be -1.0 stored as f64 bits
        let bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
        let min_val = f64::from_bits(bits);
        assert_eq!(min_val, -1.0f64, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
    }

    #[test]
    fn test_reduce_max_i16() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_schema_with_type(type_code::I16);

        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // 3 rows with I16 values, all same PK
        let delta = make_batch_typed_i16(&in_schema, &[
            (1, 1, -100i16), (1, 1, 200i16), (1, 1, 50i16),
        ]);

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AggOp::Max, col_type_code: TypeCode::I16, _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, TypeCode::U64, None, None,
        );
        assert_eq!(out.count, 1);
        let max_val = crate::util::read_i64_le(out.col_data(0), 0);
        assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
    }

    // -----------------------------------------------------------------------
    // Fix 6: gather_reduce MIN retraction
    // -----------------------------------------------------------------------

    #[test]
    fn test_gather_reduce_min_retraction() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        // Schema: pk(U128), min_val(I64)
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        // Tick 1: partial MIN=5 from one worker → global MIN=5
        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);

        let mut partial1 = Batch::with_schema(schema, 1);

        partial1.extend_pk(1u128);
        partial1.extend_weight(&1i64.to_le_bytes());
        partial1.extend_null_bmp(&0u64.to_le_bytes());
        partial1.extend_col(0, &5i64.to_le_bytes());
        partial1.count += 1;

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
        assert_eq!(out1.count, 1);
        let min1 = crate::util::read_i64_le(out1.col_data(0), 0);
        assert_eq!(min1, 5);

        // Tick 2: partial MIN=3 from one worker. The old global (5) should be folded in
        // via merge_accumulated with weight=1 → combine(5). New MIN should be min(3, 5) = 3.
        let prev_out = Rc::new(out1);
        let mut to_ch2 = CursorHandle::from_owned(&[prev_out], schema);

        let mut partial2 = Batch::with_schema(schema, 1);

        partial2.extend_pk(1u128);
        partial2.extend_weight(&1i64.to_le_bytes());
        partial2.extend_null_bmp(&0u64.to_le_bytes());
        partial2.extend_col(0, &3i64.to_le_bytes());
        partial2.count += 1;

        let out2 = op_gather_reduce(&partial2, to_ch2.cursor_mut(), &schema, &[agg]);
        // Should have: retract old (5, w=-1) + insert new (3, w=+1)
        assert_eq!(out2.count, 2, "should retract old MIN and emit new MIN");
        let retracted = crate::util::read_i64_le(out2.col_data(0), 0);
        assert_eq!(retracted, 5, "retraction should be old MIN value 5");
        assert_eq!(out2.get_weight(0), -1);
        let new_min = crate::util::read_i64_le(out2.col_data(0), 8);
        assert_eq!(new_min, 3, "new MIN should be 3 (min of old 5 and partial 3)");
        assert_eq!(out2.get_weight(1), 1);
    }

    // -----------------------------------------------------------------------
    // UUID non-PK GROUP BY correctness
    // -----------------------------------------------------------------------

    /// Schema: pk(U64) + uuid_payload(UUID). UUID is at payload index 0.
    fn make_schema_u64_pk_uuid_payload() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::UUID, 0),
            ],
            &[0],
        )
    }

    /// Schema: pk(U64) + uuid_col(UUID) + i64_col(I64).
    fn make_schema_u64_uuid_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::UUID, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn build_batch_u64_uuid(schema: &SchemaDescriptor, rows: &[(u64, u128)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, uuid) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &uuid.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn build_batch_u64_uuid_i64(schema: &SchemaDescriptor, rows: &[(u64, u128, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, uuid, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &uuid.to_le_bytes());
            b.extend_col(1, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_compare_by_group_cols_uuid_non_pk() {
        // UUID non-PK column used as GROUP BY column. Before the fix, compare_by_group_cols
        // falls to the else branch with cs=16, panicking on a_buf[..16] (buf is [u8; 8]).
        let schema = make_schema_u64_pk_uuid_payload();
        let uuid_lo: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0001u128;
        let uuid_hi: u128 = 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFFu128;
        let batch = build_batch_u64_uuid(&schema, &[(1, uuid_lo), (2, uuid_hi)]);
        let mb = batch.as_mem_batch();
        let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
        let descs = &descs_arr[..descs_len];

        // uuid_lo < uuid_hi (compare by the 128-bit value)
        let ord = compare_by_group_cols(&mb, 0, 1, descs);
        assert_eq!(ord, std::cmp::Ordering::Less, "uuid_lo row must compare less than uuid_hi row");

        let ord2 = compare_by_group_cols(&mb, 1, 0, descs);
        assert_eq!(ord2, std::cmp::Ordering::Greater, "uuid_hi row must compare greater than uuid_lo row");

        let ord3 = compare_by_group_cols(&mb, 0, 0, descs);
        assert_eq!(ord3, std::cmp::Ordering::Equal, "same row must compare equal to itself");
    }

    #[test]
    fn test_argsort_delta_uuid_group() {
        // argsort_delta with UUID group column: calls compare_by_group_cols.
        // Before fix: panics. After fix: rows sorted by UUID value.
        let schema = make_schema_u64_pk_uuid_payload();
        let uuid_a: u128 = 0x1000_0000_0000_0000_0000_0000_0000_0001u128;
        let uuid_b: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0002u128;
        // uuid_b < uuid_a (lower high byte)
        let batch = build_batch_u64_uuid(&schema, &[(1, uuid_a), (2, uuid_b)]);
        let indices = argsort_delta(&batch, &schema, &[1]);
        assert_eq!(indices.len(), 2);
        // Row with uuid_b (row 1) should sort before row with uuid_a (row 0)
        assert_eq!(indices[0], 1, "uuid_b (smaller) must sort first");
        assert_eq!(indices[1], 0, "uuid_a (larger) must sort second");
    }

    #[test]
    fn test_extract_group_key_uuid_multi_col() {
        // Multi-column GROUP BY that includes a UUID column. Before fix, extract_group_key's
        // hash loop uses a [u8; 8] buffer for UUID (cs=16), panicking on buf[..16].
        let schema = make_schema_u64_uuid_i64();
        let uuid_a: u128 = 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_0001u128;
        let uuid_b: u128 = 0x1111_2222_3333_4444_5555_6666_7777_8888u128;
        let batch = build_batch_u64_uuid_i64(&schema, &[
            (1, uuid_a, 42i64),
            (2, uuid_b, 42i64),
            (3, uuid_a, 43i64),
        ]);
        let mb = batch.as_mem_batch();

        // GROUP BY (uuid_col=1, i64_col=2)
        let key0 = extract_group_key(&mb, 0, &schema, &[1, 2]); // uuid_a, 42
        let key1 = extract_group_key(&mb, 1, &schema, &[1, 2]); // uuid_b, 42
        let key2 = extract_group_key(&mb, 2, &schema, &[1, 2]); // uuid_a, 43
        let key0b = extract_group_key(&mb, 0, &schema, &[1, 2]); // same as key0

        assert_ne!(key0, key1, "different UUIDs same int must yield different group keys");
        assert_ne!(key0, key2, "same UUID different int must yield different group keys");
        assert_ne!(key1, key2, "different UUID different int must yield different group keys");
        assert_eq!(key0, key0b, "same inputs must yield the same group key");
    }

    // -----------------------------------------------------------------------
    // GI group-key over-read bug: narrow-type group column
    // -----------------------------------------------------------------------

    /// GI path reads the group key with a hardcoded col_size=8. When the group
    /// column is narrower (e.g. I32, stride=4) and group_start_idx > 0, the
    /// stride-8 indexing walks into the adjacent I64 column region, producing a
    /// garbage gc_u64_val. The GI seek then misses and history rows are not
    /// fetched, so MIN returns only the delta value instead of the true minimum.
    #[test]
    fn test_reduce_gi_i32_group_key_overread() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        // Input: U64 pk | I32 grp (4 bytes) | I64 val (8 bytes)
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );

        // Output: U128 hash-pk | I32 grp | I64 min (nullable)
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let gi_schema = crate::ops::index::make_gi_schema();

        // trace_in: history row for grp=5, val=200, source pk=30
        let ti_batch = Rc::new({
            let mut b = Batch::with_schema(in_schema, 1);
            b.extend_pk(30u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &5i32.to_le_bytes());
            b.extend_col(1, &200i64.to_le_bytes());
            b.count += 1;
            b.sorted = true;
            b.consolidated = true;
            ConsolidatedBatch::new_unchecked(b).into_inner()
        });

        // GI: gc_u64=5 → source pk=30
        let gi_batch = Rc::new(make_gi_batch(&[(30, 5, 0)]).into_inner());

        // Empty trace_out (no prior aggregate)
        let to_batch = Rc::new(Batch::empty(2, 16));

        // Delta: 2 groups so that grp=5 is at group_start_idx=1 after argsort.
        // With the bug: get_col_ptr(1, pi=0, col_size=8) uses stride 8 on a
        // column with stride 4, landing at offset 48+8=56 in the batch buffer
        // which is the start of the I64 val region. It reads val[row0]=100 as
        // gc_u64_val instead of 5, the GI seek misses, and MIN(grp=5)=300.
        let delta = {
            let mut b = Batch::with_schema(in_schema, 2);
            b.extend_pk(10u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &3i32.to_le_bytes());
            b.extend_col(1, &100i64.to_le_bytes());
            b.count += 1;
            b.extend_pk(20u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &5i32.to_le_bytes());
            b.extend_col(1, &300i64.to_le_bytes());
            b.count += 1;
            b.sorted = true;
            b.consolidated = true;
            ConsolidatedBatch::new_unchecked(b)
        };

        let mut ti_handle = CursorHandle::from_owned(&[ti_batch], in_schema);
        let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
        let mut to_handle = CursorHandle::from_owned(&[to_batch], out_schema);

        let agg = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta,
            Some(ti_handle.cursor_mut()),
            to_handle.cursor_mut(),
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            None, false, TypeCode::U64, &[], None,
            Some(gi_handle.cursor_mut()),
            1u32,
            TypeCode::I32,
            None, None,
        );

        assert_eq!(out.count, 2, "expected 2 output groups (grp=3 and grp=5)");

        // Output payload: col 0 = I32 grp (4 bytes/row), col 1 = I64 min (8 bytes/row)
        let grp_data = out.col_data(0);
        let min_data = out.col_data(1);
        let mut min_for_5 = None;
        for i in 0..2 {
            let g = i32::from_le_bytes(grp_data[i * 4..(i + 1) * 4].try_into().unwrap());
            if g == 5 {
                let m = i64::from_le_bytes(min_data[i * 8..(i + 1) * 8].try_into().unwrap());
                min_for_5 = Some(m);
            }
        }
        let m = min_for_5.expect("no output row for grp=5");
        assert_eq!(m, 200,
            "MIN(grp=5) must include history row val=200; \
             got {m} — GI group-key over-read produced a garbage gc_u64_val");
    }

    // -----------------------------------------------------------------------
    // GROUP BY containing the PK column (mixed pk/non-pk group_by_cols).
    // -----------------------------------------------------------------------

    /// Schema: U64 pk (col 0) | I64 other (col 1). pk_index = 0.
    fn make_schema_pk0_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    /// Schema: I64 other (col 0) | U64 pk (col 1). pk_index = 1.
    fn make_schema_pk1_i64_u64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1],
        )
    }

    /// Build a 2-col batch (pk, other) with explicit pk values and `other` payload.
    /// Works for either pk_index=0 or pk_index=1 since extend_col(pi, ..) addresses
    /// the dense payload region — the non-PK column always lives at payload index 0.
    fn build_pk_other(schema: &SchemaDescriptor, rows: &[(u64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, other) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &other.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_extract_group_key_includes_pk_pki0() {
        // GROUP BY [pk, other] with pk_index=0: hash loop must dispatch via
        // is_pk_col, not call payload_idx(0, 0) and underflow.
        let schema = make_schema_pk0_u64_i64();
        let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
        let mb = batch.as_mem_batch();

        let k_pk10_v100 = extract_group_key(&mb, 0, &schema, &[0, 1]);
        let k_pk20_v100 = extract_group_key(&mb, 1, &schema, &[0, 1]);
        let k_pk10_v200 = extract_group_key(&mb, 2, &schema, &[0, 1]);
        let k_pk10_v100_again = extract_group_key(&mb, 0, &schema, &[0, 1]);

        assert_ne!(k_pk10_v100, k_pk20_v100, "different PKs, same other → distinct keys");
        assert_ne!(k_pk10_v100, k_pk10_v200, "same PK, different other → distinct keys");
        assert_eq!(k_pk10_v100, k_pk10_v100_again, "same row → same key");
    }

    #[test]
    fn test_extract_group_key_includes_pk_pki1() {
        // GROUP BY [other, pk] with pk_index=1: previously read the wrong
        // payload column when c_idx == pki for non-zero pk_index.
        let schema = make_schema_pk1_i64_u64();
        let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
        let mb = batch.as_mem_batch();

        // group_by [col 0 = other, col 1 = pk]
        let k_pk10_v100 = extract_group_key(&mb, 0, &schema, &[0, 1]);
        let k_pk20_v100 = extract_group_key(&mb, 1, &schema, &[0, 1]);
        let k_pk10_v200 = extract_group_key(&mb, 2, &schema, &[0, 1]);

        assert_ne!(k_pk10_v100, k_pk20_v100);
        assert_ne!(k_pk10_v100, k_pk10_v200);
    }

    #[test]
    fn test_compare_by_group_cols_includes_pk() {
        // Sort/compare path must dispatch on the PK sentinel rather than
        // dereferencing a fake pi for the PK column.
        let schema = make_schema_pk0_u64_i64();
        let batch = build_pk_other(&schema, &[(10, 100), (20, 100)]);
        let mb = batch.as_mem_batch();

        let (descs_arr, descs_len) = build_sort_descs(&schema, &[0, 1]);
        let descs = &descs_arr[..descs_len];
        // First desc covers PK — must use the sentinel.
        assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL);

        assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Less);
        assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Greater);
        assert_eq!(compare_by_group_cols(&mb, 0, 0, descs), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_argsort_delta_pk_in_group() {
        // Multi-column group containing PK must reach compare_by_group_cols
        // and use the sentinel branch — must not panic.
        let schema = make_schema_pk0_u64_i64();
        let batch = build_pk_other(&schema, &[
            (20, 100),
            (10, 200),
            (10, 100),
        ]);
        let indices = argsort_delta(&batch, &schema, &[0, 1]);
        assert_eq!(indices.len(), 3);
        // Sorted by (pk, other): (10,100), (10,200), (20,100)
        let mb = batch.as_mem_batch();
        let pks: Vec<u64> = indices.iter().map(|&i| mb.get_pk(i as usize) as u64).collect();
        assert_eq!(pks, vec![10, 10, 20]);
        let others: Vec<i64> = indices.iter().map(|&i| {
            i64::from_le_bytes(mb.get_col_ptr(i as usize, 0, 8).try_into().unwrap())
        }).collect();
        assert_eq!(others, vec![100, 200, 100]);
    }

    // -----------------------------------------------------------------------
    // NULL group columns must form a distinct group (not merged with 0).
    // -----------------------------------------------------------------------

    /// Schema: U64 pk | nullable I64.
    fn make_schema_pk_nullable_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        )
    }

    /// Build a 2-col batch (pk, nullable_i64). For null rows, payload bytes
    /// are zero (DirectWriter convention) and the null bit at payload pi=0 is set.
    fn build_pk_null_i64(schema: &SchemaDescriptor, rows: &[(u64, Option<i64>)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            let null_word: u64 = if val.is_none() { 1 } else { 0 };
            b.extend_null_bmp(&null_word.to_le_bytes());
            // Nulls store as zero bytes — same byte pattern as integer 0.
            b.extend_col(0, &val.unwrap_or(0).to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_extract_group_key_null_distinct_from_zero() {
        let schema = make_schema_pk_nullable_i64();
        let batch = build_pk_null_i64(&schema, &[
            (1, None),
            (2, Some(0)),
            (3, Some(7)),
            (4, None),
        ]);
        let mb = batch.as_mem_batch();

        let k_null = extract_group_key(&mb, 0, &schema, &[1]);
        let k_zero = extract_group_key(&mb, 1, &schema, &[1]);
        let k_seven = extract_group_key(&mb, 2, &schema, &[1]);
        let k_null2 = extract_group_key(&mb, 3, &schema, &[1]);

        assert_ne!(k_null, k_zero, "NULL must form a distinct group from 0");
        assert_ne!(k_null, k_seven);
        assert_ne!(k_zero, k_seven);
        assert_eq!(k_null, k_null2, "two NULL rows must collapse into the same group");
    }

    #[test]
    fn test_compare_by_group_cols_nulls_first() {
        let schema = make_schema_pk_nullable_i64();
        let batch = build_pk_null_i64(&schema, &[
            (1, Some(7)),
            (2, None),
            (3, None),
        ]);
        let mb = batch.as_mem_batch();
        let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
        let descs = &descs_arr[..descs_len];

        // NULL < 7 (NULLS FIRST)
        assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Less);
        assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Greater);
        // NULL == NULL → equal (same group)
        assert_eq!(compare_by_group_cols(&mb, 1, 2, descs), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_argsort_delta_nullable_no_packed_sort() {
        // Nullable single-column group must skip the packed-sort fast path
        // (which sorts raw bytes and would interleave NULLs with 0s) and
        // route through compare_by_group_cols where NULL < non-NULL.
        let schema = make_schema_pk_nullable_i64();
        let batch = build_pk_null_i64(&schema, &[
            (1, Some(0)),
            (2, None),
            (3, Some(5)),
            (4, None),
        ]);
        let indices = argsort_delta(&batch, &schema, &[1]);
        let mb = batch.as_mem_batch();
        // NULLs must be adjacent (single group), not interleaved with 0s.
        let null_word_at = |i: u32| mb.get_null_word(i as usize) & 1 != 0;
        let null_positions: Vec<usize> = indices.iter().enumerate()
            .filter(|&(_, &i)| null_word_at(i))
            .map(|(p, _)| p)
            .collect();
        assert_eq!(null_positions.len(), 2, "expected 2 NULL rows");
        // NULLS FIRST: both nulls at positions 0 and 1.
        assert_eq!(null_positions, vec![0, 1]);
    }

    // -----------------------------------------------------------------------
    // UUID single-column GROUP BY: extract_gc_u64 must mix high+low halves.
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_gc_u64_uuid_distinguishes_high_bits() {
        // Two UUIDs differing only in the high 64 bits previously truncated
        // to the same low-64 value, colliding in AVI buckets.
        let schema = make_schema_u64_pk_uuid_payload();
        let uuid_a: u128 = 0x0000_0000_0000_0001_DEAD_BEEF_CAFE_BABEu128;
        let uuid_b: u128 = 0xFFFF_FFFF_FFFF_FFFF_DEAD_BEEF_CAFE_BABEu128;
        // Sanity: low 64 bits identical
        assert_eq!(uuid_a as u64, uuid_b as u64);

        let batch = build_batch_u64_uuid(&schema, &[(1, uuid_a), (2, uuid_b)]);
        let mb = batch.as_mem_batch();

        let gc_a = extract_gc_u64(&mb, 0, &schema, &[1]);
        let gc_b = extract_gc_u64(&mb, 1, &schema, &[1]);

        assert_ne!(gc_a, gc_b,
            "UUIDs differing only in high 64 bits must produce distinct AVI bucket keys; \
             pre-fix truncation collided them");
    }

    // -----------------------------------------------------------------------
    // emit_finalized_row: U128 PK projected through CopyCol must not panic
    // when the destination column size is 16 bytes.
    // -----------------------------------------------------------------------

    #[test]
    fn test_emit_finalized_row_u128_pk_copy_col() {
        use crate::expr::{ExprProgram, EXPR_COPY_COL};

        // Raw output schema: U128 pk | I64 cnt
        let raw_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        // Finalized schema: U128 pk_out | U128 pk_copy | I64 cnt
        let fin_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        // Two COPY_COL instructions: copy col 0 (PK) and col 1 (cnt).
        // Layout per instruction: [op, dst, a1=src_col, a2]. classify_output_cols
        // reads src_col from a1 (instr[base + 2]).
        let code: Vec<i64> = vec![
            EXPR_COPY_COL, 0, 0, 0, // copy raw col 0 (PK) → fin col 1
            EXPR_COPY_COL, 0, 1, 0, // copy raw col 1 (cnt) → fin col 2
        ];
        let mut prog = ExprProgram::new(code, 0, 0, vec![]);
        prog.resolve_column_indices(&raw_schema);

        // Build raw_output with one row: pk = a wide U128, cnt = 42
        let pk: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210u128;
        let mut raw_output = Batch::with_schema(raw_schema, 1);
        raw_output.extend_pk(pk);
        raw_output.extend_weight(&1i64.to_le_bytes());
        raw_output.extend_null_bmp(&0u64.to_le_bytes());
        raw_output.extend_col(0, &42i64.to_le_bytes());
        raw_output.count += 1;

        let mut fin_output = Batch::with_schema(fin_schema, 1);
        // Must not panic on the 16-byte PK slice. Pre-fix: `pk as u64` produced
        // 8 bytes and `[..cs]` with cs=16 panicked.
        let mut ctx = crate::expr::FinalizeContext::new(&prog, &raw_schema);
        emit_finalized_row(
            &mut fin_output, &raw_output, 0,
            pk, 1,
            &prog, &raw_schema, &fin_schema, &mut ctx,
        );

        assert_eq!(fin_output.count, 1);
        // The PK copy lands in finalized payload column 0 (fin col 1, since fin col 0 is PK).
        let copied = u128::from_le_bytes(fin_output.col_data(0)[..16].try_into().unwrap());
        assert_eq!(copied, pk, "U128 PK must round-trip through emit_finalized_row");
    }

    // -----------------------------------------------------------------------
    // Compound-PK reduce: byte-form emit + Accumulator PK-column read + order
    // -----------------------------------------------------------------------

    /// 2×U64 compound-PK input schema. pk_indices = [0, 1]; payload col is I64.
    fn make_compound_pk_2xu64_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// Build a 2×U64 compound-PK batch. Rows: (pk0, pk1, weight, val).
    fn make_batch_compound_2xu64(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, i64, i64)],
    ) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));
        for &(pk0, pk1, w, val) in rows {
            let mut pk_bytes = [0u8; 16];
            pk_bytes[0..8].copy_from_slice(&pk0.to_le_bytes());
            pk_bytes[8..16].copy_from_slice(&pk1.to_le_bytes());
            b.extend_pk_bytes(&pk_bytes);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// emit_reduce_row natural-PK byte path on a 2×U64 compound PK: PK bytes
    /// must be copied verbatim from the source row, not packed from group_key.
    #[test]
    fn test_emit_reduce_row_compound_pk_bytes() {
        let in_schema = make_compound_pk_2xu64_schema();

        // Output schema matches what build_reduce_output_schema would produce
        // for group_set_eq_pk on this input with a COUNT aggregate:
        // 2 PK cols (U64,U64) followed by I64 count.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );

        let pk0: u64 = 0xAAAA_BBBB_CCCC_DDDDu64;
        let pk1: u64 = 0x1111_2222_3333_4444u64;
        let input = make_batch_compound_2xu64(&in_schema, &[(pk0, pk1, 1, 99)]);
        let mb = input.as_mem_batch();

        let mut output = Batch::with_schema(out_schema, 1);
        let agg = AggDescriptor {
            col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
        };
        let accs: Vec<Accumulator> = vec![Accumulator::new(&agg, &in_schema)];
        // Synthetic group_key is irrelevant on the byte path; pass arbitrary value.
        emit_reduce_row(
            &mut output, &mb, 0,
            0u128, 1,
            &[0u64], false,
            &accs, &in_schema, &out_schema,
            &[0u32, 1u32], true /* use_natural_pk */, 1,
        );

        assert_eq!(output.count, 1);
        let mut expected = [0u8; 16];
        expected[0..8].copy_from_slice(&pk0.to_le_bytes());
        expected[8..16].copy_from_slice(&pk1.to_le_bytes());
        assert_eq!(output.get_pk_bytes(0), &expected[..],
            "compound natural-PK output must copy source row's PK bytes verbatim");
    }

    /// Accumulator MIN on the SECOND PK column of a 2×U64 compound PK.
    /// Regression: the second PK column must be decoded by walking its
    /// byte offset within the PK region, not by widening the whole
    /// region to u128 (which would yield column 0).
    #[test]
    fn test_reduce_min_pk_col_compound_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_compound_pk_2xu64_schema();

        // Output: full natural compound PK + I64 agg, matching the
        // build_reduce_output_schema layout for group_set_eq_pk.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0, 1],
        );

        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // Two distinct compound PKs whose pk_col_0 and pk_col_1 disagree
        // about ordering: pk_col_1 values are 7 and 3 → MIN must be 3.
        let delta = make_batch_compound_2xu64(&in_schema, &[
            (10, 7, 1, 100),
            (20, 3, 1, 200),
        ]);

        // MIN over the SECOND PK column (col_idx=1).
        let agg = AggDescriptor {
            col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema,
            &[0u32, 1u32], &[agg],
            None, false, TypeCode::U64, &[], None,
            None, 0, TypeCode::U64, None, None,
        );
        // group_by_pk: each (pk0, pk1) is its own group, so we get one row
        // per input row; MIN within each group equals the row's pk_col_1.
        assert_eq!(out.count, 2);
        let mins: Vec<i64> = (0..out.count)
            .map(|i| crate::util::read_i64_le(out.col_data(0), i * 8))
            .collect();
        // Output is in pk_indices order = [0, 1] ascending, so (10,7) precedes (20,3).
        assert_eq!(mins, vec![7, 3],
            "MIN(pk_col_1) per single-row group must equal that row's pk_col_1");
    }

    /// Single-PK U64 MIN(pk_col) — sanity check that the byte-offset
    /// PK-read path produces the same result as the prior u128 path.
    #[test]
    fn test_reduce_min_pk_col_single_pk_u64() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_schema_u64_i64();
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // Input is PK-sorted from consolidation; the group_by_pk fast path
        // passes that order straight through.
        let delta = make_batch(&in_schema, &[(7, 1, 0), (42, 1, 0), (99, 1, 0)]);

        let agg = AggDescriptor {
            col_idx: 0, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
        };
        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema,
            &[0u32], &[agg],
            None, false, TypeCode::U64, &[], None,
            None, 0, TypeCode::U64, None, None,
        );
        // GROUP BY pk → each row is its own group; MIN(pk) per group equals the row's pk.
        assert_eq!(out.count, 3);
        let mins: Vec<i64> = (0..out.count)
            .map(|i| crate::util::read_i64_le(out.col_data(0), i * 8))
            .collect();
        assert_eq!(mins, vec![7, 42, 99]);
    }

    /// Permuted group_by_cols on a compound PK must still emit rows in
    /// pk_indices order (the input is PK-sorted from consolidation; the
    /// fast path skips the sort and passes row order through).
    #[test]
    fn test_reduce_group_by_pk_permuted_preserves_pk_order() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_compound_pk_2xu64_schema();
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0, 1],
        );

        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // Two PKs whose [0,1] and [1,0] orderings disagree: (1,2) vs (2,1).
        // PK-sorted (pk_indices=[0,1]) order: (1,2) then (2,1).
        let delta = make_batch_compound_2xu64(&in_schema, &[
            (1, 2, 1, 10),
            (2, 1, 1, 20),
        ]);

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        // group_by_cols permuted to [1, 0] — a valid set permutation of pk_indices.
        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema,
            &[1u32, 0u32], &[agg],
            None, false, TypeCode::U64, &[], None,
            None, 0, TypeCode::U64, None, None,
        );

        assert_eq!(out.count, 2);
        let row0_pk = out.get_pk_bytes(0);
        let row1_pk = out.get_pk_bytes(1);
        let p0_col0 = u64::from_le_bytes(row0_pk[0..8].try_into().unwrap());
        let p0_col1 = u64::from_le_bytes(row0_pk[8..16].try_into().unwrap());
        let p1_col0 = u64::from_le_bytes(row1_pk[0..8].try_into().unwrap());
        let p1_col1 = u64::from_le_bytes(row1_pk[8..16].try_into().unwrap());
        assert_eq!((p0_col0, p0_col1), (1, 2),
            "first emitted row must be (1, 2) in pk_indices order");
        assert_eq!((p1_col0, p1_col1), (2, 1),
            "second emitted row must be (2, 1) in pk_indices order");
    }

    // -----------------------------------------------------------------------
    // Compound-PK subset grouping: PK-region access must be per-PK-column
    // (pre-fix the slow path widened the entire region and split groups
    // that share the addressed PK column but differ in other PK columns).
    // -----------------------------------------------------------------------

    /// compare_by_group_cols on the PK-sentinel branch must compare only
    /// the addressed PK column. Two rows that share `pk_col_0` but differ
    /// in `pk_col_1` must compare Equal under `GROUP BY pk_col_0`.
    #[test]
    fn test_compare_by_group_cols_pk_sentinel_compound_subset() {
        let schema = make_compound_pk_2xu64_schema();
        let batch = make_batch_compound_2xu64(&schema, &[
            (10, 7, 1, 100),
            (10, 9, 1, 200),
            (20, 7, 1, 300),
        ]);
        let mb = batch.as_mem_batch();

        let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
        let descs = &descs_arr[..descs_len];
        assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL,
            "subset group on PK col 0 must produce a PK-sentinel SortDesc");
        assert_eq!(descs[0].pk_off, 0, "pk_col_0 byte offset within PK region");

        // Same pk_col_0 (10), different pk_col_1 → Equal under GROUP BY pk_col_0.
        assert_eq!(
            compare_by_group_cols(&mb, 0, 1, descs),
            std::cmp::Ordering::Equal,
            "rows with same pk_col_0 must form one group regardless of pk_col_1",
        );
        // Different pk_col_0 → ordering follows pk_col_0.
        assert_eq!(compare_by_group_cols(&mb, 0, 2, descs), std::cmp::Ordering::Less);
        assert_eq!(compare_by_group_cols(&mb, 2, 0, descs), std::cmp::Ordering::Greater);
    }

    /// compare_by_group_cols on PK-sentinel with `GROUP BY pk_col_1`
    /// (non-zero PK byte offset) must isolate pk_col_1.
    #[test]
    fn test_compare_by_group_cols_pk_sentinel_compound_pk_col_1() {
        let schema = make_compound_pk_2xu64_schema();
        let batch = make_batch_compound_2xu64(&schema, &[
            (1, 50, 1, 100),
            (2, 50, 1, 200),
            (3, 60, 1, 300),
        ]);
        let mb = batch.as_mem_batch();

        let (descs_arr, descs_len) = build_sort_descs(&schema, &[1u32]);
        let descs = &descs_arr[..descs_len];
        assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL);
        assert_eq!(descs[0].pk_off, 8, "pk_col_1 byte offset within PK region");

        // Same pk_col_1 (50), different pk_col_0 → Equal.
        assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Equal);
        // Different pk_col_1 → ordering follows pk_col_1.
        assert_eq!(compare_by_group_cols(&mb, 0, 2, descs), std::cmp::Ordering::Less);
    }

    /// Single-PK U64 with `GROUP BY pk` must be bit-identical to the prior
    /// whole-region widen path — pk_off = 0, cs = pk_stride = 8.
    #[test]
    fn test_compare_by_group_cols_pk_sentinel_single_pk_bit_identical() {
        let schema = make_schema_u64_i64();
        let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
        let mb = batch.as_mem_batch();

        let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
        let descs = &descs_arr[..descs_len];
        assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL);
        assert_eq!(descs[0].pk_off, 0);
        assert_eq!(descs[0].cs, 8);

        assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Less);
        assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Greater);
        assert_eq!(compare_by_group_cols(&mb, 0, 2, descs), std::cmp::Ordering::Equal);
    }

    /// cursor_matches_group on a PK-sentinel SortDesc with compound PK must
    /// match rows that share the addressed PK column but differ elsewhere.
    #[test]
    fn test_cursor_matches_group_pk_sentinel_compound_subset() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_compound_pk_2xu64_schema();
        // Cursor row: (pk0=10, pk1=99). Exemplar row: (pk0=10, pk1=42).
        // GROUP BY pk_col_0 → must match (both share pk0=10).
        let cursor_batch = Rc::new(make_batch_compound_2xu64(&schema, &[
            (10, 99, 1, 0),
        ]));
        let exemplar_batch = make_batch_compound_2xu64(&schema, &[
            (10, 42, 1, 0),
            (20, 42, 1, 0),
        ]);
        let exemplar_mb = exemplar_batch.as_mem_batch();

        let mut cursor_handle = CursorHandle::from_owned(&[cursor_batch], schema);
        let cursor = cursor_handle.cursor_mut();
        cursor.seek(0u128);
        assert!(cursor.valid, "cursor must be positioned on the single row");

        let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
        let descs = &descs_arr[..descs_len];

        // Row 0 (pk0=10) shares pk_col_0 with the cursor → match.
        assert!(cursor_matches_group(cursor, &exemplar_mb, 0, descs),
            "exemplar (10,42) and cursor (10,99) share pk_col_0=10");
        // Row 1 (pk0=20) differs from the cursor's pk_col_0=10 → no match.
        assert!(!cursor_matches_group(cursor, &exemplar_mb, 1, descs),
            "exemplar (20,42) differs from cursor (10,99) in pk_col_0");
    }

    /// extract_group_key on `GROUP BY pk_col_0` (single PK column of a
    /// compound PK) must return the same u128 for two rows that share
    /// pk_col_0 — distinct pk_col_1 values must not collide them into
    /// different groups.
    #[test]
    fn test_extract_group_key_single_pk_col_compound_subset() {
        let schema = make_compound_pk_2xu64_schema();
        let batch = make_batch_compound_2xu64(&schema, &[
            (10, 50, 1, 0),
            (10, 99, 1, 0),
            (20, 50, 1, 0),
        ]);
        let mb = batch.as_mem_batch();

        let k0 = extract_group_key(&mb, 0, &schema, &[0u32]);
        let k1 = extract_group_key(&mb, 1, &schema, &[0u32]);
        let k2 = extract_group_key(&mb, 2, &schema, &[0u32]);

        assert_eq!(k0, 10u128, "key must equal pk_col_0 value (10), not whole PK region");
        assert_eq!(k0, k1, "rows sharing pk_col_0 must hash to the same group key");
        assert_eq!(k2, 20u128);
        assert_ne!(k0, k2);
    }

    /// Pair-test on single-PK U64: extract_group_key must still return
    /// the full PK region (bit-identical to the prior whole-region widen).
    #[test]
    fn test_extract_group_key_single_pk_col_single_pk_bit_identical() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(42, 1, 100), (99, 1, 200)]);
        let mb = batch.as_mem_batch();

        let k0 = extract_group_key(&mb, 0, &schema, &[0u32]);
        let k1 = extract_group_key(&mb, 1, &schema, &[0u32]);
        assert_eq!(k0, 42u128, "single PK widens to the same value as before");
        assert_eq!(k1, 99u128);
    }

    /// End-to-end op_reduce: GROUP BY pk_col_0 (a strict subset of a
    /// compound PK) with COUNT(*). Pre-fix the slow path widened the
    /// whole PK region and split every (pk_col_0, pk_col_1) pair into
    /// its own group; the fix collapses rows sharing pk_col_0.
    #[test]
    fn test_op_reduce_compound_pk_group_by_subset_count() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let in_schema = make_compound_pk_2xu64_schema();
        // GROUP BY a single U64 column → use_natural_pk via
        // is_single_col_natural_pk. Output: U64 pk + I64 count.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let empty_out = Rc::new(Batch::empty(1, 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        // PK-sorted (pk0, pk1): (1,10), (1,20), (2,10).
        let delta = make_batch_compound_2xu64(&in_schema, &[
            (1, 10, 1, 0),
            (1, 20, 1, 0),
            (2, 10, 1, 0),
        ]);

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema,
            &[0u32], &[agg],
            None, false, TypeCode::U64, &[], None,
            None, 0, TypeCode::U64, None, None,
        );

        // Two groups: pk_col_0=1 (count=2), pk_col_0=2 (count=1).
        // Pre-fix the count would be 3 (one row per (pk0, pk1) pair).
        assert_eq!(out.count, 2, "GROUP BY pk_col_0 collapses (1,10) and (1,20) into one group");

        // Output rows in pk_col_0 ascending order (slow path argsorts).
        let mut entries: Vec<(u64, i64)> = (0..out.count)
            .map(|i| {
                let pk_bytes = out.get_pk_bytes(i);
                let pk = u64::from_le_bytes(pk_bytes.try_into().unwrap());
                let cnt = crate::util::read_i64_le(out.col_data(0), i * 8);
                (pk, cnt)
            })
            .collect();
        entries.sort_by_key(|&(pk, _)| pk);
        assert_eq!(entries, vec![(1, 2), (2, 1)]);
    }
}
