//! Reduce operator: accumulator, group key, argsort, AVI, op_reduce, op_gather_reduce.

use crate::schema::{SchemaDescriptor, type_code};
use crate::schema::type_code::STRING as TYPE_STRING;
use crate::memtable::OwnedBatch;
use crate::merge::MemBatch;
use crate::read_cursor::ReadCursor;

use super::util::{
    consolidate_owned, append_cursor_row_to_batch, write_string_from_batch, payload_idx,
    extract_group_key,
};

// ---------------------------------------------------------------------------
// Aggregate opcodes (matches gnitz/dbsp/functions.py)
// ---------------------------------------------------------------------------

const AGG_COUNT: u8 = 1;
const AGG_SUM: u8 = 2;
const AGG_MIN: u8 = 3;
const AGG_MAX: u8 = 4;
const AGG_COUNT_NON_NULL: u8 = 5;

/// Descriptor for one aggregate function, passed across FFI.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: u8,
    pub col_type_code: u8,
    pub _pad: [u8; 2],
}

const _: () = assert!(std::mem::size_of::<AggDescriptor>() == 8);
const _: () = assert!(std::mem::align_of::<AggDescriptor>() == 4);


/// Accumulator: internal state for one aggregate column.
struct Accumulator {
    acc: i64,
    has_value: bool,
    agg_op: u8,
    col_type_code: u8,
    col_idx: u32,
}

impl Accumulator {
    fn new(desc: &AggDescriptor) -> Self {
        Accumulator {
            acc: 0,
            has_value: false,
            agg_op: desc.agg_op,
            col_type_code: desc.col_type_code,
            col_idx: desc.col_idx,
        }
    }

    fn reset(&mut self) {
        self.acc = 0;
        self.has_value = false;
    }

    fn is_linear(&self) -> bool {
        self.agg_op == AGG_COUNT || self.agg_op == AGG_SUM || self.agg_op == AGG_COUNT_NON_NULL
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
        self.col_type_code == type_code::F64 || self.col_type_code == type_code::F32
    }

    /// Step: incorporate one input row into the accumulator.
    fn step_from_batch(
        &mut self,
        mb: &MemBatch,
        row: usize,
        schema: &SchemaDescriptor,
        weight: i64,
    ) {
        let col_idx = self.col_idx as usize;
        let pk_index = schema.pk_index as usize;

        // COUNT ignores nulls entirely
        if self.agg_op != AGG_COUNT {
            let payload_idx = payload_idx(col_idx, pk_index);
            let null_word = mb.get_null_word(row);
            if (null_word >> payload_idx) & 1 != 0 {
                return; // null value — skip
            }
        }

        let first = !self.has_value;
        self.has_value = true;
        let is_f = self.is_float();

        match self.agg_op {
            AGG_COUNT => {
                self.acc = self.acc.wrapping_add(weight);
            }
            AGG_COUNT_NON_NULL => {
                // Already checked null above
                self.acc = self.acc.wrapping_add(weight);
            }
            AGG_SUM => {
                if is_f {
                    let val_f = read_col_value_f64(mb, row, col_idx, pk_index, self.col_type_code);
                    let cur_f = f64::from_bits(self.acc as u64);
                    let w_f = weight as f64;
                    self.acc = f64::to_bits(cur_f + val_f * w_f) as i64;
                } else {
                    let val = read_col_value(mb, row, col_idx, pk_index, self.col_type_code);
                    self.acc = self.acc.wrapping_add(val.wrapping_mul(weight));
                }
            }
            AGG_MIN => {
                if is_f {
                    let v = read_col_value_f64(mb, row, col_idx, pk_index, self.col_type_code);
                    if first || v < f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(v) as i64;
                    }
                } else {
                    let v = read_col_value(mb, row, col_idx, pk_index, self.col_type_code);
                    if first || v < self.acc {
                        self.acc = v;
                    }
                }
            }
            AGG_MAX => {
                if is_f {
                    let v = read_col_value_f64(mb, row, col_idx, pk_index, self.col_type_code);
                    if first || v > f64::from_bits(self.acc as u64) {
                        self.acc = f64::to_bits(v) as i64;
                    }
                } else {
                    let v = read_col_value(mb, row, col_idx, pk_index, self.col_type_code);
                    if first || v > self.acc {
                        self.acc = v;
                    }
                }
            }
            _ => {}
        }
    }

    /// Merge an already-accumulated value (from trace_out) into this accumulator.
    /// For linear aggregates (COUNT, SUM): applies weight arithmetic.
    /// For MIN/MAX with positive weight: delegates to combine().
    /// For MIN/MAX with negative weight: no-op (cannot un-MIN/un-MAX).
    fn merge_accumulated(&mut self, value_bits: u64, weight: i64) {
        let is_f = self.is_float();
        match self.agg_op {
            AGG_COUNT | AGG_COUNT_NON_NULL => {
                let prev = value_bits as i64;
                self.acc = self.acc.wrapping_add(prev.wrapping_mul(weight));
                self.has_value = true;
            }
            AGG_SUM => {
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
            AGG_MIN | AGG_MAX => {
                if weight > 0 {
                    self.combine(value_bits);
                }
                // Negative weight: algebraically unsound for MIN/MAX — skip.
            }
            _ => {}
        }
    }

    /// Combine a partial aggregate from another shard.
    fn combine(&mut self, other_bits: u64) {
        let is_f = self.is_float();
        match self.agg_op {
            AGG_COUNT | AGG_COUNT_NON_NULL => {
                let prev = other_bits as i64;
                self.acc = self.acc.wrapping_add(prev);
                self.has_value = true;
            }
            AGG_SUM => {
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
            AGG_MIN => {
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
            AGG_MAX => {
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
            _ => {}
        }
    }
}

/// Read a column value as sign-extended i64, respecting actual column size.
/// STRING columns: reads first 8 bytes of the 16-byte German String struct (compare key).
#[inline]
fn read_col_value(mb: &MemBatch, row: usize, col_idx: usize, pk_index: usize, col_type_code: u8) -> i64 {
    let pi = payload_idx(col_idx, pk_index);
    let cs = crate::schema::type_size(col_type_code) as usize;
    let ptr = mb.get_col_ptr(row, pi, cs);
    if col_type_code == type_code::STRING {
        // STRING agg: compare key is first 8 bytes of 16-byte German String
        i64::from_le_bytes(ptr[..8].try_into().unwrap())
    } else {
        crate::schema::read_signed(ptr, cs)
    }
}

/// Read a float column value as f64, with proper F32→F64 promotion.
#[inline]
fn read_col_value_f64(mb: &MemBatch, row: usize, col_idx: usize, pk_index: usize, col_type_code: u8) -> f64 {
    let pi = payload_idx(col_idx, pk_index);
    if col_type_code == type_code::F32 {
        let ptr = mb.get_col_ptr(row, pi, 4);
        f32::from_bits(u32::from_le_bytes(ptr.try_into().unwrap())) as f64
    } else {
        let ptr = mb.get_col_ptr(row, pi, 8);
        f64::from_bits(u64::from_le_bytes(ptr.try_into().unwrap()))
    }
}

// ---------------------------------------------------------------------------
// Group key extraction
// ---------------------------------------------------------------------------

/// Extract 64-bit group key for AVI composite keys.
pub(super) fn extract_gc_u64(
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u64 {
    let pki = schema.pk_index as usize;
    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let tc = schema.columns[c_idx].type_code;
        if tc != type_code::U128 && tc != TYPE_STRING
            && tc != type_code::F32 && tc != type_code::F64
        {
            let pi = payload_idx(c_idx, pki);
            let cs = crate::schema::type_size(tc) as usize;
            let ptr = mb.get_col_ptr(row, pi, cs);
            let mut buf = [0u8; 8];
            buf[..cs].copy_from_slice(ptr);
            return u64::from_le_bytes(buf);
        }
    }
    let (lo, _hi) = extract_group_key(mb, row, schema, group_by_cols);
    lo
}

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

/// Compare two rows by group columns.
fn compare_by_group_cols(
    mb: &MemBatch,
    row_a: usize,
    row_b: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> std::cmp::Ordering {
    let pki = schema.pk_index as usize;
    for &c_idx_u32 in group_by_cols {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        let pi = payload_idx(c_idx, pki);

        let ord = if tc == TYPE_STRING {
            let a_bytes = mb.get_col_ptr(row_a, pi, 16);
            let b_bytes = mb.get_col_ptr(row_b, pi, 16);
            use crate::schema::compare_german_strings;
            compare_german_strings(a_bytes, mb.blob, b_bytes, mb.blob)
        } else if tc == type_code::F64 {
            let a_ptr = mb.get_col_ptr(row_a, pi, 8);
            let b_ptr = mb.get_col_ptr(row_b, pi, 8);
            let a_f = f64::from_bits(crate::util::read_u64_le(a_ptr, 0));
            let b_f = f64::from_bits(crate::util::read_u64_le(b_ptr, 0));
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        } else if tc == type_code::F32 {
            let a_ptr = mb.get_col_ptr(row_a, pi, 4);
            let b_ptr = mb.get_col_ptr(row_b, pi, 4);
            let a_f = f32::from_bits(crate::util::read_u32_le(a_ptr, 0));
            let b_f = f32::from_bits(crate::util::read_u32_le(b_ptr, 0));
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        } else if tc == type_code::U128 {
            let a_ptr = mb.get_col_ptr(row_a, pi, 16);
            let b_ptr = mb.get_col_ptr(row_b, pi, 16);
            let a_lo = u64::from_le_bytes(a_ptr[0..8].try_into().unwrap());
            let a_hi = u64::from_le_bytes(a_ptr[8..16].try_into().unwrap());
            let b_lo = u64::from_le_bytes(b_ptr[0..8].try_into().unwrap());
            let b_hi = u64::from_le_bytes(b_ptr[8..16].try_into().unwrap());
            (a_hi, a_lo).cmp(&(b_hi, b_lo))
        } else if tc == type_code::I64 || tc == type_code::I32
            || tc == type_code::I16 || tc == type_code::I8
        {
            let cs = crate::schema::type_size(tc) as usize;
            let a_ptr = mb.get_col_ptr(row_a, pi, cs);
            let b_ptr = mb.get_col_ptr(row_b, pi, cs);
            let a_v = crate::schema::read_signed(a_ptr, cs);
            let b_v = crate::schema::read_signed(b_ptr, cs);
            a_v.cmp(&b_v)
        } else {
            let cs = crate::schema::type_size(tc) as usize;
            let a_ptr = mb.get_col_ptr(row_a, pi, cs);
            let b_ptr = mb.get_col_ptr(row_b, pi, cs);
            let mut a_buf = [0u8; 8];
            let mut b_buf = [0u8; 8];
            a_buf[..cs].copy_from_slice(a_ptr);
            b_buf[..cs].copy_from_slice(b_ptr);
            u64::from_le_bytes(a_buf).cmp(&u64::from_le_bytes(b_buf))
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Argsort delta batch by group columns.
fn argsort_delta(
    batch: &OwnedBatch,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> Vec<u32> {
    let mb = batch.as_mem_batch();
    let n = batch.count;
    let mut indices: Vec<u32> = (0..n as u32).collect();
    if n <= 1 {
        return indices;
    }

    // Fast path: single I64 column
    let pki = schema.pk_index as usize;
    if group_by_cols.len() == 1 {
        let ci = group_by_cols[0] as usize;
        let tc = schema.columns[ci].type_code;
        if tc == type_code::I64 {
            let pi = payload_idx(ci, pki);
            let keys: Vec<i64> = (0..n)
                .map(|i| {
                    let ptr = mb.get_col_ptr(i, pi, 8);
                    i64::from_le_bytes(ptr.try_into().unwrap())
                })
                .collect();
            indices.sort_unstable_by_key(|&i| keys[i as usize]);
            return indices;
        }
    }

    indices.sort_unstable_by(|&a, &b| {
        compare_by_group_cols(&mb, a as usize, b as usize, schema, group_by_cols)
    });
    indices
}

/// Sort batch by (PK, payload) without consolidation.
/// Used by op_gather_reduce where we need to see each partial separately.
fn sort_owned(batch: &OwnedBatch, schema: &SchemaDescriptor) -> OwnedBatch {
    let n = batch.count;
    if n <= 1 || batch.sorted {
        return batch.clone_batch();
    }

    let mb = batch.as_mem_batch();
    let pks: Vec<u128> = (0..n).map(|i| mb.get_pk(i)).collect();
    let mut indices: Vec<u32> = (0..n as u32).collect();
    indices.sort_unstable_by(|&a, &b| {
        let ai = a as usize;
        let bi = b as usize;
        match pks[ai].cmp(&pks[bi]) {
            std::cmp::Ordering::Equal => {
                crate::columnar::compare_rows(schema, &mb, ai, &mb, bi)
            }
            ord => ord,
        }
    });

    // Scatter-copy in sorted order
    let mut output = OwnedBatch::with_schema(*schema, n);
    output.count = 0;
    for &idx in &indices {
        output.append_batch(batch, idx as usize, idx as usize + 1);
    }
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
    agg_col_type_code: u8,
    acc: &mut Accumulator,
) -> bool {
    avi_cursor.seek(0, gc_u64);
    gnitz_debug!("avi_lookup: seek(0, {:#x}) valid={}", gc_u64, avi_cursor.valid);
    while avi_cursor.valid {
        let k_hi = avi_cursor.current_key_hi;
        let k_lo = avi_cursor.current_key_lo;
        let w = avi_cursor.current_weight;
        gnitz_debug!("avi_lookup: at pk_lo={:#x} pk_hi={:#x} w={}", k_lo, k_hi, w);
        if k_hi != gc_u64 {
            break;
        }
        if avi_cursor.current_weight > 0 {
            let mut encoded = avi_cursor.current_key_lo;
            if for_max {
                encoded = !encoded;
            }
            let tc = agg_col_type_code;
            if tc == type_code::I64 || tc == type_code::I32
                || tc == type_code::I16 || tc == type_code::I8
            {
                encoded = (encoded as i64).wrapping_sub(1i64 << 63) as u64;
            } else if tc == type_code::F64 {
                encoded = ieee_order_bits_reverse(encoded);
            } else if tc == type_code::F32 {
                encoded = ieee_order_bits_f32_reverse(encoded) as u64;
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
pub fn op_reduce(
    delta: &OwnedBatch,
    trace_in_cursor: Option<&mut ReadCursor>,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    avi_for_max: bool,
    avi_agg_col_type_code: u8,
    avi_group_by_cols: &[u32],
    avi_input_schema: Option<&SchemaDescriptor>,
    gi_cursor: Option<&mut ReadCursor>,
    gi_col_idx: u32,
    _gi_col_type_code: u8,
    finalize_prog: Option<&crate::expr::ExprProgram>,
    finalize_out_schema: Option<&SchemaDescriptor>,
) -> (OwnedBatch, Option<OwnedBatch>) {
    let num_aggs = agg_descs.len();
    let num_out_cols = output_schema.num_columns as usize;
    let out_npc = num_out_cols - 1;
    let in_pki = input_schema.pk_index as usize;

    // Linearity check
    let all_linear = agg_descs.iter().all(|d| {
        d.agg_op == AGG_COUNT || d.agg_op == AGG_SUM || d.agg_op == AGG_COUNT_NON_NULL
    });

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    let consolidated;
    let working: &OwnedBatch = if all_linear {
        delta
    } else {
        consolidated = consolidate_owned(delta, input_schema);
        &consolidated
    };

    let n = working.count;
    if n == 0 {
        let empty_fin = finalize_out_schema.map(|fs| {
            OwnedBatch::empty(fs.num_columns as usize - 1)
        });
        return (OwnedBatch::empty(out_npc), empty_fin);
    }

    // group_by_pk detection
    let group_by_pk = group_by_cols.len() == 1
        && group_by_cols[0] as usize == in_pki;

    // Argsort
    let sorted_indices = if group_by_pk {
        (0..n as u32).collect()
    } else {
        argsort_delta(&working, input_schema, group_by_cols)
    };

    let mb = working.as_mem_batch();

    // Determine output mapping: use_natural_pk
    let use_natural_pk = if group_by_cols.len() == 1 {
        let grp_tc = input_schema.columns[group_by_cols[0] as usize].type_code;
        grp_tc == type_code::U64 || grp_tc == type_code::U128
    } else {
        false
    };

    let mut raw_output = OwnedBatch::empty(out_npc);
    let mut fin_output = finalize_out_schema.map(|fs| {
        OwnedBatch::empty(fs.num_columns as usize - 1)
    });

    let mut accs: Vec<Accumulator> = agg_descs.iter().map(Accumulator::new).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    // We need mutable access to optional cursors. Take ownership via Option::take pattern.
    // The caller passes these as Option<&mut ReadCursor>, but we need to use them
    // multiple times in the loop. They're already &mut so we can use them directly.
    let mut trace_in = trace_in_cursor;
    let mut avi = avi_cursor;
    let mut gi = gi_cursor;

    let mut idx = 0usize;
    let mut num_groups = 0usize;
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = sorted_indices[group_start_pos] as usize;

        let (group_key_lo, group_key_hi) = if group_by_pk {
            (mb.get_pk_lo(group_start_idx), mb.get_pk_hi(group_start_idx))
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
                if mb.get_pk_lo(curr_idx) != group_key_lo
                    || mb.get_pk_hi(curr_idx) != group_key_hi
                {
                    break;
                }
            } else {
                if compare_by_group_cols(&mb, curr_idx, group_start_idx, input_schema, group_by_cols)
                    != std::cmp::Ordering::Equal
                {
                    break;
                }
            }

            let w = mb.get_weight(curr_idx);
            for (_k, acc) in accs.iter_mut().enumerate() {
                if acc.is_linear() {
                    acc.step_from_batch(&mb, curr_idx, input_schema, w);
                }
            }
            idx += 1;
        }

        // Retraction: read old value from trace_out
        trace_out_cursor.seek(group_key_lo, group_key_hi);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key_lo == group_key_lo
            && trace_out_cursor.current_key_hi == group_key_hi;

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
                group_key_lo, group_key_hi, -1,
                &old_vals, true, // use_old_val=true
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out)) =
                (finalize_prog, finalize_out_schema, &mut fin_output)
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key_lo, group_key_hi, -1,
                    prog, output_schema, fin_schema,
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
                // Full replay path: build replay batch from trace_in + delta
                let mut replay = OwnedBatch::with_schema(*input_schema, 32);
                replay.count = 0;
                replay.sorted = false;
                replay.consolidated = false;

                if let Some(ref mut ti_cursor) = trace_in {
                    if group_by_pk {
                        ti_cursor.seek(group_key_lo, group_key_hi);
                        while ti_cursor.valid
                            && ti_cursor.current_key_lo == group_key_lo
                            && ti_cursor.current_key_hi == group_key_hi
                        {
                            append_cursor_row_to_batch(&mut replay, ti_cursor, input_schema);
                            ti_cursor.advance();
                        }
                    } else if let Some(ref mut gi_c) = gi {
                        // GI path
                        let pki = input_schema.pk_index as usize;
                        let gi_ci = gi_col_idx as usize;
                        let pi = if gi_ci < pki { gi_ci } else { gi_ci - 1 };
                        let ptr = mb.get_col_ptr(group_start_idx, pi, 8);
                        let gc_u64_val = u64::from_le_bytes(ptr.try_into().unwrap());
                        gi_c.seek(0, gc_u64_val);
                        while gi_c.valid {
                            let gk_hi = gi_c.current_key_hi;
                            if gk_hi != gc_u64_val {
                                break;
                            }
                            if gi_c.current_weight > 0 {
                                let spk_lo = gi_c.current_key_lo;
                                // spk_hi is in payload col 1 (first payload col = col index 1)
                                let spk_hi_ptr = gi_c.col_ptr(1, 8);
                                let spk_hi = if !spk_hi_ptr.is_null() {
                                    let bytes = unsafe { std::slice::from_raw_parts(spk_hi_ptr, 8) };
                                    u64::from_le_bytes(bytes.try_into().unwrap())
                                } else {
                                    0
                                };
                                if let Some(ref mut ti) = trace_in {
                                    ti.seek(spk_lo, spk_hi);
                                    while ti.valid
                                        && ti.current_key_lo == spk_lo
                                        && ti.current_key_hi == spk_hi
                                    {
                                        append_cursor_row_to_batch(&mut replay, ti, input_schema);
                                        ti.advance();
                                    }
                                }
                            }
                            gi_c.advance();
                        }
                    } else {
                        // Fallback: full trace scan
                        ti_cursor.seek(0, 0);
                        let ti_mb_exemplar_row = group_start_idx;
                        while ti_cursor.valid {
                            // Compare group columns between cursor row and exemplar
                            if cursor_matches_group(
                                ti_cursor, &mb, ti_mb_exemplar_row,
                                input_schema, group_by_cols,
                            ) {
                                append_cursor_row_to_batch(&mut replay, ti_cursor, input_schema);
                            }
                            ti_cursor.advance();
                        }
                    }
                }

                // Append delta rows to replay
                for k in group_start_pos..idx {
                    let d_idx = sorted_indices[k] as usize;
                    append_membatch_row_to_batch(&mut replay, &mb, d_idx, input_schema);
                }

                // Consolidate replay and step all accumulators
                let merged = consolidate_owned(&replay, input_schema);
                for acc in accs.iter_mut() {
                    acc.reset();
                }
                let merged_mb = merged.as_mem_batch();
                for m in 0..merged.count {
                    let w = merged_mb.get_weight(m);
                    if w > 0 {
                        for acc in accs.iter_mut() {
                            acc.step_from_batch(&merged_mb, m, input_schema, w);
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
                group_key_lo, group_key_hi, 1,
                &old_vals, false, // use_old_val=false
                &accs, input_schema, output_schema,
                group_by_cols, use_natural_pk, num_aggs,
            );
            if let (Some(prog), Some(fin_schema), Some(ref mut fin_out)) =
                (finalize_prog, finalize_out_schema, &mut fin_output)
            {
                emit_finalized_row(
                    fin_out, &raw_output, raw_output.count - 1,
                    group_key_lo, group_key_hi, 1,
                    prog, output_schema, fin_schema,
                );
            }
        }

        num_groups += 1;
    }

    // Output flags
    raw_output.sorted = group_by_pk;
    if group_by_pk {
        raw_output.consolidated = true;
    }

    gnitz_debug!(
        "op_reduce: in={} groups={} out={} fin={}",
        n, num_groups, raw_output.count,
        fin_output.as_ref().map_or(0, |b| b.count)
    );

    (raw_output, fin_output)
}

/// Emit one reduce output row.
fn emit_reduce_row(
    output: &mut OwnedBatch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key_lo: u64,
    group_key_hi: u64,
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
    let out_pki = output_schema.pk_index as usize;
    let in_pki = input_schema.pk_index as usize;
    let num_out_cols = output_schema.num_columns as usize;

    output.pk_lo.extend_from_slice(&group_key_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&group_key_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());

    // Build null word and payload columns
    let mut null_word: u64 = 0;
    let mut out_pi = 0usize;

    for ci in 0..num_out_cols {
        if ci == out_pki {
            continue;
        }
        let col = &output_schema.columns[ci];
        let cs = col.size as usize;

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
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            } else {
                output.col_data[out_pi].extend_from_slice(&bits.to_le_bytes()[..cs]);
            }
        } else if use_natural_pk {
            // use_natural_pk: no group exemplar columns in output (PK IS the group)
            // This shouldn't happen — with use_natural_pk, non-agg non-PK cols don't exist
            { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
        } else {
            // Group exemplar column: ci=1..N maps to group_by_cols[ci-1]
            let grp_idx = ci - 1; // skip PK at 0
            if grp_idx < group_by_cols.len() {
                let src_ci = group_by_cols[grp_idx] as usize;
                let src_pi = payload_idx(src_ci, in_pki);
                // Check null from input
                let in_null = input_mb.get_null_word(exemplar_row);
                if (in_null >> src_pi) & 1 != 0 {
                    null_word |= 1u64 << out_pi;
                    { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
                } else if col.type_code == TYPE_STRING {
                    write_string_from_batch(
                        &mut output.col_data[out_pi],
                        &mut output.blob,
                        input_mb, exemplar_row, src_pi,
                    );
                } else {
                    let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                    output.col_data[out_pi].extend_from_slice(src);
                }
            } else {
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            }
        }
        out_pi += 1;
    }

    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());
    output.count += 1;
}

/// Emit a finalized row by evaluating the finalize ExprProgram on the raw output.
///
/// Handles COPY_COL (copy column from raw→finalized), EMIT (computed value),
/// and EMIT_NULL (null column) instructions by pre-scanning the bytecode.
fn emit_finalized_row(
    fin_output: &mut OwnedBatch,
    raw_output: &OwnedBatch,
    raw_row: usize,
    group_key_lo: u64,
    group_key_hi: u64,
    weight: i64,
    prog: &crate::expr::ExprProgram,
    raw_schema: &SchemaDescriptor,
    fin_schema: &SchemaDescriptor,
) {
    use crate::expr::{EXPR_COPY_COL, EXPR_EMIT, EXPR_EMIT_NULL};

    let raw_mb = raw_output.as_mem_batch();
    let raw_pki = raw_schema.pk_index as usize;
    let fin_pki = fin_schema.pk_index as usize;
    let null_word = raw_mb.get_null_word(raw_row);

    // Pre-scan bytecode: classify each output column as COPY_COL, EMIT, or EMIT_NULL
    #[derive(Clone, Copy)]
    enum OutCol {
        CopyCol(usize), // source logical col in raw schema
        Emit(usize),     // index in the eval emit sequence
        EmitNull,
    }

    let mut out_cols: Vec<OutCol> = Vec::new();
    let mut emit_count = 0usize;
    for i in 0..prog.num_instrs as usize {
        let base = i * 4;
        let op = prog.code[base];
        if op == EXPR_COPY_COL {
            let src_col = prog.code[base + 2] as usize;
            out_cols.push(OutCol::CopyCol(src_col));
        } else if op == EXPR_EMIT {
            out_cols.push(OutCol::Emit(emit_count));
            emit_count += 1;
        } else if op == EXPR_EMIT_NULL {
            out_cols.push(OutCol::EmitNull);
        }
    }

    // Create emit targets only for EMIT columns
    let buf_rows = raw_row + 1;
    let mut emit_bufs: Vec<Vec<u8>> = Vec::with_capacity(emit_count);
    for _ in 0..emit_count {
        emit_bufs.push(vec![0u8; buf_rows * 8]); // 8 bytes max per column
    }

    let emit_targets: Vec<crate::expr::EmitTarget> = emit_bufs
        .iter_mut()
        .enumerate()
        .map(|(i, buf)| crate::expr::EmitTarget {
            base: buf.as_mut_ptr(),
            stride: 8,
            payload_col: i,
        })
        .collect();

    // Evaluate (only EMIT instructions write to targets)
    let (_result, _is_null, eval_emit_mask) = crate::expr::eval_with_emit(
        prog, &raw_mb, raw_row, raw_schema.pk_index, null_word, &emit_targets,
    );

    // Build the finalized output row
    fin_output.pk_lo.extend_from_slice(&group_key_lo.to_le_bytes());
    fin_output.pk_hi.extend_from_slice(&group_key_hi.to_le_bytes());
    fin_output.weight.extend_from_slice(&weight.to_le_bytes());

    let mut fin_null_mask: u64 = 0;
    let mut fpi = 0usize;
    let mut out_col_idx = 0usize;

    for ci in 0..fin_schema.num_columns as usize {
        if ci == fin_pki {
            continue;
        }
        let cs = fin_schema.columns[ci].size as usize;

        if out_col_idx < out_cols.len() {
            match out_cols[out_col_idx] {
                OutCol::CopyCol(src_col) => {
                    if src_col == raw_pki {
                        // Source is the PK column — read from pk_lo
                        let pk_lo = raw_mb.get_pk_lo(raw_row);
                        fin_output.col_data[fpi].extend_from_slice(&pk_lo.to_le_bytes()[..cs]);
                    } else {
                        let src_pi = payload_idx(src_col, raw_pki);
                        if (null_word >> src_pi) & 1 != 0 {
                            fin_null_mask |= 1u64 << fpi;
                            { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
                        } else if raw_schema.columns[src_col].type_code == TYPE_STRING {
                            write_string_from_batch(
                                &mut fin_output.col_data[fpi],
                                &mut fin_output.blob,
                                &raw_mb, raw_row, src_pi,
                            );
                        } else {
                            let src = raw_mb.get_col_ptr(raw_row, src_pi, cs);
                            fin_output.col_data[fpi].extend_from_slice(src);
                        }
                    }
                }
                OutCol::Emit(eidx) => {
                    if (eval_emit_mask >> eidx) & 1 != 0 {
                        fin_null_mask |= 1u64 << fpi;
                        { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
                    } else {
                        let off = raw_row * 8;
                        fin_output.col_data[fpi].extend_from_slice(
                            &emit_bufs[eidx][off..off + cs],
                        );
                    }
                }
                OutCol::EmitNull => {
                    fin_null_mask |= 1u64 << fpi;
                    { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
                }
            }
        } else {
            { let cur = fin_output.col_data[fpi].len(); fin_output.col_data[fpi].resize(cur + cs, 0); }
        }

        fpi += 1;
        out_col_idx += 1;
    }

    fin_output.null_bmp.extend_from_slice(&fin_null_mask.to_le_bytes());
    fin_output.count += 1;
}

/// Append a row from a MemBatch to an OwnedBatch.
fn append_membatch_row_to_batch(
    output: &mut OwnedBatch,
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
) {
    let pki = schema.pk_index as usize;
    output.pk_lo.extend_from_slice(&mb.get_pk_lo(row).to_le_bytes());
    output.pk_hi.extend_from_slice(&mb.get_pk_hi(row).to_le_bytes());
    output.weight.extend_from_slice(&mb.get_weight(row).to_le_bytes());
    let null_word = mb.get_null_word(row);
    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());

    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pki {
            continue;
        }
        let col = &schema.columns[ci];
        let cs = col.size as usize;
        let is_null = (null_word >> pi) & 1 != 0;
        if is_null {
            let new_len = output.col_data[pi].len() + cs;
            output.col_data[pi].resize(new_len, 0);
        } else if col.type_code == TYPE_STRING {
            write_string_from_batch(
                &mut output.col_data[pi],
                &mut output.blob,
                mb, row, pi,
            );
        } else {
            let src = mb.get_col_ptr(row, pi, cs);
            output.col_data[pi].extend_from_slice(src);
        }
        pi += 1;
    }
    output.count += 1;
}

/// Check if a cursor's current row matches the group columns of an exemplar row.
fn cursor_matches_group(
    cursor: &ReadCursor,
    exemplar_mb: &MemBatch,
    exemplar_row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> bool {
    let pki = schema.pk_index as usize;
    for &c_idx_u32 in group_by_cols {
        let c_idx = c_idx_u32 as usize;
        let col = &schema.columns[c_idx];
        let cs = col.size as usize;
        let pi = payload_idx(c_idx, pki);

        let cursor_ptr = cursor.col_ptr(c_idx, cs);
        if cursor_ptr.is_null() {
            return false;
        }
        let cursor_bytes = unsafe { std::slice::from_raw_parts(cursor_ptr, cs) };
        let exemplar_bytes = exemplar_mb.get_col_ptr(exemplar_row, pi, cs);

        if col.type_code == TYPE_STRING {
            let cursor_blob = cursor.blob_ptr();
            let cmp = crate::schema::compare_german_strings(
                cursor_bytes, if cursor_blob.is_null() { &[] } else { unsafe { std::slice::from_raw_parts(cursor_blob, cursor.blob_len()) } },
                exemplar_bytes, exemplar_mb.blob,
            );
            if cmp != std::cmp::Ordering::Equal {
                return false;
            }
        } else {
            if cursor_bytes != exemplar_bytes {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// op_gather_reduce
// ---------------------------------------------------------------------------

/// Gather-reduce: merge partial aggregate deltas from workers.
pub fn op_gather_reduce(
    partial_batch: &OwnedBatch,
    trace_out_cursor: &mut ReadCursor,
    partial_schema: &SchemaDescriptor,
    agg_descs: &[AggDescriptor],
) -> OwnedBatch {
    let num_aggs = agg_descs.len();
    let num_out_cols = partial_schema.num_columns as usize;
    let out_npc = num_out_cols - 1;

    // Sort without consolidation
    let sorted = sort_owned(partial_batch, partial_schema);
    let n = sorted.count;
    if n == 0 {
        return OwnedBatch::empty(out_npc);
    }

    let smb = sorted.as_mem_batch();

    // Derive group_indices layout
    let num_group_cols = num_out_cols - 1 - num_aggs; // -1 for PK
    let _use_natural_pk_gather = num_group_cols == 0;

    let mut output = OwnedBatch::empty(out_npc);
    let mut accs: Vec<Accumulator> = agg_descs.iter().map(Accumulator::new).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];

    let mut idx = 0usize;
    while idx < n {
        let group_key_lo = smb.get_pk_lo(idx);
        let group_key_hi = smb.get_pk_hi(idx);
        let exemplar_row = idx;

        for acc in accs.iter_mut() {
            acc.reset();
        }

        // Accumulate all partial deltas for this group
        while idx < n
            && smb.get_pk_lo(idx) == group_key_lo
            && smb.get_pk_hi(idx) == group_key_hi
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

        // Read old global from trace_out
        trace_out_cursor.seek(group_key_lo, group_key_hi);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_key_lo == group_key_lo
            && trace_out_cursor.current_key_hi == group_key_hi;

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
                group_key_lo, group_key_hi, -1,
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
                group_key_lo, group_key_hi, 1,
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
fn emit_gather_row(
    output: &mut OwnedBatch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key_lo: u64,
    group_key_hi: u64,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    schema: &SchemaDescriptor,
    num_aggs: usize,
) {
    let pki = schema.pk_index as usize;
    let num_cols = schema.num_columns as usize;

    output.pk_lo.extend_from_slice(&group_key_lo.to_le_bytes());
    output.pk_hi.extend_from_slice(&group_key_hi.to_le_bytes());
    output.weight.extend_from_slice(&weight.to_le_bytes());

    let mut null_word: u64 = 0;
    let in_null = input_mb.get_null_word(exemplar_row);
    let mut out_pi = 0usize;

    for ci in 0..num_cols {
        if ci == pki {
            continue;
        }
        let col = &schema.columns[ci];
        let cs = col.size as usize;

        let agg_base = num_cols - num_aggs;
        if ci >= agg_base {
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            } else {
                output.col_data[out_pi].extend_from_slice(&bits.to_le_bytes()[..cs]);
            }
        } else {
            // Group exemplar column: copy from input
            let src_pi = out_pi; // same position
            if (in_null >> src_pi) & 1 != 0 {
                null_word |= 1u64 << out_pi;
                { let cur = output.col_data[out_pi].len(); output.col_data[out_pi].resize(cur + cs, 0); }
            } else if col.type_code == TYPE_STRING {
                write_string_from_batch(
                    &mut output.col_data[out_pi],
                    &mut output.blob,
                    input_mb, exemplar_row, src_pi,
                );
            } else {
                let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                output.col_data[out_pi].extend_from_slice(src);
            }
        }
        out_pi += 1;
    }

    output.null_bmp.extend_from_slice(&null_word.to_le_bytes());
    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, SHORT_STRING_THRESHOLD};
    use crate::memtable::OwnedBatch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn make_schema_u64_f32() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::F32, size: 4, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch_f32(schema: &SchemaDescriptor, rows: &[(u64, i64, f32)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_bits().to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// 3-column source schema: U64 pk (pk_index=0), I64 grp, STRING val (nullable).
    fn make_schema_3col_grp_str() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        s.columns[2] = SchemaColumn {
            type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0,
        };
        s
    }

    /// 3-column reduce output schema: U128 pk (pk_index=0), I64 grp, I64 agg (nullable).
    fn make_reduce_str_out_schema() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        s.columns[2] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 1, _pad: 0 };
        s
    }

    /// Build a 3-column OwnedBatch (U64 pk, I64 grp, STRING val) from tuples.
    /// All strings must be <= 12 bytes (inline, no blob needed).
    fn make_batch_3col_grp_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64, &str)],
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, grp, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&grp.to_le_bytes());
            let bytes = val.as_bytes();
            assert!(bytes.len() <= SHORT_STRING_THRESHOLD, "use inline strings only");
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
            gs[4..4 + bytes.len()].copy_from_slice(bytes);
            b.col_data[1].extend_from_slice(&gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn make_gi_schema() -> SchemaDescriptor {
        use crate::schema::SchemaColumn;
        let mut s = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        s.columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        s
    }

    /// Build a GI OwnedBatch (U128 pk: ck_lo=source_pk_lo, ck_hi=gc_u64; I64 payload: spk_hi).
    fn make_gi_batch(rows: &[(u64, u64, i64)]) -> OwnedBatch {
        let gi_schema = make_gi_schema();
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(gi_schema, n.max(1));
        b.count = 0;
        for &(ck_lo, gc_u64, spk_hi) in rows {
            b.pk_lo.extend_from_slice(&ck_lo.to_le_bytes());
            b.pk_hi.extend_from_slice(&gc_u64.to_le_bytes());
            b.weight.extend_from_slice(&1i64.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&spk_hi.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_reduce_sum_retraction() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;
        use crate::schema::type_code;

        // Input: pk(U64), grp(I64), val(I64)
        let mut in_schema = make_schema_u64_i64();
        in_schema.num_columns = 3;
        in_schema.columns[2] = crate::schema::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };

        // Output: pk(U128), grp(I64), sum(I64)
        let mut out_schema = crate::schema::SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [crate::schema::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = crate::schema::SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = crate::schema::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        out_schema.columns[2] = crate::schema::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        // Empty trace_out
        let empty_out = Arc::new(OwnedBatch::empty(2));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // Tick 1: insert 3 rows in group 10: val=100, val=200, val=300
        let mut delta1 = OwnedBatch::with_schema(in_schema, 3);
        delta1.count = 0;
        for (pk, val) in [(1u64, 100i64), (2, 200), (3, 300)] {
            delta1.pk_lo.extend_from_slice(&pk.to_le_bytes());
            delta1.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            delta1.weight.extend_from_slice(&1i64.to_le_bytes());
            delta1.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            delta1.col_data[0].extend_from_slice(&10i64.to_le_bytes()); // grp=10
            delta1.col_data[1].extend_from_slice(&val.to_le_bytes());
            delta1.count += 1;
        }
        delta1.sorted = true;
        delta1.consolidated = true;

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AGG_SUM, col_type_code: type_code::I64, _pad: [0; 2],
        };

        let (out1, _) = op_reduce(
            &delta1, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        // SUM of (100+200+300) = 600
        assert_eq!(out1.count, 1);
        let sum1 = crate::util::read_i64_le(&out1.col_data[1], 0);
        assert_eq!(sum1, 600);

        // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
        // Need trace_out with previous aggregate
        let prev_out = Arc::new(out1);
        let mut to_ch2 = unsafe { create_cursor_from_snapshots(&[prev_out], &[], out_schema) };

        let mut delta2 = OwnedBatch::with_schema(in_schema, 1);
        delta2.count = 0;
        delta2.pk_lo.extend_from_slice(&2u64.to_le_bytes());
        delta2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        delta2.weight.extend_from_slice(&(-1i64).to_le_bytes());
        delta2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        delta2.col_data[0].extend_from_slice(&10i64.to_le_bytes());
        delta2.col_data[1].extend_from_slice(&200i64.to_le_bytes());
        delta2.count += 1;
        delta2.sorted = true;
        delta2.consolidated = true;

        let (out2, _) = op_reduce(
            &delta2, None, &mut to_ch2.cursor,
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        // Output: retract old sum (600, w=-1) + insert new sum (400, w=+1) = 2 rows
        assert_eq!(out2.count, 2);
    }

    #[test]
    fn test_reduce_count() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;
        use crate::schema::type_code;

        // Input: pk(U64), val(I64)
        let in_schema = make_schema_u64_i64();

        // Output: pk(U128), count(I64)
        let mut out_schema = crate::schema::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [crate::schema::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = crate::schema::SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = crate::schema::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // 3 rows: pk=1,2,3 all GROUP BY pk (single group using pk as group)
        let delta = make_batch(&in_schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let agg = AggDescriptor {
            col_idx: 0, agg_op: AGG_COUNT, col_type_code: type_code::I64, _pad: [0; 2],
        };

        // GROUP BY pk → each row is its own group
        let (out, _) = op_reduce(
            &delta, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        // Each pk forms its own group, COUNT=1 for each
        assert_eq!(out.count, 3);
        for i in 0..3 {
            let count = crate::util::read_i64_le(&out.col_data[0], i * 8);
            assert_eq!(count, 1, "each single-row group has count=1");
        }
    }

    /// GI path bug: same PK, two different string payloads — the `if` must be `while`.
    #[test]
    fn test_reduce_gi_same_pk_multiple_payloads() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let input_schema = make_schema_3col_grp_str();
        let output_schema = make_reduce_str_out_schema();
        let gi_schema = make_gi_schema();

        // trace_in: apple and zebra both at PK=1 (apple sorts first by payload)
        let ti_batch = Arc::new(make_batch_3col_grp_str(
            &input_schema,
            &[(1, 1, 1, "apple"), (1, 1, 1, "zebra")],
        ));

        // GI: only PK=1 → group gc_u64=1
        let gi_batch = Arc::new(make_gi_batch(&[(1, 1, 0)]));

        // trace_out: empty (no previous aggregate, no retraction emitted)
        let to_batch = Arc::new(OwnedBatch::empty(output_schema.num_columns as usize - 1));

        // delta: retract apple at PK=1
        let delta = make_batch_3col_grp_str(&input_schema, &[(1, -1, 1, "apple")]);

        let mut ti_handle =
            unsafe { create_cursor_from_snapshots(&[ti_batch], &[], input_schema) };
        let mut gi_handle =
            unsafe { create_cursor_from_snapshots(&[gi_batch], &[], gi_schema) };
        let mut to_handle =
            unsafe { create_cursor_from_snapshots(&[to_batch], &[], output_schema) };

        // MAX on STRING agg col (col_idx=2, type=STRING); no AVI
        let agg_desc = AggDescriptor {
            col_idx: 2,
            agg_op: AGG_MAX,
            col_type_code: type_code::STRING,
            _pad: [0; 2],
        };

        let (out, _fin) = op_reduce(
            &delta,
            Some(&mut ti_handle.cursor),
            &mut to_handle.cursor,
            &input_schema,
            &output_schema,
            &[1u32],            // group_by_cols: col 1 (grp)
            &[agg_desc],
            None,               // avi_cursor
            false,              // avi_for_max
            type_code::STRING,  // avi_agg_col_type_code (unused; no AVI)
            &[1u32],            // avi_group_by_cols (unused)
            None,               // avi_input_schema
            Some(&mut gi_handle.cursor), // gi_cursor
            1u32,               // gi_col_idx: grp column
            type_code::I64,     // gi_col_type_code
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
        let agg = crate::util::read_i64_le(&out.col_data[1], 0);
        assert_eq!(agg, zebra_ck,
            "MAX of {{zebra+1}} must be zebra_ck; got {agg} (apple_ck={apple_ck})");
    }

    // -----------------------------------------------------------------------
    // op_gather_reduce
    // -----------------------------------------------------------------------

    #[test]
    fn test_gather_reduce_retraction() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;
        use crate::schema::type_code;

        // Schema: pk(U128), count(I64) — same as partial/output schema
        let mut schema = crate::schema::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [crate::schema::SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = crate::schema::SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        schema.columns[1] = crate::schema::SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        // Tick 1: two partial COUNT=2 from different workers → global COUNT=4
        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], schema) };

        let mut partial1 = OwnedBatch::with_schema(schema, 2);
        partial1.count = 0;
        // Two entries for same group key (pk=1), count=2 each
        for count in [2i64, 2] {
            partial1.pk_lo.extend_from_slice(&1u64.to_le_bytes());
            partial1.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            partial1.weight.extend_from_slice(&1i64.to_le_bytes());
            partial1.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            partial1.col_data[0].extend_from_slice(&count.to_le_bytes());
            partial1.count += 1;
        }

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AGG_SUM, col_type_code: type_code::I64, _pad: [0; 2],
        };

        let out1 = op_gather_reduce(&partial1, &mut to_ch.cursor, &schema, &[agg]);
        assert_eq!(out1.count, 1);
        let global_count = crate::util::read_i64_le(&out1.col_data[0], 0);
        assert_eq!(global_count, 4);

        // Tick 2: retract 1 from each worker → partial counts are -1 each → global delta = -2
        let prev_out = Arc::new(out1);
        let mut to_ch2 = unsafe { create_cursor_from_snapshots(&[prev_out], &[], schema) };

        let mut partial2 = OwnedBatch::with_schema(schema, 2);
        partial2.count = 0;
        for count in [-1i64, -1] {
            partial2.pk_lo.extend_from_slice(&1u64.to_le_bytes());
            partial2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            partial2.weight.extend_from_slice(&1i64.to_le_bytes());
            partial2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            partial2.col_data[0].extend_from_slice(&count.to_le_bytes());
            partial2.count += 1;
        }

        let out2 = op_gather_reduce(&partial2, &mut to_ch2.cursor, &schema, &[agg]);
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
        let ord = compare_by_group_cols(&mb, 0, 1, &schema, &[1]);
        assert_eq!(ord, std::cmp::Ordering::Less);
        let ord2 = compare_by_group_cols(&mb, 1, 0, &schema, &[1]);
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
            let pi = payload_idx(1, 0); // col_idx=1, pk_index=0
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
        let cs = crate::schema::type_size(tc);
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: tc, size: cs, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch_typed_i32(schema: &SchemaDescriptor, rows: &[(u64, i64, i32)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn make_batch_typed_i16(schema: &SchemaDescriptor, rows: &[(u64, i64, i16)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_reduce_sum_i32() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let in_schema = make_schema_with_type(type_code::I32);

        // Output: pk(U128), sum(I64)
        let mut out_schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // 3 rows with I32 values, group by PK
        let delta = make_batch_typed_i32(&in_schema, &[
            (1, 1, 100i32), (2, 1, 200i32), (3, 1, -50i32),
        ]);

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AGG_SUM, col_type_code: type_code::I32, _pad: [0; 2],
        };

        // GROUP BY pk → each row is its own group
        let (out, _) = op_reduce(
            &delta, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        assert_eq!(out.count, 3);
        // Check values: row offsets depend on PK order (group_by_pk path)
        let sum0 = crate::util::read_i64_le(&out.col_data[0], 0);
        let sum1 = crate::util::read_i64_le(&out.col_data[0], 8);
        let sum2 = crate::util::read_i64_le(&out.col_data[0], 16);
        assert_eq!(sum0, 100, "SUM of I32 100");
        assert_eq!(sum1, 200, "SUM of I32 200");
        assert_eq!(sum2, -50, "SUM of I32 -50 (sign extension)");
    }

    #[test]
    fn test_reduce_min_f32() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let in_schema = make_schema_with_type(type_code::F32);

        let mut out_schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // One group: 3 F32 values
        let mut in_schema_3 = in_schema;
        in_schema_3.num_columns = 3;
        in_schema_3.columns[2] = SchemaColumn {
            type_code: type_code::F32, size: 4, nullable: 0, _pad: 0,
        };

        // Use a 3-col input schema: pk(U64), grp(I32), val(F32)
        // Simpler: just use 2-col with GROUP BY pk
        let delta = make_batch_f32(&in_schema, &[
            (1, 1, 3.5f32), (1, 1, -1.0f32), (1, 1, 7.0f32),
        ]);

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AGG_MIN, col_type_code: type_code::F32, _pad: [0; 2],
        };

        // GROUP BY pk → all 3 rows in same group
        let (out, _) = op_reduce(
            &delta, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        assert_eq!(out.count, 1);
        // MIN should be -1.0 stored as f64 bits
        let bits = u64::from_le_bytes(out.col_data[0][0..8].try_into().unwrap());
        let min_val = f64::from_bits(bits);
        assert_eq!(min_val, -1.0f64, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
    }

    #[test]
    fn test_reduce_max_i16() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let in_schema = make_schema_with_type(type_code::I16);

        let mut out_schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        out_schema.columns[0] = SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        out_schema.columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], out_schema) };

        // 3 rows with I16 values, all same PK
        let delta = make_batch_typed_i16(&in_schema, &[
            (1, 1, -100i16), (1, 1, 200i16), (1, 1, 50i16),
        ]);

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AGG_MAX, col_type_code: type_code::I16, _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta, None, &mut to_ch.cursor,
            &in_schema, &out_schema, &[0u32], &[agg],
            None, false, 0, &[], None, None, 0, 0, None, None,
        );
        assert_eq!(out.count, 1);
        let max_val = crate::util::read_i64_le(&out.col_data[0], 0);
        assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
    }

    // -----------------------------------------------------------------------
    // Fix 6: gather_reduce MIN retraction
    // -----------------------------------------------------------------------

    #[test]
    fn test_gather_reduce_min_retraction() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        // Schema: pk(U128), min_val(I64)
        let mut schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = SchemaColumn {
            type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
        };
        schema.columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 1, _pad: 0,
        };

        // Tick 1: partial MIN=5 from one worker → global MIN=5
        let empty_out = Arc::new(OwnedBatch::empty(1));
        let mut to_ch = unsafe { create_cursor_from_snapshots(&[empty_out], &[], schema) };

        let mut partial1 = OwnedBatch::with_schema(schema, 1);
        partial1.count = 0;
        partial1.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        partial1.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        partial1.weight.extend_from_slice(&1i64.to_le_bytes());
        partial1.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        partial1.col_data[0].extend_from_slice(&5i64.to_le_bytes());
        partial1.count += 1;

        let agg = AggDescriptor {
            col_idx: 1, agg_op: AGG_MIN, col_type_code: type_code::I64, _pad: [0; 2],
        };

        let out1 = op_gather_reduce(&partial1, &mut to_ch.cursor, &schema, &[agg]);
        assert_eq!(out1.count, 1);
        let min1 = crate::util::read_i64_le(&out1.col_data[0], 0);
        assert_eq!(min1, 5);

        // Tick 2: partial MIN=3 from one worker. The old global (5) should be folded in
        // via merge_accumulated with weight=1 → combine(5). New MIN should be min(3, 5) = 3.
        let prev_out = Arc::new(out1);
        let mut to_ch2 = unsafe { create_cursor_from_snapshots(&[prev_out], &[], schema) };

        let mut partial2 = OwnedBatch::with_schema(schema, 1);
        partial2.count = 0;
        partial2.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        partial2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        partial2.weight.extend_from_slice(&1i64.to_le_bytes());
        partial2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        partial2.col_data[0].extend_from_slice(&3i64.to_le_bytes());
        partial2.count += 1;

        let out2 = op_gather_reduce(&partial2, &mut to_ch2.cursor, &schema, &[agg]);
        // Should have: retract old (5, w=-1) + insert new (3, w=+1)
        assert_eq!(out2.count, 2, "should retract old MIN and emit new MIN");
        let retracted = crate::util::read_i64_le(&out2.col_data[0], 0);
        assert_eq!(retracted, 5, "retraction should be old MIN value 5");
        assert_eq!(out2.get_weight(0), -1);
        let new_min = crate::util::read_i64_le(&out2.col_data[0], 8);
        assert_eq!(new_min, 3, "new MIN should be 3 (min of old 5 and partial 3)");
        assert_eq!(out2.get_weight(1), 1);
    }
}
