//! Scalar function enum: predicates and projections for DBSP operators.
//!
//! Wraps ExprProgram and universal predicate/projection behind a single enum
//! so that FFI passes one opaque handle for any function type.

use crate::compact::{
    type_code, SchemaDescriptor, SHORT_STRING_THRESHOLD,
};
use crate::expr::{self, EmitTarget, ExprProgram};
use crate::memtable::OwnedBatch;
use crate::merge::MemBatch;
use crate::util::read_u32_le;

// ---------------------------------------------------------------------------
// MapAnalysis — pre-extracted metadata from ExprMap bytecode
// ---------------------------------------------------------------------------

pub struct MapAnalysis {
    pub copy_src_cols: Vec<u32>,
    pub copy_out_payloads: Vec<u32>,
    pub copy_type_codes: Vec<u8>,
    pub null_payloads: Vec<u32>,
    pub emit_out_payloads: Vec<u32>,
    pub has_compute: bool,
}

impl MapAnalysis {
    pub fn from_program(prog: &ExprProgram) -> Self {
        let mut copy_src = Vec::new();
        let mut copy_out = Vec::new();
        let mut copy_tc = Vec::new();
        let mut null_pl = Vec::new();
        let mut emit_out = Vec::new();
        let mut has_compute = false;

        for i in 0..prog.num_instrs as usize {
            let base = i * 4;
            let op = prog.code[base];
            let dst = prog.code[base + 1];
            let a1 = prog.code[base + 2];
            let a2 = prog.code[base + 3];

            if op == expr::EXPR_COPY_COL {
                copy_src.push(a1 as u32);
                copy_out.push(a2 as u32);
                copy_tc.push(dst as u8);
            } else if op == expr::EXPR_EMIT_NULL {
                null_pl.push(a1 as u32);
            } else {
                has_compute = true;
                if op == expr::EXPR_EMIT {
                    emit_out.push(a2 as u32);
                }
            }
        }

        MapAnalysis {
            copy_src_cols: copy_src,
            copy_out_payloads: copy_out,
            copy_type_codes: copy_tc,
            null_payloads: null_pl,
            emit_out_payloads: emit_out,
            has_compute,
        }
    }
}

// ---------------------------------------------------------------------------
// ScalarFuncKind enum
// ---------------------------------------------------------------------------

pub enum ScalarFuncKind {
    Null,
    ExprPredicate(ExprProgram),
    ExprMap {
        program: ExprProgram,
        analysis: MapAnalysis,
    },
    UniversalPredicate {
        col_idx: u32,
        op: u8,
        val_bits: u64,
        is_float: bool,
    },
    UniversalProjection {
        src_indices: Vec<u32>,
        src_types: Vec<u8>,
    },
}

// Function opcodes (must match functions.py)
const OP_EQ: u8 = 1;
const OP_GT: u8 = 2;
const OP_LT: u8 = 3;

impl ScalarFuncKind {
    pub fn kind_name(&self) -> &'static str {
        match self {
            ScalarFuncKind::Null => "null",
            ScalarFuncKind::ExprPredicate(_) => "expr_pred",
            ScalarFuncKind::ExprMap { .. } => "expr_map",
            ScalarFuncKind::UniversalPredicate { .. } => "univ_pred",
            ScalarFuncKind::UniversalProjection { .. } => "univ_proj",
        }
    }

    /// Filter predicate: returns true if row passes.
    pub fn evaluate_predicate(
        &self,
        batch: &MemBatch,
        row: usize,
        schema: &SchemaDescriptor,
    ) -> bool {
        match self {
            ScalarFuncKind::Null => true,
            ScalarFuncKind::ExprPredicate(prog) => {
                let (val, is_null) =
                    expr::eval_predicate(prog, batch, row, schema.pk_index);
                !is_null && val != 0
            }
            ScalarFuncKind::UniversalPredicate {
                col_idx,
                op,
                val_bits,
                is_float,
            } => {
                let ci = *col_idx as usize;
                let pki = schema.pk_index as usize;
                if *is_float {
                    let pi = if ci < pki { ci } else { ci - 1 };
                    let ptr = batch.get_col_ptr(row, pi, 8);
                    let bits = i64::from_le_bytes(ptr.try_into().unwrap());
                    let val = f64::from_ne_bytes(bits.to_ne_bytes());
                    let con = f64::from_ne_bytes((*val_bits as i64).to_ne_bytes());
                    match *op {
                        OP_EQ => val == con,
                        OP_GT => val > con,
                        OP_LT => val < con,
                        _ => true,
                    }
                } else {
                    let raw = if ci == pki {
                        batch.get_pk_lo(row) as i64
                    } else {
                        let pi = if ci < pki { ci } else { ci - 1 };
                        let ptr = batch.get_col_ptr(row, pi, 8);
                        i64::from_le_bytes(ptr.try_into().unwrap())
                    };
                    let con = *val_bits as i64;
                    match *op {
                        OP_EQ => raw == con,
                        OP_GT => raw > con,
                        OP_LT => raw < con,
                        _ => true,
                    }
                }
            }
            _ => true,
        }
    }

    /// Batch-level map: populate output batch from input batch.
    pub fn evaluate_map_batch(
        &self,
        in_batch: &OwnedBatch,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> OwnedBatch {
        match self {
            ScalarFuncKind::ExprMap { program, analysis } => {
                expr_map_batch(in_batch, in_schema, out_schema, program, analysis)
            }
            ScalarFuncKind::UniversalProjection {
                src_indices,
                src_types,
            } => universal_projection_batch(in_batch, in_schema, out_schema, src_indices, src_types),
            _ => {
                // Null/predicate variants: identity copy
                in_batch.clone_batch()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ExprMap batch implementation (6-phase)
// ---------------------------------------------------------------------------

fn expr_map_batch(
    in_batch: &OwnedBatch,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    program: &ExprProgram,
    analysis: &MapAnalysis,
) -> OwnedBatch {
    let n = in_batch.count;
    if n == 0 {
        let out_npc = out_schema.num_columns as usize - 1;
        return OwnedBatch::empty(out_npc);
    }

    let out_npc = out_schema.num_columns as usize - 1;
    let in_pki = in_schema.pk_index as usize;
    let out_pki = out_schema.pk_index as usize;

    let mut output = OwnedBatch::empty(out_npc);

    // Phase 1: system columns — copy pk_lo, pk_hi, weight
    output.pk_lo = in_batch.pk_lo.clone();
    output.pk_hi = in_batch.pk_hi.clone();
    output.weight = in_batch.weight.clone();
    output.null_bmp = vec![0u8; n * 8]; // will be filled in phase 5
    output.count = n;

    // Phase 2: COPY_COL columns
    for k in 0..analysis.copy_src_cols.len() {
        let src_ci = analysis.copy_src_cols[k] as usize;
        let out_payload = analysis.copy_out_payloads[k] as usize;
        let tc = analysis.copy_type_codes[k];

        if src_ci == in_pki {
            // Copy PK lo values to a payload column
            let out_ci = if out_payload < out_pki {
                out_payload
            } else {
                out_payload + 1
            };
            let stride = out_schema.columns[out_ci].size as usize;
            let mut buf = vec![0u8; n * stride];
            for row in 0..n {
                let pk_lo = in_batch.get_pk_lo(row);
                buf[row * stride..row * stride + 8]
                    .copy_from_slice(&pk_lo.to_le_bytes());
            }
            output.col_data[out_payload] = buf;
        } else if tc == type_code::STRING {
            let in_pi = if src_ci < in_pki { src_ci } else { src_ci - 1 };
            let stride = in_schema.columns[src_ci].size as usize;
            // Copy struct bytes, relocate blobs
            let src_col = &in_batch.col_data[in_pi];
            let mut dest_col = src_col[..n * stride].to_vec();
            for row in 0..n {
                let off = row * stride;
                let src_struct = &src_col[off..off + stride];
                let length = read_u32_le(src_struct, 0) as usize;
                if length > SHORT_STRING_THRESHOLD {
                    let old_offset =
                        u64::from_le_bytes(src_struct[8..16].try_into().unwrap()) as usize;
                    let src_data = &in_batch.blob[old_offset..old_offset + length];
                    let new_offset = output.blob.len();
                    output.blob.extend_from_slice(src_data);
                    dest_col[off + 8..off + 16]
                        .copy_from_slice(&(new_offset as u64).to_le_bytes());
                }
            }
            output.col_data[out_payload] = dest_col;
        } else {
            let in_pi = if src_ci < in_pki { src_ci } else { src_ci - 1 };
            let stride = in_schema.columns[src_ci].size as usize;
            output.col_data[out_payload] = in_batch.col_data[in_pi][..n * stride].to_vec();
        }
    }

    // Phase 3: EMIT_NULL columns — zero-fill
    for &null_pl in &analysis.null_payloads {
        let out_payload = null_pl as usize;
        let out_ci = if out_payload < out_pki {
            out_payload
        } else {
            out_payload + 1
        };
        let stride = out_schema.columns[out_ci].size as usize;
        output.col_data[out_payload] = vec![0u8; n * stride];
    }

    // Phase 4: EMIT columns — pre-allocate, per-row compute
    let emit_count = analysis.emit_out_payloads.len();
    let mut emit_targets: Vec<EmitTarget> = Vec::with_capacity(emit_count);

    if analysis.has_compute {
        for &epl in &analysis.emit_out_payloads {
            let out_payload = epl as usize;
            let out_ci = if out_payload < out_pki {
                out_payload
            } else {
                out_payload + 1
            };
            let stride = out_schema.columns[out_ci].size as usize;
            output.col_data[out_payload] = vec![0u8; n * stride];
            let base = output.col_data[out_payload].as_mut_ptr();
            emit_targets.push(EmitTarget {
                base,
                stride,
                payload_col: out_payload,
            });
        }
    }

    // Phase 5: Null bitmap + per-row compute
    let in_mb = in_batch.as_mem_batch();
    let null_arr = output.null_bmp.as_mut_ptr() as *mut u64;

    for row in 0..n {
        let in_null = in_batch.get_null_word(row);
        let mut out_null: u64 = 0;

        // COPY_COL null bits
        for k in 0..analysis.copy_src_cols.len() {
            let src_ci = analysis.copy_src_cols[k] as usize;
            if src_ci == in_pki {
                continue;
            }
            let in_pi = if src_ci < in_pki { src_ci } else { src_ci - 1 };
            let out_payload = analysis.copy_out_payloads[k] as usize;
            out_null |= ((in_null >> in_pi) & 1) << out_payload;
        }

        // EMIT_NULL bits
        for &null_pl in &analysis.null_payloads {
            out_null |= 1u64 << null_pl;
        }

        // Execute compute
        if analysis.has_compute {
            let (_, _, mask) = expr::eval_with_emit(
                program,
                &in_mb,
                row,
                in_schema.pk_index,
                in_null,
                &emit_targets,
            );
            out_null |= mask;
        }

        unsafe {
            *null_arr.add(row) = out_null;
        }
    }

    output.sorted = false;
    output.consolidated = false;
    output
}

// ---------------------------------------------------------------------------
// UniversalProjection batch implementation
// ---------------------------------------------------------------------------

fn universal_projection_batch(
    in_batch: &OwnedBatch,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    src_indices: &[u32],
    src_types: &[u8],
) -> OwnedBatch {
    let n = in_batch.count;
    let out_npc = out_schema.num_columns as usize - 1;
    if n == 0 {
        return OwnedBatch::empty(out_npc);
    }

    let in_pki = in_schema.pk_index as usize;
    let out_pki = out_schema.pk_index as usize;

    let mut output = OwnedBatch::empty(out_npc);

    // System columns
    output.pk_lo = in_batch.pk_lo.clone();
    output.pk_hi = in_batch.pk_hi.clone();
    output.weight = in_batch.weight.clone();
    output.null_bmp = vec![0u8; n * 8];
    output.count = n;

    // Payload columns
    for (i, &src_ci_u32) in src_indices.iter().enumerate() {
        let src_ci = src_ci_u32 as usize;
        let out_pi = i;
        let tc = src_types[i];

        if src_ci == in_pki {
            // Copy PK lo to payload column
            let out_ci = if out_pi < out_pki { out_pi } else { out_pi + 1 };
            let stride = out_schema.columns[out_ci].size as usize;
            let mut buf = vec![0u8; n * stride];
            for row in 0..n {
                let pk_lo = in_batch.get_pk_lo(row);
                buf[row * stride..row * stride + 8]
                    .copy_from_slice(&pk_lo.to_le_bytes());
            }
            output.col_data[out_pi] = buf;
        } else if tc == type_code::STRING {
            let in_pi = if src_ci < in_pki { src_ci } else { src_ci - 1 };
            let stride = in_schema.columns[src_ci].size as usize;
            let src_col = &in_batch.col_data[in_pi];
            let mut dest_col = src_col[..n * stride].to_vec();
            for row in 0..n {
                let off = row * stride;
                let src_struct = &src_col[off..off + stride];
                let length = read_u32_le(src_struct, 0) as usize;
                if length > SHORT_STRING_THRESHOLD {
                    let old_offset =
                        u64::from_le_bytes(src_struct[8..16].try_into().unwrap()) as usize;
                    let src_data = &in_batch.blob[old_offset..old_offset + length];
                    let new_offset = output.blob.len();
                    output.blob.extend_from_slice(src_data);
                    dest_col[off + 8..off + 16]
                        .copy_from_slice(&(new_offset as u64).to_le_bytes());
                }
            }
            output.col_data[out_pi] = dest_col;
        } else {
            let in_pi = if src_ci < in_pki { src_ci } else { src_ci - 1 };
            let stride = in_schema.columns[src_ci].size as usize;
            output.col_data[out_pi] = in_batch.col_data[in_pi][..n * stride].to_vec();
        }
    }

    // Null bitmap
    let null_arr = output.null_bmp.as_mut_ptr() as *mut u64;
    for row in 0..n {
        let in_null = in_batch.get_null_word(row);
        let mut out_null: u64 = 0;
        for (i, &src_ci_u32) in src_indices.iter().enumerate() {
            let src_ci = src_ci_u32 as usize;
            if src_ci == in_pki {
                continue;
            }
            let in_pi = if src_ci < in_pki { src_ci } else { src_ci - 1 };
            out_null |= ((in_null >> in_pi) & 1) << i;
        }
        unsafe {
            *null_arr.add(row) = out_null;
        }
    }

    output.sorted = false;
    output.consolidated = false;
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::SchemaColumn;
    use crate::expr;

    fn make_schema(num_cols: u32, pk_index: u32, col_types: &[(u8, u8)]) -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        for (i, &(tc, sz)) in col_types.iter().enumerate() {
            columns[i] = SchemaColumn {
                type_code: tc,
                size: sz,
                nullable: if i == pk_index as usize { 0 } else { 1 },
                _pad: 0,
            };
        }
        SchemaDescriptor {
            num_columns: num_cols,
            pk_index,
            columns,
        }
    }

    fn make_int_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, u64, &[i64])],
    ) -> OwnedBatch {
        let npc = schema.num_columns as usize - 1;
        let mut batch = OwnedBatch::empty(npc);
        batch.schema = Some(*schema);
        for &(pk, weight, null_word, cols) in rows {
            batch.pk_lo.extend_from_slice(&pk.to_le_bytes());
            batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            batch.weight.extend_from_slice(&weight.to_le_bytes());
            batch.null_bmp.extend_from_slice(&null_word.to_le_bytes());
            let mut pi = 0;
            for ci in 0..schema.num_columns as usize {
                if ci == schema.pk_index as usize {
                    continue;
                }
                if pi < cols.len() {
                    batch.col_data[pi].extend_from_slice(&cols[pi].to_le_bytes());
                }
                pi += 1;
            }
            batch.count += 1;
        }
        batch
    }

    #[test]
    fn test_universal_predicate_int() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[
            (1, 1, 0, &[42]),
            (2, 1, 0, &[10]),
        ]);
        let mb = batch.as_mem_batch();

        let func = ScalarFuncKind::UniversalPredicate {
            col_idx: 1,
            op: OP_EQ,
            val_bits: 42u64,
            is_float: false,
        };
        assert!(func.evaluate_predicate(&mb, 0, &schema));
        assert!(!func.evaluate_predicate(&mb, 1, &schema));
    }

    #[test]
    fn test_universal_projection_batch() {
        // 3 cols: PK(U64), col1(I64), col2(I64). Project: [col2, col1]
        let in_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        // Output: PK(U64), out0=col2, out1=col1
        let out_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let batch = make_int_batch(&in_schema, &[
            (1, 1, 0, &[10, 20]),
            (2, 1, 0, &[30, 40]),
        ]);

        let func = ScalarFuncKind::UniversalProjection {
            src_indices: vec![2, 1],
            src_types: vec![type_code::I64, type_code::I64],
        };
        let result = func.evaluate_map_batch(&batch, &in_schema, &out_schema);
        assert_eq!(result.count, 2);

        // Row 0: col2 was 20, col1 was 10 → output should be [20, 10]
        let r0_col0 = i64::from_le_bytes(result.col_data[0][0..8].try_into().unwrap());
        let r0_col1 = i64::from_le_bytes(result.col_data[1][0..8].try_into().unwrap());
        assert_eq!(r0_col0, 20);
        assert_eq!(r0_col1, 10);
    }

    #[test]
    fn test_expr_map_batch_copy_and_emit() {
        // Schema: PK(U64), col1(I64), col2(I64)
        let in_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        // Output: PK(U64), out0=copy(col1), out1=emit(col1+col2)
        let out_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);

        let batch = make_int_batch(&in_schema, &[
            (1, 1, 0, &[10, 20]),
        ]);

        // Bytecode: COPY_COL col1→payload0, then compute col1+col2 and EMIT→payload1
        let code = vec![
            expr::EXPR_COPY_COL, type_code::I64 as i64, 1, 0, // copy col1 → out payload 0
            expr::EXPR_LOAD_COL_INT, 0, 1, 0, // r0 = col1
            expr::EXPR_LOAD_COL_INT, 1, 2, 0, // r1 = col2
            expr::EXPR_INT_ADD, 2, 0, 1,       // r2 = r0 + r1
            expr::EXPR_EMIT, 0, 2, 1,          // emit r2 → out payload 1
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let analysis = MapAnalysis::from_program(&prog);

        let func = ScalarFuncKind::ExprMap { program: prog, analysis };
        let result = func.evaluate_map_batch(&batch, &in_schema, &out_schema);
        assert_eq!(result.count, 1);

        // out payload 0 = copy of col1 = 10
        let v0 = i64::from_le_bytes(result.col_data[0][0..8].try_into().unwrap());
        assert_eq!(v0, 10);
        // out payload 1 = col1 + col2 = 30
        let v1 = i64::from_le_bytes(result.col_data[1][0..8].try_into().unwrap());
        assert_eq!(v1, 30);
    }

    #[test]
    fn test_empty_batch() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = OwnedBatch::empty(1);

        let func = ScalarFuncKind::UniversalProjection {
            src_indices: vec![1],
            src_types: vec![type_code::I64],
        };
        let result = func.evaluate_map_batch(&batch, &schema, &schema);
        assert_eq!(result.count, 0);
    }
}
