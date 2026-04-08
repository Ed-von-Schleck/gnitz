//! Scalar function types for DBSP filter and map operators.
//!
//! A `Plan` cleanly separates columnar operations (column moves, null
//! permutation) from per-row operations (expression interpreter).
//! `ScalarFuncKind` wraps Plan behind a single enum so that the VM
//! passes one opaque handle for any function type.

use crate::schema::{
    type_code, SchemaDescriptor, SHORT_STRING_THRESHOLD,
};
use crate::expr::{self, EmitTarget, ExprProgram};
use crate::storage::{OwnedBatch, MemBatch};
use crate::util::read_u32_le;

// ---------------------------------------------------------------------------
// ScalarFuncKind enum
// ---------------------------------------------------------------------------

pub enum ScalarFuncKind {
    Plan(Plan),
}

// Function opcodes (must match functions.py)
const OP_EQ: u8 = 1;
const OP_GT: u8 = 2;
const OP_LT: u8 = 3;

impl ScalarFuncKind {
    pub fn kind_name(&self) -> &'static str {
        let ScalarFuncKind::Plan(_) = self;
        "plan"
    }

    /// Filter predicate: returns true if row passes.
    pub fn evaluate_predicate(
        &self,
        batch: &MemBatch,
        row: usize,
        schema: &SchemaDescriptor,
    ) -> bool {
        let ScalarFuncKind::Plan(p) = self;
        p.evaluate_predicate(batch, row, schema)
    }

    /// Batch-level map: populate output batch from input batch.
    pub fn evaluate_map_batch(
        &self,
        in_batch: &OwnedBatch,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> OwnedBatch {
        let ScalarFuncKind::Plan(p) = self;
        p.execute_map(in_batch, in_schema, out_schema)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a payload index to a schema column index, accounting for the PK gap.
#[inline]
fn payload_to_col(payload: usize, pk_index: usize) -> usize {
    if payload < pk_index { payload } else { payload + 1 }
}

/// Copy a single column from `in_batch` to `output`.
fn copy_column(
    in_batch: &OwnedBatch,
    output: &mut OwnedBatch,
    cm: &ColMove,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
) {
    let n = in_batch.count;
    let in_pki = in_schema.pk_index as usize;
    let out_pki = out_schema.pk_index as usize;

    if cm.src_ci == in_pki {
        let out_ci = payload_to_col(cm.dst_payload, out_pki);
        let stride = out_schema.columns[out_ci].size as usize;
        let dst = output.col_data_mut(cm.dst_payload);
        for row in 0..n {
            let pk_lo = in_batch.get_pk(row) as u64;
            dst[row * stride..row * stride + 8]
                .copy_from_slice(&pk_lo.to_le_bytes());
        }
    } else if cm.type_code == type_code::STRING {
        let in_pi = if cm.src_ci < in_pki { cm.src_ci } else { cm.src_ci - 1 };
        let stride = in_schema.columns[cm.src_ci].size as usize;
        let src_col = in_batch.col_data(in_pi);
        output.col_data_mut(cm.dst_payload).copy_from_slice(&src_col[..n * stride]);
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
                output.col_data_mut(cm.dst_payload)[off + 8..off + 16]
                    .copy_from_slice(&(new_offset as u64).to_le_bytes());
            }
        }
    } else {
        let in_pi = if cm.src_ci < in_pki { cm.src_ci } else { cm.src_ci - 1 };
        let stride = in_schema.columns[cm.src_ci].size as usize;
        output.col_data_mut(cm.dst_payload).copy_from_slice(&in_batch.col_data(in_pi)[..n * stride]);
    }
}

// ---------------------------------------------------------------------------
// NullPerm — columnar null bitmap permutation
// ---------------------------------------------------------------------------

pub struct NullPerm {
    pairs: Vec<(u8, u8)>,
    /// Bitmask of always-null payload positions (from EMIT_NULL).
    constant: u64,
}

impl NullPerm {
    /// Build from (src_col, dst_payload) pairs, skipping PK columns.
    fn from_col_pairs(src_cols: &[u32], dst_payloads: &[u32], null_payloads: &[u32], pk_index: u32) -> Self {
        let pki = pk_index as usize;
        let mut pairs = Vec::new();
        for k in 0..src_cols.len() {
            let src_ci = src_cols[k] as usize;
            if src_ci == pki {
                continue;
            }
            let in_pi = if src_ci < pki { src_ci } else { src_ci - 1 };
            pairs.push((in_pi as u8, dst_payloads[k] as u8));
        }
        let mut constant: u64 = 0;
        for &null_pl in null_payloads {
            constant |= 1u64 << null_pl;
        }
        NullPerm { pairs, constant }
    }

    /// Build from a projection: output payload i ← src column index.
    fn from_projection(src_indices: &[u32], pk_index: u32) -> Self {
        let dst_payloads: Vec<u32> = (0..src_indices.len() as u32).collect();
        Self::from_col_pairs(src_indices, &dst_payloads, &[], pk_index)
    }

    #[inline]
    fn apply(&self, in_null: u64) -> u64 {
        let mut out: u64 = self.constant;
        for &(src, dst) in &self.pairs {
            out |= ((in_null >> src) & 1) << dst;
        }
        out
    }

    fn apply_column(&self, in_null_bmp: &[u8], n: usize) -> Vec<u8> {
        let mut out = vec![0u8; n * 8];
        let out_arr = out.as_mut_ptr() as *mut u64;
        for row in 0..n {
            let off = row * 8;
            let in_null = u64::from_le_bytes(in_null_bmp[off..off + 8].try_into().unwrap());
            unsafe {
                *out_arr.add(row) = self.apply(in_null);
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Plan — unified representation for filter and map operations
// ---------------------------------------------------------------------------

pub struct ColMove {
    pub src_ci: usize,
    pub dst_payload: usize,
    pub type_code: u8,
}

pub enum FilterKernel {
    PassAll,
    CompareConst {
        col_idx: u32,
        op: u8,
        val_bits: u64,
        is_float: bool,
    },
    Interpreted(ExprProgram),
}

pub enum ComputeKernel {
    None,
    Interpreted {
        prog: ExprProgram,
        emit_payloads: Vec<usize>,
    },
}

pub struct Plan {
    filter: FilterKernel,
    col_moves: Vec<ColMove>,
    null_perm: NullPerm,
    compute: ComputeKernel,
}

/// Empty plan with only a filter kernel set.
fn filter_only(filter: FilterKernel) -> Plan {
    Plan {
        filter,
        col_moves: Vec::new(),
        null_perm: NullPerm { pairs: Vec::new(), constant: 0 },
        compute: ComputeKernel::None,
    }
}

impl Plan {
    /// Filter via constant comparison (replaces UniversalPredicate).
    pub fn from_compare(col_idx: u32, op: u8, val_bits: u64, is_float: bool) -> Self {
        filter_only(FilterKernel::CompareConst { col_idx, op, val_bits, is_float })
    }

    /// Filter via interpreted expression (replaces ExprPredicate).
    pub fn from_predicate(prog: ExprProgram) -> Self {
        filter_only(FilterKernel::Interpreted(prog))
    }

    /// Projection plan from source indices (replaces UniversalProjection).
    pub fn from_projection(src_indices: &[u32], src_types: &[u8], pk_index: u32) -> Self {
        let col_moves: Vec<ColMove> = src_indices
            .iter()
            .zip(src_types.iter())
            .enumerate()
            .map(|(i, (&src_ci, &tc))| ColMove {
                src_ci: src_ci as usize,
                dst_payload: i,
                type_code: tc,
            })
            .collect();
        let null_perm = NullPerm::from_projection(src_indices, pk_index);
        Plan {
            filter: FilterKernel::PassAll,
            col_moves,
            null_perm,
            compute: ComputeKernel::None,
        }
    }

    /// Map plan from ExprProgram bytecode (replaces ExprMap + MapAnalysis).
    pub fn from_map(prog: ExprProgram, pk_index: u32) -> Self {
        let mut copy_src_cols = Vec::new();
        let mut copy_out_payloads = Vec::new();
        let mut copy_type_codes = Vec::new();
        let mut null_payloads = Vec::new();
        let mut emit_payloads = Vec::new();
        let mut has_compute = false;

        for i in 0..prog.num_instrs as usize {
            let base = i * 4;
            let op = prog.code[base];
            let dst = prog.code[base + 1];
            let a1 = prog.code[base + 2];
            let a2 = prog.code[base + 3];

            if op == expr::EXPR_COPY_COL {
                copy_src_cols.push(a1 as u32);
                copy_out_payloads.push(a2 as u32);
                copy_type_codes.push(dst as u8);
            } else if op == expr::EXPR_EMIT_NULL {
                null_payloads.push(a1 as u32);
            } else {
                has_compute = true;
                if op == expr::EXPR_EMIT {
                    emit_payloads.push(a2 as usize);
                }
            }
        }

        let col_moves: Vec<ColMove> = copy_src_cols
            .iter()
            .zip(copy_out_payloads.iter())
            .zip(copy_type_codes.iter())
            .map(|((&src, &dst), &tc)| ColMove {
                src_ci: src as usize,
                dst_payload: dst as usize,
                type_code: tc,
            })
            .collect();

        let null_perm = NullPerm::from_col_pairs(
            &copy_src_cols,
            &copy_out_payloads,
            &null_payloads,
            pk_index,
        );

        let compute = if has_compute {
            ComputeKernel::Interpreted { prog, emit_payloads }
        } else {
            ComputeKernel::None
        };

        Plan {
            filter: FilterKernel::PassAll,
            col_moves,
            null_perm,
            compute,
        }
    }

    /// Evaluate predicate for a single row.
    pub fn evaluate_predicate(
        &self,
        batch: &MemBatch,
        row: usize,
        schema: &SchemaDescriptor,
    ) -> bool {
        match &self.filter {
            FilterKernel::PassAll => true,
            FilterKernel::CompareConst { col_idx, op, val_bits, is_float } => {
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
                        batch.get_pk(row) as i64
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
            FilterKernel::Interpreted(prog) => {
                let (val, is_null) =
                    expr::eval_predicate(prog, batch, row, schema.pk_index);
                !is_null && val != 0
            }
        }
    }

    /// Execute map: system column clone → col_moves → NullPerm → compute.
    pub fn execute_map(
        &self,
        in_batch: &OwnedBatch,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> OwnedBatch {
        let n = in_batch.count;
        let out_npc = out_schema.num_columns as usize - 1;
        if n == 0 {
            return OwnedBatch::empty(out_npc);
        }

        let out_pki = out_schema.pk_index as usize;
        let mut output = OwnedBatch::with_schema(*out_schema, n);
        output.count = n;

        // System columns
        output.pk_lo_data_mut().copy_from_slice(in_batch.pk_lo_data());
        output.pk_hi_data_mut().copy_from_slice(in_batch.pk_hi_data());
        output.weight_data_mut().copy_from_slice(in_batch.weight_data());

        // Column moves
        for cm in &self.col_moves {
            copy_column(in_batch, &mut output, cm, in_schema, out_schema);
        }

        // EMIT_NULL columns — already zero-filled by with_schema

        // Null bitmap (columnar)
        let null_bmp_result = self.null_perm.apply_column(in_batch.null_bmp_data(), n);
        output.null_bmp_data_mut().copy_from_slice(&null_bmp_result);

        // Compute kernel
        if let ComputeKernel::Interpreted { prog, emit_payloads } = &self.compute {
            let mut emit_targets: Vec<EmitTarget> = Vec::with_capacity(emit_payloads.len());
            for &out_payload in emit_payloads {
                let out_ci = payload_to_col(out_payload, out_pki);
                let stride = out_schema.columns[out_ci].size as usize;
                // Column is already zero-filled; get raw pointer for emit
                let base = output.col_data_mut(out_payload).as_mut_ptr();
                emit_targets.push(EmitTarget { base, stride, payload_col: out_payload });
            }

            let in_mb = in_batch.as_mem_batch();
            let null_arr = output.null_bmp_data_mut().as_mut_ptr() as *mut u64;
            for row in 0..n {
                let in_null = in_batch.get_null_word(row);
                let (_, _, mask) = expr::eval_with_emit(
                    prog, &in_mb, row, in_schema.pk_index, in_null, &emit_targets,
                );
                unsafe {
                    *null_arr.add(row) |= mask;
                }
            }
        }

        output.sorted = false;
        output.consolidated = false;
        output
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaColumn;
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
        let mut batch = OwnedBatch::with_schema(*schema, rows.len().max(1));
        for &(pk, weight, null_word, cols) in rows {
            batch.extend_pk_lo(&pk.to_le_bytes());
            batch.extend_pk_hi(&0u64.to_le_bytes());
            batch.extend_weight(&weight.to_le_bytes());
            batch.extend_null_bmp(&null_word.to_le_bytes());
            let mut pi = 0;
            for ci in 0..schema.num_columns as usize {
                if ci == schema.pk_index as usize {
                    continue;
                }
                if pi < cols.len() {
                    batch.extend_col(pi, &cols[pi].to_le_bytes());
                }
                pi += 1;
            }
            batch.count += 1;
        }
        batch
    }

    #[test]
    fn test_compare_predicate_int() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[
            (1, 1, 0, &[42]),
            (2, 1, 0, &[10]),
        ]);
        let mb = batch.as_mem_batch();

        let func = ScalarFuncKind::Plan(Plan::from_compare(1, OP_EQ, 42u64, false));
        assert!(func.evaluate_predicate(&mb, 0, &schema));
        assert!(!func.evaluate_predicate(&mb, 1, &schema));
    }

    #[test]
    fn test_projection_batch() {
        let in_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let out_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let batch = make_int_batch(&in_schema, &[
            (1, 1, 0, &[10, 20]),
            (2, 1, 0, &[30, 40]),
        ]);

        let func = ScalarFuncKind::Plan(Plan::from_projection(
            &[2, 1],
            &[type_code::I64, type_code::I64],
            in_schema.pk_index,
        ));
        let result = func.evaluate_map_batch(&batch, &in_schema, &out_schema);
        assert_eq!(result.count, 2);

        let r0_col0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        let r0_col1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(r0_col0, 20);
        assert_eq!(r0_col1, 10);
    }

    #[test]
    fn test_map_copy_and_emit() {
        let in_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let out_schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);

        let batch = make_int_batch(&in_schema, &[
            (1, 1, 0, &[10, 20]),
        ]);

        let code = vec![
            expr::EXPR_COPY_COL, type_code::I64 as i64, 1, 0,
            expr::EXPR_LOAD_COL_INT, 0, 1, 0,
            expr::EXPR_LOAD_COL_INT, 1, 2, 0,
            expr::EXPR_INT_ADD, 2, 0, 1,
            expr::EXPR_EMIT, 0, 2, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);

        let func = ScalarFuncKind::Plan(Plan::from_map(prog, in_schema.pk_index));
        let result = func.evaluate_map_batch(&batch, &in_schema, &out_schema);
        assert_eq!(result.count, 1);

        let v0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(v0, 10);
        let v1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(v1, 30);
    }

    #[test]
    fn test_empty_batch() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = OwnedBatch::empty(1);

        let func = ScalarFuncKind::Plan(Plan::from_projection(
            &[1], &[type_code::I64], schema.pk_index,
        ));
        let result = func.evaluate_map_batch(&batch, &schema, &schema);
        assert_eq!(result.count, 0);
    }
}
