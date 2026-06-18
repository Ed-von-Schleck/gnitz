//! Scalar function types for DBSP filter and map operators.
//!
//! A `Plan` cleanly separates columnar operations (column moves, null
//! permutation) from per-row operations (expression interpreter).
//! `ScalarFuncKind` wraps Plan behind a single enum so that the VM
//! passes one opaque handle for any function type.

use std::cell::RefCell;

use super::batch::{eval_batch, EvalScratch, MORSEL, NULL_WORDS_PER_REG};
use super::program::{self as expr, ExprProgram, OutputColKind};
use crate::schema::{SchemaDescriptor, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{Batch, MemBatch};

// ---------------------------------------------------------------------------
// ScalarFuncKind enum
// ---------------------------------------------------------------------------

#[allow(private_interfaces)]
pub enum ScalarFuncKind {
    Plan(Plan),
}

impl ScalarFuncKind {
    pub fn kind_name(&self) -> &'static str {
        let ScalarFuncKind::Plan(_) = self;
        "plan"
    }

    /// Filter predicate: returns true if row passes. Test-only helper used by
    /// the batch evaluator's regression tests at m=1; the production path
    /// always goes through `run_filter`.
    #[cfg(test)]
    pub(crate) fn evaluate_predicate(&self, batch: &MemBatch, row: usize) -> bool {
        let ScalarFuncKind::Plan(p) = self;
        p.evaluate_predicate(batch, row)
    }

    /// Run the filter over all `n` rows, invoking `append_range(start, end)`
    /// for each contiguous run of passing rows. The closure must not touch
    /// the `Plan` (a `RefCell` borrow on `scratch` is held while it runs).
    pub fn run_filter<F: FnMut(usize, usize)>(&self, mb: &MemBatch, n: usize, append_range: F) {
        let ScalarFuncKind::Plan(p) = self;
        p.run_filter(mb, n, append_range);
    }

    /// Batch-level map: populate output batch from input batch.
    pub fn evaluate_map_batch(&self, in_batch: &Batch, out_schema: &SchemaDescriptor) -> Batch {
        let ScalarFuncKind::Plan(p) = self;
        p.execute_map(in_batch, out_schema)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output`. `cm.src_pi` holds the
/// resolved payload byte: `SENTINEL` indicates the PK column, otherwise it is
/// the dense payload index in the input batch. `cm.stride` is precomputed at
/// construction time — the byte width of the destination column. `blob_cache` is
/// shared across all ColMoves of one `execute_map` (and pooled across ticks via
/// `BlobCacheGuard`) so identical STRING/BLOB heap spans are appended to the
/// output blob at most once. It is `None` only when the output schema has no
/// German-string column, in which case the STRING/BLOB branch is never reached.
fn copy_column(
    in_batch: &Batch,
    output: &mut Batch,
    cm: &ColMove,
    mut blob_cache: Option<&mut crate::schema::BlobCache>,
) {
    let n = in_batch.count;
    let stride = cm.stride as usize;

    if cm.src_pi == PAYLOAD_MAPPING_PK_SENTINEL {
        // PK region holds OPK bytes; decode the addressed column back to native
        // LE before writing it into the payload. A raw byte copy would be wrong
        // for signed (sign-flipped) and big-endian-encoded columns.
        let dst = output.col_data_mut(cm.dst_payload);
        let pk_off = cm.pk_byte_offset as usize;
        let mut le = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..n {
            let opk = in_batch.get_pk_bytes(row);
            gnitz_wire::decode_pk_column(&opk[pk_off..pk_off + stride], cm.type_code, &mut le[..stride]);
            dst[row * stride..row * stride + stride].copy_from_slice(&le[..stride]);
        }
    } else if gnitz_wire::is_german_string(cm.type_code) {
        // STRING and BLOB share the 16-byte German-string struct: a long
        // (out-of-line) value's heap-offset field points into the source batch's
        // blob region, so the bytes must be relocated into the output blob or the
        // offsets dangle once the source batch is dropped. The shared BlobCache
        // deduplicates identical spans across all columns/rows of this MAP.
        let in_pi = cm.src_pi as usize;
        let src_col = in_batch.col_data(in_pi);
        for row in 0..n {
            let off = row * stride;
            let cell = crate::schema::relocate_german_string_vec(
                &src_col[off..off + stride],
                &in_batch.blob,
                &mut output.blob,
                blob_cache.as_deref_mut(),
            );
            output.col_data_mut(cm.dst_payload)[off..off + 16].copy_from_slice(&cell);
        }
    } else {
        let in_pi = cm.src_pi as usize;
        debug_assert!(
            n * stride <= in_batch.col_data(in_pi).len(),
            "copy_column: n*stride ({}*{}={}) > in_batch.col_data({}).len()={} \
             (batch count={}, payload cols={})",
            n,
            stride,
            n * stride,
            in_pi,
            in_batch.col_data(in_pi).len(),
            in_batch.count,
            in_batch.num_payload_cols(),
        );
        output
            .col_data_mut(cm.dst_payload)
            .copy_from_slice(&in_batch.col_data(in_pi)[..n * stride]);
    }
}

// ---------------------------------------------------------------------------
// NullPerm — columnar null bitmap permutation
// ---------------------------------------------------------------------------

struct NullPerm {
    pairs: Vec<(u8, u8)>,
    /// Bitmask of always-null payload positions (from EMIT_NULL).
    constant: u64,
}

impl NullPerm {
    /// Build from (src_pi_byte, dst_payload) pairs. `src_pi_bytes` holds
    /// resolved payload bytes — `SENTINEL` for the PK column (skipped here,
    /// since the PK has no null bit), otherwise the dense payload index.
    fn from_col_pairs(src_pi_bytes: &[u8], dst_payloads: &[u32], null_payloads: &[u32]) -> Self {
        let mut pairs = Vec::new();
        for k in 0..src_pi_bytes.len() {
            let src = src_pi_bytes[k];
            if src == PAYLOAD_MAPPING_PK_SENTINEL {
                continue;
            }
            pairs.push((src, dst_payloads[k] as u8));
        }
        let mut constant: u64 = 0;
        for &null_pl in null_payloads {
            constant |= 1u64 << null_pl;
        }
        NullPerm { pairs, constant }
    }

    /// Build from a projection: output payload i ← src column at payload byte.
    fn from_projection(src_pi_bytes: &[u8]) -> Self {
        let dst_payloads: Vec<u32> = (0..src_pi_bytes.len() as u32).collect();
        Self::from_col_pairs(src_pi_bytes, &dst_payloads, &[])
    }

    #[inline]
    fn apply(&self, in_null: u64) -> u64 {
        let mut out: u64 = self.constant;
        for &(src, dst) in &self.pairs {
            out |= ((in_null >> src) & 1) << dst;
        }
        out
    }

    /// Write the permuted null bitmap directly into `out` (one u64 per row).
    /// `out.len()` must be `n * 8`.
    fn apply_column_into(&self, in_null_bmp: &[u8], out: &mut [u8], n: usize) {
        debug_assert_eq!(out.len(), n * 8);
        for row in 0..n {
            let off = row * 8;
            let in_null = u64::from_le_bytes(in_null_bmp[off..off + 8].try_into().unwrap());
            out[off..off + 8].copy_from_slice(&self.apply(in_null).to_le_bytes());
        }
    }
}

// ---------------------------------------------------------------------------
// Plan — unified representation for filter and map operations
// ---------------------------------------------------------------------------

struct ColMove {
    /// Resolved payload byte for the source: `SENTINEL` for the PK column,
    /// otherwise the dense payload index in the input batch.
    src_pi: u8,
    /// Dense payload index in the output batch.
    dst_payload: usize,
    /// Type code of the column (used to dispatch the string path and to decode
    /// the OPK bytes of a PK source column).
    type_code: u8,
    /// Precomputed byte width of the destination column. For non-PK copies
    /// this equals the input column's stride (same type). For PK→payload copies
    /// the destination carries the same type as the source PK column
    /// (`build_reindex_program` emits the source type code), so the stride —
    /// taken from the *output* schema — equals the source OPK column's width.
    stride: u8,
    /// Byte offset of this column within the OPK PK region. Valid only when
    /// `src_pi == PAYLOAD_MAPPING_PK_SENTINEL`; 0 for single-column PKs.
    pk_byte_offset: u8,
}

enum FilterKernel {
    PassAll,
    Interpreted {
        prog: ExprProgram,
        /// Precomputed `prog.is_strictly_non_nullable(in_schema)`.
        no_nulls: bool,
    },
}

#[allow(clippy::large_enum_variant)]
enum ComputeKernel {
    None,
    Interpreted {
        prog: ExprProgram,
        emit_payloads: Vec<usize>,
        emit_regs: Vec<usize>,
        /// Output byte width per EMIT target, parallel to `emit_payloads`.
        emit_strides: Vec<u8>,
        /// Precomputed `prog.is_strictly_non_nullable(in_schema)`.
        no_nulls: bool,
    },
}

pub(crate) struct Plan {
    filter: FilterKernel,
    col_moves: Vec<ColMove>,
    null_perm: NullPerm,
    compute: ComputeKernel,
    scratch: RefCell<EvalScratch>,
}

fn filter_only(filter: FilterKernel) -> Plan {
    Plan {
        filter,
        col_moves: Vec::new(),
        null_perm: NullPerm {
            pairs: Vec::new(),
            constant: 0,
        },
        compute: ComputeKernel::None,
        scratch: RefCell::new(EvalScratch::new()),
    }
}

/// Stride for a single ColMove. For non-PK copies (`src_pi != SENTINEL`),
/// source and destination strides are equal — the column is moved, not
/// reshaped. For PK→payload copies (`src_pi == SENTINEL`), the destination
/// payload column must carry the same type as the source PK column:
/// `build_reindex_program` emits the source type code and `reindex_output_schema`
/// places the same type in the output payload, so the stride from `out_schema`
/// is always identical to the source OPK column's byte width. Reading more
/// bytes than the OPK column occupies would alias the next PK column.
fn col_move_stride(src_pi: u8, dst_payload: usize, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> u8 {
    if src_pi == PAYLOAD_MAPPING_PK_SENTINEL {
        let out_ci = out_schema.payload_col_idx(dst_payload);
        out_schema.columns[out_ci].size()
    } else {
        let src_ci = in_schema.payload_col_idx(src_pi as usize);
        in_schema.columns[src_ci].size()
    }
}

impl Plan {
    /// Filter via interpreted expression.
    pub fn from_predicate(mut prog: ExprProgram, schema: &SchemaDescriptor) -> Self {
        prog.resolve_column_indices(schema);
        prog.classify(true);
        let no_nulls = prog.is_strictly_non_nullable(schema);
        filter_only(FilterKernel::Interpreted { prog, no_nulls })
    }

    /// Projection plan from source indices.
    pub fn from_projection(
        src_indices: &[u32],
        src_types: &[u8],
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Self {
        let src_pi_bytes: Vec<u8> = src_indices
            .iter()
            .map(|&ci| in_schema.payload_mapping_byte(ci as usize))
            .collect();
        let col_moves: Vec<ColMove> = src_indices
            .iter()
            .zip(src_pi_bytes.iter())
            .zip(src_types.iter())
            .enumerate()
            .map(|(i, ((&ci, &src_pi), &tc))| {
                let pk_byte_offset = if src_pi == PAYLOAD_MAPPING_PK_SENTINEL {
                    in_schema.pk_byte_offset(ci as usize)
                } else {
                    0u8
                };
                ColMove {
                    src_pi,
                    dst_payload: i,
                    type_code: tc,
                    stride: col_move_stride(src_pi, i, in_schema, out_schema),
                    pk_byte_offset,
                }
            })
            .collect();
        let null_perm = NullPerm::from_projection(&src_pi_bytes);
        Plan {
            filter: FilterKernel::PassAll,
            col_moves,
            null_perm,
            compute: ComputeKernel::None,
            scratch: RefCell::new(EvalScratch::new()),
        }
    }

    /// Map plan from ExprProgram bytecode.
    pub fn from_map(mut prog: ExprProgram, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Self {
        prog.resolve_column_indices(in_schema);
        // After resolve_column_indices, EXPR_COPY_COL.a1 already holds the
        // resolved payload byte (SENTINEL for PK, dense payload index otherwise).
        let mut copy_src_pi_bytes: Vec<u8> = Vec::new();
        let mut copy_pk_byte_offsets: Vec<u8> = Vec::new();
        let mut copy_out_payloads: Vec<u32> = Vec::new();
        let mut copy_type_codes: Vec<u8> = Vec::new();
        let mut null_payloads: Vec<u32> = Vec::new();
        let mut emit_payloads: Vec<usize> = Vec::new();
        let mut emit_regs: Vec<usize> = Vec::new();
        let mut has_compute = false;

        for i in 0..prog.num_instrs as usize {
            let base = i * 4;
            let op = prog.code[base];
            let dst = prog.code[base + 1];
            let a1 = prog.code[base + 2];
            let a2 = prog.code[base + 3];

            if op == expr::EXPR_COPY_COL {
                // After resolve, a1 is a payload index (>= 0) or, for a PK
                // source column, `-(pk_byte_offset) - 1` (negative sentinel).
                let (src_pi, pk_off) = if a1 < 0 {
                    (PAYLOAD_MAPPING_PK_SENTINEL, (-a1 - 1) as u8)
                } else {
                    (a1 as u8, 0u8)
                };
                copy_src_pi_bytes.push(src_pi);
                copy_pk_byte_offsets.push(pk_off);
                copy_out_payloads.push(a2 as u32);
                copy_type_codes.push(dst as u8);
            } else if op == expr::EXPR_EMIT_NULL {
                null_payloads.push(a1 as u32);
            } else {
                has_compute = true;
                if op == expr::EXPR_EMIT {
                    emit_regs.push(a1 as usize);
                    emit_payloads.push(a2 as usize);
                }
            }
        }

        let col_moves: Vec<ColMove> = copy_src_pi_bytes
            .iter()
            .zip(copy_out_payloads.iter())
            .zip(copy_type_codes.iter())
            .zip(copy_pk_byte_offsets.iter())
            .map(|(((&src, &dst), &tc), &pk_off)| ColMove {
                src_pi: src,
                dst_payload: dst as usize,
                type_code: tc,
                stride: col_move_stride(src, dst as usize, in_schema, out_schema),
                pk_byte_offset: pk_off,
            })
            .collect();

        let null_perm = NullPerm::from_col_pairs(&copy_src_pi_bytes, &copy_out_payloads, &null_payloads);

        let compute = if has_compute {
            let emit_strides: Vec<u8> = emit_payloads
                .iter()
                .map(|&p| {
                    let out_ci = out_schema.payload_col_idx(p);
                    out_schema.columns[out_ci].size()
                })
                .collect();
            debug_assert_eq!(emit_strides.len(), emit_regs.len());
            debug_assert_eq!(emit_strides.len(), emit_payloads.len());
            let no_nulls = prog.is_strictly_non_nullable(in_schema);
            ComputeKernel::Interpreted {
                prog,
                emit_payloads,
                emit_regs,
                emit_strides,
                no_nulls,
            }
        } else {
            ComputeKernel::None
        };

        Plan {
            filter: FilterKernel::PassAll,
            col_moves,
            null_perm,
            compute,
            scratch: RefCell::new(EvalScratch::new()),
        }
    }

    /// Evaluate predicate for a single row. Thin wrapper over `eval_batch` at
    /// m=1: the value lives at `regs[r * MORSEL]` (or, when `result_reg` is
    /// bit_only and nullable, at bit 0 of `bool_bits[r * NULL_WORDS_PER_REG]`,
    /// since the unpack to `regs` is skipped in that case).
    #[cfg(test)]
    pub(crate) fn evaluate_predicate(&self, batch: &MemBatch, row: usize) -> bool {
        let FilterKernel::Interpreted { prog, no_nulls } = &self.filter else {
            return true;
        };
        let mut scratch = self.scratch.borrow_mut();
        scratch.ensure_capacity(prog.num_regs as usize, *no_nulls, 1);
        eval_batch(prog, batch, row, 1, &mut scratch);
        if prog.num_regs == 0 {
            return false;
        }
        let r = prog.result_reg as usize;
        let is_null = !*no_nulls && (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0;
        if is_null {
            return false;
        }
        if !*no_nulls && prog.is_bit_only(r) {
            (scratch.bool_bits[r * NULL_WORDS_PER_REG] & 1) != 0
        } else {
            scratch.regs[r * MORSEL] != 0
        }
    }

    /// Run the filter over all `n` rows of `mb`, invoking `append_range` for
    /// each maximal contiguous run of passing rows. PassAll passes the whole
    /// range in one call. The bitmap stays inside `scratch` — no per-call
    /// `Vec<u64>` allocation.
    pub fn run_filter<F: FnMut(usize, usize)>(&self, mb: &MemBatch, n: usize, mut append_range: F) {
        let FilterKernel::Interpreted { prog, no_nulls } = &self.filter else {
            if n > 0 {
                append_range(0, n);
            }
            return;
        };
        let no_nulls = *no_nulls;
        let num_regs = prog.num_regs as usize;
        let result_reg = prog.result_reg as usize;

        let mut scratch = self.scratch.borrow_mut();
        scratch.ensure_capacity(num_regs, no_nulls, n);

        let words = n.div_ceil(64);

        // Fast-path gate: result_reg must be bit_only (i.e. produced by a
        // boolean op so `bool_bits[result_reg]` is fresh) AND we must be on
        // the nullable arm (in `no_nulls` mode bool_bits is unallocated).
        // Predicates like `WHERE int_col` produce result_reg from
        // LOAD_PAYLOAD_INT — not a bool producer — so the per-row scan over
        // `regs` is the only correct path for them.
        let use_fast_path = !no_nulls && prog.is_bit_only(result_reg);

        // Slow path uses `|=` and needs zeroed words; fast path overwrites
        // every word, so the fill is a wasted pass there.
        if !use_fast_path {
            scratch.filter_bits[..words].fill(0);
        }

        for morsel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - morsel_start);
            eval_batch(prog, mb, morsel_start, m, &mut scratch);

            if use_fast_path {
                // Word-level merge: filter bit = truthy & !null. morsel_start
                // is 64-aligned (MORSEL=256), so each morsel maps onto a
                // contiguous run of `filter_bits` words.
                let base = result_reg * NULL_WORDS_PER_REG;
                let words_m = m.div_ceil(64);
                let filter_word_base = morsel_start / 64;
                let tail_bits = m % 64;
                let full_words = if tail_bits != 0 { words_m - 1 } else { words_m };
                for w in 0..full_words {
                    let vw = scratch.bool_bits[base + w];
                    let nw = scratch.null_bits[base + w];
                    scratch.filter_bits[filter_word_base + w] = vw & !nw;
                }
                if tail_bits != 0 {
                    // Mask the dirty tail: ops like IS_NOT_NULL leave 1s
                    // beyond `m % 64` (`bool_bits = !null_word`); without
                    // this mask those phantom bits become false-positive
                    // passing rows in `filter_bits`.
                    let w = words_m - 1;
                    let vw = scratch.bool_bits[base + w];
                    let nw = scratch.null_bits[base + w];
                    let mask = (1u64 << tail_bits) - 1;
                    scratch.filter_bits[filter_word_base + w] = (vw & !nw) & mask;
                }
            } else {
                let base_r = result_reg * MORSEL;
                let base_null = result_reg * NULL_WORDS_PER_REG;
                for i in 0..m {
                    let abs = morsel_start + i;
                    let is_null = !no_nulls && (scratch.null_bits[base_null + i / 64] >> (i % 64)) & 1 != 0;
                    if !is_null && scratch.regs[base_r + i] != 0 {
                        scratch.filter_bits[abs / 64] |= 1u64 << (abs % 64);
                    }
                }
            }
        }

        scan_filter_bits(&scratch.filter_bits[..words], n, &mut append_range);
    }

    /// Execute map: system column clone → col_moves → NullPerm → compute.
    /// `out_schema` is still required here because constructing the output
    /// batch (`Batch::with_schema`) needs the full schema. All per-column
    /// stride information is baked into the Plan.
    pub fn execute_map(&self, in_batch: &Batch, out_schema: &SchemaDescriptor) -> Batch {
        let n = in_batch.count;
        if n == 0 {
            return Batch::empty_with_schema(out_schema);
        }

        let mut output = Batch::with_schema(*out_schema, n);
        output.count = n;

        // PK copy: bulk when strides match. When they differ (reindex maps,
        // e.g. U64 input → U128 synthetic PK), the PK region is left zero-
        // initialized here; `op_map` then overwrites it via `PkPromoter::
        // promote_into` or `reindex_hash_row`, both of which emit OPK bytes.
        let in_pk = in_batch.pk_data();
        let out_pk = output.pk_data_mut();
        if in_pk.len() == out_pk.len() {
            out_pk.copy_from_slice(in_pk);
        }
        output.weight_data_mut().copy_from_slice(in_batch.weight_data());

        // Column moves. The dedup cache is drawn from the thread-local pool only
        // when the output schema has a German-string column (else `get_mut()` is
        // None and the STRING/BLOB copy path is never taken). Reserve the output
        // blob once: dedup keeps it ≤ input blob size, so a single upfront reserve
        // covers every STRING/BLOB ColMove and avoids per-column reallocation.
        let mut blob_cache = crate::storage::BlobCacheGuard::acquire(out_schema, n);
        output.blob.reserve(in_batch.blob.len());
        for cm in &self.col_moves {
            copy_column(in_batch, &mut output, cm, blob_cache.get_mut());
        }

        // EMIT_NULL columns — already zero-filled by with_schema

        // Null bitmap (columnar) — written directly into the output buffer.
        // Split-borrow: `in_batch` and `output` are distinct allocations.
        {
            let in_nb = in_batch.null_bmp_data();
            self.null_perm.apply_column_into(in_nb, output.null_bmp_data_mut(), n);
        }

        // Compute kernel
        if let ComputeKernel::Interpreted {
            prog,
            emit_payloads,
            emit_regs,
            emit_strides,
            no_nulls,
        } = &self.compute
        {
            let no_nulls = *no_nulls;
            let num_regs = prog.num_regs as usize;

            let in_mb = in_batch.as_mem_batch();
            let mut scratch = self.scratch.borrow_mut();
            scratch.ensure_capacity(num_regs, no_nulls, n);

            let null_arr = output.null_bmp_data_mut().as_mut_ptr() as *mut u64;

            for morsel_start in (0..n).step_by(MORSEL) {
                let m = MORSEL.min(n - morsel_start);
                eval_batch(prog, &in_mb, morsel_start, m, &mut scratch);

                // EMIT: write each computed register to its output column
                for ((&out_payload, &reg), &stride_u8) in
                    emit_payloads.iter().zip(emit_regs.iter()).zip(emit_strides.iter())
                {
                    let stride = stride_u8 as usize;
                    let dst8 = output.col_data_mut(out_payload);
                    let base_r = reg * MORSEL;
                    let base_null_r = reg * NULL_WORDS_PER_REG;
                    for i in 0..m {
                        let row = morsel_start + i;
                        let is_null = !no_nulls && (scratch.null_bits[base_null_r + i / 64] >> (i % 64)) & 1 != 0;
                        let val = if is_null { 0i64 } else { scratch.regs[base_r + i] };
                        dst8[row * stride..(row + 1) * stride].copy_from_slice(&val.to_le_bytes()[..stride]);

                        // Merge null bit into row-major output null bitmap
                        if is_null {
                            unsafe {
                                *null_arr.add(row) |= 1u64 << out_payload;
                            }
                        }
                    }
                }
            }
        }

        output.sorted = false;
        output.consolidated = false;
        output
    }
}

// ---------------------------------------------------------------------------
// FinalizeContext — hoisted state for the per-row finalize evaluator
// ---------------------------------------------------------------------------

/// Per-program state for the REDUCE finalize expression. One instance is
/// constructed before the group loop and reused across every emitted row.
///
/// Everything that is constant across rows — the scratch register file, the
/// `no_nulls` flag, the EMIT-instruction → source-register map, and the
/// instruction-order column classification used by the caller — is computed
/// once and survives for the lifetime of the context.
pub(crate) struct FinalizeContext {
    scratch: EvalScratch,
    no_nulls: bool,
    /// One entry per `EXPR_EMIT` instruction (in program order): the source
    /// register that EMIT reads.
    emit_regs: Vec<usize>,
    /// Classification of every output-producing instruction in program order.
    /// Indexed by destination payload column at the call site.
    out_cols: Vec<OutputColKind>,
}

impl FinalizeContext {
    pub fn new(prog: &ExprProgram, in_schema: &SchemaDescriptor) -> Self {
        let no_nulls = prog.is_strictly_non_nullable(in_schema);
        let mut scratch = EvalScratch::new();
        scratch.ensure_capacity(prog.num_regs as usize, no_nulls, 1);
        let emit_regs: Vec<usize> = prog
            .code
            .chunks_exact(4)
            .filter(|i| i[0] == expr::EXPR_EMIT)
            .map(|i| i[2] as usize)
            .collect();
        let out_cols = prog.classify_output_cols();
        FinalizeContext {
            scratch,
            no_nulls,
            emit_regs,
            out_cols,
        }
    }

    /// Classification of each output-producing instruction (in program order).
    pub fn out_cols(&self) -> &[OutputColKind] {
        &self.out_cols
    }

    /// Evaluate `prog` over a single row of `mb` into the cached scratch.
    /// The result of each EMIT can subsequently be read with `read_emit`.
    pub fn eval_row(&mut self, prog: &ExprProgram, mb: &MemBatch, row: usize) {
        eval_batch(prog, mb, row, 1, &mut self.scratch);
    }

    /// Read the value and null flag for the `eidx`-th EMIT instruction.
    /// Must be called after `eval_row`.
    pub fn read_emit(&self, eidx: usize) -> (i64, bool) {
        let reg = self.emit_regs[eidx];
        let val = self.scratch.regs[reg * MORSEL];
        let is_null = !self.no_nulls && (self.scratch.null_bits[reg * NULL_WORDS_PER_REG] & 1) != 0;
        (val, is_null)
    }
}

/// Walk `bits` and call `append_range(start, end)` for every maximal run of
/// set bits. Fast paths: skip all-zero words; emit a whole-word run for
/// all-ones words. The mixed case bit-scans the word.
fn scan_filter_bits<F: FnMut(usize, usize)>(bits: &[u64], n: usize, append_range: &mut F) {
    let mut range_start: isize = -1;
    for (w, &word) in bits.iter().enumerate() {
        let row_base = w * 64;
        let chunk = (row_base + 64).min(n) - row_base;

        if word == 0 {
            if range_start >= 0 {
                append_range(range_start as usize, row_base);
                range_start = -1;
            }
        } else if word == u64::MAX || (chunk < 64 && word == (1u64 << chunk) - 1) {
            if range_start < 0 {
                range_start = row_base as isize;
            }
        } else {
            for i in 0..chunk {
                let passes = (word >> i) & 1 != 0;
                let abs = row_base + i;
                if passes {
                    if range_start < 0 {
                        range_start = abs as isize;
                    }
                } else if range_start >= 0 {
                    append_range(range_start as usize, abs);
                    range_start = -1;
                }
            }
        }
    }
    if range_start >= 0 {
        append_range(range_start as usize, n);
    }
}
