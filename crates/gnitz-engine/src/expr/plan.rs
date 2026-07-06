//! Scalar function types for DBSP filter and map operators.
//!
//! `ScalarFunc` cleanly separates columnar operations (column moves, null
//! permutation) from per-row operations (the expression interpreter). The VM
//! passes one opaque `*const ScalarFunc` handle for any filter / map /
//! projection.

use std::cell::RefCell;

use super::batch::{eval_batch, EvalScratch, MORSEL, NULL_WORDS_PER_REG};
use super::program::{ColSrc, LogicalProgram, OutputColKind, ResolvedProgram};
use crate::schema::{SchemaDescriptor, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{Batch, MemBatch};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output`. `cm.src_pi` holds the
/// resolved payload byte: `SENTINEL` indicates the PK column, otherwise it is
/// the dense payload index in the input batch. The source is read at its own
/// type's width; when the destination slot (`cm.stride`, from the output
/// schema) is wider — a promoted integer column — the copy sign/zero-extends
/// the value into it.
///
/// `blob_cache` doubles as the STRING/BLOB mode switch. `Some`: each cell is
/// relocated into `output.blob` (else its heap offset dangles once the source
/// batch is dropped); the cache is shared across all ColMoves of one
/// `evaluate_map_batch` (and pooled across ticks via `BlobCacheGuard`) so
/// identical heap spans are appended at most once. `None`: the caller has
/// already shared `in_batch`'s blob into `output` (blob passthrough — every
/// long string's heap offset stays valid against the identical blob), so
/// STRING/BLOB columns fall into the equal-stride bulk copy below and the
/// 16-byte structs are copied verbatim. (A map never widens a string column,
/// so both strides are 16.)
fn copy_column(
    in_batch: &Batch,
    output: &mut Batch,
    cm: &ColMove,
    mut blob_cache: Option<&mut crate::schema::BlobCache>,
) {
    let n = in_batch.count;
    let stride = cm.stride as usize; // destination write width
    let src_stride = crate::schema::type_size(cm.type_code) as usize; // source read width

    if cm.src_pi == PAYLOAD_MAPPING_PK_SENTINEL {
        // PK region holds OPK bytes; decode the addressed column back to native
        // LE before writing it into the payload. A raw byte copy would be wrong
        // for signed (sign-flipped) and big-endian-encoded columns.
        let dst = output.col_data_mut(cm.dst_payload);
        let pk_off = cm.pk_byte_offset as usize;
        let mut le = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..n {
            let opk = in_batch.get_pk_bytes(row);
            // Read the source column's OWN width from the OPK region (not the wider
            // destination stride, which would over-read into the next PK column),
            // decode to native LE, then widen if the output slot is wider.
            gnitz_wire::decode_pk_column(&opk[pk_off..pk_off + src_stride], cm.type_code, &mut le[..src_stride]);
            let out = &mut dst[row * stride..row * stride + stride];
            if src_stride == stride {
                out.copy_from_slice(&le[..stride]);
            } else {
                gnitz_wire::widen_native_le(&le[..src_stride], cm.type_code, out);
            }
        }
    } else if gnitz_wire::is_german_string(cm.type_code) && blob_cache.is_some() {
        // STRING and BLOB share the 16-byte German-string struct: a long
        // (out-of-line) value's heap-offset field points into the source batch's
        // blob region. Relocate each cell's bytes into the output blob; the
        // shared BlobCache deduplicates identical spans across all columns/rows
        // of this MAP.
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
        if src_stride == stride {
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
        } else {
            // Wider destination slot (a promoted integer column): sign/zero-extend
            // the narrower source into it, one row at a time.
            let src = in_batch.col_data(in_pi);
            let dst = output.col_data_mut(cm.dst_payload);
            for row in 0..n {
                gnitz_wire::widen_native_le(
                    &src[row * src_stride..row * src_stride + src_stride],
                    cm.type_code,
                    &mut dst[row * stride..row * stride + stride],
                );
            }
        }
    }
}

/// Whether `evaluate_map_batch` may share `in_batch`'s blob and copy German-
/// string structs verbatim (see [`copy_column`]) instead of relocating every
/// cell. True iff some `ColMove` copies a German-string column (else there is
/// nothing to copy) AND every input German-string column is copied by some
/// `ColMove` — so the shared blob carries no dead heap. A map never computes a
/// string (EMIT writes ≤8-byte values), so every string output column is a
/// verbatim passthrough of an input string column. Mirrors `op_filter`'s
/// blob-passthrough, which is unconditional only because a filter drops no
/// column (though a filter's row subset, like a relocate's forgone dedup, can
/// still carry more blob bytes than a from-scratch relocate would).
fn compute_blob_passthrough(in_schema: &SchemaDescriptor, col_moves: &[ColMove]) -> bool {
    if !col_moves.iter().any(|cm| gnitz_wire::is_german_string(cm.type_code)) {
        return false;
    }
    // Each input German-string column (never a PK, so it has a dense payload
    // index) must be the source of some ColMove; a dropped one leaves dead heap.
    (0..in_schema.num_columns())
        .filter(|&ci| gnitz_wire::is_german_string(in_schema.columns[ci].type_code))
        .all(|ci| {
            let pi = in_schema.payload_mapping_byte(ci);
            col_moves.iter().any(|cm| cm.src_pi == pi)
        })
}

// ---------------------------------------------------------------------------
// NullPerm — columnar null bitmap permutation
// ---------------------------------------------------------------------------

#[derive(Default)]
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
// ScalarFunc — unified representation for filter and map operations
// ---------------------------------------------------------------------------

struct ColMove {
    /// Resolved payload byte for the source: `SENTINEL` for the PK column,
    /// otherwise the dense payload index in the input batch.
    src_pi: u8,
    /// Dense payload index in the output batch.
    dst_payload: usize,
    /// SOURCE type code of the column — dispatches the string path, decodes the
    /// OPK bytes of a PK source column, determines the source read width, and
    /// (when widening) drives the extension signedness: value-preserving
    /// widening always extends per the source's signedness, even when the
    /// destination's sign class differs (e.g. U32 promoted into an I64 slot).
    type_code: u8,
    /// Destination write width, taken from the output schema. Wider than the
    /// source column's own width only for a promoted integer column (currently
    /// produced by cross-width set-ops), where the copy sign/zero-extends the
    /// source into the slot.
    stride: u8,
    /// Byte offset of this column within the OPK PK region. Valid only when
    /// `src_pi == PAYLOAD_MAPPING_PK_SENTINEL`; 0 for single-column PKs.
    pk_byte_offset: u8,
}

struct InterpretedFilter {
    prog: ResolvedProgram,
    /// Precomputed `prog.is_strictly_non_nullable(in_schema)`.
    no_nulls: bool,
}

struct InterpretedCompute {
    prog: ResolvedProgram,
    emit_payloads: Vec<usize>,
    emit_regs: Vec<usize>,
    /// Output byte width per EMIT target, parallel to `emit_payloads`.
    emit_strides: Vec<u8>,
    /// Precomputed `prog.is_strictly_non_nullable(in_schema)`.
    no_nulls: bool,
}

pub struct ScalarFunc {
    filter: Option<InterpretedFilter>,
    col_moves: Vec<ColMove>,
    null_perm: NullPerm,
    compute: Option<InterpretedCompute>,
    /// Precomputed [`compute_blob_passthrough`]: skip per-cell string relocation
    /// and share the input blob when no string column is dropped.
    blob_passthrough: bool,
    scratch: RefCell<EvalScratch>,
}

impl ScalarFunc {
    /// Filter via interpreted expression.
    pub fn from_predicate(logical: LogicalProgram, schema: &SchemaDescriptor) -> Self {
        let prog = logical.resolve(schema, /* is_filter = */ true);
        let no_nulls = prog.is_strictly_non_nullable(schema);
        ScalarFunc {
            filter: Some(InterpretedFilter { prog, no_nulls }),
            col_moves: Vec::new(),
            null_perm: NullPerm::default(),
            compute: None,
            blob_passthrough: false, // a predicate copies no columns
            scratch: RefCell::new(EvalScratch::new()),
        }
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
                let out_ci = out_schema.payload_col_idx(i);
                ColMove {
                    src_pi,
                    dst_payload: i,
                    type_code: tc,
                    stride: out_schema.columns[out_ci].size(),
                    pk_byte_offset,
                }
            })
            .collect();
        let null_perm = NullPerm::from_projection(&src_pi_bytes);
        let blob_passthrough = compute_blob_passthrough(in_schema, &col_moves);
        ScalarFunc {
            filter: None,
            col_moves,
            null_perm,
            compute: None,
            blob_passthrough,
            scratch: RefCell::new(EvalScratch::new()),
        }
    }

    /// Map plan from a logical expression program.
    pub fn from_map(logical: LogicalProgram, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Self {
        let prog = logical.resolve(in_schema, /* is_filter = */ false);
        // One pass over the typed output classification — `ColSrc` carries the
        // PK-vs-payload distinction, so there is no negative-sentinel decode and
        // no parallel-`Vec` bytecode re-walk.
        let out_cols = prog.classify_output_cols();
        let mut col_moves: Vec<ColMove> = Vec::new();
        let mut null_payloads: Vec<u32> = Vec::new();
        let mut emit_payloads: Vec<usize> = Vec::new();
        let mut emit_regs: Vec<usize> = Vec::new();
        for k in &out_cols {
            match *k {
                OutputColKind::CopyCol { src, out_payload, tc } => {
                    let (src_pi, pk_byte_offset) = match src {
                        ColSrc::Pk { off } => (PAYLOAD_MAPPING_PK_SENTINEL, off),
                        ColSrc::Payload(pi) => (pi, 0u8),
                    };
                    let out_ci = out_schema.payload_col_idx(out_payload as usize);
                    col_moves.push(ColMove {
                        src_pi,
                        dst_payload: out_payload as usize,
                        type_code: tc,
                        stride: out_schema.columns[out_ci].size(),
                        pk_byte_offset,
                    });
                }
                OutputColKind::Emit { reg, out_payload } => {
                    emit_regs.push(reg);
                    emit_payloads.push(out_payload);
                }
                OutputColKind::EmitNull { out_payload } => null_payloads.push(out_payload),
            }
        }

        // Null permutation: copied columns carry their source null bit (PK
        // sentinels are skipped inside `from_col_pairs`); EMIT_NULL columns are
        // unconditionally null. Both derive from the same walk.
        let null_src: Vec<u8> = col_moves.iter().map(|c| c.src_pi).collect();
        let null_dst: Vec<u32> = col_moves.iter().map(|c| c.dst_payload as u32).collect();
        let null_perm = NullPerm::from_col_pairs(&null_src, &null_dst, &null_payloads);

        // Compute is needed iff the program emits a computed register: every
        // compute instruction exists only to feed an EMIT.
        let compute = if emit_regs.is_empty() {
            None
        } else {
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
            Some(InterpretedCompute {
                prog,
                emit_payloads,
                emit_regs,
                emit_strides,
                no_nulls,
            })
        };

        let blob_passthrough = compute_blob_passthrough(in_schema, &col_moves);
        ScalarFunc {
            filter: None,
            col_moves,
            null_perm,
            compute,
            blob_passthrough,
            scratch: RefCell::new(EvalScratch::new()),
        }
    }

    /// Evaluate predicate for a single row. Thin wrapper over `eval_batch` at
    /// m=1: the value lives at `regs[r * MORSEL]` (or, when `result_reg` is
    /// bit_only and nullable, at bit 0 of `bool_bits[r * NULL_WORDS_PER_REG]`,
    /// since the unpack to `regs` is skipped in that case).
    #[cfg(test)]
    pub(crate) fn evaluate_predicate(&self, batch: &MemBatch, row: usize) -> bool {
        let Some(InterpretedFilter { prog, no_nulls }) = &self.filter else {
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
    /// each maximal contiguous run of passing rows. The no-filter case passes
    /// the whole range in one call. The bitmap stays inside `scratch` — no
    /// per-call `Vec<u64>` allocation.
    pub fn run_filter<F: FnMut(usize, usize)>(&self, mb: &MemBatch, n: usize, mut append_range: F) {
        let Some(InterpretedFilter { prog, no_nulls }) = &self.filter else {
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
        // `Instr::LoadPayloadInt` — not a bool producer — so the per-row scan over
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
    /// stride information is baked into the `ScalarFunc`.
    pub fn evaluate_map_batch(&self, in_batch: &Batch, out_schema: &SchemaDescriptor) -> Batch {
        let n = in_batch.count;
        if n == 0 {
            return Batch::empty_with_schema(out_schema);
        }

        let mut output = Batch::with_schema(*out_schema, n);
        output.count = n;

        // PK copy: bulk when strides match. When they differ (reindex maps,
        // e.g. U64 input → U128 synthetic PK), the PK region is left zero-
        // initialized here; `op_map` then overwrites it via the reindex packer
        // or `reindex_hash_row`, both of which emit OPK bytes.
        let in_pk = in_batch.pk_data();
        let out_pk = output.pk_data_mut();
        if in_pk.len() == out_pk.len() {
            out_pk.copy_from_slice(in_pk);
        }
        output.weight_data_mut().copy_from_slice(in_batch.weight_data());

        // Column moves. When no string column is dropped (`blob_passthrough`),
        // share the input blob once and copy every String/Blob struct verbatim —
        // no per-row relocation and no dedup cache (`copy_column` gets `None`).
        // Otherwise relocate each cell: the dedup cache is drawn from the
        // thread-local pool only when the output has a German-string column (else
        // `get_mut()` is None and the relocate path is never reached), and one
        // upfront `reserve` (dedup keeps the output blob ≤ the input's) covers
        // every ColMove without per-column realloc.
        let mut blob_cache = if self.blob_passthrough {
            output.share_blob_from(in_batch);
            crate::storage::BlobCacheGuard::empty()
        } else {
            let cache = crate::storage::BlobCacheGuard::acquire(out_schema, n);
            if cache.is_active() {
                output.blob.reserve(in_batch.blob.len());
            }
            cache
        };
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
        if let Some(InterpretedCompute {
            prog,
            emit_payloads,
            emit_regs,
            emit_strides,
            no_nulls,
        }) = &self.compute
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

        // `output` came from `with_schema` (`Raw`) and only raw region writes
        // above, so it stays `Raw` — `op_map` keeps it that way.
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
    /// Classification of every output-producing instruction in program order.
    /// Indexed by destination payload column at the call site.
    out_cols: Vec<OutputColKind>,
}

impl FinalizeContext {
    pub fn new(prog: &ResolvedProgram, in_schema: &SchemaDescriptor) -> Self {
        let no_nulls = prog.is_strictly_non_nullable(in_schema);
        let mut scratch = EvalScratch::new();
        scratch.ensure_capacity(prog.num_regs as usize, no_nulls, 1);
        let out_cols = prog.classify_output_cols();
        FinalizeContext {
            scratch,
            no_nulls,
            out_cols,
        }
    }

    /// Classification of each output-producing instruction (in program order).
    pub fn out_cols(&self) -> &[OutputColKind] {
        &self.out_cols
    }

    /// Evaluate `prog` over a single row of `mb` into the cached scratch.
    /// The result of each EMIT can subsequently be read with `read_emit`.
    pub fn eval_row(&mut self, prog: &ResolvedProgram, mb: &MemBatch, row: usize) {
        eval_batch(prog, mb, row, 1, &mut self.scratch);
    }

    /// Read the value and null flag for the EMIT whose source is register `reg`.
    /// Must be called after `eval_row`.
    pub fn read_emit(&self, reg: usize) -> (i64, bool) {
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
