//! Scalar function types for DBSP filter and map operators.
//!
//! `ScalarFunc` cleanly separates columnar operations (column moves, null
//! permutation) from per-row operations (the expression interpreter). The VM
//! passes one opaque `*const ScalarFunc` handle for any filter / map /
//! projection.

use std::cell::RefCell;

use super::batch::{eval_batch, EvalScratch, MORSEL, NULL_WORDS_PER_REG};
use super::program::{Instr, LogicalProgram, ResolvedProgram};
use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output`. `cm.src` is the resolved
/// source locator (PK byte window or dense payload slot). The source is read
/// at its own type's width; when the destination slot (`cm.stride`, from the
/// output schema) is wider — a promoted integer column — the copy
/// sign/zero-extends the value into it.
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

    // Destructure the locator ONCE before the row loops: the per-row bodies
    // below stay free of locator dispatch (and of `native_le_bytes`'s
    // by-value [u8; 16] materialization).
    match cm.src {
        ColumnLocator::Pk {
            byte_off,
            size,
            type_code,
        } => {
            // PK region holds OPK bytes; decode the addressed column back to native
            // LE before writing it into the payload. A raw byte copy would be wrong
            // for signed (sign-flipped) and big-endian-encoded columns.
            let dst = output.col_data_mut(cm.dst_payload);
            let pk_off = byte_off as usize;
            let src_stride = size as usize;
            let mut le = [0u8; crate::schema::MAX_PK_BYTES];
            for row in 0..n {
                let opk = in_batch.get_pk_bytes(row);
                // Read the source column's OWN width from the OPK region (not the wider
                // destination stride, which would over-read into the next PK column),
                // decode to native LE, then widen if the output slot is wider.
                gnitz_wire::decode_pk_column(&opk[pk_off..pk_off + src_stride], type_code, &mut le[..src_stride]);
                let out = &mut dst[row * stride..row * stride + stride];
                if src_stride == stride {
                    out.copy_from_slice(&le[..stride]);
                } else {
                    gnitz_wire::widen_native_le(&le[..src_stride], type_code, out);
                }
            }
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            let in_pi = slot as usize;
            let src_stride = size as usize; // source read width
            if gnitz_wire::is_german_string(type_code) && blob_cache.is_some() {
                // STRING and BLOB share the 16-byte German-string struct: a long
                // (out-of-line) value's heap-offset field points into the source batch's
                // blob region. Relocate each cell's bytes into the output blob; the
                // shared BlobCache deduplicates identical spans across all columns/rows
                // of this MAP.
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
            } else if src_stride == stride {
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
                        type_code,
                        &mut dst[row * stride..row * stride + stride],
                    );
                }
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
    if !col_moves
        .iter()
        .any(|cm| gnitz_wire::is_german_string(cm.src.type_code()))
    {
        return false;
    }
    // Each input German-string column (never a PK, so it has a dense payload
    // index) must be the source of some ColMove; a dropped one leaves dead heap.
    (0..in_schema.num_columns())
        .filter(|&ci| gnitz_wire::is_german_string(in_schema.columns[ci].type_code))
        .all(|ci| {
            let pi = in_schema.payload_mapping_byte(ci);
            col_moves
                .iter()
                .any(|cm| matches!(cm.src, ColumnLocator::Payload { slot, .. } if slot == pi))
        })
}

// ---------------------------------------------------------------------------
// NullPerm — columnar null bitmap permutation
// ---------------------------------------------------------------------------

#[derive(Default)]
struct NullPerm {
    pairs: Vec<(u8, u8)>,
}

impl NullPerm {
    /// Build from the column moves (a PK source is skipped — the PK has no
    /// null bit).
    fn new(col_moves: &[ColMove]) -> Self {
        let pairs = col_moves
            .iter()
            .filter_map(|cm| match cm.src {
                ColumnLocator::Pk { .. } => None,
                ColumnLocator::Payload { slot, .. } => Some((slot, cm.dst_payload as u8)),
            })
            .collect();
        NullPerm { pairs }
    }

    #[inline]
    fn apply(&self, in_null: u64) -> u64 {
        let mut out: u64 = 0;
        for &(src, dst) in &self.pairs {
            out |= ((in_null >> src) & 1) << dst;
        }
        out
    }

    /// Write the permuted null bitmap directly into `out` (one u64 per row).
    /// `out.len()` must be `n * 8`.
    fn apply_column_into(&self, in_null_bmp: &[u8], out: &mut [u8], n: usize) {
        debug_assert_eq!(out.len(), n * 8);
        if self.pairs.is_empty() {
            // Pure-compute map: nothing to permute. The output null region is
            // zeroed on every construction path (fresh alloc, `recycle_buf`
            // clear, `resize(size, 0)` re-zero), so an all-zero permutation is
            // a no-op.
            return;
        }
        for row in 0..n {
            let off = row * 8;
            let in_null = crate::foundation::codec::read_u64_le(in_null_bmp, off);
            out[off..off + 8].copy_from_slice(&self.apply(in_null).to_le_bytes());
        }
    }
}

// ---------------------------------------------------------------------------
// ScalarFunc — unified representation for filter and map operations
// ---------------------------------------------------------------------------

struct ColMove {
    /// Resolved source column: PK byte window or dense payload slot, plus the
    /// SOURCE type code and width. The type dispatches the string path, decodes
    /// the OPK bytes of a PK source column, and (when widening) drives the
    /// extension signedness: value-preserving widening always extends per the
    /// source's signedness, even when the destination's sign class differs
    /// (e.g. U32 promoted into an I64 slot).
    src: ColumnLocator,
    /// Dense payload index in the output batch.
    dst_payload: usize,
    /// Destination write width, taken from the output schema. Wider than the
    /// source column's own width only for a promoted integer column (currently
    /// produced by cross-width set-ops), where the copy sign/zero-extends the
    /// source into the slot.
    stride: u8,
}

/// One EMIT target: computed register `reg` → output payload column `payload`
/// at write width `stride` (from the output schema).
struct EmitCol {
    payload: usize,
    reg: usize,
    stride: u8,
}

struct InterpretedCompute {
    prog: ResolvedProgram,
    emits: Vec<EmitCol>,
}

/// A compiled scalar function. The role is fixed at construction — the
/// compiler/VM dispatch is per-node, so a func is only ever driven through the
/// entry point matching its variant. Newtype over the private enum so the
/// variant fields keep the module's internal visibility.
pub struct ScalarFunc(Repr);

enum Repr {
    /// Filter predicate. `prog.no_nulls` carries the nullability verdict
    /// against the schema it was resolved against.
    Predicate {
        prog: ResolvedProgram,
        scratch: RefCell<EvalScratch>,
    },
    /// Map/projection: columnar moves + null permutation + optional per-row
    /// compute kernel, with the owned output schema.
    Map {
        col_moves: Vec<ColMove>,
        null_perm: NullPerm,
        compute: Option<InterpretedCompute>,
        /// Precomputed [`compute_blob_passthrough`]: skip per-cell string
        /// relocation and share the input blob when no string column is dropped.
        blob_passthrough: bool,
        /// Boxed: the descriptor is by far the largest field and would bloat
        /// every `Repr` (clippy: large_enum_variant); one indirection per
        /// batch is free.
        out_schema: Box<SchemaDescriptor>,
        scratch: RefCell<EvalScratch>,
    },
}

impl ScalarFunc {
    /// Filter via interpreted expression.
    pub fn from_predicate(logical: LogicalProgram, schema: &SchemaDescriptor) -> Self {
        ScalarFunc(Repr::Predicate {
            prog: logical.resolve(schema, /* is_filter = */ true),
            scratch: RefCell::new(EvalScratch::default()),
        })
    }

    /// Map plan from a logical expression program. A pure projection is the
    /// special case where every instruction is a `CopyCol` (see
    /// [`LogicalProgram::copy_cols`]): `compute` is `None` and the plan reduces to
    /// `col_moves` + `null_perm`.
    pub fn from_map(logical: LogicalProgram, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Self {
        let prog = logical.resolve(in_schema, /* is_filter = */ false);
        // One pass over the resolved instructions — `CopyCol`'s locator carries
        // the PK-vs-payload distinction, so there is no negative-sentinel decode
        // and no parallel-`Vec` bytecode re-walk.
        let out_stride = |payload: usize| out_schema.columns[out_schema.payload_col_idx(payload)].size();
        let mut col_moves: Vec<ColMove> = Vec::new();
        let mut emits: Vec<EmitCol> = Vec::new();
        for instr in &prog.instrs {
            match *instr {
                Instr::CopyCol { src, out } => col_moves.push(ColMove {
                    src,
                    dst_payload: out as usize,
                    stride: out_stride(out as usize),
                }),
                Instr::Emit { src, out } => emits.push(EmitCol {
                    payload: out as usize,
                    reg: src as usize,
                    stride: out_stride(out as usize),
                }),
                _ => {}
            }
        }

        // Null permutation: copied columns carry their source null bit (PK
        // sources are skipped inside `NullPerm::new` — the PK has no null bit).
        let null_perm = NullPerm::new(&col_moves);

        // Compute is needed iff the program emits a computed register: every
        // compute instruction exists only to feed an EMIT.
        let compute = (!emits.is_empty()).then_some(InterpretedCompute { prog, emits });

        let blob_passthrough = compute_blob_passthrough(in_schema, &col_moves);
        ScalarFunc(Repr::Map {
            col_moves,
            null_perm,
            compute,
            blob_passthrough,
            out_schema: Box::new(*out_schema),
            scratch: RefCell::new(EvalScratch::default()),
        })
    }

    /// The map's owned output schema. Callers that construct or stamp the
    /// output batch outside `evaluate_map_batch` (op_map's reindex arms) read
    /// it here instead of carrying a parallel schema operand.
    pub fn map_out_schema(&self) -> &SchemaDescriptor {
        match &self.0 {
            Repr::Map { out_schema, .. } => out_schema,
            Repr::Predicate { .. } => unreachable!("predicate ScalarFunc has no output schema"),
        }
    }

    /// Evaluate predicate for a single row. Thin wrapper over `eval_batch` at
    /// m=1: the value lives at `regs[r * MORSEL]` (or, when `result_reg` is
    /// bit_only and nullable, at bit 0 of `bool_bits[r * NULL_WORDS_PER_REG]`,
    /// since the unpack to `regs` is skipped in that case).
    #[cfg(test)]
    pub(crate) fn evaluate_predicate(&self, batch: &MemBatch, row: usize) -> bool {
        let Repr::Predicate { prog, scratch } = &self.0 else {
            unreachable!("evaluate_predicate on a Map ScalarFunc");
        };
        let no_nulls = prog.no_nulls;
        let mut scratch = scratch.borrow_mut();
        scratch.ensure_capacity(prog.num_regs as usize, no_nulls, 1);
        eval_batch(prog, batch, row, 1, &mut scratch);
        if prog.num_regs == 0 {
            return false;
        }
        let r = prog.result_reg as usize;
        let is_null = !no_nulls && (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0;
        if is_null {
            return false;
        }
        if !no_nulls && prog.is_bit_only(r) {
            (scratch.bool_bits[r * NULL_WORDS_PER_REG] & 1) != 0
        } else {
            scratch.regs[r * MORSEL] != 0
        }
    }

    /// Run the filter over all `n` rows of `mb`, invoking `append_range` for
    /// each maximal contiguous run of passing rows. The bitmap stays inside
    /// `scratch` — no per-call `Vec<u64>` allocation.
    pub fn run_filter<F: FnMut(usize, usize)>(&self, mb: &MemBatch, n: usize, mut append_range: F) {
        let Repr::Predicate { prog, scratch } = &self.0 else {
            unreachable!("run_filter on a Map ScalarFunc (the VM dispatch is per-node)");
        };
        let no_nulls = prog.no_nulls;
        let num_regs = prog.num_regs as usize;
        let result_reg = prog.result_reg as usize;

        let mut scratch = scratch.borrow_mut();
        scratch.ensure_capacity(num_regs, no_nulls, n);

        let words = n.div_ceil(64);

        // `no_nulls` mode allocates no `bool_bits` (capacity 0), so its arm
        // scans `regs` per row and needs zeroed words for the `|=`. On the
        // nullable arm the result register is ALWAYS packed —
        // `classify_registers` marks a filter's result as a bool input, so
        // every producer (boolean ops natively, value producers through
        // `maybe_pack_bool_bits`) populates `bool_bits[result_reg]` — and the
        // word-level merge overwrites every word.
        if no_nulls {
            scratch.filter_bits[..words].fill(0);
        }

        for morsel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - morsel_start);
            eval_batch(prog, mb, morsel_start, m, &mut scratch);

            if no_nulls {
                let base_r = result_reg * MORSEL;
                for i in 0..m {
                    let abs = morsel_start + i;
                    if scratch.regs[base_r + i] != 0 {
                        scratch.filter_bits[abs / 64] |= 1u64 << (abs % 64);
                    }
                }
            } else {
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
            }
        }

        scan_filter_bits(&scratch.filter_bits[..words], n, &mut append_range);
    }

    /// Execute map: system column clone → col_moves → NullPerm → compute. The
    /// output schema and all per-column stride information are baked into the
    /// `ScalarFunc`.
    pub fn evaluate_map_batch(&self, in_batch: &Batch) -> Batch {
        let Repr::Map {
            col_moves,
            null_perm,
            compute,
            blob_passthrough,
            out_schema,
            scratch,
        } = &self.0
        else {
            unreachable!("evaluate_map_batch on a Predicate ScalarFunc (the VM dispatch is per-node)");
        };
        let n = in_batch.count;
        if n == 0 {
            return Batch::empty_with_schema(out_schema);
        }

        let mut output = Batch::with_schema(**out_schema, n);
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
        let mut blob_cache = if *blob_passthrough {
            output.share_blob_from(in_batch);
            crate::storage::BlobCacheGuard::empty()
        } else {
            let cache = crate::storage::BlobCacheGuard::acquire(out_schema, n);
            if cache.is_active() {
                output.blob.reserve(in_batch.blob.len());
            }
            cache
        };
        for cm in col_moves {
            copy_column(in_batch, &mut output, cm, blob_cache.get_mut());
        }

        // Null bitmap (columnar) — written directly into the output buffer.
        // Split-borrow: `in_batch` and `output` are distinct allocations.
        {
            let in_nb = in_batch.null_bmp_data();
            null_perm.apply_column_into(in_nb, output.null_bmp_data_mut(), n);
        }

        // Compute kernel
        if let Some(InterpretedCompute { prog, emits }) = compute {
            let no_nulls = prog.no_nulls;
            let num_regs = prog.num_regs as usize;

            let in_mb = in_batch.as_mem_batch();
            let mut scratch = scratch.borrow_mut();
            scratch.ensure_capacity(num_regs, no_nulls, n);

            for morsel_start in (0..n).step_by(MORSEL) {
                let m = MORSEL.min(n - morsel_start);
                eval_batch(prog, &in_mb, morsel_start, m, &mut scratch);

                // EMIT: write each computed register to its output column.
                // Value store and null merge run as two sequential loops, each
                // taking a fresh borrow of `output`. `Batch::data` is a single
                // `Vec<u8>` holding every region, so a pointer into the null
                // region derived once and held across a `col_data_mut` reborrow
                // would be invalidated by it.
                for e in emits {
                    let out_payload = e.payload;
                    let stride = e.stride as usize;
                    let base_r = e.reg * MORSEL;
                    let base_null_r = e.reg * NULL_WORDS_PER_REG;

                    {
                        let dst8 = output.col_data_mut(out_payload);
                        if stride == 8 {
                            // Monomorphic fast arm: computed outputs are
                            // overwhelmingly 8-byte, and a constant-width store
                            // lets the loop vectorize.
                            for i in 0..m {
                                let is_null =
                                    !no_nulls && (scratch.null_bits[base_null_r + i / 64] >> (i % 64)) & 1 != 0;
                                let val = if is_null { 0i64 } else { scratch.regs[base_r + i] };
                                let off = (morsel_start + i) * 8;
                                dst8[off..off + 8].copy_from_slice(&val.to_le_bytes());
                            }
                        } else {
                            for i in 0..m {
                                let is_null =
                                    !no_nulls && (scratch.null_bits[base_null_r + i / 64] >> (i % 64)) & 1 != 0;
                                let val = if is_null { 0i64 } else { scratch.regs[base_r + i] };
                                let off = (morsel_start + i) * stride;
                                dst8[off..off + stride].copy_from_slice(&val.to_le_bytes()[..stride]);
                            }
                        }
                    }

                    // The two bitmaps are transposed: `scratch.null_bits` is
                    // register-major (bit i = row i), the output bitmap row-major
                    // (one u64 per row, bit c = payload column c). The merge is a
                    // scatter, never a word-at-a-time OR.
                    if !no_nulls {
                        let words = m.div_ceil(64);
                        let nb = output.null_bmp_data_mut();
                        for w in 0..words {
                            let mut null_word = scratch.null_bits[base_null_r + w];
                            // Every producer writes only rows `0..m`, so bits at
                            // index >= m are zero and the bit-scan stays in the
                            // morsel — no tail re-masking needed.
                            debug_assert!(
                                w + 1 < words || m.is_multiple_of(64) || (null_word >> (m % 64)) == 0,
                                "null_bits tail word has bits set beyond m={m}",
                            );
                            let lo = w * 64;
                            while null_word != 0 {
                                let bit = null_word.trailing_zeros() as usize;
                                null_word &= null_word - 1;
                                let off = (morsel_start + lo + bit) * 8;
                                let merged =
                                    u64::from_le_bytes(nb[off..off + 8].try_into().unwrap()) | (1u64 << out_payload);
                                nb[off..off + 8].copy_from_slice(&merged.to_le_bytes());
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
