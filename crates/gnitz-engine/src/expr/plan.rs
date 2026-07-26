//! Scalar function types for DBSP filter and map operators.
//!
//! `ScalarFunc` cleanly separates columnar operations (column moves, null
//! permutation) from per-row operations (the expression interpreter). The VM
//! passes one opaque `*const ScalarFunc` handle for any filter / map /
//! projection.

use std::cell::RefCell;

use super::batch::{eval_batch, for_each_null_row, EvalScratch, MORSEL, NULL_WORDS_PER_REG};
use super::program::{ExprValidateErr, Instr, LogicalProgram, ResolvedProgram};
use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

/// Average survivor-run length below which [`ScalarFunc::append_map_ranges`]
/// compacts a fragmented range list before running the compute kernel — see the
/// break-even argument there.
const COMPACT_RUN_LEN: usize = 16;

/// How a map fills the output PK region — the explicit form of a decision the
/// caller alone can make, so no path can silently leave the region unwritten.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PkFill {
    /// Inherit the input PK verbatim. Requires equal PK strides, which the
    /// circuit compiler rejects a violation of and the ad-hoc reply guard checks.
    Copy,
    /// Leave it to the caller: `op_map`'s reindex packer / row hash overwrites
    /// every row's PK immediately after, emitting OPK bytes at the output stride
    /// (which legitimately differs — e.g. U64 input → U128 synthetic PK).
    Reindex,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output`, writing output row
/// `dst_base + i` from source row `src_start + i` for `i in 0..n`. `cm.src` is
/// the resolved source locator (PK byte window or dense payload slot). The
/// source is read at its own type's width; when the destination slot
/// (`cm.stride`, from the output schema) is wider — a promoted integer column —
/// the copy sign/zero-extends the value into it.
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
    src_start: usize,
    dst_base: usize,
    n: usize,
) {
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
            for i in 0..n {
                let opk = in_batch.get_pk_bytes(src_start + i);
                // Read the source column's OWN width from the OPK region (not the wider
                // destination stride, which would over-read into the next PK column),
                // decode to native LE, then widen if the output slot is wider.
                gnitz_wire::decode_pk_column(&opk[pk_off..pk_off + src_stride], type_code, &mut le[..src_stride]);
                let row = dst_base + i;
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
                // One split borrow, so the destination region is resolved once
                // rather than per row.
                let (dst_col, dst_blob) = output.col_and_blob_mut(cm.dst_payload);
                for i in 0..n {
                    let src_off = (src_start + i) * stride;
                    let cell = crate::schema::relocate_german_string_vec(
                        &src_col[src_off..src_off + stride],
                        &in_batch.blob,
                        dst_blob,
                        blob_cache.as_deref_mut(),
                    );
                    let dst_off = (dst_base + i) * 16;
                    dst_col[dst_off..dst_off + 16].copy_from_slice(&cell);
                }
            } else if src_stride == stride {
                debug_assert!(
                    (src_start + n) * stride <= in_batch.col_data(in_pi).len(),
                    "copy_column: (src_start+n)*stride (({}+{})*{}={}) > in_batch.col_data({}).len()={} \
                     (batch count={}, payload cols={})",
                    src_start,
                    n,
                    stride,
                    (src_start + n) * stride,
                    in_pi,
                    in_batch.col_data(in_pi).len(),
                    in_batch.count,
                    in_batch.num_payload_cols(),
                );
                let src = &in_batch.col_data(in_pi)[src_start * stride..(src_start + n) * stride];
                output.col_data_mut(cm.dst_payload)[dst_base * stride..(dst_base + n) * stride].copy_from_slice(src);
            } else {
                // Wider destination slot (a promoted integer column): sign/zero-extend
                // the narrower source into it, one row at a time.
                let src = in_batch.col_data(in_pi);
                let dst = output.col_data_mut(cm.dst_payload);
                for i in 0..n {
                    let sr = src_start + i;
                    let dr = dst_base + i;
                    gnitz_wire::widen_native_le(
                        &src[sr * src_stride..sr * src_stride + src_stride],
                        type_code,
                        &mut dst[dr * stride..dr * stride + stride],
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
    // Each input German-string column must be the source of some ColMove; a
    // dropped one leaves dead heap. Compared as whole locators — `ColMove::src`
    // is exactly what `locate` produced for its source column.
    (0..in_schema.num_columns())
        .filter(|&ci| gnitz_wire::is_german_string(in_schema.columns[ci].type_code))
        .all(|ci| {
            let src = in_schema.locate(ci);
            col_moves.iter().any(|cm| cm.src == src)
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

    /// `#[inline(always)]`: called once per mapped row by `write_rows`, and at
    /// `opt-level=0` a plain hint leaves this frame a real call.
    ///
    /// Indexed, not `for &(src, dst) in &self.pairs`: at `opt-level=0` the slice
    /// iterator's `next` stays an out-of-line call, so the iterator form costs a
    /// call per (row × pair) that `inline(always)` on this body cannot remove.
    #[inline(always)]
    fn apply(&self, in_null: u64) -> u64 {
        let mut out: u64 = 0;
        let pairs = self.pairs.as_slice();
        let mut i = 0;
        while i < pairs.len() {
            let (src, dst) = pairs[i];
            out |= (gnitz_wire::null_word_get(in_null, src as usize) as u64) << dst;
            i += 1;
        }
        out
    }

    /// Permute the null words of source rows `[src_start, src_start + n)` into
    /// `out` rows `[dst_base, dst_base + n)` (one u64 per row).
    ///
    /// Always writes the whole word, including for a pure-compute map whose
    /// permutation is empty: the destination may be a recycled, uninitialized
    /// batch tail, so "already zero" is not available to assume.
    fn write_rows(&self, in_null_bmp: &[u8], src_start: usize, out: &mut [u8], dst_base: usize, n: usize) {
        let dst = &mut out[dst_base * 8..(dst_base + n) * 8];
        if self.pairs.is_empty() {
            dst.fill(0);
            return;
        }
        for row in 0..n {
            let in_null = gnitz_wire::read_u64_le(in_null_bmp, (src_start + row) * 8);
            dst[row * 8..row * 8 + 8].copy_from_slice(&self.apply(in_null).to_le_bytes());
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

/// One EMIT target: computed register `reg` → output payload column `payload`.
/// No width: `validate`'s `check_emit_slot` holds every EMIT destination to an
/// 8-byte slot, so the store is monomorphic.
struct EmitCol {
    payload: usize,
    reg: usize,
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

/// Read register `r`'s value after an m=1 `eval_batch`. On the nullable arm a
/// bit_only register is never unpacked into `regs`, so its truth value lives at
/// bit 0 of `bool_bits` instead — the one reader both `evaluate_predicate` and
/// the expression tests go through.
#[cfg(test)]
#[inline]
pub(in crate::expr) fn read_reg_row0(prog: &ResolvedProgram, scratch: &EvalScratch, r: usize) -> i64 {
    if !prog.no_nulls && prog.is_bit_only(r) {
        i64::from((scratch.bool_bits[r * NULL_WORDS_PER_REG] & 1) != 0)
    } else {
        scratch.regs[r * MORSEL]
    }
}

impl ScalarFunc {
    /// Filter via interpreted expression. Validating here rather than at each
    /// call site makes "this program was checked against the schema it runs
    /// against" an invariant of the type: the arguments `validate` needs are
    /// exactly this function's, so no construction path can skip it.
    pub fn from_predicate(logical: LogicalProgram, schema: &SchemaDescriptor) -> Result<Self, ExprValidateErr> {
        logical.validate_predicate(schema)?;
        Ok(ScalarFunc(Repr::Predicate {
            prog: logical.resolve(schema, /* is_filter = */ true),
            scratch: RefCell::new(EvalScratch::default()),
        }))
    }

    /// Map plan from a logical expression program. A pure projection is the
    /// special case where every instruction is a `CopyCol` (see
    /// [`LogicalProgram::copy_cols`]): `compute` is `None` and the plan reduces to
    /// `col_moves` + `null_perm`.
    /// Validated against both schemas here — see [`Self::from_predicate`]. The
    /// EMIT store below reads a fixed 8-byte output slot, which is `validate`'s
    /// `check_emit_slot` rule; running it inside the constructor makes that a
    /// precondition of the type rather than a convention among callers.
    pub fn from_map(
        logical: LogicalProgram,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Result<Self, ExprValidateErr> {
        logical.validate(Some(in_schema), Some(out_schema))?;
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

        Ok(ScalarFunc(Repr::Map {
            col_moves,
            null_perm,
            compute,
            blob_passthrough,
            out_schema: Box::new(*out_schema),
            scratch: RefCell::new(EvalScratch::default()),
        }))
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
    /// m=1, reading the result through [`read_reg_row0`].
    #[cfg(test)]
    pub(crate) fn evaluate_predicate(&self, batch: &MemBatch, row: usize) -> bool {
        let Repr::Predicate { prog, scratch } = &self.0 else {
            unreachable!("evaluate_predicate on a Map ScalarFunc");
        };
        let no_nulls = prog.no_nulls;
        let mut scratch = scratch.borrow_mut();
        scratch.ensure_capacity(prog.num_regs as usize, no_nulls, 1);
        eval_batch(prog, batch, row, 1, &mut scratch);
        let r = prog.result_reg as usize;
        let is_null = !no_nulls && (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0;
        !is_null && read_reg_row0(prog, &scratch, r) != 0
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
        // `from_predicate` rejects a register-free program, so a predicate's
        // `result_reg` always names an allocated register.
        let result_reg = prog.result_reg as usize;

        let mut scratch = scratch.borrow_mut();
        scratch.ensure_capacity(num_regs, no_nulls, n);

        let words = n.div_ceil(64);

        for morsel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - morsel_start);
            eval_batch(prog, mb, morsel_start, m, &mut scratch);

            if no_nulls {
                // `no_nulls` mode allocates no `bool_bits` (capacity 0), so the
                // verdict is read out of `regs`. Packed a word at a time in a
                // register — `morsel_start` is 64-aligned (MORSEL=256), so each
                // morsel maps onto a contiguous run of `filter_bits` words, and
                // writing every word whole spares both the read-modify-write and
                // the up-front zero-fill.
                let base_r = result_reg * MORSEL;
                let filter_word_base = morsel_start / 64;
                // Split borrow, and the register window sliced once: the inner
                // loop then runs over a slice of known length, so its bound and
                // base pointer are hoisted out of it entirely.
                let EvalScratch { regs, filter_bits, .. } = &mut *scratch;
                let regs = &regs[base_r..base_r + m];
                for (w, chunk) in regs.chunks(64).enumerate() {
                    let mut bits = 0u64;
                    for (i, &v) in chunk.iter().enumerate() {
                        bits |= ((v != 0) as u64) << i;
                    }
                    filter_bits[filter_word_base + w] = bits;
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

    /// The predicate's surviving row ranges over `batch`, collected into `out`
    /// (cleared first). The one definition of "which rows of this batch pass" —
    /// every range-driven consumer (`op_filter`'s gather, the ad-hoc scan's rows
    /// and fold sinks) reads the list rather than re-deriving it from
    /// [`Self::run_filter`]'s callback, which cannot carry a `?` out or be cut
    /// against a `LIMIT` window.
    ///
    /// `out` is caller-owned so it can be reused across chunks; a `[(0, n)]`
    /// singleton is the agreed spelling of "no predicate".
    pub fn filter_ranges(&self, batch: &Batch, out: &mut Vec<(usize, usize)>) {
        out.clear();
        self.run_filter(&batch.as_mem_batch(), batch.count, |start, end| out.push((start, end)));
    }

    /// Map every `[start, end)` range of `src`, in list order, onto `keeper`'s
    /// tail — the ad-hoc rows sink's fused filter→project step, replacing an
    /// `op_map` into a throwaway batch followed by `keeper.append_batch`.
    ///
    /// The PK region passes through verbatim ([`PkFill::Copy`]), so the strides
    /// must agree; that is the rows-sink reply guard, and PK *type* parity is
    /// planner-guaranteed by the verbatim passthrough clone.
    pub fn append_map_ranges(&self, src: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
        self.map_ranges_into(src, keeper, ranges, PkFill::Copy);
    }

    /// Execute map over a whole batch into a fresh output: the DBSP `op_map`
    /// entry point. `pk` says whether the output PK region is inherited verbatim
    /// or stamped by the caller's reindex afterwards.
    pub fn evaluate_map_batch(&self, in_batch: &Batch, pk: PkFill) -> Batch {
        let Repr::Map {
            blob_passthrough,
            out_schema,
            ..
        } = &self.0
        else {
            unreachable!("evaluate_map_batch on a Predicate ScalarFunc (the VM dispatch is per-node)");
        };
        let n = in_batch.count;
        if n == 0 {
            return Batch::empty_with_schema(out_schema);
        }
        // Uninitialized: `validate` requires every map to write every output
        // payload slot, and `map_rows_into` writes the PK (or the caller's
        // reindex does), weight and null regions of every row.
        let mut output = Batch::with_capacity(**out_schema, n);
        // When no string column is dropped, adopt the input blob wholesale; the
        // shared `blob_id` is then what tells `map_ranges_into` to copy every
        // String/Blob struct verbatim instead of relocating each cell.
        if *blob_passthrough {
            output.share_blob_from(in_batch);
        }
        self.map_ranges_into(in_batch, &mut output, &[(0, n)], pk);
        output
    }

    /// The one map driver: provision `out`'s tail for the ranges, pick the blob
    /// mode, and run [`Self::map_rows_into`] per range.
    ///
    /// The blob mode is the batch layer's rule — *the destination knows whether
    /// it owns the source's bytes*: equal `blob_id` and equal length means `out`
    /// holds exactly `src`'s heap, so German-string structs copy verbatim (see
    /// `Batch::append_mem_batch_ranges`, which decides identically). Otherwise
    /// each cell is relocated into `out.blob` under a dedup cache that spans the
    /// call but never outlives it: every range reads the same live `src`, so a
    /// span shared across ranges is appended once, while a later chunk (whose
    /// recycled blob buffer can reuse the same address) gets a fresh cache.
    ///
    /// A fragmented range list under a compute-bearing map is compacted first:
    /// the kernel evaluates one morsel at a time *per range*, so a list of 1-row
    /// ranges would collapse it to row-at-a-time. A pure gather has no per-row
    /// kernel and copies range-wise either way, and a handful of long runs
    /// already vectorize — compacting those would be a wasted copy of the whole
    /// chunk. The break-even is [`COMPACT_RUN_LEN`], not `MORSEL`: compaction
    /// buys a full extra copy of every survivor (~1.5–15 ns/row) to save the
    /// *per-range* setup — one morsel eval prologue, one `MemBatch` (~½ KiB by
    /// value), one scratch borrow — a few hundred cycles. Runs longer than that
    /// already amortize it, whatever the morsel width.
    fn map_ranges_into(&self, src: &Batch, out: &mut Batch, ranges: &[(usize, usize)], pk: PkFill) {
        let total = crate::storage::range_rows(ranges);
        if total == 0 {
            return;
        }
        // Compact a kernel-starving list into one contiguous range, then fall
        // into the single copy loop below against the compacted source.
        let starves_kernel = matches!(&self.0, Repr::Map { compute: Some(_), .. })
            && ranges.len() > 1
            && total < ranges.len() * COMPACT_RUN_LEN;
        let compacted;
        let (src, ranges) = match starves_kernel.then_some(src.schema).flatten() {
            Some(s) => {
                compacted = Batch::from_ranges(src, ranges, &s);
                (&compacted, &[(0, total)][..])
            }
            None => (src, ranges),
        };

        let old = out.count;
        out.reserve_rows(total);
        // Publish the new rows up front: the `*_mut` accessors are `count`-bounded,
        // so `[old, old + total)` must be inside `count` before any write.
        out.count = old + total;

        let mut cache = match out.shares_blob_with(src) {
            true => crate::storage::BlobCacheGuard::empty(),
            false => {
                let cache = crate::storage::BlobCacheGuard::acquire(self.map_out_schema(), total);
                // Dedup keeps the output blob ≤ the input's, so one reserve covers
                // every ColMove without per-column realloc.
                if cache.is_active() {
                    out.blob.reserve(src.blob.len());
                }
                cache
            }
        };
        let mut dst = old;
        for &(start, end) in ranges {
            self.map_rows_into(src, out, start, dst, end - start, cache.get_mut(), pk);
            dst += end - start;
        }

        // Matches `append_batch`: nothing downstream trusts the destination's
        // order (a payload-reordering projection over a duplicate-PK input can
        // break (PK, payload) order — the D1 fail-safe).
        out.downgrade();
    }

    /// Map source rows `[src_start, src_start + n)` onto `out` rows
    /// `[dst_base, dst_base + n)`: PK/weight passthrough, null permutation,
    /// column moves, then the compute kernel. `out.count` must already cover the
    /// destination window — every `*_mut` accessor is `count`-bounded.
    #[allow(clippy::too_many_arguments)]
    fn map_rows_into(
        &self,
        in_batch: &Batch,
        output: &mut Batch,
        src_start: usize,
        dst_base: usize,
        n: usize,
        mut blob_cache: Option<&mut crate::schema::BlobCache>,
        pk: PkFill,
    ) {
        let Repr::Map {
            col_moves,
            null_perm,
            compute,
            scratch,
            ..
        } = &self.0
        else {
            unreachable!("map_rows_into on a Predicate ScalarFunc");
        };
        if n == 0 {
            return;
        }

        if let PkFill::Copy = pk {
            let pk_st = in_batch.pk_stride() as usize;
            debug_assert_eq!(pk_st, output.pk_stride() as usize, "PkFill::Copy: PK stride mismatch");
            output.pk_data_mut()[dst_base * pk_st..(dst_base + n) * pk_st]
                .copy_from_slice(&in_batch.pk_data()[src_start * pk_st..(src_start + n) * pk_st]);
        }
        output.weight_data_mut()[dst_base * 8..(dst_base + n) * 8]
            .copy_from_slice(&in_batch.weight_data()[src_start * 8..(src_start + n) * 8]);

        // Null bitmap, written before the compute kernel: EMIT merges its null
        // bits into the word with a read-modify-write `|=`. Split-borrow:
        // `in_batch` and `output` are distinct allocations.
        {
            let in_nb = in_batch.null_bmp_data();
            null_perm.write_rows(in_nb, src_start, output.null_bmp_data_mut(), dst_base, n);
        }

        for cm in col_moves {
            copy_column(in_batch, output, cm, blob_cache.as_deref_mut(), src_start, dst_base, n);
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
                eval_batch(prog, &in_mb, src_start + morsel_start, m, &mut scratch);

                // EMIT: write each computed register to its output column.
                for e in emits {
                    let out_payload = e.payload;
                    let base_r = e.reg * MORSEL;
                    let base_null_r = e.reg * NULL_WORDS_PER_REG;
                    let row0 = dst_base + morsel_start;
                    // One split borrow: the value slots and this column's bit in
                    // the row-major NULL bitmap are written in the same pass over
                    // the null rows.
                    let (col, nb) = output.col_and_null_bmp_mut(out_payload);

                    // `check_emit_slot` holds every EMIT destination to an
                    // 8-byte slot, so the morsel's outputs are one contiguous
                    // window and the whole register image is the little-endian
                    // encoding EMIT stores — one `copy_from_slice`, no per-row
                    // bounds check and no per-row NULL branch.
                    let win = &mut col[row0 * 8..(row0 + m) * 8];
                    let regs = &scratch.regs[base_r..base_r + m];
                    // SAFETY: `i64` has no padding and no invalid bit patterns,
                    // and `main.rs` fails the build on a non-little-endian
                    // target, so an i64 slice's byte image *is* its
                    // `to_le_bytes()` sequence. `u8` is 1-aligned, and the window
                    // is `m` elements of a slice with at least `m` left.
                    let regs_le = unsafe { std::slice::from_raw_parts(regs.as_ptr().cast::<u8>(), m * 8) };
                    win.copy_from_slice(regs_le);

                    // A NULL row's value slot reads as zero, and its bit is set
                    // in the output bitmap. NULL rows are the exception, so both
                    // are done by a sparse bit-scan rather than a per-row branch
                    // that would de-vectorize the store above. The two bitmaps
                    // are transposed — `scratch.null_bits` is register-major
                    // (bit i = row i), the output bitmap row-major (one u64 per
                    // row, bit c = payload column c) — so the merge is a scatter,
                    // never a word-at-a-time OR.
                    if !no_nulls {
                        for_each_null_row(&scratch.null_bits, base_null_r, m, |i| {
                            win[i * 8..i * 8 + 8].fill(0);
                            let off = (row0 + i) * 8;
                            let mut merged = gnitz_wire::read_u64_le(nb, off);
                            gnitz_wire::null_word_set(&mut merged, out_payload, true);
                            nb[off..off + 8].copy_from_slice(&merged.to_le_bytes());
                        });
                    }
                }
            }
        }
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
