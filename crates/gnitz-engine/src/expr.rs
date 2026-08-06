//! Scalar function types for DBSP filter and map operators.
//!
//! `ScalarFunc` cleanly separates columnar operations (column moves, null
//! permutation) from per-row operations (the expression kernel). The VM passes
//! one opaque `*const ScalarFunc` handle for any filter / map / projection.
//!
//! The evaluator itself lives in `gnitz-expr`; this module is a consumer of that
//! crate, not its home, so `LogicalProgram`, the instruction model and the
//! resolved form are named `gnitz_expr::` at each call site rather than
//! re-exported here.

use gnitz_expr::{Evaluator, ExprValidateErr, LogicalProgram, MorselOut};

use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::Batch;

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
    mut blob_cache: Option<&mut crate::storage::BlobCache>,
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
            // One PK column, so 16 bytes covers every fixed-width type — not
            // MAX_PK_BYTES, which is the whole multi-column PK stride.
            let mut le = [0u8; 16];
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
                //
                // A map never widens a string column, so both sides are the same
                // 16-byte cell — asserted rather than assumed, since the loop
                // below reads and writes at that one width.
                debug_assert_eq!(
                    (src_stride, stride),
                    (16, 16),
                    "German-string column moved at a non-16-byte stride",
                );
                let src_col = in_batch.col_data(in_pi);
                // One split borrow, so the destination region is resolved once
                // rather than per row.
                let (dst_col, _, dst_blob) = output.col_null_and_blob_mut(cm.dst_payload);
                for i in 0..n {
                    let src_off = (src_start + i) * 16;
                    let cell = crate::storage::relocate_german_string_vec(
                        &src_col[src_off..src_off + 16],
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
/// `ColMove` — so the shared blob carries no dead heap.
///
/// A string EMIT alongside a passthrough is fine and needs no term here: the
/// adopted blob is the output's own buffer (`share_blob_from` copies the bytes
/// and takes the id), so the emit appends to it without touching the prefix the
/// copied cells address. Mirrors `op_filter`'s
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
            gnitz_wire::write_u64_le(dst, row * 8, self.apply(in_null));
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

/// A compiled scalar function. The role is fixed at construction — the
/// compiler/VM dispatch is per-node, so a func is only ever driven through the
/// entry point matching its variant. Newtype over the private enum so the
/// variant fields keep the module's internal visibility.
pub struct ScalarFunc(Repr);

/// Both variants are boxed so `Repr` stays pointer-sized: a `MapPlan` owns a
/// whole `SchemaDescriptor` and an `Evaluator` a whole register file, so either
/// one inline would size the enum for both. One indirection per batch is free —
/// a `ScalarFunc` is built once per plan node.
enum Repr {
    /// Filter predicate, resolved against the schema it runs on (which is what
    /// fixes its nullability verdict) and carrying its own register file.
    Predicate(Box<Evaluator>),
    Map(Box<MapPlan>),
}

/// Map/projection: columnar moves + null permutation + optional per-row compute
/// kernel, with the owned output schema. Every map method lives here rather than
/// on [`ScalarFunc`], so the internal `map_ranges_into` → `map_rows_into` chain
/// reads its fields directly instead of re-checking the variant per call.
struct MapPlan {
    col_moves: Vec<ColMove>,
    null_perm: NullPerm,
    /// `Some` iff the program emits a computed register — a pure projection is
    /// `None` and never grows a register file.
    compute: Option<Evaluator>,
    /// `(source register, output payload slot)` per `Emit`, resolved once at
    /// construction like [`Self::col_moves`]. Both lists are empty iff `compute`
    /// is `None`. Walking the evaluator's instruction stream instead would
    /// re-scan it once per morsel, which the range-driven `append_map_ranges`
    /// path (16-row ranges) pays on top of very little work.
    emits: Vec<(usize, usize)>,
    /// The same, for `EmitStr`. Kept apart from [`Self::emits`] because the two
    /// write differently: a scalar emit is one bulk copy of the register image,
    /// a string emit encodes a German-string cell per row.
    str_emits: Vec<(usize, usize)>,
    /// Precomputed [`compute_blob_passthrough`]: skip per-cell string relocation
    /// and share the input blob when no string column is dropped.
    blob_passthrough: bool,
    out_schema: SchemaDescriptor,
}

impl ScalarFunc {
    /// Filter via interpreted expression.
    pub fn from_predicate(logical: LogicalProgram, schema: &SchemaDescriptor) -> Result<Self, ExprValidateErr> {
        Ok(ScalarFunc(Repr::Predicate(Box::new(logical.resolve_filter(schema)?))))
    }

    /// Map plan from a logical expression program. A pure projection is the
    /// special case where every instruction is a `CopyCol` (see
    /// [`LogicalProgram::copy_cols`]): `compute` is `None` and the plan reduces to
    /// `col_moves` + `null_perm`.
    pub fn from_map(
        logical: LogicalProgram,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Result<Self, ExprValidateErr> {
        let ev = logical.resolve_map(in_schema, out_schema)?;
        // Both instruction-stream reads happen here, once per plan node: the
        // resolved `CopyCol` locator carries the PK-vs-payload distinction and
        // the `Emit` targets are the output slots the compute kernel writes.
        // Neither is re-derived per morsel.
        let out_stride = |payload: usize| out_schema.columns[out_schema.payload_col_idx(payload)].size();
        let col_moves: Vec<ColMove> = ev
            .copy_moves()
            .map(|(src, out)| ColMove {
                src,
                dst_payload: out as usize,
                stride: out_stride(out as usize),
            })
            .collect();
        let mut emits: Vec<(usize, usize)> = Vec::new();
        let mut str_emits: Vec<(usize, usize)> = Vec::new();
        for (reg, out, is_str) in ev.emit_targets() {
            let list = if is_str { &mut str_emits } else { &mut emits };
            list.push((reg as usize, out as usize));
        }
        // Null permutation: copied columns carry their source null bit (PK
        // sources are skipped inside `NullPerm::new` — the PK has no null bit).
        let null_perm = NullPerm::new(&col_moves);

        // Compute is needed iff the program emits a computed register: every
        // compute instruction exists only to feed an EMIT. Both lists count —
        // a view whose only computed column is a string (`SELECT id, UPPER(name)
        // FROM t`, where `id` is a ColMove) would otherwise drop the kernel and
        // ship that column's uninitialized region. Validation cannot catch it:
        // the `Emit` is in the program, so the output-coverage popcount is met.
        let compute = (!emits.is_empty() || !str_emits.is_empty()).then_some(ev);

        let blob_passthrough = compute_blob_passthrough(in_schema, &col_moves);

        Ok(ScalarFunc(Repr::Map(Box::new(MapPlan {
            col_moves,
            null_perm,
            compute,
            emits,
            str_emits,
            blob_passthrough,
            out_schema: *out_schema,
        }))))
    }

    /// The map plan. The VM's dispatch is per-node, so a func is only ever
    /// driven through the entry points matching its variant — this is the one
    /// place that says so for the map half.
    fn map(&self) -> &MapPlan {
        match &self.0 {
            Repr::Map(m) => m,
            Repr::Predicate(_) => unreachable!("map entry point on a Predicate ScalarFunc"),
        }
    }

    /// The map's owned output schema. Callers that construct or stamp the
    /// output batch outside [`Self::evaluate_map_batch`] (op_map's reindex arms)
    /// read it here instead of carrying a parallel schema operand.
    pub fn map_out_schema(&self) -> &SchemaDescriptor {
        &self.map().out_schema
    }

    /// The predicate's surviving row ranges over `batch`, collected into `out`
    /// (cleared first). The one engine spelling of "which rows of this batch
    /// pass" — every range-driven consumer (`op_filter`'s gather, the ad-hoc
    /// scan's rows and fold sinks) reads the list rather than driving
    /// [`Evaluator::filter`]'s callback itself, which cannot carry a `?` out or
    /// be cut against a `LIMIT` window.
    ///
    /// `out` is caller-owned so it can be reused across chunks; a `[(0, n)]`
    /// singleton is the agreed spelling of "no predicate".
    pub fn filter_ranges(&self, batch: &Batch, out: &mut Vec<(usize, usize)>) {
        let Repr::Predicate(ev) = &self.0 else {
            unreachable!("filter_ranges on a Map ScalarFunc (the VM dispatch is per-node)");
        };
        out.clear();
        ev.filter(&batch.as_mem_batch(), batch.count, |start, end| out.push((start, end)));
    }

    /// Map every `[start, end)` range of `src`, in list order, onto `keeper`'s
    /// tail — the ad-hoc rows sink's fused filter→project step, replacing an
    /// `op_map` into a throwaway batch followed by `keeper.append_batch`.
    ///
    /// The PK region passes through verbatim ([`PkFill::Copy`]), so the strides
    /// must agree; that is the rows-sink reply guard, and PK *type* parity is
    /// planner-guaranteed by the verbatim passthrough clone.
    pub fn append_map_ranges(&self, src: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
        self.map().map_ranges_into(src, keeper, ranges, PkFill::Copy);
    }

    /// Execute map over a whole batch into a fresh output: the DBSP `op_map`
    /// entry point. `pk` says whether the output PK region is inherited verbatim
    /// or stamped by the caller's reindex afterwards.
    pub fn evaluate_map_batch(&self, in_batch: &Batch, pk: PkFill) -> Batch {
        let map = self.map();
        let n = in_batch.count;
        if n == 0 {
            return Batch::empty_with_schema(&map.out_schema);
        }
        // Uninitialized: `validate` requires every map to write every output
        // payload slot, and `map_rows_into` writes the PK (or the caller's
        // reindex does), weight and null regions of every row.
        let mut output = Batch::with_capacity(map.out_schema, n);
        // When no string column is dropped, adopt the input blob wholesale; the
        // shared `blob_id` is then what tells `map_ranges_into` to copy every
        // String/Blob struct verbatim instead of relocating each cell.
        if map.blob_passthrough {
            output.share_blob_from(in_batch);
        }
        map.map_ranges_into(in_batch, &mut output, &[(0, n)], pk);
        output
    }
}

/// The NULL half of an EMIT, shared by the scalar (`stride` 8) and string
/// (`stride` 16) loops: a NULL row's value slot reads as zero, and its bit is set
/// in the output bitmap.
///
/// NULL rows are the exception, so both are done by a sparse bit-scan rather than
/// a per-row branch that would de-vectorize the value store. The two bitmaps are
/// transposed — the register file's null bits are register-major (bit i = row i),
/// the output bitmap row-major (one u64 per row, bit c = payload column c) — so
/// the merge is a scatter, never a word-at-a-time OR. The read-modify-write on
/// `nb` is what composes with `null_perm`'s earlier write and with any other emit
/// over the same bitmap.
#[inline]
fn emit_null_rows(
    out: &MorselOut<'_>,
    reg: usize,
    win: &mut [u8],
    nb: &mut [u8],
    row0: usize,
    out_payload: usize,
    stride: usize,
) {
    out.for_each_null_row(reg, |i| {
        win[i * stride..(i + 1) * stride].fill(0);
        let off = (row0 + i) * 8;
        let mut merged = gnitz_wire::read_u64_le(nb, off);
        gnitz_wire::null_word_set(&mut merged, out_payload, true);
        gnitz_wire::write_u64_le(nb, off, merged);
    });
}

impl MapPlan {
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
        let starves_kernel = self.compute.is_some() && ranges.len() > 1 && total < ranges.len() * COMPACT_RUN_LEN;
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
                let cache = crate::storage::BlobCacheGuard::acquire(&self.out_schema, total);
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
        mut blob_cache: Option<&mut crate::storage::BlobCache>,
        pk: PkFill,
    ) {
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
            self.null_perm
                .write_rows(in_nb, src_start, output.null_bmp_data_mut(), dst_base, n);
        }

        for cm in &self.col_moves {
            copy_column(in_batch, output, cm, blob_cache.as_deref_mut(), src_start, dst_base, n);
        }

        // Compute kernel
        if let Some(ev) = &self.compute {
            let in_mb = in_batch.as_mem_batch();
            ev.eval_morsels(&in_mb, src_start, n, |morsel_start, out| {
                let m = out.rows();
                // EMIT: write each computed register to its output column.
                // Indexed, not `for &(..) in &self.emits`: at opt-level=0 the
                // slice iterator's `next` is an out-of-line call per (morsel ×
                // emit), the same reason `NullPerm::apply` indexes.
                let emits = self.emits.as_slice();
                let row0 = dst_base + morsel_start;
                let mut e = 0;
                while e < emits.len() {
                    let (reg, out_payload) = emits[e];
                    e += 1;
                    // One split borrow: the value slots and this column's bit in
                    // the row-major NULL bitmap are written in the same pass over
                    // the null rows.
                    let (col, nb, _) = output.col_null_and_blob_mut(out_payload);

                    // One blit: the morsel's rows are contiguous in the column.
                    let win = &mut col[row0 * 8..(row0 + m) * 8];
                    win.copy_from_slice(out.reg_bytes(reg));

                    emit_null_rows(out, reg, win, nb, row0, out_payload, 8);
                }

                // String EMIT. It keeps the scalar path's two-pass shape rather
                // than branching per row on nullness, because `MorselOut` exposes
                // nullness only through `for_each_null_row` — and under
                // `no_nulls` there is no `null_bits` to index at all. The scalar
                // path's indexed loop buys nothing here: this body already does a
                // per-row `encode_german_string`.
                for &(reg, out_payload) in &self.str_emits {
                    let (col, nb, blob) = output.col_null_and_blob_mut(out_payload);
                    let win = &mut col[row0 * 16..(row0 + m) * 16];
                    for i in 0..m {
                        let cell = gnitz_wire::encode_german_string(out.str_bytes(reg, i), blob);
                        win[i * 16..i * 16 + 16].copy_from_slice(&cell);
                    }
                    emit_null_rows(out, reg, win, nb, row0, out_payload, 16);
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use gnitz_expr::{ExprValidateErr, LogicalProgram};

    use super::{PkFill, ScalarFunc};
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
    use crate::storage::Batch;

    /// Build a `Batch` of `(pk, weight, null_word, payload i64 cells)` rows against
    /// `schema` — the engine-side physical batch these tests drive `ScalarFunc`
    /// with, as opposed to the owned-buffer view `gnitz-expr`'s own tests use.
    fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u64, &[i64])]) -> Batch {
        let mut batch = Batch::with_capacity(*schema, rows.len().max(1));
        for &(pk, weight, null_word, cols) in rows {
            batch.extend_pk(pk as u128);
            batch.extend_weight(&weight.to_le_bytes());
            batch.extend_null_bmp(&null_word.to_le_bytes());
            for (pi, _col) in schema.payload_columns() {
                if pi < cols.len() {
                    batch.extend_col(pi, &cols[pi].to_le_bytes());
                }
            }
            batch.count += 1;
        }
        batch
    }

    fn make_schema(pk_index: u32, col_types: &[u8]) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::EMPTY; MAX_COLUMNS];
        for (i, &tc) in col_types.iter().enumerate() {
            let nullable = if i == pk_index as usize { 0 } else { 1 };
            columns[i] = SchemaColumn::new(tc, nullable);
        }
        SchemaDescriptor::new(&columns[..col_types.len()], &[pk_index])
    }

    #[test]
    fn test_projection_batch() {
        let in_schema = make_schema(0, &[8, 9, 9]);
        let out_schema = make_schema(0, &[8, 9, 9]);
        let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20]), (2, 1, 0, &[30, 40])]);

        let prog = LogicalProgram::copy_cols(&[2, 1]);
        let func = ScalarFunc::from_map(prog, &in_schema, &out_schema).unwrap();
        let result = func.evaluate_map_batch(&batch, PkFill::Copy);
        assert_eq!(result.count, 2);

        let r0_col0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        let r0_col1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(r0_col0, 20);
        assert_eq!(r0_col1, 10);
    }

    #[test]
    fn test_map_copy_and_emit() {
        let in_schema = make_schema(0, &[8, 9, 9]);
        let out_schema = make_schema(0, &[8, 9, 9]);

        let batch = make_int_batch(&in_schema, &[(1, 1, 0, &[10, 20])]);

        use gnitz_expr::LogicalInstr;
        let instrs = vec![
            LogicalInstr::CopyCol { src_col: 1, out: 0 },
            LogicalInstr::LoadColInt { dst: 0, col: 1 },
            LogicalInstr::LoadColInt { dst: 1, col: 2 },
            LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
            LogicalInstr::Emit { src: 2, out: 1 },
        ];
        let prog = LogicalProgram::new(instrs, 3, 2, vec![]);

        let func = ScalarFunc::from_map(prog, &in_schema, &out_schema).unwrap();
        let result = func.evaluate_map_batch(&batch, PkFill::Copy);
        assert_eq!(result.count, 1);

        let v0 = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(v0, 10);
        let v1 = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
        assert_eq!(v1, 30);
    }

    #[test]
    fn test_empty_batch() {
        let schema = make_schema(0, &[8, 9]);
        let batch = Batch::empty_with_schema(&schema);

        let func = ScalarFunc::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema).unwrap();
        let result = func.evaluate_map_batch(&batch, PkFill::Copy);
        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_map_blob_passthrough_and_fallback() {
        // German-string struct: short (≤12 bytes) inline, else heap-backed.
        fn push_gs(b: &mut Batch, pi: usize, s: &[u8]) {
            let gs = gnitz_wire::encode_german_string(s, &mut b.blob);
            b.extend_col(pi, &gs);
        }
        // Input: [U64 PK, STRING s1 (short inline), STRING s2 (long, heap-backed)].
        fn build(schema: &SchemaDescriptor) -> Batch {
            let mut b = Batch::with_capacity(*schema, 2);
            for (pk, s1, s2) in [
                (1u128, b"ab".as_slice(), b"long-string-one-xyz".as_slice()),
                (2u128, b"cd".as_slice(), b"long-string-two-abcdef".as_slice()),
            ] {
                b.extend_pk(pk);
                b.extend_weight(&1i64.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                push_gs(&mut b, 0, s1); // payload idx 0 = s1
                push_gs(&mut b, 1, s2); // payload idx 1 = s2
                b.count += 1;
            }
            b
        }

        let in_schema = make_schema(0, &[type_code::U64, type_code::STRING, type_code::STRING]);

        // (A) Keep BOTH string columns (reordered) → passthrough fires; the shared
        // blob keeps every long string's heap offset valid through the verbatim copy.
        {
            let batch = build(&in_schema);
            let out_schema = make_schema(0, &[type_code::U64, type_code::STRING, type_code::STRING]);
            let prog = LogicalProgram::copy_cols(&[2, 1]);
            let func = ScalarFunc::from_map(prog, &in_schema, &out_schema).unwrap();
            let out = func.evaluate_map_batch(&batch, PkFill::Copy);
            assert_eq!(out.count, 2);
            assert_eq!(
                crate::test_support::read_german_string(&out, 0, 0),
                b"long-string-one-xyz"
            ); // s2 → out payload 0
            assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ab"); // s1 → out payload 1
            assert_eq!(
                crate::test_support::read_german_string(&out, 0, 1),
                b"long-string-two-abcdef"
            );
            assert_eq!(crate::test_support::read_german_string(&out, 1, 1), b"cd");
        }

        // (B) Drop the long string s2 → passthrough gated OFF (a dropped string column
        // would leave dead heap in a shared blob), so the relocate path runs and the
        // output blob carries only the referenced (here empty, short-inline) spans.
        {
            let batch = build(&in_schema);
            let out_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
            let prog = LogicalProgram::copy_cols(&[1]);
            let func = ScalarFunc::from_map(prog, &in_schema, &out_schema).unwrap();
            let out = func.evaluate_map_batch(&batch, PkFill::Copy);
            assert_eq!(out.count, 2);
            assert_eq!(crate::test_support::read_german_string(&out, 0, 0), b"ab");
            assert_eq!(crate::test_support::read_german_string(&out, 0, 1), b"cd");
            assert!(
                out.blob.len() < batch.blob.len(),
                "dropped-string relocate must not copy the dead heap ({} vs {})",
                out.blob.len(),
                batch.blob.len(),
            );
        }
    }

    /// PK-source `CopyCol` through `from_map`: a compound PK's columns projected into
    /// payload slots must decode out of the OPK region back to native LE — verbatim
    /// for the U128 column, sign-flip-undone for the signed I64 column. `copy_column`
    /// reads the source column's own width at its own `tc`, so a program carrying the
    /// real source type codes round-trips both.
    #[test]
    fn test_map_pk_copy_col_u128_and_signed_i64() {
        // PK = (U128 c0, I64 c1); payload = I64 c2.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        // Out: same compound PK, then both PK columns copied into payload slots plus
        // the payload passthrough.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U128, 0), // payload 0 ← PK col 0
                SchemaColumn::new(type_code::I64, 0),  // payload 1 ← PK col 1
                SchemaColumn::new(type_code::I64, 0),  // payload 2 ← payload col 2
            ],
            &[0, 1],
        );

        let pk0: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210;
        let pk1: i64 = -5; // negative: exercises the OPK sign-bit flip on decode
        let mut batch = Batch::with_capacity(in_schema, 1);
        batch.extend_pk_opk(&in_schema, &[pk0, pk1 as u64 as u128]);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &42i64.to_le_bytes());
        batch.count += 1;

        let prog = LogicalProgram::copy_cols(&[
            0, // PK col 0 (U128) → payload 0
            1, // PK col 1 (I64) → payload 1
            2, // payload col 2 (I64) → payload 2
        ]);
        let out = ScalarFunc::from_map(prog, &in_schema, &out_schema)
            .unwrap()
            .evaluate_map_batch(&batch, PkFill::Copy);

        assert_eq!(out.count, 1);
        assert_eq!(out.pk_data(), batch.pk_data(), "PK region copied verbatim");
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(
            u128::from_le_bytes(out.col_data(0)[..16].try_into().unwrap()),
            pk0,
            "U128 PK column must round-trip through copy_column",
        );
        assert_eq!(
            i64::from_le_bytes(out.col_data(1)[..8].try_into().unwrap()),
            pk1,
            "signed I64 PK column must un-flip the OPK sign bit",
        );
        assert_eq!(i64::from_le_bytes(out.col_data(2)[..8].try_into().unwrap()), 42);
        assert_eq!(out.get_null_word(0), 0, "no copied column is null");
    }

    /// Cross-width widen through `from_map` (the shape a cross-width set-op UNION
    /// produces): a narrow source column copied into a wider promoted output slot
    /// sign/zero-extends per the SOURCE column's signedness — from the payload region
    /// and from the PK region alike.
    #[test]
    fn test_map_copy_col_widens_into_promoted_slot() {
        // PK = (U16 c0, I16 c1); payload = I8 c2 (negative), U8 c3.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U16, 0),
                SchemaColumn::new(type_code::I16, 0),
                SchemaColumn::new(type_code::I8, 0),
                SchemaColumn::new(type_code::U8, 0),
            ],
            &[0, 1],
        );
        // Every copied column lands in an I64 slot — 4 distinct narrow→wide widens.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U16, 0),
                SchemaColumn::new(type_code::I16, 0),
                SchemaColumn::new(type_code::I64, 0), // ← U16 PK  (zero-extend)
                SchemaColumn::new(type_code::I64, 0), // ← I16 PK  (sign-extend)
                SchemaColumn::new(type_code::I64, 0), // ← I8 payload (sign-extend)
                SchemaColumn::new(type_code::I64, 0), // ← U8 payload (zero-extend)
            ],
            &[0, 1],
        );

        let (c0, c1, c2, c3): (u16, i16, i8, u8) = (0xBEEF, -300, -7, 0xFE);
        let mut batch = Batch::with_capacity(in_schema, 1);
        batch.extend_pk_opk(&in_schema, &[c0 as u128, c1 as u16 as u128]);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &c2.to_le_bytes());
        batch.extend_col(1, &c3.to_le_bytes());
        batch.count += 1;

        let prog = LogicalProgram::copy_cols(&[
            0, // U16 PK  → zero-extend
            1, // I16 PK  → sign-extend
            2, // I8 payload → sign-extend
            3, // U8 payload → zero-extend
        ]);
        let out = ScalarFunc::from_map(prog, &in_schema, &out_schema)
            .unwrap()
            .evaluate_map_batch(&batch, PkFill::Copy);

        assert_eq!(out.count, 1);
        let widened = |pi: usize| i64::from_le_bytes(out.col_data(pi)[..8].try_into().unwrap());
        assert_eq!(widened(0), c0 as i64, "U16 PK zero-extends");
        assert_eq!(widened(1), c1 as i64, "I16 PK sign-extends");
        assert_eq!(widened(2), c2 as i64, "I8 payload sign-extends");
        assert_eq!(widened(3), c3 as i64, "U8 payload zero-extends");
    }

    /// An empty wire program decodes to `num_regs = 0, result_reg = 0` — framing
    /// accepts it, and `validate` cannot reject it outright because that is exactly
    /// the shape every `copy_cols` map has. As a *filter* it has no result to read,
    /// so `from_predicate` rejects it rather than letting it masquerade as a filter
    /// that passes nothing (which a client would read as an empty table).
    #[test]
    fn test_register_free_predicate_is_rejected() {
        let schema = make_schema(0, &[8, 9]);
        let prog = LogicalProgram::from_wire(&[], 0, 0, vec![]).unwrap();
        assert_eq!(
            ScalarFunc::from_predicate(prog, &schema).err(),
            Some(ExprValidateErr::PredicateWithoutResultReg)
        );
        // The same shape is legitimate as a map: `copy_cols` builds it.
        assert!(ScalarFunc::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema).is_ok());
    }

    /// The predicate wiring `gnitz-expr`'s own tests cannot reach: `from_predicate`
    /// through `filter_ranges` over a real `Batch`, including the range coalescing
    /// that turns per-row verdicts into the `(start, end)` list every consumer reads.
    #[test]
    fn test_from_predicate_filter_ranges_over_a_batch() {
        use gnitz_expr::{CmpOp, LogicalInstr};

        let schema = make_schema(0, &[type_code::U64, type_code::I64]);
        // Rows 1..=6 with col1 = 5, 20, 30, 0, 40, 50 → `col1 > 15` keeps
        // {1, 2} and {4, 5}: two runs, so a PK-only or per-row answer would differ.
        let rows: Vec<(u64, i64, u64, &[i64])> = vec![
            (1, 1, 0, &[5]),
            (2, 1, 0, &[20]),
            (3, 1, 0, &[30]),
            (4, 1, 0, &[0]),
            (5, 1, 0, &[40]),
            (6, 1, 0, &[50]),
        ];
        let batch = make_int_batch(&schema, &rows);

        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 },
            LogicalInstr::LoadConst { dst: 1, val: 15 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            },
        ];
        let func = ScalarFunc::from_predicate(LogicalProgram::new(instrs, 3, 2, vec![]), &schema).unwrap();

        let mut ranges = Vec::new();
        func.filter_ranges(&batch, &mut ranges);
        assert_eq!(ranges, vec![(1, 3), (4, 6)]);

        // `out` is cleared, not appended to, so a reused buffer cannot leak a
        // previous chunk's ranges into this one.
        func.filter_ranges(&batch, &mut ranges);
        assert_eq!(ranges, vec![(1, 3), (4, 6)]);
    }

    /// A batch of `(pk, string cells)` rows: one STRING payload column per entry
    /// of `cells`, encoded through the blob heap the map must relocate or share.
    fn make_string_batch(schema: &SchemaDescriptor, rows: &[&[&[u8]]]) -> Batch {
        let mut batch = Batch::with_capacity(*schema, rows.len().max(1));
        for (row, cells) in rows.iter().enumerate() {
            batch.extend_pk(row as u128 + 1);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            for (pi, _col) in schema.payload_columns() {
                let cell = gnitz_wire::encode_german_string(cells[pi], &mut batch.blob);
                batch.extend_col(pi, &cell);
            }
            batch.count += 1;
        }
        batch
    }

    /// A map whose *only* computed column is a string. The compute kernel is
    /// gated on the emit lists being non-empty, and a gate that counted only the
    /// scalar list would drop the kernel here and ship the STRING region
    /// uninitialized — which validation cannot catch, because the `Emit` is in
    /// the program and the output-coverage popcount is satisfied.
    #[test]
    fn map_whose_only_computed_column_is_a_string_still_runs_the_kernel() {
        use gnitz_expr::LogicalInstr;
        // [U64 pk, STRING name] -> [U64 pk, U64 id_copy, STRING upper_name].
        let in_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
        let out_schema = make_schema(0, &[type_code::U64, type_code::U64, type_code::STRING]);
        let mut batch = make_string_batch(&in_schema, &[&[b"abc"], &[b"a-long-value-past-twelve"]]);
        // The copied column is the PK, which `PkFill::Copy` carries verbatim; give
        // the output's first payload slot something to hold.
        batch.count = 2;

        let instrs = vec![
            LogicalInstr::CopyCol { src_col: 0, out: 0 },
            LogicalInstr::LoadColStr { dst: 0, col: 1 },
            LogicalInstr::StrCase {
                dst: 1,
                a: 0,
                upper: true,
            },
            LogicalInstr::Emit { src: 1, out: 1 },
        ];
        let func = ScalarFunc::from_map(LogicalProgram::new(instrs, 2, 1, vec![]), &in_schema, &out_schema).unwrap();
        let out = func.evaluate_map_batch(&batch, PkFill::Copy);
        assert_eq!(out.count, 2);
        assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ABC");
        assert_eq!(
            crate::test_support::read_german_string(&out, 1, 1),
            b"A-LONG-VALUE-PAST-TWELVE",
            "a heap-backed value must land in the output's own blob"
        );
    }

    /// A string EMIT alongside a passthrough of every input string column. The
    /// copied cells still resolve against the adopted blob after the emit has
    /// appended to it, and the output stops sharing once it has.
    #[test]
    fn string_emit_composes_with_blob_passthrough() {
        use gnitz_expr::LogicalInstr;
        // [U64 pk, STRING s] -> [U64 pk, STRING s_copy, STRING s_upper].
        let in_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
        let out_schema = make_schema(0, &[type_code::U64, type_code::STRING, type_code::STRING]);
        let batch = make_string_batch(&in_schema, &[&[b"a-long-value-past-twelve"], &[b"short"]]);

        let instrs = vec![
            LogicalInstr::CopyCol { src_col: 1, out: 0 },
            LogicalInstr::LoadColStr { dst: 0, col: 1 },
            LogicalInstr::StrCase {
                dst: 1,
                a: 0,
                upper: true,
            },
            LogicalInstr::Emit { src: 1, out: 1 },
        ];
        let func = ScalarFunc::from_map(LogicalProgram::new(instrs, 2, 1, vec![]), &in_schema, &out_schema).unwrap();
        let out = func.evaluate_map_batch(&batch, PkFill::Copy);

        assert_eq!(
            crate::test_support::read_german_string(&out, 0, 0),
            b"a-long-value-past-twelve"
        );
        assert_eq!(crate::test_support::read_german_string(&out, 0, 1), b"short");
        assert_eq!(
            crate::test_support::read_german_string(&out, 1, 0),
            b"A-LONG-VALUE-PAST-TWELVE"
        );
        assert_eq!(crate::test_support::read_german_string(&out, 1, 1), b"SHORT");
        assert!(
            !out.shares_blob_with(&batch),
            "appending the emitted bytes must end the sharing, so a later append relocates"
        );
    }

    /// A NULL string row must ship a zeroed cell *and* its bitmap bit — a
    /// non-deterministic value there would leave an insert and its retraction
    /// unable to cancel.
    #[test]
    fn null_string_emit_zeroes_the_cell_and_sets_the_bit() {
        use gnitz_expr::LogicalInstr;
        let in_schema = make_schema(0, &[type_code::U64, type_code::STRING]);
        let out_schema = make_schema(0, &[type_code::U64, type_code::U64, type_code::STRING]);
        let mut batch = make_string_batch(&in_schema, &[&[b"abc"], &[b"def"]]);
        // Row 1's source string is NULL.
        gnitz_wire::write_u64_le(batch.null_bmp_data_mut(), 8, 1);

        let instrs = vec![
            LogicalInstr::CopyCol { src_col: 0, out: 0 },
            LogicalInstr::LoadColStr { dst: 0, col: 1 },
            LogicalInstr::StrCase {
                dst: 1,
                a: 0,
                upper: true,
            },
            LogicalInstr::Emit { src: 1, out: 1 },
        ];
        let func = ScalarFunc::from_map(LogicalProgram::new(instrs, 2, 1, vec![]), &in_schema, &out_schema).unwrap();
        let out = func.evaluate_map_batch(&batch, PkFill::Copy);

        assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ABC");
        assert_eq!(&out.col_data(1)[16..32], &[0u8; 16], "a NULL cell is all zeros");
        assert_eq!(
            gnitz_wire::read_u64_le(out.null_bmp_data(), 8) & 2,
            2,
            "the output bitmap bit for the string column must be set"
        );
    }
}
