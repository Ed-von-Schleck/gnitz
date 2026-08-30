//! [`MapPlan`] — the columnar driver behind every DBSP map and projection.
//!
//! A filter needs nothing on top of the resolved program and is a bare
//! `gnitz_expr::Evaluator` wherever one is held; a map is this plan, which
//! separates the columnar work (column moves, null permutation) from the
//! per-row expression kernel the evaluator runs.
//!
//! The evaluator itself lives in `gnitz-expr`; this module is a consumer of that
//! crate, not its home, so `LogicalProgram`, the instruction model and the
//! resolved form are named `gnitz_expr::` at each call site rather than
//! re-exported here.

use gnitz_expr::{Evaluator, ExprValidateErr, LogicalProgram, MorselOut};

use crate::foundation::xxh::RowHasher;
use crate::schema::key::{NarrowPkOpk, ReindexPacker};
use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::Batch;

/// One verbatim column move: `(source locator, output payload slot, destination
/// write width)`, as `gnitz-expr` resolved it.
type ColCopy = (ColumnLocator, u32, u8);

/// One map step's row window: source rows `[src, src + n)` onto destination rows
/// `[dst, dst + n)`. The three travel together through every columnar body
/// below, so they are one value rather than three positional `usize`s a caller
/// can transpose.
#[derive(Clone, Copy)]
struct RowWindow {
    src: usize,
    dst: usize,
    n: usize,
}

/// How a copy moves a German-string cell. The destination's blob identity and
/// the dedup cache are one decision, so they are one value: `Verbatim` with a
/// live cache is a state that cannot be built.
enum BlobMode<'a> {
    /// `output` already holds exactly `in_batch`'s heap, so every long string's
    /// offset resolves the same on both sides and the 16-byte structs copy
    /// through the ordinary equal-stride bulk copy.
    Verbatim,
    /// Relocate each cell's bytes into `output.blob` — else its heap offset
    /// dangles once the source is dropped. `Some` deduplicates identical spans
    /// across every column and row of one map; `None` relocates without dedup,
    /// as it does everywhere else this type appears.
    Relocate(Option<&'a mut crate::storage::BlobCache>),
}

impl BlobMode<'_> {
    /// Reborrow for one column's copy, so the caller keeps ownership across the
    /// copy list.
    fn reborrow(&mut self) -> BlobMode<'_> {
        match self {
            BlobMode::Verbatim => BlobMode::Verbatim,
            BlobMode::Relocate(cache) => BlobMode::Relocate(cache.as_deref_mut()),
        }
    }
}

/// Average survivor-run length below which [`MapPlan::append_map_ranges`]
/// compacts a fragmented range list before running the compute kernel — see the
/// break-even argument there.
const COMPACT_RUN_LEN: usize = 16;

/// Where a map's output PK region comes from. Owned by the plan rather than
/// passed per call, so the region cannot be left unwritten between two
/// statements and no caller can pair a plan with the wrong stamp.
pub(crate) enum PkSource {
    /// Copy the input PK region verbatim. Requires equal PK strides, which the
    /// circuit compiler rejects a violation of and the ad-hoc reply guard
    /// checks.
    Inherit,
    /// Pack the reindex columns' OPK bytes contiguously into the output PK — the
    /// `_join_pk` of an equijoin / GROUP BY repartition. The output stride
    /// legitimately differs from the input's (U64 input → U128 synthetic PK).
    Pack(ReindexPacker),
    /// Hash the full output row (every payload column) into each PK, giving
    /// EXCEPT/INTERSECT/DISTINCT their full-row set identity.
    HashRow { branch_id: u8 },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output` over `w`, moving any
/// German-string cell per `blob` (a map never widens a string column, so both
/// strides are 16). The source is read at its own type's width; when the
/// destination slot (`dst_stride`, from the output schema) is wider — a promoted
/// integer column — the copy sign/zero-extends the value into it.
fn copy_column(
    in_batch: &Batch,
    output: &mut Batch,
    &(src_loc, dst_payload, dst_stride): &ColCopy,
    blob: BlobMode<'_>,
    w: RowWindow,
) {
    let RowWindow {
        src: src_start,
        dst: dst_base,
        n,
    } = w;
    let dst_payload = dst_payload as usize;
    let stride = dst_stride as usize; // destination write width

    // Destructured ONCE before the row loops, so the per-row bodies below carry
    // no locator dispatch (and no `native_le_bytes` by-value [u8; 16]).
    match src_loc {
        ColumnLocator::Pk {
            byte_off,
            size,
            type_code,
        } => {
            // PK region holds OPK bytes; decode the addressed column back to
            // native LE. A raw byte copy would be wrong for signed (sign-flipped)
            // and big-endian-encoded columns. Both regions are resolved once, as
            // in the Payload arms below.
            let pk = in_batch.pk_data();
            let pk_stride = in_batch.pk_stride() as usize;
            let dst = output.col_data_mut(dst_payload);
            let pk_off = byte_off as usize;
            let src_stride = size as usize;
            // Plan-time constant, so it selects a loop rather than branching per
            // row. Each read is the source column's OWN width — the wider
            // destination stride would over-read into the next PK column.
            if src_stride == stride {
                for i in 0..n {
                    let off = (src_start + i) * pk_stride + pk_off;
                    let row = dst_base + i;
                    gnitz_wire::decode_pk_column(
                        &pk[off..off + stride],
                        type_code,
                        &mut dst[row * stride..row * stride + stride],
                    );
                }
            } else {
                // One PK column, so 16 bytes covers every fixed-width type — not
                // MAX_PK_BYTES, which is the whole multi-column PK stride.
                let mut le = [0u8; 16];
                for i in 0..n {
                    let off = (src_start + i) * pk_stride + pk_off;
                    gnitz_wire::decode_pk_column(&pk[off..off + src_stride], type_code, &mut le[..src_stride]);
                    let row = dst_base + i;
                    gnitz_wire::widen_native_le(
                        &le[..src_stride],
                        type_code,
                        &mut dst[row * stride..row * stride + stride],
                    );
                }
            }
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            let in_pi = slot as usize;
            let src_stride = size as usize; // source read width
            if let (true, BlobMode::Relocate(mut cache)) = (gnitz_wire::is_german_string(type_code), blob) {
                // STRING and BLOB share the 16-byte German-string struct, whose
                // heap-offset field points into the source batch's blob.
                // Asserted, not assumed: the loop below reads and writes at that
                // one width.
                debug_assert_eq!(
                    (src_stride, stride),
                    (16, 16),
                    "German-string column moved at a non-16-byte stride",
                );
                let src_col = in_batch.col_data(in_pi);
                // One split borrow, so the destination region is resolved once
                // rather than per row.
                let (dst_col, _, dst_blob) = output.col_null_and_blob_mut(dst_payload);
                for i in 0..n {
                    let src_off = (src_start + i) * 16;
                    let cell = crate::storage::relocate_german_string_vec(
                        &src_col[src_off..src_off + 16],
                        &in_batch.blob,
                        dst_blob,
                        cache.as_deref_mut(),
                    );
                    let dst_off = (dst_base + i) * 16;
                    dst_col[dst_off..dst_off + 16].copy_from_slice(&cell);
                }
            } else if src_stride == stride {
                debug_assert!(
                    (src_start + n) * stride <= in_batch.col_data(in_pi).len(),
                    "copy_column: source column {in_pi} is shorter than rows [{src_start}, {}) at stride {stride}",
                    src_start + n
                );
                let src = &in_batch.col_data(in_pi)[src_start * stride..(src_start + n) * stride];
                output.col_data_mut(dst_payload)[dst_base * stride..(dst_base + n) * stride].copy_from_slice(src);
            } else {
                // Wider destination slot (a promoted integer column): sign/zero-extend
                // the narrower source into it, one row at a time.
                let src = in_batch.col_data(in_pi);
                let dst = output.col_data_mut(dst_payload);
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

/// What a map's copy list does to the input's German-string columns. Named by
/// cause rather than by action, because the action differs per caller:
/// `evaluate_map_batch` adopts the input blob under `KeepsEveryString`, while
/// `append_map_ranges` relocates in every state, its keeper never sharing a
/// chunk's blob.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StringMoves {
    /// No copy carries a German-string column: nothing to relocate, nothing to
    /// share.
    NoStrings,
    /// Some copy carries one, and some input string column is dropped — whose
    /// dead heap a shared blob would carry.
    DropsAString,
    /// Every input German-string column is carried by some copy, so a shared
    /// blob has no dead heap. A string emit alongside is fine: the adopted blob
    /// is the output's own buffer, so the emit appends past the prefix the
    /// copied cells address.
    KeepsEveryString,
}

impl StringMoves {
    fn new(in_schema: &SchemaDescriptor, copies: &[ColCopy]) -> Self {
        // `has_german_string` is a cached field, so it settles a string-free
        // input without touching the copy list. An early-out, not the first
        // term: a map dropping *every* string column still has it set.
        if !in_schema.has_german_string() || !copies.iter().any(|c| gnitz_wire::is_german_string(c.0.type_code())) {
            return StringMoves::NoStrings;
        }
        // Compared as whole locators — a copy's source is exactly what `locate`
        // produced for its source column.
        let keeps_all = (0..in_schema.num_columns())
            .filter(|&ci| gnitz_wire::is_german_string(in_schema.columns[ci].type_code))
            .all(|ci| {
                let src = in_schema.locate(ci);
                copies.iter().any(|c| c.0 == src)
            });
        match keeps_all {
            true => StringMoves::KeepsEveryString,
            false => StringMoves::DropsAString,
        }
    }
}

// ---------------------------------------------------------------------------
// NullPerm — columnar null bitmap permutation
// ---------------------------------------------------------------------------

/// How a map derives each output row's null word from its input row's — the
/// three shapes the `(source slot, destination slot)` pair list collapses to.
enum NullPerm {
    /// No copied source can carry a set bit, so every output word is zero.
    Zero,
    /// Every pair moves a bit to the slot it already occupies, so the whole
    /// permutation is one AND against the kept slots' mask. Any prefix
    /// projection lands here; the same-arity-same-order case never reaches a
    /// map at all (the compiler elides it as an identity).
    Mask(u64),
    /// Some bit changes slot: one shift-test-shift-or per pair per row.
    Permute(Vec<(u8, u8)>),
}

impl NullPerm {
    /// Build from the column moves. A source contributes a pair only if it can
    /// carry a set bit: a PK source has no null bit at all, and a `NOT NULL`
    /// payload source is believed over the bit the batch carries for it. The
    /// mask is the evaluator's own `nullable_slots`, the one this program's
    /// nullability verdict was resolved against.
    fn new(copies: &[ColCopy], nullable: u64) -> Self {
        let pairs: Vec<(u8, u8)> = copies
            .iter()
            .filter_map(|&(src, dst_payload, _)| match src {
                ColumnLocator::Pk { .. } => None,
                ColumnLocator::Payload { slot, .. } => {
                    gnitz_wire::null_word_get(nullable, slot as usize).then_some((slot, dst_payload as u8))
                }
            })
            .collect();
        if pairs.is_empty() {
            return NullPerm::Zero;
        }
        if pairs.iter().all(|&(src, dst)| src == dst) {
            return NullPerm::Mask(pairs.iter().fold(0u64, |m, &(src, _)| m | 1u64 << src));
        }
        NullPerm::Permute(pairs)
    }

    /// `#[inline(always)]`: called once per mapped row by `write_rows`, and at
    /// `opt-level=0` a plain hint leaves this frame a real call.
    ///
    /// Indexed, not `for &(src, dst) in pairs`: at `opt-level=0` the slice
    /// iterator's `next` stays an out-of-line call, so the iterator form costs a
    /// call per (row × pair) that `inline(always)` on this body cannot remove.
    #[inline(always)]
    fn permute(pairs: &[(u8, u8)], in_null: u64) -> u64 {
        let mut out: u64 = 0;
        let mut i = 0;
        while i < pairs.len() {
            let (src, dst) = pairs[i];
            out |= (gnitz_wire::null_word_get(in_null, src as usize) as u64) << dst;
            i += 1;
        }
        out
    }

    /// Derive the null words of `out` rows `[dst_base, dst_base + n)` from
    /// source rows `[src_start, src_start + n)` (one u64 per row).
    ///
    /// Always writes the whole window, every arm included: the destination may
    /// be a recycled, uninitialized batch tail, so "already zero" is never
    /// available to assume.
    fn write_rows(&self, in_null_bmp: &[u8], src_start: usize, out: &mut [u8], dst_base: usize, n: usize) {
        let dst = &mut out[dst_base * 8..(dst_base + n) * 8];
        let pairs = match self {
            NullPerm::Zero => {
                dst.fill(0);
                return;
            }
            NullPerm::Mask(mask) => {
                for row in 0..n {
                    let in_null = gnitz_wire::read_u64_le(in_null_bmp, (src_start + row) * 8);
                    gnitz_wire::write_u64_le(dst, row * 8, in_null & mask);
                }
                return;
            }
            NullPerm::Permute(pairs) => pairs.as_slice(),
        };
        for row in 0..n {
            let in_null = gnitz_wire::read_u64_le(in_null_bmp, (src_start + row) * 8);
            gnitz_wire::write_u64_le(dst, row * 8, Self::permute(pairs, in_null));
        }
    }
}

// ---------------------------------------------------------------------------
// MapPlan
// ---------------------------------------------------------------------------

/// Map/projection: the resolved program's columnar moves and computed columns,
/// the null permutation, where the output PK comes from, and the output schema
/// it stamps.
pub(crate) struct MapPlan {
    /// The resolved program: the copy list, the two emit lists and the per-row
    /// kernel all read through it. Kept even for a pure projection, whose
    /// register file is a handful of empty `Vec`s that `ensure_capacity` never
    /// grows.
    ev: Evaluator,
    null_perm: NullPerm,
    /// Where the output PK region comes from.
    pk_source: PkSource,
    /// What the copy list does to the input's string columns — the blob mode's
    /// plan-time half.
    string_moves: StringMoves,
    out_schema: SchemaDescriptor,
}

/// The NULL half of an emit, shared by the scalar (`stride` 8) and string
/// (`stride` 16) loops: a NULL row's value slot reads as zero, and its bit is set
/// in the output bitmap.
///
/// A sparse bit-scan, not a per-row branch that would de-vectorize the value
/// store. The two bitmaps are transposed — register-major in the register file,
/// row-major in the output — so the merge is a scatter, and its read-modify-write
/// is what composes with `null_perm`'s earlier write and with any other emit.
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

/// Set every row's PK to a hash of its full payload content. Identical row
/// content (including null pattern and string/blob bytes) yields an identical
/// 128-bit PK; any difference yields a distinct PK. This implements full-row
/// set membership for EXCEPT/INTERSECT/DISTINCT.
///
/// Per payload column in order: a 1-byte null marker, then the column's content
/// — raw little-endian for fixed-width, length-prefixed for STRING/BLOB. That is
/// independent of inline-vs-heap string layout, so equal logical rows hash
/// equally.
///
/// Set membership is keyed on the hash alone, so a ~2^-64 birthday collision
/// silently coalesces two distinct elements. An accepted tradeoff, not a checked
/// error.
fn reindex_hash_row(out_schema: &SchemaDescriptor, output: &mut Batch, branch_id: u8) {
    let n = output.count;
    debug_assert!(
        out_schema.pk_stride() as usize <= gnitz_wire::NARROW_PK_MAX_BYTES,
        "reindex_hash_row: synthetic key stride exceeds NARROW_PK_MAX_BYTES"
    );
    // Hashing borrows the batch immutably and the write-back needs it mutably, so
    // the two cannot interleave per row. Buffering a chunk of keys on the stack
    // keeps both passes in cache and costs no allocation, whatever `n` is.
    const CHUNK: usize = 256;
    let mut keys = [0u128; CHUNK];
    let mut start = 0;
    while start < n {
        let end = (start + CHUNK).min(n);
        {
            let mb = output.as_mem_batch();
            // ~280-byte stack-allocated streaming hasher; `reset()` between rows
            // costs only a handful of word stores, and fixed-width columns are fed
            // straight from the column slot with no intermediate copy.
            let mut hasher = RowHasher::new();
            for row in start..end {
                hasher.reset();
                // Branch discriminator: distinguishes identical payloads arriving
                // on the left vs right side of a UNION ALL so they do not collide
                // to a single PK (which would collapse their +2 weight to +1).
                hasher.update(&[branch_id]);
                let null_word = mb.get_null_word(row);
                for (pi, col) in out_schema.payload_columns() {
                    let is_null = gnitz_wire::null_word_get(null_word, pi);
                    hasher.update(&[is_null as u8]);
                    if is_null {
                        continue;
                    }
                    if gnitz_wire::is_german_string(col.type_code) {
                        let sb = mb.get_col_ptr(row, pi, 16);
                        crate::schema::key::hash_german_string_content(&mut hasher, sb, mb.blob);
                    } else {
                        let cs = col.size() as usize;
                        hasher.update(mb.get_col_ptr(row, pi, cs));
                    }
                }
                keys[row - start] = hasher.digest128();
            }
        }
        // Straight into the PK region, hoisted once per chunk: the borrow dance
        // above is what forces the chunking, not a per-row accessor.
        let stride = output.pk_stride() as usize;
        let pk = output.pk_data_mut();
        for row in start..end {
            // Synthetic U128 (unsigned): OPK == big-endian, right-aligned into
            // the stride (and debug-checked to fit in it).
            let key = NarrowPkOpk::new(keys[row - start], stride);
            pk[row * stride..(row + 1) * stride].copy_from_slice(key.bytes());
        }
        start = end;
    }
    // An in-place PK rewrite can break (PK, payload) order.
    output.downgrade();
}

impl MapPlan {
    /// Map plan from a logical expression program. A pure projection is the
    /// special case where the program computes nothing and every sink is a
    /// column copy (see [`LogicalProgram::copy_cols`]): the plan reduces to the
    /// copy list + `null_perm`.
    pub(crate) fn from_map(
        logical: LogicalProgram,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
        pk_source: PkSource,
    ) -> Result<Self, ExprValidateErr> {
        let ev = logical.resolve_map(in_schema, out_schema)?;
        // Null permutation: copied columns carry their source null bit. Sources
        // that cannot have one are dropped inside `NullPerm::new`.
        let null_perm = NullPerm::new(ev.copies(), ev.nullable_slots());
        let string_moves = StringMoves::new(in_schema, ev.copies());

        Ok(MapPlan {
            ev,
            null_perm,
            pk_source,
            string_moves,
            out_schema: *out_schema,
        })
    }

    /// Map every `[start, end)` range of `src`, in list order, onto `keeper`'s
    /// tail — the ad-hoc rows sink's fused filter→project step, replacing an
    /// [`Self::evaluate_map_batch`] into a throwaway batch followed by
    /// `keeper.append_batch`.
    ///
    /// The PK region passes through verbatim ([`PkSource::Inherit`]), so the
    /// strides must agree; that is the rows-sink reply guard, and PK *type*
    /// parity is planner-guaranteed by the verbatim passthrough clone.
    pub(crate) fn append_map_ranges(&self, src: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
        debug_assert!(
            matches!(self.pk_source, PkSource::Inherit),
            "append_map_ranges: a stamped PK would overwrite the keeper's earlier chunks",
        );
        self.map_ranges_into(src, keeper, ranges);
    }

    /// Execute the map over a whole batch into a fresh output — the DBSP MAP
    /// instruction's whole body. The result is `Raw`: a payload-reordering
    /// projection over a duplicate-PK input can break (PK, payload) order (the
    /// D1 fail-safe).
    pub(crate) fn evaluate_map_batch(&self, in_batch: &Batch) -> Batch {
        let n = in_batch.count;
        if n == 0 {
            return Batch::empty_with_schema(&self.out_schema);
        }
        // Uninitialized: `validate` requires every map to write every output
        // payload slot, and `map_rows_into` writes the PK (or [`Self::stamp_pk`]
        // does), weight and null regions of every row.
        let mut output = Batch::with_capacity(self.out_schema, n);
        // When no string column is dropped, adopt the input blob wholesale; the
        // shared `blob_id` is then what tells `map_ranges_into` to copy every
        // String/Blob struct verbatim instead of relocating each cell.
        if self.string_moves == StringMoves::KeepsEveryString {
            output.share_blob_from(in_batch);
        }
        self.map_ranges_into(in_batch, &mut output, &[(0, n)]);
        debug_assert_eq!(output.count, n, "MAP output row count must equal input row count");
        self.stamp_pk(in_batch, &mut output);
        gnitz_debug!("map: in={} out={}", n, output.count);
        output
    }

    /// Stamp every row's PK per [`Self::pk_source`] — the two arms
    /// `map_rows_into` leaves the region alone for. After the payload, because
    /// the hash arm identifies the output row it reads.
    fn stamp_pk(&self, in_batch: &Batch, output: &mut Batch) {
        match &self.pk_source {
            PkSource::Inherit => {}
            PkSource::Pack(packer) => {
                debug_assert_eq!(output.pk_stride() as usize, packer.out_stride);
                let src = in_batch.as_mem_batch();
                let (n, stride) = (output.count, packer.out_stride);
                // Straight into the destination region: `pack_into` fully
                // overwrites the slot it is handed, so no staging buffer.
                let pk = output.pk_data_mut();
                for row in 0..n {
                    packer.pack_into(&mut pk[row * stride..(row + 1) * stride], &src, row);
                }
                // An in-place PK rewrite can break (PK, payload) order.
                output.downgrade();
            }
            PkSource::HashRow { branch_id } => reindex_hash_row(&self.out_schema, output, *branch_id),
        }
    }

    /// The one map driver: provision `out`'s tail for the ranges, pick the
    /// [`BlobMode`] off [`Batch::shares_blob_with`] (the same rule
    /// `Batch::append_ranges_inner` reads), and run [`Self::map_rows_into`] per
    /// range. The dedup cache spans the call and never outlives it: every range
    /// reads the same live `src`, while a later chunk's recycled blob buffer can
    /// reuse an address this one saw.
    ///
    /// Why [`COMPACT_RUN_LEN`] and not `MORSEL` bounds the compaction below:
    /// compaction buys a full extra copy of every survivor (~1.5–15 ns/row) to
    /// save the *per-range* kernel setup — one eval prologue, one scratch borrow
    /// — a few hundred cycles. Runs longer than that already amortize it,
    /// whatever the morsel width.
    fn map_ranges_into(&self, src: &Batch, out: &mut Batch, ranges: &[(usize, usize)]) {
        let total = crate::storage::range_rows(ranges);
        if total == 0 {
            return;
        }
        // Compact a kernel-starving list into one contiguous range: the kernel
        // evaluates one morsel *per range*, so a list of 1-row ranges would
        // collapse it to row-at-a-time. A pure gather copies range-wise anyway.
        //
        // A *relocating* gather, not `Batch::from_ranges`: that one shares the
        // source heap wholesale, dead bytes included, and this arm fires exactly
        // on the selective-predicate-plus-string shape where most of it is dead.
        let starves_kernel = self.ev.emits_anything() && ranges.len() > 1 && total < ranges.len() * COMPACT_RUN_LEN;
        let compacted;
        let (src, ranges) = if starves_kernel {
            compacted = {
                let mut c = Batch::with_capacity(src.schema, total);
                c.append_ranges(src, ranges);
                c
            };
            (&compacted, &[(0, total)][..])
        } else {
            (src, ranges)
        };

        let old = out.count;
        out.reserve_rows(total);
        // Publish the new rows up front: the `*_mut` accessors are `count`-bounded,
        // so `[old, old + total)` must be inside `count` before any write.
        out.count = old + total;

        let shares_blob = out.shares_blob_with(&src.as_mem_batch());
        // Worth a TLS pool pop only when some *copy* relocates a cell: a map
        // whose STRING output comes only from `str_emits` (`SELECT id,
        // UPPER(name)`) has no possible cache entry.
        let mut cache = match self.string_moves != StringMoves::NoStrings && !shares_blob {
            true => crate::storage::BlobCacheGuard::acquire(&self.out_schema, total),
            false => crate::storage::BlobCacheGuard::empty(),
        };
        // A different question: does this plan grow the output heap at all — a
        // string emit does, with no cache entry to its name. This is the only
        // presize `out.blob` ever gets, so dropping it trades one malloc for
        // geometric regrowth.
        if self.out_schema.has_german_string() && !shares_blob && !src.blob.is_empty() {
            out.blob
                .reserve(crate::storage::prorated_blob_cap(src.blob.len(), src.count, total));
        }
        let mut dst = old;
        for &(start, end) in ranges {
            let w = RowWindow {
                src: start,
                dst,
                n: end - start,
            };
            let blob = match shares_blob {
                true => BlobMode::Verbatim,
                false => BlobMode::Relocate(cache.get_mut()),
            };
            self.map_rows_into(src, out, w, blob);
            dst += w.n;
        }

        // Matches `append_batch`: nothing downstream trusts the destination's
        // order (a payload-reordering projection over a duplicate-PK input can
        // break (PK, payload) order — the D1 fail-safe).
        out.downgrade();
    }

    /// Map one row window: PK/weight passthrough, null permutation, column
    /// moves, then the compute kernel. `out.count` must already cover the
    /// destination window — every `*_mut` accessor is `count`-bounded.
    fn map_rows_into(&self, in_batch: &Batch, output: &mut Batch, w: RowWindow, mut blob: BlobMode<'_>) {
        let RowWindow {
            src: src_start,
            dst: dst_base,
            n,
        } = w;
        if let PkSource::Inherit = self.pk_source {
            let pk_st = in_batch.pk_stride() as usize;
            debug_assert_eq!(
                pk_st,
                output.pk_stride() as usize,
                "PkSource::Inherit: PK stride mismatch"
            );
            output.pk_data_mut()[dst_base * pk_st..(dst_base + n) * pk_st]
                .copy_from_slice(&in_batch.pk_data()[src_start * pk_st..(src_start + n) * pk_st]);
        }
        output.weight_data_mut()[dst_base * 8..(dst_base + n) * 8]
            .copy_from_slice(&in_batch.weight_data()[src_start * 8..(src_start + n) * 8]);

        // Null bitmap, written before the compute kernel: an emit merges its null
        // bits into the word with a read-modify-write `|=`. Split-borrow:
        // `in_batch` and `output` are distinct allocations.
        {
            let in_nb = in_batch.null_bmp_data();
            self.null_perm
                .write_rows(in_nb, src_start, output.null_bmp_data_mut(), dst_base, n);
        }

        for c in self.ev.copies() {
            copy_column(in_batch, output, c, blob.reborrow(), w);
        }

        // Compute kernel
        if self.ev.emits_anything() {
            let in_mb = in_batch.as_mem_batch();
            self.ev.eval_morsels(&in_mb, src_start, n, |morsel_start, out| {
                let m = out.rows();
                // Emits: write each computed register to its output column. One
                // `Iterator::next` per emit, against a `copy_from_slice` of up
                // to 2 KiB — unlike `NullPerm`'s per-row loop, where the call
                // would land per (row × pair).
                let row0 = dst_base + morsel_start;
                for &(reg, out_payload) in self.ev.scalar_emits() {
                    let (reg, out_payload) = (reg as usize, out_payload as usize);
                    // One split borrow: the value slots and this column's bit in
                    // the row-major NULL bitmap are written in the same pass over
                    // the null rows.
                    let (col, nb, _) = output.col_null_and_blob_mut(out_payload);

                    // One blit: the morsel's rows are contiguous in the column.
                    let win = &mut col[row0 * 8..(row0 + m) * 8];
                    debug_assert_eq!(win.len(), out.reg_bytes(reg).len(), "scalar emit stride is not 8");
                    win.copy_from_slice(out.reg_bytes(reg));

                    emit_null_rows(out, reg, win, nb, row0, out_payload, 8);
                }

                // String emits. Two passes like the scalar path rather than a
                // per-row nullness branch, because `MorselOut` exposes nullness
                // only through `for_each_null_row` — and under `no_nulls` there
                // is no `null_bits` to index at all.
                for &(reg, out_payload) in self.ev.str_emits() {
                    let (reg, out_payload) = (reg as usize, out_payload as usize);
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
    use gnitz_expr::{ExprValidateErr, LogicalProgram, Reg, Sink};

    use super::{MapPlan, PkSource};
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
    use crate::storage::Batch;

    /// Build a `Batch` of `(pk, weight, null_word, payload i64 cells)` rows against
    /// `schema` — the engine-side physical batch these tests drive [`MapPlan`]
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
        let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
        let result = func.evaluate_map_batch(&batch);
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

        use gnitz_expr::{IntArithOp, LogicalInstr};
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ];
        let sinks = vec![Sink::Col(1), Sink::Reg(Reg(2))];
        let prog = LogicalProgram::new(instrs, sinks, None, vec![]);

        let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
        let result = func.evaluate_map_batch(&batch);
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

        let func = MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema, PkSource::Inherit).unwrap();
        let result = func.evaluate_map_batch(&batch);
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
            let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
            let out = func.evaluate_map_batch(&batch);
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
            let func = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit).unwrap();
            let out = func.evaluate_map_batch(&batch);
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
        let out = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit)
            .unwrap()
            .evaluate_map_batch(&batch);

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
        let out = MapPlan::from_map(prog, &in_schema, &out_schema, PkSource::Inherit)
            .unwrap()
            .evaluate_map_batch(&batch);

        assert_eq!(out.count, 1);
        let widened = |pi: usize| i64::from_le_bytes(out.col_data(pi)[..8].try_into().unwrap());
        assert_eq!(widened(0), c0 as i64, "U16 PK zero-extends");
        assert_eq!(widened(1), c1 as i64, "I16 PK sign-extends");
        assert_eq!(widened(2), c2 as i64, "I8 payload sign-extends");
        assert_eq!(widened(3), c3 as i64, "U8 payload zero-extends");
    }

    /// An instruction-free program has no result register — framing accepts it,
    /// and `validate` cannot reject it outright because that is exactly the shape
    /// every `copy_cols` map has. The *filter* and *scalar* roles are the
    /// ones that read a result register back, so each rejects it rather than letting
    /// it masquerade as a filter that passes nothing (which a client would read as
    /// an empty table).
    #[test]
    fn test_register_free_predicate_is_rejected() {
        let schema = make_schema(0, &[8, 9]);
        let prog = LogicalProgram::from_wire(&[], &[], None, vec![]).unwrap();
        assert_eq!(
            prog.resolve_filter(&schema).err(),
            Some(ExprValidateErr::ResultRegRequired)
        );
        // The same shape is legitimate as a map: `copy_cols` builds it.
        assert!(MapPlan::from_map(LogicalProgram::copy_cols(&[1]), &schema, &schema, PkSource::Inherit).is_ok());
    }

    /// The predicate wiring over a real `Batch`, including the range coalescing
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
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: 15 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                a: Reg(0),
                b: Reg(1),
            },
        ];
        let func = LogicalProgram::new(instrs, Vec::new(), Some(Reg(2)), vec![])
            .resolve_filter(&schema)
            .unwrap();

        let mut ranges = Vec::new();
        func.filter_ranges(&batch.as_mem_batch(), &mut ranges);
        assert_eq!(ranges, vec![(1, 3), (4, 6)]);

        // `out` is cleared, not appended to, so a reused buffer cannot leak a
        // previous chunk's ranges into this one.
        func.filter_ranges(&batch.as_mem_batch(), &mut ranges);
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
        // The copied column is the PK, which `PkSource::Inherit` carries verbatim; give
        // the output's first payload slot something to hold.
        batch.count = 2;

        let instrs = vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ];
        let sinks = vec![Sink::Col(0), Sink::Reg(Reg(1))];
        let func = MapPlan::from_map(
            LogicalProgram::new(instrs, sinks, None, vec![]),
            &in_schema,
            &out_schema,
            PkSource::Inherit,
        )
        .unwrap();
        let out = func.evaluate_map_batch(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ABC");
        assert_eq!(
            crate::test_support::read_german_string(&out, 1, 1),
            b"A-LONG-VALUE-PAST-TWELVE",
            "a heap-backed value must land in the output's own blob"
        );
    }

    /// A string emit alongside a passthrough of every input string column. The
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
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ];
        let sinks = vec![Sink::Col(1), Sink::Reg(Reg(1))];
        let func = MapPlan::from_map(
            LogicalProgram::new(instrs, sinks, None, vec![]),
            &in_schema,
            &out_schema,
            PkSource::Inherit,
        )
        .unwrap();
        let out = func.evaluate_map_batch(&batch);

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
            !out.shares_blob_with(&batch.as_mem_batch()),
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
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ];
        let sinks = vec![Sink::Col(0), Sink::Reg(Reg(1))];
        let func = MapPlan::from_map(
            LogicalProgram::new(instrs, sinks, None, vec![]),
            &in_schema,
            &out_schema,
            PkSource::Inherit,
        )
        .unwrap();
        let out = func.evaluate_map_batch(&batch);

        assert_eq!(crate::test_support::read_german_string(&out, 1, 0), b"ABC");
        assert_eq!(&out.col_data(1)[16..32], &[0u8; 16], "a NULL cell is all zeros");
        assert_eq!(
            gnitz_wire::read_u64_le(out.null_bmp_data(), 8) & 2,
            2,
            "the output bitmap bit for the string column must be set"
        );
    }

    #[test]
    fn map_with_pack_pk_source_promotes_payload_to_pk() {
        use crate::schema::key::ReindexPacker;
        use crate::test_support::{make_batch, make_schema_u64_i64};
        // `PkSource::Pack` rewrites the output PK by reading the referenced
        // column through the reindex packer. Verifies (1) every row's output PK
        // matches the source column value, (2) the resulting batch is correctly
        // marked unsorted/unconsolidated (the stamp destroys PK order).

        // Input: PK u64, payload i64. Reindex on the payload (col 1) — the
        // new output PK is each row's payload value.
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 200), (2, 1, 100), (3, 1, 300)]);

        // Projection plan: output keeps the same single payload column.
        let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
        let plan = MapPlan::from_map(
            LogicalProgram::copy_cols(&[1]),
            &schema,
            &schema,
            PkSource::Pack(packer),
        )
        .unwrap();
        let out = plan.evaluate_map_batch(&batch);
        assert_eq!(out.count, 3);
        // Each output row's PK is the sign-aware OPK image of its source payload
        // value (col 1 is I64): `widen_pk_be(encode_pk_column(v))`, i.e. the value
        // with its sign bit flipped, matching how `ColumnLocator::route_key` routes the same
        // value. A raw-native `== 200` assertion would falsely fail signed reindex.
        let opk_i64 = |v: i64| ((v as u64) ^ 0x8000_0000_0000_0000) as u128;
        assert_eq!(out.get_pk(0), opk_i64(200));
        assert_eq!(out.get_pk(1), opk_i64(100));
        assert_eq!(out.get_pk(2), opk_i64(300));
        // Payload itself is unchanged by the projection.
        let payload = |row: usize| gnitz_wire::read_i64_le(out.col_data(0), row * 8);
        assert_eq!(payload(0), 200);
        assert_eq!(payload(1), 100);
        assert_eq!(payload(2), 300);
        // The PK stamp destroys PK order — output must be marked accordingly.
        assert!(!out.is_sorted(), "a stamped PK must not be marked sorted");
        assert!(!out.is_consolidated(), "a stamped PK must not be marked consolidated");
    }

    // -----------------------------------------------------------------------
    // Benchmark
    // -----------------------------------------------------------------------

    /// The whole `Instr::Map` body — the copy loop plus the PK stamp — over a
    /// reindex map and a string-emitting map. `reindex_pack_bench` covers the
    /// packer alone, and neither `make bench` nor the `scan_spec` benches
    /// resolve this loop.
    ///
    /// `cd crates && cargo test -p gnitz-engine --release map_ranges_bench -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn map_ranges_bench() {
        use crate::schema::key::ReindexPacker;
        use gnitz_expr::LogicalInstr;
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 200_000;
        const ITERS: usize = 20;

        // --- Reindex map: [U64 PK, I64, I64] reindexed on col 1, both payload
        // columns kept — the equijoin / GROUP BY repartition shape.
        let rx_in = make_schema(0, &[type_code::U64, type_code::I64, type_code::I64]);
        let mut rx_batch = Batch::with_capacity(rx_in, N);
        for i in 0..N as u64 {
            rx_batch.extend_pk(i as u128);
            rx_batch.extend_weight(&1i64.to_le_bytes());
            rx_batch.extend_null_bmp(&0u64.to_le_bytes());
            rx_batch.extend_col(0, &i.wrapping_mul(2_654_435_761).to_le_bytes());
            rx_batch.extend_col(1, &(!i).to_le_bytes());
            rx_batch.count += 1;
        }
        let rx_packer = ReindexPacker::new(&rx_in, &[1], &[]).unwrap();
        let rx_out = rx_packer.output_schema(&rx_in, &[1, 2]).unwrap();
        let rx_plan = MapPlan::from_map(
            LogicalProgram::copy_cols(&[1, 2]),
            &rx_in,
            &rx_out,
            PkSource::Pack(rx_packer),
        )
        .unwrap();

        // --- String-emitting map: [U64 PK, STRING] -> [U64 PK, STRING upper],
        // the shape whose STRING output comes only from `str_emits`.
        let se_in = make_schema(0, &[type_code::U64, type_code::STRING]);
        let se_out = make_schema(0, &[type_code::U64, type_code::STRING]);
        let mut se_batch = Batch::with_capacity(se_in, N);
        for i in 0..N {
            se_batch.extend_pk(i as u128);
            se_batch.extend_weight(&1i64.to_le_bytes());
            se_batch.extend_null_bmp(&0u64.to_le_bytes());
            // Past SHORT_STRING_THRESHOLD, so every cell is heap-backed and the
            // emit grows the output blob.
            let v = format!("row-{i:012}-payload");
            let cell = gnitz_wire::encode_german_string(v.as_bytes(), &mut se_batch.blob);
            se_batch.extend_col(0, &cell);
            se_batch.count += 1;
        }
        let se_plan = MapPlan::from_map(
            LogicalProgram::new(
                vec![
                    LogicalInstr::LoadColStr { col: 1 },
                    LogicalInstr::StrCase { a: Reg(0), upper: true },
                ],
                vec![Sink::Reg(Reg(1))],
                None,
                vec![],
            ),
            &se_in,
            &se_out,
            PkSource::Inherit,
        )
        .unwrap();

        for (name, plan, src) in [("reindex", &rx_plan, &rx_batch), ("str_emit", &se_plan, &se_batch)] {
            let t0 = Instant::now();
            let mut acc = 0usize;
            for _ in 0..ITERS {
                let out = plan.evaluate_map_batch(black_box(src));
                acc = acc.wrapping_add(out.count).wrapping_add(out.blob.len());
                black_box(&out);
            }
            let secs = t0.elapsed().as_secs_f64();
            println!(
                "map_ranges_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, checksum {acc})",
                (N * ITERS) as f64 / secs / 1e6,
            );
        }
    }
}
