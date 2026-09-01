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

use gnitz_expr::{Evaluator, ExprValidateErr, LogicalProgram};

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
pub enum PkSource {
    /// Copy the input PK region verbatim. Requires equal PK strides, which a
    /// compiled circuit gets by construction (every `Inherit` node derives its
    /// output schema from the input's own PK region); the one enforcement point
    /// is the ad-hoc rows-sink guard over a client-supplied reply schema.
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
    pub(crate) fn new(in_schema: &SchemaDescriptor, copies: &[ColCopy]) -> Self {
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
    pub(crate) fn new(copies: &[ColCopy], nullable: u64) -> Self {
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
pub struct MapPlan {
    /// The resolved program: the copy list, the two emit lists and the per-row
    /// kernel all read through it. Kept even for a pure projection, whose
    /// register file is empty — it has no registers to size.
    ev: Evaluator,
    null_perm: NullPerm,
    /// Where the output PK region comes from.
    pk_source: PkSource,
    /// What the copy list does to the input's string columns — the blob mode's
    /// plan-time half.
    string_moves: StringMoves,
    out_schema: SchemaDescriptor,
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
    pub fn from_map(
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
    pub fn evaluate_map_batch(&self, in_batch: &Batch) -> Batch {
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

                    out.write_null_rows(reg, win, nb, row0, out_payload);
                }

                // String emits. Two passes like the scalar path rather than a
                // per-row nullness branch, because `MorselOut` exposes nullness
                // only through `write_null_rows` — and under `no_nulls` there
                // is no `null_bits` to index at all. Both passes live on
                // `MorselOut`, so the per-row loops run at gnitz-expr's
                // opt-level rather than this crate's.
                for &(reg, out_payload) in self.ev.str_emits() {
                    let (reg, out_payload) = (reg as usize, out_payload as usize);
                    let (col, nb, blob) = output.col_null_and_blob_mut(out_payload);
                    let win = &mut col[row0 * 16..(row0 + m) * 16];
                    out.write_str_cells(reg, win, blob);
                    out.write_null_rows(reg, win, nb, row0, out_payload);
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/expr.rs"]
mod tests;
