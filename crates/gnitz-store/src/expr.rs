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
use crate::schema::key::ReindexPacker;
use crate::schema::{ColumnLocator, DerivedSchema, OpBuildErr, SchemaColumn, SchemaDescriptor};
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
    /// across every column and row of one map; `None` relocates without dedup.
    Relocate(Option<&'a mut crate::storage::BlobCache>),
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
    blob: &mut BlobMode<'_>,
    w: RowWindow,
) {
    let RowWindow { src: src_start, dst: dst_base, n } = w;
    if n == 0 {
        // Every arm below is a no-op over an empty window, and the PK arm's
        // source span is expressed off row `n - 1`.
        return;
    }
    let dst_payload = dst_payload as usize;
    let stride = dst_stride as usize; // destination write width

    // Destructured ONCE before the row loops, so the per-row bodies below carry
    // no locator dispatch (and no `native_le_bytes` by-value [u8; 16]).
    match src_loc {
        ColumnLocator::Pk { byte_off, size, type_code } => {
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
                // The width is unswitched out too, so `decode_pk_column` takes
                // fixed-size arrays and its own width match folds away.
                macro_rules! decode_rows {
                    ($w:expr) => {{
                        const W: usize = $w;
                        // Both windows cut once, as `Instr::LoadPk`'s row loop
                        // does: three bounds checks per row the loop drops.
                        let base = src_start * pk_stride + pk_off;
                        let cells = &pk[base..base + (n - 1) * pk_stride + W];
                        let out = &mut dst[dst_base * W..(dst_base + n) * W];
                        for (d, k) in out.chunks_exact_mut(W).zip((0..n).map(|i| i * pk_stride)) {
                            let s: &[u8; W] = cells[k..k + W].try_into().unwrap();
                            gnitz_wire::decode_pk_column(s, type_code, d.try_into().unwrap());
                        }
                    }};
                }
                // `decode_pk_column`'s own domain: a PK column is 1, 2, 4, 8 or
                // 16 bytes, and it says so itself on anything else.
                match stride {
                    1 => decode_rows!(1),
                    2 => decode_rows!(2),
                    4 => decode_rows!(4),
                    8 => decode_rows!(8),
                    16 => decode_rows!(16),
                    other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
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
            if let (true, BlobMode::Relocate(cache)) = (gnitz_wire::is_german_string(type_code), blob) {
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
    /// Where the output PK region comes from.
    pk_source: PkSource,
    /// Some copy carries a German-string column, so a cell can be relocated.
    copies_a_string: bool,
    /// The input has German-string columns and every one is carried by a copy,
    /// so its heap holds no bytes the output would adopt dead.
    keeps_every_string: bool,
    /// This map reproduces its input row unchanged, so a caller holding the input
    /// can hand it on instead of running the plan.
    is_identity: bool,
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
fn reindex_hash_row(output: &mut Batch, branch_id: u8) {
    let n = output.count;
    // The PK *is* the digest, so the OPK region is its big-endian bytes.
    const KEY_BYTES: usize = std::mem::size_of::<u128>();
    assert_eq!(
        output.pk_stride() as usize,
        KEY_BYTES,
        "a hash-row PK is one U128 column"
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
            // Stack-allocated streaming hasher; `reset()` between rows costs
            // only a handful of word stores, and fixed-width columns are fed
            // straight from the column slot with no intermediate copy.
            let mut hasher = RowHasher::new();
            for row in start..end {
                hasher.reset();
                // Branch discriminator: distinguishes identical payloads arriving
                // on the left vs right side of a UNION ALL so they do not collide
                // to a single PK (which would collapse their +2 weight to +1).
                hasher.update(&[branch_id]);
                let null_word = mb.get_null_word(row);
                for (pi, col) in output.schema.payload_columns() {
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
        let pk = &mut output.pk_data_mut()[start * KEY_BYTES..end * KEY_BYTES];
        for (key, dst) in keys.iter().zip(pk.as_chunks_mut::<KEY_BYTES>().0) {
            *dst = key.to_be_bytes();
        }
        start = end;
    }
    // An in-place PK rewrite can break (PK, payload) order.
    output.downgrade();
}

/// Output schema of a computed-projection `Map`: the input's PK region (the map
/// inherits it verbatim, [`PkSource::Inherit`]), then one payload column per
/// declared `(type_code, nullable)` slot. The declared slots ARE the layout;
/// `MapPlan::from_map` is what catches a program disagreeing with them.
fn compute_map_output_schema(
    in_schema: &SchemaDescriptor,
    out_cols: &[(u8, bool)],
) -> Result<SchemaDescriptor, OpBuildErr> {
    let over = || OpBuildErr::shape("compute map: output exceeds MAX_COLUMNS");
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema).ok_or_else(over)?;
    for &(tc, nullable) in out_cols {
        b.push(SchemaColumn::new(tc, nullable as u8)).ok_or_else(over)?;
    }
    Ok(b.finish())
}

/// Output schema of a HashRow (set-op full-row identity) Map: a synthetic U128
/// PK at slot 0, then the projected payload columns, each promoted to its
/// carried target but keeping THIS SIDE's nullability. Per-side, not the
/// operator-merged view nullability: an INTERSECT/EXCEPT leaf is `distinct`-ed
/// before the tuple-tightening combine, so its row comparator must classify by
/// what this side can actually emit.
///
/// An absent target keeps the SOURCE type — not `gnitz_wire::resolve_reindex_type`,
/// which would derive a *key* type and land a payload column on U128. Typing the
/// output column at the target is what puts the promotion in front of
/// `check_copy_types`, inside `from_map`.
fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    cols: &[gnitz_wire::ReindexSlot],
) -> Result<SchemaDescriptor, OpBuildErr> {
    let over = || OpBuildErr::shape("hash-row map: output exceeds MAX_COLUMNS");
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(crate::schema::type_code::U128, 0))
        .ok_or_else(over)?;
    for &(c, tgt) in cols {
        let src = in_schema
            .column(c as usize)
            .ok_or_else(|| OpBuildErr::oob_col("hash-row map: column", c, in_schema))?;
        b.push(SchemaColumn::new(tgt.map_or(src.type_code, |t| t as u8), src.nullable))
            .ok_or_else(over)?;
    }
    Ok(b.finish())
}

impl MapPlan {
    /// The plan for a circuit's MAP node: one derivation of its `(output schema,
    /// map program, PK source)` triple, and the trust boundary each kind's
    /// client-supplied column list clears. Every kind ends in the same
    /// [`Self::from_map`], so an elided map is validated like any other.
    pub fn from_wire(in_schema: &SchemaDescriptor, mk: &gnitz_wire::MapKind) -> Result<Self, OpBuildErr> {
        let (out_schema, prog, pk_source) = match mk {
            gnitz_wire::MapKind::Compute(map) => return Self::from_compute_map(in_schema, map),

            gnitz_wire::MapKind::Reindex { keep, key, .. } => {
                // The packer is built first because it *is* the layout: its output
                // schema reads the promoters the per-row pack writes through, so
                // the reindexed `_join_pk` and the delta scatter co-partition by
                // construction. Same packer the exchange scatter builds from the
                // circuit's own slots.
                let packer = ReindexPacker::new(in_schema, key)?;
                let out_schema = packer.output_schema(in_schema, keep)?;
                (out_schema, LogicalProgram::copy_cols(keep), PkSource::Pack(packer))
            }

            gnitz_wire::MapKind::HashRow { cols, branch_id } => {
                let out_schema = hashrow_output_schema(in_schema, cols)?;
                let proj: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
                (
                    out_schema,
                    LogicalProgram::copy_cols(&proj),
                    PkSource::HashRow { branch_id: *branch_id },
                )
            }

            gnitz_wire::MapKind::Projection(cols) => {
                // A *payload* column, not merely an in-range one: `project_schema`
                // skips a PK index while `copy_cols` still numbers a sink for it.
                for &c in cols {
                    if in_schema.try_payload_idx(c as usize).is_none() {
                        return Err(OpBuildErr::shape(format!(
                            "projection map: column {c} is not a payload column of a {}-column schema",
                            in_schema.num_columns()
                        )));
                    }
                }
                // `cols` is bounded per entry but not in length, and duplicates
                // are legal, so a long list still overruns the fixed schema array.
                let out_schema = crate::schema::project_schema(in_schema, cols)
                    .ok_or_else(|| OpBuildErr::shape("projection map: output exceeds MAX_COLUMNS"))?;
                (out_schema, LogicalProgram::copy_cols(cols), PkSource::Inherit)
            }
        };
        Self::from_map(prog, in_schema, &out_schema, pk_source)
            .map_err(|e| OpBuildErr::Program("map: program/schema mismatch", e))
    }

    /// A computed projection: [`gnitz_wire::MapKind::Compute`]'s whole body, and
    /// also a read spec's pre-map. The output schema is derived, never shipped —
    /// the map inherits the input's PK region verbatim ([`PkSource::Inherit`]),
    /// so no caller can describe a PK region the map does not produce.
    pub(crate) fn from_compute_map(
        in_schema: &SchemaDescriptor,
        map: &gnitz_wire::ComputeMap,
    ) -> Result<Self, OpBuildErr> {
        let out_schema = compute_map_output_schema(in_schema, &map.out_cols)?;
        // The only map whose program is client bytes; every other kind builds
        // one from a column list. Rejected, not skipped: skipping a corrupt blob
        // would leave the output at the default empty schema.
        let prog = LogicalProgram::from_blob(&map.program, "map")
            .map_err(|e| OpBuildErr::Program("map: invalid program", e))?;
        Self::from_map(prog, in_schema, &out_schema, PkSource::Inherit)
            .map_err(|e| OpBuildErr::Program("map: program/schema mismatch", e))
    }

    /// Map plan from a logical expression program. A pure projection is the
    /// special case where the program computes nothing and every sink is a
    /// column copy (see [`LogicalProgram::copy_cols`]): the plan reduces to the
    /// copy list and the resolved program's null permutation.
    pub fn from_map(
        logical: LogicalProgram,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
        pk_source: PkSource,
    ) -> Result<Self, ExprValidateErr> {
        // Read before `resolve_map` consumes the logical form. Only under
        // `Inherit`: a reindex or hash-row overwrites every row's PK.
        let is_identity = matches!(pk_source, PkSource::Inherit)
            && in_schema.same_physical_layout(out_schema)
            && logical.sequential_copy_base() == Some(in_schema.pk_indices().len());
        let ev = logical.resolve_map(in_schema, out_schema)?;
        let copies_a_string = ev
            .copies()
            .iter()
            .any(|c| gnitz_wire::is_german_string(c.0.type_code()));
        // A copy's source locator is exactly what `locate` gives for its column.
        let is_copied = |ci| ev.copies().iter().any(|c| c.0 == in_schema.locate(ci));
        let keeps_every_string = in_schema.has_german_string()
            && (0..in_schema.num_columns())
                .filter(|&ci| gnitz_wire::is_german_string(in_schema.columns[ci].type_code))
                .all(is_copied);

        Ok(MapPlan {
            ev,
            pk_source,
            copies_a_string,
            keeps_every_string,
            is_identity,
            out_schema: *out_schema,
        })
    }

    /// The schema this plan stamps on its output.
    pub fn out_schema(&self) -> &SchemaDescriptor {
        &self.out_schema
    }

    /// True iff running this map would reproduce its input batch. A compiler
    /// elides such a node entirely and lets its consumers read the input.
    pub fn is_identity(&self) -> bool {
        self.is_identity
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
            "append_map_ranges: any other source leaves the keeper's PK region unwritten",
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
        // Uninitialized: `validate` makes every map write every payload slot,
        // and the two calls below cover the PK, weight and null regions.
        let mut output = Batch::with_capacity(&self.out_schema, n);
        // When no string column is dropped, adopt the input blob wholesale; the
        // shared `blob_id` is then what tells `map_ranges_into` to copy every
        // String/Blob struct verbatim instead of relocating each cell.
        if self.keeps_every_string {
            output.share_blob_from(in_batch);
        }
        self.map_ranges_into(in_batch, &mut output, &[(0, n)]);
        // The one source that keys on the finished output row.
        if let PkSource::HashRow { branch_id } = &self.pk_source {
            reindex_hash_row(&mut output, *branch_id);
        }
        output
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
                let mut c = Batch::with_capacity(&src.schema, total);
                c.append_ranges(&src.as_mem_batch(), ranges);
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
        // Worth a TLS pool pop only when some *copy* relocates a cell.
        let mut cache = match self.copies_a_string && !shares_blob {
            true => crate::storage::BlobCacheGuard::acquire(&out.schema, total),
            false => crate::storage::BlobCacheGuard::empty(),
        };
        // A different question: does this plan grow the output heap at all — a
        // string emit does, with no cache entry to its name. This is the only
        // presize `out.blob` ever gets, so dropping it trades one malloc for
        // geometric regrowth.
        if out.schema.has_german_string() && !shares_blob && !src.blob.is_empty() {
            out.reserve_blob(crate::storage::prorated_blob_cap(src.blob.len(), src.count, total));
        }
        let mut dst = old;
        for &(start, end) in ranges {
            let w = RowWindow { src: start, dst, n: end - start };
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

    /// Map one row window: PK, weight, null permutation, column moves, then the
    /// compute kernel. `out.count` must already cover the destination window —
    /// every `*_mut` accessor is `count`-bounded.
    fn map_rows_into(&self, in_batch: &Batch, output: &mut Batch, w: RowWindow, mut blob: BlobMode<'_>) {
        let RowWindow { src: src_start, dst: dst_base, n } = w;
        // Both PK sources that read the *input* row, so both belong to the
        // window rather than to a pass over the finished batch.
        match &self.pk_source {
            PkSource::Inherit => {
                let pk_st = in_batch.pk_stride() as usize;
                debug_assert_eq!(
                    pk_st,
                    output.pk_stride() as usize,
                    "PkSource::Inherit: PK stride mismatch"
                );
                output.pk_data_mut()[dst_base * pk_st..(dst_base + n) * pk_st]
                    .copy_from_slice(&in_batch.pk_data()[src_start * pk_st..(src_start + n) * pk_st]);
            }
            PkSource::Pack(packer) => {
                debug_assert_eq!(output.pk_stride() as usize, packer.out_stride);
                let src = in_batch.as_mem_batch();
                let stride = packer.out_stride;
                // `pack_into` overwrites the slot it is handed, so no staging buffer.
                let pk = &mut output.pk_data_mut()[dst_base * stride..(dst_base + n) * stride];
                for (i, dst) in pk.chunks_exact_mut(stride).enumerate() {
                    packer.pack_into(dst, &src, src_start + i);
                }
                // An in-place PK rewrite can break (PK, payload) order.
                output.downgrade();
            }
            // Hashes the finished output row, so `evaluate_map_batch` stamps it
            // once the payload below is written.
            PkSource::HashRow { .. } => {}
        }
        output.weight_data_mut()[dst_base * 8..(dst_base + n) * 8]
            .copy_from_slice(&in_batch.weight_data()[src_start * 8..(src_start + n) * 8]);

        // Null bitmap, written before the compute kernel: an emit merges its null
        // bits into the word with a read-modify-write `|=`. Split-borrow:
        // `in_batch` and `output` are distinct allocations.
        {
            let in_nb = in_batch.null_bmp_data();
            self.ev
                .null_perm()
                .write_rows(in_nb, src_start, output.null_bmp_data_mut(), dst_base, n);
        }

        for c in self.ev.copies() {
            copy_column(in_batch, output, c, &mut blob, w);
        }

        // Compute kernel
        if self.ev.emits_anything() {
            let in_mb = in_batch.as_mem_batch();
            self.ev.eval_morsels(&in_mb, src_start, n, |morsel_start, out| {
                // Emits: write each computed register to its output column. One
                // `Iterator::next` per emit, against a `copy_from_slice` of up
                // to 2 KiB — unlike `NullPerm`'s per-row loop, where the call
                // would land per (row × pair).
                let row0 = dst_base + morsel_start;
                for &(reg, out_payload, stride) in self.ev.scalar_emits() {
                    let (reg, out_payload) = (reg as usize, out_payload as usize);
                    // One split borrow: the value slots and this column's bit in
                    // the row-major NULL bitmap are written in the same pass over
                    // the null rows.
                    let (col, nb, _) = output.col_null_and_blob_mut(out_payload);
                    out.emit_scalar_cells(reg, col, nb, row0, out_payload, stride as usize);
                }

                // String emits. Two passes inside the emit rather than a per-row
                // nullness branch, because under `no_nulls` there is no
                // `null_bits` to index at all. Both passes live on `MorselOut`,
                // so the per-row loops run at gnitz-expr's opt-level rather than
                // this crate's.
                for &(reg, out_payload) in self.ev.str_emits() {
                    let (reg, out_payload) = (reg as usize, out_payload as usize);
                    let (col, nb, blob) = output.col_null_and_blob_mut(out_payload);
                    out.emit_str_cells(reg, col, nb, blob, row0, out_payload);
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
