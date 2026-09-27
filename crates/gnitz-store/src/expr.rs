//! [`MapPlan`] — the columnar driver behind every DBSP map and projection.
//!
//! A filter needs nothing on top of the resolved program and is a bare
//! `gnitz_expr::RowFilter` wherever one is held; a map is this plan, which adds
//! the PK, weight and column moves around the computed columns
//! `gnitz_expr::MapEval` writes.
//!
//! The evaluator itself lives in `gnitz-expr`; this module is a consumer of that
//! crate, not its home, so `LogicalProgram`, the instruction model and the
//! resolved form are named `gnitz_expr::` at each call site rather than
//! re-exported here.

use gnitz_expr::{ColCopy, ExprValidateErr, LogicalProgram, MapEval};

use crate::schema::key::{locate_key_col, FoldCols, ReindexPacker};
use crate::schema::{ColumnLocator, DerivedSchema, OpBuildErr, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode};
use crate::storage::Batch;

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

/// Average survivor-run length below which a computing map copies its survivors
/// into one range first: below it, the copy costs less than the kernel's setup
/// per range.
const COMPACT_RUN_LEN: usize = 128;

/// [`COMPACT_RUN_LEN`] for a copy-only map packing a reindex key, whose
/// per-range setup is smaller.
const PACK_COMPACT_RUN_LEN: usize = 24;

/// Where a map's output PK region comes from. Owned by the plan rather than
/// passed per call, so the region cannot be left unwritten between two
/// statements and no caller can pair a plan with the wrong stamp.
pub enum PkSource {
    /// Copy the input PK region verbatim, into an output schema whose PK is the
    /// input's.
    Inherit,
    /// Pack the reindex columns' OPK bytes contiguously into the output PK — the
    /// `_join_pk` of an equijoin / GROUP BY repartition. The output stride
    /// legitimately differs from the input's (U64 input → U128 synthetic PK).
    Pack(ReindexPacker),
    /// Hash the full output row (every payload column) into each PK.
    HashRow,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output` over `w`, moving any
/// German-string cell per `blob` (a map never widens a string column, so both
/// strides are 16). The source is read at its own type's width and
/// sign/zero-extended into a wider (promoted) destination slot.
fn copy_column(
    in_batch: &Batch,
    output: &mut Batch,
    &ColCopy {
        src: src_loc,
        slot: dst_payload,
        width: stride,
    }: &ColCopy,
    blob: &mut BlobMode<'_>,
    w: RowWindow,
) {
    let RowWindow { src: src_start, dst: dst_base, n } = w;
    if n == 0 {
        // Every arm below is a no-op over an empty window, and the PK arm's
        // source span is expressed off row `n - 1`.
        return;
    }

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
                let signed = type_code.is_signed_int();
                let rows = pk[src_start * pk_stride..(src_start + n) * pk_stride].chunks_exact(pk_stride);
                // A constant width, so `decode_pk_cell`'s own width match folds away.
                macro_rules! decode_rows {
                    ($w:expr) => {{
                        const W: usize = $w;
                        assert!(pk_off + W <= pk_stride, "a PK column lies inside the PK");
                        let out = &mut dst[dst_base * W..(dst_base + n) * W];
                        for (d, row) in out.as_chunks_mut::<W>().0.iter_mut().zip(rows) {
                            let s: &[u8; W] = row[pk_off..pk_off + W].try_into().unwrap();
                            gnitz_wire::decode_pk_cell(s, signed, d);
                        }
                    }};
                }
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
            if let (true, BlobMode::Relocate(cache)) = (type_code.is_german_string(), blob) {
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
    ev: MapEval,
    /// Where the output PK region comes from.
    pk_source: PkSource,
    /// Some copy carries a German-string column, so a cell can be relocated.
    copies_a_string: bool,
    /// The input has German-string columns and every one is carried by a copy,
    /// so its heap holds no bytes the output would adopt dead.
    keeps_every_string: bool,
    /// A row with any of these null bits set is dropped: a
    /// [`gnitz_wire::NullKeys::Drop`] reindex's nullable key columns.
    null_key_mask: u64,
    out_schema: SchemaDescriptor,
}

/// The `[start, end)` runs of `batch`'s rows with no bit of `mask` set.
fn key_defined_runs(batch: &Batch, mask: u64) -> Vec<(usize, usize)> {
    let words = batch.null_bmp_data().as_chunks::<8>().0;
    // A batch with no NULL key takes only this branch-free pass.
    if words.iter().fold(0u64, |a, w| a | u64::from_le_bytes(*w)) & mask == 0 {
        return vec![(0, batch.count)];
    }
    let defined: Vec<u64> = words
        .chunks(64)
        .map(|block| {
            block.iter().enumerate().fold(0, |bits, (i, w)| {
                bits | u64::from(u64::from_le_bytes(*w) & mask == 0) << i
            })
        })
        .collect();
    let mut runs = Vec::new();
    gnitz_expr::scan_filter_bits(&defined, batch.count, &mut runs);
    runs
}

/// Set every row's PK to the [`FoldCols`] digest of its payload columns, so equal
/// rows share a PK. Two distinct rows whose 128-bit digests collide become one
/// element; accepted, not checked.
fn reindex_hash_row(output: &mut Batch) {
    let n = output.count;
    // The PK *is* the digest, so the OPK region is its big-endian bytes.
    const KEY_BYTES: usize = std::mem::size_of::<u128>();
    assert_eq!(
        output.pk_stride() as usize,
        KEY_BYTES,
        "a hash-row PK is one U128 column"
    );
    // Hashing borrows `output` and the write-back mutates it, so keys are staged
    // per chunk.
    const CHUNK: usize = 256;
    let mut keys = [0u128; CHUNK];
    let fold = FoldCols::new(output.schema().payload_locators());
    let mut start = 0;
    while start < n {
        let end = (start + CHUNK).min(n);
        {
            let mb = output.as_mem_batch();
            for row in start..end {
                keys[row - start] = fold.key_row(&mb, row, mb.get_null_word(row));
            }
        }
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
    out_cols: &[(TypeCode, bool)],
) -> Result<SchemaDescriptor, OpBuildErr> {
    let over = |e| OpBuildErr::shape(format!("compute map: output {e}"));
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema).map_err(over)?;
    for &(tc, nullable) in out_cols {
        b.push(SchemaColumn::new(tc, nullable)).map_err(over)?;
    }
    Ok(b.finish())
}

/// Output schema of a HashRow Map: a U128 PK, then each projected column at its
/// target type (the source's when absent) and its source nullability. Typed at
/// the target, the promotion is what `from_map`'s `check_copy_types` screens.
fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    cols: &[gnitz_wire::ReindexSlot],
) -> Result<SchemaDescriptor, OpBuildErr> {
    let over = |e| OpBuildErr::shape(format!("hash-row map: output {e}"));
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(crate::schema::TypeCode::U128, false))
        .map_err(over)?;
    for &(c, tgt) in cols {
        // A key column, not merely an in-range one — the screen the reindex and
        // top-N key kinds clear at this same boundary.
        locate_key_col(in_schema, c, "hash-row map")?;
        let src = in_schema
            .column(c as usize)
            .ok_or_else(|| OpBuildErr::oob_col("hash-row map: column", c, in_schema))?;
        b.push(SchemaColumn::new(tgt.unwrap_or(src.type_code), src.nullable))
            .map_err(over)?;
    }
    Ok(b.finish())
}

impl MapPlan {
    /// The plan for a circuit's MAP node: one derivation of its `(output schema,
    /// map program, PK source)` triple, and the trust boundary each kind's
    /// client-supplied column list clears. Every kind ends in the same
    /// [`Self::from_map`], so an elided map is validated like any other.
    pub fn from_wire(in_schema: &SchemaDescriptor, mk: &gnitz_wire::MapKind) -> Result<Self, OpBuildErr> {
        let mut null_key_mask = 0;
        let (out_schema, prog, pk_source) = match mk {
            gnitz_wire::MapKind::Compute(map) => return Self::from_compute_map(in_schema, map),

            gnitz_wire::MapKind::Reindex { keep, key, nulls, .. } => {
                // The packer is built first because it *is* the layout: its output
                // schema reads the promoters the per-row pack writes through, so
                // the reindexed `_join_pk` and the delta scatter co-partition by
                // construction. Same packer the exchange scatter builds from the
                // circuit's own slots.
                let packer = ReindexPacker::new(in_schema, key)?;
                let out_schema = packer.output_schema(in_schema, keep)?;
                if *nulls == gnitz_wire::NullKeys::Drop {
                    null_key_mask = key
                        .iter()
                        .filter_map(|&(c, _)| in_schema.payload_slot(c as usize))
                        .fold(0, |mask, slot| mask | 1u64 << slot)
                        & in_schema.nullable_payload_slots();
                }
                (out_schema, LogicalProgram::copy_cols(keep), PkSource::Pack(packer))
            }

            gnitz_wire::MapKind::HashRow { cols } => {
                let out_schema = hashrow_output_schema(in_schema, cols)?;
                let proj: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
                (out_schema, LogicalProgram::copy_cols(&proj), PkSource::HashRow)
            }

            gnitz_wire::MapKind::Projection(cols) => {
                // A *payload* column, not merely an in-range one: `project_schema`
                // skips a PK index while `copy_cols` still numbers a sink for it.
                for &c in cols {
                    if in_schema.payload_slot(c as usize).is_none() {
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
        let plan = Self::from_map(prog, in_schema, &out_schema, pk_source)
            .map_err(|e| OpBuildErr::Program("map: program/schema mismatch", e))?;
        Ok(MapPlan { null_key_mask, ..plan })
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
        let prog =
            LogicalProgram::from_blob(&map.program).map_err(|e| OpBuildErr::Program("map: invalid program", e))?;
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
        let ev = logical.resolve_map(in_schema, out_schema)?;
        let copies_a_string = ev.copies().iter().any(|c| c.src.type_code().is_german_string());
        // A copy's source locator is exactly what `locate` gives for its column.
        let is_copied = |ci| {
            let loc = in_schema.locate(ci);
            ev.copies().iter().any(|c| c.src == loc)
        };
        let keeps_every_string = in_schema.has_german_string()
            && (0..in_schema.num_columns())
                .filter(|&ci| in_schema.columns[ci].type_code.is_german_string())
                .all(is_copied);

        Ok(MapPlan {
            ev,
            pk_source,
            copies_a_string,
            keeps_every_string,
            null_key_mask: 0,
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
        matches!(self.pk_source, PkSource::Inherit) && self.ev.is_identity()
    }

    /// Map every `[start, end)` range of `src`, in order, onto `keeper`'s tail;
    /// `keeper` is in this plan's output schema.
    pub(crate) fn append_map_ranges(&mut self, src: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
        debug_assert!(
            matches!(self.pk_source, PkSource::Inherit),
            "append_map_ranges: any other source leaves the keeper's PK region unwritten",
        );
        self.map_ranges_into(src, keeper, ranges);
    }

    /// The map over `in_batch`, less a [`gnitz_wire::NullKeys::Drop`] reindex's
    /// NULL-keyed rows, into a fresh `Raw` output.
    pub fn evaluate_map_batch(&mut self, in_batch: &Batch) -> Batch {
        let whole = [(0, in_batch.count)];
        let runs;
        let ranges: &[(usize, usize)] = match self.null_key_mask {
            0 => &whole,
            mask => {
                runs = key_defined_runs(in_batch, mask);
                &runs
            }
        };
        let n = crate::storage::range_rows(ranges);
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
        self.map_ranges_into(in_batch, &mut output, ranges);
        // The one source that keys on the finished output row.
        if let PkSource::HashRow = self.pk_source {
            reindex_hash_row(&mut output);
        }
        output
    }

    /// Map `ranges` of `src` onto `out`'s tail.
    fn map_ranges_into(&mut self, src: &Batch, out: &mut Batch, ranges: &[(usize, usize)]) {
        let total = crate::storage::range_rows(ranges);
        if total == 0 {
            return;
        }
        let blob_cap = crate::storage::prorated_blob_cap(src.blob.len(), src.count, total);
        let compact_below = match (self.ev.emits_anything(), &self.pk_source) {
            (true, _) => COMPACT_RUN_LEN,
            (false, PkSource::Pack(_)) => PACK_COMPACT_RUN_LEN,
            (false, _) => 0,
        };
        let starves_kernel = ranges.len() > 1 && total < ranges.len() * compact_below;
        let compacted;
        let (src, ranges) = if starves_kernel {
            compacted = Batch::from_ranges(src, ranges, src.schema(), 0);
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
            true => crate::storage::BlobCacheGuard::acquire(out.schema(), total),
            false => crate::storage::BlobCacheGuard::empty(),
        };
        // For relocated copies and string emits alike.
        if out.schema().has_german_string() && !shares_blob && blob_cap != 0 {
            out.reserve_blob(blob_cap);
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

    /// Map one row window: PK, weight, column moves, then the null words and
    /// computed columns. `out.count` must already cover the destination window —
    /// every `*_mut` accessor is `count`-bounded.
    fn map_rows_into(&mut self, in_batch: &Batch, output: &mut Batch, w: RowWindow, mut blob: BlobMode<'_>) {
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
                let stride = packer.out_stride;
                let pk = &mut output.pk_data_mut()[dst_base * stride..];
                packer.pack_rows(pk, stride, &in_batch.as_mem_batch(), src_start, n);
                // An in-place PK rewrite can break (PK, payload) order.
                output.downgrade();
            }
            // Hashes the finished output row, so `evaluate_map_batch` stamps it
            // once the payload below is written.
            PkSource::HashRow => {}
        }
        output.weight_data_mut()[dst_base * 8..(dst_base + n) * 8]
            .copy_from_slice(&in_batch.weight_data()[src_start * 8..(src_start + n) * 8]);

        for c in self.ev.copies() {
            copy_column(in_batch, output, c, &mut blob, w);
        }
        self.ev
            .write_computed(&in_batch.as_mem_batch(), src_start, n, output, dst_base);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/expr.rs"]
mod tests;
