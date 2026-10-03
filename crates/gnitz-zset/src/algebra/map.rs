//! [`MapPlan`] — the columnar driver behind every DBSP map and projection.
//!
//! A filter needs nothing on top of the resolved program and is a bare
//! `gnitz_expr::RowFilter` wherever one is held; a map is this plan, which adds
//! the PK, weight and column moves around the computed columns
//! `gnitz_expr::MapEval` writes.

use gnitz_expr::{ColCopy, ExprValidateErr, LogicalProgram, MapEval};

use super::reindex::{locate_key_col, FoldCols, ReindexPacker};
use crate::repr::{copy_string_cells, Batch, BlobCache};
use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode};
use gnitz_wire::{zip_cells, FixedInt};

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
enum PkSource {
    /// Copy the input PK region verbatim, into an output schema whose PK is the
    /// input's.
    Inherit,
    /// Pack the reindex columns' OPK bytes contiguously into the output PK — the
    /// `_join_pk` of an equijoin / GROUP BY repartition. The output stride
    /// legitimately differs from the input's (U64 input → U128 synthetic PK).
    Pack(ReindexPacker),
    /// Hash each output row's payload columns into its PK.
    HashRow(FoldCols),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Copy a single column from `in_batch` to `output` over `w`, a German-string
/// cell rebased per [`copy_string_cells`]. The source is read at its own
/// type's width and sign/zero-extended into a wider (promoted) destination slot.
fn copy_column(
    in_batch: &Batch,
    output: &mut Batch,
    &ColCopy {
        src: src_loc,
        slot: dst_payload,
        width: stride,
    }: &ColCopy,
    heap_at: Option<usize>,
    cache: &mut BlobCache,
    w: RowWindow,
) {
    let RowWindow { src: src_start, dst: dst_base, n } = w;
    // Destructured once, so each arm below is one column kernel over the window.
    match src_loc {
        ColumnLocator::Pk { byte_off, size, type_code } => {
            // The PK region holds OPK bytes, decoded back to native LE at the
            // source column's own width.
            let pk_stride = in_batch.schema().pk_stride();
            let pk = &in_batch.pk_data()[src_start * pk_stride..(src_start + n) * pk_stride];
            let dst = &mut output.col_data_mut(dst_payload)[dst_base * stride..(dst_base + n) * stride];
            let (off, src_stride) = (byte_off as usize, size as usize);
            if src_stride == stride {
                gnitz_wire::decode_pk_cells(pk, pk_stride, off, stride, type_code.is_signed_int(), dst);
            } else {
                widen_column(pk, pk_stride, off, type_code, true, stride, dst);
            }
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            let in_pi = slot as usize;
            let src_stride = size as usize; // source read width
            if type_code.is_german_string() {
                // STRING and BLOB share the 16-byte German-string struct, whose
                // heap-offset field points into the source batch's blob.
                debug_assert_eq!(
                    (src_stride, stride),
                    (16, 16),
                    "German-string column moved at a non-16-byte stride",
                );
                let src = &in_batch.col_data(in_pi)[src_start * 16..(src_start + n) * 16];
                // One split borrow, so the destination region is resolved once.
                let (dst_col, _, dst_blob) = output.col_null_and_blob_mut(dst_payload);
                let dst = &mut dst_col[dst_base * 16..(dst_base + n) * 16];
                copy_string_cells(dst, src, in_batch.blob(), dst_blob, heap_at, cache);
                return;
            }
            debug_assert!(
                (src_start + n) * src_stride <= in_batch.col_data(in_pi).len(),
                "copy_column: source column {in_pi} is shorter than rows [{src_start}, {}) at stride {src_stride}",
                src_start + n
            );
            let src = &in_batch.col_data(in_pi)[src_start * src_stride..(src_start + n) * src_stride];
            let dst = &mut output.col_data_mut(dst_payload)[dst_base * stride..(dst_base + n) * stride];
            if src_stride == stride {
                dst.copy_from_slice(src);
            } else {
                widen_column(src, src_stride, 0, type_code, false, stride, dst);
            }
        }
    }
}

/// [`widen_cells`] at the slot width `dw`: 2, 4 or 8, a widened slot being a fixed int.
fn widen_column(src: &[u8], src_stride: usize, off: usize, tc: TypeCode, pk: bool, dw: usize, dst: &mut [u8]) {
    let fi = FixedInt::from_type_code(tc).expect("a widened column is a fixed int");
    match dw {
        2 => widen_cells::<2>(src, src_stride, off, fi, pk, dst),
        4 => widen_cells::<4>(src, src_stride, off, fi, pk, dst),
        8 => widen_cells::<8>(src, src_stride, off, fi, pk, dst),
        other => unreachable!("a widened slot is 2/4/8 bytes, got {other}"),
    }
}

/// Widen each `fi` cell at byte `off` of `src`'s `src_stride`-byte rows — a PK column's
/// OPK cell when `pk`, else a native one — into `dst`'s `DW`-byte native cells.
fn widen_cells<const DW: usize>(src: &[u8], src_stride: usize, off: usize, fi: FixedInt, pk: bool, dst: &mut [u8]) {
    let out = dst.as_chunks_mut::<DW>().0.iter_mut();
    gnitz_wire::for_each_fixed_int!(fi, |FI| {
        const W: usize = FI.width();
        let store = |v: i64, d: &mut [u8; DW]| *d = v.to_le_bytes()[..DW].try_into().unwrap();
        match pk {
            true => zip_cells::<W, _>(src, src_stride, off, out, |c, d| {
                store(gnitz_wire::decode_opk_i64(c, FI), d)
            }),
            false => zip_cells::<W, _>(src, src_stride, off, out, |c, d| store(FI.decode_le_i64(c), d)),
        }
    });
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
    /// The input's German-string payload slots some copy reads.
    copied_string_slots: u64,
    /// A row with any of these null bits set is dropped: a
    /// [`gnitz_wire::NullKeys::Drop`] reindex's nullable key columns.
    null_key_mask: u64,
    out_schema: SchemaDescriptor,
}

/// Set every row's PK to the [`FoldCols`] digest of its payload columns, so equal
/// rows share a PK. Two distinct rows whose 128-bit digests collide become one
/// element; accepted, not checked.
fn reindex_hash_row(output: &mut Batch, fold: &FoldCols) {
    let n = output.count;
    // The PK *is* the digest, so the OPK region is its big-endian bytes.
    const KEY_BYTES: usize = std::mem::size_of::<u128>();
    assert_eq!(
        output.schema().pk_stride(),
        KEY_BYTES,
        "a hash-row PK is one U128 column"
    );
    // Hashing borrows `output` and the write-back mutates it, so keys are staged
    // per chunk.
    const CHUNK: usize = 256;
    let mut keys = [0u128; CHUNK];
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
}

/// Output schema of a computed-projection `Map`: the input's PK region (the map
/// inherits it verbatim, [`PkSource::Inherit`]), then one payload column per
/// declared `(type_code, nullable)` slot. The declared slots ARE the layout;
/// `MapPlan::from_map` is what catches a program disagreeing with them.
fn compute_map_output_schema(
    in_schema: &SchemaDescriptor,
    out_cols: &[(TypeCode, bool)],
) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema);
    for &(tc, nullable) in out_cols {
        b.push(SchemaColumn::new(tc, nullable));
    }
    b.finish().map_err(|e| format!("compute map: output {e}"))
}

/// Output schema of a HashRow Map: a U128 PK, then each projected column at its
/// slot type and its source nullability. Typed at the slot, the promotion is
/// what the copy kernel screens.
fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    cols: &[gnitz_wire::ReindexSlot],
) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(crate::schema::TypeCode::U128, false));
    for &(c, t) in cols {
        // A key column, not merely an in-range one — the screen the reindex and
        // top-N key kinds clear at this same boundary.
        locate_key_col(in_schema, c, "hash-row map")?;
        let src = in_schema.columns[c as usize];
        b.push(SchemaColumn::new(t, src.nullable));
    }
    b.finish().map_err(|e| format!("hash-row map: output {e}"))
}

impl MapPlan {
    /// The plan for a circuit's MAP node: one derivation of its `(output schema,
    /// map program, PK source)` triple, and the trust boundary each kind's
    /// client-supplied column list clears. Every kind ends in the same
    /// [`Self::from_map`], so an elided map is validated like any other.
    pub fn from_wire(in_schema: &SchemaDescriptor, mk: &gnitz_wire::MapKind) -> Result<Self, String> {
        let mut null_key_mask = 0;
        let (out_schema, prog, pk_source) = match mk {
            gnitz_wire::MapKind::Compute(map) => return Self::from_compute_map(in_schema, map),

            gnitz_wire::MapKind::Reindex { keep, key, nulls, .. } => {
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
                let fold = FoldCols::new(out_schema.payload_locators());
                (out_schema, LogicalProgram::copy_cols(&proj), PkSource::HashRow(fold))
            }

            gnitz_wire::MapKind::Projection(cols) => {
                let out_schema =
                    crate::schema::project_schema(in_schema, cols).map_err(|e| format!("projection map: {e}"))?;
                (out_schema, LogicalProgram::copy_cols(cols), PkSource::Inherit)
            }
        };
        let plan = Self::from_map(prog, in_schema, &out_schema, pk_source)
            .map_err(|e| format!("map: program/schema mismatch: {e}"))?;
        Ok(MapPlan { null_key_mask, ..plan })
    }

    /// A computed projection: [`gnitz_wire::MapKind::Compute`]'s whole body, and
    /// also a read spec's pre-map. The output schema is derived, never shipped —
    /// the map inherits the input's PK region verbatim ([`PkSource::Inherit`]),
    /// so no caller can describe a PK region the map does not produce.
    pub(crate) fn from_compute_map(in_schema: &SchemaDescriptor, map: &gnitz_wire::ComputeMap) -> Result<Self, String> {
        let out_schema = compute_map_output_schema(in_schema, &map.out_cols)?;
        // The only map whose program is client bytes; every other kind builds
        // one from a column list. Rejected, not skipped: skipping a corrupt blob
        // would leave the output at the default empty schema.
        let prog = LogicalProgram::from_blob(&map.program).map_err(|e| format!("map: invalid program: {e}"))?;
        Self::from_map(prog, in_schema, &out_schema, PkSource::Inherit)
            .map_err(|e| format!("map: program/schema mismatch: {e}"))
    }

    /// Map plan from a logical expression program. A pure projection is the
    /// special case where the program computes nothing and every sink is a
    /// column copy (see [`LogicalProgram::copy_cols`]): the plan reduces to the
    /// copy list and the resolved program's null permutation.
    fn from_map(
        logical: LogicalProgram,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
        pk_source: PkSource,
    ) -> Result<Self, ExprValidateErr> {
        let ev = logical.resolve_map(in_schema, out_schema)?;
        let copied_string_slots = ev.copies().iter().fold(0u64, |slots, c| match c.src {
            ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => slots | 1u64 << slot,
            _ => slots,
        });

        Ok(MapPlan {
            ev,
            pk_source,
            copied_string_slots,
            null_key_mask: 0,
            out_schema: *out_schema,
        })
    }

    /// The schema this plan stamps on its output.
    pub fn out_schema(&self) -> &SchemaDescriptor {
        &self.out_schema
    }

    /// The input column behind each output payload slot, when this map only
    /// re-keys its rows onto leading bytes of their own PK: the input, read in
    /// its own order, then holds each output key's rows together. `None` for
    /// every other map.
    pub fn rekeys_onto_pk_prefix(&self) -> Option<Vec<ColumnLocator>> {
        let PkSource::Pack(packer) = &self.pk_source else {
            return None;
        };
        packer.pk_range().filter(|&(at, _)| at == 0)?;
        // A PK column is never NULL, and a reindex copies each kept column at
        // its own type.
        debug_assert!(self.null_key_mask == 0 && !self.ev.emits_anything());
        debug_assert!(self.ev.copies().iter().all(|c| c.width == c.src.size()));
        Some(self.ev.copies().iter().map(|c| c.src).collect())
    }

    /// Whether some row can be dropped: a [`gnitz_wire::NullKeys::Drop`] re-key over
    /// a nullable key column.
    pub fn drops_null_keys(&self) -> bool {
        self.null_key_mask != 0
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
    /// NULL-keyed rows, into a fresh unconsolidated output.
    pub fn evaluate_map_batch(&mut self, in_batch: &Batch) -> Batch {
        let whole = [(0, in_batch.count)];
        let runs;
        let ranges: &[(usize, usize)] = match self.null_key_mask {
            0 => &whole,
            mask => {
                runs = in_batch.runs_without_nulls(mask);
                &runs
            }
        };
        let n = crate::repr::range_rows(ranges);
        if n == 0 {
            return Batch::empty_with_schema(&self.out_schema);
        }
        // Uninitialized: `validate` makes every map write every payload slot,
        // and the calls below cover the PK, weight and null regions.
        let mut output = Batch::with_capacity(&self.out_schema, n);
        self.map_ranges_into(in_batch, &mut output, ranges);
        // The one source that keys on the finished output row.
        if let PkSource::HashRow(fold) = &self.pk_source {
            reindex_hash_row(&mut output, fold);
        }
        output
    }

    /// Map `ranges` of `src` onto `out`'s tail, window by window. Ranges too
    /// short to feed the per-window kernel are gathered into one first.
    fn map_ranges_into(&mut self, src: &Batch, out: &mut Batch, ranges: &[(usize, usize)]) {
        let total = crate::repr::range_rows(ranges);
        if total == 0 {
            return;
        }
        let compact_below = match (self.ev.emits_anything(), &self.pk_source) {
            (true, _) => COMPACT_RUN_LEN,
            (false, PkSource::Pack(_)) => PACK_COMPACT_RUN_LEN,
            (false, _) => 0,
        };
        let starves_kernel = ranges.len() > 1 && total < ranges.len() * compact_below;
        let compacted = starves_kernel.then(|| Batch::from_ranges(src, ranges, 0));
        let whole = [(0, total)];
        let (src, ranges) = match &compacted {
            Some(c) => (c, &whole[..]),
            None => (src, ranges),
        };
        let heap_at = out.carry_heap(&src.as_mem_batch(), self.copied_string_slots, ranges);

        let blob_cap = crate::repr::prorated_blob_cap(src.blob().len(), src.count, total);
        let old = out.grow_rows(total);

        let mut cache = BlobCache::new(out.string_cells(total));
        // For relocated copies and string emits alike.
        if out.schema().has_german_string() && heap_at.is_none() && blob_cap != 0 {
            out.reserve_blob(blob_cap);
        }
        let mut dst = old;
        for &(start, end) in ranges {
            let w = RowWindow { src: start, dst, n: end - start };
            self.map_rows_into(src, out, w, heap_at, &mut cache);
            dst += w.n;
        }
    }

    /// Map one row window: PK, weight, column moves, then the null words and
    /// computed columns. `out.count` must already cover the destination window —
    /// every `*_mut` accessor is `count`-bounded.
    ///
    /// `#[inline(always)]`: it runs once per window, and a window can be one row.
    #[inline(always)]
    fn map_rows_into(
        &mut self,
        in_batch: &Batch,
        output: &mut Batch,
        w: RowWindow,
        heap_at: Option<usize>,
        cache: &mut BlobCache,
    ) {
        let RowWindow { src: src_start, dst: dst_base, n } = w;
        // Both PK sources that read the *input* row, so both belong to the
        // window rather than to a pass over the finished batch.
        match &self.pk_source {
            PkSource::Inherit => {
                let pk_st = in_batch.schema().pk_stride();
                debug_assert_eq!(
                    pk_st,
                    output.schema().pk_stride(),
                    "PkSource::Inherit: PK stride mismatch"
                );
                output.pk_data_mut()[dst_base * pk_st..(dst_base + n) * pk_st]
                    .copy_from_slice(&in_batch.pk_data()[src_start * pk_st..(src_start + n) * pk_st]);
            }
            PkSource::Pack(packer) => {
                debug_assert_eq!(output.schema().pk_stride(), packer.out_stride);
                let stride = packer.out_stride;
                let pk = &mut output.pk_data_mut()[dst_base * stride..];
                packer.pack_rows(pk, stride, &in_batch.as_mem_batch(), &[(src_start, src_start + n)]);
            }
            // Hashes the finished output row, so `evaluate_map_batch` stamps it
            // once the payload below is written.
            PkSource::HashRow(_) => {}
        }
        output.weight_data_mut()[dst_base * 8..(dst_base + n) * 8]
            .copy_from_slice(&in_batch.weight_data()[src_start * 8..(src_start + n) * 8]);

        for c in self.ev.copies() {
            copy_column(in_batch, output, c, heap_at, cache, w);
        }
        self.ev
            .write_computed(&in_batch.as_mem_batch(), src_start, n, output, dst_base);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/map.rs"]
mod tests;
