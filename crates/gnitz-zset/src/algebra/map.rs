//! [`MapPlan`] — the columnar driver behind every DBSP map and projection.
//!
//! A filter needs nothing on top of the resolved program and is a bare
//! `gnitz_expr::RowFilter` wherever one is held; a map is this plan, which adds
//! the PK, weight and column moves around the computed columns
//! `gnitz_expr::MapEval` writes.

use gnitz_expr::{ColCopy, LogicalProgram, MapEval};

use super::reindex::{locate_key_col, FoldCols, ReindexPacker};
use crate::repr::{copy_runs, Batch, DirectWriter};
use crate::schema::{project_schema, ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, TypeCode};
use gnitz_wire::{int_cell, zip_cells, FixedInt};

/// One map step's row window: source rows `[src, src + n)` onto destination rows
/// `[dst, dst + n)`: one value rather than three positional `usize`s a
/// [`copy_column`] caller can transpose.
#[derive(Clone, Copy)]
pub(crate) struct RowWindow {
    pub(crate) src: usize,
    pub(crate) dst: usize,
    pub(crate) n: usize,
}

/// Average survivor-run length below which a map that computes, or that copies
/// fixed-width columns alone, copies its survivors into one range first: below
/// it, the copy costs less than the kernel's setup per range.
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
    /// A row with any of the `drop_nulls` null bits set is dropped: a
    /// [`gnitz_wire::NullKeys::Drop`] reindex's nullable key columns.
    Pack { packer: ReindexPacker, drop_nulls: u64 },
    /// Hash each output row's payload columns into its PK.
    HashRow(FoldCols),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The input's German-string payload slots `copies` read.
pub(crate) fn copied_string_slots(copies: &[ColCopy]) -> u64 {
    copies.iter().fold(0, |slots, c| match c.src {
        ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => slots | 1 << slot,
        _ => slots,
    })
}

/// Rebase the string columns [`copy_column`] wrote for `copies`, their cells
/// read from `src_blob`, onto the heap [`Batch::carry_heap`] left at `heap_at`.
pub(crate) fn rebase_copied_strings(
    copies: &[ColCopy],
    output: &mut DirectWriter<'_>,
    src_blob: &[u8],
    heap_at: Option<usize>,
) {
    for c in copies {
        if matches!(c.src, ColumnLocator::Payload { type_code, .. } if type_code.is_german_string()) {
            output.rebase_string_col(c.slot, gnitz_wire::Heap::Plain(src_blob), heap_at);
        }
    }
}

/// Copy a single column from `in_batch` to `output` over `w`, a German-string
/// cell verbatim: the caller rebases the column. The source is read at its own
/// type's width and sign/zero-extended into a wider (promoted) destination slot.
pub(crate) fn copy_column(
    in_batch: &Batch,
    output: &mut DirectWriter<'_>,
    &ColCopy {
        src: src_loc,
        slot: dst_payload,
        width: stride,
    }: &ColCopy,
    w: RowWindow,
) {
    let RowWindow { src: src_start, dst: dst_base, n } = w;
    // Destructured once, so each arm below is one column kernel over the window.
    match src_loc {
        ColumnLocator::Pk { byte_off, size, type_code } => {
            // The PK region holds OPK bytes, decoded back to native LE at the
            // source column's own width.
            let pk_stride = in_batch.schema().pk_stride();
            let pk = &in_batch.pk()[src_start * pk_stride..(src_start + n) * pk_stride];
            let dst = &mut output.col_mut(dst_payload)[dst_base * stride..(dst_base + n) * stride];
            let (off, src_stride) = (byte_off as usize, size as usize);
            if src_stride == stride {
                gnitz_wire::decode_pk_cells(pk, pk_stride, off, stride, type_code.is_signed_int(), dst);
            } else {
                widen_column::<true>(pk, pk_stride, off, type_code, stride, dst);
            }
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            let in_pi = slot as usize;
            let src_stride = size as usize; // source read width
            debug_assert!(
                !type_code.is_german_string() || (src_stride, stride) == (16, 16),
                "German-string column moved at a non-16-byte stride",
            );
            debug_assert!(
                (src_start + n) * src_stride <= in_batch.col_data(in_pi).len(),
                "copy_column: source column {in_pi} is shorter than rows [{src_start}, {}) at stride {src_stride}",
                src_start + n
            );
            let src = &in_batch.col_data(in_pi)[src_start * src_stride..(src_start + n) * src_stride];
            let dst = &mut output.col_mut(dst_payload)[dst_base * stride..(dst_base + n) * stride];
            if src_stride == stride {
                dst.copy_from_slice(src);
            } else {
                widen_column::<false>(src, src_stride, 0, type_code, stride, dst);
            }
        }
    }
}

/// [`widen_cells`] at the slot width `dw`: 2, 4 or 8, a widened slot being a fixed int.
fn widen_column<const PK: bool>(src: &[u8], src_stride: usize, off: usize, tc: TypeCode, dw: usize, dst: &mut [u8]) {
    let fi = FixedInt::from_type_code(tc).expect("a widened column is a fixed int");
    match dw {
        2 => widen_cells::<2, PK>(src, src_stride, off, fi, dst),
        4 => widen_cells::<4, PK>(src, src_stride, off, fi, dst),
        8 => widen_cells::<8, PK>(src, src_stride, off, fi, dst),
        other => unreachable!("a widened slot is 2/4/8 bytes, got {other}"),
    }
}

/// Widen each `fi` cell at byte `off` of `src`'s `src_stride`-byte rows — a PK column's
/// OPK cell under `PK`, else a native one — into `dst`'s `DW`-byte native cells.
/// Out of line: inlined into [`copy_column`], its loop compiles to more
/// instructions per row.
#[inline(never)]
fn widen_cells<const DW: usize, const PK: bool>(
    src: &[u8],
    src_stride: usize,
    off: usize,
    fi: FixedInt,
    dst: &mut [u8],
) {
    let out = dst.as_chunks_mut::<DW>().0.iter_mut();
    gnitz_wire::for_each_fixed_int!(fi, |FI| {
        const W: usize = FI.width();
        zip_cells::<W, _>(src, src_stride, off, out, |c, d| {
            *d = int_cell::<PK>(FI, c).to_le_bytes()[..DW].try_into().unwrap()
        })
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
    out_schema: SchemaDescriptor,
}

/// Set the PK of each of `output`'s rows, whose payload and null words are
/// written, to the [`FoldCols`] digest of its payload columns, so equal rows
/// share a PK. Two distinct rows whose 128-bit digests collide become one
/// element; accepted, not checked.
fn reindex_hash_row(output: &mut DirectWriter<'_>, fold: &FoldCols) {
    let (first, n) = (output.first_row(), output.rows());
    // The PK *is* the digest, so the OPK region is its big-endian bytes.
    const KEY_BYTES: usize = std::mem::size_of::<u128>();
    assert_eq!(output.schema.pk_stride(), KEY_BYTES, "a hash-row PK is one U128 column");
    // Hashing borrows `output` and the write-back mutates it, so keys are staged
    // per chunk.
    const CHUNK: usize = 256;
    let mut keys = [0u128; CHUNK];
    let mut start = 0;
    while start < n {
        let end = (start + CHUNK).min(n);
        {
            let mb = output.written();
            for (key, row) in keys.iter_mut().zip(first + start..first + end) {
                *key = fold.key_row(&mb, row);
            }
        }
        let pk = &mut output.pk_mut()[start * KEY_BYTES..end * KEY_BYTES];
        for (key, dst) in keys.iter().zip(pk.as_chunks_mut::<KEY_BYTES>().0) {
            *dst = key.to_be_bytes();
        }
        start = end;
    }
}

/// Output schema of a HashRow Map: a U128 PK, then each projected column at its
/// slot type and its source nullability. Typed at the slot, the promotion is
/// what the copy kernel screens.
fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    cols: &[gnitz_wire::ReindexSlot],
) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(TypeCode::U128, false));
    for &(c, t) in cols {
        // A key column, not merely an in-range one — the screen the reindex and
        // top-N key kinds clear at this same boundary.
        let (src, _) = locate_key_col(in_schema, c, "hash-row map")?;
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
        let (out_schema, prog, pk_source) = match mk {
            gnitz_wire::MapKind::Compute(map) => return Self::from_compute_map(in_schema, map),

            gnitz_wire::MapKind::Reindex { keep, key, nulls, .. } => {
                let packer = ReindexPacker::new(in_schema, key)?;
                let out_schema = packer.output_schema(in_schema, keep)?;
                let drop_nulls = match nulls {
                    gnitz_wire::NullKeys::Drop => {
                        key.iter()
                            .filter_map(|&(c, _)| in_schema.payload_slot(c as usize))
                            .fold(0, |mask, slot| mask | 1u64 << slot)
                            & in_schema.nullable_payload_slots()
                    }
                    gnitz_wire::NullKeys::Keep => 0,
                };
                let pk_source = PkSource::Pack { packer, drop_nulls };
                (out_schema, LogicalProgram::copy_cols(keep), pk_source)
            }

            gnitz_wire::MapKind::HashRow { cols } => {
                let out_schema = hashrow_output_schema(in_schema, cols)?;
                let proj: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
                let fold = FoldCols::new(out_schema.payload_locators());
                (out_schema, LogicalProgram::copy_cols(&proj), PkSource::HashRow(fold))
            }

            gnitz_wire::MapKind::Projection(cols) => {
                let out_schema = project_schema(in_schema, cols).map_err(|e| format!("projection map: {e}"))?;
                (out_schema, LogicalProgram::copy_cols(cols), PkSource::Inherit)
            }
        };
        Self::from_map(prog, in_schema, &out_schema, pk_source)
    }

    /// A computed projection: [`gnitz_wire::MapKind::Compute`]'s whole body, and
    /// also a read spec's pre-map. The output schema is derived, never shipped —
    /// the map inherits the input's PK region verbatim ([`PkSource::Inherit`]),
    /// so no caller can describe a PK region the map does not produce.
    pub(crate) fn from_compute_map(in_schema: &SchemaDescriptor, map: &gnitz_wire::ComputeMap) -> Result<Self, String> {
        // The input's PK region, then one payload column per declared
        // `(type_code, nullable)` slot. The declared slots are the layout;
        // `from_map` catches a program disagreeing with them.
        let mut b = DerivedSchema::new();
        b.push_pk_of(in_schema);
        for &(tc, nullable) in &map.out_cols {
            b.push(SchemaColumn::new(tc, nullable));
        }
        let out_schema = b.finish().map_err(|e| format!("compute map: output {e}"))?;
        // The only map whose program is client bytes; every other kind builds
        // one from a column list. Rejected, not skipped: skipping a corrupt blob
        // would leave the output at the default empty schema.
        let prog = LogicalProgram::from_blob(&map.program).map_err(|e| format!("map: invalid program: {e}"))?;
        Self::from_map(prog, in_schema, &out_schema, PkSource::Inherit)
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
    ) -> Result<Self, String> {
        let ev = logical
            .resolve_map(in_schema, out_schema)
            .map_err(|e| format!("map: program/schema mismatch: {e}"))?;
        Ok(MapPlan {
            copied_string_slots: copied_string_slots(ev.copies()),
            ev,
            pk_source,
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
        let PkSource::Pack { packer, drop_nulls } = &self.pk_source else {
            return None;
        };
        packer.pk_range().filter(|&(at, _)| at == 0)?;
        // A PK column is never NULL, and a reindex copies each kept column at
        // its own type.
        debug_assert!(*drop_nulls == 0 && !self.ev.emits_anything());
        debug_assert!(self.ev.copies().iter().all(|c| c.width == c.src.size()));
        Some(self.ev.copies().iter().map(|c| c.src).collect())
    }

    /// True iff this map copies its input's PK region verbatim, and so keeps
    /// every row under its own key.
    pub fn inherits_pk(&self) -> bool {
        matches!(self.pk_source, PkSource::Inherit)
    }

    /// True iff running this map would reproduce its input batch. A compiler
    /// elides such a node entirely and lets its consumers read the input.
    pub fn is_identity(&self) -> bool {
        self.inherits_pk() && self.ev.is_identity()
    }

    /// The map over `in_batch`, less a [`gnitz_wire::NullKeys::Drop`] reindex's
    /// NULL-keyed rows, into a fresh unconsolidated output.
    pub fn evaluate_map_batch(&mut self, in_batch: &Batch) -> Batch {
        let whole = [(0, in_batch.count)];
        let runs;
        let ranges: &[(usize, usize)] = match self.pk_source {
            PkSource::Pack { drop_nulls, .. } if drop_nulls != 0 => {
                runs = in_batch.runs_without_nulls(drop_nulls);
                &runs
            }
            _ => &whole,
        };
        let n = crate::repr::range_rows(ranges);
        if n == 0 {
            return Batch::empty_with_schema(&self.out_schema);
        }
        // Uninitialized: `validate` makes every map write every payload slot,
        // and the calls below cover the PK, weight and null regions.
        let mut output = Batch::with_capacity(&self.out_schema, n);
        self.append_map_ranges(in_batch, &mut output, ranges);
        output
    }

    /// Map every `[start, end)` range of `src`, in order, onto the tail of `out`,
    /// which is in this plan's output schema. Ranges too short to feed the
    /// per-window kernel are gathered into one first.
    pub(crate) fn append_map_ranges(&mut self, src: &Batch, out: &mut Batch, ranges: &[(usize, usize)]) {
        let total = crate::repr::range_rows(ranges);
        if total == 0 {
            return;
        }
        let compact_below = match (self.ev.emits_anything(), &self.pk_source) {
            (true, _) => COMPACT_RUN_LEN,
            (false, PkSource::Pack { .. }) => PACK_COMPACT_RUN_LEN,
            // Fixed-width columns gather as cheaply as they map; a string would
            // be relocated twice.
            (false, _) if src.schema().string_payload_slots() == 0 => COMPACT_RUN_LEN,
            (false, _) => 0,
        };
        let starves_kernel = ranges.len() > 1 && total < ranges.len() * compact_below;
        let compacted = starves_kernel.then(|| Batch::from_ranges(src, ranges, 0));
        let whole = [(0, total)];
        let (src, ranges) = match &compacted {
            Some(c) => (c, &whole[..]),
            None => (src, ranges),
        };
        // Declined, the carry reserves for relocated copies and string emits alike.
        let heap_at = out.carry_heap(&src.as_mem_batch(), self.copied_string_slots, ranges);
        out.append_session(total).write(total, |out| {
            let mb = src.as_mem_batch();
            match &self.pk_source {
                PkSource::Inherit => {
                    let stride = src.schema().pk_stride();
                    debug_assert_eq!(stride, out.schema.pk_stride(), "PkSource::Inherit: PK stride mismatch");
                    copy_runs::<0>(src.pk(), out.pk_mut(), ranges.iter().copied(), stride);
                }
                PkSource::Pack { packer, .. } => {
                    debug_assert_eq!(out.schema.pk_stride(), packer.out_stride);
                    packer.pack_rows(out.pk_mut(), packer.out_stride, &mb, ranges);
                }
                // Hashes the finished output row, below.
                PkSource::HashRow(_) => {}
            }
            copy_runs::<8>(src.weight(), out.weight_mut(), ranges.iter().copied(), 8);
            let mut dst = 0;
            for &(start, end) in ranges {
                let w = RowWindow { src: start, dst, n: end - start };
                for c in self.ev.copies() {
                    copy_column(src, out, c, w);
                }
                self.ev.write_computed(&mb, start, w.n, out, dst);
                dst += w.n;
            }
            rebase_copied_strings(self.ev.copies(), out, src.blob(), heap_at);
            if let PkSource::HashRow(fold) = &self.pk_source {
                reindex_hash_row(out, fold);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/map.rs"]
mod bench;
