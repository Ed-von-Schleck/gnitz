//! Columns copied verbatim from one batch into another: [`ColMoves`], the copies
//! with the null bits and German-string slots that follow from them, and the
//! kernels that write a copy's null words, cells and string heap offsets — over
//! a row window, or gathered from listed rows.

use gnitz_expr::{ColCopy, ColumnLocator};

use super::int_cell;
use crate::repr::{Batch, DirectWriter, MemBatch, relocated_cell};
use crate::schema::SchemaDescriptor;
use gnitz_wire::{FixedInt, Heap, RowSource, TypeCode, zip_cells};

/// One copy step's row window: source rows `[src, src + n)` onto destination rows
/// `[dst, dst + n)`.
#[derive(Clone, Copy)]
pub(super) struct RowWindow {
    pub(super) src: usize,
    pub(super) dst: usize,
    pub(super) n: usize,
}

/// Columns copied verbatim from an input row into an output row, with the null
/// bits and German-string slots that follow from them.
pub struct ColMoves {
    copies: Box<[ColCopy]>,
    /// The nearest null move — for most copy lists the only one, held inline so
    /// a per-row [`Self::null_word`] reads no heap. Its mask is zero when no
    /// nullable column is copied.
    first: NullMove,
    rest: Vec<NullMove>,
    /// The input's German-string payload slots some copy reads.
    string_slots: u64,
}

/// The source bits that all travel one distance: `up` slots toward the high end
/// or `down` toward the low end, one of the two being zero.
#[derive(Clone, Copy)]
struct NullMove {
    mask: u64,
    up: u8,
    down: u8,
}

impl NullMove {
    #[inline(always)]
    fn apply(self, word: u64) -> u64 {
        ((word & self.mask) << self.up) >> self.down
    }
}

impl ColMoves {
    /// The moves of `copies`, each reading a column of `src`.
    pub fn new(copies: Vec<ColCopy>, src: &SchemaDescriptor) -> Self {
        let nullable = src.nullable_payload_slots();
        let mut moves: Vec<NullMove> = Vec::new();
        let mut string_slots = 0u64;
        for c in &copies {
            let ColumnLocator::Payload { slot: src, type_code, .. } = c.src else {
                continue;
            };
            if type_code.is_german_string() {
                string_slots |= 1u64 << src;
            }
            if !gnitz_wire::null_word_get(nullable, src as usize) {
                continue;
            }
            let dst = c.slot as u8;
            let (up, down) = (dst.saturating_sub(src), src.saturating_sub(dst));
            match moves.iter_mut().find(|m| (m.up, m.down) == (up, down)) {
                Some(m) => m.mask |= 1u64 << src,
                None => moves.push(NullMove { mask: 1u64 << src, up, down }),
            }
        }
        // Nearest first: bits that stay put take the first pass, which then
        // needs no shift.
        moves.sort_by_key(|m| m.up.max(m.down));
        let mut moves = moves.into_iter();
        let first = moves.next().unwrap_or(NullMove { mask: 0, up: 0, down: 0 });
        ColMoves {
            copies: copies.into_boxed_slice(),
            first,
            rest: moves.collect(),
            string_slots,
        }
    }

    /// The copies, in the order they were given.
    #[inline]
    pub fn copies(&self) -> &[ColCopy] {
        &self.copies
    }

    /// The input's German-string payload slots some copy reads.
    #[inline]
    pub fn string_slots(&self) -> u64 {
        self.string_slots
    }

    /// One input row's null word as its output row's.
    #[inline(always)]
    pub fn null_word(&self, word: u64) -> u64 {
        self.rest
            .iter()
            .fold(self.first.apply(word), |bits, m| bits | m.apply(word))
    }

    /// Derive the null words of `out` rows `[dst_base, dst_base + n)` from
    /// source rows `[src_start, src_start + n)`, one u64 per row. The whole
    /// window is written: the destination may be an uninitialized tail.
    pub(super) fn write_null_rows(
        &self,
        in_null_bmp: &[u8],
        src_start: usize,
        out: &mut [u8],
        dst_base: usize,
        n: usize,
    ) {
        let dst = &mut out[dst_base * 8..(dst_base + n) * 8];
        let src = in_null_bmp[src_start * 8..(src_start + n) * 8].as_chunks::<8>().0;
        let word = |w: &[u8; 8]| u64::from_le_bytes(*w);
        let (first, rest) = (self.first, &self.rest[..]);
        if first.mask == 0 {
            return dst.fill(0);
        }
        // A move at a time, each pass one uniform shift; several moves go block by
        // block so the words they revisit stay in L1.
        let block = if rest.is_empty() { n.max(1) } else { 256 };
        for (d, s) in dst.as_chunks_mut::<8>().0.chunks_mut(block).zip(src.chunks(block)) {
            if first.up == 0 && first.down == 0 {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = (word(s) & first.mask).to_le_bytes();
                }
            } else {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = first.apply(word(s)).to_le_bytes();
                }
            }
            for &mv in rest {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = (word(d) | mv.apply(word(s))).to_le_bytes();
                }
            }
        }
    }

    /// The moves of `src`'s rows `rows` onto this writer's rows `0..`, in order:
    /// each row's whole null word, holding the copied columns' bits and no other,
    /// then each copied cell, a German-string cell relocated into the writer's
    /// heap. A copy is at its source column's own width: none widens.
    pub fn gather(&self, src: &MemBatch, rows: impl Iterator<Item = usize> + Clone, out: &mut DirectWriter<'_>) {
        let words = out.null_bmp_mut().as_chunks_mut::<8>().0;
        if self.first.mask == 0 {
            words.fill([0; 8]);
        } else {
            for (word, row) in words.iter_mut().zip(rows.clone()) {
                *word = self.null_word(src.get_null_word(row)).to_le_bytes();
            }
        }
        for c in self.copies() {
            gather_column(src, c, rows.clone(), out);
        }
    }
}

/// Copy `moves` of `src` rows `0..n` onto `out` rows `0..n`: each row's whole
/// null word, holding the copied columns' bits and no other, the copied cells,
/// and each German-string column rebased onto the heap
/// [`crate::repr::Batch::carry_heap`] left at `heap_at`.
pub fn copy_columns(src: &MemBatch, out: &mut DirectWriter<'_>, moves: &ColMoves, n: usize, heap_at: Option<usize>) {
    copy_window(src, out, moves, RowWindow { src: 0, dst: 0, n });
    rebase_copied_strings(moves, out, src.blob(), heap_at);
}

/// Every row of `b` in a fresh `out_schema` batch of `b`'s PK stride: its PK and
/// weight, and each payload column with its null bit at output slot
/// `first_slot + pi`, over `b`'s heap. Every row's null word also holds
/// `null_bits`. `rest` then writes each output column no input column lands in.
pub(super) fn copy_rows_into(
    b: &Batch,
    out_schema: &SchemaDescriptor,
    first_slot: usize,
    null_bits: u64,
    rest: impl FnOnce(&mut DirectWriter<'_>),
) -> Batch {
    let in_schema = b.schema();
    debug_assert_eq!(out_schema.pk_stride(), in_schema.pk_stride());
    debug_assert!(out_schema.num_payload_cols() >= first_slot + in_schema.num_payload_cols());
    let n = b.len();
    let mut out = Batch::with_capacity(out_schema, n);
    let heap_at = out.carry_heap(&b.as_mem_batch(), in_schema.string_payload_slots(), &[(0, n)], 0);
    out.write_rows(n, |w| {
        let (pk, weights, nulls) = w.fixed_mut();
        pk.copy_from_slice(b.pk());
        weights.copy_from_slice(b.weight());
        // Every input column moves `first_slot` slots up, its null bit with it.
        if first_slot == 0 && null_bits == 0 {
            nulls.copy_from_slice(b.null_bmp());
        } else {
            let words = nulls
                .as_chunks_mut::<8>()
                .0
                .iter_mut()
                .zip(b.null_bmp().as_chunks::<8>().0);
            let word = |s: &[u8; 8]| u64::from_le_bytes(*s);
            match first_slot {
                // Unshifted, the loop compiles to fewer instructions per row.
                0 => words.for_each(|(d, s)| *d = (word(s) | null_bits).to_le_bytes()),
                _ => words
                    .for_each(|(d, s)| *d = (gnitz_wire::null_word_at(word(s), first_slot) | null_bits).to_le_bytes()),
            }
        }
        for (pi, col) in in_schema.payload_columns() {
            w.col_mut(first_slot + pi).copy_from_slice(b.col_data(pi));
            if col.type_code.is_german_string() {
                w.rebase_string_col(first_slot + pi, Heap::Plain(b.blob()), heap_at.into());
            }
        }
        rest(w);
    });
    out
}

/// [`ColMoves::gather`]'s cells of one column. Out of line for the reason
/// [`copy_column`] is.
#[inline(never)]
fn gather_column(src: &MemBatch, c: &ColCopy, rows: impl Iterator<Item = usize>, out: &mut DirectWriter<'_>) {
    debug_assert_eq!(c.width, c.src.size(), "a gathered column keeps its width");
    let ColumnLocator::Payload { slot, size, type_code } = c.src else {
        let mut scratch = [0u8; 16];
        for (cell, row) in out.col_mut(c.slot).chunks_exact_mut(c.width).zip(rows) {
            cell.copy_from_slice(c.src.native_le_bytes(src, row, &mut scratch));
        }
        return;
    };
    let col = src.col_data(slot as usize);
    if type_code.is_german_string() {
        let cells = col.as_chunks::<16>().0;
        let (dst, heap, mut cache) = out.string_col_mut(c.slot);
        for (cell, row) in dst.as_chunks_mut::<16>().0.iter_mut().zip(rows) {
            *cell = relocated_cell(&cells[row], src.heap(), heap, cache.as_deref_mut());
        }
    } else {
        gnitz_wire::by_cell_width!(size as usize, |T| {
            const N: usize = size_of::<T>();
            let cells = col.as_chunks::<N>().0;
            for (cell, row) in out.col_mut(c.slot).as_chunks_mut::<N>().0.iter_mut().zip(rows) {
                *cell = cells[row];
            }
        })
    }
}

/// Copy `moves` of `in_batch` onto `output` over `w`: each row's whole null word,
/// holding the copied columns' bits and no other, then each copied cell, a
/// German-string cell verbatim: the caller rebases those columns.
pub(super) fn copy_window(in_batch: &MemBatch, output: &mut DirectWriter<'_>, moves: &ColMoves, w: RowWindow) {
    moves.write_null_rows(in_batch.null_bmp(), w.src, output.null_bmp_mut(), w.dst, w.n);
    for c in moves.copies() {
        copy_column(in_batch, output, c, w);
    }
}

/// Rebase the string columns [`copy_window`] wrote for `moves`, their cells
/// read from `src_blob`, onto the heap [`crate::repr::Batch::carry_heap`] left at
/// `heap_at`.
pub(super) fn rebase_copied_strings(
    moves: &ColMoves,
    output: &mut DirectWriter<'_>,
    src_blob: &[u8],
    heap_at: Option<usize>,
) {
    for c in moves.copies() {
        if matches!(c.src, ColumnLocator::Payload { type_code, .. } if type_code.is_german_string()) {
            output.rebase_string_col(c.slot, gnitz_wire::Heap::Plain(src_blob), heap_at.into());
        }
    }
}

/// [`copy_window`]'s cells of one column. The source is read at its own type's
/// width and sign/zero-extended into a wider (promoted) destination slot. Out of
/// line: inlined into the loop over copies, the PK arm's setup is hoisted into a
/// prologue every window pays, payload-only ones included.
#[inline(never)]
fn copy_column(
    in_batch: &MemBatch,
    output: &mut DirectWriter<'_>,
    &ColCopy {
        src: src_loc,
        slot: dst_payload,
        width: dw,
    }: &ColCopy,
    w: RowWindow,
) {
    let RowWindow { src: src_start, dst: dst_base, n } = w;
    let dst = &mut output.col_mut(dst_payload)[dst_base * dw..(dst_base + n) * dw];
    match src_loc {
        ColumnLocator::Pk { byte_off, size, type_code } => {
            // The PK region holds OPK bytes, decoded back to native LE at the
            // source column's own width.
            let pk_stride = in_batch.schema().pk_stride();
            let pk = &in_batch.pk()[src_start * pk_stride..(src_start + n) * pk_stride];
            let (off, src_stride) = (byte_off as usize, size as usize);
            if src_stride == dw {
                gnitz_wire::decode_pk_cells(pk, pk_stride, off, type_code, dst);
            } else {
                widen_column::<true>(pk, pk_stride, off, type_code, dw, dst);
            }
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            let in_pi = slot as usize;
            let src_stride = size as usize;
            debug_assert!(
                !type_code.is_german_string() || (src_stride, dw) == (16, 16),
                "German-string column moved at a non-16-byte stride",
            );
            debug_assert!(
                (src_start + n) * src_stride <= in_batch.col_data(in_pi).len(),
                "copy_column: source column {in_pi} is shorter than rows [{src_start}, {}) at stride {src_stride}",
                src_start + n
            );
            let src = &in_batch.col_data(in_pi)[src_start * src_stride..(src_start + n) * src_stride];
            if src_stride == dw {
                dst.copy_from_slice(src);
            } else {
                widen_column::<false>(src, src_stride, 0, type_code, dw, dst);
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

#[cfg(test)]
#[path = "tests/copy.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/copy.rs"]
mod bench;
