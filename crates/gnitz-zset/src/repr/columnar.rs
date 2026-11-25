//! A region of a Z-set's columns as one pointer and stride, and the row
//! sources whose regions are read so: the N-way merges and the column-first
//! copies read every source through [`ColumnarSource`].

use std::ops::Range;

use super::batch::MemBatch;
use super::batch::uninit_bytes;
use gnitz_wire::RowSource;

/// A column as `base + row * stride`. Stride 0 reads one shared element for
/// every row. `base` itself need not lie inside the column: a decoded window is
/// rebased so that its first row lands on its first cell.
#[derive(Clone, Copy)]
pub struct ColPtr {
    pub base: *const u8,
    pub stride: usize,
}

/// The bytes a column of zero cells reads, any cell width up to its length.
static ZERO_CELL: [u8; 16] = [0; 16];

impl ColPtr {
    /// A column whose every row reads zero cells of up to 16 bytes.
    #[inline(always)]
    pub fn zeroes() -> Self {
        ColPtr { base: ZERO_CELL.as_ptr(), stride: 0 }
    }

    /// Row `i`'s address.
    #[inline(always)]
    pub(crate) fn row_ptr(self, i: usize) -> *const u8 {
        self.base.wrapping_add(i * self.stride)
    }

    /// Row `i`'s first `len` bytes.
    ///
    /// # Safety
    /// Row `i` is a row of a live column this addresses, and `len` does not
    /// exceed its element width.
    #[inline(always)]
    pub unsafe fn row<'a>(self, i: usize, len: usize) -> &'a [u8] {
        // SAFETY: the caller's contract above.
        unsafe { std::slice::from_raw_parts(self.row_ptr(i), len) }
    }

    /// Copy rows `start..` of this column of `width`-byte elements onto `dst`,
    /// `dst.len() / width` rows.
    ///
    /// # Safety
    /// Those rows are rows of a live column this addresses.
    pub unsafe fn copy_rows(self, start: usize, width: usize, dst: &mut [u8]) {
        if self.stride != 0 {
            // SAFETY: the caller's contract above.
            return dst.copy_from_slice(unsafe { std::slice::from_raw_parts(self.row_ptr(start), dst.len()) });
        }
        if dst.is_empty() {
            return;
        }
        // One element repeated: write it once, then double the written prefix,
        // so any element width costs O(log rows) copies.
        // SAFETY: the caller's contract above; a zero stride is one element.
        dst[..width].copy_from_slice(unsafe { self.row(0, width) });
        let mut filled = width;
        while filled < dst.len() {
            let n = filled.min(dst.len() - filled);
            dst.copy_within(..n, filled);
            filled += n;
        }
    }
}

/// A `RowSource` that also carries the Z-set weight: a storage row the N-way
/// merge folds. Its regions are read as [`ColPtr`]s, in place or decoded for a
/// window of rows.
pub trait ColumnarSource: RowSource {
    /// The row's signed Z-set weight / multiplicity (region 1).
    fn get_weight(&self, row: usize) -> i64;

    /// Whether this source's rows are (PK, coarse weight) pairs with no payload —
    /// a capacity-bounded view's skeleton shard.
    fn is_skeleton(&self) -> bool;

    /// Region `r` as a [`ColPtr`] readable in place at every row a caller reads,
    /// null words exactly as [`RowSource::get_null_word`] reads them; `None`
    /// where it is read through a decode.
    fn in_place(&self, r: usize) -> Option<ColPtr>;

    /// Region `r` as a [`ColPtr`] readable at the rows of `window`, decoded into
    /// `decoded` where it is not held in place; null words exactly as
    /// [`RowSource::get_null_word`] reads them.
    fn region(&self, r: usize, window: &Range<usize>, decoded: &mut DecodedColumns) -> ColPtr;
}

impl<'a> ColumnarSource for MemBatch<'a> {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        MemBatch::get_weight(self, row)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        false
    }

    fn in_place(&self, r: usize) -> Option<ColPtr> {
        Some(ColPtr {
            base: self.data.as_ptr().wrapping_add(self.region_start(r)),
            stride: self.schema.region_stride(r),
        })
    }

    fn region(&self, r: usize, _window: &Range<usize>, _decoded: &mut DecodedColumns) -> ColPtr {
        self.in_place(r).expect("a batch holds every region in place")
    }
}

/// The columns a source decoded to be viewed at a window of its rows, freed
/// with the [`UnifiedSet`](super::UnifiedSet) or the window that views them.
#[derive(Default)]
pub struct DecodedColumns(Vec<Vec<u8>>);

impl DecodedColumns {
    /// A column of `width`-byte cells readable at `window`, which `decode`
    /// writes whole given the window's bytes.
    pub fn column(&mut self, window: &Range<usize>, width: usize, decode: impl FnOnce(&mut [u8])) -> ColPtr {
        // SAFETY: `decode` writes every cell of `column`.
        let mut column = unsafe { uninit_bytes(window.len() * width) };
        decode(&mut column);
        // Rebased so that row `window.start` reads the column's first cell.
        let base = column.as_ptr().wrapping_sub(window.start * width);
        self.0.push(column);
        ColPtr { base, stride: width }
    }
}
