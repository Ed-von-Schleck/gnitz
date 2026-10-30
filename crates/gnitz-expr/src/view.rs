//! [`BatchView`], the whole-region shape the vectorized kernels read through,
//! and [`MapTarget`], the batch a map writes into.

use gnitz_wire::RowSource;

/// A [`RowSource`] that can additionally hand out whole regions — the shape the
/// vectorized expression kernels need, and the one a flat region-based batch
/// (or an adapter that materializes one) can satisfy.
///
/// Region accessors return the WHOLE column/region, never a morsel slice: the
/// kernels index them absolutely.
///
/// The two shapes must address the same bytes; `debug_assert_coherent`
/// states the equations and checks them in debug builds.
///
/// A batch view's [`heap`](RowSource::heap) is [`Heap::Plain`](gnitz_wire::Heap::Plain):
/// a batch holds its long values verbatim.
pub trait BatchView: RowSource {
    /// Payload column `payload_col` in full (`rows * col_size` bytes, native LE).
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8];
    /// The null-bitmap region in full (`rows * 8` bytes, u64 LE per row).
    fn null_bmp(&self) -> &[u8];
    /// The OPK PK region in full (`rows * stride` bytes) and its per-row stride.
    fn pk_region(&self) -> (&[u8], usize);
}

/// Assert that `row` of `mb` reads the same through both shapes, for the
/// `(payload_col, col_size)` slots in `payload_reads`:
///   `get_col_ptr(row, pi, sz) == &col_data(pi, sz)[row*sz .. row*sz + sz]`
///   `get_null_word(row)       == gnitz_wire::read_u64_le(null_bmp(), row*8)`
///   `get_pk_bytes(row)        == &pk_region().0[row*s .. row*s + s]`, `s = .1`
#[cfg(debug_assertions)]
pub(crate) fn debug_assert_coherent(
    mb: &dyn BatchView,
    row: usize,
    payload_reads: impl IntoIterator<Item = (usize, usize)>,
) {
    assert_eq!(
        mb.get_null_word(row),
        gnitz_wire::read_u64_le(mb.null_bmp(), row * 8),
        "row {row}: null word"
    );
    let (pk, s) = mb.pk_region();
    assert_eq!(mb.get_pk_bytes(row), &pk[row * s..row * s + s], "row {row}: PK bytes");
    for (pi, sz) in payload_reads {
        assert_eq!(
            mb.get_col_ptr(row, pi, sz),
            &mb.col_data(pi, sz)[row * sz..row * sz + sz],
            "row {row}: payload column {pi}"
        );
    }
}

/// The batch a map writes its computed columns into.
pub trait MapTarget {
    /// The row-major null bitmap, 8 bytes per row.
    fn null_bmp_mut(&mut self) -> &mut [u8];
    /// Payload slot `pi`'s cell region, the null bitmap, and the blob heap a string cell spills into.
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>);
}
