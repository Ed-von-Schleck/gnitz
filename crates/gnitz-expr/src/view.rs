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
/// CONTRACT binding the two shapes:
///   `get_col_ptr(row, pi, sz) == &col_data(pi, sz)[row*sz .. row*sz + sz]`
///   `get_null_word(row)       == gnitz_wire::read_u64_le(null_bmp(), row*8)`
///   `get_pk_bytes(row)        == &pk_region().0[row*s .. row*s + s]`, `s = .1`
///
/// [`RowSource`]'s per-row half deliberately has **no default bodies** off these:
/// a default would make direct cell addressing opt-in, so deleting an override
/// would still compile and still pass, silently costing a second multiply and
/// range check per read in the debug build the E2E suite runs.
pub trait BatchView: RowSource {
    /// Payload column `payload_col` in full (`rows * col_size` bytes, native LE).
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8];
    /// The null-bitmap region in full (`rows * 8` bytes, u64 LE per row).
    fn null_bmp(&self) -> &[u8];
    /// The OPK PK region in full (`rows * stride` bytes) and its per-row stride.
    /// Returned as one pair rather than two accessors so a kernel that walks the
    /// region pays one call to obtain both, matching the single `col_data` call
    /// its payload counterpart makes.
    fn pk_region(&self) -> (&[u8], usize);
}

/// The batch a map writes its computed columns into.
pub trait MapTarget {
    /// The row-major null bitmap, 8 bytes per row.
    fn null_bmp_mut(&mut self) -> &mut [u8];
    /// Payload slot `pi`'s cell region, the null bitmap, and the blob heap a string cell spills into.
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>);
}
