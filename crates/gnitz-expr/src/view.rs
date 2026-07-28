//! The batch shapes the evaluator and the resolved-addressing types read
//! through: [`RowSource`] (one row at a time) and [`BatchView`] (whole regions,
//! for the vectorized kernels).

/// Read one row's columns. The **one** per-row access shape in the system: the
/// resolved-addressing types ([`crate::ColumnLocator`]) bind to it, the engine's
/// `ColumnarSource` extends it with the Z-set weight, and [`BatchView`] extends
/// it with the region accessors the vectorized kernels need. A source that can
/// only address cells — a shard mapping whose column is a scalar constant, a
/// cursor's current row — implements this and nothing more.
///
/// Static dispatch only — never take `&dyn RowSource`; it would put a vtable on
/// the per-row locator paths. (Convention: the trait is dyn-compatible, nothing
/// enforces this.)
///
/// All lifetimes are tied to `&self`, NOT decoupled — a client adapter owns the
/// buffers it materializes and can only lend them for `&self`.
pub trait RowSource {
    /// The row's packed OPK PK-region bytes (`pk_stride` wide).
    fn get_pk_bytes(&self, row: usize) -> &[u8];
    /// The row's null-bitmap word (bit N = payload slot N is NULL).
    fn get_null_word(&self, row: usize) -> u64;
    /// `col_size` bytes of payload column `payload_col` (native LE) in `row`.
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8];
    /// The variable-length string/blob heap the German-string cells point into.
    fn blob(&self) -> &[u8];
}

/// A [`RowSource`] that can additionally hand out whole regions — the shape the
/// vectorized expression kernels need, and the one a flat region-based batch
/// (or an adapter that materializes one) can satisfy.
///
/// Region accessors return the WHOLE column/region, never a morsel slice: the
/// kernels index them absolutely. Implementor preconditions:
/// `col_data(pi, sz).len() == rows * sz`, `null_bmp().len() == rows * 8`.
///
/// The per-row accessors [`RowSource`] requires are **not** derived from these,
/// so an implementor that can compute a cell address directly does not pay to
/// slice the whole region first. Deriving `get_col_ptr(row, c, s)` as
/// `&col_data(c, s)[row * s..][..s]` costs a row-count load, a second multiply
/// and a second range check per read — free once LICM hoists them, but the
/// debug build has no LICM and is what the whole E2E suite runs (see the
/// crate-root inlining rule for why that build decides these questions).
///
/// CONTRACT binding the two shapes, checked by [`assert_batchview_consistent`]:
///   `get_col_ptr(row, pi, sz) == &col_data(pi, sz)[row*sz .. row*sz + sz]`
///   `get_null_word(row)       == gnitz_wire::read_u64_le(null_bmp(), row*8)`
///   `get_pk_bytes(row)        == &pk_region().0[row*s .. row*s + s]`, `s = .1`
///
/// There are deliberately **no default bodies** for the per-row half. Defaulting
/// it off the region accessors would make direct addressing opt-in: delete an
/// override and everything still compiles, every test passes, and the slow path
/// is taken in every build.
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

/// Assert the region/per-row contract on [`BatchView`] for `rows` rows and the
/// given `(payload_col, col_size)` pairs. Every implementor must pass.
///
/// A normal `pub fn`, not a `#[cfg(test)]` helper, so every crate that adds an
/// implementor can call it from its own test tree. For a flat-region batch the
/// property is near-tautological (both accessors derive the same region offset);
/// for an adapter that materializes columns out of a foreign representation —
/// mapping payload slots to logical columns, or dispatching on a column-data
/// enum — it is the only thing standing between a mis-mapped slot and silently
/// wrong query results.
pub fn assert_batchview_consistent<B: BatchView>(v: &B, rows: usize, cols: &[(usize, usize)]) {
    let bmp = v.null_bmp();
    assert_eq!(bmp.len(), rows * 8, "null_bmp() must be rows * 8 bytes");
    for row in 0..rows {
        assert_eq!(
            v.get_null_word(row),
            gnitz_wire::read_u64_le(bmp, row * 8),
            "get_null_word({row}) disagrees with null_bmp()",
        );
    }
    let (pk, stride) = v.pk_region();
    assert_eq!(pk.len(), rows * stride, "pk_region() must be rows * stride bytes");
    for row in 0..rows {
        assert_eq!(
            v.get_pk_bytes(row),
            &pk[row * stride..row * stride + stride],
            "get_pk_bytes({row}) disagrees with pk_region()",
        );
    }
    for &(pi, sz) in cols {
        let region = v.col_data(pi, sz);
        assert_eq!(
            region.len(),
            rows * sz,
            "col_data({pi}, {sz}) must be rows * col_size bytes",
        );
        for row in 0..rows {
            assert_eq!(
                v.get_col_ptr(row, pi, sz),
                &region[row * sz..row * sz + sz],
                "get_col_ptr({row}, {pi}, {sz}) disagrees with col_data({pi}, {sz})",
            );
        }
    }
}

#[cfg(test)]
mod tests;
