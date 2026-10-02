//! The batch shapes the evaluator and the resolved-addressing types read
//! through: [`RowSource`] (one row at a time) and [`BatchView`] (whole regions,
//! for the vectorized kernels), plus the by-slot cell readers over the former.

/// Read one row's columns. The **one** per-row access shape in the system: the
/// resolved-addressing types ([`crate::ColumnLocator`]) bind to it, the engine's
/// `ColumnarSource` extends it with the Z-set weight, and [`BatchView`] extends
/// it with the region accessors the vectorized kernels need. A source that can
/// address cells but has no contiguous `rows * col_size` region to hand out — a
/// shard mapping whose column is a scalar constant — implements this and
/// nothing more. Every source is a whole multi-row batch, which is why
/// [`Self::row_count`] sits here and not one level up.
///
/// Static dispatch only wherever a *cell* is read per row: a `&dyn RowSource`
/// there would put an indirect call on every [`crate::ColumnLocator`] read.
/// A whole-batch consumer that resolves the regions once per morsel is not one
/// of those, and the evaluator's kernels take `&dyn BatchView`.
/// (Convention: the trait is dyn-compatible, nothing enforces this.)
///
/// All lifetimes are tied to `&self`, NOT decoupled — a client adapter owns the
/// buffers it materializes and can only lend them for `&self`.
pub trait RowSource {
    /// The row's packed PK-region bytes (`pk_stride` wide), **order-preserving
    /// at-rest (OPK)** at every source, a client `ZSetBatch` included. That is
    /// what the OPK-inverting readers on
    /// [`crate::ColumnLocator`] assume; [`assert_batchview_consistent`] is where
    /// an implementor proves it.
    fn get_pk_bytes(&self, row: usize) -> &[u8];
    /// The row's null-bitmap word (bit N = payload slot N is NULL).
    fn get_null_word(&self, row: usize) -> u64;
    /// `col_size` bytes of payload column `payload_col` (native LE) in `row`.
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8];
    /// The variable-length string/blob heap the German-string cells point into.
    fn blob(&self) -> &[u8];
    /// Rows in this source. The bound every whole-source walk reads — the
    /// evaluator's [`crate::RowFilter::ranges`], the engine's N-way merge
    /// — so that a caller can never drive a view past its own end with a count it
    /// carried alongside. `#[inline(always)]` on every implementor: the per-row
    /// and per-morsel callers live in gnitz-zset, at opt-level 0.
    fn row_count(&self) -> usize;
}

/// A borrowed source reads as the source it borrows.
impl<T: RowSource + ?Sized> RowSource for &T {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        (**self).get_pk_bytes(row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        (**self).get_null_word(row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        (**self).get_col_ptr(row, payload_col, col_size)
    }
    #[inline(always)]
    fn blob(&self) -> &[u8] {
        (**self).blob()
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        (**self).row_count()
    }
}

/// A [`RowSource`] that can additionally hand out whole regions — the shape the
/// vectorized expression kernels need, and the one a flat region-based batch
/// (or an adapter that materializes one) can satisfy.
///
/// Region accessors return the WHOLE column/region, never a morsel slice: the
/// kernels index them absolutely.
///
/// CONTRACT binding the two shapes, checked by [`assert_batchview_consistent`]:
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

/// One PK-column expectation for [`assert_batchview_consistent`]: type code,
/// OPK byte offset, and the column's **native** value per row, sign-extended
/// into a `u128` so one element type states it at any width or signedness.
pub type PkColExpect<'a> = (gnitz_wire::TypeCode, usize, &'a [u128]);

/// Assert the region/per-row contract on [`BatchView`] for `rows` rows, the
/// given `(payload_col, col_size)` pairs and the given PK columns. Every
/// implementor must pass.
///
/// A normal `pub fn`, not a `#[cfg(test)]` helper, so every crate that adds an
/// implementor can call it from its own test tree. For a flat-region batch the
/// region/per-row property is near-tautological; for an adapter that
/// materializes columns out of a foreign representation it is the only thing
/// standing between a mis-mapped slot and silently wrong query results.
///
/// `pk` is what makes the PK region's *encoding* checkable rather than merely
/// self-consistent — the two halves agree under a native-LE implementor just as
/// readily as under an OPK one. Its offsets are pinned absolutely, and `rows` is
/// the caller's own expectation, because a harness that read either off the view
/// would leave exactly what it exists to catch unchecked.
pub fn assert_batchview_consistent<B: BatchView>(v: &B, rows: usize, cols: &[(usize, usize)], pk: &[PkColExpect<'_>]) {
    assert_eq!(v.row_count(), rows, "row_count()");
    let bmp = v.null_bmp();
    assert_eq!(bmp.len(), rows * 8, "null_bmp() must be rows * 8 bytes");
    for row in 0..rows {
        assert_eq!(
            v.get_null_word(row),
            gnitz_wire::read_u64_le(bmp, row * 8),
            "get_null_word({row}) disagrees with null_bmp()",
        );
    }
    let (pk_region, stride) = v.pk_region();
    assert_eq!(
        pk_region.len(),
        rows * stride,
        "pk_region() must be rows * stride bytes"
    );
    for row in 0..rows {
        assert_eq!(
            v.get_pk_bytes(row),
            &pk_region[row * stride..row * stride + stride],
            "get_pk_bytes({row}) disagrees with pk_region()",
        );
    }
    for &(type_code, byte_off, vals) in pk {
        let size = type_code.wire_stride();
        assert_eq!(
            vals.len(),
            rows,
            "PK column at offset {byte_off}: one expected value per row"
        );
        for (row, &want) in vals.iter().enumerate() {
            let mut native = [0u8; 16];
            gnitz_wire::decode_pk_cell(
                &v.get_pk_bytes(row)[byte_off..byte_off + size],
                type_code.is_signed_int(),
                &mut native[..size],
            );
            assert_eq!(
                &native[..size],
                &want.to_le_bytes()[..size],
                "PK column at offset {byte_off}, row {row}: the region is not OPK",
            );
        }
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

// ---------------------------------------------------------------------------
// Generic payload-cell readers
// ---------------------------------------------------------------------------
//
// [`RowSource`] is the whole surface reading one cell needs, so one spelling
// serves a batch, a stored row, a positioned read cursor and a mirror alike.

/// One row's fixed 8-byte payload slot `pi`, little-endian.
pub fn payload_u64<S: RowSource>(src: &S, row: usize, pi: usize) -> u64 {
    gnitz_wire::read_u64_le(src.get_col_ptr(row, pi, 8), 0)
}

/// One row's German-string (STRING or BLOB) payload slot `pi`, resolved through
/// the source's own blob heap so a value over 12 bytes reads back whole.
#[inline(always)]
pub fn payload_bytes<S: RowSource>(src: &S, row: usize, pi: usize) -> &[u8] {
    let cell = src.get_col_ptr(row, pi, 16);
    gnitz_wire::german_string_content(cell, src.blob())
}

/// [`payload_bytes`] as a `&str`; empty when not UTF-8. The borrowed twin of
/// [`payload_string`], for a consumer that only reads the cell.
pub fn payload_str<S: RowSource>(src: &S, row: usize, pi: usize) -> &str {
    std::str::from_utf8(payload_bytes(src, row, pi)).unwrap_or_default()
}

/// [`payload_str`] as an owned `String`, for a consumer that keeps the value.
pub fn payload_string<S: RowSource>(src: &S, row: usize, pi: usize) -> String {
    payload_str(src, row, pi).to_string()
}

/// Whether one row's payload slot `pi` holds NULL — the null-bit member of this
/// family, so a reader that needs both the bit and the value addresses them
/// through the same payload index.
pub fn payload_is_null<S: RowSource>(src: &S, row: usize, pi: usize) -> bool {
    gnitz_wire::null_word_get(src.get_null_word(row), pi)
}

#[cfg(test)]
#[path = "tests/view.rs"]
mod tests;
