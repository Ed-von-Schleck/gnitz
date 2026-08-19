//! Shared columnar data access trait and generic row comparison.

use std::cmp::Ordering;

use super::merge::ColPtr;
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;
use gnitz_wire::{compare_german_strings, null_word_get, read_unsigned_exact};

// ---------------------------------------------------------------------------
// ColumnarSource trait
// ---------------------------------------------------------------------------

/// A row source that also carries the Z-set weight — i.e. a *storage* row
/// rather than a bare evaluator input.
///
/// The per-row accessors are [`RowSource`], the one shared definition the
/// resolved-addressing types ([`crate::schema::ColumnLocator`]) bind to, so any
/// `ColumnarSource` can be read through a locator directly. This trait adds
/// exactly the one method the evaluator has no use for.
///
/// It cannot be folded into `BatchView` in the other direction: `MappedShard`,
/// `RowRef` and `CursorSource` can address a cell but have no contiguous
/// `rows * col_size` region to hand out (a shard column may be a scalar
/// constant), so the split is at the per-row/region seam, not at this one. The
/// N-way merge's per-source walk bound is `RowSource::row_count`, one level
/// down: every row source is a whole batch, weight-bearing or not.
pub(crate) trait ColumnarSource: RowSource {
    /// The row's signed Z-set weight / multiplicity (region[1]).
    fn get_weight(&self, row: usize) -> i64;

    /// Whether this source's rows are (PK, coarse weight) pairs with no payload —
    /// a capacity-bounded view's skeleton shard. `false` for every in-memory
    /// source, which always carries its payload.
    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Generic compare_rows
// ---------------------------------------------------------------------------

/// Compare two rows from any [`RowSource`] implementations by payload columns.
///
/// This is the canonical implementation with the hoisted null_word optimisation:
/// null words are read once per row outside the column loop.
#[inline]
pub(crate) fn compare_rows<A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    compare_rows_impl::<false, A, B>(schema, src_a, row_a, src_b, row_b, 0)
}

/// [`compare_rows`] with a set of payload columns excluded (`skip_mask` bit `pi`
/// skips payload column `pi`; payload count ≤ 64 by the null-word invariant) —
/// the catalog CAS's "a rewrite pair may differ only in these fields" probe.
/// Cold path; `compare_rows` monomorphizes with the skip test compiled out
/// (`SKIP = false`).
pub(crate) fn compare_rows_except<A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
    skip_mask: u64,
) -> Ordering {
    compare_rows_impl::<true, A, B>(schema, src_a, row_a, src_b, row_b, skip_mask)
}

#[inline]
fn compare_rows_impl<const SKIP: bool, A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
    skip_mask: u64,
) -> Ordering {
    let null_word_a = src_a.get_null_word(row_a);
    let null_word_b = src_b.get_null_word(row_b);
    // Both blob arenas are loop-invariant, and only the German-string arm of
    // `cmp_col_window` reads them. For a `MemBatch` that is a field load, but a
    // `MappedShard`/`CursorSource`/`RowRef` resolves the mmap behind two calls —
    // which this would otherwise pay twice per payload column per comparison, on
    // the hottest comparator in the merge path.
    let (blob_a, blob_b) = (src_a.blob(), src_b.blob());

    for (payload_col, col) in schema.payload_columns() {
        if SKIP && (skip_mask >> payload_col) & 1 != 0 {
            continue;
        }
        let null_a = null_word_get(null_word_a, payload_col);
        let null_b = null_word_get(null_word_b, payload_col);
        if null_a && null_b {
            continue;
        }
        if null_a {
            return Ordering::Less;
        }
        if null_b {
            return Ordering::Greater;
        }

        let cs = col.size() as usize;
        let ord = cmp_col_window(
            src_a.get_col_ptr(row_a, payload_col, cs),
            blob_a,
            src_b.get_col_ptr(row_b, payload_col, cs),
            blob_b,
            col.type_code,
        );

        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

/// Compare two equal-width column windows of the given raw `u8` type code,
/// dispatching German strings (STRING/BLOB) to content comparison through their
/// backing blob arenas and every fixed-width type to `gnitz_wire::cmp_typed_le`
/// (which is deliberately string-free — a mis-routed 16-byte string window hits
/// its width `unreachable!` rather than silently mis-comparing). The blob slices
/// back each side's German-string heap payload (ignored for non-string columns).
///
/// This is the single home for the "STRING and BLOB share the 16-byte layout, so
/// they must be compared by content before the fixed-width dispatch" rule:
/// `compare_rows` and the group-by / payload comparators in `ops` all route through
/// here rather than re-spelling the per-type dispatch (a missed site would mis-order
/// a BLOB key).
#[inline]
pub(crate) fn cmp_col_window(a: &[u8], a_blob: &[u8], b: &[u8], b_blob: &[u8], type_code: u8) -> Ordering {
    if gnitz_wire::is_german_string(type_code) {
        compare_german_strings(a, a_blob, b, b_blob)
    } else {
        gnitz_wire::cmp_typed_le(a, b, type_code)
    }
}

use crate::schema::key::PkSortKey; // the OPK register sort key, for the seek fast path below

// ---------------------------------------------------------------------------
// Sorted-stream lower-bound search (stateless + galloping)
// ---------------------------------------------------------------------------

/// Lower bound over `[lo, hi)`: the first index whose row sorts at-or-after the
/// probe, where `lt(i)` reports row `i < probe`. The byte and register seek paths
/// differ only in `lt` — a slice `memcmp` vs an OPK-register integer compare — so
/// the search skeleton lives here once.
#[inline]
fn lower_bound_by(mut lo: usize, mut hi: usize, lt: impl Fn(usize) -> bool) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if lt(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Galloping lower bound over `[0, count)`, seeded at `hint`: `O(log gap)` forward
/// when the boundary is after the hint, `O(1)` when the boundary IS the hint
/// (consecutive keys in one inter-row gap, or a run past the source end with
/// `hint == count`), and a bounded `[0, hint)` search when it is before. Correct
/// for ANY hint, and since `[0, hint] ⊆ [0, count)` it is **never asymptotically
/// worse** than `lower_bound_by(0, count, …)` — a backward or stale hint forfeits
/// only the speedup, at the cost of at most two extra comparisons. `lt(i)` reports
/// row `i < probe`, as in [`lower_bound_by`].
#[inline]
fn gallop_by(count: usize, hint: usize, lt: impl Fn(usize) -> bool) -> usize {
    let h = hint.min(count);
    if h < count && lt(h) {
        // boundary strictly after the hint
        let mut lo = h; // invariant: row `lo` sorts before the probe
        let mut step = 1usize;
        while lo + step < count && lt(lo + step) {
            lo += step;
            step *= 2;
        }
        let hi = (lo + step).min(count); // row `hi` sorts >= probe, or hi == count
        return lower_bound_by(lo + 1, hi, lt);
    }
    if h == 0 || lt(h - 1) {
        return h;
    } // boundary is exactly h (incl. h == count)
    lower_bound_by(0, h, lt) // genuine overshoot: bounded [0, h)
}

/// Byte-`memcmp` lower bound over `[lo, hi)`. `get(i)` yields row `i`'s OPK PK
/// bytes; memcmp order equals typed order at every PK width. The named `'a` is
/// load-bearing: a bare `Fn(usize) -> &[u8]` desugars to a higher-ranked bound the
/// `|i| self.get_pk_bytes(i)` closures (result borrows `self`) cannot satisfy.
#[inline]
fn binary_lower_bound<'a>(lo: usize, hi: usize, key: &[u8], get: &impl Fn(usize) -> &'a [u8]) -> usize {
    lower_bound_by(lo, hi, |i| get(i) < key)
}

/// Galloping byte-`memcmp` lower bound; see [`gallop_by`]. The seek fallback for PK
/// strides past the register widths, and the byte oracle in tests.
#[inline]
fn gallop_lower_bound_bytes<'a>(count: usize, key: &[u8], hint: usize, get: impl Fn(usize) -> &'a [u8]) -> usize {
    gallop_by(count, hint, |i| get(i) < key)
}

// ---------------------------------------------------------------------------
// Seek fast path: load each `pk_stride` OPK key into a register and compare it as
// an integer. For two equal-width OPK images the integer order is the byte order
// (`compare_pk_bytes`), so a register search returns the identical lower bound as
// the byte search above — pinned by `lower_bound_opk_matches_byte_search`. The key
// is `PkSortKey::from_opk`, the same register key the merge/sort drive uses, and
// the stride→width dispatch mirrors `reduce::sort::sort_indices_by_pk`. Every
// `from_opk` arm is a full order-preserving key (the `[u128; 2]` arm's low limb
// settles a leading-16-byte tie), so the dispatch is exact through 32-byte strides;
// wider PKs fall back to the byte search.
// ---------------------------------------------------------------------------

/// Binds the probe and row keys of the two OPK seek entry points below to the
/// width-matched `PkSortKey` and hands the resulting `lt` predicate to the given
/// search; the width taxonomy itself is `key::pk_width_dispatch`. The two
/// searches (stateless vs. galloping) stay separate algorithms behind it.
macro_rules! opk_width_dispatch {
    ($stride:expr, $key:expr, $get:expr, |$lt:ident| $search:expr, $bytes_fallback:expr) => {
        crate::schema::key::pk_width_dispatch!(
            $stride,
            |K| {
                let p = K::from_opk($key);
                let $lt = |i: usize| K::from_opk($get(i)) < p;
                $search
            },
            $bytes_fallback
        )
    };
}

/// Stride-dispatched OPK lower bound — the seek-path entry point. `stride` is the
/// stored key width (`pk_stride`); `key` is exactly `stride` OPK bytes (a full key,
/// or a prefix the caller already zero-padded to `stride`).
#[inline]
pub(crate) fn lower_bound_opk<'a>(count: usize, key: &[u8], stride: usize, get: impl Fn(usize) -> &'a [u8]) -> usize {
    debug_assert_eq!(key.len(), stride, "seek probe width must equal pk_stride");
    opk_width_dispatch!(
        stride,
        key,
        get,
        |lt| lower_bound_by(0, count, lt),
        binary_lower_bound(0, count, key, &get)
    )
}

/// Stride-dispatched galloping lower bound; see [`lower_bound_opk`]. Same dispatch,
/// same byte fallback.
#[inline]
pub(crate) fn gallop_opk<'a>(
    count: usize,
    key: &[u8],
    hint: usize,
    stride: usize,
    get: impl Fn(usize) -> &'a [u8],
) -> usize {
    debug_assert_eq!(key.len(), stride, "seek probe width must equal pk_stride");
    opk_width_dispatch!(
        stride,
        key,
        get,
        |lt| gallop_by(count, hint, lt),
        gallop_lower_bound_bytes(count, key, hint, get)
    )
}

// ---------------------------------------------------------------------------
// Region-addressed seek entry points
//
// Every sorted PK-region holder — an owned `Batch`, a `MappedShard`, and the
// `Run` that is either — seeks through the same `(count, stride, ColPtr)` triple.
// These two wrappers own the row-addressing `unsafe`, so it is written once here
// rather than repeated at each holder's method.
// ---------------------------------------------------------------------------

/// First row of `pk` whose OPK bytes are `>= key`. `key` must be exactly
/// `stride` OPK bytes; memcmp order is typed order at every width.
///
/// # Safety
/// `pk` must address at least `count` rows of `stride` bytes.
#[inline]
pub(crate) unsafe fn seek_lower_bound(count: usize, stride: usize, pk: ColPtr, key: &[u8]) -> usize {
    lower_bound_opk(count, key, stride, |i| pk.row(i, stride))
}

/// [`seek_lower_bound`] seeded at `hint` (the caller's live position):
/// `O(log gap)` when the boundary is just ahead, `O(1)` when it IS the hint,
/// never worse than the from-scratch search.
///
/// # Safety
/// As [`seek_lower_bound`].
#[inline]
pub(crate) unsafe fn seek_advance_to(count: usize, stride: usize, pk: ColPtr, key: &[u8], hint: usize) -> usize {
    gallop_opk(count, key, hint, stride, |i| pk.row(i, stride))
}

// ---------------------------------------------------------------------------
// Fast path: fixed-width integer, non-nullable schemas (any signedness)
// ---------------------------------------------------------------------------

/// True iff every payload column is non-nullable and a fixed-width integer of
/// ≤ 8 bytes (I8..I64 or U8..U64, any signedness). U128/UUID excluded. This is
/// the exact predicate `SchemaDescriptor::new` evaluates once into
/// `payload_cmp`, so we read that cached field rather than re-walk the columns.
#[inline]
pub(crate) fn schema_is_fixedint_nonnull(schema: &SchemaDescriptor) -> bool {
    schema.payload_cmp == crate::schema::PayloadCmpKind::FixedIntNonnull
}

/// Fast path for non-nullable fixed-width integer payloads of any signedness.
/// Maps each column to an order-preserving u64: zero-extend, then flip the sign
/// bit for signed columns. Produces the same order as `compare_rows` while
/// skipping null-bitmap reads and the per-column type-code dispatch. Caller MUST
/// guarantee `schema_is_fixedint_nonnull(schema)`.
#[inline]
pub(crate) fn compare_rows_fixedint_nonnull<A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    debug_assert!(
        schema_is_fixedint_nonnull(schema),
        "compare_rows_fixedint_nonnull on a non-fixedint or nullable schema",
    );
    for (payload_col, col) in schema.payload_columns() {
        let cs = col.size() as usize;
        // Branchless sign-flip: signed columns flip their MSB so two's-complement
        // negatives sort below non-negatives; `is_signed` is 0 for unsigned
        // columns, so the XOR is a no-op there. `cs*8-1 ∈ {7,15,31,63}` is always
        // a valid u64 shift, because `FixedIntNonnull` admits a column only if
        // `is_fixed_int(type_code)` — which is exactly "1/2/4/8 bytes", pinned by
        // `gnitz_wire`'s `is_fixed_int_implies_shiftable_width`. Reads
        // `size`/`is_signed` only — never `type_code`.
        let sign_flip = (col.is_signed() as u64) << (cs * 8 - 1);
        // `get_col_ptr` returns exactly `cs` bytes, so this is the exact-width
        // read: the width form's `bytes[..cs]` bound is an out-of-line call at
        // `opt-level=0`, twice per comparison, on the hottest merge comparator.
        let av = read_unsigned_exact(src_a.get_col_ptr(row_a, payload_col, cs)) ^ sign_flip;
        let bv = read_unsigned_exact(src_b.get_col_ptr(row_b, payload_col, cs)) ^ sign_flip;
        let ord = av.cmp(&bv);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Select the payload row comparator from `$schema.payload_cmp` and hand it to
/// a generic helper. `with_payload_cmp!(schema, func, args...)` expands to
/// `func(args..., cmp)`, appending the selected comparator as the trailing
/// argument. The comparator stays an inlined closure — a stored `fn` pointer
/// would turn each comparison into an indirect call — so codegen matches an
/// open-coded match.
///
/// The closure is passed *directly* into the call, so closure signature
/// deduction makes it higher-ranked over each source's borrow
/// (`for<'a> Fn(&'a Src, ..)`) — what every `_with`/`_inner` helper's `Fn` bound
/// requires. `func` is any path: a free fn, `Self::assoc`, or (via UFCS)
/// `Self::method` with the receiver passed as the first argument. Helpers that
/// need to *adapt* the comparator (e.g. wrap it over fixed operands) must do so
/// inside the called helper, where the comparator is a concrete generic
/// parameter rather than a lifetime-pinned `let` binding.
macro_rules! with_payload_cmp {
    ($schema:expr, $f:path $(, $arg:expr)* $(,)?) => {
        match $schema.payload_cmp {
            $crate::schema::PayloadCmpKind::FixedIntNonnull => $f(
                $($arg,)*
                |s, a, ai, b, bi| $crate::storage::compare_rows_fixedint_nonnull(s, a, ai, b, bi),
            ),
            $crate::schema::PayloadCmpKind::Generic => $f(
                $($arg,)*
                |s, a, ai, b, bi| $crate::storage::compare_rows(s, a, ai, b, bi),
            ),
        }
    };
}
pub(crate) use with_payload_cmp;

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::pk_only_schema;

    /// A minimal ColumnarSource for unit tests.
    struct TestBatch {
        null_bmp: Vec<u8>,
        col_data: Vec<Vec<u8>>,
        blob: Vec<u8>,
    }

    impl RowSource for TestBatch {
        // TestBatch models payload-only comparison; PK/weight are never read through it.
        fn get_pk_bytes(&self, _row: usize) -> &[u8] {
            &[]
        }
        fn get_null_word(&self, row: usize) -> u64 {
            gnitz_wire::read_u64_le(&self.null_bmp, row * 8)
        }
        fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
            let off = row * col_size;
            &self.col_data[payload_col][off..off + col_size]
        }
        fn blob(&self) -> &[u8] {
            &self.blob
        }
        fn row_count(&self) -> usize {
            self.null_bmp.len() / 8
        }
    }

    impl ColumnarSource for TestBatch {
        fn get_weight(&self, _row: usize) -> i64 {
            1
        }
    }

    /// Build a 3-column schema: [PK:U64, nullable I64, F64].
    fn make_schema_nullable_float() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0],
        )
    }

    /// Build a TestBatch with [nullable I64, F64] payload columns.
    /// Each row is (null_word, col0_i64, col1_f64).
    fn batch_from_rows(rows: &[(u64, i64, f64)]) -> TestBatch {
        let n = rows.len();
        let mut null_bmp = Vec::with_capacity(n * 8);
        let mut col0 = Vec::with_capacity(n * 8);
        let mut col1 = Vec::with_capacity(n * 8);

        for &(nw, c0, c1_f) in rows {
            null_bmp.extend_from_slice(&nw.to_le_bytes());
            col0.extend_from_slice(&c0.to_le_bytes());
            col1.extend_from_slice(&c1_f.to_bits().to_le_bytes());
        }

        TestBatch {
            null_bmp,
            col_data: vec![col0, col1],
            blob: vec![],
        }
    }

    /// Null sorts below non-null.
    #[test]
    fn test_compare_rows_null_lt_non_null() {
        let schema = make_schema_nullable_float();
        // Row A: col1 = NULL (null_word bit 0 set), col2 = -5.0
        // Row B: col1 = 10, col2 = 5.0
        let batch = batch_from_rows(&[
            (1, 0, -5.0), // row 0: null_word=1 → payload col 0 is null
            (0, 10, 5.0), // row 1: null_word=0 → nothing null
        ]);
        // null < non-null
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
        // non-null > null
        assert_eq!(compare_rows(&schema, &batch, 1, &batch, 0), Ordering::Greater);
    }

    /// Two nulls compare equal, so the next column decides.
    #[test]
    fn test_compare_rows_null_eq_null() {
        let schema = make_schema_nullable_float();
        // Both rows have col1 = NULL, col2 differs
        let batch = batch_from_rows(&[
            (1, 0, -5.0), // null col1, f64 = -5.0
            (1, 0, 5.0),  // null col1, f64 = 5.0
        ]);
        // null == null → fall through to col2: -5.0 < 5.0
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    /// Float ordering: 5.0 > -5.0.
    #[test]
    fn test_compare_rows_float() {
        let schema = make_schema_nullable_float();
        // Row B: col1 = 10, col2 = 5.0
        // Row C: col1 = 10, col2 = -5.0
        let batch = batch_from_rows(&[(0, 10, 5.0), (0, 10, -5.0)]);
        // col1 equal (10 == 10), col2: 5.0 > -5.0
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Greater);
    }

    /// A row compares equal to itself.
    #[test]
    fn test_compare_rows_equality() {
        let schema = make_schema_nullable_float();
        let batch = batch_from_rows(&[(0, 10, -5.0)]);
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 0), Ordering::Equal);
    }

    /// Helper to build a single-payload-column TestBatch.
    fn single_col_batch(null_words: &[u64], col_data: Vec<u8>) -> TestBatch {
        let mut null_bmp = Vec::new();
        for &nw in null_words {
            null_bmp.extend_from_slice(&nw.to_le_bytes());
        }
        TestBatch {
            null_bmp,
            col_data: vec![col_data],
            blob: vec![],
        }
    }

    /// Test signed integer comparison: negative < positive via sign extension.
    #[test]
    fn test_compare_rows_signed_int() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );

        let mut col0 = Vec::new();
        col0.extend_from_slice(&(-42i64).to_le_bytes());
        col0.extend_from_slice(&42i64.to_le_bytes());
        let batch = single_col_batch(&[0, 0], col0);
        // -42 < 42
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    /// Test U128 column comparison.
    #[test]
    fn test_compare_rows_u128() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U128, 0),
            ],
            &[0],
        );

        let mut col0 = Vec::new();
        // Row 0: u128 = 1 (lo=1, hi=0)
        col0.extend_from_slice(&1u64.to_le_bytes());
        col0.extend_from_slice(&0u64.to_le_bytes());
        // Row 1: u128 = (1 << 64) (lo=0, hi=1)
        col0.extend_from_slice(&0u64.to_le_bytes());
        col0.extend_from_slice(&1u64.to_le_bytes());
        let batch = single_col_batch(&[0, 0], col0);
        // 1 < (1 << 64)
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    /// A 16-byte I128 payload column (a cross-sign `_join_pk` surfaced into a
    /// payload slot) must order as a SIGNED two's-complement value: -1 < 0 < 1.
    /// The pre-fix wildcard arm hit `read_signed(.., 16)` => unreachable!, and an
    /// unsigned-u128 reading would sort -1 (= u128::MAX bits) above 0 and 1.
    #[test]
    fn test_compare_rows_i128_signed() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I128, 0),
            ],
            &[0],
        );
        let mut col0 = Vec::new();
        for v in [-1i128, 0, 1] {
            col0.extend_from_slice(&v.to_le_bytes());
        }
        let batch = single_col_batch(&[0, 0, 0], col0);
        // -1 < 0 < 1 (signed), not unsigned (where -1's bits = u128::MAX > 0, 1).
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
        assert_eq!(compare_rows(&schema, &batch, 1, &batch, 2), Ordering::Less);
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 2), Ordering::Less);
        assert_eq!(compare_rows(&schema, &batch, 2, &batch, 0), Ordering::Greater);
    }

    // Distinct UUID payloads must not collapse to Equal: a 16-byte read that
    // returned 0 would make every UUID compare Equal and silently drop rows in
    // consolidation/compaction.
    #[test]
    fn test_compare_rows_uuid_distinct() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::UUID, 0),
            ],
            &[0],
        );

        let mut col0 = Vec::new();
        // Row 0: lo=1, hi=0
        col0.extend_from_slice(&1u64.to_le_bytes());
        col0.extend_from_slice(&0u64.to_le_bytes());
        // Row 1: lo=0, hi=1
        col0.extend_from_slice(&0u64.to_le_bytes());
        col0.extend_from_slice(&1u64.to_le_bytes());
        let batch = single_col_batch(&[0, 0], col0);
        assert_ne!(
            compare_rows(&schema, &batch, 0, &batch, 1),
            Ordering::Equal,
            "distinct UUID payloads must not compare Equal",
        );
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    // Unsigned payloads with the high bit set must not be read as negative,
    // which would reverse sort order: 0 < MAX must hold for U64/U32/U16/U8.
    #[test]
    fn test_compare_rows_unsigned_high_bit() {
        for (tc, size) in [
            (type_code::U64, 8usize),
            (type_code::U32, 4),
            (type_code::U16, 2),
            (type_code::U8, 1),
        ] {
            let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0]);
            let max = u64::MAX >> (64 - size * 8);
            let mut col0 = Vec::new();
            col0.extend_from_slice(&0u64.to_le_bytes()[..size]);
            col0.extend_from_slice(&max.to_le_bytes()[..size]);
            let batch = single_col_batch(&[0, 0], col0);
            assert_eq!(
                compare_rows(&schema, &batch, 0, &batch, 1),
                Ordering::Less,
                "0 must be less than the max unsigned value for type {tc}",
            );
        }
    }

    /// Test that NaN values produce a stable total order (not all-Equal, which
    /// would violate transitivity and cause sort algorithms to misbehave).
    #[test]
    fn test_compare_rows_nan() {
        let schema = make_schema_nullable_float();
        // Row 0: col0=0, col1=NaN
        // Row 1: col0=0, col1=1.0
        let batch = batch_from_rows(&[(0, 0, f64::NAN), (0, 0, 1.0)]);
        // total_cmp: positive NaN is ordered above all finite values
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Greater);
        assert_eq!(compare_rows(&schema, &batch, 1, &batch, 0), Ordering::Less);
        // NaN vs NaN → Equal (IEEE 754 total order)
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 0), Ordering::Equal);
    }

    /// Test STRING column comparison via compare_rows (short strings).
    #[test]
    fn test_compare_rows_string() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );

        let mut col0 = Vec::new();
        let mut blob = Vec::new();
        col0.extend_from_slice(&gnitz_wire::encode_german_string(b"abc", &mut blob));
        col0.extend_from_slice(&gnitz_wire::encode_german_string(b"abd", &mut blob));

        let batch = single_col_batch(&[0, 0], col0);
        // "abc" < "abd"
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    // -----------------------------------------------------------------------
    // Fast path: schema_is_fixedint_nonnull / compare_rows_fixedint_nonnull
    // -----------------------------------------------------------------------

    fn make_schema(cols: &[(u8, u8)]) -> SchemaDescriptor {
        // First column is PK (U64); subsequent are payload columns.
        let mut columns = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        for (i, &(tc, nullable)) in cols.iter().enumerate() {
            columns[i + 1] = SchemaColumn::new(tc, nullable);
        }
        let n = 1 + cols.len();
        SchemaDescriptor::new(&columns[..n], &[0])
    }

    /// Encode `vals` (each value's low `cs` bytes, two's-complement for signed,
    /// zero-extended for unsigned) into a single-payload-column batch and assert
    /// the fast path matches the generic comparator for every ordered pair.
    fn check_single_col_fixedint(tc: u8, vals: &[i128]) {
        let schema = make_schema(&[(tc, 0)]);
        let cs = gnitz_wire::wire_stride(tc);
        let mut col = Vec::new();
        for &v in vals {
            col.extend_from_slice(&v.to_le_bytes()[..cs]);
        }
        let batch = single_col_batch(&vec![0u64; vals.len()], col);
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                assert_eq!(
                    compare_rows_fixedint_nonnull(&schema, &batch, i, &batch, j),
                    compare_rows(&schema, &batch, i, &batch, j),
                    "tc={tc} mismatch at ({i}, {j})",
                );
            }
        }
    }

    /// Verify the fast path agrees with the generic `compare_rows` for signed,
    /// unsigned, and mixed-sign non-nullable fixed-int schemas across widths.
    #[test]
    fn test_compare_rows_fixedint_nonnull_matches_generic() {
        // Signed: MIN / -1 / 0 / 1 / MAX exercise the sign-flip across the MSB.
        check_single_col_fixedint(type_code::I8, &[i8::MIN as i128, -1, 0, 1, i8::MAX as i128]);
        check_single_col_fixedint(type_code::I16, &[i16::MIN as i128, -1, 0, 1, i16::MAX as i128]);
        check_single_col_fixedint(type_code::I32, &[i32::MIN as i128, -1, 0, 1, i32::MAX as i128]);
        check_single_col_fixedint(type_code::I64, &[i64::MIN as i128, -1, 0, 1, i64::MAX as i128]);
        // Unsigned: include high-bit-set values that signed reads would invert.
        check_single_col_fixedint(type_code::U8, &[0, 1, 0x7F, 0x80, 0xFF]);
        check_single_col_fixedint(type_code::U16, &[0, 1, 0x7FFF, 0x8000, 0xFFFF]);
        check_single_col_fixedint(type_code::U32, &[0, 1, 0x7FFF_FFFF, 0x8000_0000, 0xFFFF_FFFF]);
        check_single_col_fixedint(
            type_code::U64,
            &[0, 1, 0x7FFF_FFFF_FFFF_FFFF, 0x8000_0000_0000_0000, u64::MAX as i128],
        );

        // Multi-column schemas. (I32, I64) is all-signed (primary diff col 0,
        // tie-break col 1); (I32, U32) and (U64, I64) mix signedness, which the
        // per-column sign flip must handle without falling back.
        for (c0, c1) in [
            (type_code::I32, type_code::I64),
            (type_code::I32, type_code::U32),
            (type_code::U64, type_code::I64),
        ] {
            let schema = make_schema(&[(c0, 0), (c1, 0)]);
            let (col0, col1) = mixed_rows(c0, c1);
            let n = col0.len() / gnitz_wire::wire_stride(c0);
            let mut null_bmp = Vec::new();
            for _ in 0..n {
                null_bmp.extend_from_slice(&0u64.to_le_bytes());
            }
            let batch = TestBatch {
                null_bmp,
                col_data: vec![col0, col1],
                blob: vec![],
            };
            for i in 0..n {
                for j in 0..n {
                    assert_eq!(
                        compare_rows_fixedint_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                        "mixed ({c0},{c1}) mismatch at ({i}, {j})",
                    );
                }
            }
        }
    }

    /// Build two payload columns of types `(a, b)` with rows spanning negative,
    /// zero, small-positive, and high-bit-set values so signedness matters.
    fn mixed_rows(a: u8, b: u8) -> (Vec<u8>, Vec<u8>) {
        let cs_a = gnitz_wire::wire_stride(a);
        let cs_b = gnitz_wire::wire_stride(b);
        // (col0, col1) value pairs as i128 images; -1 stresses signed ordering,
        // the large positive stresses unsigned high-bit ordering.
        let pairs: &[(i128, i128)] = &[(-1, 5), (-1, 7), (0, 0), (1, -1), (1, 9)];
        let mut col0 = Vec::new();
        let mut col1 = Vec::new();
        for &(v0, v1) in pairs {
            col0.extend_from_slice(&v0.to_le_bytes()[..cs_a]);
            col1.extend_from_slice(&v1.to_le_bytes()[..cs_b]);
        }
        (col0, col1)
    }

    /// `schema_is_fixedint_nonnull`: all-signed, all-unsigned, and mixed-sign
    /// non-null schemas all pass; nullable and U128/UUID/F32/F64/STRING/BLOB fail.
    #[test]
    fn test_schema_is_fixedint_nonnull_bounds() {
        // All-signed, non-nullable → true
        assert!(schema_is_fixedint_nonnull(&make_schema(&[
            (type_code::I8, 0),
            (type_code::I16, 0),
            (type_code::I32, 0),
            (type_code::I64, 0),
        ])));
        // All-unsigned, non-nullable → true
        assert!(schema_is_fixedint_nonnull(&make_schema(&[
            (type_code::U8, 0),
            (type_code::U16, 0),
            (type_code::U32, 0),
            (type_code::U64, 0),
        ])));
        // Mixed signed/unsigned, non-nullable → true
        assert!(schema_is_fixedint_nonnull(&make_schema(&[
            (type_code::I64, 0),
            (type_code::U32, 0),
        ])));
        // Empty payload (all-PK) → vacuously true
        assert!(schema_is_fixedint_nonnull(&pk_only_schema(&[type_code::U64])));

        // Nullable fixed int → false
        assert!(!schema_is_fixedint_nonnull(&make_schema(&[(type_code::I32, 1)])));
        assert!(!schema_is_fixedint_nonnull(&make_schema(&[(type_code::U32, 1)])));

        // Each non-fixed-int type → false (U128/UUID exceed 8 bytes; floats/strings/blobs)
        for tc in [
            type_code::U128,
            type_code::UUID,
            type_code::F32,
            type_code::F64,
            type_code::STRING,
            type_code::BLOB,
        ] {
            assert!(
                !schema_is_fixedint_nonnull(&make_schema(&[(tc, 0)])),
                "expected schema with type_code={tc} to be rejected",
            );
        }

        // A single disqualifier among valid columns kills it.
        assert!(!schema_is_fixedint_nonnull(&make_schema(&[
            (type_code::I64, 0),
            (type_code::U128, 0),
        ])));
    }

    // -----------------------------------------------------------------------
    // compare_rows_fixedint_nonnull ≡ compare_rows property test
    // -----------------------------------------------------------------------

    mod fixedint_proptest {
        use super::*;
        use proptest::prelude::*;

        fn arb_fixedint_type() -> impl Strategy<Value = u8> {
            prop_oneof![
                Just(type_code::I8),
                Just(type_code::U8),
                Just(type_code::I16),
                Just(type_code::U16),
                Just(type_code::I32),
                Just(type_code::U32),
                Just(type_code::I64),
                Just(type_code::U64),
            ]
        }

        /// `(payload type codes, n rows, per-column row-major bytes)`. 1..=4
        /// columns over all eight fixed-int types spans every sign combination
        /// including mixed; random bytes frequently set the high bit, so a
        /// signedness bug surfaces as a fast-vs-generic disagreement.
        fn arb_case() -> impl Strategy<Value = (Vec<u8>, usize, Vec<Vec<u8>>)> {
            (prop::collection::vec(arb_fixedint_type(), 1..=4), 2usize..=6usize).prop_flat_map(|(types, n)| {
                let cols: Vec<_> = types
                    .iter()
                    .map(|&t| {
                        let cs = gnitz_wire::wire_stride(t);
                        prop::collection::vec(any::<u8>(), n * cs)
                    })
                    .collect();
                (Just(types), Just(n), cols)
            })
        }

        proptest! {
            /// The load-bearing guarantee: the fixed-int fast path produces the
            /// same order as the generic comparator for every random row pair,
            /// across all widths {1,2,4,8} and every signed/unsigned mix.
            #[test]
            fn fixedint_matches_generic((types, n, col_data) in arb_case()) {
                let payload: Vec<(u8, u8)> = types.iter().map(|&t| (t, 0)).collect();
                let schema = make_schema(&payload);
                prop_assert!(schema_is_fixedint_nonnull(&schema));

                let batch = TestBatch { null_bmp: vec![0u8; n * 8], col_data, blob: vec![] };
                for i in 0..n {
                    for j in 0..n {
                        prop_assert_eq!(
                            compare_rows_fixedint_nonnull(&schema, &batch, i, &batch, j),
                            compare_rows(&schema, &batch, i, &batch, j),
                            "mismatch at ({}, {})", i, j,
                        );
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // gallop_lower_bound_bytes / binary_lower_bound
    // -----------------------------------------------------------------------

    /// Galloping seek equals the from-scratch lower bound for EVERY hint and key.
    /// Sweeps `hint` across `0..=count` (gallop branch, O(1) boundary-at-hint,
    /// `hint == count` run-off, and the overshoot fallback all fall out of the
    /// full sweep) and `key` across below-min / present / absent-between /
    /// duplicate / above-max values. 2-byte BE keys so memcmp order = numeric.
    #[test]
    fn gallop_lower_bound_matches_binary_over_all_hints() {
        let vals: [u16; 8] = [10, 10, 20, 30, 30, 30, 40, 50]; // duplicates + gaps
        let arr: Vec<[u8; 2]> = vals.iter().map(|v| v.to_be_bytes()).collect();
        let count = arr.len();
        let get = |i: usize| &arr[i][..];

        let probes: [u16; 12] = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60];
        for &p in &probes {
            let key = p.to_be_bytes();
            // Naive linear reference: first index whose bytes are >= key.
            let expected = (0..count).find(|&i| get(i) >= &key[..]).unwrap_or(count);
            assert_eq!(
                binary_lower_bound(0, count, &key, &get),
                expected,
                "binary_lower_bound key={p}"
            );
            for hint in 0..=count {
                assert_eq!(
                    gallop_lower_bound_bytes(count, &key, hint, get),
                    expected,
                    "gallop key={p} hint={hint}"
                );
            }
        }
    }

    /// `count == 0` returns 0 for every hint and never indexes the (empty) array.
    #[test]
    fn gallop_lower_bound_count_zero() {
        let arr: Vec<[u8; 2]> = vec![];
        let get = |i: usize| &arr[i][..];
        let key = 7u16.to_be_bytes();
        assert_eq!(gallop_lower_bound_bytes(0, &key, 0, get), 0);
        assert_eq!(binary_lower_bound(0, 0, &key, &get), 0);
    }

    /// `lower_bound_opk` / `gallop_opk` (the register dispatch) return the identical
    /// index as the byte oracle (`binary_lower_bound` / `gallop_lower_bound_bytes`)
    /// for every full-`stride` probe, across all four arms — `u64` (≤8), `u128`
    /// (9..=16), `[u128; 2]` (17..=32), and the byte fallback (>32). Order
    /// equivalence between the register and byte compare per width is the
    /// load-bearing property; the `from_opk` packers themselves are additionally
    /// pinned by `pack_pk_be_specialization_matches_naive` and the reduce argsort
    /// tests (incl. the `[u128; 2]` leading-16 tie). Values span two bytes, so a
    /// wrong-endian load would miscompare.
    #[test]
    fn lower_bound_opk_matches_byte_search() {
        // Right-align a value into the OPK tail (high bytes zero, the natural layout
        // for a small key) at any stride; `* 7` leaves gaps for between-key probes.
        let enc = |val: u64, stride: usize| -> Vec<u8> {
            let mut k = vec![0u8; stride];
            let t = stride.min(8);
            k[stride - t..].copy_from_slice(&val.to_be_bytes()[8 - t..]);
            k
        };
        for &stride in &[4usize, 8, 12, 16, 24, 32, 40] {
            let n = 200usize;
            let region: Vec<u8> = (0..n).flat_map(|i| enc((i as u64) * 7 + 3, stride)).collect();
            let get = |i: usize| &region[i * stride..i * stride + stride];

            // Every stored key, a below-all and above-all key, and one landing in
            // each inter-key gap (the last lands above all). All exactly `stride`.
            let mut probes: Vec<Vec<u8>> = (0..n).map(|i| get(i).to_vec()).collect();
            probes.push(enc(0, stride)); // below all (the min stored key encodes 3)
            probes.push(vec![0xffu8; stride]); // above all
            probes.extend((0..n).map(|i| enc((i as u64) * 7 + 3 + 4, stride))); // in-gap

            for p in &probes {
                assert_eq!(p.len(), stride);
                let oracle = binary_lower_bound(0, n, p, &get);
                assert_eq!(lower_bound_opk(n, p, stride, get), oracle, "stride={stride} p={p:02x?}");
                for &hint in &[0usize, n / 3, n, oracle] {
                    assert_eq!(gallop_lower_bound_bytes(n, p, hint, get), oracle);
                    assert_eq!(
                        gallop_opk(n, p, hint, stride, get),
                        oracle,
                        "gallop stride={stride} p={p:02x?}"
                    );
                }
            }
        }
    }
}
