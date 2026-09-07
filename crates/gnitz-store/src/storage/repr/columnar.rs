//! Shared columnar data access trait, the generic payload-cell readers, and
//! generic row comparison.

use std::cmp::Ordering;

use super::merge::ColPtr;
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;
use gnitz_wire::{cmp_col_window, null_word_get, read_unsigned_exact};

// ---------------------------------------------------------------------------
// ColumnarSource trait
// ---------------------------------------------------------------------------

/// A row source that also carries the Z-set weight — i.e. a *storage* row
/// rather than a bare evaluator input.
///
/// The per-row accessors are [`RowSource`], the one shared definition the
/// resolved-addressing types ([`crate::schema::ColumnLocator`]) bind to, so any
/// `ColumnarSource` can be read through a locator directly. This trait adds the
/// two the evaluator has no use for: the Z-set weight, and whether the row is a
/// payload-less skeleton.
///
/// It cannot be folded into `BatchView` in the other direction: `MappedShard`,
/// and the `Run` that may wrap one, can address a cell but have no contiguous
/// `rows * col_size` region to hand out — a shard column may be stored as one
/// constant element, or be absent from the file entirely — so the split is at
/// the per-row/region seam, not at this one. The
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
// Generic payload-cell readers
// ---------------------------------------------------------------------------
//
// `RowSource` is the whole surface reading one cell needs, so one spelling
// serves a `Batch`, a `StoredRow`, a positioned `ReadCursor` and a mirror alike.

/// One row's fixed 8-byte payload slot `pi`, little-endian.
pub fn payload_u64<S: RowSource>(src: &S, row: usize, pi: usize) -> u64 {
    let cell = src.get_col_ptr(row, pi, 8);
    // Exactly 8 bytes by construction — `get_col_ptr` returns the width asked for.
    u64::from_le_bytes(cell.try_into().expect("an 8-byte payload cell"))
}

/// One row's German-string (STRING or BLOB) payload slot `pi`, resolved through
/// the source's own blob heap so a value over 12 bytes reads back whole.
pub fn payload_bytes<S: RowSource>(src: &S, row: usize, pi: usize) -> &[u8] {
    let cell = src.get_col_ptr(row, pi, 16);
    gnitz_wire::german_string_content(cell, src.blob())
}

/// [`payload_bytes`] as a `String`; empty when not UTF-8.
pub fn payload_string<S: RowSource>(src: &S, row: usize, pi: usize) -> String {
    String::from_utf8(payload_bytes(src, row, pi).to_vec()).unwrap_or_default()
}

/// Whether one row's payload slot `pi` holds NULL — the null-bit member of this
/// family, so a reader that needs both the bit and the value addresses them
/// through the same payload index.
pub fn payload_is_null<S: RowSource>(src: &S, row: usize, pi: usize) -> bool {
    gnitz_wire::null_word_get(src.get_null_word(row), pi)
}

// ---------------------------------------------------------------------------
// Generic compare_rows
// ---------------------------------------------------------------------------

/// Compare two rows from any [`RowSource`] implementations by payload columns.
///
/// This is the canonical implementation with the hoisted null_word optimisation:
/// null words are read once per row outside the column loop.
#[inline]
pub fn compare_rows<A: RowSource, B: RowSource>(
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
pub fn compare_rows_except<A: RowSource, B: RowSource>(
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
    // `MappedShard` (and the `Run` wrapping one) resolves the mmap behind two
    // calls — which this would otherwise pay twice per payload column per
    // comparison, on the hottest comparator in the merge path.
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
// the stride→width dispatch is the shared `pk_width_dispatch!`. Every
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
        // a valid u64 shift: `FixedIntNonnull` admits a column only if
        // `gnitz_wire::is_fixed_int(type_code)`, whose eight type codes are
        // exactly the 1/2/4/8-byte widths. Reads `size`/`is_signed` only —
        // never `type_code`.
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
                |s, a, ai, b, bi| $crate::storage::columnar::compare_rows_fixedint_nonnull(s, a, ai, b, bi),
            ),
            $crate::schema::PayloadCmpKind::Generic => $f(
                $($arg,)*
                |s, a, ai, b, bi| $crate::storage::columnar::compare_rows(s, a, ai, b, bi),
            ),
        }
    };
}
pub(crate) use with_payload_cmp;

/// First row index past the equal-PK group beginning at `start`, in a PK-sorted
/// source. Requires `start < src.row_count()`. A linear step, not a seek: every
/// caller reaches it having just landed on the group's first row, where the
/// group is short and a gallop would cost more.
#[inline]
pub(crate) fn pk_group_end<S: RowSource>(src: &S, start: usize) -> usize {
    let k = src.get_pk_bytes(start);
    let count = src.row_count();
    let mut j = start + 1;
    while j < count && crate::schema::key::pk_bytes_eq(src.get_pk_bytes(j), k) {
        j += 1;
    }
    j
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/columnar.rs"]
mod tests;
