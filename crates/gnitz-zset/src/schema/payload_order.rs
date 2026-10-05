//! The payload row order — the (PK, payload) total order's second term — and the
//! per-schema choice between its two comparators, made once in
//! [`SchemaDescriptor::new`] and read through [`with_payload_cmp!`].

use std::cmp::Ordering;

use super::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::RowSource;
use gnitz_wire::{cmp_col_window, null_word_get, read_unsigned_exact};

/// Which comparator orders a schema's payload columns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PayloadCmpKind {
    /// All payload columns are non-nullable fixed-width ints ≤ 8 bytes (any sign).
    /// Vacuously true for zero-payload (all-PK) schemas.
    FixedIntNonnull,
    /// Any schema with a disqualifying payload column: nullable, float, string,
    /// blob, or U128/UUID. (Mixed signed/unsigned fixed ints stay in the fast
    /// path — only a non-fixed-int column falls back here.)
    Generic,
}

impl PayloadCmpKind {
    /// The comparator for a schema whose payload columns are `payload`.
    pub(super) fn of(mut payload: impl Iterator<Item = SchemaColumn>) -> Self {
        match payload.all(|c| !c.nullable && c.type_code.is_fixed_int()) {
            true => PayloadCmpKind::FixedIntNonnull,
            false => PayloadCmpKind::Generic,
        }
    }
}

/// Compare two rows in the full (PK, payload) order.
pub(crate) fn compare_full_rows<A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    super::key::compare_pk_bytes(src_a.get_pk_bytes(row_a), src_b.get_pk_bytes(row_b))
        .then_with(|| compare_rows(schema, src_a, row_a, src_b, row_b))
}

/// Compare two rows from any [`RowSource`] implementations by payload columns.
///
/// Null words are read once per row outside the column loop.
#[inline]
pub(crate) fn compare_rows<A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    let null_word_a = src_a.get_null_word(row_a);
    let null_word_b = src_b.get_null_word(row_b);
    // Both blob arenas are loop-invariant, and only the German-string arm of
    // `cmp_col_window` reads them, so they are hoisted out of the column loop.
    let (blob_a, blob_b) = (src_a.blob(), src_b.blob());

    for (payload_col, col) in schema.payload_columns() {
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

/// A payload row order. Generic over both sources, so one selected order serves
/// every source pairing a merge seat puts under it.
pub(crate) trait PayloadOrder: Copy {
    fn compare<A: RowSource, B: RowSource>(
        self,
        schema: &SchemaDescriptor,
        a: &A,
        ra: usize,
        b: &B,
        rb: usize,
    ) -> Ordering;
}

/// The order [`PayloadCmpKind::FixedIntNonnull`] selects.
#[derive(Clone, Copy)]
pub(crate) struct FixedIntNonnull;
/// The order [`PayloadCmpKind::Generic`] selects.
#[derive(Clone, Copy)]
pub(crate) struct Generic;

impl PayloadOrder for FixedIntNonnull {
    /// Each column read as an order-preserving u64: zero-extend, then flip the
    /// sign bit for signed columns. Same order as [`compare_rows`], with no
    /// null-bitmap read and no per-column type dispatch.
    #[inline]
    fn compare<A: RowSource, B: RowSource>(self, s: &SchemaDescriptor, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        debug_assert!(
            s.payload_cmp == PayloadCmpKind::FixedIntNonnull,
            "FixedIntNonnull on a non-fixedint or nullable schema",
        );
        for (payload_col, col) in s.payload_columns() {
            let cs = col.size() as usize;
            // `cs*8-1 ∈ {7,15,31,63}`: `FixedIntNonnull` admits only
            // `is_fixed_int` type codes, which are exactly the 1/2/4/8-byte
            // widths. `is_signed` is 0 for unsigned columns, so the XOR is then
            // a no-op; for signed ones it flips the MSB, putting two's-complement
            // negatives below non-negatives.
            let sign_flip = (col.is_signed() as u64) << (cs * 8 - 1);
            // `get_col_ptr` returns exactly `cs` bytes.
            let av = read_unsigned_exact(a.get_col_ptr(ra, payload_col, cs)) ^ sign_flip;
            let bv = read_unsigned_exact(b.get_col_ptr(rb, payload_col, cs)) ^ sign_flip;
            let ord = av.cmp(&bv);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

impl PayloadOrder for Generic {
    #[inline]
    fn compare<A: RowSource, B: RowSource>(self, s: &SchemaDescriptor, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        compare_rows(s, a, ra, b, rb)
    }
}

/// `f(args..., payload)` with the schema's [`PayloadOrder`] appended: one
/// monomorphization of `f` per order.
macro_rules! with_payload_cmp {
    ($schema:expr, $f:path $(, $arg:expr)* $(,)?) => {
        match $schema.payload_cmp {
            $crate::schema::payload_order::PayloadCmpKind::FixedIntNonnull => {
                $f($($arg,)* $crate::schema::payload_order::FixedIntNonnull)
            }
            $crate::schema::payload_order::PayloadCmpKind::Generic => {
                $f($($arg,)* $crate::schema::payload_order::Generic)
            }
        }
    };
}
pub(crate) use with_payload_cmp;

#[cfg(test)]
#[path = "tests/payload_order.rs"]
mod tests;
