//! The payload row order — the (PK, payload) total order's second term — and the
//! per-schema choice between its two comparators, cached on the descriptor at
//! construction and read through [`with_payload_cmp!`].

use std::cmp::Ordering;

use super::key::compare_pk_ordering;
use super::SchemaDescriptor;
use gnitz_wire::{cmp_col_window, compare_german_strings, native_key_image, null_word_get, RowSource};

/// Compare two rows in the full (PK, payload) order.
pub(crate) fn compare_full_rows<A: RowSource, B: RowSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    compare_pk_ordering(src_a.get_pk_bytes(row_a), src_b.get_pk_bytes(row_b))
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

    for (slot, col) in schema.payload_columns() {
        let null_a = null_word_get(null_word_a, slot);
        let null_b = null_word_get(null_word_b, slot);
        if null_a && null_b {
            continue;
        }
        if null_a {
            return Ordering::Less;
        }
        if null_b {
            return Ordering::Greater;
        }

        let cs = col.size();
        let (a, b) = (src_a.get_col_ptr(row_a, slot, cs), src_b.get_col_ptr(row_b, slot, cs));
        let ord = match col.type_code.is_german_string() {
            true => compare_german_strings(a.try_into().unwrap(), src_a.heap(), b.try_into().unwrap(), src_b.heap()),
            false => cmp_col_window(a, b, col.type_code),
        };

        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

/// A payload row order. Generic over both sources, so one selected order serves
/// every source pairing a merge seat puts under it.
pub(crate) trait PayloadOrder: Copy {
    /// Whether the order is [`FixedIntNonnull`]'s: each column's cells read as
    /// an order-preserving integer.
    const FIXED_INT: bool;

    fn compare<A: RowSource, B: RowSource>(
        self,
        schema: &SchemaDescriptor,
        a: &A,
        ra: usize,
        b: &B,
        rb: usize,
    ) -> Ordering;
}

/// The order of a schema whose [`SchemaDescriptor::payload_is_fixed_int_nonnull`].
#[derive(Clone, Copy)]
pub(crate) struct FixedIntNonnull;
/// The order of every other schema.
#[derive(Clone, Copy)]
pub(crate) struct Generic;

impl PayloadOrder for FixedIntNonnull {
    const FIXED_INT: bool = true;

    /// Each column by its cells' key images: the same order as [`compare_rows`],
    /// with no null-bitmap read and no per-type dispatch, one width dispatch
    /// serving both cells.
    #[inline]
    fn compare<A: RowSource, B: RowSource>(self, s: &SchemaDescriptor, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        debug_assert!(
            s.payload_is_fixed_int_nonnull(),
            "FixedIntNonnull on a non-fixedint or nullable schema",
        );
        for (slot, col) in s.payload_columns() {
            let cs = col.size();
            let (ca, cb) = (a.get_col_ptr(ra, slot, cs), b.get_col_ptr(rb, slot, cs));
            // A fixed-int image fits 64 bits, which compare cheaper than 128.
            macro_rules! cmp {
                ($w:literal) => {{
                    let image = |c: &[u8]| native_key_image::<$w>(c.try_into().unwrap(), col.is_signed()) as u64;
                    image(ca).cmp(&image(cb))
                }};
            }
            let ord = match cs {
                1 => cmp!(1),
                2 => cmp!(2),
                4 => cmp!(4),
                8 => cmp!(8),
                w => unreachable!("a fixed-int column of {w} bytes"),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

impl PayloadOrder for Generic {
    const FIXED_INT: bool = false;

    #[inline]
    fn compare<A: RowSource, B: RowSource>(self, s: &SchemaDescriptor, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        compare_rows(s, a, ra, b, rb)
    }
}

/// `$body` with `$p` bound to the schema's [`PayloadOrder`]: one copy of `$body`
/// per order.
macro_rules! with_payload_cmp {
    ($schema:expr, |$p:ident| $body:expr $(,)?) => {
        if $schema.payload_is_fixed_int_nonnull() {
            let $p = $crate::schema::payload_order::FixedIntNonnull;
            $body
        } else {
            let $p = $crate::schema::payload_order::Generic;
            $body
        }
    };
}
pub(crate) use with_payload_cmp;

#[cfg(test)]
#[path = "tests/payload_order.rs"]
mod tests;
