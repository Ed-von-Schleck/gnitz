//! The payload row order — the (PK, payload) total order's second term — and the
//! per-schema choice between its two comparators, cached on the descriptor at
//! construction and read through [`with_payload_cmp!`].

use std::cmp::Ordering;

use super::SchemaDescriptor;
use super::key::compare_pk_ordering;
use gnitz_wire::{RowSource, cmp_col_window, compare_german_strings, native_key_image, null_word_get};

/// A payload row order. Generic over both sources, so one selected order also
/// compares rows of two source types, as the read cursor does a run's row
/// against a batch's.
pub trait PayloadOrder: Copy {
    fn compare<A: RowSource, B: RowSource>(
        self,
        schema: &SchemaDescriptor,
        a: &A,
        ra: usize,
        b: &B,
        rb: usize,
    ) -> Ordering;

    /// Compare two rows in the full (PK, payload) order.
    fn compare_full<A: RowSource, B: RowSource>(
        self,
        schema: &SchemaDescriptor,
        a: &A,
        ra: usize,
        b: &B,
        rb: usize,
    ) -> Ordering {
        compare_pk_ordering(a.get_pk_bytes(ra), b.get_pk_bytes(rb)).then_with(|| self.compare(schema, a, ra, b, rb))
    }
}

/// The order of a schema whose [`SchemaDescriptor::payload_is_fixed_int_nonnull`].
#[derive(Clone, Copy)]
pub struct FixedIntNonnull;
/// The order of every other schema: each column by its type's order, NULL below
/// every value.
#[derive(Clone, Copy)]
pub struct Generic;

impl PayloadOrder for FixedIntNonnull {
    /// Each column by its cells' key images: the same order as [`Generic`],
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
    /// Null words are read once per row outside the column loop. Each cell's
    /// dispatch is written out here rather than taken from
    /// `ColumnLocator::cmp_non_null`, which retires more instructions per row on
    /// this, the merge's comparator.
    #[inline]
    fn compare<A: RowSource, B: RowSource>(self, s: &SchemaDescriptor, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        let null_word_a = a.get_null_word(ra);
        let null_word_b = b.get_null_word(rb);
        for (slot, col) in s.payload_columns() {
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
            let (ca, cb) = (a.get_col_ptr(ra, slot, cs), b.get_col_ptr(rb, slot, cs));
            let ord = match col.type_code.is_german_string() {
                true => compare_german_strings(ca.try_into().unwrap(), a.heap(), cb.try_into().unwrap(), b.heap()),
                false => cmp_col_window(ca, cb, col.type_code),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

/// `$body` with `$p` bound to the schema's [`PayloadOrder`]: one copy of `$body`
/// per order.
#[macro_export]
#[doc(hidden)]
macro_rules! with_payload_cmp {
    ($schema:expr, |$p:ident| $body:expr $(,)?) => {
        if $schema.payload_is_fixed_int_nonnull() {
            let $p = $crate::schema::FixedIntNonnull;
            $body
        } else {
            let $p = $crate::schema::Generic;
            $body
        }
    };
}
pub use crate::with_payload_cmp;

#[cfg(test)]
#[path = "tests/payload_order.rs"]
mod tests;
