//! Resolved column addressing: where a logical column physically lives in a row
//! and the reads through it, and the ranking of rows under ORDER BY keys.

use std::cmp::Ordering;

use gnitz_wire::{ScalarKind, TypeCode};

use crate::SchemaDescriptor;
use gnitz_wire::RowSource;

/// Where a logical column's value physically lives in a row, resolved once from
/// the schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnLocator {
    /// In the OPK-encoded PK region at `byte_off`; never NULL.
    Pk {
        byte_off: u8,
        size: u8,
        type_code: TypeCode,
    },
    /// Native little-endian in payload slot `slot`, which is also its null bit.
    Payload { slot: u8, size: u8, type_code: TypeCode },
}

const _: () = assert!(
    std::mem::size_of::<ColumnLocator>() <= 8,
    "ColumnLocator must stay within 8 bytes",
);
const _: () = assert!(
    gnitz_wire::MAX_PK_BYTES <= u8::MAX as usize,
    "ColumnLocator::Pk::byte_off is u8; the PK region no longer fits it",
);
const _: () = assert!(
    gnitz_wire::MAX_COLUMNS <= u8::MAX as usize,
    "ColumnLocator::Payload::slot is u8; the payload slot space no longer fits it",
);

impl ColumnLocator {
    #[inline(always)]
    pub fn size(&self) -> usize {
        match *self {
            ColumnLocator::Pk { size, .. } | ColumnLocator::Payload { size, .. } => size as usize,
        }
    }

    #[inline(always)]
    pub fn type_code(&self) -> TypeCode {
        match *self {
            ColumnLocator::Pk { type_code, .. } | ColumnLocator::Payload { type_code, .. } => type_code,
        }
    }

    /// The payload slot, which is also the null bit; `None` for a PK column.
    #[inline(always)]
    pub fn payload_slot(&self) -> Option<usize> {
        match *self {
            ColumnLocator::Pk { .. } => None,
            ColumnLocator::Payload { slot, .. } => Some(slot as usize),
        }
    }

    /// This column's bit in a null word; no bit for a PK column, which is never NULL.
    #[inline(always)]
    pub fn null_bit(&self) -> u64 {
        self.payload_slot().map_or(0, |slot| 1 << slot)
    }

    /// True iff this column is NULL in `row`.
    #[inline(always)]
    pub fn is_null(&self, mb: &impl RowSource, row: usize) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { .. } => self.is_null_word(mb.get_null_word(row)),
        }
    }

    /// [`Self::is_null`] against `row`'s already-read null word.
    #[inline(always)]
    pub fn is_null_word(&self, null_word: u64) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { slot, .. } => gnitz_wire::null_word_get(null_word, slot as usize),
        }
    }

    /// Raw at-rest bytes of the column in `row`: OPK for a PK column, native
    /// little-endian for a payload column, the 16-byte German-string cell for a
    /// STRING/BLOB.
    #[inline(always)]
    pub fn bytes<'b>(&self, mb: &'b impl RowSource, row: usize) -> &'b [u8] {
        match *self {
            ColumnLocator::Pk { byte_off, size, .. } => {
                let o = byte_off as usize;
                &mb.get_pk_bytes(row)[o..o + size as usize]
            }
            ColumnLocator::Payload { slot, size, .. } => mb.get_col_ptr(row, slot as usize, size as usize),
        }
    }

    /// A STRING/BLOB column's content in `row`, resolved through the source's
    /// heap.
    #[inline(always)]
    pub fn content<'b>(&self, mb: &'b impl RowSource, row: usize) -> &'b [u8] {
        gnitz_wire::german_string_content(self.bytes(mb, row), mb.blob())
    }

    /// Native little-endian value bytes of the column in `row`; a PK column is
    /// OPK-decoded into `scratch`.
    #[inline(always)]
    pub fn native_le_bytes<'a, 'b: 'a>(
        &self,
        mb: &'b impl RowSource,
        row: usize,
        scratch: &'a mut [u8; 16],
    ) -> &'a [u8] {
        match *self {
            ColumnLocator::Pk { size, type_code, .. } => {
                let dst = &mut scratch[..size as usize];
                gnitz_wire::decode_pk_cell(self.bytes(mb, row), type_code.is_signed_int(), dst);
                dst
            }
            ColumnLocator::Payload { .. } => self.bytes(mb, row),
        }
    }

    /// The ≤8-byte integer value in `row`, widened to `i64`. `fi` is the
    /// column's own type.
    #[inline(always)]
    pub fn decode_i64(&self, mb: &impl RowSource, row: usize, fi: gnitz_wire::FixedInt) -> i64 {
        debug_assert_eq!(gnitz_wire::FixedInt::from_type_code(self.type_code()), Some(fi));
        match *self {
            ColumnLocator::Pk { .. } => gnitz_wire::decode_opk_i64(self.bytes(mb, row), fi),
            ColumnLocator::Payload { .. } => fi.decode_le_i64(self.bytes(mb, row)),
        }
    }

    /// Order two rows on this column, **both known non-NULL**: a PK window by
    /// its OPK bytes, a payload window by typed value (STRING/BLOB by content).
    #[inline(always)]
    pub fn cmp_non_null<A: RowSource, B: RowSource>(&self, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        match *self {
            ColumnLocator::Pk { .. } => self.bytes(a, ra).cmp(self.bytes(b, rb)),
            ColumnLocator::Payload { type_code, .. } => {
                gnitz_wire::cmp_col_window(self.bytes(a, ra), a.blob(), self.bytes(b, rb), b.blob(), type_code)
            }
        }
    }

    /// The value in `row` as its OPK bytes read as a big-endian integer; a NULL
    /// cell encodes as its zero value. A STRING/BLOB yields its cell, not its
    /// content.
    #[inline(always)]
    pub fn opk_image(&self, mb: &impl RowSource, row: usize) -> u128 {
        let cell = self.bytes(mb, row);
        match *self {
            ColumnLocator::Pk { .. } => gnitz_wire::widen_pk_be(cell),
            ColumnLocator::Payload { size, type_code, .. } => {
                let signed = type_code.is_signed_int();
                if size == 16 {
                    u128::from_le_bytes(cell.try_into().unwrap()) ^ ((signed as u128) << 127)
                } else {
                    (gnitz_wire::read_unsigned_exact(cell) ^ ((signed as u64) << (size * 8 - 1))) as u128
                }
            }
        }
    }
}

/// One resolved ORDER BY key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderLocator {
    pub loc: ColumnLocator,
    pub desc: bool,
    /// NULLs sort first, whatever `desc` says.
    pub nulls_first: bool,
}

impl OrderLocator {
    /// The key `wire` names, over a column already located.
    #[inline]
    pub fn of(loc: ColumnLocator, wire: &gnitz_wire::OrderKey) -> Self {
        OrderLocator {
            loc,
            desc: wire.desc,
            nulls_first: wire.nulls_first,
        }
    }

    /// Whether the 8-byte image is the key's whole order.
    #[inline]
    fn image_is_whole(&self) -> bool {
        ScalarKind::from_type_code(self.loc.type_code()).is_some()
    }
}

/// The keys `order` names over `schema`, then under `total` every column ascending, NULLS
/// FIRST, PK columns leading: a total order over distinct rows. Panics on a column `schema`
/// does not have.
pub fn order_locators(order: &[gnitz_wire::OrderKey], schema: &SchemaDescriptor, total: bool) -> Vec<OrderLocator> {
    let mut keys: Vec<OrderLocator> = order
        .iter()
        .map(|k| OrderLocator::of(schema.locate(k.col as usize), k))
        .collect();
    if total {
        let asc = |loc| OrderLocator { loc, desc: false, nulls_first: true };
        keys.extend(schema.pk_cols().iter().map(|&c| asc(schema.locate(c as usize))));
        keys.extend(schema.payload_locators().into_iter().map(asc));
    }
    keys
}

/// Lexicographic over `keys`.
#[inline(always)]
pub fn cmp_order_keys<S: RowSource>(keys: &[OrderLocator], src: &S, ra: usize, rb: usize) -> Ordering {
    let na = src.get_null_word(ra);
    let nb = src.get_null_word(rb);
    for key in keys {
        let (xa, xb) = (key.loc.is_null_word(na), key.loc.is_null_word(nb));
        if xa != xb {
            return if xa == key.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        if xa {
            continue;
        }
        let mut ord = key.loc.cmp_non_null(src, ra, src, rb);
        if key.desc {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// The [`ScalarKind::order_image`] of the column's value in `row`. `kind` is the column's own.
#[inline(always)]
pub fn order_bits(loc: &ColumnLocator, src: &impl RowSource, row: usize, kind: ScalarKind) -> u64 {
    debug_assert_eq!(ScalarKind::from_type_code(loc.type_code()), Some(kind));
    match kind {
        ScalarKind::Int(fi) => kind.order_image(loc.decode_i64(src, row, fi) as u64),
        // Floats are never PK columns, so these bytes are native.
        ScalarKind::F32 => kind.order_image(u32::from_le_bytes(loc.bytes(src, row).try_into().unwrap()) as u64),
        ScalarKind::F64 => kind.order_image(u64::from_le_bytes(loc.bytes(src, row).try_into().unwrap())),
    }
}

/// A row with one of its order keys as integers.
#[derive(Clone, Copy)]
struct Ranked {
    image: u64,
    row: u32,
    /// Where a NULL key stands against a value, as [`cmp_order_keys`] places it.
    rank: u8,
}

/// A row's place under its leading order key alone.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lead(u8, u64);

impl Ranked {
    #[inline(always)]
    fn lead(&self) -> Lead {
        Lead(self.rank, self.image)
    }
}

/// `rows` of `src`, each with its image under `key`: an integer whose order never contradicts
/// the key's.
fn rank_rows<S: RowSource>(key: &OrderLocator, src: &S, rows: impl Iterator<Item = u32>, out: &mut Vec<Ranked>) {
    #[inline(always)]
    fn fill<S: RowSource>(
        key: &OrderLocator,
        src: &S,
        rows: impl Iterator<Item = u32>,
        out: &mut Vec<Ranked>,
        image: impl Fn(usize) -> u64,
    ) {
        let (loc, desc, nulls_first) = (key.loc, key.desc, key.nulls_first);
        out.extend(rows.map(|row| {
            let r = row as usize;
            if loc.is_null_word(src.get_null_word(r)) {
                return Ranked { image: 0, row, rank: !nulls_first as u8 };
            }
            let v = image(r);
            Ranked {
                image: if desc { !v } else { v },
                row,
                rank: nulls_first as u8,
            }
        }));
    }
    let loc = key.loc;
    let tc = loc.type_code();
    match ScalarKind::from_type_code(tc) {
        Some(ScalarKind::Int(fi)) => gnitz_wire::for_each_fixed_int!(fi, |FI| {
            fill(key, src, rows, out, |r| order_bits(&loc, src, r, ScalarKind::Int(FI)))
        }),
        Some(kind) => fill(key, src, rows, out, |r| order_bits(&loc, src, r, kind)),
        None if tc.is_german_string() => fill(key, src, rows, out, |r| {
            gnitz_wire::german_string_lead(loc.bytes(src, r), src.blob())
        }),
        // A 16-byte integer's high half.
        None => fill(key, src, rows, out, |r| (loc.opk_image(src, r) >> 64) as u64),
    }
}

/// Sort `run`, whose images are `keys[0]`'s, by `keys`; rows the keys do not tell apart keep
/// their order. The images sort first, and each run they leave tied is sorted by the rest.
fn sort_run<S: RowSource>(keys: &[OrderLocator], src: &S, run: &mut [Ranked]) {
    run.sort_by_key(Ranked::lead);
    let whole = keys[0].image_is_whole();
    if whole && keys.len() == 1 {
        return;
    }
    let mut scratch = Vec::new();
    for tie in run.chunk_by_mut(|a, b| a.lead() == b.lead()).filter(|t| t.len() > 1) {
        match whole {
            true => {
                scratch.clear();
                rank_rows(&keys[1], src, tie.iter().map(|r| r.row), &mut scratch);
                tie.copy_from_slice(&scratch);
                sort_run(&keys[1..], src, tie);
            }
            false => tie.sort_by(|a, b| cmp_order_keys(keys, src, a.row as usize, b.row as usize)),
        }
    }
}

/// The rows of `src` under order keys, each carrying its leading key's image, so that ranking
/// them reads the batch only for rows the image leaves tied.
pub struct RowRanking<'a, S> {
    keys: &'a [OrderLocator],
    src: &'a S,
    rows: Vec<Ranked>,
}

impl<'a, S: RowSource> RowRanking<'a, S> {
    /// Panics on no key.
    #[inline(never)]
    pub fn new(keys: &'a [OrderLocator], src: &'a S) -> Self {
        let n = src.row_count();
        assert!(n <= u32::MAX as usize, "row count exceeds u32");
        let mut rows = Vec::with_capacity(n);
        rank_rows(&keys[0], src, 0..n as u32, &mut rows);
        RowRanking { keys, src, rows }
    }

    /// Keep the `k` smallest rows, in no order; every row when `k` covers them.
    pub fn keep_smallest(&mut self, k: usize) {
        let RowRanking { keys, src, rows } = self;
        if k < rows.len() {
            if k > 0 {
                let tail = &keys[keys[0].image_is_whole() as usize..];
                rows.select_nth_unstable_by(k - 1, |a, b| match a.lead().cmp(&b.lead()) {
                    Ordering::Equal if !tail.is_empty() => cmp_tail(tail, *src, a.row, b.row),
                    ord => ord,
                });
            }
            rows.truncate(k);
        }
    }

    /// Drop every row whose lead is above `bound`.
    pub fn drop_above(&mut self, bound: Lead) {
        self.rows.retain(|r| r.lead() <= bound);
    }

    /// The largest lead among the rows.
    pub fn max_lead(&self) -> Option<Lead> {
        self.rows.iter().map(Ranked::lead).max()
    }

    /// The rows, in no particular order.
    pub fn rows(&self) -> impl ExactSizeIterator<Item = u32> + '_ {
        self.rows.iter().map(|r| r.row)
    }

    /// The rows ascending; rows the keys do not tell apart keep their order.
    pub fn sorted(mut self) -> Vec<u32> {
        sort_run(self.keys, self.src, &mut self.rows);
        self.rows().collect()
    }
}

/// Out of line: the images decide most comparisons, and this body inlined would crowd them.
#[inline(never)]
fn cmp_tail<S: RowSource>(tail: &[OrderLocator], src: &S, a: u32, b: u32) -> Ordering {
    cmp_order_keys(tail, src, a as usize, b as usize)
}

#[cfg(test)]
#[path = "tests/locator.rs"]
mod tests;
