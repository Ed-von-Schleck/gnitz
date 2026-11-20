//! The order of rows under ORDER BY keys: a key resolved against a schema, the image an
//! ordered index keys a row by under it, and the ranking of a batch's rows in memory.

use std::cmp::Ordering;

use gnitz_wire::{OrderKey, RowSource, TypeCode, ValueForm, WideForm};

use super::order_image::{ImageCol, image_slot_col, scalar_images};
use crate::repr::MemBatch;
use crate::schema::{SchemaColumn, SchemaDescriptor, stable_radix_sort_by_key};

/// The rank byte leading a nullable key's image in an index.
const RANK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);

/// One resolved ORDER BY key: the column's order image, complemented for DESC, and where a
/// NULL, which has no image, stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderCol {
    pub image: ImageCol,
    /// NULLs sort first, whatever the direction.
    nulls_first: bool,
    /// Whether the column can hold a NULL; only then does an index image carry a rank byte.
    nullable: bool,
    /// The low half of a 16-byte integer, which [`order_cols`] lists right behind the key
    /// itself: a ranking's 8-byte lead is this key's image's low half, and every other
    /// 16-byte integer key's high half.
    low_half: bool,
}

impl OrderCol {
    /// `key` over `schema`. `Err` on a column `schema` does not have.
    pub fn resolve(schema: &SchemaDescriptor, key: &OrderKey) -> Result<Self, String> {
        let (col, loc) = schema.wire_col("order key column", key.col)?;
        Ok(OrderCol {
            image: ImageCol::new(loc, key.desc),
            nulls_first: key.nulls_first,
            nullable: col.nullable,
            low_half: false,
        })
    }

    #[inline]
    fn is_null(&self, src: &impl RowSource, row: usize) -> bool {
        self.nullable && self.image.loc.is_null(src, row)
    }

    /// Where a row stands under the key on being NULL alone, 0 before 1. The direction
    /// does not flip it.
    #[inline(always)]
    pub(crate) fn rank(&self, is_null: bool) -> u8 {
        (is_null != self.nulls_first) as u8
    }

    /// The PK columns [`Self::write_lead`] fills: the rank byte of a nullable key, then
    /// the image slot.
    pub fn lead_cols(&self) -> impl Iterator<Item = SchemaColumn> {
        let slot = image_slot_col(self.image.is_wide());
        self.nullable.then_some(RANK_COL).into_iter().chain([slot])
    }

    /// Append the key's image for `row`: the rank byte of a nullable key, then, non-NULL
    /// only, the order image.
    #[inline]
    pub fn append_image(&self, src: &impl RowSource, row: usize, out: &mut Vec<u8>) {
        let is_null = self.is_null(src, row);
        if self.nullable {
            out.push(self.rank(is_null));
        }
        if !is_null {
            self.image.append(src, row, out);
        }
    }

    /// Write the key's lead for `row` into `lead`, [`Self::lead_cols`] wide: the rank byte
    /// of a nullable key, then the image, zero-padded or cut to its slot. A string's whole
    /// image is appended to `image`.
    #[inline]
    pub fn write_lead(&self, src: &impl RowSource, row: usize, lead: &mut [u8], image: &mut Vec<u8>) {
        let is_null = self.is_null(src, row);
        let slot = match self.nullable {
            true => {
                lead[0] = self.rank(is_null);
                &mut lead[1..]
            }
            false => lead,
        };
        match is_null {
            true => slot.fill(0),
            false => self.image.write_slot(src, row, slot, image),
        }
    }

    /// [`Self::write_lead`] for every row of `mb` of a scalar key, a column at a time: the
    /// `n`-th row's into the window at `at` of the `n`-th `stride`-byte key of `keys`.
    pub fn write_leads(&self, mb: &MemBatch<'_>, keys: &mut [u8], stride: usize, at: usize) {
        let rows = [(0, mb.row_count())];
        let scalar = self
            .image
            .write_slots(mb, &rows, keys, stride, at + self.nullable as usize, 8);
        debug_assert!(scalar, "a wide image is written a row at a time");
        self.stamp_ranks(mb, keys, stride, at, |_, _| {});
    }

    /// Every row's image of a scalar key into `cells`, one 16-byte inline string cell per
    /// row, as `encode_german_string` writes the image [`Self::append_image`] appends: at
    /// most 9 bytes, so always inline.
    pub fn write_inline_images(&self, mb: &MemBatch<'_>, cells: &mut [u8]) {
        const OFF: usize = gnitz_wire::GERMAN_INLINE_OFF;
        cells.fill(0);
        let scalar = self
            .image
            .write_slots(mb, &[(0, cells.len() / 16)], cells, 16, OFF + self.nullable as usize, 8);
        debug_assert!(scalar, "a wide image is written a row at a time");
        let len = (8 + self.nullable as u32).to_le_bytes();
        for cell in cells.as_chunks_mut::<16>().0 {
            cell[..4].copy_from_slice(&len);
        }
        self.stamp_ranks(mb, cells, 16, OFF, |cell, is_null| {
            if is_null {
                cell[..4].copy_from_slice(&1u32.to_le_bytes());
            }
        });
    }

    /// Over `keys`, whose `n`-th `stride`-byte key holds the `n`-th row's scalar image at
    /// `at + 1` as if the row were non-NULL: each row's rank byte at `at`, and a NULL row's
    /// image zeroed, as [`Self::write_lead`] leaves it. `stamp` then gets each key and
    /// whether its row is NULL. A key without a rank byte is left as it is.
    #[inline]
    fn stamp_ranks(
        &self,
        mb: &MemBatch<'_>,
        keys: &mut [u8],
        stride: usize,
        at: usize,
        mut stamp: impl FnMut(&mut [u8], bool),
    ) {
        if !self.nullable {
            return;
        }
        let bit = self.image.loc.null_bit();
        let words = mb.null_bmp().as_chunks::<8>().0;
        for (key, word) in keys.chunks_exact_mut(stride).zip(words) {
            let is_null = u64::from_le_bytes(*word) & bit != 0;
            key[at] = self.rank(is_null);
            if is_null {
                key[at + 1..at + 9].fill(0);
            }
            stamp(key, is_null);
        }
    }
}

/// The keys `order` names over `schema`, then under `total` every column ascending, NULLS
/// FIRST, PK columns leading: a total order over distinct rows. A 16-byte integer is two
/// keys, itself then its low half, so every key but a string's ranks whole on its 8-byte
/// lead. `Err` on a column `schema` does not have.
pub fn order_cols(order: &[OrderKey], schema: &SchemaDescriptor, total: bool) -> Result<Vec<OrderCol>, String> {
    let mut keys = Vec::with_capacity(order.len());
    let mut push = |key: OrderCol| {
        keys.push(key);
        if key.image.loc.type_code().is_wide_int() {
            keys.push(OrderCol { low_half: true, ..key });
        }
    };
    for k in order {
        push(OrderCol::resolve(schema, k)?);
    }
    if total {
        let payload = (0..schema.num_columns() as u32).filter(|&c| !schema.is_pk_col(c as usize));
        for c in schema.pk_cols().iter().copied().chain(payload) {
            let key = OrderKey { col: c, desc: false, nulls_first: true };
            push(OrderCol::resolve(schema, &key)?);
        }
    }
    Ok(keys)
}

/// Lexicographic over `keys`.
#[inline(always)]
pub fn cmp_order_keys<S: RowSource>(keys: &[OrderCol], src: &S, ra: usize, rb: usize) -> Ordering {
    let na = src.get_null_word(ra);
    let nb = src.get_null_word(rb);
    for key in keys {
        let loc = &key.image.loc;
        let (xa, xb) = (loc.is_null_word(na), loc.is_null_word(nb));
        if xa != xb {
            // One is NULL: rank 0 against rank 1.
            return match key.rank(xa) {
                0 => Ordering::Less,
                _ => Ordering::Greater,
            };
        }
        if xa {
            continue;
        }
        let mut ord = loc.cmp_non_null(src, ra, src, rb);
        if key.image.invert {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// A row with one of its order keys as integers.
#[derive(Clone, Copy, Default)]
struct Ranked {
    image: u64,
    row: u32,
    /// The key's [`OrderCol::rank`] of the row.
    rank: u8,
}

impl Ranked {
    /// The row's place under its leading order key alone: the rank above the image, one
    /// integer, which compares in fewer instructions than the pair.
    #[inline(always)]
    fn lead(&self) -> u128 {
        (self.rank as u128) << 64 | self.image as u128
    }
}

/// The rows [`rank_rows`] gives images: the first `n` of a source, appended to a vector, or
/// a run's, ranked in place.
enum Rows<'a> {
    First(u32, &'a mut Vec<Ranked>),
    Run(&'a mut [Ranked]),
}

/// `rows` of `src`, each with its image under `key`: an integer whose order never contradicts
/// the key's.
fn rank_rows(key: &OrderCol, src: &MemBatch, rows: Rows<'_>) {
    #[inline(always)]
    fn fill(key: &OrderCol, src: &MemBatch, rows: Rows<'_>, image: impl Fn(usize) -> u64) {
        let (loc, desc) = (key.image.loc, key.image.invert);
        let (null_rank, value_rank) = (key.rank(true), key.rank(false));
        let rank = move |row: u32| {
            let r = row as usize;
            if loc.is_null_word(src.get_null_word(r)) {
                return Ranked { image: 0, row, rank: null_rank };
            }
            let v = image(r);
            Ranked {
                image: if desc { !v } else { v },
                row,
                rank: value_rank,
            }
        };
        match rows {
            Rows::First(n, out) => out.extend((0..n).map(rank)),
            Rows::Run(run) => run.iter_mut().for_each(|r| *r = rank(r.row)),
        }
    }
    let loc = key.image.loc;
    match key.image.kind {
        ValueForm::Scalar(kind) => scalar_images!(loc, kind, src, |cells, cs, image| {
            fill(key, src, rows, |r| image(&cells[r * cs..]))
        }),
        ValueForm::Wide(WideForm::German) => fill(key, src, rows, |r| {
            gnitz_wire::german_string_lead(loc.german_cell(src, r), src.heap())
        }),
        // One half of a 16-byte integer.
        ValueForm::Wide(WideForm::Int128 { .. }) => {
            let shift = if key.low_half { 0 } else { 64 };
            fill(key, src, rows, |r| (loc.opk_image(src, r) >> shift) as u64)
        }
    }
}

/// Sort `run` by lead, stably. Several workers' replies concatenated stand in a few
/// ascending runs, which a merge sort joins in a pass per halving of their number: fewer
/// passes than the radix sort makes while the runs are few.
fn sort_by_lead(run: &mut [Ranked]) {
    const FEW_RUNS: usize = 8;
    match run.windows(2).filter(|w| w[0].lead() > w[1].lead()).count() {
        0 => {}
        // Every pair strictly descending: reversed, no tie changes its order.
        descents if descents == run.len() - 1 => run.reverse(),
        descents if descents < FEW_RUNS => run.sort_by_key(Ranked::lead),
        _ => stable_radix_sort_by_key(run, Ranked::lead),
    }
}

/// Sort `run`, whose images are `keys[0]`'s, by `keys`; rows the keys do not tell apart keep
/// their order. The images sort first, and each run they leave tied is sorted by the rest.
fn sort_run(keys: &[OrderCol], src: &MemBatch, run: &mut [Ranked]) {
    sort_by_lead(run);
    // Whether the 8-byte lead is the key's whole order.
    let whole = keys[0].image.fits_slot();
    if whole && keys.len() == 1 {
        return;
    }
    for tie in run.chunk_by_mut(|a, b| a.lead() == b.lead()).filter(|t| t.len() > 1) {
        match whole {
            true => {
                rank_rows(&keys[1], src, Rows::Run(tie));
                sort_run(&keys[1..], src, tie);
            }
            false => tie.sort_by(|a, b| cmp_order_keys(keys, src, a.row as usize, b.row as usize)),
        }
    }
}

/// The rows of `src` under order keys, each carrying its leading key's image, so that ranking
/// them reads the batch only for rows the image leaves tied.
pub struct RowRanking<'a> {
    keys: &'a [OrderCol],
    src: &'a MemBatch<'a>,
    rows: Vec<Ranked>,
}

impl<'a> RowRanking<'a> {
    /// Panics on no key.
    #[inline(never)]
    pub fn new(keys: &'a [OrderCol], src: &'a MemBatch<'a>) -> Self {
        let n = src.row_count();
        assert!(n <= u32::MAX as usize, "row count exceeds u32");
        let mut rows = Vec::with_capacity(n);
        rank_rows(&keys[0], src, Rows::First(n as u32, &mut rows));
        RowRanking { keys, src, rows }
    }

    /// Keep the `k` smallest rows, in no order; every row when `k` covers them.
    pub fn keep_smallest(&mut self, k: usize) {
        let RowRanking { keys, src, rows } = self;
        if k < rows.len() {
            if k > 0 {
                let tail = &keys[keys[0].image.fits_slot() as usize..];
                rows.select_nth_unstable_by(k - 1, |a, b| match a.lead().cmp(&b.lead()) {
                    Ordering::Equal if !tail.is_empty() => cmp_tail(tail, src, a.row, b.row),
                    ord => ord,
                });
            }
            rows.truncate(k);
        }
    }

    /// The smallest rows whose summed `weight` covers `window`, in no particular order,
    /// among those whose lead is at most `bound`; every such row when they fall short of
    /// it. The boundary row stays whole. Every row weighs at least 1.
    pub(crate) fn keep_covering(mut self, window: i64, bound: Option<u128>, weight: impl Fn(u32) -> i64) -> Covering {
        if let Some(bound) = bound {
            self.rows.retain(|r| r.lead() <= bound);
        }
        self.keep_smallest(window as usize);
        let mut summed = self.rows().fold(0i64, |a, r| a.saturating_add(weight(r)));
        // Every row weighs ≥ 1, so the kept rows cover the window; a surplus is the only way a
        // comparator-larger row among them is droppable.
        if summed > window {
            self.sort();
            summed = 0;
            let cut = self.rows().position(|r| {
                summed = summed.saturating_add(weight(r));
                summed >= window
            });
            if let Some(i) = cut {
                self.rows.truncate(i + 1);
            }
        }
        let bound = self.max_lead();
        Covering {
            rows: self.rows().collect(),
            summed,
            bound,
        }
    }

    /// The largest lead among the rows. A sort leaves a tie its later keys' images, so the
    /// leading key ranks the rows again first.
    fn max_lead(&mut self) -> Option<u128> {
        rank_rows(&self.keys[0], self.src, Rows::Run(&mut self.rows));
        self.rows.iter().map(Ranked::lead).max()
    }

    /// The rows, in no particular order.
    fn rows(&self) -> impl ExactSizeIterator<Item = u32> {
        self.rows.iter().map(|r| r.row)
    }

    /// Order the rows ascending; rows the keys do not tell apart keep their order.
    fn sort(&mut self) {
        sort_run(self.keys, self.src, &mut self.rows);
    }

    /// The rows ascending; rows the keys do not tell apart keep their order.
    pub fn sorted(mut self) -> Vec<u32> {
        self.sort();
        self.rows().collect()
    }
}

/// What [`RowRanking::keep_covering`] keeps.
pub(crate) struct Covering {
    /// The kept rows, in no particular order.
    pub(crate) rows: Vec<u32>,
    /// Their summed weight.
    pub(crate) summed: i64,
    /// The largest lead among them.
    pub(crate) bound: Option<u128>,
}

/// Out of line: the images decide most comparisons, and this body inlined would crowd them.
#[inline(never)]
fn cmp_tail(tail: &[OrderCol], src: &MemBatch, a: u32, b: u32) -> Ordering {
    cmp_order_keys(tail, src, a as usize, b as usize)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
