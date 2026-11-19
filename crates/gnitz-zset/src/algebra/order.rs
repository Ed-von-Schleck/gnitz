//! The order of rows under ORDER BY keys.

use std::cmp::Ordering;

use gnitz_wire::{OrderKey, RowSource, ScalarKind, ValueForm, WideForm};

use gnitz_expr::ColumnLocator;

use super::order_image::order_bits;
use crate::repr::MemBatch;
use crate::schema::{SchemaDescriptor, stable_radix_sort_by_key};

/// One resolved ORDER BY key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderLocator {
    loc: ColumnLocator,
    desc: bool,
    /// NULLs sort first, whatever `desc` says.
    nulls_first: bool,
    /// How far a 16-byte integer's image is shifted down: 64 for its high half, 0 for its
    /// low half, which [`order_locators`] lists right behind it.
    shift: u8,
}

impl OrderLocator {
    /// Whether the 8-byte image is the key's whole order.
    #[inline]
    fn image_is_whole(&self) -> bool {
        !self.loc.type_code().is_german_string()
    }
}

/// The keys `order` names over `schema`, then under `total` every column ascending, NULLS
/// FIRST, PK columns leading: a total order over distinct rows. A 16-byte integer is two
/// keys, its high half then its low half, so every key but a string's ranks whole on its
/// 8-byte image. `Err` on a column `schema` does not have.
pub fn order_locators(order: &[OrderKey], schema: &SchemaDescriptor, total: bool) -> Result<Vec<OrderLocator>, String> {
    let mut keys = Vec::with_capacity(order.len());
    let mut push = |loc: ColumnLocator, desc, nulls_first| {
        let tc = loc.type_code();
        let wide = tc.is_wide_int();
        let halves: &[u8] = if wide { &[64, 0] } else { &[0] };
        keys.extend(
            halves
                .iter()
                .map(|&shift| OrderLocator { loc, desc, nulls_first, shift }),
        );
    };
    for k in order {
        let (_, loc) = schema.wire_col("order key column", k.col)?;
        push(loc, k.desc, k.nulls_first);
    }
    if total {
        for &c in schema.pk_cols() {
            push(schema.locate(c as usize), false, true);
        }
        for loc in schema.payload_locators() {
            push(loc, false, true);
        }
    }
    Ok(keys)
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

/// A row with one of its order keys as integers.
#[derive(Clone, Copy, Default)]
struct Ranked {
    image: u64,
    row: u32,
    /// Where a NULL key stands against a value, as [`cmp_order_keys`] places it.
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
fn rank_rows(key: &OrderLocator, src: &MemBatch, rows: Rows<'_>) {
    #[inline(always)]
    fn fill(key: &OrderLocator, src: &MemBatch, rows: Rows<'_>, image: impl Fn(usize) -> u64) {
        let (loc, desc, nulls_first) = (key.loc, key.desc, key.nulls_first);
        let rank = |row: u32| {
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
        };
        match rows {
            Rows::First(n, out) => out.extend((0..n).map(rank)),
            Rows::Run(run) => run.iter_mut().for_each(|r| *r = rank(r.row)),
        }
    }
    let loc = key.loc;
    let tc = loc.type_code();
    match tc.form() {
        ValueForm::Scalar(ScalarKind::Int(fi)) => gnitz_wire::for_each_fixed_int!(fi, |FI| {
            fill(key, src, rows, |r| order_bits(&loc, src, r, ScalarKind::Int(FI)))
        }),
        ValueForm::Scalar(kind) => fill(key, src, rows, |r| order_bits(&loc, src, r, kind)),
        ValueForm::Wide(WideForm::German) => fill(key, src, rows, |r| {
            gnitz_wire::german_string_lead(loc.german_cell(src, r), src.heap())
        }),
        // One half of a 16-byte integer.
        ValueForm::Wide(WideForm::Int128 { .. }) => {
            fill(key, src, rows, |r| (loc.opk_image(src, r) >> key.shift) as u64)
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
fn sort_run(keys: &[OrderLocator], src: &MemBatch, run: &mut [Ranked]) {
    sort_by_lead(run);
    let whole = keys[0].image_is_whole();
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
    keys: &'a [OrderLocator],
    src: &'a MemBatch<'a>,
    rows: Vec<Ranked>,
}

impl<'a> RowRanking<'a> {
    /// Panics on no key.
    #[inline(never)]
    pub fn new(keys: &'a [OrderLocator], src: &'a MemBatch<'a>) -> Self {
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
                let tail = &keys[keys[0].image_is_whole() as usize..];
                rows.select_nth_unstable_by(k - 1, |a, b| match a.lead().cmp(&b.lead()) {
                    Ordering::Equal if !tail.is_empty() => cmp_tail(tail, src, a.row, b.row),
                    ord => ord,
                });
            }
            rows.truncate(k);
        }
    }

    /// Drop every row whose lead is above `bound`.
    pub(crate) fn drop_above(&mut self, bound: u128) {
        self.rows.retain(|r| r.lead() <= bound);
    }

    /// The largest lead among the rows. A sort leaves a tie its later keys' images, so the
    /// leading key ranks the rows again first.
    pub(crate) fn max_lead(&mut self) -> Option<u128> {
        rank_rows(&self.keys[0], self.src, Rows::Run(&mut self.rows));
        self.rows.iter().map(Ranked::lead).max()
    }

    /// The rows, in no particular order.
    pub(crate) fn rows(&self) -> impl ExactSizeIterator<Item = u32> {
        self.rows.iter().map(|r| r.row)
    }

    /// Order the rows ascending; rows the keys do not tell apart keep their order.
    pub(crate) fn sort(&mut self) {
        sort_run(self.keys, self.src, &mut self.rows);
    }

    /// Keep the first `n` rows.
    pub(crate) fn truncate(&mut self, n: usize) {
        self.rows.truncate(n);
    }

    /// The rows ascending; rows the keys do not tell apart keep their order.
    pub fn sorted(mut self) -> Vec<u32> {
        self.sort();
        self.rows().collect()
    }
}

/// Out of line: the images decide most comparisons, and this body inlined would crowd them.
#[inline(never)]
fn cmp_tail(tail: &[OrderLocator], src: &MemBatch, a: u32, b: u32) -> Ordering {
    cmp_order_keys(tail, src, a as usize, b as usize)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
