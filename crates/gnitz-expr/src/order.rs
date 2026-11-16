//! The order of rows under ORDER BY keys, and the weight-slot cut of LIMIT/OFFSET.

use std::cmp::Ordering;

use gnitz_wire::{OrderKey, RowSource, ScalarKind};

use crate::{ColumnLocator, SchemaDescriptor};

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
        let wide = ScalarKind::from_type_code(tc).is_none() && !tc.is_german_string();
        let halves: &[u8] = if wide { &[64, 0] } else { &[0] };
        keys.extend(
            halves
                .iter()
                .map(|&shift| OrderLocator { loc, desc, nulls_first, shift }),
        );
    };
    for k in order {
        let (_, loc) = schema.wire_col("order key column", u32::from(k.col))?;
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

/// The rows [`rank_rows`] gives images: the first `n` of a source, appended to a vector, or
/// a run's, ranked in place.
enum Rows<'a> {
    First(u32, &'a mut Vec<Ranked>),
    Run(&'a mut [Ranked]),
}

/// `rows` of `src`, each with its image under `key`: an integer whose order never contradicts
/// the key's.
fn rank_rows<S: RowSource>(key: &OrderLocator, src: &S, rows: Rows<'_>) {
    #[inline(always)]
    fn fill<S: RowSource>(key: &OrderLocator, src: &S, rows: Rows<'_>, image: impl Fn(usize) -> u64) {
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
    match ScalarKind::from_type_code(tc) {
        Some(ScalarKind::Int(fi)) => gnitz_wire::for_each_fixed_int!(fi, |FI| {
            fill(key, src, rows, |r| order_bits(&loc, src, r, ScalarKind::Int(FI)))
        }),
        Some(kind) => fill(key, src, rows, |r| order_bits(&loc, src, r, kind)),
        None if tc.is_german_string() => fill(key, src, rows, |r| {
            gnitz_wire::german_string_lead(loc.german_cell(src, r), src.heap())
        }),
        // One half of a 16-byte integer.
        None => fill(key, src, rows, |r| (loc.opk_image(src, r) >> key.shift) as u64),
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

    /// The largest lead among the rows. A sort leaves a tie its later keys' images, so the
    /// leading key ranks the rows again first.
    pub fn max_lead(&mut self) -> Option<Lead> {
        rank_rows(&self.keys[0], self.src, Rows::Run(&mut self.rows));
        self.rows.iter().map(Ranked::lead).max()
    }

    /// The rows, in no particular order.
    pub fn rows(&self) -> impl ExactSizeIterator<Item = u32> {
        self.rows.iter().map(|r| r.row)
    }

    /// Order the rows ascending; rows the keys do not tell apart keep their order.
    pub fn sort(&mut self) {
        sort_run(self.keys, self.src, &mut self.rows);
    }

    /// Keep the first `n` rows.
    pub fn truncate(&mut self, n: usize) {
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
fn cmp_tail<S: RowSource>(tail: &[OrderLocator], src: &S, a: u32, b: u32) -> Ordering {
    cmp_order_keys(tail, src, a as usize, b as usize)
}

/// The weight slots of a window still to skip, then to keep: OFFSET and LIMIT
/// counted in logical rows, an element of weight `w` standing for `w` of them.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct WeightSlots {
    pub skip: u64,
    pub keep: u64,
}

impl WeightSlots {
    /// The whole input: no OFFSET, no LIMIT. A SQL LIMIT is at most `i64::MAX`, so a
    /// `keep` of `u64::MAX` names none.
    pub const ALL: WeightSlots = WeightSlots { skip: 0, keep: u64::MAX };

    /// The slot the window ends before; `None` when it keeps everything past `skip`.
    pub fn end(&self) -> Option<u64> {
        (self.keep != u64::MAX).then(|| self.skip.saturating_add(self.keep))
    }

    /// How many of the next element's `weight` slots are kept.
    #[inline]
    pub fn take(&mut self, weight: i64) -> i64 {
        let slots = weight.max(0) as u64;
        let skipped = slots.min(self.skip);
        self.skip -= skipped;
        let take = (slots - skipped).min(self.keep);
        self.keep -= take;
        take as i64
    }
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
