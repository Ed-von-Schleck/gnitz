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
}

impl OrderLocator {
    /// Whether the 8-byte image is the key's whole order.
    #[inline]
    fn image_is_whole(&self) -> bool {
        ScalarKind::from_type_code(self.loc.type_code()).is_some()
    }
}

/// The keys `order` names over `schema`, then under `total` every column ascending, NULLS
/// FIRST, PK columns leading: a total order over distinct rows. `Err` on a column `schema`
/// does not have.
pub fn order_locators(order: &[OrderKey], schema: &SchemaDescriptor, total: bool) -> Result<Vec<OrderLocator>, String> {
    let mut keys = order
        .iter()
        .map(|k| {
            let (_, loc) = schema.wire_col("order key column", u32::from(k.col))?;
            Ok(OrderLocator {
                loc,
                desc: k.desc,
                nulls_first: k.nulls_first,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if total {
        let asc = |loc| OrderLocator { loc, desc: false, nulls_first: true };
        keys.extend(schema.pk_cols().iter().map(|&c| asc(schema.locate(c as usize))));
        keys.extend(schema.payload_locators().into_iter().map(asc));
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
            gnitz_wire::german_string_lead(loc.bytes(src, r), src.heap())
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
