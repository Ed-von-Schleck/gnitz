//! An aggregate, and the running values of a list of them.

use std::cmp::Ordering;
use std::ops::Range;
use std::rc::Rc;

use crate::repr::{Batch, MemBatch};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{for_each_fixed_int, int_cell, AggFunc, FixedInt, ScalarKind};
use gnitz_wire::{RowSource, TypeCode};

use crate::algebra::order_image::{
    scalar_image, scalar_native_of_image, wide_native, wide_native_of_image, ImageCol, ImageKind, WideKind,
};

/// One aggregate: the column it reads, the output column it fills, and what a
/// row does to a group's value. The values live in an [`AggValues`].
#[derive(Clone, Copy)]
pub(crate) struct Agg {
    /// The aggregated column in an input row.
    src: ColumnLocator,
    /// This aggregate's column in an output row.
    out: ColumnLocator,
    /// What an input row's `src` does to a group's value.
    kind: StepKind,
    /// What an output row's `out` does to a group's value.
    merge: StepKind,
}

#[derive(Clone, Copy)]
enum StepKind {
    Count,
    CountNonNull,
    /// A float sum holds `f64` bits whatever the source width.
    Sum(ScalarKind),
    /// A scalar extreme is held as the value index holds it, complemented for
    /// MAX, so the smaller image always wins.
    Extreme {
        max: bool,
        kind: ImageKind,
    },
}

impl StepKind {
    /// `None` iff `op` sums and `tc` has no scalar register image.
    fn of(op: AggFunc, tc: TypeCode) -> Option<Self> {
        Some(match op {
            AggFunc::Count => StepKind::Count,
            AggFunc::CountNonNull => StepKind::CountNonNull,
            AggFunc::Sum => StepKind::Sum(ScalarKind::from_type_code(tc)?),
            AggFunc::Min | AggFunc::Max => StepKind::Extreme {
                max: op == AggFunc::Max,
                kind: ImageKind::of(tc),
            },
        })
    }
}

/// The running values of an aggregate list, one per aggregate per group, each
/// aggregate's side by side.
pub(crate) struct AggValues {
    /// A count, a sum, or a scalar extreme's image.
    acc: Vec<i64>,
    /// Whether an extreme holds a value. Empty for a list without one.
    has: Vec<bool>,
    /// Where in `heap` a wide extreme's native bytes start, their length, and
    /// the room they have. Empty for a list without one.
    wide: Vec<(usize, u32, u32)>,
    /// A value is overwritten in place while it fits its room, and moves to the
    /// end, into twice the room, when it does not.
    heap: Vec<u8>,
    /// The list, shared with its plan: aggregate `k` owns value column `k`.
    aggs: Rc<[Agg]>,
    /// Whether an aggregate of the list is an extreme, a wide one.
    extremes: bool,
    wides: bool,
    groups: usize,
    /// Groups each aggregate has room for: aggregate `k`'s values start at
    /// `k * cap`.
    cap: usize,
}

impl AggValues {
    /// `groups` empty groups of `aggs`.
    pub(crate) fn new(aggs: &Rc<[Agg]>, groups: usize) -> Self {
        let (extremes, wides) = (aggs.iter().any(|a| !a.is_linear()), aggs.iter().any(Agg::is_wide));
        let n = aggs.len() * groups;
        AggValues {
            acc: vec![0; n],
            has: vec![false; if extremes { n } else { 0 }],
            wide: vec![(0, 0, 0); if wides { n } else { 0 }],
            heap: Vec::new(),
            aggs: Rc::clone(aggs),
            extremes,
            wides,
            groups,
            cap: groups,
        }
    }

    /// Grow to `groups` groups, each new one empty.
    pub(crate) fn resize(&mut self, groups: usize) {
        if groups > self.cap {
            // Each aggregate's values move to where its wider column starts.
            fn regrow<T: Copy>(v: &mut Vec<T>, aggs: usize, old: usize, cap: usize, empty: T) {
                let mut grown = vec![empty; aggs * cap];
                for k in 0..aggs {
                    grown[k * cap..k * cap + old].copy_from_slice(&v[k * old..(k + 1) * old]);
                }
                *v = grown;
            }
            let (aggs, old) = (self.aggs.len(), self.cap);
            let cap = groups.max(2 * old);
            regrow(&mut self.acc, aggs, old, cap, 0);
            if self.extremes {
                regrow(&mut self.has, aggs, old, cap, false);
            }
            if self.wides {
                regrow(&mut self.wide, aggs, old, cap, (0, 0, 0));
            }
            self.cap = cap;
        }
        self.groups = groups;
    }

    /// Where aggregate `k`'s value for group `g` lies.
    #[inline(always)]
    fn at(&self, k: usize, g: usize) -> usize {
        debug_assert!(g < self.groups);
        k * self.cap + g
    }

    /// Aggregate `k`'s values, one per group, for a column kernel.
    #[inline(always)]
    fn column(&mut self, k: usize) -> Slots<'_> {
        let rows = k * self.cap..k * self.cap + self.groups;
        Slots {
            acc: &mut self.acc[rows.clone()],
            has: self.has.get_mut(rows).unwrap_or_default(),
        }
    }

    /// Aggregate `k`'s value for group `g` alone, as group 0.
    #[inline(always)]
    fn slot(&mut self, k: usize, g: usize) -> Slots<'_> {
        let at = self.at(k, g);
        Slots {
            acc: std::slice::from_mut(&mut self.acc[at]),
            has: self.has.get_mut(at..=at).unwrap_or_default(),
        }
    }

    #[inline(always)]
    fn wide(&self, at: usize) -> &[u8] {
        let (start, len, _) = self.wide[at];
        &self.heap[start..start + len as usize]
    }

    #[inline(always)]
    fn store_wide(&mut self, at: usize, v: &[u8]) {
        let (start, _, room) = self.wide[at];
        let len = v.len() as u32;
        if len <= room {
            self.heap[start..start + v.len()].copy_from_slice(v);
            self.wide[at].1 = len;
        } else {
            let room = len.max(room.saturating_mul(2));
            let start = self.heap.len();
            self.heap.extend_from_slice(v);
            self.heap.resize(start + room as usize, 0);
            self.wide[at] = (start, len, room);
        }
        self.has[at] = true;
    }
}

/// One aggregate's scalar values, one per group, borrowed for a kernel.
pub(crate) struct Slots<'a> {
    acc: &'a mut [i64],
    has: &'a mut [bool],
}

/// A group's running value as a kernel carries it, and what a row's
/// contribution does to it.
pub(crate) trait Reg: Copy {
    /// A row's contribution.
    type In;
    fn load(vals: &Slots, g: usize) -> Self;
    fn store(self, vals: &mut Slots, g: usize);
    /// The value after `x`; `None` where `x` leaves it as it is.
    fn absorb(self, x: Self::In) -> Option<Self>;

    /// Group `g` after `x`.
    #[inline(always)]
    fn step(vals: &mut Slots, g: usize, x: Self::In) {
        if let Some(r) = Self::load(vals, g).absorb(x) {
            r.store(vals, g);
        }
    }
}

/// A count or an integer sum, wrapping.
#[derive(Clone, Copy)]
struct IntSum(i64);

impl Reg for IntSum {
    type In = i64;
    #[inline(always)]
    fn load(vals: &Slots, g: usize) -> Self {
        IntSum(vals.acc[g])
    }
    #[inline(always)]
    fn store(self, vals: &mut Slots, g: usize) {
        vals.acc[g] = self.0;
    }
    #[inline(always)]
    fn absorb(self, x: i64) -> Option<Self> {
        Some(IntSum(self.0.wrapping_add(x)))
    }
}

/// A float sum: `f64` bits whatever the source width.
#[derive(Clone, Copy)]
struct FloatSum(f64);

impl Reg for FloatSum {
    type In = f64;
    #[inline(always)]
    fn load(vals: &Slots, g: usize) -> Self {
        FloatSum(f64::from_bits(vals.acc[g] as u64))
    }
    #[inline(always)]
    fn store(self, vals: &mut Slots, g: usize) {
        vals.acc[g] = self.0.to_bits() as i64;
    }
    #[inline(always)]
    fn absorb(self, x: f64) -> Option<Self> {
        Some(FloatSum(self.0 + x))
    }
}

/// A scalar extreme: the least image so far, and whether there is one.
#[derive(Clone, Copy)]
struct Least(u64, bool);

impl Reg for Least {
    type In = u64;
    #[inline(always)]
    fn load(vals: &Slots, g: usize) -> Self {
        Least(vals.acc[g] as u64, vals.has[g])
    }
    #[inline(always)]
    fn store(self, vals: &mut Slots, g: usize) {
        (vals.acc[g], vals.has[g]) = (self.0 as i64, self.1);
    }
    #[inline(always)]
    fn absorb(self, image: u64) -> Option<Self> {
        (!self.1 || image < self.0).then_some(Least(image, true))
    }
}

/// Which group each row of a fold belongs to.
pub(crate) trait Groups: Copy {
    /// The group of the fold's `i`-th row, which lies in its `range`-th range.
    fn of(self, range: usize, i: usize) -> usize;

    /// Step the group of each of `rows` — the fold's rows `at`, its `range`-th
    /// range — by the row's `contribution`: here, where they are all one group.
    #[inline(always)]
    fn fold<R: Reg, V>(
        self,
        vals: &mut Slots,
        range: usize,
        at: Range<usize>,
        rows: impl Iterator<Item = V>,
        contribution: impl Fn(V) -> Option<R::In>,
    ) {
        let g = self.of(range, at.start);
        rows.fold(R::load(vals, g), |r, v| {
            contribution(v).and_then(|x| r.absorb(x)).unwrap_or(r)
        })
        .store(vals, g);
    }
}

/// The rows of a fold's `i`-th range in one group: group `i` under
/// [`Self::EACH`], group 0 under [`Self::FIRST`].
#[derive(Clone, Copy)]
pub(crate) struct RangeGroups(usize);

impl RangeGroups {
    pub(crate) const EACH: Self = RangeGroups(usize::MAX);
    pub(crate) const FIRST: Self = RangeGroups(0);
}

impl Groups for RangeGroups {
    #[inline(always)]
    fn of(self, range: usize, _: usize) -> usize {
        range & self.0
    }
}

/// A fold's `i`-th row in group `self[i]`.
impl Groups for &[u32] {
    #[inline(always)]
    fn of(self, _: usize, i: usize) -> usize {
        self[i] as usize
    }

    #[inline(always)]
    fn fold<R: Reg, V>(
        self,
        vals: &mut Slots,
        _: usize,
        at: Range<usize>,
        rows: impl Iterator<Item = V>,
        contribution: impl Fn(V) -> Option<R::In>,
    ) {
        for (&g, v) in self[at].iter().zip(rows) {
            if let Some(x) = contribution(v) {
                R::step(vals, g as usize, x);
            }
        }
    }
}

impl Agg {
    /// `op` over input column `src`, emitting into output column `out`.
    pub(crate) fn new(op: AggFunc, src: ColumnLocator, out: ColumnLocator) -> Self {
        let step = |op: AggFunc, loc: ColumnLocator| {
            StepKind::of(op, loc.type_code())
                .expect("AggFunc::output_type admits a sum only over a scalar register image")
        };
        Agg {
            src,
            out,
            kind: step(op, src),
            merge: step(op.merge_op(), out),
        }
    }

    #[inline(always)]
    pub(crate) fn is_linear(&self) -> bool {
        !matches!(self.kind, StepKind::Extreme { .. })
    }

    fn is_wide(&self) -> bool {
        matches!(self.kind, StepKind::Extreme { kind: ImageKind::Wide(_), .. })
    }

    /// A count or an integer sum: exact whatever the row order.
    pub(crate) fn is_exact_linear(&self) -> bool {
        matches!(
            self.kind,
            StepKind::Count | StepKind::CountNonNull | StepKind::Sum(ScalarKind::Int(_))
        )
    }

    /// A MIN/MAX's column as the value index keys it, its extreme first; `None`
    /// for a linear aggregate.
    #[inline(always)]
    pub(crate) fn index_image(&self) -> Option<ImageCol> {
        match self.kind {
            StepKind::Extreme { max, kind } => Some(ImageCol { loc: self.src, kind, invert: max }),
            _ => None,
        }
    }
}

impl AggValues {
    /// The net row count a COUNT, or a combine's sum of partial COUNTs, holds for
    /// group `g`.
    #[inline(always)]
    pub(crate) fn count_value(&self, k: usize, g: usize) -> i64 {
        debug_assert!(matches!(
            self.aggs[k].kind,
            StepKind::Count | StepKind::Sum(ScalarKind::Int(FixedInt::I64))
        ));
        self.acc[self.at(k, g)]
    }

    /// Empty group `g` of extreme `k`.
    #[inline(always)]
    pub(crate) fn clear_extreme(&mut self, k: usize, g: usize) {
        let at = self.at(k, g);
        self.has[at] = false;
    }

    /// Seed group `g` of MIN/MAX `k` with the extreme image the value index
    /// holds for it.
    pub(crate) fn seed_from_index(&mut self, k: usize, g: usize, image: &[u8]) {
        let StepKind::Extreme { max, kind } = self.aggs[k].kind else {
            unreachable!("only an extreme is seeded from the value index")
        };
        let at = self.at(k, g);
        match kind {
            ImageKind::Scalar(_) => {
                self.acc[at] = u64::from_be_bytes(image[..8].try_into().unwrap()) as i64;
                self.has[at] = true;
            }
            ImageKind::Wide(kind) => self.store_wide(at, &wide_native_of_image(kind, max, image)),
        }
    }

    /// What one row at `weight` does to group `g` of aggregate `k`.
    #[inline(always)]
    fn apply(
        &mut self,
        k: usize,
        g: usize,
        (kind, loc): (StepKind, ColumnLocator),
        rows: &impl RowSource,
        row: usize,
        weight: i64,
    ) {
        if !matches!(kind, StepKind::Count) && loc.is_null(rows, row) {
            return;
        }
        match kind {
            StepKind::Count | StepKind::CountNonNull => IntSum::step(&mut self.slot(k, g), 0, weight),
            StepKind::Sum(ScalarKind::Int(fi)) => IntSum::step(
                &mut self.slot(k, g),
                0,
                loc.decode_i64(rows, row, fi).wrapping_mul(weight),
            ),
            StepKind::Sum(ScalarKind::F32) => {
                let v = f32::from_le_bytes(loc.bytes(rows, row).try_into().unwrap());
                FloatSum::step(&mut self.slot(k, g), 0, v as f64 * weight as f64)
            }
            StepKind::Sum(ScalarKind::F64) => {
                let v = f64::from_le_bytes(loc.bytes(rows, row).try_into().unwrap());
                FloatSum::step(&mut self.slot(k, g), 0, v * weight as f64)
            }
            StepKind::Extreme { max, kind } => {
                debug_assert!(weight > 0, "an extreme cannot retract: step it with positive rows only");
                match kind {
                    ImageKind::Scalar(kind) => {
                        Least::step(&mut self.slot(k, g), 0, scalar_image(&loc, kind, max, rows, row))
                    }
                    ImageKind::Wide(kind) => {
                        let mut scratch = [0u8; 16];
                        let v = wide_native(&loc, kind, rows, row, &mut scratch);
                        let wins = if max { Ordering::Greater } else { Ordering::Less };
                        let at = self.at(k, g);
                        if !self.has[at] || kind.cmp_native(&v, self.wide(at)) == wins {
                            self.store_wide(at, &v);
                        }
                    }
                }
            }
        }
    }

    /// Step aggregate `k` over rows `ranges` of `mb`, each into its group: one
    /// pass over its column, in row order. A row at a non-positive weight steps no
    /// extreme.
    pub(crate) fn fold(&mut self, k: usize, mb: &MemBatch, ranges: &[(usize, usize)], groups: impl Groups) {
        let Agg { kind, src, .. } = self.aggs[k];
        macro_rules! fold {
            ($n:expr, $opk:expr, $reg:ty, $contribution:expr) => {
                fold_cells::<{ $n }, { $opk }, $reg>(&mut self.column(k), mb, src, ranges, groups, $contribution)
            };
        }
        // `$contribution` over the source column's `$n`-byte cells: a payload
        // column's native ones, or a PK column's OPK images under `$opk`.
        macro_rules! fold_ints {
            ($n:expr, $reg:ty, |$opk:ident| $contribution:expr) => {
                match src {
                    ColumnLocator::Payload { .. } => {
                        const $opk: bool = false;
                        fold!($n, $opk, $reg, $contribution)
                    }
                    ColumnLocator::Pk { .. } => {
                        const $opk: bool = true;
                        fold!($n, $opk, $reg, $contribution)
                    }
                }
            };
        }
        match kind {
            // Reads no column.
            StepKind::Count => fold!(0, false, IntSum, |_, w, _| Some(w)),
            // A PK column is never NULL.
            StepKind::CountNonNull => fold!(0, false, IntSum, |_, w, null| Some(if null { 0 } else { w })),
            StepKind::Sum(ScalarKind::Int(fi)) => for_each_fixed_int!(fi, |FI| {
                fold_ints!(FI.width(), IntSum, |OPK| |v, w, null| {
                    Some(if null {
                        0
                    } else {
                        int_cell::<OPK>(FI, &v).wrapping_mul(w)
                    })
                })
            }),
            // A float is never a PK column.
            StepKind::Sum(ScalarKind::F32) => fold!(4, false, FloatSum, |v, w, null| {
                (!null).then(|| f32::from_le_bytes(v) as f64 * w as f64)
            }),
            StepKind::Sum(ScalarKind::F64) => fold!(8, false, FloatSum, |v, w, null| {
                (!null).then(|| f64::from_le_bytes(v) * w as f64)
            }),
            StepKind::Extreme { max, kind: ImageKind::Scalar(scalar) } => {
                // A MAX keeps the smallest complement, as the value index does.
                let flip = if max { u64::MAX } else { 0 };
                match scalar {
                    ScalarKind::Int(fi) => for_each_fixed_int!(fi, |FI| {
                        fold_ints!(FI.width(), Least, |OPK| move |v, w, null| {
                            (w > 0 && !null)
                                .then(|| ScalarKind::Int(FI).order_image(int_cell::<OPK>(FI, &v) as u64) ^ flip)
                        })
                    }),
                    ScalarKind::F32 => fold!(4, false, Least, move |v, w, null| {
                        (w > 0 && !null).then(|| ScalarKind::F32.order_image(u32::from_le_bytes(v) as u64) ^ flip)
                    }),
                    ScalarKind::F64 => fold!(8, false, Least, move |v, w, null| {
                        (w > 0 && !null).then(|| ScalarKind::F64.order_image(u64::from_le_bytes(v)) ^ flip)
                    }),
                }
            }
            // A wide extreme compares native bytes, a row at a time.
            StepKind::Extreme { kind: ImageKind::Wide(_), .. } => {
                let mut i = 0;
                for (range, &(s, e)) in ranges.iter().enumerate() {
                    for row in s..e {
                        let w = mb.get_weight(row);
                        if w > 0 {
                            self.apply(k, groups.of(range, i), (kind, src), mb, row, w);
                        }
                        i += 1;
                    }
                }
            }
        }
    }

    /// What `rows`' row at `weight` does to group `g` of every aggregate. An
    /// extreme takes a positive weight only.
    #[inline(always)]
    pub(crate) fn step(&mut self, g: usize, rows: &impl RowSource, row: usize, weight: i64) {
        for k in 0..self.aggs.len() {
            let Agg { kind, src, .. } = self.aggs[k];
            self.apply(k, g, (kind, src), rows, row, weight);
        }
    }

    /// Empty every group.
    pub(crate) fn clear(&mut self) {
        self.acc.fill(0);
        self.has.fill(false);
    }

    /// Give group `to` the values group `from` holds.
    pub(crate) fn copy(&mut self, from: usize, to: usize) {
        for k in 0..self.aggs.len() {
            let (from, to) = (self.at(k, from), self.at(k, to));
            self.acc[to] = self.acc[from];
            if let StepKind::Extreme { kind, .. } = self.aggs[k].kind {
                self.has[to] = self.has[from];
                if matches!(kind, ImageKind::Wide(_)) && self.has[from] {
                    let value = self.wide(from).to_vec();
                    self.store_wide(to, &value);
                }
            }
        }
    }

    /// Whether groups `a` and `b` hold the same values.
    #[inline]
    pub(crate) fn same(&self, a: usize, b: usize) -> bool {
        self.aggs.iter().enumerate().all(|(k, agg)| {
            let (a, b) = (self.at(k, a), self.at(k, b));
            match agg.kind {
                StepKind::Extreme { kind, .. } => {
                    self.has[a] == self.has[b]
                        && (!self.has[a]
                            || match kind {
                                ImageKind::Scalar(_) => self.acc[a] == self.acc[b],
                                ImageKind::Wide(_) => self.wide(a) == self.wide(b),
                            })
                }
                _ => self.acc[a] == self.acc[b],
            }
        })
    }

    /// Fold into group `g` the values of a previously-emitted output row: every
    /// aggregate's, or only the linear ones' unless `extremes`.
    pub(crate) fn fold_stored(&mut self, g: usize, out_row: &impl RowSource, row: usize, extremes: bool) {
        for k in 0..self.aggs.len() {
            let Agg { merge, out, .. } = self.aggs[k];
            if extremes || self.aggs[k].is_linear() {
                self.apply(k, g, (merge, out), out_row, row, 1);
            }
        }
    }

    /// Emit one `[key…, group columns…, aggregates…]` row at weight +1, copying
    /// the group columns from `group`'s row at its locators — the ground row has
    /// none — and the aggregates from group `g`.
    pub(crate) fn emit_row(
        &self,
        output: &mut Batch,
        group: Option<(&MemBatch, usize, &[ColumnLocator])>,
        out_pk_bytes: &[u8],
        g: usize,
    ) {
        output.begin_row(out_pk_bytes, 1);
        if let Some((src, row, locs)) = group {
            output.append_cells_from(locs, src, row);
        }
        self.emit(g, output);
        output.commit_row();
    }

    /// Write group `g`'s value of every aggregate into its column of the row
    /// `out` has open; an extreme with no value renders NULL.
    #[inline(always)]
    pub(crate) fn emit(&self, g: usize, out: &mut Batch) {
        for (k, agg) in self.aggs.iter().enumerate() {
            let ColumnLocator::Payload { slot, size, .. } = agg.out else {
                unreachable!("an aggregate is a payload column")
            };
            let (pi, size) = (slot as usize, size as usize);
            let at = self.at(k, g);
            match agg.kind {
                StepKind::Extreme { .. } if !self.has[at] => out.put_null(pi),
                StepKind::Extreme {
                    kind: ImageKind::Wide(WideKind::Bytes), ..
                } => out.extend_col_blob(pi, self.wide(at)),
                StepKind::Extreme {
                    kind: ImageKind::Wide(WideKind::Fixed(_)),
                    ..
                } => out.extend_col(pi, self.wide(at)),
                StepKind::Extreme { kind: ImageKind::Scalar(kind), max } => {
                    let bits = scalar_native_of_image(kind, max, self.acc[at] as u64);
                    out.extend_col(pi, &bits.to_le_bytes()[..size])
                }
                _ => out.extend_col(pi, &(self.acc[at] as u64).to_le_bytes()[..size]),
            }
        }
    }
}

/// Rows `ranges` of source column `src`, in row order, each stepping
/// its group by what `contribution` makes of its `(cell, weight, is NULL)`. The
/// cells are `N` bytes: a PK column's under `OPK`, else a payload column's, which
/// alone can be NULL. `N = 0` reads no cell.
#[inline(never)]
fn fold_cells<const N: usize, const OPK: bool, R: Reg>(
    vals: &mut Slots,
    mb: &MemBatch,
    src: ColumnLocator,
    ranges: &[(usize, usize)],
    groups: impl Groups,
    contribution: impl Fn([u8; N], i64, bool) -> Option<R::In>,
) {
    let weights = mb.weight().as_chunks::<8>().0;
    let nulls = mb.null_bmp().as_chunks::<8>().0;
    let (cells, stride, off) = match src {
        _ if N == 0 => (&[][..], 0, 0),
        ColumnLocator::Pk { byte_off, .. } => (mb.pk(), mb.pk_stride(), byte_off as usize),
        ColumnLocator::Payload { slot, .. } => (mb.col_data(slot as usize), N, 0),
    };
    // A PK column has no null bit: a constant under `OPK`, so its loop tests none.
    let null_bit = if OPK { 0 } else { src.null_bit() };
    let mut at = 0;
    for (range, &(s, e)) in ranges.iter().enumerate() {
        let fold_rows = at..at + (e - s);
        at = fold_rows.end;
        let rows = weights[s..e]
            .iter()
            .zip(&nulls[s..e])
            .map(|(w, nw)| (i64::from_le_bytes(*w), u64::from_le_bytes(*nw) & null_bit != 0));
        if N == 0 {
            groups.fold::<R, _>(vals, range, fold_rows, rows, |(w, null)| contribution([0; N], w, null));
        } else if !OPK || stride == N {
            let rows = cells[s * N..e * N].as_chunks::<N>().0.iter().zip(rows);
            groups.fold::<R, _>(vals, range, fold_rows, rows, |(v, (w, null))| contribution(*v, w, null));
        } else {
            let rows = cells[s * stride..e * stride].chunks_exact(stride).zip(rows);
            groups.fold::<R, _>(vals, range, fold_rows, rows, |(row, (w, null))| {
                contribution(row[off..off + N].try_into().unwrap(), w, null)
            });
        }
    }
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
