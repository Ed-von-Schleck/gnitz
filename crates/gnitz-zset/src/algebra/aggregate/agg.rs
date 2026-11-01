//! An aggregate, and the running values of a list of them.

use std::cmp::Ordering;
use std::ops::Range;
use std::rc::Rc;

use crate::algebra::{vec_bytes, zeroed};
use crate::repr::{Batch, DirectWriter, MemBatch};
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
    fn of(op: AggFunc, tc: TypeCode) -> Self {
        match op {
            AggFunc::Count => StepKind::Count,
            AggFunc::CountNonNull => StepKind::CountNonNull,
            AggFunc::Sum => StepKind::Sum(
                ScalarKind::from_type_code(tc)
                    .expect("AggFunc::output_type admits a sum only over a scalar register image"),
            ),
            AggFunc::Min | AggFunc::Max => StepKind::Extreme {
                max: op == AggFunc::Max,
                kind: ImageKind::of(tc),
            },
        }
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
    groups: usize,
    /// Groups each aggregate has room for: aggregate `k`'s values start at
    /// `k * cap`.
    cap: usize,
}

/// Whether a list carries an extremum, and so a `has` value per group.
fn extremes(aggs: &[Agg]) -> bool {
    aggs.iter().any(|a| !a.is_linear())
}

/// Whether a list carries a wide aggregate, and so a `wide` value per group.
fn wides(aggs: &[Agg]) -> bool {
    aggs.iter().any(Agg::is_wide)
}

impl AggValues {
    /// `groups` empty groups of `aggs`.
    pub(crate) fn new(aggs: &Rc<[Agg]>, groups: usize) -> Self {
        let mut values = AggValues {
            acc: Vec::new(),
            has: Vec::new(),
            wide: Vec::new(),
            heap: Vec::new(),
            aggs: Rc::clone(aggs),
            groups: 0,
            cap: 0,
        };
        values.reset(groups);
        values
    }

    /// `groups` empty groups, in the buffers this already holds.
    pub(crate) fn reset(&mut self, groups: usize) {
        // Laid out again at `cap = groups`: every value sits at `k * cap + g`.
        let n = self.aggs.len() * groups;
        zeroed(&mut self.acc, n);
        if extremes(&self.aggs) {
            zeroed(&mut self.has, n);
        }
        if wides(&self.aggs) {
            zeroed(&mut self.wide, n);
        }
        self.heap.clear();
        self.groups = groups;
        self.cap = groups;
    }

    /// The bytes this holds allocated.
    pub(crate) fn held_bytes(&self) -> usize {
        vec_bytes(&self.acc) + vec_bytes(&self.has) + vec_bytes(&self.wide) + vec_bytes(&self.heap)
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
            if extremes(&self.aggs) {
                regrow(&mut self.has, aggs, old, cap, false);
            }
            if wides(&self.aggs) {
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

    /// Give group `at` room for `len` bytes and return where they start, for the
    /// caller to write. A group whose room is too small moves to the end of
    /// `heap`, into room for the larger of `len` and twice its old room. Sets its
    /// length to `len` and marks it holding a value.
    #[inline(always)]
    fn reserve_wide(&mut self, at: usize, len: usize) -> usize {
        let (start, _, room) = self.wide[at];
        let len32 = len as u32;
        let start = if len32 <= room {
            start
        } else {
            let room = len32.max(room.saturating_mul(2));
            let start = self.heap.len();
            self.heap.resize(start + room as usize, 0);
            self.wide[at].0 = start;
            self.wide[at].2 = room;
            start
        };
        self.wide[at].1 = len32;
        self.has[at] = true;
        start
    }

    /// Store `v` as group `at`'s value, growing as [`Self::reserve_wide`] does.
    /// A move appends `v` itself, so only the room past it is zeroed.
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

    /// The value after each of `xs` in turn, a `None` leaving it as it is.
    #[inline(always)]
    fn fold_run(self, xs: impl Iterator<Item = Option<Self::In>>) -> Self {
        xs.fold(self, |r, x| x.and_then(|x| r.absorb(x)).unwrap_or(r))
    }

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

    /// The least of the run with no branch per row, so it vectorizes; `any`
    /// keeps a real image of `u64::MAX` apart from no image at all.
    #[inline(always)]
    fn fold_run(self, xs: impl Iterator<Item = Option<u64>>) -> Self {
        let (mut least, mut any) = (u64::MAX, false);
        for x in xs {
            any |= x.is_some();
            least = least.min(x.unwrap_or(u64::MAX));
        }
        match (any, self.1) {
            (false, _) => self,
            (true, false) => Least(least, true),
            (true, true) => Least(self.0.min(least), true),
        }
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
        R::load(vals, g).fold_run(rows.map(contribution)).store(vals, g);
    }
}

/// The rows of a fold's `i`-th range in one group.
#[derive(Clone, Copy)]
pub(crate) enum RangeGroups {
    /// Group `i`.
    Each,
    /// Group 0.
    First,
}

impl Groups for RangeGroups {
    #[inline(always)]
    fn of(self, range: usize, _: usize) -> usize {
        match self {
            RangeGroups::Each => range,
            RangeGroups::First => 0,
        }
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
        Agg {
            src,
            out,
            kind: StepKind::of(op, src.type_code()),
            merge: StepKind::of(op.merge_op(), out.type_code()),
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
            StepKind::Extreme { max, .. } => Some(ImageCol::new(self.src, max)),
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
            ImageKind::Wide(kind) => {
                let start = self.reserve_wide(at, image.len());
                let room = &mut self.heap[start..start + image.len()];
                room.copy_from_slice(image);
                // The native bytes are never longer than their image.
                self.wide[at].1 = wide_native_of_image(kind, max, room) as u32;
            }
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
        self.fold_column(k, (kind, src), mb, ranges, groups);
    }

    /// Fold into group `groups[i]` the values of output row `i` of `stored`,
    /// every row of it: each aggregate over its own column, as a previously
    /// emitted row's value merges.
    pub(crate) fn fold_stored_rows(&mut self, stored: &MemBatch, groups: &[u32]) {
        debug_assert_eq!(stored.len(), groups.len());
        for k in 0..self.aggs.len() {
            let Agg { merge, out, .. } = self.aggs[k];
            self.fold_column(k, (merge, out), stored, &[(0, stored.len())], groups);
        }
    }

    /// [`Self::fold`] of aggregate `k` stepping by `kind` over column `src`.
    fn fold_column(
        &mut self,
        k: usize,
        (kind, src): (StepKind, ColumnLocator),
        mb: &MemBatch,
        ranges: &[(usize, usize)],
        groups: impl Groups,
    ) {
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
                    let (src, len, _) = self.wide[from];
                    let dst = self.reserve_wide(to, len as usize);
                    self.heap.copy_within(src..src + len as usize, dst);
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

    /// Fold into group `g` the values of a previously-emitted output row.
    pub(crate) fn fold_stored(&mut self, g: usize, out_row: &impl RowSource, row: usize) {
        let nulls = out_row.get_null_word(row);
        for k in 0..self.aggs.len() {
            let Agg { merge, out, .. } = self.aggs[k];
            match (merge, out) {
                // An integer sum's merge over an 8-byte cell, read in place.
                (StepKind::Sum(ScalarKind::Int(_)), ColumnLocator::Payload { slot, size: 8, .. })
                    if !gnitz_wire::null_word_get(nulls, slot as usize) =>
                {
                    let cell = out_row.get_col_ptr(row, slot as usize, 8);
                    let at = self.at(k, g);
                    self.acc[at] = self.acc[at].wrapping_add(i64::from_le_bytes(cell.try_into().unwrap()));
                }
                _ => self.fold_stored_cell(k, g, out_row, row),
            }
        }
    }

    /// [`Self::fold_stored`] of aggregate `k` alone.
    #[inline(never)]
    fn fold_stored_cell(&mut self, k: usize, g: usize, out_row: &impl RowSource, row: usize) {
        let Agg { merge, out, .. } = self.aggs[k];
        self.apply(k, g, (merge, out), out_row, row, 1);
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

    /// [`Self::emit`] a column at a time: group `g`'s value of every aggregate
    /// into row `g` of `w`, for every row it writes. An extreme with no value
    /// renders NULL, its bit ORed into the null words `w` already holds.
    pub(crate) fn emit_columns(&self, w: &mut DirectWriter<'_>) {
        let n = w.rows();
        debug_assert!(n <= self.groups);
        for (k, agg) in self.aggs.iter().enumerate() {
            let ColumnLocator::Payload { slot, size, .. } = agg.out else {
                unreachable!("an aggregate is a payload column")
            };
            let (pi, size) = (slot as usize, size as usize);
            let values = k * self.cap..k * self.cap + n;
            let acc = &self.acc[values.clone()];
            let StepKind::Extreme { kind, max } = agg.kind else {
                put_cells(w.col_mut(pi), size, acc.iter().map(|&v| v as u64));
                continue;
            };
            let has = &self.has[values.clone()];
            match kind {
                ImageKind::Scalar(kind) => {
                    let native = |(&v, &has): (&i64, &bool)| match has {
                        true => scalar_native_of_image(kind, max, v as u64),
                        false => 0,
                    };
                    put_cells(w.col_mut(pi), size, acc.iter().zip(has).map(native));
                }
                ImageKind::Wide(WideKind::Fixed(_)) => {
                    for ((cell, at), &has) in w.col_mut(pi).chunks_exact_mut(size).zip(values.clone()).zip(has) {
                        match has {
                            true => cell.copy_from_slice(self.wide(at)),
                            false => cell.fill(0),
                        }
                    }
                }
                ImageKind::Wide(WideKind::Bytes) => {
                    let (cells, heap, _) = w.string_col_mut(pi);
                    for ((cell, at), &has) in cells.chunks_exact_mut(16).zip(values.clone()).zip(has) {
                        match has {
                            true => cell.copy_from_slice(&gnitz_wire::encode_german_string(self.wide(at), heap)),
                            false => cell.fill(0),
                        }
                    }
                }
            }
            for (word, _) in w.null_bmp_mut().chunks_exact_mut(8).zip(has).filter(|(_, &has)| !has) {
                word[pi / 8] |= 1 << (pi % 8);
            }
        }
    }

    /// For each `(row, g)` of `rows`, group `g`'s value of every aggregate into
    /// row `row` of `w`, over whatever cells it held. An extreme renders NULL
    /// where it has no value and clears its null bit where it has one; a count
    /// or a sum, never NULL, leaves its bit as it was.
    pub(crate) fn emit_at(&self, w: &mut DirectWriter<'_>, rows: impl Iterator<Item = (usize, usize)> + Clone) {
        for (k, agg) in self.aggs.iter().enumerate() {
            let ColumnLocator::Payload { slot, size, .. } = agg.out else {
                unreachable!("an aggregate is a payload column")
            };
            let (pi, size) = (slot as usize, size as usize);
            let at = |g: usize| self.at(k, g);
            let StepKind::Extreme { kind, max } = agg.kind else {
                put_cells_at(
                    w.col_mut(pi),
                    size,
                    rows.clone().map(|(row, g)| (row, self.acc[at(g)] as u64)),
                );
                continue;
            };
            match kind {
                ImageKind::Scalar(kind) => {
                    let native = |(row, g): (usize, usize)| match self.has[at(g)] {
                        true => (row, scalar_native_of_image(kind, max, self.acc[at(g)] as u64)),
                        false => (row, 0),
                    };
                    put_cells_at(w.col_mut(pi), size, rows.clone().map(native));
                }
                ImageKind::Wide(WideKind::Fixed(_)) => {
                    let col = w.col_mut(pi);
                    for (row, g) in rows.clone() {
                        let cell = &mut col[row * size..(row + 1) * size];
                        match self.has[at(g)] {
                            true => cell.copy_from_slice(self.wide(at(g))),
                            false => cell.fill(0),
                        }
                    }
                }
                ImageKind::Wide(WideKind::Bytes) => {
                    let (cells, heap, _) = w.string_col_mut(pi);
                    for (row, g) in rows.clone() {
                        let cell = &mut cells[row * 16..(row + 1) * 16];
                        match self.has[at(g)] {
                            true => cell.copy_from_slice(&gnitz_wire::encode_german_string(self.wide(at(g)), heap)),
                            false => cell.fill(0),
                        }
                    }
                }
            }
            set_null_bits(
                w.null_bmp_mut(),
                pi,
                rows.clone().map(|(row, g)| (row, !self.has[at(g)])),
            );
        }
    }

    /// Group `g`'s values against the aggregate columns of row `row` of
    /// `stored`, an output row, each by its type's order, NULL below every
    /// value: the payload order of two output rows whose other columns are equal,
    /// as the aggregates are the trailing ones.
    pub(crate) fn cmp_stored(&self, g: usize, stored: &impl RowSource, row: usize) -> Ordering {
        let nulls = stored.get_null_word(row);
        for (k, agg) in self.aggs.iter().enumerate() {
            let at = self.at(k, g);
            let ord = match (agg.kind, agg.out) {
                // A count or an integer sum over an 8-byte cell, never NULL.
                (
                    StepKind::Count | StepKind::CountNonNull | StepKind::Sum(ScalarKind::Int(_)),
                    ColumnLocator::Payload { slot, size: 8, type_code },
                ) => {
                    debug_assert!(!gnitz_wire::null_word_get(nulls, slot as usize));
                    cmp_int8(
                        self.acc[at] as u64,
                        stored.get_col_ptr(row, slot as usize, 8),
                        type_code,
                    )
                }
                // An 8-byte integer extreme with a value, against a cell holding one.
                (
                    StepKind::Extreme {
                        kind: ImageKind::Scalar(kind @ ScalarKind::Int(_)),
                        max,
                    },
                    ColumnLocator::Payload { slot, size: 8, type_code },
                ) if self.has[at] && !gnitz_wire::null_word_get(nulls, slot as usize) => {
                    let native = scalar_native_of_image(kind, max, self.acc[at] as u64);
                    cmp_int8(native, stored.get_col_ptr(row, slot as usize, 8), type_code)
                }
                _ => self.cmp_stored_cell(k, at, stored, row, nulls),
            };
            if ord.is_ne() {
                return ord;
            }
        }
        Ordering::Equal
    }

    /// [`Self::cmp_stored`] of aggregate `k`'s value `at` alone.
    #[inline(never)]
    fn cmp_stored_cell(&self, k: usize, at: usize, stored: &impl RowSource, row: usize, nulls: u64) -> Ordering {
        let agg = &self.aggs[k];
        let ColumnLocator::Payload { slot, size, type_code } = agg.out else {
            unreachable!("an aggregate is a payload column")
        };
        let (pi, size) = (slot as usize, size as usize);
        let null = gnitz_wire::null_word_get(nulls, pi);
        match agg.kind {
            StepKind::Extreme { .. } if !self.has[at] => match null {
                true => Ordering::Equal,
                false => Ordering::Less,
            },
            _ if null => Ordering::Greater,
            StepKind::Extreme {
                kind: ImageKind::Wide(WideKind::Bytes), ..
            } => self.wide(at).cmp(&agg.out.content(stored, row)),
            StepKind::Extreme {
                kind: ImageKind::Wide(WideKind::Fixed(_)),
                ..
            } => gnitz_wire::cmp_col_window(self.wide(at), stored.get_col_ptr(row, pi, size), type_code),
            StepKind::Extreme { kind: ImageKind::Scalar(kind), max } => {
                let bits = scalar_native_of_image(kind, max, self.acc[at] as u64);
                gnitz_wire::cmp_col_window(
                    &bits.to_le_bytes()[..size],
                    stored.get_col_ptr(row, pi, size),
                    type_code,
                )
            }
            _ => gnitz_wire::cmp_col_window(
                &(self.acc[at] as u64).to_le_bytes()[..size],
                stored.get_col_ptr(row, pi, size),
                type_code,
            ),
        }
    }
}

/// `values` into `col`'s cells of `size` bytes each, each value's low `size`
/// bytes, little-endian.
#[inline(always)]
fn put_cells(col: &mut [u8], size: usize, values: impl Iterator<Item = u64>) {
    gnitz_wire::by_cell_width!(size, |T| {
        let cells = col.as_chunks_mut::<{ size_of::<T>() }>().0;
        for (cell, v) in cells.iter_mut().zip(values) {
            *cell = (v as T).to_le_bytes();
        }
    })
}

/// The 8-byte integer `v` against `cell`'s, both of integer type `tc`.
#[inline(always)]
fn cmp_int8(v: u64, cell: &[u8], tc: gnitz_wire::TypeCode) -> Ordering {
    let cell = u64::from_le_bytes(cell.try_into().unwrap());
    match tc.is_signed_int() {
        true => (v as i64).cmp(&(cell as i64)),
        false => v.cmp(&cell),
    }
}

/// Each `(row, v)` of `values` into row `row` of `col`'s cells of `size` bytes
/// each: `v`'s low `size` bytes, little-endian.
#[inline(always)]
fn put_cells_at(col: &mut [u8], size: usize, values: impl Iterator<Item = (usize, u64)>) {
    gnitz_wire::by_cell_width!(size, |T| {
        let cells = col.as_chunks_mut::<{ size_of::<T>() }>().0;
        for (row, v) in values {
            cells[row] = (v as T).to_le_bytes();
        }
    })
}

/// Payload column `pi`'s bit in the null word of each `(row, null)` of `rows`,
/// set where `null` and cleared elsewhere.
#[inline(always)]
fn set_null_bits(null_bmp: &mut [u8], pi: usize, rows: impl Iterator<Item = (usize, bool)>) {
    let (byte, bit) = (pi / 8, 1u8 << (pi % 8));
    let words = null_bmp.as_chunks_mut::<8>().0;
    for (row, null) in rows {
        let b = &mut words[row][byte];
        *b = (*b & !bit) | if null { bit } else { 0 };
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
