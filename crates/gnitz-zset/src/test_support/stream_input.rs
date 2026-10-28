//! The input the stream operators' tests drive the reduce, the top-N and the
//! window with: a schema of every column form, its rows, the group key forms
//! over it, and each aggregate of one group's rows by its SQL definition.

use std::cmp::Ordering;

use proptest::prelude::*;

use super::internal::{FLOATS, STRS};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::{cmp_col_window, AggDescriptor, AggFunc, FixedInt, TypeCode};

/// `[I64 val NULL, U64 id (pk), STRING s NULL, I64 grp (pk), F64 f, F32 x,
/// U128 w, I16 n NULL]`, keyed `(id, grp)`.
pub(crate) fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, true),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::F32, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I16, true),
        ],
        &[1, 3],
    )
}

/// One input row, a field per column in schema order; `f` and `x` as their bits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Row {
    pub(crate) val: Option<i64>,
    pub(crate) id: u64,
    pub(crate) s: Option<&'static str>,
    pub(crate) grp: i64,
    pub(crate) f: u64,
    pub(crate) x: u32,
    pub(crate) w: u128,
    pub(crate) n: Option<i16>,
}

/// One native cell per column, `None` for NULL.
pub(crate) type Cells = Vec<Option<Vec<u8>>>;

impl Row {
    /// The row's cells of `group_cols`, in that order.
    pub(crate) fn group_key(&self, group_cols: &[u32]) -> Cells {
        let row = cells(self);
        group_cols.iter().map(|&c| row[c as usize].clone()).collect()
    }
}

pub(crate) fn cells(r: &Row) -> Cells {
    vec![
        r.val.map(|v| v.to_le_bytes().to_vec()),
        Some(r.id.to_le_bytes().to_vec()),
        r.s.map(|s| s.as_bytes().to_vec()),
        Some(r.grp.to_le_bytes().to_vec()),
        Some(r.f.to_le_bytes().to_vec()),
        Some(r.x.to_le_bytes().to_vec()),
        Some(r.w.to_le_bytes().to_vec()),
        r.n.map(|n| n.to_le_bytes().to_vec()),
    ]
}

pub(crate) fn batch(rows: &[(Row, i64)]) -> Batch {
    let mut b = BatchBuilder::new(&schema());
    for &(r, w) in rows {
        b.begin_row_natives(&[r.id as u128, r.grp as u128], w);
        b.put_opt_int(r.val.map(|v| v as u128));
        match r.s {
            Some(s) => b.put_string(s),
            None => b.put_null(),
        }
        b.put_int(r.f as u128);
        b.put_int(r.x as u128);
        b.put_int(r.w);
        b.put_opt_int(r.n.map(|n| n as u128));
        b.end_row();
    }
    b.finish()
}

/// Worker `w`'s share of `delta` over two workers: its rows sharded by id, as
/// the exchange routes a keyed input.
pub(crate) fn shard(delta: &[(Row, i64)], w: u64) -> Batch {
    let mine: Vec<(Row, i64)> = delta.iter().copied().filter(|(r, _)| r.id % 2 == w).collect();
    batch(&mine)
}

/// Every group key form: V₀, the whole PK, its leading column, a non-leading
/// signed PK column's image, a 16-byte payload column's image, the fold of a
/// nullable column, of a column and a string, and of a set one column wider
/// than a value index's key packs.
pub(crate) const GROUPS: [&[u32]; 8] = [&[], &[1, 3], &[1], &[3], &[6], &[0], &[3, 2], &[0, 1, 3]];

const VALS: [i64; 5] = [i64::MIN, -1, 0, 1, i64::MAX];
const IDS: [u64; 4] = [0, 1, 1 << 63, u64::MAX];
/// Dyadic and non-zero, so a sum of them is exact in any order and never `-0.0`.
const DYADICS: [f32; 4] = [-1.5, 0.25, 2.0, 4.5];
/// Pairs equal in their high and in their low eight bytes.
const WIDES: [u128; 4] = [1, 1 << 64, (1 << 64) + 1, u128::MAX];
const NARROWS: [i16; 4] = [i16::MIN, -1, 1, i16::MAX];

pub(crate) fn arb_row() -> impl Strategy<Value = Row> {
    (
        prop::option::of(prop::sample::select(&VALS[..])),
        prop::sample::select(&IDS[..]),
        prop::option::of(prop::sample::select(&STRS[..])),
        -1i64..1,
        prop::sample::select(&FLOATS[..]),
        prop::sample::select(&DYADICS[..]),
        prop::sample::select(&WIDES[..]),
        prop::option::of(prop::sample::select(&NARROWS[..])),
    )
        .prop_map(|(val, id, s, grp, f, x, w, n)| Row {
            val,
            id,
            s,
            grp,
            f: f.to_bits(),
            x: x.to_bits(),
            w,
            n,
        })
}

/// The typed order of two cells of column type `tc`: a string's by its content.
fn cmp_cells(tc: TypeCode, a: &[u8], b: &[u8]) -> Ordering {
    match tc {
        TypeCode::String => a.cmp(b),
        _ => cmp_col_window(a, b, tc),
    }
}

pub(crate) const fn agg(agg_op: AggFunc, col_idx: u32) -> AggDescriptor {
    AggDescriptor { agg_op, col_idx }
}

/// `agg` over one group's weighted rows of [`schema`], by its SQL definition: a
/// float sum is each value widened to `f64` times its weight, added in row
/// order.
pub(crate) fn agg_of(agg: AggDescriptor, rows: &[(Cells, i64)]) -> Option<Vec<u8>> {
    let col = agg.col_idx as usize;
    let tc = schema().columns()[col].type_code;
    let live = || rows.iter().filter_map(|(r, w)| r[col].as_deref().map(|c| (c, *w)));
    let i64_cell = |v: i64| Some(v.to_le_bytes().to_vec());
    match agg.agg_op {
        AggFunc::Count => i64_cell(rows.iter().map(|(_, w)| w).sum()),
        AggFunc::CountNonNull => i64_cell(live().map(|(_, w)| w).sum()),
        AggFunc::Sum if matches!(tc, TypeCode::F32 | TypeCode::F64) => {
            let widen = |c: &[u8]| match tc {
                TypeCode::F32 => f32::from_le_bytes(c.try_into().unwrap()) as f64,
                _ => f64::from_le_bytes(c.try_into().unwrap()),
            };
            let sum = live().fold(0f64, |a, (c, w)| a + widen(c) * w as f64);
            Some(sum.to_le_bytes().to_vec())
        }
        AggFunc::Sum => {
            let int = FixedInt::from_type_code(tc).expect("an integer sum reads a narrow integer");
            i64_cell(live().fold(0, |a, (c, w)| a.wrapping_add(int.decode_le_i64(c).wrapping_mul(w))))
        }
        AggFunc::Min | AggFunc::Max => {
            let mut present: Vec<&[u8]> = live().map(|(c, _)| c).collect();
            present.sort_by(|a, b| cmp_cells(tc, a, b));
            let extreme = match agg.agg_op {
                AggFunc::Min => present.first(),
                _ => present.last(),
            };
            extreme.map(|c| c.to_vec())
        }
    }
}
