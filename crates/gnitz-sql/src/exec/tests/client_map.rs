use super::*;
use crate::project::reply_program;
use crate::test_support::Cell::{Int, Null, Str};
use crate::test_support::{col, keyed_batch, ncol, rows_of, schema};
use gnitz_wire::{ColumnDef, TypeCode};

/// A spilled body, and one the map computes.
const LONG: &str = "a string past the inline prefix";
const FALLBACK: &str = "a computed fallback past the inline prefix";

/// `(k I64 PK, v I64 NULL, s STRING NULL)`.
fn source() -> Schema {
    schema(
        vec![
            col("k", TypeCode::I64),
            ncol("v", TypeCode::I64),
            ncol("s", TypeCode::String),
        ],
        &[0],
    )
}

/// The reply map of the SELECT list `sql` over [`source`]: the hidden key, then the items.
pub(super) fn map_of(src: &Schema, sql: &[&str]) -> ClientMap {
    let payload: Vec<_> = sql
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let bound = crate::test_support::bind_sql(e, src);
            let def = ColumnDef::new(format!("c{i}"), bound.infer_ty(src.columns()).tc, true);
            (bound, def)
        })
        .collect();
    let (out, program) = reply_program(payload, src).unwrap();
    ClientMap::new(Some(program), src, Arc::new(out)).unwrap()
}

/// Every row keeps its key and weight. A PK copy is the key's native value — the OPK sign
/// flip undone — and never NULL. A payload copy moves its source region, cloned only while
/// a later slot still reads it. A computed string spills into the output arena while a
/// copied one keeps its offset into the source's, so both read back from one batch.
#[test]
fn a_map_keeps_key_and_weight_and_fills_each_slot() {
    let src = source();
    let fallback = format!("COALESCE(CASE WHEN v IS NULL THEN s END, '{FALLBACK}')");
    // (items, whether each output slot holds a source region, rows)
    for (items, moved, want) in [
        (&["v", "k"][..], [true, false], [[Int(7), Int(-5)], [Null, Int(3)]]),
        (&["v", "v"], [false, true], [[Int(7), Int(7)], [Null, Null]]),
        (
            &["s", &fallback],
            [true, false],
            [[Str(LONG), Str(FALLBACK)], [Null, Str(FALLBACK)]],
        ),
        (&["s", "v + 1"], [true, false], [[Str(LONG), Int(8)], [Null, Null]]),
        // The source's own layout: the batch comes back as it went in.
        (&["v", "s"], [true, true], [[Int(7), Str(LONG)], [Null, Null]]),
    ] {
        let b = keyed_batch(
            &src,
            &[(&[(-5i64) as u128], 1, &[Int(7), Str(LONG)]), (&[3], 2, &[Null, Null])],
        );
        let (pks, regions) = (
            b.pks.clone(),
            b.payload.iter().map(|c| c.bytes.as_ptr()).collect::<Vec<_>>(),
        );
        let mut map = map_of(&src, items);
        let out = map.apply(b);
        assert_eq!(out.pks, pks, "{items:?}");
        assert_eq!(
            out.payload
                .iter()
                .map(|c| regions.contains(&c.bytes.as_ptr()))
                .collect::<Vec<_>>(),
            moved,
            "{items:?}"
        );
        let want: Vec<_> = want.into_iter().map(Vec::from).zip([1, 2]).collect();
        assert_eq!(rows_of(map.out_schema(), &out), want, "{items:?}");
    }
}
