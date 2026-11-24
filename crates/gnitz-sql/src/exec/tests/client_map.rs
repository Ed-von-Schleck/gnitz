use super::*;
use crate::ir::BoundExpr;
use crate::test_support::Cell::{Int, Null, Str};
use crate::test_support::{LONG, col, keyed_batch, ncol, rows_of, schema};
use gnitz_wire::{ColumnDef, TypeCode};

/// A spilled body the map computes.
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

/// The map of the SELECT list `sql` over `src`: the hidden key, then the items.
pub(super) fn map_of(src: &Schema, sql: &[&str]) -> ClientMap {
    let key = src
        .pk_cols()
        .iter()
        .map(|&c| BoundExpr::ColRef(c as usize))
        .zip(src.hidden_key_columns());
    let items = sql.iter().enumerate().map(|(i, e)| {
        let bound = crate::test_support::bind_sql(e, src);
        let def = ColumnDef::new(format!("c{i}"), bound.infer_ty(src.columns()).tc, true);
        (bound, def)
    });
    let (items, cols): (Vec<_>, Vec<_>) = key.chain(items).unzip();
    let out = Schema::from_parts(cols, gnitz_expr::leading_pk(src.pk_cols().len())).unwrap();
    let program = crate::project::payload_program(&items, &out, src).unwrap();
    ClientMap::new(Some(program), Arc::new(src.clone()), Arc::new(out)).unwrap()
}

/// Every row keeps its key and weight. A PK copy is the key's native value — the OPK sign
/// flip undone — and never NULL. A computed string spills into the output heap while a
/// copied one keeps its offset into the source's, so both read back from one batch; with
/// no copied string the output heap holds only what the map computes, and none at all
/// when nothing is.
#[test]
fn a_map_keeps_key_and_weight_and_fills_each_slot() {
    let src = source();
    let fallback = format!("COALESCE(CASE WHEN v IS NULL THEN s END, '{FALLBACK}')");
    // (items, rows, the output heap's length where the case pins it)
    for (items, want, want_blob) in [
        (&["v", "k"][..], [[Int(7), Int(-5)], [Null, Int(3)]], Some(0)),
        (&["v", "v"], [[Int(7), Int(7)], [Null, Null]], None),
        (
            &["s", &fallback],
            [[Str(LONG), Str(FALLBACK)], [Null, Str(FALLBACK)]],
            None,
        ),
        (&["s", "v + 1"], [[Str(LONG), Int(8)], [Null, Null]], None),
        // A computed string and no copied one: the heap holds only the re-encoded body.
        (
            &["v", "COALESCE(s, 'x')"],
            [[Int(7), Str(LONG)], [Null, Str("x")]],
            Some(LONG.len()),
        ),
        // The source's own layout: the batch comes back as it went in.
        (&["v", "s"], [[Int(7), Str(LONG)], [Null, Null]], None),
    ] {
        let b = keyed_batch(
            &src,
            &[(&[(-5i64) as u128], 1, &[Int(7), Str(LONG)]), (&[3], 2, &[Null, Null])],
        );
        let pks = b.view().pk().to_vec();
        let mut map = map_of(&src, items);
        let out = map.apply(b);
        assert_eq!(out.view().pk(), pks, "{items:?}");
        let want: Vec<_> = want.into_iter().map(Vec::from).zip([1, 2]).collect();
        assert_eq!(rows_of(&out), want, "{items:?}");
        if let Some(n) = want_blob {
            assert_eq!(out.view().blob().len(), n, "{items:?}");
        }
    }
}
