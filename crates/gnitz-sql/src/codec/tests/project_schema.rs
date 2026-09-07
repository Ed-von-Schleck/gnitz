use super::*;
use gnitz_core::TypeCode;

fn schema(names: &[&str], pk_cols: &[u32]) -> Schema {
    Schema {
        columns: names.iter().map(|n| ColumnDef::new(*n, TypeCode::I64, false)).collect(),
        pk_cols: pk_cols.to_vec(),
    }
}

/// `items`/`out_cols` for a projection naming source columns by index; a
/// `None` entry is a computed item.
fn projection(schema: &Schema, srcs: &[Option<usize>]) -> (Vec<ProjItem>, Vec<ColumnDef>) {
    let items = srcs
        .iter()
        .map(|s| match s {
            Some(ci) => ProjItem::PassThrough { src_col: *ci },
            None => ProjItem::Computed { bound_expr: BoundExpr::ColRef(0) },
        })
        .collect();
    let cols = srcs
        .iter()
        .enumerate()
        .map(|(i, s)| match s {
            Some(ci) => schema.columns[*ci].clone(),
            None => ColumnDef::new(format!("_expr{i}"), TypeCode::I64, true),
        })
        .collect();
    (items, cols)
}

/// Every source PK column lands in slots `0..k` in PK order: one already there
/// stays, a later one is moved (shifting what it passes), an absent one is
/// prepended hidden, and a second reference to a PK column stays a payload
/// copy. The returned permutation maps each output slot to its former index.
#[test]
#[allow(clippy::type_complexity)]
fn place_pk_front_pins_the_full_source_pk_to_the_leading_slots() {
    let rows: &[(&[&str], &[u32], &[Option<usize>], &[(&str, bool)], &[Option<usize>])] = &[
        (
            &["name", "age", "id"],
            &[2],
            &[Some(0), Some(1), Some(2)],
            &[("id", false), ("name", false), ("age", false)],
            &[Some(2), Some(0), Some(1)],
        ),
        (
            &["id", "name"],
            &[0],
            &[Some(0), Some(1)],
            &[("id", false), ("name", false)],
            &[Some(0), Some(1)],
        ),
        (
            &["id", "name"],
            &[0],
            &[Some(0), Some(0), Some(1)],
            &[("id", false), ("id", false), ("name", false)],
            &[Some(0), Some(1), Some(2)],
        ),
        (
            &["a", "b", "c"],
            &[0, 1],
            &[Some(1), Some(2), Some(0)],
            &[("a", false), ("b", false), ("c", false)],
            &[Some(2), Some(0), Some(1)],
        ),
        (
            &["a", "b", "c"],
            &[0, 1],
            &[Some(0), Some(2)],
            &[("a", false), ("b", true), ("c", false)],
            &[Some(0), None, Some(1)],
        ),
        (
            &["a", "b", "c"],
            &[0, 1],
            &[Some(2)],
            &[("a", true), ("b", true), ("c", false)],
            &[None, None, Some(0)],
        ),
        (
            &["id", "name"],
            &[0],
            &[None, Some(1)],
            &[("id", true), ("_expr0", false), ("name", false)],
            &[None, Some(0), Some(1)],
        ),
    ];
    for (names, pk_cols, srcs, want_cols, want_perm) in rows {
        let s = schema(names, pk_cols);
        let (mut items, mut cols) = projection(&s, srcs);
        let perm = place_pk_front(&mut items, &mut cols, &s);
        let got: Vec<(&str, bool)> = cols.iter().map(|c| (c.name.as_str(), c.is_hidden)).collect();
        assert_eq!(got, *want_cols, "{names:?} {srcs:?}");
        assert_eq!(perm, *want_perm, "{names:?} {srcs:?}");
        for (slot, &pk) in s.pk_cols.iter().enumerate() {
            assert!(
                items[slot].passthrough_src() == Some(pk as usize),
                "{names:?} {srcs:?}: slot {slot} must pass through PK column {pk}"
            );
        }
    }
}
