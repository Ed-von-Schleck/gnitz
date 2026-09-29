use super::*;
use crate::hir::ColIdGen;
use crate::ir::BExpr;
use gnitz_wire::TypeCode;

/// A frame over `I64` columns `names` keyed by `pk_cols`, its layout minted from `ids`.
fn frame(ids: &ColIdGen, names: &[&str], pk_cols: &[u32]) -> Frame {
    let columns: Vec<ColumnDef> = names.iter().map(|n| ColumnDef::new(*n, TypeCode::I64, false)).collect();
    Frame {
        layout: columns.iter().map(|_| Some(ids.next())).collect(),
        schema: Arc::new(Schema::from_parts(columns, pk_cols.to_vec()).unwrap()),
    }
}

/// A projection naming `input`'s columns by slot; a `None` entry is a computed item.
fn projection(ids: &ColIdGen, input: &Frame, srcs: &[Option<usize>]) -> Vec<ProjEntry> {
    srcs.iter()
        .enumerate()
        .map(|(i, s)| match s {
            Some(ci) => ProjEntry {
                expr: BExpr::ColRef(input.layout[*ci].unwrap()),
                out: HirCol::new(ids.next(), input.schema.columns[*ci].clone()),
            },
            None => ProjEntry {
                expr: BExpr::LitInt(1),
                out: HirCol::new(ids.next(), ColumnDef::new(format!("_expr{i}"), TypeCode::I64, true)),
            },
        })
        .collect()
}

/// Every source PK column lands in slots `0..k` in PK order: one already there
/// stays, a later one is moved (shifting what it passes), an absent one is
/// prepended hidden with no identity, and a second reference to a PK column stays
/// a payload copy. Each slot keeps the id of the item it came from.
#[test]
#[allow(clippy::type_complexity)]
fn project_slots_pins_the_full_source_pk_to_the_leading_slots() {
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
    for (names, pk_cols, srcs, want_cols, want_items) in rows {
        let ids = ColIdGen::new();
        let input = frame(&ids, names, pk_cols);
        let items = projection(&ids, &input, srcs);
        let slots = project_slots(&items, &input).unwrap();
        let got: Vec<(&str, bool)> = slots.iter().map(|(_, _, c)| (c.name.as_str(), c.is_hidden)).collect();
        assert_eq!(got, *want_cols, "{names:?} {srcs:?}");
        let got_ids: Vec<Option<ColId>> = slots.iter().map(|(_, id, _)| *id).collect();
        let want_ids: Vec<Option<ColId>> = want_items.iter().map(|i| i.map(|i| items[i].out.id)).collect();
        assert_eq!(got_ids, want_ids, "{names:?} {srcs:?}");
        for (slot, &pk) in input.schema.pk_cols.iter().enumerate() {
            assert!(
                slots[slot].0.passthrough_src() == Some(pk as usize),
                "{names:?} {srcs:?}: slot {slot} must pass through PK column {pk}"
            );
        }
    }
}

/// A permutation of the PK is sent as the PK list, and so keyed by it; any
/// other group set is sent as written.
#[test]
fn reduce_group_sends_a_pk_permutation_as_the_pk_list() {
    use gnitz_expr::SchemaFacts;
    let ids = ColIdGen::new();
    let f = frame(&ids, &["a", "b", "c"], &[0, 1]);
    let id = |slot: usize| f.layout[slot].unwrap();
    assert_eq!(f.reduce_group(&[id(1), id(0)]).unwrap(), vec![0, 1]);
    assert_eq!(f.reduce_group(&[id(0), id(1)]).unwrap(), vec![0, 1]);
    assert_eq!(f.reduce_group(&[id(2), id(1)]).unwrap(), vec![2, 1]);
    assert_eq!(
        f.schema.reduce_out_key(&f.reduce_group(&[id(1), id(0)]).unwrap()),
        gnitz_wire::ReduceOutKey::Natural
    );
}
