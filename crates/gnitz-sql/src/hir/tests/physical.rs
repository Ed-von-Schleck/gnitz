use super::*;
use crate::hir::ColIdGen;
use crate::test_support::col;
use gnitz_wire::TypeCode;

/// A frame of `[a (key) | b, c, d]` under fresh ids, and those ids.
fn frame() -> (Frame, Vec<ColId>) {
    let gen = ColIdGen::new();
    let ids: Vec<ColId> = (0..4).map(|_| gen.next()).collect();
    let slots = ids
        .iter()
        .zip(["a", "b", "c", "d"])
        .map(|(&id, name)| (Some(id), col(name, TypeCode::I64)));
    (Frame::new(slots, 1).unwrap(), ids)
}

fn names(s: &Schema) -> Vec<&str> {
    s.columns.iter().map(|c| c.name.as_str()).collect()
}

/// The root's numbering is a label over the frame's regions: the key column
/// stands where the root names it, an unnamed one leads, and an unnamed payload
/// column keeps its place among the payload.
#[test]
fn a_root_renumbers_a_frame_without_moving_a_payload_column() {
    let (f, ids) = frame();
    let (a, b, c, d) = (ids[0], ids[1], ids[2], ids[3]);
    for (root, want, key) in [
        (vec![a, b, c, d], ["a", "b", "c", "d"], 0),
        (vec![b, a, c, d], ["b", "a", "c", "d"], 1),
        (vec![b, c, d, a], ["b", "c", "d", "a"], 3),
        // No item names the key.
        (vec![b, c, d], ["a", "b", "c", "d"], 0),
        // An unnamed payload column between two named ones, and behind them.
        (vec![b, a, d], ["b", "a", "c", "d"], 1),
        (vec![a, b], ["a", "b", "c", "d"], 0),
        (vec![d, a], ["b", "c", "d", "a"], 3),
    ] {
        let s = f.schema_in_order(root.iter().copied()).unwrap().0;
        assert_eq!((names(&s), &s.pk_cols[..]), (want.to_vec(), &[key][..]), "{root:?}");
        assert!(s.same_region_types(f.schema.as_ref()), "{root:?}");
    }
}

/// A root that exchanges two payload columns, or names one twice, is refused:
/// it would be another layout, which only a projection can produce.
#[test]
fn a_root_moving_a_payload_column_is_refused() {
    let (f, ids) = frame();
    for root in [vec![ids[2], ids[1]], vec![ids[0], ids[3], ids[1]], vec![ids[1], ids[1]]] {
        assert!(f.schema_in_order(root.iter().copied()).is_err(), "{root:?}");
    }
}
