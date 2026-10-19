use super::*;
use crate::hir::ColIdGen;
use crate::test_support::col;
use gnitz_wire::TypeCode;

/// The root's numbering is a label over the frame `[a b c d]`'s regions, keyed on
/// its leading `npk` columns: the key columns stand where the root names them in
/// PK-list order, an unnamed one leads, and an unnamed payload column keeps its
/// place among the payload. A root that exchanges two payload columns, or names
/// one twice, is refused: it would be another layout, which only a projection can
/// produce. Each row is `(npk, root, (shown, key))`.
#[test]
fn a_root_renumbers_a_frame_without_moving_a_payload_column() {
    let names = ["a", "b", "c", "d"];
    #[rustfmt::skip]
    let cases = [
        (1, "a b c d", Some(("a b c d", "a"))),
        (1, "b a c d", Some(("b a c d", "a"))),
        (1, "b c d a", Some(("b c d a", "a"))),
        // No item names the key.
        (1, "b c d", Some(("a b c d", "a"))),
        // An unnamed payload column between two named ones, and behind them.
        (1, "b a d", Some(("b a c d", "a"))),
        (1, "a b", Some(("a b c d", "a"))),
        (1, "d a", Some(("b c d a", "a"))),
        // A compound key keeps its PK-list order wherever its columns stand.
        (2, "b a c d", Some(("b a c d", "a b"))),
        (2, "c b", Some(("a c b d", "a b"))),
        (1, "c b", None),
        (1, "a d b", None),
        (1, "b b", None),
    ];
    for (npk, root, want) in cases {
        let ids = ColIdGen::new();
        let f = Frame::new(names.map(|n| (Some(ids.next()), col(n, TypeCode::I64))), npk).unwrap();
        let id = |n: &str| f.layout[names.iter().position(|&m| m == n).unwrap()].unwrap();
        let got = f.schema_in_order(root.split(' ').map(id)).ok().map(|(s, _)| {
            assert!(s.layout().same_region_types(f.schema.layout()), "{root}");
            let name = |c: usize| s.columns()[c].name.as_str();
            let shown: Vec<&str> = (0..s.columns().len()).map(name).collect();
            let key: Vec<&str> = s.pk_cols().iter().map(|&c| name(c as usize)).collect();
            (shown.join(" "), key.join(" "))
        });
        assert_eq!(
            got,
            want.map(|(s, k)| (s.to_string(), k.to_string())),
            "npk={npk} root={root}"
        );
    }
}
