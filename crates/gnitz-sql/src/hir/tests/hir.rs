use super::*;

/// A differing set-op pair promotes on the join-key ladder, and only to a target
/// a column copy can widen into.
#[test]
fn set_op_common_type_admits_what_a_column_copy_widens() {
    let common = |l: TypeCode, r: TypeCode| set_op_common_type(l.into(), r.into()).map(|t| t.tc);
    // Identical types are their own common type, even where no join key could be.
    assert_eq!(common(TypeCode::F64, TypeCode::F64), Some(TypeCode::F64));
    assert_eq!(common(TypeCode::U32, TypeCode::I32), Some(TypeCode::I64));
    assert_eq!(common(TypeCode::I32, TypeCode::I64), Some(TypeCode::I64));
    // The I128 collapse and every 16-byte / non-integer differing pair are
    // rejected: a column copy widens only into a ≤8-byte integer slot.
    assert_eq!(common(TypeCode::U64, TypeCode::I64), None);
    assert_eq!(common(TypeCode::U128, TypeCode::I64), None);
    assert_eq!(common(TypeCode::F64, TypeCode::F32), None);
    // A column copy moves bytes: it cannot turn days into microseconds, nor
    // either into a plain integer the client would read as a count.
    assert_eq!(common(TypeCode::Date, TypeCode::Timestamp), None);
    assert_eq!(common(TypeCode::Date, TypeCode::I32), None);
    assert_eq!(common(TypeCode::I64, TypeCode::Timestamp), None);
    // A DECIMAL unions only with the same DECIMAL: the stored integers of two
    // scales, or of a scale and an integer column, never mean the same value.
    assert_eq!(
        set_op_common_type(ColType::decimal(2), ColType::decimal(2)),
        Some(ColType::decimal(2))
    );
    assert_eq!(set_op_common_type(ColType::decimal(2), ColType::decimal(3)), None);
    assert_eq!(set_op_common_type(ColType::decimal(0), TypeCode::I64.into()), None);
}

/// `unique_key` reads a relation's `pk_repeats` flag and carries the key through
/// the nodes that keep a set a set: a top-N, and a join matching each row of the
/// keyed side at most once. An ALL set operation has none.
#[test]
fn unique_key_is_the_flag_carried_through_set_preserving_nodes() {
    use crate::test_support::{col, schema};
    let ids = ColIdGen::new();
    let i = TypeCode::I64;
    let get = |tid: u64, pk_repeats: bool| {
        let desc = RelDescriptor {
            tid,
            class: gnitz_wire::RelClass::View,
            pk_repeats,
            serial: false,
            schema: Arc::new(schema(vec![col("id", i), col("k", i)], &[0])),
            indexes: Vec::new(),
        };
        RelExpr::get(&ids, Arc::new(desc))
    };
    let id_of = |rel: &Rc<RelExpr>, at: usize| rel.cols()[at].id;
    let (set, other, bag) = (get(1, false), get(2, false), get(3, true));
    let (set_id, other_id) = (id_of(&set, 0), id_of(&other, 0));
    assert_eq!(set.unique_key(), Some(vec![set_id]));
    assert_eq!(bag.unique_key(), None);

    let union = |all| RelExpr::set_op(&ids, SetOpKind::Union, all, Rc::clone(&set), Rc::clone(&other)).unwrap();
    assert_eq!(union(true).unique_key(), None);
    assert_eq!(union(false).unique_key().map(|k| k.len()), Some(2));

    let top = |rel: &Rc<RelExpr>| RelExpr::top_n(Rc::clone(rel), Vec::new(), Vec::new(), 2, 0);
    assert_eq!(top(&set).unique_key(), Some(vec![set_id]));
    assert_eq!(top(&bag).unique_key(), None);

    // `left.<lcol> = right.<rcol>`, by column position.
    let join = |left: &Rc<RelExpr>, right: &Rc<RelExpr>, kind, lcol, rcol| {
        let on = BExpr::bin(
            BExpr::ColRef(id_of(left, lcol)),
            BinOp::Eq,
            BExpr::ColRef(id_of(right, rcol)),
        );
        RelExpr::join(Rc::clone(left), Rc::clone(right), kind, vec![on]).unwrap()
    };
    for (kind, lcol, rcol, want) in [
        // The right key is equated: each left row matches at most one right row.
        (JoinType::Inner, 1, 0, Some(vec![set_id])),
        (JoinType::Left, 1, 0, Some(vec![set_id])),
        // The left key is equated: the mirror.
        (JoinType::Inner, 0, 1, Some(vec![other_id])),
        (JoinType::Right, 0, 1, Some(vec![other_id])),
        // A preserved side matched many times, and a key equated on neither side.
        (JoinType::Right, 1, 0, None),
        (JoinType::Left, 0, 1, None),
        (JoinType::Full, 0, 0, None),
        (JoinType::Inner, 1, 1, None),
        // One output row per left row, whatever the right side matches.
        (JoinType::Semi, 1, 1, Some(vec![set_id])),
        (JoinType::Anti, 1, 1, Some(vec![set_id])),
    ] {
        assert_eq!(
            join(&set, &other, kind, lcol, rcol).unique_key(),
            want,
            "{kind:?} {lcol} = {rcol}"
        );
    }
    // A bag on the keyed side is no key, however it is equated.
    assert_eq!(join(&bag, &other, JoinType::Inner, 1, 0).unique_key(), None);
    assert_eq!(join(&set, &bag, JoinType::Inner, 1, 0).unique_key(), None);
}
