use super::*;
use crate::hir::{ColIdGen, KeyPair};
use crate::test_support::{col, schema};
use ExistsKind::{Anti, Mark, Semi};
use JoinPlan::{Band, Cross, Equi, PureRange};
use JoinType::{Exists, Full, Inner, Left, Right};
use gnitz_wire::RangeRel::Le;
use std::sync::Arc;

/// Four `U64` columns keyed on the slots `pk`, their ids minted from `ids`.
fn frame(ids: &ColIdGen, names: [&str; 4], pk: &[u32]) -> Frame {
    Frame {
        layout: names.map(|_| Some(ids.next())).to_vec(),
        schema: Arc::new(schema(names.map(|n| col(n, TypeCode::U64)).to_vec(), pk)),
    }
}

/// Every `join_keep` rule over left `(v, id, k, x)` keyed on `id` and right
/// `(id, k, y, w)` keyed on `(w, id)`, joined by `l.k = r.k` (Equi), `l.x <= r.y`
/// (PureRange), both (Band), or neither (Cross). `reads` are the `(side, slot)`s the
/// levels above the join read; a Mark join's also read its mark column.
#[test]
fn join_keep_rules() {
    let ids = ColIdGen::new();
    let frames = [
        frame(&ids, ["v", "id", "k", "x"], &[1]),
        frame(&ids, ["id", "k", "y", "w"], &[3, 0]),
    ];
    let mark = ids.next();
    let col = |side: usize, slot: usize| frames[side].layout[slot].unwrap();
    let eq = KeyPair {
        cols: [col(0, 2), col(1, 1)],
        ty: TypeCode::U64.into(),
    };
    let range = KeyPair {
        cols: [col(0, 3), col(1, 2)],
        ty: TypeCode::U64.into(),
    };
    /// The plan, the kind, what is read above the join, and each side's keep list,
    /// a pinned PK at its front.
    type Case = (JoinPlan, JoinType, &'static [(usize, usize)], [&'static [u32]; 2]);
    #[rustfmt::skip]
    let cases: &[Case] = &[
        // Rule 4: a pair-PK side keeps its PK, in PK-list order; an INNER keeps no key
        // for a ν it lacks.
        (Band(Le),  Inner, &[], [&[1], &[3, 0]]),
        (PureRange(Le), Inner, &[], [&[1], &[3, 0]]),
        (Cross, Inner, &[], [&[1], &[3, 0]]),
        // Rule 3: each side with a ν keeps its keys, behind its pinned PK.
        (Band(Le),  Left,  &[], [&[1, 2, 3], &[3, 0]]),
        (Band(Le),  Right, &[], [&[1], &[3, 0, 1, 2]]),
        (Band(Le),  Full,  &[], [&[1, 2, 3], &[3, 0, 1, 2]]),
        (PureRange(Le), Left,  &[], [&[1, 3], &[3, 0]]),
        // A decorrelated kind has a ν over its left and pins that side alone.
        (Band(Le),  Exists(Semi),  &[], [&[1, 2, 3], &[]]),
        (PureRange(Le), Exists(Anti),  &[], [&[1, 3], &[]]),
        // The pinned PK leads even ahead of a lower slot read above, and appears once.
        (Band(Le),  Left,  &[(0, 0), (0, 1)], [&[1, 0, 2, 3], &[3, 0]]),
        (Band(Le),  Left,  &[(1, 1)], [&[1, 2, 3], &[3, 0, 1]]),
        // An equi side keeps only what is read above the join; its ν is a linear difference.
        (Equi, Left,  &[], [&[], &[]]),
        (Equi, Right, &[], [&[], &[]]),
        (Equi, Full,  &[], [&[], &[]]),
        (Equi, Exists(Mark(mark)), &[], [&[], &[]]),
        (Equi, Exists(Mark(mark)), &[(0, 3)], [&[3], &[]]),
        (Equi, Left,  &[(0, 3)], [&[3], &[]]),
        (Equi, Inner, &[(1, 3)], [&[], &[3]]),
        (Equi, Inner, &[], [&[], &[]]),
    ];
    for &(plan, kind, reads, want) in cases {
        let class = JoinClass {
            eq: matches!(plan, Equi | Band(_)).then_some(eq).into_iter().collect(),
            range: matches!(plan, Band(_) | PureRange(_)).then_some((range, Le)),
        };
        class.admit(kind).unwrap();
        assert_eq!(class.plan(), plan);
        let refs: ColSet = reads
            .iter()
            .map(|&(side, slot)| col(side, slot))
            .chain(matches!(kind, Exists(Mark(_))).then_some(mark))
            .collect();
        let got = join_keep(&refs, &class, kind, out_key(plan, kind), frames.each_ref()).unwrap();
        let got = got.each_ref().map(Vec::as_slice);
        assert_eq!(got, want, "{plan:?} {kind:?} reads={reads:?}");
    }
}
