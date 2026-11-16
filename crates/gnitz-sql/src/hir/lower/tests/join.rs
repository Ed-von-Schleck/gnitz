use super::*;
use crate::hir::{ColIdGen, KeyPair};
use crate::test_support::{col, schema};
use JoinType::{Anti, Full, Inner, Left, Mark, Right, Semi};
use Shape::{Band, Cross, Equi, PureRange};
use gnitz_wire::RangeRel;
use std::sync::Arc;

/// Four `U64` columns keyed on the slots `pk`, their ids minted from `ids`.
fn frame(ids: &ColIdGen, names: [&str; 4], pk: &[u32]) -> Frame {
    Frame {
        layout: names.map(|_| Some(ids.next())).to_vec(),
        schema: Arc::new(schema(names.map(|n| col(n, TypeCode::U64)).to_vec(), pk)),
    }
}

/// Which keys a `join_keep_rules` case joins on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Equi,
    Band,
    PureRange,
    Cross,
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
    /// The shape, the kind, what is read above the join, and each side's keep list and
    /// pinned PK arity.
    type Case = (Shape, JoinType, &'static [(usize, usize)], [(&'static [u32], usize); 2]);
    #[rustfmt::skip]
    let cases: &[Case] = &[
        // Rule 4: a pair-PK side keeps its PK, in PK-list order; an INNER keeps no key
        // for a ν it lacks.
        (Band,  Inner, &[], [(&[1], 1), (&[3, 0], 2)]),
        (PureRange, Inner, &[], [(&[1], 1), (&[3, 0], 2)]),
        (Cross, Inner, &[], [(&[1], 1), (&[3, 0], 2)]),
        // Rule 3: each side with a ν keeps its keys, behind its pinned PK.
        (Band,  Left,  &[], [(&[1, 2, 3], 1), (&[3, 0], 2)]),
        (Band,  Right, &[], [(&[1], 1), (&[3, 0, 1, 2], 2)]),
        (Band,  Full,  &[], [(&[1, 2, 3], 1), (&[3, 0, 1, 2], 2)]),
        (PureRange, Left,  &[], [(&[1, 3], 1), (&[3, 0], 2)]),
        // A decorrelated kind has a ν over its left and pins that side alone.
        (Band,  Semi,  &[], [(&[1, 2, 3], 1), (&[], 0)]),
        (PureRange, Anti,  &[], [(&[1, 3], 1), (&[], 0)]),
        // The pinned PK leads even ahead of a lower slot read above, and appears once.
        (Band,  Left,  &[(0, 0), (0, 1)], [(&[1, 0, 2, 3], 1), (&[3, 0], 2)]),
        (Band,  Left,  &[(1, 1)], [(&[1, 2, 3], 1), (&[3, 0, 1], 2)]),
        // An equi side keeps only what is read above the join; its ν is a linear difference.
        (Equi, Left,  &[], [(&[], 0), (&[], 0)]),
        (Equi, Right, &[], [(&[], 0), (&[], 0)]),
        (Equi, Full,  &[], [(&[], 0), (&[], 0)]),
        (Equi, Mark(mark), &[], [(&[], 0), (&[], 0)]),
        (Equi, Mark(mark), &[(0, 3)], [(&[3], 0), (&[], 0)]),
        (Equi, Left,  &[(0, 3)], [(&[3], 0), (&[], 0)]),
        (Equi, Inner, &[(1, 3)], [(&[], 0), (&[3], 0)]),
        (Equi, Inner, &[], [(&[], 0), (&[], 0)]),
    ];
    for &(shape, kind, reads, want) in cases {
        let class = JoinClass {
            eq: matches!(shape, Equi | Band).then_some(eq).into_iter().collect(),
            range: matches!(shape, Band | PureRange).then_some((range, RangeRel::Le)),
        };
        class.admit(kind).unwrap();
        let plan = class.plan();
        let planned = match plan {
            JoinPlan::Equi => Equi,
            JoinPlan::Band(_) => Band,
            JoinPlan::PureRange(..) => PureRange,
            JoinPlan::Cross => Cross,
        };
        assert_eq!(planned, shape);
        let refs: ColSet = reads
            .iter()
            .map(|&(side, slot)| col(side, slot))
            .chain(matches!(kind, Mark(_)).then_some(mark))
            .collect();
        let got = join_keep(&refs, &class, kind, out_key(plan, kind), &frames).unwrap();
        let got = got.each_ref().map(|(keep, pa)| (keep.as_slice(), *pa));
        assert_eq!(got, want, "{shape:?} {kind:?} reads={reads:?}");
    }
}
