use super::*;
use crate::hir::{ColIdGen, EqPair, HirRange};
use gnitz_core::Schema;
use gnitz_wire::RangeRel;
use std::sync::Arc;
use JoinShape::{Band, Cross, Equi, PureRange};
use JoinType::{Anti, Full, Inner, Left, Mark, Right, Semi};

/// Four `U64` columns keyed on slot `pk`, their ids minted from `ids`.
fn frame(ids: &ColIdGen, names: [&str; 4], pk: u32) -> Frame {
    let cols: Vec<ColumnDef> = names.iter().map(|&n| ColumnDef::new(n, TypeCode::U64, false)).collect();
    Frame {
        layout: cols.iter().map(|_| Some(ids.next())).collect(),
        schema: Arc::new(Schema::from_parts(cols, &[pk]).unwrap()),
    }
}

/// Every `join_keep` rule over left `(v, id, k, x)` keyed on `id` and right
/// `(id, k, y, w)` keyed on `id`, joined by `l.k = r.k` (Equi), `l.x <= r.y`
/// (PureRange), both (Band), or neither (Cross). `reads` are the `(side, slot)`s the
/// demand reads; a Mark join's demand also reads its mark column.
#[test]
fn join_keep_rules() {
    const MARK: ColId = ColId(u32::MAX);
    /// The shape, the kind, the demand's reads, and each side's keep list and
    /// pinned PK arity.
    type Case = (
        JoinShape,
        JoinType,
        &'static [(usize, usize)],
        [(&'static [u32], usize); 2],
    );
    #[rustfmt::skip]
    let cases: &[Case] = &[
        // Rule 4: a pair-PK side keeps its PK; an INNER keeps no key for a ν it lacks.
        (Band,  Inner, &[], [(&[1], 1), (&[0], 1)]),
        (PureRange, Inner, &[], [(&[1], 1), (&[0], 1)]),
        (Cross, Inner, &[], [(&[1], 1), (&[0], 1)]),
        // Rule 3: each side with a ν keeps its keys, behind its pinned PK.
        (Band,  Left,  &[], [(&[1, 2, 3], 1), (&[0], 1)]),
        (Band,  Right, &[], [(&[1], 1), (&[0, 1, 2], 1)]),
        (Band,  Full,  &[], [(&[1, 2, 3], 1), (&[0, 1, 2], 1)]),
        (PureRange, Left,  &[], [(&[1, 3], 1), (&[0], 1)]),
        // A decorrelated kind has a ν over its left and pins that side alone.
        (Band,  Semi,  &[], [(&[1, 2, 3], 1), (&[], 0)]),
        (PureRange, Anti,  &[], [(&[1, 3], 1), (&[], 0)]),
        // The pinned PK leads even ahead of a lower demanded slot, and appears once.
        (Band,  Left,  &[(0, 0), (0, 1)], [(&[1, 0, 2, 3], 1), (&[0], 1)]),
        // Rule 5: a side with a ν that keeps nothing keeps column 0; one without keeps nothing.
        (Equi, Left,  &[], [(&[0], 0), (&[], 0)]),
        (Equi, Right, &[], [(&[], 0), (&[0], 0)]),
        (Equi, Full,  &[], [(&[0], 0), (&[0], 0)]),
        (Equi, Mark(MARK), &[], [(&[0], 0), (&[], 0)]),
        (Equi, Left,  &[(0, 3)], [(&[3], 0), (&[], 0)]),
        // Rules 1 + 2 decide alone; a join reading nothing keeps left column 0.
        (Equi, Inner, &[(1, 3)], [(&[], 0), (&[3], 0)]),
        (Equi, Inner, &[], [(&[0], 0), (&[], 0)]),
    ];
    for &(shape, kind, reads, want) in cases {
        let (eq, range) = match shape {
            Equi => (true, false),
            Band => (true, true),
            PureRange => (false, true),
            Cross => (false, false),
        };
        let ids = ColIdGen::new();
        let frames = [
            frame(&ids, ["v", "id", "k", "x"], 1),
            frame(&ids, ["id", "k", "y", "w"], 0),
        ];
        let col = |side: usize, slot: usize| frames[side].layout[slot].unwrap();
        let class = JoinClass {
            eq: match eq {
                true => vec![EqPair {
                    left: col(0, 2),
                    right: col(1, 1),
                    tc: TypeCode::U64,
                }],
                false => Vec::new(),
            },
            range: range.then(|| HirRange {
                left: col(0, 3),
                right: col(1, 2),
                op: RangeRel::Le,
                tc: TypeCode::U64,
            }),
        };
        let preds: Vec<HirExpr> = reads
            .iter()
            .map(|&(side, slot)| col(side, slot))
            .chain(matches!(kind, Mark(_)).then_some(MARK))
            .map(BExpr::ColRef)
            .collect();
        let down = Demand { items: &[], where_preds: &preds };
        let got = join_keep(down, &class, kind, &frames).unwrap();
        let got = got.each_ref().map(|(keep, pa)| (keep.as_slice(), *pa));
        assert_eq!(got, want, "{shape:?} {kind:?} reads={reads:?}");
    }
}
