use super::*;
use crate::hir::{ColIdGen, EqPair};
use gnitz_core::TypeCode;

/// A `Frame` over `U64` columns named `(name, nullable)`, its layout minted
/// from `ids`.
fn frame(ids: &ColIdGen, names: &[(&str, bool)], pk_cols: Vec<u32>) -> Frame {
    let cols: Vec<ColumnDef> = names
        .iter()
        .map(|&(n, nullable)| ColumnDef::new(n, TypeCode::U64, nullable))
        .collect();
    Frame {
        layout: cols.iter().map(|_| ids.next()).collect(),
        schema: Arc::new(Schema::from_parts(cols, pk_cols).unwrap()),
    }
}

fn class(eq: Vec<EqPair>, range: Option<super::super::HirRange>) -> JoinClass {
    JoinClass { eq, range }
}

const NO_DEMAND: Demand<'static> = Demand { items: &[], where_preds: &[] };

/// A shape packing its output key out of the payload keeps that side's PK alone,
/// at the front of the keep list: the ν fallback must not add a second identity.
#[test]
fn a_pinned_side_keeps_its_pk_alone() {
    let ids = ColIdGen::new();
    let left = frame(&ids, &[("v", false), ("id", false)], vec![1]);
    let right = frame(&ids, &[("id", false), ("w", false)], vec![0]);
    let range = super::super::HirRange {
        left: left.layout[0],
        right: right.layout[1],
        op: gnitz_core::RangeRel::Lt,
        tc: TypeCode::U64,
    };
    let cls = class(Vec::new(), Some(range));
    // A range conjunct alone: the key columns are not kept by the demand rules,
    // only the pinned PKs are.
    let sides = join_sides(NO_DEMAND, &cls, JoinType::Inner, [left, right]);
    assert_eq!((sides[0].keep.as_slice(), sides[0].pa()), (&[1u32][..], 1));
    assert_eq!((sides[1].keep.as_slice(), sides[1].pa()), (&[0u32][..], 1));
}

/// An equi join nothing reads from still emits rows, so the left side keeps one
/// column as its trace identity — and the right, having no ν and no demand,
/// keeps none.
#[test]
fn an_unreferenced_equi_join_keeps_one_left_column() {
    let ids = ColIdGen::new();
    let left = frame(&ids, &[("k", false), ("v", false)], vec![0]);
    let right = frame(&ids, &[("k", false), ("w", false)], vec![0]);
    let cls = class(
        vec![EqPair {
            left: left.layout[0],
            right: right.layout[0],
            tc: TypeCode::U64,
        }],
        None,
    );
    let sides = join_sides(NO_DEMAND, &cls, JoinType::Inner, [left, right]);
    assert_eq!((sides[0].keep.as_slice(), sides[0].pa()), (&[0u32][..], 0));
    assert!(sides[1].keep.is_empty());
}

/// A LEFT join's preserved side keeps its nullable equi key: `map_reindex`
/// collapses a NULL key to synthetic PK 0, colliding with a real `k = 0` row.
#[test]
fn a_preserved_side_keeps_its_nullable_key() {
    let ids = ColIdGen::new();
    let left = frame(&ids, &[("id", false), ("k", true)], vec![0]);
    let right = frame(&ids, &[("id", false), ("k", true)], vec![0]);
    let cls = class(
        vec![EqPair {
            left: left.layout[1],
            right: right.layout[1],
            tc: TypeCode::U64,
        }],
        None,
    );
    let sides = join_sides(NO_DEMAND, &cls, JoinType::Left, [left, right]);
    // Left has a ν and a nullable key at slot 1: that key, not the Rule 5 fallback.
    assert_eq!(sides[0].keep.as_slice(), &[1u32][..]);
    // The right side has no ν, so its equally-nullable key is not protected.
    assert!(sides[1].keep.is_empty());
}

/// A band ν is keyed by the source PK, so it keeps every key column; an equi ν
/// only a nullable one.
#[test]
fn a_band_nu_keeps_its_key_columns() {
    let ids = ColIdGen::new();
    let left = frame(
        &ids,
        &[("id", false), ("k", false), ("x", false), ("v", false)],
        vec![0],
    );
    let right = frame(&ids, &[("id", false), ("k", false), ("y", false)], vec![0]);
    let eq = vec![EqPair {
        left: left.layout[1],
        right: right.layout[1],
        tc: TypeCode::U64,
    }];
    let range = super::super::HirRange {
        left: left.layout[2],
        right: right.layout[2],
        op: gnitz_core::RangeRel::Le,
        tc: TypeCode::U64,
    };
    let sides = join_sides(
        NO_DEMAND,
        &class(eq.clone(), Some(range)),
        JoinType::Left,
        [left.clone(), right.clone()],
    );
    // The pinned PK, then both key columns; the payload `v` nothing reads is not kept.
    assert_eq!(sides[0].keep.as_slice(), &[0u32, 1, 2][..]);
    let sides = join_sides(NO_DEMAND, &class(eq, None), JoinType::Left, [left, right]);
    // An equi ν over a NOT NULL key keeps nothing past the Rule 5 fallback.
    assert_eq!(sides[0].keep.as_slice(), &[0u32][..]);
}
