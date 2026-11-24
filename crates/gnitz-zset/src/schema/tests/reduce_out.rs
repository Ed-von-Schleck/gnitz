use super::*;
use crate::test_support::keyed_schema;

fn col(tc: TypeCode) -> SchemaColumn {
    SchemaColumn::new(tc, false)
}

/// The reduce output key: the source PK list keys on itself, and so does a
/// single NOT NULL PK-eligible column (eligibility is `TypeCode::is_pk_eligible`'s,
/// swept there); anything else — no column, a nullable, float or string column,
/// a partial, reordered or wider set — folds into a synthetic key. Each schema
/// has three columns, the leading `pk` of them `U64 NOT NULL` and the rest of
/// the case's type.
#[test]
fn natural_key_picks_the_output_key() {
    use TypeCode as T;
    let (natural, synthetic_fold) = (true, false);
    // (pk, group, (non-PK type code, nullable), natural)
    let rows = [
        (1, &[][..], (T::U64, false), synthetic_fold),
        (1, &[1], (T::U64, true), synthetic_fold),
        (1, &[1], (T::U64, false), natural),
        (1, &[1], (T::F64, false), synthetic_fold),
        (1, &[1], (T::String, false), synthetic_fold),
        (1, &[0], (T::String, true), natural),
        (2, &[0, 1], (T::String, true), natural),
        (2, &[1, 0], (T::String, true), synthetic_fold),
        (2, &[0], (T::String, true), natural),
        (2, &[0, 1, 2], (T::U64, false), synthetic_fold),
        (2, &[1, 2], (T::U64, false), synthetic_fold),
    ];
    for (pk, group, (tc, nullable), want) in rows {
        let schema = keyed_schema(&vec![T::U64; pk], &vec![SchemaColumn::new(tc, nullable); 3 - pk]);
        assert_eq!(natural_key(&schema, group), want, "{pk} {group:?} {tc:?} {nullable}");
    }
}

#[test]
fn reduce_out_layout_is_the_key_region_then_the_unspelled_row() {
    use ReduceOutSlot::{Carried, Key, SyntheticKey};
    let schema = |c2_nullable: bool, pk: &[u32]| {
        let u64_col = |nullable| SchemaColumn::new(TypeCode::U64, nullable);
        SchemaDescriptor::new(
            &[col(TypeCode::U64), u64_col(true), u64_col(c2_nullable), u64_col(true)],
            pk,
        )
    };
    // A nullable column takes the fold key, which spells no input column, so the whole row
    // rides behind it — group columns included, since the synthetic key is not one of them.
    let fold = schema(true, &[0]);
    assert_eq!(
        reduce_out_layout(&fold, &[2], 0..4),
        vec![SyntheticKey, Carried(0), Carried(1), Carried(2), Carried(3)]
    );
    // A fold's row is its group set, a repeated column carried once per occurrence.
    assert_eq!(
        reduce_out_layout(&fold, &[1, 1], [1, 1]),
        vec![SyntheticKey, Carried(1), Carried(1)]
    );
    // A natural key column is in the PK region, so it is not carried again.
    assert_eq!(
        reduce_out_layout(&schema(false, &[0]), &[2], 0..4),
        vec![Key(2), Carried(0), Carried(1), Carried(3)]
    );
    // The PK list keys on every PK column.
    assert_eq!(
        reduce_out_layout(&schema(false, &[0, 2]), &[0, 2], 0..4),
        vec![Key(0), Key(2), Carried(1), Carried(3)]
    );
}
