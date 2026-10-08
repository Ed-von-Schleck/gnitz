use super::*;
use crate::test_support::{
    bind_conjunct, bind_where, col, compound_schema_u64_u64, ix, pk_schema, schema, two_col, uq,
};
use gnitz_wire::{ColType, ColumnDef, PkBuf};
use Tier as T;

const U64_MAX: u128 = u64::MAX as u128;
const UUID_LIT: &str = "'550e8400-e29b-41d4-a716-446655440000'";
const UUID_VALUE: u128 = 0x550e8400_e29b_41d4_a716_446655440000;

/// `v`'s key image in a `tc` column.
fn img(tc: TypeCode, v: i128) -> u128 {
    key_image(tc, v as u128)
}

/// One candidate: its tier, its bound, and the conjuncts (by position) it leaves
/// residual.
type Ranked = (Tier, ReadBound, Vec<usize>);

/// Every candidate `sql` admits over `schema` and `indexes`, best first.
fn ranked_of(sql: &str, schema: &Schema, indexes: &[RelIndex]) -> Vec<Ranked> {
    ranked_conjuncts(&bind_where(sql, schema), schema, indexes)
}

fn ranked_conjuncts(conjuncts: &[BoundExpr], schema: &Schema, indexes: &[RelIndex]) -> Vec<Ranked> {
    ranked(conjuncts, schema, indexes)
        .into_iter()
        .map(|(tier, c)| {
            let left = (0..conjuncts.len()).filter(|i| !c.consumed.contains(i)).collect();
            (tier, c.bound, left)
        })
        .collect()
}

/// The range over the key list `cols` with `eq` pinned and the next column's
/// image in `lo..=hi`.
fn rng(cols: &[u32], eq: &[u128], lo: u128, hi: u128) -> ReadBound {
    ReadBound::Range(KeyRange::new(
        PkColList::from_slice(cols),
        eq,
        Cut::before(lo),
        Cut::after(hi),
    ))
}

/// The key set of `keys`, each the PK columns' native values in PK-list order.
fn keys(schema: &Schema, keys: &[&[u128]]) -> ReadBound {
    let bufs: Vec<PkBuf> = keys.iter().map(|k| schema.layout().opk_key_cols(k)).collect();
    ReadBound::PkSet(PkKeys::from_keys(
        schema.layout().pk_stride(),
        bufs.iter().map(|b| b.pk_bytes()),
    ))
}

/// `(id U64 pk, a tc, b tc)`, `b` nullable per the flag.
fn schema3(tc: TypeCode, b_nullable: bool) -> Schema {
    schema(
        vec![
            col("id", TypeCode::U64),
            col("a", tc),
            ColumnDef::new("b", tc, b_nullable),
        ],
        &[0],
    )
}

/// `(id U64 pk, x, a, b, c)`, every column U64 NOT NULL.
fn five_u64() -> Schema {
    schema(["id", "x", "a", "b", "c"].map(|n| col(n, TypeCode::U64)).to_vec(), &[0])
}

// ------------------------------------------------------------------
// One conjunct, one pin
// ------------------------------------------------------------------

/// The images `sql` admits on the `val` column of a `(pk, val tc)` table; `None`
/// when the conjunct is no term.
fn pin(tc: TypeCode, sql: &str) -> Option<RangeInclusive<u128>> {
    let schema = two_col(tc);
    match term(&bind_conjunct(sql, &schema), &schema)? {
        (1, Pin::Range(r)) => Some(r),
        (col, _) => panic!("{sql}: a pin on column {col}"),
    }
}

/// Each comparison against a literal as the interval of key images it admits: the
/// literal on either side, a value outside the type saturating, one between two
/// values cutting at its neighbour. A conjunct of any other shape, or over a
/// column with no key image, is no term.
#[test]
fn a_comparison_pins_the_images_it_admits() {
    use TypeCode::*;
    let max = |tc: TypeCode| image_max(tc).unwrap();
    let at = |tc, v| img(tc, v)..=img(tc, v);
    let uuid_eq = format!("val = {UUID_LIT}");
    let rows: Vec<(TypeCode, &str, Option<RangeInclusive<u128>>)> = vec![
        // Each operator, in both operand orders.
        (I64, "val = 5", Some(at(I64, 5))),
        (I64, "5 = t.val", Some(at(I64, 5))),
        (I64, "val >= 5", Some(img(I64, 5)..=max(I64))),
        (I64, "val > 5", Some(img(I64, 6)..=max(I64))),
        (I64, "5 < val", Some(img(I64, 6)..=max(I64))),
        (I64, "val <= 5", Some(0..=img(I64, 5))),
        (I64, "5 >= val", Some(0..=img(I64, 5))),
        (I64, "val < 5", Some(0..=img(I64, 4))),
        // A signed value packs at the column's own width.
        (I64, "val = -1", Some(at(I64, -1))),
        (I32, "val = -1", Some(at(I32, -1))),
        (I16, "val = -1", Some(at(I16, -1))),
        (I8, "val = -5", Some(at(I8, -5))),
        (I32, "val >= -5", Some(img(I32, -5)..=max(I32))),
        // Past `i64` the literal is wide, and still packs exactly.
        (U64, "val = 18446744073709551615", Some(U64_MAX..=U64_MAX)),
        (
            U128,
            "val >= 340282366920938463463374607431768211455",
            Some(u128::MAX..=u128::MAX),
        ),
        (U128, "val > 340282366920938463463374607431768211455", Some(NOTHING)),
        (UUID, &uuid_eq, Some(UUID_VALUE..=UUID_VALUE)),
        // The strict operators at the type's edges admit nothing.
        (I32, "val > 2147483647", Some(NOTHING)),
        (I32, "val < -2147483648", Some(NOTHING)),
        (U64, "val > 18446744073709551615", Some(NOTHING)),
        // A literal outside the type admits nothing or everything.
        (U64, "val = -1", Some(NOTHING)),
        (I32, "val >= 3000000000", Some(NOTHING)),
        (I32, "val <= 3000000000", Some(0..=max(I32))),
        (I32, "val >= -3000000000", Some(0..=max(I32))),
        (I32, "val < -3000000000", Some(NOTHING)),
        (I32, "val <= -340282366920938463463374607431768211455", Some(NOTHING)),
        (U8, "val < 300", Some(0..=255)),
        (U128, "val >= -5", Some(0..=u128::MAX)),
        // A literal between two values cuts at the neighbour it admits.
        (I32, "val > 1.5", Some(img(I32, 2)..=max(I32))),
        (I32, "val < 1.5", Some(0..=img(I32, 1))),
        // No term: a float column, `<>`, a computed operand, a negated range, and
        // a list on a column that is no PK column.
        (F64, "val = 1", None),
        (F64, "val < 5", None),
        (I64, "val <> 5", None),
        (I64, "val + 1 > 3", None),
        (I64, "val NOT BETWEEN 1 AND 2", None),
        (I64, "val IN (1, 2)", None),
    ];
    for (tc, sql, want) in rows {
        assert_eq!(pin(tc, sql), want, "{tc:?}: {sql}");
    }
}

// ------------------------------------------------------------------
// The candidate list
// ------------------------------------------------------------------

/// Assert the whole candidate list of each `(indexes, sql)` over `schema`.
fn check(schema: &Schema, rows: Vec<(Vec<RelIndex>, &str, Vec<Ranked>)>) {
    for (indexes, sql, want) in rows {
        assert_eq!(ranked_of(sql, schema, &indexes), want, "{sql} over {indexes:?}");
    }
}

/// A WHERE that names whole keys gathers them, however the point or the list is
/// spelled; whatever does not bind the PK stays residual.
#[test]
fn a_where_naming_whole_keys_is_a_key_set() {
    let s = pk_schema(TypeCode::U64);
    let one = |sql, key: u128, left: Vec<usize>| (vec![], sql, vec![(T::PkKeys, keys(&s, &[&[key]]), left)]);
    check(
        &s,
        vec![
            one("id = 5", 5, vec![]),
            one("t.id = 5", 5, vec![]),
            one("id >= 5 AND id <= 5", 5, vec![]),
            one("id > 4 AND id <= 5", 5, vec![]),
            one("id >= 5 AND id < 6", 5, vec![]),
            one("id >= 18446744073709551615", U64_MAX, vec![]),
            one("id > 18446744073709551614", U64_MAX, vec![]),
            one("id <= 0", 0, vec![]),
            one("id < 1", 0, vec![]),
            one("id = 1 AND v = 9", 1, vec![1]),
            (
                vec![],
                "v > 5 AND id IN (7, 9)",
                vec![(T::PkKeys, keys(&s, &[&[7], &[9]]), vec![0])],
            ),
            // A NULL member admits no row, and one between two values names none.
            (
                vec![],
                "id IN (1, NULL, 1.5)",
                vec![(T::PkKeys, keys(&s, &[&[1]]), vec![])],
            ),
            // No member is a key: the range that admits nothing.
            (
                vec![],
                "id IN (NULL, 1.5)",
                vec![(T::PkRange, rng(&[0], &[], 1, 0), vec![])],
            ),
        ],
    );

    let c = compound_schema_u64_u64();
    let set = |sql, ks: &[&[u128]], left: Vec<usize>| (vec![], sql, vec![(T::PkKeys, keys(&c, ks), left)]);
    check(
        &c,
        vec![
            set("a = 1 AND b = 2", &[&[1, 2]], vec![]),
            set("b = 2 AND a = 1", &[&[1, 2]], vec![]),
            set("a = 1 AND b = 2 AND v = 9", &[&[1, 2]], vec![2]),
            set("a >= 1 AND a <= 1 AND b = 2", &[&[1, 2]], vec![]),
            set("a = 1 AND b >= 18446744073709551615", &[&[1, U64_MAX]], vec![]),
            set("a IN (7, 8) AND b = 3", &[&[7, 3], &[8, 3]], vec![]),
            set("a IN (1, 1, 2) AND b = 3", &[&[1, 3], &[2, 3]], vec![]),
            set("a IN (7) AND b = 3", &[&[7, 3]], vec![]),
            // A prefix names a key group, never a key.
            (vec![], "a IN (1, 2)", vec![]),
            (
                vec![],
                "a = 1",
                vec![(T::PinnedPkRange, rng(&[0, 1], &[], 1, 1), vec![])],
            ),
            (
                vec![],
                "a = 1 AND v = 9",
                vec![(T::PinnedPkRange, rng(&[0, 1], &[], 1, 1), vec![1])],
            ),
            // Contradictory pins fold to the interval that admits nothing.
            (
                vec![],
                "a = 1 AND a = 2",
                vec![(T::PkRange, rng(&[0, 1], &[], 2, 1), vec![])],
            ),
        ],
    );

    // Crossed lists, the PK list against schema order and a signed column beside
    // an unsigned one.
    let crossed = schema(vec![col("a", TypeCode::I32), col("b", TypeCode::U16)], &[1, 0]);
    let a = |v: i32| v as u32 as u128;
    check(
        &crossed,
        vec![(
            vec![],
            "a IN (2, -1, -3) AND b IN (5, 1)",
            vec![(
                T::PkKeys,
                keys(
                    &crossed,
                    &[
                        &[1, a(-3)],
                        &[1, a(-1)],
                        &[1, a(2)],
                        &[5, a(-3)],
                        &[5, a(-1)],
                        &[5, a(2)],
                    ],
                ),
                vec![],
            )],
        )],
    );
}

/// Every PK type's literal names the one key an INSERT of it stores, as a point
/// and as a list.
#[test]
fn every_pk_type_names_its_key() {
    use TypeCode::*;
    let uuid2 = "'6ba7b810-9dad-11d1-80b4-00c04fd430c8'";
    for (tc, lit, native) in [
        (I8, "-1", (-1i8 as u8) as u128),
        (I16, "-1", (-1i16 as u16) as u128),
        (I32, "-1", (-1i32 as u32) as u128),
        (I64, "-1", (-1i64 as u64) as u128),
        (I64, "-9223372036854775808", (i64::MIN as u64) as u128),
        (U16, "65535", 65535),
        (U32, "4294967295", 4294967295),
        (U64, "18446744073709551615", U64_MAX),
        // `-0` names the value `0`, so an unsigned column takes it.
        (U128, "-0", 0),
        (UUID, "-0", 0),
        (UUID, UUID_LIT, UUID_VALUE),
    ] {
        let s = pk_schema(tc);
        let want = vec![(T::PkKeys, keys(&s, &[&[native]]), vec![])];
        assert_eq!(ranked_of(&format!("id = {lit}"), &s, &[]), want, "{tc:?} = {lit}");
        // A repeat keeps the list a list, and is no second key.
        assert_eq!(
            ranked_of(&format!("id IN ({lit}, {lit})"), &s, &[]),
            want,
            "{tc:?} IN {lit}"
        );
    }
    let s = pk_schema(UUID);
    assert_eq!(
        ranked_of(&format!("id IN ({uuid2}, {})", UUID_LIT), &s, &[]),
        vec![(
            T::PkKeys,
            keys(&s, &[&[UUID_VALUE], &[0x6ba7b810_9dad_11d1_80b4_00c04fd430c8]]),
            vec![]
        )]
    );
    // A member that spells no UUID is no key list at all.
    assert_eq!(
        ranked_of(&format!("id IN ({}, 'not-a-uuid')", UUID_LIT), &s, &[]),
        vec![]
    );
}

/// A cross product of lists past the cap gathers nothing; a lone list that long gathers.
#[test]
fn a_list_product_past_the_cap_is_no_set() {
    let schema = compound_schema_u64_u64();
    let list = |col: usize, n: i64| BoundExpr::InList {
        inner: Box::new(BoundExpr::ColRef(col)),
        items: (0..n).map(BoundExpr::LitInt).collect(),
    };
    const { assert!(300 * 300 > MAX_CROSS_PRODUCT_KEYS) };
    assert_eq!(ranked_conjuncts(&[list(0, 300), list(1, 300)], &schema, &[]), vec![]);
    let point = bind_conjunct("b = 3", &schema);
    let lone = ranked_conjuncts(&[list(0, MAX_CROSS_PRODUCT_KEYS as i64 + 1), point], &schema, &[]);
    assert!(
        matches!(&lone[..], [(T::PkKeys, ReadBound::PkSet(keys), left)]
            if keys.len() == MAX_CROSS_PRODUCT_KEYS + 1 && left.is_empty()),
        "{:?}",
        lone.iter().map(|(tier, ..)| *tier).collect::<Vec<_>>()
    );
}

/// A PK range, on every key type: the interval its conjuncts fold to, every one
/// of them consumed — the 16-byte types have no register to re-impose one in.
#[test]
fn a_pk_range_is_the_interval_its_conjuncts_fold_to() {
    use TypeCode::*;
    let decimal = schema(
        vec![ColumnDef::typed("id", ColType::decimal(2), false), col("v", I64)],
        &[0],
    );
    for (s, sql, tier, lo, hi) in [
        (pk_schema(U64), "id > 5", T::PkRange, 6, U64_MAX),
        (pk_schema(U64), "id < -1", T::PkRange, 1, 0),
        (pk_schema(U64), "id = -1", T::PkRange, 1, 0),
        (pk_schema(U64), "id = 3.5", T::PkRange, 1, 0),
        // Bounding nothing, it still consumes its conjunct.
        (pk_schema(U64), "id > -5", T::UnboundedPkRange, 0, U64_MAX),
        (pk_schema(U128), "id > 5 AND id > 7", T::PkRange, 8, u128::MAX),
        (pk_schema(I128), "id > 5", T::PkRange, img(I128, 6), u128::MAX),
        (
            pk_schema(UUID),
            &format!("id > {}", UUID_LIT)[..],
            T::PkRange,
            UUID_VALUE + 1,
            u128::MAX,
        ),
        // A literal finer than the column's scale cuts after the value below it.
        (decimal, "id < 1.005", T::PkRange, 0, img(Decimal, 100)),
    ] {
        assert_eq!(
            ranked_of(sql, &s, &[]),
            vec![(tier, rng(&[0], &[], lo, hi), vec![])],
            "{sql}"
        );
    }
}

/// What an index's column list bounds: its leading points, then the next column's
/// interval, with every conjunct on another column left residual.
#[test]
fn an_index_is_bounded_by_its_point_prefix_and_one_interval() {
    use TypeCode::*;
    let index = |cols: &[u32], eq: &[u128], lo, hi, left: Vec<usize>| vec![(T::Index, rng(cols, eq, lo, hi), left)];
    check(
        &schema3(U64, false),
        vec![
            (vec![ix(&[1])], "a = 5", index(&[1], &[], 5, 5, vec![])),
            (vec![ix(&[1])], "a BETWEEN 5 AND 9", index(&[1], &[], 5, 9, vec![])),
            (vec![ix(&[1, 2])], "a = 5", index(&[1, 2], &[], 5, 5, vec![])),
            (vec![ix(&[1, 2])], "a = 5 AND b = 7", index(&[1, 2], &[5], 7, 7, vec![])),
            (
                vec![ix(&[1, 2])],
                "a = 5 AND b > 10",
                index(&[1, 2], &[5], 11, U64_MAX, vec![]),
            ),
            (
                vec![ix(&[1, 2])],
                "a = 7 AND b < 50",
                index(&[1, 2], &[7], 0, 49, vec![]),
            ),
            (
                vec![ix(&[2])],
                "b > 10 AND a = 7",
                index(&[2], &[], 11, U64_MAX, vec![1]),
            ),
            // Every pin on a column intersects into its one interval.
            (
                vec![ix(&[1])],
                "a > 5 AND a > 10",
                index(&[1], &[], 11, U64_MAX, vec![]),
            ),
            (vec![ix(&[1])], "a = 5 AND a > 3", index(&[1], &[], 5, 5, vec![])),
            // No conjunct names the index's leading column.
            (vec![ix(&[2])], "a = 5", vec![]),
            (vec![ix(&[1])], "a + b > 3", vec![]),
            (vec![ix(&[1])], "a NOT BETWEEN 5 AND 9", vec![]),
            (vec![ix(&[1])], "a IN (1, 2)", vec![]),
            // A PK column is an index column like any other.
            (
                vec![ix(&[0])],
                "id > 5",
                vec![
                    (T::PkRange, rng(&[0], &[], 6, U64_MAX), vec![]),
                    (T::Index, rng(&[0], &[], 6, U64_MAX), vec![]),
                ],
            ),
        ],
    );
    // An index holds no row with a NULL in any of its columns, so an uncovered
    // nullable trailing column leaves it unusable.
    check(&schema3(U64, true), vec![(vec![ix(&[1, 2])], "a = 5", vec![])]);
    // Signed values fold in signed order, not in the order of their images.
    let i32_max = image_max(I32).unwrap();
    check(
        &schema3(I32, false),
        vec![
            (
                vec![ix(&[1])],
                "a > -5 AND a > 3",
                index(&[1], &[], img(I32, 4), i32_max, vec![]),
            ),
            (
                vec![ix(&[1])],
                "a < -5 AND a < 3",
                index(&[1], &[], 0, img(I32, -6), vec![]),
            ),
        ],
    );
    // A string end on a DATE column is a key through the rule its equality takes.
    check(
        &schema3(Date, false),
        vec![(
            vec![ix(&[1])],
            "a >= '2024-01-01'",
            index(&[1], &[], img(Date, 19723), image_max(Date).unwrap(), vec![]),
        )],
    );
    // A 16-byte column has no register, so its bound consumes the conjunct.
    check(
        &schema3(U128, false),
        vec![(vec![ix(&[1])], "a IN (7)", index(&[1], &[], 7, 7, vec![]))],
    );
    check(
        &schema3(UUID, false),
        vec![(
            vec![ix(&[1])],
            &format!("a = {}", UUID_LIT)[..],
            index(&[1], &[], UUID_VALUE, UUID_VALUE, vec![]),
        )],
    );
    // Over a compound PK's second column no PK candidate exists: nothing pins the
    // leading PK column.
    let compound = schema(["a", "b", "v"].map(|n| col(n, U64)).to_vec(), &[0, 1]);
    check(
        &compound,
        vec![
            (vec![ix(&[1, 2])], "b = 1 AND v = 2", index(&[1, 2], &[1], 2, 2, vec![])),
            (vec![ix(&[1, 2])], "b > 5", index(&[1, 2], &[], 6, U64_MAX, vec![])),
        ],
    );
    // A float column has no key image.
    check(&two_col(F64), vec![(vec![ix(&[1])], "val = 1", vec![])]);
}

/// The order of the list. Each row flips with the rule it names.
#[test]
fn candidates_rank_by_tier_then_by_how_much_they_pin() {
    let s = five_u64(); // (id pk, x, a, b, c)
    let point = |cols: &[u32], v| rng(cols, &[], v, v);
    check(
        &s,
        vec![
            // A point covering a whole unique index outranks a deeper prefix of a
            // non-unique one.
            (
                vec![uq(&[1]), ix(&[2, 3])],
                "x = 1 AND a = 2 AND b = 3",
                vec![
                    (T::UniqueIndexPoint, point(&[1], 1), vec![1, 2]),
                    (T::Index, rng(&[2, 3], &[2], 3, 3), vec![0]),
                ],
            ),
            // A prefix of a unique index is no unique point; at equal depth the
            // narrower index leads, whatever the declared order.
            (
                vec![uq(&[2, 3, 4]), ix(&[2, 3])],
                "a = 1",
                vec![
                    (T::Index, point(&[2, 3], 1), vec![]),
                    (T::Index, point(&[2, 3, 4], 1), vec![]),
                ],
            ),
            // A point pins its column where an interval only narrows one.
            (
                vec![ix(&[3]), ix(&[2])],
                "a = 5 AND b BETWEEN 1 AND 9",
                vec![
                    (T::Index, point(&[2], 5), vec![1, 2]),
                    (T::Index, rng(&[3], &[], 1, 9), vec![0]),
                ],
            ),
            // With nothing pinned, two bounded sides lead one.
            (
                vec![ix(&[2]), ix(&[3])],
                "a > 5 AND b BETWEEN 1 AND 9",
                vec![
                    (T::Index, rng(&[3], &[], 1, 9), vec![0]),
                    (T::Index, rng(&[2], &[], 6, U64_MAX), vec![1, 2]),
                ],
            ),
            // An unpinned PK range yields to a full unique point, and to nothing else.
            (
                vec![uq(&[2])],
                "id > 0 AND a = 42",
                vec![
                    (T::UniqueIndexPoint, point(&[2], 42), vec![0]),
                    (T::PkRange, rng(&[0], &[], 1, U64_MAX), vec![1]),
                ],
            ),
            (
                vec![ix(&[2])],
                "id > 0 AND a = 42",
                vec![
                    (T::PkRange, rng(&[0], &[], 1, U64_MAX), vec![1]),
                    (T::Index, point(&[2], 42), vec![0]),
                ],
            ),
            (
                vec![uq(&[2, 3])],
                "id > 0 AND a = 42",
                vec![
                    (T::PkRange, rng(&[0], &[], 1, U64_MAX), vec![1]),
                    (T::Index, point(&[2, 3], 42), vec![0]),
                ],
            ),
            // A key set leads everything.
            (
                vec![uq(&[2])],
                "id = 7 AND a = 42",
                vec![
                    (T::PkKeys, keys(&s, &[&[7]]), vec![1]),
                    (T::UniqueIndexPoint, point(&[2], 42), vec![0]),
                ],
            ),
            // A PK range bounding nothing falls behind every index.
            (
                vec![ix(&[2])],
                "id >= 0 AND a = 7",
                vec![
                    (T::Index, point(&[2], 7), vec![0]),
                    (T::UnboundedPkRange, rng(&[0], &[], 0, U64_MAX), vec![1]),
                ],
            ),
        ],
    );
    // A unique point is one however it is spelled, `0` on the type's lower edge
    // included.
    for sql in ["a = 0", "a >= 0 AND a <= 0", "a <= 0", "a < 1"] {
        let got = ranked_of(&format!("id > 0 AND {sql}"), &s, &[uq(&[2])]);
        assert_eq!(
            got.first().map(|(tier, bound, _)| (*tier, bound)),
            Some((T::UniqueIndexPoint, &point(&[2], 0))),
            "{sql}"
        );
    }
    // A PK range pinning a leading column keeps its place ahead of a unique point.
    let tenants = schema(
        ["tenant", "id", "email"].map(|n| col(n, TypeCode::U64)).to_vec(),
        &[0, 1],
    );
    check(
        &tenants,
        vec![
            (
                vec![uq(&[2])],
                "tenant = 7 AND email = 42",
                vec![
                    (T::PinnedPkRange, rng(&[0, 1], &[], 7, 7), vec![1]),
                    (T::UniqueIndexPoint, point(&[2], 42), vec![0]),
                ],
            ),
            (
                vec![uq(&[2])],
                "tenant = 7 AND id > 0 AND email = 42",
                vec![
                    (T::PinnedPkRange, rng(&[0, 1], &[7], 1, U64_MAX), vec![2]),
                    (T::UniqueIndexPoint, point(&[2], 42), vec![0, 1]),
                ],
            ),
        ],
    );
}
