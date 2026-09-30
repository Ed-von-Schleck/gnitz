use super::*;
use crate::test_rng::Rng;
use crate::test_support::{opk_pk, pk_only_schema, random_schema};

fn col(tc: TypeCode) -> SchemaColumn {
    SchemaColumn::new(tc, false)
}

// ── Layout ───────────────────────────────────────────────────────────────

/// Over random schemas — any column types and nullability, the PK anywhere and
/// in any order — every layout fact the descriptor caches at construction is
/// the one `SchemaFacts` derives from its columns, and the schema record
/// round-trips it.
#[test]
fn cached_layout_matches_the_derivation() {
    let mut rng = Rng::new(0x005C_4E3A);
    for _ in 0..2000 {
        let s = random_schema(&mut rng, TypeCode::ALL, true);
        assert_eq!(s.pk_stride(), SchemaFacts::pk_stride(&s), "{s:?}");
        assert_eq!(s.num_payload_cols(), SchemaFacts::num_payload_cols(&s), "{s:?}");
        for (pi, c) in s.payload_columns() {
            let ci = SchemaFacts::payload_col_idx(&s, pi);
            assert_eq!(s.payload_col_idx(pi), ci, "{s:?}");
            assert!(*c == s.columns[ci], "{s:?}: payload slot {pi}");
        }
        assert!(s.pk_columns().map(|(ci, _)| ci as u32).eq(s.pk_cols().iter().copied()));
        assert_eq!(s.has_german_string(), s.string_payload_slots() != 0, "{s:?}");
        assert_eq!(decode_schema_block(&encode_schema_block(&s)).unwrap(), s);
    }
}

/// The distribution prefix is the leading PK columns' OPK width; the `0`
/// sentinel and a placement no router slices take the full PK.
#[test]
fn placement_sets_the_distribution_prefix() {
    use TypeCode::{I64, U32, U64};
    // PK (U32, U64, U64): distinct widths, so each prefix stride is unambiguous.
    let cols = [col(U32), col(U64), col(U64), col(I64)];
    let keyed = |prefix_len| Placement::Keyed { prefix_len };
    for (placement, resolved, dist) in [
        (Placement::KEYED_DEFAULT, keyed(3), 20),
        (keyed(3), keyed(3), 20),
        (keyed(2), keyed(2), 12),
        (keyed(1), keyed(1), 4),
        (Placement::Replicated, Placement::Replicated, 20),
        (Placement::Local, Placement::Local, 20),
    ] {
        let s = SchemaDescriptor::new_with_placement(&cols, &[0, 1, 2], placement);
        assert_eq!((s.placement(), s.dist_stride()), (resolved, dist), "{placement:?}");
    }
}

// ── Derived schemas ──────────────────────────────────────────────────────

/// The PK columns in PK-list order, then each named payload column in the order
/// named: a PK or out-of-range entry skipped, a repeat kept.
#[test]
fn project_schema_places_the_pk_then_the_named_payload() {
    use TypeCode::{I32, I64, U32, U64};
    let opt = SchemaColumn::new(I64, true);
    let input = SchemaDescriptor::new(&[col(I32), col(U64), opt, col(U32)], &[3, 1]);
    assert_eq!(
        project_schema(&input, &[2, 3, 0, 99, 2]).unwrap(),
        SchemaDescriptor::new(&[col(U32), col(U64), opt, col(I32), opt], &[0, 1]),
    );
    // The bound is PK-inclusive: a payload count that alone fits still
    // overflows once the PK columns are prepended.
    assert_eq!(project_schema(&input, &[0; MAX_COLUMNS - 1]), None);
}

/// A schema is a trailing append of another iff every earlier column keeps its
/// position, type and PK membership.
#[test]
fn trailing_append_keeps_every_earlier_column() {
    use TypeCode::{String, I64, U64};
    let prev = SchemaDescriptor::new(&[col(U64), col(I64)], &[0]);
    let cases: [(&[SchemaColumn], &[u32], bool, &str); 5] = [
        (&[col(U64), col(I64)], &[0], true, "unchanged"),
        (&[col(U64), col(I64), col(String)], &[0], true, "a column appended"),
        (&[col(U64), col(U64)], &[0], false, "a column retyped"),
        (&[col(U64), col(I64)], &[1], false, "the PK moved"),
        (&[col(U64)], &[0], false, "a column dropped"),
    ];
    for (cols, pk, want, what) in cases {
        assert_eq!(
            SchemaDescriptor::new(cols, pk).is_trailing_append_of(&prev),
            want,
            "{what}"
        );
    }
}

// ── PK rendering and coverage ────────────────────────────────────────────

/// `format_pk_bytes` decodes each column out of the OPK image at its own
/// offset and width. The signed arms are what a native-LE read would get
/// wrong (OPK flips the sign bit), and a compound PK wider than 16 bytes is
/// what a `u128` key form could not carry at all.
#[test]
fn format_pk_bytes_renders_every_pk_column_from_its_opk_image() {
    for (tc, v, want) in [
        (TypeCode::I8, -5i128, "-5"),
        (TypeCode::I16, -300, "-300"),
        (TypeCode::I32, -70_000, "-70000"),
        (TypeCode::I64, i64::MIN as i128, "-9223372036854775808"),
        (TypeCode::U8, 200, "200"),
        (TypeCode::U64, u64::MAX as i128, "18446744073709551615"),
        (
            TypeCode::U128,
            u128::MAX as i128,
            "340282366920938463463374607431768211455",
        ),
        (TypeCode::I128, -1, "-1"),
        (TypeCode::I128, i128::MIN, "-170141183460469231731687303715884105728"),
    ] {
        let schema = pk_only_schema(&[tc]);
        assert_eq!(
            schema.format_pk_bytes(&opk_pk(&schema, &[v as u128])),
            want,
            "type {tc}"
        );
    }

    let uuid = pk_only_schema(&[TypeCode::UUID]);
    let v = 0x550e8400_e29b_41d4_a716_446655440000u128;
    assert_eq!(
        uuid.format_pk_bytes(&opk_pk(&uuid, &[v])),
        "550e8400-e29b-41d4-a716-446655440000",
        "a UUID renders from all 128 bits, not a truncated low word",
    );

    // 24-byte compound PK: every column read at its own offset.
    let wide = pk_only_schema(&[TypeCode::U64; 3]);
    assert_eq!(wide.format_pk_bytes(&opk_pk(&wide, &[7, 8, 9])), "7, 8, 9");
}

/// `covers_pk` asks containment, not equality: any order, and any extra column.
#[test]
fn covers_pk_accepts_any_superset_of_the_pk_columns() {
    let col = col(TypeCode::U64);

    let compound = SchemaDescriptor::new(&[col; 4], &[1, 2]);
    assert!(compound.covers_pk(&[1, 2]));
    assert!(compound.covers_pk(&[2, 1]), "the PK's own order is irrelevant");
    assert!(compound.covers_pk(&[2, 3, 1]), "an extra column does not weaken it");
    assert!(!compound.covers_pk(&[1]), "half a compound PK determines no row");
    assert!(!compound.covers_pk(&[0, 3]));

    let single = SchemaDescriptor::new(&[col; 2], &[0]);
    assert!(single.covers_pk(&[0]));
    assert!(single.covers_pk(&[1, 0]));
    assert!(!single.covers_pk(&[1]));
}

// ── Admission ────────────────────────────────────────────────────────────

/// Every shape the constructor aborts on, `try_new` refuses and a schema record
/// carrying it fails to decode — the two paths an untrusted column list takes.
/// The wire codec admits each such record; the refusal is this crate's.
#[test]
fn admission_refuses_what_the_constructor_aborts_on() {
    let k = col(TypeCode::U64);
    let cases: [(&str, Vec<SchemaColumn>, Vec<u32>); 7] = [
        ("no PK column", vec![k], vec![]),
        ("PK index past the columns", vec![k], vec![1]),
        ("the same column twice", vec![k, k], vec![0, 0]),
        (
            "nullable PK column",
            vec![SchemaColumn::new(TypeCode::U64, true)],
            vec![0],
        ),
        ("PK-ineligible column type", vec![col(TypeCode::F64)], vec![0]),
        (
            "PK arity past MAX_PK_COLUMNS",
            vec![k; MAX_PK_COLUMNS + 1],
            (0..=MAX_PK_COLUMNS as u32).collect(),
        ),
        ("column count past MAX_COLUMNS", vec![k; MAX_COLUMNS + 1], vec![0]),
    ];
    for (what, cols, pk) in &cases {
        assert!(SchemaDescriptor::try_new(cols, pk).is_err(), "try_new: {what}");
        assert!(
            std::panic::catch_unwind(|| SchemaDescriptor::new(cols, pk)).is_err(),
            "new: {what}"
        );
        let record = gnitz_wire::schema_block::encode(
            cols.iter().map(|c| SchemaBlockCol {
                ty: ColType::of(c.type_code),
                nullable: c.nullable,
                hidden: false,
                name: b"k",
            }),
            pk,
        );
        assert!(decode_schema_block(&record).is_err(), "decode: {what}");
    }
    assert!(SchemaDescriptor::try_new(&[k, k], &[1, 0]).is_ok());
}

/// A schema record's arity prefix carries no redundancy of its own, so every
/// flip in it must either be refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    let cols = [
        col(TypeCode::U64),
        col(TypeCode::I64),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::F64, true),
    ];
    let reference = SchemaDescriptor::new(&cols, &[0]);
    let mut buf = encode_schema_block(&reference);
    let prefix = 4 + 1 + reference.pk_cols().len();
    crate::test_support::sweep_bit_flips(&mut buf, 0..prefix, |byte, bit, buf| {
        if let Ok(decoded) = decode_schema_block(buf) {
            assert_ne!(decoded, reference, "prefix byte {byte} bit {bit} changed nothing");
        }
    });
}
