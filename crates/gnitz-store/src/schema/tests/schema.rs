use super::*;

// ── Derived operator-output schemas ─────────────────────────────────────

#[test]
fn test_project_schema_compound_pk() {
    // Compound-PK input: 4 columns, PK = (col1, col2). Project [0, 3].
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[1, 2],
    );
    let out = project_schema(&input, &[0, 3]).unwrap();
    // Two PK columns + two non-PK projected columns = 4 total.
    assert_eq!(out.num_columns(), 4);
    assert_eq!(out.pk_indices(), &[0, 1]);

    // Single-PK input collapses back to pk_indices = [0].
    let input_single = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out_single = project_schema(&input_single, &[1]).unwrap();
    assert_eq!(out_single.pk_indices(), &[0]);

    // The bound is PK-inclusive: a payload count that alone fits still
    // overflows once the PK columns are prepended.
    let wide: Vec<u32> = vec![1; crate::schema::MAX_COLUMNS];
    assert_eq!(project_schema(&input_single, &wide), None);
}

#[test]
fn test_identity_map_detection() {
    let a = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let b = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    assert!(a.same_physical_layout(&b));

    let c = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    assert!(!a.same_physical_layout(&c));
}

// ── Reduce output key ────────────────────────────────────────────────────

/// A nullable single group column must NOT be promoted to the natural PK
/// (the PK region has no null bitmap); a non-nullable PK-eligible one is,
/// signed or not, and so is the PK itself.
#[test]
fn nullable_group_col_is_not_natural_reduce_key() {
    let nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[1],
    );
    assert_eq!(nullable.reduce_out_key(&[0]), ReduceOutKey::SyntheticFold);

    let non_nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    );
    assert_eq!(non_nullable.reduce_out_key(&[1]), ReduceOutKey::Natural);
    assert_eq!(non_nullable.reduce_out_key(&[0]), ReduceOutKey::Natural);

    let signed = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    assert_eq!(signed.reduce_out_key(&[1]), ReduceOutKey::Natural);
}

// ── Placement / distribution prefix (CLUSTER BY) ────────────────────────

/// 3-column compound PK `(U32, U64, U64)` + one payload, so the columns have
/// distinct widths and a prefix stride is unambiguous.
fn three_col_pk_schema(dist_k: u8) -> SchemaDescriptor {
    three_col_placed(Placement::Keyed { prefix_len: dist_k })
}

fn three_col_placed(placement: Placement) -> SchemaDescriptor {
    SchemaDescriptor::new_with_placement(
        &[
            SchemaColumn::new(TypeCode::U32, false), // col 0: 4 bytes
            SchemaColumn::new(TypeCode::U64, false), // col 1: 8 bytes
            SchemaColumn::new(TypeCode::U64, false), // col 2: 8 bytes
            SchemaColumn::new(TypeCode::I64, false), // payload
        ],
        &[0, 1, 2],
        placement,
    )
}

#[test]
fn default_dist_is_full_pk() {
    // `new` (no clause), `Keyed { prefix_len: 0 }`, and `Keyed { prefix_len:
    // |PK| }` all yield dist_stride == pk_stride and a normalized k == pk_count.
    let pk_stride = 4 + 8 + 8; // U32 + U64 + U64
    for s in [
        three_col_pk_schema(0), // 0 = persisted default sentinel
        three_col_pk_schema(3), // explicit full PK
        SchemaDescriptor::new(
            // bare `new`
            &[
                SchemaColumn::new(TypeCode::U32, false),
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::I64, false),
            ],
            &[0, 1, 2],
        ),
    ] {
        assert_eq!(s.pk_stride(), pk_stride);
        assert_eq!(s.dist_stride(), s.pk_stride(), "default: dist == full PK");
        assert_eq!(s.placement(), Placement::Keyed { prefix_len: 3 });
    }
}

#[test]
fn dist_stride_sums_leading_prefix_columns() {
    // k=1 ⇒ just col 0 (U32 = 4 bytes).
    let s1 = three_col_pk_schema(1);
    assert_eq!(s1.placement(), Placement::Keyed { prefix_len: 1 });
    assert_eq!(s1.dist_stride(), 4);
    // k=2 ⇒ col 0 + col 1 (U32 + U64 = 12 bytes).
    let s2 = three_col_pk_schema(2);
    assert_eq!(s2.placement(), Placement::Keyed { prefix_len: 2 });
    assert_eq!(s2.dist_stride(), 12);
}

#[test]
fn dist_prefix_out_of_range_is_rejected_at_the_decode_boundary() {
    // A k past |PK| is only reachable from a corrupted or forged catalog flag,
    // and is refused where that row can be named — not silently normalized to
    // the full PK here, which would route a corrupt row as if it were sound.
    let props = gnitz_wire::TableProps {
        stream: false,
        serial: false,
        distribution: gnitz_wire::TableDistribution::Keyed { prefix_len: 99 },
    };
    assert!(props.validate(3).is_err(), "k=99 over a 3-column PK");
    assert!(props.validate(99).is_ok(), "k == |PK| is the full-PK route");
    // The descriptor's own prefix sum stays in range regardless: it walks the PK
    // columns it has, so a forged k cannot run `dist_stride` past `pk_stride`.
    assert_eq!(
        three_col_pk_schema(99).dist_stride(),
        three_col_pk_schema(0).pk_stride()
    );
}

/// A relation the router slices by nothing takes the full-PK width, so every
/// `worker_for_pk` slice over it stays in range.
#[test]
fn unkeyed_placement_takes_the_full_pk_width() {
    for p in [Placement::Replicated, Placement::Local] {
        let s = three_col_placed(p);
        assert_eq!(s.placement(), p);
        assert_eq!(s.dist_stride(), s.pk_stride(), "{p:?}");
    }
}

#[test]
fn test_schema_column_layout_and_is_signed() {
    // `SchemaColumn` is 4 bytes, and `SchemaDescriptor` holds MAX_COLUMNS of
    // them by value: a fifth field here would breach the size pin.
    assert_eq!(std::mem::size_of::<SchemaColumn>(), 4);

    // `is_signed` is derived from `type_code` in `new()`: true for I8..I64,
    // false for every unsigned / float / string / blob type.
    for tc in [TypeCode::I8, TypeCode::I16, TypeCode::I32, TypeCode::I64] {
        assert!(
            SchemaColumn::new(tc, false).is_signed(),
            "type_code {tc} must be signed"
        );
        // Nullability does not change signedness.
        assert!(
            SchemaColumn::new(tc, true).is_signed(),
            "nullable type_code {tc} must be signed"
        );
    }
    for tc in [
        TypeCode::U8,
        TypeCode::U16,
        TypeCode::U32,
        TypeCode::U64,
        TypeCode::U128,
        TypeCode::UUID,
        TypeCode::F32,
        TypeCode::F64,
        TypeCode::String,
        TypeCode::Blob,
    ] {
        assert!(
            !SchemaColumn::new(tc, false).is_signed(),
            "type_code {tc} must not be signed"
        );
    }
}

#[test]
fn test_new_constructs_schema() {
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::String, true),
    ];
    let s = SchemaDescriptor::new(&cols, &[0]);
    assert_eq!(s.num_columns(), 3);
    assert_eq!(s.pk_indices(), &[0]);
    assert_eq!(s.columns[0].type_code, TypeCode::U64);
    assert_eq!(s.columns[1].type_code, TypeCode::I64);
    assert_eq!(s.columns[2].type_code, TypeCode::String);

    // Trailing slots are `SchemaColumn::EMPTY` — padding nothing reads.
    assert_eq!(s.columns[3].size(), 0);

    // payload_columns() walks non-PK indices in logical order.
    let payload: Vec<usize> = s.payload_columns().map(|(pi, _)| s.payload_col_idx(pi)).collect();
    assert_eq!(payload, vec![1, 2]);

    // Non-zero pk_index round-trips (use I64 col at index 1, not STRING).
    let s2 = SchemaDescriptor::new(&cols, &[1]);
    assert_eq!(s2.pk_indices(), &[1]);

    // Empty placeholder (Default-style).
    let empty = SchemaDescriptor::new(&[], &[]);
    assert_eq!(empty.num_columns(), 0);
}

#[test]
fn test_payload_slot_around_pk() {
    // pk_index = 1: col 0 maps to payload 0, col 2 maps to payload 1, and
    // the PK column (1) has no payload slot.
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[1],
    );
    assert_eq!(s.payload_slot(0), Some(0));
    assert_eq!(s.payload_slot(2), Some(1));
    assert_eq!(s.payload_slot(1), None);
}

#[test]
#[should_panic(expected = "locate: col_idx 3 out of bounds")]
fn test_locate_out_of_bounds_panics() {
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    );
    let _ = s.locate(3);
}

/// The descriptor constructor admits exactly the wire allow-list — no more
/// (STRING/BLOB carry an unrelocatable heap offset, floats break the
/// byte-equal key contract) and no less. Driven off `is_pk_eligible` rather
/// than a hand-listed set so a newly added type code is covered the moment
/// it exists.
#[test]
fn test_pk_eligibility_matches_the_wire_allow_list() {
    for &tc in TypeCode::ALL {
        assert!(tc.wire_stride() > 0, "type_code {tc} has no width");
        let cols = [SchemaColumn::new(tc, false)];
        let built = std::panic::catch_unwind(|| SchemaDescriptor::new(&cols, &[0])).is_ok();
        assert_eq!(
            built,
            tc.is_pk_eligible(),
            "type_code {tc}: descriptor and wire allow-list disagree on PK eligibility",
        );
    }
}

/// `pk_stride` sums the PK columns' widths, and `pk_columns()` yields them in
/// pk-list order — at both arities, and at a PK index that is not 0.
#[test]
fn test_pk_stride_and_pk_columns() {
    let pk_cols = |s: &SchemaDescriptor| -> Vec<(usize, usize, TypeCode)> {
        s.pk_columns()
            .enumerate()
            .map(|(ord, (ci, c))| (ord, ci, c.type_code))
            .collect()
    };

    // Compound: [U64, U32], both PK. Stride = 8 + 4.
    let compound = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
        ],
        &[0, 1],
    );
    assert_eq!(compound.pk_stride(), 12);
    assert_eq!(pk_cols(&compound), vec![(0, 0, TypeCode::U64), (1, 1, TypeCode::U32)]);

    // Single PK at column 1, so the pk-list position and the column index
    // differ.
    let single = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[1],
    );
    assert_eq!(single.pk_stride(), 8);
    assert_eq!(pk_cols(&single), vec![(0, 1, TypeCode::I64)]);
}

#[test]
fn test_max_pk_columns_boundary() {
    // Construct exactly MAX_PK_COLUMNS PK columns so a future bump
    // of the constant keeps exercising the boundary case.
    let cols = [SchemaColumn::new(TypeCode::U64, false); MAX_PK_COLUMNS];
    let pks: Vec<u32> = (0..MAX_PK_COLUMNS as u32).collect();
    let s = SchemaDescriptor::new(&cols, &pks);
    assert_eq!(s.pk_indices().len(), MAX_PK_COLUMNS);
    let collected: Vec<(usize, usize)> = s.pk_columns().enumerate().map(|(ord, (ci, _))| (ord, ci)).collect();
    let expected: Vec<(usize, usize)> = (0..MAX_PK_COLUMNS).map(|k| (k, k)).collect();
    assert_eq!(collected, expected);
    assert_eq!(s.pk_stride(), MAX_PK_COLUMNS * 8);
}

#[test]
#[should_panic(expected = "duplicate PK column index")]
fn test_duplicate_pk_guard_panics_in_release() {
    // No cfg(debug_assertions) gate — guard is a hard assert! and
    // must fire in release too.
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::U64, false),
    ];
    let _ = SchemaDescriptor::new(&cols, &[0, 0]);
}

// ── ColumnTable ──────────────────────────────────────────────────────────

/// The descriptor's `ColumnTable` answers are what it was built from, and its
/// cached `pk_stride` is the derived one.
#[test]
fn schema_descriptor_column_table() {
    use TypeCode::{String, F64, I32, U64};
    // (columns as (type_code, nullable), pk list, not_null_payload_slots)
    type Shape = (&'static [(TypeCode, bool)], &'static [u32], u64);
    let shapes: &[Shape] = &[
        (&[(U64, false), (I32, false), (String, true)], &[0], 0b01),
        (&[(String, true), (U64, false), (F64, false)], &[1], 0b10),
        (&[(I32, false), (U64, false), (F64, true), (I32, false)], &[0, 1], 0b10),
        (
            &[(U64, false), (String, true), (F64, false), (I32, false)],
            &[3, 0],
            0b10,
        ),
        (&[(U64, false), (I32, false)], &[0, 1], 0),
    ];
    for &(cols, pk, not_null) in shapes {
        let scols: Vec<SchemaColumn> = cols.iter().map(|&(tc, n)| SchemaColumn::new(tc, n)).collect();
        let s = SchemaDescriptor::new(&scols, pk);
        assert_eq!(ColumnTable::num_columns(&s), cols.len(), "{pk:?}");
        assert_eq!(ColumnTable::pk_cols(&s), pk, "{pk:?}");
        for (ci, &(tc, n)) in cols.iter().enumerate() {
            assert_eq!(ColumnTable::col_type_code(&s, ci), tc, "{pk:?}: col {ci}");
            assert_eq!(ColumnTable::col_nullable(&s, ci), n, "{pk:?}: col {ci}");
        }
        assert_eq!(s.not_null_payload_slots(), not_null, "{pk:?}: not_null_payload_slots");
        assert_eq!(s.pk_stride(), SchemaFacts::pk_stride(&s), "{pk:?}: cached pk_stride");
    }
}

// ── PK rendering ─────────────────────────────────────────────────────────

/// `format_pk_bytes` decodes each column out of the OPK image at its own
/// offset and width. The signed arms are what a native-LE read would get
/// wrong (OPK flips the sign bit), and a compound PK wider than 16 bytes is
/// what a `u128` key form could not carry at all.
#[test]
fn format_pk_bytes_renders_every_pk_column_from_its_opk_image() {
    use crate::test_support::shared::opk_pk;

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
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(tc, false)], &[0]);
        assert_eq!(
            schema.format_pk_bytes(&opk_pk(&schema, &[v as u128])),
            want,
            "type {tc}"
        );
    }

    let uuid = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::UUID, false)], &[0]);
    let v = 0x550e8400_e29b_41d4_a716_446655440000u128;
    assert_eq!(
        uuid.format_pk_bytes(&opk_pk(&uuid, &[v])),
        "550e8400-e29b-41d4-a716-446655440000",
        "a UUID renders from all 128 bits, not a truncated low word",
    );

    // 24-byte compound PK: every column read at its own offset.
    let wide = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false); 3], &[0, 1, 2]);
    assert!(wide.pk_stride() > 16);
    assert_eq!(wide.format_pk_bytes(&opk_pk(&wide, &[7, 8, 9])), "7, 8, 9");
}

// ── PK coverage ──────────────────────────────────────────────────────────

/// `covers_pk` asks containment, not equality: any order, and any extra column.
#[test]
fn covers_pk_accepts_any_superset_of_the_pk_columns() {
    let col = SchemaColumn::new(TypeCode::U64, false);

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

// ── Admission: the fallible constructor and the wire record ──────────────

/// Every rule `new_with_placement` would abort the process on is an `Err` here.
#[test]
fn try_new_rejects_what_the_panicking_constructor_aborts_on() {
    let key = SchemaColumn::new(TypeCode::U64, false);
    let wide_pk: Vec<u32> = (0..=MAX_PK_COLUMNS as u32).collect();

    assert!(SchemaDescriptor::try_new(&[key, key], &[0]).is_ok());
    assert!(SchemaDescriptor::try_new(&[key], &[]).is_err(), "no PK column");
    assert!(
        SchemaDescriptor::try_new(&[key], &[1]).is_err(),
        "PK index past the columns"
    );
    assert!(
        SchemaDescriptor::try_new(&[key, key], &[0, 0]).is_err(),
        "the same column twice"
    );
    assert!(
        SchemaDescriptor::try_new(&[SchemaColumn::new(TypeCode::U64, true)], &[0]).is_err(),
        "nullable PK column"
    );
    assert!(
        SchemaDescriptor::try_new(&[SchemaColumn::new(TypeCode::F64, false)], &[0]).is_err(),
        "PK-ineligible column type"
    );
    assert!(
        SchemaDescriptor::try_new(&[key; MAX_PK_COLUMNS + 1], &wide_pk).is_err(),
        "PK arity past MAX_PK_COLUMNS"
    );
    // The shared PK validator bounds indices against the column count, never
    // the column count itself.
    assert!(
        SchemaDescriptor::try_new(&[key; MAX_COLUMNS + 1], &[0]).is_err(),
        "column count past MAX_COLUMNS"
    );
}

/// A nullable or PK-ineligible key column is a schema rule, so the wire codec
/// admits such a record and this decode is what refuses it.
#[test]
fn decode_schema_block_rejects_a_nullable_or_ineligible_pk_column() {
    use gnitz_wire::schema_block::{ColMeta, SchemaBlockCol};

    let col = |tc, nullable| SchemaBlockCol {
        ty: gnitz_wire::ColType::of(tc),
        meta: ColMeta { nullable, ..Default::default() },
        name: b"k",
    };
    for bad in [col(TypeCode::U64, true), col(TypeCode::F64, false)] {
        let record = gnitz_wire::schema_block::encode([bad].into_iter(), &[0]);
        assert!(
            gnitz_wire::schema_block::decode(&record, |_| Ok(())).is_ok(),
            "the record itself is well-formed"
        );
        assert!(decode_schema_block(&record).is_err());
    }
}

/// The record's decode bounds only its own arrays; an empty, out-of-range or
/// duplicate PK list is a schema rule, refused here.
#[test]
fn decode_schema_block_rejects_an_empty_out_of_range_or_duplicate_pk() {
    use gnitz_wire::schema_block::{ColMeta, SchemaBlockCol};

    let col = SchemaBlockCol {
        ty: gnitz_wire::ColType::of(TypeCode::U64),
        meta: ColMeta::default(),
        name: b"k",
    };
    for pk in [&[][..], &[3], &[1, 1]] {
        let record = gnitz_wire::schema_block::encode([col; 3].into_iter(), pk);
        assert!(
            gnitz_wire::schema_block::decode(&record, |_| Ok(())).is_ok(),
            "{pk:?}: the record itself is well-formed"
        );
        assert!(decode_schema_block(&record).is_err(), "{pk:?}");
    }
}

#[test]
fn schema_roundtrip_wire_preserves_pk_order() {
    let u64c = SchemaColumn::new(TypeCode::U64, false);
    let u32c = SchemaColumn::new(TypeCode::U32, false);
    let cases: &[(&[SchemaColumn], &[u32])] = &[
        (&[u64c, u64c], &[0, 1]),
        (&[u64c, u64c], &[1, 0]),
        (&[u32c, u32c, u32c, u32c], &[0, 1, 2, 3]),
    ];
    for &(cols, pk_indices) in cases {
        let original = SchemaDescriptor::new(cols, pk_indices);
        let block = encode_schema_block(&original);
        let decoded = decode_schema_block(&block).unwrap();
        assert!(
            original == decoded,
            "pk_indices {pk_indices:?} did not survive wire round-trip",
        );
    }
}
