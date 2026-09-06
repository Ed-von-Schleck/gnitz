use super::*;

// ── Derived operator-output schemas ─────────────────────────────────────

#[test]
fn test_project_schema_compound_pk() {
    // Compound-PK input: 4 columns, PK = (col1, col2). Project [0, 3].
    let input = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
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
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let b = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    assert!(a.same_physical_layout(&b));

    let c = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    assert!(!a.same_physical_layout(&c));
}

// ── Reduce output key ────────────────────────────────────────────────────

/// A nullable single group column must NOT be promoted to the natural PK
/// (the PK region has no null bitmap); a non-nullable one is. Grouping by
/// the PK itself takes precedence as `PkPermutation`.
#[test]
fn nullable_group_col_is_not_natural_reduce_key() {
    let nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[1],
    );
    assert_eq!(nullable.reduce_out_key(&[0]), ReduceOutKey::SyntheticFold);

    let non_nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );
    assert_eq!(non_nullable.reduce_out_key(&[1]), ReduceOutKey::SingleNaturalCol);
    assert_eq!(non_nullable.reduce_out_key(&[0]), ReduceOutKey::PkPermutation);
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
            SchemaColumn::new(type_code::U32, 0), // col 0: 4 bytes
            SchemaColumn::new(type_code::U64, 0), // col 1: 8 bytes
            SchemaColumn::new(type_code::U64, 0), // col 2: 8 bytes
            SchemaColumn::new(type_code::I64, 0), // payload
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
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
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
        replicated: false,
        stream: false,
        dist_prefix_len: 99,
    };
    assert!(props.validate_against_pk(3).is_err(), "k=99 over a 3-column PK");
    assert!(props.validate_against_pk(99).is_ok(), "k == |PK| is the full-PK route");
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
    for tc in [type_code::I8, type_code::I16, type_code::I32, type_code::I64] {
        assert!(SchemaColumn::new(tc, 0).is_signed(), "type_code {tc} must be signed");
        // Nullability does not change signedness.
        assert!(
            SchemaColumn::new(tc, 1).is_signed(),
            "nullable type_code {tc} must be signed"
        );
    }
    for tc in [
        type_code::U8,
        type_code::U16,
        type_code::U32,
        type_code::U64,
        type_code::U128,
        type_code::UUID,
        type_code::F32,
        type_code::F64,
        type_code::STRING,
        type_code::BLOB,
    ] {
        assert!(
            !SchemaColumn::new(tc, 0).is_signed(),
            "type_code {tc} must not be signed"
        );
    }
}

#[test]
fn test_new_constructs_schema() {
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::STRING, 1),
    ];
    let s = SchemaDescriptor::new(&cols, &[0]);
    assert_eq!(s.num_columns(), 3);
    assert_eq!(s.pk_indices(), &[0]);
    assert_eq!(s.columns[0].type_code, type_code::U64);
    assert_eq!(s.columns[1].type_code, type_code::I64);
    assert_eq!(s.columns[2].type_code, type_code::STRING);

    // Trailing slots are `SchemaColumn::EMPTY` — the padding type code
    // nothing reads.
    assert_eq!(s.columns[3].type_code, 0);

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
fn test_try_payload_idx_around_pk() {
    // pk_index = 1: col 0 maps to payload 0, col 2 maps to payload 1, and
    // the PK column (1) has no payload slot.
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[1],
    );
    assert_eq!(s.try_payload_idx(0), Some(0));
    assert_eq!(s.try_payload_idx(2), Some(1));
    assert_eq!(s.try_payload_idx(1), None);
}

#[test]
#[should_panic(expected = "locate: col_idx 3 out of bounds")]
fn test_locate_out_of_bounds_panics() {
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
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
    for tc in 0u8..=255 {
        let Some(size) = TypeCode::try_from_u8(tc).map(|t| t.wire_stride()) else {
            continue;
        };
        assert!(size > 0, "type_code {tc} has no width");
        let cols = [SchemaColumn::new(tc, 0)];
        let built = std::panic::catch_unwind(|| SchemaDescriptor::new(&cols, &[0])).is_ok();
        assert_eq!(
            built,
            gnitz_wire::is_pk_eligible(tc),
            "type_code {tc}: descriptor and wire allow-list disagree on PK eligibility",
        );
    }
}

/// `pk_stride` sums the PK columns' widths, and `pk_columns()` yields them in
/// pk-list order — at both arities, and at a PK index that is not 0.
#[test]
fn test_pk_stride_and_pk_columns() {
    let pk_cols = |s: &SchemaDescriptor| -> Vec<(usize, usize, u8)> {
        s.pk_columns()
            .enumerate()
            .map(|(ord, (ci, c))| (ord, ci, c.type_code))
            .collect()
    };

    // Compound: [U64, U32], both PK. Stride = 8 + 4.
    let compound = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
        ],
        &[0, 1],
    );
    assert_eq!(compound.pk_stride(), 12);
    assert_eq!(pk_cols(&compound), vec![(0, 0, type_code::U64), (1, 1, type_code::U32)]);

    // Single PK at column 1, so the pk-list position and the column index
    // differ.
    let single = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ],
        &[1],
    );
    assert_eq!(single.pk_stride(), 8);
    assert_eq!(pk_cols(&single), vec![(0, 1, type_code::I64)]);
}

#[test]
fn test_max_pk_columns_boundary() {
    // Construct exactly MAX_PK_COLUMNS PK columns so a future bump
    // of the constant keeps exercising the boundary case.
    let cols = [SchemaColumn::new(type_code::U64, 0); MAX_PK_COLUMNS];
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
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let _ = SchemaDescriptor::new(&cols, &[0, 0]);
}

// ── SchemaFacts conformance ──────────────────────────────────────────────

/// `SchemaDescriptor`'s `SchemaFacts` forwarders must report exactly what
/// the schema itself does, over the shared shape matrix. The harness is the
/// only way to reach the trait methods: most collide by name with an
/// inherent method that Rust would prefer in receiver-dot position, so a
/// forwarder that silently reimplements — and thereby flips `no_nulls` or
/// the `check_col` admissibility dispatch — would go unnoticed by a direct
/// check.
#[test]
fn schema_descriptor_conforms_to_schema_facts() {
    gnitz_expr::assert_schema_facts_matrix(|cols, pk| {
        let scols: Vec<SchemaColumn> = cols
            .iter()
            .map(|&(tc, nullable)| SchemaColumn::new(tc, nullable as u8))
            .collect();
        let pk_idx: Vec<u32> = pk.iter().map(|&i| i as u32).collect();
        SchemaDescriptor::new(&scols, &pk_idx)
    });
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
        (type_code::I8, -5i128, "-5"),
        (type_code::I16, -300, "-300"),
        (type_code::I32, -70_000, "-70000"),
        (type_code::I64, i64::MIN as i128, "-9223372036854775808"),
        (type_code::U8, 200, "200"),
        (type_code::U64, u64::MAX as i128, "18446744073709551615"),
        (
            type_code::U128,
            u128::MAX as i128,
            "340282366920938463463374607431768211455",
        ),
    ] {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(tc, 0)], &[0]);
        assert_eq!(
            schema.format_pk_bytes(&opk_pk(&schema, &[v as u128])),
            want,
            "type {tc}"
        );
    }

    let uuid = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);
    let v = 0x550e8400_e29b_41d4_a716_446655440000u128;
    assert_eq!(
        uuid.format_pk_bytes(&opk_pk(&uuid, &[v])),
        "550e8400-e29b-41d4-a716-446655440000",
        "a UUID renders from all 128 bits, not a truncated low word",
    );

    // 24-byte compound PK: every column read at its own offset.
    let wide = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0); 3], &[0, 1, 2]);
    assert!(wide.pk_stride() > 16);
    assert_eq!(wide.format_pk_bytes(&opk_pk(&wide, &[7, 8, 9])), "7, 8, 9");
}
