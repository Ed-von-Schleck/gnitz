use super::*;

/// `Schema` must answer the shared `SchemaFacts` shape matrix exactly. This
/// is where an OPK byte offset or a payload-slot off-by-one would
/// miscompute silently rather than error, and the harness is the only way to
/// reach the trait methods — two of them collide by name with an inherent
/// method Rust prefers in receiver-dot position.
#[test]
fn schema_conforms_to_schema_facts() {
    gnitz_expr::assert_schema_facts_matrix(|cols, pk| {
        let columns: Vec<ColumnDef> = cols
            .iter()
            .enumerate()
            .map(|(i, &(tc, nullable))| ColumnDef::new(format!("c{i}"), TypeCode::from_validated_u8(tc), nullable))
            .collect();
        Schema::from_parts(columns, pk.iter().map(|&c| c as u32).collect()).expect("client-valid schema")
    });
}

#[test]
fn validate_parts_enforces_full_rule_set() {
    let cols = vec![
        ColumnDef::new("a", TypeCode::U64, false),    // 0: eligible, non-null
        ColumnDef::new("b", TypeCode::I32, false),    // 1: eligible, non-null
        ColumnDef::new("s", TypeCode::String, false), // 2: ineligible type
        ColumnDef::new("n", TypeCode::U64, true),     // 3: nullable
        ColumnDef::new("f", TypeCode::F64, false),    // 4: ineligible type
    ];

    // Valid single and compound PKs (including the I128 join-key type).
    assert!(Schema::validate_parts(&[0], &cols).is_ok());
    assert!(Schema::validate_parts(&[0, 1], &cols).is_ok());

    // Each rule rejects, and names itself. The wording is `PkRule`'s.
    for (pk, want) in [
        (&[][..], "at least one column"),             // empty
        (&[0, 1, 0, 1, 0][..], "out of range 1..=4"), // over-long
        (&[9][..], "index 9 out of bounds"),          // out of range
        (&[0, 0][..], "column 0 twice"),              // duplicate
        (&[3][..], "must not be nullable"),           // nullable column
        (&[2][..], "only fixed-width integer"),       // STRING is ineligible
        (&[4][..], "only fixed-width integer"),       // F64 is ineligible
    ] {
        let got = Schema::validate_parts(pk, &cols).unwrap_err();
        assert!(got.contains(want), "pk {pk:?}: {got:?} does not mention {want:?}");
    }

    // Column-count cap: the null bitmap is one u64, so > MAX_COLUMNS rejects.
    let wide: Vec<ColumnDef> = (0..=MAX_COLUMNS)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, i > 0))
        .collect();
    assert_eq!(
        Schema::validate_parts(&[0], &wide).unwrap_err(),
        format!("column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})", MAX_COLUMNS + 1)
    );
    assert!(Schema::validate_parts(&[0], &wide[..MAX_COLUMNS]).is_ok());
}

#[test]
fn filler_columns_encode_without_panic() {
    // The client delete path builds retraction batches from `filler_columns`
    // (bypassing BatchAppender, whose `add_row` takes a scalar PK). Cover
    // every payload family — including a nullable String — the same way
    // `push` exercises them: validate, then encode.
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("i", TypeCode::I64, false),    // Fixed, 8B
            ColumnDef::new("big", TypeCode::I128, false), // Fixed, 16B
            ColumnDef::new("u", TypeCode::U128, false),   // Fixed, 16B
            ColumnDef::new("uid", TypeCode::UUID, false), // Fixed, 16B
            ColumnDef::new("s", TypeCode::String, true),  // Strings, nullable
            ColumnDef::new("b", TypeCode::Blob, false),   // Bytes
        ],
        pk_cols: vec![0],
    };
    let count = 2;
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, [10, 20]),
        weights: vec![-1; count],
        nulls: vec![0; count],
        payload: ZSetBatch::filler_columns(&schema, count),
        blob: vec![],
    };
    batch.validate(&schema).expect("filler batch must validate");
    let _ = crate::protocol::wal_block::encode_wal_block(7, &batch);
}

#[test]
fn test_num_payload_cols() {
    // 2-column schema → 1 payload column.
    let s = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    assert_eq!(s.num_payload_cols(), 1);

    // pk_index not at column 0 → same answer (columns.len() - pk_cols.len()).
    let s = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("c", TypeCode::U64, false),
        ],
        pk_cols: vec![2],
    };
    assert_eq!(s.num_payload_cols(), 3);
}

#[test]
fn test_num_columns() {
    let s = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    assert_eq!(s.num_columns(), 1);

    let s = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    assert_eq!(s.num_columns(), 3);
}

#[test]
fn test_wire_stride_string() {
    assert_eq!(
        TypeCode::String.wire_stride(),
        16,
        "String wire stride must be 16 (German String struct: 4B len + 4B prefix + 8B ptr/inline)"
    );
}

#[test]
fn test_wire_stride_all() {
    assert_eq!(TypeCode::U8.wire_stride(), 1);
    assert_eq!(TypeCode::I8.wire_stride(), 1);
    assert_eq!(TypeCode::U16.wire_stride(), 2);
    assert_eq!(TypeCode::I16.wire_stride(), 2);
    assert_eq!(TypeCode::U32.wire_stride(), 4);
    assert_eq!(TypeCode::I32.wire_stride(), 4);
    assert_eq!(TypeCode::F32.wire_stride(), 4);
    assert_eq!(TypeCode::U64.wire_stride(), 8);
    assert_eq!(TypeCode::I64.wire_stride(), 8);
    assert_eq!(TypeCode::F64.wire_stride(), 8);
    assert_eq!(TypeCode::String.wire_stride(), 16);
    assert_eq!(TypeCode::U128.wire_stride(), 16);
}

// --- Step 1: Schema equality tests ---

#[test]
fn test_schema_eq() {
    let a = Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    let b = a.clone();
    assert_eq!(a, b);
}

#[test]
fn test_schema_ne_col_name() {
    let a = Schema {
        columns: vec![ColumnDef::new("id", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let b = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    assert_ne!(a, b);
}

#[test]
fn test_schema_ne_pk_index() {
    let a = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let b = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ],
        pk_cols: vec![1],
    };
    assert_ne!(a, b);
}

#[test]
fn test_schema_ne_type_code() {
    let a = Schema {
        columns: vec![ColumnDef::new("x", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let b = Schema {
        columns: vec![ColumnDef::new("x", TypeCode::I64, false)],
        pk_cols: vec![0],
    };
    assert_ne!(a, b);
}

// --- types_match (warm-push guard) tests ---

fn one_col(name: &str, tc: TypeCode, nullable: bool) -> Schema {
    Schema {
        columns: vec![ColumnDef::new(name, tc, nullable)],
        pk_cols: vec![0],
    }
}

#[test]
fn types_match_false_on_pk_type_u64_vs_i64() {
    // The core bug: a U64-pk batch against an I64 table must NOT take the
    // warm path (different OPK image for the same logical value).
    let u = one_col("pk", TypeCode::U64, false);
    let i = one_col("pk", TypeCode::I64, false);
    assert!(!u.types_match(&i));
}

#[test]
fn types_match_ignores_column_names() {
    // A name-only difference must still take the warm fast path: the
    // server validator ignores names.
    let a = one_col("pk", TypeCode::U64, false);
    let b = one_col("id", TypeCode::U64, false);
    assert!(a.types_match(&b));
}

#[test]
fn types_match_false_on_nullability() {
    let a = one_col("x", TypeCode::U64, false);
    let b = one_col("x", TypeCode::U64, true);
    assert!(!a.types_match(&b));
}

#[test]
fn types_match_false_on_pk_cols() {
    let a = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let mut b = a.clone();
    b.pk_cols = vec![1];
    assert!(!a.types_match(&b));
}

#[test]
fn types_match_false_on_column_count() {
    let a = one_col("pk", TypeCode::U64, false);
    let b = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    assert!(!a.types_match(&b));
}

// --- Step 2: validate() tests ---

#[test]
fn test_validate_empty_batch() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("val", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let batch = ZSetBatch::new(&schema);
    assert!(batch.validate(&schema).is_ok());
}

#[test]
fn test_validate_valid_batch() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("val", TypeCode::I64, false),
            ColumnDef::new("name", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1u128, 1).i64_val(10).str_val("a");
        a.add_row(2u128, 1).i64_val(20).str_val("b");
        a.add_row(3u128, 1).i64_val(30).null();
    }
    assert!(batch.validate(&schema).is_ok());
}

#[test]
fn test_validate_mismatched_weights() {
    let schema = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    batch.pks.push_u128(&schema, 1);
    // weights is empty — mismatch
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("weights"));
}

#[test]
fn test_validate_mismatched_strings() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    batch.pks.push_u128(&schema, 1);
    batch.weights.push(1);
    batch.nulls.push(0);
    // Strings column is empty — mismatch (16 bytes per German cell)
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("payload slot 0: length 0 != expected 16"), "{err}");
}

#[test]
fn test_validate_mismatched_fixed() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    batch.pks.push_u128(&schema, 1);
    batch.weights.push(1);
    batch.nulls.push(0);
    // Fixed column 1 is empty (needs 8 bytes) — a byte length, not a count
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("payload slot 0: length 0 != expected 8"), "{err}");
}

#[test]
fn test_validate_rejects_null_bit_on_not_null_column() {
    // A null bit on a NOT NULL payload column must be rejected: FK/unique
    // validation would skip the value while decoders read it as live data.
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1u128, 1).i64_val(10);
    }
    assert!(batch.validate(&schema).is_ok(), "clean batch must pass");
    // Flip the null bit on payload col 0 (`v`, NOT NULL) → rejected.
    batch.nulls[0] |= 1 << 0;
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("NOT NULL"), "got: {err}");
}

#[test]
fn test_validate_allows_null_bit_on_nullable_column() {
    // The same null bit on a NULLABLE payload column is fine.
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1u128, 1).i64_val(10);
    }
    batch.nulls[0] |= 1 << 0;
    assert!(
        batch.validate(&schema).is_ok(),
        "a null bit on a nullable column must be accepted"
    );
}

/// A two-column wide-PK schema whose `Bytes` PK buffer is not a whole
/// number of `stride`-byte rows must be rejected by `validate`.
fn wide_pk_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ],
        pk_cols: vec![0, 1],
    }
}

#[test]
fn test_validate_wide_pk_buffer_not_multiple_of_stride() {
    let schema = wide_pk_schema();
    // stride 16 (two U64 PK cols), buffer 20 bytes → 1.25 rows.
    // Keep weights/nulls consistent with the truncated row count (1) so
    // only the stride-divisibility check can trip.
    let batch = ZSetBatch {
        pks: PkColumn { stride: 16, buf: vec![0u8; 20] },
        weights: vec![1],
        nulls: vec![0],
        payload: vec![],
        blob: vec![],
    };
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("multiple of stride"), "unexpected error: {err}");
}

#[test]
fn test_validate_wide_pk_zero_stride() {
    let schema = wide_pk_schema();
    // A zero stride would panic the `len()`/modulo divides; validate must
    // reject it as the malformed-input gate.
    let batch = ZSetBatch {
        pks: PkColumn { stride: 0, buf: vec![0u8; 16] },
        weights: vec![],
        nulls: vec![],
        payload: vec![],
        blob: vec![],
    };
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("stride must be non-zero"), "unexpected error: {err}");
}

#[test]
#[should_panic(expected = "payload layout mismatch")]
fn test_extend_from_payload_count_mismatch_panics() {
    let schema2 = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let schema1 = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let mut a = ZSetBatch::new(&schema1);
    let b = ZSetBatch::new(&schema2);
    a.extend_from_owned(b);
}

/// Equal slot counts are not enough: a batch whose slot holds another type would
/// have its cells read at that type's width.
#[test]
#[should_panic(expected = "payload layout mismatch")]
fn extend_from_owned_refuses_a_different_payload_layout() {
    let typed = |tc| Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", tc, false),
        ],
        pk_cols: vec![0],
    };
    let mut a = ZSetBatch::new(&typed(TypeCode::I64));
    a.extend_from_owned(ZSetBatch::new(&typed(TypeCode::F64)));
}

/// `extend_from_owned` (move) concatenates rows across String and Bytes
/// columns, preserving values and moving the heap buffers.
#[test]
fn test_extend_from_owned_concatenates() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("b", TypeCode::Blob, false),
        ],
        pk_cols: vec![0],
    };
    let build = |base: u128| {
        let mut z = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut z, &schema);
            a.add_row(base, 1)
                .str_val("hello world is long enough")
                .bytes_val(&[1, 2, 3, 4, 5]);
            a.add_row(base + 1, -1).str_val("short").bytes_val(&[9, 9]);
        }
        z
    };

    let mut acc = build(1);
    acc.extend_from_owned(build(10));

    assert_eq!(acc.len(), 4);
    // Values from both halves survive in order.
    assert_eq!(acc.pks.to_vec_u128(&schema), vec![1u128, 2, 10, 11]);
    assert_eq!(acc.weights, vec![1, -1, 1, -1]);
    {
        let v = &acc.payload[0].bytes;
        assert_eq!(
            german_strings(&acc, 0),
            [
                "hello world is long enough",
                "short",
                "hello world is long enough",
                "short",
            ],
            "{v:?}"
        );
    }
}

/// `(pk U64 | v I64 | s STRING)` over pks `0..n`, each `s` spilled to the arena.
fn retain_fixture(n: u128) -> (Schema, ZSetBatch) {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    };
    let mut z = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut z, &schema);
        for pk in 0..n {
            a.add_row(pk, pk as i64 + 1)
                .i64_val(pk as i64 * 10)
                .str_val(&format!("a spilled string number {pk}"));
        }
    }
    (schema, z)
}

/// Several runs compact down in order, and a moved German cell still reads its
/// spilled body out of the untouched arena.
#[test]
fn retain_ranges_keeps_the_named_runs_in_order() {
    let (schema, mut z) = retain_fixture(6);
    z.retain_ranges(&[(1, 2), (3, 5)]);
    assert!(z.validate(&schema).is_ok());
    assert_eq!(z.pks.to_vec_u128(&schema), vec![1u128, 3, 4]);
    assert_eq!(z.weights, vec![2, 4, 5]);
    assert_eq!(
        z.payload[0].bytes,
        [10i64, 30, 40].iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>()
    );
    assert_eq!(
        german_strings(&z, 1),
        [
            "a spilled string number 1",
            "a spilled string number 3",
            "a spilled string number 4"
        ]
    );
}

#[test]
fn retain_ranges_over_the_whole_batch_is_a_no_op() {
    let (_, mut z) = retain_fixture(3);
    let before = z.clone();
    z.retain_ranges(&[(0, 3)]);
    assert_eq!(z, before);
}

#[test]
fn retain_ranges_with_no_ranges_empties_the_batch() {
    let (schema, mut z) = retain_fixture(3);
    z.retain_ranges(&[]);
    assert!(z.is_empty());
    assert!(z.validate(&schema).is_ok());
}

#[test]
fn test_validate_wrong_column_count() {
    let schema2 = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let schema1 = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let batch = ZSetBatch::new(&schema1);
    let err = batch.validate(&schema2).unwrap_err();
    assert!(err.contains("payload slot count"), "{err}");
}

// --- Step 3: BatchAppender tests ---

#[test]
fn test_appender_single_row() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(42u128, 1)
        .u64_val(100)
        .u64_val(200);
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.pks.get(&schema, 0), 42);
    assert_eq!(batch.weights[0], 1);
    {
        let buf = &batch.payload[0].bytes;
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 100);
    }
    {
        let buf = &batch.payload[1].bytes;
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 200);
    }
}

#[test]
fn test_appender_multi_row() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1u128, 1).u64_val(10);
        a.add_row(2u128, 1).u64_val(20);
        a.add_row(3u128, -1).u64_val(30);
    }
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.pks.to_vec_u128(&schema), vec![1u128, 2u128, 3u128]);
    assert_eq!(batch.weights, vec![1, 1, -1]);
    {
        let buf = &batch.payload[0].bytes;
        assert_eq!(buf.len(), 24);
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 10);
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 20);
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 30);
    }
}

#[test]
fn test_appender_string_col() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(1u128, 1)
        .u64_val(42)
        .str_val("hello");
    assert_eq!(batch.len(), 1);
    assert_eq!(german_strings(&batch, 1), ["hello"]);
}

#[test]
fn test_appender_u128_col() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("big", TypeCode::U128, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(1u128, 1)
        .u128_val(((0xBEEF_u128) << 64) | 0xDEAD);
    let b = &batch.payload[0].bytes;
    assert_eq!(b, &(((0xBEEF_u128) << 64) | 0xDEAD).to_le_bytes());
}

#[test]
fn test_appender_mixed_types() {
    // pk(0) + U64 + String + I64 + String
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::String, false),
            ColumnDef::new("c", TypeCode::I64, false),
            ColumnDef::new("d", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(1u128, 1)
        .u64_val(100)
        .str_val("hello")
        .i64_val(-5)
        .str_val("world");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        u64::from_le_bytes(batch.payload[0].bytes[0..8].try_into().unwrap()),
        100
    );
    assert_eq!(german_strings(&batch, 1), ["hello"]);
    assert_eq!(i64::from_le_bytes(batch.payload[2].bytes[0..8].try_into().unwrap()), -5);
    assert_eq!(german_strings(&batch, 3), ["world"]);
}

#[test]
fn test_appender_pk_not_at_zero() {
    // pk_index=2: columns [A(0), B(1), PK(2), C(3)]
    let schema = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("c", TypeCode::U64, false),
        ],
        pk_cols: vec![2],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(99u128, 1)
        .u64_val(10) // cursor 0 -> ci 0 (A)
        .u64_val(20) // cursor 1 -> ci 1 (B)
        .u64_val(30); // cursor 2 -> ci 3 (C), skips pk_index=2

    assert_eq!(batch.pks.get(&schema, 0), 99);
    {
        let buf = &batch.payload[0].bytes;
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 10);
    }
    {
        let buf = &batch.payload[1].bytes;
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 20);
    }
    {
        let buf = &batch.payload[2].bytes;
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 30);
    }
}

// --- Step 4: Type-mismatch panics ---

fn kv_schema(v: TypeCode) -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", v, false),
        ],
        pk_cols: vec![0],
    }
}

/// A fixed-width value written into a German-string column panics before the
/// region can go out of shape — one message for `u64_val`/`i64_val`/`u128_val`,
/// since they share one body.
#[test]
#[should_panic(expected = "a fixed-width value cannot be written to the String column")]
fn a_fixed_value_in_a_string_column_panics() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u64_val(42);
}

#[test]
#[should_panic(expected = "a string/blob value cannot be written to the U64 column")]
fn a_string_in_a_fixed_column_panics() {
    let schema = kv_schema(TypeCode::U64);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(1u128, 1)
        .str_val("oops");
}

/// A 16-byte write into an 8-byte column shares the `Fixed` variant, so only
/// the declared stride can catch it — and it is caught at the write, not
/// deferred to `validate`'s region-length check.
#[test]
#[should_panic(expected = "U64 column at payload slot 0 takes 8 bytes")]
fn a_wide_value_in_a_narrow_column_panics() {
    let schema = kv_schema(TypeCode::U64);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u128_val(1);
}

#[test]
fn test_appender_then_validate() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1u128, 1).i64_val(10).str_val("hello");
        a.add_row(2u128, -1).i64_val(20).null();
    }
    assert!(batch.validate(&schema).is_ok());
}

// --- Site C: PkBuf::from_bytes ---

#[test]
fn pk_tuple_from_bytes_max_accepts() {
    let bytes = vec![0xabu8; MAX_PK_BYTES];
    let t = PkBuf::from_bytes(&bytes);
    assert_eq!(t.width(), MAX_PK_BYTES);
}

#[test]
#[should_panic(expected = "PkBuf::from_bytes: length")]
fn pk_tuple_from_bytes_over_panics() {
    PkBuf::from_bytes(&[0u8; MAX_PK_BYTES + 1]);
}

// --- `null()` sets the null bitmap bit ---

fn nullable_str_blob_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, true),
            ColumnDef::new("b", TypeCode::Blob, true),
        ],
        pk_cols: vec![0],
    }
}

#[test]
fn null_sets_the_bitmap_bit() {
    let schema = nullable_str_blob_schema();
    // payload_idx(col 1 = String) = 0 → bit 0; payload_idx(col 2 = Blob) = 1 → bit 1
    let str_bit = 1u64 << schema.payload_idx(1);
    let blob_bit = 1u64 << schema.payload_idx(2);
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1, 1).null().null();
    }
    assert_eq!(batch.nulls[0], str_bit | blob_bit, "both null bits must be set");
    assert!(batch.validate(&schema).is_ok());
}

#[test]
fn a_null_cell_round_trips_as_null() {
    use crate::protocol::wal_block::{decode_wal_block_verified, encode_wal_block};
    let schema = nullable_str_blob_schema();
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        // No null_mask call — `null()` must be self-sufficient.
        a.add_row(42, 1).null().null();
    }
    let encoded = encode_wal_block(1, &batch);
    let (decoded, _) = decode_wal_block_verified(&encoded, &schema).unwrap();
    assert_eq!(decoded.nulls[0], batch.nulls[0], "null bitmap round-trips");
    // The read side gates on the bitmap; the cells themselves are zeroed.
    assert_eq!(decoded.payload[0].bytes, [0u8; 16], "String NULL cell is zeroed");
    assert_eq!(decoded.payload[1].bytes, [0u8; 16], "Blob NULL cell is zeroed");
}

// --- §5.2: add_row under-push tripwire ---

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "BatchAppender: row got")]
fn closing_an_under_pushed_row_trips_the_tripwire() {
    use gnitz_wire::sys_rows::SysRowSink;
    let schema = nullable_str_blob_schema(); // 2 payload columns
    let mut batch = ZSetBatch::new(&schema);
    let mut a = BatchAppender::new(&mut batch, &schema);
    a.begin_row(&[1], 1);
    a.put_null(); // only 1 of 2 payload cols pushed
    a.end_row(); // should panic: the row is incomplete
}

#[test]
fn a_fresh_appender_writes_onto_a_populated_batch() {
    let schema = nullable_str_blob_schema();
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1, 1).str_val("x").bytes_val(b"y");
    }
    let mut a2 = BatchAppender::new(&mut batch, &schema);
    a2.add_row(2, 1).str_val("z").bytes_val(b"w");
    assert_eq!(batch.pks.len(), 2);
}

/// Every cell of a STRING payload slot, as content strings.
fn german_strings(batch: &ZSetBatch, pi: usize) -> Vec<String> {
    batch.payload[pi]
        .bytes
        .as_chunks::<16>()
        .0
        .iter()
        .map(|cell| String::from_utf8(gnitz_wire::german_string_content(cell, &batch.blob).to_vec()).unwrap())
        .collect()
}

/// `(pk U64 | s STRING | n I64)`, row `i` carrying `vals[i]`, `n = 10 i` and weight `i + 1`.
fn string_batch(vals: &[&str]) -> (Schema, ZSetBatch) {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("n", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    for (i, v) in vals.iter().enumerate() {
        b.pks.push_u128(&schema, i as u128);
        b.weights.push(i as i64 + 1);
        b.nulls.push(0);
        let cell = gnitz_wire::encode_german_string(v.as_bytes(), &mut b.blob);
        b.payload[0].bytes.extend_from_slice(&cell);
        b.payload[1].bytes.extend_from_slice(&(i as i64 * 10).to_le_bytes());
    }
    (schema, b)
}

/// Every STRING cell's content, in row order.
fn string_contents(b: &ZSetBatch) -> Vec<Vec<u8>> {
    b.payload[0]
        .bytes
        .as_chunks::<16>()
        .0
        .iter()
        .map(|c| gnitz_wire::german_string_content(c, &b.blob).to_vec())
        .collect()
}

const GATHER_VALS: [&str; 4] = [
    "a long string that spills past the inline prefix",
    "tiny",
    "another long string that also spills into the heap",
    "x",
];

/// A cut gather is the row-by-row rebuild, cell for cell, and its arena holds
/// exactly the survivors' spilled bytes — which the rebuild's re-encode also holds.
#[test]
fn a_cut_gather_matches_a_row_by_row_rebuild() {
    let (schema, b) = string_batch(&GATHER_VALS);
    let rows = [(2usize, 1i64), (0, 1), (3, 4)];
    let mut want = ZSetBatch::new(&schema);
    for &(r, w) in &rows {
        want.copy_row_at(&b, r, w);
    }
    let got = b.gather(&rows);
    assert_eq!(got.pks, want.pks);
    assert_eq!(got.weights, want.weights);
    assert_eq!(got.nulls, want.nulls);
    assert_eq!(got.payload[1], want.payload[1]);
    assert_eq!(string_contents(&got), string_contents(&want));
    assert_eq!(got.blob.len(), want.blob.len(), "only the survivors' heap bytes");
    got.validate(&schema).unwrap();
}

/// A gather keeping every row moves the arena whole: every cell keeps its offset.
#[test]
fn a_whole_gather_moves_the_arena() {
    let (schema, b) = string_batch(&GATHER_VALS);
    let perm = [(3usize, 1i64), (1, 1), (0, 2), (2, 1)];
    let contents = string_contents(&b);
    let want: Vec<Vec<u8>> = perm.iter().map(|&(r, _)| contents[r].clone()).collect();
    let arena = b.blob.as_ptr();
    let got = b.gather(&perm);
    assert_eq!(got.blob.as_ptr(), arena);
    assert_eq!(string_contents(&got), want);
    assert_eq!(got.weights, [1, 1, 2, 1]);
    got.validate(&schema).unwrap();
}
