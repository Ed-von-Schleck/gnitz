use super::*;
use crate::protocol::wal_block::{decode_wal_block, encode_wal_block};
use crate::test_support::{german_col, payload_of};
use gnitz_expr::{
    BatchView, CmpOp, ExprResults, IntArithOp, LogicalInstr, LogicalProgram, Output, Reg, ScalarEval, SchemaFacts,
};

/// `Schema`'s `ColumnTable` answers are what it was built from.
#[test]
fn schema_column_table() {
    use gnitz_expr::ColumnTable;
    use TypeCode as T;
    // (columns as (type, nullable), pk list, not_null_payload_slots)
    type Shape = (&'static [(TypeCode, bool)], &'static [u32], u64);
    let shapes: &[Shape] = &[
        (&[(T::U64, false), (T::I32, false), (T::String, true)], &[0], 0b01),
        (&[(T::String, true), (T::U64, false), (T::F64, false)], &[1], 0b10),
        (
            &[(T::I32, false), (T::U64, false), (T::F64, true), (T::I32, false)],
            &[0, 1],
            0b10,
        ),
        (
            &[(T::U64, false), (T::String, true), (T::F64, false), (T::I32, false)],
            &[3, 0],
            0b10,
        ),
        (&[(T::U64, false), (T::I32, false)], &[0, 1], 0),
    ];
    for &(cols, pk, not_null) in shapes {
        let columns: Vec<ColumnDef> = cols
            .iter()
            .enumerate()
            .map(|(i, &(tc, n))| ColumnDef::new(format!("c{i}"), tc, n))
            .collect();
        let s = Schema::from_parts(columns, pk.to_vec()).expect("client-valid schema");
        assert_eq!(ColumnTable::num_columns(&s), cols.len(), "{pk:?}");
        assert_eq!(ColumnTable::pk_cols(&s), pk, "{pk:?}");
        for (ci, &(tc, n)) in cols.iter().enumerate() {
            assert_eq!(ColumnTable::col_type_code(&s, ci), tc, "{pk:?}: col {ci}");
            assert_eq!(ColumnTable::col_nullable(&s, ci), n, "{pk:?}: col {ci}");
        }
        assert_eq!(s.not_null_payload_slots(), not_null, "{pk:?}: not_null_payload_slots");
    }
}

#[test]
fn validate_enforces_full_rule_set() {
    let cols = vec![
        ColumnDef::new("a", TypeCode::U64, false),    // 0: eligible, non-null
        ColumnDef::new("b", TypeCode::I32, false),    // 1: eligible, non-null
        ColumnDef::new("s", TypeCode::String, false), // 2: ineligible type
        ColumnDef::new("n", TypeCode::U64, true),     // 3: nullable
        ColumnDef::new("f", TypeCode::F64, false),    // 4: ineligible type
    ];

    let schema = |pk: &[u32], columns: &[ColumnDef]| Schema {
        columns: columns.to_vec(),
        pk_cols: pk.to_vec(),
    };

    // Valid single and compound PKs (including the I128 join-key type).
    assert!(schema(&[0], &cols).validate().is_ok());
    assert!(schema(&[0, 1], &cols).validate().is_ok());

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
        let got = schema(pk, &cols).validate().unwrap_err();
        assert!(got.contains(want), "pk {pk:?}: {got:?} does not mention {want:?}");
    }

    // Column-count cap: the null bitmap is one u64, so > MAX_COLUMNS rejects.
    let wide: Vec<ColumnDef> = (0..=MAX_COLUMNS)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, i > 0))
        .collect();
    assert_eq!(
        schema(&[0], &wide).validate().unwrap_err(),
        format!("column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})", MAX_COLUMNS + 1)
    );
    assert!(schema(&[0], &wide[..MAX_COLUMNS]).validate().is_ok());
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
    let _ = encode_wal_block(&batch);
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

// --- types_match tests ---

fn one_col(name: &str, tc: TypeCode, nullable: bool) -> Schema {
    Schema {
        columns: vec![ColumnDef::new(name, tc, nullable)],
        pk_cols: vec![0],
    }
}

#[test]
fn types_match_false_on_pk_type_u64_vs_i64() {
    // Same width, different OPK image for the same logical value.
    let u = one_col("pk", TypeCode::U64, false);
    let i = one_col("pk", TypeCode::I64, false);
    assert!(!u.types_match(&i));
}

#[test]
fn types_match_ignores_column_names() {
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
        a.add_row(1u128, 1).i64_val(0);
        a.add_row(2u128, 1).i64_val(10);
    }
    batch.nulls[0] |= 1 << 0;
    assert!(
        batch.validate(&schema).is_ok(),
        "a null bit over a zeroed cell of a nullable column must be accepted"
    );
    // Over a cell holding a value, the bit is a second NULL encoding.
    batch.nulls[1] |= 1 << 0;
    let err = batch.validate(&schema).unwrap_err();
    assert!(err.contains("holds a value under NULL"), "got: {err}");
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
    // payload_slot(col 1 = String) = 0 → bit 0; payload_slot(col 2 = Blob) = 1 → bit 1
    let str_bit = 1u64 << gnitz_expr::SchemaFacts::payload_slot(&schema, 1).unwrap();
    let blob_bit = 1u64 << gnitz_expr::SchemaFacts::payload_slot(&schema, 2).unwrap();
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
    let schema = nullable_str_blob_schema();
    let mut batch = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut batch, &schema);
        // No null_mask call — `null()` must be self-sufficient.
        a.add_row(42, 1).null().null();
    }
    let encoded = encode_wal_block(&batch);
    let decoded = decode_wal_block(&encoded, &schema).unwrap();
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

/// A gather naming every row in place at its own weight is the batch itself; one
/// clipped weight is not.
#[test]
fn an_in_place_gather_is_the_batch() {
    let (schema, b) = string_batch(&GATHER_VALS);
    let in_place: Vec<(usize, i64)> = b.weights.iter().copied().enumerate().collect();
    let weights = b.weights.as_ptr();
    let got = b.gather(&in_place);
    assert_eq!(got.weights.as_ptr(), weights);

    let mut clipped = in_place;
    let last = clipped.len() - 1;
    clipped[last].1 += 1;
    let weights = got.weights.as_ptr();
    let cut = got.gather(&clipped);
    assert_ne!(cut.weights.as_ptr(), weights);
    assert_eq!(cut.weights[last], clipped[last].1);
    cut.validate(&schema).unwrap();
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

// ── The client batch as a `BatchView` ───────────────────────────────────

/// Every row's integer result of a scalar program.
fn row_values(ev: &mut ScalarEval, mb: &dyn BatchView) -> Vec<Option<i128>> {
    match ev.eval_all(mb) {
        ExprResults::Int(vals) => vals,
        ExprResults::Str { .. } => panic!("row_values over a string-valued program"),
    }
}

// ── Fixture A: a compound PK (ci3, ci0) listed out of column order ───────

const PK3: [i64; 3] = [-5, 100, 1i64 << 40];
const PK0: [u32; 3] = [7, 0, u32::MAX];
const C1: [i32; 3] = [10, -20, 30];
const C2: [i64; 3] = [1000, 0 /* NULL */, -3000];
const C6: [u128; 3] = [1, u128::MAX, 1u128 << 100];
/// Row 0 spills (> 12 bytes), row 1 is inline, row 2 is NULL.
const C4: [&str; 3] = ["aaa long value that spills", "short", ""];
const C5: [&[u8]; 3] = [b"zzz long blob value spilling", b"bb", b""];

fn fixture_a_schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("k1", TypeCode::U32, false),
            ColumnDef::new("a", TypeCode::I32, false),
            ColumnDef::new("b", TypeCode::I64, true),
            ColumnDef::new("k0", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, true),
            ColumnDef::new("z", TypeCode::Blob, true),
            ColumnDef::new("w", TypeCode::U128, false),
        ],
        vec![3, 0],
    )
    .expect("fixture A is a client-valid schema")
}

/// One PK row's native little-endian columns, in PK-list order.
fn pk12(k0: i64, k1: u32) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..8].copy_from_slice(&k0.to_le_bytes());
    out[8..12].copy_from_slice(&k1.to_le_bytes());
    out
}

fn fixture_a_batch() -> ZSetBatch {
    let schema = fixture_a_schema();
    let mut pks = PkColumn::empty_for_schema(&schema);
    for row in 0..3 {
        pks.push_bytes(&schema, &pk12(PK3[row], PK0[row]));
    }
    let mut blob = Vec::new();
    let mut c1 = Vec::new();
    let mut c2 = Vec::new();
    for row in 0..3 {
        c1.extend_from_slice(&C1[row].to_le_bytes());
        c2.extend_from_slice(&C2[row].to_le_bytes());
    }
    ZSetBatch {
        pks,
        weights: vec![1, -1, 3],
        // Row 1 nulls ci2; row 2 nulls ci4 and ci5.
        nulls: vec![0, 0b10, 0b1100],
        payload: payload_of(
            &schema,
            vec![
                c1,
                c2,
                german_col(&[Some(C4[0].as_bytes()), Some(C4[1].as_bytes()), None], &mut blob),
                german_col(&[Some(C5[0]), Some(C5[1]), None], &mut blob),
                C6.iter().flat_map(|v| v.to_le_bytes()).collect(),
            ],
        ),
        blob,
    }
}

/// Every payload slot of fixture A as `(slot, width)`.
const A_SLOTS: [(usize, usize); 5] = [(0, 4), (1, 8), (2, 16), (3, 16), (4, 16)];

// ── Fixture B: a single narrow PK — the shape real traffic has ───────────

fn fixture_b_schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("k", TypeCode::U32, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        vec![0],
    )
    .expect("fixture B is a client-valid schema")
}

const B_PK: [u64; 3] = [1, 2, u32::MAX as u64];

fn fixture_b_batch() -> ZSetBatch {
    let mut v = Vec::new();
    for x in [7i64, -8, 9] {
        v.extend_from_slice(&x.to_le_bytes());
    }
    ZSetBatch {
        pks: PkColumn::from_natives(&fixture_b_schema(), B_PK.iter().map(|&x| x as u128)),
        weights: vec![1; 3],
        nulls: vec![0; 3],
        payload: payload_of(&fixture_b_schema(), vec![v]),
        blob: vec![],
    }
}

// ── The region/per-row contract ──────────────────────────────────────────

/// Fixture A's PK columns as the harness wants them, at their PK-list
/// offsets: ci3 (I64) first, ci0 (U32) second.
fn a_pk_expect() -> [(Vec<u128>, TypeCode, usize); 2] {
    [
        (PK3.iter().map(|&v| v as u128).collect(), TypeCode::I64, 0),
        (PK0.iter().map(|&v| v as u128).collect(), TypeCode::U32, 8),
    ]
}

#[test]
fn zsetbatch_satisfies_the_region_per_row_contract() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();
    {
        let pk = a_pk_expect();
        gnitz_expr::assert_batchview_consistent(
            &batch,
            3,
            &A_SLOTS,
            &[(pk[0].1, pk[0].2, &pk[0].0), (pk[1].1, pk[1].2, &pk[1].0)],
        );
    }
    let empty = ZSetBatch::new(&schema);
    gnitz_expr::assert_batchview_consistent(&empty, 0, &A_SLOTS, &[]);
}

#[test]
fn locate_addresses_the_pk_region_the_builder_hands_out() {
    let check =
        |schema: &Schema, batch: &ZSetBatch, rows: usize, cols: &[(usize, usize)], want: &[(usize, &[u128])]| {
            let pk: Vec<gnitz_expr::PkColExpect<'_>> = want
                .iter()
                .map(|&(ci, vals)| match SchemaFacts::locate(schema, ci) {
                    gnitz_expr::ColumnLocator::Pk { byte_off, type_code, .. } => (type_code, byte_off as usize, vals),
                    other => panic!("column {ci} must locate to the PK region, got {other:?}"),
                })
                .collect();
            gnitz_expr::assert_batchview_consistent(batch, rows, cols, &pk);
        };

    let a_k0: Vec<u128> = PK3.iter().map(|&v| v as u128).collect();
    let a_k1: Vec<u128> = PK0.iter().map(|&v| v as u128).collect();
    check(
        &fixture_a_schema(),
        &fixture_a_batch(),
        3,
        &A_SLOTS,
        &[(3, &a_k0), (0, &a_k1)],
    );

    let b_k: Vec<u128> = B_PK.iter().map(|&v| v as u128).collect();
    check(&fixture_b_schema(), &fixture_b_batch(), 3, &[(0, 8)], &[(0, &b_k)]);
}

// ── The shared evaluator over a client batch ─────────────────────────────

#[test]
fn the_shared_evaluator_reads_a_client_batch() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // ci3 + ci1: a PK column plus a payload column.
    let mut ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 3 },
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Output::Result(Reg(2)),
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("program resolves against the client schema");

    for row in 0..3 {
        let v = row_values(&mut ev, &batch)[row].expect("row {row} must not be null");
        assert_eq!(v, i128::from(PK3[row] + C1[row] as i64), "row {row}");
    }
}

#[test]
fn nullable_payload_null_bits_reach_the_evaluator() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // ci3 + ci2, a nullable payload column.
    let mut ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 3 },
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Output::Result(Reg(2)),
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("program resolves against the client schema");

    assert_eq!(row_values(&mut ev, &batch)[0], Some(i128::from(PK3[0] + C2[0])));
    assert!(
        row_values(&mut ev, &batch)[1].is_none(),
        "row 1 nulls the nullable column"
    );
    assert_eq!(row_values(&mut ev, &batch)[2], Some(i128::from(PK3[2] + C2[2])));
}

#[test]
fn filter_over_the_region_path() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // ci2 > 0: row 0 passes (1000), row 1 is NULL (dropped by
    // `bool_bits & !null_bits`), row 2 fails (-3000).
    let mut ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        Output::Result(Reg(2)),
        vec![],
    )
    .resolve_filter(&schema)
    .expect("predicate resolves against the client schema");

    let mut ranges: Vec<(usize, usize)> = Vec::new();
    ev.ranges(&batch, &mut ranges);
    assert_eq!(ranges, vec![(0, 1)]);
}

#[test]
fn string_columns_compare_through_the_shared_blob_heap() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // STRING (ci4) vs BLOB (ci5): both pass `check_col(GermanString)`.
    let mut ev = LogicalProgram::new(
        vec![LogicalInstr::StrColCol { op: CmpOp::Lt, col_a: 4, col_b: 5 }],
        Output::Result(Reg(0)),
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("string compare resolves against the client schema");

    // Row 0 spills both columns into the batch's one heap.
    for row in 0..2 {
        let want = i128::from(C4[row].as_bytes() < C5[row]);
        assert_eq!(row_values(&mut ev, &batch)[row], Some(want), "row {row}");
    }
    assert!(
        row_values(&mut ev, &batch)[2].is_none(),
        "row 2 nulls both string columns"
    );
}

// ── Encode/decode round trip ─────────────────────────────────────────────

#[test]
fn fixture_round_trips_through_encode_and_decode() {
    for (schema, batch) in [
        (fixture_a_schema(), fixture_a_batch()),
        (fixture_b_schema(), fixture_b_batch()),
    ] {
        let encoded = encode_wal_block(&batch);
        let decoded = decode_wal_block(&encoded, &schema).expect("block decodes");
        assert_eq!(decoded, batch, "batch must survive encode -> decode");
    }
}

/// A permutation of the PK is sent as the PK list, and so keyed by it; any
/// other group set is sent as written.
#[test]
fn reduce_group_sends_a_pk_permutation_as_the_pk_list() {
    let s = Schema::from_parts(
        vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
            ColumnDef::new("c", TypeCode::I64, false),
        ],
        vec![0, 1],
    )
    .expect("client-valid schema");
    assert_eq!(s.reduce_group(&[1, 0]), vec![0, 1]);
    assert_eq!(s.reduce_group(&[0, 1]), vec![0, 1]);
    assert_eq!(s.reduce_group(&[2, 1]), vec![2, 1]);
    assert_eq!(
        s.reduce_out_key(&s.reduce_group(&[1, 0])),
        gnitz_wire::ReduceOutKey::Natural
    );
}
