use super::*;
use crate::protocol::wal_block::decode_wal_block_into;
use crate::retraction_batch;
use crate::test_support::{encode_wal_block, kv_schema};
use gnitz_expr::{
    payload_str, payload_u64, BatchView, CmpOp, ExprResults, IntArithOp, LogicalInstr, LogicalProgram, Reg, ScalarEval,
    SchemaFacts, Sink,
};

/// A STRING/BLOB column region from its values, spilling into `blob`; `None` is
/// a NULL cell, which the region zero-fills.
fn german_col(vals: &[Option<&[u8]>], blob: &mut Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 16);
    for v in vals {
        out.extend_from_slice(&gnitz_wire::encode_german_string(v.unwrap_or(&[]), blob));
    }
    out
}

/// `schema`'s payload regions holding `regions`, one per payload slot in slot
/// order, each typed as its schema column.
fn payload_of(schema: &Schema, regions: Vec<Vec<u8>>) -> Vec<PayloadColumn> {
    assert_eq!(regions.len(), schema.num_payload_cols(), "one region per payload slot");
    schema
        .payload_columns()
        .zip(regions)
        .map(|((_, _, c), bytes)| {
            let mut col = PayloadColumn::new(c.ty.tc);
            col.bytes = bytes;
            col
        })
        .collect()
}

#[test]
fn validate_enforces_full_rule_set() {
    let cols = vec![
        ColumnDef::new("a", TypeCode::U64, false),    // 0: eligible, non-null
        ColumnDef::new("b", TypeCode::I32, false),    // 1: eligible, non-null
        ColumnDef::new("s", TypeCode::String, false), // 2: ineligible type
        ColumnDef::new("n", TypeCode::U64, true),     // 3: nullable
    ];

    let schema = |pk: &[u32], columns: &[ColumnDef]| Schema {
        columns: columns.to_vec(),
        pk_cols: pk.to_vec(),
    };

    assert!(schema(&[0], &cols).validate().is_ok());
    assert!(schema(&[0, 1], &cols).validate().is_ok());

    // The rule set is `gnitz_wire::validate_pk_tuple`'s, tested there; these
    // pin what this caller hands it — its arity cap and each column's type and
    // nullability.
    for (pk, want) in [
        (&[0, 1, 0, 1, 0][..], "out of range 1..=4"),
        (&[3][..], "primary key column 'n' must not be nullable"),
        (
            &[2][..],
            "primary key column 's' has type_code STRING; only fixed-width integer",
        ),
    ] {
        let got = schema(pk, &cols).validate().unwrap_err();
        assert!(got.contains(want), "pk {pk:?}: {got:?} does not mention {want:?}");
    }

    // Column-count cap: the null bitmap is one u64, so > MAX_COLUMNS rejects.
    let wide: Vec<ColumnDef> = (0..=MAX_COLUMNS)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, i > 0))
        .collect();
    let got = schema(&[0], &wide).validate().unwrap_err();
    assert!(got.contains("MAX_COLUMNS"), "{got}");
    assert!(schema(&[0], &wide[..MAX_COLUMNS]).validate().is_ok());
}

/// `(pk U64 | v I64 | s STRING nullable)`, two valid rows, the second NULL in `s`.
fn validate_fixture() -> (Schema, ZSetBatch) {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b);
        a.add_row(1, 1).i64_val(10).str_val("x");
        a.add_row(2, -1).i64_val(20).null();
    }
    (schema, b)
}

/// Each way a batch can disagree with its schema or with itself is refused and
/// named; a NULL over a zeroed cell of a nullable column is not one of them.
#[test]
fn validate_refuses_each_malformed_batch() {
    let (schema, base) = validate_fixture();
    base.validate(&schema)
        .expect("a NULL over a zeroed nullable cell validates");

    type Mutation = fn(&mut ZSetBatch);
    let cases: [(Mutation, &str); 6] = [
        (|b| _ = b.weights.pop(), "weights length"),
        (|b| _ = b.nulls.pop(), "nulls length"),
        (
            |b| b.payload[0].bytes.clear(),
            "payload slot 0: length 0 != expected 16",
        ),
        (
            |b| b.payload[1].bytes.clear(),
            "payload slot 1: length 0 != expected 32",
        ),
        (|b| b.nulls[0] |= 1, "NOT NULL column 'v'"),
        (|b| b.nulls[0] |= 2, "holds a value under NULL"),
    ];
    for (mutate, want) in cases {
        let mut b = base.clone();
        mutate(&mut b);
        let err = b.validate(&schema).unwrap_err();
        assert!(err.contains(want), "{err:?} does not mention {want:?}");
    }

    // The same batch against a schema of another layout.
    for (other, want) in [
        (kv_schema(TypeCode::I64), "payload slot count"),
        (
            Schema {
                columns: vec![
                    ColumnDef::new("pk", TypeCode::U32, false),
                    ColumnDef::new("v", TypeCode::I64, false),
                    ColumnDef::new("s", TypeCode::String, true),
                ],
                pk_cols: vec![0],
            },
            "mismatched key column types: expected [U32], got [U64]",
        ),
        (
            Schema {
                columns: vec![
                    ColumnDef::new("pk", TypeCode::U64, false),
                    ColumnDef::new("v", TypeCode::F64, false),
                    ColumnDef::new("s", TypeCode::String, true),
                ],
                pk_cols: vec![0],
            },
            "type I64 != schema type F64",
        ),
    ] {
        let err = base.validate(&other).unwrap_err();
        assert!(err.contains(want), "{err:?} does not mention {want:?}");
    }
}

#[test]
#[should_panic(expected = "extend_from_owned: layout mismatch")]
fn extend_from_owned_refuses_a_different_payload_count() {
    let pk_only = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    ZSetBatch::new(&pk_only).extend_from_owned(ZSetBatch::new(&kv_schema(TypeCode::U64)));
}

/// Equal slot counts are not enough: a batch whose slot holds another type would
/// have its cells read at that type's width.
#[test]
#[should_panic(expected = "extend_from_owned: layout mismatch")]
fn extend_from_owned_refuses_a_different_payload_type() {
    ZSetBatch::new(&kv_schema(TypeCode::I64)).extend_from_owned(ZSetBatch::new(&kv_schema(TypeCode::F64)));
}

/// A key column's sign is part of the layout: a `U64` key and an `I64` key share
/// a stride and encode differently.
#[test]
fn a_key_of_another_sign_is_another_layout() {
    let signed = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::I64, false)],
        pk_cols: vec![0],
    };
    let unsigned = Schema {
        columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    assert_eq!(
        ZSetBatch::new(&unsigned).layout_matches(&signed).unwrap_err(),
        "mismatched key column types: expected [I64], got [U64]"
    );
    let extend = std::panic::catch_unwind(|| ZSetBatch::new(&signed).extend_from_owned(ZSetBatch::new(&unsigned)));
    assert!(extend.is_err(), "extend_from_owned across key types must panic");
}

/// The scalar key verbs are a single-column key's; a compound key is written one
/// native value per column.
#[test]
fn a_compound_key_has_no_scalar_form() {
    let schema = fixture_a_schema();
    let panics = |f: &(dyn Fn() + std::panic::RefUnwindSafe)| std::panic::catch_unwind(f).is_err();
    assert!(panics(&|| {
        BatchAppender::new(&mut ZSetBatch::new(&schema)).add_row(1, 1);
    }));
    assert!(panics(&|| _ = fixture_a_batch().pks.get(0)));
    assert!(panics(&|| _ = PkColumn::from_natives(&schema, [1])));
    for arity in [&[1][..], &[1, 2, 3]] {
        assert!(
            panics(&|| PkColumn::empty_for_schema(&schema).push_natives(arity)),
            "{arity:?}"
        );
    }
}

/// `extend_from_owned` concatenates rows across String and Bytes columns, and
/// shifts each moved German cell onto its body's new place in the arena.
#[test]
fn extend_from_owned_concatenates() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("b", TypeCode::Blob, false),
        ],
        pk_cols: vec![0],
    };
    // Each half spills a body of its own, so an unshifted cell reads the wrong one.
    let long = |base: u128| format!("spilled string number {base}");
    let build = |base: u128| {
        let mut z = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut z);
            a.add_row(base, 1).str_val(&long(base)).bytes_val(&[1, 2, 3, 4, 5]);
            a.add_row(base + 1, -1).str_val("short").bytes_val(&[9, 9]);
        }
        z
    };

    let mut acc = build(1);
    acc.extend_from_owned(build(10));

    let pks: Vec<u128> = (0..acc.len()).map(|r| acc.pks.get(r)).collect();
    assert_eq!(pks, [1, 2, 10, 11]);
    assert_eq!(acc.weights, [1, -1, 1, -1]);
    let strs: Vec<&str> = (0..acc.len()).map(|r| payload_str(&acc, r, 0)).collect();
    assert_eq!(strs, [long(1).as_str(), "short", long(10).as_str(), "short"]);
    acc.validate(&schema).unwrap();
}

/// Every writer lands its value in its own payload slot — the PK column is
/// skipped wherever it sits — and `null()` sets the slot's bit over a zeroed
/// cell. A second appender continues the batch the first one left.
#[test]
fn the_appender_writes_each_value_into_its_slot() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("i", TypeCode::I64, false),
            ColumnDef::new("f", TypeCode::F64, false),
            ColumnDef::new("w", TypeCode::U128, false),
            ColumnDef::new("b", TypeCode::Blob, true),
        ],
        pk_cols: vec![1],
    };
    let spilled = "a string long enough to spill";
    let wide = (0xBEEF_u128 << 64) | 0xDEAD;
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch)
        .add_row(7, 1)
        .u64_val(100)
        .str_val(spilled)
        .i64_val(-5)
        .f64_val(1.5)
        .u128_val(wide)
        .bytes_val(b"xy");
    BatchAppender::new(&mut batch)
        .add_row(8, -1)
        .u64_val(200)
        .str_val("short")
        .i64_val(6)
        .f64_val(-2.5)
        .u128_val(1)
        .null();

    let le = |cells: &[[u8; 8]]| cells.concat();
    let mut blob = Vec::new();
    let want = ZSetBatch {
        pks: PkColumn::from_natives(&schema, [7, 8]),
        weights: vec![1, -1],
        nulls: vec![0, 1 << 5],
        payload: payload_of(
            &schema,
            vec![
                le(&[100u64.to_le_bytes(), 200u64.to_le_bytes()]),
                german_col(&[Some(spilled.as_bytes()), Some(b"short")], &mut blob),
                le(&[(-5i64).to_le_bytes(), 6i64.to_le_bytes()]),
                le(&[1.5f64.to_le_bytes(), (-2.5f64).to_le_bytes()]),
                [wide.to_le_bytes(), 1u128.to_le_bytes()].concat(),
                german_col(&[Some(b"xy"), None], &mut blob),
            ],
        ),
        blob,
    };
    assert_eq!(batch, want);
    batch.validate(&schema).unwrap();
}

/// `int_val` writes each integer at its own column's width — two's complement
/// for a signed one, DATE as its I32 day count.
#[test]
fn int_val_writes_at_the_column_width() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("n", TypeCode::I16, false),
            ColumnDef::new("d", TypeCode::Date, false),
            ColumnDef::new("u", TypeCode::U8, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch)
        .add_row(1, 1)
        .int_val(-2)
        .int_val(-1)
        .int_val(255);
    assert_eq!(batch.payload[0].bytes, (-2i16).to_le_bytes());
    assert_eq!(batch.payload[1].bytes, (-1i32).to_le_bytes());
    assert_eq!(batch.payload[2].bytes, [255]);
    batch.validate(&schema).unwrap();
}

#[test]
#[should_panic(expected = "256 is out of range for U8")]
fn int_val_refuses_a_value_its_column_cannot_hold() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("u", TypeCode::U8, false),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).int_val(256);
}

/// A fixed-width value written into a German-string column panics before the
/// region can go out of shape — one message for `u64_val`/`i64_val`/`u128_val`,
/// since they share one body.
#[test]
#[should_panic(expected = "a fixed-width value cannot be written to the String column")]
fn a_fixed_value_in_a_string_column_panics() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).u64_val(42);
}

#[test]
#[should_panic(expected = "a String value cannot be written to the U64 column")]
fn a_string_in_a_fixed_column_panics() {
    let schema = kv_schema(TypeCode::U64);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).str_val("oops");
}

/// A 16-byte write into an 8-byte column lands in a fixed-width column, so only
/// the declared stride can catch it — and it is caught at the write, not
/// deferred to `validate`'s region-length check.
#[test]
#[should_panic(expected = "U64 column at payload slot 0 takes 8 bytes")]
fn a_wide_value_in_a_narrow_column_panics() {
    let schema = kv_schema(TypeCode::U64);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).u128_val(1);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "BatchAppender: row got")]
fn closing_an_under_pushed_row_trips_the_tripwire() {
    use gnitz_wire::sys_rows::SysRowSink;
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, true),
            ColumnDef::new("b", TypeCode::Blob, true),
        ],
        pk_cols: vec![0],
    };
    let mut batch = ZSetBatch::new(&schema);
    let mut a = BatchAppender::new(&mut batch);
    a.begin_row(&[1], 1);
    a.put_null(); // only 1 of 2 payload cols pushed
    a.end_row();
}

// ── Row selection: retain_ranges and gather ──────────────────────────────

const GATHER_VALS: [&str; 4] = [
    "a long string that spills past the inline prefix",
    "tiny",
    "another long string that also spills into the heap",
    "x",
];

/// `(pk U64 | s STRING | n I64)` over `GATHER_VALS`: row `i` has pk `i`,
/// weight `i + 1` and `n = 10 i`.
fn string_batch() -> (Schema, ZSetBatch) {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("n", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b);
        for (i, v) in GATHER_VALS.iter().enumerate() {
            a.add_row(i as u128, i as i64 + 1).str_val(v).i64_val(i as i64 * 10);
        }
    }
    (schema, b)
}

/// A `string_batch` row as its values: `(pk, weight, s, n)`.
type Row = (u128, i64, String, u64);

fn rows(b: &ZSetBatch) -> Vec<Row> {
    (0..b.len())
        .map(|r| {
            (
                b.pks.get(r),
                b.weights[r],
                payload_str(b, r, 0).to_owned(),
                payload_u64(b, r, 1),
            )
        })
        .collect()
}

/// `retain_ranges` keeps the named runs in order; a moved German cell still reads
/// its spilled body out of the untouched arena.
#[test]
fn retain_ranges_keeps_the_named_runs_in_order() {
    let (schema, b) = string_batch();
    let all = rows(&b);
    for (ranges, keep) in [
        (&[(0, 1), (2, 4)][..], &[0, 2, 3][..]),
        (&[(0, 4)][..], &[0, 1, 2, 3][..]),
        (&[][..], &[][..]),
    ] {
        let mut z = b.clone();
        z.retain_ranges(ranges);
        z.validate(&schema).unwrap();
        let want: Vec<Row> = keep.iter().map(|&r| all[r].clone()).collect();
        assert_eq!(rows(&z), want, "{ranges:?}");
    }
}

/// A cut gather is the named rows at their weights, and its arena holds exactly
/// the survivors' spilled bytes — which a row-by-row rebuild also holds.
#[test]
fn a_cut_gather_matches_a_row_by_row_rebuild() {
    let (schema, b) = string_batch();
    let picks = [(2usize, 1i64), (0, 1), (3, 4)];
    let mut want = ZSetBatch::new(&schema);
    for &(r, w) in &picks {
        want.copy_row_at(&b, r, w);
    }
    let got = b.gather(&picks);
    assert_eq!(rows(&got), rows(&want));
    assert_eq!(got.blob.len(), want.blob.len(), "only the survivors' heap bytes");
    got.validate(&schema).unwrap();
}

/// A row copied at two weights and patched in one cell of the second copy
/// differs only there: a string cell, spilled in the source, and an integer one.
#[test]
fn a_copied_row_differs_only_where_it_is_patched() {
    let (schema, b) = string_batch();
    let mut pair = ZSetBatch::new(&schema);
    pair.copy_row_at(&b, 0, -1);
    pair.copy_row_at(&b, 0, 1);
    pair.set_string_cell(1, 0, "patched");
    pair.set_u64_cell(1, 1, 77);
    pair.validate(&schema).unwrap();
    assert_eq!(
        rows(&pair),
        [(0, -1, GATHER_VALS[0].to_owned(), 0), (0, 1, "patched".to_owned(), 77)]
    );
}

/// A gather naming every row in place at its own weight is the batch itself; one
/// clipped weight is not.
#[test]
fn an_in_place_gather_is_the_batch() {
    let (schema, b) = string_batch();
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
    let (schema, b) = string_batch();
    let perm = [(3usize, 1i64), (1, 1), (0, 2), (2, 1)];
    let all = rows(&b);
    let want: Vec<Row> = perm
        .iter()
        .map(|&(r, w)| (all[r].0, w, all[r].2.clone(), all[r].3))
        .collect();
    let arena = b.blob.as_ptr();
    let got = b.gather(&perm);
    assert_eq!(got.blob.as_ptr(), arena);
    assert_eq!(rows(&got), want);
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

fn fixture_a_batch() -> ZSetBatch {
    let schema = fixture_a_schema();
    let mut pks = PkColumn::empty_for_schema(&schema);
    for row in 0..3 {
        pks.push_natives(&[PK3[row] as u128, PK0[row] as u128]);
    }
    let mut blob = Vec::new();
    ZSetBatch {
        pks,
        weights: vec![1, -1, 3],
        // Row 1 nulls ci2; row 2 nulls ci4 and ci5.
        nulls: vec![0, 0b10, 0b1100],
        payload: payload_of(
            &schema,
            vec![
                C1.iter().flat_map(|v| v.to_le_bytes()).collect(),
                C2.iter().flat_map(|v| v.to_le_bytes()).collect(),
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
    let schema = fixture_b_schema();
    let mut b = ZSetBatch::new(&schema);
    let mut a = BatchAppender::new(&mut b);
    for (pk, v) in B_PK.into_iter().zip([7i64, -8, 9]) {
        a.add_row(pk as u128, 1).i64_val(v);
    }
    b
}

// ── The region/per-row contract ──────────────────────────────────────────

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
    check(
        &fixture_a_schema(),
        &ZSetBatch::new(&fixture_a_schema()),
        0,
        &A_SLOTS,
        &[],
    );

    let b_k: Vec<u128> = B_PK.iter().map(|&v| v as u128).collect();
    check(&fixture_b_schema(), &fixture_b_batch(), 3, &[(0, 8)], &[(0, &b_k)]);
}

// ── The shared evaluator over a client batch ─────────────────────────────

/// `ci3 + ci`: a PK column plus a payload column, NOT NULL (ci1) and nullable
/// (ci2) — row 1's null bit reaches the evaluator.
#[test]
fn the_shared_evaluator_reads_a_client_batch() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();
    let c1: [Option<i64>; 3] = C1.map(|v| Some(v.into()));
    let c2 = [Some(C2[0]), None, Some(C2[2])];
    for (ci, col) in [(1, c1), (2, c2)] {
        let mut ev = LogicalProgram::new(
            vec![
                LogicalInstr::LoadCol { col: 3 },
                LogicalInstr::LoadCol { col: ci },
                LogicalInstr::IntArith {
                    op: IntArithOp::Add,
                    a: Reg(0),
                    b: Reg(1),
                },
            ],
            vec![Sink::Reg(Reg(2))],
            vec![],
        )
        .resolve_scalar(&schema)
        .expect("program resolves against the client schema");
        let want: Vec<Option<i128>> = (0..3).map(|r| col[r].map(|v| i128::from(PK3[r] + v))).collect();
        assert_eq!(row_values(&mut ev, &batch), want, "ci3 + ci{ci}");
    }
}

#[test]
fn filter_over_the_region_path() {
    let schema = fixture_a_schema();
    let batch = fixture_a_batch();

    // ci2 > 0: row 0 passes (1000), row 1 is NULL (dropped by
    // `bool_bits & !null_bits`), row 2 fails (-3000).
    let mut ev = LogicalProgram::new(
        vec![
            LogicalInstr::LoadCol { col: 2 },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        vec![Sink::Reg(Reg(2))],
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
        vec![Sink::Reg(Reg(0))],
        vec![],
    )
    .resolve_scalar(&schema)
    .expect("string compare resolves against the client schema");

    // Row 0 spills both columns into the batch's one heap; row 2 nulls both.
    let lt = |r: usize| Some(i128::from(C4[r].as_bytes() < C5[r]));
    assert_eq!(row_values(&mut ev, &batch), [lt(0), lt(1), None]);
}

// ── Encode/decode round trip ─────────────────────────────────────────────

/// A batch survives encode → decode byte for byte, and decoding its block twice
/// into one sink is the batch extended by itself — the second block's German
/// cells shifted onto the sink's arena tail.
#[test]
fn batches_round_trip_through_a_wal_block() {
    // The widest admissible client PK: PK_LIST_MAX_COLS U128 columns.
    let wide = Schema::from_parts(
        vec![
            ColumnDef::new("a", TypeCode::U128, false),
            ColumnDef::new("b", TypeCode::U128, false),
            ColumnDef::new("c", TypeCode::U128, false),
            ColumnDef::new("d", TypeCode::U128, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        vec![0, 1, 2, 3],
    )
    .expect("the widest admissible client PK");
    assert_eq!(wide.pk_stride(), gnitz_wire::PK_LIST_MAX_COLS * 16);
    let mut wide_batch = ZSetBatch::new(&wide);
    wide_batch.pks.push_natives(&[u128::MAX, 1 << 64, 42, (1 << 64) + 7]);
    wide_batch.pks.push_natives(&[9, 8, 7, 6]);
    wide_batch.weights = vec![1, -2];
    wide_batch.nulls = vec![0, 0];
    wide_batch.payload[0].bytes = [-1i64, 123].iter().flat_map(|v| v.to_le_bytes()).collect();

    let a = fixture_a_schema();
    let mut a_keys = PkColumn::empty_for_schema(&a);
    a_keys.push_natives(&[10, 0]);
    a_keys.push_natives(&[20, 0]);
    let a_retraction = retraction_batch(&a, a_keys);
    for (schema, batch) in [
        (fixture_a_schema(), fixture_a_batch()),
        (fixture_b_schema(), fixture_b_batch()),
        (wide, wide_batch),
        (fixture_a_schema(), a_retraction),
        (fixture_a_schema(), ZSetBatch::new(&fixture_a_schema())),
    ] {
        let block = encode_wal_block(&batch);
        let mut sink = ZSetBatch::new(&schema);
        decode_wal_block_into(&mut sink, &block, &schema).expect("block decodes");
        assert_eq!(sink, batch, "batch must survive encode -> decode");

        decode_wal_block_into(&mut sink, &block, &schema).expect("block decodes");
        let mut twice = batch.clone();
        twice.extend_from_owned(batch);
        assert_eq!(sink, twice, "a second block appends at the arena tail");
    }
}

/// Every fact a `Schema` carries must survive the block round-trip: column
/// types, nullability, names, the `hidden` marker, and the declared PK order
/// (which is not column order here).
#[test]
fn schema_survives_the_block_roundtrip() {
    let original = Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false).hidden(),
            ColumnDef::new("name", TypeCode::String, true),
            ColumnDef::new("score", TypeCode::F64, false),
            ColumnDef::new("tag", TypeCode::I32, true),
            ColumnDef::new("uuid", TypeCode::U128, false),
        ],
        pk_cols: vec![4, 0],
    };
    assert_eq!(Schema::from_block(&original.to_block()).unwrap(), original);
}

/// The client's PK arity cap is `gnitz_wire::PK_LIST_MAX_COLS` — a key it accepts must
/// round-trip through the persisted PK-list word — so it must refuse a record
/// the shared codec, capped wider, admits.
#[test]
fn a_pk_wider_than_the_client_codec_is_rejected() {
    let n = gnitz_wire::PK_LIST_MAX_COLS + 1;
    let wide = Schema {
        columns: (0..n).map(|_| ColumnDef::new("k", TypeCode::U64, false)).collect(),
        pk_cols: (0..n as u32).collect(),
    };
    let block = wide.to_block();
    assert!(
        gnitz_wire::schema_block::decode(&block, |_| Ok(())).is_ok(),
        "the codec admits it"
    );
    assert!(Schema::from_block(&block).is_err());
}

/// A row pushed column by column is the row `push_natives` writes, and a column
/// that fails leaves nothing of its row behind.
#[test]
fn push_row_encodes_each_column_and_appends_nothing_on_error() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::I32, false),
            ColumnDef::new("v", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::U64, false),
        ],
        pk_cols: vec![2, 0],
    };
    let natives = [u64::MAX as u128, (-7i32) as u32 as u128];
    let widths = [8, 4];
    let mut got = PkColumn::empty_for_schema(&schema);
    got.push_row(|k, buf| {
        buf.extend_from_slice(&natives[k].to_le_bytes()[..widths[k]]);
        Ok::<_, ()>(())
    })
    .unwrap();
    let mut want = PkColumn::empty_for_schema(&schema);
    want.push_natives(&natives);
    assert_eq!(got, want);

    let before = got.clone();
    let failed = got.push_row(|k, buf| {
        buf.extend_from_slice(&natives[k].to_le_bytes()[..widths[k]]);
        if k == 1 {
            Err("second column")
        } else {
            Ok(())
        }
    });
    assert_eq!((failed, &got), (Err("second column"), &before));
}

/// Instructions per pushed key row, by key shape.
#[test]
#[ignore]
fn pk_column_push_bench() {
    const ROWS: usize = 1_000_000;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let key_schema = |types: &[TypeCode]| Schema {
        columns: types.iter().map(|&tc| ColumnDef::new("k", tc, false)).collect(),
        pk_cols: (0..types.len() as u32).collect(),
    };
    for types in [
        &[TypeCode::I64][..],
        &[TypeCode::U64, TypeCode::U64],
        &[TypeCode::I32, TypeCode::U128, TypeCode::I16],
    ] {
        let schema = std::hint::black_box(key_schema(types));
        let mut col = PkColumn::empty_for_schema(&schema);
        col.reserve(ROWS);
        let ((), instr) = counter.measure(|| {
            for i in 0..ROWS as u128 {
                let natives = [i, i ^ 0x55, i & 0x7fff];
                col.push_natives(&natives[..types.len()]);
            }
        });
        std::hint::black_box(&col);
        println!("push_natives {types:?}: {:.1} instr/row", instr as f64 / ROWS as f64);
    }
    let schema = std::hint::black_box(key_schema(&[TypeCode::I64]));
    let (col, instr) = counter.measure(|| PkColumn::from_natives(&schema, 0..ROWS as u128));
    std::hint::black_box(&col);
    println!("from_natives [I64]: {:.1} instr/row", instr as f64 / ROWS as f64);
}
