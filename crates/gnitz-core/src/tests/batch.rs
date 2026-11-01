use super::*;
use crate::test_support::{decode_wal_block, encode_wal_block, kv_schema};
use crate::BatchAppender;
use gnitz_expr::{BatchView, CmpOp, IntArithOp, LogicalInstr, LogicalProgram, Reg, ScalarCell, ScalarEval, Sink};
use gnitz_wire::{payload_str, payload_u64, ColumnDef};
use std::convert::Infallible;

/// A STRING/BLOB column region from its values, spilling into `blob`; `None` is
/// a NULL cell, which the region zero-fills.
fn german_col(vals: &[Option<&[u8]>], blob: &mut Vec<u8>) -> Vec<u8> {
    vals.iter()
        .flat_map(|v| gnitz_wire::encode_german_string(v.unwrap_or(&[]), blob))
        .collect()
}

/// `schema`'s payload regions holding `regions`, one per payload slot in slot
/// order, each typed as its schema column.
fn payload_of(schema: &Schema, regions: Vec<Vec<u8>>) -> Vec<PayloadColumn> {
    assert_eq!(
        regions.len(),
        schema.layout().num_payload_cols(),
        "one region per payload slot"
    );
    schema
        .payload_columns()
        .zip(regions)
        .map(|((_, _, c), bytes)| PayloadColumn { tc: c.ty.tc, bytes })
        .collect()
}

/// Each way a batch can disagree with its schema or with itself is refused and
/// named; a NULL over a zeroed cell of a nullable column is not one of them.
#[test]
fn validate_refuses_each_malformed_batch() {
    // `(pk U64 | v I64 | s STRING nullable)`, two valid rows, the second NULL in `s`.
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
            ColumnDef::new("s", TypeCode::String, true),
        ],
        &[0],
    )
    .unwrap();
    let mut base = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut base);
        a.add_row(1, 1).int_val(10).str_val("x");
        a.add_row(2, -1).int_val(20).null();
    }
    base.validate(&schema)
        .expect("a NULL over a zeroed nullable cell validates");

    type Mutation = fn(&mut ZSetBatch);
    let cases: [(Mutation, &str); 8] = [
        (|b| _ = b.weights.pop(), "pk region length"),
        (|b| b.pks.push_natives(&[3]), "pk region length"),
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
        (|b| b.nulls[0] |= 1 << 2, "past the last payload column"),
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
            Schema::from_parts(
                vec![
                    ColumnDef::new("pk", TypeCode::U32, false),
                    ColumnDef::new("v", TypeCode::I64, false),
                    ColumnDef::new("s", TypeCode::String, true),
                ],
                &[0],
            )
            .unwrap(),
            "mismatched key column types: expected [U32], got [U64]",
        ),
        (
            Schema::from_parts(
                vec![
                    ColumnDef::new("pk", TypeCode::U64, false),
                    ColumnDef::new("v", TypeCode::F64, false),
                    ColumnDef::new("s", TypeCode::String, true),
                ],
                &[0],
            )
            .unwrap(),
            "type I64 != schema type F64",
        ),
    ] {
        let err = base.validate(&other).unwrap_err();
        assert!(err.contains(want), "{err:?} does not mention {want:?}");
    }
}

/// Batches that differ in one layout clause each are refused: the payload slot count, a
/// slot's type (whose cells would be read at another width), and a key column's sign.
#[test]
fn same_layout_refuses_each_differing_clause() {
    let key_only = |tc| Schema::from_parts(vec![ColumnDef::new("pk", tc, false)], &[0]).unwrap();
    for (a, b) in [
        (key_only(TypeCode::U64), kv_schema(TypeCode::U64)),
        (kv_schema(TypeCode::I64), kv_schema(TypeCode::F64)),
        (key_only(TypeCode::I64), key_only(TypeCode::U64)),
    ] {
        let e = std::panic::catch_unwind(|| ZSetBatch::new(&a).extend_from_owned(ZSetBatch::new(&b))).unwrap_err();
        assert_eq!(e.downcast_ref::<&str>(), Some(&"extend_from_owned: layout mismatch"));
    }
}

/// A key column's sign is part of the layout: a `U64` key and an `I64` key share
/// a stride and encode differently.
#[test]
fn a_key_of_another_sign_is_another_layout() {
    let signed = Schema::from_parts(vec![ColumnDef::new("pk", TypeCode::I64, false)], &[0]).unwrap();
    let unsigned = Schema::from_parts(vec![ColumnDef::new("pk", TypeCode::U64, false)], &[0]).unwrap();
    assert_eq!(
        ZSetBatch::new(&unsigned).layout_matches(&signed).unwrap_err(),
        "mismatched key column types: expected [I64], got [U64]"
    );
}

/// The scalar key verbs are a single-column key's; a compound key is written one
/// native value per column.
#[test]
fn a_compound_key_has_no_scalar_form() {
    let schema = fixture_a_schema();
    let panics = |f: &(dyn Fn() + std::panic::RefUnwindSafe)| std::panic::catch_unwind(f).is_err();
    let mut b = ZSetBatch::new(&schema);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        BatchAppender::new(&mut b).add_row(1, 1);
    }))
    .is_err());
    assert!(
        b.weights.is_empty() && b.nulls.is_empty() && b.pks.is_empty(),
        "a refused key leaves no partial row"
    );
    assert!(panics(&|| _ = fixture_a_batch().pks.get(0)));
    assert!(panics(&|| _ = PkColumn::from_natives(&schema, [1])));
    for arity in [&[1][..], &[1, 2, 3]] {
        assert!(
            panics(&|| PkColumn::empty_for_schema(&schema).push_natives(arity)),
            "{arity:?}"
        );
    }
}

/// A key column built from whole native columns is the one `push_natives` builds
/// a row at a time, for a compound key of mixed sign and for a lone wide key; a
/// column shorter than the others is refused.
#[test]
fn from_native_columns_is_push_natives_over_every_row() {
    let compound = Schema::from_parts(
        vec![
            ColumnDef::new("a", TypeCode::I16, false),
            ColumnDef::new("b", TypeCode::U32, false),
        ],
        &[0, 1],
    )
    .unwrap();
    let a: [i16; 5] = [i16::MIN, -1, 0, 1, i16::MAX];
    let b: [u32; 5] = [0, 1, 0x8000_0000, 7, u32::MAX];
    let mut want = PkColumn::empty_for_schema(&compound);
    for (&x, &y) in a.iter().zip(&b) {
        want.push_natives(&[u128::from(x as u16), u128::from(y)]);
    }
    let cols = vec![
        a.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>(),
        b.iter().flat_map(|y| y.to_le_bytes()).collect(),
    ];
    assert_eq!(PkColumn::from_native_columns(&compound, &cols), want);

    let wide = Schema::from_parts(vec![ColumnDef::new("k", TypeCode::U128, false)], &[0]).unwrap();
    let vals = [0, 1, u128::MAX, 1 << 127, 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210];
    let cells: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
    assert_eq!(
        PkColumn::from_native_columns(&wide, &[cells]),
        PkColumn::from_natives(&wide, vals)
    );

    let short = vec![cols[0].clone(), cols[1][..16].to_vec()];
    let refused = std::panic::catch_unwind(|| PkColumn::from_native_columns(&compound, &short));
    assert!(refused.is_err(), "a short key column must panic");
}

/// `extend_from_owned` concatenates rows across String and Bytes columns, and
/// shifts each moved German cell onto its body's new place in the arena.
#[test]
fn extend_from_owned_concatenates() {
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("b", TypeCode::Blob, false),
        ],
        &[0],
    )
    .unwrap();
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
    let strs: Vec<String> = (0..acc.len())
        .map(|r| payload_str(&acc, r, 0).unwrap().into_owned())
        .collect();
    assert_eq!(strs, [long(1).as_str(), "short", long(10).as_str(), "short"]);
    acc.validate(&schema).unwrap();
}

/// Every writer lands its value in its own payload slot — the PK column is
/// skipped wherever it sits — and `null()` sets the slot's bit over a zeroed
/// cell. A second appender continues the batch the first one left.
#[test]
fn the_appender_writes_each_value_into_its_slot() {
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("i", TypeCode::I64, false),
            ColumnDef::new("f", TypeCode::F64, false),
            ColumnDef::new("w", TypeCode::U128, false),
            ColumnDef::new("b", TypeCode::Blob, true),
        ],
        &[1],
    )
    .unwrap();
    let spilled = "a string long enough to spill";
    let wide = (0xBEEF_u128 << 64) | 0xDEAD;
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch)
        .add_row(7, 1)
        .int_val(100)
        .str_val(spilled)
        .int_val(-5)
        .f64_val(1.5)
        .u128_val(wide)
        .bytes_val(b"xy");
    BatchAppender::new(&mut batch)
        .add_row(8, -1)
        .int_val(200)
        .str_val("short")
        .int_val(6)
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
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("n", TypeCode::I16, false),
            ColumnDef::new("d", TypeCode::Date, false),
            ColumnDef::new("u", TypeCode::U8, false),
        ],
        &[0],
    )
    .unwrap();
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
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("u", TypeCode::U8, false),
        ],
        &[0],
    )
    .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).int_val(256);
}

/// A fixed-width value written into a German-string column panics before the
/// region can go out of shape — one message for `f64_val`/`f32_val`/`u128_val`,
/// since they share one body.
#[test]
#[should_panic(expected = "a fixed-width value cannot be written to the String column")]
fn a_fixed_value_in_a_string_column_panics() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).u128_val(42);
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
    use gnitz_wire::SysRowSink;
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, true),
            ColumnDef::new("b", TypeCode::Blob, true),
        ],
        &[0],
    )
    .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut a = BatchAppender::new(&mut batch);
    a.begin_row(&[1], 1);
    a.null(); // only 1 of 2 payload cols pushed
    a.end_row();
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "BatchAppender: row got 1 of 2")]
fn starting_a_row_after_an_under_pushed_one_trips_the_tripwire() {
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
            ColumnDef::new("w", TypeCode::I64, false),
        ],
        &[0],
    )
    .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut a = BatchAppender::new(&mut batch);
    a.add_row(1, 1).int_val(1);
    a.add_row(2, 1);
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
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("n", TypeCode::I64, false),
        ],
        &[0],
    )
    .unwrap();
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b);
        for (i, v) in GATHER_VALS.iter().enumerate() {
            a.add_row(i as u128, i as i64 + 1).str_val(v).int_val(i as i64 * 10);
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
                payload_str(b, r, 0).unwrap().into_owned(),
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
        (&[(0, 2), (2, 4)][..], &[0, 1, 2, 3][..]),
        (&[][..], &[][..]),
    ] {
        let mut z = b.clone();
        z.retain_ranges(ranges);
        z.validate(&schema).unwrap();
        let want: Vec<Row> = keep.iter().map(|&r| all[r].clone()).collect();
        assert_eq!(rows(&z), want, "{ranges:?}");
    }
}

/// A rollback drops every row since the mark: a whole row with its spilled string, and a key-only partial row.
#[test]
fn truncate_to_drops_the_rows_and_heap_since_the_mark() {
    let (_, want) = string_batch();
    let mut b = want.clone();
    let m = b.mark();
    BatchAppender::new(&mut b)
        .add_row(9, 1)
        .str_val(GATHER_VALS[0])
        .int_val(0);
    b.pks.push_natives(&[10]);
    b.truncate_to(m);
    assert_eq!(b, want);
}

/// A cut gather is the named rows at their weights, and its arena holds exactly
/// the survivors' spilled bytes — which a row-by-row rebuild also holds.
#[test]
fn a_cut_gather_matches_a_row_by_row_rebuild() {
    let (schema, b) = string_batch();
    let picks = [2u32, 0, 3];
    let mut want = ZSetBatch::new(&schema);
    for &r in &picks {
        want.copy_row_at(&b, r as usize, b.weights[r as usize]);
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

/// A gather naming every row in place is the batch itself.
#[test]
fn an_in_place_gather_is_the_batch() {
    let (schema, b) = string_batch();
    let in_place: Vec<u32> = (0..b.len() as u32).collect();
    let weights = b.weights.as_ptr();
    let got = b.gather(&in_place);
    assert_eq!(got.weights.as_ptr(), weights);
    got.validate(&schema).unwrap();
}

/// A gather keeping every row moves the arena whole: every cell keeps its offset.
#[test]
fn a_whole_gather_moves_the_arena() {
    let (schema, b) = string_batch();
    let perm = [3u32, 1, 0, 2];
    let all = rows(&b);
    let want: Vec<Row> = perm.iter().map(|&r| all[r as usize].clone()).collect();
    let arena = b.blob.as_ptr();
    let got = b.gather(&perm);
    assert_eq!(got.blob.as_ptr(), arena);
    assert_eq!(rows(&got), want);
    got.validate(&schema).unwrap();
}

/// A batch over a compound key with payload cells of every stride, NULLs among the
/// nullable ones, and `strings` STRING columns holding short and spilled values.
fn mixed_batch(strings: usize) -> (Schema, ZSetBatch) {
    let mut columns = vec![
        ColumnDef::new("a", TypeCode::U32, false),
        ColumnDef::new("u8c", TypeCode::U8, true),
        ColumnDef::new("b", TypeCode::I64, false),
        ColumnDef::new("i16c", TypeCode::I16, false),
        ColumnDef::new("i32c", TypeCode::I32, true),
        ColumnDef::new("u128c", TypeCode::U128, false),
    ];
    columns.extend((0..strings).map(|_| ColumnDef::new("s", TypeCode::String, true)));
    let schema = Schema::from_parts(columns, &[2, 0]).unwrap();
    let mut b = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut b);
    for r in 0..40u128 {
        let row = app.add_row_natives(&[r * 7 % 5, r], if r % 4 == 0 { -2 } else { 1 + r as i64 % 3 });
        if r % 3 == 0 {
            row.null();
        } else {
            row.int_val(r as i64 % 200);
        }
        row.int_val(-(r as i64));
        if r % 5 == 0 {
            row.null();
        } else {
            row.int_val(r as i64 * 1000);
        }
        row.u128_val(r << 100 | r);
        for s in 0..strings as u128 {
            match (r + s) % 3 {
                0 => row.null(),
                1 => row.str_val("short"),
                _ => row.str_val(GATHER_VALS[(r + s) as usize % GATHER_VALS.len()]),
            };
        }
    }
    b.validate(&schema).unwrap();
    (schema, b)
}

/// A row's key, weight, null word and payload cells, a German cell by its content.
type RowCells = (Vec<u8>, i64, u64, Vec<Vec<u8>>);

/// Every row of `b` as [`RowCells`].
fn cells_of(b: &ZSetBatch) -> Vec<RowCells> {
    (0..b.len())
        .map(|r| {
            let cells = b
                .payload
                .iter()
                .map(|c| {
                    if c.tc.is_german_string() {
                        gnitz_wire::german_string_content(&c.bytes.as_chunks::<16>().0[r], &b.blob).to_vec()
                    } else {
                        c.bytes[r * c.stride()..][..c.stride()].to_vec()
                    }
                })
                .collect();
            (b.pks.get_bytes(r).to_vec(), b.weights[r], b.nulls[r], cells)
        })
        .collect()
}

/// A borrowing gather is a row-by-row copy at the source weights: byte for byte with one
/// STRING column, and row for row, on an arena of the same size, with two — whose spills it
/// lays out column by column where the row copy interleaves them. The consuming gather holds
/// the same rows.
#[test]
fn a_borrowing_gather_matches_a_row_by_row_copy() {
    for strings in [0, 1, 2] {
        let (schema, b) = mixed_batch(strings);
        for picks in [
            (0..b.len() as u32).collect::<Vec<u32>>(),
            (0..b.len() as u32).filter(|r| r % 3 != 0).collect(),
            vec![17, 3, 39, 0, 22],
            vec![],
        ] {
            let mut want = ZSetBatch::new(&schema);
            for &r in &picks {
                want.copy_row_at(&b, r as usize, b.weights[r as usize]);
            }
            let got = b.gathered(&picks);
            got.validate(&schema).unwrap();
            if strings < 2 {
                assert_eq!(got, want, "{strings} strings, {picks:?}");
            } else {
                assert_eq!(cells_of(&got), cells_of(&want), "{picks:?}");
                assert_eq!(got.blob.len(), want.blob.len(), "{picks:?}");
            }
            assert_eq!(
                cells_of(&b.clone().gather(&picks)),
                cells_of(&want),
                "{strings} strings, {picks:?}"
            );
        }
    }
}

/// Appending every column whole, in slot order, onto an empty batch builds what a borrowing
/// gather of every row builds, byte for byte.
#[test]
fn appending_columns_whole_matches_a_full_gather() {
    for strings in [0, 1, 2] {
        let (schema, b) = mixed_batch(strings);
        let mut got = ZSetBatch::new(&schema);
        got.pks.buf = b.pks.buf.clone();
        got.weights = b.weights.clone();
        got.nulls = b.nulls.clone();
        for pi in 0..b.payload.len() {
            got.append_column_from(pi, &b, pi);
        }
        let all: Vec<u32> = (0..b.len() as u32).collect();
        assert_eq!(got, b.gathered(&all), "{strings} strings");
        got.validate(&schema).unwrap();
    }
}

// ── The client batch as a `BatchView` ───────────────────────────────────

/// Every row's integer result of a scalar program.
fn row_values(ev: &mut ScalarEval, mb: &dyn BatchView) -> Vec<Option<i128>> {
    let mut vals = Vec::new();
    ev.for_each(mb, |_, cell| {
        let ScalarCell::Int(v) = cell else {
            panic!("row_values over a string-valued program")
        };
        vals.push(v);
        Ok::<_, Infallible>(())
    })
    .unwrap();
    vals
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
        &[3, 0],
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

// ── Fixture B: a single narrow PK — the shape real traffic has ───────────

fn fixture_b_schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("k", TypeCode::U32, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        &[0],
    )
    .expect("fixture B is a client-valid schema")
}

const B_PK: [u64; 3] = [1, 2, u32::MAX as u64];

fn fixture_b_batch() -> ZSetBatch {
    let schema = fixture_b_schema();
    let mut b = ZSetBatch::new(&schema);
    let mut a = BatchAppender::new(&mut b);
    for (pk, v) in B_PK.into_iter().zip([7i64, -8, 9]) {
        a.add_row(pk as u128, 1).int_val(v);
    }
    b
}

// ── PK columns read through `ColumnLocator` ─────────────────────────────

#[test]
fn locate_addresses_the_pk_region_the_builder_hands_out() {
    let check = |schema: &Schema, batch: &ZSetBatch, want: &[(usize, &[u128])]| {
        for &(ci, vals) in want {
            let loc = schema.layout().locate(ci);
            assert!(
                matches!(loc, gnitz_expr::ColumnLocator::Pk { .. }),
                "column {ci} is a PK column"
            );
            assert_eq!(batch.len(), vals.len());
            for (row, &v) in vals.iter().enumerate() {
                let mut scratch = [0u8; 16];
                let got = loc.native_le_bytes(batch, row, &mut scratch);
                assert_eq!(got, &v.to_le_bytes()[..got.len()], "col {ci} row {row}");
            }
        }
    };

    let a_k0: Vec<u128> = PK3.iter().map(|&v| v as u128).collect();
    let a_k1: Vec<u128> = PK0.iter().map(|&v| v as u128).collect();
    check(&fixture_a_schema(), &fixture_a_batch(), &[(3, &a_k0), (0, &a_k1)]);

    let b_k: Vec<u128> = B_PK.iter().map(|&v| v as u128).collect();
    check(&fixture_b_schema(), &fixture_b_batch(), &[(0, &b_k)]);
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
        .and_then(|p| p.resolve_scalar(schema.layout()))
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
    let mut ev = gnitz_zset_testkit::cmp_all(&[(CmpOp::Gt, 2, 0)])
        .resolve_filter(schema.layout())
        .expect("predicate resolves against the client schema");

    assert_eq!(ev.ranges(&batch), [(0, 1)]);
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
    .and_then(|p| p.resolve_scalar(schema.layout()))
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
        &[0, 1, 2, 3],
    )
    .expect("the widest admissible client PK");
    assert_eq!(wide.layout().pk_stride(), gnitz_wire::PK_LIST_MAX_COLS * 16);
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
    let a_retraction = ZSetBatch::retraction(&a, a_keys);
    for (schema, batch) in [
        (fixture_a_schema(), fixture_a_batch()),
        (fixture_b_schema(), fixture_b_batch()),
        (wide, wide_batch),
        (fixture_a_schema(), a_retraction),
    ] {
        let block = encode_wal_block(&batch);
        let mut sink = decode_wal_block(&block, &schema).expect("block decodes");
        assert_eq!(sink, batch, "batch must survive encode -> decode");

        decode_wal_block_into(&mut sink, &block, &schema).expect("block decodes");
        let mut twice = batch.clone();
        twice.extend_from_owned(batch);
        assert_eq!(sink, twice, "a second block appends at the arena tail");
    }
}

// ── WAL blocks and region lists ─────────────────────────────────────────

/// A German cell whose heap offset overruns the heap is a `DecodeError`.
#[test]
fn a_string_cell_pointing_past_the_blob_is_refused() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch)
        .add_row(1, 1)
        .str_val("a value well past the inline cell");
    let mut forged = batch.clone();
    // A German cell's bytes 8..16 are its heap offset; the block copies it verbatim.
    forged.payload[0].bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());

    match decode_wal_block(&encode_wal_block(&forged), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("not in canonical form"), "{m}"),
        other => panic!("expected a decode error, got {other:?}"),
    }

    let mut sink = batch.clone();
    assert!(decode_wal_block_into(&mut sink, &encode_wal_block(&forged), &schema).is_err());
    assert_eq!(sink, batch, "a refused block leaves the sink as it was");
}

/// A process's own regions are appended behind the rows a batch already holds,
/// each long cell reading its content at the heap's new place.
#[test]
fn own_regions_are_appended_with_their_long_cells_rebased() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch)
        .add_row(1, 1)
        .str_val("ab")
        .add_row(2, -1)
        .str_val("a value well past the inline cell");
    let regions = batch.wire_regions();

    let mut sink = batch.clone();
    sink.extend_from(&ZSetBatch::from_own_regions(&schema, &regions).unwrap());
    assert_eq!(sink.len(), 4);
    assert_eq!(sink.weights, [1, -1, 1, -1]);
    let cells = sink.payload[0].bytes.as_chunks::<16>().0;
    let content = |i: usize| gnitz_wire::german_string_content(&cells[i], &sink.blob);
    assert_eq!(content(1), content(3));
    assert_eq!(content(3), b"a value well past the inline cell");
    assert_eq!(content(2), b"ab");
}

/// A region list that does not lay out its schema — a region short, or one
/// not as long as its rows — is a `DecodeError`.
#[test]
fn a_region_list_that_does_not_lay_out_its_schema_is_refused() {
    let schema = kv_schema(TypeCode::I64);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).int_val(7);
    let regions = batch.wire_regions();
    let decode = |regions: &[&[u8]]| ZSetBatch::from_own_regions(&schema, regions);
    decode(&regions).expect("the batch's own regions decode");

    let short = &regions[..regions.len() - 1];
    let mut ragged = regions.to_vec();
    ragged[gnitz_wire::REG_PAYLOAD_START] = &[0; 4];
    for (case, list) in [
        ("a region short", short),
        ("a ragged region", &ragged[..]),
        ("no regions", &[]),
    ] {
        assert!(matches!(decode(list), Err(ProtocolError::DecodeError(_))), "{case}");
    }
}

/// A null bit under a NOT NULL column is refused at decode, naming the column.
#[test]
fn a_null_bit_under_a_not_null_column_is_a_decode_error() {
    let schema = kv_schema(TypeCode::I64);
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b);
        a.add_row(1, 1).int_val(7);
        a.add_row(2, 1).int_val(8);
    }
    let mut sink = b.clone();
    b.nulls[1] = 1;
    match decode_wal_block(&encode_wal_block(&b), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("NOT NULL column 'v'"), "{m}"),
        other => panic!("expected a NOT NULL decode error, got {other:?}"),
    }

    let before = sink.clone();
    assert!(decode_wal_block_into(&mut sink, &encode_wal_block(&b), &schema).is_err());
    assert_eq!(sink, before, "a refused block leaves the sink as it was");
}
