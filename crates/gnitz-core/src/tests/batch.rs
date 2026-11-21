use super::*;
use crate::test_support::{decode_wal_block, encode_wal_block, kv_schema};
use gnitz_expr::{BatchView, CmpOp, IntArithOp, LogicalInstr, LogicalProgram, Reg, ScalarCell, ScalarEval, Sink};
use gnitz_wire::{ColumnDef, PkKeys, TypeCode, payload_str, payload_u64};

/// A STRING/BLOB column region from its values, spilling into `blob`; `None` is
/// a NULL cell, which the region zero-fills.
fn german_col(vals: &[Option<&[u8]>], blob: &mut Vec<u8>) -> Vec<u8> {
    vals.iter()
        .flat_map(|v| gnitz_wire::encode_german_string(v.unwrap_or(&[]), blob))
        .collect()
}

/// A batch of `schema` from its regions: `payload` one per payload slot in slot order, each
/// in its schema column's type, against `heap`.
fn batch_of(
    schema: &Arc<Schema>,
    keys: &PkKeys,
    weights: &[i64],
    nulls: &[u64],
    payload: &[Vec<u8>],
    heap: &[u8],
) -> ZSetBatch {
    assert_eq!(
        payload.len(),
        schema.layout().num_payload_cols(),
        "one region per payload slot"
    );
    let rows = Batch::write_new_carrying(schema.layout(), weights.len(), 0, &[(heap, 0, 0)], |w| {
        let (pk, weight, null_bmp) = w.fixed_mut();
        pk.copy_from_slice(keys.as_bytes());
        weight.copy_from_slice(gnitz_wire::as_le_bytes(weights));
        null_bmp.copy_from_slice(gnitz_wire::as_le_bytes(nulls));
        for (pi, cells) in payload.iter().enumerate() {
            w.col_mut(pi).copy_from_slice(cells);
        }
    });
    ZSetBatch::of(schema, rows)
}

/// `null_breach` names each null-rule breach with its row and payload slot; a NULL over
/// a zeroed cell of a nullable column is not one.
#[test]
fn null_breach_names_each_malformed_row() {
    // `(pk U64 | v I64 | s STRING nullable)`, two valid rows, the second NULL in `s`.
    let schema = Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
                ColumnDef::new("s", TypeCode::String, true),
            ],
            &[0],
        )
        .unwrap(),
    );
    let mut base = ZSetBatch::new(&schema);
    {
        let mut a = base.builder();
        a.begin_row(1, 1).put_int(10).put_string("x");
        a.begin_row(2, -1).put_int(20).put_null();
    }
    assert_eq!(
        base.view().null_breach(),
        None,
        "a NULL over a zeroed nullable cell is no breach"
    );

    // (null bits set on row 0, the breach and its (row, payload slot))
    let cases = [
        (1u64, ("a null bit on a NOT NULL column", (0, 0))),
        (2, ("a non-zero cell under a NULL", (0, 1))),
        (1 << 2, ("a null bit on a NOT NULL column", (0, 2))),
    ];
    for (bits, want) in cases {
        let mut b = base.clone();
        let word = b.view().get_null_word(0) | bits;
        set_null_word(&mut b, 0, word);
        assert_eq!(b.view().null_breach(), Some(want));
    }
}

/// Batches that differ in one layout clause each are refused: the payload slot count, a
/// slot's type (whose cells would be read at another width), and a key column's sign.
#[test]
fn same_layout_refuses_each_differing_clause() {
    let key_only = |tc| Arc::new(Schema::from_parts(vec![ColumnDef::new("pk", tc, false)], &[0]).unwrap());
    for (a, b) in [
        (key_only(TypeCode::U64), kv_schema(TypeCode::U64)),
        (kv_schema(TypeCode::I64), kv_schema(TypeCode::F64)),
        (key_only(TypeCode::I64), key_only(TypeCode::U64)),
    ] {
        let e = std::panic::catch_unwind(|| ZSetBatch::new(&a).extend_from_owned(ZSetBatch::new(&b))).unwrap_err();
        assert_eq!(
            e.downcast_ref::<&str>(),
            Some(&"with_schema: the schema's region types are not the batch's")
        );
    }
}

/// The scalar key verbs are a single-column key's; a compound key is written one
/// native value per column.
#[test]
fn a_compound_key_has_no_scalar_form() {
    let schema = fixture_a_schema();
    let panics = |f: &(dyn Fn() + std::panic::RefUnwindSafe)| std::panic::catch_unwind(f).is_err();
    let mut b = ZSetBatch::new(&schema);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            b.builder().begin_row(1, 1);
        }))
        .is_err()
    );
    assert!(b.is_empty(), "a refused key leaves no partial row");
    assert!(panics(&|| _ = fixture_a_batch().pk(0)));
    for arity in [&[1][..], &[1, 2, 3]] {
        assert!(panics(&|| _ = schema.pk_keys([arity])), "{arity:?}");
    }
}

/// `extend_from_owned` concatenates rows across String and Bytes columns, and
/// shifts each moved German cell onto its body's new place in the arena.
#[test]
fn extend_from_owned_concatenates() {
    let schema = Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
                ColumnDef::new("b", TypeCode::Blob, false),
            ],
            &[0],
        )
        .unwrap(),
    );
    // Each half spills a body of its own, so an unshifted cell reads the wrong one.
    let long = |base: u128| format!("spilled string number {base}");
    let build = |base: u128| {
        let mut z = ZSetBatch::new(&schema);
        {
            let mut a = z.builder();
            a.begin_row(base, 1).put_string(&long(base)).put_blob(&[1, 2, 3, 4, 5]);
            a.begin_row(base + 1, -1).put_string("short").put_blob(&[9, 9]);
        }
        z
    };

    let mut acc = build(1);
    acc.extend_from_owned(build(10));

    let pks: Vec<u128> = (0..acc.len()).map(|r| acc.pk(r)).collect();
    assert_eq!(pks, [1, 2, 10, 11]);
    assert_eq!(acc.view().weights().collect::<Vec<_>>(), [1, -1, 1, -1]);
    let strs: Vec<String> = (0..acc.len())
        .map(|r| payload_str(&acc.view(), r, 0).unwrap().into_owned())
        .collect();
    assert_eq!(strs, [long(1).as_str(), "short", long(10).as_str(), "short"]);
    assert_eq!(acc.view().null_breach(), None);
}

/// Every writer lands its value in its own payload slot — the PK column is
/// skipped wherever it sits — and `null()` sets the slot's bit over a zeroed
/// cell. A second appender continues the batch the first one left.
#[test]
fn the_appender_writes_each_value_into_its_slot() {
    let schema = Arc::new(
        Schema::from_parts(
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
        .unwrap(),
    );
    let spilled = "a string long enough to spill";
    let wide = (0xBEEF_u128 << 64) | 0xDEAD;
    let mut batch = ZSetBatch::new(&schema);
    batch
        .builder()
        .begin_row(7, 1)
        .put_int(100)
        .put_string(spilled)
        .put_int((-5i128) as u128)
        .put_float(1.5)
        .put_int(wide)
        .put_blob(b"xy");
    batch
        .builder()
        .begin_row(8, -1)
        .put_int(200)
        .put_string("short")
        .put_int(6)
        .put_float(-2.5)
        .put_int(1)
        .put_null();

    let le = |cells: &[[u8; 8]]| cells.concat();
    let mut blob = Vec::new();
    let payload = [
        le(&[100u64.to_le_bytes(), 200u64.to_le_bytes()]),
        german_col(&[Some(spilled.as_bytes()), Some(b"short")], &mut blob),
        le(&[(-5i64).to_le_bytes(), 6i64.to_le_bytes()]),
        le(&[1.5f64.to_le_bytes(), (-2.5f64).to_le_bytes()]),
        [wide.to_le_bytes(), 1u128.to_le_bytes()].concat(),
        german_col(&[Some(b"xy"), None], &mut blob),
    ];
    let want = batch_of(
        &schema,
        &schema.pk_keys([[7], [8]]),
        &[1, -1],
        &[0, 1 << 5],
        &payload,
        &blob,
    );
    assert_eq!(batch, want);
    assert_eq!(batch.view().null_breach(), None);
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
fn string_batch() -> (Arc<Schema>, ZSetBatch) {
    let schema = Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
                ColumnDef::new("n", TypeCode::I64, false),
            ],
            &[0],
        )
        .unwrap(),
    );
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = b.builder();
        for (i, v) in GATHER_VALS.iter().enumerate() {
            a.begin_row(i as u128, i as i64 + 1)
                .put_string(v)
                .put_int((i as i64 * 10) as u128);
        }
    }
    (schema, b)
}

/// A `string_batch` row as its values: `(pk, weight, s, n)`.
type Row = (u128, i64, String, u64);

fn rows(b: &ZSetBatch) -> Vec<Row> {
    let v = b.view();
    (0..b.len())
        .map(|r| {
            (
                b.pk(r),
                b.view().get_weight(r),
                payload_str(&v, r, 0).unwrap().into_owned(),
                payload_u64(&v, r, 1),
            )
        })
        .collect()
}

/// `retain_ranges` keeps the named runs in order; a moved German cell still reads
/// its spilled body out of the untouched arena.
#[test]
fn retain_ranges_keeps_the_named_runs_in_order() {
    let (_, b) = string_batch();
    let all = rows(&b);
    for (ranges, keep) in [
        (&[(0, 1), (2, 4)][..], &[0, 2, 3][..]),
        (&[(0, 4)][..], &[0, 1, 2, 3][..]),
        (&[(0, 2), (2, 4)][..], &[0, 1, 2, 3][..]),
        (&[][..], &[][..]),
    ] {
        let mut z = b.clone();
        z.retain_ranges(ranges);
        assert_eq!(z.view().null_breach(), None);
        let want: Vec<Row> = keep.iter().map(|&r| all[r].clone()).collect();
        assert_eq!(rows(&z), want, "{ranges:?}");
    }
}

/// A rollback drops every row since the mark, with its spilled string; a row opened and
/// never committed is no row at all.
#[test]
fn truncate_to_drops_the_rows_and_heap_since_the_mark() {
    let (_, want) = string_batch();
    let mut b = want.clone();
    let m = b.mark();
    b.builder().begin_row(9, 1).put_string(GATHER_VALS[0]).put_int(0);
    b.builder().begin_row(10, 1);
    b.truncate_to(m);
    assert_eq!(b, want);
}

/// A cut gather is the named rows at their weights, as a row-by-row rebuild holds them.
#[test]
fn a_cut_gather_matches_a_row_by_row_rebuild() {
    let (schema, b) = string_batch();
    let picks = [2u32, 0, 3];
    let mut want = ZSetBatch::new(&schema);
    for &r in &picks {
        want.copy_row_at(&b, r as usize, b.view().get_weight(r as usize));
    }
    let got = b.gather(&picks);
    assert_eq!(rows(&got), rows(&want));
    assert_eq!(got.view().null_breach(), None);
}

/// A gather naming every row in place is the batch itself.
#[test]
fn an_in_place_gather_is_the_batch() {
    let (_, b) = string_batch();
    let in_place: Vec<u32> = (0..b.len() as u32).collect();
    let arena = b.view().pk().as_ptr();
    let got = b.gather(&in_place);
    assert_eq!(got.view().pk().as_ptr(), arena);
    assert_eq!(got.view().null_breach(), None);
}

/// A gather keeping every row reorders it, every string read back as it was.
#[test]
fn a_whole_gather_reorders_every_row() {
    let (_, b) = string_batch();
    let perm = [3u32, 1, 0, 2];
    let all = rows(&b);
    let want: Vec<Row> = perm.iter().map(|&r| all[r as usize].clone()).collect();
    let got = b.gather(&perm);
    assert_eq!(rows(&got), want);
    assert_eq!(got.view().null_breach(), None);
}

/// A batch over a compound key with payload cells of every stride, NULLs among the
/// nullable ones, and `strings` STRING columns holding short and spilled values.
fn mixed_batch(strings: usize) -> (Arc<Schema>, ZSetBatch) {
    let mut columns = vec![
        ColumnDef::new("a", TypeCode::U32, false),
        ColumnDef::new("u8c", TypeCode::U8, true),
        ColumnDef::new("b", TypeCode::I64, false),
        ColumnDef::new("i16c", TypeCode::I16, false),
        ColumnDef::new("i32c", TypeCode::I32, true),
        ColumnDef::new("u128c", TypeCode::U128, false),
    ];
    columns.extend((0..strings).map(|_| ColumnDef::new("s", TypeCode::String, true)));
    let schema = Arc::new(Schema::from_parts(columns, &[2, 0]).unwrap());
    let mut b = ZSetBatch::new(&schema);
    let mut app = b.builder();
    for r in 0..40u128 {
        let row = app.begin_row_natives(&[r * 7 % 5, r], if r % 4 == 0 { -2 } else { 1 + r as i64 % 3 });
        if r % 3 == 0 {
            row.put_null();
        } else {
            row.put_int((r as i64 % 200) as u128);
        }
        row.put_int((-(r as i64)) as u128);
        if r % 5 == 0 {
            row.put_null();
        } else {
            row.put_int((r as i64 * 1000) as u128);
        }
        row.put_int(r << 100 | r);
        for s in 0..strings as u128 {
            match (r + s) % 3 {
                0 => row.put_null(),
                1 => row.put_string("short"),
                _ => row.put_string(GATHER_VALS[(r + s) as usize % GATHER_VALS.len()]),
            };
        }
    }
    assert_eq!(b.view().null_breach(), None);
    (schema, b)
}

/// A row's key, weight, null word and payload cells, a German cell by its content.
type RowCells = (Vec<u8>, i64, u64, Vec<Vec<u8>>);

/// Every row of `b` as [`RowCells`].
fn cells_of(b: &ZSetBatch) -> Vec<RowCells> {
    (0..b.len())
        .map(|r| {
            let cells = (0..b.view().schema().num_payload_cols())
                .map(|pi| {
                    let layout = b.view().schema();
                    let tc = layout.columns()[layout.payload_col_idx(pi)].type_code;
                    if tc.is_german_string() {
                        gnitz_wire::german_string_content(
                            &b.view().col_data(pi).as_chunks::<16>().0[r],
                            b.view().blob(),
                        )
                        .to_vec()
                    } else {
                        b.view().col_data(pi)[r * tc.wire_stride()..][..tc.wire_stride()].to_vec()
                    }
                })
                .collect();
            (
                b.view().get_pk_bytes(r).to_vec(),
                b.view().get_weight(r),
                b.view().get_null_word(r),
                cells,
            )
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
                want.copy_row_at(&b, r as usize, b.view().get_weight(r as usize));
            }
            let got = b.gathered(&picks);
            assert_eq!(got.view().null_breach(), None);
            if strings < 2 {
                assert_eq!(got, want, "{strings} strings, {picks:?}");
            } else {
                assert_eq!(cells_of(&got), cells_of(&want), "{picks:?}");
                assert_eq!(got.view().blob().len(), want.view().blob().len(), "{picks:?}");
            }
            assert_eq!(
                cells_of(&b.clone().gather(&picks)),
                cells_of(&want),
                "{strings} strings, {picks:?}"
            );
        }
    }
}

// ── The client batch as a `BatchView` ───────────────────────────────────

/// Every row's integer result of a scalar program.
fn row_values(ev: &mut ScalarEval, mb: &dyn BatchView) -> Vec<Option<i128>> {
    let mut vals = Vec::new();
    ev.for_each(mb, |cell| {
        let ScalarCell::Int(v) = cell else {
            panic!("row_values over a string-valued program")
        };
        vals.push(v);
    });
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

fn fixture_a_schema() -> Arc<Schema> {
    Arc::new(
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
        .expect("fixture A is a client-valid schema"),
    )
}

fn fixture_a_batch() -> ZSetBatch {
    let schema = fixture_a_schema();
    let pks = schema.pk_keys((0..3).map(|row| [PK3[row] as u128, PK0[row] as u128]));
    let mut blob = Vec::new();
    let payload = [
        C1.iter().flat_map(|v| v.to_le_bytes()).collect(),
        C2.iter().flat_map(|v| v.to_le_bytes()).collect(),
        german_col(&[Some(C4[0].as_bytes()), Some(C4[1].as_bytes()), None], &mut blob),
        german_col(&[Some(C5[0]), Some(C5[1]), None], &mut blob),
        C6.iter().flat_map(|v| v.to_le_bytes()).collect(),
    ];
    // Row 1 nulls ci2; row 2 nulls ci4 and ci5.
    batch_of(&schema, &pks, &[1, -1, 3], &[0, 0b10, 0b1100], &payload, &blob)
}

// ── Fixture B: a single narrow PK — the shape real traffic has ───────────

fn fixture_b_schema() -> Arc<Schema> {
    Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("k", TypeCode::U32, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            &[0],
        )
        .expect("fixture B is a client-valid schema"),
    )
}

const B_PK: [u64; 3] = [1, 2, u32::MAX as u64];

fn fixture_b_batch() -> ZSetBatch {
    let schema = fixture_b_schema();
    let mut b = ZSetBatch::new(&schema);
    let mut a = b.builder();
    for (pk, v) in B_PK.into_iter().zip([7i64, -8, 9]) {
        a.begin_row(pk as u128, 1).put_int(v as u128);
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
            let rows = batch.view();
            for (row, &v) in vals.iter().enumerate() {
                let mut scratch = [0u8; 16];
                let got = loc.native_le_bytes(&rows, row, &mut scratch);
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
        assert_eq!(row_values(&mut ev, &batch.view()), want, "ci3 + ci{ci}");
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

    assert_eq!(ev.ranges(&batch.view()), [(0, 1)]);
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
    assert_eq!(row_values(&mut ev, &batch.view()), [lt(0), lt(1), None]);
}

// ── Encode/decode round trip ─────────────────────────────────────────────

/// A batch survives encode → decode byte for byte, and decoding its block twice
/// into one sink is the batch extended by itself — the second block's German
/// cells shifted onto the sink's arena tail.
#[test]
fn batches_round_trip_through_a_wal_block() {
    // The widest admissible client PK: PK_LIST_MAX_COLS U128 columns.
    let wide = Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("a", TypeCode::U128, false),
                ColumnDef::new("b", TypeCode::U128, false),
                ColumnDef::new("c", TypeCode::U128, false),
                ColumnDef::new("d", TypeCode::U128, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            &[0, 1, 2, 3],
        )
        .expect("the widest admissible client PK"),
    );
    assert_eq!(wide.layout().pk_stride(), gnitz_wire::PK_LIST_MAX_COLS * 16);
    let mut wide_batch = ZSetBatch::new(&wide);
    for (key, w, v) in [
        ([u128::MAX, 1 << 64, 42, (1 << 64) + 7], 1, -1i64),
        ([9, 8, 7, 6], -2, 123),
    ] {
        wide_batch.builder().begin_row_natives(&key, w).put_int(v as u128);
    }

    let a = fixture_a_schema();
    let a_retraction = ZSetBatch::key_rows(&a, a.pk_keys([[10, 0], [20, 0]]).as_bytes(), -1);
    for (schema, batch) in [
        (fixture_a_schema(), fixture_a_batch()),
        (fixture_b_schema(), fixture_b_batch()),
        (wide, wide_batch),
        (fixture_a_schema(), a_retraction),
    ] {
        let block = encode_wal_block(&batch);
        let mut sink = decode_wal_block(&block, &schema).expect("block decodes");
        assert_eq!(sink, batch, "batch must survive encode -> decode");

        sink.extend_from(&decode_wal_block(&block, &schema).expect("block decodes"));
        let mut twice = batch.clone();
        twice.extend_from_owned(batch);
        assert_eq!(sink, twice, "a second block appends at the arena tail");
    }
}

// ── WAL blocks ─────────────────────────────────────────

/// A German cell whose heap offset overruns the heap is a `DecodeError`.
#[test]
fn a_string_cell_pointing_past_the_blob_is_refused() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    batch
        .builder()
        .begin_row(1, 1)
        .put_string("a value well past the inline cell");
    let mut forged = batch.clone();
    // A German cell's bytes 8..16 are its heap offset; the block copies it verbatim.
    forged.rewrite(|w| w.col_mut(0)[8..16].copy_from_slice(&u64::MAX.to_le_bytes()));

    match decode_wal_block(&encode_wal_block(&forged), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("not in canonical form"), "{m}"),
        other => panic!("expected a decode error, got {other:?}"),
    }
}

/// A null bit under a NOT NULL column is refused at decode.
#[test]
fn a_null_bit_under_a_not_null_column_is_a_decode_error() {
    let schema = kv_schema(TypeCode::I64);
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = b.builder();
        a.begin_row(1, 1).put_int(7);
        a.begin_row(2, 1).put_int(8);
    }
    set_null_word(&mut b, 1, 1);
    let block = encode_wal_block(&b);
    match decode_wal_block(&block, &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("NOT NULL column"), "{m}"),
        other => panic!("expected a NOT NULL decode error, got {other:?}"),
    }
}

// ── Decoding several blocks ─────────────────────────────────────────────

/// A decoded batch takes every write.
#[test]
fn a_decoded_batch_takes_every_write() {
    let (schema, batch) = string_batch();
    let mut got = decode_wal_block(&encode_wal_block(&batch), &schema).unwrap();
    let mut want = batch.clone();
    for b in [&mut got, &mut want] {
        b.rewrite(|w| {
            gnitz_wire::write_u64_le(w.col_mut(1), 8, 99);
            w.weight_mut()[..8].copy_from_slice(&(-7i64).to_le_bytes());
            let (cells, heap, _) = w.string_col_mut(0);
            let spilled = gnitz_wire::encode_german_string(b"a replacement long enough to spill as well", heap);
            cells[32..48].copy_from_slice(&spilled);
        });
        b.builder().begin_row(9, 2).put_string(GATHER_VALS[2]).put_int(3);
        b.retain_ranges(&[(1, 5)]);
    }
    assert_eq!(rows(&got), rows(&want));
    assert_eq!(got.view().null_breach(), None);
}

/// Bytes a frame carries in front of its data block and behind it.
const FRAME_HEAD: usize = 37;
const FRAME_TAIL: usize = 5;

/// `block` inside an owned frame of its own, between bytes that are not the block's.
fn owned_frame(block: &[u8]) -> FrameBlock<'static> {
    let frame = [&[0xA5; FRAME_HEAD][..], block, &[0x5A; FRAME_TAIL]].concat();
    FrameBlock {
        frame: FrameBytes::Owned(frame.into()),
        block: FRAME_HEAD..FRAME_HEAD + block.len(),
    }
}

/// A lone block of an owned frame is read where it lies, held or terminal: the regions and
/// the heap are the frame's bytes.
#[test]
fn a_lone_owned_block_is_read_in_its_frame() {
    for (schema, batch) in [
        (fixture_a_schema(), fixture_a_batch()),
        (fixture_b_schema(), fixture_b_batch()),
        string_batch(),
    ] {
        let block = encode_wal_block(&batch);
        for held in [false, true] {
            let frame = owned_frame(&block);
            let at = frame.frame.as_ptr() as usize + FRAME_HEAD + gnitz_wire::wal::WAL_HEADER_SIZE;
            let got = match held {
                false => decode_wal_blocks(Vec::new(), Some(frame), &schema),
                true => decode_wal_blocks(vec![frame], None, &schema),
            }
            .unwrap();
            assert_eq!(got, batch);
            assert_eq!(got.view().pk().as_ptr() as usize, at, "the regions are the frame's");
            assert_eq!(got.view().null_breach(), None);
        }
    }
}

/// Several blocks concatenate into one batch of their summed rows, each block's German
/// cells moved onto its heap's place behind the earlier ones.
#[test]
fn several_blocks_concatenate_into_one_arena() {
    let (schema, a) = string_batch();
    let b = a.gathered(&[3, 0]);
    let c = a.gathered(&[2]);
    let (ba, bb, bc) = (encode_wal_block(&a), encode_wal_block(&b), encode_wal_block(&c));
    let last = FrameBlock {
        frame: FrameBytes::Borrowed(&bc[..]),
        block: 0..bc.len(),
    };
    let got = decode_wal_blocks(vec![owned_frame(&ba), owned_frame(&bb)], Some(last), &schema).unwrap();
    let mut want = a.clone();
    want.extend_from(&b);
    want.extend_from(&c);
    assert_eq!(got, want);
    assert_eq!(got.view().null_breach(), None);

    // One bad block refuses the whole train.
    let mut forged = c.clone();
    forged.rewrite(|w| w.col_mut(0)[8..16].copy_from_slice(&u64::MAX.to_le_bytes()));
    let bf = encode_wal_block(&forged);
    assert!(decode_wal_blocks(vec![owned_frame(&ba)], Some(owned_frame(&bf)), &schema).is_err());
}

/// Once several blocks are joined, the owned frames they lay in go to the frame-buffer
/// pool, which hands each to the next frame of its size.
#[test]
fn joined_frames_go_to_the_pool() {
    // The pool holds buffers while a connection's deframer is alive.
    let _open = gnitz_wire::Deframer::<()>::default();
    let (schema, a) = string_batch();
    let block = encode_wal_block(&a);
    // Sizes no other test of the crate asks the pool for, neither within twice the other.
    let held = [3 << 20, 7 << 20].map(|cap| {
        let mut frame = Vec::with_capacity(cap);
        frame.extend_from_slice(&block);
        FrameBlock {
            frame: FrameBytes::Owned(frame.into()),
            block: 0..block.len(),
        }
    });
    let at = held.each_ref().map(|b| b.frame.as_ptr());
    let got = decode_wal_blocks(Vec::from(held), None, &schema).unwrap();
    assert_eq!(got.len(), 2 * a.len());
    for (cap, at) in [3 << 20, 7 << 20].into_iter().zip(at) {
        let again = gnitz_wire::frame_buf::try_alloc(cap).unwrap();
        assert_eq!((again.as_ptr(), again.capacity()), (at, cap), "the buffer given back");
    }
}

/// No block is the empty batch.
#[test]
fn no_block_is_the_empty_batch() {
    let schema = fixture_b_schema();
    assert_eq!(
        decode_wal_blocks(Vec::new(), None, &schema).unwrap(),
        ZSetBatch::new(&schema)
    );
}

/// Overwrite row `row`'s null word with `w`.
fn set_null_word(b: &mut ZSetBatch, row: usize, w: u64) {
    b.rewrite(|d| d.null_bmp_mut()[row * 8..(row + 1) * 8].copy_from_slice(&w.to_le_bytes()));
}
