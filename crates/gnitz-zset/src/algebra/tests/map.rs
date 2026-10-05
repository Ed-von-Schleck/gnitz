use gnitz_expr::{LogicalInstr, LogicalProgram, Reg, Sink};
use gnitz_wire::{MapKind, NullKeys};

use super::MapPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode, MAX_COLUMNS};
use crate::test_support::{make_batch, make_schema_i64pk_i64, make_schema_u64_i64, opk_pk, weighted_rows};

/// `(pk, weight, payload cells)` rows against `schema`, `None` a NULL cell.
pub(super) fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, &[Option<i64>])]) -> Batch {
    let mut batch = BatchBuilder::new(schema);
    for &(pk, weight, cells) in rows {
        batch.begin_row(pk as u128, weight);
        for &cell in cells {
            batch.put_opt_int(cell.map(|v| v as u128));
        }
        batch.end_row();
    }
    batch.finish()
}

/// A U64 PK at column 0, then nullable payload columns of `col_types[1..]`.
pub(super) fn make_schema(col_types: &[TypeCode]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = col_types
        .iter()
        .enumerate()
        .map(|(i, &tc)| SchemaColumn::new(tc, i != 0))
        .collect();
    SchemaDescriptor::new(&cols, &[0])
}

/// A computed map of `program` over `in_schema` into the payload slots `out_cols`.
pub(super) fn compute(in_schema: &SchemaDescriptor, program: LogicalProgram, out_cols: &[(TypeCode, bool)]) -> MapPlan {
    let map = gnitz_wire::ComputeMap {
        program: program.to_blob_bytes(),
        out_cols: out_cols.to_vec(),
    };
    MapPlan::from_compute_map(in_schema, &map).unwrap()
}

/// A projection of `in_schema` onto its payload columns `cols`.
pub(super) fn project(in_schema: &SchemaDescriptor, cols: &[u32]) -> MapPlan {
    MapPlan::from_wire(in_schema, &MapKind::Projection(cols.to_vec())).unwrap()
}

/// Every row keeps its PK and weight, NULLs included, while copied and
/// computed columns fill the output slots in sink order.
#[test]
fn a_computed_map_copies_and_computes_every_row_at_its_weight() {
    use gnitz_expr::IntArithOp;
    let in_schema = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let prog = LogicalProgram::new(
        vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::LoadCol { col: 2 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        vec![Sink::Col(2), Sink::Col(1), Sink::Reg(Reg(2))],
        vec![],
    );
    let mut plan = compute(&in_schema, prog, &[(TypeCode::I64, true); 3]);

    let input = make_int_batch(
        &in_schema,
        &[
            (1, 3, &[Some(10), Some(20)]),
            (2, -2, &[None, Some(40)]),
            (3, 1, &[Some(-5), Some(7)]),
        ],
    );
    let want = make_int_batch(
        plan.out_schema(),
        &[
            (1, 3, &[Some(20), Some(10), Some(30)]),
            (2, -2, &[Some(40), None, None]),
            (3, 1, &[Some(7), Some(-5), Some(2)]),
        ],
    );
    assert_eq!(weighted_rows(&plan.evaluate_map_batch(&input)), weighted_rows(&want));

    let empty = plan.evaluate_map_batch(&Batch::empty_with_schema(&in_schema));
    assert_eq!((empty.count, empty.schema()), (0, plan.out_schema()));
}

#[test]
fn test_map_blob_passthrough_and_fallback() {
    // Input: [U64 PK, STRING s1 (short inline), STRING s2 (long, heap-backed)].
    let in_schema = make_schema(&[TypeCode::U64, TypeCode::String, TypeCode::String]);
    let mut batch = BatchBuilder::new(&in_schema);
    for (pk, s1, s2) in [
        (1u128, b"ab".as_slice(), b"long-string-one-xyz".as_slice()),
        (2u128, b"cd".as_slice(), b"long-string-two-abcdef".as_slice()),
    ] {
        batch.begin_row(pk, 1);
        batch.put_blob(s1);
        batch.put_blob(s2);
        batch.end_row();
    }
    let batch = batch.finish();

    // (A) Keep BOTH string columns (reordered) → passthrough fires; the shared
    // blob keeps every long string's heap offset valid through the verbatim copy.
    let out = project(&in_schema, &[2, 1]).evaluate_map_batch(&batch);
    assert_eq!(out.count, 2);
    assert_eq!(gnitz_expr::payload_bytes(&out, 0, 0), b"long-string-one-xyz"); // s2 → out payload 0
    assert_eq!(gnitz_expr::payload_bytes(&out, 0, 1), b"ab"); // s1 → out payload 1
    assert_eq!(gnitz_expr::payload_bytes(&out, 1, 0), b"long-string-two-abcdef");
    assert_eq!(gnitz_expr::payload_bytes(&out, 1, 1), b"cd");

    // (B) Drop the long string s2 → passthrough gated OFF, so the relocate path runs and the
    // output blob carries only the referenced (here empty, short-inline) spans.
    let out = project(&in_schema, &[1]).evaluate_map_batch(&batch);
    assert_eq!(out.count, 2);
    assert_eq!(gnitz_expr::payload_bytes(&out, 0, 0), b"ab");
    assert_eq!(gnitz_expr::payload_bytes(&out, 1, 0), b"cd");
    assert!(
        out.blob().len() < batch.blob().len(),
        "dropped-string relocate must not copy the dead heap ({} vs {})",
        out.blob().len(),
        batch.blob().len(),
    );
}

/// A narrow source column copied into a wider promoted output slot (the shape a
/// cross-width set-op UNION produces) sign/zero-extends per the SOURCE column's
/// signedness — from the payload region and from the PK region alike.
#[test]
fn test_map_copy_col_widens_into_promoted_slot() {
    // PK = (U16 c0, I16 c1); payload = I8 c2 (negative), U8 c3.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U16, false),
            SchemaColumn::new(TypeCode::I16, false),
            SchemaColumn::new(TypeCode::I8, false),
            SchemaColumn::new(TypeCode::U8, false),
        ],
        &[0, 1],
    );
    let (c0, c1, c2, c3): (u16, i16, i8, u8) = (0xBEEF, -300, -7, 0xFE);
    let mut batch = BatchBuilder::new(&in_schema);
    batch.begin_row_natives(&[c0 as u128, c1 as u16 as u128], 1i64);
    batch.put_int(c2 as u128);
    batch.put_int(c3 as u128);
    batch.end_row();
    let batch = batch.finish();

    // Every column lands in an I64 slot — 4 distinct narrow→wide widens.
    let out = compute(
        &in_schema,
        LogicalProgram::copy_cols(&[0, 1, 2, 3]),
        &[(TypeCode::I64, false); 4],
    )
    .evaluate_map_batch(&batch);
    assert_eq!(out.count, 1);
    let widened = |pi: usize| i64::from_le_bytes(out.col_data(pi)[..8].try_into().unwrap());
    assert_eq!(widened(0), c0 as i64, "U16 PK zero-extends");
    assert_eq!(widened(1), c1 as i64, "I16 PK sign-extends");
    assert_eq!(widened(2), c2 as i64, "I8 payload sign-extends");
    assert_eq!(widened(3), c3 as i64, "U8 payload zero-extends");
}

/// A widened copy over many rows, from a PK column that is the whole key (cells
/// back to back) and from one inside a wider key, and from a payload column.
#[test]
fn a_widened_copy_holds_each_rows_value() {
    use TypeCode::{I16, I32, I64, U16, U32, U64, U8};
    let vals: Vec<i64> = (0..300i64).map(|i| (i - 150) * 7_654_321).collect();
    for (src, slot) in [(I32, I64), (U32, I64), (I16, I32), (U16, U64), (U8, I16)] {
        let fi = gnitz_wire::FixedInt::from_type_code(src).unwrap();
        let native = |v: i64| (v as u128) & gnitz_wire::image_mask(fi.width());
        let want: Vec<i64> = vals.iter().map(|&v| fi.unpack(native(v))).collect();
        for (pk, col) in [(&[src][..], 0u32), (&[U64, src], 1), (&[U64], 1)] {
            let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
            cols.push(SchemaColumn::new(src, false));
            let in_schema = SchemaDescriptor::new(&cols, &(0..pk.len() as u32).collect::<Vec<_>>());
            let mut batch = BatchBuilder::new(&in_schema);
            for (row, &v) in vals.iter().enumerate() {
                let natives: Vec<u128> = pk
                    .iter()
                    .map(|&t| if t == src { native(v) } else { row as u128 })
                    .collect();
                batch.begin_row_natives(&natives, 1);
                batch.put_int(native(v));
                batch.end_row();
            }
            let out = compute(&in_schema, LogicalProgram::copy_cols(&[col]), &[(slot, false)])
                .evaluate_map_batch(&batch.finish());
            let w = slot.wire_stride();
            let got: Vec<&[u8]> = out.col_data(0).chunks_exact(w).collect();
            let want: Vec<Vec<u8>> = want.iter().map(|v| v.to_le_bytes()[..w].to_vec()).collect();
            assert_eq!(got, want, "{src} -> {slot}, PK {pk:?} column {col}");
        }
    }
}

/// A computed string beside a copy of its source column: the copy still
/// resolves against the blob the emit appends to, a heap-backed result lands in
/// the output's own blob, and a NULL source ships a zeroed computed cell — a
/// non-deterministic value there would leave an insert and its retraction
/// unable to cancel. The emit is the map's only computed column, so the compute
/// kernel runs on the string emit list alone.
#[test]
fn a_string_emit_composes_with_a_copy_of_its_source() {
    let in_schema = make_schema(&[TypeCode::U64, TypeCode::String]);
    let long = b"a-long-value-past-twelve".as_slice();
    let mut batch = BatchBuilder::new(&in_schema);
    for (pk, s) in [(1u128, long), (2, b"short"), (3, b"def")] {
        batch.begin_row(pk, 1);
        batch.put_blob(s);
        batch.end_row();
    }
    let mut batch = batch.finish();
    // Row 2's source is NULL, over real string bytes.
    gnitz_wire::write_u64_le(batch.null_bmp_data_mut(), 16, 1);

    let prog = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ],
        vec![Sink::Col(1), Sink::Reg(Reg(1))],
        vec![],
    );
    let mut plan = compute(&in_schema, prog, &[(TypeCode::String, true); 2]);
    let out = plan.evaluate_map_batch(&batch);

    let mut want = BatchBuilder::new(plan.out_schema());
    for (pk, cells) in [
        (1u128, Some((long, b"A-LONG-VALUE-PAST-TWELVE".as_slice()))),
        (2, Some((b"short", b"SHORT"))),
        (3, None),
    ] {
        want.begin_row(pk, 1);
        match cells {
            Some((copy, upper)) => {
                want.put_blob(copy);
                want.put_blob(upper);
            }
            None => {
                want.put_null();
                want.put_null();
            }
        }
        want.end_row();
    }
    assert_eq!(weighted_rows(&out), weighted_rows(&want.finish()));
    assert_eq!(
        &out.col_data(1)[32..48],
        &[0u8; 16],
        "a NULL computed cell is all zeros"
    );
}

/// A reindex stamps each row's PK with the OPK image of its key column — the
/// sign flip included — keeps the row's weight, and drops the consolidated claim.
#[test]
fn a_reindex_keys_each_row_by_the_opk_image_of_its_key_column() {
    let schema = make_schema_u64_i64();
    let rows = [(1, 1, 200), (2, -3, 100), (3, 1, -300)];
    let batch = make_batch(&schema, &rows);
    let out = MapPlan::from_wire(&schema, &reindex_on(&schema, &[1], vec![1], NullKeys::Keep))
        .unwrap()
        .evaluate_map_batch(&batch);
    assert_eq!(out.count, rows.len());
    for (r, &(_, w, v)) in rows.iter().enumerate() {
        assert_eq!(out.get_pk_bytes(r), opk_pk(&make_schema_i64pk_i64(), &[v as u128]));
        assert_eq!(out.get_weight(r), w);
        assert_eq!(gnitz_wire::read_i64_le(out.col_data(0), r * 8), v);
    }
    assert!(!out.is_consolidated(), "a stamped PK must drop the consolidated claim");
}

/// A hash-row map keys a row by its payload *content*, not by its cell bytes:
/// equal rows must land on one PK, or EXCEPT/INTERSECT cannot cancel them, and
/// unequal rows must not, or two elements coalesce into one.
#[test]
fn hash_row_keys_a_row_by_its_content_across_nulls_strings_and_blobs() {
    const LONG_A: &[u8] = b"the-quick-brown-fox";
    // Equal in the 4 bytes the cell inlines as its prefix, different past them.
    const LONG_B: &[u8] = b"the-quick-brown-cat";

    // (payload value, is NULL, STRING content, BLOB content). Each cell is
    // encoded as its row is built, so equal content lands at unequal offsets.
    let rows: &[(i64, bool, &[u8], &[u8])] = &[
        (5, false, LONG_A, b"blob-past-twelve-bytes"),
        // 1: row 0 again.
        (5, false, LONG_A, b"blob-past-twelve-bytes"),
        // 2..=4: one column changed each.
        (7, false, LONG_A, b"blob-past-twelve-bytes"),
        (5, false, LONG_B, b"blob-past-twelve-bytes"),
        (5, false, LONG_A, b"blob-past-twelve-other"),
        // 5, 6: NULL payload cells whose underlying bytes differ.
        (111, true, LONG_A, b"blob-past-twelve-bytes"),
        (222, true, LONG_A, b"blob-past-twelve-bytes"),
        // 7, 8: inline cells, twice.
        (5, false, b"abc", b"abc"),
        (5, false, b"abc", b"abc"),
        // 9, 10: the two columns' contents swapped.
        (5, false, b"ab", b"cd"),
        (5, false, b"cd", b"ab"),
    ];

    // The rows repeat past the map's key chunk, so every chunk must key alike.
    const REPEATS: usize = 60;
    let in_schema = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::String, TypeCode::Blob]);
    let mut batch = BatchBuilder::new(&in_schema);
    for row in 0..rows.len() * REPEATS {
        let (v, is_null, s, b) = rows[row % rows.len()];
        batch.begin_row(row as u128, 1);
        match is_null {
            true => batch.put_null(),
            false => batch.put_int(v as u128),
        }
        batch.put_blob(s);
        batch.put_blob(b);
        batch.end_row();
    }
    let batch = batch.finish();

    let hash_row = MapKind::HashRow {
        cols: vec![(1, TypeCode::I64), (2, TypeCode::String), (3, TypeCode::Blob)],
    };
    let out = MapPlan::from_wire(&in_schema, &hash_row)
        .unwrap()
        .evaluate_map_batch(&batch);
    assert_eq!(out.count, batch.count);
    let pk = |row: usize| out.get_pk_bytes(row);

    for row in rows.len()..out.count {
        assert_eq!(pk(row), pk(row % rows.len()), "row {row} keys unlike its first copy");
    }
    assert_eq!(pk(0), pk(1), "equal content must key equally, at whatever heap offset");
    assert_eq!(
        pk(5),
        pk(6),
        "a NULL cell hashes as its marker, not as the bytes under it"
    );
    assert_eq!(pk(7), pk(8), "and so must two rows of inline cells");
    for (a, b, what) in [
        (0, 2, "a differing payload value"),
        (0, 3, "a long string differing only past its inline prefix"),
        (0, 4, "a differing BLOB beside an equal STRING"),
        (0, 5, "a NULL cell against a non-NULL one"),
        (0, 7, "inline cells against heap-backed ones"),
        (9, 10, "the two string columns' contents swapped"),
    ] {
        assert_ne!(pk(a), pk(b), "{what} must move the PK");
    }
    assert!(!out.is_consolidated(), "a stamped PK must not be marked consolidated");
}

/// Every PK width is its own monomorphization in `copy_column`'s unswitched PK
/// arm. A compound PK whose list order is not its column order drives all five at
/// once: each column lands in a payload slot of its own type, as the native
/// little-endian value with the OPK sign flip undone, while the PK region and
/// the weights carry over unchanged.
#[test]
fn a_compound_permuted_pk_decodes_at_every_width() {
    let pk_types = [
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I128,
    ];
    // PK-list order, deliberately not column order: each column's OPK byte
    // offset comes from this list, so a width mixed up here would be caught as
    // a wrong value rather than a wrong length.
    let pk_order: [u32; 5] = [3, 0, 4, 1, 2];
    let cols = pk_types.map(|t| SchemaColumn::new(t, false));
    let in_schema = SchemaDescriptor::new(&cols, &pk_order);

    // Both extremes and a wrap-adjacent value per width: the sign flip is what
    // orders these, and dropping it shows up first at the ends.
    let rows: [([i128; 5], i64); 3] = [
        ([i8::MIN as i128, 0, i32::MIN as i128, 0, i128::MIN], 3),
        ([-1, 40_000, -1, u64::MAX as i128, -1], -1),
        ([i8::MAX as i128, u16::MAX as i128, i32::MAX as i128, 1, i128::MAX], 2),
    ];
    let mut batch = BatchBuilder::new(&in_schema);
    for (vals, w) in &rows {
        let natives: Vec<u128> = pk_order.iter().map(|&ci| vals[ci as usize] as u128).collect();
        batch.begin_row_natives(&natives, *w);
        batch.end_row();
    }
    let batch = batch.finish();

    // One payload slot per PK column at that column's own type, so the copy
    // widens nothing and the bytes written are exactly what the decode produced.
    let out = compute(
        &in_schema,
        LogicalProgram::copy_cols(&[0, 1, 2, 3, 4]),
        &pk_types.map(|t| (t, true)),
    )
    .evaluate_map_batch(&batch);
    assert_eq!(out.pk_data(), batch.pk_data());
    assert_eq!(out.weight_data(), batch.weight_data());
    for (row, (vals, _)) in rows.iter().enumerate() {
        for (pi, (&v, &t)) in vals.iter().zip(&pk_types).enumerate() {
            let w = t.wire_stride();
            assert_eq!(
                &out.col_data(pi)[row * w..row * w + w],
                &(v as u128).to_le_bytes()[..w],
                "row {row}, PK column {pi} (type {t}) decoded wrong",
            );
        }
    }
}

// ── MapPlan::from_wire — the circuit-node trust boundary ────────────────
//
// Every kind's column lists are client-supplied catalog data: each way they can
// overrun a schema array, address an unnumbered copy slot, or promote outside the
// copy kernel's domain must be a named rejection, not a panic or a truncated key.

/// The guard that refused `mk` over `in_schema`.
fn wire_rejection(in_schema: &SchemaDescriptor, mk: MapKind) -> String {
    MapPlan::from_wire(in_schema, &mk)
        .map(|_| "a plan")
        .expect_err("expected a rejection")
        .to_string()
}

#[test]
fn a_projection_must_name_payload_columns_that_fit_one_schema() {
    let s = make_schema_u64_i64();
    let proj = |cols: Vec<u32>| wire_rejection(&s, MapKind::Projection(cols));
    assert_eq!(
        proj(vec![200]),
        "projection map: column 200 is not a payload column of a 2-column schema"
    );
    // A PK source: the PK region already carries it.
    assert_eq!(
        proj(vec![0]),
        "projection map: column 0 is not a payload column of a 2-column schema"
    );
    // Each index is bounded but the list length is not, and duplicates are legal.
    // Exactly MAX_COLUMNS payload sources already overflow — the output also
    // carries the input's PK column, which a length-only bound misses.
    assert_eq!(
        proj(vec![1; MAX_COLUMNS]),
        format!(
            "projection map: column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})",
            MAX_COLUMNS + 1
        )
    );
}

/// A hash-row map over a `(U128 PK, I64)` input carries its payload column into
/// an identical layout, yet rewrites every PK: the program alone reads as an
/// identity, and only the PK source rules it out.
#[test]
fn a_hash_row_map_carrying_every_column_is_not_an_identity() {
    let s = make_schema(&[TypeCode::U128, TypeCode::I64]);
    let plan = MapPlan::from_wire(&s, &MapKind::HashRow { cols: vec![(1, TypeCode::I64)] })
        .expect("a well-formed hash-row map");
    assert_eq!(plan.out_schema(), &s, "the output layout is the input's");
    assert!(!plan.is_identity(), "a hash-row map is never an identity");
    assert!(
        project(&s, &[1]).is_identity(),
        "the same copy under the inherited PK is one"
    );
}

/// An auxiliary reindex on `key`, keeping `keep`.
fn reindex_slots(key: Vec<gnitz_wire::ReindexSlot>, keep: Vec<u32>, nulls: NullKeys) -> MapKind {
    MapKind::Reindex {
        keep,
        key,
        role: gnitz_wire::ReindexRole::Auxiliary,
        nulls,
    }
}

/// [`reindex_slots`] on `key` of `s`, each slot self-typed.
pub(super) fn reindex_on(s: &SchemaDescriptor, key: &[u32], keep: Vec<u32>, nulls: NullKeys) -> MapKind {
    reindex_slots(crate::test_support::self_typed_slots(s, key), keep, nulls)
}

#[test]
fn reindex_key_and_kept_column_lists_are_bounds_checked() {
    let reindex = |s: &SchemaDescriptor, keep, key: Vec<u32>| reindex_on(s, &key, keep, NullKeys::Keep);
    let three = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    // A compound key and a pruned payload both build — and neither is an
    // identity, since a reindex overwrites every row's PK.
    for (s, keep, key) in [
        (make_schema_u64_i64(), vec![0, 1], vec![0, 1]),
        (three, vec![2], vec![0]),
    ] {
        let plan = MapPlan::from_wire(&s, &reindex(&s, keep, key)).expect("a well-formed reindex");
        assert!(!plan.is_identity(), "a reindex is never an identity");
    }

    let s = make_schema_u64_i64();
    assert_eq!(
        wire_rejection(&s, reindex(&s, vec![9], vec![0])),
        "reindex map: payload column 9 out of range (2 cols)"
    );
    assert_eq!(
        wire_rejection(&s, reindex_slots(vec![(9, TypeCode::I64)], vec![0], NullKeys::Keep)),
        "reindex key: column 9 out of range (2 cols)"
    );
    // A key longer than MAX_PK_COLUMNS overflows the output schema's fixed PK array.
    let n = crate::schema::MAX_PK_COLUMNS + 1;
    let wide = SchemaDescriptor::new(&vec![SchemaColumn::new(TypeCode::U64, false); n], &[0]);
    assert_eq!(
        wire_rejection(&wide, reindex(&wide, vec![0], (0..n as u32).collect())),
        format!("reindex key: {n} columns exceeds the {}-column PK limit", n - 1)
    );
}

/// A `Drop` reindex over a nullable key is the filter-then-reindex it replaces,
/// wherever its NULL runs start and end — across a 64-row block, at the batch's
/// two ends, one row long, or covering every row. A `Keep` one re-keys the NULL
/// rows too.
#[test]
fn a_drop_reindex_is_filter_then_reindex_and_keep_keeps_null_keys() {
    // `(U64 pk, I64 NULL key, I64 NULL)`: the key is payload slot 0.
    let s = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let patterns: [&dyn Fn(usize) -> bool; 6] = [
        &|i| i % 7 == 0,
        &|i| (60..140).contains(&i),
        &|i| !(64..190).contains(&i),
        &|i| i % 2 == 1,
        &|i| i != 127,
        &|_| true,
    ];
    for (p, is_null) in patterns.iter().enumerate() {
        let cells: Vec<[Option<i64>; 2]> = (0..200)
            .map(|i| {
                let key = (!is_null(i)).then_some(i as i64 - 100);
                [key, (i % 3 != 0).then_some(-(i as i64))]
            })
            .collect();
        let rows = |keep_null: bool| -> Vec<(u64, i64, &[Option<i64>])> {
            (0..200)
                .filter(|&i| keep_null || !is_null(i))
                .map(|i| (i as u64, [3, -1, 1, 2, -2][i % 5], &cells[i][..]))
                .collect()
        };
        let (batch, filtered) = (make_int_batch(&s, &rows(true)), make_int_batch(&s, &rows(false)));
        for keep in [vec![2], vec![1, 2]] {
            let run = |nulls| MapPlan::from_wire(&s, &reindex_on(&s, &[1], keep.clone(), nulls)).unwrap();
            let mut drop = run(NullKeys::Drop);
            let dropped = drop.evaluate_map_batch(&batch);
            assert_eq!(dropped.schema(), drop.out_schema());
            assert_eq!(
                weighted_rows(&dropped),
                weighted_rows(&run(NullKeys::Keep).evaluate_map_batch(&filtered)),
                "pattern {p}"
            );
            assert_eq!(run(NullKeys::Keep).evaluate_map_batch(&batch).count, batch.count);
        }
    }
}

/// The set-op full-row identity map: a synthetic U128 PK over the projected
/// payload, each slot optionally promoted to a wider fixed-int type so a
/// cross-width pair (`I32 UNION I64`) hashes one physical layout.
#[test]
fn a_hash_row_map_promotes_within_the_copy_kernel_domain_or_is_rejected() {
    let hash_row = |c: u32, tc: TypeCode| MapKind::HashRow { cols: vec![(c, tc)] };
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
        ],
        &[0],
    );
    assert!(
        MapPlan::from_wire(&s, &hash_row(1, TypeCode::U32)).is_ok(),
        "no promotion"
    );
    assert!(
        MapPlan::from_wire(&s, &hash_row(1, TypeCode::I64)).is_ok(),
        "U32 → I64 is the ≤8-byte widen the copy kernel supports"
    );
    assert_eq!(
        wire_rejection(&s, hash_row(9, TypeCode::U32)),
        "hash-row map: column 9 out of range (2 cols)"
    );
    assert!(
        wire_rejection(&s, hash_row(1, TypeCode::String)).starts_with("map: program/schema mismatch"),
        "a German string is not a fixed-int widen"
    );
}

/// A corrupt computed-map blob, garbled or empty, is refused, not decoded into a
/// plan that maps garbage — and the rejection names the decode, not the schema.
#[test]
fn a_corrupt_compute_map_program_is_rejected() {
    for program in [vec![0xff; 8], Vec::new()] {
        let mk = MapKind::Compute(gnitz_wire::ComputeMap {
            program,
            out_cols: vec![(TypeCode::I64, false)],
        });
        assert!(wire_rejection(&make_schema_u64_i64(), mk).starts_with("map: invalid program"));
    }
}

/// A declaration wider than a schema holds is refused by the derivation.
#[test]
fn compute_map_refuses_an_over_wide_declaration() {
    let mk = MapKind::Compute(gnitz_wire::ComputeMap {
        program: vec![1, 2, 3],
        out_cols: vec![(TypeCode::I64, false); MAX_COLUMNS],
    });
    assert_eq!(
        wire_rejection(&make_schema_u64_i64(), mk),
        format!(
            "compute map: output column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})",
            MAX_COLUMNS + 1
        )
    );
}
