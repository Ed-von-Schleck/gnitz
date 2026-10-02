use gnitz_expr::{LogicalInstr, LogicalProgram, Reg, Sink};
use gnitz_wire::{MapKind, NullKeys};

use super::MapPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode, MAX_COLUMNS};
use crate::test_support::{
    make_batch, make_schema_i64pk_i64, make_schema_u64_i64, opk_pk, read_german_string, weighted_rows,
};

/// `(pk, weight, payload cells)` rows against `schema`, `None` a NULL cell.
fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, &[Option<i64>])]) -> Batch {
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
fn make_schema(col_types: &[TypeCode]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = col_types
        .iter()
        .enumerate()
        .map(|(i, &tc)| SchemaColumn::new(tc, i != 0))
        .collect();
    SchemaDescriptor::new(&cols, &[0])
}

/// A computed map of `program` over `in_schema` into the payload slots `out_cols`.
fn compute(in_schema: &SchemaDescriptor, program: LogicalProgram, out_cols: &[(TypeCode, bool)]) -> MapPlan {
    let map = gnitz_wire::ComputeMap {
        program: program.to_blob_bytes(),
        out_cols: out_cols.to_vec(),
    };
    MapPlan::from_compute_map(in_schema, &map).unwrap()
}

/// A projection of `in_schema` onto its payload columns `cols`.
fn project(in_schema: &SchemaDescriptor, cols: &[u32]) -> MapPlan {
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
    assert_eq!(read_german_string(&out, 0, 0), b"long-string-one-xyz"); // s2 → out payload 0
    assert_eq!(read_german_string(&out, 1, 0), b"ab"); // s1 → out payload 1
    assert_eq!(read_german_string(&out, 0, 1), b"long-string-two-abcdef");
    assert_eq!(read_german_string(&out, 1, 1), b"cd");

    // (B) Drop the long string s2 → passthrough gated OFF, so the relocate path runs and the
    // output blob carries only the referenced (here empty, short-inline) spans.
    let out = project(&in_schema, &[1]).evaluate_map_batch(&batch);
    assert_eq!(out.count, 2);
    assert_eq!(read_german_string(&out, 0, 0), b"ab");
    assert_eq!(read_german_string(&out, 0, 1), b"cd");
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
fn reindex_on(s: &SchemaDescriptor, key: &[u32], keep: Vec<u32>, nulls: NullKeys) -> MapKind {
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

// -----------------------------------------------------------------------
// Benchmark
// -----------------------------------------------------------------------

/// Retired instructions of the map driver: `evaluate_map_batch` over whole
/// batches, and `append_map_ranges` over one range (`*_whole`) and over `R`-row
/// runs with 16-row gaps (`*_r{R}`), where `COMPACT_RUN_LEN` decides.
/// Difference two pass counts, one shape per process:
///
///   cargo build -p gnitz-zset --release --tests
///   for s in reindex permute proj_keep_str proj_drop_str int3_whole int3_r16 upper_r16 permute_r1; do for p in 1 21; do \
///     GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-zset --release map_ranges_bench -- --ignored --nocapture
///   done; done
#[test]
#[ignore]
fn map_ranges_bench() {
    use gnitz_expr::IntArithOp;
    use std::hint::black_box;

    const N: usize = 262_144;
    const GAP: usize = 16;
    let passes: usize = std::env::var("GNITZ_BENCH_PASSES").map_or(1, |v| v.parse().unwrap());
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    let driven = |name: &str| only == "all" || only == name;
    let mut n_selected = 0usize;
    let mut acc = 0usize;

    // --- Reindex map: [U64 PK, I64, I64] reindexed on col 1, the source PK and
    // both payload columns kept — the equijoin / GROUP BY repartition shape.
    // Column 0 is what puts a `ColumnLocator::Pk` copy in the loop; the keep-set
    // rules retain the source PK on every one of those.
    let rx_in = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64]);
    let mut rx_batch = BatchBuilder::new(&rx_in);
    for i in 0..N as u64 {
        rx_batch.begin_row(i as u128, 1i64);
        rx_batch.put_int(i.wrapping_mul(2_654_435_761) as u128);
        rx_batch.put_int((!i) as u128);
        rx_batch.end_row();
    }
    let rx_batch = rx_batch.finish();
    let mut rx_plan = MapPlan::from_wire(&rx_in, &reindex_on(&rx_in, &[1], vec![0, 1, 2], NullKeys::Keep)).unwrap();

    // --- String source: [U64 PK, STRING], every cell past the inline threshold
    // so every one is heap-backed and an emit grows the output blob.
    let str_in = make_schema(&[TypeCode::U64, TypeCode::String]);
    let mut str_batch = BatchBuilder::new(&str_in);
    for i in 0..N {
        str_batch.begin_row(i as u128, 1i64);
        str_batch.put_blob(format!("row-{i:012}-payload").as_bytes());
        str_batch.end_row();
    }
    let str_batch = str_batch.finish();
    let upper = || {
        let prog = LogicalProgram::new(
            vec![
                LogicalInstr::LoadColStr { col: 1 },
                LogicalInstr::StrCase { a: Reg(0), upper: true },
            ],
            vec![Sink::Reg(Reg(1))],
            vec![],
        );
        compute(&str_in, prog, &[(TypeCode::String, true)])
    };
    let mut se_plan = upper();

    // --- Integer source: [U64 PK, I64, I64, I64], and a map computing three
    // columns out of it.
    let int_in = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64, TypeCode::I64]);
    let mut int_batch = BatchBuilder::new(&int_in);
    for i in 0..N as u64 {
        int_batch.begin_row(i as u128, 1);
        for pi in 0..3u64 {
            int_batch.put_int((i.wrapping_mul(2_654_435_761 + pi) % 1000) as u128);
        }
        int_batch.end_row();
    }
    let int_batch = int_batch.finish();
    let arith = |op, a, b| LogicalInstr::IntArith { op, a: Reg(a), b: Reg(b) };
    let int3 = || {
        let prog = LogicalProgram::new(
            vec![
                LogicalInstr::LoadCol { col: 1 },
                LogicalInstr::LoadCol { col: 2 },
                LogicalInstr::LoadCol { col: 3 },
                arith(IntArithOp::Add, 0, 1),
                arith(IntArithOp::Mul, 1, 2),
                arith(IntArithOp::Sub, 2, 0),
            ],
            vec![Sink::Reg(Reg(3)), Sink::Reg(Reg(4)), Sink::Reg(Reg(5))],
            vec![],
        );
        compute(&int_in, prog, &[(TypeCode::I64, true); 3])
    };
    // A pure projection that moves every payload column to another slot.
    let mut permute = project(&int_in, &[3, 1, 2]);

    // --- Hash-row maps: [U64 PK, payload...] -> [U128 hash PK, payload...], the
    // set-op / DISTINCT leaf. Fixed-width shapes on both sides of the fold's
    // stack arm, and heap-backed strings beside integers.
    let hash_row = |name: &'static str, payload: &[TypeCode]| {
        let mut in_tcs = vec![TypeCode::U64];
        in_tcs.extend_from_slice(payload);
        let in_schema = make_schema(&in_tcs);
        let mut batch = BatchBuilder::new(&in_schema);
        for i in 0..N as u64 {
            batch.begin_row(i as u128, 1);
            for (pi, &tc) in payload.iter().enumerate() {
                if tc == TypeCode::String {
                    batch.put_string(&format!("row-{i:012}-payload"));
                } else {
                    batch.put_int(i.wrapping_mul(2_654_435_761 + pi as u64) as u128);
                }
            }
            batch.end_row();
        }
        let batch = batch.finish();
        let cols = (1..).zip(payload.iter().copied()).collect();
        let plan = MapPlan::from_wire(&in_schema, &MapKind::HashRow { cols }).unwrap();
        (name, plan, batch)
    };
    const I64: TypeCode = TypeCode::I64;
    const STR: TypeCode = TypeCode::String;
    let mut hash_rows = [
        hash_row("hash_row[i64x2]", &[I64, I64]),
        hash_row("hash_row[i64x10]", &[I64; 10]),
        hash_row("hash_row[i64,str]", &[I64, STR]),
        hash_row("hash_row[i64x3,str]", &[I64, I64, I64, STR]),
    ];

    // --- Two heap-backed string columns, [U64 PK, STRING, STRING], and a
    // projection keeping both, or only the first.
    let str2_in = make_schema(&[TypeCode::U64, TypeCode::String, TypeCode::String]);
    let mut str2_batch = BatchBuilder::new(&str2_in);
    for i in 0..N {
        str2_batch.begin_row(i as u128, 1i64);
        str2_batch.put_string(&format!("first-{i:016}-payload-first"));
        str2_batch.put_string(&format!("second-{i:016}-payload-second"));
        str2_batch.end_row();
    }
    let str2_batch = str2_batch.finish();
    let (mut keep_str, mut drop_str) = (project(&str2_in, &[1, 2]), project(&str2_in, &[1]));

    // --- Column copies: one column of `[pk..., payload]` copied into an I64 slot,
    // widened from a PK or a payload cell, or decoded at its own width.
    let col_copy = |name: &'static str, pk: &[TypeCode], payload: TypeCode, col: u32| {
        let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
        cols.push(SchemaColumn::new(payload, false));
        let in_schema = SchemaDescriptor::new(&cols, &(0..pk.len() as u32).collect::<Vec<_>>());
        let mut batch = BatchBuilder::new(&in_schema);
        for i in 0..N as u64 {
            let natives: Vec<u128> = (0..pk.len() as u64).map(|c| (i + c) as u32 as u128).collect();
            batch.begin_row_natives(&natives, 1);
            batch.put_int(i.wrapping_mul(2_654_435_761) as u16 as u128);
            batch.end_row();
        }
        let plan = compute(&in_schema, LogicalProgram::copy_cols(&[col]), &[(TypeCode::I64, false)]);
        (name, plan, batch.finish())
    };
    let mut col_copies = [
        col_copy("widen_pk_i32", &[TypeCode::I32], TypeCode::I64, 0),
        col_copy("widen_pk_i32_u64", &[TypeCode::I32, TypeCode::U64], TypeCode::I64, 0),
        col_copy("widen_payload_i32", &[TypeCode::U64], TypeCode::I32, 1),
        col_copy("widen_payload_u16", &[TypeCode::U64], TypeCode::U16, 1),
        col_copy("copy_pk_i64", &[TypeCode::I64], TypeCode::I64, 0),
    ];

    // Whole-batch shapes, through `evaluate_map_batch`.
    let whole = [
        ("reindex", &mut rx_plan, &rx_batch),
        ("str_emit", &mut se_plan, &str_batch),
        ("permute", &mut permute, &int_batch),
        ("proj_keep_str", &mut keep_str, &str2_batch),
        ("proj_drop_str", &mut drop_str, &str2_batch),
    ]
    .into_iter()
    .chain(hash_rows.iter_mut().map(|(name, plan, batch)| (*name, plan, &*batch)))
    .chain(col_copies.iter_mut().map(|(name, plan, batch)| (*name, plan, &*batch)));
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for (name, plan, src) in whole {
        if !driven(name) {
            continue;
        }
        n_selected += 1;
        // Untimed: the batch pool holds an arena of the output's size.
        black_box(plan.evaluate_map_batch(src));
        let ((), instructions) = counter.measure(|| {
            for _ in 0..passes {
                let out = plan.evaluate_map_batch(black_box(src));
                acc = acc.wrapping_add(out.count).wrapping_add(out.blob().len());
                black_box(&out);
            }
        });
        println!(
            "map_ranges_bench {name}: {:.1} instr/row",
            instructions as f64 / (passes * N) as f64
        );
    }

    // Range shapes, through `append_map_ranges`: one range, and `r`-row runs with
    // `GAP`-row gaps.
    let runs = |r: usize| -> Vec<(usize, usize)> { (0..N).step_by(r + GAP).map(|s| (s, (s + r).min(N))).collect() };
    let mut shapes: Vec<(String, Vec<(usize, usize)>)> = vec![("whole".to_string(), vec![(0, N)])];
    shapes.extend([1usize, 4, 16, 64, 128, 256].map(|r| (format!("r{r}"), runs(r))));
    for (family, mut plan, src) in [
        ("int3", int3(), &int_batch),
        ("upper", upper(), &str_batch),
        ("permute", project(&int_in, &[3, 1, 2]), &int_batch),
    ] {
        let mut keeper = Batch::empty_with_schema(plan.out_schema());
        for (suffix, ranges) in &shapes {
            if !driven(&format!("{family}_{suffix}")) {
                continue;
            }
            n_selected += 1;
            for _ in 0..passes {
                keeper.clear();
                plan.append_map_ranges(black_box(src), &mut keeper, ranges);
                acc = acc.wrapping_add(keeper.count).wrapping_add(keeper.blob().len());
            }
            let survivors: usize = ranges.iter().map(|&(s, e)| e - s).sum();
            println!(
                "map_ranges_bench {family}_{suffix}: {} ranges, {survivors} survivors",
                ranges.len()
            );
        }
    }
    println!(
        "map_ranges_bench shape={only} passes={passes} n={N} acc={}",
        black_box(acc)
    );
    assert!(n_selected > 0, "GNITZ_BENCH_SHAPE matched nothing: {only:?}");
}

/// A `Drop` reindex against the `IS NOT NULL` filter it replaces, at NULL keys
/// spread evenly so every survivor run is short.
/// `cd crates && cargo test -p gnitz-zset --release reindex_drop_null_keys_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn reindex_drop_null_keys_bench() {
    use std::hint::black_box;
    const N: usize = 64 * 1024;
    const ITERS: usize = 50;
    let s = make_schema(&[TypeCode::U64, TypeCode::I64, TypeCode::I64, TypeCode::I64]);
    let counters = [
        gnitz_foundation::perf::Counter::instructions(),
        gnitz_foundation::perf::Counter::cycles(),
    ];
    let plan = |nulls| MapPlan::from_wire(&s, &reindex_on(&s, &[1], vec![1, 2, 3], nulls)).unwrap();
    let mut not_null = LogicalProgram::new(
        vec![LogicalInstr::IsNull { col: 1, invert: true }],
        vec![Sink::Reg(Reg(0))],
        vec![],
    )
    .resolve_filter(&s)
    .unwrap();

    // A first pass that prints nothing: the allocator's first large batches pay
    // for mapping fresh memory, which would land on whichever arm runs first.
    for (warm, null_every) in [
        (true, 10),
        (false, 0usize),
        (false, 100),
        (false, 32),
        (false, 16),
        (false, 10),
    ] {
        let is_null = |i: usize| null_every != 0 && i.is_multiple_of(null_every);
        let cells: Vec<[Option<i64>; 3]> = (0..N)
            .map(|i| {
                [
                    (!is_null(i)).then_some((i as i64 * 7919) % 4096),
                    Some(i as i64),
                    Some(-(i as i64)),
                ]
            })
            .collect();
        let rows: Vec<(u64, i64, &[Option<i64>])> =
            cells.iter().enumerate().map(|(i, c)| (i as u64, 1, &c[..])).collect();
        let batch = make_int_batch(&s, &rows);
        let (mut keep, mut drop) = (plan(NullKeys::Keep), plan(NullKeys::Drop));
        let pct = if null_every == 0 {
            0.0
        } else {
            100.0 / null_every as f64
        };
        let arm = |name: &str, f: &mut dyn FnMut()| {
            if warm {
                return f();
            }
            let t = crate::test_support::bench_time(ITERS, &mut *f);
            let per_row = |v: f64| v / N as f64;
            let [instrs, cycles] = counters.each_ref().map(|c| {
                c.as_ref()
                    .map_or("-".into(), |c| format!("{:.1}", per_row(c.measure(&mut *f).1 as f64)))
            });
            println!(
                "{pct:>4.1}% NULL  {name:<28} {:>7.2} ns/row  {instrs:>7} instr/row  {cycles:>7} cycles/row",
                per_row(t.as_nanos() as f64 / ITERS as f64),
            );
        };
        arm("(a) filter + reindex", &mut || {
            let filtered = crate::algebra::op_filter(&batch, &mut not_null);
            black_box(keep.evaluate_map_batch(filtered.as_ref().unwrap_or(&batch)));
        });
        arm("(a) drop reindex", &mut || {
            black_box(drop.evaluate_map_batch(&batch));
        });
        arm("(b) reindex + clone", &mut || {
            let all = keep.evaluate_map_batch(&batch);
            black_box(Batch::clone(&all));
            black_box(all);
        });
        arm("(b) keep + drop reindex", &mut || {
            black_box(drop.evaluate_map_batch(&batch));
            black_box(keep.evaluate_map_batch(&batch));
        });
    }
}
