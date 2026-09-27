use super::*;
use crate::{ColType, TypeCode};

fn col(tc: TypeCode, name: &str, nullable: bool) -> SchemaBlockCol<'_> {
    SchemaBlockCol {
        ty: ColType::of(tc),
        meta: ColMeta { nullable, ..Default::default() },
        name: name.as_bytes(),
    }
}

/// Every column `record` holds, and its PK indices.
fn decode_cols(record: &[u8]) -> Result<(Vec<SchemaBlockCol<'_>>, PkIndices), String> {
    let mut cols = Vec::new();
    let pk = decode(record, |c| {
        cols.push(c);
        Ok(())
    })?;
    Ok((cols, pk))
}

fn decode_err(record: &[u8]) -> String {
    decode_cols(record).expect_err("expected a rejection")
}

/// `n` U64 key columns, all of them in the PK, in column order.
fn all_key_cols(n: usize) -> Vec<SchemaBlockCol<'static>> {
    (0..n).map(|_| col(TypeCode::U64, "k", false)).collect()
}

fn simple() -> Vec<u8> {
    let cols = [
        col(TypeCode::U64, "id", false),
        col(TypeCode::String, "a_rather_long_column_name", true),
    ];
    encode(cols.iter().copied(), &[0])
}

#[test]
fn roundtrips_shape_names_and_pk_order() {
    let cols = [
        col(TypeCode::U64, "b", false),
        col(TypeCode::I32, "a", false),
        col(TypeCode::String, "payload_name_over_twelve", true),
    ];
    // Declared order `(a, b)`, not column order `(b, a)`.
    let record = encode(cols.iter().copied(), &[1, 0]);
    let (got, pk) = decode_cols(&record).unwrap();
    assert_eq!(got, cols.to_vec());
    assert_eq!(pk.as_slice(), &[1, 0]);
}

/// Every `ColMeta` field and the column type's scale survive the record.
#[test]
fn column_meta_survives_the_record() {
    let mut cols = Vec::new();
    for nullable in [false, true] {
        for hidden in [false, true] {
            // Scale 0 is every non-DECIMAL column, and `MAX_DECIMAL_SCALE` the
            // widest a DECIMAL admits — so neither edge may bleed into a
            // neighbour.
            for scale in [0u8, 7, crate::decimal::MAX_DECIMAL_SCALE] {
                cols.push(SchemaBlockCol {
                    ty: ColType::decimal(scale),
                    meta: ColMeta { nullable, hidden },
                    name: b"c",
                });
            }
        }
    }
    // Column 0 is the key, so it must not be nullable: swap it for a plain one.
    cols[0] = col(TypeCode::U64, "k", false);
    let record = encode(cols.iter().copied(), &[0]);
    assert_eq!(decode_cols(&record).unwrap().0, cols);
}

/// The record's arity cap is `MAX_PK_COLUMNS`, which the engine's intermediate
/// circuit schemas reach. A consumer with a narrower key limit applies it itself.
#[test]
fn a_five_column_pk_decodes_to_five_indices() {
    let cols = all_key_cols(MAX_PK_COLUMNS);
    let pk: Vec<u32> = (0..MAX_PK_COLUMNS as u32).collect();
    let record = encode(cols.iter().copied(), &pk);
    assert_eq!(decode_cols(&record).unwrap().1.as_slice(), pk.as_slice());
}

/// The one PK rule decode keeps: the arity bound on its own `pk_indices` array.
#[test]
fn a_pk_wider_than_max_pk_columns_is_refused() {
    let wide_pk: Vec<u32> = (0..=MAX_PK_COLUMNS as u32).collect();
    assert_eq!(
        decode_err(&encode(all_key_cols(MAX_PK_COLUMNS + 1).into_iter(), &wide_pk)),
        format!(
            "schema record: pk column count {} exceeds {MAX_PK_COLUMNS}",
            MAX_PK_COLUMNS + 1
        )
    );
}

/// A forged `column_count`: too large is out of range, and too small leaves the
/// column section with bytes nothing claimed.
#[test]
fn a_forged_column_count_is_rejected() {
    let mut over = simple();
    crate::write_u32_le(&mut over, 0, MAX_COLUMNS as u32 + 1);
    assert_eq!(
        decode_err(&over),
        format!(
            "schema record: column count {} out of range 1..={MAX_COLUMNS}",
            MAX_COLUMNS + 1
        )
    );

    let mut under = simple();
    crate::write_u32_le(&mut under, 0, 1);
    assert!(
        decode_err(&under).contains("trailing bytes"),
        "a lowered count must leave the second column's bytes unclaimed"
    );
}

/// Every guard that reads the column section, against the forgery that trips
/// it. The message is what separates them: a merged table asserting only "an
/// error" would pass on the wrong guard.
#[test]
fn each_column_guard_rejects_its_own_forgery() {
    // The column section opens right after `column_count`, `pk_count` and the
    // one PK index byte.
    const COL0: usize = 4 + 1 + 1;
    let cases: [(&str, usize, u8); 3] = [
        // No valid type code.
        ("schema record: invalid column type 0/0", COL0, 0),
        // A flags byte outside the three defined bits.
        ("schema record: unknown flag bits 0x08", COL0 + 1, 1 << 3),
        // A scale on a type that carries none.
        ("schema record: invalid column type 8/3", COL0 + 2, 3),
    ];
    for (want, off, byte) in cases {
        let mut record = simple();
        record[off] = byte;
        assert_eq!(decode_err(&record), want);
    }
}

/// A record cut at any length is an `Err`, never a panic.
#[test]
fn a_record_truncated_anywhere_is_an_error() {
    let record = simple();
    for cut in 0..record.len() {
        assert!(decode_cols(&record[..cut]).is_err(), "cut at {cut}");
    }
    assert!(decode_cols(&record).is_ok(), "the whole record still decodes");
}

/// Bytes past the last column mean the sender and this decoder disagree about
/// the layout.
#[test]
fn trailing_bytes_are_rejected() {
    let mut record = simple();
    record.push(0);
    assert!(decode_err(&record).contains("trailing bytes"));
}

/// A hostile name length cannot force an over-large read.
#[test]
fn a_forged_name_length_is_rejected() {
    let mut record = simple();
    // Column 0's name prefix: past `column_count`, `pk_count`, one PK index,
    // and the type/flags/scale bytes.
    crate::write_u32_le(&mut record, 4 + 1 + 1 + 3, u32::MAX);
    assert!(decode_err(&record).contains("truncated"));
}

/// The exact bytes of a compound-PK schema whose last column carries every flag
/// and a long name. The two adapters that build a record are pinned against each
/// other elsewhere, which a shift in both would pass; this pins the format
/// itself, which `gnitz-mirror` relies on as a view's schema identity.
#[test]
fn the_record_layout_is_fixed() {
    let long = "a_rather_long_column_name";
    let cols = [
        col(TypeCode::U64, "b", false),
        col(TypeCode::I32, "a", false),
        SchemaBlockCol {
            ty: ColType::decimal(9),
            meta: ColMeta { nullable: true, hidden: true },
            name: long.as_bytes(),
        },
    ];
    #[rustfmt::skip]
    let mut want: Vec<u8> = vec![
        3, 0, 0, 0,                  // column_count
        2,                           // pk_count
        1, 0,                        // PK indices, in declared PK-tuple order
        8, 0, 0, 1, 0, 0, 0, b'b',   // U64, no flags, scale 0, name "b"
        6, 0, 0, 1, 0, 0, 0, b'a',   // I32, no flags, scale 0, name "a"
        18, 0b11, 9, 25, 0, 0, 0,    // DECIMAL, nullable|hidden, scale 9
    ];
    want.extend_from_slice(long.as_bytes());
    assert_eq!(encode(cols.iter().copied(), &[1, 0]), want);
}

/// Each difference `check_same_types` compares is refused, and each names itself
/// distinctly — pairwise distinctness tests the message without pinning prose.
/// What it does not compare (names, the hidden flag) is admitted.
#[test]
fn check_same_types_names_each_mismatch_distinctly() {
    let two_cols =
        |second: SchemaBlockCol<'static>| encode([col(TypeCode::U64, "id", false), second].into_iter(), &[0]);
    let want = two_cols(col(TypeCode::I64, "v", false));
    assert_eq!(check_same_types(&want, &want), Ok(()), "a record matches itself");

    let renamed_and_hidden = two_cols(SchemaBlockCol {
        meta: ColMeta { nullable: false, hidden: true },
        ..col(TypeCode::I64, "renamed", false)
    });
    assert_eq!(
        check_same_types(&renamed_and_hidden, &want),
        Ok(()),
        "names and the hidden flag are not compared"
    );

    let cases = [
        ("count", encode([col(TypeCode::U64, "id", false)].into_iter(), &[0])),
        (
            "pk",
            encode(
                [col(TypeCode::U64, "id", false), col(TypeCode::I64, "v", false)].into_iter(),
                &[1],
            ),
        ),
        ("type", two_cols(col(TypeCode::F64, "v", false))),
        ("nullable", two_cols(col(TypeCode::I64, "v", true))),
    ];
    let mut msgs: Vec<String> = cases
        .iter()
        .map(|(what, got)| check_same_types(got, &want).expect_err(what))
        .collect();

    // Scale alone: the one difference a decoded descriptor cannot see.
    let decimal = |scale| {
        encode(
            [
                col(TypeCode::U64, "id", false),
                SchemaBlockCol {
                    ty: ColType::decimal(scale),
                    ..col(TypeCode::U64, "d", false)
                },
            ]
            .into_iter(),
            &[0],
        )
    };
    assert_eq!(check_same_types(&decimal(4), &decimal(4)), Ok(()));
    msgs.push(check_same_types(&decimal(2), &decimal(4)).expect_err("scale"));

    for i in 0..msgs.len() {
        for j in (i + 1)..msgs.len() {
            assert_ne!(msgs[i], msgs[j], "cases {i} and {j} report the same message");
        }
    }
}

/// A `got` that is not byte-equal to `want` is validated: a truncated one is a
/// decode error, not a match.
#[test]
fn check_same_types_refuses_a_malformed_record() {
    let want = simple();
    let truncated = &want[..want.len() - 1];
    let err = check_same_types(truncated, &want).expect_err("a truncated record");
    assert!(err.starts_with("schema record: "), "{err}");
}
