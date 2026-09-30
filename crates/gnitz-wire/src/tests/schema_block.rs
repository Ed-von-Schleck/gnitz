use super::*;
use crate::{ColType, TypeCode};

fn col(tc: TypeCode, name: &str, nullable: bool) -> SchemaBlockCol<'_> {
    SchemaBlockCol {
        ty: ColType::of(tc),
        nullable,
        hidden: false,
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

/// Both column flags and the column type's scale survive the record, and so
/// does a PK at the record's arity cap, `MAX_PK_COLUMNS`, which the engine's
/// intermediate circuit schemas reach — a consumer with a narrower key limit
/// applies it itself.
#[test]
fn column_meta_and_the_widest_pk_survive_the_record() {
    let mut cols = Vec::new();
    for nullable in [false, true] {
        for hidden in [false, true] {
            // Scale 0 is every non-DECIMAL column, and `MAX_DECIMAL_SCALE` the
            // widest a DECIMAL admits — so neither edge may bleed into a
            // neighbour.
            for scale in [0u8, 7, crate::decimal::MAX_DECIMAL_SCALE] {
                cols.push(SchemaBlockCol {
                    ty: ColType::decimal(scale),
                    nullable,
                    hidden,
                    name: b"c",
                });
            }
        }
    }
    // Column 0 is the key, so it must not be nullable: swap it for a plain one.
    cols[0] = col(TypeCode::U64, "k", false);
    let wide_pk: Vec<u32> = (0..MAX_PK_COLUMNS as u32).collect();
    for (cols, pk) in [(cols, vec![0]), (all_key_cols(MAX_PK_COLUMNS), wide_pk)] {
        let record = encode(cols.iter().copied(), &pk);
        let (got, got_pk) = decode_cols(&record).unwrap();
        assert_eq!((got, got_pk.as_slice()), (cols, pk.as_slice()));
    }
}

/// Every guard, against the forgery that trips it. The message is what separates
/// them: a merged table asserting only "an error" would pass on the wrong guard.
#[test]
fn each_guard_rejects_its_own_forgery() {
    // The column section opens right after `column_count`, `pk_count` and the
    // one PK index byte; a column is type, flags, scale, then its name prefix.
    const COL0: usize = 4 + 1 + 1;
    let forge = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut r = simple();
        f(&mut r);
        r
    };
    let wide_pk: Vec<u32> = (0..=MAX_PK_COLUMNS as u32).collect();
    let cases = [
        (
            forge(&|r| crate::write_u32_le(r, 0, 0)),
            format!("column count 0 out of range 1..={MAX_COLUMNS}"),
        ),
        (
            forge(&|r| crate::write_u32_le(r, 0, MAX_COLUMNS as u32 + 1)),
            format!("column count {} out of range", MAX_COLUMNS + 1),
        ),
        // A lowered count leaves the second column's bytes unclaimed.
        (forge(&|r| crate::write_u32_le(r, 0, 1)), "trailing bytes".into()),
        (forge(&|r| r.push(0)), "trailing bytes".into()),
        (
            encode(all_key_cols(MAX_PK_COLUMNS + 1).into_iter(), &wide_pk),
            format!("pk column count {} exceeds {MAX_PK_COLUMNS}", MAX_PK_COLUMNS + 1),
        ),
        (forge(&|r| r[COL0] = 0), "invalid column type 0/0".into()),
        (forge(&|r| r[COL0 + 1] = 1 << 3), "unknown flag bits 0x08".into()),
        // A scale on a type that carries none.
        (forge(&|r| r[COL0 + 2] = 3), "invalid column type 8/3".into()),
        // A hostile name length cannot force an over-large read.
        (
            forge(&|r| crate::write_u32_le(r, COL0 + 3, u32::MAX)),
            "truncated".into(),
        ),
    ];
    for (record, want) in cases {
        let err = decode_err(&record);
        assert!(
            err.starts_with("schema record: ") && err.contains(&want),
            "{err:?} lacks {want:?}"
        );
    }
    let record = simple();
    for cut in 0..record.len() {
        assert!(decode_cols(&record[..cut]).is_err(), "cut at {cut}");
    }
}

/// The exact bytes of a compound-PK schema whose last column carries every flag
/// and a long name — the one pin of the format, which `gnitz-mirror` relies on
/// as a view's schema identity.
#[test]
fn the_record_layout_is_fixed() {
    let long = "a_rather_long_column_name";
    let cols = [
        col(TypeCode::U64, "b", false),
        col(TypeCode::I32, "a", false),
        SchemaBlockCol {
            ty: ColType::decimal(9),
            nullable: true,
            hidden: true,
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
    // PK order is the declared `(a, b)`, not column order `(b, a)`.
    let (got, pk) = decode_cols(&want).unwrap();
    assert_eq!((got, pk.as_slice()), (cols.to_vec(), &[1u32, 0][..]));
}

/// Each difference `check_same_types` compares is refused as a mismatch, and
/// each names itself distinctly — pairwise distinctness tests the message
/// without pinning prose.
/// What it does not compare (names, the hidden flag) is admitted.
#[test]
fn check_same_types_names_each_mismatch_distinctly() {
    let two_cols =
        |second: SchemaBlockCol<'static>| encode([col(TypeCode::U64, "id", false), second].into_iter(), &[0]);
    let want = two_cols(col(TypeCode::I64, "v", false));
    assert_eq!(check_same_types(&want, &want), Ok(()), "a record matches itself");

    let renamed_and_hidden = two_cols(SchemaBlockCol {
        hidden: true,
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
        assert!(msgs[i].starts_with("Schema mismatch: "), "case {i}: {}", msgs[i]);
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
