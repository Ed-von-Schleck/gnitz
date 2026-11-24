use super::*;
use crate::TypeCode;

fn col(tc: TypeCode, name: &str, nullable: bool) -> RecordCol<'_> {
    RecordCol {
        ty: ColType::of(tc),
        nullable,
        hidden: false,
        name,
    }
}

/// Every column `record` holds, and its PK indices.
fn decode_cols(record: &[u8]) -> Result<(Vec<RecordCol<'_>>, PkIndices), String> {
    let mut cols = Vec::new();
    let pk = decode(record, |c| cols.push(c))?;
    Ok((cols, pk))
}

/// `n` U64 key columns, all of them in the PK, in column order.
fn all_key_cols(n: usize) -> Vec<RecordCol<'static>> {
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
            for scale in [0u8, 7, crate::MAX_DECIMAL_SCALE] {
                cols.push(RecordCol {
                    ty: ColType::decimal(scale),
                    nullable,
                    hidden,
                    name: "c",
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
    // The column count follows `pk_count` and the one PK index byte, and the
    // column section follows it; a column is type, flags, scale, then its name
    // prefix.
    const COUNT: usize = 2 + 1;
    const COL0: usize = COUNT + 2;
    let set_count = |r: &mut Vec<u8>, n: u16| r[COUNT..COL0].copy_from_slice(&n.to_le_bytes());
    let forge = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut r = simple();
        f(&mut r);
        r
    };
    let wide_pk: Vec<u32> = (0..=MAX_PK_COLUMNS as u32).collect();
    let cases = [
        (forge(&|r| set_count(r, 0)), "at least one column".into()),
        (
            forge(&|r| set_count(r, MAX_COLUMNS as u16 + 1)),
            format!("columns: {} entries exceeds cap {MAX_COLUMNS}", MAX_COLUMNS + 1),
        ),
        // A lowered count leaves the second column's bytes unclaimed.
        (forge(&|r| set_count(r, 1)), "trailing bytes".into()),
        (forge(&|r| r.push(0)), "trailing bytes".into()),
        (
            encode(all_key_cols(MAX_PK_COLUMNS + 1).into_iter(), &wide_pk),
            format!(
                "pk columns: {} entries exceeds cap {MAX_PK_COLUMNS}",
                MAX_PK_COLUMNS + 1
            ),
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
        // The first byte of the first column's name, "id".
        (forge(&|r| r[COL0 + 7] = 0xFF), "text is not UTF-8".into()),
    ];
    for (record, want) in cases {
        let err = decode_cols(&record).expect_err("expected a rejection");
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
/// and a long name. `gnitz-mirror` relies on the format as a view's schema
/// identity.
#[test]
fn the_record_layout_is_fixed() {
    let long = "a_rather_long_column_name";
    let cols = [
        col(TypeCode::U64, "b", false),
        col(TypeCode::I32, "a", false),
        RecordCol {
            ty: ColType::decimal(9),
            nullable: true,
            hidden: true,
            name: long,
        },
    ];
    #[rustfmt::skip]
    let mut want: Vec<u8> = vec![
        2, 0,                        // pk_count
        1, 0,                        // PK indices, in declared PK-tuple order
        3, 0,                        // column_count
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

/// Each difference from `want` is refused as a mismatch, and each names itself
/// distinctly — pairwise distinctness tests the message without pinning prose.
/// A `got` that is not byte-equal to `want` is validated: a truncated one is a
/// decode error.
#[test]
fn check_shape_names_each_mismatch_distinctly() {
    let two_cols = |second: RecordCol<'static>| encode([col(TypeCode::U64, "id", false), second].into_iter(), &[0]);
    let want = two_cols(col(TypeCode::I64, "v", false));
    assert_eq!(check_shape(&want, &want), Ok(()), "a record matches itself");

    let renamed_and_hidden = two_cols(RecordCol {
        hidden: true,
        ..col(TypeCode::I64, "renamed", false)
    });
    let named = check_shape(&renamed_and_hidden, &want).expect_err("a name or hidden flag");

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
        .map(|(what, got)| check_shape(got, &want).expect_err(what))
        .collect();
    msgs.push(named);

    // Scale alone: the one difference a decoded descriptor cannot see.
    let decimal = |scale| {
        encode(
            [
                col(TypeCode::U64, "id", false),
                RecordCol {
                    ty: ColType::decimal(scale),
                    ..col(TypeCode::U64, "d", false)
                },
            ]
            .into_iter(),
            &[0],
        )
    };
    msgs.push(check_shape(&decimal(2), &decimal(4)).expect_err("scale"));

    for i in 0..msgs.len() {
        assert!(msgs[i].starts_with("Schema mismatch: "), "case {i}: {}", msgs[i]);
        for j in (i + 1)..msgs.len() {
            assert_ne!(msgs[i], msgs[j], "cases {i} and {j} report the same message");
        }
    }

    let err = check_shape(&want[..want.len() - 1], &want).expect_err("a truncated record");
    assert!(err.starts_with("schema record: "), "{err}");
}

/// Two clients' columns that differ only in their names and hidden flags encode
/// one shape record, so a push from either matches the engine's byte for byte.
#[test]
fn differently_named_columns_encode_one_shape_record() {
    let shape = |cols: &[crate::ColumnDef]| encode(cols.iter().map(crate::ColumnDef::shape_col), &[0]);
    let engine = shape(&[
        crate::ColumnDef::new("id", TypeCode::U64, false).hidden(),
        crate::ColumnDef::new("price", ColType::decimal(4), true),
    ]);
    let client = shape(&[
        crate::ColumnDef::new("key", TypeCode::U64, false),
        crate::ColumnDef::new("p", ColType::decimal(4), true),
    ]);
    assert_eq!(client, engine);
    assert_eq!(check_shape(&client, &engine), Ok(()));
    let (cols, _) = decode_cols(&engine).unwrap();
    assert!(cols.iter().all(|c| c.name.is_empty() && !c.hidden), "{cols:?}");
    assert_eq!(
        cols[1].ty,
        ColType::decimal(4),
        "the shape record keeps a DECIMAL's scale"
    );
}
