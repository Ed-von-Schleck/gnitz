use super::*;

/// Every system table's key must be admissible for its own column list. The
/// pair is the thing both crates build from, so it is validated here rather
/// than trusted at each derivation site. Swept over `SYS_FAMILIES`, so a newly
/// added family is covered without editing this test.
#[test]
fn system_table_keys_are_valid_for_their_columns() {
    for f in SYS_FAMILIES {
        let (name, cols, pk) = (f.name, f.cols, f.pk_cols);
        assert!(cols.len() <= MAX_COLUMNS, "{name}: too many columns");
        crate::validate_pk_tuple(pk, cols.len(), PK_LIST_MAX_COLS, |c| {
            let col = &cols[c as usize];
            (col.type_code, col.nullable)
        })
        .unwrap_or_else(|rule| panic!("{name}: invalid primary key: {rule}"));
    }
}

/// The in-memory constructor and the persisted codec are the same list at every
/// arity, and no packed list is the PK-store sentinel. The word is persisted, so
/// its layout is pinned as a literal.
#[test]
fn a_pk_col_list_roundtrips_through_its_packed_word() {
    for cols in [&[0u32][..], &[3], &[1, PK_LIST_COL_MAX], &[2, 5, 7], &[9, 1, 4, 6]] {
        let list = PkColList::from_slice(cols);
        assert_eq!(list.as_slice(), cols);
        let packed = pack_pk_cols(cols);
        assert_eq!(unpack_pk_cols(packed), Ok(list), "{cols:?}");
        assert_eq!(
            probe_key_columns(packed),
            Some(packed),
            "{cols:?}: never the PK sentinel"
        );
    }
    assert_eq!(probe_key_columns(PROBE_KEYSPACE_PK), None);
    assert_eq!(pack_pk_cols(&[3, 9]), 1 << 63 | 9 << 11 | 3 << 4 | 2);
}

/// A flag-clear word names no column list, whatever its other bits say — `0` is
/// the one an `arg1` carries when it means the relation's own PK store — and a
/// crafted count of zero or past the cap is refused, so no `PkColList` with such
/// a count is ever built.
#[test]
fn unpack_refuses_an_untagged_word_and_a_crafted_count() {
    use crate::PkRule::*;
    let over = PK_LIST_MAX_COLS + 1;
    for (w, want) in [
        (0, NotPacked),
        (7, NotPacked),
        (PK_LIST_PACKED_FLAG, Empty),
        (
            PK_LIST_PACKED_FLAG | over as u64,
            TooManyColumns { count: over, max: PK_LIST_MAX_COLS },
        ),
        (
            PK_LIST_PACKED_FLAG | 15,
            TooManyColumns { count: 15, max: PK_LIST_MAX_COLS },
        ),
    ] {
        assert_eq!(unpack_pk_cols(w), Err(want), "{w:#x}");
    }
}

#[test]
#[should_panic(expected = "out of range")]
fn from_slice_panics_on_empty() {
    let _ = PkColList::from_slice(&[]);
}

#[test]
fn validate_dist_prefix_accepts_leading_rejects_rest() {
    // Exact leading prefixes of PK (0, 1) are accepted, returning k.
    assert_eq!(validate_dist_prefix(&[0, 1], &[0]), Ok(1));
    assert_eq!(validate_dist_prefix(&[0, 1], &[0, 1]), Ok(2));
    // A single-column PK: only the whole PK is a valid prefix.
    assert_eq!(validate_dist_prefix(&[3], &[3]), Ok(1));
    // Reordered PK so the distribution column leads: prefix is the new lead.
    assert_eq!(validate_dist_prefix(&[2, 1], &[2]), Ok(1));

    // Non-leading PK column, non-contiguous-prefix, wrong order, empty, and
    // over-long lists are all rejected.
    assert!(validate_dist_prefix(&[0, 1], &[1]).is_err(), "non-leading PK column");
    assert!(validate_dist_prefix(&[0, 1, 2], &[0, 2]).is_err(), "skips col 1");
    assert!(validate_dist_prefix(&[0, 1], &[1, 0]).is_err(), "wrong order");
    assert!(validate_dist_prefix(&[0, 1], &[]).is_err(), "empty");
    assert!(validate_dist_prefix(&[0, 1], &[0, 1, 2]).is_err(), "longer than PK");
    assert!(validate_dist_prefix(&[0, 1], &[5]).is_err(), "non-PK column");
}

/// Each catalog flags word round-trips, pins its persisted bits as literals —
/// comparing against the constants the packer is written from would hold for any
/// value it gave them — and accepts exactly the single bits its layout defines.
#[test]
fn catalog_flag_words_roundtrip_pin_and_refuse_undefined_bits() {
    let dists = (0..=PK_LIST_MAX_COLS as u8)
        .map(|prefix_len| TableDistribution::Keyed { prefix_len })
        .chain([TableDistribution::Replicated]);
    for distribution in dists {
        for stream in [false, true] {
            for serial in [false, true] {
                let p = TableProps { stream, serial, distribution };
                assert_eq!(TableProps::from_flags(p.pack()), Ok(p));
            }
        }
    }
    for b in [false, true] {
        assert_eq!(
            ViewFlags::from_flags(ViewFlags { pk_repeats: b }.pack()),
            Ok(ViewFlags { pk_repeats: b })
        );
        assert_eq!(
            IndexProps::from_flags(IndexProps { is_unique: b }.pack()),
            Ok(IndexProps { is_unique: b })
        );
    }

    let t = |stream, serial, distribution| TableProps { stream, serial, distribution }.pack();
    assert_eq!(TableProps::default().pack(), 0);
    assert_eq!(t(false, false, TableDistribution::Replicated), 0b001);
    assert_eq!(t(true, false, TableDistribution::default()), 0b010);
    assert_eq!(t(false, true, TableDistribution::default()), 0b100);
    assert_eq!(t(false, false, TableDistribution::Keyed { prefix_len: 2 }), 2 << 8);
    assert_eq!(
        (
            ViewFlags { pk_repeats: true }.pack(),
            IndexProps { is_unique: true }.pack()
        ),
        (1, 1)
    );

    for bit in 0..64u32 {
        let w = 1u64 << bit;
        assert_eq!(
            TableProps::from_flags(w).is_ok(),
            bit <= 2 || (8..16).contains(&bit),
            "table bit {bit}"
        );
        assert_eq!(ViewFlags::from_flags(w).is_ok(), bit == 0, "view bit {bit}");
        assert_eq!(IndexProps::from_flags(w).is_ok(), bit == 0, "index bit {bit}");
    }
    // The one state the packing can hold and `TableProps` cannot.
    assert!(TableProps::from_flags(0b01 | 2 << 8)
        .unwrap_err()
        .contains("mutually exclusive"));
}

/// `validate` refuses what the flags word can hold but the table cannot: a
/// distribution prefix longer than the PK, and a SERIAL table that is a stream
/// or whose PK is not its one generated column.
#[test]
fn table_props_validate_refuses_what_the_pk_cannot_carry() {
    let keyed = |prefix_len| TableProps {
        distribution: TableDistribution::Keyed { prefix_len },
        ..TableProps::default()
    };
    assert_eq!(keyed(3).validate(3), Ok(()));
    assert!(keyed(4).validate(3).unwrap_err().contains("exceeds PK column count"));
    let serial = TableProps { serial: true, ..TableProps::default() };
    assert_eq!(serial.validate(1), Ok(()));
    assert!(serial.validate(2).unwrap_err().contains("one SERIAL column"));
    let stream = TableProps { stream: true, ..serial };
    assert!(stream.validate(1).unwrap_err().contains("a stream cannot be SERIAL"));
}

/// The system-shape digest moves with every axis of a family's stored identity.
#[test]
fn fold_family_separates_every_stored_axis() {
    const A: &[WireSysCol] = &[col("id", TypeCode::U64, false), col("v", TypeCode::U64, false)];
    const RENAMED: &[WireSysCol] = &[col("id", TypeCode::U64, false), col("w", TypeCode::U64, false)];
    const RETYPED: &[WireSysCol] = &[col("id", TypeCode::U64, false), col("v", TypeCode::I64, false)];
    const NULLABLE: &[WireSysCol] = &[col("id", TypeCode::U64, false), col("v", TypeCode::U64, true)];
    let base = fold_family(0, &fam(1, "_t", A, LEADING_COL_PK));
    let others = [
        fam(2, "_t", A, LEADING_COL_PK),
        fam(1, "_u", A, LEADING_COL_PK),
        fam(1, "_t", RENAMED, LEADING_COL_PK),
        fam(1, "_t", RETYPED, LEADING_COL_PK),
        fam(1, "_t", NULLABLE, LEADING_COL_PK),
        fam(1, "_t", A, LEADING_PAIR_PK),
    ];
    for (i, other) in others.iter().enumerate() {
        assert_ne!(fold_family(0, other), base, "variant {i}");
    }
}

/// Every PK list of up to 4 distinct indices, in every order, over 1..=6
/// columns, against the naive "`pi`-th non-PK column".
#[test]
fn payload_col_idx_and_payload_slot_number_the_non_pk_columns() {
    fn lists(n: u32, len: usize, cur: &mut Vec<u32>, out: &mut Vec<Vec<u32>>) {
        out.push(cur.clone());
        if cur.len() == len {
            return;
        }
        for c in 0..n {
            if !cur.contains(&c) {
                cur.push(c);
                lists(n, len, cur, out);
                cur.pop();
            }
        }
    }
    for n in 1..=6u32 {
        let mut all = Vec::new();
        lists(n, 4.min(n as usize), &mut Vec::new(), &mut all);
        for pk in &all {
            let payload: Vec<usize> = (0..n).filter(|c| !pk.contains(c)).map(|c| c as usize).collect();
            for (pi, &ci) in payload.iter().enumerate() {
                assert_eq!(payload_col_idx(pk, pi), ci, "n {n}, pk {pk:?}, pi {pi}");
            }
            for ci in 0..n as usize {
                let want = payload.iter().position(|&c| c == ci);
                assert_eq!(payload_slot(pk, ci), want, "n {n}, pk {pk:?}, ci {ci}");
            }
        }
    }
}

#[test]
fn a_user_identifier_is_nonempty_charset_and_not_underscore_led() {
    for name in ["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
        assert!(validate_user_identifier(name).is_ok(), "rejected valid: {name}");
    }
    for name in [
        "_private",
        "_",
        "__init__",
        "",
        "has space",
        "has-dash",
        "has.dot",
        "has@",
        "table$",
    ] {
        assert!(validate_user_identifier(name).is_err(), "accepted invalid: {name}");
    }
    assert_eq!(canonical_identifier("MyTab_1"), Ok("mytab_1".into()));
    assert!(canonical_identifier("_x").is_err());
}
