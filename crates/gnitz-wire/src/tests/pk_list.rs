use super::*;

/// The in-memory constructor and the persisted codec are the same list at every
/// arity. The word is persisted, so its layout is pinned as a literal.
#[test]
fn a_pk_col_list_roundtrips_through_its_packed_word() {
    for cols in [
        &[0u32][..],
        &[3],
        &[1, MAX_COLUMNS as u32 - 1],
        &[2, 5, 7],
        &[9, 1, 4, 6],
    ] {
        let list = PkColList::from_slice(cols);
        assert_eq!(list.as_slice(), cols);
        assert_eq!(PkColList::unpack(list.pack()), Ok(list), "{cols:?}");
    }
    assert_eq!(PkColList::from_slice(&[3, 9]).pack(), 9 << 11 | 3 << 4 | 2);
}

/// A crafted count of zero or past the cap, a repeated column, a column no
/// schema has and a bit outside the list's fields are refused.
#[test]
fn unpack_refuses_a_crafted_list() {
    use PkRule::*;
    let over = PK_LIST_MAX_COLS + 1;
    for (w, want) in [
        (0, Empty),
        (over as u64, TooManyColumns { count: over, max: PK_LIST_MAX_COLS }),
        (15, TooManyColumns { count: 15, max: PK_LIST_MAX_COLS }),
        (2 | 1 << 4 | 1 << 11, Duplicate { col: 1 }),
        (
            1 | (MAX_COLUMNS as u64) << 4,
            IndexOutOfRange { col: MAX_COLUMNS as u32 },
        ),
        (1 << 63 | 1 | 3 << 4, NonCanonical),
        (1 | 3 << 4 | 1 << 11, NonCanonical),
        (1 | 3 << 4 | 1 << 32, NonCanonical),
    ] {
        assert_eq!(PkColList::unpack(w), Err(want), "{w:#x}");
    }
}

#[test]
#[should_panic(expected = "at least one column")]
fn from_slice_panics_on_empty() {
    let _ = PkColList::from_slice(&[]);
}

/// The PK admission rule, one refusal per rule.
#[test]
fn validate_pk_tuple_names_each_rule() {
    use TypeCode::*;
    // (type, nullable) per column.
    let cols = [(U64, false), (I32, false), (String, false), (F64, false), (I64, true)];
    let check = |pk: &[u32], max| validate_pk_tuple(pk, cols.len(), max, |c| cols[c as usize]);
    assert_eq!(check(&[0, 1], 4), Ok(()));
    for (pk, max, want) in [
        (&[][..], 4, PkRule::Empty),
        (&[0, 1], 1, PkRule::TooManyColumns { count: 2, max: 1 }),
        (&[5], 4, PkRule::IndexOutOfRange { col: 5 }),
        (&[1, 1], 4, PkRule::Duplicate { col: 1 }),
        (&[2], 4, PkRule::NotEligible { col: 2, type_code: String }),
        (&[3], 4, PkRule::NotEligible { col: 3, type_code: F64 }),
        (&[4], 4, PkRule::Nullable { col: 4 }),
    ] {
        assert_eq!(check(pk, max), Err(want), "{pk:?}");
    }
    // An index key waives nullability; its arity shares the limit with the source PK.
    let index = |key: &[u32], src_pk_len| validate_index_key(key, cols.len(), src_pk_len, |c| cols[c as usize].0);
    assert_eq!(index(&[4], MAX_PK_COLUMNS - 1), Ok(PkColList::from_slice(&[4])));
    assert_eq!(
        index(&[0, 4], MAX_PK_COLUMNS - 1),
        Err(PkRule::TooManyColumns { count: 2, max: 1 })
    );
}

/// A rule names a column by the name its caller holds for it, and by its index
/// where the caller holds none.
#[test]
fn a_pk_rule_names_the_columns_its_caller_can() {
    let names = ["id", "price"];
    let name = |c: u32| names.get(c as usize).copied();
    let role = PkListRole::PrimaryKey;
    for (rule, named, positional) in [
        (
            PkRule::Duplicate { col: 1 },
            "primary key names column 'price' twice",
            "primary key names column 1 twice",
        ),
        (
            PkRule::Nullable { col: 0 },
            "primary key column 'id' must not be nullable",
            "primary key column 0 must not be nullable",
        ),
        (
            PkRule::Nullable { col: 7 },
            "primary key column 7 must not be nullable",
            "primary key column 7 must not be nullable",
        ),
        (
            PkRule::IndexOutOfRange { col: 1 },
            "primary key index 1 out of bounds",
            "primary key index 1 out of bounds",
        ),
    ] {
        assert_eq!(rule.named(role, name), named, "{rule:?}");
        assert_eq!(rule.for_role(role), positional, "{rule:?}");
    }
    let float = PkRule::NotEligible { col: 1, type_code: TypeCode::F64 };
    assert!(
        float
            .named(role, name)
            .starts_with("primary key column 'price' has type_code F64;")
    );
    assert!(
        float
            .for_role(role)
            .starts_with("primary key column 1 has type_code F64;")
    );
}
