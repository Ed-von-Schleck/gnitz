use super::*;

/// A COL_TAB key packs `(owner_id, col_idx)` into one word, so both halves
/// must be bounded: an overflowing index or owner would alias another
/// column's record rather than fail.
#[test]
fn col_id_packing_is_bounded_on_both_halves() {
    assert!(pack_col_id(1, (1 << COL_ID_IDX_BITS) - 1).is_ok());
    assert!(pack_col_id(1, 1 << COL_ID_IDX_BITS).is_err());
    let max_owner = u64::MAX >> COL_ID_IDX_BITS;
    assert!(pack_col_id(max_owner, 0).is_ok());
    assert!(pack_col_id(max_owner + 1, 0).is_err());
    assert!(pack_col_id(u64::MAX, 0).is_err());
    assert_eq!(unpack_col_id(pack_col_id(12345, 7).unwrap()), (12345, 7));
}

/// Every system table's key must be admissible for its own column list. The
/// pair is the thing both crates build from, so it is validated here rather
/// than trusted at each derivation site. Swept over `SYS_FAMILIES` (plus the
/// meta-schema block, which is not a family), so a newly added family is
/// covered without editing this test.
#[test]
fn system_table_keys_are_valid_for_their_columns() {
    let meta = ("meta_schema", META_SCHEMA_COLS, LEADING_COL_PK);
    let families = SYS_FAMILIES
        .iter()
        .map(|f| (f.name, f.cols, f.pk_cols))
        .chain(std::iter::once(meta));
    for (name, cols, pk) in families {
        assert!(cols.len() <= MAX_COLUMNS, "{name}: too many columns");
        crate::validate_pk_tuple(pk, cols.len(), |c| {
            let col = &cols[c as usize];
            (col.type_code as u8, col.nullable)
        })
        .unwrap_or_else(|rule| panic!("{name}: invalid primary key: {rule}"));
    }
}

/// The in-memory constructor and the persisted codec are the same list at every
/// arity: `from_slice` and `pack`→`unpack` must agree, and both must read back
/// the columns they were given.
#[test]
fn from_slice_and_the_packed_codec_agree_at_every_arity() {
    for cols in [
        vec![0u32],
        vec![3u32],
        vec![1u32, PK_LIST_COL_MAX],
        vec![2u32, 5, 7],
        vec![9u32, 1, 4, 6],
    ] {
        let list = PkColList::from_slice(&cols);
        assert_eq!(list.as_slice(), cols.as_slice());
        assert_eq!(list.decoded_count(), cols.len());
        assert!(list.is_well_formed());
        assert_eq!(unpack_pk_cols(pack_pk_cols(&cols)), list, "{cols:?}");
    }
}

/// A crafted packed word can name a count of zero or one past the cap.
/// `decoded_count()` must return it **raw** — that is the value `is_well_formed`
/// gates on — while `as_slice()` clamps rather than panics, so the malformed
/// list survives as far as the schema validation that returns it as an `Err`.
#[test]
fn a_crafted_pk_col_count_survives_to_is_well_formed() {
    for n in [0usize, PK_LIST_MAX_COLS + 1, (1 << PK_LIST_COUNT_BITS) - 1] {
        let list = unpack_pk_cols(PK_LIST_PACKED_FLAG | n as u64);
        assert_eq!(list.decoded_count(), n, "the raw count must reach is_well_formed");
        assert!(!list.is_well_formed(), "count {n} is out of range");
        assert!(list.as_slice().len() <= PK_LIST_MAX_COLS, "as_slice must clamp");
    }
}

/// `HAS_PK_WANT_HOLDER` rides bit 62 of the same `seek_col_idx` word that
/// carries the packed column list, and `pk_cols_word` is what strips it. Both
/// directions are used: the worker reads the directive off a word carrying a
/// maximal list, and the dispatch arms recover that list unchanged from a word
/// carrying the directive.
#[test]
fn the_want_holder_bit_is_clear_of_every_packed_column_list() {
    let maximal: Vec<u32> = (0..PK_LIST_MAX_COLS as u32).map(|i| PK_LIST_COL_MAX - i).collect();
    for cols in [vec![0u32], vec![PK_LIST_COL_MAX], vec![1u32, 2, 3, 4], maximal] {
        let packed = pack_pk_cols(&cols);
        assert_eq!(packed & HAS_PK_WANT_HOLDER, 0, "{cols:?} must leave bit 62 clear");
        let with_directive = packed | HAS_PK_WANT_HOLDER;
        assert_eq!(pk_cols_word(with_directive), packed, "{cols:?}");
        assert_eq!(unpack_pk_cols(pk_cols_word(with_directive)).as_slice(), cols.as_slice());
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

#[test]
fn table_flags_roundtrip() {
    // Default (not replicated, not a stream, k = 0 = full PK) is all-clear.
    assert_eq!(TableProps::default().pack(), 0);
    // Every field combination survives, and k rides in byte 1 clear of the bits.
    for &replicated in &[false, true] {
        for &stream in &[false, true] {
            for dist_prefix_len in 0..=PK_LIST_MAX_COLS {
                let p = TableProps {
                    replicated,
                    stream,
                    dist_prefix_len,
                };
                assert_eq!(TableProps::from_flags(p.pack()), p);
            }
        }
    }
    // `TABLE_TAB.flags` is persisted, so the bit *positions* are a wire
    // contract: pinned as literals, since comparing against the constants the
    // packer is written from would hold for any value it gave them.
    let props = |replicated, stream, dist_prefix_len| TableProps {
        replicated,
        stream,
        dist_prefix_len,
    };
    assert_eq!(props(true, false, 0).pack(), 0b01);
    assert_eq!(props(false, true, 0).pack(), 0b10);
    assert_eq!(props(false, false, 2).pack(), 2 << 8);
    assert_eq!(props(true, true, 2).pack(), 0b11 | (2 << 8));

    // Reserved bits are ignored on decode, so a word a later version widened
    // still yields the fields defined today.
    let reserved = 0b1111_1100u64 | (0xFFu64 << 16);
    assert_eq!(
        TableProps::from_flags(props(true, true, 2).pack() | reserved),
        props(true, true, 2),
    );
}

/// The two halves of the `seek_by_index` key wire format are inverses at
/// every arity, including a prefix seek that supplies fewer values than the
/// index has columns.
#[test]
fn index_key_slots_roundtrip() {
    let all: [u128; PK_LIST_MAX_COLS] = [1, u128::MAX, 1 << 100, 0];
    for k in 1..=PK_LIST_MAX_COLS {
        let vals = &all[..k];
        let (buf, len) = pack_index_key_slots(vals);
        assert_eq!(len, k * INDEX_KEY_SLOT);
        // `split_wire` routes slot 0 to seek_pk and the rest to the tail.
        let seek_pk = u128::from_le_bytes(buf[..INDEX_KEY_SLOT].try_into().unwrap());
        let extra = &buf[INDEX_KEY_SLOT..len];
        let back = unpack_index_key_slots(seek_pk, extra, PK_LIST_MAX_COLS).expect("well-formed");
        assert_eq!(back.as_slice(), vals, "arity {k}");
        // A prefix seek is accepted; more values than the arity is not.
        assert!(unpack_index_key_slots(seek_pk, extra, k).is_ok());
        if k > 1 {
            assert!(unpack_index_key_slots(seek_pk, extra, k - 1).is_err(), "over-arity");
        }
    }
}

/// A tail that is not a whole number of slots is rejected rather than
/// silently dropping its trailing bytes.
#[test]
fn index_key_slots_reject_a_misaligned_tail() {
    assert!(unpack_index_key_slots(7, &[0u8; 15], PK_LIST_MAX_COLS).is_err());
    assert!(unpack_index_key_slots(7, &[0u8; 17], PK_LIST_MAX_COLS).is_err());
}
