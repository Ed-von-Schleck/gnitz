use super::*;

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
        assert_eq!(unpack_pk_cols(pack_pk_cols(&cols)), Ok(list), "{cols:?}");
    }
}

/// A crafted packed word can name a count of zero or one past the cap. Both are
/// refused at the decode, so no `PkColList` with such a count is ever built.
#[test]
fn a_crafted_pk_col_count_is_rejected_at_unpack() {
    let over = [PK_LIST_MAX_COLS + 1, (1 << PK_LIST_COUNT_BITS) - 1];
    assert_eq!(unpack_pk_cols(PK_LIST_PACKED_FLAG), Err(crate::PkRule::Empty));
    for n in over {
        assert_eq!(
            unpack_pk_cols(PK_LIST_PACKED_FLAG | n as u64),
            Err(crate::PkRule::TooManyColumns { count: n })
        );
    }
}

/// A flag-clear word names no column list, whatever its other bits say. `0` is
/// the one a `seek_col_idx` carries when it means the relation's own PK store,
/// and a non-zero one would otherwise read as "the index on column 7".
#[test]
fn a_flag_clear_word_is_refused() {
    for raw in [0u64, 7] {
        assert_eq!(unpack_pk_cols(raw), Err(crate::PkRule::NotPacked), "{raw}");
    }
}

/// A packed list occupies the low 32 bits plus the flag at bit 63, leaving bits
/// [32..63) clear — room for any later directive in the same word. The count
/// field makes it non-zero, which is what keeps `PROBE_KEYSPACE_PK` a keyspace
/// no column list can name.
#[test]
fn a_packed_list_leaves_the_reserved_bits_clear() {
    assert_eq!(crate::probe_key_columns(crate::PROBE_KEYSPACE_PK), None);
    for cols in [&[0u32][..], &[3][..], &[3, 9, 40, 64][..]] {
        let packed = pack_pk_cols(cols);
        assert_eq!(packed >> 63, 1, "{cols:?}: the packed flag is bit 63");
        assert_eq!((packed >> 32) & 0x7FFF_FFFF, 0, "{cols:?}: bits [32..63) are reserved");
        assert_eq!(
            crate::probe_key_columns(packed),
            Some(packed),
            "{cols:?}: never the PK sentinel"
        );
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
                let p = TableProps { replicated, stream, dist_prefix_len };
                assert_eq!(TableProps::from_flags(p.pack()), p);
            }
        }
    }
    // `TABLE_TAB.flags` is persisted, so the bit *positions* are a wire
    // contract: pinned as literals, since comparing against the constants the
    // packer is written from would hold for any value it gave them.
    let props = |replicated, stream, dist_prefix_len| TableProps { replicated, stream, dist_prefix_len };
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
        // `split_ctrl_key` routes slot 0 to seek_pk and the rest to the tail.
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
