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
            Err(crate::PkRule::TooManyColumns { count: n, max: PK_LIST_MAX_COLS })
        );
    }
}

/// A flag-clear word names no column list, whatever its other bits say. `0` is
/// the one an `arg1` carries when it means the relation's own PK store,
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
    let keyed = |stream, prefix_len| TableProps {
        stream,
        serial: false,
        distribution: TableDistribution::Keyed { prefix_len },
    };
    let replicated = |stream| TableProps {
        stream,
        serial: false,
        distribution: TableDistribution::Replicated,
    };
    // Every field combination survives, and k rides in byte 1 clear of the bits.
    for &stream in &[false, true] {
        for serial in [false, true] {
            let r = TableProps { serial, ..replicated(stream) };
            assert_eq!(TableProps::from_flags(r.pack()).unwrap(), r);
            for prefix_len in 0..=PK_LIST_MAX_COLS as u8 {
                let p = TableProps { serial, ..keyed(stream, prefix_len) };
                assert_eq!(TableProps::from_flags(p.pack()).unwrap(), p);
            }
        }
    }
    // `TABLE_TAB.flags` is persisted, so the bit *positions* are a wire
    // contract: pinned as literals, since comparing against the constants the
    // packer is written from would hold for any value it gave them.
    assert_eq!(replicated(false).pack(), 0b01);
    assert_eq!(keyed(true, 0).pack(), 0b10);
    assert_eq!(keyed(false, 2).pack(), 2 << 8);
    assert_eq!(replicated(true).pack(), 0b11);
    assert_eq!(keyed(true, 2).pack(), 0b10 | (2 << 8));
    assert_eq!(TableProps { serial: true, ..keyed(false, 0) }.pack(), 0b100);
}

/// A SERIAL table is a table keyed by its one generated column: `validate`
/// refuses the flag on a stream and over a compound PK.
#[test]
fn table_props_validate_refuses_a_serial_stream_and_a_compound_serial_pk() {
    let serial = TableProps { serial: true, ..TableProps::default() };
    assert_eq!(serial.validate(1), Ok(()));
    assert!(serial.validate(2).unwrap_err().contains("one SERIAL column"));
    let stream = TableProps { stream: true, ..serial };
    assert!(stream.validate(1).unwrap_err().contains("a stream cannot be SERIAL"));
}

/// `VIEW_TAB.flags` round-trips, pins its persisted bit position, and refuses
/// every other bit.
#[test]
fn view_flags_round_trip_and_refuse_reserved_bits() {
    for pk_repeats in [false, true] {
        let f = ViewFlags { pk_repeats };
        assert_eq!(ViewFlags::from_flags(f.pack()).unwrap(), f);
    }
    assert_eq!(ViewFlags { pk_repeats: true }.pack(), 0b1);
    for reserved in [1u64 << 1, 1 << 8, 1 << 63] {
        assert!(ViewFlags::from_flags(reserved).is_err(), "bit {reserved:#x}");
    }
}

/// A bit outside the defined set is refused, not ignored.
#[test]
fn table_flags_refuse_reserved_bits() {
    for reserved in [1u64 << 3, 1 << 7, 1 << 16, 1 << 63] {
        assert!(TableProps::from_flags(reserved).is_err(), "bit {reserved:#x}");
    }
}

/// A replicated table carrying a distribution prefix is the one state the
/// packing can hold and `TableProps` cannot.
#[test]
fn table_flags_refuse_replicated_with_a_prefix() {
    let err = TableProps::from_flags(0b01 | (2 << 8)).unwrap_err();
    assert!(err.contains("mutually exclusive"), "{err}");
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
