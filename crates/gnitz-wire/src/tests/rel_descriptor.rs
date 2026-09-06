use super::*;

fn roundtrip(d: &RelDescriptorBlob, num_columns: usize) -> RelDescriptorBlob {
    RelDescriptorBlob::decode(&d.encode(), num_columns)
        .expect("decode")
        .expect("present")
}

/// An empty blob is the absent answer, and a present one always encodes to
/// more than that — so the two can never be confused. Both ride inline in
/// the control block's German-string cell, costing a control-only frame.
#[test]
fn absence_is_the_empty_blob() {
    assert_eq!(RelDescriptorBlob::decode(&[], 0), Ok(None));
    let present = RelDescriptorBlob::default().encode();
    assert_eq!(present.len(), HEADER_LEN);
    assert_eq!(crate::control::german_spill_len(present.len()), 0);
}

/// The feed bit round-trips on a plain view and is refused on every other
/// class — a bounded one included, since `capacity` and `delta` are refused
/// together everywhere else. A blob claiming otherwise is rejected rather
/// than believed.
#[test]
fn delta_flag_roundtrips_on_a_view_and_is_refused_elsewhere() {
    let d = RelDescriptorBlob {
        class: RelClass::View,
        delta: true,
        ..Default::default()
    };
    assert_eq!(roundtrip(&d, 2), d);
    for class in [RelClass::Table, RelClass::Stream, RelClass::BoundedView] {
        let mut bytes = RelDescriptorBlob { class, ..Default::default() }.encode();
        bytes[1] |= DESC_FLAG_DELTA;
        assert!(RelDescriptorBlob::decode(&bytes, 2).unwrap_err().contains("delta feed"));
    }
}

/// The encoder's own admission rule must be the decoder's: a `BoundedView`
/// carrying a feed would otherwise encode cleanly and then fail its own decode.
/// `debug_assert`, so this is a debug-build claim — which is where the E2E suite
/// and every unit run live.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "only a plain view can carry a delta feed")]
fn encoding_a_bounded_view_with_a_feed_is_refused() {
    let _ = RelDescriptorBlob {
        class: RelClass::BoundedView,
        delta: true,
        ..Default::default()
    }
    .encode();
}

#[test]
fn empty_lists_roundtrip() {
    let d = RelDescriptorBlob::default();
    assert_eq!(roundtrip(&d, 3), d);
}

#[test]
fn multi_column_index_and_multi_fk_roundtrip() {
    let d = RelDescriptorBlob {
        class: RelClass::BoundedView,
        replicated: true,
        delta: false,
        fks: vec![
            RelFk {
                col_idx: 0,
                fk_col_idx: 0,
                fk_table_id: 16,
            },
            RelFk {
                col_idx: 2,
                fk_col_idx: 1,
                fk_table_id: 99,
            },
        ],
        indexes: vec![
            RelIndex {
                cols: PkColList::from_slice(&[1]),
                is_unique: true,
            },
            RelIndex {
                cols: PkColList::from_slice(&[2, 0, 1]),
                is_unique: false,
            },
        ],
    };
    assert_eq!(roundtrip(&d, 3), d);
}

/// Every class survives a roundtrip alongside `replicated`, which is orthogonal
/// to all of them: reading `replicated` back as false on a replicated stream
/// would make every worker hold a full copy while the client still folded
/// per-worker partials.
#[test]
fn every_class_roundtrips_with_replicated() {
    for class in [RelClass::Table, RelClass::Stream, RelClass::View, RelClass::BoundedView] {
        for &replicated in &[false, true] {
            let d = RelDescriptorBlob { class, replicated, ..Default::default() };
            let back = roundtrip(&d, 2);
            assert_eq!(back, d, "{class:?} replicated={replicated}");
        }
    }
}

/// Every non-empty prefix short of the whole must be an error — only the
/// fully-empty blob means "absent".
#[test]
fn truncated_blob_is_an_error() {
    let d = RelDescriptorBlob {
        indexes: vec![RelIndex {
            cols: PkColList::from_slice(&[0]),
            is_unique: true,
        }],
        ..Default::default()
    };
    let bytes = d.encode();
    for cut in 1..bytes.len() {
        let err = RelDescriptorBlob::decode(&bytes[..cut], 4).unwrap_err();
        assert!(
            err.starts_with("rel descriptor:"),
            "a {cut}-byte prefix must fail as a rel descriptor, got: {err}"
        );
    }
}

/// Every decode guard, against the forgery that trips it. The message is what
/// separates them: reading an unknown flag bit as "not a view" would let a
/// client INSERT into one, and a malformed packed column count read as a
/// silently truncated `PkColList` would name the wrong index.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    let flags = |bad: u8| {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes[1] = bad;
        bytes
    };
    let trailing = {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes.push(0);
        bytes
    };
    let bad_version = {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes[0] = VERSION + 1;
        bytes
    };
    let bad_index_count = {
        let mut bytes = RelDescriptorBlob {
            indexes: vec![RelIndex {
                cols: PkColList::from_slice(&[0]),
                is_unique: false,
            }],
            ..Default::default()
        }
        .encode();
        // Overwrite the packed column list with a word whose count field (bits
        // [0..4)) is past `PK_LIST_MAX_COLS`.
        crate::write_u64_le(
            &mut bytes,
            HEADER_LEN,
            (crate::pack_pk_cols(&[0, 1, 2, 3]) & !0xF) | 0xF,
        );
        bytes
    };
    // An FK naming a column past the schema's column count: admissible at 6
    // columns, out of range at 5, so no consumer can index with it.
    let fk_col_5 = RelDescriptorBlob {
        fks: vec![RelFk {
            col_idx: 5,
            fk_col_idx: 0,
            fk_table_id: 16,
        }],
        ..Default::default()
    }
    .encode();
    assert!(RelDescriptorBlob::decode(&fk_col_5, 6).is_ok(), "in range at 6 columns");

    // (what, blob, the consumer's column count, the message that must name it)
    let cases: &[(&str, Vec<u8>, usize, &str)] = &[
        ("bounded without view", flags(DESC_FLAG_BOUNDED), 4, "no relation class"),
        (
            "two class bits",
            flags(DESC_FLAG_VIEW | DESC_FLAG_STREAM),
            4,
            "no relation class",
        ),
        ("every class bit", flags(DESC_FLAG_CLASS), 4, "no relation class"),
        // Bit 5: the lowest bit above `DESC_FLAG_DELTA`, so this names no flag
        // rather than setting one that exists.
        ("unknown flag bit", flags(1 << 5), 0, "unknown flag bits"),
        ("trailing byte", trailing, 0, "trailing"),
        ("unknown version", bad_version, 0, "rel descriptor"),
        ("index column count", bad_index_count, 4, "out of range"),
        ("out-of-range FK column", fk_col_5, 5, "foreign key names column 5"),
    ];
    for (what, bytes, num_columns, want) in cases {
        let err = RelDescriptorBlob::decode(bytes, *num_columns).expect_err(what);
        assert!(err.contains(want), "{what}: {err:?} does not name {want:?}");
    }
}
