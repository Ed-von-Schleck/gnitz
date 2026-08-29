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
        let mut bytes = RelDescriptorBlob {
            class,
            ..Default::default()
        }
        .encode();
        bytes[1] |= DESC_FLAG_DELTA;
        assert!(RelDescriptorBlob::decode(&bytes, 2).unwrap_err().contains("delta feed"));
    }
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
            let d = RelDescriptorBlob {
                class,
                replicated,
                ..Default::default()
            };
            let back = roundtrip(&d, 2);
            assert_eq!(back, d, "{class:?} replicated={replicated}");
        }
    }
}

/// The class bits are a closed set: a word naming no class is rejected rather
/// than silently read as one of them.
#[test]
fn a_flag_word_naming_no_class_is_rejected() {
    let mut bytes = RelDescriptorBlob::default().encode();
    for bad in [DESC_FLAG_BOUNDED, DESC_FLAG_VIEW | DESC_FLAG_STREAM, DESC_FLAG_CLASS] {
        bytes[1] = bad;
        let err = RelDescriptorBlob::decode(&bytes, 4).unwrap_err();
        assert!(err.contains("no relation class"), "flags {bad:#04x} gave: {err}");
    }
}

/// Every non-empty prefix short of the whole must be an error — only the
/// fully-empty blob means "absent".
#[test]
fn truncated_blob_is_an_error() {
    let d = RelDescriptorBlob {
        indexes: vec![RelIndex {
            cols: PkColList::single(0),
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

#[test]
fn trailing_bytes_are_rejected() {
    let mut bytes = RelDescriptorBlob::default().encode();
    bytes.push(0);
    let err = RelDescriptorBlob::decode(&bytes, 0).unwrap_err();
    assert!(err.contains("trailing"), "got: {err}");
}

#[test]
fn unknown_version_is_rejected() {
    let mut bytes = RelDescriptorBlob::default().encode();
    bytes[0] = VERSION + 1;
    assert!(RelDescriptorBlob::decode(&bytes, 0).is_err());
}

/// An unknown flag bit is an error, never ignored: reading it as "not a
/// view" would let a client INSERT into one.
#[test]
fn unknown_flag_bits_are_rejected() {
    let mut bytes = RelDescriptorBlob::default().encode();
    bytes[1] = 0xF0;
    let err = RelDescriptorBlob::decode(&bytes, 0).unwrap_err();
    assert!(err.contains("unknown flag bits"), "got: {err}");
}

/// A malformed packed column count must surface as an error, not as a
/// silently truncated `PkColList` — the `is_well_formed` guard.
#[test]
fn malformed_index_column_count_is_an_error() {
    let mut bytes = RelDescriptorBlob {
        indexes: vec![RelIndex {
            cols: PkColList::single(0),
            is_unique: false,
        }],
        ..Default::default()
    }
    .encode();
    // Overwrite the packed column list with a word whose count field (bits
    // [0..4)) is past `PK_LIST_MAX_COLS`.
    let bad = (crate::pack_pk_cols(&[0, 1, 2, 3]) & !0xF) | 0xF;
    bytes[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&bad.to_le_bytes());
    let err = RelDescriptorBlob::decode(&bytes, 4).unwrap_err();
    assert!(err.contains("out of range"), "got: {err}");
}

/// An FK naming a column past the schema's column count is an error, so no
/// consumer can index with it.
#[test]
fn out_of_range_fk_column_is_an_error() {
    let d = RelDescriptorBlob {
        fks: vec![RelFk {
            col_idx: 5,
            fk_col_idx: 0,
            fk_table_id: 16,
        }],
        ..Default::default()
    };
    let bytes = d.encode();
    assert!(RelDescriptorBlob::decode(&bytes, 6).is_ok());
    let err = RelDescriptorBlob::decode(&bytes, 5).unwrap_err();
    assert!(err.contains("foreign key names column 5"), "got: {err}");
}
