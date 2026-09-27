use super::*;

fn roundtrip(d: &RelDescriptorBlob) -> RelDescriptorBlob {
    RelDescriptorBlob::decode(&d.encode()).expect("decode")
}

#[test]
fn multi_column_index_roundtrip() {
    let d = RelDescriptorBlob {
        class: RelClass::BoundedView,
        pk_repeats: true,
        serial: true,
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
    assert_eq!(roundtrip(&d), d);
}

#[test]
fn every_class_roundtrips() {
    for &class in RelClass::ALL {
        for pk_repeats in [false, true] {
            let d = RelDescriptorBlob { class, pk_repeats, ..Default::default() };
            assert_eq!(roundtrip(&d), d, "{class:?} pk_repeats={pk_repeats}");
        }
    }
}

#[test]
fn every_proper_prefix_is_an_error() {
    let d = RelDescriptorBlob {
        indexes: vec![RelIndex {
            cols: PkColList::from_slice(&[0]),
            is_unique: true,
        }],
        ..Default::default()
    };
    let bytes = d.encode();
    for cut in 0..bytes.len() {
        let err = RelDescriptorBlob::decode(&bytes[..cut]).unwrap_err();
        assert!(
            err.starts_with("rel descriptor:"),
            "a {cut}-byte prefix must fail as a rel descriptor, got: {err}"
        );
    }
}

#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    let with_byte = |at: usize, v: u8| {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes[at] = v;
        bytes
    };
    let trailing = {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes.push(0);
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
        // A column-count field (bits [0..4)) past `PK_LIST_MAX_COLS`.
        let first_entry = RelDescriptorBlob::default().encode().len();
        crate::write_u64_le(
            &mut bytes,
            first_entry,
            (crate::pack_pk_cols(&[0, 1, 2, 3]) & !0xF) | 0xF,
        );
        bytes
    };

    // (what, blob, the message that must name it)
    let cases: &[(&str, Vec<u8>, &str)] = &[
        ("unknown class", with_byte(0, 5), "unknown relation class 5"),
        ("pk_repeats byte 2", with_byte(1, 2), "neither 0 nor 1"),
        ("serial byte 2", with_byte(2, 2), "neither 0 nor 1"),
        ("trailing byte", trailing, "trailing"),
        ("index column count", bad_index_count, "out of range"),
    ];
    for (what, bytes, want) in cases {
        let err = RelDescriptorBlob::decode(bytes).expect_err(what);
        assert!(err.contains(want), "{what}: {err:?} does not name {want:?}");
    }
}
