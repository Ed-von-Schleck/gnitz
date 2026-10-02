use super::*;

#[test]
fn every_descriptor_shape_roundtrips() {
    let two_indexes = vec![
        RelIndex {
            cols: PkColList::from_slice(&[1]),
            is_unique: true,
        },
        RelIndex {
            cols: PkColList::from_slice(&[2, 0, 1]),
            is_unique: false,
        },
    ];
    for &class in RelClass::ALL {
        for (pk_repeats, serial) in [(false, false), (true, false), (false, true), (true, true)] {
            for indexes in [vec![], two_indexes.clone()] {
                let d = RelDescriptorBlob { class, pk_repeats, serial, indexes };
                assert_eq!(RelDescriptorBlob::decode(&d.encode()), Ok(d.clone()), "{d:?}");
            }
        }
    }
}

/// Every decode guard, against the forgery that trips it; and a blob cut
/// anywhere fails as a rel descriptor.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    let base = RelDescriptorBlob {
        indexes: vec![RelIndex {
            cols: PkColList::from_slice(&[0]),
            is_unique: false,
        }],
        ..Default::default()
    }
    .encode();
    // The first index entry: its packed column list, then its flags word.
    let ix = RelDescriptorBlob::default().encode().len();
    let forge = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = base.clone();
        f(&mut b);
        b
    };

    // (what, blob, the message that must name it)
    let cases: &[(&str, Vec<u8>, &str)] = &[
        ("unknown class", forge(&|b| b[0] = 5), "unknown RelClass 5"),
        ("pk_repeats byte 2", forge(&|b| b[1] = 2), "neither 0 nor 1"),
        ("serial byte 2", forge(&|b| b[2] = 2), "neither 0 nor 1"),
        ("trailing byte", forge(&|b| b.push(0)), "trailing"),
        (
            "index column count",
            forge(&|b| crate::write_u64_le(b, ix, crate::PK_LIST_PACKED_FLAG | 0xF)),
            "out of range",
        ),
        (
            "untagged index list",
            forge(&|b| crate::write_u64_le(b, ix, 0)),
            "no packed-list flag",
        ),
        (
            "index flags",
            forge(&|b| crate::write_u64_le(b, ix + 8, 2)),
            "unknown bits",
        ),
    ];
    for (what, bytes, want) in cases {
        let err = RelDescriptorBlob::decode(bytes).expect_err(what);
        assert!(err.contains(want), "{what}: {err:?} does not name {want:?}");
    }
    for cut in 0..base.len() {
        let err = RelDescriptorBlob::decode(&base[..cut]).unwrap_err();
        assert!(err.starts_with("rel descriptor:"), "a {cut}-byte prefix: {err}");
    }
}
