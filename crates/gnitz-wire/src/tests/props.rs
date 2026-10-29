use super::*;

/// The `TABLE_TAB` flags word round-trips, pins its persisted bits as literals —
/// comparing against the constants the packer is written from would hold for any
/// value it gave them — and accepts exactly the single bits its layout defines.
#[test]
fn catalog_flag_words_roundtrip_pin_and_refuse_undefined_bits() {
    let dists = (0..=crate::PK_LIST_MAX_COLS as u8)
        .map(|prefix_len| TableDistribution::Keyed { prefix_len })
        .chain([TableDistribution::Replicated]);
    for distribution in dists {
        for stream in [false, true] {
            for serial in [false, true] {
                for compress in [false, true] {
                    let p = TableProps { stream, serial, distribution, compress };
                    assert_eq!(TableProps::from_flags(p.pack()), Ok(p));
                }
            }
        }
    }

    let t = |stream, serial, distribution| {
        TableProps {
            stream,
            serial,
            distribution,
            compress: false,
        }
        .pack()
    };
    assert_eq!(TableProps::default().pack(), 0);
    assert_eq!(t(false, false, TableDistribution::Replicated), 0b001);
    assert_eq!(t(true, false, TableDistribution::default()), 0b010);
    assert_eq!(t(false, true, TableDistribution::default()), 0b100);
    let compressed = TableProps { compress: true, ..TableProps::default() };
    assert_eq!(compressed.pack(), 0b1000);
    assert_eq!(t(false, false, TableDistribution::Keyed { prefix_len: 2 }), 2 << 8);

    for bit in 0..64u32 {
        let w = 1u64 << bit;
        assert_eq!(
            TableProps::from_flags(w).is_ok(),
            bit <= 3 || (8..16).contains(&bit),
            "table bit {bit}"
        );
    }
    // The one state the packing can hold and `TableProps` cannot.
    assert!(TableProps::from_flags(0b01 | 2 << 8)
        .unwrap_err()
        .contains("mutually exclusive"));
}

/// `validate` refuses what the flags word can hold but the table cannot: a
/// distribution prefix longer than the PK, a compressed stream, and a SERIAL
/// table that is a stream or whose PK is not its one generated column.
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
    let compressed = TableProps { compress: true, ..stream };
    assert!(compressed
        .validate(1)
        .unwrap_err()
        .contains("a stream cannot be compressed"));
}

/// `cluster_by` gives a keyed distribution the prefix, and refuses one on a
/// replicated table unless it is `0`, no prefix at all.
#[test]
fn cluster_by_takes_a_prefix_only_on_a_keyed_distribution() {
    let keyed = TableDistribution::Keyed { prefix_len: 3 };
    assert_eq!(keyed.cluster_by(1), Ok(TableDistribution::Keyed { prefix_len: 1 }));
    assert_eq!(keyed.cluster_by(0), Ok(TableDistribution::default()));
    assert_eq!(
        TableDistribution::Replicated.cluster_by(0),
        Ok(TableDistribution::Replicated)
    );
    assert!(TableDistribution::Replicated
        .cluster_by(2)
        .unwrap_err()
        .contains("(k=2)"));
}
