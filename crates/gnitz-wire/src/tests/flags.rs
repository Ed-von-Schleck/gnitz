use super::*;

/// Every combination of field values survives `unpack(pack())`, so a field
/// bleeding into a neighbour's bits shows up as a wrong neighbour.
#[test]
fn wire_flags_roundtrip_every_combination() {
    for &verb in ClientVerb::ALL {
        for &conflict_mode in WireConflictMode::ALL {
            for &probe_mode in WireProbeMode::ALL {
                for (bits, &lane) in (0..4u8).flat_map(|bits| WireLane::ALL.iter().map(move |l| (bits, l))) {
                    let f = WireFlags {
                        verb,
                        conflict_mode,
                        probe_mode,
                        continuation: bits & 1 != 0,
                        scan_last: bits & 2 != 0,
                        lane,
                    };
                    assert_eq!(WireFlags::unpack(f.pack()), Ok(f), "{f:?}");
                }
            }
        }
    }
}

/// A word naming a verb or mode this build does not define, or setting a bit
/// outside the layout, is refused rather than coerced — coercing would turn a
/// mode the server does not implement into a silent upsert on a client's push.
#[test]
fn wire_flags_reject_unknown() {
    for bit in (16..32).chain(40..64) {
        assert!(WireFlags::unpack(1 << bit).is_err(), "bit {bit}");
    }
    // Bits 32/33 are the control codec's, so the flags word neither refuses nor
    // carries them.
    assert_eq!(WireFlags::unpack(3 << 32), Ok(WireFlags::default()));
    // The two-bit fields leave one value unused.
    let probe = u64::from(crate::outside(WireProbeMode::from_wire));
    let lane = u64::from(crate::outside(WireLane::from_wire));
    assert!(probe < 4 && lane < 4, "a two-bit field holds no unknown value");
    for (w, want) in [
        (u64::from(crate::outside(ClientVerb::from_wire)), "unknown request verb"),
        (
            u64::from(crate::outside(WireConflictMode::from_wire)) << 8,
            "unknown conflict mode",
        ),
        (probe << 36, "unknown probe mode"),
        (lane << 38, "unknown lane"),
    ] {
        assert_eq!(WireFlags::unpack(w), Err(format!("flags: {want}").as_str()), "{w:#x}");
    }
}
