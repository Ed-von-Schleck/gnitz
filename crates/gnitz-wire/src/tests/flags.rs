use super::*;

/// Every combination of field values survives `unpack(pack())`, so a field
/// bleeding into a neighbour's bits shows up as a wrong neighbour.
#[test]
fn wire_flags_roundtrip_every_combination() {
    for &verb in ClientVerb::ALL {
        for &conflict_mode in WireConflictMode::ALL {
            for &probe_mode in WireProbeMode::ALL {
                for bits in 0..8u8 {
                    let f = WireFlags {
                        verb,
                        conflict_mode,
                        probe_mode,
                        continuation: bits & 1 != 0,
                        scan_last: bits & 2 != 0,
                        pushed: bits & 4 != 0,
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
    for bit in (16..32).chain(39..64) {
        assert!(WireFlags::unpack(1 << bit).is_err(), "bit {bit}");
    }
    // Bits 32/33 are the control codec's, so the flags word neither refuses nor
    // carries them.
    assert_eq!(WireFlags::unpack(3 << 32), Ok(WireFlags::default()));
    let bad_verb = (0..=u8::MAX).find(|&b| ClientVerb::from_wire(b).is_none()).unwrap();
    let bad_mode = (0..=u8::MAX)
        .find(|&b| WireConflictMode::from_wire(b).is_none())
        .unwrap();
    assert!(WireFlags::unpack(bad_verb as u64).is_err());
    assert!(WireFlags::unpack((bad_mode as u64) << 8).is_err());
}
