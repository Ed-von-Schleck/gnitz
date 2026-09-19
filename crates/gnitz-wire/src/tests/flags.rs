use super::*;

/// Every field at its extremes survives `unpack(pack())` — each alone, so a
/// field bleeding into a neighbour's bits shows up as a wrong neighbour.
#[test]
fn wire_flags_roundtrip() {
    let base = WireFlags::default();
    let mut cases = vec![base];
    cases.extend(ClientVerb::ALL.iter().map(|&verb| WireFlags { verb, ..base }));
    cases.extend(
        WireConflictMode::ALL
            .iter()
            .map(|&conflict_mode| WireFlags { conflict_mode, ..base }),
    );
    cases.extend(
        WireProbeMode::ALL
            .iter()
            .map(|&probe_mode| WireFlags { probe_mode, ..base }),
    );
    cases.extend([0, 1, u16::MAX].map(|schema_version| WireFlags { schema_version, ..base }));
    cases.extend([
        WireFlags { continuation: true, ..base },
        WireFlags { batch_consolidated: true, ..base },
        WireFlags { scan_last: true, ..base },
        WireFlags {
            backfill: BackfillDecision::Checkpoint,
            ..base
        },
        WireFlags { backfill_pad: true, ..base },
    ]);
    cases.push(WireFlags {
        verb: ClientVerb::AllocIds,
        conflict_mode: WireConflictMode::Error,
        schema_version: u16::MAX,
        continuation: true,
        batch_consolidated: true,
        scan_last: true,
        probe_mode: WireProbeMode::Project,
        backfill: BackfillDecision::Checkpoint,
        backfill_pad: true,
    });
    for f in cases {
        assert_eq!(WireFlags::unpack(f.pack()), Ok(f), "{f:?}");
    }
}

/// A word naming a verb or mode this build does not define, or setting a bit
/// outside the layout, is refused rather than coerced — coercing would turn a
/// mode the server does not implement into a silent upsert on a client's push.
#[test]
fn wire_flags_reject_unknown() {
    assert!(WireFlags::unpack(1 << 42).is_err());
    // Bits 32/33 are the control codec's, so the flags word neither refuses nor
    // carries them.
    assert_eq!(WireFlags::unpack(3 << 32), Ok(WireFlags::default()));
    assert!(WireFlags::unpack(3 << 39).is_err());
    assert!(WireFlags::unpack(1 << 63).is_err());
    assert!(WireFlags::unpack(13).is_err());
    assert!(WireFlags::unpack(2 << 8).is_err());
}
