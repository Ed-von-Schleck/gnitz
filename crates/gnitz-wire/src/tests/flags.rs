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
        WireFlags { has_schema: true, ..base },
        WireFlags { has_data: true, ..base },
        WireFlags { continuation: true, ..base },
        WireFlags { batch_consolidated: true, ..base },
        WireFlags { scan_last: true, ..base },
        WireFlags { scan_fifo_reply: true, ..base },
    ]);
    cases.push(WireFlags {
        verb: ClientVerb::AllocIndexId,
        conflict_mode: WireConflictMode::Error,
        schema_version: u16::MAX,
        has_schema: true,
        has_data: true,
        continuation: true,
        batch_consolidated: true,
        scan_last: true,
        scan_fifo_reply: true,
        probe_mode: WireProbeMode::Project,
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
    assert!(WireFlags::unpack(1 << 40).is_err());
    assert!(WireFlags::unpack(1 << 63).is_err());
    assert!(WireFlags::unpack(13).is_err());
    assert!(WireFlags::unpack(2 << 8).is_err());
}

/// Only a push carries rows. A data block on any other verb — `Scan`
/// included, which is the dangerous one, since that frame would otherwise be
/// answered with a streamed table dump — is a malformed frame.
#[test]
fn client_verb_rejects_data_on_a_non_push_verb() {
    let with_data = |verb| WireFlags {
        verb,
        has_data: true,
        ..Default::default()
    };
    assert_eq!(with_data(ClientVerb::Push).client_verb(), Ok(ClientVerb::Push));
    for &verb in ClientVerb::ALL {
        if verb == ClientVerb::Push {
            continue;
        }
        assert!(
            with_data(verb).client_verb().is_err(),
            "{verb:?} must not accept a data block"
        );
    }
}
