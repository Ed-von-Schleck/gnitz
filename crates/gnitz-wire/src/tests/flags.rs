use super::*;

/// The per-column metadata word both ends of the schema block encode and
/// decode: every field round-trips, and a non-PK column reports no position
/// however the other bits are set.
#[test]
fn col_meta_flags_roundtrip() {
    for &nullable in &[false, true] {
        for &hidden in &[false, true] {
            for &serial in &[false, true] {
                for pk_pos in [None, Some(0u8), Some(1), Some(3), Some(u8::MAX)] {
                    let f = pack_col_meta_flags(nullable, hidden, serial, pk_pos);
                    assert_eq!(col_meta_nullable(f), nullable);
                    assert_eq!(col_meta_hidden(f), hidden);
                    assert_eq!(col_meta_serial(f), serial);
                    assert_eq!(col_meta_pk_pos(f), pk_pos);
                }
            }
        }
    }
}

/// The conflict mode round-trips through its flag bits without disturbing them,
/// and bits naming no mode decode to `None` rather than being coerced —
/// coercing would turn a mode the server does not implement into a silent
/// upsert on a client's push.
#[test]
fn conflict_mode_roundtrips_and_rejects_unknown() {
    for &mode in WireConflictMode::ALL {
        let flags = wire_flags_set_conflict_mode(FLAG_PUSH, mode);
        assert_eq!(wire_flags_get_conflict_mode(flags), Some(mode));
        assert_eq!(flags & FLAG_PUSH, FLAG_PUSH, "the mode must not disturb other bits");
    }
    for raw in 2u8..=u8::MAX {
        assert_eq!(wire_flags_get_conflict_mode((raw as u64) << 16), None, "byte {raw}");
    }
}

/// A frame setting two verb bits is refused, not resolved by branch order —
/// otherwise a client reaches a verb it did not name.
#[test]
fn client_verb_rejects_two_verbs() {
    assert!(ClientVerb::from_flags(FLAG_SEEK | FLAG_PUSH).is_err());
    assert!(ClientVerb::from_flags(FLAG_DDL_TXN | FLAG_SCAN_MULTI).is_err());
    assert!(ClientVerb::from_flags(FLAG_ALLOCATE_TABLE_ID | FLAG_ALLOCATE_INDEX_ID).is_err());
}

/// Only a push carries rows. A data block on any other verb — `Scan`
/// included, which is the dangerous one, since that frame would otherwise be
/// answered with a streamed table dump — is a malformed frame.
#[test]
fn client_verb_rejects_data_on_a_non_push_verb() {
    assert_eq!(ClientVerb::from_flags(FLAG_HAS_DATA | FLAG_PUSH), Ok(ClientVerb::Push));
    for &verb in ClientVerb::ALL {
        if verb == ClientVerb::Push {
            continue;
        }
        assert!(
            ClientVerb::from_flags(verb.as_wire() | FLAG_HAS_DATA).is_err(),
            "{verb:?} must not accept a data block"
        );
    }
}
