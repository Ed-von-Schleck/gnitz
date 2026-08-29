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
    // The pre-compound spelling: a single-PK column is `IS_PK` with a zero
    // position byte.
    assert_eq!(pack_col_meta_flags(false, false, false, Some(0)), META_FLAG_IS_PK);
    assert_eq!(pack_col_meta_flags(true, false, false, None), META_FLAG_NULLABLE);
}

/// The conflict mode round-trips through its flag bits, and a byte naming no
/// mode is rejected rather than coerced. Coercing would turn a mode the
/// server does not implement into a silent upsert on a client's push.
#[test]
fn conflict_mode_roundtrips_and_rejects_unknown() {
    for mode in [WireConflictMode::Update, WireConflictMode::Error] {
        let flags = wire_flags_set_conflict_mode(FLAG_PUSH, mode);
        assert_eq!(wire_flags_get_conflict_mode(flags), Some(mode));
        assert_eq!(flags & FLAG_PUSH, FLAG_PUSH, "the mode must not disturb other bits");
    }
    for raw in 2u8..=u8::MAX {
        assert_eq!(WireConflictMode::from_u8(raw), None, "byte {raw} names no mode");
        let flags = (raw as u64) << 16;
        assert_eq!(wire_flags_get_conflict_mode(flags), None);
    }
}

/// Every verb round-trips through its own bit. The const guard proves the
/// bits are distinct; this proves each one decodes back to the variant that
/// produced it, which a disjointness assert cannot express.
#[test]
fn client_verb_roundtrips_through_its_bit() {
    for verb in ClientVerb::ALL {
        assert_eq!(ClientVerb::from_flags(verb.bit()), Ok(verb));
    }
}

/// A plain SCAN names no verb bit, so a frame carrying only the packed schema
/// version must resolve to `Scan` rather than to whatever bit happens to be
/// tested first.
#[test]
fn client_verb_reads_absence_as_scan() {
    assert_eq!(ClientVerb::from_flags(0), Ok(ClientVerb::Scan));
    let versioned = wire_flags_set_schema_version(0, 7);
    assert_eq!(ClientVerb::from_flags(versioned), Ok(ClientVerb::Scan));
    // The conflict-mode field is likewise not a verb.
    let moded = wire_flags_set_conflict_mode(0, WireConflictMode::Error);
    assert_eq!(ClientVerb::from_flags(moded), Ok(ClientVerb::Scan));
}

/// A frame setting two verb bits is refused, not resolved by branch order —
/// otherwise a client reaches a verb it did not name.
#[test]
fn client_verb_rejects_two_verbs() {
    assert!(ClientVerb::from_flags(FLAG_SEEK | FLAG_PUSH).is_err());
    assert!(ClientVerb::from_flags(FLAG_DDL_TXN | FLAG_SCAN_MULTI).is_err());
    // Including the pair that a `target_id`-gated router would have let
    // through to the plain-scan tail.
    assert!(ClientVerb::from_flags(FLAG_ALLOCATE_TABLE_ID | FLAG_ALLOCATE_INDEX_ID).is_err());
}

/// Only a push carries rows. A data block on any other verb — `Scan`
/// included, which is the dangerous one, since that frame would otherwise be
/// answered with a streamed table dump — is a malformed frame.
#[test]
fn client_verb_rejects_data_on_a_non_push_verb() {
    assert_eq!(ClientVerb::from_flags(FLAG_HAS_DATA | FLAG_PUSH), Ok(ClientVerb::Push));
    assert!(
        ClientVerb::from_flags(FLAG_HAS_DATA).is_err(),
        "a data block with no verb bit must not read as a scan"
    );
    for verb in ClientVerb::ALL {
        if verb == ClientVerb::Push {
            continue;
        }
        assert!(
            ClientVerb::from_flags(verb.bit() | FLAG_HAS_DATA).is_err(),
            "{verb:?} must not accept a data block"
        );
    }
}

#[test]
fn validate_scan_multi_tids_accepts_valid_lists() {
    assert!(validate_scan_multi_tids(&[7]).is_ok());
    let full: Vec<u64> = (0..SCAN_MULTI_MAX_RELATIONS as u64).collect();
    assert!(validate_scan_multi_tids(&full).is_ok(), "a full-cap list is valid");
}

#[test]
fn validate_scan_multi_tids_rejects_empty() {
    assert_eq!(
        validate_scan_multi_tids(&[]).unwrap_err(),
        "SCAN_MULTI: empty relation list"
    );
}

#[test]
fn validate_scan_multi_tids_rejects_over_cap() {
    let over: Vec<u64> = (0..=SCAN_MULTI_MAX_RELATIONS as u64).collect();
    let err = validate_scan_multi_tids(&over).unwrap_err();
    assert!(err.starts_with("SCAN_MULTI: too many relations"), "got: {err}");
}

#[test]
fn validate_scan_multi_tids_rejects_duplicate() {
    assert_eq!(
        validate_scan_multi_tids(&[7, 8, 7]).unwrap_err(),
        "SCAN_MULTI: duplicate relation 7"
    );
}
