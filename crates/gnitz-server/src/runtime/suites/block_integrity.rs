//! The descriptive bytes of a WAL block — its 32-byte header, outside the block's
//! own checksum, so only the exact region-size relations constrain it — and the
//! per-slot checksum a zoned SAL group's directory carries.

use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};
use gnitz_store::schema::decode_schema_block;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_wire::{WAL_HEADER_SIZE, WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_TID};

/// A schema WAL block for a 4-column schema.
fn schema_block_4col() -> Vec<u8> {
    use gnitz_store::schema::SchemaColumn;
    use gnitz_wire::type_code;
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::F64, 1),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);
    crate::catalog::encode_schema_block(&schema, 7)
}

// ---------------------------------------------------------------------------
// The header's forgeable fields, through the real consumers
// ---------------------------------------------------------------------------

/// A forged lower `COUNT` on a schema block passes the col_idx monotonicity
/// check on the truncated prefix `[0, 1, …]`, so only the exact region size
/// rejects it — every fixed-stride region must be exactly `count` rows wide, and
/// a lowered `count` leaves each one too long. The descriptor it would otherwise
/// yield is what every later block for that table is decoded against.
#[test]
fn schema_block_count_forgeries_are_rejected() {
    let clean = schema_block_4col();
    assert_eq!(
        decode_schema_block(&clean).expect("clean schema block").num_columns(),
        4
    );
    for forged_count in [3u32, 2, 1] {
        let mut buf = clean.clone();
        gnitz_wire::write_u32_le(&mut buf, WAL_OFF_COUNT, forged_count);
        assert_eq!(
            decode_schema_block(&buf).err(),
            Some("schema block region size mismatch"),
            "schema COUNT 4 -> {forged_count} must be rejected"
        );
    }
}

/// Every single-bit flip in either block kind's header, driven through the
/// parser the SAL replay path uses. An accepted decode must be observationally
/// identical to the clean one. `TID` is deliberately unconstrained, and the
/// assertion below says so rather than letting it pass as "no observable
/// change".
#[test]
fn single_bit_header_sweep_changes_nothing_observable() {
    let schema = make_schema_u64_i64();
    let clean_data = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).encode_to_wire_vec(7, true);
    let reference = Batch::decode_from_wal_block(&clean_data, &schema, true).expect("clean");
    let ref_rows: Vec<(u128, i64)> = (0..reference.len())
        .map(|i| (reference.get_pk(i), reference.get_weight(i)))
        .collect();

    let mut tid_accepts = 0usize;
    let mut buf = clean_data.clone();
    sweep_bit_flips(&mut buf, 0..WAL_HEADER_SIZE, |byte, bit, buf| {
        if (WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8).contains(&byte) {
            return; // the checksum field itself: a flip there is caught by design
        }
        let Ok(decoded) = Batch::decode_from_wal_block(buf, &schema, true) else {
            return;
        };
        if (WAL_OFF_TID..WAL_OFF_TID + 4).contains(&byte) {
            tid_accepts += 1;
        }
        assert_eq!(
            decoded.len(),
            reference.len(),
            "byte {byte} bit {bit} changed the row count"
        );
        let got: Vec<(u128, i64)> = (0..decoded.len())
            .map(|i| (decoded.get_pk(i), decoded.get_weight(i)))
            .collect();
        assert_eq!(got, ref_rows, "byte {byte} bit {bit} changed the rows");
    });
    assert_eq!(
        tid_accepts, 32,
        "every `TID` flip is accepted and inert — nothing on the SAL path reads it, \
         and routing uses the group header's digest-protected `target_id`"
    );

    let mut buf = schema_block_4col();
    let reference = decode_schema_block(&buf).expect("clean");
    sweep_bit_flips(&mut buf, 0..WAL_HEADER_SIZE, |byte, bit, buf| {
        if (WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8).contains(&byte) {
            return;
        }
        let Ok(decoded) = decode_schema_block(buf) else {
            return;
        };
        assert_eq!(
            decoded.num_columns(),
            reference.num_columns(),
            "schema byte {byte} bit {bit} changed the column count"
        );
        for c in 0..decoded.num_columns() {
            assert_eq!(
                decoded.columns[c].type_code, reference.columns[c].type_code,
                "schema byte {byte} bit {bit} changed column {c}"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// The SAL slot checksum
// ---------------------------------------------------------------------------

/// Every bit of a slot of a zoned push group — laid out by the fast path
/// (`scatter::with_group`), the SAL's highest-volume writer — is covered by that
/// slot's directory checksum, and by no other slot's. Clearing `has_data` is the
/// costliest flip in that span: the decode reads `Ok` with no batch, so a
/// committed push slot's rows would vanish silently.
#[test]
fn every_bit_of_a_zoned_slot_is_covered_by_its_own_checksum() {
    use crate::runtime::sal::fixtures::{group_at, TestLog};

    let sal = TestLog::new(1 << 20, 2, 1);
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
    let mut excl = sal.writer.lock_exclusive();
    let scope = excl.begin(5, "test");
    sal.push_group(5, 16, schema, &batch, |g| scope.write(g, true));
    scope.commit().expect("sentinel fits");
    drop(excl);

    let msg = group_at(sal.log(), 0);
    let slots: Vec<(u32, Vec<u8>)> = msg.slots_written().map(|(w, b)| (w, b.to_vec())).collect();
    assert_eq!(slots.len(), 2, "both slots are written");
    for (w, bytes) in &slots {
        assert!(msg.slot_intact(*w, bytes), "clean slot {w} verifies");
    }

    let (victim, clean) = &slots[0];
    let mut buf = clean.clone();
    sweep_bit_flips(&mut buf, 0..clean.len(), |byte, bit, buf| {
        assert!(
            !msg.slot_intact(*victim, buf),
            "slot {victim} byte {byte} bit {bit} must fail its checksum"
        );
        for (w, bytes) in &slots[1..] {
            assert!(
                msg.slot_intact(*w, bytes),
                "slot {w} is untouched by slot {victim}'s damage"
            );
        }
    });
}

/// An unzoned group's directory entries hold checksum 0: nothing replays it, so
/// nothing is spent hashing it.
#[test]
fn an_unzoned_groups_entries_hold_checksum_0() {
    use crate::runtime::sal::fixtures::{group_at, TestLog};

    let sal = TestLog::new(1 << 20, 2, 1);
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
    sal.push_group(5, 16, schema, &batch, |g| sal.writer.lock_exclusive().write(g));

    let msg = group_at(sal.log(), 0);
    let entry = msg.dir.len() / msg.slots() as usize;
    for w in 0..msg.slots() as usize {
        assert_eq!(gnitz_wire::read_u64_le(msg.dir, w * entry + 4), 0, "slot {w}");
    }
}
