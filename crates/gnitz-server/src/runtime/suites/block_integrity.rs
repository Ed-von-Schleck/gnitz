//! The descriptive bytes a frame carries outside any checksum — a data block's
//! WAL header and a schema record's arity prefix — and the per-slot checksum a
//! zoned SAL group's directory carries.

use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};
use gnitz_store::schema::decode_schema_block;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_wire::wal::{WAL_HEADER_SIZE, WAL_OFF_TID};

/// A schema record for a 4-column schema, keyed by its first column.
fn schema_record_4col() -> Vec<u8> {
    use gnitz_store::schema::SchemaColumn;
    use gnitz_wire::type_code;
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::F64, 1),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);
    crate::catalog::encode_schema_block(&schema)
}

// ---------------------------------------------------------------------------
// The header's forgeable fields, through the real consumers
// ---------------------------------------------------------------------------

/// A forged lower `column_count` leaves every field it does read well-formed,
/// so only the trailing-bytes rule rejects it.
#[test]
fn schema_record_column_count_forgeries_are_rejected() {
    let clean = schema_record_4col();
    assert_eq!(
        decode_schema_block(&clean).expect("clean schema record").num_columns(),
        4
    );
    for forged_count in [3u32, 2, 1] {
        let mut buf = clean.clone();
        gnitz_wire::write_u32_le(&mut buf, 0, forged_count);
        let err = decode_schema_block(&buf).expect_err("column_count 4 -> {forged_count}");
        assert!(
            err.contains("trailing bytes"),
            "column_count 4 -> {forged_count}: {err}"
        );
    }
}

/// The arity prefix carries no redundancy of its own, so every flip in it must
/// either be refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    let mut buf = schema_record_4col();
    let reference = decode_schema_block(&buf).expect("clean");
    let prefix = 4 + 1 + reference.pk_indices().len();
    sweep_bit_flips(&mut buf, 0..prefix, |byte, bit, buf| {
        let Ok(decoded) = decode_schema_block(buf) else {
            return;
        };
        let same = decoded.num_columns() == reference.num_columns()
            && decoded.pk_indices() == reference.pk_indices()
            && (0..decoded.num_columns()).all(|c| {
                decoded.columns[c].type_code == reference.columns[c].type_code
                    && decoded.columns[c].nullable == reference.columns[c].nullable
            });
        assert!(!same, "schema prefix byte {byte} bit {bit} changed nothing observable");
    });
}

/// Every single-bit flip in a data block's header, driven through the parser
/// the SAL replay path uses. The header carries no checksum, so structural
/// relations alone hold every field but `TID`.
#[test]
fn single_bit_header_sweep_changes_nothing_observable() {
    let schema = make_schema_u64_i64();
    let clean_data = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).encode_to_wire_vec(7);
    let reference = Batch::decode_from_wal_block(&clean_data, &schema).expect("clean");
    let ref_rows: Vec<(u128, i64)> = (0..reference.len())
        .map(|i| (reference.get_pk(i), reference.get_weight(i)))
        .collect();

    let mut tid_accepts = 0usize;
    let mut buf = clean_data.clone();
    sweep_bit_flips(&mut buf, 0..WAL_HEADER_SIZE, |byte, bit, buf| {
        let Ok(decoded) = Batch::decode_from_wal_block(buf, &schema) else {
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
}

// ---------------------------------------------------------------------------
// The SAL slot checksum
// ---------------------------------------------------------------------------

/// Every bit of a slot of a zoned push group — laid out by the fast path
/// (`scatter::with_group`), the SAL's highest-volume writer — is covered by that
/// slot's directory checksum, and by no other slot's. Clearing the data-block bit is the
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
