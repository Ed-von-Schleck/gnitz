//! The descriptive bytes of a WAL block: its 32-byte header (outside the block's
//! own checksum, so only the exact region-size relations constrain it) and the
//! control block, whose own checksum this file exercises.

use crate::runtime::sal::GroupTargets;
use crate::runtime::wire::{WireData, WireMsg};
use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};
use gnitz_store::schema::decode_schema_block;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;
use gnitz_wire::control::{peek_control_block, peek_control_block_ipc};
use gnitz_wire::STATUS_OK;
use gnitz_wire::{WAL_HEADER_SIZE, WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_SIZE, WAL_OFF_TID};

/// A checksummed schema WAL block for a 4-column schema.
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
        decode_schema_block(&clean, true)
            .expect("clean schema block")
            .num_columns(),
        4
    );
    for forged_count in [3u32, 2, 1] {
        let mut buf = clean.clone();
        gnitz_wire::write_u32_le(&mut buf, WAL_OFF_COUNT, forged_count);
        assert_eq!(
            decode_schema_block(&buf, true).err(),
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
    let (reference, _) = Batch::decode_from_wal_block(&clean_data, &schema, true).expect("clean");
    let ref_rows: Vec<(u128, i64)> = (0..reference.len())
        .map(|i| (reference.get_pk(i), reference.get_weight(i)))
        .collect();

    let mut tid_accepts = 0usize;
    let mut buf = clean_data.clone();
    sweep_bit_flips(&mut buf, 0..WAL_HEADER_SIZE, |byte, bit, buf| {
        if (WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8).contains(&byte) {
            return; // the checksum field itself: a flip there is caught by design
        }
        let Ok((decoded, _)) = Batch::decode_from_wal_block(buf, &schema, true) else {
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
    let reference = decode_schema_block(&buf, true).expect("clean");
    sweep_bit_flips(&mut buf, 0..WAL_HEADER_SIZE, |byte, bit, buf| {
        if (WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8).contains(&byte) {
            return;
        }
        let Ok(decoded) = decode_schema_block(buf, true) else {
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
// The control block
// ---------------------------------------------------------------------------

/// Every descriptive byte of the control block — `seek_pk`, `seek_col_idx`,
/// `request_id`, `target_id`, `client_id`, `status`, the layout bits, the
/// directory — is covered by one sweep over the checksummed span.
///
/// `FLAG_HAS_DATA` is the costliest bit in that span: `decode_wire_body` routes
/// on it, so clearing it returns `Ok` with no batch and a committed push slot's
/// rows vanish silently. Nothing but the checksum can catch that — `Ok` with no
/// batch is the legitimate reading of every row-less slot, so the shape itself
/// cannot be made fatal.
#[test]
fn every_single_bit_flip_in_the_control_block_body_is_rejected() {
    // A full checksummed frame — control + schema + data — as
    // `SalWriter::write` writes one into a SAL slot.
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10)]);
    let block = crate::catalog::encode_schema_block(&schema, 7);
    let mut buf = WireMsg {
        target_id: 7,
        request_id: 0x1234_5678_9ABC_DEF0,
        schema_block: Some(&block),
        data: WireData::Whole(Some(&batch)),
        ..Default::default()
    }
    .encode_to_vec();
    sweep_bit_flips(&mut buf, WAL_HEADER_SIZE..CTRL_BLOCK_SIZE_NO_BLOB, |byte, bit, buf| {
        assert!(
            crate::runtime::wire::decode_wire(buf).is_err(),
            "control-block byte {byte} bit {bit} must be rejected"
        );
    });
}

/// `SIZE` frames the rest of the slot and sits outside the block's own checksum,
/// so only the exact `CTRL_BLOCK_SIZE_NO_BLOB + blob_len` relation constrains it.
/// Both blob shapes, so a relation written as a constant would still fail.
#[test]
fn the_control_blocks_size_field_is_exact() {
    for error_msg in [
        b"".as_slice(),
        b"an error message well past the twelve-byte inline threshold",
    ] {
        let mut buf = vec![0u8; 1024];
        let n = gnitz_wire::control::encode_ctrl_block(
            &mut buf,
            0,
            &gnitz_wire::control::ControlHeader {
                status: STATUS_OK,
                target_id: 7,
                ..Default::default()
            },
            error_msg,
            &[],
            true,
        );
        buf.truncate(n);
        assert_eq!(peek_control_block(&buf).expect("clean control block").block_size, n);
        assert!(
            n > CTRL_BLOCK_SIZE_NO_BLOB || error_msg.is_empty(),
            "the spill fixture must actually spill"
        );

        for delta in [-1i64, 1] {
            let mut forged = buf.clone();
            gnitz_wire::write_u32_le(&mut forged, WAL_OFF_SIZE, (n as i64 + delta) as u32);
            assert_eq!(
                peek_control_block_ipc(&forged).err(),
                Some("control block size disagrees with its blob region"),
                "SIZE {n} {delta:+} must be rejected on the blob relation, not framed through"
            );
        }
    }
}

/// The push fast path (`scatter::with_group`) stamps the control-block checksum
/// too — the SAL's highest-volume writer, and the one every other fixture here
/// misses.
#[test]
fn the_push_fast_paths_slots_carry_a_verifiable_control_block() {
    use crate::runtime::master::scatter::{with_commit_indices, with_group};
    use crate::runtime::sal::fixtures::{group_at, TestLog};
    use crate::runtime::sal::{DirectGroup, SalMessageKind};

    let nw = 2usize;
    let sal = TestLog::new(1 << 20, nw, 1);
    let writer = &sal.writer;

    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
    let relation = crate::runtime::wire::WireSchema::encoded(16, schema);
    let req_ids: Vec<u64> = (0..nw as u64).collect();
    let base = DirectGroup {
        targets: GroupTargets::All(&req_ids),
        lsn: 5,
        ..DirectGroup::new(SalMessageKind::Push)
    };
    with_commit_indices(&batch, &schema, nw, |wi| {
        with_group(&batch, wi, &relation, base, |g| writer.write(g)).expect("group fits")
    });

    let msg = group_at(sal.log(), 0);
    let slot = msg.slot(0).expect("slot 0 carries bytes");
    crate::runtime::wire::decode_wire(slot).expect("the fast path's slot must verify");

    // A flipped control byte in that slot fails the same checksum.
    let mut forged = slot.to_vec();
    forged[WAL_HEADER_SIZE] ^= 1;
    assert!(
        crate::runtime::wire::decode_wire(&forged).is_err(),
        "the fast path must stamp a control-block checksum, or nothing verifies it"
    );
}
