//! The descriptive bytes of a WAL block: its 32-byte header (outside the block's
//! own checksum, so only the exact region-size relations constrain it) and the
//! control block (checksummed but never verified until now).

use crate::runtime::wire::{
    build_schema_wire_block, decode_schema_block, encode_ctrl_block_direct, peek_control_block, peek_control_block_ipc,
    WireData, WireMsg, CTRL_BLOCK_SIZE_NO_BLOB, STATUS_OK,
};
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;
use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips, SharedRegion};
use gnitz_wire::{WAL_HEADER_SIZE, WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID};

/// A checksummed data WAL block over `rows` of the standard `(u64 pk, i64 val)`
/// schema.
fn data_block(rows: &[(u64, i64, i64)]) -> (SchemaDescriptor, Vec<u8>) {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, rows);
    let mut buf = vec![0u8; batch.wire_byte_size()];
    batch.encode_to_wire(7, &mut buf, 0, true);
    (schema, buf)
}

/// A checksummed schema WAL block for a 4-column schema.
fn schema_block_4col() -> Vec<u8> {
    use crate::schema::{type_code, SchemaColumn};
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::F64, 1),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);
    build_schema_wire_block(&schema, 7)
}

fn set_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

// ---------------------------------------------------------------------------
// The header's forgeable fields, through the real consumers
// ---------------------------------------------------------------------------

/// `COUNT` is a `u32` outside the block's own checksum, so `n → 0` is a
/// single-bit flip whenever `n` is a power of two. Only the exact region-size
/// relation rejects it.
#[test]
fn data_block_count_forgeries_are_rejected() {
    let (schema, clean) = data_block(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    for forged_count in [0u32, 2, 1000] {
        let mut buf = clean.clone();
        set_u32(&mut buf, WAL_OFF_COUNT, forged_count);
        assert_eq!(
            Batch::decode_from_wal_block(&buf, &schema, true).err(),
            Some("data WAL region size mismatch"),
            "COUNT 3 -> {forged_count} must be rejected by the exact region-size relation"
        );
    }

    let mut buf = clean.clone();
    set_u32(&mut buf, WAL_OFF_NUM_REGIONS, 4);
    assert!(
        Batch::decode_from_wal_block(&buf, &schema, true).is_err(),
        "a forged region count must be rejected"
    );
}

/// The shape every `FLAG_DDL_SYNC` group and every single-row `INSERT` takes:
/// `COUNT 1 -> 0` is one bit, and the row's bytes are still in the block.
#[test]
fn one_row_data_block_count_zero_is_rejected() {
    let (schema, clean) = data_block(&[(42, 1, 99)]);
    assert_eq!(
        Batch::decode_from_wal_block(&clean, &schema, true)
            .map(|(b, _)| b.count)
            .ok(),
        Some(1)
    );
    let mut buf = clean;
    // Bit 0 of COUNT: 1 -> 0.
    buf[WAL_OFF_COUNT] ^= 1;
    assert_eq!(
        Batch::decode_from_wal_block(&buf, &schema, true).err(),
        Some("data WAL region size mismatch"),
        "a single-bit COUNT 1 -> 0 flip must not decode as an empty block"
    );
}

/// A genuinely empty block still round-trips — the case the deleted `n == 0`
/// short-circuit existed to serve.
#[test]
fn empty_data_block_still_decodes() {
    let schema = make_schema_u64_i64();
    let batch = Batch::empty_with_schema(&schema);
    let mut buf = vec![0u8; batch.wire_byte_size()];
    batch.encode_to_wire(7, &mut buf, 0, true);
    let (decoded, _) = Batch::decode_from_wal_block(&buf, &schema, true).expect("an empty block decodes");
    assert_eq!(decoded.count, 0);
}

/// A forged lower `COUNT` on a schema block passes the col_idx monotonicity
/// check on the truncated prefix `[0, 1, …]`, so only the exact region size
/// rejects it — and the descriptor it would yield is what every later block for
/// that table is decoded against.
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
        set_u32(&mut buf, WAL_OFF_COUNT, forged_count);
        assert_eq!(
            decode_schema_block(&buf, true).err(),
            Some("schema col_idx region OOB"),
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
    let (schema, clean_data) = data_block(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let (reference, _) = Batch::decode_from_wal_block(&clean_data, &schema, true).expect("clean");
    let ref_rows: Vec<(u128, i64)> = (0..reference.count)
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
            decoded.count, reference.count,
            "byte {byte} bit {bit} changed the row count"
        );
        let got: Vec<(u128, i64)> = (0..decoded.count)
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

/// A full checksummed wire frame: control + schema + data, as `write_group_direct`
/// writes one into a SAL slot.
fn wire_frame(schema: &SchemaDescriptor, batch: &Batch, flags: u64) -> Vec<u8> {
    let msg = WireMsg {
        target_id: 7,
        flags,
        request_id: 0x1234_5678_9ABC_DEF0,
        schema: Some(schema),
        data: WireData::Whole(Some(batch)),
        ..Default::default()
    };
    let mut buf = vec![0u8; msg.size()];
    msg.encode(&mut buf, 0);
    buf
}

/// `decode_wire_body` routes on `FLAG_HAS_DATA`, so clearing it returns `Ok`
/// with no batch and a committed push slot's rows vanish silently. Only the
/// control block's own checksum catches it: `Ok` with no batch is the legitimate
/// reading of every row-less slot, so the shape itself cannot be made fatal.
#[test]
fn a_cleared_flag_has_data_bit_is_an_error_not_an_empty_slot() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let marker: u64 = 0x0BAD_C0DE_0000_0000;
    let clean = wire_frame(&schema, &batch, marker);
    assert_eq!(
        crate::runtime::wire::decode_wire(&clean)
            .expect("clean frame")
            .data_batch
            .map(|b| b.count),
        Some(2)
    );

    // The encoder ORs FLAG_HAS_SCHEMA / FLAG_HAS_DATA / the layout bits onto the
    // marker, so locate the cell by the value actually written.
    let flags_cell = {
        let ctrl = &clean[..CTRL_BLOCK_SIZE_NO_BLOB];
        (WAL_HEADER_SIZE..CTRL_BLOCK_SIZE_NO_BLOB - 8)
            .find(|&off| {
                let v = u64::from_le_bytes(ctrl[off..off + 8].try_into().unwrap());
                v & marker == marker
            })
            .expect("flags cell present")
    };

    let mut forged = clean;
    let cleared = u64::from_le_bytes(forged[flags_cell..flags_cell + 8].try_into().unwrap()) & !(1u64 << 49);
    forged[flags_cell..flags_cell + 8].copy_from_slice(&cleared.to_le_bytes());
    assert_eq!(
        crate::runtime::wire::decode_wire(&forged).err(),
        Some("control block checksum mismatch"),
        "clearing FLAG_HAS_DATA must be an error, not a silently row-less slot"
    );
}

/// Every descriptive byte of the control block — `seek_pk`, `seek_col_idx`,
/// `request_id`, `target_id`, `client_id`, `status`, the layout bits, the
/// directory — is covered by one sweep over the checksummed span.
#[test]
fn every_single_bit_flip_in_the_control_block_body_is_rejected() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10)]);
    let mut buf = wire_frame(&schema, &batch, 0);
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
        let n = encode_ctrl_block_direct(
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
            set_u32(&mut forged, WAL_OFF_SIZE, (n as i64 + delta) as u32);
            assert_eq!(
                peek_control_block_ipc(&forged).err(),
                Some("control block size disagrees with its blob region"),
                "SIZE {n} {delta:+} must be rejected on the blob relation, not framed through"
            );
        }
    }
}

/// The push fast path (`scatter_wire_group`) stamps the control-block checksum
/// too — the SAL's highest-volume writer, and the one every other fixture here
/// misses.
#[test]
fn the_push_fast_paths_slots_carry_a_verifiable_control_block() {
    use crate::runtime::master::scatter::with_commit_indices;
    use crate::runtime::sal::{SalReader, SalWriter, FLAG_PUSH};
    use crate::storage::compute_wire_props;

    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let nw = 2usize;
        let efds: Vec<i32> = (0..nw).map(|_| crate::foundation::posix_io::eventfd_create()).collect();
        let writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
        writer.reset(0, 1);

        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
        let block = build_schema_wire_block(&schema, 16);
        let props = compute_wire_props(&schema);
        let req_ids: Vec<u64> = (0..nw as u64).collect();
        with_commit_indices(&batch, &schema, nw, |wi| {
            writer
                .scatter_wire_group(
                    &batch,
                    wi,
                    &schema,
                    16,
                    5,
                    FLAG_PUSH,
                    0,
                    0,
                    &req_ids,
                    Some(block.as_slice()),
                    Some(props),
                )
                .expect("group fits")
        });

        let slot = SalReader::for_walk(ptr as *const u8, 0, size)
            .slot_at(0, 0)
            .expect("slot 0 carries bytes");
        crate::runtime::wire::decode_wire(slot).expect("the fast path's slot must verify");

        // And a flipped control byte in that slot is now detected.
        let mut forged = slot.to_vec();
        forged[WAL_HEADER_SIZE] ^= 1;
        assert!(
            crate::runtime::wire::decode_wire(&forged).is_err(),
            "the fast path must stamp a control-block checksum, or nothing verifies it"
        );

        for &e in &efds {
            libc::close(e);
        }
    }
}
