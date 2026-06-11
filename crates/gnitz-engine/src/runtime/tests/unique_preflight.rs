//! Unit tests for the CREATE UNIQUE INDEX pre-flight building blocks: the
//! worker's sorted-span frame train (`send_unique_preflight_keys`), the
//! per-row span projection (`IndexKeySpec::key_bytes`), and the master's
//! per-span merge accounting (`PreflightAccumulator`). The full distributed
//! path (fan-out, k-way merge over live W2M trains, DDL integration) is
//! covered by the multi-worker E2E suite.
//!
//! Every key is the OPK leading-key span (`PkBuf`) — equality-correct and
//! byte-orderable at any width — replacing the old single-`u128` key.

use rustc_hash::FxHashSet;

use crate::catalog::make_index_schema;
use crate::runtime::master::PreflightAccumulator;
use crate::runtime::sal::{
    unique_preflight_wire_schema, SalMessageKind, FLAG_UNIQUE_PREFLIGHT,
};
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::w2m_ring;
use crate::runtime::wire::{
    self, peek_control_block, FLAG_CONTINUATION, FLAG_HAS_SCHEMA, FLAG_SCAN_LAST,
    SchemaWithVersion,
};
use crate::runtime::worker::send_unique_preflight_keys;
use crate::schema::{IndexKeySpec, SchemaColumn, SchemaDescriptor, type_code};
use crate::storage::{Batch, PkBuf};

// ---------------------------------------------------------------------------
// Span helpers
// ---------------------------------------------------------------------------

/// OPK leading-key span of a single U128 value (U128 OPK == big-endian). The
/// 16-byte span the round-trip tests below ship over the wire.
fn span_u128(v: u128) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
}

/// Frame schema of a single-U128-span pre-flight reply (one U128 PK column),
/// so the round-trip tests ship a 16-byte PK span per row.
fn u128_frame_schema() -> SchemaDescriptor {
    // Index columns are all non-nullable (PK region); nullable=0 throughout.
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0])
}

// ---------------------------------------------------------------------------
// Frame-train round-trip
// ---------------------------------------------------------------------------

fn with_test_ring(f: impl FnOnce(&W2mWriter, &W2mReceiver)) {
    const CAP: usize = 1 << 20;
    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            CAP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1, 0,
        ) as *mut u8
    };
    assert!(!region.is_null());
    unsafe { w2m_ring::init_region_for_tests(region, CAP as u64) };
    let writer = W2mWriter::new(region, CAP as u64);
    let receiver = W2mReceiver::new(vec![region]);
    f(&writer, &receiver);
    unsafe { libc::munmap(region as *mut libc::c_void, CAP) };
}

/// Drain every frame of one pre-flight train from the ring, asserting the
/// flag/schema discipline the master's merge relies on, and return the spans
/// decoded the way the merge decodes them: the whole PK region of each row
/// (`get_pk_bytes` → `PkBuf`), continuations against the saved schema hint.
fn drain_train(receiver: &W2mReceiver, expected_req_id: u64) -> Vec<PkBuf> {
    let mut keys = Vec::new();
    let mut saved_schema: Option<(SchemaDescriptor, u16)> = None;
    let mut frames = 0usize;
    loop {
        let slot = receiver.try_read_slot(0).expect("frame missing from train");
        assert_eq!(
            slot.internal_req_id, expected_req_id as u32,
            "ring prefix must carry the request id",
        );
        let ctrl = peek_control_block(slot.bytes()).expect("ctrl decodes");
        assert_eq!(ctrl.status, 0);
        assert_ne!(
            ctrl.flags & FLAG_CONTINUATION, 0,
            "every pre-flight frame carries FLAG_CONTINUATION",
        );
        let last = ctrl.flags & FLAG_SCAN_LAST != 0;
        if frames == 0 {
            assert_ne!(
                ctrl.flags & FLAG_HAS_SCHEMA, 0,
                "first frame must carry the synthetic schema block",
            );
        } else {
            assert_eq!(
                ctrl.flags & FLAG_HAS_SCHEMA, 0,
                "continuation frames must not re-send the schema",
            );
        }
        let server_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
        let ctrl_size = ctrl.block_size;
        let hint = saved_schema.as_ref().map(|(s, v)| SchemaWithVersion {
            descriptor: s, version: *v,
        });
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl_size, ctrl, hint)
            .expect("frame decodes");
        if saved_schema.is_none() {
            let s = zc.schema.expect("first frame schema");
            // The reply schema's PK region IS the OPK leading-key span; every
            // column is in the PK, no payload columns.
            assert!(s.num_columns() >= 1);
            assert_eq!(s.pk_indices().len(), s.num_columns());
            saved_schema = Some((s, server_version));
        }
        if let Some(ref mb) = zc.data_batch {
            for i in 0..mb.count {
                keys.push(PkBuf::from_bytes(mb.get_pk_bytes(i)));
            }
        }
        frames += 1;
        drop(zc);
        drop(slot);
        if last { break; }
    }
    keys
}

/// Multi-frame train: spans split across frames at `keys_per_frame`, the
/// terminal frame tagged FLAG_SCAN_LAST, and every span — including extreme
/// u128s — round-trips through the wire to the exact byte span.
#[test]
fn preflight_train_multi_frame_key_roundtrip() {
    let keys: Vec<PkBuf> = [
        0u128,
        1,
        41,
        42,
        (i64::MAX as u64) as u128,
        ((-2i64) as u64) as u128,
        ((-1i64) as u64) as u128,
        u128::MAX - 1,
        u128::MAX,
    ].into_iter().map(span_u128).collect();
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 77, &keys, &frame_schema, 9001, 4);
        let got = drain_train(receiver, 9001);
        assert_eq!(got, keys);
        assert!(receiver.try_read_slot(0).is_none(), "no frames after terminal");
    });
}

/// A train whose span count is an exact multiple of the frame size must not
/// emit a trailing empty frame: the last full frame is the terminal one.
#[test]
fn preflight_train_exact_frame_boundary() {
    let keys: Vec<PkBuf> = (0..8u128).map(span_u128).collect();
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 77, &keys, &frame_schema, 42, 4);
        let got = drain_train(receiver, 42);
        assert_eq!(got, keys);
        assert!(receiver.try_read_slot(0).is_none());
    });
}

/// An empty partition still answers with exactly one empty terminal frame so
/// the master's drain sees the train end.
#[test]
fn preflight_train_empty_partition_single_terminal_frame() {
    let frame_schema = u128_frame_schema();
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 77, &[], &frame_schema, 7, 4);
        let slot = receiver.try_read_slot(0).expect("terminal frame");
        let ctrl = peek_control_block(slot.bytes()).expect("ctrl decodes");
        assert_eq!(ctrl.status, 0);
        assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0, "single frame must be terminal");
        assert_eq!(ctrl.flags & wire::FLAG_HAS_DATA, 0, "no data on empty train");
        drop(slot);
        assert!(receiver.try_read_slot(0).is_none());
    });
}

/// A composite (>16-byte) span round-trips through the wire frame: the reply
/// schema's PK region holds the full span verbatim, the regression the old
/// fixed-`U128` reply column could not represent.
#[test]
fn preflight_train_composite_wide_span_roundtrip() {
    // Two U64 index columns → a 16-byte composite leading span, plus a U64
    // source PK; the frame schema is derived exactly as both endpoints do.
    let idx_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0, 1, 2],
    );
    let frame_schema = unique_preflight_wire_schema(&idx_schema, 2);
    // Spans are (a_be ++ b_be), 16 bytes. Two spans share their leading 8 bytes
    // but differ in the trailing column — distinct keys a u128 truncation to the
    // leading column would falsely merge.
    let span = |a: u64, b: u64| {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_be_bytes());
        buf[8..].copy_from_slice(&b.to_be_bytes());
        PkBuf::from_bytes(&buf)
    };
    let keys = vec![span(7, 1), span(7, 2), span(9, 1)];
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 5, &keys, &frame_schema, 3, 2);
        let got = drain_train(receiver, 3);
        assert_eq!(got, keys);
        assert_eq!(got[0].pk_bytes().len(), 16, "composite span is the full 16 bytes");
    });
}

// ---------------------------------------------------------------------------
// Worker projection: IndexKeySpec::key_bytes feeding the train
// ---------------------------------------------------------------------------

/// Mirror of the worker's per-chunk projection loop: build each positive-weight
/// row's OPK span via `IndexKeySpec::key_bytes`, emitting it once per unit of
/// weight (capped at a pair), skipping any-NULL rows. Returns the SORTED spans.
fn project_sorted(batch: &Batch, owner: &SchemaDescriptor, cols: &[u32]) -> Vec<PkBuf> {
    let idx_schema = make_index_schema(cols, owner).expect("index schema");
    let spec = IndexKeySpec::new(cols, owner, &idx_schema);
    let mb = batch.as_mem_batch();
    let mut keys: Vec<PkBuf> = Vec::new();
    let mut keybuf = PkBuf::empty(0);
    for row in 0..batch.count {
        let w = batch.get_weight(row);
        if w <= 0 { continue; }
        if !spec.key_bytes(&mb, row, &mut keybuf) { continue; }
        keys.push(keybuf);
        if w > 1 { keys.push(keybuf); }
    }
    keys.sort_unstable();
    keys
}

/// The worker projection (`IndexKeySpec::key_bytes` on the owner schema) and the wire
/// round-trip compose to preserve spans end-to-end for a signed payload column:
/// equal signed values (including negatives) produce equal spans on the master
/// side, and retractions and NULLs never enter the stream.
#[test]
fn preflight_signed_payload_projection_roundtrip() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let mut batch = Batch::with_schema(schema, 8);
    // (pk, val, weight, null): two rows share val=-5 (the duplicate the
    // pre-flight exists to catch), one NULL, one retracted row.
    let rows: [(u128, i64, i64, u64); 6] = [
        (1, -5, 1, 0),
        (2, 300, 1, 0),
        (3, -5, 1, 0),
        (4, 0, 1, 1),   // NULL val: skipped
        (5, 7, -1, 0),  // retracted: skipped
        (6, i64::MIN, 1, 0),
    ];
    for &(pk, val, weight, null_word) in &rows {
        unsafe {
            batch.append_row_simple(pk, weight, null_word, &[val], &[0], &[std::ptr::null()], &[0]);
        }
    }

    let keys = project_sorted(&batch, &schema, &[1]);

    // Expected spans: U64-promoted OPK (big-endian of the two's-complement u64
    // bits) of the four non-NULL, non-retracted values, sorted byte-lex.
    let expected: Vec<PkBuf> = {
        let mut v: Vec<PkBuf> = [-5i64, 300, -5, i64::MIN]
            .iter()
            .map(|&x| PkBuf::from_bytes(&(x as u64).to_be_bytes()))
            .collect();
        v.sort_unstable();
        v
    };
    assert_eq!(keys, expected, "projection must keep two's-complement bits, OPK-encoded");

    let frame_schema = unique_preflight_wire_schema(&make_index_schema(&[1], &schema).unwrap(), 1);
    with_test_ring(|writer, receiver| {
        send_unique_preflight_keys(writer, 5, &keys, &frame_schema, 11, 3);
        let got = drain_train(receiver, 11);
        assert_eq!(got, keys);
        // The duplicate pair is adjacent in the sorted stream — exactly what the
        // master's prev == popped check rejects.
        let dup = PkBuf::from_bytes(&((-5i64) as u64).to_be_bytes());
        assert_eq!(got.iter().filter(|&&k| k == dup).count(), 2);
    });
}

/// A consolidated row at weight 2 is the same (PK, payload) twice: the worker
/// collection contract (one span per unit of weight, capped at a pair) makes
/// the multiplicity visible to the merge as an adjacent equal pair, and the
/// accumulator's verdict is duplicate.
#[test]
fn preflight_weight2_row_emits_adjacent_pair() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let mut batch = Batch::with_schema(schema, 4);
    let rows: [(u128, i64, i64, u64); 3] = [
        (1, 7, 1, 0),
        (2, 9, 2, 0),   // consolidated duplicate: weight 2
        (3, 11, 1, 0),
    ];
    for &(pk, val, weight, null_word) in &rows {
        unsafe {
            batch.append_row_simple(pk, weight, null_word, &[val], &[0], &[std::ptr::null()], &[0]);
        }
    }

    let keys = project_sorted(&batch, &schema, &[1]);
    let s = |v: i64| PkBuf::from_bytes(&(v as u64).to_be_bytes());
    assert_eq!(keys, vec![s(7), s(9), s(9), s(11)], "weight-2 row must emit its span twice");

    let mut acc = PreflightAccumulator::new(1000);
    assert!(!offer_all(&mut acc, &keys), "the adjacent pair must flip the verdict");
    assert!(acc.duplicate);
}

/// A composite `UNIQUE (a, b)` span packs both columns: two rows differing only
/// in `b` produce DISTINCT spans (admitted), and two rows whose low 8 bytes
/// (column `a`) collide but whose full span differs are distinct — the
/// regression a `u128` leading-column truncation would have falsely merged.
#[test]
fn preflight_composite_projection_distinguishes_trailing_column() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),   // pk
            SchemaColumn::new(type_code::U64, 1),   // a
            SchemaColumn::new(type_code::U64, 2),   // b
        ],
        &[0],
    );
    let mut batch = Batch::with_schema(schema, 4);
    // (pk, a, b): (10,7,1) and (11,7,2) share a=7 but differ in b → distinct.
    let rows: [(u128, u64, u64); 2] = [(10, 7, 1), (11, 7, 2)];
    for &(pk, a, b) in &rows {
        unsafe {
            batch.append_row_simple(
                pk, 1, 0, &[a as i64, b as i64], &[0, 0],
                &[std::ptr::null(), std::ptr::null()], &[0, 0]);
        }
    }
    let keys = project_sorted(&batch, &schema, &[1, 2]);
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1], "rows differing only in the trailing column are distinct");
    assert_eq!(keys[0].pk_bytes().len(), 16, "composite span spans both columns");

    // No duplicate: the accumulator admits both.
    let mut acc = PreflightAccumulator::new(1000);
    assert!(offer_all(&mut acc, &keys));
    assert!(!acc.duplicate);
}

/// `IndexKeySpec::key_bytes` produces exactly the leading `idx_key_size` bytes
/// that `batch_project_index` writes as the index PK prefix — so the in-memory
/// key, the projected index entry, and the seek prefix all agree, across a
/// signed / unsigned / U128 column mix.
#[test]
fn index_key_spec_equals_projected_leading_span() {
    use crate::dag::DagEngine;
    // Owner: PK id U64; a I64 (signed payload), b U128 (payload).
    let owner = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ],
        &[0],
    );
    // Composite unique on (a, b): promoted (I64→U64, U128→U128) = 8 + 16 = 24.
    let cols = [1u32, 2];
    let idx_schema = make_index_schema(&cols, &owner).unwrap();
    let spec = IndexKeySpec::new(&cols, &owner, &idx_schema);
    let idx_key_size = spec.key_size();
    assert_eq!(idx_key_size, 8 + 16, "I64→U64 (8) + U128 (16)");

    let mut batch = Batch::with_schema(owner, 4);
    let rows: [(u128, i64, u128); 3] = [
        (1, -3, 100),
        (2, 7, u128::MAX),
        (3, i64::MIN, 0),
    ];
    for &(id, a, b) in &rows {
        unsafe {
            batch.append_row_simple(
                id, 1, 0,
                &[a, b as u64 as i64], &[0, (b >> 64) as u64],
                &[std::ptr::null(), std::ptr::null()], &[0, 0]);
        }
    }

    // Reference: the projected index entry's leading idx_key_size bytes.
    let projected = DagEngine::batch_project_index(&batch, &cols, &owner, &idx_schema);
    assert_eq!(projected.count, rows.len());

    let mb = batch.as_mem_batch();
    let mut keybuf = PkBuf::empty(0);
    for row in 0..batch.count {
        assert!(spec.key_bytes(&mb, row, &mut keybuf));
        assert_eq!(
            keybuf.pk_bytes(), &projected.get_pk_bytes(row)[..idx_key_size],
            "key_bytes must equal the projected entry's leading span (row {row})",
        );
    }
}

/// A row NULL in ANY indexed column is skipped (`key_bytes` → false) —
/// SQL NULL-distinctness, mirroring `batch_project_index`.
#[test]
fn index_key_spec_skips_any_null_column() {
    let owner = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 1),   // a: nullable payload
            SchemaColumn::new(type_code::U64, 1),   // b: nullable payload
        ],
        &[0],
    );
    let cols = [1u32, 2];
    let idx_schema = make_index_schema(&cols, &owner).unwrap();
    let mut batch = Batch::with_schema(owner, 4);
    // (id, a, b, null_word over payload slots): both present, a NULL, b NULL.
    let rows: [(u128, i64, i64, u64); 3] = [
        (1, 5, 6, 0b00),   // both present → indexed
        (2, 5, 6, 0b01),   // a NULL → skipped
        (3, 5, 6, 0b10),   // b NULL → skipped
    ];
    for &(id, a, b, null_word) in &rows {
        unsafe {
            batch.append_row_simple(
                id, 1, null_word, &[a, b], &[0, 0],
                &[std::ptr::null(), std::ptr::null()], &[0, 0]);
        }
    }
    let spec = IndexKeySpec::new(&cols, &owner, &idx_schema);
    let mb = batch.as_mem_batch();
    let mut keybuf = PkBuf::empty(0);
    assert!(spec.key_bytes(&mb, 0, &mut keybuf), "both columns present ⇒ indexed");
    assert!(!spec.key_bytes(&mb, 1, &mut keybuf),
        "NULL in the first indexed column ⇒ skipped");
    assert!(!spec.key_bytes(&mb, 2, &mut keybuf),
        "NULL in the second indexed column ⇒ skipped");
}

// ---------------------------------------------------------------------------
// Merge accounting: verdict + all-or-nothing seed
// ---------------------------------------------------------------------------

fn offer_all(acc: &mut PreflightAccumulator, keys: &[PkBuf]) -> bool {
    for &k in keys {
        if !acc.offer(k) { return false; }
    }
    true
}

/// Adjacent equal spans — within one worker's run or as two workers' equal
/// heads, indistinguishable at this layer — flip the verdict; the verdict is
/// monotonic thereafter.
#[test]
fn accumulator_adjacent_equal_is_duplicate() {
    let mut acc = PreflightAccumulator::new(1000);
    assert!(offer_all(&mut acc, &[span_u128(1), span_u128(2), span_u128(3)]));
    assert!(!acc.offer(span_u128(3)), "equal to prev ⇒ duplicate");
    assert!(!acc.offer(span_u128(4)), "verdict is monotonic");
    assert!(acc.duplicate);
}

#[test]
fn accumulator_distinct_keys_no_duplicate() {
    let mut acc = PreflightAccumulator::new(1000);
    let keys: Vec<PkBuf> = [1u128, 2, 3, 100, u128::MAX].into_iter().map(span_u128).collect();
    assert!(offer_all(&mut acc, &keys));
    assert!(!acc.duplicate);
    let expected: FxHashSet<PkBuf> = keys.iter().copied().collect();
    let (seed, capped) = acc.into_seed();
    assert_eq!(seed, expected, "seed under cap is the complete distinct set");
    assert!(!capped, "under-cap seed must not report capped");
}

/// Crossing the cap clears the partial seed whole and never repopulates it:
/// the seed is complete-or-empty, never truncated (a truncated warm filter
/// would prove "absent" for a present key — a uniqueness hole).
#[test]
fn accumulator_seed_over_cap_is_empty_never_truncated() {
    let mut acc = PreflightAccumulator::new(3);
    assert!(offer_all(&mut acc, &[span_u128(10), span_u128(20), span_u128(30)]));
    assert!(acc.offer(span_u128(40)), "cap overflow is not a duplicate");
    assert!(acc.offer(span_u128(50)));
    assert!(!acc.duplicate);
    let (seed, capped) = acc.into_seed();
    assert!(seed.is_empty(), "seed must be cleared whole on cap overflow");
    assert!(capped, "overflow must report capped so the seed publishes a capped filter");
}

#[test]
fn accumulator_seed_at_exactly_cap_is_complete() {
    let mut acc = PreflightAccumulator::new(3);
    assert!(offer_all(&mut acc, &[span_u128(10), span_u128(20), span_u128(30)]));
    let (seed, capped) = acc.into_seed();
    assert_eq!(seed.len(), 3, "exactly cap spans must keep the full seed");
    assert!(!capped, "exactly cap spans must not report capped");
}

/// A duplicate found after the cap has been crossed is still detected — the
/// verdict never depends on the seed.
#[test]
fn accumulator_duplicate_after_cap_crossing() {
    let mut acc = PreflightAccumulator::new(2);
    assert!(offer_all(&mut acc, &[span_u128(1), span_u128(2), span_u128(3), span_u128(4)]));
    assert!(!acc.offer(span_u128(4)));
    assert!(acc.duplicate);
    let (seed, _capped) = acc.into_seed();
    assert!(seed.is_empty());
}

/// `PkBuf` byte order is a valid merge order: byte-lexicographic comparison of
/// the OPK spans equals the order the worker sorts and the master's heap pops,
/// at any width. A composite span sorts by its leading column, then trailing.
#[test]
fn pkbuf_byte_order_is_lexicographic() {
    let span = |a: u64, b: u64| {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_be_bytes());
        buf[8..].copy_from_slice(&b.to_be_bytes());
        PkBuf::from_bytes(&buf)
    };
    let mut v = vec![span(2, 0), span(1, 9), span(1, 1), span(2, 0)];
    v.sort_unstable();
    assert_eq!(v, vec![span(1, 1), span(1, 9), span(2, 0), span(2, 0)],
        "sort is by leading column then trailing — byte-lexicographic");
    // A narrower span sorts before a wider one sharing its prefix (memcmp over
    // bytes[..len]).
    assert!(PkBuf::from_bytes(&[1u8, 2]) < PkBuf::from_bytes(&[1u8, 2, 0]));
}

// ---------------------------------------------------------------------------
// SAL classification
// ---------------------------------------------------------------------------

#[test]
fn unique_preflight_classifies_and_is_unicast() {
    let kind = SalMessageKind::classify(FLAG_UNIQUE_PREFLIGHT);
    assert_eq!(kind, SalMessageKind::UniquePreflight);
    assert!(
        !kind.is_broadcast(),
        "pre-flight is unicast-shaped: per-worker req_id slots, like Scan",
    );
}
