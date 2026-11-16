use super::{ANCHOR_BYTES, SalLog};
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::SalReader;
use crate::runtime::sal::fixtures::{group_at, scan, test_log};
use crate::runtime::sal::format::{
    HDR_PREFIX, PREFIX_BYTES, PRESENT, SalMessageKind, epoch_word, group_header_size, payload_rows,
};
use crate::runtime::sal::group::{Group, Routed};
use crate::runtime::w2m::fixtures::id;
use crate::test_support::{make_batch, make_schema_u64_i64, sweep_bit_flips};
use gnitz_store::relation::PkRule;
use gnitz_wire::MAX_WORKERS;
use gnitz_zset::schema::encode_schema_block;
use std::fs::File;

// ---------------------------------------------------------------------------
// Framing: payloads, the directory and the cursor
// ---------------------------------------------------------------------------

/// A per-worker group gives each addressed worker its own bytes at every
/// width — unaddressed workers interleaved, odd sizes padded — and the next
/// group starts where it ends.
#[test]
fn a_per_worker_group_gives_each_addressed_worker_its_own_bytes() {
    for width in [1usize, 2, 3, MAX_WORKERS] {
        let log = test_log(1 << 20, width, 1);
        let bufs: Vec<Vec<u8>> = (0..width)
            .map(|w| if w % 2 == 1 { vec![] } else { vec![w as u8; 97 + w] })
            .collect();
        let payloads: Vec<&[u8]> = bufs.iter().map(Vec::as_slice).collect();
        log.forge(7, true, SalMessageKind::ScanSpec, &payloads);
        log.forge(8, false, SalMessageKind::ScanSpec, &payloads);

        let groups: Vec<_> = log.log().walk(1).collect();
        assert_eq!(groups.len(), 2, "width {width}");
        assert_eq!(groups[1].base, groups[0].end, "width {width}: the stride");
        assert_eq!(groups[1].end, log.cursor(), "width {width}: the writer's advance");
        for (g, header) in groups.iter().zip([(7, true), (8, false)]) {
            assert_eq!(
                (g.head.request.target_id, g.head.zoned(), g.width() as usize),
                (header.0, header.1, width)
            );
            assert_eq!(
                g.payloads().count(),
                width.div_ceil(2),
                "width {width}: one per addressed worker"
            );
            for (w, buf) in bufs.iter().enumerate() {
                let written = (!buf.is_empty()).then_some(buf.as_slice());
                assert_eq!(g.slot(w as u32), written, "width {width}: worker {w}");
            }
            assert_eq!(g.slot(width as u32), None, "width {width}: a worker past it");
        }
    }
}

/// A group whose every addressed worker is sent the same message holds it once:
/// one directory entry, the same bytes for each of them, none for the rest.
#[test]
fn a_shared_group_gives_every_addressed_worker_the_same_bytes() {
    let log = test_log(1 << 20, 4, 1);
    let batch = make_batch(&make_schema_u64_i64(), &[(1, 1, 10), (2, 1, 20)]);
    let record = encode_schema_block(batch.schema());
    let push = |to| Group {
        to,
        ..Group::push(16, PkRule::Enforce, &record, Routed::Same(batch.wire_whole()))
    };
    let mut excl = log.excl();
    excl.write(&push(WorkerSet::ALL)).expect("group fits");
    let second = log.cursor();
    excl.write(&push(WorkerSet::one(1).with(3))).expect("group fits");
    let third = log.cursor();
    excl.write(&push(WorkerSet::EMPTY)).expect("group fits");

    for (base, addressed) in [
        (0, [true; 4]),
        (second, [false, true, false, true]),
        (third, [false; 4]),
    ] {
        let msg = group_at(log.log(), base);
        let payloads: Vec<_> = msg.payloads().collect();
        assert_eq!(payloads.len(), 1, "one directory entry");
        assert_eq!(msg.width(), 4);
        for (w, addressed) in addressed.into_iter().enumerate() {
            assert_eq!(msg.slot(w as u32), addressed.then_some(payloads[0]), "worker {w}");
        }
    }
    assert_eq!(
        third - second,
        second,
        "a group is as long whichever workers share its payload"
    );
}

/// `wal.sal` is never truncated, so a group can land on an offset a wider group
/// used before it. The set it addresses is what makes the leftover directory
/// entries unreachable.
#[test]
fn a_group_exposes_only_its_own_payloads_over_a_wider_groups_leftover() {
    let log = test_log(1 << 20, 8, 1);
    // A group for all eight workers leaves a fully populated directory at offset 0.
    let wide: Vec<Vec<u8>> = (0..8).map(|i| vec![0xA0 + i as u8; 64]).collect();
    let wide_refs: Vec<&[u8]> = wide.iter().map(|b| b.as_slice()).collect();
    log.forge(0, false, SalMessageKind::ScanSpec, &wide_refs);

    // The same offset rewritten by a group for two of them.
    log.excl().checkpoint_reset();
    log.forge(0, false, SalMessageKind::ScanSpec, &[&[0x11u8; 32], &[0x22u8; 32]]);

    let msg = group_at(log.log(), 0);
    assert_eq!(msg.payloads().count(), 2, "the group's own count is the narrow one");
    let lens: Vec<_> = (0..8).map(|w| msg.slot(w).map(<[u8]>::len)).collect();
    assert_eq!(lens, [Some(32), Some(32), None, None, None, None, None, None]);
}

/// A group carrying per-worker blobs sends each worker its own blob in place
/// of the request's.
#[test]
fn a_group_with_per_worker_blobs_sends_each_worker_its_own_blob() {
    let log = test_log(1 << 20, 3, 1);
    let blobs: Vec<Vec<u8>> = vec![vec![1; 40], Vec::new(), vec![3; 400]];
    log.excl()
        .read(scan(16, &[9; 7]), WorkerSet::ALL, Some(&blobs), id(1))
        .expect("group fits");

    let msg = group_at(log.log(), 0);
    assert_eq!(msg.payloads().count(), blobs.len(), "one payload per worker");
    for (w, blob) in blobs.iter().enumerate() {
        let slot = msg.slot(w as u32).expect("every worker is written");
        let (sent, rows) = payload_rows(slot, |_| None).expect("a payload decodes");
        assert_eq!((sent, rows.is_none()), (&blob[..], true), "worker {w}");
    }
}

// ---------------------------------------------------------------------------
// The group header's own integrity: a digest over the header, which names the
// group's byte offset. Every field swept below drives a routing or replay
// decision, so the digest is what stands between a damaged log and a wrong one.
// ---------------------------------------------------------------------------

/// Every scalar field and every directory entry lives inside the digested span;
/// the digest field itself is excluded from it but is what the verdict compares
/// against. One bit anywhere in the header must therefore fail the read — and
/// the live drain, whose prefix test reads none of those bits, still reaches
/// the group, where it fail-stops instead of parking.
#[test]
fn every_single_bit_flip_in_a_group_header_is_rejected() {
    let mut log = test_log(1 << 20, 3, 1);
    let buf = vec![0x5Au8; 64];
    log.forge(42, true, SalMessageKind::ScanSpec, &[&buf, &[], &buf]);

    let view = *log.log();
    let reader = log.reader(0);
    assert!(view.group_at(0, 1).is_some());

    let hdr = log.header_mut(0);
    let hdr_len = hdr.len();
    sweep_bit_flips(hdr, 0..hdr_len, |byte, bit, _| {
        assert!(
            view.group_at(0, 1).is_none(),
            "header byte {byte} bit {bit} must fail the digest"
        );
        assert!(
            !reader.is_empty(),
            "header byte {byte} bit {bit}: the live drain still reaches the group"
        );
    });
}

/// A header names its own byte offset inside the digested span, which makes a
/// valid header self-locating: a header that verified anywhere it was copied to
/// would let a walk read one group's header over another's bytes.
#[test]
fn a_header_only_verifies_at_the_offset_it_was_published_at() {
    let mut log = test_log(1 << 20, 1, 1);
    let buf = vec![0x11u8; 64];
    log.forge(42, false, SalMessageKind::ScanSpec, &[&buf]);
    let second = log.forge(43, false, SalMessageKind::ScanSpec, &[&buf]);

    // Group B's whole header over group A's — same epoch, same shape, a
    // different address.
    let b_hdr = log.header_mut(second).to_vec();
    log.header_mut(0).copy_from_slice(&b_hdr);
    assert!(
        log.log().group_at(0, 1).is_none(),
        "a header published elsewhere must not verify here"
    );
}

/// A view that cannot hold the whole group reads no group in it, wherever the
/// view ends.
#[test]
fn a_probe_never_reads_past_the_end_of_the_mapping() {
    let log = test_log(1 << 20, 1, 1);
    let base = log.forge(0, false, SalMessageKind::ScanSpec, &[&[0u8; 8]]);
    let end = group_at(log.log(), base).end as usize;
    let reads_in = |len: usize| SalLog { ring_len: len, ..*log.log() }.group_at(0, 1).is_some();

    let header = PREFIX_BYTES + group_header_size(1);
    for (view, len) in [
        ("the prefix", PREFIX_BYTES - 1),
        ("the fixed part", PREFIX_BYTES + HDR_PREFIX - 1),
        ("the directory", header - 1),
        ("the payload", end - 1),
    ] {
        assert!(!reads_in(len), "{view}");
    }
    assert!(reads_in(end), "the whole group");
}

/// A zone member at the top epoch and a nonzero base reads back where it was
/// written, each addressed worker's payload in directory order, and intact
/// until a payload byte changes; the prefix's copy of the top epoch passes the
/// live drain's test.
#[test]
fn a_zone_member_is_intact_until_a_payload_byte_changes() {
    let mut log = test_log(1 << 20, 3, u32::MAX);
    log.forge(1, false, SalMessageKind::ScanSpec, &[&[0u8; 8]]);
    let base = log.forge(2, true, SalMessageKind::ScanSpec, &[&[1u8; 8], &[], &[2u8; 21]]);
    assert!(base > 0);

    let msg = group_at(log.log(), base);
    assert_eq!((msg.base, msg.end), (base, log.cursor()));
    assert_eq!(msg.payloads().map(<[u8]>::len).collect::<Vec<_>>(), [8, 21]);
    assert!(msg.intact(), "a zone member's payload checksum verifies");
    assert!(
        log.log().published_at(base, u32::MAX),
        "the prefix's epoch copy passes the live gate"
    );

    let payload_at = base as usize + PREFIX_BYTES + group_header_size(2);
    log.ring_mut(payload_at..payload_at + 1)[0] ^= 1;
    assert!(!group_at(log.log(), base).intact(), "a flipped payload byte fails it");
}

/// The prefix gate compares an unauthenticated copy of the epoch, so a leftover
/// whose prefix epoch flipped up to the current one passes it with an entirely
/// intact older header. A prefix the master published carries its header's
/// epoch, so the mismatch is damage.
#[test]
fn a_raised_prefix_epoch_over_an_older_header_passes_the_gate_and_does_not_verify() {
    let mut log = test_log(1 << 20, 1, 1);
    log.forge(7, false, SalMessageKind::ScanSpec, &[&[0u8; 32]]);
    // Raise only the prefix's epoch copy: 1 -> 2, header untouched.
    log.prefix_mut(0).copy_from_slice(&epoch_word(2, PRESENT).to_ne_bytes());
    let reader = SalReader::new(*log.log(), 0, 2);
    assert!(!reader.is_empty(), "the raised prefix passes the gate");
    assert!(
        log.log().group_at(0, 2).is_none(),
        "and the older header behind it is damage"
    );
}

/// The file reaches `len` before the mapping exists, so the store lands on a
/// real page instead of raising SIGBUS; a remap keeps what the file holds, and a
/// smaller one neither shrinks it nor maps less of it.
#[test]
fn mapping_extends_the_file_and_never_shrinks_it() {
    let file: &'static File = Box::leak(Box::new(tempfile::tempfile().unwrap()));
    let len = || file.metadata().unwrap().len();
    let log = SalLog::map(file, ANCHOR_BYTES + 8192).unwrap();
    assert_eq!(len(), (ANCHOR_BYTES + 8192) as u64);
    unsafe { log.ring.add(4096).write(42) };
    let log = SalLog::map(file, ANCHOR_BYTES + 4097).unwrap();
    assert_eq!(len(), (ANCHOR_BYTES + 8192) as u64);
    assert_eq!(log.ring_len, 8192, "the whole file is mapped");
    assert_eq!(unsafe { log.ring.add(4096).read() }, 42);
}
