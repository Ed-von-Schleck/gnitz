use gnitz_store::relation::PkRule;
use std::num::NonZeroU64;

use super::*;
use crate::catalog::SysFamily;
use crate::runtime::sal::fixtures::test_log;
use crate::runtime::sal::{GroupData, GroupTargets, SalGroup, SalReader};
use crate::test_support::{make_batch, make_schema_u64_i64, schema_tab_batch};
use gnitz_wire::peek_control_block;
use gnitz_zset::schema::encode_schema_block;

/// Every request, each field a value no other field of it holds, so a swapped
/// argument decodes to a different request.
fn every_request() -> Vec<SalRequest<'static>> {
    let cols = PkColList::from_slice(&[3, 1]);
    vec![
        SalRequest::Shutdown,
        Apply::Flush.into(),
        Apply::FlushEph { generation: 11 }.into(),
        Apply::DdlSync { family: SysFamily::Sequence }.into(),
        Apply::Backfill { views: vec![13, 14].into() }.into(),
        Apply::Push { tid: 15, rule: PkRule::Enforce }.into(),
        Apply::Push { tid: 50, rule: PkRule::Fresh }.into(),
        Apply::Tick {
            first_round: 16,
            tids: vec![17, 18].into(),
        }
        .into(),
        Read::HasPk {
            tid: 19,
            probe: Probe::Pk,
            keys: vec![38u8, 39].into(),
        }
        .into(),
        Read::HasPk {
            tid: 20,
            probe: Probe::PkColumn(21),
            keys: vec![40u8].into(),
        }
        .into(),
        Read::HasPk {
            tid: 22,
            probe: Probe::Index(cols, NonZeroU64::new(23).unwrap()),
            keys: vec![41u8, 42, 43].into(),
        }
        .into(),
        Read::KeySpans { tid: 24, cols, claim: false }.into(),
        Read::KeySpans { tid: 37, cols, claim: true }.into(),
        Read::ReleasePreflight.into(),
        Read::ScanSpec {
            tid: 25,
            reply_layout: 26,
            spec: vec![27u8, 28, 29].into(),
        }
        .into(),
        Read::Delta {
            view: 30,
            after_tick: 31,
            reply_layout: 36,
            spec: vec![33u8, 34, 35].into(),
        }
        .into(),
        Read::Pin { snapshot: 44 }.into(),
        Read::Unpin { snapshot: 45 }.into(),
        Read::SnapshotPage {
            snapshot: 46,
            tid: 47,
            page: vec![48u8, 49].into(),
        }
        .into(),
        Read::Backup { dir: b"/backup".to_vec().into() }.into(),
    ]
}

/// The head a worker decodes is the one `lay_out` wrote: every request through
/// a written group, the two that carry rows through their own constructors.
#[test]
fn every_request_decodes_from_its_written_group() {
    let batch = make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]);
    let record = encode_schema_block(batch.schema());
    let mut groups: Vec<SalGroup> = every_request().into_iter().map(SalGroup::new).collect();
    groups.push(SalGroup::push(
        15,
        PkRule::Fresh,
        &record,
        GroupData::Same(batch.wire_whole()),
        GroupTargets::SILENT,
    ));
    let schemas = schema_tab_batch(&[(100, "s", 1)]);
    groups.push(SalGroup::ddl_sync(SysFamily::Schema, &schemas));
    let mut kinds: Vec<u8> = groups.iter().map(|g| g.request.encode().0.as_wire()).collect();
    kinds.sort_unstable();
    kinds.dedup();
    assert_eq!(kinds.len(), SalMessageKind::ALL.len(), "every kind is written");

    for group in groups {
        // A fresh log each: a flush moves a reader to the next epoch.
        let log = test_log(1 << 20, 2, 1);
        log.excl().write(&group).expect("group fits");
        let (msg, slot) = SalReader::new(log.log(), 1, 1)
            .next()
            .expect("the group addresses the worker");
        let control = peek_control_block(slot).expect("a control block");
        let decoded = SalRequest::decode(
            msg.head.kind,
            msg.head.target_id,
            &control.hdr,
            &slot[control.blob.clone()],
        );
        assert_eq!(decoded.as_ref(), Ok(&group.request));
        let (kind, target, _) = group.request.encode();
        assert_eq!(msg.head.kind, kind);
        assert_eq!(msg.head.target_id, target);
        assert_eq!(control.hdr.target_id, 0, "a payload carries no target");
        assert_eq!(control.data.is_some(), group.schema.is_some(), "{:?}", group.request);
    }
}

#[test]
fn a_blob_of_the_wrong_width_is_refused() {
    let hdr = ControlHeader::default();
    assert!(SalRequest::decode(SalMessageKind::Tick, 0, &hdr, &[0u8; 12]).is_err());
}

/// A `Push` group's rule word is 0 or 1.
#[test]
fn a_push_rule_no_group_writes_is_refused() {
    let hdr = ControlHeader { arg0: 2, ..Default::default() };
    assert!(SalRequest::decode(SalMessageKind::Push, 15, &hdr, &[]).is_err());
}

/// A `HasPk` group's arguments must be the ones its probe kind writes.
#[test]
fn a_probe_no_kind_writes_is_refused() {
    let index = PkColList::from_slice(&[2, 5]).pack();
    for (kind, arg0, arg1) in [
        (SalMessageKind::HasPk, 5, 0),
        (SalMessageKind::HasPk, 0, index),
        (SalMessageKind::HasPkColumn, 1 << 32, 0),
        (SalMessageKind::HasPkColumn, 4, index),
        (SalMessageKind::HasPkIndex, 0, index),
        (SalMessageKind::HasPkIndex, 9, 0),
        (SalMessageKind::HasPkIndex, 9, 7),
    ] {
        let hdr = ControlHeader { arg0, arg1, ..Default::default() };
        assert!(
            SalRequest::decode(kind, 0, &hdr, &[]).is_err(),
            "{kind:?} {arg0} {arg1:#x}"
        );
    }
}

/// A decoded read borrows the bytes it came in; `into_owned` copies them, and the
/// copy outlives those bytes.
#[test]
fn a_read_borrows_its_blob_until_owned() {
    let hdr = ControlHeader { arg0: 7, ..Default::default() };
    let owned = {
        let blob = vec![1u8, 2, 3];
        let SalRequest::Read(read) = SalRequest::decode(SalMessageKind::ScanSpec, 5, &hdr, &blob).unwrap() else {
            panic!("a scan is a read")
        };
        let Read::ScanSpec { spec: Cow::Borrowed(spec), .. } = &read else {
            panic!("a decoded spec borrows its blob")
        };
        assert_eq!(spec.as_ptr(), blob.as_ptr());
        read.into_owned()
    };
    assert!(matches!(
        *owned,
        Read::ScanSpec { tid: 5, reply_layout: 7, spec: Cow::Owned(ref s) } if s == &[1, 2, 3]
    ));
}

#[test]
fn into_owned_keeps_every_read() {
    for request in every_request() {
        let SalRequest::Read(read) = request else { continue };
        let expected = format!("{read:?}");
        assert_eq!(format!("{:?}", *read.into_owned()), expected);
    }
}
