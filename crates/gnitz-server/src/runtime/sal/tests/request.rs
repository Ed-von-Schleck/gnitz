use gnitz_store::relation::PkRule;
use std::num::NonZeroU64;

use super::*;
use crate::catalog::SysFamily;
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::fixtures::{group_at, test_log};
use crate::runtime::sal::format::payload_rows;
use crate::runtime::sal::group::{Group, Payloads, Routed};
use crate::runtime::w2m::fixtures::id;
use crate::test_support::encode_schema_record;
use crate::test_support::{certified, make_batch, make_schema_u64_i64, schema_tab_batch};

/// Every request, each field a value no other field of it holds, so a swapped
/// argument decodes to a different request.
fn every_request() -> Vec<SalRequest<'static>> {
    let cols = PkColList::from_slice(&[3, 1]);
    vec![
        Apply::Flush.into(),
        Apply::FlushEph { generation: 11 }.into(),
        Ingest::DdlSync { family: SysFamily::Schema }.into(),
        Apply::Backfill { views: vec![13, 14].into() }.into(),
        Ingest::Push { tid: 15, rule: PkRule::Enforce }.into(),
        Ingest::Push { tid: 50, rule: PkRule::Fresh }.into(),
        Apply::Tick {
            first_round: 16,
            tids: vec![17, 18].into(),
        }
        .into(),
        Read::HasPk {
            tid: 19,
            probe: Probe::Pk,
            keys: &[38, 39],
        }
        .into(),
        Read::HasPk {
            tid: 20,
            probe: Probe::PkColumn(21),
            keys: &[40],
        }
        .into(),
        Read::HasPk {
            tid: 22,
            probe: Probe::Index(cols, NonZeroU64::new(23).unwrap()),
            keys: &[41, 42, 43],
        }
        .into(),
        Read::KeySpans { tid: 24, cols, claim: false }.into(),
        Read::KeySpans { tid: 37, cols, claim: true }.into(),
        Read::ReleasePreflight.into(),
        Read::ScanSpec {
            tid: 25,
            reply_layout: 26,
            spec: &[27, 28, 29],
        }
        .into(),
        Read::Delta {
            view: 30,
            after_tick: 31,
            reply_layout: 36,
            spec: &[33, 34, 35],
        }
        .into(),
        Read::Pin { snapshot: 44 }.into(),
        Read::Unpin { snapshot: 45 }.into(),
        Read::SnapshotPage { snapshot: 46, tid: 47, page: &[48, 49] }.into(),
        Read::Backup { dir: "/backup" }.into(),
    ]
}

/// The request a worker decodes is the one `lay_out` wrote: every request through
/// a written group, the two that carry rows through their own constructors.
#[test]
fn every_request_decodes_from_its_written_group() {
    let batch = certified(make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]));
    let record = encode_schema_record(batch.schema());
    let requests = every_request();
    let schemas = schema_tab_batch(&[(100, "s", 1)]);
    let groups: Vec<(&SalRequest, Group)> = (requests.iter())
        .map(|request| {
            let group = match *request {
                SalRequest::Read(ref read) => Group::read(read, WorkerSet::ALL, None, id(1)),
                SalRequest::Apply(ref apply) => Group::apply(apply),
                SalRequest::Ingest(Ingest::Push { tid, rule }) => {
                    Group::push(tid, rule, &record, Routed::Same(batch.wire_whole()))
                }
                SalRequest::Ingest(Ingest::DdlSync { family }) => Group::ddl_sync(family, &schemas),
            };
            (request, group)
        })
        .collect();
    let mut kinds: Vec<u8> = groups.iter().map(|(_, g)| g.request.kind.as_wire()).collect();
    kinds.sort_unstable();
    kinds.dedup();
    assert_eq!(kinds.len(), SalMessageKind::ALL.len(), "every kind is written");

    for (request, group) in groups {
        let log = test_log(1 << 20, 2, 1);
        log.excl().write(&group).expect("group fits");
        let msg = group_at(log.log(), 0);
        let slot = msg.slot(1).expect("the group addresses the worker");
        let (blob, rows) = payload_rows(slot, |_| None).expect("a payload");
        let decoded = SalRequest::decode(&msg.head.request, blob);
        assert_eq!(decoded.as_ref(), Ok(request));
        assert_eq!(msg.head.request, group.request);
        let carries_rows = matches!(group.data, Payloads::Shared(Some(_)) | Payloads::EachRows(_));
        assert_eq!(rows.is_some(), carries_rows, "{request:?}");
    }
}

#[test]
fn a_blob_of_the_wrong_width_is_refused() {
    let head = RequestHead::new(SalMessageKind::Tick, 0, 0, 0);
    assert!(SalRequest::decode(&head, &[0u8; 12]).is_err());
}

/// A `Push` group's rule word is 0 or 1.
#[test]
fn a_push_rule_no_group_writes_is_refused() {
    let head = RequestHead::new(SalMessageKind::Push, 15, 2, 0);
    assert!(SalRequest::decode(&head, &[]).is_err());
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
        assert!(
            SalRequest::decode(&RequestHead::new(kind, 0, arg0, arg1), &[]).is_err(),
            "{kind:?} {arg0} {arg1:#x}"
        );
    }
}

/// A kept read is the read it was kept from, once the blob that read borrowed
/// is gone.
#[test]
fn a_kept_read_outlives_the_blob_it_was_decoded_from() {
    for request in every_request() {
        let SalRequest::Read(read) = request else { continue };
        let (head, blob) = read.encode();
        let kept = {
            let blob = blob.to_vec();
            let SalRequest::Read(decoded) = SalRequest::decode(&head, &blob).unwrap() else {
                panic!("{read:?} decodes to a read")
            };
            decoded.keep()
        };
        assert_eq!(kept.read(), read);
    }
}
