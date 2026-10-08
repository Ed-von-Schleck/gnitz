use std::num::NonZeroU64;

use super::*;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupData, GroupTargets, SalReader};
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_wire::control::peek_control_block;
use gnitz_zset::schema::encode_schema_block;

/// Every request, each field a value no other field of it holds, so a swapped
/// argument decodes to a different request.
fn every_request() -> Vec<SalRequest<'static>> {
    let cols = PkColList::from_slice(&[3, 1]);
    vec![
        SalRequest::Shutdown,
        Apply::Flush.into(),
        Apply::FlushEph { generation: 11 }.into(),
        Apply::DdlSync { family: 12 }.into(),
        Apply::Backfill { views: vec![13, 14].into() }.into(),
        Apply::Push { tid: 15 }.into(),
        Apply::Tick {
            first_round: 16,
            tids: vec![17, 18].into(),
        }
        .into(),
        Read::HasPk { tid: 19, probe: Probe::Pk }.into(),
        Read::HasPk { tid: 20, probe: Probe::PkColumn(21) }.into(),
        Read::HasPk {
            tid: 22,
            probe: Probe::IndexAll(cols, NonZeroU64::new(23).unwrap()),
        }
        .into(),
        Read::HasPk { tid: 37, probe: Probe::Index(cols) }.into(),
        Read::KeySpans { tid: 24, cols }.into(),
        Read::ScanSpec {
            tid: 25,
            reply_layout: 26,
            spec: vec![27u8, 28, 29].into(),
        }
        .into(),
        Read::delta(30, 31, 32, &[33, 34, 35], 36).into(),
    ]
}

fn head_of(msg: &WireMsg<'_>) -> ControlHeader {
    ControlHeader {
        status: msg.status,
        target_id: msg.target_id,
        flags: msg.flags,
        arg0: msg.arg0,
        arg1: msg.arg1,
    }
}

#[test]
fn every_request_decodes_from_its_own_template() {
    for request in every_request() {
        let template = request.template();
        let decoded = SalRequest::decode(request.kind(), &head_of(&template), template.blob);
        assert_eq!(decoded.as_ref(), Ok(&request));
    }
}

/// The head a worker decodes is the one `lay_out` wrote: every request through
/// a written group, the two that carry rows through their own constructors.
#[test]
fn every_request_decodes_from_its_written_group() {
    let batch = make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]);
    let record = encode_schema_block(batch.schema());
    let mut groups: Vec<DirectGroup> = every_request().into_iter().map(DirectGroup::new).collect();
    groups.push(DirectGroup::push(
        15,
        &record,
        GroupData::Same(batch.wire_whole()),
        GroupTargets::UNADDRESSED,
    ));
    groups.push(DirectGroup::ddl_sync(12, &record, &batch));

    for group in groups {
        // A fresh log each: a flush moves a reader to the next epoch.
        let log = TestLog::new(1 << 20, 2, 1);
        log.excl().write(&group).expect("group fits");
        let (msg, slot) = SalReader::new(log.log(), 1, 1)
            .next()
            .expect("the group addresses the worker");
        let control = peek_control_block(slot).expect("a control block");
        let decoded = SalRequest::decode(msg.kind, &control.hdr, &slot[control.blob.clone()]);
        assert_eq!(decoded.as_ref(), Ok(&group.request));
        assert_eq!(msg.kind, group.request.kind());
        assert_eq!(msg.target_id, group.request.template().target_id);
        assert_eq!(control.data.is_some(), group.schema.is_some(), "{:?}", group.request);
    }
}

#[test]
fn a_blob_of_the_wrong_width_is_refused() {
    let hdr = ControlHeader::default();
    for (kind, blob) in [
        (SalMessageKind::Tick, &[0u8; 12][..]),
        (SalMessageKind::DeltaRead, &[0u8; 7][..]),
        (SalMessageKind::DeltaRead, &[][..]),
    ] {
        assert!(
            SalRequest::decode(kind, &hdr, blob).is_err(),
            "{kind:?} over {} bytes",
            blob.len()
        );
    }
}
