use std::num::NonZeroU64;

use super::*;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupData, GroupTargets, SalReader};
use crate::runtime::wire::WireSchema;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_wire::control::peek_control_block;

/// Every request, each field a value no other field of it holds, so a swapped
/// argument decodes to a different request.
fn every_request() -> Vec<SalRequest<'static>> {
    let cols = PkColList::from_slice(&[3, 1]);
    vec![
        SalRequest::Shutdown,
        Apply::Flush.into(),
        Apply::FlushEph { generation: 11 }.into(),
        Apply::DdlSync { family: 12 }.into(),
        Apply::Backfill { source: 13, view: 14 }.into(),
        Apply::Push { tid: 15 }.into(),
        Apply::Tick {
            first_round: 16,
            tids: [17u64, 18]
                .iter()
                .flat_map(|t| t.to_le_bytes())
                .collect::<Vec<u8>>()
                .into(),
        }
        .into(),
        Read::HasPk { tid: 19, probe: Probe::Pk }.into(),
        Read::HasPk { tid: 20, probe: Probe::PkColumn(21) }.into(),
        Read::HasPk {
            tid: 22,
            probe: Probe::Index(cols, NonZeroU64::new(23).unwrap()),
        }
        .into(),
        Read::KeySpans { tid: 24, cols }.into(),
        Read::ScanSpec {
            tid: 25,
            reply_layout: 26,
            spec: vec![27u8, 28, 29].into(),
        }
        .into(),
        Read::Delta {
            view: 30,
            after_tick: 31,
            cut_round: 32,
            reply_layout: 33,
        }
        .into(),
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
    let relation = WireSchema::encoded(15, batch.schema());
    let family = WireSchema::encoded(12, batch.schema());
    let mut groups: Vec<DirectGroup> = every_request().into_iter().map(DirectGroup::new).collect();
    groups.push(DirectGroup::push(
        &relation,
        GroupData::Same(batch.wire_whole()),
        GroupTargets::UNADDRESSED,
    ));
    groups.push(DirectGroup::ddl_sync(&family, &batch));

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
        (SalMessageKind::DeltaRead, &[0u8; 12][..]),
        (SalMessageKind::DeltaRead, &[][..]),
    ] {
        assert!(
            SalRequest::decode(kind, &hdr, blob).is_err(),
            "{kind:?} over {} bytes",
            blob.len()
        );
    }
}
