//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! A frame is a prologue header naming the verb (`target_id = 0`), then items to
//! the end of the frame, each a control frame of that verb with no blob. A
//! `DELTA_POLL` prologue's `arg0` is the poll's wait in milliseconds; every other
//! prologue field is zero.
//!
//! | Verb         | Item header                                              | Sections                  |
//! |--------------|----------------------------------------------------------|---------------------------|
//! | `DDL_TXN`    | `target_id` = system family                              | data block                |
//! | `PUSH_TXN`   | `target_id`; `flags.conflict_mode`; `arg0` = basis; `arg1` = descriptor token | schema record, data block |
//! | `SCAN_MULTI` | `target_id`; `arg0` = reply layout digest                | none                      |
//! | `DELTA_POLL` | `target_id` = view (≠ 0); `arg0` = reply layout digest; `arg1` = after_tick | none   |
//!
//! **A reply fault's `target_id`:** a `DELTA_POLL` fault naming one of its views
//! ends that position alone; every other fault ends the request.

use crate::control::{append_frame, frame_size, peek_control_block, ControlHeader, DecodedControl, CTRL_HEADER_SIZE};
use crate::region::Regions;
use crate::{ClientVerb, WireFlags, WireStatus};

/// Maximum relations in one `SCAN_MULTI`. The master holds one scan lease and
/// one reply train of bookkeeping per relation; a handful of related tables
/// covers a realistic consistent snapshot.
pub(crate) const SCAN_MULTI_MAX_RELATIONS: usize = 16;

/// The basis of a `PUSH_TXN` family built from no read. No read reports it, and
/// no commit exceeds it.
pub const BLIND: u64 = u64::MAX;

/// One item to encode: its header (the verb is the frame's) and its sections.
pub struct FrameItem<'a> {
    pub hdr: ControlHeader,
    pub schema: Option<&'a [u8]>,
    pub data: Option<Regions<'a>>,
}

/// Encode a multi-item `verb` frame, without the 4-byte frame length prefix.
pub fn encode_items(verb: ClientVerb, items: &[FrameItem<'_>]) -> Vec<u8> {
    encode_items_under(verb, 0, items)
}

/// [`encode_items`] with the prologue's `arg0`.
fn encode_items_under(verb: ClientVerb, arg0: u64, items: &[FrameItem<'_>]) -> Vec<u8> {
    let size = CTRL_HEADER_SIZE
        + items
            .iter()
            .map(|it| frame_size(&[], it.schema, it.data.as_deref()))
            .sum::<usize>();
    let mut out = Vec::with_capacity(size);
    let prologue = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        arg0,
        ..Default::default()
    };
    append_frame(&mut out, &prologue, &[], None, None);
    for it in items {
        let mut hdr = it.hdr;
        hdr.flags.verb = verb;
        append_frame(&mut out, &hdr, &[], it.schema, it.data.as_deref());
    }
    debug_assert_eq!(out.len(), size);
    out
}

/// A multi-item verb's item shape — whether each item carries a schema record
/// and a data block — and its item cap; `None` for a single-item verb.
pub(crate) const fn item_shape(verb: ClientVerb) -> Option<(bool, bool, usize)> {
    match verb {
        ClientVerb::DdlTxn => Some((false, true, usize::MAX)),
        ClientVerb::PushTxn => Some((true, true, usize::MAX)),
        ClientVerb::ScanMulti => Some((false, false, SCAN_MULTI_MAX_RELATIONS)),
        ClientVerb::DeltaPoll => Some((false, false, usize::MAX)),
        _ => None,
    }
}

/// Split a multi-item `verb` frame's body into its items, in send order: each
/// item's own frame bytes and its peeked control. An item's `body` is the empty
/// range at its end.
pub fn decode_items(body: &[u8], verb: ClientVerb) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    let Some((schema, data, cap)) = item_shape(verb) else {
        return Err(format!("{verb:?} is not a multi-item verb"));
    };
    let item = |rest: &[u8]| -> Result<DecodedControl, String> {
        let ctrl = peek_control_block(rest)?;
        if ctrl.hdr.status != WireStatus::Ok {
            return Err(format!("an item carries status {:?}", ctrl.hdr.status));
        }
        if ctrl.hdr.flags.verb != verb {
            return Err(format!("an item names verb {:?}", ctrl.hdr.flags.verb));
        }
        if !ctrl.blob.is_empty() {
            return Err("an item carries a blob".into());
        }
        if ctrl.schema.is_some() != schema {
            return Err(format!(
                "an item {} a schema record",
                if schema { "lacks" } else { "carries" }
            ));
        }
        if ctrl.data.is_some() != data {
            return Err(format!(
                "an item {} a data block",
                if data { "lacks" } else { "carries" }
            ));
        }
        Ok(ctrl)
    };
    if body.is_empty() {
        return Err(format!("{verb:?}: empty item list"));
    }
    let mut rest = body;
    let mut out = Vec::new();
    while !rest.is_empty() {
        if out.len() == cap {
            return Err(format!("{verb:?}: too many items (max {cap})"));
        }
        let mut ctrl = item(rest).map_err(|e| format!("{verb:?} item {}: {e}", out.len()))?;
        let end = ctrl.body.start;
        ctrl.body = end..end;
        out.push((&rest[..end], ctrl));
        rest = &rest[end..];
    }
    Ok(out)
}

/// One SCAN_MULTI item: every row of `tid`, replied in the layout whose digest
/// is `reply_layout`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanMultiItem {
    pub tid: u64,
    pub reply_layout: u64,
}

/// Encode a `SCAN_MULTI` frame, without the 4-byte frame length prefix.
pub fn encode_scan_multi(relations: &[ScanMultiItem]) -> Vec<u8> {
    let items: Vec<FrameItem> = relations
        .iter()
        .map(|r| FrameItem {
            hdr: ControlHeader {
                target_id: r.tid,
                arg0: r.reply_layout,
                ..Default::default()
            },
            schema: None,
            data: None,
        })
        .collect();
    encode_items(ClientVerb::ScanMulti, &items)
}

/// Decode a `SCAN_MULTI` frame body into its items, in request order.
pub fn decode_scan_multi(body: &[u8]) -> Result<Vec<ScanMultiItem>, String> {
    Ok(decode_items(body, ClientVerb::ScanMulti)?
        .into_iter()
        .map(|(_, c)| ScanMultiItem {
            tid: c.hdr.target_id,
            reply_layout: c.hdr.arg0,
        })
        .collect())
}

/// One DELTA_POLL item: every delta `view_id` recorded after round `after_tick`
/// (`0` = the whole view), replied in the layout whose digest is `reply_layout`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaPollItem {
    pub view_id: u64,
    pub after_tick: u64,
    pub reply_layout: u64,
}

/// Encode a `DELTA_POLL` frame, without the 4-byte frame length prefix. The
/// server may hold its reply `wait_ms` while no view has anything new.
pub fn encode_delta_poll(views: &[DeltaPollItem], wait_ms: u64) -> Vec<u8> {
    let items: Vec<FrameItem> = views
        .iter()
        .map(|v| FrameItem {
            hdr: ControlHeader {
                target_id: v.view_id,
                arg0: v.reply_layout,
                arg1: v.after_tick,
                ..Default::default()
            },
            schema: None,
            data: None,
        })
        .collect();
    encode_items_under(ClientVerb::DeltaPoll, wait_ms, &items)
}

/// Decode a `DELTA_POLL` frame into its wait in milliseconds and its items.
/// View id `0` is refused: it is the id of a fault ending the request.
pub fn decode_delta_poll(prologue: &ControlHeader, body: &[u8]) -> Result<(u64, Vec<DeltaPollItem>), String> {
    let views: Result<Vec<DeltaPollItem>, String> = decode_items(body, ClientVerb::DeltaPoll)?
        .into_iter()
        .map(|(_, c)| match c.hdr.target_id {
            0 => Err("DeltaPoll: view id 0 names no view".to_string()),
            view_id => Ok(DeltaPollItem {
                view_id,
                after_tick: c.hdr.arg1,
                reply_layout: c.hdr.arg0,
            }),
        })
        .collect();
    Ok((prologue.arg0, views?))
}

#[cfg(test)]
#[path = "tests/txn_frame.rs"]
mod tests;
