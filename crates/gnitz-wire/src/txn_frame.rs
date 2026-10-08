//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! A frame is a prologue header naming the verb (`target_id = 0`), then items to
//! the end of the frame, each a control frame of that verb. A `DELTA_POLL`'s
//! prologue `arg0` is [`delta_poll_kept`]; every other prologue field is zero.
//!
//! | Verb         | Item header                                              | Sections                  |
//! |--------------|----------------------------------------------------------|---------------------------|
//! | `DDL_TXN`    | `target_id` = system family                              | data block                |
//! | `PUSH_TXN`   | `target_id`; `flags.conflict_mode`; `arg0` = basis; `arg1` = descriptor token | schema record, data block |
//! | `SCAN_MULTI` | `target_id`; `arg0` = reply layout digest                | none                      |
//! | `DELTA_POLL` | `target_id` = view (≠ 0); `arg0` = reply layout digest; `arg1` = descriptor token | blob = cursor tag, after_tick, `ReadSpec` |
//!
//! **A reply fault's `target_id`:** a `DELTA_POLL` fault naming one of its views
//! ends that position alone; every other fault ends the request.

use crate::control::{
    append_frame, frame_size, peek_control_block, ControlHeader, DecodedControl, Target, CTRL_HEADER_SIZE,
};
use crate::region::Regions;
use crate::{ClientVerb, WireFlags, WireStatus};
use std::num::NonZeroU64;
use std::ops::Range;

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
    /// Empty for none; only a verb whose items may carry one decodes with it.
    pub blob: &'a [u8],
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
            .map(|it| frame_size(it.blob, it.schema, it.data.as_deref()))
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
        append_frame(&mut out, &hdr, it.blob, it.schema, it.data.as_deref());
    }
    debug_assert_eq!(out.len(), size);
    out
}

/// The sections an item of a multi-item verb carries, and how many items one
/// frame may hold.
#[derive(Clone, Copy)]
pub(crate) struct ItemShape {
    /// Whether an item may carry a blob.
    pub(crate) blob: bool,
    pub(crate) schema: bool,
    pub(crate) data: bool,
    pub(crate) cap: usize,
}

/// A multi-item verb's item shape; `None` for a single-item verb.
pub(crate) const fn item_shape(verb: ClientVerb) -> Option<ItemShape> {
    let (blob, schema, data, cap) = match verb {
        ClientVerb::DdlTxn => (false, false, true, usize::MAX),
        ClientVerb::PushTxn => (false, true, true, usize::MAX),
        ClientVerb::ScanMulti => (false, false, false, SCAN_MULTI_MAX_RELATIONS),
        ClientVerb::DeltaPoll => (true, false, false, usize::MAX),
        _ => return None,
    };
    Some(ItemShape { blob, schema, data, cap })
}

/// Split a multi-item `verb` frame's body into its items, in send order: each
/// item's own frame bytes and its peeked control. An item's `body` is the empty
/// range at its end.
pub fn decode_items(body: &[u8], verb: ClientVerb) -> Result<Vec<(&[u8], DecodedControl)>, String> {
    let Some(ItemShape { blob, schema, data, cap }) = item_shape(verb) else {
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
        if !ctrl.blob.is_empty() && !blob {
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
            blob: &[],
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

/// A delta-feed position, held by the client alone: the boot, relation and
/// spec its rounds belong to (`tag`), and the last round of the view it covers
/// (`tick`).
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct DeltaCursor {
    pub tag: u64,
    pub tick: NonZeroU64,
}

impl DeltaCursor {
    /// The cursor a flat `(tag, tick)` pair spells; tick 0 spells none.
    pub fn from_pair(tag: u64, tick: u64) -> Option<DeltaCursor> {
        NonZeroU64::new(tick).map(|tick| DeltaCursor { tag, tick })
    }

    /// This cursor as a flat `(tag, tick)` pair.
    pub fn pair(self) -> (u64, u64) {
        (self.tag, self.tick.get())
    }
}

/// One item of a DELTA_POLL: `spec` applied to every delta `view` recorded
/// after `from`, replied in the layout whose digest is `reply_layout`, which is
/// the spec's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaPollItem<'a> {
    /// The view, and the token of the RESOLVE answer this read was built from.
    pub view: Target,
    /// The cursor read from; `None` reads the view whole.
    pub from: Option<DeltaCursor>,
    pub reply_layout: u64,
    /// An encoded `ReadSpec` forwarding rows with no cut; `ReadSpec::all_rows`
    /// of no bound for the view's own rows. The worker's decode is the trust
    /// boundary.
    pub spec: &'a [u8],
}

/// The bytes an item's blob opens with: the cursor's tag, then its tick, both
/// zero for none.
const CURSOR_SIZE: usize = 16;

/// Encode a `DELTA_POLL` frame, without the 4-byte frame length prefix. Item
/// `i` is kept as the subscription `keep + i`.
pub fn encode_delta_poll(views: &[DeltaPollItem<'_>], keep: Option<u64>) -> Vec<u8> {
    let mut blobs = Vec::with_capacity(views.iter().map(|v| CURSOR_SIZE + v.spec.len()).sum());
    let ranges: Vec<Range<usize>> = views
        .iter()
        .map(|v| {
            let start = blobs.len();
            let (tag, tick) = v.from.map_or((0, 0), DeltaCursor::pair);
            blobs.extend_from_slice(&tag.to_le_bytes());
            blobs.extend_from_slice(&tick.to_le_bytes());
            blobs.extend_from_slice(v.spec);
            start..blobs.len()
        })
        .collect();
    let items: Vec<FrameItem> = views
        .iter()
        .zip(ranges)
        .map(|(v, blob)| FrameItem {
            hdr: ControlHeader::naming(ClientVerb::DeltaPoll, v.view, v.reply_layout),
            blob: &blobs[blob],
            schema: None,
            data: None,
        })
        .collect();
    encode_items_under(ClientVerb::DeltaPoll, keep.unwrap_or(0), &items)
}

/// The subscription id item 0 of the `DELTA_POLL` under `prologue` is kept
/// as, the rest counting up, wrapping.
pub fn delta_poll_kept(prologue: &ControlHeader) -> Option<impl Iterator<Item = u64>> {
    let first = prologue.arg0;
    (first != 0).then(|| (0u64..).map(move |i| first.wrapping_add(i)))
}

/// Decode the items of a `DELTA_POLL` frame. View id `0` is refused: it is the
/// id of a fault ending the request.
pub fn decode_delta_items(body: &[u8]) -> Result<Vec<DeltaPollItem<'_>>, String> {
    decode_items(body, ClientVerb::DeltaPoll)?
        .into_iter()
        .map(|(item, c)| {
            if c.hdr.target_id == 0 {
                return Err("DeltaPoll: view id 0 names no view".to_string());
            }
            let Some((cursor, spec)) = item[c.blob.clone()].split_first_chunk::<CURSOR_SIZE>() else {
                return Err(format!("DeltaPoll: view {} carries no cursor", c.hdr.target_id));
            };
            let (tag, tick) = cursor.split_at(8);
            Ok(DeltaPollItem {
                view: c.hdr.target(),
                from: DeltaCursor::from_pair(
                    u64::from_le_bytes(tag.try_into().unwrap()),
                    u64::from_le_bytes(tick.try_into().unwrap()),
                ),
                reply_layout: c.hdr.arg0,
                spec,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "tests/txn_frame.rs"]
mod tests;
