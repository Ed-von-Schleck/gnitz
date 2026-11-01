//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! A frame is a prologue header naming the verb (`target_id = 0`), then items to
//! the end of the frame, each a control frame of that verb. A `DELTA_POLL`'s
//! prologue `arg0` is [`delta_poll_kept`]; every other prologue field is zero,
//! which [`DecodedControl::client_verb`] checks.
//!
//! | Verb         | Item header                                              | Sections                  |
//! |--------------|----------------------------------------------------------|---------------------------|
//! | `DDL_TXN`    | `target_id` = system family                              | data block                |
//! | `PUSH_TXN`   | `target_id`; `flags.conflict_mode`; `arg0` = basis; `arg1` = descriptor token | schema record, data block |
//! | `SCAN_MULTI` | `target_id`; `arg0` = reply layout digest; `arg1` = descriptor token | blob = `ReadSpec` |
//! | `DELTA_POLL` | `target_id` = view (≠ 0); `arg0` = reply layout digest; `arg1` = descriptor token | blob = cursor tag, after_tick, `ReadSpec` |
//!
//! **A reply fault's `target_id`:** a `DELTA_POLL` fault naming one of its views
//! ends that position alone; every other fault ends the request.
//!
//! **A `DELTA_POLL` reply** is one train per view, ended by a terminal whose
//! `target_id` is the view, `arg0` the tick of the [`DeltaCursor`] to read from
//! next and `arg1` its tag. Tick 0 is never sent.
//!
//! The module also holds the [`DeltaCursor`] a `DELTA_POLL` item and terminal
//! carry, and the held-ids blob of a `SYNC_PUSHED` reply ([`encode_held`]).

use crate::control::frame_size;
use crate::region::Regions;
use crate::{
    append_frame, frame_head_size, peek_control_block, ClientVerb, ControlHeader, DecodedControl, Reader, Target,
    WireConflictMode, WireFlags, Writer, CTRL_HEADER_SIZE,
};
use std::fmt;
use std::num::NonZeroU64;

/// Maximum relations in one `SCAN_MULTI`. The master holds one scan lease and
/// one reply train of bookkeeping per relation; a handful of related tables
/// covers a realistic consistent snapshot.
const SCAN_MULTI_MAX_RELATIONS: usize = 16;

/// The basis of a `PUSH_TXN` family built from no read. No read reports it, and
/// no commit exceeds it.
pub const BLIND: u64 = u64::MAX;

/// One family of a `PUSH_TXN` or `DDL_TXN`: the relation it writes, a
/// `PUSH_TXN` family's conflict mode and basis, its schema record where the
/// verb carries one, and its rows.
pub struct FrameItem<'a> {
    pub target: Target,
    pub conflict_mode: WireConflictMode,
    pub basis: u64,
    pub schema: Option<&'a [u8]>,
    pub data: Regions<'a>,
}

/// A multi-item `verb` frame: a prologue of `arg0` as given and every other
/// field zero, then the items `items` appends, which are `body` bytes exactly,
/// so appending them never reallocates.
fn multi_frame(verb: ClientVerb, arg0: u64, body: usize, items: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut out = Vec::with_capacity(CTRL_HEADER_SIZE + body);
    let hdr = ControlHeader {
        flags: WireFlags { verb, ..Default::default() },
        arg0,
        ..Default::default()
    };
    append_frame(&mut out, &hdr, &[], None, None);
    let cap = out.capacity();
    items(&mut out);
    debug_assert_eq!(out.capacity(), cap, "{verb:?} frame outgrew its size");
    out
}

/// Encode a `PUSH_TXN` or `DDL_TXN` frame, without the 4-byte frame length
/// prefix.
pub fn encode_items(verb: ClientVerb, items: &[FrameItem<'_>]) -> Vec<u8> {
    debug_assert!(
        item_shape(verb).is_some_and(|s| s.data),
        "{verb:?} items carry no data block"
    );
    let body = items
        .iter()
        .map(|it| frame_size(&[], it.schema, Some(&it.data[..])))
        .sum();
    multi_frame(verb, 0, body, |out| {
        for it in items {
            let mut hdr = ControlHeader::naming(verb, it.target, it.basis);
            hdr.flags.conflict_mode = it.conflict_mode;
            append_frame(out, &hdr, &[], it.schema, Some(&it.data[..]));
        }
    })
}

/// The sections an item of a multi-item verb carries, and how many items one
/// frame may hold.
pub(crate) struct ItemShape {
    /// Whether a decoded item may carry a blob; an item carrying one where
    /// this is false is refused.
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
        ClientVerb::ScanMulti => (true, false, false, SCAN_MULTI_MAX_RELATIONS),
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
        ctrl.hdr.check_client_authored(verb == ClientVerb::PushTxn)?;
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

/// One scan: the encoded `ReadSpec` `spec` over `target`, replied in the layout
/// whose digest is `reply_layout`. A `SCAN_MULTI` item, and what a `SCAN_SPEC`
/// request's header and blob spell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanItem<'a> {
    /// The relation, and the token of the RESOLVE answer this read was built from.
    pub target: Target,
    pub reply_layout: u64,
    pub spec: &'a [u8],
}

impl<'a> ScanItem<'a> {
    /// The scan the frame `frame`, peeked as `ctrl`, spells: `target_id` and
    /// the descriptor token in `arg1` name the relation, `arg0` is the reply
    /// layout digest, and the blob is the spec.
    pub fn of(frame: &'a [u8], ctrl: &DecodedControl) -> Self {
        ScanItem {
            target: ctrl.hdr.target(),
            reply_layout: ctrl.hdr.arg0,
            spec: &frame[ctrl.blob.clone()],
        }
    }
}

/// Encode a `SCAN_MULTI` frame, without the 4-byte frame length prefix.
pub fn encode_scan_multi(scans: &[ScanItem<'_>]) -> Vec<u8> {
    let body = scans.iter().map(|s| frame_head_size(s.spec.len(), None)).sum();
    multi_frame(ClientVerb::ScanMulti, 0, body, |out| {
        for s in scans {
            let hdr = ControlHeader::naming(ClientVerb::ScanMulti, s.target, s.reply_layout);
            append_frame(out, &hdr, s.spec, None, None);
        }
    })
}

/// Decode a `SCAN_MULTI` frame body into its items, in request order.
pub fn decode_scan_multi(body: &[u8]) -> Result<Vec<ScanItem<'_>>, String> {
    Ok(decode_items(body, ClientVerb::ScanMulti)?
        .into_iter()
        .map(|(item, c)| ScanItem::of(item, &c))
        .collect())
}

/// Bytes [`DeltaCursor::write_opt`] writes: two words.
const DELTA_CURSOR_BYTES: usize = 16;

/// A delta-feed position, held by the client alone: the boot, relation and
/// spec its rounds belong to (`tag`), and the last round of the view it covers
/// (`tick`).
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct DeltaCursor {
    pub tag: u64,
    pub tick: NonZeroU64,
}

impl DeltaCursor {
    /// The cursor a flat `(tag, tick)` pair spells; tick 0 spells none. A
    /// `DELTA_POLL` terminal carries the pair as `(arg1, arg0)`.
    pub fn from_pair(tag: u64, tick: u64) -> Option<DeltaCursor> {
        NonZeroU64::new(tick).map(|tick| DeltaCursor { tag, tick })
    }

    /// This cursor as a flat `(tag, tick)` pair.
    pub fn pair(self) -> (u64, u64) {
        (self.tag, self.tick.get())
    }

    /// Write `cursor` as the words it is carried in: its tag, then its tick,
    /// both zero for none.
    pub fn write_opt(w: &mut Writer, cursor: Option<Self>) {
        let (tag, tick) = cursor.map_or((0, 0), Self::pair);
        w.u64(tag).u64(tick);
    }

    /// [`Self::write_opt`]'s inverse.
    pub fn read_opt(r: &mut Reader) -> Result<Option<Self>, String> {
        let (tag, tick) = (r.u64()?, r.u64()?);
        Ok(Self::from_pair(tag, tick))
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
    /// of no bound for the view's own rows.
    pub spec: &'a [u8],
}

/// A subscription's id: what a `DELTA_POLL` keeps a view under and a
/// `SYNC_PUSHED` names. A prologue `arg0` of `0` keeps none, so no
/// subscription is `0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubId(NonZeroU64);

impl SubId {
    /// The first id a client hands out.
    pub const FIRST: SubId = SubId(NonZeroU64::MIN);

    /// The id `id` spells; `None` for `0`.
    pub const fn new(id: u64) -> Option<SubId> {
        match NonZeroU64::new(id) {
            Some(id) => Some(SubId(id)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for SubId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Encode a `DELTA_POLL` frame, without the 4-byte frame length prefix. Item
/// `i` is kept as the subscription [`kept_as`]`(keep, i)`.
pub fn encode_delta_poll(views: &[DeltaPollItem<'_>], keep: Option<SubId>) -> Vec<u8> {
    let body = views
        .iter()
        .map(|v| frame_head_size(DELTA_CURSOR_BYTES + v.spec.len(), None))
        .sum();
    let widest = views.iter().map(|v| v.spec.len()).max().unwrap_or(0);
    let mut blob = Writer::with_capacity(DELTA_CURSOR_BYTES + widest);
    multi_frame(ClientVerb::DeltaPoll, keep.map_or(0, SubId::get), body, |out| {
        for v in views {
            blob.clear();
            DeltaCursor::write_opt(&mut blob, v.from);
            blob.raw(v.spec);
            let hdr = ControlHeader::naming(ClientVerb::DeltaPoll, v.view, v.reply_layout);
            append_frame(out, &hdr, blob.as_slice(), None, None);
        }
    })
}

/// The subscription id item `item` of a `DELTA_POLL` keeping from `first` is
/// kept as. Panics past `u64::MAX`, where [`delta_poll_kept`] refuses a poll.
pub fn kept_as(first: SubId, item: usize) -> SubId {
    let id = first.0.checked_add(item as u64);
    SubId(id.expect("a subscription id past u64::MAX"))
}

/// The subscription id each of the `items` items of the `DELTA_POLL` under
/// `prologue` is kept as, in item order; `None` for a poll that keeps none.
/// A poll whose ids would run past `u64::MAX` is refused.
pub fn delta_poll_kept(prologue: &ControlHeader, items: usize) -> Result<Option<impl Iterator<Item = SubId>>, String> {
    let Some(first) = SubId::new(prologue.arg0) else {
        return Ok(None);
    };
    if first.get().checked_add(items.saturating_sub(1) as u64).is_none() {
        return Err(format!(
            "DeltaPoll: {items} subscriptions from {first} run past u64::MAX"
        ));
    }
    Ok(Some((0..items).map(move |item| kept_as(first, item))))
}

/// A `SYNC_PUSHED` blob: the subscription ids its connection still holds.
pub fn encode_held(ids: impl IntoIterator<Item = SubId>) -> Vec<u8> {
    ids.into_iter().flat_map(|id| id.get().to_le_bytes()).collect()
}

/// The ids an [`encode_held`] blob names; one naming `0` is refused.
pub fn decode_held(blob: &[u8]) -> Result<Vec<SubId>, String> {
    let ids: Vec<u64> = crate::le_scalars(blob).map_err(|e| format!("SyncPushed: held ids: {e}"))?;
    ids.into_iter()
        .map(|id| SubId::new(id).ok_or_else(|| "SyncPushed: held ids: 0 names no subscription".to_string()))
        .collect()
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
            let blob = &item[c.blob];
            let mut r = Reader::new(blob);
            let from = DeltaCursor::read_opt(&mut r)
                .map_err(|_| format!("DeltaPoll: view {} carries no cursor", c.hdr.target_id))?;
            Ok(DeltaPollItem {
                view: c.hdr.target(),
                from,
                reply_layout: c.hdr.arg0,
                spec: &blob[r.pos()..],
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "tests/txn_frame.rs"]
mod tests;
