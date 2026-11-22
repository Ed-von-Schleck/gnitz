//! The four **multi-item request frames** — `PUSH_TXN`, `DDL_TXN`, `SCAN_MULTI`
//! and `DELTA_POLL` — in both directions.
//!
//! A frame is a prologue header naming the verb (`target_id = 0`), then items to
//! the end of the frame, each a control frame of that verb. A `DELTA_POLL`'s
//! prologue `arg0` is the first subscription id its items are kept under
//! ([`DeltaPoll::keep`]); every other prologue field is zero, which
//! [`DecodedControl::client_verb`] checks. The sections each verb's items
//! carry are [`decode_items`]'s to check.
//!
//! | Verb         | Item header                                              | Sections                  |
//! |--------------|----------------------------------------------------------|---------------------------|
//! | `DDL_TXN`    | `target_id` = system family                              | data block                |
//! | `PUSH_TXN`   | `target_id`; `flags.conflict_mode`; `arg0` = basis; `arg1` = descriptor token | blob = shape record; data block |
//! | `SCAN_MULTI` | `target_id`; `arg0` = reply layout digest; `arg1` = descriptor token | blob = `ReadSpec` |
//! | `DELTA_POLL` | `target_id` = view (≠ 0); `arg0` = reply layout digest; `arg1` = descriptor token | blob = cursor tag, after_tick, `ReadSpec` |
//!
//! **A reply fault's `target_id`:** a `DELTA_POLL` fault naming one of its views
//! ends that position alone; every other fault ends the request.
//!
//! **A `DELTA_POLL` reply** is one train per view, ended by a terminal whose
//! `target_id` is the view and whose `arg0`/`arg1` carry the tag and tick of the
//! [`DeltaCursor`] to read from next. Tick 0 is never sent.
//!
//! The module also holds the delta feed's ids: the [`DeltaCursor`] a
//! `DELTA_POLL` item and terminal carry, and the held-ids blob ([`encode_held`])
//! of a `SYNC_PUSHED` request, a single-item verb.

use crate::control::{Section, VerbShape, frame_size, read_control};
use crate::region::Regions;
use crate::{
    CTRL_HEADER_SIZE, ClientVerb, ControlHeader, DecodedControl, Reader, SysFamily, Target, WireConflictMode, Writer,
    append_frame,
};
use std::fmt;
use std::num::NonZeroU64;
use std::ops::Range;

/// The basis of a `PUSH_TXN` family built from no read. No read reports it, and
/// no commit exceeds it.
pub const BLIND: u64 = u64::MAX;

/// One family of a `PUSH_TXN` or `DDL_TXN`: the relation it writes, a
/// `PUSH_TXN` family's conflict mode, basis and shape record, and its rows.
pub struct FrameItem<'a> {
    pub target: Target,
    pub conflict_mode: WireConflictMode,
    pub basis: u64,
    /// A `PUSH_TXN` family's relation's shape record, which names no column;
    /// empty for a `DDL_TXN` family.
    pub blob: &'a [u8],
    pub data: Regions<'a>,
}

/// Append a multi-item `verb` frame to `out`: a prologue of `arg0` as given and
/// every other field zero, then the items `items` appends, which are `body`
/// bytes exactly, so appending them never reallocates.
fn multi_frame(out: &mut Vec<u8>, verb: ClientVerb, arg0: u64, body: usize, items: impl FnOnce(&mut Vec<u8>)) {
    let at = out.len();
    crate::frame_buf::reserve(out, CTRL_HEADER_SIZE + body);
    append_frame(out, &ControlHeader::naming(verb, Target::from(0), arg0), &[], None);
    items(out);
    debug_assert_eq!(out.len() - at, CTRL_HEADER_SIZE + body, "{verb:?} frame missized");
}

/// Append a `PUSH_TXN` or `DDL_TXN` frame to `out`, without the 4-byte frame
/// length prefix.
pub fn encode_items(out: &mut Vec<u8>, verb: ClientVerb, items: &[FrameItem<'_>]) {
    let shape = VerbShape::of(verb);
    debug_assert!(
        shape.items.is_some() && shape.sections.data == Section::Must,
        "{verb:?} items carry no data block"
    );
    let body = items.iter().map(|it| frame_size(it.blob, Some(&it.data[..]))).sum();
    multi_frame(out, verb, 0, body, |out| {
        for it in items {
            let hdr = ControlHeader::item(verb, it.target, it.basis, it.conflict_mode);
            append_frame(out, &hdr, it.blob, Some(&it.data[..]));
        }
    })
}

/// Append a `DDL_TXN` frame to `out`: one item per system family, named by its
/// table id alone, its block in `rows`.
pub fn encode_ddl_txn<'a>(out: &mut Vec<u8>, families: impl IntoIterator<Item = (SysFamily, Regions<'a>)>) {
    let items: Vec<FrameItem<'a>> = families
        .into_iter()
        .map(|(family, data)| FrameItem {
            target: Target::from(family),
            conflict_mode: WireConflictMode::default(),
            basis: 0,
            blob: &[],
            data,
        })
        .collect();
    encode_items(out, ClientVerb::DdlTxn, &items)
}

/// Split the item list at `body` of the multi-item `verb` frame `frame` into
/// its items, in send order: each item's peeked control, its ranges indexing
/// `frame`. An item's `body` is the empty range at its end.
pub fn decode_items(frame: &[u8], body: Range<usize>, verb: ClientVerb) -> Result<Vec<DecodedControl>, String> {
    let shape = VerbShape::of(verb);
    let Some(items) = shape.items else {
        return Err(format!("{verb:?} is not a multi-item verb"));
    };
    let item = |r: &mut Reader<'_>| -> Result<DecodedControl, String> {
        let ctrl = read_control(r)?;
        // `continuation` is the frame's, on its prologue.
        ctrl.hdr.check_client_authored(false)?;
        if ctrl.hdr.flags.verb != Some(verb) {
            return Err(format!("an item names verb {:?}", ctrl.hdr.flags.verb));
        }
        shape.sections.check(&ctrl)?;
        Ok(ctrl)
    };
    if body.is_empty() {
        return Err(format!("{verb:?}: empty item list"));
    }
    let mut r = Reader::new(&frame[..body.end]);
    r.take(body.start)?;
    let mut out = Vec::new();
    while r.remaining() > 0 {
        if out.len() == items.cap {
            return Err(format!("{verb:?}: too many items (max {})", items.cap));
        }
        out.push(item(&mut r).map_err(|e| format!("{verb:?} item {}: {e}", out.len()))?);
    }
    Ok(out)
}

/// One scan: the encoded `ReadSpec` `spec` over `target`, replied in the layout
/// whose digest is `reply_layout`: one `SCAN_MULTI` item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanItem<'a> {
    /// The relation, and the token of the RESOLVE answer this read was built from.
    pub target: Target,
    pub reply_layout: u64,
    pub spec: &'a [u8],
}

/// Append a `SCAN_MULTI` frame to `out`, without the 4-byte frame length
/// prefix.
pub fn encode_scan_multi(out: &mut Vec<u8>, scans: &[ScanItem<'_>]) {
    let body = scans.iter().map(|s| frame_size(s.spec, None)).sum();
    multi_frame(out, ClientVerb::ScanMulti, 0, body, |out| {
        for s in scans {
            let hdr = ControlHeader::naming(ClientVerb::ScanMulti, s.target, s.reply_layout);
            append_frame(out, &hdr, s.spec, None);
        }
    })
}

/// Decode a `SCAN_MULTI` frame body into its items, in request order.
pub fn decode_scan_multi(body: &[u8]) -> Result<Vec<ScanItem<'_>>, String> {
    Ok(decode_items(body, 0..body.len(), ClientVerb::ScanMulti)?
        .into_iter()
        .map(|c| ScanItem {
            target: c.hdr.target(),
            reply_layout: c.hdr.arg0,
            spec: &body[c.blob],
        })
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
    /// The cursor a flat `(tag, tick)` pair spells; tick 0 spells none.
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

/// Append a `DELTA_POLL` frame to `out`, without the 4-byte frame length
/// prefix. Item `i` is kept as the subscription [`kept_as`]`(keep, i)`.
pub fn encode_delta_poll(out: &mut Vec<u8>, views: &[DeltaPollItem<'_>], keep: Option<SubId>) {
    let body = views
        .iter()
        .map(|v| CTRL_HEADER_SIZE + DELTA_CURSOR_BYTES + v.spec.len())
        .sum();
    let widest = views.iter().map(|v| v.spec.len()).max().unwrap_or(0);
    let mut blob = Writer::with_capacity(DELTA_CURSOR_BYTES + widest);
    multi_frame(out, ClientVerb::DeltaPoll, keep.map_or(0, SubId::get), body, |out| {
        for v in views {
            blob.clear();
            DeltaCursor::write_opt(&mut blob, v.from);
            blob.raw(v.spec);
            let hdr = ControlHeader::naming(ClientVerb::DeltaPoll, v.view, v.reply_layout);
            append_frame(out, &hdr, blob.as_slice(), None);
        }
    })
}

/// The subscription id item `item` of a `DELTA_POLL` keeping from `first` is
/// kept as. Panics past `u64::MAX`, where [`decode_delta_poll`] refuses a poll.
pub fn kept_as(first: SubId, item: usize) -> SubId {
    let id = first.0.checked_add(item as u64);
    SubId(id.expect("a subscription id past u64::MAX"))
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

/// A decoded `DELTA_POLL`: its items, and the first subscription id it keeps
/// them under.
#[derive(Debug)]
pub struct DeltaPoll<'a> {
    pub items: Vec<DeltaPollItem<'a>>,
    /// The prologue's `arg0`; `None` for a poll that keeps none.
    pub keep: Option<SubId>,
}

impl DeltaPoll<'_> {
    /// The subscription id each item is kept as, in item order; `None` for a
    /// poll that keeps none.
    pub fn kept(&self) -> Option<impl Iterator<Item = SubId> + use<>> {
        let n = self.items.len();
        self.keep.map(|first| (0..n).map(move |i| kept_as(first, i)))
    }
}

/// Decode the item list `body` of a `DELTA_POLL` frame under `prologue`. View
/// id `0` is refused: it is the id of a fault ending the request. So is a poll
/// whose kept ids would run past `u64::MAX`.
pub fn decode_delta_poll<'a>(body: &'a [u8], prologue: &ControlHeader) -> Result<DeltaPoll<'a>, String> {
    let items = decode_items(body, 0..body.len(), ClientVerb::DeltaPoll)?
        .into_iter()
        .map(|c| {
            if c.hdr.target_id == 0 {
                return Err("DeltaPoll: view id 0 names no view".to_string());
            }
            let blob = &body[c.blob];
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
        .collect::<Result<Vec<_>, String>>()?;
    let keep = SubId::new(prologue.arg0);
    if let Some(first) = keep
        && first.get().checked_add(items.len().saturating_sub(1) as u64).is_none()
    {
        return Err(format!(
            "DeltaPoll: {} subscriptions from {first} run past u64::MAX",
            items.len()
        ));
    }
    Ok(DeltaPoll { items, keep })
}

#[cfg(test)]
#[path = "tests/multi_frame.rs"]
mod tests;
