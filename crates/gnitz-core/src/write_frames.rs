//! A write as the frames it goes in: one, or, past the session's ceiling,
//! several cut at row boundaries — a `PUSH` into frames of one chunk of its
//! batch each, a `PUSH_TXN` into frames of whole families and chunks of them,
//! every frame but the last setting `continuation`. The server commits the
//! frames as one write, and reads the consecutive chunks of one family as
//! that family.

use gnitz_wire::{
    CTRL_HEADER_SIZE, ClientVerb, ControlHeader, MAX_FRAME_PAYLOAD, Target, append_frame, encode_frame_head,
};
use gnitz_zset::repr::WireCut;
use smallvec::SmallVec;
use std::sync::Arc;

use crate::ZSetBatch;

/// One family of a write: the item header each of its chunks carries, and its
/// rows, whose schema's shape record each chunk carries too.
#[derive(Clone)]
pub(crate) struct WriteFamily {
    pub(crate) hdr: ControlHeader,
    pub(crate) batch: Arc<ZSetBatch>,
}

/// One family's rows within a frame: a chunk of them, or `None` for a family
/// with no rows, which goes as a `PUSH` with no data block, or a `PUSH_TXN`
/// item with a block of no rows.
struct Chunk {
    family: usize,
    rows: Option<WireCut>,
}

/// A write of `verb` — `PUSH`, whose one family is `families[0]`, or
/// `PUSH_TXN` — cut into frames of at most `ceiling` payload bytes. A frame
/// carries at least one row, so a row wider than `ceiling` goes in a frame of
/// its own past it. It holds its families, so its frames can be encoded one
/// at a time as a socket takes them.
pub(crate) struct SpanningWrite {
    verb: ClientVerb,
    families: SmallVec<[WriteFamily; 1]>,
    /// Every frame's chunks, in send order.
    chunks: SmallVec<[Chunk; 1]>,
    /// Each frame, in send order: where its chunks end in `chunks`, and its
    /// payload bytes.
    frames: SmallVec<[(usize, usize); 1]>,
}

impl SpanningWrite {
    /// Cut `families` into frames of at most `ceiling` bytes, filling each
    /// before the next opens; `Err` with a frame's size where one row's
    /// frame is past [`MAX_FRAME_PAYLOAD`].
    pub(crate) fn cut(verb: ClientVerb, families: SmallVec<[WriteFamily; 1]>, ceiling: usize) -> Result<Self, usize> {
        debug_assert!(matches!(verb, ClientVerb::Push | ClientVerb::PushTxn));
        debug_assert!(
            verb == ClientVerb::PushTxn || families.len() == 1,
            "a PUSH writes one family"
        );
        let txn = verb == ClientVerb::PushTxn;
        let prologue = if txn { CTRL_HEADER_SIZE } else { 0 };
        // What a family with no rows adds past its head: a `PUSH_TXN` item
        // carries a block header of no rows.
        let no_rows = if txn { gnitz_wire::wal::WAL_HEADER_SIZE } else { 0 };
        let mut write = SpanningWrite {
            verb,
            families: SmallVec::new(),
            chunks: SmallVec::new(),
            frames: SmallVec::new(),
        };
        // The open frame: its first chunk, and its bytes so far.
        let (mut open, mut size) = (0, prologue);
        let close = |write: &mut Self, open: &mut usize, size: &mut usize| -> Result<(), usize> {
            if *size > MAX_FRAME_PAYLOAD {
                return Err(*size);
            }
            write.frames.push((write.chunks.len(), *size));
            (*open, *size) = (write.chunks.len(), prologue);
            Ok(())
        };
        for (family, f) in families.iter().enumerate() {
            let (blob, rows) = (f.batch.schema().shape_record(), f.batch.view());
            let head = CTRL_HEADER_SIZE + blob.len();
            if rows.is_empty() {
                if write.chunks.len() > open && size + head + no_rows > ceiling {
                    close(&mut write, &mut open, &mut size)?;
                }
                write.chunks.push(Chunk { family, rows: None });
                size += head + no_rows;
                continue;
            }
            // A family that fits the open frame whole goes over its heap as it
            // stands.
            if let Some(whole) = rows.wire_whole()
                && size + head + whole.byte_size() <= ceiling
            {
                size += head + whole.byte_size();
                write.chunks.push(Chunk { family, rows: Some(whole.cut()) });
                continue;
            }
            let mut start = 0;
            while let Some(chunk) = rows.wire_rows_within(start, ceiling.saturating_sub(size + head)) {
                // A chunk past what the frame has left goes in a frame of its own.
                if write.chunks.len() > open && size + head + chunk.byte_size() > ceiling {
                    close(&mut write, &mut open, &mut size)?;
                    continue;
                }
                start += chunk.rows();
                size += head + chunk.byte_size();
                write.chunks.push(Chunk { family, rows: Some(chunk.cut()) });
                if start < rows.len() {
                    close(&mut write, &mut open, &mut size)?;
                }
            }
        }
        close(&mut write, &mut open, &mut size)?;
        write.families = families;
        Ok(write)
    }

    /// The frames, in send order.
    pub(crate) fn frames(&self) -> usize {
        self.frames.len()
    }

    /// Append frame `at`'s payload to `out`.
    pub(crate) fn encode(&self, at: usize, out: &mut Vec<u8>) {
        let more = at + 1 < self.frames.len();
        let first = at.checked_sub(1).map_or(0, |p| self.frames[p].0);
        let (end, size) = self.frames[at];
        gnitz_wire::frame_buf::reserve(out, size);
        let start = out.len();
        let txn = self.verb == ClientVerb::PushTxn;
        if txn {
            let mut prologue = ControlHeader::naming(ClientVerb::PushTxn, Target::from(0), 0);
            prologue.flags.continuation = more;
            append_frame(out, &prologue, &[], None);
        }
        for chunk in &self.chunks[first..end] {
            let f = &self.families[chunk.family];
            let (blob, view) = (f.batch.schema().shape_record(), f.batch.view());
            let mut hdr = f.hdr;
            hdr.flags.continuation = more && !txn;
            let Some(rows) = chunk.rows.map(|cut| view.wire_cut(cut)) else {
                append_frame(out, &hdr, blob, txn.then(|| view.regions()).as_deref());
                continue;
            };
            let item = out.len();
            let len = CTRL_HEADER_SIZE + blob.len() + rows.byte_size();
            // SAFETY: the frame's `size` bytes are reserved above, and the
            // item's are written whole below: the control header and blob by
            // `encode_frame_head`, the block by `WireRows::encode`, which
            // fills every byte of its `byte_size` (gnitz-zset's
            // `DirectWriter`).
            #[allow(clippy::uninit_vec)]
            unsafe {
                out.set_len(item + len);
            }
            let block = item + encode_frame_head(&mut out[item..], &hdr, blob, true);
            let written = rows.encode(&mut out[block..]);
            debug_assert_eq!(block + written, out.len(), "a chunk encodes to its size");
        }
        debug_assert_eq!(out.len() - start, size, "a frame encodes to its size");
    }
}

#[cfg(test)]
#[path = "tests/write_frames.rs"]
mod tests;
