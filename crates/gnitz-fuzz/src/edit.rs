//! Edits: what an input does to its seed's traffic. Each places itself by the
//! anatomy of the frame it edits, so it lands on structure a decoder accepted,
//! and each is total: an edit with nothing to act on leaves the traffic as it
//! was.

use crate::anatomy::Anatomy;
use crate::seed::Frame;
use gnitz_wire::fields::Field;

/// Connections an edit can move a frame onto.
pub const MAX_CONNS: u32 = 4;

/// One edit of a conversation's sent frames. `frame` names a frame by position
/// and `pick` a part of it, each wrapping into range; `how` is the rest of
/// what the edit does, read differently by each kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edit {
    /// Rewrite one integer: a decoder-read field for an even `pick`, a data
    /// cell for an odd one.
    Int {
        frame: u16,
        pick: u16,
        how: u64,
    },
    /// Drop one item of a counted list, its count and every enclosing length
    /// kept true.
    DropItem {
        frame: u16,
        pick: u16,
        how: u64,
    },
    /// Repeat one item of a counted list, likewise.
    RepeatItem {
        frame: u16,
        pick: u16,
        how: u64,
    },
    /// Rewrite one string's content, every enclosing length kept true: cut,
    /// grown, emptied, or another string's content.
    Text {
        frame: u16,
        pick: u16,
        how: u64,
    },
    /// Corrupt bytes, keeping no length true.
    Bytes {
        frame: u16,
        pick: u16,
        how: u64,
    },
    DropFrame {
        frame: u16,
    },
    RepeatFrame {
        frame: u16,
    },
    /// Swap a frame with the one after it.
    SwapFrame {
        frame: u16,
    },
    /// Send a frame over another connection.
    MoveFrame {
        frame: u16,
        conn: u32,
    },
}

/// Encoded bytes of one edit: kind, frame, pick, how.
pub const EDIT_BYTES: usize = 13;

impl Edit {
    /// The edit `bytes` encodes; every byte string of the length is one.
    pub fn decode(bytes: &[u8; EDIT_BYTES]) -> Edit {
        let frame = u16::from_le_bytes([bytes[1], bytes[2]]);
        let pick = u16::from_le_bytes([bytes[3], bytes[4]]);
        let how = u64::from_le_bytes(bytes[5..].try_into().unwrap());
        match bytes[0] % 9 {
            0 => Edit::Int { frame, pick, how },
            1 => Edit::DropItem { frame, pick, how },
            2 => Edit::RepeatItem { frame, pick, how },
            3 => Edit::Text { frame, pick, how },
            4 => Edit::Bytes { frame, pick, how },
            5 => Edit::DropFrame { frame },
            6 => Edit::RepeatFrame { frame },
            7 => Edit::SwapFrame { frame },
            _ => Edit::MoveFrame { frame, conn: pick as u32 % MAX_CONNS },
        }
    }

    pub fn encode(&self) -> [u8; EDIT_BYTES] {
        let (kind, frame, pick, how) = match *self {
            Edit::Int { frame, pick, how } => (0, frame, pick, how),
            Edit::DropItem { frame, pick, how } => (1, frame, pick, how),
            Edit::RepeatItem { frame, pick, how } => (2, frame, pick, how),
            Edit::Text { frame, pick, how } => (3, frame, pick, how),
            Edit::Bytes { frame, pick, how } => (4, frame, pick, how),
            Edit::DropFrame { frame } => (5, frame, 0, 0),
            Edit::RepeatFrame { frame } => (6, frame, 0, 0),
            Edit::SwapFrame { frame } => (7, frame, 0, 0),
            Edit::MoveFrame { frame, conn } => (8, frame, conn as u16, 0),
        };
        let mut out = [0u8; EDIT_BYTES];
        out[0] = kind;
        out[1..3].copy_from_slice(&frame.to_le_bytes());
        out[3..5].copy_from_slice(&pick.to_le_bytes());
        out[5..].copy_from_slice(&how.to_le_bytes());
        out
    }

    /// Apply this edit to `sent`.
    pub fn apply(&self, sent: &mut Vec<Frame>) {
        if sent.is_empty() {
            return;
        }
        let (Edit::Int { frame, .. }
        | Edit::DropItem { frame, .. }
        | Edit::RepeatItem { frame, .. }
        | Edit::Text { frame, .. }
        | Edit::Bytes { frame, .. }
        | Edit::DropFrame { frame }
        | Edit::RepeatFrame { frame }
        | Edit::SwapFrame { frame }
        | Edit::MoveFrame { frame, .. }) = *self;
        let (i, n) = (frame as usize % sent.len(), sent.len());
        match *self {
            Edit::DropFrame { .. } => drop(sent.remove(i)),
            Edit::RepeatFrame { .. } => sent.insert(i + 1, sent[i].clone()),
            Edit::SwapFrame { .. } => sent.swap(i, (i + 1) % n),
            Edit::MoveFrame { conn, .. } => sent[i].conn = conn,
            Edit::Int { pick, how, .. } => int(&mut sent[i].bytes, pick, how),
            Edit::DropItem { pick, how, .. } => item(&mut sent[i].bytes, pick, how, false),
            Edit::RepeatItem { pick, how, .. } => item(&mut sent[i].bytes, pick, how, true),
            Edit::Text { pick, how, .. } => text(&mut sent[i].bytes, pick, how),
            Edit::Bytes { pick, how, .. } => bytes(&mut sent[i].bytes, pick, how),
        }
    }
}

/// `pick` wrapped into `0..n`; `None` for nothing to pick from.
fn wrap(pick: u64, n: usize) -> Option<usize> {
    (n > 0).then(|| (pick % n as u64) as usize)
}

fn read_int(frame: &[u8], f: &Field) -> u64 {
    let mut v = [0u8; 8];
    v[..f.width].copy_from_slice(&frame[f.range()]);
    u64::from_le_bytes(v)
}

fn int(frame: &mut [u8], pick: u16, how: u64) {
    let anatomy = Anatomy::of(frame);
    let pool = if pick.is_multiple_of(2) {
        &anatomy.fields
    } else {
        &anatomy.cells
    };
    let Some(at) = wrap(u64::from(pick / 2), pool.len()) else {
        return;
    };
    let f = pool[at];
    let bits = f.width as u32 * 8;
    let mask = if bits == 64 { u64::MAX } else { (1 << bits) - 1 };
    let old = read_int(frame, &f);
    let arg = how >> 3;
    let new = match how & 7 {
        // The values every bound check is written around.
        0 => [
            0,
            1,
            2,
            mask,
            mask - 1,
            1 << (bits - 1),
            (1 << (bits - 1)) - 1,
            mask >> 1 ^ 1,
        ][arg as usize % 8],
        1 => old.wrapping_add(arg as i8 as u64),
        2 => old ^ (1 << (arg % u64::from(bits))),
        3 => arg,
        // Another field's value of the same width: an id, a column index or a
        // type code that is valid somewhere else in this frame.
        4 => {
            let peers: Vec<&Field> = pool.iter().filter(|p| p.width == f.width).collect();
            read_int(frame, peers[(arg % peers.len() as u64) as usize])
        }
        5 => old.wrapping_sub(1),
        6 => old.wrapping_add(1),
        _ => !old,
    } & mask;
    frame[f.range()].copy_from_slice(&new.to_le_bytes()[..f.width]);
}

fn item(frame: &mut Vec<u8>, pick: u16, how: u64, repeat: bool) {
    let anatomy = Anatomy::of(frame);
    let Some(l) = wrap(u64::from(pick), anatomy.lists.len()) else {
        return;
    };
    let list = &anatomy.lists[l];
    let Some(k) = wrap(how, list.items.len()) else { return };
    let item = list.items[k].clone();
    let count = u16::from_le_bytes([frame[list.count], frame[list.count + 1]]);
    let count = if repeat {
        count.wrapping_add(1)
    } else {
        count.wrapping_sub(1)
    };
    let mut counted = frame.clone();
    counted[list.count..list.count + 2].copy_from_slice(&count.to_le_bytes());
    *frame = match repeat {
        true => anatomy.splice(&counted, item.end..item.end, &frame[item.clone()]),
        false => anatomy.splice(&counted, item, &[]),
    };
}

fn text(frame: &mut Vec<u8>, pick: u16, how: u64) {
    let anatomy = Anatomy::of(frame);
    let Some(s) = wrap(u64::from(pick), anatomy.strings.len()) else {
        return;
    };
    let content = anatomy.strings[s].content.clone();
    let old = &frame[content.clone()];
    let arg = how >> 2;
    let new: Vec<u8> = match how & 3 {
        0 => old[..(arg % (old.len() as u64 + 1)) as usize].to_vec(),
        1 => old
            .iter()
            .chain(old)
            .copied()
            .chain(std::iter::repeat_n(arg as u8, (arg >> 8) as usize % 64))
            .collect(),
        2 => Vec::new(),
        _ => {
            let other = &anatomy.strings[(arg % anatomy.strings.len() as u64) as usize];
            frame[other.content.clone()].to_vec()
        }
    };
    *frame = anatomy.splice(frame, content, &new);
}

fn bytes(frame: &mut Vec<u8>, pick: u16, how: u64) {
    if frame.is_empty() {
        return;
    }
    let at = (pick as usize * frame.len()) >> 16;
    let arg = how >> 3;
    match how & 7 {
        0 => frame[at] ^= 1 << (arg % 8),
        1 => frame[at] = arg as u8,
        2 => frame.insert(at, arg as u8),
        3 => drop(frame.drain(at..frame.len().min(at + 1 + arg as usize % 16))),
        4 => frame.truncate(at.max(1)),
        5 => {
            let end = frame.len().min(at + 1 + arg as usize % 64);
            let chunk = frame[at..end].to_vec();
            frame.splice(end..end, chunk);
        }
        6 => frame.extend(std::iter::repeat_n(arg as u8, 1 + (arg >> 8) as usize % 64)),
        _ => frame[at] = frame[at].wrapping_add(arg as u8 | 1),
    }
}

#[cfg(test)]
#[path = "tests/edit.rs"]
mod tests;
