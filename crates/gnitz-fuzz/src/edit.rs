//! Edits: what an input does to its seed's traffic. Each places itself by the
//! anatomy of the frame it edits, so it lands on structure a decoder accepted,
//! and each is total: an edit with nothing to act on leaves the traffic as it
//! was.

use crate::anatomy::Anatomy;
use crate::seed::Frame;
use gnitz_wire::fields::Field;

/// Connections an edit can move a frame onto.
pub const MAX_CONNS: u32 = 4;

/// What an edit does: each kind reads the edit's `pick` and `how` its own way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Rewrite one integer: a decoder-read field for an even `pick`, a data
    /// cell for an odd one.
    Int,
    /// Drop one item of a counted list, its count and every enclosing length
    /// kept true.
    DropItem,
    /// Repeat one item of a counted list, likewise.
    RepeatItem,
    /// Rewrite one string's content, every enclosing length kept true: cut,
    /// grown, emptied, or another string's content.
    Text,
    /// Corrupt bytes, keeping no length true.
    Bytes,
    DropFrame,
    RepeatFrame,
    /// Swap a frame with the one after it.
    SwapFrame,
    /// Send a frame over connection `pick % MAX_CONNS`.
    MoveFrame,
}

pub(crate) const KINDS: [Kind; 9] = [
    Kind::Int,
    Kind::DropItem,
    Kind::RepeatItem,
    Kind::Text,
    Kind::Bytes,
    Kind::DropFrame,
    Kind::RepeatFrame,
    Kind::SwapFrame,
    Kind::MoveFrame,
];

/// One edit of a conversation's sent frames. `frame` names a frame by position
/// and `pick` a part of it, each wrapping into range; `how` is the rest of
/// what the edit does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Edit {
    pub kind: Kind,
    pub frame: u16,
    pub pick: u16,
    pub how: u64,
}

/// Encoded bytes of one edit: kind, frame, pick, how.
pub const EDIT_BYTES: usize = 13;

impl Edit {
    /// The edit `bytes` encodes; every byte string of the length is one.
    pub fn decode(bytes: &[u8; EDIT_BYTES]) -> Edit {
        Edit {
            kind: KINDS[bytes[0] as usize % KINDS.len()],
            frame: u16::from_le_bytes([bytes[1], bytes[2]]),
            pick: u16::from_le_bytes([bytes[3], bytes[4]]),
            how: u64::from_le_bytes(bytes[5..].try_into().unwrap()),
        }
    }

    /// Apply this edit to `sent`.
    pub fn apply(&self, sent: &mut Vec<Frame>) {
        if sent.is_empty() {
            return;
        }
        let (i, n) = (self.frame as usize % sent.len(), sent.len());
        let (pick, how) = (self.pick, self.how);
        match self.kind {
            Kind::DropFrame => drop(sent.remove(i)),
            Kind::RepeatFrame => sent.insert(i + 1, sent[i].clone()),
            Kind::SwapFrame => sent.swap(i, (i + 1) % n),
            Kind::MoveFrame => sent[i].conn = u32::from(pick) % MAX_CONNS,
            Kind::Int => int(&mut sent[i].bytes, pick, how),
            Kind::DropItem => item(&mut sent[i].bytes, pick, how, false),
            Kind::RepeatItem => item(&mut sent[i].bytes, pick, how, true),
            Kind::Text => text(&mut sent[i].bytes, pick, how),
            Kind::Bytes => bytes(&mut sent[i].bytes, pick, how),
        }
    }
}

/// `pick` wrapped into `0..n`; `None` for nothing to pick from.
fn wrap(pick: u64, n: usize) -> Option<usize> {
    (n > 0).then(|| (pick % n as u64) as usize)
}

fn read_int(frame: &[u8], f: &Field) -> u128 {
    let mut v = [0u8; 16];
    v[..f.width].copy_from_slice(&frame[f.range()]);
    u128::from_le_bytes(v)
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
    let mask = u128::MAX >> (128 - bits);
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
        1 => old.wrapping_add(arg as i8 as i128 as u128),
        2 => old ^ (1 << (arg % u64::from(bits))),
        3 => u128::from(arg),
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
    frame[list.count..list.count + 2].copy_from_slice(&count.to_le_bytes());
    *frame = match repeat {
        true => anatomy.splice(frame, item.clone(), &frame[item.clone()].repeat(2)),
        false => anatomy.splice(frame, item, &[]),
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
