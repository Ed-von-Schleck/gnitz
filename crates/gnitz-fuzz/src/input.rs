//! A fuzz input: how to boot the server, which seed, and the edits to make to
//! its traffic. Every byte string decodes to one, so an engine mutating bytes
//! mutates inputs; [`Input::encode`] is the inverse for the inputs a seed
//! starts as.

use crate::edit::{Edit, EDIT_BYTES};
use crate::seed::Frame;

/// How the server an input runs against is booted, and how its session ends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Boot {
    /// Index into [`WORKERS`].
    pub workers: u8,
    /// Over TLS with an access token, rather than the AF_UNIX socket, which
    /// asks for none.
    pub tls: bool,
    /// Kill the server once the traffic is played and boot it again on the
    /// same data directory.
    pub crash: bool,
    /// Checkpoint every few kilobytes of log rather than at a quarter-full SAL.
    pub checkpoints: bool,
}

/// The worker counts a [`Boot`] chooses among.
pub const WORKERS: [usize; 4] = [2, 1, 4, 3];

impl Boot {
    pub fn workers(&self) -> usize {
        WORKERS[self.workers as usize % WORKERS.len()]
    }

    fn decode(b: u8) -> Boot {
        Boot {
            workers: b & 3,
            tls: b & 4 != 0,
            crash: b & 8 != 0,
            checkpoints: b & 16 != 0,
        }
    }

    fn encode(&self) -> u8 {
        (self.workers & 3) | u8::from(self.tls) << 2 | u8::from(self.crash) << 3 | u8::from(self.checkpoints) << 4
    }
}

/// One fuzz input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Input {
    pub boot: Boot,
    /// The seed, by its position among the seeds.
    pub seed: u16,
    pub edits: Vec<Edit>,
}

/// Bytes ahead of the edits: the boot byte and the seed selector.
const HEAD_BYTES: usize = 3;

impl Input {
    /// The input `bytes` encodes. Missing head bytes read as zero, and a
    /// trailing part of an edit is ignored.
    pub fn decode(bytes: &[u8]) -> Input {
        let mut head = [0u8; HEAD_BYTES];
        let n = bytes.len().min(HEAD_BYTES);
        head[..n].copy_from_slice(&bytes[..n]);
        Input {
            boot: Boot::decode(head[0]),
            seed: u16::from_le_bytes([head[1], head[2]]),
            edits: bytes[n..]
                .chunks_exact(EDIT_BYTES)
                .map(|c| Edit::decode(c.try_into().unwrap()))
                .collect(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![self.boot.encode()];
        out.extend_from_slice(&self.seed.to_le_bytes());
        for e in &self.edits {
            out.extend_from_slice(&e.encode());
        }
        out
    }

    /// `sent`, a seed's frames, with this input's edits made in order.
    pub fn edit(&self, mut sent: Vec<Frame>) -> Vec<Frame> {
        for e in &self.edits {
            e.apply(&mut sent);
        }
        sent
    }
}

#[cfg(test)]
#[path = "tests/input.rs"]
mod tests;
