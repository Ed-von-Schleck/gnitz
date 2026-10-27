//! A recording of the frames clients exchanged with one server: every frame a
//! connection sent or received after its HELLO, in the order this process saw
//! them, tagged with the connection.
//!
//! A process whose `GNITZ_RECORD_FRAMES` names a directory records every
//! connection it opens there. The connections of one process to one target
//! share a recording, so a test that opens several against its own server
//! leaves one conversation; recordings are the fuzzer's seeds.
//!
//! A recording opens with the wire format version it was taken under, and
//! [`read`] refuses any other: a frame of another version is not one a replay
//! could send.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use gnitz_wire::wal::WAL_FORMAT_VERSION;
use gnitz_wire::{Reader, Writer};

/// The directory every connection of this process records into.
const RECORD_ENV: &str = "GNITZ_RECORD_FRAMES";

/// What a recording file is named with.
pub const EXT: &str = "frames";

const MAGIC: &[u8; 4] = b"GNZR";

/// Which way a recorded frame went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Sent = 0,
    Received = 1,
}

/// One recorded frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recorded<'a> {
    /// The connection, numbered from 0 in the order this process opened them.
    pub conn: u32,
    pub dir: Direction,
    pub frame: &'a [u8],
}

impl Recorded<'_> {
    fn write(&self, w: &mut Writer) {
        w.u8(self.dir as u8).u32(self.conn).bytes32(self.frame);
    }
}

/// The recording of `records`, in order: what a recording file holds.
pub fn encode(records: &[Recorded]) -> Vec<u8> {
    let mut w = Writer::new();
    w.raw(MAGIC).u32(WAL_FORMAT_VERSION);
    records.iter().for_each(|r| r.write(&mut w));
    w.into_vec()
}

/// Every frame of the recording `bytes`, in order.
pub fn read(bytes: &[u8]) -> Result<Vec<Recorded<'_>>, String> {
    let mut r = Reader::new(bytes);
    if r.take(MAGIC.len()).ok() != Some(MAGIC) {
        return Err("not a frame recording".into());
    }
    let version = r.u32().map_err(|_| "not a frame recording")?;
    if version != WAL_FORMAT_VERSION {
        return Err(format!(
            "recorded at wire format {version:#x}, this build speaks {WAL_FORMAT_VERSION:#x}"
        ));
    }
    let mut out = Vec::new();
    while r.remaining() > 0 {
        let mut record = || {
            let dir = match r.u8()? {
                0 => Direction::Sent,
                1 => Direction::Received,
                d => return Err(format!("names direction {d}")),
            };
            Ok(Recorded { dir, conn: r.u32()?, frame: r.bytes32()? })
        };
        let record = record().map_err(|e| format!("record {}: {e}", out.len()))?;
        out.push(record);
    }
    Ok(out)
}

/// A connection's handle on its recording, a file every connection of this
/// process to its target shares.
pub(crate) struct Tap {
    file: Arc<Mutex<File>>,
    conn: u32,
}

impl Tap {
    /// The tap of a connection to `target`; `None` unless this process records.
    pub(crate) fn open(target: &str) -> Option<Tap> {
        Some(Tap::open_in(&gnitz_foundation::env::env_path(RECORD_ENV)?, target))
    }

    /// The tap of a connection to `target`, recording into `dir`.
    fn open_in(dir: &Path, target: &str) -> Tap {
        /// A recording's file, and the connections opened onto it.
        struct Recording {
            file: Arc<Mutex<File>>,
            conns: u32,
        }
        static RECORDINGS: Mutex<BTreeMap<PathBuf, Recording>> = Mutex::new(BTreeMap::new());
        let name = format!(
            "{:016x}-{}.{EXT}",
            gnitz_wire::checksum(target.as_bytes()),
            std::process::id()
        );
        let path = dir.join(name);
        let mut recordings = RECORDINGS.lock().unwrap_or_else(|p| p.into_inner());
        let recording = recordings.entry(path).or_insert_with_key(|path| {
            std::fs::create_dir_all(dir).unwrap_or_else(|e| panic!("{RECORD_ENV}: create {}: {e}", dir.display()));
            let mut file =
                File::create(path).unwrap_or_else(|e| panic!("{RECORD_ENV}: create {}: {e}", path.display()));
            file.write_all(&encode(&[]))
                .unwrap_or_else(|e| panic!("{RECORD_ENV}: write {}: {e}", path.display()));
            Recording {
                file: Arc::new(Mutex::new(file)),
                conns: 0,
            }
        });
        let conn = recording.conns;
        recording.conns += 1;
        Tap { file: Arc::clone(&recording.file), conn }
    }

    /// Record `frame`, which this connection sent or received. One write per
    /// record, so the connections sharing a file never interleave inside one.
    pub(crate) fn note(&self, dir: Direction, frame: &[u8]) {
        let mut w = Writer::with_capacity(1 + 4 + 4 + frame.len());
        Recorded { conn: self.conn, dir, frame }.write(&mut w);
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        file.write_all(&w.into_vec())
            .unwrap_or_else(|e| panic!("{RECORD_ENV}: a frame could not be recorded: {e}"));
    }
}

#[cfg(test)]
#[path = "tests/recording.rs"]
mod tests;
