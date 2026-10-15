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

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use gnitz_wire::read_u32_le;
use gnitz_wire::wal::WAL_FORMAT_VERSION;

/// The directory every connection of this process records into.
pub const RECORD_ENV: &str = "GNITZ_RECORD_FRAMES";

const MAGIC: &[u8; 4] = b"GNZR";
const FILE_HEADER: usize = 8;
/// A record's direction byte, connection and frame length.
const RECORD_HEADER: usize = 9;

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

/// Every frame of the recording `bytes`, in order.
pub fn read(bytes: &[u8]) -> Result<Vec<Recorded<'_>>, String> {
    if bytes.len() < FILE_HEADER || &bytes[..4] != MAGIC {
        return Err("not a frame recording".into());
    }
    let version = read_u32_le(bytes, 4);
    if version != WAL_FORMAT_VERSION {
        return Err(format!(
            "recorded at wire format {version:#x}, this build speaks {WAL_FORMAT_VERSION:#x}"
        ));
    }
    let mut rest = &bytes[FILE_HEADER..];
    let mut out = Vec::new();
    while !rest.is_empty() {
        if rest.len() < RECORD_HEADER {
            return Err(format!("record {} is truncated", out.len()));
        }
        let dir = match rest[0] {
            0 => Direction::Sent,
            1 => Direction::Received,
            d => return Err(format!("record {} names direction {d}", out.len())),
        };
        let conn = read_u32_le(rest, 1);
        let len = read_u32_le(rest, 5) as usize;
        let frame = rest[RECORD_HEADER..]
            .get(..len)
            .ok_or_else(|| format!("record {} runs past the recording", out.len()))?;
        out.push(Recorded { conn, dir, frame });
        rest = &rest[RECORD_HEADER + len..];
    }
    Ok(out)
}

/// One recording file, shared by every connection of this process to its target.
struct Recording {
    file: Mutex<File>,
    next_conn: AtomicU32,
}

/// A connection's handle on its recording.
pub(crate) struct Tap {
    recording: Arc<Recording>,
    conn: u32,
}

impl Tap {
    /// The tap of a connection to `target`; `None` unless this process records.
    pub(crate) fn open(target: &str) -> Option<Tap> {
        Some(Tap::open_in(&gnitz_foundation::env::env_path(RECORD_ENV)?, target))
    }

    /// The tap of a connection to `target`, recording into `dir`.
    fn open_in(dir: &Path, target: &str) -> Tap {
        static RECORDINGS: Mutex<Option<HashMap<PathBuf, Arc<Recording>>>> = Mutex::new(None);
        let name = format!(
            "{:016x}-{}.frames",
            gnitz_wire::checksum(target.as_bytes()),
            std::process::id()
        );
        let path = dir.join(name);
        let mut recordings = RECORDINGS.lock().unwrap_or_else(|p| p.into_inner());
        let recording = recordings
            .get_or_insert_default()
            .entry(path)
            .or_insert_with_key(|path| {
                std::fs::create_dir_all(dir).unwrap_or_else(|e| panic!("{RECORD_ENV}: create {}: {e}", dir.display()));
                let mut file =
                    File::create(path).unwrap_or_else(|e| panic!("{RECORD_ENV}: create {}: {e}", path.display()));
                let mut header = MAGIC.to_vec();
                header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
                file.write_all(&header)
                    .unwrap_or_else(|e| panic!("{RECORD_ENV}: write {}: {e}", path.display()));
                Arc::new(Recording {
                    file: Mutex::new(file),
                    next_conn: AtomicU32::new(0),
                })
            });
        let conn = recording.next_conn.fetch_add(1, Ordering::Relaxed);
        Tap { recording: Arc::clone(recording), conn }
    }

    /// Record `frame`, which this connection sent or received. One write per
    /// record, so the connections sharing a file never interleave inside one.
    pub(crate) fn note(&self, dir: Direction, frame: &[u8]) {
        let mut record = Vec::with_capacity(RECORD_HEADER + frame.len());
        record.push(dir as u8);
        record.extend_from_slice(&self.conn.to_le_bytes());
        record.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        record.extend_from_slice(frame);
        let mut file = self.recording.file.lock().unwrap_or_else(|p| p.into_inner());
        file.write_all(&record)
            .unwrap_or_else(|e| panic!("{RECORD_ENV}: a frame could not be recorded: {e}"));
    }
}

#[cfg(test)]
#[path = "tests/recording.rs"]
mod tests;
