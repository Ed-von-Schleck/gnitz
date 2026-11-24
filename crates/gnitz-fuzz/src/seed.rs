//! Seeds: recorded conversations, each what its connections sent and received.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use gnitz_core::recording::{self, Direction};

/// One frame of a conversation, and the connection it went over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub conn: u32,
    pub bytes: Vec<u8>,
}

/// One recorded conversation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Seed {
    /// What the connections queued to send, in the order they queued it.
    pub sent: Vec<Frame>,
    /// What they received, in the order they received it.
    pub received: Vec<Frame>,
}

impl Seed {
    /// The conversation a recording holds.
    pub fn from_recording(bytes: &[u8]) -> Result<Seed, String> {
        let mut seed = Seed::default();
        for r in recording::read(bytes)? {
            let frame = Frame { conn: r.conn, bytes: r.frame.to_vec() };
            match r.dir {
                Direction::Sent => seed.sent.push(frame),
                Direction::Received => seed.received.push(frame),
            }
        }
        Ok(seed)
    }
}

/// The directory of seeds an input's selector picks from, for every target.
pub const SEEDS_ENV: &str = "GNITZ_FUZZ_SEEDS";

/// The seeds of one directory, in file-name order: an input names a seed by
/// its position, so the order is fixed for as long as the directory is.
pub struct Seeds {
    paths: Vec<PathBuf>,
}

impl Seeds {
    pub fn open(dir: &Path) -> io::Result<Seeds> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
            .map(|e| e.map(|e| e.path()))
            .collect::<io::Result<_>>()?;
        paths.retain(|p| p.extension().is_some_and(|e| e == recording::EXT));
        paths.sort();
        Ok(Seeds { paths })
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// The seed an input's selector `pick` names; the selector wraps, so every
    /// selector names one.
    pub fn pick(&self, pick: u16) -> Result<(Seed, &Path), String> {
        if self.paths.is_empty() {
            return Err("no seeds".into());
        }
        let at = pick as usize % self.paths.len();
        Ok((self.load(at)?, &self.paths[at]))
    }

    /// The seed at position `at`.
    pub fn load(&self, at: usize) -> Result<Seed, String> {
        let path = &self.paths[at];
        let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Seed::from_recording(&bytes).map_err(|e| format!("{}: {e}", path.display()))
    }
}

/// The access token the HELLO starts carry, in the minted shape.
pub const HELLO_TOKEN: &str = "gnitz_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const _: () = assert!(HELLO_TOKEN.len() <= gnitz_wire::MAX_TOKEN_BYTES);

/// Write every target's starting inputs for `seeds` into `starts/<target>`:
/// one per seed for the traffic targets, every expression program the seeds
/// carry for the VM's, every seed's replies as one stream for the client's,
/// and a HELLO before a seed's first frames for the handshake's.
pub fn write_starts(seeds: &Seeds, starts: &Path) -> io::Result<()> {
    let [traffic, program, reply, hello] = ["traffic", "program", "reply", "hello"].map(|t| starts.join(t));
    for d in [&traffic, &program, &reply, &hello] {
        std::fs::create_dir_all(d)?;
    }
    for i in 0..seeds.len() {
        let input = crate::input::Input::unedited(i as u16);
        std::fs::write(traffic.join(format!("seed-{i:05}")), input)?;
        let seed = seeds.load(i).map_err(io::Error::other)?;
        for frame in &seed.sent {
            for p in crate::anatomy::Anatomy::of(&frame.bytes).programs {
                let text = &frame.bytes[p];
                std::fs::write(program.join(format!("{:016x}", gnitz_wire::checksum(text))), text)?;
            }
        }
        let replies: Vec<u8> = seed
            .received
            .iter()
            .filter(|f| f.conn == 0)
            .flat_map(|f| gnitz_wire::framed(&f.bytes))
            .collect();
        std::fs::write(reply.join(format!("seed-{i:05}")), replies)?;
        // Its first byte cuts the stream into reads; the rest is a connection.
        let opening = std::iter::once(0)
            .chain(gnitz_wire::framed(&gnitz_wire::client_hello(HELLO_TOKEN.as_bytes())))
            .chain(seed.sent.iter().take(2).flat_map(|f| gnitz_wire::framed(&f.bytes)))
            .collect::<Vec<u8>>();
        std::fs::write(hello.join(format!("seed-{i:05}")), opening)?;
    }
    Ok(())
}

/// The most a seed's connections may send between them. A fuzz input costs
/// its seed's whole traffic every run, so a bulk load is a benchmark rather
/// than a seed; what its frames are made of, smaller seeds carry too.
pub const MAX_SEED_SENT: usize = 1 << 20;

/// What [`import`] kept of a directory of recordings.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Imported {
    pub seeds: usize,
    /// Recordings that sent nothing, or sent exactly what a kept one did.
    pub duplicates: usize,
    /// Recordings that sent more than [`MAX_SEED_SENT`].
    pub oversized: usize,
    /// Recordings of another wire format, or torn ones.
    pub unreadable: usize,
}

/// Copy every distinct recording in `recordings` into `seeds`, named by the
/// digest of what it sent: two tests that sent the same frames are one seed,
/// and importing again changes nothing.
pub fn import(recordings: &Path, seeds: &Path) -> io::Result<Imported> {
    std::fs::create_dir_all(seeds)?;
    let mut out = Imported::default();
    let mut seen = HashSet::new();
    for path in Seeds::open(recordings)?.paths {
        let bytes = std::fs::read(&path)?;
        let Ok(seed) = Seed::from_recording(&bytes) else {
            out.unreadable += 1;
            continue;
        };
        let digest = seed
            .sent
            .iter()
            .fold(gnitz_wire::ChecksumStream::default(), |mut h, f| {
                h.update(&f.conn.to_le_bytes());
                h.update(&(f.bytes.len() as u64).to_le_bytes());
                h.update(&f.bytes);
                h
            })
            .digest();
        if seed.sent.is_empty() || !seen.insert(digest) {
            out.duplicates += 1;
            continue;
        }
        if seed.sent.iter().map(|f| f.bytes.len()).sum::<usize>() > MAX_SEED_SENT {
            out.oversized += 1;
            continue;
        }
        std::fs::write(seeds.join(format!("{digest:016x}.{}", recording::EXT)), &bytes)?;
        out.seeds += 1;
    }
    Ok(out)
}

#[cfg(test)]
#[path = "tests/seed.rs"]
mod tests;
