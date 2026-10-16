//! The blob of a `SNAPSHOT_PAGE` request.

use crate::codec::{decode_all, Writer};
use crate::{MAX_PK_BYTES, MAX_WORKERS};

/// The most rows one page holds.
pub const MAX_PAGE_ROWS: u64 = 1 << 20;

/// How many rows a page of a snapshot holds, what it is read after, and in
/// which layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    /// The reply layout's digest, which the pinned store's must equal.
    pub layout: u64,
    /// The rows the page holds at most, `1..=MAX_PAGE_ROWS`, shared among its stores.
    pub limit: u64,
    /// Per answering store, by rank: the OPK key its page starts strictly above,
    /// `None` from its first row. Empty starts every store at its first row.
    pub after: Vec<Option<Vec<u8>>>,
}

impl PageRequest {
    /// The rows each of `of` answering stores puts in a page of `limit` rows.
    pub fn rows_per_store(limit: u64, of: usize) -> u64 {
        limit.div_ceil(of as u64)
    }

    /// The key the page of the store at `rank` of `of` starts above.
    pub fn after_at(&self, rank: usize, of: usize) -> Result<Option<&[u8]>, String> {
        match self.after.len() {
            0 => Ok(None),
            n if n == of => Ok(self.after[rank].as_deref()),
            n => Err(format!("a page naming {n} cursors, for {of} stores")),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u64(self.layout).u64(self.limit).u32(self.after.len() as u32);
        for after in &self.after {
            match after {
                Some(key) => w.bool(true).bytes32(key),
                None => w.bool(false),
            };
        }
        w.into_vec()
    }

    pub fn decode(blob: &[u8]) -> Result<PageRequest, String> {
        decode_all(blob, "snapshot page", |r| {
            let layout = r.u64()?;
            let limit = r.u64()?;
            if !(1..=MAX_PAGE_ROWS).contains(&limit) {
                return Err(format!("a page of {limit} rows (1..={MAX_PAGE_ROWS})"));
            }
            let count = r.u32()? as usize;
            if count > MAX_WORKERS {
                return Err(format!("a page naming {count} cursors (at most {MAX_WORKERS})"));
            }
            let mut after = Vec::with_capacity(count);
            for _ in 0..count {
                after.push(match r.bool()? {
                    true => {
                        let key = r.bytes32()?;
                        if key.is_empty() || key.len() > MAX_PK_BYTES {
                            return Err(format!("a {}-byte key", key.len()));
                        }
                        Some(key.to_vec())
                    }
                    false => None,
                });
            }
            Ok(PageRequest { layout, limit, after })
        })
    }
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
