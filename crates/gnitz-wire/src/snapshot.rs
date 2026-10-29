//! The blob of a `SNAPSHOT_PAGE` request.

use crate::codec::{decode_all, Writer};
use crate::{PkBuf, MAX_WORKERS};

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
    pub after: Vec<Option<PkBuf>>,
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
            n if n == of => Ok(self.after[rank].as_ref().map(PkBuf::pk_bytes)),
            n => Err(format!("a page naming {n} cursors, for {of} stores")),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u64(self.layout).u64(self.limit).list(&self.after);
        w.into_vec()
    }

    pub fn decode(blob: &[u8]) -> Result<PageRequest, String> {
        decode_all(blob, "snapshot page", |r| {
            let layout = r.u64()?;
            let limit = r.u64()?;
            if !(1..=MAX_PAGE_ROWS).contains(&limit) {
                return Err(format!("a page of {limit} rows (1..={MAX_PAGE_ROWS})"));
            }
            let after = r.items("cursors", MAX_WORKERS, |r| match r.get::<Option<PkBuf>>()? {
                Some(k) if k.pk_bytes().is_empty() => Err("a 0-byte key".into()),
                k => Ok(k),
            })?;
            Ok(PageRequest { layout, limit, after })
        })
    }
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
