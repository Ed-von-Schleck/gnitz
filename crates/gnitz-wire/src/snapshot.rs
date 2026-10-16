//! The blob of a `SNAPSHOT_PAGE` request.

use crate::codec::{decode_all, Writer};
use crate::MAX_PK_BYTES;

/// The most rows one page asks a worker for.
pub const MAX_PAGE_ROWS: u64 = 1 << 20;

/// How many rows a page of a snapshot holds, what it is read after, and in
/// which layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    /// The reply layout's digest, which the pinned store's must equal.
    pub layout: u64,
    /// The rows each worker answers at most, `1..=MAX_PAGE_ROWS`.
    pub limit: u64,
    /// The OPK key the page starts strictly above; `None` from the first row.
    pub after: Option<Vec<u8>>,
}

impl PageRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u64(self.layout).u64(self.limit);
        match &self.after {
            Some(key) => w.bool(true).bytes32(key),
            None => w.bool(false),
        };
        w.into_vec()
    }

    pub fn decode(blob: &[u8]) -> Result<PageRequest, String> {
        decode_all(blob, "snapshot page", |r| {
            let layout = r.u64()?;
            let limit = r.u64()?;
            if !(1..=MAX_PAGE_ROWS).contains(&limit) {
                return Err(format!("a page of {limit} rows (1..={MAX_PAGE_ROWS})"));
            }
            let after = match r.bool()? {
                true => {
                    let key = r.bytes32()?;
                    if key.is_empty() || key.len() > MAX_PK_BYTES {
                        return Err(format!("a {}-byte key", key.len()));
                    }
                    Some(key.to_vec())
                }
                false => None,
            };
            Ok(PageRequest { layout, limit, after })
        })
    }
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
