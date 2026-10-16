//! `manifest.json`: what a dump holds, and the format it is written in.

use serde::{Deserialize, Serialize};

use crate::DumpError;

/// Names the files a dump is.
const FORMAT: &str = "gnitz-dump";

/// Bumped by hand for any change to what a dump's files hold or how they are
/// laid out. A restore reads every version up to this one, so a bump keeps a
/// reader for the one it replaces; it refuses a later one.
pub(crate) const VERSION: u64 = 1;

/// One base table or stream of a dump.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    pub(crate) schema: String,
    pub(crate) name: String,
    /// The rows' file, relative to the dump; `None` for a stream, which holds none.
    pub(crate) file: Option<String>,
    pub(crate) rows: u64,
    /// The last id the table's SERIAL sequence handed out; `None` for a table
    /// whose sequence handed none out, or that has none.
    pub(crate) serial: Option<u64>,
}

/// One view of a dump.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ViewEntry {
    pub(crate) schema: String,
    pub(crate) name: String,
}

/// Everything a dump holds, by name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    /// The schemas `schema.sql` creates.
    pub(crate) schemas: Vec<String>,
    /// In creation order, which is the order a table's rows are restored in.
    pub(crate) tables: Vec<TableEntry>,
    /// The views `post-data.sql` creates.
    pub(crate) views: Vec<ViewEntry>,
    /// Every index the dump creates, by name: an index name is the database's,
    /// not its relation's.
    pub(crate) indexes: Vec<String>,
}

/// `manifest.json` as written: the header, then the manifest's own fields.
#[derive(Serialize)]
struct Doc<'a> {
    format: &'static str,
    version: u64,
    gnitz: &'static str,
    #[serde(flatten)]
    manifest: &'a Manifest,
}

/// What a reader checks of `manifest.json` before reading the rest.
#[derive(Deserialize)]
struct Header {
    format: String,
    version: u64,
}

impl Manifest {
    pub(crate) fn to_json(&self) -> String {
        let doc = Doc {
            format: FORMAT,
            version: VERSION,
            gnitz: env!("CARGO_PKG_VERSION"),
            manifest: self,
        };
        serde_json::to_string_pretty(&doc).expect("a manifest serializes")
    }

    pub(crate) fn from_json(text: &str) -> Result<Manifest, DumpError> {
        let bad = |what: &str| DumpError::from(format!("manifest.json: {what}"));
        let header: Header = serde_json::from_str(text).map_err(|e| bad(&e.to_string()))?;
        if header.format != FORMAT {
            return Err(bad("not a gnitz dump"));
        }
        if !(1..=VERSION).contains(&header.version) {
            return Err(bad(&format!(
                "format version {}, this build restores 1 to {VERSION}",
                header.version
            )));
        }
        serde_json::from_str(text).map_err(|e| bad(&e.to_string()))
    }
}

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
