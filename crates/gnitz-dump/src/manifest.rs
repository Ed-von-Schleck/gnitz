//! `manifest.json`: what a dump holds, and the format it is written in.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::DumpError;

/// Names the files a dump is.
const FORMAT: &str = "gnitz-dump";

/// Bumped by hand for any change to what a dump's files hold or how they are
/// laid out; a restore refuses every other.
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
    /// with no sequence or one that handed none out.
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
}

impl Manifest {
    pub(crate) fn to_json(&self) -> String {
        let mut doc = serde_json::to_value(self).expect("a manifest serializes");
        let header = json!({ "format": FORMAT, "version": VERSION, "gnitz": env!("CARGO_PKG_VERSION") });
        if let (Value::Object(doc), Value::Object(header)) = (&mut doc, header) {
            doc.extend(header);
        }
        serde_json::to_string_pretty(&doc).expect("a manifest serializes")
    }

    pub(crate) fn from_json(text: &str) -> Result<Manifest, DumpError> {
        let bad = |what: &str| DumpError::from(format!("manifest.json: {what}"));
        let doc: Value = serde_json::from_str(text).map_err(|e| bad(&e.to_string()))?;
        if doc["format"] != FORMAT {
            return Err(bad("not a gnitz dump"));
        }
        let version = doc["version"].as_u64().ok_or_else(|| bad("no version"))?;
        if version != VERSION {
            return Err(bad(&format!("format version {version}, this build restores {VERSION}")));
        }
        serde_json::from_value(doc).map_err(|e| bad(&e.to_string()))
    }
}

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
