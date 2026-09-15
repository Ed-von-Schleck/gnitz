//! The relation-descriptor blob: everything a statement needs to know about one
//! relation beyond its column layout, carried in a RESOLVE reply's control-header
//! blob alongside the schema block.
//!
//! The split from the schema block is by cost, not by category: that block is
//! built once per table, cached, and re-sent on every cold scan, seek, push and
//! SAL group, so anything added to it is paid for system-wide. This blob is
//! built per resolve and never cached, so the facts only a resolve needs — the
//! relation's kind and placement, its foreign keys, its secondary indexes —
//! ride here instead of taxing every schema block in the system.
//!
//! Layout (all little-endian):
//!
//! ```text
//! u8   version
//! u8   flags         bit 0 = replicated; bits 1..4 = the relation's class
//!                    (bit 1 = view, bit 2 = capacity-bounded, bit 3 = stream);
//!                    bit 4 = the view carries a delta feed
//! u16  fk_count
//! u16  index_count
//!      fk_count    × { u32 col_idx, u32 fk_col_idx, u64 fk_table_id }
//!      index_count × { u64 packed_cols, u64 flags (bit 0 = is_unique) }
//! ```
//!
//! An **empty** blob is the relation-absent answer — a successful reply, not an
//! error, so each caller renders whichever "not found" wording it owes. That is
//! why [`RelDescriptorBlob::decode`] returns an `Option`: absence has no fields,
//! so it gets no inhabited struct to carry meaningless ones.
//!
//! Both counts are exact — the lists are never partial, so `index_count == 0`
//! means "this relation has no index" and needs no presence bit to tell it from
//! an omission.

use crate::codec::{Reader, Writer};
use crate::{unpack_pk_cols, PkColList};

/// Blob format version. Bump only on a layout change; the decoder rejects any
/// other value rather than guessing at field offsets.
const VERSION: u8 = 1;

/// Descriptor `flags` bit 0: the relation's rows are a full copy on every worker.
const DESC_FLAG_REPLICATED: u8 = 1 << 0;
/// Descriptor `flags` bits 1..4: the [`RelClass`] encoding. Spare bits of the
/// existing byte, so no field offset moves and `VERSION` stays put.
const DESC_FLAG_VIEW: u8 = 1 << 1;
const DESC_FLAG_BOUNDED: u8 = 1 << 2;
const DESC_FLAG_STREAM: u8 = 1 << 3;
const DESC_FLAG_CLASS: u8 = DESC_FLAG_VIEW | DESC_FLAG_BOUNDED | DESC_FLAG_STREAM;
/// Descriptor `flags` bit 4: the view was created `WITH (delta = …)`, so it keeps
/// its recent deltas in a store of its own and answers a DELTA_POLL.
///
/// A bool beside the class rather than a fourth [`RelClass`] variant: the two are
/// orthogonal in principle — `capacity` and `delta` are refused together today
/// only because a bounded view hydrates from a store no tick governs — so folding
/// it in would square `View`/`BoundedView` the moment that is lifted.
/// [`RelDescriptorBlob::decode`] holds today's rule instead.
const DESC_FLAG_DELTA: u8 = 1 << 4;

/// What a relation *is* — the one vocabulary the client and the engine share for
/// this. A single value rather than three independent booleans, so the impossible
/// combinations (a bounded non-view, a view that is also a stream) cannot be built
/// on either side or arrive off the wire.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub enum RelClass {
    #[default]
    Table,
    /// A storeless, append-only ingestion point. It holds no rows, so it may be
    /// written but not read — except inside a view body, which is what it is for.
    Stream,
    View,
    /// A view created `WITH (capacity = …)`. Views may not be created over one (the
    /// leaf rule) and `ALTER VIEW … AS` may not retarget one.
    BoundedView,
}

impl RelClass {
    /// What to call this relation in a message to the user. Both view classes are
    /// "view": the rules that turn on the capacity carry their own wording.
    pub fn noun(self) -> &'static str {
        match self {
            RelClass::Table => "table",
            RelClass::Stream => "stream",
            RelClass::View | RelClass::BoundedView => "view",
        }
    }

    /// True for both view classes.
    pub fn is_view(self) -> bool {
        matches!(self, RelClass::View | RelClass::BoundedView)
    }

    fn to_flags(self) -> u8 {
        match self {
            RelClass::Table => 0,
            RelClass::Stream => DESC_FLAG_STREAM,
            RelClass::View => DESC_FLAG_VIEW,
            RelClass::BoundedView => DESC_FLAG_VIEW | DESC_FLAG_BOUNDED,
        }
    }

    fn from_flags(flags: u8) -> Result<RelClass, String> {
        match flags & DESC_FLAG_CLASS {
            0 => Ok(RelClass::Table),
            DESC_FLAG_STREAM => Ok(RelClass::Stream),
            DESC_FLAG_VIEW => Ok(RelClass::View),
            f if f == DESC_FLAG_VIEW | DESC_FLAG_BOUNDED => Ok(RelClass::BoundedView),
            f => Err(format!("rel descriptor: no relation class for flag bits {f:#04x}")),
        }
    }
}

/// Fixed header size: version + flags + the two counts.
const HEADER_LEN: usize = 6;

/// One column's foreign key: `col_idx` references `(fk_table_id, fk_col_idx)`.
/// A stored row never sets one half without the other, so the pair travels
/// together and `fk_table_id != 0` enumerates exactly the FK-carrying columns.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct RelFk {
    pub col_idx: u32,
    pub fk_col_idx: u32,
    pub fk_table_id: u64,
}

/// One secondary index: its full declared column list and whether it is unique.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct RelIndex {
    pub cols: PkColList,
    pub is_unique: bool,
}

/// The decoded descriptor blob.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RelDescriptorBlob {
    pub class: RelClass,
    /// The relation's rows are a full copy on every worker.
    pub replicated: bool,
    /// The view keeps a delta feed: a subscriber discovers the capability here
    /// instead of probing for it with a read that errors. Never set on a
    /// non-view.
    pub delta: bool,
    pub fks: Vec<RelFk>,
    pub indexes: Vec<RelIndex>,
}

impl RelDescriptorBlob {
    /// Serialise to the version-prefixed LE byte sequence above.
    ///
    /// The two counts are `u16`, which bounds what a *sender* may express.
    /// Neither list can reach that: `MAX_COLUMNS` caps the FK list at 65, and an
    /// index list long enough to overflow a `u16` would have overrun the frame
    /// payload cap long before. `debug_assert` rather than a runtime error —
    /// this is the engine's own state, not client input.
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(self.fks.len() <= u16::MAX as usize && self.indexes.len() <= u16::MAX as usize);
        let mut w = Writer::with_capacity(HEADER_LEN + 16 * (self.fks.len() + self.indexes.len()));
        debug_assert!(
            !self.delta || self.class == RelClass::View,
            "only a plain view can carry a delta feed"
        );
        let flags = self.class.to_flags()
            | if self.replicated { DESC_FLAG_REPLICATED } else { 0 }
            | if self.delta { DESC_FLAG_DELTA } else { 0 };
        w.u8(VERSION)
            .u8(flags)
            .u16(self.fks.len() as u16)
            .u16(self.indexes.len() as u16);
        for fk in &self.fks {
            w.u32(fk.col_idx).u32(fk.fk_col_idx).u64(fk.fk_table_id);
        }
        for ix in &self.indexes {
            w.u64(crate::pack_pk_cols(ix.cols.as_slice()))
                .u64(crate::IndexProps { is_unique: ix.is_unique }.pack());
        }
        w.into_vec()
    }

    /// Parse a descriptor blob at the trust boundary; `Ok(None)` is the
    /// relation-absent answer. `num_columns` is the column count of the schema
    /// block that arrived in the same frame.
    ///
    /// [`Reader`] bounds every *read*; the value bound this adds is that an FK's
    /// `col_idx` names a real column, so every consumer can index
    /// `schema.columns[col_idx]` without a bound of its own.
    pub fn decode(buf: &[u8], num_columns: usize) -> Result<Option<Self>, String> {
        if buf.is_empty() {
            return Ok(None);
        }
        let mut r = Reader::new(buf, "rel descriptor");
        let version = r.u8()?;
        if version != VERSION {
            return Err(format!("rel descriptor: unknown version {version}"));
        }
        let flags = r.u8()?;
        if flags & !(DESC_FLAG_REPLICATED | DESC_FLAG_CLASS | DESC_FLAG_DELTA) != 0 {
            return Err(format!("rel descriptor: unknown flag bits {flags:#04x}"));
        }
        let class = RelClass::from_flags(flags)?;
        let delta = flags & DESC_FLAG_DELTA != 0;
        // A plain view, not merely any view: `capacity` and `delta` are refused
        // together at the SQL layer and again at `view_registration`, so a
        // `BoundedView` carrying a feed is a combination the system does not
        // build. This is the boundary that keeps it unbuildable off the wire too.
        if delta && class != RelClass::View {
            return Err(format!(
                "rel descriptor: a delta feed on a {} names no relation",
                match class {
                    RelClass::BoundedView => "capacity-bounded view",
                    other => other.noun(),
                }
            ));
        }
        let fk_count = r.u16()? as usize;
        let index_count = r.u16()? as usize;

        let mut fks = Vec::with_capacity(fk_count.min(num_columns));
        for _ in 0..fk_count {
            let col_idx = r.u32()?;
            let fk_col_idx = r.u32()?;
            let fk_table_id = r.u64()?;
            if col_idx as usize >= num_columns {
                return Err(format!(
                    "rel descriptor: foreign key names column {col_idx}, but the relation has {num_columns} columns"
                ));
            }
            fks.push(RelFk { col_idx, fk_col_idx, fk_table_id });
        }

        // Cap the reservation by what the blob could still hold (16 bytes per
        // entry), so a corrupt count cannot drive a huge allocation before the
        // first read fails — the same bound the FK arm gets from `num_columns`.
        let mut indexes = Vec::with_capacity(index_count.min(r.remaining() / 16));
        for _ in 0..index_count {
            let cols = unpack_pk_cols(r.u64()?)
                .map_err(|rule| format!("rel descriptor: index {}", rule.for_role(crate::PkListRole::ColumnList)))?;
            let entry_flags = r.u64()?;
            indexes.push(RelIndex {
                cols,
                is_unique: crate::IndexProps::from_flags(entry_flags).is_unique,
            });
        }

        r.expect_consumed()?;
        Ok(Some(RelDescriptorBlob {
            class,
            replicated: flags & DESC_FLAG_REPLICATED != 0,
            delta,
            fks,
            indexes,
        }))
    }
}

#[cfg(test)]
#[path = "tests/rel_descriptor.rs"]
mod tests;
