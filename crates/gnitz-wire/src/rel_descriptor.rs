//! The relation-descriptor blob: everything a statement needs to know about one
//! relation beyond its column layout, carried in a RESOLVE reply's control-block
//! `seek_pk_extra` slot alongside the schema block.
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
//!                    (bit 1 = view, bit 2 = capacity-bounded, bit 3 = stream)
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

/// Bit 0 of an index entry's `flags` word.
const INDEX_FLAG_UNIQUE: u64 = 1;

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
        let flags = self.class.to_flags() | if self.replicated { DESC_FLAG_REPLICATED } else { 0 };
        w.u8(VERSION)
            .u8(flags)
            .u16(self.fks.len() as u16)
            .u16(self.indexes.len() as u16);
        for fk in &self.fks {
            w.u32(fk.col_idx).u32(fk.fk_col_idx).u64(fk.fk_table_id);
        }
        for ix in &self.indexes {
            w.u64(crate::pack_pk_cols(ix.cols.as_slice()))
                .u64(if ix.is_unique { INDEX_FLAG_UNIQUE } else { 0 });
        }
        w.into_vec()
    }

    /// Parse a descriptor blob at the trust boundary; `Ok(None)` is the
    /// relation-absent answer. `num_columns` is the column count of the schema
    /// block that arrived in the same frame.
    ///
    /// [`Reader`] bounds every *read*; the two checks this adds bound the
    /// *values*:
    ///
    /// * an index's `packed_cols` must decode to a well-formed column list —
    ///   `PkColList::as_slice` silently clamps an out-of-range count, so without
    ///   this an over-long list reads back truncated and an empty one reads back
    ///   as zero columns;
    /// * an FK's `col_idx` must name a real column, so every consumer can index
    ///   `schema.columns[col_idx]` without a bound of its own.
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
        if flags & !(DESC_FLAG_REPLICATED | DESC_FLAG_CLASS) != 0 {
            return Err(format!("rel descriptor: unknown flag bits {flags:#04x}"));
        }
        let class = RelClass::from_flags(flags)?;
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
            fks.push(RelFk {
                col_idx,
                fk_col_idx,
                fk_table_id,
            });
        }

        // Cap the reservation by what the blob could still hold (16 bytes per
        // entry), so a corrupt count cannot drive a huge allocation before the
        // first read fails — the same bound the FK arm gets from `num_columns`.
        let mut indexes = Vec::with_capacity(index_count.min(r.remaining() / 16));
        for _ in 0..index_count {
            let cols = unpack_pk_cols(r.u64()?);
            let entry_flags = r.u64()?;
            if !cols.is_well_formed() {
                return Err(format!(
                    "rel descriptor: index column-list count {} out of range 1..={}",
                    cols.decoded_count(),
                    crate::PK_LIST_MAX_COLS
                ));
            }
            indexes.push(RelIndex {
                cols,
                is_unique: entry_flags & INDEX_FLAG_UNIQUE != 0,
            });
        }

        r.expect_consumed()?;
        Ok(Some(RelDescriptorBlob {
            class,
            replicated: flags & DESC_FLAG_REPLICATED != 0,
            fks,
            indexes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(d: &RelDescriptorBlob, num_columns: usize) -> RelDescriptorBlob {
        RelDescriptorBlob::decode(&d.encode(), num_columns)
            .expect("decode")
            .expect("present")
    }

    /// An empty blob is the absent answer, and a present one always encodes to
    /// more than that — so the two can never be confused. Both ride inline in
    /// the control block's German-string cell, costing a control-only frame.
    #[test]
    fn absence_is_the_empty_blob() {
        assert_eq!(RelDescriptorBlob::decode(&[], 0), Ok(None));
        let present = RelDescriptorBlob::default().encode();
        assert_eq!(present.len(), HEADER_LEN);
        assert_eq!(crate::control::german_spill_len(present.len()), 0);
    }

    #[test]
    fn empty_lists_roundtrip() {
        let d = RelDescriptorBlob::default();
        assert_eq!(roundtrip(&d, 3), d);
    }

    #[test]
    fn multi_column_index_and_multi_fk_roundtrip() {
        let d = RelDescriptorBlob {
            class: RelClass::BoundedView,
            replicated: true,
            fks: vec![
                RelFk {
                    col_idx: 0,
                    fk_col_idx: 0,
                    fk_table_id: 16,
                },
                RelFk {
                    col_idx: 2,
                    fk_col_idx: 1,
                    fk_table_id: 99,
                },
            ],
            indexes: vec![
                RelIndex {
                    cols: PkColList::from_slice(&[1]),
                    is_unique: true,
                },
                RelIndex {
                    cols: PkColList::from_slice(&[2, 0, 1]),
                    is_unique: false,
                },
            ],
        };
        assert_eq!(roundtrip(&d, 3), d);
    }

    /// Every class survives a roundtrip alongside `replicated`, which is orthogonal
    /// to all of them: reading `replicated` back as false on a replicated stream
    /// would make every worker hold a full copy while the client still folded
    /// per-worker partials.
    #[test]
    fn every_class_roundtrips_with_replicated() {
        for class in [RelClass::Table, RelClass::Stream, RelClass::View, RelClass::BoundedView] {
            for &replicated in &[false, true] {
                let d = RelDescriptorBlob {
                    class,
                    replicated,
                    ..Default::default()
                };
                let back = roundtrip(&d, 2);
                assert_eq!(back, d, "{class:?} replicated={replicated}");
            }
        }
    }

    /// The class bits are a closed set: a word naming no class is rejected rather
    /// than silently read as one of them.
    #[test]
    fn a_flag_word_naming_no_class_is_rejected() {
        let mut bytes = RelDescriptorBlob::default().encode();
        for bad in [DESC_FLAG_BOUNDED, DESC_FLAG_VIEW | DESC_FLAG_STREAM, DESC_FLAG_CLASS] {
            bytes[1] = bad;
            let err = RelDescriptorBlob::decode(&bytes, 4).unwrap_err();
            assert!(err.contains("no relation class"), "flags {bad:#04x} gave: {err}");
        }
    }

    /// Every non-empty prefix short of the whole must be an error — only the
    /// fully-empty blob means "absent".
    #[test]
    fn truncated_blob_is_an_error() {
        let d = RelDescriptorBlob {
            indexes: vec![RelIndex {
                cols: PkColList::single(0),
                is_unique: true,
            }],
            ..Default::default()
        };
        let bytes = d.encode();
        for cut in 1..bytes.len() {
            let err = RelDescriptorBlob::decode(&bytes[..cut], 4).unwrap_err();
            assert!(
                err.starts_with("rel descriptor:"),
                "a {cut}-byte prefix must fail as a rel descriptor, got: {err}"
            );
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes.push(0);
        let err = RelDescriptorBlob::decode(&bytes, 0).unwrap_err();
        assert!(err.contains("trailing"), "got: {err}");
    }

    #[test]
    fn unknown_version_is_rejected() {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes[0] = VERSION + 1;
        assert!(RelDescriptorBlob::decode(&bytes, 0).is_err());
    }

    /// An unknown flag bit is an error, never ignored: reading it as "not a
    /// view" would let a client INSERT into one.
    #[test]
    fn unknown_flag_bits_are_rejected() {
        let mut bytes = RelDescriptorBlob::default().encode();
        bytes[1] = 0xF0;
        let err = RelDescriptorBlob::decode(&bytes, 0).unwrap_err();
        assert!(err.contains("unknown flag bits"), "got: {err}");
    }

    /// A malformed packed column count must surface as an error, not as a
    /// silently truncated `PkColList` — the `is_well_formed` guard.
    #[test]
    fn malformed_index_column_count_is_an_error() {
        let mut bytes = RelDescriptorBlob {
            indexes: vec![RelIndex {
                cols: PkColList::single(0),
                is_unique: false,
            }],
            ..Default::default()
        }
        .encode();
        // Overwrite the packed column list with a word whose count field (bits
        // [0..4)) is past `PK_LIST_MAX_COLS`.
        let bad = (crate::pack_pk_cols(&[0, 1, 2, 3]) & !0xF) | 0xF;
        bytes[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&bad.to_le_bytes());
        let err = RelDescriptorBlob::decode(&bytes, 4).unwrap_err();
        assert!(err.contains("out of range"), "got: {err}");
    }

    /// An FK naming a column past the schema's column count is an error, so no
    /// consumer can index with it.
    #[test]
    fn out_of_range_fk_column_is_an_error() {
        let d = RelDescriptorBlob {
            fks: vec![RelFk {
                col_idx: 5,
                fk_col_idx: 0,
                fk_table_id: 16,
            }],
            ..Default::default()
        };
        let bytes = d.encode();
        assert!(RelDescriptorBlob::decode(&bytes, 6).is_ok());
        let err = RelDescriptorBlob::decode(&bytes, 5).unwrap_err();
        assert!(err.contains("foreign key names column 5"), "got: {err}");
    }
}
