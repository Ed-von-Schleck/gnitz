//! A user view's definition as its planner records it in the view's catalog
//! row: the body as written, and every place the body names a catalog relation.
//!
//! The body is kept as text so a client can write the view again — `\d+`, a
//! dump — and each relation it names is kept by id, so a rename of that
//! relation or of its columns leaves the definition true. Writing the body back
//! out puts the names the catalog holds then where the original ones stood; the
//! SQL layer does that, since it owns quoting. The engine stores the encoding
//! verbatim and reads none of it.

use std::ops::Range;

use gnitz_wire::{decode_all, Reader, Wire, Writer};

/// Bumped by hand for any change to [`ViewDefinition::encode`]'s layout.
const ENCODING_VERSION: u8 = 1;

/// A list in an encoding holds at most this many entries: the most a count
/// encodes and still tells apart from one too long to count.
const MAX_ENTRIES: usize = u16::MAX as usize - 1;

/// A user view's body as written, and the relations it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewDefinition {
    /// The query after the statement's `AS`, verbatim.
    pub query: String,
    /// The names the body gives its visible output columns, before the
    /// statement's own column list renames them.
    pub body_columns: Vec<String>,
    /// Each place `query` names a catalog relation, ascending by position and
    /// disjoint.
    pub refs: Vec<RelationRef>,
}

/// One place a view body names a catalog relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationRef {
    /// The relation named.
    pub tid: u64,
    /// The byte range of the relation's name in [`ViewDefinition::query`].
    pub name: Range<u32>,
    /// The byte offset in the query just past the declared alias's name; `None`
    /// where the reference declares no alias.
    pub alias_end: Option<u32>,
    /// The name the body reads the relation by: the declared alias, else the
    /// relation's name as written.
    pub alias: String,
    /// The relation's visible column names when the view was created, which the
    /// body reads it by; `None` where the reference renames them with a column
    /// alias list of its own.
    pub columns: Option<Vec<String>>,
}

impl ViewDefinition {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(ENCODING_VERSION)
            .put(&self.query)
            .list(&self.body_columns)
            .list(&self.refs);
        w.into_vec()
    }

    /// The definition `bytes` encodes, held to [`Self::check`].
    pub fn decode(bytes: &[u8]) -> Result<ViewDefinition, String> {
        decode_all(bytes, "view definition", |r| {
            let version = r.u8()?;
            if version != ENCODING_VERSION {
                return Err(format!(
                    "encoding version {version}, this build reads {ENCODING_VERSION}"
                ));
            }
            let def = ViewDefinition {
                query: r.get()?,
                body_columns: r.list("names", MAX_ENTRIES)?,
                refs: r.list("relation references", MAX_ENTRIES)?,
            };
            def.check()?;
            Ok(def)
        })
    }

    /// The rules [`Self::encode`]'s output keeps: every range lies on character
    /// boundaries of the query, in order and apart.
    pub fn check(&self) -> Result<(), String> {
        let mut floor = 0;
        for r in &self.refs {
            let ends_in_order = r.alias_end.is_none_or(|end| end >= r.name.end);
            if r.name.start < floor || r.name.start >= r.name.end || !ends_in_order {
                return Err("relation references overlap or are out of order".to_string());
            }
            let mut edges = [r.name.start, r.name.end].into_iter().chain(r.alias_end);
            if !edges.all(|at| self.query.is_char_boundary(at as usize)) {
                return Err("a relation reference lies outside the query".to_string());
            }
            floor = r.alias_end.unwrap_or(r.name.end);
        }
        Ok(())
    }
}

impl Wire for RelationRef {
    fn write(&self, w: &mut Writer) {
        w.u64(self.tid)
            .u32(self.name.start)
            .u32(self.name.end)
            .put(&self.alias_end)
            .put(&self.alias);
        match &self.columns {
            Some(cols) => w.bool(true).list(cols),
            None => w.bool(false),
        };
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(RelationRef {
            tid: r.u64()?,
            name: r.u32()?..r.u32()?,
            alias_end: r.get()?,
            alias: r.get()?,
            columns: r.bool()?.then(|| r.list("names", MAX_ENTRIES)).transpose()?,
        })
    }
}

#[cfg(test)]
#[path = "tests/view_def.rs"]
mod tests;
