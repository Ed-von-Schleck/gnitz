//! A relation's name.

use std::hash::{Hash, Hasher};

/// A relation's schema and its name there, each a name a user may give: the
/// engine's own relations have none. Two spellings of one catalog name are
/// equal.
#[derive(Clone, Debug)]
pub struct RelName {
    /// `schema.name`, folded to its catalog form.
    key: String,
    /// `schema.name` as it was spelled; the same length as `key`.
    spelled: String,
    /// Where the `.` is, in both.
    dot: usize,
}

impl RelName {
    pub fn new(schema: &str, name: &str) -> Result<Self, String> {
        let key = gnitz_wire::qualified_key(
            &gnitz_wire::canonical_identifier(schema)?,
            &gnitz_wire::canonical_identifier(name)?,
        );
        Ok(RelName {
            key,
            spelled: gnitz_wire::qualified_key(schema, name),
            dot: schema.len(),
        })
    }

    /// `text` as `schema.name`, or as a name in `default_schema`.
    pub fn parse(default_schema: &str, text: &str) -> Result<Self, String> {
        let (schema, name) = text.split_once('.').unwrap_or((default_schema, text));
        Self::new(schema, name)
    }

    pub fn schema(&self) -> &str {
        &self.key[..self.dot]
    }

    pub fn name(&self) -> &str {
        &self.key[self.dot + 1..]
    }

    /// `schema.name`, as the catalog and a RESOLVE spell it.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The name alone, as it was spelled.
    pub fn spelled_name(&self) -> &str {
        &self.spelled[self.dot + 1..]
    }

    /// The same schema's relation `name`.
    pub fn sibling(&self, name: &str) -> Result<Self, String> {
        Self::new(&self.spelled[..self.dot], name)
    }
}

impl PartialEq for RelName {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for RelName {}

impl Hash for RelName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

/// `schema.name`, as it was spelled.
impl std::fmt::Display for RelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.spelled)
    }
}

#[cfg(test)]
#[path = "tests/rel_name.rs"]
mod tests;
