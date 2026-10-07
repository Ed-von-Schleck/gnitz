//! A relation's name.

use std::hash::{Hash, Hasher};

/// A relation's schema and its name there, each a name a user may give: the
/// engine's own relations have none. The one schema it may spell that no user
/// can create is [`gnitz_wire::LOCAL_SCHEMA`], whose relations are the client's
/// own. Two spellings of one catalog name are equal.
#[derive(Clone, Debug)]
pub struct RelName {
    /// `schema.name`, folded to its catalog form.
    key: String,
    /// `schema.name` as it was spelled, the schema being the default's spelling
    /// where the name was written alone; the same length as `key`.
    spelled: String,
    /// Where the `.` is, in both.
    dot: usize,
    /// Whether the schema was written, rather than supplied as a default.
    qualified: bool,
}

impl RelName {
    /// `schema.name`, both written.
    pub fn new(schema: &str, name: &str) -> Result<Self, String> {
        Self::build(schema, name, true)
    }

    /// `name`, written alone, in `schema`.
    pub fn unqualified(schema: &str, name: &str) -> Result<Self, String> {
        Self::build(schema, name, false)
    }

    fn build(schema: &str, name: &str, qualified: bool) -> Result<Self, String> {
        // The one reserved schema a name may spell: a client's own.
        let schema_key = match schema.eq_ignore_ascii_case(gnitz_wire::LOCAL_SCHEMA) {
            true => gnitz_wire::LOCAL_SCHEMA.to_string(),
            false => gnitz_wire::canonical_identifier(schema)?,
        };
        let key = gnitz_wire::qualified_key(&schema_key, &gnitz_wire::canonical_identifier(name)?);
        Ok(RelName {
            key,
            spelled: gnitz_wire::qualified_key(schema, name),
            dot: schema.len(),
            qualified,
        })
    }

    /// `text` as `schema.name`, or as a name in `default_schema`.
    pub fn parse(default_schema: &str, text: &str) -> Result<Self, String> {
        match text.split_once('.') {
            Some((schema, name)) => Self::new(schema, name),
            None => Self::unqualified(default_schema, text),
        }
    }

    /// Whether the schema was written.
    pub fn is_qualified(&self) -> bool {
        self.qualified
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
        Self::build(&self.spelled[..self.dot], name, self.qualified)
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

/// The name as it was written: with its schema only where that was spelled.
impl std::fmt::Display for RelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.qualified {
            true => &self.spelled,
            false => self.spelled_name(),
        })
    }
}

#[cfg(test)]
#[path = "tests/rel_name.rs"]
mod tests;
