//! Catalog identifiers: the charset every stored name is drawn from, the
//! user-name rule and its canonical fold, the qualified key, and the two reserved
//! schema names.

/// The character set every stored catalog name is drawn from; the client builds
/// generated index names from it.
pub fn is_valid_ident_char(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

/// The first name `names` repeats, folded case-insensitively as SQL identifiers
/// are, and returned as it was spelled at the repeat.
pub fn first_duplicate<'a>(mut names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    names.find(|n| !seen.insert(n.to_ascii_lowercase()))
}

/// The schema the system families live in. No user can create it: it starts
/// with `_`, which [`validate_user_identifier`] refuses every name a user gives.
pub const SYSTEM_SCHEMA: &str = "_system";

/// The schema every database holds from its first boot, where an unqualified
/// name resolves unless a session names another.
pub const PUBLIC_SCHEMA: &str = "public";

/// Reject empty names, names starting with `_` (reserved for the engine's own
/// internal relation and index names) and names outside `[A-Za-z0-9_]`.
pub fn validate_user_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Identifier cannot be empty".into());
    }
    if name.starts_with('_') {
        return Err(format!(
            "User identifiers cannot start with '_' (reserved for system prefix): {name}"
        ));
    }
    if !name.bytes().all(is_valid_ident_char) {
        return Err(format!("Identifier contains invalid characters: {name}"));
    }
    Ok(())
}

/// [`validate_user_identifier`], then the canonical stored form: an ASCII
/// lowercase fold. Validating first is what makes the fold safe — over
/// `[A-Za-z0-9_]` it is injective on the classes SQL already considers equal,
/// which it is not over arbitrary bytes.
///
/// The definition of catalog-name case-insensitivity. A client's relation name
/// applies the same ASCII fold to its whole `schema.name` once both parts pass
/// [`validate_user_identifier`].
pub fn canonical_identifier(name: &str) -> Result<String, String> {
    validate_user_identifier(name)?;
    Ok(name.to_ascii_lowercase())
}

/// The name of the chain segment with view id `vid`. It is unique because vids
/// are, and no SQL surface can spell it because [`validate_user_identifier`]
/// refuses a leading `_`.
pub fn segment_name(vid: u64) -> String {
    format!("_seg{vid}")
}

/// `"schema.relation"`: the catalog key when both names are canonical. `.`
/// is outside [`is_valid_ident_char`], so no pair can produce another pair's key.
pub fn qualified_key(schema_name: &str, name: &str) -> String {
    [schema_name, ".", name].concat()
}

#[cfg(test)]
#[path = "tests/ident.rs"]
mod tests;
