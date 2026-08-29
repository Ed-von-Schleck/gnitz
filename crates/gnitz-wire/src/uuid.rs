//! Canonical UUID text codec — the one format/parse pair every crate uses.
//!
//! A UUID is stored as a plain `u128` column value; only its text form is
//! defined here. `format_uuid` renders the canonical lowercase hyphenated
//! form; `parse_uuid` accepts exactly the canonical 36-char hyphenated form
//! (hyphen positions validated) or exactly 32 plain hex digits — nothing
//! else.

/// Render a UUID value in the canonical lowercase 8-4-4-4-12 hyphenated form.
pub fn format_uuid(v: u128) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (v >> 96) as u32,
        (v >> 80) as u16,
        (v >> 64) as u16,
        (v >> 48) as u16,
        v & 0x0000_ffff_ffff_ffff
    )
}

/// Parse a UUID string: the canonical 36-char hyphenated form (hyphens at
/// positions 8/13/18/23, all other chars hex) or exactly 32 plain hex digits.
/// Surrounding ASCII whitespace is trimmed. Returns `None` for anything else
/// — arbitrary hyphen placement and short hex are rejected.
pub fn parse_uuid(s: &str) -> Option<u128> {
    let s = s.trim();
    let b = s.as_bytes();
    let mut v: u128 = 0;
    let hyphenated = b.len() == 36 && b[8] == b'-' && b[13] == b'-' && b[18] == b'-' && b[23] == b'-';
    if !hyphenated && b.len() != 32 {
        return None;
    }
    for (i, &ch) in b.iter().enumerate() {
        if hyphenated && matches!(i, 8 | 13 | 18 | 23) {
            continue;
        }
        let digit = (ch as char).to_digit(16)?;
        v = (v << 4) | digit as u128;
    }
    Some(v)
}

#[cfg(test)]
#[path = "tests/uuid.rs"]
mod tests;
