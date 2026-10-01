//! Canonical UUID text codec — the one format/parse pair every crate uses.
//!
//! A UUID is stored as a plain `u128` column value; only its text form is
//! defined here. `format_uuid` renders the canonical lowercase hyphenated
//! form; `parse_uuid` accepts exactly the canonical 36-char hyphenated form
//! (hyphen positions validated) or exactly 32 plain hex digits — nothing
//! else.

/// Render a UUID value in the canonical lowercase 8-4-4-4-12 hyphenated form.
pub fn format_uuid(v: u128) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    // Where each byte's two digits land: the hyphens sit at 8, 13, 18 and 23.
    const AT: [usize; 16] = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
    let mut out = [b'-'; 36];
    for (byte, at) in v.to_be_bytes().into_iter().zip(AT) {
        out[at] = HEX[(byte >> 4) as usize];
        out[at + 1] = HEX[(byte & 0xf) as usize];
    }
    String::from_utf8(out.to_vec()).expect("hex digits and hyphens")
}

/// Parse a UUID string: the canonical 36-char hyphenated form (hyphens at
/// positions 8/13/18/23, all other chars hex) or exactly 32 plain hex digits.
/// Surrounding whitespace is trimmed (`str::trim`, so Unicode whitespace
/// counts). Returns `None` for anything else
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
