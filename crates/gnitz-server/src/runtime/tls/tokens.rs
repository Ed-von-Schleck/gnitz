//! Access tokens, `gnitz_` and 64 random hex digits, and the file `--auth-tokens`
//! names: one `<name> <sha256-hex>` line per token, holding no token itself.

use std::collections::hash_map::Entry;

use ring::digest::{digest, SHA256};
use ring::rand::{SecureRandom as _, SystemRandom};
use rustc_hash::FxHashMap;

use super::config::write_file;

/// What every minted token opens with, so a secret scanner recognises a leaked one.
const TOKEN_PREFIX: &str = "gnitz_";

/// The longest token name.
const MAX_NAME_BYTES: usize = 64;

/// The tokens one read of the file holds, by digest.
#[derive(Debug, Default)]
pub(crate) struct Tokens {
    by_digest: FxHashMap<[u8; 32], String>,
}

impl Tokens {
    /// Read and check the token file at `path`. Every malformed line refuses
    /// the whole file, naming the line.
    pub(crate) fn load(path: &str) -> Result<Tokens, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("auth tokens {path:?}: {e}"))?;
        parse(&text).map_err(|e| format!("auth tokens {path:?}: {e}"))
    }

    /// The number of tokens held.
    pub(crate) fn len(&self) -> usize {
        self.by_digest.len()
    }

    /// The name `token` is held under; `None` when it is none held.
    pub(crate) fn check(&self, token: &[u8]) -> Option<&str> {
        self.by_digest.get(&sha256(token)).map(String::as_str)
    }
}

pub(super) fn sha256(bytes: &[u8]) -> [u8; 32] {
    digest(&SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("a SHA-256 digest is 32 bytes")
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// Why `name` cannot name a token, if it cannot.
fn check_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && name.bytes().all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "token name {name:?} must be 1 to {MAX_NAME_BYTES} of A-Z a-z 0-9 _ . -"
        ))
    }
}

/// The token file's lines: `<name> <sha256-hex>`, blank lines and `#` comments
/// skipped. A name or a digest held twice is refused.
fn parse(text: &str) -> Result<Tokens, String> {
    let mut tokens = Tokens::default();
    let mut names = rustc_hash::FxHashSet::default();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let at = |e: String| format!("line {}: {e}", i + 1);
        let mut fields = line.split_whitespace();
        let (Some(name), Some(hex), None) = (fields.next(), fields.next(), fields.next()) else {
            return Err(at("expected `<name> <sha256-hex>`".to_string()));
        };
        check_name(name).map_err(at)?;
        let digest = from_hex32(hex).ok_or_else(|| at(format!("{hex:?} is not 64 hex digits")))?;
        if !names.insert(name.to_string()) {
            return Err(at(format!("a token named {name:?} is already held")));
        }
        match tokens.by_digest.entry(digest) {
            Entry::Occupied(e) => return Err(at(format!("{name:?} and {:?} hold the same digest", e.get()))),
            Entry::Vacant(e) => e.insert(name.to_string()),
        };
    }
    Ok(tokens)
}

/// Mint a token named `name` into the token file at `path`, created owner-only,
/// and return it. A malformed file, or one holding `name`, is left unchanged.
pub(crate) fn mint(path: &str, name: &str) -> Result<String, String> {
    check_name(name)?;
    let mut text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(format!("auth tokens {path:?}: {e}")),
    };
    let mut secret = [0u8; 32];
    SystemRandom::new()
        .fill(&mut secret)
        .map_err(|_| "the OS random number generator failed".to_string())?;
    let token = format!("{TOKEN_PREFIX}{}", to_hex(&secret));
    // A file that does not end its last line gets one, so the new line stands alone.
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text += &format!("{name} {}\n", to_hex(&sha256(token.as_bytes())));
    // The file with the new line must load, which refuses a name already held.
    parse(&text).map_err(|e| format!("auth tokens {path:?}: {e}"))?;
    write_file(path, text.as_bytes(), 0o600)?;
    Ok(token)
}

#[cfg(test)]
#[path = "tests/tokens.rs"]
mod tests;
