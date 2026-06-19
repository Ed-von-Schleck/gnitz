//! Payload null-bitmap accessors (CLAUDE.md §6).
//!
//! The null word packs one bit per *payload* column: bit `pi` is the `pi`-th
//! non-PK column in schema order (`Schema::payload_idx`). These two helpers are
//! the single read/write convention shared by the INSERT, ON-CONFLICT, residual
//! eval, projection-remap, and SET paths, replacing the open-coded
//! `(word >> pi) & 1` / `word |= 1 << pi` bit-twiddling those sites used.

/// True iff payload null-bit `pi` is set in `word` (the column is NULL).
pub(crate) fn null_word_get(word: u64, pi: usize) -> bool {
    (word >> pi) & 1 == 1
}

/// Set (`is_null == true`) or clear (`is_null == false`) payload null-bit `pi`
/// in `word`.
pub(crate) fn null_word_set(word: &mut u64, pi: usize, is_null: bool) {
    if is_null {
        *word |= 1u64 << pi;
    } else {
        *word &= !(1u64 << pi);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_set_roundtrip() {
        let mut w = 0u64;
        assert!(!null_word_get(w, 3));
        null_word_set(&mut w, 3, true);
        assert!(null_word_get(w, 3));
        assert_eq!(w, 0b1000);
        // Clearing leaves the other bits untouched.
        null_word_set(&mut w, 5, true);
        null_word_set(&mut w, 3, false);
        assert!(!null_word_get(w, 3));
        assert!(null_word_get(w, 5));
        assert_eq!(w, 0b100000);
    }
}
