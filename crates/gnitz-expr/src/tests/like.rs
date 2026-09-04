use super::*;

/// The default escape, as the SQL binder supplies it.
const ESC: Option<u8> = Some(b'\\');

fn like(pattern: &str) -> LikeMatcher {
    LikeMatcher::compile(pattern.as_bytes(), ESC, false)
}

fn ilike(pattern: &str) -> LikeMatcher {
    LikeMatcher::compile(pattern.as_bytes(), ESC, true)
}

/// The same pattern forced through the `Generic` walk, so every specialization
/// can be asserted equal to the walk it stands in for rather than merely
/// self-consistent.
fn generic_twin(pattern: &[u8], escape: Option<u8>, ci: bool) -> LikeMatcher {
    let mut toks = tokenize(pattern, escape).0;
    if ci {
        fold_lits(&mut toks);
    }
    LikeMatcher { kind: LikeKind::Generic(toks), ci }
}

/// The matcher shape a pattern compiles to, as text — what the specialization
/// pins assert on.
fn shape(pattern: &str) -> String {
    let text = |l: &[u8]| String::from_utf8_lossy(l).into_owned();
    match &like(pattern).kind {
        LikeKind::Exact(l) => format!("Exact({})", text(l)),
        LikeKind::Prefix(l) => format!("Prefix({})", text(l)),
        LikeKind::Suffix(l) => format!("Suffix({})", text(l)),
        LikeKind::Contains(l) => format!("Contains({})", text(l)),
        LikeKind::Generic(_) => "Generic".to_string(),
    }
}

/// Assert the specialization and its `Generic` twin agree on every haystack.
fn agrees_with_generic(pattern: &str, haystacks: &[&[u8]]) {
    let (m, g) = (like(pattern), generic_twin(pattern.as_bytes(), ESC, false));
    for h in haystacks {
        assert_eq!(
            m.matches(h),
            g.matches(h),
            "pattern {pattern:?} disagrees with its Generic twin on {h:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Specialization
// ---------------------------------------------------------------------------

#[test]
fn specialization_table_matches_the_token_sequence() {
    assert_eq!(shape(""), "Exact()");
    assert_eq!(shape("abc"), "Exact(abc)");
    assert_eq!(shape("lit%"), "Prefix(lit)");
    assert_eq!(shape("%"), "Prefix()");
    assert_eq!(shape("%lit"), "Suffix(lit)");
    assert_eq!(shape("%lit%"), "Contains(lit)");
}

#[test]
fn a_wildcard_inside_a_run_stays_generic() {
    // Never `Prefix("a_c")` — the `_` is a token, not a literal byte.
    assert_eq!(shape("a_c%"), "Generic");
    // Never `Contains("abc")`, which would wrongly accept `"abc"` itself.
    assert_eq!(shape("%_abc%"), "Generic");
    assert_eq!(shape("_"), "Generic");
    assert_eq!(shape("%_%"), "Generic");
}

#[test]
fn redundant_spellings_still_reach_the_table() {
    // Adjacent `%` collapse …
    assert_eq!(shape("%%a"), "Suffix(a)");
    assert_eq!(shape("%%lit%%"), "Contains(lit)");
    // … and adjacent literal bytes merge into one run.
    assert_eq!(shape(r"\%"), "Exact(%)");
    assert_eq!(shape(r"a\%b"), "Exact(a%b)");
}

// ---------------------------------------------------------------------------
// Matching, per shape
// ---------------------------------------------------------------------------

#[test]
fn each_shape_hits_and_misses() {
    assert!(like("abc").matches(b"abc"));
    assert!(!like("abc").matches(b"abcd"));
    assert!(!like("abc").matches(b"ab"));

    assert!(like("ab%").matches(b"abcd"));
    assert!(like("ab%").matches(b"ab"));
    assert!(!like("ab%").matches(b"ac"));

    assert!(like("%cd").matches(b"abcd"));
    assert!(like("%cd").matches(b"cd"));
    assert!(!like("%cd").matches(b"abce"));

    assert!(like("%bc%").matches(b"abcd"));
    assert!(like("%bc%").matches(b"bc"));
    assert!(!like("%bc%").matches(b"abd"));

    agrees_with_generic("abc", &[b"abc", b"abcd", b"ab", b""]);
    agrees_with_generic("ab%", &[b"abcd", b"ab", b"ac", b""]);
    agrees_with_generic("%cd", &[b"abcd", b"cd", b"abce", b""]);
    agrees_with_generic("%bc%", &[b"abcd", b"bc", b"abd", b""]);
}

#[test]
fn the_empty_string_and_the_bare_wildcards() {
    // `''` matches only `''`.
    assert!(like("").matches(b""));
    assert!(!like("").matches(b"a"));
    // `%` matches everything, the empty string included.
    assert!(like("%").matches(b""));
    assert!(like("%").matches(b"anything"));
    // `%_%` needs one character, so it does not match `''`.
    assert!(!like("%_%").matches(b""));
    assert!(like("%_%").matches(b"a"));
}

#[test]
fn underscore_consumes_one_character_not_one_byte() {
    assert!(like("_ö_").matches("aöb".as_bytes()));
    assert!(like("_").matches("é".as_bytes()));
    assert!(!like("_").matches("éé".as_bytes()));
    // On invalid UTF-8 `_` inherits SUBSTRING's rule: a lone continuation byte
    // starts no character, so it is consumed together with the byte after it.
    assert!(like("_").matches(&[0x80, 0x41]));
}

#[test]
fn generic_backtracking() {
    assert!(like("%a_c%").matches(b"xxabcyy"));
    assert!(like("%a_c%").matches(b"aabcc"));
    assert!(!like("%a_c%").matches(b"xxabbcyy"));
    assert!(like("%aa%aa%").matches(b"aaaaa"));
    assert!(!like("%aa%aa%").matches(b"aaa"));
    // Only passes if an `AnyOne` with no haystack left backtracks to the anchor
    // instead of hard-failing.
    assert!(!like("%a_").matches(b"xa"));
}

#[test]
fn the_anchor_walk_terminates() {
    // No `b` anywhere: the anchor walks to end-of-haystack and the walk ends.
    assert!(!like("%a%b").matches(&[b'a'; 64]));
    // The classic backtracking-regex bait costs one anchor walk, not an
    // exponential blowup — the single anchor is why.
    assert!(!like("%a%a%a%a%a%ab").matches(&[b'a'; 400]));
}

// ---------------------------------------------------------------------------
// The byte-granular anchor
// ---------------------------------------------------------------------------

#[test]
fn the_anchor_advances_by_one_byte() {
    // A boundary-aligned retry never proposes position 1 here, so it would miss
    // a match the specializations see.
    assert!(like("%A%B%").matches(&[0x41, 0x93, 0x42]));
    agrees_with_generic("%ab", &[&[0x80, b'a', b'b']]);
    agrees_with_generic("%a%", &[&[0x80, b'a', b'b']]);
}

#[test]
fn a_literal_starting_mid_character_agrees_with_its_specialization() {
    // A literal run that *begins* with a continuation byte: the walk and
    // `Contains` must answer alike, and both must find the match at offset 1.
    let h = [0x41u8, 0x93, 0x42];
    assert!(LikeMatcher::compile(b"%\x93%B", ESC, false).matches(&h));
    assert!(LikeMatcher::compile(b"%\x93%", ESC, false).matches(&h));
}

#[test]
fn a_literal_longer_than_the_remaining_haystack_does_not_panic() {
    // The `Lit` arm's length guard, under the case-insensitive comparator.
    assert!(!ilike("%aBcDeF").matches(b"ab"));
    assert!(!ilike("%x%aBcDeF%").matches(b"xab"));
}

// ---------------------------------------------------------------------------
// Escape
// ---------------------------------------------------------------------------

#[test]
fn the_escape_makes_the_next_character_literal() {
    assert!(like(r"100\%").matches(b"100%"));
    assert!(!like(r"100\%").matches(b"1000"));
    assert!(like(r"a\_b").matches(b"a_b"));
    assert!(!like(r"a\_b").matches(b"axb"));
    assert!(like(r"a\\b").matches(br"a\b"));
    // Escaping an ordinary character is legal and means the character itself.
    assert!(like(r"\a").matches(b"a"));
}

#[test]
fn escaping_can_be_disabled() {
    let m = LikeMatcher::compile(br"100\%", None, false);
    // `\` is an ordinary byte and `%` is still a wildcard.
    assert!(m.matches(br"100\"));
    assert!(m.matches(br"100\abc"));
    assert!(!m.matches(b"100%"));
}

#[test]
fn an_escape_that_is_itself_a_wildcard() {
    // The escape is dispatched before the wildcards, so the chosen byte stops
    // being one and the other stays live.
    let m = LikeMatcher::compile(b"%%a_", Some(b'%'), false);
    assert!(m.matches(b"%ab"));
    assert!(!m.matches(b"%a"));

    let m = LikeMatcher::compile(b"__%", Some(b'_'), false);
    assert!(m.matches(b"_xyz"));
    assert!(!m.matches(b"ax"));
}

#[test]
fn a_trailing_live_escape_stays_total() {
    // The binder rejects this shape; the tokenizer must still answer, taking the
    // escape byte as a literal of its own.
    assert!(like(r"ab\").matches(br"ab\"));
    assert!(!like(r"ab\").matches(b"ab"));
}

#[test]
fn the_trailing_escape_rule_is_the_tokenizer_walk() {
    // Two bytes: the trailing `\` is live.
    assert!(like_pattern_ends_with_live_escape(br"a\", ESC));
    // Three bytes: the second `\` is escaped by the first, so the pattern is the
    // literal `a\` and is legal. A `ends_with` check gets exactly this wrong.
    assert!(!like_pattern_ends_with_live_escape(br"a\\", ESC));
    // With escaping disabled nothing is ever a live escape.
    assert!(!like_pattern_ends_with_live_escape(br"a\", None));
}

// ---------------------------------------------------------------------------
// The `Generic` anchor jump
// ---------------------------------------------------------------------------

/// An independent oracle for the walk: plain backtracking recursion, which
/// shares none of the anchored walk's resume rule and so cannot be wrong the
/// same way `generic_twin` would be. Exponential in the `%` count, which is why
/// it stays here and takes only short haystacks.
fn ref_match(toks: &[LikeTok], h: &[u8], ci: bool) -> bool {
    match toks.first() {
        None => h.is_empty(),
        Some(LikeTok::AnyMany) => (0..=h.len()).any(|k| ref_match(&toks[1..], &h[k..], ci)),
        // The same character step the walk takes, so the oracle differs from it
        // only in how it backtracks.
        Some(LikeTok::AnyOne) => !h.is_empty() && ref_match(&toks[1..], &h[crate::chars::char_offset(h, 0, 1)..], ci),
        Some(LikeTok::Lit(l)) => {
            h.len() >= l.len() && lit_eq(&h[..l.len()], l, ci) && ref_match(&toks[1..], &h[l.len()..], ci)
        }
    }
}

/// Assert the compiled matcher and the oracle agree on every haystack, LIKE and
/// ILIKE alike — the second is what drives `find`'s two-candidate scan.
fn agrees_with_reference(pattern: &[u8], haystacks: &[&[u8]]) {
    for ci in [false, true] {
        let mut toks = tokenize(pattern, ESC).0;
        if ci {
            fold_lits(&mut toks);
        }
        let m = LikeMatcher::compile(pattern, ESC, ci);
        for h in haystacks {
            assert_eq!(
                m.matches(h),
                ref_match(&toks, h, ci),
                "pattern {:?} (ci={ci}) disagrees with the reference on {h:?}",
                String::from_utf8_lossy(pattern),
            );
        }
    }
}

/// Every shape the anchor can resume into. `agrees_with_generic` cannot cover
/// these — they *are* `Generic` — so the oracle above stands in for the
/// byte-at-a-time slide the literal jump replaced.
#[test]
fn the_generic_anchor_jump_answers_as_the_byte_slide_did() {
    // The anchor token is `AnyOne`, not a literal, so no jump is taken at all.
    agrees_with_reference(b"%_abc%", &[b"abc", b"xabc", b"xxabcyy", b"", b"_abc", b"AXABC"]);
    // The anchor literal never occurs anywhere: the jump answers `false` at once
    // rather than sliding to the end.
    agrees_with_reference(
        b"%foo%bar%",
        &[b"xxxxxxxxxxxx", b"foo", b"barfoo", b"a foo b bar c", b"FOO..BAR"],
    );
    // It occurs only past a partial match, so the scan must restart one byte
    // past each failed candidate rather than past the whole needle.
    agrees_with_reference(b"%ab%ab%", &[b"aXab", b"abab", b"aabab", b"ab", b"aab", b"aAbAB"]);
    // Two literals either side of a `_`, so the token the anchor resumes on
    // alternates between a literal and a wildcard.
    agrees_with_reference(b"%a_c%d%", &[b"abcd", b"zabczzd", b"abc", b"dabc", b"a_cd", b"ZABCZZD"]);
    // A trailing literal, which resumes with no `%` left behind it.
    agrees_with_reference(b"%x%yz", &[b"xyz", b"xxyz", b"xyzz", b"yz", b"XxYZ"]);
}

/// `%` is byte-granular, and the anchor jump must stay so: it lands on
/// `find`'s byte offsets, which are not character boundaries on bytes that are
/// not valid UTF-8.
#[test]
fn the_anchor_jump_stays_byte_granular_on_non_utf8() {
    // A lone continuation byte has no character start at all, and a truncated
    // multi-byte sequence puts the literal mid-character.
    let bytes: [&[u8]; 6] = [
        &[0x80, b'a', b'b', 0x80, b'a', b'b'],
        &[0xE2, 0x82, b'a', b'b'],
        &[0xFF, 0xFE, b'a', b'b', 0xFF],
        b"ab",
        &[0x80],
        &[],
    ];
    agrees_with_reference(b"%ab%b%", &bytes);
    agrees_with_reference(b"%_ab%", &bytes);
    agrees_with_reference(&[b'%', 0x80, b'%', b'a', b'%'], &bytes);
}

/// `find` restarts one byte past a failed candidate, not one needle past it, so
/// an occurrence overlapping a partial match is still found.
#[test]
fn a_contains_scan_restarts_one_byte_past_a_failed_candidate() {
    agrees_with_generic("%ab%", &[b"aXab", b"aab", b"aaab", b"ab", b"a"]);
    assert!(like("%ab%").matches(b"aXab"));
    assert!(ilike("%aB%").matches(b"AxaB"));
}

// ---------------------------------------------------------------------------
// ILIKE
// ---------------------------------------------------------------------------

#[test]
fn ilike_folds_ascii_case_in_every_shape() {
    assert!(ilike("AbC").matches(b"aBc"));
    assert!(ilike("aB%").matches(b"AbCd"));
    assert!(ilike("%cD").matches(b"abCD"));
    assert!(ilike("%Bc%").matches(b"aBCd"));
    assert!(ilike("%a_C%").matches(b"xxAbcyy"));
    assert!(!ilike("AbC").matches(b"aBcd"));
}

#[test]
fn ilike_folds_ascii_only() {
    // The ASCII letters fold and ß is the same byte pair on both sides.
    assert!(ilike("straße%").matches("STRAßEN".as_bytes()));
    // Capital ẞ is a different sequence entirely, and SS is different text.
    assert!(!ilike("straße%").matches("STRAẞEN".as_bytes()));
    assert!(!ilike("straße%").matches("STRASSEN".as_bytes()));
}

#[test]
fn ilike_folds_after_tokenizing() {
    // With `ESCAPE 'X'`, `"aXb"` is the literal `"ab"`. Folding the pattern
    // first would make it `"axb"`, in which the tokenizer finds no escape at all.
    let m = LikeMatcher::compile(b"aXb", Some(b'X'), true);
    assert!(m.matches(b"ab"));
    assert!(m.matches(b"AB"));
    assert!(!m.matches(b"axb"));
}
