use super::*;

fn encode(p: &str, escape: Option<char>) -> Option<Vec<u8>> {
    LikePattern::encode(p, escape).map(|p| p.as_bytes().to_vec())
}

/// `p` encoded under the default escape, as the SQL binder supplies it.
fn enc(p: &str) -> Vec<u8> {
    encode(p, Some('\\')).unwrap()
}

/// `p` encoded with escaping disabled — for patterns that contain no escape.
fn raw(p: &str) -> Vec<u8> {
    encode(p, None).unwrap()
}

fn like(pattern: &str) -> LikeMatcher {
    LikeMatcher::compile(&enc(pattern), false)
}

fn ilike(pattern: &str) -> LikeMatcher {
    LikeMatcher::compile(&enc(pattern), true)
}

fn hit(m: &LikeMatcher, h: &[u8]) -> bool {
    m.matches(h, &mut Vec::new())
}

/// The same pattern forced through the `Generic` walk, which every
/// specialization must agree with.
fn generic_twin(pattern: &[u8], ci: bool) -> LikeMatcher {
    LikeMatcher {
        kind: LikeKind::Generic(tokenize(pattern, ci)),
        ci,
    }
}

/// The matcher shape a pattern compiles to, as text — what the specialization
/// pins assert on.
fn shape(pattern: &str) -> String {
    let text = |l: &[u8]| String::from_utf8_lossy(l).into_owned();
    match &like(pattern).kind {
        LikeKind::Exact(l) => format!("Exact({})", text(l)),
        LikeKind::Prefix(l) => format!("Prefix({})", text(l)),
        LikeKind::Suffix(l) => format!("Suffix({})", text(l)),
        LikeKind::Contains(f) => format!("Contains({})", text(f.needle())),
        LikeKind::Generic(_) => "Generic".to_string(),
    }
}

/// Assert the specialization and its `Generic` twin agree on every haystack.
fn agrees_with_generic(pattern: &str, haystacks: &[&[u8]]) {
    let (m, g) = (like(pattern), generic_twin(&enc(pattern), false));
    for h in haystacks {
        assert_eq!(
            hit(&m, h),
            hit(&g, h),
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
    assert!(hit(&like("abc"), b"abc"));
    assert!(!hit(&like("abc"), b"abcd"));
    assert!(!hit(&like("abc"), b"ab"));

    assert!(hit(&like("ab%"), b"abcd"));
    assert!(hit(&like("ab%"), b"ab"));
    assert!(!hit(&like("ab%"), b"ac"));

    assert!(hit(&like("%cd"), b"abcd"));
    assert!(hit(&like("%cd"), b"cd"));
    assert!(!hit(&like("%cd"), b"abce"));

    assert!(hit(&like("%bc%"), b"abcd"));
    assert!(hit(&like("%bc%"), b"bc"));
    assert!(!hit(&like("%bc%"), b"abd"));

    agrees_with_generic("abc", &[b"abc", b"abcd", b"ab", b""]);
    agrees_with_generic("ab%", &[b"abcd", b"ab", b"ac", b""]);
    agrees_with_generic("%cd", &[b"abcd", b"cd", b"abce", b""]);
    agrees_with_generic("%bc%", &[b"abcd", b"bc", b"abd", b""]);
}

#[test]
fn the_empty_string_and_the_bare_wildcards() {
    // `''` matches only `''`.
    assert!(hit(&like(""), b""));
    assert!(!hit(&like(""), b"a"));
    // `%` matches everything, the empty string included.
    assert!(hit(&like("%"), b""));
    assert!(hit(&like("%"), b"anything"));
    // `%_%` needs one character, so it does not match `''`.
    assert!(!hit(&like("%_%"), b""));
    assert!(hit(&like("%_%"), b"a"));
}

#[test]
fn underscore_consumes_one_character_not_one_byte() {
    assert!(hit(&like("_ö_"), "aöb".as_bytes()));
    assert!(hit(&like("_"), "é".as_bytes()));
    assert!(!hit(&like("_"), "éé".as_bytes()));
}

#[test]
fn a_wildcard_run_is_its_underscores_then_one_skip() {
    let toks = tokenize(&raw("%_%_a"), false);
    assert!(
        matches!(toks.as_slice(), [LikeTok::AnyOne, LikeTok::AnyOne, LikeTok::SkipTo(_)]),
        "the run is its two `_`s, then its `%` joined to the literal"
    );
    assert!(!hit(&like("%_%"), b""));
    assert!(hit(&like("_%_"), b"ab"));
    assert!(!hit(&like("_%_"), b"a"));
    assert!(hit(&like("a%_%_%b"), b"axyb"));
    assert!(!hit(&like("a%_%_%b"), b"axb"));
    agrees_with_reference(&raw("%_%a%__%"), &[b"xa", b"xaaa", "éaéé".as_bytes(), b"a"]);
}

#[test]
fn generic_backtracking() {
    assert!(hit(&like("%a_c%"), b"xxabcyy"));
    assert!(hit(&like("%a_c%"), b"aabcc"));
    assert!(!hit(&like("%a_c%"), b"xxabbcyy"));
    assert!(hit(&like("%aa%aa%"), b"aaaaa"));
    assert!(!hit(&like("%aa%aa%"), b"aaa"));
    // Only passes if an `AnyOne` with no haystack left backtracks to the anchor
    // instead of hard-failing.
    assert!(!hit(&like("%a_"), b"xa"));
}

#[test]
fn the_anchor_walk_terminates() {
    // No `b` anywhere: the anchor walks to end-of-haystack and the walk ends.
    assert!(!hit(&like("%a%b"), &[b'a'; 64]));
    // The classic backtracking-regex bait costs one anchor walk, not an
    // exponential blowup — the single anchor is why.
    assert!(!hit(&like("%a%a%a%a%a%ab"), &[b'a'; 400]));
}

#[test]
fn a_literal_longer_than_the_remaining_haystack_does_not_panic() {
    assert!(!hit(&ilike("%aBcDeF"), b"ab"));
    assert!(!hit(&ilike("%x%aBcDeF%"), b"xab"));
}

// ---------------------------------------------------------------------------
// Escape
// ---------------------------------------------------------------------------

#[test]
fn the_escape_makes_the_next_character_literal() {
    assert!(hit(&like(r"100\%"), b"100%"));
    assert!(!hit(&like(r"100\%"), b"1000"));
    assert!(hit(&like(r"a\_b"), b"a_b"));
    assert!(!hit(&like(r"a\_b"), b"axb"));
    assert!(hit(&like(r"a\\b"), br"a\b"));
    // Escaping an ordinary character is legal and means the character itself.
    assert!(hit(&like(r"\a"), b"a"));
}

#[test]
fn escaping_can_be_disabled() {
    let m = LikeMatcher::compile(&raw(r"100\%"), false);
    // `\` is an ordinary byte and `%` is still a wildcard.
    assert!(hit(&m, br"100\"));
    assert!(hit(&m, br"100\abc"));
    assert!(!hit(&m, b"100%"));
}

#[test]
fn an_escape_that_is_itself_a_wildcard() {
    // The escape is checked before the wildcards, so the chosen character stops
    // being one and the other stays live.
    assert_eq!(encode("%%a_", Some('%')).unwrap(), [b'%', b'a', ANY_ONE]);
    let m = LikeMatcher::compile(&encode("%%a_", Some('%')).unwrap(), false);
    assert!(hit(&m, b"%ab"));
    assert!(!hit(&m, b"%a"));

    assert_eq!(encode("__%", Some('_')).unwrap(), [b'_', ANY_MANY]);
    let m = LikeMatcher::compile(&encode("__%", Some('_')).unwrap(), false);
    assert!(hit(&m, b"_xyz"));
    assert!(!hit(&m, b"ax"));
}

#[test]
fn a_trailing_live_escape_is_refused_by_the_encoder() {
    // Two characters: the trailing `\` is live, with nothing left to make literal.
    assert_eq!(encode(r"a\", Some('\\')), None);
    // Three: the second `\` is escaped by the first, so the pattern is the
    // literal `a\` and is legal.
    assert_eq!(encode(r"a\\", Some('\\')).unwrap(), br"a\");
    // With escaping disabled nothing is ever a live escape.
    assert_eq!(encode(r"a\", None).unwrap(), br"a\");
}

#[test]
fn a_multi_byte_escape_character() {
    let p = encode("100§%§§", Some('§')).unwrap();
    assert_eq!(p, "100%§".as_bytes());
    assert!(hit(&LikeMatcher::compile(&p, false), "100%§".as_bytes()));
    // The escape makes a multi-byte character literal too.
    assert_eq!(encode("a\\éb", Some('\\')).unwrap(), "aéb".as_bytes());
}

#[test]
fn the_wildcards_encode_to_bytes_utf8_text_never_contains() {
    assert_eq!(raw("a%b_c"), [b'a', ANY_MANY, b'b', ANY_ONE, b'c']);
}

// ---------------------------------------------------------------------------
// The `Generic` walk against an oracle
// ---------------------------------------------------------------------------

/// Plain backtracking recursion, sharing none of the walk's resume rule.
/// Exponential in the `%` count, so short haystacks only.
fn ref_match(toks: &[LikeTok], h: &[u8], ci: bool) -> bool {
    match toks.first() {
        None => h.is_empty(),
        Some(LikeTok::AnyRest) => true,
        Some(LikeTok::SkipTo(l)) => {
            let l = l.needle();
            (0..=h.len().saturating_sub(l.len())).any(|k| {
                h.len() >= k + l.len()
                    && anchored_eq(&h[k..k + l.len()], l, ci)
                    && ref_match(&toks[1..], &h[k + l.len()..], ci)
            })
        }
        // The walk's own character step: the two differ only in how they
        // backtrack.
        Some(LikeTok::AnyOne) => !h.is_empty() && ref_match(&toks[1..], &h[crate::chars::char_offset(h, 1)..], ci),
        // On the unfolded haystack, so the oracle stays independent of the fold.
        Some(LikeTok::Lit(l)) => {
            let l = l.needle();
            h.len() >= l.len() && anchored_eq(&h[..l.len()], l, ci) && ref_match(&toks[1..], &h[l.len()..], ci)
        }
    }
}

/// The compiled matcher agrees with the oracle on every haystack, LIKE and ILIKE
/// alike.
fn agrees_with_reference(pattern: &[u8], haystacks: &[&[u8]]) {
    for ci in [false, true] {
        let toks = tokenize(pattern, ci);
        let m = LikeMatcher::compile(pattern, ci);
        assert!(matches!(m.kind, LikeKind::Generic(_)), "{pattern:?} is not Generic");
        for h in haystacks {
            assert_eq!(
                hit(&m, h),
                ref_match(&toks, h, ci),
                "pattern {:?} (ci={ci}) disagrees with the reference on {h:?}",
                String::from_utf8_lossy(pattern),
            );
        }
    }
}

/// Every place the anchor literal can sit.
#[test]
fn the_generic_walk_agrees_with_the_oracle() {
    // A `%` ahead of a `_`.
    agrees_with_reference(&raw("%_abc%"), &[b"abc", b"xabc", b"xxabcyy", b"", b"_abc", b"AXABC"]);
    // The anchor literal never occurs.
    agrees_with_reference(
        &raw("%foo%bar%"),
        &[b"xxxxxxxxxxxx", b"foo", b"barfoo", b"a foo b bar c", b"FOO..BAR"],
    );
    // It occurs only overlapping a partial match.
    agrees_with_reference(&raw("%ab%ab%"), &[b"aXab", b"abab", b"aabab", b"ab", b"aab", b"aAbAB"]);
    // A `_` between the anchor literal and the next `%`.
    agrees_with_reference(
        &raw("%a_c%d%"),
        &[b"abcd", b"zabczzd", b"abc", b"dabc", b"a_cd", b"ZABCZZD"],
    );
    // A trailing literal, with no `%` after it.
    agrees_with_reference(&raw("%x%yz"), &[b"xyz", b"xxyz", b"xyzz", b"yz", b"XxYZ"]);
    // Long haystacks: dense false candidates, a hit at the end, and none at all.
    let dense = b"nexn".repeat(20);
    let tail = [dense.as_slice(), b"nxq"].concat();
    let upper = [b"NEXN".repeat(20).as_slice(), b"NxQ"].concat();
    agrees_with_reference(&raw("%n_q%"), &[&dense, &tail, &upper]);
    agrees_with_reference(
        &raw("%ne%xq"),
        &[&dense, &tail, &upper, &[dense.as_slice(), b"xq"].concat()],
    );
}

/// An occurrence overlapping a failed candidate is still found.
#[test]
fn a_contains_scan_restarts_one_byte_past_a_failed_candidate() {
    agrees_with_generic("%ab%", &[b"aXab", b"aab", b"aaab", b"ab", b"a"]);
    assert!(hit(&like("%ab%"), b"aXab"));
    assert!(hit(&ilike("%aB%"), b"AxaB"));
}

// ---------------------------------------------------------------------------
// ILIKE
// ---------------------------------------------------------------------------

#[test]
fn ilike_folds_ascii_case_in_every_shape() {
    assert!(hit(&ilike("AbC"), b"aBc"));
    assert!(hit(&ilike("aB%"), b"AbCd"));
    assert!(hit(&ilike("%cD"), b"abCD"));
    assert!(hit(&ilike("%Bc%"), b"aBCd"));
    assert!(hit(&ilike("%a_C%"), b"xxAbcyy"));
    assert!(!hit(&ilike("AbC"), b"aBcd"));
}

#[test]
fn ilike_contains_over_a_long_haystack() {
    let mut h = vec![b'x'; 64];
    h[40..46].copy_from_slice(b"nEEDLE");
    assert!(hit(&ilike("%NeEdLe%"), &h));
    assert!(!hit(&like("%NeEdLe%"), &h));
    h[45] = b'x';
    assert!(!hit(&ilike("%NeEdLe%"), &h));
}

#[test]
fn only_the_scanning_shapes_fold_the_haystack() {
    for (pattern, folds) in [
        ("aB%", false),
        ("%aB", false),
        ("aB", false),
        ("%aB%", true),
        ("%a_B%", true),
    ] {
        let mut folded = Vec::new();
        ilike(pattern).matches(b"xAbx", &mut folded);
        assert_eq!(!folded.is_empty(), folds, "{pattern:?}");
    }
}

#[test]
fn ilike_folds_ascii_only() {
    // The ASCII letters fold and ß is the same byte pair on both sides.
    assert!(hit(&ilike("straße%"), "STRAßEN".as_bytes()));
    // Capital ẞ is a different sequence entirely, and SS is different text.
    assert!(!hit(&ilike("straße%"), "STRAẞEN".as_bytes()));
    assert!(!hit(&ilike("straße%"), "STRASSEN".as_bytes()));
}

#[test]
fn ilike_with_an_alphabetic_escape() {
    // With `ESCAPE 'X'`, `"aXb"` is the literal `"ab"`; folding it must not
    // revive the `X`.
    let m = LikeMatcher::compile(&encode("aXb", Some('X')).unwrap(), true);
    assert!(hit(&m, b"ab"));
    assert!(hit(&m, b"AB"));
    assert!(!hit(&m, b"axb"));
}
