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
// Matching
// ---------------------------------------------------------------------------

/// Every pattern against haystacks either side of its answer: the compiled
/// matcher, whatever shape it specializes to, and the oracle must both give the
/// verdict.
#[test]
fn each_pattern_matches_exactly_its_haystacks() {
    const E: Option<char> = Some('\\');
    let mut needle = vec![b'x'; 64];
    needle[40..46].copy_from_slice(b"nEEDLE");
    let mut broken = needle.clone();
    broken[45] = b'x';
    // (pattern, escape, ILIKE, haystack, verdict)
    type Case<'a> = (&'a str, Option<char>, bool, &'a [u8], bool);
    let cases: &[Case<'_>] = &[
        // Each specialization, both ways.
        ("abc", E, false, b"abc", true),
        ("abc", E, false, b"abcd", false),
        ("abc", E, false, b"ab", false),
        ("ab%", E, false, b"abcd", true),
        ("ab%", E, false, b"ab", true),
        ("ab%", E, false, b"ac", false),
        ("%cd", E, false, b"abcd", true),
        ("%cd", E, false, b"cd", true),
        ("%cd", E, false, b"abce", false),
        ("%bc%", E, false, b"abcd", true),
        ("%bc%", E, false, b"bc", true),
        ("%bc%", E, false, b"abd", false),
        // An occurrence overlapping a failed candidate is still found.
        ("%ab%", E, false, b"aXab", true),
        ("%ab%", E, false, b"aab", true),
        ("%aB%", E, true, b"AxaB", true),
        // `''` matches only `''`, `%` everything, and `%_%` needs one character.
        ("", E, false, b"", true),
        ("", E, false, b"a", false),
        ("%", E, false, b"", true),
        ("%", E, false, b"anything", true),
        ("%_%", E, false, b"", false),
        ("%_%", E, false, b"a", true),
        // `_` consumes one character, not one byte.
        ("_ö_", E, false, "aöb".as_bytes(), true),
        ("_", E, false, "é".as_bytes(), true),
        ("_", E, false, "éé".as_bytes(), false),
        ("_%_", E, false, b"ab", true),
        ("_%_", E, false, b"a", false),
        ("a%_%_%b", E, false, b"axyb", true),
        ("a%_%_%b", E, false, b"axb", false),
        // Backtracking, down to an `AnyOne` with no haystack left.
        ("%a_c%", E, false, b"xxabcyy", true),
        ("%a_c%", E, false, b"aabcc", true),
        ("%a_c%", E, false, b"xxabbcyy", false),
        ("%aa%aa%", E, false, b"aaaaa", true),
        ("%aa%aa%", E, false, b"aaa", false),
        ("%a_", E, false, b"xa", false),
        // A literal longer than the remaining haystack.
        ("%aBcDeF", E, true, b"ab", false),
        ("%x%aBcDeF%", E, true, b"xab", false),
        // The escape makes the next character literal, an ordinary one included.
        (r"100\%", E, false, b"100%", true),
        (r"100\%", E, false, b"1000", false),
        (r"a\_b", E, false, b"a_b", true),
        (r"a\_b", E, false, b"axb", false),
        (r"a\\b", E, false, br"a\b", true),
        (r"\a", E, false, b"a", true),
        // With escaping disabled `\` is an ordinary byte, and `%` still a wildcard.
        (r"100\%", None, false, br"100\", true),
        (r"100\%", None, false, br"100\abc", true),
        (r"100\%", None, false, b"100%", false),
        // An escape that is itself a wildcard stops being one; the other stays
        // live.
        ("%%a_", Some('%'), false, b"%ab", true),
        ("%%a_", Some('%'), false, b"%a", false),
        ("__%", Some('_'), false, b"_xyz", true),
        ("__%", Some('_'), false, b"ax", false),
        ("100§%§§", Some('§'), false, "100%§".as_bytes(), true),
        // ILIKE folds ASCII case in every shape, and ASCII only: ß is the same
        // bytes on both sides, where ẞ and SS are other text.
        ("AbC", E, true, b"aBc", true),
        ("AbC", E, true, b"aBcd", false),
        ("aB%", E, true, b"AbCd", true),
        ("%cD", E, true, b"abCD", true),
        ("%Bc%", E, true, b"aBCd", true),
        ("%a_C%", E, true, b"xxAbcyy", true),
        ("%NeEdLe%", E, true, &needle, true),
        ("%NeEdLe%", E, false, &needle, false),
        ("%NeEdLe%", E, true, &broken, false),
        ("straße%", E, true, "STRAßEN".as_bytes(), true),
        ("straße%", E, true, "STRAẞEN".as_bytes(), false),
        ("straße%", E, true, "STRASSEN".as_bytes(), false),
        // With `ESCAPE 'X'`, `aXb` is the literal `ab`; folding it must not
        // revive the `X`.
        ("aXb", Some('X'), true, b"ab", true),
        ("aXb", Some('X'), true, b"AB", true),
        ("aXb", Some('X'), true, b"axb", false),
    ];
    for &(pattern, escape, ci, h, want) in cases {
        let p = encode(pattern, escape).unwrap();
        let label = format!(
            "{pattern:?} (escape {escape:?}, ci={ci}) on {:?}",
            String::from_utf8_lossy(h)
        );
        assert_eq!(hit(&LikeMatcher::compile(&p, ci), h), want, "{label}");
        assert_eq!(ref_match(&tokenize(&p, ci), h, ci), want, "{label}: the oracle");
    }
}

#[test]
fn a_wildcard_run_is_its_underscores_then_one_skip() {
    let toks = tokenize(&raw("%_%_a"), false);
    assert!(
        matches!(toks.as_slice(), [LikeTok::AnyOne, LikeTok::AnyOne, LikeTok::SkipTo(_)]),
        "the run is its two `_`s, then its `%` joined to the literal"
    );
    agrees_with_reference(&raw("%_%a%__%"), &[b"xa", b"xaaa", "éaéé".as_bytes(), b"a"]);
}

#[test]
fn the_anchor_walk_terminates() {
    // No `b` anywhere: the anchor walks to end-of-haystack and the walk ends.
    assert!(!hit(&like("%a%b"), &[b'a'; 64]));
    // The classic backtracking-regex bait costs one anchor walk, not an
    // exponential blowup — the single anchor is why.
    assert!(!hit(&like("%a%a%a%a%a%ab"), &[b'a'; 400]));
}

// ---------------------------------------------------------------------------
// Escape
// ---------------------------------------------------------------------------

#[test]
fn an_escape_that_is_itself_a_wildcard() {
    // The escape is checked before the wildcards, so the chosen character stops
    // being one and the other stays live.
    assert_eq!(encode("%%a_", Some('%')).unwrap(), [b'%', b'a', ANY_ONE]);
    assert_eq!(encode("__%", Some('_')).unwrap(), [b'_', ANY_MANY]);
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
    assert_eq!(encode("100§%§§", Some('§')).unwrap(), "100%§".as_bytes());
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

// ---------------------------------------------------------------------------
// ILIKE
// ---------------------------------------------------------------------------

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
