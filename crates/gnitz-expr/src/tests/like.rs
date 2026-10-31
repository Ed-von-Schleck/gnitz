use super::*;
use crate::test_support::words;

fn encode(p: &str, escape: Option<char>) -> Option<Vec<u8>> {
    LikePattern::encode(p, escape).map(|p| p.as_bytes().to_vec())
}

fn hit(m: &LikeMatcher, h: &[u8]) -> bool {
    m.matches(h, &mut Vec::new())
}

/// The LIKE definition over the encoded pattern, sharing nothing with the
/// matcher. Exponential in the `%` count.
fn glob(p: &[u8], h: &[u8], ci: bool) -> bool {
    let fold = |b: u8| if ci { b.to_ascii_lowercase() } else { b };
    match p.split_first() {
        None => h.is_empty(),
        Some((&ANY_MANY, rest)) => (0..=h.len())
            .filter(|&i| h.get(i).is_none_or(|&b| b & 0xC0 != 0x80))
            .any(|i| glob(rest, &h[i..], ci)),
        Some((&ANY_ONE, rest)) => std::str::from_utf8(h)
            .expect("a wildcard is reached on a character boundary")
            .chars()
            .next()
            .is_some_and(|c| glob(rest, &h[c.len_utf8()..], ci)),
        Some((&b, rest)) => h.first().is_some_and(|&x| fold(x) == fold(b)) && glob(rest, &h[1..], ci),
    }
}

fn agrees(p: &[u8], h: &[u8]) {
    for ci in [false, true] {
        assert_eq!(
            hit(&LikeMatcher::compile(p, ci), h),
            glob(p, h, ci),
            "{:?} (ci={ci}) on {:?}",
            String::from_utf8_lossy(p),
            String::from_utf8_lossy(h)
        );
    }
}

/// Every string of up to `max` characters over `alphabet`.
fn strings(alphabet: &[&str], max: u32) -> Vec<String> {
    words(alphabet, max).into_iter().map(|w| w.concat()).collect()
}

/// Every pattern of up to four symbols against every haystack of up to four
/// characters, LIKE and ILIKE.
#[test]
fn every_short_pattern_agrees_with_the_definition() {
    // `É` beside `é`: ASCII folding must keep them apart.
    let haystacks = strings(&["a", "A", "b", "é", "É"], 4);
    for pattern in strings(&["a", "B", "é", "%", "_"], 4) {
        let p = encode(&pattern, None).unwrap();
        for h in &haystacks {
            agrees(&p, h.as_bytes());
        }
    }
}

/// Haystacks past the short-haystack cutoff, so the literals are searched by
/// the vectorised finder: dense false candidates, a hit at the end, a hit that
/// only folding finds, and one broken a byte before its end.
#[test]
fn long_haystacks_agree_with_the_definition() {
    let mut needle = vec![b'x'; 64];
    needle[40..46].copy_from_slice(b"nEEDLE");
    let mut broken = needle.clone();
    broken[45] = b'x';
    let dense = b"nexn".repeat(20);
    let haystacks = [
        needle,
        broken,
        [dense.as_slice(), b"nxq"].concat(),
        [b"NEXN".repeat(20).as_slice(), b"NxQ"].concat(),
        [dense.as_slice(), b"xq"].concat(),
        dense,
    ];
    for pattern in ["%NeEdLe%", "%needle", "%n_q%", "%ne%xq", "%nexn%nexn", "%x%yz", "x%x_q"] {
        for h in &haystacks {
            agrees(&encode(pattern, None).unwrap(), h);
        }
    }
}

#[test]
fn the_anchor_walk_terminates() {
    let like = |p| LikeMatcher::compile(&encode(p, None).unwrap(), false);
    // No `b` anywhere: the anchor walks to end-of-haystack and the walk ends.
    assert!(!hit(&like("%a%b"), &[b'a'; 64]));
    // The classic backtracking-regex bait costs one anchor walk, not an
    // exponential blowup — the single anchor is why.
    assert!(!hit(&like("%a%a%a%a%a%ab"), &[b'a'; 400]));
}

/// Each SQL pattern's encoding under its escape; a trailing live escape is
/// refused.
#[test]
fn each_pattern_encodes_to_its_bytes() {
    let e = Some('\\');
    for (sql, escape, want) in [
        ("a%b_c", None, Some(vec![b'a', ANY_MANY, b'b', ANY_ONE, b'c'])),
        (r"100\%", e, Some(b"100%".to_vec())),
        (r"a\_b", e, Some(b"a_b".to_vec())),
        (r"a\\b", e, Some(br"a\b".to_vec())),
        (r"\a", e, Some(b"a".to_vec())),
        ("a\\éb", e, Some("aéb".as_bytes().to_vec())),
        (r"a\", e, None),
        (r"a\", None, Some(br"a\".to_vec())),
        (r"100\%", None, Some([br"100\".as_slice(), &[ANY_MANY]].concat())),
        ("%%a_", Some('%'), Some(vec![b'%', b'a', ANY_ONE])),
        ("__%", Some('_'), Some(vec![b'_', ANY_MANY])),
        ("100§%§§", Some('§'), Some("100%§".as_bytes().to_vec())),
        ("aXb", Some('X'), Some(b"ab".to_vec())),
    ] {
        assert_eq!(encode(sql, escape), want, "{sql:?} under {escape:?}");
    }
}

/// The shape each pattern compiles to: only a pattern no anchored shape
/// expresses takes the backtracking walk.
#[test]
fn each_pattern_specializes_to_its_shape() {
    let shape = |pattern: &str| {
        let text = |l: &[u8]| String::from_utf8_lossy(l).into_owned();
        match LikeMatcher::compile(&encode(pattern, Some('\\')).unwrap(), false).kind {
            LikeKind::Exact(l) => format!("Exact({})", text(&l)),
            LikeKind::Prefix(l) => format!("Prefix({})", text(&l)),
            LikeKind::Suffix(l) => format!("Suffix({})", text(&l)),
            LikeKind::Contains(f) => format!("Contains({})", text(f.needle())),
            LikeKind::Generic(_) => "Generic".to_string(),
        }
    };
    for (pattern, want) in [
        ("", "Exact()"),
        ("abc", "Exact(abc)"),
        (r"a\%b", "Exact(a%b)"),
        ("lit%", "Prefix(lit)"),
        ("%", "Prefix()"),
        ("%lit", "Suffix(lit)"),
        ("%%a", "Suffix(a)"),
        ("%lit%", "Contains(lit)"),
        ("%%lit%%", "Contains(lit)"),
        ("a_c%", "Generic"),
        ("%_abc%", "Generic"),
        ("_", "Generic"),
        ("%_%", "Generic"),
    ] {
        assert_eq!(shape(pattern), want, "{pattern:?}");
    }
}

/// The text a pattern matches exactly, and the text it matches as a prefix.
#[test]
fn a_pattern_names_its_literal_and_its_prefix() {
    for (pattern, literal, prefix) in [
        ("", Some(""), None),
        ("abc", Some("abc"), None),
        (r"a\%b", Some("a%b"), None),
        ("lit%", None, Some("lit")),
        ("lit%%", None, Some("lit")),
        ("%", None, Some("")),
        (r"a\%%", None, Some("a%")),
        ("é%", None, Some("é")),
        ("%lit", None, None),
        ("a%b%", None, None),
        ("a_%", None, None),
        ("_", None, None),
    ] {
        let p = LikePattern::encode(pattern, Some('\\')).unwrap();
        assert_eq!((p.literal(), p.prefix()), (literal, prefix), "{pattern:?}");
    }
}

/// ILIKE folds the haystack only for the scanning shapes; an anchored one reads
/// just the literal's length of it in place.
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
        LikeMatcher::compile(&encode(pattern, None).unwrap(), true).matches(b"xAbx", &mut folded);
        assert_eq!(!folded.is_empty(), folds, "{pattern:?}");
    }
}

/// A glob's `*` and `?` are the wildcards, and LIKE's own `%` and `_` are
/// literals.
#[test]
fn a_glob_encodes_star_and_question_mark_and_keeps_the_rest_literal() {
    for (glob, want) in [
        ("ord*", vec![b'o', b'r', b'd', ANY_MANY]),
        ("?rders", [&[ANY_ONE][..], b"rders"].concat()),
        ("*", vec![ANY_MANY]),
        ("public.*", [&b"public."[..], &[ANY_MANY]].concat()),
        ("a_b%", b"a_b%".to_vec()),
    ] {
        assert_eq!(LikePattern::glob(glob).as_bytes(), want, "{glob:?}");
    }
}
