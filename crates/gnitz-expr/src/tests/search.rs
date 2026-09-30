use memchr::memmem::Finder;

use super::{fields, find, find_lit};

/// Every string of up to `max` bytes over `alphabet`.
fn words(alphabet: &[u8], max: u32) -> Vec<Vec<u8>> {
    (0..=max)
        .flat_map(|len| {
            (0..alphabet.len().pow(len)).map(move |code| {
                (0..len)
                    .map(|i| alphabet[code / alphabet.len().pow(i) % alphabet.len()])
                    .collect()
            })
        })
        .collect()
}

/// Both searches against the first window equal to the needle — `Some(0)` for
/// an empty one — over every short haystack and needle of a two-letter
/// alphabet, and over each short haystack repeated past the cutoff where the
/// byte loop hands over to the vectorised scan, against needles long enough to
/// take the `memcmp` compare.
#[test]
fn find_is_the_first_window_equal_to_the_needle() {
    let first_window = |h: &[u8], n: &[u8]| match n.is_empty() {
        true => Some(0),
        false => h.windows(n.len()).position(|w| w == n),
    };
    let short = words(b"ab", 10);
    let long: Vec<Vec<u8>> = words(b"ab", 5)
        .iter()
        .filter(|h| !h.is_empty())
        .flat_map(|h| {
            let base: Vec<u8> = h.iter().copied().cycle().take(37).collect();
            [[base.as_slice(), b"abbba"].concat(), base]
        })
        .collect();
    let needles: Vec<Vec<u8>> = words(b"ab", 3)
        .into_iter()
        .chain([b"abbba".to_vec(), b"ababab".to_vec()])
        .collect();
    for n in &needles {
        let finder = Finder::new(n).into_owned();
        for h in short.iter().chain(&long) {
            let want = first_window(h, n);
            assert_eq!(find(h, n), want, "find {:?} in {:?}", n, h);
            assert_eq!(find_lit(h, &finder), want, "find_lit {:?} in {:?}", n, h);
        }
    }
}

/// The fields are `str::split`'s: left to right, non-overlapping, one field
/// when the delimiter never occurs.
#[test]
fn fields_are_the_split_on_the_delimiter() {
    for s in words(b"ab,", 7) {
        let s = String::from_utf8(s).unwrap();
        for d in [",", "a", "ab", ",,", "ba,"] {
            let got: Vec<&str> = fields(s.as_bytes(), d.as_bytes()).map(|(a, b)| &s[a..b]).collect();
            assert_eq!(got, s.split(d).collect::<Vec<_>>(), "{s:?} on {d:?}");
        }
    }
}
