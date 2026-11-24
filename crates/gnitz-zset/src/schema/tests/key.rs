use super::*;
use gnitz_foundation::rng::Rng;
use std::collections::HashMap;

/// Distinct keys of one width at most [`PROBE_KEY_INJECTIVE_BYTES`] never share a
/// `probe_key`: every key of one and two bytes, and for each wider width the
/// keys that differ in one byte from a random base, every byte position and
/// value.
#[test]
fn probe_key_is_injective_up_to_its_injective_width() {
    let mut rng = Rng::new(0x1A1E_C71F);
    for stride in 1..=PROBE_KEY_INJECTIVE_BYTES {
        let keys: Vec<Vec<u8>> = match stride {
            1 => (0..=u8::MAX).map(|b| vec![b]).collect(),
            2 => (0..=u16::MAX).map(|k| k.to_be_bytes().to_vec()).collect(),
            _ => {
                let base = rng.next_u64().to_be_bytes()[..stride].to_vec();
                (0..stride)
                    .flat_map(|at| (0..=u8::MAX).map(move |b| (at, b)))
                    .map(|(at, b)| {
                        let mut key = base.clone();
                        key[at] = b;
                        key
                    })
                    .collect()
            }
        };
        let mut seen = HashMap::new();
        for k in keys {
            if let Some(other) = seen.insert(probe_key(&k), k.clone()) {
                assert_eq!(other, k, "stride {stride}: two keys share a probe_key");
            }
        }
    }
}

/// `group_floor` is a group zero-padded to the stride and `key_above` the group
/// plus one, padded alike — the carry running left — for every two-byte group,
/// at strides that pad it by nothing, a byte, and past 16 bytes; no key lies
/// above an all-`0xFF` or an empty group.
#[test]
fn group_floor_and_key_above_are_the_group_and_its_successor_padded() {
    for stride in [2, 3, 20] {
        let padded = |g: u16| {
            let mut k = g.to_be_bytes().to_vec();
            k.resize(stride, 0);
            k
        };
        for g in 0..=u16::MAX {
            let group = g.to_be_bytes();
            assert_eq!(
                group_floor(&group, stride).pk_bytes(),
                padded(g),
                "{g:#x} stride {stride}"
            );
            assert_eq!(
                key_above(&group, stride).map(|k| k.pk_bytes().to_vec()),
                g.checked_add(1).map(padded),
                "{g:#x} stride {stride}"
            );
        }
    }
    assert_eq!(key_above(&[], 4), None);
}

/// `group_band(first, last)` holds exactly the two-byte keys whose leading byte
/// is in `[first, last]`, and is `None` when no group is.
#[test]
fn group_band_holds_exactly_the_keys_of_its_groups() {
    let edges = [0u8, 1, 0x7F, 0x80, 0xFE, 0xFF];
    for first in edges {
        for last in edges {
            let band = group_band(&[first], &[last], 2);
            assert_eq!(band.is_none(), first > last, "[{first:#x}, {last:#x}]");
            for key in (0..=u16::MAX).map(u16::to_be_bytes) {
                let inside = band.as_ref().is_some_and(|(start, end)| {
                    start.pk_bytes() <= &key[..] && end.as_ref().is_none_or(|e| key[..] < *e.pk_bytes())
                });
                assert_eq!(
                    inside,
                    (first..=last).contains(&key[0]),
                    "[{first:#x}, {last:#x}] {key:?}"
                );
            }
        }
    }
}
