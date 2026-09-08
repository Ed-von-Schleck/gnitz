//! Order-image tests: the byte string whose plain lexicographic order is the
//! column's typed order, and its inverse.

use super::{append_wide_image, wide_native_of_image};
use crate::schema::key::leading_u64;
use crate::schema::TypeCode;
use gnitz_wire::WideKind;

/// A wide image orders as the native value does — reversed when inverted — on
/// the whole image and on the key's [`leading_u64`] slot wherever that differs,
/// and decodes back. Byte strings cover NULs, prefixes, and values longer than
/// the slot: the cases prefix-freeness exists for.
#[test]
fn wide_image_orders_and_round_trips() {
    let strings: Vec<&[u8]> = vec![
        b"",
        b"\0",
        b"\0\0",
        b"a",
        b"a\0",
        b"a\0b",
        b"ab",
        b"abc",
        b"b",
        b"shared-prefix-1",
        b"shared-prefix-10",
        b"shared-prefix-2",
    ];
    let ints: Vec<[u8; 16]> = [i128::MIN, -1, 0, 1, i128::MAX, u64::MAX as i128 + 1]
        .into_iter()
        .map(i128::to_le_bytes)
        .collect();
    let cases: Vec<(WideKind, Vec<&[u8]>)> = vec![
        (WideKind::Bytes, strings),
        (WideKind::Fixed(TypeCode::I128), ints.iter().map(|b| &b[..]).collect()),
        (WideKind::Fixed(TypeCode::U128), ints.iter().map(|b| &b[..]).collect()),
    ];
    let mut ia = Vec::new();
    let mut ib = Vec::new();
    for (kind, vals) in cases {
        for invert in [false, true] {
            for a in &vals {
                ia.clear();
                append_wide_image(kind, invert, a, &mut ia);
                assert_eq!(&*wide_native_of_image(kind, invert, &ia), *a, "{kind:?} {a:?}");
                for b in &vals {
                    ib.clear();
                    append_wide_image(kind, invert, b, &mut ib);
                    let want = if invert {
                        kind.cmp_native(b, a)
                    } else {
                        kind.cmp_native(a, b)
                    };
                    assert_eq!(ia.cmp(&ib), want, "{kind:?} max={invert} {a:?} vs {b:?}");
                    let slot = leading_u64(&ia).cmp(&leading_u64(&ib));
                    assert!(
                        slot == want || slot.is_eq(),
                        "{kind:?} max={invert} slot {a:?} vs {b:?}"
                    );
                }
            }
        }
    }
}
