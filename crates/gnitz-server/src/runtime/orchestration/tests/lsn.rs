use super::*;

/// Overlapping reservations get distinct, strictly-monotone zones; a
/// drifted family counter (floor) lifts the next zone past itself and a
/// lower floor never lowers one.
#[test]
fn reserve_is_monotone_and_dominates_the_floor() {
    let lsns = ZoneLsnAllocator::new(100);
    assert_eq!(lsns.reserve(0), 101, "first zone is high-water + 1");
    assert_eq!(lsns.reserve(0), 102, "second zone steps past the first");
    assert_eq!(lsns.reserve(150), 151, "a drifted floor lifts the zone past it");
    assert_eq!(lsns.reserve(10), 152, "a low floor never lowers the next zone");
}

/// Publish is monotone-max: a later zone's fsync completing first must not
/// be regressed by an earlier zone's late completion.
#[test]
fn publish_is_monotone_max() {
    let lsns = ZoneLsnAllocator::new(100);
    let (a, b) = (lsns.reserve(0), lsns.reserve(0));
    lsns.publish(b);
    assert_eq!(lsns.published(), b);
    lsns.publish(a);
    assert_eq!(lsns.published(), b, "a late lower zone never lowers the watermark");
}
