use super::*;

/// Overlapping reservations get distinct, strictly-monotone zones.
#[test]
fn reserve_is_monotone() {
    let lsns = ZoneLsnAllocator::new(100);
    assert_eq!(lsns.reserve(), 101, "first zone is high-water + 1");
    assert_eq!(lsns.reserve(), 102, "second zone steps past the first");
}

/// Publish is monotone-max: a later zone's fsync completing first must not
/// be regressed by an earlier zone's late completion.
#[test]
fn publish_is_monotone_max() {
    let lsns = ZoneLsnAllocator::new(100);
    let (a, b) = (lsns.reserve(), lsns.reserve());
    lsns.publish(b);
    assert_eq!(lsns.published(), b);
    lsns.publish(a);
    assert_eq!(lsns.published(), b, "a late lower zone never lowers the watermark");
}
