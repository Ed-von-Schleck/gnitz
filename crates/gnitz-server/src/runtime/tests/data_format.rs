use super::*;

/// The data directory's format, pinned. When this fails, a change moved a word
/// a data directory's files are read by: pin the new value, and the release
/// that ships it tells its users to dump and restore, since its server refuses
/// every data directory the last one wrote.
#[test]
fn data_format_is_pinned() {
    assert_eq!(
        DATA_FORMAT, 0x19cc_28b0_cdcc_b37e,
        "the data directory format moved: {DATA_FORMAT:#018x}"
    );
}

#[test]
fn a_refusal_names_both_releases_and_the_way_across() {
    let msg = refusal("/var/lib/gnitz", &Foreign::Stamped { release: "0.0.1".into() });
    for part in [
        "gnitz 0.0.1",
        &format!("gnitz {RELEASE}"),
        "\\dump",
        "\\restore",
        "nothing in it was changed",
    ] {
        assert!(msg.contains(part), "{part:?} missing from: {msg}");
    }
}
