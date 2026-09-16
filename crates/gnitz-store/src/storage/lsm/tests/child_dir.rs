use super::*;

#[test]
fn parse_inverts_name_for_every_grammar() {
    for addr in [
        ChildAddr::Worker { rank: 0, of: 1 },
        ChildAddr::Worker { rank: 3, of: 64 },
        ChildAddr::Scratch { child: "_reduce_9_3", rank: 2 },
        ChildAddr::Index { id: 7 },
        ChildAddr::Delta { rank: 0 },
        ChildAddr::Delta { rank: 5 },
    ] {
        let name = addr.name();
        assert_eq!(ChildAddr::parse(&name), Some(addr), "round-trip of {name}");
        assert_eq!(addr.manifest("/d"), format!("/d/{name}/manifest.bin"));
    }
}

#[test]
fn parse_rejects_names_in_no_grammar() {
    for name in [
        "part_0",
        "part_255",
        "rep_3",
        "w3",
        "wof",
        "w3of",
        "wxof4",
        "w3ofx",
        "scratch_x_wq",
        "idx_",
        "idx_x",
        "delta_w",
        "delta_wx",
        "delta_0",
        "w0of0",
        "w3of3",
        "w01of2",
        "idx_07",
        "manifest.bin",
    ] {
        assert_eq!(ChildAddr::parse(name), None, "{name} is not a child dir");
    }
}

/// The resume verdict reads the launched ranks' output stores and every
/// scratch child. A name admitted here that no checkpoint round stamps would
/// carry no generation, and so invalidate its view on every boot.
#[test]
fn state_children_are_the_output_stores_and_the_scratch() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap().to_string();
    for name in [
        "w0of2",
        "w1of2",
        "scratch_agg_w0",
        "scratch_agg_w1",
        "delta_w0", // in neither checkpoint round
        "idx_7",    // no index child ever sits under a view
        "w0of4",    // the previous layout, consumed by the boot repartition
        "not_a_child",
    ] {
        std::fs::create_dir_all(format!("{dir}/{name}")).unwrap();
    }

    let mut got: Vec<String> = state_child_manifests(&dir, 2)
        .unwrap()
        .iter()
        .map(|m| {
            m.strip_prefix(&format!("{dir}/"))
                .and_then(|rest| rest.split('/').next())
                .expect("a manifest sits one level under the relation directory")
                .to_string()
        })
        .collect();
    got.sort();
    assert_eq!(got, ["scratch_agg_w0", "scratch_agg_w1", "w0of2", "w1of2"]);
}

/// An output store's manifest is enumerated whether or not its directory is
/// there — an absent one must read as a mismatch, not vanish from the verdict.
#[test]
fn state_children_name_every_launched_rank_on_an_empty_directory() {
    let tmp = tempfile::tempdir().unwrap();
    assert_eq!(state_child_manifests(tmp.path().to_str().unwrap(), 3).unwrap().len(), 3);
}

#[test]
fn ownership_follows_the_launched_count() {
    // A worker child survives iff it was laid out for exactly this count.
    assert!(ChildAddr::Worker { rank: 2, of: 3 }.is_owned_by(3));
    assert!(!ChildAddr::Worker { rank: 0, of: 2 }.is_owned_by(4));
    assert!(!ChildAddr::Worker { rank: 0, of: 8 }.is_owned_by(4));
    // Scratch is judged by rank alone.
    let scratch = |rank| ChildAddr::Scratch { child: "agg", rank };
    assert!(scratch(1).is_owned_by(2));
    assert!(!scratch(7).is_owned_by(2));
    // A delta child is judged by rank alone too — which is what makes a
    // narrowed worker count reclaim the ranks it dropped.
    assert!(ChildAddr::Delta { rank: 1 }.is_owned_by(2));
    assert!(!ChildAddr::Delta { rank: 2 }.is_owned_by(2));
}

#[test]
fn cluster_children_is_the_launched_set() {
    let got: Vec<String> = cluster_children(3).map(|c| c.name()).collect();
    assert_eq!(got, ["w0of3", "w1of3", "w2of3"]);
    assert!(cluster_children(3).all(|c| c.is_owned_by(3)));
}
