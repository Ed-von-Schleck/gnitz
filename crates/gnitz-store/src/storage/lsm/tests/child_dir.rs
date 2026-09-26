use super::*;

#[test]
fn parse_inverts_name_for_every_grammar() {
    for slot in [Slot::SOLO, Slot::new(2, 3), Slot::new(3, 64)] {
        for kind in [
            ChildKind::Rows,
            ChildKind::Scratch("_reduce_9_3"),
            ChildKind::Scratch("agg_w3"),
            ChildKind::Delta,
            ChildKind::Index(7),
        ] {
            let addr = ChildAddr { kind, slot };
            let name = addr.name();
            assert_eq!(ChildAddr::parse(&name), Some(addr), "round-trip of {name}");
            assert_eq!(addr.manifest("/d"), format!("/d/{name}/manifest.bin"));
        }
    }
}

#[test]
fn parse_rejects_names_in_no_grammar() {
    for name in [
        "foo",
        "foo_w0of1",
        "t_12",
        "w3",
        "wof",
        "w3of",
        "wxof4",
        "w3ofx",
        "scratch_x_wq",
        "scratch_x_w3",
        "scratch__w0of1x",
        "idx_",
        "idx_7",
        "idx_x_w0of1",
        "idx_-3_w0of1",
        "idx_07_w0of1",
        "delta_w",
        "delta_wx",
        "delta_w0",
        "delta_0",
        "w0of0",
        "w3of3",
        "w01of2",
        "manifest.bin",
    ] {
        assert_eq!(ChildAddr::parse(name), None, "{name} is not a child dir");
    }
}

#[test]
fn ownership_follows_the_launched_count() {
    let owned = |name: &str, n: u32| ChildAddr::parse(name).unwrap().slot.of == n;
    for name in ["w2of3", "scratch_agg_w1of3", "delta_w0of3", "idx_7_w2of3"] {
        assert!(owned(name, 3), "{name} at 3");
        assert!(!owned(name, 4), "{name} at 4");
    }
}

fn stamp(dir: &str, generation: u64) {
    std::fs::create_dir_all(dir).unwrap();
    let m = manifest::Manifest {
        stamp: manifest::ManifestStamp {
            checkpoint_gen: generation,
            ..Default::default()
        },
        run_bytes: 0,
        caller_record: Vec::new(),
        entries: Vec::new(),
    };
    manifest::prepare(dir, &manifest::encode(&m)).unwrap().commit().unwrap();
}

#[test]
fn children_at_generation_reads_the_rows_and_the_scratch() {
    const G: u64 = 5;
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap().to_string();
    assert!(!children_at_generation(&dir, 2, G), "an absent rank is a mismatch");

    for name in ["w0of2", "w1of2", "scratch_agg_w0of2", "scratch_agg_w1of2"] {
        stamp(&format!("{dir}/{name}"), G);
    }
    assert!(children_at_generation(&dir, 2, G));

    for name in ["delta_w0of2", "idx_7_w0of2", "w0of4", "not_a_child"] {
        std::fs::create_dir_all(format!("{dir}/{name}")).unwrap();
    }
    assert!(children_at_generation(&dir, 2, G), "unstamped kinds are not read");

    stamp(&format!("{dir}/scratch_agg_w1of2"), G - 1);
    assert!(
        !children_at_generation(&dir, 2, G),
        "a stale scratch child is a mismatch"
    );
}
