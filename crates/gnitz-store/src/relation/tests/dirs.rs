use super::*;
use crate::storage::flush_barrier;
use crate::test_support::{make_schema_u64_i64, scratch_table};

#[test]
fn parse_inverts_name_for_every_grammar() {
    for slot in [Slot::SOLO, Slot::new(2, 3), Slot::new(3, 64)] {
        for kind in [
            ChildKind::Rows,
            ChildKind::Scratch("_reduce_9_3"),
            ChildKind::Scratch("agg_w3"),
            ChildKind::Delta,
            ChildKind::Index(PkColList::from_slice(&[7])),
            ChildKind::Index(PkColList::from_slice(&[1, 3])),
        ] {
            let addr = ChildAddr { kind, slot };
            let name = addr.name();
            assert_eq!(ChildAddr::parse(&name), Some(addr), "round-trip of {name}");
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
        "idx_1-_w0of1",
        "idx_1-1_w0of1",
        "idx_-1_w0of1",
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

/// Publish an empty store at `dir` under checkpoint mark `generation`.
fn stamp(dir: &str, generation: u64) {
    flush_barrier([&mut scratch_table(dir, make_schema_u64_i64())], generation).unwrap();
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
