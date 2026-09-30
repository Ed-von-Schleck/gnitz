use super::*;

#[test]
fn cpu_list_round_trips_and_rejects_malformed_lists_whole() {
    for (cpus, s) in [
        (vec![0, 1, 2, 3, 8, 10, 11], "0-3,8,10-11"),
        (vec![5], "5"),
        (vec![], ""),
    ] {
        assert_eq!(fmt_cpu_list(&cpus), s);
        assert_eq!(parse_cpu_list(s), cpus);
    }
    for (s, cpus) in [
        ("0-1,8,10-11\n", vec![0, 1, 8, 10, 11]),
        ("3,0-2,1", vec![0, 1, 2, 3]),
        ("0,,2", vec![0, 2]),
        // Malformed yields nothing rather than a wrong set, even beside a
        // well-formed field.
        ("3-1", vec![]),
        ("0-x", vec![]),
        ("0,x", vec![]),
    ] {
        assert_eq!(parse_cpu_list(s), cpus, "{s:?}");
    }
}

/// Adjacent-numbered SMT pairs: `{0, 1}`, `{2, 3}`, …
fn smt_pairs(c: u32) -> Vec<u32> {
    vec![c & !1, c | 1]
}

#[test]
fn handout_order_spreads_nodes_and_groups_siblings() {
    let allowed: Vec<u32> = (0..6).collect();
    // An unreadable node list is one node.
    assert_eq!(
        handout_order(&allowed, &[], smt_pairs),
        vec![vec![0, 1], vec![2, 3], vec![4, 5]]
    );
    // n0.c0, n1.c0, n0.c1, …
    assert_eq!(
        handout_order(&allowed, &[vec![0, 2, 4], vec![1, 3, 5]], |c| vec![c]),
        vec![vec![0], vec![1], vec![2], vec![3], vec![4], vec![5]]
    );
    // Inconsistent sibling lists and overlapping nodes still yield disjoint
    // cores covering `allowed`.
    let siblings = |c: u32| match c {
        0 => vec![0, 2],
        1 => vec![1, 2], // claims 2, yet files only itself
        2 => vec![0, 2],
        3 => vec![], // unreadable
        4 => vec![4],
        5 => vec![3, 5], // claims 3, yet files only itself
        _ => unreachable!(),
    };
    // Nodes both name 0 and 3; neither names 4 or 5.
    assert_eq!(
        handout_order(&allowed, &[vec![0, 1, 2, 3], vec![0, 3]], siblings),
        vec![vec![0, 2], vec![5], vec![1], vec![4], vec![3]]
    );
}

/// Each worker takes the next core; the master takes every CPU left, sorted,
/// and at least one whole core.
#[test]
fn assign_leaves_the_master_at_least_one_core() {
    let order = || vec![vec![0, 1], vec![6, 7], vec![2, 3], vec![4, 5]];
    let p = assign(order(), 3).unwrap();
    assert_eq!(p.workers, vec![vec![0, 1], vec![6, 7], vec![2, 3]]);
    assert_eq!(p.master, vec![4, 5]);
    let p = assign(order(), 1).unwrap();
    assert_eq!(p.master, vec![2, 3, 4, 5, 6, 7]);
    // 5 would fit on 8 CPUs only by splitting siblings.
    for workers in [4, 5] {
        assert!(assign(order(), workers).is_none(), "{workers} workers");
    }
}
