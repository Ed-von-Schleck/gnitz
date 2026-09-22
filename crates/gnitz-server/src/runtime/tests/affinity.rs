use super::*;

/// No SMT: every CPU is its own core.
fn no_smt(c: u32) -> Vec<u32> {
    vec![c]
}

#[test]
fn cpu_list_parses_ranges_singletons_and_mixes() {
    assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
    assert_eq!(parse_cpu_list("0,6"), vec![0, 6]);
    assert_eq!(parse_cpu_list("0-1,8,10-11\n"), vec![0, 1, 8, 10, 11]);
    assert_eq!(parse_cpu_list(""), Vec::<u32>::new());
    assert_eq!(parse_cpu_list("3-1"), Vec::<u32>::new(), "reversed range is not a set");
    assert_eq!(
        parse_cpu_list("0-x"),
        Vec::<u32>::new(),
        "garbage yields nothing, not a wrong set"
    );
}

#[test]
fn cpu_list_formats_in_the_spelling_it_parses() {
    for (cpus, s) in [
        (vec![0, 1, 2, 3, 8, 10, 11], "0-3,8,10-11"),
        (vec![5], "5"),
        (vec![], ""),
    ] {
        assert_eq!(fmt_cpu_list(&cpus), s);
        assert_eq!(parse_cpu_list(s), cpus);
    }
}

/// Adjacent-numbered SMT pairs: `{0, 1}`, `{2, 3}`, …
fn smt_pairs(c: u32) -> Vec<u32> {
    vec![c & !1, c | 1]
}

#[test]
fn each_worker_gets_a_core_and_the_master_gets_the_rest() {
    let allowed: Vec<u32> = (0..12).collect();
    let order = handout_order(&allowed, std::slice::from_ref(&allowed), smt_pairs);
    let p = assign(order, 4).unwrap();
    assert_eq!(p.workers, vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]]);
    assert_eq!(p.master, vec![8, 9, 10, 11], "master takes both leftover cores");
}

#[test]
fn no_placement_when_the_master_would_be_left_no_core() {
    let allowed: Vec<u32> = (0..8).collect();
    let order = || handout_order(&allowed, std::slice::from_ref(&allowed), smt_pairs);
    assert_eq!(order().len(), 4);
    assert!(assign(order(), 4).is_none(), "4 workers cannot take all 4 cores");
    assert!(assign(order(), 9).is_none(), "and never by splitting siblings");
}

#[test]
fn cores_alternate_across_nodes() {
    let allowed: Vec<u32> = (0..6).collect();
    let order = handout_order(&allowed, &[vec![0, 2, 4], vec![1, 3, 5]], no_smt);
    let p = assign(order, 4).unwrap();
    assert_eq!(
        p.workers,
        vec![vec![0], vec![1], vec![2], vec![3]],
        "n0.c0, n1.c0, n0.c1, n1.c1"
    );
    assert_eq!(p.master, vec![4, 5]);
}

#[test]
fn an_unreadable_node_list_puts_every_core_in_one_node() {
    let allowed: Vec<u32> = (0..6).collect();
    assert_eq!(
        handout_order(&allowed, &[], smt_pairs),
        vec![vec![0, 1], vec![2, 3], vec![4, 5]]
    );
}

#[test]
fn an_inconsistent_topology_still_yields_disjoint_cores_covering_allowed() {
    let allowed: Vec<u32> = (0..6).collect();
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
    let order = handout_order(&allowed, &[vec![0, 1, 2, 3], vec![0, 3]], siblings);
    assert_eq!(order, vec![vec![0, 2], vec![5], vec![1], vec![4], vec![3]]);
}

#[test]
fn this_machine_reads_back_a_consistent_topology() {
    let allowed = allowed_cpus();
    assert!(!allowed.is_empty(), "sched_getaffinity must report at least one CPU");
    let mut seen: Vec<u32> = read_order(&allowed).into_iter().flatten().collect();
    seen.sort_unstable();
    assert_eq!(seen, allowed, "every allowed CPU is in exactly one core");
}
