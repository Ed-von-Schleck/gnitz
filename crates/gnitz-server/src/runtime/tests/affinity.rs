use super::*;

fn nodes(shape: &[&[&[u32]]]) -> Vec<Vec<Core>> {
    shape.iter().map(|n| n.iter().map(|c| c.to_vec()).collect()).collect()
}

fn all(shape: &[Vec<Core>]) -> Vec<u32> {
    let mut v: Vec<u32> = shape.iter().flatten().flatten().copied().collect();
    v.sort_unstable();
    v
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

/// 6 cores x 2 SMT threads, one node, 4 workers: a whole core each, and the
/// master holds every CPU none of them took.
#[test]
fn each_worker_gets_a_core_and_the_master_gets_the_rest() {
    let n = nodes(&[&[&[0, 1], &[2, 3], &[4, 5], &[6, 7], &[8, 9], &[10, 11]]]);
    let p = assign(&n, &all(&n), 4).unwrap();
    assert_eq!(p.workers, vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]]);
    assert_eq!(p.master, vec![8, 9, 10, 11], "master takes both leftover cores");
}

/// No fallback tier: workers that would consume every core leave the master
/// nothing, so nothing is pinned at all.
#[test]
fn no_placement_when_the_master_would_be_left_no_core() {
    let n = nodes(&[&[&[0, 1], &[2, 3], &[4, 5], &[6, 7]]]);
    assert_eq!(
        assign(&n, &all(&n), 4).unwrap_err(),
        4,
        "4 workers cannot take all 4 cores"
    );
    assert_eq!(
        assign(&n, &all(&n), 9).unwrap_err(),
        4,
        "and never by splitting siblings"
    );
    let one = nodes(&[&[&[4, 5]]]);
    assert_eq!(
        assign(&one, &all(&one), 1).unwrap_err(),
        1,
        "a single-core cpuset seats nobody"
    );
}

#[test]
fn cores_alternate_across_nodes() {
    let n = nodes(&[&[&[0], &[2], &[4]], &[&[1], &[3], &[5]]]);
    let p = assign(&n, &all(&n), 4).unwrap();
    assert_eq!(
        p.workers,
        vec![vec![0], vec![1], vec![2], vec![3]],
        "n0.c0, n1.c0, n0.c1, n1.c1"
    );
    assert_eq!(p.master, vec![4, 5]);
}

#[test]
fn no_smt_gives_one_cpu_per_worker() {
    let n = nodes(&[&[&[0], &[1], &[2], &[3]]]);
    let p = assign(&n, &all(&n), 3).unwrap();
    assert_eq!(p.workers, vec![vec![0], vec![1], vec![2]]);
    assert_eq!(p.master, vec![3]);
}

/// An allowed CPU that no online node names is in no core, yet it is still
/// this server's to use — so it lands in the master's mask. This is why
/// `assign` takes `allowed` instead of deriving it from the cores.
#[test]
fn an_allowed_cpu_named_by_no_node_still_reaches_the_master() {
    let n = nodes(&[&[&[0, 1], &[2, 3]]]);
    let p = assign(&n, &[0, 1, 2, 3, 20, 21], 1).unwrap();
    assert_eq!(p.workers, vec![vec![0, 1]]);
    assert_eq!(p.master, vec![2, 3, 20, 21]);
}

/// The sysfs walk against this machine's real shape: no CPU is claimed by
/// two cores, and every CPU that lands in one is a CPU this server may use.
/// Not every allowed CPU need appear — one that no online node names is
/// dropped here and reaches the master through `assign`'s complement.
#[test]
fn this_machine_reads_back_a_consistent_topology() {
    let allowed = allowed_cpus();
    assert!(!allowed.is_empty(), "sched_getaffinity must report at least one CPU");
    let seen = all(&cores_by_node(&allowed));
    let mut uniq = seen.clone();
    uniq.dedup();
    assert_eq!(uniq, seen, "no CPU is claimed by two cores");
    assert!(seen.iter().all(|c| allowed.contains(c)), "every grouped CPU is allowed");
}

#[test]
fn the_boot_record_names_every_process() {
    let n = nodes(&[&[&[0, 1], &[2, 3], &[4, 5]]]);
    let p = assign(&n, &all(&n), 2).unwrap();
    assert_eq!(p.describe(), "W0 [0, 1] W1 [2, 3] master [4, 5]");
}
