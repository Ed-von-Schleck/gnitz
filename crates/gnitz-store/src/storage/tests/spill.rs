use super::*;

/// 8-byte big-endian record of `v` — a single promoted-U64 index key.
fn rec(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// Drain a producer to a Vec of the `u64`s its 8-byte records encode,
/// checking `remaining` stays exact along the way.
fn drain(mut p: KeyProducer) -> Vec<u64> {
    let mut out = Vec::new();
    let mut left = p.remaining();
    while let Some(k) = p.next() {
        out.push(u64::from_be_bytes(k.try_into().unwrap()));
        left -= 1;
        assert_eq!(p.remaining(), left, "remaining must track next() exactly");
    }
    assert_eq!(left, 0);
    out
}

/// Reference: every pushed record, sorted, multiplicity intact.
fn reference_sorted(vals: &[u64]) -> Vec<u64> {
    let mut v = vals.to_vec();
    v.sort_unstable();
    v
}

#[test]
fn fast_path_no_spill_sorts_in_ram() {
    let dir = tempfile::tempdir().unwrap();
    // Budget far above the data: nothing spills.
    let mut s = SpillSort::new(dir.path().to_str().unwrap(), 8, 1 << 20);
    let vals = [5u64, 1, 9, 1, 3, 7, 2];
    for &v in &vals {
        s.push(&rec(v)).unwrap();
    }
    let p = s.finish().unwrap();
    assert!(matches!(p, KeyProducer::Fast(_)), "no spill ⇒ fast path");
    assert_eq!(drain(p), reference_sorted(&vals));
}

#[test]
fn empty_input_yields_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let s = SpillSort::new(dir.path().to_str().unwrap(), 8, 1 << 20);
    let p = s.finish().unwrap();
    assert!(matches!(p, KeyProducer::Fast(_)));
    assert_eq!(p.remaining(), 0);
    assert!(drain(p).is_empty());
}

#[test]
fn multi_run_merge_equals_reference_with_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    // Budget = 4 records (32 bytes) ⇒ a spill every 4 pushed records,
    // forcing many runs. Include duplicates (weight-style pairs and repeats).
    let mut s = SpillSort::new(dir.path().to_str().unwrap(), 8, 32);
    let vals: Vec<u64> = vec![
        50, 3, 3, 40, 12, 7, 40, 1, 99, 2, 2, 2, 60, 61, 40, 0, 100, 5, 5, 30, 31, 32, 33, 34, 35,
    ];
    for &v in &vals {
        s.push(&rec(v)).unwrap();
    }
    let p = s.finish().unwrap();
    assert!(matches!(p, KeyProducer::Merge(_)), "over budget ⇒ merge path");
    assert_eq!(
        drain(p),
        reference_sorted(&vals),
        "k-way merge must equal the sorted reference (order and multiplicity)",
    );
}

#[test]
fn duplicate_straddling_runs_is_adjacent_after_merge() {
    let dir = tempfile::tempdir().unwrap();
    // Push value X first, many distinct values to force several spills, then
    // X again — the two Xs are guaranteed to land in different runs, and the
    // merge must still bring them adjacent (what a duplicate check needs).
    let mut s = SpillSort::new(dir.path().to_str().unwrap(), 8, 32); // 4 records/run
    const X: u64 = 500_000;
    s.push(&rec(X)).unwrap();
    for v in 0..40u64 {
        s.push(&rec(v)).unwrap();
    }
    s.push(&rec(X)).unwrap();
    let out = drain(s.finish().unwrap());
    // Two Xs, and they are adjacent (X is the max here, so they trail).
    let first = out.iter().position(|&v| v == X).unwrap();
    assert_eq!(out.iter().filter(|&&v| v == X).count(), 2);
    assert_eq!(out[first], X);
    assert_eq!(out[first + 1], X, "the straddling duplicate merges adjacently");
    // And the whole stream is globally sorted with multiplicity intact.
    let mut expected: Vec<u64> = (0..40).collect();
    expected.push(X);
    expected.push(X);
    assert_eq!(out, expected);
}

#[test]
fn wide_composite_record_round_trips_through_spill() {
    let dir = tempfile::tempdir().unwrap();
    // 16-byte composite records (two u64 columns), tiny budget forces spills.
    let stride = 16usize;
    let mut s = SpillSort::new(dir.path().to_str().unwrap(), stride, 48); // 3 records/run
    let mk = |a: u64, b: u64| {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_be_bytes());
        buf[8..].copy_from_slice(&b.to_be_bytes());
        buf
    };
    // Rows sharing the leading column but differing in the trailing one are
    // distinct composites — the merge must order by the full 16 bytes.
    let rows = [(7, 3), (7, 1), (2, 9), (7, 2), (2, 9), (5, 5), (1, 1), (9, 0)];
    for &(a, b) in &rows {
        s.push(&mk(a, b)).unwrap();
    }
    let mut p = s.finish().unwrap();
    assert!(matches!(p, KeyProducer::Merge(_)));
    let mut got: Vec<(u64, u64)> = Vec::new();
    while let Some(k) = p.next() {
        assert_eq!(k.len(), 16, "composite record keeps full width through spill");
        let a = u64::from_be_bytes(k[..8].try_into().unwrap());
        let b = u64::from_be_bytes(k[8..].try_into().unwrap());
        got.push((a, b));
    }
    let mut expected = rows.to_vec();
    expected.sort_unstable();
    assert_eq!(got, expected);
}
