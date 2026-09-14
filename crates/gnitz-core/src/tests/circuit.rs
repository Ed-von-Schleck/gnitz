use super::*;

/// `base → seg → final` lowered with a symbolic upstream substitutes to the
/// circuit inline real-id allocation would have produced, and leaves the
/// base-table id alone — an id absent from the map passes through.
#[test]
fn resolve_seg_ids_reproduces_inline_allocation() {
    let (base, seg_real) = (100u64, 4096u64);
    let seg_a = segment_id(0);
    let chain = |seg: u64| {
        let mut cb = CircuitBuilder::new();
        let up = cb.input_delta(base, None);
        let inp = cb.input_delta(seg, None);
        cb.sink(up);
        cb.sink(inp);
        cb.build()
    };

    let mut symbolic = chain(seg_a);
    symbolic
        .resolve_seg_ids(&HashMap::from([(seg_a, seg_real)]))
        .expect("every tag is in the map");

    let inline = chain(seg_real);
    assert_eq!(symbolic, inline, "substitution must reproduce the inline circuit");
    assert_eq!(inline.dependencies(), vec![base, seg_real]);
}

/// One bound survives `build`: the first, and only on a source scanned once.
#[test]
fn build_keeps_one_bound_on_a_source_scanned_once() {
    let bound = || {
        Some(gnitz_wire::IndexBound {
            idx_cols: gnitz_wire::PkColList::from_slice(&[2]),
            desc: gnitz_wire::RangeDescriptor::new(&[5], gnitz_wire::Cut::Before(0), gnitz_wire::Cut::After(0)),
        })
    };
    let bounds = |scans: &[(u64, bool)]| -> Vec<bool> {
        let mut cb = CircuitBuilder::new();
        for &(source, bounded) in scans {
            let scan = cb.input_delta(source, bounded.then(bound).flatten());
            cb.sink(scan);
        }
        cb.build()
            .nodes
            .values()
            .filter_map(|op| match op {
                OpNode::ScanDelta { bound, .. } => Some(bound.is_some()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(bounds(&[(1, true), (2, false)]), [true, false]);
    assert_eq!(bounds(&[(1, true), (2, true)]), [true, false], "the first bound wins");
    assert_eq!(
        bounds(&[(1, true), (1, false)]),
        [false, false],
        "a source scanned twice shares one backfill"
    );
}

/// A tag that survives the substitution is rejected, whether the map is empty
/// or merely missing that one entry.
#[test]
fn resolve_seg_ids_rejects_a_surviving_tag() {
    let tagged = |source: u64| {
        let mut cb = CircuitBuilder::new();
        let up = cb.input_delta(source, None);
        cb.sink(up);
        cb.build()
    };
    assert!(
        tagged(segment_id(0)).resolve_seg_ids(&HashMap::new()).is_err(),
        "an empty map resolves nothing"
    );

    // A tagged source absent from a non-empty map: the lowering path that mints
    // a segment id and forgets to register it.
    let err = tagged(segment_id(7))
        .resolve_seg_ids(&HashMap::from([(segment_id(0), 17u64)]))
        .expect_err("tagged source");
    assert!(
        format!("{err}").contains(&segment_id(7).to_string()),
        "the error names the unresolved id: {err}"
    );
}
