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
