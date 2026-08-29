use super::*;
use gnitz_wire::type_code;

fn empty_prog() -> ExprProgram {
    ExprProgram {
        num_regs: 0,
        result_reg: 0,
        code: Vec::new(),
        const_strings: Vec::new(),
    }
}

/// The reindex target list a node carries, built through the public builder.
fn built_target_tcs(reindex_cols: &[usize], target_tcs: &[u8]) -> Vec<u8> {
    let mut cb = CircuitBuilder::new(7, 100);
    let input = cb.input_delta();
    let nid = cb.map_reindex(input, reindex_cols, target_tcs, empty_prog(), ReindexRole::ScatterKey);
    match cb.build().nodes.remove(&nid) {
        Some(OpNode::Map(MapKind::Reindex { reindex_target_tcs, .. })) => reindex_target_tcs,
        other => panic!("expected Map(Reindex), got {other:?}"),
    }
}

/// `&[]` is the spelling for "nothing promoted", and the builder expands it
/// to the parallel all-zero vector the encoding needs — so no call site
/// carries a length it could get wrong. A stated list is carried verbatim.
#[test]
fn promotion_targets_are_parallel_to_the_key_columns() {
    assert_eq!(
        built_target_tcs(&[2, 5], &[]),
        vec![0, 0],
        "an absent list expands to one zero per key column"
    );
    assert_eq!(built_target_tcs(&[2, 5], &[0, type_code::I64]), vec![0, type_code::I64]);
}

/// A stated list of the wrong length is a caller bug, and fails at the
/// caller's own site rather than as a padded encoding two crates away.
#[test]
#[should_panic(expected = "one promotion target per key column")]
fn a_mis_sized_promotion_target_list_is_refused() {
    built_target_tcs(&[2, 5], &[0]);
}

/// A two-segment chain lowered with symbolic ids substitutes to the circuit
/// inline real-id allocation would have produced (same nodes, same `view_id`,
/// same `ScanDelta.source`), and leaves the base-table id alone.
#[test]
fn resolve_seg_ids_reproduces_inline_allocation() {
    // `base → seg → final`, built once with symbolic ids and once with the
    // real ones the substitution assigns.
    let (base, seg_real, final_real) = (100u64, 4096u64, 4097u64);
    let (seg_a, seg_b) = (segment_id(0), segment_id(1));
    let chain = |seg: u64, fin: u64| {
        let mut cb = CircuitBuilder::new(fin, seg);
        let up = cb.input_delta_tagged(base);
        let inp = cb.input_delta();
        cb.sink(up);
        cb.sink(inp);
        cb.build()
    };

    let mut symbolic = chain(seg_a, seg_b);
    let map = HashMap::from([(seg_a, seg_real), (seg_b, final_real)]);
    symbolic.resolve_seg_ids(&map).expect("every tag is in the map");

    let inline = chain(seg_real, final_real);
    assert_eq!(symbolic.view_id, inline.view_id);
    assert_eq!(
        format!("{:?}", symbolic.into_rows()),
        format!("{:?}", inline.into_rows()),
        "substitution must produce the inline-allocated rows"
    );
}

/// An identity pass over an empty map leaves a real relation id untouched:
/// the shape a bundle of caller-preset ids takes.
#[test]
fn resolve_seg_ids_leaves_real_ids_alone() {
    let mut cb = CircuitBuilder::new(17, 100);
    let inp = cb.input_delta();
    cb.sink(inp);
    let mut circuit = cb.build();
    circuit.resolve_seg_ids(&HashMap::new()).expect("no tag to resolve");
    assert_eq!(circuit.view_id, 17);
    assert_eq!(circuit.dependencies(), vec![100]);
}

/// A tag that survives the substitution is rejected, in `view_id` and in a
/// `ScanDelta.source` alike, whether the map is empty or merely missing that
/// one entry.
#[test]
fn resolve_seg_ids_rejects_a_surviving_tag() {
    // A tagged `view_id` with nothing to resolve it.
    let mut cb = CircuitBuilder::new(segment_id(0), 100);
    let inp = cb.input_delta();
    cb.sink(inp);
    let mut circuit = cb.build();
    assert!(circuit.resolve_seg_ids(&HashMap::new()).is_err(), "tagged view_id");

    // A tagged source absent from a non-empty map: the lowering path that
    // mints a segment id and forgets to register it.
    let mut cb = CircuitBuilder::new(segment_id(0), 100);
    let up = cb.input_delta_tagged(segment_id(7));
    cb.sink(up);
    let mut circuit = cb.build();
    let map = HashMap::from([(segment_id(0), 17u64)]);
    let err = circuit.resolve_seg_ids(&map).expect_err("tagged source");
    assert!(
        format!("{err}").contains(&format!("{}", segment_id(7))),
        "the error names the unresolved id: {err}"
    );
}
