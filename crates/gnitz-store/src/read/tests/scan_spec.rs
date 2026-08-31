use super::*;
use crate::schema::Placement;
use crate::schema::{type_code, SchemaColumn};
use crate::test_support::{opk_pk, pk_only_schema};
use gnitz_wire::Cut::{After, Before};

fn opk_u64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec() // U64 OPK is plain big-endian
}

/// `pk >= 5` → `[OPK(5), +∞)`. The single-column mainline: no equality pins,
/// `prefix_len == pk_stride`, an unbounded upper edge.
#[test]
fn pk_range_ge_unbounded_above() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Before(5), After(u64::MAX as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(5));
    assert!(end.is_none(), "unbounded above → end None");
}

/// `pk > 5` → `[OPK(6), +∞)` — the degenerate no-pad `succ` on the whole key.
#[test]
fn pk_range_gt_increments_whole_key() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], After(5), After(u64::MAX as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(6));
    assert!(end.is_none());
}

/// `pk < 10` → `[OPK(0), OPK(10))`.
#[test]
fn pk_range_lt() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Before(0), Before(10));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(0));
    assert_eq!(end.unwrap().pk_bytes(), opk_u64(10));
}

/// Full-PK point lookup `pk = 5` → `[OPK(5), OPK(6))` (degenerate cuts).
#[test]
fn pk_range_point_lookup() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Before(5), After(5));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(5));
    assert_eq!(end.unwrap().pk_bytes(), opk_u64(6));
}

/// An inverted interval (`pk > 10 AND pk < 3`) drains to zero rows.
#[test]
fn pk_range_inverted_is_empty() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], After(10), Before(3));
    assert_eq!(pk_range_keys(&s, &d).unwrap(), None);
}

/// A signed PK: `pk > -1` seeks to `OPK(0)` (sign-flip order); `After(i64::MAX)`
/// overflows `succ` → unbounded above.
#[test]
fn pk_range_signed_i64() {
    let s = pk_only_schema(&[type_code::I64]);
    let neg1 = (-1i64 as u64) as u128;
    let d = RangeDescriptor::new(&[], After(neg1), After((i64::MAX as u64) as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start, opk_key(&s, &0i64.to_le_bytes()));
    assert!(end.is_none());
}

/// Compound PK `(a, b)` with `a = 5 AND b > 3`: the range column is `b`, so
/// `start` seeks past `(5, 3)` and stays within the `a == 5` group.
#[test]
fn pk_range_compound_prefix_eq() {
    let s = pk_only_schema(&[type_code::U64, type_code::U64]);
    let d = RangeDescriptor::new(&[5], After(3), After(u64::MAX as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    // start = OPK(5,4) — the prefix `(5,3)` incremented on b.
    let s54 = opk_key(&s, &{
        let mut v = Vec::new();
        v.extend_from_slice(&5u64.to_le_bytes());
        v.extend_from_slice(&4u64.to_le_bytes());
        v
    });
    assert_eq!(start, s54);
    // end = the successor of `(5, MAX)` — carries into `a`, i.e. OPK(6, 0).
    let s60 = opk_key(&s, &{
        let mut v = Vec::new();
        v.extend_from_slice(&6u64.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes());
        v
    });
    assert_eq!(end.unwrap(), s60);
}

/// `n_eq` at the PK arity leaves no range column — a trust-boundary reject.
#[test]
fn pk_range_no_range_column_errs() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[5], Before(0), After(0));
    assert!(pk_range_keys(&s, &d).is_err());
}

// ── scan_spec_worker — the master's confinement test ─────────────────────

/// Worker count the confinement tests route against.
const NW: usize = 4;

/// A full point is confined to the worker of its own PK bytes, at every PK
/// shape — single, wide, and compound (where the point pins the leading
/// columns through `eq_vals` and points at the last).
#[test]
fn scan_spec_worker_confines_a_full_point() {
    let u64s = pk_only_schema(&[type_code::U64]);
    assert_eq!(
        scan_spec_worker(&u64s, &RangeDescriptor::new(&[], Before(42), After(42)), NW),
        Some(u64s.worker_for_pk(&opk_pk(&u64s, &[42]), NW))
    );

    let u128s = pk_only_schema(&[type_code::U128]);
    let wide = (1u128 << 100) | 7;
    assert_eq!(
        scan_spec_worker(&u128s, &RangeDescriptor::new(&[], Before(wide), After(wide)), NW),
        Some(u128s.worker_for_pk(&opk_pk(&u128s, &[wide]), NW))
    );

    let comp = pk_only_schema(&[type_code::U32, type_code::U64]);
    assert_eq!(
        scan_spec_worker(&comp, &RangeDescriptor::new(&[9], Before(4), After(4)), NW),
        Some(comp.worker_for_pk(&opk_pk(&comp, &[9, 4]), NW))
    );
}

/// With a `Keyed { prefix_len: 1 }` placement every row sharing the leading
/// column lands on one worker, so pinning it and ranging the trailing column
/// is confined — to the same worker full points on `(a, b)` reach. At the
/// full-PK default the same bound spans workers.
#[test]
fn scan_spec_worker_follows_the_distribution_prefix() {
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
    // `a = 7 AND b > 3` — a whole trailing-column range inside one `a` group.
    let ranged = RangeDescriptor::new(&[7], After(3), After(u64::MAX as u128));
    let want = prefix.worker_for_pk(&opk_pk(&prefix, &[7, 0]), NW);
    assert_eq!(scan_spec_worker(&prefix, &ranged, NW), Some(want));
    for b in [4u128, u64::MAX as u128] {
        assert_eq!(
            scan_spec_worker(&prefix, &RangeDescriptor::new(&[7], Before(b), After(b)), NW),
            Some(want),
            "a full point on (7, {b}) shares the group's worker"
        );
    }

    let full = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 2 });
    assert_eq!(
        scan_spec_worker(&full, &ranged, NW),
        None,
        "hashing the whole PK spreads one `a` group across workers"
    );
}

/// A maximal-value point carries out of `succ`, so its range has no `end` —
/// the all-`0xFF` last key must still confine it rather than broadcast. The
/// signed maximum's OPK is all-`0xFF` too (sign-flip).
#[test]
fn scan_spec_worker_confines_a_maximal_point() {
    for tc in [type_code::U64, type_code::I64] {
        let s = pk_only_schema(&[tc]);
        let max = if tc == type_code::U64 {
            u64::MAX as u128
        } else {
            i64::MAX as u128
        };
        let d = RangeDescriptor::new(&[], Before(max), After(max));
        assert!(
            pk_range_keys(&s, &d).unwrap().unwrap().1.is_none(),
            "After(max) carries out"
        );
        assert_eq!(
            scan_spec_worker(&s, &d, NW),
            Some(s.worker_for_pk(&opk_pk(&s, &[max]), NW))
        );
    }
}

/// A range wider than one worker's key span is not confinable: owners are a
/// hash of the key, not monotone in key order, so only a whole-range prefix
/// match proves confinement. A provably-empty range is not confinable
/// either — the worker answers it (a fold sink still owes its ground row).
#[test]
fn scan_spec_worker_declines_a_multi_key_range() {
    let s = pk_only_schema(&[type_code::U64]);
    assert_eq!(
        scan_spec_worker(&s, &RangeDescriptor::new(&[], Before(0), After(1000)), NW),
        None
    );
    assert_eq!(
        scan_spec_worker(&s, &RangeDescriptor::new(&[], After(1000), Before(0)), NW),
        None,
        "an inverted range is provably empty"
    );
    // Unbounded above from a non-maximal start: the last key is 0xFF…FF.
    assert_eq!(
        scan_spec_worker(&s, &RangeDescriptor::new(&[], Before(5), After(u64::MAX as u128)), NW),
        None
    );
}

// ---------------------------------------------------------------------------
// Fold pre-map — the trust boundary
//
// `AggReadSpec.pre_map`/`pre_payload` are the client's, and the schema they
// describe is *derived* here rather than shipped. These pin that a frame no
// planner would send is refused rather than aborting the worker: the derivation
// runs through `DerivedSchema`, whose `push`/`push_pk` reject exactly what
// `SchemaDescriptor::new` asserts on (and its asserts are release-active).
//
// The fused map->fold loop over real rows is covered end-to-end by the Python
// aggregate suite, which is the only place a *valid* pre-map program exists —
// building one here would mean reimplementing the planner's expression compiler.
// ---------------------------------------------------------------------------

/// `(id U64 PK, v I64)` — a one-column key so the derived reduce input is the PK
/// plus whatever `pre_payload` declares.
fn premap_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn premap_spec(pre_map: Vec<u8>, pre_payload: Vec<(u8, bool)>) -> AggReadSpec {
    AggReadSpec {
        group_cols: vec![1],
        aggs: vec![],
        pre_map,
        pre_payload,
    }
}

/// The rejection message, or a panic naming the shape that was accepted.
/// `MapPlan` is not `Debug`, so the `Ok` half cannot go through `unwrap_err`.
fn premap_err(spec: &AggReadSpec) -> String {
    match compile_fold_pre_map(spec, &premap_src()) {
        Err(e) => e,
        Ok(_) => panic!("the pre-map derivation accepted a frame it must refuse"),
    }
}

/// No program is the ordinary fold: the source *is* the reduce input, and
/// nothing is compiled.
#[test]
fn fold_pre_map_is_absent_without_a_program() {
    let spec = premap_spec(Vec::new(), Vec::new());
    assert!(compile_fold_pre_map(&spec, &premap_src()).unwrap().is_none());
}

/// A declaration wider than one schema can hold. `MAX_COLUMNS` payload columns
/// on top of a 1-column key is one past the limit — the case that would reach
/// `SchemaDescriptor::new`'s release-active `assert!` and abort the worker if the
/// derivation did not go through `DerivedSchema` first.
#[test]
fn fold_pre_map_refuses_an_over_wide_declaration() {
    let wide: Vec<(u8, bool)> = (0..crate::schema::MAX_COLUMNS)
        .map(|_| (type_code::I64, false))
        .collect();
    let err = premap_err(&premap_spec(vec![1, 2, 3], wide));
    assert!(
        err.contains("not a legal reduce input"),
        "an over-wide pre-map must be refused by the derivation, got: {err}"
    );
}

/// A corrupt program blob is refused by the shared map compiler, not decoded
/// into a plan that folds over garbage.
#[test]
fn fold_pre_map_refuses_a_corrupt_program() {
    let err = premap_err(&premap_spec(vec![0xff; 8], vec![(type_code::I64, false)]));
    assert!(
        err.contains("projection program"),
        "a corrupt pre-map blob must be refused by the program decoder, got: {err}"
    );
}
