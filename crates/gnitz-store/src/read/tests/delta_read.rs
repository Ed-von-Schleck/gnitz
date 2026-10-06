//! A delta read under a spec. A subscriber that bootstraps under one and polls
//! under the same one holds that spec applied to the view, weight for weight.

use super::*;
use crate::relation::{RelationKind, RelationSpec};
use crate::test_support::{
    between, img, make_batch_raw, make_schema_u64_i64, map_of, payload0_i64, relation_fixture, RelationFixture, Rng,
    TID,
};
use gnitz_expr::{CmpOp, ExprBuilder, LogicalInstr, LogicalProgram, SchemaFacts, Sink};
use gnitz_wire::{Cut, KeyRange, PkColList, ReadSink, TypeCode, ViewProps};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_zset_testkit::{zset_of, RowKey};
use std::collections::HashMap;

/// `(k1, k2, a, b, s)`: the PK is `(k2, k1)`.
type Row = (u64, i64, i64, Option<i64>, u8);

/// `a I64 | k1 U64 pk | b I64? | k2 I64 pk | s STRING`, PK list `[3, 1]`.
fn view_schema() -> SchemaDescriptor {
    let c = SchemaColumn::new;
    SchemaDescriptor::new(
        &[
            c(TypeCode::I64, false),
            c(TypeCode::U64, false),
            c(TypeCode::I64, true),
            c(TypeCode::I64, false),
            c(TypeCode::String, false),
        ],
        &[3, 1],
    )
}

/// The reply of a projection onto `a`: `k2, k1 | a`.
fn slim_schema() -> SchemaDescriptor {
    let c = SchemaColumn::new;
    SchemaDescriptor::new(
        &[
            c(TypeCode::I64, false),
            c(TypeCode::U64, false),
            c(TypeCode::I64, false),
        ],
        &[0, 1],
    )
}

fn batch(rows: &HashMap<Row, i64>) -> Batch {
    let schema = view_schema();
    let mut b = BatchBuilder::new(&schema);
    for (&(k1, k2, a, bv, s), &w) in rows {
        b.begin_row_natives(&[k2 as u64 as u128, k1 as u128], w);
        b.put_int(a as u64 as u128);
        b.put_opt_int(bv.map(|v| v as u64 as u128));
        b.put_string(&"x".repeat(s as usize));
        b.end_row();
    }
    b.finish()
}

fn slim_batch(rows: &HashMap<Row, i64>) -> Batch {
    let schema = slim_schema();
    let mut b = BatchBuilder::new(&schema);
    for (&(k1, k2, a, _, _), &w) in rows {
        b.begin_row_natives(&[k2 as u64 as u128, k1 as u128], w);
        b.put_int(a as u64 as u128);
        b.end_row();
    }
    b.finish()
}

/// One round's delta against `state`, which it leaves at non-negative weights.
fn step_delta(rng: &mut Rng, state: &mut HashMap<Row, i64>, churn: bool) -> HashMap<Row, i64> {
    let mut delta: HashMap<Row, i64> = HashMap::new();
    for _ in 0..1 + rng.gen_range(6) {
        let live: Vec<Row> = state.keys().copied().collect();
        let fresh = |rng: &mut Rng| -> Row {
            (
                rng.gen_range(6),
                rng.gen_range(7) as i64 - 3,
                rng.gen_range(20) as i64 - 5,
                (rng.gen_range(3) > 0).then(|| rng.gen_range(4) as i64),
                rng.gen_range(20) as u8,
            )
        };
        let kind = if churn { 2 } else { rng.gen_range(3) };
        match (kind, live.is_empty()) {
            (0, _) | (_, true) => {
                let r = fresh(rng);
                let w = 1 + rng.gen_range(3) as i64;
                *delta.entry(r).or_default() += w;
                *state.entry(r).or_default() += w;
            }
            (1, _) => {
                let r = live[rng.gen_range(live.len() as u64) as usize];
                let w = 1 + rng.gen_range(state[&r] as u64) as i64;
                *delta.entry(r).or_default() -= w;
                *state.entry(r).or_default() -= w;
            }
            _ => {
                // An UPDATE that keeps the key, and under `churn` keeps `a` too.
                let r = live[rng.gen_range(live.len() as u64) as usize];
                let f = fresh(rng);
                let n = (r.0, r.1, if churn { r.2 } else { f.2 }, f.3, f.4);
                let w = state[&r];
                *delta.entry(r).or_default() -= w;
                *state.entry(r).or_default() -= w;
                *delta.entry(n).or_default() += w;
                *state.entry(n).or_default() += w;
            }
        }
        state.retain(|_, w| *w != 0);
    }
    delta.retain(|_, w| *w != 0);
    delta
}

/// `k2 >= lo AND b > 1`: a key column and a nullable payload column.
fn k2_ge_or_b(lo: i64) -> Vec<u8> {
    let mut eb = ExprBuilder::new();
    let v = eb.emit(LogicalInstr::LoadCol { col: 3 });
    let c = eb.emit(LogicalInstr::LoadConst { val: lo, unsigned: false });
    let ge = eb.emit(LogicalInstr::Cmp { op: CmpOp::Ge, a: v, b: c });
    let b = eb.emit(LogicalInstr::LoadCol { col: 2 });
    let one = eb.emit(LogicalInstr::LoadConst { val: 1, unsigned: false });
    let gt = eb.emit(LogicalInstr::Cmp { op: CmpOp::Gt, a: b, b: one });
    let keep = eb.emit(LogicalInstr::BoolBinary { a: ge, b: gt, is_or: false });
    eb.build(vec![Sink::Reg(keep)]).unwrap().to_blob_bytes()
}

struct Case {
    name: &'static str,
    spec: ReadSpec,
    keep: Box<dyn Fn(&Row) -> bool>,
    slim: bool,
}

fn cases() -> Vec<Case> {
    let rows = |map| ReadSink { map, kind: SinkKind::Rows { cut: None } };
    let slim = || map_of(LogicalProgram::copy_cols(&[0]), &slim_schema());
    let schema = view_schema();
    let key = |k2: i64, k1: u64| {
        schema
            .opk_key_cols(&[k2 as u64 as u128, k1 as u128])
            .pk_bytes()
            .to_vec()
    };
    let set: Vec<(i64, u64)> = vec![(-3, 0), (-1, 2), (0, 0), (0, 5), (2, 1), (3, 3)];
    let keys: Vec<Vec<u8>> = set.iter().map(|&(a, b)| key(a, b)).collect();
    let pk_set = || ReadBound::PkSet(PkKeys::from_keys(16, keys.iter().map(Vec::as_slice)));
    let range = ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[3]),
        &[],
        Cut::before(img(-1)),
        Cut::before(img(2)),
    ));
    // Most keys the model draws: under the lagging subscriber's rounds, more
    // probes than a keyed read gathers, so it walks the band.
    let wide: Vec<Vec<u8>> = (-3..=3)
        .flat_map(|k2| (0..5).map(move |k1| (k2, k1)))
        .map(|(a, b)| key(a, b))
        .collect();
    let wide_set = ReadBound::PkSet(PkKeys::from_keys(16, wide.iter().map(Vec::as_slice)));
    let set2 = set.clone();
    let set3 = set.clone();
    vec![
        Case {
            name: "identity",
            spec: ReadSpec {
                bound: ReadBound::None,
                predicate: vec![],
                sink: rows(None),
            },
            keep: Box::new(|_| true),
            slim: false,
        },
        Case {
            name: "payload predicate",
            spec: ReadSpec {
                bound: ReadBound::None,
                predicate: between(0, 0, Some(8)),
                sink: rows(None),
            },
            keep: Box::new(|r| (0..8).contains(&r.2)),
            slim: false,
        },
        Case {
            name: "pk + nullable predicate",
            spec: ReadSpec {
                bound: ReadBound::None,
                predicate: k2_ge_or_b(0),
                sink: rows(None),
            },
            keep: Box::new(|r| r.1 >= 0 && r.3.is_some_and(|b| b > 1)),
            slim: false,
        },
        Case {
            name: "projection",
            spec: ReadSpec {
                bound: ReadBound::None,
                predicate: vec![],
                sink: rows(slim()),
            },
            keep: Box::new(|_| true),
            slim: true,
        },
        Case {
            name: "predicate + projection",
            spec: ReadSpec {
                bound: ReadBound::None,
                predicate: k2_ge_or_b(-1),
                sink: rows(slim()),
            },
            keep: Box::new(|r| r.1 >= -1 && r.3.is_some_and(|b| b > 1)),
            slim: true,
        },
        Case {
            name: "pk range",
            spec: ReadSpec {
                bound: range,
                predicate: between(0, -2, Some(12)),
                sink: rows(None),
            },
            keep: Box::new(|r| (-1..2).contains(&r.1) && (-2..12).contains(&r.2)),
            slim: false,
        },
        Case {
            name: "pk set",
            spec: ReadSpec {
                bound: pk_set(),
                predicate: vec![],
                sink: rows(None),
            },
            keep: Box::new(move |r| set2.contains(&(r.1, r.0))),
            slim: false,
        },
        Case {
            name: "wide pk set + projection",
            spec: ReadSpec {
                bound: wide_set,
                predicate: between(0, -5, Some(9)),
                sink: rows(slim()),
            },
            keep: Box::new(|r| r.0 < 5 && (-5..9).contains(&r.2)),
            slim: true,
        },
        Case {
            name: "pk set + predicate + projection",
            spec: ReadSpec {
                bound: pk_set(),
                predicate: between(0, 0, Some(10)),
                sink: rows(slim()),
            },
            keep: Box::new(move |r| set3.contains(&(r.1, r.0)) && (0..10).contains(&r.2)),
            slim: true,
        },
    ]
}

fn fed() -> RelationFixture {
    let kind = RelationKind::View(ViewProps::Fed {
        delta_bytes: std::num::NonZeroU64::new(1 << 30).unwrap(),
    });
    relation_fixture(kind, view_schema(), &[], [])
}

fn add(acc: &mut HashMap<RowKey, i64>, b: &Batch, schema: &SchemaDescriptor) {
    for (k, w) in zset_of(b, schema) {
        *acc.entry(k).or_default() += w;
    }
    acc.retain(|_, w| *w != 0);
}

/// What the spec keeps of `state`, in the reply's layout.
fn expect(case: &Case, state: &HashMap<Row, i64>) -> HashMap<RowKey, i64> {
    let kept: HashMap<Row, i64> = state
        .iter()
        .filter(|(r, _)| (case.keep)(r))
        .map(|(r, w)| (*r, *w))
        .collect();
    match case.slim {
        true => zset_of(&slim_batch(&kept), &slim_schema()),
        false => zset_of(&batch(&kept), &view_schema()),
    }
}

/// Each case under two subscribers: one polling every few rounds, one that
/// bootstraps early and polls once at the end, across every round in between.
#[test]
fn a_subscriber_under_a_spec_holds_the_spec_of_the_view() {
    let mut polls = 0;
    for seed in 0..40u64 {
        let mut rng = Rng::new(seed * 7919 + 1);
        let mut r = fed();
        let mut state = HashMap::new();
        let cases = cases();
        // Per (case, lagging): the copy and its cursor.
        let mut subs: Vec<(usize, bool, HashMap<RowKey, i64>, u64)> = (0..cases.len())
            .flat_map(|ci| [false, true].map(|lagging| (ci, lagging, HashMap::new(), 0)))
            .collect();
        let mut round = 1;
        const STEPS: usize = 90;
        for step in 0..STEPS {
            // The round counter is the master's, shared by every relation.
            round += 1 + rng.gen_range(3);
            let delta = step_delta(&mut rng, &mut state, false);
            if !delta.is_empty() {
                r.ingest_at(TID, batch(&delta), Some(round), false).unwrap();
            }
            for (ci, lagging, copy, cursor) in subs.iter_mut() {
                let due = match lagging {
                    true => step == 3 || step == STEPS - 1,
                    false => rng.gen_range(4) == 0,
                };
                if !due {
                    continue;
                }
                let case = &cases[*ci];
                let out = if case.slim { slim_schema() } else { view_schema() };
                let reply = r
                    .delta_read(TID, *cursor, round, case.spec.clone(), out.layout_digest())
                    .unwrap_or_else(|e| panic!("{}: {e:?}", case.name));
                if *cursor == 0 {
                    copy.clear();
                }
                add(copy, &reply, &out);
                *cursor = round;
                polls += 1;
                assert_eq!(
                    *copy,
                    expect(case, &state),
                    "seed {seed} {} lagging={lagging} round {round}",
                    case.name
                );
            }
        }
    }
    assert!(polls > 4000, "{polls}");
}

/// An UPDATE of a column the map drops is a retraction and an insert of one
/// mapped row, which the reply nets to nothing.
#[test]
fn an_update_of_a_dropped_column_ships_nothing() {
    let mut rng = Rng::new(99);
    let mut r = fed();
    let mut state = HashMap::new();
    let d = step_delta(&mut rng, &mut state, false);
    r.ingest_at(TID, batch(&d), Some(2), false).unwrap();
    let mut churned = 0;
    for round in 3..200 {
        let d = step_delta(&mut rng, &mut state, true);
        churned += d.len();
        if !d.is_empty() {
            r.ingest_at(TID, batch(&d), Some(round), false).unwrap();
        }
    }
    assert!(churned > 100);
    let case = cases().into_iter().find(|c| c.name == "projection").unwrap();
    let reply = r
        .delta_read(TID, 2, 199, case.spec, slim_schema().layout_digest())
        .unwrap();
    assert_eq!(reply.len(), 0);
}

/// A fed view's delta read answers the rounds `(after_tick, cut_tick]` at each
/// round's own weights — both sides of a pair the output store folded away — in the
/// view's layout; `after_tick = 0` reads the view whole. Any other layout, or a view
/// with no feed, is refused.
#[test]
fn a_delta_read_answers_the_rounds_past_its_cursor() {
    let schema = make_schema_u64_i64();
    let kind = RelationKind::View(ViewProps::Fed {
        delta_bytes: std::num::NonZeroU64::new(1 << 20).unwrap(),
    });
    let mut r = relation_fixture(kind, schema, &[], []);
    r.ingest_at(TID, make_batch_raw(&schema, &[(7, 1, 70)]), Some(4), false)
        .unwrap();
    r.ingest_at(TID, make_batch_raw(&schema, &[(7, -1, 70), (8, 1, 80)]), Some(5), false)
        .unwrap();
    let own = schema.layout_digest();
    let whole = || ReadSpec::all_rows(ReadBound::None);
    // `(id, weight, val)`, sorted: a reply's order is not a contract.
    let read = |after_tick| {
        let b = r.delta_read(TID, after_tick, 5, whole(), own).unwrap();
        let mut rows: Vec<_> = (0..b.len())
            .map(|i| (b.get_pk(i) as u64, b.get_weight(i), payload0_i64(&*b, i)))
            .collect();
        rows.sort_unstable();
        rows
    };
    assert_eq!(read(3), [(7, -1, 70), (7, 1, 70), (8, 1, 80)]);
    assert_eq!(read(4), [(7, -1, 70), (8, 1, 80)]);
    assert_eq!(read(5), []);
    assert_eq!(read(0), [(8, 1, 80)], "the output store folded the pair away");

    r.register(RelationSpec {
        id: TID + 1,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
        placement: Placement::full_pk(&schema),
        pk_repeats: false,
    })
    .unwrap();
    for (id, after_tick, layout) in [(TID, 0, own ^ 1), (TID, 3, own ^ 1), (TID + 1, 0, own)] {
        let Err(err) = r.delta_read(id, after_tick, 5, whole(), layout) else {
            panic!("relation {id} at {after_tick} must be refused");
        };
        assert!(matches!(err.status, WireStatus::Error), "{err:?}");
    }
}

/// A delta read forwards rows: a cut is refused at every cursor, and so is a key
/// list of another width.
#[test]
fn a_delta_read_refuses_what_is_not_a_plain_rows_spec() {
    let mut r = fed();
    r.ingest_at(TID, batch(&HashMap::from([((1, 1, 1, None, 1), 1)])), Some(2), false)
        .unwrap();
    let own = view_schema().layout_digest();
    let cut = ReadSpec {
        bound: ReadBound::None,
        predicate: vec![],
        sink: ReadSink {
            map: None,
            kind: SinkKind::Rows { cut: crate::test_support::cut(1, vec![]) },
        },
    };
    let short = ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, [&[0u8; 8][..]])));
    for after_tick in [0, 1] {
        assert!(r.delta_read(TID, after_tick, 2, cut.clone(), own).is_err());
    }
    assert!(r.delta_read(TID, 1, 2, short, own).is_err());
}
