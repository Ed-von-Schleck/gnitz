//! A delta read under a spec. A subscriber that bootstraps under one and polls
//! under the same one holds that spec applied to the view, weight for weight.

use super::*;
use crate::relation::{RelationKind, RelationSpec, StoreConfig};
use crate::test_support::{
    between, fed_view, img, make_batch_raw, make_schema_u64_i64, map_of, payload0_i64, relation_fixture,
    relation_fixture_with, RelationFixture, Rng, TID,
};
use gnitz_expr::{CmpOp, ExprBuilder, LogicalInstr, LogicalProgram, SchemaFacts, Sink};
use gnitz_wire::{Cut, KeyRange, PkColList, PkKeys, ReadSink, TypeCode, ViewProps};
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
    let kind = fed_view(1 << 30);
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
/// Every other seed spills the feed to shards and reads it a few rows a chunk,
/// and a round's delta sometimes arrives as two ingests.
#[test]
fn a_subscriber_under_a_spec_holds_the_spec_of_the_view() {
    let mut polls = 0;
    for seed in 0..40u64 {
        let mut rng = Rng::new(seed * 7919 + 1);
        let mut r = match seed % 2 {
            0 => fed(),
            _ => {
                let config = StoreConfig {
                    ram_tier_bytes: 1 << 11,
                    ..StoreConfig::default()
                };
                let kind = fed_view(1 << 30);
                let mut r = relation_fixture_with(config, kind, view_schema(), &[], []);
                r.set_scan_chunk_rows(1 + rng.gen_range(16) as usize);
                r
            }
        };
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
            // A view stepped twice in one schedule ingests twice in one round.
            let (first, second): (HashMap<Row, i64>, HashMap<Row, i64>) = match rng.gen_range(4) {
                0 => delta.iter().partition(|(row, _)| row.0 % 2 == 0),
                _ => (delta, HashMap::new()),
            };
            for part in [first, second].iter().filter(|part| !part.is_empty()) {
                r.ingest_at(TID, batch(part), Some(round), false).unwrap();
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
                    .delta_read(TID, *cursor, case.spec.clone(), out.layout_digest())
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
    let reply = r.delta_read(TID, 2, case.spec, slim_schema().layout_digest()).unwrap();
    assert_eq!(reply.len(), 0);
}

/// A delta read answers the rounds after `after_tick` at each round's own
/// weights, both sides of a pair the output store folded away; `after_tick = 0`
/// reads the view whole. Another layout, or a view with no feed, is refused.
#[test]
fn a_delta_read_answers_the_rounds_past_its_cursor() {
    let schema = make_schema_u64_i64();
    let kind = fed_view(1 << 20);
    let mut r = relation_fixture(kind, schema, &[], []);
    r.ingest_at(TID, make_batch_raw(&schema, &[(7, 1, 70)]), Some(4), false)
        .unwrap();
    r.ingest_at(TID, make_batch_raw(&schema, &[(7, -1, 70), (8, 1, 80)]), Some(5), false)
        .unwrap();
    let own = schema.layout_digest();
    let whole = || ReadSpec::all_rows(ReadBound::None);
    // `(id, weight, val)`, sorted: a reply's order is not a contract.
    let read = |after_tick| {
        let b = r.delta_read(TID, after_tick, whole(), own).unwrap();
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
        let Err(err) = r.delta_read(id, after_tick, whole(), layout) else {
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
        assert!(r.delta_read(TID, after_tick, cut.clone(), own).is_err());
    }
    assert!(r.delta_read(TID, 1, short, own).is_err());
}

/// A view that is all key has no payload column to copy: its delta rows are
/// their keys and weights.
#[test]
fn a_key_only_view_reads_its_deltas() {
    let schema = crate::test_support::pk_only_schema(&[TypeCode::U64]);
    let kind = fed_view(1 << 20);
    let mut r = relation_fixture(kind, schema, &[], []);
    let keys = |rows: &[(u64, i64)]| {
        let mut b = BatchBuilder::new(&schema);
        for &(id, w) in rows {
            b.begin_row(id as u128, w);
            b.end_row();
        }
        b.finish()
    };
    r.ingest_at(TID, keys(&[(7, 1)]), Some(4), false).unwrap();
    r.ingest_at(TID, keys(&[(7, -1), (8, 1)]), Some(5), false).unwrap();
    let b = r
        .delta_read(TID, 3, ReadSpec::all_rows(ReadBound::None), schema.layout_digest())
        .unwrap();
    let mut rows: Vec<_> = (0..b.len()).map(|i| (b.get_pk(i) as u64, b.get_weight(i))).collect();
    rows.sort_unstable();
    assert_eq!(rows, [(7, -1), (7, 1), (8, 1)]);
}

// ---------------------------------------------------------------------------
// Rounds of every size, under budgets that drop, against a model
// ---------------------------------------------------------------------------

fn random_delta(rng: &mut Rng, state: &mut HashMap<Row, i64>, ops: u64) -> HashMap<Row, i64> {
    let mut delta: HashMap<Row, i64> = HashMap::new();
    for _ in 0..ops {
        let fresh = |rng: &mut Rng| -> Row {
            (
                rng.gen_range(6),
                rng.gen_range(7) as i64 - 3,
                rng.gen_range(20) as i64 - 5,
                (rng.gen_range(3) > 0).then(|| rng.gen_range(4) as i64),
                rng.gen_range(20) as u8,
            )
        };
        let kind = rng.gen_range(10);
        if kind < 6 || state.is_empty() {
            let r = fresh(rng);
            let w = 1 + rng.gen_range(3) as i64;
            *delta.entry(r).or_default() += w;
            *state.entry(r).or_default() += w;
        } else {
            // Not uniform over the live rows, but cheap: the first in hash order.
            let skip = rng.gen_range(state.len().min(50) as u64) as usize;
            let r = *state.keys().nth(skip).unwrap();
            let w = state[&r];
            *delta.entry(r).or_default() -= w;
            state.remove(&r);
            if kind >= 8 {
                let f = fresh(rng);
                let n = (r.0, r.1, f.2, f.3, f.4);
                *delta.entry(n).or_default() += w;
                *state.entry(n).or_default() += w;
            }
        }
    }
    delta.retain(|_, w| *w != 0);
    state.retain(|_, w| *w != 0);
    delta
}

fn expected(case: &Case, rows: &HashMap<Row, i64>) -> HashMap<RowKey, i64> {
    let mut kept: HashMap<Row, i64> = HashMap::new();
    let mut slim: HashMap<RowKey, i64> = HashMap::new();
    for (r, w) in rows.iter().filter(|(r, _)| (case.keep)(r)) {
        kept.insert(*r, *w);
    }
    if !case.slim {
        return zset_of(&batch(&kept), &view_schema());
    }
    // Rows that differ only in what the map drops are one mapped row.
    for (r, w) in &kept {
        let one = HashMap::from([(*r, 1i64)]);
        for (k, _) in zset_of(&slim_batch(&one), &slim_schema()) {
            *slim.entry(k).or_default() += *w;
        }
    }
    slim.retain(|_, w| *w != 0);
    slim
}

#[test]
fn reads_over_rounds_of_every_size_match_a_model() {
    let (mut reads, mut expired, mut dropped_seeds, mut big_rounds) = (0u64, 0u64, 0u64, 0u64);
    // Twelve seeds: every budget under every RAM tier and chunk size.
    for seed in 0..12u64 {
        let mut rng = Rng::new(seed * 104_729 + 7);
        let budget = match seed % 3 {
            0 => 1 << 30,
            1 => 400_000,
            _ => 60_000,
        };
        let config = StoreConfig {
            ram_tier_bytes: if seed % 2 == 0 {
                1 << 11
            } else {
                StoreConfig::default().ram_tier_bytes
            },
            ..StoreConfig::default()
        };
        let mut r = relation_fixture_with(config, fed_view(budget), view_schema(), &[], []);
        r.set_scan_chunk_rows([3, 200, 1000, 8192][(seed % 4) as usize]);
        let mut state = HashMap::new();
        // Per ingest: its round and its net delta.
        let mut log: Vec<(u64, HashMap<Row, i64>)> = Vec::new();
        let cases = cases();
        let mut round = 1;
        for _ in 0..50 {
            round += 1 + rng.gen_range(3);
            let ops = match rng.gen_range(6) {
                0 => 1 + rng.gen_range(4),
                1 => 100 + rng.gen_range(80),
                2 => 300 + rng.gen_range(500),
                3 => 120 + rng.gen_range(20),
                _ => 10 + rng.gen_range(40),
            };
            let delta = random_delta(&mut rng, &mut state, ops);
            let captured = delta.clone();
            big_rounds += u64::from(delta.len() >= 128);
            let (first, second): (HashMap<Row, i64>, HashMap<Row, i64>) = match rng.gen_range(4) {
                0 => delta.iter().partition(|(row, _)| row.0 % 2 == 0),
                _ => (delta, HashMap::new()),
            };
            for part in [first, second] {
                if part.is_empty() {
                    continue;
                }
                r.ingest_at(TID, batch(&part), Some(round), false).unwrap();
                log.push((round, part));
            }
            if rng.gen_range(5) == 0 {
                // An ingest that consolidates to nothing captures nothing.
                let ghost: Row = (1u64, 1i64, 1i64, None, 1u8);
                let schema = view_schema();
                let mut b = BatchBuilder::new(&schema);
                for w in [2, -2] {
                    b.begin_row_natives(&[ghost.1 as u64 as u128, ghost.0 as u128], w);
                    b.put_int(ghost.2 as u64 as u128);
                    b.put_opt_int(None);
                    b.put_string("x");
                    b.end_row();
                }
                r.ingest_at(TID, b.finish(), Some(round), false).unwrap();
            }
            // The round just captured, read alone, is what went in.
            if !captured.is_empty() {
                let reply = r
                    .delta_read(TID, round - 1, cases[0].spec.clone(), view_schema().layout_digest())
                    .unwrap();
                let mut got = HashMap::new();
                add(&mut got, &reply, &view_schema());
                assert_eq!(
                    got,
                    zset_of(&batch(&captured), &view_schema()),
                    "seed {seed} round {round}"
                );
            }
            let floor = r.relation_or_err(TID).unwrap().feed().unwrap().dropped_through();
            let rounds: Vec<u64> = log.iter().map(|(r, _)| *r).collect();
            for _ in 0..5 * usize::from(!rounds.is_empty()) {
                let case = &cases[rng.gen_range(cases.len() as u64) as usize];
                let pick = |rng: &mut Rng| rounds[rng.gen_range(rounds.len() as u64) as usize];
                let after = match rng.gen_range(8) {
                    0 => floor.max(1),
                    1 => floor.saturating_sub(1).max(1),
                    2 => floor + 1,
                    3 => round,
                    4 => round - 1,
                    _ => pick(&mut rng),
                };
                let out = if case.slim { slim_schema() } else { view_schema() };
                let reply = r.delta_read(TID, after, case.spec.clone(), out.layout_digest());
                if after < floor {
                    let Err(err) = reply else {
                        panic!("seed {seed}: cursor {after} below the floor {floor} was accepted")
                    };
                    assert!(matches!(err.status, WireStatus::DeltaExpired), "seed {seed}: {err:?}");
                    expired += 1;
                    continue;
                }
                let reply =
                    reply.unwrap_or_else(|e| panic!("seed {seed} {} after {after} floor {floor}: {e:?}", case.name));
                let mut band: HashMap<Row, i64> = HashMap::new();
                for (at, delta) in &log {
                    if after < *at {
                        for (row, w) in delta {
                            *band.entry(*row).or_default() += *w;
                        }
                    }
                }
                band.retain(|_, w| *w != 0);
                let mut got = HashMap::new();
                add(&mut got, &reply, &out);
                assert_eq!(
                    got,
                    expected(case, &band),
                    "seed {seed} {} after {after} floor {floor} round {round}",
                    case.name
                );
                reads += 1;
            }
        }
        let floor = r.relation_or_err(TID).unwrap().feed().unwrap().dropped_through();
        dropped_seeds += u64::from(floor > 0);
    }
    assert!(
        reads > 1000 && expired > 20 && dropped_seeds >= 4 && big_rounds > 100,
        "{reads} {expired} {dropped_seeds} {big_rounds}"
    );
}
