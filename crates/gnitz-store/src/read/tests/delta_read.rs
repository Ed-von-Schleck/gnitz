//! A delta read under a spec. A subscriber that bootstraps under one and polls
//! under the same one holds that spec applied to the view, weight for weight.

use super::*;
use crate::relation::{RelationKind, RelationSpec, StoreConfig};
use crate::test_support::{
    batch_rows, between, cmp_all, col_range, cut, fed_view, id_rows, img, make_batch_raw, make_schema_u64_i64, map_of,
    pk_only_schema, pk_payload_schema, relation_fixture, relation_fixture_with, rows_spec, spec, Rng, TID,
};
use gnitz_expr::{CmpOp, LogicalProgram};
use gnitz_wire::{PkKeys, ReadSink, TypeCode, ViewBudget};
use gnitz_zset::algebra::Slot;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};
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

/// The fed view [`reads_past_any_cursor_match_a_model`] reads: its model
/// holds several rows under one key, some at weights above 1.
fn feed_spec(budget: u64) -> RelationSpec {
    RelationSpec {
        pk_repeats: true,
        ..spec(TID, fed_view(budget), view_schema())
    }
}

/// The reply of a projection onto `a`: `k2, k1 | a`.
fn slim_schema() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::I64, TypeCode::U64])
}

fn batch<'a>(rows: impl IntoIterator<Item = (&'a Row, &'a i64)>) -> Batch {
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

fn fresh(rng: &mut Rng) -> Row {
    (
        rng.gen_range(6),
        rng.gen_range(7) as i64 - 3,
        rng.gen_range(20) as i64 - 5,
        (rng.gen_range(3) > 0).then(|| rng.gen_range(4) as i64),
        rng.gen_range(20) as u8,
    )
}

/// One round of `ops` inserts, retractions and updates against `state`, which it
/// leaves at non-negative weights, netted.
fn delta(rng: &mut Rng, state: &mut HashMap<Row, i64>, ops: u64) -> HashMap<Row, i64> {
    let mut delta: HashMap<Row, i64> = HashMap::new();
    let mut add = |row: Row, w: i64, state: &mut HashMap<Row, i64>| {
        *delta.entry(row).or_default() += w;
        *state.entry(row).or_default() += w;
        state.retain(|_, w| *w != 0);
    };
    for _ in 0..ops {
        let kind = rng.gen_range(10);
        if kind < 6 || state.is_empty() {
            add(fresh(rng), 1 + rng.gen_range(3) as i64, state);
            continue;
        }
        // Not uniform over the live rows, but cheap: among the first in hash order.
        let skip = rng.gen_range(state.len().min(50) as u64) as usize;
        let (&row, &held) = state.iter().nth(skip).unwrap();
        let w = 1 + rng.gen_range(held as u64) as i64;
        add(row, -w, state);
        if kind >= 8 {
            // An UPDATE that keeps the key.
            let f = fresh(rng);
            add((row.0, row.1, f.2, f.3, f.4), w, state);
        }
    }
    delta.retain(|_, w| *w != 0);
    delta
}

struct Case {
    name: &'static str,
    spec: ReadSpec,
    keep: Box<dyn Fn(&Row) -> bool>,
}

impl Case {
    /// The layout its reply is in: the slim one under a map.
    fn out(&self) -> SchemaDescriptor {
        match self.spec.sink.map {
            Some(_) => slim_schema(),
            None => view_schema(),
        }
    }

    /// What the spec keeps of `rows`, in the reply's layout: rows that differ only in
    /// what a map drops are one mapped row.
    fn expect(&self, rows: &HashMap<Row, i64>) -> HashMap<RowKey, i64> {
        let kept: HashMap<Row, i64> = rows
            .iter()
            .filter(|(r, _)| (self.keep)(r))
            .map(|(r, w)| (*r, *w))
            .collect();
        let out = self.out();
        match self.spec.sink.map {
            Some(_) => zset_of(&slim_batch(&kept), &out),
            None => zset_of(&batch(&kept), &out),
        }
    }
}

fn cases() -> Vec<Case> {
    let case = |name, bound, predicate, slim: bool, keep: Box<dyn Fn(&Row) -> bool>| {
        let map = slim
            .then(|| map_of(LogicalProgram::copy_cols(&[0]), &slim_schema()))
            .flatten();
        let sink = ReadSink { map, kind: SinkKind::Rows { cut: None } };
        Case {
            name,
            spec: ReadSpec { bound, predicate, sink },
            keep,
        }
    };
    // `k2 >= lo AND b > 1`: a key column and a nullable payload column.
    let k2_ge_and_b = |lo| cmp_all(&[(CmpOp::Ge, 3, lo), (CmpOp::Gt, 2, 1)]).to_blob_bytes();
    // A key's image is `k2`'s, then `k1`'s.
    let keys = |set: &[(i64, u64)]| {
        let images = set.iter().map(|&(k2, k1)| img(k2) << 64 | k1 as u128);
        ReadBound::PkSet(PkKeys::from_images(16, images))
    };
    const SET: [(i64, u64); 6] = [(-3, 0), (-1, 2), (0, 0), (0, 5), (2, 1), (3, 3)];
    // Most keys the model draws, so a round's keyed rows are most of it.
    let wide: Vec<(i64, u64)> = (-3..=3).flat_map(|k2| (0..5).map(move |k1| (k2, k1))).collect();
    let range = col_range(3, img(-1), img(1));
    let none = || ReadBound::None;
    vec![
        case("identity", none(), vec![], false, Box::new(|_| true)),
        case(
            "payload predicate",
            none(),
            between(0, 0, 8),
            false,
            Box::new(|r| (0..8).contains(&r.2)),
        ),
        case(
            "pk + nullable predicate",
            none(),
            k2_ge_and_b(0),
            false,
            Box::new(|r| r.1 >= 0 && r.3.is_some_and(|b| b > 1)),
        ),
        case("projection", none(), vec![], true, Box::new(|_| true)),
        case(
            "predicate + projection",
            none(),
            k2_ge_and_b(-1),
            true,
            Box::new(|r| r.1 >= -1 && r.3.is_some_and(|b| b > 1)),
        ),
        case(
            "pk range",
            range,
            between(0, -2, 12),
            false,
            Box::new(|r| (-1..2).contains(&r.1) && (-2..12).contains(&r.2)),
        ),
        case(
            "pk set",
            keys(&SET),
            vec![],
            false,
            Box::new(|r| SET.contains(&(r.1, r.0))),
        ),
        case(
            "wide pk set + projection",
            keys(&wide),
            between(0, -5, 9),
            true,
            Box::new(|r| r.0 < 5 && (-5..9).contains(&r.2)),
        ),
        case(
            "pk set + predicate + projection",
            keys(&SET),
            between(0, 0, 10),
            true,
            Box::new(|r| SET.contains(&(r.1, r.0)) && (0..10).contains(&r.2)),
        ),
    ]
}

fn add(acc: &mut HashMap<RowKey, i64>, b: &Batch, schema: &SchemaDescriptor) {
    for (k, w) in zset_of(b, schema) {
        *acc.entry(k).or_default() += w;
    }
    acc.retain(|_, w| *w != 0);
}

/// One subscriber: a case's copy of the view, and the cursor it polls from.
struct Subscriber {
    case: usize,
    lagging: bool,
    copy: HashMap<RowKey, i64>,
    cursor: u64,
}

/// Every case read past random cursors, and held by two subscribers — one polling
/// every few rounds, one that bootstraps early and polls once at the end — over
/// rounds of every size, under budgets that drop rounds, every RAM tier and chunk
/// size. A read below the retained floor is refused as expired, and a subscriber so
/// refused discards its copy and bootstraps at 0, as every subscriber must. A round's
/// delta sometimes arrives as two ingests, and an ingest that consolidates to nothing
/// captures nothing. A mapped reply is consolidated: an UPDATE of a column the map
/// drops ships nothing.
#[test]
fn reads_past_any_cursor_match_a_model() {
    let (mut reads, mut polls, mut rebooted, mut expired, mut dropped_seeds, mut big_rounds) = (0, 0, 0, 0, 0, 0);
    // Twelve seeds: every budget under every RAM tier and chunk size.
    for seed in 0..12u64 {
        let mut rng = Rng::new(seed * 104_729 + 7);
        let budget = [1 << 30, 400_000, 60_000][(seed % 3) as usize];
        let config = StoreConfig {
            ram_tier_bytes: [1 << 11, StoreConfig::default().ram_tier_bytes][(seed % 2) as usize],
            ..StoreConfig::default()
        };
        let mut r = relation_fixture_with(config, Slot::SOLO, feed_spec(budget), view_schema(), &[], []);
        r.set_scan_chunk_rows([1, 3, 200, 8192][(seed % 4) as usize]);
        let cases = cases();
        let mut subs: Vec<Subscriber> = (0..cases.len())
            .flat_map(|case| [false, true].map(|lagging| (case, lagging)))
            .map(|(case, lagging)| Subscriber {
                case,
                lagging,
                copy: HashMap::new(),
                cursor: 0,
            })
            .collect();
        let mut state = HashMap::new();
        // Per ingest: its round and its net delta.
        let mut log: Vec<(u64, HashMap<Row, i64>)> = Vec::new();
        let mut round = 1;
        const STEPS: usize = 50;
        for step in 0..STEPS {
            // The round counter is the master's, shared by every relation.
            round += 1 + rng.gen_range(3);
            let ops = match rng.gen_range(6) {
                0 => 1 + rng.gen_range(4),
                1 => 100 + rng.gen_range(80),
                2 => 300 + rng.gen_range(500),
                3 => 120 + rng.gen_range(20),
                _ => 10 + rng.gen_range(40),
            };
            let delta = delta(&mut rng, &mut state, ops);
            big_rounds += u64::from(delta.len() >= 128);
            // A view stepped twice in one schedule ingests twice in one round.
            let (first, second): (HashMap<Row, i64>, HashMap<Row, i64>) = match rng.gen_range(4) {
                0 => delta.into_iter().partition(|(row, _)| row.0 % 2 == 0),
                _ => (delta, HashMap::new()),
            };
            for part in [first, second].into_iter().filter(|part| !part.is_empty()) {
                r.ingest_round(TID, batch(&part), round, false).unwrap();
                log.push((round, part));
            }
            if rng.gen_range(5) == 0 {
                let ghost: Row = (1, 1, 1, None, 1);
                r.ingest_round(TID, batch([(&ghost, &2), (&ghost, &-2)]), round, false)
                    .unwrap();
            }

            for sub in subs.iter_mut() {
                let due = match sub.lagging {
                    true => step == 3 || step == STEPS - 1,
                    false => rng.gen_range(6) == 0,
                };
                if !due {
                    continue;
                }
                let case = &cases[sub.case];
                let (out, spec) = (case.out(), || case.spec.clone());
                let reply = match r.delta_read(TID, sub.cursor, spec(), out.layout_digest()) {
                    Err(e) if matches!(e.status, WireStatus::DeltaExpired) => {
                        sub.copy.clear();
                        rebooted += 1;
                        r.delta_read(TID, 0, spec(), out.layout_digest())
                    }
                    reply => reply,
                };
                let reply = reply.unwrap_or_else(|e| panic!("seed {seed} {}: {e:?}", case.name));
                add(&mut sub.copy, &reply, &out);
                sub.cursor = round;
                polls += 1;
                assert_eq!(
                    sub.copy,
                    case.expect(&state),
                    "seed {seed} {} lagging={} round {round}",
                    case.name,
                    sub.lagging
                );
            }

            let floor = r.relation_or_err(TID).unwrap().feed().unwrap().dropped_through();
            let rounds: Vec<u64> = log.iter().map(|(r, _)| *r).collect();
            for _ in 0..3 * usize::from(!rounds.is_empty()) {
                let case = &cases[rng.gen_range(cases.len() as u64) as usize];
                let after = match rng.gen_range(8) {
                    0 => floor.max(1),
                    1 => floor.saturating_sub(1).max(1),
                    2 => floor + 1,
                    3 => round,
                    4 => round - 1,
                    _ => rounds[rng.gen_range(rounds.len() as u64) as usize],
                };
                let out = case.out();
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
                for (row, w) in log.iter().filter(|(at, _)| after < *at).flat_map(|(_, d)| d) {
                    *band.entry(*row).or_default() += *w;
                }
                band.retain(|_, w| *w != 0);
                let got = zset_of(&reply, &out);
                let what = format!("seed {seed} {} after {after} floor {floor} round {round}", case.name);
                assert_eq!(got, case.expect(&band), "{what}");
                if case.spec.sink.map.is_some() {
                    assert_eq!(reply.len(), got.len(), "{what}: a mapped reply is consolidated");
                }
                reads += 1;
            }
        }
        let floor = r.relation_or_err(TID).unwrap().feed().unwrap().dropped_through();
        dropped_seeds += u64::from(floor > 0);
    }
    assert!(
        reads > 1200 && polls > 900 && rebooted > 100 && expired > 150 && dropped_seeds >= 4 && big_rounds > 150,
        "{reads} {polls} {rebooted} {expired} {dropped_seeds} {big_rounds}"
    );
}

/// A delta read answers the rounds after `after_tick` at each round's own weights,
/// both sides of a pair the output store folded away; `after_tick = 0` reads the
/// view whole. A view that is all key has no payload column to copy: its delta rows
/// are their keys and weights.
#[test]
fn a_delta_read_answers_the_rounds_past_its_cursor() {
    let schema = make_schema_u64_i64();
    let mut r = relation_fixture(fed_view(1 << 20), schema, &[], []);
    r.ingest_round(TID, make_batch_raw(&schema, &[(7, 1, 70)]), 4, false)
        .unwrap();
    r.ingest_round(TID, make_batch_raw(&schema, &[(7, -1, 70), (8, 1, 80)]), 5, false)
        .unwrap();
    let whole = || ReadSpec::all_rows(ReadBound::None);
    let read = |after_tick| batch_rows(&r.delta_read(TID, after_tick, whole(), schema.layout_digest()).unwrap());
    assert_eq!(read(3), [(7, -1, 70), (7, 1, 70), (8, 1, 80)]);
    assert_eq!(read(4), [(7, -1, 70), (8, 1, 80)]);
    assert_eq!(read(5), []);
    assert_eq!(read(0), [(8, 1, 80)], "the output store folded the pair away");

    let keys = pk_only_schema(&[TypeCode::U64]);
    let mut r = relation_fixture(fed_view(1 << 20), keys, &[], []);
    r.ingest_round(TID, id_rows(&keys, [(7, 1)], |_, _| {}), 4, false)
        .unwrap();
    r.ingest_round(TID, id_rows(&keys, [(7, -1), (8, 1)], |_, _| {}), 5, false)
        .unwrap();
    let b = r.delta_read(TID, 3, whole(), keys.layout_digest()).unwrap();
    let mut rows: Vec<_> = (0..b.len())
        .map(|i| (gnitz_wire::widen_pk_be(b.get_pk_bytes(i)) as u64, b.get_weight(i)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, [(7, -1), (7, 1), (8, 1)]);
}

/// A delta read is refused for another layout at every cursor — before the floor is
/// consulted — for a view with no feed, for a cut, which a delta read cannot forward,
/// and for a key list of another width.
#[test]
fn a_delta_read_refuses_what_it_cannot_answer() {
    let fed = relation_fixture(fed_view(1 << 30), view_schema(), &[], []);
    let plain = relation_fixture(RelationKind::View(ViewBudget::Plain), view_schema(), &[], []);
    let own = view_schema().layout_digest();
    let whole = || ReadSpec::all_rows(ReadBound::None);
    let first = || rows_spec(None, cut(1, vec![]));
    let short = ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_images(8, [0])));
    let cases = [
        (&fed, 0, whole(), own ^ 1, "reply schema does not match"),
        (&fed, 3, whole(), own ^ 1, "reply schema does not match"),
        (&plain, 0, whole(), own, "holds no delta feed"),
        (&fed, 0, first(), own, "uncut"),
        (&fed, 1, first(), own, "uncut"),
        (&fed, 1, short, own, "stride"),
    ];
    for (r, after_tick, spec, layout, why) in cases {
        let Err(err) = r.delta_read(TID, after_tick, spec.clone(), layout) else {
            panic!("{spec:?} at {after_tick} must be refused");
        };
        assert!(matches!(err.status, WireStatus::Error), "{err:?}");
        assert!(err.text.contains(why), "{err:?}");
    }
}

/// Rounds of alternating small and large sizes, read past a cursor under a
/// filter at a chunk size a run of small rounds fills exactly: every round's rows
/// arrive once, at their own weights.
#[test]
fn a_delta_read_reads_each_round_once_however_rounds_are_chunked() {
    let schema = make_schema_u64_i64();
    let mut r = relation_fixture(fed_view(1 << 30), schema, &[], []);
    r.set_scan_chunk_rows(10);
    // 4 + 4 + 2 fills a chunk of 10; 1 + 7 leaves no room for the 8 after it.
    let sizes = [3, 5, 200, 4, 4, 2, 130, 1, 7, 8, 129, 10, 1];
    let mut want = Vec::new();
    let mut id = 0u64;
    for (at, &n) in sizes.iter().enumerate() {
        let weight = at as i64 + 1;
        let rows: Vec<_> = (0..n)
            .map(|_| {
                id += 1;
                (id, weight, (id % 7) as i64)
            })
            .collect();
        want.extend(rows.iter().copied().filter(|&(_, _, val)| val < 5));
        r.ingest_round(TID, make_batch_raw(&schema, &rows), at as u64 + 2, false)
            .unwrap();
    }
    want.sort_unstable();
    let spec = ReadSpec {
        predicate: between(1, 0, 5),
        ..rows_spec(None, None)
    };
    let got = r.delta_read(TID, 1, spec, schema.layout_digest()).unwrap();
    assert_eq!(batch_rows(&got), want);
}
