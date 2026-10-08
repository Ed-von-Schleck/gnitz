use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};

fn round_of(rows: u64) -> Rc<Batch> {
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (0..rows).map(|id| (id, 1, id as i64)).collect();
    Rc::new(make_batch_raw(&schema, &rows).into_consolidated())
}

fn lens(feed: &Feed, after: u64) -> Vec<usize> {
    feed.rounds(after).map(|b| b.len()).collect()
}

/// A read takes the rounds after its cursor, whatever rounds the feed skipped.
#[test]
fn rounds_are_read_past_a_cursor() {
    let mut feed = Feed::new(u64::MAX);
    for (round, rows) in [(3, 1), (4, 2), (9, 3), (9, 4), (12, 5)] {
        feed.record(round, round_of(rows));
    }
    assert_eq!(lens(&feed, 0), [1, 2, 3, 4, 5]);
    assert_eq!(lens(&feed, 3), [2, 3, 4, 5]);
    assert_eq!(lens(&feed, 8), [3, 4, 5]);
    assert_eq!(lens(&feed, 9), [5]);
    assert_eq!(lens(&feed, 12), [] as [usize; 0]);
    assert_eq!(feed.dropped_through(), 0);
}

/// The budget bounds the rows held: the oldest rounds go, whole, and the floor
/// is the last one gone. The newest round stays whatever it takes.
#[test]
fn the_oldest_rounds_go_past_the_budget() {
    let one = held(&round_of(10));
    let mut feed = Feed::new(3 * one as u64);
    for round in 1..=3 {
        feed.record(round, round_of(10));
    }
    assert_eq!((feed.dropped_through(), feed.retained_bytes()), (0, 3 * one));
    // Two deltas under one round are one round.
    feed.record(4, round_of(10));
    feed.record(4, round_of(10));
    assert_eq!(feed.dropped_through(), 2);
    assert_eq!(lens(&feed, 2), [10, 10, 10]);
    assert_eq!(feed.retained_bytes(), 3 * one);
    feed.record(5, round_of(10));
    assert_eq!((feed.dropped_through(), feed.retained_bytes()), (3, 3 * one));
    feed.record(6, round_of(10));
    assert_eq!(feed.dropped_through(), 4, "a round goes whole");
    assert_eq!(feed.retained_bytes(), 2 * one);

    feed.record(7, round_of(1000));
    assert_eq!(feed.dropped_through(), 6);
    assert_eq!(lens(&feed, 6), [1000], "the newest round outlives the budget");
    assert!(feed.retained_bytes() > 3 * one);
}

/// Against a brute-force list under the same rule: whole rounds go, oldest
/// first, while over budget, never the newest.
#[test]
fn a_feed_holds_what_a_list_under_its_rule_holds() {
    use gnitz_zset_testkit::Rng;
    for seed in 0..200u64 {
        let mut rng = Rng::new(seed + 1);
        let budget = 2_000 + rng.gen_range(40_000) as usize;
        let mut feed = Feed::new(budget as u64);
        let mut model: Vec<(u64, usize, usize)> = Vec::new(); // (round, rows, held)
        let mut floor = 0u64;
        let mut round = 1;
        for _ in 0..200 {
            if rng.gen_range(4) != 0 {
                round += 1 + rng.gen_range(3);
            }
            let rows = match rng.gen_range(5) {
                0 => 1,
                1 => 500 + rng.gen_range(2000),
                _ => 1 + rng.gen_range(60),
            };
            let delta = round_of(rows);
            let h = held(&delta);
            feed.record(round, delta);
            model.push((round, rows as usize, h));
            while model.iter().map(|m| m.2).sum::<usize>() > budget {
                let oldest = model[0].0;
                if oldest == round {
                    break;
                }
                floor = oldest;
                model.retain(|m| m.0 != oldest);
            }
            assert_eq!(feed.dropped_through(), floor, "seed {seed}");
            assert_eq!(feed.retained_bytes(), model.iter().map(|m| m.2).sum::<usize>());
            let after = rng.gen_range(round + 2);
            let want: Vec<usize> = model.iter().filter(|m| after < m.0).map(|m| m.1).collect();
            assert_eq!(lens(&feed, after), want, "seed {seed} after {after}");
        }
    }
}
