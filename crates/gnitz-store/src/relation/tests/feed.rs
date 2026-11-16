use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};
use gnitz_foundation::rng::Rng;

fn round_of(rows: u64) -> Rc<Batch> {
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (0..rows).map(|id| (id, 1, id as i64)).collect();
    Rc::new(make_batch_raw(&schema, &rows).into_consolidated())
}

fn lens(feed: &Feed, after: u64) -> Option<Vec<usize>> {
    feed.rounds(after).map(|rounds| rounds.map(|b| b.len()).collect())
}

/// Against the whole history: a feed holds the longest run of the newest whole
/// rounds within budget — the newest round alone when even that is over — and
/// its floor is the round before them.
#[test]
fn a_feed_holds_its_newest_whole_rounds_within_budget() {
    for seed in 0..200u64 {
        let mut rng = Rng::new(seed + 1);
        let budget = 2_000 + rng.gen_range(40_000) as usize;
        let mut feed = Feed::new(budget as u64);
        let mut history: Vec<(u64, usize, usize)> = Vec::new(); // (round, rows, held)
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
            history.push((round, rows as usize, held(&delta)));
            feed.record(round, delta);

            // The first history entry the feed still holds: the newest round's,
            // or an older round boundary whose suffix fits the budget.
            let mut first = history.partition_point(|m| m.0 < round);
            while first > 0 {
                let older = history.partition_point(|m| m.0 < history[first - 1].0);
                if history[older..].iter().map(|m| m.2).sum::<usize>() > budget {
                    break;
                }
                first = older;
            }
            let (floor, held_now) = (first.checked_sub(1).map_or(0, |i| history[i].0), &history[first..]);
            assert_eq!(feed.dropped_through(), floor, "seed {seed}");
            assert_eq!(feed.bytes, held_now.iter().map(|m| m.2).sum::<usize>());
            let after = rng.gen_range(round + 2);
            let want: Vec<usize> = held_now.iter().filter(|m| after < m.0).map(|m| m.1).collect();
            match lens(&feed, after) {
                None => assert!(after < floor, "seed {seed} after {after}: refused at the floor"),
                Some(got) => {
                    assert!(after >= floor, "seed {seed} after {after}: answered below the floor");
                    assert_eq!(got, want, "seed {seed} after {after}");
                }
            }
        }
    }
}
