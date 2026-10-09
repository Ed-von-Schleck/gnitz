//! Voluntary context switches the server spends on "push one row, read a view
//! over it", per iteration, master and workers apart: one park each.

use super::*;
use gnitz_core::BatchAppender;
use gnitz_wire::{ReadBound, ReadSpec, WireConflictMode};

const WORKERS: usize = 4;
const ITERS: u64 = 2000;

/// Child pids of `pid`, over all its threads.
fn children(pid: u32) -> Vec<u32> {
    let mut out = Vec::new();
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let kids = std::fs::read_to_string(task.unwrap().path().join("children")).unwrap_or_default();
        out.extend(kids.split_whitespace().map(|p| p.parse::<u32>().unwrap()));
    }
    out
}

/// Voluntary context switches of every thread of `pid`.
fn voluntary_switches(pid: u32) -> u64 {
    let mut total = 0;
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let status = std::fs::read_to_string(task.unwrap().path().join("status")).unwrap();
        let line = status
            .lines()
            .find(|l| l.starts_with("voluntary_ctxt_switches"))
            .unwrap();
        total += line.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap();
    }
    total
}

/// A filter keeps its rows on the worker that holds them; a GROUP BY trades
/// them in an exchange round.
#[test]
#[ignore]
fn view_read_after_write_bench() {
    for (shape, body) in [
        ("filter", "SELECT pk, a FROM t WHERE a >= 0"),
        ("group by", "SELECT a, COUNT(*) AS n FROM t GROUP BY a"),
    ] {
        let mut db = Db::boot(WORKERS);
        db.exec("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)");
        db.exec(&format!("CREATE VIEW v AS {body}"));
        let (t, v) = (db.rel("t"), db.rel("v"));
        let all = ReadSpec::all_rows(ReadBound::None);

        let masters = children(std::process::id());
        assert_eq!(masters.len(), 1, "one server");
        let workers = children(masters[0]);
        assert_eq!(workers.len(), WORKERS);
        let sample = || {
            let of_workers = workers.iter().map(|&w| voluntary_switches(w)).sum::<u64>();
            (voluntary_switches(masters[0]), of_workers)
        };

        let (m0, w0) = sample();
        for i in 0..ITERS {
            let mut batch = ZSetBatch::new(&t.schema);
            BatchAppender::new(&mut batch)
                .add_row(i as u128, 1)
                .i64_val((i % 8) as i64);
            block_on(db.client.push(t.tid, &t.schema, batch, WireConflictMode::Update)).unwrap();
            let got = block_on(db.client.scan_spec(v.tid, &all, &v.schema)).unwrap().batch;
            let rows: i64 = got.weights.iter().sum();
            assert_eq!(
                rows as u64,
                (i + 1).min(if shape == "filter" { u64::MAX } else { 8 }),
                "{shape}"
            );
        }
        let (m1, w1) = sample();
        println!(
            "view_read_after_write [{shape}]: master {:.2} vcsw/iter  workers {:.2} vcsw/iter",
            (m1 - m0) as f64 / ITERS as f64,
            (w1 - w0) as f64 / ITERS as f64,
        );
    }
}
