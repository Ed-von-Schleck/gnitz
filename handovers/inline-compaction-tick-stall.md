# Inline LSM compaction stalls the worker for seconds

Research handover. Everything under **Validated** was proved from source or measured
on this machine during the session; everything under **Open** was not.

## Goal

Decide and then implement how a store's L0 compaction stops being an unbounded,
synchronous unit of work executed on the worker's only thread inside a view tick.
The work itself is inherent LSM write amplification and is not the target — its
*burstiness* is. Today one tick in ~1300 pays for the ~160 MiB of ingest that the
other 1300 performed.

## Validated: the call chain

Per tick, every dirty view's output store is flushed on the worker thread:

- `query/dag/exec.rs:378` — `for vid in dirty_views { self.flush_view_or_abort(vid); }`
- `query/dag/ingest.rs:145` → `DagRegistry::flush(view_id)` (`ingest.rs:125`) → `entry.handle.flush()`
- `query/dag/store_handle.rs:147` → `Table::flush()`
- `storage/lsm/table/flush.rs:28` — `Table::flush()` *is* `flush_barrier([self], FlushRound::Base)`
- `flush_prepare` (`flush.rs:93`): a `Rederive` store (every view output store) on the
  Base round runs `flush_to_ram()` and returns `FlushOutcome::Done`
- `flush_to_ram` (`flush.rs:60`) → `spill_in_memory_to_disk()` once the RAM tier is full
- `persist_l0_run` (`flush.rs:133`) writes the shard, registers it, and then calls
  `self.compact_if_needed()` at `flush.rs:172` — its own comment: *"The only path that
  grows L0, so the only place its fan-in can cross `L0_COMPACT_THRESHOLD`."*
- `Table::compact_if_needed` (`storage/lsm/table/mod.rs:652`) → `ShardIndex::run_compact`
  (`storage/lsm/shard_index/index.rs:278`)

`run_compact` is not bounded: it merges **all** of L0, then runs
`compact_guards_if_needed()` across every level, then `compact_guard_vertical()` when
`levels[0].total_file_count() > L1_TARGET_FILES`.

Thresholds (`storage/lsm/shard_index/mod.rs:23-32`): `MAX_LEVELS=3`,
`L0_COMPACT_THRESHOLD=4`, `GUARD_FILE_THRESHOLD=4`, `LMAX_FILE_THRESHOLD=1`,
`L1_TARGET_FILES=16`. RAM-tier ceiling `INMEM_CEILING = 32 MiB`
(`storage/lsm/table/mod.rs:43`, override `GNITZ_RAM_TIER_BYTES`).

`query/vm/mod.rs:225` states the position outright: *"there is no background compactor."*

## Validated: nothing else on that worker runs meanwhile

The worker is a single blocking loop — `sal_reader.wait(1000)` then `drain_sal()`
(`runtime/orchestration/worker/mod.rs:360-390`), and `drain_sal` dispatches SAL
messages one at a time (`worker/mod.rs:394-420`). There is no `std::thread::spawn`
anywhere in `gnitz-engine` outside `#[cfg(test)]` code. So for the whole duration of a
compaction the worker answers no tick ACK, no seek, and no scan; the master's tick
`join_all`s those ACKs (`runtime/orchestration/executor.rs:1-14`).

## Validated: measured cost

Machine: 12 cores, 60 GiB RAM, btrfs (`compress=zstd:1`) over LUKS, `powersave`
governor. Release build, load average < 2 during the runs. **These absolute numbers
are this box's; the production filesystem is XFS and was not measured.**

### Storage unit level

A temporary `#[ignore]`d bench (source at the end of this file) drove 32,768,000 rows
(1250 MiB raw at 40 B/row) as 8000 ticks of 4096 rows into one `Rederive` store at the
production 32 MiB ceiling, timing every `Table::flush()`:

```
rows 32768000 (1250 MiB raw)  wall 20.92s  ticks 8000
per-tick flush ms: p50 0.002  p90 0.002  p99 48.884  p99.9 111.725  max 2843.5
spills:      33 events, 2.86s total ( 86.5 ms avg)
compactions:  6 events, 5.85s total (974.4 ms avg, 27.9% of wall)
live shards at end: 11
```

The median tick costs 2 µs and the worst costs 2.84 s. **27.9% of the entire ingest
wall time is spent inside 6 events out of 8000.** Two earlier runs of the same bench on
a busy box produced maxima of 4887 ms and 7181 ms, so the figure scales with
contention.

### Client-visible, through the real server

Load driver (source at the end of this file): release server, `--workers=4`, one table
plus one `GROUP BY` view, `INSERT … VALUES` of 1000 rows per statement, 180 s:

```
request ms: p50 7.54  p90 10.31  p99 13.01  p99.9 63.17  max 2342.2
wall 180.00s  frames 22467 (125/s)  rows 22467000 (124816/s)  batch 1000  workers 4
```

A single INSERT blocked for **2.34 s**, 310× the median. Repeating for 120 s with
`GNITZ_CHECKPOINT_BYTES=900000000` (raised so the run writes less than the threshold)
still gave `max 2084.2 ms`, and the storage-unit bench above has no SAL and no
checkpoint at all — so the tail is the spill/compaction path, not the checkpoint.

### Related, already documented

After the 1250 MiB unit run, `tmp/bench_compact2/t/` held 39 plain spill shards
(~20 MB each, ~780 MB) while the index reported 11 live shards. This is the
documented design, not a defect: superseded compaction inputs go to
`pending_deletions` and the only production drain is `flush_barrier.rs:79`, which runs
only for a table that published a manifest — which a `Rederive` store on the Base
round never does (`flush_prepare` returns `Done` first). `shard_index/index.rs:466-472`
says so: *"bytes on disk exceed this by the compaction garbage accumulated since the
last checkpoint."* Relevant here only because a pacing change alters when that garbage
is produced.

## Open — must be answered before designing

1. **Can the work be sliced, and where does a slice boundary go?** `run_compact` is
   three nested units (L0 merge → per-guard folds → vertical fold); `compact_one_guard`
   (`shard_index/index.rs:363`) already operates on one guard. Whether a partially
   completed cascade leaves a valid index, and whether the CLAUDE.md "fold totality"
   precondition for capacity-bounded views survives a *partial* compaction, is not
   analysed. CLAUDE.md explicitly warns: *"A future partial compaction that broke it
   would corrupt bounded views."* This is the first thing to settle.
2. **Where can a worker yield?** Compaction currently runs inside a tick, where the VM
   holds trace cursors (`query/vm/mod.rs:222-235` explains compaction is the epoch
   path's job precisely because it mutates shard state). Whether a deferred unit can
   run at the top of `drain_sal` instead — with no cursors open — was not verified.
3. **What bounds L0 while compaction is deferred?** Read amplification grows with L0
   fan-in; no measurement of the read-path cost per extra L0 shard was taken.
4. **Base tables.** Only the per-tick view-output path (`Rederive`) was traced and
   measured. `SalReplay` stores reach `persist_l0_run` through the same
   `flush_prepare`, but when their flush runs, and how large those compactions are,
   was not established.
5. **Is the merge itself unnecessarily slow?** 5.85 s of compaction for 1250 MiB of
   ingest was measured but not profiled; no claim is made about whether the merge is
   near hardware limits.

## Where to continue

- `storage/lsm/shard_index/index.rs` — `should_compact`, `run_compact`,
  `compact_guards_if_needed`, `compact_guard_vertical`, `compact_one_guard`.
- `storage/lsm/table/flush.rs:133-185` — `persist_l0_run`, the sole trigger site.
- `runtime/orchestration/worker/mod.rs:360-420` — the loop that would host a paced unit.
- `query/vm/mod.rs:222-235` — why the trigger sits on the epoch path today.

## Reproducing the measurements

Both harnesses were reverted; the tree is unchanged. Recreate them as follows.

`crates/gnitz-engine/src/storage/lsm/table/bench_compaction.rs`, registered with
`#[cfg(test)] mod bench_compaction;` next to `mod bench_flush;` in
`storage/lsm/table/mod.rs:219`. Run with:

```bash
cd crates && GNITZ_BENCH_DIR=$(realpath ../tmp)/bench_compact \
  cargo test -p gnitz-engine --release compaction_stall_bench \
  -- --ignored --nocapture --test-threads=1
```

```rust
use super::super::batch::Batch;
use super::{RecoverySource, Table};
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

fn make_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn push_row(b: &mut Batch, pk: u64, payload: i64, weight: i64) {
    b.extend_pk(pk as u128);
    b.extend_weight(&weight.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &payload.to_le_bytes());
    b.extend_col(1, &payload.to_le_bytes());
    b.count += 1;
}

#[test]
#[ignore = "benchmark"]
fn compaction_stall_bench() {
    use std::time::Instant;

    let schema = make_schema();
    let rows_per_tick: usize = 4096;
    let ticks: usize = 8000;

    let dir = std::path::PathBuf::from(std::env::var("GNITZ_BENCH_DIR").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let mut table = Table::new(
        dir.join("t").to_str().unwrap(),
        schema,
        7,
        RecoverySource::Rederive { resume_at: None },
    )
    .unwrap();

    let mut all_ms: Vec<f64> = Vec::with_capacity(ticks);
    let (mut spill_ms, mut compact_ms) = (0.0f64, 0.0f64);
    let (mut n_spill, mut n_compact) = (0usize, 0usize);
    let mut key: u64 = 0;
    let t_all = Instant::now();
    for _ in 0..ticks {
        let mut b = Batch::with_capacity(schema, rows_per_tick);
        for _ in 0..rows_per_tick {
            push_row(&mut b, key, key as i64, 1);
            key += 1;
        }
        table.ingest_owned_batch(b).unwrap();
        let before = table.all_shard_arcs().len();
        let t0 = Instant::now();
        table.flush().unwrap();
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        let after = table.all_shard_arcs().len();
        if after < before {
            compact_ms += ms;
            n_compact += 1;
        } else if after > before {
            spill_ms += ms;
            n_spill += 1;
        }
        all_ms.push(ms);
    }
    let wall = t_all.elapsed().as_secs_f64();
    all_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| all_ms[((all_ms.len() - 1) as f64 * p) as usize];
    let rows = ticks * rows_per_tick;
    println!("rows {} ({:.0} MiB raw)  wall {:.2}s  ticks {}", rows, rows as f64 * 40.0 / 1048576.0, wall, ticks);
    println!(
        "per-tick flush ms: p50 {:.3}  p90 {:.3}  p99 {:.3}  p99.9 {:.3}  max {:.1}",
        pct(0.50), pct(0.90), pct(0.99), pct(0.999), all_ms[all_ms.len() - 1]
    );
    println!("spills: {} events, {:.2}s total ({:.1} ms avg)", n_spill, spill_ms / 1000.0,
        if n_spill > 0 { spill_ms / n_spill as f64 } else { 0.0 });
    println!("compactions: {} events, {:.2}s total ({:.1} ms avg, {:.1}% of wall)",
        n_compact, compact_ms / 1000.0,
        if n_compact > 0 { compact_ms / n_compact as f64 } else { 0.0 },
        100.0 * compact_ms / 1000.0 / wall);
    println!("live shards at end: {}", table.all_shard_arcs().len());
}
```

The end-to-end driver is a standalone script (kept out of the tree). It spawns
`gnitz-server-release` into a fresh `tmp/` directory, creates one table and one
`GROUP BY` view, runs `INSERT … VALUES` in a loop for `SECS`, and reports request
latency percentiles plus per-process CPU from `/proc/<pid>/stat` and migrations from
`/proc/<pid>/sched`. It must be run from `crates/gnitz-py` so `import gnitz` resolves
to the maturin-installed extension:

```bash
cd crates/gnitz-py && SECS=180 BATCH=1000 W=4 uv run python <script>
```
