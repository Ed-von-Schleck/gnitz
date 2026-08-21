# Four TigerBeetle-derived ideas, measured and rejected

Companion record to `inline-compaction-tick-stall.md` and
`release-build-invariant-checks.md`. These four were investigated to the same depth and
did **not** clear the bar. Kept so the measurements are not repeated.

Machine for every measurement: 12 cores, 60 GiB RAM, btrfs (`compress=zstd:1`) over
LUKS, `powersave` governor, load average < 3 unless noted. Release build.

## 1. Start shard writeback early (`sync_file_range`) instead of deferring to the checkpoint

**What is true.** Shard images are written with buffered `write_all_at`
(`storage/repr/shard_file.rs:622-634`). There is no `O_DIRECT`, no `sync_file_range` and
no `posix_fadvise` anywhere in the workspace. A new shard registers unswept
(`storage/lsm/shard_index/index.rs:43`) and stays in `unsynced_paths()` (`index.rs:68`)
until a barrier fsyncs the whole list at once (`storage/lsm/flush_barrier.rs:84-135`).
So dirty pages accumulate and the checkpoint pays for all of them.

**Measured.** 800 MiB written as 40 × 20 MiB files, then `fdatasync` on each, three
reps, with and without `sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WRITE)` after each
file:

| | write phase | fsync phase | total |
|---|---|---|---|
| current behaviour | 0.99–1.06 s | **0.52–0.62 s** | 1.51–1.66 s |
| + early writeback | 1.38–1.42 s | **0.11–0.18 s** | 1.51–1.60 s |

**Why rejected.** It is a cost *shift*, not a saving: ~0.4 s moves out of the checkpoint
into the write path, and the same single-threaded worker pays either way. Total is
unchanged within noise. The shift is mildly favourable in principle (a checkpoint is a
cluster-wide quiesce, a spill blocks one worker) but it is ~0.4 s per ~800 MiB against
the 974 ms-per-event compaction stalls documented in the companion handover — an order
of magnitude smaller, for a change to the durability path.

**Note.** `O_DIRECT` proper is additionally blocked by the shard layout:
`storage/repr/layout.rs:24-26` sets `HEADER_SIZE = ALIGNMENT = 64`, so regions are
cache-line aligned, not sector aligned. It would need a format change and a
`SHARD_VERSION` bump. The version of this idea that would be a real saving — issuing
shard writes asynchronously so they overlap with computation — requires the worker to
have an event loop, which it does not (`runtime/orchestration/worker/mod.rs:360-390` is
a blocking futex loop). That is a much larger change than the one measured here.

## 2. io_uring multishot recv / registered buffers / registered files

**What is true.** The reactor uses exactly `Recv`, `Send`, `AcceptMulti`, `Fsync`,
`Timeout`, `AsyncCancel`, `FutexWaitV`, all on `types::Fd` (`runtime/reactor/uring.rs`).
No `.register` call exists anywhere: no registered files, no provided-buffer ring, no
SQPOLL. A client frame costs at least two SQE/CQE pairs — a 4-byte header recv
(`runtime/reactor/conn.rs:239`), then a payload recv (`conn.rs:370,394`), then the header
re-arm (`conn.rs:406`) — plus one malloc per inbound payload (`runtime/reactor/io.rs:33`).
SQEs are already batched into one `io_uring_enter` per loop iteration
(`runtime/reactor/runloop.rs:52-58`), so the syscall itself is amortized.

**Measured.** Sustained load, `--workers=4`, `INSERT` of 100 rows per statement:
556 frames/s, 55,600 rows/s, **master CPU 766 µs per frame**. At 1000 rows per
statement: 125 frames/s, 124,800 rows/s, **master CPU 1315 µs per frame**. The peak
frame rate observed anywhere in the benchmark suite was 556/s
(`test_insert_bulk_throughput`, 55,646 rows/s at 100 rows/frame).

**Why rejected.** The entire prize — one SQE write plus one malloc/free per frame — is
on the order of 10²  ns against 10⁶ ns of master CPU already spent per frame. Four
orders of magnitude. Nothing about the workload's shape can close that: the system is
latency-bound (p50 1.9 ms per round trip at 100 rows) and would need a frame rate
roughly 10⁵× higher before the recv path registered.

## 3. Pin worker processes to cores

**What is true.** No `sched_setaffinity`, no `CPU_SET`, no affinity handling of any kind
exists in the workspace. Workers are forked processes each running a blocking loop.
Migration rates under load are high in absolute terms: master 146–187/s, each worker
47–64/s at 550 frames/s.

**Measured.** Same load driver, `--workers=4`, 15 s, three interleaved reps, comparing
free scheduling against `taskset -cp` pinning the master to CPU 0 and each worker to
CPUs 1–4:

| | migrations | master CPU/frame | rows/s |
|---|---|---|---|
| free | 146, 174, 172 /s | 737.6, 707.0, 731.3 µs | 57033, 57136, 57249 |
| pinned | 0.1, 0.0, 0.1 /s | 730.4, 717.2, 721.5 µs | 55854, 56330, 56176 |

**Why rejected.** Pinning eliminates 100% of migrations and buys nothing: master CPU per
frame moves −0.3% (inside noise) and throughput is **1.8% worse**, consistently across
all three reps. On this hardware a migration is cheap — single socket, shared L3, and
each process needs well under one core, so the scheduler's freedom to pick an idle core
is worth more than cache locality.

**Scope — and why this rejection is provisional.** The test box is an AMD Ryzen 5 7640U
laptop: 6 physical cores, 1 socket, 1 NUMA node, `powersave` governor, with no process
above 42% of a core. `cpu0` and `cpu1` are SMT siblings (both report `0-1` in
`thread_siblings_list`), so the naive mapping used here put the master and worker 0 on
one physical core. None of the conditions under which pinning should pay — saturated
cores, many workers, cross-socket memory — were present. This item is therefore carried
forward rather than closed: see `handovers/worker-cpu-affinity.md`.

## 4. Verify shard checksums on the read path / survive a bad sector

**What is true.** Every shard region carries an xxh3 checksum written at
`storage/repr/shard_file.rs:554`, but `MappedShard::open`'s `validate_checksums`
parameter is `false` on the production registration path
(`storage/lsm/shard_index/mod.rs:66`) and `true` only for compaction inputs
(`storage/lsm/compact.rs:26`) and the boot relayout (`storage/lsm/child_dir.rs:300`).
`shard_reader/open.rs:6-11` states the reason: the payload regions are demand-paged, so
hashing them at open would fault in a whole shard for a point lookup. The descriptive
prefix and the XOR8 filter *are* verified unconditionally. Reads go through
`Mmap::open_ro` (`foundation/posix_io.rs:465`), so an unreadable sector is a SIGBUS —
`storage/spill.rs:272-274` says so explicitly — and no SIGBUS handler is installed
(only SIGTERM/SIGINT, `runtime/orchestration/executor.rs:658-663`).

**Measured.** xxh3 runs at **39.1 GiB/s** on this box (512 MiB in 0.013 s), so
verification CPU is not the obstacle; the obstacle is exactly the one the code names —
region granularity is a whole column, so verify-on-read means faulting in the whole
region.

**Why rejected, for now.** There is nothing to repair from: gnitz is single-node and
`WITH (replicated = true)` is intra-node fan-out, not a consensus group. The best
achievable outcome is turning a silent wrong answer (or an uncatchable SIGBUS) into a
diagnosable fail-stop. The technically sound version — per-block checksums so
verification granularity matches paging granularity — is a shard format change, and
CLAUDE.md's pre-alpha rule means that change is no more expensive later than now, so
there is no "do it while the format is malleable" argument. Revisit when replication
exists, or if the deployment target's filesystem is confirmed to provide no data
checksums of its own.
