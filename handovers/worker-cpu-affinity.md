# Pin worker processes to cores

Research handover. Everything under **Validated** was proved from source or measured
on this machine during the session; everything under **Open** was not. The measurement
here is a *negative* result on unrepresentative hardware — read the scope limits before
giving it any weight.

## Goal

Give each forked worker a stable CPU, under a policy that respects SMT topology, NUMA
locality and cgroup cpuset limits, and measure it on server hardware where the premise
actually holds: many cores, cores saturated, and possibly more than one socket.

## Validated: there is no affinity handling of any kind

`sched_setaffinity`, `sched_getaffinity`, `cpu_set_t`, `CPU_SET` — none of these appear
anywhere in the workspace. Nothing queries the CPU count either: no
`available_parallelism`, no `num_cpus`, no `_SC_NPROCESSORS_ONLN`. The worker count
comes only from `--workers=N`, validated against `MAX_WORKERS` in `main.rs:84-91`.

`MAX_WORKERS = 64` (`crates/gnitz-wire/src/pk.rs:310`), so a 1:1 rank→core mapping is in
range for a 64-thread server and saturates at that point.

## Validated: the insertion point is unambiguous

A worker is a forked process running a single blocking loop
(`runtime/orchestration/worker/mod.rs:360-390`), and there is no `std::thread::spawn`
in non-test code anywhere in `gnitz-engine`. One process is therefore one runnable
entity and affinity is a single process-level call — no per-thread policy needed.

`run_worker_child` (`runtime/bootstrap.rs:734`) is the whole life of the child. It
already runs, in order: `prctl(PR_SET_PDEATHSIG)`, a `getppid` re-check, then

```rust
crate::foundation::worker_ctx::set_worker_identity(w as u32, num_workers);
crate::foundation::worker_ctx::set_worker_role();
```

at `bootstrap.rs:758-759` — the point where the rank `w` is latched and before any log
redirect, catalog work, store re-home, SAL replay or backfill. A pin belongs
immediately after that. The master could be pinned in `server_main` before
`acquire_shared_ipc` and the fork.

`libc` is already a direct dependency used pervasively (`foundation/posix_io.rs` is
nothing but raw libc calls), so `libc::sched_setaffinity` / `libc::cpu_set_t` add no
dependency.

## Validated: what the shared regions imply for NUMA

Both IPC regions are created by the master **before** the fork, in
`acquire_shared_ipc` (`runtime/bootstrap.rs:667-722`):

- **SAL** — a file-backed mmap of `{data_dir}/wal.sal`, `Backing::Reserved` (fallocate
  reserves the blocks; `bootstrap.rs:684`). The master is its sole writer; every worker
  reads it.
- **W2M rings** — one memfd per worker, `Backing::Sized` because *"a memfd's pages are
  RAM charged on first touch"* (`bootstrap.rs:697-699`), with `madvise_hugepage` applied
  (`bootstrap.rs:702`) and only the ring **header** initialized pre-fork by
  `w2m_ring::init_region` (`bootstrap.rs:709-711`). Each worker writes its own ring; the
  master reads all of them.

So page placement is decided by first touch, and first touch is split by role: SAL body
pages land on whichever node the master runs on, each W2M ring body on its own worker's
node. On a multi-socket machine that makes every SAL read cross-node for any worker not
co-located with the master, and every W2M read cross-node for the master. **A pinning
policy that ignores this can make things worse than the scheduler's default**, which at
least migrates a process toward the memory it is touching. This is the part of the work
that is not mechanical.

## Validated: measured on this machine — and why that says little

Test box: **AMD Ryzen 5 7640U** — a laptop CPU. 6 physical cores / 12 threads, **1
socket, 1 NUMA node**, `powersave` governor, 60 GiB RAM.

Load: release server, `--workers=4`, one table plus one `GROUP BY` view,
`INSERT … VALUES` of 100 rows per statement, 15 s per run, three interleaved reps.
Pinning applied with `taskset -cp`: master → cpu0, workers → cpu1..4.

| | migrations | master CPU/frame | rows/s |
|---|---|---|---|
| free | 146, 174, 172 /s | 737.6, 707.0, 731.3 µs | 57033, 57136, 57249 |
| pinned | 0.1, 0.0, 0.1 /s | 730.4, 717.2, 721.5 µs | 55854, 56330, 56176 |

Pinning worked — migrations went to zero — and bought nothing: master CPU per frame
−0.3% (inside noise), throughput 1.8% *worse* across all three reps.

**Four reasons this result does not transfer**, each verified rather than assumed:

1. **The mapping was wrong.** `/sys/devices/system/cpu/cpu0/topology/thread_siblings_list`
   and `cpu1`'s both read `0-1` — cpu0 and cpu1 are SMT siblings of one physical core.
   The naive index mapping therefore co-scheduled the master (the hottest process, ~42%
   of a core) and worker 0 on a single physical core. A topology-aware mapping would
   have used physical cores first. This alone could account for the −1.8%.
2. **One NUMA node.** There is no locality to win here; the entire NUMA half of the
   argument is untested.
3. **Nothing was saturated.** Master ~42% of a core, each worker ~6%. With 12 threads
   and 5 busy processes, the scheduler always had an idle core to move to, so a
   migration cost nearly nothing and the freedom to migrate was worth something.
4. **Wrong workload.** An ingest workload leaves the workers nearly idle with small
   per-tick batches. The cache footprint a migration would actually cost — operator
   traces, merge state — belongs to view maintenance, which was not exercised.

## Open — the questions the design must answer

1. **Which cores, and in what order?** Physical cores before SMT siblings, presumably;
   whether to use SMT siblings at all when `workers < physical cores` is untested.
2. **Where does the master go?** It was the hottest process in every measurement taken.
   Whether it gets a dedicated physical core, and whether that core should be node-local
   to the SAL, is unanswered.
3. **NUMA policy.** Given the first-touch split above: pin memory explicitly
   (`mbind`/`set_mempolicy`), interleave the SAL across nodes, keep all workers on the
   master's node until that node is full, or ignore it? No measurement exists.
4. **cgroup cpusets.** A container may be restricted to an arbitrary CPU subset, and
   `sched_setaffinity` to a CPU outside it fails with `EINVAL`. The allowed set must be
   read with `sched_getaffinity(0, …)` on self and pinning confined to it — never a
   hardcoded index and never `nproc`. Untested.
5. **Default and configuration surface.** Off by default, or auto when
   `workers <= allowed CPUs`? A CLI flag alongside `--workers`, or a `GNITZ_*` env knob?
   The numeric-knob precedent is `foundation/env.rs::env_num` (a zero or unparseable
   value falls back to the default, so it cannot zero a knob); the string precedent is
   `GNITZ_LOG_LEVEL` (`main.rs:97`).
6. **Failure handling.** Whether a failed pin is fatal, a warning, or silent. Every
   other best-effort hint in the codebase is silent-on-failure
   (`try_set_nocow`, `madvise_hugepage` — `foundation/posix_io.rs:125,150`), which is
   the local convention to follow unless there is a reason not to.

## Where to continue

- `runtime/bootstrap.rs:734-760` — `run_worker_child`, the insertion point; rank `w` is
  in scope and nothing has been allocated yet.
- `runtime/bootstrap.rs:667-722` — `acquire_shared_ipc`, where SAL and W2M placement is
  decided by first touch.
- `foundation/posix_io.rs` — where a `set_affinity` / `allowed_cpus` helper belongs, next
  to `try_set_nocow` and `madvise_hugepage`, which are the same shape (best-effort,
  errno-swallowing hints).
- `foundation/worker_ctx.rs` — rank and worker count, if a pin site outside the
  bootstrap ever needs them.

## Experiments that would settle it

Run these **on the target server**, not on a laptop:

1. Reproduce the A/B with a topology-aware mapping — physical cores first, master on its
   own physical core — and compare against both free scheduling *and* the naive
   index mapping, so the topology effect is separated from the pinning effect.
2. Use a worker-bound workload: `benchmarks/combined/test_view_maintenance.py` and the
   join/TPCH cases, not `micro/test_insert.py`. Workers must be near saturation for the
   question to be meaningful.
3. Drive `--workers` up to the core count and beyond, since oversubscription is the
   regime where pinning most plausibly hurts.
4. Report **CPU per frame and p99/p99.9 latency**, not throughput alone, and interleave
   the arms; wall-clock alone on a shared box is not decisive.
5. Collect `perf stat -e cpu-migrations,context-switches,cache-misses,LLC-load-misses`
   per process. Note that on this box `perf_event_paranoid` was `2` and per-process
   software events were refused — `/proc/<pid>/sched` (`se.nr_migrations`, `nr_switches`)
   and `/proc/<pid>/stat` (utime+stime) were used instead and are sufficient for
   migrations and CPU time.
6. On a multi-socket box, additionally compare workers spread across nodes against
   workers packed onto the master's node, since the SAL's pages follow the master.

The load driver used for the numbers above reports frames/s, per-process CPU-per-frame
and per-process migrations; its construction is described at the end of
`handovers/inline-compaction-tick-stall.md`.
