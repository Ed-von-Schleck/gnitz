# The two largest-scale runs execute with the Z-set invariant checks compiled out

Research handover. Everything under **Validated** was proved from source or measured
on this machine during the session; everything under **Open** was not.

## Goal

Make it possible to run the benchmark suite and the release E2E suite against an
optimized binary that still verifies the Z-set layout contracts, so that a silent
weight corruption at a data volume only reachable in a release build is caught rather
than absorbed. The measured cost is small and the mechanism needs no code change —
what is missing is a build profile, a target, and the decision to use them.

## Validated: what is compiled out, and what it guards

Non-test code in `gnitz-engine` contains **122 `debug_assert*` calls across 49 files**
against **29 `assert*` calls** that survive optimization (counted by walking every
`.rs` file, cutting at the first `#[cfg(test)]`, excluding `tests/`, `test_*` and
`bench*` files). There are additionally **33 `#[cfg(debug_assertions)]` sites**, 21 of
them in `storage/repr/batch.rs`.

The load-bearing ones are the layout certifiers. `Batch::certify_layout`
(`storage/repr/batch.rs:1162`) installs a `Sorted` / `Consolidated` claim and verifies
it only under `#[cfg(debug_assertions)]`:

```rust
pub(crate) fn certify_layout(&mut self, layout: Layout, schema: &SchemaDescriptor) {
    #[cfg(debug_assertions)]
    self.debug_verify_null_bits(schema);
    #[cfg(debug_assertions)]
    match layout {
        Layout::Raw => {}
        Layout::Sorted => self.debug_verify_sorted(schema),
        Layout::Consolidated => self.debug_verify_consolidated(schema),
    }
    self.layout = layout;
}
```

`sorted_verified` (`batch.rs:1133`) and `consolidated_verified` (`batch.rs:1147`) follow
the same shape, and their doc comments say they exist to be used *"at any skip-point
that trusts the claim to avoid a re-fold."* Consumers do exactly that:

- `ops/exchange/relay.rs:302-314` — `op_relay_scatter_consolidated_mode` debug-verifies
  each source *"before the merge-walk fast-paths on it"*
- `ops/linear.rs:138` — `if batch_a.sorted_verified(out_schema) && b.sorted_verified(…)`

CLAUDE.md names the failure mode this guards: an unsorted input to a merge produces
*"wrong weights silently — no error, no assertion."* In a release build there is no
assertion, by construction.

## Validated: which builds run with the checks off

From the `Makefile`:

- `test:` → `cargo test --workspace …` — debug, checks **on**
- `e2e:` → depends on `server` (`cargo build … ` → `crates/target/debug/gnitz-server`) — checks **on**
- `bench:` → depends on `release-server` + `pyext-release` — checks **off**
- `e2e-release:` / `release-test:` → `GNITZ_SERVER_BIN=../../gnitz-server-release` — checks **off**

So the functional suites are covered, and the only two runs that reach large data
volumes are the two that run blind.

## Validated: no code change is required

`-C debug-assertions=yes` enables both the `debug_assert!` macros and the
`cfg(debug_assertions)` blocks. A binary built with

```bash
cd crates && RUSTFLAGS="-C target-cpu=x86-64-v3 -C debug-assertions=yes -C overflow-checks=no" \
  CARGO_TARGET_DIR=target/dbgassert cargo build --release -p gnitz-engine --bin gnitz-server
```

builds and runs (verified: it served the load driver below for six 15-second runs).
The `target-cpu` restatement is required — `crates/.cargo/config.toml` and the Makefile
both document that Cargo's `RUSTFLAGS` env var *replaces* the config's rustflags rather
than appending. A `[profile.release-checked] inherits = "release"` entry in
`crates/Cargo.toml` avoids that trap entirely and keeps a separate build cache;
`[profile.release]` there already sets `panic = "abort"` and `lto = true`, which such a
profile would inherit.

## Validated: measured cost

Machine: 12 cores, 60 GiB RAM, btrfs (`compress=zstd:1`) over LUKS. Load average < 3.
Workload: release server, `--workers=4`, one table plus one `GROUP BY` view,
`INSERT … VALUES` of 100 rows per statement, 15 s per run, **three interleaved reps**
alternating the two binaries. Both binaries: `--release` (LTO, `panic=abort`),
`target-cpu=x86-64-v3`; the instrumented one adds `-C debug-assertions=yes` and
explicitly *not* `overflow-checks`.

| metric | plain release | + debug-assertions | delta |
|---|---|---|---|
| rows/s | 55151, 57113, 55324 | 55609, 55973, 54218 | −1.1% (inside noise) |
| master CPU per frame (µs) | 766.4, 713.2, 784.4 | 760.0, 768.2, 788.2 | +2.4% |
| **worker CPU per frame (µs)** | 102.7, 103.9, 101.2 | 115.1, 116.7, 119.3 | **+14.0%** |

The worker figure is consistent across all three reps with no overlap between the two
sets, so it is a real signal. End-to-end throughput does not move because on this
workload the workers sit at ~6% of a core and the master (~42% of a core) is the
constraint.

## Open — must be answered before adopting

1. **Does the E2E suite pass against such a build?** Never run. `make e2e-release` with
   `GNITZ_SERVER_BIN` pointed at the instrumented binary is the first experiment. An
   assertion that is too strict — or that only holds under the debug build's timing —
   would surface here.
2. **Cost on a worker-bound workload.** Only an ingest workload was measured, where the
   workers are nearly idle. The `combined/` benchmark tier (view maintenance, joins,
   TPCH) is where +14% worker CPU could become +14% end-to-end. Not measured.
3. **Whether `overflow-checks` should ride along.** Deliberately excluded from the
   measurement to isolate the assertions; its cost was not measured.
4. **Whether the benchmark suite should run the instrumented binary by default.** That
   would make every recorded number incomparable with the existing
   `benchmarks/results/` history; a separate target that is run on demand does not.

## Where to continue

- `crates/Cargo.toml:17` — the `[profile.release]` block a `release-checked` profile
  would inherit from.
- `crates/.cargo/config.toml` — the `RUSTFLAGS`-replacement trap, if the profile route
  is not taken.
- `Makefile` — `release-server`, `e2e-release`, `bench` are the three targets that would
  gain a checked sibling.
- `crates/gnitz-engine/src/storage/repr/batch.rs:1133-1270` — `sorted_verified`,
  `consolidated_verified`, `certify_layout`, `debug_verify_sorted`,
  `debug_verify_consolidated`; the O(n)-per-batch pass that dominates the measured cost.
