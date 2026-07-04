# Collision-free, reboot-stable compaction output naming

Vertical (routed) compaction output filenames can collide across independent
compaction calls, and `write_shard_streaming`'s finalizing `renameat` overwrites
the colliding live shard with no check — **silent, cross-restart, unrecoverable
data loss on durable base tables** under sustained write volume. The horizontal
path carries the same latent hazard across a reboot (its per-call tag resets to
0 each boot). Both are one root cause: compaction output names are built from
components that are not unique across a table's lifetime. Fix: name every
compaction output by a **persisted, strictly-monotonic per-table compaction
sequence** plus the **destination guard key**, so no two outputs ever share a
basename — in any call sequence, across any restart.

Pre-alpha: the manifest format changes (no compatibility concern), and no legacy
naming remains.

---

## Root cause (verified against source)

Routed compaction (`compact_routed` → `merge_and_route`,
`storage/lsm/compact/merge.rs`) names each output through a closure that
interpolates the **0-based destination-guard loop index** and a shared LSN tag:

```rust
// storage/lsm/compact/merge.rs — the vertical / L0→L1 output name (current)
move |g| format!("{dir}/shard_{table_id}_{lsn_tag}_L{level_num}_G{g}.db")
// compact_routed: `for g in 0..guard_keys.len() { let path = name_for(g); ... }`
```

Two components break lifetime-uniqueness:

- **`g` is the loop index, reset to 0 every call** — not the guard key. Distinct
  compaction calls both start at `g = 0`.
- **`lsn_tag = vert_max_lsn`** (`shard_index/index.rs`, `compact_guard_vertical`)
  is a `max` over the source and destination guards' entry `max_lsn`. It is **not
  unique per call**. The load-bearing fact: `commit_l0_to_l1` (`index.rs`) stamps
  **every** per-guard L1 output of one `run_compact` with the **same** shared
  `l0_max_lsn = M` (one `l0_max_lsn` computed per `run_compact`, applied to all
  destination guards). So two well-separated guards can both legitimately top out
  at `max_lsn = M`.

Consequently, two later `compact_guard_vertical` calls that select **different**
worst guards whose merged ranges both top at `max_lsn = M`, and whose
**destination guards are disjoint** (so neither call consumes/supersedes the
other's output), each emit their single output at `g = 0` with `lsn_tag = M` —
producing the identical basename `shard_{tid}_{M}_L{level}_G0.db`.

`write_shard_streaming` finalizes with `renameat(tmp → basename)`
(`storage/lsm/shard_file.rs`) and performs **no live-file check**, so the second
call's rename atomically replaces the first call's live shard on disk. Within the
running session the loss is masked — the first `ShardEntry`'s mmap still holds
the pre-rename inode (`rename(2)` does not invalidate mappings) — and surfaces on
the next reopen, when `load_manifest` re-opens the shared filename: the manifest
references one basename for two distinct guards, so one guard's entire key range
is silently lost.

The **horizontal** path (`compact_one_guard`) is immune to the cross-guard
variant because it names with the **stable `guard_key`** plus the per-call
`compact_seq`:

```
hcomp_{tid}_L{level}_G{guard_key}_{compact_seq}.db
```

but it carries the **reboot** variant of the same root cause: `compact_seq`
starts at 0 each boot and is not restored by `load_manifest`, so after a restart
the first re-compaction of a guard reuses a `compact_seq` value already baked
into a live, manifest-referenced `hcomp` file, colliding with it.

## Blast radius / severity

- **Who:** durable tables (base and durable/system) that accumulate enough
  on-disk data to trigger **vertical compaction** — L1→L2 fires when an L1's file
  count exceeds `L1_TARGET_FILES = 16`, i.e. a table past many flush + L0→L1
  cycles. Also L2→L3 (`compact_guard_vertical(2)`). L0→L1-only tables avoid the
  cross-guard variant (their `l0_max_lsn` tag is already strictly increasing) but
  are still exposed to the horizontal reboot variant. Ephemeral tables are
  unaffected — they wipe all shard files at open (`erase_stale_shards`) and never
  reload a manifest.
- **When it manifests:** the on-disk clobber happens the instant the second
  `renameat` runs, but is masked within the session; it becomes real, silent data
  loss on the **next restart**, when `load_manifest` re-opens the shared
  filename. The disjoint-guard variant loses one guard's key range; a
  same-destination-guard variant (where `pending_deletions` then unlinks the live
  merged file) makes `ShardEntry::open` fail at load — the **whole table fails to
  open**.
- **Severity:** high — silent, cross-restart, unrecoverable.

---

## Design

Name **every** compaction output — horizontal and vertical — by
`{table_id}_{compact_seq}_L{level_num}_G{guard_key}`, where:

- **`compact_seq`** is a per-table monotonic counter, **persisted in the
  manifest** and reloaded at boot, incremented before every compaction call
  (already the case: `self.compact_seq += 1` runs at the top of `run_compact`,
  `compact_one_guard`, and `compact_guard_vertical`). Never reset, never reused
  across the table's lifetime.
- **`guard_key`** is the *destination* guard's stable key (`guard_keys[g]`), not
  the positional loop index.

This unifies the routed path onto the horizontal path's proven scheme (both keyed
by `guard_key` + `compact_seq`) and rests on one trivially-checkable invariant
(`compact_seq` is globally unique per call), rather than on subtle,
path-specific LSN-assignment reasoning. An LSN-derived tag (`vert_max_lsn` /
`guard_max_lsn`) is explicitly rejected: it is only safe on the horizontal path
(per-guard `max_lsn` strictly increases per re-compaction) and is exactly what
makes the vertical path collidable, because `commit_l0_to_l1` shares one
`l0_max_lsn` across guards.

### `storage/lsm/compact/merge.rs`

`compact_routed` passes the destination guard key to the name closure instead of
the index:

```rust
// signature: name_for takes the guard key, not the loop index
mut name_for: impl FnMut(u128) -> String,
// in the loop body (was `name_for(g)`):
let path = name_for(guard_keys[g]);
```

`compact_shards`' `|_| path.clone()` closure already ignores its argument (now
typed `u128`), unchanged.

`merge_and_route` replaces its `lsn_tag: u64` parameter with `compact_seq: u64`
and rewrites the closure:

```rust
move |gk| format!("{dir}/shard_{table_id}_{compact_seq}_L{level_num}_G{gk}.db"),
```

### `storage/lsm/shard_index/index.rs`

- `run_compact`: drop the `lsn_tag` local; keep `l0_max_lsn` (still passed to
  `commit_l0_to_l1`). Pass `self.compact_seq` to `merge_and_route` in the former
  `lsn_tag` position (`self.compact_seq += 1` already ran at the top).
- `compact_guard_vertical`: drop the `lsn_tag` block; keep `vert_max_lsn` (still
  the `ShardEntry::open` `max_lsn`). Pass `self.compact_seq` to `merge_and_route`
  in the former `lsn_tag` position (`self.compact_seq += 1` already ran at the
  top).

`compact_one_guard` (horizontal) already names by `guard_key` + `compact_seq`;
no name change — persisting `compact_seq` (below) is what closes its reboot
variant.

### `storage/lsm/manifest.rs` — persist `compact_seq`

The header's currently-reserved, zeroed region carries the new field; no version
bump (pre-alpha, no legacy manifests; a manifest lacking it reads 0):

- `const OFF_COMPACT_SEQ: usize = 32;` (occupies `[32,40)`; `[40,64)` stays
  reserved). Update the header doc-comment.
- `serialize(...)`: after writing `global_max_lsn`, add
  `write_u64_le(out_buf, OFF_COMPACT_SEQ, compact_seq);`.
- `parse(...)`: alongside the `global_max_lsn` read, add
  `*out_compact_seq = read_u64_le(buf, OFF_COMPACT_SEQ);`.
- Thread `compact_seq` (in) / `out_compact_seq` (out) through `prepare_file` /
  `write_file` / `read_file`, mirroring `global_max_lsn` exactly. `entry_count`
  unchanged. `ENTRY_SIZE_*` static asserts unaffected (header-only change).

### `storage/lsm/shard_index/persist.rs`

- `load_manifest`: read `compact_seq` via `read_file` and set
  `self.compact_seq = compact_seq;` **unconditionally** (before/after the entry
  loop — it must load even when this table contributes no entries to a shared-dir
  manifest).
- `publish_manifest`: `manifest::write_file(&cpath, &entries, global_lsn,
  self.compact_seq)`.
- `prepare_manifest_with_pending`: `manifest::prepare_file(manifest_path,
  &entries, global_lsn, self.compact_seq)`.

Both durable write paths (`compact_if_needed → publish_manifest`; the flush path
`prepare_manifest_with_pending`) now persist the counter. `Table::new` calls
`load_manifest` then `gc_orphans` at boot before any compaction runs, so the
counter is restored first.

### Test call-site updates

In-crate `manifest.rs` test call sites take a mechanical `, 0`
(`serialize`/`write_file`/`prepare_file`) or `&mut 0u64` (`parse`/`read_file`);
`roundtrip` / `write_read_file_roundtrip` gain an assertion that the counter
survives. Existing tests that hard-code the old routed name are updated:

- `compact/mod.rs::test_merge_and_route_multi_guard_matches_row_at_a_time`:
  reconstructed `shard_7_42_L1_G{g}.db` → `shard_7_42_L1_G{guard_keys[g]}.db`
  (the former `lsn_tag` `42` is now `compact_seq`; the file suffix is the guard
  key). Assert against the paths already returned in the `routed`
  `Vec<(u128, String)>`; use `guard_keys[g]` only for the empty-guard existence
  check.
- `shard_index/index.rs::test_compact_guard_vertical_failure_leaves_index_unchanged`:
  blocker `shard_42_100_L2_G0.db` → `shard_42_1_L2_G0.db` (that call's
  `compact_seq` becomes 1; fallback dest guard key 0).

---

## Collision-impossibility argument

Every compaction output basename is `shard_{tid}_{seq}_L{level}_G{gk}.db` (routed)
or `hcomp_{tid}_L{level}_G{gk}_{seq}.db` (horizontal), with `seq =
self.compact_seq` at the emitting call and `gk` a guard key.

- **Lemma 1 — `seq` is globally unique per call, across restarts.**
  `compact_seq` is incremented before every compaction call and is persisted in
  the manifest and reloaded at boot, so it is strictly monotonic over the table's
  entire lifetime; two distinct calls never share a value, even across a restart.
- **Lemma 2 — within one call, `gk` is unique.** Guards within a level have
  unique keys (`get_or_create_guard` dedups on `guard_key`); the routed
  `guard_keys` are exactly those distinct destination keys (or a single fallback
  key). Outputs of one call share `seq` but carry distinct `gk`.

Any two outputs therefore differ in `seq` (different calls) or in `gk` (same
call), so all basenames are pairwise distinct across the table's lifetime.
`renameat` can never target a path naming any other shard — live, orphan, or a
consumed input. This covers:

- **Cross-guard, shared `M`, disjoint dest** (the confirmed data-loss bug): the
  two outputs get distinct `gk` and distinct `seq` → distinct names. No clobber.
- **Same-destination-guard re-compaction** (repeated LSN): the two calls have
  distinct `seq` → distinct names; the first output (now a consumed input) has a
  different name from the second, so `renameat` doesn't overwrite it and
  `pending_deletions` deletes only the genuinely-superseded first file.
- **L0→L1 adds-to-guard:** a new L0→L1 output can share a guard with a live,
  non-input file, but its `seq` is strictly greater than any prior output's, so
  its name is new.
- **Crash before publish:** a crashed compaction's `seq` bump is not persisted,
  so post-restart `seq` re-climbs and may reissue those values — but the crashed
  outputs were never manifested, so `gc_orphans` deletes them at boot **before**
  any new compaction runs; a reissued `seq` only ever names a file that no longer
  exists. ∎

## Restart / manifest-parse safety

- **No filename is parsed for data.** Every consumer matches only the
  `shard_{tid}_` / `hcomp_{tid}_` / `eph_shard_{tid}_` **prefix**
  (`persist.rs::gc_orphans`, `table/mod.rs::erase_stale_shards`, the tree-scan
  helpers). `load_manifest` opens the filename verbatim and reads
  `min_lsn`/`max_lsn`/`level`/`guard_key` from **dedicated entry fields**, never
  from the name. The new names keep the `shard_{tid}_` prefix, so orphan-GC and
  stale-erase are unaffected.
- **No format ambiguity.** The flush name `shard_{tid}_{lsn}.db` has no `_L`;
  routed names always contain `_L{level}_G{key}` — structurally distinct, as
  today. hcomp names are untouched. Distinct vertical dest levels never share the
  L0→L1 dest level; within a level, `seq` + `gk` disambiguate.

---

## Tests

Deterministic unit tests in `shard_index/index.rs`, mirroring the existing
vertical tests:

- **Two disjoint guards, shared `M`, no clobber** (the confirmed bug).
  `ensure_level(2)`. L1: guard `100` = 2 entries (keys 100, 110, `max_lsn=100`);
  guard `5000` = 2 entries (keys 5000, 5010, `max_lsn=100`). L2: pre-seed guard
  `100` (key 250, `max_lsn=100`) and guard `5000` (key 6000, `max_lsn=100`) so the
  two source ranges route to disjoint dest guards. Call `compact_guard_vertical(1)`
  twice (worst guard 100, then 5000; all merged ranges top at `max_lsn=100`).
  Assert the two resulting L2 entry filenames are **distinct** and both files
  exist; then `publish_manifest` → `load_manifest` into a fresh `ShardIndex` →
  assert all keys {100,110,5000,5010,250,6000} are findable. Pre-fix: both emit
  `shard_42_100_L2_G0.db`, the second clobbers the first, reload loses guard 100.
- **Same-destination-guard re-compaction + `try_cleanup`** (the deferred
  variant). `ensure_level(2)`. L2: guard `100` (key 250, `max_lsn=100`). L1:
  guard `100` = 2 entries (keys 100, 110, `max_lsn=100`). Call
  `compact_guard_vertical(1)` (consumes L2 guard 100, writes merged `G100`);
  re-add L1 guard `100` with 2 more entries (keys 120, 130); call
  `compact_guard_vertical(1)` again; then `try_cleanup()`. Assert the live L2
  guard-100 entry's file exists after `try_cleanup`; `publish_manifest` →
  `load_manifest` → all keys {100,110,120,130,250} findable. Pre-fix: call 2's
  output path equals call 1's file; `pending_deletions` deletes the live file →
  reload fails.
- **Manifest round-trip** asserts `compact_seq` reloads to its persisted value.

E2E (`make e2e`, `GNITZ_WORKERS=4`): a sustained-ingest test on a durable base
table large enough to drive L1→L2 vertical compaction across multiple guards,
then SIGKILL-restart and full scan — assert the table opens and every row is
present (would fail pre-fix once a cross-guard collision has clobbered a live
shard).

---

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`) after
each.

- [ ] 1. **Persist `compact_seq` in the manifest.** `OFF_COMPACT_SEQ` header
  field; `serialize`/`parse`/`prepare_file`/`write_file`/`read_file` thread it;
  `load_manifest` restores `self.compact_seq`; `publish_manifest` /
  `prepare_manifest_with_pending` write it. Mechanical test call-site updates +
  round-trip assertion. Closes the horizontal reboot-collision on its own (no
  name change: persisted `compact_seq` never reuses a live `hcomp` value).
- [ ] 2. **Name routed compaction outputs by `compact_seq` + destination guard
  key.** `compact_routed` passes `guard_keys[g]`; `merge_and_route` takes
  `compact_seq` and interpolates it with the guard key; `run_compact` /
  `compact_guard_vertical` drop `lsn_tag` and pass `self.compact_seq`. Update the
  two hard-coded-name tests. Add the two deterministic collision tests and the
  E2E vertical-compaction restart test.
