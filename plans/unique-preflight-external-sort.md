# Bound CREATE UNIQUE INDEX pre-flight memory with an external sort

A worker's `CREATE UNIQUE INDEX` pre-flight materialises **every** key span of its
whole partition in RAM before sorting. `handle_unique_preflight`
(`worker/mod.rs:1310-1372`) drains the owner table chunk-by-chunk, projects each
positive-weight row's OPK leading-key span, and pushes it into a single
`keys: Vec<PkBuf>` (`:1337-1360`), then `keys.sort_unstable()` (`:1361`) and
streams the sorted slice to the master (`send_unique_preflight_keys`, `:1363`). A
`PkBuf` is 81 bytes (`[u8;80]` + len; `master/mod.rs:141-142`), so a partition of
25 M rows holds ~2 GB of keys per worker — and a weight-`w` consolidated row emits
its span `w` times (`:1352-1357`), so churned tables cost more. `CREATE UNIQUE
INDEX` on a large table OOM-kills workers.

The peak is worker-local. The master side is already bounded: `merge_index_scan`
(`master/preflight.rs:160-183`) does a streaming k-way merge over the per-worker
sorted streams with `O(num_workers)` memory, and the `PreflightAccumulator` seed
collection is capped at `UNIQUE_FILTER_CAP = 1_000_000` distinct spans
(`preflight.rs:113-157`, `master/mod.rs:150`), clearing whole on overflow. So only
the worker's accumulate-then-sort needs bounding.

Fix: replace the unbounded in-RAM accumulate with an **external merge sort** —
sort and spill fixed-width run files past a byte budget, then k-way merge them,
streaming the globally-sorted spans to the master frame by frame. Peak RAM
becomes one budget plus the merge heap, independent of partition size. The master
still receives one globally-sorted stream per worker, so its merge and
duplicate-detection are unchanged.

Pre-alpha: no compatibility concern.

---

## Current mechanics (verified against source)

- **Worker accumulate-then-sort.** `handle_unique_preflight`
  (`worker/mod.rs:1310-1372`): `open_store_cursor(owner_id)` →
  `drain_chunk(chunk_rows)` loop; per row `spec.key_bytes(&mb, row, &mut keybuf)`
  projects the OPK span; `keys.push(keybuf)` (twice if weight > 1, `:1355-1357`);
  after the loop `keys.sort_unstable()` and `send_unique_preflight_keys(&keys,
  ..)`. `keys` holds the entire partition's spans — the only unbounded structure.
- **Span width is fixed.** Every span is exactly `pk_stride` of the synthetic
  frame schema `unique_preflight_wire_schema(&idx_schema, cols.len())`
  (`protocol/sal.rs:143`) — the sum of the promoted indexed columns' OPK widths.
  `IndexKeySpec::key_bytes` writes exactly that many bytes. So spans are
  fixed-width records; a spill file needs no per-record length prefix.
- **The send is already a frame train.** `send_unique_preflight_keys`
  (`worker/mod.rs:1660-1712`) walks `keys` in `keys_per_frame` slices, filling a
  reusable `chunk: Batch`, and emits each as a `FLAG_CONTINUATION` frame
  (terminal frame also `FLAG_SCAN_LAST`). It consumes only a `&[PkBuf]` slice —
  the only reason the full `Vec` must exist at call time.
- **Master expects one sorted stream per worker.** `merge_index_scan`
  (`preflight.rs:160-199`) primes a heap with each worker's minimum and pops equal
  spans adjacently; `PreflightAccumulator::offer` (`preflight.rs:139-150`) flags a
  duplicate when `prev == key`. This requires each worker's stream to be globally
  sorted so equal spans (within- and cross-partition) are adjacent. An external
  sort that emits the merged (globally sorted) run preserves this exactly.
- **The worker already writes to a per-table on-disk tree.** `flush_family_prepare`
  / `flush_chunk` (`worker/mod.rs:1470-1489`) write shard + manifest files under
  the owner table's partition directory during a flush, so the worker has a
  writable, per-owner directory to spill into.

## Design

Add a byte budget `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES` (default 128 MiB of key
bytes ≈ 1.6 M single-column i64 spans). Rewrite the collection half of
`handle_unique_preflight`:

1. **Accumulate + spill.** Push projected spans into an in-RAM `Vec<PkBuf>`,
   tracking `bytes = len * pk_stride`. When `bytes >= budget`, `sort_unstable`
   the Vec and append it to a new spill run — a fixed-`pk_stride`-stride file
   under the owner's partition dir — then clear the Vec. Continue draining.
2. **Finish.**
   - **Zero spills** (partition fit the budget): the in-RAM Vec is the whole set
     — `sort_unstable` and hand it to the existing slice sink. No disk, identical
     to today's fast path.
   - **≥1 spill:** sort+spill the final partial Vec, then **k-way merge** the run
     files: a `BinaryHeap<Reverse<(PkBuf, run_idx)>>` seeded with each run's first
     span; pop the min, feed it to the streaming sink, refill from `run_idx`. Byte-
     lexicographic order via `PkBuf: Ord` — the same order `sort_unstable` and the
     master's heap use, so equal spans stay adjacent across runs (duplicate
     detection intact; weight-`w` spans, spilled `w` times, merge adjacently).
3. **Streaming sink.** Generalise `send_unique_preflight_keys` to pull from a
   producer instead of indexing a slice: keep the `keys_per_frame` chunk-fill /
   encode / `FLAG_SCAN_LAST`-on-exhaustion logic verbatim, but source spans from
   `next: impl FnMut() -> Option<PkBuf>` (the fast path passes a slice iterator;
   the merge path passes the heap drain). Terminal-frame detection uses a
   one-span lookahead (peek) so the last frame still carries `FLAG_SCAN_LAST`, and
   an empty producer still emits one empty terminal frame (unchanged contract).
4. **Cleanup.** Wrap the spill files in an RAII guard that `unlink`s them on drop,
   so success, an early `?`, and a panic all remove every run file. Spill I/O
   errors surface as the existing `Err(String)` return (the master already drains
   the fan-out and leaves catalog + filter state untouched on a pre-flight fault,
   per `handle_unique_preflight`'s doc and the `GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR`
   seam, `:1314-1317`).

Peak worker RAM = one budget of spans + the merge heap (`O(num_runs)` spans) +
one output frame — bounded regardless of partition size. `open_store_cursor` +
`drain_chunk` already stream the table, so the row scan was never the problem;
only the accumulation was.

## Correctness

- **Same bytes to the master, same order.** The merge emits every span the old
  path did (including weight-`w` duplicates), in the same global byte order, as
  one sorted stream. `merge_index_scan` and `PreflightAccumulator` are untouched
  and behave identically — the CREATE-time duplicate verdict and the filter seed
  are byte-for-byte the same.
- **Fast path unchanged.** Below the budget nothing spills; the code path is
  today's `sort_unstable` + slice send, so small/medium indexes pay nothing.
- **Fault isolation preserved.** A spill/merge error returns `Err`, which the
  master handles exactly as today's pre-flight faults (fan-out drained, no catalog
  or unique-filter mutation). RAII unlink prevents leaked run files on every exit.
- **NULL / weight rules unchanged.** Projection still skips `key_bytes → false`
  (NULL in any indexed column) and non-positive weights; only *where* sorted spans
  live changes.

## Tests

- **Large-partition pre-flight stays bounded and correct (E2E, `make e2e`,
  `GNITZ_WORKERS=4`).** Tiny `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES` (e.g. 64 KiB) so
  a few thousand rows force multiple spills. `CREATE UNIQUE INDEX` on a table with
  all-distinct values succeeds; a duplicate value present in the data is rejected;
  after creation the index enforces uniqueness on a fresh INSERT. Run once with a
  duplicate straddling two spill runs (values chosen so equal spans land in
  different runs) to prove cross-run adjacency detection.
- **Spill fast-path parity (E2E).** With the default budget, a small unique index
  behaves exactly as before (no spill files created — assert the owner dir has
  none post-creation).
- **External-sort unit test (Rust).** Feed a known multi-run span set through the
  spill+merge and assert the merged stream equals `sort_unstable` of the input
  (order and multiplicity), and that run files are unlinked on both the ok and
  error paths.

## Sequencing

One commit; the tree is green (`make verify` + `make e2e`) after it.

- [ ] 1. **External-sort the pre-flight.** Add the spill budget; rewrite
  `handle_unique_preflight`'s collection to accumulate → sort+spill fixed-stride
  runs → k-way merge; generalise `send_unique_preflight_keys` to a producer sink
  with one-span lookahead; RAII-unlink the run files. Add the large-partition,
  fast-path-parity, and external-sort unit tests.
