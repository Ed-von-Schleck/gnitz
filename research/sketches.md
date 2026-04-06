# Sketch Algorithms in Gnitz: Opportunities Beyond Bloom/XOR8

---

## Current State

Gnitz uses two sketch structures, both for the **same purpose**: fast-fail membership testing before an expensive binary search.

| Structure | File | Where used | Purpose |
|---|---|---|---|
| `BloomFilter` | `storage/bloom.py` | `MemTable` | Skip binary search over sorted runs for PKs not in the table |
| `Xor8Filter` | `storage/xor8.py` | `ShardHandle` | Skip mmap binary search for PKs definitely not in a shard |

Both answer only one question: "does this **primary key** exist?" Both are insertion-only. The Bloom filter is mutable (rows accumulate over the MemTable's lifetime); the XOR8 is static (built once at shard flush, persisted to a `.xor8` sidecar). Neither tracks any information about non-PK column values, frequencies, or distributions.

The rest of gnitz has zero probabilistic infrastructure: no cardinality estimates, no frequency tracking, no quantile information, no approximate aggregates.

---

## The Z-Set Constraint

Before identifying opportunities, the central constraint must be stated clearly: **gnitz's DBSP model allows negative weights (retractions)**. A row with weight -1 cancels a prior insertion. Most classical sketch algorithms are insertion-only and produce undefined or inaccurate results under deletions.

This constraint partitions sketch use cases into three categories:

**A. Write-once contexts — any sketch works.** Shard files are flushed once and never modified. Sketches computed at flush time are structurally sound with no deletion concern.

**B. Delta-batch statistics — signed counter variants work.** Each tick's delta batch is a complete, self-contained batch where weights can be ±. Count Sketch (with 4-universal hashing and signed counters) handles this correctly, unlike classic Count-Min Sketch.

**C. Aggregate query results for mutable tables — hard.** `COUNT(DISTINCT col)` or `APPROX_PERCENTILE` in an incremental view over mutable tables requires either deletion-friendly sketches (KLL±, which is 2024/2025 research-level) or restriction to append-only tables (detectable via the append-only optimization in `optimizations.md` §Tier 2 item #11).

---

## Opportunity 1: ExaLogLog Cardinality Sketches in Shard Metadata

**Value: High. Category A (write-once). No Z-Set constraint issues.**

### The Gap

Gnitz currently stores per-shard `(pk_min, pk_max)` bounds in `ShardHandle`. The shard flush path in `shard_table.py` / `writer_table.py` writes all column data but records zero statistics about column value distributions. This is the root reason gnitz has no join cardinality estimates, no filter selectivity estimates, and cannot implement join ordering heuristics (Tier 2 item #15 in `optimizations.md`).

### Algorithm Selection: ExaLogLog Over HLL/ULL

The cardinality estimation space has moved rapidly. HyperLogLog's known successors now include:

| Algorithm | Space vs. HLL | Notes |
|---|---|---|
| HyperLogLog | baseline | Industry standard until recently |
| UltraLogLog (ULL) | −28% | Same register structure, improved estimator |
| **ExaLogLog (EDBT March 2025)** | **−43%** | Same practical guarantees; designed for exa-scale; by the same author as ULL |
| Huffman-Bucket Sketch (preprint March 2026) | ~optimal O(m) | Uses Huffman coding on buckets; too new to implement |

**ExaLogLog is the correct choice for gnitz today.** It provides the same core properties as HLL (idempotent, mergeable via per-bucket max, constant-time insert) with 43% less space — better than ULL's 28%. At M = 1024 registers HLL requires ~1280 bytes per column per shard; ExaLogLog achieves the same ~1% relative error in ~730 bytes. For a table with 20 non-PK columns and 100 shards, that is 2.3 MB vs. 1.3 MB for the total statistics sidecar footprint — not enormous, but ExaLogLog costs nothing extra to implement over HLL and the algorithm is now formally published with a reference implementation.

The Huffman-Bucket preprint (March 2026, days old at time of writing) achieves theoretically optimal O(m) bounds using Huffman coding on the bucket registers. It is not yet production-ready and should be monitored but not implemented now.

### What ExaLogLog Enables

- **Join column cardinality**: given column `c` in table `T`, `n_distinct(c)` is the union of ExaLogLog sketches across all shards for `T`. Merge by taking per-bucket maxima — identical to HLL union. This is the single most valuable input for join cardinality estimates.
- **Filter selectivity**: combined with the existing min/max bounds, `n_distinct` bounds selectivity under a uniform assumption. `n_distinct(c) / row_count` ≈ probability that a random row matches a specific value.
- **Join side selection**: prefer integrating the trace for the table with fewer distinct join-key values — smaller trace, less state.
- **Zone map effectiveness prediction**: as `optimizations.md` §24 notes, zone maps are only useful when column values are correlated with PK ordering. Per-shard ExaLogLog sketches reveal this: if `shard_n_distinct / global_n_distinct` is small, zone map pruning is effective for that column.

### Implementation Shape

ExaLogLog is structurally identical to HLL: a `Vec<u8>` of M bytes (registers), hashed with XXHash. The difference is in the register update rule and final estimator formula, not in the data structure layout. The core loop:

```
add(key):
    h = xxh.hash_u128_inline(pk_lo, pk_hi, seed_lo, seed_hi)
    bucket = h >> (64 - LOG2_M)
    # ExaLogLog uses a different register value encoding for space efficiency
    # (details in Ertl 2025, EDBT); insert complexity remains O(1)

merge(other):
    for i in range(M):
        registers[i] = max(registers[i], other.registers[i])  # identical to HLL

estimate():
    # ExaLogLog estimator (Ertl 2025) — replaces HLL's harmonic mean formula
```

Persistence: write as a `.exll` sidecar per shard (same pattern as `.xor8`). Load in `ShardHandle.__init__` alongside `load_xor8`. Aggregation across shards in `ShardIndex`: per-bucket max across all handles, O(K × M).

The full implementation is comparable in complexity to `xor8.py`.

---

## Opportunity 2: Count Sketch + TP-Sketch for Delta-Batch Frequency Statistics

**Value: Medium-High. Category B (delta processing). Signed-counter variant handles Z-Set weights.**

### The Gap

`optimizations.md` Phase B proposes tracking "delta size" as a rolling mean. This is batch-level (how many rows?), not key-level (which keys appear most often?). For two critical optimizations — detecting GROUP BY skew before the exchange, and predicting which groups will dominate `op_reduce` output — per-key frequency information is needed, not just batch size.

### Count Sketch vs. Count-Min Sketch

The external research report recommends **Count-Min Sketch** for frequency estimation. For gnitz specifically, **Count Sketch** (Charikar, Chen & Farach-Colton 2002) is the correct variant:

- Count-Min uses `+1` increments only; under-estimates are bounded but negative updates break its guarantees
- **Count Sketch** uses signed `±1` increments (or ±weight) with 4-universal hashing; median estimator across `d` rows gives unbiased estimates with both positive and negative updates
- Z-Set delta batches carry signed weights; Count Sketch handles these correctly by design

### Pre-Hashed Insertion: A Gnitz-Specific Speedup

AWS DaMoN 2025 published a Count Sketch implementation achieving **12.3× speedup** over Apache DataSketches by exploiting **pre-hashed insertions**: when a database system has already computed a hash value for a key (e.g., for a hash join), that hash can be fed directly into the sketch's update path, eliminating redundant hashing.

This matters directly for gnitz:
- `op_exchange_shard` already calls `hash_u128_inline(pk_lo, pk_hi)` to route rows to workers
- `op_reduce` already calls `_extract_group_key` which includes Murmur3 hashing of the group-by columns
- The XOR8 lookup in `ShardIndex.find_all_shards_and_indices` already has a per-key hash from `_hash_key`

In each of these hot paths, the group key hash is already in hand. A Count Sketch update in the same loop costs only the counter array accesses — no additional hash computation. The 12.3× AWS speedup is partially attributable to avoiding re-hashing; gnitz gets this for free by design.

### Heavy Hitters vs. Persistent Items: TP-Sketch

Classic Count Sketch finds **heavy hitters** — keys with the highest frequency within a single delta batch. But for gnitz's GROUP BY skew problem, the operationally relevant question is different: which group keys appear in many *consecutive* ticks, not which group key dominates a single tick?

A group key that appears in 30% of one tick's delta batch but never again is not a skew problem. A group key that appears in every tick at 2% each — accumulating 100% participation over 50 ticks — is exactly the key that will cause consistent reduce state pressure and should be pre-aggregated.

**TP-Sketch (February 2026)** addresses this directly. The "Threshold-based Persistent Sketch" classifies items by *persistence* across non-overlapping time windows rather than absolute frequency within one window. It uses a dynamic global threshold to protect genuinely persistent items from cache eviction, outperforming prior approaches (P-Sketch, Pandora 2024) significantly on persistence metrics while using less memory than sliding-window approaches.

In gnitz's model, each DBSP tick is a natural time window. A TP-Sketch over group keys maintained across ticks gives a "persistence score" per group key — the number of consecutive ticks in which this key appeared in any delta batch. High-persistence keys are:
- Candidates for hierarchical aggregation (their group state is large and stable)
- Candidates for pre-aggregation before the exchange (they will predictably appear in every worker's delta)
- Indicators that a view's GROUP BY column has a hot partition

TP-Sketch can run in parallel with Count Sketch or replace it. The two answer different questions: Count Sketch answers "what was the heaviest key this tick?" while TP-Sketch answers "which keys have been present for many consecutive ticks?" For gnitz's adaptive optimization goals, TP-Sketch's question is more actionable.

### What the Combined System Enables

**Short-term (Count Sketch):**
- GROUP BY skew detection within a single tick — identifies keys dominating one delta batch
- Adaptive exchange sub-batch sizing — pre-sized allocations eliminate reallocation in `repartition_batch`
- Input to Phase B EMA delta tracking — augments batch-size rolling mean with per-key dimension

**Medium-term (TP-Sketch):**
- Cross-tick persistence tracking — identifies chronically hot group keys
- Hierarchical aggregation trigger — when a group key's persistence score exceeds threshold, the executor can flag the view for hierarchical aggregation restructuring
- Tick-rate adaptation input — if no keys are persistent (all heavy hitters are random per tick), uniform routing is near-optimal and pre-aggregation adds overhead rather than saving it

### Implementation Shape

**Count Sketch**: a `d × w` matrix of signed integers:
- `d = 3`, `w = 1024`: ~3% relative error for items with frequency > 0.3% of total, 12KB
- Update in `op_reduce` hot path: reuse the group key hash from `_extract_group_key` (pre-hashed insertion optimization — no extra hashing)
- Query: median of `s_i(key) × matrix[i][h_i(key)]` over `d` rows
- Simpler than `xor8.py` — flat array, no retry loop

**TP-Sketch**: a compact hash table of `(key_hash → persistence_counter)` pairs with eviction policy:
- Entries survive eviction only if their counter exceeds a global dynamic threshold
- Update: at the end of each tick, increment counters for all keys that appeared; decay counters for all keys that did not
- Footprint: O(K) where K is the number of actively tracked persistent items — typically small

**Placement**: a per-view `DeltaStatistics` object maintained by the executor, updated from the delta batch after each tick. Not stored to disk (ephemeral, rolling window).

---

## Opportunity 3: HLL as Approximate COUNT(DISTINCT) Aggregate

**Value: Medium. Category A for append-only, Category C (hard) for mutable tables.**

### The Gap

Gnitz currently has no `COUNT(DISTINCT col)` aggregate. The DBSP framework handles exact distinct counting through the `op_distinct` operator (full Z-Set state) combined with `op_reduce` + COUNT. This is correct but requires O(|distinct values|) state: the complete set of distinct values must be maintained in an `EphemeralTable`.

HLL achieves the same result approximately with O(M) = O(1280 bytes) state per group, independent of the number of distinct values.

### The Z-Set Problem

For mutable base tables, a row retraction (weight -1) should decrement the distinct count if and only if no other row with the same value remains. HLL has no deletion operation — once a bucket register is updated, it cannot be reduced. This is fundamental, not an implementation detail.

Three tractable approaches:

1. **Restrict to append-only tables.** If the source table has never received a DELETE or weight-negating batch, HLL is exact. Gnitz can detect this (optimizations.md §11, "append-only source detection"). An HLL aggregate on an append-only table is correct.

2. **Sliding-window HLL.** If the view's source has bounded retention (e.g., a time-windowed view over recent events), maintain two HLL sketches (current window, previous window) and swap at window boundaries. This is the "SWAMP" pattern from streaming systems. Not applicable to general views but useful for time-series analytics views.

3. **Sketch-of-sketches for group-level approximate distinct.** For `SELECT group_key, COUNT(DISTINCT val_col) FROM t GROUP BY group_key` over mutable data: maintain one HLL sketch per active group key in the reduce operator's trace, snapshot the estimate at tick boundaries. Retractions only matter when a value is deleted AND that was the last occurrence. For high-cardinality columns with rare deletions, the error from missed decrements is negligible. This is an explicit accuracy-vs-memory trade-off, opt-in only.

### What It Enables

- `APPROX_COUNT_DISTINCT(col)` aggregate — same user-facing SQL as BigQuery/Snowflake
- Memory reduction from O(|distinct values|) to O(M) per group
- Serializable sketch state: HLL sketches can be stored in the reduce operator's trace (replacing the full exemplar rows), enabling massive state compression for high-cardinality distinct-count views

### Implementation Shape

`HllAggregate` as a new `AggregateFunction` variant. The `step` method runs the HLL `add` algorithm; `get_value_bits` returns the estimate packed as a `u64`. The challenge: `AggregateFunction` currently stores a scalar accumulator (`acc: i64`). HLL requires an array of registers. This requires either (a) embedding a `Vec<u8>` in the aggregate object, or (b) encoding a compact HLL representation in a fixed-width integer (feasible for HLL with ≤64 registers, unusably small for good accuracy).

The practical approach: store HLL register arrays as `Vec<u8>` in the aggregate, following the pattern of `BloomFilter`. The aggregate is reset by zeroing the register array.

---

## Opportunity 4: KLL Sketch for APPROX_PERCENTILE

**Value: Medium. Category A for append-only. Category C (research-level) for mutable tables.**

### The Gap

Gnitz supports MIN and MAX but no `PERCENTILE_CONT`, `PERCENTILE_DISC`, `APPROX_MEDIAN`, or any quantile aggregate. These require O(N log N) sorting of all values in a group if implemented exactly. KLL computes approximate quantiles with O(ε⁻² log²(1/δ)) space for ε-accuracy with probability 1-δ.

### The Z-Set Problem (Stronger Than for HLL)

HLL's deletion problem is bounded error; KLL's is potentially unbounded. If a value is deleted from the underlying table, it may have been promoted to a high-level compactor in the KLL structure and cannot be removed. The error introduced is proportional to the fraction of the total weight that deletions represent. For append-heavy workloads with rare deletions, this is acceptable. For workloads with symmetric inserts/deletes, it is not.

KLL± (VLDB 2024/2025) extends KLL to a "turnstile" model supporting both insertions and bounded deletions, but the implementation complexity is substantially higher and the library is research-level.

### Tractable Path

Same as HLL: restrict `APPROX_PERCENTILE` to append-only sources initially. For the statistics infrastructure (Phase A in `optimizations.md`), KLL sketches per non-PK column per shard (built at flush time, Category A) give the quantile histogram that the statistics proposal requires — with no deletion concern since shards are immutable.

**KLL per column in shard metadata** is the highest-value immediate deployment: when writing a shard, build a KLL sketch for each numeric column. Store as a `.kll` sidecar. This gives the per-column quantile histogram for the statistics layer without requiring any incremental KLL infrastructure. The flush path already reads every column value — KLL construction is a single-pass addition.

### What It Enables

- Per-shard per-column quantile estimates (for Phase A statistics)
- `APPROX_PERCENTILE(col, 0.95)` on append-only views
- Equi-depth histogram construction from KLL sketches at analyze time
- Better join selectivity estimates than min/max alone (a uniform distribution between min and max is often wrong; quantiles bound the error on range selectivity estimates)

### Implementation Shape

KLL is a list of "compactors" at increasing levels. Each compactor is a sorted array of samples; when it overflows, it randomly keeps half the items (even or odd positions after sorting) and promotes them to the next level. The implementation requires:

- A `Vec<Vec<i64>>` or `Vec<Vec<f64>>` (one inner vec per level)
- Merge: insert batch → add to level-0 compactor → compact up as needed
- Query: merge all levels with their implicit weights (level i has weight 2^i), sort, scan for target rank

The space is O(ε⁻¹ log²(1/ε)) items, each 8 bytes. For 1% quantile error, ~100 items at 800 bytes. This is smaller than an HLL sketch per column.

---

## Opportunity 5: Theta Sketch for Join Output Cardinality

**Value: Medium-Low initially, high once multi-way joins land.**

### What HLL Cannot Do

HLL estimates `n_distinct(T.col)` (how many distinct values exist in column `col` of table `T`). For join cardinality estimation, the key question is: how many rows from `L` will match rows from `R` on the join column? This requires estimating `|distinct(L.join_key) ∩ distinct(R.join_key)|` — **set intersection cardinality**, not just set cardinality.

HLL supports union (`merge by max`) but not intersection. The inclusion-exclusion approximation `|A ∩ B| ≈ |A| + |B| - |A ∪ B|` requires all three, and `|A ∪ B|` from HLL has error, making `|A ∩ B|` estimates unreliable for small intersections (common in filtered join conditions).

### Theta Sketches

A Theta sketch (KMV sketch variant, as in Apache DataSketches) represents a set as the K smallest hash values of its elements. Union, intersection, and difference are computed over these hash sets. Key properties:

- Union: merge two sorted K-lists, take the K smallest
- Intersection estimate: `|A ∩ B| ≈ |intersection of K-lists| × (max(theta_A, max(theta_B)))⁻¹`
- Error: `O(1/√K)` — same as HLL for union, better than HLL for intersection

For gnitz's join cardinality, one Theta sketch per join-key column per shard gives:
- Intra-table intersection: do shards overlap in join key values? (determines cross-shard join contribution)
- Cross-table intersection: `|L.key ∩ R.key|` estimated by merging Theta sketches from all shards of each table

### Implementation Shape

A Theta sketch is a sorted array of K hash values (K = 1024 for ~3% error) plus a `theta` threshold. `add(key)`: hash key → if hash < theta, insert into sorted array; if array overflows, set theta = max(array), discard items > theta. Simpler than XOR8 construction. Serialize as a `.theta` sidecar per shard column.

This is lower priority than HLL because:
1. HLL covers the single-table cardinality case (90% of join estimation)
2. Multi-way joins are Tier 3 work (`optimizations.md` item #18, #22)
3. HLL union suffices for the common equijoin case with no filter on the join key

Theta sketches become highly valuable once delta joins and join order optimization are active.

---

## Opportunity 6: Universal Sketches (DaVinci / LMQ)

**Value: Medium, architectural. Relevant once multiple sketch types are deployed.**

### The Multi-Sketch Problem

Opportunities 1–5 propose deploying up to four separate sketch types:
- ExaLogLog per column (cardinality)
- KLL per column (quantiles)
- Count Sketch / TP-Sketch per view (frequency/persistence)
- Theta Sketch per join-key column (intersection cardinality)

Each sketch type requires its own construction, persistence, merge logic, and query interface. As the statistics layer grows, this becomes an engineering maintenance burden.

**DaVinci Sketch (2025)** addresses this with a single data structure handling up to **9 measurement tasks** simultaneously — including heavy hitters, distinct counting, inner join size estimation, and others. In multi-task scenarios it reduces total memory usage by over **59%** compared to maintaining the same tasks with independent sketches.

**LMQ-Sketch (DISC October 2025)** — "Lagom Multi-Query Sketch" — takes a complementary approach: fully lock-free concurrent inserts and queries from a single one-pass data structure. The lock-free property is directly useful in gnitz's single-process model where sketch updates could run on background threads, and the "one pass, multiple query types" property is valuable: LMQ is designed for databases where a row should update all statistics in a single scan, not in separate passes per sketch type.

### What This Means for Gnitz

**Near-term (building the statistics layer):** implement ExaLogLog, KLL, and Count Sketch as separate structures following the established sidecar pattern. This is simpler to implement, debug, and test independently.

**Medium-term (consolidation):** once multiple sketch types coexist and the shard-level statistics protocol is stable, evaluate replacing the per-column ExaLogLog + KLL combination with a universal sketch that covers both cardinality and quantiles from a single sidecar file. The 59% memory reduction is meaningful when a table has many columns and many shards.

**The incremental approach is still safer**: universal sketches have more complex internal state than single-purpose sketches. Rust makes polymorphic internal data structures straightforward via enums and generics, but the engineering complexity of validating a universal sketch's correctness across all measurement tasks favors building single-purpose sketches first and consolidating later once each is independently tested.

### AeroSketch (SIGMOD 2026)

Accepted to SIGMOD 2026: AeroSketch is a near-optimal matrix sketching framework for persistent, sliding-window, and distributed streams. It maintains continuous matrix approximations across distributed nodes.

This is relevant to gnitz's multi-worker architecture (currently in development) but only once the exchange and worker coordination layers are mature. Matrix sketches are most useful for graph-structured data and linear algebra over distributed relations — not gnitz's current SQL workload. Flag for reconsideration when multi-worker joins across partitions are the dominant performance concern.

---

## Opportunity 7: Bloom Filter Upgrade Assessment

**The MemTable Bloom filter is correct as-is. No action needed.**

The external research report mentions Binary Fuse Filters as a Bloom replacement. Binary Fuse filters are **static** (built once over a complete key set). The MemTable Bloom filter is **mutable** (rows are inserted incrementally as batches arrive). Static filters do not apply to the MemTable use case.

For the shard use case, XOR8 is already deployed and is the correct static filter choice. XOR8 is equivalent to Binary Fuse 8-bit in space efficiency and faster in lookup (3 array accesses vs. Binary Fuse's similar structure). No reason to replace either.

**The Ribbon Filter** (another static filter alternative) provides slightly better space efficiency than XOR8 for small sets but adds construction complexity with no meaningful benefit over XOR8 at gnitz's shard sizes. XOR8 was the right choice and remains so.

---

## Opportunity Summary and Priority Ordering

| Opportunity | Algorithm | Category | Priority | What it enables |
|---|---|---|---|---|
| **Cardinality per column in shard metadata** | ExaLogLog (EDBT 2025) | A (write-once) | **High** | n_distinct for join ordering, filter selectivity, zone map guidance; 43% less space than HLL |
| **Frequency statistics for delta batches** | Count Sketch + TP-Sketch (Feb 2026) | B (signed-counter) | **High** | GROUP BY skew detection (single-tick), cross-tick persistence tracking, exchange buffer pre-sizing |
| **Quantile histograms per column in shard metadata** | KLL | A (write-once) | **Medium** | equi-depth histogram statistics, range selectivity beyond min/max |
| **Approximate COUNT DISTINCT aggregate** | ExaLogLog (same impl) | A (append-only) | **Medium** | COUNT DISTINCT at O(730 bytes) state per group vs. O(\|distinct values\|) |
| **APPROX_PERCENTILE aggregate** | KLL | A (append-only) | **Medium-Low** | APPROX_PERCENTILE, APPROX_MEDIAN |
| **Universal sketch consolidation** | DaVinci / LMQ (2025) | A+B | **Medium (future)** | −59% memory for combined statistics once multiple types coexist; simpler API |
| **Join intersection cardinality** | Theta Sketch / K-minwise (OmniSketch) | A (write-once) | **Medium — build alongside ExaLogLog** | cross-table \|L.key ∩ R.key\| estimation; breaks independence assumption in join ordering; prerequisite for delta join planning |
| **Distributed matrix sketching** | AeroSketch (SIGMOD 2026) | Distributed | **Deferred** | multi-worker join statistics once exchange layer matures |
| **Bloom/XOR8 replacement** | — | — | **None** | already optimal |

---

## Architectural Notes for Rust Implementation

### Memory layout
Sketch backing arrays are owned `Vec<u8>` (or typed vectors as appropriate). Sketches have shard-level or tick-level lifetime — they are created during flush or tick processing and dropped when no longer needed. Rust's ownership model handles this naturally.

### Hashing
XXHash64 (via the `xxhash-rust` crate) is the correct hash for all sketch structures. For Count Sketch's requirement of `d` independent hash functions: derive by seeding XXHash with different seed values per row (the same technique as XOR8's seed-retry construction). For Theta sketches, a single unseeded hash is sufficient.

### Sidecar file pattern
ExaLogLog, KLL, and Theta sketches at the shard level should follow `.xor8`'s sidecar pattern: a `filename + ".exll"`, `filename + ".kll_c<col_idx>"`, and `filename + ".theta_c<col_idx>"` file written atomically alongside the shard. `ShardHandle` loads them at open time with a fallback to `None` if the sidecar does not exist. This ensures backwards compatibility: old shards without sidecars work (sketches are absent but no error); new shards have full statistics.

### sketch_oxide
`sketch_oxide` is a 2025 pure-Rust library containing 41 production-ready sketch implementations including UltraLogLog, Binary Fuse Filters, REQ, and others. It is a candidate dependency for the server — evaluate whether its implementations are suitable before building custom ones. The `gnitz-py` and `gnitz-capi` Rust crates could also use it for client-side analytics.

### Column indexing
The shard's directory already maps `col_to_reg_map` (column index → register offset in the columnar layout). Sketch construction can iterate column buffers using the same mapping.

### Multi-column correlation
The `optimizations.md` Phase A proposal mentions "multi-column correlation sketches." This is a Count Sketch (or CMS) over `(col_a_value, col_b_value)` pairs. The implementation is identical to the per-column sketch but keyed on a combined hash. This defers naturally: build per-column sketches first, extend to cross-column pairs once the single-column statistics infrastructure is in place.

---

## Connection to Existing Optimization Plans

The sketches proposed here are not independent features — they are the data layer that unlocks several already-identified optimizations in `research/optimizations.md`:

- **ExaLogLog + KLL in shard metadata** → enables Phase A statistics layer → enables join ordering (item #15), filter selectivity estimates, predicate pushdown; ExaLogLog replaces HLL/ULL with 43% better space efficiency (EDBT March 2025)
- **Count Sketch + TP-Sketch in executor** → enables Phase B adaptive feedback → adaptive exchange buffer sizing (item #7), tick-rate adaptation; TP-Sketch (Feb 2026) adds cross-tick persistence tracking that Count Sketch alone cannot provide
- **ExaLogLog APPROX_COUNT_DISTINCT** → enables approximate GROUP BY aggregation at lower state cost; reuses same ExaLogLog implementation as shard statistics
- **Theta Sketch** → enables cross-table join cardinality → enables delta join planning (item #22) and join ordering cost model
- **KLL quantile histograms** → enables equi-depth histogram statistics → enables better selectivity estimates beyond uniform distribution assumption
- **DaVinci / LMQ universal sketches** → once multiple sketch types coexist, consolidate into a single structure for 59% memory reduction and single-pass update semantics; deferred until individual sketches are stable

None of these require ML infrastructure. All are implementable in Rust, either from scratch or using established crates. They form the "classical statistics foundation" (Phase A) that `optimizations.md` identifies as the prerequisite for everything adaptive and learned.

---

## Broader Landscape: What Applies to Gnitz and What Doesn't

The 2025–2026 database industry has four major sketch-adjacent movements. Each is assessed honestly below.

---

### 1. OmniSketch for Multi-Join Cardinality — Directly Relevant

**What it is.** OmniSketch (late 2025, tested on DuckDB) combines Count-Min Sketch with **K-minwise hashing** to estimate multi-way join sizes *without* the independence assumption. Traditional histogram-based cardinality estimation assumes `P(A.x = v AND B.x = v) = P(A.x = v) × P(B.x = v)` — i.e., the value distributions of two columns are independent. This assumption fails badly for correlated data (e.g., `orders.region` and `customers.region` are not independent when `orders` has a FK to `customers`). OmniSketch directly measures the overlap between two columns' value sets via K-minwise intersection, reducing intermediate result size estimates by up to **1,000×** in reported benchmarks.

**The connection to gnitz's Theta Sketch (Opportunity 7).** K-minwise hashing is the same algorithm as the Theta sketch / KMV sketch already identified in Opportunity 7. A Theta sketch *is* a K-minwise hash sketch. OmniSketch formalizes the multi-join extension: build one Theta sketch per join-key column; estimate `|A.join_key ∩ B.join_key|` via sketch intersection; use this as the join output cardinality estimate instead of `n_distinct(A.join_key) × n_distinct(B.join_key) / max(n_distinct)`.

For gnitz this means: Opportunity 7 (Theta Sketch per join-key column) is not just a "future nice-to-have for delta joins." It is specifically the data structure that OmniSketch uses to break the independence assumption — the most impactful cardinality estimation improvement identified in the 2025 VLDB retrospective. If join ordering and multi-way joins are on gnitz's roadmap, the Theta sketch is the right first investment, not the ExaLogLog (which only estimates column cardinality independently).

**What gnitz can implement today.** Gnitz currently has single equijoins only. OmniSketch's full multi-join algorithm (chaining intersections across N tables) is Tier 3 work. But the prerequisite — one Theta sketch per join-key column per shard — can be built alongside ExaLogLog sidecars now. The sketch intersection math is pure arithmetic over sorted arrays, implementable in Rust without any change to the query execution path.

**The independence assumption problem in gnitz's current design.** `optimizations.md` item #15 (join ordering) proposes using `n_distinct` from shard metadata to choose the smaller table's trace. This uses per-table `n_distinct` independently — it is the independence assumption in thin disguise. If gnitz adds multi-way joins before adding OmniSketch, the join ordering will be wrong for correlated data. OmniSketch + Theta sketches is the correct solution to this specific problem.

---

### 2. Approximate Sketches for Dynamic Filters — Future Direction, Not Now

**What it is.** A 2025/2026 research direction that extends pre-built sketches (computed over an entire column) to answer cardinality questions for arbitrary `WHERE` clause predicates. A standard ExaLogLog sketch on `orders.user_id` estimates `n_distinct(user_id)` for the entire table. It cannot directly answer "what is `n_distinct(user_id)` for `WHERE status = 'paid'`?" without rebuilding from scratch.

The research uses learning-based methods (lightweight models trained on the sketch + predicate pair) to extrapolate filtered cardinalities from global sketches.

**Why it doesn't apply to gnitz now.** Gnitz does not yet have the statistics foundation (Phase A) or runtime feedback (Phase B) that would make filtered cardinality estimation actionable. Additionally, the learning-based approach requires training infrastructure that gnitz does not have.

**The classical fallback.** For gnitz's most common filter patterns (equality predicates on low-cardinality columns like `status`, `region`, `type`), the classical solution is **stratified sketches**: when flushing a shard, build a separate ExaLogLog per distinct value of commonly-filtered columns. A shard with 5 distinct `status` values gets 5 ExaLogLog sketches per payload column. This is O(V × C × M) bytes per shard (V = distinct filter values, C = payload columns, M = registers), tractable for low-cardinality filter columns, prohibitive for high-cardinality ones.

This is a meaningful future addition but requires knowing which columns are commonly used as filters — information that only exists after the statistics layer (Phase A) and runtime feedback (Phase B) are in place. Defer until then.

---

### 3. Vector Databases / LSH / PQ — Out of Scope

**Not applicable to gnitz.** LSH (Locality-Sensitive Hashing) and PQ (Product Quantization) are approximate nearest-neighbor algorithms for high-dimensional vector spaces. They compress floating-point vectors into compact probabilistic representations for similarity search. Gnitz is a relational database with Z-Set semantics, integer/float/string column types, and SQL query semantics. There is no vector type, no embedding storage, and no similarity search path.

The "two-tier probabilistic execution" pattern (coarse retrieval via compressed index → deterministic re-ranking) is the vector database query pattern, not a SQL query pattern. If gnitz ever adds a vector column type and `<=>` distance operator, revisit. Until then, this entire area is architecturally orthogonal.

The broader point about "sketch-first pipelines" is already gnitz's direction: XOR8 for membership fast-fail, then binary search. ExaLogLog for cardinality estimation, then cost-based join ordering. The pattern is sound; the vector-specific algorithms are not the vehicle.

---

### 4. Differential Privacy + Sketches — Interesting Signal, No Near-Term Action

**What it is.** Because Count-Min Sketch and related structures are **linear** (sketch(A + B) = sketch(A) + sketch(B)), individual nodes in a distributed system can add localized DP noise to their local sketch before sending it to the aggregator. The aggregator merges noisy sketches and gets accurate global statistics without seeing any raw data. This pattern underlies privacy-preserving analytics platforms at Google, Apple, and Meta.

**What's interesting for gnitz's multi-worker design.** Gnitz's multi-worker architecture partitions data across N processes, each holding 1/N of the 256 hash partitions. For the shard statistics sidecars (ExaLogLog, KLL, Count Sketch), workers naturally build per-partition sketches independently and the master merges them at query time. This is *structurally identical* to the distributed sketch merging pattern from DP literature — gnitz gets the distributed sketch merging property for free from the linear structure of its sketches.

Differential privacy itself is not a current gnitz requirement: there is no multi-tenant isolation model, no user-level privacy budget, and no regulatory context driving it. The DP noise addition step is not needed for correctness, and adding it without a well-defined privacy model would only degrade accuracy.

**The ICML 2025 adaptive search protection** (combining DP with approximate nearest neighbor sketches to prevent query-based extraction of training data) is relevant only to AI/ML database systems. Out of scope.

**Practical takeaway.** Design gnitz's sketch merging interfaces to be linearly composable — `merge(sketch_A, sketch_B) = combined_sketch` — because this is already required for correctness (merging per-shard ExaLogLog sketches to get per-table n_distinct), and it happens to be the prerequisite for any future privacy layer. This is already the natural design; no extra work required.

---

### 5. eBPF / SmartNIC Sketch Offloading — Out of Scope

**Not applicable to gnitz.** NitroSketch (eBPF in the Linux kernel), FASTeller, and Elastic Sketch on SmartNICs/FPGAs are designed for **network packet-level stream processing**: counting flows, detecting heavy hitters in IP traffic, monitoring at line-rate before data hits the server. The use case is fundamentally different from gnitz's.

Gnitz's ingestion path: `INSERT` SQL → Rust client → Unix domain socket IPC → server process → WAL → MemTable. The bottleneck is not network packet processing at line rate; it is SQL execution, WAL I/O, and MemTable management. Pushing a Count Sketch into eBPF would require the sketch keys to be packet-level fields (source IP, destination port), not SQL column values. There is no architectural path to benefit.

The only tangentially relevant observation: gnitz's multi-worker IPC uses Unix domain sockets. If gnitz ever processes very high-rate sensor/telemetry data and the IPC layer becomes the bottleneck, kernel-bypass techniques (io_uring, not eBPF) would be the right tool — and that has nothing to do with sketches.

---

### What the Broader Landscape Confirms for Gnitz's Sketch Roadmap

The four trends above, filtered through gnitz's architecture, converge on two concrete additions to the priority ordering:

1. **Upgrade Theta Sketch (Opportunity 7) from "low now" to "medium, build now alongside ExaLogLog."** OmniSketch's published results confirm that K-minwise intersection (= Theta sketch intersection) is the most effective known technique for join cardinality under correlated data — the exact problem that ruins independence-assumption-based join ordering. Gnitz should build the Theta sketch sidecar in the same pass as the ExaLogLog sidecar at flush time, even before multi-way joins land.

2. **Stratified sketches per low-cardinality filter column are the classical answer to the dynamic-filter problem**, once Phase A statistics reveal which columns are commonly used in WHERE clauses. This is a Phase B/C item, not Phase A.

Everything else in the 2026 landscape — vector search, eBPF offloading, DP noise injection — is orthogonal to gnitz's current architecture and can be cleanly deferred or permanently out-of-scoped.

---

## Count-Min Sketch-Driven Flush and Compaction (HTAP Pattern)

### The HTAP Framing

HTAP (Hybrid Transactional/Analytical Processing) systems maintain two tiers: a mutable row-oriented delta store for fresh writes, and an immutable columnar store for analytical scans. Compaction merges the delta into the columnar store. The key insight from recent work (Colibri, VLDB) is that compaction timing should be driven by a workload heat map: keep hot rows in the delta store (they'll be updated again), flush cold rows to the columnar store (they've stabilized).

Gnitz has exactly this two-tier structure:

| HTAP concept | Gnitz equivalent |
|---|---|
| Mutable delta store | `MemTable` (one per partition, bounded by `max_bytes`) |
| Immutable columnar store | Shard files (`TableShardView`, mmap'd via `ShardHandle`) |
| Delta → columnar merge | `MemTable.flush()` → `writer_table.write_batch_to_shard` |
| Columnar compaction | `EphemeralTable._compact()` → `compactor.compact_shards` |
| "Compact too early" cost | CPU wasted on shard merges + fragmented shard reads |
| "Compact too late" cost | `UnifiedCursor` must merge more sorted runs at read time |

Both flush and compaction decisions are currently blind to workload frequency:
- `MemTable.should_flush()` (`memtable.py:102`): fires at `_total_bytes() > max_bytes * 3/4` — pure size
- `ShardIndex._check_compaction_health()` (`index.py:144`): fires at `len(handles) > compaction_threshold` — pure count

### The Z-Set Argument Is Stronger Than in Standard HTAP

Standard HTAP's motivation for delaying flush of hot rows: avoid flushing a row and then immediately re-reading it for the next update. The cost is one extra disk read.

In gnitz's Z-Set model, the motivation is stronger: **algebraic cancellation in memory eliminates disk writes entirely**.

Consider a shopping cart item that is inserted (weight +1) then removed (weight -1) within the same MemTable window. The MemTable's `_merge_runs_to_consolidated` consolidates these: net weight = 0, the row is excluded from the flush output. Zero shard entries. Zero compaction work.

If a flush occurs between the +1 and -1 writes, each generates a separate shard entry. Compaction must later read both entries, consolidate them to weight 0, and write a compacted shard with neither entry — spending disk I/O, CPU, and compaction bandwidth to undo work that never needed to happen.

The frequency of this pattern is directly observable from the delta batch stream: a key that appears in many consecutive delta batches (alternating +1/-1, or receiving repeated +1 updates as a counter) is generating exactly this churn. A Count-Min Sketch over PK churn detects which keys, or which partitions, are generating premature flush waste.

### Three Distinct Sketch-Driven Decisions

**Decision 1: When to flush a MemTable (flush timing).**

The current `should_flush()` threshold is 75% of `max_bytes`. Adding a frequency signal: if a partition is receiving writes at high rate (Count-Min query on the partition index returns high count), it will likely receive more writes before the analytical read window. Delay flush to allow more in-MemTable consolidation. If a partition is cold (low count), flush proactively to reclaim capacity for hot partitions.

This doesn't require per-PK frequency. Partition-level frequency (256 possible values) is sufficient. A dedicated Count-Min instance over partition indices (a 3×256 int array = 3KB) updates once per ingested row: `sketch.update(partition_index, 1)`.

**Decision 2: Adaptive MemTable capacity allocation per partition.**

Today all 256 partitions share the same `max_bytes` limit (set at construction time in `hooks.py`). Hot partitions hit the limit and flush frequently; cold partitions sit well under the limit indefinitely. This is inefficient in both directions.

With a frequency heat map: re-balance capacity so hot partitions get larger MemTable budgets (more room for consolidation) and cold partitions get smaller ones (flush earlier to reclaim total memory). The total memory budget stays constant; the allocation across 256 partitions becomes workload-adaptive.

This is a more invasive change than Decision 1 (requires modifying `PartitionedTable` to allocate varying `arena_size` per partition), but the payoff is higher: reduces the write amplification from premature flushing of hot partitions.

**Decision 3: Adaptive compaction threshold per partition.**

`ShardIndex._check_compaction_health()` uses a fixed `compaction_threshold = 4`. With frequency signals:

- **Hot partition (high read rate)**: lower threshold (2–3 shards). Compact aggressively. Fewer shards means `UnifiedCursor` merges fewer sorted runs, improving analytical scan performance on frequently-queried data.
- **Cold partition (low read rate)**: higher threshold (8–16 shards). Delay compaction. No reads are being hurt by shard fragmentation; save the CPU.

This is the classical "adaptive compaction threshold" identified in `optimizations.md`. The Count-Min Sketch over partition read frequency (separate from write frequency) is the signal. Read events are observable when `create_cursor()` is called on a partition — increment the sketch entry for that partition.

### Count-Min Sketch Is Correct Here (Exception to the Signed-Counter Rule)

The general rule in this document is that gnitz's Z-Set signed weights require Count Sketch (not Count-Min Sketch) for correctness. This use case is the exception.

The purpose here is measuring **churn magnitude** (absolute write frequency), not net Z-Set weight. Whether a write is weight +1 or weight -1, it counts as one "write event" toward the heat signal — a row being retracted is just as hot as a row being inserted. The sketch should be updated with `+1` for every write event, regardless of the event's Z-Set weight.

Count-Min Sketch with unsigned `+1` increments gives an upper-bound frequency estimate, which is exactly the right signal for flush scheduling: err on the side of "this partition might be hot" rather than "this partition is definitely cold." Under-flushing a hot partition wastes memory; over-flushing a cold partition wastes CPU. The conservative overestimate from Count-Min Sketch matches the desired asymmetric cost model.

Count Sketch's median-of-signed-estimates would give a different, unbiased answer — but "is this partition receiving many writes?" is better served by a conservative overestimate than an unbiased one. Count-Min Sketch is correct and appropriate here.

### Decay: The Cooling Signal

The heat map needs to decay over time. A partition that was hot last hour but idle now should eventually be treated as cold. The Colibri model handles this with periodic sketch reset or sliding window counts.

For gnitz:
- **Periodic reset**: at each DBSP tick boundary, halve all Count-Min counters (right-shift by 1). This gives exponential decay: a partition written 100 times this tick then never again will have count ~100 → ~50 → ~25 → ... → ~0 over subsequent ticks. The half-life is one tick.
- **Counter cap**: cap counters at a maximum value (e.g., 255) to prevent cold-start bias from one large initial batch dominating forever.
- **Decay rate tuning**: the half-life should be configurable. For fast-moving workloads (new hot partitions emerge frequently), fast decay (per-tick halving). For stable workloads (hot partitions stay hot for hours), slow decay (halve every N ticks).

This decay mechanism transforms the Count-Min from a cumulative frequency counter into a recency-weighted frequency estimator — exactly the "heat map" described in the HTAP literature.

### Implementation in Context

The Count-Min Sketch for flush/compaction scheduling is a third sketch type alongside:
- Count Sketch (Opportunity 2): for detecting GROUP BY column skew in delta batches (signed weights needed)
- TP-Sketch (Opportunity 2): for detecting persistent group keys across ticks (cross-tick identity)

The three serve different purposes and cannot be unified into one. The Count-Min for flush scheduling is the simplest of the three: unsigned counters, partition-granularity only, decay-with-reset. It is a 3×256 integer array — 3KB. It slots into the PartitionedTable's ingest path (`ingest_batch` iterates sub-batches per partition) with a single additional increment per partition touched.

---

## S3-FIFO and Ghost Queues: Eviction Policy for Gnitz

### What S3-FIFO Is

S3-FIFO (SOSP 2023, Yang et al., CMU) is a cache eviction policy that replaces LRU with three queues:

- **S (small FIFO, ~10% of capacity)**: all incoming items land here first
- **M (main FIFO, ~90% of capacity)**: items promoted from S when evicted with access bit set
- **G (ghost FIFO)**: metadata-only ring of recently-evicted items from S (keys only, no data)

The access bit on each cache entry is a single bit set when the item is read. At eviction time from S: if the bit is set (item was re-accessed), promote to M and clear the bit. If unset (cold item, never re-accessed), evict outright. Items evicted from M go to the ghost queue G.

The ghost queue's role: when a new item is admitted and its key matches a ghost entry (recently evicted), it goes directly to M rather than S — it already proved it has reuse value. This eliminates thrashing without the overhead of LFU's full frequency counters or LRU's linked-list churn.

The "probabilistic frequency tracking" extension (TinyLFU / S3-FIFO-F) augments ghost queue lookups with a Count-Min Sketch: instead of just binary ghost presence, the sketch provides a frequency score. Items with frequency above a threshold bypass S entirely and enter M directly. This prevents cache pollution from scan-once access patterns.

### What Gnitz Has Today

Gnitz has no buffer pool manager in the classical sense. There are three distinct caching-adjacent decisions made today:

1. **MemTable flush** (`memtable.py`): triggered when `_total_bytes() > max_bytes × 3/4`. Size-based, no frequency signal. All 256 partition MemTables are managed independently.

2. **Shard handle lifetime** (`index.py`, `ShardHandle`): all shards are kept mmap'd from `add_handle` until `replace_handles` during compaction removes them. No eviction, no pool limit. For a user table with 256 partitions × 4 shards each (before compaction) = 1024 open mmap regions.

3. **State eviction** (`optimizations.md` item #20): not yet implemented. Noria-style partial materialization would evict cold partitions from EphemeralTable trace state. Today, trace registers grow unboundedly.

### Where S3-FIFO Applies

**Application 1: ShardHandle pool (immediate, tractable).**

`ShardIndex` keeps every shard handle open indefinitely. For large tables approaching Linux's `max_map_count` limit (65,536 by default, shared across the process), this becomes a constraint. More immediately, keeping hundreds of mmap regions warm when only a few partitions are hot wastes OS page cache pressure.

S3-FIFO maps cleanly:
- **S queue**: recently-flushed shard handles (new shards start cold)
- **M queue**: handles that were accessed while in S (proven reuse)
- **G queue**: `(pk_min, pk_max, table_id, filename)` tuples for recently-closed handles — 48 bytes each, not the full mmap

When a cursor query hits `find_all_shards_and_indices`, if the target key falls in the range of a ghost entry, the handle is reopened directly into M (main queue) — it just demonstrated reuse. The ghost queue lookup is O(1) via a compact hash set over handle identifiers.

The access bit: set in `find_all_shards_and_indices` when a shard is included in the lookup result. Cleared at eviction check time.

**Application 2: Trace state eviction policy (prerequisite for item #20).**

When `optimizations.md` item #20 (state eviction) is implemented, the eviction policy determines which of the 256 PartitionedTable partitions to evict from memory. S3-FIFO is the correct policy:

- **S**: newly-active partitions (just received their first delta batch this session)
- **M**: partitions that have been accessed multiple times since last eviction
- **G**: recently-evicted partition identifiers — if a query immediately needs an evicted partition, promote on reconstruction

Without the ghost queue, a naive LRU eviction policy thrashes: if the working set is 200 partitions but memory fits 180, the 20 overflow partitions get evicted and immediately re-requested, causing a reconstruction loop. The ghost queue detects this pattern and promotes the hot-but-evicted partitions to M, extending their residence time.

**Application 3: Frequency-aware MemTable flush ordering.**

Currently all partitions' MemTables are flushed independently based on size. A partition that receives a write every 5ms will flush frequently; one that hasn't seen writes in minutes holds a small MemTable uselessly. With a Count-Min Sketch (already proposed in Opportunity 2) tracking write frequency per partition, the flush scheduler can prioritize:

- Hot partitions (high write frequency): flush early to reclaim MemTable capacity for the next delta batch
- Cold partitions (low write frequency): flush proactively to free capacity, accepting that they'll likely stay cold

This is S3-FIFO's frequency oracle applied to flush scheduling rather than eviction. The same Count Sketch built for GROUP BY skew detection (Opportunity 2) serves double duty here — partition write frequency and group key frequency are both extractable from the delta batch.

### The Convergence with Opportunity 2

The Count Sketch proposed in Opportunity 2 (delta-batch frequency statistics) and the S3-FIFO frequency oracle are the same data structure applied to two different keys:

- Opportunity 2's Count Sketch: keyed on group-by column values → detects GROUP BY skew
- S3-FIFO's frequency oracle: keyed on partition index → detects hot vs. cold partitions

Both use signed-weight updates from delta batches. Both are maintained in the executor per tick. They can share a single `DeltaStatistics` object with separate sketch instances or a single sketch over a composite key — the choice depends on whether the memory overhead of two 12KB sketches is meaningful (it isn't, but the single-pass update is slightly cleaner).

### Implementation Shape

For the ShardHandle pool specifically:

```
ShardHandlePool(capacity):
    small_queue: circular FIFO of (handle, access_bit), size = capacity / 10
    main_queue: circular FIFO of (handle, access_bit), size = capacity * 9 / 10
    ghost_set: compact hash set of (pk_min_lo, pk_min_hi, table_id), capacity × 1 entry

admit(handle):
    if handle.key_range in ghost_set:
        ghost_set.remove(handle.key_range)
        main_queue.push(handle, access_bit=False)  # ghost hit → main
    else:
        small_queue.push(handle, access_bit=False)  # cold start → small

access(handle):
    handle.access_bit = True

evict_one():
    # Try to evict from small first
    while small_queue not empty:
        h, bit = small_queue.front()
        if bit:
            small_queue.pop_front()
            main_queue.push(h, access_bit=False)  # promote
        else:
            small_queue.pop_front()
            ghost_set.add(h.key_range)
            h.close()
            return
    # Fall back to main queue
    h, _ = main_queue.pop_front()
    h.close()
```

The ghost set's memory: 256-entry ring of 24 bytes each = 6KB. The full pool tracking adds ~8 bytes per handle (queue pointers). For 1024 open handles (256 partitions × 4 shards), total overhead is ~8KB — negligible.

---

## Invertible Bloom Lookup Tables: Set Reconciliation for Gnitz

### What IBLTs Are

An Invertible Bloom Lookup Table (Eppstein et al., SIGCOMM 2011) extends the Bloom filter with a property that a standard Bloom filter lacks: **recovery**. Given two IBLTs representing two sets, you can compute their symmetric difference `S_A Δ S_B` by subtracting one from the other and "peeling" the result. Communication cost is O(|S_A Δ S_B|) — proportional to the difference, not to the total set sizes.

Each IBLT cell stores:
- `count`: signed integer, number of insertions minus deletions mapping to this cell
- `key_sum`: XOR of all keys mapping to this cell
- `hash_sum`: XOR of `hash(key)` for all keys (used to verify a recovered key is genuine)

With `k` hash functions mapping each key to `k` cells:
- `insert(key)`: for each of the `k` cells, increment count, XOR key into key_sum, XOR hash(key) into hash_sum
- `delete(key)`: same with decrement / XOR (XOR is self-inverse)
- `subtract(A, B)`: cellwise A.count - B.count, A.key_sum XOR B.key_sum, A.hash_sum XOR B.hash_sum
- `peel()`: while any cell has `count = ±1`, its `key_sum` is the sole key (verify via `hash_sum`); delete it from its `k` cells; repeat until empty (success) or stuck (need larger table)

Required table size: `≥ 1.5 × d` cells where `d = |S_A Δ S_B|`. For `d = 0`, a 0-byte table suffices (vacuous). For `d ≈ 100`, ~150 cells × 20 bytes = 3KB.

### Gnitz Has No Content-Level Consistency Verification

Gnitz currently has checksums at two levels:
- Per-shard file: the `dir_checksums` array in `TableShardView` covers per-column data regions (byte-level integrity)
- WAL: written atomically, replayed at startup

What checksums catch: bit corruption, truncated files, wrong version.
What checksums cannot catch: **semantic inconsistencies** — rows that were supposed to be in the store but aren't, rows that appear in two partitions, a view trace that diverged from the base table, worker state that missed a delta batch.

IBLT-based reconciliation catches the semantic inconsistencies that checksums miss.

### Application 1: WAL Replay Completeness (High Value, Low Complexity)

During startup recovery, `PersistentTable` replays uncommitted WAL entries into the MemTable. The current code trusts this replay is complete. An IBLT can verify it:

- When writing the WAL, incrementally update an IBLT over the transaction's PKs. Write the final IBLT as a WAL sidecar at WAL close time (just before marking the WAL committed).
- On recovery, replay the WAL into the MemTable as today. Then build an IBLT over the recovered MemTable's PK set. Subtract the two IBLTs. The difference should be empty. If it isn't, the peeled entries are the missing PKs — log them and either retry or flag the recovery as incomplete.

The IBLT at WAL close time is O(|transaction| × 20 bytes) for a 0-difference expectation. Since the expected difference after correct replay is 0, the minimum IBLT size is small (just enough to handle up to some bounded failure — say 1000 missed rows = 30KB). The sidecar is written once and checked once per recovery.

### Application 2: Compaction Correctness (Medium Value, Low Complexity)

`EphemeralTable._compact()` merges N shard files into one via `compactor.compact_shards`. The merged shard should contain exactly the net non-zero-weight rows from the source shards. Currently there is no post-compaction verification.

A weight-aware IBLT can verify this. Each IBLT cell carries an additional `weight_sum: i64`. Insert `(pk, weight)` into the source shards' IBLT; insert `(pk, net_weight)` into the result shard's IBLT; subtract and peel. For correct compaction, the result is empty (all weights cancel). A non-empty result reveals which PKs were dropped or have wrong weights.

This is only needed for debugging and correctness testing, not production hot-path. The compactor already has access to source shard views (via the same mmap infrastructure as normal shard reads) so building the IBLT pre-compaction is a single sequential scan — same access pattern as the compaction itself.

### Application 3: Multi-Worker Exchange Verification (High Value, Required for Correctness)

In gnitz's multi-worker design, `op_exchange_shard` routes each row in a delta batch to a target worker based on `hash(pk) & worker_mask`. The receiving worker ingests its sub-batch. If a row is lost in transit (IPC error, buffer overflow, delivery failure), it silently disappears — the DBSP circuit continues computing on incomplete data, producing wrong view outputs with no error signal.

IBLT-based exchange verification:
- Sender builds an IBLT over all rows sent to worker W (`pk, weight` pairs), sends the IBLT alongside the data batch as a verification trailer (cheap: O(expected_batch_size × 20 bytes))
- Receiver builds an IBLT over all rows it ingested
- Receiver subtracts the two IBLTs, peels: any non-empty result is a lost or corrupted row
- Receiver ACKs to sender with pass/fail; sender retransmits failed rows

The IPC protocol in `ipc.py` already has a framing layer. Adding an IBLT trailer to the batch message is a protocol extension, not an architectural change. The IBLT for a typical delta batch of 1000 rows is ~30KB — small compared to the batch payload.

This is the correctness-critical application. Silent data loss in the exchange operator is the failure mode that causes view divergence with no observable error. IBLT reconciliation is the right tool precisely because it is O(lost rows), not O(all rows).

### Application 4: Cross-Partition Routing Invariant (Medium Value)

`PartitionedTable.ingest_batch` routes each row to exactly one partition via `_partition_for_key`. The invariant: for any PK `k`, it appears in partition `hash(k) & 0xFF` and nowhere else. Today this is trusted by construction; no runtime check exists.

An IBLT verification pass after bulk ingest: build one IBLT per partition over its PKs; XOR all IBLTs together (equivalent to subtracting all from a combined set). For correct routing, every PK appears in exactly one partition's IBLT, so the XOR-of-all-IBLTs is empty (each PK's key_sum contribution appears in exactly one IBLT and is XOR'd away). Any non-empty peeled result is a routing error.

This is a debugging tool, not a production hot-path check. Run it on test ingestion paths to validate the routing logic, not on every production ingest.

### Application 5: View Trace vs. Base Table Consistency (Medium Value, Deferred)

A view's trace (EphemeralTable backing the `op_reduce`, `op_distinct`, or `op_join` operators) should, at any quiescent point, be exactly the DBSP computation result over the current base table. After crash-recovery with partial WAL replay, this invariant may be violated.

IBLT-based view validation: build an IBLT over the view trace's PK set; build an IBLT over the expected PK set (derived from the base table by running a full recomputation scan). Subtract and peel. Non-empty result = view diverged from base table; the peeled entries identify which groups or rows need recomputation.

This requires running the DBSP computation offline to generate the "expected" IBLT — expensive for large tables. It is a recovery diagnostic tool, not a routine check. Deferred until view backfill (item #16) and state eviction (item #20) are implemented, since those are the code paths that can introduce view divergence.

### Implementation Shape

IBLT for gnitz's U128 PKs:

```
Cell layout (per cell, 28 bytes):
    count:    i32     (4 bytes)
    key_lo:   u64     (8 bytes)  — XOR of pk_lo fields
    key_hi:   u64     (8 bytes)  — XOR of pk_hi fields
    hash_sum: u64     (8 bytes)  — XOR of hash(key) for verification

IBLT(num_cells, k=3):
    cells: raw array of num_cells × 28 bytes

insert(pk_lo, pk_hi, weight=1):
    h = xxh.hash_u128_inline(pk_lo, pk_hi)
    for each of k hash functions derived from h:
        cell_idx = derived_bucket(h, i, num_cells)
        cells[cell_idx].count += weight
        cells[cell_idx].key_lo ^= pk_lo
        cells[cell_idx].key_hi ^= pk_hi
        cells[cell_idx].hash_sum ^= h

subtract(other):
    for i in range(num_cells):
        cells[i].count -= other.cells[i].count
        cells[i].key_lo ^= other.cells[i].key_lo
        cells[i].key_hi ^= other.cells[i].key_hi
        cells[i].hash_sum ^= other.cells[i].hash_sum

peel() -> list of (pk_lo, pk_hi, count):
    results = []
    queue = [i for i in range(num_cells) if cells[i].count == ±1]
    while queue:
        i = queue.pop()
        if cells[i].count not in {1, -1}: continue
        # verify: hash(key_lo, key_hi) == hash_sum
        pk_lo, pk_hi = cells[i].key_lo, cells[i].key_hi
        if xxh.hash_u128_inline(pk_lo, pk_hi) != cells[i].hash_sum: continue
        results.append((pk_lo, pk_hi, cells[i].count))
        # remove from all k cells
        remove(pk_lo, pk_hi, cells[i].count)
        # check if any neighbor cells became count=±1
        for each of k cells of (pk_lo, pk_hi):
            if cells[j].count == ±1: queue.append(j)
    return results
```

This is a straightforward `Vec<u8>` allocation of `num_cells * 28` bytes. No external dependencies. Implementation complexity is comparable to the XOR8 filter — simpler construction, more complex peel.

For gnitz's U128 PKs the XOR-of-keys is a natural 128-bit operation (using the existing `lo/hi` split). The hash for verification reuses `xxh.hash_u128_inline`.

### IBLT Size Selection

| Use case | Expected `d` | Table size | Memory |
|---|---|---|---|
| WAL replay verification | 0 (normal) / ≤1000 (fault) | 1500 cells | 42KB |
| Compaction verification | 0 (normal) / ≤100 (fault) | 150 cells | 4.2KB |
| Exchange per-batch verification | 0 (normal) / ≤batch_size | 1.5× batch_size cells | ~batch_size × 42 bytes |
| Cross-partition routing check | 0 (normal) | 150 cells (diagnostic only) | 4.2KB |

For exchange verification specifically, the IBLT size scales with expected batch size. For a 1000-row delta batch, the IBLT trailer is 42KB — about 4% overhead on a batch carrying 10KB of column data. Whether this is acceptable depends on the expected batch size and error rate. For a zero-error expectation (correct implementations), the overhead is entirely wasted in normal operation. This is a deployment option, not a mandatory feature.
