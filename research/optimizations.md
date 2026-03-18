# SQL-to-DBSP Optimization Research: Feldera, Theory, and Gnitz

---

## Part I: Feldera's Optimization Pipeline

Feldera's SQL-to-DBSP compiler is a two-stage system: Apache Calcite handles relational algebra optimization, then a custom CircuitOptimizer applies 75+ passes on the DBSP circuit IR.

### Stage 1: Calcite Relational Optimizer (18 HepPlanner phases)

All heuristic (HepPlanner), not cost-based.

| Phase | Rules / Techniques |
|---|---|
| Merge identical ops | PROJECT_MERGE, UNION_MERGE, AGGREGATE_MERGE, INTERSECT_MERGE, MINUS_MERGE |
| Constant folding | Custom `ReduceExpressionsRule` (evaluates constant subtrees, handles FILTER/PROJECT/JOIN/WINDOW/CALC), `ValuesReduceRule` (merges constant VALUE relations with filters/projections) |
| Empty relation pruning | 9 `PruneEmptyRules` for UNION/JOIN/FILTER/SORT/AGGREGATE |
| Set op simplification | Converts UNION-of-FILTERs → single FILTER with OR; INTERSECT_FILTER_TO_FILTER; MINUS_FILTER_TO_FILTER |
| Sort removal | SORT_REMOVE, SORT_REMOVE_REDUNDANT, SORT_REMOVE_CONSTANT_KEYS |
| Subquery decorrelation | Custom `InnerDecorrelator` (two passes), `CorrelateUnionSwap`, `UNNEST_DECORRELATE`, full `RelDecorrelator.decorrelateQuery()` |
| Join order (3+ joins) | JOIN_TO_MULTI_JOIN → MULTI_JOIN_OPTIMIZE_BUSHY (bushy trees) → MULTI_JOIN_OPTIMIZE (linear fallback); hypergraph via JOIN_TO_HYPER_GRAPH + HYPER_GRAPH_OPTIMIZE |
| Predicate/filter pushdown | FILTER_INTO_JOIN, JOIN_CONDITION_PUSH, EXPAND_FILTER_DISJUNCTION_GLOBAL (3 passes) |
| Distinct expansion | AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN, AGGREGATE_EXPAND_DISTINCT_AGGREGATES |
| Dead code | PROJECT_REMOVE, UNION_REMOVE, AGGREGATE_REMOVE, PROJECT_JOIN_REMOVE |
| Window expansion | PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW, PROJECT_OVER_SUM_TO_SUM0 |

### Stage 2: CircuitOptimizer (75+ passes in sequence)

Key categories:

**Incremental computation (DBSP-specific):**
- **IncrementalizeVisitor**: Wraps inputs with `Integrate`, sinks with `Differentiate` — the core chain rule application
- **OptimizeIncrementalVisitor** (2 passes): For linear ops (Map, Filter, Negate, FlatMap, etc.), moves integrators through them (`I(f(x)) = f(I(x))`). For bilinear ops (Join, Sum, Subtract), moves integrators when all inputs have them
- **RemoveIAfterD**: Eliminates `Integrate(Differentiate(x))` pairs

**Distinct optimization:**
- `distinct(distinct) = distinct` (idempotence)
- `distinct(map(distinct)) = distinct(map)` (pull out inner distincts)
- `filter(distinct) = distinct(filter)` (reorder for filter efficiency)
- `join(distinct_a, distinct_b) = distinct(join)` (merge distincts across join)

**Operator fusion:**
- **FilterJoinVisitor** (2 passes): `filter(join)` → `JoinFilterMap`, `filter(left_join)` → `LeftJoinFilterMap`, `filter(star_join)` → `StarJoinFilterMap`
- **ChainVisitor** (2 passes): Fuses consecutive Map/MapIndex/Filter chains into a single `Chain` operator
- **CreateStarJoins**: Converts trees of joins into a single `StarJoin` operator
- **LinearPostprocessRetainKeys**: Fuses aggregate postprocessing with retain-keys operators

**Dead code and algebraic simplification:**
- **DeadCode** (6 passes), **Simplify** (4 passes with constant propagation + identity removal)
- **MergeSums**: `sum(sum(a,b),c) → sum(a,b,c)`, removes single-input sums
- **RemoveFilters** (constant TRUE/FALSE), **RemoveNoops**, **RemoveIdentityOperators**
- **PropagateEmptySources**: Propagates empty-relation knowledge early

**Projection optimization:**
- **UnusedFields** (iterative): Static analysis to find unused columns, inserts projection operators to drop them
- **OptimizeProjections** (3 passes): Merges projections with preceding operators, pushes column selections down

**CSE and graph-level:**
- **CSE** (3 passes): Operator-level common subexpression elimination — finds structurally identical sub-circuits, maps to single canonical instance
- **InnerCSE**: Expression-level CSE inside operator functions
- **CloneOperatorsWithFanout**: For cheap operators with fanout > 1 whose successors can merge with them, clone to reduce communication overhead

**Aggregation:**
- **ExpandAggregates**: Detects append-only sources and selects appropriate aggregate strategy; groups aggregates by linearity (SUM/COUNT/MIN/MAX = linear → delta rules; UDFs = non-linear → full state)
- **IndexedInputs**: Creates keyed inputs for aggregation

**Monotonicity:**
- **MonotoneAnalyzer**: Annotates which operators are monotone (output-only-grows), enabling further incremental optimizations and retain-keys optimizations

**Join balancing:**
- **BalancedJoins**: Marks joins that can use adaptive hash join (dynamic load balancing), disabled inside recursive components or with GC operators

**Other:**
- **PushDifferentialsUp**: Moves `Differentiate` before Map/MapIndex when fanout = 1 (last-minute optimization)
- **LowerAsof**: Implements ASOF join
- **ExpandHop**: Implements time-windowed aggregation

---

## Part II: Broader Optimization Research

### DBSP Theory Optimizations (Academic)

The foundational DBSP paper (Budiu et al., VLDB 2023) provides the algebraic basis for the optimizations Feldera implements, plus a few it doesn't fully exploit:

- **Chain rule**: `(Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ` for linear operators — directly gives modular, heuristic-free incrementalization
- **Bilinearity of join**: `δ(A ⋈ B) = (δA ⋈ B) + (A ⋈ δB) + (δA ⋈ δB)` — gnitz already implements this (delta-trace + delta-delta)
- **Arrangement reuse**: A trace (integrated relation) maintained for one join can serve multiple operators. Feldera implements this via CSE; gnitz does not yet

**DBToaster Higher-Order Deltas** (VLDB 2012): Recursively differentiates delta queries themselves. For `V = R ⋈ S ⋈ T`, maintains `V_ST(b) = Σ S(b,c)·T(c,a)` as a pre-aggregated auxiliary — reduces per-update cost from O(|S|×|T|) to O(1). Gnitz and Feldera both use first-order deltas only.

**F-IVM Factorized Representation**: For q-hierarchical queries, constant-time per-update maintenance. At RelationalAI, 76% of production queries are effectively q-hierarchical under functional dependencies.

### PostgreSQL Planner Techniques

Key techniques PostgreSQL uses that gnitz lacks:

| Technique | What it does |
|---|---|
| DP join enumeration | Bottom-up DP over all subsets of relations (up to 12 tables) |
| GEQO | Genetic algorithm for 12+ table queries |
| Semi/anti-join conversion | `IN`/`EXISTS` → semi-join, `NOT IN`/`NOT EXISTS` → anti-join; enables all join strategies |
| Left join elimination | Removes unnecessary LEFT JOINs when right side is unique and columns unused |
| Equivalence classes | Tracks `a=b=c` chains; generates implied join predicates; propagates sort order |
| Partition pruning | Eliminates irrelevant partitions at plan time (static) and execute time (dynamic) |
| JIT/LLVM | Native code for WHERE expressions, projections, aggregate accumulators, tuple deforming |
| Memoize | Caches inner-side results in nested-loop joins for repeated lookups with same outer key |
| Two-phase aggregation | Partial aggregate per partition + final aggregate after shuffle |
| Presorted aggregate | Stream-based aggregate when input is already sorted (skips sort step) |
| Incremental sort | Avoids full re-sort when input is partially pre-sorted on leading keys |
| CTE inlining | `WITH` clauses inlined when non-recursive and referenced once (PostgreSQL 12+) |
| Partitionwise join/agg | Processes each partition pair independently; enables natural parallelism |

### Flink Streaming-Specific Optimizations

| Technique | Description |
|---|---|
| MiniBatch aggregation | Buffers records, fires once buffer fills; turns N state lookups → 1 lookup for same key |
| Local-Global (two-phase) aggregation | Pre-aggregates before shuffle (local), final aggregation after (global); eliminates shuffle hotspots |
| Split DISTINCT | Adds bucket key `HASH_CODE(key) % N`, two-level aggregation to eliminate COUNT DISTINCT skew |
| MultiJoin | Processes multiple chained joins sharing a key simultaneously with zero intermediate state |
| Sub-plan reuse | Shares identical subexpressions across multiple sinks in the same job |

### ML/Learned Optimizations

**Cardinality estimation** (most impactful):
- **NeuroCard** (VLDB 2020): Autoregressive density model over full outer join; 8.5× better than histograms on JOB-Light
- **MSCN** (2018): Multi-set convolutional network; good baseline
- **PRICE** (arXiv 2024): Zero-shot cross-database transfer

**Plan selection** (without replacing optimizer):
- **Bao** (SIGMOD 2021): Bandit selection among PostgreSQL hint-steered plans; up to 70% latency reduction
- **Lero** (VLDB 2023): Pairwise plan ranker — learns "plan A beats plan B" (easier than predicting latency); up to 70% speedup; best practical result

**Streaming-specific cost models**:
- **Learned Cost Models for Streaming** (VLDB 2025): Extends learned cost modeling to streaming; encodes state sizes, watermarks, update rates — batch ML models ignore these entirely

---

## Part III: Gnitz vs. Feldera — Architectural Differences

### Gnitz Is a Database, Not a Stream Processor

This distinction matters throughout the optimization discussion. Feldera and Materialize are *stream processors*: their source tables are external streams (Kafka topics, CDC feeds from other databases). They do not own primary data. Gnitz is a *database*: it stores primary data directly in its own ZSetStore. When you `INSERT INTO t VALUES (...)`, gnitz owns and persists that data. The incremental view computation is gnitz's core value proposition, but the base tables are gnitz's own storage — not a passthrough from an external source.

Consequences:
- Table cardinality is fully observable. A table's row count is computable from shard metadata without any new infrastructure.
- Classical per-table statistics (column distributions, n_distinct, min/max) are applicable, not just delta batch sizes.
- Creating an index on existing data requires a backfill scan of stored rows — gnitz already does this for secondary indexes (`_backfill_index`), and the same mechanism applies to future maintenance.
- State eviction with on-demand reconstruction from base tables (Noria-style) is architecturally possible precisely because gnitz owns the base data. A pure stream processor cannot evict and reconstruct because it does not own the source.

### Two Query Paths

Gnitz has two distinct query paths that require separate optimization treatment:

1. **Circuit path (views)**: `CREATE VIEW` persists a circuit graph in 5 system tables. `compile_from_graph` compiles it to an instruction list. The executor runs the circuit per tick, driven by incoming delta batches. This is the DBSP incremental path — the main subject of this document.

2. **DML path (ad-hoc SQL)**: The Rust SQL client (`gnitz-sql`) handles `SELECT`, `INSERT`, `UPDATE`, `DELETE`. For SELECT, it attempts PK equality seek → secondary index seek → error. Non-indexed, non-PK WHERE clauses are rejected outright; users must `CREATE INDEX` first or use a `CREATE VIEW`. There is no optimizer in this path — access method selection is purely heuristic. UPDATE and DELETE fall back to full scan when no index applies, with client-side predicate evaluation.

The research in Parts IV–VI focuses on the circuit path. The DML path's access method selection and statistics needs are a separate, currently unaddressed problem.

### Execution Model

| Dimension | Feldera | Gnitz |
|---|---|---|
| SQL frontend | Apache Calcite (Java) | Custom Rust parser + binder (sqlparser crate) |
| Optimizer | 18-phase Calcite HepPlanner + 75+ circuit passes | None — direct SQL → circuit |
| DBSP execution | Compiled Rust (LLVM JIT via cargo) | RPython bytecode VM with PyPy JIT |
| Expression eval | Rust closures (compiled at circuit-gen time) | 31-opcode expression VM, JIT-unrolled |
| Operator granularity | Operator-per-row with Rust generics | SoA columnar batches (ArenaZSetBatch) |
| Storage | Embedded RocksDB-style | Custom ZSetStore (SoA columnar, sorted+consolidated) |
| Incremental model | Fully incremental (stream-in, stream-out) | Delta + trace per operator |
| Multi-worker | Thread-per-worker within a process (N threads share memory); multi-host via RPC (tarpc/TCP) | Multi-process with hash-partitioned exchange (in progress) |
| Join types implemented | All DBSP operators incl. ASOF | delta-trace, delta-delta, anti, semi-join |
| Current SQL coverage | Full SQL (windows, subqueries, recursion) | Select, filter, project, group-by, single equijoin, set ops |

### Key Structural Differences

1. **No logical plan layer**: Gnitz's planner directly builds circuit graphs from bound SQL. Feldera has a full relational algebra layer where most high-value optimizations happen. This is the biggest gap.

2. **Column-oriented batches**: Gnitz's SoA `ArenaZSetBatch` is architecturally different from Feldera's row-at-a-time Rust generics. Gnitz can do bulk memcpy per column — a hardware advantage Feldera lacks — but optimization passes need to be aware of the batch layout.

3. **RPython JIT semantics**: Gnitz's JIT specializes on `(pc, program)` as green variables, meaning the JIT can inline entire operator bodies across the VM loop. This is a very different JIT strategy from Feldera's LLVM. Gnitz's JIT is implicitly operator-fusing when the JIT traces through the main loop.

4. **Multi-worker architecture**: Feldera uses N threads within a single process (sharing memory); it also supports multi-host via RPC. Gnitz uses N separate OS processes communicating via IPC. Both have explicit hash-based shard/exchange operators with three partitioning policies in Feldera's case (Shard, Broadcast, Balance). Neither system currently implements two-phase (local-global) aggregation or Split DISTINCT — both shard first, then aggregate at the worker that owns the key. However, the optimization pressure differs: Feldera's intra-process shuffle is cheap (shared memory), while gnitz's inter-process IPC shuffle is expensive, so pre-aggregation before the shuffle is significantly more valuable for gnitz.

5. **System table-based circuit compilation**: Gnitz's `compile_from_graph` reads the circuit from persistent system tables at view creation time. Feldera compiles circuits to Rust source at SQL-compilation time. Gnitz's approach enables runtime circuit introspection and modification, but any optimization pass must operate on the serialized graph representation or at register-allocation time.

6. **Primary storage ownership**: Feldera's source tables are external streams; it does not own them. Gnitz owns its base tables in ZSetStore. This means gnitz can maintain authoritative table statistics, perform index backfills, support state eviction with reconstruction, and eventually expose classical query planning over base table data — capabilities a pure stream processor cannot have.

---

## Part IV: Optimization Roadmap for Gnitz

### Tier 1: High Value, Tractable Now

**1. Filter + Map fusion at circuit level** *(ChainVisitor / FilterJoinVisitor analog)*
- **Where**: `compile_from_graph` / new circuit-level graph pass
- **What**: When a MAP follows a FILTER on the same stream, fuse them into a single operator call. Eliminates one full pass over the batch per row.
- **Also**: `filter(join)` → combined `JoinFilterMap` that applies the predicate inside the join loop, avoiding intermediate batch allocation.

**2. Dead operator elimination** *(DeadCode pass)*
- **Where**: `compile_from_graph`
- **What**: The topological sort already exists; add a backwards reachability pass to mark operators with no downstream sinks as dead and skip emitting instructions for them.

**3. Unused column pruning** *(UnusedFields)*
- **Where**: Rust SQL planner (`planner.rs`) or `compile_from_graph`
- **What**: Track which columns of each relation are actually consumed downstream. Insert projection operators to drop unused columns before expensive operators (joins, reduces). Reduces both memory bandwidth and join/reduce state size.
- **Highest value for**: Joins where only 2-3 columns of a wide table are needed.

**4. Constant folding in expression programs** *(ReduceExpressionsRule)*
- **Where**: `ExprBuilder` in `gnitz-sql/src/expr.rs` and/or expression VM compiler
- **What**: When both operands of a binary expression are constants, evaluate at compile time. Collapse `IS NULL` on a NOT NULL column to `FALSE`. Fold `x + 0`, `x * 1`, `x AND TRUE`, `x OR FALSE`.
- **Current state**: `ExprBuilder` generates opcodes mechanically; no folding.

**5. Distinct idempotence and filter reordering** *(OptimizeDistinctVisitor)*
- **Where**: `compile_from_graph`
- **What**: `DISTINCT(DISTINCT(x)) = DISTINCT(x)`. `FILTER(DISTINCT(x)) = DISTINCT(FILTER(x))` — move filter before distinct to reduce the rows entering the distinct state table.

**6. Predicate pushdown into joins** *(FILTER_INTO_JOIN)*
- **Where**: Rust SQL planner, or new circuit pass
- **What**: If a filter immediately follows a join and the filter predicate only references one side's columns, push it before the join. Reduces rows entering the join operator.

**7. Efficient bulk routing in exchange** *(gnitz-specific)*
- **Where**: `exchange.py` — `repartition_batch`
- **What**: `repartition_batch` currently routes row-by-row via `_direct_append_row`, which bypasses bulk column copy. Refactor to accumulate row indices per destination worker, then use `append_batch` (memcpy per column) to fill each sub-batch. This is the most direct win for exchange throughput.
- **Note**: This is distinct from two-phase aggregation and more universally applicable (every shuffled operator benefits, not just aggregates).

**8. MergeSums / identity union elimination** *(MergeSums)*
- **Where**: `compile_from_graph`
- **What**: A UNION node with only one input is an identity — eliminate it. Consecutive UNIONs (e.g., multiple UNION ALL branches) can be collapsed to a single N-ary union instruction.

### Tier 2: Medium Term

**9. Two-phase (local-global) aggregation for exchange** *(Flink Local-Global)*
- **Where**: `compile_from_graph` / instruction emission, in coordination with exchange operator
- **What**: When a REDUCE follows an EXCHANGE_SHARD, add a partial REDUCE before the shard (each worker pre-aggregates its local partition) and a final REDUCE after the gather. Exact for SUM/COUNT/MIN/MAX.
- **Nuance**: In a DBSP system, what flows through the exchange is already a *delta batch*, not a full relation. The reduce operator processes `Agg(history + δ) - Agg(history)` — so the shuffle volume is bounded by the size of changes per tick, not by table size. This makes the Flink argument (pre-aggregation reduces N events → K keys) less extreme here: gnitz's gain is a constant factor (collapsing duplicate group keys within a delta batch before the IPC hop), not orders of magnitude. Also, gnitz's `ConsolidatedScope` already coalesces duplicate group keys within a single batch before processing — giving implicit MiniBatch semantics. Two-phase aggregation is still worth doing to reduce IPC data, but should be viewed as a quality-of-life improvement for multi-worker rather than a critical correctness concern.

**10. Trace/arrangement sharing** *(CSE at circuit level + Differential Dataflow Arrangements)*
- **Where**: `compile_from_graph` register allocation
- **What**: If multiple views join against the same base table on the same key, they each currently maintain their own integrated trace register. Share the trace across views — one `INTEGRATE` populates the shared trace, multiple `JOIN_DELTA_TRACE` instructions read from it.
- **Impact**: Memory usage reduction proportional to the number of views sharing a base table trace.
- **Gnitz consideration**: Register allocation currently assigns one trace register per node. Sharing requires cross-view register allocation — a significant change to `compile_from_graph`.

**11. Append-only source detection for aggregates** *(ExpandAggregates append-only path)*
- **Where**: `compile_from_graph` or executor
- **What**: If a source table only ever receives INSERTs, the delta stream has no negative weights. For SUM/COUNT, the aggregate only increases — no retraction needed. Enable a simpler, faster aggregate code path that skips the `old_weight` history lookup.
- **Gnitz advantage**: Because gnitz owns its base tables, it can track this property authoritatively (has any DELETE or weight-negating batch ever been applied to this table?), rather than relying on external stream connector hints as Feldera must.

**12. Group-key hash sharing between REDUCE and EXCHANGE**
- **Where**: `compile_from_graph` + `exchange.py`
- **What**: Gnitz already uses the same Murmur3 hash for both REDUCE group keys and EXCHANGE_SHARD routing. Compute the hash once and pass it through, rather than recomputing in each operator.

**13. Semi/anti-join for correlated IN/EXISTS**
- **Where**: Rust SQL planner
- **What**: Gnitz already routes INTERSECT/EXCEPT to semi/anti-join operators. Extend to handle correlated `IN`/`NOT IN`/`EXISTS`/`NOT EXISTS` subquery patterns — currently unsupported.

**14. CTE and subquery flattening**
- **Where**: Rust SQL planner / binder
- **What**: Gnitz already inlines non-recursive CTEs as table aliases. Extend to inline scalar subqueries in SELECT lists and single-table subqueries in FROM as view aliases, so more optimization passes can see the full query shape.

**15. Join order heuristics**
- **Where**: Rust SQL planner — needs a proper logical plan layer first
- **What**: When gnitz supports multiple joins, apply simple join-ordering heuristics (smallest estimated relation first, filter-first). Start with heuristics before full DP enumeration. Bushy trees reduce intermediate state depth compared to left-deep.
- **Gnitz advantage**: Unlike pure stream processors, gnitz knows the current size of each base table (computable from shard metadata). This makes cost-based join ordering more tractable: the planner can use actual row counts rather than guessing from schema alone.
- **Independence assumption caveat**: Ordering by `n_distinct(join_key)` alone still assumes that column distributions are independent across tables. For correlated data (e.g., `orders.region` and `customers.region` when `orders` has a FK to `customers`), this assumption fails badly. **Theta sketch intersection** (K-minwise hashing, equivalent to OmniSketch's mechanism — see `research/sketches.md` §Opportunity 7 and §Broader Landscape) directly measures `|L.join_key ∩ R.join_key|` without the independence assumption and is the correct cost estimate for join output size. Build Theta sketch sidecars alongside ExaLogLog sidecars at shard flush time; use their intersection estimate as the primary join cardinality signal.
- **Prerequisite**: The `LogicalPlan` layer (currently declared but unused) needs to be activated.

**16. View backfill on creation**
- **Where**: `ViewEffectHook` in `catalog/hooks.py`, executor
- **What**: When `CREATE VIEW` fires against a table that already has rows, the view's EphemeralTable is empty. Currently the view is only populated when new delta batches arrive. For a database with existing data, a newly created view should be initialized from stored base table data. The mechanism already exists for secondary indexes (`_backfill_index`); the same pattern needs to apply to views.
- **Gnitz-specific**: This problem does not exist for pure stream processors, which start from empty state and fill via incoming streams. It exists for gnitz precisely because gnitz is a database with persistent primary storage.

### Tier 3: Longer Term / Research

**17. Split DISTINCT for exchange skew** *(Flink Split Distinct)*
- **Where**: Circuit compilation + exchange
- **What**: For COUNT DISTINCT in a multi-worker setup, add a bucket dimension (`hash(distinct_key) % B`) to the group key, then two-level reduce. Eliminates hot-partition skew on distinct aggregations.
- **Nuance**: Like two-phase aggregation, the DBSP delta model reduces the severity of this problem compared to Flink. Skew only matters if one group key receives a disproportionate share of *delta rows* per tick. Still worth implementing for correctness under adversarial workloads.

**18. MultiJoin operator** *(Flink MultiJoin)*
- **Where**: New opcode + circuit compiler
- **What**: For chains of joins sharing a common key, process them all simultaneously with zero intermediate state. Gnitz's delta-trace semantics may enable a natural implementation since the trace is the integrated base table.

**19. Higher-order delta queries** *(DBToaster-style)*
- **Where**: Circuit compilation
- **What**: For queries with multi-way joins updated at high frequency, pre-aggregate sub-joins as auxiliary views. Reduces per-update cost from O(|table|) to O(1) for some patterns. Requires materialized auxiliary views tracked in the system tables.

**20. State eviction / partial materialization** *(Noria-style)*
- **Where**: ZSetStore / PartitionedTable
- **What**: Evict cold state (infrequently queried keys) from memory, reconstruct on-demand from base tables. Gnitz's PartitionedTable architecture (256 independent partitions) is a natural base for this — each partition can be evicted and reloaded independently. This is only viable because gnitz owns primary storage; a stream processor with an external source cannot guarantee reconstruction.
- **Eviction policy**: The correct eviction algorithm is **S3-FIFO** (SOSP 2023) with ghost queues. Plain LRU thrashes when the working set slightly exceeds capacity — the ghost queue prevents immediately re-evicting a partition that was just reconstructed. The same S3-FIFO structure also governs the `ShardHandle` pool (which shard mmaps to keep open). The Count-Min Sketch-based frequency oracle used for HTAP-style flush scheduling (see `research/sketches.md`) provides the frequency signal that S3-FIFO needs to promote hot-but-evicted partitions directly to the main queue on reconstruction. See `research/sketches.md` §S3-FIFO for implementation detail.

**21. Learned plan selection** *(Bao-style hint bandit — see Part V Phase C)*
- **Where**: `compile_from_graph` strategy knobs, once multiple plan alternatives exist
- **What**: Thompson-sampling bandit over a small set of optimization hint sets (e.g., {default}, {push-filter-before-join}, {two-phase-agg-enabled}, {trace-sharing-enabled}). Each hint set is a bundle of compile_from_graph flags. Reward = observed tick processing time for the view. Bounded worst case = classical plan. Zero cold start (explores all variants). Implementable in pure RPython. Requires the statistics layer (Phase A) and adaptive feedback (Phase B) first. See Part V for the full three-phase proposal.

**22. Delta joins** *(Materialize pattern — see Part VI §3.5)*
- **Where**: `compile_from_graph` / circuit planner, requires arrangement sharing first
- **What**: For an N-way join `A ⋈ B ⋈ C`, build N delta pipelines instead of sequential binary joins. Each pipeline joins `δInput_i` against pre-built traces of all other inputs. Zero intermediate materialized state. Materialize's TPC-H Q3 benchmark: 4,173,794 maintained records with binary joins → 23,240 with delta joins. Requires Tier 2 item #10 (trace sharing) as a prerequisite.

**23. Late materialization for wide-table joins** *(Materialize pattern — see Part VI §5.7)*
- **Where**: `compile_from_graph` join planner
- **What**: For wide tables, store only `(join_key, pk)` in the join-side trace rather than the full row. After the join produces matching key pairs, do a final point-lookup on the primary trace to fetch the remaining columns. Reduces trace memory by `|join_key + PK| / |full_row_width|` — significant for tables with many payload columns.

**24. Zone maps / per-shard column statistics** *(filter pushdown to storage)*
- **Where**: `gnitz/storage/index.py` → `ShardHandle`; `shard_table.py`; cursor construction in `op_scan_trace`
- **What**: Store per-shard `(col_idx, min_val, max_val)` in shard metadata. When the circuit runs `op_scan_trace` followed by `op_filter`, push the filter predicate into cursor construction and skip shards whose column range cannot satisfy it.
- **Shard ordering and PK type**: Shard files are sorted by the actual user-supplied PK value. For U64 PKs, this is the raw integer; for U128 PKs, gnitz stores the full 128-bit value in (pk_lo, pk_hi) components and sorts by the same numeric comparison. Zone map effectiveness for non-PK columns depends entirely on how correlated the PK ordering is with those columns:
  - **U64 auto-increment PKs**: strong temporal correlation — rows inserted together share a shard, so temporal and event columns have tight per-shard ranges. Zone maps effective.
  - **U128 UUIDv7 PKs**: UUIDv7 encodes a 48-bit millisecond timestamp in its most-significant bits, making it approximately time-ordered. Shards sorted by UUIDv7 value are therefore approximately sorted by insertion time. Any column correlated with time (created_at, event_time, status progressions) will have tight per-shard ranges. Zone maps are highly effective — comparable to a time-series columnar store. Additionally, time-range queries expressed as UUIDv7 ranges (`WHERE id BETWEEN uuid_at(T1) AND uuid_at(T2)`) already get PK-range shard pruning for free from the existing `ShardIndex` machinery, with no zone map infrastructure needed. Zone maps would extend this to separate timestamp columns and non-temporal correlated columns.
  - **U64 arbitrary / U128 random PKs**: no meaningful correlation with other columns; zone maps would rarely prune shards.
- **Partition routing does not affect intra-partition ordering**: The PartitionedTable routes rows via `hash(pk_lo, pk_hi) & 0xFF`. For UUIDv7, the random suffix bits ensure uniform partition distribution even though the PK is time-ordered. Within each partition, the UUIDv7 ordering is preserved and shards remain approximately time-ordered. There is no temporal locality across partitions, but per-partition ordering is sufficient for zone maps.
- **REDUCE output exception**: Views produced by `op_reduce` use a synthetic Murmur3 hash as their PK (not the raw group key value). For these tables, non-PK zone maps face even weaker pruning since the row ordering is hash-driven with no natural column correlation.
- **Key prerequisite**: Zone maps are only actionable if the filter predicate can be inspected before the shard is opened. Currently `op_filter` operates on rows returned by cursors — the predicate is invisible to the storage layer. Zone maps require pushing an ExprProgram or at minimum a column-range hint down into `create_cursor()`. Without this predicate-pushdown step, zone maps can be stored but never consulted.
- **Compared to XOR8**: The existing XOR8 filter already does PK-level membership pruning (not range pruning, but false-negative-free membership). Zone maps extend this to arbitrary column range predicates, but with a meaningfully weaker guarantee and more infrastructure required.
- **Predicting zone map effectiveness**: ExaLogLog sketches per shard (see `research/sketches.md` §Opportunity 1) provide `shard_n_distinct(col)`. The ratio `shard_n_distinct / global_n_distinct` reveals column-PK correlation: a low ratio means the shard contains a tight slice of the column's value domain, making range pruning effective. A ratio near 1.0 means the shard's values are spread across the full domain — zone maps won't prune it. This signal should gate whether zone map infrastructure is built for a given column at all.

**25. Approximate COUNT(DISTINCT col) aggregate** *(ExaLogLog-backed)*
- **Where**: `gnitz/dbsp/functions.py` (new `HllAggregate` class); `compile_from_graph` aggregate dispatch
- **What**: `APPROX_COUNT_DISTINCT(col)` aggregate backed by ExaLogLog. Reduces state from O(|distinct values|) per group to O(730 bytes) per group — significant for high-cardinality distinct-count views where the exact DISTINCT + COUNT chain materialises the full set in an EphemeralTable trace.
- **Constraint**: Restricted to append-only source tables initially (item #11 detects this property). ExaLogLog has no deletion operation; retractions from mutable tables would accumulate error. On append-only tables the estimate is correct by construction.
- **Reuses**: The same ExaLogLog implementation built for shard statistics (§Phase A above). No new algorithm to implement.

**26. Approximate quantile aggregates** *(KLL-backed)*
- **Where**: `gnitz/dbsp/functions.py` (new `KllAggregate` class); SQL planner aggregate binding
- **What**: `APPROX_PERCENTILE(col, 0.95)`, `APPROX_MEDIAN(col)`. KLL sketch per group in the reduce operator's trace. Same space constraints as ExaLogLog-backed COUNT DISTINCT — O(100 items × 8 bytes) per group regardless of group size.
- **Constraint**: Append-only source tables only until KLL± (2024/2025 research) matures. On mutable tables, retractions cannot be removed from the KLL compactor hierarchy.
- **Reuses**: The same KLL implementation built for shard quantile statistics. Per-column KLL at shard flush time (Phase A) and per-group KLL as an aggregate function are the same algorithm with different key contexts.

**27. IBLT-based exchange consistency verification** *(correctness, not performance)*
- **Where**: `gnitz/server/ipc.py` — exchange batch send/receive protocol; `gnitz/server/executor.py`
- **What**: Attach an Invertible Bloom Lookup Table (IBLT) as a verification trailer to each exchange batch. The sender builds an IBLT over the batch's PKs and weights; the receiver rebuilds from ingested rows; subtracting the two and peeling reveals any rows lost in IPC transit. Communication overhead: O(expected_loss_count × 28 bytes) — effectively zero for correct implementations, proportional to errors only when errors occur.
- **Why this is correctness-critical**: Silent row loss in `op_exchange_shard` causes view divergence with no observable error signal — the DBSP circuit continues computing on incomplete delta batches and produces wrong view outputs indefinitely. IBLTs are the only sub-linear mechanism that can detect this without re-sending all data.
- **Secondary uses**: WAL replay completeness verification (build IBLT over WAL transactions, compare against recovered MemTable), compaction correctness (IBLT over source shard PKs vs. result shard PKs), cross-partition routing invariant (XOR of all partition IBLTs should be empty). See `research/sketches.md` §IBLTs for full analysis and implementation shape.

---

## Part V: Timely Dataflow, Differential Dataflow, and Materialize

These systems share DBSP's core model (incremental computation over Z-set-like weighted collections) but differ meaningfully in architecture. Understanding the differences clarifies which techniques transfer to gnitz and which do not.

---

### 1. Timely Dataflow

**Naiad** (Murray et al., SOSP 2013), rewritten in Rust as Timely Dataflow, solves the tension prior streaming systems could not: expressiveness (cyclic graphs, iteration) versus low latency (pipelined, no global barriers). Flink uses periodic Chandy-Lamport barrier injection; Spark Streaming uses synchronous mini-batches. Both impose global synchronization at epoch boundaries. Timely eliminates this.

**The partial-order timestamp model.** Timely's central innovation is that timestamps need not be totally ordered. For flat streaming: timestamps are `u64` — equivalent to DBSP's tick number. For iterative computation: timestamps are `Product<G, T>` — pairs with componentwise order, where `(g1, t1) ≤ (g2, t2)` iff `g1 ≤ g2 AND t1 ≤ t2`. This means `(4, 10)` and `(5, 0)` are **incomparable** — epoch 4's iteration 10 and epoch 5's iteration 0 can proceed concurrently. A single-integer clock requires full serialization between them.

What this enables over DBSP's single clock:
1. **Concurrent outer epochs with inner iteration**: Epoch N+1's input processing can begin while epoch N's fixed-point iteration is still converging.
2. **Correct incremental joins under nested iteration**: Joins at different `(epoch, iteration)` coordinates produce output at the least-upper-bound timestamp without spurious recomputation.
3. **Distributed execution without a global barrier**: Each worker advances independently; coordination happens only through the progress protocol.

**The capability system.** An operator can only emit a message at timestamp `t'` if it holds a capability for some `t` from which `t'` is reachable. When an operator is done with timestamp `t`, it drops its capability. The runtime tracks capability counts and in-flight message counts globally; when both reach zero for a timestamp, that timestamp is closed. This is the distributed, asynchronous analog of DBSP's `step()` return — but without a central driver.

**Workers are SPMD threads**, not separate processes. Every worker runs identical dataflow construction code. Within a process, workers communicate via shared memory; across processes, via TCP. Data is partitioned between workers by exchange operators keyed on a hash function — same-key records always land on the same worker.

**For gnitz**: Timely's partial-order timestamps are only needed for (a) distributed async multi-process execution and (b) pipelined recursion where outer epochs and inner iterations must interleave. For gnitz's current synchronous barrier-per-tick model, a single integer tick is sufficient. The capability system becomes relevant only when gnitz's multi-process workers need to advance independently without a master barrier.

---

### 2. Differential Dataflow

DD (McSherry, CIDR 2013) is a higher-level incremental relational layer built on Timely. Where Timely is a low-level dataflow substrate, DD provides the operators: `map`, `filter`, `join`, `reduce`, `distinct`, `iterate`.

**The data model.** DD's fundamental unit is a `(key, value, time, diff)` quadruple. A collection at time `t` is the set of `(key, value)` pairs whose weight-sum over all `s ≤ t` is non-zero. This is **the same algebraic structure as DBSP's Z-sets** — integer-weighted multisets. The key formal difference: DD embeds the time dimension in every data record (4-dimensional), while DBSP's Z-sets exist at an implicit logical time (3-dimensional: key, value, weight), with the time dimension expressed through the stream abstraction. Mathematically equivalent; computationally different implications.

**Arrangements: the central efficiency mechanism.** An arrangement is DD's equivalent of a database index. It transforms a raw update stream into pre-indexed, reference-counted, immutable batches with shared historical access.

An arrangement consists of:
1. **A stream of immutable batches** `Stream<Rc<Batch<K, V, T, R>>>`. Each batch is a sorted, indexed collection of `(K, V, T, diff)` records covering a contiguous time interval.
2. **A trace**: a `Spine<B>` — a hierarchy of logarithmic-sized batches that supports efficient merging using the same geometric merge policy as LSM compaction: merge adjacent levels of similar size.
3. **Shared ownership**: batches are reference-counted. Multiple operators hold references to the same batch. A `TraceReader` holds a "since" capability that prevents compaction beyond its interest frontier.

Why arrangements dominate DD's performance: in a naive implementation, five queries that all join against the `edges` table maintain five separate private hash maps. With arrangements, one `arrange_by_key()` call builds the index once; all five queries hold a reference to the same `Arranged<G, T>` handle. From McSherry: "Reusing arranged data eliminates redundant shuffling, index maintenance computations, and in-memory index copies."

**DD's trace vs. DBSP's integral.** DBSP's `I(s)[t] = Σ_{i≤t} s[i]` — a running sum. It maintains the current Z-set; there is no historical query access. DD's `Spine<B>` retains the full history of batches back to its compaction frontier and can answer "what was the state of this collection at any past timestamp?" This extra power is only needed for partial-order timestamps (iterative computation). For a totally-ordered single clock — gnitz's model — the two are equivalent for all operator semantics. The DD trace is like a bitemporal index; DBSP's integral is a running total.

**Iterative computation in DD.** `iterate` creates a fixed-point loop using Timely feedback. A `Variable` feeds back into its own definition; the iteration timestamp is `Product<OuterTime, u32>`. Convergence is detected when the Timely frontier for the iteration scope closes (no in-flight messages, no held capabilities). Non-trivial: users must manually insert `consolidate()` to cancel `+1`/`-1` pairs for the same record, preventing infinite cycling.

**Total-order specializations.** DD provides `count_total()`, `distinct_total()`, and related operators specialized for totally-ordered timestamps. These are substantially faster than the general partial-order variants because they can use simpler data structures. Since gnitz's tick model is always totally ordered, it can always use the equivalent of these specialized operators.

---

### 3. Materialize

Materialize is a commercial SQL streaming database built on Timely + DD. SQL is compiled through a multi-stage IR pipeline (`HirRelationExpr` → `MirRelationExpr` → `LirRelationExpr`) to a Timely dataflow program. The architecture separates `environmentd` (control plane: SQL parsing, optimization, catalog, timestamp coordination) from `clusterd` (data plane: Timely workers). Data persists via a durable S3-backed layer (Persist) between the two.

#### Delta Joins

The most important Materialize innovation. For an N-way join `A ⋈ B ⋈ C`, standard sequential binary joins compile to `(A ⋈ B) ⋈ C` and must maintain `A ⋈ B` as an intermediate materialized collection. For large inputs, this dominates memory usage.

Materialize's delta join: build **N delta pipelines**, one per input. The pipeline for input `Ai` joins `δAi` against pre-built arrangements of all other inputs:

```
δ_out = (δA ⋈ arr(B) ⋈ arr(C))
      + (arr(A) ⋈ δB ⋈ arr(C))
      + (arr(A) ⋈ arr(B) ⋈ δC)
```

(Cross-delta terms `δA ⋈ δB` are typically zero — inputs rarely change simultaneously at the same tick.) The outputs are unioned. Result: **zero intermediate materialized state**. The only maintained state is the input arrangements.

TPC-H Q3 (Materialize blog): 4,173,794 maintained records with binary joins → 23,240 with delta joins. The 23,240 ≈ 2 × 11,620 output records — confirming zero intermediate state.

Prerequisite: delta joins only pay off when arrangements are shared across pipelines. Without sharing, each pipeline builds its own copy of `arr(B)`, costing more than the binary join approach. Delta joins require arrangement sharing as a foundation.

#### Arrangement Reuse / Cross-Query Sharing

If an arrangement for `(table, key_columns)` already exists in the system — either from a `CREATE INDEX` statement or built by a prior query — new operators that need the same arrangement receive a reference to the existing one rather than creating a new copy. This is cross-query sharing: not just within one plan, but across all concurrently-maintained views.

The memory reduction is direct: K views that all join against table T on the same key require O(|T|) memory instead of O(K × |T|).

#### Join Ordering

Materialize's join ordering at the MIR level is guided by **available arrangements** (indexes). Since arrangement reuse is the dominant cost factor, the optimizer prefers join orders that reuse existing arrangements over those that minimize intermediate result size. This is a different cost model from PostgreSQL's page-count-based model and reflects the incremental system's economics: arrangement maintenance cost is amortized across all ticks; plan compilation is once.

Materialize exposes query hints (`AGGREGATE INPUT GROUP SIZE`, `DISTINCT ON INPUT GROUP SIZE`) to guide hierarchical aggregation planning when groups are large.

#### Hierarchical Aggregation

A standard `Reduce` aggregates all records with the same key in one flat pass. For groups with O(N) records, each tick's delta update requires re-scanning up to N history records (to compute `Agg(history + δ) - Agg(history)`). Materialize's hierarchical aggregation builds a **tree-shaped reduce**: pre-aggregate subsets of records at intermediate levels, reducing the per-update cost to O(log N) for commutative aggregates (SUM, COUNT, MIN, MAX).

Gnitz's `op_reduce` (reduce.py) does a flat reduce. For workloads with large groups, hierarchical aggregation would be a meaningful improvement — at the cost of more complex circuit structure and additional intermediate state.

#### Consistency Model

Materialize provides strict serializability without two-phase locking. `environmentd` assigns a logical timestamp `t` to each query; the `clusterd` workers must advance their Timely frontiers to `t` before the query is answered. The trade-off: if one input source is slow (e.g., a low-frequency CDC stream), queries joining against it must wait.

#### Late Materialization

For wide tables, Materialize creates **narrow arrangements** containing only `(join_key, primary_key)` instead of the full row. The join produces matching key pairs; a final lookup on the primary arrangement fetches full rows. This reduces arrangement memory by the ratio `|join_key + PK| / |full_row_width|`.

---

### 4. Key Differences: Timely/DD vs. DBSP/Gnitz

| Concept | Timely/DD | DBSP/Gnitz |
|---|---|---|
| Timestamp type | Any `PartialOrd` lattice; `Product<G,T>` for nested loops | Single integer tick (total order) |
| Progress tracking | Distributed capability/frontier protocol (async gossip) | External `step()` call (synchronous) |
| Data model | `(key, val, time, diff)` 4-tuples; time explicit in record | Z-sets at implicit tick; (key, val, weight) triples |
| Trace | `Spine<Batch>`: time-indexed history, queryable at any frontier | `integrate` stream: running sum, no explicit history |
| Arrangement | First-class, reference-counted, cross-query shared | Per-circuit trace register; no first-class sharing |
| Multi-worker | SPMD threads; exchange for partitioning | Multi-process (planned); partitioned storage |
| Iterative computation | `Product<G,T>` timestamps; epoch and iteration concurrent | Fixed-point circuit; sequential per tick |
| Formal foundation | Operational (bottom-up engineering) | Algebraic (circuit-over-groups, formal proofs) |
| Join strategy | Delta joins (zero intermediate state) | Sequential binary joins (intermediate Z-sets) |
| State compaction | Spine geometric merge (in-memory LSM for operator state) | ZSetStore LSM (disk-oriented) |

**The DD trace's extra power (partial-order timestamps, historical queries) is only needed for iterative computation.** For gnitz's current use case — non-recursive views on a single clock — DBSP's integral is equivalent and simpler. The DD/Timely machinery adds complexity whose payoff only materializes for recursive Datalog-style queries.

**Where Timely/DD is clearly ahead**: arrangement sharing and delta joins. These are architectural features that fall out naturally from DD's data model (shared reference-counted batches) and do not strictly require partial-order timestamps. They are directly applicable to gnitz.

---

### 5. What Gnitz Should Learn: Ranked by Impact

**1. Arrangement sharing (global trace registry)** — the highest-value DD technique for gnitz. See Tier 2 item #10 in Part IV. The implementation: a registry keyed by `(table_id, key_columns)`. When compiling a new view, check if a trace for the required key already exists; if so, share it. Reference-count the trace handle; free when no operators reference it. Memory reduction: O(K × |T|) → O(|T|) for K views sharing table T.

**2. Delta joins** — requires arrangement sharing as a prerequisite. Tier 3 item #22 in Part IV. Directly expressible in DBSP algebra: emit N delta pipelines per N-way join, each joining `δInput_i` against shared traces of all other inputs. The DBSP formula `δ(A ⋈ B ⋈ C)` expands naturally to the three-pipeline form. Zero intermediate materialized state.

**3. Barrier-per-tick progress protocol** — for gnitz's planned multi-process workers. Timely's full capability system is overkill for gnitz's current architecture (where the master drives ticks synchronously). The minimal viable approach: master signals "tick t begin," workers process their partition's delta batch, workers signal "tick t done," master collects all done signals and advances to `t+1`. This is the Spark mini-batch model applied to DBSP.

**4. Total-order specializations** — since gnitz's clock is always totally ordered, operator implementations should exploit this. The reduce operator can use simpler monotone-safe data structures; the join trace can avoid partial-order machinery entirely. This is the equivalent of DD's `*_total()` operator variants.

**5. Hierarchical aggregation** (Tier 3) — for views with large groups. Particularly valuable for long-running analytics views where the same group accumulates many records over time. Materialize's hints-based approach (user specifies expected group size) is pragmatic; gnitz's statistics layer (Phase A in Part V) could eventually detect large groups automatically.

**6. Late materialization** (Tier 3 item #23 in Part IV) — for wide tables. Join traces hold only `(join_key, pk)`, final point-lookup fetches remaining columns.

**7. Zone maps** (Tier 3 item #24 in Part IV) — per-shard min/max column statistics for predicate-based shard skipping, contingent on predicate pushdown to the storage layer.

---

## Part VI: Learned and Adaptive Optimizations

### Research Landscape

The field divides cleanly into three maturity tiers.

**Production-proven (shipped and helping real users):**
- **SQL Server Intelligent Query Processing (IQP)**: Adaptive join switching, memory grant feedback, CE feedback, parameter-sensitive plan optimization. None of these use neural networks — they are runtime feedback loops. In production since SQL Server 2017/2019/2022. These are the most battle-tested adaptive techniques in existence.
- **Oracle Adaptive Plans**: Defers hash-join vs. nested-loop choice to runtime using a statistics collector node. Switches mid-execution if actual row counts diverge from estimates. Shipped since Oracle 12c (2013).
- **Amazon Redshift Stage**: Per-cluster XGBoost ensemble predicts query execution time for workload management (queue placement, memory grants, concurrency scaling). 20.3% latency reduction vs. prior heuristic predictor. The global GNN model is still under evaluation due to regressions. "Better data beats bigger data" — per-database specialization beats larger training sets.
- **Extended statistics** (PostgreSQL `CREATE STATISTICS`, SQL Server column group stats): Multi-column correlation capture using classical methods. Shipped in PostgreSQL 10 (2017). Closes a large fraction of the independence-assumption gap with zero ML infrastructure.
- **XOR8 filters** (Graf & Lemire): Gnitz already uses these. Correct choice — 20-40% less space and faster than Bloom filters for static membership testing.

**Academic but mature enough to evaluate:**
- **BayesCard**: Bayesian network cardinality estimator. Demonstrated 13.3% end-to-end improvement in PostgreSQL. Strong incremental update support (structure is stable; only parameters need updating). Closest to production-ready of any neural CE approach.
- **FactorJoin** (SIGMOD 2023): Factors estimation into per-table learned models + histogram-style join structure. 40x lower latency and 100x smaller than prior learned SOTA. Most practical learned CE for large schemas.
- **PRICE** (VLDB 2025): Transformer pretrained on 30 databases. Zero-shot cardinality estimation. Fine-tuning with 500 labeled examples matches database-specific training. Genuine cross-database transfer.
- **Bao** (SIGMOD 2021, Best Paper): Thompson-sampling bandit over query hint sets. Preserves classical optimizer structure; ML only selects among PostgreSQL-guided plan variants. Bounded worst case (worst plan = original optimizer plan). Converges in ~100-200 queries. Reported ~2x speedup on JOB. Closest to production-deployable of any learned join ordering approach.
- **Lero** (VLDB 2023): Pairwise plan ranker ("which of these two plans is faster?"). Easier than absolute latency prediction. Up to 70% execution time reduction vs. PostgreSQL.
- **ALECE** (VLDB 2024): Attention-based CE for dynamic workloads. Data-encoder aggregations update on data changes without full retraining — most incremental-friendly CE model.
- **PGM-Index** (VLDB 2020): Piecewise linear positional index. Fully dynamic (insertions/deletions via delta buffers). 1140x less space than B-tree at comparable lookup performance. Provable O(log ε) bounds. Mature open-source C++ library.
- **ALEX** (SIGMOD 2020, Microsoft Research): Updatable adaptive learned index. Up to 4.1x throughput vs. B+-tree on read-heavy workloads.

**Academic only / not ready:**
- End-to-end neural optimizers (Balsa, Neo, LOGER, GLO, LLM-QO): The VLDB 2024 paper "Is Your Learned Query Optimizer Behaving As You Expect?" (Lehmann et al.) shows PostgreSQL outperforms all tested learned optimizers under rigorous train/test splits. Overfitting to training query templates is a critical unsolved problem.
- Neural cardinality estimators (NeuroCard, FLAT, UAE, FACE): No production deployment in any database. Training cost + full-retraining requirement on data changes + overfitting are blockers.

**The honest bottom line (Leis, VLDB 2025 "Still Asking"):** Cardinality estimation errors dominate bad plans by a large margin over cost model errors or join order enumeration strategy. The simplest, most robust improvement is better statistics — extended statistics, adaptive histogram buckets, or lightweight incremental summaries — not neural networks.

---

### Learned Filters: Gnitz Already Made the Right Choice

XOR8 (already in `gnitz/storage/xor8.py`) is the correct choice for per-shard membership filtering. A 2025 analysis ("How to Train Your Filter: Should You Learn, Stack or Adapt?", arXiv 2602.13484) shows that learned Bloom filters are up to 10,000x slower in query throughput than adaptive/XOR filters due to model inference overhead — a cost "systematically overlooked in prior evaluations." Learned filters win only in extremely space-constrained settings with stable, predictable query distributions.

For gnitz's incremental delta model specifically: Z-Set shard files are write-once (immutable after flush), so static XOR8 filters are structurally optimal. No need to revisit this.

---

### Statistics in Gnitz: Two Layers Are Needed

Gnitz is a database with primary storage, not a pure stream processor. This means two distinct kinds of statistics are both applicable and necessary.

**Layer 1: Static base-table statistics.** Because gnitz owns its base tables, classical per-table statistics are just as relevant as in PostgreSQL. Every change flows through `ingest_batch` on the ZSetStore — gnitz has complete visibility into its own data. Some statistics are already available without new infrastructure: approximate row counts can be computed by summing `row_count` fields from shard file headers across all ShardHandle entries in the ShardIndex. Building per-column histograms, n_distinct estimates, and min/max ranges on top of this is a straightforward extension at ingest time.

These static statistics inform the circuit compilation path: join-side selection (which table's trace to integrate), filter selectivity estimates for predicate pushdown decisions, and join order heuristics. They also inform the DML path (if predicate metadata is ever sent to the server side).

**Layer 2: Dynamic delta statistics.** The DBSP circuit operates on delta batches, not full table scans. The operationally relevant quantity for adaptive circuit optimization is the expected size of a delta batch for a given table at a given tick — i.e., how many rows changed. This is workload-driven and bursty, not schema-driven. Delta size determines exchange buffer sizing, join output estimation, and tick-rate tuning. No published learned model directly targets this problem; lightweight online summaries (rolling mean, exponential moving average) are the right approach.

Neither layer replaces the other. Static statistics are appropriate for compile-time decisions (circuit layout, operator ordering). Dynamic statistics are appropriate for runtime adaptation (buffer sizing, tick coarsening, adaptive join feedback).

---

### Proposal: Learned and Adaptive Optimizations for Gnitz

This is a three-phase path from "zero" to "learned," grounded in what is proven to work.

#### Phase A: Statistics Foundation (Prerequisite for Everything Else)

Before any optimization — learned or classical — that requires cardinality knowledge, gnitz needs a statistics layer. Currently it has none beyond what can be directly computed from shard metadata.

**What to build:**
- A `StatisticsCollector` that runs alongside shard flush. Per-table, per-column: **ExaLogLog** sketches for `n_distinct` estimates (43% more space-efficient than HyperLogLog, same merge semantics; persisted as `.exll` sidecars), **KLL** sketches for quantile histograms (per-column range selectivity beyond uniform assumption; persisted as `.kll_cN` sidecars), MIN/MAX (already partially available from `ShardHandle` PK bounds), and row COUNT (from shard file headers). See `research/sketches.md` §Opportunity 1 and §Opportunity 3 for algorithm selection rationale.
- For a full initial picture on existing tables: a background ANALYZE-style sweep that reads shard files and builds initial sketches. Since gnitz owns the shards, this is straightforward.
- Multi-column correlation tracking: for pairs of columns appearing together in WHERE clauses or join conditions, maintain a joint frequency sketch (**Count Sketch**, not Count-Min Sketch — gnitz's Z-Set delta batches carry signed weights and Count-Min Sketch's unsigned-only model breaks under retractions) or a sampled MCV list. This is PostgreSQL `CREATE STATISTICS` semantics, adapted for incremental updates.
- Store statistics in a system table. Expose them to `compile_from_graph`.

**What this enables:** Filter selectivity estimates, join-side selection based on table sizes, and initial heuristic join ordering — all at circuit-compile time. Static statistics are more useful for gnitz than for Feldera because gnitz knows the full base table state, not just the incoming stream.

**Implementation stage:** RPython executor + system tables. No ML infrastructure required. This is entirely classical.

#### Phase B: Adaptive Runtime Feedback (SQL Server IQP Pattern)

Once basic statistics exist, layer in runtime feedback loops. The key insight from all production deployments is that **feedback-based adaptation beats neural models** for reliability and cold-start behavior.

**What to build:**

1. **Delta size and frequency tracking**: The executor already processes delta batches per tick. Record (table_id, tick, batch_size) in a rolling window; use the rolling mean/percentile as the expected delta size for exchange buffer pre-allocation and `repartition_batch` capacity hints. Additionally, maintain a **Count Sketch** (signed counters, not Count-Min — Z-Set weights) over group-key column values per tick to detect per-key frequency, and a **TP-Sketch** (Threshold-based Persistent Sketch, Feb 2026) over group keys across ticks to detect chronically hot groups. Batch-size EMA answers "how much data?"; Count Sketch answers "which keys dominate this tick?"; TP-Sketch answers "which keys appear in every tick?" — all three are needed for adaptive exchange and aggregation decisions. See `research/sketches.md` §Opportunity 2 for algorithm details and the pre-hashed insertion optimization.

2. **Adaptive join size feedback**: After each `JOIN_DELTA_TRACE` or `JOIN_DELTA_DELTA` execution, record (view_id, input_delta_size, output_size). If the join expansion factor consistently deviates from the estimate (based on Phase A statistics), adjust the estimate for the next tick. This is the DBSP analog of Oracle's adaptive plan statistics collector.

3. **Reduce output size feedback**: After each `op_reduce`, record (view_id, input_groups, output_delta_size). Use as a prior for downstream operators' buffer allocation.

4. **Tick-rate adaptation** (gnitz-specific, no analog in literature): DBSP's tick granularity — how many changes to batch before triggering computation — is currently fixed. A lightweight online model of observed delta rates (exponential moving average) could adaptively coarsen ticks under low-rate conditions (fewer IPC round-trips) and fine-grain them under high-rate conditions (lower latency). No ML model; just adaptive thresholds.

**What this replaces / improves:**
- Fixed-capacity buffer allocations → adaptive pre-allocation
- Fixed tick intervals → workload-adaptive tick sizing
- No feedback today → join output size estimates improve over time

#### Phase C: Learned Optimization (Once Query Optimizer Exists)

Once gnitz has a query optimizer with multiple plan choices (join orderings, filter placement alternatives, distinct pushdown variants), Bao-style hint selection is the pragmatic first learned component:

**Bao for gnitz:**
- Define a small set of **optimization hint sets**: e.g., {default}, {push-filter-before-join}, {push-distinct-before-filter}, {two-phase-agg-enabled}, {trace-sharing-enabled}. Each is a knob in `compile_from_graph`.
- For each incoming view compilation request, the bandit selects a hint set using Thompson sampling.
- After the view executes for some number of ticks, the executor records the accumulated processing time for that view (wall time per tick, summed over a window). This is the reward signal.
- The bandit updates its belief for the selected hint set and converges toward the best-performing strategy.

Why Bao over end-to-end learned optimizers:
- **Zero cold start**: Before any learning, Thompson sampling explores all hint sets, defaulting to classical optimizer behavior.
- **Bounded worst case**: Every hint set produces a valid plan; the worst outcome is the classical plan.
- **Online**: Learns from observed execution, no training data or labeled queries needed.
- **Interpretable**: When the bandit prefers hint set {push-filter-before-join}, it's telling you that predicate pushdown is worth it for this view's workload.
- **No ML infrastructure during compilation**: The bandit is a simple per-view probability vector (5-10 hint sets), not a neural network. The reward signal is observed tick processing time, which the executor already has.

**Longer-term: learned cardinality for join ordering:**
Once gnitz supports multi-way joins and the statistics layer from Phase A is mature, **FactorJoin** is the best fit:
- Factored into per-table models (using Phase A histograms as input features) + join-key histogram structure
- 40x lower inference latency than NeuroCard-style dense models
- Works on the per-table data distributions already tracked in Phase A
- Handles schema changes gracefully (re-learns per-table models independently)

**For delta batch size prediction specifically**, ALECE's architecture is the most applicable:
- Data-encoder: lightweight attention over column statistics (updated per batch as aggregations, not full retraining)
- Query-analyzer: maps operator graph topology to expected delta sizes
- Online update: only the data-encoder aggregations change when a batch arrives; the attention network is fixed

This would require research/implementation beyond existing libraries — it is a novel application of ALECE to DBSP's incremental semantics.

---

### Classical Optimizations: Replace or Augment?

| Classical Technique | Learned/Adaptive Alternative | Verdict |
|---|---|---|
| **XOR8 membership filter** (already in gnitz) | Learned Bloom filter | **Keep XOR8.** Learned filters are 10,000x slower in throughput. Gnitz already made the right call. |
| **Binary search in sorted shard columns** | PGM-Index positional lookup | **Worth evaluating.** PGM-Index supports dynamic sorted data, 1140x space savings, O(log ε) lookup. Effectiveness depends on PK type: for auto-increment U64 PKs, the key sequence is perfectly monotone and learned positions are near-exact. For UUIDv7 U128 PKs, the 48-bit timestamp prefix provides a monotone backbone with random perturbation from the suffix — still highly amenable to piecewise-linear approximation, since the dominant variation is monotone. For arbitrary random U64 or U128 PKs, no structure to exploit. Mature C++ library exists. The primary constraint is RPython FFI compatibility. |
| **Fixed MemTable flush threshold** (`should_flush` at 75% of `max_bytes`) | Count-Min Sketch write-frequency heat map per partition | **Straightforward improvement.** Track write events per partition with a Count-Min Sketch (unsigned frequency; Count-Min is correct here because churn magnitude is wanted, not signed net weight). Hot partitions flush earlier to reclaim capacity; cold partitions defer to allow Z-Set algebraic cancellation in memory before hitting disk. Periodic counter halving provides exponential decay. See `research/sketches.md` §Count-Min Sketch-Driven Flush. |
| **Fixed compaction threshold** (`compaction_threshold=4`) | Adaptive threshold driven by read amplification + S3-FIFO for shard handle pool | **Straightforward improvement.** Track read amplification (shards scanned per lookup) as a rolling statistic; trigger compaction when amplification exceeds a target. Separately, manage the `ShardHandle` pool with **S3-FIFO** ghost queues: close cold handles, keep recently-re-accessed handles in the main queue. See `research/sketches.md` §S3-FIFO. |
| **Histogram-based selectivity** (does not exist yet) | Neural cardinality estimation | **Build histograms first.** Even the best learned CE model is only worth it after classical extended statistics are exhausted. The independence-assumption gap (which learned models close) is only the residual error after proper multi-column stats. |
| **Heuristic join ordering** (does not exist yet) | Bao hint selection | **Plan for Bao as the first learned layer.** Classical heuristics (smallest-relation-first, filter-first) should be the default; Bao selects among pre-defined strategy bundles learned from observed workload. |
| **Static tick interval** | Adaptive tick sizing (exponential moving average of delta rates) | **Easy win, novel to gnitz.** No ML needed. Use EMA of observed delta batch sizes to tune the tick coarsening/fine-graining dynamically. |
| **Fixed exchange buffer capacity** | Delta-size-tracking feedback (Phase B) | **Easy win.** Use rolling mean of observed delta batch sizes to pre-size exchange sub-batches, reducing reallocation during `repartition_batch`. |

---

### The Case Against Neural Learned Optimizers (For Gnitz Now)

The honest argument against adopting neural learned optimization components in gnitz at this stage:

1. **No optimizer yet means no plan choices to learn over.** Learned optimizers work by selecting among alternatives. Until gnitz has a working query optimizer with multiple plan candidates, there is nothing to learn over.

2. **No training data.** Gnitz has zero query workload history. Every learned model needs either pre-training data (PRICE approach) or query observation time (Bao/Lero). The cold-start period is unavoidable.

3. **The 2024 VLDB verdict.** Under rigorous train/test splits, PostgreSQL outperforms all tested learned optimizers. The problem is overfitting to training query templates. Gnitz's workload will be even smaller and potentially more varied.

4. **RPython ML inference constraint.** ML models that run in the query compilation or execution path must be embeddable in RPython-translated C code. PyTorch/XGBoost are not available in the RPython environment. Bao-style bandit (simple probability vectors + Thompson sampling) is implementable in pure RPython. Neural forward passes are not.

5. **The adaptive feedback loops are easier and proven.** SQL Server IQP's adaptive joins, Oracle's adaptive plans, and Redshift's Stage all demonstrate that feedback-based runtime adaptation — without any neural network — closes a large fraction of the cardinality estimation gap. These are lower-risk, lower-complexity, and require no training data.

**Summary recommendation**: Build the statistics foundation (Phase A), then the adaptive feedback loops (Phase B), then — when the optimizer has multiple plan variants — Bao-style hint selection as the first learned component (Phase C). Neural cardinality estimation should be deferred until Phase A+B are complete and the remaining estimation gap is measurable.

---

## Part VII: Summary

### Optimization-to-Stage Mapping

```
SQL Text
    │
    ▼
[Rust SQL Planner]
    │  Tier 1: constant folding, predicate pushdown, CTE inlining
    │  Tier 2: join order (uses base table row counts from shard metadata),
    │           subquery flattening, semi/anti-join patterns
    ▼
CircuitGraph (system tables)
    │
    ▼
[compile_from_graph]
    │  Tier 1: dead operator elim, filter+map fusion, distinct rewriting,
    │           MergeSums, unused column pruning
    │  Tier 2: filter+join fusion, trace sharing (→ arrangement registry),
    │           bulk exchange routing, two-phase agg (constant-factor win),
    │           append-only detection, group-key hash sharing,
    │           late materialization, delta joins (requires sharing first)
    ▼
Instruction list (VM program)
    │
    ▼
[RPython JIT VM]
    │  Already: expression JIT, SoA bulk copy, RPython tracing JIT
    │  Future:  streaming cost model awareness
    ▼
ZSetStore / PartitionedTable
    │  Tier 2: view backfill on creation (from existing base table data)
    │  Tier 3: zone maps (per-shard column min/max; requires predicate
    │           pushdown into cursor construction as prerequisite),
    │           state eviction, partial materialization
    │  Adaptive: compaction threshold via read-amplification tracking
    ▼
Statistics layer (Phase A)
    │  Base table: row counts (shard headers), ExaLogLog n_distinct + Theta sketch
    │              per join-key column (shard sidecars), KLL quantile histograms
    │              (shard sidecars), min/max (existing ShardHandle bounds);
    │              Count Sketch (signed) for multi-column correlation
    │  Delta path: rolling batch-size EMA, Count Sketch per group key (single
    │              tick heavy hitters), TP-Sketch per group key (cross-tick
    │              persistence), Count-Min Sketch per partition (write frequency
    │              for flush scheduling)
    │  See research/sketches.md for algorithm selection and implementation shape
    ▼
Adaptive feedback (Phase B)
    │  Delta size tracking, join output feedback, tick-rate adaptation
    ▼
Learned selection (Phase C)
       Bao-style hint bandit over compile_from_graph strategy knobs
```

### What Feldera Does That Gnitz Should Prioritize

1. **UnusedFields** — column pruning before joins/reduces
2. **ChainVisitor** — filter+map fusion into single pass
3. **OptimizeDistinctVisitor** — distinct reordering (filter before distinct)
4. **FilterJoinVisitor** — push predicate into join loop
5. **ReduceExpressionsRule** — constant folding in expression programs
6. **DeadCode** — dead operator elimination in `compile_from_graph`
7. **CSE** — trace sharing across views on the same base table

### What Feldera Doesn't Do That Gnitz Needs

1. **Two-phase (local-global) aggregation** — Neither Feldera nor gnitz implements this. The Flink argument (pre-aggregation reduces N events → K keys) is weaker in a DBSP context: what gets shuffled is already a *delta batch*, not a full relation scan, so the shuffle volume is inherently bounded by the size of changes. Gnitz's `ConsolidatedScope` also already gives implicit MiniBatch semantics by processing the whole batch at once. Two-phase aggregation is still a worthwhile constant-factor IPC reduction, but it is not the order-of-magnitude win it is in Flink.
2. **Split DISTINCT for exchange skew** — Same nuance applies. Useful for adversarial workloads with delta-level skew; not a top priority.
3. **Delta joins** (Materialize pattern) — N delta pipelines for N-way joins, zero intermediate materialized state. Directly expressible in DBSP algebra; requires arrangement sharing first. See Part V §3.5 and Tier 3 item #22.
4. **Global arrangement registry** — DD/Materialize's first-class arrangement sharing across queries. Reduces join trace memory from O(K×|T|) to O(|T|) for K views sharing table T. See Part V §5.1 and Tier 2 item #10.
5. **Columnar bulk-copy-aware fusion** — gnitz's SoA batches allow column-level memcpy that Feldera's row-at-a-time Rust generics model can't exploit; operator fusion should preserve batch layout, not break it into rows
6. **Group-key hash sharing between REDUCE and EXCHANGE** — gnitz already uses the same Murmur3 hash for both; making this explicit at circuit level avoids recomputing the same hash in two separate operators
7. **State eviction via PartitionedTable** — gnitz's partition-per-shard architecture (256 independent partitions) is a natural base for Noria-style partial materialization; Feldera has no equivalent (it does not own its source data)
8. **View backfill on creation** — gnitz stores primary data, so a newly created view over an existing table must be initialized from stored rows; Feldera starts from empty stream state and has no equivalent problem
9. **Zone maps per shard** — per-shard min/max column statistics for predicate-based shard skipping; requires predicate pushdown into cursor construction as a prerequisite
