# Compound / wide PK views — remaining extensions

## 1. Remaining work

Each item below is an independent, unimplemented future change.

- **Range / non-equi join — remaining extensions.** INNER and LEFT OUTER range /
  band joins exist: a `DeltaTraceRange` operator with a symmetric source-PK re-key
  + output exchange; band joins (`n_eq ≥ 1`) scatter the input delta by the
  equality prefix, pure range joins (`n_eq == 0`) broadcast it. Open extensions:
  - **Pure-range distribution beyond broadcast** (`n_eq == 0`): a range-aware
    (order-preserving) exchange that range-partitions both sides so a probe
    touches only boundary-overlapping workers, and/or a write-once broadcast SAL
    group read by all workers instead of `num_workers` cloned batches. The general
    fix for broadcast cost at large `W`; needs partition-boundary metadata +
    rebalancing.
  - **Multiple range conjuncts / residual ON predicates**: a post-join `Filter`
    over the normalized output (the operator exists; the 3VL bookkeeping and
    planning surface do not).
- **Python / C binding surface for compound-PK *result* rows** — views whose
  *output* PK is itself compound. (A join view's source PK rides as payload, so
  join/GROUP-BY result rows are unaffected; this is only about views that persist a
  multi-column output PK.)
- **`map_reindex` elision for PK-aligned joins** — when the join key equals the
  source PK, the `ReindexPacker::promote_into` PK rewrite re-copies OPK bytes the
  `execute_map` bulk PK copy already wrote. The dominant costs (batch allocation,
  per-row SENTINEL decode of each source PK column, non-PK bulk copies) cannot be
  elided without a zero-copy batch type or a `JoinDirect` operator that reads PK
  columns from the PK region on the fly; skipping `map_reindex` outright would drop
  the source-PK payload column users can SELECT. Larger feature.
- **AVI/GI cursor caching across epochs** — separate optimization project.
- **Monolithic physical-plan serialization in the Views catalog** — separate
  architectural project.

## 2. By-design boundaries (not future work)

- **Equi-joins do not use secondary indexes.** `map_reindex` repartitions both
  sides onto the join key, and `integrate_trace` accumulates each side into a
  trace table whose PK *is* that join key (`_join_pk`). The join operator
  `seek_bytes`-seeks the sorted trace and merge-walks the equal-key run, reading
  the matched payload straight from the trace cursor (`join_dt_merge_walk`; the
  size-adaptive `join_dt_swapped` flips driver/probe when the delta outgrows the
  trace). So the trace already is a *covering*, key-sorted index on the join key,
  co-partitioned with the probe. A secondary index is strictly worse on two
  counts baked into how it is stored: it is partitioned by **source PK** (the
  index is a child table of the base table, populated from the same
  partition-local delta during integrate, so maintenance stays exchange-free at
  the cost of value-locality), so one indexed value's entries scatter across
  every worker (point lookups broadcast-and-merge across all partitions), and
  each entry is only
  `(promoted key, src_pk)` — non-covering, forcing a PK heap fetch for the
  payload. An equi-join needs same-key rows co-located *with* their payload, which
  is exactly what reindex+trace gives and a source-PK-partitioned index does not.
  Secondary indexes serve **single-table** range predicates; the range / non-equi
  **join** likewise keeps the reindex+trace shape, swapping the equality probe for
  an ordered range walk — the index loses there too (non-covering, user-managed
  lifecycle, absent over filtered inputs).
- **Mixed string/native equijoin keys** (`VARCHAR = U128`, etc.) stay rejected: a
  128-bit string content hash never byte-equals a native integer encoding, so the
  join would match nothing. A permanent semantic boundary, not a deferral.
- **Wide-unsigned cross-sign equijoin keys** — a 128-bit unsigned side
  (`U128`/`UUID`, e.g. `DECIMAL(38,0)`) joined with any signed `I*`. Their faithful
  common type is a signed-256 type that does not exist, so `join_key_common_type`
  returns `None` and the planner rejects the pair. Permanent, not a deferral.
  (Narrower unsigned sides up to `U64` promote into `I16`/`I32`/`I64`/`I128`.)
- **Delta-delta join term (`ΔA ⋈ ΔB`)** is intentionally never emitted: the engine
  activates exactly one input delta per epoch, so the cross-term is realized across
  consecutive epochs via `integrate_trace`. It is lost only for a self-join, which
  is rejected upstream — so a join of two distinct tables needs no third term.
- **Float / STRING / BLOB PK columns** are excluded by the byte-equal key contract
  (IEEE-754 `-0.0`/`+0.0` and German-string content hashing both break
  `memcmp`-order equality). Float *join keys* are likewise rejected.

## 3. Invariants any future work here must preserve

- **No narrow-PK guards.** One byte path serves every width: no `pk_is_wide()`
  dispatch in merge/cursor/reduce, no width gate on the memtable Bloom or point
  lookup. The merge heap key is the raw OPK prefix with a `compare_pk_bytes`
  (`memcmp`) tie-break. The narrow `pk_cache` exchange optimization is the only
  sanctioned width special-case.
- **OPK invariant.** Every Batch/shard/SAL PK region is order-preserving
  big-endian; `memcmp` gives the typed lexicographic order. A PK column is copied
  into payload only through the `decode_pk_column` path (`EXPR_COPY_COL` PK source,
  or `EXPR_LOAD_PK_SIGNED_INT`/`EXPR_LOAD_PK_UNSIGNED_INT`) — never a raw byte copy
  of OPK bytes, which would corrupt wire output, FK lookups, and expression reads.
  The payload region is always native LE. A *promoted* PK-source join key is
  recovered via `decode_pk_column` then re-encoded at `T` (`ColPromoter`), never a
  raw widen of OPK bytes.
- **One packer routes both sides.** The trace-side `_join_pk` and the delta-scatter
  routing key are produced by the same `ReindexPacker` at every key arity and width
  (including cross-width and cross-sign promotion) — the single source of the
  co-partition guarantee. The join-shard column list
  (`reindex_cols_through_filters`) is the reindex Map's sequence verbatim, never
  column-deduplicated, so an overlapping key (`a.x = b.p AND a.x = b.q` → `[x, x]`)
  and its per-slot promotion targets survive to the scatter, and the two sibling
  reindex Maps of a nullable LEFT-join key (identical sequences) collapse to one,
  never doubled.
- **Cross-sign promotion is value-preserving.** A carried target `T` may differ in
  sign class from its source only as `join_key_common_type` allows: an unsigned
  source promotes into a **strictly wider** signed `T` (an equal-width signed type
  cannot hold the unsigned range — distinct values would alias), and a signed
  source never promotes into an unsigned `T`. The compiler's reindex guard
  (`compiler.rs`) and `encode_pk_column_promoted` both rely on this; the unsigned
  side zero-extends and the signed side sign-extends so equal numeric values pack
  byte-identically.
- **`widen_pk_be` vs `pack_pk_be`.** `widen_pk_be` (value accessor, right-aligned)
  lives in `batch.rs`; `pack_pk_be` (sort key, left-aligned) lives in `merge.rs`.
  Never substitute one for the other.
- **Per-column key encoding.** Each PK / join-key column packs its own slot at its
  type `T_i` (integer OPK bytes, or a per-column 16-byte STRING/BLOB content hash).
  Never hash the combined tuple into one slot.
- **German-string (STRING *and* BLOB) handling.** BLOB shares STRING's 16-byte
  out-of-line struct; any path that special-cases STRING (relocation, the SAL
  fast-path `schema_wire_safe` gate) must use `is_german_string`, not a bare
  `== STRING`, or a long BLOB cell's heap offset dangles / a 0-cap blob arena
  overflows.
- **Batch PK-variant ↔ arity.** `ZSetBatch::validate` asserts `Bytes ⟺
  pk_count() ≥ 2`; a view emitting a compound PK must produce a `Bytes` batch.
- **Planner/compiler schema agreement.** The planner's reduce-output `pk_cols` must
  exactly match `build_reduce_output_schema` (compiler.rs), and the `_join_pk` stamp
  must match `reindex_output_schema` — both derive a slot type from the same carried
  `T` (`resolve_reindex_type`); a divergence silently scrambles output column
  positions or fails `schemas_physically_identical`.
- **Self-join stays rejected** (the 2-term DBSP join drops `ΔT ⋈ ΔT`), independent
  of PK width.
