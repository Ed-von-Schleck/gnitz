# Non-equi (range) join over ordered reindex traces

## Goal

Support an incremental join view whose ON clause contains **one range
conjunct** (`<`, `<=`, `>`, `>=`) between columns of the two tables, optionally
behind equality conjuncts:

```sql
CREATE VIEW v AS SELECT a.id, b.id, b.t
  FROM a JOIN b ON a.x < b.y;                          -- pure range join
CREATE VIEW w AS SELECT *
  FROM a JOIN b ON a.k = b.k AND a.lo <= b.t;          -- band join: eq prefix + range
```

Today both are rejected by `collect_equijoin_keys`
(`gnitz-sql/src/planner.rs:1561`: *"JOIN ON: only an AND-conjunction of column
equijoins is supported"*). After this change they are planned as a
`DeltaTraceRange` join: the standard symmetric 2-term DBSP join where the
equality probe (`seek_bytes` + equal-key-run walk) is replaced by an **ordered
range walk** over the other side's reindex trace, distributed by **broadcasting
the probe delta** and **re-keying the output symmetrically**.

This slice implements `wide-pk-incremental-views.md` §1 item 1, building on the
single-table range-scan substrate: ordered secondary-index range scans over
half-open key intervals.

## Design summary (what is new, in one paragraph)

Both sides reindex onto `[eq keys…, range key]` at the pair-wise common
promoted types (`join_key_common_type`), exactly like an equi-join — so each
side's `IntegrateTrace` table is **already an ordered arrangement** of that
side by the range key (the PK region is OPK at rest; memcmp = typed order,
signed included). Per epoch, the active delta is **broadcast** to all workers
(the equality scatter cannot route a range probe: matches live everywhere);
each worker probes its **partitioned** slice of the other side's trace with a
half-open `[start, end)` byte interval derived from each delta row's own
packed key — every match is emitted exactly once cluster-wide. Trace
integration under broadcast keeps only the worker-owned rows via a new
`PartitionFilter` operator. Because the two join terms emit with different
delta-side keys (`x` vs `y`), the output is **re-keyed onto the source-PK
pair** `(a.pk…, b.pk…)` — the only identity under which an insert's `+1` and a
later retraction's `-1` (emitted in different epochs, on different workers,
from opposite terms) are byte-identical — and then **exchanged by that output
PK** (`ExchangeShard`), so the view output table is PK-partitioned like every
other view and cancellation happens in the normal LSM consolidation path. No
client/wire-protocol change; everything is planner + engine + master relay.

## Background — current state (verified against code)

**Equi-join circuit** (`gnitz-sql/src/planner.rs:1009-1351`,
`execute_create_join_view`): per side `input_delta_tagged` → optional
`Filter(multi_null_filter_prog)` (only when a key column is nullable, :1128-1173)
→ `map_reindex(cols, target_tcs, build_reindex_program)` (:1141, :1174) →
`integrate_trace` (:1176-1177); two join terms
`join_with_trace_node(reindex_a, trace_b)` / `(reindex_b, trace_a)`
(:1178-1179); per-term `Map(Projection)` normalizes payload column order to
`[A cols, B cols]` (:1181-1193); `union`; optional user-projection `Map`;
`sink` (:1325-1330). View PK = the k synthetic `_join_pk` columns (:1333-1335).
Self-join rejected (:1065). LEFT JOIN decomposition at :1197-1227.

**Key promotion**: `join_key_common_type` (`gnitz-wire/src/types.rs:352-399`)
returns the common type `T` per pair; `validate_join_key_pair`
(`planner.rs:1446-1467`) rejects floats and one-sided strings. Both sides
encode equal values **byte-identically** at `T` (`encode_pk_column_promoted`:
unsigned zero-extends, signed sign-extends), and every Batch/shard PK region is
order-preserving big-endian (OPK invariant, `wide-pk-incremental-views.md` §3)
— so the reindexed trace is memcmp-ordered by the typed key value, signed
columns included.

**Distribution** (the part the range join must change):

- Joins compile with **no** `ExchangeShard` node — the whole circuit is the
  `pre` plan (`compiler.rs:2222-2244`, 0-exchange arm). Co-partitioning is
  achieved at *input relay* time: in `evaluate_dag_multi_worker`
  (`dag.rs:1167`, join-shard branch :1240-1255) each worker sends its local
  source-delta slice to the master (`FLAG_EXCHANGE`), the master's
  `prepare_relay` (`runtime/master.rs:733`) looks up
  `get_join_shard_cols(view_id, source_id)` (`dag.rs:656`, the reindex Map's
  column/target-tc pairs via `reindex_cols_through_filters`,
  `compiler.rs:488`), scatters the **raw** rows with
  `RouteMode::JoinPromote` (`ops/exchange.rs:210`, packing the reindex key via
  `compound_join_packer` :252 and hashing with `partition_for_pk_bytes`), and
  `emit_relay` (:786) broadcasts one `FLAG_EXCHANGE_RELAY` group with
  per-worker destination batches, echoing `source_id` for the worker's
  `do_exchange_wait` demux. The worker then runs the whole circuit on its
  join-key-partitioned slice.
- `prepare_relay` dispatches on `source_id` (:745-756): `> 0` → join-shard
  input relay; `== 0` → mid-circuit `ExchangeShard` relay using
  `get_shard_cols` + `RouteMode::GroupKey`. **The two relay kinds for one view
  are already distinguishable** — the range join uses both, one per leg.
- GROUP BY / set-op views use a mid-circuit `ExchangeShard` node; the dag
  driver's `has_exchange` branch (`dag.rs:1216-1239`) runs `pre` → sets the
  pre-plan `exchange_schema` (`get_exchange_schema`, compiled from
  `pre.out_reg`'s schema at `compiler.rs:2278`) → `exchange.do_exchange(view,
  pre_result, 0)` → `consolidate_exchanged` → `post`. **`has_join_shard` is
  NOT join-specific**: it is `!get_join_shard_cols(view_id, src_id).is_empty()`
  (`dag.rs:1201`), and `reindex_cols_through_filters` (`compiler.rs:488`)
  matches *"a join (or group) reindex"* (its own comment, :505) — so a GROUP BY
  / reduce / single-sided set-op view has **both** `has_join_shard == true`
  **and** `has_exchange == true`. The two arms do not separate by boolean
  value; only the if/else **ordering** (`has_exchange` checked first, :1216)
  routes GROUP BY to the exchange arm and equi-joins (which have
  `has_exchange == false`, 0-exchange) to the `has_join_shard` arm (:1240).
  **Consequence (load-bearing):** the new range-join branch must NOT key on
  `has_join_shard && has_exchange` — that predicate is true for every GROUP BY
  view and would capture and break them. It must key on the precise
  `view_is_range_join(view_id)` predicate (§7).
- The single-worker drivers (`evaluate_dag` :1076, `execute_epoch_for_dag`
  :1419) run pre → post back-to-back with no IPC; `skip_exchange` shortcuts
  exist for trivially co-partitioned views (:1206-1209) and a join-shard
  co-partition shortcut at :1242-1244. **Neither shortcut may apply to a range
  join** (a range probe needs the full delta even when the key equals the
  source PK).
- There is **no broadcast primitive** in `ops/exchange.rs` — every scatter
  hashes each row to exactly one destination.

**Join operators** (`ops/join.rs`): `op_join_delta_trace` (:226-255)
consolidates the delta, picks delta-driven `join_dt_merge_walk` (:262-324,
per-row `cursor.seek_bytes(pk)` + equal-key-run walk, weight
`w_delta.wrapping_mul(w_trace)`, skip 0) or trace-driven `join_dt_swapped`
(:326-369) when `|Δ| > |trace|` (the `n > trace_len` heuristic, :250). Row
emission via `write_join_row` (:419-474): output layout
`[delta PK ‖ delta payload ‖ trace payload]`, schema from
`merge_schemas_for_join` (`compiler.rs:779`). `ReadCursor::seek_bytes`
(`storage/read_cursor.rs:518`) is an arbitrary-position lower-bound re-seek
(rebuilds the tournament tree) — the index range scan's
`resolve_index_entry_into` already re-seeks one cursor in arbitrary key order,
so per-delta-row repositioning (including backward) is supported.

**VM** (`vm.rs`): `Instr` enum :15-74 (`JoinDT { delta_reg, trace_reg, out_reg,
right_schema_idx }` is the template to mirror); `execute_epoch_multi` (:754)
carries **no worker identity** — `(worker_id, num_workers)` must be baked into
emitted instructions at compile time (compilation runs per worker process;
plan caches are per-process).

**Wire circuit** (`gnitz-wire/src/circuit.rs`): one opcode per join kind
(`OPCODE_JOIN_DELTA_TRACE = 5`, `_OUTER = 22`); node parameters ride in
`CircuitNodeColumn { kind, position, value1, value2 }` rows decoded by
`decode_op_node` (:218). Highest opcode today is 31; 32/33 are free.

**Half-open range walk.** The single-table range scan (`seek_by_index_range`,
`catalog/store.rs:1041-1137`) walks a half-open `[start, end)` byte interval.
Its bounds ride in `RangeDescriptor { eq, n_eq, start: Cut, end: Cut }`
(`gnitz-wire/src/range.rs`), where `Cut { Before(u128), After(u128) }` is a
two-variant cut descriptor; the Cut→byte-key mapping is the inline `cut_key`
closure (`store.rs:1094-1101`) over a fixed-width successor
`increment_key_in_place` (`store.rs:1824`, returns `bool` — `false` on
carry-out of an all-`0xFF` or empty key). That mapping is keyed at
`index_key_type` and is driven by one planner-chosen `Cut` per bound, not by a
trace-slot-vs-delta-slot `RangeRel` that derives *both* interval ends from one
packed delta key, so it is not reusable as-is. This plan reuses only
`increment_key_in_place` and builds the §3 `RangeRel`→cut-point derivation.

## Design

### 1. The formula is unchanged

Join with an arbitrary predicate is bilinear; the symmetric 2-term form
(`foundations.md` §3) applies verbatim under single-source-per-epoch:

```
d(A ⋈θ B) = dA ⋈θ z⁻¹(I(B)) + dB ⋈θ z⁻¹(I(A))
```

Output weight stays `w_delta × w_trace`. Only the *match set* per delta row
changes: from "trace rows with equal key" to "trace rows whose range slot
satisfies θ relative to the delta row's slot, within the same equality-prefix
group".

### 2. The ordered trace is the probe structure — not the secondary index

The reindex trace is the probe structure, strictly better than a dedicated
secondary index and already present in the join shape:

- **Covering**: the trace stores the full payload; a secondary index entry is
  `(promoted key, src_pk)` and every hit would pay a source-PK heap fetch
  (`resolve_index_entry_into`).
- **View-owned lifecycle**: `_int_{view}_{nid}` tables are created/dropped with
  the view; a user index can be dropped out from under the join.
- **Present over filtered inputs**: the trace integrates the post-`Filter`
  stream; a base-table index does not reflect the view's WHERE pushdown.
- **Promotion-consistent**: both sides pack at the pair's common type `T`, so a
  probe bound is the delta row's own PK bytes, **no decode/re-encode at all**.
  (An index is keyed at `index_key_type`, a different promotion.)

Strings/BLOBs are excluded from the *range pair* (their 16-byte content hash is
equality-correct but not order-preserving); they remain legal in the equality
prefix.

### 3. The probe: per-row half-open byte interval

Canonicalize the range conjunct to `L.x OP R.y` (flip `b.y > a.x` →
`a.x < b.y`). Each join term carries a `RangeRel` — the relation the **trace**
slot must satisfy versus the **delta** slot:

| OP (`L.x OP R.y`) | term AB (delta=A, trace=B): `{y : x OP y}` | term BA (delta=B, trace=A): `{x : x OP y}` |
|---|---|---|
| `<`  | `rel = Gt` | `rel = Lt` |
| `<=` | `rel = Ge` | `rel = Le` |
| `>`  | `rel = Lt` | `rel = Gt` |
| `>=` | `rel = Le` | `rel = Ge` |

(term AB's rel is the converse of OP; term BA's rel is OP itself.)

The trace PK is `[eq slots…, range slot]`, stride = `eq_size + slot_size`,
arity `n_eq + 1`. For a delta row with PK bytes `p`, let
`eq = p[..eq_size]`, `d = p[eq_size..]`, and `succ` = fixed-width byte
successor (`increment_key_in_place`, `None` when all-`0xFF` or empty). Cut
points over the full trace-PK space:

| rel | `start` | `end` (`None` = table end) |
|---|---|---|
| `Gt` | `succ(eq‖d)`; `None` → row matches nothing | `succ(eq)` zero-padded; `n_eq == 0` or `succ` fails → `None` |
| `Ge` | `eq‖d` | same as `Gt` |
| `Lt` | `eq ‖ 0x00*slot` | `Some(eq‖d)` |
| `Le` | `eq ‖ 0x00*slot` | `succ(eq‖d)`; `None` → table end |

The walk per delta row:

```rust
cursor.seek_bytes(&start);
while cursor.valid
    && end.as_deref().is_none_or(|e| cursor.current_pk_cmp_bytes(e).is_lt())
{
    let w_out = w_delta.wrapping_mul(cursor.current_weight);
    if w_out != 0 { write_join_row(output, delta_mb, i, cursor, w_out, ..); }
    cursor.advance();
}
```

`cursor.valid` and `cursor.current_weight` are public fields;
`current_pk_cmp_bytes` (`read_cursor.rs:563`) is the OPK-memcmp comparator the
equi-join merge-walk uses (returns `std::cmp::Ordering`; `current_pk_bytes()`,
:540, returns the raw region if a bare slice compare is preferred). Pure byte compares are valid here for the
same reasons the single-table range scan's are: OPK at rest, both sides
byte-identical at the common `T`, and (PK, payload) sort puts every row of a PK
interval in one contiguous run.
`succ` carries out of the range slot ripple into the equality bytes for free
(e.g. `eq‖0xFF…` under `Le` → the next equality group's first key == the
`Gt`/`Ge` `end` — empty tail, no special case). Equal-key runs with mixed
payloads need no handling beyond what `<` on full PK bytes gives.

Lift `increment_key_in_place` out of `catalog/store.rs:1824` (file-private,
returning `bool`) into a shared module (e.g. `storage/range_key.rs`) and host
the §3 `RangeRel`→cut-point derivation (`range_cut_points`) there.
`increment_key_in_place` then has two consumers: `seek_by_index_range`'s
existing `cut_key` closure and the new `range_cut_points`. The cut-point
*derivation* is not shared: the index scan drives off native-`u128` `Cut`
values re-encoded through `seek_prefix` (`RangeDescriptor { eq: [u128; …],
start/end: Cut }`), whereas the probe drives off the delta row's raw OPK PK
bytes — different representations, so `seek_by_index_range` keeps its own
`cut_key` closure and does **not** call `range_cut_points`.

Per-row `seek_bytes` repositioning is O(log T) and supports the backward seeks
that `Lt`/`Le` with `n_eq == 0` require (all rows share `start = 0x00…`).
Monotone cursor reuse for `Gt`/`Ge` (delta is consolidated ⇒ sorted ⇒ `start`
non-decreasing) and a shared-prefix walk for `Lt`/`Le` are follow-on
optimizations, not slice 1.

### 4. Distribution: broadcast in, ownership-filtered traces, re-keyed exchange out

**Why broadcast.** The equality scatter routes a row to `hash(key)`'s owner;
a range probe's matches are spread over the entire key space — no single
destination exists. The delta (small, per-epoch) is broadcast; the traces
(large, accumulated) stay partitioned, each row owned by exactly one worker.
Union over workers of `Δ_full ⋈θ trace_w` = the full term, each match emitted
exactly once (on the matched trace row's owner).

**Trace ownership under broadcast.** Every worker receives the full delta but
must integrate only its owned slice into its trace, or traces replicate and
matches duplicate. New operator `PartitionFilter` between `map_reindex` and
`integrate_trace`: keep row iff
`worker_for_partition(partition_for_pk_bytes(pk_bytes), num_workers) == worker_id`
— the same hash the equality scatter uses on the same packed bytes, so the
trace partitioning is identical to what `RouteMode::JoinPromote` would have
produced. `(worker_id, num_workers)` are baked into the emitted instruction at
compile time (each worker compiles its own plan; single-process = `(0, 1)` =
keep-all). Deterministic per row across epochs; a worker-count change
re-partitions traces exactly as it already breaks equi-join co-partitioning —
no new constraint. Because `(worker_id, num_workers)` are compile-time
constants, each worker's compiled VM `Program` is worker-specific (one wire
circuit, a different emitted `PartitionFilter` per worker); `num_workers` is
fixed for a process's lifetime, so a topology change is a restart and recompile,
not a live plan-cache invalidation.

**Why the output must be re-keyed.** `write_join_row` emits with the
delta-side PK. For an equi-join both terms emit the *same* key (equality), so
a `+1` from term AB and the cancelling `-1` from a later term-BA retraction
are byte-identical and consolidate. For a range pair the two terms emit
`OPK(x)` vs `OPK(y)` — different PK bytes, produced on different workers
(b's owner vs a's owner). They would never meet, leaving permanent `+1`/`-1`
ghost pairs spread across workers that every read would have to merge away.
Therefore: after the union, a `map_reindex` onto the **source-PK pair**
`[a.pk cols…, b.pk cols…]` (symmetric — both terms' normalized payload carries
both sides' PK columns, moved there by the per-side reindex), then an
**`ExchangeShard` on those columns**, then the sink. Every emission for a
logical pair `(a, b)` routes to the same worker with identical
(PK, payload) bytes; cancellation happens in normal LSM consolidation. The
view output table is partitioned by its PK like every other view — scans,
seeks, and downstream views need **no changes**.

**Epoch flow** (multi-worker, new dag branch — keyed on
`view_is_range_join(view_id)`, placed first in the chain, before the plain
`has_exchange` arm):

```
worker: local ΔA slice ──FLAG_EXCHANGE──► master: prepare_relay(src_id>0)
                                            └─ range join? → BROADCAST (concat all, every worker)
worker: pre plan on full ΔA:
    Filter(NOT NULL keys) → map_reindex[eq…,range]
        ├─ PartitionFilter → integrate_trace_A   (owned slice only)
        └─ JoinDTRange(ΔA_full, trace_B_w)       (probe, rel_ab)
    → Map(proj normalize) → union → map_reindex[(a.pk…, b.pk…)] → Map(user proj)
                       ──FLAG_EXCHANGE──► master: prepare_relay(src_id==0)
                                            └─ ExchangeShard cols → GroupKey scatter
worker: post plan: consolidate_exchanged → IntegrateSink
```

Two master round-trips per epoch (range-join views only). Both
`do_exchange` calls run unconditionally — even with empty batches — to keep
exchange-barrier participation (dev-guide checklist). The relay demux already
distinguishes the legs by `(view_id, source_id)`.

**Single-worker / single-process**: `execute_epoch_for_dag` runs pre → post
locally; broadcast degenerates to identity, `PartitionFilter(0,1)` keeps all.

### 5. NULL semantics

A NULL in any ON-clause column (equality or range) matches nothing (SQL 3VL).
Reuse the existing mechanism unchanged: when any key column of a side is
nullable, insert `Filter(multi_null_filter_prog(all key cols, …, false))`
before that side's reindex (`planner.rs:1128-1140`) — the range column is just
one more key column in the list. INNER join only, so no null-fill branches.

### 6. Planner admission rules

- ON = AND-conjunction of ≥ 0 equality conjuncts + **exactly one** range
  conjunct, every conjunct `col OP col` across the two tables. Anything else
  keeps today's error (a second range conjunct, OR, expressions: reject with a
  message naming the one-range-conjunct rule).
- Equality pairs: existing `validate_join_key_pair` rules verbatim.
- Range pair: `join_key_common_type` must exist **and** both sides must be
  fixed-width integers — reject STRING/BLOB (`is_german_string`) with
  *"range join key … content hash is not order-preserving"*; floats already
  rejected.
- `n_eq + 1 ≤ PK_LIST_MAX_COLS` (same cap as the equality path).
- Output PK: the binding constraint is the **count cap**
  `a.pk_count + b.pk_count ≤ PK_LIST_MAX_COLS` (= 4). Route the synthesized
  `[a.pk…, b.pk…]` list through the existing `validate_pk_cols`
  (`catalog/sys_tables.rs:64`, count check at :71). Its `MAX_PK_BYTES` stride
  ceiling (:105) is **non-binding** here: `MAX_PK_BYTES = MAX_PK_COLUMNS·16
  = 80` (`gnitz-wire/src/catalog.rs:124`), while any ≤ 4-column pair-PK is at
  most `4·16 = 64` bytes, so anything that passes the count passes the stride.
  Keep `validate_pk_cols` as the single defensive gate, but the only rejection
  the pair-PK can actually hit is the 4-column count.
- LEFT/OUTER + range conjunct: reject (*"LEFT JOIN with a range predicate is
  not supported"*). Anti/semi-join machinery is equality-shaped; out of scope.
- Self-join: still rejected (source-id discriminator, unchanged).

## The change, by file

### 1. `crates/gnitz-wire/src/circuit.rs` — two opcodes, one kind, one param row

- `pub const OPCODE_JOIN_DELTA_TRACE_RANGE: u64 = 32;`
  `pub const OPCODE_PARTITION_FILTER: u64 = 33;`
- `pub enum RangeRel { Lt, Le, Gt, Ge }` (u64-convertible for the wire).
- `JoinKind::DeltaTraceRange { n_eq: u8, rel: RangeRel }` (keeps `Copy`);
  `OpNode::PartitionFilter` (no payload — worker identity is compile-time).
- Encode: range join node = opcode 32 + one `CircuitNodeColumn { kind:
  NODE_COL_KIND_RANGE_JOIN = 7 (next free kind; current max is 6), position: 0, value1: n_eq,
  value2: rel as u64 }`. Decode arms in `decode_op_node` (reject unknown
  `rel`/missing param row). `PartitionFilter` is opcode-only.

### 2. `crates/gnitz-core/src/circuit.rs` — builder methods

Next to `join_with_trace_node` (:376):
`join_with_trace_range_node(delta, trace_node, n_eq: u8, rel: RangeRel)`;
`partition_filter(input)`. `shard()` (:440), `map_reindex` (:280), `map`
(:313) are reused as-is.

### 3. `crates/gnitz-sql/src/planner.rs` — extraction + circuit assembly

- **`collect_join_predicates`** (generalizing `collect_equijoin_keys`
  :1509-1565): flatten the AND tree; classify each leaf via the existing
  cross-table column resolution into equality pairs or range conjuncts
  (`BinaryOp { Lt | LtEq | Gt | GtEq }`), canonicalizing operand order to
  left-table-first (flip the operator when the right table's column is the
  left operand). 0 range conjuncts → existing equi path **byte-identical**
  (this function must be a pure superset; the equi circuit and its
  serialization must not change). 1 range conjunct → range path. ≥ 2 → error.
- **Validation** per §6 admission rules; compute `target_tcs` for all
  `n_eq + 1` pairs (range pair last), per-side carried tcs via
  `carried_reindex_tc` exactly as :1088-1094.
- **Circuit** (range path), reusing the equi skeleton:
  - per side: NULL filter over *all* key cols when nullable → `map_reindex`
    with `[eq cols…, range col]` → `partition_filter` → `integrate_trace`.
    The join terms consume the **unfiltered** reindex node:
    `join_with_trace_range_node(reindex_a, trace_b, n_eq, rel_ab)` and
    `(reindex_b, trace_a, n_eq, rel_ba)` per the §3 table.
  - per-term normalize projections `proj_ab`/`proj_ba` verbatim (:1181-1193,
    with `k = n_eq + 1`), `union`.
  - **re-key**: `map_reindex(merged, &pair_pk_cols, &zero_tcs,
    build_reindex_program(union_schema))` where `pair_pk_cols[i]` are the
    union-output positions of A's PK columns (`k + a_pk_idx`) then B's
    (`k + left_n + b_pk_idx`) over the layout `[_join_pk_0..k, A cols, B cols]`;
    targets all `0` (self-derive — no cross-side promotion, each slot keeps its
    source PK type).
  - user projection `Map` over the re-key output, selecting the
    `pa + pb` leading PK-copy columns' payload twins implicitly (as the equi
    path does for `_join_pk`) plus the user's selected columns; then
    `shard(node, &pair_pk_idxs)` with `pair_pk_idxs == (0..pa+pb).collect()`
    → `sink`. (`shard()` takes a `&[usize]` column **list**, not a range — do
    not pass `&[0..pa+pb]`.)
  - **Routing invariant**: the `ExchangeShard` columns must equal `view_pk` in
    strict order (`shard_cols == (0..pa+pb)`). The output relay's
    `RouteMode::GroupKey` scatter routes by the packed PK region
    (`partition_for_pk_bytes`, identical to view scan/seek) **only** when
    `col_indices == schema.pk_indices()` (`fill_worker_indices`,
    `ops/exchange.rs:281`; contract on `shard_cols_match_pk`, `schema.rs:439`).
    A subset or permutation silently falls to a column-wise hash that diverges
    from scan routing, stranding rows on a worker the scan never reads. The
    GROUP-BY-equals-PK reduce view (`planner.rs:1876`) routes the same way;
    assert this equality in the planner. A pair-PK of wide types can exceed
    16 bytes; `partition_for_pk_bytes` (`storage/partitioned_table.rs:458`)
    then hashes the full region via `xxh::checksum` instead of the `u128`
    `widen_pk_be` path — consistently for both the scatter and view scan/seek
    (same function, same packed bytes), so wide compound pair-PKs route
    correctly with no extra handling.
  - Output schema: `pa + pb` leading `ColumnDef`s named `_pair_pk_{i}`
    (types = source PK column types, non-nullable) + user-selected columns;
    `view_pk = 0..pa+pb`. Duplicate-name rejection as today.
  - **Fuse the re-key and the user projection (recommended default, not
    optional)**: emit a single `map_reindex` whose program produces exactly the
    user columns over `pair_pk_cols`. Besides saving a node, this is the safer
    form: it never materializes the synthetic `_join_pk` slots (positions
    `0..k`), whose *values* differ between term AB (`OPK(a.x)`) and term BA
    (`OPK(b.y)`). A non-fused path is correct only as long as the trailing user
    projection drops `0..k`; the fused path makes that drop structural, so a
    differing key slot can never leak into the exchanged/consolidated output and
    leave a permanent cross-worker ghost.

### 4. `crates/gnitz-engine/src/ops/join.rs` — the operator

```rust
pub fn op_join_delta_trace_range(
    delta: &Batch, cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor, right_schema: &SchemaDescriptor,
    n_eq: usize, rel: RangeRel,
) -> Batch
```

Consolidate the delta first (as `op_join_delta_trace` :226 does; that operator
is adaptive and swaps to a trace-driven walk when `|Δ| > |trace|` — the range op
stays delta-driven in slice 1); derive
`eq_size` / `slot_size` from `right_schema`'s leading PK columns
(`debug_assert` the delta side agrees — common-`T` construction guarantees
it); per delta row compute `[start, end)` from the §3 table via the shared
cut-point helper; walk and emit through `write_join_row` unchanged (delta =
left). Mark the output unsorted (probe order is delta-major, payload-runs
arbitrary). No swapped/trace-driven variant in slice 1.

Shared helper: `storage/range_key.rs` (or `ops/util.rs`) hosting the
`increment_key_in_place` lifted from `catalog/store.rs:1824` plus a new
`range_cut_points(eq, d, rel, stride) -> Option<(start, Option<end>)>`
(`None` = provably empty) implementing the §3 table. `succ(eq)` for the
`Gt`/`Ge` `end` is `increment_key_in_place` on the `eq`-prefix sub-slice
followed by zero-padding the slot region. Only `increment_key_in_place` is
shared with `seek_by_index_range`; `range_cut_points` is range-join-specific
(byte/`RangeRel`-shaped, not native-`u128`/`Cut`-shaped), so the index scan
does not consume it.

`ops/linear.rs` (or `ops/exchange.rs`): `op_partition_filter(batch, worker_id,
num_workers) -> Batch` — copy-through of rows whose
`worker_for_partition(partition_for_pk_bytes(pk_bytes(row)), num_workers)`
equals `worker_id`. Identity (no copy) when `num_workers == 1`.

### 5. `crates/gnitz-engine/src/vm.rs` — two instructions

- `Instr::JoinDTRange { delta_reg, trace_reg, out_reg, right_schema_idx,
  n_eq: u8, rel: RangeRel }` — dispatch to `op_join_delta_trace_range`,
  trace-cursor plumbing copied from `JoinDT`.
- `Instr::PartitionFilter { in_reg, out_reg, worker_id: u32, num_workers: u32 }`.

### 6. `crates/gnitz-engine/src/compiler.rs` — emit arms + worker identity

- `OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel })`: output schema via
  `merge_schemas_for_join` (:779, unchanged); `builder.add_join_dt_range(…)`.
- `OpNode::PartitionFilter`: pass-through schema; emit with the worker's
  `(worker_id, num_workers)`.
- Worker identity is **already a compile-time static**: `WORKER_RANK`
  (`compiler.rs:23`) is set by `set_worker_rank(worker_id)` post-fork
  (`runtime/worker.rs:383`), where `num_workers` is also in scope (:375/:386).
  Reuse it: expose `worker_rank()` (currently file-private, :31) and add a
  sibling `NUM_WORKERS` static set in the same call. This is lighter than new
  DAG-context fields and matches the existing pattern; both default to
  `(0, 1)` for single-process and unit tests (`PartitionFilter(0,1)` =
  keep-all).
- `reindex_cols_through_filters` (:488) is untouched: it walks
  `ScanDelta → Filter* → Map(reindex)`; `PartitionFilter` sits *after* the
  Map. Verify with the existing unit tests plus one new case.
- The 1-exchange split (:2250-2319) already handles the range-join circuit
  (everything before `ExchangeShard` → pre, sink → post; the pre VM owns the
  trace tables exactly as equi-join pre-only plans do).

### 7. `crates/gnitz-engine/src/dag.rs` — broadcast detection + driver branch

- `view_is_range_join(view_id) -> bool`: any `Join(DeltaTraceRange { .. })`
  node in the meta circuit; cached like `join_shard_cols_cache`. This is the
  **only** correct branch discriminator (see below).
- `evaluate_dag_multi_worker` (:1167-1259): the runtime branch chain is
  `has_exchange && view_has_side_b` (two-sided set-ops, :1211) → `has_exchange`
  (:1216) → `has_join_shard` (:1240) → plain pre (:1256). Insert the range-join
  branch keyed on **`view_is_range_join(view_id)`**, placed **first in the
  chain** (before the two-sided arm at :1211). It must precede the `has_exchange`
  arm at :1216 because a range join has `has_exchange == true` (its output
  `ExchangeShard`) and would otherwise be swallowed there, which never relays
  the join input. **Do not** gate it on `has_join_shard && has_exchange`: that
  predicate is also true for every GROUP BY / reduce / single-sided set-op view
  (`has_join_shard` matches group reindexes — `compiler.rs:505`), so it would
  divert them into the broadcast path and corrupt them. `view_is_range_join` is
  exclusive (a range join is never a two-sided set-op), so placing it first is
  safe:

  ```rust
  let out_delta = if self.view_is_range_join(view_id) {
      // Range join: broadcast input relay → pre → output exchange → post.
      // No co-partition shortcut: a range probe needs the full delta even
      // when the join key equals the source PK.
      let mut input_ws = input;            // set source schema as :1246-1252
      let bc = exchange.do_exchange(view_id, &input_ws, src_id);
      let exchange_schema = self.get_exchange_schema(view_id)
          .unwrap_or_else(|| self.tables[&view_id].schema);
      let pre_result = match self.execute_pre_phase(view_id, bc, src_id) {
          Some(mut b) => { b.set_schema(exchange_schema); b }
          None => Batch::with_schema(exchange_schema, 0),
      };
      let post_in = exchange.do_exchange(view_id, &pre_result, 0);
      let post_in = Self::consolidate_exchanged(post_in, &exchange_schema);
      self.execute_post_phase(view_id, post_in)
  } else if has_exchange && self.view_has_side_b(view_id) {
      // ... existing arms unchanged ...
  ```

- `plan_source_co_partitioned` (:855) and `skip_exchange` (:1206-1209) must
  not short-circuit this branch (they are simply not consulted in it).

### 8. `crates/gnitz-engine/src/runtime/master.rs` + `ops/exchange.rs` — broadcast relay

- `prepare_relay` (:733) receives the per-worker source slices as
  `PendingRelay.payloads` (`Vec<Option<Batch>>`, one entry per worker — each
  worker's base-table-PK-partitioned slice — disjoint, so their concatenation is
  the full delta with no duplication). After the `is_join` classification (:745-756),
  when `is_join && cat.dag.view_is_range_join(view_id)` → skip the scatter and
  call a new `op_relay_broadcast(sources, num_workers)`: concatenate all
  non-empty payloads once, clone per worker (`Vec<Batch>` of length
  `num_workers`). It is a new sibling of `op_repartition_batches_mode` /
  `op_relay_scatter_consolidated_mode`, which take `col_indices` + `RouteMode`;
  the broadcast helper needs neither. `emit_relay` and the worker's
  `do_exchange_wait` are unchanged (per-worker batches are just larger). The
  `source_id == 0` output relay takes the existing `get_shard_cols` +
  `RouteMode::GroupKey` path verbatim.
- SAL relay space: a broadcast group carries ~`num_workers ×` the delta bytes;
  the existing `sal_has_relay_space` throttle covers it. (De-duplicating the
  shared bytes into one group read by all workers is a follow-on.)

## Correctness invariants to preserve

- **OPK ordering is the probe's whole correctness argument.** Byte compares
  against `[start, end)` are valid only because trace PK regions are
  order-preserving at the common promoted `T` on both sides: `encode_pk_column`
  (`gnitz-wire/src/pk.rs`) writes each PK column big-endian and flips the sign
  bit of signed columns at rest, so `compare_pk_bytes` is a plain memcmp that is
  numerically correct for unsigned and signed keys at any width. Never compare
  un-promoted natives; never hand-roll a bound encode — the bound *is* the
  delta row's packed PK bytes. (`foundations.md` §6 already documents this
  OPK-at-rest layout and its memcmp comparator; the probe relies on it as
  written — no foundations edit is needed.)
- **One packer still routes both sides.** Trace contents, the
  `PartitionFilter` ownership hash, and (in the equality fallback paths) the
  scatter all key off the same `ReindexPacker` output bytes. The ownership
  filter must hash the packed PK region, not re-derive from source columns.
- **Symmetric output identity.** Every emission for a pair `(a, b)` must
  reach the sink with byte-identical (PK, payload) regardless of term, epoch,
  or emitting worker: re-key onto the source-PK pair *after* the normalize
  projections, and exchange on exactly those columns. The synthetic `_join_pk`
  slots at union positions `0..k` **differ** between terms (the range slot is
  `OPK(a.x)` for term AB vs `OPK(b.y)` for term BA) and must not survive into the
  exchanged/consolidated output; they are dropped because every user and
  wildcard projection references only positions `≥ k`. Even `SELECT a.x, b.y` is
  safe: those re-project the *source-table copies* of the columns (carried in
  both sides' payload, identical across terms), not the synthetic key slots. Any
  asymmetry leaves permanent cross-worker ghosts.
- **Exchange schema contract** (dev-guide): the pre-plan's `out_reg` schema —
  the post-re-key, post-projection layout — is what the output relay
  serializes; `consolidate_exchanged` before the sink stays mandatory
  (probe output is unsorted, mixed-sign).
- **Z-set multiplicity**: output weight = `w_delta.wrapping_mul(w_trace)`,
  skip zero, no positivity assumption on trace nets.
- **Single-source-per-epoch** keeps `dA ⋈ dB = 0`; the delta-delta range term
  is never emitted; self-join stays rejected.
- **Barrier participation**: both `do_exchange` calls run on every worker
  every epoch, including empty deltas.
- **Equi-join byte-stability**: zero range conjuncts must produce a circuit
  byte-identical to today's (serialization included) — guard with the
  existing planner serialization tests.
- **NULL exclusion**: rows with NULL in any ON column never enter reindex on
  either side (filter precedes reindex; the trace never holds them).
- **Backfill rides the broadcast relay.** CREATE VIEW over populated tables must
  replay each source's existing rows through the *same* broadcast input relay as
  live deltas; a local-only backfill replay would miss every cross-partition
  match (`a` owned by `w`, `b` owned by `w'`). Match completeness then follows
  from replaying both sources: backfilling A builds `trace_A` (no matches yet),
  backfilling B fires term BA against the full `trace_A`.

## Migration order

1. **Shared cut-point helper** — lift `increment_key_in_place` from
   `catalog/store.rs:1824` into a shared module and build the §3
   `RangeRel`→cut-point derivation there. The single-table scan's `cut_key`
   logic is index-scan-specific and not reusable directly.
2. **Wire + builder** (§1, §2): opcodes, `RangeRel`, node columns, encode/
   decode round-trip tests. Compiles standalone.
3. **Operator** (§4): `op_join_delta_trace_range` + `op_partition_filter`
   unit-tested against seeded tables, no IPC.
4. **VM + compiler** (§5, §6): instructions, emit arms, worker identity.
   Single-process range-join views work end-to-end (`evaluate_dag` pre/post,
   broadcast degenerate).
5. **Dag driver + master broadcast** (§7, §8): multi-worker path.
6. **Planner** (§3): SQL surface; rejection tests; equi byte-stability test.
7. `make e2e` (`GNITZ_WORKERS=4`) with the suite below.

Steps 2–4 ship a working single-process feature; 5 makes it distributed; 6
exposes it.

## Testing

Engine unit (`ops/join.rs` tests, seeded trace + delta, no IPC):

- All four `RangeRel`s over an unsigned key: exact match sets, inclusive vs
  exclusive boundaries (`x == y` matches `Le`/`Ge` only).
- Equality prefix (`n_eq = 1`): probe stops at the group edge — a trace row in
  the next `eq` group with an in-range slot must not match; `Le` with
  `d = 0xFF…` ripples into the next group (empty tail, no panic).
- `n_eq = 0`, `rel = Lt`, multiple delta rows: exercises backward re-seek of
  one cursor.
- Signed range pair via cross-sign promotion (e.g. `U32` vs `I64` at common
  `T = I64`) including negatives: contiguous typed interval, no inversion —
  guards signed-key order inversion at the trace layer.
- Weights: delta retraction rows (`w_delta = -1`) emit negated products;
  `w_trace = 2` doubles; zero products skipped.
- `op_partition_filter`: rows kept ⟺ owned partition; `(0, 1)` keeps all;
  empty in → empty out.
- Cut-point helper: the four rels × {interior value, all-`0x00`, all-`0xFF`,
  ripple into eq prefix}, plus `succ` unit coverage.

Planner unit (`gnitz-sql`):

- rel mapping per the §3 table for all four operators, both operand orders.
- Rejections: two range conjuncts; OR; LEFT + range; string/blob range pair;
  `pa + pb > 4`; self-join. Equi-only ON: circuit serialization byte-identical
  to today.

Multi-worker E2E (`make e2e`, `GNITZ_WORKERS=4`, new `TestRangeJoin` in
`test_workers.py`):

- Pure range join `ON a.x < b.y`, rows scattered across partitions, both
  insert orders (a-then-b, b-then-a) — result equals a python-side
  cross-filter reference. All four operators.
- Band join `ON a.k = b.k AND a.lo <= b.t` — composite trace key, group-edge
  correctness.
- **Retraction across workers** (the load-bearing test): insert matching
  `(a, b)`, verify the view row; delete `b` → row gone (the `-1` was emitted
  by the opposite term on a different worker and must cancel through the
  output exchange); re-insert → row back. Repeat deleting `a`. UPDATE
  (retract+insert) on the range column moving a pair in/out of range.
- Backfill: CREATE VIEW over already-populated tables yields the full match
  set (broadcast path under backfill replay).
- Scan and PK-seek on the view (pair-PK routing matches GroupKey scatter
  routing — the compound-key alignment invariant).
- NULL range/eq column rows match nothing.
- Signed band over the wire (negative bounds survive promotion + broadcast).
- Empty-delta epochs (push rows matching nothing) — no barrier deadlock with
  the double exchange.
- View-over-range-join-view (e.g. GROUP BY over `v`) stays consistent under
  retraction — output deltas are well-formed for downstream circuits.
- **Branch-routing regression** (guards the §7 discriminator): in the same
  cluster as a range-join view, run a plain GROUP BY view and a single-sided
  set-op view (both have `has_join_shard == true` and `has_exchange == true`)
  and confirm they still produce correct results — i.e. they take the
  `has_exchange` arm, not the range-join broadcast branch. A Rust unit test on
  the dag asserting `view_is_range_join` is `false` for a GROUP BY meta-circuit
  pins the predicate cheaply.

## Out of scope (follow-ons, in rough order of value)

- **Equality-prefix co-partition scatter** (`n_eq ≥ 1`): scatter both sides by
  the equality prefix instead of broadcasting — restores single-destination
  routing and partition-local probes for band joins. Same circuit, different
  relay mode + no `PartitionFilter`; needs the prefix-only hash on both legs.
- **LEFT/OUTER range join** (range-aware anti-join semantics).
- **Multiple range conjuncts / residual ON predicates** (post-join `Filter`
  over the normalized output — the machinery exists, the 3VL bookkeeping and
  planning surface do not).
- **Probe-loop performance**: monotone cursor reuse for `Gt`/`Ge`,
  shared-prefix single walk for `Lt`/`Le`, a trace-driven swapped variant for
  `|Δ| > |trace|`.
- **Broadcast relay dedup**: one shared SAL group read by all workers instead
  of `num_workers` cloned batches.
- **Range-aware (order-preserving) exchange**: range-partition both sides by
  key intervals so probes touch only boundary-overlapping workers — the
  general fix for broadcast cost at large `W`; requires partition-boundary
  metadata and rebalancing. The broadcast design above is deliberately the
  minimal correct distribution.
