# Ad-hoc bounded reads: the parameterized scan (`ReadSpec`)

## 1. Problem

Every direct-SELECT access path fetches its **entire** matched set to the client and applies
WHERE-residual, projection, ORDER BY, OFFSET and LIMIT there:

- `AccessPath::ScanAll => client.scan(tid)` (`crates/gnitz-sql/src/dml/select.rs:101`) has no limit
  parameter anywhere in the chain (`GnitzClient::scan`, `crates/gnitz-core/src/client.rs:427`), so
  `SELECT * FROM t LIMIT 10` transfers the whole table.
- `client.seek_by_index` / `seek_by_index_range` return every index-matched row; the residual filter
  runs client-side (`crates/gnitz-sql/src/exec/residual.rs`), so a low-selectivity prefix transfers
  ~the table and discards it client-side.
- The residual interpreter (`InterpBackend`, `crates/gnitz-sql/src/exec/eval.rs`) only evaluates
  fixed-width-integer expressions; one string/float conjunct anywhere in the WHERE disqualifies every
  index candidate (`bind_thin_residuals`, `select.rs:230-241`) and reroutes the whole query — index
  probe included — to the transient executor's full scan.
- `WHERE pk IN (v1..vN)` runs N sequential `client.seek` round trips (`seek_pk_multi`,
  `crates/gnitz-sql/src/dml/plan.rs:249-275`); `WHERE pk = 1 OR pk = 2` gets no fast path at all.
- PK range predicates (`WHERE pk > 5`) never bound anything: the thin path has no PK-range arm, and
  the shared equality collector skips PK columns (`collect_eq_conjuncts`,
  `crates/gnitz-sql/src/dml/plan.rs:405`; pinned for the circuit path by `pk_equality_never_bounds`,
  `crates/gnitz-sql/src/plan/index_bound.rs:203-208`).
- No column pruning: the scan/seek wire protocol carries no projection, so `SELECT id FROM t` ships
  every BLOB column of every matched row.
- The ordering sink (`crates/gnitz-sql/src/exec/order.rs`) is correct but operates after the full
  transfer; OFFSET pagination re-transfers the matched set per page.

This plan moves bound extraction, predicate evaluation, projection, and ORDER BY/LIMIT top-k onto
the workers as a **parameterized scan**: one wire descriptor (`ReadSpec`) carried by the existing
scan machinery, executed as a stateless, single-pass, bounded-memory sink over each worker's merged
partition cursor. No DBSP circuit, no operator state, no exchange. SELECT and the committed-row
fetch of UPDATE/DELETE move onto it; the thin-path type gate and the client-side full-batch sort
are deleted.

Out of scope here (unchanged behavior): aggregates, DISTINCT, GROUP BY, joins, set-ops, subqueries
and CTEs keep their current routing; the transient executor is untouched.

## 2. Design overview

```
SQL layer                      wire                    master                one worker
─────────                      ────                    ──────                ──────────
plan_read_spec(select):        FLAG_SCAN_SPEC          route by bound:       ONE merged cursor over all
  bound   (PK range | index    header + ReadSpec       full-pin PkRange →      owned partitions
           range | PK set)     blob in seek_pk_extra   owner worker;           (Full | PkRange walk |
  predicate (expr blob)        + reply-schema block    else fan out            BoundedIndexCursor |
  projection (map blob)                                (replicated → W0);      PK-set gather)
  order keys + limit_k                                 view target →         loop drain_chunk:
                                                       drain ticks first       op_filter(predicate)
client: k-way-merge W                                  forward trains,         op_map(projection)
per-worker sorted runs                                 mark worker-final       sink: top-k | early-stop
(full order comparator),                               frames                  | sort | passthrough
window offset/limit,                                                         reply: schema always
strip hidden columns                                                         in-frame, cache-bypassing
```

The reply row shape: the source PK region is always physically present and **always hidden**;
every SELECT item — PK columns included — is materialized in the payload region in SELECT order
via `COPY_COL`; ORDER BY keys absent from the projection are appended as hidden payload columns.
This is a **SELECT-specific variant** of `place_pk_front`
(`crates/gnitz-sql/src/codec/project_schema.rs:113-140`): the shared function *promotes* a
user-projected PK to the visible front slot (the CREATE VIEW behavior, which stays), whereas the
read path always hidden-prepends the PK and emits any projected PK column as an ordinary payload
`COPY_COL`. Engine support is confirmed: `from_map` builds one independent `ColMove` per
`COPY_COL` (duplicates fine) and copies PK-source columns into payload slots
(`crates/gnitz-engine/src/expr/plan.rs:286-310`); the PK-region bulk copy fires because in/out PK
strides match (`expr/plan.rs:462-466`). The engine `SchemaDescriptor` carries no hidden concept —
hidden flags live only in the wire schema block (`hidden_mask`, `META_FLAG_HIDDEN`) — so the
worker **echoes the client's raw reply-schema block bytes** in replies, never rebuilding the block
from the descriptor. Duplicate names are safe because name resolution skips hidden columns
(`find_unique_column`, `crates/gnitz-sql/src/bind/resolve.rs:61-74`). `tbl.*` stays unsupported
(parity: `resolve_proj_col` rejects qualified wildcards, `project_schema.rs:55-59`).

## 3. Wire: `ReadSpec` (gnitz-wire)

New module `crates/gnitz-wire/src/read_spec.rs`, re-exported beside `RangeDescriptor`. One
definition consumed by client and engine.

```rust
pub struct ReadSpec {
    pub bound: ReadBound,
    /// Compiled predicate over the SOURCE schema ("EXPR" blob, `decode_expr_blob` format).
    /// Empty = no residual (bound alone is exact).
    pub predicate: Vec<u8>,
    /// Compiled projection map program over the SOURCE schema producing the reply schema's
    /// payload columns. Empty = identity (reply schema == source schema).
    pub projection: Vec<u8>,
    /// ORDER BY keys, applied in sequence; column indices refer to the REPLY schema.
    pub order: Vec<OrderKey>,      // len ≤ MAX_ORDER_KEYS
    /// OFFSET+LIMIT in logical rows (summed weight). 0 = unbounded.
    pub limit_k: u64,
}

pub enum ReadBound {
    None,
    /// Range over the PK column list, `RangeDescriptor` semantics: n_eq leading equality pins
    /// plus start/end cuts on the next column. A full-PK point lookup is expressed as
    /// n_eq = pk_count − 1 with the degenerate cuts [Before(v), After(v)] on the last column
    /// (the `seek_by_index` precedent, `catalog/store_io.rs:171-182`) — `n_eq` never equals the
    /// column count (`index_range_keys` rejects that shape, `store_io.rs:432-439`).
    PkRange(RangeDescriptor),
    /// Secondary-index range: packed index column list + descriptor — the same pair
    /// `seek_by_index_range` ships today (`pack_pk_cols` + `RangeDescriptor`).
    IndexRange { idx_cols: u64, desc: RangeDescriptor },
    /// `pk IN (…)` for single-column PKs: native u128 values, ascending, deduplicated.
    PkSet(Vec<u128>),
}

pub struct OrderKey {
    pub col: u16,          // reply-schema column index (may name a hidden column)
    pub desc: bool,
    pub nulls_first: bool,
}

pub const MAX_ORDER_KEYS: usize = 8;
pub const MAX_PK_SET_KEYS: usize = 65_536;
pub const MAX_READ_SPEC_BYTES: usize = 2 << 20;   // 2 MiB; max PkSet (1 MiB) fits with headroom
```

Byte layout (version-prefixed, little-endian, fixed order):

```
u8  version = 1
u8  bound_kind            0=None 1=PkRange 2=IndexRange 3=PkSet
u8  n_order_keys          ≤ MAX_ORDER_KEYS
u8  reserved = 0
u64 limit_k
[bound]     PkRange: RangeDescriptor::encode() (2 + 16·(n_eq+2) bytes, self-sizing)
            IndexRange: u64 idx_cols, then RangeDescriptor::encode()
            PkSet: u32 count (≤ MAX_PK_SET_KEYS), then count × u128 LE, ascending, deduped
[order]     n_order_keys × { u16 col, u8 flags (bit0 desc, bit1 nulls_first), u8 reserved }
[predicate] u32 len, then len bytes ("EXPR" blob; len 0 = none)
[projection]u32 len, then len bytes ("EXPR" blob; len 0 = identity)
```

`ReadSpec::encode() -> Vec<u8>` / `ReadSpec::decode(&[u8]) -> Result<ReadSpec, &'static str>` with
every cap enforced at decode (count/length overflow, unsorted/duplicate PkSet, trailing bytes,
unknown kinds/versions all rejected — same trust posture as `RangeDescriptor::decode`).

### 3a. Flag and framing

- **`FLAG_SCAN_SPEC = 1 << 10`** (1024). Bit 9 is **taken** on the SAL leg
  (`FLAG_EXCHANGE_RELAY: u32 = 512`, `crates/gnitz-engine/src/runtime/protocol/sal.rs:100`,
  tested by `classify` before any scan arm); bit 10 is free in both the u64 wire namespace
  (`crates/gnitz-wire/src/flags.rs`) and the u32 SAL namespace (gap between `EXCHANGE_RELAY`=512
  and `BACKFILL`=2048). The engine adds
  `pub const FLAG_SCAN_SPEC: u32 = gnitz_wire::FLAG_SCAN_SPEC as u32;` in `sal.rs` plus a
  **cross-namespace disjointness assert** in `sal.rs` enumerating every low-bit SAL flag against
  every low-bit wire flag — the existing `flags.rs:212-242` assert treats bits 0-15 as one opaque
  block and cannot catch such collisions; extend it to check low-bit wire flags pairwise as well.
- **Request encoder** (new, `crates/gnitz-core/src/protocol/message.rs`): no existing encoder
  carries an arbitrary `seek_pk_extra` blob *and* a schema block (`send_message_with_extra` is
  documented schema-free, `message.rs:354-381`; `encode_message_parts` derives `seek_pk_extra`
  from `PkTuple::split_wire`, capped at 64 B). Add `encode_scan_spec_parts(target_id, client_id,
  flags, spec_blob, schema_block)` producing a control block whose `seek_pk_extra` BLOB column
  holds the spec and whose schema block is the reply schema (built like the transient out-schema
  block: names, type codes, pk indices, hidden flags). Request size stays far below
  `MAX_FRAME_PAYLOAD_SERVER` (64 MiB).
- **Scan-group writer** (new, `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs`):
  `write_one_scan_group` injects the server's cached schema block and empty `seek_pk_extra`
  (`dispatch.rs:165,178-179`), so it cannot carry the spec. Add `write_scan_spec_group` over
  `write_group_with_req_ids` (`dispatch.rs:110-148`), which already exposes
  `prebuilt_schema_block` + `seek_pk_extra` — thread the client's spec blob and reply-schema block
  through verbatim.

## 4. Engine: decode, route, execute

### 4a. SAL classification and dispatch

- `SalMessageKind::ScanSpec`, classified from the SAL-side `FLAG_SCAN_SPEC` — insert in `classify`
  (`sal.rs:285-334`) with priority directly above `SEEK_BY_INDEX_RANGE`.
- `is_broadcast()` = false (unicast slots per worker; a missing slot means "not for us"), matching
  `Seek`/`Scan` (`sal.rs:372-387`).
- `is_live_point_traffic()` = **true** — a ScanSpec is a read exactly like `Scan`/`Seek`
  (`sal.rs:353-365`). This adds no new reentrancy class: `Scan` and `SeekByIndexRange` already run
  inline in `InEval` (`worker/mod.rs:592-595`).
- The worker dispatch matrix (`match (ctx, kind)`, `worker/mod.rs:565-781`) gains the
  `(TopLevel, ScanSpec)` and `(InEval, ScanSpec)` cells (both inline, the "all others" posture at
  `:768-780`), the doc-table row (`:577-595`), and an entry in the walk-the-matrix test
  (`:2165-2166`).

### 4b. Master routing (`runtime/orchestration/master`)

New handler `handle_scan_spec` in `executor.rs` beside `handle_scan`, and a
`fan_out_scan_spec_async` in `master/dispatch.rs` mirroring `fan_out_scan_async`
(`dispatch.rs:1125-1156`) — the same generic train forwarding (`forward_scan_slots` keys on
slots/req_ids, not request kind) and terminal frame. Differences:

1. **View freshness**: if `target_id` is a view, drain pending ticks first via the existing
   `drain_then_lock` discipline (`executor.rs:1817-1824`), exactly as `handle_scan` does. The
   catalog-schema negotiation (`negotiate_scan_schema`, `executor.rs:1840-1848`) is **not** used —
   the reply schema is the client's block, passed through.
2. **Fan-out set**, from a routing-only decode of the spec (the master validates the frame
   structurally; it never compiles the programs):
   - `PkRange` whose cuts pin one full key (n_eq = count−1, `[Before(v), After(v))`): the master
     derives the routing OPK **from the descriptor's pinned values** via a new master-side helper
     `opk_from_pins(&SchemaDescriptor, &RangeDescriptor) -> PkBuf`: pack each `eq`/cut value at
     its native column width into a native-LE buffer, then encode through the existing source-PK
     encoder `key::opk_key` (`crates/gnitz-engine/src/schema/key.rs:96`) — the master already
     resolves the `SchemaDescriptor` via `get_schema_desc` (`master/dispatch.rs:798-802`).
     `seek_opk_bytes` is unusable here: it reads `seek_pk_extra` as the wide-PK suffix
     (`schema.rs:97-119`), and that channel now carries the spec. Route to
     `worker_for_partition(schema.partition_for_pk(..), nw)`.
   - replicated relation: unicast to worker 0 (`replicated_unicast`, `master/mod.rs:150-157`).
   - everything else: fan out to all workers.
3. **Worker-final run boundaries need no master change.** The worker already sets
   `FLAG_SCAN_LAST` on the terminal frame of every train (`worker/reply.rs:310,401,500`), the
   master forwards slots verbatim (`drain_scan_train` → `send_slot`, `dispatch.rs:1640`), and the
   client's `recv_scan` simply ignores the flag today (`connection.rs:433`). Two existing
   guarantees are load-bearing and pinned here: every dispatched worker sends **at least one**
   (possibly empty) marked frame (the master joins all slots, `await_scan_slots`), and the
   master's terminal scan frame (flags 0, no `FLAG_CONTINUATION`,
   `make_terminal_scan_frame`, `executor.rs:1893-1906`) ends the stream — so the client needs no
   knowledge of the worker count.

### 4c. Worker execution (`runtime/orchestration/worker`)

New arm `SalMessageKind::ScanSpec` delegating to catalog-level
`scan_spec_family(target_id, &spec, &reply_schema)` so the fetch stays in the catalog layer like
`scan_family`/`seek_family`. Execution runs **once per worker over one merged cursor** —
`PartitionedTable::open_cursor` already merges every owned partition's runs and shards into a
single globally (PK, payload)-sorted, ghost-folding `ReadCursor`
(`storage/lsm/partitioned_table.rs:226-249`); every existing read works this way and so does this
one. There is no per-partition-slot loop.

1. **Cursor from bound**:
   - `None` → the merged `open_cursor()`.
   - `PkRange` → derive `[start: PkBuf, end: Option<PkBuf>)` against the PK column list via a new
     `pk_range_keys` sibling of `index_range_keys` (`catalog/store_io.rs:419-478`), then
     `cursor.seek_bytes(start)` + advance while `current_pk_cmp_bytes(end) == Less` — the
     established bounded-walk pattern (`storage/lsm/read_cursor/mod.rs`).
   - `IndexRange` → the existing bounded machinery: resolve the index, `index_range_keys`,
     `count_range_raw` selectivity gate (`INDEX_SCAN_RATIO = 16`,
     `catalog/store_io.rs:344,362`), `BoundedIndexCursor` on pass, full cursor on fail. The gate +
     open logic is extracted from `open_source_cursor` (`store_io.rs:291-358`) into a shared
     `open_bounded_source(table, idx_cols, desc) -> SourceCursor` consumed by both the circuit
     backfill path and this one — one implementation, two callers.
   - `PkSet` → sorted-key gather over the merged cursor via `advance_to_exact_live`
     (`read_cursor/mod.rs:376-379`), the `gather_family_bytes` pattern
     (`catalog/store_io.rs:103-137`): one live row copied per matched key. **Pinned assumption**:
     a `PkSet` target is unique on the named PK — `try_extract_pk_in` resolves the column via
     `find_unique_column`, which skips hidden columns, so only base-table PKs (DML-enforced
     `unique_pk`) and simple-view inherited base PKs are nameable; synthetic non-unique view keys
     are hidden and unreachable. If a nameable non-unique single-column PK ever becomes
     reachable, this gather must switch to a full-group walk. Keys the worker doesn't own simply
     miss (partition placement guarantees absence); no ownership pre-filter needed.
2. **Compile programs once per request** (not per chunk): predicate →
   `decode_expr_blob` → `LogicalProgram::from_wire(..).validate(Some(src_schema), None)` →
   `ScalarFunc::from_predicate` (`crates/gnitz-engine/src/expr/program.rs:453`,
   `expr/plan.rs:273`); projection → same decode →
   `validate(Some(src_schema), Some(reply_schema))` → `ScalarFunc::from_map`. Any validation
   failure is a clean per-request error reply (`CompileError::Rejected` posture,
   `query/compiler/emit.rs:14-19`) — client-supplied programs are untrusted input and are already
   fully validated by this path. The reply schema block is validated structurally (decode +
   `SchemaDescriptor` construction) before use.
3. **Chunk loop**: `drain_chunk(ddl_scan_chunk_rows)` (`storage/lsm/read_cursor/output.rs:171-179`;
   default `DDL_SCAN_CHUNK_ROWS = 65_536`) → `op_filter(chunk, pred, schema)`
   (`ops/linear.rs:21`) → `op_map(chunk, proj, ..)` (`ops/linear.rs:78`) → sink:
   - **Top-k** (`order` non-empty, `limit_k > 0`): keep a single keeper `Batch`; append surviving
     rows; when keeper logical rows (summed weight) exceed `2 × limit_k`, sort the keeper by the
     order comparator and truncate to the smallest prefix whose summed weight ≥ `limit_k`
     (rows kept whole — a weight-w row at the boundary is never split; the client window does the
     exact split). Amortized O(n log k) with at most ~2k logical rows resident. After the loop,
     sort once more and truncate. Once the keeper is full, its current maximum is a cutoff: chunk
     rows ordering strictly after it are dropped before appending.
   - **Sorted, unbounded** (`order` non-empty, `limit_k == 0`): accumulate all survivors, one
     final sort. Same worst-case memory as today's `full_scan` reply; the client-side sort is
     replaced by a W-way merge.
   - **Early-stop** (`order` empty, `limit_k > 0`): append until summed weight ≥ `limit_k`, then
     stop draining chunks (for `PkSet`, stop seeking further keys).
   - **Passthrough** (neither): append all survivors.
4. **Order comparator**: keys compare via the reply batch's payload columns through
   `cmp_col_window` (`storage/repr/columnar.rs:95-100`; German strings via
   `compare_german_strings`, floats via `total_cmp`); NULL placement per `nulls_first` (absolute,
   not flipped by `desc` — the existing sink's semantics, `exec/order.rs:34-46`). Ties beyond the
   user keys break by canonical (PK bytes, payload) order (`compare_pk_bytes` + `compare_rows`) —
   total and deterministic, so pagination is stable across runs and worker counts.
5. **Reply**: one batch per worker, framed by `send_scan_response` (`worker/reply.rs:233-336`).
   The **first** frame of a worker's train carries the reply schema **unconditionally** — the
   spec reply path bypasses `wire_should_include_schema` and stamps schema version 0, because
   `ReplySchema::OneOff` otherwise reports the table's real version (`worker/reply.rs:79-82`) and
   a warm client cache would suppress the block. The schema block sent is the **client's raw
   block bytes echoed** from the SAL group's `prebuilt_schema_block` — never rebuilt via
   `build_schema_wire_block` from the hidden-agnostic `SchemaDescriptor`, which would drop the
   hidden flags. Continuation frames omit the block as today (`reply.rs:473-481`). Chunking,
   `MAX_W2M_MSG`, and the STRING single-frame limitation behave exactly as for plain scans;
   bounded results make the STRING ceiling practically unreachable, and the unbounded-STRING case
   is no worse than today.

Weights flow through untouched: the merged cursor emits net-consolidated (PK, payload) groups
exactly once (`read_cursor/mod.rs:1-4`; chunk boundaries never split groups), so summed-weight
accounting in the sinks is exact. Live rows carry weight ≥ 1 — `debug_assert!(w > 0)` in the
sinks.

### 4d. Client receive path (cache-bypassing)

`recv_scan`'s LRU (`schema_cache.put(target_id, ..)`, `crates/gnitz-core/src/connection.rs:512-514`)
would cache a per-query projected schema under the **table's** id and corrupt subsequent plain
reads. Spec replies therefore use a dedicated `recv_scan_spec` loop that (a) never touches the
schema cache — it receives via `recv_message(fd, None, ..)` (the `fetch_indices` posture,
`connection.rs:361-378`) in a multi-frame loop mirroring `recv_scan`'s `FLAG_CONTINUATION`
handling, (b) decodes the first frame's in-frame schema into a **local** variable and passes it
as the per-frame schema hint for the block-less continuation frames (`reply.rs:473-481` gates the
block on `is_first`), and (c) splits the accumulated frames into per-worker sorted runs at each
`FLAG_SCAN_LAST` marker, ending on the master's terminal frame (no `FLAG_CONTINUATION`) without
minting an empty trailing run. Returns `Vec<ZSetBatch>` (one run per worker) to the SQL layer.

### 4e. Expr VM: `IN_SET` opcode (integer set membership)

`col IN (…)` currently desugars at bind into an OR-chain of equalities
(`crates/gnitz-sql/src/bind/structural.rs:187-208`); `ExprBuilder` allocates registers
monotonically (`crates/gnitz-core/src/expr.rs:51-53`) and the engine caps programs at
`MAX_REGS = 64` (`expr/program.rs:26,817`) — so any integer IN list beyond ~15 items cannot
compile, today on the executor path and after this plan on the read path. Fix it at the root, for
views and ad-hoc reads alike:

- `BoundExpr::InSet { col: usize, values: Vec<i64> }` — the binder keeps integer-literal IN lists
  intact instead of desugaring. Pool values are the literals' **i64 register images** (exactly
  what `LOAD_CONST` produces — the VM's equality is bit-pattern equality, `CmpOp::Eq` ignores
  signedness, `expr/batch.rs:717`), sorted ascending under signed-i64 order, deduped. This
  preserves today's bit-equality semantics (`u64col IN (-1)` matches `u64::MAX`, both in the
  engine and in `eval_expr`'s bitcast compare, `exec/eval.rs:183-188`).
- **Type gate**: `InSet` applies only when the tested column is a ≤8-byte integer type. The bind
  leaf is schema-free, so the gate lives in the consumers: each backend's `in_set` arm checks the
  column's type code and, for a float column, expands to the OR-chain float-unified comparison
  inline (today's behavior: `int_to_float` + `fcmp_eq`, `lower.rs:273-297`); wide-int and string
  columns keep today's errors. Without this gate, `WHERE fcol IN (1, 2)` would silently compare
  raw f64 bit patterns against integer images and match nothing.
- Wire blob format v2: a const-set pool section after the const-strings section (u16 set count;
  per set: u32 len + len × 8-byte LE i64 values, ascending). v1 blobs remain decodable.
- New opcode `INT_IN_SET { value_reg, set_idx }`: binary search of the register's i64 value in
  the pool; NULL input → NULL output (3VL — composes correctly with `BOOL_NOT`'s
  `NOT NULL = NULL`, `expr/batch.rs:830-833`, so `NOT IN` over a NULL excludes, SQL-correct).
- `OpcodeBackend` lowers `InSet` to one `INT_IN_SET`; the retained DML evaluator (`eval_expr`,
  §5b) walks it directly; `ThinProbe` gets a transient `in_set` arm returning "not thin" until
  its deletion later in this plan's sequence.

The `PkSet > MAX_PK_SET_KEYS` case does **not** fall back to a predicate: the SQL layer splits the
key set into ⌈N / MAX_PK_SET_KEYS⌉ successive PkSet requests and treats each reply's runs as
additional runs in the same client merge/window (correct because runs are per-worker sorted and
PK-disjoint across chunks). Without ORDER BY, the client stops issuing further chunk requests once
the accumulated logical rows satisfy `limit_k`. **Snapshot pin**: each chunk request observes its
own committed prefix — exact parity with today's `seek_pk_multi`, whose N sequential seeks are N
independent snapshots (`dml/plan.rs:259-273`).

## 5. SQL layer: one read planner, scoped deletions

### 5a. `plan_read_spec` replaces the thin ladder

`crates/gnitz-sql/src/dml/select.rs` — `execute_select` keeps its shape-routing preamble (set-ops /
joins / DISTINCT / GROUP BY / HAVING / aggregates / WITH still route to
`execute_select_via_executor` unchanged), but every single-relation non-aggregate SELECT — bare or
computed projection alike — now builds a `ReadSpec`:

1. **Bound extraction** (evolving `classify_access` + the candidate collectors in `dml/plan.rs`):
   - Full-PK equality → `PkRange` with degenerate cuts (§3).
   - `pk IN (…)` (single-column PK; `try_extract_pk_in` already dedups,
     `dml/plan.rs:222-238`) and `pk = a OR pk = b OR …` (new: OR-of-equalities over the same
     single PK column normalizes to the same set) → `PkSet` (sorted, deduped; oversized sets are
     chunked per §4e).
   - PK prefix equalities + one PK range conjunct → `PkRange` (new arm — requires lifting the PK
     skip in `collect_eq_conjuncts` (`dml/plan.rs:405`) for this caller; the circuit path keeps
     its PK skip in `plan/index_bound.rs`).
   - Else the existing secondary-index candidate collectors
     (`collect_index_range_candidates` / `collect_index_seek_candidates`) — now **without**
     `bind_thin_residuals` pre-filtering (every candidate is viable; residuals always run
     server-side) — first structural hit wins as today → `IndexRange`.
   - Else `None`.
2. **Predicate**: the residual conjuncts (WHERE minus bound-consumed conjuncts) bound via
   `bind_single_table` and compiled with `compile_filter_program`
   (`crates/gnitz-sql/src/lower.rs:373`) — the same producer views use, so type coverage and
   NULL/3VL semantics are identical to view filters by construction. Binder rejections (128-bit
   comparisons, LIKE, string arithmetic) surface with the existing view-expression error text.
3. **Projection + reply schema**: `build_projection` → `ProjItem`s → `compile_projection_map`
   (`crates/gnitz-sql/src/codec/project_schema.rs`), with the SELECT-specific PK placement of §2:
   the source PK is always hidden-prepended and a user-projected PK column becomes an ordinary
   payload `PassThrough` in SELECT order — a new variant beside `place_pk_front`, whose
   promote-to-front behavior is CREATE VIEW's and stays. NOT `resolve_projection`
   (`exec/batch.rs:91`), which rejects computed expressions. ORDER BY key columns absent from the
   projection are appended as hidden `COPY_COL` outputs.
4. **Order/limit**: source-column ORDER BY keys resolve as today and map to their reply-schema
   (possibly hidden) payload index; a key naming a **computed alias** resolves directly to that
   alias's reply-schema index (no source column exists). ORDER BY expressions stay rejected
   (parity: `exec/order.rs:168-184`). More than `MAX_ORDER_KEYS` keys is a clean `Unsupported` —
   a (works → error) change from today's uncapped sink, accepted. `limit_k = offset + limit` when
   a LIMIT exists; `OFFSET` without `LIMIT` sends `limit_k = 0` (unbounded) and windows
   client-side.
5. **Client finish** (`exec/order.rs`, rewritten small): k-way-merge the per-worker runs from
   `recv_scan_spec` using the **full order comparator** — per-key DESC and absolute NULL
   placement, key sequence, then the (PK bytes, payload) tiebreak read from the hidden PK region
   and payload columns — byte-order-equivalent to the worker comparator (`cmp_typed_le` is shared,
   `crates/gnitz-wire/src/types.rs:191-227`; German-string order reduces to content-byte order;
   client typed PK compare ≡ OPK memcmp by the §6 bijection). Then the existing
   multiplicity-aware window (`paginate`) for OFFSET/LIMIT, then strip hidden columns for
   presentation. The full-batch sort survives only as `order_limit_passthrough` for
   executor-path results.

### 5b. UPDATE/DELETE: committed fetch moves; value evaluation and txn overlay stay client-side

`resolve_where_rows` (`crates/gnitz-sql/src/dml/mutate.rs:194-249`) changes only its
committed-row fetch:

- **No open-transaction buffer for the target table** (`net.is_empty()`): fetch via the spec —
  bound + full server-side predicate, identity projection, `order = []`, `limit_k = 0` — and use
  the returned rows directly (concatenate runs; ordering is irrelevant here). A no-WHERE DML
  fetch (the ScanAll arm) keeps plain `client.scan` — the SQL layer never emits an identity spec.
- **Buffered writes present**: fetch via the spec with the **bound only** (no server predicate),
  then run today's unchanged overlay pipeline, whose client-side predicate scope stays
  **per-bound** exactly as `resolve_where_rows` has it today (`mutate.rs:200-224`):
  - PK-point (`PkRange` full pin) and `PkSet` bounds: evaluate only the **residual** conjuncts
    over the `buffered_keys` overlay — the PK conjuncts live in the bound, so a PK type the
    client evaluator cannot compare (a UUID/U128 PK equality) never reaches it, exactly as
    today.
  - All other bounds: evaluate the **full WHERE** over the `buffered_all` overlay
    (`overlay_batch` re-inserts every buffered full row even when the committed fetch missed it,
    `overlay.rs:77-110`, which is what keeps an index-bounded fetch correct against buffered
    rows whose indexed columns changed).

Therefore the client-side machinery is **scoped, not deleted**:

- `eval_expr` (`exec/eval.rs:226`) stays — it is the DML row-value evaluator powering UPDATE SET
  (`eval_set_expr`, `mutate.rs:46,133`) and INSERT ON CONFLICT DO UPDATE (`insert.rs:465-466`),
  and the overlay re-filter. It gains an `InSet` arm (§4e).
- `residual.rs`'s `bind_residuals`/`matching_indices` stay for the overlay path only.
- **Deleted**: `expr_is_thin`/`ThinProbe` (the thin type gate), `bind_thin_residuals`,
  `projection_is_thin`, the thin/executor fork in `select.rs`, `seek_pk_multi` and its sequential
  loop, and every read-path use of the interpreter on committed rows.

Two pinned semantic changes ride the evaluator switch for committed-row matching (both toward the
engine/view semantics, both tested):

- **U64 ordered comparison** becomes unsigned (the interpreter bit-reinterprets U64 as `i64` and
  compares signed — `exec/eval.rs:26,56-62,183-188`; the engine compares unsigned,
  `expr/batch.rs:719-726`). Values ≥ 2^63 now compare correctly.
- **Integer division/modulo by zero** yields NULL (interpreter returns 0, `eval.rs:169-182`;
  engine nulls the row, `expr/batch.rs:504-525`), so `WHERE a/b = 0` with `b = 0` no longer
  matches.

The overlay evaluator (`eval_expr`) is aligned to the same two semantics in this plan, so a
predicate never evaluates differently client-side vs server-side.

### 5c. Wire-op deletion: `SeekByIndexRange` only

- **Deleted end-to-end (wire op only)**: `FLAG_SEEK_BY_INDEX_RANGE`, `SalMessageKind::SeekByIndexRange`,
  the worker arm (`worker/mod.rs:1007-1026`), the master handler + `fan_out` path
  (`executor.rs:1090`, `dispatch.rs:1694`), the client encoder (`connection.rs:334-352`) and
  `GnitzClient::seek_by_index_range`. The **catalog** range-walk body stays: the kept
  `seek_by_index` is implemented as the degenerate range and delegates to it
  (`store_io.rs:171-182`); the body is re-pointed under the shared `open_bounded_source`
  extraction so one range-walk implementation serves `seek_by_index`, circuit backfills, and
  spec `IndexRange` bounds. Engine tests in `catalog/tests/index_tests.rs` keep passing against
  the catalog method; any capi/gnitz-py range binding ports to `scan_spec`.
- **Kept**: `SeekByIndex` end-to-end — it is engine-internal infrastructure (unique-holder
  verification: `seek_unique_holder`, `master/dispatch.rs:1069-1110`; preflight callers at
  `preflight.rs:757,1408`) and a public point-lookup API (capi `gnitz_seek_by_index`,
  `gnitz-capi/src/lib.rs:1288,1307`; gnitz-py `Session::seek_by_index`,
  `gnitz-py/src/lib.rs:1674,1687`). `GnitzClient::seek` likewise stays.
- `RangeDescriptor` stays (embedded in `ReadSpec`).

## 6. Semantics pinned (decisions, not options)

- **LIMIT/OFFSET count logical rows** (summed weight), never Z-set entries; worker sinks keep
  boundary rows whole and the client window performs the exact split — end result identical to
  today's sink.
- **Ordering ties are deterministic**: user keys, then PK bytes, then payload — identical queries
  paginate identically at any worker count.
- **`pk IN (…)` with LIMIT and no ORDER BY** returns a different (equally SQL-legal) arbitrary
  subset than today's IN-list-order early stop — sorted-gather order replaces list order.
- **Snapshot semantics**: per-worker snapshot at request dispatch; one shared SAL consumed
  strictly in order means every worker's cursor reflects the same committed prefix — the same
  consistency a plain scan gives today. Exception: a chunked oversized `PkSet` issues multiple
  requests, one snapshot each (parity with today's `seek_pk_multi`). A view read drains pending
  ticks first.
- **The selectivity gate stays** (`INDEX_SCAN_RATIO = 16`): an index bound covering > 1/16 of the
  local slice silently degrades to a full (still server-filtered, still limited) scan — transfer
  stays bounded either way.
- **Unsupported-in-programs predicates** (128-bit comparisons, LIKE, string arithmetic) reject at
  bind time with the existing expression-error text; there is no client-side fallback. String
  equality/range predicates *work* (STR opcodes) — previously a hard interpreter error on the
  thin path.
- **`SELECT *` with no WHERE/ORDER/LIMIT** stays a plain `scan` (unbounded streaming read,
  `full_scan` cache intact). The SQL layer never emits an identity spec.
- **>8 ORDER BY keys** and the U64/div-by-zero flips of §5b are accepted, documented behavior
  changes.

## 7. Testing

Rust (`crates/gnitz-wire`, `crates/gnitz-engine` unit/integration):
- `ReadSpec` encode/decode round-trip; decode rejects: bad version/kind, order-key count > 8,
  PkSet count > cap, unsorted/duplicate PkSet, length overflows, trailing bytes.
- Expr blob v2: const-set pool round-trip; `INT_IN_SET` eval (hit/miss/NULL, signed images incl.
  `u64col IN (-1)` matching `u64::MAX`, empty set, `NOT IN` with NULL input); the float-column
  type-gate expansion (`fcol IN (1,2)` matches `1.0`); v1 blobs still decode.
- Flag disjointness: the new cross-namespace assert; `FLAG_SCAN_SPEC` classifies as `ScanSpec`
  and never as `ExchangeRelay`.
- `pk_range_keys`: full pin (degenerate cuts), prefix+range, empty range, compound PKs, signed PK
  columns (OPK sign-flip order); `opk_from_pins` agrees with worker-side OPK encoding
  byte-for-byte.
- Worker sink units: top-k with weights (a weight-3 row fills 3 logical slots and is kept whole at
  the boundary), cutoff pruning, tie determinism (PK tiebreak), early-stop honors summed weight,
  hidden order-key columns present in reply, NULL placement asc/desc × nulls_first/last.
- `open_bounded_source` extraction: circuit backfill behavior unchanged; spec path and backfill
  path share the gate. Ported `seek_by_index_range` tests.
- Predicate/projection compile rejection: malformed blob, OOB column, wide-column load, reg-cap
  overflow → clean error reply, worker continues.
- PkSet gather: unowned keys miss silently; run splitting at `FLAG_SCAN_LAST` incl. empty worker
  runs; dispatch-matrix walk test includes `ScanSpec`.
- Cache hygiene: a plain scan issued after a projected spec read of the same relation decodes
  against the correct (source) schema — the spec reply never touched the LRU.
- Semantic pins: U64 unsigned comparison ≥ 2^63; `a/b = 0` with `b = 0` matches nothing;
  overlay evaluator agrees with the engine on both.
E2E (`crates/gnitz-py`, `GNITZ_WORKERS=4`):
- Existing `test_order_by.py` / `test_sql.py` / `test_dml.py` suites pass unchanged (identical
  results, new path) — except tests pinning IN-list-order LIMIT subsets, updated per §6.
- New `test_read_spec.py`: ORDER BY + LIMIT across partitions with weighted rows (UNION ALL view
  source); `LIMIT k` with/without ORDER BY; OFFSET windows; OFFSET without LIMIT; string-predicate
  WHERE served server-side; large `pk IN` (one request; > cap chunking); `pk = a OR pk = b`;
  integer `col IN` with 1000 items (IN_SET); PK range WHERE on compound and signed PKs;
  `SELECT a+1 FROM t WHERE pk = 5`; `SELECT v, id FROM t` column order with `id` the PK;
  `SELECT a, a FROM t`; ORDER BY a non-projected column; ORDER BY a computed alias; view target
  reflects an immediately-preceding push; UPDATE/DELETE with string WHERE; transactional
  UPDATE after a buffered write to the same table matches the overlaid rows.
- `make verify` and `make e2e WORKERS=4` green.

## 8. Sequencing

- [ ] gnitz-wire: `ReadSpec`/`ReadBound`/`OrderKey` + encode/decode + caps + `FLAG_SCAN_SPEC`
      (bit 10) + extended low-bit disjointness assert + unit tests.
- [ ] gnitz-wire + gnitz-core + engine: expr blob v2 (const-set pool), `INT_IN_SET` opcode,
      `BoundExpr::InSet` + binder change + type-gated arms in `OpcodeBackend`, `eval_expr`, and
      a transient not-thin `ThinProbe` arm + tests.
- [ ] engine (refactor, no behavior change): extract `open_bounded_source` from
      `open_source_cursor`, re-pointing `seek_by_index`'s range-walk substrate onto it.
- [ ] engine: `SalMessageKind::ScanSpec` (classify, cross-namespace assert, unicast,
      live-point-traffic), dispatch-matrix cells + walk test, `handle_scan_spec` +
      `fan_out_scan_spec_async` + `write_scan_spec_group`, `pk_range_keys`, `opk_from_pins`
      (added here with their first callers, so no dead-code window fails `make verify`),
      worker arm + `scan_spec_family` + sinks + echoed in-frame reply schema; unit tests.
- [ ] gnitz-core: `encode_scan_spec_parts`, `GnitzClient::scan_spec`, cache-bypassing
      `recv_scan_spec` returning per-worker runs.
- [ ] gnitz-sql: `plan_read_spec`, SELECT rewiring, ORDER BY/limit mapping, client k-way merge +
      window + hidden strip; UPDATE/DELETE committed-fetch move + overlay split; evaluator
      semantic alignment (U64, div-by-zero); deletions (`expr_is_thin`, `bind_thin_residuals`,
      `projection_is_thin`, thin fork, `seek_pk_multi`).
- [ ] deletion sweep: `SeekByIndexRange` end-to-end + test ports; capi/py range-binding port.
- [ ] e2e: `test_read_spec.py`; full suite; `make verify`.
