# Ad-hoc SELECT ordering & pagination sink (ORDER BY / OFFSET / LIMIT)

## 1. Problem

Ad-hoc SELECT has no ORDER BY, no OFFSET, and no correct LIMIT. `reject_unhonored_query_clauses`
(`crates/gnitz-sql/src/plan/validate.rs:295`) rejects `order_by` **unconditionally** (`validate.rs:335`),
a separate site rejects OFFSET inside `select.rs`, and the only LIMIT support is an ad-hoc client-side slice
(`extract_limit`, `dml/plan.rs:66`) applied inline in `select.rs` — plus a fetch-cap pushdown
(`seek_pk_multi` `row_cap`, `dml/plan.rs:240`) that early-stops the `pk IN` seek.

This sink adds ORDER BY / OFFSET / LIMIT as a **client-side pass over the fetched batch**. Its target is
**today's thin path** (`execute_select`, `dml/select.rs`), which resolves **base tables and views** — so a
result can carry hidden keys (`_group_pk`/`_join_pk`/`_set_pk` from a GROUP BY / join / set-op view),
weight > 1 (a `UNION ALL` view), and every scalar type. The sink handles all of these. (When the on-demand
executor path lands, the same sink applies to its streamed result unchanged — but that path does not exist
yet, so this plan is scoped to the thin path.)

## 2. The comparator & pipeline — `exec/order.rs`

**A from-scratch typed comparator** (the engine's `cmp_typed_le` lives in `gnitz-engine` and is not
importable across the crate layering, so `exec/order.rs` reimplements it). It **dispatches per key on
`schema.columns[ci].type_code`** — the full `TypeCode` enum — **not** on the client `ColData` variant:
`ColData::Fixed(Vec<u8>)` buckets all of U8/I8/U16/I16/U32/I32/U64/I64/F32/F64, and `ColData::U128s` buckets
U128/UUID/I128 (`crates/gnitz-core/src/protocol/types.rs:585`), so the variant tag alone loses the type; the
schema's `type_code` is the discriminator, exactly as the engine's `cmp_col_window` dispatches
(`storage/repr/columnar.rs:83-116`). Per type, matching the engine's order: unsigned by magnitude
(U8/16/32/64), signed via sign-extension, `F32`/`F64` `total_cmp`, `U128`/`UUID` unsigned `u128::cmp`, `I128`
signed `i128::cmp`, `STRING`/`BLOB` byte-wise.

- **PK-column keys read a different accessor.** `compare_rows` is payload-only (PK compares separately via
  OPK-byte `compare_pk_bytes`), so an ORDER BY key that resolves to a PK column is **not** in
  `batch.columns` — read it from `batch.pks`. A **single-column** PK is held as a native (de-OPK'd) value
  (`PkColumn::U64s`/`U128s`, `crates/gnitz-core/src/protocol/wal_block.rs:404` reverses the OPK transform):
  dispatch through the **same** per-type comparator via `PkColumn::get`. A **compound** PK is held as raw
  OPK stride bytes (`PkColumn::Bytes`). Compound PK is **fully supported and thin-path-reachable** (a plain
  `SELECT *` on a 2–5-column compound-PK table is exercised today by `test_compound_pk.py`), and
  `PkColumn::get` **panics** on `Bytes` — so the sink must not call it. Order a compound-PK column by its OPK
  byte sub-range (the column's fixed offset+width within the stride, from the schema), compared **unsigned**
  — order-preserving by OPK construction (CLAUDE.md §6), so no per-type decode is needed. PK columns are
  non-nullable, so PK keys need no NULL handling.
- **NULL detection is bitmap-based, uniformly.** `ColData::Fixed`/`U128s` have **no** per-value null
  sentinel (a NULL is zero-filled filler); only `Strings`/`Bytes` carry `Option`. So detect NULL via
  `null_word_get(batch.nulls[row], schema.payload_idx(ci))` (the mechanism `exec/eval.rs:62` already uses)
  across all four variants — never an `Option::is_none()` check.
- **STRING/BLOB byte-wise == the engine's content order.** `ColData::Strings`/`Bytes` can only hold an owned
  `String`/`Vec<u8>` (materialized on wire-decode, `types.rs:586`) — never the raw 16-byte German-string
  struct — and `compare_german_strings` (`gnitz-engine/src/schema.rs:1326`) is a prefix/length fast-path
  provably equal to plain lexicographic byte order. BLOB shares the identical path. No collation exists
  (`COLLATE` rejected at the planner, `validate.rs:899`).

**Pipeline — sort BEFORE projection.** The batch handed to the sink is the **full fetched row** (thin path
projects client-side *after* the fetch, `select.rs:188`). Sorting before projection is required for
correctness: `SELECT a FROM t ORDER BY b` and `SELECT v AS foo FROM t ORDER BY v` both reference columns
absent from the *projected* schema, and a post-projection sink cannot see them. So: build a row-index
permutation over the full batch, **stable-sort** by the ORDER BY keys, apply the OFFSET/LIMIT slice (§3),
then `project` the surviving rows and materialize via `copy_batch_row`. A plain `SELECT` with no ORDER BY,
no OFFSET, and no effective LIMIT is a **zero-copy passthrough**.

**NULL placement is absolute**, not flipped by DESC: default NULLS LAST for ASC, FIRST for DESC; explicit
`NULLS FIRST/LAST` honored. (This is SQL ORDER-BY null placement — a separate concern from the Z-set
consolidation order's `null < non-null`, which this comparator does not use.)

**Key resolution (dual, mirroring SQL).** Each ORDER BY key resolves as: (1) a **1-based visible-output
position** — the *n*-th column after skipping `META_FLAG_HIDDEN` (`Schema::visible_columns()`,
`types.rs:191`; a view result carries a hidden column at physical index 0, so naive `pos-1` sorts by the
hidden key) — **bounds-checked** (out-of-range → clean error, not a panic); (2) an **output column
name/alias** (the projected schema, whose `ColumnDef` carries the alias, `exec/batch.rs:66`; a duplicate
output name → ambiguity error, matching `find_unique_column`, `bind/resolve.rs:249`); or (3) a **source
column name**, resolved against the pre-projection schema's **visible** columns only (`visible_columns()`,
so hidden synthetic keys like `_join_pk` are not name-addressable) — this is what makes `ORDER BY b` for a
non-projected `b` work. Output name (2) is tried before source name (3), matching SQL's output-column
precedence for an unqualified ORDER BY name. Qualified refs (`t.a`) inherit the crate's **qualifier-blind**
resolution (`single_relation_col_name` strips the qualifier, `ast_util.rs`), so `t.b` binds by the bare name
`b` — alias-first per (2), a documented divergence from strict SQL consistent with the rest of the crate. An
ORDER BY key
that is **an expression** (`a+b`, `upper(x)`) or the sqlparser variants `ORDER BY ALL` / `WITH FILL` /
`INTERPOLATE` (reachable under `GenericDialect`) is a clean **`Unsupported`** error — an explicit scope cut,
not silent mishandling. `ORDER BY <n> DESC` and `NULLS` options come from `OrderByExpr.options`.

## 3. LIMIT / OFFSET semantics

**Count multiplicity, not entries.** A result is a Z-set: one entry per `(PK, payload)` with an integer
**weight**, never expanded to per-row copies (`gnitz-py/src/lib.rs:1388`). A `UNION ALL` / `INTERSECT ALL` /
`EXCEPT ALL` view (scannable on the thin path) yields genuine weight > 1
(`plan/view/set_op.rs:311-343`, `ALL` leaves unclamped). The existing `apply_limit`
(`exec/batch.rs:260`) truncates **entries**, so `... UNION ALL ... ORDER BY city LIMIT 3` where `NYC` has
weight 3 returns 3 entries = **7 logical rows** — off by 4. The sink instead walks the sorted permutation by
**running logical position**: entry *i* occupies `[C_i, C_i + w_i)` where `C_i` is the cumulative weight
before it, and its surviving weight for the window `[offset, offset+limit)` is
`max(0, min(C_i + w_i, offset+limit) − max(C_i, offset))`. This single formula handles OFFSET mid-entry,
LIMIT mid-entry, **and both cuts inside the same entry** (a two-independent-passes implementation gets that
last case wrong — the offset-partial must carry into the limit count). An entry with surviving weight 0 is
dropped; a boundary entry keeps its reduced weight. `copy_batch_row` (`exec/batch.rs:8`) copies
`src.weights[i]` **verbatim** — it has no weight override — so after the gather copy the sink must
**overwrite** `dst.weights[last] = surviving_weight` (or use a `copy_batch_row_with_weight` variant); calling
`copy_batch_row` alone would re-emit the full stored weight and reintroduce the off-by-N. The walk assumes
`w_i ≥ 1`: consolidation drops **net-zero** entries (ghost elimination) but not negative ones, so positivity
rests on the DBSP bag invariant (base tables positive; `UNION ALL` sums positives; `EXCEPT ALL` clamps via
`positive_diff`, `set_op.rs:337`) — add a `debug_assert!(w > 0)` so a future non-positive scannable source
fails loudly. For an all-weight-1 result (a base-table scan, a `unique_pk` relation) this is **identical** to
entry-counting. Weight itself is **not orderable** — it is not a `schema.columns` entry, only a per-row
attribute — so `ORDER BY` cannot target it; and because the client shows one row-object per entry, an
iterable's length counts **entries**, so it can be smaller than `LIMIT` when weights > 1 (inherent to the
non-expanding Z-set client, not a defect).

**Disable the `pk IN` fetch-cap under ORDER BY.** `execute_select` feeds `extract_limit` to **two** sites:
`seek_pk_multi`'s `row_cap` (`select.rs:109`), which early-stops the seek after `limit` keys **in IN-list
order**, and the post-fetch slice. A client-side ORDER BY over a `row_cap`-truncated fetch sorts the *wrong*
rows: `SELECT * FROM t WHERE pk IN (1..5) ORDER BY v LIMIT 2` fetches pk=1,2 and returns those, not the two
smallest `v`. So: pass `row_cap = None` on the `pk IN` path whenever ORDER BY is present; when there is no
ORDER BY, the cap is valid but must be `offset + limit` (not `limit`), else `LIMIT 5 OFFSET 10` fetches 5
and the OFFSET skips them to empty. This makes the sink the **single** LIMIT owner — `row_cap` becomes a
pure no-ORDER-BY fetch optimization, not a second LIMIT.

**Deterministic pagination when ORDER BY is present.** A scan is per-worker sorted runs concatenated in rank
order — no global merge — so a stable sort's tie-break over equal ORDER BY keys is worker-count-dependent,
and a LIMIT/OFFSET cut through a tie group would return different rows at W=1 vs W=4. **When ORDER BY is
present with a cut, append the full row identity** — the PK region (compared wholesale via `compare_pk_bytes`,
raw OPK bytes with no per-column decode, so a compound PK needs no special handling here) then the payload
columns in schema order (hidden included) — as the final sort term, completing the ORDER BY into a total
order so the page is a deterministic function of the data. This is **one uniform rule for every shape** (no `_group_pk`/`_join_pk`
name-matching): a consolidated Z-set has unique `(PK, payload)` by construction (CLAUDE.md §4a), so the full
row identity is always a total order — it dominates the fan-out-join case (where `_join_pk` alone is
non-unique) automatically. It is a near-free extra comparison in the sort that is already running; it does
**not** apply to a bare LIMIT with no ORDER BY (which stays SQL-arbitrary and keeps its `row_cap` fast-path).

**Precondition:** the batch reaching the sink is a **consolidated, positive-weight integral** with unique
`(PK, payload)` per entry — true because an ad-hoc SELECT returns the materialized state (not a delta;
scans certify `Consolidated`, `ops/scan.rs`), base tables are positive, and set-ops clamp via
`positive_part`. The uniqueness the tiebreaker relies on also requires **one copy per output PK at the
gather edge** — which holds: a replicated/broadcast relation is **single-sourced at the gather edge**
(`replicated_unicast` reads worker 0 only when `relation_output_is_replicated`, `master/mod.rs:139`;
`dispatch_scan_fanout` signals just that one worker), so exactly one copy per PK reaches the sink with
weights intact — no W-copy ties. The replicated-source test below exercises this path.

## 4. Envelope honoring

Add `order_by: bool` to `HonoredQueryClauses` (`validate.rs:276`); gate the ORDER BY reject
(`validate.rs:335`) on it — `order_by: true` only at the direct-SELECT site (every other caller passes
`false`). Add `extract_offset` mirroring `extract_limit` (`dml/plan.rs:66`), including its **silent-ignore**
of an unparseable/negative value (→ no offset), for consistency. LIMIT and OFFSET are one `limit_clause`
(`extract_limit` already reads both `LimitOffset` and `OffsetCommaLimit`), so removing the OFFSET reject and
adding `extract_offset` compose. **Remove only the OFFSET arm** of the `select.rs` reject block — **keep the
`LIMIT … BY` (ClickHouse) reject**, else it is silently accepted-then-ignored. **Delete the inline
`apply_limit` slice** in `select.rs` and route LIMIT through this sink — one implementation.

## 5. Cost & non-goal

The sink is **O(result)**, client-side; for an unfiltered `SELECT * FROM t ORDER BY x` it sorts the whole
relation in RAM, and a string/blob result must fit one `MAX_W2M_MSG` frame (`worker/reply.rs:182`, the
existing view-scan limit, inherited).

Non-goal: a server-side ordered/bounded/paged result sink — out of scope.

## 6. Testing

- **Rust unit** (`exec/order.rs`): the comparator across every type incl. negatives, U64 high-bit,
  `-0.0`/NaN (`total_cmp`), strings, `U128`/`UUID`/`I128`; a **single-column PK ORDER BY key** (native, via
  `PkColumn::get`) **and a compound-PK column key** (OPK byte sub-range, unsigned — no panic, no `get` call);
  NULL detected via the bitmap (a NULL in a `Fixed`/`U128s`
  column, not just `Strings`); ASC/DESC; NULLS default + explicit, not flipped by DESC; multi-key;
  positional key over a hidden-column (view) schema (sorts by the visible column, not the hidden key);
  positional out-of-range → error; ORDER BY an expression / `ORDER BY ALL` → `Unsupported`; zero-copy
  passthrough.
- **Multiplicity LIMIT/OFFSET**: over a weight-3 entry straddling the cut, the boundary entry survives with
  the correct reduced weight and the logical-row count matches SQL bag semantics (not entry-counting);
  **OFFSET and LIMIT both landing inside one entry** (e.g. weight 5, OFFSET 1 LIMIT 1 → surviving weight 1)
  is correct (the overlap formula, not two independent passes).
- **Thin-path top-N**: `SELECT * FROM t WHERE pk IN (…) ORDER BY v LIMIT n` returns the true n smallest
  (asserts `row_cap` is disabled under ORDER BY); `SELECT a FROM t ORDER BY b` (non-projected key) and
  `SELECT v AS foo FROM t ORDER BY v` (source name vs alias) both succeed (asserts sort-before-projection).
- **Determinism**: the tiebreaker makes a `LIMIT`/`OFFSET` cut over a non-unique ORDER BY key return the
  **same row set** regardless of input permutation and at W=1 vs W=4 — over a **fan-out join view** (one
  left row, several right matches, tied ORDER BY column) *and* over a **replicated-source view** (asserts
  the gather-dedup precondition), not only a GROUP BY/DISTINCT fixture.
- **`make verify`** then **`make e2e WORKERS=4`** green.
