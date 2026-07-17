# Retire the transient executor: uniform derivation rejection, EXPLAIN, and the deletion sweep

## 1. Problem

After single-relation reads run on the parameterized scan and single-relation aggregates run on
the fold sink, the transient executor — compile a one-shot DBSP circuit from client-shipped
families, drive it over the full base, stream, tear down — serves only join-shaped queries:
2-way joins, 2-side set operations, and EXISTS/IN/mark subqueries. For those it remains every
inch the machinery it always was: per-query operator-state build and disk spill of both join
sides, a full exchange of every row, a monotone never-recycled id band, a dedicated
reader/writer lock against CREATE VIEW, scratch-directory GC, a client-circuit trust boundary
whose backstop is a whole-cluster fail-stop abort, and a SAL head-of-line freeze window during
every linear drive. The product answer for derived relations is a **view** — the engine maintains
it incrementally, which is the entire thesis of the system. This plan removes the executor and
its lifecycle end to end, replaces the remaining shapes with one uniform, actionable rejection,
inlines pass-through CTEs so trivial WITH queries keep working, and adds `EXPLAIN` so the read
path's access decisions are visible instead of inferred.

Net effect on the ad-hoc surface (everything else is already served by the read/aggregate paths):

| Shape | Today | After |
|---|---|---|
| 2-way JOIN (equi/band/range, inner+outer) | runs (full trace build per query) | rejected → view |
| UNION / INTERSECT / EXCEPT (2 sides) | runs | rejected → view |
| EXISTS / IN / mark subquery | runs | rejected → view |
| scalar subquery, 3+-way join, self-join, DISTINCT/GROUP BY over join, derived table, non-pass-through CTE | rejected (multi-segment) | rejected, same uniform message |
| WITH pass-through CTE over one relation | runs via executor | runs via the read path (inlined) |
| EXPLAIN | unsupported | supported for SELECT |

## 2. The uniform rejection, and the pinned routing order

`execute_select` (`crates/gnitz-sql/src/dml/select.rs`) replaces both remaining routes into
`execute_select_via_executor` with one error. Routing runs in this fixed order (each step's
precondition includes every earlier step):

1. **Body kind**: a non-`Select` body that is not a `SetOperation` — `VALUES`, a parenthesized
   `SetExpr::Query`, `TABLE t` — and a FROM-less `SELECT 1` get plain `Unsupported` errors
   naming the shape (today they error inside `ViewShape::classify`,
   `plan/view/dispatch.rs:292-309`; the derivation template would be false advice — CREATE VIEW
   rejects the identical bodies). A `SetOperation` body → `reject_derivation("set operation")`.
2. **CTE inlining** (§3): every CTE inlines or the query rejects.
3. **FROM shape**: `from.len() > 1` (comma join) and explicit `joins` →
   `reject_derivation("JOIN")` (the message adds "rewrite as an explicit JOIN" for the comma
   form); a FROM factor that is not a plain `TableFactor::Table` →
   `reject_derivation("derived table in FROM")`.
4. **Subquery walk** (new — no existing preamble check detects these, and after the thin fork's
   deletion they would otherwise die as generic bind errors): one AST walk over the selection
   and projection detecting `Expr::Exists` / `Expr::InSubquery` / `Expr::Subquery` and the
   ANY/ALL comparison forms — the same recognizers `ViewShape::classify` consumes
   (`plan/view/dispatch.rs:328-386`) — mapping to `reject_derivation("EXISTS/IN subquery")` /
   `reject_derivation("scalar subquery")`.
5. **Aggregate arm** (the fold sink) — its precondition is all of the above, so
   `SELECT COUNT(*) FROM t1, t2` rejects as JOIN at step 3 rather than silently aggregating one
   table.
6. **The read spec** (everything else).

The error:

```
ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).
CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it.
```

`{construct}` names what was detected: `JOIN`, `set operation`, `EXISTS/IN subquery`,
`scalar subquery`, `derived table in FROM`, `non-pass-through CTE`. One template, one code path
(`reject_derivation(construct)`), asserted verbatim by tests. The `segments.len() != 1` guard and
its message die with the executor route — rejection now happens **before** any circuit
compilation, from the AST alone (the step-1 shapes are the only non-template errors, since the
template's CREATE VIEW advice would be wrong for them).

## 3. Pass-through CTEs join the read path

Today any `WITH` routes wholesale to the executor (`select.rs:61-64` checks
`query.with.is_none()` before anything else), so `WITH x AS (SELECT * FROM t) SELECT * FROM x
WHERE g > 5` — a shape the read path serves trivially — would become a works→error regression.
The pass-through detection and aliasing already exist as a pure binder-level substitution:
`inline_passthrough_cte` (`crates/gnitz-sql/src/plan/view/dispatch.rs:483-545`) resolves a CTE
that is a bare single-table identity/positional projection (no WHERE/DISTINCT/GROUP BY/HAVING)
directly to the underlying table's `(tid, schema)` via `binder.cache_alias` — no circuit, no
segment.

`inline_passthrough_cte` itself is not directly reusable — it takes the `ViewChain` and falls
back to `compile_hidden_body` (a hidden segment) for non-identity projections
(`dispatch.rs:528-532`), and `inline_ctes` routes WHERE'd/joined CTEs
(`is_compilable_hidden_body`, `dispatch.rs:395-406`) to `compile_hidden_body` before it is ever
reached. The reusable core is extracted as a **pure predicate**:
`cte_passthrough(client, cte, binder) -> Option<(tid, schema)>` — a single table **or view**
FROM, identity or positional projection (column-list renames included, `dispatch.rs:534-545`),
no honored clauses. `execute_select` runs it per CTE before shape routing: `Some` aliases via
`binder.cache_alias` and routing proceeds on the rewritten FROM; `None` →
`reject_derivation("non-pass-through CTE")` — covering both the WHERE'd/joined and the
non-identity-projection cases. `inline_ctes`/`inline_passthrough_cte` keep their behavior for
CREATE VIEW by consuming the same predicate internally (falling back to `compile_hidden_body`
on `None`); the existing recursive-WITH and query-envelope errors keep their messages. CTE name
shadowing is preserved (the binder cache is probed before the catalog,
`bind/resolve.rs:183-185`). Pinned by the existing pass-through semantics test shape
(`test_cte_plain_passthrough_accepted`, `crates/gnitz-sql/tests/planner_cte.rs:177`) extended to
the ad-hoc route.

## 4. EXPLAIN

New arm in `execute_statement`'s match (`crates/gnitz-sql/src/dispatch.rs:53-113`) for
`sqlparser::ast::Statement::Explain` (present in sqlparser 0.62,
`ast/mod.rs:4602-4623`):

- Accepted: `EXPLAIN <SELECT>` with default options only. `analyze`, `estimate`, `verbose`,
  `query_plan`, a non-`Explain` `describe_alias` (`DESC`/`DESCRIBE <select>` parse as
  `Statement::Explain` too), non-default `format`/`options`, and non-SELECT inner statements are
  `Unsupported`; `Statement::ExplainTable` (bare `DESC t`) stays unmatched.
- **Plans without executing the read**: runs `plan_read_spec` (and the aggregate arm) exactly as
  the query would — including the metadata lookups planning always does (name resolution,
  `client.table_indexes`) — but issues no data-path request. Derivation shapes return the §2
  rejection as the error — the plan for a query with no plan is its rejection.
- Output: `SqlResult::Rows` with a two-column schema: a **hidden U64 line-number PK** (a schema
  must have a PK and STRING is not PK-eligible — `Schema::validate_parts`,
  `crates/gnitz-core/src/protocol/types.rs:248-270`; the hidden column is stripped at
  presentation like every hidden column and makes line order deterministic) plus `plan` (TEXT,
  non-null). Weight-1 rows, one per line, surfacing through gnitz-py's existing `Rows` path
  (`crates/gnitz-py/src/lib.rs:1723-1736`) with no binding changes. Lines, in order:
  1. `read <table|view> <name>` (a view notes `drains pending ticks`)
  2. bound: `seek pk = <v>` | `pk range [..]` | `pk set (<n> keys, <m> requests)` |
     `index range on (<cols>) — used at runtime iff ≤ 1/16 of the local slice matches` |
     `full scan`
  3. `predicate: server-side (<n> conjuncts)` | `predicate: none`
  4. `projection: <n> columns` (+ `<k> hidden order keys` when applicable)
  5. aggregates: `group by (<cols>): <ops>` | `distinct (<cols>)` | absent; with
     `per-worker group cap <cap>`
  6. `order/limit: server top-k <k> + client window` | `server early-stop <k>` |
     `server sort + client merge` (ORDER BY without LIMIT; `+ client window` with OFFSET) |
     `client sort` (bounded aggregate results) | `streamed`

The selectivity-gate caveat in line 2 is the honest answer to runtime-measured index choice:
EXPLAIN reports the candidate and the rule, not a pretending-to-know verdict.

## 5. The deletion sweep

Everything below was verified transient-only (definition + all callers). Shared infrastructure
that stays is listed explicitly at the end.

**gnitz-sql** (dies with the rerouting, first commit):
- `execute_select_via_executor` (`dml/select.rs:250`) and both remaining routes into it.
- `compile_query_to_circuit` (`plan/view/dispatch.rs:187`) — the wrapper and its re-exports
  (`plan/view/mod.rs:15`, `plan/mod.rs:13`) only; `build_query_segments` and every per-shape
  emitter stay (CREATE VIEW's compiler).
- `ViewChain::new_transient` (`plan/view/mod.rs:38-80`); `ViewChain::new` / `mint_id`'s durable
  branch stay.
- `crates/gnitz-sql/tests/transient_executor.rs` — ported, not dropped (§7).

**gnitz-core**:
- `GnitzClient::run_query` (`client.rs:1230`), `Session::run_transient` (`connection.rs:277`),
  `encode_run_transient` (`protocol/message.rs:330`), the `TRANSIENT_PROVISIONAL_VIEW_ID`
  schema-cache key use (`connection.rs:286`).

**gnitz-wire**:
- `FLAG_RUN_TRANSIENT` (bit 63 — freed; removed from the disjointness array,
  `flags.rs:217-234`), `TRANSIENT_PROVISIONAL_VIEW_ID` (`flags.rs:126`).

**gnitz-engine — runtime**:
- executor.rs: the `FLAG_RUN_TRANSIENT` route (`:1022`), `handle_run_transient` (`:2156`),
  `drive_transient` (`:2175`), `teardown_transient` (`:2308`), `decode_transient_frame`
  (`:2114`), `alloc_transient_id` (`:153`) + `next_transient_id` (`:146,:338`).
- **`drive_rwlock` is deleted entirely** (`executor.rs:141,337`): its only reader is
  `drive_transient` (`:2183`) and its only writer the CREATE VIEW arm of `handle_ddl_txn`
  (`:2411`) — with no reader, the writer excludes nothing. The `_drive_excl` acquisition and its
  ordering comment go, as does the second stale rationale in the tick loop's Quiesce comment
  (`executor.rs:686-691`); CREATE VIEW's committer barrier, tick quiesce, and catalog write lock
  are untouched. What actually orders ad-hoc reads against CREATE VIEW — stated here because §7
  pins it — is: every ad-hoc read emits its single SAL group under `catalog_rwlock.read()`
  (the `drain_then_lock` discipline `handle_scan` already follows, `:1817-1824,:1859-1862`, and
  `handle_scan_spec` must follow for tables and views alike), CREATE VIEW holds
  `catalog_rwlock.write()` from `:2429` through `fan_out_backfill`, and a spec scan is one
  atomic group — so no read can interleave into the DDL-to-backfill window.
- master/dispatch.rs: `broadcast_transient_family` (`:1258`), `write_run_transient_drive`
  (`:1288`), `broadcast_drop_transient` (`:1323`); the SAL-side `FLAG_RUN_TRANSIENT`
  (`sal.rs:152`) / `FLAG_DROP_TRANSIENT` (`sal.rs:159`) constants and their `master/mod.rs`
  imports.
- SAL: `SalMessageKind::{RunTransient, DropTransient}` (`sal.rs:266-267`), their `classify`
  arms (`:298-303`) and `is_broadcast` entries (`:372-387`).
- worker/mod.rs: `handle_run_transient` (`:1257`), `DeferredRunTransient` (`:77-92`),
  `DeferredControl::{RunTransient, DropTransient}` variants (`:68`) + replay arms (`:524,:530`),
  `drop_transient` (`:1313`), `transient_frames` (`:277,:417,:2018`), the four dispatch-matrix
  cells (`:690-708`) + doc-table rows (`:583-584`) + walk-test entries (`ALL_KINDS`
  `:2156-2157`, `must_defer` `:2174-2175`, assert arm `:2206-2213`).
- **The between-chunks yield machinery is deleted whole**: `handle_backfill`'s
  `yield_between_chunks` parameter (`:1164-1243`) — its `true` caller is transient-only, so the
  parameter goes and the body keeps only the CREATE-VIEW stop-the-world behavior;
  `drain_live_traffic_between_chunks` (`:1327`), `peek_sal_kind` (`:1351`), and
  `SalMessageKind::is_live_point_traffic` (`sal.rs:353`) each have exactly one caller inside
  that chain (verified) and go with it — including the `ScanSpec` entry the read-spec work added
  to the allowlist.

**gnitz-engine — catalog / dag**:
- hooks.rs: `register_transient_meta` (`:259`), `forget_transient` (`:294`),
  `transient_scratch_dir` (`:214`) + `transient_root` (`:218`), `gc_transient_scratch` (`:231`)
  + its `bootstrap.rs:453` call.
- `RelationKind::Transient` (`query/dag/mod.rs:107`) and every match arm: the `recovery_source`
  arm (`:130`); `in_dep_tab` (`:150-153`) becomes constantly true — the method is deleted and
  its two callers (`meta.rs:274`, `mod.rs:302`) simplified; `checkpointed` needs no change
  (matches `View` only).
- query/dag: `transient_prepare` (`:474`), `compile_transient` (`:410`),
  `build_transient_loaded` (`:577`), `has_plan` (`:449`), the `meta` pre-injection sites
  (`:430,:490`); `compiler::build_loaded_from_batches` (`load.rs:61` — sole caller chain is
  transient) with its `ReadCursor::from_owned` uses (`load.rs:73-75`); `assemble_circuit` and
  `load_circuit` stay (the CREATE VIEW / sys-table loader).
- **Band guards re-founded, not deleted**: `TRANSIENT_ID_BASE`/`TRANSIENT_ID_LIMIT`
  (`sys_tables.rs:63,72`) go, but the two guards they powered are re-anchored on the real
  physical invariant they were indirectly protecting — relation ids are **u32 by contract**
  (SAL group header, shard filenames, `Batch::encode_to_wire` all narrow):
  `precheck_family` (`write_path.rs:314-321`) rejects a durable relation id `> u32::MAX`, and
  `allocate_table_id`'s assert (`registry.rs:197-206`) becomes the same bound. The band test
  (`atomicity_tests.rs:642-674`) ports to the u32 bound.

**Stale doc references on kept code** (updated, not deleted): the family-order doc citing
`run_query` (`crates/gnitz-wire/src/catalog.rs:188` → cite `create_view_chain` only), the two
`append_circuit_rows` comments naming the transient executor
(`crates/gnitz-core/src/client.rs:1164,:1630`), and the misnamed dag test
`transient_view_meta_from_loaded_carries_range_join_relay_routing`
(`query/dag/mod.rs:1185` — compiles against kept code; renamed to its view-only property).

**plans/**: `plans/adhoc-query-chains.md` is deleted — the multi-segment chain design is
obsolete (nothing in code references it; verified).

**Shared infrastructure explicitly kept** (verified non-transient users):
`build_query_segments` + all view emitters, `inline_ctes`, `emit_groups_await_acks`
(`run_tick`'s emission loop), `drain_pending_ticks` (`scan_multi_body` + `drain_then_lock`),
`drain_then_lock` (view seek/scan), `handle_backfill` (CREATE VIEW), `child_scratch_dir` (every
view's operator tables), `flush_view_or_abort`, `RecoverySource::Rederive` (indexes, tests,
utils), `ViewMeta::from_loaded` (view meta memoization), `order_limit_passthrough` (the
aggregate finisher's sort/window), `eval_expr` (DML SET / ON CONFLICT / overlay / HAVING),
`CIRCUIT_*_TAB` families and `decode_sys_family` (DDL), `SalMessageKind::Backfill` and the
CREATE VIEW backfill path.

## 6. Semantics pinned (decisions, not options)

- The rejection is detected from the **AST**, pre-compilation; its message is one template with
  the construct name; tests assert the exact text.
- Pass-through CTE inlining on the read route matches `inline_passthrough_cte`'s existing
  definition of pass-through exactly — no new acceptance surface, no partial inlining (one
  non-pass-through CTE rejects the whole query).
- `EXPLAIN` performs planning-time metadata lookups only, never a data-path request; its output
  schema and line vocabulary are part of the tested surface.
- Worker-abort semantics for the deleted machinery (client disconnect mid-anything, scratch GC)
  need no successor: the read path holds no per-query server state beyond the request handler's
  stack.
- The freed wire bit 63 and SAL bits 20/21 are left free (no opportunistic reuse in this plan).

## 7. Testing

Ports (same properties, new expectations):
- `crates/gnitz-sql/tests/transient_executor.rs` → `adhoc_surface.rs`: joins / set-ops /
  EXISTS / IN / mark shapes now assert the §2 rejection verbatim (each construct name once);
  `pk_equality_stays_thin_and_is_correct`, `where_on_string/float_column…`,
  `executor_path_serves_order_by_limit_offset`, `group_by_matches_view`,
  `non_integral_literal_against_int_column…`, `binder_errors_survive…`,
  `synthetic_key_slots_are_hidden…`, `repeated…`/`reflects_writes…` keep their assertions on
  the read/aggregate paths; `multi_segment_query_is_rejected…`,
  `transient_band_scan_source_is_rejected` are deleted with their subject;
  `same_relation_anti_join_is_still_rejected` re-expresses as a plain rejection.
- Engine tests deleted with their subjects:
  `build_loaded_from_batches_rejects_out_of_range_node_id`
  (`query/compiler/mod.rs:1958` — direct caller of two deleted symbols) and
  `transient_replication_verdict_reads_off_the_stamped_schema` (`query/dag/mod.rs:1226` —
  constructs `RelationKind::Transient` and pins the deleted `in_dep_tab` shortcut).
- `crates/gnitz-py/tests/test_transient_executor.py` is deleted; its weight-correctness content
  is already owned by the read-spec/aggregate e2e suites; its join/set-op/EXISTS tests become
  rejection assertions in `test_sql.py`.
- `test_sql.py:186` and the `test_indices.py` executor-fallback tests re-word to the read path
  (same results, no executor).
- The dispatch-matrix walk test shrinks to 15 kinds (the original 17, plus `ScanSpec`, minus
  `SeekByIndexRange` and the two transient kinds) / 2 must-defer entries; the band atomicity
  test becomes the u32-bound test.

New:
- `EXPLAIN` e2e: seek / pk-range / pk-set / index-range / full-scan / aggregate / top-k lines;
  EXPLAIN of a join returns the rejection error; `EXPLAIN INSERT` unsupported.
- Pass-through CTE over table and view on the read path (results + EXPLAIN); non-pass-through
  CTE rejection.
- CREATE VIEW under concurrent ad-hoc reads (the lock-free replacement for
  `test_transient_concurrent_with_pushes_and_create_view`): reads before/during/after a CREATE
  VIEW succeed and ingestion continues.
- `make verify` and `make e2e WORKERS=4` green after every commit below.

## 8. Sequencing

- [ ] gnitz-sql rerouting: `reject_derivation` + uniform message, pass-through CTE inlining on
      the read route (extraction from `inline_ctes`), `EXPLAIN`; delete
      `execute_select_via_executor` / `compile_query_to_circuit` / `ViewChain::new_transient`;
      port `transient_executor.rs` → `adhoc_surface.rs`; pytest updates (delete
      `test_transient_executor.py`, re-word executor-fallback tests, add rejection + EXPLAIN +
      CTE tests).
- [ ] gnitz-core deletions: `run_query`, `Session::run_transient`, `encode_run_transient`,
      the provisional-id cache key.
- [ ] gnitz-engine sweep: wire flags, SAL kinds + classify + broadcast entries, worker handlers
      + deferred variants + `transient_frames` + dispatch-matrix cells/doc/walk-test, the
      between-chunks yield machinery (`yield_between_chunks`,
      `drain_live_traffic_between_chunks`, `peek_sal_kind`, `is_live_point_traffic`), master
      writers, executor handlers + id allocator, `drive_rwlock`, catalog hooks + boot GC,
      `RelationKind::Transient` + `in_dep_tab`, dag transient fns +
      `build_loaded_from_batches`, band constants → u32-contract guards + test port.
- [ ] delete `plans/adhoc-query-chains.md`; full `make e2e WORKERS=4` + `make verify`.
