# Relation resolve: one master-served descriptor RPC on the DML path

## Goal

Replace the client's three-whole-system-table relation resolution with **one
master-served descriptor RPC**, answered from typed maps the master already
maintains. Ad-hoc statements drop from 4–6 server round trips to 2–3, and from
~10 KB of catalog per statement to ~0.3 KB. GET_INDICES is generalized into that
RPC, not deleted.

The client keeps a per-name descriptor **hint**, never a trusted cache: it always
sends a resolve, and the master resolves the name live under one catalog read
guard. The descriptor a statement plans against is therefore at most one round
trip old — today's contract, and strictly more consistent than today (§2). The
hint exists only to let the master *elide* the schema block.

**Scope: the DML path.** DDL keeps the whole-catalog scan path (§3). Measured on
a full E2E trace, DDL statements account for roughly half of all client catalog
scans but under 10% of catalog bytes, so this plan captures ~90% of the byte
defect and ~51% of the round-trip defect. The DDL half is a different change —
it is about the client reconstructing `-1` retraction rows the server could
derive itself — and is deliberately not attempted here.

## The problem, measured

Every SQL statement brackets a **statement-scoped** catalog snapshot
(`GnitzClient::begin_catalog_snapshot` / `end_catalog_snapshot`,
`crates/gnitz-core/src/client.rs:300-317`, bracketed per statement at
`crates/gnitz-sql/src/lib.rs:101-103`) and drops it at statement end. Every
statement therefore re-resolves from whole-system-table scans. Client→server
round trips, traced on a live 4-worker server (SCHEMA_TAB = tid 1,
TABLE_TAB = 2, VIEW_TAB = 3, COL_TAB = 4):

| Statement | Requests today | Count |
|---|---|---|
| `SELECT * FROM t WHERE x = 5` (indexed non-PK) | scan 1, scan 2, scan 4, GET_INDICES, SCAN_SPEC | **5** |
| `SELECT * FROM t WHERE id = 2` (PK point) | scan 1, scan 2, scan 4, SCAN_SPEC | **4** |
| `SELECT * FROM t` (bare star) | scan 1, scan 2, scan 4, plain scan of t | **4** |
| `INSERT` (warm push) | scan 1, scan 2, scan 4, push | **4** |
| `UPDATE t SET … WHERE x = 5` | scan 1, scan 2, scan 4, GET_INDICES, SCAN_SPEC, txn frame | **6** |
| `SELECT * FROM <view>` | scan 1, scan 2, scan 3, scan 4, plain scan | **5** |

A PK point read skips GET_INDICES by design (`pk_bound_is_preemptible`,
`access.rs:255-276`). The bare-star `SELECT *` takes the plain-scan path
(`gnitz-sql/src/dml/select.rs:214-231` → `client.scan(tid)`), which carries **no
request flag at all** — it is the fallthrough dispatch keyed only on `target_id`
(`executor.rs:1222`).

Each catalog scan ships the **whole** system table. Measured reply bytes for the
`WHERE x = 5` statement (656-byte result), against a catalog grown with 5-column
filler tables:

| filler tables | SCHEMA_TAB | TABLE_TAB | COL_TAB | GET_INDICES | catalog total |
|---|---|---|---|---|---|
| 0 | 408 | 2 121 | 7 178 | 560 | **10 267 B** |
| 25 | 408 | 4 321 | 21 178 | 256 | **26 163 B** |
| 50 | 408 | 6 521 | 35 178 | 256 | **42 363 B** |
| 100 | 408 | 10 921 | 63 178 | 256 | **74 763 B** |
| 200 | 408 | 19 721 | 119 178 | 256 | **139 563 B** |

Linear at ≈645 B per table, paid by **every** statement. COL_TAB dominates
(≈560 B/table): over a full E2E run it is 59.9% of all catalog bytes, TABLE_TAB
a further 27.2%.

**Reachable magnitude.** The largest live catalog anywhere in the tree is **10
tables** (`gnitz-py/tests/test_scan_multi.py:384`, `test_persistence.py:671`);
TPC-H is 5 (`benchmarks/helpers/tpch.py:26-47`), HTAP 4, the shared session
server peaks at 8. So the cost reachable *today* is ≈10 KB and 4–6 round trips
per statement — about 15× the result, not the 213× the 200-table row suggests. A
dropped table's rows leave the scan (measured: 50 tables created then dropped
returns COL_TAB to 7 066 B, vs 7 178 B at zero), so DDL churn does not inflate
it. The **round-trip half is independent of catalog size** and is the larger
defect; the byte half grows without bound in a real deployment.

The path is heavily exercised: one full E2E run issues **209 290 client→server
requests over 1 862 connections**, of which **40 326 are catalog scans**.

## The principle

The client asks the wrong question. It downloads O(catalog) to answer an O(1)
question — "what is the shape of relation X?" — that the master can answer from
typed maps it already maintains, one of which is the encoded schema block it
already ships on every read reply:

| client needs | master already has | file:line |
|---|---|---|
| schema name → id | `schema_by_name` | `catalog/cache.rs:31` |
| `"schema.rel"` → id (tables **and** views) | `entity_by_qname`, fed by TABLE_TAB *and* VIEW_TAB (`cache.rs:169-204`) | `cache.rs:33` |
| pk column list (views included) | `pk_col_of`, fed from `VIEWTAB_PAY_PK_COL_IDX` too (`cache.rs:229-233`) | `cache.rs:40` |
| columns (any owner kind) | `col_defs` via `read_column_defs` | `cache.rs:43`, `registry.rs:93-100` |
| the exact schema bytes the client decodes | `schema_wire_cache` | `cache.rs:46` |
| index list `(cols, is_unique)` | `entry.index_circuits` | `catalog/metadata.rs:22` |
| relation kind / existence | `dag.relation_kind(tid)` | `executor.rs:1558` |
| replicated placement | `dag.relation_is_replicated(tid)` | `query/dag/meta.rs:235-239` |

Both per-table maps are built lazily, not eagerly — `get_or_build_schema_wire_block`
(`executor.rs:228-235` → `runtime/protocol/wire.rs:123-148`) handles the miss and
awaits nothing, so building them inside a read-guarded handler is sound.

**The decisive simplification** is that an ad-hoc DML statement resolves exactly
one relation. `reject_derivation` (`gnitz-sql/src/dml/select.rs:47-53`) rejects
JOIN, set operations, EXISTS/IN subqueries, scalar subqueries, derived tables in
FROM, and non-pass-through CTEs — the remedy is a view, which resolves as one
relation. UPDATE/DELETE/INSERT each target one relation. So a statement's whole
catalog need is **one descriptor**, and one round trip fetches it.

**Why the descriptor is a hint and not a trusted cache.** A cached descriptor's
staleness is a *name → tid binding* staleness. The only per-relation validators
on the wire — `schema_version` (bits 24–39) and `index_version` (bits 40–47) —
are keyed by **tid**, so they structurally cannot certify a binding: after
`ALTER TABLE t RENAME TO u; CREATE TABLE t`, a request stamped with the old
descriptor's tid is asking about `u` and every tid-keyed check agrees it is
fresh. Three further facts close off making them do this job:

- `schema_version` is not a validation token, it is an **elision** token:
  `wire_should_include_schema(cv, sv) = cv == 0 || cv != sv`
  (`gnitz-wire/src/flags.rs:290-292`), and the decoder hard-fails when a data
  frame arrives with the block elided but nothing in `Session::schema_cache` —
  `DecodeError("FLAG_HAS_DATA without FLAG_HAS_SCHEMA and no cached schema")`
  (`gnitz-core/src/protocol/message.rs:389-395`).
- Only bits **55 and 63** are free — 2 bits, not another 16-bit field.
- There is no verb to hang a binding check on for the common paths anyway: the
  plain scan carries no flag (`executor.rs:1222`), and `roundtrip_push`'s
  `SchemaMismatch` recovery re-sends to the **same `target_id`**
  (`connection.rs:589-605`), so it heals schema identity and cannot heal a
  binding.

So the binding must be validated by a request that **carries the name**, against
the map that owns it (`entity_by_qname`) — which is the resolve RPC itself.
Resolving live on each statement makes that entire class of defect unreachable.

## Design

### 1. `FLAG_RESOLVE`: one master-served relation descriptor

A new request verb on **bit 54** — the bit `FLAG_GET_INDICES` occupies today.
Resolve subsumes it, so this costs zero new flag bits.

**Request:** control-only. `target_id` carries the hint's tid (0 when none). The
`seek_pk_extra` blob carries the canonical `(schema_name, relation_name)`. The
flags word carries `wire_flags_set_schema_version(FLAG_RESOLVE,
session.cached_schema_version(hint_tid))` and the hint's `index_version` in bits
40–47. Bits 24–39 therefore keep **exactly** the meaning they already have —
"the version of the schema this session has cached under that tid"
(`connection.rs:456-458`, `:476-478`) — so no existing reader of those bits
changes, and there is no second schema cache to keep in step.

**Reply**, built under `shared.catalog_rwlock.read()` (the lock
`handle_get_indices`'s dispatch arm already takes, `executor.rs:1186-1197`):

- **Not found** → `STATUS_ERROR` with the schema-qualified "not found" text
  `lookup_schema_id` / `lookup_table_record` produce today. Read from the live
  catalog under the lock, so it is authoritative — never a stale miss.
- **Found** → `STATUS_OK`, flags stamped with the server's `schema_version` and
  `index_version`, and:
  - the **control blob** carries the live `tid`, the relation kind, the packed pk
    column list, the `relation_is_replicated` bool, and the index list as
    `(packed_cols u64, is_unique u8)` pairs — 9 bytes per index;
  - the **schema block** carries the relation's columns, elided iff
    `req.target_id == live_tid && !wire_should_include_schema(cv, sv)`.

**The reply never carries a data batch.** That is deliberate. A frame has one
schema-block slot and one data-batch slot (`WireMsg`,
`runtime/protocol/wire.rs:357-395`), and a data block with the schema elided is
decoded against `Session::schema_cache[target_id]`
(`protocol/message.rs:406-425`). Shipping the index list as an INDEX_META `Batch`
while eliding the relation's schema block — the ordinary state after a
`CREATE INDEX`, where `schema_version` matches but `index_version` does not —
would pass the hint branch's version check at `message.rs:389-395` and silently
decode INDEX_META rows against the relation's schema. Putting the list in the
control blob makes the reply shape invariant (schema block present or absent,
nothing else), so that misdecode is unreachable.

It is also smaller. Measured, an INDEX_META `Batch` costs a 200 B fixed block +
104 B for the first row + 32 B per row to convey 9 bytes of real information per
index (`executor.rs:1691-1694`); the control blob costs the 9 bytes. The client
currently **discards** the reply's `seek_pk_extra`
(`let (ctrl_header, error_msg, _seek_pk_extra) = …`, `message.rs:368`), so
`Message` gains one field — a smaller change than a bespoke multi-frame receive
path, which `drain_reply_train` could not serve anyway (it merges one schema and
concatenates data batches, `connection.rs:373-394`).

**The `req.target_id == live_tid` conjunct is what makes rename and
drop-and-recreate self-correcting**: the hint simply fails to match, the full
descriptor is sent, and the statement plans against the new relation. No error,
no retry, no invalidation rule. Eviction from `Session::schema_cache` yields a
stamped version of 0, which also forces the block — self-healing in the same way.

**The reply must carry every field a `gnitz_core::Schema` has**, specifically
`is_serial`, `fk_table_id`, and `fk_col_idx`. This is not optional: `drop_column`
reads `cd.is_serial` and `cd.fk_table_id` off the schema returned by
`resolve_table_id` (`ddl/alter.rs:214-224` via
`resolve_alter_base_table_with_schema`, `alter.rs:378-384`), not off a scan.
Omitting them would silently accept
`ALTER TABLE t DROP COLUMN <fk_col>`, which is rejected today. (The `fk_table_id`
assignments at `ddl/table.rs:343` and `:386` are **writes** into the planner's own
freshly-built column list, not reads — they do not license omitting the fields.)

Wire cost: three new per-column fields in the schema block. `is_serial` and a
2-bit-wide FK presence marker fit the free bit 3 of `pack_col_meta_flags`
(`gnitz-wire/src/flags.rs:394`, which uses bits 0–2 and 8–15); the FK target
`(table_id, col_idx)` needs two columns in `meta_schema()`. Engine-side,
`ColumnDef` (`catalog/types.rs:7-17`) gains `is_serial` — COL_TAB already stores
it and `scan_column_defs` (`registry.rs:73-80`) is the only production
constructor, but the struct has **53 literal construction sites** in
`gnitz-engine` and derives no `Default`. The masks thread through
`build_schema_wire_block` → `schema_to_batch` (`runtime/protocol/wire.rs:92-170`)
alongside the existing `hidden_mask` at **22 call sites**, `pack_col_meta_flags`
gains a parameter (production callers at `flags.rs:393`, `codec.rs:22`,
`wire.rs:170`), and the client's `schema_to_batch` / `batch_to_schema` pair
(`protocol/codec.rs:9-31`, `:36-81`) must round-trip them or the test at
`codec.rs:260` regresses. Budget ~75 mechanical edits, not a handful.

### 2. The client hint map

`GnitzClient` replaces `catalog_snapshot`, `indices_refreshed`, and `index_cache`
with one map keyed by canonical qualified name:

```rust
struct RelHint {
    tid: u64,
    is_view: bool,
    replicated: bool,
    index_version: u8,
    indexes: Arc<Vec<IndexMeta>>,
}
/// Send-only hint, never a substitute for resolving. Bounded LRU; eviction
/// costs one un-elided reply, never correctness. The schema is NOT held here —
/// `Session::schema_cache` owns it, so the two can never disagree.
hints: LruCache<String, RelHint>,
```

`resolve_table_id`, `resolve_table_or_view_id`, `resolve_relation_kind`,
`table_replicated`, `table_indexes`, and `index_for_column` are reimplemented over
one private `resolve(name)` that **always sends `FLAG_RESOLVE`**, passing whatever
hint it holds and installing the reply. The schema is installed into
`Session::schema_cache[live_tid]` with the reply's version — the same (schema,
version) pairing `recv_cached` performs from a frame's own flags
(`connection.rs:504-507`) — so the warm-push contract at `executor.rs:1032-1034`
sees a resolve-seeded entry and a scan-seeded entry identically. The `gnitz-py`
async transport builds a bare `Session` with no `GnitzClient`
(`gnitz-py/src/lib.rs:2091`) and holds no hints, so it is untouched.

**The statement bracket is narrowed, not deleted.** It becomes a per-statement
memo of resolved names, so one statement resolves each name once. This is
load-bearing, not an optimization: `resolve_alter_base_table_with_schema`
(`alter.rs:378-384`) resolves the same name twice within one statement. The
`scan_catalog` snapshot branch is deleted; `scan_catalog` becomes a plain scan for
the DDL callers (§3).

**This is strictly more consistent than today.** Today a statement's three
catalog scans are three unsynchronized round trips at three SAL cuts
(`client.rs:322-334`), so a concurrent DDL can hand one statement a TABLE_TAB row
and a newer COL_TAB. The resolve builds the whole descriptor under one read
guard. The residual window — a DDL landing between the resolve and the read — is
exactly today's plan-vs-execute window, unchanged.

### 3. DDL keeps the scan path

DDL must reproduce byte-identical `-1` retraction rows for the engine's CAS, so
it reads raw catalog rows rather than a projected descriptor: `lookup_schema_id`,
`lookup_table_record`, `find_view_record`, `extract_col_entries`, and
`alter_col_pair` stay, over a plain (unsnapshotted) `scan_catalog`.
`alter_col_pair` does its own `scan_catalog(COL_TAB)` (`client.rs:1423-1426`), so
the retraction rows are unaffected by §1's projected schema.

Two whole-IDX_TAB scans also stay on the DDL path: `index_name_cols`
(`client.rs:680-697`, the `CREATE INDEX` duplicate-name probe) and
`drop_index_by_name` (`client.rs:631-670`). §1's argument that an IDX_TAB scan is
the wrong shape for the *DML* index list does not reach them; they are part of
the DDL half named in the Scope note.

DDL statements resolve their names through §2 like everything else, then scan for
the raw rows they must retract. A multi-source `CREATE VIEW` therefore pays one
resolve per source **plus** the DDL scans, where today the statement snapshot
shared three scans across all sources — a small round-trip increase on that one
statement shape, on a path that is 10 277 of 209 290 requests (4.9%) and already
fsync-bound.

Nothing here is a correctness requirement: the server re-validates the FK gate
authoritatively (`validate_fk_column`), and every resolve is live.

### 4. Deletions and one behaviour decision

Deleted:

- `FLAG_GET_INDICES`, `handle_get_indices` and its dispatch arm
  (`executor.rs:1186-1197`, `:1669-1690`), `Session::fetch_indices`,
  `GnitzClient::refresh_indices`, `index_cache`, `indices_refreshed`, and
  `SCHEMA_CACHE_CAP` at `client.rs:13` (its only user; the unrelated same-named
  constant at `connection.rs:24` stays).
- `catalog_snapshot` and the snapshot branch of `scan_catalog`.
- `IndexListMemo` (`access.rs:650-666`) — its premise ("`table_indexes` ALWAYS
  hits the wire") becomes false. The range→equality collector fall-through then
  reads the hint twice per statement, which is free.
- The stale GET_INDICES doc references at `access.rs:511`, `access.rs:1629`, and
  `dml/plan.rs:98`, and the `table_indexes` / `index_for_column` doc comments
  (`client.rs:490-518`).

Retained: the per-table `index_version` counter, its bump and its purge — it is
the index-elision validator the resolve reply uses. `get_index_version` and the
`metadata.rs:80` pass-through stay.

**`pk_bound_is_preemptible` (`access.rs:255-276`) keeps its behaviour; only its
comment changes.** Its second clause is justified today by "`table_indexes`
always hits the wire — this keeps the GET_INDICES probe off the common
`WHERE pk > x` read", which stops being true. But the clause is a real semantic
guard independent of that cost — a PK bound with nothing pinned is only worth
abandoning when there is an index-eligible equality to build a unique point
from — so the predicate stands and the plan shapes it produces are unchanged.
Rewrite the comment to state that criterion; do not delete the guard.

No new status code, no new `ClientError` variant, no client-side invalidation
rule, no statement retry, and no changes to any existing verb's frames.

## Accounting

| Statement | Today | After (warm hint) | After (cold / post-DDL) |
|---|---|---|---|
| `SELECT … WHERE x = 5` | 5 RT, ~10 KB | **2 RT, ~0.3 KB** | 2 RT, ~0.7 KB |
| `SELECT … WHERE id = 2` | 4 RT, ~10 KB | **2 RT** | 2 RT |
| `SELECT * FROM t` | 4 RT, ~10 KB | **2 RT** | 2 RT |
| `INSERT` (warm push) | 4 RT, ~10 KB | **2 RT** | 2 RT |
| `UPDATE … WHERE x = 5` | 6 RT | **3 RT** | 3 RT |
| `SELECT * FROM <view>` | 5 RT | **2 RT** | 2 RT |
| DDL | 3–4 scans + push | 1 resolve/name + DDL scans + push | same |

Measured block sizes behind those numbers: a schema block is ≈344 + 56·N bytes
(624 B at 5 columns, 2 584 B at 40); a control-only reply is 256 B; the index
list is 9 B/index in the blob.

**What the hint is worth, stated against the real alternative.** Against a
resolve that always sends the full reply, the hint saves the schema block —
~0.8 KB per statement at a 5-column table — and **zero** round trips. On the
10 267 B baseline: always-full resolve removes 89.5% of the catalog bytes, the
hint takes it to 97.5%. The 4–6 → 2 collapse is entirely the resolve's doing.
The hint earns its place because it is now four fields and a `LruCache`, with no
invalidation rule and no second copy of the schema.

## Tests

Engine unit (`catalog/tests/`, `runtime/tests/`):
- resolve: found / not-found; a view resolves through the same `entity_by_qname`
  and `pk_col_of` paths as a table; `is_serial` and the FK target round-trip
  through the schema block.
- elision: a matching `(target_id, schema_version)` elides the schema block; a
  hint whose tid is stale (rename or drop-and-recreate) elides it **even when the
  version numbers coincide** — the `req.target_id == live_tid` conjunct; a stamped
  version of 0 always forces the block.
- the reply never sets `FLAG_HAS_DATA`, at any index count.

Client/planner unit (`gnitz-sql`):
- `plan/index_bound.rs` closure-counter tests rewritten (the memo and its
  `calls == 1` assertions are deleted with `IndexListMemo`).
- a statement resolves each name exactly once, including
  `ALTER TABLE … DROP COLUMN`, which resolves it twice without the memo.
- `DROP COLUMN` still rejects a SERIAL column and an FK-carrying column when the
  schema came from a resolve rather than a scan — the regression guard for §1's
  field list.

E2E (`gnitz-py`, `GNITZ_WORKERS=4`, new `tests/test_relation_resolve.py`) — each
runs the second client's statement with a warm hint, since these are the shapes a
trusted cache would get wrong:
- client A `ALTER TABLE t RENAME TO u` then `CREATE TABLE t`; idle client B's next
  bare `SELECT * FROM t` (the plain-scan path) returns the **new** t's rows, and
  its next `INSERT INTO t` writes into the **new** t.
- client A `DROP TABLE t` then recreates it; B's next `SELECT` and `INSERT`
  succeed against the new relation with no error and no reconnect.
- client A `CREATE INDEX` on B's table; B's next statement plans against it
  (the `index_version`-only mismatch — the case that would misdecode under a
  data-block reply).
- client A `DROP INDEX` on a U128-bound column; B's next `SELECT … WHERE
  wide = ?` replans without an index. (An `exact` index bound on a dropped index
  is a hard engine error — `catalog/scan_spec.rs:196-217` →
  `catalog/store_io.rs:160-172`; `exact` is set only when the WHERE cannot
  compile to a wire predicate, `gnitz-sql/src/dml/plan.rs:188-230`.)
- client A `ALTER COLUMN … DROP NOT NULL`; B's next binary `push` through the
  **raw** surface (`resolve_table` + `push`) succeeds without reconnecting.
- own-DDL: `CREATE INDEX` then `SELECT` on the same connection plans against the
  new index; `CREATE TABLE` then immediate `INSERT`/`SELECT` works.
- repeated SELECTs with no DDL in between issue exactly two requests each, and
  the resolve reply stays flat as the catalog grows.

## Sequencing

- [ ] **Schema-block fields:** `is_serial` and the FK target through
  `pack_col_meta_flags` / `meta_schema()` / `build_schema_wire_block` /
  `schema_to_batch`, the engine `ColumnDef` field and `scan_column_defs`, and the
  client `codec.rs` round-trip.
- [ ] **`Message` surfaces the reply control blob** (`message.rs:368` currently
  discards `seek_pk_extra`).
- [ ] **The resolve verb:** `FLAG_RESOLVE` on bit 54; `handle_resolve` serving
  from `schema_by_name` / `entity_by_qname` / `pk_col_of` / `schema_wire_cache` /
  `index_circuits` / `dag.relation_kind` / `dag.relation_is_replicated`, with the
  tid-conjoined schema elision and the blob-packed index list; delete
  `handle_get_indices` + its dispatch arm, `FLAG_GET_INDICES`,
  `Session::fetch_indices`; engine unit tests.
- [ ] **Client switch:** the `hints` LRU and the private `resolve`, installing the
  schema into `Session::schema_cache`; the six resolver methods reimplemented over
  it with rewritten docs; the statement bracket narrowed to a per-name memo; the
  deletions and the `pk_bound_is_preemptible` comment in §4.
- [ ] **E2E suite** per the test list above; full `make verify` + `make e2e`.
