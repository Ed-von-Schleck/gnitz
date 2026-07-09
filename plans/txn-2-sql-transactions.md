# SQL transactions: BEGIN / COMMIT / ROLLBACK

## Problem

The SQL surface has no transaction statements. Every DML statement executes
immediately: `dispatch.rs`'s statement match has no transaction arms, so
`BEGIN`/`COMMIT`/`ROLLBACK` fall through to
`GnitzSqlError::Unsupported` (`gnitz-sql/src/dispatch.rs:16-54`). A user
cannot group an INSERT into `orders` and an UPDATE of `inventory` into one
atomic, durable unit from SQL.

**Engine-side prerequisite (stated as facts, implemented separately):**
`gnitz-core` exposes a `TxnBuffer` with **run-splitting** semantics — ops
append to a table's last family while the mode matches, else open a new
family, so per-table op order is preserved end to end (buffer call order =
frame order = engine validation-fold and worker-application order); deletes
are `-1` filler-payload rows in Update-mode families; schemas are taken as
`&Schema`; and `net_state(tid, pk_bytes) -> Option<BufferedNet>` returns
the owned net buffered state of a PK (`Present(one-row ZSetBatch)` /
`Deleted`) by folding the table's families in order.
`GnitzClient::commit_txn(TxnBuffer) -> Result<u64 /* zone LSN */, ClientError>`
ships one `FLAG_PUSH_TXN` frame; the engine validates the bundle as a unit
(Error-mode PK uniqueness cumulative in per-table frame order — a PK
"exists" for an Error family iff it is committed-and-not-deleted-by-an-
earlier-family or inserted-by-an-earlier-family; FK checked against the
post-transaction state) and commits it in one SAL zone — all-or-nothing on
crash, one fsync, one ACK. That surface is delivered by the user-table
transaction-frame work that precedes this plan; this plan consumes it and
adds nothing to it.

Everything this plan adds is client-side: parser arms, a transaction state
slot on the persistent client object, buffering interception at the three
DML write sites, and result/exception surface.

## Ground facts (verified against the live tree)

- `sqlparser = "0.56"` (`gnitz-sql/Cargo.toml:10`) already parses all three
  statements: `Statement::StartTransaction { modes, begin, transaction,
  modifier, statements, exception_statements, has_end_keyword }`,
  `Statement::Commit { chain, end, modifier }`,
  `Statement::Rollback { chain, savepoint }` (sqlparser-0.56.0
  `src/ast/mod.rs:3547-3608`). Plain `BEGIN` parses with `begin: true` and
  everything else empty/None; `END` parses as `Commit { end: true }`;
  `START TRANSACTION READ ONLY` lands in `modes`
  (`parser/mod.rs:14488-14558`). Bare `SAVEPOINT x` / `RELEASE SAVEPOINT x`
  are separate variants (`Statement::Savepoint`,
  `Statement::ReleaseSavepoint`, `ast/mod.rs:3900-3908`) that already fall
  to dispatch's `_ => Unsupported` arm and stay there.
- The only object that lives across `execute_sql` calls is the core
  `GnitzClient`: `SqlPlanner` and `Binder` are constructed per call
  (`gnitz-sql/src/lib.rs:33-58`, `gnitz-py/src/lib.rs:1741-1787`; the
  C API does the same, `gnitz-capi/src/lib.rs:1337-1378`). Transaction
  state therefore lives on `GnitzClient`.
- The three DML write sites to intercept:
  - INSERT (`gnitz-sql/src/dml/insert.rs:221-260`): three
    `client.push_with_mode` calls — `ConflictPlan::Error` (mode `Error`),
    `DoNothingPk` (client-side filter then mode `Error`), `DoUpdatePk`
    (client-side merge then one `Update`-mode push of the whole merged
    batch, merged conflict rows and non-conflicting inserts alike,
    `insert.rs:249-260`).
  - UPDATE (`gnitz-sql/src/dml/mutate.rs:293-303`): one
    `client.push_with_mode(..., WireConflictMode::Update)`.
  - DELETE (`gnitz-sql/src/dml/mutate.rs:333-352`): one
    `client.delete(table_id, schema, pks)`.
  All three sites hold the schema as `&Schema` (UPDATE/DELETE via
  `actual_schema = resolved.schema.as_deref().unwrap_or(&*schema)`,
  `mutate.rs:294,334`; INSERT holds an `Rc<Schema>`, which deref-coerces),
  matching `client.push_with_mode`'s parameter (`client.rs:278-284`).
- The client-side reads those statements perform (`resolve_where_rows`'s
  scan/seek/seek-by-index ladder, `mutate.rs:182-236`;
  `client_side_filter_do_nothing`, `insert.rs:325-356`;
  `client_side_merge_do_update`, `insert.rs:368-412`) read the server's
  committed state; the two ON CONFLICT helpers call `client.seek` inside
  their per-PK loops, so any buffer consultation must use owned data
  (`net_state` returns owned) fetched per PK, never a live buffer borrow
  across the seek.
- `SqlResult` (`gnitz-sql/src/lib.rs:22-31`) has no transaction variants.
  Python translates each variant into a dict with a `"type"` tag via an
  exhaustive match (`gnitz-py/src/lib.rs:1749-1783`); the C API also
  matches `SqlResult` exhaustively, flattening to a single `u64 out_id`
  (`gnitz-capi/src/lib.rs:1359-1366`) — both matches gain arms.
- RETURNING is supported only on the plain-INSERT path via client-side
  projection of the already-built batch (`insert.rs:230-236`, fallible —
  it rejects non-PK column lists); the autocommit code pushes *before*
  projecting.
- `SqlPlanner::execute` loops the parsed statements against the same
  client and aborts on the first error, discarding earlier results
  (`gnitz-sql/src/lib.rs:47-57`).
- A real zone LSN is never 0 (`ZoneLsnAllocator::reserve` is
  `max(seed, floor) + 1`, `runtime/orchestration/lsn.rs:42-46`), so 0 is
  free to mean "empty commit".

## Semantics (committed)

- **A transaction is a client-buffered write batch.** `BEGIN` opens a
  buffer on the client; DML statements build their batches exactly as
  today but append them to the buffer instead of pushing; `COMMIT` ships
  the buffer as one atomic frame; `ROLLBACK` discards it. No server state,
  no locks, no connection pinning between statements. Because the buffer
  preserves per-table op order (run-splitting), the frame the engine
  validates and applies **is** the statement order — `INSERT a; DELETE b;
  INSERT b` commits as exactly that sequence, and its outcome never
  depends on which unrelated statement happened to touch a table first.
- **Queries read the committed basis; mutations re-target through the
  transaction's own net writes.** Three precise layers:
  1. **SELECT** reads the committed basis, period. Buffered writes are
     invisible to queries.
  2. **UPDATE / DELETE row resolution**: candidate rows are *discovered*
     against the committed basis (the `resolve_where_rows` ladder decides
     which rows to fetch, unchanged), and then *matched and evaluated*
     against the transaction's effective payloads. Per fetched PK,
     `net_state` decides the effective row:
     - `Deleted` → excluded (otherwise `DELETE k; UPDATE k` would
       resurrect `k` at COMMIT — a state no sequential execution can
       produce);
     - `Present(row)` → the buffered payload replaces the committed one
       as the effective row;
     - untouched → the committed payload.
     Matched-ness is then decided by evaluating the WHERE against the
     **effective** row — specifically, the predicate part not already
     guaranteed by the fetch: on the `Filtered` paths that is the **full
     `where_expr`** (an index seek consumes its equality conjuncts out of
     the residual, and a `Present` payload can un-satisfy exactly those
     consumed conjuncts, so the residual alone is insufficient); on
     `PkSeek`/`PkMultiSeek` the PK-binding conjuncts are invariant under
     substitution, so the residual alone is exact. SET expressions
     compute over the effective rows. This makes consecutive UPDATEs
     compound (`SET x = x + 1` twice yields +2, as sequential execution
     does) and makes SET/WHERE evaluation after a delete-then-reinsert
     operate on the reinserted payload, never the committed ghost.
     Two divergences, documented:
     - **Transaction-born rows are undiscoverable**: a PK with no
       committed row is never fetched, so UPDATE/DELETE cannot match a
       row born in the transaction (`INSERT new k; UPDATE k` matches 0
       rows).
     - **Index-seek discovery sees committed index values only**: a
       `Filtered` access served by a secondary-index seek probes the
       committed index, so a row whose *buffered* payload newly satisfies
       an indexed WHERE (but whose committed payload does not) is not
       discovered. Exclusion is exact on every path; inclusion via an
       index seek is committed-only. (The scan paths have no such gap.)
  3. **ON CONFLICT conflict detection** consults the full net buffered
     state (`net_state`) in addition to the committed basis — resolving a
     conflict against a row the same transaction already buffered would
     otherwise manufacture a guaranteed commit failure (details below).
- **Constraint enforcement is at COMMIT, by the engine.** Plain INSERT's
  duplicate-PK rejection, unique secondary indexes, and FK checks all run
  server-side when the buffer commits — against the committed state at
  commit time, under the engine's table locks, with per-table statement
  order preserved. A duplicate INSERT therefore fails at COMMIT exactly
  when a sequential execution would have failed at the statement
  (`INSERT k` with `k` committed and not deleted earlier in the
  transaction → rejected; `DELETE k; INSERT k` → accepted). A conflicting
  concurrent commit between statement execution and COMMIT surfaces as a
  clean COMMIT error, not silent corruption.
- **Statement errors do not poison the transaction.** A statement that
  fails (parse, bind, resolve, build, RETURNING projection) has buffered
  nothing — every write site buffers only as its final, infallible step;
  in particular the RETURNING projection is validated and applied
  **before** the batch is buffered (the autocommit path pushes first,
  `insert.rs:224-236`; the transaction path reorders, which is safe since
  the batch is fully built either way). The transaction stays open and
  prior buffered statements stand. This deliberately diverges from
  PostgreSQL's abort-on-error, which exists to fence partial server-side
  effects; here a failed statement has no buffered effects (it may still
  have consumed SERIAL ids — gaps, as with ROLLBACK).
- **A single `execute_sql` call that opened the transaction rolls it back
  on error.** `SqlPlanner::execute` aborts the statement loop on the
  first error; if the failing call itself executed `BEGIN` (the common
  `execute_sql("BEGIN; ...; COMMIT")` shape) and the transaction is still
  open, the planner rolls it back before returning the error — otherwise
  the stranded open buffer would silently swallow the caller's subsequent
  "autocommit" statements. A transaction opened by an *earlier* call is
  left open (non-poisoning applies; the caller owns its lifecycle).
- **COMMIT consumes the transaction, success or failure.** On engine
  rejection (validation, SAL space, schema drift) the error surfaces and
  the transaction is closed with the buffer discarded; the application
  re-runs the whole transaction. `COMMIT` of an empty buffer performs no
  wire roundtrip and reports `lsn = 0` (unreachable as a real zone LSN).
- **DDL is rejected inside a transaction** with
  `DDL is not allowed inside a transaction`: the reachable catalog-mutating
  arms are `CreateTable`, `CreateView`, `CreateIndex`, and `Drop`
  (TABLE|VIEW|INDEX — `execute_drop` supports exactly those,
  `plan/ddl.rs:606-631`); there is no CREATE SCHEMA arm (it already falls
  to `Unsupported`). DDL has its own atomic commit mechanism, and
  interleaving catalog changes with batches buffered under the old schema
  would guarantee commit-time schema-mismatch failures. (Another
  *client's* concurrent DDL remains possible and surfaces as the engine's
  schema-mismatch error at COMMIT.)
- **SERIAL allocation happens at statement time** (ids are stamped into
  the batch before buffering, exactly as before pushing today); ROLLBACK
  leaves id-range gaps. Standard, documented.

## Statement surface

`dispatch.rs` gains three arms ahead of the existing ones:

- `Statement::StartTransaction { .. }` → `BEGIN` / `START TRANSACTION`.
  Rejected clauses (via the existing `reject_unhonored_*` pattern,
  `plan/validate.rs`): non-empty `modes`, `modifier`, `statements`,
  `exception_statements` — plain `BEGIN` only (`transaction` and
  `has_end_keyword` are inert parse artifacts). Errors if a transaction
  is already open: `transaction already open`.
- `Statement::Commit { chain, end, modifier }` → `COMMIT` (and `END`).
  Rejected: `chain`, `modifier`. Errors if no transaction is open:
  `no transaction open`.
- `Statement::Rollback { chain, savepoint }` → `ROLLBACK`. Rejected:
  `chain`, any `savepoint`. Errors if no transaction is open:
  `no transaction open`.
- `Statement::Savepoint` / `Statement::ReleaseSavepoint` stay unhandled
  (`_ => Unsupported`), which is the correct rejection.

All other statements consult the transaction state: DML buffers (below),
SELECT (`Statement::Query`) passes through unchanged, and the four DDL
arms error at the top when a transaction is open.

## Client-side state

`GnitzClient` (`gnitz-core/src/client.rs`) gains:

```rust
txn: Option<TxnBuffer>,          // None = autocommit (today's behavior)

pub fn txn_active(&self) -> bool;
pub fn txn_begin(&mut self) -> Result<(), ClientError>;      // Err if active
pub fn txn_rollback(&mut self) -> Result<(), ClientError>;   // Err if not active
pub fn txn_commit(&mut self) -> Result<u64, ClientError>;    // Err if not active
pub fn txn_buffer_mut(&mut self) -> Option<&mut TxnBuffer>;  // write access
pub fn txn_net_state(&self, tid: u64, pk: &[u8]) -> Option<BufferedNet>; // read access
```

`txn_commit` **takes the buffer out of the slot before the fallible
send** — so an engine-side COMMIT failure leaves the transaction already
closed (the "COMMIT consumes the transaction" rule is this one line), and
the opened-this-call rollback rule then finds no open transaction and does
nothing: no double-rollback path exists. All three state-machine errors
(`transaction already open` / `no transaction open`) are raised by these
client methods; the dispatch arms only translate them.

The SQL layer writes through the buffer's `push`/`push_with_mode`/`delete`
and reads through `txn_net_state` — which returns **owned** data, so the
ON CONFLICT helpers can consult it per PK between their `client.seek`
calls with no borrow of the client outstanding. The buffer-or-push
decision is a small helper used at each write site:

```rust
// gnitz-sql/src/dml/mod.rs, shared by insert.rs / mutate.rs
fn write_or_buffer(client: &mut GnitzClient, tid: u64, schema: &Schema,
                   batch: &ZSetBatch, mode: WireConflictMode) -> Result<Option<u64>, ClientError> {
    match client.txn_buffer_mut() {
        Some(buf) => { buf.push_with_mode(tid, schema, batch, mode); Ok(None) }
        None => client.push_with_mode(tid, schema, batch, mode).map(Some),
    }
}
fn delete_or_buffer(client: &mut GnitzClient, tid: u64, schema: &Schema,
                    pks: PkColumn) -> Result<(), ClientError>;   // same shape
```

(`&Schema` is the type all three sites can supply and the type
`push_with_mode` takes.) `Option<u64>` carries the autocommit LSN
(currently discarded by all callers, unchanged).

## DML behavior inside a transaction

- **Plain INSERT** (`ConflictPlan::Error`): build the batch exactly as
  today (SERIAL stamped, `insert.rs:150-219`), apply the RETURNING
  projection if present, then buffer with mode `Error`. Duplicate
  detection happens at COMMIT (engine-side, cumulative in statement
  order).
- **INSERT ... ON CONFLICT DO NOTHING**: the client-side existence filter
  (`client_side_filter_do_nothing`) checks, per PK: `net_state` first —
  `Present` ⇒ conflict (drop the row), `Deleted` ⇒ no conflict (keep) —
  else the committed state via the existing seek. Surviving rows buffer
  with mode `Error`. Without the buffer check, two DO NOTHING inserts of
  the same new PK inside one transaction would buffer two `+1` Error rows
  and fail the whole transaction at COMMIT — the opposite of DO NOTHING's
  contract.
- **INSERT ... ON CONFLICT DO UPDATE**: the client-side merge
  (`client_side_merge_do_update`) resolves the conflict source per PK as:
  `net_state` `Present(row)` ⇒ merge against that owned buffered row;
  `net_state` `Deleted` ⇒ no conflict — the transaction removed the row,
  so the insert applies as-is (merging against the committed row would
  resurrect data the transaction deleted); no net entry ⇒ the committed
  row via the existing seek. The whole merged batch buffers with mode
  `Update`, **ordered after** the run holding any earlier insert of the
  same PK (run-splitting appends to a matching trailing Update run or
  opens a new one; either way per-table order is preserved) — so at apply
  time the merged row wins by order, and at validation time the earlier
  Error-mode insert is checked *before* the merged row exists in the
  overlay, both matching statement order.
- **UPDATE**: discover via `resolve_where_rows` (unchanged), then build
  **one effective batch**: fetched rows are copied under `actual_schema`
  with each `Present` PK's row spliced in from its `BufferedNet` (which
  carries a one-row `ZSetBatch`, type-compatible by construction —
  buffered batches were built under the catalog schema) and `Deleted`
  PKs dropped. The WHERE (full `where_expr` on `Filtered` paths, residual
  on PK paths) and `write_set_rows` then run over that effective batch,
  exactly as they run over the reply batch today. The reported affected
  count is the surviving matched set. Buffer with mode `Update`.
- **DELETE**: same discovery + effective-batch construction + WHERE
  re-evaluation; buffer the surviving PK list via `TxnBuffer::delete`.
  The reported affected count is likewise the surviving matched set.

## Results and errors

`SqlResult` gains three variants:

```rust
TransactionStarted,
TransactionCommitted { lsn: u64 },     // lsn = 0 for an empty commit
TransactionRolledBack,
```

Python mapping (`gnitz-py/src/lib.rs`, exhaustive match at 1749-1783):
`{"type": "TransactionStarted"}`,
`{"type": "TransactionCommitted", "lsn": lsn}`,
`{"type": "TransactionRolledBack"}`. The C API's exhaustive `SqlResult`
match (`gnitz-capi/src/lib.rs:1359-1366`) gains the three arms:
`TransactionCommitted { lsn } => lsn`, the other two `=> 0`. No new C
entry points.

Errors keep the existing single-exception shape (`GnitzError` with the
flattened message). The transaction-control errors (`transaction already
open`, `no transaction open`, `DDL is not allowed inside a transaction`)
are ordinary `GnitzSqlError` strings.

A multi-statement `execute_sql("BEGIN; INSERT ...; COMMIT")` works with no
special handling beyond the opened-this-call rollback rule: the planner
loops statements against the same client, and the state lives on the
client, so transactions also span *multiple* `execute_sql` calls on one
connection (the buffer keys families by resolved tid, so a `schema_name`
change between calls cannot corrupt it).

`GnitzClient` drop / connection close with an open transaction discards
the buffer silently — identical to ROLLBACK, nothing was ever sent
(`PyGnitzClient.__exit__` only closes the socket,
`gnitz-py/src/lib.rs:1495-1507`).

## Tests

**Rust unit (gnitz-sql):** dispatch arms — state-machine errors, and the
**complete** rejected-clause matrix: `BEGIN READ ONLY` (modes),
BEGIN with `modifier` / `statements` / `exception_statements`,
`COMMIT AND CHAIN`, COMMIT with `modifier`, `ROLLBACK AND CHAIN`,
`ROLLBACK TO SAVEPOINT x`; where `GenericDialect` cannot parse a variant,
the unit test constructs the `Statement` AST value directly. Bare
`SAVEPOINT`/`RELEASE SAVEPOINT` stay Unsupported. `write_or_buffer` /
`delete_or_buffer` routing; opened-this-call rollback on mid-loop error;
`txn_commit` closes the transaction before the fallible send.

**E2E (pytest, `GNITZ_WORKERS=4`):**
- `BEGIN; INSERT a; INSERT b; COMMIT` — atomic across two tables; a view
  joining both reflects both after the commit's scan.
- `ROLLBACK` discards: tables unchanged, SERIAL ids gapped, server never
  saw a frame.
- Statement-order semantics through the buffer (the run-splitting cases):
  - `INSERT new k1; DELETE k2 (committed); INSERT k2; COMMIT` — commits;
    k2 replaced (order preserved despite the earlier Error-family run).
  - `UPDATE t ...; INSERT k (committed, no prior delete); COMMIT` —
    COMMIT rejected with the duplicate-key error (the unrelated earlier
    UPDATE must not launder the duplicate).
  - `UPDATE t ...; INSERT new k; INSERT k ON CONFLICT DO UPDATE; COMMIT` —
    commits; the merged row wins.
- Read semantics: SELECT inside a transaction does not see buffered
  writes; UPDATE and DELETE of a transaction-born row (no committed
  counterpart) match 0 rows; `DELETE k; UPDATE k` matches 0 rows and `k`
  stays deleted after COMMIT (no resurrection); `DELETE k; DELETE k`
  reports 1 then 0; `UPDATE k SET x = x + 1` twice compounds (+2 after
  COMMIT); `DELETE k; INSERT k (new payload); UPDATE k SET ...` computes
  SET over the **reinserted** payload; both WHERE disagreement directions
  pinned on a **non-indexed** payload column: buffered payload fails a
  WHERE the committed payload passes → 0 rows (exclusion exact), and the
  delete-reinsert triple with a WHERE only the reinserted payload
  satisfies, discovered via scan → matches; the **index-seek divergence**
  pinned: an indexed WHERE satisfied only by the buffered payload does
  not discover the row (committed-only inclusion, documented), and an
  indexed WHERE whose consumed equality conjunct the buffered payload
  un-satisfies matches 0 rows (full-`where_expr` re-check).
- ON CONFLICT: two `DO NOTHING` inserts of the same new PK in one
  transaction → 1 row after COMMIT; `DO NOTHING` after `DELETE k` inserts;
  `DO UPDATE` conflicting with a row buffered earlier merges against the
  buffered row; `DO UPDATE` after `DELETE k` applies the insert as-is;
  `DO UPDATE` conflicting with a committed row merges against it.
- Constraint-at-COMMIT: plain INSERT of a PK committed by *another*
  connection after BEGIN but before COMMIT → COMMIT fails, both tables
  unmodified, transaction closed.
- FK across the transaction: insert parent + child in one transaction in
  either statement order commits; child referencing a parent deleted in
  the same transaction fails at COMMIT.
- Statement error mid-transaction (multi-call shape) leaves the
  transaction open and COMMIT commits the earlier statements; the
  single-call `execute_sql("BEGIN; <failing>; COMMIT")` shape rolls back
  and a subsequent INSERT autocommits normally.
- `INSERT ... RETURNING <unsupported list>` inside a transaction errors
  and buffers nothing (COMMIT of the otherwise-empty transaction is a
  no-op).
- DDL inside a transaction rejected (CREATE TABLE, CREATE VIEW, CREATE
  INDEX, DROP); `BEGIN` twice rejected; `COMMIT`/`ROLLBACK` without a
  transaction rejected; `END` behaves as COMMIT.
- RETURNING on plain INSERT inside a transaction returns the stamped ids
  before COMMIT; after ROLLBACK those ids are absent from the table.
- Transaction spanning multiple `execute_sql` calls; and the full
  `BEGIN; ...; COMMIT` in a single call.
- Empty transaction: `BEGIN; COMMIT` → `TransactionCommitted { lsn: 0 }`,
  no server frame.
