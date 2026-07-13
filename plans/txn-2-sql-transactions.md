# SQL transactions: BEGIN / COMMIT / ROLLBACK

## Problem

The SQL surface has no transaction statements. Every DML statement executes
immediately: `dispatch::execute_statement`'s match has no transaction arms, so
`BEGIN` / `COMMIT` / `ROLLBACK` fall through to the `_ => Unsupported` arm
(`gnitz-sql/src/dispatch.rs:50-53`). A user cannot group an INSERT into `orders`
and an UPDATE of `inventory` into one atomic, durable unit from SQL.

The engine and the core client already implement an atomic multi-table
**write-batch** transaction (delivered by the preceding user-table
transaction-frame work). What is missing is the SQL front end that drives it —
and the **read-your-own-writes** layer that makes a sequence of SQL statements
inside `BEGIN … COMMIT` behave the way SQL users expect. That layer does **not**
exist yet and is the substance of this plan.

## What the prerequisite delivers (verified against the live tree)

The core client (`gnitz-core/src/client.rs`) exposes a **write-only** `TxnBuffer`
and a two-call commit path:

- `struct TxnBuffer` (`client.rs:1234-1293`) with private fields
  `families: Vec<(u64, Schema, ZSetBatch, WireConflictMode)>` and
  `last_family_of: HashMap<u64, usize>`. Public methods: `push`, `push_with_mode`,
  `delete(tid, &Schema, PkColumn)` (builds `-1` filler-payload rows via
  `retraction_batch`, `client.rs:111-123`, appended mode `Update`). **Run-splitting**
  (`append`, `client.rs:1279-1292`): extend the tid's last family when the mode
  matches, else open a new family; empty batches contribute nothing. Per-tid op
  order is preserved end to end: buffer call order = family frame order.
- **No read-back.** `TxnBuffer` has no per-PK lookup and no families accessor;
  `families` is only ever written. (`net_state`, `BufferedNet`, etc. exist nowhere
  in compiled code.)
- `GnitzClient::begin(&mut self) -> TxnBuffer` (`client.rs:595-597`) returns an
  **owned** buffer; the client holds **no** transaction field (its fields are
  `session`, `index_cache`, `serial_cache`, `catalog_snapshot`, `client.rs:223-232`;
  the sole struct-literal construction is `connect`, `client.rs:236`).
- `GnitzClient::commit_txn(&mut self, buf: TxnBuffer) -> Result<u64, ClientError>`
  (`client.rs:602`) ships one `FLAG_PUSH_TXN` frame, returns the zone LSN, and
  **consumes** `buf`. An **empty** buffer returns `Ok(0)` with no wire roundtrip.

Engine-side invariants this plan relies on (all pre-SAL, under the union of the
involved table locks, covered by the prerequisite's passing tests in
`gnitz-py/tests/test_transactions.py`):

- **Validation is a whole-bundle fold** (`master/preflight.rs`). Per table, an
  `Overlay` folds the families in frame order, last op per PK wins (`fold_family`,
  `preflight.rs:561-576`: `w>0 ⇒ Inserted`, `w<0 ⇒ Deleted`). Error-mode PK
  existence is **cumulative in prefix-fold order** (`txn_check_pk`,
  `preflight.rs:1535-1573`): a PK "exists" for an Error family iff an earlier
  family Inserted it, or it is committed and no earlier family Deleted it. Unique
  secondary indices (U-SEC) and FK existence/RESTRICT are checked against the
  **post-transaction survivor set** (`preflight.rs:655-663`). These checks read
  *committed state at commit time*; there is **no** read-set / OCC / value-CAS
  check (see the isolation note in Semantics).
- **Apply is atomic and sequentially visible** (`committer.rs`,
  `query/dag/ingest.rs`). The N families emit as N `FLAG_PUSH` groups in one SAL
  zone under one sentinel — one fsync, one ACK, all-or-nothing on crash. Workers
  apply them in frame order and each family's `enforce_unique_pk`
  (`ingest.rs:295-366`) sees prior families' store writes; intra-family duplicate
  PKs resolve **last-write-wins in row order** (`ingest.rs:342-349`), and a `-1`
  for a PK neither stored nor inserted is dropped (no negative phantom). UPDATE and
  DELETE buffer only the new `+1` row (or a `-1` filler); the old-row retraction is
  synthesized at apply by `enforce_unique_pk`'s retract-before-insert
  (`ingest.rs:332-339`). This is what makes `DELETE k; INSERT k` (the replace
  idiom) commit as the reinserted value, verified by
  `test_txn_replace_idiom_delete_then_error_insert`.

**Client fold ≡ engine state fold (the correctness pin).** Every engine fold that
produces *state* is pure last-op-by-weight-sign, last-write-wins per PK, with no
weight summing: the preflight model `fold_family` (which projects the FK/U-SEC
survivor set, `preflight.rs:655-661`) and the apply-time `enforce_unique_pk`
(retract-before-insert + intra-family last-insert-wins in buffer order,
`ingest.rs:332-362`). The client's `net_of` (below) folds identically, so the
effective state the client reads for UPDATE/DELETE/ON CONFLICT is exactly the state
the engine commits. **Buffer order *is* apply order:** `handle_push` ingests each
group without consolidate/sort (`worker/mod.rs:915`), the exchange scatter preserves
per-worker relative order (`preflight.rs:2344`), and same-PK rows co-partition — so
the family order and within-family row order the client buffered survive to
`enforce_unique_pk` unchanged; no reordering can flip an outcome. The **one** engine
fold that is *not* last-op-by-sign is the Error-mode duplicate *rejection* check
(`txn_check_pk` via `pk_net_weight_and_dup_count`, `preflight.rs:585-599, 1546-1571`):
it rejects any Error-mode family whose per-PK positive-occurrence count is `> 1`
(saturating; deletes in the same family do not clear it). The client deliberately
does **not** mirror this — plain-INSERT duplicates are a COMMIT-time whole-transaction
abort *by design* (see Semantics), and the net-buffered ON CONFLICT checks below keep
a `DO NOTHING`/`DO UPDATE` from ever buffering a second `+1` for a live PK, so only a
genuine plain-INSERT duplicate ever trips it.

Everything below is new work: the client-side transaction slot, the SQL parser
arms, the **read-your-own-writes overlay**, and the result/exception surface.

## Semantics (committed)

**A transaction is a client-buffered write batch, computed against a
read-your-own-writes view of committed state plus the transaction's own buffered
writes.** `BEGIN` opens a buffer on the client; each DML statement builds its
batch exactly as autocommit does but (a) reads the *effective* pre-statement state
(committed ∪ this transaction's net buffered writes) where it needs to read, and
(b) appends its batch to the buffer instead of pushing. `COMMIT` ships the buffer
as one atomic frame; `ROLLBACK` discards it. No server state, no locks, no
connection pinning.

The core requirement is **sequential correctness**: the committed outcome equals
what running the statements one-at-a-time would produce. The engine applies
buffered families in frame order, sequentially visible, with intra-family
last-write-wins — so the committed outcome is sequential *iff each statement's
buffered delta is correct given the state after all prior buffered statements*.
UPDATE/DELETE deltas depend on current row values and ON CONFLICT depends on
current existence, so each such statement **reads the transaction's own buffered
writes**. A plain INSERT's delta is positional (independent of current state), so
it buffers blindly.

Three read layers:

1. **SELECT reads the committed basis, period.** Buffered writes are invisible to
   queries — the transaction is a staged write-set, and a SELECT reads the current
   committed database. A SELECT resolves a base table *or* a view through one
   seek/scan path (`binder.resolve` distinguishes them but returns a table-like
   relation for both): a **view**'s content is an engine-side circuit the client
   cannot re-derive over the transaction's uncommitted base deltas, so a view read is
   necessarily committed-only. This plan makes **base-table** SELECTs committed-only
   too, deliberately. Base-table read-your-own-writes *is* achievable — it would reuse
   `net_of`/`build_effective` — but only by threading the overlay through direct
   SELECT's **own**, distinct access-path ladder (`select.rs` has a range-index path
   and no full-scan fallback, neither of which `resolve_where_rows` shares), a
   separable body of work kept out of this write-path-focused plan. So SELECT is
   uniformly committed-only here. Two documented consequences of the staging model:
   `INSERT k; SELECT … WHERE k` returns no rows (invisible until COMMIT), and
   `UPDATE … WHERE k` reports "1 row affected" — a forward-looking count of rows the
   staged UPDATE will change at COMMIT — while a following `SELECT … WHERE k` returns
   0 (the committed row still holds its old value). These are **limitations** of the
   staging model, not features: a SQL user expecting read-your-own-writes for SELECT
   does not get it in this version, and there is no RETURNING escape hatch on
   UPDATE/DELETE or ON CONFLICT (only plain INSERT has RETURNING).

2. **UPDATE / DELETE resolve against the effective state** (committed ∪ buffered
   net). Candidate rows are the committed rows the existing access-path ladder
   fetches *net-overridden by the buffer*, plus the transaction's own buffered
   rows; the WHERE is evaluated against the **effective** rows. Per the
   transaction's net fold of a PK:
   - `Deleted` → excluded (`DELETE k; UPDATE k` matches nothing, no resurrection);
   - `Present(row)` → the buffered payload is the effective row;
   - untouched → the committed payload.
   Consecutive UPDATEs compound (`SET x = x+1` twice yields +2); a delete-then-
   reinsert operates on the reinserted payload; a row born in the transaction
   (`INSERT new k; UPDATE k`) is discovered and matched, and `INSERT k; DELETE k`
   removes `k`. The buffered side is unindexed and predicate-filtered, so inclusion
   via **every** access path is exact (no index-seek divergence).

3. **ON CONFLICT conflict detection consults the net buffered state** in addition
   to the committed basis. `Present` ⇒ conflict; `Deleted` ⇒ no conflict; untouched
   ⇒ the committed seek decides. Without this, two `DO NOTHING` inserts of the same
   new PK would buffer two Error `+1` rows and fail the whole transaction at COMMIT
   — the opposite of `DO NOTHING`'s contract — and a `DO UPDATE` conflicting with a
   row the transaction already inserted would Error-insert it twice and guarantee a
   commit-time PK violation.

**Isolation guarantee — scope.** A gnitz transaction provides **atomicity and
constraint consistency**, not write-conflict / lost-update protection. Constraint
enforcement (plain-INSERT duplicate PK, unique secondary indices, FK) runs at
COMMIT, engine-side, against committed state under the involved table locks, with
per-table statement order preserved — so a duplicate INSERT fails at COMMIT
exactly when a sequential run would fail (`INSERT k` with `k` committed and not
deleted earlier → rejected; `DELETE k; INSERT k` → accepted), and a **constraint**
conflict from another client's concurrent commit surfaces as a clean COMMIT error.
But there is **no** read-set/OCC/value check: a value-dependent write
(`UPDATE t SET x = x + 1`) computes its new payload from a client read taken at
*statement* time, and a concurrent commit that changes that row's value between
the statement and COMMIT is **silently lost** (a classic lost update). This is the
same read-modify-write race autocommit already has, widened to the whole
transaction span — which, since a transaction may span multiple `execute_sql`
calls with user think-time between them, can be long. Write-conflict detection is
explicitly out of scope for this plan.

Other rules:

- **Statement errors do not poison the transaction.** A statement that fails
  (parse, bind, resolve, build, RETURNING projection) has buffered nothing — every
  write site buffers only as its final, infallible step; the RETURNING projection
  runs **before** the batch is buffered. The transaction stays open and prior
  buffered statements stand. (A failed statement may still have consumed SERIAL ids
  — gaps, as with ROLLBACK.) *Caveat, by design:* a plain-INSERT **duplicate PK**
  is a COMMIT-time constraint check, not a statement-time error — so
  `INSERT k(committed); …` does not "skip the INSERT and proceed"; it makes the
  whole transaction abort at COMMIT (matching PostgreSQL, where a duplicate INSERT
  aborts the transaction). Statement-time non-poisoning covers errors the client
  detects while building a batch; COMMIT-time constraint violations abort the whole
  bundle. Because run-splitting coalesces consecutive same-mode statements into one
  family and the engine's duplicate check is per-family on a positive-occurrence
  count, two plain INSERTs of the same PK — across statements in one Error run, or
  as duplicate rows in a single `INSERT … VALUES` — abort the transaction at COMMIT
  (the ON CONFLICT paths never buffer a second `+1` for a live PK, so only genuine
  plain-INSERT duplicates trip it). The client does not pre-detect this; it is the
  engine's per-family `pos > 1` rejection, surfaced as a clean whole-transaction abort.
  **Divergence from PostgreSQL, documented loudly:** a *non-constraint* statement
  error (bind/resolve/build/RETURNING) does **not** abort the transaction the way
  Postgres does — the transaction stays open, prior buffered work survives, and a
  later COMMIT commits it. An application that wants Postgres "any error aborts the
  txn" semantics must issue `ROLLBACK` itself on a statement error.
- **A single `execute_sql` call that opened the transaction rolls it back on
  error.** `SqlPlanner::execute` aborts its statement loop on the first error; if
  the transaction was **not** active when the call began but is active at the error
  (this call's `BEGIN` opened it), the planner rolls it back before returning —
  otherwise the stranded open buffer would silently swallow the caller's subsequent
  autocommit statements. A transaction opened by an *earlier* call is left open
  (the caller owns its lifecycle). **Consequence to document:** the same statement
  sequence has different durability depending on how it is split across `execute_sql`
  calls — `execute_sql("BEGIN; good; <bad>; COMMIT")` auto-rolls-back and commits
  nothing, whereas the same four as separate calls leave the txn open with `good`
  buffered and a later `COMMIT` commits it. Call boundaries are semantically
  meaningful here.
- **Open transactions and connection reuse (pooling landmine).** An open `BEGIN`
  from an earlier `execute_sql` call captures every subsequent statement on that
  connection into the buffer until an explicit COMMIT/ROLLBACK; a dropped or reused
  connection discards the buffer (`Drop` == ROLLBACK). There is no server-side state
  (no locks, no snapshot, no pin), so an abandoned transaction leaks only client RAM
  — which is why no transaction timeout is needed — but any connection-pool or reuse
  path **must** assert `!client.txn_active()` (or force a rollback) on handoff, or it
  will silently fold a borrower's autocommit writes into a stranger's transaction.
- **COMMIT consumes the transaction, success or failure.** On engine rejection the
  error surfaces and the transaction is closed with the buffer discarded; the
  application re-runs the whole transaction. `COMMIT` of an empty buffer performs no
  wire roundtrip and reports `lsn = 0` (a real zone LSN is never 0:
  `ZoneLsnAllocator::reserve` is `max(seed, floor)+1`).
- **DDL is rejected inside a transaction** with `DDL is not allowed inside a
  transaction`; the rejection is a non-poisoning statement error (the transaction
  stays open). DDL has its own atomic commit mechanism, and interleaving catalog
  changes with batches buffered under the old schema would guarantee commit-time
  schema-mismatch failures. (Another *client's* concurrent DDL surfaces as the
  engine's schema-mismatch error at COMMIT.)
- **SERIAL allocation happens at statement time** (ids are stamped before
  buffering); ROLLBACK leaves id-range gaps. Standard, documented.

## Client-side transaction state (gnitz-core)

`GnitzClient` gains one field (`connect`, `client.rs:236`, adds `txn: None` to its
struct literal) and holds the buffer across `execute_sql` calls (the buffer keys
families by resolved tid, so a `schema_name` change between calls cannot corrupt
it):

```rust
txn: Option<TxnBuffer>,          // None = autocommit (today's behavior)
```

and these methods (all state-machine errors are raised here; the dispatch arms only
translate them):

```rust
pub fn txn_active(&self) -> bool { self.txn.is_some() }

pub fn txn_begin(&mut self) -> Result<(), ClientError> {           // Err if active
    if self.txn.is_some() { return Err(ClientError::ServerError("transaction already open".into())); }
    self.txn = Some(self.begin());   // begin() -> TxnBuffer; resolves to owned before the field store
    Ok(())
}

pub fn txn_rollback(&mut self) -> Result<(), ClientError> {        // Err if not active
    self.txn.take().map(|_| ()).ok_or_else(|| ClientError::ServerError("no transaction open".into()))
}

pub fn txn_commit(&mut self) -> Result<u64, ClientError> {         // Err if not active
    // Take the buffer OUT before the fallible send: an engine-side failure leaves
    // the transaction already closed ("COMMIT consumes the transaction"), so the
    // opened-this-call rollback rule then finds nothing open — no double path.
    let buf = self.txn.take().ok_or_else(|| ClientError::ServerError("no transaction open".into()))?;
    self.commit_txn(buf)
}

pub fn txn_buffer_mut(&mut self) -> Option<&mut TxnBuffer> { self.txn.as_mut() }
pub fn txn_buffer(&self) -> Option<&TxnBuffer> { self.txn.as_ref() }
```

`TxnBuffer` gains one **read accessor** — the whole read-back surface the engine's
apply/validation fold implies — yielding a tid's families in frame order:

```rust
// gnitz-core/src/client.rs, impl TxnBuffer
pub fn iter_families_of(&self, tid: u64) -> impl Iterator<Item = &ZSetBatch> + '_ {
    self.families.iter().filter(move |(t, ..)| *t == tid).map(|(_, _, b, _)| b)
}
```

The conflict mode is not yielded: the fold decides Present/Deleted from the weight
sign alone (mirroring `fold_family`), so the mode would be dead. `ClientError` has no
dedicated transaction variant; `ServerError(String)` carries the message and
`GnitzSqlError::Exec` wraps it (`error.rs:8`, `From<ClientError>` at
`error.rs:26-30`). `GnitzClient` drop / `close` with an open transaction discards
`txn` by plain `Drop` — identical to ROLLBACK, nothing was ever sent
(`PyGnitzClient::close` just takes `inner` and closes the socket,
`gnitz-py/src/lib.rs:1428-1432`).

## The transaction overlay

A new module `gnitz-sql/src/dml/overlay.rs` holds exactly two items — `OwnedNet` and
`net_of` — which fold the buffer into an **owned** per-PK net view, shared by
`mutate.rs` (UPDATE/DELETE) and `insert.rs` (ON CONFLICT). Everything that touches
`ResolvedRows` or `write_set_rows` (`resolve_where_rows_txn`, `build_effective`,
`fetch_filtered_raw`) **must** live in `mutate.rs`: both are private to that file (no
`pub(crate)`), so a separate module cannot construct or call them. `copy_batch_row`
(`exec/batch.rs:8`) is `pub(crate)`, reachable from `overlay.rs`.

`net_of` reuses `PkTuple` as a map key (it is `Copy` and implements `Hash`/`Eq` on
`(stride, as_bytes())`, `types.rs:561-573`). `PkTuple`/`PkColumn` live in the client's
own native little-endian key domain — **not** the engine's OPK. The buffered-row keys
(`PkColumn::get_tuple`) and the seek keys the access paths build (`PkTuple::from_u128`
for `pk IN`, the PK-list-order packing for a PK point-seek) encode in that one domain,
so a given logical key is byte-identical however it enters the map — the overlay
inherits exactly the PK support of the autocommit seek paths it sits beside (no new
width or compound-key limitation, no OPK transform). Returning **owned** rows (not
borrows) lets callers then borrow the client mutably (`seek`, buffer) with no
outstanding buffer borrow — so there is one net type, not a borrow/owned pair.

```rust
use std::collections::HashMap;
use gnitz_core::{PkTuple, Schema, TxnBuffer, ZSetBatch};
use crate::exec::batch::copy_batch_row;

pub(crate) enum OwnedNet { Present(ZSetBatch), Deleted }   // Present = an owned one-row batch

/// Fold `tid`'s buffered families (frame order, row order) into PK → net-op, last
/// op per PK by weight sign — the same fold the engine applies (`fold_family` /
/// `enforce_unique_pk`). One pass; a superseded Present copy is overwritten in
/// place. Cost is O(buffered rows for `tid`) per resolving statement — acceptable
/// for the bounded transactions this surface targets (if a huge-buffer txn ever
/// makes the per-statement re-fold bite, the fix is an incrementally-maintained
/// map updated as each statement buffers, not a filter over this scan).
pub(crate) fn net_of(buf: &TxnBuffer, tid: u64, schema: &Schema) -> HashMap<PkTuple, OwnedNet> {
    let stride = schema.pk_stride() as u8;
    let mut net: HashMap<PkTuple, OwnedNet> = HashMap::new();
    for batch in buf.iter_families_of(tid) {
        for i in 0..batch.len() {
            let w = batch.weights[i];
            if w == 0 { continue; }
            let pk = batch.pks.get_tuple(i, stride);
            let op = if w > 0 {
                let mut one = ZSetBatch::new(schema);
                copy_batch_row(batch, i, &mut one, schema);
                OwnedNet::Present(one)
            } else {
                OwnedNet::Deleted
            };
            net.insert(pk, op);   // last op per PK wins; drops any superseded copy
        }
    }
    net
}
```

**UPDATE/DELETE discovery.** `resolve_where_rows` (`mutate.rs:179-233`) is left
as-is for autocommit; a sibling `resolve_where_rows_txn` (in `mutate.rs`, where
`ResolvedRows` and its private fields are visible) mirrors its four access-path
arms but merges the buffer and re-derives the match. Both live in `mutate.rs`;
`net_of`/`OwnedNet` are imported from `overlay.rs`. Per arm the committed side is
fetched exactly as autocommit fetches it, then `build_effective` overlays the
buffer and the arm's predicate re-derives `matched`:

```rust
// gnitz-sql/src/dml/mutate.rs
fn resolve_where_rows_txn(
    client: &mut GnitzClient, tid: u64, schema: &Schema, selection: Option<&Expr>,
) -> Result<ResolvedRows, GnitzSqlError> {
    let stride = schema.pk_stride() as u8;
    // Fold the buffer ONCE (owned; the `&client` borrow is released here, before the
    // `&mut client` fetches below). Only reached under `txn_active`, so unwrap is safe.
    let net = net_of(client.txn_buffer().unwrap(), tid, schema);
    match classify_access(selection, schema) {
        AccessPath::ScanAll => {
            let (schema_opt, committed, _) = client.scan(tid)?;
            let actual = schema_opt.as_deref().unwrap_or(schema);
            let eff = build_effective(committed.as_ref(), &net, actual, stride, true, &[]);
            let matched = (0..eff.len()).collect();
            Ok(ResolvedRows { schema: schema_opt, batch: Some(eff), matched })
        }
        AccessPath::PkSeek { pk, residual } => {
            let (schema_opt, committed, _) = client.seek(tid, &pk)?;
            let actual = schema_opt.as_deref().unwrap_or(schema);
            let eff = build_effective(committed.as_ref(), &net, actual, stride, false, &[pk]);
            let preds = bind_residuals(&residual, schema)?;
            let matched = matching_indices(&preds, &eff, actual)?;
            Ok(ResolvedRows { schema: schema_opt, batch: Some(eff), matched })
        }
        AccessPath::PkMultiSeek { pks } => {
            let (schema_opt, committed) = seek_pk_multi(client, tid, schema, &pks, None)?;
            let keys: Vec<PkTuple> = pks.iter().map(|&k| PkTuple::from_u128(stride, k)).collect();
            let actual = schema_opt.as_deref().unwrap_or(schema);
            let eff = build_effective(committed.as_ref(), &net, actual, stride, false, &keys);
            let matched = (0..eff.len()).collect();          // no residual on this path
            Ok(ResolvedRows { schema: schema_opt, batch: Some(eff), matched })
        }
        AccessPath::Filtered { where_expr } => {
            // Fetch committed RAW: reuse the index-seek-or-scan ladder to FETCH,
            // but do NOT apply the index candidate's reduced residual and do NOT
            // short-circuit on an empty index hit — buffered rows are unindexed and
            // must see the FULL predicate, and a committed row moved into the WHERE
            // span by a buffered override is caught by the full net enumeration.
            let (schema_opt, committed) = fetch_filtered_raw(client, tid, schema, where_expr)?;
            let actual = schema_opt.as_deref().unwrap_or(schema);
            let eff = build_effective(committed.as_ref(), &net, actual, stride, true, &[]);
            let preds = bind_residuals(&[where_expr], schema)?;
            let matched = matching_indices(&preds, &eff, actual)?;
            Ok(ResolvedRows { schema: schema_opt, batch: Some(eff), matched })
        }
    }
}

/// Committed rows the buffer did NOT touch, plus buffered Present rows (all, when
/// `add_all`, else only `add_keys`). All copies under `actual` — buffered batches
/// are layout-identical to the reply (same base table; DDL is barred in a txn and
/// base-table columns are never hidden, so reply-schema ≡ catalog-schema ≡
/// buffered-batch layout).
fn build_effective(
    committed: Option<&ZSetBatch>, net: &HashMap<PkTuple, OwnedNet>,
    actual: &Schema, stride: u8, add_all: bool, add_keys: &[PkTuple],
) -> ZSetBatch {
    let mut eff = ZSetBatch::new(actual);
    if let Some(b) = committed {
        for i in 0..b.len() {
            if !net.contains_key(&b.pks.get_tuple(i, stride)) { copy_batch_row(b, i, &mut eff, actual); }
        }
    }
    if add_all {
        for op in net.values() {
            if let OwnedNet::Present(one) = op { copy_batch_row(one, 0, &mut eff, actual); }
        }
    } else {
        for k in add_keys {
            if let Some(OwnedNet::Present(one)) = net.get(k) { copy_batch_row(one, 0, &mut eff, actual); }
        }
    }
    eff
}
```

`fetch_filtered_raw` is `resolve_where_rows`'s `Filtered` arm (`mutate.rs:211-230`)
with the residual filtering stripped out — it returns the raw committed batch, and
the overlay re-applies the full predicate afterward. It drops the trailing `u64`
(the seek LSN echo) from `seek_by_index`'s three-tuple reply:

```rust
// gnitz-sql/src/dml/mutate.rs
fn fetch_filtered_raw(
    client: &mut GnitzClient, tid: u64, schema: &Schema, where_expr: &Expr,
) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), GnitzSqlError> {
    let candidates = collect_index_seek_candidates(where_expr, schema, || client.table_indexes(tid))
        .map_err(GnitzSqlError::Exec)?;
    for (col_indices, key_vals, _residual) in candidates {   // ignore the reduced residual
        match client.seek_by_index(tid, col_indices.as_slice(), &key_vals) {
            Ok((schema_opt, batch_opt, _)) => return Ok((schema_opt, batch_opt)),  // raw, no short-circuit filter
            Err(ClientError::NoIndex) => continue,
            Err(e) => return Err(GnitzSqlError::Exec(e)),
        }
    }
    let (schema_opt, batch_opt, _) = client.scan(tid)?;
    Ok((schema_opt, batch_opt))
}
```

The raw seek is a superset of the committed match set: `collect_index_seek_candidates`
only ever consumes top-level AND-conjunct equality terms (`flatten_conjuncts` keeps any
`OR` as one opaque residual leaf, `ast_util.rs:229-241`), so a chosen index candidate is
a *necessary* condition of the full predicate; a bare disjunction yields no candidate and
falls through to the full scan. The full predicate re-filters both committed and buffered
rows in `resolve_where_rows_txn`'s `Filtered` arm.

Borrow order: `net_of(client.txn_buffer()…)` (`&client`, returns owned) is done
**once, first**; then each arm's committed fetch (`&mut client`, returns owned); then
build/filter (owned, no client borrow). `net` holds no client borrow, so the `&mut`
fetches coexist with it. No overlapping borrows.

`execute_update`/`execute_delete` pick `resolve_where_rows_txn` over
`resolve_where_rows` when `client.txn_active()`, then proceed unchanged over
`resolved.batch`/`resolved.matched` — see the UPDATE/DELETE entries under *DML
behaviour* for the exact call sites and terminal-write routing.

## Module wiring & imports

The new code is all inside the private `gnitz-sql/src/dml/` module tree; no crate is
newly depended on. Exact wiring (the plan snippets assume these are present):

- **`dml/mod.rs`** (currently `mod insert; mod mutate; mod plan; mod select;` + 3
  re-exports, no `use`): add `mod overlay;` beside `mod plan;`, and — for the new
  `write_or_buffer`/`delete_or_buffer` helpers that live here — add
  `use gnitz_core::{ClientError, GnitzClient, PkColumn, Schema, WireConflictMode, ZSetBatch};`.
- **`dml/overlay.rs`** (new): the three `use` lines shown in the `net_of` block above.
- **`dml/mutate.rs`**: add `PkTuple` to the existing `use gnitz_core::{…}` line, add
  `use std::collections::HashMap;` (for `build_effective`'s `net: &HashMap<PkTuple,
  OwnedNet>` parameter), and `use crate::dml::overlay::{net_of, OwnedNet};`. (No
  `HashSet` — the single-pass `net_of` needs none.)
- **`dml/insert.rs`**: `PkTuple` is already imported; add
  `use std::collections::HashMap;` (for the helpers' `net: Option<&HashMap<…>>`
  parameter) and `use crate::dml::overlay::{net_of, OwnedNet};`. The existing
  `std::collections::HashSet::new()` uses for `seen_pks` stay fully-qualified.

## DML behaviour inside a transaction

Each write site's terminal write goes through one of two helpers in
`gnitz-sql/src/dml/mod.rs` (`&Schema` is the type all sites supply and
`push_with_mode`/`delete` take; the autocommit LSN is dropped, as every DML caller
already discards it today):

```rust
pub(crate) fn write_or_buffer(client: &mut GnitzClient, tid: u64, schema: &Schema,
                   batch: &ZSetBatch, mode: WireConflictMode) -> Result<(), ClientError> {
    match client.txn_buffer_mut() {
        Some(buf) => { buf.push_with_mode(tid, schema, batch, mode); Ok(()) }
        None => client.push_with_mode(tid, schema, batch, mode).map(|_| ()),
    }
}
pub(crate) fn delete_or_buffer(client: &mut GnitzClient, tid: u64, schema: &Schema,
                    pks: PkColumn) -> Result<(), ClientError> {
    match client.txn_buffer_mut() {
        Some(buf) => { buf.delete(tid, schema, pks); Ok(()) }
        None => client.delete(tid, schema, pks),
    }
}
```

The two ON CONFLICT helpers gain one parameter — `net: Option<&HashMap<PkTuple,
OwnedNet>>`, `None` in autocommit — so autocommit keeps its exact per-PK `client.seek`
path and the txn path consults the buffer first. `net.and_then(|n| n.get(&pk))`
collapses the two `None`s that both mean "fall back to the committed seek" (autocommit
`net == None`, **or** a PK the transaction has not touched `n.get(&pk) == None`); only a
live buffered `Present`/`Deleted` short-circuits it. Each ON CONFLICT arm of
`execute_insert` folds the net once, in a txn only, releasing the buffer borrow before
the seeks (`net_of` returns owned):

```rust
let net = if client.txn_active() { Some(net_of(client.txn_buffer().unwrap(), tid, &schema)) } else { None };
```

- **Plain INSERT** (`ConflictPlan::Error`, `insert.rs:238-254`): build the batch as
  today (SERIAL stamped, `insert.rs:187-235`), **project RETURNING before the write**,
  then `write_or_buffer(… Error)`. The reorder is mandatory: in a txn a failed
  projection must buffer nothing (non-poisoning), and it also fixes the autocommit
  path, where `RETURNING <bad>` today commits the row *then* fails in `apply_projection`
  (`insert.rs:247`, after the push at `:240`) — verified to break no existing test.
  `apply_projection` consumes the batch, so project a clone and write the original
  (`ZSetBatch` is `Clone`):
  ```rust
  ConflictPlan::Error => {
      if let Some(items) = returning {
          let (proj_schema, proj_batch) = apply_projection(items, &schema, Some(batch.clone()))?;
          write_or_buffer(client, tid, &schema, &batch, WireConflictMode::Error)?;
          return Ok(SqlResult::Rows { schema: proj_schema, batch: proj_batch });
      }
      write_or_buffer(client, tid, &schema, &batch, WireConflictMode::Error)?;
      Ok(SqlResult::RowsAffected { count: n })
  }
  ```
  Duplicate detection is at COMMIT (the per-family check above).
- **INSERT … ON CONFLICT DO NOTHING** (`client_side_filter_do_nothing`,
  `insert.rs:340-371`): add the `net` parameter; replace the existence probe
  (`insert.rs:356-361`) with:
  ```rust
  let exists = match net.and_then(|n| n.get(&pk)) {
      Some(OwnedNet::Present(_)) => true,        // buffered live ⇒ conflict, drop
      Some(OwnedNet::Deleted)    => false,       // buffered deleted ⇒ no conflict, keep
      None => {                                  // autocommit, or PK untouched by the txn
          let (_sch, found, _lsn) = client.seek(tid, &pk)?;
          matches!(found, Some(b) if !b.pks.is_empty())
      }
  };
  if exists { continue; }
  surviving_indices.push(i);
  ```
  Consulting the buffer is what stops two `DO NOTHING` inserts of one new PK from
  buffering two `+1` Error rows and tripping the commit-time per-family duplicate check.
- **INSERT … ON CONFLICT DO UPDATE** (`client_side_merge_do_update`,
  `insert.rs:383-427`): add the `net` parameter; replace the existing-row resolution
  (`insert.rs:410-424`) so the buffered row, when present, is the effective existing
  row — used as **both** `build_merged_row`'s carry source **and** `eval_do_update_rhs`'s
  evaluation base, so `DO UPDATE SET x = x + 1` reads the buffered `x`:
  ```rust
  let mut committed_existing: Option<ZSetBatch> = None;
  let existing: Option<&ZSetBatch> = match net.and_then(|n| n.get(&pk)) {
      Some(OwnedNet::Present(one)) => Some(one),   // one: &ZSetBatch already — pass as-is, not &one
      Some(OwnedNet::Deleted)      => None,        // txn deleted it ⇒ no conflict
      None => {                                    // autocommit, or PK untouched
          let (_sch, e, _lsn) = client.seek(tid, &pk)?;
          committed_existing = e.filter(|b| !b.pks.is_empty());
          committed_existing.as_ref()
      }
  };
  match existing {
      None     => copy_batch_row(batch, i, &mut out, schema),              // plain insert
      Some(ex) => build_merged_row(batch, i, ex, 0, schema, &mut out, |ci| {
          asn_by_col[ci].map(|rhs| eval_do_update_rhs(rhs, ex, batch, i, schema)).transpose()
      })?,
  }
  ```
  Run-splitting places the Update family after any earlier Error family, so an earlier
  plain insert of the same PK is validated (and applied) before the merged row
  supersedes it by order — matching statement order.
- **ON CONFLICT call sites** (`execute_insert`, `insert.rs:255-275`): compute `net`
  (above) at the top of each arm, thread `net.as_ref()` into the helper, and route the
  terminal push through `write_or_buffer`:
  ```rust
  ConflictPlan::DoNothingPk => {
      let net = if client.txn_active() { Some(net_of(client.txn_buffer().unwrap(), tid, &schema)) } else { None };
      let (filtered, surviving_count) = client_side_filter_do_nothing(client, tid, &schema, &batch, net.as_ref())?;
      if surviving_count > 0 { write_or_buffer(client, tid, &schema, &filtered, WireConflictMode::Error)?; }
      Ok(SqlResult::RowsAffected { count: surviving_count })
  }
  ConflictPlan::DoUpdatePk { assignments } => {
      let net = if client.txn_active() { Some(net_of(client.txn_buffer().unwrap(), tid, &schema)) } else { None };
      let merged = client_side_merge_do_update(client, tid, &schema, &batch, &assignments, net.as_ref())?;
      if !merged.pks.is_empty() { write_or_buffer(client, tid, &schema, &merged, WireConflictMode::Update)?; }
      Ok(SqlResult::RowsAffected { count: n })
  }
  ```
- **UPDATE** (`execute_update`, `mutate.rs:261-301`): swap the resolver at
  `mutate.rs:290` — `let resolved = if client.txn_active() {
  resolve_where_rows_txn(client, table_id, &schema, selection.as_ref())? } else {
  resolve_where_rows(client, table_id, &schema, selection.as_ref())? };` — then
  `write_set_rows` runs over `resolved.batch` exactly as today (`mutate.rs:293-299`),
  routing the terminal `push_with_mode` through `write_or_buffer(… Update)`. Reported
  count is `resolved.matched.len()`.
- **DELETE** (`execute_delete`, `mutate.rs:307`; the `client.delete` call site is
  `mutate.rs:330`): same resolver swap; collect the matched PKs from `resolved.batch`
  and route the terminal `client.delete` through `delete_or_buffer`. Deleting a
  transaction-born or buffered-Present row is a `-1` filler in an Update-mode family
  (run-splitting coalesces it with a preceding Update-mode op — a merged `DO UPDATE`,
  an earlier UPDATE — rather than always opening a new family); either way, at apply
  `enforce_unique_pk`'s store probe retracts the currently-stored row by PK regardless
  of the filler payload, so no payload-match is required.

## Statement surface (dispatch)

`dispatch::execute_statement` (`dispatch.rs:23-53`) gains three arms ahead of the
existing ones. Each unhonored-clause check is small (2–3 field tests), so inline it in
the arm using `unsupported_clause(context, clause)` (`plan/validate.rs:136`) — three
thin `reject_*` helpers are not worth it. The sqlparser-0.56 variants and the fields
to reject:

- `Statement::StartTransaction { modes, begin, transaction, modifier, statements,
  exception_statements, has_end_keyword }` → `BEGIN` / `START TRANSACTION`. Plain
  `BEGIN` parses to `begin:true` with everything else empty/`None`. Reject
  non-empty `modes` (`READ ONLY`), `modifier` (`GenericDialect` recognizes
  `BEGIN DEFERRED`/`BEGIN TRY`), non-empty `statements`, `exception_statements`
  (`transaction`, `begin`, `has_end_keyword` are inert). Then `client.txn_begin()?`
  → `SqlResult::TransactionStarted`.
- `Statement::Commit { chain, end, modifier }` → `COMMIT` (and `END`, `end:true`).
  Reject `chain`, `modifier`. Then `let lsn = client.txn_commit()?;` →
  `SqlResult::TransactionCommitted { lsn }`.
- `Statement::Rollback { chain, savepoint }` → `ROLLBACK`. Reject `chain`,
  `savepoint` (`ROLLBACK TO SAVEPOINT x`). Then `client.txn_rollback()?` →
  `SqlResult::TransactionRolledBack`.
- `Statement::Savepoint { .. }` / `Statement::ReleaseSavepoint { .. }` stay
  `_ => Unsupported`. (Confirmed: `Merge`, `Truncate`, and every other
  state-mutating statement also fall to `_ => Unsupported`, which never touches
  `client`/`txn`, so no write can escape the buffer via an uncovered statement.)

The four DDL arms — `CreateTable`, `CreateView`, `CreateIndex`, `Drop` (the only
catalog-mutating arms; there is no CREATE SCHEMA arm) — gain a guard as their **first**
statement, ahead of the existing `reject_unhonored_*` call: if `client.txn_active()`,
return `GnitzSqlError::Unsupported("DDL is not allowed inside a transaction".to_string())`.
`Statement::Query` (SELECT) is unchanged (committed basis).

`SqlPlanner::execute` (`lib.rs:56-67`) gains the opened-this-call rollback hook
(`end_catalog_snapshot` still runs unconditionally before the match, preserving
today's "snapshot always ended, even on error"):

```rust
pub fn execute(&mut self, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let txn_was_active = self.client.txn_active();
    let mut results = Vec::with_capacity(stmts.len());
    for stmt in &stmts {
        self.client.begin_catalog_snapshot();
        let r = dispatch::execute_statement(self.client, &self.schema_name, stmt);
        self.client.end_catalog_snapshot();
        match r {
            Ok(res) => results.push(res),
            Err(e) => {
                if !txn_was_active && self.client.txn_active() { let _ = self.client.txn_rollback(); }
                return Err(e);
            }
        }
    }
    Ok(results)
}
```

`execute_sql("BEGIN; INSERT …; COMMIT")` works with no further handling: the loop
runs statements against the same persistent client, so transactions also span
*multiple* `execute_sql` calls on one connection. (On a COMMIT failure `txn_commit`
already took the buffer out, so `txn_active()` is `false` in the error branch — no
double-rollback.)

## Results and errors

`SqlResult` (`lib.rs:24-31`) gains three variants:

```rust
TransactionStarted,
TransactionCommitted { lsn: u64 },     // lsn = 0 for an empty commit
TransactionRolledBack,
```

Both exhaustive `SqlResult` matches gain arms (neither uses a wildcard, so the
compiler forces them):

- Python (`gnitz-py/src/lib.rs:1679-1713`): `{"type": "TransactionStarted"}`,
  `{"type": "TransactionCommitted", "lsn": lsn}`, `{"type": "TransactionRolledBack"}`.
- C API (`gnitz-capi/src/lib.rs:1341-1348`): `TransactionCommitted { lsn } => lsn`,
  the other two `=> 0`. A raw C caller cannot disambiguate the three via `out_id`
  alone — acceptable and consistent with the existing convention (`Dropped` and
  `Rows` already both map to 0). No new C entry points; `gnitz_execute_sql_query`'s
  `if let SqlResult::Rows` at `1445` needs no change.

The transaction-control errors (`transaction already open`, `no transaction open`,
`DDL is not allowed inside a transaction`) surface through the existing
single-exception shape. The Python `PyTxn` / `with client.transaction()` binding
(`gnitz-py/src/lib.rs:1519-1816`) is a parallel surface on the same core buffer and
is untouched.

## Tests

**Rust unit (gnitz-sql):**
- Dispatch arms — state-machine errors (`BEGIN` twice; `COMMIT`/`ROLLBACK` with no
  transaction) and the complete rejected-clause matrix: `BEGIN READ ONLY` (modes),
  `BEGIN DEFERRED` (modifier), `COMMIT AND CHAIN`, `ROLLBACK AND CHAIN`,
  `ROLLBACK TO SAVEPOINT x` (savepoint). `GenericDialect` parses all of these directly
  (modes/chain/savepoint/modifier parsing is dialect-unconditional), so every case is a
  plain `Parser::parse_sql` call — no hand-built AST. Bare `SAVEPOINT`/`RELEASE
  SAVEPOINT` stay `Unsupported`. `END` behaves as `COMMIT`.
- `net_of` (Present/Deleted last-op-per-PK, including a same-PK `+1,-1,+1` sequence
  and a superseded Present) and `build_effective` (committed net-override,
  transaction-born inclusion, `add_keys` vs `add_all`, predicate re-filter after a
  payload override) on hand-built buffers.
- `write_or_buffer` / `delete_or_buffer` routing (autocommit vs buffered).
- `txn_commit` closes the transaction before the fallible send; opened-this-call
  rollback on a mid-loop error; a transaction opened by an earlier call is left open.

**E2E (pytest, `GNITZ_WORKERS=4`):**
- `BEGIN; INSERT a; INSERT b; COMMIT` — atomic across two tables; a view joining
  both reflects both after the commit's scan. `ROLLBACK` discards: tables unchanged,
  SERIAL ids gapped, server never saw a frame.
- Read-your-own-writes:
  - `UPDATE k SET x = x+1` twice compounds (+2), reporting 1 then 1.
  - `DELETE k; UPDATE k` matches 0 rows and `k` stays deleted (no resurrection);
    `DELETE k; DELETE k` reports 1 then 0.
  - `DELETE k; INSERT k (new payload); UPDATE k SET …` computes SET over the
    **reinserted** payload.
  - Transaction-born rows discovered: `INSERT new k; UPDATE k SET x=99` → `k`=99;
    `INSERT new k; DELETE k` → `k` absent; `INSERT new k; UPDATE t SET x=0` (no
    WHERE) updates `k` too.
  - WHERE re-evaluation both directions on a **non-indexed** column (buffered
    payload fails a WHERE the committed payload passes → 0 rows; delete-reinsert with
    a WHERE only the reinserted payload satisfies → matches) and on an **indexed**
    column (the buffered-row scan is unindexed → inclusion exact regardless of
    index: an indexed WHERE satisfied only by the buffered payload discovers the row;
    a committed row moved out of the span by a buffered override matches 0 rows).
- SELECT boundary: `INSERT k; SELECT … WHERE k` returns no rows; and
  `UPDATE … WHERE k` reports 1 affected while a following `SELECT … WHERE k` in the
  same transaction returns 0 rows — both resolved after COMMIT.
- Statement order through the buffer (run-splitting):
  - `INSERT new k1; DELETE k2 (committed); INSERT k2; COMMIT` — commits; `k2`
    replaced.
  - `UPDATE t …; INSERT k (committed, no prior delete); COMMIT` — COMMIT rejected
    with the duplicate-key error (the unrelated earlier UPDATE must not launder it).
  - `UPDATE t …; INSERT new k; INSERT k ON CONFLICT DO UPDATE; COMMIT` — commits;
    the merged row wins.
- ON CONFLICT: two `DO NOTHING` inserts of the same new PK → 1 row after COMMIT;
  `DO NOTHING` after `DELETE k` inserts; `DO UPDATE` conflicting with a row buffered
  earlier merges against the buffered row; `DO UPDATE` after `DELETE k` applies the
  insert as-is; `DO UPDATE` conflicting with a committed row merges against it.
- Constraint-at-COMMIT: plain INSERT of a PK committed by *another* connection after
  BEGIN but before COMMIT → COMMIT fails, both tables unmodified, transaction closed.
  FK: parent+child in one transaction (either order) commits; child referencing a
  parent deleted in the same transaction fails at COMMIT.
- Statement error mid-transaction (multi-call shape) leaves the transaction open and
  COMMIT commits the earlier statements; the single-call
  `execute_sql("BEGIN; <failing>; COMMIT")` shape rolls back and a subsequent INSERT
  autocommits normally. `INSERT … RETURNING <unsupported list>` inside a transaction
  errors and buffers nothing (and, autocommit, no longer commits the row).
- DDL inside a transaction rejected (CREATE TABLE, CREATE VIEW, CREATE INDEX, DROP)
  and leaves the transaction open. RETURNING on plain INSERT inside a transaction
  returns the stamped ids before COMMIT; after ROLLBACK those ids are absent.
- Transaction spanning multiple `execute_sql` calls, and the full `BEGIN; …; COMMIT`
  in a single call. Empty transaction: `BEGIN; COMMIT` →
  `TransactionCommitted { lsn: 0 }`, no server frame.
- **Out of scope (documented, no test asserting protection):** a concurrent commit
  that changes a row's value between an in-transaction `UPDATE … SET x = x + 1` and
  COMMIT is silently lost — this plan provides atomicity and constraint consistency,
  not write-conflict/OCC protection.
