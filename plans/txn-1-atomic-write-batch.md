# Atomic write-batch transactions: the user-table TXN frame

## Problem

A client cannot commit writes to more than one user table atomically. Each
`push` is one table, one roundtrip, one durability unit; two pushes to two
tables can be split by a crash (first ACKed and durable, second lost) and are
freely interleaved with other clients' writes. There is no client-visible way
to say "these batches land together or not at all".

The engine already has every load-bearing ingredient:

- **The recovery-side atomic unit is a zone** — one LSN shared by any number
  of SAL groups, closed by a header-only `FLAG_DDL_SYNC | FLAG_TXN_COMMIT`
  sentinel (`write_commit_sentinel`, `runtime/protocol/sal.rs:1204-1224`).
  Recovery is two-pass: `collect_committed_lsns` admits an LSN iff a group
  with `FLAG_TXN_COMMIT` exists at it (`runtime/bootstrap.rs:62-81`), and
  `recover_sal` skips every group whose LSN is not in that set
  (`bootstrap.rs:87-138`). Nothing in that walker is DDL-specific — it keys
  on the flag and the raw LSN only. The sentinel is physically last in the
  zone and both recovery passes stop at the first empty/stale-epoch prefix,
  so a torn zone is never admitted.
- **The committer already writes multi-group zones.** One debounced batch
  emits one `FLAG_PUSH` group per `(tid, mode)` run, all at a single
  `zone_lsn` reserved once (`committer.rs:564-587`), then one sentinel via
  `commit_zone` (`committer.rs:613-620`), one fdatasync, then the ACKs. So
  multi-table crash atomicity happens today *by accident of debouncing*; it
  is just not client-addressable.
- **A client-facing multi-batch frame exists for system tables.**
  `push_ddl_txn` (`gnitz-core/src/connection.rs:147-155`) bundles N
  `(tid, schema, batch)` families into one `FLAG_DDL_TXN` frame; the master
  rejects any user tid there (`sys_family_schema` returns `None` for
  `tid >= FIRST_USER_TABLE_ID`, `executor.rs:1490-1498`).
- **Deletes are already plain pushes.** `GnitzClient::delete`
  (`gnitz-core/src/client.rs:543-590`) builds a `ZSetBatch` with
  `weights = vec![-1; count]` and inert filler payloads and sends it through
  `conn.push`; the worker resolves the stored payload itself
  (`retract_pk_bytes` in `enforce_unique_pk`,
  `query/dag/mod.rs:1976-1988`). A transaction therefore needs no special
  delete representation — a delete is a family of `-1` rows.
- **Workers need nothing.** The `SalMessageKind::Push` arm
  (`worker/mod.rs:981-991`) consumes only `target_id` + batch per group;
  conflict handling is master-side. N consecutive `FLAG_PUSH` groups at one
  LSN dispatch through the existing arm unchanged, and Push is dispatched
  inline in every worker context (only Tick/DdlSync are deferred), so
  SAL order is per-worker application order.

This plan adds the client-addressable version: a **TXN frame** bundling
N user-table families, validated as a unit under the union of the involved
table locks, emitted as N `FLAG_PUSH` groups inside one zone with one
sentinel, one fdatasync, one ACK carrying the zone LSN.

## Semantics (committed)

- **Atomic + durable.** On `Ok(lsn)` the whole transaction is fdatasynced
  under one zone LSN; a crash at any point recovers all families or none.
- **Abort is pre-SAL only.** Every rejection (shape, schema, validation,
  SAL-space) happens before any byte of the transaction reaches the SAL, so
  an `Err` reply means "nothing committed" — with two **new** deliberate
  fail-stop exceptions, both `gnitz_fatal_abort`, beyond the two
  pre-existing commit-lifecycle fail-stop aborts this path inherits
  unchanged (a failed `commit_zone`, `committer.rs:617-620`, and a failed
  fsync, `committer.rs:710-713`). In all four the final outcome is decided
  by whether the sentinel reached disk — the zone recovers in full or not
  at all, never partially:
  1. **any** emission failure of a transaction family — a `sal_begin_group`
     capacity failure or a caught panic inside `write_commit_group` — **after**
     an earlier family of the same transaction was committed to the SAL. A
     committed family is already published (its `SalGroup::commit` advanced the
     write cursor and stored the size prefix, so live workers observe it) and
     the sentinel cannot un-commit it, so the only fail-safe response is to
     crash and let recovery drop the whole sentinel-less zone. Under the
     lock-held exact fit check (Committer section) this window is unreachable
     in normal operation: the check guarantees every family fits before the
     first byte is written, so a later `sal_begin_group` failure can only be
     arithmetic divergence and a panic can only be a logic bug — both fail-stop.
  2. a worker-side apply failure on a transaction family group **after**
     the sentinel. In normal operation this one is unreachable: the only
     worker-side `Err` for a `FLAG_PUSH` group is "table not registered"
     (`catalog/store_io.rs:13-20`), which the catalog read lock held
     through the ACK excludes (a concurrent DROP needs the write lock,
     `executor.rs:1549`); every other apply failure is already fail-stop
     today (`query/dag/mod.rs:1098-1106`). Defense-in-depth.
  In both cases the client observes a dropped connection — the standard
  ambiguous-commit outcome of a lost commit reply. This keeps "an `Err`
  reply implies nothing committed" true: the plain push arm's per-push
  worker-error reporting (folded into `write_err` in Phase C after the
  sentinel, `committer.rs:652-660`) never produces an `Err` reply for a
  transaction.
- **Isolated at commit.** Validation and emission happen under the union of
  the families' FK lock sets. `needs_lock` contains every table that is an
  FK parent, an FK child, has a unique index, or is `unique_pk`
  (`catalog/cache.rs:450-463`) — with the `unique_pk` shape rule below,
  every transactable table self-locks. The full union is held from before
  validation through the committer ACK; **no lock is released early**. That
  hold is load-bearing twice over: it freezes committed state under the
  validation gathers/probes, and it excludes a transaction and an
  overlapping single push from ever occupying one committer batch (both
  hold the contended table lock through their own ACK), which is what makes
  the committer's transactions-first emission order (below) unable to
  reorder same-table groups.
- **A transaction may share its zone with unrelated concurrent pushes.**
  Zone atomicity is per-LSN all-or-nothing; a superset zone strictly
  strengthens the guarantee. A single push whose group fails
  `sal_begin_group`'s bounds check is *wholly* absent from the SAL (the
  cursor never advances, `sal.rs:492-495`), so a failed unrelated push
  never contaminates the shared zone.
- **View visibility is unchanged (asynchronous).** Each family buffers into
  `pending_deltas[tid]` and ticks per source as today. A scan drains all
  pending ticks before reading (`handle_scan`, `executor.rs:1279-1310`), so
  the common read path observes the transaction's view effects together; no
  stronger view-atomicity is claimed.
- **Constraint semantics.** Uniqueness (PK and unique secondary indexes) is
  validated *cumulatively in frame order*: family N is checked against
  committed state overlaid with the simulated outcome of families
  `1..N-1`. Error-mode PK existence matches sequential pushes exactly,
  including order-sensitivity (retire-then-take of a unique value passes
  in that order and is rejected in the other; take-then-retire rejects).
  Unique-secondary release exemptions are **strictly wider** than
  sequential pushes — the release test is semantic (derived from the fold)
  rather than inherited from sequential's payload artifacts (a delete
  row's filler payload can never match the machinery's exact
  `(holder, value)` pair, and its `is_upsert` gate keys on the colliding
  row, not the releasing one, `preflight.rs:984-998`) — so a transaction
  accepts everything sequential pushes accept plus shapes whose
  post-state is single-holder-valid, and never accepts a duplicate.
  Foreign keys are validated against the *post-transaction* state
  (deferred-constraint semantics): a bundle is FK-valid iff the state
  after applying **all** families satisfies every constraint — strictly
  more permissive than sequential pushes (parent and child may arrive in
  either frame order), which is the point of having transactions.

## Wire format

New request flag, next free high bit (current top is `FLAG_DDL_TXN = 1 << 57`,
`gnitz-wire/src/flags.rs:62`; the compile-time disjointness guard at
`flags.rs:83-118` gains the new constant):

```rust
pub const FLAG_PUSH_TXN: u64 = 1 << 58;
```

Frame layout (client → master), following the `encode_ddl_txn` pattern
(`gnitz-core/src/protocol/message.rs:264-281`):

```
control block            Header { target_id: 0, flags: FLAG_PUSH_TXN, client_id, .. }
u32 family_count         LE, >= 1
per family:
  u8  conflict_mode      WireConflictMode::as_u8 (0 = Update, 1 = Error)
  [schema block]         the client's meta-schema WAL block for the family's
                         table — the same bytes encode_message embeds for a
                         schema-bearing push (built inline at message.rs:230-236;
                         this plan factors that into a reusable
                         encode_schema_block(schema, tid) helper). Self-sizing
                         via its WAL header (offset 16).
  [wal block]            encode_wal_block(schema, tid, batch); embeds its own
                         table_id (offset 8) and total size (offset 16)
```

Both per-family blocks are WAL blocks; the engine decoder walks them with
`wal_block_slice_at` (`runtime/protocol/wire.rs:1153-1162`) — no redundant
length prefixes (a length field the walker ignores is a latent divergence
bug). The schema block decodes with the existing `decode_schema_block`
(`wire.rs:971`), exactly as the push frame's does.

- The schema block is **always present** (unlike the version-negotiated plain
  push). The master validates it against the catalog per family
  (`validate_schema_match`, as the push arm does at `executor.rs:923-931` —
  it compares column count/pk/type/nullable, not names); any mismatch fails
  the whole transaction with a clean error. There is no schema-refresh retry
  inside a transaction: a concurrent DDL between buffer time and commit
  surfaces as a commit error the application re-runs.
- Frame size is bounded by the existing ingress cap
  (`MAX_FRAME_PAYLOAD_SERVER = 64 MiB`, `gnitz-wire/src/handshake.rs:10`).
  The client returns a clean local error when the encoded frame exceeds it.
- Engine decoder: `decode_push_txn(data) -> Result<Vec<TxnFamilyWire>, &str>`
  in `runtime/protocol/wire.rs`, mirroring `decode_ddl_txn`
  (`wire.rs:1164-1193`) with the mode byte and the schema block per family;
  `TxnFamilyWire { tid: i64, mode: u8, schema_block: &[u8], wal_block: &[u8] }`
  (borrowed slices, same lifetime discipline as `decode_ddl_txn`'s result).
  Truncation at any field rejects the whole frame.

Reply: the standard single-frame ACK, `seek_pk = zone LSN`. No target-id
correlation is needed on the client: `push_ddl_txn` already receives the
next frame uncorrelated (`connection.rs:153`); the txn path copies that.

**Bundle shape rules.** The frame-local rules run at decode time (handler
step 1, no catalog access); the catalog-dependent rules run under the
catalog read lock (handler step 2):

- *Frame-local:* `family_count >= 1`; every family `batch.count >= 1`;
  every tid `>= FIRST_USER_TABLE_ID` (system families stay exclusive to
  `FLAG_DDL_TXN`). Families may repeat a `(tid, mode)` pair freely: for
  each tid, the frame positions of its families define its op order
  (validation fold and worker application alike), so a client expressing
  "insert, delete, insert" on one table emits three families in that
  order. Cross-tid interleaving is unconstrained (FK validation is
  post-transaction, order-free).
- *Catalog-dependent:* every tid exists (`has_id`); every family's schema
  matches (`validate_schema_match`); every family's table is registered
  `unique_pk` (below).
- **Every family's table must be registered `unique_pk`.** The validation
  model below simulates the `enforce_unique_pk` apply path; a
  non-`unique_pk` table (creatable only through the raw client API's
  `create_table(..., unique_pk: false, ..)`, `gnitz-core/src/client.rs:700-755`)
  ingests raw Z-set addition with no LWW and no stored-payload delete
  resolution (`query/dag/mod.rs:1050-1067`), which the simulation does not
  model — and for which a `-1` filler-payload row does not even remove
  data. Such families are rejected: `TXN: table {tid} is not unique_pk`
  (`table_has_unique_pk` reads the master-resident catalog,
  `catalog/metadata.rs:121-123`; `unique_pk` is set only at CREATE, no
  ALTER path toggles it, and this check incidentally rejects view tids,
  which register `unique_pk = false`). Every SQL-created table is
  `unique_pk`, so the SQL surface is unaffected.

## Client API

New core buffer type in `gnitz-core/src/client.rs` (owned, no borrow of the
client — the same object backs the Python binding):

```rust
pub struct TxnBuffer {
    // (tid, schema, batch, mode) in creation order; per tid, maximal
    // same-mode runs of the caller's op sequence (run-splitting)
    families: Vec<(u64, Arc<Schema>, ZSetBatch, WireConflictMode)>,
    last_family_of: FxHashMap<u64, usize>,
}
impl TxnBuffer {
    pub fn push(&mut self, tid: u64, schema: &Schema, batch: &ZSetBatch);          // mode Update
    pub fn push_with_mode(&mut self, ..., mode: WireConflictMode);
    pub fn delete(&mut self, tid: u64, schema: &Schema, pks: PkColumn);            // -1 rows, filler payload
    /// Net buffered state of one PK: fold this tid's families in order.
    pub fn net_state(&self, tid: u64, pk_bytes: &[u8]) -> Option<BufferedNet>;
}
pub enum BufferedNet { Present(ZSetBatch /* one row, copied out of the family batch — null word and blob bytes included */), Deleted }
```

- `push`/`delete` append to the tid's **last** family when its mode matches,
  else open a new family (run-splitting). **Per-table op order is therefore
  preserved end to end**: buffer call order = family frame order = the
  engine's validation-fold and worker-application order. An op sequence
  `delete(k)`, `push_error(k)` on one table emits an Update family `[D(k)]`
  followed by an Error family `[I(k)]` — the replace idiom, in order.
  Appends within a run use `ZSetBatch::extend_from`
  (`gnitz-core/src/protocol/types.rs:704`); no client-side consolidation
  (the engine's apply is order-defined last-op-wins per PK, which the
  validation section mirrors exactly).
- `net_state` is the client-side mirror of the engine's fold: walk the
  tid's families in order, last op per PK wins (`w > 0` ⇒
  `Present(that row, owned)`, `w < 0` ⇒ `Deleted`, `w == 0` rows skipped
  exactly as in the engine fold), `None` when the PK is untouched.
  Returns owned data so callers can consult it between wire calls without
  holding a buffer borrow.
- `delete` builds the `-1` batch exactly as `GnitzClient::delete` does today
  (`client.rs:543-590`); that construction moves into a shared helper both
  call. Schemas are taken as `&Schema` (the type every existing client
  write path uses, `client.rs:278-284`) and stored as `Arc` internally.

Commit path (`begin`/`commit_txn` on `GnitzClient` in
`gnitz-core/src/client.rs`; `push_txn` on `Connection` in
`gnitz-core/src/connection.rs`, beside `push_ddl_txn`):

```rust
impl GnitzClient {
    pub fn begin(&mut self) -> TxnBuffer;                                    // trivial constructor
    pub fn commit_txn(&mut self, buf: TxnBuffer) -> Result<u64, ClientError> // -> zone LSN
}
impl Connection {
    pub fn push_txn(&mut self, families: &[...]) -> Result<u64, ClientError>
}
```

- `commit_txn` on an **empty** buffer is `Ok(0)` with no wire roundtrip.
- Dropping a `TxnBuffer` without committing is rollback: nothing was ever
  sent.
- Each family's batch is `batch.validate(schema)`-checked client-side before
  encoding, as `push_ddl_txn` does (`connection.rs:148-150`).

Python (`gnitz-py`): bind `TxnBuffer` and `commit_txn` (owned object — no
borrow lifetime issue; the context manager lives in `_client.py`):

```python
with client.transaction() as txn:
    txn.push(orders_tid, schema, batch)
    txn.delete(carts_tid, cart_schema, pks)
# __exit__ commits on clean exit, discards on exception
```

## Master handler

`handle_push_txn` in `runtime/orchestration/executor.rs`, dispatched on
`flags & FLAG_PUSH_TXN != 0` in the same pre-alloc branch position as the
`FLAG_DDL_TXN` route (`executor.rs:814-817` — before the `target_id == 0`
alloc block, so it cannot collide with alloc RPCs or empty-batch scans).
Steps, mirroring the push arm (`executor.rs:933-1006`):

1. Decode the frame; enforce the **frame-local** shape rules.
2. Take the catalog **read** lock; enforce the **catalog-dependent** shape
   rules (`has_id`, `unique_pk`, `validate_schema_match` — schemas via
   `shared.get_schema_desc`, as `executor.rs:925`); decode each family's
   batch via `Batch::decode_from_wal_block` with the catalog schema and
   `b.downgrade()` (trust boundary, as `handle_ddl_txn` does at
   `executor.rs:1499-1512`).
3. Acquire the lock union: `⋃ fk_lock_set(tid)` over all families, sorted
   ascending, deduped, acquired in order (`fk_lock_set`,
   `catalog/metadata.rs:229-247`; acquisition pattern `executor.rs:944-951`).
   One global ascending-tid order with the full set acquired up front at a
   single acquisition site cannot deadlock against the push arm's subset.
   Locks are held through the committer ACK, exactly like the push arm
   (`_tlocks` scope, `executor.rs:948-1005`).
4. Local + distributed validation (next section).
5. Check `shared.draining.get()` **immediately before** the committer send —
   the same late placement as the push arm (`executor.rs:977-983`), after
   the potentially long validation awaits, so a drain that begins during
   validation still rejects the transaction.
6. Send `CommitRequest::Txn { families, done }`; await; reply `Ok(zone_lsn)`
   or the error string.

## Validation: the bundle simulation

The worker apply path for `unique_pk` tables is **non-linear**:
`enforce_unique_pk` (`query/dag/mod.rs:1948-2016`) normalizes weights to ±1
(`normalize_unique_pk_weights`, `dag/mod.rs:1925-1934`), makes a later `+1`
row retract an earlier same-PK row (LWW, `dag/mod.rs:1993-1998`), makes a
`-1` row retract the stored row at most once (`store_retracted`,
`dag/mod.rs:1976-1988` — and `retract_pk_bytes` is net-weight-aware, so
this composes correctly across families), and drops a delete of an absent
key as a no-op (`dag/mod.rs:2003-2007`). Net-weight arithmetic therefore
does **not** predict row survival; validation simulates the apply.

For each tid touched by the bundle the master builds, in frame order across
that tid's families and row order within each family, the per-PK last-op
map (keys are the row's OPK bytes, `batch.get_pk_bytes`):

```
sim[tid] : PkBuf -> LastOp        LastOp = Inserted { family, row } | Deleted
    w > 0 row  =>  Inserted            (any positive weight = one insert, per
    w < 0 row  =>  Deleted              weight normalization)
    w == 0 row =>  skipped              (enforce_unique_pk skips them too,
                                         dag/mod.rs:1972-1975)
```

The same fold evaluated over a **prefix** of families (`1..N-1`) yields the
overlay Rule U needs; evaluated over the whole bundle it yields the
post-transaction state Rules F1/F2 need. One pass computes both: walk
families in frame order, validating family N against the running overlay
*before* folding family N into it. **Prefix scoping is total:** `retired_N`
and `added_N` are the definitions below evaluated on the prefix fold only —
touched-PK set, survival, and key images all restricted to families
`1..N-1` (an `added` entry whose PK a later prefix family Deletes is gone
from `added_N`; a PK touched only by family N contributes nothing to
either prefix set).

Derived quantities:

- **surviving(tid)** = the `Inserted` entries of the whole-bundle fold — PK
  plus that row's payload.
- **exists_after(tid, pk)** = fold touched pk ? last op is `Inserted`
  : committed(pk).
- **old values**: for any rule that needs the *committed* value of a column
  of a touched PK (deletes carry filler payloads; upserts retire the old
  row's values implicitly), the master resolves them with
  `execute_gather_async(tid, touched_pks, cols)` per table — the identical
  mechanism the RESTRICT path already uses (`preflight.rs:467-477`); it
  omits absent PKs and yields `None` for NULL columns
  (`dispatch.rs:1430-1437`). The projected column set is the union of that
  table's FK-referenced columns and its unique-secondary-index columns;
  PK-embedded columns are extracted from the OPK bytes instead
  (`preflight.rs:501-508`). `execute_gather_async` packs the projection mask
  into the gather group's `seek_col_idx` control field via `pack_gather_cols`,
  which encodes **at most 8** columns (`sal.rs:165-175`) and returns `None`
  beyond that. A transactable table can legitimately exceed 8 projected columns
  (e.g. three FKs plus two composite unique indexes), so the transaction path
  **chunks** the projection into slices of ≤ 8 columns, issues one
  `execute_gather_async` per chunk (all chunks over the identical
  `touched_pks` set, so a PK either exists committed in every chunk or is
  omitted from every chunk), and merges the resulting `GatherMap`s into one
  keyed by PK: the unified map has `stride = project.len()`, and for each PK in
  the (identical across chunks) index the per-chunk value slices are
  concatenated in chunk order. The single-chunk case (≤ 8 columns) is the
  degenerate one-gather path. This lifts the 8-column ceiling that would
  otherwise make a wide-schema table permanently untransactable with no
  client-side workaround.
- **retired(tid, idx)** / **added(tid, idx)**: per index key (FK-referenced
  column or unique-index key), the values removed / introduced by the fold:
  `retired` = old committed key images of touched PKs whose surviving state
  is absent or carries a different key; `added` = surviving rows' key
  images, each with its holder PK. **NULL rules follow the index layer:** a
  NULL (old or new) value is skipped — NULLs are NULL-distinct and
  unindexed (`preflight.rs:810-813`) — and for composite unique indexes the
  "value" is the promoted composite key image (`IndexKeySpec`,
  `catalog/validation.rs:243-287`), not a single column scalar. Prefix
  variants (`retired_N`/`added_N`) come from the running overlay.

Committed state is stable across all gathers and probes because they run
under the acquired table locks, and probes/gathers ride the SAL, which
workers consume strictly sequentially — every previously ACKed push
(memtable included) is visible to them.

All keys derived from bundle rows for comparison against probe results are
encoded through the same `build_check_batch` promotion as the probe keys
themselves (`preflight.rs:426-428` — the child column's type drives
sign-extension into the promoted index image), so membership tests compare
identical byte images.

Master-side cost is bounded by the frame cap: the fold and gather maps are
O(bundle rows × projected columns) (the gather sink has no reply cap,
`dispatch.rs:1521-1544`, but its key set is the bundle's touched PKs, ≤ 64
MiB of frame). The one check with unbounded *committed-side* fan-in is Rule
F2's exemption fetch, which is explicitly capped below.

### Rule U — uniqueness, cumulative in frame order

Family N is validated against committed state ⊕ the running overlay of
families `1..N-1` (prefix-scoped `retired_N`/`added_N` — the sets carry
frame position; whole-bundle sets would be order-blind and would wrongly
accept take-then-retire):

- **In-family checks unchanged**: intra-batch duplicate-PK rejection for
  Error mode (`preflight.rs:334-341` — probe keys are net-positive PKs, so
  an in-family delete-then-insert of a committed key already passes today),
  `validate_unique_indices`'s in-batch `seen`/retraction logic
  (`catalog/validation.rs:176-268`).
- **Error-mode PK existence**: a PK "exists" for family N iff
  `(committed(pk) ∧ pk not Deleted in overlay) ∨ pk Inserted in overlay`.
  Reject Error-mode rows whose PK exists. This accepts the committed-key
  replace idiom split across families (Update family deletes `k`, Error
  family re-inserts `k` — exactly what `TxnBuffer` produces for
  "delete k; insert k", since deletes buffer into the Update family) and
  rejects `(Update-insert k, Error-insert k)` frame order, both matching
  sequential pushes.
- **Unique secondary indexes**: the worker apply path enforces **nothing**
  for unique secondary indexes (`ingest_store_and_indices` projects and
  ingests index batches unconditionally, `dag/mod.rs:1091-1112`), so master
  preflight is the only enforcement and must see cross-family conflicts.
  Per unique index, family N's row with key image `v` conflicts iff the
  overlay-adjusted holder of `v` exists, is a different PK, **and family N
  does not itself release that holder**:
  - holder existence: `v ∈ (committed_found ∖ retired_N) ∪ added_N`, holder
    ≠ the row's PK. `committed_found` comes from the existing Phase-2
    distributed probes (presence-only — sufficient, because a committed
    holder that is a *touched* PK is identified through the gather, and an
    *untouched* committed holder is necessarily a different PK ⇒ conflict;
    `seek_unique_holder`, `dispatch.rs:1165-1207`, remains the per-value
    fallback).
  - release exemption (the **semantic release rule**, one committed
    definition): family N releases holder `h` of value `v` iff family N's
    own rows' last op on `h` is a `Deleted`, or an `Inserted` whose key
    image `≠ v` (a NULL key image counts as `≠ v`, coherent with the
    index layer's NULL-distinct rule). The rule is evaluated from the
    fold, never from row payloads, and applies to **both** committed and
    overlay-Inserted holders, *in addition to* the unchanged in-family
    machinery (whose own exemptions, `preflight.rs:956-1004`, stay as
    they are — the layers compose disjunctively, so the machinery's
    narrower committed-holder gates can only be widened, never
    tightened). Per the Semantics section this is deliberately **wider
    than sequential pushes**: sequential's exact-`(holder, value)`-pair
    and `is_upsert` gates reject shapes like `[D(h), I(k,v)]` against a
    committed `h(v)` (the delete row's filler payload can never form the
    pair) and `[I(h,w), I(k,v)]` (fresh-PK colliding row), although both
    post-states are single-holder-valid; the semantic rule accepts them.
    It also covers the overlay case that motivated it: a value shift
    across transaction-created rows — family 1 inserts `(h,5),(k,6)`,
    family 2 upserts `(k,5),(h,7)` — is accepted.
  Order-sensitivity is preserved: retire-then-take passes,
  take-then-retire rejects, and no shape with two live post-state holders
  is accepted (in-family `seen` bars same-family duplicates; the prefix
  holder tracking bars cross-family ones; a released holder's row
  genuinely retracts at apply, since family groups apply in frame order).

### Rule F1 — FK existence, post-transaction

For every FK constraint on a bundled child table: for each FK value `v`
carried by the child bundle's **surviving** rows (rows deleted or
LWW-overwritten by frame order need no parent; NULL values are skipped as
today, `preflight.rs:380-382`):

```
exists_after_parent(v) = (v ∈ committed_found ∧ v ∉ retired(parent, ref_col))
                        ∨ v ∈ added(parent, ref_col)
```

`committed_found` is the existing Phase-1 parent probe's found-set —
`p1_results[idx]` *is* the found key set for both the PK fast path and the
unique-index broadcast (the worker echoes exactly the matched probe keys).
The plain-push path is the degenerate case (`retired = added = ∅`), where
per-key membership reduces to the current `found < expected_count`
comparison (`preflight.rs:578-592`); both paths share one implementation.

This accepts parent+child inserts in either frame order, rejects a child
insert referencing a parent that the bundle deletes or upserts away
(upsert-retirement is in `retired` via the gather), and accepts
delete-then-reinsert of the parent key (surviving row carries the value ⇒
`added`).

### Rule F2 — FK RESTRICT, post-transaction

For every bundled table that is an FK parent, the values needing a RESTRICT
check are those the transaction removes *and does not re-add*:

```
V_check(parent, ref_col) = { v ∈ retired(parent, ref_col) | ¬exists_after_parent(v) }
```

(The `∖ added` term is required for coherence with the deferred-FK
definition: a bundle that re-keys a parent — delete PK `a` holding `v`,
insert PK `b` with `v` — leaves `v` existing post-transaction, so committed
children referencing `v` are fine, exactly as Rule F1 would judge a new
child of `v` in the same bundle.)

Evaluation is presence-probe-first, mirroring today's batched shape
(`preflight.rs:535-543` — one `build_check_batch` per child index carrying
all values):

1. One batched presence probe per (child table, fk_col) over all of
   `V_check`. Values with no committed hit are done (no children).
2. A hit value `v` where the bundle has **no** family on that child table
   is a violation immediately — untouched committed children exist, and
   only a bundled child family could exempt them.
3. Every hit value `v` where the bundle **does** have a child family requires
   the exemption fetch: `fan_out_seek_by_index_collect_async(child_tid,
   fk_col, v)` (`dispatch.rs:1010-1031`) — the broadcast-and-merge-all-rows
   primitive; NOT `fan_out_seek_by_index_async` (`dispatch.rs:902-1000`),
   which is a unique-index point-seek that forwards a single worker's slot
   and hard-errors on chunked replies, silently missing children on other
   workers for a non-unique FK index. **These fetches run concurrently, not
   sequentially**: the loop pushes one `fan_out_seek_by_index_collect_async`
   future per `v` into a `Vec` and drives them with
   `crate::runtime::reactor::join_all_unpin`, exactly as the existing unique
   pre-flight exemption path does (`preflight.rs:957-979`). Sequential `await`s
   would stall the reactor for one round-trip per value while the full lock
   union is held — up to `TXN_RESTRICT_FETCH_LIMIT` (below) round-trips
   serialized, blocking every other writer to these tables cluster-wide; each
   fetch's own SAL emission still serializes briefly on `sal_writer_excl`, but
   the reply waits overlap. Violation iff any returned child
   row still references `v` after the child-table fold: PK untouched →
   violation; surviving state absent → exempt; surviving FK value `= v` →
   violation, `≠ v` (a surviving NULL counts as `≠ v`, coherent with F1's
   NULL skip) → exempt (Update-mode re-pointing of an existing child
   row exempts it — the worker's LWW retracts the old referencing row
   implicitly, so requiring an explicit `-1` row would falsely reject).
4. **Fetch budget (committed constant):** at most
   `TXN_RESTRICT_FETCH_LIMIT = 1024` distinct values may reach step 3 per
   transaction; beyond it the transaction is rejected with
   `TXN: RESTRICT exemption fetch limit exceeded; split the transaction`.
   Each fetch is additionally bounded by the collect primitive's own reply
   cap (`MAX_FRAME_PAYLOAD_CLIENT = 256 MiB`, `dispatch.rs:1126-1140`);
   the transaction path wraps that error as
   `TXN: RESTRICT exemption fetch for a hot FK value exceeds the reply
   cap; split the transaction` (the primitive's own "add a tighter
   predicate or LIMIT" text is scan advice, meaningless on a commit).
   This is an honest scale bound on "delete parent + all its children in
   one transaction": it works up to ~256 MiB of committed child rows per
   hot FK value, and fails cleanly pre-SAL beyond that. Both bounds keep
   a delete-heavy bundle from turning validation into an unbounded
   master-side materialization under held locks.

New child rows referencing a removed `v` need no check here: Rule F1
already fails them.

### Failure

Failure of any check fails the **whole transaction** before any SAL byte is
written; nothing needs compensation (user-table data is applied by workers
only after SAL emission). The transaction entry point is a new
`validate_txn_distributed_async(disp_ptr, reactor, sal_excl, &families)`;
`validate_all_distributed_async` (`preflight.rs:277-284`) keeps its
signature for the plain push path, both built on the shared per-key
implementation.

## Committer changes

New request variant, one `done` for the whole bundle:

```rust
pub enum CommitRequest {
    Push { tid, batch, mode, done },                       // unchanged
    Txn(PendingTxn),
    Barrier { kind, done },                                // unchanged
}
pub struct PendingTxn {
    pub families: Vec<TxnFamily>,                          // frame order
    pub done: oneshot::Sender<Result<u64, String>>,
}
pub struct TxnFamily { pub tid: i64, pub mode: WireConflictMode, pub batch: Batch }
```

- **The batch carrier grows a third arm.** `PendingBatch` becomes
  `(Vec<PendingPush>, Vec<PendingTxn>, Vec<barriers>)`; **all three**
  exhaustive `CommitRequest` match sites route the new variant:
  `start_batch` (`committer.rs:209-219`) seeds it, `debounce_drain`
  (`committer.rs:238-259`) drains it, and `await_servicing`
  (`committer.rs:419-438`) holds it across a checkpoint sequence exactly
  like a held push. The arity change ripples beyond the match sites:
  `run()` destructures the carrier at three sites and
  `run_checkpoint_sequence` threads held pushes through the sequence
  (`committer.rs:134-196, 322-329`) — both carry the txn vector
  alongside — and `run()`'s commit
  gate `if !pushes.is_empty()` (`committer.rs:188-190`) becomes
  `if !pushes.is_empty() || !txns.is_empty()`, else a transaction-only
  batch never reaches `commit_pushes` and its clients hang. The
  `MAX_PENDING_ROWS` row counter includes txn family counts at **all
  three** counting sites: `start_batch` when the first request is a `Txn`,
  `debounce_drain`'s initial sum (`committer.rs:233-236`), and the
  in-loop `row_count +=` for requests received during the drain
  (`committer.rs:240`). A `Txn` is one indivisible entry, never split.
  `commit_pushes` takes the txn vector alongside `pushes`.
- **Group formation** (`commit_pushes` Phase A, `committer.rs:448-552`):
  single pushes keep the sort-and-merge exactly as is. Each transaction's
  families are **not** merged and **not** sorted: each family becomes its
  own `GroupInfo { txn: Some(txn_idx), .. }` in frame order. After group
  formation and before Phase B, a **reconciliation pass** enforces
  all-or-none marking per transaction: if any of a transaction's groups has
  `write_err` (Phase-A panic path — note the existing single-push pattern
  marks one group and `continue`s, `committer.rs:520-537`, which is exactly
  wrong for a bundle), every group of that transaction gets `write_err`, so
  Phase B emits none of them.
- **Emission order: transactions first.** Phase B emits all transaction
  groups (in arrival order, families in frame order), then the merged
  single-push groups, then the one sentinel. Transactions get the zone's
  fresh space; singles behind them keep today's graceful per-push
  degradation (`g.write_err`, `committer.rs:581-586`). This reorders only
  disjoint-table groups (see Semantics: a transaction and a single push on
  an overlapping table serialize on the table lock and cannot share a
  batch); XOR8 filter ingest is per-tid (`committer.rs:727-744`) and
  `tick_tids` is per-source — both order-insensitive across distinct tids.
- **Fit check, under the lock (load-bearing for atomicity).** Today a group
  that fails `sal_begin_group`'s bounds check (`write_cursor + total >
  mmap_size`, `sal.rs:492-495`) fails individually while sibling groups
  succeed and the sentinel still covers them — for a transaction that would
  be a partial commit both live and at recovery. Two-level defense:
  - **Advisory, in `run()`**: the existing checkpoint decision
    (`committer.rs:171-186`) additionally fires when the batch contains a
    transaction whose **sound upper bound** `U` exceeds
    `sal_remaining_bytes()` (new accessor; today only boolean predicates
    exist, `dispatch.rs:569,1795`) **and** `U ≤ mmap_size`. Per family,
    `U = 8 + GROUP_HEADER_SIZE + W × align8(ctrl + schema_block +
    data_wire_block_size_cached(count_total, npc, stride))` — every worker slot bounded by the
    whole family's data (a slot's rows are a subset and the size helper is
    monotone in row count; for a non-`wire_safe` family the family's own
    wal-block byte length bounds any slot's data) — plus the sentinel.
    `U` dominates the exact footprint **by construction**, which is what
    makes the gate sound in both directions: `U ≤ remaining` implies the
    transaction fits now (no advisory needed, no stuck-transient window),
    and `U ≤ mmap_size` implies a checkpoint makes it fit (the advisory
    checkpoint is guaranteed useful — fires at most once per such
    transaction, no storm). When `U > mmap_size` the advisory stays
    silent and the authoritative check decides. `U` is constant-time — no
    partitioning, no sub-batch builds. (The lock-free
    `sal_remaining_bytes()` read is benign: all SAL writers are
    cooperative tasks on the single reactor thread.)
  - **Authoritative, in Phase B under `sal_writer_excl`**: immediately
    before a transaction's *first* group, compute its exact footprint
    (`txn_groups_sal_size`, below) — all its family groups + the sentinel —
    against the actual remaining space at the current cursor.
    `footprint > mmap_size` → fail with the **terminal** error
    `transaction exceeds SAL capacity` (retrying can never succeed).
    Otherwise insufficient at the current cursor → fail with the
    **transient** error `transaction exceeds SAL space` **and set the
    committer's one-shot `force_checkpoint` flag**, which the next `run()`
    iteration honors exactly like `sal_needs_checkpoint()` — so a client
    retry is guaranteed progress even when `U > mmap_size` kept the
    advisory silent (transient failure writes zero bytes and would
    otherwise never move the cursor toward a checkpoint on an idle
    server). Either way the transaction fails cleanly with zero of its
    bytes written and the batch continues. This cannot race: the lock is
    held from the check through the transaction's last group. No
    checkpoint runs here (`flush_round` re-acquires `sal_writer_excl`,
    `committer.rs:289` — re-entry would self-deadlock).
  - **Per-family emission is fail-stop once the transaction has committed a
    family.** Each family group is emitted under `guard_panic("commit_write")`
    exactly as the push arm is, so both a `sal_begin_group` capacity failure
    (returned `Err`) and a panic inside `write_commit_group` (e.g. a fault in
    `scatter_wire_group`, an OOM in the non-`wire_safe` sub-batch build) surface
    as one `Err`. A transaction tracks whether any of its families has committed
    (`SalGroup::commit` has run, advancing the cursor and publishing the group):
    - a failure on a family **before** the transaction has committed any family
      writes zero of its bytes (a failed `sal_begin_group` never advances the
      cursor; a panic between `sal_begin_group` and `commit` leaves the group
      uncommitted and its space reclaimed by the next writer), so the whole
      transaction fails cleanly — mark every one of its groups `write_err` and
      emit none of them, exactly like the reconciliation pass;
    - a failure on a family **after** the transaction has committed an earlier
      family is a partial commit that cannot be undone — `gnitz_fatal_abort`.
    Under the authoritative fit check every family is guaranteed to fit, so this
    second branch is unreachable in normal operation (a later `sal_begin_group`
    failure is arithmetic divergence, a panic is a logic bug); it exists so a
    "can't happen" fault fails the whole node rather than silently tearing a
    committed zone. This makes the byte-exactness of `txn_groups_sal_size`
    load-bearing — an under-count would let a later family's `sal_begin_group`
    fail *after* a sibling committed, tripping the abort — so its equality test
    below is mandatory, string/blob and empty-slot cases included. The
    single-push arm's per-group graceful `write_err`+`continue` degradation is
    retained **only** for single pushes; a transaction group never takes it.
- **Footprint arithmetic** (`txn_groups_sal_size`, new, side-effect-free;
  a `MasterDispatcher` method in `master/dispatch.rs` — it needs
  `cached_schema_block` and the routing helpers that already live there,
  which is why the `sal`-private size helper below gets a visibility bump):
  per family, per-worker slot sizes via the same partitioning
  (`with_worker_indices` / `with_broadcast_indices` — pure, TLS-scratch
  based, re-callable; the sizing closure skips `record_index_routing`'s
  side effect, `dispatch.rs:1692`), then:
  - `wire_safe` families: `ctrl + schema_block +
    data_wire_block_size_cached(count_w, npc, stride)` for `count_w > 0`,
    `ctrl + schema_block` for an **empty** slot (`data_sz = 0`,
    `sal.rs:1033-1039` — the closed form must not be applied at
    `count_w = 0`);
  - non-`wire_safe` (string/blob) families: sizes depend on per-worker blob
    heap totals, so the pre-check **materializes the per-worker sub-batches
    once**, sizes them via `wire_size` (`sal.rs:869-877`), and drops them;
    emission rebuilds them (`sal.rs:976-1009`). The double partition cost is
    accepted and applies to transactions only. This must not be shortcut into
    an allocation-free "sum each row's German-string overflow length" pass: a
    sub-batch's blob heap is built by `relocate_german_string_vec` under an
    active `BlobCache` (`schema.rs:1230-1265`, keyed by
    `(src_blob ptr, offset, length)`), which copies each distinct long-string
    span **at most once per worker slot**. Two rows in one slot referencing the
    identical source span contribute the span's bytes once, not twice, so a
    naive per-row sum over-counts and breaks the byte-exactness the fatal-abort
    path depends on. Measuring the actually-built sub-batch is the only exact
    computation.
  - group total `8 + GROUP_HEADER_SIZE + Σ align8(size)` over non-zero
    slots (`sal.rs:465-495`); sentinel `8 + GROUP_HEADER_SIZE`
    (`sal.rs:1204-1224`). `data_wire_block_size_cached` (`sal.rs:403`,
    currently module-private) becomes `pub(crate)`.
- **Emission**: per family, the existing
  `write_commit_group(tid, zone_lsn, batch, mode, req_ids)`
  (`dispatch.rs:1675-1718`) — same shared `zone_lsn`; then the batch's
  single `commit_zone` + fsync, unchanged.
- **ACK resolution.** The early-error return (`committer.rs:589-598` — it
  currently drains only `pushes` and returns; a missed txn sender is a hung
  client) and the success loop (`committer.rs:757-765`) both gain a second
  pass: resolve each `PendingTxn.done` once — `Ok(zone_lsn)` iff **all** its
  family groups have `write_err == None`, else the first family error.
  Every exit path of `commit_pushes` resolves every txn `done`; the
  shutdown path is covered because held txns flow through the trailing
  `commit_pushes`, and a genuinely dropped sender surfaces to the executor
  as `rx.await → Err` → "committer shut down" (`executor.rs:1001-1003`),
  not a hang.
  Both resolution loops drain `pushes` by each group's `[start, end)`
  range (`pushes.drain(..g.end - g.start)`, `committer.rs:593,758`); a
  transaction's `GroupInfo`s have an **empty** pushes-range
  (`start == end`), so the existing drains skip them and the txn pass
  resolves the `PendingTxn.done` senders separately.
  A **worker ACK error** on a transaction family group in Phase C
  (`first_worker_error_opt`, `committer.rs:652-660`) is post-sentinel:
  `gnitz_fatal_abort` per the Semantics section, not an `Err` reply.
- **Tick accounting** (`committer.rs:682-694`): per family, identical to a
  push group. `fut_slots`/`ack_slots` sizing already handles variable group
  counts (`committer.rs:636-660`).

## What explicitly does not change

- **Workers**: zero changes. Family groups are ordinary `FLAG_PUSH` groups.
- **Recovery**: zero changes. The two-pass committed-LSN walk already treats
  any multi-group zone atomically; the per-table flushed-LSN watermark
  (`bootstrap.rs:116-122`) splitting a recovered zone (some families already
  flushed, others replayed) is weight-correct. The crash seam
  `GNITZ_INJECT_PUSH_ABORT=after_groups` (`committer.rs:600-611`) exercises
  the pre-sentinel crash window unmodified. Its doc comment disclaims
  post-restart invisibility assertions; that disclaimer is stale — between
  checkpoints, spilled shards are manifest-less and unsynced
  (`storage/lsm/table/flush.rs:242-282,376-383`) and `Table::new(SalReplay)`
  loads only the manifest — so this plan updates the comment to state the
  invisibility guarantee it actually provides.
- **`FLAG_DDL_TXN`**: untouched; system families remain exclusive to it,
  user families excluded from it.
- **The plain push path**: `validate_all_distributed_async` semantics,
  per-push ACK behavior, and worker-error reporting stay exactly as they
  are.
- **SERIAL allocation, secondary-index maintenance, exchange/backfill,
  checkpointing**: untouched; families flow through the same ingest path as
  pushes.

## Error cases

All rows below are pre-SAL, whole-transaction failures (nothing committed);
the two new fail-stop cases from the Semantics section are listed last (the
inherited `commit_zone`/fsync fail-stop aborts are unchanged and not
transaction-specific).

| Case | Outcome |
|------|---------|
| empty bundle / empty family batch | `TXN: empty family bundle` / `TXN: empty batch for table {tid}` |
| non-user tid / unknown tid | `TXN: {tid} is not a user table` / `table {tid} not found` |
| non-`unique_pk` table (views included) | `TXN: table {tid} is not unique_pk` |
| schema mismatch | existing `validate_schema_match` error text |
| uniqueness (Rule U) / FK (Rules F1, F2) violation | the existing per-check error texts |
| RESTRICT fetch budget exceeded | `TXN: RESTRICT exemption fetch limit exceeded; split the transaction` |
| RESTRICT fetch reply cap exceeded | `TXN: RESTRICT exemption fetch for a hot FK value exceeds the reply cap; split the transaction` |
| Phase-A panic on any family | the panic message, whole transaction failed (reconciliation pass) |
| SAL space insufficient at emission (transient) | `transaction exceeds SAL space` |
| footprint > SAL capacity (terminal) | `transaction exceeds SAL capacity` |
| frame > 64 MiB | client-side `ClientError` before send |
| server draining | `server shutting down` |
| emission failure (`sal_begin_group` capacity or caught panic) on a family before the transaction committed any family | whole transaction fails cleanly, zero bytes written (marked like the reconciliation pass) |
| emission failure (`sal_begin_group` capacity or caught panic) on a family after the transaction committed an earlier family | `gnitz_fatal_abort` (unreachable under the fit check); outcome decided by the sentinel on disk |
| worker apply error after the sentinel | `gnitz_fatal_abort`; outcome decided by the sentinel on disk |

## Tests

**Rust unit (gnitz-core):** frame encode/decode roundtrip (multi-family,
both modes, delete families); truncated-frame decode errors; `TxnBuffer`
run-splitting semantics (mode alternation opens new families in call
order; `net_state` folds insert/delete/reinsert correctly);
empty-commit no-op.

**Rust engine:**
- `txn_groups_sal_size` equals the bytes `scatter_wire_group` /
  `write_group_direct` actually consume — partitioned, replicated,
  empty-worker-slot, and string/blob (non-wire-safe) schemas. Mandatory:
  the Phase-B fatal-abort path makes exactness load-bearing.
- Committer: transactions emitted first, families unsorted in frame order;
  `CommitRequest::Txn` held intact across a checkpoint sequence
  (`await_servicing`) and across a `MAX_PENDING_ROWS` boundary (including
  first-request-is-Txn seeding); Phase-A reconciliation marks all of a
  transaction's groups on any family failure; a Phase-B emission failure on a
  transaction's **first** family (before it commits any family — inject a
  first-family `write_commit_group` error) fails the whole transaction cleanly
  with zero bytes written and leaves sibling single pushes in the batch
  unaffected; every `commit_pushes` exit path (early-error return included)
  resolves the txn `done`.
- Chunked gather: a table whose (FK-referenced ∪ unique-index) projection
  exceeds 8 columns resolves old values across multiple `execute_gather_async`
  chunks and the merged `GatherMap` returns, per touched PK, every projected
  column's committed value in projection order (equal to a single hypothetical
  gather were the 8-column cap absent); NULL columns stay `None`.
- Simulation unit tests: last-op maps for insert/delete/reinsert,
  weight-normalized (+2) rows, delete-of-absent; prefix vs whole-bundle
  fold divergence (take-then-retire); `retired`/`added` derivation with
  gathered old values, NULL columns skipped, composite index key images.

**E2E (pytest, `GNITZ_WORKERS=4`):**
- Two-table transaction: both visible after commit; a view joining both
  tables reflects both after a scan.
- Crash atomicity: `GNITZ_INJECT_PUSH_ABORT=after_groups` on a two-table
  transaction → after restart, neither table has the rows
  (`GNITZ_CHECKPOINT_BYTES` pinned high so no checkpoint races the window).
- FK bundle semantics, each with the FK spanning worker partitions:
  parent+child insert in both frame orders passes; child insert referencing
  a parent deleted (or upserted away, non-PK unique referenced column) in
  the same bundle fails; parent-delete + all-children-delete passes;
  parent-delete + partial-children-delete fails; parent-delete + child
  Update-mode re-pointing to another parent passes; parent
  delete-then-reinsert + child insert passes; parent re-key (delete PK a
  holding v, insert PK b holding v) with committed children referencing v
  passes.
- Uniqueness: duplicate-key (Error) in one family aborts the whole bundle,
  sibling table unmodified; `(Update-insert k, Error-insert k)` frame order
  rejects; Update-family delete of committed k + Error-family insert of k
  passes (replace idiom); two families inserting the same unique-secondary
  value on different PKs rejects; retire-then-take of a unique value in
  frame order passes; take-then-retire rejects; blind delete-then-insert of
  an uncommitted key passes; value shift across transaction-created rows
  (family 1 inserts `(h,5),(k,6)`, family 2 upserts `(k,5),(h,7)`) passes
  via the semantic release rule; the two wider-than-sequential shapes pass
  and are pinned as such: `[D(h), I(k,v)]` against committed `h(v)`, and
  `[I(h,w), I(k,v)]` against committed `h(v)` (sequential pushes reject
  both; the transaction accepts both; post-state single-holder asserted).
- Shape: non-`unique_pk` table family rejected; RESTRICT fetch budget
  exceeded rejected cleanly; parent-delete with committed children and NO
  child family in the bundle rejected (F2 step 2).
- Wide schema: a table with more than 8 projected old-value columns (several
  FKs plus composite unique indexes) transacts correctly through the chunked
  gather — an upsert that retires an FK-referenced value and a unique-index
  value at once is validated (F1/F2/U see the gathered old values) and commits.
- Concurrent single pushes to a bundled table during a transaction storm:
  per-PK weight conservation on scans.
- Zone sharing with disjoint tables: transactions on tables {A,B}
  concurrent with pipelined single pushes on table C (no lock overlap, so
  they share committer batches/zones); all ACKs correct, per-PK weights
  conserved on all three tables, and an oversized single push on C failing
  mid-storm never affects a transaction's outcome.
- Oversized transaction: terminal `exceeds SAL capacity` on a can-never-fit
  bundle; transient `exceeds SAL space` recovers on the first retry (the
  `force_checkpoint` flag guarantees progress even when the advisory was
  silent); server stays healthy throughout, no repeated checkpoints for
  the same retried transaction.
- Python `with client.transaction()` commit and exception-rollback paths.
