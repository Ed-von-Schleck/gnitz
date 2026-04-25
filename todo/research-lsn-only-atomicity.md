# Research: LSN as the Sole Atomicity Mechanism

*Written 2026-04-25. Research-only document — proposes nothing, fixes nothing.
Triggered by Issue 3 of `architectural-brittleness-type-system-and-schema-limits.md`,
extended by the question: what would it take for the LSN to be the only mechanism
the system needs for atomicity, and to make this generalize to atomic zones beyond
DDL?*

---

## 1. The question, sharpened

The DDL post-mortem in the brittleness document showed that a CREATE TABLE is not
atomic: `COL_TAB` and `TABLE_TAB` writes get distinct LSNs (K and K+1 from the SAL
writer's internal counter), and a crash window between them leaves orphan column
records that no future operation can clean up. The current architecture relies on a
mosaic of mechanisms to paper over this:

- A **single fdatasync** at the end of a DDL covers all broadcasts (executor.rs:757–773).
- The **epoch fence** stops post-checkpoint replay from contaminating pre-checkpoint state
  (bootstrap.rs:67–69, 137–141).
- **Z-set arithmetic** makes accidental double-application of insertions idempotent
  (weight +1 +1 = +2, observed as one row when filter is `weight > 0`).
- A per-table **`max_flushed_lsn`** filters already-flushed user-table replays
  (bootstrap.rs:50–54, 88–89).
- **Hook ordering** in `replay_catalog` processes TABLE_TAB before COL_TAB so dangling
  COL_TAB rows are simply skipped at replay time (catalog/bootstrap.rs).

These all *work*, but together they form what the user described as "a mosaic of
mechanisms." The research question:

> Is there a design in which **the LSN alone** is the atomicity primitive — such that
> the same mechanism that protects DDL also generalizes to other atomic zones (e.g.,
> multi-statement DML, composite migrations) without adding new transaction IDs,
> markers, or shadow state?

The honest answer is "almost." This document explores what "almost" means, what the
remaining vestigial pieces are (footers, sentinels), and which are intrinsic to any
write-ahead-log protocol versus contingent on the gnitz design.

---

## 2. Current state, precisely

### 2.1 LSN is allocated at three different sites with different semantics

There are **two distinct LSN counters** in the system:

1. **`shared.ingest_lsn: Rc<Cell<u64>>`** — the *logical* LSN exposed to clients
   (`executor.rs:86, 750`). Bumped once per push group in the committer
   (`committer.rs:359–361`), once per DDL operation in the executor
   (`executor.rs:750–751`). This is what the client sees in the ACK.

2. **`SalWriter::lsn: u64`** — the *physical* per-SAL-group sequence
   (`sal.rs:294, 360–361, 417–418`). Incremented every time a SAL group is written:
   per `write_group_direct`, per `write_broadcast_direct`. Each `broadcast_ddl` in a
   DDL therefore burns one of these.

These two counters drift in DDL: a CREATE TABLE bumps `ingest_lsn` once but emits
two SAL groups (COL_TAB then TABLE_TAB), so the SAL writer's internal counter
advances by two. The client's "DDL LSN" in the ACK is the post-bump value of
`ingest_lsn`, which doesn't directly correspond to either of the SAL group LSNs.

This drift is the architectural crack through which orphans appear. The SAL knows
about per-group LSNs; recovery walks per-group LSNs; but the *atomic zone* (the DDL)
spans multiple groups and is not first-class anywhere in the SAL.

### 2.2 The atomic zone exists only as an fsync boundary

The DDL executor (`executor.rs:757–773`) does:

```rust
let drained = drain_pending_broadcasts();          // queued from hooks
let fsync_fut = {
    let _sal_excl = sal_writer_excl.lock().await;  // serialize SAL writes
    for (tid, bat) in &drained {
        broadcast_ddl(tid, bat);                   // write SAL group, no fsync
    }
    reactor.fsync(sal_fd)                          // ONE fsync for all of them
};
let fsync_rc = fsync_fut.await;
// ACK only after fsync
```

This is a *de facto* atomic zone visible to the client: the ACK is sent only after
the single fsync. From the client's perspective, all-or-nothing holds.

But on the *server* recovery path, the atomic zone is invisible. The SAL is a flat
sequence of per-group records; recovery walks it group by group. There is no
"begin DDL" / "end DDL" structure in the file, no shared LSN tying the broadcasts
together. If the OS happens to flush some dirty mmap pages before a crash but not
others, recovery can encounter:

- The first broadcast group only (orphan COL_TAB)
- The second broadcast group only (logically impossible without first, but possible
  on disk if pages flush out-of-order — though the size-prefix sentinel pattern
  partially mitigates this)
- Both broadcasts but no fsync barrier (still recovers; no client ACK was sent, but
  the data is durable)

The size-prefix sentinel (`sal.rs:96, atomic_store_u64`) does provide *per-group*
torn-write protection: workers reading a group with size prefix 0 treat it as
end-of-log. But this is a per-group protection, not an atomic-zone protection.

### 2.3 The protections that compensate

Because the SAL has no native atomic-zone concept, the system papers over it with:

1. **Single-fsync atomicity (durability):** All broadcasts in a DDL share one fsync.
   The fsync either persists all of them or none get ACKed. From the client's
   perspective: atomic.

2. **mmap visibility ordering:** Workers see broadcasts via Release/Acquire on the
   size prefix. They never see partial groups in memory.

3. **Hook-replay ordering:** On recovery, `replay_catalog` processes TABLE_TAB rows
   before COL_TAB rows. If a TABLE_TAB row is missing for some `new_tid`, the COL_TAB
   rows for that tid are simply never consulted (orphan, but harmless to schema
   correctness).

4. **Z-set idempotence under reapplication:** If the SAL is replayed on top of
   already-flushed shard data (which happens when `flushed_lsn` is not consulted —
   system tables don't have it), insertions accumulate to higher weights but stay
   semantically one row.

5. **Epoch fence:** Prevents pre-checkpoint SAL data from being replayed after a
   post-checkpoint state exists.

6. **Per-table `flushed_lsn` for user tables:** Real LSN-based dedup, but only on
   user tables.

### 2.4 What this collection actually buys

It buys **safety against double-application** and **safety against the client
believing a half-completed DDL succeeded**. It does **not** buy:

- **Cleanup of orphans on crash recovery.** Orphans accumulate forever.
- **Idempotent DDL retry.** A retry creates a new table ID; the old orphan ID
  becomes permanently dead.
- **A single recovery rule.** Recovery has special cases for system tables (no
  LSN dedup) vs user tables (LSN dedup), and the correctness argument for the
  former depends on Z-set idempotence + hook ordering rather than any single
  invariant.
- **An atomic-zone primitive that generalizes.** Today's "DDL atomicity" is a
  property of the executor code path (one fsync, one write-lock). User-level
  multi-statement transactions cannot piggyback on this without rewriting the
  executor; nothing in the SAL or the recovery code is abstract over zones.

---

## 3. What "LSN-only atomicity" could mean

There are at least three distinct interpretations of the user's phrase. Worth
naming them so the design discussion is precise.

### Interpretation A: the LSN is the atomic unit

All operations within an atomic zone share **one LSN**. Recovery applies records by
LSN; an LSN is either fully applied or fully discarded. This is the model PostgreSQL
uses for a single XLOG record (which can touch many pages).

### Interpretation B: a high-water-mark LSN is the commit barrier

There are still per-record LSNs, but a separate `committed_lsn` watermark advances
only when an atomic zone closes durably. Recovery applies records up to
`committed_lsn` and ignores anything beyond. This is the model used by InnoDB redo
logs and many distributed systems (Raft commit index, Spanner safe time).

### Interpretation C: same LSN, multiple records, with a commit sentinel

Multiple records share one LSN, and a final "commit" record marks the LSN as
durable. Recovery groups by LSN, applies only LSNs whose commit sentinel is
present. This is a hybrid: the LSN names the atomic zone (A), and a sentinel marks
its completion (B-style watermark, but encoded inline).

The user's phrasing — *the only mechanism we need for atomicity is the LSN* —
most naturally maps to interpretation **A**, with the understanding that *some*
sentinel/footer is intrinsic to any WAL-based atomicity (you have to be able to tell
"is this LSN complete on disk?" from disk content alone). The sentinel doesn't add a
*second* atomicity mechanism; it's just the encoding of "this LSN closed."

The rest of this document treats interpretation A as the target and explores how
much sentinel machinery is unavoidable.

---

## 4. Design space

Five concrete designs, ordered by how much they restructure the existing code.

### Design 0: Status quo with a startup GC sweep (baseline)

Keep the current per-group LSN scheme. On `CatalogEngine::open()`, after recovery,
run a one-shot pass:

```
for each (owner_id, owner_kind=TABLE) in sys_columns:
    if no row in sys_tables with table_id = owner_id:
        retract all matching sys_columns rows
```

Same for index columns, view columns, etc.

**Pros:** Tiny change. No protocol changes. Solves the orphan-accumulation half of
the problem.

**Cons:** Does not solve the "one LSN per atomic zone" goal. Does not generalize.
Doesn't address the latent 65-column panic root cause. Reactive, not preventive.

This is the cheapest path to "no orphan accumulation" but explicitly *not* the
direction the user asked about.

### Design 1: Group multiple records into one SAL group at one LSN

Today, a SAL group's payload is one wire-encoded message per worker, with one
target_id in the group header. Extend the wire format so a worker slot can carry
**a list** of `(target_id, batch)` pairs. The SAL group header still has one LSN;
the entire group is atomic.

For DDL: the executor builds a single multi-record wire payload containing the
COL_TAB batch and the TABLE_TAB batch, writes one SAL group, fsyncs, ACKs.

**Pros:**
- True interpretation-A atomicity: one LSN names the zone.
- No commit footer / sentinel needed beyond what the SAL group already has.
- Workers process the group atomically (a single SAL message, applied as one unit).
- Recovery is unchanged in shape — still walks groups — but each group is now an
  atomic zone.

**Cons:**
- **Wire-format change.** `encode_wire_into` / `decode_wire` must support multi-batch
  payloads. The header in `wal_block.rs` describes one batch; this must become a
  list. Every wire consumer (workers, recovery, scan responses) gets touched.
- **Per-group target_id loses meaning.** The SAL header's `target_id` field becomes
  vestigial or repurposed (e.g., to indicate "multi-record group"). All code that
  branches on `target_id` (wal_block decode, recovery dispatch, hook lookup) must
  change.
- **Worker dispatch loop changes.** Currently a worker reads a group, dispatches
  by `flags`, ingests one batch into one table. Multi-record means a loop inside
  the dispatch.
- **Hook fan-out is bounded by the encoder.** The hook system queues
  `pending_broadcasts` with cascading hooks adding to the queue. The drained queue
  must be encoded as one wire payload — this means the encoder size estimate must
  cover the whole drained list, and the SAL slot must be large enough. Today each
  group is sized to one batch.

This is the cleanest interpretation-A design but it's a substantial wire-format
refactor. The blast radius touches every wire consumer.

### Design 2: LSN-shared SAL groups with a commit sentinel record

Keep the existing wire format (one batch per SAL group). But change the LSN
allocation: when a DDL starts an atomic zone, all SAL groups it emits share the
same LSN K. After the last record, write a small "commit" group with
`flags = FLAG_DDL_SYNC | FLAG_TXN_COMMIT`, target_id = 0, payload = empty,
LSN = K. Then fsync. Then advance the SAL writer's LSN to K+1.

Recovery:
```
group_buf: HashMap<LSN, Vec<group>>
for each group in SAL (in order):
    if group.flags has FLAG_TXN_COMMIT:
        for g in group_buf.remove(group.lsn):
            apply(g)        # all-or-nothing
    else:
        group_buf[group.lsn].push(group)
# anything left in group_buf at end of SAL is uncommitted; discard
```

**Pros:**
- Wire format unchanged.
- LSN is the atomic unit (one LSN = one zone).
- Generalizable: any caller can wrap a sequence of broadcasts in
  `begin_zone()` / `commit_zone()` and get atomicity for free.
- Recovery becomes a single rule for both system and user tables: apply by LSN,
  only if commit sentinel is present.
- Existing per-table `flushed_lsn` continues to work unchanged — it just becomes
  "max committed LSN flushed for this table."

**Cons:**
- **Commit sentinel is a new mechanism, even if small.** The flag is just a flag,
  but the recovery rule depends on it. The user's phrasing might object to this;
  pragmatically, *some* "this LSN is closed" mark is intrinsic to any zone protocol.
- **Workers must defer visibility.** Currently workers apply each SAL group as
  they see it. With deferred-commit semantics, workers must buffer per-LSN until
  the commit sentinel arrives. This is a non-trivial change to the worker hot path
  and to the catalog evaluation order: hooks fire on apply, not on receipt.
  - **Possible escape hatch:** because DDL holds the catalog write lock, workers
    can apply eagerly *and* the master can simply roll back uncommitted groups in
    memory if a panic happens before fsync. No external observer (query, client)
    sees uncommitted state because the write lock blocks them. On crash recovery,
    the SAL doesn't have the commit sentinel, so the orphan groups are skipped.
  - This works for DDL because of the write lock. For *user* multi-statement
    transactions there's no write lock; visibility deferral becomes essential.
- **Memory cost on recovery.** Recovery must buffer all groups for an unclosed
  LSN. Bounded by the largest atomic zone, which for DDL is small but for
  user transactions could be arbitrary.
- **`shared.ingest_lsn` and `SalWriter::lsn` must agree.** Today they drift; under
  this design they reunify (both advance per zone, not per group). This is a
  positive simplification but requires touching the committer and executor.

This is the most natural fit for "LSN as the atomic unit, with minimal new
machinery." The commit sentinel is the unavoidable "closed?" mark.

### Design 3: Two-phase write with explicit BEGIN/COMMIT records

Like Design 2 but with both a BEGIN record and a COMMIT record framing the zone.
Recovery accepts records only between matching BEGIN/COMMIT pairs.

**Pros:**
- More auditable on disk inspection (operators can see zone boundaries explicitly).
- Allows pre-allocating LSN ranges (BEGIN reserves LSN K..K+N for the zone).

**Cons:**
- Strictly more sentinel machinery than Design 2 with no atomicity benefit. The
  BEGIN record duplicates information already encoded in "first record at LSN K."
- The pre-allocated LSN range complicates LSN allocation (must know N upfront,
  but cascading hooks make N dynamic).

Worse than Design 2 on every axis except "human-readable on disk." Skip.

### Design 4: Watermark-only (committed_lsn as separate state)

Per-record LSNs as today. A separate `committed_lsn` watermark on disk advances
only when an atomic zone closes. Recovery applies up to `committed_lsn` and
ignores everything past.

**Pros:**
- No commit sentinel inline — the watermark is separate metadata.
- Clean conceptual model (every LSN ≤ committed_lsn is durable).

**Cons:**
- **Watermark is a separate atomicity mechanism.** Violates the user's "LSN is the
  only mechanism" goal — now there's an LSN *and* a watermark, both essential.
- Watermark update must itself be atomic and durable, which usually means a
  separate fsync of a small file or a dedicated SAL slot. This is the kind of
  belt-and-suspenders state the user wanted to avoid.
- Replay model requires a *separate* "uncommitted region" pass: the gap between
  `committed_lsn` and the end of the SAL is *neither* applied nor erased, which
  means there's a durable "limbo" state. Operators inspecting the SAL see records
  whose status is "maybe."

Conceptually clean but adds a second atomicity primitive. Wrong direction for the
stated goal.

---

## 5. The key architectural decisions

Five binary decisions distinguish the designs above. Anyone implementing this needs
to take a position on each.

### 5.1 Multi-record-per-LSN inline (Design 1) vs LSN-shared groups (Design 2)?

Design 1 is more elegant (one LSN, one SAL group, atomic by construction) but
requires a wire-format change. Design 2 keeps the wire format and uses a commit
sentinel.

**Pragmatic answer:** Design 2. The wire format is one of the most spread-out
abstractions in the codebase (per the brittleness document, every type code
addition touches it). Avoiding wire-format changes for an orthogonal concern is
the right call.

### 5.2 Eager apply by workers vs deferred apply until commit?

If groups within a zone share an LSN but workers apply eagerly, an in-memory
visibility window opens between first record and commit fsync. For DDL this is
hidden by the catalog write lock. For user multi-statement transactions it isn't.

**Pragmatic answer:** Eager apply, but require zones to hold an exclusive lock on
the visible state during the zone. For DDL: the existing catalog write lock. For
user transactions: row-level or table-level locks (substantial new infrastructure;
not a goal of this research).

The two are decoupled: shipping the LSN-zone primitive does not require building
user transactions. Only the recovery semantics (skip uncommitted LSNs) is required.

### 5.3 Commit sentinel as a flag vs as a dedicated record type?

A flag on an empty broadcast (Design 2 as written) vs a new SAL group flavor
(possibly a 8-byte header-only record).

**Pragmatic answer:** A flag. The empty broadcast is small; adding a new wire
shape for the commit case duplicates infrastructure.

### 5.4 Recovery: per-LSN buffering vs single forward pass?

Recovery in Design 2 either buffers groups per-LSN (memory bounded by largest
zone) or does two passes (one to find committed LSNs, one to apply them).

**Pragmatic answer:** Two passes. Memory cost of per-LSN buffering is unbounded in
the limit (long zone). Two passes are O(SAL size) each, and the SAL is bounded
by `SAL_MMAP_SIZE` = 1 GiB.

### 5.5 What about the existing `max_flushed_lsn` per table?

Under Design 2, an LSN is either committed or not. `max_flushed_lsn` per table
becomes "max LSN whose commit-sentinel is on disk and whose data has been merged
into shard files." The semantics are unchanged, but the meaning of LSN cleans up:
no more "this LSN was emitted but never committed" gray area.

For system tables, today there is no `max_flushed_lsn` — recovery always replays
from offset 0, relying on Z-set idempotence. Under Design 2, system tables can
adopt the same `max_flushed_lsn` mechanism as user tables, with
`flush_all_system_tables()` updating it. This unifies recovery: one rule, both
table classes.

---

## 6. Generalization: what "more atomic zones" would unlock

The user explicitly cited future "atomic zones" beyond DDL. With Design 2 in
place, the following become straightforward:

### 6.1 Multi-statement DML transactions

```
client: BEGIN
client: INSERT INTO t1 ...   → server queues, no SAL write yet
client: INSERT INTO t2 ...   → server queues
client: COMMIT               → server emits all groups under one LSN +
                                commit sentinel + fsync, ACK
```

The atomic zone semantics fall out for free. The only new infrastructure is
client-side staging (which transaction ID the client uses to associate
statements) and server-side staging (queueing batches per transaction). The SAL
and recovery are already correct.

### 6.2 Composite DDL (multiple DDL statements as one zone)

```
CREATE TABLE t1 (...);
CREATE INDEX ix1 ON t1(...);
CREATE VIEW v1 AS SELECT ... FROM t1;
```

Today these are three separate DDLs, each with its own LSN and ACK. If any one
fails, partial state remains. Under Design 2, all three can be wrapped:

```
BEGIN DDL ZONE;
CREATE TABLE t1 (...);
CREATE INDEX ix1 ON t1(...);
CREATE VIEW v1 AS SELECT ... FROM t1;
COMMIT DDL ZONE;
```

All three operations share one LSN and one fsync. Failure rolls back atomically.

### 6.3 Schema migrations

A migration that does (alter column type, backfill, drop old column) is naturally
an atomic zone. Under Design 2, the migration runs as one zone; on crash, the
migration is either fully applied or fully reverted, no half-state.

### 6.4 Compaction-with-checkpoint

Compaction merges shard files; checkpoint advances `flushed_lsn`. If compaction
crashes mid-merge, recovery must understand the merge wasn't durable. Today this
is handled by writing the new shard file then renaming it (filesystem atomicity).
Under Design 2, the compaction event is just another zone: emit the manifest
update, the shard rename, and the LSN advance under one zone.

### 6.5 Cross-shard operations

Any future feature that touches multiple shards or multiple system tables (e.g.,
foreign key cascade with rebuilt indexes, partition merge, distributed snapshot)
gets atomicity for free if it can be expressed as a sequence of SAL groups.

---

## 7. What is *not* solved by LSN-only atomicity

This research question is narrower than it might first appear. Several adjacent
problems are out of scope.

### 7.1 The 65-column panic root cause

The latent bug described in the brittleness document (Issue 3, the unresolved
panic) is about *how a 65-row COL_TAB record set ended up in `read_column_defs`
output*. Atomicity changes do not explain or fix this. The fix is independent
instrumentation in `read_column_defs`. (The brittleness doc already proposes
this in Section 6 of its summary.)

### 7.2 Two-phase commit across shards

Atomicity within a single gnitz instance is the question here. Cross-instance
atomicity (sharded gnitz) is a different problem requiring 2PC or consensus.

### 7.3 Long-running zones and SAL pressure

A zone that emits thousands of records before committing keeps SAL space
reserved (no checkpoint can advance through an uncommitted zone). Pathological
zones could exhaust the 1 GiB SAL. This needs a separate policy (zone size limits,
or fail-on-checkpoint-pressure).

### 7.4 Read isolation during a zone

Eager apply (chosen above) means workers see uncommitted state in memory.
Concurrent reads need a way to filter this out — for DDL, the catalog write lock
suffices; for user transactions, this requires an MVCC or snapshot read mechanism.
Neither is in scope for the atomicity primitive itself.

### 7.5 The SAL recovery model for system vs user tables

Design 2 unifies recovery (one rule), but adopting `max_flushed_lsn` for system
tables is a separate change with its own correctness argument (the existing
Z-set idempotence and hook ordering must remain valid as backups even when LSN
dedup is in effect). Worth doing, but separate work item.

---

## 8. Recommendation summary

If the goal is "the only atomicity mechanism is the LSN," the cleanest path is
**Design 2**: a sequence of SAL groups at the same LSN, terminated by a
flag-marked empty group, with recovery that applies LSNs only when their commit
sentinel is on disk.

The unavoidable extras beyond pure "LSN, nothing else":

- A 1-bit flag on the SAL group header (`FLAG_TXN_COMMIT` or similar) — this is
  the closure-marker, not a second atomicity primitive.
- A two-pass recovery (first pass identifies committed LSNs; second applies).

Everything else falls out:

- DDL atomicity (today's main pain point) — fixed.
- Orphan COL_TAB on crash — eliminated; uncommitted LSNs are skipped at recovery.
- Generalization to user transactions, composite DDL, migrations — natural fit.
- Recovery becomes one rule for all table classes — system and user tables apply
  by `lsn > max_flushed_lsn AND lsn ≤ max_committed_lsn`.

The primary cost is touching the executor's drain-and-fsync loop to share an
LSN across all broadcasts and emit a commit sentinel, and touching the recovery
loops in `bootstrap.rs` to do two passes. The wire format, the worker hot path,
and the hook system stay unchanged.

---

## 9. Open questions to resolve before implementation

These are the questions a follow-up design doc would need to answer:

1. **What's the upper bound on records per zone?** (Bounds recovery memory and SAL
   space allocated for uncommitted zones.)

2. **Should the commit sentinel be empty or carry zone metadata?** (E.g., zone
   size, originating client ID — useful for debugging, not for correctness.)

3. **How does a zone interact with FLAG_FLUSH (checkpoint)?** (Checkpoint must
   not commit-truncate the SAL while an uncommitted zone is in progress, or its
   records will vanish.)

4. **What happens if a zone is abandoned mid-write (panic in the executor before
   the commit sentinel)?** (The executor today already handles partial drain via
   `drain_pending_broadcasts()` clearing on error. With Design 2, the SAL groups
   already written to mmap are simply orphaned LSN groups; recovery skips them.
   But workers may have applied them in memory — does the catalog write lock
   release leave the catalog dirty? Need a rollback path or a guarantee that
   panics within a zone abort the server.)

5. **Should `shared.ingest_lsn` and `SalWriter::lsn` be unified?** (Pragmatically
   yes — the drift today is a source of bugs. Under Design 2 they share semantics.)

6. **How does this interact with the per-group `target_id` in the SAL header?**
   (Multi-target zones leave `target_id` ambiguous. The natural answer: each
   group within the zone has its own `target_id` as today, only the LSN is
   shared.)

7. **What is the migration plan for an existing on-disk SAL?** (A SAL written
   under the old protocol has no commit sentinels. Recovery must handle both
   formats during the transition. Simplest: bump epoch on first boot under new
   code, treat all pre-bump entries as legacy.)

---

## 10. Code sites that would change under Design 2

Concrete file/line targets, for sizing only — not a plan, not an implementation
sketch.

| Site | Today | Under Design 2 |
|------|-------|----------------|
| `runtime/sal.rs:360, 417` (`SalWriter::lsn += 1`) | Per group | Per zone (held externally, advanced by commit sentinel emission) |
| `runtime/sal.rs:27–36` (flag constants) | Existing flags | Add `FLAG_TXN_COMMIT` |
| `runtime/executor.rs:750–771` (DDL emission) | One LSN bump, N broadcasts, one fsync | One zone-LSN, N broadcasts, one commit sentinel, one fsync |
| `runtime/master.rs:753` (`broadcast_ddl`) | One group per call | Unchanged; caller wraps with begin/commit |
| `runtime/bootstrap.rs:44–110` (`recover_from_sal`) | Per-group LSN dedup | Two-pass: collect committed LSNs, then apply |
| `runtime/bootstrap.rs:121–172` (`recover_system_tables_from_sal`) | No LSN dedup, Z-set idempotence | Same two-pass model; gain LSN dedup |
| `catalog/store.rs:531` (`get_max_flushed_lsn`) | Per-user-table only | Extend to system tables (unifies recovery) |
| `runtime/committer.rs:359–361` (push group LSN) | One LSN per group | One LSN per zone (a push request is implicitly a one-record zone) |
| Worker dispatch (worker.rs) | Apply each group on receipt | Optional change: defer if FLAG_TXN_COMMIT not yet seen. For DDL (write-locked) this is unnecessary; for future user transactions it would be required. |

The shape of the change is concentrated in the SAL writer, the DDL executor, and
the two recovery functions. Worker behavior need not change for DDL, only for
future user transactions.

---

## 11. Closing observation

The current architecture protects clients from observing partial DDL through
fsync ordering and protects internal state from divergence through Z-set
idempotence and hook ordering. Both are correct, but they're correct *separately*
and they don't generalize — a future "user transaction" feature would need its
own protocol layered on top.

The LSN is already the durable ordering primitive of the SAL. Making it also the
atomicity primitive (Design 2) collapses the durability protocol and the
atomicity protocol into one mechanism. The only thing standing between today's
code and that future is:

1. Per-zone (not per-group) LSN allocation.
2. A commit-sentinel flag.
3. Two-pass recovery.

Everything else — Z-set idempotence, hook ordering, mmap visibility, the
write-lock-protected DDL window — remains valid as a *defense in depth*, not as
the primary correctness argument. That inversion (LSN as primary, the rest as
defense in depth) is what "LSN is the only mechanism we need" really means in
practice.
