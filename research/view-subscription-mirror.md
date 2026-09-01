# Subscribing to a view or table: a client-side maintained copy

Research notes. Read of the tree at `e0b31a19`; measurements taken there unless
stated otherwise.

A client keeps an automatically-updated copy of a view or table and answers reads
against it locally instead of over the wire. This memo states what the feature
does, the architecture it lands on, and the evidence for every claim the design
rests on. It is not a plan.

---

## 1. Behaviour

| | |
|---|---|
| subscribable | plain views, capacity-bounded views, base tables, streams |
| what a handle is | **both** a per-tick delta feed and the integral it maintains. A stream collapses to feed-only: its integral is empty by definition |
| local read surface | full ad-hoc **single-relation** reads: bound + predicate + projection + ORDER BY/LIMIT + GROUP BY fold — i.e. a `ReadSpec` |
| how a read is expressed | the handle takes an **encoded `ReadSpec` + reply schema** — the wire read verbs, not SQL. SQL reaches it through one pipeline whose execution target is a parameter (§7) |
| what a read returns | a weighted Z-set in the wire reply form — a **single-worker** reply, which the ordinary client-side finishing accepts unchanged (§6.2) |
| freshness | tick-atomic (never half a tick or half a transaction), monotonic, arbitrary lag, lag exposed, explicit wait-until-LSN |
| own uncommitted writes | not overlaid — the copy is committed server state only. This is what a remote `SELECT` already does: `overlay::` is reached only from `insert.rs` and `mutate.rs`, so no read path overlays the txn buffer today |
| several handles | independent LSNs; no common cut |
| falling behind | coalesce into one Z-set; past a per-subscription byte cap the accumulator is discarded and the subscription is marked reseed-needed (§9.1) |
| who drives freshness | deadline-armed piggyback, with the staleness floor a server-wide knob (§8) |
| capacity | the client may bound **its own** copy in bytes of registered shard files, independently of any server-side capacity; eviction is by write recency and a read touching an evicted key fetches it from upstream (§3). A stream has no integral, so nothing to bound |
| secondary indexes | the server relation's indexes are mirrored automatically |
| lifetime | persistent on the client's disk; on restart the handle blocks until caught up |
| offline | not supported — a live connection is required |
| DDL | an ALTER transparently reseeds under the new schema and the handle survives; DROP (or a rename out from under it) invalidates with a typed error |
| float aggregates | same *shape* as the server's answer, not necessarily the same *value* (§11.2) |

Scale target: both corners, neither favoured. Which one matters is not knowable
yet, so no decision here may foreclose the other — the per-subscription byte cap
of §9.1 is sized for that reason, and §9.2's use of the global catalog lock is
the one place where the many-subscriber corner could still force a change.

---

## 2. What the tree already provides

**The connection is strictly request/response.** `connection_loop`
(`runtime/orchestration/executor.rs:592`) receives one message, handles it to
completion, then receives the next. Its comment states why replies leave in
request order: clients correlate pipelined replies positionally (`gnitz.aio`
gathers a mixed group onto one round trip; `Session::recv_cached` rejects an
out-of-order `target_id` as a protocol error). There is **no server-initiated
frame class anywhere**, and **no registry of connected clients** — a `Peer` is
owned by its own `connection_loop` task and is reachable from nowhere else.

**The delta already exists as a first-class object.** A view's per-tick output
delta is produced by `DagEngine::evaluate_dag_multi_worker`
(`query/dag/exec.rs:334`) and ingested into the view's output store. Base tables
and streams need no tick at all — the effective batch is in hand at commit
(`committer.rs`, Phase C).

**Pending work per relation is already tracked.** `shared.tick_rows` is a
`tid -> pending row count` map bumped on every commit; the auto-tick fires when
any tid reaches `TICK_COALESCE_ROWS = 10_000` (`executor.rs:53`,
`committer.rs:859-877`).

**`Table` is already the right unit.** Its own doc: *"per-`Table` (= per relation
per worker)"* (`storage/lsm/table/mod.rs:27`). A `Table` is an unpartitioned
single store; partitioning lives above it (`child_dir`, `ChildAddr`,
`repartition`). A client mirror is one `Table` at W=1.

**The restart policy exists by name.** `RecoverySource::Rederive { resume_at }`
(`storage/lsm/table/mod.rs:81`) — resume from the manifest if it carries the
expected generation, else erase and rebuild from source. That is "resume the
copy, else reseed", and it is what view output stores and secondary indexes
already use.

**The client already evaluates over batches.** `ZSetBatchView` implements
`gnitz_expr::RowSource` (`gnitz-core/protocol/regions.rs:208`), and
`gnitz-sql/exec/{residual,order,agg_finish}` already run predicates, ORDER
BY/LIMIT and aggregate folds client-side — SQL finishing is client-side by
design.

---

## 3. Where the feature set lives today

| requirement | code | rung |
|---|---|---|
| ordered Z-set store, (PK,payload) identity, arbitrary weights, duplicate PKs | `storage::Table` in its view-output-store configuration | L3 |
| persist + resume-or-reseed | `RecoverySource::Rederive{resume_at}` + `lsm/manifest.rs` | L3 |
| capacity + skeleton rows | `lsm/shard_index`, `lsm/read_cursor` fold | L3 |
| rehydrate a skeleton on read | `catalog/store_io::materialize_bounded_store` | L6 |
| maintain mirrored secondary indexes | `schema::IndexKeySpec` + `query/dag/ingest::batch_project_index` | L1 + L5 |
| execute a `ReadSpec` | `catalog/scan_spec.rs` -> `ops::AdhocFold`, `expr::ScalarFunc` | L6/L4/L3 |

The read-execution half already has a client twin, deliberately. The **store**
half does not, and it is where CLAUDE.md says a mistake is silent ("wrong weights
silently — no error, no assertion") and where the capacity feature's correctness
rests on **fold totality**, a whole-program argument about which merges can see a
skeleton row.

The capacity row is the one that transfers least, and what a client bound inherits
from `enforce_capacity` (`shard_index/index.rs:513`) and from the `read_cursor`
fold that reads its output shapes the feature:

- **The unit is bytes of registered shard files.** `resident_bytes()` sums every
  registered shard's `file_len()`; the RAM tier is bounded separately by
  `inmem_ceiling` (§10.1), so the two compose rather than compete.
- **Only shards hold skeletons.** "Compaction writes a skeleton, so the memtable
  and RAM tier never hold one" (`:106`) — recent deltas are always hydrated, and
  a bound only bites data that has reached the terminal level.
- **Eviction is by write recency, not read recency.** Victim choice is the
  terminal guard with the smallest `max_lsn`, and the code says why: "nothing
  anywhere records that a row was *read*". A client bound therefore evicts what
  was written longest ago. It is a residency bound, not an LRU cache, and a mirror
  read pattern uncorrelated with write order gets no locality from it.
- **There is a floor.** A capacity below the store's *skeleton floor* — the size
  of a fully dehydrated store — cannot be met; the sweep converges there across
  spills. The floor scales with key count, so a mirror's minimum footprint is set
  by how many keys it holds, not by how wide they are.
- **The fold is per PK group, not per cursor.** `merge_less` orders a skeleton
  row ahead of the hydrated rows sharing its PK (and answers "neither is less" for
  a skeleton/skeleton pair, so an absent payload is never compared);
  `merge_eq_payload` groups any pair where either side is skeleton, ahead of the
  payload comparator; and `drive_merge` compares each later head against the group
  *exemplar*, which that ordering has made the skeleton row. So a PK group holding
  a skeleton row folds whole, and a hydrated-only group keeps its (PK, payload)
  split **even in a cursor that has skeleton sources** — `ReadCursor`'s
  cursor-wide `any_skeleton` is a cost gate, false exactly where the test would
  answer false anyway, not a coarser rule. The engine's own case is
  `a_skeleton_row_coarsens_its_whole_pk_group`. This is the shape a second
  implementation would have to match, down to the ordering: the skeleton row is
  the exemplar because its columns read back as the zero cell, against which an
  all-zero (or, on a nullable schema, all-null) hydrated row compares `Equal`.
- **Rehydration changes source, not mechanism.** `materialize_bounded_store`
  collects skeleton keys and hydrates them from the view's own operator traces or
  source store; a mirror has neither and fetches from upstream instead. That
  fetch is an ordinary `scan_spec` — see below.

The `VIEW_TAB` row is the only thing that sets a capacity today, so a bounded
base-table or mirrored store is a configuration nothing currently exercises. The
eligibility rules around it (no bounded view over a bounded view or a stream)
exist because rehydration reads the *source store*, so they neither apply to a
mirror nor constrain it.

The skeleton fetch needs no new wire interaction. `ReadBound::PkSet(Vec<u128>)`
(`gnitz-wire/read_spec.rs:138`) is already in the spec vocabulary — a `pk IN (…)`
gather, capped at `MAX_PK_SET_KEYS` (65,536) per request with the overflow policy
stated at the constant: *"A longer list is served as several gathers or as an
ordinary predicate scan, never rejected."* The server side already executes it —
`scan_spec.rs:248` turns the bound into a `PkSetGather` over OPK-sorted keys —
and the request is broadcast, so each worker contributes the keys it holds and a
client that cannot know the placement does not need to.

So a mirror rehydrates with the shape `materialize_bounded_store` already uses:
walk the local store, copy hydrated rows and collect skeleton keys, then hydrate
that key list — the one substitution being an outbound `scan_spec` where the
server reads `BoundedRead::Keys` locally. Both halves come back ascending and
disjoint by PK, so the merge needs no sort.

One width limit applies: a `PkSet` key is a `u128`, and the bound's doc notes the
worker rejects keys beyond *"the PK's width"*. A relation whose `pk_stride`
exceeds 16 bytes cannot express its keys this way and falls to the documented
alternative, an ordinary predicate scan. That is reachable, not theoretical:
`MAX_PK_COLUMNS` is 5 and `MAX_PK_BYTES` 80, so a three-column `BIGINT` PK at 24
bytes already exceeds the width.

Positivity, which CLAUDE.md pairs with fold totality to make one summed weight
per key exact, is enforced only for base tables — `enforce_unique_pk` states the
contract as "per-PK accumulated weight ∈ {0, 1}". No equivalent check guards a
view output store; non-negativity there holds by construction, because every
operator that can emit a negative intermediate feeds an integral whose
accumulated value is a bag. A mirror inherits that same by-construction property
and no more.

---

## 4. Architecture: the copy is an embedded engine

The copy lives **in-process**, like SQLite, and the client links the
below-runtime engine stack as a library.

### 4.1 The cost of a local round trip

A local round trip is not free. From this repo's recorded benchmark run
(`benchmarks/results/20260810_145209/w4_c1`, commit `5d7b23b0`, W=4, AF_UNIX,
1 client):

| case | p50 | throughput |
|---|---|---|
| `features/test_reads::test_seek_by_index` (point seek) | **0.028 ms** | 33.8k/s |
| `test_view_passthrough_seek` | 0.081 ms | — |
| `test_view_scan` | 0.083 ms | — |
| `test_view_seek_natural` | 0.121 ms | — |
| `combined/test_serving` | 0.219 ms | 4.4k ops/s |

A point read floors at ~28 us; realistic point/serving reads land at 80-220 us,
i.e. 5k-35k local reads/s per client on one connection. These are W=4 figures and
a single-worker server would sit below them — an index-bound read fans out to all
four workers here (`scan_spec_route`, §6.2), where at W=1 it would not. What does
not change with worker count is the structure: the post-fork master owns no user
store (`catalog/store_lsn.rs:15`, whose comment at `:27` reads *"the post-fork
master owns no store"*), so every read crosses **two process boundaries** plus the
SAL/eventfd wake and the W2M ring. A sidecar would also mean a second process to
supervise, a data dir, and >=16 MiB of preallocated SAL (`MIN_SAL_BYTES`,
`runtime/protocol/sal.rs:201`).

Binary size is not the axis: the release server is **3.67 MB** stripped against a
client `libgnitz.so` that is already **6.98 MB**.

### 4.2 What a client-side store would have to re-derive

A hand-written client store must re-derive the store half of §3. What drops out
for a single-partition, single-threaded copy is the distribution and DDL
machinery: boot relayout across worker counts (`lsm/repartition.rs`, 243),
per-worker child directories (`lsm/child_dir.rs`, 120), row routing by PK
(`repr/scatter.rs`, 241) and the CREATE UNIQUE INDEX preflight sort
(`storage/spill.rs`, 192) — 796 lines, counted per whole file, so a few generic
helpers inside them (`fsync_dir`, for one) would be kept rather than dropped.

A capacity bound removes nothing further; it adds. `enforce_capacity` is built on
the level and compaction machinery: its victim search walks
`levels[TERMINAL_LEVEL_IDX]`, step 1 dehydrates through `compact_one_guard`, step
2 pushes data down with `vertical_fold` / `run_compact`, and `compact_one_guard`
reaches `compact::merge_and_route` (`shard_index/index.rs:252`). Both are
therefore required, bounded or not, and both are counted below.

Figures on both sides of the ledger are **production lines**, and the rule has to
be stated exactly, because a looser reading of it moves these numbers by more than
the argument they carry: walk each path, skipping any directory named `tests` and
any file named `tests.rs`, `bench*.rs`, `*_tests.rs` or `*_proptest.rs`; truncate
each remaining file at a `#[cfg(test)]` line immediately followed by `mod tests`;
count the lines that are neither blank nor comment-only. The file-name half is
what `storage/` needs — it holds no `tests` directory at all, its test and bench
code sitting in sibling files (`tests.rs`, `bench.rs`, `bench_flush.rs`,
`data_roundtrip_proptest.rs`), which a directory-only filter counts as production
and which inflates `lsm/read_cursor/` from 660 to 2,378 and the layer from 7,360
to 9,414. **Every line figure in this memo is on that rule** — the table below,
the 796 above, §5.2's read path and §12's engine-lib total alike.

| | lines |
|---|---|
| `repr/batch.rs` — batch representation | 1,393 |
| `repr/shard_file.rs` + `repr/shard_reader/` — on-disk image and mmap reader | 978 |
| `repr/{batch_wire,columnar,heap,layout}.rs` — region codec, comparators, loser tree | 665 |
| `lsm/read_cursor/` — N-way merge, ghost elimination, skeleton fold | 660 |
| `repr/merge.rs` — merge sources, blob cache, German-string relocation | 608 |
| `lsm/shard_index/` — levels, guards, the capacity sweep | 586 |
| `lsm/table/mod.rs` — the store facade | 352 |
| `lsm/{run,run_set}.rs` — sorted runs and the RAM tier | 277 |
| `lsm/manifest.rs` — durable resume | 223 |
| `lsm/compact.rs` — shard merge and per-guard routing | 127 |
| **total** | **5,869** |

As an outer bound on the same measure, the whole storage layer is 7,360
production lines, 6,564 after the exclusions above. The 695 the table does not
name are the storage facade, the flush barrier, index gather, the batch pool, the
XOR8/bloom filters, PK-uniqueness enforcement and shard naming.

That is the engine's spend on those concerns — an upper bound on what could be
reused, not a floor on what a purpose-built store would cost. Much of it is
engine-specific: batch pooling, layout claims and certification, the pooled
blob-cache around German-string relocation, hostile-input validation of mmap'd
shards. The relocation itself is not — any row copy between two batches with
separate blob heaps rewrites the cell's heap offset, and a single-partition client
store does that on every merge-output row; what it sheds is the pooled cache, not
the copy. A single-threaded, single-partition client store would be smaller, and
how much smaller is not established — and §13 records why settling it would not
change anything.

Line count is the weaker half of the argument. The invariants do not shrink with
the implementation: whatever its size, a second store is a second place where
(PK, payload) ordering, ghost elimination and the skeleton fold have to be right,
and CLAUDE.md's rule for that code is that a mistake in it is silent — "no error,
no assertion". It also carries the one deliberate exception to (PK, payload)
element identity, maintained in a different crate from the copy it must agree
with.

The client is Linux-only, which is accepted. `gnitz-core` today calls `libc`
directly (`writev`/`iovec`, `socketpair`, `setsockopt`, `recv`, `shutdown`,
`_SC_IOV_MAX`, 32x `close`, `std::os::fd` x17) and carries zero `target_os`
because it targets one family, not because it is universal. The delta is
Unix-family -> Linux->=5 (`O_TMPFILE`, `FS_IOC_SETFLAGS`, `MADV_HUGEPAGE`,
`io_uring` in `lsm/flush_barrier.rs`).

### 4.3 Crate shape

A **new crate** over `gnitz-core` + the engine lib holds the subscription handle,
the local store and the local read path. `gnitz-core` stays lean and engine-free,
so a client that only does remote reads carries none of it.
`gnitz-sql`/`py` depend on the new crate only if they want mirrors.

Scope: mirrors + single-relation ad-hoc reads. Local views are **out of scope**.
The crate can compile circuits, but nothing in the handle exposes that, and
exposing it would mean owning backfill, tick scheduling and capacity semantics
client-side.

---

## 5. The cut: L6/L7

The extracted crate is **an embeddable single-node gnitz**: the Z-set store, the
DBSP operators, the circuit compiler and the catalog — everything that answers a
query and maintains a view — with none of the multi-process server that `runtime`
provides (fork, SAL, reactor, W2M rings, client protocol). It is defined by what
it is, not by what was left behind; `gnitz-engine` minus `runtime` happens to be
exactly that set.

A mirror instantiates a real `CatalogEngine` through `open(base_dir,
num_workers)`, which ensures a data directory and creates `SysFamily::COUNT`
system tables, one `Table` each (`catalog/bootstrap.rs`). Relations and their
index circuits go in the table registry; no view circuits are compiled, so
`DagEngine`'s plan cache, dependency map and view metadata stay empty. A
mirror-holding process therefore owns a data directory and the nine catalog
tables — the footprint of an embedded database, not of a cache.

**Zero up-edges into `runtime` from anywhere below it** (grep of `crate::runtime`
outside `runtime/` and `main.rs`), and the compiler agrees: moving `foundation`,
`schema`, `schema.rs`, `storage`, `expr.rs`, `ops`, `query`, `catalog`,
`test_support.rs` and `test_rng.rs` into a new `gnitz-db` crate gives

```
cargo check -p gnitz-db  ->  exit 0, 0 errors, 178 warnings, 5.85 s
```

with **zero source edits** — the only new files are a `Cargo.toml` and a 14-line
`lib.rs`. Warnings span all seven moved modules (storage 46, catalog 44,
foundation 40, query 22, ops 16, schema 7, expr 3), so the whole crate
type-checked. `gnitz-engine` keeps `runtime` + `main.rs` and depends on the new
crate, so no `lib.rs` is added to `gnitz-engine`.

That check is the extracted crate compiling **by itself**, which is what "no
up-edges" buys. Compiling `runtime` back **against** it is the other direction
and is not free; §5.1 measures what it costs.

Two incidental properties: the extracted crate needs **fewer** dependencies than
`gnitz-engine` (no `rustls`, no `rcgen` — runtime/TLS only), and every macro is
already cross-crate-safe — the five log macros and the two dispatch macros
(`with_payload_cmp`, `pk_width_dispatch`) all use `$crate::`, so they need
`#[macro_export]` and nothing else.

### 5.1 API surface

`runtime` names 67 distinct items from below, against 91 for a cut at L2/L3
(`storage`+`schema` alone). Named items are the smaller half; the member closure
behind them is the larger, and cutting higher does not avoid it.

The closure converges. Driven to a green `cargo check -p gnitz-engine` — the
reverse direction from §5's `-p gnitz-db` — it publishes **338 declarations**:

| kind | count |
|---|---|
| functions and methods | 203 |
| re-exports (`use`) | 62 |
| structs | 23 |
| struct fields | 24 |
| modules | 15 |
| enums | 8 |
| traits, type aliases | 3 |

62 of those re-exports were flipped wholesale rather than on demand — the moved
modules hold 63 — so the surface the compiler actually demands is between ~276
and 338. The waves that produce it are items first (E0603, then E0364/E0365 on
re-exports), then members (E0624 methods, E0616 fields), and four types rustc
reports with no error code at all (`Placement`, `ApplyContext`, `JoinScatterKey`,
`SourceCursor`).

Two of the 338 are not visibility changes:

- **`FkEdge::delta_key` is an orphan.** `runtime/orchestration/master/preflight.rs`
  defines an inherent `impl FkEdge` on a type that lives in `catalog/types.rs`
  and moves down with it. That is E0116 — an inherent impl cannot be defined
  outside its type's crate — and no visibility change fixes it; the impl has to
  move beside the type. It is the one structural blocker in the cut, and it is
  six lines.
- **`PooledSendBuf(pub(crate) Vec<u8>)`** is a tuple struct whose field the
  runtime constructs and indexes directly, so the field itself goes public.

`runtime` needs nothing else beyond the path rewrite. What the closure costs is
judgement rather than effort: `ApplyContext` is the clearest case, a type whose
own doc says it "lives in its own module so its fields are invisible to the
sibling modules that consume it", making the enter/exit balance of its scope
helpers "a compile-time guarantee rather than a convention". Publishing the type
leaves that intact — its fields stay private and only the name is exported — but
each of the 338 is a decision of that shape, and a script cannot make them.

### 5.2 What fixes the boundary at L6/L7

Not API size. §5.1 measures this cut at 338 declarations; the equivalent figure
for L2/L3 was never driven out, and the two proxies available disagree — named
items favour this cut (67 against 91), `pub(crate)` ceilings favour the lower one
(633 against 328). Two properties of the tree decide it instead.

**The read path the client needs is not separable from the operators.** It is
`catalog/scan_spec.rs` (the `ReadSpec` executor, 422), `catalog/store_io.rs`
(cursor opening + `materialize_bounded_store`, 323) and `ops::AdhocFold` (the
GROUP BY fold sink, reached at `scan_spec.rs:323`). None of the three sits at L3
or below, and none detaches:

- `AdhocFold` (174) is built on the reduce operator's aggregate core —
  `agg::{Accumulator, AggDescriptor}`, `emit::emit_reduce_row`,
  `plan::{build_reduce_output_schema, ReducePlan}`, `sort::compare_by_group_cols`,
  555 further lines. A maintained view's aggregate and an ad-hoc one are one
  implementation by design, so the fold sink cannot leave the operator layer.
- `scan_spec` + `store_io` reach **twenty** distinct `CatalogEngine` members,
  among them role state (`owns_stores`), system-table access
  (`seek_family_bytes`) and ingest (`apply_local`). No "store registry" narrow
  enough to be a storage concept covers that set.

Relocating the read path is therefore ~1,474 lines including the shared aggregate
core, plus most of `CatalogEngine`, onto a boundary that 94.5% of
storage-touching commits already cross. That is a quarter of §4.2's storage half,
not half of it — the read path is the smaller body of code, and it is the
structural coupling above, not its size, that keeps it above L3.

**The caller set stays closed.** Both consumers — the server's `runtime` and the
client mirror — are in-tree, so the whole-program safety arguments on
`scatter_multi_source`, `flush_barrier`, `MemBatch::blob_id` and
`set_layout_unchecked` remain whole-program arguments. Publishing to an unbounded
external caller set is what would break them.

### 5.3 Residual work, in order

1. Mechanical: rewrite `crate::{foundation,catalog,expr,ops,query,schema,storage}`
   -> `gnitz_db::...` in `runtime` (117 errors, two classes, one sed).
2. `#[macro_export]` on seven macros; keep a `pub use crate::NAME;` beside the two
   dispatch macros so intra-crate paths still resolve.
3. The 338-declaration visibility closure (§5.1), and the relocation of
   `impl FkEdge` out of `preflight.rs`. The closure needs per-item judgement, not
   a script.
4. Move `decode_schema_block` (`runtime/protocol/wire.rs:428`) down beside
   `SchemaDescriptor`. It is seven lines, touches only `gnitz_wire` and
   `crate::schema`, has zero runtime coupling, and its own doc calls it *"only the
   projection onto the engine's own type"*. `runtime` is its sole current caller,
   so the cut compiles without it — but the mirror cannot reach it there.

---

## 6. The wire format is the converter

`gnitz-core` holds `Schema`/`ZSetBatch`; the engine holds
`SchemaDescriptor`/`Batch`. **No converter has to be written**, in either
direction, for either type — the wire format already is one, and both halves of
each direction are shipping code exercised on every request:

| direction | client half | engine half |
|---|---|---|
| schema out | `encode_schema_block` (`gnitz-core/protocol/codec.rs:19`) | `decode_schema_block` (`runtime/protocol/wire.rs:428`) |
| batch out | `encode_batch_to_wal_block` (`wal_block.rs:77`) | `Batch::decode_from_wal_block` / `decode_mem_batch_from_wal_block` (`batch_wire.rs:304`) |
| batch back | client block decode (`wal_block.rs:113`) | `Batch::encode_to_wire` (`batch_wire.rs:195`) |

Schema conversion happens **once per relation**, not per read and not per row.

### 6.1 Delta ingest is conversion-free

A received delta is a WAL block, and `Batch::decode_from_wal_block` puts it
straight into engine form: one parse plus one bulk copy per region, with the blob
heap copied wholesale and the 16-byte German-string structs bulk-copied verbatim
— *"No per-row string relocation"* (`batch_wire.rs:202-208`).

It has to be that entry point and not the inner `decode_mem_batch_from_wal_block`,
whose own contract confines it to the W2M ring: it skips
`validate_string_heap_extents`, and *"every client frame lands in
`Batch::decode_from_wal_block`, which does validate"*. A subscription delta
arrives over a socket, so it is a client frame by that rule, and the unvalidated
decode would be admissible only for a schema with no STRING or BLOB payload
column. The public entry runs the same parse and the same bulk copies; it adds the
long-string extent check.

The raw frame is reachable without decoding it first.
`ClientTransport::recv_framed` (`gnitz-core/protocol/transport/mod.rs:114`)
returns the frame as `Vec<u8>`; `recv_message` is the layer above it, and it is
that layer — not the transport — which produces a `Message { data_batch:
Option<ZSetBatch> }`. The subscription loop reads frames and decodes them into
`Batch`, so no `ZSetBatch` is built on the ingest path.

### 6.2 A local read emits a wire reply, not a byte-copy

The mirror runs the same reply encode the worker runs (`Batch::encode_to_wire`,
already `pub`) and hands `gnitz-sql` a reply in the wire form. No converter is
written and none of `gnitz-sql`'s 147 `ZSetBatch` sites move: the wire form is
the interface, and the transport is the optional part.

A local reply is a **single-worker** reply, not a byte-copy of what W workers
would have sent. `scan_spec_route` (`master/dispatch.rs:29`) broadcasts unless
the relation is replicated or the bound is a confined `PkRange` — and an
`IndexRange` bound is never confined, since a secondary index is one
unpartitioned table per worker. A fanned-out read therefore returns W frames,
each carrying that worker's own top-k slice and its own partial groups;
`read_spec_finish` and `agg_finish` reconcile them. The mirror produces one
frame, which the same finishing code accepts unchanged: `drain_reply_train`
concatenates every frame's batch through `extend_from_owned` before returning
one batch (`connection.rs:419-440`), so `read_spec_finish` and `agg_finish` never
see frame boundaries or a worker count — a one-frame reply is a shorter
concatenation, not a different shape. Equivalence is at the level of the finished
answer, and it is differentially testable against a W=1 server.

---

## 7. The read path

The handle is a wire peer, not a SQL surface. `gnitz-engine` has **no
`sqlparser` dependency** — the executor has never seen SQL — and
`Session::scan_spec(target_id, spec: &[u8], reply_schema: &Schema)`
(`gnitz-core/connection.rs:459`) takes an already-encoded opaque blob and
forwards it verbatim; `gnitz-core` never *constructs* a `ReadSpec`, `gnitz-sql`
does. The crate graph runs `gnitz-sql -> gnitz-core`, so the mirror crate sits
beside the SQL front end; SQL at the handle would either invert that or force
`gnitz-sql` to route behind the app's back.

The mirror decodes the same encoded `ReadSpec` and runs it through the same
`catalog/scan_spec.rs`. One descriptor, one executor, two transports — the
drift-safety rule `ReadSpec` and `RangeDescriptor` already state
(`read_spec.rs:8`, `range.rs:7`).

Finishing does not move. A `ReadSpec` reply is partial either way:
`read_spec_finish` sorts/windows the concatenation (`dml/select.rs:375`) and
`agg_finish` combines groups, grounds rows, computes AVG/NullfillSum and applies
HAVING (`:503`).

### 7.1 One pipeline, six methods

The SELECT pipeline reaches its source in exactly six places, across
`dml/{select,plan,overlay}`, `bind/resolve`, `access` and `exec/*`:

```
scan(table_id)                            -> ScanResult
scan_spec(table_id, spec, reply_schema)   -> Option<ZSetBatch>
seek(table_id, pk)                        -> ScanResult
table_indexes(table_id)                   -> Arc<Vec<IndexMeta>>
resolve_relation(schema_name, name)       -> (Arc<Schema>, RelKind)
txn_buffer()                              -> Option<&TxnBuffer>
```

That set is complete. `gnitz-sql` reaches the transport only through
`GnitzClient` — it never names `Session` — and every one of its 73 client
bindings is called `client`, so the call sites enumerate exhaustively: 35
methods, of which the other 29 are DDL (`ddl/`), view compilation
(`hir/lower/reduce.rs`), read-modify-write (`dml/rmw.rs`) or statement lifecycle
(`lib.rs`). None is on the ad-hoc SELECT path, and none can target a mirror.

All six already exist on `GnitzClient` with these shapes, so it satisfies the
seam by delegating to what it already does — no behaviour change to the remote
path. There is one pipeline whose execution target is a parameter, not a unified
path plus a local one. DML and DDL stay on the concrete client; a mirror is
read-only.

Two behaviours fall out of the seam rather than needing code:

- **`txn_buffer() -> None` is "committed server state only".** `dml/overlay.rs`
  states the map is empty in autocommit, that `HashMap::new()` does not allocate,
  and that "its callers short-circuit on an empty map before touching a row". The
  mirror's no-overlay semantics *is* the autocommit path; nothing branches on
  local-vs-remote.
- **Lag needs no new API.** `ScanResult`'s third element is already "the server
  LSN at which the read was served". A mirror read fills it with the copy's LSN.

---

## 8. Freshness: deadline-armed piggyback

The failure mode of pure piggyback is one state: pending source rows exist and
nothing drives a tick. That state is already tracked (`shared.tick_rows`), so
covering it needs no new bookkeeping:

- A relation with >=1 subscriber arms a deadline when its pending count goes
  0 -> non-zero. Any tick from any cause zeroes the count and disarms it.
- Where the write rate fills 10 000 pending rows within the staleness window, the
  threshold trips first and the subscription adds **no ticks at all**. Below that
  rate the deadline is what fires, so the added-tick rate is one per window —
  which is the knob's cost, and the reason it is set globally rather than per
  subscription.
- Idle: no pending rows, no armed timer, no empty ticks — an idle subscription
  costs nothing.
- Trickle: at most one added tick per staleness window, carrying everything
  accumulated in it. This **is** extra work, and the window is a throughput lever
  rather than only a latency one. Ingest cost is `ticks x resident rows`: each
  tick pays a RAM-tier fold over the whole resident tier, near-independent of how
  many rows the tick carries. Measured on `(U64 pk, I64)` at 1M rows, holding
  total rows constant and varying only the tick count: 10 ticks cost 9.9 ns/row,
  10,000 ticks cost ~9,700 — and at a fixed 100-row tick, 10x the rows costs
  ~103x the time, quadratic in tick count while the store grows. A relation whose
  live set is bounded by churn scales with that live set instead (10k live keys,
  10,000 ticks: 218 ns/row; 200k keys: 2,527). So a longer window buys an avoided
  whole-tier fold per tick on top of the cancellation, and the default should be
  set long.
- The deadline is **per relation**, not per subscriber.

The deadline belongs to `tick_loop` (`executor.rs:756`), the spawned reactor task
that already receives `TickTrigger`s on an `mpsc` and coalesces them. It owns the
timing, so the commit path keeps sending triggers exactly as it does and arms
nothing itself.

Only views need it; base tables and streams are fresh at commit for free.

A relation written constantly and read rarely costs *more* subscribed than
queried. The feature pays as a function of the read:write ratio on that relation.

---

## 9. Delivery: an unbounded reply train on its own connection

With no peer registry (§2), the master cannot address a client except from inside
that client's own task while handling that client's request. The existing
streaming reply is the right shape: `FLAG_CONTINUATION` already means "more frames
follow", and `drain_reply_train` (`gnitz-core/connection.rs:419`) loops until a
frame arrives without it. A subscription is that train, unbounded — each tick's
delta is a continuation frame group, and the train terminates when the
subscription does. No new frame class and no peer registry; what it does require
of the handler is below.

- **The parked handler must hold no catalog lock.** The ordinary read handlers
  do: `handle_scan_spec` opens with `let Some((_g, kind)) = read_lock(shared,
  peer, client_id, target_id).await` and `_g` lives across `fan_out_scan(…)
  .await` and the reply. `read_lock` (`executor.rs:2041`) returns a guard on
  `shared.catalog_rwlock` — the **global** catalog lock, not a per-relation one —
  and DDL takes its write side, so a subscription parked in that shape stops
  every DDL in the cluster for as long as the subscription lives.

  A subscription reads no store while parked; it drains an accumulator that ticks
  fill. So it can acquire, validate through `target_kind_or_reject`, and release
  before parking. What makes that safe is the accumulator being invalidated when
  its relation is dropped or altered — a mechanism that does not exist today and
  that §9.2 supplies.
- **A subscription occupies its connection.** `connection_loop` handles one
  message to completion before receiving the next, so a parked subscription blocks
  every other request on that fd. A subscription therefore needs its own
  connection — one per client process, not one per relation.
- **That connection multiplexes relations by `target_id`.** Each frame already
  carries it. The ordinary receive path cannot be reused: `recv_cached` rejects a
  `STATUS_OK` reply whose `target_id` differs from the expected one
  (`connection.rs:561`), so the subscription train gets its own receive loop that
  dispatches on `target_id`. Cross-relation tick atomicity is not needed — handles
  are independent.
- **The per-delta LSN** rides in `seek_pk`, present on every control block;
  `drain_reply_train` reads the LSN only from the terminal frame, so the
  subscription loop reads it per frame instead.

The request needs no new wire field. A control block already carries
`target_id`, `flags`, `seek_pk` (U128) with a `seek_pk_extra` blob,
`seek_col_idx` and `request_id`. One relation rides in `target_id`, or a set
rides in the blob exactly as `SCAN_MULTI` names its relations
(`validate_scan_multi_tids`, capped at 16); the resume LSN rides in `seek_pk`,
the same field a reply uses to carry one. Bits 0, 1 and 13 are unallocated in the
SAL block and 55 and 63 above it — and since the request is a routing hint
consumed at `handle_message`, never written to the SAL, a high bit fits it the
way `RESOLVE` and `SCAN_MULTI` take theirs.

Connection caps are not a concern: `max_conns` (default 256) is TLS-only
(`executor.rs:508`); AF_UNIX has no cap.

Parking on "a delta is ready" is an existing primitive, not a new one.
`reactor::sync::mpsc` gives `Receiver::recv()` as a future and `try_recv()` for a
non-blocking drain, and `tick_loop` (`executor.rs:756`, spawned at `:449`)
already runs that shape: a spawned reactor task parked on an `mpsc::Receiver`,
coalescing what arrives. A subscription handler parks on its accumulator's
receiver the same way.

### 9.1 Backpressure

Ticks write into a per-subscription accumulator that a separate drain loop
empties. Without it a slow subscriber's backlog is a **queue of frames**
(history); with it the backlog is **one consolidated Z-set** (the changed-key
set), which is what makes "coalesce, then reseed" expressible.

Consolidation bounds the backlog per subscriber but not across them: the
accumulators live in server memory, so the total is O(subscribers x changed
keys), and a hot relation's changed-key set can be the whole relation. **Each
accumulator therefore carries a byte cap.** Past it the accumulator is discarded
and the subscription is marked reseed-needed; the next emission reseeds it
through §9.3, which needs no verb the server does not already have. Server memory
is bounded by cap x subscribers, and the cost of exceeding it lands on the
subscriber that fell behind rather than on the relation's other readers.

The cap is what keeps both scale corners open. A large relation with few
subscribers never reaches it; many subscribers on a hot relation hit it and
degrade to reseeds instead of growing server memory without limit.

Eviction is not a hazard for a merely-busy subscriber: `client_send_timeout`
defaults to **30 s** and the deadline is *per frame* — *"a client making steady
progress across a large train is never penalised — only one that makes zero
progress for the full window (a stalled or maliciously zero-window peer) is
evicted"* (`reactor/conn.rs:11-16`). A subscriber dead for 30 s is evicted, which
lands it in the reseed path anyway.

The W2M ring is not at risk: the master copies a slot's bytes out synchronously
before awaiting any send — *"a client that stalls this send cannot pin a slot and
fill the worker's W2M ring"* (`master/train.rs:210-213`).

### 9.2 Invalidation

A relation dropped or altered under a live subscription must stop its train, and
the layering settles how. `drop_relation` (`catalog/hooks.rs:327`) is the funnel
every drop passes through, but it sits at L6 and the accumulator is runtime-owned
at L7 — the catalog cannot notify it, and giving it a way to would be an up-edge
into `runtime`, the one thing §5 establishes does not exist anywhere.

The handler observes instead of being told, and needs nothing new to do it. It
already takes the catalog read lock per emission (§9), and DDL holds the write
side, so re-running `target_kind_or_reject` (`executor.rs:1720`) inside that
window is serialized against every DDL by construction. A dropped relation makes
`target_kind` fail, which sends the same error frame the ordinary read path sends
and ends the train. An altered one surfaces through the schema version the frame
already carries, and the client reseeds under the new schema (§9.3).

Re-validating per emission rather than once at subscribe is what makes the
lock-release of §9 sound: between two emissions the handler holds nothing, and
the next emission re-establishes the relation still exists before it writes a
byte.

The lock it takes is the **global** `catalog_rwlock`, not a per-relation one, so
every emission contends with every DDL cluster-wide. One subscriber on a slow
tick is nothing; many subscribers on a fast-ticking hot relation is the one place
in this design where the scale corner could force a change — either a per-relation
lock for the validity check, or a validity token cheap enough to test without the
catalog.

### 9.3 Reseed

The server holds no per-subscription state across a disconnect: there is no peer
registry (§2), and the accumulator lives in the handler's task, which dies with
the connection. So it never resumes from a client's LSN — it has nothing to
resume from, and the resume/snapshot choice does not exist on the server side.

Reseed is the existing `scan` verb. It returns the relation at a cut together
with the LSN it was served at, which is exactly what a subscription needs to
start its train from. A client holding a persisted copy at an older LSN is
holding a Z-set, and the snapshot is a Z-set, so catching up is `snapshot −
copy`: negate the copy, add the snapshot, consolidate, apply the result as one
delta. The copy moves forward atomically and no new verb is involved.

That is also what client-side persistence buys, and it is not bandwidth — the
whole relation crosses the wire either way. It is **disk churn**: the local store
rewrites only the keys that actually changed, rather than being erased and
refilled.

---

## 10. Running the engine stack inside a client process

§4 puts the copy in-process; these are the conditions that placement has to meet.
Neither is a blocker — the first is a set of properties a mirror already satisfies
or can pass explicitly, the second is one missing API on `gnitz-core`.

### 10.1 What the store requires

- **Single-threaded, non-yielding.** `flush_barrier`'s SAFETY argument is *"the
  worker's checkpoint collects them from the DAG while the engine is
  single-threaded and cannot yield, so the table set is frozen"*
  (`lsm/flush_barrier.rs:45`); it takes `*mut Table`.
- **Thread-local pools.** `batch_pool`, `DRAIN_BUFFER`, `BLOB_CACHE_POOL` are
  `thread_local!` — harmless under single-threaded access, wasteful or wrong if
  batches cross threads.
- **Process-global identity is uniform and correct.** `worker_rank`,
  `num_workers`, `is_worker`/`is_master` are read below runtime in six places
  (`catalog/store_lsn.rs:31,131`, `catalog/hooks.rs:721`,
  `catalog/index_backfill.rs:50`, `lsm/child_dir.rs:38`,
  `query/compiler/mod.rs:10`). Every mirror in a client process is rank 0 of 1
  with the worker role, so one process-wide setting is correct for all of them.
- **`COMMITTED_GENERATION` is not shared state for a mirror.**
  `RecoverySource::Rederive { resume_at }` takes the generation as an explicit
  parameter at every construction site; the global is read in exactly one place,
  the `rederive_checkpointed_now()` convenience (`lsm/table/mod.rs:100`), which
  the server's checkpoint flow uses. A mirror passes its own.
- **`inmem_ceiling()` needs a per-instance form.** `Table::new` reads the
  process-wide value once (`lsm/table/mod.rs:267`), but the per-table budget is
  already a real field — `set_inmem_ceiling_for_test` (`:517`) calls
  `RunSet::set_budget` behind a `#[cfg(test)]` gate. Several mirrors at 32 MiB
  each is a sizing problem whose mechanism exists and needs promoting out of the
  test gate.

### 10.2 Threadless async

The operative constraint is *no yield inside a store mutation*, not *one thread*.
The engine already satisfies it: `Worker::handle_push` is a plain **synchronous
`fn`** (`runtime/orchestration/worker/mod.rs:865`) calling
`evaluate_dag_multi_worker` synchronously (`:1248`) — async at the I/O edge, a
non-yielding synchronous region in the middle. The client takes the same shape,
with no threads:

1. **Async at the edge.** The host event loop (asyncio `add_reader`, tokio
   `AsyncFd`, or a bare `poll()`) drives the socket.
2. **Synchronous, atomic apply** once a whole tick's delta has arrived, in one
   non-yielding call. Tick-atomicity falls out rather than needing enforcement.
3. **Reads drain first.** A read non-blockingly applies whatever has already
   arrived, then reads — so the app never has to remember to pump, and the store
   is only ever touched from inside a library call.
4. **The feed is `await next_delta()`** — park on fd readiness, apply, return the
   delta. A sync app gets the blocking twin.

This requires a readiness-based framing API that does not exist: `gnitz-core`'s
`recv_framed` is unconditionally blocking, with no `MSG_DONTWAIT`, `O_NONBLOCK`
or `set_nonblocking` anywhere in the crate. That is why `gnitz-py`'s
`AsyncTransport` spawns an I/O thread (`gnitz-py/src/lib.rs:2221`) — the thread
exists only to turn blocking I/O into futures. Adding `fd()` + a `try_recv`
returning `WouldBlock` removes the need for a thread here, and lets the existing
async Python transport drop its own; `gnitz-py` is converted along with it.

---

## 11. Costs and limits

### 11.1 A local read is not a bare memory read

It costs one bulk region encode plus the existing client-side decode. That decode
is per-cell — `String::from_utf8` per string cell (`wal_block.rs:219`) and
`to_vec()` per fixed region (`:227`) — against the engine's one bulk copy per
region. Measured over a `(U64 pk, I64)` schema, release build, as ns per call:

| rows | `encode_to_wire` | `decode_wal_block` | total |
|---:|---:|---:|---:|
| 1 | 54 | 230 | 284 |
| 100 | 190 | 1,158 | 1,348 |
| 1,000 | 1,559 | 7,293 | 8,852 |
| 10,000 | 15,627 | 41,772 | 57,399 |
| 100,000 | 186,341 | 1,880,839 | 2,067,180 |

The decode is 4-15x the encode and dominates, which is the per-cell/bulk
asymmetry showing up as time. It also goes superlinear between 10k and 100k rows
— 4.2 ns/row to 18.8 — where the owned `ZSetBatch` outgrows cache.

**Neither number is overhead the mirror adds.** A remote read runs the same
encode on the worker and the identical decode on the client; the mirror removes
the transport between them and nothing else. So the honest comparison for a local
read is its own floor against the round trip it replaces: **284 ns for a point
read against 28 us** (§4.1), about 100x, and ~8.9 us for a thousand rows against
a realistic 80-220 us serving read. The saving is the syscalls, the two process
boundaries and the network, and it is largest exactly where reads are small and
frequent.

The superlinearity above 10k rows is a property of the existing client decode and
applies to remote reads equally, so it argues for streaming a large result rather
than against mirroring it.

### 11.2 Float aggregates diverge from the server

Float SUM/AVG is order-dependent — a function of (query, data, worker count,
access path, chunk size) — and the ad-hoc fold sums per-worker partials in reply
order. The mirror is one partition; the server is W. So `AVG(x)` over the copy
can differ in the low bits from the identical query at the same LSN on the
server. This is the same divergence the server already has between two worker
counts, and the existing contract already says to use an integer type where
exactness matters.

---

## 12. Two duplications the design leaves in place

The project states a drift-safety rule twice by name — client and engine "share
this encoder/decoder so they cannot drift" (`range.rs:7`, `read_spec.rs:8`).
Every wire **descriptor** obeys it. The wire **payload** — the Z-set itself —
does not:

| | client | engine |
|---|---|---|
| framing | `gnitz_wire::wal::{encode, validate_and_parse}` (`wal_block.rs:77,113`) | `gnitz_wire::wal` (`batch_wire.rs:21`) — **shared** |
| container | `ZSetBatch` / `ColData::Strings(Vec<Option<String>>)` | `Batch` — two `Vec<u8>` (data + blob), German strings in the arena |
| decode | per-cell `String::from_utf8`, per-region `to_vec()`, **and a per-row, per-PK-column OPK→LE walk** | one bulk copy per region, German strings verbatim, PK region kept as-is |

Two costs hold the containers apart. `gnitz-sql` is fully container-concrete —
147 `ZSetBatch` references, and `exec/{agg_finish,batch,order,residual}` use
`RowSource` **zero** times, so there is no generic seam to widen, only 147 sites
to rewrite. And a single container puts `gnitz-core` on the engine lib, which
carries 31,140 production lines (~108k raw) and a Linux->=5 floor into every
client including one that
only does remote reads (§4.3).

The PK asymmetry is the one that is easy to miss. `PkColumn` holds **native LE**
values and the region at rest is OPK, so `build_pk_region_into` is the client's
single encode point and `decode_wal_block` walks every row back, column by column,
through `decode_pk_column` — where the engine keeps the region verbatim, since
OPK order *is* its sort order. It is inside §11.1's measured decode already; it is
listed here because it is a third per-row cost, not just a container difference —
and it is off the mirror's *ingest* path, which decodes straight to `Batch` (§6.1)
and never builds a `PkColumn`.

§6.2 keeps the cost off this feature: local replies are wire blocks, so nothing
new straddles the two containers.

**Positional reply correlation** is the second. `request_id` exists on every
control block and is described as the "reactor reply-routing key", but the client
hardcodes it to `0` (`message.rs:131`) and matches replies by position. Tagging
them would collapse §9's dedicated subscription connection and its separate
receive loop into one multiplexed connection. What holds it in place is
per-connection reply ordering: it is what makes a pipelined `gnitz.aio` gather
resolve to the right futures, and what `recv_cached`'s ordering check enforces.
Changing it is a protocol-wide semantic change, wider than this feature.

---

## 13. Closed, blocking, open

### Closed

**How much smaller a purpose-built client store would be than §4.2's 5,869
lines.** It does not decide anything, so it is not worth settling. Three
size-independent locks already hold §4's decision, and no value of that number
touches any of them:

- §5.2's coupling is structural. `AdhocFold` sits on the reduce operator's
  aggregate core because a maintained view's aggregate and an ad-hoc one are one
  implementation *by design*, and `scan_spec` + `store_io` reach twenty distinct
  `CatalogEngine` members. A smaller store does not move the read path.
- §1's scale target forecloses the alternative outright: *"no decision here may
  foreclose the other."* A hand-written store gives bounds plus the client-side
  finishing that already exists in `gnitz-sql/exec/{residual,order,agg_finish}` —
  which means no pushdown, so a large mirror pays a whole-relation decode per
  query (§11.1: ~2.07 ms per 100k rows). That closes the large-relation corner.
- §4.3 already answers the one objection that is not about store size: the new
  crate keeps `gnitz-core` engine-free, so a remote-only client carries none of
  the 31k production lines §12 worries about.

§4.2's second argument says as much itself — *"whatever its size"*. The line count
is context for a decision, not an input to it.

### Blocking

Nothing. Both former entries are settled below; what remains is design work
inside §9.1, not a missing measurement.

**The accumulator byte cap: `min(the relation's own byte size, an absolute
per-subscriber ceiling)`.** The ceiling is a server-memory policy read off
`cap x subscribers`; the relation-size term is what the measurement settles.
Applying an accumulated delta costs one RAM-tier fold, and so does a reseed — a
reseed is the whole relation applied as a single delta. CPU therefore bounds the
cap from neither side, and the only quantity the cap trades is **server memory
against wire bytes**. Below the relation's size, shipping the accumulator is
strictly fewer bytes than a reseed; at or above it the accumulator ships as much
as a reseed would while still pinning the memory, so discarding is strictly
better. That is the break-even, and it is a size, not a tuning constant.

**Delta-ingest cost, measured** (`(U64 pk, I64)`, release, in-process; absolute
wall-clock on the measuring box varies 20-45% run to run, so only the ratios are
load-bearing):

| what varies | result |
|---|---|
| delta size, 1M rows total, same end state | 100-row deltas 13.8-20.0 us/row ingest; 100k-row deltas 6.3-8.5 ns/row — ~1,900x |
| total rows, fixed 100-row deltas | 10x the rows costs ~103x the time — quadratic in tick count |
| live set, fixed 1M rows and 10,000 ticks | 10k keys 218 ns/row, 50k 718, 200k 2,527 — linear in the live set |
| decode half | 2-7 ns/row above 1,000-row deltas, ~33 at 100-row deltas; 32 B/row on the wire |

The model is `ingest ~ ticks x resident rows`: a tick's cost is a fold over the
resident tier, near-independent of the delta's own row count. That is what §8
now records as the staleness window's real cost, and it is why the accumulator
earns its place on throughput grounds and not only on backpressure grounds — it
collapses N ticks of fold into one.

### Open

**The read:write break-even §11 asserts qualitatively.** With the ingest number
above in hand it becomes arithmetic against §11.1's read figures; without it the
memo can say only that the feature pays as a function of the read:write ratio.

**Whether §9.2's global `catalog_rwlock` survives the many-subscriber corner.**
§9.2 names it as the one place in the design where that corner could force a
change — either a per-relation lock for the validity check, or a validity token
cheap enough to test without the catalog. Nothing decides it yet; it is recorded
here so it is not rediscovered.
