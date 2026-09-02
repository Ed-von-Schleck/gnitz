# gnitz-store surface: byte-keyed seek, one error type, explicit worker slot and config, registry-owned index stores, thread contract

Five changes to the `gnitz-store` API that both consumers (`gnitz-server`, `gnitz-mirror`)
drive through. Each removes a fact that is currently derived in two places or reached
through a channel the store does not own. Every change is cold-path: nothing below
touches a merge comparator, a region layout, a kernel loop, or a per-row accessor.

Implementation order is the section order: §2 gives every later verb its error type,
§3 gives §4 its slot and budgets, §5 is documentation.

Stays as it is: `flush_barrier`'s `GNITZ_DISABLE_IO_URING` read (an I/O backend toggle
at the point of I/O, not a constructor), `schema::key::seek_opk_bytes` (see §1), and
every `String` or `&'static str` return on the pure derivations, decoders and plan
constructors (`schema::make_index_schema`, `decode_schema_block`,
`decode_mem_batch_from_wal_block`, `RangeProbe::new`, `ReducePlan::new`).

---

## 1. Byte-keyed seek: the store's seek takes OPK bytes

### Today

`RelationRegistry::seek_family(&self, table_id, seek_pk: u128, seek_pk_extra: &[u8], hydrator)`
takes the wire frame's split of a PK (`low` 16 native LE bytes plus a wide suffix),
decodes it through `schema::key::seek_opk_bytes`, and calls `seek_family_bytes`. The
master already decodes the same pair for routing in
`runtime/orchestration/master/dispatch.rs::fan_out_seek`. Every other store seek verb
takes `&[u8]`.

### Design

- `RelationRegistry::seek_family(u128, ..)` is deleted; `seek_family_bytes` is renamed `seek_family`, signature otherwise unchanged (its error type changes in §2):
  ```rust
  pub fn seek_family(&self, table_id: i64, pk: &[u8], hydrator: Option<&mut dyn SkeletonHydrator>) -> Result<Option<Batch>, String>
  ```
  The `read` rung then names `seek_opk_bytes` nowhere. `seek_opk_bytes` and its four tests stay in `schema::key`; it is the one native-image-to-OPK encoder for the wire pair, and the server is its only caller.
- `CatalogEngine::seek_family` in `catalog/view_state.rs` keeps its signature and does the decode the store did:
  ```rust
  pub(crate) fn seek_family(&mut self, table_id: i64, seek_pk: u128, seek_pk_extra: &[u8]) -> Result<(Option<Batch>, SchemaDescriptor), String> {
      let schema = self.registry.table_entry(table_id)?.schema;
      let opk = gnitz_store::schema::key::seek_opk_bytes(&schema, seek_pk, seek_pk_extra)
          .map_err(|e| format!("seek: table {table_id}: {e}"))?;
      let (dag, registry) = self.dag_and_registry_mut();
      Ok((registry.seek_family(table_id, opk.pk_bytes(), Some(dag))?, schema))
  }
  ```
  Its two production callers (the worker's `SalMessageKind::Seek` arm and `serve_seek`'s system-table arm; `fan_out_seek` decodes the pair for routing only and never seeks) and the eleven catalog-suite sites spelling `engine.seek_family(tid, N as u128, &[]).unwrap().0` are untouched.
- The three direct `seek_family_bytes` test sites rename to `seek_family`: `catalog/suites/engine_tests.rs` (the `CIRCUIT_NODES_TAB_ID` seek) and `catalog/suites/wide_pk_validation.rs` (two, in `wide_pk_seek_family_bytes_resolves_non_pk_col` and the native-versus-bytes comparison test, which keeps comparing the two paths).

---

## 2. `StoreError`: the one error type a store verb returns

### Today

Five error channels cross the crate boundary: `Result<_, String>` on the registry
verbs, the directory primitives, the read verbs and their private helpers,
`SkeletonHydrator::hydrate_keys`, `repartition_relation` and `SpillSort`;
`IngestError` on `ingest_returning_effective` alone; `gnitz_wire::WireFault` on
`scan_spec_family` alone, constructed inside the store in `open_delta_cursor` with
`STATUS_DELTA_EXPIRED`; `StorageError` on the `Table` verbs; `String` and
`&'static str` on the pure derivations and decoders.

The server branches on exactly two distinctions: `IngestError::Rejected` versus
`IngestError::Storage` in the worker's push-apply arm (reply versus fatal abort), and
`STATUS_DELTA_EXPIRED` versus `STATUS_ERROR` at the scan-spec reply. The mirror
branches on none and flattens everything to `MirrorError::Engine(String)`.

### Design

`StoreError` is defined in `storage/error.rs` beside `StorageError`, and `storage/mod.rs`
re-exports both (`pub use error::{StorageError, StoreError};`). Every function in the
sweep sits at or above the `storage` rung, so nothing moves.

```rust
// crates/gnitz-store/src/storage/error.rs

/// Why a store verb did not do what it was asked. The three variants differ in what
/// the caller may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// Nothing was applied and no state diverged: the message is the whole
    /// answer to whoever asked, whether the fault is the frame's, the caller's
    /// own state, or an internal one (a replay that fails, a walk that meets a
    /// skeleton row with no hydrator).
    Rejected(String),
    /// The store failed to read or write. Committed data may not have reached it,
    /// which leaves this process diverged from whatever durable log carried it.
    Storage { context: String, err: StorageError },
    /// A delta cursor below the round this worker has already dropped. The text is
    /// the refusal as the reader phrases it; the variant is what a wire reply
    /// branches on.
    DeltaExpired(String),
}

impl StoreError {
    pub fn rejected(message: impl Into<String>) -> Self {
        StoreError::Rejected(message.into())
    }
    pub fn storage(context: impl Into<String>, err: StorageError) -> Self {
        StoreError::Storage { context: context.into(), err }
    }
    /// Prefix a `Storage` context with what the caller was doing; the other
    /// variants are the callee's whole answer and pass through unchanged.
    pub fn in_context(self, what: &str) -> Self {
        match self {
            StoreError::Storage { context, err } => StoreError::Storage { context: format!("{what}: {context}"), err },
            other => other,
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Rejected(m) | StoreError::DeltaExpired(m) => f.write_str(m),
            StoreError::Storage { context, err } => write!(f, "{context}: {err}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// The server's and the mirror's error plumbing is string-typed; this is what keeps
/// `?` working at every one of their call sites.
impl From<StoreError> for String {
    fn from(e: StoreError) -> String {
        e.to_string()
    }
}
```

There is no `From<StorageError>` and no `From<String>`: a storage failure needs its
context, and a string needs its class. Internal sites spell `StoreError::rejected(..)`
and `StoreError::storage(.., e)`, including where a callee still returns `String`
(`make_index_schema` in `add_index`; `pk_range_keys` in `range_cursor`) or
`StorageError`. A caller that wraps a callee's `StoreError` with its own context
(`rebuild_relation_store`'s `"{what} tid={tid}"`) uses `in_context`.

`Rejected` and `DeltaExpired` carry today's message verbatim, so every text the E2E
suite asserts on is unchanged; `open_delta_cursor` lifts its existing `format!` into
`DeltaExpired`. `Storage` renders `{context}: {err}`, a new text at each of its sites;
all are boot-time or I/O failures, and no E2E test asserts on one.

`IngestError` is deleted. Its `Rejected(String)` is `StoreError::Rejected`; its
`Storage(e)` is `StoreError::Storage { context: "ingest into relation {id}", err }`.

`gnitz_wire::WireFault` is no longer named anywhere in `gnitz-store`. `gnitz_wire`
carries a blanket `impl<S: Into<String>> From<S> for WireFault` that maps to
`STATUS_ERROR`, so with `From<StoreError> for String` a bare `?` on a `StoreError`
inside any `WireFault`-returning function compiles and silently downgrades an expired
cursor to a generic error. The store error therefore never reaches one: the only
producer the wire sees, `CatalogEngine::scan_spec_family`, keeps returning
`Result<Batch, WireFault>` and maps at its one site:

```rust
// crates/gnitz-server/src/catalog/view_state.rs
registry.scan_spec_family(target_id, spec, reply_schema, cut_tick, Some(dag)).map_err(|e| match e {
    StoreError::DeltaExpired(text) => WireFault { status: gnitz_wire::STATUS_DELTA_EXPIRED, text },
    other => WireFault::from(other.to_string()),
})
```

`staged_dir` becomes generic over the error type; it never inspects the error, and
its server callers' closures return `String`:

```rust
pub fn staged_dir<T, E>(dir: &str, f: impl FnOnce() -> Result<T, E>) -> Result<T, E>
```

The `SkeletonHydrator` trait itself changes, and loses the `chunk_rows` parameter it
was passed beside the registry that owns the value:

```rust
// crates/gnitz-store/src/read/mod.rs
pub trait SkeletonHydrator {
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: i64, keys: Vec<u8>, coarse: &[i64])
        -> Result<Batch, StoreError>;
}
```

`materialize_hydrated` stops reading `scan_chunk_rows` for the call; the `DagEngine`
impl reads `registry.scan_chunk_rows()` at its one use, the `next_chunk` bound.

### Signature sweep

Every function below changes its error type to `StoreError`; the variant each site
produces is fixed here so no two implementers pick differently.

| Function | Variant |
|---|---|
| `relation::dirs::ensure_dir` | `Storage { context: "create directory '{path}'", err: e.into() }` |
| `relation::dirs::lock_data_dir` | open failure: `Storage { context: "open data-directory lock '{path}'", err: e.into() }`; a non-`EWOULDBLOCK` flock error: `Storage { context: "lock data directory '{base_dir}'", err: e.into() }`; the retry timeout: `Rejected("data directory '{base_dir}' is already held")` |
| `RelationRegistry::register`, `build_relation_store`, `build_delta_store` | too many columns for a feed: `Rejected`; `Table` open failure: `Storage { context: "open relation {id} (dir={directory})" }` / `"open delta store of view {id} (dir={directory})"` |
| `RelationRegistry::swap_table_schema` | a descriptor that is not a trailing append: `Rejected` (today's text); `Table::swap_schema` failure: `Storage { context: "ALTER on table {id}: reopening shards" }` |
| `RelationRegistry::table_entry`, `validate_index_cols`, `TableEntry::delta_feed_or_err` | `Rejected`, same texts (`"relation {id} is not registered"`) |
| `RelationRegistry::rehome_stores`, `rebuild_relation_store`, `reset_store` | unknown id: `Rejected`; a `build_relation_store` error: passed through `in_context("{what} tid={tid}")` |
| `RelationRegistry::ingest_returning_effective` | `Rejected(msg)` where it was `IngestError::Rejected`; `Storage { context: "ingest into relation {id}" }` where it was `IngestError::Storage` |
| `RelationRegistry::flush_ephemeral_outputs` | `Storage { context: "ephemeral output flush" }` |
| `RelationRegistry::{scan_family, seek_family, gather_family_bytes, seek_by_index, seek_by_index_range, open_index_source, scan_spec_family}` and every private helper in `read/store_io.rs` and `read/scan_spec.rs` that returns `String` or `WireFault` — `materialize_hydrated`; `scan_spec_source_schema`, `open_scan_spec_cursor`, `open_delta_cursor`, `open_index_bound_cursor`, `range_cursor`, `pk_set_opk_keys`, `resolve_rows_projection`, `compile_fold_pre_map`, `run_scan_fold_sink`, `resolve_order_locs`, `compile_predicate`, `compile_projection` — plus `IndexScan::Decline`, whose payload becomes `StoreError` so `open_index_range`'s `table_entry` error passes through unmapped | `Rejected` for every `format!` / `.into()` site, and `.map_err(StoreError::rejected)` on every `?` over a callee that stays on `String` — `pk_range_keys` in `range_cursor`, and `IndexKeySpec::range_keys` in `open_index_range` (`Decline(e)` becomes `Decline(StoreError::rejected(e))`); `DeltaExpired` at the one site in `open_delta_cursor`; `Storage` is never produced here (no read verb writes) |
| `read::SkeletonHydrator::hydrate_keys` | `Rejected` (the `DagEngine` impl's `format!` sites) |
| `storage::SpillSort::{push, finish}` and its private `ensure_spill_fd` / `spill_run` | every failure is an `io::Error` from `posix_io`: `Storage { context: "external sort: <today's step text>", err: e.into() }` — the three step texts (cannot create spill file in {dir} / spill write failed / mmap spill file failed) stay |
| `storage::repartition_relation` and its private `classify` | `classify` returns `StoreError` itself: the unknown-grammar refusal `Rejected` (today's text); its `cstr` and `manifest::read_file` failures `Storage { context: "repartition {rel_dir}: manifest of w{rank}of{of}" }`. In `repartition_relation` the `Layout::Unplaceable` refusal: `Rejected` (today's text); the rewrite/link and `fsync_dir` arms: `Storage { context: "repartition {rel_dir} to {launched} worker(s)" }` |
| `ops::reduce::AdhocFold::{new, fold_ranges}` (crate-private) | `Rejected` (the group-cap and shape messages) |

### Server call sites

- Worker push-apply (`runtime/orchestration/worker/mod.rs`, the `match self.cat().registry_mut().ingest_returning_effective(..)`): the arms become `Err(StoreError::Rejected(msg) | StoreError::DeltaExpired(msg)) => return Err(msg)` (an ingest never produces the second, and the or-pattern is what keeps the match total without a third arm) and `Err(e @ StoreError::Storage { .. }) => gnitz_fatal_abort!(..)` with the same text. The `IngestError` import becomes `gnitz_store::storage::StoreError`.
- `catalog/mod.rs`'s `use gnitz_store::storage::{..}` gains `StoreError`, which `view_state.rs` reaches through its `use super::*;` for the variant pattern.
- `catalog/view_state.rs`: `scan_spec_family` maps as shown above; `scan_family` and `open_source_cursor` tail-return a store result into a `Result<_, String>`, which is not a `?` and does not convert, so each ends in `.map_err(String::from)`.
- `query/dag/mod.rs` `DagEngine::swap_table_schema`: the same tail-return, the same `.map_err(String::from)`.
- `runtime/orchestration/executor.rs` `handle_seek_by_index`: binds `validate_index_cols`'s error as `msg` and sends `msg.as_bytes()`; it becomes `msg.to_string().as_bytes()`.
- `query/dag/hydrate.rs` `impl SkeletonHydrator for DagEngine`: returns `Result<Batch, StoreError>`. Every `String` the body produces is wrapped in `StoreError::rejected`: its three `return Err(format!(..))` sites (unregistered view, not compiled as bounded, not a single-phase plan), its two `.map_err(|..| format!(..))?` sites (the unregistered seed source, the failed replay chunk), and the bare `self.ensure_compiled(registry, view_id)?`, which becomes `.map_err(StoreError::rejected)?`.
- Every other server site is a `?` into a `Result<_, String>` and compiles unchanged through `From<StoreError> for String`.

### Tests

- `relation/tests/relation.rs`: the `Err(IngestError::Storage(StorageError::Io(_)))` pattern becomes `Err(StoreError::Storage { err: StorageError::Io(_), .. })`.
- `read/tests/scan_spec.rs`: the `run` helper returns `Result<Batch, StoreError>`, its `WireFault` import becomes `crate::storage::StoreError`, and its three `err.text.contains(..)` assertions become `err.to_string().contains(..)`.
- `catalog/suites/scan_spec_tests.rs`: unchanged; it reads `err.text` off the `WireFault` the catalog wrapper still returns.

### Mirror call sites

`gnitz-core` does not depend on `gnitz-store`, so `From<StoreError> for MirrorError`
cannot be written. One helper at module level in `crates/gnitz-mirror/src/handle.rs`:

```rust
pub(crate) fn engine(e: StoreError) -> MirrorError {
    MirrorError::Engine(e.to_string())
}
```

Sites: in `Mirror::open`, `ensure_dir(base_dir)?` and `lock_data_dir(base_dir)?` become
`.map_err(engine)?`; `Mirror::enter`'s `self.registry.register(..)?` likewise;
`checkpoint_inner`'s `flush_ephemeral_outputs(generation)?` likewise; `reads.rs`
`scan_inner`'s `scan_family(.., None)?` likewise, and `scan_spec_inner`'s
`.map_err(|f| MirrorError::Engine(f.text))` becomes `.map_err(engine)`. `apply.rs`'s
ingest site and `invalidate_inner`'s `reset_store` site already format through
`Display` inside `self.poison(format!(..))` and are unchanged. The mirror's poison
policy is unchanged: it poisons exactly where it poisons today.

---

## 3. `Slot`, `StoreConfig`, `RamBudgets`: no ambient identity, no constructor reads the environment

### Today

`foundation/worker_ctx.rs` holds three process-global atomics: rank, count, role. The
store reads the rank in `ChildAddr::this_worker` and `ChildAddr::delta_for_this_worker`,
in `ingest_view_delta` (a replicated view's feed is captured on rank 0 only) and in
`reset_store` (this rank's scratch children), and the role in `assert_pre_fork`. The
registry already threads the worker *count* explicitly because the ambient value is
wrong pre-fork; the rank has the same hazard and is still ambient. The server sets the
atomics in `runtime/bootstrap.rs`; the compiler reads them at `query/compiler/emit.rs`
(the scratch child's rank, the `WorkerFilter` emission, the global-aggregate owner
test). After §4 the role has one reader left, the catalog's index hook. The mirror
asserts `is_standalone()` against a role it never set.

`RelationRegistry::new` reads `GNITZ_SCAN_CHUNK_ROWS`; `Table::new` reads
`GNITZ_RAM_TIER_BYTES` through a process-wide latch and hard-codes the memtable
budget, with `Table::with_memtable_budget` as a second constructor for tests;
`adhoc_group_cap()` reads `GNITZ_ADHOC_GROUP_CAP` through a second latch. A mirror in a
client process therefore honours three server tuning variables.

### Design

Three plain values, passed once.

```rust
// crates/gnitz-store/src/storage/lsm/child_dir.rs

/// Which worker this process is, of how many: the one input that decides which
/// `w{k}of{n}` child every store of this process opens.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Slot {
    pub rank: u32,
    pub of: u32,
}

impl Slot {
    /// A one-worker process: the mirror, and every unit test.
    pub const SOLO: Slot = Slot { rank: 0, of: 1 };

    /// Panics on `rank >= of` or `of == 0`: a slot outside its own layout names a
    /// child the boot sweep deletes as unowned.
    pub fn new(rank: u32, of: u32) -> Slot {
        assert!(of >= 1 && rank < of, "slot {rank} of {of}");
        Slot { rank, of }
    }
}

impl<'a> ChildAddr<'a> {
    pub fn worker(slot: Slot) -> Self {
        ChildAddr::Worker { rank: slot.rank, of: slot.of }
    }
    pub(crate) fn delta(slot: Slot) -> Self {
        ChildAddr::Delta { rank: slot.rank }
    }
}
```

`ChildAddr::this_worker` and `ChildAddr::delta_for_this_worker` are deleted.
`storage/mod.rs` re-exports `Slot` beside `ChildAddr` and `RamBudgets` beside
`Table`; both submodules stay private. `Slot` is not `ChildAddr::Worker` reused: that
variant names one child of one relation and its enum carries the `Scratch` arm's
lifetime, where the slot is the process identity every child address (worker, delta,
scratch, index child) is derived from.

```rust
// crates/gnitz-store/src/storage/lsm/table/mod.rs

/// The two RAM budgets of one `Table`. `Default` is what `Table::new` opens with;
/// the numbers and their measurements are documented on the impl.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RamBudgets {
    /// Ingest runs fold into the RAM tier once they pass this.
    pub memtable_bytes: usize,
    /// The RAM tier spills to a shard past this.
    pub ram_tier_bytes: usize,
}

impl Default for RamBudgets {
    fn default() -> Self {
        RamBudgets { memtable_bytes: 192 << 10, ram_tier_bytes: 32 << 20 }
    }
}

impl Table {
    /// [`Self::with_budgets`] at [`RamBudgets::default`].
    pub fn new(dir: &str, schema: SchemaDescriptor, table_id: u32, recovery_source: RecoverySource) -> Result<Self, StorageError>

    pub fn with_budgets(
        dir: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        recovery_source: RecoverySource,
        ram: RamBudgets,
    ) -> Result<Self, StorageError>
}
```

`Table::new` keeps its four parameters and every caller that opens at the defaults is
untouched. The doc comments now on `INMEM_CEILING` and `MEMTABLE_BUDGET` move onto
`RamBudgets::default`. `INMEM_CEILING`, `MEMTABLE_BUDGET`, `inmem_ceiling()` and
`Table::with_memtable_budget` are deleted; the four `with_memtable_budget` callers
(`storage/lsm/tests/table.rs`'s opening helper and one direct call,
`storage/suites/data_roundtrip_proptest.rs`, `ops/reduce/bench_secondary_index.rs`,
`test_support/shared.rs::scratch_table`) become `with_budgets(.., RamBudgets { memtable_bytes: <that>, ..Default::default() })`.
`storage/lsm/table/bench_flush.rs` measures RAM-tier fold and compaction behaviour and
today picks the override up through the latch: its four `Table::new` calls open through
`with_budgets` with one file-local `RamBudgets` built from
`env_num("GNITZ_RAM_TIER_BYTES", RamBudgets::default().ram_tier_bytes)`.

```rust
// crates/gnitz-store/src/relation/mod.rs

/// Everything a registry is tuned by. `Default` is the production value of every
/// field; the server overrides fields from its environment before constructing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StoreConfig {
    pub ram: RamBudgets,
    /// Rows per `drain_chunk` on every chunked scan this process drives; bounds
    /// peak scan memory at O(chunk × row_width). Never zero.
    pub scan_chunk_rows: usize,
    /// Per-worker distinct-group cap for the ad-hoc aggregate fold; bounds the
    /// accumulator matrix at `cap × aggregates × size_of::<Accumulator>()`.
    pub adhoc_group_cap: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig { ram: RamBudgets::default(), scan_chunk_rows: 65_536, adhoc_group_cap: 65_536 }
    }
}

pub struct RelationRegistry {
    pub(crate) tables: FxHashMap<i64, TableEntry>,
    pub(crate) slot: Slot,
    pub(crate) config: StoreConfig,
    pub(crate) owns_stores: bool,
    /// Set by `rehome`, never cleared. "Pre-fork" is `owns_stores && !rehomed`: the
    /// post-fork master detached, every worker rehomed, and only the pre-fork master
    /// and a standalone process have done neither.
    pub(crate) rehomed: bool,
    pub(crate) resume_generation: u64,
    pub(crate) recorded_topology: u64,
}

impl RelationRegistry {
    pub fn new(slot: Slot, config: StoreConfig) -> Self   // config.scan_chunk_rows clamped to >= 1
    pub fn slot(&self) -> Slot
    pub fn config(&self) -> StoreConfig
    /// `slot().of`; kept because the server reads the count, never the rank, at over a dozen sites.
    pub fn num_workers(&self) -> u32
    pub fn scan_chunk_rows(&self) -> usize             // config.scan_chunk_rows
    pub fn set_scan_chunk_rows(&mut self, rows: usize) // clamps to >= 1, as today
}
```

The field-doc on today's `scan_chunk_rows` field moves onto `StoreConfig::scan_chunk_rows`.
The `SCAN_CHUNK_ROWS` constant and `adhoc_fold.rs`'s `ADHOC_GROUP_CAP` constant and
`adhoc_group_cap()` latch are deleted, with the latch's two re-exports in
`ops/mod.rs` and `ops/reduce/mod.rs`; the values live in `StoreConfig::default`. The
one server test naming `SCAN_CHUNK_ROWS` (`catalog/suites/reopen_rebuild_tests.rs`)
reads `StoreConfig::default().scan_chunk_rows`.

Every ambient or latched read in the store becomes a field read, and every bare read
of the two replaced fields follows the rename:

- `relation/build.rs` `build_relation_store`: `ChildAddr::worker(self.slot)`, `Table::with_budgets(.., self.config.ram)`. `build_delta_store` is an associated function with no receiver; it gains `slot: Slot, ram: RamBudgets` parameters, uses `ChildAddr::delta(slot)` and `Table::with_budgets(.., ram)`, and its one call site passes `self.slot, self.config.ram`.
- `relation/ingest.rs` `ingest_view_delta`: `self.slot.rank == 0`, read into a local before the `get_mut`.
- `relation/store_lsn.rs` `reset_store`: `self.slot.rank`, `ChildAddr::worker(self.slot).manifest(&dir)`; `reconcile_child_dirs`: `reclaim_retired_children(&entry.directory, self.slot.of)`.
- `relation/store_lsn.rs` `assert_pre_fork`: keeps its name and asserts `self.owns_stores && !self.rehomed`. It guards `reconcile_child_dirs` and the catalog's `compute_invalid_views` exactly as today, for every caller, without the store knowing a role.
- `relation/mod.rs` `launched_topology_word`: `topology_word(self.slot.of)`.
- `read/scan_spec.rs`: the `ScanSinkCtx` chunk bound and the `AdhocFold::new` cap read `self.config.scan_chunk_rows` and `self.config.adhoc_group_cap`; `read/store_io.rs`: the two `self.scan_chunk_rows` reads (`materialize_hydrated`, the index-bounded cursor's `matches.min(..)`) read `self.config.scan_chunk_rows`.
- `storage/lsm/repartition.rs`: its internal `Table::new` is untouched. Repartition is a boot-time relayout whose source stores are opened, read and closed inside the call; a budget only shapes ingest, which that walk never does.

`rehome_stores` becomes `rehome`, the one writer of the slot after construction, and
at this section is otherwise today's function (§4 extends it to index circuits):

```rust
/// Become `slot`: record it, then re-open every store this process holds that is
/// not homed at `slot`'s child. The worker calls it once, post-fork, before any
/// other registry verb. A handle already homed there is left alone — re-opening
/// one would briefly put two live `Table`s on one directory — as is every system
/// family: single-partition, flat under `_system_catalog`, never at a child.
pub fn rehome(&mut self, slot: Slot) -> Result<(), StoreError> {
    self.slot = slot;
    self.rehomed = true;
    let home = ChildAddr::worker(slot);
    // today's body from here on
}
```

`foundation/worker_ctx.rs`, its `pub mod` line, its bullet in `foundation/mod.rs`'s leaf
list and its entry in `lib.rs`'s list of public submodules are deleted. The one
remaining reader of the role, the catalog's index hook, reads a `CatalogEngine` field
instead of a process-global:

```rust
// crates/gnitz-server/src/catalog/mod.rs
pub(crate) struct CatalogEngine {
    // ...
    /// True in the master process, pre- and post-fork: the one process whose index
    /// copies stay permanently empty, so its index hook skips the backfill the
    /// forked workers run slice-local. `open` leaves it false (every unit test);
    /// `run_server` opens through `open_master`; the forked child clears it in
    /// `become_worker` before any catalog work.
    pub(in crate::catalog) is_master: bool,
}
```

`CatalogEngine::open(base_dir, num_workers)` keeps its signature and sets it false;
`open_master(base_dir, num_workers)` is the same body with it true; `become_worker(&mut self)`
sets it false. `runtime/bootstrap.rs` `run_server` calls `open_master` where it calls
`open` today and drops `set_master_role`; the worker child calls
`catalog.become_worker()` where it calls `set_worker_role` today, and drops
`set_worker_identity`. The engine is inherited through the fork as a raw pointer, so
the child's clear is what makes a live `CREATE INDEX` broadcast to a worker backfill.

### Server call sites

- `catalog/bootstrap.rs` `CatalogEngine::open` / `open_master`: before the system-table loop:
  ```rust
  let defaults = StoreConfig::default();
  let config = StoreConfig {
      ram: RamBudgets {
          ram_tier_bytes: env_num("GNITZ_RAM_TIER_BYTES", defaults.ram.ram_tier_bytes),
          ..defaults.ram
      },
      scan_chunk_rows: env_num("GNITZ_SCAN_CHUNK_ROWS", defaults.scan_chunk_rows),
      adhoc_group_cap: env_num("GNITZ_ADHOC_GROUP_CAP", defaults.adhoc_group_cap),
  };
  ```
  `env_num` refuses a zero and an unparseable value, which is the rule the registry's field doc states today. The system-table loop opens through `Table::with_budgets(.., config.ram)`; the registry is built as `RelationRegistry::new(Slot::new(0, num_workers), config)`. `self.registry.config()` answers later readers. `catalog/mod.rs`'s `use gnitz_store::storage::{..}` gains `Slot, RamBudgets` and its `use gnitz_store::relation::{..}` gains `StoreConfig`, which `bootstrap.rs` reaches through `use super::*;`.
- `runtime/bootstrap.rs` worker child: `set_worker_identity` and `set_worker_role` go; `catalog.become_worker()` is called right after the child takes `catalog` off the raw pointer, ahead of the `rehome` call. The `rehome_stores()` call becomes `rehome(Slot::new(w as u32, num_workers))`. Nothing between the fork and those two calls reads the slot or the role (log init, CPU pinning, fd closes, `SalReader::new`).
- `runtime/bootstrap.rs` `run_server`: `set_master_role` goes; `CatalogEngine::open_master`.
- `catalog/hooks.rs` index hook: `self.is_master`.
- `query/compiler/mod.rs` `ViewSite` gains two fields, filled where it is built:
  ```rust
  pub(super) struct ViewSite<'a> {
      pub(in crate::query) dir: &'a str,
      pub(in crate::query) id: u64,
      pub(in crate::query) recovery: RecoverySource,
      pub(in crate::query) slot: Slot,
      pub(in crate::query) ram: RamBudgets,
  }
  ```
  `query/dag/mod.rs` `compile_circuit` fills `slot: registry.slot(), ram: registry.config().ram`; the one test construction in `query/compiler/tests/emit.rs` fills `Slot::SOLO, RamBudgets::default()`. `emit.rs` `create_child_table` reads `rank: self.site.slot.rank` and opens through `Table::with_budgets(.., self.site.ram)`; the `WorkerFilter` emission and the global-aggregate owner test read `ctx.site.slot` once into a local. The `worker_ctx` import in `compiler/mod.rs` goes. `SchemaSource` is untouched.

  The master's one compile, the CREATE VIEW pre-flight, now sees `(0, W)` where the atomics gave it `(0, 1)`. The verdict is unchanged: the plan is dropped, `WorkerFilter` emission has no rejection path, and the owner test feeds only `seeds_ground`. `preflight_compile`'s doc in `query/dag/mod.rs` is rewritten to say the pre-flight compiles under the launched slot and that no rejection reads it.
- `catalog/index_backfill.rs`'s role read disappears with §4.
- `RelationRegistry::new(1)` in `relation/tests/relation.rs` ×5, `read/tests/scan_spec.rs` ×2, `query/dag/tests/dag.rs` ×3 becomes `RelationRegistry::new(Slot::SOLO, StoreConfig::default())`.

### Mirror call sites

- `handle.rs` `Mirror::open`: the `worker_ctx` import and the `is_standalone` check are deleted, and the doc line "Fails if the process has taken a server role" with them. There is no ambient state left for a mirror to be wrong about: it homes at `w0of1` whatever process it runs in, under a directory it locks itself.
- `Mirror::open` builds its config beside the checkpoint knob it already reads there:
  ```rust
  let config = StoreConfig {
      ram: RamBudgets {
          ram_tier_bytes: env_num("GNITZ_MIRROR_RAM_TIER_BYTES", RamBudgets::default().ram_tier_bytes),
          ..Default::default()
      },
      ..Default::default()
  };
  let mut registry = RelationRegistry::new(Slot::SOLO, config);
  ```
  The mirror stops honouring `GNITZ_RAM_TIER_BYTES`, `GNITZ_SCAN_CHUNK_ROWS` and `GNITZ_ADHOC_GROUP_CAP`; its own knobs are `GNITZ_MIRROR_CHECKPOINT_BYTES` and `GNITZ_MIRROR_RAM_TIER_BYTES`, read by the host crate's entry point, not by a store constructor.
- `tests/mirror.rs` `resident_footprint_bench`: the child-environment tuple becomes `("GNITZ_MIRROR_RAM_TIER_BYTES", "262144")`.
- `tests/mirror.rs`: `RelationRegistry::new(2).launched_topology_word()` becomes `gnitz_store::storage::topology_word(2)`; `ChildAddr::this_worker(1)` becomes `ChildAddr::worker(Slot::SOLO)`.

---

## 4. `RelationRegistry::add_index`: the registry opens, homes and rehomes index stores

### Today

Five server sites derive an index's schema, and three of them also its key spec:
`catalog/hooks.rs` (the index hook), `catalog/index_backfill.rs` (`backfill_index`,
`promote_index_to_unique`), `catalog/precheck.rs`, `master/preflight.rs` and
`worker/mod.rs` (the unique pre-flight). `new_index_table` opens the index `Table` in
the server, choosing the home off the ambient role (worker: `idx_dir/w{rank}of{n}`;
master and standalone: `idx_dir` itself), and `add_index_circuit` computes the key spec
a second time. `backfill_all_indexes` re-opens every index table at worker boot through
`replace_index_table`, which returns `Option<*mut Table>`, and the projection loop
ingests through `unsafe { &mut *table }`. The registry already models a master holding
an index circuit with no store (`StoreHandle::Detached`, opening empty).

### Design

An index store follows the relation-store rule at every step: opened at the slot's
child by the registry, inherited through the fork, left alone by `rehome` when already
homed and re-opened otherwise, and backfilled at boot only when its open resumed
nothing and it holds nothing.

```rust
// crates/gnitz-store/src/relation/mod.rs

impl RelationRegistry {
    /// Enter a secondary index on `owner` over `cols` and open this process's
    /// store for it under `<owner dir>/idx_<index_id>/w{rank}of{n}`. Creates the
    /// index directory on every process, so the post-fork master — which holds no
    /// store and registers `Detached` — still owns the path a later DROP removes.
    /// Whether the open resumed is read off the entered circuit
    /// (`IndexCircuitEntry::resumed_from_checkpoint`), the one place that fact lives.
    pub fn add_index(&mut self, owner: i64, index_id: i64, cols: &[u32], is_unique: bool) -> Result<(), StoreError> {
        let (owner_schema, owner_dir) = {
            let e = self.table_entry(owner)?;
            if e.index_circuit_on(cols).is_some() {
                return Err(StoreError::rejected(format!("table {owner} already carries an index on {cols:?}")));
            }
            (e.schema, e.directory.clone())
        };
        let index_schema = crate::schema::make_index_schema(cols, &owner_schema).map_err(StoreError::rejected)?;
        let idx_dir = ChildAddr::Index { id: index_id }.dir(&owner_dir);
        ensure_dir(&idx_dir)?;
        let handle = match self.owns_stores {
            false => StoreHandle::Detached,
            true => StoreHandle::owned(Self::open_index_table(
                self.slot,
                self.rederive_source(),
                self.config.ram,
                &idx_dir,
                index_id,
                index_schema,
            )?),
        };
        let key_spec = crate::schema::IndexKeySpec::new(cols, &owner_schema, &index_schema);
        self.tables.get_mut(&owner).expect("resolved above").index_circuits.push(IndexCircuitEntry {
            col_indices: PkColList::from_slice(cols),
            index_id,
            handle,
            index_schema,
            key_spec,
            is_unique,
        });
        Ok(())
    }

    /// This process's store of one index: at `slot`'s child of `idx_dir`, under
    /// the given rederive policy and RAM budgets. An associated function so a
    /// caller holding `&mut` into `tables` can open without a second borrow of
    /// `self`.
    fn open_index_table(
        slot: Slot,
        recovery: RecoverySource,
        ram: RamBudgets,
        idx_dir: &str,
        index_id: i64,
        schema: SchemaDescriptor,
    ) -> Result<Box<Table>, StoreError> {
        Table::with_budgets(&ChildAddr::worker(slot).dir(idx_dir), schema, index_id as u32, recovery, ram)
            .map(Box::new)
            .map_err(|e| StoreError::storage(format!("open index {index_id} (dir={idx_dir})"), e))
    }
}

impl IndexCircuitEntry {
    /// Whether this process's store of the index came back from a checkpoint
    /// manifest at its open — the open this process made, or the one it inherited
    /// through the fork; `false` for a detached circuit.
    pub fn resumed_from_checkpoint(&self) -> bool {
        self.handle.as_owned().is_some_and(Table::resumed_from_checkpoint)
    }

    /// Rows this process's store estimates it holds; `0` for a detached circuit.
    pub fn estimated_rows(&self) -> usize {
        self.handle.as_owned().map_or(0, Table::estimated_rows)
    }
}
```

`rehome` (§3) gains its index half, after the relation loop and before `Ok(())`:

```rust
    let (recovery, ram) = (self.rederive_source(), self.config.ram);
    for entry in self.tables.values_mut() {
        if entry.kind == RelationKind::SystemCatalog {
            continue;
        }
        let owner_dir = entry.directory.clone();
        for ic in &mut entry.index_circuits {
            let idx_dir = ChildAddr::Index { id: ic.index_id }.dir(&owner_dir);
            if ic.handle.as_owned().is_none_or(|t| t.directory() == home.dir(&idx_dir)) {
                continue;
            }
            ic.handle = StoreHandle::owned(Self::open_index_table(slot, recovery, ram, &idx_dir, ic.index_id, ic.index_schema)?);
        }
    }
```

The loop holds `&mut self.tables` only; `open_index_table` is associated, and `home`,
`slot`, `recovery` and `ram` are locals. `Option::is_none_or` is stable since Rust
1.82; the workspace builds on the Fedora system toolchain, well past it. `add_index_circuit` and
`replace_index_table` are deleted. `remove_index_circuit` and
`set_index_circuit_uniqueness` stay.

The home is one rule for every process: `idx_dir/w{rank}of{n}`. The pre-fork master
opens `w0of{W}` during `replay_catalog`; worker 0 inherits that open and keeps it,
exactly as it keeps its inherited relation stores; every other rank re-opens at its own
child; the post-fork master registers `Detached`. Worker 0's inherited open answers
`resumed_from_checkpoint` as a fresh open at its rank would: `resume_generation` and
`recorded_topology` are both set before `replay_catalog`, and the only later writer,
the boot checkpoint's `record_topology`, moves the verdict from mismatch to match after
a mismatched open has already erased the child.

### Server: `catalog/index_backfill.rs`

`new_index_table` and `IndexProjectionTarget` are deleted. Whether a pass ingests is a
caller policy like `check_dups`, and every target of one call shares it:

```rust
fn stream_index_projection(&mut self, owner_id: i64, targets: &[PkColList], check_dups: bool, ingest: bool) -> Result<(), String>
```

Per chunk and per target the circuit is resolved by column list:

```rust
let ic = self
    .registry
    .index_circuit_for_cols(owner_id, cols.as_slice())
    .ok_or_else(|| format!("index circuit on {:?} of {owner_id} vanished", cols.as_slice()))?;
let projected = gnitz_store::storage::batch_project_index(&chunk, &ic.key_spec, &ic.index_schema);
if projected.is_empty() { continue; }
if check_dups && projected_chunk_has_dup_keys(&projected, ic.key_spec.key_size(), &mut seen[ti]) {
    return Err(self.unique_create_dup_err(owner_id, cols.as_slice()));
}
if ingest {
    ic.ingest_owned_batch(projected)
        .map_err(|e| format!("index backfill: ingest failed (owner {owner_id}): {e}"))?;
}
```

The cursor `handle` owns its runs, and the `ic` borrow is dead on the path that reaches
the `&mut self` call to `unique_create_dup_err`, so the loop compiles under NLL with no
`*mut`.

- `backfill_index(&mut self, owner_id, cols: &[u32], check_dups)`: the emptiness guard reads `index_circuit_for_cols(owner_id, cols).ok_or_else(|| format!("backfill_index: no circuit on {cols:?} of {owner_id}"))?.estimated_rows() != 0` (the memtable counts — `estimated_rows` sums both RAM tiers and the shards — so a store the hook just filled reads non-empty with no flush); then `stream_index_projection(owner_id, &[PkColList::from_slice(cols)], check_dups, true)`.
- `backfill_all_indexes`: no re-open (`rehome` did it). The worklist is every `(owner_id, Vec<PkColList>)` whose circuits report `!resumed_from_checkpoint() && estimated_rows() == 0`, snapshotted as today; each owner's targets are streamed with `stream_index_projection(owner_id, &targets, false, true)`, and `rebuilt` counts targets as today. A store that resumed holds its rows; a fresh non-resuming open is empty, because both non-resuming `Rederive` arms erase; a non-resumed store that holds rows was backfilled earlier in this process, which is what makes a second call a no-op rather than an addition (`catalog/suites/reopen_rebuild_tests.rs::backfill_all_indexes_rebuilds_exactly_once` asserts exactly that, on weights and seeks, never on paths).
- `promote_index_to_unique(&mut self, owner_id, cols)`: the `owner_schema` parameter goes (one caller, the index hook); `stream_index_projection(owner_id, &[PkColList::from_slice(cols)], true, false)`. Its `make_index_schema` and `IndexKeySpec::new` go.

### Server: `catalog/hooks.rs`, the index hook

```rust
let idx_dir = index_dir(&owner_dir, idx_id);
let cols = *cols;
staged_dir(&idx_dir, || {
    self.registry.add_index(owner_id, idx_id, cols.as_slice(), is_unique)?;
    let resumed = self
        .registry
        .index_circuit_for_cols(owner_id, cols.as_slice())
        .is_some_and(IndexCircuitEntry::resumed_from_checkpoint);
    // The master never populates its index copies; workers and standalone
    // backfill from their local base slice unless the store resumed from a
    // checkpointed manifest, which already holds those rows.
    if !self.ctx.in_rollback() && !self.is_master && !resumed {
        let check = is_unique && self.ctx.mode() == ApplyMode::Live;
        if let Err(e) = self.backfill_index(owner_id, cols.as_slice(), check) {
            // The circuit was entered before the backfill so the projection
            // could ingest through it; a failed CREATE INDEX leaves no circuit.
            self.registry.remove_index_circuit(owner_id, cols.as_slice());
            return Err(e);
        }
    }
    Ok(())
})
```

The hook's own `make_index_schema` call goes, and with it and the
`promote_index_to_unique` parameter the hook's `owner_schema` local, which nothing
else reads. `index_dir` (`catalog/utils.rs`) stays: it is `ChildAddr::Index { id }.dir(..)`
by definition, so the stage's path and the path `add_index` builds are one grammar,
and its other two callers (the DROP retraction, the orphan sweep) are untouched. `catalog/mod.rs`'s `use gnitz_store::relation::{..}` gains `IndexCircuitEntry` for the
`is_some_and` path above. `staged_dir` still removes `idx_dir` on `Err`, after
`remove_index_circuit`'s `retain` has dropped the `Table` and its fds.

The remaining derivation sites stay, each for a reason the registry cannot serve:
`catalog/precheck.rs` validates the column list at the catalog trust boundary before
any registration exists; `master/preflight.rs` and `worker/mod.rs` run the CREATE
UNIQUE INDEX pre-flight, which sorts the keys of an index that does not exist yet.

### Tests

- `relation/tests/relation.rs` (two `add_index_circuit` sites): `add_index(50, 999, &[2], false).unwrap()` / `add_index(70, 999, &[1], false).unwrap()`; the hand-opened index `Table` goes; rows are ingested through `registry.index_circuit_for_cols(70, &[1]).unwrap().ingest_owned_batch(batch)`. `register_entry` gains a `directory: String` parameter: the two index tests pass a fresh `relation_test_dir("<name>_owner")` (a sibling of the owner `Table`'s own dir, never inside it), since `add_index` creates `idx_<id>` under it; every other caller passes `String::new()` as today. `test_flush_includes_index_circuits` asserted shards after `registry.flush(70)` on an index it had opened `SalReplay`; `add_index` opens `Rederive`, and a base round only folds a rederived store to RAM. It becomes `ephemeral_flush_includes_index_circuits`: after the ingest it calls `registry.flush_ephemeral_outputs(1).unwrap()` — the round that force-persists every rederived store, index circuits included, through `collect_base_flush_tables` — and counts `shard_` files under `ChildAddr::Index { id: 999 }.dir(&owner_dir)` joined with `ChildAddr::worker(Slot::SOLO)`'s name.
- `catalog/suites/index_tests.rs` (`test_seek_by_index_range_wide_pk_collect_sort_resolve`) and `catalog/suites/wide_pk_validation.rs` (`setup_wide_unique`), one `add_index_circuit` site each: after `register_owned` of the base, `add_index(tid, tid + 1, &[3], <is_unique>).unwrap()`; the index batch is projected through the circuit's own `key_spec` and `index_schema` (`let ic = registry.index_circuit_for_cols(tid, &[3]).unwrap(); ic.ingest_owned_batch(batch_project_index(&bb, &ic.key_spec, &ic.index_schema))`, before `bb` moves into the base ingest), so each test's `make_index_schema` and `IndexKeySpec::new` go with the `Table::new` and `flush` it opened the index with. The index lands at `<dir>/idx_<tid+1>/w0of1`, a sibling of the hand-placed `<dir>/base`; the cursor reads the memtable, so no flush replaces the one removed.
- `catalog/suites/reopen_rebuild_tests.rs` `backfill_all_indexes_rebuilds_exactly_once`: logic unchanged; its header comment, which describes the standalone parent-dir home and a re-create on every call, now describes `idx_dir/w0of1` and a call that leaves a homed, filled store alone.
- `catalog/suites/reopen_rebuild_tests.rs` `index_rebuild_forced_by_topology_change`: today it pokes `set_recorded_topology` *after* a resuming open and expects `backfill_all_indexes()` to return 1, which only the unconditional re-open delivered; a topology mismatch is only ever visible at open. It is rewritten to make the mismatch visible where it really is: the helper `checkpointed_table_with_index(dir)` gains a `recorded_workers: u32` parameter it hands to its `record_topology` call (`index_rebuild_is_skipped_after_resume` passes 1), and this test passes 4 — the `_sequences` row the next open recovers. The reopen at one worker then finds `topology_matches()` false, the hook's open erases, and, being standalone, the hook backfills the index; the test keeps its `rederive_source() == Rederive { resume_at: None }` assertion and then asserts `!index_circuit_for_cols(tid, &[1]).unwrap().resumed_from_checkpoint()`, the index weight `N`, and `backfill_all_indexes() == 0` (the hook's fill is in the memtable, which `estimated_rows` counts).
- `crates/gnitz-py/tests/test_indices.py` gains `test_index_rebuilds_across_worker_count_change(own_server)`, beside `test_index_resumes_across_clean_restart` and built from the same statements: start at 4 workers, create `idxres.t` with 64 rows and `CREATE INDEX ON t(g)`, `own_server.restart(graceful=True, workers=2)`, then assert every eighth `seek_by_index` resolves to its PK, `own_server.rebuilt_index_counts() == [1, 1]`, and that every subdirectory of `<data_dir>/idxres/t_<tid>/idx_*/` is named `w0of2` or `w1of2` — the retired `w{k}of4` children were swept and every launched rank re-derived its slice. No E2E test today restarts an indexed table at a different worker count; this is the one path where every rank's home moves at once.

---

## 5. The registry's thread contract, stated where the type is

### Today

`RelationRegistry` is `!Send` and `!Sync` by auto-trait: every `Table` caches its last
full scan as an `Rc<Batch>`, and every run it holds is an `Rc<Batch>` or an
`Rc<MappedShard>` (`storage/lsm/run.rs`). The store says nothing about whether a host
may move one across threads. `gnitz-mirror/src/handle.rs`'s safety comment on
`unsafe impl Send for Mirror {}` reconstructs the argument from the store's internals,
and `gnitz-tokio` and `gnitz-py` each rely on it.

### Design

The contract is written on the type that owns it, as a doc section on
`RelationRegistry` in `relation/mod.rs`:

> **Thread contract.** `!Send + !Sync`: every `Table` holds its runs and its cached
> full scan behind `Rc`, and `StoreHandle` mutates through an `UnsafeCell`. Nothing
> under a registry is thread-affine: the production `thread_local!`s are recycled
> scratch (`batch_pool`, the merge blob cache, the cursor output buffers, the exchange
> relay's row scratch) and hold nothing between two calls; the data-directory lock is
> an open file description; each flush builds its io_uring ring for the call. The one
> exception is `#[cfg(test)]`-only: the shard index's compaction-statistics counter,
> which a store moved between threads splits across two copies and which only an
> `#[ignore]`d bench reads. A host may therefore move a registry to another thread
> provided that (1) it has exclusive access to the registry for the move, and (2) no
> value the registry handed out that holds one of those `Rc`s — an `Rc<Batch>` from
> `full_scan` or `scan_family`, a `ReadCursor`, `SourceCursor` or `PkSetGather`, whose
> runs are `Rc`s — is alive outside the registry at that moment: those refcounts are
> non-atomic, and a clone touched from the old thread while the registry is used on
> the new one is a data race.

`gnitz-mirror/src/handle.rs` keeps its `unsafe impl Send for Mirror {}` and its
comment's first sentence, which is the mirror's actual guarantee of (2): "no handle
into the registry can escape — every `Rc` the store mints is reached only from inside
it". The sentences that re-derive the store's internals (the `thread_local!`s, the
`flock`, the ring, the statistics counter) are replaced by a citation of the section
above; the host-side half, which edge orders the handover for each of the three hosts,
stays.

---

## Verification

1. `make verify` (fmt, clippy with warnings denied, all Rust tests). Both rung guards
   stay green by construction: `StoreError` lives in `storage`, which every rung in
   the sweep may name, and the server gains no module. The Rust guards that pin the
   new index home are `catalog/suites/reopen_rebuild_tests.rs` (the index checkpoint
   resume section, `backfill_all_indexes_rebuilds_exactly_once`, and the rewritten
   `index_rebuild_forced_by_topology_change`) and `catalog/suites/index_tests.rs`.
2. `grep -rn 'worker_ctx\|IngestError\|seek_family_bytes\|add_index_circuit\|replace_index_table\|new_index_table\|IndexProjectionTarget\|with_memtable_budget\|this_worker\|delta_for_this_worker\|inmem_ceiling\|adhoc_group_cap()\|is_standalone\|set_master_role\|set_worker_role' crates/ --include=*.rs` returns nothing.
3. `grep -rn 'WireFault\|env_num\|env_flag' crates/gnitz-store/src --include=*.rs | grep -v '/tests/\|bench\|foundation/env.rs\|flush_barrier.rs'` returns nothing.
4. `make e2e` at the default four workers, and `make e2e WORKERS=1`. The tests that set
   `GNITZ_SCAN_CHUNK_ROWS` and `GNITZ_RAM_TIER_BYTES` in the server's environment
   exercise the environment path in `CatalogEngine::open`; the delta-feed tests
   exercise the `STATUS_DELTA_EXPIRED` mapping in `catalog/view_state.rs`.
5. `make e2e K='ind'` at four workers covers the changed index home: CREATE INDEX on a
   populated table, UNIQUE promotion, a duplicate-key rejection, DROP INDEX, the
   restart that resumes index stores from a checkpoint
   (`test_index_resumes_across_clean_restart`, `rebuilt == 0` on every worker) and the
   new restart at a different worker count
   (`test_index_rebuilds_across_worker_count_change`, `rebuilt == 1` on every worker).
6. `cargo test -p gnitz-mirror`, and the `#[ignore]`d `resident_footprint_bench` once
   by hand, which must still report a spill-regime footprint under its 256 KiB tier.
