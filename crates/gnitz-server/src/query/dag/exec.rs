//! Epoch execution: the one pre → relay → consolidate → post pipeline every
//! compiled shape runs through, and the DAG evaluation driver.

use super::*;
use crate::query::compiler::{Side, Sides};
use gnitz_store::storage::StorageError;

/// One edge of a tick's schedule: `producer`'s output feeds `view`, and `depth`
/// is `view`'s stamped registration depth. Field order is the sort order, so
/// sorting a schedule puts every producer before the steps it feeds.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Step {
    depth: i32,
    view: i64,
    producer: i64,
}

/// What a side's relay rounds are keyed by, so a view's two sides cannot collide
/// in the master's accumulator. Its own type because the other `i64` in reach is
/// a table id, and the two are not interchangeable.
#[derive(Clone, Copy)]
struct RelayKey(i64);

impl RelayKey {
    /// The key a view with a single side relays under: its own shard columns.
    const OWN_SHARD: RelayKey = RelayKey(0);
}

/// The exchange transport of one view's epoch, with the view it relays for and
/// the elide verdict bound once so a round takes only its batch and key.
struct Relay<'a> {
    exchange: &'a mut dyn ExchangeCallback,
    view_id: i64,
    /// This view's shuffle is a proven no-op, so a round hands its batch back.
    elide: bool,
}

impl Relay<'_> {
    /// One repartition round. Run even for an empty batch, so the collective
    /// rounds stay balanced across workers.
    fn round(&mut self, batch: Batch, key: RelayKey) -> Batch {
        match self.elide {
            true => batch,
            false => self.exchange.do_exchange(self.view_id, &batch, key.0),
        }
    }
}

impl DagEngine {
    // ── Epoch execution ─────────────────────────────────────────────────

    /// Run one view's epoch: compile it if it is not cached, route the delta
    /// through the exchange where the view's routing metadata says it must, then
    /// execute the compiled plan. `None` when a phase produced nothing.
    fn run_view_epoch(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        input: Batch,
        src_id: i64,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<Option<Batch>, String> {
        if !self.ensure_compiled(registry, view_id)? {
            gnitz_warn!("dag: run_view_epoch — no plan for view_id={}", view_id);
            return Ok(None);
        }
        // Routing comes off the one memoized `ViewMeta` the master relay also
        // reads; the plan supplies only its executable shape. Taken before the
        // plan borrow, so the memo lookup and the cache read do not overlap.
        let meta = (!registry.relation_is_replicated(view_id)).then(|| self.view_meta(registry, view_id));
        let input = match meta.as_ref().is_some_and(|m| m.scatters(src_id)) {
            true => exchange.do_exchange(view_id, &input, src_id),
            false => input,
        };
        let plan = self.cache.get_mut(&view_id).expect("ensure_compiled inserted it");
        let elide = match meta.as_ref() {
            // A replicated view holds every source in full: nothing to repartition.
            None => true,
            // A lone side's rows already sit on the worker that owns them; a
            // set-op pair's two sides must still meet on one worker.
            Some(m) => m.skips_exchange && matches!(plan.sides, Sides::Unary(_)),
        };
        let mut relay = Relay {
            exchange,
            view_id,
            elide,
        };
        Self::run_plan(&mut plan.sides, &mut plan.post, input, src_id, &mut relay).map_err(|e| e.to_string())
    }

    /// Seed every phase of a compiled plan and run its post combine.
    ///
    /// Which sides are active derives from the plan and `src_id`, both
    /// worker-identical — so the collective relay rounds stay balanced.
    fn run_plan(
        sides: &mut Sides,
        post: &mut SubPlan,
        input: Batch,
        src_id: i64,
        relay: &mut Relay<'_>,
    ) -> Result<Option<Batch>, StorageError> {
        // Each arm hands `execute_epoch_multi` a fixed-size array: the seed count
        // is the side count, known here, so no arm heap-allocates to carry it.
        match sides {
            Sides::Unexchanged => {
                let seed = sub_seed(post, input, src_id);
                vm::execute_epoch_multi(&mut post.vm, [seed])
            }
            Sides::Unary(side) => {
                let seed = Self::run_side(side, Some(input), src_id, RelayKey::OWN_SHARD, relay)?;
                vm::execute_epoch_multi(&mut post.vm, [seed])
            }
            Sides::Pair([(key_a, a), (key_b, b)]) => {
                // A side takes the delta iff it scans the delta's source, so
                // `a UNION a` runs both. The last taker is handed the batch and
                // the other a copy.
                let (da, db) = match (*key_a == src_id, *key_b == src_id) {
                    (true, true) => (Some(input.clone_batch()), Some(input)),
                    (true, false) => (Some(input), None),
                    (false, true) => (None, Some(input)),
                    (false, false) => (None, None),
                };
                let seeds = [
                    Self::run_side(a, da, src_id, RelayKey(*key_a), relay)?,
                    Self::run_side(b, db, src_id, RelayKey(*key_b), relay)?,
                ];
                vm::execute_epoch_multi(&mut post.vm, seeds)
            }
        }
    }

    /// One side's seed for the post phase, `None` where the delta does not reach
    /// it.
    ///
    /// The consolidate is mandatory: the relay concatenates rows from every
    /// worker (and a HashRow reindex scrambles PK order), and the post phase's
    /// distinct/join operators assume sorted, weight-merged input.
    fn run_side(
        side: &mut Side,
        delta: Option<Batch>,
        src_id: i64,
        key: RelayKey,
        relay: &mut Relay<'_>,
    ) -> Result<(u16, Batch), StorageError> {
        // `exchange_schema()` IS the side's out-register schema, which the VM
        // stamps on what it returns — so the wire encode sees the side's
        // pre-exchange schema, never the view's combine-widened final one.
        let schema = side.exchange_schema();
        let Some(delta) = delta else {
            return Ok((side.seed_reg, Batch::empty_with_schema(&schema)));
        };
        let seed = sub_seed(&side.plan, delta, src_id);
        let pre =
            vm::execute_epoch_multi(&mut side.plan.vm, [seed])?.unwrap_or_else(|| Batch::empty_with_schema(&schema));
        Ok((side.seed_reg, relay.round(pre, key).into_consolidated(&schema)))
    }

    // ── DAG traversal driver ────────────────────────────────────────────

    /// One [`Step`] per dependency edge out of `source_id`'s forward closure, in
    /// execution order. A pure function of the dep map and the stamped depths,
    /// both worker-identical — which is what keeps the workers in lockstep.
    fn tick_schedule(&mut self, registry: &RelationRegistry, source_id: i64) -> Vec<Step> {
        // A push into a table no view scans is the common case, and reaches
        // nothing.
        if !self.has_dependents(registry, source_id) {
            return Vec::new();
        }
        let mut producers = self.dependent_closure(registry, vec![source_id]);
        producers.insert(source_id);
        let mut schedule: Vec<Step> = Vec::new();
        for producer in producers {
            for &view in self.dep.forward.get(&producer).into_iter().flatten() {
                // The dep map is built from `CircuitNodes`, which can still name a
                // relation the registry no longer holds.
                if let Some(entry) = registry.entry(view) {
                    schedule.push(Step {
                        depth: entry.depth,
                        view,
                        producer,
                    });
                }
            }
        }
        schedule.sort_unstable();
        schedule
    }

    /// Run `view_id`'s epoch over `input` and ingest whatever it produced into
    /// the view's family.
    ///
    /// Returns whether it produced rows, and the output itself only when `emit`
    /// says a later step reads it — the store moves the batch otherwise, saving
    /// the whole copy for a leaf view and for every backfill chunk. Never cloned
    /// here: the store hands back what it borrowed.
    fn run_and_ingest(
        &mut self,
        registry: &mut RelationRegistry,
        view_id: i64,
        src_id: i64,
        input: Batch,
        emit: Emit,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<(bool, Option<Batch>), String> {
        let produced = self
            .run_view_epoch(registry, view_id, input, src_id, exchange)?
            .filter(|b| !b.is_empty());
        let Some(out) = produced else { return Ok((false, None)) };
        let echo = registry
            .ingest_view_delta(view_id, out, emit.round(), emit.fans_out())
            .map_err(|e| format!("view store ingest failed (view_id={view_id}): {e}"))?;
        Ok((true, echo))
    }

    /// Run `schedule` to completion, seeded with `seed_producer`'s output.
    fn drive(
        &mut self,
        registry: &mut RelationRegistry,
        schedule: &[Step],
        seed_producer: i64,
        seed: Batch,
        round: u64,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<(), String> {
        // Which steps each producer feeds, in schedule order — so the group's
        // last entry is the last consumer of that producer's output.
        let mut feeds: FxHashMap<i64, Vec<usize>> = FxHashMap::default();
        for (i, step) in schedule.iter().enumerate() {
            feeds.entry(step.producer).or_default().push(i);
        }
        let mut inputs: Vec<Option<Batch>> = (0..schedule.len()).map(|_| None).collect();
        let seeded = feeds.get(&seed_producer).map_or(&[][..], Vec::as_slice);
        Self::fan_out(&mut inputs, seeded, seed);

        for (i, step) in schedule.iter().enumerate() {
            // No input means the producer never ran — a relation the registry no
            // longer holds — so this edge carries nothing.
            let Some(input) = inputs[i].take() else { continue };
            let emit = Emit::Tick {
                round,
                fans_out: feeds.contains_key(&step.view),
            };
            let (_, echo) = self.run_and_ingest(registry, step.view, step.producer, input, emit, exchange)?;
            let Some(fed) = feeds.get(&step.view) else { continue };
            // A view that produced nothing still fans an empty batch, so an
            // exchange-dependent consumer runs and collective rounds stay in
            // lockstep.
            let out = echo.unwrap_or_else(|| {
                let entry = registry.entry(step.view).expect("the schedule names registered views");
                Batch::empty_with_schema(&entry.schema)
            });
            Self::fan_out(&mut inputs, fed, out);
        }
        Ok(())
    }

    /// Hand `delta` to each of the steps a producer feeds, in schedule order. The
    /// last of them is handed the batch itself and the rest a copy, so at most
    /// two copies of a delta are live at once.
    fn fan_out(inputs: &mut [Option<Batch>], fed: &[usize], delta: Batch) {
        let Some((&last, rest)) = fed.split_last() else {
            return;
        };
        for &i in rest {
            Self::deposit(&mut inputs[i], delta.clone_batch());
        }
        Self::deposit(&mut inputs[last], delta);
    }

    /// Put `delta` into one step's input slot: a fill when the slot holds no rows,
    /// a union otherwise. An empty slot takes the fill rather than the union
    /// because `op_union` against an empty operand clones the other one whole.
    fn deposit(slot: &mut Option<Batch>, delta: Batch) {
        match slot.take().filter(|b| !b.is_empty()) {
            Some(held) => {
                // The held batch's schema, not the incoming one: it selects
                // `payload_cmp` and is what the union's result is certified under.
                let schema = held.schema;
                *slot = Some(ops::op_union(held, &delta, &schema));
            }
            None => *slot = Some(delta),
        }
    }

    /// Run every edge `source_id` reaches.
    ///
    /// No flush. A delta is queryable out of the memtable the moment it is
    /// ingested, and the memtable's own budget bounds the tier below it; draining
    /// every tick only made that tier re-merge its whole window once per tick.
    ///
    /// `tick_round` is the master-allocated round this evaluation runs under. It
    /// stamps every fed view's captured delta — what makes "give me what changed
    /// since N" answerable.
    pub(crate) fn evaluate_dag(
        &mut self,
        registry: &mut RelationRegistry,
        source_id: i64,
        delta: Batch,
        tick_round: u64,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<(), String> {
        let schedule = self.tick_schedule(registry, source_id);
        if schedule.is_empty() {
            return Ok(());
        }
        self.drive(registry, &schedule, source_id, delta, tick_round, exchange)
    }

    /// Drive ONE view's epoch for a distributed-backfill chunk and ingest its
    /// output into the view's family. Returns true iff the chunk produced rows.
    ///
    /// **View-scoped**, where [`Self::evaluate_dag`] drives the whole closure: a
    /// live CREATE VIEW must not re-drive the source's existing dependents, which
    /// are already populated. Boot has none, so it keeps the closure driver.
    pub(crate) fn backfill_chunk(
        &mut self,
        registry: &mut RelationRegistry,
        view_id: i64,
        source_id: i64,
        delta: Batch,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<bool, String> {
        if !registry.has_id(view_id) {
            return Ok(false);
        }
        // Durable would double-count: such a relation loads its shards from its
        // manifest at open. Here as well as in the ingest verb, which a chunk
        // producing no rows never reaches.
        debug_assert!(
            registry.relation_kind(view_id).is_none_or(|k| k.is_view()),
            "distributed backfill into durable relation {view_id}: \
             would double-count loaded shards",
        );
        let (produced, _) = self.run_and_ingest(registry, view_id, source_id, delta, Emit::Backfill, exchange)?;
        Ok(produced)
    }

    /// End `view_id`'s backfill, `produced` being whether any chunk produced rows.
    ///
    /// Releases the last chunk's pinned registers and trace cursors, then folds
    /// the view's memtable into its RAM tier once — so the view's first reads open
    /// over fewer sources, at one fold per backfill rather than one per chunk.
    pub(crate) fn finish_backfill(
        &mut self,
        registry: &mut RelationRegistry,
        view_id: i64,
        produced: bool,
    ) -> Result<(), String> {
        if let Some(plan) = self.cache.get_mut(&view_id) {
            for sub in plan.sub_plans_mut() {
                sub.vm.release();
            }
        }
        match produced {
            true => registry
                .flush(view_id)
                .map_err(|e| format!("view store flush failed (view_id={view_id}): {e}")),
            false => Ok(()),
        }
    }
}

/// Why one view epoch is running, and what its output is still wanted for.
/// One value rather than two flags, so the combinations that do not exist — a
/// backfill stamping a round, a tick without one — cannot be spelled.
#[derive(Clone, Copy)]
enum Emit {
    /// A tick step at `round`, whose output a later step reads when `fans_out`.
    Tick { round: u64, fans_out: bool },
    /// A backfill chunk: its rows never enter a delta store, and no other step
    /// of this drive reads them.
    Backfill,
}

impl Emit {
    /// The round a fed view's captured delta is stamped with; `None` keeps a
    /// backfill's rows out of the delta store, which a bootstrap read carries.
    fn round(self) -> Option<u64> {
        match self {
            Emit::Tick { round, .. } => Some(round),
            Emit::Backfill => None,
        }
    }

    fn fans_out(self) -> bool {
        matches!(self, Emit::Tick { fans_out: true, .. })
    }
}

/// The `(register, batch)` that seeds a sub-pipeline with `source_id`'s delta:
/// the register that source routes to, or the plan's single input where it
/// routes no source of that id.
fn sub_seed(sub: &SubPlan, input: Batch, source_id: i64) -> (u16, Batch) {
    let in_reg = sub.source_reg_map.get(&source_id).copied().unwrap_or(sub.in_reg);
    (in_reg, input)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
