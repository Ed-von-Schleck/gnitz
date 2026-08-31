//! Per-key hydration for a capacity-bounded view: recompute one key's output
//! rows by re-running the view's own compiled program over a key-restricted
//! seed.
//!
//! A bounded view's store keeps only skeleton rows past its capacity — the PK
//! and one coarse weight — so a read that touches such a key has to reproduce
//! its payload. Nothing is stored to make that possible: the hydration ground
//! truth is state the view already keeps at full fidelity. For a linear body
//! that is the source relation's own store; for an inner equi-join it is the two
//! `integrate_trace` tables the compiled circuit already maintains.
//!
//! Exactness: restriction to a key is linear and time-invariant, so it commutes
//! with integration — `σ_k(I(s)) = I(σ_k(s))` — and the inner equi-join is
//! per-key, `σ_k(A ⋈ B) = σ_k(A) ⋈ σ_k(B)`. The compiled two-term union exists
//! to produce *deltas* incrementally; the state they integrate to is that single
//! product, which is what a replay over the two trace integrals computes.

use super::*;
use crate::query::compiler::{HydrationSeed, PlanShape};
use crate::storage::PkSetGather;

impl DagEngine {
    /// Recompute the output rows of the capacity-bounded view `view_id` for
    /// `keys` — the flat concatenation of the OPK images, ascending — whose
    /// per-key coarse weights are `coarse`. One consolidated batch in the view's
    /// own schema.
    ///
    /// `keys` is taken by value because the gather below owns its key list; the
    /// caller built the buffer for this call and has no further use for it.
    ///
    /// `chunk_rows` bounds the seed batch only; `JoinDT`'s cogroup accepts
    /// multi-key deltas, so the chunking costs nothing but peak memory.
    pub(crate) fn hydrate_keys(
        &mut self,
        view_id: i64,
        keys: Vec<u8>,
        coarse: &[i64],
        chunk_rows: usize,
    ) -> Result<Batch, String> {
        let Some(view_schema) = self.tables.get(&view_id).map(|e| e.schema) else {
            return Err(format!("hydrate: view {view_id} is not a registered relation"));
        };
        // The plan cache is lazy and is dropped on every rebuild, so a read
        // arriving before the view's first tick would otherwise find nothing.
        // `Ok(false)` is that same registry lookup, which the check above already
        // passed, so only a real compile failure leaves here.
        let compiled = self.ensure_compiled(view_id)?;
        debug_assert!(compiled, "ensure_compiled false for a registered view");
        let hydration = self.cache[&view_id]
            .hydration
            .ok_or_else(|| format!("hydrate: view {view_id} was not compiled as capacity-bounded"))?;

        let mut out = Batch::empty_with_schema(&view_schema);
        if keys.is_empty() {
            return Ok(out);
        }

        // The seed cursor and the register the replay feeds it to. A linear body
        // seeds the source relation's store at the program's start; a join seeds
        // one branch's trace mid-program, because the join key is not the source
        // PK and a key-restricted feed at the `ScanDelta` register would mean
        // scanning the whole source.
        //
        // The ranged cursor open takes `&self` and the cursor owns its runs via
        // `Rc`, so both borrows of the registry end here and the VM is free to
        // run below.
        let plan = self.cache.get_mut(&view_id).expect("ensure_compiled inserted the plan");
        let PlanShape::Single(sub) = &mut plan.shape else {
            return Err(format!("hydrate: view {view_id} is not a single-phase plan"));
        };
        // Cursors only — no `compact_owned_traces`, because a read must not mutate
        // shard state. A linear circuit owns no trace table, so this is a no-op
        // there.
        sub.vm.bind_trace_cursors();
        let seed_schema = sub.vm.program.reg_meta[hydration.in_reg as usize].schema;
        // The gather opens over the range its own key list spans.
        let mut gather = match hydration.seed {
            HydrationSeed::Relation(source) => {
                let entry = self
                    .tables
                    .get(&source)
                    .ok_or_else(|| format!("hydrate: view {view_id} source {source} is unregistered"))?;
                // A linear view's physical PK is the leading source-PK columns,
                // byte-identical to the source PK, so the store's own keys index
                // the source directly.
                PkSetGather::open(keys, seed_schema, |s, e| entry.open_cursor_in_range(s, e))
            }
            HydrationSeed::Trace(seed_table) => {
                let trace = sub.vm.table(seed_table);
                PkSetGather::open(keys, seed_schema, |s, e| trace.open_cursor_in_range(s, e))
            }
        };
        while let Some(seed) = gather.next_chunk(chunk_rows) {
            // Reusing the cached `VmHandle`'s own register file is safe in both
            // directions: the dispatch clears every delta register on entry, so
            // hydration starts clean and leaves nothing a later epoch can read
            // (the next epoch clears again and re-binds its trace cursors through
            // `bind_trace_cursors`).
            let produced = vm::execute_epoch_replay(&mut sub.vm, (hydration.in_reg, seed), hydration.start_pc)
                .map_err(|e| format!("hydrate: view {view_id} replay failed: {e}"))?;
            if let Some(b) = produced {
                debug_assert!(
                    b.schema.same_physical_layout(&view_schema),
                    "hydration produced a batch that is not in the view's schema",
                );
                out.append_batch(&b, 0, b.count);
            }
        }

        let out = out.into_consolidated(&view_schema);
        debug_assert_hydration_matches(&out, gather.keys(), coarse);
        Ok(out)
    }
}

/// Tripwire: the replay's per-PK weight sum must equal the coarse weight the
/// skeleton row carried, by linearity of the PK projection.
///
/// One co-walk, not a scan per key: `keys` is ascending by construction and
/// `into_consolidated` sorted `out` by (PK, payload), so the two run in step.
/// That also catches a PK `out` holds rows for that `keys` never named, which a
/// per-key lookup cannot see.
///
/// The `cfg!` return is what a `#[cfg(debug_assertions)]` on the function would
/// not give: the assertions vanish in release either way, but the co-walk itself
/// is O(rows) and would otherwise still run.
fn debug_assert_hydration_matches(out: &Batch, keys: &[u8], coarse: &[i64]) {
    if !cfg!(debug_assertions) {
        return;
    }
    use crate::schema::key::compare_pk_bytes;
    let stride = keys.len() / coarse.len();
    let mut ki = 0;
    let mut i = 0;
    while i < out.count {
        let pk = out.get_pk_bytes(i);
        // Every skeleton key carries a strictly positive coarse weight, so a key
        // the replay produced nothing for is a bug — as is a key it produced rows
        // for that no skeleton row named.
        while ki < coarse.len() && compare_pk_bytes(&keys[ki * stride..(ki + 1) * stride], pk).is_lt() {
            debug_assert!(false, "hydration produced no rows for skeleton key {ki}");
            ki += 1;
        }
        debug_assert!(
            ki < coarse.len() && keys[ki * stride..(ki + 1) * stride] == *pk,
            "hydration produced rows for a PK no skeleton row named",
        );
        let mut sum = 0i64;
        while i < out.count && out.get_pk_bytes(i) == pk {
            sum += out.get_weight(i);
            i += 1;
        }
        debug_assert_eq!(
            sum,
            coarse.get(ki).copied().unwrap_or(0),
            "hydration weight mismatch for key {pk:?}",
        );
        ki += 1;
    }
    debug_assert_eq!(
        ki,
        coarse.len(),
        "hydration produced no rows for a trailing skeleton key"
    );
}
