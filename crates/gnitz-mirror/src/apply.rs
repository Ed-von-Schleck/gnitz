//! Applying a delta: the bootstrap that replaces a copy and the poll that
//! advances it.
//!
//! **Two shapes arrive, and the caller states which.** A bootstrap answers with
//! the view's own store in the view's own schema, and the copy is erased first;
//! a poll answers with the delta store's rows, whose key is the server's round
//! number prepended to the view's key.
//!
//! Confusing the two is silent: a polled batch's PK region is eight bytes wider,
//! and the ingest's only shape check is the payload-column count, which matches
//! either way. So the caller states only which arrived, and [`Mirror`] pairs it
//! with the schemas the registration recorded.
//!
//! **A read never polls, and that is a decision rather than an omission.** The
//! host drives [`Mirror::poll`], and a read answers off whatever the last one
//! applied. A read that polled would be a read with a round trip in it — the one
//! thing a mirror exists to remove — and it would have it on every read, not
//! just the ones whose answer had changed.

use gnitz_core::{ClientError, RawBlock};
use gnitz_engine::storage::Batch;

use crate::error::MirrorError;
use crate::handle::{Mirror, MirroredView};

impl Mirror {
    /// Advance every mirrored view by one poll each.
    ///
    /// A view whose cursor has expired — a foreign tag, or a cursor below a
    /// worker's retained floor — is re-resolved and reseeded in place; that is
    /// the single recovery the feed's contract names, and it is not optional.
    ///
    /// A poll does **not** drive a tick server-side: a delta read answers "what
    /// has happened", not "what is current", so a push the tick loop has not run
    /// yet is a round the next poll carries.
    pub fn poll(&mut self) -> Result<(), MirrorError> {
        self.check_poison()?;
        for tid in self.mirrored_ids() {
            self.advance_or_bootstrap(tid)?;
        }
        Ok(())
    }

    pub(crate) fn advance_or_bootstrap(&mut self, table_id: u64) -> Result<(), MirrorError> {
        let view = self.view(table_id)?;
        let Some(cursor) = view.cursor else {
            return self.bootstrap(table_id);
        };
        let reply_schema = std::sync::Arc::clone(&view.delta_reply_schema);
        match self.client.delta_poll_raw(table_id, cursor, &reply_schema) {
            Ok((blocks, next)) => {
                self.guarded("applying a poll", |m| m.ingest_blocks(table_id, blocks, true))?;
                self.view_mut(table_id)?.cursor = Some(next);
                self.checkpoint_if_due()
            }
            // The one recovery the feed names, and it is single: discard the copy
            // and bootstrap. It re-resolves first, because a foreign tag is
            // exactly how the server reports a relation recreated under the same
            // name — a bootstrap at the old id would read a relation that no
            // longer exists.
            Err(ClientError::DeltaExpired) => self.reseed_after_expiry(table_id),
            // A poll can also fail because the id itself is gone rather than
            // because its tag moved: a `DROP VIEW v; CREATE VIEW v …` upstream
            // leaves the mirror holding an id that names nothing, and the read is
            // refused outright rather than answered with a foreign tag. So any
            // other refusal is re-resolved once — if the name now answers a
            // different id, that is what happened and the recovery is the same;
            // if it answers the same id, the error is real and is the caller's.
            Err(e) => match self.relation_id_moved(table_id) {
                Ok(true) => self.reseed_after_expiry(table_id),
                _ => Err(MirrorError::Upstream(e)),
            },
        }
    }

    /// Whether `table_id`'s name now resolves to a different id upstream.
    fn relation_id_moved(&mut self, table_id: u64) -> Result<bool, MirrorError> {
        let view = self.view(table_id)?;
        let (schema_name, name) = (view.schema_name.clone(), view.name.clone());
        let (_, kind) = self.client.resolve_relation(&schema_name, &name)?;
        Ok(kind.tid != table_id)
    }

    /// **A live expiry re-resolves by name; it does not bootstrap in place.**
    ///
    /// Erasing and re-reading under the *same* id is only correct when the id is
    /// still the relation the host asked for, and it may not be: a foreign tag is
    /// exactly how the server reports a relation recreated under the same name,
    /// so a bootstrap at the old id would read a relation that no longer exists.
    /// If the descriptor still matches, the tag moved for the other reason — a
    /// server restart — and the bootstrap is under the id already registered. One
    /// path either way, and it is the path a reopen takes too.
    fn reseed_after_expiry(&mut self, table_id: u64) -> Result<(), MirrorError> {
        // Drop the cursor first: whatever the reconcile finds, the copy it ends
        // up with must come from a bootstrap, not from a round the expired cursor
        // named.
        let view = self.view_mut(table_id)?;
        view.cursor = None;
        let (schema_name, name) = (view.schema_name.clone(), view.name.clone());
        let tid = self.reconcile_registration(&schema_name, &name)?;
        self.bootstrap(tid)
    }

    /// Replace the copy with the view's whole current value.
    ///
    /// The erase unlinks this worker's child manifest, so the rebuilt store's
    /// `Rederive` open peeks `None` and *erases* the stale shards rather than
    /// reloading them, then rebuilds the handle empty. It is exactly the state
    /// transition a bootstrap needs, and it compiles nothing.
    pub(crate) fn bootstrap(&mut self, table_id: u64) -> Result<(), MirrorError> {
        self.check_poison()?;
        let view_schema = self.view(table_id)?.schema.clone();
        // The cursor goes first, and stays gone until the reply is applied: the
        // erase below leaves a store holding nothing, so a cursor surviving a
        // failed read would have the next poll deliver `(T, …]` onto an empty
        // copy and lose everything at or below `T` with no error anywhere.
        self.view_mut(table_id)?.cursor = None;
        self.engine
            .reset_view_output_for_rebuild(table_id as i64)
            .map_err(|e| self.poison(format!("erasing the copy of {table_id} failed: {e}")))?;

        let (blocks, cursor) = self.client.delta_bootstrap_raw(table_id, &view_schema)?;
        self.guarded("applying a bootstrap", |m| m.ingest_blocks(table_id, blocks, false))?;
        self.view_mut(table_id)?.cursor = Some(cursor);
        self.checkpoint_if_due()
    }

    /// The entry for `table_id`, or the error every mirrored-relation lookup
    /// reports.
    fn view(&self, table_id: u64) -> Result<&MirroredView, MirrorError> {
        self.views
            .get(&table_id)
            .ok_or_else(|| MirrorError::Engine(format!("relation {table_id} is not mirrored")))
    }

    fn view_mut(&mut self, table_id: u64) -> Result<&mut MirroredView, MirrorError> {
        self.views
            .get_mut(&table_id)
            .ok_or_else(|| MirrorError::Engine(format!("relation {table_id} is not mirrored")))
    }

    /// Decode each block and ingest it. `stamped` says which of the two shapes
    /// arrived — a poll's rows, keyed by the round number prepended to the
    /// view's key, or a bootstrap's, already in the view's own shape. Both
    /// schemas come from the registration, so the pairing cannot be got wrong
    /// here.
    ///
    /// The decode is the engine's public entry, not the ring-internal one: that
    /// one skips the long-string extent check because its contract confines it
    /// to the W2M ring, and every frame arriving over a socket is a client
    /// frame.
    ///
    /// The batch comes back `Layout::Raw`, which is what makes the ingest's
    /// sort-and-fold run. A sender-supplied `Consolidated` tag would let every
    /// downstream merge fold weights against the wrong element, and the merge's
    /// own sortedness check is `#[cfg(debug_assertions)]` — absent in a release
    /// host.
    ///
    /// The ingest is the checked catalog entry, not the raw store: it rejects a
    /// system id and checks the batch's payload-column count against the
    /// registered schema. For a view it skips PK-uniqueness enforcement, which
    /// is what a view needs — a view's PK is not unique.
    ///
    /// Takes the blocks by value so each frame buffer is freed as its rows land
    /// in the store. The client hands over the whole train — a bootstrap's is
    /// the view's entire value — but freeing as we go keeps that off the peak:
    /// at the busiest moment roughly one frame is live, and the decode and the
    /// store ingest of that frame dominate what the train itself costs.
    fn ingest_blocks(&mut self, table_id: u64, blocks: Vec<RawBlock>, stamped: bool) -> Result<(), MirrorError> {
        let view = self.view(table_id)?;
        let (view_desc, delta_desc) = (view.view_desc, view.delta_desc);
        let in_desc = if stamped { delta_desc } else { view_desc };
        let mut applied = 0usize;
        for raw in blocks {
            let block = raw.block();
            applied += block.len();
            let (batch, _) = Batch::decode_from_wal_block(block, &in_desc, false)
                .map_err(|e| self.poison(format!("decoding a delta for {table_id} failed: {e}")))?;
            let batch = if stamped {
                batch.stripped_of_pk_prefix(&in_desc, &view_desc)
            } else {
                batch
            };
            if batch.count == 0 {
                continue;
            }
            self.engine
                .ingest_returning_effective(table_id as i64, batch)
                .map_err(|e| self.poison(format!("applying a delta to {table_id} failed: {e}")))?;
        }
        self.note_applied(applied);
        Ok(())
    }

    fn checkpoint_if_due(&mut self) -> Result<(), MirrorError> {
        if self.checkpoint_is_due() {
            // Reported after the delta it followed was applied, and the handle
            // stays usable: what failed is the durability of the copy, not the
            // copy.
            return self.checkpoint();
        }
        Ok(())
    }
}
