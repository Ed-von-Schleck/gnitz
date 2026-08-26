//! Applying a delta: the bootstrap that replaces a copy and the poll that
//! advances it.

use gnitz_core::{ClientError, RawBlock};
use gnitz_engine::foundation::fault::Seam;
use gnitz_engine::storage::Batch;

use crate::error::MirrorError;
use crate::handle::{Mirror, MirroredView};

/// `GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR`: fail the next bootstrap's upstream read
/// once, after the copy has been erased. Debug-only. The window is unreachable
/// otherwise — the server's only refusal here is an oversized reply.
static BOOTSTRAP_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR");

/// What one view's poll did. The round it left the copy at is
/// [`Mirror::cursor_of`]; this says only whether the copy is a continuation of
/// what the caller last saw.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PollOutcome {
    /// The relation's server id, which a recreated view moves.
    pub view_id: u64,
    /// The call bootstrapped rather than applying deltas, so anything derived
    /// from the copy's previous contents is stale in a way no delta explains.
    ///
    /// The cursor cannot carry this: an expiry-driven reseed inside one boot
    /// keeps the tag and moves the tick forward, exactly as an ordinary advance
    /// does.
    pub reseeded: bool,
}

/// Which reply shape a train carries. The bytes do not say; the caller does.
#[derive(Clone, Copy)]
enum Shape {
    /// A bootstrap's: the view's own rows, in the view's own schema.
    Plain,
    /// A poll's: the delta store's rows, keyed by the round number prepended to
    /// the view's key.
    Stamped,
}

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
    ///
    /// **Every view is attempted; the first failure is returned after the loop**,
    /// naming the view for [`Mirror::forget_view`] — a view dropped upstream
    /// fails forever, and stopping at it would freeze every view behind it. An
    /// `Err` therefore carries no report, and a host that sees one treats every
    /// mirrored view as possibly reseeded.
    pub fn poll(&mut self) -> Result<Vec<PollOutcome>, MirrorError> {
        self.check_poison()?;
        let mut first: Option<MirrorError> = None;
        let mut out = Vec::new();
        for tid in self.mirrored_ids() {
            match self.advance_or_bootstrap(tid) {
                Ok(outcome) => out.push(outcome),
                Err(e) => {
                    first.get_or_insert(self.named_failure(tid, e));
                }
            }
        }
        first.map_or(Ok(out), Err)
    }

    /// `e`, prefixed with which view produced it — what a host needs to pick the
    /// one to [`Mirror::forget_view`]. Only a message is prefixed: a classified
    /// upstream refusal is something a host branches on, and keeps its class.
    fn named_failure(&self, table_id: u64, e: MirrorError) -> MirrorError {
        let Some(view) = self.views.get(&table_id) else {
            return e;
        };
        let who = |m: String| format!("view '{}.{}' ({table_id}): {m}", view.schema_name, view.name);
        match e {
            MirrorError::Upstream(ClientError::ServerError(m)) => {
                MirrorError::Upstream(ClientError::ServerError(who(m)))
            }
            MirrorError::Engine(m) => MirrorError::Engine(who(m)),
            MirrorError::Poisoned(m) => MirrorError::Poisoned(who(m)),
            e => e,
        }
    }

    pub(crate) fn advance_or_bootstrap(&mut self, table_id: u64) -> Result<PollOutcome, MirrorError> {
        self.check_poison()?;
        let Some(&cursor) = self.cursors.get(&table_id) else {
            return self.bootstrap(table_id);
        };
        let reply_schema = std::sync::Arc::clone(&self.view(table_id)?.delta_reply_schema);
        match self.client.delta_poll_raw(table_id, cursor, &reply_schema) {
            Ok((blocks, next)) => {
                self.guarded("applying a poll", |m| m.ingest_blocks(table_id, blocks, Shape::Stamped))?;
                self.cursors.insert(table_id, next);
                self.checkpoint_if_due()?;
                Ok(PollOutcome {
                    view_id: table_id,
                    reseeded: false,
                })
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
        Ok(self.client.resolve_relation(&schema_name, &name)?.tid != table_id)
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
    fn reseed_after_expiry(&mut self, table_id: u64) -> Result<PollOutcome, MirrorError> {
        // Drop the cursor first: whatever the reconcile finds, the copy it ends
        // up with must come from a bootstrap, not from a round the expired cursor
        // named.
        self.cursors.remove(&table_id);
        let view = self.view(table_id)?;
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
    pub(crate) fn bootstrap(&mut self, table_id: u64) -> Result<PollOutcome, MirrorError> {
        self.check_poison()?;
        let view_schema = std::sync::Arc::clone(&self.view(table_id)?.desc.schema);
        // Everything between here and the insert below is a copy that does not
        // exist, and the missing cursor is what says so: a cursor surviving a
        // failed read would have the next poll deliver `(T, …]` onto an erased
        // store and lose everything at or below `T` in silence.
        self.cursors.remove(&table_id);
        self.engine
            .reset_view_output_for_rebuild(table_id as i64)
            .map_err(|e| self.poison(format!("erasing the copy of {table_id} failed: {e}")))?;

        let (blocks, cursor) = if BOOTSTRAP_ERROR.take_once() {
            Err(ClientError::ServerError("injected bootstrap read failure".to_string()))
        } else {
            self.client.delta_bootstrap_raw(table_id, &view_schema)
        }?;
        self.guarded("applying a bootstrap", |m| {
            m.ingest_blocks(table_id, blocks, Shape::Plain)
        })?;
        self.cursors.insert(table_id, cursor);
        self.checkpoint_if_due()?;
        Ok(PollOutcome {
            view_id: table_id,
            reseeded: true,
        })
    }

    /// The entry for `table_id`, or the error every mirrored-relation lookup
    /// reports.
    fn view(&self, table_id: u64) -> Result<&MirroredView, MirrorError> {
        self.views
            .get(&table_id)
            .ok_or_else(|| MirrorError::Engine(format!("relation {table_id} is not mirrored")))
    }

    /// Decode each block under the schema `shape` names — both come from the
    /// registration — and ingest it.
    ///
    /// Two engine entries here have a near twin that is silently wrong for a
    /// socket frame, so each is named for what it keeps: the **public** decode
    /// keeps the long-string extent check the ring-internal one drops, and the
    /// `Layout::Raw` it returns keeps the ingest's sort-and-fold, where a
    /// sender's `Consolidated` claim would fold weights onto the wrong element
    /// past a sortedness check that is debug-only.
    fn ingest_blocks(&mut self, table_id: u64, blocks: Vec<RawBlock>, shape: Shape) -> Result<(), MirrorError> {
        let view = self.view(table_id)?;
        let (view_desc, delta_desc) = (view.view_desc, view.delta_desc);
        let in_desc = match shape {
            Shape::Plain => view_desc,
            Shape::Stamped => delta_desc,
        };
        let mut applied = 0usize;
        for raw in blocks {
            let block = raw.block();
            applied += block.len();
            let (batch, _) = Batch::decode_from_wal_block(block, &in_desc, false)
                .map_err(|e| self.poison(format!("decoding a delta for {table_id} failed: {e}")))?;
            let batch = match shape {
                Shape::Plain => batch,
                Shape::Stamped => batch.stripped_of_pk_prefix(&in_desc, &view_desc),
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
