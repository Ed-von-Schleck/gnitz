//! Answering reads locally, and delegating the rest.
//!
//! The handle takes an **encoded `ReadSpec` plus a reply schema** — the wire
//! read verbs, not SQL. The engine has no parser, the client core never
//! constructs a `ReadSpec` and the SQL crate does, and the crate graph runs
//! `gnitz-sql → gnitz-core`; SQL at this seam would invert that.
//!
//! A local reply is a **single-worker** reply, and the existing client-side
//! finishing accepts it unchanged: neither the rows finisher nor the aggregate
//! finisher mentions a frame count or a worker count. The remote path
//! establishes that by concatenating every frame's batch before returning one; a
//! local reply is already one.
//!
//! **The read path encodes and decodes; the ingest path does not, and the
//! asymmetry is not an oversight.** A read has to end in a `ZSetBatch` whatever
//! happens, because that is what the finishers consume, so the engine→wire→client
//! conversion is work the remote path pays too and the mirror merely relocates.
//! An applied delta ends in an engine `Batch`, so a detour through `ZSetBatch`
//! would be a conversion *added* by mirroring. What the mirror does not relocate
//! is the staging: the server streams its encode out in frame-sized chunks,
//! where this holds the keeper, its whole wire block and the decoded batch live
//! at once. Narrow the bound and it is nothing; full-scan a large view and the
//! peak is three copies of the result where a remote client holds one.

use std::sync::Arc;

use gnitz_core::protocol::decode_wal_block;
use gnitz_core::{ClientError, IndexMeta, ReadTarget, RelKind, Schema, ZSetBatch};
use gnitz_engine::schema::SchemaDescriptor;
use gnitz_engine::storage::Batch;

use crate::handle::Mirror;
use crate::register::descriptor_of;

impl ReadTarget for Mirror {
    /// Forwarded to the client the handle owns.
    ///
    /// The bracket is on the trait because dropping it would cost the delegated
    /// path a round trip: `resolve_relation` populates the statement scope by
    /// name and `table_indexes` reads it back by id, so with no scope an
    /// unmirrored relation read through here costs two RESOLVEs where the plain
    /// planner costs one.
    fn begin_statement(&mut self) {
        self.client.begin_statement();
    }

    fn end_statement(&mut self) {
        self.client.end_statement();
    }

    /// A mirrored name resolves out of its registration, and **that is what
    /// keeps a mirrored SELECT round-trip-free at all**: the client's own
    /// resolve caches into a statement scope that is dropped whole, so it is one
    /// RESOLVE per relation per statement by design. Delegating it would put a
    /// round trip back into every mirrored SELECT.
    ///
    /// What that trades is the staleness of the name → id binding, and it is the
    /// staleness the mirror already has: a mirrored read answers as of the last
    /// poll, and the binding falls under that rather than being an exception to
    /// it. The detector is the feed — the next poll's tag stops continuing — and
    /// the recovery re-resolves before it reseeds.
    fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<(Arc<Schema>, RelKind), ClientError> {
        // The local catalog's own name index answers this: the mirror registered
        // the relation through it, under the same lower-cased spelling and the
        // same `"schema.name"` key the server's catalog uses. A mirrored view
        // owns no secondary index either, so `RelKind` is complete here.
        let qname = gnitz_core::qualified_name(schema_name, name);
        if let Some(view) = self
            .engine
            .entity_id_by_qname(&qname)
            .and_then(|tid| self.views.get(&(tid as u64)))
        {
            return Ok((Arc::clone(&view.schema), view.kind));
        }
        self.client.resolve_relation(schema_name, name)
    }

    fn scan(&mut self, table_id: u64) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), ClientError> {
        if !self.mirrors(table_id) {
            return ReadTarget::scan(&mut self.client, table_id);
        }
        self.check_poison().map_err(as_client_error)?;
        let view_schema = Arc::clone(&self.views[&table_id].schema);
        let (batch, desc) = self
            .engine
            .scan_family(table_id as i64)
            .map_err(ClientError::ServerError)?;
        let rows = reply_batch(&batch, &desc, table_id, &view_schema)?;
        Ok((Some(view_schema), Some(rows)))
    }

    /// Run the spec against the local copy through the engine's own executor,
    /// and reply through the same encode the worker runs.
    ///
    /// The tick cut it takes is the master's, which only a `Delta` bound reads;
    /// `0` is passed, and a `Delta` bound never reaches it — registration writes
    /// `delta_bytes = 0`, and a `Delta` bound is refused against a relation whose
    /// feed is absent at every round.
    fn scan_spec(
        &mut self,
        table_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<Option<ZSetBatch>, ClientError> {
        if !self.mirrors(table_id) {
            return self.client.scan_spec(table_id, spec, reply_schema);
        }
        self.check_poison().map_err(as_client_error)?;
        let spec = gnitz_wire::ReadSpec::decode(spec)
            .map_err(|e| ClientError::ServerError(format!("mirror: read spec: {e}")))?;
        let reply_desc = descriptor_of(reply_schema).map_err(|e| ClientError::ServerError(e.to_string()))?;
        let keeper = self
            .engine
            .scan_spec_family(table_id as i64, &spec, &reply_desc, 0)
            .map_err(|f| ClientError::ServerError(f.text))?;
        if keeper.count == 0 {
            return Ok(None);
        }
        Ok(Some(reply_batch(&keeper, &reply_desc, table_id, reply_schema)?))
    }

    /// A mirrored view owns no secondary index — not a simplification but a
    /// fact: only a base table may own one, so the empty list is exact. That is
    /// also what keeps a descriptor-cache miss from turning the planner's index
    /// probe into a round trip of its own.
    fn table_indexes(&mut self, table_id: u64) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
        if self.mirrors(table_id) {
            return Ok(Arc::new(Vec::new()));
        }
        self.client.table_indexes(table_id)
    }
}

/// Turn an engine `Batch` into the `ZSetBatch` the client finishers consume, by
/// the same wire block a remote reply would have carried.
///
/// The decode is the client's own block decoder — the step `parse_response`
/// calls — so a local reply and a remote one cannot be decoded by different
/// rules.
fn reply_batch(
    batch: &Batch,
    desc: &SchemaDescriptor,
    table_id: u64,
    schema: &Schema,
) -> Result<ZSetBatch, ClientError> {
    let mut block = vec![0u8; batch.wire_byte_size()];
    let written = batch.encode_to_wire(table_id as u32, &mut block, 0, false);
    debug_assert_eq!(written, block.len(), "wire_byte_size must size its own encode");
    debug_assert_eq!(
        desc.num_columns(),
        schema.num_columns(),
        "a local reply must be encoded under the schema it is decoded against",
    );
    let (rows, _) = decode_wal_block(&block, schema).map_err(|e| ClientError::ServerError(e.to_string()))?;
    Ok(rows)
}

fn as_client_error(e: crate::MirrorError) -> ClientError {
    ClientError::ServerError(e.to_string())
}
