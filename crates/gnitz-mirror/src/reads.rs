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
use gnitz_core::{ClientError, GnitzClient, ReadTarget, RelDescriptor, Schema, ZSetBatch};
use gnitz_engine::schema::SchemaDescriptor;
use gnitz_engine::storage::Batch;

use crate::handle::Mirror;
use crate::register::descriptor_of;

impl ReadTarget for Mirror {
    fn client_mut(&mut self) -> &mut GnitzClient {
        &mut self.client
    }

    /// A mirrored name resolves out of its registration, which is what keeps a
    /// mirrored SELECT round-trip-free: the client's own resolve caches into a
    /// statement scope that is dropped whole, so it is one RESOLVE per relation
    /// per statement. Delegating would put that round trip back into every
    /// mirrored SELECT.
    ///
    /// The trade is the staleness of the name → id binding, which is the
    /// staleness the mirror already has: a mirrored read answers as of the last
    /// poll. The detector is the feed (the next poll's tag stops continuing) and
    /// the recovery re-resolves before it reseeds. Answered off the registration
    /// rather than [`Mirror::readable`], so a name whose copy is not valid still
    /// binds locally and only the read it feeds goes upstream.
    ///
    /// The descriptor is the one registration resolved upstream, returned as it
    /// arrived rather than rebuilt field by field.
    fn describe_relation(&mut self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        // The local catalog's own name index answers this: the mirror registered
        // the relation through it, under the same lower-cased spelling and the
        // same `"schema.name"` key the server's catalog uses.
        let qname = gnitz_core::qualified_name(schema_name, name);
        if let Some(view) = self
            .engine
            .entity_id_by_qname(&qname)
            .and_then(|tid| self.views.get(&(tid as u64)))
        {
            let desc = Arc::clone(&view.desc);
            self.client.record_relation(schema_name, name, Some(Arc::clone(&desc)));
            return Ok(Some(desc));
        }
        ReadTarget::describe_relation(&mut self.client, schema_name, name)
    }

    fn scan(&mut self, table_id: u64) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), ClientError> {
        let Some(view) = self.readable(table_id) else {
            return ReadTarget::scan(&mut self.client, table_id);
        };
        let view_schema = Arc::clone(&view.desc.schema);
        self.check_poison()?;
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
        self.check_poison()?;
        let spec = gnitz_wire::ReadSpec::decode(spec)
            .map_err(|e| ClientError::ServerError(format!("mirror: read spec: {e}")))?;
        let reply_desc = descriptor_of(reply_schema)?;
        let keeper = self
            .engine
            .scan_spec_family(table_id as i64, &spec, &reply_desc, 0)
            .map_err(|f| ClientError::ServerError(f.text))?;
        if keeper.count == 0 {
            return Ok(None);
        }
        Ok(Some(reply_batch(&keeper, &reply_desc, table_id, reply_schema)?))
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
