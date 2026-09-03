//! Answering a read off a copy.
//!
//! The store takes an **encoded `ReadSpec` plus a reply schema** — the wire read
//! verbs, not SQL. The engine has no parser, the client core never constructs a
//! `ReadSpec` and the SQL crate does, and the crate graph runs
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
//! conversion is work the remote path pays too and the copy merely relocates. An
//! applied delta ends in an engine `Batch`, so a detour through `ZSetBatch` would
//! be a conversion *added* by mirroring. A full scan holds the keeper, its wire
//! block and the decoded batch at once; narrow the bound and it is nothing.

use gnitz_core::protocol::decode_wal_block;
use gnitz_core::{MirrorError, Schema, ZSetBatch};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;

use crate::handle::{engine, Mirror};
use crate::register::descriptor_of;

impl Mirror {
    /// Every row of the copy, decoded under `schema` — the client-side schema
    /// its registration resolved.
    pub(crate) fn scan_inner(&mut self, table_id: u64, schema: &Schema) -> Result<ZSetBatch, MirrorError> {
        let (batch, desc) = self.registry.scan_family(table_id as i64, None).map_err(engine)?;
        reply_batch(&batch, &desc, table_id, schema)
    }

    /// Run the spec against the copy through the engine's own executor, and reply
    /// through the same encode the worker runs.
    ///
    /// The tick cut it takes is the master's, which only a `Delta` bound reads;
    /// `0` is passed, and a `Delta` bound never reaches it — a copy is registered
    /// with no delta budget, and a `Delta` bound is refused against a relation
    /// whose feed is absent at every round.
    ///
    /// The hydrator is `None`, which the compiler can see: no copy holds a
    /// skeleton row, because none is registered with a capacity budget.
    pub(crate) fn scan_spec_inner(
        &mut self,
        table_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError> {
        let spec =
            gnitz_wire::ReadSpec::decode(spec).map_err(|e| MirrorError::Engine(format!("mirror: read spec: {e}")))?;
        let reply_desc = descriptor_of(reply_schema)?;
        let keeper = self
            .registry
            .scan_spec_family(table_id as i64, &spec, &reply_desc, 0, None)
            .map_err(engine)?;
        reply_batch(&keeper, &reply_desc, table_id, reply_schema)
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
) -> Result<ZSetBatch, MirrorError> {
    let block = batch.encode_to_wire_vec(table_id as u32, false);
    debug_assert_eq!(
        desc.num_columns(),
        schema.num_columns(),
        "a local reply must be encoded under the schema it is decoded against",
    );
    let (rows, _) = decode_wal_block(&block, schema).map_err(|e| MirrorError::Engine(e.to_string()))?;
    Ok(rows)
}
