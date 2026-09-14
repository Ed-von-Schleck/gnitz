//! Answering a read off a copy.
//!
//! The store takes an **encoded `ReadSpec` plus a reply schema** — the wire read
//! verbs, not SQL. The engine has no parser and the SQL crate is where a query
//! becomes a spec, and the crate graph runs `gnitz-sql → gnitz-core`; SQL at this
//! seam would invert that.
//!
//! A local reply is a **single-worker** reply, and the existing client-side
//! finishing accepts it unchanged: neither the rows finisher nor the aggregate
//! finisher mentions a frame count or a worker count. The remote path
//! establishes that by concatenating every frame's batch before returning one; a
//! local reply is already one.
//!
//! A local reply decodes the engine batch's regions through the client's own
//! block decoder, so a local and a remote reply are decoded by one rule, with no
//! wire buffer between.

use gnitz_core::protocol::decode_regions_into;
use gnitz_core::{MirrorError, Schema, ZSetBatch};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;

use crate::handle::{engine, Mirror};
use crate::register::descriptor_of;

impl Mirror {
    /// Every row of the copy, decoded under `schema` — the client-side schema
    /// its registration resolved.
    pub(crate) fn scan_inner(&mut self, table_id: u64, schema: &Schema) -> Result<ZSetBatch, MirrorError> {
        let (batch, desc) = self.registry.scan(table_id as i64, None).map_err(engine)?;
        reply_batch(&batch, &desc, schema)
    }

    /// Run the spec against the copy through the engine's own executor, and reply
    /// through the client's block decoder.
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
            .scan_spec(table_id as i64, &spec, &reply_desc, 0, None)
            .map_err(engine)?;
        reply_batch(&keeper, &reply_desc, reply_schema)
    }
}

/// The engine batch as the `ZSetBatch` the client finishers consume, decoded from its own
/// regions by the client's block decoder.
fn reply_batch(batch: &Batch, desc: &SchemaDescriptor, schema: &Schema) -> Result<ZSetBatch, MirrorError> {
    debug_assert_eq!(
        desc.num_columns(),
        schema.num_columns(),
        "a local reply must be produced under the schema it is decoded against",
    );
    let mut regions: [&[u8]; gnitz_wire::MAX_WIRE_REGIONS] = [&[]; gnitz_wire::MAX_WIRE_REGIONS];
    let n = batch.fill_regions(&mut regions);
    let mut rows = ZSetBatch::new(schema);
    decode_regions_into(&mut rows, &regions[..n], batch.len(), schema)
        .map_err(|e| MirrorError::Engine(e.to_string()))?;
    Ok(rows)
}
