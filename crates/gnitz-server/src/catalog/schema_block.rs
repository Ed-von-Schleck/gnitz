//! The meta-schema block: this crate's adapter to the shared codec.
//!
//! The block's layout and every rule about what makes one admissible live in
//! `gnitz_wire::schema_block` — the one implementation the client runs too.
//! What stays here is the translation between a [`SchemaDescriptor`] (+ the
//! [`ColumnDef`]s that name it) and the codec's neutral per-column facts.
//!
//! The decode half is `gnitz_store::schema::decode_schema_block`, one module down:
//! `ColumnDef` lives above `schema` in the layering, so these encoders cannot
//! join it there without forking the column projection.

use super::{CatalogEngine, ColumnDef, SchemaWireEntry};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_wire::schema_block::{ColMeta, SchemaBlockCol};
use std::rc::Rc;

/// One [`SchemaBlockCol`] per column: the physical shape from `schema`, and the
/// per-column catalog facts (name, `ColMeta::hidden`, `ColMeta::serial`, the
/// DECIMAL scale) from
/// `defs`.
///
/// `defs` is all-or-nothing, not per-column: a relation either has one COL_TAB
/// row per physical column — so `defs[ci]` describes `schema.columns[ci]` — or
/// it has none, and every name comes out empty with every catalog flag clear.
fn schema_block_cols<'a>(schema: &SchemaDescriptor, defs: Option<&'a [ColumnDef]>) -> Vec<SchemaBlockCol<'a>> {
    let ncols = schema.num_columns();
    debug_assert!(defs.is_none_or(|d| d.len() == ncols));
    (0..ncols)
        .map(|ci| {
            let col = &schema.columns[ci];
            let def = defs.map(|d| &d[ci]);
            SchemaBlockCol {
                type_code: col.type_code,
                // For compound PKs the position within `pk_indices()` is what
                // determines decode order — column order ≠ PK order in general
                // (e.g. `PRIMARY KEY (b, a)`). Carrying the position lets the
                // decoder rebuild `pk_indices` exactly as the user wrote them.
                meta: ColMeta {
                    nullable: col.nullable != 0,
                    hidden: def.is_some_and(|d| d.is_hidden),
                    serial: def.is_some_and(|d| d.is_serial),
                    scale: def.map_or(0, |d| d.scale),
                    pk_pos: schema
                        .pk_indices()
                        .iter()
                        .position(|&p| p as usize == ci)
                        .map(|p| p as u8),
                },
                name: def.map_or(&b""[..], |d| d.name.as_bytes()),
            }
        })
        .collect()
}

/// Encode `schema`'s physical column shape, without the names and catalog flags
/// nothing engine-side reads.
pub(crate) fn encode_schema_block(schema: &SchemaDescriptor, tid: u32) -> Vec<u8> {
    gnitz_wire::schema_block::encode(tid, &schema_block_cols(schema, None))
}

/// [`encode_schema_block`] plus the per-column catalog facts the descriptor
/// does not carry — name, `is_hidden`, `is_serial`. The block a *client*
/// decodes into a `Schema`, so it is the one that must be named.
pub(in crate::catalog) fn encode_named_schema_block(
    schema: &SchemaDescriptor,
    defs: &[ColumnDef],
    tid: u32,
) -> Vec<u8> {
    gnitz_wire::schema_block::encode(tid, &schema_block_cols(schema, Some(defs)))
}

impl CatalogEngine {
    /// The cached schema wire entry for `tid` — the encoded block, the schema
    /// version it was built at. On a miss the
    /// block is built from the catalog's column defs and stored; it is
    /// invalidated alongside them whenever DDL modifies the table.
    ///
    /// The caller supplies the resolved `schema`: what to do when `tid` names
    /// no relation differs per call site and stays with them.
    pub(crate) fn schema_wire_entry(&mut self, tid: i64, schema: &SchemaDescriptor) -> SchemaWireEntry {
        if let Some(cached) = self.get_cached_schema_wire_block(tid) {
            return cached;
        }
        // One COL_TAB row per physical column — a system family included, since
        // `bootstrap` writes each one's self-description — is what makes
        // `defs[ci]` describe `schema.columns[ci]`. A caller with a *projected*
        // schema builds a one-off anonymous block instead of coming here.
        let defs = self.read_column_defs(tid);
        let block = Rc::new(if defs.is_empty() {
            encode_schema_block(schema, tid as u32)
        } else {
            encode_named_schema_block(schema, &defs, tid as u32)
        });
        let entry = SchemaWireEntry {
            block,
            version: self.get_schema_version(tid),
        };
        self.set_schema_wire_block(tid, entry.clone());
        entry
    }

    /// The schema block a reply to a client at `client_version` carries, and the
    /// version its flags report. The one place that negotiation is written — the
    /// master reply path and the worker's both come here.
    ///
    /// `descriptor` is consulted only on a miss (as is `None` from it), so a
    /// warm reply pays no descriptor copy and no hash probe. Reading the version
    /// first is safe because [`Self::clear_col_cache_no_bump`] drops the cache
    /// entry *before* the bump, so a surviving entry always matches it.
    pub(crate) fn negotiated_schema_block(
        &mut self,
        tid: i64,
        client_version: u16,
        descriptor: impl FnOnce(&Self) -> Option<SchemaDescriptor>,
    ) -> (Option<Rc<Vec<u8>>>, u16) {
        let server_version = self.get_schema_version(tid);
        // Server versions start at 1 and wrap back to 1, so a client's 0 never matches.
        if client_version == server_version {
            return (None, server_version);
        }
        let block = descriptor(self).map(|s| self.schema_wire_entry(tid, &s).block);
        (block, server_version)
    }
}
