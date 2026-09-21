//! The meta-schema record: this crate's adapter to the shared codec.
//!
//! The record's layout and every rule about what makes one admissible live in
//! `gnitz_wire::schema_block` — the one implementation the client runs too.
//! What stays here is the translation between a [`SchemaDescriptor`] (+ the
//! [`ColumnDef`]s that name it) and the codec's neutral per-column facts.
//!
//! The decode half is `gnitz_store::schema::decode_schema_block`, one module down:
//! `ColumnDef` lives above `schema` in the layering, so these encoders cannot
//! join it there without forking the column projection.

use super::{CatalogEngine, ColumnDef};
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
                meta: ColMeta {
                    nullable: col.nullable != 0,
                    hidden: def.is_some_and(|d| d.is_hidden),
                    serial: def.is_some_and(|d| d.is_serial),
                    scale: def.map_or(0, |d| d.scale),
                },
                name: def.map_or(&b""[..], |d| d.name.as_bytes()),
            }
        })
        .collect()
}

/// Encode `schema`'s physical column shape, without the names and catalog flags
/// nothing engine-side reads.
pub(crate) fn encode_schema_block(schema: &SchemaDescriptor) -> Vec<u8> {
    gnitz_wire::schema_block::encode(&schema_block_cols(schema, None), schema.pk_indices())
}

/// [`encode_schema_block`] plus the per-column catalog facts the descriptor
/// does not carry — name, `is_hidden`, `is_serial`. The record a *client*
/// decodes into a `Schema`, so it is the one that must be named.
pub(in crate::catalog) fn encode_named_schema_block(schema: &SchemaDescriptor, defs: &[ColumnDef]) -> Vec<u8> {
    gnitz_wire::schema_block::encode(&schema_block_cols(schema, Some(defs)), schema.pk_indices())
}

/// The schema version reported for an id no registered relation holds.
const UNREGISTERED_SCHEMA_VERSION: u16 = 1;

impl CatalogEngine {
    /// The current schema version of `table_id`.
    pub(crate) fn schema_version_of(&self, table_id: i64) -> u16 {
        self.caches
            .relations
            .get(&table_id)
            .map_or(UNREGISTERED_SCHEMA_VERSION, |e| e.schema_version.get())
    }

    /// `tid`'s named schema record; `None` for an unregistered id.
    pub(crate) fn schema_block(&self, tid: i64) -> Option<Rc<Vec<u8>>> {
        self.caches.relations.get(&tid).map(|e| e.schema_block.clone())
    }

    /// The schema record a reply to a client at `client_version` carries, and the
    /// version its flags report.
    pub(crate) fn negotiated_schema_block(&self, tid: i64, client_version: u16) -> (Option<Rc<Vec<u8>>>, u16) {
        let Some(entry) = self.caches.relations.get(&tid) else {
            return (None, UNREGISTERED_SCHEMA_VERSION);
        };
        let version = entry.schema_version.get();
        let block = (client_version != version).then(|| entry.schema_block.clone());
        (block, version)
    }
}
