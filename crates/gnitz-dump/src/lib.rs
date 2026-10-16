//! Logical dump and restore: a database written out as SQL and Parquet, and
//! read back into another — of this build or a later one.
//!
//! A dump is a directory:
//!
//! | File | Holds |
//! |---|---|
//! | `manifest.json` | The format version, and every schema, table and view the dump holds, each table with its rows' file and its SERIAL position |
//! | `schema.sql` | `CREATE SCHEMA` and `CREATE TABLE` in creation order: every table and stream, its key, foreign keys and unique constraints |
//! | `data/<n>.parquet` | One base table's rows ([`columns`] maps the types) |
//! | `post-data.sql` | Every table's secondary `CREATE INDEX`, then each view in creation order, its `CREATE VIEW` followed by its own `CREATE INDEX` |
//!
//! Every part is read from one snapshot, so a dump is one cut of the whole
//! database however long it takes and whatever is written meanwhile. Nothing of
//! the engine's own encodings is in it — no ids, no circuits, no shard bytes —
//! so a build whose formats moved on restores it; a view comes back from its
//! recorded SQL, planned anew, under the names its sources carry now.
//!
//! A restore runs `schema.sql`, pushes each table's rows — a table that
//! references itself and takes more than one push in an order where every
//! row's parents come no later than it — puts each SERIAL sequence where the
//! dump left it, and runs `post-data.sql`, so a view fills once from its loaded
//! sources and is planned with every index its sources have. It refuses a
//! database holding any schema, relation or index name the dump creates. Streams hold no rows, view contents are derived,
//! and a delta feed's retained rounds and a client's mirrors are no state of
//! the database, so none of them is dumped.

mod columns;
mod manifest;
mod order;

use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use gnitz_core::{
    Catalog, CatalogRelation, ClientError, GnitzClient, RelDescriptor, RelName, Schema, ZSetBatch, DEFAULT_PAGE_ROWS,
};
use gnitz_wire::{FixedInt, RelClass, WireConflictMode, SYSTEM_SCHEMA};

use columns::{TableReader, TableWriter};
use manifest::{Manifest, TableEntry, ViewEntry};

/// The bytes one restore push carries at most, well under a frame.
const PUSH_BYTES: usize = 16 << 20;

/// What went wrong, in words.
#[derive(Debug)]
pub struct DumpError(pub String);

impl fmt::Display for DumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DumpError {}

impl From<String> for DumpError {
    fn from(s: String) -> Self {
        DumpError(s)
    }
}

impl From<&str> for DumpError {
    fn from(s: &str) -> Self {
        DumpError(s.to_string())
    }
}

impl From<ClientError> for DumpError {
    fn from(e: ClientError) -> Self {
        DumpError(e.to_string())
    }
}

impl From<std::io::Error> for DumpError {
    fn from(e: std::io::Error) -> Self {
        DumpError(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for DumpError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        DumpError(format!("parquet: {e}"))
    }
}

impl From<gnitz_sql::GnitzSqlError> for DumpError {
    fn from(e: gnitz_sql::GnitzSqlError) -> Self {
        DumpError(e.to_string())
    }
}

/// What a dump wrote, or a restore read back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub schemas: usize,
    pub tables: usize,
    pub views: usize,
    pub rows: u64,
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} schema(s), {} table(s), {} view(s) and {} row(s)",
            self.schemas, self.tables, self.views, self.rows
        )
    }
}

impl Summary {
    fn of(manifest: &Manifest) -> Summary {
        Summary {
            schemas: manifest.schemas.len(),
            tables: manifest.tables.len(),
            views: manifest.views.len(),
            rows: manifest.tables.iter().map(|t| t.rows).sum(),
        }
    }
}

/// Dump the database `client` is connected to into `dir`, which must not exist
/// or be empty.
pub async fn dump(client: &mut GnitzClient, dir: &Path) -> Result<Summary, DumpError> {
    fs::create_dir_all(dir)?;
    if fs::read_dir(dir)?.next().is_some() {
        return Err(DumpError(format!("{} is not empty", dir.display())));
    }
    let snapshot = client.snapshot_open().await?;
    let dumped = dump_at(client, snapshot, dir).await;
    let closed = client.snapshot_close(snapshot).await;
    let summary = dumped?;
    closed?;
    Ok(summary)
}

async fn dump_at(client: &mut GnitzClient, snapshot: u64, dir: &Path) -> Result<Summary, DumpError> {
    let catalog = client.snapshot_catalog(snapshot).await?;
    let serials = client.snapshot_serials(snapshot).await?;
    let mut relations: Vec<&CatalogRelation> = catalog.relations.iter().collect();
    relations.sort_by_key(|r| r.id);

    let mut manifest = Manifest {
        schemas: catalog
            .schemas
            .iter()
            .filter(|s| *s != SYSTEM_SCHEMA && *s != gnitz_wire::PUBLIC_SCHEMA)
            .cloned()
            .collect(),
        ..Manifest::default()
    };
    let mut pre: Vec<String> = manifest
        .schemas
        .iter()
        .map(|s| gnitz_sql::create_schema_statement(s))
        .collect();
    // Every table's indexes ahead of every view, so each view is planned with
    // every index its sources have.
    let (mut table_indexes, mut views) = (Vec::new(), Vec::new());
    fs::create_dir(dir.join("data"))?;
    for rel in relations {
        let class = rel.class();
        if class == RelClass::Catalog {
            continue;
        }
        manifest.indexes.extend(rel.indexes.iter().map(|i| i.name.clone()));
        match class {
            RelClass::Catalog => unreachable!("skipped above"),
            RelClass::Table | RelClass::Stream => {
                pre.push(gnitz_sql::create_table_statement(&catalog, rel)?);
                table_indexes.extend(gnitz_sql::create_index_statements(rel));
                let mut entry = TableEntry {
                    schema: rel.schema.clone(),
                    name: rel.name.clone(),
                    file: None,
                    rows: 0,
                    serial: serials.get(&rel.id).copied(),
                };
                if class == RelClass::Table {
                    let file = format!("data/{}.parquet", manifest.tables.len());
                    entry.rows = dump_rows(client, snapshot, rel, &dir.join(&file))
                        .await
                        .map_err(|e| DumpError(format!("{}: {e}", rel.qualified())))?;
                    entry.file = Some(file);
                }
                manifest.tables.push(entry);
            }
            RelClass::View | RelClass::BoundedView | RelClass::FedView => {
                views.push(gnitz_sql::create_view_statement(&catalog, rel)?);
                views.extend(gnitz_sql::create_index_statements(rel));
                manifest.views.push(ViewEntry {
                    schema: rel.schema.clone(),
                    name: rel.name.clone(),
                });
            }
        }
    }
    fs::write(dir.join("schema.sql"), script(&pre))?;
    table_indexes.extend(views);
    fs::write(dir.join("post-data.sql"), script(&table_indexes))?;
    // Last, so a directory holding one is a whole dump.
    fs::write(dir.join("manifest.json"), manifest.to_json())?;
    Ok(Summary::of(&manifest))
}

/// `statements` as one script, each ended by `;`.
fn script(statements: &[String]) -> String {
    let mut out = String::from("-- Written by gnitz-dump.\n\n");
    for s in statements {
        out.push_str(s);
        out.push_str(";\n\n");
    }
    out
}

/// Every row of base table `rel` as `snapshot` holds it, into the file `path`.
async fn dump_rows(
    client: &mut GnitzClient,
    snapshot: u64,
    rel: &CatalogRelation,
    path: &Path,
) -> Result<u64, DumpError> {
    // The layout the store holds, dropped columns included; the file has the
    // visible ones.
    let columns: Vec<_> = rel.columns.iter().map(|c| c.def.clone()).collect();
    let schema = Arc::new(Schema::from_parts(columns, rel.pk.as_slice())?);
    let mut writer = TableWriter::create(path, &schema)?;
    client
        .snapshot_read(snapshot, rel.id, &schema, DEFAULT_PAGE_ROWS, |batch| {
            writer.write(&batch)
        })
        .await?;
    writer.finish()
}

/// Restore the dump in `dir` into the database `client` is connected to.
///
/// A table that references itself and takes more than one push arrives in an
/// order where every row's parents come no later than it, a reference cycle's
/// rows in one push. Refused before anything is written when the database holds
/// a schema, relation or index name the dump creates. Past that, each statement and each push commits on its own, so a
/// restore that fails part way leaves what it created: restore into a fresh
/// database again.
pub async fn restore(client: &mut GnitzClient, dir: &Path) -> Result<Summary, DumpError> {
    let read = |file: &str| fs::read_to_string(dir.join(file)).map_err(|e| DumpError(format!("{file}: {e}")));
    let manifest = Manifest::from_json(&read("manifest.json")?)?;
    let (pre, post) = (read("schema.sql")?, read("post-data.sql")?);

    let existing = client.catalog().await?;
    if let Some(s) = manifest.schemas.iter().find(|s| existing.schemas.contains(s)) {
        return Err(DumpError(format!("the database already holds schema {s}")));
    }
    let names = manifest
        .tables
        .iter()
        .map(|t| (&t.schema, &t.name))
        .chain(manifest.views.iter().map(|v| (&v.schema, &v.name)));
    for (schema, name) in names {
        if existing.get(&RelName::new(schema, name)?).is_some() {
            return Err(DumpError(format!("the database already holds {schema}.{name}")));
        }
    }
    let mut held = existing.relations.iter().flat_map(|r| &r.indexes);
    if let Some(i) = held.find(|i| manifest.indexes.contains(&i.name)) {
        return Err(DumpError(format!("the database already holds index {}", i.name)));
    }

    gnitz_sql::execute(client, gnitz_wire::PUBLIC_SCHEMA, &pre)
        .await
        .map_err(|e| DumpError(format!("schema.sql: {e}")))?;
    let catalog = client.catalog().await?;
    for t in &manifest.tables {
        let name = RelName::new(&t.schema, &t.name)?;
        let desc = client.resolve_relation(&name).await?;
        if let Some(file) = &t.file {
            let rows = restore_rows(client, &catalog, &desc, &dir.join(file))
                .await
                .map_err(|e| DumpError(format!("{file}: {e}")))?;
            if rows != t.rows {
                return Err(DumpError(format!("{file} holds {rows} rows, the manifest {}", t.rows)));
            }
        }
        if let Some(last) = t.serial {
            client.reserve_serial_exact(&desc, last).await?;
        }
    }
    gnitz_sql::execute(client, gnitz_wire::PUBLIC_SCHEMA, &post)
        .await
        .map_err(|e| DumpError(format!("post-data.sql: {e}")))?;
    Ok(Summary::of(&manifest))
}

/// Push the rows of the table file `path` into the restored table `desc`.
async fn restore_rows(
    client: &mut GnitzClient,
    catalog: &Catalog,
    desc: &RelDescriptor,
    path: &Path,
) -> Result<u64, DumpError> {
    let mut reader = TableReader::open(path, &desc.schema)?;
    let rel = catalog
        .relation(desc.tid)
        .ok_or_else(|| DumpError::from("its table is not in the restored catalog"))?;
    // The columns that reference this table's own key.
    let refs: Vec<usize> = rel
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.fk.is_some_and(|fk| fk.table_id == desc.tid))
        .map(|(ci, _)| ci)
        .collect();
    let schema = &desc.schema;
    let (mut rows, mut pending) = (0, ZSetBatch::new(schema));
    while let Some(group) = reader.next_group()? {
        rows += group.len() as u64;
        // A table that references itself is pushed whole or in its order, so it is held whole.
        if refs.is_empty()
            && !pending.is_empty()
            && push_bytes(schema, &pending) + push_bytes(schema, &group) > PUSH_BYTES
        {
            let full = std::mem::replace(&mut pending, ZSetBatch::new(schema));
            push_rows(client, desc, full, &refs).await?;
        }
        // Moves `group` when `pending` is empty.
        pending.extend_from_owned(group);
    }
    push_rows(client, desc, pending, &refs).await?;
    // The table was new, so a key the file holds twice replaced a row rather than being refused.
    let held = held_rows(client, rel).await?;
    if held != rows {
        return Err(DumpError(format!("{rows} rows, of which {held} have distinct keys")));
    }
    Ok(rows)
}

/// The bytes `batch` takes in a push in `schema`'s layout.
fn push_bytes(schema: &Schema, batch: &ZSetBatch) -> usize {
    batch.blob.len() + batch.len() * schema.layout().row_width()
}

/// Push `batch` into `desc`: whole when it fits one push, else in pushes of at
/// most `PUSH_BYTES`, each row after the rows it references through `refs`, and
/// a reference cycle's rows in one push.
async fn push_rows(
    client: &mut GnitzClient,
    desc: &RelDescriptor,
    batch: ZSetBatch,
    refs: &[usize],
) -> Result<(), DumpError> {
    let schema = &desc.schema;
    if batch.is_empty() {
        return Ok(());
    }
    if push_bytes(schema, &batch) <= PUSH_BYTES {
        client.push(desc, schema, batch, WireConflictMode::Update).await?;
        return Ok(());
    }
    let (order, cyclic) = order::order(&batch, schema, refs);
    let mut chunk = ZSetBatch::new(schema);
    for (at, &i) in order.iter().enumerate() {
        chunk.copy_row_at(&batch, i as usize, 1);
        let next = at + 1;
        // From `cyclic` on, the rows on a cycle and every row waiting on one go in one push.
        if next == cyclic || (next < cyclic && push_bytes(schema, &chunk) >= PUSH_BYTES) {
            let full = std::mem::replace(&mut chunk, ZSetBatch::new(schema));
            client.push(desc, schema, full, WireConflictMode::Update).await?;
        }
    }
    if !chunk.is_empty() {
        client.push(desc, schema, chunk, WireConflictMode::Update).await?;
    }
    Ok(())
}

/// The rows restored table `rel` holds.
async fn held_rows(client: &mut GnitzClient, rel: &CatalogRelation) -> Result<u64, DumpError> {
    let sql = format!("SELECT COUNT(*) FROM {}", gnitz_sql::qualified_name(rel));
    let results = gnitz_sql::execute(client, gnitz_wire::PUBLIC_SCHEMA, &sql).await?;
    let [gnitz_sql::SqlResult::Rows(reply)] = &results[..] else {
        return Err(DumpError::from("a count that answered no rows"));
    };
    let mut cells = reply.schema.visible_columns();
    match (cells.next(), cells.next(), reply.batch.len()) {
        (Some((ci, _)), None, 1) => {
            let count = reply
                .schema
                .layout()
                .locate(ci)
                .decode_i64(&reply.batch, 0, FixedInt::I64);
            Ok(count as u64)
        }
        _ => Err(DumpError::from("a count that is not one cell")),
    }
}
