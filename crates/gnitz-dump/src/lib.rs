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
//! | `post-data.sql` | `CREATE VIEW` in creation order, then `CREATE INDEX` |
//!
//! Every part is read from one snapshot, so a dump is one cut of the whole
//! database however long it takes and whatever is written meanwhile. Nothing of
//! the engine's own encodings is in it — no ids, no circuits, no shard bytes —
//! so a build whose formats moved on restores it; a view comes back from its
//! recorded SQL, planned anew, under the names its sources carry now.
//!
//! A restore runs `schema.sql`, pushes each table's rows — a table that
//! references itself in an order every row's parents arrive no later than it —
//! puts each SERIAL sequence where the dump left it, and runs `post-data.sql`,
//! so a view fills once from its loaded sources. It refuses a database holding
//! any name the dump creates. Streams hold no rows, view contents are derived,
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
    Catalog, CatalogRelation, ClientError, GnitzClient, RelDescriptor, RelName, RelProps, Schema, ZSetBatch,
    DEFAULT_PAGE_ROWS,
};
use gnitz_wire::{WireConflictMode, SYSTEM_SCHEMA};

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
    let serials: std::collections::HashMap<u64, u64> = client.snapshot_serials(snapshot).await?.into_iter().collect();
    let mut relations: Vec<&CatalogRelation> = catalog.relations.iter().filter(|r| r.schema != SYSTEM_SCHEMA).collect();
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
    let (mut views, mut indexes) = (Vec::new(), Vec::new());
    fs::create_dir(dir.join("data"))?;
    for rel in relations {
        let secondary = rel
            .indexes
            .iter()
            .filter(|i| matches!(rel.props, RelProps::View(_)) || !i.is_unique);
        indexes.extend(secondary.map(|i| gnitz_sql::create_index_statement(rel, i)));
        match rel.props {
            RelProps::Table(props) => {
                pre.push(gnitz_sql::create_table_statement(&catalog, rel)?);
                let mut entry = TableEntry {
                    schema: rel.schema.clone(),
                    name: rel.name.clone(),
                    file: None,
                    rows: 0,
                    serial: serials.get(&rel.id).copied().filter(|&last| props.serial && last > 0),
                };
                if !props.stream {
                    let file = format!("data/{}.parquet", manifest.tables.len());
                    entry.rows = dump_rows(client, snapshot, rel, &dir.join(&file)).await?;
                    entry.file = Some(file);
                }
                manifest.tables.push(entry);
            }
            RelProps::View(_) => {
                views.push(gnitz_sql::create_view_statement(&catalog, rel)?);
                manifest.views.push(ViewEntry {
                    schema: rel.schema.clone(),
                    name: rel.name.clone(),
                });
            }
        }
    }
    fs::write(dir.join("schema.sql"), script(&pre))?;
    views.extend(indexes);
    fs::write(dir.join("post-data.sql"), script(&views))?;
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
            writer
                .write(&batch)
                .map_err(|e| ClientError::from(format!("{}: {e}", rel.qualified())))
        })
        .await?;
    writer.finish()
}

/// Restore the dump in `dir` into the database `client` is connected to.
///
/// Refused before anything is written when the database holds a name the dump
/// creates. Past that, each statement and each push commits on its own, so a
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

    gnitz_sql::execute(client, gnitz_wire::PUBLIC_SCHEMA, &pre)
        .await
        .map_err(|e| DumpError(format!("schema.sql: {e}")))?;
    let catalog = client.catalog().await?;
    for t in &manifest.tables {
        let name = RelName::new(&t.schema, &t.name)?;
        let desc = client.resolve_relation(&name).await?;
        if let Some(file) = &t.file {
            let rows = restore_rows(client, &catalog, &desc, &dir.join(file)).await?;
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
    let edges: Vec<(usize, usize)> = catalog
        .relation(desc.tid)
        .map(|rel| {
            rel.columns
                .iter()
                .enumerate()
                .filter_map(|(ci, c)| {
                    c.fk.filter(|fk| fk.table_id == desc.tid)
                        .map(|fk| (ci, fk.col as usize))
                })
                .collect()
        })
        .unwrap_or_default();
    let mut rows = 0;
    if edges.is_empty() {
        while let Some(batch) = reader.next_group()? {
            rows += batch.len() as u64;
            push_all(client, desc, batch, None).await?;
        }
        return Ok(rows);
    }
    // Ordering needs the whole table at once.
    let mut all = ZSetBatch::new(&desc.schema);
    while let Some(batch) = reader.next_group()? {
        all.extend_from_owned(batch);
    }
    let rows = all.len() as u64;
    let order = order::order(&all, &desc.schema, &edges);
    push_all(client, desc, all, Some(order)).await?;
    Ok(rows)
}

/// Push the rows of `batch` into `desc` — in `order` where one is given — as
/// many pushes as their bytes take.
async fn push_all(
    client: &mut GnitzClient,
    desc: &RelDescriptor,
    batch: ZSetBatch,
    order: Option<Vec<u32>>,
) -> Result<(), DumpError> {
    let row_bytes = batch.pks.stride() + 16 + batch.payload.iter().map(|c| c.stride()).sum::<usize>();
    let bytes = |b: &ZSetBatch| b.blob.len() + b.len() * row_bytes;
    let order = match order {
        None if bytes(&batch) < PUSH_BYTES => {
            if !batch.is_empty() {
                client.push(desc, &desc.schema, batch, WireConflictMode::Error).await?;
            }
            return Ok(());
        }
        None => (0..batch.len() as u32).collect(),
        Some(order) => order,
    };
    let mut chunk = ZSetBatch::new(&desc.schema);
    for i in order {
        chunk.copy_row_at(&batch, i as usize, 1);
        if bytes(&chunk) >= PUSH_BYTES {
            let full = std::mem::replace(&mut chunk, ZSetBatch::new(&desc.schema));
            client.push(desc, &desc.schema, full, WireConflictMode::Error).await?;
        }
    }
    if !chunk.is_empty() {
        client.push(desc, &desc.schema, chunk, WireConflictMode::Error).await?;
    }
    Ok(())
}
