//! Logical dump and restore: a database written out as SQL and Parquet, and
//! read back into another — of this build or a later one.
//!
//! A dump is a directory:
//!
//! | File | Holds |
//! |---|---|
//! | `manifest.json` | The format version and the release that wrote it; the schemas, tables, views and index names the dump creates; each table with its rows' file, row count and SERIAL position |
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
//! A restore runs `schema.sql`, puts each SERIAL sequence where the dump left
//! it, pushes each table's rows, and runs `post-data.sql`, so a view fills
//! once from its loaded sources and is planned with every index its sources
//! have. Streams hold no rows, view contents are derived, and a delta feed's
//! retained rounds and a client's mirrors are no state of the database, so none
//! of them is dumped.

mod columns;
mod manifest;
mod order;

use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use gnitz_core::{CatalogRelation, ClientError, DEFAULT_PAGE_ROWS, GnitzClient, RelName, Schema, ZSetBatch};
use gnitz_foundation::posix_io::{create_dir_all_durable, fsync_dir, write_file_durable};
use gnitz_wire::{FixedInt, PUBLIC_SCHEMA, RelClass, SYSTEM_SCHEMA, WireConflictMode};

use columns::{TableReader, TableWriter};
use manifest::{Manifest, TableEntry, ViewEntry};

/// The bytes at which a restore push is cut. A push goes past them by at most
/// one row, or by one set of rows that reference one another round a cycle
/// (see [`order::order`]), which goes in one push whatever its size; a quarter
/// of a frame leaves room for that and the frame's headers.
const PUSH_BYTES: usize = gnitz_wire::MAX_FRAME_PAYLOAD / 4;

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
/// or be empty. `manifest.json` appears last, once every other file is
/// durable, so a directory without one is no dump, and a later dump refuses it
/// as not empty.
pub async fn dump(client: &mut GnitzClient, dir: &Path) -> Result<Summary, DumpError> {
    create_dir_all_durable(dir).map_err(|e| at(dir, e))?;
    if fs::read_dir(dir).map_err(|e| at(dir, e))?.next().is_some() {
        return Err(DumpError(format!("{} is not empty", dir.display())));
    }
    let data = dir.join("data");
    fs::create_dir(&data).map_err(|e| at(&data, e))?;
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
            .filter(|s| *s != SYSTEM_SCHEMA && *s != PUBLIC_SCHEMA)
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
    let (mut post, mut views) = (Vec::new(), Vec::new());
    // Each table entry's relation. A base table's rows are read once every
    // statement has rendered, so a relation that renders no statement refuses
    // the dump before any rows are read.
    let mut tables = Vec::new();
    for rel in relations {
        let class = rel.class();
        match class {
            RelClass::Catalog => continue,
            RelClass::Table | RelClass::Stream => {
                pre.push(gnitz_sql::create_table_statement(&catalog, rel)?);
                post.extend(gnitz_sql::create_index_statements(rel));
                tables.push(rel);
                manifest.tables.push(TableEntry {
                    schema: rel.schema.clone(),
                    name: rel.name.clone(),
                    file: (class == RelClass::Table).then(|| format!("data/{}.parquet", manifest.tables.len())),
                    rows: 0,
                    serial: serials.get(&rel.id).copied(),
                });
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
        manifest.indexes.extend(rel.indexes.iter().map(|i| i.name.clone()));
    }
    post.extend(views);
    for (entry, rel) in manifest.tables.iter_mut().zip(tables) {
        if let Some(file) = &entry.file {
            entry.rows = dump_rows(client, snapshot, rel, &dir.join(file))
                .await
                .map_err(|e| DumpError(format!("{}: {e}", rel.qualified())))?;
        }
    }
    let data = dir.join("data");
    fsync_dir(&data).map_err(|e| at(&data, e))?;
    let write = |name: &str, text: String| {
        let path = dir.join(name);
        write_file_durable(&path, text.as_bytes(), 0o644).map_err(|e| at(&path, e))
    };
    write("schema.sql", script(&pre))?;
    write("post-data.sql", script(&post))?;
    // Last, once every other file is durable, so a directory holding one is a whole dump.
    write("manifest.json", manifest.to_json())?;
    Ok(Summary::of(&manifest))
}

/// `e`, which happened at `path`.
fn at(path: &Path, e: std::io::Error) -> DumpError {
    DumpError(format!("{}: {e}", path.display()))
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
    // Dropped columns included; the file has the visible ones.
    let schema = stored_schema(rel)?;
    let mut writer = TableWriter::create(path, &schema)?;
    client
        .snapshot_read(snapshot, rel.id, &schema, DEFAULT_PAGE_ROWS, |batch| {
            writer.write(&batch)
        })
        .await?;
    writer.finish()
}

/// The layout table `rel`'s store holds its rows in.
fn stored_schema(rel: &CatalogRelation) -> Result<Arc<Schema>, DumpError> {
    let columns = rel.columns.iter().map(|c| c.def.clone()).collect();
    Ok(Arc::new(Schema::from_parts(columns, rel.pk.as_slice())?))
}

/// Restore the dump in `dir` into the database `client` is connected to.
///
/// Refused before anything is written when the database holds a schema,
/// relation or index name the dump creates. Past that, each statement and each
/// push commits on its own, so a restore that fails part way leaves what it
/// created: restore into a fresh database again. A table that references
/// itself is held whole in memory, and each set of its rows that reference one
/// another round a cycle goes in one push beside up to a quarter of a frame of
/// other rows, so must fit in the rest of the frame.
pub async fn restore(client: &mut GnitzClient, dir: &Path) -> Result<Summary, DumpError> {
    let read = |file: &str| {
        let path = dir.join(file);
        fs::read_to_string(&path).map_err(|e| at(&path, e))
    };
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

    gnitz_sql::execute(client, PUBLIC_SCHEMA, &pre)
        .await
        .map_err(|e| DumpError(format!("schema.sql: {e}")))?;
    let catalog = client.catalog().await?;
    for t in &manifest.tables {
        let name = RelName::new(&t.schema, &t.name)?;
        let rel = catalog
            .get(&name)
            .ok_or_else(|| DumpError(format!("schema.sql created no {name}")))?;
        // Ahead of the rows: a reservation is acknowledged before it is
        // durable, and the durable push of any row holding one of its ids makes
        // it durable too.
        if let Some(last) = t.serial {
            client.reserve_serial_exact(rel.id, last).await?;
        }
        if let Some(file) = &t.file {
            let in_table = |e| DumpError(format!("{name} ({file}): {e}"));
            let rows = restore_rows(client, rel, &dir.join(file)).await.map_err(in_table)?;
            if rows != t.rows {
                return Err(in_table(DumpError(format!("{rows} rows, the manifest {}", t.rows))));
            }
        }
    }
    gnitz_sql::execute(client, PUBLIC_SCHEMA, &post)
        .await
        .map_err(|e| DumpError(format!("post-data.sql: {e}")))?;
    Ok(Summary::of(&manifest))
}

/// Push the rows of the table file `path` into restored table `rel`.
async fn restore_rows(client: &mut GnitzClient, rel: &CatalogRelation, path: &Path) -> Result<u64, DumpError> {
    let schema = &stored_schema(rel)?;
    let mut reader = TableReader::open(path, schema)?;
    // The columns that reference this table's own key.
    let refs: Vec<usize> = rel
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.fk.is_some_and(|fk| fk.table_id == rel.id))
        .map(|(ci, _)| ci)
        .collect();
    let (mut rows, mut chunk) = (0, ZSetBatch::new(schema));
    // Whole row groups coalesce into a push; one too large for a push is cut in
    // `order::order`'s order, and its last rows carry on into the next.
    while let Some(mut group) = reader.next_group()? {
        // A table that references itself is ordered whole, so it is one group.
        if !refs.is_empty() {
            while let Some(more) = reader.next_group()? {
                group.extend_from_owned(more);
            }
        }
        rows += group.len() as u64;
        if push_bytes(schema, &chunk) + push_bytes(schema, &group) > PUSH_BYTES {
            flush(client, rel.id, schema, &mut chunk).await?;
        }
        if push_bytes(schema, &group) <= PUSH_BYTES {
            chunk.extend_from_owned(group);
        } else {
            for (r, may_end) in order::order(&group, schema, &refs) {
                chunk.copy_row_at(&group, r as usize, 1);
                if may_end && push_bytes(schema, &chunk) >= PUSH_BYTES {
                    flush(client, rel.id, schema, &mut chunk).await?;
                }
            }
        }
    }
    flush(client, rel.id, schema, &mut chunk).await?;
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

/// Push `chunk`, of `schema`, into table `tid` when it holds rows, and leave it empty.
async fn flush(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    chunk: &mut ZSetBatch,
) -> Result<(), DumpError> {
    if !chunk.is_empty() {
        let full = std::mem::replace(chunk, ZSetBatch::new(schema));
        client.push(tid, schema, full, WireConflictMode::Update).await?;
    }
    Ok(())
}

/// The rows restored table `rel` holds.
async fn held_rows(client: &mut GnitzClient, rel: &CatalogRelation) -> Result<u64, DumpError> {
    let sql = format!("SELECT COUNT(*) FROM {}", gnitz_sql::qualified_name(rel));
    let results = gnitz_sql::execute(client, PUBLIC_SCHEMA, &sql).await?;
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
