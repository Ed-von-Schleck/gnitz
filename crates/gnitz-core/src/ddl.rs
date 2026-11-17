//! Catalog DDL as a client writes it: every create, drop and alter is one
//! bundle of system-family rows, committed as one DDL zone.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use gnitz_wire::{
    Circuit, CircuitRow, ColTabRow, ColTabSlot, ColumnDef, FkAction, FkRef, IdxTabRow, IdxTabSlot, KeyRange, PkColList,
    RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, ReadBound, ReadSpec, SchemaTabRow, SchemaTabSlot, SysFamily, SysRow,
    TableProps, TableTabRow, ViewParts, ViewProps, ViewTabRow, payload_bytes, payload_str, payload_u64,
};

use crate::catalog::{CATALOG_FAMILIES, Catalog, IndexRow, idx_rows};
use crate::client::GnitzClient;
use crate::connection::{RelDescriptor, Request, Target};
use crate::error::{ClientError, absent, not_found};
use crate::{BatchAppender, PkColumn, ProtocolError, RelName, Schema, ZSetBatch, sys_schema};

/// The index in `schemas`, SCHEMA_TAB's rows, of the live row named
/// `schema_name` (already canonical).
fn schema_row(schemas: &ZSetBatch, schema_name: &str) -> Option<usize> {
    schemas
        .live_rows()
        .find(|&i| payload_bytes(schemas, i, SchemaTabSlot::name as usize) == schema_name.as_bytes())
}

/// The id of the live schema `schema_name` (already canonical) in `schemas`.
fn live_schema_id(schemas: &ZSetBatch, schema_name: &str) -> Option<u64> {
    schema_row(schemas, schema_name).map(|at| gnitz_wire::sys_leading_id(schemas.pks.get_bytes(at)))
}

/// The catalog name of an unnamed index on `cols` of `table`, whose columns are
/// `columns`: `{schema}__{table}__idx_{col1}_{col2}…`, suffixed `_{n}` (n ≥ 2)
/// where `taken` holds it.
fn auto_index_name(table: &RelName, columns: &[ColumnDef], cols: &[u32], taken: &HashSet<String>) -> String {
    let col_names: Vec<&str> = cols.iter().map(|&c| &*columns[c as usize].name).collect();
    let name = format!("{}__{}__idx_{}", table.schema(), table.name(), col_names.join("_"));
    // A quoted column name holds any byte; an index name holds identifier bytes.
    let base: String = name
        .bytes()
        .map(|b| {
            if gnitz_wire::is_valid_ident_char(b) {
                b.to_ascii_lowercase() as char
            } else {
                '_'
            }
        })
        .collect();
    if !taken.contains(&base) {
        return base;
    }
    (2u32..)
        .map(|n| format!("{base}_{n}"))
        .find(|candidate| !taken.contains(candidate))
        .expect("u32 range exhausted")
}

/// One inline `UNIQUE` constraint of a `CREATE TABLE`. `name` is the catalog
/// index name `DROP INDEX` matches; `None` takes an [`auto_index_name`] free of
/// every live index name and every other name in the bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineUniqueIndex {
    pub cols: PkColList,
    pub name: Option<String>,
}

/// Each of `specs`' catalog names: its written one, canonical, else an
/// [`auto_index_name`] free of `taken` — the live names and every written one —
/// and of each auto-name handed out before it. A written name is left as
/// written: a live collision with one is the engine's refusal.
fn inline_index_names(
    table: &RelName,
    columns: &[ColumnDef],
    specs: &[InlineUniqueIndex],
    mut taken: HashSet<String>,
) -> Result<Vec<String>, String> {
    let written: Vec<Option<String>> = specs
        .iter()
        .map(|spec| spec.name.as_deref().map(gnitz_wire::canonical_identifier).transpose())
        .collect::<Result<_, _>>()?;
    taken.extend(written.iter().flatten().cloned());
    Ok(specs
        .iter()
        .zip(written)
        .map(|(spec, written)| {
            written.unwrap_or_else(|| {
                let name = auto_index_name(table, columns, spec.cols.as_slice(), &taken);
                taken.insert(name.clone());
                name
            })
        })
        .collect())
}

/// A `CREATE TABLE`: the table, its FOREIGN KEY columns and its inline UNIQUE
/// indexes, which [`GnitzClient::create_table`] writes as one DDL bundle.
pub struct TableDef {
    pub name: RelName,
    pub schema: Schema,
    /// Each FOREIGN KEY column's target, by column index.
    pub fks: BTreeMap<usize, FkTarget>,
    pub props: TableProps,
    /// Each inline UNIQUE constraint's columns and the catalog name its index
    /// takes, if written.
    pub unique_indexes: Vec<InlineUniqueIndex>,
}

impl TableDef {
    /// `name` over `schema`, with no foreign key, unique index or option.
    pub fn new(name: RelName, schema: Schema) -> Self {
        TableDef {
            name,
            schema,
            fks: BTreeMap::new(),
            props: TableProps::default(),
            unique_indexes: Vec::new(),
        }
    }
}

/// The column a FOREIGN KEY column references. `SelfTable` names the table
/// being created, which has no id yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FkTarget {
    Table(FkRef),
    SelfTable { col: u32, on_delete: FkAction },
}

/// What [`GnitzClient::alter_column`] changes about one column: one cell of
/// its COL_TAB row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnChange<'a> {
    /// `ALTER TABLE … RENAME COLUMN … TO`.
    Rename(&'a str),
    /// `ALTER TABLE … DROP COLUMN`: the column stays physically present, so
    /// the table keeps its layout and comparator.
    Drop,
    /// `ALTER TABLE … ALTER COLUMN … DROP NOT NULL`: once the catalog reports
    /// the column nullable, `ZSetBatch::validate` permits a null bit there.
    DropNotNull,
}

/// The rows of one DDL transaction: one batch per system family it writes.
#[derive(Default)]
pub(crate) struct DdlBundle(Vec<(SysFamily, ZSetBatch)>);

impl DdlBundle {
    /// `family`'s batch, opened on first use — so a family nothing wrote to has
    /// no entry, and no family has two.
    fn batch(&mut self, family: SysFamily) -> &mut ZSetBatch {
        let at = match self.0.iter().position(|(f, _)| *f == family) {
            Some(at) => at,
            None => {
                self.0.push((family, ZSetBatch::new(sys_schema(family))));
                self.0.len() - 1
            }
        };
        &mut self.0[at].1
    }

    /// `scanned`'s row `i`, a stored `family` row, at `-1`.
    fn retract(&mut self, family: SysFamily, scanned: &ZSetBatch, i: usize) {
        self.batch(family).copy_row_at(scanned, i, -1);
    }

    /// `row` at `weight`, in its own family's batch.
    fn put<R: SysRow>(&mut self, row: &R, weight: i64) {
        row.write(&mut BatchAppender::new(self.batch(R::FAMILY)), weight);
    }

    /// The relations whose descriptor the bundle changes: every row names its
    /// relation in its leading key column, an index row in `owner_id`.
    fn relations_written(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();
        for (family, rows) in &self.0 {
            for i in 0..rows.len() {
                ids.insert(match *family {
                    SysFamily::Index => payload_u64(rows, i, IdxTabSlot::owner_id as usize),
                    _ => gnitz_wire::sys_leading_id(rows.pks.get_bytes(i)),
                });
            }
        }
        ids
    }

    /// Each view the bundle retracts, with the name it writes it back under
    /// — a rename — or `None` — a drop.
    pub(crate) fn views_moved(&self) -> Vec<(u64, Option<String>)> {
        let Some((_, b)) = self.0.iter().find(|(family, _)| *family == SysFamily::View) else {
            return Vec::new();
        };
        let vid = |i| gnitz_wire::sys_leading_id(b.pks.get_bytes(i));
        (0..b.len())
            .filter(|&i| b.weights[i] < 0)
            .map(vid)
            .map(|v| {
                let new = b.live_rows().find(|&j| vid(j) == v).map(|j| {
                    payload_str(b, j, RELTAB_PAY_NAME)
                        .expect("a bundle this client built names its views")
                        .into_owned()
                });
                (v, new)
            })
            .collect()
    }

    /// Whether `other` holds the same live rows, family for family.
    fn same_rows(&self, other: &DdlBundle) -> bool {
        // A German cell's offset is relative to the arena of the batch it sits
        // in, so each side is copied onto an arena laid out in key order.
        fn in_key_order(rows: &ZSetBatch) -> ZSetBatch {
            let mut live: Vec<u32> = rows.live_rows().map(|i| i as u32).collect();
            live.sort_unstable_by_key(|&i| rows.pks.get_bytes(i as usize));
            rows.gathered(&live)
        }
        self.0.len() == other.0.len()
            && self.0.iter().all(|(family, rows)| {
                other
                    .0
                    .iter()
                    .any(|(f, theirs)| f == family && in_key_order(rows) == in_key_order(theirs))
            })
    }
}

/// The symbolic id by which a [`ViewBundle`] circuit's `ScanDelta` names
/// `segments[j]`. No durable relation id reaches
/// [`gnitz_wire::CATALOG_ID_CEILING`], so it is no relation's own.
pub fn segment_id(j: u64) -> u64 {
    gnitz_wire::CATALOG_ID_CEILING + j
}

/// One view in a [`ViewBundle`].
pub struct PlannedView {
    pub circuit: Circuit,
    pub schema: Arc<Schema>,
    /// [`ViewTabRow::pk_repeats`], stated by the emitter that minted the key.
    pub pk_repeats: bool,
}

/// A [`GnitzClient::create_view_chain`] bundle: the user-named view and the
/// hidden segments it owns, in dependency order. [`segment_id`]`(j)` names
/// `segments[j]`.
pub struct ViewBundle {
    pub name: RelName,
    /// The user-named view's options; `None` where the statement states none:
    /// the standing view's when replacing, the defaults for a new view.
    pub props: Option<ViewProps>,
    /// The id of the view the chain supersedes in the same zone.
    pub replacing: Option<u64>,
    pub segments: Vec<PlannedView>,
    pub view: PlannedView,
    /// The encoded record of the user-named view's body that its row stores,
    /// verbatim: opaque to this crate and to the engine.
    pub definition: Vec<u8>,
}

impl ViewBundle {
    /// The chain's catalog rows at `+1`, its first member standing at `base`:
    /// each segment named `_seg{vid}` and owned by the user-named view,
    /// which stands last under its own name and props, recording `definition`.
    fn put_rows(&self, b: &mut DdlBundle, definition: &[u8], schema_id: u64, base: u64) {
        let owner_vid = base + self.segments.len() as u64;
        for (pv, vid) in self.segments.iter().chain([&self.view]).zip(base..) {
            let mut circuit = pv.circuit.clone();
            // A `segment_id` past the chain's end becomes an id no lower than the
            // user-named view's own, which the engine refuses.
            for src in circuit.sources_mut() {
                if let Some(j) = src.checked_sub(gnitz_wire::CATALOG_ID_CEILING) {
                    *src = base + j;
                }
            }
            // A segment's name is unique because vids are, and no SQL surface can
            // spell it because gnitz_wire::validate_user_identifier rejects a leading
            // `_`; the owner_view_id column records its ownership.
            let (name, owner, props, definition) = if vid == owner_vid {
                (
                    self.name.name().to_string(),
                    None,
                    self.props.unwrap_or_default(),
                    definition,
                )
            } else {
                (format!("_seg{vid}"), Some(owner_vid), ViewProps::default(), &[][..])
            };

            // A foreign key constrains a base table, not a view.
            append_col_rows(b, vid, pv.schema.columns(), &BTreeMap::new());
            let circuit = circuit.encode();
            b.put(
                &CircuitRow {
                    view_id: vid,
                    circuit: circuit.as_slice().into(),
                },
                1,
            );
            // The VIEW_TAB register hook triggers server-side compilation.
            b.put(
                &ViewTabRow::of(
                    vid,
                    schema_id,
                    name,
                    PkColList::from_slice(pv.schema.pk_cols()),
                    ViewParts { props, owner, pk_repeats: pv.pk_repeats },
                    definition,
                ),
                1,
            );
        }
    }
}

impl GnitzClient {
    // ── Indexes and the catalog ────────────────────────────────────────────

    /// Index `cols` of `owner`, named `owner_name`, in that order, under the
    /// catalog name `name`. With no name it takes the first free
    /// [`auto_index_name`], and is refused where an index of `owner` on `cols`
    /// already serves it, whatever that index is named: unique where `is_unique`.
    pub async fn create_index(
        &mut self,
        owner_name: &RelName,
        owner: &RelDescriptor,
        cols: PkColList,
        name: Option<&str>,
        is_unique: bool,
    ) -> Result<u64, ClientError> {
        let (index_name, index_id) = match name {
            Some(name) => (gnitz_wire::canonical_identifier(name)?, self.alloc_ids(1).await?),
            None => {
                let ([indexes], index_id) = self.cut_and_ids([SysFamily::Index], 1).await?;
                let live = idx_rows(&indexes)?;
                let serves = |r: &IndexRow| r.owner == owner.tid && r.cols == cols && (r.is_unique || !is_unique);
                if let Some((_, same)) = live.iter().find(|(_, r)| serves(r)) {
                    return Err(ClientError::from(format!(
                        "an index on these columns already exists as '{}'",
                        same.name
                    )));
                }
                let taken: HashSet<String> = live.into_iter().map(|(_, r)| r.name).collect();
                let name = auto_index_name(owner_name, owner.schema.columns(), cols.as_slice(), &taken);
                (name, index_id)
            }
        };
        let mut b = DdlBundle::default();
        b.put(
            &IdxTabRow {
                index_id,
                owner_id: owner.tid,
                source_col_idx: cols,
                name: index_name.as_str().into(),
                is_unique,
            },
            1,
        );
        self.commit_ddl(b).await?;
        Ok(index_id)
    }

    /// Drop indexes by name as **one** DDL zone: the whole set retires or none of
    /// it does, and a name repeated in `index_names` retires once.
    pub async fn drop_indexes_by_name(&mut self, index_names: &[&str], if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(index_names, "index", if_exists, |_| true).await
    }

    /// `ALTER TABLE … DROP CONSTRAINT`: the UNIQUE index `name` of table `tid`.
    /// The `-1` is the stored row, so the engine's CAS re-proves owner and
    /// uniqueness against the live row.
    pub async fn drop_unique_constraint(&mut self, tid: u64, name: &str, if_exists: bool) -> Result<(), ClientError> {
        self.drop_index_rows(&[name], "constraint", if_exists, |r| r.owner == tid && r.is_unique)
            .await
    }

    /// Retract the live IDX_TAB rows named in `names` that pass `matches`.
    async fn drop_index_rows(
        &mut self,
        names: &[&str],
        noun: &'static str,
        if_exists: bool,
        matches: impl Fn(&IndexRow) -> bool,
    ) -> Result<(), ClientError> {
        let scanned = self.sys_rows(SysFamily::Index, ReadBound::None).await?;
        let rows = idx_rows(&scanned)?;
        let mut b = DdlBundle::default();
        let mut retracted = Vec::new();
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            match rows.iter().find(|(_, r)| r.name == name && matches(r)) {
                Some(&(i, _)) if retracted.contains(&i) => {}
                Some(&(i, _)) => {
                    retracted.push(i);
                    b.retract(SysFamily::Index, &scanned, i);
                }
                None if if_exists => {}
                None => return Err(not_found(noun, name)),
            }
        }
        self.commit_ddl(b).await
    }

    /// Every live secondary index. Names come back canonical (lowercase): every
    /// writer folds them at store time.
    pub async fn index_rows(&mut self) -> Result<Vec<IndexRow>, ClientError> {
        let scanned = self.sys_rows(SysFamily::Index, ReadBound::None).await?;
        Ok(idx_rows(&scanned)?.into_iter().map(|(_, r)| r).collect())
    }

    /// The whole catalog, read at one cut.
    pub async fn catalog(&mut self) -> Result<Catalog, ClientError> {
        let all = ReadSpec::all_rows(ReadBound::None);
        let families = self.sys_cut(CATALOG_FAMILIES, &all).await?;
        Ok(Catalog::decode(&families)?)
    }

    // ── Schemas and relations ──────────────────────────────────────────────

    /// Commit `bundle` as one DDL zone; refused inside a transaction. One that
    /// holds no row opens no zone.
    async fn commit_ddl(&mut self, bundle: DdlBundle) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::from("DDL is not allowed inside a transaction"));
        }
        if bundle.0.is_empty() {
            return Ok(());
        }
        self.ack(Request::DdlTxn(&bundle.0)).await?;
        let written = bundle.relations_written();
        self.kept.retain(|_, rel| !written.contains(&rel.tid));
        self.ddl_committed(&bundle).await;
        Ok(())
    }

    /// `CREATE SCHEMA [IF NOT EXISTS]`. With `if_not_exists` and the schema
    /// standing, the id fetched alongside the probe goes unused: a gap in the
    /// id sequence and nothing more.
    pub async fn create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<(), ClientError> {
        // Refuses the empty string, a leading `_` (the reserved system prefix)
        // and illegal characters.
        let name = gnitz_wire::canonical_identifier(name)?;
        let schema_id = if if_not_exists {
            let ([schemas], fresh) = self.cut_and_ids([SysFamily::Schema], 1).await?;
            if live_schema_id(&schemas, &name).is_some() {
                return Ok(());
            }
            fresh
        } else {
            self.alloc_ids(1).await?
        };
        let mut b = DdlBundle::default();
        b.put(&SchemaTabRow { schema_id, name: name.as_str().into() }, 1);
        self.commit_ddl(b).await
    }

    /// `DROP SCHEMA`: the schemas in `names`, as one DDL bundle. Under
    /// `cascade` the bundle takes every table and view in them too, and a
    /// relation outside them that depends on one inside fails it, so nothing
    /// is dropped; without, the engine refuses a schema still holding a
    /// relation. A missing schema is not found, or skipped under `if_exists`.
    pub async fn drop_schema(&mut self, names: &[&str], cascade: bool, if_exists: bool) -> Result<(), ClientError> {
        let all = ReadSpec::all_rows(ReadBound::None);
        let [schemas, views, tables] = self
            .sys_cut([SysFamily::Schema, SysFamily::View, SysFamily::Table], &all)
            .await?;
        let mut dropped = Vec::new();
        for name in names {
            let name = gnitz_wire::canonical_identifier(name)?;
            match schema_row(&schemas, &name) {
                Some(at) if dropped.contains(&at) => {}
                Some(at) => dropped.push(at),
                None if if_exists => {}
                None => return Err(not_found("schema", name)),
            }
        }

        let mut b = DdlBundle::default();
        if cascade {
            let ids: Vec<u64> = dropped
                .iter()
                .map(|&at| gnitz_wire::sys_leading_id(schemas.pks.get_bytes(at)))
                .collect();
            for (family, scanned) in [(SysFamily::View, views), (SysFamily::Table, tables)] {
                for i in scanned.live_rows() {
                    if ids.contains(&payload_u64(&scanned, i, RELTAB_PAY_SCHEMA_ID)) {
                        b.retract(family, &scanned, i);
                    }
                }
            }
        }
        for at in dropped {
            b.retract(SysFamily::Schema, &schemas, at);
        }
        self.commit_ddl(b).await
    }

    /// Register `def` as one DDL bundle and return the table's id.
    pub async fn create_table(&mut self, def: &TableDef) -> Result<u64, ClientError> {
        let TableDef {
            name: table,
            schema,
            fks,
            props,
            unique_indexes,
        } = def;
        let pk = PkColList::from_slice(schema.pk_cols());
        if let Some(&col) = fks.keys().find(|&&col| col >= schema.columns().len()) {
            return Err(ClientError::from(format!(
                "create_table: a foreign key on column {col}, past the table's {} columns",
                schema.columns().len()
            )));
        }

        // The table's id, then one per inline UNIQUE index; the live index
        // names only where one is unnamed, which a replayed dump never is.
        let n = 1 + unique_indexes.len() as u64;
        let (schemas, live, new_tid) = if unique_indexes.iter().any(|spec| spec.name.is_none()) {
            let ([schemas, indexes], new_tid) = self.cut_and_ids([SysFamily::Schema, SysFamily::Index], n).await?;
            let live = idx_rows(&indexes)?.into_iter().map(|(_, r)| r.name).collect();
            (schemas, live, new_tid)
        } else {
            let ([schemas], new_tid) = self.cut_and_ids([SysFamily::Schema], n).await?;
            (schemas, HashSet::new(), new_tid)
        };
        let schema_id = live_schema_id(&schemas, table.schema()).ok_or_else(|| not_found("schema", table.schema()))?;
        let index_names = inline_index_names(table, schema.columns(), unique_indexes, live)?;

        let mut b = DdlBundle::default();
        append_col_rows(&mut b, new_tid, schema.columns(), fks);
        b.put(&TableTabRow::of(new_tid, schema_id, table.name(), pk, *props), 1);
        for (k, spec) in unique_indexes.iter().enumerate() {
            b.put(
                &IdxTabRow {
                    index_id: new_tid + 1 + k as u64,
                    owner_id: new_tid,
                    source_col_idx: spec.cols,
                    name: index_names[k].as_str().into(),
                    is_unique: true,
                },
                1,
            );
        }
        self.commit_ddl(b).await?;

        Ok(new_tid)
    }

    /// Drop tables as one DDL zone; the engine cascades each one's indexes off
    /// their owner. See `drop_relations` for the batch rules.
    pub async fn drop_table(&mut self, tables: &[RelName], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(SysFamily::Table, tables, if_exists).await
    }

    /// Create `bundle` in one atomic `DDL_TXN` and return its id. A view it
    /// replaces that already is `bundle` stays under its id, and nothing is
    /// written.
    pub async fn create_view_chain(&mut self, mut bundle: ViewBundle) -> Result<u64, ClientError> {
        let view = &bundle.name;
        let n_segments = bundle.segments.len() as u64;
        let n_views = n_segments + 1;

        let mut b = DdlBundle::default();
        let (schema_id, base) = match bundle.replacing {
            Some(vid) => {
                // Where `bundle` stands if it is the chain under `vid`: one
                // allocation holds a chain, the user-named view last.
                let base = vid.saturating_sub(n_segments);
                let members = ReadSpec::all_rows(ReadBound::Range(KeyRange::new(
                    PkColList::from_slice(&[0]),
                    &[],
                    base as u128,
                    vid as u128,
                )));
                let families = [SysFamily::View, SysFamily::Column, SysFamily::Circuit];
                let rows = self.sys_cut(families, &members).await?;
                let standing = DdlBundle(families.into_iter().zip(rows).collect());
                let views = &standing.0[0].1;
                let i = views
                    .live_rows()
                    .find(|&i| gnitz_wire::sys_leading_id(views.pks.get_bytes(i)) == vid)
                    .ok_or_else(|| not_found("view", view))?;
                let decode_err = |e| ProtocolError::DecodeError(format!("view {vid}: {e}"));
                let row = ViewTabRow::read(views, i).map_err(decode_err)?;
                let schema_id = row.schema_id;
                if bundle.props.is_none() {
                    bundle.props = Some(row.parts().map_err(decode_err)?.props);
                }

                // The definition says how the body was spelled, which is no
                // part of what the view is: another spelling of the same body
                // is the same view, and keeps the standing definition.
                let mut want = DdlBundle::default();
                bundle.put_rows(&mut want, &row.definition, schema_id, base);
                if want.same_rows(&standing) {
                    return Ok(vid);
                }
                // Only the user-named view, ahead of the new chain's `+1`s: the
                // engine cascades its segments.
                b.retract(SysFamily::View, views, i);
                (schema_id, self.alloc_ids(n_views).await?)
            }
            None => {
                let ([schemas], base) = self.cut_and_ids([SysFamily::Schema], n_views).await?;
                let schema_id =
                    live_schema_id(&schemas, view.schema()).ok_or_else(|| not_found("schema", view.schema()))?;
                (schema_id, base)
            }
        };

        bundle.put_rows(&mut b, &bundle.definition, schema_id, base);
        self.commit_ddl(b).await?;
        Ok(base + n_segments)
    }

    /// Drop views as one DDL zone; the engine cascades each one's hidden segments
    /// off `owner_view_id`, so the client never names a segment. See
    /// `drop_relations` for the batch rules.
    pub async fn drop_view(&mut self, views: &[RelName], if_exists: bool) -> Result<(), ClientError> {
        self.drop_relations(SysFamily::View, views, if_exists).await
    }

    /// Retire every named relation of `family` in **one** DDL zone: the whole set
    /// goes or none does, and a name repeated in `names` retires once. Every name
    /// is looked up in one cut of SCHEMA_TAB, TABLE_TAB and VIEW_TAB, matched as
    /// RESOLVE matches it: the live schema row by name, then the live relation row
    /// by schema id and name.
    ///
    /// `if_exists` answers a name that does not resolve — its schema missing
    /// included — and nothing else: a name held by the other family, a dependent
    /// view and an FK child outside the batch all still fail the statement.
    /// Without it, a missing schema is refused as a missing schema.
    async fn drop_relations(
        &mut self,
        family: SysFamily,
        names: &[RelName],
        if_exists: bool,
    ) -> Result<(), ClientError> {
        // Nothing to look up, or a transaction `commit_ddl` refuses: no cut.
        if names.is_empty() || self.txn.is_some() {
            return self.commit_ddl(DdlBundle::default()).await;
        }
        let all = ReadSpec::all_rows(ReadBound::None);
        let [schemas, tables, views] = self
            .sys_cut([SysFamily::Schema, SysFamily::Table, SysFamily::View], &all)
            .await?;
        let mut b = DdlBundle::default();
        for (k, name) in names.iter().enumerate() {
            // RelName compares by its folded `schema.name`, so a repeat spelled
            // differently is still a repeat; the cut would find its row again.
            if names[..k].contains(name) {
                continue;
            }
            let Some(sid) = live_schema_id(&schemas, name.schema()) else {
                if if_exists {
                    continue;
                }
                return Err(not_found("schema", name.schema()));
            };
            let held = [(SysFamily::Table, &tables), (SysFamily::View, &views)]
                .into_iter()
                .find_map(|(f, rows)| {
                    rows.live_rows()
                        .find(|&i| {
                            payload_u64(rows, i, RELTAB_PAY_SCHEMA_ID) == sid
                                && payload_bytes(rows, i, RELTAB_PAY_NAME) == name.name().as_bytes()
                        })
                        .map(|i| (f, rows, i))
                });
            match held {
                Some((f, rows, i)) if f == family => b.retract(family, rows, i),
                Some((f, ..)) => {
                    // A name held by the other kind — `DROP TABLE <view>` — is
                    // refused as such, never as a `not_found` a caller may read as
                    // "already gone".
                    let what = self.resolve(name).await?.map_or(f.row_noun(), |d| d.class.describe());
                    let noun = family.row_noun();
                    return Err(format!("'{name}' is a {what}; DROP {} requires a {noun}", noun.to_uppercase()).into());
                }
                None if if_exists => {}
                None => return Err(not_found(family.row_noun(), name)),
            }
        }
        self.commit_ddl(b).await
    }

    /// Rename `rel`: a `(-1, +1)` rewrite pair of its TABLE_TAB / VIEW_TAB row.
    pub async fn alter_rename_relation(&mut self, rel: &RelDescriptor, new_name: &str) -> Result<(), ClientError> {
        let new_name = gnitz_wire::canonical_identifier(new_name)?;
        let tid = rel.tid;
        self.rewrite_sys_row(
            rel.class.family(),
            &[tid as u128],
            || absent(format!("relation {tid} not found")),
            |b, row| b.set_string_cell(row, RELTAB_PAY_NAME, &new_name),
        )
        .await
    }

    /// Apply `change` to column `col_idx` of relation `tid`: a `(-1, +1)`
    /// COL_TAB rewrite pair, the one cell `change` names edited at `+1`.
    pub async fn alter_column(
        &mut self,
        tid: u64,
        col_idx: usize,
        change: ColumnChange<'_>,
    ) -> Result<(), ClientError> {
        self.rewrite_sys_row(
            SysFamily::Column,
            &[tid as u128, col_idx as u128],
            || absent(format!("column index {col_idx} not found on table {tid}")),
            |b, row| match change {
                ColumnChange::Rename(name) => b.set_string_cell(row, ColTabSlot::name as usize, name),
                ColumnChange::Drop => b.set_u64_cell(row, ColTabSlot::is_hidden as usize, 1),
                ColumnChange::DropNotNull => b.set_u64_cell(row, ColTabSlot::is_nullable as usize, 1),
            },
        )
        .await
    }

    /// `ALTER TABLE … ADD COLUMN`: `def` appended to `rel` after every physical
    /// column, dropped ones included.
    pub async fn alter_add_column(&mut self, rel: &RelDescriptor, def: &ColumnDef) -> Result<(), ClientError> {
        let (tid, col_idx) = (rel.tid, rel.schema.layout().num_columns());

        let mut b = DdlBundle::default();
        b.put(&ColTabRow::of(tid, col_idx as u64, def, None), 1);
        self.commit_ddl(b).await
    }

    /// Push a `(-1, +1)` rewrite pair on the live `family` row keyed `key`: the
    /// stored row at `-1`, and a copy of it at `+1` that `patch` edits in place.
    async fn rewrite_sys_row(
        &mut self,
        family: SysFamily,
        key: &[u128],
        missing: impl FnOnce() -> ClientError,
        patch: impl FnOnce(&mut ZSetBatch, usize),
    ) -> Result<(), ClientError> {
        let (scanned, i) = self.seek_sys_row(family, key, missing).await?;
        let mut b = DdlBundle::default();
        b.retract(family, &scanned, i);
        let rows = b.batch(family);
        rows.copy_row_at(&scanned, i, 1);
        patch(rows, 1);
        self.commit_ddl(b).await
    }

    // ── System-family reads ────────────────────────────────────────────────

    /// `families`' rows at one cut, in that order, and the first of `n` fresh
    /// catalog ids, asked for in one round trip. Ids a caller leaves untaken are
    /// a gap in the id sequence and nothing more.
    async fn cut_and_ids<const N: usize>(
        &mut self,
        families: [SysFamily; N],
        n: u64,
    ) -> Result<([ZSetBatch; N], u64), ClientError> {
        let ids = self.session.submit(Request::AllocIds(n));
        let cut = self.sys_cut(families, &ReadSpec::all_rows(ReadBound::None)).await?;
        Ok((cut, self.wait(ids).await?))
    }

    /// `families`' rows under `spec`, with no DDL between them.
    async fn sys_cut<const N: usize>(
        &mut self,
        families: [SysFamily; N],
        spec: &ReadSpec,
    ) -> Result<[ZSetBatch; N], ClientError> {
        let scans = families.map(|family| (Target::from(family), spec, sys_schema(family)));
        let mut replies = self.scan_many(&scans).await?.into_iter();
        Ok(std::array::from_fn(|_| {
            replies.next().expect("one reply per scan").batch
        }))
    }

    /// System family `family`'s rows under `bound`, decoded under its own schema;
    /// the server checks the reply against that schema's layout.
    async fn sys_rows(&mut self, family: SysFamily, bound: ReadBound) -> Result<ZSetBatch, ClientError> {
        Ok(self
            .scan_served(family, &ReadSpec::all_rows(bound), sys_schema(family))
            .await?
            .batch)
    }

    /// The live `family` row keyed `key` — its PK columns' native values in
    /// PK-list order — by one master-local keyed read: the
    /// reply batch and the row's index in it, for a caller to copy the stored row
    /// out of. `PkSet` is exact, so a live row in the reply is that key's; none is
    /// `missing()`.
    async fn seek_sys_row(
        &mut self,
        family: SysFamily,
        key: &[u128],
        missing: impl FnOnce() -> ClientError,
    ) -> Result<(ZSetBatch, usize), ClientError> {
        let mut keys = PkColumn::empty_for_schema(sys_schema(family));
        keys.push_natives(key);
        let batch = self.sys_rows(family, ReadBound::PkSet(keys.keys())).await?;
        let i = batch.live_rows().next().ok_or_else(missing)?;
        Ok((batch, i))
    }
}

/// Append one `COL_TAB` row per column of `owner_id`, at `+1`, each carrying
/// its target in `fks`.
fn append_col_rows(b: &mut DdlBundle, owner_id: u64, columns: &[ColumnDef], fks: &BTreeMap<usize, FkTarget>) {
    for (i, cd) in columns.iter().enumerate() {
        let fk = fks.get(&i).map(|&t| match t {
            FkTarget::Table(fk) => fk,
            FkTarget::SelfTable { col, on_delete } => FkRef { table_id: owner_id, col, on_delete },
        });
        b.put(&ColTabRow::of(owner_id, i as u64, cd, fk), 1);
    }
}

#[cfg(test)]
#[path = "tests/ddl.rs"]
mod tests;
