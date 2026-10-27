use super::*;
use crate::test_support::rel;
use crate::{sys_schema, BatchAppender};
use gnitz_wire::sys_rows::{FkAction, FkRef, SysRow};
use gnitz_wire::{ColumnDef, ReadBound, TableDistribution, TypeCode, ViewBudget};
use std::num::NonZeroU64;

/// The [`CATALOG_FAMILIES`]' rows, as a cut reads them back.
struct Rows([ZSetBatch; CATALOG_FAMILIES.len()]);

impl Rows {
    fn new() -> Self {
        Rows(CATALOG_FAMILIES.map(|family| ZSetBatch::new(sys_schema(family))))
    }

    fn put<R: SysRow>(&mut self, row: &R, weight: i64) -> &mut Self {
        let at = CATALOG_FAMILIES.iter().position(|&f| f == R::FAMILY).unwrap();
        row.write(&mut BatchAppender::new(&mut self.0[at]), weight);
        self
    }

    fn schema(&mut self, schema_id: u64, name: &str) -> &mut Self {
        self.put(&SchemaTabRow { schema_id, name: name.into() }, 1)
    }

    fn columns(&mut self, owner_id: u64, cols: &[(&str, Option<FkRef>)]) -> &mut Self {
        for (i, &(name, fk)) in cols.iter().enumerate() {
            let def = ColumnDef::new(name, TypeCode::I64, i > 0);
            self.put(&ColTabRow::of(owner_id, i as u64, &def, fk), 1);
        }
        self
    }

    fn table(&mut self, table_id: u64, schema_id: u64, name: &str, props: TableProps) -> &mut Self {
        let row = TableTabRow {
            table_id,
            schema_id,
            name: name.into(),
            pk_col_idx: PkColList::from_slice(&[0]).pack(),
            flags: props.pack(),
        };
        self.put(&row, 1)
    }

    /// View `view_id`, a segment of `owner` when there is one, scanning `sources`.
    fn view(&mut self, view_id: u64, name: &str, owner: u64, props: ViewProps, sources: &[u64]) -> &mut Self {
        let (capacity_bytes, delta_bytes) = props.budget.row_words();
        let row = ViewTabRow {
            view_id,
            schema_id: 16,
            name: name.into(),
            pk_col_idx: PkColList::from_slice(&[0]).pack(),
            capacity_bytes,
            delta_bytes,
            owner_view_id: owner,
            pk_repeats: 0,
            compress: props.compress as u64,
            definition: Default::default(),
        };
        let mut circuit = Circuit::default();
        for &source in sources {
            circuit.input_delta(source, ReadBound::None);
        }
        let circuit = circuit.encode();
        self.put(&row, 1).put(
            &CircuitRow {
                view_id,
                circuit: circuit.as_slice().into(),
            },
            1,
        )
    }

    fn decode(&self) -> Catalog {
        Catalog::decode(&self.0).unwrap()
    }
}

fn names<'a>(rels: impl Iterator<Item = &'a CatalogRelation>) -> Vec<String> {
    rels.map(CatalogRelation::qualified).collect()
}

/// Every class a RESOLVE answers, from the row that states it.
#[test]
fn a_relation_is_classed_by_its_own_row() {
    let fed = ViewProps {
        budget: ViewBudget::Fed {
            delta_bytes: NonZeroU64::new(1 << 20).unwrap(),
        },
        compress: false,
    };
    let mut rows = Rows::new();
    rows.schema(1, SYSTEM_SCHEMA)
        .schema(16, "app")
        .table(2, 1, "tables", TableProps::default())
        .table(17, 16, "t", TableProps::default())
        .table(18, 16, "events", TableProps { stream: true, ..Default::default() })
        .view(19, "v", 0, ViewProps::default(), &[17])
        .view(20, "feed", 0, fed, &[17]);
    let catalog = rows.decode();

    let class = |schema: &str, name: &str| catalog.get(&rel(schema, name)).unwrap().class();
    assert_eq!(class(SYSTEM_SCHEMA, "tables"), RelClass::Catalog);
    assert_eq!(class("app", "t"), RelClass::Table);
    assert_eq!(class("app", "events"), RelClass::Stream);
    assert_eq!(class("app", "v"), RelClass::View);
    assert_eq!(class("app", "feed"), RelClass::FedView);
    assert_eq!(catalog.schemas, ["_system", "app"]);
    assert_eq!(
        names(catalog.relations.iter()),
        ["_system.tables", "app.events", "app.feed", "app.t", "app.v"],
    );
}

/// A table's columns, key, foreign keys and indexes, each decoded from its own
/// family's rows.
#[test]
fn a_table_carries_its_columns_key_fks_and_indexes() {
    let fk = FkRef {
        table_id: 17,
        col: 0,
        on_delete: FkAction::Cascade,
    };
    let props = TableProps {
        distribution: TableDistribution::Replicated,
        ..Default::default()
    };
    let mut rows = Rows::new();
    rows.schema(16, "app")
        .table(17, 16, "parent", TableProps::default())
        .columns(17, &[("id", None)])
        .table(18, 16, "child", props)
        .columns(18, &[("id", None), ("parent_id", Some(fk)), ("note", None)]);
    let index = IdxTabRow {
        index_id: 19,
        owner_id: 18,
        source_col_idx: PkColList::from_slice(&[2, 1]).pack(),
        name: "child_note".into(),
        is_unique: 1,
    };
    rows.put(&index, 1);
    let catalog = rows.decode();

    let child = catalog.get(&rel("app", "child")).unwrap();
    assert_eq!(child.props, RelProps::Table(props));
    let cols: Vec<_> = child.columns.iter().map(|c| (c.def.name.as_str(), c.fk)).collect();
    assert_eq!(cols, [("id", None), ("parent_id", Some(fk)), ("note", None)]);
    assert_eq!(child.column_names(&child.pk), ["id"]);
    let [ix] = &child.indexes[..] else {
        panic!("{:?}", child.indexes)
    };
    assert_eq!((ix.name.as_str(), ix.is_unique), ("child_note", true));
    assert_eq!(child.column_names(&ix.cols), ["note", "parent_id"]);
    assert!(catalog.get(&rel("app", "parent")).unwrap().indexes.is_empty());
}

/// A view's segments are its own: it reads what its chain scans from outside
/// it, and no segment is a relation of its own.
#[test]
fn a_view_reads_through_its_chain() {
    let mut rows = Rows::new();
    rows.schema(16, "app")
        .table(17, 16, "a", TableProps::default())
        .table(18, 16, "b", TableProps::default())
        .view(19, "_seg19", 21, ViewProps::default(), &[17])
        .view(20, "_seg20", 21, ViewProps::default(), &[19, 18])
        .view(21, "joined", 0, ViewProps::default(), &[20])
        .view(22, "over", 0, ViewProps::default(), &[21, 17]);
    let catalog = rows.decode();

    let reads = |name: &str| catalog.get(&rel("app", name)).unwrap().reads.clone();
    assert_eq!(reads("joined"), [17, 18]);
    assert_eq!(reads("over"), [17, 21]);
    assert!(reads("a").is_empty());
    assert_eq!(names(catalog.readers(17)), ["app.joined", "app.over"]);
    assert_eq!(names(catalog.readers(21)), ["app.over"]);
    assert!(catalog.relation(19).is_none(), "a segment is no relation");
}

/// A row whose words do not decode fails the read, naming the relation.
#[test]
fn an_undecodable_row_fails_the_read() {
    let mut rows = Rows::new();
    let props = TableProps {
        stream: true,
        serial: true,
        ..Default::default()
    };
    rows.schema(16, "app").table(17, 16, "bad", props);
    let err = Catalog::decode(&rows.0).unwrap_err().to_string();
    assert!(err.contains("table 'bad'") && err.contains("SERIAL"), "{err}");
}

/// A relation's columns must state every position once, in order: a gap would
/// shift each later column under the key and index positions naming it.
#[test]
fn a_column_gap_fails_the_read() {
    let mut rows = Rows::new();
    rows.schema(16, "app").table(17, 16, "t", TableProps::default());
    let def = ColumnDef::new("late", TypeCode::I64, false);
    rows.put(&ColTabRow::of(17, 1, &def, None), 1);
    let err = Catalog::decode(&rows.0).unwrap_err().to_string();
    assert!(
        err.contains("column 1 of 17") && err.contains("expected index 0"),
        "{err}"
    );
}
