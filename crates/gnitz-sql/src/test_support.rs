//! Shared `#[cfg(test)]` helpers: column, schema, catalog and batch builders,
//! and the parse/bind entry points the unit tests and the suites drive.

use crate::catalog::Catalog;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{PlannedView, RelDescriptor, RelName, Schema, ZSetBatch};
use gnitz_expr::{CmpOp, ColumnLocator};
use gnitz_wire::payload_u64;
use gnitz_wire::{ColType, ColumnDef, FixedInt, PkColList, RelClass, RelIndex, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder};
use sqlparser::ast::Expr;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// The schema every test catalog resolves under.
pub(crate) const SN: &str = "s";

/// A NOT NULL column.
pub(crate) fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// A nullable column.
pub(crate) fn ncol(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, true)
}

/// `[_k hidden, id, a, ID hidden, A]` — a visible name shared with a hidden
/// column, and a visible name written twice (as a `SELECT *` join view carries).
pub(crate) fn shadowed_cols() -> Vec<ColumnDef> {
    let u = |n| col(n, TypeCode::U64);
    vec![u("_k").hidden(), u("id"), u("a"), u("ID").hidden(), u("A")]
}

/// A schema, held to the rules every decoded schema is.
pub(crate) fn schema(columns: Vec<ColumnDef>, pk_cols: &[u32]) -> Schema {
    Schema::from_parts(columns, pk_cols).expect("a valid test schema")
}

/// A non-unique secondary index over `cols`.
pub(crate) fn ix(cols: &[u32]) -> RelIndex {
    RelIndex {
        cols: PkColList::from_slice(cols),
        is_unique: false,
    }
}

/// A unique secondary index over `cols`.
pub(crate) fn uq(cols: &[u32]) -> RelIndex {
    RelIndex { is_unique: true, ..ix(cols) }
}

/// A relation descriptor.
pub(crate) fn rel(
    tid: u64,
    class: RelClass,
    columns: Vec<ColumnDef>,
    pk_cols: Vec<u32>,
    indexes: Vec<RelIndex>,
) -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        tid,
        class,
        pk_repeats: class == RelClass::Stream,
        serial: false,
        schema: Arc::new(schema(columns, &pk_cols)),
        indexes,
        token: 0,
    })
}

/// A plain, partitioned, unindexed base table.
pub(crate) fn table(tid: u64, columns: Vec<ColumnDef>, pk_cols: Vec<u32>) -> Arc<RelDescriptor> {
    rel(tid, RelClass::Table, columns, pk_cols, Vec::new())
}

/// `name` in [`SN`].
pub(crate) fn in_sn(name: &str) -> RelName {
    RelName::new(SN, name).unwrap()
}

/// A catalog of exactly the relations inserted into it, with [`SN`] as the
/// session's schema; any other name is absent.
#[derive(Default)]
pub(crate) struct TestCatalog {
    rels: RefCell<HashMap<RelName, Arc<RelDescriptor>>>,
    /// The names probed, each once, in order.
    asked: RefCell<Vec<RelName>>,
}

impl TestCatalog {
    pub(crate) fn insert(&self, name: &RelName, desc: Arc<RelDescriptor>) {
        self.rels.borrow_mut().insert(name.clone(), desc);
    }

    /// The names probed since the last call.
    pub(crate) fn take_asked(&self) -> Vec<RelName> {
        self.asked.take()
    }
}

impl Catalog for TestCatalog {
    fn schema_name(&self) -> &str {
        SN
    }

    fn probe(&self, name: &RelName) -> Option<Arc<RelDescriptor>> {
        let mut asked = self.asked.borrow_mut();
        if !asked.contains(name) {
            asked.push(name.clone());
        }
        self.rels.borrow().get(name).cloned()
    }
}

/// A catalog holding `rels` under [`SN`]; any other name is absent.
pub(crate) fn catalog(rels: Vec<(&str, Arc<RelDescriptor>)>) -> TestCatalog {
    let cat = TestCatalog::default();
    for (name, desc) in rels {
        cat.insert(&in_sn(name), desc);
    }
    cat
}

/// Register `view` under `name` as a `class` relation, as the server would after
/// `CREATE VIEW`.
pub(crate) fn register(cat: &TestCatalog, name: &str, tid: u64, class: RelClass, view: &PlannedView) {
    cat.insert(
        &in_sn(name),
        Arc::new(RelDescriptor {
            tid,
            class,
            pk_repeats: view.pk_repeats,
            serial: false,
            schema: Arc::clone(&view.schema),
            indexes: Vec::new(),
            token: 0,
        }),
    );
}

/// `(id pk_tc PK, v I64)`.
pub(crate) fn pk_schema(pk_tc: TypeCode) -> Schema {
    schema(vec![col("id", pk_tc), col("v", TypeCode::I64)], &[0])
}

/// `(pk U64 PK, val val_tc nullable)`.
pub(crate) fn two_col(val_tc: TypeCode) -> Schema {
    schema(vec![col("pk", TypeCode::U64), ncol("val", val_tc)], &[0])
}

/// `(pk U64 PK, c1, c2, …)`, the payload columns of types `tys`, all nullable.
pub(crate) fn typed_schema<T: Into<ColType> + Copy>(tys: &[T]) -> Arc<Schema> {
    let mut columns = vec![col("pk", TypeCode::U64)];
    columns.extend(
        tys.iter()
            .enumerate()
            .map(|(i, &ty)| ColumnDef::new(format!("c{}", i + 1), ty, true)),
    );
    Arc::new(schema(columns, &[0]))
}

/// Each comparison as SQL spells it, beside the operator that holds with the
/// operands transposed and the opcode operand it lowers to.
pub(crate) const CMP: [(&str, &str, CmpOp); 6] = [
    ("=", "=", CmpOp::Eq),
    ("<>", "<>", CmpOp::Ne),
    ("<", ">", CmpOp::Lt),
    ("<=", ">=", CmpOp::Le),
    (">", "<", CmpOp::Gt),
    (">=", "<=", CmpOp::Ge),
];

/// Whether `a op b`.
pub(crate) fn holds(op: CmpOp, a: i128, b: i128) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Le => a <= b,
        CmpOp::Gt => a > b,
        CmpOp::Ge => a >= b,
    }
}

/// A column type, literals to compare it against, and the integers its column
/// stores (`None` a NULL).
pub(crate) type ExactCase = (ColType, &'static [&'static str], &'static [Option<i128>]);

/// Columns compared against literals at and past their edges.
pub(crate) const EXACT_CASES: &[ExactCase] = {
    const P53: i128 = 1 << 53;
    &[
        (
            ColType::of(TypeCode::U64),
            &[
                "-5",
                "-1",
                "0",
                "1",
                "9223372036854775808",
                "18446744073709551615",
                "18446744073709551616",
            ],
            &[
                Some(0),
                Some(1),
                Some(1 << 63),
                Some((1 << 63) - 1),
                Some(u64::MAX as i128),
                None,
            ],
        ),
        (
            ColType::of(TypeCode::I8),
            &["-129", "-128", "127", "128"],
            &[Some(-128), Some(-1), Some(0), Some(127), None],
        ),
        (
            ColType::of(TypeCode::I64),
            &[
                "1.5",
                "-1.5",
                "2.5",
                "-2.5",
                "9007199254740993.5",
                "-9007199254740992.5",
            ],
            &[
                Some(-3),
                Some(-2),
                Some(-1),
                Some(1),
                Some(2),
                Some(3),
                Some(P53),
                Some(P53 + 1),
                Some(P53 + 2),
                Some(-P53 - 1),
                None,
            ],
        ),
        // A literal finer than a DECIMAL column's scale, one whose scale-up would
        // wrap an `i64` included.
        (
            ColType::decimal(2),
            &["1.005", "1.000000000000000001"],
            &[None, Some(100), Some(101), Some(1000), Some(-1000)],
        ),
    ]
};

/// The exact answer to `x op lit`, `x` an integer a `ty` column stores and `lit`
/// a numeric literal's text, compared as the rationals they are.
pub(crate) fn holds_exact(ty: ColType, x: i128, op: CmpOp, lit: &str) -> bool {
    let (mag, scale) = crate::literal::decimal_of_number_text(lit.trim_start_matches('-')).expect(lit);
    let v = if lit.starts_with('-') { -mag } else { mag };
    holds(
        op,
        x * 10i128.pow(u32::from(scale)),
        v * 10i128.pow(u32::from(ty.scale)),
    )
}

/// One payload cell, written into a batch or read back out of a result.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Cell<'a> {
    Int(i128),
    F64(f64),
    Str(&'a str),
    Null,
}

impl Cell<'_> {
    /// Append this cell as the row's next payload column.
    pub(crate) fn push(self, app: &mut BatchBuilder<&mut Batch>) {
        match self {
            Cell::Int(v) => app.put_int(v as u128),
            Cell::F64(v) => app.put_float(v),
            Cell::Str(s) => app.put_string(s),
            Cell::Null => app.put_null(),
        };
    }
}

/// A batch of `schema`: one row per `(key, weight, cells)`, the key as its columns'
/// natives, one cell per payload slot.
pub(crate) fn keyed_batch(schema: &Schema, rows: &[(&[u128], i64, &[Cell])]) -> ZSetBatch {
    let mut b = ZSetBatch::new(&Arc::new(schema.clone()));
    let mut app = b.builder();
    for &(key, w, cells) in rows {
        app.begin_row_natives(key, w);
        cells.iter().for_each(|c| c.push(&mut app));
    }
    b
}

/// A batch of `schema`, single-column PK: one weight-1 row per entry of `rows`,
/// its PK the row's 1-based position, its payload the entry's cells.
pub(crate) fn batch_of(schema: &Schema, rows: &[&[Cell]]) -> ZSetBatch {
    let keys: Vec<u128> = (1..=rows.len() as u128).collect();
    let rows: Vec<_> = keys
        .iter()
        .zip(rows)
        .map(|(k, &cells)| (std::slice::from_ref(k), 1, cells))
        .collect();
    keyed_batch(schema, &rows)
}

/// Each row of `batch` as its payload cells and weight — what [`batch_of`]
/// wrote, read back.
pub(crate) fn rows_of(batch: &ZSetBatch) -> Vec<(Vec<Cell<'_>>, i64)> {
    let locs = batch.schema().layout().payload_locators();
    let b = &batch.view();
    let cell = |r: usize, pi: usize, loc: &ColumnLocator| {
        let tc = loc.type_code();
        if loc.is_null(b, r) {
            Cell::Null
        } else if tc.is_german_string() {
            let cell = b.col_data(pi)[r * 16..(r + 1) * 16]
                .try_into()
                .expect("a German cell is 16 bytes");
            Cell::Str(std::str::from_utf8(gnitz_wire::german_string_content(cell, b.blob())).unwrap())
        } else if tc == TypeCode::F64 {
            Cell::F64(f64::from_bits(payload_u64(b, r, pi)))
        } else {
            let fi = FixedInt::from_type_code(tc).expect("an integer column");
            let v = loc.decode_i64(b, r, fi);
            Cell::Int(if fi.is_signed() { v as i128 } else { v as u64 as i128 })
        }
    };
    (0..b.len())
        .map(|r| {
            (
                locs.iter().enumerate().map(|(pi, loc)| cell(r, pi, loc)).collect(),
                b.get_weight(r),
            )
        })
        .collect()
}

/// Parse + bind an expression against `schema` as relation `t`, which is what
/// every qualified reference in these tests writes; a refusal panics, naming `sql`.
pub(crate) fn bind_sql(sql: &str, schema: &Schema) -> BoundExpr {
    crate::bind::bind_single_table(&parse_expr_sql(sql), schema, "t").unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The message of the rejection `r` must be.
pub(crate) fn rejected<T>(r: Result<T, GnitzSqlError>) -> String {
    match r {
        Err(e) => e
            .rejection()
            .unwrap_or_else(|| panic!("expected a rejection, got {e:?}"))
            .to_owned(),
        Ok(_) => panic!("expected a rejection, got Ok"),
    }
}

/// Assert `r` is a rejection whose message contains `want` — the guard's
/// identity, not its wording. `what` labels the failure (the SQL, usually).
pub(crate) fn assert_rejects<T>(what: &str, r: Result<T, GnitzSqlError>, want: &str) {
    let msg = match r {
        Err(e) => match e.rejection() {
            Some(m) => m.to_owned(),
            None => panic!("`{what}`: expected a rejection, got {e:?}"),
        },
        Ok(_) => panic!("`{what}` was accepted"),
    };
    assert!(
        msg.contains(want),
        "for `{what}`\n  expected substring: {want:?}\n  got: {msg:?}"
    );
}

/// [`bind_sql`] of a WHERE predicate, as its bound conjuncts.
pub(crate) fn bind_where(sql: &str, schema: &Schema) -> Vec<BoundExpr> {
    bind_sql(sql, schema).conjuncts()
}

/// [`bind_where`] for a predicate that is one conjunct — the input of the
/// single-conjunct recognizers.
pub(crate) fn bind_conjunct(sql: &str, schema: &Schema) -> BoundExpr {
    let mut conjuncts = bind_where(sql, schema);
    assert_eq!(conjuncts.len(), 1, "{sql}: one conjunct");
    conjuncts.pop().expect("one conjunct")
}

/// Parse a bare SQL expression (e.g. a WHERE predicate) via [`crate::GnitzDialect`].
pub(crate) fn parse_expr_sql(src: &str) -> Expr {
    use sqlparser::parser::Parser;
    Parser::new(&crate::GnitzDialect)
        .try_with_sql(src)
        .unwrap()
        .parse_expr()
        .unwrap()
}

/// The first statement of `sql`, parsed via [`crate::GnitzDialect`] — the same dialect
/// the planner parses with, so a test sees the AST shape the planner receives.
pub(crate) fn parse_stmt(sql: &str) -> sqlparser::ast::Statement {
    use sqlparser::parser::Parser;
    Parser::parse_sql(&crate::GnitzDialect, sql)
        .expect("parses")
        .into_iter()
        .next()
        .expect("one statement")
}

/// [`parse_stmt`] narrowed to the `Query` it must be.
pub(crate) fn parse_query(sql: &str) -> sqlparser::ast::Query {
    match parse_stmt(sql) {
        sqlparser::ast::Statement::Query(q) => *q,
        other => panic!("not a query: {other}"),
    }
}
