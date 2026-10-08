#![cfg(feature = "integration")]

//! gnitz-sql against a private engine, one module per surface.
//!
//! Everything a test asserts about data goes through [`Db::rows`], which reads
//! the result as a Z-set — each distinct row with its summed weight — so a lost
//! row, a duplicated one and a row at the wrong weight all read differently.

mod ddl;
mod dml;
mod placement;
mod resolve;
mod views;

use gnitz_core::block_on;
use std::rc::Rc;
use std::sync::Arc;

use gnitz_core::{ClientError, GnitzClient, RelDescriptor, RelName, ScanReply, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlResult};
use gnitz_test_harness::{unique_schema, ServerHandle};
use gnitz_wire::{FixedInt, TypeCode, WireFault, WireStatus};

/// `schema.name`.
pub fn rel(schema: &str, name: &str) -> RelName {
    RelName::new(schema, name).unwrap()
}

/// Who refused a statement: the planner, or the client or engine under the
/// status a caller branches on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Refusal {
    Rejected,
    Refused(WireStatus),
}
pub use Refusal::*;

/// What [`Db::rows`] reads a NULL cell as.
pub const NULL: i64 = i64::MIN;

/// A client on a private server, bound to a fresh schema.
pub struct Db {
    pub client: GnitzClient,
    pub sn: String,
    srv: Rc<ServerHandle>,
}

impl Db {
    pub fn boot(workers: usize) -> Db {
        Self::on(ServerHandle::start_n(workers))
    }

    pub fn boot_with_env(workers: usize, env: &[(&str, &str)]) -> Db {
        Self::on(ServerHandle::start_with_env(workers, env))
    }

    fn on(srv: ServerHandle) -> Db {
        let sn = unique_schema("s");
        let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
        block_on(client.create_schema(&sn)).unwrap();
        Db { client, sn, srv: Rc::new(srv) }
    }

    /// A second client on the same server and schema.
    pub fn peer(&self) -> Db {
        Db {
            client: GnitzClient::connect(self.srv.sock_path()).unwrap(),
            sn: self.sn.clone(),
            srv: Rc::clone(&self.srv),
        }
    }

    pub fn try_exec(&mut self, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
        block_on(gnitz_sql::execute(&mut self.client, &self.sn, sql))
    }

    /// Execute `sql`, returning its last statement's result.
    pub fn exec(&mut self, sql: &str) -> SqlResult {
        let mut out = self.try_exec(sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        out.pop().unwrap()
    }

    /// Assert `sql` is refused by `want` with a message containing `needle` —
    /// the guard's identity, not its wording.
    pub fn refuses(&mut self, sql: &str, want: Refusal, needle: &str) {
        let e = self
            .try_exec(sql)
            .err()
            .unwrap_or_else(|| panic!("`{sql}` was accepted"));
        let (got, msg) = match &e {
            GnitzSqlError::Rejected(m) => (Rejected, m.as_str()),
            GnitzSqlError::Client(ClientError::Refused(WireFault { status, text })) => {
                (Refused(*status), text.as_str())
            }
            other => panic!("`{sql}`: {other:?}"),
        };
        assert_eq!(got, want, "`{sql}`: {e:?}");
        assert!(msg.contains(needle), "`{sql}`: expected {needle:?} in {msg:?}");
    }

    pub fn affected(&mut self, sql: &str) -> usize {
        match self.exec(sql) {
            SqlResult::RowsAffected { count } => count,
            other => panic!("expected RowsAffected from `{sql}`, got {other:?}"),
        }
    }

    pub fn read(&mut self, sql: &str) -> (Arc<Schema>, ZSetBatch) {
        match self.exec(sql) {
            SqlResult::Rows(ScanReply { schema, batch, .. }) => (schema, batch),
            other => panic!("expected Rows from `{sql}`, got {other:?}"),
        }
    }

    /// The named columns of `sql`'s result as a Z-set: each distinct row once,
    /// its summed weight appended, zero-weight rows dropped, sorted. A NULL cell
    /// reads as [`NULL`]; a float cell must hold an integer.
    pub fn rows(&mut self, sql: &str, cols: &[&str]) -> Vec<Vec<i64>> {
        let (schema, batch) = self.read(sql);
        let idxs: Vec<usize> = cols.iter().map(|c| col_idx(&schema, c)).collect();
        let mut zset = std::collections::BTreeMap::<Vec<i64>, i64>::new();
        for r in 0..batch.len() {
            let row = idxs.iter().map(|&ci| cell(&schema, &batch, ci, r)).collect();
            *zset.entry(row).or_default() += batch.weights[r];
        }
        zset.into_iter()
            .filter(|&(_, w)| w != 0)
            .map(|(mut row, w)| {
                row.push(w);
                row
            })
            .collect()
    }

    /// [`Self::rows`] of `SELECT * FROM rel`.
    pub fn scan(&mut self, rel: &str, cols: &[&str]) -> Vec<Vec<i64>> {
        self.rows(&format!("SELECT * FROM {rel}"), cols)
    }

    /// `INSERT INTO table (cols) VALUES …` for integer row tuples.
    pub fn insert(&mut self, table: &str, cols: &[&str], rows: &[Vec<i64>]) {
        let vals: Vec<String> = rows
            .iter()
            .map(|r| format!("({})", r.iter().map(i64::to_string).collect::<Vec<_>>().join(", ")))
            .collect();
        self.exec(&format!(
            "INSERT INTO {table} ({}) VALUES {}",
            cols.join(", "),
            vals.join(", ")
        ));
    }

    pub fn rel(&mut self, name: &str) -> Arc<RelDescriptor> {
        block_on(self.client.resolve_relation(&rel(&self.sn, name))).unwrap()
    }

    /// Whether `name` resolves.
    pub fn exists(&mut self, name: &str) -> bool {
        block_on(self.client.resolve(&rel(&self.sn, name))).unwrap().is_some()
    }

    /// `name`'s secondary indexes as `(column names, is_unique)`, sorted.
    pub fn indexes(&mut self, name: &str) -> Vec<(Vec<String>, bool)> {
        let rel = self.rel(name);
        let mut out: Vec<_> = rel
            .indexes
            .iter()
            .map(|ix| {
                let cols = ix
                    .cols
                    .as_slice()
                    .iter()
                    .map(|&c| rel.schema.columns()[c as usize].name.clone())
                    .collect();
                (cols, ix.is_unique)
            })
            .collect();
        out.sort();
        out
    }
}

/// The user-visible column named `name`.
pub fn col_idx(schema: &Schema, name: &str) -> usize {
    schema
        .visible_columns()
        .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("column '{name}' not in {:?}", visible_names(schema)))
        .0
}

/// User-visible column names, lowercased.
pub fn visible_names(s: &Schema) -> Vec<String> {
    s.visible_columns().map(|(_, c)| c.name.to_lowercase()).collect()
}

/// Column `ci` of `row` as [`Db::rows`] reads it.
pub fn cell(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> i64 {
    let loc = schema.layout().locate(ci);
    if loc.is_null(batch, row) {
        return NULL;
    }
    let f = match schema.columns()[ci].ty.tc {
        TypeCode::F64 => f64::from_le_bytes(loc.bytes(batch, row).try_into().unwrap()),
        TypeCode::F32 => f32::from_le_bytes(loc.bytes(batch, row).try_into().unwrap()) as f64,
        tc => return loc.decode_i64(batch, row, FixedInt::from_type_code(tc).expect("an integer column")),
    };
    assert_eq!(f.fract(), 0.0, "column {ci} row {row}: {f} is not an integer");
    f as i64
}

/// The expected side of a [`Db::rows`] compare when every row is present
/// exactly once: each row with a trailing `1`, sorted.
pub fn at_weight_one(rows: &[Vec<i64>]) -> Vec<Vec<i64>> {
    let mut out: Vec<Vec<i64>> = rows.iter().map(|r| r.iter().copied().chain([1]).collect()).collect();
    out.sort();
    out
}
