//! The test helpers only this crate uses.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-zset`,
//! so it names crate-internals through `crate::` and needs nothing published on
//! its behalf.

use std::cmp::Ordering;
use std::path::Path;
use std::rc::Rc;

use crate::repr::{
    from_runs, from_runs_in_band, Batch, BatchBuilder, Codebook, MappedShard, MemBatch, ReadCursor, Run, ShardWriteOpts,
};
use crate::schema::{group_band, Generic, PayloadOrder, SchemaColumn, SchemaDescriptor};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{PkBuf, TypeCode};

use super::rng::Rng;
use super::shared::{make_schema_pk_u64_payload_string, self_typed_slots, u64_pk_schema, zset_of, zset_sum};

/// The reindex of `source` onto its columns `key`, each at its own slot type,
/// keeping `keep` and doing `nulls` with a NULL key: with `Drop`, the map a
/// join's trace integrates behind.
pub(crate) fn rekey_plan(
    source: &SchemaDescriptor,
    key: &[u32],
    keep: &[u32],
    nulls: gnitz_wire::NullKeys,
) -> crate::algebra::MapPlan {
    let rekey = gnitz_wire::MapKind::Reindex {
        keep: keep.to_vec(),
        key: self_typed_slots(source, key),
        nulls,
    };
    crate::algebra::MapPlan::from_wire(source, &rekey).expect("fixture reindex is well-formed")
}

/// An operator's trace in a test: one consolidated run per ingest, read through
/// one cursor — what a store's memtable holds between folds.
pub(crate) struct TestTrace {
    schema: SchemaDescriptor,
    runs: Vec<Rc<Batch>>,
}

impl TestTrace {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        TestTrace { schema, runs: Vec::new() }
    }

    /// `rows` dealt round-robin into `runs` runs: an element can cancel across
    /// runs.
    pub(crate) fn dealt(rows: &Batch, runs: usize) -> Self {
        let mut trace = TestTrace::new(*rows.schema());
        for k in 0..runs {
            let rows_k: Vec<(usize, usize)> = (k..rows.len()).step_by(runs).map(|r| (r, r + 1)).collect();
            trace.ingest(Batch::from_ranges(rows, &rows_k, 0));
        }
        trace
    }

    /// Add `batch` as one more run.
    pub(crate) fn ingest(&mut self, mut batch: Batch) {
        batch.set_schema(&self.schema);
        let run = batch.into_consolidated();
        if !run.is_empty() {
            self.runs.push(Rc::new(run));
        }
    }

    /// The runs, oldest first.
    pub(crate) fn runs(&self) -> &[Rc<Batch>] {
        &self.runs
    }

    /// A cursor over every run.
    pub(crate) fn cursor(&self) -> ReadCursor {
        from_runs(self.runs.iter().cloned().map(Run::Mem), self.schema)
    }

    /// A cursor over the rows whose PK begins with a key in `[first, last]` and
    /// over no other: an operator that reads outside the keys it opened its
    /// history at finds nothing there.
    pub(crate) fn cursor_within(&self, first: &[u8], last: &[u8]) -> ReadCursor {
        let (start, end) =
            group_band(first, last, self.schema.pk_stride()).expect("`first <= last`, so the band holds a key");
        let runs = self.runs.iter().cloned().map(Run::Mem);
        let end = end.as_ref().map(PkBuf::pk_bytes);
        from_runs_in_band(runs, self.schema, start.pk_bytes(), end)
    }
}

/// One column of one row as its native little-endian cell, a string as its
/// content; `None` for NULL.
pub(crate) fn cell(mb: &MemBatch, loc: ColumnLocator, row: usize) -> Option<Vec<u8>> {
    let mut scratch = [0u8; 16];
    (!loc.is_null(mb, row)).then(|| match loc.type_code().is_german_string() {
        true => loc.content(mb, row).to_vec(),
        false => loc.native_le_bytes(mb, row, &mut scratch).to_vec(),
    })
}

/// A native little-endian cell of up to 16 bytes as the zero-extended value
/// `BatchBuilder::put_int` writes back at the cell's own width.
pub(crate) fn le_cell(cell: &[u8]) -> u128 {
    let mut v = [0u8; 16];
    v[..cell.len()].copy_from_slice(cell);
    u128::from_le_bytes(v)
}

/// A native of the integer type `tc` drawn from its zero, one, signed extremes
/// and all-ones, zero-extended: values tie often, so a tie falls through to the
/// next column.
pub(crate) fn edge_native(rng: &mut Rng, tc: TypeCode) -> u128 {
    let mask = gnitz_wire::image_mask(tc.wire_stride());
    let top = (mask >> 1) + 1;
    rng.pick(&[0, 1, top - 1, top, mask])
}

/// A random schema: `1..=MAX_PK_COLUMNS` PK columns of PK-eligible types and
/// `0..=6` payload columns drawn from `payload`, nullable or not, all
/// interleaved at random positions, with the PK list in random order.
pub(crate) fn random_schema(rng: &mut Rng, payload: &[TypeCode], nullable: bool) -> SchemaDescriptor {
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    let n_pk = 1 + rng.gen_range(gnitz_wire::MAX_PK_COLUMNS as u64) as usize;
    let n_payload = rng.gen_range(7) as usize;
    let mut cols: Vec<SchemaColumn> = (0..n_pk)
        .map(|_| SchemaColumn::new(rng.pick(&pk_types), false))
        .collect();
    for _ in 0..n_payload {
        let null = nullable && rng.gen_range(2) == 1;
        cols.push(SchemaColumn::new(rng.pick(payload), null));
    }
    // `pos[i]` is column i's position; the PK list is the PK columns' positions.
    let mut pos: Vec<u32> = (0..cols.len() as u32).collect();
    rng.shuffle(&mut pos);
    let mut placed = cols.clone();
    for (c, &p) in cols.iter().zip(&pos) {
        placed[p as usize] = *c;
    }
    SchemaDescriptor::new(&placed, &pos[..n_pk])
}

// ---------------------------------------------------------------------------
// The Z-set fold oracle
// ---------------------------------------------------------------------------

/// `got` is the Z-set sum of `inputs`: one row per element, strictly ascending
/// in (PK, payload) order.
pub(crate) fn assert_folds(inputs: &[Batch], got: &Batch, what: &str) {
    let schema = got.schema();
    let want = zset_sum(inputs, schema);
    assert_eq!(zset_of(got, schema), want, "{what}: the Z-set sum");
    assert_eq!(got.len(), want.len(), "{what}: one row per element");
    let mb = got.as_mem_batch();
    for r in 1..got.len() {
        assert_eq!(
            Generic.compare_full(schema, &mb, r - 1, &mb, r),
            Ordering::Less,
            "{what}: rows {} and {r} out of order",
            r - 1
        );
    }
}

/// A `U64` key over `U8`, nullable `I64` and `U16` columns: a row is 35 bytes
/// and no payload region starts at a multiple of 8 of them.
pub(crate) fn ragged_schema() -> SchemaDescriptor {
    u64_pk_schema(&[
        SchemaColumn::new(TypeCode::U8, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::U16, false),
    ])
}

/// `rows` rows of [`ragged_schema`], every fifth with its `I64` NULL.
pub(crate) fn ragged_batch(rows: usize) -> Batch {
    let mut b = BatchBuilder::new(&ragged_schema());
    for i in 0..rows as u128 {
        b.begin_row(i, 1 + i as i64 % 3);
        b.put_int(i % 251);
        b.put_opt_int((i % 5 != 0).then_some(i * 10));
        b.put_int(i % 65_521);
        b.end_row();
    }
    b.finish()
}

/// `(pk, value)` rows at weight 1 over a U64 PK and one STRING payload column,
/// in the order given.
pub(crate) fn string_batch(rows: &[(u64, impl AsRef<str>)]) -> Batch {
    let mut b = BatchBuilder::new(&make_schema_pk_u64_payload_string());
    for (pk, v) in rows {
        b.begin_row(*pk as u128, 1);
        b.put_string(v.as_ref());
        b.end_row();
    }
    b.finish()
}

/// Payload column 0's string on every row, in order.
pub(crate) fn read_strings(batch: &Batch) -> Vec<Vec<u8>> {
    (0..batch.len())
        .map(|row| gnitz_wire::payload_bytes(batch, row, 0).to_vec())
        .collect()
}

/// Row `i`'s sentence: words of a short list, so that a symbol table codes a
/// heap of them to a fraction of its bytes, and unique to `i`.
pub(crate) fn prose(i: usize) -> String {
    const WORDS: [&str; 12] = [
        "the", "river", "under", "morning", "light", "carried", "every", "lantern", "toward", "harbour", "while",
        "sailors",
    ];
    let mut s = format!("{i} ");
    for k in 0..6 + i % 9 {
        s.push_str(WORDS[(i * 7 + k * 5) % WORDS.len()]);
        s.push(' ');
    }
    s
}

/// Write `batch` to `path` as a shard whose heap is coded under `codebook`,
/// and map it.
pub(crate) fn map_coded_shard(path: &Path, batch: &Batch, codebook: &Codebook) -> Rc<MappedShard> {
    let path = path.to_str().unwrap();
    let opts = ShardWriteOpts {
        codebook: Some(codebook),
        ..Default::default()
    };
    batch.write_as_shard(path, opts).unwrap();
    let shard = MappedShard::open(path, batch.schema()).unwrap();
    assert!(
        shard.coded_heap().is_some(),
        "premise: the heap codes to less than three quarters"
    );
    Rc::new(shard)
}

/// U64 pk + a single BLOB payload column.
pub(crate) fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    u64_pk_schema(&[SchemaColumn::new(TypeCode::Blob, false)])
}

/// `batch` written to `path` and mapped back, as a store maps the shards it
/// writes.
pub(crate) fn map_shard(path: &Path, batch: &Batch) -> Rc<MappedShard> {
    let path = path.to_str().unwrap();
    batch.write_as_shard(path, ShardWriteOpts::default()).unwrap();
    Rc::new(MappedShard::open(path, batch.schema()).unwrap())
}

/// Strings that are prefixes of one another, one escaping `0x00`, and two that
/// share more leading bytes than an index key's lead slot holds.
pub(crate) const STRS: [&str; 10] = [
    "",
    "a",
    "ab",
    "\0b",
    "shared-lead-slot-prefix-x",
    "shared-lead-slot-prefix-y",
    "a\0",
    "a\0\0",
    "a\0b-a-string-past-the-inline-cell-with-a-zero",
    "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
];

/// Floats `total_cmp` tells apart though `==` does not, and the ones it places
/// past every finite value.
pub(crate) const FLOATS: [f64; 6] = [-0.0, 0.0, -1.5, 2.5, f64::INFINITY, f64::NAN];

/// `(pk, weight, payload cells)` rows against `schema`, `None` a NULL cell.
pub(crate) fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, &[Option<i64>])]) -> Batch {
    let mut batch = BatchBuilder::new(schema);
    for &(pk, weight, cells) in rows {
        batch.begin_row(pk as u128, weight);
        for &cell in cells {
            batch.put_opt_int(cell.map(|v| v as u128));
        }
        batch.end_row();
    }
    batch.finish()
}
