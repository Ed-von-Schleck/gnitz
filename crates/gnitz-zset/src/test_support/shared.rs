//! The test helpers other crates' tests share with this crate's own,
//! compiled here and as `gnitz-zset-testkit` from this one source. Every path is
//! spelled `gnitz_zset::`, so a helper sees only the public API; one that needs a
//! crate-internal belongs in [`super::internal`].

use std::collections::HashMap;

use proptest::prelude::*;

use gnitz_wire::RowSource;
use gnitz_wire::TypeCode;
use std::cmp::Ordering;

use gnitz_expr::ColumnLocator;
use gnitz_zset::repr::{Batch, BatchBuilder, MemBatch};
use gnitz_zset::schema::{Generic, PayloadOrder, SchemaColumn, SchemaDescriptor};

/// A schema with one PK column per type code in `tcs`, plus a single trailing
/// I64 payload column. [`pk_only_schema`] is the no-payload counterpart.
pub fn pk_payload_schema(tcs: &[TypeCode]) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = tcs.iter().map(|&t| SchemaColumn::new(t, false)).collect();
    cols.push(SchemaColumn::new(TypeCode::I64, false));
    let pk: Vec<u32> = (0..tcs.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// `cols` of `schema` as join-key slots, each at the column's own slot type.
pub fn self_typed_slots(schema: &SchemaDescriptor, cols: &[u32]) -> Vec<gnitz_wire::ReindexSlot> {
    cols.iter()
        .map(|&c| (c, schema.columns()[c as usize].type_code.reindex_output_type()))
        .collect()
}

/// The canonical narrow test schema: U64 pk + a single I64 payload column.
pub fn make_schema_u64_i64() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U64])
}

/// Build a batch over [`make_schema_u64_i64`]-shaped schemas from native
/// `(pk, weight, payload)` tuples and certify it consolidated. **Rows must
/// arrive pre-sorted by (PK, payload) with no net-zero duplicates** — the
/// certification is a claim the caller makes, and `certify_consolidated` only
/// debug-verifies it; a lying claim would let a consumer skip-point silently
/// mis-fold weights.
pub fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = make_batch_raw(schema, rows);
    b.certify_consolidated();
    b
}

/// [`make_batch`] without the consolidated certification: the batch stays
/// honestly unconsolidated, for tests whose rows are unsorted or that exercise the
/// sort/fold paths themselves.
pub fn make_batch_raw(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let rows: Vec<(u128, i64, i64)> = rows.iter().map(|&(pk, w, v)| (pk as u128, w, v)).collect();
    make_batch_u128_raw(schema, &rows)
}

/// A U64 PK ahead of the `payload` columns.
pub fn u64_pk_schema(payload: &[SchemaColumn]) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend_from_slice(payload);
    SchemaDescriptor::new(&cols, &[0])
}

/// A schema of 1..=8 columns interleaving `1..=max_pk` PK columns — PK-eligible,
/// non-nullable, the PK list in a random declared order — with columns of any
/// type, nullable or not. So the PK columns are neither a prefix nor ascending,
/// which is where a closed form for the payload numbering breaks.
///
/// `max_pk` bounds the PK arity: the engine's schemas run to `MAX_PK_COLUMNS`,
/// the secondary-index schema width, but the persisted client codec caps at
/// `PK_LIST_MAX_COLS`.
pub fn arb_schema(max_pk: usize) -> impl Strategy<Value = SchemaDescriptor> {
    // n_cols ≥ 1, so `1..=n_cols.min(max_pk)` is never empty.
    (1usize..=8)
        .prop_flat_map(move |n_cols| {
            (
                prop::collection::vec((prop::sample::select(TypeCode::ALL), any::<bool>()), n_cols),
                Just((0..n_cols as u32).collect::<Vec<u32>>()).prop_shuffle(),
                1usize..=n_cols.min(max_pk),
            )
        })
        .prop_map(|(cols, order, k)| {
            let pk = &order[..k];
            let cols: Vec<SchemaColumn> = cols
                .into_iter()
                .zip(0u32..)
                .map(|((tc, nullable), i)| match pk.contains(&i) {
                    // An ineligible draw becomes a U64, so every builder accepts
                    // the schema.
                    true if tc.is_pk_eligible() => SchemaColumn::new(tc, false),
                    true => SchemaColumn::new(TypeCode::U64, false),
                    false => SchemaColumn::new(tc, nullable),
                })
                .collect();
            SchemaDescriptor::new(&cols, pk)
        })
}

/// Set by [`run_test_in_child`] on the child it spawns; the body test guards on
/// [`in_child_test`] so a normal sweep skips it.
const CHILD_TEST_VAR: &str = "GNITZ_RUN_CHILD_TEST";

/// True only inside a re-exec'd child spawned by [`run_test_in_child`].
pub fn in_child_test() -> bool {
    std::env::var(CHILD_TEST_VAR).is_ok()
}

/// Re-run the current test binary filtered to `internal_test`, with `envs` set,
/// and hand back the child's exit status and captured output.
///
/// The reason a test needs a child at all rather than setting the variable
/// itself: a fault seam — and the `io_uring` verdict — reads its variable once per
/// process, and the test runner runs a crate's tests as
/// threads of one process, so an in-process `set_var` would race every other
/// test and lose to whichever read first. A direct in-process fail-stop would
/// also terminate the runner. `internal_test` must return early unless
/// [`in_child_test`], so a normal test sweep skips it.
///
/// `--exact`, so the filter cannot also pick up a sibling whose name contains
/// this one. Pass the caller's `module_path!()` as `module`: libtest's filter
/// path is the module path minus the crate segment, so deriving it here keeps
/// the filter right when a test module moves.
pub fn run_test_in_child(module: &str, internal_test: &str, envs: &[(&str, &str)]) -> std::process::Output {
    let filter = match module.split_once("::") {
        Some((_krate, path)) => format!("{path}::{internal_test}"),
        None => internal_test.to_string(),
    };
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.arg("--exact").arg(&filter).arg("--nocapture");
    cmd.env(CHILD_TEST_VAR, "1");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().unwrap()
}

/// Assert the child exited cleanly **and** ran its one test. The exit code alone
/// proves nothing: `libtest` exits 0 when its filter matches nothing, so
/// renaming a child would otherwise leave its caller green covering nothing.
pub fn assert_child_ok(out: &std::process::Output, what: &str) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.code() == Some(0) && stdout.contains("1 passed"),
        "{what}\n-- child stdout --\n{stdout}-- child stderr --\n{stderr}",
    );
}

/// Flip each bit of `buf[span]` in turn, run `check(byte, bit, buf)` with it
/// damaged, and restore it — so no case can leave the fixture altered for the
/// ones after it.
pub fn sweep_bit_flips(buf: &mut [u8], span: std::ops::Range<usize>, mut check: impl FnMut(usize, usize, &[u8])) {
    for byte in span {
        for bit in 0..8 {
            buf[byte] ^= 1 << bit;
            check(byte, bit, buf);
            buf[byte] ^= 1 << bit;
        }
    }
}

/// The conjunction of `col <op> lit` over fixed-int columns — the program shape a
/// `WHERE` of such conjuncts compiles to, built through the client's own
/// `ExprBuilder`.
pub fn cmp_all(conjuncts: &[(gnitz_expr::CmpOp, u32, i64)]) -> gnitz_expr::LogicalProgram {
    use gnitz_expr::LogicalInstr::{BoolBinary, Cmp, LoadCol, LoadConst};
    let mut eb = gnitz_expr::ExprBuilder::new();
    let keep = conjuncts
        .iter()
        .map(|&(op, col, lit)| {
            let a = eb.emit(LoadCol { col });
            let b = eb.emit(LoadConst { val: lit, unsigned: false });
            eb.emit(Cmp { op, a, b })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .reduce(|a, b| eb.emit(BoolBinary { a, b, is_or: false }))
        .expect("at least one conjunct");
    eb.build(vec![gnitz_expr::Sink::Reg(keep)])
        .expect("a well-formed program")
}

/// A row's logical identity: its PK bytes plus one entry per payload column.
pub type RowKey = (Vec<u8>, Vec<Option<Vec<u8>>>);

/// Reduce a row of any source — an engine batch or a client reply — to its
/// logical identity. NULL is `None`, distinct from an empty string
/// `Some(vec![])` — keeping the raw null word out of the key, so its unused bits
/// cannot cause a spurious mismatch while every meaningful null bit is still
/// reflected. STRING / BLOB cells are **decoded**:
/// the 16-byte German-string struct embeds a blob offset, so two batches holding
/// the same logical string carry different struct bytes.
pub fn row_key<S: RowSource>(src: &S, schema: &SchemaDescriptor, row: usize) -> RowKey {
    let nw = src.get_null_word(row);
    let vals = schema
        .payload_columns()
        .map(|(pi, col)| {
            if gnitz_wire::null_word_get(nw, pi) {
                return None;
            }
            let tc = col.type_code;
            let raw = src.get_col_ptr(row, pi, tc.wire_stride());
            Some(if tc.is_german_string() {
                gnitz_wire::german_string_bytes(raw.first_chunk().unwrap(), src.heap()).into_owned()
            } else {
                raw.to_vec()
            })
        })
        .collect();
    (src.get_pk_bytes(row).to_vec(), vals)
}

/// An all-PK schema: one column per type code in `types`, every column a PK
/// column (`pk_indices = 0..n`) and no payload — the generic PK-shape builder
/// for the OPK encode/compare/route tests.
pub fn pk_only_schema(types: &[TypeCode]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = types.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
    let pk: Vec<u32> = (0..types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// OPK-encode native PK column values (one per PK column, in PK-list order) into
/// the canonical order-preserving key — the exact bytes the ingest path
/// produces. Signed columns are passed as `v as u128` (the low `size()`
/// little-endian bytes are the two's-complement image the encoder sign-flips).
pub fn opk_pk(schema: &SchemaDescriptor, vals: &[u128]) -> Vec<u8> {
    schema.opk_key_cols(vals).pk_bytes().to_vec()
}

/// [`make_batch_raw`] over [`make_schema_u128_i64`]-shaped schemas — native
/// u128 PKs, rows left unconsolidated in the order given.
pub fn make_batch_u128_raw(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk, w);
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// [`make_batch_u128_raw`] plus the consolidated certification. **Rows must
/// arrive pre-sorted by (PK, payload) with no net-zero duplicates** — the
/// certification is a claim the caller makes, and `certify_consolidated` only
/// debug-verifies it.
pub fn make_batch_u128(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
    let mut b = make_batch_u128_raw(schema, rows);
    b.certify_consolidated();
    b
}

/// U128 pk + a single I64 payload column — the 16-byte-PK sibling of
/// [`make_schema_u64_i64`].
pub fn make_schema_u128_i64() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U128])
}

/// U64 pk + a single STRING payload column.
pub fn make_schema_pk_u64_payload_string() -> SchemaDescriptor {
    u64_pk_schema(&[SchemaColumn::new(TypeCode::String, false)])
}

/// [`make_batch_bytes`] under [`make_schema_pk_u64_payload_string`].
pub fn make_string_batch(rows: &[(u64, i64, &[u8])]) -> Batch {
    make_batch_bytes(&make_schema_pk_u64_payload_string(), rows)
}

/// [`make_batch_bytes_raw`] certified consolidated: the rows must already be in
/// (PK, payload) order.
pub fn make_batch_bytes(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> Batch {
    let mut b = make_batch_bytes_raw(schema, rows);
    b.certify_consolidated();
    b
}

/// A `(U64 pk, STRING|BLOB payload)` batch of `(pk, weight, bytes)` rows, in the
/// order given.
pub fn make_batch_bytes_raw(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk as u128, w);
        b.put_blob(val);
        b.end_row();
    }
    b.finish()
}

/// The batch as the Z-Set it denotes: `Σ weight` per logical row identity, with
/// the net-zero entries dropped. Emission order and the physical encoding of a
/// string both drop out, which is what lets two independently produced batches
/// be compared.
pub fn zset_of(batch: &Batch, schema: &SchemaDescriptor) -> HashMap<RowKey, i64> {
    zset_sum(std::slice::from_ref(batch), schema)
}

/// Every row of `batch` as its logical identity and weight, in batch order —
/// [`zset_of`] for a comparison where order and unfolded duplicates count.
pub fn weighted_rows(batch: &Batch) -> Vec<(RowKey, i64)> {
    (0..batch.len())
        .map(|row| (row_key(batch, batch.schema(), row), batch.get_weight(row)))
        .collect()
}

/// Whether the kind admits the pair of PK regions `(dpk, tpk)`. `rel` relates the
/// **left** slot to the right one, so the delta's side decides the operand order.
fn join_pair_matches(kind: gnitz_wire::JoinKind, delta_is_right: bool, eq_size: usize, dpk: &[u8], tpk: &[u8]) -> bool {
    match kind {
        gnitz_wire::JoinKind::Cross => true,
        gnitz_wire::JoinKind::Equi => dpk == tpk,
        gnitz_wire::JoinKind::Range { rel, .. } => {
            if dpk[..eq_size] != tpk[..eq_size] {
                return false;
            }
            // Equal-width OPK slot slices, so a raw byte compare IS the typed
            // comparison the relation names.
            let (d, s) = (&dpk[eq_size..], &tpk[eq_size..]);
            let (l, r) = match delta_is_right {
                true => (s, d),
                false => (d, s),
            };
            match rel {
                gnitz_wire::RangeRel::Lt => l < r,
                gnitz_wire::RangeRel::Le => l <= r,
                gnitz_wire::RangeRel::Gt => l > r,
                gnitz_wire::RangeRel::Ge => l >= r,
            }
        }
    }
}

/// Brute-force reference: every `(delta row, trace row)` pair the kind admits,
/// composed at weight `w_d · w_t` into the key, the left side's payload cells and
/// then the right's — spelled out independently of the join kernel.
/// Returns the Z-Set it denotes and the number of pairs with a non-zero product.
pub fn join_reference(
    kind: gnitz_wire::JoinKind,
    delta_is_right: bool,
    delta_schema: &SchemaDescriptor,
    trace_schema: &SchemaDescriptor,
    delta: &Batch,
    trace: &Batch,
) -> (HashMap<RowKey, i64>, usize) {
    let eq_size = match kind {
        // Every key column but the last is an equality slot.
        gnitz_wire::JoinKind::Range { .. } => trace_schema.pk_prefix_stride(trace_schema.pk_cols().len() - 1),
        _ => 0,
    };
    let mut m: HashMap<RowKey, i64> = HashMap::new();
    let mut rows = 0usize;
    for i in 0..delta.len() {
        let dpk = delta.get_pk_bytes(i);
        for j in 0..trace.len() {
            let tpk = trace.get_pk_bytes(j);
            if !join_pair_matches(kind, delta_is_right, eq_size, dpk, tpk) {
                continue;
            }
            let w = delta.get_weight(i).wrapping_mul(trace.get_weight(j));
            if w == 0 {
                continue;
            }
            rows += 1;
            let key: Vec<u8> = match (kind, delta_is_right) {
                (gnitz_wire::JoinKind::Cross, true) => [tpk, dpk].concat(),
                (gnitz_wire::JoinKind::Cross, false) => [dpk, tpk].concat(),
                _ => dpk.to_vec(),
            };
            let d_cells = row_key(delta, delta_schema, i).1;
            let t_cells = row_key(trace, trace_schema, j).1;
            let (mut cells, tail) = match delta_is_right {
                true => (t_cells, d_cells),
                false => (d_cells, t_cells),
            };
            cells.extend(tail);
            *m.entry((key, cells)).or_insert(0) += w;
        }
    }
    m.retain(|_, w| *w != 0);
    (m, rows)
}

/// `batch`, which holds rows, encoded whole as one WAL block in a buffer of its own.
pub fn encode_to_wire_vec(batch: &Batch) -> Vec<u8> {
    let whole = batch.wire_whole().expect("a batch of no rows encodes to no block");
    let mut out = vec![0; whole.byte_size()];
    whole.encode(&mut out);
    out
}

/// U64 pk + two I64 payload columns — the flush/merge fixtures' shape, where a
/// second payload column is what makes a partially-written row detectable.
pub fn pk_u64_two_i64_schema() -> SchemaDescriptor {
    u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false); 2])
}

/// Build a batch from raw OPK key bytes, one row per `(pk, weight, payload)`,
/// with a single non-null I64 payload at slot 0.
///
/// The PK is stored verbatim. Every consumer below the encode boundary treats it
/// as an opaque ordered byte string, so this one builder serves every stride.
/// [`opk_pk`] produces the bytes from native column values.
pub fn make_batch_opk(schema: &SchemaDescriptor, rows: &[(impl AsRef<[u8]>, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for (pk, w, val) in rows {
        let pk = pk.as_ref();
        assert_eq!(pk.len(), schema.pk_stride(), "PK bytes must be exactly one stride wide");
        b.begin_row_bytes(pk, *w);
        b.put_int(*val as u128);
        b.end_row();
    }
    b.finish()
}

/// Payload column 0 of row `row`, an 8-byte integer.
pub fn payload0_i64<S: RowSource>(src: &S, row: usize) -> i64 {
    gnitz_wire::payload_u64(src, row, 0) as i64
}

/// The `(pk, weight, payload)` rows [`make_batch_raw`] would build `batch` from,
/// sorted: a batch's row order is not what a test compares.
pub fn batch_rows(batch: &Batch) -> Vec<(u64, i64, i64)> {
    let mut rows: Vec<_> = (0..batch.len())
        .map(|i| {
            (
                gnitz_wire::widen_pk_be(batch.get_pk_bytes(i)) as u64,
                batch.get_weight(i),
                payload0_i64(batch, i),
            )
        })
        .collect();
    rows.sort_unstable();
    rows
}

/// The Z-set sum of `inputs`, net-zero elements dropped.
pub fn zset_sum(inputs: &[Batch], schema: &SchemaDescriptor) -> HashMap<RowKey, i64> {
    let mut z: HashMap<RowKey, i64> = HashMap::new();
    for b in inputs {
        for row in 0..b.len() {
            *z.entry(row_key(b, schema, row)).or_insert(0) += b.get_weight(row);
        }
    }
    z.retain(|_, w| *w != 0);
    z
}

/// Fixed-int schemas at both edges of every argsort key form — `≤8` (strides 1,
/// 2, 4, 8), `9..=16` (12, 16), the packed `17..=28` (17, 24, 28), `29..=32` (29,
/// 32) and the `>32` fallback (80) — one whose fixed-int payload spans every
/// column width, signed and not, past one 128-bit sort key, and a
/// nullable-string schema under the generic comparator at a narrow PK, a wide
/// one, and one whose PK columns sit among the payload columns in an order other
/// than the schema's.
pub(crate) fn fold_schemas() -> Vec<SchemaDescriptor> {
    use TypeCode::*;
    let generic = |pk: &[TypeCode]| {
        let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
        cols.extend([SchemaColumn::new(String, true), SchemaColumn::new(I64, true)]);
        SchemaDescriptor::new(&cols, &(0..pk.len() as u32).collect::<Vec<_>>())
    };
    let wide_ints = [I8, U16, I32, U64, I64, I16, U8].map(|t| SchemaColumn::new(t, false));
    vec![
        pk_payload_schema(&[U8]),
        pk_payload_schema(&[U16]),
        pk_payload_schema(&[I64]),
        pk_payload_schema(&[I32]),
        pk_payload_schema(&[U32, U64]),
        pk_payload_schema(&[U64, I32]),
        pk_payload_schema(&[U128]),
        pk_payload_schema(&[U128, U8]),
        pk_payload_schema(&[U64; 3]),
        pk_payload_schema(&[U128, U64, U32]),
        pk_payload_schema(&[U128, U64, U32, U8]),
        pk_payload_schema(&[U128, U128]),
        pk_payload_schema(&[U128; 5]),
        u64_pk_schema(&wide_ints),
        generic(&[U64]),
        generic(&[U64; 3]),
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(String, true),
                SchemaColumn::new(U64, false),
                SchemaColumn::new(I64, true),
                SchemaColumn::new(I32, false),
            ],
            &[3, 1],
        ),
    ]
}

/// `(pk bytes, weight, string, ints)`; `None` is NULL. `ints` sets each
/// fixed-int column as [`fold_batch`] reads it.
pub type FoldRow = (Vec<u8>, i64, Option<u8>, Option<u8>);

/// One inline string and three spilling ones, two of them equal in length and
/// in all but their last byte.
const FOLD_STRS: [&[u8]; 4] = [
    b"inline",
    b"a-long-string-that-spills",
    b"a-long-string-that-spillz",
    b"another-long-spilling-value",
];

/// A `fold_schemas` schema and rows over up to eight keys of its stride that
/// differ in their first byte, their middle one and their last — so a wide key's
/// rows can share a prefix, tie on the span past it and differ behind it — with
/// small weight and payload domains, so folds and ghost cancels are common.
pub fn arb_fold_case() -> impl Strategy<Value = (SchemaDescriptor, Vec<FoldRow>)> {
    prop::sample::select(fold_schemas()).prop_flat_map(|s| {
        let stride = s.pk_stride();
        let row = (
            0u8..2,
            0u8..4,
            -3i64..=3,
            prop::option::of(0..FOLD_STRS.len() as u8),
            prop::option::of(0u8..4),
        );
        let rows = (
            prop::collection::vec(any::<u8>(), stride),
            prop::collection::vec(row, 0..40),
        )
            .prop_map(move |(base, rows)| {
                rows.into_iter()
                    .map(|(lead, tail, w, s, v)| {
                        let mut pk = base.clone();
                        pk[0] = lead;
                        pk[stride / 2] = pk[stride / 2].wrapping_add(tail & 2);
                        pk[stride - 1] = pk[stride - 1].wrapping_add(tail & 1);
                        (pk, w, s, v)
                    })
                    .collect()
            });
        (Just(s), rows)
    })
}

/// `n` rows of keys of `stride` that differ in their first byte's low bit and
/// their last byte alone — so a batch of at least 256 of them takes the radix
/// sort — with repeats, cancels and payload ties.
pub fn radix_fold_rows(stride: usize, n: u64) -> Vec<FoldRow> {
    (0..n)
        .map(|i| {
            let r = gnitz_foundation::rng::mix(i);
            let mut pk = vec![0u8; stride];
            pk[0] = (r >> 40) as u8 & 1;
            pk[stride - 1] = (r >> 48) as u8;
            let w = (r >> 20) as i64 % 4 - 1;
            (pk, w, Some((r >> 8) as u8 % 4), Some((r >> 12) as u8 % 4))
        })
        .collect()
}

/// `rows` as an unconsolidated batch over a `fold_schemas` schema. A NULL and a
/// zero hold the same cell bytes and must not fold; each long string lands at its
/// own heap offset, and equal ones must.
///
/// A fixed-int column holds 0 or -1, whose order a sign flip reverses: the first
/// half of the payload columns as bit 0 of the row's `ints` says, the rest as bit
/// 1, so rows can tie on the leading columns and differ behind them. `None` is
/// NULL in a nullable column and 0 in another.
pub fn fold_batch(schema: &SchemaDescriptor, rows: &[FoldRow]) -> Batch {
    let n = schema.num_payload_cols();
    let mut b = BatchBuilder::new(schema);
    for (pk, w, s, v) in rows {
        b.begin_row_bytes(pk, *w);
        for (pc, col) in schema.payload_columns() {
            match (col.type_code.is_german_string(), s, v) {
                (true, Some(i), _) => {
                    b.put_blob(FOLD_STRS[*i as usize]);
                }
                (true, None, _) | (false, _, None) if col.nullable => {
                    b.put_null();
                }
                (false, _, v) => {
                    let bit = v.unwrap_or(0) >> (2 * pc >= n) as u8 & 1;
                    b.put_int(if bit == 1 { u128::MAX } else { 0 });
                }
                (true, None, _) => unreachable!("fold schemas' strings are nullable"),
            }
        }
        b.end_row();
    }
    b.finish()
}

/// The reindex of `source` onto its columns `key`, each at its own slot type,
/// keeping `keep` and doing `nulls` with a NULL key: with `Drop`, the map a
/// join's trace integrates behind.
pub fn rekey_plan(
    source: &SchemaDescriptor,
    key: &[u32],
    keep: &[u32],
    nulls: gnitz_wire::NullKeys,
) -> gnitz_zset::algebra::MapPlan {
    let rekey = gnitz_wire::MapKind::Reindex {
        keep: keep.to_vec(),
        key: self_typed_slots(source, key),
        nulls,
    };
    gnitz_zset::algebra::MapPlan::from_wire(source, &rekey).expect("fixture reindex is well-formed")
}

/// One column of one row as its native little-endian cell, a string as its
/// content; `None` for NULL.
pub fn cell(mb: &MemBatch, loc: ColumnLocator, row: usize) -> Option<Vec<u8>> {
    let mut scratch = [0u8; 16];
    (!loc.is_null(mb, row)).then(|| match loc.type_code().is_german_string() {
        true => loc.content(mb, row).to_vec(),
        false => loc.native_le_bytes(mb, row, &mut scratch).to_vec(),
    })
}

// ---------------------------------------------------------------------------
// The Z-set fold oracle
// ---------------------------------------------------------------------------

/// `got` is the Z-set sum of `inputs`: one row per element, strictly ascending
/// in (PK, payload) order.
pub fn assert_folds(inputs: &[Batch], got: &Batch, what: &str) {
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

/// `(pk, value)` rows at weight 1 over a U64 PK and one STRING payload column,
/// in the order given.
pub fn string_batch(rows: &[(u64, impl AsRef<str>)]) -> Batch {
    let mut b = BatchBuilder::new(&make_schema_pk_u64_payload_string());
    for (pk, v) in rows {
        b.begin_row(*pk as u128, 1);
        b.put_string(v.as_ref());
        b.end_row();
    }
    b.finish()
}

/// Payload column 0's string on every row, in order.
pub fn read_strings(batch: &Batch) -> Vec<Vec<u8>> {
    (0..batch.len())
        .map(|row| gnitz_wire::payload_bytes(batch, row, 0).to_vec())
        .collect()
}

/// Row `i`'s sentence: words of a short list, so that a symbol table codes a
/// heap of them to a fraction of its bytes, and unique to `i`.
pub fn prose(i: usize) -> String {
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

/// Strings that are prefixes of one another, one escaping `0x00`, and two that
/// share more leading bytes than an index key's lead slot holds.
pub const STRS: [&str; 10] = [
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
pub const FLOATS: [f64; 6] = [-0.0, 0.0, -1.5, 2.5, f64::INFINITY, f64::NAN];
