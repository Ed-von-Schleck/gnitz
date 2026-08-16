//! A client `ZSetBatch` in the §6 region shape — the one form everything
//! downstream reads: `gnitz_wire::wal::encode` frames these regions into a WAL
//! block, and the shared `gnitz-expr` evaluator runs its kernels over them.
//!
//! One builder serves both. The encode path and the evaluator want the same
//! list — PK, weight, null bitmap, one region per payload slot in slot order,
//! blob heap last — so the §6 rule for a `ZSetBatch` is stated here once.
//!
//! Every [`gnitz_expr::RowSource`]/[`gnitz_expr::BatchView`] method below is
//! `#[inline(always)]`; see [`gnitz_expr::BatchView`] for why the plain hint is
//! not enough.

use super::types::{null_word_get, ColData, PkColumn, Schema, ZSetBatch};
use gnitz_wire::{as_le_bytes, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK};

// ── The region builder ───────────────────────────────────────────────────────

/// The buffers a `ZSetBatch` does **not** already hold in §6 region form: the
/// OPK PK region (a [`PkColumn`] holds native LE values or on-wire LE bytes,
/// never OPK) and the 16-byte German-string cells plus the blob arena they
/// point into. A `Fixed` column *is* a region and is borrowed in place.
///
/// Hoist one above a loop over batches: these buffers keep their capacity
/// across views. The region *list* itself is rebuilt per view.
#[derive(Default)]
pub struct ViewBuffers {
    pk_region: Vec<u8>,
    /// Indexed by payload slot: `rows * 16` German cells for a String/Blob
    /// column, empty for every other slot.
    str_cols: Vec<Vec<u8>>,
    blob: Vec<u8>,
}

impl ViewBuffers {
    /// Refill from `batch` and lend a view over it. The only way to obtain a
    /// [`ZSetBatchView`]: buffers and batch are paired in one call, so a view
    /// over stale buffers cannot be written, and the batch cannot be mutated
    /// while a view is live. The view also holds the buffers, so a second
    /// concurrent view over the same `ViewBuffers` is a compile error — a caller
    /// that needs two live views owns two `ViewBuffers`.
    pub fn view<'a>(&'a mut self, batch: &'a ZSetBatch, schema: &Schema) -> ZSetBatchView<'a> {
        let pk_stride = schema.pk_stride();
        let regions = self.regions(batch, schema);
        // The three regions the evaluator reads per row are hoisted out of the
        // list; `col_data` / `null_bmp` are read once per morsel and stay indexed.
        let (pk, blob) = (regions[REG_PK], regions[regions.len() - 1]);
        ZSetBatchView {
            regions,
            pk,
            blob,
            batch,
            pk_stride,
        }
    }

    /// `batch` as the §6 canonical region list: PK, weight, null bitmap, one
    /// region per payload slot in slot order, blob heap last — exactly what
    /// [`gnitz_wire::wal::encode`] frames.
    ///
    /// Row counts are `ZSetBatch::validate`'s rule and are not restated; what is
    /// asserted here is the built region's own length and variant, which
    /// validate cannot state for a batch that never went through it.
    pub(crate) fn regions<'a>(&'a mut self, batch: &'a ZSetBatch, schema: &Schema) -> Vec<&'a [u8]> {
        let (rows, pk_stride, npc) = (batch.len(), schema.pk_stride(), schema.num_payload_cols());

        build_pk_region_into(&mut self.pk_region, &batch.pks, pk_stride, schema);
        self.blob.clear();
        self.str_cols.resize_with(npc, Vec::new);
        // String and Blob are separate arms because their cell iterators have
        // different concrete types, so one arm could not produce both.
        for (pi, ci, _) in schema.payload_columns() {
            match &batch.columns[ci] {
                ColData::Strings(v) => encode_german_col_into(
                    &mut self.str_cols[pi],
                    &batch.nulls,
                    pi,
                    v.iter().map(|o| o.as_deref().map(str::as_bytes)),
                    &mut self.blob,
                ),
                ColData::Bytes(v) => encode_german_col_into(
                    &mut self.str_cols[pi],
                    &batch.nulls,
                    pi,
                    v.iter().map(|o| o.as_deref()),
                    &mut self.blob,
                ),
                ColData::Fixed(_) => {}
            }
        }

        // Buffers are final; the shared reborrow lends them for the caller's
        // lifetime. The element type is declared so both arms coerce to `&[u8]`.
        let me = &*self;
        let mut regions: Vec<&[u8]> = Vec::with_capacity(gnitz_wire::wal::num_regions(npc));
        regions.push(&me.pk_region);
        regions.push(as_le_bytes(&batch.weights));
        regions.push(as_le_bytes(&batch.nulls));
        // One length rule, one place. Every region is indexed absolutely — by a
        // kernel (`col_data(pi, sz)[row * sz ..]`, `get_pk_bytes`) or by the
        // framer — so a wrong length is an out-of-bounds read one crate away
        // from its cause.
        assert_eq!(me.pk_region.len(), rows * pk_stride, "pk region length");
        for (pi, ci, col) in schema.payload_columns() {
            let cd = &batch.columns[ci];
            // The variant is decided by the *declared* type, never by the one
            // found: a String-typed column carrying `Fixed` has a valid
            // `rows * 16` length, so the length assert below cannot see it, and
            // the string kernels would read those bytes as German cells.
            // `ZSetBatch::validate` states the same rule for the push path; a
            // view can be built for a batch that never went through it.
            assert!(
                cd.matches_type(col.type_code),
                "ZSetBatch column {ci}: ColData variant contradicts schema type {:?}",
                col.type_code
            );
            let r: &[u8] = match cd {
                ColData::Fixed(v) => v,
                ColData::Strings(_) | ColData::Bytes(_) => &me.str_cols[pi],
            };
            assert_eq!(
                r.len(),
                rows * col.type_code.wire_stride(),
                "payload region {pi} (column {ci}) length"
            );
            regions.push(r);
        }
        regions.push(&me.blob); // the blob arena is always the last region
        regions
    }
}

/// Build the PK region as **order-preserving big-endian** (OPK) bytes. The
/// in-memory [`PkColumn`] holds native LE values; this is the single client-side
/// encode point (the server stores the region verbatim and `decode_wal_block`
/// does the inverse). `schema` supplies per-column type codes for signed
/// sign-flipping. The framer places the region at its aligned offset, so this
/// writes just the tightly-packed region bytes.
fn build_pk_region_into(dst: &mut Vec<u8>, pks: &PkColumn, pk_stride: usize, schema: &Schema) {
    debug_assert_eq!(pks.stride as usize, pk_stride, "PK column stride != schema stride");
    let row_count = pks.buf.len().checked_div(pk_stride).unwrap_or(0);
    resize_zeroed(dst, pks.buf.len());
    // Collect (col_size, type_code) once; avoids schema re-iteration per row. A
    // scalar PK is the one-column case of the same walk: unsigned → plain
    // big-endian, signed (including a lone 16-byte I128 join key) → big-endian
    // with the leading sign bit flipped.
    let col_info: Vec<(usize, u8)> = schema.pk_col_codes().collect();
    for row in 0..row_count {
        let (lo, hi) = (row * pk_stride, (row + 1) * pk_stride);
        gnitz_wire::encode_pk_tuple(col_info.iter().copied(), &pks.buf[lo..hi], &mut dst[lo..hi]);
    }
}

/// `dst` as exactly `n` zeroed bytes, reusing its capacity. Not `Vec::resize`:
/// that is a per-element write loop, which LLVM turns into a memset only from
/// `-O1` up — at `opt-level=0`, the build the whole E2E suite runs, it costs
/// ~43 instructions per byte on every client push.
fn resize_zeroed(dst: &mut Vec<u8>, n: usize) {
    dst.clear();
    dst.reserve(n);
    // SAFETY: `reserve` guarantees `n` bytes of capacity, and they are zeroed
    // before `set_len` publishes them, so no uninitialized byte is observable.
    unsafe {
        std::ptr::write_bytes(dst.as_mut_ptr(), 0, n);
        dst.set_len(n);
    }
}

/// Encode a STRING/BLOB column region: one 16-byte German-string struct per row
/// (a zeroed struct for a null or `None` cell), spilling long values into
/// `blob`. Shared by the STRING and BLOB arms — both are byte-oriented German
/// strings, differing only in the source cell type.
fn encode_german_col_into<'a>(
    dst: &mut Vec<u8>,
    nulls: &[u64],
    payload_idx: usize,
    cells: impl ExactSizeIterator<Item = Option<&'a [u8]>>,
    blob: &mut Vec<u8>,
) {
    dst.clear();
    dst.reserve(cells.len() * 16);
    for (row, val) in cells.enumerate() {
        let is_null = null_word_get(nulls[row], payload_idx);
        if let (false, Some(b)) = (is_null, val) {
            dst.extend_from_slice(&gnitz_wire::encode_german_string(b, blob));
        } else {
            dst.extend_from_slice(&[0u8; 16]);
        }
    }
}

// ── The view ─────────────────────────────────────────────────────────────────

/// A [`ZSetBatch`] presented as §6 regions. Every accessor is one index — the
/// slot-to-buffer question was answered once, when the list was built.
///
/// `pk` and `blob` are the same two regions the list already holds, hoisted out
/// of it because they are read per row.
/// `gnitz_expr::assert_batchview_consistent` pins the region and per-row
/// readings against each other.
///
/// The view also carries the batch it was built over, so a caller that needs
/// both — registers through the evaluator, a `ColData` cell directly — passes
/// one value instead of a `(&view, &batch)` pair that would type-check even when
/// mismatched.
pub struct ZSetBatchView<'a> {
    regions: Vec<&'a [u8]>,
    pk: &'a [u8],
    blob: &'a [u8],
    batch: &'a ZSetBatch,
    pk_stride: usize,
}

impl<'a> ZSetBatchView<'a> {
    /// The batch this view presents.
    pub fn batch(&self) -> &'a ZSetBatch {
        self.batch
    }
}

impl gnitz_expr::RowSource for ZSetBatchView<'_> {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let s = self.pk_stride;
        &self.pk[row * s..row * s + s]
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.batch.nulls[row]
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, pi: usize, sz: usize) -> &[u8] {
        &self.regions[REG_PAYLOAD_START + pi][row * sz..row * sz + sz]
    }

    #[inline(always)]
    fn blob(&self) -> &[u8] {
        self.blob
    }
}

impl gnitz_expr::BatchView for ZSetBatchView<'_> {
    /// The slot was resolved when the list was built, so `col_size` only has to
    /// agree — it comes from the program's resolve-time schema, the region from
    /// the view's, and this is the one place the two meet.
    #[inline(always)]
    fn col_data(&self, pi: usize, col_size: usize) -> &[u8] {
        let region = self.regions[REG_PAYLOAD_START + pi];
        debug_assert_eq!(region.len() % col_size, 0, "col_data({pi}, {col_size}) width mismatch");
        region
    }

    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        self.regions[REG_NULL_BMP]
    }

    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (self.pk, self.pk_stride)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{ColumnDef, TypeCode};
    use crate::protocol::wal_block::{decode_wal_block_verified, encode_wal_block};
    use gnitz_expr::{CmpOp, LogicalInstr, LogicalProgram, RowSource, SchemaFacts, StrOp};

    // ── Fixture A: permuted, non-contiguous compound PK ──────────────────────
    //
    // ci0 U32   PK, SECOND in the PK list -> OPK offset 8
    // ci1 I32   payload slot 0, non-nullable
    // ci2 I64   payload slot 1, nullable
    // ci3 I64   PK, FIRST in the PK list  -> OPK offset 0
    // ci4 STRING payload slot 2, nullable
    // ci5 BLOB   payload slot 3, nullable
    // ci6 U128   payload slot 4, non-nullable
    //
    // A naive `payload_col_idx` (the identity) and a column-order OPK
    // derivation both pass on a PK-at-column-0 schema and fail here.

    const PK3: [i64; 3] = [-5, 100, 1i64 << 40];
    const PK0: [u32; 3] = [7, 0, u32::MAX];
    const C1: [i32; 3] = [10, -20, 30];
    const C2: [i64; 3] = [1000, 0 /* NULL */, -3000];
    const C6: [u128; 3] = [1, u128::MAX, 1u128 << 100];
    /// Row 0 spills (> 12 bytes), row 1 is inline, row 2 is NULL.
    const C4: [&str; 3] = ["aaa long value that spills", "short", ""];
    const C5: [&[u8]; 3] = [b"zzz long blob value spilling", b"bb", b""];

    fn fixture_a_schema() -> Schema {
        Schema::from_parts(
            vec![
                ColumnDef::new("k1", TypeCode::U32, false),
                ColumnDef::new("a", TypeCode::I32, false),
                ColumnDef::new("b", TypeCode::I64, true),
                ColumnDef::new("k0", TypeCode::I64, false),
                ColumnDef::new("s", TypeCode::String, true),
                ColumnDef::new("z", TypeCode::Blob, true),
                ColumnDef::new("w", TypeCode::U128, false),
            ],
            vec![3, 0],
        )
        .expect("fixture A is a client-valid schema")
    }

    /// One PK row, laid out in **PK-list order** (I64 at 0..8, U32 at 8..12) —
    /// the order `build_pk_region_into`'s `Bytes` arm walks. Built with
    /// `push_bytes`, never `push_u128`, whose `Bytes` arm would sign-extend the
    /// negative I64 over the U32 column.
    fn pk12(k0: i64, k1: u32) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..8].copy_from_slice(&k0.to_le_bytes());
        out[8..12].copy_from_slice(&k1.to_le_bytes());
        out
    }

    fn fixture_a_batch() -> ZSetBatch {
        let mut pks = PkColumn {
            stride: 12,
            buf: Vec::new(),
        };
        for row in 0..3 {
            pks.push_bytes(&pk12(PK3[row], PK0[row]));
        }
        let mut c1 = Vec::new();
        let mut c2 = Vec::new();
        for row in 0..3 {
            c1.extend_from_slice(&C1[row].to_le_bytes());
            c2.extend_from_slice(&C2[row].to_le_bytes());
        }
        ZSetBatch {
            pks,
            weights: vec![1, -1, 3],
            // Row 1 nulls payload slot 1 (ci2); row 2 nulls slots 2 and 3
            // (ci4/ci5) — so the bitmap is not uniformly zero.
            nulls: vec![0, 0b10, 0b1100],
            columns: vec![
                ColData::Fixed(vec![]), // ci0: PK placeholder
                ColData::Fixed(c1),
                ColData::Fixed(c2),
                ColData::Fixed(vec![]), // ci3: PK placeholder
                ColData::Strings(vec![Some(C4[0].to_string()), Some(C4[1].to_string()), None]),
                ColData::Bytes(vec![Some(C5[0].to_vec()), Some(C5[1].to_vec()), None]),
                ColData::Fixed(C6.iter().flat_map(|v| v.to_le_bytes()).collect()),
            ],
        }
    }

    /// Every payload slot of fixture A as `(slot, width)`.
    const A_SLOTS: [(usize, usize); 5] = [(0, 4), (1, 8), (2, 16), (3, 16), (4, 16)];

    // ── Fixture B: a single narrow PK — the shape real traffic has ───────────

    fn fixture_b_schema() -> Schema {
        Schema::from_parts(
            vec![
                ColumnDef::new("k", TypeCode::U32, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            vec![0],
        )
        .expect("fixture B is a client-valid schema")
    }

    const B_PK: [u64; 3] = [1, 2, u32::MAX as u64];

    fn fixture_b_batch() -> ZSetBatch {
        let mut v = Vec::new();
        for x in [7i64, -8, 9] {
            v.extend_from_slice(&x.to_le_bytes());
        }
        ZSetBatch {
            pks: PkColumn::from_u128s(4, B_PK.iter().map(|&x| x as u128)),
            weights: vec![1; 3],
            nulls: vec![0; 3],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(v)],
        }
    }

    // ── The region/per-row contract ──────────────────────────────────────────

    #[test]
    fn zsetbatchview_satisfies_the_region_per_row_contract() {
        let schema = fixture_a_schema();
        let batch = fixture_a_batch();
        let mut bufs = ViewBuffers::default();
        {
            let view = bufs.view(&batch, &schema);
            gnitz_expr::assert_batchview_consistent(&view, 3, &A_SLOTS);
        }
        // A residual can legitimately get an empty batch: every region length
        // must degrade to 0.
        let empty = ZSetBatch::new(&schema);
        let view = bufs.view(&empty, &schema);
        gnitz_expr::assert_batchview_consistent(&view, 0, &A_SLOTS);
    }

    #[test]
    fn locate_addresses_the_pk_region_the_builder_hands_out() {
        // Take the address from `locate`, never from the test's own arithmetic.
        let check = |schema: &Schema, batch: &ZSetBatch, ci: usize, want: &[&[u8]]| {
            let mut bufs = ViewBuffers::default();
            let view = bufs.view(batch, schema);
            let (byte_off, size, tc) = match SchemaFacts::locate(schema, ci) {
                gnitz_expr::ColumnLocator::Pk {
                    byte_off,
                    size,
                    type_code,
                } => (byte_off as usize, size as usize, type_code),
                other => panic!("column {ci} must locate to the PK region, got {other:?}"),
            };
            for (row, want_row) in want.iter().enumerate() {
                let opk = &view.get_pk_bytes(row)[byte_off..byte_off + size];
                let native = gnitz_wire::decode_pk_column_owned(opk, tc);
                assert_eq!(&native[..size], *want_row, "column {ci}, row {row}");
            }
        };

        let a_schema = fixture_a_schema();
        let a_batch = fixture_a_batch();
        let k0: Vec<[u8; 8]> = PK3.iter().map(|v| v.to_le_bytes()).collect();
        let k1: Vec<[u8; 4]> = PK0.iter().map(|v| v.to_le_bytes()).collect();
        check(
            &a_schema,
            &a_batch,
            3,
            &k0.iter().map(|b| b.as_slice()).collect::<Vec<_>>(),
        );
        check(
            &a_schema,
            &a_batch,
            0,
            &k1.iter().map(|b| b.as_slice()).collect::<Vec<_>>(),
        );

        let b_schema = fixture_b_schema();
        let b_batch = fixture_b_batch();
        let bk: Vec<[u8; 4]> = B_PK.iter().map(|v| (*v as u32).to_le_bytes()).collect();
        check(
            &b_schema,
            &b_batch,
            0,
            &bk.iter().map(|b| b.as_slice()).collect::<Vec<_>>(),
        );
    }

    // ── The shared evaluator over a client batch ─────────────────────────────

    #[test]
    fn the_shared_evaluator_reads_a_client_batch() {
        let schema = fixture_a_schema();
        let batch = fixture_a_batch();
        let mut bufs = ViewBuffers::default();
        let view = bufs.view(&batch, &schema);

        // ci3 is a PK column: `LoadColInt` accepts one (`ColKind::FixedInt` is
        // not payload-only) and lowers to `Instr::LoadPk`, so this exercises
        // `locate`'s PK arm, its payload arm and the region addressing together.
        let ev = LogicalProgram::new(
            vec![
                LogicalInstr::LoadColInt { dst: 0, col: 3 },
                LogicalInstr::LoadColInt { dst: 1, col: 1 },
                LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
            ],
            3,
            2,
            vec![],
        )
        .resolve_scalar(&schema)
        .expect("program resolves against the client schema");

        for row in 0..3 {
            let (v, is_null) = ev.eval_row(&view, row);
            assert_eq!(v, PK3[row] + C1[row] as i64, "row {row}");
            // The program names only non-nullable slots, so `no_nulls` is on.
            assert!(!is_null, "row {row} must not be null");
        }
    }

    #[test]
    fn nullable_payload_null_bits_reach_the_evaluator() {
        let schema = fixture_a_schema();
        let batch = fixture_a_batch();
        let mut bufs = ViewBuffers::default();
        let view = bufs.view(&batch, &schema);

        // A different program from the one above: `is_strictly_non_nullable`
        // tests only the slots the instructions name, so naming ci2 (payload
        // slot 1, nullable) is what forces `no_nulls` off.
        let ev = LogicalProgram::new(
            vec![
                LogicalInstr::LoadColInt { dst: 0, col: 3 },
                LogicalInstr::LoadColInt { dst: 1, col: 2 },
                LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
            ],
            3,
            2,
            vec![],
        )
        .resolve_scalar(&schema)
        .expect("program resolves against the client schema");

        assert_eq!(ev.eval_row(&view, 0), (PK3[0] + C2[0], false));
        assert!(ev.eval_row(&view, 1).1, "row 1 nulls the nullable column");
        assert_eq!(ev.eval_row(&view, 2), (PK3[2] + C2[2], false));
    }

    #[test]
    fn filter_over_the_region_path() {
        let schema = fixture_a_schema();
        let batch = fixture_a_batch();
        let mut bufs = ViewBuffers::default();
        let view = bufs.view(&batch, &schema);

        // ci2 > 0: row 0 passes (1000), row 1 is NULL (dropped by
        // `bool_bits & !null_bits`), row 2 fails (-3000).
        let ev = LogicalProgram::new(
            vec![
                LogicalInstr::LoadColInt { dst: 0, col: 2 },
                LogicalInstr::LoadConst { dst: 1, val: 0 },
                LogicalInstr::Cmp {
                    op: CmpOp::Gt,
                    dst: 2,
                    a: 0,
                    b: 1,
                },
            ],
            3,
            2,
            vec![],
        )
        .resolve_filter(&schema)
        .expect("predicate resolves against the client schema");

        let mut ranges: Vec<(usize, usize)> = Vec::new();
        ev.filter(&view, 3, |s, e| ranges.push((s, e)));
        assert_eq!(ranges, vec![(0, 1)]);
    }

    #[test]
    fn string_columns_compare_through_the_shared_blob_heap() {
        let schema = fixture_a_schema();
        let batch = fixture_a_batch();
        let mut bufs = ViewBuffers::default();
        let view = bufs.view(&batch, &schema);

        // STRING (ci4) vs BLOB (ci5): both pass `check_col(GermanString)`.
        let ev = LogicalProgram::new(
            vec![LogicalInstr::StrColCol {
                op: StrOp::Lt,
                dst: 0,
                col_a: 4,
                col_b: 5,
            }],
            1,
            0,
            vec![],
        )
        .resolve_scalar(&schema)
        .expect("string compare resolves against the client schema");

        // Row 0 spills in both columns: with a per-column arena the two cells
        // would share a heap offset and the comparison would read the wrong
        // bytes (and come out equal, not less-than).
        for row in 0..2 {
            let want = (C4[row].as_bytes() < C5[row]) as i64;
            assert_eq!(ev.eval_row(&view, row), (want, false), "row {row}");
        }
        assert!(ev.eval_row(&view, 2).1, "row 2 nulls both string columns");
    }

    #[test]
    #[should_panic(expected = "ColData variant contradicts schema type")]
    fn region_builder_rejects_a_batch_whose_coldata_contradicts_the_schema() {
        let schema = fixture_a_schema();
        let mut batch = fixture_a_batch();
        // The one fault `ZSetBatch::validate` structurally cannot see: it
        // matches on the variant it finds, never against the declared type.
        batch.columns[4] = ColData::Fixed(vec![0u8; 3 * 16]);
        let mut bufs = ViewBuffers::default();
        let _ = bufs.view(&batch, &schema);
    }

    #[test]
    fn repeated_views_do_not_grow_the_buffers() {
        let schema = fixture_a_schema();
        let batch = fixture_a_batch();
        let mut reused = ViewBuffers::default();
        for _ in 0..3 {
            let view = reused.view(&batch, &schema);
            assert!(!view.blob().is_empty(), "fixture A spills into the arena");
        }
        let mut fresh = ViewBuffers::default();
        {
            let _ = fresh.view(&batch, &schema);
        }
        assert!(!fresh.blob.is_empty(), "the fixture must exercise the arena");
        // Every buffer that grows with the data: three views must leave each of
        // them the size one view does.
        assert_eq!(reused.blob.len(), fresh.blob.len(), "blob arena");
        assert_eq!(reused.pk_region.len(), fresh.pk_region.len(), "pk region");
        let cells: Vec<usize> = reused.str_cols.iter().map(Vec::len).collect();
        assert_eq!(cells, fresh.str_cols.iter().map(Vec::len).collect::<Vec<_>>());
        assert!(cells.iter().any(|&n| n > 0), "the fixture must exercise the cells");
    }

    // ── The encode path shares the builder ───────────────────────────────────

    #[test]
    fn fixture_round_trips_through_encode_and_decode() {
        for (schema, batch, tid) in [
            (fixture_a_schema(), fixture_a_batch(), 77u32),
            (fixture_b_schema(), fixture_b_batch(), 5u32),
        ] {
            let encoded = encode_wal_block(&schema, tid, &batch);
            let (decoded, got_tid) = decode_wal_block_verified(&encoded, &schema).expect("block decodes");
            assert_eq!(got_tid, tid);
            assert_eq!(decoded, batch, "batch must survive encode -> decode");
        }
    }
}
