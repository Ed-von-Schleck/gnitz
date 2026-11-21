//! [`MapPlan`] — the columnar driver behind every DBSP map and projection.
//!
//! A filter needs nothing on top of the resolved program and is a bare
//! `gnitz_expr::RowFilter` wherever one is held; a map is this plan, which adds
//! the PK, weight and column copies around the computed columns
//! `gnitz_expr::MapEval` writes.

use gnitz_expr::{ColCopy, ColumnLocator, ExprValidateErr, LogicalProgram, MapEval};

use super::copy::{ColMoves, RowWindow, copy_window, rebase_copied_strings};
use super::key_pack::{FoldCols, IDENTITY_CHUNK, KeyPacker, locate_key_col, pk_run};
use crate::repr::{Batch, DirectWriter, MemBatch, copy_runs};
use crate::schema::{DerivedSchema, SchemaColumn, SchemaDescriptor};
use gnitz_wire::TypeCode;

/// Average survivor-run length below which a map that computes, or that copies
/// fixed-width columns alone, copies its survivors into one range first: below
/// it, the copy costs less than the kernel's setup per range.
const COMPACT_RUN_LEN: usize = 128;

/// [`COMPACT_RUN_LEN`] for a copy-only map packing a reindex key, whose
/// per-range setup is smaller.
const PACK_COMPACT_RUN_LEN: usize = 24;

/// Where a map's output PK region comes from.
#[derive(Clone)]
enum PkSource {
    /// Copy the input PK region verbatim, into an output schema whose PK is the
    /// input's.
    Inherit,
    /// Pack the reindex columns' OPK bytes contiguously into the output PK: a
    /// join's `_join_pk`, or a set operation's element key. Its stride is the key
    /// slots' summed widths, which may differ from the input's: a slot can be
    /// wider than its column (U64 joined with I64 packs as I128), and a string
    /// key packs as a U128 hash.
    /// A row with any of the `drop_nulls` null bits set is dropped: a
    /// [`gnitz_wire::NullKeys::Drop`] reindex's nullable key columns.
    Pack { packer: KeyPacker, drop_nulls: u64 },
    /// Hash each output row's payload columns into its PK.
    HashRow(FoldCols),
}

// ---------------------------------------------------------------------------
// MapPlan
// ---------------------------------------------------------------------------

/// Map/projection: the resolved program's column copies and computed columns,
/// where the output PK comes from, and the output schema it stamps.
#[derive(Clone)]
pub struct MapPlan {
    ev: MapEval,
    /// The copied columns of `ev`'s map.
    moves: ColMoves,
    pk_source: PkSource,
    output_schema: SchemaDescriptor,
    /// The map reproduces its input, given the input's PK carried through.
    reproduces_input: bool,
}

/// A map program `resolve_map` refuses against its two schemas, as a circuit
/// node's refusal.
fn map_mismatch(e: ExprValidateErr) -> String {
    format!("map: program/schema mismatch: {e}")
}

/// Set the PK of each of `output`'s rows, whose payload and null words are
/// written, to the [`FoldCols`] digest of its payload columns, so equal rows
/// share a PK. Two distinct rows whose 128-bit digests collide become one
/// element; accepted, not checked.
fn stamp_hash_row_pks(output: &mut DirectWriter<'_>, fold: &FoldCols) {
    let (first, n) = (output.first_row(), output.rows());
    // The PK *is* the digest, so the OPK region is its big-endian bytes.
    const KEY_BYTES: usize = std::mem::size_of::<u128>();
    assert_eq!(output.schema.pk_stride(), KEY_BYTES, "a hash-row PK is one U128 column");
    // Hashing borrows `output` and the write-back mutates it, so keys are staged
    // per chunk.
    let mut keys = [0u128; IDENTITY_CHUNK];
    for start in (0..n).step_by(IDENTITY_CHUNK) {
        let end = (start + IDENTITY_CHUNK).min(n);
        let keys = &mut keys[..end - start];
        fold.identities(&output.written(), first + start, keys);
        let pk = &mut output.pk_mut()[start * KEY_BYTES..end * KEY_BYTES];
        for (key, dst) in keys.iter().zip(pk.as_chunks_mut::<KEY_BYTES>().0) {
            *dst = key.to_be_bytes();
        }
    }
}

/// Output schema of a HashRow Map: a U128 PK, then each projected column at its
/// slot type and its source nullability. Typed at the slot, the promotion is
/// what the copy kernel screens.
fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    cols: &[gnitz_wire::ReindexSlot],
) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(TypeCode::U128, false));
    for &(c, t) in cols {
        // A key column: a float is refused, as for a reindex key.
        let (src, _) = locate_key_col(in_schema, c, "hash-row map")?;
        b.push(SchemaColumn::new(t, src.nullable));
    }
    b.finish().map_err(|e| format!("hash-row map: output {e}"))
}

impl MapPlan {
    /// The plan for a circuit's MAP node: one derivation of its `(output schema,
    /// map program, PK source)` triple, and the trust boundary each kind's
    /// client-supplied column list clears. Every kind ends in the same
    /// [`Self::from_map`], so an elided map is validated like any other.
    pub fn from_wire(in_schema: &SchemaDescriptor, mk: &gnitz_wire::MapKind) -> Result<Self, String> {
        let (out_schema, prog, pk_source) = match mk {
            gnitz_wire::MapKind::Compute(map) => return Self::from_compute_map(in_schema, map),

            gnitz_wire::MapKind::Reindex { keep, key, nulls } => {
                let packer = KeyPacker::new(in_schema, key)?;
                let out_schema = packer.output_schema(in_schema, keep)?;
                let drop_nulls = match nulls {
                    gnitz_wire::NullKeys::Drop => {
                        in_schema.payload_bits(key.iter().map(|&(c, _)| c)) & in_schema.nullable_payload_slots()
                    }
                    gnitz_wire::NullKeys::Keep => 0,
                };
                let pk_source = PkSource::Pack { packer, drop_nulls };
                (out_schema, LogicalProgram::copy_cols(keep), pk_source)
            }

            gnitz_wire::MapKind::HashRow { cols } => {
                let out_schema = hashrow_output_schema(in_schema, cols)?;
                let proj: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
                let fold = FoldCols::new(out_schema.payload_locators());
                (out_schema, LogicalProgram::copy_cols(&proj), PkSource::HashRow(fold))
            }
        };
        Self::from_map(prog, in_schema, out_schema, pk_source).map_err(map_mismatch)
    }

    /// A computed projection: [`gnitz_wire::MapKind::Compute`]'s whole body, and
    /// also a read spec's pre-map. The PK region is the input's, copied verbatim
    /// ([`PkSource::Inherit`]); the payload slots are the declared `out_cols`.
    pub(crate) fn from_compute_map(in_schema: &SchemaDescriptor, map: &gnitz_wire::ComputeMap) -> Result<Self, String> {
        // The input's PK region, then one payload column per declared
        // `(type_code, nullable)` slot. The declared slots are the layout;
        // `from_map` catches a program disagreeing with them.
        let mut b = DerivedSchema::new();
        b.push_pk_of(in_schema);
        for &(tc, nullable) in &map.out_cols {
            b.push(SchemaColumn::new(tc, nullable));
        }
        let out_schema = b.finish().map_err(|e| format!("compute map: output {e}"))?;
        // The only map whose program is client bytes; every other kind builds
        // one from a column list.
        let prog = LogicalProgram::from_blob(&map.program).map_err(|e| format!("map: invalid program: {e}"))?;
        Self::from_map(prog, in_schema, out_schema, PkSource::Inherit).map_err(map_mismatch)
    }

    /// The map `prog` from `in_schema` onto `output_schema`, every row keeping
    /// its key: `output_schema`'s key columns are its input's.
    pub fn inheriting(
        prog: LogicalProgram,
        in_schema: &SchemaDescriptor,
        output_schema: SchemaDescriptor,
    ) -> Result<Self, ExprValidateErr> {
        Self::from_map(prog, in_schema, output_schema, PkSource::Inherit)
    }

    /// Map plan from a logical expression program. A pure projection is the
    /// special case where the program computes nothing and every sink is a
    /// column copy (see [`LogicalProgram::copy_cols`]): the plan reduces to the
    /// map's column moves.
    fn from_map(
        logical: LogicalProgram,
        in_schema: &SchemaDescriptor,
        output_schema: SchemaDescriptor,
        pk_source: PkSource,
    ) -> Result<Self, ExprValidateErr> {
        let (ev, copies) = logical.resolve_map(in_schema, &output_schema)?;
        // One layout on both sides, which fixes equal payload counts and types,
        // and every slot copied from its own input slot. With no emits copy `i`
        // writes slot `i`, and the program is never run, so instructions no
        // sink reads do not count against it.
        let reproduces_input = !ev.emits_anything()
            && in_schema.same_layout(&output_schema)
            && copies.iter().enumerate().all(|(i, c)| c.src.payload_slot() == Some(i));
        let moves = ColMoves::new(copies, in_schema);
        Ok(MapPlan {
            ev,
            moves,
            pk_source,
            output_schema,
            reproduces_input,
        })
    }

    /// The schema this plan stamps on its output.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.output_schema
    }

    /// The input column behind each output payload slot, when this map only
    /// re-keys its rows onto leading bytes of their own PK: the input, read in
    /// its own order, then holds each output key's rows together. `None` for
    /// every other map.
    pub fn rekeys_onto_pk_prefix(&self) -> Option<Vec<ColumnLocator>> {
        let PkSource::Pack { packer, drop_nulls } = &self.pk_source else {
            return None;
        };
        packer
            .identity_columns()
            .as_deref()
            .and_then(pk_run)
            .filter(|&(at, _)| at == 0)?;
        // A PK column is never NULL, and a reindex copies each kept column at
        // its own type.
        debug_assert!(*drop_nulls == 0 && !self.ev.emits_anything());
        debug_assert!(self.copies().iter().all(|c| c.width == c.src.size()));
        Some(self.copies().iter().map(|c| c.src).collect())
    }

    /// The input columns this map copies into output payload slots.
    pub fn copies(&self) -> &[ColCopy] {
        self.moves.copies()
    }

    /// True iff this map copies its input's PK region verbatim, and so keeps
    /// every row under its own key.
    pub fn inherits_pk(&self) -> bool {
        matches!(self.pk_source, PkSource::Inherit)
    }

    /// True iff running this map would reproduce its input batch. A compiler
    /// elides such a node entirely and lets its consumers read the input.
    pub fn is_identity(&self) -> bool {
        self.inherits_pk() && self.reproduces_input
    }

    /// The map over `in_batch`, less a [`gnitz_wire::NullKeys::Drop`] reindex's
    /// NULL-keyed rows, into a fresh unconsolidated output.
    pub fn evaluate_map_batch(&mut self, in_batch: &MemBatch) -> Batch {
        let whole = [(0, in_batch.len())];
        let runs;
        let ranges: &[(usize, usize)] = match self.pk_source {
            PkSource::Pack { drop_nulls, .. } if drop_nulls != 0 => {
                runs = in_batch.runs_without_nulls(drop_nulls);
                &runs
            }
            _ => &whole,
        };
        // Sized exactly: a first reservation would round a small delta's rows up.
        let mut output = Batch::with_capacity(&self.output_schema, crate::repr::range_rows(ranges));
        self.map_ranges(in_batch, &mut output, ranges);
        output
    }

    /// Map every `[start, end)` range of `src`, in order, onto the tail of `out`,
    /// which is in this plan's output schema.
    pub(crate) fn append_map_ranges(&mut self, src: &MemBatch, out: &mut Batch, ranges: &[(usize, usize)]) {
        debug_assert!(
            !matches!(self.pk_source, PkSource::Pack { drop_nulls, .. } if drop_nulls != 0),
            "a NULL-dropping reindex maps through evaluate_map_batch"
        );
        self.map_ranges(src, out, ranges);
    }

    /// [`Self::append_map_ranges`], every row of `ranges` mapped. Ranges too
    /// short to feed the per-window kernel are gathered into one first.
    fn map_ranges(&mut self, src: &MemBatch, out: &mut Batch, ranges: &[(usize, usize)]) {
        let total = crate::repr::range_rows(ranges);
        if total == 0 {
            return;
        }
        let compact_below = match (self.ev.emits_anything(), &self.pk_source) {
            (true, _) => COMPACT_RUN_LEN,
            (false, PkSource::Pack { .. }) => PACK_COMPACT_RUN_LEN,
            // Fixed-width columns gather as cheaply as they map; a string would
            // be relocated twice.
            (false, _) if src.schema().string_payload_slots() == 0 => COMPACT_RUN_LEN,
            (false, _) => 0,
        };
        let starves_kernel = ranges.len() > 1 && total < ranges.len() * compact_below;
        let compacted = starves_kernel.then(|| {
            let mut c = Batch::with_capacity(src.schema(), total);
            c.append_ranges(src, ranges);
            c
        });
        let whole = [(0, total)];
        let (mb, ranges) = match &compacted {
            Some(c) => (c.as_mem_batch(), &whole[..]),
            None => (*src, ranges),
        };
        let src = &mb;
        // Declined, the carry reserves for relocated copies and string emits alike.
        let heap_at = out.carry_heap(&mb, self.moves.string_slots(), ranges, 0);
        // The reserved rows are uninitialized: `resolve_map` gives every output
        // payload slot one sink, and the writes below cover the PK, weight and
        // null regions.
        out.write_rows(total, |out| {
            match &self.pk_source {
                PkSource::Inherit => {
                    let stride = src.schema().pk_stride();
                    debug_assert_eq!(stride, out.schema.pk_stride(), "PkSource::Inherit: PK stride mismatch");
                    copy_runs::<0>(src.pk(), out.pk_mut(), ranges, stride);
                }
                PkSource::Pack { packer, .. } => {
                    debug_assert_eq!(out.schema.pk_stride(), packer.out_stride());
                    packer.pack_rows(out.pk_mut(), packer.out_stride(), &mb, ranges);
                }
                // Hashes the finished output row, below.
                PkSource::HashRow(_) => {}
            }
            copy_runs::<8>(src.weight(), out.weight_mut(), ranges, 8);
            let mut dst = 0;
            for &(start, end) in ranges {
                let w = RowWindow { src: start, dst, n: end - start };
                // The window's null words first: an emit sets its NULL bits on them.
                copy_window(src, out, &self.moves, w);
                self.ev.write_computed(&mb, start, w.n, out, dst);
                dst += w.n;
            }
            rebase_copied_strings(&self.moves, out, src.blob(), heap_at);
            if let PkSource::HashRow(fold) = &self.pk_source {
                stamp_hash_row_pks(out, fold);
            }
        });
    }
}

#[cfg(test)]
#[path = "tests/map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/map.rs"]
mod bench;
