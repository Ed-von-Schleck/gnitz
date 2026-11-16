//! The delta-trace inner join, equi, non-equi and keyless.
//!
//! One opcode over a compiler-baked [`JoinPlan`]: `Equi` co-groups the delta
//! against the trace on equal key, `Range` walks an ordered half-open span per
//! delta equality group, `Cross` walks the whole trace once against the whole
//! delta. All three name the same emission — the product of a contiguous
//! delta run with the cursor's current trace row — and all write
//! `[key, left payload…, right payload…]` over the SQL sides, keyed by the delta
//! PK, or by `[left PK…, right PK…]` under `Cross`.
use std::cell::Cell;
use std::cmp::Ordering;

use crate::repr::{
    Batch, CursorRow, ReadCursor, Run, copy_runs, pk_group_end, prorated_blob_cap, relocated_cell, row_long_bytes,
    runs_where, should_relocate_blob, stride, stride_dispatch, with_scratch,
};
use crate::schema::{DerivedSchema, SchemaDescriptor, band, compare_pk_ordering, group_floor, key_above};

use crate::algebra::MapPlan;
use crate::stream::OpenAt;
use crate::stream::clamp::clamp_cap;
use gnitz_expr::{ColCopy, ColMoves, ColumnLocator};
use gnitz_wire::{ClampKind, Heap, JoinKind, PkBuf, RangeRel, RowSource, TypeCode, null_word_at};

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// One join instruction's plan: how it probes its trace, where each input's
/// columns land in the output row, and the schema it writes under. The compiler
/// bakes it from the two input schemas, which the kernel never sees.
pub struct JoinPlan {
    output_schema: SchemaDescriptor,
    walk: Walk,
    /// Each trace payload column: where the rows the cursor walks hold it, and
    /// the output slot it fills, with the null bits it carries. Those rows are
    /// the trace's own, or the rows of the relation the trace re-keys.
    t_moves: ColMoves,
    /// The trace's columns lead the output payload: the delta is the right side.
    trace_leads: bool,
    /// The ceiling each trace row's weight is clamped to, from a floor of 0,
    /// where the stored trace is a clamp's input history and the joined side is
    /// that clamp's output; a row clamped to 0 meets nothing.
    trace_cap: Option<i64>,
}

/// Which trace walk a probe drives.
#[derive(Clone, Copy)]
enum Walk {
    /// Equal key: the walked rows each delta group's PK prefixes. Over a stored
    /// trace, those rows are the trace group at that PK. Over a source's rows,
    /// they are every source row under that leading key.
    Equi {
        /// The walked rows are the trace's own, so a group's rows arrive in the
        /// order the output gives their columns; otherwise they are the rows of
        /// the relation the trace re-keys.
        trace_ordered: bool,
    },
    /// An ordered span within an equality group.
    Range(RangeProbe),
    /// Every trace row, for every delta row: the keyless join. No key decides a
    /// match, so both key regions ride into the output as the pair key.
    Cross,
}

impl JoinPlan {
    /// The layout [`op_join_delta_trace`] writes under this plan.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.output_schema
    }

    /// Resolve a wire join node against its two input schemas, or name the
    /// precondition the circuit violated. A circuit is client-supplied catalog
    /// data, so these are refusals rather than debug asserts.
    ///
    /// The output is the key region, then both sides' payloads in SQL side
    /// order.
    pub fn from_wire(
        kind: JoinKind,
        delta_is_right: bool,
        delta: &SchemaDescriptor,
        trace: &SchemaDescriptor,
    ) -> Result<JoinPlan, String> {
        Self::new(kind, delta_is_right, delta, trace, Walked::Trace)
    }

    /// [`Self::from_wire`] for an equi join whose trace is not stored: the trace
    /// is `source`'s rows as `rekey` maps them, onto leading bytes of their own
    /// PK, so the probe walks `source`'s rows and reads the trace's columns out
    /// of them. The output is what the stored trace would give.
    pub fn over_source(
        delta_is_right: bool,
        delta: &SchemaDescriptor,
        source: &SchemaDescriptor,
        rekey: &MapPlan,
    ) -> Result<JoinPlan, String> {
        let cols = rekey
            .rekeys_onto_pk_prefix()
            .ok_or("join: the trace is not its source re-keyed onto leading primary-key columns")?;
        Self::new(
            JoinKind::Equi,
            delta_is_right,
            delta,
            rekey.output_schema(),
            Walked::Source(source, cols),
        )
    }

    /// The plan of `delta` against `trace`, read out of the rows `walked` names.
    fn new(
        kind: JoinKind,
        delta_is_right: bool,
        delta: &SchemaDescriptor,
        trace: &SchemaDescriptor,
        walked: Walked<'_>,
    ) -> Result<JoinPlan, String> {
        let trace_ordered = matches!(walked, Walked::Trace);
        let (walked, t_cols) = match walked {
            Walked::Trace => (trace, trace.payload_locators()),
            Walked::Source(source, cols) => (source, cols),
        };
        let (left, right) = match delta_is_right {
            true => (trace, delta),
            false => (delta, trace),
        };
        let mut b = DerivedSchema::new();
        b.push_pk_of(left);
        if kind == JoinKind::Cross {
            // The keyless join matches on neither key, so it mints the pair.
            b.push_pk_of(right);
        }
        b.push_payload_of(left);
        b.push_payload_of(right);
        let output_schema = b.finish().map_err(|e| format!("join: merged schema {e}"))?;

        // The left SQL side's payload leads.
        let t_first = match delta_is_right {
            true => 0,
            false => delta.num_payload_cols(),
        };
        let walk = match kind {
            JoinKind::Equi => {
                same_pk_types(delta, trace)?;
                Walk::Equi { trace_ordered }
            }
            JoinKind::Range { rel } => {
                same_pk_types(delta, trace)?;
                Walk::Range(RangeProbe::new(trace, rel, delta_is_right))
            }
            JoinKind::Cross => Walk::Cross,
        };
        let t_moves = ColMoves::new(
            (t_first..)
                .zip(t_cols)
                .map(|(slot, src)| ColCopy { src, slot, width: src.size() })
                .collect(),
            walked.nullable_payload_slots(),
        );
        Ok(JoinPlan {
            output_schema,
            walk,
            t_moves,
            trace_leads: delta_is_right,
            trace_cap: None,
        })
    }

    /// This plan over a trace holding `kind`'s input history in place of its
    /// output's integral. The integral of a clamp's output is the clamp of its
    /// input's integral, element by element, so each trace weight is clamped as
    /// it is read.
    pub fn clamped_trace(mut self, kind: ClampKind) -> JoinPlan {
        self.trace_cap = Some(clamp_cap(kind));
        self
    }
}

/// The rows a probe's cursor walks.
enum Walked<'a> {
    /// The trace's own.
    Trace,
    /// The rows of the relation the trace re-keys, in this schema, which hold
    /// the trace's payload columns at these locators.
    Source(&'a SchemaDescriptor, Vec<ColumnLocator>),
}

/// A keyed walk reads one side's PK region as the other's, so the two key
/// layouts must be identical down to the OPK encoding each column type implies.
fn same_pk_types(delta: &SchemaDescriptor, trace: &SchemaDescriptor) -> Result<(), String> {
    fn types(s: &SchemaDescriptor) -> impl Iterator<Item = TypeCode> {
        s.pk_columns().map(|c| c.type_code)
    }
    if types(delta).eq(types(trace)) {
        return Ok(());
    }
    Err("join: delta and trace PK column types differ (both sides must reindex at the pair's common type)".into())
}

// ---------------------------------------------------------------------------
// The range probe
// ---------------------------------------------------------------------------

/// The two decisions a range relation makes, plus the width the equality prefix
/// operates at.
#[derive(Clone, Copy)]
struct RangeProbe {
    /// Width in OPK bytes of the equality-pinned leading slots; the rest of a PK
    /// region is the range slot.
    eq_size: usize,
    /// Matches lie above the delta row's slot, not below it.
    above: bool,
    /// The cut falls below the delta row's own slot, so an equal trace slot lies
    /// on the far side of it.
    cuts_below: bool,
}

impl RangeProbe {
    /// Resolve a wire `left REL right` against the trace schema's key region.
    fn new(trace: &SchemaDescriptor, rel: RangeRel, delta_is_right: bool) -> RangeProbe {
        // The key is `[eq slots…, range slot]`, in PK order: the equality prefix
        // is every key column but the last.
        let eq_size = trace.pk_prefix_stride(trace.pk_cols().len() - 1);
        // `rel` relates left to right; the probe relates trace to delta.
        let rel = match delta_is_right {
            true => rel,
            false => rel.converse(),
        };
        RangeProbe {
            eq_size,
            above: rel.bounds_below(),
            cuts_below: rel.bounds_below() == rel.admits_equal(),
        }
    }

    /// The trace key range this probe covers for a delta row whose PK region is
    /// `pk` (`equality prefix ‖ range slot`). `None` when it is provably empty.
    fn cut_points(&self, pk: &[u8]) -> Option<(PkBuf, Option<PkBuf>)> {
        let (group, stride) = (&pk[..self.eq_size], pk.len());
        let split = match self.cuts_below {
            true => Some(PkBuf::from_bytes(pk)),
            false => key_above(pk, stride),
        };
        match self.above {
            true => band(split?, key_above(group, stride)),
            false => band(group_floor(group, stride), split),
        }
    }

    /// The greatest ordering a delta slot may have against a trace slot while
    /// still falling before the split that slot induces in the group: `Ge` and
    /// `Lt` put an equal slot before it, `Gt` and `Le` after.
    #[inline]
    fn last_before_split(&self) -> Ordering {
        match self.cuts_below {
            true => Ordering::Equal,
            false => Ordering::Less,
        }
    }
}

// ---------------------------------------------------------------------------
// The operator
// ---------------------------------------------------------------------------

/// One emission of a walk: the delta run `[rs, re)` against one trace row, by
/// its cursor handle.
#[derive(Clone, Copy)]
struct Pairing {
    rs: u32,
    re: u32,
    at: CursorRow,
    w_trace: i64,
}

impl Pairing {
    #[inline(always)]
    fn delta_rows(&self) -> std::ops::Range<usize> {
        self.rs as usize..self.re as usize
    }

    #[inline(always)]
    fn len(&self) -> usize {
        (self.re - self.rs) as usize
    }
}

/// Join delta rows against the trace, writing `[key, left payload…, right
/// payload…]` under [`JoinPlan::output_schema`].
///
/// The walk only records which delta run meets which trace row; the output is
/// then written one region at a time over that list. Emission is trace-major
/// under every probe: each trace row is walked once and meets one contiguous
/// delta run. So the output is in general unfolded and not
/// (PK, payload)-sorted — under `Cross` not even PK-sorted, since each trace row
/// re-emits the whole delta — and downstream re-sorts and consolidates it.
///
/// An equal-key walk claims its output consolidated where that order is the
/// output's own: each delta run met one trace row, or the trace's rows arrived
/// in output order and either lead the payload or met single delta rows.
pub fn op_join_delta_trace(delta: &Batch, trace: OpenAt<'_>, plan: &JoinPlan) -> Batch {
    debug_assert!(delta.is_consolidated());
    let n = delta.len();
    if n == 0 {
        return Batch::empty_with_schema(&plan.output_schema);
    }
    // Every trace row a walk can meet begins with a delta row's key under `Equi`,
    // with its equality prefix under `Range`, and with nothing under `Cross`.
    let prefix = match plan.walk {
        Walk::Equi { .. } => delta.schema().pk_stride(),
        Walk::Range(range) => range.eq_size,
        Walk::Cross => 0,
    };
    let cursor = &mut trace(&delta.get_pk_bytes(0)[..prefix], &delta.get_pk_bytes(n - 1)[..prefix]);
    // The thread's buffer, reused across calls; still grown rather than
    // pre-sized, since a walk can match nothing.
    with_scratch(&PAIRS, |pairs| {
        let mut emit = |rs: usize, re: usize, c: &ReadCursor| {
            debug_assert!(rs < re, "every walk emits a non-empty delta run");
            let w_trace = match plan.trace_cap {
                Some(cap) => c.current_weight().clamp(0, cap),
                None => c.current_weight(),
            };
            // Dropping a pairing keeps the rest in walk order.
            if w_trace == 0 {
                return;
            }
            pairs.push(Pairing {
                rs: rs as u32,
                re: re as u32,
                at: c.current_position(),
                w_trace,
            });
        };
        match plan.walk {
            Walk::Equi { .. } => equi_merge_walk(delta, cursor, emit),
            Walk::Range(range) => range_merge_walk(delta, cursor, range, emit),
            Walk::Cross => cursor.for_each_row_while(|_| true, |c| emit(0, n, c)),
        }
        debug_assert!(
            pairs.is_sorted_by(|a, b| a.rs <= b.rs),
            "every walk emits delta runs by ascending start"
        );
        let ordered = match plan.walk {
            // A delta run that meets several trace rows keeps the output order only
            // when they arrive in it and lead the payload or the run is one row.
            Walk::Equi { trace_ordered } => pairs
                .windows(2)
                .all(|w| w[0].rs != w[1].rs || (trace_ordered && (plan.trace_leads || w[0].len() == 1))),
            Walk::Range(_) | Walk::Cross => false,
        };
        let mut out = write_pairings(delta, cursor, plan, pairs);
        if ordered {
            out.certify_consolidated();
        }
        out
    })
}

thread_local! {
    /// The pairings buffer of [`op_join_delta_trace`], reused across calls.
    static PAIRS: Cell<Vec<Pairing>> = const { Cell::new(Vec::new()) };
}

/// The output rows `pairs` names, in list order, one region at a time, less any
/// whose weight product is zero.
fn write_pairings(delta: &Batch, cursor: &ReadCursor, plan: &JoinPlan, pairs: &[Pairing]) -> Batch {
    let rows: usize = pairs.iter().map(Pairing::len).sum();
    let output_schema = &plan.output_schema;
    if rows == 0 {
        return Batch::empty_with_schema(output_schema);
    }
    let d_schema = delta.schema();
    let mut out = Batch::with_capacity(output_schema, rows);

    // The heap is reserved once, with room for the long trace cells the
    // pairings relocate. They are summed only over a short list: there the
    // heap's regrowth costs more than the pass, while over a long list
    // regrowth is amortised and the pass is the larger cost.
    let trace_long = match pairs.len() <= MAX_RESERVE_SCAN_PAIRS {
        true => trace_long_bytes(cursor, plan, pairs),
        false => 0,
    };
    // The delta's heap rides whole unless relocating the emitted cells is
    // cheaper; the rows no pairing names are what it then holds dead.
    let strings = d_schema.string_payload_slots();
    let heap_at = match strings != 0 && !should_relocate_blob(delta.blob().len(), delta.len(), rows) {
        true => {
            // The union of the pairings' delta runs, as maximal sorted disjoint runs.
            let mut kept: Vec<(usize, usize)> = Vec::new();
            for (s, e) in delta_runs(pairs) {
                match kept.last_mut() {
                    Some(k) if s <= k.1 => k.1 = k.1.max(e),
                    _ => kept.push((s, e)),
                }
            }
            out.carry_heap(&delta.as_mem_batch(), strings, &kept, trace_long)
        }
        false => {
            // The delta's long cells are relocated as well, each output row
            // taking its share of the delta's heap.
            let room = trace_long + prorated_blob_cap(delta.blob().len(), delta.len(), rows);
            if room != 0 {
                out.reserve_blob(room);
            }
            None
        }
    };

    // The delta's payload follows the trace's where the trace is the left side,
    // and so does its key in a pair key.
    let d_first = if plan.trace_leads {
        plan.t_moves.copies().len()
    } else {
        0
    };
    let trace_row = |p: &Pairing| cursor.row_at(p.at);
    let any_ghost = out.write_rows(rows, |w| {
        let mut any_ghost = false;
        let (pk, weights, nulls) = w.fixed_mut();
        match plan.walk {
            // No key decides a match, so the pair `[left PK…, right PK…]` is minted
            // per row.
            Walk::Cross => {
                let (pk_len, d_len) = (output_schema.pk_stride(), d_schema.pk_stride());
                let (d_key, t_key) = match plan.trace_leads {
                    true => (pk_len - d_len..pk_len, 0..pk_len - d_len),
                    false => (0..d_len, d_len..pk_len),
                };
                // One window of every key at a time: each pairing's delta PKs
                // over its run, then its trace PK repeated over the same rows.
                let d_pk = delta.pk();
                let d_runs = pairs
                    .iter()
                    .map(|p| (&d_pk[p.rs as usize * d_len..p.re as usize * d_len], d_len, p.len()));
                stride_dispatch!(d_len, put_key_windows, pk, pk_len, d_key.start, d_runs);
                let t_runs = pairs.iter().map(|p| {
                    let (t_src, t_row) = trace_row(p);
                    (t_src.get_pk_bytes(t_row), 0, p.len())
                });
                stride_dispatch!(pk_len - d_len, put_key_windows, pk, pk_len, t_key.start, t_runs);
            }
            // A keyed plan's output key is the delta's PK region verbatim.
            _ => stride_dispatch!(d_schema.pk_stride(), copy_runs, delta.pk(), pk, delta_runs(pairs)),
        }
        {
            let src = delta.weight().as_chunks::<8>().0;
            let mut dst = weights.as_chunks_mut::<8>().0.iter_mut();
            for p in pairs {
                for (w_delta, w_out) in src[p.delta_rows()].iter().zip(&mut dst) {
                    let w = i64::from_le_bytes(*w_delta).wrapping_mul(p.w_trace);
                    any_ghost |= w == 0;
                    *w_out = w.to_le_bytes();
                }
            }
        }
        {
            // Each half's null bits rebase onto the slot its columns land at.
            let src = delta.null_bmp().as_chunks::<8>().0;
            let mut dst = nulls.as_chunks_mut::<8>().0.iter_mut();
            for p in pairs {
                let (t_src, t_row) = trace_row(p);
                let t_bits = plan.t_moves.null_word(t_src.get_null_word(t_row));
                for (d_null, word) in src[p.delta_rows()].iter().zip(&mut dst) {
                    *word = (t_bits | null_word_at(u64::from_le_bytes(*d_null), d_first)).to_le_bytes();
                }
            }
        }

        for (pi, col) in d_schema.payload_columns() {
            let slot = d_first + pi;
            stride_dispatch!(
                col.size(),
                copy_runs,
                delta.col_data(pi),
                w.col_mut(slot),
                delta_runs(pairs)
            );
            if col.type_code.is_german_string() {
                w.rebase_string_col(slot, Heap::Plain(delta.blob()), heap_at.into());
            }
        }

        for &ColCopy { src, slot, .. } in plan.t_moves.copies() {
            // Destructured once per column, so no pairing dispatches on the locator.
            match src {
                ColumnLocator::Payload { slot: pi, type_code, .. } if type_code.is_german_string() => {
                    // One relocation per trace row, whatever its fan-out.
                    let (dst, dst_blob, mut cache) = w.string_col_mut(slot);
                    repeat_cells::<16>(cursor, dst, pairs, |src, row| {
                        relocated_cell(
                            src.german_cell(row, pi as usize),
                            src.heap(),
                            dst_blob,
                            cache.as_deref_mut(),
                        )
                    });
                }
                ColumnLocator::Payload { slot: pi, size, .. } => {
                    let pi = pi as usize;
                    gnitz_wire::by_cell_width!(size as usize, |T| repeat_cells::<{ size_of::<T>() }>(
                        cursor,
                        w.col_mut(slot),
                        pairs,
                        |src, row| src.get_col_ptr(row, pi, size_of::<T>()).try_into().unwrap()
                    ))
                }
                ColumnLocator::Pk { byte_off, size, type_code } => {
                    let (off, signed) = (byte_off as usize, type_code.is_signed_int());
                    gnitz_wire::by_cell_width!(size as usize, |T| repeat_cells::<{ size_of::<T>() }>(
                        cursor,
                        w.col_mut(slot),
                        pairs,
                        |src, row| {
                            let mut cell = [0u8; size_of::<T>()];
                            let pk = src.get_pk_bytes(row);
                            gnitz_wire::decode_pk_cell(&pk[off..off + size_of::<T>()], signed, &mut cell);
                            cell
                        }
                    ))
                }
            }
        }
        any_ghost
    });

    if any_ghost {
        // A weight product that wrapped to zero is not a Z-set element.
        let live = runs_where(out.weight().as_chunks::<8>().0, |w| i64::from_le_bytes(*w) != 0);
        return Batch::from_ranges(&out, &live, 0);
    }
    out
}

/// The longest pairings list [`write_pairings`] measures its trace strings over
/// before writing.
const MAX_RESERVE_SCAN_PAIRS: usize = 256;

/// The heap bytes of the long cells in the trace's string columns that `pairs`
/// names, one row per pairing: an upper bound on what the relocation appends,
/// as the blob cache stores a span two cells share once.
fn trace_long_bytes(cursor: &ReadCursor, plan: &JoinPlan, pairs: &[Pairing]) -> usize {
    let mask = plan.t_moves.string_slots();
    if mask == 0 {
        return 0;
    }
    pairs
        .iter()
        .map(|p| {
            let (src, row) = cursor.row_at(p.at);
            row_long_bytes(src, mask, row)
        })
        .sum()
}

/// Each pairing's delta run, as [`copy_runs`] takes it. Runs ascend by start:
/// every walk meets the delta's groups in ascending order, and `Cross` emits
/// the whole delta each time.
#[inline(always)]
fn delta_runs(pairs: &[Pairing]) -> impl Iterator<Item = (usize, usize)> {
    pairs.iter().map(|p| (p.rs as usize, p.re as usize))
}

/// For each `(cells, step, n)` of `runs`, `n` cells of `width` bytes read
/// from `cells` `step` bytes apart (a step of 0 repeats one cell), each
/// written at byte `off` of the next `key_len`-byte key of `keys`.
#[inline(always)]
fn put_key_windows<'a, const N: usize>(
    keys: &mut [u8],
    key_len: usize,
    off: usize,
    runs: impl Iterator<Item = (&'a [u8], usize, usize)>,
    width: usize,
) {
    let w = stride::<N>(width);
    let mut keys = keys.chunks_exact_mut(key_len);
    for (cells, step, n) in runs {
        for (j, key) in keys.by_ref().take(n).enumerate() {
            key[off..off + w].copy_from_slice(&cells[j * step..j * step + w]);
        }
    }
}

/// Each pairing's trace cell, produced by `cell` from its walked row, written
/// once per row of its delta run.
#[inline(always)]
fn repeat_cells<const N: usize>(
    cursor: &ReadCursor,
    dst: &mut [u8],
    pairs: &[Pairing],
    mut cell: impl FnMut(&Run, usize) -> [u8; N],
) {
    let mut cells = dst.as_chunks_mut::<N>().0.iter_mut();
    for p in pairs {
        let (src, row) = cursor.row_at(p.at);
        let cell = cell(src, row);
        cells.by_ref().take(p.len()).for_each(|c| *c = cell);
    }
}

// ---------------------------------------------------------------------------
// The equi walk
// ---------------------------------------------------------------------------

/// Equal-key merge walk over a fresh cursor. Both pointers galloping-skip to catch
/// up, so the cost is bounded by the smaller side's matches whichever side that is.
///
/// The cursor's rows may be keyed wider than the delta: a delta group then
/// matches the rows its PK prefixes, which sit together because the cursor
/// orders by the whole PK.
fn equi_merge_walk(delta: &Batch, cursor: &mut ReadCursor, mut emit: impl FnMut(usize, usize, &ReadCursor)) {
    let (n, k) = (delta.len(), delta.schema().pk_stride());
    let mut i = 0;
    while i < n {
        let dk = delta.get_pk_bytes(i);
        if cursor.seek_pk_group_ascending(dk) {
            let j = pk_group_end(delta, i, k); // delta group
            cursor.for_each_pk_group_row(dk, |c| emit(i, j, c));
            i = j;
        } else if cursor.is_valid() {
            i = delta.seek(&cursor.current_pk_bytes()[..k], Some(i)); // skip delta
        } else {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// The range walk
// ---------------------------------------------------------------------------

/// Trace-driven equality-group merge walk. One cut spans a whole delta equality
/// group, so a group costs one seek and one pass over the covered trace span,
/// with a monotone delta pointer naming the matching run per trace row. Every
/// trace row of a span matches the delta row that cut it, so no emitted run is
/// empty.
fn range_merge_walk(
    delta: &Batch,
    cursor: &mut ReadCursor,
    probe: RangeProbe,
    mut emit: impl FnMut(usize, usize, &ReadCursor),
) {
    let (eq_size, above) = (probe.eq_size, probe.above);
    let (stride, last) = (delta.schema().pk_stride(), probe.last_before_split());
    let mut lo = 0;
    while lo < delta.len() {
        let hi = pk_group_end(delta, lo, eq_size);
        // The group shares one bound, so the row whose other bound spans
        // widest cuts for all of them.
        let cut_row = if above { lo } else { hi - 1 };
        let Some((start, end)) = probe.cut_points(delta.get_pk_bytes(cut_row)) else {
            // Provably empty, so the cursor never moved here and says nothing
            // about the trace past this group.
            lo = hi;
            continue;
        };
        // Backward-capable: under `Gt`/`Ge` the start cut can lie below where
        // the last sweep left the cursor.
        cursor.advance_to(start.pk_bytes());
        let end = end.as_ref().map(PkBuf::pk_bytes);
        let mut ptr = lo; // monotone across the sweep
        cursor.for_each_row_while(
            // Every key in [start, end) carries the group's equality prefix, so
            // the end bound alone delimits the group.
            |pk| end.is_none_or(|e| compare_pk_ordering(pk, e).is_lt()),
            |c| {
                let s = &c.current_pk_bytes()[eq_size..];
                // Matching rows are a group prefix under `Gt`/`Ge` and a suffix
                // under `Lt`/`Le`, so the pointer walks the leading rows either way.
                while ptr < hi && compare_pk_ordering(&delta.get_pk_bytes(ptr)[eq_size..], s) <= last {
                    ptr += 1;
                }
                let (rs, re) = if above { (lo, ptr) } else { (ptr, hi) };
                emit(rs, re, c);
            },
        );
        // Group spans ascend, so once the trace runs out inside one span no later
        // group can match either.
        if !cursor.is_valid() {
            return;
        }
        // The cursor sits past the span, so its equality prefix names the next
        // group the trace can match: the delta groups below it are skipped by
        // seeking to that group's zero-padded floor.
        let trace_group = &cursor.current_pk_bytes()[..eq_size];
        lo = hi;
        if hi < delta.len() && compare_pk_ordering(&delta.get_pk_bytes(hi)[..eq_size], trace_group).is_lt() {
            lo = delta.seek(group_floor(trace_group, stride).pk_bytes(), Some(hi));
        }
    }
}

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/join.rs"]
mod bench;
