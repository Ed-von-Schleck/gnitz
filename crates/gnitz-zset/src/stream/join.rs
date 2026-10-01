//! The delta-trace inner join, equi, non-equi and keyless.
//!
//! One opcode over a compiler-baked [`JoinProbe`]: `Equi` co-groups the delta
//! against the trace on equal key, `Range` walks an ordered half-open span per
//! delta equality group, `Cross` walks the whole trace once against the whole
//! delta. All three name the same emission — the product of a contiguous
//! delta run with the cursor's current trace row — and all write
//! `[key, left payload…, right payload…]` over the SQL sides, keyed by the delta
//! PK, or by `[left PK…, right PK…]` under `Cross`.

use crate::schema::ColumnTable;
use std::cmp::Ordering;
use std::ops::Range;

use crate::repr::{
    copy_string_cells, pk_group_end, pk_prefix_group_end, relocate_german_string_vec, runs_where, should_relocate_blob,
    width_dispatch, Batch, BlobCache, ReadCursor,
};
use crate::schema::key::{compare_pk_ordering, key_range_between_cuts, KeyCut, PkBuf};
use crate::schema::{DerivedSchema, SchemaDescriptor, SchemaFacts, MAX_PK_BYTES};

use gnitz_expr::RowSource;
use gnitz_wire::{null_word_at, JoinKind, RangeRel, TypeCode};

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// One join instruction's compiler-baked artifact: how it probes its trace, and
/// the schema it writes under.
pub struct JoinPlan {
    pub probe: JoinProbe,
    pub out_schema: SchemaDescriptor,
}

/// How one join instruction probes its trace, and where each input's columns
/// land in the output row — both baked by the compiler, which holds the two
/// input schemas the kernel never sees.
#[derive(Clone, Copy)]
pub struct JoinProbe {
    walk: Walk,
    /// The output payload slots the delta's columns fill, and the trace's.
    d_slots: Slots,
    t_slots: Slots,
}

impl JoinProbe {
    /// Whether the walk reads the trace only at the delta's own PKs.
    pub fn probes_delta_keys(&self) -> bool {
        matches!(self.walk, Walk::Equi)
    }
}

/// A half-open span of the output row: the payload slots one input fills, or
/// the bytes its key occupies in the pair key.
#[derive(Clone, Copy)]
struct Slots {
    start: u16,
    end: u16,
}

impl Slots {
    fn new(start: usize, end: usize) -> Slots {
        debug_assert!(start <= end && end <= u16::MAX as usize);
        Slots { start: start as u16, end: end as u16 }
    }

    #[inline]
    fn range(self) -> Range<usize> {
        self.start as usize..self.end as usize
    }
}

/// Which trace walk a probe drives.
#[derive(Clone, Copy)]
enum Walk {
    /// Equal key: the trace group at each delta group's own PK.
    Equi,
    /// An ordered span within an equality group.
    Range(RangeProbe),
    /// Every trace row, for every delta row: the keyless join. No key decides a
    /// match, so both key regions ride into the output as the pair key, at the
    /// spans this arm carries.
    Cross { pk_len: u16, d_key: Slots, t_key: Slots },
}

impl JoinPlan {
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
        let out_schema = b.finish().map_err(|e| format!("join: merged schema {e}"))?;

        // Both regions run the left SQL side first, so one split serves either.
        let halves = |l: usize, total: usize| match delta_is_right {
            true => (Slots::new(l, total), Slots::new(0, l)),
            false => (Slots::new(0, l), Slots::new(l, total)),
        };
        let (d_slots, t_slots) = halves(left.num_payload_cols(), out_schema.num_payload_cols());
        let walk = match kind {
            JoinKind::Equi => {
                same_pk_types(delta, trace)?;
                Walk::Equi
            }
            JoinKind::Range { n_eq, rel } => {
                same_pk_types(delta, trace)?;
                Walk::Range(RangeProbe::new(trace, n_eq, rel, delta_is_right)?)
            }
            JoinKind::Cross => {
                let (d_key, t_key) = halves(left.pk_stride(), out_schema.pk_stride());
                Walk::Cross {
                    pk_len: out_schema.pk_stride() as u16,
                    d_key,
                    t_key,
                }
            }
        };
        Ok(JoinPlan {
            probe: JoinProbe { walk, d_slots, t_slots },
            out_schema,
        })
    }
}

/// A keyed walk reads one side's PK region as the other's, so the two key
/// layouts must be identical down to the OPK encoding each column type implies.
fn same_pk_types(delta: &SchemaDescriptor, trace: &SchemaDescriptor) -> Result<(), String> {
    fn types(s: &SchemaDescriptor) -> impl Iterator<Item = TypeCode> + '_ {
        s.pk_columns().map(|(_, c)| c.type_code)
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
    fn new(trace: &SchemaDescriptor, n_eq: u8, rel: RangeRel, delta_is_right: bool) -> Result<RangeProbe, String> {
        // The reindexed key is `[eq slots…, range slot]`.
        if n_eq as usize + 1 != trace.pk_cols().len() {
            return Err("range join: n_eq does not match trace key arity".to_string());
        }
        // In PK order, so the range slot always keeps a span of its own.
        let eq_size = trace
            .pk_columns()
            .take(n_eq as usize)
            .map(|(_, c)| c.size() as usize)
            .sum();
        // `rel` relates left to right; the probe relates trace to delta.
        let rel = match delta_is_right {
            true => rel,
            false => rel.converse(),
        };
        Ok(RangeProbe {
            eq_size,
            above: rel.bounds_below(),
            cuts_below: rel.bounds_below() == rel.admits_equal(),
        })
    }

    /// The trace key range this probe covers for a delta row whose PK region is
    /// `pk` (`equality prefix ‖ range slot`). `None` when it is provably empty.
    fn cut_points(&self, pk: &[u8]) -> Option<(PkBuf, Option<PkBuf>)> {
        let group = &pk[..self.eq_size];
        let slot = KeyCut::new(pk, !self.cuts_below);
        let (start, end) = match self.above {
            true => (slot, KeyCut::above(group)),
            false => (KeyCut::min_of(group), slot),
        };
        key_range_between_cuts(start, end, pk.len())
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

/// One emission of a walk: the delta run `[rs, re)` against one trace row, named
/// by its cursor position.
#[derive(Clone, Copy)]
struct Pairing {
    rs: u32,
    re: u32,
    src: u32,
    row: u32,
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
/// payload…]` under the `out_schema` [`JoinPlan::from_wire`] derived above.
///
/// The walk only records which delta run meets which trace row; the output is
/// then written one region at a time over that list. Emission is trace-major
/// under every probe: each trace row is walked once and producted against a
/// contiguous delta run. So the output is unfolded and not (PK, payload)-sorted
/// — under `Cross` not even PK-sorted, since each trace row re-emits the whole
/// delta; it carries no consolidated claim, and downstream re-sorts and
/// consolidates.
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    out_schema: &SchemaDescriptor,
    probe: JoinProbe,
) -> Batch {
    // The VM folds this register before any reader.
    debug_assert!(delta.is_consolidated());
    let n = delta.count;
    if n == 0 {
        return Batch::empty_with_schema(out_schema);
    }
    // Grown, not pre-sized: a walk can match nothing.
    let mut pairs: Vec<Pairing> = Vec::new();
    let mut rows = 0usize;
    let mut emit = |rs: usize, re: usize, c: &ReadCursor| {
        if rs == re {
            return;
        }
        let (src, row) = c.current_position();
        pairs.push(Pairing {
            rs: rs as u32,
            re: re as u32,
            src: src as u32,
            row: row as u32,
            w_trace: c.current_weight,
        });
        rows += re - rs;
    };
    match probe.walk {
        Walk::Equi => equi_merge_walk(delta, cursor, emit),
        Walk::Range(range) => range_merge_walk(delta, cursor, range, emit),
        Walk::Cross { .. } => {
            cursor.rewind();
            cursor.for_each_row_while(|_| true, |c| emit(0, n, c));
        }
    }
    write_pairings(delta, cursor, out_schema, probe, &pairs, rows)
}

/// The `rows` output rows `pairs` names, in list order, one region at a time.
fn write_pairings(
    delta: &Batch,
    cursor: &ReadCursor,
    out_schema: &SchemaDescriptor,
    probe: JoinProbe,
    pairs: &[Pairing],
    rows: usize,
) -> Batch {
    if rows == 0 {
        return Batch::empty_with_schema(out_schema);
    }
    let d_schema = delta.schema();
    let (d_first, t_first) = (probe.d_slots.start as usize, probe.t_slots.start as usize);
    let trace_row = |p: &Pairing| (cursor.source_at(p.src as usize), p.row as usize);
    let mut out = Batch::with_capacity(out_schema, rows);
    out.grow_rows(rows);

    match probe.walk {
        // No key decides a match, so the pair `[left PK…, right PK…]` is minted
        // per row.
        Walk::Cross { pk_len, d_key, t_key } => {
            let (d_key, t_key) = (d_key.range(), t_key.range());
            let mut keys = out.pk_data_mut().chunks_exact_mut(pk_len as usize);
            for p in pairs {
                let (t_src, t_row) = trace_row(p);
                let t_pk = t_src.get_pk_bytes(t_row);
                for i in p.delta_rows() {
                    let key = keys.next().expect("one key per output row");
                    key[d_key.clone()].copy_from_slice(delta.get_pk_bytes(i));
                    key[t_key.clone()].copy_from_slice(t_pk);
                }
            }
        }
        // A keyed probe's output key is the delta's PK region verbatim.
        _ => width_dispatch!(
            d_schema.pk_stride(),
            copy_runs,
            delta.pk_data(),
            out.pk_data_mut(),
            pairs
        ),
    }

    let mut any_ghost = false;
    {
        let src = delta.weight_data().as_chunks::<8>().0;
        let mut dst = out.weight_data_mut().as_chunks_mut::<8>().0.iter_mut();
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
        let src = delta.null_bmp_data().as_chunks::<8>().0;
        let mut dst = out.null_bmp_data_mut().as_chunks_mut::<8>().0.iter_mut();
        for p in pairs {
            let (t_src, t_row) = trace_row(p);
            let t_bits = null_word_at(t_src.get_null_word(t_row), t_first);
            for (d_null, word) in src[p.delta_rows()].iter().zip(&mut dst) {
                *word = (t_bits | null_word_at(u64::from_le_bytes(*d_null), d_first)).to_le_bytes();
            }
        }
    }

    // The delta's heap rides whole unless relocating the emitted cells is
    // cheaper; the rows no pairing names are what it then holds dead.
    let strings = d_schema.string_payload_slots();
    let heap_at = match strings != 0 && !should_relocate_blob(delta.blob().len(), delta.count, rows) {
        true => {
            let mut emitted = vec![false; delta.count];
            pairs.iter().for_each(|p| emitted[p.delta_rows()].fill(true));
            let kept = runs_where(delta.count, |row| emitted[row]);
            out.carry_heap(&delta.as_mem_batch(), strings, strings, &kept)
        }
        false => None,
    };
    let mut cache = BlobCache::new(rows);

    for (pi, col) in d_schema.payload_columns() {
        let src = delta.col_data(pi);
        let (dst, _, dst_blob) = out.col_null_and_blob_mut(d_first + pi);
        if !col.type_code.is_german_string() {
            width_dispatch!(col.size() as usize, copy_runs, src, dst, pairs);
            continue;
        }
        let mut at = 0;
        for p in pairs {
            let (run, end) = (p.delta_rows(), at + p.len());
            copy_string_cells(
                &mut dst[at * 16..end * 16],
                &src[run.start * 16..run.end * 16],
                delta.blob(),
                dst_blob,
                heap_at,
                &mut cache,
            );
            at = end;
        }
    }

    for (pi, col) in cursor.schema.payload_columns() {
        let (dst, _, dst_blob) = out.col_null_and_blob_mut(t_first + pi);
        if !col.type_code.is_german_string() {
            width_dispatch!(col.size() as usize, repeat_cells, cursor, pi, dst, pairs);
            continue;
        }
        // One relocation per trace row, whatever its fan-out.
        let mut cells = dst.as_chunks_mut::<16>().0.iter_mut();
        for p in pairs {
            let (t_src, t_row) = trace_row(p);
            let cell = relocate_german_string_vec(
                t_src.get_col_ptr(t_row, pi, 16),
                t_src.blob(),
                dst_blob,
                Some(&mut cache),
            );
            cells.by_ref().take(p.len()).for_each(|c| *c = cell);
        }
    }

    if any_ghost {
        // A weight product that wrapped to zero is not a Z-set element.
        let live = runs_where(rows, |row| out.get_weight(row) != 0);
        return Batch::from_ranges(&out, &live, 0);
    }
    out
}

/// Each pairing's delta run of `width`-byte cells, copied out of `src` onto
/// `dst` back to back.
#[inline(always)]
fn copy_runs<const N: usize>(src: &[u8], dst: &mut [u8], pairs: &[Pairing], width: usize) {
    // `N = 0` is the runtime width a compound PK stride takes.
    let w = if N == 0 { width } else { N };
    let mut at = 0usize;
    for p in pairs {
        let (rs, n) = (p.rs as usize, p.len());
        if n == 1 && N != 0 {
            // A constant width keeps the one-row run a load and a store.
            let cell: [u8; N] = src[rs * N..rs * N + N].try_into().unwrap();
            dst[at * N..at * N + N].copy_from_slice(&cell);
        } else {
            dst[at * w..(at + n) * w].copy_from_slice(&src[rs * w..(rs + n) * w]);
        }
        at += n;
    }
}

/// Each pairing's trace cell of payload column `pi`, written once per row of its
/// delta run.
#[inline(always)]
fn repeat_cells<const N: usize>(cursor: &ReadCursor, pi: usize, dst: &mut [u8], pairs: &[Pairing], width: usize) {
    assert!(N != 0, "a payload column is 1, 2, 4, 8 or 16 bytes, not {width}");
    let mut cells = dst.as_chunks_mut::<N>().0.iter_mut();
    for p in pairs {
        let src = cursor.source_at(p.src as usize);
        let cell: [u8; N] = src.get_col_ptr(p.row as usize, pi, N).try_into().unwrap();
        cells.by_ref().take(p.len()).for_each(|c| *c = cell);
    }
}

// ---------------------------------------------------------------------------
// The equi walk
// ---------------------------------------------------------------------------

/// Equal-key merge walk over a fresh cursor. Both pointers galloping-skip to catch
/// up, so the cost is bounded by the smaller side's matches whichever side that is.
fn equi_merge_walk(delta: &Batch, m: &mut ReadCursor, mut emit: impl FnMut(usize, usize, &ReadCursor)) {
    let n = delta.count;
    let mut i = 0;
    while i < n {
        let dk = delta.get_pk_bytes(i);
        if m.seek_pk_group_ascending(dk) {
            let j = pk_group_end(delta, i); // delta group
            m.for_each_pk_group_row(dk, |c| emit(i, j, c));
            i = j;
        } else if m.valid {
            i = delta.advance_to(m.current_pk_bytes(), i); // skip delta
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
/// with a monotone delta pointer naming the matching run per trace row.
fn range_merge_walk(
    delta: &Batch,
    cursor: &mut ReadCursor,
    probe: RangeProbe,
    mut emit: impl FnMut(usize, usize, &ReadCursor),
) {
    let (eq_size, above) = (probe.eq_size, probe.above);
    let (stride, last) = (delta.schema().pk_stride(), probe.last_before_split());
    // A group's lower bound: its equality prefix over an all-zero range slot,
    // so only the prefix is ever rewritten.
    let mut group_lb = [0u8; MAX_PK_BYTES];
    let mut lo = 0;
    while lo < delta.count {
        let hi = pk_prefix_group_end(delta, lo, eq_size);
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
        if !cursor.valid {
            return;
        }
        // The cursor sits past the span, so its equality prefix names the next
        // group the trace can match. `advance_to` is an absolute lower bound,
        // not a forward one, so `hi` floors it.
        let trace_group = &cursor.current_pk_bytes()[..eq_size];
        lo = hi;
        if hi < delta.count && compare_pk_ordering(&delta.get_pk_bytes(hi)[..eq_size], trace_group).is_lt() {
            group_lb[..eq_size].copy_from_slice(trace_group);
            lo = delta.advance_to(&group_lb[..stride], hi).max(hi);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;
