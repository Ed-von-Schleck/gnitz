//! The delta-trace inner join, equi, non-equi and keyless.
//!
//! One opcode over a compiler-baked [`JoinProbe`]: `Equi` co-groups the delta
//! against the trace on equal key, `Range` walks an ordered half-open span per
//! delta equality group, `Cross` walks the whole trace once against the whole
//! delta. All three drive the same emission — the product of a contiguous
//! delta run with the cursor's current trace row — and all write
//! `[key, left payload…, right payload…]` over the SQL sides, keyed by the delta
//! PK, or by `[left PK…, right PK…]` under `Cross`.

use std::cmp::Ordering;
use std::ops::Range;

use crate::schema::key::{compare_pk_ordering, key_range_between_cuts, KeyCut, PkBuf};
use crate::schema::{DerivedSchema, OpBuildErr, SchemaDescriptor, MAX_PK_BYTES};
use crate::storage::{pk_group_end, pk_prefix_group_end, Batch, BlobCacheGuard, ReadCursor};

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
    ) -> Result<JoinPlan, OpBuildErr> {
        let (left, right) = match delta_is_right {
            true => (trace, delta),
            false => (delta, trace),
        };
        let over = |e| OpBuildErr::shape(format!("join: merged schema {e}"));
        let mut b = DerivedSchema::new();
        b.push_pk_of(left).map_err(over)?;
        if kind == JoinKind::Cross {
            // The keyless join matches on neither key, so it mints the pair.
            b.push_pk_of(right).map_err(over)?;
        }
        for (_, c) in left.payload_columns().chain(right.payload_columns()) {
            b.push(*c).map_err(over)?;
        }
        let out_schema = b.finish();

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
fn same_pk_types(delta: &SchemaDescriptor, trace: &SchemaDescriptor) -> Result<(), OpBuildErr> {
    fn types(s: &SchemaDescriptor) -> impl Iterator<Item = TypeCode> + '_ {
        s.pk_columns().map(|(_, c)| c.type_code)
    }
    if types(delta).eq(types(trace)) {
        return Ok(());
    }
    Err(OpBuildErr::shape(
        "join: delta and trace PK column types differ (both sides must reindex at the pair's common type)",
    ))
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
    fn new(trace: &SchemaDescriptor, n_eq: u8, rel: RangeRel, delta_is_right: bool) -> Result<RangeProbe, OpBuildErr> {
        // The reindexed key is `[eq slots…, range slot]`.
        if n_eq as usize + 1 != trace.pk_indices().len() {
            return Err(OpBuildErr::shape("range join: n_eq does not match trace key arity"));
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

/// Join delta rows against the trace, writing `[key, left payload…, right
/// payload…]` under the `out_schema` [`JoinPlan::from_wire`] derived above.
///
/// Emission is trace-major under every probe: each trace row is walked once and
/// producted against a contiguous, random-access delta run. So the output is
/// unfolded and not (PK, payload)-sorted — under `Cross` not even PK-sorted,
/// since each trace row re-emits the whole delta; it carries no layout claim,
/// and downstream re-sorts and consolidates.
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    out_schema: &SchemaDescriptor,
    probe: JoinProbe,
) -> Batch {
    // Folded by the VM at this register's first reader (`OpFacts::consolidates_in`).
    debug_assert!(delta.is_consolidated());
    let n = delta.count;
    if n == 0 {
        return Batch::empty_with_schema(out_schema);
    }
    let delta_mb = delta.as_mem_batch();
    // Widened once: the emit reads both ends per output row.
    let (d_slots, t_slots) = (probe.d_slots.range(), probe.t_slots.range());

    // The keyless probe reads no key and emits exactly `n × |trace|` rows;
    // for a keyed probe `n` is only a hint.
    let (rows, mut pair) = match probe.walk {
        Walk::Cross { pk_len, d_key, t_key } => {
            cursor.rewind();
            let key = PairKey {
                buf: [0u8; MAX_PK_BYTES],
                len: pk_len as usize,
                delta: d_key.range(),
                trace: t_key.range(),
            };
            (n.saturating_mul(cursor.estimated_length()), Some(key))
        }
        _ => (n, None),
    };

    let mut output = Batch::with_capacity(out_schema, rows);
    let mut cache = BlobCacheGuard::acquire(out_schema, rows);

    let mut emit = |rs: usize, re: usize, c: &ReadCursor| {
        let w_trace = c.current_weight;
        let (t_src, t_row) = c.current_row_source();
        let t_null = t_src.get_null_word(t_row);
        // Every row this call writes shares one trace row, so its null bits
        // rebase once rather than per delta row.
        let t_bits = null_word_at(t_null, t_slots.start);
        if let Some(k) = pair.as_mut() {
            k.buf[k.trace.clone()].copy_from_slice(c.current_pk_bytes());
        }
        for i in rs..re {
            let w_out = delta_mb.get_weight(i).wrapping_mul(w_trace);
            if w_out == 0 {
                continue;
            }
            let d_pk_bytes = delta_mb.get_pk_bytes(i);
            let pk = match pair.as_mut() {
                Some(k) => {
                    k.buf[k.delta.clone()].copy_from_slice(d_pk_bytes);
                    &k.buf[..k.len]
                }
                None => d_pk_bytes,
            };
            output.begin_row(pk, w_out);

            let d_null = delta_mb.get_null_word(i);
            output.append_payload_cols(d_slots.clone(), out_schema, &delta_mb, i, d_null, cache.get_mut());
            output.append_payload_cols(t_slots.clone(), out_schema, t_src, t_row, t_null, cache.get_mut());
            // Each half's null bits rebase onto the slot its columns landed at.
            output.commit_row(t_bits | null_word_at(d_null, d_slots.start));
        }
    };

    match probe.walk {
        Walk::Equi => equi_merge_walk(delta, cursor, emit),
        Walk::Range(range) => range_merge_walk(delta, cursor, range, emit),
        Walk::Cross { .. } => cursor.for_each_row_while(|_| true, |c| emit(0, n, c)),
    }

    output
}

/// `Cross`'s output key under construction: the pair `[left PK…, right PK…]`,
/// assembled per row because neither source key is the output's. A keyed probe
/// has none — its key is the delta's PK region verbatim.
struct PairKey {
    buf: [u8; MAX_PK_BYTES],
    len: usize,
    delta: Range<usize>,
    trace: Range<usize>,
}

// ---------------------------------------------------------------------------
// The equi walk
// ---------------------------------------------------------------------------

/// Equal-key merge walk. Both pointers galloping-skip to catch up, so the cost
/// is bounded by the smaller side's matches whichever side that is. A fresh
/// cursor sits on the trace's first live group, which can lie above `delta[0]`,
/// so the opening seek is the backward-capable one.
fn equi_merge_walk(delta: &Batch, m: &mut ReadCursor, mut emit: impl FnMut(usize, usize, &ReadCursor)) {
    let n = delta.count;
    if n == 0 {
        return;
    }
    m.advance_to(delta.get_pk_bytes(0));
    let mut i = 0;
    while i < n && m.valid {
        let dk = delta.get_pk_bytes(i);
        match compare_pk_ordering(dk, m.current_pk_bytes()) {
            Ordering::Less => i = delta.advance_to(m.current_pk_bytes(), i), // skip delta
            // The comparison above IS the forward gallop's precondition.
            Ordering::Greater => m.advance_to_forward(dk), // skip trace
            Ordering::Equal => {
                let j = pk_group_end(delta, i); // delta group
                m.for_each_pk_group_row(dk, |c| emit(i, j, c));
                i = j;
            }
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
    let (stride, last) = (delta.pk_stride() as usize, probe.last_before_split());
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
