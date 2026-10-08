//! The circuit primitives the join shell in [`super::join`] composes.

use super::super::{JoinClass, JoinType};
use super::join::JoinSide;
use crate::error::GnitzSqlError;
use crate::rules::reject_arity;

use gnitz_core::Schema;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, JoinKind};
use gnitz_wire::{Circuit, ColumnDef, NodeId, NullKeys, RangeRel, ReindexRole, ReindexSlot, TypeCode};

/// This side's reindex key over `cols`, each slot typed at its `slot_tcs` type.
fn side_reindex_key(cols: &[usize], slot_tcs: &[TypeCode]) -> Vec<ReindexSlot> {
    cols.iter().zip(slot_tcs).map(|(&c, &t)| (c as u32, t)).collect()
}

/// `(all, reindex)`: `input` re-keyed onto its join key, `reindex` without its
/// NULL-keyed rows and `all` with them where `emits_unmatched`. The side's delta
/// is scattered by the key's leading `routed` slots.
fn keyed_side(
    cb: &mut Circuit,
    side: &JoinSide,
    input: NodeId,
    tcs: &[TypeCode],
    routed: usize,
    emits_unmatched: bool,
) -> Result<(NodeId, NodeId), GnitzSqlError> {
    let key = side_reindex_key(&side.key, tcs);
    let role = side.scatter_key(&key[..routed])?;
    let reindex = cb.map_reindex(input, &key, &side.keep, role.clone(), NullKeys::Drop);
    // With no nullable key column the two re-keys keep the same rows.
    let nullable = side.key.iter().any(|&c| side.frame.schema.columns[c].is_nullable);
    let all = match emits_unmatched && nullable {
        true => cb.map_reindex(input, &key, &side.keep, role, NullKeys::Keep),
        false => reindex,
    };
    Ok((all, reindex))
}

/// An equi join's shared nodes: each side's [`keyed_side`] `all`, the re-key
/// each side's delta joins through — and whose integral the other side's delta
/// probes — and the symmetric 2-term join over `[_join_pk, kept-A, kept-B]`.
pub(super) struct EquiPrologue {
    pub(super) all: [NodeId; 2],
    reindex: [NodeId; 2],
    pub(super) inner: NodeId,
}

impl EquiPrologue {
    /// Side `p`'s keyed rows that some row of the other side matches, each at its
    /// own weight `w_P · [S > 0]`, laid out like `all[p]`: `p`'s reindex joined
    /// against the other side's key set.
    pub(super) fn matched(&self, cb: &mut Circuit, p: usize) -> NodeId {
        let o = 1 - p;
        let keys = cb.map(self.reindex[o], &[]);
        let mut deltas = self.reindex;
        deltas[o] = cb.distinct(keys);
        cb.join(deltas, deltas, JoinKind::Equi)
    }
}

pub(super) fn equi_prologue(
    cb: &mut Circuit,
    class: &JoinClass,
    sides: &[JoinSide; 2],
    inputs: [NodeId; 2],
    kind: JoinType,
    b_unique: bool,
) -> Result<EquiPrologue, GnitzSqlError> {
    let tcs = class.key_tcs();
    let side = |cb: &mut Circuit, i: usize| {
        keyed_side(cb, &sides[i], inputs[i], &tcs, tcs.len(), kind.emits_unmatched(i == 0))
    };
    let (all_b, reindex_b) = side(cb, 1)?;
    let (all_a, reindex_a) = side(cb, 0)?;
    // A decorrelated join asks only whether a key exists: B as a set weighs every
    // match 1, so the joined A rows are the matched set at their own weight.
    let b_delta = match kind.is_decorrelated() && !b_unique {
        true => cb.distinct(reindex_b),
        false => reindex_b,
    };
    let reindex = [reindex_a, b_delta];
    let inner = cb.join(reindex, reindex, JoinKind::Equi);
    Ok(EquiPrologue { all: [all_a, all_b], reindex, inner })
}

/// The output PK columns a re-key onto source PKs mints: one per listed
/// `(schema, pk column)`, at the column's own key slot type, non-nullable, and
/// hidden. The two producers below differ only in `name`.
fn rekey_pk_coldefs<'a>(
    cols: impl IntoIterator<Item = (&'a Schema, u32)>,
    name: impl Fn(usize, &ColumnDef) -> String,
) -> Vec<ColumnDef> {
    cols.into_iter()
        .enumerate()
        .map(|(slot, (schema, c))| {
            let src = &schema.columns[c as usize];
            ColumnDef::new(name(slot, src), src.ty.tc.reindex_output_type(), false).hidden()
        })
        .collect()
}

/// The leading output PK columns of a range/band or cross join: A's then B's
/// source PK, `_pair_pk_{slot}`-numbered across both sides.
pub(super) fn pair_pk_coldefs(left_schema: &Schema, right_schema: &Schema) -> Vec<ColumnDef> {
    rekey_pk_coldefs(
        left_schema
            .pk_cols
            .iter()
            .map(|&c| (left_schema, c))
            .chain(right_schema.pk_cols.iter().map(|&c| (right_schema, c))),
        |slot, _| format!("_pair_pk_{slot}"),
    )
}

/// The output PK columns of a range-correlated EXISTS/IN view: the outer source
/// PK under its own names (it also rides the payload verbatim).
pub(crate) fn src_pk_coldefs(schema: &Schema) -> Vec<ColumnDef> {
    rekey_pk_coldefs(schema.pk_cols.iter().map(|&c| (schema, c)), |_, src| src.name.clone())
}

/// Re-key `node`, laid out `[lead…, each of `sides`' kept payload in order]`, onto
/// those sides' pinned source PKs — each at the front of its kept payload —
/// keeping the payloads and dropping the lead region.
fn rekey_pinned(cb: &mut Circuit, node: NodeId, lead: usize, sides: &[JoinSide]) -> NodeId {
    let (mut key, mut at) = (Vec::new(), lead);
    for s in sides {
        let pinned = &s.keep[..s.pa()];
        key.extend(pinned.iter().enumerate().map(|(j, &c)| {
            let tc = s.frame.schema.columns[c as usize].ty.tc;
            ((at + j) as u32, tc.reindex_output_type())
        }));
        at += s.n();
    }
    let keep: Vec<u32> = (lead as u32..at as u32).collect();
    cb.map_reindex(node, &key, &keep, ReindexRole::Auxiliary, NullKeys::Keep)
}

/// Re-key `node`, which carries `side`'s source rows, onto that source's PK.
/// `route`: the leading key slots this re-key states `side`'s delta is scattered
/// by (see `ReindexRole::ScatterKey`), `None` where it states no route.
pub(super) fn rekey_on_source_pk(
    cb: &mut Circuit,
    node: NodeId,
    side: &JoinSide,
    route: Option<usize>,
) -> Result<NodeId, GnitzSqlError> {
    let schema = &side.frame.schema;
    let key: Vec<ReindexSlot> = schema
        .pk_cols
        .iter()
        .map(|&c| (c, schema.columns[c as usize].ty.tc.reindex_output_type()))
        .collect();
    let role = match route {
        Some(n) => side.scatter_key(&key[..n])?,
        None => ReindexRole::Auxiliary,
    };
    Ok(cb.map_reindex(node, &key, &side.keep, role, NullKeys::Keep))
}

/// A range/band outer join's null-fill branch: `ν_P`, the other side's half
/// all-NULL, keyed on the pair-PK like the inner terms.
pub(super) fn emit_range_null_fill_tail(
    cb: &mut Circuit,
    sides: &[JoinSide; 2],
    nf_keyed: NodeId,
    preserved_is_left: bool,
) -> NodeId {
    let (p, o) = match preserved_is_left {
        true => (&sides[0], &sides[1]),
        false => (&sides[1], &sides[0]),
    };
    // [P.pk × p.pa(), A, B], one of the two payload halves all-NULL.
    let nullfill = cb.null_extend(nf_keyed, &o.kept_type_codes(), !preserved_is_left);
    rekey_pinned(cb, nullfill, p.pa(), sides)
}

/// What [`range_prologue`] hands back. `op` is the ON clause's left-to-right range
/// relation, which both terms carry verbatim.
pub(super) struct RangePrologue<'a> {
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
    /// A's range-keyed delta; `None` for an EXISTS/IN over a pure range, which
    /// reads only the threshold.
    reindex_a: Option<NodeId>,
    reindex_b: NodeId,
    /// A's owned slice keyed on its source PK, NULL range keys included — built for
    /// a pure range with a ν over A.
    owned_a: Option<NodeId>,
    /// The slice of A's range-keyed delta whose integral B's delta probes.
    int_a: NodeId,
    /// The key arity: the eq prefix plus the one range slot — the width of the
    /// key region every term leads with.
    k: usize,
    /// The range slot's key type.
    range_tc: TypeCode,
    op: RangeRel,
}

impl RangePrologue<'_> {
    /// The two range terms over `[key region, A, B]`. A pure range — a key of
    /// the range slot alone — integrates B's worker-filtered slice: an unfiltered
    /// integral against the broadcast A delta would emit each pair once per worker.
    pub(super) fn merged(&self, cb: &mut Circuit) -> NodeId {
        let reindex_a = self.reindex_a.expect("the pair terms read A's range-keyed delta");
        let int_b = match self.k {
            1 => cb.worker_filter(self.reindex_b),
            _ => self.reindex_b,
        };
        let kind = JoinKind::Range { rel: self.op };
        cb.join([reindex_a, self.reindex_b], [self.int_a, int_b], kind)
    }

    /// `(A_owned, matched)` for a pure range: A's owned slice and the rows of it
    /// matching the one-row threshold `m = MAX/MIN(b.range)`, both keyed on
    /// `[a.pk…, A]`, so `A_owned − matched` is the unmatched set.
    pub(super) fn threshold(&self, cb: &mut Circuit) -> (NodeId, NodeId) {
        let want_max = !self.op.bounds_below();
        let agg_func = if want_max { WireAggFunc::Max } else { WireAggFunc::Min };

        // B's range keys alone: rows sharing one consolidate before the MIN/MAX reads it.
        let keys = cb.map(self.reindex_b, &[]);
        // Local over the broadcast B, so every worker holds the same extremum. No
        // ground row: a `m = NULL` seed would break `A − 0 = A` over an empty B.
        // The COUNT is the cardinality gate every reduce carries; the reindex
        // below keeps no payload, so it goes no further.
        let specs = [
            AggDescriptor { agg_op: agg_func, col_idx: 0 },
            AggDescriptor::COUNT_STAR,
        ];
        // [_group_pk:U128, m:Tc, count]
        let red = cb.reduce_multi_local(keys, &[], &specs);
        // `m` is never NULL: B's NULL range keys never reached the reduce.
        // `m` is a MIN/MAX of B's range slot.
        let reindex_m = cb.map_reindex(red, &[(1, self.range_tc)], &[], ReindexRole::Auxiliary, NullKeys::Keep);

        // `m` carries no payload, so both terms are `[_join_pk × k, A]`.
        let deltas = [self.int_a, reindex_m];
        let matched_raw = cb.join(deltas, deltas, JoinKind::Range { rel: self.op });
        let owned = self.owned_a.expect("a pure range with a ν over A owns A first");
        (owned, rekey_pinned(cb, matched_raw, self.k, &self.sides[..1]))
    }

    /// `(P_all, ν_P)` for a band: P's input and its rows no inner row matches, both
    /// keyed on P's source PK, over the [`Self::merged`] output.
    pub(super) fn nu(
        &self,
        cb: &mut Circuit,
        merged: NodeId,
        is_left: bool,
        other_unique: bool,
    ) -> Result<(NodeId, NodeId), GnitzSqlError> {
        let i = usize::from(!is_left);
        let base = self.k + if is_left { 0 } else { self.sides[0].n() };
        let pi = rekey_pinned(cb, merged, base, std::slice::from_ref(&self.sides[i]));
        let all = rekey_on_source_pk(cb, self.inputs[i], &self.sides[i], None)?;
        // `π` is P's matched multiplicity, which only a side unique on the
        // equality key bounds by `all`; otherwise the difference is clamped at 0.
        let nu = match other_unique {
            true => cb.difference(all, pi),
            false => cb.positive_diff(all, pi),
        };
        Ok((all, nu))
    }

    /// The [`Self::merged`] output re-keyed onto the source-PK pair `_pair_pk`,
    /// dropping the per-term key region: every consumer reads the payload alone,
    /// and each PK column also rides at the front of its side's kept payload.
    pub(super) fn pair_keyed(&self, cb: &mut Circuit, merged: NodeId) -> NodeId {
        rekey_pinned(cb, merged, self.k, self.sides)
    }
}

/// Re-key both sides on `[eq slots…, range slot]`, NULL keys dropped.
pub(super) fn range_prologue<'a>(
    cb: &mut Circuit,
    class: &JoinClass,
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
    kind: JoinType,
) -> Result<RangePrologue<'a>, GnitzSqlError> {
    let range = class.range.expect("range_prologue receives a range class");
    let tcs = class.key_tcs();
    // No frame covers the terms: the join frame's key may be narrower than theirs.
    reject_arity(
        "range JOIN terms",
        tcs.len() + sides[0].n() + sides[1].n(),
        gnitz_wire::MAX_COLUMNS,
    )?;
    let pure = class.eq.is_empty();
    let reindex =
        |cb: &mut Circuit, i: usize| keyed_side(cb, &sides[i], inputs[i], &tcs, class.eq.len(), false).map(|(_, r)| r);
    let reindex_a = match pure && kind.is_decorrelated() {
        true => None,
        false => Some(reindex(cb, 0)?),
    };
    let (owned_a, int_a) = match reindex_a {
        _ if pure && kind.has_nu(true) => {
            let (owned, int_a) = own_a(cb, &sides[0], inputs[0], range.tc, reindex_a.is_none())?;
            (Some(owned), int_a)
        }
        Some(r) if pure => (None, cb.worker_filter(r)),
        Some(r) => (None, r),
        None => unreachable!("an EXISTS/IN over a pure range has a ν over A"),
    };
    let reindex_b = reindex(cb, 1)?;
    Ok(RangePrologue {
        sides,
        inputs,
        reindex_a,
        reindex_b,
        owned_a,
        int_a,
        k: tcs.len(),
        range_tc: range.tc,
        op: range.op,
    })
}

/// `(owned, int_a)` for a pure range: A's rows re-keyed onto their source PK and
/// kept on its owner, and that slice re-keyed onto the range column, NULL range
/// keys dropped. `scatter` makes the PK re-key A's scatter key.
fn own_a(
    cb: &mut Circuit,
    side: &JoinSide,
    input: NodeId,
    range_tc: TypeCode,
    scatter: bool,
) -> Result<(NodeId, NodeId), GnitzSqlError> {
    let owned = rekey_on_source_pk(cb, input, side, scatter.then_some(side.frame.schema.pk_cols.len()))?;
    let owned = cb.worker_filter(owned); // [a.pk, kept A]
    let &[range_slot] = side.key.as_slice() else {
        unreachable!("a pure range keys on its range column alone")
    };
    let at = side.keep.iter().position(|&c| c as usize == range_slot);
    let at = side.pa() + at.expect("the keep rule keeps a pure range's range column");
    let keep: Vec<u32> = (side.pa() as u32..(side.pa() + side.n()) as u32).collect();
    // [range key, kept A]
    let int_a = cb.map_reindex(
        owned,
        &[(at as u32, range_tc)],
        &keep,
        ReindexRole::Auxiliary,
        NullKeys::Drop,
    );
    Ok((owned, int_a))
}

/// The hidden `_join_pk` output PK columns of an equi join, one per key slot at
/// its pair's common type.
pub(crate) fn join_pk_coldefs(target_tcs: &[TypeCode]) -> Vec<ColumnDef> {
    let k = target_tcs.len();
    target_tcs
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let name = if k == 1 {
                "_join_pk".to_string()
            } else {
                format!("_join_pk_{i}")
            };
            ColumnDef::new(name, t, false).hidden()
        })
        .collect()
}
