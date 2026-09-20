//! The AST-free circuit primitives the one join shell drives: the equi and
//! range/band prologues, the term pair each emits, the pure-range threshold
//! pipeline, the null-fill tails, and the synthetic-key column defs.
//!
//! The axis against [`super::prims`] is granularity: `prims` builds one program or
//! emits at most one node; every item here emits a multi-node DBSP construction.

use super::super::{slot_of, HirRange, JoinClass, JoinType};
use super::prims::{null_gate, rekey_on_source_pk, self_derived_key};
use super::JoinSide;
use crate::error::GnitzSqlError;

use gnitz_core::{Circuit, ColumnDef, NodeId, RangeRel, ReindexRole, ReindexSlot, Schema, TypeCode};
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, JoinKind};

/// One side's join key columns — the equality columns, then the range column — as
/// positions in its own layout.
fn join_key_slots(class: &JoinClass, side: &JoinSide, is_left: bool) -> Result<Vec<usize>, GnitzSqlError> {
    class
        .key_cols(is_left)
        .map(|id| slot_of(&side.frame.layout, id))
        .collect()
}

/// This side's reindex key: each slot's source column paired with the carried
/// target type from `carried_reindex_tc` — `T_i` only where this side's own
/// self-derived type disagrees with it, else `None`, so the engine stays the
/// sole producer of the derived slot type.
fn side_reindex_key(cols: &[usize], coldefs: &[ColumnDef], slot_tcs: &[TypeCode]) -> Vec<ReindexSlot> {
    cols.iter()
        .zip(slot_tcs)
        .map(|(&c, &t)| (c as u32, coldefs[c].type_code.carried_reindex_tc(t)))
        .collect()
}

/// `ΔA ⋈ z⁻¹I(B) + ΔB ⋈ z⁻¹I(A)`, both terms in side order `[key, A, B]`.
/// Shared by the equi, range/band and cross builders.
pub(super) fn join_terms(cb: &mut Circuit, [da, db]: [NodeId; 2], [ta, tb]: [NodeId; 2], kind: JoinKind) -> NodeId {
    let ab = cb.join(da, tb, kind, false);
    let ba = cb.join(db, ta, kind, true);
    cb.union(ab, ba)
}

/// One join side as a prologue gated and reindexed it.
pub(crate) struct EquiSide<'a> {
    side: &'a JoinSide,
    /// This side's rows with NULL keys kept, re-keyed ungated, for a side emitting
    /// its unmatched rows; any other side's `reindex`, which nothing reads as `all`.
    all: NodeId,
    reindex: NodeId,
}

/// NULL-gate `input` over `cols` (SQL 3VL: a NULL key matches nothing) and
/// reindex it onto its key — the one side of either prologue.
///
/// A `gate_after` side re-keys the ungated input once, as `all`, and gates that
/// re-key over the nullable key columns its keep list carries, so its matched rows
/// and `all` are byte-identical and cancel.
fn gated_side<'a>(
    cb: &mut Circuit,
    side: &'a JoinSide,
    input: NodeId,
    cols: Vec<usize>,
    tcs: &[TypeCode],
    gate_after: bool,
) -> Result<EquiSide<'a>, GnitzSqlError> {
    let columns = &side.frame.schema.columns;
    let key = side_reindex_key(&cols, columns, tcs);
    let role = side.scatter_key(&key)?;
    if !gate_after {
        let gated = null_gate(cb, input, &cols, columns)?;
        let reindex = cb.map_reindex(gated, &key, &side.keep, role);
        return Ok(EquiSide { side, all: reindex, reindex });
    }
    let all = cb.map_reindex(input, &key, &side.keep, role);
    let k = tcs.len();
    let gate_cols: Vec<usize> = cols
        .iter()
        .filter(|&&c| columns[c].is_nullable)
        .map(|&c| {
            let at = side.keep.iter().position(|&kept| kept as usize == c);
            k + at.expect("the keep rule keeps a nullable key column of a side emitting its unmatched rows")
        })
        .collect();
    let coldefs: Vec<ColumnDef> = join_pk_coldefs(tcs)
        .into_iter()
        .chain(side.coldefs.iter().cloned())
        .collect();
    let reindex = null_gate(cb, all, &gate_cols, &coldefs)?;
    Ok(EquiSide { side, all, reindex })
}

/// `all − π`, where `π` is `all`'s matched multiplicity keyed like it. Clamped at
/// zero unless the other side is unique on the equality key, which bounds it by
/// `all`.
fn unmatched(cb: &mut Circuit, all: NodeId, pi: NodeId, other_unique: bool) -> NodeId {
    match other_unique {
        true => cb.difference(all, pi),
        false => cb.positive_diff(all, pi),
    }
}

/// What [`equi_prologue`] hands back: the two sides as it built them, the term
/// pair over them, and the per-slot common types.
pub(crate) struct EquiTerms<'a> {
    sides: [EquiSide<'a>; 2],
    tcs: Vec<TypeCode>,
    /// The inner output: both terms over `[_join_pk, kept-A, kept-B]`, unioned.
    pub(crate) inner: NodeId,
}

impl<'a> EquiTerms<'a> {
    /// The join key arity — the width of the `_join_pk` region every term and
    /// null-fill branch leads with.
    fn k(&self) -> usize {
        self.tcs.len()
    }

    /// This side's kept-payload width in the merged `[_join_pk, kept-A, kept-B]`
    /// layout.
    fn kept_n(&self, is_left: bool) -> usize {
        self.side(is_left).side.n()
    }

    /// The type codes of that side's kept payload.
    pub(crate) fn kept_type_codes(&self, is_left: bool) -> Vec<u8> {
        self.side(is_left).side.kept_type_codes()
    }

    /// `P_all` — that side's input re-keyed onto the join key, NULL keys kept.
    pub(crate) fn p_all(&self, is_left: bool) -> NodeId {
        self.side(is_left).all
    }

    /// `ν_P`: that side's rows no row of `inner` matches, keyed like `inner`.
    pub(crate) fn nu(&self, cb: &mut Circuit, inner: NodeId, is_left: bool, other_unique: bool) -> NodeId {
        let p0 = self.k() + if is_left { 0 } else { self.kept_n(true) };
        let proj: Vec<u32> = (p0..p0 + self.kept_n(is_left)).map(|c| c as u32).collect();
        let pi = cb.map(inner, &proj); // π_P(inner) = [_join_pk, P]
        unmatched(cb, self.p_all(is_left), pi, other_unique)
    }

    fn side(&self, is_left: bool) -> &EquiSide<'a> {
        &self.sides[usize::from(!is_left)]
    }
}

/// Gate and reindex both sides on the eq pairs, then emit the symmetric 2-term
/// join over them. `b_unique` says B is unique on the equality columns.
pub(crate) fn equi_prologue<'a>(
    cb: &mut Circuit,
    class: &JoinClass,
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
    kind: JoinType,
    b_unique: bool,
) -> Result<EquiTerms<'a>, GnitzSqlError> {
    let tcs: Vec<TypeCode> = class.eq.iter().map(|p| p.tc).collect();
    let side = |cb: &mut Circuit, is_left: bool| {
        let i = usize::from(!is_left);
        let cols = join_key_slots(class, &sides[i], is_left)?;
        gated_side(cb, &sides[i], inputs[i], cols, &tcs, kind.emits_unmatched(is_left))
    };
    let b = side(cb, false)?;
    let a = side(cb, true)?;
    // A decorrelated join asks only whether a key exists: B as a set weighs every
    // match 1, so the joined A rows are the matched set at their own weight.
    let b_delta = match kind.is_decorrelated() && !b_unique {
        true => cb.distinct(b.reindex),
        false => b.reindex,
    };
    let trace_a = cb.integrate_trace(a.reindex);
    let trace_b = cb.integrate_trace(b_delta);
    let inner = join_terms(cb, [a.reindex, b_delta], [trace_a, trace_b], JoinKind::Equi);
    Ok(EquiTerms { sides: [a, b], tcs, inner })
}

/// The output PK columns a re-key onto source PKs mints: one per listed
/// `(schema, pk column)`, typed by the reindex's self-derive output type,
/// non-nullable, and hidden. The two producers below differ only in `name`.
fn rekey_pk_coldefs<'a>(
    cols: impl IntoIterator<Item = (&'a Schema, u32)>,
    name: impl Fn(usize, &ColumnDef) -> String,
) -> Vec<ColumnDef> {
    cols.into_iter()
        .enumerate()
        .map(|(slot, (schema, c))| {
            let src = &schema.columns[c as usize];
            ColumnDef::new(name(slot, src), src.type_code.reindex_output_type(), false).hidden()
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

/// The pair-PK slot list `[a.pk…, b.pk…]` over a layout carrying A's kept payload
/// from `a_base` and B's from `b_base`. Each side's PK arity comes from the side,
/// so only the two bases vary. One home, so the inner terms and the null-fill
/// branches key byte-identically and cancel.
fn pair_pk_slots(sides: &[JoinSide; 2], a_base: usize, b_base: usize) -> Vec<usize> {
    (a_base..a_base + sides[0].pa())
        .chain(b_base..b_base + sides[1].pa())
        .collect()
}

/// The shared null-fill tail of a range/band outer join: null-extend `ν_P` with
/// the O-side NULL columns on O's own side of the payload, then re-key onto the
/// pair-PK. Either preserved side lands on the full `[pair-PK, A, B]` the inner
/// terms and the post-union WHERE read.
pub(super) fn emit_range_null_fill_tail(
    cb: &mut Circuit,
    sides: &[JoinSide; 2],
    nf_keyed: NodeId,
    preserved_is_left: bool,
) -> NodeId {
    let (left, right) = (&sides[0], &sides[1]);

    let (p, o) = if preserved_is_left {
        (left, right)
    } else {
        (right, left)
    };
    let (p_pk, p_n, o_n) = (p.pa(), p.n(), o.n());
    // [P.pk × p_pk, A, B], one of the two payload halves all-NULL.
    let nullfill = cb.null_extend(nf_keyed, &o.kept_type_codes(), !preserved_is_left);

    // Each side's PK sits at the front of its own kept columns; the NULL side's
    // packs to the synthetic 0.
    let nf_pair_pk_cols = pair_pk_slots(sides, p_pk, p_pk + left.n());

    // Drop the leading P.pk key region: its columns also ride inside P.
    let keep: Vec<u32> = (p_pk as u32..(p_pk + p_n + o_n) as u32).collect();
    cb.map_reindex(
        nullfill,
        &self_derived_key(&nf_pair_pk_cols),
        &keep,
        ReindexRole::Auxiliary,
    )
}

/// Re-key `node`, which carries `side`'s kept payload from `base` with its pinned
/// source PK at the front, onto that PK, keeping the payload. One home, so every
/// operand re-keyed onto a side's source PK is byte-identical and cancels.
fn rekey_payload_pk(cb: &mut Circuit, node: NodeId, base: usize, side: &JoinSide) -> NodeId {
    let pk: Vec<usize> = (base..base + side.pa()).collect();
    let keep: Vec<u32> = (base as u32..(base + side.n()) as u32).collect();
    cb.map_reindex(node, &self_derived_key(&pk), &keep, ReindexRole::Auxiliary)
}

/// What [`range_prologue`] hands back. `op` is the ON clause's left-to-right range
/// relation, which both terms carry verbatim.
pub(crate) struct RangePrologue<'a> {
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
    /// A's range-keyed delta; `None` for an EXISTS/IN over a pure range, which
    /// reads only the threshold.
    reindex_a: Option<NodeId>,
    reindex_b: NodeId,
    /// A's owned slice keyed on its source PK, NULL range keys included — built for
    /// a pure range with a ν over A.
    owned_a: Option<NodeId>,
    int_a: NodeId,
    trace_a: NodeId,
    all_tcs: Vec<TypeCode>,
    op: RangeRel,
}

impl RangePrologue<'_> {
    /// The key arity: the eq prefix plus the one range slot — the width of the
    /// key region every term leads with.
    fn k(&self) -> usize {
        self.all_tcs.len()
    }

    /// The eq-prefix width the range probe matches on before comparing.
    fn n_eq(&self) -> usize {
        self.k() - 1
    }

    /// The two range terms over `[key region, A, B]`. A pure range integrates B's
    /// worker-filtered slice: an unfiltered trace against the broadcast A delta
    /// would emit each pair once per worker.
    pub(crate) fn merged(&self, cb: &mut Circuit) -> NodeId {
        let n_eq = self.n_eq() as u8;
        let reindex_a = self.reindex_a.expect("the pair terms read A's range-keyed delta");
        let int_b = match n_eq {
            0 => cb.worker_filter(self.reindex_b),
            _ => self.reindex_b,
        };
        let trace_b = cb.integrate_trace(int_b);
        let kind = JoinKind::Range { n_eq, rel: self.op };
        join_terms(cb, [reindex_a, self.reindex_b], [self.trace_a, trace_b], kind)
    }

    /// `(A_owned, matched)` for a pure range: A's owned slice and the rows of it
    /// matching the one-row threshold `m = MAX/MIN(b.range)`, both keyed on
    /// `[a.pk…, A]`, so `A_owned − matched` is the unmatched set.
    pub(crate) fn threshold(&self, cb: &mut Circuit) -> (NodeId, NodeId) {
        let left = &self.sides[0];
        let (k, n_eq) = (self.k(), self.n_eq() as u8);
        let want_max = matches!(self.op, RangeRel::Lt | RangeRel::Le);
        let agg_func = if want_max { WireAggFunc::Max } else { WireAggFunc::Min };

        // Decodes the OPK key into a native payload value, so MIN/MAX order values.
        let mbh = cb.map_hash_row(self.reindex_b, &[(0, None)], 0);
        // Local over the broadcast B, so every worker holds the same extremum. No
        // ground row: a `m = NULL` seed would break `A − 0 = A` over an empty B.
        // The COUNT is the cardinality gate every reduce carries; the reindex
        // below keeps no payload, so it goes no further.
        let specs = [
            AggDescriptor { agg_op: agg_func, col_idx: 1 },
            AggDescriptor::COUNT_STAR,
        ];
        let red = cb.reduce_multi_local(mbh, &[], &specs, false); // [_group_pk:U128, m:Tc, count]
        let reindex_m = cb.map_reindex(red, &self_derived_key(&[1]), &[], ReindexRole::Auxiliary);
        let trace_m = cb.integrate_trace(reindex_m);

        // `m` carries no payload, so both terms are `[_join_pk × k, A]`.
        let kind = JoinKind::Range { n_eq, rel: self.op };
        let matched_raw = join_terms(cb, [self.int_a, reindex_m], [self.trace_a, trace_m], kind);
        let owned = self.owned_a.expect("a pure range with a ν over A owns A first");
        (owned, rekey_payload_pk(cb, matched_raw, k, left))
    }

    /// `(P_all, ν_P)` for a band: P's input and its rows no inner row matches, both
    /// keyed on P's source PK, over the [`Self::merged`] output.
    pub(crate) fn nu(&self, cb: &mut Circuit, merged: NodeId, is_left: bool, other_unique: bool) -> (NodeId, NodeId) {
        let i = usize::from(!is_left);
        let base = self.k() + if is_left { 0 } else { self.sides[0].n() };
        let pi = rekey_payload_pk(cb, merged, base, &self.sides[i]);
        let all = rekey_on_source_pk(cb, self.inputs[i], &self.sides[i], ReindexRole::Auxiliary);
        (all, unmatched(cb, all, pi, other_unique))
    }

    /// The [`Self::merged`] output re-keyed onto the source-PK pair `_pair_pk`,
    /// dropping the per-term key region: every consumer reads the payload alone,
    /// and each PK column also rides at the front of its side's kept payload.
    pub(crate) fn pair_keyed(&self, cb: &mut Circuit, merged: NodeId) -> NodeId {
        let (k, pl, pr) = (self.k(), self.sides[0].n(), self.sides[1].n());
        cb.map_reindex(
            merged,
            &self_derived_key(&pair_pk_slots(self.sides, k, k + pl)),
            &(k as u32..(k + pl + pr) as u32).collect::<Vec<_>>(),
            ReindexRole::Auxiliary,
        )
    }
}

/// Gate and reindex both sides on `[eq slots…, range slot]`, and integrate A.
///
/// A pure range with a ν over A integrates A's PK-owned slice, which is also the
/// minuend of its unmatched set, NULL range keys included; an INNER one integrates
/// this worker's slice of the broadcast A.
pub(crate) fn range_prologue<'a>(
    cb: &mut Circuit,
    class: &JoinClass,
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
    kind: JoinType,
) -> Result<RangePrologue<'a>, GnitzSqlError> {
    let range = class.range.expect("range_prologue receives a range class");
    let all_tcs: Vec<TypeCode> = class.eq.iter().map(|p| p.tc).chain([range.tc]).collect();
    let pure = class.eq.is_empty();
    let reindex = |cb: &mut Circuit, is_left: bool| -> Result<NodeId, GnitzSqlError> {
        let i = usize::from(!is_left);
        let cols = join_key_slots(class, &sides[i], is_left)?;
        Ok(gated_side(cb, &sides[i], inputs[i], cols, &all_tcs, false)?.reindex)
    };
    let reindex_a = match pure && kind.is_decorrelated() {
        true => None,
        false => Some(reindex(cb, true)?),
    };
    let (owned_a, int_a) = match reindex_a {
        _ if pure && kind.has_nu(true) => {
            let (owned, int_a) = own_a(cb, &sides[0], inputs[0], range, reindex_a.is_none())?;
            (Some(owned), int_a)
        }
        Some(r) if pure => (None, cb.worker_filter(r)),
        Some(r) => (None, r),
        None => unreachable!("an EXISTS/IN over a pure range has a ν over A"),
    };
    let trace_a = cb.integrate_trace(int_a);
    let reindex_b = reindex(cb, false)?;
    Ok(RangePrologue {
        sides,
        inputs,
        reindex_a,
        reindex_b,
        owned_a,
        int_a,
        trace_a,
        all_tcs,
        op: range.op,
    })
}

/// `(owned, int_a)`: A's rows re-keyed onto their source PK and kept on its owner,
/// and that slice NULL-gated and re-keyed onto the range column. `scatter` makes
/// the PK re-key A's scatter key.
fn own_a(
    cb: &mut Circuit,
    side: &JoinSide,
    input: NodeId,
    range: HirRange,
    scatter: bool,
) -> Result<(NodeId, NodeId), GnitzSqlError> {
    let role = match scatter {
        true => side.scatter_pk(),
        false => ReindexRole::Auxiliary,
    };
    let range_slot = slot_of(&side.frame.layout, range.left)?;
    let owned = rekey_on_source_pk(cb, input, side, role);
    let owned = cb.worker_filter(owned); // [a.pk, kept A]
    let pa = side.pa();
    let coldefs: Vec<ColumnDef> = src_pk_coldefs(&side.frame.schema)
        .into_iter()
        .chain(side.coldefs.iter().cloned())
        .collect();
    let at = side.keep.iter().position(|&c| c as usize == range_slot);
    let cols = vec![pa + at.expect("the keep rule keeps a pure range's range column")];
    let gated = null_gate(cb, owned, &cols, &coldefs)?;
    let key = side_reindex_key(&cols, &coldefs, &[range.tc]);
    let keep: Vec<u32> = (pa as u32..(pa + side.n()) as u32).collect();
    let int_a = cb.map_reindex(gated, &key, &keep, ReindexRole::Auxiliary); // [range key, kept A]
    Ok((owned, int_a))
}

/// The k synthetic `_join_pk` output PK columns of an equi join / equi EXISTS
/// view, hidden (a physical PK column, not a presentation column — `SELECT *`
/// omits it and it is not name-resolvable). Each slot carries its pair's common
/// type `T_i` — the single persisted stride both sides' reindex Maps and every
/// cross-process consumer re-derive. The first column keeps the name
/// `_join_pk` at k = 1 (preserving the shippable single-key catalog name);
/// composite keys use `_join_pk_{i}`.
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
