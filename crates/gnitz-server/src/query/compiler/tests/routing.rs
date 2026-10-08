use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::{make_schema_u64_i64, pk_payload_schema, scan_keyed, scan_routed, self_typed_slots};
use gnitz_wire::{Circuit, ComputeMap, JoinKind, KeyRange, NullKeys, PkColList, RangeRel, ReindexRole, TypeCode};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{Placement, SchemaColumn, Slot};

/// A registry for one worker of four: a view computed whole by one worker keeps
/// no route.
fn sources<S: Into<Source>>(rows: impl IntoIterator<Item = (u64, S)>) -> RelationRegistry {
    sources_at(Slot::new(0, 4), rows)
}

/// `derive`'s routing metadata, placed as a one-column-PK view.
fn derive(c: Circuit, registry: &RelationRegistry) -> Result<ViewMeta, String> {
    routing(&loaded(c), registry)
}

/// A U64 PK and five I64 payload columns: every key a fixture names is payload.
fn wide_schema() -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..5).map(|_| SchemaColumn::new(TypeCode::I64, false)));
    SchemaDescriptor::new(&cols, &[0])
}

/// 3-column compound PK `(U32, U64, U64)` + one payload.
fn clustered_schema() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U32, TypeCode::U64, TypeCode::U64])
}

/// [`clustered_schema`], placed by its leading PK column.
fn clustered_source() -> Source {
    let schema = clustered_schema();
    Source::from(schema).placed(Placement::keyed(&schema, 1))
}

/// A range join: a pure range over a one-slot key, a band over a wider one.
fn range() -> JoinKind {
    JoinKind::Range { rel: RangeRel::Lt }
}

/// `d ⋈ I(d)`: the smallest circuit in which `d`'s source reaches a join.
fn join_with_own_trace(c: &mut Circuit, d: NodeId, kind: JoinKind) -> NodeId {
    c.join(d, d, kind, false)
}

/// A reindex of `input` on `cols` of [`wide_schema`], stated as the route of the
/// source `input` reads.
fn states_route(c: &mut Circuit, input: NodeId, cols: &[u32]) -> NodeId {
    let key = self_typed_slots(&wide_schema(), cols);
    let role = ReindexRole::ScatterKey { source_cols: cols.to_vec() };
    c.map_reindex(input, &key, &[0], role, NullKeys::Keep)
}

/// How many leading slots of a `key_len`-slot key a join of `kind` co-locates
/// its sides by: an equi join's whole key, a range join's equality prefix, none
/// of a cross join's.
fn routed(kind: JoinKind, key_len: usize) -> usize {
    match kind {
        JoinKind::Equi => key_len,
        JoinKind::Range { .. } => key_len - 1,
        JoinKind::Cross => 0,
    }
}

/// [`scan_routed`] by the slots a join of `kind` routes `key` by.
fn scan_joined(c: &mut Circuit, source: u64, key: &[gnitz_wire::ReindexSlot], kind: JoinKind) -> NodeId {
    scan_routed(c, source, key, routed(kind, key.len()))
}

/// A reindex on `key` that states no route.
fn aux_reindex(c: &mut Circuit, input: NodeId, key: &[gnitz_wire::ReindexSlot]) -> NodeId {
    c.map_reindex(input, key, &[0], ReindexRole::Auxiliary, NullKeys::Keep)
}

/// `relay` is keyed and splits rows by the join key over `cols` of `schema`: it
/// sends every row of a probe whose columns all differ where that key does.
fn assert_routes_by(relay: Option<&Relay>, schema: &SchemaDescriptor, cols: &[u32]) {
    let Some(Relay::Round(got) | Relay::Share(got)) = relay else {
        panic!("a keyed relay");
    };
    let mut bb = BatchBuilder::new(schema);
    for i in 0..64u64 {
        bb.begin_row(i as u128, 1);
        for c in 0..schema.num_payload_cols() as u64 {
            bb.put_u64((i << 8 | c).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
        bb.end_row();
    }
    let probe = bb.finish();
    let want = ScatterPlan::join(schema, &self_typed_slots(schema, cols)).unwrap();
    let (mut got_lists, mut want_lists) = (Vec::new(), Vec::new());
    assert_eq!(
        got.route(&probe, &mut got_lists, 4),
        want.route(&probe, &mut want_lists, 4)
    );
}

// ── The output exchange ─────────────────────────────────────────────────

/// The output shard moves nothing exactly where a walk back through row-local
/// nodes ends at a scan whose rows are already placed by the shard key. Exchanging
/// is correct too, so nothing above this layer can tell the skip fired.
#[test]
fn the_output_exchange_is_skipped_only_behind_a_row_local_walk_to_a_scan_placed_by_the_shard_key() {
    // PK last, so a shard column behind a map — an index into the map's output,
    // whose leading slots are the source PK — is not the source's column index.
    let i64 = SchemaColumn::new(TypeCode::I64, false);
    let u64 = SchemaColumn::new(TypeCode::U64, false);
    let pk_last = SchemaDescriptor::new(&[i64, i64, u64], &[2]);
    let clustered = clustered_schema();
    let compute = |c: &mut Circuit, n: NodeId| {
        c.map_expr(
            n,
            ComputeMap {
                program: dummy_expr_blob(),
                out_cols: vec![],
            },
        )
    };
    type Mid = fn(&mut Circuit, NodeId) -> NodeId;
    let mapped = SchemaDescriptor::new(&[u64, i64, i64], &[0]);
    let key_only = SchemaDescriptor::new(&[u64], &[0]);
    let rekeyed = SchemaDescriptor::new(&[i64, i64], &[0]);
    // (why, the source, the walk, the schema it hands the shard, the columns the
    // shard's reader groups by, skipped)
    type Case = (&'static str, Source, Mid, SchemaDescriptor, &'static [u32], bool);
    let clustered_by_prefix = clustered_source();
    let cases: [Case; 14] = [
        ("a bare scan", pk_last.into(), |_, n| n, pk_last, &[2], true),
        ("a payload column", pk_last.into(), |_, n| n, pk_last, &[0], false),
        ("an empty key", pk_last.into(), |_, n| n, pk_last, &[], false),
        (
            "the CLUSTER BY prefix",
            clustered_by_prefix,
            |_, n| n,
            clustered,
            &[0],
            true,
        ),
        (
            "not the leading PK column",
            clustered_by_prefix,
            |_, n| n,
            clustered,
            &[1],
            false,
        ),
        (
            "a filter is transparent",
            pk_last.into(),
            |c, n| c.filter(n, dummy_expr_blob()),
            pk_last,
            &[2],
            true,
        ),
        (
            "a copy list carries the PK region to its leading slot",
            pk_last.into(),
            |c, n| c.map(n, &[0, 1]),
            mapped,
            &[0],
            true,
        ),
        (
            "a payload slot behind a map",
            pk_last.into(),
            |c, n| c.map(n, &[0, 1]),
            mapped,
            &[1],
            false,
        ),
        (
            "an expression map carries the PK region",
            pk_last.into(),
            compute,
            key_only,
            &[0],
            true,
        ),
        (
            "filters and maps interleave",
            pk_last.into(),
            |c, n| {
                let f = c.filter(n, dummy_expr_blob());
                let m = c.map(f, &[0, 1]);
                c.filter(m, dummy_expr_blob())
            },
            mapped,
            &[0],
            true,
        ),
        (
            "a reindex re-keys the PK",
            pk_last.into(),
            |c, n| aux_reindex(c, n, &[(0, TypeCode::I64)]),
            rekeyed,
            &[0],
            false,
        ),
        (
            "a WorkerFilter is not a Filter",
            pk_last.into(),
            |c, n| c.worker_filter(n),
            pk_last,
            &[2],
            false,
        ),
        (
            "a fan-in draws from two sources",
            pk_last.into(),
            |c, n| {
                let other = scan(c, 8);
                c.union(n, other)
            },
            pk_last,
            &[2],
            false,
        ),
        (
            "a filter over a fan-in",
            pk_last.into(),
            |c, n| {
                let other = scan(c, 8);
                let u = c.union(n, other);
                c.filter(u, dummy_expr_blob())
            },
            pk_last,
            &[2],
            false,
        ),
    ];
    for (why, schema, mid, at_shard, cols, want) in cases {
        let mut c = Circuit::default();
        let source = scan(&mut c, 7);
        let tip = mid(&mut c, source);
        let shard = c.shard(tip);
        let scatter = ScatterPlan::group(&at_shard, cols).expect(why);
        assert_eq!(
            skips_output_exchange(&loaded(c), shard, &scatter, &sources([(7, schema)])),
            want,
            "{why}"
        );
    }
}

// ── A join's sources ────────────────────────────────────────────────────

/// The two-term join of sources 7 and 9, each reindexed on `key_cols` and
/// optionally clamped to a set, behind an output shard.
fn join_meta_in(kind: JoinKind, key_cols: &[u32], distinct: [bool; 2], ext: RelationRegistry) -> ViewMeta {
    let mut c = Circuit::default();
    let deltas = [(7, distinct[0]), (9, distinct[1])].map(|(source, set)| {
        let schema = ext.relation(source).expect("a registered source").schema();
        let keyed = scan_joined(&mut c, source, &self_typed_slots(&schema, key_cols), kind);
        match set {
            true => c.distinct(keyed),
            false => keyed,
        }
    });
    let joined = c.join_terms(deltas, deltas, kind);
    c.shard(joined);
    derive(c, &ext).expect("fixture routes")
}

/// A source's relay follows what its join needs co-located and what is already
/// in place: a keyed join's sources meet on the key's owner unless they sit there
/// or a replicated partner makes every match locally; a keyless join's matches
/// share no key, so a partitioned source is broadcast.
#[test]
fn a_joins_sources_relay_only_where_their_matches_are_not_already_local() {
    use Route::{Broadcast, Round, Stays};
    let base = Source::from(make_schema_u64_i64());
    let replicated = base.placed(Placement::Replicated);
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = Source::from(pk_payload_schema(&[TypeCode::U64; 2]));
    let clustered = compound.placed(Placement::keyed(&compound.schema, 1));
    let plain = [false; 2];
    /// `(why, join kind, key columns, set-clamped sides, sources 7 and 9, their routes)`.
    type Case = (
        &'static str,
        JoinKind,
        &'static [u32],
        [bool; 2],
        [Source; 2],
        [Route; 2],
    );
    let cases: [Case; 14] = [
        (
            "the key is the PK sequence both sides are placed by",
            JoinKind::Equi,
            &[0, 1],
            plain,
            [compound; 2],
            [Stays, Stays],
        ),
        (
            "the permuted PK is not the distribution prefix",
            JoinKind::Equi,
            &[1, 0],
            plain,
            [compound; 2],
            [Round, Round],
        ),
        ("a payload key", JoinKind::Equi, &[1], plain, [base; 2], [Round, Round]),
        (
            "a replicated partner: the partitioned side joins its full local copy",
            JoinKind::Equi,
            &[1],
            plain,
            [replicated, base],
            [Stays, Stays],
        ),
        (
            "replicated ⋈ replicated",
            JoinKind::Equi,
            &[1],
            plain,
            [replicated; 2],
            [Stays, Stays],
        ),
        (
            "a partitioned side clamped to a set needs every row of a key on one worker",
            JoinKind::Equi,
            &[1],
            [false, true],
            [replicated, base],
            [Stays, Round],
        ),
        (
            "a replicated side clamps its whole copy on every worker",
            JoinKind::Equi,
            &[1],
            [true, false],
            [replicated, base],
            [Stays, Stays],
        ),
        (
            "a band join's equality prefix is the CLUSTER BY key",
            range(),
            &[0, 1],
            plain,
            [clustered; 2],
            [Stays, Stays],
        ),
        (
            "the full-PK distribution is wider than a band join's equality prefix",
            range(),
            &[0, 1],
            plain,
            [compound; 2],
            [Round, Round],
        ),
        (
            "a band join beside a replicated partner",
            range(),
            &[0, 1],
            plain,
            [compound.placed(Placement::Replicated), compound],
            [Stays, Stays],
        ),
        (
            "an equi join on the key its sources are placed by",
            JoinKind::Equi,
            &[0],
            plain,
            [base; 2],
            [Stays, Stays],
        ),
        (
            "a pure-range join's matches spread over the whole other side",
            range(),
            &[0],
            plain,
            [base; 2],
            [Broadcast, Broadcast],
        ),
        (
            "a cross join's too",
            JoinKind::Cross,
            &[0],
            plain,
            [base; 2],
            [Broadcast, Broadcast],
        ),
        (
            "a replicated source already holds what a broadcast would hand it",
            range(),
            &[1],
            plain,
            [replicated, base],
            [Stays, Broadcast],
        ),
    ];
    for (why, kind, key_cols, distinct, [a, b], want) in cases {
        let meta = join_meta_in(kind, key_cols, distinct, sources([(7, a), (9, b)]));
        assert_eq!([7, 9].map(|tid| route(meta.source_route(tid))), want, "{why}");
    }
}

/// A view one worker computes whole relays nothing. A key a relay would route
/// by is validated all the same: a restart may launch more workers.
#[test]
fn a_self_contained_view_keeps_no_route_and_still_validates_its_keys() {
    let joined = |key: &[gnitz_wire::ReindexSlot]| {
        let mut c = Circuit::default();
        let deltas = [7, 9].map(|source| scan_keyed(&mut c, source, key));
        c.join_terms(deltas, deltas, JoinKind::Equi);
        c
    };
    let base = Source::from(make_schema_u64_i64());
    let replicated = base.placed(Placement::Replicated);
    let on_payload = self_typed_slots(&base.schema, &[1]);
    let routes = |meta: &ViewMeta| [7, 9].map(|tid| route(meta.source_route(tid)));

    let solo = sources_at(Slot::SOLO, [(7, base), (9, base)]);
    for (why, registry) in [
        ("one worker", &solo),
        ("every source replicated", &sources([(7, replicated), (9, replicated)])),
    ] {
        let meta = derive(joined(&on_payload), registry).unwrap();
        assert!(meta.self_contained, "{why}");
        assert_eq!(routes(&meta), [Route::Stays, Route::Stays], "{why}");
    }
    let refused = rejection(derive(joined(&[(9, TypeCode::I64)]), &solo));
    assert!(refused.starts_with("source 7 scatter key"), "{refused}");

    let meta = derive(joined(&on_payload), &sources([(7, base), (9, base)])).unwrap();
    assert!(!meta.self_contained);
    assert_eq!(routes(&meta), [Route::Round, Route::Round]);
}

/// A source is routed by the slots it states: a range join's sides state its
/// key's equality prefix — none for a key of the range slot alone, which
/// broadcasts — and an equi join's the whole key.
#[test]
fn a_source_routes_by_the_prefix_of_its_key_it_states() {
    let wide = || sources([(7, wide_schema()), (9, wide_schema())]);
    let band = join_meta_in(range(), &[3, 4], [false; 2], wide());
    assert_routes_by(band.source_route(7), &wide_schema(), &[3]);
    let wider = join_meta_in(range(), &[2, 3, 4], [false; 2], wide());
    assert_routes_by(wider.source_route(7), &wide_schema(), &[2, 3]);
    let pure = join_meta_in(range(), &[3], [false; 2], wide());
    assert_eq!(route(pure.source_route(7)), Route::Broadcast);
    let equi = join_meta_in(JoinKind::Equi, &[3, 4], [false; 2], wide());
    assert_routes_by(equi.source_route(7), &wide_schema(), &[3, 4]);
}

/// The replicated source 7 and the partitioned source 9 meet in an equi join, and
/// 7's reindexed delta also feeds a `Union` beside the join — the shape of an
/// outer or semi join's preserved side. Read there it counts once per worker, so
/// neither source may skip.
#[test]
fn a_replicated_delta_feeding_more_than_its_join_scatters_every_source() {
    let base = make_schema_u64_i64();
    let key = self_typed_slots(&base, &[1]);
    let mut c = Circuit::default();
    let (a, b) = (scan_keyed(&mut c, 7, &key), scan_keyed(&mut c, 9, &key));
    let joined = c.join(a, b, JoinKind::Equi, false);
    c.union(a, joined);
    let ext = sources([(7, Source::from(base).placed(Placement::Replicated)), (9, base.into())]);
    let meta = derive(c, &ext).unwrap();
    assert_eq!(
        [7, 9].map(|tid| route(meta.source_route(tid))),
        [Route::Share, Route::Round]
    );
}

/// A source whose re-key only an owner filter reads never takes the
/// replicated-partner skip: its filter drops every row the relay did not place.
#[test]
fn an_owner_trimmed_source_routes_by_the_key_it_states() {
    let base = Source::from(make_schema_u64_i64());
    let replicated = base.placed(Placement::Replicated);
    let trimmed = |kind: JoinKind, key_cols: &[u32], [a, b]: [Source; 2]| {
        let mut c = Circuit::default();
        let keyed = scan_keyed(&mut c, 7, &self_typed_slots(&a.schema, key_cols));
        let owned = c.worker_filter(keyed);
        let delta = scan_joined(&mut c, 9, &self_typed_slots(&b.schema, &[1]), kind);
        c.join(delta, owned, kind, false);
        derive(c, &sources([(7, a), (9, b)])).unwrap()
    };
    let routes = |meta: &ViewMeta| [7, 9].map(|tid| route(meta.source_route(tid)));

    let meta = trimmed(JoinKind::Equi, &[1], [base, replicated]);
    assert_eq!(routes(&meta), [Route::Round, Route::Stays]);
    assert_routes_by(meta.source_route(7), &base.schema, &[1]);

    let meta = trimmed(range(), &[0], [base, base]);
    assert_eq!(
        routes(&meta),
        [Route::Stays, Route::Broadcast],
        "already on its PK's owner"
    );

    let meta = trimmed(range(), &[0], [replicated, base]);
    assert_eq!(
        routes(&meta),
        [Route::Share, Route::Broadcast],
        "a replicated copy is kept by key where it stands"
    );
    assert_routes_by(meta.source_route(7), &replicated.schema, &[0]);
}

/// A source states one route however many reindexes name it, and none when it
/// feeds no join: source 20 reaches the output through a `Union` beside the join's
/// output, so its rows are already where the view needs them.
#[test]
fn a_source_routes_by_the_one_key_it_states() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 10);
    let delta = states_route(&mut c, source, &[2]);
    let again = states_route(&mut c, source, &[2]);
    let joined = c.join(delta, again, JoinKind::Equi, false);
    let other = scan(&mut c, 20);
    c.union(joined, other);
    let meta = derive(c, &sources([(10, wide_schema()), (20, wide_schema())])).unwrap();
    assert_routes_by(meta.source_route(10), &wide_schema(), &[2]);
    assert_eq!(route(meta.source_route(20)), Route::Stays);
}

/// A `Map` between the scan and the reindex moves the key, so the route is read
/// as the source's own columns: reading the node's own key would scatter source
/// 7 by column 2 of a relation whose join column is 5.
#[test]
fn the_route_is_the_stated_one_not_the_reindex_nodes_own_key() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 7);
    let moved = c.map(source, &[5]);
    let role = ReindexRole::ScatterKey { source_cols: vec![5] };
    let delta = c.map_reindex(moved, &[(2, TypeCode::I64)], &[0], role, NullKeys::Keep);
    join_with_own_trace(&mut c, delta, JoinKind::Equi);
    let meta = derive(c, &sources([(7, wide_schema())])).unwrap();
    assert_routes_by(meta.source_route(7), &wide_schema(), &[5]);
}

/// A circuit is client-supplied, and workers route by its metadata mid-round,
/// where a refusal is fatal — so each route `derive` cannot stand behind is
/// refused there, on the master and on every worker alike.
#[test]
fn a_circuit_derive_cannot_route_is_rejected() {
    let cases: [(Build, &str); 5] = [
        // Integrated wherever it was produced, beside a trace scattered by the key.
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let delta = aux_reindex(c, source, &self_typed_slots(&wide_schema(), &[1]));
                join_with_own_trace(c, delta, JoinKind::Equi);
            },
            "source 7 feeds a join and states no scatter key",
        ),
        // Scattering by either key alone leaves the trace side keyed by the other.
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let (a, b) = (states_route(c, source, &[1]), states_route(c, source, &[2]));
                c.join(a, b, JoinKind::Equi, false);
            },
            "source 7 feeds several distinct scatter keys",
        ),
        // A column the relation has not got.
        (
            |c: &mut Circuit| {
                let delta = scan_keyed(c, 7, &[(9, TypeCode::I64)]);
                join_with_own_trace(c, delta, JoinKind::Equi);
            },
            "source 7 scatter key",
        ),
        (
            |c: &mut Circuit| {
                // A union's rows are no one source's.
                let (a, b) = (scan(c, 7), scan(c, 9));
                let both = c.union(a, b);
                states_route(c, both, &[1]);
            },
            "a scatter key over no scanned source",
        ),
        (
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 8));
                c.union(a, b);
            },
            "relation 8 is not registered",
        ),
    ];
    for (build, guard) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let refused = rejection(derive(c, &sources([(7, wide_schema()), (9, wide_schema())])));
        assert!(refused.starts_with(guard), "{guard}: got {refused}");
    }
}

// ── Placement ───────────────────────────────────────────────────────────

/// A view's rows sit where its circuit leaves them: on every worker when every
/// source is replicated, on its own key's owner behind an exchange or a join, on
/// a source's owner while the walk back from the output keeps that source's PK
/// region and nothing relays its delta — and otherwise wherever they were produced.
#[test]
fn a_view_is_placed_by_where_its_circuit_leaves_its_rows() {
    let clustered = clustered_source();
    let inherited = clustered.placement;
    let replicated = clustered.placed(Placement::Replicated);
    // A view of `pk_arity` PK columns, and the placement by its own key.
    let view = |pk_arity: usize| pk_payload_schema(&[TypeCode::U32, TypeCode::U64, TypeCode::U64][..pk_arity]);
    let own_key = Placement::full_pk(&view(3));
    let cases: [(&str, Build, [Source; 2], usize, Placement); 12] = [
        (
            "a bare scan re-emits its source's PK region",
            |c: &mut Circuit| {
                scan(c, 7);
            },
            [clustered; 2],
            3,
            inherited,
        ),
        (
            "at another PK arity the prefix names other columns",
            |c: &mut Circuit| {
                scan(c, 7);
            },
            [clustered; 2],
            2,
            Placement::Local,
        ),
        (
            "a filter and a projection keep the PK region",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                let f = c.filter(a, dummy_expr_blob());
                c.map(f, &[0]);
            },
            [clustered; 2],
            3,
            inherited,
        ),
        (
            "a bare reindex exchanges nothing and still replaces the PK region",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                aux_reindex(c, a, &[(3, TypeCode::I64)]);
            },
            [clustered; 2],
            3,
            Placement::Local,
        ),
        (
            "a WorkerFilter is not a Filter",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                c.worker_filter(a);
            },
            [clustered; 2],
            3,
            Placement::Local,
        ),
        (
            "a shard moves the rows to the view's own key",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                c.shard(a);
            },
            [clustered; 2],
            3,
            own_key,
        ),
        (
            "a join carries no shard and still re-keys",
            |c: &mut Circuit| {
                let d = scan_keyed(c, 7, &[(3, TypeCode::I64)]);
                join_with_own_trace(c, d, JoinKind::Equi);
            },
            [clustered; 2],
            3,
            own_key,
        ),
        (
            "two keyed sources exchange nothing, so each row stays on its own source's owner",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                c.union(a, b);
            },
            [clustered; 2],
            3,
            Placement::Local,
        ),
        (
            "a source's rows walked back to its scan, while a join relays its delta",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                let role = ReindexRole::ScatterKey { source_cols: vec![3] };
                let d = c.map_reindex(a, &[(3, TypeCode::I64)], &[0], role, NullKeys::Keep);
                join_with_own_trace(c, d, JoinKind::Equi);
                c.filter(a, dummy_expr_blob());
            },
            [clustered; 2],
            3,
            Placement::Local,
        ),
        (
            "every source replicated: each worker computes the whole result",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                let u = c.union(a, b);
                c.shard(u);
            },
            [replicated; 2],
            3,
            Placement::Replicated,
        ),
        (
            "a replicated source beside a keyed one",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                c.union(a, b);
            },
            [replicated, clustered],
            3,
            Placement::Local,
        ),
        (
            "a source whose rows no key places",
            |c: &mut Circuit| {
                scan(c, 7);
            },
            [clustered.placed(Placement::Local); 2],
            3,
            Placement::Local,
        ),
    ];
    for (why, build, [a, b], pk_arity, want) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let (_, placement) = ViewMeta::derive(&loaded(c), &sources([(7, a), (9, b)]), &view(pk_arity)).unwrap();
        assert_eq!(placement, want, "{why}");
    }
}

// ── Backfill bounds ─────────────────────────────────────────────────────

/// A backfill bound survives per source scanned once; a source scanned twice
/// shares one backfill cursor, so it keeps none even where one scan is bounded.
#[test]
fn a_source_scanned_once_keeps_its_backfill_bound() {
    let bound = |col: u32| ReadBound::Range(KeyRange::point(PkColList::from_slice(&[col]), &[], 0));
    let bounds_of = |scans: [(u64, ReadBound); 2]| {
        let mut c = Circuit::default();
        let [a, b] = scans.clone().map(|(source, bound)| c.input_delta(source, bound));
        c.union(a, b);
        let meta = derive(c, &sources([(10, wide_schema()), (11, wide_schema())])).unwrap();
        scans.map(|(source, _)| meta.source_bound(source))
    };
    assert_eq!(
        bounds_of([(10, bound(2)), (11, ReadBound::None)]),
        [bound(2), ReadBound::None],
        "each source scanned once keeps its own"
    );
    assert_eq!(
        bounds_of([(10, bound(2)), (10, ReadBound::None)]),
        [ReadBound::None, ReadBound::None],
        "a source scanned twice has none"
    );
}
