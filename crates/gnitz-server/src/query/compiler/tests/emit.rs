use super::*;
use crate::query::compiler::fixtures::*;
use crate::query::vm::Vm;
use crate::test_support::{make_schema_u64_i64, map_copy, reindexed_on_col1, scan_keyed, self_typed_slots};
use gnitz_store::relation::RelationKind::{self, BaseTable, Stream};
use gnitz_wire::{Circuit, JoinKind, ReindexSlot, TypeCode};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::schema::SchemaColumn;

/// `build` compiled whole over `registry`.
fn plan(registry: &RelationRegistry, build: impl FnOnce(&mut Circuit)) -> Result<Vm, String> {
    let mut c = Circuit::default();
    build(&mut c);
    compile(&c, registry, false).map(|out| out.vm)
}

/// A join's two terms each read an integral as it stood before the epoch, so a
/// relation feeding a term's delta and its integral would lose their product in
/// every epoch of it — whether through two scans, through one delta computed
/// from the other, or through an integrand the other delta does not feed.
/// A `Filter` program that does not decode aborts the compile: passing every row
/// instead would turn the WHERE into WHERE TRUE.
#[test]
fn a_join_one_relation_feeds_twice_and_a_corrupt_filter_are_rejected() {
    use TypeCode::I64;
    let schema = make_schema_u64_i64();
    let registry = sources([(10, schema), (11, schema)]);
    let cases: [(Build, &str); 5] = [
        (
            |c| {
                let deltas = [10, 10].map(|s| reindexed_on_col1(c, s, I64));
                c.join(deltas, deltas, JoinKind::Equi);
            },
            "join: one relation feeds a delta and the integral it probes",
        ),
        (
            |c| {
                let da = reindexed_on_col1(c, 10, I64);
                let dm = c.negate(da);
                c.join([da, dm], [da, dm], JoinKind::Equi);
            },
            "join: one relation feeds a delta and the integral it probes",
        ),
        (
            |c| {
                let [da, db, again] = [10, 11, 10].map(|s| reindexed_on_col1(c, s, I64));
                let ib = c.union(db, again);
                c.join([da, db], [da, ib], JoinKind::Equi);
            },
            "join: one relation feeds a delta and the integral it probes",
        ),
        (
            |c| {
                let a = scan(c, 10);
                c.filter(a, vec![0xFF; 16]);
            },
            "filter: invalid predicate program",
        ),
        (
            |c| {
                let a = scan(c, 10);
                c.filter(a, Vec::new());
            },
            "filter: invalid predicate program",
        ),
    ];
    for (build, guard) in cases {
        let refused = rejection(plan(&registry, build));
        assert!(refused.starts_with(guard), "{guard}: got {refused}");
    }
    let two = plan(&registry, |c| {
        let deltas = [10, 11].map(|s| reindexed_on_col1(c, s, I64));
        c.join(deltas, deltas, JoinKind::Equi);
    });
    assert!(two.is_ok(), "two relations, one delta each");
}

// ── An integral that is its source's own store ────────────────────────────

/// `(a, b | v)` keyed `(a, b)`, `a` of type `a` and the rest U64.
fn ab(a: TypeCode) -> SchemaDescriptor {
    let col = SchemaColumn::new(TypeCode::U64, false);
    SchemaDescriptor::new(&[SchemaColumn::new(a, false), col, col], &[0, 1])
}

/// `10 ⋈ 11` on `10.a = 11.k`, 11 being `(k | w)`: 10 re-keyed onto `key`, a
/// `distinct` in between where `distinct`, each side stating as its route the
/// slots of its key a join of `kind` meets on.
fn rekeyed_join(c: &mut Circuit, key: &[ReindexSlot], distinct: bool, kind: JoinKind) {
    use gnitz_wire::{NullKeys, ReadBound};
    // Both sides pack at the one type the pair shares.
    let b_key = [(0, key[0].1)];
    // A range join's sides meet on all but its last key slot.
    let routed = |key: &[ReindexSlot]| match kind {
        JoinKind::Range { .. } => key[..key.len() - 1].to_vec(),
        _ => key.to_vec(),
    };
    let mut a = c.input_scattered(10, ReadBound::None, Some(routed(key)));
    let b = c.input_scattered(11, ReadBound::None, Some(routed(&b_key)));
    if distinct {
        a = c.distinct(a);
    }
    let ra = c.map_reindex(a, key, &[1, 2], NullKeys::Drop);
    let rb = c.map_reindex(b, &b_key, &[1], NullKeys::Drop);
    c.join([ra, rb], [ra, rb], kind);
}

const A: [ReindexSlot; 1] = [(0, TypeCode::U64)];
const ON_A: Build = |c| rekeyed_join(c, &A, false, JoinKind::Equi);

/// An integral is read off its source table exactly where the table's store
/// holds, on this worker and in key order, every row the integral would.
#[test]
fn an_integral_is_its_source_table_only_where_the_table_holds_what_it_would() {
    use TypeCode::{U32, U64, U128, UUID};
    let table = Source::from(ab(U64));
    let partner = Source::from(make_schema_u64_i64());
    // Beside other workers, the table's delta must also stay where the table
    // holds it.
    let third = Slot::new(2, 4);
    let cases: [(&str, Slot, RelationKind, Source, Source, Build, bool); 11] = [
        ("a PK prefix", Slot::SOLO, BaseTable, table, partner, ON_A, true),
        (
            "a key that is no PK prefix",
            Slot::SOLO,
            BaseTable,
            table,
            partner,
            |c| rekeyed_join(c, &[(1, U64)], false, JoinKind::Equi),
            false,
        ),
        (
            "an operator between the scan and the re-key",
            Slot::SOLO,
            BaseTable,
            table,
            partner,
            |c| rekeyed_join(c, &A, true, JoinKind::Equi),
            false,
        ),
        (
            "a join that walks a range of keys",
            Slot::SOLO,
            BaseTable,
            table,
            partner,
            |c| rekeyed_join(c, &A, false, JoinKind::Range { rel: gnitz_wire::RangeRel::Lt }),
            false,
        ),
        (
            "a key packed wider than its column",
            Slot::SOLO,
            BaseTable,
            ab(U32).into(),
            partner,
            ON_A,
            false,
        ),
        (
            "a key packed at another type of the same bytes",
            Slot::SOLO,
            BaseTable,
            ab(UUID).into(),
            partner,
            |c| rekeyed_join(c, &[(0, U128)], false, JoinKind::Equi),
            true,
        ),
        (
            "a stream holds no rows",
            Slot::SOLO,
            Stream,
            table,
            partner,
            ON_A,
            false,
        ),
        (
            "placed by the key",
            third,
            BaseTable,
            table.placed(Placement::keyed(&table.schema, 1)),
            partner,
            ON_A,
            true,
        ),
        (
            "replicated",
            third,
            BaseTable,
            table.placed(Placement::Replicated),
            partner,
            ON_A,
            true,
        ),
        (
            "placed by the whole PK, relayed by the key",
            third,
            BaseTable,
            table,
            partner,
            ON_A,
            false,
        ),
        // A replicated partner makes every match where the row already is, so the
        // delta takes no relay.
        (
            "placed by the whole PK beside a replicated partner",
            third,
            BaseTable,
            table,
            partner.placed(Placement::Replicated),
            ON_A,
            true,
        ),
    ];
    for (why, slot, kind, wide, partner, build, want) in cases {
        let (registry, _dir) = tables_at(slot, [(10, kind, wide), (11, BaseTable, partner)]);
        let vm = plan(&registry, build).expect(why);
        let reads_10 = vm
            .ops()
            .any(|op| matches!(op, Op::JoinDT { trace: Integral::Relation(10), .. }));
        assert_eq!(reads_10, want, "{why}");
    }
}

// ── One integral per integrand ────────────────────────────────────────────

/// A join names the node whose integral it probes, so the two joins reading the
/// preserved side's re-key — the inner term and the matched term — share one
/// store; and the matched join reads B's key set out of the history of the
/// clamp that makes it, so no store holds that key set twice.
#[test]
fn two_joins_probing_one_integrand_share_one_integral() {
    use TypeCode::U64;
    let (nn, nullable) = (SchemaColumn::new(U64, false), SchemaColumn::new(U64, true));
    let a = SchemaDescriptor::new(&[nn, nn, nullable], &[0]);
    let b = SchemaDescriptor::new(&[nn, nn, nn], &[0]);
    let (c, _) = crate::test_support::left_join_circuit(10, 11, 1);
    let vm = compile(&c, &sources([(10, a), (11, b)]), false).unwrap().vm;
    let (mut probes, mut distinct, mut hists) = (0, Vec::new(), Vec::new());
    for op in vm.ops() {
        match op {
            Op::JoinDT { trace, .. } => {
                probes += 1;
                if !distinct.contains(trace) {
                    distinct.push(*trace);
                }
            }
            Op::WeightClamp { hist: ClampHistory::Own(hist), .. } => hists.push(Integral::Own(*hist)),
            _ => {}
        }
    }
    assert_eq!(probes, 4, "the inner and the matched join's two terms each");
    assert_eq!(distinct.len(), 3, "A's re-key, B's, and B's key set: {distinct:?}");
    assert_eq!(hists.len(), 1, "the clamp making B's key set keeps its history");
    assert!(
        distinct.contains(&hists[0]),
        "the key set is read out of the clamp's history: {distinct:?}"
    );
}

// ── A reduce whose output is the view's ───────────────────────────────────

/// A reduce that is the view's output reads the view's store as its output
/// trace; behind another operator the store holds other rows than the trace would.
#[test]
fn a_view_store_stands_in_for_the_output_trace_of_the_output_node() {
    let cases: [(&str, Stage, bool); 2] = [
        ("the output", |c, a| c.reduce_local(a, count_by(&[1])), true),
        (
            "behind a negate",
            |c, a| {
                let r = c.reduce_local(a, count_by(&[1]));
                c.negate(r)
            },
            false,
        ),
    ];
    let registry = sources([(10, make_schema_u64_i64())]);
    for (why, tip, want) in cases {
        let vm = plan(&registry, |c| {
            let a = scan(c, 10);
            tip(c, a);
        })
        .expect(why);
        assert_eq!(vm.reads_view_store(), want, "{why}");
    }
}

// ── A delta's relay ───────────────────────────────────────────────────────

/// The relays of `build`'s plan over sources 7 and 9, as worker 0 of `of`:
/// `(rounds, shares)`, a source's relay round among the rounds.
fn relays(of: u32, sources: [Source; 2], build: impl FnOnce(&mut Circuit)) -> (usize, usize) {
    let registry = sources_at(Slot::new(0, of), [(7, sources[0]), (9, sources[1])]);
    let mut c = Circuit::default();
    build(&mut c);
    let out = compile(&c, &registry, false).expect("the fixture compiles");
    let relayed = out.source_ids().filter(|&s| out.scan(s).relay.is_some()).count();
    let rounds = out.vm.ops().filter(|op| matches!(op, Op::Round(_))).count();
    let shares = out.vm.ops().filter(|op| matches!(op, Op::Share(_))).count();
    (relayed + rounds, shares)
}

/// A delta stays where its rows already are, is kept by share where every worker
/// holds or computed the same rows, and takes a round otherwise — whether a
/// source's route relays it or an exchange does.
#[test]
fn a_delta_is_relayed_only_where_it_is_not_already_in_place() {
    let keyed = Source::from(make_schema_u64_i64());
    let replicated = keyed.placed(Placement::Replicated);
    let negated: Build = |c| {
        let a = scan(c, 7);
        let n = c.negate(a);
        c.shard(n);
    };
    // `7 ⋈ 9` on a payload column, 7's re-key also read beside the join: read
    // there a replicated delta counts once per worker, so neither source skips.
    let routed: Build = |c| {
        let key = self_typed_slots(&make_schema_u64_i64(), &[1]);
        let (a, b) = (scan_keyed(c, 7, &key), scan_keyed(c, 9, &key));
        c.negate(a);
        c.join([a, b], [a, b], JoinKind::Equi);
    };
    type Case = (&'static str, u32, [Source; 2], Build, (usize, usize));
    let cases: [Case; 10] = [
        (
            "a shard on the key its scan's rows are placed by",
            4,
            [keyed; 2],
            |c| {
                let a = scan(c, 7);
                c.shard(a);
            },
            (0, 0),
        ),
        ("a negate keeps each row on its worker", 4, [keyed; 2], negated, (0, 0)),
        ("one worker", 1, [keyed; 2], negated, (0, 0)),
        (
            "a view over replicated sources alone is computed whole on every worker",
            4,
            [replicated, keyed],
            negated,
            (0, 0),
        ),
        (
            "every worker computed the same rows of a replicated side",
            4,
            [replicated, keyed],
            |c| {
                let [a, b] = [7, 9].map(|s| {
                    let leaf = scan(c, s);
                    let n = c.negate(leaf);
                    c.shard(n)
                });
                c.union(a, b);
            },
            (0, 1),
        ),
        (
            "a replica's share is one worker's rows: an exchange behind it by another key takes a round",
            4,
            [replicated, keyed],
            |c| {
                let b = scan(c, 9);
                c.shard(b);
                let a = scan(c, 7);
                let a = c.negate(a);
                let a = c.shard(a);
                let a = c.negate(a);
                c.reduce(a, count_by(&[1]));
            },
            (1, 1),
        ),
        (
            "a hash-row map over a replicated side keeps it whole",
            4,
            [replicated, keyed],
            |c| {
                scan(c, 9);
                let a = scan(c, 7);
                c.map_hash_row(a, &self_typed_slots(&make_schema_u64_i64(), &[1]));
            },
            (0, 1),
        ),
        (
            "a routed replicated source is shared",
            4,
            [replicated, keyed],
            routed,
            (1, 1),
        ),
        (
            "a routed partitioned source takes a round",
            4,
            [keyed; 2],
            routed,
            (2, 0),
        ),
        (
            // Each source's relay runs, and so does the reduce's exchange.
            "a reduce's exchange behind a scan a join's relay moved off its owner",
            4,
            [keyed; 2],
            |c| {
                let key = self_typed_slots(&make_schema_u64_i64(), &[1]);
                let deltas = [7, 9].map(|source| scan_keyed(c, source, &key));
                c.join(deltas, deltas, JoinKind::Equi);
                // `scan_keyed` pushed 7's scan first.
                c.reduce(0, count_by(&[0]));
            },
            (3, 0),
        ),
    ];
    for (why, of, sources, build, want) in cases {
        assert_eq!(relays(of, sources, build), want, "{why}");
    }
}

/// A reduce's co-location moves nothing exactly where its input's rows already
/// sit on the owner of its group key.
#[test]
fn the_output_exchange_is_skipped_only_where_its_input_sits_by_its_key() {
    let i64 = SchemaColumn::new(TypeCode::I64, false);
    let u64 = SchemaColumn::new(TypeCode::U64, false);
    // PK last, so a group column behind a map — an index into the map's output,
    // whose leading slots are the source PK — is not the source's column index.
    let pk_last = Source::from(SchemaDescriptor::new(&[i64, i64, u64], &[2]));
    let clustered = crate::test_support::pk_payload_schema(&[TypeCode::U32, TypeCode::U64, TypeCode::U64]);
    let clustered = Source::from(clustered).placed(Placement::keyed(&clustered, 1));
    let bare: Stage = |_, n| n;
    let fan_in: Stage = |c, n| {
        let other = scan(c, 9);
        c.union(n, other)
    };
    let cases: [(&str, Source, Stage, &[u32], bool); 14] = [
        ("a bare scan", pk_last, bare, &[2], true),
        ("a payload column", pk_last, bare, &[0], false),
        ("an empty key", pk_last, bare, &[], false),
        ("the CLUSTER BY prefix", clustered, bare, &[0], true),
        ("not the leading PK column", clustered, bare, &[1], false),
        (
            "a filter is transparent",
            pk_last,
            |c, n| c.filter(n, predicate(0)),
            &[2],
            true,
        ),
        (
            "a copy list carries the PK region to its leading slot",
            pk_last,
            |c, n| map_copy(c, n, &[(0, TypeCode::I64), (1, TypeCode::I64)]),
            &[0],
            true,
        ),
        (
            "a payload slot behind a map",
            pk_last,
            |c, n| map_copy(c, n, &[(0, TypeCode::I64), (1, TypeCode::I64)]),
            &[1],
            false,
        ),
        (
            "a keys-only map carries the PK region",
            pk_last,
            |c, n| map_copy(c, n, &[]),
            &[0],
            true,
        ),
        (
            "filters and maps interleave",
            pk_last,
            |c, n| {
                let f = c.filter(n, predicate(0));
                let m = map_copy(c, f, &[(0, TypeCode::I64), (1, TypeCode::I64)]);
                c.filter(m, predicate(1))
            },
            &[0],
            true,
        ),
        (
            "a reindex re-keys the PK",
            pk_last,
            |c, n| {
                let key = [(0, TypeCode::I64)];
                c.map_reindex(n, &key, &[0], gnitz_wire::NullKeys::Keep)
            },
            &[0],
            false,
        ),
        ("a fan-in of two sources placed by the key", pk_last, fan_in, &[2], true),
        (
            "a filter over a fan-in",
            pk_last,
            |c, n| {
                let other = scan(c, 9);
                let u = c.union(n, other);
                c.filter(u, predicate(0))
            },
            &[2],
            true,
        ),
        ("a fan-in on a payload column", pk_last, fan_in, &[0], false),
    ];
    for (why, source, mid, group, skipped) in cases {
        let (rounds, _) = relays(4, [source; 2], |c| {
            let a = scan(c, 7);
            let tip = mid(c, a);
            c.reduce(tip, count_by(group));
        });
        assert_eq!(rounds == 0, skipped, "{why}");
    }
}

// ── A clamp whose history is its sources' stores ──────────────────────────

/// `10 EXCEPT ALL 11` over leaves keyed by their own PK, each behind an exchange
/// on it.
const KEYED_EXCEPT: Build = |c| {
    let [a, b] = [10, 11].map(|s| {
        let leaf = scan(c, s);
        c.shard(leaf)
    });
    c.positive_diff(a, b);
};

/// A clamp declares no history of its own exactly where each term of its input
/// is a base table's rows on the worker that stores them.
#[test]
fn a_clamp_reads_its_sources_where_they_hold_its_history() {
    use crate::query::vm::ClampHistory;
    let table = Source::from(ab(TypeCode::U64));
    let by_prefix = table.placed(Placement::keyed(&table.schema, 1));
    let replicated = table.placed(Placement::Replicated);
    let third = Slot::new(2, 4);
    /// (why, worker, tables 10 and 11, the circuit, how many clamps keep a
    /// history of their own)
    type Case = (&'static str, Slot, [(RelationKind, Source); 2], Build, usize);
    let cases: [Case; 8] = [
        ("one worker", Slot::SOLO, [(BaseTable, table); 2], KEYED_EXCEPT, 0),
        (
            "an exchange that moves nothing",
            third,
            [(BaseTable, table); 2],
            KEYED_EXCEPT,
            0,
        ),
        (
            "a replicated side's share",
            third,
            [(BaseTable, replicated), (BaseTable, table)],
            KEYED_EXCEPT,
            0,
        ),
        (
            "a filter and a projection on the way",
            third,
            [(BaseTable, table); 2],
            |c| {
                let a = scan(c, 10);
                let a = c.filter(a, predicate(2));
                let a = map_copy(c, a, &[(2, TypeCode::U64)]);
                let b = scan(c, 11);
                c.positive_diff(a, b);
            },
            0,
        ),
        (
            "an exchange that moves a side placed by its PK prefix",
            third,
            [(BaseTable, by_prefix), (BaseTable, table)],
            KEYED_EXCEPT,
            1,
        ),
        (
            "a stream holds no rows",
            Slot::SOLO,
            [(Stream, table), (BaseTable, table)],
            KEYED_EXCEPT,
            1,
        ),
        (
            "a re-key onto a content hash",
            Slot::SOLO,
            [(BaseTable, table); 2],
            |c| {
                let [a, b] = [10, 11].map(|s| {
                    let leaf = scan(c, s);
                    c.map_hash_row(leaf, &[(0, TypeCode::U64), (2, TypeCode::U64)])
                });
                c.positive_diff(a, b);
            },
            1,
        ),
        (
            // The outer clamp reads the inner one's output, which no store holds.
            "a chain of clamps",
            Slot::SOLO,
            [(BaseTable, table); 2],
            |c| {
                let [a, b] = [10, 11].map(|s| scan(c, s));
                let inner = c.positive_diff(a, b);
                c.distinct(inner);
            },
            1,
        ),
    ];
    for (why, slot, [(k10, s10), (k11, s11)], build, want) in cases {
        let (registry, _dir) = tables_at(slot, [(10, k10, s10), (11, k11, s11)]);
        let vm = plan(&registry, build).expect(why);
        let own = vm
            .ops()
            .filter(|op| matches!(op, Op::WeightClamp { hist: ClampHistory::Own(_), .. }));
        assert_eq!(own.count(), want, "{why}");
    }
}

/// A scan reads the payload columns its readers can tell from zeroes: a filter
/// and an exchange hand their rows on to readers of their own, a map and a
/// reduce read the columns they name, and every other reader — the view's store
/// among them — reads them all.
#[test]
fn a_scan_reads_the_columns_its_readers_name() {
    use TypeCode::I64;
    // PK column 0, then payload columns 1..=5 at payload bits 0..=4.
    let schema = crate::test_support::u64_pk_schema(&[SchemaColumn::new(I64, false); 5]);
    let cases: [(&str, Build, u64); 9] = [
        ("the output", |c| _ = scan(c, 10), 0b11111),
        (
            "a map",
            |c| {
                let a = scan(c, 10);
                map_copy(c, a, &[(2, I64)]);
            },
            0b00010,
        ),
        (
            "a filter hands its rows on",
            |c| {
                let a = scan(c, 10);
                let kept = c.filter(a, predicate(3));
                map_copy(c, kept, &[(2, I64)]);
            },
            0b00110,
        ),
        (
            "a filter the view stores",
            |c| {
                let a = scan(c, 10);
                c.filter(a, predicate(3));
            },
            0b11111,
        ),
        (
            "an exchange hands its rows on",
            |c| {
                let a = scan(c, 10);
                let placed = c.shard(a);
                map_copy(c, placed, &[(1, I64)]);
            },
            0b00001,
        ),
        (
            "a reduce's group columns",
            |c| {
                let a = scan(c, 10);
                c.reduce(a, count_by(&[4]));
            },
            0b01000,
        ),
        (
            "a top-N keeps its rows",
            |c| {
                let a = scan(c, 10);
                let by = gnitz_wire::OrderKey { col: 1, desc: false, nulls_first: false };
                c.top_n(a, &[4], &[by], 3, 0);
            },
            0b11111,
        ),
        (
            "two readers",
            |c| {
                let a = scan(c, 10);
                let x = map_copy(c, a, &[(1, I64)]);
                let y = map_copy(c, a, &[(5, I64)]);
                c.union(x, y);
            },
            0b10001,
        ),
        (
            "two scans of one source",
            |c| {
                let x = scan(c, 10);
                let x = map_copy(c, x, &[(1, I64)]);
                let y = scan(c, 10);
                let y = map_copy(c, y, &[(2, I64)]);
                c.union(x, y);
            },
            0b00011,
        ),
    ];
    for (why, build, read) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let out = compile(&c, &sources_at(Slot::new(0, 4), [(10, schema)]), false).expect(why);
        assert_eq!(out.scan(10).read, read, "{why}: {:#07b}", out.scan(10).read);
    }
}
