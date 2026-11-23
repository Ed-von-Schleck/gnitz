use super::*;
use crate::relation::{IndexClaim, Landing, PkRule, RelationKind};
use crate::test_support::{
    RelationFixture, TID, batch_rows, col_range, img, make_batch, make_schema_u64_i64, pk_only_schema,
    relation_fixture, zset_of,
};
use gnitz_foundation::rng::Rng;
use gnitz_trace::runs::Run;
use gnitz_wire::{PkColList, ReadSpec, TypeCode, ViewBudget, image_mask, opk_bias};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor, pk_keys_of_images};

const NBASE: u64 = 200;

/// A `kind` relation `(id U64 PK | val I64)` of `NBASE` rows at `val = val_of(id)`,
/// indexed on `val`.
fn fixture(kind: RelationKind, val_of: impl Fn(u64) -> i64) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..NBASE).map(|id| (id, 1, val_of(id))).collect();
    relation_fixture(kind, schema, &[&[1]], [make_batch(&schema, &rows)])
}

fn x10(id: u64) -> i64 {
    id as i64 * 10
}

/// The bound `val ∈ [lo, hi)`.
fn val_range(lo: i64, hi: i64) -> ReadBound {
    col_range(1, img(lo), img(hi - 1))
}

/// Drain a cursor to its `(id, weight, val)` rows, `chunk` rows at a time so a chunk
/// boundary lands mid-range, checking each chunk is non-empty and PK-ascending.
fn drain_all(cur: &mut SourceCursor, chunk: usize) -> Vec<(u64, i64, i64)> {
    let mut out = Vec::new();
    while let Some(b) = cur.drain_chunk(chunk) {
        assert!(!b.is_empty(), "drain_chunk yielded an empty chunk");
        for i in 1..b.len() {
            assert!(
                gnitz_wire::widen_pk_be(b.get_pk_bytes(i - 1)) < gnitz_wire::widen_pk_be(b.get_pk_bytes(i)),
                "chunk {chunk}: rows out of PK order"
            );
        }
        out.extend(batch_rows(&b));
    }
    out.sort_unstable();
    out
}

/// `ids` at weight 1 and `val = id * 10`.
fn rows(ids: impl IntoIterator<Item = u64>) -> Vec<(u64, i64, i64)> {
    ids.into_iter().map(|id| (id, 1, x10(id))).collect()
}

/// A range walks the index only while its entries are at most 1/16 of the base's rows
/// — 12 of 200 do, 13 do not — and yields exactly its rows at every chunk size; past
/// the gate, and with no index at all, the source is a full scan and the range comes
/// back unapplied, for the caller's filter. A range holding no entry opens no walk.
/// A read at now, which probes the base table for each entry's row, yields what the
/// sealed read's merge gather does.
#[test]
fn a_range_walks_the_index_only_within_the_selectivity_gate() {
    // With `val` anti-correlated to the PK, a range at val's minimum makes the first
    // chunk end on the base's last PK group — exhausting the base cursor — and every
    // later chunk's first probe sorts below it: a backward re-seek.
    let anti = |id: u64| (NBASE - id) as i64 * 10;
    let point = ReadBound::Range(KeyRange::point(PkColList::from_slice(&[1]), &[img(730)]));
    let inverted = col_range(1, img(901), img(299));
    // (val_of, indexed, bound, walks the index, ids).
    let x10: fn(u64) -> i64 = x10;
    let cases = [
        (x10, true, val_range(500, 600), true, (50..60).collect()),
        (x10, true, val_range(500, 620), true, (50..62).collect()),
        (x10, true, point.clone(), true, vec![73]),
        (x10, true, inverted, true, vec![]),
        (anti, true, val_range(10, 110), true, (190..200).collect()),
        (x10, true, val_range(500, 630), false, (0..NBASE).collect()),
        (x10, true, val_range(0, i64::MAX), false, (0..NBASE).collect()),
        (x10, false, point, false, (0..NBASE).collect()),
    ];
    for (val_of, indexed, bound, walks, ids) in cases {
        let mut r = fixture(RelationKind::BaseTable, val_of);
        if !indexed {
            r.release_index(TID, IndexClaim::Index { id: TID + 1, unique: false });
        }
        let want: Vec<_> = ids.into_iter().map(|id| (id, 1, val_of(id))).collect();
        for chunk in [1, 3, 7, 64, usize::MAX] {
            let relation = r.relation(TID).unwrap();
            let (read, unapplied) = relation.open_read(bound.clone(), u64::MAX).unwrap();
            assert_eq!(unapplied.map(ReadBound::Range), (!walks).then(|| bound.clone()));
            for mut cur in [relation.open_sealed(bound.clone(), u64::MAX).unwrap(), read] {
                assert_eq!(cur.walks_index(), walks && !want.is_empty(), "{bound:?}");
                assert_eq!(drain_all(&mut cur, chunk), want, "{bound:?} chunk {chunk}");
            }
        }
    }
}

/// A key list over a store whose keys net 0 or 1 — a base table's or a system
/// family's — is probed key by key at now; a view's is gathered off a cursor. Either
/// way it yields what the sealed read's merge gather does, a retracted key included.
#[test]
fn a_key_list_is_probed_only_where_every_key_nets_0_or_1() {
    let schema = make_schema_u64_i64();
    let base = make_batch(&schema, &rows(0..NBASE));
    let keys = || ReadBound::PkSet(pk_keys_of_images(8, [2, 5, 7, 300]));
    for (kind, probed) in [
        (RelationKind::BaseTable, true),
        (RelationKind::SystemCatalog, true),
        (RelationKind::View(ViewBudget::Plain), false),
    ] {
        let mut r = relation_fixture(kind, schema, &[], [base.clone()]);
        r.ingest(TID, make_batch(&schema, &[(5, -1, 50)])).unwrap();
        let relation = r.relation(TID).unwrap();
        let (mut read, unapplied) = relation.open_read(keys(), u64::MAX).unwrap();
        assert_eq!(unapplied, None);
        assert!(read.walk.is_none(), "{kind:?}: a key list is a listed gather");
        assert_eq!(matches!(read.source, Source::Unique(_)), probed, "{kind:?}");
        let mut sealed = relation.open_sealed(keys(), u64::MAX).unwrap();
        assert_eq!(drain_all(&mut read, 2), rows([2, 7]), "{kind:?}");
        assert_eq!(drain_all(&mut sealed, 2), rows([2, 7]), "{kind:?}");
    }
}

/// A backfill's read: an index holds a pending row's entry from its ingest on,
/// so at the sealed cut it is walked only while nothing is pending. With rows
/// above the cut the whole sealed store is read.
#[test]
fn a_sealed_read_walks_an_index_only_over_a_store_whose_cuts_agree() {
    let mut r = fixture(RelationKind::BaseTable, x10);

    let mut cur = r
        .relation(TID)
        .unwrap()
        .open_sealed(val_range(500, 600), u64::MAX)
        .unwrap();
    assert!(cur.walks_index(), "nothing pending: the index");
    assert_eq!(drain_all(&mut cur, 7), rows(50..60));

    // One pending row inside the range, one outside it.
    let pending = make_batch(&make_schema_u64_i64(), &[(NBASE, 1, 555), (NBASE + 1, 1, 9_000)]);
    r.land(TID, pending, Landing::Pending, PkRule::Enforce).unwrap();
    let with_pending = || {
        let mut rows = rows(50..60);
        rows.push((NBASE, 1, 555));
        rows
    };

    let mut cur = r
        .relation(TID)
        .unwrap()
        .open_sealed(val_range(500, 600), u64::MAX)
        .unwrap();
    assert!(!cur.walks_index(), "rows pending: the sealed store");
    assert_eq!(drain_all(&mut cur, 7), rows(0..NBASE), "the pending rows are left out");

    let (mut cur, _) = r
        .relation(TID)
        .unwrap()
        .open_read(val_range(500, 600), u64::MAX)
        .unwrap();
    assert!(cur.walks_index(), "a read at now walks the index");
    assert_eq!(drain_all(&mut cur, 7), with_pending());

    r.seal(TID).unwrap().expect("rows were pending");
    let mut cur = r
        .relation(TID)
        .unwrap()
        .open_sealed(val_range(500, 600), u64::MAX)
        .unwrap();
    assert!(cur.walks_index(), "sealed: the cuts agree again");
    assert_eq!(drain_all(&mut cur, 7), with_pending());
}

/// The walk sees the index its relation's writes left: a retracted row is gone, a row
/// updated into the range appears, one updated within it appears once, and one updated
/// out of it is gone — over a base table, and over a view, whose index is fed its
/// output deltas and whose rows keep their own weights through the walk.
#[test]
fn a_walk_follows_the_relations_writes() {
    let base = [
        (55, -1, 550),
        (10, -1, 100),
        (10, 1, 555),
        (51, -1, 510),
        (51, 1, 530),
        (52, -1, 520),
        (52, 1, 5200),
    ];
    // 51 leaves the range, 90 enters it at weight 2.
    let view = [(51, -1, 510), (51, 1, 9000), (90, -1, 900), (90, 2, 555)];
    let cases = [
        (
            RelationKind::BaseTable,
            &base[..],
            vec![(10, 1, 555), (50, 1, 500), (51, 1, 530), (53, 1, 530), (54, 1, 540)],
        ),
        (
            RelationKind::View(ViewBudget::Plain),
            &view[..],
            [rows([50, 52, 53, 54, 55]), vec![(90, 2, 555)]].concat(),
        ),
    ];
    for (kind, delta, want) in cases {
        let mut r = fixture(kind, x10);
        r.ingest(TID, make_batch(&make_schema_u64_i64(), delta)).unwrap();
        let (mut cur, unapplied) = r
            .relation(TID)
            .unwrap()
            .open_read(val_range(500, 560), u64::MAX)
            .unwrap();
        assert!(cur.walks_index(), "{kind:?}");
        assert_eq!(unapplied, None);
        assert_eq!(drain_all(&mut cur, 4), want, "{kind:?}");
    }
}

/// A walk over a composite index — an equality prefix, then a range on the next
/// column, cut at the edges of every integer width, over a compound PK up to 24 bytes
/// wide whose order is not the index's, with NULLs, retracted rows and updated ones —
/// reads exactly the rows its bounds admit, whether it walks the index or scans under
/// the range.
#[test]
fn a_composite_range_reads_exactly_the_rows_its_bounds_admit() {
    use TypeCode::*;
    const TYPES: [TypeCode; 10] = [U8, I8, U16, I16, U32, I32, U64, I64, U128, I128];
    const ROWS: u64 = 320;
    let (mut walked, mut scanned) = (0, 0);
    for seed in 0..40 {
        let mut rng = Rng::new(seed);
        let pick = |rng: &mut Rng, n: usize| rng.gen_range(n as u64) as usize;
        // PK `(k.., id)`: up to two leading I64 columns, then the row's id; an index
        // entry's key, its columns then the PK's, is at most four columns.
        let n_pk = 1 + pick(&mut rng, 3);
        let n_ix = 1 + pick(&mut rng, 4 - n_pk);
        let types: Vec<TypeCode> = (0..n_ix).map(|_| TYPES[pick(&mut rng, TYPES.len())]).collect();
        let mut cols = vec![SchemaColumn::new(I64, false); n_pk - 1];
        cols.push(SchemaColumn::new(U64, false));
        cols.extend(types.iter().map(|&tc| SchemaColumn::new(tc, rng.gen_range(2) == 0)));
        let pk: Vec<u32> = (0..n_pk as u32).collect();
        let schema = SchemaDescriptor::new(&cols, &pk);
        let ix_cols: Vec<u32> = (n_pk as u32..(n_pk + n_ix) as u32).collect();

        // A row is its id and each indexed column's key image, `None` for NULL.
        type Row = (u64, Vec<Option<u128>>);
        let image = |rng: &mut Rng, ci: usize| -> Option<u128> {
            let (tc, mask) = (types[ci], image_mask(types[ci].wire_stride()));
            let bias = opk_bias(tc);
            let edges = [0, 1, bias.wrapping_sub(1) & mask, bias, bias + 1, mask - 1, mask];
            if cols[n_pk + ci].nullable && rng.gen_range(8) == 0 {
                return None;
            }
            Some(match rng.gen_range(3) {
                0 => edges[pick(rng, edges.len())],
                _ => ((rng.next_u64() as u128) << 64 | rng.next_u64() as u128) & mask,
            })
        };
        let batch = |rows: &[(Row, i64)]| {
            let mut bb = BatchBuilder::new(&schema);
            for ((id, imgs), w) in rows {
                let mut natives = vec![((*id % 3) as i64 - 1) as u64 as u128; n_pk - 1];
                natives.push(*id as u128);
                bb.begin_row_natives(&natives, *w);
                for (&tc, img) in types.iter().zip(imgs) {
                    bb.put_opt_int(img.map(|i| i ^ opk_bias(tc)));
                }
                bb.end_row();
            }
            bb.finish()
        };

        let mut live: Vec<Row> = (0..ROWS)
            .map(|id| (id, (0..n_ix).map(|ci| image(&mut rng, ci)).collect()))
            .collect();
        let first: Vec<_> = live.iter().map(|row| (row.clone(), 1)).collect();
        // Retract one row in six, and move another one in six to new values.
        let mut second = Vec::new();
        live.retain_mut(|row| match rng.gen_range(6) {
            0 => {
                second.push((row.clone(), -1));
                false
            }
            1 => {
                second.push((row.clone(), -1));
                row.1 = (0..n_ix).map(|ci| image(&mut rng, ci)).collect();
                second.push((row.clone(), 1));
                true
            }
            _ => true,
        });
        let r = relation_fixture(
            RelationKind::BaseTable,
            schema,
            &[&ix_cols],
            [batch(&first), batch(&second)],
        );

        // The rows the index holds: a query is drawn around one.
        let indexed: Vec<&Row> = live
            .iter()
            .filter(|(_, imgs)| imgs.iter().all(Option::is_some))
            .collect();
        for _ in 0..40 {
            let n_eq = pick(&mut rng, n_ix);
            let (_, at) = indexed[pick(&mut rng, indexed.len())];
            let eq: Vec<u128> = (0..n_eq)
                .map(|ci| at[ci].or_else(|| image(&mut rng, ci)).unwrap_or(0))
                .collect();
            let bound = |rng: &mut Rng| {
                let (_, other) = &live[pick(rng, live.len())];
                let img = match rng.gen_range(2) {
                    0 => at[n_eq].or(other[n_eq]),
                    _ => image(rng, n_eq),
                };
                img.unwrap_or(0)
            };
            let (first, last) = match rng.gen_range(4) {
                0 => {
                    let at = bound(&mut rng);
                    (at, at)
                }
                // One in two inverted.
                1 => (bound(&mut rng), bound(&mut rng)),
                _ => {
                    let (a, b) = (bound(&mut rng), bound(&mut rng));
                    (a.min(b), a.max(b))
                }
            };
            let range = KeyRange::new(PkColList::from_slice(&ix_cols), &eq, first, last);
            let admitted: Vec<_> = live
                .iter()
                .filter(|(_, imgs)| {
                    let Some(imgs) = imgs.iter().copied().collect::<Option<Vec<u128>>>() else {
                        return false;
                    };
                    imgs[..n_eq] == eq[..] && first <= imgs[n_eq] && imgs[n_eq] <= last
                })
                .map(|row| (row.clone(), 1))
                .collect();

            let bound = ReadBound::Range(range);
            match r
                .relation(TID)
                .unwrap()
                .open_read(bound.clone(), u64::MAX)
                .unwrap()
                .0
                .walks_index()
            {
                true => walked += usize::from(!admitted.is_empty()),
                false => scanned += 1,
            }
            let got = r.scan_spec(TID, ReadSpec::all_rows(bound), schema.layout_digest(), None);
            assert_eq!(
                zset_of(&got.unwrap(), &schema),
                zset_of(&batch(&admitted), &schema),
                "seed {seed} {types:?} {range:?}"
            );
        }
    }
    assert!(
        walked > 400 && scanned > 400,
        "{walked} walks with hits, {scanned} scans"
    );
}

/// The walk gathers every row under an in-range entry's source PK, so an owner
/// holding two rows under one PK trips it.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "two rows under one PK")]
fn an_index_walk_over_a_repeating_key_trips_the_gather() {
    let schema = make_schema_u64_i64();
    // PK 1 twice, one of its values in range; the rest keep the walk under the gate.
    let rows: Vec<_> = [(1, 1, 5), (1, 1, 500)]
        .into_iter()
        .chain((2..64).map(|pk| (pk, 1, 1000)))
        .collect();
    let view = RelationKind::View(ViewBudget::Plain);
    let r = relation_fixture(view, schema, &[&[1]], [make_batch(&schema, &rows)]);
    let mut cur = r.relation(TID).unwrap().open_sealed(val_range(5, 6), u64::MAX).unwrap();
    assert!(cur.walks_index());
    cur.drain_chunk(1024);
}

/// A range over a PK prefix walks the table's own store even when an index names the
/// same columns: `INDEX(a)` under `PRIMARY KEY (a, b)` is never walked for `a`.
#[test]
fn a_pk_prefix_range_walks_the_store_over_a_matching_index() {
    let schema = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let mut bb = BatchBuilder::new(&schema);
    for a in 0..8u128 {
        for b in 0..3u128 {
            bb.begin_row_natives(&[a, b], 1);
            bb.end_row();
        }
    }
    let r = relation_fixture(RelationKind::BaseTable, schema, &[&[0]], [bb.finish()]);

    let range = KeyRange::point(PkColList::from_slice(&[0]), &[5]);
    let (mut cur, unapplied) = r
        .relation(TID)
        .unwrap()
        .open_read(ReadBound::Range(range), u64::MAX)
        .unwrap();
    assert!(!cur.walks_index(), "a PK walk, not the index");
    assert_eq!(unapplied, None, "a PK walk applies the whole range");
    let b = cur.drain_chunk(64).unwrap();
    assert!(cur.drain_chunk(64).is_none());
    assert_eq!(keyed(&b), (0..3).map(|b| (5 << 64 | b, 1)).collect::<Vec<_>>());
}

/// A reply's `(key, weight)` rows, in reply order.
fn keyed(reply: &Batch) -> Vec<(u128, i64)> {
    (0..reply.len())
        .map(|i| (gnitz_wire::widen_pk_be(reply.get_pk_bytes(i)), reply.get_weight(i)))
        .collect()
}

/// A PK probe echoes, and a column probe answers with that column, each named key a
/// live row carries — not one retracted, not one never held.
#[test]
fn a_pk_probe_answers_each_key_a_live_row_carries() {
    let mut r = fixture(RelationKind::BaseTable, x10);
    r.ingest(TID, make_batch(&make_schema_u64_i64(), &[(5, -1, 50)]))
        .unwrap();
    let keys = pk_keys_of_images(8, [2, 5, 7, 300]);
    assert_eq!(keyed(&r.probe(TID, Probe::Pk, keys.clone()).unwrap()), [(2, 1), (7, 1)]);
    let column = r.probe(TID, Probe::PkColumn(1), keys).unwrap();
    assert_eq!(batch_rows(&column), rows([2, 7]));
}

/// A PK or column probe reads a store that holds one live row per key, so a
/// view's is refused.
#[test]
fn a_pk_or_column_probe_of_a_view_is_refused() {
    let r = fixture(RelationKind::View(ViewBudget::Plain), x10);
    for probe in [Probe::Pk, Probe::PkColumn(1)] {
        let err = r.probe(TID, probe, pk_keys_of_images(8, [2])).map(drop).unwrap_err();
        assert!(err.contains("may net above 1"), "{probe:?}: {err}");
    }
}

/// An index probe answers a held span with every stored entry, up to a cap counted
/// across the keys, and a span no row holds with nothing. Its one cursor answers each
/// key as a seek of its own does: a run of misses ahead of a hit, a hit ahead of its
/// neighbour, and a miss past the last entry.
#[test]
fn an_index_probe_answers_each_held_span_up_to_its_cap() {
    // Ids 2k and 2k + 1 share `val = 10k`.
    let val_of = |id: u64| (id / 2) as i64 * 10;
    let r = fixture(RelationKind::BaseTable, val_of);
    let probe = |cap: u64, vals: &[i64]| {
        let index = Probe::Index(PkColList::from_slice(&[1]), std::num::NonZeroU64::new(cap).unwrap());
        let spans = pk_keys_of_images(8, vals.iter().map(|&v| img(v)));
        keyed(&r.probe(TID, index, spans).unwrap())
    };
    // An entry is its span, then its holder's id.
    let entries =
        |ids: std::ops::Range<u64>| -> Vec<_> { ids.map(|id| (img(val_of(id)) << 64 | id as u128, 1)).collect() };
    for (cap, vals, held) in [
        (1, &[20, 25][..], 4..5),
        (2, &[20, 25], 4..6),
        (8, &[20, 25], 4..6),
        // The cap is the reply's, not a key's: it is reached inside the second span.
        (3, &[10, 15, 20, 30], 2..5),
        (7, &[10, 15, 20, 30], 2..8),
    ] {
        assert_eq!(probe(cap, vals), entries(held), "cap {cap} {vals:?}");
    }
    let vals = [-5, 1, 2, 3, 10, 20, 21, 30, 5000];
    let alone: Vec<_> = vals.iter().flat_map(|&v| probe(64, &[v])).collect();
    assert_eq!(probe(64, &vals), alone);
    assert_eq!(alone, entries(2..8));
}

/// A key list at a stride other than the store it reads, an index the relation has not
/// got, and an id that names no relation are each refused.
#[test]
fn a_read_of_what_the_relation_has_not_got_is_refused() {
    let r = fixture(RelationKind::BaseTable, x10);
    let index = |cols: &[u32]| Probe::Index(PkColList::from_slice(cols), std::num::NonZeroU64::MIN);
    let span = || pk_keys_of_images(8, [img(2)]);
    // A whole entry is a span and a source PK: wider than either store's key.
    let entry = || PkKeys::from_sorted(16, Vec::new());
    let refusals = [
        (
            r.relation(TID)
                .unwrap()
                .open_read(ReadBound::PkSet(entry()), u64::MAX)
                .map(drop),
            "stride",
        ),
        (r.probe(999_999, index(&[1]), span()).map(drop), "not registered"),
        (r.probe(TID, index(&[0, 1]), span()).map(drop), "no index"),
        (r.probe(TID, index(&[1]), entry()).map(drop), "stride"),
        (r.probe(TID, Probe::Pk, entry()).map(drop), "stride"),
        (r.probe(TID, Probe::PkColumn(1), entry()).map(drop), "stride"),
    ];
    for (i, (got, why)) in refusals.into_iter().enumerate() {
        let err = got.expect_err(why);
        assert!(err.contains(why), "refusal {i}: {err}");
    }
}

/// A stream holds no rows, so a PK or column probe of one, or a source opened
/// over it at either cut, is refused.
#[test]
fn a_read_of_a_stream_is_refused() {
    let r = relation_fixture(RelationKind::Stream, make_schema_u64_i64(), &[], []);
    for probe in [Probe::Pk, Probe::PkColumn(1)] {
        let err = r.probe(TID, probe, pk_keys_of_images(8, [2])).map(drop).unwrap_err();
        assert!(err.contains("is a stream"), "{probe:?}: {err}");
    }
    let relation = r.relation(TID).unwrap();
    for err in [
        relation.open_read(ReadBound::None, u64::MAX).map(drop).unwrap_err(),
        relation.open_sealed(ReadBound::None, u64::MAX).map(drop).unwrap_err(),
    ] {
        assert!(err.contains("is a stream"), "{err}");
    }
}

/// A drain that skips payload columns emits the rows and weights of one that
/// does not, every kept cell as it, and each skipped cell as a zero that is
/// not NULL: through the walk of an index range gathering off the merge of the
/// store's runs, and through a key list and an index range probed key by key
/// from a base table's store.
#[test]
fn a_drain_writes_skipped_payload_columns_as_zeroes() {
    use gnitz_wire::{null_word_get, payload_bytes, payload_u64};
    const N: u64 = 800;
    // `(id U64 PK | a I64 NULL | s STRING | v I64)`, indexed on `v`.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    // Generation `generation` of key `pk`, at `v = pk * 10 + generation`.
    let run = |rows: &mut dyn Iterator<Item = (u64, u64, i64)>| {
        let mut bb = BatchBuilder::new(&schema);
        for (pk, generation, w) in rows {
            bb.begin_row(pk as u128, w);
            bb.put_opt_int((pk % 3 != 1).then_some((pk * 10 + generation) as u128));
            bb.put_string(&format!("{pk:0>24}/{generation}"));
            bb.put_int((pk * 10 + generation) as u128);
            bb.end_row();
        }
        bb.finish()
    };
    // Every key at generation 0; a later round rewrites every third key, and a
    // third deletes every tenth: the first two flushed, the third in memory.
    let latest = |pk: u64| u64::from(pk.is_multiple_of(3));
    let rounds = [
        run(&mut (0..N).map(|pk| (pk, 0, 1))),
        run(&mut (0..N).step_by(3).flat_map(|pk| [(pk, 0, -1), (pk, 1, 1)])),
        run(&mut (0..N).step_by(10).map(|pk| (pk, latest(pk), -1))),
    ];
    let mut r = relation_fixture(RelationKind::BaseTable, schema, &[&[3]], []);
    let [first, second, third] = rounds;
    for rows in [first, second] {
        r.ingest(TID, rows).unwrap();
        r.checkpoint_base().unwrap();
    }
    r.ingest(TID, third).unwrap();
    let relation = r.relation(TID).unwrap();
    let kinds: Vec<bool> = (relation.table_or_err().unwrap().runs())
        .map(|run| matches!(run, Run::Shard(_)))
        .collect();
    assert!(
        kinds.contains(&true) && kinds.contains(&false),
        "shards and an in-memory run"
    );
    let range = || col_range(3, img(1000), img(1399));
    // Every other key, the deleted ones among them.
    let keys = || ReadBound::PkSet(pk_keys_of_images(8, (0..N).step_by(2).map(u128::from)));
    let open = |label, read| match label {
        "index range" => relation.open_sealed(range(), read).unwrap(),
        "unique pk set" => relation.open_read(keys(), read).unwrap().0,
        _ => relation.open_read(range(), read).unwrap().0,
    };
    for label in ["index range", "unique pk set", "unique index range"] {
        let shape = match open(label, u64::MAX) {
            SourceCursor { source: Source::Merge(_), walk: Some(_) } => "index range",
            SourceCursor { source: Source::Unique(_), walk: None } => "unique pk set",
            SourceCursor { source: Source::Unique(_), walk: Some(_) } => "unique index range",
            _ => "another source",
        };
        assert_eq!(shape, label);
        let drain = |read| {
            let mut c = open(label, read);
            std::iter::from_fn(|| c.drain_chunk(16)).collect::<Vec<_>>()
        };
        let want = drain(u64::MAX);
        assert!(!want.is_empty(), "{label}: rows");
        for skip in [0b001, 0b010, 0b011, 0b110] {
            let got = drain(!skip);
            assert_eq!(got.len(), want.len(), "{label} {skip:#b}: chunks");
            for (g, w) in got.iter().zip(&want) {
                assert_eq!(g.len(), w.len(), "{label} {skip:#b}: rows");
                assert!(g.is_consolidated(), "{label} {skip:#b}");
                for i in 0..g.len() {
                    assert_eq!(
                        (g.get_pk_bytes(i), g.get_weight(i)),
                        (w.get_pk_bytes(i), w.get_weight(i))
                    );
                    for pi in 0..3 {
                        let null = |b: &Batch| null_word_get(b.get_null_word(i), pi);
                        let cell = |b: &Batch| match pi {
                            1 => payload_bytes(b, i, pi).into_owned(),
                            _ => payload_u64(b, i, pi).to_le_bytes().to_vec(),
                        };
                        if skip >> pi & 1 == 1 {
                            assert!(!null(g), "{label} {skip:#b}: row {i} column {pi} is NULL");
                            assert!(
                                cell(g).iter().all(|&b| b == 0),
                                "{label} {skip:#b}: row {i} column {pi}"
                            );
                        } else {
                            assert_eq!(
                                (null(g), cell(g)),
                                (null(w), cell(w)),
                                "{label} {skip:#b}: row {i} column {pi}"
                            );
                        }
                    }
                }
            }
        }
    }
}
