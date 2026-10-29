use super::*;
use crate::relation::Cut as At;
use crate::relation::IndexClaim;
use crate::test_support::{
    batch_rows, col_range, img, make_batch_raw, make_schema_u64_i64, pk_only_schema, relation_fixture, zset_of,
    RelationFixture, Rng, TID,
};
use gnitz_wire::{image_mask, opk_bias, PkColList, ReadSpec, TypeCode, ViewBudget};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

const NBASE: u64 = 200;

/// A `kind` relation `(id U64 PK | val I64)` of `NBASE` rows at `val = val_of(id)`,
/// indexed on `val`.
fn fixture(kind: RelationKind, val_of: impl Fn(u64) -> i64) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..NBASE).map(|id| (id, 1, val_of(id))).collect();
    relation_fixture(kind, schema, &[&[1]], [make_batch_raw(&schema, &rows)])
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
/// back unapplied, for the caller's filter.
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
            let (mut cur, unapplied) = r.relation(TID).unwrap().open_bound(bound.clone(), At::Now).unwrap();
            assert_eq!(matches!(cur, SourceCursor::Bounded(_)), walks, "{bound:?}");
            assert_eq!(unapplied.map(ReadBound::Range), (!walks).then(|| bound.clone()));
            assert_eq!(drain_all(&mut cur, chunk), want, "{bound:?} chunk {chunk}");
        }
    }
}

/// A backfill's read: an index holds a pending row's entry from its ingest on,
/// so at the sealed cut it is walked only while nothing is pending. With rows
/// above the cut the whole sealed store is read and the range handed back.
#[test]
fn a_sealed_read_walks_an_index_only_over_a_store_whose_cuts_agree() {
    let mut r = fixture(RelationKind::BaseTable, x10);

    let (mut cur, unapplied) = r
        .relation(TID)
        .unwrap()
        .open_bound(val_range(500, 600), At::Sealed)
        .unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)), "nothing pending: the index");
    assert_eq!(unapplied, None);
    assert_eq!(drain_all(&mut cur, 7), rows(50..60));

    // One pending row inside the range, one outside it.
    let pending = make_batch_raw(&make_schema_u64_i64(), &[(NBASE, 1, 555), (NBASE + 1, 1, 9_000)]);
    r.ingest_pending(TID, pending).unwrap();
    let with_pending = || {
        let mut rows = rows(50..60);
        rows.push((NBASE, 1, 555));
        rows
    };

    let (mut cur, unapplied) = r
        .relation(TID)
        .unwrap()
        .open_bound(val_range(500, 600), At::Sealed)
        .unwrap();
    assert!(matches!(cur, SourceCursor::Full(_)), "rows pending: the sealed store");
    assert_eq!(
        unapplied.map(ReadBound::Range).unwrap(),
        val_range(500, 600),
        "the range is the caller's to apply"
    );
    assert_eq!(drain_all(&mut cur, 7), rows(0..NBASE), "the pending rows are left out");

    let (mut cur, _) = r
        .relation(TID)
        .unwrap()
        .open_bound(val_range(500, 600), At::Now)
        .unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)), "a read at now walks the index");
    assert_eq!(drain_all(&mut cur, 7), with_pending());

    r.seal(TID).unwrap().expect("rows were pending");
    let (mut cur, unapplied) = r
        .relation(TID)
        .unwrap()
        .open_bound(val_range(500, 600), At::Sealed)
        .unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)), "sealed: the cuts agree again");
    assert_eq!(unapplied, None);
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
        r.ingest(TID, make_batch_raw(&make_schema_u64_i64(), delta)).unwrap();
        let (mut cur, unapplied) = r
            .relation(TID)
            .unwrap()
            .open_bound(val_range(500, 560), At::Now)
            .unwrap();
        assert!(matches!(cur, SourceCursor::Bounded(_)), "{kind:?}");
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
            match r.relation(TID).unwrap().open_bound(bound.clone(), At::Now).unwrap().0 {
                SourceCursor::Bounded(_) => walked += usize::from(!admitted.is_empty()),
                _ => scanned += 1,
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
    let r = relation_fixture(view, schema, &[&[1]], [make_batch_raw(&schema, &rows)]);
    let (mut cur, _) = r.relation(TID).unwrap().open_bound(val_range(5, 6), At::Now).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
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
        .open_bound(ReadBound::Range(range), At::Now)
        .unwrap();
    assert!(matches!(cur, SourceCursor::Full(_)), "a PK walk, not the index");
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
    r.ingest(TID, make_batch_raw(&make_schema_u64_i64(), &[(5, -1, 50)]))
        .unwrap();
    let keys = PkKeys::from_images(8, [2, 5, 7, 300]);
    assert_eq!(keyed(&r.probe(TID, Probe::Pk, keys.clone()).unwrap()), [(2, 1), (7, 1)]);
    let column = r.probe(TID, Probe::PkColumn(1), keys).unwrap();
    assert_eq!(batch_rows(&column), rows([2, 7]));
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
        let spans = PkKeys::from_images(8, vals.iter().map(|&v| img(v)));
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
    let span = || PkKeys::from_images(8, [img(2)]);
    // A whole entry is a span and a source PK: wider than either store's key.
    let entry = || PkKeys::from_sorted(16, Vec::new());
    let refusals = [
        (
            r.relation(TID)
                .unwrap()
                .open_bound(ReadBound::PkSet(entry()), At::Now)
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
