//! Dispatch-loop tests: epochs of hand-built programs, one per path the VM
//! itself owns — operator semantics are `gnitz-zset`'s to test.

use super::*;
use crate::test_support::{
    cmp_all, join_reference, make_batch_u128, make_batch_u128_raw, make_schema_u128_i64, register_full_pk,
    weighted_rows, zset_of,
};
use gnitz_store::relation::{RelationKind, StoreConfig};
use gnitz_wire::{AggDescriptor, AggFunc, JoinKind, TypeCode, ViewBudget};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::schema::SchemaColumn;

// ── Fixtures ─────────────────────────────────────────────────────────────

impl Vm {
    /// Every instruction's op, in program order; the compiler's tests read
    /// emitted programs through it.
    pub(in crate::query) fn ops(&self) -> impl Iterator<Item = &Op> {
        self.instructions.iter().map(|i| &i.op)
    }
}

/// The view a test program's children are opened under.
const VIEW_ID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

/// `make_schema_u128_i64` with a nullable payload: a label wider than the
/// batches a test feeds under it.
fn nullable() -> SchemaDescriptor {
    let cols = [
        SchemaColumn::new(TypeCode::U128, false),
        SchemaColumn::new(TypeCode::I64, true),
    ];
    SchemaDescriptor::new(&cols, &[0])
}

/// The share of rows a worker owns by their whole PK.
fn own_rows(schema: &SchemaDescriptor) -> Rc<ScatterPlan> {
    Rc::new(ScatterPlan::native(Placement::full_pk(schema)))
}

/// Push `plan`'s reduce over `in_reg`, declaring its output trace and, for a
/// MIN/MAX, its value index. Returns the output register and trace.
fn reduce(p: &mut ProgramBuilder, in_reg: DeltaReg, plan: stream::ReducePlan) -> (DeltaReg, StateIdx) {
    let out_schema = *plan.output_schema();
    let out_trace = p.declare("reduce".into(), out_schema);
    let index = plan.index_schema().map(|schema| p.declare("avidx".into(), *schema));
    let op = Op::Reduce {
        out_trace: Integral::Own(out_trace),
        index,
        plan: Box::new(plan),
    };
    (p.push(in_reg, out_schema, op), out_trace)
}

/// Push a distinct over `reg`, declaring its history. Returns the output register
/// and the history.
fn distinct(p: &mut ProgramBuilder, reg: DeltaReg) -> (DeltaReg, StateIdx) {
    let hist = p.declare("hist".into(), p.schema_of(reg));
    let op = Op::WeightClamp {
        hist: ClampHistory::Own(hist),
        kind: ClampKind::Distinct,
    };
    (p.push(reg, p.schema_of(reg), op), hist)
}

/// A built program, the operator state it runs over, and what that state lives
/// in. Every run it drives to its end must leave every register free.
struct TestVm {
    vm: Vm,
    state: CircuitState,
    registry: RelationRegistry,
    /// The sources the program has been fed no row of.
    unfed: Vec<u64>,
    _dir: tempfile::TempDir,
}

impl TestVm {
    /// Finish `p` with output `out`, opening every child store it declares in a
    /// fresh directory as the DAG opens a compiled view's.
    fn open(p: ProgramBuilder, out: DeltaReg) -> TestVm {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
        register_full_pk(
            &mut registry,
            VIEW_ID,
            RelationKind::View(ViewBudget::Plain),
            make_schema_u128_i64(),
        );
        let vm = p.finish(out);
        TestVm {
            state: CircuitState::open(&registry, VIEW_ID, vm.layout()).unwrap(),
            vm,
            registry,
            unfed: Vec::new(),
            _dir: dir,
        }
    }

    /// Run `f` over the program and the stores an epoch of it runs over.
    #[track_caller]
    fn drive<T>(&mut self, f: impl FnOnce(&mut Vm, &mut Stores<'_>) -> Result<T, String>) -> T {
        let mut stores = Stores {
            own: &mut self.state,
            registry: &self.registry,
            unfed: &self.unfed,
        };
        let out = f(&mut self.vm, &mut stores).unwrap();
        assert!(
            self.vm.batches.iter().all(Batch::is_empty),
            "a run left a register holding rows"
        );
        out
    }

    /// The ground rows the program's global aggregates owe, each minted in an
    /// epoch of its own.
    fn ground(&mut self) -> Batch {
        self.drive(mint_ground_rows)
    }

    /// One tick's epoch over `seed`, taken, of a program that runs no round.
    fn epoch(&mut self, reg: DeltaReg, mut seed: Batch) -> Batch {
        self.epoch_with(reg, &mut seed, true, |_, _| panic!("the program stands at a round"))
    }

    /// One tick's epoch over the lent `seed`, each round it stands at answered by
    /// `gather`, which is handed the round's rows and whether they were lent.
    fn epoch_with(
        &mut self,
        reg: DeltaReg,
        seed: &mut Batch,
        take: bool,
        mut gather: impl FnMut(&Batch, bool) -> Batch,
    ) -> Batch {
        self.drive(|vm, stores| {
            let mut epoch = Epoch::tick(reg, seed, take);
            let mut gathered = None;
            loop {
                match run(vm, stores, &mut epoch, gathered.take())? {
                    Ran::Done(out) => return Ok(out),
                    Ran::Round { batch, .. } => {
                        let lent = matches!(batch, Cow::Borrowed(_));
                        gathered = Some(gather(&batch, lent));
                    }
                }
            }
        })
    }

    fn replay(&mut self, reg: DeltaReg, seed: Batch) -> Batch {
        let entry = self.vm.replay_entry(reg).unwrap();
        self.drive(|vm, stores| replay_chunk(vm, stores, entry, seed))
    }

    /// The rows one child store holds, at their net weights.
    fn held(&self, idx: StateIdx) -> Rc<Batch> {
        self.state.cursor_between(idx, &[], &[]).materialize()
    }
}

impl std::ops::Deref for TestVm {
    type Target = Vm;

    fn deref(&self) -> &Vm {
        &self.vm
    }
}

/// `out` holds exactly `rows` — `(pk, weight, payload)` over a
/// `make_schema_u128_i64`-shaped schema — in that order.
#[track_caller]
fn assert_rows(out: &Batch, rows: &[(u128, i64, i64)]) {
    assert_eq!(
        weighted_rows(out),
        weighted_rows(&make_batch_u128_raw(out.schema(), rows))
    );
}

/// Each row's payload cells as signed integers of their own width, with its
/// weight, sorted — the Z-set of an output whose key the test need not spell.
fn int_rows(b: &Batch) -> Vec<(Vec<Option<i128>>, i64)> {
    let int = |cell: Vec<u8>| {
        let fill = if cell.last().is_some_and(|&hi| hi & 0x80 != 0) {
            0xFF
        } else {
            0
        };
        let mut le = [fill; 16];
        le[..cell.len()].copy_from_slice(&cell);
        i128::from_le_bytes(le)
    };
    let mut rows: Vec<_> = weighted_rows(b)
        .into_iter()
        .map(|((_, cells), w)| (cells.into_iter().map(|c| c.map(int)).collect(), w))
        .collect();
    rows.sort();
    rows
}

// ── Registers ────────────────────────────────────────────────────────────

/// `A + B`, the second operand a filter of the first: both non-empty, the
/// second empty, and both empty, epoch after epoch on one program — so each
/// epoch also sees nothing of the one before. The output leaves under its
/// register's label, wider than the batches fed, and a register nothing reads is
/// freed all the same.
#[test]
fn a_union_sums_its_operands_in_every_epoch() {
    let schema = nullable();
    let mut p = ProgramBuilder::default();
    let a = p.seed(schema);
    let filter = cmp_all(&[(gnitz_expr::CmpOp::Gt, 1, 15)])
        .resolve_filter(&schema)
        .unwrap();
    let b = p.push(a, schema, Op::Filter(Box::new(filter)));
    let out = p.push(a, schema, Op::Union { in_b: b });
    p.push(a, schema, Op::Negate);
    let mut vm = TestVm::open(p, out);

    type Rows = &'static [(u128, i64, i64)];
    let cases: [(Rows, Rows); 3] = [
        (
            &[(1, 1, 10), (2, 1, 20), (3, 1, 30)],
            &[(1, 1, 10), (2, 2, 20), (3, 2, 30)],
        ),
        (&[(5, 1, 5)], &[(5, 1, 5)]),
        (&[], &[]),
    ];
    for (rows, want) in cases {
        let got = vm.epoch(a, make_batch_u128(&make_schema_u128_i64(), rows));
        assert_eq!(*got.schema(), schema);
        assert_rows(&got, want);
    }
}

/// A `Union` with `in_a == in_b` is `Z + Z`, which doubles every weight. The
/// double saturates, so a consolidated operand's `i64::MIN` row stays a row
/// where a wrapping double would leave a ghost under the claim.
#[test]
fn a_self_union_doubles_every_weight() {
    let schema = make_schema_u128_i64();
    let mut p = ProgramBuilder::default();
    let r0 = p.seed(schema);
    let out = p.push(r0, schema, Op::Union { in_b: r0 });
    let mut vm = TestVm::open(p, out);
    let rows = [(1, 1, 10), (2, 3, 20), (3, i64::MIN, 30), (4, i64::MAX, 40)];
    let got = vm.epoch(r0, make_batch_u128(&schema, &rows));
    assert!(got.is_consolidated());
    assert_rows(&got, &[(1, 2, 10), (2, 6, 20), (3, i64::MIN, 30), (4, i64::MAX, 40)]);
}

// ── Integrates run after the instructions, and a replay runs none ────────

/// A clamp reads its history as it stood before the epoch: 0 → 3 crosses in,
/// 3 → 2 stays, 2 → 0 crosses out. Integrated first, the first epoch would see
/// 3 → 3 and emit nothing.
#[test]
fn a_history_is_read_before_the_epoch_integrates_into_it() {
    let schema = make_schema_u128_i64();
    let mut p = ProgramBuilder::default();
    let r0 = p.seed(schema);
    let (out, _) = distinct(&mut p, r0);
    let mut vm = TestVm::open(p, out);

    for (w, want) in [(3, &[(1, 1, 42)][..]), (-1, &[]), (-2, &[(1, -1, 42)])] {
        assert_rows(&vm.epoch(r0, make_batch_u128(&schema, &[(1, w, 42)])), want);
    }
}

/// `ΔA ⋈ z⁻¹I(B) + ΔB ⋈ z⁻¹I(A)` over UNSORTED deltas carrying a cancelling
/// duplicate and two payloads on one key, the sources' epochs interleaved: each
/// epoch's output is its delta joined with the other side as integrated before
/// it — a cross join across every run of it, an equi join at the delta's keys.
#[test]
fn a_two_term_join_joins_each_delta_with_the_other_sides_past() {
    let schema = make_schema_u128_i64();
    type Rows = &'static [(u128, i64, i64)];
    // `(from B, rows)`: the last B epoch retracts a row A has matched.
    let epochs: [(bool, Rows); 4] = [
        (false, &[(0, 1, 30)]),
        (true, &[(2, 1, 200), (1, 1, 100), (1, -1, 100), (1, 1, 101)]),
        (false, &[(2, 1, 20), (1, 1, 10), (1, 2, 10), (1, 1, 11)]),
        (true, &[(2, -1, 200), (0, 1, 300)]),
    ];
    for kind in [JoinKind::Equi, JoinKind::Cross] {
        let plan = |right| stream::JoinPlan::from_wire(kind, right, &schema, &schema).unwrap();
        let out_schema = *plan(false).output_schema();
        let mut p = ProgramBuilder::default();
        let (trace_a, trace_b) = (p.declare("ta".into(), schema), p.declare("tb".into(), schema));
        let (a, b) = (p.seed(schema), p.seed(schema));
        let ab = p.push(
            a,
            out_schema,
            Op::JoinDT {
                trace: Integral::Own(trace_b),
                plan: Box::new(plan(false)),
            },
        );
        let ba = p.push(
            b,
            out_schema,
            Op::JoinDT {
                trace: Integral::Own(trace_a),
                plan: Box::new(plan(true)),
            },
        );
        let out = p.push(ab, out_schema, Op::Union { in_b: ba });
        p.integrate(a, trace_a);
        p.integrate(b, trace_b);
        let mut vm = TestVm::open(p, out);

        // Per side, every delta it has been fed. Bilinear, so the product of the
        // raw batches denotes the product of the Z-sets they denote.
        let mut past = [Batch::empty_with_schema(&schema), Batch::empty_with_schema(&schema)];
        for (i, (from_b, rows)) in epochs.into_iter().enumerate() {
            let delta = make_batch_u128_raw(&schema, rows);
            let (want, _) = join_reference(kind, from_b, &schema, &schema, &delta, &past[!from_b as usize]);
            assert!(i < 2 || !want.is_empty(), "premise: {kind:?} epoch {i} matches");
            let got = vm.epoch(if from_b { b } else { a }, delta.clone());
            assert_eq!(zset_of(&got, &out_schema), want, "{kind:?} epoch {i}");
            past[from_b as usize].append_batch(&delta);
        }
    }
}

/// A join whose trace is a table's own store reads it as the program last
/// absorbed it — up to its cut, without the rows ingested above it — and a table
/// the program has been fed nothing of reads empty, whatever it holds.
#[test]
fn a_join_over_its_source_reads_it_up_to_its_cut() {
    const TABLE: u64 = VIEW_ID + 1;
    let schema = make_schema_u128_i64();
    let rekey = gnitz_wire::MapKind::Reindex {
        keep: vec![1],
        key: vec![(0, TypeCode::U128)],
        role: gnitz_wire::ReindexRole::Auxiliary,
        nulls: gnitz_wire::NullKeys::Drop,
    };
    let rekey = MapPlan::from_wire(&schema, &rekey).unwrap();
    let plan = stream::JoinPlan::over_source(false, &schema, &schema, &rekey).unwrap();
    let out_schema = *plan.output_schema();
    let mut p = ProgramBuilder::default();
    let a = p.seed(schema);
    let out = p.push(
        a,
        out_schema,
        Op::JoinDT {
            trace: Integral::Relation(TABLE),
            plan: Box::new(plan),
        },
    );
    let mut vm = TestVm::open(p, out);
    register_full_pk(&mut vm.registry, TABLE, RelationKind::BaseTable, make_schema_u128_i64());
    let sealed = make_batch_u128(&schema, &[(1, 1, 100), (2, 1, 200)]);
    vm.registry.ingest(TABLE, sealed.clone()).unwrap();
    let pending = make_batch_u128(&schema, &[(2, -1, 200), (3, 1, 300)]);
    vm.registry.ingest_pending(TABLE, pending).unwrap();

    let delta = make_batch_u128(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let (want, rows) = join_reference(JoinKind::Equi, false, &schema, &schema, &delta, &sealed);
    assert_eq!(rows, 2, "premise: keys 1 and 2 pair, 3 does not");
    assert_eq!(zset_of(&vm.epoch(a, delta.clone()), &out_schema), want);

    vm.unfed = vec![TABLE];
    assert!(vm.epoch(a, delta).is_empty());
}

/// A hydration replay seeds any register, a mid-program one included, and runs
/// from its first reader past every stateful operator its seed does not reach,
/// integrating nothing: an integrate would write the seed back and double the
/// trace's weights on every read. It is refused where the seed reaches a
/// stateful operator, directly or through another operator.
#[test]
fn a_replay_runs_past_what_its_seed_does_not_reach_and_integrates_nothing() {
    let schema = make_schema_u128_i64();
    let mut p = ProgramBuilder::default();
    let trace = p.declare("trace".into(), schema);
    let (a, b) = (p.seed(schema), p.seed(schema));
    let mid = p.push(a, schema, Op::Negate);
    let nb = p.push(b, schema, Op::Negate);
    let (clamped, hist) = distinct(&mut p, nb);
    let out = p.push(mid, schema, Op::Union { in_b: clamped });
    p.integrate(out, trace);
    let mut vm = TestVm::open(p, out);
    assert!(vm.replay_entry(nb).is_err(), "the clamp reads it");
    assert!(vm.replay_entry(b).is_err(), "the clamp reads it through the negate");

    assert_rows(&vm.epoch(b, make_batch_u128(&schema, &[(1, -1, 10)])), &[(1, 1, 10)]);
    let held = |vm: &TestVm| [hist, trace].map(|t| weighted_rows(&vm.held(t)));
    let before = held(&vm);
    let rows = [(2, 1, 20)];
    assert_rows(&vm.replay(a, make_batch_u128(&schema, &rows)), &[(2, -1, 20)]);
    assert_rows(&vm.replay(mid, make_batch_u128(&schema, &rows)), &rows);
    assert_rows(&vm.replay(clamped, make_batch_u128(&schema, &rows)), &rows);
    assert_eq!(held(&vm), before);
}

// ── An operator's own index ──────────────────────────────────────────────

/// A value-indexed global reduce: its V₀ row is minted in an epoch of its own,
/// which opens no value index, flows on through the reduce's readers and lands
/// in its trace; an empty epoch after it skips the pass. A raw delta is folded
/// before both the kernel and the index, which is ingested before the reduce
/// opens it, and a retracted minimum is replaced by the next one it holds.
#[test]
fn a_global_min_mints_its_ground_row_then_tracks_its_history() {
    let schema = make_schema_u128_i64();
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = stream::ReducePlan::from_wire(&schema, &[], &aggs, true).unwrap();
    let mut p = ProgramBuilder::default();
    let r0 = p.seed(schema);
    let (reduced, trace) = reduce(&mut p, r0, plan);
    let out = p.push(reduced, p.schema_of(reduced), Op::Negate);
    let mut vm = TestVm::open(p, out);

    assert_eq!(
        int_rows(&vm.ground()),
        vec![(vec![None, Some(0)], -1)],
        "−V₀ downstream"
    );
    assert_eq!(
        int_rows(&vm.held(trace)),
        vec![(vec![None, Some(0)], 1)],
        "V₀ in the trace"
    );
    let mut epoch = |rows: &[(u128, i64, i64)]| int_rows(&vm.epoch(r0, make_batch_u128_raw(&schema, rows)));
    assert!(epoch(&[]).is_empty(), "an empty epoch mints nothing");
    assert_eq!(
        epoch(&[(1, 1, 10), (3, 1, 7), (2, 1, 5), (3, -1, 7)]),
        vec![(vec![None, Some(0)], 1), (vec![Some(5), Some(2)], -1)],
    );
    assert_eq!(
        epoch(&[(2, -1, 5)]),
        vec![(vec![Some(5), Some(2)], 1), (vec![Some(10), Some(1)], -1)],
    );
    assert_eq!(int_rows(&vm.held(trace)), vec![(vec![Some(10), Some(1)], 1)]);
}

/// A `TopN` instruction ingests its index entries after it runs, so the second
/// epoch sees the first one's row and displaces it instead of adding a row
/// beside it.
#[test]
fn a_second_topn_epoch_displaces_the_first_ones_row() {
    let schema = make_schema_u128_i64();
    let order = [gnitz_wire::OrderKey { col: 1, desc: true, nulls_first: false }];
    let plan = stream::OrderedPlan::top_n(&schema, &[], &order, 1, 0).unwrap();
    let out_schema = *plan.output_schema();
    let mut p = ProgramBuilder::default();
    let index_table = p.declare("topnidx".into(), *plan.index_schema());
    let r0 = p.seed(schema);
    let out = p.push(r0, out_schema, Op::Ordered { index: index_table, plan: Box::new(plan) });
    let mut vm = TestVm::open(p, out);

    // The output row carries the input row whole: its PK, then its value.
    let first = vm.epoch(r0, make_batch_u128(&schema, &[(1, 1, 10)]));
    assert_eq!(int_rows(&first), vec![(vec![Some(1), Some(10)], 1)]);
    // A strictly greater value displaces the top row entirely.
    assert_eq!(
        int_rows(&vm.epoch(r0, make_batch_u128(&schema, &[(2, 1, 99)]))),
        vec![(vec![Some(1), Some(10)], -1), (vec![Some(2), Some(99)], 1)],
    );
}

// ── Exchange rounds ──────────────────────────────────────────────────────

/// A round sends its operand under the operand register's label, the program
/// resumes from what it gathered rather than from what it sent, and the operand
/// is lent, left to a later reader.
#[test]
fn a_round_sends_its_operand_and_resumes_from_what_it_gathered() {
    let schema = nullable();
    let mut p = ProgramBuilder::default();
    let r0 = p.seed(schema);
    let negated = p.push(r0, schema, Op::Negate);
    let gathered = p.round(negated, own_rows(&schema));
    let out = p.push(gathered, schema, Op::Union { in_b: negated });
    let mut vm = TestVm::open(p, out);

    // The seed comes under its producer's narrower label.
    let mut seed = make_batch_u128(&make_schema_u128_i64(), &[(1, 1, 10)]);
    let mut sent = Vec::new();
    let got = vm.epoch_with(r0, &mut seed, true, |rows, lent| {
        sent.push((*rows.schema() == schema, lent, weighted_rows(rows)));
        make_batch_u128(&schema, &[(7, 2, 70)])
    });
    assert_eq!(
        sent,
        [(true, true, weighted_rows(&make_batch_u128(&schema, &[(1, -1, 10)])))]
    );
    assert_rows(&got, &[(1, -1, 10), (7, 2, 70)]);
}

/// A round over the seed lends the caller's batch on, taking it only where the
/// caller let it, and sends it as its register holds it: folded in place where
/// a broadcast owes its reader the fold — one sort, not one per worker — and as
/// the caller made it under a keyed round, whose receiver folds. A lent seed
/// comes back as sent, a taken one empty.
#[test]
fn a_round_over_the_seed_sends_it_as_its_register_holds_it() {
    let schema = make_schema_u128_i64();
    let raw = [(2, 1, 20), (1, 1, 10), (2, 1, 20)];
    for broadcast in [false, true] {
        for take in [false, true] {
            let plan = if broadcast {
                Rc::new(ScatterPlan::broadcast())
            } else {
                own_rows(&schema)
            };
            let mut p = ProgramBuilder::default();
            let r0 = p.seed(schema);
            let gathered = p.round(r0, plan);
            let (out, _) = distinct(&mut p, gathered);
            let mut vm = TestVm::open(p, out);

            let mut seed = make_batch_u128_raw(&schema, &raw);
            let mut sent = Vec::new();
            let got = vm.epoch_with(r0, &mut seed, take, |rows, lent| {
                sent.push((rows.is_consolidated(), lent));
                rows.clone()
            });
            let case = format!("broadcast {broadcast}, take {take}");
            assert_eq!(sent, [(broadcast, !take)], "{case}");
            assert_rows(&got, &[(1, 1, 10), (2, 1, 20)]);
            let back: &[_] = match (take, broadcast) {
                (true, _) => &[],
                (false, true) => &[(1, 1, 10), (2, 2, 20)],
                (false, false) => &raw,
            };
            assert_eq!(
                weighted_rows(&seed),
                weighted_rows(&make_batch_u128_raw(&schema, back)),
                "{case}"
            );
        }
    }
}

/// A round runs iff the epoch's seed reaches it, whatever the seed holds: an
/// empty delta of a source that feeds it still runs it, and a delta of another
/// source never does. A replay runs on one worker, so it may not seed a register
/// that reaches a round, and passes one its seed does not reach.
#[test]
fn a_round_runs_by_what_its_epoch_seeds_not_by_what_the_seed_holds() {
    let schema = make_schema_u128_i64();
    let mut p = ProgramBuilder::default();
    let (a, b) = (p.seed(schema), p.seed(schema));
    let relayed = p.round(a, own_rows(&schema));
    let out = p.push(relayed, schema, Op::Union { in_b: b });
    let mut vm = TestVm::open(p, out);
    assert!(!vm.idles_on_empty(a) && vm.idles_on_empty(b));

    let mut rounds = 0;
    let mut echo = |rows: &Batch, _| {
        rounds += 1;
        rows.clone()
    };
    let mut none = Batch::empty_with_schema(&schema);
    assert!(vm.epoch_with(a, &mut none, true, &mut echo).is_empty());
    let mut rows = make_batch_u128(&schema, &[(1, 1, 10)]);
    assert_rows(&vm.epoch_with(b, &mut rows, true, &mut echo), &[(1, 1, 10)]);
    assert_eq!(rounds, 1, "the empty epoch of `a`, and not the epoch of `b`");

    assert!(vm.replay_entry(a).is_err());
    assert_rows(&vm.replay(b, make_batch_u128(&schema, &[(3, 1, 30)])), &[(3, 1, 30)]);
}
