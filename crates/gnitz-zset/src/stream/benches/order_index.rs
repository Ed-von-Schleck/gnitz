use super::tests::Harness;
use super::OrderIndex;
use super::OrderedPlan;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::mix;
use gnitz_foundation::perf::Counter;
use gnitz_wire::{OrderKey, TypeCode};

const N: u64 = 1 << 16;

/// By entry shape: the index's bytes per row, the instructions per delta row of
/// building a bulk delta's entries and of folding them into index order, and
/// those of three whole top-10 epochs — into an empty index, beside its rows,
/// and the retraction of the first.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn order_index_batch_bench() {
    let counter = Counter::instructions();
    const INT: SchemaColumn = SchemaColumn::new(TypeCode::I64, false);
    const INT_NULL: SchemaColumn = SchemaColumn::new(TypeCode::I64, true);
    const STR: SchemaColumn = SchemaColumn::new(TypeCode::String, false);
    const STR_NULL: SchemaColumn = SchemaColumn::new(TypeCode::String, true);
    /// `(label, the columns behind the U64 id, group columns, ORDER BY columns,
    /// distinct values of the first ORDER BY column, groups)`. Every other
    /// column ascends with the id, as a timestamp does.
    type Case = (
        &'static str,
        &'static [SchemaColumn],
        &'static [u32],
        &'static [u16],
        u64,
        u64,
    );
    let cases: [Case; 15] = [
        ("global, I64 key", &[INT], &[], &[1], u64::MAX, 4096),
        ("I64 group, I64 key", &[INT, INT], &[1], &[2], u64::MAX, 4096),
        // Keys that agree on every byte but the lowest.
        ("I64 group, dense I64 key", &[INT, INT], &[1], &[2], 256, 4096),
        (
            "nullable I64 group, I64 key",
            &[INT_NULL, INT],
            &[1],
            &[2],
            u64::MAX,
            4096,
        ),
        (
            "2 nullable I64 groups, I64 key",
            &[INT_NULL, INT_NULL, INT],
            &[1, 2],
            &[3],
            u64::MAX,
            4096,
        ),
        (
            "I64 group, nullable I64 key",
            &[INT, INT_NULL],
            &[1],
            &[2],
            u64::MAX,
            4096,
        ),
        (
            "I64 group, 3 keys",
            &[INT, INT, INT_NULL, INT],
            &[1],
            &[2, 3, 4],
            u64::MAX,
            4096,
        ),
        ("I64 group, 2 keys, 4 leads", &[INT, INT, INT], &[1], &[2, 3], 4, 4096),
        (
            "I64 group, 3 keys, 4 leads",
            &[INT, INT, INT_NULL, INT],
            &[1],
            &[2, 3, 4],
            4,
            4096,
        ),
        (
            "I64 group, key + STRING, 4 leads",
            &[INT, INT, STR],
            &[1],
            &[2, 3],
            4,
            4096,
        ),
        (
            "I64 group, I64 key, 7 carried",
            &[INT, INT, INT_NULL, INT_NULL, STR_NULL, STR_NULL, INT, INT],
            &[1],
            &[2],
            u64::MAX,
            4096,
        ),
        (
            "16 groups, 3 keys, 4 leads",
            &[INT, INT, INT, INT],
            &[1],
            &[2, 3, 4],
            4,
            16,
        ),
        (
            "16 groups, key + STRING, 4 leads",
            &[INT, INT, STR],
            &[1],
            &[2, 3],
            4,
            16,
        ),
        ("I64 group, STRING key", &[INT, STR], &[1], &[2], u64::MAX, 4096),
        (
            "I64 group, STRING + key, 4 leads",
            &[INT, STR, INT],
            &[1],
            &[2, 3],
            4,
            4096,
        ),
    ];
    for (label, tail, group_cols, order, leads, groups) in cases {
        let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
        cols.extend_from_slice(tail);
        let schema = SchemaDescriptor::new(&cols, &[0]);
        let make = |salt: u64| -> Batch {
            let mut b = BatchBuilder::new(&schema);
            for i in salt * N..(salt + 1) * N {
                b.begin_row(i as u128, 1);
                for (c, col) in (1u32..).zip(tail) {
                    let v = match c as u16 == order[0] {
                        true => mix(i) % leads,
                        false if groups == 16 => i + c as u64,
                        false => mix(i + c as u64),
                    };
                    match (col.type_code, col.nullable) {
                        _ if group_cols.contains(&c) => b.put_int((mix(i) % groups) as u128),
                        (TypeCode::String, _) => b.put_string(&format!("{v:040}")),
                        (_, true) => b.put_opt_int((i % 4 != 0).then_some(v as u128)),
                        _ => b.put_u64(v),
                    }
                }
                b.end_row();
            }
            b.finish().into_consolidated()
        };
        let (d1, d2) = (make(1), make(2));
        let keys: Vec<OrderKey> = order
            .iter()
            .map(|&col| OrderKey { col, desc: false, nulls_first: false })
            .collect();
        let (index, _) = OrderIndex::new(&schema, group_cols, &keys, []).unwrap();
        let (entries, build) = counter.measure(|| std::hint::black_box(index.batch(&d1)));
        assert_eq!(entries.count, d1.count, "one entry per delta row");
        let bytes = entries.total_bytes() as f64 / N as f64;
        let pk = index.schema.pk_stride();
        let (folded, fold) = counter.measure(|| std::hint::black_box(entries.to_consolidated()));
        assert_eq!(folded.count, d1.count, "no two entries are one element");
        let mut h = Harness::new(OrderedPlan::top_n(&schema, group_cols, &keys, 10, 0).unwrap());
        let retraction = d1.clone().negated().into_consolidated();
        let [e1, e2, e3] = [&d1, &d2, &retraction].map(|d| h.measure(&counter, d) as f64 / N as f64);
        println!(
            "order_index_batch_bench {label:<33} index {bytes:6.1} B/row (pk {pk:2}), build {:6.1}, fold {:7.1}; \
             top-10 empty {e1:7.1}, populated {e2:7.1}, retraction {e3:7.1} instr/row",
            build as f64 / N as f64,
            fold as f64 / N as f64,
        );
    }
}
