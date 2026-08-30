//! The [`SchemaFacts`] consistency harness, and proof it is not vacuous.

use crate::test_support::TestSchema;
use crate::{assert_schema_facts_consistent, ColumnLocator, SchemaFacts};
use gnitz_wire::type_code as tc;

/// Column table and PK list of the fixture below. The PK list is **permuted and
/// non-contiguous** (`PRIMARY KEY (c2, c0)`), the shape a harness that inferred
/// PK order from column order could not see: `c2` sits at OPK offset 0 and `c0`
/// at 8, the reverse of their column order.
const TINY: [(u8, bool); 4] = [(tc::U32, false), (tc::F64, true), (tc::I64, false), (tc::STRING, false)];
const TINY_PK: [usize; 2] = [2, 0];

/// Which faithful answer [`Faulty`] corrupts. The two are the methods an
/// implementor may write by hand rather than derive: `payload_col_idx` off a
/// precomputed table, and the nullability masks off a precomputed field.
enum Fault {
    PayloadColIdxOffByOne,
    NullableMaskDrifted,
}

/// A faithful [`TestSchema`] with exactly one answer perturbed — the fault as
/// data, so one set of forwarders serves every case.
struct Faulty(TestSchema, Fault);

impl SchemaFacts for Faulty {
    fn locate(&self, ci: usize) -> ColumnLocator {
        self.0.locate(ci)
    }
    fn num_payload_cols(&self) -> usize {
        self.0.num_payload_cols()
    }
    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }
    fn col_type_code(&self, ci: usize) -> u8 {
        self.0.col_type_code(ci)
    }
    fn col_nullable(&self, ci: usize) -> bool {
        self.0.col_nullable(ci)
    }
    fn payload_col_idx(&self, pi: usize) -> usize {
        self.0.payload_col_idx(pi) + usize::from(matches!(self.1, Fault::PayloadColIdxOffByOne))
    }
    fn nullable_payload_slots(&self) -> u64 {
        self.0.nullable_payload_slots() << u32::from(matches!(self.1, Fault::NullableMaskDrifted))
    }
}

/// Run the harness over [`TestSchema`] — the fixture every kernel test in this
/// crate resolves its programs against, so its payload slots and OPK offsets are
/// what decide those tests' column addresses.
#[test]
fn schema_facts_harness_accepts_a_faithful_impl() {
    crate::assert_schema_facts_matrix(TestSchema::new);
}

/// The harness must not be vacuous: an off-by-one `payload_col_idx` — the
/// forwarder-reimplementation failure mode that silently type-checks a column
/// or register sink against a neighbouring output column — has to fail it.
#[test]
#[should_panic(expected = "payload_col_idx")]
fn schema_facts_harness_rejects_an_off_by_one_payload_col_idx() {
    assert_faulty(Fault::PayloadColIdxOffByOne);
}

/// The nullability masks decide which null bits a batch may carry, and an
/// implementor is free to override them off a precomputed field — so the harness
/// has to fail one that disagrees with the column table it was built from.
#[test]
#[should_panic(expected = "nullable_payload_slots")]
fn schema_facts_harness_rejects_a_drifted_nullable_mask() {
    assert_faulty(Fault::NullableMaskDrifted);
}

fn assert_faulty(fault: Fault) {
    assert_schema_facts_consistent(&Faulty(TestSchema::new(&TINY, &TINY_PK), fault), &TINY, &TINY_PK);
}
