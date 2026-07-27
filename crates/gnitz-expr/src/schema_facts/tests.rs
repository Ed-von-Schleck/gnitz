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

/// [`TestSchema`] with `payload_col_idx` — the one direction an implementor
/// writes by hand, independently of `locate` — shifted by one. Everything else
/// forwards, so the harness sees exactly that fault and nothing else.
struct OffByOnePayloadColIdx(TestSchema);

impl SchemaFacts for OffByOnePayloadColIdx {
    fn locate(&self, ci: usize) -> ColumnLocator {
        self.0.locate(ci)
    }
    fn payload_col_idx(&self, pi: usize) -> usize {
        self.0.payload_col_idx(pi) + 1
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
}

/// Run the harness over [`TestSchema`] — the fixture every kernel test in this
/// crate resolves its programs against, so its payload slots and OPK offsets are
/// what decide those tests' `no_nulls` and column addresses.
#[test]
fn schema_facts_harness_accepts_a_faithful_impl() {
    crate::assert_schema_facts_matrix(TestSchema::new);
}

/// The harness must not be vacuous: an off-by-one `payload_col_idx` — the
/// forwarder-reimplementation failure mode that silently flips `no_nulls` —
/// has to fail it.
#[test]
#[should_panic(expected = "payload_col_idx")]
fn schema_facts_harness_rejects_an_off_by_one_payload_col_idx() {
    assert_schema_facts_consistent(
        &OffByOnePayloadColIdx(TestSchema::new(&TINY, &TINY_PK)),
        &TINY,
        &TINY_PK,
    );
}
