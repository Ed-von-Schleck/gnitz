//! Unit tests for the resolved-addressing substrate, driven through a
//! non-`MemBatch` [`BatchView`].
//!
//! `TestView` is not decoration. The engine is a binary crate, so its physical
//! batch is unreachable from here; this owned-buffer view is the only proof the
//! locator paths work against a batch they were not written for — and it is the
//! seed of the client-side adapter that will read the same expressions.

use super::*;
use gnitz_wire::type_code as tc;

/// A [`BatchView`] over owned buffers, laid out region-wise like the physical
/// batch: one packed OPK PK region, one null-bitmap word per row, and one
/// contiguous buffer per payload slot.
struct TestView {
    rows: usize,
    pk_stride: usize,
    pk: Vec<u8>,
    nulls: Vec<u8>,
    cols: Vec<Vec<u8>>,
    blob: Vec<u8>,
}

impl TestView {
    fn new(rows: usize, pk_stride: usize) -> Self {
        TestView {
            rows,
            pk_stride,
            pk: vec![0u8; rows * pk_stride],
            nulls: vec![0u8; rows * 8],
            cols: Vec::new(),
            blob: Vec::new(),
        }
    }

    /// Append a payload column whose cells are `col_size` bytes wide.
    fn push_col(&mut self, col_size: usize) -> usize {
        self.cols.push(vec![0u8; self.rows * col_size]);
        self.cols.len() - 1
    }

    /// OPK-encode `native` (native-LE bytes of type `type_code`) into the PK
    /// region of `row` at `byte_off`.
    fn set_pk_col(&mut self, row: usize, byte_off: usize, native: &[u8], type_code: u8) {
        let base = row * self.pk_stride + byte_off;
        gnitz_wire::encode_pk_column(native, type_code, &mut self.pk[base..base + native.len()]);
    }

    fn set_payload(&mut self, row: usize, pi: usize, native: &[u8]) {
        let sz = native.len();
        self.cols[pi][row * sz..row * sz + sz].copy_from_slice(native);
    }

    fn set_null(&mut self, row: usize, slot: usize) {
        let base = row * 8;
        let word = gnitz_wire::read_u64_le(&self.nulls, base) | (1u64 << slot);
        self.nulls[base..base + 8].copy_from_slice(&word.to_le_bytes());
    }
}

impl RowSource for TestView {
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        &self.pk[row * self.pk_stride..(row + 1) * self.pk_stride]
    }
    fn get_null_word(&self, row: usize) -> u64 {
        gnitz_wire::read_u64_le(&self.nulls, row * 8)
    }
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        &self.cols[payload_col][row * col_size..row * col_size + col_size]
    }
    fn blob(&self) -> &[u8] {
        &self.blob
    }
}

impl BatchView for TestView {
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8] {
        let c = &self.cols[payload_col];
        debug_assert_eq!(c.len(), self.rows * col_size);
        c
    }
    fn null_bmp(&self) -> &[u8] {
        &self.nulls
    }
}

/// A three-row view with a compound `(U32, I64)` PK and payload slots
/// `0: I32`, `1: U128`, `2: U64`.
fn fixture() -> TestView {
    let mut v = TestView::new(3, 12);
    assert_eq!(v.push_col(4), 0);
    assert_eq!(v.push_col(16), 1);
    assert_eq!(v.push_col(8), 2);
    for (row, (a, b)) in [(7u32, -1i64), (0, 0), (u32::MAX, i64::MIN)].into_iter().enumerate() {
        v.set_pk_col(row, 0, &a.to_le_bytes(), tc::U32);
        v.set_pk_col(row, 4, &b.to_le_bytes(), tc::I64);
        v.set_payload(row, 0, &(-3i32).to_le_bytes());
        v.set_payload(row, 1, &(1u128 << 100).to_le_bytes());
        v.set_payload(row, 2, &(row as u64).to_le_bytes());
    }
    v
}

#[test]
fn test_view_satisfies_the_region_per_row_contract() {
    let v = fixture();
    assert_batchview_consistent(&v, 3, &[(0, 4), (1, 16), (2, 8)]);
}

#[test]
fn pk_locator_reads_decode_the_opk_sign_flip() {
    let v = fixture();
    // Trailing signed column of a compound PK — non-zero `byte_off`.
    let signed = ColumnLocator::Pk {
        byte_off: 4,
        size: 8,
        type_code: tc::I64,
    };
    // Leading unsigned column.
    let unsigned = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    assert_eq!(signed.size(), 8);
    assert_eq!(signed.type_code(), tc::I64);
    // `bytes` is the at-rest OPK image: big-endian with the sign bit flipped.
    let mut want = [0u8; 8];
    gnitz_wire::encode_pk_column(&(-1i64).to_le_bytes(), tc::I64, &mut want);
    assert_eq!(signed.bytes(&v, 0), &want[..]);
    assert_eq!(unsigned.bytes(&v, 0), &7u32.to_be_bytes()[..]);
    // `native_le_bytes` undoes it.
    let mut scratch = [0u8; 16];
    assert_eq!(signed.native_le_bytes(&v, 0, &mut scratch), &(-1i64).to_le_bytes()[..]);
    assert_eq!(signed.native_le_bytes(&v, 2, &mut scratch), &i64::MIN.to_le_bytes()[..]);
    assert_eq!(
        unsigned.native_le_bytes(&v, 2, &mut scratch),
        &u32::MAX.to_le_bytes()[..]
    );
    // `native_key` is the zero-extended two's-complement value.
    assert_eq!(signed.native_key(&v, 0), (-1i64) as u64 as u128);
    assert_eq!(signed.native_key(&v, 2), i64::MIN as u64 as u128);
    assert_eq!(unsigned.native_key(&v, 0), 7u128);
    // `route_key` is the widened OPK image — sign-flipped, so -1 lands below 0.
    assert!(signed.route_key(&v, 2) < signed.route_key(&v, 0));
    assert!(signed.route_key(&v, 0) < signed.route_key(&v, 1));
    assert_eq!(unsigned.route_key(&v, 0), 7u128);
}

#[test]
fn payload_locator_reads_are_verbatim_native_le() {
    let v = fixture();
    let narrow = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::I32,
    };
    let wide = ColumnLocator::Payload {
        slot: 1,
        size: 16,
        type_code: tc::U128,
    };
    assert_eq!(narrow.bytes(&v, 1), &(-3i32).to_le_bytes()[..]);
    let mut scratch = [0u8; 16];
    assert_eq!(narrow.native_le_bytes(&v, 1, &mut scratch), &(-3i32).to_le_bytes()[..]);
    assert_eq!(narrow.native_key(&v, 1), (-3i32) as u32 as u128);
    // A 16-byte cell reads all 16 bytes, not a zero-extended low 8.
    assert_eq!(wide.bytes(&v, 2), &(1u128 << 100).to_le_bytes()[..]);
    assert_eq!(wide.native_key(&v, 2), 1u128 << 100);
    assert_eq!(wide.route_key(&v, 2), 1u128 << 100);
}

#[test]
fn is_null_reads_the_addressed_slot_bit() {
    let mut v = fixture();
    v.set_null(1, 2);
    let slot0 = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::I32,
    };
    let slot2 = ColumnLocator::Payload {
        slot: 2,
        size: 8,
        type_code: tc::U64,
    };
    let pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    assert!(slot2.is_null(&v, 1));
    assert!(!slot2.is_null(&v, 0));
    assert!(!slot0.is_null(&v, 1), "a set bit at slot 2 must not read as slot 0");
    assert!(!pk.is_null(&v, 1), "PK columns are never null");
}

// ---------------------------------------------------------------------------
// SchemaFacts
// ---------------------------------------------------------------------------

/// Column table and PK list of the fixture below. The PK list is **permuted and
/// non-contiguous** (`PRIMARY KEY (c2, c0)`), the shape a harness that inferred
/// PK order from column order could not see: `c2` sits at OPK offset 0 and `c0`
/// at 8, the reverse of their column order.
const TINY: [(u8, bool); 4] = [(tc::U32, false), (tc::F64, true), (tc::I64, false), (tc::STRING, false)];
const TINY_PK: [usize; 2] = [2, 0];
/// Per column: OPK byte offset for a PK column (indices 0 and 2), dense payload
/// slot otherwise (indices 1 and 3) — two namespaces, disjoint by column.
const TINY_ADDR: [u8; 4] = [8, 0, 0, 1];

/// A schema whose `payload_col_idx` is derived independently of `locate` — the
/// one direction an implementor writes by hand, and the one the harness has to
/// be able to catch.
struct TinySchema {
    /// Injected fault: `payload_col_idx` shifts by one.
    broken_payload_col_idx: bool,
}

impl SchemaFacts for TinySchema {
    fn locate(&self, ci: usize) -> ColumnLocator {
        let (type_code, _) = TINY[ci];
        let size = gnitz_wire::wire_stride(type_code) as u8;
        if TINY_PK.contains(&ci) {
            ColumnLocator::Pk {
                byte_off: TINY_ADDR[ci],
                size,
                type_code,
            }
        } else {
            ColumnLocator::Payload {
                slot: TINY_ADDR[ci],
                size,
                type_code,
            }
        }
    }
    fn payload_col_idx(&self, pi: usize) -> usize {
        let ci = (0..TINY.len()).filter(|c| !TINY_PK.contains(c)).nth(pi).unwrap();
        ci + self.broken_payload_col_idx as usize
    }
    fn num_payload_cols(&self) -> usize {
        TINY.len() - TINY_PK.len()
    }
    fn num_columns(&self) -> usize {
        TINY.len()
    }
    fn col_type_code(&self, ci: usize) -> u8 {
        TINY[ci].0
    }
    fn col_nullable(&self, ci: usize) -> bool {
        TINY[ci].1
    }
}

#[test]
fn schema_facts_harness_accepts_a_faithful_impl() {
    assert_schema_facts_consistent(
        &TinySchema {
            broken_payload_col_idx: false,
        },
        &TINY,
        &TINY_PK,
    );
}

/// The harness must not be vacuous: an off-by-one `payload_col_idx` — the
/// forwarder-reimplementation failure mode that silently flips `no_nulls` —
/// has to fail it.
#[test]
#[should_panic(expected = "payload_col_idx")]
fn schema_facts_harness_rejects_an_off_by_one_payload_col_idx() {
    assert_schema_facts_consistent(
        &TinySchema {
            broken_payload_col_idx: true,
        },
        &TINY,
        &TINY_PK,
    );
}
