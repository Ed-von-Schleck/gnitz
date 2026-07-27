//! The [`BatchView`] region/per-row contract, checked against a batch the
//! kernels were not written for — the shape a client-side adapter must satisfy.

use crate::test_support::{locator_fixture as fixture, TestView};
use crate::{assert_batchview_consistent, BatchView, RowSource};

#[test]
fn test_view_satisfies_the_region_per_row_contract() {
    let v = fixture();
    assert_batchview_consistent(&v, 3, &[(0, 4), (1, 16), (2, 8)]);
}

/// A view whose per-row accessor addresses a different payload slot than its
/// region accessor — the failure mode the harness exists for, since the
/// implementor it guards (a client adapter mapping payload slots onto a foreign
/// column representation) computes the two addresses independently.
struct MisMappedSlot(TestView);

impl RowSource for MisMappedSlot {
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        self.0.get_pk_bytes(row)
    }
    fn get_null_word(&self, row: usize) -> u64 {
        self.0.get_null_word(row)
    }
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        // Slot 1 forgets its redirect and reads slot 0. Both are 8 bytes wide,
        // so every length check in the harness still passes — only the byte
        // comparison catches it.
        let redirected = if payload_col == 1 { 0 } else { payload_col };
        self.0.get_col_ptr(row, redirected, col_size)
    }
    fn blob(&self) -> &[u8] {
        self.0.blob()
    }
}

impl BatchView for MisMappedSlot {
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8] {
        self.0.col_data(payload_col, col_size)
    }
    fn null_bmp(&self) -> &[u8] {
        self.0.null_bmp()
    }
}

#[test]
#[should_panic(expected = "get_col_ptr")]
fn contract_harness_rejects_a_mismapped_payload_slot() {
    let mut v = TestView::new(2, 8);
    assert_eq!(v.push_col(8), 0);
    assert_eq!(v.push_col(8), 1);
    for row in 0..2 {
        v.set_payload(row, 0, &(row as u64).to_le_bytes());
        v.set_payload(row, 1, &(100 + row as u64).to_le_bytes());
    }
    assert_batchview_consistent(&MisMappedSlot(v), 2, &[(0, 8), (1, 8)]);
}
