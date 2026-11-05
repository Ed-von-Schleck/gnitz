//! Frames the unit tests take apart, built with the client's own encoders.

use std::num::NonZeroU64;

use gnitz_core::{encode_ddl_txn, sys_schema, BatchAppender, ZSetBatch};
use gnitz_expr::{CmpOp, ExprBuilder, LogicalInstr, Sink};
use gnitz_wire::txn_frame::{encode_scan_multi, ScanItem};
use gnitz_wire::{
    Circuit, CircuitRow, KeyRange, OrderKey, PkColList, ReadBound, ReadSink, ReadSpec, RowsCut, SinkKind, SysRow,
    Target, CIRCUIT_TAB,
};

/// `col > 7`, as an expression program's blob.
pub(crate) fn predicate() -> Vec<u8> {
    let mut b = ExprBuilder::new();
    let a = b.emit(LogicalInstr::LoadCol { col: 1 });
    let seven = b.emit(LogicalInstr::LoadConst { val: 7, unsigned: false });
    let gt = b.emit(LogicalInstr::Cmp { op: CmpOp::Gt, a, b: seven });
    b.build(vec![Sink::Reg(gt)])
        .expect("a well-formed program")
        .to_blob_bytes()
}

/// A scan of source 50 filtered by `predicate`, behind a scan of source 51
/// nothing reads, encoded. The filter's input is node 0 with any one node
/// dropped or repeated, so every item edit of the node list leaves a circuit
/// that decodes.
pub(crate) fn circuit(predicate: &[u8]) -> Vec<u8> {
    let mut c = Circuit::default();
    let scan = c.input_delta(50, ReadBound::None);
    c.input_delta(51, ReadBound::None);
    c.filter(scan, predicate.to_vec());
    c.encode()
}

/// A `DDL_TXN` of one `circuits` row: view 100's `circuit`.
pub(crate) fn ddl_frame(circuit: &[u8]) -> Vec<u8> {
    let mut batch = ZSetBatch::new(sys_schema(CIRCUIT_TAB));
    CircuitRow { view_id: 100, circuit: circuit.into() }.write(&mut BatchAppender::new(&mut batch), 1);
    encode_ddl_txn(&[(CIRCUIT_TAB, batch)])
}

/// A `SCAN_MULTI` of relation 9 under `spec`.
pub(crate) fn scan_frame(spec: &ReadSpec) -> Vec<u8> {
    let mut out = Vec::new();
    let spec = spec.encode();
    encode_scan_multi(
        &mut out,
        &[ScanItem {
            target: Target::from(9),
            reply_layout: 0,
            spec: &spec,
        }],
    );
    out
}

/// The first five rows passing [`predicate`], ordered by the columns `order`.
pub(crate) fn cut(order: &[u16]) -> ReadSpec {
    let order = order
        .iter()
        .map(|&col| OrderKey { col, desc: false, nulls_first: false });
    let cut = RowsCut {
        k: NonZeroU64::new(5).unwrap(),
        order: order.collect(),
    };
    ReadSpec {
        bound: ReadBound::None,
        predicate: predicate(),
        sink: ReadSink {
            map: None,
            kind: SinkKind::Rows { cut: Some(cut) },
        },
    }
}

/// One frame of each shape the anatomy takes apart: a `circuits` row, a scan
/// filtered and cut in order, and a scan of one 16-byte key image.
pub(crate) fn frames() -> [Vec<u8>; 3] {
    let point = KeyRange::point(PkColList::from_slice(&[0]), &[u128::MAX]);
    [
        ddl_frame(&circuit(&predicate())),
        scan_frame(&cut(&[1, 2])),
        scan_frame(&ReadSpec::all_rows(ReadBound::Range(point))),
    ]
}
