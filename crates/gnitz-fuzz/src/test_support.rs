//! Frames the unit tests take apart, built with the client's own encoders.

use gnitz_core::{encode_ddl_txn, sys_schema, BatchAppender, ZSetBatch};
use gnitz_expr::{CmpOp, ExprBuilder, LogicalInstr, Sink};
use gnitz_wire::control::{append_frame, ControlHeader, Target};
use gnitz_wire::sys_rows::{CircuitRow, SysRow};
use gnitz_wire::{Circuit, ClientVerb, ReadBound, ReadSpec, CIRCUIT_TAB};

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

/// A scan of source 50 filtered by [`predicate`], encoded.
pub(crate) fn circuit() -> Vec<u8> {
    let mut c = Circuit::default();
    let scan = c.input_delta(50, ReadBound::None);
    c.filter(scan, predicate());
    c.encode()
}

/// A `DDL_TXN` of one `circuits` row: view 100's [`circuit`].
pub(crate) fn ddl_frame() -> Vec<u8> {
    let mut batch = ZSetBatch::new(sys_schema(CIRCUIT_TAB));
    let circuit = circuit();
    CircuitRow {
        view_id: 100,
        circuit: circuit.as_slice().into(),
    }
    .write(&mut BatchAppender::new(&mut batch), 1);
    encode_ddl_txn(&[(CIRCUIT_TAB, batch)])
}

/// A `SCAN_SPEC` of every row of relation 9 within `bound`.
pub(crate) fn scan_frame(bound: ReadBound) -> Vec<u8> {
    let mut out = Vec::new();
    let hdr = ControlHeader::naming(ClientVerb::ScanSpec, Target::from(9), 0);
    append_frame(&mut out, &hdr, &ReadSpec::all_rows(bound).encode(), None, None);
    out
}
