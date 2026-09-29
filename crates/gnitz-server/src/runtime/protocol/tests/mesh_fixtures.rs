use super::*;
use crate::runtime::w2m::fixtures::test_ring;

/// `nw` workers' views of one mesh of `outbox_bytes` outboxes, each with the
/// wakes of every peer's ring. The mapping and the rings are leaked, as a
/// worker's are.
pub(crate) fn meshes(nw: usize, outbox_bytes: usize) -> Vec<Mesh> {
    let base = create_region(nw, outbox_bytes).expect("map the mesh");
    let rings: Vec<*mut u8> = (0..nw).map(|_| unsafe { test_ring(4096) }.leak()).collect();
    (0..nw)
        .map(|rank| unsafe { Mesh::new(base, rank, rings.iter().map(|&p| SalWake::new(p)).collect()) })
        .collect()
}
