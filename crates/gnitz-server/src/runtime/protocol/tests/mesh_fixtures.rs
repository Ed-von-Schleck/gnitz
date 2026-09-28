use super::*;
use crate::runtime::w2m::fixtures::test_ring;

/// `nw` workers' views of one mesh, each with the wakes of every peer's ring.
/// The mapping and the rings are leaked, as a worker's are.
pub(crate) fn meshes(nw: usize) -> Vec<Mesh> {
    let base = create_region(nw).expect("map the mesh");
    let rings: Vec<*mut u8> = (0..nw).map(|_| unsafe { test_ring(4096) }.leak()).collect();
    (0..nw)
        .map(|rank| unsafe { Mesh::new(base, rank, rings.iter().map(|&p| SalWake::new(p)).collect()) })
        .collect()
}
