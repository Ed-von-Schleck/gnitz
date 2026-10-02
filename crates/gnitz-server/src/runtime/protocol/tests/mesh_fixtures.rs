use super::*;
use crate::runtime::w2m::fixtures::test_rings;

/// `nw` workers' views of one mesh of `outbox_bytes` outboxes, each with the
/// wakes of every peer's ring.
pub(crate) fn meshes(nw: usize, outbox_bytes: usize) -> Vec<Mesh> {
    create(outbox_bytes, &test_rings(vec![4096; nw]).2).expect("map the mesh")
}
