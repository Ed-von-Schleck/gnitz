use super::*;

/// `nw` workers' views of one mesh of `outbox_bytes` outboxes, over parks of
/// their own.
pub(crate) fn meshes(nw: usize, outbox_bytes: usize) -> Vec<Mesh> {
    let parks = WorkerParks::create(nw).expect("map the parks");
    create(outbox_bytes, parks).expect("map the mesh")
}
