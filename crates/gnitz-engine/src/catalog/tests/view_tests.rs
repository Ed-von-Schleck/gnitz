// Tests previously drove engine.create_view(qname, &CircuitGraph, sql) on the
// CatalogEngine surface. That method (and its CircuitGraph DTO) were removed
// alongside the circuit-graph schema redesign — production paths reach the
// engine via `client.rs::create_view_with_circuit` over the wire instead.
//
// Wire-driven coverage lives in:
//   - crates/gnitz-core/tests/integration.rs (Rust-side end-to-end via GnitzClient)
//   - tests/test_*.py (Python E2E suite)
//
// A re-port of the deleted unit tests is tracked separately; no production
// code path reads `engine.create_view` after the redesign.
