//! Tests no single module in `storage` owns: a property test over the whole
//! data round-trip, reaching the subsystem's surface rather than any one
//! module's private items.

mod data_roundtrip_proptest;
