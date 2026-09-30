//! What `dml` runs client-side over a seek/scan reply: the aggregate-fold finisher, the
//! projection map, and the ORDER BY / OFFSET / LIMIT sink.

pub(crate) mod agg_finish;
pub(crate) mod client_map;
pub(crate) mod order;
