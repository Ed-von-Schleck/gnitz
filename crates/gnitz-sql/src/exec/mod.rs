//! What `dml` runs client-side over a batch it holds: the ad-hoc fold's finish
//! over its partial reply, the projection map (a fold's finalize, a FROM-less SELECT's
//! constant row and `INSERT … RETURNING`), and the ORDER BY / OFFSET / LIMIT cut over a
//! result.

pub(crate) mod agg_finish;
pub(crate) mod client_map;
pub(crate) mod order;
