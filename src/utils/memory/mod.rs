//! Internal memory helpers used by the runtime's slab- and pool-backed
//! allocations.

pub mod pool;
pub mod provider;
pub(crate) mod provider_owned_pool;
pub mod slab;
