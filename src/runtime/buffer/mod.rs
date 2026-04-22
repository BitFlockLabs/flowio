//! Buffer traits and types for runtime I/O operations.
//!
//! The buffer facility is split by ownership and mutability:
//! - [`IoBuffMut`] — mutable, exclusively owned, structured headroom/payload/tailroom buffer
//! - [`IoBuff`] — frozen, shared, structure-preserving buffer for send/write paths
//! - [`IoBuffView`] — read-only byte subview for slicing without preserving region structure
//!
//! For vectored I/O:
//! - [`iobuffvec::IoBuffVecMut`] holds mutable recv/read segments
//! - [`iobuffvec::IoBuffVec`] holds frozen send/write segments
//!
//! For zero-alloc steady-state reuse:
//! - [`pool::IoBuffPool`] produces identically-shaped pool-backed [`IoBuffMut`] values
//!
//! Module layout:
//! - `iobuff` — core buffer types, traits, and error codes
//! - `iobuffvec` — vectored I/O buffer chains
//! - `pool` — pool allocator for zero-allocation steady-state reuse
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - For fixed-shape steady-state reads and writes, prefer
//!   [`pool::IoBuffPool`] plus [`IoBuffMut`]. That is the best buffer fast
//!   path in this crate because it avoids heap allocation after warmup.
//! - Use [`IoBuff`] when already-built bytes should be frozen once and then
//!   reused or fanned out zero-copy.
//! - Use [`iobuffvec::IoBuffVecMut`] / [`iobuffvec::IoBuffVec`] only when a
//!   protocol is already naturally segmented.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use [`IoBuffMut::new`] for fixed-shape steady-state
//!   buffers. Use [`pool::IoBuffPool`] instead. [`IoBuffMut::new`] is the
//!   better fit when shapes vary or simplicity matters more than reuse.
//! - Prefer not to use vectored chains for one contiguous payload. Use one
//!   contiguous buffer instead because it is simpler and usually faster.
//! - Prefer not to use [`IoBuffView`] as the primary transport buffer shape.
//!   It is a parsing/slicing helper; keep the original structured buffer on
//!   the hot path when later mutation or reuse matters.
//!
//! # Example
//! ```
//! use flowio::runtime::buffer::iobuffvec::IoBuffVec;
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//!
//! let mut pool = IoBuffPool::new(IoBuffPoolConfig {
//!     headroom: 8,
//!     payload: 64,
//!     tailroom: 4,
//!     objs_per_slab: 16,
//! })
//! .unwrap();
//! pool.init();
//!
//! let mut buf = pool.alloc().unwrap();
//! buf.payload_append(b"payload").unwrap();
//! buf.headroom_prepend(b"H:").unwrap();
//! buf.tailroom_append(b":T").unwrap();
//!
//! let frozen = buf.freeze();
//! let view = frozen.slice(2..9).unwrap();
//! assert_eq!(view.bytes(), b"payload");
//!
//! let chain: IoBuffVec<1> = [frozen].into();
//! assert_eq!(chain.len(), 11);
//! ```

mod iobuff;
pub mod iobuffvec;
pub mod pool;

pub use iobuff::*;
