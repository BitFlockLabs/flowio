//! Buffer traits and types for runtime I/O operations.
//!
//! The buffer facility is split by ownership and mutability:
//! - [`IoBuffMut`] — mutable, exclusively owned, structured headroom/payload/tailroom buffer
//! - [`IoBuff`] — frozen, shared, structure-preserving buffer for send/write paths
//! - [`IoBuffView`] — read-only byte subview for slicing without preserving region structure
//! - [`IoBuffOwnedView`] — consuming read-only byte subview that owns the
//!   original [`IoBuff`] without taking another reference
//!
//! For vectored I/O:
//! - [`iobuffvec::IoBuffVecMut`] holds mutable recv/read segments
//! - [`iobuffvec::IoBuffVec`] holds frozen send/write segments
//! - [`iobuffvec::IoBuffReadOnlyVec`] holds generic read-only send/write segments
//!
//! For steady-state reuse without per-buffer allocator traffic once enough
//! slab capacity has been acquired:
//! - [`pool::IoBuffPool`] produces identically-shaped pool-backed [`IoBuffMut`] values
//! - [`bytes`] provides checked allocation-free primitive byte reads/writes
//!   for protocol encoders and decoders
//!
//! Module layout:
//! - `bytes` — checked byte-order helpers and cursors over byte slices
//! - `iobuff` — core buffer types, traits, and error codes
//! - `iobuffvec` — vectored I/O buffer chains
//! - `pool` — slab-backed allocator for fixed-shape buffer reuse
//!
//! # Fast-Path Guidance
//!
//! Preferred on the fast path:
//! - For fixed-shape steady-state reads and writes,
//!   [`pool::IoBuffPool`] plus [`IoBuffMut`] reuses slots without per-buffer
//!   heap allocation once sufficient slab capacity exists.
//! - Use [`IoBuff`] when already-built bytes should be frozen once and then
//!   reused or fanned out zero-copy.
//! - Use [`iobuffvec::IoBuffVecMut`] / [`iobuffvec::IoBuffVec`] /
//!   [`iobuffvec::IoBuffReadOnlyVec`] only when a protocol is already
//!   naturally segmented.
//!
//! - Use [`IoBuff::try_mut`] when a frozen buffer is expected to be sole-owned
//!   and must become writable without copying.
//!
//! Avoid on the fast path:
//! - Avoid [`IoBuffMut::new`] for fixed-shape steady-state
//!   buffers. Use [`pool::IoBuffPool`] instead. [`IoBuffMut::new`] is the
//!   better fit when shapes vary or simplicity matters more than reuse.
//! - Avoid [`IoBuff::make_mut`] when the buffer may be shared: that path
//!   allocates and copies. Keep exclusive [`IoBuffMut`] ownership or use
//!   [`IoBuff::try_mut`] when copying is not acceptable.
//! - A vectored chain adds no value for one already-contiguous payload; one
//!   contiguous buffer represents that layout directly.
//! - [`IoBuffView`] exposes only a byte range. Keep an [`IoBuff`] or
//!   [`IoBuffMut`] when later code needs the structured region metadata.
//!
//! # Example
//! ```
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
//! assert_eq!(frozen.bytes(), b"H:payload:T");
//! ```

pub mod bytes;
mod iobuff;
pub mod iobuffvec;
pub mod pool;

pub use iobuff::*;
