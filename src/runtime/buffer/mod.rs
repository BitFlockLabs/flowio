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
//! - `iobuff` — Core buffer types, traits, and error codes
//! - `iobuffvec` — Vectored I/O buffer chains.
//! - `pool` — Pool allocator for zero-alloc fast-path buffer allocation.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::buffer::iobuffvec::IoBuffVec;
//! use flowio::runtime::buffer::IoBuffMut;
//!
//! let mut buf = IoBuffMut::new(8, 64, 4).unwrap();
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
