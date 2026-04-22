//! A single-threaded async runtime built on Linux `io_uring`.
//!
//! `flowio` keeps the public surface concrete and transport-oriented. The
//! crate provides a zero-allocation fast path for steady-state runtime work,
//! transport implementations for Unix, TCP, UDP, and one-to-one SCTP sockets,
//! plus a client-side rustls TLS wrapper for connected TCP streams and a
//! narrow hostname-resolution helper. All transport I/O uses a rental buffer
//! pattern: callers pass buffers by value and receive them back alongside the
//! operation result.
//!
//! The public API is organized around three layers:
//! - [`runtime`] — executor, reactor, timers, and the buffer facility
//! - [`net`] — concrete transport types built on top of the runtime core,
//!   including TLS and hostname resolution helpers
//! - [`utils`] — intrusive lists and memory primitives used internally by the
//!   runtime and transports
//!
//! The buffer facility supports:
//! - heap-owned mutable buffers via [`runtime::buffer::IoBuffMut`]
//! - frozen shared buffers via [`runtime::buffer::IoBuff`]
//! - read-only subviews via [`runtime::buffer::IoBuffView`]
//! - vectored chains via [`runtime::buffer::iobuffvec::IoBuffVec`] /
//!   [`runtime::buffer::iobuffvec::IoBuffVecMut`]
//! - pool-backed zero-alloc reuse via [`runtime::buffer::pool::IoBuffPool`]
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - Keep one long-lived [`runtime::executor::Executor`] per thread. Creating
//!   executors is setup work, not steady-state fast-path work.
//! - For fixed-shape steady-state I/O buffers, prefer
//!   [`runtime::buffer::pool::IoBuffPool`] plus
//!   [`runtime::buffer::IoBuffMut`]. After warmup, that avoids heap
//!   allocation and deallocation on the hot path.
//! - Prefer [`net::tcp::TcpConnector`] and [`net::sctp::SctpConnector`] for
//!   repeated outbound connects because they reuse stable connector state
//!   across attempts.
//! - On stream transports and TLS, prefer `read` / `write` when the caller can
//!   handle partial progress. Those are the lowest-overhead APIs because they
//!   submit a single transport operation and avoid `_exact` / `_all` retry
//!   bookkeeping.
//! - Use vectored APIs only when data is already naturally segmented. For one
//!   contiguous payload, the contiguous buffer APIs stay simpler and usually
//!   faster.
//!
//! Prefer not to use on the fast path:
//! - Do not rebuild an executor around each request or operation. Reuse the
//!   thread's long-lived executor instead.
//! - Prefer not to use [`runtime::buffer::IoBuffMut::new`] for fixed-shape
//!   steady-state buffers. Use [`runtime::buffer::pool::IoBuffPool`] instead.
//! - Prefer not to use [`net::tcp::TcpStream::connect`] and timeout
//!   convenience variants in repeated outbound loops. Use
//!   [`net::tcp::TcpConnector`] or [`net::sctp::SctpConnector`] instead.
//! - Prefer not to use `read_exact` / `write_all` and their vectored
//!   equivalents unless the protocol truly requires complete-buffer semantics.
//!   Use `read` / `write` or `readv` / `writev` when the caller can track
//!   progress explicitly.
//! - Prefer not to keep hostname resolution and per-step deadline wrappers in
//!   the steady-state data path. [`net::resolver`] and [`runtime::timer`] are
//!   setup/control-path APIs; resolve once and use coarser-grained timeouts
//!   around larger operations.
//!
//! # Modules
//!
//! - [`runtime`] — executor, reactor, timer wheel, buffer pools, and task
//!   management.
//! - [`net`] — concrete transport types built on the runtime core, including
//!   client-side TLS over TCP.
//! - [`utils`] — intrusive data structures and memory primitives.
//!
//! # Example
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//! use flowio::runtime::executor::Executor;
//! use std::error::Error;
//!
//! let mut pool = IoBuffPool::new(IoBuffPoolConfig {
//!     headroom: 8,
//!     payload: 64,
//!     tailroom: 0,
//!     objs_per_slab: 16,
//! })?;
//! pool.init();
//!
//! let mut executor = Executor::new()?;
//! executor.run(async move {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut send = pool.alloc().unwrap();
//!     send.payload_append(b"hello").unwrap();
//!     send.headroom_prepend(b"H:").unwrap();
//!
//!     let (res, _buf) = left.write_all(send).await;
//!     res.unwrap();
//!
//!     let recv = pool.alloc().unwrap();
//!     let (res, buf) = right.read_exact(recv, 7).await;
//!     res.unwrap();
//!     assert_eq!(buf.bytes(), b"H:hello");
//! })?;
//! # Ok::<(), Box<dyn Error>>(())
//! ```

pub mod net;
pub mod runtime;
pub mod utils;
