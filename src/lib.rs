#![doc = include_str!("../README.md")]

//! A single-threaded async runtime built on Linux `io_uring`.
//!
//! `flowio` keeps the public surface concrete and transport-oriented. The
//! crate provides pooled, allocation-avoiding fast paths for steady-state
//! runtime work, transport implementations for Unix, TCP, UDP, and one-to-one
//! SCTP sockets, plus a client-side rustls TLS wrapper for connected TCP
//! streams and a narrow hostname-resolution helper. All transport I/O uses a
//! rental buffer pattern: callers pass buffers by value and receive them back
//! alongside the operation result.
//!
//! The public API is organized around two layers:
//! - [`runtime`] — executor, reactor, timers, and the buffer facility
//! - [`net`] — concrete transport types built on top of the runtime core,
//!   including TLS and hostname resolution helpers
//!
//! Low-level intrusive-list and allocator primitives live under an internal
//! `utils` module. They remain public for source compatibility during the
//! alpha period, but they are not a supported user-facing API.
//!
//! # API Stability
//!
//! This is a `0.1.1-alpha` crate. The supported user-facing surface is the
//! documented [`runtime`] and [`net`] API plus the buffer helpers they expose.
//! Source compatibility is not guaranteed during the alpha series, but public
//! API changes are tracked through `public-api.txt` and should be deliberate.
//! FlowIO requires Linux kernel 5.11 or newer because timed `io_uring` waits
//! use `IORING_ENTER_EXT_ARG`; executor construction reports
//! [`std::io::ErrorKind::Unsupported`] when the running kernel lacks that
//! feature.
//!
//! Items hidden from generated docs, including `utils` and fuzzing-only
//! parser hooks, are reserved implementation infrastructure. They may remain
//! `pub` for crate-internal wiring or temporary source compatibility, but they
//! are not part of the supported API contract.
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
//! - The zero-allocation steady-state expectation applies after runtime,
//!   timer, buffer, and transport paths are warmed. DNS resolution and TLS
//!   handshakes are setup/control-plane work and may allocate.
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
//!   setup/control-plane APIs; resolve once and use coarser-grained timeouts
//!   around larger operations.
//!
//! # Modules
//!
//! - [`runtime`] — executor, reactor, timer wheel, buffer pools, and task
//!   management.
//! - [`net`] — concrete transport types built on the runtime core, including
//!   client-side TLS over TCP.
//!
//! # Example
//! The example uses complete-buffer APIs to keep framing short. On a hot path,
//! use `read` / `write` when the caller can handle partial progress
//! explicitly.
//!
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
#[doc(hidden)]
pub mod utils;

/// Fuzzing-only re-exports of internal parsers. Enabled by the dev-only
/// `fuzzing` feature and consumed exclusively by the out-of-source `fuzz/`
/// crate; not part of the supported public API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
