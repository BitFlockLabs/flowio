//! A single-threaded async runtime built on Linux `io_uring`.
//!
//! `flowio` provides a zero-allocation fast-path runtime with transport
//! implementations for Unix, TCP, UDP, and one-to-one SCTP sockets, plus a
//! client-side rustls TLS wrapper for connected TCP streams. All I/O uses a
//! rental buffer pattern — callers pass buffers by value and receive them
//! back alongside the result on completion.
//!
//! The public API is organized around three layers:
//! - [`runtime`] — executor, reactor, timers, and the buffer facility
//! - [`net`] — concrete transport types built on top of the runtime core
//! - [`utils`] — intrusive lists and memory primitives used internally
//!
//! The buffer facility supports:
//! - heap-owned mutable buffers via [`runtime::buffer::IoBuffMut`]
//! - frozen shared buffers via [`runtime::buffer::IoBuff`]
//! - read-only subviews via [`runtime::buffer::IoBuffView`]
//! - vectored chains via [`runtime::buffer::iobuffvec::IoBuffVec`] /
//!   [`runtime::buffer::iobuffvec::IoBuffVecMut`]
//! - pool-backed zero-alloc reuse via [`runtime::buffer::pool::IoBuffPool`]
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
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut send = IoBuffMut::new(8, 64, 0).unwrap();
//!     send.payload_append(b"hello").unwrap();
//!     send.headroom_prepend(b"H:").unwrap();
//!
//!     let (res, _buf) = left.write_all(send).await;
//!     res.unwrap();
//!
//!     let recv = IoBuffMut::new(8, 64, 0).unwrap();
//!     let (res, buf) = right.read_exact(recv, 7).await;
//!     res.unwrap();
//!     assert_eq!(buf.bytes(), b"H:hello");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

pub mod net;
pub mod runtime;
pub mod utils;
