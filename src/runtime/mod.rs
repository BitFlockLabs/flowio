//! Single-threaded async runtime built on `io_uring`.
//!
//! The runtime keeps kernel-visible state in stable runtime-owned slots and
//! exposes concrete futures for transport, timer, and minimal bootstrap I/O
//! operations.
//!
//! The main user-facing pieces are:
//! - [`executor`] for driving async work
//! - [`buffer`] for heap-, pool-, and vectored I/O buffers
//! - [`timer`] for runtime-native sleeps and timeouts
//! - [`io`] for minimal runtime-owned operations such as `NOP`
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - Construct the executor once and keep it alive for the lifetime of the
//!   thread that owns the runtime.
//! - Prefer pool-backed buffers from [`buffer::pool::IoBuffPool`] for
//!   fixed-shape steady-state I/O because that avoids allocator churn after
//!   warmup.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to construct a fresh executor around each request or
//!   operation. Reuse the thread's long-lived executor instead.
//! - Prefer not to use [`timer`] and [`io`] as substitutes for the transport
//!   data path exposed under [`crate::net`]. They are control-path/runtime
//!   helpers.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::executor::Executor;
//! use flowio::runtime::timer::sleep;
//! use std::time::Duration;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     sleep(Duration::from_millis(1)).await.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

pub mod buffer;
pub mod executor;
pub(crate) mod fd;
pub mod io;
pub mod op;
pub mod reactor;
#[allow(dead_code)]
pub(crate) mod retained;
pub mod task;
pub mod timer;
