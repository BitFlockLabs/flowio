//! Single-threaded async runtime built on `io_uring`.
//!
//! The runtime exposes transport-specific futures for Unix, TCP, UDP, and SCTP
//! while keeping kernel-visible state in stable runtime-owned slots.
//!
//! The main user-facing pieces are:
//! - [`executor`] for driving async work
//! - [`buffer`] for heap-, pool-, and vectored I/O buffers
//! - [`timer`] for runtime-native sleeps and timeouts
//! - [`io`] for minimal runtime-owned operations such as `NOP`
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
pub mod task;
pub mod timer;
