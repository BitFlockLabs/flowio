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
//!
//! Only one [`executor::Executor::run`] may be active on a thread. Runtime I/O
//! and timer futures must be polled by the FlowIO executor that first submitted
//! or armed them. Polling without an active FlowIO run or with another
//! executor's task waker returns [`std::io::ErrorKind::NotConnected`]. A
//! submitted rental I/O future retains its caller-owned buffer until the
//! original completion arrives, then returns that buffer with the error.
//! Standard task wakers must be cloned, woken, and dropped on their owner
//! thread. Debug builds assert this contract; no cross-thread relay exists.
//! Timeout wrappers distinguish deadline expiry ([`timer::TimeoutError::Elapsed`])
//! from timer-runtime failure ([`timer::TimeoutError::Runtime`]). A timeout
//! validates its active and origin executor context before polling the wrapped
//! future. After validation, the wrapped future retains priority and an
//! immediately ready result allocates no timer entry.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the fast path:
//! - Construct the executor once and keep it alive for the lifetime of the
//!   thread that owns the runtime.
//! - Use [`executor::Executor::spawn`] inside that run boundary, or
//!   [`executor::Executor::try_spawn`] when admission failure must return the
//!   unpolled future.
//! - Prefer pool-backed buffers from [`buffer::pool::IoBuffPool`] for
//!   fixed-shape steady-state I/O because that avoids allocator churn after
//!   enough slots have been acquired and returned.
//!
//! Avoid on the fast path:
//! - Do not construct a fresh executor or enter a new [`executor::Executor::run`]
//!   boundary around each request. Spawn work inside the long-lived run.
//! - Do not arm a separate timer around every small I/O step when one
//!   [`timer::timeout_at`] around the protocol phase preserves the required
//!   deadline semantics.
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
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod io;
pub(crate) mod op;
pub mod reactor;
pub(crate) mod refcount;
#[allow(dead_code)]
pub(crate) mod retained;
#[cfg(feature = "test-support")]
pub(crate) mod retained_test_support;
pub(crate) mod task;
#[cfg(any(debug_assertions, feature = "test-support"))]
pub(crate) mod test_hooks;
pub mod timer;
