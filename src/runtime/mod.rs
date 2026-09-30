//! Single-threaded async runtime built on `io_uring`.
//!
//! [`executor`] drives async work, [`buffer`] provides owned I/O buffers,
//! and [`timer`] provides sleeps and timeouts. Memory referenced by the kernel
//! stays at a stable address in runtime-owned storage. Transport and timer
//! operations expose concrete future types.
//!
//! # Platform
//!
//! The supported platform is x86-64 Linux 5.11 or newer. Timed waits require
//! `IORING_ENTER_EXT_ARG`; executor construction returns
//! [`std::io::ErrorKind::Unsupported`] when the kernel lacks that feature.
//! `IORING_OP_CLOSE` is available from Linux 5.6, and the 14-byte SCTP event
//! subscription layout is available from Linux 5.5. Both fit this runtime
//! floor; the SCTP transport does not use the older 13-byte layout.
//!
//! # Ownership and cancellation
//!
//! Runtime state, tasks, buffers, transport handles, polled futures, and task
//! wakers belong to one OS thread. An idle socket may be used sequentially by
//! different FlowIO executors on that thread. Only one
//! [`executor::Executor::run`] may be active on the thread at a time.
//!
//! Once FlowIO queues an I/O request or arms a timer, its future must be polled
//! by that executor. Polling outside an active run or through another
//! executor returns [`std::io::ErrorKind::NotConnected`]. An unsubmitted
//! rental operation returns its buffer immediately; a submitted operation
//! retains it until FlowIO observes the operation's completion, then returns
//! it with the error. Dropping an in-flight read can discard bytes from a
//! racing completion. Treat read cancellation as a protocol boundary unless
//! the protocol has its own recovery path.
//!
//! If bounded shutdown abandons a ring before observing an operation's
//! completion, the future cannot safely return its buffer and remains pending.
//! The operation's kernel-visible storage, buffer, and descriptor stay alive
//! until process exit. Each abandoned reactor can retain at most
//! `ring_entries` descriptors through these operations; repeated abandonments
//! accumulate these sets and can exhaust process descriptors.
//!
//! Standard task wakers must be cloned, woken, and dropped on their owner
//! thread. Debug builds assert this rule; release builds use a direct,
//! allocation-free wake path. There is no cross-thread waker relay or
//! inter-executor queue. Application-owned bounded cross-thread queues may
//! carry unpolled, runtime-independent `Send` requests and results; the
//! receiving owner thread must create and poll the FlowIO task. Live FlowIO
//! state, buffers, sockets, polled futures, and task wakers cannot cross that
//! boundary.
//!
//! Timeout wrappers distinguish [`timer::TimeoutError::Elapsed`] from
//! [`timer::TimeoutError::Runtime`], which preserves the underlying I/O error.
//! They validate the active and origin executor before polling the wrapped
//! future. After validation, that future has priority; an immediately ready
//! result needs no timer entry.
//!
//! # Allocation and capacity
//!
//! Task, timer, I/O-operation, and buffer pools acquire slabs on demand. Warm
//! representative work before measuring allocation behavior. The number of
//! operation slots used by submitted I/O is capped by
//! [`reactor::ReactorConfig::ring_entries`]. Nonempty vectored operations accept
//! at most 1,024 active iovecs, including SCTP message sends and receives.
//! Empty segments and unused chain capacity do
//! not count. Task and timer pools have no configurable total slab cap, so a
//! strict process-memory ceiling requires an external limit.
//!
//! Each distinct socket descriptor has one shared ownership allocation on
//! its owner thread. TCP, Unix, UDP, and SCTP construction or adoption each
//! allocate one, as do TCP and SCTP listeners. TLS shares the TCP allocation,
//! a Unix pair creates two, and a TCP split clone creates one for its
//! duplicated descriptor. Failure invokes Rust's global allocation-error
//! handler, normally aborting the process rather than returning a typed
//! FlowIO error. Allocation and final deallocation may synchronize or block
//! inside the selected allocator. A descriptor-backed operation takes one
//! non-atomic ownership reference when first queued and holds it until FlowIO
//! observes completion; retries reuse it, and taking or releasing a non-final
//! reference does not allocate.
//!
//! # Socket closing
//!
//! Each executor owns one close-worker thread for socket closes that may
//! block. A fresh FlowIO socket with no raw-fd exposure or descriptor alias
//! has known nonpositive `SO_LINGER` and needs no option lookup. Adopted
//! sockets, exposed descriptors, aliases, and sockets accepted from an exposed
//! listener query `SO_LINGER` once when their last owner is dropped.
//!
//! Nonpositive-linger sockets normally close through batched `io_uring`
//! requests. FlowIO keeps each descriptor owned in bounded storage until the
//! kernel has consumed its close request. If a final listener owner is
//! released while its accept completion is being processed, its nonpositive
//! close can be deferred in the reactor's bounded queue until completion
//! processing releases the ring; no nested ring access is needed. If a close
//! cannot be queued, the nonpositive-linger descriptor closes directly.
//!
//! Positive or unknown linger uses the worker queue, which holds at most
//! `ring_entries` descriptors; the worker can hold one more while closing it.
//! Enqueueing never waits. If the queue is full or disconnected, FlowIO tries
//! to disable linger and closes directly. A failed attempt to disable linger
//! leaves the risk that direct close blocks for the original linger period.
//! Shutdown destroys the ring before releasing unsubmitted close owners, then
//! drains and joins the worker; a queued positive-linger close can delay
//! shutdown. Outside an executor, descriptors close directly without a linger
//! query.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the fast path:
//! - Construct the executor once and keep it alive for the lifetime of the
//!   thread that owns the runtime.
//! - Use [`executor::Executor::spawn`] inside that run boundary, or
//!   [`executor::Executor::try_spawn`] when a spawn failure must return the
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
pub(crate) mod retained;
#[cfg(feature = "test-support")]
pub(crate) mod retained_test_support;
pub(crate) mod task;
#[cfg(any(debug_assertions, feature = "test-support"))]
pub(crate) mod test_hooks;
pub mod timer;
