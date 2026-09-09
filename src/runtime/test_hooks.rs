//! Development-only fault injection hooks for crate tests.
//!
//! The module is compiled in debug builds or when `test-support` is enabled;
//! normal release builds exclude it. The thread-local counters make otherwise
//! unreachable pressure and kernel-error paths deterministic without changing
//! production behavior when no hook is armed.

use std::cell::Cell;
use std::io;
thread_local! {
    static FAIL_OP_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_TIMER_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RAW_SQE_SUBMITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RING_SUBMITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RING_SUBMIT_ERRNO: Cell<i32> = const { Cell::new(0) };
    static FAIL_RING_WAITS: Cell<usize> = const { Cell::new(0) };
    static FAIL_RING_WAIT_ERRNO: Cell<i32> = const { Cell::new(0) };
    #[cfg(debug_assertions)]
    static FAIL_IOBUFF_POOL_SLAB_ALLOCS: Cell<usize> = const { Cell::new(0) };
    static FAIL_REACTOR_EXT_ARG_PROBES: Cell<usize> = const { Cell::new(0) };
    static FORCE_REACTOR_SHUTDOWN_FALLBACKS: Cell<usize> = const { Cell::new(0) };
}

/// Makes the next completion-state allocation on this thread fail.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn fail_next_op_alloc() {
    FAIL_OP_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next timer-entry allocation on this thread fail.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn fail_next_timer_alloc() {
    FAIL_TIMER_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next raw reactor SQE submission on this thread fail with
/// `WouldBlock`.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn fail_next_sqe_submit() {
    fail_next_raw_sqe_submit();
}

/// Makes the next raw reactor SQE submission on this thread fail with
/// `WouldBlock`.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn fail_next_raw_sqe_submit() {
    FAIL_RAW_SQE_SUBMITS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next raw `io_uring_enter` submit call on this thread fail with a
/// specific OS errno.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn fail_next_ring_submit_errno(errno: i32) {
    FAIL_RING_SUBMITS.with(|fails| fails.set(fails.get().saturating_add(1)));
    FAIL_RING_SUBMIT_ERRNO.with(|stored| stored.set(errno));
}

/// Makes the next `io_uring_enter` wait call on this thread fail with a
/// specific OS errno.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn fail_next_ring_wait_errno(errno: i32) {
    FAIL_RING_WAITS.with(|fails| fails.set(fails.get().saturating_add(1)));
    FAIL_RING_WAIT_ERRNO.with(|stored| stored.set(errno));
}

/// Makes the next `IoBuffPool` slab allocation on this thread fail.
#[doc(hidden)]
#[cfg(all(test, debug_assertions))]
pub(crate) fn fail_next_iobuff_pool_slab_alloc() {
    FAIL_IOBUFF_POOL_SLAB_ALLOCS.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next reactor feature validation report missing
/// `IORING_ENTER_EXT_ARG` support.
#[doc(hidden)]
#[cfg(all(test, not(miri)))]
pub(crate) fn fail_next_reactor_ext_arg_probe() {
    FAIL_REACTOR_EXT_ARG_PROBES.with(|fails| fails.set(fails.get().saturating_add(1)));
}

/// Makes the next reactor shutdown skip its ordinary bounded drain and enter
/// the fallback path immediately.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn force_next_reactor_shutdown_fallback() {
    FORCE_REACTOR_SHUTDOWN_FALLBACKS.with(|forces| {
        forces.set(forces.get().saturating_add(1));
    });
}

#[inline(always)]
pub(crate) fn take_op_alloc_failure() -> bool {
    FAIL_OP_ALLOCS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_timer_alloc_failure() -> bool {
    FAIL_TIMER_ALLOCS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_raw_sqe_submit_failure() -> Option<io::Error> {
    FAIL_RAW_SQE_SUBMITS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            None
        } else {
            fails.set(remaining - 1);
            Some(io::Error::from(io::ErrorKind::WouldBlock))
        }
    })
}

#[inline(always)]
#[cfg(test)]
pub(crate) fn raw_sqe_submit_failures_remaining() -> usize {
    FAIL_RAW_SQE_SUBMITS.with(Cell::get)
}

#[inline(always)]
pub(crate) fn take_ring_submit_failure() -> Option<io::Error> {
    FAIL_RING_SUBMITS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            None
        } else {
            fails.set(remaining - 1);
            let errno = FAIL_RING_SUBMIT_ERRNO.with(|stored| stored.get());
            if errno == 0 {
                Some(io::Error::from(io::ErrorKind::WouldBlock))
            } else {
                Some(io::Error::from_raw_os_error(errno))
            }
        }
    })
}

#[inline(always)]
pub(crate) fn take_ring_wait_failure() -> Option<io::Error> {
    FAIL_RING_WAITS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            None
        } else {
            fails.set(remaining - 1);
            let errno = FAIL_RING_WAIT_ERRNO.with(|stored| stored.get());
            Some(io::Error::from_raw_os_error(errno))
        }
    })
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn ring_wait_failures_remaining() -> usize {
    FAIL_RING_WAITS.with(Cell::get)
}

/// Returns the number of injected ring-submit failures not yet consumed on
/// this thread.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn ring_submit_failures_remaining() -> usize {
    FAIL_RING_SUBMITS.with(Cell::get)
}

#[inline(always)]
#[cfg(debug_assertions)]
pub(crate) fn take_iobuff_pool_slab_alloc_failure() -> bool {
    FAIL_IOBUFF_POOL_SLAB_ALLOCS.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_reactor_ext_arg_probe_failure() -> bool {
    FAIL_REACTOR_EXT_ARG_PROBES.with(|fails| {
        let remaining = fails.get();
        if remaining == 0 {
            false
        } else {
            fails.set(remaining - 1);
            true
        }
    })
}

#[inline(always)]
pub(crate) fn take_reactor_shutdown_fallback() -> bool {
    FORCE_REACTOR_SHUTDOWN_FALLBACKS.with(|forces| {
        let remaining = forces.get();
        if remaining == 0 {
            false
        } else {
            forces.set(remaining - 1);
            true
        }
    })
}

/// Returns the number of forced reactor-shutdown fallbacks not yet consumed on
/// this thread.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn reactor_shutdown_fallbacks_remaining() -> usize {
    FORCE_REACTOR_SHUTDOWN_FALLBACKS.with(Cell::get)
}

#[cfg(feature = "test-support")]
pub(crate) mod bind_setup {
    use std::cell::Cell;
    use std::marker::PhantomData;
    use std::os::fd::RawFd;
    use std::rc::Rc;

    /// Socket constructor whose setup is being observed.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum BindTransport {
        /// TCP listener construction.
        Tcp,
        /// UDP socket construction.
        Udp,
        /// SCTP listener construction.
        Sctp,
    }

    /// One explicit socket setup boundary.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum BindStage {
        /// Socket creation.
        Socket,
        /// SCTP socket configuration.
        Configure,
        /// Address reuse setup.
        ReuseAddress,
        /// Port reuse setup.
        ReusePort,
        /// Binding the local address.
        Bind,
        /// Starting listener operation.
        Listen,
        /// Reading the assigned local address.
        LocalAddress,
    }

    /// Fixed-capacity record of one constructor invocation.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct BindSetupReport {
        /// Reached boundaries, followed by empty slots.
        pub trace: [Option<BindStage>; 6],
        /// Number of populated trace slots.
        pub trace_len: usize,
        /// Whether a seventh boundary was reached.
        pub trace_overflow: bool,
        /// Whether another transport used the active observation.
        pub unexpected_transport: bool,
        /// Descriptor acquired by the constructor, if creation succeeded.
        pub socket_fd: Option<RawFd>,
        /// Whether a second or negative acquired descriptor was reported.
        pub invalid_socket_report: bool,
        /// Number of injected failures, either zero or one.
        pub injections: u8,
        /// Whether the requested failure boundary was never reached.
        pub failure_pending: bool,
    }

    #[derive(Clone, Copy)]
    struct ProbeState {
        transport: BindTransport,
        fail_at: Option<(BindStage, i32)>,
        report: BindSetupReport,
    }

    thread_local! {
        static PROBE: Cell<Option<ProbeState>> = const { Cell::new(None) };
    }

    /// Owner-thread scope for one bounded setup observation.
    ///
    /// Dropping the scope clears only observation metadata. It never closes a
    /// descriptor or performs constructor cleanup.
    pub struct BindSetupProbe {
        active: bool,
        owner_thread: PhantomData<Rc<()>>,
    }

    /// Starts one observation, optionally failing one boundary once.
    ///
    /// # Panics
    ///
    /// Panics if a scope is already active or the requested errno is not positive.
    pub fn arm(transport: BindTransport, failure: Option<(BindStage, i32)>) -> BindSetupProbe {
        assert!(failure.is_none_or(|(_, errno)| errno > 0));
        PROBE.with(|slot| {
            assert!(
                slot.get().is_none(),
                "bind setup observation already active"
            );
            slot.set(Some(ProbeState {
                transport,
                fail_at: failure,
                report: BindSetupReport {
                    trace: [None; 6],
                    trace_len: 0,
                    trace_overflow: false,
                    unexpected_transport: false,
                    socket_fd: None,
                    invalid_socket_report: false,
                    injections: 0,
                    failure_pending: failure.is_some(),
                },
            }));
        });
        BindSetupProbe {
            active: true,
            owner_thread: PhantomData,
        }
    }

    impl BindSetupProbe {
        /// Ends this scope and returns its fixed-size observations.
        pub fn finish(mut self) -> BindSetupReport {
            let state = PROBE
                .with(Cell::take)
                .expect("active bind setup observation");
            self.active = false;
            state.report
        }
    }

    impl Drop for BindSetupProbe {
        fn drop(&mut self) {
            if self.active {
                PROBE.with(|slot| slot.set(None));
            }
        }
    }

    #[inline]
    pub(crate) fn before_stage(transport: BindTransport, stage: BindStage) -> Option<i32> {
        PROBE.with(|slot| {
            let mut state = slot.get()?;
            state.report.unexpected_transport |= state.transport != transport;
            if state.report.trace_len < state.report.trace.len() {
                state.report.trace[state.report.trace_len] = Some(stage);
                state.report.trace_len += 1;
            } else {
                state.report.trace_overflow = true;
            }
            let failure = match state.fail_at {
                Some((wanted, errno)) if state.transport == transport && wanted == stage => {
                    state.fail_at = None;
                    state.report.failure_pending = false;
                    state.report.injections += 1;
                    Some(errno)
                }
                _ => None,
            };
            slot.set(Some(state));
            failure
        })
    }

    #[inline]
    pub(crate) fn note_socket(transport: BindTransport, fd: RawFd) {
        PROBE.with(|slot| {
            if let Some(mut state) = slot.get() {
                state.report.unexpected_transport |= state.transport != transport;
                state.report.invalid_socket_report |= state.report.socket_fd.is_some() || fd < 0;
                state.report.socket_fd = Some(fd);
                slot.set(Some(state));
            }
        });
    }

    #[inline]
    pub(crate) fn raw_error(errno: i32) -> libc::c_int {
        // SAFETY: Linux exposes a writable errno cell for the calling thread.
        // The pointer is used only for this immediate store and is not retained.
        unsafe {
            *libc::__errno_location() = errno;
        }
        -1
    }
}
