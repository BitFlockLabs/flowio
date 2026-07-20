//! Runtime-owned file descriptor wrapper with provenance-aware close behavior.
//!
//! Internally created sockets start with known nonpositive `SO_LINGER` and can
//! use the reactor's batched close path without a terminal `getsockopt`.
//! External adoption, public raw-fd exposure, aliases, and uncertain listener
//! inheritance monotonically taint that proof. Uncertain sockets are classified
//! at terminal drop: positive or unclassifiable linger goes to the executor's
//! bounded close worker, while proven nonpositive linger normally uses the
//! reactor close path. A final listener owner released during CQ reclamation
//! uses a no-ring route. Outside an executor, drop preserves ordinary
//! direct-close behavior.

use crate::runtime::executor::{
    CloseAdmission, CloseSubmission, has_active_close_context, note_close_direct,
    note_close_linger_classification_failure, note_close_linger_query, note_close_linger_waiver,
    try_admit_close, try_submit_close,
};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::sync::Arc;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::AtomicI32;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(any(test, feature = "test-support"))]
const DISTINCTIVE_TEST_FD_START: RawFd = 128;
#[cfg(any(test, feature = "test-support"))]
const DISTINCTIVE_TEST_FD_STRIDE: RawFd = 8;
#[cfg(any(test, feature = "test-support"))]
static NEXT_DISTINCTIVE_TEST_FD_BASE: AtomicI32 = AtomicI32::new(DISTINCTIVE_TEST_FD_START);

/// Last trustworthy knowledge about a runtime socket's linger state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LingerProvenance {
    /// FlowIO created the socket and no operation capable of changing its
    /// shared socket options has occurred.
    KnownNonPositive,
    /// External code or a socket alias may have enabled positive linger.
    Uncertain,
}

/// Thin owner for a descriptor managed by the runtime.
pub(crate) struct RuntimeFd {
    /// Raw descriptor value, or `INVALID` after ownership has been moved out.
    fd: RawFd,
    /// Monotonic uncertainty bit. Atomic storage preserves the wrapper's
    /// existing `Send`/`Sync` auto traits when public raw access takes `&self`.
    linger_uncertain: AtomicBool,
}

impl RuntimeFd {
    const INVALID: RawFd = -1;

    #[inline(always)]
    pub(crate) const fn from_fresh_raw_fd(fd: RawFd) -> Self {
        Self {
            fd,
            linger_uncertain: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    pub(crate) fn from_fresh_owned(fd: OwnedFd) -> Self {
        Self::from_fresh_raw_fd(fd.into_raw_fd())
    }

    #[inline(always)]
    pub(crate) fn from_external_owned(fd: OwnedFd) -> Self {
        Self::from_owned_with_provenance(fd, LingerProvenance::Uncertain)
    }

    #[inline(always)]
    pub(crate) fn from_owned_with_provenance(fd: OwnedFd, provenance: LingerProvenance) -> Self {
        Self {
            fd: fd.into_raw_fd(),
            linger_uncertain: AtomicBool::new(provenance == LingerProvenance::Uncertain),
        }
    }

    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Returns the public raw descriptor and permanently invalidates the proof
    /// that shared socket options remain at their FlowIO-created defaults.
    #[inline(always)]
    pub(crate) fn expose_raw_fd(&self) -> RawFd {
        self.mark_linger_uncertain();
        self.fd
    }

    #[inline(always)]
    pub(crate) fn mark_linger_uncertain(&self) {
        self.linger_uncertain.store(true, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn linger_provenance(&self) -> LingerProvenance {
        if self.linger_uncertain.load(Ordering::Relaxed) {
            LingerProvenance::Uncertain
        } else {
            LingerProvenance::KnownNonPositive
        }
    }

    /// Moves the raw descriptor and its provenance out, leaving this wrapper
    /// empty.
    #[inline(always)]
    fn take_for_drop(&mut self) -> (RawFd, LingerProvenance) {
        let fd = self.fd;
        self.fd = Self::INVALID;
        (fd, self.linger_provenance())
    }

    /// Closes this owner without submitting a new SQE to the active ring.
    ///
    /// Retained listener owners can be released while the reactor still holds
    /// a mutable completion-queue view. Their final drop must not re-enter the
    /// same ring, but uncertain positive linger must still use the bounded
    /// close worker.
    fn close_without_ring(mut self) {
        let (fd, provenance) = self.take_for_drop();
        if fd < 0 {
            return;
        }

        // SAFETY: `take_for_drop` moved this wrapper's sole valid descriptor
        // ownership into the temporary owner.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };

        if !has_active_close_context() {
            drop(owned);
            return;
        }

        close_owned_in_active_context(owned, provenance, false);
    }
}

impl Drop for RuntimeFd {
    fn drop(&mut self) {
        let (fd, provenance) = self.take_for_drop();
        if fd < 0 {
            return;
        }

        // SAFETY: `take_for_drop` moved this wrapper's sole valid descriptor
        // ownership into the temporary owner.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };

        if !has_active_close_context() {
            drop(owned);
            return;
        }

        close_owned_in_active_context(owned, provenance, true);
    }
}

/// Listener owner retained by a readiness SQE.
///
/// TCP and SCTP listeners are owner-thread values while an accept operation is
/// live. If this retained reference is the final listener owner, it uses the
/// no-ring close route because orphan completion reclamation can drop it while
/// the reactor's completion queue is still borrowed.
pub(crate) struct RetainedListenerFd {
    fd: Option<Arc<RuntimeFd>>,
}

impl RetainedListenerFd {
    #[inline(always)]
    pub(crate) fn new(fd: &Arc<RuntimeFd>) -> Self {
        Self {
            fd: Some(Arc::clone(fd)),
        }
    }

    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.fd
            .as_ref()
            .map_or(RuntimeFd::INVALID, |fd| fd.raw_fd())
    }
}

impl Drop for RetainedListenerFd {
    fn drop(&mut self) {
        let Some(fd) = self.fd.take() else {
            return;
        };

        match Arc::try_unwrap(fd) {
            Ok(fd) => fd.close_without_ring(),
            Err(fd) => drop(fd),
        }
    }
}

fn close_owned_in_active_context(owned: OwnedFd, provenance: LingerProvenance, allow_ring: bool) {
    let route = match provenance {
        LingerProvenance::KnownNonPositive => CloseRoute::Ring,
        LingerProvenance::Uncertain => {
            note_close_linger_query();
            classify_close_linger(owned.as_raw_fd())
        }
    };
    close_owned_by_route(owned, route, allow_ring);
}

fn close_owned_by_route(owned: OwnedFd, route: CloseRoute, allow_ring: bool) {
    match route {
        CloseRoute::Ring => {
            if allow_ring {
                match try_submit_close(owned) {
                    CloseSubmission::Submitted => return,
                    CloseSubmission::OutsideExecutor(owned) | CloseSubmission::Rejected(owned) => {
                        close_direct(owned);
                        return;
                    }
                }
            } else {
                close_direct(owned);
                return;
            }
        }
        CloseRoute::Direct => {
            close_direct(owned);
            return;
        }
        CloseRoute::Unknown => note_close_linger_classification_failure(),
        CloseRoute::Worker => {}
    }

    match try_admit_close(owned) {
        CloseAdmission::Admitted => {}
        CloseAdmission::OutsideExecutor(owned) => drop(owned),
        CloseAdmission::Full(owned) | CloseAdmission::Disconnected(owned) => {
            match disable_linger_for_fallback(owned.as_raw_fd()) {
                Ok(()) => note_close_linger_waiver(true, false),
                Err(_) => note_close_linger_waiver(false, true),
            }
            close_direct(owned);
        }
    }
}

#[inline(always)]
fn close_direct(owned: OwnedFd) {
    note_close_direct();
    drop(owned);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseRoute {
    Ring,
    Direct,
    Worker,
    Unknown,
}

#[inline(always)]
fn classify_close_linger(fd: RawFd) -> CloseRoute {
    classify_linger_result(read_linger(fd))
}

#[inline(always)]
fn classify_linger_result(result: io::Result<libc::linger>) -> CloseRoute {
    match result {
        Ok(linger) => classify_linger_value(&linger),
        Err(err) if linger_is_unsupported_or_invalid(&err) => CloseRoute::Direct,
        Err(_) => CloseRoute::Unknown,
    }
}

fn read_linger(fd: RawFd) -> io::Result<libc::linger> {
    let mut linger = libc::linger {
        l_onoff: 0,
        l_linger: 0,
    };
    let mut len = std::mem::size_of::<libc::linger>() as libc::socklen_t;
    // SAFETY: `linger` and `len` provide writable storage of the exact option
    // type and size for this descriptor query.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::addr_of_mut!(linger).cast(),
            std::ptr::addr_of_mut!(len),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    if len as usize != std::mem::size_of::<libc::linger>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SO_LINGER returned an unexpected option length",
        ));
    }
    Ok(linger)
}

#[inline(always)]
fn classify_linger_value(linger: &libc::linger) -> CloseRoute {
    if linger.l_onoff != 0 && linger.l_linger > 0 {
        CloseRoute::Worker
    } else {
        CloseRoute::Ring
    }
}

#[inline(always)]
fn linger_is_unsupported_or_invalid(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::ENOTSOCK) | Some(libc::EBADF) | Some(libc::ENOPROTOOPT)
    )
}

/// Neutralizes linger after bounded worker admission fails.
///
/// Known-positive and unclassifiable states both reach this exceptional path.
/// Setting the option directly avoids depending on a second successful query;
/// failure is recorded before the same sole owner closes.
fn disable_linger_for_fallback(fd: RawFd) -> io::Result<()> {
    let disabled = libc::linger {
        l_onoff: 0,
        l_linger: 0,
    };
    // SAFETY: `disabled` is initialized and borrowed for the exact option
    // byte count; the kernel copies it during this call.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::addr_of!(disabled).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Creates a distinctive closeable fd for internal fd-close tests.
///
/// The duplicate is placed above the ordinary low fd range, within the current
/// soft fd limit, so parallel tests are unlikely to recycle the same numeric
/// descriptor before close assertions run.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn distinctive_closeable_test_fd() -> io::Result<RawFd> {
    let mut fds = [-1; 2];
    // SAFETY: `fds` provides writable storage for the two descriptors returned
    // by socketpair; no pointers outlive this call.
    let rc = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    let min_fd = next_distinctive_test_fd_floor(fds[0]);
    // SAFETY: `fds[0]` is an open descriptor from the successful socketpair,
    // and `min_fd` is bounded to the process soft fd limit when available.
    let test_fd = unsafe { libc::fcntl(fds[0], libc::F_DUPFD_CLOEXEC, min_fd) };
    let test_fd = if test_fd >= 0 {
        close_raw_fd(fds[0]);
        test_fd
    } else {
        fds[0]
    };
    close_raw_fd(fds[1]);
    Ok(test_fd)
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn raw_fd_is_closed(fd: RawFd) -> bool {
    // SAFETY: F_GETFD reads descriptor metadata and accepts any integer fd;
    // EBADF is the expected result for a closed descriptor.
    let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    rc == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
}

#[inline(always)]
#[cfg(any(test, feature = "test-support"))]
fn close_raw_fd(fd: RawFd) {
    // SAFETY: test helpers call this only after taking sole ownership of `fd`.
    unsafe {
        libc::close(fd);
    }
}

#[cfg(any(test, feature = "test-support"))]
fn next_distinctive_test_fd_floor(fd: RawFd) -> RawFd {
    let candidate = NEXT_DISTINCTIVE_TEST_FD_BASE
        .fetch_add(DISTINCTIVE_TEST_FD_STRIDE, Ordering::Relaxed)
        .saturating_add(fd.rem_euclid(DISTINCTIVE_TEST_FD_STRIDE));
    match soft_open_fd_limit() {
        Some(limit) if limit > DISTINCTIVE_TEST_FD_START => candidate.min(limit - 1),
        _ => fd,
    }
}

#[cfg(all(any(test, feature = "test-support"), not(miri)))]
fn soft_open_fd_limit() -> Option<RawFd> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is writable for the duration of getrlimit.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if rc != 0 {
        return None;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return Some(RawFd::MAX);
    }
    RawFd::try_from(limit.rlim_cur).ok()
}

#[cfg(all(any(test, feature = "test-support"), miri))]
fn soft_open_fd_limit() -> Option<RawFd> {
    // Miri does not emulate getrlimit. Falling back to the socketpair fd keeps
    // close-ownership coverage while avoiding an unsupported host query.
    None
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::runtime::executor::Executor;

    fn set_linger(fd: RawFd, onoff: libc::c_int, seconds: libc::c_int) {
        let linger = libc::linger {
            l_onoff: onoff,
            l_linger: seconds,
        };
        // SAFETY: linger is initialized and borrowed for the exact option
        // byte count during this call.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::addr_of!(linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "setsockopt(SO_LINGER) failed");
    }

    fn get_linger(fd: RawFd) -> libc::linger {
        let mut linger = libc::linger {
            l_onoff: 0,
            l_linger: 0,
        };
        let mut len = std::mem::size_of::<libc::linger>() as libc::socklen_t;
        // SAFETY: linger and len provide writable storage for this option.
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::addr_of_mut!(linger).cast(),
                std::ptr::addr_of_mut!(len),
            )
        };
        assert_eq!(rc, 0, "getsockopt(SO_LINGER) failed");
        assert_eq!(len as usize, std::mem::size_of::<libc::linger>());
        linger
    }

    #[test]
    fn distinctive_closeable_test_fd_does_not_immediately_reuse_closed_fd() {
        let first = distinctive_closeable_test_fd().expect("first distinctive fd failed");
        close_raw_fd(first);

        let second = distinctive_closeable_test_fd().expect("second distinctive fd failed");
        let reused_first = second == first;
        close_raw_fd(second);

        assert!(
            !reused_first,
            "distinctive test fd helper immediately reused a closed fd"
        );
        assert!(
            raw_fd_is_closed(first),
            "first distinctive fd should remain closed after second allocation"
        );
    }

    #[test]
    fn runtime_fd_drop_outside_executor_closes_directly() {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        drop(RuntimeFd::from_fresh_raw_fd(raw));
        assert!(
            raw_fd_is_closed(raw),
            "outside-executor RuntimeFd drop must close its sole descriptor"
        );
    }

    #[test]
    fn final_retained_listener_owner_closes_without_reentering_the_ring() {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        let listener = Arc::new(RuntimeFd::from_fresh_raw_fd(raw));
        let retained = RetainedListenerFd::new(&listener);
        drop(listener);

        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async move {
                drop(retained);
            })
            .expect("retained-listener close run failed");

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_ring_submissions, 0);
            assert_eq!(stats.close_direct_closes, 1);
            assert_eq!(stats.close_worker_admissions, 0);
        }
        assert!(
            raw_fd_is_closed(raw),
            "final retained listener owner must close its descriptor"
        );
    }

    #[test]
    fn unknown_linger_state_routes_conservatively_to_the_worker() {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        // SAFETY: the helper returned one open descriptor whose sole ownership
        // moves into this test owner.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut executor = Executor::new().expect("executor construction failed");

        executor
            .run(async move {
                close_owned_by_route(owned, CloseRoute::Unknown, true);
            })
            .expect("unknown-linger worker run failed");
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_direct_closes, 0);
            assert_eq!(stats.close_worker_admissions, 1);
            assert_eq!(stats.close_linger_classification_failures, 1);
        }
        drop(executor);
        assert!(raw_fd_is_closed(raw));
    }

    #[test]
    fn non_socket_descriptor_has_no_linger_and_closes_directly() {
        let mut fds = [-1; 2];
        // SAFETY: fds provides exact writable storage for both pipe owners.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
        assert_eq!(rc, 0, "pipe2 failed");
        // SAFETY: successful pipe2 returned two distinct sole owners.
        let endpoint = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let peer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        let raw = endpoint.as_raw_fd();
        assert_eq!(classify_close_linger(raw), CloseRoute::Direct);

        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async move {
                drop(RuntimeFd::from_external_owned(endpoint));
            })
            .expect("non-socket direct-close run failed");
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_direct_closes, 1);
            assert_eq!(stats.close_worker_admissions, 0);
        }
        assert!(raw_fd_is_closed(raw));
        drop(executor);
        drop(peer);
    }

    #[test]
    fn linger_classification_preserves_disabled_and_abortive_modes() {
        let mut fds = [-1; 2];
        // SAFETY: fds has exact writable space for a socketpair result.
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        assert_eq!(rc, 0, "socketpair failed");
        // SAFETY: successful socketpair returned two distinct sole owners.
        let endpoint = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let peer = unsafe { OwnedFd::from_raw_fd(fds[1]) };

        assert_eq!(
            classify_close_linger(endpoint.as_raw_fd()),
            CloseRoute::Ring
        );
        let disabled = get_linger(endpoint.as_raw_fd());
        assert_eq!(disabled.l_onoff, 0);

        set_linger(endpoint.as_raw_fd(), 1, 0);
        assert_eq!(
            classify_close_linger(endpoint.as_raw_fd()),
            CloseRoute::Ring
        );
        let abortive = get_linger(endpoint.as_raw_fd());
        assert_ne!(abortive.l_onoff, 0);
        assert_eq!(abortive.l_linger, 0);

        set_linger(endpoint.as_raw_fd(), 1, 2);
        assert_eq!(
            classify_close_linger(endpoint.as_raw_fd()),
            CloseRoute::Worker
        );
        disable_linger_for_fallback(endpoint.as_raw_fd()).unwrap();
        let waived = get_linger(endpoint.as_raw_fd());
        assert_eq!(waived.l_onoff, 0);

        drop(endpoint);
        drop(peer);
    }
}

#[cfg(test)]
mod policy_tests {
    use super::{
        CloseRoute, LingerProvenance, RuntimeFd, classify_linger_result, classify_linger_value,
    };
    use std::io;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn runtime_fd_preserves_send_and_sync_auto_traits() {
        assert_send_sync::<RuntimeFd>();
    }

    #[test]
    fn linger_provenance_is_monotonic_after_raw_exposure() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-1);
        assert_eq!(
            runtime.linger_provenance(),
            LingerProvenance::KnownNonPositive
        );
        assert_eq!(runtime.expose_raw_fd(), -1);
        assert_eq!(runtime.linger_provenance(), LingerProvenance::Uncertain);
        runtime.mark_linger_uncertain();
        assert_eq!(runtime.linger_provenance(), LingerProvenance::Uncertain);
    }

    #[test]
    fn only_enabled_positive_linger_requires_the_worker() {
        for linger in [
            libc::linger {
                l_onoff: 0,
                l_linger: 3,
            },
            libc::linger {
                l_onoff: 1,
                l_linger: 0,
            },
            libc::linger {
                l_onoff: 1,
                l_linger: -1,
            },
        ] {
            assert_eq!(classify_linger_value(&linger), CloseRoute::Ring);
        }
        assert_eq!(
            classify_linger_value(&libc::linger {
                l_onoff: 1,
                l_linger: 3,
            }),
            CloseRoute::Worker
        );
    }

    #[test]
    fn unsupported_or_invalid_descriptors_close_directly_but_other_failures_are_unknown() {
        for errno in [libc::ENOTSOCK, libc::EBADF, libc::ENOPROTOOPT] {
            assert_eq!(
                classify_linger_result(Err(io::Error::from_raw_os_error(errno))),
                CloseRoute::Direct
            );
        }
        assert_eq!(
            classify_linger_result(Err(io::Error::from_raw_os_error(libc::EIO))),
            CloseRoute::Unknown
        );
    }
}
