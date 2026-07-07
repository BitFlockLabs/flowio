//! Runtime-owned file descriptor wrapper with deferred-close drop behavior.
//!
//! `RuntimeFd` is intentionally thin: it owns only a raw fd and delegates
//! close-offload to the runtime when executor context is available. If the fd
//! is dropped outside the runtime or the reactor cannot accept another close
//! op, drop falls back to direct `close(2)`.

use crate::runtime::executor::try_submit_detached_close;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicI32, Ordering};

const DISTINCTIVE_TEST_FD_START: RawFd = 128;
const DISTINCTIVE_TEST_FD_STRIDE: RawFd = 8;
static NEXT_DISTINCTIVE_TEST_FD_BASE: AtomicI32 = AtomicI32::new(DISTINCTIVE_TEST_FD_START);

/// Thin owner for a descriptor managed by the runtime.
pub(crate) struct RuntimeFd {
    /// Raw descriptor value, or `INVALID` after ownership has been moved out.
    fd: RawFd,
}

impl RuntimeFd {
    const INVALID: RawFd = -1;

    #[inline(always)]
    pub const fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    #[inline(always)]
    /// Moves the raw descriptor out and leaves this wrapper empty.
    pub fn take_raw_fd(&mut self) -> RawFd {
        let fd = self.fd;
        self.fd = Self::INVALID;
        fd
    }
}

impl AsRawFd for RuntimeFd {
    #[inline(always)]
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for RuntimeFd {
    fn drop(&mut self) {
        let fd = self.take_raw_fd();
        if fd < 0 {
            return;
        }

        if !try_submit_detached_close(fd) {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

/// Creates a distinctive closeable fd for internal fd-close tests.
///
/// The duplicate is placed above the ordinary low fd range, within the current
/// soft fd limit, so parallel tests are unlikely to recycle the same numeric
/// descriptor before close assertions run.
#[doc(hidden)]
pub(crate) fn distinctive_closeable_test_fd() -> io::Result<RawFd> {
    let mut fds = [-1; 2];
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
pub(crate) fn raw_fd_is_closed(fd: RawFd) -> bool {
    let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    rc == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
}

#[inline(always)]
fn close_raw_fd(fd: RawFd) {
    unsafe {
        libc::close(fd);
    }
}

fn next_distinctive_test_fd_floor(fd: RawFd) -> RawFd {
    let candidate = NEXT_DISTINCTIVE_TEST_FD_BASE
        .fetch_add(DISTINCTIVE_TEST_FD_STRIDE, Ordering::Relaxed)
        .saturating_add(fd.rem_euclid(DISTINCTIVE_TEST_FD_STRIDE));
    match soft_open_fd_limit() {
        Some(limit) if limit > DISTINCTIVE_TEST_FD_START => candidate.min(limit - 1),
        _ => fd,
    }
}

fn soft_open_fd_limit() -> Option<RawFd> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if rc != 0 {
        return None;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return Some(RawFd::MAX);
    }
    RawFd::try_from(limit.rlim_cur).ok()
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

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
}
