//! Runtime-owned file descriptor wrapper with deferred-close drop behavior.
//!
//! `RuntimeFd` is intentionally thin: it owns only a raw fd and delegates
//! close-offload to the runtime when executor context is available. If the fd
//! is dropped outside the runtime or the reactor cannot accept another close
//! op, drop falls back to direct `close(2)`.

use crate::runtime::executor::try_submit_detached_close;
use std::os::fd::{AsRawFd, RawFd};

/// Thin owner for a descriptor managed by the runtime.
pub(crate) struct RuntimeFd
{
    fd: RawFd,
}

impl RuntimeFd
{
    const INVALID: RawFd = -1;

    #[inline(always)]
    pub const fn new(fd: RawFd) -> Self
    {
        Self { fd }
    }

    #[inline(always)]
    pub fn take_raw_fd(&mut self) -> RawFd
    {
        let fd = self.fd;
        self.fd = Self::INVALID;
        fd
    }
}

impl AsRawFd for RuntimeFd
{
    #[inline(always)]
    fn as_raw_fd(&self) -> RawFd
    {
        self.fd
    }
}

impl Drop for RuntimeFd
{
    fn drop(&mut self)
    {
        let fd = self.take_raw_fd();
        if fd < 0
        {
            return;
        }

        if !try_submit_detached_close(fd)
        {
            unsafe {
                libc::close(fd);
            }
        }
    }
}
