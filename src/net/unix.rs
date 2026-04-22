//! Unix stream transport with generic buffer support.
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - For steady-state stream I/O, [`UnixStream::read`] / [`UnixStream::write`]
//!   are the lowest-overhead contiguous APIs when the caller can handle short
//!   reads and writes.
//! - Use vectored APIs only when data is already segmented. For one
//!   contiguous payload, the contiguous APIs stay simpler and usually faster.
//! - For fixed-shape hot-path buffers, pair Unix stream I/O with
//!   [`crate::runtime::buffer::pool::IoBuffPool`].
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use [`UnixStream::read_exact`] / [`UnixStream::write_all`]
//!   unless the protocol requires complete-buffer semantics. Use
//!   [`UnixStream::read`] / [`UnixStream::write`] instead when the caller can
//!   track progress explicitly.
//!
//! The examples below often use `_all` / `_exact` variants because they keep
//! framing simple in documentation. On the hot path, prefer the partial-I/O
//! APIs when the caller can handle progress explicitly.
//!
//! # Example
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let (res, _buf) = left.write_all(b"ping".to_vec()).await;
//!     res.unwrap();
//!
//!     let (res, buf) = right.read_exact(vec![0u8; 4], 4).await;
//!     res.unwrap();
//!     assert_eq!(&buf[..], b"ping");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same operations work with [`IoBuffMut`] / [`IoBuff`]:
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
//!     buf.payload_append(b"ping").unwrap();
//!     let (res, _) = left.write_all(buf).await;
//!     res.unwrap();
//!
//!     let recv = IoBuffMut::new(0, 64, 0).unwrap();
//!     let (res, buf) = right.read_exact(recv, 4).await;
//!     res.unwrap();
//!     assert_eq!(buf.payload_bytes(), b"ping");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Vectored I/O with [`IoBuffVecMut`] / [`IoBuffVec`]:
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut seg1 = IoBuffMut::new(0, 32, 0).unwrap();
//!     seg1.payload_append(b"hello").unwrap();
//!     let mut seg2 = IoBuffMut::new(0, 32, 0).unwrap();
//!     seg2.payload_append(b" world").unwrap();
//!     let chain: IoBuffVec<2> = [seg1.freeze(), seg2.freeze()].into();
//!     let (res, _) = left.writev_all(chain).await;
//!     res.unwrap();
//!
//!     let recv = IoBuffVecMut::<2>::from_array([
//!         IoBuffMut::new(0, 6, 0).unwrap(),
//!         IoBuffMut::new(0, 5, 0).unwrap(),
//!     ]);
//!     let (res, chain) = right.readv_exact(recv, 11).await;
//!     res.unwrap();
//!     assert_eq!(chain.get(0).unwrap().payload_bytes(), b"hello ");
//!     assert_eq!(chain.get(1).unwrap().payload_bytes(), b"world");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! [`IoBuffMut`]: crate::runtime::buffer::IoBuffMut
//! [`IoBuff`]: crate::runtime::buffer::IoBuff
//! [`IoBuffVecMut`]: crate::runtime::buffer::iobuffvec::IoBuffVecMut
//! [`IoBuffVec`]: crate::runtime::buffer::iobuffvec::IoBuffVec

use super::stream;
use crate::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::fd::RuntimeFd;
use std::io;
use std::os::fd::{AsRawFd, RawFd};

/// Connected Unix stream.
///
/// On the steady-state fast path, keep the stream alive and reuse it for many
/// reads and writes rather than reconstructing it around each operation.
///
/// # Example
/// ```no_run
/// use flowio::net::unix::UnixStream;
/// use flowio::runtime::executor::Executor;
///
/// let mut executor = Executor::new()?;
/// executor.run(async {
///     let (mut left, mut right) = UnixStream::pair().unwrap();
///     let (res, _) = left.write_all(b"hello".to_vec()).await;
///     res.unwrap();
///     let (res, buf) = right.read_exact(vec![0u8; 5], 5).await;
///     res.unwrap();
///     assert_eq!(&buf[..], b"hello");
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct UnixStream {
    /// Owned runtime-managed Unix stream descriptor.
    fd: RuntimeFd,
}

impl UnixStream {
    /// Creates a connected Unix socket pair.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::unix::UnixStream;
    ///
    /// let (_left, _right) = UnixStream::pair()?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn pair() -> io::Result<(Self, Self)> {
        let mut fds = [0 as libc::c_int; 2];
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok((Self::from_raw_fd(fds[0]), Self::from_raw_fd(fds[1])))
    }

    /// Wraps an already-owned file descriptor in a runtime Unix stream.
    pub fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            fd: RuntimeFd::new(fd),
        }
    }

    /// Reads up to `len` bytes into `buffer`.
    ///
    /// The buffer is consumed and returned alongside the result on completion
    /// (rental pattern).  Up to `len` bytes will be read; the actual count is
    /// returned in the `Ok` variant.
    ///
    /// This is the lowest-overhead contiguous receive API when the caller can
    /// handle short reads and track framing itself.
    pub fn read<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> stream::ReadFuture<'_, B, Self> {
        stream::ReadFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Writes the initialized portion of `buffer`.
    ///
    /// The buffer is consumed and returned alongside the result on completion
    /// (rental pattern).  The number of bytes actually written is returned in
    /// the `Ok` variant.
    ///
    /// This is the lowest-overhead contiguous send API when the caller can
    /// handle short writes itself.
    pub fn write<B: IoBuffReadOnly>(&mut self, buffer: B) -> stream::WriteFuture<'_, B, Self> {
        stream::WriteFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Writes the entire buffer, handling partial writes internally.
    ///
    /// Returns `(Ok(n), buffer)` where `n` equals `buffer.len()` on
    /// success.  On error the buffer is returned with an unspecified amount
    /// already written.
    ///
    /// This is not the lowest-overhead send fast path because it may
    /// resubmit after partial writes. Prefer [`UnixStream::write`] when the
    /// caller can handle partial progress.
    pub fn write_all<B: IoBuffReadOnly>(
        &mut self,
        buffer: B,
    ) -> stream::WriteAllFuture<'_, B, Self> {
        stream::WriteAllFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Reads exactly `len` bytes into the buffer, handling partial reads.
    ///
    /// Returns `(Ok(len), buffer)` on success.  Returns `UnexpectedEof` if
    /// the peer closes before `len` bytes arrive.
    ///
    /// This is not the lowest-overhead receive fast path because it may
    /// resubmit after partial reads. Prefer [`UnixStream::read`] when the
    /// caller can handle partial progress.
    pub fn read_exact<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> stream::ReadExactFuture<'_, B, Self> {
        stream::ReadExactFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Scatter-read into a vectored buffer chain.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes read is returned in `Ok`.
    ///
    /// Use this when the receive path is already naturally segmented. For a
    /// single contiguous destination buffer, prefer [`UnixStream::read`].
    pub fn readv<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
    ) -> stream::ReadvFuture<'_, N, Self> {
        stream::ReadvFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write from a vectored buffer chain.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes written is returned in `Ok`.
    ///
    /// Use this when the send path is already naturally segmented. For one
    /// contiguous payload, prefer [`UnixStream::write`].
    pub fn writev<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
    ) -> stream::WritevFuture<'_, N, Self> {
        stream::WritevFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write the entire vectored chain, handling partial writes.
    ///
    /// Returns `(Ok(n), chain)` where `n` equals the total byte count on
    /// success.  On error the chain is returned with an unspecified amount
    /// already written.
    ///
    /// This is the complete-buffer vectored convenience API, not the
    /// lowest-overhead vectored send fast path. Prefer [`UnixStream::writev`]
    /// when the caller can handle partial progress.
    pub fn writev_all<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
    ) -> stream::WritevAllFuture<'_, N, Self> {
        stream::WritevAllFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Scatter-read exactly `len` total bytes into a vectored chain.
    ///
    /// Returns `(Ok(len), chain)` on success.  Returns `UnexpectedEof` if
    /// the peer closes before `len` bytes arrive.
    ///
    /// This is the complete-buffer vectored convenience API, not the
    /// lowest-overhead vectored receive fast path. Prefer [`UnixStream::readv`]
    /// when the caller can handle partial progress.
    pub fn readv_exact<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
        len: usize,
    ) -> stream::ReadvExactFuture<'_, N, Self> {
        stream::ReadvExactFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Sets the `SO_SNDBUF` socket send buffer size.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_send_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_SNDBUF` socket send buffer size.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        super::sock_send_buffer_size(self.fd.as_raw_fd())
    }

    /// Sets the `SO_RCVBUF` socket receive buffer size.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_recv_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_RCVBUF` socket receive buffer size.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        super::sock_recv_buffer_size(self.fd.as_raw_fd())
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        let rc = unsafe { libc::shutdown(self.fd.as_raw_fd(), how) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
