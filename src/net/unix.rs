//! Unix stream transport with generic buffer support.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the per-message fast path:
//! - For steady-state stream I/O, [`UnixStream::read`] / [`UnixStream::write`]
//!   perform one contiguous submission and return short reads or writes to the
//!   caller.
//! - Use vectored APIs only when data is already segmented. For one
//!   contiguous payload, the contiguous APIs avoid iovec scratch.
//! - For fixed-shape hot-path buffers, pair Unix stream I/O with
//!   [`crate::runtime::buffer::pool::IoBuffPool`].
//!
//! Avoid on the per-message fast path:
//! - Avoid [`UnixStream::read_exact`] / [`UnixStream::write_all`]
//!   unless the protocol requires complete-buffer semantics. Use
//!   [`UnixStream::read`] / [`UnixStream::write`] instead when the caller can
//!   track progress explicitly.
//! - Avoid the immediate `try_*` methods as a readiness loop. They do not
//!   register a reactor waiter; use normal async I/O except at an
//!   already-expired deadline edge.
//!
//! The examples below often use `_all` / `_exact` variants because they keep
//! framing simple in documentation. On the hot path, prefer the partial-I/O
//! APIs when the caller can handle progress explicitly.
//!
//! The `try_*` methods are deadline-edge helpers that perform one immediate
//! nonblocking syscall without reactor registration or retry. Use the async
//! `read` / `write` APIs for normal FlowIO-managed Unix stream I/O.
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
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//! use flowio::runtime::executor::Executor;
//!
//! let mut pool = IoBuffPool::new(IoBuffPoolConfig {
//!     headroom: 0,
//!     payload: 64,
//!     tailroom: 0,
//!     objs_per_slab: 8,
//! }).unwrap();
//! pool.init();
//!
//! let mut executor = Executor::new()?;
//! executor.run(async move {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut buf = pool.alloc().unwrap();
//!     buf.payload_append(b"ping").unwrap();
//!     let (res, _) = left.write_all(buf).await;
//!     res.unwrap();
//!
//!     let recv = pool.alloc().unwrap();
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

use super::{WriteBufferChain, WritevProjection, stream};
use crate::runtime::buffer::iobuffvec::IoBuffVecMut;
use crate::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
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
        // SAFETY: `fds` has space for both descriptors required by socketpair.
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok((Self::from_raw_fd(fds[0]), Self::from_raw_fd(fds[1])))
    }

    /// Takes ownership of a Unix stream descriptor and closes it on drop.
    ///
    /// The caller must transfer sole descriptor ownership to FlowIO and must
    /// not close it or wrap the same raw descriptor in another owning handle.
    /// The descriptor must refer to a valid stream socket. Deadline-edge
    /// `try_*` methods additionally require it to be nonblocking.
    pub fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            fd: RuntimeFd::new(fd),
        }
    }

    stream::impl_stream_rw!(UnixStream, "flowio::net::unix::UnixStream");

    /// Sets the `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// stream setup instead of changing it per write.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_send_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        super::sock_send_buffer_size(self.fd.as_raw_fd())
    }

    /// Sets the `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// stream setup instead of changing it per read.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_recv_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        super::sock_recv_buffer_size(self.fd.as_raw_fd())
    }

    /// Shuts down the read, write, or both halves of this connection.
    ///
    /// This is connection control-plane work, normally used for teardown or
    /// protocol half-close rather than steady-state data transfer.
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
