//! TCP transport with generic buffer support.
//!
//! Provides [`TcpListener`] for accepting connections and [`TcpConnector`]
//! for establishing outbound connections.  All read/write operations use the
//! rental pattern — the caller-provided buffer is consumed and returned
//! alongside the result on completion.
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - For repeated outbound connects, prefer [`TcpConnector`]. It reuses stable
//!   connect-slot state across attempts.
//! - For steady-state stream I/O, [`TcpStream::read`] / [`TcpStream::write`]
//!   are the lowest-overhead contiguous APIs when the caller can handle short
//!   reads and writes.
//! - For timeout-edge callers whose phase deadline has already reached
//!   `Duration::ZERO`, the `try_*` methods attempt one nonblocking syscall on
//!   the existing socket and return immediately with the rental buffer.
//! - Use vectored APIs only when data is already segmented. For one
//!   contiguous payload, the contiguous APIs stay simpler and usually faster.
//! - For fixed-shape hot-path buffers, pair TCP with
//!   [`crate::runtime::buffer::pool::IoBuffPool`].
//! - Use [`TcpStream::try_clone_for_split`] only during connection setup when
//!   separate read/write owners are needed. The handles share one kernel TCP
//!   stream.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use [`TcpStream::connect`] and
//!   [`TcpStream::connect_timeout`] in repeated outbound loops. Use
//!   [`TcpConnector`] instead.
//! - Prefer not to use [`TcpStream::read_exact`] / [`TcpStream::write_all`]
//!   unless the protocol requires complete-buffer semantics. Use
//!   [`TcpStream::read`] / [`TcpStream::write`] instead when the caller can
//!   track progress explicitly.
//!
//! The examples below often use `_all` / `_exact` variants because they keep
//! framing simple in documentation. On the hot path, prefer the partial-I/O
//! APIs when the caller can handle progress explicitly.
//!
//! # Example
//! ```no_run
//! use flowio::net::tcp::{TcpConnector, TcpListener};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let mut listener =
//!         TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).unwrap();
//!     let addr = listener.local_addr();
//!
//!     let _ = Executor::spawn(async move {
//!         let (mut stream, _peer) = listener.accept().await.unwrap();
//!         let recv = vec![0u8; 4];
//!         let (res, buf) = stream.read_exact(recv, 4).await;
//!         res.unwrap();
//!         assert_eq!(&buf[..], b"ping");
//!     });
//!
//!     let mut connector = TcpConnector::new();
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let (res, _buf) = stream.write_all(b"ping".to_vec()).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same operations work with [`IoBuffMut`] / [`IoBuff`] for zero-copy
//! buffer management:
//! ```no_run
//! use flowio::net::tcp::{TcpConnector, TcpListener};
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let mut listener =
//!         TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).unwrap();
//!     let addr = listener.local_addr();
//!
//!     let _ = Executor::spawn(async move {
//!         let (mut stream, _peer) = listener.accept().await.unwrap();
//!         let recv = IoBuffMut::new(0, 64, 0).unwrap();
//!         let (res, buf) = stream.read_exact(recv, 4).await;
//!         res.unwrap();
//!         assert_eq!(buf.payload_bytes(), b"ping");
//!     });
//!
//!     let mut connector = TcpConnector::new();
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
//!     buf.payload_append(b"ping").unwrap();
//!     let (res, _buf) = stream.write_all(buf).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Vectored I/O with [`IoBuffVecMut`] / [`IoBuffVec`]:
//! ```no_run
//! use flowio::net::tcp::{TcpConnector, TcpListener};
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let mut listener =
//!         TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).unwrap();
//!     let addr = listener.local_addr();
//!
//!     let _ = Executor::spawn(async move {
//!         let (mut stream, _peer) = listener.accept().await.unwrap();
//!         let recv = IoBuffVecMut::<2>::from_array([
//!             IoBuffMut::new(0, 5, 0).unwrap(),
//!             IoBuffMut::new(0, 6, 0).unwrap(),
//!         ]);
//!         let (res, chain) = stream.readv_exact(recv, 11).await;
//!         res.unwrap();
//!         assert_eq!(chain.get(0).unwrap().payload_bytes(), b"hello");
//!         assert_eq!(chain.get(1).unwrap().payload_bytes(), b" world");
//!     });
//!
//!     let mut connector = TcpConnector::new();
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let mut seg1 = IoBuffMut::new(0, 32, 0).unwrap();
//!     seg1.payload_append(b"hello").unwrap();
//!     let mut seg2 = IoBuffMut::new(0, 32, 0).unwrap();
//!     seg2.payload_append(b" world").unwrap();
//!     let chain: IoBuffVec<2> = [seg1.freeze(), seg2.freeze()].into();
//!     let (res, _chain) = stream.writev_all(chain).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! [`IoBuffMut`]: crate::runtime::buffer::IoBuffMut
//! [`IoBuff`]: crate::runtime::buffer::IoBuff
//! [`IoBuffVecMut`]: crate::runtime::buffer::iobuffvec::IoBuffVecMut
//! [`IoBuffVec`]: crate::runtime::buffer::iobuffvec::IoBuffVec
//!
//! Timed repeated connects use the reusable connector plus the runtime timer
//! wheel:
//! ```no_run
//! use flowio::net::tcp::TcpConnector;
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//! use std::time::Duration;
//!
//! let mut executor = Executor::new()?;
//! let mut connector = TcpConnector::new();
//! executor.run(async move {
//!     let _ = connector.connect_timeout(
//!         SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
//!         Duration::from_secs(1),
//!     )
//!     .unwrap()
//!     .await;
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use super::stream;
use super::{
    WritevProjection, close_fd, close_if_valid, current_local_addr, current_peer_addr,
    get_sock_opt, new_nonblocking_socket, set_reuse_addr, set_reuse_port, set_sock_opt,
    socket_addr_from_c, socket_addr_to_c, socket_domain,
};
use crate::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    drop_op_ptr_unchecked, poll_ctx_from_waker, refresh_op_waiter_from_waker, submit_retained_sqe,
};
use crate::runtime::fd::RuntimeFd;
use crate::runtime::op::CompletionState;
use crate::runtime::timer::{Elapsed, Timeout, timeout};
use io_uring::{opcode, types};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

// ---------------------------------------------------------------------------
// AcceptSlot / ConnectSlot
// ---------------------------------------------------------------------------

/// Reusable accept-side submission state kept by [`TcpListener`].
struct AcceptSlot {
    /// Completion state for the current or last accept submission.
    state_ptr: *mut CompletionState,
    /// True while an [`AcceptFuture`] is borrowing this slot.
    in_use: bool,
}

impl AcceptSlot {
    fn new() -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
        }
    }

    fn prepare(&mut self) -> io::Result<()> {
        if self.in_use || !self.state_ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.in_use = true;
        Ok(())
    }

    fn drop_future(&mut self) {
        if !self.state_ptr.is_null() {
            unsafe {
                if (*self.state_ptr).is_completed() && (*self.state_ptr).result >= 0 {
                    close_fd((*self.state_ptr).result as RawFd);
                }
                drop_op_ptr_unchecked(&mut self.state_ptr);
            }
        }

        self.in_use = false;
    }

    fn drop_cached_state(&mut self) {
        // Normal safe use drops AcceptFuture before TcpListener. This also
        // handles safe `mem::forget(AcceptFuture)` teardown, where the slot can
        // still hold an in-flight or completed accept state when the listener
        // is finally dropped. A completed accepted fd is owned by this slot and
        // must be closed before the cached state is released.
        self.drop_future();
    }

    fn poll_accept(
        &mut self,
        fd: RawFd,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        if !self.state_ptr.is_null() {
            let state = unsafe { &*self.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                let payload = unsafe {
                    (*pctx.reactor()).take_retained_payload::<RetainedAcceptAddr>(self.state_ptr)
                };
                unsafe { (*pctx.reactor()).free_op(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                if result < 0 {
                    return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                }
                let remote_addr = match socket_addr_from_c(&payload.addr, payload.addrlen) {
                    Ok(addr) => addr,
                    Err(err) => {
                        close_fd(result as RawFd);
                        return Poll::Ready(Err(err));
                    }
                };
                return Poll::Ready(Ok((TcpStream::from_raw_fd(result as RawFd), remote_addr)));
            }
        }

        if self.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                self.in_use = false;
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            self.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedAcceptAddr::new();

            unsafe {
                (*state_ptr).set_close_result_fd_on_orphan();
                if let Err((e, _payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let sqe = opcode::Accept::new(
                            types::Fd(fd),
                            payload.addr_ptr_mut(),
                            payload.addrlen_ptr_mut(),
                        )
                        .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
                        .build()
                        .user_data(state_ptr as u64);
                        Ok(sqe)
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    return Poll::Ready(Err(e));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, self.state_ptr) };
        Poll::Pending
    }
}

/// Reusable connect-side submission state kept by [`TcpConnector`].
struct ConnectSlot {
    /// Completion state for the current or last connect submission.
    state_ptr: *mut CompletionState,
    /// True while a [`ConnectFuture`] is borrowing this slot.
    in_use: bool,
    /// Socket being connected for the current attempt.
    fd: RawFd,
    /// Prepared remote address for the current attempt.
    addr: Option<RetainedConnectAddr>,
}

impl ConnectSlot {
    fn new() -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
            fd: -1,
            addr: None,
        }
    }

    fn prepare(&mut self, addr: SocketAddr) -> io::Result<()> {
        if self.in_use || !self.state_ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.cleanup_fd();
        self.in_use = true;
        self.fd = match new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM) {
            Ok(fd) => fd,
            Err(err) => {
                self.in_use = false;
                return Err(err);
            }
        };
        self.addr = Some(RetainedConnectAddr::from_socket_addr(addr));
        Ok(())
    }

    fn cleanup_fd(&mut self) {
        close_if_valid(&mut self.fd);
    }

    fn take_stream(&mut self) -> TcpStream {
        let fd = self.fd;
        self.fd = -1;
        TcpStream::from_raw_fd(fd)
    }

    fn drop_future(&mut self) {
        if !self.state_ptr.is_null() {
            unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        }

        self.addr = None;
        self.cleanup_fd();
        self.in_use = false;
    }

    fn drop_cached_state(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.in_use = false;
    }

    fn poll_connect(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<TcpStream>> {
        if !self.state_ptr.is_null() {
            let state = unsafe { &*self.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    self.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
                return Poll::Ready(Ok(self.take_stream()));
            }
        }

        if self.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                self.in_use = false;
                self.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            self.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = match self.addr.take() {
                Some(payload) => payload,
                None => {
                    unsafe { (*pctx.reactor()).free_op(state_ptr) };
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::InvalidInput)));
                }
            };

            unsafe {
                if let Err((e, _payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let sqe = opcode::Connect::new(
                            types::Fd(self.fd),
                            payload.addr_ptr(),
                            payload.addrlen,
                        )
                        .build()
                        .user_data(state_ptr as u64);
                        Ok(sqe)
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(e));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, self.state_ptr) };
        Poll::Pending
    }
}

struct RetainedAcceptAddr {
    /// Kernel-written peer address storage for the accepted connection.
    addr: libc::sockaddr_storage,
    /// Address buffer length passed to and updated by `accept`.
    addrlen: libc::socklen_t,
}

impl RetainedAcceptAddr {
    fn new() -> Self {
        Self {
            addr: unsafe { std::mem::zeroed() },
            addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        }
    }

    fn addr_ptr_mut(&mut self) -> *mut libc::sockaddr {
        &mut self.addr as *mut libc::sockaddr_storage as *mut libc::sockaddr
    }

    fn addrlen_ptr_mut(&mut self) -> *mut libc::socklen_t {
        &mut self.addrlen
    }
}

#[derive(Clone, Copy)]
struct RetainedConnectAddr {
    /// Prepared peer address retained until connect completion.
    addr: libc::sockaddr_storage,
    /// Length of the prepared peer address.
    addrlen: libc::socklen_t,
}

impl RetainedConnectAddr {
    fn from_socket_addr(addr: SocketAddr) -> Self {
        let (addr, addrlen) = socket_addr_to_c(addr);
        Self { addr, addrlen }
    }

    fn addr_ptr(&self) -> *const libc::sockaddr {
        &self.addr as *const libc::sockaddr_storage as *const libc::sockaddr
    }
}

// ---------------------------------------------------------------------------
// TcpStream
// ---------------------------------------------------------------------------

/// Connected TCP stream.
///
/// Obtained from [`TcpListener::accept`], [`TcpConnector::connect`], or
/// [`TcpStream::connect`].
///
/// On the steady-state fast path, keep the stream alive and reuse it for many
/// reads and writes rather than reconnecting repeatedly.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::TcpStream;
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut executor = Executor::new()?;
/// executor.run(async move {
///     let mut stream = TcpStream::connect(
///         SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
///     ).unwrap().await.unwrap();
///     let (res, _) = stream.write_all(b"hello".to_vec()).await;
///     res.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct TcpStream {
    /// Owned runtime-managed connected socket descriptor.
    fd: RuntimeFd,
}

impl TcpStream {
    /// Wraps an already-owned connected socket.
    ///
    /// FlowIO-created TCP sockets are nonblocking. Callers that pass an
    /// external descriptor must preserve that invariant; the `try_*`
    /// deadline-edge APIs rely on the fd already being nonblocking.
    pub fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            fd: RuntimeFd::new(fd),
        }
    }

    /// Returns the local address of this socket.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        current_local_addr(self.fd.as_raw_fd())
    }

    /// Duplicates this connected stream descriptor for explicit read/write
    /// split ownership.
    ///
    /// The duplicate is a separate runtime-owned descriptor referring to the
    /// same underlying socket. Use this during connection setup when one task
    /// needs to own reads while another owns writes. This is control-plane
    /// setup work, not a per-message fast-path operation. Dropping one handle
    /// closes only that descriptor; the underlying TCP stream remains open
    /// while another duplicated handle is alive.
    pub fn try_clone_for_split(&self) -> io::Result<Self> {
        let fd = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self::from_raw_fd(fd))
    }

    /// Returns the peer address of this socket.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        current_peer_addr(self.fd.as_raw_fd())
    }

    /// Enables or disables `TCP_NODELAY` (Nagle's algorithm).
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// connection setup instead of toggling it per message.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &(nodelay as libc::c_int),
        )
    }

    /// Returns the current `TCP_NODELAY` setting.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn nodelay(&self) -> io::Result<bool> {
        let val: libc::c_int =
            get_sock_opt(self.fd.as_raw_fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY)?;
        Ok(val != 0)
    }

    /// Enables or disables `SO_KEEPALIVE`.
    ///
    /// This only toggles the socket-level keepalive flag. Platform-specific
    /// keepalive probe intervals and counts are not configured by this method.
    /// Apply it during connection setup instead of toggling it per message.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &(keepalive as libc::c_int),
        )
    }

    /// Sets the `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// connection setup instead of changing it per write.
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
    /// connection setup instead of changing it per read.
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

    /// Attempts one nonblocking read syscall and returns immediately.
    ///
    /// This is a deadline-edge primitive for callers whose phase timeout has
    /// already reached `Duration::ZERO`. It does not submit an `io_uring`
    /// operation, register a waiter, park, retry, or allocate. If no data is
    /// immediately available on the existing nonblocking socket, it returns
    /// [`io::ErrorKind::WouldBlock`] and returns `buffer` unchanged.
    ///
    /// Prefer [`TcpStream::read`] for normal FlowIO async I/O.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpStream;
    ///
    /// # fn deadline_edge_read(mut stream: TcpStream) {
    /// let (result, buffer) = stream.try_read(vec![0u8; 1024], 1024);
    /// if let Ok(n) = result {
    ///     let _bytes = &buffer[..n];
    /// }
    /// # }
    /// ```
    pub fn try_read<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> (io::Result<usize>, B) {
        stream::try_read_once(self.fd.as_raw_fd(), buffer, len)
    }

    /// Attempts one nonblocking read syscall into the current payload tail.
    ///
    /// On success, only the bytes actually read are appended to `buffer`.
    /// Existing payload bytes are preserved. If no data is immediately
    /// available, this returns [`io::ErrorKind::WouldBlock`] and leaves the
    /// payload length unchanged.
    ///
    /// This is a deadline-edge primitive, not a replacement for
    /// [`TcpStream::read_exact_append`] in normal async protocol flow.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpStream;
    /// use flowio::runtime::buffer::IoBuffMut;
    ///
    /// # fn deadline_edge_append(mut stream: TcpStream, buffer: IoBuffMut) {
    /// let (result, buffer) = stream.try_read_append(buffer, 128);
    /// if result.is_err() {
    ///     let _retry_later = buffer;
    /// }
    /// # }
    /// ```
    pub fn try_read_append(
        &mut self,
        buffer: IoBuffMut,
        len: usize,
    ) -> (io::Result<usize>, IoBuffMut) {
        stream::try_read_append_once(self.fd.as_raw_fd(), buffer, len)
    }

    /// Attempts one nonblocking write syscall and returns immediately.
    ///
    /// This sends from the initialized bytes in `buffer` with no reactor
    /// registration and no retry. If the socket cannot accept bytes now, it
    /// returns [`io::ErrorKind::WouldBlock`] and returns `buffer` unchanged.
    ///
    /// Prefer [`TcpStream::write`] for normal FlowIO async I/O.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpStream;
    ///
    /// # fn deadline_edge_write(mut stream: TcpStream) {
    /// let (result, buffer) = stream.try_write(b"ping".to_vec());
    /// if result.is_err() {
    ///     let _retry_later = buffer;
    /// }
    /// # }
    /// ```
    pub fn try_write<B: IoBuffReadOnly + 'static>(&mut self, buffer: B) -> (io::Result<usize>, B) {
        stream::try_write_once(self.fd.as_raw_fd(), buffer)
    }

    /// Attempts one nonblocking projected gather-write syscall.
    ///
    /// FlowIO projects borrowed byte pieces from the owned `source` into
    /// bounded stack-owned `iovec` scratch, performs one `sendmsg`, and
    /// returns the source immediately. Message bytes are not copied, and no
    /// retained operation state is created. Projections above 1024 non-empty
    /// pieces are rejected with [`io::ErrorKind::InvalidInput`].
    ///
    /// This is a deadline-edge primitive. Prefer
    /// [`TcpStream::writev_projected`] / [`TcpStream::writev_all_projected`]
    /// for normal FlowIO async I/O.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpStream;
    /// use flowio::net::{WritevPieces, WritevProjection};
    /// use std::io;
    ///
    /// struct Frame {
    ///     header: [u8; 2],
    ///     body: Vec<u8>,
    /// }
    ///
    /// impl WritevProjection for Frame {
    ///     fn writev_count_and_len(&self) -> (usize, usize) {
    ///         (2, self.header.len() + self.body.len())
    ///     }
    ///
    ///     fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
    ///         pieces.push(&self.header)?;
    ///         pieces.push(&self.body)?;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # fn deadline_edge_projected(mut stream: TcpStream) {
    /// let frame = Frame {
    ///     header: *b"H:",
    ///     body: b"ping".to_vec(),
    /// };
    /// let (result, frame) = stream.try_writev_projected(frame);
    /// if result.is_err() {
    ///     let _retry_later = frame;
    /// }
    /// # }
    /// ```
    pub fn try_writev_projected<T: WritevProjection>(
        &mut self,
        source: T,
    ) -> (io::Result<usize>, T) {
        stream::try_writev_projected_once(self.fd.as_raw_fd(), source)
    }

    /// Reads up to `len` bytes into `buffer`.
    ///
    /// The buffer is consumed and returned alongside the result on completion
    /// (rental pattern); the actual byte count is returned in the `Ok` variant.
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
    /// (rental pattern); the actual byte count is returned in the `Ok` variant.
    ///
    /// This is the lowest-overhead contiguous send API when the caller can
    /// handle short writes itself.
    pub fn write<B: IoBuffReadOnly + 'static>(
        &mut self,
        buffer: B,
    ) -> stream::WriteFuture<'_, B, Self> {
        stream::WriteFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Writes the entire buffer, handling partial writes internally.
    ///
    /// Returns `(Ok(n), buffer)` where `n` equals `buffer.len()` on success.
    /// On error the buffer is returned with an unspecified amount already
    /// written.
    ///
    /// This is the complete-buffer convenience API, not the lowest-overhead
    /// send fast path, because it may resubmit after partial writes. Prefer
    /// [`TcpStream::write`] when the caller can handle partial progress.
    pub fn write_all<B: IoBuffReadOnly + 'static>(
        &mut self,
        buffer: B,
    ) -> stream::WriteAllFuture<'_, B, Self> {
        stream::WriteAllFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Reads exactly `len` bytes, handling partial reads internally.
    ///
    /// Returns `(Ok(len), buffer)` on success. Returns `UnexpectedEof` if the
    /// peer closes before `len` bytes arrive.
    ///
    /// This is not the lowest-overhead receive fast path because it may
    /// resubmit after partial reads. Prefer [`TcpStream::read`] when the
    /// caller can handle partial progress.
    pub fn read_exact<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> stream::ReadExactFuture<'_, B, Self> {
        stream::ReadExactFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Appends exactly `len` bytes to the current payload end of `buffer`.
    ///
    /// Returns `UnexpectedEof` if the peer closes before `len` bytes arrive.
    /// On success the returned buffer payload length is the original payload
    /// length plus `len`; on EOF or error it includes any bytes appended before
    /// completion.
    ///
    /// This preserves [`TcpStream::read_exact`] semantics while supporting
    /// staged protocol reads into one [`IoBuffMut`].
    ///
    /// This is not the lowest-overhead receive fast path because it may
    /// resubmit after partial reads. Prefer [`TcpStream::read`] when the
    /// caller can handle partial progress and manage staged framing directly.
    pub fn read_exact_append(
        &mut self,
        buffer: IoBuffMut,
        len: usize,
    ) -> stream::ReadExactAppendFuture<'_, Self> {
        stream::ReadExactAppendFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Scatter-read into a vectored buffer chain.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes read is returned in `Ok`.
    ///
    /// Use this when the receive path is already naturally segmented. For a
    /// single contiguous destination buffer, prefer [`TcpStream::read`].
    ///
    /// # Errors
    /// Returns `InvalidInput` if the chain has no writable segments.
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
    /// Empty chains complete with `Ok(0)` without submitting kernel I/O.
    ///
    /// Use this when the send path is already naturally segmented. For one
    /// contiguous payload, prefer [`TcpStream::write`].
    pub fn writev<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
    ) -> stream::WritevFuture<'_, N, Self> {
        stream::WritevFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write from a generic read-only vectored buffer chain.
    ///
    /// The chain owns buffers implementing [`IoBuffReadOnly`] and is returned
    /// alongside the result. This is the zero-copy send path for already
    /// encoded non-FlowIO buffer segments. Empty chains complete with `Ok(0)`
    /// without submitting kernel I/O.
    ///
    /// Use this when the send path is already naturally segmented. For one
    /// contiguous payload, prefer [`TcpStream::write`].
    pub fn writev_read_only<B: IoBuffReadOnly + 'static, const N: usize>(
        &mut self,
        buffer: IoBuffReadOnlyVec<B, N>,
    ) -> stream::WritevReadOnlyFuture<'_, B, N, Self> {
        stream::WritevReadOnlyFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write projected pieces from one compact owned source.
    ///
    /// FlowIO retains `source`, then projects borrowed byte slices from that
    /// retained source into retained kernel-facing `iovec` scratch. This is
    /// the zero-copy send path for protocols with one compact owner/carrier
    /// and many already-encoded pieces. Empty projections complete with
    /// `Ok(0)` without submitting kernel I/O.
    ///
    /// Use this when the send path is already naturally segmented inside the
    /// retained carrier. For one contiguous payload, prefer [`TcpStream::write`].
    pub fn writev_projected<T: WritevProjection>(
        &mut self,
        source: T,
    ) -> stream::WritevProjectedFuture<'_, T, Self> {
        stream::WritevProjectedFuture::new(self.fd.as_raw_fd(), source)
    }

    /// Gather-write the entire vectored chain, handling partial writes.
    ///
    /// Returns `(Ok(n), chain)` where `n` equals the total byte count on
    /// success.  On error the chain is returned with an unspecified amount
    /// already written. Empty chains complete with `Ok(0)` without submitting
    /// kernel I/O.
    ///
    /// This is the complete-buffer vectored convenience API, not the
    /// lowest-overhead vectored send fast path. Prefer [`TcpStream::writev`]
    /// when the caller can handle partial progress.
    pub fn writev_all<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
    ) -> stream::WritevAllFuture<'_, N, Self> {
        stream::WritevAllFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write the entire generic read-only vectored chain.
    ///
    /// Returns `(Ok(n), chain)` where `n` equals the total byte count on
    /// success. The future materializes `iovec` scratch once and advances it
    /// in place across partial writes. Empty chains complete with `Ok(0)`
    /// without submitting kernel I/O.
    ///
    /// This is the complete-buffer convenience API. Prefer
    /// [`TcpStream::writev_read_only`] when the caller can handle partial
    /// progress.
    pub fn writev_all_read_only<B: IoBuffReadOnly + 'static, const N: usize>(
        &mut self,
        buffer: IoBuffReadOnlyVec<B, N>,
    ) -> stream::WritevAllReadOnlyFuture<'_, B, N, Self> {
        stream::WritevAllReadOnlyFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write all projected pieces from one compact owned source.
    ///
    /// Returns `(Ok(n), source)` where `n` equals the projected total byte
    /// count on success. On error the source is returned with an unspecified
    /// amount already written. Empty projections complete with `Ok(0)` without
    /// submitting kernel I/O.
    ///
    /// This is the complete-buffer convenience API. Prefer
    /// [`TcpStream::writev_projected`] when the caller can handle partial
    /// progress.
    pub fn writev_all_projected<T: WritevProjection>(
        &mut self,
        source: T,
    ) -> stream::WritevAllProjectedFuture<'_, T, Self> {
        stream::WritevAllProjectedFuture::new(self.fd.as_raw_fd(), source)
    }

    /// Scatter-read exactly `len` total bytes into a vectored chain.
    ///
    /// Returns `(Ok(len), chain)` on success.  Returns `UnexpectedEof` if
    /// the peer closes before `len` bytes arrive. A zero `len` completes with
    /// `Ok(0)` without submitting kernel I/O.
    ///
    /// This is the complete-buffer vectored convenience API, not the
    /// lowest-overhead vectored receive fast path. Prefer [`TcpStream::readv`]
    /// when the caller can handle partial progress.
    pub fn readv_exact<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
        len: usize,
    ) -> stream::ReadvExactFuture<'_, N, Self> {
        stream::ReadvExactFuture::new(self.fd.as_raw_fd(), buffer, len)
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// TcpConnector
// ---------------------------------------------------------------------------

impl TcpStream {
    /// Convenience method that creates a one-shot connection to the given
    /// address. For repeated connections, use [`TcpConnector`] to reuse the
    /// connector's slot metadata across attempts.
    ///
    /// This is not the repeated-connect fast path.
    pub fn connect(addr: SocketAddr) -> io::Result<OwnedConnectFuture> {
        OwnedConnectFuture::new(addr)
    }

    /// Convenience method that connects to the given address with a deadline.
    ///
    /// Returns `TimedOut` if the connection does not complete before the
    /// provided duration elapses. For repeated outbound connections, prefer
    /// [`TcpConnector::connect_timeout`] so the connector can reuse its slot
    /// metadata across attempts.
    ///
    /// This is the convenience timeout API, not the repeated-connect fast
    /// path.
    pub fn connect_timeout(
        addr: SocketAddr,
        timeout_duration: Duration,
    ) -> io::Result<OwnedConnectTimeoutFuture> {
        Ok(OwnedConnectTimeoutFuture {
            inner: timeout(timeout_duration, Self::connect(addr)?),
        })
    }
}

/// TCP connector that reuses one connect slot across attempts.
///
/// The slot keeps stable socket/address metadata across attempts. Each
/// individual connect submission still uses the reactor completion-state pool.
///
/// This is the best TCP API to use on the repeated outbound connect fast path.
/// Prefer [`TcpStream::connect`] only for occasional convenience connects.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::TcpConnector;
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut executor = Executor::new()?;
/// let mut connector = TcpConnector::new();
/// executor.run(async move {
///     let mut stream = connector
///         .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)))
///         .unwrap().await.unwrap();
///     let (res, _) = stream.write_all(b"hello".to_vec()).await;
///     res.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct TcpConnector {
    /// Reusable connect state kept across connection attempts.
    connect_slot: ConnectSlot,
}

/// Equivalent to [`TcpConnector::new()`].
impl Default for TcpConnector {
    fn default() -> Self {
        Self {
            connect_slot: ConnectSlot::new(),
        }
    }
}

impl TcpConnector {
    /// Creates a new connector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts connecting to the provided remote address.
    ///
    /// This is the repeated-connect fast-path API. Prefer
    /// [`TcpStream::connect`] only for occasional convenience connects.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<ConnectFuture<'_>> {
        self.connect_slot.prepare(addr)?;
        Ok(ConnectFuture {
            slot: &mut self.connect_slot,
        })
    }

    /// Starts connecting to the provided remote address with a deadline.
    ///
    /// Returns `TimedOut` if the connection does not complete before the
    /// provided duration elapses. This is the repeated-connect timeout API;
    /// prefer it over [`TcpStream::connect_timeout`] when the same connector
    /// is reused across attempts.
    pub fn connect_timeout(
        &mut self,
        addr: SocketAddr,
        timeout_duration: Duration,
    ) -> io::Result<ConnectTimeoutFuture<'_>> {
        Ok(ConnectTimeoutFuture {
            inner: timeout(timeout_duration, self.connect(addr)?),
        })
    }
}

impl Drop for TcpConnector {
    fn drop(&mut self) {
        self.connect_slot.drop_cached_state();
        self.connect_slot.cleanup_fd();
    }
}

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------

/// Listening TCP socket with a reusable accept slot.
///
/// Listener creation (bind/listen) and `accept` are connection setup /
/// control-plane work, not a per-message data fast path.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::TcpListener;
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut listener = TcpListener::bind(
///     SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
///     128,
/// )?;
/// let mut executor = Executor::new()?;
/// executor.run(async move {
///     let (mut stream, _peer) = listener.accept().await.unwrap();
///     let (res, _buf) = stream.read_exact(vec![0u8; 4], 4).await;
///     res.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct TcpListener {
    /// Owned listening socket descriptor.
    fd: RuntimeFd,
    /// Cached local address assigned after bind/listen.
    local_addr: SocketAddr,
    /// Reusable accept state kept across accepts.
    accept_slot: AcceptSlot,
}

impl TcpListener {
    /// Binds a nonblocking listener with `SO_REUSEADDR` and starts listening.
    pub fn bind(addr: SocketAddr, backlog: i32) -> io::Result<Self> {
        Self::bind_inner(addr, backlog, false)
    }

    /// Binds a nonblocking listener with both `SO_REUSEADDR` and
    /// `SO_REUSEPORT`, then starts listening.  Useful for multi-process
    /// server patterns where multiple listeners share the same port.
    pub fn bind_reuse_port(addr: SocketAddr, backlog: i32) -> io::Result<Self> {
        Self::bind_inner(addr, backlog, true)
    }

    fn bind_inner(addr: SocketAddr, backlog: i32, reuse_port: bool) -> io::Result<Self> {
        let fd = new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM)?;

        if let Err(err) = set_reuse_addr(fd) {
            close_fd(fd);
            return Err(err);
        }

        if reuse_port && let Err(err) = set_reuse_port(fd) {
            close_fd(fd);
            return Err(err);
        }

        let (sockaddr, sockaddr_len) = socket_addr_to_c(addr);
        let bind_res = unsafe {
            libc::bind(
                fd,
                &sockaddr as *const _ as *const libc::sockaddr,
                sockaddr_len,
            )
        };
        if bind_res < 0 {
            let err = io::Error::last_os_error();
            close_fd(fd);
            return Err(err);
        }

        let listen_res = unsafe { libc::listen(fd, backlog) };
        if listen_res < 0 {
            let err = io::Error::last_os_error();
            close_fd(fd);
            return Err(err);
        }

        let local_addr = match current_local_addr(fd) {
            Ok(addr) => addr,
            Err(err) => {
                close_fd(fd);
                return Err(err);
            }
        };

        Ok(Self {
            fd: RuntimeFd::new(fd),
            local_addr,
            accept_slot: AcceptSlot::new(),
        })
    }

    /// Returns the local address currently assigned to the listener.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Starts accepting one incoming connection.
    ///
    /// This returns a future directly for compatibility with existing callers.
    /// A concurrent accept on the same listener is reported as an error when
    /// the returned future is first polled; safe borrowing makes that path
    /// unreachable except through intentionally leaked/forgotten futures.
    ///
    /// # Errors
    ///
    /// The returned future resolves with [`io::ErrorKind::WouldBlock`] if the
    /// listener's reusable accept slot is still occupied by a previous future
    /// or if runtime operation capacity cannot accept the submission.
    pub fn accept(&mut self) -> AcceptFuture<'_> {
        let input_error = self.accept_slot.prepare().err();
        let prepared = input_error.is_none();
        AcceptFuture {
            fd: self.fd.as_raw_fd(),
            slot: &mut self.accept_slot,
            input_error,
            prepared,
        }
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        // Usually a no-op because the borrow held by AcceptFuture drops first.
        // Keep it for forgotten futures so cached accept state is still
        // orphaned/reclaimed through the reactor.
        self.accept_slot.drop_cached_state();
    }
}

// ---------------------------------------------------------------------------
// AcceptFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct AcceptFuture<'a> {
    /// Listening socket descriptor used for the submitted accept request.
    fd: RawFd,
    /// Borrowed reusable accept slot owned by the listener.
    slot: &'a mut AcceptSlot,
    /// Deferred slot-state error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// True when this future successfully prepared and owns the slot.
    prepared: bool,
}

impl Future for AcceptFuture<'_> {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(err) = this.input_error.take() {
            return Poll::Ready(Err(err));
        }
        this.slot.poll_accept(this.fd, cx)
    }
}

impl Drop for AcceptFuture<'_> {
    fn drop(&mut self) {
        if self.prepared {
            self.slot.drop_future();
        }
    }
}

#[doc(hidden)]
/// Test-only accept-slot fd cleanup probe; not a stable public API.
pub fn test_accept_slot_drop_cached_state_closes_completed_fd() -> io::Result<()> {
    let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
    let mut state = CompletionState::empty();
    state.result = fd;
    state.set_completed();

    let mut slot = AcceptSlot::new();
    slot.in_use = true;
    slot.state_ptr = &mut state;

    slot.drop_cached_state();

    if !slot.state_ptr.is_null() || slot.in_use {
        return Err(io::Error::from(io::ErrorKind::Other));
    }
    if !crate::runtime::fd::raw_fd_is_closed(fd) {
        return Err(io::Error::from(io::ErrorKind::Other));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ConnectFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct ConnectFuture<'a> {
    /// Borrowed reusable connect slot owned by the connector.
    slot: &'a mut ConnectSlot,
}

impl Future for ConnectFuture<'_> {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.slot.poll_connect(cx)
    }
}

impl Drop for ConnectFuture<'_> {
    fn drop(&mut self) {
        self.slot.drop_future();
    }
}

fn map_connect_timeout(result: Result<io::Result<TcpStream>, Elapsed>) -> io::Result<TcpStream> {
    match result {
        Ok(result) => result,
        Err(_) => Err(io::Error::from(io::ErrorKind::TimedOut)),
    }
}

/// Connect future with a relative timeout for a reusable [`TcpConnector`].
#[doc(hidden)]
pub struct ConnectTimeoutFuture<'a> {
    /// Timeout wrapper around the reusable-slot connect future.
    inner: Timeout<ConnectFuture<'a>>,
}

impl Future for ConnectTimeoutFuture<'_> {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx) {
            Poll::Ready(result) => Poll::Ready(map_connect_timeout(result)),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// OwnedConnectFuture
// ---------------------------------------------------------------------------

/// Self-contained connect future returned by [`TcpStream::connect`].
/// Owns its socket and prepared address so no external [`TcpConnector`] is
/// needed. Repeated connections should use [`TcpConnector`] to avoid rebuilding
/// the reusable slot wrapper.
#[doc(hidden)]
pub struct OwnedConnectFuture {
    /// Completion state for the one-shot connect submission.
    state_ptr: *mut CompletionState,
    /// Socket owned by this self-contained connect future until success or drop.
    fd: RawFd,
    /// Prepared remote address for the one-shot connect attempt.
    addr: Option<RetainedConnectAddr>,
}

impl OwnedConnectFuture {
    fn new(addr: SocketAddr) -> io::Result<Self> {
        let fd = match new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM) {
            Ok(fd) => fd,
            Err(err) => {
                return Err(err);
            }
        };
        Ok(Self {
            state_ptr: std::ptr::null_mut(),
            fd,
            addr: Some(RetainedConnectAddr::from_socket_addr(addr)),
        })
    }

    fn cleanup_fd(&mut self) {
        close_if_valid(&mut self.fd);
    }

    fn take_stream(&mut self) -> TcpStream {
        let fd = self.fd;
        self.fd = -1;
        TcpStream::from_raw_fd(fd)
    }
}

impl Future for OwnedConnectFuture {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    this.cleanup_fd();
                    return Poll::Ready(Err(err));
                }

                return Poll::Ready(Ok(this.take_stream()));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                this.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = match this.addr.take() {
                Some(payload) => payload,
                None => {
                    unsafe { (*pctx.reactor()).free_op(state_ptr) };
                    this.state_ptr = std::ptr::null_mut();
                    this.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::InvalidInput)));
                }
            };

            unsafe {
                if let Err((e, _payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let sqe = opcode::Connect::new(
                            types::Fd(this.fd),
                            payload.addr_ptr(),
                            payload.addrlen,
                        )
                        .build()
                        .user_data(state_ptr as u64);
                        Ok(sqe)
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    this.cleanup_fd();
                    return Poll::Ready(Err(e));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl Drop for OwnedConnectFuture {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.cleanup_fd();
    }
}

/// Self-contained connect future with a relative timeout.
#[doc(hidden)]
pub struct OwnedConnectTimeoutFuture {
    /// Timeout wrapper around the self-contained one-shot connect future.
    inner: Timeout<OwnedConnectFuture>,
}

impl Future for OwnedConnectTimeoutFuture {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx) {
            Poll::Ready(result) => Poll::Ready(map_connect_timeout(result)),
            Poll::Pending => Poll::Pending,
        }
    }
}
