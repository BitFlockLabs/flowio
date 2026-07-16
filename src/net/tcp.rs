//! TCP transport with generic buffer support.
//!
//! Provides [`TcpListener`] for accepting connections and [`TcpConnector`]
//! for establishing outbound connections.  All read/write operations use the
//! rental pattern — the caller-provided buffer is consumed and returned
//! alongside the result on completion.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the per-message fast path:
//! - For steady-state stream I/O, [`TcpStream::read`] / [`TcpStream::write`]
//!   perform one contiguous submission and return short reads or writes to the
//!   caller.
//! - For timeout-edge callers whose phase deadline has already reached
//!   `Duration::ZERO`, the `try_*` methods attempt one nonblocking syscall on
//!   the existing socket and return immediately with the rental buffer.
//! - Use vectored APIs only when data is already segmented. For one
//!   contiguous payload, the contiguous APIs avoid iovec scratch.
//! - For fixed-shape hot-path buffers, pair TCP with
//!   [`crate::runtime::buffer::pool::IoBuffPool`].
//! - Use [`TcpStream::try_clone_for_split`] only during connection setup when
//!   separate read/write owners are needed. The handles share one kernel TCP
//!   stream.
//!
//! Avoid on the per-message fast path:
//! - Avoid [`TcpStream::read_exact`] / [`TcpStream::write_all`]
//!   unless the protocol requires complete-buffer semantics. Use
//!   [`TcpStream::read`] / [`TcpStream::write`] instead when the caller can
//!   track progress explicitly.
//! - Avoid the immediate `try_*` methods as a readiness loop. They do not
//!   register a reactor waiter; use the normal async methods except at an
//!   already-expired deadline edge.
//!
//! On a repeated connection path, prefer [`TcpConnector`] over
//! [`TcpStream::connect`] / [`TcpStream::connect_timeout`]. It reuses the
//! connector's slot wrapper, although every attempt still creates and
//! configures a fresh nonblocking socket.
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
//!     Executor::spawn(async move {
//!         let (mut stream, _peer) = listener.accept().await.unwrap();
//!         let recv = vec![0u8; 4];
//!         let (res, buf) = stream.read_exact(recv, 4).await;
//!         res.unwrap();
//!         assert_eq!(&buf[..], b"ping");
//!     }).unwrap();
//!
//!     let mut connector = TcpConnector::new();
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let (res, _buf) = stream.write_all(b"ping".to_vec()).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same operations work with pool-backed [`IoBuffMut`]. Freezing a send
//! buffer into [`IoBuff`] changes only the handle; it does not copy bytes:
//! ```no_run
//! use flowio::net::tcp::{TcpConnector, TcpListener};
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! fn pool() -> IoBuffPool {
//!     let mut pool = IoBuffPool::new(IoBuffPoolConfig {
//!         headroom: 0,
//!         payload: 64,
//!         tailroom: 0,
//!         objs_per_slab: 8,
//!     }).unwrap();
//!     pool.init();
//!     pool
//! }
//!
//! let mut server_pool = pool();
//! let mut client_pool = pool();
//! let mut executor = Executor::new()?;
//! executor.run(async move {
//!     let mut listener =
//!         TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).unwrap();
//!     let addr = listener.local_addr();
//!
//!     Executor::spawn(async move {
//!         let (mut stream, _peer) = listener.accept().await.unwrap();
//!         let recv = server_pool.alloc().unwrap();
//!         let (res, buf) = stream.read_exact(recv, 4).await;
//!         res.unwrap();
//!         assert_eq!(buf.payload_bytes(), b"ping");
//!     }).unwrap();
//!
//!     let mut connector = TcpConnector::new();
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let mut buf = client_pool.alloc().unwrap();
//!     buf.payload_append(b"ping").unwrap();
//!     let (res, _buf) = stream.write_all(buf.freeze()).await;
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
//!     Executor::spawn(async move {
//!         let (mut stream, _peer) = listener.accept().await.unwrap();
//!         let recv = IoBuffVecMut::<2>::from_array([
//!             IoBuffMut::new(0, 5, 0).unwrap(),
//!             IoBuffMut::new(0, 6, 0).unwrap(),
//!         ]);
//!         let (res, chain) = stream.readv_exact(recv, 11).await;
//!         res.unwrap();
//!         assert_eq!(chain.get(0).unwrap().payload_bytes(), b"hello");
//!         assert_eq!(chain.get(1).unwrap().payload_bytes(), b" world");
//!     }).unwrap();
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
//!     let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 8080));
//!     for _ in 0..2 {
//!         let _ = connector
//!             .connect_timeout(addr, Duration::from_secs(1))
//!             .unwrap()
//!             .await;
//!     }
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use super::stream;
use super::{
    WriteBufferChain, WritevProjection, close_fd, close_if_valid, current_local_addr,
    current_peer_addr, get_sock_opt, new_nonblocking_socket, set_reuse_addr, set_reuse_port,
    set_sock_opt, socket_addr_from_c, socket_addr_to_c, socket_domain,
};
use crate::runtime::buffer::iobuffvec::IoBuffVecMut;
use crate::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    completed_op_ctx_from_waker, drop_op_ptr_unchecked, poll_ctx_from_waker,
    refresh_op_waiter_from_waker, submit_retained_sqe, validate_local_io_result,
};
use crate::runtime::fd::RuntimeFd;
use crate::runtime::op::CompletionState;
use crate::runtime::timer::{Timeout, TimeoutError, timeout};
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
                let op_ctx = unsafe { completed_op_ctx_from_waker(cx, self.state_ptr) };
                let payload = unsafe {
                    (*op_ctx.reactor()).take_retained_payload::<RetainedAcceptAddr>(self.state_ptr)
                };
                unsafe { (*op_ctx.reactor()).free_op(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                if op_ctx.context_rejected() {
                    if result >= 0 {
                        close_fd(result as RawFd);
                    }
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
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
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    self.in_use = false;
                    return Poll::Ready(Err(err));
                }
            };
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
                let op_ctx = unsafe { completed_op_ctx_from_waker(cx, self.state_ptr) };
                unsafe { (*op_ctx.reactor()).free_op(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                if op_ctx.context_rejected() {
                    self.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    self.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
                return Poll::Ready(Ok(self.take_stream()));
            }
        }

        if self.state_ptr.is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
            };
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
    /// Takes ownership of an already-connected socket and closes it on drop.
    ///
    /// The caller must transfer sole descriptor ownership to FlowIO and must
    /// not close it or wrap the same raw descriptor in another owning handle.
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

    stream::impl_stream_rw!(TcpStream, "flowio::net::tcp::TcpStream");
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
    /// This is appropriate for an isolated setup-time attempt. For repeated
    /// attempts, [`TcpConnector`] reuses its slot wrapper.
    pub fn connect(addr: SocketAddr) -> io::Result<OwnedConnectFuture> {
        OwnedConnectFuture::new(addr)
    }

    /// Convenience method that connects to the given address with a deadline.
    ///
    /// Returns `TimedOut` if the connection does not complete before the
    /// provided duration elapses. Timer-runtime failures, including
    /// `OutOfMemory`, propagate with their original [`io::ErrorKind`]. For
    /// repeated outbound connections, prefer [`TcpConnector::connect_timeout`]
    /// so the connector can reuse its slot metadata across attempts.
    ///
    /// This is appropriate for an isolated timed attempt. For repeated timed
    /// attempts, reuse [`TcpConnector::connect_timeout`].
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
/// The connector reuses its slot storage across attempts. Each attempt creates
/// a fresh nonblocking socket and prepared peer address, and each submission
/// still uses the reactor completion-state pool.
///
/// Reusing this type avoids rebuilding the slot wrapper for each outbound
/// attempt. [`TcpStream::connect`] provides a self-contained one-shot future.
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
    /// This is the preferred repeated-connection API because it reuses the
    /// connector-owned slot wrapper. It still creates and configures a fresh
    /// socket for this attempt. Use [`TcpStream::connect`] for an isolated
    /// convenience connection.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<ConnectFuture<'_>> {
        self.connect_slot.prepare(addr)?;
        Ok(ConnectFuture {
            slot: &mut self.connect_slot,
        })
    }

    /// Starts connecting to the provided remote address with a deadline.
    ///
    /// Returns `TimedOut` if the connection does not complete before the
    /// provided duration elapses. Timer-runtime failures, including
    /// `OutOfMemory`, propagate with their original [`io::ErrorKind`]. This is
    /// the repeated-connect timeout API; prefer it over
    /// [`TcpStream::connect_timeout`] when the same connector is reused across
    /// attempts.
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
        // Safe non-forgotten use drops AcceptFuture before this exclusive
        // owner. The explicit cleanup handles a forgotten future whose cached
        // accept state remains in the listener.
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
            return Poll::Ready(validate_local_io_result(cx, Err(err)));
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

#[cfg(feature = "test-support")]
pub(crate) mod test_support {
    use super::*;

    /// Verifies forgotten-future listener teardown closes a completed accepted
    /// descriptor before releasing its cached completion state.
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

fn map_connect_timeout(
    result: Result<io::Result<TcpStream>, TimeoutError>,
) -> io::Result<TcpStream> {
    match result {
        Ok(result) => result,
        Err(TimeoutError::Elapsed) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        Err(TimeoutError::Runtime(err)) => Err(err),
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
                let op_ctx = unsafe { completed_op_ctx_from_waker(cx, this.state_ptr) };
                unsafe { (*op_ctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                if op_ctx.context_rejected() {
                    this.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    this.cleanup_fd();
                    return Poll::Ready(Err(err));
                }

                return Poll::Ready(Ok(this.take_stream()));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    this.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
            };
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
