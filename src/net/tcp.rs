//! TCP transport with generic buffer support.
//!
//! Provides [`TcpListener`] for accepting connections and [`TcpConnector`]
//! for establishing outbound connections.  All read/write operations use the
//! rental pattern — the caller-provided buffer is consumed and returned
//! alongside the result on completion.
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
//! Timed connects use the same connect futures plus the runtime timer wheel:
//! ```no_run
//! use flowio::net::tcp::TcpStream;
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//! use std::time::Duration;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let _ = TcpStream::connect_timeout(
//!         SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
//!         Duration::from_secs(1),
//!     )
//!     .unwrap()
//!     .await;
//! })?;
//! # Ok::<(), std::io::Error>(())

use super::stream;
use super::{
    close_fd, close_if_valid, current_local_addr, current_peer_addr, get_sock_opt,
    new_nonblocking_socket, set_reuse_addr, set_reuse_port, set_sock_opt, socket_addr_from_c,
    socket_addr_to_c, socket_domain,
};
use crate::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{drop_op_ptr_unchecked, poll_ctx_from_waker, submit_tracked_sqe};
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

struct AcceptSlot
{
    state_ptr: *mut CompletionState,
    in_use: bool,
    addr: libc::sockaddr_storage,
    addrlen: libc::socklen_t,
}

impl AcceptSlot
{
    fn new() -> Self
    {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
            addr: unsafe { std::mem::zeroed() },
            addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        }
    }

    fn prepare(&mut self)
    {
        debug_assert!(!self.in_use, "tcp accept slot already in use");
        self.in_use = true;
        self.addrlen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    }

    fn drop_future(&mut self)
    {
        if !self.state_ptr.is_null()
        {
            unsafe {
                if (*self.state_ptr).is_completed() && (*self.state_ptr).result >= 0
                {
                    close_fd((*self.state_ptr).result as RawFd);
                }
                drop_op_ptr_unchecked(&mut self.state_ptr);
            }
        }

        self.in_use = false;
    }

    fn drop_cached_state(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.in_use = false;
    }

    fn poll_accept(
        &mut self,
        fd: RawFd,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>>
    {
        if !self.state_ptr.is_null()
        {
            let state = unsafe { &*self.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                if result < 0
                {
                    return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                }
                let remote_addr = match socket_addr_from_c(&self.addr, self.addrlen)
                {
                    Ok(addr) => addr,
                    Err(err) =>
                    {
                        close_fd(result as RawFd);
                        return Poll::Ready(Err(err));
                    }
                };
                return Poll::Ready(Ok((TcpStream::from_raw_fd(result as RawFd), remote_addr)));
            }
        }

        if self.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                self.in_use = false;
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            self.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let addr_ptr = &mut self.addr as *mut libc::sockaddr_storage as *mut libc::sockaddr;
            let addrlen_ptr = &mut self.addrlen as *mut libc::socklen_t;
            let sqe = opcode::Accept::new(types::Fd(fd), addr_ptr, addrlen_ptr)
                .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

struct ConnectSlot
{
    state_ptr: *mut CompletionState,
    in_use: bool,
    fd: RawFd,
    addr: libc::sockaddr_storage,
    addrlen: libc::socklen_t,
}

impl ConnectSlot
{
    fn new() -> Self
    {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
            fd: -1,
            addr: unsafe { std::mem::zeroed() },
            addrlen: 0,
        }
    }

    fn prepare(&mut self, addr: SocketAddr) -> io::Result<()>
    {
        debug_assert!(!self.in_use, "tcp connect slot already in use");
        self.cleanup_fd();
        self.in_use = true;
        self.fd = match new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM)
        {
            Ok(fd) => fd,
            Err(err) =>
            {
                self.in_use = false;
                return Err(err);
            }
        };
        let (storage, addrlen) = socket_addr_to_c(addr);
        self.addr = storage;
        self.addrlen = addrlen;
        Ok(())
    }

    fn cleanup_fd(&mut self)
    {
        close_if_valid(&mut self.fd);
    }

    fn take_stream(&mut self) -> TcpStream
    {
        let fd = self.fd;
        self.fd = -1;
        TcpStream::from_raw_fd(fd)
    }

    fn drop_future(&mut self)
    {
        if !self.state_ptr.is_null()
        {
            unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        }

        self.cleanup_fd();
        self.in_use = false;
    }

    fn drop_cached_state(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.in_use = false;
    }

    fn poll_connect(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<TcpStream>>
    {
        if !self.state_ptr.is_null()
        {
            let state = unsafe { &*self.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(self.state_ptr) };
                self.state_ptr = std::ptr::null_mut();
                self.in_use = false;

                if result < 0
                {
                    let err = io::Error::from_raw_os_error(-result);
                    self.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
                return Poll::Ready(Ok(self.take_stream()));
            }
        }

        if self.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                self.in_use = false;
                self.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            self.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let addr_ptr = &self.addr as *const libc::sockaddr_storage as *const libc::sockaddr;
            let sqe = opcode::Connect::new(types::Fd(self.fd), addr_ptr, self.addrlen)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    self.state_ptr = std::ptr::null_mut();
                    self.in_use = false;
                    self.cleanup_fd();
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
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
pub struct TcpStream
{
    fd: RuntimeFd,
}

impl TcpStream
{
    /// Wraps an already-owned connected socket.
    pub fn from_raw_fd(fd: RawFd) -> Self
    {
        Self {
            fd: RuntimeFd::new(fd),
        }
    }

    /// Returns the local address of this socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr>
    {
        current_local_addr(self.fd.as_raw_fd())
    }

    /// Returns the peer address of this socket.
    pub fn peer_addr(&self) -> io::Result<SocketAddr>
    {
        current_peer_addr(self.fd.as_raw_fd())
    }

    /// Enables or disables `TCP_NODELAY` (Nagle's algorithm).
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()>
    {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &(nodelay as libc::c_int),
        )
    }

    /// Returns the current `TCP_NODELAY` setting.
    pub fn nodelay(&self) -> io::Result<bool>
    {
        let val: libc::c_int =
            get_sock_opt(self.fd.as_raw_fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY)?;
        Ok(val != 0)
    }

    /// Enables or disables `SO_KEEPALIVE`.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()>
    {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &(keepalive as libc::c_int),
        )
    }

    /// Sets the `SO_SNDBUF` socket send buffer size.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()>
    {
        super::set_sock_send_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_SNDBUF` socket send buffer size.
    pub fn send_buffer_size(&self) -> io::Result<usize>
    {
        super::sock_send_buffer_size(self.fd.as_raw_fd())
    }

    /// Sets the `SO_RCVBUF` socket receive buffer size.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()>
    {
        super::set_sock_recv_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_RCVBUF` socket receive buffer size.
    pub fn recv_buffer_size(&self) -> io::Result<usize>
    {
        super::sock_recv_buffer_size(self.fd.as_raw_fd())
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()>
    {
        let how = match how
        {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        let rc = unsafe { libc::shutdown(self.fd.as_raw_fd(), how) };
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Reads up to `len` bytes into `buffer`.
    pub fn read<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> stream::ReadFuture<'_, B, Self>
    {
        stream::ReadFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Writes the initialized portion of `buffer`.
    pub fn write<B: IoBuffReadOnly>(&mut self, buffer: B) -> stream::WriteFuture<'_, B, Self>
    {
        stream::WriteFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Writes the entire buffer, handling partial writes internally.
    ///
    /// Returns `(Ok(n), buffer)` where `n` equals `buffer.len()` on
    /// success.
    pub fn write_all<B: IoBuffReadOnly>(&mut self, buffer: B)
    -> stream::WriteAllFuture<'_, B, Self>
    {
        stream::WriteAllFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Reads exactly `len` bytes, handling partial reads internally.
    ///
    /// Returns `UnexpectedEof` if the peer closes before `len` bytes arrive.
    pub fn read_exact<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> stream::ReadExactFuture<'_, B, Self>
    {
        stream::ReadExactFuture::new(self.fd.as_raw_fd(), buffer, len)
    }

    /// Scatter-read into a vectored buffer chain.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes read is returned in `Ok`.
    pub fn readv<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
    ) -> stream::ReadvFuture<'_, N, Self>
    {
        stream::ReadvFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write from a vectored buffer chain.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes written is returned in `Ok`.
    pub fn writev<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
    ) -> stream::WritevFuture<'_, N, Self>
    {
        stream::WritevFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Gather-write the entire vectored chain, handling partial writes.
    ///
    /// Returns `(Ok(n), chain)` where `n` equals the total byte count on
    /// success.  On error the chain is returned with an unspecified amount
    /// already written.
    pub fn writev_all<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
    ) -> stream::WritevAllFuture<'_, N, Self>
    {
        stream::WritevAllFuture::new(self.fd.as_raw_fd(), buffer)
    }

    /// Scatter-read exactly `len` total bytes into a vectored chain.
    ///
    /// Returns `(Ok(len), chain)` on success.  Returns `UnexpectedEof` if
    /// the peer closes before `len` bytes arrive.
    pub fn readv_exact<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
        len: usize,
    ) -> stream::ReadvExactFuture<'_, N, Self>
    {
        stream::ReadvExactFuture::new(self.fd.as_raw_fd(), buffer, len)
    }
}

impl AsRawFd for TcpStream
{
    fn as_raw_fd(&self) -> RawFd
    {
        self.fd.as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// TcpConnector
// ---------------------------------------------------------------------------

impl TcpStream
{
    /// Convenience method that creates a one-shot connection to the given
    /// address. For repeated connections, use [`TcpConnector`] to reuse the
    /// connector's slot metadata across attempts.
    pub fn connect(addr: SocketAddr) -> io::Result<OwnedConnectFuture>
    {
        OwnedConnectFuture::new(addr)
    }

    /// Convenience method that connects to the given address with a deadline.
    ///
    /// Returns `TimedOut` if the connection does not complete before the
    /// provided duration elapses. For repeated outbound connections, prefer
    /// [`TcpConnector::connect_timeout`] so the connector can reuse its slot
    /// metadata across attempts.
    pub fn connect_timeout(
        addr: SocketAddr,
        timeout_duration: Duration,
    ) -> io::Result<OwnedConnectTimeoutFuture>
    {
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
pub struct TcpConnector
{
    connect_slot: ConnectSlot,
}

/// Equivalent to [`TcpConnector::new()`].
impl Default for TcpConnector
{
    fn default() -> Self
    {
        Self {
            connect_slot: ConnectSlot::new(),
        }
    }
}

impl TcpConnector
{
    /// Creates a new connector.
    pub fn new() -> Self
    {
        Self::default()
    }

    /// Starts connecting to the provided remote address.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<ConnectFuture<'_>>
    {
        self.connect_slot.prepare(addr)?;
        Ok(ConnectFuture {
            slot: &mut self.connect_slot,
        })
    }

    /// Starts connecting to the provided remote address with a deadline.
    ///
    /// Returns `TimedOut` if the connection does not complete before the
    /// provided duration elapses.
    pub fn connect_timeout(
        &mut self,
        addr: SocketAddr,
        timeout_duration: Duration,
    ) -> io::Result<ConnectTimeoutFuture<'_>>
    {
        Ok(ConnectTimeoutFuture {
            inner: timeout(timeout_duration, self.connect(addr)?),
        })
    }
}

impl Drop for TcpConnector
{
    fn drop(&mut self)
    {
        self.connect_slot.drop_cached_state();
        self.connect_slot.cleanup_fd();
    }
}

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------

/// Listening TCP socket with a reusable accept slot.
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
pub struct TcpListener
{
    fd: RuntimeFd,
    local_addr: SocketAddr,
    accept_slot: AcceptSlot,
}

impl TcpListener
{
    /// Binds a nonblocking listener with `SO_REUSEADDR` and starts listening.
    pub fn bind(addr: SocketAddr, backlog: i32) -> io::Result<Self>
    {
        Self::bind_inner(addr, backlog, false)
    }

    /// Binds a nonblocking listener with both `SO_REUSEADDR` and
    /// `SO_REUSEPORT`, then starts listening.  Useful for multi-process
    /// server patterns where multiple listeners share the same port.
    pub fn bind_reuse_port(addr: SocketAddr, backlog: i32) -> io::Result<Self>
    {
        Self::bind_inner(addr, backlog, true)
    }

    fn bind_inner(addr: SocketAddr, backlog: i32, reuse_port: bool) -> io::Result<Self>
    {
        let fd = new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM)?;

        if let Err(err) = set_reuse_addr(fd)
        {
            close_fd(fd);
            return Err(err);
        }

        if reuse_port && let Err(err) = set_reuse_port(fd)
        {
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
        if bind_res < 0
        {
            let err = io::Error::last_os_error();
            close_fd(fd);
            return Err(err);
        }

        let listen_res = unsafe { libc::listen(fd, backlog) };
        if listen_res < 0
        {
            let err = io::Error::last_os_error();
            close_fd(fd);
            return Err(err);
        }

        let local_addr = match current_local_addr(fd)
        {
            Ok(addr) => addr,
            Err(err) =>
            {
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
    pub fn local_addr(&self) -> SocketAddr
    {
        self.local_addr
    }

    /// Starts accepting one incoming connection.
    pub fn accept(&mut self) -> AcceptFuture<'_>
    {
        self.accept_slot.prepare();
        AcceptFuture {
            fd: self.fd.as_raw_fd(),
            slot: &mut self.accept_slot,
        }
    }
}

impl AsRawFd for TcpListener
{
    fn as_raw_fd(&self) -> RawFd
    {
        self.fd.as_raw_fd()
    }
}

impl Drop for TcpListener
{
    fn drop(&mut self)
    {
        self.accept_slot.drop_cached_state();
    }
}

// ---------------------------------------------------------------------------
// AcceptFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct AcceptFuture<'a>
{
    fd: RawFd,
    slot: &'a mut AcceptSlot,
}

impl Future for AcceptFuture<'_>
{
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };
        this.slot.poll_accept(this.fd, cx)
    }
}

impl Drop for AcceptFuture<'_>
{
    fn drop(&mut self)
    {
        self.slot.drop_future();
    }
}

// ---------------------------------------------------------------------------
// ConnectFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct ConnectFuture<'a>
{
    slot: &'a mut ConnectSlot,
}

impl Future for ConnectFuture<'_>
{
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };
        this.slot.poll_connect(cx)
    }
}

impl Drop for ConnectFuture<'_>
{
    fn drop(&mut self)
    {
        self.slot.drop_future();
    }
}

fn map_connect_timeout(result: Result<io::Result<TcpStream>, Elapsed>) -> io::Result<TcpStream>
{
    match result
    {
        Ok(result) => result,
        Err(_) => Err(io::Error::from(io::ErrorKind::TimedOut)),
    }
}

/// Connect future with a relative timeout for a reusable [`TcpConnector`].
pub struct ConnectTimeoutFuture<'a>
{
    inner: Timeout<ConnectFuture<'a>>,
}

impl Future for ConnectTimeoutFuture<'_>
{
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
        {
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
pub struct OwnedConnectFuture
{
    state_ptr: *mut CompletionState,
    fd: RawFd,
    addr: libc::sockaddr_storage,
    addrlen: libc::socklen_t,
}

impl OwnedConnectFuture
{
    fn new(addr: SocketAddr) -> io::Result<Self>
    {
        let fd = match new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM)
        {
            Ok(fd) => fd,
            Err(err) =>
            {
                return Err(err);
            }
        };
        let (storage, addrlen) = socket_addr_to_c(addr);
        Ok(Self {
            state_ptr: std::ptr::null_mut(),
            fd,
            addr: storage,
            addrlen,
        })
    }

    fn cleanup_fd(&mut self)
    {
        close_if_valid(&mut self.fd);
    }

    fn take_stream(&mut self) -> TcpStream
    {
        let fd = self.fd;
        self.fd = -1;
        TcpStream::from_raw_fd(fd)
    }
}

impl Future for OwnedConnectFuture
{
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };
        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                if result < 0
                {
                    let err = io::Error::from_raw_os_error(-result);
                    this.cleanup_fd();
                    return Poll::Ready(Err(err));
                }

                return Poll::Ready(Ok(this.take_stream()));
            }
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                this.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let addr_ptr = &this.addr as *const libc::sockaddr_storage as *const libc::sockaddr;
            let sqe = opcode::Connect::new(types::Fd(this.fd), addr_ptr, this.addrlen)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    this.cleanup_fd();
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

impl Drop for OwnedConnectFuture
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.cleanup_fd();
    }
}

/// Self-contained connect future with a relative timeout.
pub struct OwnedConnectTimeoutFuture
{
    inner: Timeout<OwnedConnectFuture>,
}

impl Future for OwnedConnectTimeoutFuture
{
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
        {
            Poll::Ready(result) => Poll::Ready(map_connect_timeout(result)),
            Poll::Pending => Poll::Pending,
        }
    }
}
