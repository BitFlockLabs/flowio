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
    AcceptReadinessSlot as AcceptSlot, ConnectSubmissionSlot, RetainedConnectAddr,
    WriteBufferChain, WritevProjection, close_fd, current_local_addr, current_peer_addr,
    get_sock_opt, map_connect_timeout, new_nonblocking_socket, set_reuse_addr, set_reuse_port,
    set_sock_opt, socket_addr_from_c, socket_addr_to_c, socket_domain,
};
use crate::runtime::buffer::iobuffvec::IoBuffVecMut;
use crate::runtime::buffer::{IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::validate_local_io_result;
use crate::runtime::fd::{LingerProvenance, RuntimeFd, RuntimeFdOpState};
#[cfg(any(test, feature = "test-support"))]
use crate::runtime::op::CompletionState;
use crate::runtime::timer::{Timeout, timeout};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

// ---------------------------------------------------------------------------
// AcceptSlot / ConnectSlot
// ---------------------------------------------------------------------------

fn finish_accepted_stream_with_provenance(
    accepted_fd: OwnedFd,
    provenance: LingerProvenance,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
) -> io::Result<(TcpStream, SocketAddr)> {
    let stream = TcpStream {
        fd: RuntimeFd::from_owned_with_provenance(accepted_fd, provenance),
    };
    let remote_addr = socket_addr_from_c(addr, addrlen)?;
    Ok((stream, remote_addr))
}

#[cfg(test)]
fn finish_accepted_stream(
    accepted_fd: OwnedFd,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
) -> io::Result<(TcpStream, SocketAddr)> {
    finish_accepted_stream_with_provenance(
        accepted_fd,
        LingerProvenance::KnownNonPositive,
        addr,
        addrlen,
    )
}

/// Reusable TCP connect state with no transport-specific completion data.
type ConnectSlot = ConnectSubmissionSlot<()>;

fn prepare_connect_slot(slot: &mut ConnectSlot, addr: SocketAddr) -> io::Result<()> {
    if slot.in_use || !slot.state_ptr.is_null() {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }
    slot.cleanup_fd();
    slot.in_use = true;
    let fd = match new_nonblocking_socket(socket_domain(addr), libc::SOCK_STREAM) {
        Ok(fd) => {
            // SAFETY: socket creation returned one fresh descriptor and this
            // local becomes its sole owner before any fallible later step.
            unsafe { OwnedFd::from_raw_fd(fd) }
        }
        Err(err) => {
            slot.in_use = false;
            return Err(err);
        }
    };
    slot.fd = Some(fd);
    slot.addr = Some(RetainedConnectAddr::from_socket_addr(addr));
    Ok(())
}

fn finish_connected_tcp_stream(fd: OwnedFd, _completion_data: &()) -> io::Result<TcpStream> {
    Ok(TcpStream {
        fd: RuntimeFd::from_fresh_owned(fd),
    })
}

#[inline(always)]
fn poll_tcp_connect(slot: &mut ConnectSlot, cx: &mut Context<'_>) -> Poll<io::Result<TcpStream>> {
    slot.poll_connect(cx, finish_connected_tcp_stream)
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
/// The stream is an owner-OS-thread value and is neither [`Send`](std::marker::Send)
/// nor [`Sync`](std::marker::Sync).
/// An idle stream may be used by another FlowIO executor on that same thread;
/// once I/O is submitted, its future and completion state remain with the
/// originating executor through the target completion.
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

fn finish_split_clone(source: &RuntimeFd, duplicate: io::Result<OwnedFd>) -> io::Result<RuntimeFd> {
    let duplicate = duplicate?;
    source.mark_linger_uncertain();
    Ok(RuntimeFd::from_external_owned(duplicate))
}

impl TcpStream {
    /// Safely takes ownership of an already-connected socket.
    ///
    /// The descriptor is closed exactly once when the FlowIO stream is
    /// dropped. `OwnedFd` proves unique close ownership, but does not validate
    /// the socket type or configuration. The descriptor must refer to a valid
    /// connected TCP stream and must be nonblocking when using the `try_*`
    /// deadline-edge APIs.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpStream;
    /// use std::net::{Ipv4Addr, SocketAddr};
    /// use std::os::fd::OwnedFd;
    ///
    /// let standard = std::net::TcpStream::connect(
    ///     SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
    /// )?;
    /// standard.set_nonblocking(true)?;
    /// let owned: OwnedFd = standard.into();
    /// let _stream = TcpStream::from_owned_fd(owned);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// Safe code cannot adopt the same owner twice:
    /// ```compile_fail
    /// use flowio::net::tcp::TcpStream;
    /// use std::os::fd::OwnedFd;
    ///
    /// let owned: OwnedFd = std::fs::File::open("/dev/null").unwrap().into();
    /// let _first = TcpStream::from_owned_fd(owned);
    /// let _second = TcpStream::from_owned_fd(owned);
    /// ```
    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        Self {
            fd: RuntimeFd::from_external_owned(fd),
        }
    }

    /// Takes ownership of a bare connected-socket descriptor.
    ///
    /// FlowIO-created TCP sockets are nonblocking. Callers that pass an
    /// external descriptor must preserve that invariant; the `try_*`
    /// deadline-edge APIs rely on the fd already being nonblocking.
    ///
    /// # Safety
    ///
    /// `fd` must be a valid open descriptor for which the caller owns the sole
    /// close responsibility. After this call, the caller must not close `fd`,
    /// reuse it, or create another owning wrapper for the same descriptor.
    ///
    /// Calling raw adoption without an explicit safety boundary is rejected:
    /// ```compile_fail
    /// use flowio::net::tcp::TcpStream;
    ///
    /// let _stream = TcpStream::from_raw_fd(3);
    /// ```
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        // SAFETY: the caller promises sole ownership of this valid descriptor.
        Self::from_owned_fd(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// Returns the local address currently assigned to this socket.
    ///
    /// Each call queries the live descriptor with `getsockname(2)`; no local
    /// address is cached. This is socket status/control-plane lookup, not the
    /// per-message data fast path.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error from `getsockname(2)`, or
    /// [`io::ErrorKind::InvalidData`] if the kernel returns an unsupported or
    /// malformed socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        current_local_addr(self.fd.raw_fd())
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
        let fd = unsafe { libc::fcntl(self.fd.raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        let duplicate = if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: F_DUPFD_CLOEXEC returned a new descriptor owned only by
            // this call; no other owning wrapper has been constructed for it.
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        };
        finish_split_clone(&self.fd, duplicate).map(|fd| Self { fd })
    }

    /// Returns the peer address currently assigned to this socket.
    ///
    /// Each call queries the live descriptor with `getpeername(2)`; no peer
    /// address is cached. This is socket status/control-plane lookup, not the
    /// per-message data fast path.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error from `getpeername(2)`, or
    /// [`io::ErrorKind::InvalidData`] if the kernel returns an unsupported or
    /// malformed socket address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        current_peer_addr(self.fd.raw_fd())
    }

    /// Enables or disables `TCP_NODELAY` (Nagle's algorithm).
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// connection setup instead of toggling it per message.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        set_sock_opt(
            self.fd.raw_fd(),
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
            get_sock_opt(self.fd.raw_fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY)?;
        Ok(val != 0)
    }

    /// Enables or disables `SO_KEEPALIVE`.
    ///
    /// This only toggles the socket-level keepalive flag. Platform-specific
    /// keepalive probe intervals and counts are not configured by this method.
    /// Apply it during connection setup instead of toggling it per message.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        set_sock_opt(
            self.fd.raw_fd(),
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
        super::set_sock_send_buffer_size(self.fd.raw_fd(), size)
    }

    /// Returns the current `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        super::sock_send_buffer_size(self.fd.raw_fd())
    }

    /// Sets the `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// connection setup instead of changing it per read.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_recv_buffer_size(self.fd.raw_fd(), size)
    }

    /// Returns the current `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        super::sock_recv_buffer_size(self.fd.raw_fd())
    }

    /// Shuts down the read, write, or both halves of this connection.
    ///
    /// This is connection control-plane work, normally used for teardown or
    /// protocol half-close rather than steady-state data transfer.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        super::shutdown_socket(self.fd.raw_fd(), how)
    }

    /// Returns the descriptor for FlowIO-owned operations without exposing it
    /// outside the crate or invalidating known linger provenance.
    ///
    /// Callers must not leak the descriptor or mutate shared socket options.
    /// Public raw-descriptor access continues to use [`AsRawFd`] below and
    /// permanently marks the descriptor's linger state uncertain.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn raw_fd_for_internal_io(&self) -> RawFd {
        self.fd.raw_fd()
    }

    /// Clones one owner-thread descriptor lease for a self-contained staged
    /// TLS raw-I/O future. The future moves this exact lease into its
    /// completion state on first submission and drops it locally if unpolled.
    #[inline(always)]
    pub(crate) fn staged_fd_op_state_for_internal_io(&self) -> RuntimeFdOpState<'static> {
        self.fd.lease().into_op_state()
    }

    stream::impl_stream_rw!(TcpStream, "flowio::net::tcp::TcpStream");
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.expose_raw_fd()
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
            connect_slot: ConnectSlot::new(()),
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
        prepare_connect_slot(&mut self.connect_slot, addr)?;
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
        self.connect_slot.retire_cached_state();
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

        let fd = RuntimeFd::from_fresh_raw_fd(fd);
        Ok(Self {
            accept_slot: AcceptSlot::new(&fd),
            fd,
            local_addr,
        })
    }

    /// Returns the local address captured during successful listener
    /// construction.
    ///
    /// [`Self::bind`] and [`Self::bind_reuse_port`] query `getsockname(2)` once
    /// after `bind(2)` and `listen(2)` succeed, so a kernel-selected port from
    /// a port-zero bind is included. This method copies that cached address
    /// without a syscall, allocation, or runtime-context lookup. It does not
    /// refresh after changes made through the raw descriptor.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns whether FlowIO has permanently latched the listener's accept
    /// readiness as unusable.
    ///
    /// A `true` result is sticky: later [`Self::accept`] calls fail with
    /// [`io::ErrorKind::ConnectionAborted`] until the listener is dropped and
    /// rebuilt. A `false` result means only that FlowIO has not latched that
    /// state; it is not a general socket-health probe. This is a pure
    /// userspace query with no syscall, allocation, or runtime-context lookup.
    ///
    /// # Example
    /// ```
    /// use flowio::net::tcp::TcpListener;
    ///
    /// fn supervisor_should_rebuild(listener: &TcpListener) -> bool {
    ///     listener.is_terminal()
    /// }
    /// ```
    pub fn is_terminal(&self) -> bool {
        self.accept_slot.is_terminal()
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
    /// or if runtime operation capacity cannot accept the submission. A
    /// `POLLHUP` or `POLLNVAL` readiness with no queued connection latches the
    /// listener and returns [`io::ErrorKind::ConnectionAborted`]. Later accepts
    /// return the same non-retryable kind without another readiness submission.
    /// A bare `POLLERR` gets one internal readiness rearm; if it recurs for the
    /// same accept while `accept4` still reports `EAGAIN`, that exact
    /// [`io::ErrorKind::WouldBlock`] result is returned without latching.
    /// Readiness containing `POLLHUP` or `POLLNVAL` remains terminal even when
    /// `POLLERR` is also present. A positive `POLLNVAL` confirmed as `EBADF`
    /// preserves that raw errno for the current future while latching the same
    /// later fail-fast state.
    /// `EMFILE` and `ENFILE` propagate exactly without latching or rearming.
    /// The slot preserves the observed readiness, so the next accept polled in
    /// the owner context makes one direct nonblocking `accept4` attempt without
    /// another readiness submission. FlowIO performs no hidden retry, timer,
    /// or backoff; callers should relieve descriptor pressure and apply bounded
    /// backoff before retrying. If the direct attempt returns
    /// [`io::ErrorKind::WouldBlock`], the retained mask is classified by the
    /// same rules above: HUP/NVAL latches, bare `POLLERR` uses its bounded
    /// budget, and plain stale readiness takes the ordinary rearm.
    /// A future that reports the occupied-slot error never claims that slot;
    /// later polls park without replacing the previous accept's waiter.
    ///
    /// Dropping a prepared pending accept cancels only its readiness wait; it
    /// does not consume a connection already queued in the listener backlog.
    /// An unprepared future owns no wait, so dropping it leaves the earlier
    /// accept untouched. If the listener's raw fd is exposed, the caller must
    /// not concurrently accept from it or race changes to its file-status
    /// flags.
    pub fn accept(&mut self) -> AcceptFuture<'_> {
        let input_error = self.accept_slot.prepare().err();
        let prepared = input_error.is_none();
        AcceptFuture {
            slot: &mut self.accept_slot,
            input_error,
            prepared,
        }
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.expose_raw_fd()
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

/// Future returned by [`TcpListener::accept`] for one incoming connection.
///
/// It resolves to the connected [`TcpStream`] and its peer address. The
/// future borrows the listener's reusable accept slot, so a listener can have
/// at most one live accept future. Dropping a prepared pending future cancels
/// its readiness wait without consuming a connection from the listener
/// backlog; dropping an unprepared future cannot affect the earlier owner.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::{AcceptFuture, TcpListener};
///
/// fn accept(listener: &mut TcpListener) -> AcceptFuture<'_> {
///     listener.accept()
/// }
/// ```
pub struct AcceptFuture<'a> {
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
        this.slot
            .poll_accept(this.prepared, cx, finish_accepted_stream_with_provenance)
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

    /// Verifies cached readiness teardown does not interpret a readiness mask
    /// as an accepted descriptor.
    pub fn test_accept_slot_drop_cached_state_preserves_unrelated_fd() -> io::Result<()> {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
        let mut state = CompletionState::empty();
        state.result = fd;
        state.set_completed();

        let listener_fd = RuntimeFd::from_fresh_raw_fd(fd);
        let mut slot = AcceptSlot::new(&listener_fd);
        slot.in_use = true;
        slot.state_ptr = &mut state;

        slot.drop_cached_state();

        if !slot.state_ptr.is_null() || slot.in_use {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        if crate::runtime::fd::raw_fd_is_closed(fd) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        drop(slot);
        drop(listener_fd);
        if !crate::runtime::fd::raw_fd_is_closed(fd) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConnectFuture
// ---------------------------------------------------------------------------

/// Future returned by [`TcpConnector::connect`] for one connection attempt.
///
/// It resolves to a connected [`TcpStream`]. The future borrows the
/// connector's reusable slot, so the connector becomes available for another
/// attempt after this future completes or is dropped.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::{ConnectFuture, TcpConnector};
/// use std::net::SocketAddr;
///
/// fn connect<'a>(
///     connector: &'a mut TcpConnector,
///     peer: SocketAddr,
/// ) -> std::io::Result<ConnectFuture<'a>> {
///     connector.connect(peer)
/// }
/// ```
pub struct ConnectFuture<'a> {
    /// Borrowed reusable connect slot owned by the connector.
    slot: &'a mut ConnectSlot,
}

impl Future for ConnectFuture<'_> {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        poll_tcp_connect(this.slot, cx)
    }
}

impl Drop for ConnectFuture<'_> {
    fn drop(&mut self) {
        self.slot.drop_future();
    }
}

/// Future returned by [`TcpConnector::connect_timeout`] for one timed
/// connection attempt.
///
/// It resolves to a connected [`TcpStream`], or to
/// [`io::ErrorKind::TimedOut`] when the relative timeout expires. The future
/// borrows the connector's reusable slot for the duration of the attempt.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::{ConnectTimeoutFuture, TcpConnector};
/// use std::net::SocketAddr;
/// use std::time::Duration;
///
/// fn connect_with_timeout<'a>(
///     connector: &'a mut TcpConnector,
///     peer: SocketAddr,
/// ) -> std::io::Result<ConnectTimeoutFuture<'a>> {
///     connector.connect_timeout(peer, Duration::from_secs(1))
/// }
/// ```
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
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::{OwnedConnectFuture, TcpStream};
/// use std::net::SocketAddr;
///
/// fn connect(peer: SocketAddr) -> std::io::Result<OwnedConnectFuture> {
///     TcpStream::connect(peer)
/// }
/// ```
pub struct OwnedConnectFuture {
    /// One-shot submission state owned by this future.
    slot: ConnectSlot,
}

impl OwnedConnectFuture {
    fn new(addr: SocketAddr) -> io::Result<Self> {
        let mut slot = ConnectSlot::new(());
        prepare_connect_slot(&mut slot, addr)?;
        Ok(Self { slot })
    }
}

impl Future for OwnedConnectFuture {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        poll_tcp_connect(&mut this.slot, cx)
    }
}

impl Drop for OwnedConnectFuture {
    fn drop(&mut self) {
        self.slot.drop_future();
    }
}

/// Self-contained future returned by [`TcpStream::connect_timeout`].
///
/// It owns the socket and prepared address for one timed connection attempt
/// and resolves to [`io::ErrorKind::TimedOut`] when the relative timeout
/// expires.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::{OwnedConnectTimeoutFuture, TcpStream};
/// use std::net::SocketAddr;
/// use std::time::Duration;
///
/// fn connect_with_timeout(
///     peer: SocketAddr,
/// ) -> std::io::Result<OwnedConnectTimeoutFuture> {
///     TcpStream::connect_timeout(peer, Duration::from_secs(1))
/// }
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(miri))]
    #[test]
    fn tcp_unsubmitted_connect_drop_paths_close_prepared_sockets() {
        let future_fd = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("reusable connect fd creation failed");
        let mut future_slot = ConnectSlot::new(());
        future_slot.in_use = true;
        // SAFETY: the test-created descriptor has no other owner.
        future_slot.fd = Some(unsafe { OwnedFd::from_raw_fd(future_fd) });
        future_slot.addr = Some(RetainedConnectAddr::from_socket_addr(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));
        drop(ConnectFuture {
            slot: &mut future_slot,
        });
        assert!(future_slot.state_ptr.is_null());
        assert!(!future_slot.in_use);
        assert!(future_slot.fd.is_none());
        assert!(future_slot.addr.is_none());
        assert!(crate::runtime::fd::raw_fd_is_closed(future_fd));

        let owned_fd = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("owned connect fd creation failed");
        let mut owned_slot = ConnectSlot::new(());
        owned_slot.in_use = true;
        // SAFETY: the test-created descriptor has no other owner.
        owned_slot.fd = Some(unsafe { OwnedFd::from_raw_fd(owned_fd) });
        owned_slot.addr = Some(RetainedConnectAddr::from_socket_addr(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));
        drop(OwnedConnectFuture { slot: owned_slot });
        assert!(crate::runtime::fd::raw_fd_is_closed(owned_fd));
    }

    #[cfg(not(miri))]
    #[test]
    fn tcp_connect_faults_reset_borrowed_and_owned_slots() {
        crate::runtime::executor::with_ringless_poll_context_for_test(1, |owner, cx| {
            let remote_addr = SocketAddr::from(([127, 0, 0, 1], 9));
            let reusable_fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("reusable connect fd creation failed");
            let mut reusable = ConnectSlot::new(());
            reusable.in_use = true;
            // SAFETY: the test-created descriptor has no other owner.
            reusable.fd = Some(unsafe { OwnedFd::from_raw_fd(reusable_fd) });
            reusable.addr = Some(RetainedConnectAddr::from_socket_addr(remote_addr));

            crate::runtime::test_hooks::fail_next_op_alloc();
            assert!(matches!(
                poll_tcp_connect(&mut reusable, cx),
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::WouldBlock
            ));
            assert!(reusable.state_ptr.is_null());
            assert!(!reusable.in_use);
            assert!(reusable.fd.is_none());
            assert!(
                reusable.addr.is_some(),
                "allocation failure must precede retained-address transfer"
            );
            assert!(crate::runtime::fd::raw_fd_is_closed(reusable_fd));

            prepare_connect_slot(&mut reusable, remote_addr)
                .expect("allocation-failed reusable slot did not prepare again");
            reusable.cleanup_fd();
            let retry_fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("retry connect fd creation failed");
            // SAFETY: the test-created descriptor has no other owner.
            reusable.fd = Some(unsafe { OwnedFd::from_raw_fd(retry_fd) });
            drop(ConnectFuture {
                slot: &mut reusable,
            });
            assert!(crate::runtime::fd::raw_fd_is_closed(retry_fd));

            let owned_fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("owned connect fd creation failed");
            let mut owned_slot = ConnectSlot::new(());
            owned_slot.in_use = true;
            // SAFETY: the test-created descriptor has no other owner.
            owned_slot.fd = Some(unsafe { OwnedFd::from_raw_fd(owned_fd) });
            owned_slot.addr = Some(RetainedConnectAddr::from_socket_addr(remote_addr));
            let mut owned = OwnedConnectFuture { slot: owned_slot };

            crate::runtime::test_hooks::fail_next_sqe_submit();
            assert!(matches!(
                Pin::new(&mut owned).poll(cx),
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::WouldBlock
            ));
            assert!(owned.slot.state_ptr.is_null());
            assert!(!owned.slot.in_use);
            assert!(owned.slot.fd.is_none());
            assert!(
                owned.slot.addr.is_none(),
                "submission failure must retire the transferred address"
            );
            assert!(crate::runtime::fd::raw_fd_is_closed(owned_fd));
            assert_eq!(
                crate::runtime::test_hooks::raw_sqe_submit_failures_remaining(),
                0
            );

            let reactor = owner.reactor_ptr();
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn ring_abandoned_tcp_connect_retains_submitted_socket_with_state() {
        crate::runtime::executor::with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state_ptr.is_null(), "TCP connect state allocation failed");
            let fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("TCP connect fd creation failed");
            // SAFETY: the test-created descriptor has no other owner.
            let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let retained = unsafe {
                (&mut *reactor).alloc_retained_payload(crate::net::RetainedConnectPayload::new(
                    owned_fd,
                    RetainedConnectAddr::from_socket_addr(SocketAddr::from(([127, 0, 0, 1], 9))),
                ))
            };
            unsafe {
                (*state_ptr).attach_retained_payload(retained);
                (*state_ptr).set_ring_abandoned();
            }

            let mut slot = ConnectSlot::new(());
            slot.state_ptr = state_ptr;
            slot.in_use = true;
            assert!(matches!(
                poll_tcp_connect(&mut slot, cx),
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
            ));
            assert!(slot.state_ptr.is_null());
            assert!(!slot.in_use);
            assert!(slot.fd.is_none());
            assert!(slot.addr.is_none());
            assert!(unsafe { (*state_ptr).is_ring_abandoned() });
            assert!(!crate::runtime::fd::raw_fd_is_closed(fd));

            unsafe {
                (*state_ptr).restore_completed_orphaned_after_ringless_abandonment_for_test();
                crate::runtime::reactor::Reactor::free_op_unchecked(reactor, state_ptr);
            }
            assert!(crate::runtime::fd::raw_fd_is_closed(fd));
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn tcp_terminal_accept_readiness_latches_and_later_accepts_fail_fast() {
        crate::net::test_terminal_accept_readiness(
            "TCP",
            |slot, cx, state_ptr| {
                slot.state_ptr = state_ptr;
                slot.in_use = true;
                let mut accept = AcceptFuture {
                    slot,
                    input_error: None,
                    prepared: true,
                };
                let outcome = Future::poll(Pin::new(&mut accept), cx);
                drop(accept);
                outcome
            },
            |slot, cx| {
                let input_error = slot.prepare().err();
                let prepared = input_error.is_none();
                let mut accept = AcceptFuture {
                    slot,
                    input_error,
                    prepared,
                };
                let outcome = Future::poll(Pin::new(&mut accept), cx);
                drop(accept);
                outcome
            },
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn tcp_bare_poll_error_budget_exhaustion_does_not_latch_listener() {
        crate::net::test_bare_poll_error_budget_exhaustion("TCP", |slot, cx, state_ptr| {
            slot.state_ptr = state_ptr;
            slot.in_use = true;
            let mut accept = AcceptFuture {
                slot,
                input_error: None,
                prepared: true,
            };
            let outcome = Future::poll(Pin::new(&mut accept), cx);
            drop(accept);
            outcome
        });
    }

    #[test]
    fn tcp_unprepared_accept_future_parks_without_touching_slot() {
        crate::net::test_unprepared_accept_future_parks("TCP", |slot, cx, input_error| {
            let mut accept = AcceptFuture {
                slot,
                input_error: Some(input_error),
                prepared: false,
            };
            let first = Future::poll(Pin::new(&mut accept), cx);
            let second = Future::poll(Pin::new(&mut accept), cx);
            drop(accept);
            (first, second)
        });
    }

    #[test]
    fn accepted_owned_fd_closes_when_peer_address_decode_fails() {
        let raw = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("distinctive fd creation failed");
        // SAFETY: the test helper returned one descriptor owned only here.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: all-zero socket-address storage is valid initialized bytes;
        // the zero length makes decoding fail before its family is consulted.
        let addr = unsafe { std::mem::zeroed() };

        if finish_accepted_stream(owned, &addr, 0).is_ok() {
            panic!("zero-length accepted address should fail");
        }
        assert!(
            crate::runtime::fd::raw_fd_is_closed(raw),
            "accepted TCP fd leaked on address decode failure"
        );
    }

    #[test]
    fn failed_split_clone_does_not_taint_the_source() {
        let source = RuntimeFd::from_fresh_raw_fd(-1);
        let result = finish_split_clone(&source, Err(io::Error::from_raw_os_error(libc::EMFILE)));

        let err = match result {
            Err(err) => err,
            Ok(_) => panic!("injected duplication failure should surface"),
        };
        assert_eq!(err.raw_os_error(), Some(libc::EMFILE));
        assert_eq!(
            source.linger_provenance(),
            LingerProvenance::KnownNonPositive
        );
    }

    #[test]
    fn internal_raw_fd_access_preserves_linger_provenance() {
        let stream = TcpStream {
            fd: RuntimeFd::from_fresh_raw_fd(-1),
        };

        assert_eq!(stream.raw_fd_for_internal_io(), -1);
        assert_eq!(
            stream.fd.linger_provenance(),
            LingerProvenance::KnownNonPositive
        );

        assert_eq!(stream.as_raw_fd(), -1);
        assert_eq!(stream.fd.linger_provenance(), LingerProvenance::Uncertain);
    }
}
