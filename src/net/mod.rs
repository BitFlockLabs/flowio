//! Transport implementations built on top of the runtime core.
//!
//! Each transport exposes generic buffer I/O through the [`IoBuffReadOnly`] /
//! [`IoBuffReadWrite`] traits — any type that provides a stable pointer to a
//! contiguous byte region (`Vec<u8>`, `Box<[u8]>`, etc.) can be used directly.
//!
//! Stream transports also expose concrete vectored APIs built on
//! [`IoBuffVec`](crate::runtime::buffer::iobuffvec::IoBuffVec) and
//! [`IoBuffVecMut`](crate::runtime::buffer::iobuffvec::IoBuffVecMut):
//! - TCP / Unix: `readv`, `writev`, `writev_all`, `readv_exact`
//! - SCTP: `recv_msg_vectored`, `send_msg_vectored`
//!
//! TCP and Unix streams also support [`WritevProjection`], which lets a caller
//! pass one compact owned carrier and project borrowed byte pieces from that
//! retained carrier into FlowIO-owned kernel-facing `iovec` scratch.
//!
//! Client-side TLS is provided separately by [`tls`], which wraps an existing
//! connected [`tcp::TcpStream`] with an explicit rustls-driven handshake and
//! encrypted I/O API.
//!
//! Hostname resolution is provided by [`resolver`], which offers a small
//! FlowIO-native DNS helper for turning host names into `SocketAddr` values
//! before connecting transports such as TCP or SCTP.
//!
//! UDP remains single-datagram and therefore uses single-buffer sends and
//! receives only.
//!
//! # Error Semantics
//!
//! Async transport operations return `io::Error` through their futures. For
//! rental I/O methods, the caller-owned buffer or chain is returned alongside
//! the result, including on recoverable errors.
//!
//! [`io::ErrorKind::WouldBlock`] can report internal FlowIO pressure rather
//! than socket readiness: completion-state capacity exhaustion capped by the
//! executor's `ReactorConfig::ring_entries`, io_uring submission-queue
//! pressure, retained `iovec` scratch allocation pressure, or a reusable
//! listener/connector slot that is still occupied by an active or intentionally
//! forgotten future. The first three cases may become available after the
//! executor makes progress; a busy reusable slot becomes available only when
//! the previous future completes or is dropped, or when the owning
//! listener/connector is dropped.
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - For repeated outbound connections, prefer reusable connector types such
//!   as [`tcp::TcpConnector`] and [`sctp::SctpConnector`] because they keep
//!   stable connector state across attempts.
//! - On stream transports, `read` / `write` and `readv` / `writev` are the
//!   lowest-overhead I/O APIs when the caller can handle partial progress.
//! - For fixed-peer UDP, prefer [`udp::UdpSocket::connect`] plus `send` /
//!   `recv` because that avoids per-datagram destination handling.
//! - Use vectored APIs only when payloads are already segmented. For one
//!   contiguous payload, the contiguous APIs are the simpler fast-path
//!   alternative.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use the one-shot connect helpers in repeated outbound
//!   loops. Use [`tcp::TcpConnector`] or [`sctp::SctpConnector`] instead.
//! - Prefer not to use `_exact` / `_all` variants unless complete-buffer
//!   semantics are required. Use partial-I/O APIs instead when the caller can
//!   track progress explicitly.
//! - Prefer not to use `send_to` / `recv_from` when the peer is stable. Use
//!   connected UDP `send` / `recv` instead.
//! - Prefer not to resolve names in the steady-state data path. [`resolver`]
//!   is a setup/control-plane helper; resolve once and reuse the resulting
//!   `SocketAddr` values.
//!
//! The examples below often use `_all` / `_exact` variants because they make
//! protocol framing obvious in docs. On the hot path, prefer partial-I/O APIs
//! when the caller can manage progress explicitly.
//!
//! [`IoBuffReadOnly`]: crate::runtime::buffer::IoBuffReadOnly
//! [`IoBuffReadWrite`]: crate::runtime::buffer::IoBuffReadWrite
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
//!     let (res, _buf) = left.write_all(b"hello".to_vec()).await;
//!     res.unwrap();
//!
//!     let (res, buf) = right.read_exact(vec![0u8; 5], 5).await;
//!     res.unwrap();
//!     assert_eq!(&buf[..], b"hello");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same transport methods also work with [`crate::runtime::buffer::IoBuffMut`]:
//! ```no_run
//! use flowio::net::unix::UnixStream;
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::executor::Executor;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let (mut left, mut right) = UnixStream::pair().unwrap();
//!
//!     let mut send = IoBuffMut::new(0, 64, 0).unwrap();
//!     send.payload_append(b"hello").unwrap();
//!     let (res, _send) = left.write_all(send).await;
//!     res.unwrap();
//!
//!     let recv = IoBuffMut::new(0, 64, 0).unwrap();
//!     let (res, recv) = right.read_exact(recv, 5).await;
//!     res.unwrap();
//!     assert_eq!(recv.payload_bytes(), b"hello");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use std::io;
use std::mem::MaybeUninit;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::RawFd;

pub mod resolver;
pub mod sctp;
pub(crate) mod send_sqe;
pub(crate) mod stream;
pub mod tcp;
pub mod tls;
#[cfg(debug_assertions)]
#[doc(hidden)]
pub mod tls_test_peer;
pub mod udp;
pub mod unix;

/// Safe projection interface for retained owned vectored writes.
///
/// Implement this for compact owned message carriers that can expose their
/// already-encoded byte pieces as borrowed slices. FlowIO moves the carrier
/// into retained operation state before calling [`WritevProjection::project_writev`],
/// so slices may safely point into inline fields or owned allocations inside
/// the carrier. The retained carrier and FlowIO-owned `iovec` scratch remain
/// alive until the original write CQE retires, even if the future is dropped.
///
/// `writev_count_and_len` must report the number of active non-empty pieces
/// and the total byte length that `project_writev` will push. Empty pieces are
/// ignored by [`WritevPieces::push`]. Mismatches are rejected with
/// [`io::ErrorKind::InvalidInput`].
///
/// This trait does not expose a borrowed-SQE API. Callers pass ownership of
/// the carrier to the stream method and receive it back with the I/O result.
///
/// This is a fast-path API when a protocol already owns a compact message
/// carrier with segmented byte fields. Prefer the contiguous stream `write`
/// APIs for one contiguous byte range, and prefer non-`_all` projected writes
/// when the caller can track partial progress explicitly.
///
/// # Example
/// ```no_run
/// use flowio::net::tcp::TcpStream;
/// use flowio::net::{WritevPieces, WritevProjection};
/// use std::io;
///
/// struct Message {
///     header: [u8; 4],
///     body: Vec<u8>,
/// }
///
/// impl WritevProjection for Message {
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
/// # async fn send(mut stream: TcpStream, msg: Message) -> io::Result<Message> {
/// let (result, msg) = stream.writev_projected(msg).await;
/// result?;
/// Ok(msg)
/// # }
/// ```
pub trait WritevProjection: 'static {
    /// Returns `(active_non_empty_piece_count, total_byte_len)`.
    fn writev_count_and_len(&self) -> (usize, usize);

    /// Projects borrowed byte pieces from this retained carrier.
    ///
    /// The lifetime on `pieces` ties every pushed slice to the borrow of
    /// `self`, preventing safe implementations from pushing temporary slices
    /// that die when this method returns.
    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()>;
}

/// Sink used by [`WritevProjection`] implementations to expose write pieces.
///
/// Values of this type are constructed only by FlowIO. Implementations push
/// slices borrowed from the retained carrier; FlowIO stores only pointer/length
/// metadata in retained scratch and never copies the slice bytes.
///
/// This type belongs to the projected vectored-write fast path. It should be
/// used only inside [`WritevProjection::project_writev`]; callers do not build
/// it directly.
///
/// # Example
/// ```
/// use flowio::net::{WritevPieces, WritevProjection};
/// use std::io;
///
/// struct Pair {
///     first: [u8; 2],
///     second: [u8; 2],
/// }
///
/// impl WritevProjection for Pair {
///     fn writev_count_and_len(&self) -> (usize, usize) {
///         (2, self.first.len() + self.second.len())
///     }
///
///     fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
///         pieces.push(&self.first)?;
///         pieces.push(&self.second)?;
///         Ok(())
///     }
/// }
/// ```
pub struct WritevPieces<'a> {
    /// FlowIO-owned retained scratch where projected slice metadata is written.
    iovecs: &'a mut [MaybeUninit<libc::iovec>],
    /// Number of initialized non-empty `iovec` entries.
    count: usize,
    /// Sum of bytes represented by initialized entries.
    total: usize,
}

impl<'a> WritevPieces<'a> {
    #[inline(always)]
    pub(crate) fn new(iovecs: &'a mut [MaybeUninit<libc::iovec>]) -> Self {
        Self {
            iovecs,
            count: 0,
            total: 0,
        }
    }

    /// Adds a non-empty byte piece to the projected write.
    ///
    /// Empty slices are ignored, matching FlowIO's existing owned-chain
    /// `writev` behavior. If the projection pushes more non-empty pieces than
    /// were reported by `writev_count_and_len`, this returns
    /// [`io::ErrorKind::InvalidInput`].
    #[inline(always)]
    pub fn push(&mut self, bytes: &'a [u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        if self.count >= self.iovecs.len() {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let total = self
            .total
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;

        self.iovecs[self.count].write(libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        });
        self.count += 1;
        self.total = total;
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn count(&self) -> usize {
        self.count
    }

    #[inline(always)]
    pub(crate) fn total(&self) -> usize {
        self.total
    }
}

// ---------------------------------------------------------------------------
// Shared option helpers for rental-pattern futures.
// All I/O futures store buffers in `Option<B>` so they can move the buffer out
// exactly once on completion or error. These helpers preserve that invariant
// without carrying `expect()` branches in the hot path.
// ---------------------------------------------------------------------------

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_take<T>(opt: &mut Option<T>) -> T {
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.take().unwrap_unchecked() }
}

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_ref<T>(opt: &Option<T>) -> &T {
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.as_ref().unwrap_unchecked() }
}

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_mut<T>(opt: &mut Option<T>) -> &mut T {
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.as_mut().unwrap_unchecked() }
}

const READ_LEN_EXCEEDS_WRITABLE: &str = "length exceeds writable buffer capacity";
const LEN_EXCEEDS_U32: &str = "length exceeds io_uring u32 byte-count limit";

#[inline(always)]
pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[inline(always)]
pub(crate) fn invalid_input_kind() -> io::Error {
    io::Error::from(io::ErrorKind::InvalidInput)
}

/// Validates a caller-supplied read length against the writable capacity that
/// the buffer actually exposes to the kernel.
pub(crate) fn checked_read_len(_op: &str, requested: usize, writable: usize) -> io::Result<u32> {
    if requested > writable {
        return Err(invalid_input(READ_LEN_EXCEEDS_WRITABLE));
    }
    if requested > u32::MAX as usize {
        return Err(invalid_input(LEN_EXCEEDS_U32));
    }

    Ok(requested as u32)
}

/// Validates a contiguous send length against io_uring opcodes that accept a
/// 32-bit byte count.
pub(crate) fn checked_send_len(_op: &str, requested: usize) -> io::Result<u32> {
    if requested > u32::MAX as usize {
        return Err(invalid_input(LEN_EXCEEDS_U32));
    }

    Ok(requested as u32)
}

fn socket_domain(addr: SocketAddr) -> libc::c_int {
    match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

fn new_nonblocking_socket(domain: libc::c_int, kind: libc::c_int) -> io::Result<RawFd> {
    let fd = unsafe { libc::socket(domain, kind | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

#[inline(always)]
fn close_fd(fd: RawFd) {
    unsafe {
        libc::close(fd);
    }
}

#[inline(always)]
fn close_if_valid(fd: &mut RawFd) {
    if *fd >= 0 {
        close_fd(*fd);
        *fd = -1;
    }
}

fn set_reuse_addr(fd: RawFd) -> io::Result<()> {
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, &1i32)
}

fn socket_addr_to_c(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len;

    match addr {
        SocketAddr::V4(v4) => {
            let sockaddr_in = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sockaddr_in.sin_family = libc::AF_INET as libc::sa_family_t;
            sockaddr_in.sin_port = v4.port().to_be();
            sockaddr_in.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        }
        SocketAddr::V6(v6) => {
            let sockaddr_in6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sockaddr_in6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sockaddr_in6.sin6_port = v6.port().to_be();
            sockaddr_in6.sin6_addr.s6_addr = v6.ip().octets();
            sockaddr_in6.sin6_flowinfo = v6.flowinfo();
            sockaddr_in6.sin6_scope_id = v6.scope_id();
            len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        }
    }

    (storage, len)
}

fn socket_addr_from_c(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    let family = storage.ss_family as libc::c_int;

    if family == libc::AF_INET && len >= std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
    {
        let sockaddr_in = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
        let ip = std::net::Ipv4Addr::from(sockaddr_in.sin_addr.s_addr.to_ne_bytes());
        let port = u16::from_be(sockaddr_in.sin_port);
        return Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)));
    }

    if family == libc::AF_INET6
        && len >= std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
    {
        let sockaddr_in6 = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
        let ip = std::net::Ipv6Addr::from(sockaddr_in6.sin6_addr.s6_addr);
        let port = u16::from_be(sockaddr_in6.sin6_port);
        return Ok(SocketAddr::V6(SocketAddrV6::new(
            ip,
            port,
            sockaddr_in6.sin6_flowinfo,
            sockaddr_in6.sin6_scope_id,
        )));
    }

    Err(io::Error::from(io::ErrorKind::InvalidData))
}

fn current_local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let rc =
        unsafe { libc::getsockname(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    socket_addr_from_c(&storage, len)
}

fn current_peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let rc =
        unsafe { libc::getpeername(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    socket_addr_from_c(&storage, len)
}

pub(super) struct MsgHdrInit {
    pub(super) name: *mut libc::c_void,
    pub(super) namelen: libc::socklen_t,
    pub(super) iov: *mut libc::iovec,
    pub(super) iovlen: usize,
    pub(super) control: *mut libc::c_void,
    pub(super) controllen: usize,
}

#[inline(always)]
pub(super) fn write_msghdr(dst: &mut MaybeUninit<libc::msghdr>, init: MsgHdrInit) {
    dst.write(libc::msghdr {
        msg_name: init.name,
        msg_namelen: init.namelen,
        msg_iov: init.iov,
        msg_iovlen: init.iovlen,
        msg_control: init.control,
        msg_controllen: init.controllen,
        msg_flags: 0,
    });
}

fn set_reuse_port(fd: RawFd) -> io::Result<()> {
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, &1i32)
}

// Shared socket buffer option helpers used by TcpStream, UnixStream, and UdpSocket.

fn sock_send_buffer_size(fd: RawFd) -> io::Result<usize> {
    let val: libc::c_int = get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF)?;
    Ok(val as usize)
}

fn set_sock_send_buffer_size(fd: RawFd, size: usize) -> io::Result<()> {
    let size = socket_buffer_size_to_c_int(size)?;
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &size)
}

fn sock_recv_buffer_size(fd: RawFd) -> io::Result<usize> {
    let val: libc::c_int = get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF)?;
    Ok(val as usize)
}

fn set_sock_recv_buffer_size(fd: RawFd, size: usize) -> io::Result<()> {
    let size = socket_buffer_size_to_c_int(size)?;
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &size)
}

fn socket_buffer_size_to_c_int(size: usize) -> io::Result<libc::c_int> {
    if size > libc::c_int::MAX as usize {
        return Err(invalid_input("socket buffer size exceeds c_int::MAX"));
    }

    Ok(size as libc::c_int)
}

fn set_sock_opt<T>(fd: RawFd, level: libc::c_int, name: libc::c_int, value: &T) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            value as *const T as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn get_sock_opt<T: Default>(fd: RawFd, level: libc::c_int, name: libc::c_int) -> io::Result<T> {
    let mut value = T::default();
    let mut len = std::mem::size_of::<T>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            &mut value as *mut T as *mut libc::c_void,
            &mut len,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_buffer_size_conversion_accepts_c_int_max() {
        assert_eq!(
            socket_buffer_size_to_c_int(libc::c_int::MAX as usize).expect("conversion failed"),
            libc::c_int::MAX
        );
    }

    #[test]
    fn socket_buffer_size_conversion_rejects_overflow() {
        let err = socket_buffer_size_to_c_int(libc::c_int::MAX as usize + 1)
            .expect_err("oversize socket buffer should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn checked_read_len_rejects_over_writable_with_static_message() {
        let err = checked_read_len("read", 2, 1).expect_err("oversize read should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), READ_LEN_EXCEEDS_WRITABLE);
    }

    #[test]
    fn checked_lengths_reject_u32_overflow_with_static_message() {
        let oversized = u32::MAX as usize + 1;

        let read_err =
            checked_read_len("read", oversized, usize::MAX).expect_err("oversize read should fail");
        assert_eq!(read_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(read_err.to_string(), LEN_EXCEEDS_U32);

        let send_err = checked_send_len("write", oversized).expect_err("oversize send should fail");
        assert_eq!(send_err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(send_err.to_string(), LEN_EXCEEDS_U32);
    }

    #[test]
    fn msghdr_init_writes_all_kernel_fields() {
        let mut hdr = MaybeUninit::uninit();
        let mut name = 0u8;
        let mut byte = 0u8;
        let mut iovec = libc::iovec {
            iov_base: &mut byte as *mut u8 as *mut libc::c_void,
            iov_len: 1,
        };
        let mut control = [0u8; 8];
        let name_ptr = &mut name as *mut u8 as *mut libc::c_void;
        let iov_ptr = &mut iovec as *mut libc::iovec;
        let control_ptr = control.as_mut_ptr() as *mut libc::c_void;

        write_msghdr(
            &mut hdr,
            MsgHdrInit {
                name: name_ptr,
                namelen: 7,
                iov: iov_ptr,
                iovlen: 1,
                control: control_ptr,
                controllen: control.len(),
            },
        );

        let hdr = unsafe { hdr.assume_init() };
        assert_eq!(hdr.msg_name, name_ptr);
        assert_eq!(hdr.msg_namelen, 7);
        assert_eq!(hdr.msg_iov, iov_ptr);
        assert_eq!(hdr.msg_iovlen, 1);
        assert_eq!(hdr.msg_control, control_ptr);
        assert_eq!(hdr.msg_controllen, control.len());
        assert_eq!(hdr.msg_flags, 0);
    }
}
