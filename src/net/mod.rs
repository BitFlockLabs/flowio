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
//! UDP remains single-datagram and therefore uses single-buffer sends and
//! receives only.
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
//! The same transport methods also work with [`IoBuffMut`]:
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
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::RawFd;

pub mod sctp;
pub(crate) mod stream;
pub mod tcp;
pub mod udp;
pub mod unix;

// ---------------------------------------------------------------------------
// Shared option helpers — avoid expect()/unwrap() in fast-path code.
// All I/O futures store buffers in Option<B> and consume them exactly once.
// These helpers replace expect()/unwrap() with debug_assert + unwrap_unchecked.
// ---------------------------------------------------------------------------

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_take<T>(opt: &mut Option<T>) -> T
{
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.take().unwrap_unchecked() }
}

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_ref<T>(opt: &Option<T>) -> &T
{
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.as_ref().unwrap_unchecked() }
}

/// # Safety
/// The caller must guarantee the option is `Some`.
#[inline(always)]
pub(crate) unsafe fn opt_mut<T>(opt: &mut Option<T>) -> &mut T
{
    debug_assert!(opt.is_some(), "buffer option was None (internal invariant)");
    unsafe { opt.as_mut().unwrap_unchecked() }
}

pub(crate) fn checked_read_len(_op: &str, requested: usize, writable: usize) -> io::Result<u32>
{
    if requested > writable
    {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    if requested > u32::MAX as usize
    {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    Ok(requested as u32)
}

fn socket_domain(addr: SocketAddr) -> libc::c_int
{
    match addr
    {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

fn new_nonblocking_socket(domain: libc::c_int, kind: libc::c_int) -> io::Result<RawFd>
{
    let fd = unsafe { libc::socket(domain, kind | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) };
    if fd < 0
    {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

#[inline(always)]
fn close_fd(fd: RawFd)
{
    unsafe {
        libc::close(fd);
    }
}

#[inline(always)]
fn close_if_valid(fd: &mut RawFd)
{
    if *fd >= 0
    {
        close_fd(*fd);
        *fd = -1;
    }
}

fn set_reuse_addr(fd: RawFd) -> io::Result<()>
{
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, &1i32)
}

fn socket_addr_to_c(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t)
{
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len;

    match addr
    {
        SocketAddr::V4(v4) =>
        {
            let sockaddr_in = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sockaddr_in.sin_family = libc::AF_INET as libc::sa_family_t;
            sockaddr_in.sin_port = v4.port().to_be();
            sockaddr_in.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        }
        SocketAddr::V6(v6) =>
        {
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
) -> io::Result<SocketAddr>
{
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

fn current_local_addr(fd: RawFd) -> io::Result<SocketAddr>
{
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let rc =
        unsafe { libc::getsockname(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0
    {
        return Err(io::Error::last_os_error());
    }

    socket_addr_from_c(&storage, len)
}

fn current_peer_addr(fd: RawFd) -> io::Result<SocketAddr>
{
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let rc =
        unsafe { libc::getpeername(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0
    {
        return Err(io::Error::last_os_error());
    }

    socket_addr_from_c(&storage, len)
}

fn set_reuse_port(fd: RawFd) -> io::Result<()>
{
    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, &1i32)
}

// Shared socket buffer option helpers used by TcpStream, UnixStream, and UdpSocket.

fn sock_send_buffer_size(fd: RawFd) -> io::Result<usize>
{
    let val: libc::c_int = get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF)?;
    Ok(val as usize)
}

fn set_sock_send_buffer_size(fd: RawFd, size: usize) -> io::Result<()>
{
    set_sock_opt(
        fd,
        libc::SOL_SOCKET,
        libc::SO_SNDBUF,
        &(size as libc::c_int),
    )
}

fn sock_recv_buffer_size(fd: RawFd) -> io::Result<usize>
{
    let val: libc::c_int = get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF)?;
    Ok(val as usize)
}

fn set_sock_recv_buffer_size(fd: RawFd, size: usize) -> io::Result<()>
{
    set_sock_opt(
        fd,
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        &(size as libc::c_int),
    )
}

fn set_sock_opt<T>(fd: RawFd, level: libc::c_int, name: libc::c_int, value: &T) -> io::Result<()>
{
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            value as *const T as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn get_sock_opt<T: Default>(fd: RawFd, level: libc::c_int, name: libc::c_int) -> io::Result<T>
{
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
    if rc < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}
