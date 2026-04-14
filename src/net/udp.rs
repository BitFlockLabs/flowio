//! UDP transport for connected and unconnected datagram flows.
//!
//! All send/recv operations are single-datagram and use the rental pattern —
//! the caller-provided buffer is consumed and returned alongside the result on
//! completion.  Any type implementing [`IoBuffReadOnly`] / [`IoBuffReadWrite`] can be used
//! (`Vec<u8>`, `Box<[u8]>`, etc.).
//!
//! [`IoBuffReadOnly`]: crate::runtime::buffer::IoBuffReadOnly
//! [`IoBuffReadWrite`]: crate::runtime::buffer::IoBuffReadWrite
//!
//! # Example
//! ```no_run
//! use flowio::net::udp::UdpSocket;
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
//!     let peer = socket.local_addr();
//!     socket.connect(peer).unwrap();
//!
//!     let (res, _buf) = socket.send(b"ping".to_vec()).await;
//!     res.unwrap();
//!
//!     let (res, buf) = socket.recv(vec![0u8; 4], 4).await;
//!     let len = res.unwrap();
//!     assert_eq!(&buf[..len], b"ping");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The same single-datagram API works with [`IoBuffMut`]:
//! ```no_run
//! use flowio::net::udp::UdpSocket;
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
//!     let peer = socket.local_addr();
//!     socket.connect(peer).unwrap();
//!
//!     let mut send = IoBuffMut::new(0, 64, 0).unwrap();
//!     send.payload_append(b"ping").unwrap();
//!     let (res, _send) = socket.send(send).await;
//!     res.unwrap();
//!
//!     let recv = IoBuffMut::new(0, 64, 0).unwrap();
//!     let (res, recv) = socket.recv(recv, 4).await;
//!     let len = res.unwrap();
//!     assert_eq!(len, 4);
//!     assert_eq!(recv.payload_bytes(), b"ping");
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use super::{
    checked_read_len, close_fd, current_local_addr, get_sock_opt, new_nonblocking_socket,
    set_reuse_addr, set_sock_opt, socket_addr_from_c, socket_addr_to_c, socket_domain,
};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{drop_op_ptr_unchecked, poll_ctx_from_waker, submit_tracked_sqe};
use crate::runtime::fd::RuntimeFd;
use crate::runtime::op::CompletionState;
use io_uring::{opcode, types};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Datagram socket with generic buffer support.
///
/// # Example
/// ```no_run
/// use flowio::net::udp::UdpSocket;
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
/// socket.connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 9000)))?;
/// let mut executor = Executor::new()?;
/// executor.run(async move {
///     let (res, _) = socket.send(b"hello".to_vec()).await;
///     res.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct UdpSocket
{
    fd: RuntimeFd,
    local_addr: SocketAddr,
    peer_addr: Option<SocketAddr>,
}

impl UdpSocket
{
    /// Binds a UDP socket to the requested local address.
    pub fn bind(addr: SocketAddr) -> io::Result<Self>
    {
        let fd = new_nonblocking_socket(socket_domain(addr), libc::SOCK_DGRAM)?;

        if let Err(err) = set_reuse_addr(fd)
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
            peer_addr: None,
        })
    }

    /// Returns the local address currently assigned to the socket.
    pub fn local_addr(&self) -> SocketAddr
    {
        self.local_addr
    }

    /// Returns the connected peer address, or `None` if not connected.
    pub fn peer_addr(&self) -> Option<SocketAddr>
    {
        self.peer_addr
    }

    /// Connects the socket to a default peer for `send` and `recv`.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<()>
    {
        let (sockaddr, sockaddr_len) = socket_addr_to_c(addr);
        let rc = unsafe {
            libc::connect(
                self.fd.as_raw_fd(),
                &sockaddr as *const _ as *const libc::sockaddr,
                sockaddr_len,
            )
        };
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        self.peer_addr = Some(addr);

        Ok(())
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

    /// Enables or disables `SO_BROADCAST`.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()>
    {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BROADCAST,
            &(broadcast as libc::c_int),
        )
    }

    /// Returns the current `SO_BROADCAST` setting.
    pub fn broadcast(&self) -> io::Result<bool>
    {
        let val: libc::c_int =
            get_sock_opt(self.fd.as_raw_fd(), libc::SOL_SOCKET, libc::SO_BROADCAST)?;
        Ok(val != 0)
    }

    /// Starts one connected receive into the provided buffer.
    pub fn recv<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvFuture<'_, B>
    {
        let mut input_error = None;
        let len = match checked_read_len("recv", len, buffer.writable_len())
        {
            Ok(len) => len,
            Err(err) =>
            {
                input_error = Some(err);
                0
            }
        };
        RecvFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one connected send from the provided buffer.
    pub fn send<B: IoBuffReadOnly>(&mut self, buffer: B) -> SendFuture<'_, B>
    {
        SendFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            _marker: PhantomData,
        }
    }

    /// Starts one unconnected receive and also returns the sending peer.
    pub fn recv_from<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize)
    -> RecvFromFuture<'_, B>
    {
        let mut input_error = None;
        let len = match checked_read_len("recv_from", len, buffer.writable_len())
        {
            Ok(len) => len,
            Err(err) =>
            {
                input_error = Some(err);
                0
            }
        };
        RecvFromFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            input_error,
            addr: MaybeUninit::uninit(),
            addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            iovec: MaybeUninit::uninit(),
            msghdr: MaybeUninit::uninit(),
            _marker: PhantomData,
        }
    }

    /// Starts one unconnected send to the provided destination.
    pub fn send_to<B: IoBuffReadOnly>(&mut self, buffer: B, addr: SocketAddr)
    -> SendToFuture<'_, B>
    {
        let (storage, addrlen) = socket_addr_to_c(addr);
        SendToFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            addr: storage,
            addrlen,
            iovec: MaybeUninit::uninit(),
            msghdr: MaybeUninit::uninit(),
            _marker: PhantomData,
        }
    }
}

impl AsRawFd for UdpSocket
{
    fn as_raw_fd(&self) -> RawFd
    {
        self.fd.as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// Option helpers — avoid expect()/unwrap() in fast-path code.
use super::{opt_mut, opt_ref, opt_take};

// ---------------------------------------------------------------------------
// RecvFuture (connected recv via IORING_OP_RECV)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvFuture<'a, B: IoBuffReadWrite>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    len: u32,
    input_error: Option<io::Error>,
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvFuture<'_, B>
{
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                if result < 0
                {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }
                let actual = result as usize;
                unsafe { buffer.set_written_len(actual) };
                return Poll::Ready((Ok(actual), buffer));
            }
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let buf = unsafe { opt_mut(&mut this.buffer) };
            let ptr = buf.as_mut_ptr();
            let sqe = opcode::Recv::new(types::Fd(this.fd), ptr, this.len)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendFuture (connected send via IORING_OP_SEND)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendFuture<'a, B: IoBuffReadOnly>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadOnly> Future for SendFuture<'_, B>
{
    type Output = (io::Result<usize>, B);

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

                let buffer = unsafe { opt_take(&mut this.buffer) };
                if result < 0
                {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }
                return Poll::Ready((Ok(result as usize), buffer));
            }
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let buf = unsafe { opt_ref(&this.buffer) };
            let ptr = buf.as_ptr();
            let len = buf.len() as u32;
            let sqe = opcode::Send::new(types::Fd(this.fd), ptr, len)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvFromFuture (unconnected recv via IORING_OP_RECVMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvFromFuture<'a, B: IoBuffReadWrite>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    len: u32,
    input_error: Option<io::Error>,
    addr: MaybeUninit<libc::sockaddr_storage>,
    addrlen: libc::socklen_t,
    iovec: MaybeUninit<libc::iovec>,
    msghdr: MaybeUninit<libc::msghdr>,
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvFromFuture<'_, B>
{
    type Output = (io::Result<(usize, SocketAddr)>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if !this.state_ptr.is_null()
        {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                if result < 0
                {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }

                let actual = result as usize;
                unsafe { buffer.set_written_len(actual) };

                let msg = unsafe { this.msghdr.assume_init_ref() };
                let addr = match unsafe {
                    socket_addr_from_c(this.addr.assume_init_ref(), msg.msg_namelen)
                }
                {
                    Ok(addr) => addr,
                    Err(err) => return Poll::Ready((Err(err), buffer)),
                };
                return Poll::Ready((Ok((actual, addr)), buffer));
            }
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let buf = unsafe { opt_mut(&mut this.buffer) };
            let buffer_ptr = buf.as_mut_ptr();

            this.iovec.write(libc::iovec {
                iov_base: buffer_ptr as *mut libc::c_void,
                iov_len: this.len as usize,
            });
            this.msghdr.write(libc::msghdr {
                msg_name: this.addr.as_mut_ptr() as *mut libc::c_void,
                msg_namelen: this.addrlen,
                msg_iov: this.iovec.as_mut_ptr(),
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            });

            let sqe = opcode::RecvMsg::new(types::Fd(this.fd), this.msghdr.as_mut_ptr())
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFromFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendToFuture (unconnected send via IORING_OP_SENDMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendToFuture<'a, B: IoBuffReadOnly>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    addr: libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    iovec: MaybeUninit<libc::iovec>,
    msghdr: MaybeUninit<libc::msghdr>,
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadOnly> Future for SendToFuture<'_, B>
{
    type Output = (io::Result<usize>, B);

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

                let buffer = unsafe { opt_take(&mut this.buffer) };
                if result < 0
                {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }
                return Poll::Ready((Ok(result as usize), buffer));
            }
        }

        if this.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let buf = unsafe { opt_ref(&this.buffer) };
            let buffer_ptr = buf.as_ptr();
            let len = buf.len();

            this.iovec.write(libc::iovec {
                iov_base: buffer_ptr as *mut libc::c_void,
                iov_len: len,
            });
            this.msghdr.write(libc::msghdr {
                msg_name: &mut this.addr as *mut libc::sockaddr_storage as *mut libc::c_void,
                msg_namelen: this.addrlen,
                msg_iov: this.iovec.as_mut_ptr(),
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            });

            let sqe = opcode::SendMsg::new(types::Fd(this.fd), this.msghdr.as_ptr())
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    let buffer = opt_take(&mut this.buffer);
                    return Poll::Ready((Err(e), buffer));
                }
            }
        }

        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendToFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}
