//! UDP transport for connected and unconnected datagram flows.
//!
//! All send/recv operations are single-datagram and use the rental pattern —
//! the caller-provided buffer is consumed and returned alongside the result on
//! completion.  Any type implementing [`IoBuffReadOnly`] / [`IoBuffReadWrite`] can be used
//! (`Vec<u8>`, `Box<[u8]>`, etc.).
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - If the peer is stable, call [`UdpSocket::connect`] once and then use
//!   [`UdpSocket::send`] / [`UdpSocket::recv`]. That is the fixed-peer UDP
//!   fast path in this crate. Connected [`UdpSocket::recv`] uses the kernel
//!   `recv` operation, which does not report datagram truncation flags; callers
//!   that must reject oversized connected datagrams should use
//!   [`UdpSocket::recv_msg`].
//! - For fixed-shape datagram buffers on the hot path, prefer
//!   [`crate::runtime::buffer::pool::IoBuffPool`] plus
//!   [`crate::runtime::buffer::IoBuffMut`].
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use [`UdpSocket::send_to`] / [`UdpSocket::recv_from`] when
//!   the peer is stable. Use connected UDP `send` / `recv` instead.
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
//! The same single-datagram API works with [`crate::runtime::buffer::IoBuffMut`]:
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
    MsgHdrInit, checked_read_len, checked_send_len, close_fd, current_local_addr, get_sock_opt,
    new_nonblocking_socket, set_reuse_addr, set_sock_opt, socket_addr_from_c, socket_addr_to_c,
    socket_domain, write_msghdr,
};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    drop_op_ptr_unchecked, poll_ctx_from_waker, refresh_op_waiter_from_waker, submit_retained_sqe,
};
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
/// On the fast path, keep the socket open and connected to a default peer if
/// that peer is stable. Prefer not to rebuild sockets or use `send_to` /
/// `recv_from` in that case; connected `send` / `recv` is the intended
/// fixed-peer fast path. Use `recv_msg` on connected sockets when the caller
/// must reject truncated datagrams.
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
pub struct UdpSocket {
    /// Owned datagram socket descriptor.
    fd: RuntimeFd,
    /// Cached local address assigned after bind.
    local_addr: SocketAddr,
    /// Connected default peer used by `send`/`recv`, if any.
    peer_addr: Option<SocketAddr>,
}

impl UdpSocket {
    /// Binds a UDP socket to the requested local address.
    ///
    /// This is socket setup work. Keep the bound socket alive for steady-state
    /// datagram I/O rather than rebinding per message.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let fd = new_nonblocking_socket(socket_domain(addr), libc::SOCK_DGRAM)?;

        if let Err(err) = set_reuse_addr(fd) {
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
            peer_addr: None,
        })
    }

    /// Returns the local address currently assigned to the socket.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the connected peer address, or `None` if not connected.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// Connects the socket to a default peer for `send` and `recv`.
    ///
    /// For fixed-peer UDP traffic, this is the preferred fast-path setup
    /// because it enables the lower-overhead connected send/recv APIs.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<()> {
        let (sockaddr, sockaddr_len) = socket_addr_to_c(addr);
        let rc = unsafe {
            libc::connect(
                self.fd.as_raw_fd(),
                &sockaddr as *const _ as *const libc::sockaddr,
                sockaddr_len,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        self.peer_addr = Some(addr);

        Ok(())
    }

    /// Sets the `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// socket setup instead of changing it per datagram.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_send_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        super::sock_send_buffer_size(self.fd.as_raw_fd())
    }

    /// Sets the `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// socket setup instead of changing it per datagram.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        super::set_sock_recv_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// Returns the current `SO_RCVBUF` socket receive buffer size.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        super::sock_recv_buffer_size(self.fd.as_raw_fd())
    }

    /// Enables or disables `SO_BROADCAST`.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// socket setup instead of toggling it per datagram.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BROADCAST,
            &(broadcast as libc::c_int),
        )
    }

    /// Returns the current `SO_BROADCAST` setting.
    ///
    /// This is socket status/control-plane lookup, not the per-datagram data
    /// fast path.
    pub fn broadcast(&self) -> io::Result<bool> {
        let val: libc::c_int =
            get_sock_opt(self.fd.as_raw_fd(), libc::SOL_SOCKET, libc::SO_BROADCAST)?;
        Ok(val != 0)
    }

    /// Starts one connected receive into the provided buffer.
    ///
    /// This is the preferred receive API on the fixed-peer UDP fast path.
    pub fn recv<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_read_len("recv", len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
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

    /// Starts one connected `recvmsg` receive into the provided buffer.
    ///
    /// This uses the connected socket peer like [`UdpSocket::recv`], but asks
    /// the kernel for message flags and rejects truncated datagrams with
    /// [`io::ErrorKind::InvalidData`]. Use it when a fixed-peer caller needs
    /// truncation detection; keep [`UdpSocket::recv`] for the lower-overhead
    /// fast path when buffer sizing is guaranteed by the protocol.
    pub fn recv_msg<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvMsgFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_read_len("recv_msg", len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        RecvMsgFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one connected send from the provided buffer.
    ///
    /// This is the preferred send API on the fixed-peer UDP fast path.
    pub fn send<B: IoBuffReadOnly>(&mut self, buffer: B) -> SendFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_send_len("udp send", buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        SendFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one unconnected receive and also returns the sending peer.
    ///
    /// Use this instead of [`UdpSocket::recv`] when peer addresses vary per
    /// datagram or when the sender address is needed by the caller.
    pub fn recv_from<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> RecvFromFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_read_len("recv_from", len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
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
            _marker: PhantomData,
        }
    }

    /// Starts one unconnected send to the provided destination.
    ///
    /// Use this instead of [`UdpSocket::send`] when the destination varies per
    /// datagram.
    pub fn send_to<B: IoBuffReadOnly>(
        &mut self,
        buffer: B,
        addr: SocketAddr,
    ) -> SendToFuture<'_, B> {
        let (storage, addrlen) = socket_addr_to_c(addr);
        SendToFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            addr: storage,
            addrlen,
            _marker: PhantomData,
        }
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// Option helpers — avoid expect()/unwrap() in fast-path code.
use super::opt_take;

struct RetainedRecvPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while connected recv is live.
    buffer: B,
}

struct RetainedRecvMsgPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while connected recvmsg is live.
    buffer: B,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Message header initialized in retained storage before submission.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSendPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while connected send is live.
    buffer: B,
}

// These retained msg payloads become self-referential after their msghdr is
// initialized to point at embedded iovec fields and, for address-bearing
// operations, embedded sockaddr storage. Construct them only inside stable
// retained storage and initialize pointers in the submit_retained_sqe closure,
// after the payload has reached its final address.
struct RetainedRecvFromPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while recvmsg is live.
    buffer: B,
    /// Kernel-written source address storage for the received datagram.
    addr: MaybeUninit<libc::sockaddr_storage>,
    /// Capacity passed to the kernel for `addr`; read back through `msghdr`.
    addrlen: libc::socklen_t,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSendToPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while sendmsg is live.
    buffer: B,
    /// Prepared destination address retained for the kernel.
    addr: libc::sockaddr_storage,
    /// Length of the prepared destination address.
    addrlen: libc::socklen_t,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

fn zeroed_sockaddr_storage() -> MaybeUninit<libc::sockaddr_storage> {
    MaybeUninit::zeroed()
}

// ---------------------------------------------------------------------------
// RecvFuture (connected recv via IORING_OP_RECV)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvFuture<'a, B: IoBuffReadWrite> {
    /// Connected socket descriptor used for this receive.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Maximum datagram bytes requested from the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedRecvPayload<B>>(this.state_ptr)
                };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let mut buffer = payload.buffer;
                if result < 0 {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }
                let actual = result as usize;
                unsafe { buffer.set_written_len(actual) };
                return Poll::Ready((Ok(actual), buffer));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedRecvPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_mut_ptr();
                        Ok(opcode::Recv::new(types::Fd(this.fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvMsgFuture (connected recv via IORING_OP_RECVMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvMsgFuture<'a, B: IoBuffReadWrite> {
    /// Connected socket descriptor used for this receive.
    fd: RawFd,
    /// Completion state for the submitted recvmsg operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Maximum datagram bytes requested from the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvMsgFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedRecvMsgPayload<B>>(this.state_ptr)
                };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let mut buffer = payload.buffer;
                if result < 0 {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }

                let msg = unsafe { payload.msghdr.assume_init_ref() };
                // `msg_control` is null, so MSG_CTRUNC is not expected here;
                // keep the check defensive in case a kernel reports
                // inconsistent recvmsg flags.
                let actual = result as usize;
                unsafe { buffer.set_written_len(actual) };
                if (msg.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC)) != 0 {
                    return Poll::Ready((
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "UDP recv_msg message was truncated",
                        )),
                        buffer,
                    ));
                }

                return Poll::Ready((Ok(actual), buffer));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedRecvMsgPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                iovec: MaybeUninit::uninit(),
                msghdr: MaybeUninit::uninit(),
            };

            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let buffer_ptr = payload.buffer.as_mut_ptr();
                        payload.iovec.write(libc::iovec {
                            iov_base: buffer_ptr as *mut libc::c_void,
                            iov_len: this.len as usize,
                        });
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: std::ptr::null_mut(),
                                namelen: 0,
                                iov: payload.iovec.as_mut_ptr(),
                                iovlen: 1,
                                control: std::ptr::null_mut(),
                                controllen: 0,
                            },
                        );

                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvMsgFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendFuture (connected send via IORING_OP_SEND)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendFuture<'a, B: IoBuffReadOnly> {
    /// Connected socket descriptor used for this send.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned send buffer returned on completion.
    buffer: Option<B>,
    /// Validated datagram byte count submitted to the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadOnly> Future for SendFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedSendPayload<B>>(this.state_ptr)
                };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let buffer = payload.buffer;
                if result < 0 {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }
                return Poll::Ready((Ok(result as usize), buffer));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedSendPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_ptr();
                        Ok(opcode::Send::new(types::Fd(this.fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvFromFuture (unconnected recv via IORING_OP_RECVMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvFromFuture<'a, B: IoBuffReadWrite> {
    /// Socket descriptor used for the explicit-peer `recvmsg` receive path.
    /// The socket may also be connected; this API still asks the kernel for
    /// the source address.
    fd: RawFd,
    /// Completion state for the submitted recvmsg operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Maximum datagram bytes requested from the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadWrite> Future for RecvFromFuture<'_, B> {
    type Output = (io::Result<(usize, SocketAddr)>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedRecvFromPayload<B>>(this.state_ptr)
                };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let mut buffer = payload.buffer;
                if result < 0 {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }

                let actual = result as usize;

                let msg = unsafe { payload.msghdr.assume_init_ref() };
                // `msg_control` is null, so MSG_CTRUNC is not expected here;
                // keep the check defensive in case a kernel reports
                // inconsistent recvmsg flags.
                unsafe { buffer.set_written_len(actual) };
                if (msg.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC)) != 0 {
                    return Poll::Ready((
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "UDP recv_from message was truncated",
                        )),
                        buffer,
                    ));
                }
                let addr = match unsafe {
                    socket_addr_from_c(payload.addr.assume_init_ref(), msg.msg_namelen)
                } {
                    Ok(addr) => addr,
                    Err(err) => return Poll::Ready((Err(err), buffer)),
                };
                return Poll::Ready((Ok((actual, addr)), buffer));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedRecvFromPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                addr: zeroed_sockaddr_storage(),
                addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
                iovec: MaybeUninit::uninit(),
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let buffer_ptr = payload.buffer.as_mut_ptr();
                        payload.iovec.write(libc::iovec {
                            iov_base: buffer_ptr as *mut libc::c_void,
                            iov_len: this.len as usize,
                        });
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: payload.addr.as_mut_ptr() as *mut libc::c_void,
                                namelen: payload.addrlen,
                                iov: payload.iovec.as_mut_ptr(),
                                iovlen: 1,
                                control: std::ptr::null_mut(),
                                controllen: 0,
                            },
                        );

                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFromFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendToFuture (unconnected send via IORING_OP_SENDMSG)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendToFuture<'a, B: IoBuffReadOnly> {
    /// Socket descriptor used for the explicit-destination `sendmsg` path.
    /// The socket may also be connected; this API still sends to the provided
    /// destination address.
    fd: RawFd,
    /// Completion state for the submitted sendmsg operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned send buffer returned on completion.
    buffer: Option<B>,
    /// Prepared destination address for this datagram.
    addr: libc::sockaddr_storage,
    /// Length of the prepared destination address.
    addrlen: libc::socklen_t,
    /// Borrows the parent socket for the future lifetime.
    _marker: PhantomData<&'a mut UdpSocket>,
}

impl<B: IoBuffReadOnly> Future for SendToFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if !this.state_ptr.is_null() {
            let state = unsafe { &*this.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                let payload = unsafe {
                    (*pctx.reactor())
                        .take_retained_payload::<RetainedSendToPayload<B>>(this.state_ptr)
                };
                unsafe { (*pctx.reactor()).free_op(this.state_ptr) };
                this.state_ptr = std::ptr::null_mut();

                let buffer = payload.buffer;
                if result < 0 {
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }
                return Poll::Ready((Ok(result as usize), buffer));
            }
        }

        if this.state_ptr.is_null() {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::WouldBlock)), buffer));
            }
            this.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let addr = this.addr;
            let payload = RetainedSendToPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                addr,
                addrlen: this.addrlen,
                iovec: MaybeUninit::uninit(),
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let buffer_ptr = payload.buffer.as_ptr();
                        let len = payload.buffer.len();

                        payload.iovec.write(libc::iovec {
                            iov_base: buffer_ptr as *mut libc::c_void,
                            iov_len: len,
                        });
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: &mut payload.addr as *mut libc::sockaddr_storage
                                    as *mut libc::c_void,
                                namelen: payload.addrlen,
                                iov: payload.iovec.as_mut_ptr(),
                                iovlen: 1,
                                control: std::ptr::null_mut(),
                                controllen: 0,
                            },
                        );

                        Ok(
                            opcode::SendMsg::new(types::Fd(this.fd), payload.msghdr.as_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.state_ptr = std::ptr::null_mut();
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendToFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recv_from_addr_storage_starts_zeroed() {
        let addr = unsafe { zeroed_sockaddr_storage().assume_init() };
        assert_eq!(addr.ss_family, 0);
    }
}
