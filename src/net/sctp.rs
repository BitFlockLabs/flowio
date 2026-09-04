//! One-to-one SCTP transport with message-oriented send and receive operations.
//!
//! # Compatibility
//!
//! This implementation targets the x86-64 Linux SCTP socket API and FlowIO's
//! crate-wide x86-64 Linux 5.11-or-newer runtime floor.
//!
//! Baseline one-to-one SCTP operations are expected to work on supported
//! x86-64 Linux kernels where SCTP is enabled:
//! - [`SctpListener::bind`]
//! - [`SctpListener::accept`]
//! - [`SctpConnector::connect`]
//! - [`SctpStream::send_msg`]
//! - [`SctpStream::recv_msg`]
//!
//! FlowIO uses the 14-byte `SCTP_EVENTS` subscription layout available since
//! Linux 5.5. That predates the binding x86-64 Linux 5.11 runtime floor, so no
//! legacy 13-byte subscription fallback is attempted.
//!
//! More advanced SCTP controls and introspection depend on kernel support and
//! runtime policy for the specific socket option involved. These methods may
//! return errors such as `ENOPROTOOPT`, `EOPNOTSUPP`, `EINVAL`, `EPERM`, or
//! `EACCES` even when baseline SCTP messaging works:
//! - [`SctpStream::local_addrs`]
//! - [`SctpStream::peer_addrs`]
//! - [`SctpStream::peer_addr_params`]
//! - [`SctpStream::set_peer_addr_params`]
//! - [`SctpStream::set_primary_dest_addr`]
//! - [`SctpStream::request_peer_use_local_addr`]
//! - [`SctpStream::status`]
//! - [`SctpStream::peer_addr_info`]
//! - [`SctpStream::primary_path_info`]
//! - [`SctpStream::reconfig_supported`]
//! - [`SctpStream::enable_stream_reset`]
//! - [`SctpStream::reset_streams`]
//! - [`SctpStream::add_streams`]
//!
//! # Fast-Path Guidance
//!
//! Preferred on the per-message data fast path:
//! - If the socket is configured for data-only traffic without notifications
//!   or `SCTP_RCVINFO`, prefer [`SctpStream::send`] / [`SctpStream::recv`] on
//!   the hot path when the application guarantees receive sizing. The lean
//!   receive does not expose EOR or truncation metadata.
//! - If most sends use the same stream/PPID/flags, install
//!   [`SctpSendInfo`] once with [`SctpStream::set_default_send_info`] and keep
//!   using [`SctpStream::send`].
//! - Use vectored SCTP APIs only when payloads are already segmented. For a
//!   single contiguous message, the contiguous APIs avoid iovec scratch.
//!
//! Avoid on the per-message data fast path:
//! - Avoid the default rich socket configuration when the intended workload
//!   is data-only. Use [`SctpSocketConfig::data`] instead.
//! - Avoid [`SctpStream::send_msg`] / [`SctpStream::recv_msg`]
//!   when metadata and notifications are not needed. Use
//!   [`SctpStream::send`] / [`SctpStream::recv`] instead.
//! - Do not use the lean [`SctpStream::recv`] when record boundaries,
//!   truncation detection, or notifications are required. Use
//!   [`SctpStream::recv_msg`] or its vectored form.
//! - After dropping a rich receive, continue with a rich receive to retire its
//!   stream-owned recovery slot. Lean [`SctpStream::recv`] returns
//!   [`io::ErrorKind::InvalidInput`] without submission until that happens.
//!
//! On a repeated association path, reuse [`SctpConnector`] to preserve its
//! slot wrapper. Every attempt still creates and configures a fresh SCTP
//! socket; association establishment is not the per-message fast path. For a
//! data-only workload, construct it with [`SctpConnector::with_config`] and
//! [`SctpSocketConfig::data`].
//!
//! The examples below show message-oriented APIs because they are the most
//! explicit in documentation. For data-only hot paths, prefer
//! [`SctpStream::send`] / [`SctpStream::recv`] when their constraints fit.
//!
//! Data-only SCTP fast path:
//! ```no_run
//! use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpListener, SctpSocketConfig};
//! use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! fn pool() -> IoBuffPool {
//!     let mut pool = IoBuffPool::new(IoBuffPoolConfig {
//!         headroom: 0,
//!         payload: 256,
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
//!     let init = SctpInitConfig::diameter_default();
//!     let config = SctpSocketConfig::data(init);
//!     let mut listener =
//!         SctpListener::bind_with_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, config)
//!             .unwrap();
//!     let addr = listener.local_addr();
//!     let mut connector = SctpConnector::with_config(config);
//!
//!     Executor::spawn(async move {
//!         let (mut stream, _remote) = listener.accept().await.unwrap();
//!         let (res, buf) = stream.recv(server_pool.alloc().unwrap(), 5).await;
//!         let len = res.unwrap();
//!         let _received = &buf.payload_bytes()[..len];
//!     }).unwrap();
//!
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let mut send = client_pool.alloc().unwrap();
//!     send.payload_append(b"hello").unwrap();
//!     let (res, _buf) = stream.send(send).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Example
//! ```no_run
//! use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpListener};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let init = SctpInitConfig::diameter_default();
//!     let mut listener =
//!         SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init).unwrap();
//!     let addr = listener.local_addr();
//!     let mut connector = SctpConnector::new(init);
//!
//!     Executor::spawn(async move {
//!         let (mut stream, _remote) = listener.accept().await.unwrap();
//!         let (res, _buf) = stream.recv_msg(vec![0u8; 256], 256).await;
//!         res.unwrap();
//!     }).unwrap();
//!
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let (res, _buf) = stream.send_msg(b"hello".to_vec(), Default::default()).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Timed connects reuse the same connector slot plus the runtime timer wheel:
//! ```no_run
//! use flowio::net::sctp::{SctpConnector, SctpInitConfig};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//! use std::time::Duration;
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let init = SctpInitConfig::diameter_default();
//!     let mut connector = SctpConnector::new(init);
//!     let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 3868));
//!     for _ in 0..2 {
//!         let _ = connector
//!             .connect_timeout(addr, Duration::from_secs(1))
//!             .unwrap()
//!             .await;
//!     }
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Vectored SCTP messaging works with [`IoBuffVecMut`] / [`IoBuffVec`]:
//! ```no_run
//! use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpListener, SctpSendInfo};
//! use flowio::runtime::buffer::IoBuffMut;
//! use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
//! use flowio::runtime::executor::Executor;
//! use std::net::{Ipv4Addr, SocketAddr};
//!
//! let mut executor = Executor::new()?;
//! executor.run(async {
//!     let init = SctpInitConfig::diameter_default();
//!     let mut listener =
//!         SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init).unwrap();
//!     let addr = listener.local_addr();
//!     let mut connector = SctpConnector::new(init);
//!
//!     Executor::spawn(async move {
//!         let (mut stream, _remote) = listener.accept().await.unwrap();
//!         let recv = IoBuffVecMut::<2>::from_array([
//!             IoBuffMut::new(0, 5, 0).unwrap(),
//!             IoBuffMut::new(0, 6, 0).unwrap(),
//!         ]);
//!         let (res, _chain) = stream.recv_msg_vectored(recv).await;
//!         let (_len, _meta) = res.unwrap();
//!     }).unwrap();
//!
//!     let mut stream = connector.connect(addr).unwrap().await.unwrap();
//!     let mut seg1 = IoBuffMut::new(0, 16, 0).unwrap();
//!     seg1.payload_append(b"hello").unwrap();
//!     let mut seg2 = IoBuffMut::new(0, 16, 0).unwrap();
//!     seg2.payload_append(b" world").unwrap();
//!     let chain: IoBuffVec<2> = [seg1.freeze(), seg2.freeze()].into();
//!     let (res, _chain) = stream.send_msg_vectored(chain, SctpSendInfo::default()).await;
//!     res.unwrap();
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```

use super::stream::{
    checked_iobuffvec_write_iovec_count_and_len, fill_iobuffvec_write_iovecs,
    invalid_readv_aggregate, invalid_writev_shape,
};
use super::{
    AcceptReadinessSlot as AcceptSlot, CompletionTake, ConnectSubmissionSlot, MsgHdrInit,
    RetainedConnectAddr, checked_read_len, checked_send_len, close_fd, complete_read_with_progress,
    completion_cqe_result, current_local_addr, get_sock_opt, invalid_input, invalid_input_kind,
    map_connect_timeout, set_reuse_addr, set_sock_opt, socket_addr_from_c, socket_addr_to_c,
    socket_domain, write_msghdr,
};
use crate::net::send_sqe::{build_send_entry, build_sendmsg_entry};
use crate::runtime::buffer::bytes::{
    BufferRangeError, read_i32_at, read_u16_at, read_u16_be_at, read_u32_at,
};
use crate::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut, invalid_read_iovec_shape};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, UnsubmittedOpGuard, completed_op_ctx, drop_fd_op_state_unchecked,
    drop_op_ptr_unchecked, poll_ctx_from_waker, prepare_unsubmitted_op,
    refresh_op_waiter_from_waker, submit_initialized_retained_fd_sqe, submit_retained_fd_sqe,
    validate_local_io_result,
};
use crate::runtime::fd::{LingerProvenance, RuntimeFd, RuntimeFdOpState};
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use crate::runtime::retained::{
    RETAINED_IOVEC_MAX_COUNT, RetainedPayload, RetainedPayloadPool, with_raw_retained_slot,
};
use crate::runtime::task::release_task;
use crate::runtime::timer::{Timeout, timeout};
use crate::utils::disarm_unwind_guard;
use io_uring::{opcode, squeue, types};
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::{MaybeUninit, size_of};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::ptr::NonNull;
#[cfg(test)]
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

/// Kernel-facing notification-subscription layout used with SCTP socket
/// options on Linux.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SctpEventSubscribe {
    /// Enables the legacy `SCTP_SNDRCV` (`sctp_sndrcvinfo`) data-I/O
    /// ancillary. Modern `SCTP_RCVINFO` is enabled separately with the
    /// `SCTP_RECVRCVINFO` socket option.
    sctp_data_io_event: u8,
    /// Enables association state-change notifications.
    sctp_association_event: u8,
    /// Enables peer-address/path state-change notifications.
    sctp_address_event: u8,
    /// Enables legacy `SCTP_SEND_FAILED` notifications.
    sctp_send_failure_event: u8,
    /// Enables remote peer protocol-error notifications.
    sctp_peer_error_event: u8,
    /// Enables association shutdown notifications.
    sctp_shutdown_event: u8,
    /// Enables partial-delivery notifications.
    sctp_partial_delivery_event: u8,
    /// Enables adaptation-layer notifications.
    sctp_adaptation_layer_event: u8,
    /// Enables authentication notifications.
    sctp_authentication_event: u8,
    /// Enables sender-dry notifications.
    sctp_sender_dry_event: u8,
    /// Enables stream-reset notifications.
    sctp_stream_reset_event: u8,
    /// Enables association-reset notifications.
    sctp_assoc_reset_event: u8,
    /// Enables stream-count-change notifications.
    sctp_stream_change_event: u8,
    /// Enables newer `SCTP_SEND_FAILURE_EVENT` notifications.
    sctp_send_failure_event_event: u8,
}

impl SctpEventSubscribe {
    const fn from_mask(mask: SctpNotificationMask) -> Self {
        Self {
            sctp_data_io_event: 0,
            sctp_association_event: mask.association as u8,
            sctp_address_event: mask.address as u8,
            sctp_send_failure_event: 0,
            sctp_peer_error_event: mask.peer_error as u8,
            sctp_shutdown_event: mask.shutdown as u8,
            sctp_partial_delivery_event: mask.partial_delivery as u8,
            sctp_adaptation_layer_event: mask.adaptation as u8,
            sctp_authentication_event: mask.authentication as u8,
            sctp_sender_dry_event: mask.sender_dry as u8,
            sctp_stream_reset_event: mask.stream_reset as u8,
            sctp_assoc_reset_event: mask.assoc_reset as u8,
            sctp_stream_change_event: mask.stream_change as u8,
            sctp_send_failure_event_event: mask.send_failure as u8,
        }
    }

    #[cfg(feature = "test-support")]
    const fn notification_mask(self) -> SctpNotificationMask {
        SctpNotificationMask {
            association: self.sctp_association_event != 0,
            address: self.sctp_address_event != 0,
            send_failure: self.sctp_send_failure_event != 0
                || self.sctp_send_failure_event_event != 0,
            peer_error: self.sctp_peer_error_event != 0,
            shutdown: self.sctp_shutdown_event != 0,
            partial_delivery: self.sctp_partial_delivery_event != 0,
            adaptation: self.sctp_adaptation_layer_event != 0,
            authentication: self.sctp_authentication_event != 0,
            sender_dry: self.sctp_sender_dry_event != 0,
            stream_reset: self.sctp_stream_reset_event != 0,
            assoc_reset: self.sctp_assoc_reset_event != 0,
            stream_change: self.sctp_stream_change_event != 0,
        }
    }
}

// Linux exposes association address enumeration through internal SCTP socket options.
const SCTP_GET_PEER_ADDRS_OPT: libc::c_int = 108;
const SCTP_GET_LOCAL_ADDRS_OPT: libc::c_int = 109;
const SCTP_RECONFIG_SUPPORTED_OPT: libc::c_int = 117;
const SCTP_ENABLE_STREAM_RESET_OPT: libc::c_int = 118;
const SCTP_RESET_STREAMS_OPT: libc::c_int = 119;
const SCTP_ADD_STREAMS_OPT: libc::c_int = 121;
const SPP_HB_ENABLE: u32 = 1 << 0;
const SPP_HB_DISABLE: u32 = 1 << 1;
const SPP_HB_DEMAND: u32 = 1 << 2;
const SPP_PMTUD_ENABLE: u32 = 1 << 3;
const SPP_PMTUD_DISABLE: u32 = 1 << 4;
const SPP_SACKDELAY_ENABLE: u32 = 1 << 5;
const SPP_SACKDELAY_DISABLE: u32 = 1 << 6;
const SPP_HB_TIME_IS_ZERO: u32 = 1 << 7;
const SPP_IPV6_FLOWLABEL: u32 = 1 << 8;
const SPP_DSCP: u32 = 1 << 9;
const SCTP_ENABLE_RESET_STREAM_REQ: u32 = 0x01;
const SCTP_ENABLE_RESET_ASSOC_REQ: u32 = 0x02;
const SCTP_ENABLE_CHANGE_ASSOC_REQ: u32 = 0x04;
const SCTP_STREAM_RESET_INCOMING: u16 = 0x01;
const SCTP_STREAM_RESET_OUTGOING: u16 = 0x02;

/// Per-message SCTP send metadata.
///
/// Passed to [`SctpStream::send_msg`] and
/// [`SctpStream::send_msg_vectored`], and installable as the socket default
/// with [`SctpStream::set_default_send_info`] for use by
/// [`SctpStream::send`].
///
/// This is metadata for explicit message sends. If the same metadata applies
/// to most messages, install it once with [`SctpStream::set_default_send_info`]
/// and use [`SctpStream::send`] on the data fast path.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpSendInfo;
///
/// let info = SctpSendInfo {
///     stream_id: 1,
///     ppid: 46,
///     ..SctpSendInfo::default()
/// };
/// assert_eq!(info.stream_id, 1);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpSendInfo {
    /// SCTP stream number to send on.
    pub stream_id: u16,
    /// Send flags (e.g. `SCTP_UNORDERED`).
    pub flags: u16,
    /// Payload Protocol Identifier in host byte order; converted to network
    /// order on the wire.
    pub ppid: u32,
    /// Opaque context value returned in send-failed notifications.
    pub context: u32,
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
}

/// Per-message SCTP receive metadata reported by a message receive.
///
/// This belongs to the metadata receive path returned by
/// [`SctpStream::recv_msg`]. When the stream's stored receive-info policy is
/// disabled, absent ancillary fields remain at their defaults while
/// [`SctpRecvInfo::end_of_record`] still reflects the receive flags. Ordinary
/// data that omits receive info while the stored policy is enabled is rejected
/// as [`io::ErrorKind::InvalidData`]. It is not produced by the lean data fast
/// path; use [`SctpStream::recv`] when the caller only needs bytes.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpRecvInfo;
///
/// let info = SctpRecvInfo {
///     stream_id: 2,
///     ppid: 46,
///     ..SctpRecvInfo::default()
/// };
/// assert_eq!(info.ppid, 46);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpRecvInfo {
    /// SCTP stream number the message was received on.
    pub stream_id: u16,
    /// Stream Sequence Number.
    pub ssn: u16,
    /// Receive flags.
    pub flags: u16,
    /// Payload Protocol Identifier in host byte order; converted from network
    /// order by FlowIO.
    pub ppid: u32,
    /// Transmission Sequence Number.
    pub tsn: u32,
    /// Cumulative TSN.
    pub cumtsn: u32,
    /// Opaque context value.
    pub context: u32,
    /// Association ID.
    pub assoc_id: libc::sctp_assoc_t,
    /// Whether the kernel reported this receive as the end of an SCTP record.
    ///
    /// FlowIO rejects non-empty partial records from `recv_msg` APIs with
    /// `InvalidData`; successful data receives normally carry `true`.
    pub end_of_record: bool,
}

/// Per-peer-address SCTP parameters used by `SCTP_PEER_ADDR_PARAMS`.
///
/// This is association/path configuration, not the data fast path. Set it
/// during setup or reconfiguration and keep steady-state payload traffic on
/// [`SctpStream::send`] / [`SctpStream::recv`] when metadata is unnecessary.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::SctpPeerAddrParams;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let params = SctpPeerAddrParams {
///     flags: SctpPeerAddrParams::HEARTBEAT_ENABLE,
///     heartbeat_interval_ms: 30_000,
///     ..SctpPeerAddrParams::for_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)))
/// };
/// # let _ = params;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpPeerAddrParams {
    /// Association to target, or `0` for the default association on one-to-one
    /// sockets.
    pub assoc_id: libc::sctp_assoc_t,
    /// Specific peer transport address to target. `None` applies the settings
    /// association-wide.
    pub address: Option<SocketAddr>,
    /// Heartbeat interval for the selected path, in milliseconds.
    pub heartbeat_interval_ms: u32,
    /// Maximum retransmissions on the selected path before it is considered
    /// failed.
    pub path_max_retransmits: u16,
    /// Path MTU to advertise or enforce for the selected path.
    pub path_mtu: u32,
    /// Delayed-SACK interval in milliseconds.
    pub sack_delay_ms: u32,
    /// Bitmask of `SPP_*` behavior flags such as heartbeat/PMTU/SACK control.
    pub flags: u32,
    /// IPv6 flow label to apply when [`SctpPeerAddrParams::IPV6_FLOWLABEL`] is set.
    pub ipv6_flow_label: u32,
    /// DSCP value to apply when [`SctpPeerAddrParams::DSCP`] is set.
    pub dscp: u8,
}

/// Association-wide retransmission and RTO policy.
///
/// This is setup/control-plane configuration. It does not participate in the
/// per-message data fast path once the association is established.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpAssocConfig;
///
/// let config = SctpAssocConfig {
///     assoc_max_retrans: Some(5),
///     rto_initial_ms: Some(1_000),
///     rto_min_ms: Some(500),
///     rto_max_ms: Some(4_000),
/// };
/// assert_eq!(config.assoc_max_retrans, Some(5));
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpAssocConfig {
    /// Maximum association-level retransmissions before the association is
    /// considered failed.
    pub assoc_max_retrans: Option<u16>,
    /// Initial retransmission timeout used by the association.
    pub rto_initial_ms: Option<u32>,
    /// Minimum retransmission timeout used by the association.
    pub rto_min_ms: Option<u32>,
    /// Maximum retransmission timeout used by the association.
    pub rto_max_ms: Option<u32>,
}

/// Association-wide SCTP reconfiguration capabilities and enable flags.
///
/// These flags are used with [`SctpStream::reconfig_supported`] and
/// [`SctpStream::enable_stream_reset`].
///
/// This is reconfiguration/control-plane state, not the payload data fast
/// path.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpReconfigFlags;
///
/// let flags = SctpReconfigFlags {
///     flags: SctpReconfigFlags::RESET_STREAMS,
///     ..SctpReconfigFlags::association_default()
/// };
/// assert_eq!(flags.flags, SctpReconfigFlags::RESET_STREAMS);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpReconfigFlags {
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Bitmask of `RESET_STREAMS`, `RESET_ASSOC`, `CHANGE_ASSOC`.
    pub flags: u32,
}

impl SctpReconfigFlags {
    /// Enables stream reset requests on the association.
    pub const RESET_STREAMS: u32 = SCTP_ENABLE_RESET_STREAM_REQ;
    /// Enables association reset requests on the association.
    pub const RESET_ASSOC: u32 = SCTP_ENABLE_RESET_ASSOC_REQ;
    /// Enables association stream-count changes on the association.
    pub const CHANGE_ASSOC: u32 = SCTP_ENABLE_CHANGE_ASSOC_REQ;

    /// Creates an empty association-wide flag block.
    pub const fn association_default() -> Self {
        Self {
            assoc_id: 0,
            flags: 0,
        }
    }
}

/// Request parameters for `SCTP_RESET_STREAMS`.
///
/// This is a reconfiguration request type. Use it for explicit stream-reset
/// control operations, not for steady-state payload exchange. The
/// [`SctpResetStreams::incoming`], [`SctpResetStreams::outgoing`], and
/// [`SctpResetStreams::bidirectional`] constructors require at least one
/// stream identifier when the request is submitted. Use the corresponding
/// `all_*` constructor to request Linux's association-wide zero-count form
/// deliberately. Mutating `streams` so that it conflicts with the constructor
/// intent makes the request invalid.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{SctpResetStreams, SctpReconfigFlags};
///
/// let request = SctpResetStreams::outgoing(&[1, 3]);
/// let all = SctpResetStreams::all_bidirectional();
/// assert!(all.streams.is_empty());
/// let flags = SctpReconfigFlags {
///     flags: SctpReconfigFlags::RESET_STREAMS,
///     ..SctpReconfigFlags::association_default()
/// };
/// # let _ = (request, all, flags);
/// ```
///
/// The private intent tag prevents a downstream struct literal from turning an
/// accidental empty list into the kernel's all-stream sentinel:
///
/// ```compile_fail
/// use flowio::net::sctp::SctpResetStreams;
///
/// let _ = SctpResetStreams {
///     assoc_id: 0,
///     flags: 1,
///     streams: Vec::new(),
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SctpResetStreams {
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Direction flags (incoming, outgoing, or both).
    pub flags: u16,
    /// Stream numbers to reset. This remains empty for an explicit all-stream
    /// request.
    pub streams: Vec<u16>,
    /// Distinguishes an explicit all-stream request from a listed request.
    intent: SctpResetIntent,
}

impl SctpResetStreams {
    /// Resets the specified incoming streams.
    pub fn incoming(streams: &[u16]) -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING,
            streams: streams.to_vec(),
            intent: SctpResetIntent::Listed,
        }
    }

    /// Resets all incoming streams on the association.
    ///
    /// This deliberately selects Linux's zero-count all-stream sentinel.
    pub fn all_incoming() -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING,
            streams: Vec::new(),
            intent: SctpResetIntent::All,
        }
    }

    /// Resets the specified outgoing streams.
    pub fn outgoing(streams: &[u16]) -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_OUTGOING,
            streams: streams.to_vec(),
            intent: SctpResetIntent::Listed,
        }
    }

    /// Resets all outgoing streams on the association.
    ///
    /// This deliberately selects Linux's zero-count all-stream sentinel.
    pub fn all_outgoing() -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_OUTGOING,
            streams: Vec::new(),
            intent: SctpResetIntent::All,
        }
    }

    /// Resets the specified incoming and outgoing streams.
    pub fn bidirectional(streams: &[u16]) -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING | SCTP_STREAM_RESET_OUTGOING,
            streams: streams.to_vec(),
            intent: SctpResetIntent::Listed,
        }
    }

    /// Resets all incoming and outgoing streams on the association.
    ///
    /// This deliberately selects Linux's zero-count all-stream sentinel.
    pub fn all_bidirectional() -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING | SCTP_STREAM_RESET_OUTGOING,
            streams: Vec::new(),
            intent: SctpResetIntent::All,
        }
    }
}

/// Caller intent for the otherwise ambiguous zero-count kernel encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SctpResetIntent {
    /// Reset only the identifiers carried in `streams`.
    Listed,
    /// Deliberately use the kernel's zero-count all-stream sentinel.
    All,
}

/// Request parameters for `SCTP_ADD_STREAMS`.
///
/// This is a reconfiguration request type. Use it when expanding association
/// stream counts, not in the data fast path.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpAddStreams;
///
/// let request = SctpAddStreams::new(1, 2);
/// assert_eq!(request.outbound_streams, 2);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpAddStreams {
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Number of inbound streams to add.
    pub inbound_streams: u16,
    /// Number of outbound streams to add.
    pub outbound_streams: u16,
}

impl SctpAddStreams {
    /// Requests additional inbound and outbound streams for the association.
    pub const fn new(inbound_streams: u16, outbound_streams: u16) -> Self {
        Self {
            assoc_id: 0,
            inbound_streams,
            outbound_streams,
        }
    }
}

impl SctpPeerAddrParams {
    /// Enables heartbeats for the selected address or association.
    pub const HEARTBEAT_ENABLE: u32 = SPP_HB_ENABLE;
    /// Disables heartbeats for the selected address or association.
    pub const HEARTBEAT_DISABLE: u32 = SPP_HB_DISABLE;
    /// Forces an immediate heartbeat on the selected address.
    pub const HEARTBEAT_DEMAND: u32 = SPP_HB_DEMAND;
    /// Enables path MTU discovery.
    pub const PMTUD_ENABLE: u32 = SPP_PMTUD_ENABLE;
    /// Disables path MTU discovery.
    pub const PMTUD_DISABLE: u32 = SPP_PMTUD_DISABLE;
    /// Enables delayed SACK handling.
    pub const SACKDELAY_ENABLE: u32 = SPP_SACKDELAY_ENABLE;
    /// Disables delayed SACK handling.
    pub const SACKDELAY_DISABLE: u32 = SPP_SACKDELAY_DISABLE;
    /// Requests a zero heartbeat interval.
    pub const HEARTBEAT_TIME_IS_ZERO: u32 = SPP_HB_TIME_IS_ZERO;
    /// Applies the IPv6 flow label value.
    pub const IPV6_FLOWLABEL: u32 = SPP_IPV6_FLOWLABEL;
    /// Applies the DSCP value.
    pub const DSCP: u32 = SPP_DSCP;

    /// Returns an empty association-wide parameter block.
    pub const fn association_default() -> Self {
        Self {
            assoc_id: 0,
            address: None,
            heartbeat_interval_ms: 0,
            path_max_retransmits: 0,
            path_mtu: 0,
            sack_delay_ms: 0,
            flags: 0,
            ipv6_flow_label: 0,
            dscp: 0,
        }
    }

    /// Returns a parameter block targeting a specific transport address.
    pub const fn for_address(address: SocketAddr) -> Self {
        Self {
            address: Some(address),
            ..Self::association_default()
        }
    }
}

/// Read-only per-path SCTP state returned by `SCTP_GET_PEER_ADDR_INFO`.
///
/// This is status/control-plane metadata obtained with `getsockopt`, not a
/// per-message data fast-path type.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpPeerAddrInfo;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let info = SctpPeerAddrInfo {
///     assoc_id: 0,
///     address: SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
///     state: SctpPeerAddrInfo::ACTIVE,
///     congestion_window: 65_535,
///     srtt: 10,
///     rto: 100,
///     mtu: 1500,
/// };
/// assert_eq!(info.state, SctpPeerAddrInfo::ACTIVE);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpPeerAddrInfo {
    /// Association ID.
    pub assoc_id: libc::sctp_assoc_t,
    /// Peer transport address.
    pub address: SocketAddr,
    /// Path state (see `INACTIVE`, `ACTIVE`, etc.).
    pub state: i32,
    /// Current congestion window in bytes.
    pub congestion_window: u32,
    /// Smoothed Round-Trip Time in milliseconds.
    pub srtt: u32,
    /// Current Retransmission Timeout in milliseconds.
    pub rto: u32,
    /// Path Maximum Transmission Unit in bytes.
    pub mtu: u32,
}

impl SctpPeerAddrInfo {
    /// Path is inactive and not currently usable.
    pub const INACTIVE: i32 = 0;
    /// Path is still considered usable but is close to failure.
    pub const POTENTIALLY_FAILED: i32 = 1;
    /// Path is active and usable.
    pub const ACTIVE: i32 = 2;
    /// Path has not been fully confirmed yet.
    pub const UNCONFIRMED: i32 = 3;
    /// Kernel reported an unknown path state.
    pub const UNKNOWN: i32 = 0xffff;
}

/// Read-only association status returned by `SCTP_STATUS`.
///
/// This is status/control-plane metadata obtained with `getsockopt`, not a
/// per-message data fast-path type.
///
/// # Example
/// ```
/// use flowio::net::sctp::{SctpAssocStatus, SctpPeerAddrInfo};
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let primary_path = SctpPeerAddrInfo {
///     assoc_id: 0,
///     address: SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
///     state: SctpPeerAddrInfo::ACTIVE,
///     congestion_window: 65_535,
///     srtt: 10,
///     rto: 100,
///     mtu: 1500,
/// };
/// let status = SctpAssocStatus {
///     assoc_id: 0,
///     state: SctpAssocStatus::ESTABLISHED,
///     receiver_window: 262_144,
///     unacked_data_chunks: 0,
///     pending_data_chunks: 0,
///     inbound_streams: 16,
///     outbound_streams: 16,
///     fragmentation_point: 1200,
///     primary_path,
/// };
/// assert_eq!(status.state, SctpAssocStatus::ESTABLISHED);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpAssocStatus {
    /// Association ID.
    pub assoc_id: libc::sctp_assoc_t,
    /// Association state (see `EMPTY`, `ESTABLISHED`, etc.).
    pub state: i32,
    /// Peer's advertised receiver window in bytes.
    pub receiver_window: u32,
    /// Number of unacknowledged data chunks.
    pub unacked_data_chunks: u16,
    /// Number of data chunks pending transmission.
    pub pending_data_chunks: u16,
    /// Number of negotiated inbound streams.
    pub inbound_streams: u16,
    /// Number of negotiated outbound streams.
    pub outbound_streams: u16,
    /// Smallest size at which data will be fragmented.
    pub fragmentation_point: u32,
    /// Primary path information.
    pub primary_path: SctpPeerAddrInfo,
}

impl SctpAssocStatus {
    /// No association is currently attached.
    pub const EMPTY: i32 = 0;
    /// Association is closed.
    pub const CLOSED: i32 = 1;
    /// Association is waiting for cookie setup to complete.
    pub const COOKIE_WAIT: i32 = 2;
    /// Cookie echo was sent and association setup is in progress.
    pub const COOKIE_ECHOED: i32 = 3;
    /// Association is established and able to exchange user data.
    pub const ESTABLISHED: i32 = 4;
    /// Shutdown was requested and is waiting for in-flight data to drain.
    pub const SHUTDOWN_PENDING: i32 = 5;
    /// Shutdown sequence started locally and the SHUTDOWN chunk was sent.
    pub const SHUTDOWN_SENT: i32 = 6;
    /// Shutdown sequence started remotely and was observed locally.
    pub const SHUTDOWN_RECEIVED: i32 = 7;
    /// Final shutdown acknowledgement was sent.
    pub const SHUTDOWN_ACK_SENT: i32 = 8;
}

/// Decoded SCTP notification payloads carried by
/// [`SctpRecvMeta::Notification`].
///
/// Notifications are part of the metadata/signaling receive path. Data-only
/// fast paths should configure the socket with [`SctpSocketConfig::data`] and
/// use [`SctpStream::recv`], which returns only a byte count.
///
/// # Example
/// ```
/// use flowio::net::sctp::{SctpNotification, SctpNotificationKind};
///
/// let notification = SctpNotification::Shutdown { assoc_id: 0 };
/// assert_eq!(notification.kind(), SctpNotificationKind::Shutdown);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SctpNotification {
    /// Association state change such as up/down/restart.
    AssocChange {
        /// Kernel-reported association state.
        state: u16,
        /// Error code associated with the state change, if any.
        error: u16,
        /// Negotiated outbound stream count.
        outbound_streams: u16,
        /// Negotiated inbound stream count.
        inbound_streams: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Peer completed an SCTP shutdown sequence.
    Shutdown {
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Per-path reachability or state change notification.
    PeerAddrChange {
        /// Peer transport address whose state changed.
        addr: SocketAddr,
        /// Kernel-reported path state.
        state: i32,
        /// Associated error code, if any.
        error: i32,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Remote peer reported a protocol error.
    RemoteError {
        /// SCTP error code from the peer.
        error: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// A send failed and the kernel returned the original send metadata.
    SendFailed {
        /// Raw kernel disposition flags, including `SCTP_DATA_UNSENT` and
        /// `SCTP_DATA_SENT`; unfamiliar values are preserved.
        flags: u16,
        /// Kernel error code for the failed send.
        error: u32,
        /// Original send metadata supplied with the failed message.
        info: SctpSendInfo,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Adaptation layer indication notification.
    Adaptation {
        /// Adaptation indication value from the peer/kernel.
        indication: u32,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Partial-delivery state change notification.
    PartialDelivery {
        /// Kernel-reported partial-delivery indication.
        indication: u32,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
        /// Stream associated with the partial-delivery state.
        stream: u32,
        /// Sequence value reported by the kernel for the partial-delivery state.
        sequence: u32,
    },
    /// Sender queue became empty for the association.
    SenderDry {
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Stream reset completion or state change notification.
    ///
    /// Linux may append a variable list of stream identifiers after the
    /// association ID. FlowIO bounds that tail by the declared notification
    /// length but intentionally does not materialize it, keeping this value
    /// fixed-size, allocation-free, and [`Copy`].
    StreamReset {
        /// Kernel flags for the reset event.
        flags: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Association reset notification with TSN restart points.
    AssocReset {
        /// Kernel flags for the reset event.
        flags: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
        /// Local TSN after the reset.
        local_tsn: u32,
        /// Remote TSN after the reset.
        remote_tsn: u32,
    },
    /// Stream-count change notification.
    StreamChange {
        /// Kernel flags for the stream-change event.
        flags: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
        /// New inbound stream count.
        inbound_streams: u16,
        /// New outbound stream count.
        outbound_streams: u16,
    },
    /// Notification kind not decoded by the crate yet.
    Other {
        /// Raw SCTP notification type.
        kind: u16,
        /// Raw notification flags.
        flags: u16,
        /// Raw notification length.
        length: u32,
    },
    /// Authentication key state changed for an association.
    Authentication {
        /// Kernel flags for the authentication event.
        flags: u16,
        /// Key number affected by the event.
        key_number: u16,
        /// Alternate key number reported by the kernel.
        alternate_key_number: u16,
        /// Raw Linux authentication indication; unfamiliar values are
        /// preserved for forward compatibility.
        indication: u32,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
}

/// Coarse notification category used by transport consumers.
///
/// This is metadata/signaling classification. It is useful after
/// [`SctpStream::recv_msg`] returns a notification, and is not part of the
/// data-only [`SctpStream::recv`] fast path.
///
/// # Example
/// ```
/// use flowio::net::sctp::{SctpNotification, SctpNotificationKind};
///
/// let notification = SctpNotification::SenderDry { assoc_id: 0 };
/// assert_eq!(notification.kind(), SctpNotificationKind::SenderDry);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SctpNotificationKind {
    /// [`SctpNotification::AssocChange`].
    AssocChange,
    /// [`SctpNotification::Shutdown`].
    Shutdown,
    /// [`SctpNotification::PeerAddrChange`].
    PeerAddrChange,
    /// [`SctpNotification::RemoteError`].
    RemoteError,
    /// [`SctpNotification::SendFailed`].
    SendFailed,
    /// [`SctpNotification::Adaptation`].
    Adaptation,
    /// [`SctpNotification::PartialDelivery`].
    PartialDelivery,
    /// [`SctpNotification::SenderDry`].
    SenderDry,
    /// [`SctpNotification::StreamReset`].
    StreamReset,
    /// [`SctpNotification::AssocReset`].
    AssocReset,
    /// [`SctpNotification::StreamChange`].
    StreamChange,
    /// [`SctpNotification::Other`].
    Other,
    /// [`SctpNotification::Authentication`].
    Authentication,
}

impl SctpNotification {
    /// Returns the notification kind without exposing payload details.
    pub const fn kind(&self) -> SctpNotificationKind {
        match self {
            Self::AssocChange { .. } => SctpNotificationKind::AssocChange,
            Self::Shutdown { .. } => SctpNotificationKind::Shutdown,
            Self::PeerAddrChange { .. } => SctpNotificationKind::PeerAddrChange,
            Self::RemoteError { .. } => SctpNotificationKind::RemoteError,
            Self::SendFailed { .. } => SctpNotificationKind::SendFailed,
            Self::Adaptation { .. } => SctpNotificationKind::Adaptation,
            Self::PartialDelivery { .. } => SctpNotificationKind::PartialDelivery,
            Self::SenderDry { .. } => SctpNotificationKind::SenderDry,
            Self::StreamReset { .. } => SctpNotificationKind::StreamReset,
            Self::AssocReset { .. } => SctpNotificationKind::AssocReset,
            Self::StreamChange { .. } => SctpNotificationKind::StreamChange,
            Self::Other { .. } => SctpNotificationKind::Other,
            Self::Authentication { .. } => SctpNotificationKind::Authentication,
        }
    }
}

/// Result metadata returned by [`SctpStream::recv_msg`].
///
/// This belongs to the metadata/signaling path. For data-only associations,
/// [`SctpStream::recv`] avoids this enum and returns the received byte count.
///
/// # Example
/// ```
/// use flowio::net::sctp::{SctpRecvInfo, SctpRecvMeta};
///
/// let meta = SctpRecvMeta::Data(SctpRecvInfo::default());
/// assert!(meta.is_data());
/// assert!(meta.data().is_some());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SctpRecvMeta {
    /// Regular user data was received. The contained fields come from
    /// `SCTP_RCVINFO`. When the stream's stored receive-info policy is
    /// disabled, absent ancillary fields are defaults and `end_of_record`
    /// still reflects the receive flags; policy-enabled but absent receive
    /// info is `InvalidData`.
    Data(#[doc = "Per-message receive information or default ancillary fields."] SctpRecvInfo),
    /// An SCTP notification was received instead of user data.
    Notification(#[doc = "Decoded kernel notification payload."] SctpNotification),
}

impl SctpRecvMeta {
    /// Returns `true` when the receive completed with user data.
    pub const fn is_data(&self) -> bool {
        matches!(self, Self::Data(_))
    }

    /// Returns `true` when the receive completed with an SCTP notification.
    pub const fn is_notification(&self) -> bool {
        matches!(self, Self::Notification(_))
    }

    /// Returns a shared reference to the receive data metadata, if present.
    pub const fn data(&self) -> Option<&SctpRecvInfo> {
        match self {
            Self::Data(info) => Some(info),
            Self::Notification(_) => None,
        }
    }

    /// Returns a shared reference to the SCTP notification, if present.
    pub const fn notification(&self) -> Option<&SctpNotification> {
        match self {
            Self::Data(_) => None,
            Self::Notification(notification) => Some(notification),
        }
    }

    /// Extracts the receive data metadata, if present.
    pub const fn into_data(self) -> Option<SctpRecvInfo> {
        match self {
            Self::Data(info) => Some(info),
            Self::Notification(_) => None,
        }
    }

    /// Extracts the SCTP notification, if present.
    pub const fn into_notification(self) -> Option<SctpNotification> {
        match self {
            Self::Data(_) => None,
            Self::Notification(notification) => Some(notification),
        }
    }
}

/// Typed SCTP notification subscription policy.
///
/// This is socket setup policy. For the lean data fast path, use
/// [`SctpNotificationMask::none`] through [`SctpSocketConfig::data`].
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpNotificationMask;
///
/// let mask = SctpNotificationMask::none();
/// assert!(!mask.shutdown);
///
/// let rich = SctpNotificationMask::signaling_default();
/// assert!(rich.shutdown);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpNotificationMask {
    /// Association state notifications.
    pub association: bool,
    /// Peer-address/path state notifications.
    pub address: bool,
    /// Send-failure notifications.
    pub send_failure: bool,
    /// Remote protocol-error notifications.
    pub peer_error: bool,
    /// Shutdown notifications.
    pub shutdown: bool,
    /// Caller-visible partial-delivery notifications.
    ///
    /// FlowIO may keep the kernel event subscribed when receive metadata is
    /// enabled even if this field is false; aborts identifiable as forced only
    /// for internal resynchronization are not returned to the caller.
    pub partial_delivery: bool,
    /// Adaptation-layer notifications.
    pub adaptation: bool,
    /// Authentication-related notifications.
    pub authentication: bool,
    /// Sender-dry notifications.
    pub sender_dry: bool,
    /// Stream-reset notifications.
    pub stream_reset: bool,
    /// Association-reset notifications.
    pub assoc_reset: bool,
    /// Stream-count-change notifications.
    pub stream_change: bool,
}

impl SctpNotificationMask {
    /// Returns a mask with every SCTP notification disabled.
    pub const fn none() -> Self {
        Self {
            association: false,
            address: false,
            send_failure: false,
            peer_error: false,
            shutdown: false,
            partial_delivery: false,
            adaptation: false,
            authentication: false,
            sender_dry: false,
            stream_reset: false,
            assoc_reset: false,
            stream_change: false,
        }
    }

    /// Returns a mask with every SCTP notification enabled.
    pub const fn all() -> Self {
        Self {
            association: true,
            address: true,
            send_failure: true,
            peer_error: true,
            shutdown: true,
            partial_delivery: true,
            adaptation: true,
            authentication: true,
            sender_dry: true,
            stream_reset: true,
            assoc_reset: true,
            stream_change: true,
        }
    }

    /// Notification set used by the signaling-oriented rich socket config.
    pub const fn signaling_default() -> Self {
        Self {
            association: true,
            address: true,
            send_failure: true,
            peer_error: true,
            shutdown: true,
            partial_delivery: true,
            adaptation: true,
            authentication: false,
            sender_dry: true,
            stream_reset: true,
            assoc_reset: true,
            stream_change: true,
        }
    }

    /// Returns true when the caller requested any notification for delivery.
    const fn any(self) -> bool {
        self.association
            || self.address
            || self.send_failure
            || self.peer_error
            || self.shutdown
            || self.partial_delivery
            || self.adaptation
            || self.authentication
            || self.sender_dry
            || self.stream_reset
            || self.assoc_reset
            || self.stream_change
    }
}

impl Default for SctpNotificationMask {
    fn default() -> Self {
        Self::signaling_default()
    }
}

/// Returns the effective kernel notification mask for the receive mode.
///
/// Metadata receives can enter record-tail discard after observing a partial
/// record. Linux reports an abandoned partial delivery through PDAPI, so that
/// event must remain subscribed whenever modern receive metadata is enabled.
#[inline(always)]
const fn effective_sctp_notification_mask(
    mut notifications: SctpNotificationMask,
    recv_rcvinfo: bool,
) -> SctpNotificationMask {
    if recv_rcvinfo {
        notifications.partial_delivery = true;
    }
    notifications
}

#[repr(C, packed(4))]
/// Linux `sctp_prim` layout used to select the local primary destination.
///
/// This intentionally remains distinct from the layout-identical
/// [`SctpSetPeerPrimRaw`]: the two socket options give the address field
/// opposite local/peer meanings.
struct SctpPrimRaw {
    /// Association selected by the socket option.
    assoc_id: libc::sctp_assoc_t,
    /// Peer transport address to make primary locally.
    addr: libc::sockaddr_storage,
}

#[repr(C, packed(4))]
/// Linux `sctp_setpeerprim` layout used to request the peer's primary path.
struct SctpSetPeerPrimRaw {
    /// Association selected by the socket option.
    assoc_id: libc::sctp_assoc_t,
    /// Local transport address the peer is asked to make primary.
    addr: libc::sockaddr_storage,
}

#[repr(C, packed)]
/// Modern Linux `sctp_paddrparams` socket-option layout.
struct SctpPaddrParamsRaw {
    /// Association selected by the parameter request.
    assoc_id: libc::sctp_assoc_t,
    /// Specific peer path, or an all-zero family for association-wide values.
    address: libc::sockaddr_storage,
    /// Heartbeat interval in milliseconds.
    heartbeat_interval_ms: u32,
    /// Maximum retransmissions before the selected path is considered failed.
    path_max_retransmits: u16,
    /// Configured path MTU in bytes.
    path_mtu: u32,
    /// Delayed-SACK interval in milliseconds.
    sack_delay_ms: u32,
    /// `SPP_*` option-selection and behavior bits.
    flags: u32,
    /// IPv6 flow label selected by `SPP_IPV6_FLOWLABEL`.
    ipv6_flow_label: u32,
    /// DSCP value selected by `SPP_DSCP`.
    dscp: u8,
}

const fn align_sockopt_len(len: usize) -> usize {
    (len + 3) & !3
}

const SCTP_PADDR_PARAMS_RAW_OPT_LEN: usize =
    align_sockopt_len(std::mem::size_of::<SctpPaddrParamsRaw>());

/// Naturally aligned mirror used while converting packed peer-path options.
struct SctpPaddrParamsFields {
    /// Association selected by the parameter request.
    assoc_id: libc::sctp_assoc_t,
    /// Specific peer path, or an all-zero family for association-wide values.
    address: libc::sockaddr_storage,
    /// Heartbeat interval in milliseconds.
    heartbeat_interval_ms: u32,
    /// Maximum retransmissions before the selected path is considered failed.
    path_max_retransmits: u16,
    /// Configured path MTU in bytes.
    path_mtu: u32,
    /// Delayed-SACK interval in milliseconds.
    sack_delay_ms: u32,
    /// `SPP_*` option-selection and behavior bits.
    flags: u32,
    /// IPv6 flow label selected by `SPP_IPV6_FLOWLABEL`.
    ipv6_flow_label: u32,
    /// DSCP value selected by `SPP_DSCP`.
    dscp: u8,
}

impl SctpPaddrParamsFields {
    fn from_public(params: SctpPeerAddrParams) -> Self {
        Self {
            assoc_id: params.assoc_id,
            address: option_socket_addr_to_storage(params.address),
            heartbeat_interval_ms: params.heartbeat_interval_ms,
            path_max_retransmits: params.path_max_retransmits,
            path_mtu: params.path_mtu,
            sack_delay_ms: params.sack_delay_ms,
            flags: params.flags,
            ipv6_flow_label: params.ipv6_flow_label,
            dscp: params.dscp,
        }
    }

    fn to_public(&self) -> io::Result<SctpPeerAddrParams> {
        Ok(SctpPeerAddrParams {
            assoc_id: self.assoc_id,
            address: storage_to_option_socket_addr(self.address)?,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            path_max_retransmits: self.path_max_retransmits,
            path_mtu: self.path_mtu,
            sack_delay_ms: self.sack_delay_ms,
            flags: self.flags,
            ipv6_flow_label: self.ipv6_flow_label,
            dscp: self.dscp,
        })
    }

    fn requires_modern_sockopt(&self) -> bool {
        self.ipv6_flow_label != 0
            || self.dscp != 0
            || (self.flags & (SPP_IPV6_FLOWLABEL | SPP_DSCP)) != 0
    }
}

#[repr(C)]
/// Linux association/value pair used by SCTP reconfiguration options.
struct SctpAssocValueRaw {
    /// Association selected by the socket option.
    assoc_id: libc::sctp_assoc_t,
    /// Option-specific reconfiguration capability or enable bitmask.
    assoc_value: u32,
}

#[repr(C, packed(4))]
/// Linux `sctp_assocparams` layout for association policy and status.
struct SctpAssocParamsRaw {
    /// Association selected by the socket option.
    assoc_id: libc::sctp_assoc_t,
    /// Maximum association-level retransmission attempts.
    assoc_max_retrans: u16,
    /// Number of peer transport destinations reported by the kernel.
    peer_destinations: u16,
    /// Peer-advertised receive window in bytes.
    peer_receiver_window: u32,
    /// Local receive window in bytes.
    local_receiver_window: u32,
    /// Association cookie lifetime in milliseconds.
    cookie_life_ms: u32,
}

fn get_sctp_opt_exact<T>(fd: RawFd, name: libc::c_int, mut value: T) -> io::Result<T> {
    let expected_len = size_of::<T>();
    let mut optlen = expected_len as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_SCTP,
            name,
            (&mut value as *mut T).cast(),
            &mut optlen,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    if optlen as usize != expected_len {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(value)
}

impl SctpAssocParamsRaw {
    fn get(fd: RawFd) -> io::Result<Self> {
        get_sctp_opt_exact(
            fd,
            libc::SCTP_ASSOCINFO,
            Self {
                assoc_id: 0,
                assoc_max_retrans: 0,
                peer_destinations: 0,
                peer_receiver_window: 0,
                local_receiver_window: 0,
                cookie_life_ms: 0,
            },
        )
    }
}

#[repr(C, packed(4))]
/// Linux `sctp_rtoinfo` layout for association retransmission timers.
struct SctpRtoInfoRaw {
    /// Association selected by the socket option.
    assoc_id: libc::sctp_assoc_t,
    /// Initial retransmission timeout in milliseconds.
    rto_initial_ms: u32,
    /// Maximum retransmission timeout in milliseconds.
    rto_max_ms: u32,
    /// Minimum retransmission timeout in milliseconds.
    rto_min_ms: u32,
}

impl SctpRtoInfoRaw {
    fn get(fd: RawFd) -> io::Result<Self> {
        get_sctp_opt_exact(
            fd,
            libc::SCTP_RTOINFO,
            Self {
                assoc_id: 0,
                rto_initial_ms: 0,
                rto_max_ms: 0,
                rto_min_ms: 0,
            },
        )
    }
}

#[repr(C)]
/// Fixed header preceding stream numbers in an `SCTP_RESET_STREAMS` request.
struct SctpResetStreamsHeader {
    /// Association selected by the request.
    assoc_id: libc::sctp_assoc_t,
    /// Incoming/outgoing stream-reset direction bits.
    flags: u16,
    /// Number of trailing stream identifiers.
    number_streams: u16,
}

/// Returns the allocation-free error class for an invalid reset shape.
fn invalid_sctp_reset_request() -> io::Error {
    io::Error::from(io::ErrorKind::InvalidInput)
}

/// Validates and encodes one Linux `sctp_reset_streams` request.
fn encode_sctp_reset_streams(request: &SctpResetStreams) -> io::Result<Vec<u8>> {
    let number_streams = match (request.intent, request.streams.is_empty()) {
        (SctpResetIntent::Listed, true) | (SctpResetIntent::All, false) => {
            return Err(invalid_sctp_reset_request());
        }
        (SctpResetIntent::All, true) => 0,
        (SctpResetIntent::Listed, false) => {
            u16::try_from(request.streams.len()).map_err(|_| invalid_sctp_reset_request())?
        }
    };

    let header_len = std::mem::size_of::<SctpResetStreamsHeader>();
    let streams_len = std::mem::size_of_val(request.streams.as_slice());
    let total_len = header_len
        .checked_add(streams_len)
        .ok_or_else(invalid_sctp_reset_request)?;
    libc::socklen_t::try_from(total_len).map_err(|_| invalid_sctp_reset_request())?;

    let mut buffer = vec![0u8; total_len];
    let header = SctpResetStreamsHeader {
        assoc_id: request.assoc_id,
        flags: request.flags,
        number_streams,
    };
    unsafe {
        std::ptr::copy_nonoverlapping(
            &header as *const SctpResetStreamsHeader as *const u8,
            buffer.as_mut_ptr(),
            header_len,
        );
        if streams_len != 0 {
            std::ptr::copy_nonoverlapping(
                request.streams.as_ptr() as *const u8,
                buffer.as_mut_ptr().add(header_len),
                streams_len,
            );
        }
    }

    Ok(buffer)
}

#[repr(C)]
/// Linux `sctp_add_streams` request layout.
struct SctpAddStreamsRaw {
    /// Association selected by the request.
    assoc_id: libc::sctp_assoc_t,
    /// Number of inbound streams requested.
    inbound_streams: u16,
    /// Number of outbound streams requested.
    outbound_streams: u16,
}

macro_rules! read_unaligned_field {
    ($base:expr, $field:ident) => {{
        let base = $base;
        unsafe { std::ptr::addr_of!((*base).$field).read_unaligned() }
    }};
}

impl SctpPaddrParamsRaw {
    fn from_fields(fields: SctpPaddrParamsFields) -> Self {
        Self {
            assoc_id: fields.assoc_id,
            address: fields.address,
            heartbeat_interval_ms: fields.heartbeat_interval_ms,
            path_max_retransmits: fields.path_max_retransmits,
            path_mtu: fields.path_mtu,
            sack_delay_ms: fields.sack_delay_ms,
            flags: fields.flags,
            ipv6_flow_label: fields.ipv6_flow_label,
            dscp: fields.dscp,
        }
    }

    fn to_fields(&self) -> SctpPaddrParamsFields {
        SctpPaddrParamsFields {
            assoc_id: read_unaligned_field!(self, assoc_id),
            address: read_unaligned_field!(self, address),
            heartbeat_interval_ms: read_unaligned_field!(self, heartbeat_interval_ms),
            path_max_retransmits: read_unaligned_field!(self, path_max_retransmits),
            path_mtu: read_unaligned_field!(self, path_mtu),
            sack_delay_ms: read_unaligned_field!(self, sack_delay_ms),
            flags: read_unaligned_field!(self, flags),
            ipv6_flow_label: read_unaligned_field!(self, ipv6_flow_label),
            dscp: read_unaligned_field!(self, dscp),
        }
    }
}

#[repr(C, packed(4))]
/// Linux `sctp_paddrinfo` layout returned for one peer path.
struct SctpPaddrInfoRaw {
    /// Association containing the path.
    assoc_id: libc::sctp_assoc_t,
    /// Peer transport address identifying the path.
    address: libc::sockaddr_storage,
    /// Kernel path state.
    state: i32,
    /// Current congestion window in bytes.
    congestion_window: u32,
    /// Smoothed round-trip time in milliseconds.
    srtt: u32,
    /// Current retransmission timeout in milliseconds.
    rto: u32,
    /// Path MTU in bytes.
    mtu: u32,
}

impl SctpPaddrInfoRaw {
    fn from_address(address: SocketAddr) -> Self {
        Self {
            assoc_id: 0,
            address: option_socket_addr_to_storage(Some(address)),
            state: 0,
            congestion_window: 0,
            srtt: 0,
            rto: 0,
            mtu: 0,
        }
    }

    fn to_public(&self) -> io::Result<SctpPeerAddrInfo> {
        let address = read_unaligned_field!(self, address);
        Ok(SctpPeerAddrInfo {
            assoc_id: read_unaligned_field!(self, assoc_id),
            address: socket_addr_from_c(&address, sockaddr_len_for_storage(address)?)?,
            state: read_unaligned_field!(self, state),
            congestion_window: read_unaligned_field!(self, congestion_window),
            srtt: read_unaligned_field!(self, srtt),
            rto: read_unaligned_field!(self, rto),
            mtu: read_unaligned_field!(self, mtu),
        })
    }
}

#[repr(C, packed(4))]
/// Linux `sctp_status` layout returned for an association.
struct SctpStatusRaw {
    /// Association described by this status block.
    assoc_id: libc::sctp_assoc_t,
    /// Kernel association state.
    state: i32,
    /// Peer-advertised receiver window in bytes.
    receiver_window: u32,
    /// Number of unacknowledged data chunks.
    unacked_data_chunks: u16,
    /// Number of data chunks pending transmission.
    pending_data_chunks: u16,
    /// Negotiated inbound stream count.
    inbound_streams: u16,
    /// Negotiated outbound stream count.
    outbound_streams: u16,
    /// Current fragmentation threshold in bytes.
    fragmentation_point: u32,
    /// Status of the association's current primary path.
    primary_path: SctpPaddrInfoRaw,
}

impl SctpStatusRaw {
    fn new() -> Self {
        Self {
            assoc_id: 0,
            state: 0,
            receiver_window: 0,
            unacked_data_chunks: 0,
            pending_data_chunks: 0,
            inbound_streams: 0,
            outbound_streams: 0,
            fragmentation_point: 0,
            primary_path: SctpPaddrInfoRaw {
                assoc_id: 0,
                address: unsafe { std::mem::zeroed() },
                state: 0,
                congestion_window: 0,
                srtt: 0,
                rto: 0,
                mtu: 0,
            },
        }
    }

    fn to_public(&self) -> io::Result<SctpAssocStatus> {
        Ok(SctpAssocStatus {
            assoc_id: read_unaligned_field!(self, assoc_id),
            state: read_unaligned_field!(self, state),
            receiver_window: read_unaligned_field!(self, receiver_window),
            unacked_data_chunks: read_unaligned_field!(self, unacked_data_chunks),
            pending_data_chunks: read_unaligned_field!(self, pending_data_chunks),
            inbound_streams: read_unaligned_field!(self, inbound_streams),
            outbound_streams: read_unaligned_field!(self, outbound_streams),
            fragmentation_point: read_unaligned_field!(self, fragmentation_point),
            primary_path: read_unaligned_field!(self, primary_path).to_public()?,
        })
    }
}

#[repr(C, packed)]
/// Legacy Linux `sctp_paddrparams` layout without flow-label or DSCP fields.
struct SctpPaddrParamsRawLegacy {
    /// Association selected by the parameter request.
    assoc_id: libc::sctp_assoc_t,
    /// Specific peer path, or an all-zero family for association-wide values.
    address: libc::sockaddr_storage,
    /// Heartbeat interval in milliseconds.
    heartbeat_interval_ms: u32,
    /// Maximum retransmissions before the selected path is considered failed.
    path_max_retransmits: u16,
    /// Configured path MTU in bytes.
    path_mtu: u32,
    /// Delayed-SACK interval in milliseconds.
    sack_delay_ms: u32,
    /// Legacy `SPP_*` option-selection and behavior bits.
    flags: u32,
}

const SCTP_PADDR_PARAMS_LEGACY_OPT_LEN: usize =
    align_sockopt_len(std::mem::size_of::<SctpPaddrParamsRawLegacy>());

impl SctpPaddrParamsRawLegacy {
    fn from_fields(fields: SctpPaddrParamsFields) -> Self {
        Self {
            assoc_id: fields.assoc_id,
            address: fields.address,
            heartbeat_interval_ms: fields.heartbeat_interval_ms,
            path_max_retransmits: fields.path_max_retransmits,
            path_mtu: fields.path_mtu,
            sack_delay_ms: fields.sack_delay_ms,
            flags: fields.flags,
        }
    }

    fn to_fields(&self) -> SctpPaddrParamsFields {
        SctpPaddrParamsFields {
            assoc_id: read_unaligned_field!(self, assoc_id),
            address: read_unaligned_field!(self, address),
            heartbeat_interval_ms: read_unaligned_field!(self, heartbeat_interval_ms),
            path_max_retransmits: read_unaligned_field!(self, path_max_retransmits),
            path_mtu: read_unaligned_field!(self, path_mtu),
            sack_delay_ms: read_unaligned_field!(self, sack_delay_ms),
            flags: read_unaligned_field!(self, flags),
            ipv6_flow_label: 0,
            dscp: 0,
        }
    }
}

fn decode_peer_addr_params_sockopt(
    buffer: &[u8; SCTP_PADDR_PARAMS_RAW_OPT_LEN],
    optlen: usize,
) -> io::Result<SctpPeerAddrParams> {
    if optlen == SCTP_PADDR_PARAMS_RAW_OPT_LEN {
        let raw = unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpPaddrParamsRaw) };
        return raw.to_fields().to_public();
    }

    if optlen == SCTP_PADDR_PARAMS_LEGACY_OPT_LEN {
        let raw =
            unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpPaddrParamsRawLegacy) };
        return raw.to_fields().to_public();
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "unexpected SCTP_PEER_ADDR_PARAMS length {} (expected {} or {})",
            optlen, SCTP_PADDR_PARAMS_RAW_OPT_LEN, SCTP_PADDR_PARAMS_LEGACY_OPT_LEN
        ),
    ))
}

fn configure_accepted_owner<T, F>(owner: T, configure: F) -> io::Result<T>
where
    F: FnOnce(&T) -> io::Result<()>,
{
    configure(&owner)?;
    Ok(owner)
}

fn finish_accepted_runtime_stream_with<F>(
    accepted_fd: RuntimeFd,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    config: SctpSocketConfig,
    apply_config: F,
) -> io::Result<(SctpStream, SocketAddr)>
where
    F: FnOnce(RawFd, SctpSocketConfig, LingerProvenance) -> io::Result<()>,
{
    let remote_addr = socket_addr_from_c(addr, addrlen)?;
    let accepted_fd = configure_accepted_owner(accepted_fd, |accepted_fd| {
        apply_config(
            accepted_fd.raw_fd(),
            config,
            accepted_fd.linger_provenance(),
        )
    })?;
    Ok((
        SctpStream::from_configured_runtime_fd(accepted_fd, remote_addr, config),
        remote_addr,
    ))
}

fn finish_accepted_owned_stream_with<F>(
    accepted_fd: OwnedFd,
    accepted_linger_provenance: LingerProvenance,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    config: SctpSocketConfig,
    apply_config: F,
) -> io::Result<(SctpStream, SocketAddr)>
where
    F: FnOnce(RawFd, SctpSocketConfig, LingerProvenance) -> io::Result<()>,
{
    finish_accepted_runtime_stream_with(
        RuntimeFd::from_owned_with_provenance(accepted_fd, accepted_linger_provenance),
        addr,
        addrlen,
        config,
        apply_config,
    )
}

#[cfg(test)]
fn finish_accepted_stream(
    accepted_fd: OwnedFd,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    config: SctpSocketConfig,
) -> io::Result<(SctpStream, SocketAddr)> {
    finish_accepted_owned_stream_with(
        accepted_fd,
        LingerProvenance::KnownNonPositive,
        addr,
        addrlen,
        config,
        apply_sctp_accepted_established_config,
    )
}

fn finish_connected_runtime_stream(
    connected_fd: OwnedFd,
    remote_addr: SocketAddr,
    config: SctpSocketConfig,
) -> SctpStream {
    SctpStream::from_configured_runtime_fd(
        RuntimeFd::from_fresh_owned(connected_fd),
        remote_addr,
        config,
    )
}

/// Reusable SCTP connect state with post-establishment socket configuration.
type ConnectSlot = ConnectSubmissionSlot<SctpSocketConfig>;

fn prepare_connect_slot(
    slot: &mut ConnectSlot,
    local_addr: Option<SocketAddr>,
    remote_addr: SocketAddr,
    config: SctpSocketConfig,
) -> io::Result<()> {
    if slot.in_use || !slot.state_ptr.is_null() {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }
    slot.cleanup_fd();
    slot.completion_data = config;
    slot.in_use = true;
    let fd = match new_sctp_socket(socket_domain(remote_addr), libc::SOCK_STREAM) {
        Ok(fd) => {
            // SAFETY: socket creation returned one fresh descriptor and this
            // local becomes its sole owner before any fallible setup step.
            unsafe { OwnedFd::from_raw_fd(fd) }
        }
        Err(err) => {
            slot.in_use = false;
            return Err(err);
        }
    };
    if let Err(err) = configure_sctp_socket(fd.as_raw_fd(), config) {
        slot.in_use = false;
        return Err(err);
    }

    if let Some(local_addr) = local_addr {
        if let Err(err) = set_reuse_addr(fd.as_raw_fd()) {
            slot.in_use = false;
            return Err(err);
        }
        let (sockaddr, sockaddr_len) = socket_addr_to_c(local_addr);
        let bind_res = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                &sockaddr as *const _ as *const libc::sockaddr,
                sockaddr_len,
            )
        };
        if bind_res < 0 {
            let err = io::Error::last_os_error();
            slot.in_use = false;
            return Err(err);
        }
    }

    slot.fd = Some(fd);
    slot.addr = Some(RetainedConnectAddr::from_socket_addr(remote_addr));
    Ok(())
}

/// One-to-one SCTP listener with a reusable accept slot.
///
/// Listener creation is setup work. Accepted [`SctpStream`] values carry the
/// steady-state data path.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{SctpInitConfig, SctpListener, SctpSocketConfig};
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let listener = SctpListener::bind_with_config(
///     SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
///     128,
///     SctpSocketConfig::data(SctpInitConfig::default()),
/// )?;
/// let _local = listener.local_addr();
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct SctpListener {
    /// Listening SCTP socket descriptor.
    fd: RuntimeFd,
    /// Local address bound to the listening socket.
    local_addr: SocketAddr,
    /// Reusable accept state for at most one in-flight accept future.
    accept_slot: AcceptSlot,
    /// Socket and receive-policy configuration retained for accepted streams.
    accepted_config: SctpSocketConfig,
}

impl SctpListener {
    /// Binds a listener with `SO_REUSEADDR`, applies init parameters, enables
    /// notifications, and starts listening.
    ///
    /// This is setup/control-plane work performed once before serving; it is
    /// not on the per-message data fast path.
    pub fn bind(addr: SocketAddr, backlog: i32, initmsg: SctpInitConfig) -> io::Result<Self> {
        Self::bind_with_config(addr, backlog, SctpSocketConfig::rich(initmsg))
    }

    /// Binds a listener using the provided SCTP socket configuration.
    ///
    /// This enables `SO_REUSEADDR` before binding. A socket-option failure is
    /// returned before `bind(2)` is attempted.
    ///
    /// This is setup/control-plane work performed once before serving; it is
    /// not on the per-message data fast path.
    pub fn bind_with_config(
        addr: SocketAddr,
        backlog: i32,
        config: SctpSocketConfig,
    ) -> io::Result<Self> {
        let fd = new_sctp_socket(socket_domain(addr), libc::SOCK_STREAM)?;
        if let Err(err) = configure_sctp_socket(fd, config) {
            close_fd(fd);
            return Err(err);
        }
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
            accepted_config: config,
        })
    }

    /// Returns the local address captured during successful listener
    /// construction.
    ///
    /// [`Self::bind`] and [`Self::bind_with_config`] query `getsockname(2)`
    /// once after `bind(2)` and `listen(2)` succeed, so a kernel-selected port
    /// from a port-zero bind is included. This method copies that cached
    /// address without a syscall, allocation, or runtime-context lookup. It
    /// does not refresh after changes made through the raw descriptor.
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
    /// use flowio::net::sctp::SctpListener;
    ///
    /// fn supervisor_should_rebuild(listener: &SctpListener) -> bool {
    ///     listener.is_terminal()
    /// }
    /// ```
    pub fn is_terminal(&self) -> bool {
        self.accept_slot.is_terminal()
    }

    /// Starts accepting one SCTP association.
    ///
    /// Accepting associations is setup/control-plane work, not the per-message
    /// data fast path. The accepted [`SctpStream`] carries the steady-state
    /// data path.
    ///
    /// A concurrent accept on the same listener is reported as an error when
    /// the returned future is first polled; safe borrowing makes that path
    /// unreachable except through intentionally leaked/forgotten futures.
    ///
    /// # Errors
    ///
    /// The returned future resolves with [`io::ErrorKind::WouldBlock`] if the
    /// listener's reusable accept slot is occupied or runtime operation
    /// capacity cannot accept the submission. `POLLHUP` or `POLLNVAL`
    /// readiness with no queued association latches the listener and returns
    /// [`io::ErrorKind::ConnectionAborted`]. Later accepts return the same
    /// non-retryable kind without another readiness submission. A bare
    /// `POLLERR` gets one internal readiness rearm; if it recurs for the same
    /// accept while `accept4` still reports `EAGAIN`, that exact
    /// [`io::ErrorKind::WouldBlock`] result is returned without latching.
    /// If applying post-accept SCTP socket or association configuration fails,
    /// that error is returned, the new association is closed, no stream is
    /// published, and the listener remains reusable.
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
    /// Dropping a prepared pending accept cancels only its readiness wait and
    /// leaves an already queued association for the next accept. An unprepared
    /// future owns no wait, so dropping it leaves the earlier accept untouched.
    /// If the listener's raw fd is exposed, the caller must not concurrently
    /// accept from it or race changes to its file-status flags.
    pub fn accept(&mut self) -> AcceptFuture<'_> {
        let input_error = self.accept_slot.prepare().err();
        let prepared = input_error.is_none();
        AcceptFuture {
            slot: &mut self.accept_slot,
            accepted_config: self.accepted_config,
            input_error,
            prepared,
        }
    }
}

impl AsRawFd for SctpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.expose_raw_fd()
    }
}

impl Drop for SctpListener {
    fn drop(&mut self) {
        // Safe non-forgotten use drops AcceptFuture before this exclusive
        // owner. The explicit cleanup handles a forgotten future whose cached
        // accept state remains in the listener.
        self.accept_slot.drop_cached_state();
    }
}

/// Initial SCTP association parameters used by listeners and connectors.
///
/// This is connection setup state. Construct it before binding or connecting;
/// it is not read on the established data fast path.
///
/// # Example
/// ```
/// use flowio::net::sctp::SctpInitConfig;
///
/// let init = SctpInitConfig {
///     outbound_streams: 32,
///     inbound_streams: 32,
///     ..SctpInitConfig::default()
/// };
/// assert_eq!(init.outbound_streams, 32);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpInitConfig {
    /// Requested outbound stream count during association setup.
    pub outbound_streams: u16,
    /// Requested inbound stream count during association setup.
    pub inbound_streams: u16,
    /// Maximum number of INIT retransmissions before setup fails.
    pub max_attempts: u16,
    /// Maximum timeout for INIT retransmissions, in milliseconds.
    pub max_init_timeout_ms: u16,
}

impl Default for SctpInitConfig {
    fn default() -> Self {
        Self {
            outbound_streams: 16,
            inbound_streams: 16,
            max_attempts: 4,
            max_init_timeout_ms: 0,
        }
    }
}

impl SctpInitConfig {
    /// Returns the crate's default one-to-one association configuration.
    ///
    /// This is currently equivalent to [`SctpInitConfig::default`].
    pub fn diameter_default() -> Self {
        Self::default()
    }

    fn as_raw(self) -> libc::sctp_initmsg {
        libc::sctp_initmsg {
            sinit_num_ostreams: self.outbound_streams,
            sinit_max_instreams: self.inbound_streams,
            sinit_max_attempts: self.max_attempts,
            sinit_max_init_timeo: self.max_init_timeout_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SctpSocketOptions {
    /// Notification mask applied while the socket is unconnected or before
    /// listener-accepted associations are finalized.
    notifications: SctpNotificationMask,
    /// Whether ancillary `SCTP_RCVINFO` metadata is requested from the kernel.
    recv_rcvinfo: bool,
    /// Whether `SCTP_NODELAY` is enabled on the socket.
    nodelay: bool,
    /// Optional default send metadata used by the data fast-path send APIs.
    default_send_info: Option<SctpSendInfo>,
    /// Optional `SO_SNDBUF` size to apply.
    send_buffer_size: Option<usize>,
    /// Optional `SO_RCVBUF` size to apply.
    recv_buffer_size: Option<usize>,
}

/// SCTP socket behavior configuration shared by listeners, connectors, and
/// stream operations.
///
/// Preferred on the per-message data fast path:
/// - Use [`SctpSocketConfig::data`] when the workload is data-only and does
///   not need notifications, `SCTP_RCVINFO`, EOR, or truncation reporting.
///
/// Avoid on the per-message data fast path:
/// - Avoid [`SctpSocketConfig::rich`] / [`SctpSocketConfig::signaling`]
///   for a data-only path. They enable richer metadata behavior for signaling
///   workloads rather than the leanest data path.
///
/// # Example
/// ```
/// use flowio::net::sctp::{SctpInitConfig, SctpSocketConfig};
///
/// let config = SctpSocketConfig::data(SctpInitConfig::default());
/// assert!(!config.recv_rcvinfo);
/// assert!(!config.notifications.shutdown);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpSocketConfig {
    /// Association setup parameters used before the socket connects or starts
    /// listening.
    pub init: SctpInitConfig,
    /// Which SCTP notifications are requested for caller-visible delivery.
    ///
    /// When `recv_rcvinfo` is true, FlowIO additionally keeps the kernel
    /// partial-delivery event subscribed for internal receive recovery.
    pub notifications: SctpNotificationMask,
    /// Whether `SCTP_RCVINFO` ancillary metadata is requested from the kernel.
    pub recv_rcvinfo: bool,
    /// Whether `SCTP_NODELAY` is enabled.
    pub nodelay: bool,
    /// Optional `SO_SNDBUF` size to apply to the socket.
    pub send_buffer_size: Option<usize>,
    /// Optional `SO_RCVBUF` size to apply to the socket.
    pub recv_buffer_size: Option<usize>,
    /// Optional default send metadata applied once to the socket. This is used
    /// by the fast-path [`SctpStream::send`] API.
    pub default_send_info: Option<SctpSendInfo>,
    /// Optional association-wide retransmission and RTO policy.
    pub assoc: Option<SctpAssocConfig>,
    /// Optional association-wide peer-address defaults.
    pub default_peer_addr_params: Option<SctpPeerAddrParams>,
}

impl Default for SctpSocketConfig {
    fn default() -> Self {
        Self::rich(SctpInitConfig::default())
    }
}

impl SctpSocketConfig {
    /// Rich metadata configuration matching [`SctpSocketConfig::default`].
    ///
    /// Avoid this on the data fast path when metadata is unnecessary; use
    /// [`SctpSocketConfig::data`] instead.
    pub fn rich(init: SctpInitConfig) -> Self {
        Self::signaling(init)
    }

    /// Signaling-oriented configuration with receive metadata and the crate's
    /// default rich notification mask.
    ///
    /// Use this when signaling consumers require metadata and notifications.
    /// [`SctpSocketConfig::data`] disables both for a lean data path.
    pub fn signaling(init: SctpInitConfig) -> Self {
        Self {
            init,
            notifications: SctpNotificationMask::signaling_default(),
            recv_rcvinfo: true,
            nodelay: true,
            send_buffer_size: None,
            recv_buffer_size: None,
            default_send_info: None,
            assoc: None,
            default_peer_addr_params: None,
        }
    }

    /// Data-fast-path configuration intended for [`SctpStream::send`] /
    /// [`SctpStream::recv`].
    ///
    /// This disables ancillary receive metadata and notifications for streams
    /// that carry only application data. Consequently, [`SctpStream::recv`]
    /// does not report EOR or truncation. `recv_msg` may still be used for
    /// those receive flags, but its ancillary fields remain at their defaults;
    /// use a rich/signaling config when stream, PPID, TSN, association
    /// metadata, notifications, or partial-delivery recovery are required.
    pub fn data(init: SctpInitConfig) -> Self {
        Self {
            init,
            notifications: SctpNotificationMask::none(),
            recv_rcvinfo: false,
            nodelay: true,
            send_buffer_size: None,
            recv_buffer_size: None,
            default_send_info: None,
            assoc: None,
            default_peer_addr_params: None,
        }
    }

    fn socket_options(self) -> SctpSocketOptions {
        SctpSocketOptions {
            notifications: effective_sctp_notification_mask(self.notifications, self.recv_rcvinfo),
            recv_rcvinfo: self.recv_rcvinfo,
            nodelay: self.nodelay,
            default_send_info: self.default_send_info,
            send_buffer_size: self.send_buffer_size,
            recv_buffer_size: self.recv_buffer_size,
        }
    }
}

/// SCTP connector that reuses one connect slot across attempts.
///
/// Each individual connect submission still gets its own `CompletionState`
/// from the reactor pool. The connector reuses slot storage, while each attempt
/// creates and configures a fresh socket and prepared remote address.
///
/// Reusing this type avoids rebuilding the slot wrapper across outbound
/// attempts. For data-only associations, pair it with
/// [`SctpSocketConfig::data`] instead of the richer default config.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpSocketConfig};
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut executor = Executor::new()?;
/// let mut connector = SctpConnector::with_config(
///     SctpSocketConfig::data(SctpInitConfig::default()),
/// );
/// executor.run(async move {
///     let _stream = connector
///         .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)))
///         .unwrap()
///         .await
///         .unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct SctpConnector {
    /// Reusable connect state kept across association attempts.
    connect_slot: ConnectSlot,
    /// Socket configuration applied to newly created association sockets.
    config: SctpSocketConfig,
    /// Optional local address to bind before connecting.
    local_addr: Option<SocketAddr>,
}

impl SctpConnector {
    /// Creates a connector with the provided init configuration.
    ///
    /// This uses the richer signaling-oriented socket configuration. Prefer
    /// [`SctpConnector::with_config`] plus [`SctpSocketConfig::data`] when the
    /// target workload is the SCTP data fast path.
    pub fn new(init: SctpInitConfig) -> Self {
        Self::with_config(SctpSocketConfig::rich(init))
    }

    /// Creates a connector with the provided SCTP socket configuration.
    ///
    /// This constructor gives the caller explicit control over signaling
    /// metadata, notifications, and data-path socket options.
    pub fn with_config(config: SctpSocketConfig) -> Self {
        Self {
            connect_slot: ConnectSlot::new(SctpSocketConfig::default()),
            config,
            local_addr: None,
        }
    }

    /// Pins the connector to a specific local address before connecting.
    ///
    /// Each connect attempt enables `SO_REUSEADDR` before binding this
    /// address. Socket-option and bind failures are returned by
    /// [`SctpConnector::connect`] or [`SctpConnector::connect_timeout`].
    pub fn with_local_addr(mut self, addr: SocketAddr) -> Self {
        self.local_addr = Some(addr);
        self
    }

    /// Starts connecting to the provided remote SCTP peer.
    ///
    /// Establishing an association is setup/control-plane work, not the
    /// per-message data fast path. Reusing this connector preserves its slot
    /// wrapper, while each attempt still creates and configures a fresh socket.
    /// Once connected, keep suitable steady-state traffic on
    /// [`SctpStream::send`] / [`SctpStream::recv`].
    pub fn connect(&mut self, remote_addr: SocketAddr) -> io::Result<ConnectFuture<'_>> {
        prepare_connect_slot(
            &mut self.connect_slot,
            self.local_addr,
            remote_addr,
            self.config,
        )?;
        Ok(ConnectFuture {
            slot: &mut self.connect_slot,
            remote_addr,
        })
    }

    /// Starts connecting to the provided remote SCTP peer with a deadline.
    ///
    /// Returns `TimedOut` if the association does not complete before the
    /// provided duration elapses. Timer-runtime failures, including
    /// `OutOfMemory`, propagate with their original [`io::ErrorKind`].
    ///
    /// This is a setup/control-plane convenience wrapper, not a fast path: it
    /// pairs an outbound connect with a per-attempt timeout. Resolve and
    /// connect during setup and keep steady-state traffic on
    /// [`SctpStream::send`] / [`SctpStream::recv`].
    pub fn connect_timeout(
        &mut self,
        remote_addr: SocketAddr,
        timeout_duration: Duration,
    ) -> io::Result<ConnectTimeoutFuture<'_>> {
        Ok(ConnectTimeoutFuture {
            inner: timeout(timeout_duration, self.connect(remote_addr)?),
        })
    }
}

impl Drop for SctpConnector {
    fn drop(&mut self) {
        self.connect_slot.drop_cached_state();
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SctpRecordSync {
    Synced,
    DataTail,
    /// A fragmented notification is consumed through EOR before the
    /// supported Linux receive API can make any later data visible.
    NotificationTail,
    /// A distinct notification is being classified while the underlying
    /// abandoned record remains a data tail.
    DataNotificationTail,
}

impl SctpRecordSync {
    #[inline(always)]
    const fn is_synced(self) -> bool {
        matches!(self, Self::Synced)
    }
}

type StashedSctpRecvProcessor =
    unsafe fn(*mut Reactor, *mut CompletionState, usize, &mut SctpRecvState);

const SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN: usize = SCTP_PARTIAL_DELIVERY_MIN_LEN;
const SCTP_NESTED_PREFIX_CLASSIFIED: u8 = 0x80;

enum SctpCompletionPublication {
    Visible(io::Result<Option<SctpRecvInfo>>),
    Unpublished,
}

#[must_use]
enum SctpMetadataCompletion {
    Consume,
    Publish(io::Result<SctpRecvMeta>),
}

/// Explicit lifecycle of the stream-owned dropped metadata receive.
///
/// This byte occupies existing natural padding in [`SctpRecvState`]; the
/// pointer and processor are payload for `Live`, while an `Abandoned` pointer
/// is deliberately opaque until stream drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum StashedSctpRecvState {
    Empty,
    Live,
    Abandoned,
}

/// Dropped metadata receive retained by the stream until its CQE is processed
/// or its origin ring is terminally abandoned.
#[derive(Clone, Copy)]
struct StashedSctpRecv {
    /// In-flight/completed operation state or the opaque ownership marker for
    /// a terminally ring-abandoned operation.
    state_ptr: *mut CompletionState,
    /// Initialized iovec count needed by the vectored completion processor.
    iov_count: usize,
    /// Type-specific function that consumes the retained payload and updates
    /// record synchronization state. This is populated only while the explicit
    /// state is `Live` and is never used to classify stash lifecycle.
    process_completed: Option<StashedSctpRecvProcessor>,
}

impl StashedSctpRecv {
    const fn empty() -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            iov_count: 0,
            process_completed: None,
        }
    }
}

struct SctpRecvState {
    /// Record origin whose unpublished tail must be consumed before another
    /// caller-visible metadata completion can be classified.
    record_sync: SctpRecordSync,
    /// Whether ordinary data completions are expected to carry SCTP_RCVINFO.
    ///
    /// This records FlowIO's configured policy rather than querying a socket
    /// option on the per-message receive path.
    recv_rcvinfo_requested: Cell<bool>,
    /// Whether PDAPI notifications were explicitly requested by the caller.
    ///
    /// A false value means FlowIO subscribed only to preserve metadata-receive
    /// resynchronization, so a valid abort event is consumed internally.
    partial_delivery_visible: Cell<bool>,
    /// Whether any kernel notification is part of the caller-visible mask.
    ///
    /// When false, every notification fragment belongs to FlowIO's sole
    /// forced PDAPI subscription and can be consumed without first assembling
    /// the complete notification in a short caller buffer.
    any_notification_visible: Cell<bool>,
    /// Explicit lifecycle tag for `stashed`. This stays separate from the
    /// payload so the full-width iovec count and existing layouts are retained.
    stashed_state: StashedSctpRecvState,
    /// Low seven bits retain the nested notification prefix length. The high
    /// bit records that the full bounded prefix was classified as non-abort.
    /// This control byte consumes prior natural padding.
    nested_prefix_state: u8,
    /// Dropped metadata receive completion that must be adopted before the
    /// next metadata receive can preserve record-boundary state, or terminal
    /// opaque poison retained after origin-ring abandonment.
    stashed: StashedSctpRecv,
    /// Bounded PDAPI classifier storage used only while a notification record
    /// interrupts an abandoned data tail. Variable notification tails are
    /// never retained or copied.
    nested_notification_prefix: [u8; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN],
}

impl SctpRecvState {
    /// Creates receive state for an externally configured descriptor.
    ///
    /// FlowIO cannot infer why an external socket subscribed to PDAPI, so it
    /// surfaces every valid notification to the caller.
    const fn external() -> Self {
        Self {
            record_sync: SctpRecordSync::Synced,
            recv_rcvinfo_requested: Cell::new(false),
            partial_delivery_visible: Cell::new(true),
            any_notification_visible: Cell::new(true),
            stashed_state: StashedSctpRecvState::Empty,
            nested_prefix_state: 0,
            stashed: StashedSctpRecv::empty(),
            nested_notification_prefix: [0; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN],
        }
    }

    /// Creates receive state from the caller-requested FlowIO socket policy.
    const fn configured(config: SctpSocketConfig) -> Self {
        Self {
            record_sync: SctpRecordSync::Synced,
            recv_rcvinfo_requested: Cell::new(config.recv_rcvinfo),
            partial_delivery_visible: Cell::new(config.notifications.partial_delivery),
            any_notification_visible: Cell::new(config.notifications.any()),
            stashed_state: StashedSctpRecvState::Empty,
            nested_prefix_state: 0,
            stashed: StashedSctpRecv::empty(),
            nested_notification_prefix: [0; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN],
        }
    }

    /// Publishes one live dropped receive into the stream-owned stash.
    ///
    /// # Safety
    ///
    /// `state_ptr` must identify a live operation whose retained payload
    /// matches `process_completed`, and this receive state must be empty.
    #[inline(always)]
    unsafe fn publish_stashed_live_unchecked(
        &mut self,
        state_ptr: *mut CompletionState,
        iov_count: usize,
        process_completed: StashedSctpRecvProcessor,
    ) {
        debug_assert_eq!(self.stashed_state, StashedSctpRecvState::Empty);
        debug_assert!(self.stashed.state_ptr.is_null());
        debug_assert!(self.stashed.process_completed.is_none());
        debug_assert!(!state_ptr.is_null(), "live SCTP stash is missing");
        self.stashed = StashedSctpRecv {
            state_ptr,
            iov_count,
            process_completed: Some(process_completed),
        };
        self.stashed_state = StashedSctpRecvState::Live;
    }

    /// Converts a live stash into its terminal opaque ownership marker.
    #[inline(always)]
    fn mark_stashed_abandoned(&mut self) {
        debug_assert_eq!(self.stashed_state, StashedSctpRecvState::Live);
        debug_assert!(!self.stashed.state_ptr.is_null());
        debug_assert!(self.stashed.process_completed.is_some());
        self.stashed.iov_count = 0;
        self.stashed.process_completed = None;
        self.stashed_state = StashedSctpRecvState::Abandoned;
    }

    /// Takes one live stash and publishes `Empty` before caller-owned payload
    /// destruction or completion processing can run.
    #[inline(always)]
    fn take_stashed_live(&mut self) -> StashedSctpRecv {
        debug_assert_eq!(self.stashed_state, StashedSctpRecvState::Live);
        debug_assert!(!self.stashed.state_ptr.is_null());
        debug_assert!(self.stashed.process_completed.is_some());
        let stashed = self.stashed;
        self.stashed = StashedSctpRecv::empty();
        self.stashed_state = StashedSctpRecvState::Empty;
        stashed
    }

    /// Clears only stream-local stash metadata without following an opaque
    /// abandonment marker.
    #[inline(always)]
    fn clear_stashed_local(&mut self) {
        self.stashed = StashedSctpRecv::empty();
        self.stashed_state = StashedSctpRecvState::Empty;
    }

    #[cfg(test)]
    unsafe fn set_stashed_live_for_test(
        &mut self,
        state_ptr: *mut CompletionState,
        iov_count: usize,
        process_completed: StashedSctpRecvProcessor,
    ) {
        unsafe {
            self.publish_stashed_live_unchecked(state_ptr, iov_count, process_completed);
        }
    }

    #[cfg(test)]
    fn set_stashed_abandoned_for_test(&mut self, marker: *mut CompletionState) {
        debug_assert_eq!(self.stashed_state, StashedSctpRecvState::Empty);
        debug_assert!(!marker.is_null());
        self.stashed = StashedSctpRecv {
            state_ptr: marker,
            iov_count: 0,
            process_completed: None,
        };
        self.stashed_state = StashedSctpRecvState::Abandoned;
    }

    /// Records a successfully applied caller-requested receive policy.
    fn set_receive_policy(&self, mask: SctpNotificationMask, recv_rcvinfo_requested: bool) {
        self.recv_rcvinfo_requested.set(recv_rcvinfo_requested);
        self.partial_delivery_visible.set(mask.partial_delivery);
        self.any_notification_visible.set(mask.any());
    }

    #[inline(always)]
    fn set_record_sync(&mut self, record_sync: SctpRecordSync) {
        self.record_sync = record_sync;
        if !matches!(record_sync, SctpRecordSync::DataNotificationTail) {
            self.nested_prefix_state = 0;
        }
    }

    #[inline(always)]
    fn nested_prefix_len(&self) -> usize {
        usize::from(self.nested_prefix_state & !SCTP_NESTED_PREFIX_CLASSIFIED)
    }

    #[inline(always)]
    fn begin_nested_notification(&mut self) {
        debug_assert_eq!(self.record_sync, SctpRecordSync::DataTail);
        self.record_sync = SctpRecordSync::DataNotificationTail;
        self.nested_prefix_state = 0;
    }

    #[inline(always)]
    fn bounded_recovery_prefix_target(&self, actual: usize, msg_flags: libc::c_int) -> usize {
        if sctp_msg_notification(msg_flags) {
            self.notification_recovery_prefix_target(actual)
        } else {
            0
        }
    }

    #[cold]
    #[inline(never)]
    fn notification_recovery_prefix_target(&self, actual: usize) -> usize {
        let missing = match self.record_sync {
            SctpRecordSync::DataTail => SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN,
            SctpRecordSync::DataNotificationTail
                if (self.nested_prefix_state & SCTP_NESTED_PREFIX_CLASSIFIED) == 0 =>
            {
                SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN.saturating_sub(self.nested_prefix_len())
            }
            SctpRecordSync::Synced
            | SctpRecordSync::NotificationTail
            | SctpRecordSync::DataNotificationTail => 0,
        };
        std::cmp::min(actual, missing)
    }

    /// Appends only the fixed PDAPI classifier prefix. The currently supported
    /// Linux UAPI record is exactly 24 bytes; a declaration for any extension
    /// fails closed against this fixed slice before it can retire record
    /// synchronization. Once a full prefix is known not to be an abort, later
    /// notification-tail bytes remain opaque.
    fn append_nested_notification_prefix(
        &mut self,
        bytes: &[u8],
    ) -> Option<io::Result<SctpRecvMeta>> {
        debug_assert_eq!(self.record_sync, SctpRecordSync::DataNotificationTail);
        if (self.nested_prefix_state & SCTP_NESTED_PREFIX_CLASSIFIED) != 0 {
            return None;
        }

        let prefix_len = self.nested_prefix_len();
        debug_assert!(prefix_len <= SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN);
        let appended = std::cmp::min(
            bytes.len(),
            SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN - prefix_len,
        );
        self.nested_notification_prefix[prefix_len..prefix_len + appended]
            .copy_from_slice(&bytes[..appended]);
        let prefix_len = prefix_len + appended;
        self.nested_prefix_state = prefix_len as u8;
        if prefix_len != SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN {
            return None;
        }
        Some(parse_notification(
            &self.nested_notification_prefix[..prefix_len],
        ))
    }

    #[inline(always)]
    fn transition_unpublished_synced_completion(
        &mut self,
        actual: usize,
        header: SctpRecvHeader,
        partial_delivery_abort: bool,
    ) {
        let next =
            if partial_delivery_abort || sctp_msg_end_of_record(header.msg_flags) || actual == 0 {
                SctpRecordSync::Synced
            } else if sctp_msg_notification(header.msg_flags) {
                SctpRecordSync::NotificationTail
            } else {
                SctpRecordSync::DataTail
            };
        self.set_record_sync(next);
    }

    #[inline(always)]
    fn parse_completion_meta(
        &self,
        rcvinfo: io::Result<Option<SctpRecvInfo>>,
        msg_flags: libc::c_int,
        data_slice: &[u8],
        parsed_notification: Option<io::Result<SctpRecvMeta>>,
    ) -> io::Result<SctpRecvMeta> {
        classify_recv_meta(
            rcvinfo,
            msg_flags,
            data_slice,
            self.recv_rcvinfo_requested.get(),
            parsed_notification,
        )
    }

    fn publish_metadata_completion(
        &mut self,
        actual: usize,
        header: SctpRecvHeader,
        data_slice: &[u8],
        rcvinfo: io::Result<Option<SctpRecvInfo>>,
        parsed_notification: Option<io::Result<SctpRecvMeta>>,
        partial_delivery_abort: bool,
    ) -> SctpMetadataCompletion {
        let meta =
            self.parse_completion_meta(rcvinfo, header.msg_flags, data_slice, parsed_notification);
        if meta.is_err()
            && !partial_delivery_abort
            && sctp_msg_partial_nonempty(actual, header.msg_flags)
        {
            let tail = if sctp_msg_notification(header.msg_flags) {
                SctpRecordSync::NotificationTail
            } else {
                SctpRecordSync::DataTail
            };
            self.set_record_sync(tail);
        }
        SctpMetadataCompletion::Publish(meta)
    }

    fn process_nested_notification_completion(
        &mut self,
        actual: usize,
        header: SctpRecvHeader,
        data_slice: &[u8],
        recovery_prefix: &[u8],
        publication: SctpCompletionPublication,
    ) -> SctpMetadataCompletion {
        debug_assert_eq!(self.record_sync, SctpRecordSync::DataNotificationTail);
        let parsed_notification = self.append_nested_notification_prefix(recovery_prefix);
        let partial_delivery_abort =
            sctp_notification_retires_discard(parsed_notification.as_ref());
        if parsed_notification.is_some() && !partial_delivery_abort {
            self.nested_prefix_state |= SCTP_NESTED_PREFIX_CLASSIFIED;
        }

        if partial_delivery_abort {
            self.set_record_sync(SctpRecordSync::Synced);
        } else if sctp_msg_end_of_record(header.msg_flags) {
            self.set_record_sync(SctpRecordSync::DataTail);
        }

        match publication {
            SctpCompletionPublication::Visible(rcvinfo)
                if partial_delivery_abort && self.partial_delivery_visible.get() =>
            {
                self.publish_metadata_completion(
                    actual,
                    header,
                    data_slice,
                    rcvinfo,
                    parsed_notification,
                    true,
                )
            }
            SctpCompletionPublication::Visible(_) | SctpCompletionPublication::Unpublished => {
                SctpMetadataCompletion::Consume
            }
        }
    }

    /// Classifies and transitions one successful metadata completion exactly
    /// once. `recovery_prefix` is the first at most 24 bytes across all active
    /// iovecs only while nested recovery needs missing classifier bytes;
    /// `data_slice` preserves the ordinary contiguous parsing surface.
    fn process_metadata_completion(
        &mut self,
        actual: usize,
        header: SctpRecvHeader,
        data_slice: &[u8],
        recovery_prefix: &[u8],
        publication: SctpCompletionPublication,
    ) -> SctpMetadataCompletion {
        debug_assert_eq!(
            recovery_prefix.len(),
            self.bounded_recovery_prefix_target(actual, header.msg_flags),
            "SCTP recovery prefix does not match the missing classifier bytes"
        );

        if sctp_msg_clean_eof(actual, header) {
            self.set_record_sync(SctpRecordSync::Synced);
            return match publication {
                SctpCompletionPublication::Visible(_) => {
                    SctpMetadataCompletion::Publish(Ok(sctp_eof_recv_meta()))
                }
                SctpCompletionPublication::Unpublished => SctpMetadataCompletion::Consume,
            };
        }

        match self.record_sync {
            SctpRecordSync::NotificationTail => {
                if sctp_msg_end_of_record(header.msg_flags) {
                    self.set_record_sync(SctpRecordSync::Synced);
                }
                SctpMetadataCompletion::Consume
            }
            SctpRecordSync::DataTail if sctp_msg_notification(header.msg_flags) => {
                self.begin_nested_notification();
                self.process_nested_notification_completion(
                    actual,
                    header,
                    data_slice,
                    recovery_prefix,
                    publication,
                )
            }
            SctpRecordSync::DataTail => {
                if sctp_msg_end_of_record(header.msg_flags) {
                    self.set_record_sync(SctpRecordSync::Synced);
                }
                SctpMetadataCompletion::Consume
            }
            SctpRecordSync::DataNotificationTail if sctp_msg_notification(header.msg_flags) => self
                .process_nested_notification_completion(
                    actual,
                    header,
                    data_slice,
                    recovery_prefix,
                    publication,
                ),
            SctpRecordSync::DataNotificationTail => {
                if sctp_msg_end_of_record(header.msg_flags) {
                    self.set_record_sync(SctpRecordSync::NotificationTail);
                }
                SctpMetadataCompletion::Consume
            }
            SctpRecordSync::Synced => {
                let parsed_notification =
                    parse_sctp_notification_once(data_slice, header.msg_flags);
                let partial_delivery_abort =
                    sctp_notification_retires_discard(parsed_notification.as_ref());

                match publication {
                    SctpCompletionPublication::Unpublished => {
                        self.transition_unpublished_synced_completion(
                            actual,
                            header,
                            partial_delivery_abort,
                        );
                        SctpMetadataCompletion::Consume
                    }
                    SctpCompletionPublication::Visible(_)
                        if sctp_msg_notification(header.msg_flags)
                            && !self.any_notification_visible.get() =>
                    {
                        self.transition_unpublished_synced_completion(
                            actual,
                            header,
                            partial_delivery_abort,
                        );
                        SctpMetadataCompletion::Consume
                    }
                    SctpCompletionPublication::Visible(_)
                        if partial_delivery_abort && !self.partial_delivery_visible.get() =>
                    {
                        SctpMetadataCompletion::Consume
                    }
                    SctpCompletionPublication::Visible(rcvinfo) => self
                        .publish_metadata_completion(
                            actual,
                            header,
                            data_slice,
                            rcvinfo,
                            parsed_notification,
                            partial_delivery_abort,
                        ),
                }
            }
        }
    }

    #[cfg(test)]
    fn process_unpublished_for_test(&mut self, data_slice: &[u8], msg: &libc::msghdr) {
        let recovery_target = self.bounded_recovery_prefix_target(data_slice.len(), msg.msg_flags);
        let action = self.process_metadata_completion(
            data_slice.len(),
            SctpRecvHeader::from_msghdr(msg),
            data_slice,
            &data_slice[..recovery_target],
            SctpCompletionPublication::Unpublished,
        );
        debug_assert!(matches!(action, SctpMetadataCompletion::Consume));
    }

    #[cfg(test)]
    fn should_consume_for_test(&mut self, data_slice: &[u8], msg: &libc::msghdr) -> bool {
        let recovery_target = self.bounded_recovery_prefix_target(data_slice.len(), msg.msg_flags);
        let action = self.process_metadata_completion(
            data_slice.len(),
            SctpRecvHeader::from_msghdr(msg),
            data_slice,
            &data_slice[..recovery_target],
            SctpCompletionPublication::Visible(Ok(None)),
        );
        matches!(action, SctpMetadataCompletion::Consume)
    }

    /// Transfers an in-flight metadata receive from a dropped future into the
    /// stream-owned recovery slot.
    ///
    /// # Safety
    ///
    /// A non-null `*state_slot` must identify a live operation owned by this
    /// stream's reactor, `process_completed` must match its retained payload
    /// type, and no other receive may already be stashed.
    unsafe fn stash_unchecked(
        recv_state: *mut Self,
        state_slot: *mut *mut CompletionState,
        iov_count: usize,
        process_completed: StashedSctpRecvProcessor,
    ) {
        debug_assert!(!recv_state.is_null(), "SCTP receive state is missing");
        debug_assert!(!state_slot.is_null(), "SCTP operation slot is missing");
        let state_ptr = unsafe { *state_slot };
        if state_ptr.is_null() {
            return;
        }
        debug_assert_eq!(
            unsafe { (*recv_state).stashed_state },
            StashedSctpRecvState::Empty,
            "SCTP stream already has a stashed metadata receive"
        );
        unsafe {
            // Publish stream ownership before releasing the waiter. Its final
            // task reference may run user drop glue and synchronously re-enter
            // this receive state.
            (*recv_state).publish_stashed_live_unchecked(state_ptr, iov_count, process_completed);
            *state_slot = std::ptr::null_mut();

            let waiter = CompletionState::take_waiter_unchecked(state_ptr);
            if !waiter.is_null() {
                release_task(waiter);
            }
        }
    }

    /// Clears any waiter retained by the stream-owned dropped receive.
    ///
    /// # Safety
    ///
    /// A non-null live stashed pointer must identify a completion state that
    /// this receive state exclusively owns. A terminal abandonment marker is
    /// opaque and is deliberately not dereferenced.
    unsafe fn clear_stashed_waiter_unchecked(recv_state: *mut Self) {
        debug_assert!(!recv_state.is_null(), "SCTP receive state is missing");
        if unsafe { (*recv_state).stashed_state != StashedSctpRecvState::Live } {
            return;
        }
        let state_ptr = unsafe { (*recv_state).stashed.state_ptr };
        if !state_ptr.is_null() {
            let waiter = unsafe { CompletionState::take_waiter_unchecked(state_ptr) };
            if !waiter.is_null() {
                unsafe { release_task(waiter) };
            }
        }
    }

    /// Polls and consumes the previously dropped metadata receive, if any.
    ///
    /// # Safety
    ///
    /// A live stashed pointer and processor must satisfy
    /// [`SctpRecvState::stash_unchecked`], and `cx` must carry the FlowIO waker
    /// for the owning reactor. A terminal abandonment marker is opaque and
    /// returns before either pointer or waker is inspected.
    unsafe fn poll_stashed(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let state_ptr = match self.stashed_state {
            StashedSctpRecvState::Empty => return Poll::Ready(Ok(())),
            StashedSctpRecvState::Abandoned => {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
            }
            StashedSctpRecvState::Live => self.stashed.state_ptr,
        };

        if unsafe { !(*state_ptr).is_completed() } {
            if unsafe { refresh_op_waiter_from_waker(cx, state_ptr) } {
                self.mark_stashed_abandoned();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
            }
            return Poll::Pending;
        }

        let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
        let stashed = self.take_stashed_live();
        let process_completed = stashed.process_completed;
        debug_assert!(
            process_completed.is_some(),
            "stashed SCTP recv missing completion processor"
        );
        let process_completed = unsafe { process_completed.unwrap_unchecked() };
        unsafe { process_completed(op_ctx.reactor(), state_ptr, stashed.iov_count, self) };
        if op_ctx.context_rejected() {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Orphans/cancels a live stashed receive during stream teardown, or
    /// clears a terminal abandonment marker without dereferencing it.
    ///
    /// # Safety
    ///
    /// A non-null live stashed pointer must identify the exclusively owned
    /// operation transferred by [`SctpRecvState::stash_unchecked`]. A terminal
    /// abandonment marker must remain opaque.
    unsafe fn drop_stashed(&mut self) {
        match self.stashed_state {
            StashedSctpRecvState::Empty => {}
            StashedSctpRecvState::Abandoned => self.clear_stashed_local(),
            StashedSctpRecvState::Live => {
                let mut state_ptr = self.take_stashed_live().state_ptr;
                unsafe { drop_op_ptr_unchecked(&mut state_ptr) };
            }
        }
    }

    /// Returns whether rich receive work must restore record synchronization
    /// before a lean receive may consume another kernel completion.
    #[inline(always)]
    fn pending_metadata_recovery(&self) -> bool {
        !self.record_sync.is_synced() || self.stashed_state != StashedSctpRecvState::Empty
    }
}

#[cfg(any(test, feature = "fuzzing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SctpRecoveryFuzzScenario {
    split: usize,
    terminal: bool,
    visible: bool,
    second_is_notification: bool,
}

/// Visits the exact bounded scenario inventory used by record-recovery fuzzing.
#[cfg(any(test, feature = "fuzzing"))]
#[inline(always)]
fn for_each_sctp_recovery_fuzz_scenario(
    data: &[u8],
    mut visit: impl FnMut(SctpRecoveryFuzzScenario),
) {
    let split_limit = std::cmp::min(data.len(), SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN);
    for split in 0..=split_limit {
        for terminal in [false, true] {
            for visible in [false, true] {
                for second_is_notification in [false, true] {
                    visit(SctpRecoveryFuzzScenario {
                        split,
                        terminal,
                        visible,
                        second_is_notification,
                    });
                }
            }
        }
    }
}

/// Exercises every bounded split of one arbitrary notification prefix through
/// the production nested-data-tail classifier.
#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_sctp_record_recovery(data: &[u8]) {
    for_each_sctp_recovery_fuzz_scenario(data, |scenario| {
        let SctpRecoveryFuzzScenario {
            split,
            terminal,
            visible,
            second_is_notification,
        } = scenario;
        let mut state = SctpRecvState::external();
        state.set_record_sync(SctpRecordSync::DataTail);

        let first = &data[..split];
        let first_flags = libc::MSG_NOTIFICATION;
        let first_target = state.bounded_recovery_prefix_target(first.len(), first_flags);
        let first_publication = if visible {
            SctpCompletionPublication::Visible(Ok(None))
        } else {
            SctpCompletionPublication::Unpublished
        };
        let _ = state.process_metadata_completion(
            first.len(),
            SctpRecvHeader {
                msg_controllen: 0,
                msg_flags: first_flags,
            },
            first,
            &first[..first_target],
            first_publication,
        );

        let second = &data[split..];
        let second_flags = if second_is_notification {
            libc::MSG_NOTIFICATION
        } else {
            0
        } | if terminal { libc::MSG_EOR } else { 0 };
        let second_target = state.bounded_recovery_prefix_target(second.len(), second_flags);
        let second_publication = if visible {
            SctpCompletionPublication::Visible(Ok(None))
        } else {
            SctpCompletionPublication::Unpublished
        };
        let _ = state.process_metadata_completion(
            second.len(),
            SctpRecvHeader {
                msg_controllen: 0,
                msg_flags: second_flags,
            },
            second,
            &second[..second_target],
            second_publication,
        );
        std::hint::black_box((state.record_sync, state.nested_prefix_state));
    });
}

/// Applies one effective kernel notification mask before publishing the
/// matching caller-visible receive policy.
fn apply_sctp_notification_mask<F>(
    recv_state: &SctpRecvState,
    mask: SctpNotificationMask,
    recv_rcvinfo_requested: bool,
    apply_events: F,
) -> io::Result<()>
where
    F: FnOnce(SctpNotificationMask) -> io::Result<()>,
{
    let effective = effective_sctp_notification_mask(mask, recv_rcvinfo_requested);
    apply_events(effective)?;
    recv_state.set_receive_policy(mask, recv_rcvinfo_requested);
    Ok(())
}

/// One-to-one SCTP association with generic buffer support.
///
/// Preferred on the per-message data fast path:
/// - Use [`SctpStream::send`] / [`SctpStream::recv`] when the socket was
///   configured for data-only traffic and the caller does not need per-message
///   metadata or notifications.
///
/// Avoid on the per-message data fast path:
/// - Avoid [`SctpStream::send_msg`] / [`SctpStream::recv_msg`]
///   or the richer signaling config when the workload is just message data.
/// - Do not use [`SctpStream::recv`] when EOR, truncation detection, receive
///   metadata, or notifications are part of the protocol contract.
///
/// # Metadata receive semantics
///
/// [`SctpStream::recv_msg`] and [`SctpStream::recv_msg_vectored`] return
/// [`io::ErrorKind::InvalidData`] when the kernel reports a truncated receive
/// or a non-empty receive without SCTP end-of-record. An oversized record
/// returns that error once; later metadata receives discard its unrecoverable
/// tail through the next record boundary before resuming delivery. Until that
/// boundary is restored, the data-only [`SctpStream::recv`] path returns
/// [`io::ErrorKind::InvalidInput`] instead of consuming recovery bytes. Keep
/// using [`SctpStream::recv_msg`] or [`SctpStream::recv_msg_vectored`] until a
/// rich receive completes recovery.
///
/// A kernel zero-byte completion with no control message and no flags is clean
/// peer EOF and resolves as
/// `Ok((0, SctpRecvMeta::Data(SctpRecvInfo::default())))`. Both methods reject
/// zero-length caller destinations before submission or adoption of a prior
/// dropped metadata receive, so such a request cannot masquerade as EOF. The
/// one-entry dropped-receive stash remains owned by the stream for the next
/// valid metadata receive or stream destruction. When polled without a valid
/// FlowIO context, `NotConnected` retains precedence over local `InvalidInput`
/// and the stash is likewise untouched. When the stream's stored receive-info
/// policy is disabled, ordinary data with no receive info succeeds with default
/// ancillary fields and the kernel's end-of-record flag, including when
/// complete unrelated socket control records are present. An externally
/// adopted descriptor initially has that disabled policy because adoption
/// performs no option query. A successful later
/// [`SctpStream::set_notification_mask`] call refreshes the policy from the
/// descriptor's live `SCTP_RECVRCVINFO` setting. When the stored policy is
/// enabled, ordinary data that omits receive info is `InvalidData`. Metadata
/// receive uses fixed-capacity control storage sized for common Linux timestamp,
/// timestamping packet-info, receive-queue-overflow, and RCVINFO records; it
/// never allocates control storage per message. Additional externally enabled
/// records such as `SCTP_NXTINFO`, socket mark/priority, or Wi-Fi status are
/// skipped if they fit but are outside that guaranteed combination. When
/// `MSG_CTRUNC` arrives without usable `SCTP_RCVINFO`, `InvalidData` identifies
/// fixed control-buffer capacity exhaustion. Present malformed control instead
/// retains its specific parser diagnostic even if `MSG_CTRUNC` is also set.
/// Intact receive info remains usable when only later control was truncated.
/// Kernel receive errors are returned as `io::Error` values from the completed
/// operation.
///
/// # In-flight drop ownership
///
/// Dropping an in-flight receive or send future relinquishes the caller buffer
/// to the runtime until the original kernel completion retires; the buffer is
/// not returned to the caller on that cancellation path. Dropped metadata
/// receives retain the stream's single rich-receive lineage until they are
/// adopted by the next valid metadata receive or reclaimed by stream
/// destruction. While that rich operation occupies the stream-owned stash,
/// whether pending, completed, or exceptionally ring-abandoned, or while rich
/// receive is discarding an oversized record tail,
/// [`SctpStream::recv`] returns allocation-free
/// [`io::ErrorKind::InvalidInput`] with the exact rental buffer and submits no
/// second receive. Repeated lean rejections do not consume or modify the
/// recovery state. If the stash's origin ring is abandoned before its target
/// CQE is observed, the first rich recovery that detects abandonment converts
/// the stash to a terminal opaque ownership marker. That receive and every
/// later rich receive return [`io::ErrorKind::NotConnected`] with their exact
/// unsubmitted buffer; stream destruction clears only the local marker while
/// the abandoned operation and retained payload remain unreclaimed. Otherwise,
/// adoption updates SCTP record-boundary resynchronization state from the
/// retired completion. Keep using
/// [`SctpStream::recv_msg`] or [`SctpStream::recv_msg_vectored`] until the next
/// record boundary is reached unless rich recovery reports terminal
/// abandonment. Dropping a lean receive
/// retains its established terminal-framing policy; a later receive cannot
/// recover bytes consumed by that cancelled bare receive. Notifications
/// observed during internal discard are consumed as control events, except an
/// explicitly requested partial-delivery abort remains caller-visible while
/// retiring discard. An EOR-marked notification tail or a partial-delivery-
/// aborted notification retires discard; other notification fragments keep
/// discard active.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpSocketConfig};
/// use flowio::runtime::executor::Executor;
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// let mut executor = Executor::new()?;
/// executor.run(async move {
///     let mut connector = SctpConnector::with_config(
///         SctpSocketConfig::data(SctpInitConfig::default()),
///     );
///     let mut stream = connector
///         .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)))
///         .unwrap()
///         .await
///         .unwrap();
///     let _peer = stream.peer_addr();
///     let (res, _buf) = stream.send(b"ping".to_vec()).await;
///     res.unwrap();
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct SctpStream {
    /// Owned SCTP association socket descriptor.
    fd: RuntimeFd,
    /// Remote address recorded when the association was accepted or connected.
    remote_addr: SocketAddr,
    /// Metadata-receive resynchronization and dropped-completion state.
    recv_state: SctpRecvState,
}

impl SctpStream {
    /// Safely takes ownership of an SCTP socket and records its remote peer.
    ///
    /// The descriptor is closed exactly once when the FlowIO stream is
    /// dropped. `OwnedFd` proves unique close ownership, but does not validate
    /// the socket type, supplied peer address, or configuration. The caller is
    /// responsible for nonblocking mode and socket options compatible with the
    /// data or metadata APIs it uses. Existing partial-delivery subscriptions
    /// on an adopted descriptor remain caller-visible unless a later
    /// [`SctpStream::set_notification_mask`] call changes that policy.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::sctp::SctpStream;
    /// use std::net::SocketAddr;
    /// use std::os::fd::OwnedFd;
    ///
    /// fn adopt_configured_socket(fd: OwnedFd, peer: SocketAddr) -> SctpStream {
    ///     SctpStream::from_owned_fd(fd, peer)
    /// }
    /// ```
    ///
    /// Safe code cannot adopt the same owner twice:
    /// ```compile_fail
    /// use flowio::net::sctp::SctpStream;
    /// use std::net::{Ipv4Addr, SocketAddr};
    /// use std::os::fd::OwnedFd;
    ///
    /// let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 3868));
    /// let owned: OwnedFd = std::fs::File::open("/dev/null").unwrap().into();
    /// let _first = SctpStream::from_owned_fd(owned, peer);
    /// let _second = SctpStream::from_owned_fd(owned, peer);
    /// ```
    pub fn from_owned_fd(fd: OwnedFd, remote_addr: SocketAddr) -> Self {
        Self::from_runtime_fd_with_recv_state(
            RuntimeFd::from_external_owned(fd),
            remote_addr,
            SctpRecvState::external(),
        )
    }

    /// Takes an internally configured descriptor and preserves its requested
    /// notification-visibility policy separately from the effective kernel
    /// subscription mask.
    fn from_configured_runtime_fd(
        fd: RuntimeFd,
        remote_addr: SocketAddr,
        config: SctpSocketConfig,
    ) -> Self {
        Self::from_runtime_fd_with_recv_state(fd, remote_addr, SctpRecvState::configured(config))
    }

    fn from_runtime_fd_with_recv_state(
        fd: RuntimeFd,
        remote_addr: SocketAddr,
        recv_state: SctpRecvState,
    ) -> Self {
        Self {
            fd,
            remote_addr,
            recv_state,
        }
    }

    /// Takes ownership of a bare SCTP socket descriptor and records its peer.
    ///
    /// Callers supplying an external descriptor are responsible for applying
    /// nonblocking mode and socket options compatible with the data or
    /// metadata APIs they use. Existing partial-delivery subscriptions remain
    /// caller-visible unless a later [`SctpStream::set_notification_mask`]
    /// call changes that policy.
    ///
    /// # Safety
    ///
    /// `fd` must be a valid open descriptor for which the caller owns the sole
    /// close responsibility. After this call, the caller must not close `fd`,
    /// reuse it, or create another owning wrapper for the same descriptor.
    ///
    /// Calling raw adoption without an explicit safety boundary is rejected:
    /// ```compile_fail
    /// use flowio::net::sctp::SctpStream;
    /// use std::net::{Ipv4Addr, SocketAddr};
    ///
    /// let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 3868));
    /// let _stream = SctpStream::from_raw_fd(3, peer);
    /// ```
    pub unsafe fn from_raw_fd(fd: RawFd, remote_addr: SocketAddr) -> Self {
        // SAFETY: the caller promises sole ownership of this valid descriptor.
        Self::from_owned_fd(unsafe { OwnedFd::from_raw_fd(fd) }, remote_addr)
    }

    /// Returns the local address currently assigned to the association socket.
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

    /// Returns the peer address recorded when the association was accepted or
    /// connected.
    ///
    /// This is cached association metadata, not a live kernel query, and does
    /// not submit SCTP data-path I/O.
    pub fn peer_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Sets the `SO_SNDBUF` socket send buffer size.
    ///
    /// This is socket configuration/control-plane work. Apply it during
    /// association setup instead of changing it per send.
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
    /// association setup instead of changing it per receive.
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

    /// Shuts down the read, write, or both halves of this association socket.
    ///
    /// This is association control-plane work, normally used for teardown or
    /// protocol half-close rather than steady-state data transfer.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        super::shutdown_socket(self.fd.raw_fd(), how)
    }

    /// Returns all local addresses currently associated with the stream.
    ///
    /// This is a current Linux SCTP association snapshot, not cached metadata.
    /// Addresses retain the kernel-reported order.
    /// The query starts with space for 8 `sockaddr_storage` units and, only on
    /// `ENOMEM`, doubles that budget through 1,024 units over at most eight
    /// attempts. It is status/control-plane work, not the per-message data
    /// fast path.
    ///
    /// # Errors
    ///
    /// Non-`ENOMEM` socket-option errors return immediately, and exhaustion of
    /// the bounded retry budget preserves `ENOMEM`. Malformed returned lengths,
    /// address counts, or packed address families return
    /// [`io::ErrorKind::InvalidData`].
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        get_assoc_addrs(self.fd.raw_fd(), SCTP_GET_LOCAL_ADDRS_OPT, 0)
    }

    /// Returns all peer addresses currently associated with the stream.
    ///
    /// This is a current Linux SCTP association snapshot, not the single cached
    /// address returned by [`SctpStream::peer_addr`]. Addresses retain the
    /// kernel-reported order. The query starts with space for 8
    /// `sockaddr_storage` units and, only on `ENOMEM`, doubles that budget
    /// through 1,024 units over at most eight attempts. It is
    /// status/control-plane work, not the per-message data fast path.
    ///
    /// # Errors
    ///
    /// Non-`ENOMEM` socket-option errors return immediately, and exhaustion of
    /// the bounded retry budget preserves `ENOMEM`. Malformed returned lengths,
    /// address counts, or packed address families return
    /// [`io::ErrorKind::InvalidData`].
    pub fn peer_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        get_assoc_addrs(self.fd.raw_fd(), SCTP_GET_PEER_ADDRS_OPT, 0)
    }

    /// Returns current association status, including the current primary path.
    ///
    /// This is capability-dependent and may be unavailable on kernels with
    /// limited SCTP status support. It is status/control-plane work, not the
    /// per-message data fast path.
    pub fn status(&self) -> io::Result<SctpAssocStatus> {
        get_sctp_opt_exact(self.fd.raw_fd(), libc::SCTP_STATUS, SctpStatusRaw::new())?.to_public()
    }

    /// Returns read-only transport information for one peer address.
    ///
    /// This depends on `SCTP_GET_PEER_ADDR_INFO` support in the running
    /// kernel. It is status/control-plane work, not the per-message data fast
    /// path.
    pub fn peer_addr_info(&self, peer_addr: SocketAddr) -> io::Result<SctpPeerAddrInfo> {
        get_sctp_opt_exact(
            self.fd.raw_fd(),
            libc::SCTP_GET_PEER_ADDR_INFO,
            SctpPaddrInfoRaw::from_address(peer_addr),
        )?
        .to_public()
    }

    /// Returns read-only information for the primary transport path.
    ///
    /// This is derived from [`SctpStream::status`] and has the same capability
    /// requirements. It is status/control-plane work, not the per-message data
    /// fast path.
    pub fn primary_path_info(&self) -> io::Result<SctpPeerAddrInfo> {
        self.status().map(|status| status.primary_path)
    }

    /// Returns SCTP association reconfiguration capabilities reported by the kernel.
    ///
    /// This depends on Linux SCTP reconfiguration support and may fail even
    /// when baseline SCTP messaging is available. It is control-plane work,
    /// not the per-message data fast path.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::SctpStream;
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// let supported = stream.reconfig_supported()?;
    /// let _flags = supported.flags;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconfig_supported(&self) -> io::Result<SctpReconfigFlags> {
        let raw = get_sctp_opt_exact(
            self.fd.raw_fd(),
            SCTP_RECONFIG_SUPPORTED_OPT,
            SctpAssocValueRaw {
                assoc_id: 0,
                assoc_value: 0,
            },
        )?;

        Ok(SctpReconfigFlags {
            assoc_id: raw.assoc_id,
            flags: raw.assoc_value,
        })
    }

    /// Enables SCTP stream/association reconfiguration capabilities on this association.
    ///
    /// This is capability-dependent and may fail if SCTP reconfiguration is
    /// disabled by the kernel or by association policy. It is control-plane
    /// work, not the per-message data fast path.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::{SctpReconfigFlags, SctpStream};
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// stream.enable_stream_reset(SctpReconfigFlags {
    ///     flags: SctpReconfigFlags::RESET_STREAMS | SctpReconfigFlags::CHANGE_ASSOC,
    ///     ..SctpReconfigFlags::association_default()
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn enable_stream_reset(&self, flags: SctpReconfigFlags) -> io::Result<()> {
        let raw = SctpAssocValueRaw {
            assoc_id: flags.assoc_id,
            assoc_value: flags.flags,
        };
        set_sock_opt(
            self.fd.raw_fd(),
            libc::IPPROTO_SCTP,
            SCTP_ENABLE_STREAM_RESET_OPT,
            &raw,
        )
    }

    /// Requests a stream reset for listed streams or an explicit all-stream
    /// selection.
    ///
    /// This requires SCTP stream-reset support and appropriate association
    /// capabilities on the running kernel. It is control-plane work, not the
    /// per-message data fast path. Listed requests must contain at least one
    /// stream identifier; use an `all_*` constructor for all streams.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when a listed request is empty, an explicit
    /// all-stream request carries stream identifiers, or the list exceeds the
    /// kernel request field width.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::{SctpResetStreams, SctpStream};
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// stream.reset_streams(&SctpResetStreams::outgoing(&[1]))?;
    /// stream.reset_streams(&SctpResetStreams::all_incoming())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset_streams(&self, request: &SctpResetStreams) -> io::Result<()> {
        let buffer = encode_sctp_reset_streams(request)?;

        let rc = unsafe {
            libc::setsockopt(
                self.fd.raw_fd(),
                libc::IPPROTO_SCTP,
                SCTP_RESET_STREAMS_OPT,
                buffer.as_ptr() as *const libc::c_void,
                buffer.len() as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Requests additional inbound and outbound SCTP streams on this association.
    ///
    /// This requires SCTP association reconfiguration support and may be
    /// rejected by the kernel even when baseline SCTP messaging works. It is
    /// control-plane work, not the per-message data fast path.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::{SctpAddStreams, SctpStream};
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// stream.add_streams(SctpAddStreams::new(1, 1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_streams(&self, request: SctpAddStreams) -> io::Result<()> {
        let raw = SctpAddStreamsRaw {
            assoc_id: request.assoc_id,
            inbound_streams: request.inbound_streams,
            outbound_streams: request.outbound_streams,
        };
        set_sock_opt(
            self.fd.raw_fd(),
            libc::IPPROTO_SCTP,
            SCTP_ADD_STREAMS_OPT,
            &raw,
        )
    }

    /// Reads `SCTP_PEER_ADDR_PARAMS` for the whole association or for one specific path.
    ///
    /// On supported x86-64 Linux, the kernel response must use exactly the
    /// 152-byte legacy or 156-byte modern rounded socket-option length. Any
    /// other response length is returned as [`io::ErrorKind::InvalidData`].
    ///
    /// This is path/association status-control work, not the per-message data
    /// fast path.
    pub fn peer_addr_params(&self, address: Option<SocketAddr>) -> io::Result<SctpPeerAddrParams> {
        let mut buffer = [0u8; SCTP_PADDR_PARAMS_RAW_OPT_LEN];
        let request = SctpPaddrParamsRaw::from_fields(SctpPaddrParamsFields::from_public(
            SctpPeerAddrParams {
                address,
                ..SctpPeerAddrParams::association_default()
            },
        ));
        unsafe {
            std::ptr::copy_nonoverlapping(
                &request as *const SctpPaddrParamsRaw as *const u8,
                buffer.as_mut_ptr(),
                std::mem::size_of::<SctpPaddrParamsRaw>(),
            );
        }
        let mut optlen = buffer.len() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                self.fd.raw_fd(),
                libc::IPPROTO_SCTP,
                libc::SCTP_PEER_ADDR_PARAMS,
                buffer.as_mut_ptr() as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        decode_peer_addr_params_sockopt(&buffer, optlen as usize)
    }

    /// Applies `SCTP_PEER_ADDR_PARAMS` to the whole association or to one
    /// specific path.
    ///
    /// Use this for path-specific overrides. For guarded association-wide
    /// defaults, use [`SctpStream::set_default_peer_addr_params`].
    ///
    /// This is path/association configuration work. Apply it during setup or
    /// reconfiguration, not per message.
    pub fn set_peer_addr_params(&self, params: SctpPeerAddrParams) -> io::Result<()> {
        apply_peer_addr_params_raw(self.fd.raw_fd(), params)
    }

    /// Chooses which peer address this association sends to by default.
    ///
    /// This is a local-only primary-destination choice; it sends no request to
    /// the peer. To ask the peer to send to a particular local address, use
    /// [`SctpStream::request_peer_use_local_addr`]. This is path control-plane
    /// work, not the per-message data fast path.
    pub fn set_primary_dest_addr(&self, peer_addr: SocketAddr) -> io::Result<()> {
        let raw = SctpPrimRaw {
            assoc_id: 0,
            addr: option_socket_addr_to_storage(Some(peer_addr)),
        };
        set_sock_opt(
            self.fd.raw_fd(),
            libc::IPPROTO_SCTP,
            libc::SCTP_PRIMARY_ADDR,
            &raw,
        )
    }

    /// Requests that the peer send to the provided local address by default.
    ///
    /// Unlike [`SctpStream::set_primary_dest_addr`], this sends a wire request
    /// to the peer. Some Linux SCTP deployments reject it with `EPERM`/`EACCES`
    /// when dynamic address reconfiguration is disabled by kernel policy or
    /// association capabilities. This is path control-plane work, not the
    /// per-message data fast path.
    pub fn request_peer_use_local_addr(&self, local_addr: SocketAddr) -> io::Result<()> {
        let raw = SctpSetPeerPrimRaw {
            assoc_id: 0,
            addr: option_socket_addr_to_storage(Some(local_addr)),
        };
        set_sock_opt(
            self.fd.raw_fd(),
            libc::IPPROTO_SCTP,
            libc::SCTP_SET_PEER_PRIMARY_ADDR,
            &raw,
        )
    }

    /// Applies default SCTP send metadata to this socket. This is used by the
    /// fast-path [`SctpStream::send`] API.
    ///
    /// Apply this during setup when the metadata is stable; avoid changing it
    /// per message on the data fast path.
    pub fn set_default_send_info(&self, info: SctpSendInfo) -> io::Result<()> {
        set_sock_opt(
            self.fd.raw_fd(),
            libc::IPPROTO_SCTP,
            libc::SCTP_DEFAULT_SNDINFO,
            &raw_sndinfo_from_public(info),
        )
    }

    /// Applies a typed SCTP notification subscription mask.
    ///
    /// If the socket currently has `SCTP_RECVRCVINFO` enabled, the effective
    /// kernel mask retains the partial-delivery event even when
    /// `mask.partial_delivery` is false. Abort events identifiable as forced
    /// by that dependency are consumed as internal metadata-receive recovery;
    /// setting the field to true keeps complete abort notifications
    /// caller-visible. If other notification types are also requested, a
    /// caller buffer too short to identify a fragmented notification retains
    /// the normal truncated-notification error behavior. After the kernel
    /// accepts the new mask, the stream also records the observed
    /// `SCTP_RECVRCVINFO` setting so later metadata receives apply the matching
    /// strict-or-default ancillary policy.
    ///
    /// This is signaling setup/control-plane work. Data-only fast paths should
    /// use [`SctpSocketConfig::data`] and avoid per-message mask changes.
    pub fn set_notification_mask(&self, mask: SctpNotificationMask) -> io::Result<()> {
        self.set_notification_mask_with(mask, |effective| {
            set_sctp_events(self.fd.raw_fd(), effective)
        })
    }

    /// Queries the live receive-info setting and applies a request-scoped
    /// notification-mask operation before publishing the matching policy.
    fn set_notification_mask_with<F>(
        &self,
        mask: SctpNotificationMask,
        apply_events: F,
    ) -> io::Result<()>
    where
        F: FnOnce(SctpNotificationMask) -> io::Result<()>,
    {
        let recv_rcvinfo: libc::c_int =
            get_sock_opt(self.fd.raw_fd(), libc::IPPROTO_SCTP, libc::SCTP_RECVRCVINFO)?;
        let recv_rcvinfo_requested = recv_rcvinfo != 0;
        apply_sctp_notification_mask(&self.recv_state, mask, recv_rcvinfo_requested, apply_events)
    }

    /// Applies association-wide retransmission and RTO policy.
    ///
    /// This is association configuration work, not the per-message data fast
    /// path.
    pub fn apply_assoc_config(&self, config: &SctpAssocConfig) -> io::Result<()> {
        apply_assoc_config_raw(self.fd.raw_fd(), *config)
    }

    /// Applies association-wide peer-address defaults.
    ///
    /// `params.address` must be `None`; use [`SctpStream::set_peer_addr_params`]
    /// for path-specific overrides instead.
    ///
    /// This is association configuration work, not the per-message data fast
    /// path.
    pub fn set_default_peer_addr_params(&self, params: SctpPeerAddrParams) -> io::Result<()> {
        apply_default_peer_addr_params(self.fd.raw_fd(), params)
    }

    /// Starts one connected data receive on the fast path.
    ///
    /// This path is intended for sockets configured without SCTP
    /// notifications and without `SCTP_RCVINFO`. Its result carries only the
    /// received byte count; the rental buffer is returned beside that result.
    /// It does not expose `MSG_EOR` or truncation flags, so use it only when
    /// application framing guarantees the supplied buffer is large enough.
    /// Use [`SctpStream::recv_msg`] when record-boundary or truncation
    /// correctness depends on kernel metadata.
    /// Positive progress appends to an `IoBuffMut` payload; buffers that keep
    /// the provided zero write base publish from their beginning. A kernel
    /// zero-byte completion is clean peer EOF and preserves existing logical
    /// contents; the returned count is relative to this receive. Zero-length
    /// caller requests are rejected before submission so they cannot
    /// masquerade as EOF.
    /// This data-only path does not drive metadata receive resynchronization.
    /// If rich receive is discarding a record tail, or if a dropped
    /// `recv_msg` / `recv_msg_vectored` operation occupies the stream-owned
    /// recovery slot, this method rejects the request without changing that
    /// recovery state or submitting another receive.
    ///
    /// # Errors
    /// Returns `InvalidInput` if `len` is zero, exceeds
    /// `buffer.writable_len()`, or rich receive recovery is pending. Local
    /// length validation retains precedence;
    /// all three cases return the exact buffer after owner-context validation
    /// and before operation allocation, buffer-pointer access, or submission.
    /// Kernel receive errors are returned as `io::Error` values from the
    /// completed operation.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn recv<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> DataRecvFuture<'_, B> {
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match checked_sctp_recv_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        if input_error.is_none() && self.recv_state.pending_metadata_recovery() {
            input_error = Some(invalid_input_kind());
        }
        let state_ptr = self.fd.op_state();
        let fd = state_ptr.raw_fd();
        DataRecvFuture {
            fd,
            state_ptr,
            buffer: Some(buffer),
            write_base_len,
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one connected data send on the fast path.
    ///
    /// Per-message metadata comes from the socket's default send info, if any.
    ///
    /// # Errors
    /// Returns `InvalidInput` if the buffer has no readable bytes or its length
    /// exceeds the kernel send-count width.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send<B: IoBuffReadOnly>(&mut self, buffer: B) -> DataSendFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_sctp_send_len(buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        let state_ptr = self.fd.op_state();
        let fd = state_ptr.raw_fd();
        DataSendFuture {
            fd,
            state_ptr,
            buffer: Some(buffer),
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one message-oriented receive with SCTP metadata.
    ///
    /// This is the metadata/notification receive path. For data-only
    /// associations where ancillary metadata and notifications are disabled,
    /// prefer [`SctpStream::recv`] only when EOR and truncation reporting are
    /// also unnecessary.
    ///
    /// # Errors
    /// Returns `InvalidInput` if `len` is zero or exceeds
    /// `buffer.writable_len()`. After owner-context validation, this local
    /// error returns the exact buffer before adopting a prior dropped metadata
    /// receive; that stash remains for the next valid request.
    /// A ring-abandoned dropped receive is terminal: the detecting request and
    /// every later metadata receive return `NotConnected` without submitting
    /// the new caller buffer, and stream drop clears only the opaque local
    /// marker.
    /// Shared metadata parsing, truncation, EOF, and record-tail recovery
    /// behavior is documented on [`SctpStream`].
    ///
    /// Positive delivered bytes append to an `IoBuffMut` payload; buffers that
    /// keep the provided zero write base publish from their beginning. Bytes
    /// copied before a metadata/truncation error remain published. Clean EOF,
    /// no-progress errors, and internally discarded record tails preserve the
    /// caller-visible prefix. Returned byte counts remain relative.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn recv_msg<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvFuture<'_, B> {
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match checked_sctp_recv_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        let state_ptr = self.fd.op_state();
        let fd = state_ptr.raw_fd();
        RecvFuture {
            fd,
            state_ptr,
            buffer: Some(buffer),
            write_base_len,
            len,
            input_error,
            recv_state: &mut self.recv_state,
            _marker: PhantomData,
        }
    }

    /// Starts one message-oriented send with explicit SCTP metadata.
    ///
    /// This is the explicit-metadata send path. If the metadata is stable for
    /// the association, install it once with [`SctpStream::set_default_send_info`]
    /// and prefer [`SctpStream::send`] on the data fast path.
    ///
    /// # Errors
    /// Returns `InvalidInput` if the buffer has no readable bytes or its length
    /// exceeds the kernel send-count width.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send_msg<B: IoBuffReadOnly>(
        &mut self,
        buffer: B,
        info: SctpSendInfo,
    ) -> SendFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_sctp_send_len(buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        let state_ptr = self.fd.op_state();
        let fd = state_ptr.raw_fd();
        SendFuture {
            fd,
            state_ptr,
            buffer: Some(buffer),
            len,
            sndinfo: raw_sndinfo_from_public(info),
            input_error,
            _marker: PhantomData,
        }
    }

    /// Scatter-receive into a vectored buffer chain with SCTP metadata.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  On success, returns the total bytes received and
    /// per-message metadata (stream id, PPID, etc.) or a notification.
    ///
    /// Notification data must fit within the first writable segment of the
    /// chain; zero-length destinations are not submitted to the kernel.
    /// Use this when both segmentation and SCTP metadata/notifications matter.
    /// For a single contiguous data-only receive, prefer [`SctpStream::recv`]
    /// only when EOR and truncation reporting are unnecessary.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if the chain has no writable
    /// bytes, has more than 1,024 active writable segments, its aggregate
    /// writable byte count cannot be represented by `usize`, iovec
    /// materialization fails, or the materialized active writable-segment
    /// count or byte total differs from the construction-time snapshot. After
    /// owner-context validation, an empty, zero-writable, or over-limit chain
    /// returns unchanged before adopting a prior dropped metadata receive;
    /// that stash remains for the next valid request. Materialization and shape
    /// failures return the exact chain without submitting kernel I/O. Shared
    /// metadata requests remain terminally `NotConnected` after a dropped
    /// receive's origin ring is abandoned; they return the exact unsubmitted
    /// chain until stream destruction clears the opaque local marker. Shared
    /// metadata parsing, truncation, EOF, and record-tail recovery behavior is
    /// documented on [`SctpStream`].
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn recv_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
    ) -> RecvVectoredFuture<'_, N> {
        let (iov_count, writable, invalid_aggregate) =
            match buffer.checked_read_iovec_count_and_writable_len() {
                Some((iov_count, writable)) => (iov_count, writable, false),
                None => (0, 0, true),
            };
        let state_ptr = self.fd.op_state();
        let fd = state_ptr.raw_fd();
        RecvVectoredFuture {
            fd,
            state_ptr,
            buffer: Some(buffer),
            iov_count,
            writable,
            invalid_aggregate,
            recv_state: &mut self.recv_state,
            _marker: PhantomData,
        }
    }

    /// Gather-send from a vectored buffer chain with SCTP metadata.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern). The total number of bytes sent is returned in `Ok`. Empty and
    /// zero-readable chains are rejected without submitting kernel I/O.
    /// Use this when both segmentation and explicit SCTP metadata matter. For
    /// a single contiguous data-only send, prefer [`SctpStream::send`].
    ///
    /// # Errors
    /// Returns `InvalidInput` if the chain has no readable bytes, has more than
    /// 1,024 active readable segments, or its aggregate readable byte count
    /// cannot be represented by `usize`. Validation returns the exact chain
    /// before retained allocation or kernel submission.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
        info: SctpSendInfo,
    ) -> SendVectoredFuture<'_, N> {
        let (iov_count, total, invalid_aggregate) =
            match checked_iobuffvec_write_iovec_count_and_len(&buffer) {
                Some((iov_count, total)) => (iov_count, total, false),
                None => (0, 0, true),
            };
        let state_ptr = self.fd.op_state();
        let fd = state_ptr.raw_fd();
        SendVectoredFuture {
            fd,
            state_ptr,
            buffer: Some(buffer),
            iov_count,
            total,
            invalid_aggregate,
            sndinfo: raw_sndinfo_from_public(info),
            _marker: PhantomData,
        }
    }
}

impl AsRawFd for SctpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.expose_raw_fd()
    }
}

impl Drop for SctpStream {
    fn drop(&mut self) {
        unsafe { self.recv_state.drop_stashed() };
    }
}

use super::opt_take;

struct RetainedDataRecvPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while data-only recv is live.
    buffer: B,
}

struct RetainedDataSendPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while data-only send is live.
    buffer: B,
}

// These private C-layout types mirror the widest Linux UAPI payloads covered by
// the fixed rich-receive ancillary budget. Use the time64 layouts explicitly:
// `libc::timeval` and `libc::timespec` can still expose narrower legacy
// layouts on a 32-bit target even though the corresponding `_NEW` socket
// options return two signed 64-bit fields.
#[repr(C)]
struct LinuxKernelSockTimeval {
    tv_sec: i64,
    tv_usec: i64,
}

#[repr(C)]
struct LinuxKernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

#[repr(C)]
struct LinuxScmTimestamping {
    ts: [LinuxKernelTimespec; 3],
}

#[repr(C)]
struct LinuxScmTimestampingPktInfo {
    if_index: u32,
    pkt_length: u32,
    reserved: [u32; 2],
}

#[repr(transparent)]
struct LinuxSocketRxqOverflow(u32);

// Linux emits generic SOL_SOCKET receive metadata before SCTP_RCVINFO. Keep a
// fixed, ABI-derived budget for the common observability combination FlowIO
// supports without making per-message control storage dynamic:
//
// - one largest basic timestamp/time-NS value;
// - one largest three-value SCM_TIMESTAMPING payload;
// - SCM_TIMESTAMPING_PKTINFO;
// - SO_RXQ_OVFL; and
// - SCTP_RCVINFO.
//
// Other well-formed records are skipped when they fit. Combinations involving
// additional external descriptor policy such as SO_MARK, SO_PRIORITY,
// SCM_WIFI_STATUS, or SCTP_NXTINFO can exhaust this bound and then fail closed
// via MSG_CTRUNC.
const SOCKET_TIMESTAMP_PAYLOAD_LEN: usize = {
    let timeval_len = std::mem::size_of::<LinuxKernelSockTimeval>();
    let timespec_len = std::mem::size_of::<LinuxKernelTimespec>();
    if timeval_len > timespec_len {
        timeval_len
    } else {
        timespec_len
    }
};
const SOCKET_TIMESTAMP_CONTROL_LEN: usize = cmsg_space(SOCKET_TIMESTAMP_PAYLOAD_LEN);
const SOCKET_TIMESTAMPING_CONTROL_LEN: usize =
    cmsg_space(std::mem::size_of::<LinuxScmTimestamping>());
const SOCKET_TIMESTAMPING_PKTINFO_CONTROL_LEN: usize =
    cmsg_space(std::mem::size_of::<LinuxScmTimestampingPktInfo>());
const SOCKET_RXQ_OVFL_CONTROL_LEN: usize =
    cmsg_space(std::mem::size_of::<LinuxSocketRxqOverflow>());
const SCTP_RCVINFO_CONTROL_LEN: usize = cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>());
const SCTP_RECV_CONTROL_LEN: usize = SOCKET_TIMESTAMP_CONTROL_LEN
    + SOCKET_TIMESTAMPING_CONTROL_LEN
    + SOCKET_TIMESTAMPING_PKTINFO_CONTROL_LEN
    + SOCKET_RXQ_OVFL_CONTROL_LEN
    + SCTP_RCVINFO_CONTROL_LEN;

const _: () = {
    assert!(std::mem::size_of::<LinuxKernelSockTimeval>() == 2 * std::mem::size_of::<i64>());
    assert!(std::mem::align_of::<LinuxKernelSockTimeval>() == std::mem::align_of::<i64>());
    assert!(std::mem::offset_of!(LinuxKernelSockTimeval, tv_sec) == 0);
    assert!(std::mem::offset_of!(LinuxKernelSockTimeval, tv_usec) == 8);
    assert!(std::mem::size_of::<LinuxKernelTimespec>() == 2 * std::mem::size_of::<i64>());
    assert!(std::mem::align_of::<LinuxKernelTimespec>() == std::mem::align_of::<i64>());
    assert!(std::mem::offset_of!(LinuxKernelTimespec, tv_sec) == 0);
    assert!(std::mem::offset_of!(LinuxKernelTimespec, tv_nsec) == 8);
    assert!(SOCKET_TIMESTAMP_CONTROL_LEN >= cmsg_space(std::mem::size_of::<libc::timeval>()));
    assert!(SOCKET_TIMESTAMP_CONTROL_LEN >= cmsg_space(std::mem::size_of::<libc::timespec>()));

    assert!(
        std::mem::size_of::<LinuxScmTimestamping>()
            == 3 * std::mem::size_of::<LinuxKernelTimespec>()
    );
    assert!(
        std::mem::align_of::<LinuxScmTimestamping>() == std::mem::align_of::<LinuxKernelTimespec>()
    );
    assert!(std::mem::offset_of!(LinuxScmTimestamping, ts) == 0);

    assert!(std::mem::size_of::<LinuxScmTimestampingPktInfo>() == 4 * std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<LinuxScmTimestampingPktInfo>() == std::mem::align_of::<u32>());
    assert!(std::mem::offset_of!(LinuxScmTimestampingPktInfo, if_index) == 0);
    assert!(std::mem::offset_of!(LinuxScmTimestampingPktInfo, pkt_length) == 4);
    assert!(std::mem::offset_of!(LinuxScmTimestampingPktInfo, reserved) == 8);

    assert!(std::mem::size_of::<LinuxSocketRxqOverflow>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<LinuxSocketRxqOverflow>() == std::mem::align_of::<u32>());
    assert!(std::mem::offset_of!(LinuxSocketRxqOverflow, 0) == 0);
};

#[cfg(all(target_arch = "x86_64", target_pointer_width = "64"))]
const _: () = {
    // Linux UAPI's three leading u16 fields and libc's C-layout mirror leave
    // implicit bytes 6-7 before the first u32 field.
    assert!(std::mem::size_of::<libc::sctp_rcvinfo>() == 28);
    assert!(std::mem::align_of::<libc::sctp_rcvinfo>() == 4);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_sid) == 0);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_ssn) == 2);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_flags) == 4);
    assert!(
        std::mem::offset_of!(libc::sctp_rcvinfo, rcv_ppid)
            == std::mem::offset_of!(libc::sctp_rcvinfo, rcv_flags) + std::mem::size_of::<u16>() + 2
    );
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_ppid) == 8);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_tsn) == 12);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_cumtsn) == 16);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_context) == 20);
    assert!(std::mem::offset_of!(libc::sctp_rcvinfo, rcv_assoc_id) == 24);

    // Linux exposes the packed peer-path parameter payload through a sockopt
    // length rounded externally to four bytes. `packed(4)` would incorrectly
    // move the unaligned `path_mtu` field from byte 138 to byte 140.
    assert!(std::mem::size_of::<SctpPaddrParamsRaw>() == 155);
    assert!(std::mem::align_of::<SctpPaddrParamsRaw>() == 1);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, assoc_id) == 0);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, address) == 4);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, heartbeat_interval_ms) == 132);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, path_max_retransmits) == 136);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, path_mtu) == 138);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, sack_delay_ms) == 142);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, flags) == 146);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, ipv6_flow_label) == 150);
    assert!(std::mem::offset_of!(SctpPaddrParamsRaw, dscp) == 154);
    assert!(SCTP_PADDR_PARAMS_RAW_OPT_LEN == 156);

    assert!(std::mem::size_of::<SctpPaddrParamsRawLegacy>() == 150);
    assert!(std::mem::align_of::<SctpPaddrParamsRawLegacy>() == 1);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, assoc_id) == 0);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, address) == 4);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, heartbeat_interval_ms) == 132);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, path_max_retransmits) == 136);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, path_mtu) == 138);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, sack_delay_ms) == 142);
    assert!(std::mem::offset_of!(SctpPaddrParamsRawLegacy, flags) == 146);
    assert!(SCTP_PADDR_PARAMS_LEGACY_OPT_LEN == 152);

    assert!(SOCKET_TIMESTAMP_CONTROL_LEN == 32);
    assert!(SOCKET_TIMESTAMPING_CONTROL_LEN == 64);
    assert!(SOCKET_TIMESTAMPING_PKTINFO_CONTROL_LEN == 32);
    assert!(SOCKET_RXQ_OVFL_CONTROL_LEN == 24);
    assert!(SCTP_RCVINFO_CONTROL_LEN == 48);
    assert!(SCTP_RECV_CONTROL_LEN == 200);
};

// These retained recvmsg/sendmsg payloads become self-referential after their
// msghdr points at embedded iovec and control storage. Connected one-to-one
// receive paths do not request a per-message peer address because the stream
// already owns that association identity and no public result consumes it.
// Initialize embedded pointers only in their stable retained destination
// through the raw-slot constructors below.
struct RetainedSctpRecvPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while recvmsg is live.
    buffer: B,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Kernel-initialized control-message prefix for SCTP receive metadata.
    control: [MaybeUninit<u8>; SCTP_RECV_CONTROL_LEN],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

/// Constructs the rich scalar receive payload directly in retained storage.
///
/// All callback-capable work runs while the slot is raw and the caller still
/// owns `buffer`. The ownership transfer is the final operation before the
/// writing slot is finished, so an unexpected unwind cannot recycle partially
/// initialized ownership as a live payload.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `buffer` must contain one value, and the returned payload must be
/// attached to a state owned by that same reactor or consumed through `pool`.
/// `len` must not exceed that buffer's writable length.
#[inline(always)]
unsafe fn emplace_retained_sctp_recv_payload<B: IoBuffReadWrite>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<B>,
    len: u32,
) -> RetainedPayload<RetainedSctpRecvPayload<B>> {
    unsafe {
        with_raw_retained_slot::<RetainedSctpRecvPayload<B>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();

            // The operation state is allocated before this callback runs. The
            // callback receives only the still-future-owned buffer and may
            // synchronously re-enter this same retained pool.
            let buffer_ptr = buffer.as_mut().unwrap_unchecked().as_mut_ptr();
            std::ptr::addr_of_mut!((*dst).iovec).write(MaybeUninit::new(libc::iovec {
                iov_base: buffer_ptr as *mut libc::c_void,
                iov_len: len as usize,
            }));

            write_msghdr(
                &mut *std::ptr::addr_of_mut!((*dst).msghdr),
                MsgHdrInit {
                    name: std::ptr::null_mut(),
                    namelen: 0,
                    iov: std::ptr::addr_of_mut!((*dst).iovec).cast::<libc::iovec>(),
                    iovlen: 1,
                    control: std::ptr::addr_of_mut!((*dst).control)
                        .cast::<u8>()
                        .cast::<libc::c_void>(),
                    controllen: SCTP_RECV_CONTROL_LEN,
                },
            );

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            let source = buffer.as_mut().unwrap_unchecked() as *mut B;
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            writing.finish()
        })
    }
}

struct RetainedSctpSendPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while sendmsg is live.
    buffer: B,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Control-message storage for SCTP send metadata.
    control: [u8; SCTP_SNDINFO_CONTROL_LEN],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

const SCTP_SNDINFO_HEADER_LEN: usize = std::mem::size_of::<libc::cmsghdr>();
const SCTP_SNDINFO_DATA_OFFSET: usize = cmsg_align(SCTP_SNDINFO_HEADER_LEN);
const SCTP_SNDINFO_DATA_LEN: usize = std::mem::size_of::<libc::sctp_sndinfo>();
const SCTP_SNDINFO_CMSG_LEN: usize = SCTP_SNDINFO_HEADER_LEN + SCTP_SNDINFO_DATA_LEN;
const SCTP_SNDINFO_CONTROL_LEN: usize = cmsg_space(SCTP_SNDINFO_DATA_LEN);

#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    target_pointer_width = "64"
))]
const _: () = {
    assert!(std::mem::size_of::<libc::cmsghdr>() == 16);
    assert!(std::mem::align_of::<libc::cmsghdr>() == 8);
    assert!(std::mem::offset_of!(libc::cmsghdr, cmsg_len) == 0);
    assert!(std::mem::offset_of!(libc::cmsghdr, cmsg_level) == 8);
    assert!(std::mem::offset_of!(libc::cmsghdr, cmsg_type) == 12);

    assert!(std::mem::size_of::<libc::sctp_sndinfo>() == 16);
    assert!(std::mem::align_of::<libc::sctp_sndinfo>() == 4);
    assert!(std::mem::offset_of!(libc::sctp_sndinfo, snd_sid) == 0);
    assert!(std::mem::offset_of!(libc::sctp_sndinfo, snd_flags) == 2);
    assert!(std::mem::offset_of!(libc::sctp_sndinfo, snd_ppid) == 4);
    assert!(std::mem::offset_of!(libc::sctp_sndinfo, snd_context) == 8);
    assert!(std::mem::offset_of!(libc::sctp_sndinfo, snd_assoc_id) == 12);

    assert!(SCTP_SNDINFO_HEADER_LEN == 16);
    assert!(SCTP_SNDINFO_DATA_OFFSET == 16);
    assert!(SCTP_SNDINFO_DATA_LEN == 16);
    assert!(SCTP_SNDINFO_CMSG_LEN == 32);
    assert!(SCTP_SNDINFO_CONTROL_LEN == 32);
    assert!(SCTP_SNDINFO_DATA_OFFSET + SCTP_SNDINFO_DATA_LEN == SCTP_SNDINFO_CONTROL_LEN);
    assert!(SCTP_SNDINFO_DATA_OFFSET + std::mem::offset_of!(libc::sctp_sndinfo, snd_sid) == 16);
    assert!(SCTP_SNDINFO_DATA_OFFSET + std::mem::offset_of!(libc::sctp_sndinfo, snd_flags) == 18);
    assert!(SCTP_SNDINFO_DATA_OFFSET + std::mem::offset_of!(libc::sctp_sndinfo, snd_ppid) == 20);
    assert!(SCTP_SNDINFO_DATA_OFFSET + std::mem::offset_of!(libc::sctp_sndinfo, snd_context) == 24);
    assert!(
        SCTP_SNDINFO_DATA_OFFSET + std::mem::offset_of!(libc::sctp_sndinfo, snd_assoc_id) == 28
    );
};

/// Initializes the non-owning fields shared by retained SCTP send payloads.
///
/// # Safety
///
/// `msghdr` and `control` must point into one raw retained slot at their final
/// addresses, be properly aligned and writable for their complete field
/// sizes, and not overlap each other or the initialized iovec prefix. This
/// function initializes the complete control array before forming its mutable
/// reference. `iov` must point at `iovlen` initialized iovecs whose backing
/// allocations remain stable until the target CQE retires.
#[inline(always)]
unsafe fn init_retained_sctp_send_fields(
    msghdr: *mut MaybeUninit<libc::msghdr>,
    control: *mut [u8; SCTP_SNDINFO_CONTROL_LEN],
    iov: *mut libc::iovec,
    iovlen: usize,
    sndinfo: libc::sctp_sndinfo,
) {
    unsafe {
        // The retained slot starts uninitialized. Initialize every byte before
        // creating `&mut [u8; SCTP_SNDINFO_CONTROL_LEN]`; optimized builds may
        // eliminate these stores after seeing the two complete writes below.
        std::ptr::write_bytes(control, 0, 1);
        write_msghdr(
            &mut *msghdr,
            MsgHdrInit {
                name: std::ptr::null_mut(),
                namelen: 0,
                iov,
                iovlen,
                control: control.cast::<u8>().cast::<libc::c_void>(),
                controllen: SCTP_SNDINFO_CONTROL_LEN,
            },
        );
        write_cmsg_sndinfo(&mut *control, sndinfo);
    }
}

/// Constructs a rich scalar send payload directly in retained storage.
///
/// Pointer extraction and all metadata construction happen while `buffer`
/// remains future-owned. The buffer is moved only after every non-owning field
/// is initialized at its final address.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `buffer` must contain one value, `len` must not exceed its readable
/// length, and the returned payload must be attached to a state owned by that
/// same reactor or consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_sctp_send_payload<B: IoBuffReadOnly>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<B>,
    len: u32,
    sndinfo: libc::sctp_sndinfo,
) -> RetainedPayload<RetainedSctpSendPayload<B>> {
    unsafe {
        with_raw_retained_slot::<RetainedSctpSendPayload<B>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            let buffer_ptr = buffer.as_ref().unwrap_unchecked().as_ptr();
            std::ptr::addr_of_mut!((*dst).iovec).write(MaybeUninit::new(libc::iovec {
                iov_base: buffer_ptr as *mut libc::c_void,
                iov_len: len as usize,
            }));
            init_retained_sctp_send_fields(
                std::ptr::addr_of_mut!((*dst).msghdr),
                std::ptr::addr_of_mut!((*dst).control),
                std::ptr::addr_of_mut!((*dst).iovec).cast::<libc::iovec>(),
                1,
                sndinfo,
            );

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            let source = buffer.as_mut().unwrap_unchecked() as *mut B;
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            writing.finish()
        })
    }
}

struct RetainedSctpRecvVectoredPayload<const N: usize> {
    /// Caller-owned destination chain retained while recvmsg is live.
    buffer: IoBuffVecMut<N>,
    /// Kernel-facing iovec array pointing into `buffer` segments.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Kernel-initialized control-message prefix for SCTP receive metadata.
    control: [MaybeUninit<u8>; SCTP_RECV_CONTROL_LEN],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

/// Constructs a rich vectored receive payload directly in retained storage.
///
/// The embedded iovecs and message header are materialized at their final
/// addresses while the chain remains future-owned. Moving the chain handle
/// last preserves every segment allocation address under `IoBuffMut`'s move
/// stability, without copying the `N`-entry iovec array through the future or
/// stack.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `buffer` must contain one chain and `expected_shape` must be its
/// checked `(nonempty writable-segment count, total writable length)` snapshot.
/// A mismatch returns `InvalidInput` before the chain moves out of `buffer`. A
/// returned payload must be attached to a state owned by that same reactor or
/// consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_sctp_recv_vectored_payload<const N: usize>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<IoBuffVecMut<N>>,
    expected_shape: (usize, usize),
) -> io::Result<RetainedPayload<RetainedSctpRecvVectoredPayload<N>>> {
    unsafe {
        with_raw_retained_slot::<RetainedSctpRecvVectoredPayload<N>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            let iovecs = &mut *std::ptr::addr_of_mut!((*dst).iovecs);
            let expected_iovecs = iovecs
                .get_mut(..expected_shape.0)
                .ok_or_else(invalid_read_iovec_shape)?;
            let materialized_shape =
                fill_recv_vectored_iovecs(buffer.as_mut().unwrap_unchecked(), expected_iovecs)?;
            if materialized_shape != expected_shape {
                return Err(invalid_read_iovec_shape());
            }

            write_msghdr(
                &mut *std::ptr::addr_of_mut!((*dst).msghdr),
                MsgHdrInit {
                    name: std::ptr::null_mut(),
                    namelen: 0,
                    iov: std::ptr::addr_of_mut!((*dst).iovecs).cast::<libc::iovec>(),
                    iovlen: materialized_shape.0,
                    control: std::ptr::addr_of_mut!((*dst).control)
                        .cast::<u8>()
                        .cast::<libc::c_void>(),
                    controllen: SCTP_RECV_CONTROL_LEN,
                },
            );

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            let source = buffer.as_mut().unwrap_unchecked() as *mut IoBuffVecMut<N>;
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            Ok(writing.finish())
        })
    }
}

struct RetainedSctpSendVectoredPayload<const N: usize> {
    /// Caller-owned source chain retained while sendmsg is live.
    buffer: IoBuffVec<N>,
    /// Kernel-facing iovec array pointing into `buffer` segments.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Control-message storage for SCTP send metadata.
    control: [u8; SCTP_SNDINFO_CONTROL_LEN],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

/// Constructs a rich vectored send payload directly in retained storage.
///
/// The compacted iovec prefix and shared send metadata are materialized at
/// their final addresses while the chain remains future-owned. The chain moves
/// only after those non-owning fields are complete.
///
/// # Safety
///
/// `pool` must identify the live retained pool for the active owner-thread
/// reactor. `buffer` must contain a chain with at least one readable byte, and
/// `expected_shape` must be its checked `(nonempty segment count, total readable
/// length)` snapshot. The returned payload must be attached to a state owned by
/// that same reactor or consumed through `pool`. If checked iovec
/// materialization fails or its shape differs from the snapshot, `buffer`
/// remains untouched and no ownership is transferred into the raw slot.
#[inline(always)]
unsafe fn emplace_retained_sctp_send_vectored_payload<const N: usize>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<IoBuffVec<N>>,
    expected_shape: (usize, usize),
    sndinfo: libc::sctp_sndinfo,
) -> io::Result<RetainedPayload<RetainedSctpSendVectoredPayload<N>>> {
    unsafe {
        with_raw_retained_slot::<RetainedSctpSendVectoredPayload<N>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            let source = buffer.as_ref().unwrap_unchecked();
            let materialized_shape =
                fill_iobuffvec_write_iovecs(source, &mut *std::ptr::addr_of_mut!((*dst).iovecs))?;
            if materialized_shape != expected_shape {
                return Err(invalid_writev_shape());
            }
            init_retained_sctp_send_fields(
                std::ptr::addr_of_mut!((*dst).msghdr),
                std::ptr::addr_of_mut!((*dst).control),
                std::ptr::addr_of_mut!((*dst).iovecs).cast::<libc::iovec>(),
                materialized_shape.0,
                sndinfo,
            );

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            let source = buffer.as_mut().unwrap_unchecked() as *mut IoBuffVec<N>;
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            Ok(writing.finish())
        })
    }
}

#[derive(Clone, Copy)]
struct SctpRecvHeader {
    msg_controllen: usize,
    msg_flags: libc::c_int,
}

impl SctpRecvHeader {
    #[cfg(test)]
    fn from_msghdr(msg: &libc::msghdr) -> Self {
        Self {
            msg_controllen: msg.msg_controllen,
            msg_flags: msg.msg_flags,
        }
    }
}

struct SctpRecvCompletionMeta {
    header: SctpRecvHeader,
    rcvinfo: io::Result<Option<SctpRecvInfo>>,
}

struct SctpRecvCompletion<B> {
    meta: SctpRecvCompletionMeta,
    buffer: B,
}

struct SctpFirstIovec {
    // Holds this struct at the size and alignment an `Option<libc::iovec>`
    // would occupy, so enclosing completion types keep their footprint
    // without a `repr(C)` field-layout commitment. Never read.
    _reserved: MaybeUninit<usize>,
    descriptor: libc::iovec,
}

impl SctpFirstIovec {
    #[inline(always)]
    const fn empty() -> Self {
        Self {
            _reserved: MaybeUninit::uninit(),
            descriptor: SCTP_EMPTY_FIRST_IOVEC,
        }
    }

    #[inline(always)]
    fn present(descriptor: libc::iovec) -> Self {
        debug_assert_ne!(descriptor.iov_len, 0);
        Self {
            _reserved: MaybeUninit::uninit(),
            descriptor,
        }
    }

    #[inline(always)]
    fn descriptor(&self) -> &libc::iovec {
        &self.descriptor
    }

    #[cfg(test)]
    fn as_ref(&self) -> Option<&libc::iovec> {
        (self.descriptor.iov_len != 0).then_some(&self.descriptor)
    }
}

struct SctpRecvVectoredCompletion<const N: usize> {
    meta: SctpRecvCompletionMeta,
    first_iovec: SctpFirstIovec,
    buffer: IoBuffVecMut<N>,
}

struct StashedSctpRecvCompletion<B> {
    header: SctpRecvHeader,
    buffer: B,
}

struct StashedSctpRecvVectoredCompletion<const N: usize> {
    header: SctpRecvHeader,
    first_iovec: SctpFirstIovec,
    /// Keeps every copied iovec target alive through discard-state processing.
    buffer: IoBuffVecMut<N>,
}

#[inline(always)]
/// Copies the pointer-free receive header fields consumed after extraction.
///
/// # Safety
///
/// `msghdr` must point to the initialized header of one live retained SCTP
/// receive payload.
unsafe fn copy_sctp_recv_header(msghdr: *const MaybeUninit<libc::msghdr>) -> SctpRecvHeader {
    let msg = unsafe { (&*msghdr).assume_init_ref() };
    SctpRecvHeader {
        msg_controllen: msg.msg_controllen,
        msg_flags: msg.msg_flags,
    }
}

#[inline(always)]
/// Parses the receive information needed after retained backing is released.
///
/// # Safety
///
/// `control` and `msghdr` must point to fields of one live, uniquely owned
/// retained SCTP receive payload. `msghdr` must be initialized. When `actual`
/// is `Some`, the kernel-reported control headers and payload bytes within the
/// reported prefix must be initialized; alignment padding may remain
/// uninitialized. `actual` must be `None` after a failed CQE or rejected
/// execution context so uninitialized control storage is never inspected.
unsafe fn parse_sctp_recv_completion_meta(
    control: *const [MaybeUninit<u8>; SCTP_RECV_CONTROL_LEN],
    msghdr: *const MaybeUninit<libc::msghdr>,
    actual: Option<usize>,
) -> SctpRecvCompletionMeta {
    let header = unsafe { copy_sctp_recv_header(msghdr) };
    let rcvinfo = match actual {
        Some(actual) => unsafe {
            parse_completion_rcvinfo(
                &*control,
                header.msg_controllen,
                header.msg_flags,
                actual != 0,
            )
        },
        None => Ok(None),
    };
    SctpRecvCompletionMeta { header, rcvinfo }
}

#[inline(always)]
/// Moves one owner field from retained storage.
///
/// # Safety
///
/// `buffer` must point to the initialized, uniquely owned buffer field of a
/// retained payload whose backing will be released without running its Drop.
unsafe fn take_sctp_retained_buffer<B>(buffer: *const B) -> B {
    unsafe { buffer.read() }
}

#[inline(always)]
/// Copies the first active iovec without reading an uninitialized zero-count
/// array.
///
/// # Safety
///
/// When `iov_count` is nonzero, `iovecs` must point to an array with an
/// initialized first element.
unsafe fn copy_sctp_first_iovec<const N: usize>(
    iovecs: *const [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
) -> SctpFirstIovec {
    if iov_count == 0 {
        return SctpFirstIovec::empty();
    }
    SctpFirstIovec::present(unsafe {
        std::ptr::read(
            iovecs
                .cast::<MaybeUninit<libc::iovec>>()
                .cast::<libc::iovec>(),
        )
    })
}

#[inline(always)]
/// Extracts only the caller buffer and consumed metadata from a scalar receive.
///
/// # Safety
///
/// `payload` must point to a live, uniquely owned retained scalar receive
/// payload. The retained allocation must be released without dropping it after
/// this function returns.
unsafe fn take_sctp_recv_completion<B: IoBuffReadWrite>(
    payload: *mut RetainedSctpRecvPayload<B>,
    actual: Option<usize>,
) -> SctpRecvCompletion<B> {
    let meta = unsafe {
        parse_sctp_recv_completion_meta(
            std::ptr::addr_of!((*payload).control),
            std::ptr::addr_of!((*payload).msghdr),
            actual,
        )
    };
    // Move the sole resource-owning field last. Nothing callback-capable may
    // run between this raw move and returning the compact completion.
    let buffer = unsafe { take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)) };
    SctpRecvCompletion { meta, buffer }
}

#[inline(always)]
/// Extracts only the caller chain, first active iovec, and consumed metadata.
///
/// # Safety
///
/// `payload` must point to a live, uniquely owned retained vectored receive
/// payload. When `iov_count` is nonzero, the first iovec must be initialized
/// and point into the retained chain. The retained allocation must be released
/// without dropping it after this function returns.
unsafe fn take_sctp_recv_vectored_completion<const N: usize>(
    payload: *mut RetainedSctpRecvVectoredPayload<N>,
    iov_count: usize,
    actual: Option<usize>,
) -> SctpRecvVectoredCompletion<N> {
    let meta = unsafe {
        parse_sctp_recv_completion_meta(
            std::ptr::addr_of!((*payload).control),
            std::ptr::addr_of!((*payload).msghdr),
            actual,
        )
    };
    let first_iovec =
        unsafe { copy_sctp_first_iovec(std::ptr::addr_of!((*payload).iovecs), iov_count) };
    // Move the sole resource-owning field last. Nothing callback-capable may
    // run between this raw move and returning the compact completion.
    let buffer = unsafe { take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)) };
    SctpRecvVectoredCompletion {
        meta,
        first_iovec,
        buffer,
    }
}

#[inline(always)]
/// Extracts the fields consumed when retiring a dropped scalar receive.
///
/// # Safety
///
/// The requirements match [`take_sctp_recv_completion`].
unsafe fn take_stashed_sctp_recv_completion<B: IoBuffReadWrite>(
    payload: *mut RetainedSctpRecvPayload<B>,
) -> StashedSctpRecvCompletion<B> {
    let header = unsafe { copy_sctp_recv_header(std::ptr::addr_of!((*payload).msghdr)) };
    let buffer = unsafe { take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)) };
    StashedSctpRecvCompletion { header, buffer }
}

#[inline(always)]
/// Extracts the fields consumed when retiring a dropped vectored receive.
///
/// # Safety
///
/// The requirements match [`take_sctp_recv_vectored_completion`].
unsafe fn take_stashed_sctp_recv_vectored_completion<const N: usize>(
    payload: *mut RetainedSctpRecvVectoredPayload<N>,
    iov_count: usize,
) -> StashedSctpRecvVectoredCompletion<N> {
    let header = unsafe { copy_sctp_recv_header(std::ptr::addr_of!((*payload).msghdr)) };
    let first_iovec =
        unsafe { copy_sctp_first_iovec(std::ptr::addr_of!((*payload).iovecs), iov_count) };
    let buffer = unsafe { take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)) };
    StashedSctpRecvVectoredCompletion {
        header,
        first_iovec,
        buffer,
    }
}

#[inline(always)]
fn fill_recv_vectored_iovecs<const N: usize>(
    buffer: &mut IoBuffVecMut<N>,
    iovecs: &mut [MaybeUninit<libc::iovec>],
) -> io::Result<(usize, usize)> {
    buffer.fill_read_iovecs_and_writable_len(iovecs)
}

const ZERO_LENGTH_SCTP_RECV: &str = "zero-length SCTP receive request";
const ZERO_LENGTH_SCTP_SEND: &str = "zero-length SCTP send request";
const SCTP_SEND_AGGREGATE_OVERFLOW: &str = "SCTP vectored send byte length exceeds usize::MAX";

#[inline(always)]
fn invalid_zero_length_sctp_recv() -> io::Error {
    invalid_input(ZERO_LENGTH_SCTP_RECV)
}

#[inline(always)]
fn invalid_zero_length_sctp_send() -> io::Error {
    invalid_input(ZERO_LENGTH_SCTP_SEND)
}

#[inline(always)]
fn validate_nonempty_sctp_send(requested: usize) -> io::Result<()> {
    if requested == 0 {
        return Err(invalid_zero_length_sctp_send());
    }
    Ok(())
}

#[inline(always)]
fn validate_sctp_vectored_send_len(requested: Option<usize>) -> io::Result<()> {
    let requested = requested.ok_or_else(|| invalid_input(SCTP_SEND_AGGREGATE_OVERFLOW))?;
    validate_nonempty_sctp_send(requested)
}

#[inline(always)]
fn validate_sctp_active_iovec_count(iov_count: usize) -> io::Result<()> {
    if iov_count > RETAINED_IOVEC_MAX_COUNT {
        return Err(invalid_input_kind());
    }
    Ok(())
}

#[inline(always)]
fn checked_sctp_recv_len(requested: usize, writable: usize) -> io::Result<u32> {
    if requested == 0 {
        return Err(invalid_zero_length_sctp_recv());
    }
    checked_read_len(requested, writable)
}

#[inline(always)]
fn checked_sctp_send_len(requested: usize) -> io::Result<u32> {
    validate_nonempty_sctp_send(requested)?;
    checked_send_len(requested)
}

#[inline(always)]
fn sctp_msg_end_of_record(msg_flags: libc::c_int) -> bool {
    (msg_flags & libc::MSG_EOR) != 0
}

#[inline(always)]
fn sctp_msg_clean_eof(actual: usize, header: SctpRecvHeader) -> bool {
    actual == 0 && header.msg_controllen == 0 && header.msg_flags == 0
}

#[inline(always)]
fn sctp_msg_partial_nonempty(actual: usize, msg_flags: libc::c_int) -> bool {
    actual != 0 && !sctp_msg_end_of_record(msg_flags)
}

#[inline(always)]
fn sctp_msg_notification(msg_flags: libc::c_int) -> bool {
    (msg_flags & libc::MSG_NOTIFICATION) != 0
}

const SCTP_PARTIAL_DELIVERY_ABORTED: u32 = 0;

fn parse_sctp_notification_once(
    data_slice: &[u8],
    msg_flags: libc::c_int,
) -> Option<io::Result<SctpRecvMeta>> {
    if sctp_msg_notification(msg_flags) {
        Some(parse_notification(data_slice))
    } else {
        None
    }
}

fn sctp_notification_retires_discard(
    parsed_notification: Option<&io::Result<SctpRecvMeta>>,
) -> bool {
    matches!(
        parsed_notification,
        Some(Ok(SctpRecvMeta::Notification(
            SctpNotification::PartialDelivery {
            indication,
            ..
            }
        ))) if *indication == SCTP_PARTIAL_DELIVERY_ABORTED
    )
}

/// Returns the initialized prefix consumed by a completed scalar receive.
/// Zero progress returns an empty slice without inspecting the caller buffer.
///
/// # Safety
///
/// `actual` bytes beginning at `buffer`'s current writable base must have
/// been initialized by the completed kernel operation.
unsafe fn sctp_scalar_received_slice<B: IoBuffReadWrite>(buffer: &mut B, actual: usize) -> &[u8] {
    if actual == 0 {
        return &[];
    }
    let ptr = buffer.as_mut_ptr();
    unsafe { std::slice::from_raw_parts(ptr, actual) }
}

/// Returns the received prefix visible in the first vectored destination.
///
/// # Safety
///
/// `first_iovec` must describe the first writable destination owned by
/// `buffer`, and its base must remain readable for
/// `min(actual, iov_len)` bytes. Empty completions use the zero-length
/// non-null sentinel descriptor.
#[inline(always)]
unsafe fn sctp_first_iov_slice<'a, const N: usize>(
    _buffer: &'a IoBuffVecMut<N>,
    first_iovec: &libc::iovec,
    actual: usize,
) -> &'a [u8] {
    let safe_len = std::cmp::min(actual, first_iovec.iov_len);
    unsafe { std::slice::from_raw_parts(first_iovec.iov_base.cast::<u8>(), safe_len) }
}

const SCTP_EMPTY_FIRST_IOVEC: libc::iovec = libc::iovec {
    iov_base: NonNull::<u8>::dangling().as_ptr().cast(),
    iov_len: 0,
};

/// Copies the first fixed recovery prefix across active vectored destinations.
///
/// # Safety
///
/// The first `target` bytes across the chain's current writable regions must
/// have been initialized by one completed kernel receive. `storage` may be
/// uninitialized; this function returns only the prefix it initializes.
unsafe fn sctp_vectored_received_prefix<'a, const N: usize>(
    buffer: &mut IoBuffVecMut<N>,
    target: usize,
    storage: &'a mut MaybeUninit<[u8; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN]>,
) -> &'a [u8] {
    debug_assert!(target <= SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN);
    let destination = storage.as_mut_ptr().cast::<u8>();
    let mut copied = 0usize;
    for index in 0..buffer.segments() {
        if copied == target {
            break;
        }
        // SAFETY: `index` is bounded by the chain's initialized segment count.
        let segment = unsafe { buffer.get_mut(index).unwrap_unchecked() };
        let available = std::cmp::min(segment.writable_len(), target - copied);
        if available == 0 {
            continue;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(segment.as_mut_ptr(), destination.add(copied), available);
        }
        copied += available;
    }
    debug_assert_eq!(
        copied, target,
        "completed SCTP iovecs did not cover recovery target"
    );
    unsafe { std::slice::from_raw_parts(destination, copied) }
}

/// Defines the ordinary first-iovec view and independently bounded recovery
/// prefix for one completed vectored receive without changing the expanded
/// fast-path code shape.
macro_rules! sctp_vectored_received_slices {
    (
        $buffer:expr,
        $first_iovec:expr,
        $actual:expr,
        $recovery_target:expr,
        $prefix_storage:ident,
        $data_slice:ident,
        $recovery_prefix:ident
    ) => {
        let __sctp_received_buffer = $buffer;
        let __sctp_received_first_iovec = $first_iovec;
        let __sctp_received_actual = $actual;
        let __sctp_received_recovery_target = $recovery_target;
        debug_assert!(
            __sctp_received_recovery_target
                <= std::cmp::min(__sctp_received_actual, SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN)
        );
        let __sctp_received_first_len =
            std::cmp::min(__sctp_received_actual, __sctp_received_first_iovec.iov_len);
        let mut $prefix_storage = MaybeUninit::uninit();
        let gathered_prefix = if __sctp_received_first_len < __sctp_received_recovery_target {
            Some(unsafe {
                sctp_vectored_received_prefix(
                    &mut *__sctp_received_buffer,
                    __sctp_received_recovery_target,
                    &mut $prefix_storage,
                )
            })
        } else {
            None
        };
        let $data_slice = unsafe {
            sctp_first_iov_slice(
                &*__sctp_received_buffer,
                __sctp_received_first_iovec,
                __sctp_received_actual,
            )
        };
        let $recovery_prefix = gathered_prefix.unwrap_or(
            &$data_slice[..std::cmp::min($data_slice.len(), __sctp_received_recovery_target)],
        );
    };
}

#[inline(always)]
fn sctp_eof_recv_meta() -> SctpRecvMeta {
    SctpRecvMeta::Data(SctpRecvInfo::default())
}

#[inline(always)]
fn build_sctp_send_entry(fd: RawFd, ptr: *const u8, len: u32, user_data: u64) -> squeue::Entry {
    build_send_entry(fd, ptr, len, user_data)
}

#[inline(always)]
fn build_sctp_sendmsg_entry(fd: RawFd, msg: *const libc::msghdr, user_data: u64) -> squeue::Entry {
    build_sendmsg_entry(fd, msg, user_data)
}

/// Returns a completed stashed receive state even if caller buffer inspection
/// unwinds after its retained payload has been detached.
struct StashedSctpStateReturnGuard {
    reactor: *mut Reactor,
    state_ptr: *mut CompletionState,
}

impl StashedSctpStateReturnGuard {
    /// # Safety
    ///
    /// `reactor` must own the live completed `state_ptr`, whose target CQE has
    /// already retired.
    #[inline(always)]
    unsafe fn new(reactor: *mut Reactor, state_ptr: *mut CompletionState) -> Self {
        debug_assert!(!reactor.is_null(), "stashed receive reactor is missing");
        debug_assert!(!state_ptr.is_null(), "stashed receive state is missing");
        Self { reactor, state_ptr }
    }

    #[inline(always)]
    unsafe fn finish(self) {
        let this = disarm_unwind_guard(self);
        unsafe { Reactor::free_op_unchecked(this.reactor, this.state_ptr) };
    }
}

impl Drop for StashedSctpStateReturnGuard {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        unsafe { Reactor::free_op_unchecked(self.reactor, self.state_ptr) };
    }
}

/// Retires a dropped contiguous metadata receive and updates discard state.
///
/// # Safety
///
/// `state_ptr` must be a completed operation owned by `pctx`'s reactor with a
/// retained `RetainedSctpRecvPayload<B>`.
unsafe fn process_stashed_sctp_recv<B: IoBuffReadWrite>(
    reactor: *mut Reactor,
    state_ptr: *mut CompletionState,
    _iov_count: usize,
    recv_state: &mut SctpRecvState,
) {
    let state_return = unsafe { StashedSctpStateReturnGuard::new(reactor, state_ptr) };
    let result = unsafe { completion_cqe_result((*state_ptr).result) };
    if let Ok(actual) = result {
        let mut completion = unsafe {
            Reactor::take_retained_payload_with_unchecked::<RetainedSctpRecvPayload<B>, _>(
                reactor,
                state_ptr,
                |payload| take_stashed_sctp_recv_completion(payload),
            )
        };
        let data_slice = unsafe { sctp_scalar_received_slice(&mut completion.buffer, actual) };
        let recovery_target =
            recv_state.bounded_recovery_prefix_target(actual, completion.header.msg_flags);
        let recovery_prefix = &data_slice[..recovery_target];
        let action = recv_state.process_metadata_completion(
            actual,
            completion.header,
            data_slice,
            recovery_prefix,
            SctpCompletionPublication::Unpublished,
        );
        debug_assert!(matches!(action, SctpMetadataCompletion::Consume));
    }

    unsafe { state_return.finish() };
}

/// Retires a dropped vectored metadata receive and updates discard state.
///
/// # Safety
///
/// `state_ptr` must be a completed operation owned by `pctx`'s reactor with a
/// retained `RetainedSctpRecvVectoredPayload<N>`, and `iov_count` must describe
/// its initialized iovec prefix.
unsafe fn process_stashed_sctp_recv_vectored<const N: usize>(
    reactor: *mut Reactor,
    state_ptr: *mut CompletionState,
    iov_count: usize,
    recv_state: &mut SctpRecvState,
) {
    let result = unsafe { completion_cqe_result((*state_ptr).result) };
    if let Ok(actual) = result {
        let mut completion = unsafe {
            Reactor::take_retained_payload_with_unchecked::<RetainedSctpRecvVectoredPayload<N>, _>(
                reactor,
                state_ptr,
                |payload| take_stashed_sctp_recv_vectored_completion(payload, iov_count),
            )
        };
        let target = recv_state.bounded_recovery_prefix_target(actual, completion.header.msg_flags);
        sctp_vectored_received_slices!(
            &mut completion.buffer,
            completion.first_iovec.descriptor(),
            actual,
            target,
            prefix_storage,
            data_slice,
            recovery_prefix
        );
        let action = recv_state.process_metadata_completion(
            actual,
            completion.header,
            data_slice,
            recovery_prefix,
            SctpCompletionPublication::Unpublished,
        );
        debug_assert!(matches!(action, SctpMetadataCompletion::Consume));
    }

    unsafe { Reactor::free_op_unchecked(reactor, state_ptr) };
}

#[inline(always)]
fn debug_assert_sctp_fd_state(fd: RawFd, fd_state: &RuntimeFdOpState<'_>) {
    let state_fd = fd_state.raw_fd();
    debug_assert!(
        state_fd < 0 || fd == state_fd,
        "SCTP future raw descriptor and typed operation state diverged"
    );
}

#[inline(always)]
/// Extracts selected data from a completed SCTP payload and releases its
/// operation slot.
///
/// # Safety
///
/// A non-null submitted pointer in `fd_state` must identify a completed FlowIO operation with
/// retained payload type `T`. Cleanup uses its recorded origin reactor, and
/// `extract` must move or drop every initialized field that requires
/// destruction. The extractor receives `Some(actual)` only for a successful
/// CQE in an accepted execution context; `None` prevents inspection of
/// potentially uninitialized kernel-output storage after errors or rejection.
unsafe fn take_completed_sctp_payload_with<T: 'static, R>(
    cx: &mut Context<'_>,
    fd_state: &mut RuntimeFdOpState<'_>,
    extract: impl FnOnce(*mut T, Option<usize>) -> R,
) -> Option<CompletionTake<io::Result<usize>, R>> {
    let state_ptr = fd_state.state_ptr();
    if state_ptr.is_null() {
        return None;
    }

    let state = unsafe { &*state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = completion_cqe_result(state.result);
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
    let context_rejected = op_ctx.context_rejected();
    let actual = if context_rejected {
        None
    } else {
        result.as_ref().ok().copied()
    };
    let value = unsafe {
        op_ctx.take_retained_payload_with_unchecked::<T, R>(state_ptr, |payload| {
            extract(payload, actual)
        })
    };
    let retired = fd_state.take_state_ptr();
    debug_assert_eq!(retired, state_ptr);
    unsafe { op_ctx.free_op_unchecked(state_ptr) };
    Some(CompletionTake::from_context(
        result,
        value,
        context_rejected,
    ))
}

#[inline(always)]
/// Allocates and registers an SCTP operation state without publishing it to a
/// future before the target SQE is successfully submitted.
///
/// # Safety
///
/// `cx` must carry a valid FlowIO waker for the executor/reactor that will own
/// the new operation. The returned guard must remain the unique owner until
/// the state is submitted or released.
unsafe fn prepare_unsubmitted_sctp_state(
    cx: &mut Context<'_>,
) -> io::Result<(PollCtx, UnsubmittedOpGuard)> {
    let pctx = poll_ctx_from_waker(cx)?;
    let Some(guard) = (unsafe { prepare_unsubmitted_op(&pctx) }) else {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    };
    Ok((pctx, guard))
}

#[doc(hidden)]
pub struct DataRecvFuture<'a, B: IoBuffReadWrite> {
    /// SCTP association socket descriptor used for this data-only receive.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Logical buffer length captured before the receive writable window formed.
    write_base_len: usize,
    /// Maximum bytes requested from the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadWrite> Future for DataRecvFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_sctp_fd_state(this.fd, &this.state_ptr);

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) = unsafe {
            take_completed_sctp_payload_with::<RetainedDataRecvPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload, _| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            let (result, buffer) = completion.into_io_result(std::convert::identity);
            match result {
                Err(err) => return Poll::Ready((Err(err), buffer)),
                Ok(actual) => {
                    let completed = unsafe {
                        complete_read_with_progress(buffer, this.write_base_len, actual, Ok(actual))
                    };
                    return Poll::Ready(completed);
                }
            }
        }

        if this.state_ptr.is_null() {
            let (pctx, guard) = match unsafe { prepare_unsubmitted_sctp_state(cx) } {
                Ok(state) => state,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = RetainedDataRecvPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) = submit_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let ptr = payload.buffer.as_mut_ptr();
                        Ok(opcode::Recv::new(types::Fd(fd), ptr, this.len)
                            .build()
                            .user_data(state_ptr as u64))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for DataRecvFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct DataSendFuture<'a, B: IoBuffReadOnly> {
    /// SCTP association socket descriptor used for this data-only send.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned send buffer returned on completion.
    buffer: Option<B>,
    /// Validated contiguous send length.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadOnly> Future for DataSendFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_sctp_fd_state(this.fd, &this.state_ptr);

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) = unsafe {
            take_completed_sctp_payload_with::<RetainedDataSendPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload, _| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            let (result, buffer) = completion.into_io_result(std::convert::identity);
            return Poll::Ready((result, buffer));
        }

        if this.state_ptr.is_null() {
            let (pctx, guard) = match unsafe { prepare_unsubmitted_sctp_state(cx) } {
                Ok(state) => state,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = RetainedDataSendPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) = submit_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        let ptr = payload.buffer.as_ptr();
                        Ok(build_sctp_send_entry(fd, ptr, this.len, state_ptr as u64))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for DataSendFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct RecvFuture<'a, B: IoBuffReadWrite> {
    /// SCTP association socket descriptor used for this recvmsg path.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Logical buffer length captured before the receive writable window formed.
    write_base_len: usize,
    /// Maximum message bytes requested from the kernel.
    len: u32,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Parent stream metadata receive state shared across metadata receives.
    recv_state: &'a mut SctpRecvState,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadWrite> Future for RecvFuture<'_, B> {
    type Output = (io::Result<(usize, SctpRecvMeta)>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_sctp_fd_state(this.fd, &this.state_ptr);

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        match unsafe { this.recv_state.poll_stashed(cx) } {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(err), buffer));
            }
            Poll::Ready(Ok(())) => {}
        }

        let inspect_metadata = this.recv_state.record_sync.is_synced();
        if let Some(completion) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpRecvPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload, actual| {
                    take_sctp_recv_completion(payload, actual.filter(|_| inspect_metadata))
                },
            )
        } {
            let (result, mut completion) = match completion {
                CompletionTake::Accepted { result, value } => (result, value),
                CompletionTake::ContextRejected {
                    result,
                    value: mut completion,
                } => {
                    if let Ok(actual) = result {
                        let header = completion.meta.header;
                        // SAFETY: a successful CQE initialized exactly this
                        // prefix, and the returned buffer still owns its stable
                        // backing. Inspect a nonempty caller buffer before
                        // changing record-recovery state because its pointer
                        // callback may unwind. Zero progress bypasses the
                        // callback inside the shared slice helper.
                        let data_slice =
                            unsafe { sctp_scalar_received_slice(&mut completion.buffer, actual) };
                        let recovery_target = this
                            .recv_state
                            .bounded_recovery_prefix_target(actual, header.msg_flags);
                        let recovery_prefix = &data_slice[..recovery_target];
                        let action = this.recv_state.process_metadata_completion(
                            actual,
                            header,
                            data_slice,
                            recovery_prefix,
                            SctpCompletionPublication::Unpublished,
                        );
                        debug_assert!(matches!(action, SctpMetadataCompletion::Consume));
                    }
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::NotConnected)),
                        completion.buffer,
                    ));
                }
            };
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), completion.buffer)),
            };

            let header = completion.meta.header;
            let data_slice = unsafe { sctp_scalar_received_slice(&mut completion.buffer, actual) };
            let recovery_target = this
                .recv_state
                .bounded_recovery_prefix_target(actual, header.msg_flags);
            let recovery_prefix = &data_slice[..recovery_target];
            let action = this.recv_state.process_metadata_completion(
                actual,
                header,
                data_slice,
                recovery_prefix,
                SctpCompletionPublication::Visible(completion.meta.rcvinfo),
            );
            return match action {
                SctpMetadataCompletion::Consume => {
                    let (_, buffer) = unsafe {
                        complete_read_with_progress(
                            completion.buffer,
                            this.write_base_len,
                            0,
                            Ok(()),
                        )
                    };
                    // Non-vectored internal recovery has no reusable iovec scratch
                    // to refill: the next poll builds a fresh single-iovec payload
                    // at the same unchanged caller-visible writable tail.
                    this.buffer = Some(buffer);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                SctpMetadataCompletion::Publish(meta) => {
                    let result = meta.map(|meta| (actual, meta));
                    let completed = unsafe {
                        complete_read_with_progress(
                            completion.buffer,
                            this.write_base_len,
                            actual,
                            result,
                        )
                    };
                    Poll::Ready(completed)
                }
            };
        }

        if this.state_ptr.is_null() {
            let (pctx, guard) = match unsafe { prepare_unsubmitted_sctp_state(cx) } {
                Ok(state) => state,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = unsafe {
                emplace_retained_sctp_recv_payload(
                    Reactor::retained_payload_pool_ptr(pctx.reactor()),
                    &mut this.buffer,
                    this.len,
                )
            };
            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        Ok(
                            opcode::RecvMsg::new(types::Fd(fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFuture<'_, B> {
    fn drop(&mut self) {
        unsafe {
            let recv_state = self.recv_state as *mut SctpRecvState;
            if self.state_ptr.is_null() {
                SctpRecvState::clear_stashed_waiter_unchecked(recv_state);
            } else {
                let mut state_ptr = self.state_ptr.take_state_ptr();
                SctpRecvState::stash_unchecked(
                    recv_state,
                    &mut state_ptr,
                    0,
                    process_stashed_sctp_recv::<B>,
                );
                debug_assert!(state_ptr.is_null());
            }
        }
    }
}

#[doc(hidden)]
pub struct SendFuture<'a, B: IoBuffReadOnly> {
    /// SCTP association socket descriptor used for this sendmsg path.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned send buffer returned on completion.
    buffer: Option<B>,
    /// Validated contiguous send length.
    len: u32,
    /// Public send metadata translated into the kernel ABI layout.
    sndinfo: libc::sctp_sndinfo,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadOnly> Future for SendFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_sctp_fd_state(this.fd, &this.state_ptr);

        if this.state_ptr.is_null()
            && let Some(err) = this.input_error.take()
        {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }
        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(completion) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpSendPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload, _| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            let (result, buffer) = completion.into_io_result(std::convert::identity);
            return Poll::Ready((result, buffer));
        }

        if this.state_ptr.is_null() {
            let (pctx, guard) = match unsafe { prepare_unsubmitted_sctp_state(cx) } {
                Ok(state) => state,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = unsafe {
                emplace_retained_sctp_send_payload(
                    Reactor::retained_payload_pool_ptr(pctx.reactor()),
                    &mut this.buffer,
                    this.len,
                    this.sndinfo,
                )
            };
            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        Ok(build_sctp_sendmsg_entry(
                            fd,
                            payload.msghdr.as_ptr(),
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for SendFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvVectoredFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvVectoredFuture<'a, const N: usize> {
    /// SCTP association socket descriptor used for this vectored recvmsg path.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned vectored receive chain returned on completion.
    buffer: Option<IoBuffVecMut<N>>,
    /// Number of nonempty writable segments materialized for each submission.
    iov_count: usize,
    /// Total writable capacity paired with `iov_count` for each submission.
    writable: usize,
    /// Whether the sizing pass found an unrepresentable writable aggregate.
    invalid_aggregate: bool,
    /// Parent stream metadata receive state shared across metadata receives.
    recv_state: &'a mut SctpRecvState,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for RecvVectoredFuture<'_, N> {
    type Output = (io::Result<(usize, SctpRecvMeta)>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_sctp_fd_state(this.fd, &this.state_ptr);

        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        let input_error = if this.state_ptr.is_null() {
            if std::mem::take(&mut this.invalid_aggregate) {
                Some(invalid_readv_aggregate())
            } else if this.writable == 0 {
                Some(invalid_zero_length_sctp_recv())
            } else {
                validate_sctp_active_iovec_count(this.iov_count).err()
            }
        } else {
            None
        };
        if let Some(err) = input_error {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }

        match unsafe { this.recv_state.poll_stashed(cx) } {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(err), buffer));
            }
            Poll::Ready(Ok(())) => {}
        }

        let inspect_metadata = this.recv_state.record_sync.is_synced();
        if let Some(completion) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpRecvVectoredPayload<N>, _>(
                cx,
                &mut this.state_ptr,
                |payload, actual| {
                    take_sctp_recv_vectored_completion(
                        payload,
                        this.iov_count,
                        actual.filter(|_| inspect_metadata),
                    )
                },
            )
        } {
            let (result, mut completion) = match completion {
                CompletionTake::Accepted { result, value } => (result, value),
                CompletionTake::ContextRejected {
                    result,
                    value: mut completion,
                } => {
                    if let Ok(actual) = result {
                        let header = completion.meta.header;
                        // SAFETY: completion extraction copied the first active
                        // kernel iovec before releasing retained storage, and the
                        // returned chain still owns every referenced allocation.
                        let target = this
                            .recv_state
                            .bounded_recovery_prefix_target(actual, header.msg_flags);
                        sctp_vectored_received_slices!(
                            &mut completion.buffer,
                            completion.first_iovec.descriptor(),
                            actual,
                            target,
                            prefix_storage,
                            data_slice,
                            recovery_prefix
                        );
                        let action = this.recv_state.process_metadata_completion(
                            actual,
                            header,
                            data_slice,
                            recovery_prefix,
                            SctpCompletionPublication::Unpublished,
                        );
                        debug_assert!(matches!(action, SctpMetadataCompletion::Consume));
                    }
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::NotConnected)),
                        completion.buffer,
                    ));
                }
            };
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), completion.buffer)),
            };
            let header = completion.meta.header;
            let target = this
                .recv_state
                .bounded_recovery_prefix_target(actual, header.msg_flags);
            sctp_vectored_received_slices!(
                &mut completion.buffer,
                completion.first_iovec.descriptor(),
                actual,
                target,
                prefix_storage,
                data_slice,
                recovery_prefix
            );
            let action = this.recv_state.process_metadata_completion(
                actual,
                header,
                data_slice,
                recovery_prefix,
                SctpCompletionPublication::Visible(completion.meta.rcvinfo),
            );
            return match action {
                SctpMetadataCompletion::Consume => {
                    let mut buffer = completion.buffer;
                    unsafe {
                        buffer.distribute_written(0);
                    }
                    let Some((iov_count, writable)) =
                        buffer.checked_read_iovec_count_and_writable_len()
                    else {
                        return Poll::Ready((Err(invalid_readv_aggregate()), buffer));
                    };
                    if writable == 0 {
                        return Poll::Ready((Err(invalid_read_iovec_shape()), buffer));
                    }
                    debug_assert_eq!(
                        (iov_count, writable),
                        (this.iov_count, this.writable),
                        "SCTP vectored internal recv changed the receive chain shape"
                    );
                    this.iov_count = iov_count;
                    this.writable = writable;
                    this.buffer = Some(buffer);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                SctpMetadataCompletion::Publish(meta) => {
                    let mut buffer = completion.buffer;
                    unsafe {
                        buffer.distribute_written(actual);
                    }
                    match meta {
                        Ok(meta) => Poll::Ready((Ok((actual, meta)), buffer)),
                        Err(err) => Poll::Ready((Err(err), buffer)),
                    }
                }
            };
        }

        if this.state_ptr.is_null() {
            let (pctx, guard) = match unsafe { prepare_unsubmitted_sctp_state(cx) } {
                Ok(state) => state,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = match unsafe {
                emplace_retained_sctp_recv_vectored_payload(
                    Reactor::retained_payload_pool_ptr(pctx.reactor()),
                    &mut this.buffer,
                    (this.iov_count, this.writable),
                )
            } {
                Ok(payload) => payload,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        Ok(
                            opcode::RecvMsg::new(types::Fd(fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<const N: usize> Drop for RecvVectoredFuture<'_, N> {
    fn drop(&mut self) {
        unsafe {
            let recv_state = self.recv_state as *mut SctpRecvState;
            if self.state_ptr.is_null() {
                SctpRecvState::clear_stashed_waiter_unchecked(recv_state);
            } else {
                let mut state_ptr = self.state_ptr.take_state_ptr();
                SctpRecvState::stash_unchecked(
                    recv_state,
                    &mut state_ptr,
                    self.iov_count,
                    process_stashed_sctp_recv_vectored::<N>,
                );
                debug_assert!(state_ptr.is_null());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SendVectoredFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendVectoredFuture<'a, const N: usize> {
    /// SCTP association socket descriptor used for this vectored sendmsg path.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: RuntimeFdOpState<'a>,
    /// Caller-owned vectored send chain returned on completion.
    buffer: Option<IoBuffVec<N>>,
    /// Number of nonempty readable segments expected at materialization.
    iov_count: usize,
    /// Total readable byte count paired with `iov_count`.
    total: usize,
    /// Whether the sizing pass found an unrepresentable readable aggregate.
    invalid_aggregate: bool,
    /// Public send metadata translated into the kernel ABI layout.
    sndinfo: libc::sctp_sndinfo,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for SendVectoredFuture<'_, N> {
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        debug_assert_sctp_fd_state(this.fd, &this.state_ptr);

        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        let input_error = if this.state_ptr.is_null() {
            let requested = if std::mem::take(&mut this.invalid_aggregate) {
                None
            } else {
                Some(this.total)
            };
            validate_sctp_vectored_send_len(requested)
                .and_then(|()| validate_sctp_active_iovec_count(this.iov_count))
                .err()
        } else {
            None
        };
        if let Some(err) = input_error {
            let result = validate_local_io_result(cx, Err(err));
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((result, buffer));
        }

        if let Some(completion) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpSendVectoredPayload<N>, _>(
                cx,
                &mut this.state_ptr,
                |payload, _| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            let (result, buffer) = completion.into_io_result(std::convert::identity);
            return Poll::Ready((result, buffer));
        }

        if this.state_ptr.is_null() {
            let (pctx, guard) = match unsafe { prepare_unsubmitted_sctp_state(cx) } {
                Ok(state) => state,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = guard.state_ptr();

            let payload = match unsafe {
                emplace_retained_sctp_send_vectored_payload(
                    Reactor::retained_payload_pool_ptr(pctx.reactor()),
                    &mut this.buffer,
                    (this.iov_count, this.total),
                    this.sndinfo,
                )
            } {
                Ok(payload) => payload,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            unsafe {
                if let Err((e, payload)) = submit_initialized_retained_fd_sqe(
                    &pctx,
                    state_ptr,
                    &mut this.state_ptr,
                    payload,
                    |fd, payload| {
                        Ok(build_sctp_sendmsg_entry(
                            fd,
                            payload.msghdr.as_ptr(),
                            state_ptr as u64,
                        ))
                    },
                ) {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            guard.disarm();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr.state_ptr()) };
        Poll::Pending
    }
}

impl<const N: usize> Drop for SendVectoredFuture<'_, N> {
    fn drop(&mut self) {
        unsafe { drop_fd_op_state_unchecked(&mut self.state_ptr) };
    }
}

/// Future returned by [`SctpListener::accept`] for one incoming association.
///
/// It resolves to the connected [`SctpStream`] and its peer address. The
/// future borrows the listener's reusable accept slot, so a listener can have
/// at most one live accept future. Dropping a prepared pending future cancels
/// its readiness wait without consuming an association from the listener
/// backlog; dropping an unprepared future cannot affect the earlier owner.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{AcceptFuture, SctpListener};
///
/// fn accept(listener: &mut SctpListener) -> AcceptFuture<'_> {
///     listener.accept()
/// }
/// ```
pub struct AcceptFuture<'a> {
    /// Borrowed reusable accept slot owned by the listener.
    slot: &'a mut AcceptSlot,
    /// Configuration retained for post-accept setup and receive policy.
    accepted_config: SctpSocketConfig,
    /// Deferred slot-state error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// True when this future successfully prepared and owns the slot.
    prepared: bool,
}

impl AcceptFuture<'_> {
    fn poll_with_established_config<F>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        apply_config: F,
    ) -> Poll<io::Result<(SctpStream, SocketAddr)>>
    where
        F: FnOnce(RawFd, SctpSocketConfig, LingerProvenance) -> io::Result<()>,
    {
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(err) = this.input_error.take() {
            return Poll::Ready(validate_local_io_result(cx, Err(err)));
        }

        let accepted_config = this.accepted_config;
        this.slot.poll_accept(
            this.prepared,
            cx,
            move |accepted_fd, accepted_linger_provenance, addr, addrlen| {
                finish_accepted_owned_stream_with(
                    accepted_fd,
                    accepted_linger_provenance,
                    addr,
                    addrlen,
                    accepted_config,
                    apply_config,
                )
            },
        )
    }
}

impl Future for AcceptFuture<'_> {
    type Output = io::Result<(SctpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_with_established_config(cx, apply_sctp_accepted_established_config)
    }
}

impl Drop for AcceptFuture<'_> {
    fn drop(&mut self) {
        if self.prepared {
            self.slot.drop_future();
        }
    }
}

/// Future returned by [`SctpConnector::connect`] for one association attempt.
///
/// It resolves to a connected [`SctpStream`]. The future borrows the
/// connector's reusable slot, so the connector becomes available for another
/// attempt after this future completes or is dropped.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{ConnectFuture, SctpConnector};
/// use std::net::SocketAddr;
///
/// fn connect<'a>(
///     connector: &'a mut SctpConnector,
///     peer: SocketAddr,
/// ) -> std::io::Result<ConnectFuture<'a>> {
///     connector.connect(peer)
/// }
/// ```
pub struct ConnectFuture<'a> {
    /// Borrowed reusable connect slot owned by the connector.
    slot: &'a mut ConnectSlot,
    /// Remote address recorded into the resulting public stream on success.
    remote_addr: SocketAddr,
}

impl Future for ConnectFuture<'_> {
    type Output = io::Result<SctpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let remote_addr = this.remote_addr;
        this.slot.poll_connect(cx, move |fd, config| {
            apply_sctp_established_config(fd.as_raw_fd(), *config)?;
            Ok(finish_connected_runtime_stream(fd, remote_addr, *config))
        })
    }
}

impl Drop for ConnectFuture<'_> {
    fn drop(&mut self) {
        self.slot.drop_future();
    }
}

/// Future returned by [`SctpConnector::connect_timeout`] for one timed
/// association attempt.
///
/// It resolves to a connected [`SctpStream`], or to
/// [`io::ErrorKind::TimedOut`] when the relative timeout expires. The future
/// borrows the connector's reusable slot for the duration of the attempt.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{ConnectTimeoutFuture, SctpConnector};
/// use std::net::SocketAddr;
/// use std::time::Duration;
///
/// fn connect_with_timeout<'a>(
///     connector: &'a mut SctpConnector,
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
    type Output = io::Result<SctpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx) {
            Poll::Ready(result) => Poll::Ready(map_connect_timeout(result)),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn new_sctp_socket(domain: libc::c_int, kind: libc::c_int) -> io::Result<RawFd> {
    let fd = unsafe {
        libc::socket(
            domain,
            kind | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            libc::IPPROTO_SCTP,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

fn raw_sndinfo_from_public(info: SctpSendInfo) -> libc::sctp_sndinfo {
    libc::sctp_sndinfo {
        snd_sid: info.stream_id,
        snd_flags: info.flags,
        snd_ppid: info.ppid.to_be(),
        snd_context: info.context,
        snd_assoc_id: info.assoc_id,
    }
}

fn set_sctp_events(fd: RawFd, mask: SctpNotificationMask) -> io::Result<()> {
    let events = SctpEventSubscribe::from_mask(mask);
    set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_EVENTS, &events)?;
    Ok(())
}

fn apply_sctp_socket_options(fd: RawFd, options: SctpSocketOptions) -> io::Result<()> {
    set_sctp_events(fd, options.notifications)?;

    let recv_rcvinfo: libc::c_int = if options.recv_rcvinfo { 1 } else { 0 };
    set_sock_opt(
        fd,
        libc::IPPROTO_SCTP,
        libc::SCTP_RECVRCVINFO,
        &recv_rcvinfo,
    )?;

    let nodelay: libc::c_int = if options.nodelay { 1 } else { 0 };
    set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_NODELAY, &nodelay)?;

    if let Some(size) = options.send_buffer_size {
        super::set_sock_send_buffer_size(fd, size)?;
    }

    if let Some(size) = options.recv_buffer_size {
        super::set_sock_recv_buffer_size(fd, size)?;
    }

    if let Some(info) = options.default_send_info {
        set_sock_opt(
            fd,
            libc::IPPROTO_SCTP,
            libc::SCTP_DEFAULT_SNDINFO,
            &raw_sndinfo_from_public(info),
        )?;
    }

    Ok(())
}

fn apply_assoc_config_raw(fd: RawFd, config: SctpAssocConfig) -> io::Result<()> {
    if let Some(assoc_max_retrans) = config.assoc_max_retrans {
        let mut raw = SctpAssocParamsRaw::get(fd)?;
        raw.assoc_max_retrans = assoc_max_retrans;
        set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_ASSOCINFO, &raw)?;
    }

    if config.rto_initial_ms.is_some() || config.rto_max_ms.is_some() || config.rto_min_ms.is_some()
    {
        let mut raw = SctpRtoInfoRaw::get(fd)?;
        if let Some(rto_initial_ms) = config.rto_initial_ms {
            raw.rto_initial_ms = rto_initial_ms;
        }
        if let Some(rto_max_ms) = config.rto_max_ms {
            raw.rto_max_ms = rto_max_ms;
        }
        if let Some(rto_min_ms) = config.rto_min_ms {
            raw.rto_min_ms = rto_min_ms;
        }
        set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_RTOINFO, &raw)?;
    }

    Ok(())
}

fn set_sctp_sock_opt_bytes(fd: RawFd, name: libc::c_int, value: &[u8]) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_SCTP,
            name,
            value.as_ptr() as *const libc::c_void,
            value.len() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn paddr_params_bytes<T, const N: usize>(raw: &T) -> [u8; N] {
    const {
        assert!(std::mem::size_of::<T>() <= N);
    }
    let mut buffer = [0u8; N];
    unsafe {
        std::ptr::copy_nonoverlapping(
            raw as *const T as *const u8,
            buffer.as_mut_ptr(),
            std::mem::size_of::<T>(),
        );
    }
    buffer
}

fn apply_peer_addr_params_raw(fd: RawFd, params: SctpPeerAddrParams) -> io::Result<()> {
    let fields = SctpPaddrParamsFields::from_public(params);
    if fields.requires_modern_sockopt() {
        let raw = SctpPaddrParamsRaw::from_fields(fields);
        let bytes = paddr_params_bytes::<_, SCTP_PADDR_PARAMS_RAW_OPT_LEN>(&raw);
        set_sctp_sock_opt_bytes(fd, libc::SCTP_PEER_ADDR_PARAMS, &bytes)
    } else {
        let raw = SctpPaddrParamsRawLegacy::from_fields(fields);
        let bytes = paddr_params_bytes::<_, SCTP_PADDR_PARAMS_LEGACY_OPT_LEN>(&raw);
        set_sctp_sock_opt_bytes(fd, libc::SCTP_PEER_ADDR_PARAMS, &bytes)
    }
}

fn apply_default_peer_addr_params(fd: RawFd, params: SctpPeerAddrParams) -> io::Result<()> {
    if params.address.is_some() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    apply_peer_addr_params_raw(fd, params)
}

fn apply_sctp_established_config(fd: RawFd, config: SctpSocketConfig) -> io::Result<()> {
    if let Some(assoc) = config.assoc {
        apply_assoc_config_raw(fd, assoc)?;
    }
    if let Some(params) = config.default_peer_addr_params {
        apply_default_peer_addr_params(fd, params)?;
    }
    Ok(())
}

fn apply_sctp_accepted_established_config(
    fd: RawFd,
    config: SctpSocketConfig,
    provenance: LingerProvenance,
) -> io::Result<()> {
    // Linux inherits the listener's SCTP_EVENTS, SCTP_RECVRCVINFO,
    // SCTP_NODELAY, and SCTP_DEFAULT_SNDINFO state while the listener remains
    // FlowIO-managed. Raw exposure makes all shared socket options uncertain,
    // so preserve the established contract by restoring the complete config.
    if provenance == LingerProvenance::Uncertain {
        apply_sctp_socket_options(fd, config.socket_options())?;
    } else {
        // Some supported Linux SCTP accept paths copy effective
        // SO_SNDBUF/SO_RCVBUF values without copying the generic user-lock
        // bits. Repeat configured buffer options to preserve their explicit
        // size semantics under pressure.
        if let Some(size) = config.send_buffer_size {
            super::set_sock_send_buffer_size(fd, size)?;
        }
        if let Some(size) = config.recv_buffer_size {
            super::set_sock_recv_buffer_size(fd, size)?;
        }
    }
    apply_sctp_established_config(fd, config)
}

fn apply_sctp_init_config(fd: RawFd, config: SctpInitConfig) -> io::Result<()> {
    set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_INITMSG, &config.as_raw())
}

fn configure_sctp_socket(fd: RawFd, config: SctpSocketConfig) -> io::Result<()> {
    // Association and default-peer settings are association-scoped, so the
    // accept/connect continuations apply them after establishment.
    apply_sctp_init_config(fd, config.init)?;
    apply_sctp_socket_options(fd, config.socket_options())
}

#[repr(C)]
/// Header returned before packed addresses by Linux SCTP address enumeration.
struct SctpGetAddrsHeader {
    /// Association whose addresses are requested.
    assoc_id: libc::sctp_assoc_t,
    /// Number of packed addresses returned by a successful query.
    addr_num: u32,
}

// Query capacities are sockaddr_storage byte-budget units, not address-count
// limits. Linux packs the actual IPv4/IPv6 records, so the final 1,024-unit
// payload can hold up to 8,192 sockaddr_in records.
const INITIAL_SCTP_ASSOC_ADDR_CAPACITY: usize = 8;
const MAX_SCTP_ASSOC_ADDR_ATTEMPTS: usize = 8;
const MAX_SCTP_ASSOC_ADDR_CAPACITY: usize = 1024;
const MIN_SCTP_ASSOC_ADDR_LEN: usize = std::mem::size_of::<libc::sockaddr_in>();

const _: () = {
    assert!(MAX_SCTP_ASSOC_ADDR_ATTEMPTS > 0);
    assert!(
        INITIAL_SCTP_ASSOC_ADDR_CAPACITY << (MAX_SCTP_ASSOC_ADDR_ATTEMPTS - 1)
            == MAX_SCTP_ASSOC_ADDR_CAPACITY
    );
};

fn assoc_addrs_initial_capacity(addr_count: usize, payload_len: usize) -> usize {
    addr_count.min(payload_len / MIN_SCTP_ASSOC_ADDR_LEN)
}

fn assoc_addrs_buffer_len(
    capacity: usize,
    header_len: usize,
    storage_len: usize,
) -> io::Result<usize> {
    if capacity > MAX_SCTP_ASSOC_ADDR_CAPACITY {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let storage_bytes = capacity
        .checked_mul(storage_len)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
    let total_len = header_len
        .checked_add(storage_bytes)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
    if total_len > libc::socklen_t::MAX as usize {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    Ok(total_len)
}

/// Normalizes Linux's option-specific SCTP address length to a buffer end.
///
/// `SCTP_GET_PEER_ADDRS` reports the header plus packed-address payload.
/// Linux's frozen `SCTP_GET_LOCAL_ADDRS` ABI reports only the payload length,
/// even though the returned buffer still starts with the same header.
fn assoc_addrs_payload_end(
    optname: libc::c_int,
    returned_len: usize,
    header_len: usize,
    buffer_len: usize,
) -> io::Result<usize> {
    let payload_end = match optname {
        SCTP_GET_LOCAL_ADDRS_OPT => header_len
            .checked_add(returned_len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?,
        SCTP_GET_PEER_ADDRS_OPT => returned_len,
        _ => return Err(io::Error::from(io::ErrorKind::InvalidData)),
    };

    if payload_end < header_len || payload_end > buffer_len {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(payload_end)
}

fn get_assoc_addrs(
    fd: RawFd,
    optname: libc::c_int,
    assoc_id: libc::sctp_assoc_t,
) -> io::Result<Vec<SocketAddr>> {
    get_assoc_addrs_with(optname, assoc_id, |buffer| {
        let mut optlen = buffer.len() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_SCTP,
                optname,
                buffer.as_mut_ptr() as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(optlen as usize)
    })
}

fn get_assoc_addrs_with(
    optname: libc::c_int,
    assoc_id: libc::sctp_assoc_t,
    mut query: impl FnMut(&mut [u8]) -> io::Result<usize>,
) -> io::Result<Vec<SocketAddr>> {
    let mut capacity = INITIAL_SCTP_ASSOC_ADDR_CAPACITY;
    for attempt in 0..MAX_SCTP_ASSOC_ADDR_ATTEMPTS {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        let total_len = assoc_addrs_buffer_len(capacity, header_len, storage_len)?;
        let mut buffer = vec![0u8; total_len];

        let header = SctpGetAddrsHeader {
            assoc_id,
            addr_num: 0,
        };
        unsafe {
            std::ptr::write_unaligned(buffer.as_mut_ptr() as *mut SctpGetAddrsHeader, header);
        }

        let returned_len = match query(&mut buffer) {
            Ok(returned_len) => returned_len,
            Err(err)
                if err.raw_os_error() == Some(libc::ENOMEM)
                    && attempt + 1 < MAX_SCTP_ASSOC_ADDR_ATTEMPTS
                    && capacity < MAX_SCTP_ASSOC_ADDR_CAPACITY =>
            {
                capacity = capacity.saturating_mul(2).min(MAX_SCTP_ASSOC_ADDR_CAPACITY);
                continue;
            }
            Err(err) => return Err(err),
        };

        let payload_end = assoc_addrs_payload_end(optname, returned_len, header_len, buffer.len())?;

        let header =
            unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpGetAddrsHeader) };
        let payload = &buffer[header_len..payload_end];
        return parse_assoc_addrs(payload, header.addr_num as usize);
    }

    Err(io::Error::from(io::ErrorKind::InvalidData))
}

fn option_socket_addr_to_storage(addr: Option<SocketAddr>) -> libc::sockaddr_storage {
    match addr {
        Some(addr) => socket_addr_to_c(addr).0,
        None => unsafe { std::mem::zeroed() },
    }
}

fn sockaddr_len_for_storage(storage: libc::sockaddr_storage) -> io::Result<libc::socklen_t> {
    let family = unsafe {
        *(&storage as *const libc::sockaddr_storage as *const libc::sa_family_t) as libc::c_int
    };
    sockaddr_len_for_family(family).map(|len| len as libc::socklen_t)
}

fn sockaddr_len_for_family(family: libc::c_int) -> io::Result<usize> {
    match family {
        libc::AF_INET => Ok(std::mem::size_of::<libc::sockaddr_in>()),
        libc::AF_INET6 => Ok(std::mem::size_of::<libc::sockaddr_in6>()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SCTP sockaddr has unsupported address family",
        )),
    }
}

fn storage_to_option_socket_addr(
    storage: libc::sockaddr_storage,
) -> io::Result<Option<SocketAddr>> {
    let family = unsafe {
        *(&storage as *const libc::sockaddr_storage as *const libc::sa_family_t) as libc::c_int
    };
    match family {
        0 => Ok(None),
        libc::AF_INET => socket_addr_from_c(
            &storage,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
        .map(Some),
        libc::AF_INET6 => socket_addr_from_c(
            &storage,
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
        )
        .map(Some),
        _ => Err(io::Error::from(io::ErrorKind::InvalidData)),
    }
}

/// Parses exactly `addr_count` packed Linux SCTP association addresses.
///
/// Linux emits one concrete `sockaddr_in` or `sockaddr_in6` per family with no
/// padding between entries. The declared count must consume the full payload.
pub(crate) fn parse_assoc_addrs(payload: &[u8], addr_count: usize) -> io::Result<Vec<SocketAddr>> {
    // Bound allocation by what the returned payload could physically contain,
    // but let the forward walk retain its exact family/framing error order.
    // Every successful push consumes at least MIN_SCTP_ASSOC_ADDR_LEN bytes,
    // so this capacity is sufficient without allowing malformed counts to
    // request unrelated memory.
    let initial_capacity = assoc_addrs_initial_capacity(addr_count, payload.len());
    let mut addrs = Vec::with_capacity(initial_capacity);
    let mut remaining = payload;

    for _ in 0..addr_count {
        let family = read_u16_at(remaining, 0).map_err(byte_range_invalid_data)?;
        let family = family as libc::sa_family_t as libc::c_int;
        let entry_len = sockaddr_len_for_family(family)?;
        let entry = remaining
            .get(..entry_len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
        addrs.push(parse_assoc_addr_entry(entry, family)?);
        remaining = &remaining[entry_len..];
    }

    if !remaining.is_empty() {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    Ok(addrs)
}

fn byte_range_invalid_data(err: BufferRangeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn parse_assoc_addr_entry(bytes: &[u8], family: libc::c_int) -> io::Result<SocketAddr> {
    match family {
        libc::AF_INET => {
            if bytes.len() >= 8 {
                let port = read_u16_be_at(bytes, 2).map_err(byte_range_invalid_data)?;
                let ip = Ipv4Addr::new(bytes[4], bytes[5], bytes[6], bytes[7]);
                Ok(SocketAddr::from((ip, port)))
            } else {
                Err(io::Error::from(io::ErrorKind::InvalidData))
            }
        }
        libc::AF_INET6 => {
            if bytes.len() < std::mem::size_of::<libc::sockaddr_in6>() {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }

            let port = read_u16_be_at(bytes, 2).map_err(byte_range_invalid_data)?;
            let flowinfo = read_u32_at(bytes, 4).map_err(byte_range_invalid_data)?;
            let ip = Ipv6Addr::from([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
                bytes[22], bytes[23],
            ]);
            let scope_id = read_u32_at(bytes, 24).map_err(byte_range_invalid_data)?;
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => Err(io::Error::from(io::ErrorKind::InvalidData)),
    }
}

const fn cmsg_align(len: usize) -> usize {
    let align = std::mem::size_of::<usize>();
    (len + align - 1) & !(align - 1)
}

const fn cmsg_space(data_len: usize) -> usize {
    cmsg_align(std::mem::size_of::<libc::cmsghdr>()) + cmsg_align(data_len)
}

#[cfg(any(test, feature = "test-support"))]
/// Writes one test control-message header while leaving payload and padding
/// bytes untouched. Panics when the supplied storage cannot hold the fields.
pub(crate) fn write_test_cmsg_header(
    control: &mut [MaybeUninit<u8>],
    offset: usize,
    level: libc::c_int,
    cmsg_type: libc::c_int,
    payload_len: usize,
) -> usize {
    let data_offset = cmsg_align(std::mem::size_of::<libc::cmsghdr>());
    let cmsg_len = data_offset + payload_len;
    let mut write_bytes = |field_offset: usize, bytes: &[u8]| {
        for (slot, byte) in control[field_offset..field_offset + bytes.len()]
            .iter_mut()
            .zip(bytes)
        {
            slot.write(*byte);
        }
    };
    write_bytes(
        offset + std::mem::offset_of!(libc::cmsghdr, cmsg_len),
        &cmsg_len.to_ne_bytes(),
    );
    write_bytes(
        offset + std::mem::offset_of!(libc::cmsghdr, cmsg_level),
        &level.to_ne_bytes(),
    );
    write_bytes(
        offset + std::mem::offset_of!(libc::cmsghdr, cmsg_type),
        &cmsg_type.to_ne_bytes(),
    );
    offset + data_offset
}

#[cfg(any(test, feature = "test-support"))]
/// Appends zero-initialized test storage, then delegates the header layout to
/// [`write_test_cmsg_header`].
pub(crate) fn append_initialized_test_cmsg(
    control: &mut Vec<u8>,
    level: libc::c_int,
    cmsg_type: libc::c_int,
    payload_len: usize,
) -> usize {
    let offset = control.len();
    control.resize(offset + cmsg_space(payload_len), 0);
    // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`, and viewing
    // initialized bytes as potentially uninitialized does not expose an
    // uninitialized value or change the allocation.
    let storage = unsafe {
        std::slice::from_raw_parts_mut(
            control.as_mut_ptr().cast::<MaybeUninit<u8>>(),
            control.len(),
        )
    };
    write_test_cmsg_header(storage, offset, level, cmsg_type, payload_len)
}

fn write_cmsg_sndinfo(control: &mut [u8; SCTP_SNDINFO_CONTROL_LEN], sndinfo: libc::sctp_sndinfo) {
    let hdr = libc::cmsghdr {
        cmsg_len: SCTP_SNDINFO_CMSG_LEN as _,
        cmsg_level: libc::IPPROTO_SCTP,
        cmsg_type: libc::SCTP_SNDINFO,
    };
    unsafe {
        std::ptr::write_unaligned(control.as_mut_ptr() as *mut libc::cmsghdr, hdr);
        let data_ptr = control.as_mut_ptr().add(SCTP_SNDINFO_DATA_OFFSET);
        std::ptr::write_unaligned(data_ptr as *mut libc::sctp_sndinfo, sndinfo);
    }
}

macro_rules! production_receive_invalid_data {
    ($message:literal) => {
        io::Error::new(io::ErrorKind::InvalidData, $message)
    };
}

#[cfg(any(test, feature = "test-support"))]
macro_rules! bare_receive_invalid_data {
    ($message:literal) => {
        io::Error::from(io::ErrorKind::InvalidData)
    };
}

#[cold]
#[inline(never)]
fn missing_notification_preparse() -> io::Error {
    production_receive_invalid_data!("SCTP notification completion was not preparsed")
}

#[cfg(any(test, feature = "test-support"))]
#[cold]
#[inline(never)]
fn missing_notification_preparse_bare() -> io::Error {
    bare_receive_invalid_data!("SCTP notification completion was not preparsed")
}

macro_rules! define_parse_rcvinfo {
    ($(#[$attribute:meta])* $name:ident, $invalid_data:ident) => {
        $(#[$attribute])*
        unsafe fn $name(
            control: &[MaybeUninit<u8>],
            controllen: usize,
            end_of_record: bool,
        ) -> io::Result<Option<SctpRecvInfo>> {
            let hdr_len = std::mem::size_of::<libc::cmsghdr>();
            let min_cmsg_len = cmsg_align(hdr_len);
            let rcvinfo_len = std::mem::size_of::<libc::sctp_rcvinfo>();
            let rcvinfo_cmsg_len = min_cmsg_len + rcvinfo_len;
            let available = controllen.min(control.len());
            let mut offset = 0usize;

            while offset < available {
                let remaining = available - offset;
                if remaining < hdr_len {
                    return Err($invalid_data!(
                        "SCTP recvmsg control message header was malformed"
                    ));
                }

                // SAFETY: the caller supplies the kernel-reported prefix. A complete
                // cmsghdr fits in `remaining`; CMSG alignment padding is never read.
                let hdr = unsafe {
                    std::ptr::read_unaligned(
                        control.as_ptr().add(offset).cast::<libc::cmsghdr>(),
                    )
                };
                let cmsg_len = hdr.cmsg_len as usize;
                if cmsg_len < min_cmsg_len || cmsg_len > remaining {
                    return Err($invalid_data!(
                        "SCTP recvmsg control message length was malformed"
                    ));
                }

                if hdr.cmsg_level == libc::IPPROTO_SCTP && hdr.cmsg_type == libc::SCTP_RCVINFO {
                    if cmsg_len < rcvinfo_cmsg_len {
                        return Err($invalid_data!("SCTP_RCVINFO control message was truncated"));
                    }
                    // The guards above establish
                    // `offset + min_cmsg_len + rcvinfo_len <= available`.
                    let data_offset = offset + min_cmsg_len;

                    // SAFETY: the complete RCVINFO payload is bounded by both this
                    // record and the kernel-reported backing prefix. The ABI permits
                    // an unaligned control buffer, so use an unaligned read.
                    let info = unsafe {
                        std::ptr::read_unaligned(
                            control
                                .as_ptr()
                                .add(data_offset)
                                .cast::<libc::sctp_rcvinfo>(),
                        )
                    };
                    return Ok(Some(SctpRecvInfo {
                        stream_id: info.rcv_sid,
                        ssn: info.rcv_ssn,
                        flags: info.rcv_flags,
                        ppid: u32::from_be(info.rcv_ppid),
                        tsn: info.rcv_tsn,
                        cumtsn: info.rcv_cumtsn,
                        context: info.rcv_context,
                        assoc_id: info.rcv_assoc_id,
                        end_of_record,
                    }));
                }

                // `cmsg_len <= remaining <= control.len()`, so a valid slice bounds
                // the alignment addition below.
                let aligned_len = cmsg_align(cmsg_len);
                if aligned_len > remaining {
                    // A final complete cmsg need not include all trailing CMSG_SPACE
                    // padding. No next header can begin in this suffix.
                    break;
                }
                offset += aligned_len;
            }

            Ok(None)
        }
    };
}

define_parse_rcvinfo!(
    /// Finds the first complete SCTP_RCVINFO in a bounded control-message chain.
    ///
    /// # Safety
    ///
    /// Within `min(controllen, control.len())`, every CMSG header and declared
    /// payload byte reached by the walk must be initialized. Alignment padding may
    /// remain uninitialized.
    parse_rcvinfo,
    production_receive_invalid_data
);

define_parse_rcvinfo!(
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    /// Diagnostic-only bare-error comparator generated from the shipping walk.
    parse_rcvinfo_bare,
    bare_receive_invalid_data
);

#[inline(always)]
fn completion_uses_rcvinfo(has_data: bool, msg_flags: libc::c_int) -> bool {
    (msg_flags & libc::MSG_TRUNC) == 0
        && (!has_data || sctp_msg_end_of_record(msg_flags))
        && !sctp_msg_notification(msg_flags)
}

macro_rules! define_parse_completion_rcvinfo {
    ($(#[$attribute:meta])* $name:ident, $parse_rcvinfo:ident) => {
        $(#[$attribute])*
        unsafe fn $name(
            control: &[MaybeUninit<u8>],
            controllen: usize,
            msg_flags: libc::c_int,
            has_data: bool,
        ) -> io::Result<Option<SctpRecvInfo>> {
            if controllen == 0 || !completion_uses_rcvinfo(has_data, msg_flags) {
                return Ok(None);
            }
            unsafe { $parse_rcvinfo(control, controllen, sctp_msg_end_of_record(msg_flags)) }
        }
    };
}

define_parse_completion_rcvinfo!(
    #[inline(always)]
    /// Parses receive information only when no higher-precedence completion
    /// outcome makes ancillary metadata irrelevant.
    ///
    /// # Safety
    ///
    /// Within `min(controllen, control.len())`, every CMSG header and declared
    /// payload byte reached by ancillary parsing must be initialized. Alignment
    /// padding may remain uninitialized.
    parse_completion_rcvinfo,
    parse_rcvinfo
);

define_parse_completion_rcvinfo!(
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    parse_completion_rcvinfo_bare,
    parse_rcvinfo_bare
);

#[cfg(any(test, feature = "test-support"))]
fn parse_initialized_recv_state_meta_for_test(
    recv_state: &SctpRecvState,
    control: &[u8],
    controllen: usize,
    msg_flags: libc::c_int,
    data_slice: &[u8],
    parsed_notification: Option<io::Result<SctpRecvMeta>>,
) -> io::Result<SctpRecvMeta> {
    // SAFETY: initialized bytes may always be viewed as MaybeUninit bytes.
    let control = unsafe {
        std::slice::from_raw_parts(control.as_ptr().cast::<MaybeUninit<u8>>(), control.len())
    };
    // SAFETY: this test facade accepts fully initialized control bytes.
    let rcvinfo =
        unsafe { parse_completion_rcvinfo(control, controllen, msg_flags, !data_slice.is_empty()) };
    recv_state.parse_completion_meta(rcvinfo, msg_flags, data_slice, parsed_notification)
}

macro_rules! define_parse_recv_meta {
    ($(#[$attribute:meta])* $visibility:vis $name:ident, $with_notification:ident) => {
        $(#[$attribute])*
        $visibility fn $name(
            control: &[u8],
            controllen: usize,
            msg_flags: libc::c_int,
            data_slice: &[u8],
            recv_rcvinfo_requested: bool,
        ) -> io::Result<SctpRecvMeta> {
            let parsed_notification = parse_sctp_notification_once(data_slice, msg_flags);
            // SAFETY: initialized bytes may always be viewed as MaybeUninit bytes.
            let control = unsafe {
                std::slice::from_raw_parts(
                    control.as_ptr().cast::<MaybeUninit<u8>>(),
                    control.len(),
                )
            };
            // SAFETY: this facade accepts a fully initialized control-byte slice.
            unsafe {
                $with_notification(
                    control,
                    controllen,
                    msg_flags,
                    data_slice,
                    recv_rcvinfo_requested,
                    parsed_notification,
                )
            }
        }
    };
}

define_parse_recv_meta!(
    #[cfg(any(feature = "fuzzing", feature = "test-support"))]
    pub(crate) parse_recv_meta,
    parse_recv_meta_with_notification
);

define_parse_recv_meta!(
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    parse_recv_meta_bare,
    parse_recv_meta_with_notification_bare
);

macro_rules! define_parse_recv_meta_with_notification {
    (
        $(#[$attribute:meta])*
        $name:ident,
        $parse_completion:ident,
        $classify:ident
    ) => {
        $(#[$attribute])*
        unsafe fn $name(
            control: &[MaybeUninit<u8>],
            controllen: usize,
            msg_flags: libc::c_int,
            data_slice: &[u8],
            recv_rcvinfo_requested: bool,
            parsed_notification: Option<io::Result<SctpRecvMeta>>,
        ) -> io::Result<SctpRecvMeta> {
            let rcvinfo = unsafe {
                $parse_completion(control, controllen, msg_flags, !data_slice.is_empty())
            };
            $classify(
                rcvinfo,
                msg_flags,
                data_slice,
                recv_rcvinfo_requested,
                parsed_notification,
            )
        }
    };
}

define_parse_recv_meta_with_notification!(
    /// Interprets one completed SCTP metadata receive.
    ///
    /// # Safety
    ///
    /// Within `min(controllen, control.len())`, every CMSG header and declared
    /// payload byte reached by ancillary parsing must be initialized. Alignment
    /// padding may remain uninitialized.
    #[cfg(any(test, feature = "fuzzing", feature = "test-support"))]
    parse_recv_meta_with_notification,
    parse_completion_rcvinfo,
    classify_recv_meta
);

define_parse_recv_meta_with_notification!(
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    parse_recv_meta_with_notification_bare,
    parse_completion_rcvinfo_bare,
    classify_recv_meta_bare
);

macro_rules! define_classify_recv_meta {
    ($(#[$attribute:meta])* $name:ident, $invalid_data:ident, $missing_preparse:ident) => {
        $(#[$attribute])*
        fn $name(
            rcvinfo: io::Result<Option<SctpRecvInfo>>,
            msg_flags: libc::c_int,
            data_slice: &[u8],
            recv_rcvinfo_requested: bool,
            parsed_notification: Option<io::Result<SctpRecvMeta>>,
        ) -> io::Result<SctpRecvMeta> {
            if (msg_flags & libc::MSG_TRUNC) != 0 {
                return Err($invalid_data!("SCTP recvmsg payload was truncated"));
            }

            let end_of_record = (msg_flags & libc::MSG_EOR) != 0;
            if !end_of_record && !data_slice.is_empty() {
                return Err($invalid_data!(
                    "SCTP recvmsg payload was partial before end-of-record"
                ));
            }

            if (msg_flags & libc::MSG_NOTIFICATION) != 0 {
                return match parsed_notification {
                    Some(notification) => notification,
                    None => Err($missing_preparse()),
                };
            }

            match rcvinfo {
                Ok(Some(info)) => {
                    // The subscribed SCTP_RCVINFO cmsg was intact. Linux may still set
                    // MSG_CTRUNC for later control records this API does not consume,
                    // so keep the data path successful.
                    Ok(SctpRecvMeta::Data(info))
                }
                Ok(None) => {
                    if (msg_flags & libc::MSG_CTRUNC) != 0 {
                        Err($invalid_data!(
                            "SCTP recvmsg fixed control buffer capacity was exhausted"
                        ))
                    } else if recv_rcvinfo_requested {
                        Err($invalid_data!(
                            "SCTP recvmsg omitted requested SCTP_RCVINFO"
                        ))
                    } else {
                        Ok(SctpRecvMeta::Data(SctpRecvInfo {
                            end_of_record,
                            ..SctpRecvInfo::default()
                        }))
                    }
                }
                Err(err) => Err(err),
            }
        }
    };
}

define_classify_recv_meta!(
    #[inline(always)]
    classify_recv_meta,
    production_receive_invalid_data,
    missing_notification_preparse
);

define_classify_recv_meta!(
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    classify_recv_meta_bare,
    bare_receive_invalid_data,
    missing_notification_preparse_bare
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShortSctpNotification {
    kind: u16,
    declared_length: u32,
    required_length: usize,
}

impl std::fmt::Display for ShortSctpNotification {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SCTP notification kind {:#06x} declared length {} is shorter than required length {}",
            self.kind, self.declared_length, self.required_length
        )
    }
}

impl std::error::Error for ShortSctpNotification {}

#[inline]
fn short_sctp_notification(kind: u16, declared_length: u32, required_length: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        ShortSctpNotification {
            kind,
            declared_length,
            required_length,
        },
    )
}

fn send_failed_notification(
    flags: u16,
    error: u32,
    info: SctpSendInfo,
    assoc_id: libc::sctp_assoc_t,
) -> SctpNotification {
    SctpNotification::SendFailed {
        flags,
        error,
        info,
        assoc_id,
    }
}

fn send_info_from_sndrcvinfo(info: libc::sctp_sndrcvinfo) -> SctpSendInfo {
    SctpSendInfo {
        stream_id: info.sinfo_stream,
        flags: info.sinfo_flags,
        ppid: u32::from_be(info.sinfo_ppid),
        context: info.sinfo_context,
        assoc_id: info.sinfo_assoc_id,
    }
}

fn send_info_from_sndinfo(info: libc::sctp_sndinfo) -> SctpSendInfo {
    SctpSendInfo {
        stream_id: info.snd_sid,
        flags: info.snd_flags,
        ppid: u32::from_be(info.snd_ppid),
        context: info.snd_context,
        assoc_id: info.snd_assoc_id,
    }
}

// Linux UAPI notification layouts. These constants deliberately describe the
// byte protocol consumed below instead of shadowing kernel structs with
// `repr(C)` types. The compile-time relationships keep field ends and minimum
// record lengths synchronized while the parser retains checked byte reads and
// unaligned access.
const SCTP_NOTIFICATION_TYPE_OFFSET: usize = 0;
const SCTP_NOTIFICATION_FLAGS_OFFSET: usize = SCTP_NOTIFICATION_TYPE_OFFSET + size_of::<u16>();
const SCTP_NOTIFICATION_LENGTH_OFFSET: usize = SCTP_NOTIFICATION_FLAGS_OFFSET + size_of::<u16>();
const SCTP_NOTIFICATION_HEADER_LEN: usize = SCTP_NOTIFICATION_LENGTH_OFFSET + size_of::<u32>();

const SCTP_ASSOC_CHANGE_STATE_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_ASSOC_CHANGE_ERROR_OFFSET: usize = SCTP_ASSOC_CHANGE_STATE_OFFSET + size_of::<u16>();
const SCTP_ASSOC_CHANGE_OUTBOUND_STREAMS_OFFSET: usize =
    SCTP_ASSOC_CHANGE_ERROR_OFFSET + size_of::<u16>();
const SCTP_ASSOC_CHANGE_INBOUND_STREAMS_OFFSET: usize =
    SCTP_ASSOC_CHANGE_OUTBOUND_STREAMS_OFFSET + size_of::<u16>();
const SCTP_ASSOC_CHANGE_ASSOC_ID_OFFSET: usize =
    SCTP_ASSOC_CHANGE_INBOUND_STREAMS_OFFSET + size_of::<u16>();
const SCTP_ASSOC_CHANGE_MIN_LEN: usize =
    SCTP_ASSOC_CHANGE_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_PEER_ADDR_CHANGE_ADDRESS_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_PEER_ADDR_CHANGE_ADDRESS_LEN: usize = size_of::<libc::sockaddr_storage>();
const SCTP_PEER_ADDR_CHANGE_STATE_OFFSET: usize =
    SCTP_PEER_ADDR_CHANGE_ADDRESS_OFFSET + SCTP_PEER_ADDR_CHANGE_ADDRESS_LEN;
const SCTP_PEER_ADDR_CHANGE_ERROR_OFFSET: usize =
    SCTP_PEER_ADDR_CHANGE_STATE_OFFSET + size_of::<i32>();
const SCTP_PEER_ADDR_CHANGE_ASSOC_ID_OFFSET: usize =
    SCTP_PEER_ADDR_CHANGE_ERROR_OFFSET + size_of::<i32>();
const SCTP_PEER_ADDR_CHANGE_MIN_LEN: usize =
    SCTP_PEER_ADDR_CHANGE_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_SEND_FAILED_ERROR_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_SEND_FAILED_INFO_OFFSET: usize = SCTP_SEND_FAILED_ERROR_OFFSET + size_of::<u32>();
const SCTP_LEGACY_SEND_FAILED_ASSOC_ID_OFFSET: usize =
    SCTP_SEND_FAILED_INFO_OFFSET + size_of::<libc::sctp_sndrcvinfo>();
const SCTP_LEGACY_SEND_FAILED_MIN_LEN: usize =
    SCTP_LEGACY_SEND_FAILED_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();
const SCTP_SEND_FAILED_EVENT_ASSOC_ID_OFFSET: usize =
    SCTP_SEND_FAILED_INFO_OFFSET + size_of::<libc::sctp_sndinfo>();
const SCTP_SEND_FAILED_EVENT_MIN_LEN: usize =
    SCTP_SEND_FAILED_EVENT_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_REMOTE_ERROR_ERROR_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_REMOTE_ERROR_ASSOC_ID_PADDING_LEN: usize = size_of::<u16>();
const SCTP_REMOTE_ERROR_ASSOC_ID_OFFSET: usize =
    SCTP_REMOTE_ERROR_ERROR_OFFSET + size_of::<u16>() + SCTP_REMOTE_ERROR_ASSOC_ID_PADDING_LEN;
const SCTP_REMOTE_ERROR_MIN_LEN: usize =
    SCTP_REMOTE_ERROR_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_SHUTDOWN_ASSOC_ID_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_SHUTDOWN_MIN_LEN: usize =
    SCTP_SHUTDOWN_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_ADAPTATION_INDICATION_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_ADAPTATION_ASSOC_ID_OFFSET: usize = SCTP_ADAPTATION_INDICATION_OFFSET + size_of::<u32>();
const SCTP_ADAPTATION_MIN_LEN: usize =
    SCTP_ADAPTATION_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_AUTHENTICATION_KEY_NUMBER_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_AUTHENTICATION_ALTERNATE_KEY_NUMBER_OFFSET: usize =
    SCTP_AUTHENTICATION_KEY_NUMBER_OFFSET + size_of::<u16>();
const SCTP_AUTHENTICATION_INDICATION_OFFSET: usize =
    SCTP_AUTHENTICATION_ALTERNATE_KEY_NUMBER_OFFSET + size_of::<u16>();
const SCTP_AUTHENTICATION_ASSOC_ID_OFFSET: usize =
    SCTP_AUTHENTICATION_INDICATION_OFFSET + size_of::<u32>();
const SCTP_AUTHENTICATION_MIN_LEN: usize =
    SCTP_AUTHENTICATION_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_PARTIAL_DELIVERY_INDICATION_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_PARTIAL_DELIVERY_ASSOC_ID_OFFSET: usize =
    SCTP_PARTIAL_DELIVERY_INDICATION_OFFSET + size_of::<u32>();
const SCTP_PARTIAL_DELIVERY_STREAM_OFFSET: usize =
    SCTP_PARTIAL_DELIVERY_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();
const SCTP_PARTIAL_DELIVERY_SEQUENCE_OFFSET: usize =
    SCTP_PARTIAL_DELIVERY_STREAM_OFFSET + size_of::<u32>();
const SCTP_PARTIAL_DELIVERY_MIN_LEN: usize =
    SCTP_PARTIAL_DELIVERY_SEQUENCE_OFFSET + size_of::<u32>();

const SCTP_SENDER_DRY_ASSOC_ID_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_SENDER_DRY_MIN_LEN: usize =
    SCTP_SENDER_DRY_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_STREAM_RESET_ASSOC_ID_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_STREAM_RESET_MIN_LEN: usize =
    SCTP_STREAM_RESET_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();

const SCTP_ASSOC_RESET_ASSOC_ID_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_ASSOC_RESET_LOCAL_TSN_OFFSET: usize =
    SCTP_ASSOC_RESET_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();
const SCTP_ASSOC_RESET_REMOTE_TSN_OFFSET: usize =
    SCTP_ASSOC_RESET_LOCAL_TSN_OFFSET + size_of::<u32>();
const SCTP_ASSOC_RESET_MIN_LEN: usize = SCTP_ASSOC_RESET_REMOTE_TSN_OFFSET + size_of::<u32>();

const SCTP_STREAM_CHANGE_ASSOC_ID_OFFSET: usize = SCTP_NOTIFICATION_HEADER_LEN;
const SCTP_STREAM_CHANGE_INBOUND_STREAMS_OFFSET: usize =
    SCTP_STREAM_CHANGE_ASSOC_ID_OFFSET + size_of::<libc::sctp_assoc_t>();
const SCTP_STREAM_CHANGE_OUTBOUND_STREAMS_OFFSET: usize =
    SCTP_STREAM_CHANGE_INBOUND_STREAMS_OFFSET + size_of::<u16>();
const SCTP_STREAM_CHANGE_MIN_LEN: usize =
    SCTP_STREAM_CHANGE_OUTBOUND_STREAMS_OFFSET + size_of::<u16>();

const _: () = {
    assert!(size_of::<libc::sctp_assoc_t>() == size_of::<i32>());
};

#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    target_pointer_width = "64"
))]
const _: () = {
    assert!(size_of::<libc::sockaddr_storage>() == 128);
    assert!(size_of::<libc::sctp_assoc_t>() == 4);
    assert!(size_of::<libc::sctp_sndrcvinfo>() == 32);
    assert!(size_of::<libc::sctp_sndinfo>() == 16);
    assert!(SCTP_NOTIFICATION_FLAGS_OFFSET == 2);
    assert!(SCTP_NOTIFICATION_LENGTH_OFFSET == 4);
    assert!(SCTP_NOTIFICATION_HEADER_LEN == 8);
    assert!(SCTP_SEND_FAILED_INFO_OFFSET == 12);
    assert!(SCTP_REMOTE_ERROR_ASSOC_ID_OFFSET == 12);
    assert!(SCTP_ASSOC_CHANGE_MIN_LEN == 20);
    assert!(SCTP_PEER_ADDR_CHANGE_MIN_LEN == 148);
    assert!(SCTP_LEGACY_SEND_FAILED_MIN_LEN == 48);
    assert!(SCTP_SEND_FAILED_EVENT_MIN_LEN == 32);
    assert!(SCTP_REMOTE_ERROR_MIN_LEN == 16);
    assert!(SCTP_SHUTDOWN_MIN_LEN == 12);
    assert!(SCTP_ADAPTATION_MIN_LEN == 16);
    assert!(SCTP_AUTHENTICATION_KEY_NUMBER_OFFSET == 8);
    assert!(SCTP_AUTHENTICATION_ALTERNATE_KEY_NUMBER_OFFSET == 10);
    assert!(SCTP_AUTHENTICATION_INDICATION_OFFSET == 12);
    assert!(SCTP_AUTHENTICATION_ASSOC_ID_OFFSET == 16);
    assert!(SCTP_AUTHENTICATION_MIN_LEN == 20);
    assert!(LOCAL_SCTP_AUTHENTICATION_EVENT == 0x8008);
    assert!(SCTP_PARTIAL_DELIVERY_MIN_LEN == 24);
    assert!(SCTP_SENDER_DRY_MIN_LEN == 12);
    assert!(SCTP_STREAM_RESET_MIN_LEN == 12);
    assert!(SCTP_ASSOC_RESET_MIN_LEN == 20);
    assert!(SCTP_STREAM_CHANGE_MIN_LEN == 16);
};

fn parse_legacy_send_failed_notification(
    buffer: &[u8],
    flags: u16,
    declared_length: u32,
) -> io::Result<SctpNotification> {
    if buffer.len() < SCTP_LEGACY_SEND_FAILED_MIN_LEN {
        return Err(short_sctp_notification(
            LOCAL_SCTP_SEND_FAILED as u16,
            declared_length,
            SCTP_LEGACY_SEND_FAILED_MIN_LEN,
        ));
    }

    let error =
        read_u32_at(buffer, SCTP_SEND_FAILED_ERROR_OFFSET).map_err(byte_range_invalid_data)?;
    let sndrcvinfo_ptr = unsafe {
        buffer.as_ptr().add(SCTP_SEND_FAILED_INFO_OFFSET) as *const libc::sctp_sndrcvinfo
    };
    let sndrcvinfo = unsafe { std::ptr::read_unaligned(sndrcvinfo_ptr) };
    let assoc_id = read_i32_at(buffer, SCTP_LEGACY_SEND_FAILED_ASSOC_ID_OFFSET)
        .map_err(byte_range_invalid_data)?;
    Ok(send_failed_notification(
        flags,
        error,
        send_info_from_sndrcvinfo(sndrcvinfo),
        assoc_id,
    ))
}

fn parse_send_failed_event_notification(
    buffer: &[u8],
    flags: u16,
    declared_length: u32,
) -> io::Result<SctpNotification> {
    if buffer.len() < SCTP_SEND_FAILED_EVENT_MIN_LEN {
        return Err(short_sctp_notification(
            LOCAL_SCTP_SEND_FAILED_EVENT as u16,
            declared_length,
            SCTP_SEND_FAILED_EVENT_MIN_LEN,
        ));
    }

    let error =
        read_u32_at(buffer, SCTP_SEND_FAILED_ERROR_OFFSET).map_err(byte_range_invalid_data)?;
    let sndinfo_ptr =
        unsafe { buffer.as_ptr().add(SCTP_SEND_FAILED_INFO_OFFSET) as *const libc::sctp_sndinfo };
    let sndinfo = unsafe { std::ptr::read_unaligned(sndinfo_ptr) };
    let assoc_id = read_i32_at(buffer, SCTP_SEND_FAILED_EVENT_ASSOC_ID_OFFSET)
        .map_err(byte_range_invalid_data)?;
    Ok(send_failed_notification(
        flags,
        error,
        send_info_from_sndinfo(sndinfo),
        assoc_id,
    ))
}

pub(crate) fn parse_notification(buffer: &[u8]) -> io::Result<SctpRecvMeta> {
    if buffer.len() < SCTP_NOTIFICATION_HEADER_LEN {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let sn_type =
        read_u16_at(buffer, SCTP_NOTIFICATION_TYPE_OFFSET).map_err(byte_range_invalid_data)?;
    let sn_flags =
        read_u16_at(buffer, SCTP_NOTIFICATION_FLAGS_OFFSET).map_err(byte_range_invalid_data)?;
    let sn_length =
        read_u32_at(buffer, SCTP_NOTIFICATION_LENGTH_OFFSET).map_err(byte_range_invalid_data)?;
    if sn_length < SCTP_NOTIFICATION_HEADER_LEN as u32 || sn_length as usize > buffer.len() {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let buffer = &buffer[..sn_length as usize];

    let notification = match sn_type as libc::c_int {
        x if x == LOCAL_SCTP_ASSOC_CHANGE => {
            if buffer.len() < SCTP_ASSOC_CHANGE_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_ASSOC_CHANGE_MIN_LEN,
                ));
            }
            SctpNotification::AssocChange {
                state: read_u16_at(buffer, SCTP_ASSOC_CHANGE_STATE_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                error: read_u16_at(buffer, SCTP_ASSOC_CHANGE_ERROR_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                outbound_streams: read_u16_at(buffer, SCTP_ASSOC_CHANGE_OUTBOUND_STREAMS_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                inbound_streams: read_u16_at(buffer, SCTP_ASSOC_CHANGE_INBOUND_STREAMS_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, SCTP_ASSOC_CHANGE_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_PEER_ADDR_CHANGE => {
            if buffer.len() < SCTP_PEER_ADDR_CHANGE_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_PEER_ADDR_CHANGE_MIN_LEN,
                ));
            }

            let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buffer.as_ptr().add(SCTP_PEER_ADDR_CHANGE_ADDRESS_OFFSET),
                    &mut storage as *mut _ as *mut u8,
                    SCTP_PEER_ADDR_CHANGE_ADDRESS_LEN,
                );
            }
            let addr = socket_addr_from_c(
                &storage,
                SCTP_PEER_ADDR_CHANGE_ADDRESS_LEN as libc::socklen_t,
            )?;
            SctpNotification::PeerAddrChange {
                addr,
                state: read_i32_at(buffer, SCTP_PEER_ADDR_CHANGE_STATE_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                error: read_i32_at(buffer, SCTP_PEER_ADDR_CHANGE_ERROR_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, SCTP_PEER_ADDR_CHANGE_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        // Defensive only: FlowIO configures sctp_send_failure_event=0, but
        // untrusted/test notification bytes can still exercise this legacy layout.
        x if x == LOCAL_SCTP_SEND_FAILED => {
            parse_legacy_send_failed_notification(buffer, sn_flags, sn_length)?
        }
        x if x == LOCAL_SCTP_REMOTE_ERROR => {
            if buffer.len() < SCTP_REMOTE_ERROR_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_REMOTE_ERROR_MIN_LEN,
                ));
            }
            SctpNotification::RemoteError {
                error: read_u16_be_at(buffer, SCTP_REMOTE_ERROR_ERROR_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, SCTP_REMOTE_ERROR_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_SHUTDOWN_EVENT => {
            if buffer.len() < SCTP_SHUTDOWN_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_SHUTDOWN_MIN_LEN,
                ));
            }
            SctpNotification::Shutdown {
                assoc_id: read_i32_at(buffer, SCTP_SHUTDOWN_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_ADAPTATION_INDICATION => {
            if buffer.len() < SCTP_ADAPTATION_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_ADAPTATION_MIN_LEN,
                ));
            }
            SctpNotification::Adaptation {
                indication: read_u32_at(buffer, SCTP_ADAPTATION_INDICATION_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, SCTP_ADAPTATION_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_AUTHENTICATION_EVENT => {
            if buffer.len() < SCTP_AUTHENTICATION_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_AUTHENTICATION_MIN_LEN,
                ));
            }
            SctpNotification::Authentication {
                flags: sn_flags,
                key_number: read_u16_at(buffer, SCTP_AUTHENTICATION_KEY_NUMBER_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                alternate_key_number: read_u16_at(
                    buffer,
                    SCTP_AUTHENTICATION_ALTERNATE_KEY_NUMBER_OFFSET,
                )
                .map_err(byte_range_invalid_data)?,
                indication: read_u32_at(buffer, SCTP_AUTHENTICATION_INDICATION_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, SCTP_AUTHENTICATION_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_PARTIAL_DELIVERY_EVENT => {
            if buffer.len() < SCTP_PARTIAL_DELIVERY_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_PARTIAL_DELIVERY_MIN_LEN,
                ));
            }
            SctpNotification::PartialDelivery {
                indication: read_u32_at(buffer, SCTP_PARTIAL_DELIVERY_INDICATION_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, SCTP_PARTIAL_DELIVERY_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                stream: read_u32_at(buffer, SCTP_PARTIAL_DELIVERY_STREAM_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                sequence: read_u32_at(buffer, SCTP_PARTIAL_DELIVERY_SEQUENCE_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_SENDER_DRY_EVENT => {
            if buffer.len() < SCTP_SENDER_DRY_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_SENDER_DRY_MIN_LEN,
                ));
            }
            SctpNotification::SenderDry {
                assoc_id: read_i32_at(buffer, SCTP_SENDER_DRY_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_STREAM_RESET_EVENT => {
            if buffer.len() < SCTP_STREAM_RESET_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_STREAM_RESET_MIN_LEN,
                ));
            }
            SctpNotification::StreamReset {
                flags: sn_flags,
                assoc_id: read_i32_at(buffer, SCTP_STREAM_RESET_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_ASSOC_RESET_EVENT => {
            if buffer.len() < SCTP_ASSOC_RESET_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_ASSOC_RESET_MIN_LEN,
                ));
            }
            SctpNotification::AssocReset {
                flags: sn_flags,
                assoc_id: read_i32_at(buffer, SCTP_ASSOC_RESET_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                local_tsn: read_u32_at(buffer, SCTP_ASSOC_RESET_LOCAL_TSN_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                remote_tsn: read_u32_at(buffer, SCTP_ASSOC_RESET_REMOTE_TSN_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_STREAM_CHANGE_EVENT => {
            if buffer.len() < SCTP_STREAM_CHANGE_MIN_LEN {
                return Err(short_sctp_notification(
                    sn_type,
                    sn_length,
                    SCTP_STREAM_CHANGE_MIN_LEN,
                ));
            }
            SctpNotification::StreamChange {
                flags: sn_flags,
                assoc_id: read_i32_at(buffer, SCTP_STREAM_CHANGE_ASSOC_ID_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                inbound_streams: read_u16_at(buffer, SCTP_STREAM_CHANGE_INBOUND_STREAMS_OFFSET)
                    .map_err(byte_range_invalid_data)?,
                outbound_streams: read_u16_at(buffer, SCTP_STREAM_CHANGE_OUTBOUND_STREAMS_OFFSET)
                    .map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_SEND_FAILED_EVENT => {
            parse_send_failed_event_notification(buffer, sn_flags, sn_length)?
        }
        _ => SctpNotification::Other {
            kind: sn_type,
            flags: sn_flags,
            length: sn_length,
        },
    };

    Ok(SctpRecvMeta::Notification(notification))
}

const LOCAL_SCTP_ASSOC_CHANGE: libc::c_int = local_sctp_notification_type(1);
const LOCAL_SCTP_PEER_ADDR_CHANGE: libc::c_int = local_sctp_notification_type(2);
const LOCAL_SCTP_SEND_FAILED: libc::c_int = local_sctp_notification_type(3);
const LOCAL_SCTP_REMOTE_ERROR: libc::c_int = local_sctp_notification_type(4);
const LOCAL_SCTP_SHUTDOWN_EVENT: libc::c_int = local_sctp_notification_type(5);
const LOCAL_SCTP_PARTIAL_DELIVERY_EVENT: libc::c_int = local_sctp_notification_type(6);
const LOCAL_SCTP_ADAPTATION_INDICATION: libc::c_int = local_sctp_notification_type(7);
const LOCAL_SCTP_AUTHENTICATION_EVENT: libc::c_int = local_sctp_notification_type(8);
const LOCAL_SCTP_SENDER_DRY_EVENT: libc::c_int = local_sctp_notification_type(9);
const LOCAL_SCTP_STREAM_RESET_EVENT: libc::c_int = local_sctp_notification_type(10);
const LOCAL_SCTP_ASSOC_RESET_EVENT: libc::c_int = local_sctp_notification_type(11);
const LOCAL_SCTP_STREAM_CHANGE_EVENT: libc::c_int = local_sctp_notification_type(12);
const LOCAL_SCTP_SEND_FAILED_EVENT: libc::c_int = local_sctp_notification_type(13);

const fn local_sctp_notification_type(index: libc::c_int) -> libc::c_int {
    (1 << 15) | index
}

#[cfg(feature = "test-support")]
pub(crate) mod test_support {
    use super::*;

    /// Effective SCTP socket state used by live inheritance tests.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct SctpSocketOptionSnapshot {
        /// Effective `SCTP_EVENTS` subscription mask.
        pub notifications: SctpNotificationMask,
        /// Whether `SCTP_RECVRCVINFO` is enabled.
        pub recv_rcvinfo: bool,
        /// Whether `SCTP_NODELAY` is enabled.
        pub nodelay: bool,
        /// Effective `SO_SNDBUF` value.
        pub send_buffer_size: usize,
        /// Effective `SO_RCVBUF` value.
        pub recv_buffer_size: usize,
        /// Effective one-to-one `SCTP_DEFAULT_SNDINFO` metadata.
        pub default_send_info: SctpSendInfo,
        /// Linux `SO_BUF_LOCK` bitmask (`SOCK_SNDBUF_LOCK` and
        /// `SOCK_RCVBUF_LOCK`) when the running kernel exposes it.
        pub buffer_locks: Option<libc::c_int>,
    }

    /// Feature-gated snapshot of the stream-owned dropped-receive lifecycle.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SctpStashedRecvStateSnapshot {
        /// No dropped metadata receive is retained.
        Empty,
        /// One live in-flight or completed metadata receive is retained.
        Live,
        /// The origin ring was abandoned and only an opaque marker remains.
        Abandoned,
    }

    /// Feature-gated snapshot of metadata receive record synchronization.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SctpRecordRecoverySnapshot {
        /// The next completion begins at a record boundary.
        Synced,
        /// An unpublished data record still has bytes to discard.
        DataTail,
        /// An unpublished notification record still has opaque tail bytes.
        NotificationTail,
        /// A notification interrupts a data tail and retains only its bounded
        /// classifier prefix.
        DataNotificationTail {
            /// Number of retained prefix bytes, never greater than 24.
            prefix_len: usize,
            /// Whether the full prefix was already classified as non-abort.
            classified: bool,
        },
    }

    /// Returns whether a general SCTP capability probe may be treated as
    /// unavailable rather than as a test or benchmark failure.
    ///
    /// This policy is intentionally narrower than option-specific SCTP
    /// feature probing. Errors such as `EINVAL` and `ENOPROTOOPT` must remain
    /// visible to catch invalid probe setup and unsupported socket options
    /// separately.
    pub fn capability_unavailable(err: &io::Error) -> bool {
        matches!(
            err.raw_os_error(),
            Some(libc::EPROTONOSUPPORT)
                | Some(libc::ESOCKTNOSUPPORT)
                | Some(libc::EAFNOSUPPORT)
                | Some(libc::EPFNOSUPPORT)
                | Some(libc::EPERM)
                | Some(libc::EACCES)
        )
    }

    /// Constructs the exact post-`accept4` SCTP stream owner without requiring
    /// a live SCTP association.
    ///
    /// This deterministic allocation oracle consumes the supplied sole fd
    /// owner, routes it through the maintained accept-result descriptor-core
    /// seam, and deliberately replaces kernel established-socket configuration
    /// with a no-op. It is available only through the test-support feature.
    pub fn test_construct_sctp_accept_result(
        accepted_fd: OwnedFd,
        remote_addr: SocketAddr,
    ) -> io::Result<SctpStream> {
        let (addr, addrlen) = socket_addr_to_c(remote_addr);
        finish_accepted_owned_stream_with(
            accepted_fd,
            LingerProvenance::KnownNonPositive,
            &addr,
            addrlen,
            SctpSocketConfig::data(SctpInitConfig::default()),
            |_fd, _config, _provenance| Ok(()),
        )
        .map(|(stream, _remote_addr)| stream)
    }

    /// Constructs the exact successful SCTP connect-result owner without
    /// requiring a live SCTP association.
    ///
    /// This deterministic allocation oracle consumes the supplied sole fd
    /// owner at the maintained post-configuration connect-result seam. It is
    /// available only through the test-support feature.
    pub fn test_construct_sctp_connect_result(
        connected_fd: OwnedFd,
        remote_addr: SocketAddr,
    ) -> SctpStream {
        finish_connected_runtime_stream(
            connected_fd,
            remote_addr,
            SctpSocketConfig::data(SctpInitConfig::default()),
        )
    }

    /// Reads the effective Linux SCTP receive-notification options for tests.
    pub fn test_sctp_socket_receive_options(fd: RawFd) -> io::Result<(SctpNotificationMask, bool)> {
        let events: SctpEventSubscribe = get_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_EVENTS)?;
        let recv_rcvinfo: libc::c_int =
            get_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_RECVRCVINFO)?;
        Ok((events.notification_mask(), recv_rcvinfo != 0))
    }

    /// Reads all listener-inherited SCTP socket settings and Linux buffer
    /// lock state for tests.
    pub fn test_sctp_socket_options(fd: RawFd) -> io::Result<SctpSocketOptionSnapshot> {
        let (notifications, recv_rcvinfo) = test_sctp_socket_receive_options(fd)?;
        let nodelay: libc::c_int = get_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_NODELAY)?;
        let buffer_locks = match get_sock_opt(fd, libc::SOL_SOCKET, libc::SO_BUF_LOCK) {
            Ok(value) => Some(value),
            Err(err) if err.raw_os_error() == Some(libc::ENOPROTOOPT) => None,
            Err(err) => return Err(err),
        };

        // `sctp_sndinfo` does not implement Default, and snd_assoc_id is an
        // input selector for getsockopt. Query the one-to-one default with
        // selector zero and normalize that selector in the returned snapshot.
        let raw_send_info = get_sctp_opt_exact(
            fd,
            libc::SCTP_DEFAULT_SNDINFO,
            libc::sctp_sndinfo {
                snd_sid: 0,
                snd_flags: 0,
                snd_ppid: 0,
                snd_context: 0,
                snd_assoc_id: 0,
            },
        )?;
        let mut default_send_info = send_info_from_sndinfo(raw_send_info);
        default_send_info.assoc_id = 0;

        Ok(SctpSocketOptionSnapshot {
            notifications,
            recv_rcvinfo,
            nodelay: nodelay != 0,
            send_buffer_size: crate::net::sock_send_buffer_size(fd)?,
            recv_buffer_size: crate::net::sock_recv_buffer_size(fd)?,
            default_send_info,
            buffer_locks,
        })
    }

    /// Applies socket-level SCTP options through an exposed listener fd to
    /// simulate a caller mutation in integration tests.
    pub fn test_apply_sctp_socket_options(fd: RawFd, config: SctpSocketConfig) -> io::Result<()> {
        apply_sctp_socket_options(fd, config.socket_options())
    }

    /// Injects one request-scoped notification-mask failure after the live
    /// `SCTP_RECVRCVINFO` query and returns the effective mask it observed.
    pub fn test_fail_notification_mask_after_query(
        stream: &SctpStream,
        mask: SctpNotificationMask,
        errno: libc::c_int,
    ) -> (io::Result<()>, Option<SctpNotificationMask>) {
        let mut observed = None;
        let result = stream.set_notification_mask_with(mask, |effective| {
            observed = Some(effective);
            Err(io::Error::from_raw_os_error(errno))
        });
        (result, observed)
    }

    /// Accepts one association while injecting a request-scoped established-
    /// configuration error, and returns how many times configuration ran.
    pub async fn test_accept_with_established_config_error(
        listener: &mut SctpListener,
        errno: libc::c_int,
    ) -> (io::Result<(SctpStream, SocketAddr)>, usize) {
        let mut accept = listener.accept();
        let mut configure_calls = 0_usize;
        let result = std::future::poll_fn(|cx| {
            Pin::new(&mut accept).poll_with_established_config(cx, |_fd, _config, _provenance| {
                configure_calls += 1;
                Err(io::Error::from_raw_os_error(errno))
            })
        })
        .await;
        (result, configure_calls)
    }

    /// Returns the stream's stored receive-policy flags for integration tests.
    pub fn test_sctp_stream_receive_policy(stream: &SctpStream) -> (bool, bool, bool) {
        (
            stream.recv_state.recv_rcvinfo_requested.get(),
            stream.recv_state.partial_delivery_visible.get(),
            stream.recv_state.any_notification_visible.get(),
        )
    }

    /// Returns the stream's explicit dropped-receive lifecycle without
    /// exposing or following its operation pointer.
    pub fn test_sctp_stream_stashed_recv_state(
        stream: &SctpStream,
    ) -> SctpStashedRecvStateSnapshot {
        match stream.recv_state.stashed_state {
            StashedSctpRecvState::Empty => SctpStashedRecvStateSnapshot::Empty,
            StashedSctpRecvState::Live => SctpStashedRecvStateSnapshot::Live,
            StashedSctpRecvState::Abandoned => SctpStashedRecvStateSnapshot::Abandoned,
        }
    }

    /// Starts deterministic data-tail recovery for integration tests without
    /// requiring the kernel to trigger partial delivery.
    pub fn test_sctp_stream_begin_data_tail(stream: &mut SctpStream) {
        stream.recv_state.set_record_sync(SctpRecordSync::DataTail);
    }

    /// Applies one successful unpublished completion through the production
    /// classifier and returns its bounded record-recovery state.
    pub fn test_sctp_stream_apply_unpublished_completion(
        stream: &mut SctpStream,
        data: &[u8],
        msg_flags: libc::c_int,
    ) -> SctpRecordRecoverySnapshot {
        let header = SctpRecvHeader {
            msg_controllen: 0,
            msg_flags,
        };
        let recovery_target = stream
            .recv_state
            .bounded_recovery_prefix_target(data.len(), msg_flags);
        let recovery_prefix = &data[..recovery_target];
        let action = stream.recv_state.process_metadata_completion(
            data.len(),
            header,
            data,
            recovery_prefix,
            SctpCompletionPublication::Unpublished,
        );
        debug_assert!(matches!(action, SctpMetadataCompletion::Consume));
        test_sctp_stream_record_recovery(&stream.recv_state)
    }

    fn test_sctp_stream_record_recovery(recv_state: &SctpRecvState) -> SctpRecordRecoverySnapshot {
        match recv_state.record_sync {
            SctpRecordSync::Synced => SctpRecordRecoverySnapshot::Synced,
            SctpRecordSync::DataTail => SctpRecordRecoverySnapshot::DataTail,
            SctpRecordSync::NotificationTail => SctpRecordRecoverySnapshot::NotificationTail,
            SctpRecordSync::DataNotificationTail => {
                SctpRecordRecoverySnapshot::DataNotificationTail {
                    prefix_len: recv_state.nested_prefix_len(),
                    classified: (recv_state.nested_prefix_state & SCTP_NESTED_PREFIX_CLASSIFIED)
                        != 0,
                }
            }
        }
    }

    fn test_accept_slot_drop_preserves_readiness_mask(cached: bool) -> io::Result<()> {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
        let mut state = CompletionState::empty();
        state.result = fd;
        state.set_completed();

        let listener_fd = RuntimeFd::from_fresh_raw_fd(fd);
        let mut slot = AcceptSlot::new(&listener_fd);
        slot.in_use = true;
        slot.state_ptr = &mut state;

        if cached {
            slot.drop_cached_state();
        } else {
            slot.drop_future();
        }

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

    /// Verifies future drop releases completed readiness state without
    /// interpreting its readiness mask as a descriptor.
    pub fn test_accept_slot_drop_future_preserves_unrelated_fd() -> io::Result<()> {
        test_accept_slot_drop_preserves_readiness_mask(false)
    }

    /// Verifies forgotten-future listener teardown has the same
    /// readiness-only ownership behavior.
    pub fn test_accept_slot_drop_cached_state_preserves_unrelated_fd() -> io::Result<()> {
        test_accept_slot_drop_preserves_readiness_mask(true)
    }

    /// Verifies dropping a connector future closes the socket owned by its
    /// reusable connection slot.
    pub fn test_connect_slot_drop_future_closes_socket_fd() -> io::Result<()> {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
        let mut slot = ConnectSlot::new(SctpSocketConfig::default());
        slot.in_use = true;
        // SAFETY: the test-created descriptor has no other owner.
        slot.fd = Some(unsafe { OwnedFd::from_raw_fd(fd) });
        slot.addr = Some(RetainedConnectAddr::from_socket_addr(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));

        slot.drop_future();

        if !slot.state_ptr.is_null() || slot.in_use || slot.fd.is_some() || slot.addr.is_some() {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        if !crate::runtime::fd::raw_fd_is_closed(fd) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        Ok(())
    }

    /// Verifies connector teardown closes a prepared, not-yet-submitted socket
    /// and releases every field owned directly by its reusable slot.
    pub fn test_connect_slot_drop_cached_state_closes_socket_fd() -> io::Result<()> {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
        let mut slot = ConnectSlot::new(SctpSocketConfig::default());
        slot.in_use = true;
        // SAFETY: the test-created descriptor has no other owner.
        slot.fd = Some(unsafe { OwnedFd::from_raw_fd(fd) });
        slot.addr = Some(RetainedConnectAddr::from_socket_addr(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));

        slot.drop_cached_state();

        if !slot.state_ptr.is_null() || slot.in_use || slot.fd.is_some() || slot.addr.is_some() {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        if !crate::runtime::fd::raw_fd_is_closed(fd) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        Ok(())
    }

    /// Verifies a peer-address-parameter socket-option length is rejected as
    /// invalid data rather than parsed through an incompatible ABI layout.
    pub fn test_peer_addr_params_rejects_optlen(optlen: usize) -> io::Result<()> {
        let buffer = [0u8; SCTP_PADDR_PARAMS_RAW_OPT_LEN];
        match decode_peer_addr_params_sockopt(&buffer, optlen) {
            Err(err) if err.kind() == io::ErrorKind::InvalidData => Ok(()),
            Err(err) => Err(err),
            Ok(_) => Err(io::Error::other(
                "invalid SCTP_PEER_ADDR_PARAMS optlen was accepted",
            )),
        }
    }

    /// Runs the production SCTP notification decoder for integration tests.
    pub fn test_parse_notification(buffer: &[u8]) -> io::Result<SctpRecvMeta> {
        parse_notification(buffer)
    }

    /// Runs production ancillary-data and notification receive decoding for
    /// integration tests and fuzz-adjacent fixtures.
    pub fn test_parse_recv_meta(
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
    ) -> io::Result<SctpRecvMeta> {
        parse_recv_meta(control, controllen, msg_flags, data_slice, false)
    }

    /// Runs production SCTP ancillary-data classification with an explicit
    /// caller-requested `SCTP_RCVINFO` policy for diagnostic observers.
    pub fn test_parse_recv_meta_with_policy(
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
        recv_rcvinfo_requested: bool,
    ) -> io::Result<SctpRecvMeta> {
        parse_recv_meta(
            control,
            controllen,
            msg_flags,
            data_slice,
            recv_rcvinfo_requested,
        )
    }

    /// Runs the same SCTP ancillary-data classification branches with the
    /// diagnostic-only bare-`InvalidData` comparator.
    pub fn test_parse_recv_meta_bare_with_policy(
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
        recv_rcvinfo_requested: bool,
    ) -> io::Result<SctpRecvMeta> {
        parse_recv_meta_bare(
            control,
            controllen,
            msg_flags,
            data_slice,
            recv_rcvinfo_requested,
        )
    }

    /// Appends one zero-initialized control-message fixture using the
    /// crate's canonical test header layout.
    pub fn append_initialized_test_cmsg(
        control: &mut Vec<u8>,
        level: libc::c_int,
        cmsg_type: libc::c_int,
        payload_len: usize,
    ) -> usize {
        super::append_initialized_test_cmsg(control, level, cmsg_type, payload_len)
    }

    /// Classifies initialized receive metadata through an adopted stream's
    /// current receive-option policy.
    pub fn test_parse_stream_recv_meta(
        stream: &SctpStream,
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
    ) -> io::Result<SctpRecvMeta> {
        parse_initialized_recv_state_meta_for_test(
            &stream.recv_state,
            control,
            controllen,
            msg_flags,
            data_slice,
            None,
        )
    }

    /// Returns the local Linux ABI value for `SCTP_ASSOC_CHANGE`.
    pub const fn test_assoc_change_type() -> libc::c_int {
        LOCAL_SCTP_ASSOC_CHANGE
    }

    /// Returns the local Linux ABI value for `SCTP_ADAPTATION_INDICATION`.
    pub const fn test_adaptation_indication_type() -> libc::c_int {
        LOCAL_SCTP_ADAPTATION_INDICATION
    }

    /// Returns the local Linux ABI value for `SCTP_AUTHENTICATION_EVENT`.
    pub const fn test_authentication_event_type() -> libc::c_int {
        LOCAL_SCTP_AUTHENTICATION_EVENT
    }

    /// Returns the local Linux ABI value for `SCTP_PEER_ADDR_CHANGE`.
    pub const fn test_peer_addr_change_type() -> libc::c_int {
        LOCAL_SCTP_PEER_ADDR_CHANGE
    }

    /// Returns the local Linux ABI value for legacy `SCTP_SEND_FAILED`.
    pub const fn test_send_failed_type() -> libc::c_int {
        LOCAL_SCTP_SEND_FAILED
    }

    /// Returns the error-field byte offset in the local legacy send-failed
    /// notification layout.
    pub const fn test_send_failed_error_offset() -> usize {
        SCTP_SEND_FAILED_ERROR_OFFSET
    }

    /// Returns the send-info byte offset in the local legacy send-failed
    /// notification layout.
    pub const fn test_send_failed_info_offset() -> usize {
        SCTP_SEND_FAILED_INFO_OFFSET
    }

    /// Returns the local Linux ABI value for `SCTP_REMOTE_ERROR`.
    pub const fn test_remote_error_type() -> libc::c_int {
        LOCAL_SCTP_REMOTE_ERROR
    }

    /// Returns the local Linux ABI value for `SCTP_SHUTDOWN_EVENT`.
    pub const fn test_shutdown_event_type() -> libc::c_int {
        LOCAL_SCTP_SHUTDOWN_EVENT
    }

    /// Returns the local Linux ABI value for `SCTP_PARTIAL_DELIVERY_EVENT`.
    pub const fn test_partial_delivery_event_type() -> libc::c_int {
        LOCAL_SCTP_PARTIAL_DELIVERY_EVENT
    }

    /// Returns the local Linux ABI value for `SCTP_SENDER_DRY_EVENT`.
    pub const fn test_sender_dry_event_type() -> libc::c_int {
        LOCAL_SCTP_SENDER_DRY_EVENT
    }

    /// Returns the local Linux ABI value for `SCTP_STREAM_RESET_EVENT`.
    pub const fn test_stream_reset_event_type() -> libc::c_int {
        LOCAL_SCTP_STREAM_RESET_EVENT
    }

    /// Returns the local Linux ABI value for `SCTP_ASSOC_RESET_EVENT`.
    pub const fn test_assoc_reset_event_type() -> libc::c_int {
        LOCAL_SCTP_ASSOC_RESET_EVENT
    }

    /// Returns the local Linux ABI value for `SCTP_STREAM_CHANGE_EVENT`.
    pub const fn test_stream_change_event_type() -> libc::c_int {
        LOCAL_SCTP_STREAM_CHANGE_EVENT
    }

    /// Returns the local Linux ABI value for modern
    /// `SCTP_SEND_FAILED_EVENT`.
    pub const fn test_send_failed_event_type() -> libc::c_int {
        LOCAL_SCTP_SEND_FAILED_EVENT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::send_sqe::test_support::sqe_prefix;
    use crate::runtime::buffer::IoBuffMut;
    use crate::runtime::executor::with_ringless_poll_context_for_test;
    use crate::runtime::task::{TaskHeader, TaskVTable};
    use crate::runtime::test_hooks;

    #[test]
    fn recovery_fuzz_scenario_seam_exhausts_its_bounded_cross_product() {
        let input = [0_u8; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN + 7];

        for input_len in [0, 1, SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN, input.len()] {
            let split_limit = input_len.min(SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN);
            let mut seen = [[[[false; 2]; 2]; 2]; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN + 1];
            let mut count = 0;
            for_each_sctp_recovery_fuzz_scenario(&input[..input_len], |scenario| {
                let slot = &mut seen[scenario.split][scenario.terminal as usize]
                    [scenario.visible as usize][scenario.second_is_notification as usize];
                assert!(!*slot, "duplicate fuzz scenario: {scenario:?}");
                *slot = true;
                count += 1;
            });

            assert_eq!(count, (split_limit + 1) * 8);
            for (split, terminals) in seen.iter().enumerate().take(split_limit + 1) {
                for (terminal, publications) in terminals.iter().enumerate() {
                    for (visible, notification_sources) in publications.iter().enumerate() {
                        for (second_is_notification, present) in
                            notification_sources.iter().enumerate()
                        {
                            assert!(
                                *present,
                                "missing fuzz scenario at split={split}, terminal={terminal}, \
                                 visible={visible}, second_notification={second_is_notification}"
                            );
                        }
                    }
                }
            }
        }
    }

    fn invalid_fd_op_state() -> RuntimeFdOpState<'static> {
        RuntimeFd::from_fresh_raw_fd(RuntimeFd::INVALID)
            .lease()
            .into_op_state()
    }

    /// Creates the exact submitted-state representation used by production
    /// around one manually prepared completion fixture.
    ///
    /// # Safety
    ///
    /// `state_ptr` must be a live state whose eventual retirement owns the
    /// lease installed here.
    unsafe fn invalid_submitted_fd_op_state(
        state_ptr: *mut CompletionState,
    ) -> RuntimeFdOpState<'static> {
        let mut fd_state = invalid_fd_op_state();
        unsafe { (*state_ptr).attach_fd_lease(fd_state.take_initial_lease()) };
        unsafe { fd_state.publish_submitted_state(state_ptr) };
        fd_state
    }

    thread_local! {
        static STASH_PUBLICATION_RECV_STATE: Cell<*mut SctpRecvState> =
            const { Cell::new(std::ptr::null_mut()) };
        static STASH_PUBLICATION_STATE_SLOT: Cell<*mut *mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static STASH_PUBLICATION_EXPECTED_STATE: Cell<*mut CompletionState> =
            const { Cell::new(std::ptr::null_mut()) };
        static STASH_PUBLICATION_DESTROYS: Cell<usize> = const { Cell::new(0) };
    }

    unsafe fn inspect_stash_publication(_: *mut TaskHeader) {
        let recv_state = STASH_PUBLICATION_RECV_STATE.with(Cell::get);
        let state_slot = STASH_PUBLICATION_STATE_SLOT.with(Cell::get);
        let expected_state = STASH_PUBLICATION_EXPECTED_STATE.with(Cell::get);
        assert!(!recv_state.is_null(), "SCTP receive state was not recorded");
        assert!(
            !state_slot.is_null(),
            "SCTP source state slot was not recorded"
        );
        unsafe {
            assert_eq!(
                (*recv_state).stashed_state,
                StashedSctpRecvState::Live,
                "stream stash state was not live before waiter destruction"
            );
            assert_eq!(
                (*recv_state).stashed.state_ptr,
                expected_state,
                "stream ownership was not published before waiter destruction"
            );
            assert!(
                (*state_slot).is_null(),
                "future ownership was not detached before waiter destruction"
            );
            assert_eq!((*recv_state).stashed.iov_count, 7);
            assert!((*recv_state).stashed.process_completed.is_some());
        }
        STASH_PUBLICATION_DESTROYS.with(|count| count.set(count.get() + 1));
    }

    static STASH_PUBLICATION_VTABLE: TaskVTable = TaskVTable {
        poll: |_| Poll::Ready(()),
        finish: |_| {},
        cancel: |_| {},
        destroy: inspect_stash_publication,
    };

    #[test]
    fn sctp_recv_len_rejects_zero_and_preserves_positive_bounds() {
        let zero =
            checked_sctp_recv_len(0, 8).expect_err("zero-length SCTP receive should be rejected");
        assert_eq!(zero.kind(), io::ErrorKind::InvalidInput);

        assert_eq!(
            checked_sctp_recv_len(1, 1).expect("positive in-bounds receive should succeed"),
            1
        );
        let oversize =
            checked_sctp_recv_len(2, 1).expect_err("oversized SCTP receive should remain invalid");
        assert_eq!(oversize.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn sctp_send_len_rejects_zero_and_preserves_positive_lengths() {
        let zero = checked_sctp_send_len(0).expect_err("zero-length SCTP send should be rejected");
        assert_eq!(zero.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(zero.raw_os_error(), None);
        assert_eq!(zero.to_string(), ZERO_LENGTH_SCTP_SEND);

        assert_eq!(
            checked_sctp_send_len(1).expect("positive SCTP send should succeed"),
            1
        );
        assert!(
            validate_sctp_vectored_send_len(Some(usize::MAX)).is_ok(),
            "the exact usize boundary must remain a valid nonempty aggregate"
        );
        let overflow = validate_sctp_vectored_send_len(None)
            .expect_err("unrepresentable SCTP aggregate should be rejected");
        assert_eq!(overflow.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(overflow.to_string(), SCTP_SEND_AGGREGATE_OVERFLOW);
    }

    #[test]
    fn sctp_active_iovec_limit_matches_retained_scratch_authority() {
        validate_sctp_active_iovec_count(RETAINED_IOVEC_MAX_COUNT)
            .expect("the retained-iovec boundary should remain valid for SCTP");
        let err = validate_sctp_active_iovec_count(RETAINED_IOVEC_MAX_COUNT + 1)
            .expect_err("an SCTP active-iovec count above the shared bound should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.raw_os_error(), None);
    }

    fn assert_vectored_aggregate_rejections(cx: &mut Context<'_>, expected_kind: io::ErrorKind) {
        let mut recv_segment =
            IoBuffMut::new(0, 8, 0).expect("aggregate receive segment allocation failed");
        let recv_ptr = recv_segment.as_mut_ptr();
        let mut recv_state = SctpRecvState::external();
        let mut recv = RecvVectoredFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: invalid_fd_op_state(),
            buffer: Some(IoBuffVecMut::from_array([recv_segment])),
            iov_count: RETAINED_IOVEC_MAX_COUNT + 1,
            writable: 0,
            invalid_aggregate: true,
            recv_state: &mut recv_state,
            _marker: PhantomData,
        };
        let Poll::Ready((Err(recv_err), mut recv_chain)) = Pin::new(&mut recv).poll(cx) else {
            panic!("unrepresentable SCTP receive aggregate remained pending");
        };
        assert_eq!(recv_err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(recv_err.to_string(), invalid_readv_aggregate().to_string());
        }
        assert_eq!(
            recv_chain
                .get_mut(0)
                .expect("returned aggregate receive segment missing")
                .as_mut_ptr(),
            recv_ptr
        );
        assert!(
            Pin::new(&mut recv).poll(cx).is_pending(),
            "completed aggregate receive future did not fuse"
        );
        drop(recv);
        assert!(recv_state.stashed.state_ptr.is_null());

        let mut send_segment =
            IoBuffMut::new(0, 8, 0).expect("aggregate send segment allocation failed");
        send_segment
            .payload_append(b"payload")
            .expect("aggregate send segment initialization failed");
        let send_chain = IoBuffVec::from_array([send_segment.freeze()]);
        let send_ptr = send_chain
            .get(0)
            .expect("aggregate send segment missing")
            .as_ptr();
        let mut send = SendVectoredFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: invalid_fd_op_state(),
            buffer: Some(send_chain),
            iov_count: RETAINED_IOVEC_MAX_COUNT + 1,
            total: 0,
            invalid_aggregate: true,
            sndinfo: raw_sndinfo_from_public(SctpSendInfo::default()),
            _marker: PhantomData,
        };
        let Poll::Ready((Err(send_err), returned_send)) = Pin::new(&mut send).poll(cx) else {
            panic!("unrepresentable SCTP send aggregate remained pending");
        };
        assert_eq!(send_err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(send_err.to_string(), SCTP_SEND_AGGREGATE_OVERFLOW);
        }
        assert_eq!(
            returned_send
                .get(0)
                .expect("returned aggregate send segment missing")
                .as_ptr(),
            send_ptr
        );
        assert!(
            Pin::new(&mut send).poll(cx).is_pending(),
            "completed aggregate send future did not fuse"
        );
    }

    #[test]
    fn sctp_vectored_aggregate_errors_preserve_context_and_return_exact_owners() {
        let mut outside_cx = Context::from_waker(std::task::Waker::noop());
        assert_vectored_aggregate_rejections(&mut outside_cx, io::ErrorKind::NotConnected);

        with_ringless_poll_context_for_test(1, |owner, cx| {
            assert_vectored_aggregate_rejections(cx, io::ErrorKind::InvalidInput);

            let reactor = owner.reactor_ptr();
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 0);
            assert_eq!(stats.heap_fallbacks, 0);
        });
    }

    unsafe fn reject_abandoned_stashed_processing(
        _reactor: *mut Reactor,
        _state_ptr: *mut CompletionState,
        _iov_count: usize,
        _recv_state: &mut SctpRecvState,
    ) {
        panic!("ring-abandoned stashed receive was processed as completed");
    }

    unsafe fn reject_invalid_request_stash_processing(
        _reactor: *mut Reactor,
        _state_ptr: *mut CompletionState,
        _iov_count: usize,
        _recv_state: &mut SctpRecvState,
    ) {
        panic!("invalid metadata receive processed the prior stash");
    }

    fn assert_invalid_request_stash(
        recv_state: &SctpRecvState,
        expected_state: *mut CompletionState,
        expected_iov_count: usize,
    ) {
        assert_eq!(recv_state.stashed_state, StashedSctpRecvState::Live);
        assert_eq!(recv_state.stashed.state_ptr, expected_state);
        assert_eq!(recv_state.stashed.iov_count, expected_iov_count);
        assert!(recv_state.stashed.process_completed.is_some());
        assert_eq!(
            recv_state.record_sync,
            SctpRecordSync::DataTail,
            "invalid metadata receive changed record recovery state"
        );
    }

    fn assert_invalid_scalar_receive_precedes_stash(
        cx: &mut Context<'_>,
        expected_kind: io::ErrorKind,
        expected_iov_count: usize,
    ) {
        let mut stashed_state = CompletionState::empty();
        let stashed_state_ptr = std::ptr::addr_of_mut!(stashed_state);
        let pointer_calls = Rc::new(Cell::new(0));
        let published_len = Rc::new(Cell::new(0));
        let buffer =
            RejectedContextRecvBuffer::new(Rc::clone(&pointer_calls), Rc::clone(&published_len));
        let original_buffer_ptr = buffer.backing_ptr();
        let mut recv_state = SctpRecvState::external();
        recv_state.record_sync = SctpRecordSync::DataTail;
        unsafe {
            recv_state.set_stashed_live_for_test(
                stashed_state_ptr,
                expected_iov_count,
                reject_invalid_request_stash_processing,
            );
        }
        let mut recv = RecvFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: invalid_fd_op_state(),
            buffer: Some(buffer),
            write_base_len: 0,
            len: 0,
            input_error: Some(invalid_zero_length_sctp_recv()),
            recv_state: &mut recv_state,
            _marker: PhantomData,
        };

        let Poll::Ready((Err(err), returned)) = Pin::new(&mut recv).poll(cx) else {
            panic!("invalid scalar metadata receive did not return immediately");
        };
        assert_eq!(err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), ZERO_LENGTH_SCTP_RECV);
        }
        assert_eq!(returned.backing_ptr(), original_buffer_ptr);
        assert_eq!(pointer_calls.get(), 0);
        assert_eq!(published_len.get(), 0);
        assert_invalid_request_stash(recv.recv_state, stashed_state_ptr, expected_iov_count);
        assert!(!stashed_state.is_context_rejected());

        stashed_state.set_completed();
        assert!(
            Pin::new(&mut recv).poll(cx).is_pending(),
            "completed invalid scalar metadata receive did not stay fused"
        );
        assert_invalid_request_stash(recv.recv_state, stashed_state_ptr, expected_iov_count);
        drop(recv);
        assert_invalid_request_stash(&recv_state, stashed_state_ptr, expected_iov_count);
        drop(returned);
    }

    fn assert_invalid_vectored_receive_precedes_stash(
        cx: &mut Context<'_>,
        expected_kind: io::ErrorKind,
        expected_iov_count: usize,
    ) {
        let mut stashed_vectored_state = CompletionState::empty();
        let stashed_vectored_state_ptr = std::ptr::addr_of_mut!(stashed_vectored_state);
        let mut segment = IoBuffMut::new(4, 0, 0).expect("zero-writable segment allocation failed");
        let original_segment_ptr = segment.as_mut_ptr();
        let chain = IoBuffVecMut::from_array([segment]);
        let mut vectored_recv_state = SctpRecvState::external();
        vectored_recv_state.record_sync = SctpRecordSync::DataTail;
        unsafe {
            vectored_recv_state.set_stashed_live_for_test(
                stashed_vectored_state_ptr,
                expected_iov_count,
                reject_invalid_request_stash_processing,
            );
        }
        let mut recv = RecvVectoredFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: invalid_fd_op_state(),
            buffer: Some(chain),
            iov_count: RETAINED_IOVEC_MAX_COUNT + 1,
            writable: 0,
            invalid_aggregate: false,
            recv_state: &mut vectored_recv_state,
            _marker: PhantomData,
        };

        let Poll::Ready((Err(err), mut returned)) = Pin::new(&mut recv).poll(cx) else {
            panic!("invalid vectored metadata receive did not return immediately");
        };
        assert_eq!(err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), ZERO_LENGTH_SCTP_RECV);
        }
        assert_eq!(returned.segments(), 1);
        assert_eq!(
            returned
                .get_mut(0)
                .expect("returned zero-writable segment missing")
                .as_mut_ptr(),
            original_segment_ptr
        );
        assert_invalid_request_stash(
            recv.recv_state,
            stashed_vectored_state_ptr,
            expected_iov_count,
        );
        assert!(!stashed_vectored_state.is_context_rejected());

        stashed_vectored_state.set_completed();
        assert!(
            Pin::new(&mut recv).poll(cx).is_pending(),
            "completed invalid vectored metadata receive did not stay fused"
        );
        assert_invalid_request_stash(
            recv.recv_state,
            stashed_vectored_state_ptr,
            expected_iov_count,
        );
        drop(recv);
        assert_invalid_request_stash(
            &vectored_recv_state,
            stashed_vectored_state_ptr,
            expected_iov_count,
        );
        drop(returned);
    }

    fn assert_sctp_active_iovec_limit_rejections(
        cx: &mut Context<'_>,
        expected_kind: io::ErrorKind,
    ) {
        let mut stashed_state = CompletionState::empty();
        let stashed_state_ptr = std::ptr::addr_of_mut!(stashed_state);
        let mut recv_segment =
            IoBuffMut::new(0, 8, 0).expect("over-limit receive segment allocation failed");
        let recv_ptr = recv_segment.as_mut_ptr();
        let mut recv_state = SctpRecvState::external();
        recv_state.record_sync = SctpRecordSync::DataTail;
        unsafe {
            recv_state.set_stashed_live_for_test(
                stashed_state_ptr,
                11,
                reject_invalid_request_stash_processing,
            );
        }
        let mut recv = RecvVectoredFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: invalid_fd_op_state(),
            buffer: Some(IoBuffVecMut::from_array([recv_segment])),
            iov_count: RETAINED_IOVEC_MAX_COUNT + 1,
            writable: 8,
            invalid_aggregate: false,
            recv_state: &mut recv_state,
            _marker: PhantomData,
        };
        let Poll::Ready((Err(recv_err), mut returned_recv)) = Pin::new(&mut recv).poll(cx) else {
            panic!("over-limit SCTP receive did not return immediately");
        };
        assert_eq!(recv_err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(recv_err.raw_os_error(), None);
        }
        assert_eq!(
            returned_recv
                .get_mut(0)
                .expect("returned over-limit receive segment missing")
                .as_mut_ptr(),
            recv_ptr
        );
        assert_invalid_request_stash(recv.recv_state, stashed_state_ptr, 11);
        assert!(Pin::new(&mut recv).poll(cx).is_pending());
        drop(recv);
        assert_invalid_request_stash(&recv_state, stashed_state_ptr, 11);

        let mut send_segment =
            IoBuffMut::new(0, 8, 0).expect("over-limit send segment allocation failed");
        send_segment
            .payload_append(b"payload")
            .expect("over-limit send segment initialization failed");
        let send_chain = IoBuffVec::from_array([send_segment.freeze()]);
        let send_ptr = send_chain
            .get(0)
            .expect("over-limit send segment missing")
            .as_ptr();
        let mut send = SendVectoredFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: invalid_fd_op_state(),
            buffer: Some(send_chain),
            iov_count: RETAINED_IOVEC_MAX_COUNT + 1,
            total: 7,
            invalid_aggregate: false,
            sndinfo: raw_sndinfo_from_public(SctpSendInfo::default()),
            _marker: PhantomData,
        };
        let Poll::Ready((Err(send_err), returned_send)) = Pin::new(&mut send).poll(cx) else {
            panic!("over-limit SCTP send did not return immediately");
        };
        assert_eq!(send_err.kind(), expected_kind);
        if expected_kind == io::ErrorKind::InvalidInput {
            assert_eq!(send_err.raw_os_error(), None);
        }
        assert_eq!(
            returned_send
                .get(0)
                .expect("returned over-limit send segment missing")
                .as_ptr(),
            send_ptr
        );
        assert!(Pin::new(&mut send).poll(cx).is_pending());
    }

    #[test]
    fn sctp_active_iovec_rejections_preserve_context_owners_and_stash() {
        let mut outside_cx = Context::from_waker(std::task::Waker::noop());
        assert_sctp_active_iovec_limit_rejections(&mut outside_cx, io::ErrorKind::NotConnected);

        with_ringless_poll_context_for_test(1, |owner, cx| {
            assert_sctp_active_iovec_limit_rejections(cx, io::ErrorKind::InvalidInput);

            let reactor = owner.reactor_ptr();
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 0);
            assert_eq!(stats.heap_fallbacks, 0);
        });
    }

    #[test]
    fn invalid_metadata_receives_return_before_stash_recovery() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            assert_invalid_scalar_receive_precedes_stash(cx, io::ErrorKind::InvalidInput, 3);
            assert_invalid_vectored_receive_precedes_stash(cx, io::ErrorKind::InvalidInput, 5);

            let reactor = owner.reactor_ptr();
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[test]
    fn invalid_metadata_receives_without_runtime_do_not_adopt_stash() {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert_invalid_scalar_receive_precedes_stash(&mut cx, io::ErrorKind::NotConnected, 7);
        assert_invalid_vectored_receive_precedes_stash(&mut cx, io::ErrorKind::NotConnected, 9);
    }

    #[test]
    fn stashed_recv_state_transitions_preserve_full_width_iov_count() {
        let marker = NonNull::<CompletionState>::dangling().as_ptr();
        let mut recv_state = SctpRecvState::external();
        assert_eq!(recv_state.stashed_state, StashedSctpRecvState::Empty);

        unsafe {
            recv_state.set_stashed_live_for_test(
                marker,
                usize::MAX,
                reject_abandoned_stashed_processing,
            );
        }
        assert_eq!(recv_state.stashed_state, StashedSctpRecvState::Live);
        assert_eq!(recv_state.stashed.state_ptr, marker);
        assert_eq!(recv_state.stashed.iov_count, usize::MAX);
        assert!(recv_state.stashed.process_completed.is_some());

        recv_state.mark_stashed_abandoned();
        assert_eq!(recv_state.stashed_state, StashedSctpRecvState::Abandoned);
        assert_eq!(recv_state.stashed.state_ptr, marker);
        assert_eq!(recv_state.stashed.iov_count, 0);
        assert!(recv_state.stashed.process_completed.is_none());

        // The dangling marker makes any accidental abandonment dereference a
        // focused Miri failure; stream-local clear must only reset metadata.
        unsafe { recv_state.drop_stashed() };
        assert_eq!(recv_state.stashed_state, StashedSctpRecvState::Empty);
        assert!(recv_state.stashed.state_ptr.is_null());
    }

    #[test]
    fn sctp_stash_publishes_ownership_before_waiter_destruction() {
        STASH_PUBLICATION_DESTROYS.with(|count| count.set(0));
        let mut recv_state = SctpRecvState::external();
        let recv_state_ptr = std::ptr::addr_of_mut!(recv_state);
        let mut state = CompletionState::empty();
        let state_ptr = std::ptr::addr_of_mut!(state);
        let mut state_slot = state_ptr;
        let state_slot_ptr = std::ptr::addr_of_mut!(state_slot);
        let mut waiter = TaskHeader::new();
        waiter.vtable = &STASH_PUBLICATION_VTABLE;
        let waiter_ptr = std::ptr::addr_of_mut!(waiter);

        unsafe {
            state.register_waiter(waiter_ptr);
            release_task(waiter_ptr);
        }
        assert_eq!(waiter.refs.get(), 1);

        STASH_PUBLICATION_RECV_STATE.with(|stored| stored.set(recv_state_ptr));
        STASH_PUBLICATION_STATE_SLOT.with(|stored| stored.set(state_slot_ptr));
        STASH_PUBLICATION_EXPECTED_STATE.with(|stored| stored.set(state_ptr));
        unsafe {
            SctpRecvState::stash_unchecked(
                recv_state_ptr,
                state_slot_ptr,
                7,
                reject_abandoned_stashed_processing,
            );
        }
        STASH_PUBLICATION_RECV_STATE.with(|stored| stored.set(std::ptr::null_mut()));
        STASH_PUBLICATION_STATE_SLOT.with(|stored| stored.set(std::ptr::null_mut()));
        STASH_PUBLICATION_EXPECTED_STATE.with(|stored| stored.set(std::ptr::null_mut()));

        STASH_PUBLICATION_DESTROYS.with(|count| assert_eq!(count.get(), 1));
        assert!(state_slot.is_null());
        assert_eq!(recv_state.stashed_state, StashedSctpRecvState::Live);
        assert_eq!(recv_state.stashed.state_ptr, state_ptr);
        assert_eq!(recv_state.stashed.iov_count, 7);
        assert!(recv_state.stashed.process_completed.is_some());
        assert_eq!(waiter.refs.get(), 0);
    }

    #[test]
    fn ring_abandoned_stashed_receive_remains_terminal_until_stream_drop() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let mut abandoned = CompletionState::empty();
            abandoned.set_ring_abandoned();
            let abandoned_ptr = std::ptr::addr_of_mut!(abandoned);
            let mut stream = ringless_sctp_stream();
            unsafe {
                stream.recv_state.set_stashed_live_for_test(
                    abandoned_ptr,
                    1,
                    reject_abandoned_stashed_processing,
                );
            }

            test_hooks::fail_next_op_alloc();
            test_hooks::fail_next_raw_sqe_submit();
            for attempt in 1..=2 {
                let pointer_calls = Rc::new(Cell::new(0));
                let drops = Rc::new(Cell::new(0));
                let buffer = retained_constructor_buffer(
                    None,
                    Rc::clone(&pointer_calls),
                    Rc::clone(&drops),
                    false,
                );
                let original_buffer_ptr = buffer.bytes.as_ptr();
                let mut recv = stream.recv_msg(buffer, 16);
                let Poll::Ready((Err(err), returned)) = Pin::new(&mut recv).poll(cx) else {
                    panic!(
                        "ring-abandoned stashed rich receive attempt {attempt} remained pending"
                    );
                };
                assert_eq!(err.kind(), io::ErrorKind::NotConnected);
                assert_eq!(
                    recv.recv_state.stashed_state,
                    StashedSctpRecvState::Abandoned
                );
                assert_eq!(returned.bytes.as_ptr(), original_buffer_ptr);
                assert_eq!(
                    pointer_calls.get(),
                    0,
                    "terminal rich receive exposed its returned buffer"
                );
                assert_eq!(drops.get(), 0);
                assert_eq!(recv.recv_state.stashed.state_ptr, abandoned_ptr);
                assert_eq!(recv.recv_state.stashed.iov_count, 0);
                assert!(recv.recv_state.stashed.process_completed.is_none());
                assert!(Pin::new(&mut recv).poll(cx).is_pending());
                drop(recv);
                assert_eq!(
                    stream.recv_state.stashed_state,
                    StashedSctpRecvState::Abandoned
                );
                assert_eq!(stream.recv_state.stashed.state_ptr, abandoned_ptr);
                assert!(stream.recv_state.stashed.process_completed.is_none());
                drop(returned);
                assert_eq!(drops.get(), 1);
            }

            let mut first = IoBuffMut::new(0, 8, 0)
                .expect("terminal vectored receive first-segment allocation failed");
            let first_ptr = first.as_mut_ptr();
            let mut second = IoBuffMut::new(0, 8, 0)
                .expect("terminal vectored receive second-segment allocation failed");
            let second_ptr = second.as_mut_ptr();
            let mut recv = stream.recv_msg_vectored(IoBuffVecMut::from_array([first, second]));
            let Poll::Ready((Err(err), mut returned)) = Pin::new(&mut recv).poll(cx) else {
                panic!("ring-abandoned stashed vectored receive remained pending");
            };
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(
                returned
                    .get_mut(0)
                    .expect("returned first terminal segment missing")
                    .as_mut_ptr(),
                first_ptr
            );
            assert_eq!(
                returned
                    .get_mut(1)
                    .expect("returned second terminal segment missing")
                    .as_mut_ptr(),
                second_ptr
            );
            assert!(Pin::new(&mut recv).poll(cx).is_pending());
            drop(recv);
            assert_eq!(
                stream.recv_state.stashed_state,
                StashedSctpRecvState::Abandoned
            );
            assert_eq!(stream.recv_state.stashed.state_ptr, abandoned_ptr);
            assert!(stream.recv_state.stashed.process_completed.is_none());
            drop(returned);

            for _ in 0..2 {
                assert_lean_stash_rejection(
                    &mut stream,
                    cx,
                    abandoned_ptr,
                    StashedSctpRecvState::Abandoned,
                );
            }
            assert!(
                test_hooks::take_op_alloc_failure(),
                "terminal stash recovery attempted operation allocation"
            );
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 1);
            let injected = test_hooks::take_raw_sqe_submit_failure()
                .expect("terminal stash recovery consumed the SQE sentinel");
            assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);

            unsafe { stream.recv_state.drop_stashed() };
            assert_eq!(stream.recv_state.stashed_state, StashedSctpRecvState::Empty);
            assert!(stream.recv_state.stashed.state_ptr.is_null());
            assert_eq!(stream.recv_state.stashed.iov_count, 0);
            assert!(stream.recv_state.stashed.process_completed.is_none());
            assert!(abandoned.is_ring_abandoned());
            assert!(!abandoned.is_completed());
        });
    }

    #[test]
    fn terminally_abandoned_stash_marker_is_opaque_to_repoll_and_stream_drop() {
        let marker = NonNull::<CompletionState>::dangling().as_ptr();
        let mut stream = ringless_sctp_stream();
        stream.recv_state.set_stashed_abandoned_for_test(marker);
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let buffer =
            retained_constructor_buffer(None, Rc::clone(&pointer_calls), Rc::clone(&drops), false);
        let original_buffer_ptr = buffer.bytes.as_ptr();
        let mut recv = stream.recv_msg(buffer, 16);
        let mut cx = Context::from_waker(std::task::Waker::noop());

        let Poll::Ready((Err(err), returned)) = Pin::new(&mut recv).poll(&mut cx) else {
            panic!("opaque abandoned marker did not return a terminal error");
        };
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        assert_eq!(
            recv.recv_state.stashed_state,
            StashedSctpRecvState::Abandoned
        );
        assert_eq!(returned.bytes.as_ptr(), original_buffer_ptr);
        assert_eq!(pointer_calls.get(), 0);
        assert_eq!(drops.get(), 0);
        assert_eq!(recv.recv_state.stashed.state_ptr, marker);
        assert!(recv.recv_state.stashed.process_completed.is_none());
        drop(recv);
        assert_eq!(
            stream.recv_state.stashed_state,
            StashedSctpRecvState::Abandoned
        );
        assert_eq!(stream.recv_state.stashed.state_ptr, marker);
        drop(returned);
        assert_eq!(drops.get(), 1);

        // Miri makes any attempt by future or stream destruction to follow
        // this deliberately dangling marker an immediate failure.
        drop(stream);
    }

    #[cfg(not(miri))]
    #[test]
    fn ring_abandoned_sctp_connect_retains_submitted_socket_with_state() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let remote_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 9));
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state_ptr.is_null(), "SCTP connect state allocation failed");
            let fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("SCTP connect fd creation failed");
            // SAFETY: the test-created descriptor has no other owner.
            let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let retained = unsafe {
                (&mut *reactor).alloc_retained_payload(crate::net::RetainedConnectPayload::new(
                    owned_fd,
                    RetainedConnectAddr::from_socket_addr(remote_addr),
                ))
            };
            unsafe {
                (*state_ptr).attach_retained_payload(retained);
                (*state_ptr).set_ring_abandoned();
            }

            let mut slot = ConnectSlot::new(SctpSocketConfig::default());
            slot.state_ptr = state_ptr;
            slot.in_use = true;
            let mut connect = ConnectFuture {
                slot: &mut slot,
                remote_addr,
            };

            assert!(matches!(
                Pin::new(&mut connect).poll(cx),
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
            ));
            drop(connect);
            assert!(slot.state_ptr.is_null());
            assert!(!slot.in_use);
            assert!(slot.fd.is_none());
            assert!(slot.addr.is_none());
            assert!(unsafe { (*state_ptr).is_ring_abandoned() });
            assert!(!crate::runtime::fd::raw_fd_is_closed(fd));

            unsafe {
                (*state_ptr).restore_completed_orphaned_after_ringless_abandonment_for_test();
                Reactor::free_op_unchecked(reactor, state_ptr);
            }
            assert!(crate::runtime::fd::raw_fd_is_closed(fd));
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn sctp_connect_established_config_error_closes_transferred_socket() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let remote_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 9));
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(
                !state_ptr.is_null(),
                "SCTP completed-connect state allocation failed"
            );
            let fd = crate::runtime::fd::distinctive_closeable_test_fd()
                .expect("SCTP completed-connect fd creation failed");
            // SAFETY: the test-created descriptor has no other owner.
            let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let retained = unsafe {
                (&mut *reactor).alloc_retained_payload(crate::net::RetainedConnectPayload::new(
                    owned_fd,
                    RetainedConnectAddr::from_socket_addr(remote_addr),
                ))
            };
            unsafe {
                (*state_ptr).attach_retained_payload(retained);
                (*state_ptr).result = 0;
                (*state_ptr).set_completed();
            }

            let config = SctpSocketConfig {
                default_peer_addr_params: Some(SctpPeerAddrParams::for_address(remote_addr)),
                ..SctpSocketConfig::default()
            };
            let mut slot = ConnectSlot::new(config);
            slot.state_ptr = state_ptr;
            slot.in_use = true;
            let mut connect = ConnectFuture {
                slot: &mut slot,
                remote_addr,
            };

            let Poll::Ready(Err(err)) = Pin::new(&mut connect).poll(cx) else {
                panic!("invalid established SCTP configuration did not fail");
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            drop(connect);
            assert!(slot.state_ptr.is_null());
            assert!(!slot.in_use);
            assert!(slot.fd.is_none());
            assert!(slot.addr.is_none());
            assert!(crate::runtime::fd::raw_fd_is_closed(fd));
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn sctp_terminal_accept_readiness_latches_and_later_accepts_fail_fast() {
        crate::net::test_terminal_accept_readiness(
            "SCTP",
            |slot, cx, state_ptr| {
                slot.state_ptr = state_ptr;
                slot.in_use = true;
                let mut accept = AcceptFuture {
                    slot,
                    accepted_config: SctpSocketConfig::default(),
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
                    accepted_config: SctpSocketConfig::default(),
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
    fn sctp_bare_poll_error_budget_exhaustion_does_not_latch_listener() {
        crate::net::test_bare_poll_error_budget_exhaustion("SCTP", |slot, cx, state_ptr| {
            slot.state_ptr = state_ptr;
            slot.in_use = true;
            let mut accept = AcceptFuture {
                slot,
                accepted_config: SctpSocketConfig::default(),
                input_error: None,
                prepared: true,
            };
            let outcome = Future::poll(Pin::new(&mut accept), cx);
            drop(accept);
            outcome
        });
    }

    #[test]
    fn sctp_unprepared_accept_future_parks_without_touching_slot() {
        crate::net::test_unprepared_accept_future_parks("SCTP", |slot, cx, input_error| {
            let mut accept = AcceptFuture {
                slot,
                accepted_config: SctpSocketConfig::default(),
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
    fn sctp_vectored_receive_inspects_first_writable_segment() {
        let mut full = IoBuffMut::new(0, 4, 0).expect("full segment allocation failed");
        full.payload_append(b"full")
            .expect("full segment initialization failed");
        let zero = IoBuffMut::new(0, 0, 0).expect("zero segment allocation failed");
        let writable = IoBuffMut::new(0, 8, 0).expect("writable segment allocation failed");
        let mut chain = IoBuffVecMut::from_array([full, zero, writable]);
        let mut iovecs: [MaybeUninit<libc::iovec>; 3] =
            std::array::from_fn(|_| MaybeUninit::uninit());

        let (iov_count, writable_len) = fill_recv_vectored_iovecs(&mut chain, &mut iovecs[..1])
            .expect("SCTP receive iovec materialization failed");
        assert_eq!((iov_count, writable_len), (1, 8));
        let first = unsafe { iovecs[0].assume_init_ref() };
        unsafe {
            std::ptr::copy_nonoverlapping(b"note".as_ptr(), first.iov_base.cast::<u8>(), 4);
        }

        let first_iovec = unsafe { copy_sctp_first_iovec(&iovecs, iov_count) };
        let received = first_iov_view_from_copied_descriptor(
            &chain,
            *first_iovec.as_ref().expect("active first iovec missing"),
            4,
        );
        assert_eq!(received, b"note");
        assert_eq!(
            chain.get(0).expect("full segment missing").payload_bytes(),
            b"full"
        );
        assert_eq!(
            chain
                .get(2)
                .expect("writable segment missing")
                .payload_len(),
            0
        );
    }

    type FirstIovSliceFn<const N: usize> = for<'owner, 'descriptor> unsafe fn(
        &'owner IoBuffVecMut<N>,
        &'descriptor libc::iovec,
        usize,
    ) -> &'owner [u8];

    fn first_iov_view_from_copied_descriptor<const N: usize>(
        owner: &IoBuffVecMut<N>,
        descriptor: libc::iovec,
        actual: usize,
    ) -> &[u8] {
        unsafe { sctp_first_iov_slice(owner, &descriptor, actual) }
    }

    #[test]
    fn sctp_first_iov_slice_lifetime_is_bound_to_buffer_owner() {
        let signature: FirstIovSliceFn<1> = sctp_first_iov_slice::<1>;
        let _ = signature;

        let mut segment = IoBuffMut::new(0, 8, 0).expect("segment allocation failed");
        let first_iovec = libc::iovec {
            iov_base: segment.as_mut_ptr().cast(),
            iov_len: 8,
        };
        unsafe {
            std::ptr::copy_nonoverlapping(b"owner".as_ptr(), segment.as_mut_ptr(), 5);
        }
        let chain = IoBuffVecMut::from_array([segment]);

        let view = first_iov_view_from_copied_descriptor(&chain, first_iovec, 5);
        assert_eq!(view, b"owner");
    }

    #[derive(Clone, Copy, Debug)]
    enum SctpReadShapeDrift {
        Growth,
        Shrink,
        Total,
    }

    fn sctp_read_shape_drift_chain(
        drift: SctpReadShapeDrift,
    ) -> (IoBuffVecMut<2>, (usize, usize), [usize; 2]) {
        let mut chain = match drift {
            SctpReadShapeDrift::Growth => {
                let mut full =
                    IoBuffMut::new(0, 4, 0).expect("full SCTP segment allocation failed");
                full.payload_append(b"full")
                    .expect("full SCTP segment initialization failed");
                let writable =
                    IoBuffMut::new(0, 4, 0).expect("writable SCTP segment allocation failed");
                IoBuffVecMut::from_array([full, writable])
            }
            SctpReadShapeDrift::Shrink | SctpReadShapeDrift::Total => {
                let first = IoBuffMut::new(0, 4, 0).expect("first SCTP segment allocation failed");
                let second =
                    IoBuffMut::new(0, 4, 0).expect("second SCTP segment allocation failed");
                IoBuffVecMut::from_array([first, second])
            }
        };
        let initial_shape = chain
            .checked_read_iovec_count_and_writable_len()
            .expect("initial SCTP receive shape overflowed");
        let first = chain
            .get_mut(0)
            .expect("first SCTP shape-drift segment missing");
        match drift {
            SctpReadShapeDrift::Growth => first.reset(),
            SctpReadShapeDrift::Shrink => first
                .payload_append(b"full")
                .expect("SCTP shrink mutation failed"),
            SctpReadShapeDrift::Total => first
                .payload_append(b"x")
                .expect("SCTP total mutation failed"),
        }
        let pointers = std::array::from_fn(|index| {
            chain
                .get_mut(index)
                .expect("SCTP shape-drift segment missing")
                .as_mut_ptr() as usize
        });
        (chain, initial_shape, pointers)
    }

    #[test]
    fn sctp_recv_vectored_shape_drift_rejects_before_submission_and_returns_chain() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();

            for drift in [
                SctpReadShapeDrift::Growth,
                SctpReadShapeDrift::Shrink,
                SctpReadShapeDrift::Total,
            ] {
                let (chain, initial_shape, pointers) = sctp_read_shape_drift_chain(drift);
                let mut recv_state = SctpRecvState::external();
                let mut future: RecvVectoredFuture<'_, 2> = RecvVectoredFuture {
                    fd: RuntimeFd::INVALID,
                    state_ptr: invalid_fd_op_state(),
                    buffer: Some(chain),
                    iov_count: initial_shape.0,
                    writable: initial_shape.1,
                    invalid_aggregate: false,
                    recv_state: &mut recv_state,
                    _marker: PhantomData,
                };

                let Poll::Ready((result, mut returned)) = Pin::new(&mut future).poll(cx) else {
                    panic!("SCTP read shape drift remained pending");
                };
                let err = result.expect_err("SCTP read shape drift unexpectedly succeeded");
                assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(
                    err.to_string(),
                    crate::runtime::buffer::iobuffvec::READ_IOVEC_SHAPE_CHANGED
                );
                let returned_ptrs = std::array::from_fn(|index| {
                    returned
                        .get_mut(index)
                        .expect("returned SCTP shape-drift segment missing")
                        .as_mut_ptr() as usize
                });
                assert_eq!(returned_ptrs, pointers);
                let expected_shape = match drift {
                    SctpReadShapeDrift::Growth => (2, 8),
                    SctpReadShapeDrift::Shrink => (1, 4),
                    SctpReadShapeDrift::Total => (2, 7),
                };
                assert_eq!(
                    returned.checked_read_iovec_count_and_writable_len(),
                    Some(expected_shape)
                );
                assert!(future.state_ptr.is_null());
                assert!(
                    Pin::new(&mut future).poll(cx).is_pending(),
                    "completed SCTP shape-drift future did not fuse"
                );
                drop(future);
                assert!(recv_state.stashed.state_ptr.is_null());
                assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                assert_eq!(owner.inflight_op_count_for_test(), 0);
                drop(returned);
            }

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 3);
            assert_eq!(stats.pooled_frees, 3);
        });
    }

    fn sctp_send_shape_drift_chain() -> (IoBuffVec<2>, [usize; 2]) {
        let mut first = IoBuffMut::new(0, 3, 0).expect("first SCTP send segment allocation failed");
        first
            .payload_append(b"abc")
            .expect("first SCTP send segment initialization failed");
        let mut second =
            IoBuffMut::new(0, 5, 0).expect("second SCTP send segment allocation failed");
        second
            .payload_append(b"defgh")
            .expect("second SCTP send segment initialization failed");
        let chain = IoBuffVec::from_array([first.freeze(), second.freeze()]);
        let pointers = std::array::from_fn(|index| {
            chain
                .get(index)
                .expect("SCTP send shape-drift segment missing")
                .as_ptr() as usize
        });
        (chain, pointers)
    }

    #[test]
    fn sctp_send_vectored_shape_drift_rejects_before_submission_and_returns_chain() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();

            for expected_shape in [(1, 8), (2, 7)] {
                let (chain, pointers) = sctp_send_shape_drift_chain();
                assert_eq!(
                    checked_iobuffvec_write_iovec_count_and_len(&chain),
                    Some((2, 8))
                );
                let mut future: SendVectoredFuture<'_, 2> = SendVectoredFuture {
                    fd: RuntimeFd::INVALID,
                    state_ptr: invalid_fd_op_state(),
                    buffer: Some(chain),
                    iov_count: expected_shape.0,
                    total: expected_shape.1,
                    invalid_aggregate: false,
                    sndinfo: raw_sndinfo_from_public(SctpSendInfo::default()),
                    _marker: PhantomData,
                };

                let Poll::Ready((result, returned)) = Pin::new(&mut future).poll(cx) else {
                    panic!("SCTP send shape drift remained pending");
                };
                let err = result.expect_err("SCTP send shape drift unexpectedly succeeded");
                assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(err.to_string(), invalid_writev_shape().to_string());
                let returned_pointers = std::array::from_fn(|index| {
                    returned
                        .get(index)
                        .expect("returned SCTP send shape-drift segment missing")
                        .as_ptr() as usize
                });
                assert_eq!(returned_pointers, pointers);
                assert_eq!(
                    checked_iobuffvec_write_iovec_count_and_len(&returned),
                    Some((2, 8))
                );
                assert!(future.state_ptr.is_null());
                assert!(
                    Pin::new(&mut future).poll(cx).is_pending(),
                    "completed SCTP send shape-drift future did not fuse"
                );
                assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                assert_eq!(owner.inflight_op_count_for_test(), 0);
                drop(returned);
            }

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 2);
            assert_eq!(stats.pooled_frees, 2);
        });
    }

    struct RetainedConstructorBuffer {
        bytes: Box<[u8; 32]>,
        reenter_pool: Option<NonNull<RetainedPayloadPool>>,
        pointer_calls: Rc<Cell<usize>>,
        pointer_address: Option<Rc<Cell<usize>>>,
        drops: Rc<Cell<usize>>,
        panic_on_pointer: bool,
    }

    impl RetainedConstructorBuffer {
        fn note_pointer_access(&self) {
            self.pointer_calls.set(self.pointer_calls.get() + 1);
            if let Some(pointer_address) = self.pointer_address.as_ref() {
                pointer_address.set(self as *const Self as usize);
            }
            if let Some(mut pool) = self.reenter_pool {
                // SAFETY: this callback runs synchronously on the same owner
                // thread. The raw-slot reservation deliberately retains no
                // Rust borrow of the pool across this reentrant allocation.
                let nested = unsafe { pool.as_mut().alloc(0x68_u64) };
                let value = unsafe { nested.take(pool.as_mut()) };
                assert_eq!(value, 0x68);
            }
            if self.panic_on_pointer {
                panic!("intentional retained-payload pointer panic");
            }
        }
    }

    impl Drop for RetainedConstructorBuffer {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    unsafe impl IoBuffReadOnly for RetainedConstructorBuffer {
        fn as_ptr(&self) -> *const u8 {
            self.note_pointer_access();
            self.bytes.as_ptr()
        }

        fn len(&self) -> usize {
            self.bytes.len()
        }
    }

    unsafe impl IoBuffReadWrite for RetainedConstructorBuffer {
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.note_pointer_access();
            self.bytes.as_mut_ptr()
        }

        fn writable_len(&self) -> usize {
            self.bytes.len()
        }

        unsafe fn set_written_len(&mut self, len: usize) {
            assert!(len <= self.bytes.len());
        }
    }

    fn retained_constructor_buffer(
        pool: Option<NonNull<RetainedPayloadPool>>,
        pointer_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        panic_on_pointer: bool,
    ) -> RetainedConstructorBuffer {
        RetainedConstructorBuffer {
            bytes: Box::new([0; 32]),
            reenter_pool: pool,
            pointer_calls,
            pointer_address: None,
            drops,
            panic_on_pointer,
        }
    }

    fn retained_constructor_buffer_with_address(
        pointer_calls: Rc<Cell<usize>>,
        pointer_address: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        panic_on_pointer: bool,
    ) -> RetainedConstructorBuffer {
        RetainedConstructorBuffer {
            bytes: Box::new([0; 32]),
            reenter_pool: None,
            pointer_calls,
            pointer_address: Some(pointer_address),
            drops,
            panic_on_pointer,
        }
    }

    fn assert_lean_sctp_builder_panic_reclaims<F>(
        make_future: impl Fn(RetainedConstructorBuffer) -> F,
        state_ptr: impl Fn(&F) -> *mut CompletionState,
    ) where
        F: Future<Output = (io::Result<usize>, RetainedConstructorBuffer)>,
    {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let first_pointer_calls = Rc::new(Cell::new(0));
            let first_pointer_address = Rc::new(Cell::new(0));
            let first_drops = Rc::new(Cell::new(0));
            let mut future = make_future(retained_constructor_buffer_with_address(
                Rc::clone(&first_pointer_calls),
                Rc::clone(&first_pointer_address),
                Rc::clone(&first_drops),
                true,
            ));

            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                unsafe { Pin::new_unchecked(&mut future) }.poll(cx)
            }));
            assert!(unwind.is_err(), "buffer pointer callback should unwind");
            assert_eq!(first_pointer_calls.get(), 1);
            assert_ne!(
                first_pointer_address.get(),
                0,
                "retained payload address was not observed"
            );
            assert_eq!(
                first_drops.get(),
                1,
                "aborted retained payload did not drop exactly once"
            );

            let aborted_state = state_ptr(&future);
            assert!(
                aborted_state.is_null(),
                "pre-push builder panic published an operation state"
            );
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);

            let after_unwind = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(after_unwind.pooled_allocs, 1);
            assert_eq!(after_unwind.pooled_frees, 1);
            assert_eq!(after_unwind.pooled_reuses, 0);
            assert_eq!(after_unwind.heap_fallbacks, 0);
            assert_eq!(after_unwind.heap_frees, 0);

            assert!(
                unsafe { Pin::new_unchecked(&mut future) }
                    .poll(cx)
                    .is_pending(),
                "a caught builder panic exposed a fabricated completion"
            );
            assert_eq!(state_ptr(&future), aborted_state);

            test_hooks::fail_next_raw_sqe_submit();
            let mut first_panic = None;
            unsafe { Reactor::prepare_shutdown_unchecked(reactor, &mut first_panic) };
            assert!(first_panic.is_none());
            assert_eq!(
                test_hooks::raw_sqe_submit_failures_remaining(),
                1,
                "shutdown attempted cancellation for a never-submitted state"
            );
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            drop(future);
            assert_eq!(
                test_hooks::raw_sqe_submit_failures_remaining(),
                1,
                "dropping a never-submitted state attempted cancellation"
            );
            let injected = test_hooks::take_raw_sqe_submit_failure()
                .expect("unconsumed cancellation sentinel disappeared");
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            assert_eq!(first_drops.get(), 1);

            let replacement = unsafe { (&mut *reactor).alloc_op() };
            assert!(
                !replacement.is_null(),
                "pre-push panic did not return its operation slot"
            );
            unsafe { (&mut *reactor).free_op(replacement) };
            assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);

            let retry_pointer_calls = Rc::new(Cell::new(0));
            let retry_pointer_address = Rc::new(Cell::new(0));
            let retry_drops = Rc::new(Cell::new(0));
            let mut retry = make_future(retained_constructor_buffer_with_address(
                Rc::clone(&retry_pointer_calls),
                Rc::clone(&retry_pointer_address),
                Rc::clone(&retry_drops),
                false,
            ));
            let retry_poll = unsafe { Pin::new_unchecked(&mut retry) }.poll(cx);
            let (result, returned) = match retry_poll {
                Poll::Ready(completion) => completion,
                Poll::Pending => panic!("ringless retry unexpectedly submitted an SQE"),
            };
            assert!(result.is_err(), "ringless retry unexpectedly succeeded");
            assert_eq!(retry_pointer_calls.get(), 1);
            assert_eq!(
                retry_pointer_address.get(),
                first_pointer_address.get(),
                "retained payload block was not reused exactly"
            );
            drop(returned);
            drop(retry);
            assert_eq!(retry_drops.get(), 1);

            let final_stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(final_stats.pooled_allocs, 2);
            assert_eq!(final_stats.pooled_reuses, 1);
            assert_eq!(final_stats.pooled_frees, 2);
            assert_eq!(final_stats.heap_fallbacks, 0);
            assert_eq!(final_stats.heap_frees, 0);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[test]
    fn lean_sctp_receive_builder_panic_reclaims_payload_and_operation() {
        assert_lean_sctp_builder_panic_reclaims(
            |buffer| DataRecvFuture {
                fd: RuntimeFd::INVALID,
                state_ptr: invalid_fd_op_state(),
                buffer: Some(buffer),
                write_base_len: 0,
                len: 16,
                input_error: None,
                _marker: PhantomData::<&'static mut SctpStream>,
            },
            |future: &DataRecvFuture<'_, RetainedConstructorBuffer>| future.state_ptr.state_ptr(),
        );
    }

    #[test]
    fn lean_sctp_send_builder_panic_reclaims_payload_and_operation() {
        assert_lean_sctp_builder_panic_reclaims(
            |buffer| DataSendFuture {
                fd: RuntimeFd::INVALID,
                state_ptr: invalid_fd_op_state(),
                buffer: Some(buffer),
                len: 16,
                input_error: None,
                _marker: PhantomData::<&'static mut SctpStream>,
            },
            |future: &DataSendFuture<'_, RetainedConstructorBuffer>| future.state_ptr.state_ptr(),
        );
    }

    #[test]
    fn stashed_sctp_receive_pointer_panic_returns_completed_state() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state_ptr.is_null(), "stashed operation allocation failed");

            let pointer_calls = Rc::new(Cell::new(0));
            let pointer_address = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let mut buffer = Some(retained_constructor_buffer_with_address(
                Rc::clone(&pointer_calls),
                Rc::clone(&pointer_address),
                Rc::clone(&drops),
                false,
            ));
            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
            let mut payload =
                unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut buffer, 16) };
            assert!(buffer.is_none(), "constructor did not retain the buffer");
            let retained_address = unsafe { payload.as_ref() as *const _ as usize };
            unsafe {
                payload.as_mut().buffer.panic_on_pointer = true;
                (*state_ptr).attach_retained_payload(payload);
                (*state_ptr).result = 4;
                (*state_ptr).set_completed();
            }

            let mut recv_state = SctpRecvState::external();
            unsafe {
                recv_state.set_stashed_live_for_test(
                    state_ptr,
                    0,
                    process_stashed_sctp_recv::<RetainedConstructorBuffer>,
                );
            }

            test_hooks::fail_next_raw_sqe_submit();
            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                recv_state.poll_stashed(cx)
            }));
            assert!(
                unwind.is_err(),
                "stashed buffer pointer callback should unwind"
            );
            assert!(recv_state.stashed.state_ptr.is_null());
            assert!(recv_state.stashed.process_completed.is_none());
            assert_eq!(pointer_calls.get(), 2);
            assert_eq!(drops.get(), 1);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            assert_eq!(
                test_hooks::raw_sqe_submit_failures_remaining(),
                1,
                "stashed completion cleanup unexpectedly submitted an SQE"
            );
            let injected = test_hooks::take_raw_sqe_submit_failure()
                .expect("stashed cleanup consumed the submission sentinel");
            assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 1);
            assert_eq!(stats.pooled_frees, 1);
            assert_eq!(stats.pooled_reuses, 0);
            assert_eq!(stats.heap_fallbacks, 0);

            let replacement = unsafe { (&mut *reactor).alloc_op() };
            assert_eq!(
                replacement, state_ptr,
                "stashed completion did not recycle its operation slot"
            );
            unsafe { (&mut *reactor).free_op(replacement) };

            let retry_calls = Rc::new(Cell::new(0));
            let retry_address = Rc::new(Cell::new(0));
            let retry_drops = Rc::new(Cell::new(0));
            let mut retry_buffer = Some(retained_constructor_buffer_with_address(
                Rc::clone(&retry_calls),
                Rc::clone(&retry_address),
                Rc::clone(&retry_drops),
                false,
            ));
            let retry =
                unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut retry_buffer, 16) };
            assert_eq!(
                unsafe { retry.as_ref() as *const _ as usize },
                retained_address,
                "stashed retained block was not reused exactly"
            );
            drop(unsafe { retry.take(&mut *retained_pool.as_ptr()) });
            assert_eq!(retry_calls.get(), 1);
            assert_eq!(retry_drops.get(), 1);

            let final_stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(final_stats.pooled_allocs, 2);
            assert_eq!(final_stats.pooled_reuses, 1);
            assert_eq!(final_stats.pooled_frees, 2);
            assert_eq!(final_stats.heap_fallbacks, 0);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
        });
    }

    fn assert_retained_sctp_send_fields(
        msg: &libc::msghdr,
        control: &[u8],
        iov: *mut libc::iovec,
        iovlen: usize,
        expected: libc::sctp_sndinfo,
    ) {
        assert!(msg.msg_name.is_null());
        assert_eq!(msg.msg_namelen, 0);
        assert_eq!(msg.msg_iov, iov);
        assert_eq!(msg.msg_iovlen, iovlen);
        assert_eq!(msg.msg_control, control.as_ptr().cast_mut().cast());
        assert_eq!(msg.msg_controllen, control.len());
        assert_eq!(msg.msg_flags, 0);

        let hdr = unsafe { std::ptr::read_unaligned(control.as_ptr().cast::<libc::cmsghdr>()) };
        assert_eq!(
            hdr.cmsg_len,
            std::mem::size_of::<libc::cmsghdr>() + std::mem::size_of::<libc::sctp_sndinfo>()
        );
        assert_eq!(hdr.cmsg_level, libc::IPPROTO_SCTP);
        assert_eq!(hdr.cmsg_type, libc::SCTP_SNDINFO);
        let info = unsafe {
            std::ptr::read_unaligned(
                control
                    .as_ptr()
                    .add(cmsg_align(std::mem::size_of::<libc::cmsghdr>()))
                    .cast::<libc::sctp_sndinfo>(),
            )
        };
        assert_eq!(info.snd_sid, expected.snd_sid);
        assert_eq!(info.snd_flags, expected.snd_flags);
        assert_eq!(info.snd_ppid, expected.snd_ppid);
        assert_eq!(info.snd_context, expected.snd_context);
        assert_eq!(info.snd_assoc_id, expected.snd_assoc_id);
    }

    #[cfg(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_pointer_width = "64"
    ))]
    fn expected_sctp_sndinfo_control(
        sndinfo: libc::sctp_sndinfo,
    ) -> [u8; SCTP_SNDINFO_CONTROL_LEN] {
        let mut expected = [0_u8; SCTP_SNDINFO_CONTROL_LEN];
        expected[0..8].copy_from_slice(&32_usize.to_ne_bytes());
        expected[8..12].copy_from_slice(&libc::IPPROTO_SCTP.to_ne_bytes());
        expected[12..16].copy_from_slice(&libc::SCTP_SNDINFO.to_ne_bytes());
        expected[16..18].copy_from_slice(&sndinfo.snd_sid.to_ne_bytes());
        expected[18..20].copy_from_slice(&sndinfo.snd_flags.to_ne_bytes());
        expected[20..24].copy_from_slice(&sndinfo.snd_ppid.to_ne_bytes());
        expected[24..28].copy_from_slice(&sndinfo.snd_context.to_ne_bytes());
        expected[28..32].copy_from_slice(&sndinfo.snd_assoc_id.to_ne_bytes());
        expected
    }

    #[cfg(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_pointer_width = "64"
    ))]
    #[test]
    fn sctp_sndinfo_control_has_exact_all_field_bytes() {
        let sndinfo = libc::sctp_sndinfo {
            snd_sid: 0x1122,
            snd_flags: 0x3344,
            snd_ppid: 0x5566_7788,
            snd_context: 0x99aa_bbcc,
            snd_assoc_id: -0x0102_0304,
        };
        let mut control = [0xa5_u8; SCTP_SNDINFO_CONTROL_LEN];

        write_cmsg_sndinfo(&mut control, sndinfo);

        assert_eq!(control, expected_sctp_sndinfo_control(sndinfo));
    }

    #[cfg(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_pointer_width = "64"
    ))]
    #[test]
    fn sctp_sndinfo_control_has_exact_default_bytes() {
        let sndinfo = raw_sndinfo_from_public(SctpSendInfo::default());
        let mut control = [0xa5_u8; SCTP_SNDINFO_CONTROL_LEN];

        write_cmsg_sndinfo(&mut control, sndinfo);

        assert_eq!(control, expected_sctp_sndinfo_control(sndinfo));
        assert!(
            control[SCTP_SNDINFO_DATA_OFFSET..]
                .iter()
                .all(|byte| *byte == 0)
        );
    }

    #[test]
    fn sctp_owned_fd_adoption_transfers_exact_close_ownership() {
        let raw = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("distinctive fd creation failed");
        // SAFETY: the test helper returned one descriptor owned only here.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 3868));
        let stream = SctpStream::from_owned_fd(owned, peer);

        assert_eq!(stream.as_raw_fd(), raw);
        assert_eq!(stream.peer_addr(), peer);
        drop(stream);
        assert!(
            crate::runtime::fd::raw_fd_is_closed(raw),
            "SCTP OwnedFd transfer did not close exactly once"
        );
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

        if finish_accepted_stream(owned, &addr, 0, SctpSocketConfig::default()).is_ok() {
            panic!("zero-length accepted address should fail");
        }
        assert!(
            crate::runtime::fd::raw_fd_is_closed(raw),
            "accepted SCTP fd leaked on address decode failure"
        );
    }

    #[test]
    fn accepted_configuration_transaction_releases_or_returns_one_owner() {
        struct DropProbe<'a>(&'a Cell<usize>);

        impl Drop for DropProbe<'_> {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let failed_drops = Cell::new(0_usize);
        let err = match configure_accepted_owner(DropProbe(&failed_drops), |_| {
            Err(io::Error::from_raw_os_error(libc::EIO))
        }) {
            Ok(owner) => {
                drop(owner);
                panic!("failed accepted configuration returned its owner")
            }
            Err(err) => err,
        };
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_eq!(failed_drops.get(), 1);

        let successful_drops = Cell::new(0_usize);
        let owner = configure_accepted_owner(DropProbe(&successful_drops), |_| Ok(()))
            .expect("successful accepted configuration lost its owner");
        assert_eq!(successful_drops.get(), 0);
        drop(owner);
        assert_eq!(successful_drops.get(), 1);
    }

    #[test]
    fn completion_cqe_result_maps_error_zero_and_progress() {
        let err =
            completion_cqe_result(-libc::EPIPE).expect_err("negative CQE should map to errno");
        assert_eq!(err.raw_os_error(), Some(libc::EPIPE));
        assert_eq!(
            completion_cqe_result(0).expect("zero CQE should succeed"),
            0
        );
        assert_eq!(
            completion_cqe_result(9).expect("positive CQE should succeed"),
            9
        );
    }

    fn initialized_control(bytes: &[u8]) -> &[MaybeUninit<u8>] {
        // SAFETY: initialized bytes may always be viewed as MaybeUninit bytes.
        unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<MaybeUninit<u8>>(), bytes.len()) }
    }

    fn parse_recv_meta_with_notification_for_test(
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
        parsed_notification: Option<io::Result<SctpRecvMeta>>,
    ) -> io::Result<SctpRecvMeta> {
        parse_recv_meta_with_policy_for_test(
            control,
            controllen,
            msg_flags,
            data_slice,
            false,
            parsed_notification,
        )
    }

    fn parse_recv_meta_with_policy_for_test(
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
        recv_rcvinfo_requested: bool,
        parsed_notification: Option<io::Result<SctpRecvMeta>>,
    ) -> io::Result<SctpRecvMeta> {
        // SAFETY: this test facade accepts fully initialized control bytes.
        unsafe {
            parse_recv_meta_with_notification(
                initialized_control(control),
                controllen,
                msg_flags,
                data_slice,
                recv_rcvinfo_requested,
                parsed_notification,
            )
        }
    }

    fn parse_recv_state_meta_for_test(
        recv_state: &SctpRecvState,
        control: &[u8],
        controllen: usize,
        msg_flags: libc::c_int,
        data_slice: &[u8],
        parsed_notification: Option<io::Result<SctpRecvMeta>>,
    ) -> io::Result<SctpRecvMeta> {
        parse_initialized_recv_state_meta_for_test(
            recv_state,
            control,
            controllen,
            msg_flags,
            data_slice,
            parsed_notification,
        )
    }

    fn append_test_rcvinfo(control: &mut Vec<u8>) {
        let data_offset = append_initialized_test_cmsg(
            control,
            libc::IPPROTO_SCTP,
            libc::SCTP_RCVINFO,
            std::mem::size_of::<libc::sctp_rcvinfo>(),
        );

        let info = libc::sctp_rcvinfo {
            rcv_sid: 3,
            rcv_ssn: 4,
            rcv_flags: 5,
            rcv_ppid: 0x0607_0809_u32.to_be(),
            rcv_tsn: 10,
            rcv_cumtsn: 11,
            rcv_context: 12,
            rcv_assoc_id: 13,
        };
        macro_rules! write_info_field {
            ($field:ident) => {{
                let value = info.$field;
                let offset = data_offset + std::mem::offset_of!(libc::sctp_rcvinfo, $field);
                let bytes = value.to_ne_bytes();
                control[offset..offset + bytes.len()].copy_from_slice(&bytes);
            }};
        }
        write_info_field!(rcv_sid);
        write_info_field!(rcv_ssn);
        write_info_field!(rcv_flags);
        write_info_field!(rcv_ppid);
        write_info_field!(rcv_tsn);
        write_info_field!(rcv_cumtsn);
        write_info_field!(rcv_context);
        write_info_field!(rcv_assoc_id);
    }

    fn expected_test_rcvinfo(end_of_record: bool) -> SctpRecvInfo {
        SctpRecvInfo {
            stream_id: 3,
            ssn: 4,
            flags: 5,
            ppid: 0x0607_0809,
            tsn: 10,
            cumtsn: 11,
            context: 12,
            assoc_id: 13,
            end_of_record,
        }
    }

    #[test]
    fn test_cmsg_header_writer_preserves_payload_and_padding() {
        const OFFSET: usize = 3;
        const PAYLOAD_LEN: usize = 4;
        const SENTINEL: u8 = 0xa5;
        let data_offset = cmsg_align(std::mem::size_of::<libc::cmsghdr>());
        let mut control = [MaybeUninit::new(SENTINEL); 64];

        let payload_offset = write_test_cmsg_header(
            &mut control,
            OFFSET,
            libc::SOL_SOCKET,
            libc::SO_RXQ_OVFL,
            PAYLOAD_LEN,
        );
        assert_eq!(payload_offset, OFFSET + data_offset);

        let mut expected = [SENTINEL; 64];
        let cmsg_len = data_offset + PAYLOAD_LEN;
        let len_offset = OFFSET + std::mem::offset_of!(libc::cmsghdr, cmsg_len);
        expected[len_offset..len_offset + std::mem::size_of::<usize>()]
            .copy_from_slice(&cmsg_len.to_ne_bytes());
        let level_offset = OFFSET + std::mem::offset_of!(libc::cmsghdr, cmsg_level);
        expected[level_offset..level_offset + std::mem::size_of::<libc::c_int>()]
            .copy_from_slice(&libc::SOL_SOCKET.to_ne_bytes());
        let type_offset = OFFSET + std::mem::offset_of!(libc::cmsghdr, cmsg_type);
        expected[type_offset..type_offset + std::mem::size_of::<libc::c_int>()]
            .copy_from_slice(&libc::SO_RXQ_OVFL.to_ne_bytes());

        // SAFETY: every byte started initialized, and the writer stores only
        // initialized byte values.
        let actual =
            unsafe { std::slice::from_raw_parts(control.as_ptr().cast::<u8>(), control.len()) };
        assert_eq!(actual, expected);
    }

    #[test]
    fn parse_rcvinfo_scans_bounded_observability_prelude() {
        let mut control = Vec::new();
        append_initialized_test_cmsg(
            &mut control,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            std::mem::size_of::<LinuxKernelTimespec>(),
        );
        append_initialized_test_cmsg(
            &mut control,
            libc::SOL_SOCKET,
            libc::SCM_TIMESTAMPING_PKTINFO,
            std::mem::size_of::<LinuxScmTimestampingPktInfo>(),
        );
        append_initialized_test_cmsg(
            &mut control,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPING,
            std::mem::size_of::<LinuxScmTimestamping>(),
        );
        append_initialized_test_cmsg(
            &mut control,
            libc::SOL_SOCKET,
            libc::SO_RXQ_OVFL,
            std::mem::size_of::<LinuxSocketRxqOverflow>(),
        );
        append_test_rcvinfo(&mut control);

        assert_eq!(control.len(), SCTP_RECV_CONTROL_LEN);
        let info = unsafe { parse_rcvinfo(initialized_control(&control), control.len(), true) }
            .expect("bounded control chain should be well formed")
            .expect("RCVINFO after generic control should be found");
        assert_eq!(info, expected_test_rcvinfo(true));
    }

    #[test]
    fn parse_rcvinfo_defaults_complete_unrelated_control() {
        let mut control = Vec::new();
        append_initialized_test_cmsg(
            &mut control,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            2 * std::mem::size_of::<i64>(),
        );

        let parsed = parse_recv_meta_with_notification_for_test(
            &control,
            control.len(),
            libc::MSG_EOR,
            b"payload",
            None,
        )
        .expect("complete unrelated control should default SCTP metadata");
        assert_eq!(
            parsed,
            SctpRecvMeta::Data(SctpRecvInfo {
                end_of_record: true,
                ..SctpRecvInfo::default()
            })
        );
    }

    #[test]
    fn recv_state_rejects_absent_requested_rcvinfo_without_changing_precedence() {
        let requested =
            SctpRecvState::configured(SctpSocketConfig::rich(SctpInitConfig::default()));
        let disabled = SctpRecvState::configured(SctpSocketConfig::data(SctpInitConfig::default()));
        let external = SctpRecvState::external();
        assert!(requested.recv_rcvinfo_requested.get());
        assert!(!disabled.recv_rcvinfo_requested.get());
        assert!(!external.recv_rcvinfo_requested.get());

        let expected_default = SctpRecvMeta::Data(SctpRecvInfo {
            end_of_record: true,
            ..SctpRecvInfo::default()
        });
        for state in [&disabled, &external] {
            assert_eq!(
                parse_recv_state_meta_for_test(state, &[], 0, libc::MSG_EOR, b"payload", None,)
                    .expect("metadata-disabled data should retain default ancillary fields"),
                expected_default
            );
        }

        let missing =
            parse_recv_state_meta_for_test(&requested, &[], 0, libc::MSG_EOR, b"payload", None)
                .expect_err("requested RCVINFO absence must not silently default");
        assert_eq!(missing.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            missing.to_string(),
            "SCTP recvmsg omitted requested SCTP_RCVINFO"
        );

        let mut unrelated = Vec::new();
        append_initialized_test_cmsg(
            &mut unrelated,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            2 * std::mem::size_of::<i64>(),
        );
        assert_eq!(
            parse_recv_state_meta_for_test(
                &disabled,
                &unrelated,
                unrelated.len(),
                libc::MSG_EOR,
                b"payload",
                None,
            )
            .expect("complete unrelated control should still default when metadata is disabled"),
            expected_default
        );
        assert_eq!(
            parse_recv_state_meta_for_test(
                &requested,
                &unrelated,
                unrelated.len(),
                libc::MSG_EOR,
                b"payload",
                None,
            )
            .expect_err("a complete unrelated cmsg cannot satisfy requested RCVINFO")
            .to_string(),
            "SCTP recvmsg omitted requested SCTP_RCVINFO"
        );

        let malformed = [0u8; std::mem::size_of::<libc::cmsghdr>()];
        let malformed_error = parse_recv_state_meta_for_test(
            &requested,
            &malformed,
            malformed.len(),
            libc::MSG_EOR,
            b"payload",
            None,
        )
        .expect_err("malformed control must precede requested-info absence");
        assert_eq!(
            malformed_error.to_string(),
            "SCTP recvmsg control message length was malformed"
        );

        for state in [&requested, &disabled] {
            let control_truncated = parse_recv_state_meta_for_test(
                state,
                &[],
                0,
                libc::MSG_EOR | libc::MSG_CTRUNC,
                b"payload",
                None,
            )
            .expect_err("control capacity exhaustion must fail regardless of RCVINFO policy");
            assert_eq!(control_truncated.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                control_truncated.to_string(),
                "SCTP recvmsg fixed control buffer capacity was exhausted"
            );
        }

        let payload_truncated = parse_recv_state_meta_for_test(
            &requested,
            &[],
            0,
            libc::MSG_EOR | libc::MSG_TRUNC,
            b"payload",
            None,
        )
        .expect_err("payload truncation must precede requested-info absence");
        assert_eq!(
            payload_truncated.to_string(),
            "SCTP recvmsg payload was truncated"
        );

        let partial = parse_recv_state_meta_for_test(&requested, &[], 0, 0, b"payload", None)
            .expect_err("partial records must precede requested-info absence");
        assert_eq!(
            partial.to_string(),
            "SCTP recvmsg payload was partial before end-of-record"
        );

        let mut notification = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut notification, 8, 42);
        let parsed_notification =
            parse_sctp_notification_once(&notification, libc::MSG_NOTIFICATION | libc::MSG_EOR);
        assert!(matches!(
            parse_recv_state_meta_for_test(
                &requested,
                &[],
                0,
                libc::MSG_NOTIFICATION | libc::MSG_EOR,
                &notification,
                parsed_notification,
            ),
            Ok(SctpRecvMeta::Notification(SctpNotification::Shutdown {
                assoc_id: 42
            }))
        ));
    }

    #[test]
    fn recv_state_receive_policy_refresh_changes_absent_rcvinfo_classification() {
        let state = SctpRecvState::external();
        let expected_default = SctpRecvMeta::Data(SctpRecvInfo {
            end_of_record: true,
            ..SctpRecvInfo::default()
        });

        assert_eq!(
            parse_recv_state_meta_for_test(&state, &[], 0, libc::MSG_EOR, b"payload", None)
                .expect("external receive policy should initially default absent RCVINFO"),
            expected_default
        );

        state.set_receive_policy(SctpNotificationMask::none(), true);
        assert!(state.recv_rcvinfo_requested.get());
        assert!(!state.partial_delivery_visible.get());
        assert!(!state.any_notification_visible.get());
        let missing =
            parse_recv_state_meta_for_test(&state, &[], 0, libc::MSG_EOR, b"payload", None)
                .expect_err("refreshed receive-info request must make absence invalid");
        assert_eq!(missing.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            missing.to_string(),
            "SCTP recvmsg omitted requested SCTP_RCVINFO"
        );

        let visible = SctpNotificationMask {
            partial_delivery: true,
            ..SctpNotificationMask::none()
        };
        state.set_receive_policy(visible, false);
        assert!(!state.recv_rcvinfo_requested.get());
        assert!(state.partial_delivery_visible.get());
        assert!(state.any_notification_visible.get());
        assert_eq!(
            parse_recv_state_meta_for_test(&state, &[], 0, libc::MSG_EOR, b"payload", None)
                .expect("disabled receive-info policy should restore default metadata"),
            expected_default
        );
    }

    #[test]
    fn notification_mask_failure_retains_receive_policy_until_success() {
        let state = SctpRecvState::external();
        let requested = SctpNotificationMask::none();
        let forced_pdapi = SctpNotificationMask {
            partial_delivery: true,
            ..SctpNotificationMask::none()
        };
        let mut observed = None;

        let err = apply_sctp_notification_mask(&state, requested, true, |effective| {
            observed = Some(effective);
            Err(io::Error::from_raw_os_error(libc::EIO))
        })
        .expect_err("injected notification-mask failure unexpectedly succeeded");
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_eq!(observed, Some(forced_pdapi));
        assert_eq!(
            (
                state.recv_rcvinfo_requested.get(),
                state.partial_delivery_visible.get(),
                state.any_notification_visible.get(),
            ),
            (false, true, true),
            "failed kernel update published a new userspace receive policy"
        );

        apply_sctp_notification_mask(&state, requested, true, |effective| {
            assert_eq!(effective, forced_pdapi);
            Ok(())
        })
        .expect("notification-mask retry should succeed");
        assert_eq!(
            (
                state.recv_rcvinfo_requested.get(),
                state.partial_delivery_visible.get(),
                state.any_notification_visible.get(),
            ),
            (true, false, false)
        );
    }

    #[test]
    fn parse_rcvinfo_rejects_malformed_preceding_records_without_overrun() {
        let hdr_len = std::mem::size_of::<libc::cmsghdr>();
        let len_offset = std::mem::offset_of!(libc::cmsghdr, cmsg_len);

        let short_header = vec![0u8; hdr_len - 1];
        assert!(
            unsafe { parse_rcvinfo(initialized_control(&short_header), short_header.len(), true) }
                .is_err()
        );

        for malformed_len in [0, cmsg_align(hdr_len) - 1, hdr_len + 1] {
            let mut control = vec![0u8; hdr_len];
            control[len_offset..len_offset + std::mem::size_of::<usize>()]
                .copy_from_slice(&malformed_len.to_ne_bytes());
            assert!(
                unsafe { parse_rcvinfo(initialized_control(&control), control.len(), true) }
                    .is_err(),
                "malformed cmsg_len {malformed_len} was accepted"
            );
        }

        let mut overlong = vec![0u8; hdr_len];
        overlong[len_offset..len_offset + std::mem::size_of::<usize>()]
            .copy_from_slice(&usize::MAX.to_ne_bytes());
        assert!(
            unsafe { parse_rcvinfo(initialized_control(&overlong), usize::MAX, true) }.is_err(),
            "an overlong reported record must not read beyond backing storage"
        );
    }

    #[test]
    fn parse_rcvinfo_preserves_specific_diagnostics_when_control_is_truncated() {
        let hdr_len = std::mem::size_of::<libc::cmsghdr>();
        let short_header = vec![0u8; hdr_len - 1];
        let malformed_length = vec![0u8; hdr_len];
        let mut truncated_rcvinfo = Vec::new();
        append_initialized_test_cmsg(
            &mut truncated_rcvinfo,
            libc::IPPROTO_SCTP,
            libc::SCTP_RCVINFO,
            0,
        );

        for flags in [libc::MSG_EOR, libc::MSG_EOR | libc::MSG_CTRUNC] {
            let header_error = parse_recv_meta_with_policy_for_test(
                &short_header,
                short_header.len(),
                flags,
                b"payload",
                true,
                None,
            )
            .expect_err("a partial control header must fail");
            assert_eq!(header_error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                header_error.to_string(),
                "SCTP recvmsg control message header was malformed"
            );

            let length_error = parse_recv_meta_with_policy_for_test(
                &malformed_length,
                malformed_length.len(),
                flags,
                b"payload",
                true,
                None,
            )
            .expect_err("an invalid control-message length must fail");
            assert_eq!(length_error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                length_error.to_string(),
                "SCTP recvmsg control message length was malformed"
            );

            let rcvinfo_error = parse_recv_meta_with_policy_for_test(
                &truncated_rcvinfo,
                truncated_rcvinfo.len(),
                flags,
                b"payload",
                true,
                None,
            )
            .expect_err("a short SCTP_RCVINFO payload must fail");
            assert_eq!(rcvinfo_error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                rcvinfo_error.to_string(),
                "SCTP_RCVINFO control message was truncated"
            );
        }
    }

    #[test]
    fn parse_rcvinfo_keeps_intact_target_when_later_control_is_truncated() {
        let mut control = Vec::new();
        append_test_rcvinfo(&mut control);
        control.push(0xA5);

        let parsed = parse_recv_meta_with_policy_for_test(
            &control,
            control.len(),
            libc::MSG_EOR | libc::MSG_CTRUNC,
            b"x",
            true,
            None,
        )
        .expect("intact RCVINFO should precede irrelevant truncated control");
        assert!(matches!(
            parsed,
            SctpRecvMeta::Data(SctpRecvInfo {
                stream_id: 3,
                ppid: 0x0607_0809,
                end_of_record: true,
                ..
            })
        ));
    }

    #[test]
    fn parse_rcvinfo_skips_uninitialized_alignment_padding() {
        const CONTROL_LEN: usize = SOCKET_RXQ_OVFL_CONTROL_LEN + SCTP_RCVINFO_CONTROL_LEN;
        let mut control = [MaybeUninit::uninit(); CONTROL_LEN];

        let overflow_data = write_test_cmsg_header(
            &mut control,
            0,
            libc::SOL_SOCKET,
            libc::SO_RXQ_OVFL,
            std::mem::size_of::<u32>(),
        );
        for (slot, byte) in control[overflow_data..overflow_data + 4]
            .iter_mut()
            .zip(7_u32.to_ne_bytes())
        {
            slot.write(byte);
        }

        let rcvinfo_offset = SOCKET_RXQ_OVFL_CONTROL_LEN;
        let rcvinfo_data = write_test_cmsg_header(
            &mut control,
            rcvinfo_offset,
            libc::IPPROTO_SCTP,
            libc::SCTP_RCVINFO,
            std::mem::size_of::<libc::sctp_rcvinfo>(),
        );
        for slot in
            &mut control[rcvinfo_data..rcvinfo_data + std::mem::size_of::<libc::sctp_rcvinfo>()]
        {
            slot.write(0);
        }

        let info = unsafe { parse_rcvinfo(&control, control.len(), true) }
            .expect("uninitialized CMSG alignment padding must not be inspected")
            .expect("RCVINFO after the uninitialized padding should be found");
        assert!(info.end_of_record);
    }

    #[test]
    fn connected_sctp_recv_payload_footprints_pin_bounded_ancillary_cost() {
        #[cfg(all(target_pointer_width = "64", target_arch = "x86_64"))]
        {
            assert_eq!(std::mem::size_of::<SctpRecordSync>(), 1);
            assert_eq!(std::mem::align_of::<SctpRecordSync>(), 1);
            assert_eq!(std::mem::size_of::<StashedSctpRecvState>(), 1);
            assert_eq!(std::mem::align_of::<StashedSctpRecvState>(), 1);
            assert_eq!(std::mem::size_of::<StashedSctpRecv>(), 24);
            assert_eq!(std::mem::size_of::<SctpRecvState>(), 56);
            assert_eq!(std::mem::align_of::<SctpRecvState>(), 8);
            assert_eq!(std::mem::size_of::<SctpStream>(), 96);
            assert_eq!(std::mem::align_of::<SctpStream>(), 8);
            assert_eq!(SCTP_RCVINFO_CONTROL_LEN, 48);
            assert_eq!(SCTP_RECV_CONTROL_LEN, 200);
            assert_eq!(
                std::mem::size_of::<RetainedSctpRecvPayload<IoBuffMut>>(),
                312
            );
            assert_eq!(
                std::mem::size_of::<RetainedSctpRecvVectoredPayload<2>>(),
                376
            );
            assert_eq!(
                std::mem::size_of::<RetainedSctpRecvVectoredPayload<16>>(),
                1160
            );
            assert_eq!(std::mem::size_of::<SctpRecvCompletionMeta>(), 48);
            assert_eq!(std::mem::size_of::<SctpRecvCompletion<IoBuffMut>>(), 88);
            assert_eq!(std::mem::size_of::<SctpFirstIovec>(), 24);
            assert_eq!(std::mem::size_of::<SctpRecvVectoredCompletion<2>>(), 160);
            assert_eq!(std::mem::size_of::<SctpRecvVectoredCompletion<16>>(), 720);
            assert!(
                std::mem::size_of::<SctpRecvCompletionMeta>() < SCTP_RECV_CONTROL_LEN,
                "parsed completion metadata regained a fixed control copy"
            );
        }
    }

    #[test]
    fn retained_recv_constructor_recycles_raw_slot_after_buffer_callback_panics() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut buffer = Some(retained_constructor_buffer(
            None,
            Rc::clone(&pointer_calls),
            Rc::clone(&drops),
            true,
        ));

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            emplace_retained_sctp_recv_payload(pool_ptr, &mut buffer, 16)
        }));
        assert!(unwind.is_err(), "buffer callback should unwind");
        assert!(buffer.is_some(), "callback panic moved caller buffer");
        assert_eq!(pointer_calls.get(), 1);
        assert_eq!(drops.get(), 0);
        let after_unwind = pool.stats();
        assert_eq!(after_unwind.pooled_allocs, 1);
        assert_eq!(after_unwind.pooled_frees, 1);
        assert_eq!(after_unwind.pooled_reuses, 0);

        buffer.as_mut().unwrap().panic_on_pointer = false;
        let payload = unsafe { emplace_retained_sctp_recv_payload(pool_ptr, &mut buffer, 16) };
        assert!(buffer.is_none());
        let after_retry = pool.stats();
        assert_eq!(after_retry.pooled_allocs, 2);
        assert_eq!(after_retry.pooled_frees, 1);
        assert_eq!(after_retry.pooled_reuses, 1);

        let returned = unsafe { payload.take(&mut pool) };
        drop(returned);
        assert_eq!(pointer_calls.get(), 2);
        assert_eq!(drops.get(), 1);
        assert_eq!(pool.stats().pooled_frees, 2);
    }

    #[test]
    fn retained_recv_constructor_omits_peer_address_and_keeps_stable_pointers() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut buffer = Some(retained_constructor_buffer(
            Some(pool_ptr),
            Rc::clone(&pointer_calls),
            Rc::clone(&drops),
            false,
        ));

        let payload = unsafe { emplace_retained_sctp_recv_payload(pool_ptr, &mut buffer, 23) };
        assert!(buffer.is_none());
        assert_eq!(pointer_calls.get(), 1);

        let retained = unsafe { payload.as_ref() };
        let iovec = unsafe { retained.iovec.assume_init_ref() };
        let msg = unsafe { retained.msghdr.assume_init_ref() };
        let expected_ptr = retained.buffer.bytes.as_ptr();
        assert_eq!(iovec.iov_base, expected_ptr as *mut _);
        assert_eq!(iovec.iov_len, 23);
        assert_eq!(msg.msg_iov, retained.iovec.as_ptr() as *mut libc::iovec);
        assert_eq!(msg.msg_iovlen, 1);
        assert!(
            msg.msg_name.is_null(),
            "connected SCTP receive unexpectedly requested a peer address"
        );
        assert_eq!(msg.msg_namelen, 0);
        assert_eq!(msg.msg_control, retained.control.as_ptr() as *mut _);
        assert_eq!(msg.msg_controllen, retained.control.len());

        let returned = unsafe { payload.take(&mut pool) };
        drop(returned);
        assert_eq!(drops.get(), 1);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2, "outer plus reentrant allocation");
        assert_eq!(stats.pooled_frees, 2, "outer plus reentrant release");
    }

    #[test]
    fn sctp_scalar_completion_extraction_is_callback_free_and_preserves_owner() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut buffer = Some(retained_constructor_buffer(
            None,
            Rc::clone(&pointer_calls),
            Rc::clone(&drops),
            false,
        ));

        let mut payload = unsafe { emplace_retained_sctp_recv_payload(pool_ptr, &mut buffer, 16) };
        let retained_addr = unsafe { payload.as_mut() as *mut _ as usize };
        let expected_buffer_ptr = unsafe { payload.as_ref() }.buffer.bytes.as_ptr();
        const TAIL_POISON: u8 = 0xa5;
        let mut expected_control = Vec::new();
        append_test_rcvinfo(&mut expected_control);
        unsafe {
            let retained = payload.as_mut();
            std::ptr::write_bytes(
                retained.control.as_mut_ptr().cast::<u8>(),
                TAIL_POISON,
                retained.control.len(),
            );
            std::ptr::copy_nonoverlapping(
                expected_control.as_ptr(),
                retained.control.as_mut_ptr().cast::<u8>(),
                expected_control.len(),
            );
            let msg = retained.msghdr.assume_init_mut();
            msg.msg_controllen = expected_control.len();
            msg.msg_flags = libc::MSG_EOR | libc::MSG_CTRUNC;
        }
        assert_eq!(pointer_calls.get(), 1);

        let completion = unsafe {
            payload.take_with(&mut pool, |payload| {
                take_sctp_recv_completion(payload, Some(1))
            })
        };
        assert_eq!(
            pointer_calls.get(),
            1,
            "completion extraction invoked a caller buffer callback"
        );
        assert_eq!(drops.get(), 0, "completion extraction dropped the owner");
        assert_eq!(completion.buffer.bytes.as_ptr(), expected_buffer_ptr);
        assert_eq!(
            completion.meta.header.msg_controllen,
            expected_control.len()
        );
        assert_eq!(
            completion.meta.header.msg_flags,
            libc::MSG_EOR | libc::MSG_CTRUNC
        );
        assert_eq!(
            completion
                .meta
                .rcvinfo
                .as_ref()
                .expect("valid scalar RCVINFO should parse before extraction"),
            &Some(expected_test_rcvinfo(true))
        );
        assert_eq!(pool.stats().pooled_frees, 1);

        let retry_pointer_calls = Rc::new(Cell::new(0));
        let retry_drops = Rc::new(Cell::new(0));
        let mut retry_buffer = Some(retained_constructor_buffer(
            None,
            Rc::clone(&retry_pointer_calls),
            Rc::clone(&retry_drops),
            false,
        ));
        let pool_ptr = NonNull::from(&mut pool);
        let mut retry =
            unsafe { emplace_retained_sctp_recv_payload(pool_ptr, &mut retry_buffer, 16) };
        assert_eq!(
            unsafe { retry.as_mut() as *mut _ as usize },
            retained_addr,
            "selective extraction did not recycle the retained block"
        );
        let reused_control = unsafe {
            std::slice::from_raw_parts(
                retry.as_ref().control.as_ptr().cast::<u8>(),
                SCTP_RECV_CONTROL_LEN,
            )
        };
        assert_eq!(
            &reused_control[..expected_control.len()],
            expected_control,
            "scalar receive constructor rewrote the prior control prefix"
        );
        assert!(
            reused_control[expected_control.len()..]
                .iter()
                .all(|byte| *byte == TAIL_POISON),
            "scalar receive constructor cleared the unreported control tail"
        );
        drop(unsafe { retry.take(&mut pool) });

        drop(completion.buffer);
        assert_eq!(drops.get(), 1, "returned owner did not drop exactly once");
        assert_eq!(retry_pointer_calls.get(), 1);
        assert_eq!(retry_drops.get(), 1);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2);
        assert_eq!(stats.pooled_reuses, 1);
        assert_eq!(stats.pooled_frees, 2);
    }

    #[test]
    fn retained_send_constructor_recycles_raw_slot_after_buffer_callback_panics() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut buffer = Some(retained_constructor_buffer(
            None,
            Rc::clone(&pointer_calls),
            Rc::clone(&drops),
            true,
        ));
        let sndinfo = raw_sndinfo_from_public(SctpSendInfo::default());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            emplace_retained_sctp_send_payload(pool_ptr, &mut buffer, 16, sndinfo)
        }));
        assert!(unwind.is_err(), "buffer callback should unwind");
        assert!(buffer.is_some(), "callback panic moved caller buffer");
        assert_eq!(pointer_calls.get(), 1);
        assert_eq!(drops.get(), 0);
        let after_unwind = pool.stats();
        assert_eq!(after_unwind.pooled_allocs, 1);
        assert_eq!(after_unwind.pooled_frees, 1);
        assert_eq!(after_unwind.pooled_reuses, 0);

        buffer.as_mut().unwrap().panic_on_pointer = false;
        let payload =
            unsafe { emplace_retained_sctp_send_payload(pool_ptr, &mut buffer, 16, sndinfo) };
        assert!(buffer.is_none());
        let after_retry = pool.stats();
        assert_eq!(after_retry.pooled_allocs, 2);
        assert_eq!(after_retry.pooled_frees, 1);
        assert_eq!(after_retry.pooled_reuses, 1);

        let returned = unsafe { payload.take(&mut pool) };
        drop(returned);
        assert_eq!(pointer_calls.get(), 2);
        assert_eq!(drops.get(), 1);
        assert_eq!(pool.stats().pooled_frees, 2);
    }

    #[test]
    fn retained_send_constructor_materializes_final_storage_and_preserves_buffer() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let mut buffer = Some(retained_constructor_buffer(
            Some(pool_ptr),
            Rc::clone(&pointer_calls),
            Rc::clone(&drops),
            false,
        ));
        let info = SctpSendInfo {
            stream_id: 7,
            flags: 9,
            ppid: 0x0102_0304,
            context: 0x1122_3344,
            assoc_id: 13,
        };
        let sndinfo = raw_sndinfo_from_public(info);

        let payload =
            unsafe { emplace_retained_sctp_send_payload(pool_ptr, &mut buffer, 23, sndinfo) };
        assert!(buffer.is_none());
        assert_eq!(pointer_calls.get(), 1);

        let retained = unsafe { payload.as_ref() };
        let iovec = unsafe { retained.iovec.assume_init_ref() };
        let msg = unsafe { retained.msghdr.assume_init_ref() };
        let expected_ptr = retained.buffer.bytes.as_ptr();
        assert_eq!(iovec.iov_base, expected_ptr as *mut _);
        assert_eq!(iovec.iov_len, 23);
        assert_retained_sctp_send_fields(
            msg,
            &retained.control,
            retained.iovec.as_ptr().cast_mut(),
            1,
            sndinfo,
        );

        let returned = unsafe { payload.take(&mut pool) }.buffer;
        assert_eq!(returned.bytes.as_ptr(), expected_ptr);
        drop(returned);
        assert_eq!(drops.get(), 1);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2, "outer plus reentrant allocation");
        assert_eq!(stats.pooled_frees, 2, "outer plus reentrant release");
    }

    #[test]
    fn retained_recv_vectored_constructor_omits_peer_address_and_uses_final_storage() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);

        let mut full = IoBuffMut::new(0, 4, 0).expect("full segment allocation failed");
        full.payload_append(b"full")
            .expect("full segment initialization failed");
        let first = IoBuffMut::new(0, 7, 0).expect("first writable segment allocation failed");
        let second = IoBuffMut::new(0, 11, 0).expect("second writable segment allocation failed");
        let mut chain = IoBuffVecMut::from_array([full, first, second]);
        let first_ptr = chain
            .get_mut(1)
            .expect("first writable segment missing")
            .as_mut_ptr();
        let second_ptr = chain
            .get_mut(2)
            .expect("second writable segment missing")
            .as_mut_ptr();
        let (iov_count, writable_len) = chain
            .checked_read_iovec_count_and_writable_len()
            .expect("vectored receive shape overflowed");
        assert_eq!((iov_count, writable_len), (2, 18));
        let mut buffer = Some(chain);

        let payload = unsafe {
            emplace_retained_sctp_recv_vectored_payload(
                pool_ptr,
                &mut buffer,
                (iov_count, writable_len),
            )
        }
        .expect("vectored receive emplacement failed");
        assert!(buffer.is_none(), "constructor did not transfer the chain");

        let retained = unsafe { payload.as_ref() };
        let first_iovec = unsafe { retained.iovecs[0].assume_init_ref() };
        let second_iovec = unsafe { retained.iovecs[1].assume_init_ref() };
        let msg = unsafe { retained.msghdr.assume_init_ref() };
        assert_eq!(first_iovec.iov_base, first_ptr.cast());
        assert_eq!(first_iovec.iov_len, 7);
        assert_eq!(second_iovec.iov_base, second_ptr.cast());
        assert_eq!(second_iovec.iov_len, 11);
        assert_eq!(
            msg.msg_iov,
            retained.iovecs.as_ptr().cast_mut().cast::<libc::iovec>()
        );
        assert_eq!(msg.msg_iovlen, iov_count);
        assert!(
            msg.msg_name.is_null(),
            "connected SCTP vectored receive unexpectedly requested a peer address"
        );
        assert_eq!(msg.msg_namelen, 0);
        assert_eq!(msg.msg_control, retained.control.as_ptr().cast_mut().cast());
        assert_eq!(msg.msg_controllen, retained.control.len());

        let mut returned = unsafe { payload.take(&mut pool) }.buffer;
        assert_eq!(
            returned
                .get_mut(1)
                .expect("returned first writable segment missing")
                .as_mut_ptr(),
            first_ptr
        );
        assert_eq!(
            returned
                .get_mut(2)
                .expect("returned second writable segment missing")
                .as_mut_ptr(),
            second_ptr
        );
        drop(returned);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 1);
        assert_eq!(stats.pooled_frees, 1);
    }

    #[test]
    fn sctp_vectored_completion_extraction_is_compact_and_preserves_n16_owners() {
        const N: usize = 16;

        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let segments: [IoBuffMut; N] = std::array::from_fn(|_| {
            IoBuffMut::new(0, 8, 0).expect("vectored receive segment allocation failed")
        });
        let mut chain = IoBuffVecMut::from_array(segments);
        let segment_ptrs: [*mut u8; N] = std::array::from_fn(|index| {
            chain
                .get_mut(index)
                .expect("vectored receive segment missing")
                .as_mut_ptr()
        });
        let (iov_count, writable_len) = chain
            .checked_read_iovec_count_and_writable_len()
            .expect("vectored receive shape overflowed");
        assert_eq!((iov_count, writable_len), (N, N * 8));
        let mut buffer = Some(chain);

        let mut payload = unsafe {
            emplace_retained_sctp_recv_vectored_payload(
                pool_ptr,
                &mut buffer,
                (iov_count, writable_len),
            )
        }
        .expect("vectored receive emplacement failed");
        let retained_addr = unsafe { payload.as_mut() as *mut _ as usize };
        const TAIL_POISON: u8 = 0x5a;
        let mut expected_control = Vec::new();
        append_test_rcvinfo(&mut expected_control);
        unsafe {
            let retained = payload.as_mut();
            let first_iovec = retained.iovecs[0].assume_init_ref();
            std::ptr::copy_nonoverlapping(b"note".as_ptr(), first_iovec.iov_base.cast::<u8>(), 4);
            std::ptr::write_bytes(
                retained.control.as_mut_ptr().cast::<u8>(),
                TAIL_POISON,
                retained.control.len(),
            );
            std::ptr::copy_nonoverlapping(
                expected_control.as_ptr(),
                retained.control.as_mut_ptr().cast::<u8>(),
                expected_control.len(),
            );
            let msg = retained.msghdr.assume_init_mut();
            msg.msg_controllen = expected_control.len();
            msg.msg_flags = libc::MSG_EOR | libc::MSG_CTRUNC;
        }

        let mut completion = unsafe {
            payload.take_with(&mut pool, |payload| {
                take_sctp_recv_vectored_completion(payload, iov_count, Some(4))
            })
        };
        assert!(
            std::mem::size_of::<SctpRecvVectoredCompletion<N>>()
                < std::mem::size_of::<RetainedSctpRecvVectoredPayload<N>>(),
            "N=16 completion projection did not shrink the retained aggregate"
        );
        let first_iovec = completion
            .first_iovec
            .as_ref()
            .expect("active first iovec was not extracted");
        assert_eq!(first_iovec.iov_base, segment_ptrs[0].cast());
        assert_eq!(first_iovec.iov_len, 8);
        assert_eq!(
            first_iov_view_from_copied_descriptor(&completion.buffer, *first_iovec, 4),
            b"note"
        );
        assert_eq!(
            completion
                .meta
                .rcvinfo
                .as_ref()
                .expect("valid vectored RCVINFO should parse before extraction"),
            &Some(expected_test_rcvinfo(true))
        );
        assert_eq!(
            completion.meta.header.msg_flags,
            libc::MSG_EOR | libc::MSG_CTRUNC
        );
        for (index, expected_ptr) in segment_ptrs.into_iter().enumerate() {
            assert_eq!(
                completion
                    .buffer
                    .get_mut(index)
                    .expect("returned vectored segment missing")
                    .as_mut_ptr(),
                expected_ptr,
                "vectored segment owner moved to different backing"
            );
        }
        assert_eq!(pool.stats().pooled_frees, 1);

        let retry_segments: [IoBuffMut; N] = std::array::from_fn(|_| {
            IoBuffMut::new(0, 8, 0).expect("retry receive segment allocation failed")
        });
        let retry_chain = IoBuffVecMut::from_array(retry_segments);
        let retry_shape = retry_chain
            .checked_read_iovec_count_and_writable_len()
            .expect("retry vectored receive shape overflowed");
        let mut retry_buffer = Some(retry_chain);
        let pool_ptr = NonNull::from(&mut pool);
        let mut retry = unsafe {
            emplace_retained_sctp_recv_vectored_payload(pool_ptr, &mut retry_buffer, retry_shape)
        }
        .expect("retry vectored receive emplacement failed");
        assert_eq!(
            unsafe { retry.as_mut() as *mut _ as usize },
            retained_addr,
            "N=16 selective extraction did not recycle retained backing"
        );
        let reused_control = unsafe {
            std::slice::from_raw_parts(
                retry.as_ref().control.as_ptr().cast::<u8>(),
                SCTP_RECV_CONTROL_LEN,
            )
        };
        assert_eq!(
            &reused_control[..expected_control.len()],
            expected_control,
            "vectored receive constructor rewrote the prior control prefix"
        );
        assert!(
            reused_control[expected_control.len()..]
                .iter()
                .all(|byte| *byte == TAIL_POISON),
            "vectored receive constructor cleared the unreported control tail"
        );
        drop(unsafe { retry.take(&mut pool) });

        let uninitialized: [MaybeUninit<libc::iovec>; N] =
            std::array::from_fn(|_| MaybeUninit::uninit());
        assert!(
            unsafe { copy_sctp_first_iovec(&uninitialized, 0) }
                .as_ref()
                .is_none()
        );
        drop(completion);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2);
        assert_eq!(stats.pooled_reuses, 1);
        assert_eq!(stats.pooled_frees, 2);
    }

    #[test]
    fn discarding_vectored_completion_does_not_read_uninitialized_control() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let segments: [IoBuffMut; 2] = std::array::from_fn(|_| {
            IoBuffMut::new(0, 8, 0).expect("vectored receive segment allocation failed")
        });
        let mut chain = IoBuffVecMut::from_array(segments);
        let segment_ptrs: [*mut u8; 2] = std::array::from_fn(|index| {
            chain
                .get_mut(index)
                .expect("vectored receive segment missing")
                .as_mut_ptr()
        });
        let (iov_count, writable_len) = chain
            .checked_read_iovec_count_and_writable_len()
            .expect("vectored receive shape overflowed");
        assert_eq!((iov_count, writable_len), (2, 16));
        let mut buffer = Some(chain);

        let mut payload = unsafe {
            emplace_retained_sctp_recv_vectored_payload(
                pool_ptr,
                &mut buffer,
                (iov_count, writable_len),
            )
        }
        .expect("vectored receive emplacement failed");
        let retained_addr = unsafe { payload.as_mut() as *mut _ as usize };
        unsafe {
            let retained = payload.as_mut();
            assert_eq!(
                retained.msghdr.assume_init_ref().msg_controllen,
                SCTP_RECV_CONTROL_LEN,
                "fixture must retain the submitted control capacity"
            );
            let first_iovec = retained.iovecs[0].assume_init_ref();
            std::ptr::copy_nonoverlapping(b"tail".as_ptr(), first_iovec.iov_base.cast::<u8>(), 4);
            retained.msghdr.assume_init_mut().msg_flags = libc::MSG_EOR;
            // Do not initialize any control byte. This is the exact metadata
            // inspection decision used while record-tail recovery is
            // consuming a successful completion.
        }
        let inspect_metadata = false;
        let actual = Some(4);
        let mut completion = unsafe {
            payload.take_with(&mut pool, |payload| {
                take_sctp_recv_vectored_completion(
                    payload,
                    iov_count,
                    actual.filter(|_| inspect_metadata),
                )
            })
        };

        assert_eq!(
            completion.meta.rcvinfo.as_ref().expect("skip failed"),
            &None
        );
        assert_eq!(completion.meta.header.msg_controllen, SCTP_RECV_CONTROL_LEN);
        assert_eq!(completion.meta.header.msg_flags, libc::MSG_EOR);
        let first_iovec = completion
            .first_iovec
            .as_ref()
            .expect("active first iovec was not extracted");
        assert_eq!(first_iovec.iov_base, segment_ptrs[0].cast());
        assert_eq!(
            first_iov_view_from_copied_descriptor(&completion.buffer, *first_iovec, 4),
            b"tail"
        );
        for (index, expected_ptr) in segment_ptrs.into_iter().enumerate() {
            assert_eq!(
                completion
                    .buffer
                    .get_mut(index)
                    .expect("returned vectored segment missing")
                    .as_mut_ptr(),
                expected_ptr
            );
        }
        assert_eq!(pool.stats().pooled_frees, 1);

        let retry_segments: [IoBuffMut; 2] = std::array::from_fn(|_| {
            IoBuffMut::new(0, 8, 0).expect("retry receive segment allocation failed")
        });
        let retry_chain = IoBuffVecMut::from_array(retry_segments);
        let retry_shape = retry_chain
            .checked_read_iovec_count_and_writable_len()
            .expect("retry vectored receive shape overflowed");
        let mut retry_buffer = Some(retry_chain);
        let pool_ptr = NonNull::from(&mut pool);
        let mut retry = unsafe {
            emplace_retained_sctp_recv_vectored_payload(pool_ptr, &mut retry_buffer, retry_shape)
        }
        .expect("retry vectored receive emplacement failed");
        assert_eq!(
            unsafe { retry.as_mut() as *mut _ as usize },
            retained_addr,
            "discard extraction did not recycle retained backing"
        );
        drop(unsafe { retry.take(&mut pool) });
        drop(completion);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2);
        assert_eq!(stats.pooled_reuses, 1);
        assert_eq!(stats.pooled_frees, 2);
    }

    #[test]
    fn retained_send_vectored_constructor_compacts_final_iovecs_and_preserves_chain() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool creation failed");
        let pool_ptr = NonNull::from(&mut pool);

        let mut first = IoBuffMut::new(0, 3, 0).expect("first segment allocation failed");
        first
            .payload_append(b"abc")
            .expect("first segment initialization failed");
        let empty = IoBuffMut::new(0, 5, 0).expect("empty segment allocation failed");
        let mut second = IoBuffMut::new(0, 5, 0).expect("second segment allocation failed");
        second
            .payload_append(b"defgh")
            .expect("second segment initialization failed");
        let chain = IoBuffVec::from_array([first.freeze(), empty.freeze(), second.freeze()]);
        let first_ptr = chain.get(0).expect("first segment missing").as_ptr();
        let second_ptr = chain.get(2).expect("second segment missing").as_ptr();
        let expected_shape = checked_iobuffvec_write_iovec_count_and_len(&chain)
            .expect("SCTP vectored send shape overflowed");
        assert_eq!(expected_shape, (2, 8));
        let mut buffer = Some(chain);
        let info = SctpSendInfo {
            stream_id: 11,
            flags: 5,
            ppid: 0xa1b2_c3d4,
            context: 0x5566_7788,
            assoc_id: 17,
        };
        let sndinfo = raw_sndinfo_from_public(info);

        let payload = unsafe {
            emplace_retained_sctp_send_vectored_payload(
                pool_ptr,
                &mut buffer,
                expected_shape,
                sndinfo,
            )
        }
        .expect("SCTP vectored send emplacement failed");
        assert!(buffer.is_none(), "constructor did not transfer the chain");

        let retained = unsafe { payload.as_ref() };
        let first_iovec = unsafe { retained.iovecs[0].assume_init_ref() };
        let second_iovec = unsafe { retained.iovecs[1].assume_init_ref() };
        let msg = unsafe { retained.msghdr.assume_init_ref() };
        assert_eq!(first_iovec.iov_base, first_ptr.cast_mut().cast());
        assert_eq!(first_iovec.iov_len, 3);
        assert_eq!(second_iovec.iov_base, second_ptr.cast_mut().cast());
        assert_eq!(second_iovec.iov_len, 5);
        assert_retained_sctp_send_fields(
            msg,
            &retained.control,
            retained.iovecs.as_ptr().cast_mut().cast::<libc::iovec>(),
            2,
            sndinfo,
        );

        let returned = unsafe { payload.take(&mut pool) }.buffer;
        assert_eq!(
            returned
                .get(0)
                .expect("returned first segment missing")
                .as_ptr(),
            first_ptr
        );
        assert_eq!(
            returned
                .get(2)
                .expect("returned second segment missing")
                .as_ptr(),
            second_ptr
        );
        drop(returned);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 1);
        assert_eq!(stats.pooled_frees, 1);
    }

    #[test]
    fn sctp_data_send_entry_uses_send_with_nosignal() {
        let bytes = [1u8, 2, 3, 4];
        let entry = build_sctp_send_entry(7, bytes.as_ptr(), bytes.len() as u32, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::Send::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.user_data, 99);
    }

    #[test]
    fn sctp_metadata_send_entry_uses_sendmsg_with_nosignal() {
        let msg = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        let entry = build_sctp_sendmsg_entry(7, &msg, 99);
        let sqe = sqe_prefix(&entry);

        assert_eq!(sqe.opcode, opcode::SendMsg::CODE);
        assert_eq!(sqe.msg_flags, libc::MSG_NOSIGNAL as u32);
        assert_eq!(sqe.addr, (&msg as *const libc::msghdr) as u64);
        assert_eq!(sqe.user_data, 99);
    }

    fn expected_sctp_reset_encoding(
        assoc_id: libc::sctp_assoc_t,
        flags: u16,
        streams: &[u16],
    ) -> Vec<u8> {
        assert_eq!(
            std::mem::size_of::<SctpResetStreamsHeader>(),
            std::mem::size_of::<libc::sctp_assoc_t>() + 2 * std::mem::size_of::<u16>()
        );

        let mut expected = Vec::with_capacity(
            std::mem::size_of::<SctpResetStreamsHeader>() + std::mem::size_of_val(streams),
        );
        expected.extend_from_slice(&assoc_id.to_ne_bytes());
        expected.extend_from_slice(&flags.to_ne_bytes());
        expected.extend_from_slice(&(streams.len() as u16).to_ne_bytes());
        for stream in streams {
            expected.extend_from_slice(&stream.to_ne_bytes());
        }
        expected
    }

    #[test]
    fn sctp_reset_listed_constructors_preserve_nonempty_wire_encoding() {
        let cases = [
            (
                SctpResetStreams::incoming(&[11]),
                SCTP_STREAM_RESET_INCOMING,
            ),
            (
                SctpResetStreams::outgoing(&[12, 13]),
                SCTP_STREAM_RESET_OUTGOING,
            ),
            (
                SctpResetStreams::bidirectional(&[14]),
                SCTP_STREAM_RESET_INCOMING | SCTP_STREAM_RESET_OUTGOING,
            ),
        ];

        for (request, expected_flags) in cases {
            assert_eq!(request.intent, SctpResetIntent::Listed);
            assert_eq!(request.flags, expected_flags);
            let encoded =
                encode_sctp_reset_streams(&request).expect("nonempty listed reset should encode");
            assert_eq!(
                encoded,
                expected_sctp_reset_encoding(0, expected_flags, &request.streams)
            );
        }
    }

    #[test]
    fn sctp_reset_nonempty_encoding_preserves_assoc_duplicates_and_boundary_ids() {
        let stream_ids = [0, u16::MAX, 7, 7];
        let mut request = SctpResetStreams::bidirectional(&stream_ids);
        request.assoc_id = 0x0102_0304;

        let encoded = encode_sctp_reset_streams(&request)
            .expect("duplicate and boundary stream IDs should encode");
        assert_eq!(
            encoded,
            expected_sctp_reset_encoding(request.assoc_id, request.flags, &stream_ids)
        );
    }

    #[test]
    fn sctp_reset_all_constructors_encode_explicit_zero_count_sentinel() {
        let cases = [
            (SctpResetStreams::all_incoming(), SCTP_STREAM_RESET_INCOMING),
            (SctpResetStreams::all_outgoing(), SCTP_STREAM_RESET_OUTGOING),
            (
                SctpResetStreams::all_bidirectional(),
                SCTP_STREAM_RESET_INCOMING | SCTP_STREAM_RESET_OUTGOING,
            ),
        ];

        for (mut request, expected_flags) in cases {
            request.assoc_id = 29;
            assert_eq!(request.intent, SctpResetIntent::All);
            assert!(request.streams.is_empty());
            assert_eq!(request.flags, expected_flags);
            let encoded = encode_sctp_reset_streams(&request)
                .expect("explicit all-stream reset should encode");
            assert_eq!(
                encoded,
                expected_sctp_reset_encoding(request.assoc_id, expected_flags, &[])
            );
        }
    }

    #[test]
    fn sctp_reset_generic_empty_requests_are_invalid() {
        let requests = [
            SctpResetStreams::incoming(&[]),
            SctpResetStreams::outgoing(&[]),
            SctpResetStreams::bidirectional(&[]),
        ];

        for request in requests {
            let err = encode_sctp_reset_streams(&request)
                .expect_err("generic empty stream list should be rejected");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn sctp_reset_rejects_intent_and_list_shape_mismatches() {
        let mut listed = SctpResetStreams::outgoing(&[3]);
        listed.streams.clear();
        let err = encode_sctp_reset_streams(&listed)
            .expect_err("cleared listed request should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let mut all = SctpResetStreams::all_outgoing();
        all.streams.push(3);
        let err = encode_sctp_reset_streams(&all)
            .expect_err("all-stream request with an ID should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn sctp_reset_rejects_stream_count_above_kernel_field_width() {
        let request = SctpResetStreams::incoming(&vec![0; u16::MAX as usize + 1]);
        let err = encode_sctp_reset_streams(&request)
            .expect_err("stream count above u16::MAX should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn sctp_reset_rejects_invalid_shape_before_socket_access() {
        let raw = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("distinctive fd creation failed");
        // SAFETY: the test helper returned one descriptor owned only here.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let stream =
            SctpStream::from_owned_fd(owned, SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)));

        let err = stream
            .reset_streams(&SctpResetStreams::incoming(&[]))
            .expect_err("invalid shape should fail before setsockopt");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        drop(stream);
        assert!(crate::runtime::fd::raw_fd_is_closed(raw));
    }

    fn write_u16_ne(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_ne_bytes());
    }

    fn write_u16_be(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
    }

    fn write_u32_ne(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
    }

    fn write_i32_ne(bytes: &mut [u8], offset: usize, value: i32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
    }

    fn test_msghdr_with_flags(msg_flags: libc::c_int) -> libc::msghdr {
        libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags,
        }
    }

    fn test_recv_header(msg: &libc::msghdr) -> SctpRecvHeader {
        SctpRecvHeader::from_msghdr(msg)
    }

    fn test_notification_buffer(notification_type: libc::c_int, len: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        write_u16_ne(
            &mut bytes,
            SCTP_NOTIFICATION_TYPE_OFFSET,
            notification_type as u16,
        );
        write_u16_ne(&mut bytes, SCTP_NOTIFICATION_FLAGS_OFFSET, 0);
        write_u32_ne(&mut bytes, SCTP_NOTIFICATION_LENGTH_OFFSET, len as u32);
        bytes
    }

    fn test_partial_delivery_notification(indication: u32) -> Vec<u8> {
        let mut bytes = test_notification_buffer(
            LOCAL_SCTP_PARTIAL_DELIVERY_EVENT,
            SCTP_PARTIAL_DELIVERY_MIN_LEN,
        );
        write_u32_ne(
            &mut bytes,
            SCTP_PARTIAL_DELIVERY_INDICATION_OFFSET,
            indication,
        );
        bytes
    }

    fn test_fragmented_stream_reset_notification() -> Vec<u8> {
        const STREAMS: usize = 16;
        let mut bytes = test_notification_buffer(
            LOCAL_SCTP_STREAM_RESET_EVENT,
            SCTP_STREAM_RESET_MIN_LEN + STREAMS * std::mem::size_of::<u16>(),
        );
        write_u16_ne(&mut bytes, SCTP_NOTIFICATION_FLAGS_OFFSET, 1);
        write_i32_ne(&mut bytes, SCTP_STREAM_RESET_ASSOC_ID_OFFSET, 42);
        for stream in 0..STREAMS {
            write_u16_ne(
                &mut bytes,
                SCTP_STREAM_RESET_MIN_LEN + stream * std::mem::size_of::<u16>(),
                stream as u16,
            );
        }
        bytes
    }

    fn assert_short_sctp_notification(
        error: &io::Error,
        kind: u16,
        declared_length: u32,
        required_length: usize,
        context: &str,
    ) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{context}");
        assert_eq!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<ShortSctpNotification>()),
            Some(&ShortSctpNotification {
                kind,
                declared_length,
                required_length,
            }),
            "{context} must retain structured length diagnostics"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "SCTP notification kind {kind:#06x} declared length {declared_length} is shorter than required length {required_length}"
            ),
            "{context} lazy display"
        );
    }

    fn notification_layout_cases() -> Vec<(&'static str, Vec<u8>, SctpRecvMeta)> {
        let mut cases = Vec::new();

        let mut assoc =
            test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, SCTP_ASSOC_CHANGE_MIN_LEN);
        write_u16_ne(&mut assoc, SCTP_ASSOC_CHANGE_STATE_OFFSET, 0x0102);
        write_u16_ne(&mut assoc, SCTP_ASSOC_CHANGE_ERROR_OFFSET, 0x0304);
        write_u16_ne(
            &mut assoc,
            SCTP_ASSOC_CHANGE_OUTBOUND_STREAMS_OFFSET,
            0x0506,
        );
        write_u16_ne(&mut assoc, SCTP_ASSOC_CHANGE_INBOUND_STREAMS_OFFSET, 0x0708);
        write_i32_ne(&mut assoc, SCTP_ASSOC_CHANGE_ASSOC_ID_OFFSET, -9);
        cases.push((
            "association change",
            assoc,
            SctpRecvMeta::Notification(SctpNotification::AssocChange {
                state: 0x0102,
                error: 0x0304,
                outbound_streams: 0x0506,
                inbound_streams: 0x0708,
                assoc_id: -9,
            }),
        ));

        let peer_addr = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 25), 3868));
        let mut peer =
            test_notification_buffer(LOCAL_SCTP_PEER_ADDR_CHANGE, SCTP_PEER_ADDR_CHANGE_MIN_LEN);
        let sockaddr_base = SCTP_PEER_ADDR_CHANGE_ADDRESS_OFFSET;
        write_u16_ne(
            &mut peer,
            sockaddr_base + std::mem::offset_of!(libc::sockaddr_in, sin_family),
            libc::AF_INET as u16,
        );
        write_u16_be(
            &mut peer,
            sockaddr_base + std::mem::offset_of!(libc::sockaddr_in, sin_port),
            peer_addr.port(),
        );
        let address_offset = sockaddr_base
            + std::mem::offset_of!(libc::sockaddr_in, sin_addr)
            + std::mem::offset_of!(libc::in_addr, s_addr);
        peer[address_offset..address_offset + 4]
            .copy_from_slice(&Ipv4Addr::new(192, 0, 2, 25).octets());
        write_i32_ne(&mut peer, SCTP_PEER_ADDR_CHANGE_STATE_OFFSET, -10);
        write_i32_ne(&mut peer, SCTP_PEER_ADDR_CHANGE_ERROR_OFFSET, -11);
        write_i32_ne(&mut peer, SCTP_PEER_ADDR_CHANGE_ASSOC_ID_OFFSET, -12);
        cases.push((
            "peer address change",
            peer,
            SctpRecvMeta::Notification(SctpNotification::PeerAddrChange {
                addr: peer_addr,
                state: -10,
                error: -11,
                assoc_id: -12,
            }),
        ));

        let mut legacy =
            test_notification_buffer(LOCAL_SCTP_SEND_FAILED, SCTP_LEGACY_SEND_FAILED_MIN_LEN);
        write_u16_ne(&mut legacy, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x1112);
        write_u32_ne(&mut legacy, SCTP_SEND_FAILED_ERROR_OFFSET, 0x1314_1516);
        let legacy_info = SCTP_SEND_FAILED_INFO_OFFSET;
        write_u16_ne(
            &mut legacy,
            legacy_info + std::mem::offset_of!(libc::sctp_sndrcvinfo, sinfo_stream),
            0x1718,
        );
        write_u16_ne(
            &mut legacy,
            legacy_info + std::mem::offset_of!(libc::sctp_sndrcvinfo, sinfo_flags),
            0x191a,
        );
        write_u32_ne(
            &mut legacy,
            legacy_info + std::mem::offset_of!(libc::sctp_sndrcvinfo, sinfo_ppid),
            0x1b1c_1d1e_u32.to_be(),
        );
        write_u32_ne(
            &mut legacy,
            legacy_info + std::mem::offset_of!(libc::sctp_sndrcvinfo, sinfo_context),
            0x1f20_2122,
        );
        write_i32_ne(
            &mut legacy,
            legacy_info + std::mem::offset_of!(libc::sctp_sndrcvinfo, sinfo_assoc_id),
            -23,
        );
        write_i32_ne(&mut legacy, SCTP_LEGACY_SEND_FAILED_ASSOC_ID_OFFSET, -24);
        cases.push((
            "legacy send failed",
            legacy,
            SctpRecvMeta::Notification(SctpNotification::SendFailed {
                flags: 0x1112,
                error: 0x1314_1516,
                info: SctpSendInfo {
                    stream_id: 0x1718,
                    flags: 0x191a,
                    ppid: 0x1b1c_1d1e,
                    context: 0x1f20_2122,
                    assoc_id: -23,
                },
                assoc_id: -24,
            }),
        ));

        let mut remote =
            test_notification_buffer(LOCAL_SCTP_REMOTE_ERROR, SCTP_REMOTE_ERROR_MIN_LEN);
        write_u16_be(&mut remote, SCTP_REMOTE_ERROR_ERROR_OFFSET, 0x2526);
        write_i32_ne(&mut remote, SCTP_REMOTE_ERROR_ASSOC_ID_OFFSET, -27);
        cases.push((
            "remote error",
            remote,
            SctpRecvMeta::Notification(SctpNotification::RemoteError {
                error: 0x2526,
                assoc_id: -27,
            }),
        ));

        let mut shutdown =
            test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, SCTP_SHUTDOWN_MIN_LEN);
        write_i32_ne(&mut shutdown, SCTP_SHUTDOWN_ASSOC_ID_OFFSET, -28);
        cases.push((
            "shutdown",
            shutdown,
            SctpRecvMeta::Notification(SctpNotification::Shutdown { assoc_id: -28 }),
        ));

        let mut adaptation =
            test_notification_buffer(LOCAL_SCTP_ADAPTATION_INDICATION, SCTP_ADAPTATION_MIN_LEN);
        write_u32_ne(
            &mut adaptation,
            SCTP_ADAPTATION_INDICATION_OFFSET,
            0x292a_2b2c,
        );
        write_i32_ne(&mut adaptation, SCTP_ADAPTATION_ASSOC_ID_OFFSET, -29);
        cases.push((
            "adaptation indication",
            adaptation,
            SctpRecvMeta::Notification(SctpNotification::Adaptation {
                indication: 0x292a_2b2c,
                assoc_id: -29,
            }),
        ));

        let mut authentication =
            test_notification_buffer(LOCAL_SCTP_AUTHENTICATION_EVENT, SCTP_AUTHENTICATION_MIN_LEN);
        write_u16_ne(&mut authentication, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x1234);
        write_u16_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_KEY_NUMBER_OFFSET,
            0x1122,
        );
        write_u16_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_ALTERNATE_KEY_NUMBER_OFFSET,
            0x3344,
        );
        write_u32_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_INDICATION_OFFSET,
            0x5566_7788,
        );
        write_i32_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_ASSOC_ID_OFFSET,
            0x1020_3040,
        );
        cases.push((
            "authentication",
            authentication,
            SctpRecvMeta::Notification(SctpNotification::Authentication {
                flags: 0x1234,
                key_number: 0x1122,
                alternate_key_number: 0x3344,
                indication: 0x5566_7788,
                assoc_id: 0x1020_3040,
            }),
        ));

        let mut partial = test_notification_buffer(
            LOCAL_SCTP_PARTIAL_DELIVERY_EVENT,
            SCTP_PARTIAL_DELIVERY_MIN_LEN,
        );
        write_u32_ne(
            &mut partial,
            SCTP_PARTIAL_DELIVERY_INDICATION_OFFSET,
            0x3031_3233,
        );
        write_i32_ne(&mut partial, SCTP_PARTIAL_DELIVERY_ASSOC_ID_OFFSET, -34);
        write_u32_ne(
            &mut partial,
            SCTP_PARTIAL_DELIVERY_STREAM_OFFSET,
            0x3536_3738,
        );
        write_u32_ne(
            &mut partial,
            SCTP_PARTIAL_DELIVERY_SEQUENCE_OFFSET,
            0x393a_3b3c,
        );
        cases.push((
            "partial delivery",
            partial,
            SctpRecvMeta::Notification(SctpNotification::PartialDelivery {
                indication: 0x3031_3233,
                assoc_id: -34,
                stream: 0x3536_3738,
                sequence: 0x393a_3b3c,
            }),
        ));

        let mut sender_dry =
            test_notification_buffer(LOCAL_SCTP_SENDER_DRY_EVENT, SCTP_SENDER_DRY_MIN_LEN);
        write_i32_ne(&mut sender_dry, SCTP_SENDER_DRY_ASSOC_ID_OFFSET, -61);
        cases.push((
            "sender dry",
            sender_dry,
            SctpRecvMeta::Notification(SctpNotification::SenderDry { assoc_id: -61 }),
        ));

        let mut stream_reset =
            test_notification_buffer(LOCAL_SCTP_STREAM_RESET_EVENT, SCTP_STREAM_RESET_MIN_LEN);
        write_u16_ne(&mut stream_reset, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x3e3f);
        write_i32_ne(&mut stream_reset, SCTP_STREAM_RESET_ASSOC_ID_OFFSET, -64);
        cases.push((
            "stream reset",
            stream_reset,
            SctpRecvMeta::Notification(SctpNotification::StreamReset {
                flags: 0x3e3f,
                assoc_id: -64,
            }),
        ));

        let mut assoc_reset =
            test_notification_buffer(LOCAL_SCTP_ASSOC_RESET_EVENT, SCTP_ASSOC_RESET_MIN_LEN);
        write_u16_ne(&mut assoc_reset, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x4142);
        write_i32_ne(&mut assoc_reset, SCTP_ASSOC_RESET_ASSOC_ID_OFFSET, -67);
        write_u32_ne(
            &mut assoc_reset,
            SCTP_ASSOC_RESET_LOCAL_TSN_OFFSET,
            0x4445_4647,
        );
        write_u32_ne(
            &mut assoc_reset,
            SCTP_ASSOC_RESET_REMOTE_TSN_OFFSET,
            0x4849_4a4b,
        );
        cases.push((
            "association reset",
            assoc_reset,
            SctpRecvMeta::Notification(SctpNotification::AssocReset {
                flags: 0x4142,
                assoc_id: -67,
                local_tsn: 0x4445_4647,
                remote_tsn: 0x4849_4a4b,
            }),
        ));

        let mut stream_change =
            test_notification_buffer(LOCAL_SCTP_STREAM_CHANGE_EVENT, SCTP_STREAM_CHANGE_MIN_LEN);
        write_u16_ne(&mut stream_change, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x4c4d);
        write_i32_ne(&mut stream_change, SCTP_STREAM_CHANGE_ASSOC_ID_OFFSET, -78);
        write_u16_ne(
            &mut stream_change,
            SCTP_STREAM_CHANGE_INBOUND_STREAMS_OFFSET,
            0x4f50,
        );
        write_u16_ne(
            &mut stream_change,
            SCTP_STREAM_CHANGE_OUTBOUND_STREAMS_OFFSET,
            0x5152,
        );
        cases.push((
            "stream change",
            stream_change,
            SctpRecvMeta::Notification(SctpNotification::StreamChange {
                flags: 0x4c4d,
                assoc_id: -78,
                inbound_streams: 0x4f50,
                outbound_streams: 0x5152,
            }),
        ));

        let mut modern =
            test_notification_buffer(LOCAL_SCTP_SEND_FAILED_EVENT, SCTP_SEND_FAILED_EVENT_MIN_LEN);
        write_u16_ne(&mut modern, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x5152);
        write_u32_ne(&mut modern, SCTP_SEND_FAILED_ERROR_OFFSET, 0x5354_5556);
        let modern_info = SCTP_SEND_FAILED_INFO_OFFSET;
        write_u16_ne(
            &mut modern,
            modern_info + std::mem::offset_of!(libc::sctp_sndinfo, snd_sid),
            0x5758,
        );
        write_u16_ne(
            &mut modern,
            modern_info + std::mem::offset_of!(libc::sctp_sndinfo, snd_flags),
            0x595a,
        );
        write_u32_ne(
            &mut modern,
            modern_info + std::mem::offset_of!(libc::sctp_sndinfo, snd_ppid),
            0x5b5c_5d5e_u32.to_be(),
        );
        write_u32_ne(
            &mut modern,
            modern_info + std::mem::offset_of!(libc::sctp_sndinfo, snd_context),
            0x5f60_6162,
        );
        write_i32_ne(
            &mut modern,
            modern_info + std::mem::offset_of!(libc::sctp_sndinfo, snd_assoc_id),
            -99,
        );
        write_i32_ne(&mut modern, SCTP_SEND_FAILED_EVENT_ASSOC_ID_OFFSET, -100);
        cases.push((
            "modern send failed",
            modern,
            SctpRecvMeta::Notification(SctpNotification::SendFailed {
                flags: 0x5152,
                error: 0x5354_5556,
                info: SctpSendInfo {
                    stream_id: 0x5758,
                    flags: 0x595a,
                    ppid: 0x5b5c_5d5e,
                    context: 0x5f60_6162,
                    assoc_id: -99,
                },
                assoc_id: -100,
            }),
        ));

        cases
    }

    struct RejectedContextRecvBuffer {
        bytes: Box<[u8; 32]>,
        pointer_calls: Rc<Cell<usize>>,
        published_len: Rc<Cell<usize>>,
        panic_on_pointer: bool,
    }

    impl RejectedContextRecvBuffer {
        fn new(pointer_calls: Rc<Cell<usize>>, published_len: Rc<Cell<usize>>) -> Self {
            Self {
                bytes: Box::new([0; 32]),
                pointer_calls,
                published_len,
                panic_on_pointer: false,
            }
        }

        fn backing_ptr(&self) -> *const u8 {
            self.bytes.as_ptr()
        }
    }

    unsafe impl IoBuffReadOnly for RejectedContextRecvBuffer {
        fn as_ptr(&self) -> *const u8 {
            self.bytes.as_ptr()
        }

        fn len(&self) -> usize {
            self.published_len.get()
        }
    }

    unsafe impl IoBuffReadWrite for RejectedContextRecvBuffer {
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.pointer_calls.set(self.pointer_calls.get() + 1);
            if self.panic_on_pointer {
                panic!("rejected zero-length completion inspected its buffer");
            }
            unsafe { self.bytes.as_mut_ptr().add(self.published_len.get()) }
        }

        fn writable_len(&self) -> usize {
            self.bytes.len() - self.published_len.get()
        }

        fn write_base_len(&self) -> usize {
            self.published_len.get()
        }

        unsafe fn set_written_len(&mut self, len: usize) {
            assert!(len <= self.bytes.len());
            self.published_len.set(len);
        }
    }

    #[derive(Clone, Copy)]
    struct SyntheticRecvCompletion<'a> {
        result: i32,
        data: &'a [u8],
        msg_flags: libc::c_int,
    }

    impl<'a> SyntheticRecvCompletion<'a> {
        fn success(data: &'a [u8], msg_flags: libc::c_int) -> Self {
            Self {
                result: data.len() as i32,
                data,
                msg_flags,
            }
        }

        fn error(result: i32, msg_flags: libc::c_int) -> Self {
            assert!(result < 0);
            Self {
                result,
                data: &[],
                msg_flags,
            }
        }
    }

    fn ringless_sctp_stream() -> SctpStream {
        SctpStream::from_runtime_fd_with_recv_state(
            RuntimeFd::from_fresh_raw_fd(-1),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
            SctpRecvState::external(),
        )
    }

    fn stage_dropped_scalar_receive(
        stream: &mut SctpStream,
        reactor: *mut Reactor,
        completion: SyntheticRecvCompletion<'_>,
        pointer_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
        complete_before_drop: bool,
    ) -> *mut CompletionState {
        let state_ptr = unsafe { (&mut *reactor).alloc_op() };
        assert!(
            !state_ptr.is_null(),
            "dropped rich receive state allocation failed"
        );

        let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
        let mut buffer = Some(retained_constructor_buffer(
            None,
            Rc::clone(pointer_calls),
            Rc::clone(drops),
            false,
        ));
        let mut payload =
            unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut buffer, 32) };
        assert!(
            buffer.is_none(),
            "dropped rich receive did not retain its owner"
        );
        unsafe {
            let retained = payload.as_mut();
            if completion.result >= 0 {
                let iovec = retained.iovec.assume_init_ref();
                assert!(completion.data.len() <= iovec.iov_len);
                std::ptr::copy_nonoverlapping(
                    completion.data.as_ptr(),
                    iovec.iov_base.cast::<u8>(),
                    completion.data.len(),
                );
            }
            let msg = retained.msghdr.assume_init_mut();
            msg.msg_controllen = 0;
            msg.msg_flags = completion.msg_flags;
            (*state_ptr).attach_retained_payload(payload);
            (*state_ptr).result = completion.result;
            if complete_before_drop {
                (*state_ptr).set_completed();
            }
        }

        let mut fd_state = stream.fd.op_state();
        let fd = fd_state.raw_fd();
        unsafe {
            (*state_ptr).attach_fd_lease(fd_state.take_initial_lease());
            fd_state.publish_submitted_state(state_ptr);
        }
        let dropped: RecvFuture<'_, RetainedConstructorBuffer> = RecvFuture {
            fd,
            state_ptr: fd_state,
            buffer: None,
            write_base_len: 0,
            len: 32,
            input_error: None,
            recv_state: &mut stream.recv_state,
            _marker: PhantomData,
        };
        drop(dropped);
        assert_eq!(stream.recv_state.stashed_state, StashedSctpRecvState::Live);
        assert_eq!(stream.recv_state.stashed.state_ptr, state_ptr);
        assert_eq!(stream.recv_state.stashed.iov_count, 0);
        assert!(stream.recv_state.stashed.process_completed.is_some());
        state_ptr
    }

    fn assert_lean_stash_rejection(
        stream: &mut SctpStream,
        cx: &mut Context<'_>,
        expected_state: *mut CompletionState,
        expected_stash_state: StashedSctpRecvState,
    ) {
        let pointer_calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let buffer =
            retained_constructor_buffer(None, Rc::clone(&pointer_calls), Rc::clone(&drops), false);
        let backing = buffer.bytes.as_ptr();
        let mut future = stream.recv(buffer, 16);
        let Poll::Ready((result, returned)) = Pin::new(&mut future).poll(cx) else {
            panic!("lean receive did not reject the dropped rich receive");
        };
        let err = result.expect_err("lean receive bypassed the rich receive lineage");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.raw_os_error(), None);
        assert_eq!(returned.bytes.as_ptr(), backing);
        assert_eq!(
            pointer_calls.get(),
            0,
            "lean rejection exposed its buffer pointer"
        );
        assert_eq!(drops.get(), 0, "lean rejection dropped the returned owner");
        assert!(future.state_ptr.is_null());
        assert!(
            Pin::new(&mut future).poll(cx).is_pending(),
            "completed lean rejection did not fuse"
        );
        drop(future);
        assert_eq!(stream.recv_state.stashed_state, expected_stash_state);
        assert_eq!(stream.recv_state.stashed.state_ptr, expected_state);
        assert_eq!(
            stream.recv_state.stashed.process_completed.is_some(),
            expected_stash_state == StashedSctpRecvState::Live
        );
        drop(returned);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn lean_receive_rejects_active_record_discard_without_a_stash() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let mut stream = ringless_sctp_stream();
            stream.recv_state.record_sync = SctpRecordSync::DataTail;
            assert!(stream.recv_state.stashed.state_ptr.is_null());

            test_hooks::fail_next_op_alloc();
            test_hooks::fail_next_raw_sqe_submit();
            for attempt in 1..=2 {
                let pointer_calls = Rc::new(Cell::new(0));
                let drops = Rc::new(Cell::new(0));
                let buffer = retained_constructor_buffer(
                    None,
                    Rc::clone(&pointer_calls),
                    Rc::clone(&drops),
                    false,
                );
                let backing = buffer.bytes.as_ptr();
                let mut future = stream.recv(buffer, 16);
                let Poll::Ready((result, returned)) = Pin::new(&mut future).poll(cx) else {
                    panic!("active-discard lean receive attempt {attempt} remained pending");
                };
                let err = result.expect_err("lean receive bypassed active rich-record recovery");
                assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(err.raw_os_error(), None);
                assert_eq!(returned.bytes.as_ptr(), backing);
                assert_eq!(pointer_calls.get(), 0, "lean rejection exposed its buffer");
                assert_eq!(drops.get(), 0, "lean rejection dropped the exact owner");
                assert!(future.state_ptr.is_null());
                assert!(Pin::new(&mut future).poll(cx).is_pending());
                drop(future);
                assert_eq!(stream.recv_state.record_sync, SctpRecordSync::DataTail);
                assert!(stream.recv_state.stashed.state_ptr.is_null());
                assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
                assert_eq!(owner.inflight_op_count_for_test(), 0);
                drop(returned);
                assert_eq!(drops.get(), 1);
            }
            assert!(
                test_hooks::take_op_alloc_failure(),
                "active-discard rejection attempted operation allocation"
            );
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 1);
            let injected = test_hooks::take_raw_sqe_submit_failure()
                .expect("active-discard rejection consumed the SQE sentinel");
            assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 0);
            assert_eq!(stats.heap_fallbacks, 0);

            let zero_pointer_calls = Rc::new(Cell::new(0));
            let zero_drops = Rc::new(Cell::new(0));
            let zero_buffer = retained_constructor_buffer(
                None,
                Rc::clone(&zero_pointer_calls),
                Rc::clone(&zero_drops),
                false,
            );
            let mut zero = stream.recv(zero_buffer, 0);
            let Poll::Ready((zero_result, returned_zero)) = Pin::new(&mut zero).poll(cx) else {
                panic!("zero-length receive behind active discard remained pending");
            };
            assert_eq!(
                zero_result
                    .expect_err("zero-length receive unexpectedly succeeded")
                    .to_string(),
                ZERO_LENGTH_SCTP_RECV
            );
            assert_eq!(zero_pointer_calls.get(), 0);
            drop(zero);
            drop(returned_zero);
            assert_eq!(zero_drops.get(), 1);
            assert_eq!(stream.recv_state.record_sync, SctpRecordSync::DataTail);

            let context_pointer_calls = Rc::new(Cell::new(0));
            let context_drops = Rc::new(Cell::new(0));
            let context_buffer = retained_constructor_buffer(
                None,
                Rc::clone(&context_pointer_calls),
                Rc::clone(&context_drops),
                false,
            );
            let mut context_rejected = stream.recv(context_buffer, 16);
            let mut rejected_cx = Context::from_waker(std::task::Waker::noop());
            let Poll::Ready((context_result, returned_context)) =
                Pin::new(&mut context_rejected).poll(&mut rejected_cx)
            else {
                panic!("invalid-context receive behind active discard remained pending");
            };
            assert_eq!(
                context_result
                    .expect_err("invalid-context receive unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::NotConnected
            );
            assert_eq!(context_pointer_calls.get(), 0);
            drop(context_rejected);
            drop(returned_context);
            assert_eq!(context_drops.get(), 1);
            assert_eq!(stream.recv_state.record_sync, SctpRecordSync::DataTail);

            let eor = test_msghdr_with_flags(libc::MSG_EOR);
            assert!(
                stream.recv_state.should_consume_for_test(b"tail", &eor),
                "rich recovery did not consume the pending record tail"
            );
            assert_eq!(stream.recv_state.record_sync, SctpRecordSync::Synced);

            let direct_pointer_calls = Rc::new(Cell::new(0));
            let direct_drops = Rc::new(Cell::new(0));
            let direct_buffer = retained_constructor_buffer(
                None,
                Rc::clone(&direct_pointer_calls),
                Rc::clone(&direct_drops),
                false,
            );
            test_hooks::fail_next_raw_sqe_submit();
            let mut direct = stream.recv(direct_buffer, 16);
            let Poll::Ready((direct_result, returned_direct)) = Pin::new(&mut direct).poll(cx)
            else {
                panic!("ordinary lean receive did not reach direct submission");
            };
            assert_eq!(
                direct_result
                    .expect_err("injected direct receive unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(direct_pointer_calls.get(), 1);
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 0);
            drop(direct);
            drop(returned_direct);
            assert_eq!(direct_drops.get(), 1);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    fn assert_dropped_rich_receive_blocks_lean_and_recovers(
        completion: SyntheticRecvCompletion<'_>,
        initial_sync: SctpRecordSync,
        expected_sync: SctpRecordSync,
    ) {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let rich_pointer_calls = Rc::new(Cell::new(0));
            let rich_drops = Rc::new(Cell::new(0));
            let mut stream = ringless_sctp_stream();
            stream.recv_state.record_sync = initial_sync;
            let state_ptr = stage_dropped_scalar_receive(
                &mut stream,
                reactor,
                completion,
                &rich_pointer_calls,
                &rich_drops,
                false,
            );
            assert_eq!(rich_pointer_calls.get(), 1);
            assert_eq!(rich_drops.get(), 0);
            assert!(!unsafe { (*state_ptr).is_completed() });
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 1);
            let retained_before = unsafe { (&*reactor).retained_payload_stats() };

            test_hooks::fail_next_raw_sqe_submit();
            test_hooks::fail_next_op_alloc();
            assert_lean_stash_rejection(&mut stream, cx, state_ptr, StashedSctpRecvState::Live);
            assert!(
                test_hooks::take_op_alloc_failure(),
                "pending-stash rejection attempted operation allocation"
            );
            assert!(!unsafe { (*state_ptr).is_completed() });
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 1);
            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() },
                retained_before
            );
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 1);

            unsafe { (*state_ptr).set_completed() };
            test_hooks::fail_next_op_alloc();
            assert_lean_stash_rejection(&mut stream, cx, state_ptr, StashedSctpRecvState::Live);
            assert!(
                test_hooks::take_op_alloc_failure(),
                "completed-stash rejection attempted operation allocation"
            );
            assert!(unsafe { (*state_ptr).is_completed() });
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 1);
            assert_eq!(
                unsafe { (&*reactor).retained_payload_stats() },
                retained_before
            );
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 1);

            let recovery_pointer_calls = Rc::new(Cell::new(0));
            let recovery_drops = Rc::new(Cell::new(0));
            let recovery_buffer = retained_constructor_buffer(
                None,
                Rc::clone(&recovery_pointer_calls),
                Rc::clone(&recovery_drops),
                false,
            );
            let recovery_backing = recovery_buffer.bytes.as_ptr();
            let mut recovery = stream.recv_msg(recovery_buffer, 16);
            let Poll::Ready((result, returned)) = Pin::new(&mut recovery).poll(cx) else {
                panic!("compatible rich receive did not recover the completed stash");
            };
            assert_eq!(
                result
                    .expect_err("injected rich receive submission unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(returned.bytes.as_ptr(), recovery_backing);
            assert_eq!(recovery_pointer_calls.get(), 1);
            assert_eq!(recovery_drops.get(), 0);
            assert!(recovery.state_ptr.is_null());
            drop(recovery);
            drop(returned);
            assert_eq!(recovery_drops.get(), 1);

            assert!(stream.recv_state.stashed.state_ptr.is_null());
            assert!(stream.recv_state.stashed.process_completed.is_none());
            assert_eq!(stream.recv_state.record_sync, expected_sync);
            assert_eq!(rich_pointer_calls.get(), 2);
            assert_eq!(rich_drops.get(), 1);
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 0);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);

            let replacement = unsafe { (&mut *reactor).alloc_op() };
            assert_eq!(
                replacement, state_ptr,
                "rich recovery did not recycle its state slot"
            );
            unsafe { (&mut *reactor).free_op(replacement) };
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 2);
            assert_eq!(stats.pooled_reuses, 1);
            assert_eq!(stats.pooled_frees, 2);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(stats.heap_frees, 0);
        });
    }

    #[test]
    fn dropped_rich_receive_blocks_lean_until_partial_eor_and_notification_recovery() {
        assert_dropped_rich_receive_blocks_lean_and_recovers(
            SyntheticRecvCompletion::success(b"tail", 0),
            SctpRecordSync::Synced,
            SctpRecordSync::DataTail,
        );
        assert_dropped_rich_receive_blocks_lean_and_recovers(
            SyntheticRecvCompletion::success(b"done", libc::MSG_EOR),
            SctpRecordSync::DataTail,
            SctpRecordSync::Synced,
        );
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        assert_dropped_rich_receive_blocks_lean_and_recovers(
            SyntheticRecvCompletion::success(&abort, libc::MSG_NOTIFICATION),
            SctpRecordSync::DataTail,
            SctpRecordSync::Synced,
        );
        let notification_head = test_fragmented_stream_reset_notification();
        assert_dropped_rich_receive_blocks_lean_and_recovers(
            SyntheticRecvCompletion::success(&notification_head[..8], libc::MSG_NOTIFICATION),
            SctpRecordSync::Synced,
            SctpRecordSync::NotificationTail,
        );
    }

    #[test]
    fn lean_stash_rejection_preserves_precedence_teardown_and_notification_policy() {
        with_ringless_poll_context_for_test(1, |owner, cx| {
            let reactor = owner.reactor_ptr();
            let rich_pointer_calls = Rc::new(Cell::new(0));
            let rich_drops = Rc::new(Cell::new(0));
            let mut stream = ringless_sctp_stream();
            let state_ptr = stage_dropped_scalar_receive(
                &mut stream,
                reactor,
                SyntheticRecvCompletion::success(b"done", libc::MSG_EOR),
                &rich_pointer_calls,
                &rich_drops,
                true,
            );
            assert!(
                unsafe { (*state_ptr).is_completed() },
                "rich CQE did not complete before future destruction"
            );
            test_hooks::fail_next_raw_sqe_submit();

            let local_buffer = retained_constructor_buffer(
                None,
                Rc::new(Cell::new(0)),
                Rc::new(Cell::new(0)),
                false,
            );
            let mut local = stream.recv(local_buffer, 0);
            let Poll::Ready((local_result, local_buffer)) = Pin::new(&mut local).poll(cx) else {
                panic!("zero-length receive behind a stash remained pending");
            };
            assert_eq!(
                local_result
                    .expect_err("zero-length receive unexpectedly succeeded")
                    .to_string(),
                ZERO_LENGTH_SCTP_RECV
            );
            drop(local);
            drop(local_buffer);
            assert_eq!(stream.recv_state.stashed.state_ptr, state_ptr);

            let context_buffer = retained_constructor_buffer(
                None,
                Rc::new(Cell::new(0)),
                Rc::new(Cell::new(0)),
                false,
            );
            let mut rejected = stream.recv(context_buffer, 16);
            let mut rejected_cx = Context::from_waker(std::task::Waker::noop());
            let Poll::Ready((context_result, context_buffer)) =
                Pin::new(&mut rejected).poll(&mut rejected_cx)
            else {
                panic!("invalid-context lean rejection remained pending");
            };
            assert_eq!(
                context_result
                    .expect_err("invalid-context lean receive unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::NotConnected
            );
            drop(rejected);
            drop(context_buffer);
            assert_eq!(stream.recv_state.stashed.state_ptr, state_ptr);
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 1);

            drop(stream);
            assert_eq!(rich_pointer_calls.get(), 1);
            assert_eq!(rich_drops.get(), 1);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 1);
            let injected = test_hooks::take_raw_sqe_submit_failure()
                .expect("stream teardown consumed the receive-submission sentinel");
            assert_eq!(injected.kind(), io::ErrorKind::WouldBlock);

            let replacement = unsafe { (&mut *reactor).alloc_op() };
            assert_eq!(
                replacement, state_ptr,
                "stream drop did not recycle the stashed state"
            );
            unsafe { (&mut *reactor).free_op(replacement) };

            let mut abandoned_state = CompletionState::empty();
            abandoned_state.set_ring_abandoned();
            let abandoned_ptr = std::ptr::addr_of_mut!(abandoned_state);
            let mut abandoned_stream = ringless_sctp_stream();
            unsafe {
                abandoned_stream.recv_state.set_stashed_live_for_test(
                    abandoned_ptr,
                    0,
                    reject_abandoned_stashed_processing,
                );
            }
            assert_lean_stash_rejection(
                &mut abandoned_stream,
                cx,
                abandoned_ptr,
                StashedSctpRecvState::Live,
            );
            assert_eq!(abandoned_stream.recv_state.stashed.state_ptr, abandoned_ptr);
            abandoned_stream.recv_state.clear_stashed_local();
            drop(abandoned_stream);

            let mut signaling_stream = ringless_sctp_stream();
            assert!(signaling_stream.recv_state.any_notification_visible.get());
            let pointer_calls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let buffer = retained_constructor_buffer(
                None,
                Rc::clone(&pointer_calls),
                Rc::clone(&drops),
                false,
            );
            test_hooks::fail_next_raw_sqe_submit();
            let mut lean = signaling_stream.recv(buffer, 16);
            let Poll::Ready((result, returned)) = Pin::new(&mut lean).poll(cx) else {
                panic!("no-stash signaling receive did not reach ordinary submission");
            };
            assert_eq!(
                result
                    .expect_err("injected no-stash receive unexpectedly succeeded")
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(pointer_calls.get(), 1);
            assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 0);
            drop(lean);
            drop(returned);
            assert_eq!(drops.get(), 1);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    fn assert_rejected_scalar_completion(
        reactor: *mut Reactor,
        origin_cx: &mut Context<'_>,
        recv_state: &mut SctpRecvState,
        completion: SyntheticRecvCompletion<'_>,
    ) -> (*mut CompletionState, usize) {
        let pointer_calls = Rc::new(Cell::new(0));
        let published_len = Rc::new(Cell::new(0));
        let buffer =
            RejectedContextRecvBuffer::new(Rc::clone(&pointer_calls), Rc::clone(&published_len));
        let backing_address = buffer.backing_ptr() as usize;
        let state_ptr = unsafe { (&mut *reactor).alloc_op() };
        assert!(
            !state_ptr.is_null(),
            "rejected scalar state allocation failed"
        );

        let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
        let mut buffer = Some(buffer);
        let mut payload =
            unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut buffer, 32) };
        assert!(buffer.is_none(), "scalar buffer was not retained");
        let retained_address = unsafe { payload.as_mut() as *mut _ as usize };
        unsafe {
            let retained = payload.as_mut();
            if completion.result >= 0 {
                let iovec = retained.iovec.assume_init_ref();
                assert!(completion.data.len() <= iovec.iov_len);
                std::ptr::copy_nonoverlapping(
                    completion.data.as_ptr(),
                    iovec.iov_base.cast::<u8>(),
                    completion.data.len(),
                );
            }
            let msg = retained.msghdr.assume_init_mut();
            if completion.result == 0 {
                msg.msg_controllen = 0;
            } else {
                assert_eq!(
                    msg.msg_controllen, SCTP_RECV_CONTROL_LEN,
                    "fixture lost its uninitialized control-capacity state"
                );
            }
            msg.msg_flags = completion.msg_flags;
            retained.buffer.panic_on_pointer = completion.result <= 0;
            (*state_ptr).attach_retained_payload(payload);
        }

        let mut future: RecvFuture<'_, RejectedContextRecvBuffer> = RecvFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: unsafe { invalid_submitted_fd_op_state(state_ptr) },
            buffer: None,
            write_base_len: 0,
            len: 32,
            input_error: None,
            recv_state,
            _marker: PhantomData,
        };
        let mut rejected_cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            Pin::new(&mut future).poll(&mut rejected_cx).is_pending(),
            "incomplete scalar receive returned its retained buffer"
        );
        unsafe {
            assert!((*state_ptr).is_context_rejected());
            (*state_ptr).result = completion.result;
            (*state_ptr).set_completed();
        }

        let Poll::Ready((result, returned)) = Pin::new(&mut future).poll(origin_cx) else {
            panic!("completed rejected scalar receive remained pending");
        };
        assert_eq!(
            result
                .expect_err("rejected scalar receive unexpectedly succeeded")
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert!(future.state_ptr.is_null());
        drop(future);
        assert_eq!(returned.backing_ptr() as usize, backing_address);
        assert_eq!(
            pointer_calls.get(),
            if completion.result > 0 { 2 } else { 1 },
            "rejected completion made an unexpected caller-pointer inspection"
        );
        assert_eq!(published_len.get(), 0);
        assert_eq!(IoBuffReadOnly::len(&returned), 0);
        if completion.result > 0 {
            assert_eq!(
                &returned.bytes[..completion.data.len()],
                completion.data,
                "synthetic kernel bytes did not remain in the unpublished tail"
            );
        }
        drop(returned);
        assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
        (state_ptr, retained_address)
    }

    fn leading_full_rejected_chain() -> (IoBuffVecMut<2>, usize) {
        let mut full = IoBuffMut::new(0, 4, 0).expect("full segment allocation failed");
        full.payload_append(b"full")
            .expect("full segment initialization failed");
        let mut writable = IoBuffMut::new(0, 32, 0).expect("writable segment allocation failed");
        let writable_address = writable.as_mut_ptr() as usize;
        (IoBuffVecMut::from_array([full, writable]), writable_address)
    }

    fn assert_rejected_vectored_completion(
        reactor: *mut Reactor,
        origin_cx: &mut Context<'_>,
        recv_state: &mut SctpRecvState,
        completion: SyntheticRecvCompletion<'_>,
    ) -> (*mut CompletionState, usize) {
        let (chain, writable_address) = leading_full_rejected_chain();
        let (iov_count, writable_len) = chain
            .checked_read_iovec_count_and_writable_len()
            .expect("rejected vectored receive shape overflowed");
        assert_eq!((iov_count, writable_len), (1, 32));
        let state_ptr = unsafe { (&mut *reactor).alloc_op() };
        assert!(
            !state_ptr.is_null(),
            "rejected vector state allocation failed"
        );

        let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
        let mut buffer = Some(chain);
        let mut payload = unsafe {
            emplace_retained_sctp_recv_vectored_payload(
                retained_pool,
                &mut buffer,
                (iov_count, writable_len),
            )
        }
        .expect("rejected vectored receive emplacement failed");
        assert!(buffer.is_none(), "vectored chain was not retained");
        let retained_address = unsafe { payload.as_mut() as *mut _ as usize };
        unsafe {
            let retained = payload.as_mut();
            if completion.result >= 0 {
                let first_iovec = retained.iovecs[0].assume_init_ref();
                assert!(completion.data.len() <= first_iovec.iov_len);
                std::ptr::copy_nonoverlapping(
                    completion.data.as_ptr(),
                    first_iovec.iov_base.cast::<u8>(),
                    completion.data.len(),
                );
            }
            let msg = retained.msghdr.assume_init_mut();
            if completion.result == 0 {
                msg.msg_controllen = 0;
            } else {
                assert_eq!(
                    msg.msg_controllen, SCTP_RECV_CONTROL_LEN,
                    "fixture lost its uninitialized control-capacity state"
                );
            }
            msg.msg_flags = completion.msg_flags;
            (*state_ptr).attach_retained_payload(payload);
        }

        let mut future: RecvVectoredFuture<'_, 2> = RecvVectoredFuture {
            fd: RuntimeFd::INVALID,
            state_ptr: unsafe { invalid_submitted_fd_op_state(state_ptr) },
            buffer: None,
            iov_count,
            writable: writable_len,
            invalid_aggregate: false,
            recv_state,
            _marker: PhantomData,
        };
        let mut rejected_cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            Pin::new(&mut future).poll(&mut rejected_cx).is_pending(),
            "incomplete vectored receive returned its retained chain"
        );
        unsafe {
            assert!((*state_ptr).is_context_rejected());
            (*state_ptr).result = completion.result;
            (*state_ptr).set_completed();
        }

        let Poll::Ready((result, mut returned)) = Pin::new(&mut future).poll(origin_cx) else {
            panic!("completed rejected vectored receive remained pending");
        };
        assert_eq!(
            result
                .expect_err("rejected vectored receive unexpectedly succeeded")
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert!(future.state_ptr.is_null());
        drop(future);
        assert_eq!(
            returned
                .get(0)
                .expect("full segment missing")
                .payload_bytes(),
            b"full"
        );
        assert_eq!(
            returned
                .get(1)
                .expect("writable segment missing")
                .payload_len(),
            0
        );
        let returned_ptr = returned
            .get_mut(1)
            .expect("writable segment missing")
            .as_mut_ptr();
        assert_eq!(returned_ptr as usize, writable_address);
        assert_eq!(
            unsafe { std::slice::from_raw_parts(returned_ptr, completion.data.len()) },
            completion.data
        );
        drop(returned);
        assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
        (state_ptr, retained_address)
    }

    #[test]
    fn rejected_scalar_completion_updates_record_state_without_publishing_bytes() {
        with_ringless_poll_context_for_test(1, |owner, origin_cx| {
            let reactor = owner.reactor_ptr();
            let mut recv_state = SctpRecvState::external();

            let (first_state, first_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(b"tail", 0),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::DataTail);

            let (second_state, second_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(b"done", libc::MSG_EOR),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::Synced);
            assert_eq!(second_state, first_state);
            assert_eq!(second_retained, first_retained);

            recv_state.record_sync = SctpRecordSync::DataTail;
            let (third_state, third_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(&[], 0),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::Synced);
            assert_eq!(third_state, first_state);
            assert_eq!(third_retained, first_retained);

            recv_state.record_sync = SctpRecordSync::DataTail;
            let (fourth_state, fourth_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::error(-libc::EIO, libc::MSG_EOR),
            );
            assert_eq!(
                recv_state.record_sync,
                SctpRecordSync::DataTail,
                "failed CQE changed record-recovery state"
            );
            assert_eq!(fourth_state, first_state);
            assert_eq!(fourth_retained, first_retained);

            recv_state.record_sync = SctpRecordSync::Synced;
            let notification = test_fragmented_stream_reset_notification();
            let (fifth_state, fifth_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(&notification[..8], libc::MSG_NOTIFICATION),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::NotificationTail);
            let (sixth_state, sixth_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(&notification[8..16], libc::MSG_NOTIFICATION),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::NotificationTail);
            let (seventh_state, seventh_retained) = assert_rejected_scalar_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(
                    &notification[16..],
                    libc::MSG_NOTIFICATION | libc::MSG_EOR,
                ),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::Synced);
            for (state, retained) in [
                (fifth_state, fifth_retained),
                (sixth_state, sixth_retained),
                (seventh_state, seventh_retained),
            ] {
                assert_eq!(state, first_state);
                assert_eq!(retained, first_retained);
            }

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 7);
            assert_eq!(stats.pooled_reuses, 6);
            assert_eq!(stats.pooled_frees, 7);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[test]
    fn failed_sctp_recv_cqe_does_not_read_uninitialized_control() {
        with_ringless_poll_context_for_test(1, |owner, origin_cx| {
            let reactor = owner.reactor_ptr();
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state_ptr.is_null(), "failed-CQE state allocation failed");

            let pointer_calls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let buffer = retained_constructor_buffer(
                None,
                Rc::clone(&pointer_calls),
                Rc::clone(&drops),
                false,
            );
            let backing_address = buffer.bytes.as_ptr() as usize;
            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
            let mut buffer = Some(buffer);
            let mut payload =
                unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut buffer, 32) };
            assert!(buffer.is_none(), "failed-CQE buffer was not retained");
            let retained_address = unsafe { payload.as_mut() as *mut _ as usize };
            unsafe {
                let retained = payload.as_mut();
                assert_eq!(
                    retained.msghdr.assume_init_ref().msg_controllen,
                    SCTP_RECV_CONTROL_LEN,
                    "fixture must retain the submitted control capacity"
                );
                // Do not initialize any control byte. A failed recvmsg CQE
                // leaves this storage untouched even though msg_controllen
                // still reports the submitted capacity.
                (*state_ptr).attach_retained_payload(payload);
                (*state_ptr).result = -libc::EIO;
                (*state_ptr).set_completed();
            }

            let mut recv_state = SctpRecvState::external();
            let mut future: RecvFuture<'_, RetainedConstructorBuffer> = RecvFuture {
                fd: RuntimeFd::INVALID,
                state_ptr: unsafe { invalid_submitted_fd_op_state(state_ptr) },
                buffer: None,
                write_base_len: 32,
                len: 32,
                input_error: None,
                recv_state: &mut recv_state,
                _marker: PhantomData,
            };
            let Poll::Ready((result, returned)) = Pin::new(&mut future).poll(origin_cx) else {
                panic!("failed SCTP receive remained pending");
            };
            assert_eq!(
                result
                    .expect_err("failed SCTP receive unexpectedly succeeded")
                    .raw_os_error(),
                Some(libc::EIO)
            );
            assert!(future.state_ptr.is_null());
            drop(future);
            assert_eq!(
                returned.bytes.as_ptr() as usize,
                backing_address,
                "failed CQE returned a different owner"
            );
            assert_eq!(
                pointer_calls.get(),
                1,
                "failed CQE inspected the caller buffer after construction"
            );
            assert_eq!(drops.get(), 0, "failed CQE dropped the returned owner");

            let retry_pointer_calls = Rc::new(Cell::new(0));
            let retry_drops = Rc::new(Cell::new(0));
            let mut retry_buffer = Some(retained_constructor_buffer(
                None,
                Rc::clone(&retry_pointer_calls),
                Rc::clone(&retry_drops),
                false,
            ));
            let mut retry =
                unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut retry_buffer, 32) };
            assert_eq!(
                unsafe { retry.as_mut() as *mut _ as usize },
                retained_address,
                "failed-CQE extraction did not recycle retained backing"
            );
            let returned_retry = unsafe { retry.take(&mut *retained_pool.as_ptr()) }.buffer;

            drop(returned);
            drop(returned_retry);
            assert_eq!(drops.get(), 1);
            assert_eq!(retry_drops.get(), 1);
            assert_eq!(retry_pointer_calls.get(), 1);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 2);
            assert_eq!(stats.pooled_reuses, 1);
            assert_eq!(stats.pooled_frees, 2);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[test]
    fn rejected_stashed_zero_completion_skips_pointer_and_retires_discard() {
        with_ringless_poll_context_for_test(1, |owner, origin_cx| {
            let reactor = owner.reactor_ptr();
            let state_ptr = unsafe { (&mut *reactor).alloc_op() };
            assert!(!state_ptr.is_null(), "stashed state allocation failed");

            let pointer_calls = Rc::new(Cell::new(0));
            let published_len = Rc::new(Cell::new(0));
            let buffer = RejectedContextRecvBuffer::new(
                Rc::clone(&pointer_calls),
                Rc::clone(&published_len),
            );
            let retained_pool = unsafe { Reactor::retained_payload_pool_ptr(reactor) };
            let mut buffer = Some(buffer);
            let mut payload =
                unsafe { emplace_retained_sctp_recv_payload(retained_pool, &mut buffer, 32) };
            assert!(buffer.is_none(), "stashed buffer was not retained");
            unsafe {
                let retained = payload.as_mut();
                retained.buffer.panic_on_pointer = true;
                let msg = retained.msghdr.assume_init_mut();
                msg.msg_controllen = 0;
                msg.msg_flags = 0;
                (*state_ptr).attach_retained_payload(payload);
            }

            let mut recv_state = SctpRecvState::external();
            recv_state.record_sync = SctpRecordSync::DataTail;
            unsafe {
                recv_state.set_stashed_live_for_test(
                    state_ptr,
                    0,
                    process_stashed_sctp_recv::<RejectedContextRecvBuffer>,
                );
            }

            let mut rejected_cx = Context::from_waker(std::task::Waker::noop());
            assert!(
                unsafe { recv_state.poll_stashed(&mut rejected_cx) }.is_pending(),
                "incomplete stashed receive returned early"
            );
            unsafe {
                assert!((*state_ptr).is_context_rejected());
                (*state_ptr).result = 0;
                (*state_ptr).set_completed();
            }
            assert!(matches!(
                unsafe { recv_state.poll_stashed(origin_cx) },
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
            ));

            assert_eq!(recv_state.record_sync, SctpRecordSync::Synced);
            assert!(recv_state.stashed.state_ptr.is_null());
            assert_eq!(
                pointer_calls.get(),
                1,
                "zero-byte stashed recovery inspected the retained buffer"
            );
            assert_eq!(published_len.get(), 0);
            assert_eq!(unsafe { (&*reactor).live_op_count() }, 0);
            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 1);
            assert_eq!(stats.pooled_frees, 1);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    #[test]
    fn rejected_vectored_completion_uses_copied_iovec_without_publishing_bytes() {
        with_ringless_poll_context_for_test(1, |owner, origin_cx| {
            let reactor = owner.reactor_ptr();
            let mut recv_state = SctpRecvState::external();

            let (first_state, first_retained) = assert_rejected_vectored_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(b"tail", 0),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::DataTail);

            let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
            let (second_state, second_retained) = assert_rejected_vectored_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(&abort, libc::MSG_NOTIFICATION),
            );
            assert_eq!(
                recv_state.record_sync,
                SctpRecordSync::Synced,
                "copied active iovec did not expose the PDAPI abort"
            );
            assert_eq!(second_state, first_state);
            assert_eq!(second_retained, first_retained);

            recv_state.record_sync = SctpRecordSync::DataTail;
            let (third_state, third_retained) = assert_rejected_vectored_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::error(-libc::EIO, libc::MSG_EOR),
            );
            assert_eq!(
                recv_state.record_sync,
                SctpRecordSync::DataTail,
                "failed rejected CQE changed record-recovery state"
            );
            assert_eq!(third_state, first_state);
            assert_eq!(third_retained, first_retained);

            recv_state.record_sync = SctpRecordSync::Synced;
            let notification = test_fragmented_stream_reset_notification();
            let (fourth_state, fourth_retained) = assert_rejected_vectored_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(&notification[..16], libc::MSG_NOTIFICATION),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::NotificationTail);
            let (fifth_state, fifth_retained) = assert_rejected_vectored_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(&notification[16..32], libc::MSG_NOTIFICATION),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::NotificationTail);
            let (sixth_state, sixth_retained) = assert_rejected_vectored_completion(
                reactor,
                origin_cx,
                &mut recv_state,
                SyntheticRecvCompletion::success(
                    &notification[32..],
                    libc::MSG_NOTIFICATION | libc::MSG_EOR,
                ),
            );
            assert_eq!(recv_state.record_sync, SctpRecordSync::Synced);
            for (state, retained) in [
                (fourth_state, fourth_retained),
                (fifth_state, fifth_retained),
                (sixth_state, sixth_retained),
            ] {
                assert_eq!(state, first_state);
                assert_eq!(retained, first_retained);
            }

            let stats = unsafe { (&*reactor).retained_payload_stats() };
            assert_eq!(stats.pooled_allocs, 6);
            assert_eq!(stats.pooled_reuses, 5);
            assert_eq!(stats.pooled_frees, 6);
            assert_eq!(stats.heap_fallbacks, 0);
            assert_eq!(owner.inflight_op_count_for_test(), 0);
        });
    }

    fn notification_retires_discard_for_test(data_slice: &[u8], msg_flags: libc::c_int) -> bool {
        let parsed_notification = parse_sctp_notification_once(data_slice, msg_flags);
        sctp_notification_retires_discard(parsed_notification.as_ref())
    }

    fn process_visible_completion_for_test(
        state: &mut SctpRecvState,
        data_slice: &[u8],
        msg: &libc::msghdr,
    ) -> SctpMetadataCompletion {
        let recovery_target = state.bounded_recovery_prefix_target(data_slice.len(), msg.msg_flags);
        state.process_metadata_completion(
            data_slice.len(),
            test_recv_header(msg),
            data_slice,
            &data_slice[..recovery_target],
            SctpCompletionPublication::Visible(Ok(None)),
        )
    }

    fn process_two_segment_completion_for_test(
        state: &mut SctpRecvState,
        data: &[u8],
        first_len: usize,
        msg_flags: libc::c_int,
        visible: bool,
    ) -> (SctpMetadataCompletion, usize, usize) {
        assert!(first_len <= data.len());
        let second_len = data.len() - first_len;
        let mut first = IoBuffMut::new(0, first_len, 0).expect("first segment allocation failed");
        let mut second =
            IoBuffMut::new(0, second_len, 0).expect("second segment allocation failed");
        if first_len != 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), first.as_mut_ptr(), first_len);
            }
        }
        if second_len != 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr().add(first_len),
                    second.as_mut_ptr(),
                    second_len,
                );
            }
        }
        let first_iovec = libc::iovec {
            iov_base: first.as_mut_ptr().cast(),
            iov_len: first_len,
        };
        let mut chain = IoBuffVecMut::from_array([first, second]);
        let recovery_target = state.bounded_recovery_prefix_target(data.len(), msg_flags);
        sctp_vectored_received_slices!(
            &mut chain,
            &first_iovec,
            data.len(),
            recovery_target,
            prefix_storage,
            data_slice,
            recovery_prefix
        );
        let view_lengths = (data_slice.len(), recovery_prefix.len());
        let publication = if visible {
            SctpCompletionPublication::Visible(Ok(None))
        } else {
            SctpCompletionPublication::Unpublished
        };
        let action = state.process_metadata_completion(
            data.len(),
            SctpRecvHeader {
                msg_controllen: 0,
                msg_flags,
            },
            data_slice,
            recovery_prefix,
            publication,
        );
        (action, view_lengths.0, view_lengths.1)
    }

    fn assert_published_meta(
        action: SctpMetadataCompletion,
        expected: SctpRecvMeta,
        context: &str,
    ) {
        match action {
            SctpMetadataCompletion::Publish(Ok(actual)) => {
                assert_eq!(actual, expected, "{context}")
            }
            SctpMetadataCompletion::Publish(Err(err)) => {
                panic!("{context}: unexpected error: {err}")
            }
            SctpMetadataCompletion::Consume => panic!("{context}: completion was consumed"),
        }
    }

    #[test]
    fn vectored_public_notifications_remain_first_iovec_only_at_every_prefix_size() {
        let cases = [
            (
                "shutdown",
                test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12),
                12,
            ),
            (
                "remote error",
                test_notification_buffer(LOCAL_SCTP_REMOTE_ERROR, 16),
                16,
            ),
            (
                "association change",
                test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 20),
                20,
            ),
            (
                "partial delivery",
                test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED),
                24,
            ),
            (
                "stream reset",
                test_fragmented_stream_reset_notification(),
                44,
            ),
        ];

        for (name, notification, expected_len) in cases {
            assert_eq!(notification.len(), expected_len, "{name}: fixture size");
            let expected = parse_notification(&notification)
                .unwrap_or_else(|err| panic!("{name}: fixture did not parse: {err}"));
            let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

            let mut scalar = SctpRecvState::external();
            assert_published_meta(
                process_visible_completion_for_test(&mut scalar, &notification, &msg),
                expected,
                &format!("{name}: scalar control"),
            );

            let mut first_complete = SctpRecvState::external();
            let (action, data_len, recovery_len) = process_two_segment_completion_for_test(
                &mut first_complete,
                &notification,
                notification.len(),
                msg.msg_flags,
                true,
            );
            assert_eq!(data_len, notification.len(), "{name}: first-iovec view");
            assert_eq!(recovery_len, 0, "{name}: synced gather target");
            assert_published_meta(action, expected, &format!("{name}: vectored control"));

            let mut split = SctpRecvState::external();
            let (action, data_len, recovery_len) = process_two_segment_completion_for_test(
                &mut split,
                &notification,
                5,
                msg.msg_flags,
                true,
            );
            assert_eq!(data_len, 5, "{name}: split first-iovec view");
            assert_eq!(recovery_len, 0, "{name}: split synced gather target");
            match action {
                SctpMetadataCompletion::Publish(Err(err)) => assert_eq!(
                    err.kind(),
                    io::ErrorKind::InvalidData,
                    "{name}: split parser error"
                ),
                SctpMetadataCompletion::Publish(Ok(meta)) => {
                    panic!("{name}: split notification unexpectedly parsed as {meta:?}")
                }
                SctpMetadataCompletion::Consume => {
                    panic!("{name}: split visible notification was consumed")
                }
            }
            assert_eq!(split.record_sync, SctpRecordSync::Synced);
        }
    }

    #[test]
    fn data_notification_tail_gates_every_source_eor_and_publication_combination() {
        let hostile_data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let malformed_notification = [0xa5; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN];

        for visible in [false, true] {
            for notification in [false, true] {
                for end_of_record in [false, true] {
                    let mut state = SctpRecvState::external();
                    state.record_sync = SctpRecordSync::DataNotificationTail;
                    state.nested_notification_prefix[..5].fill(0xa5);
                    state.nested_prefix_state = 5;
                    let original_prefix = state.nested_notification_prefix;
                    let data = if notification {
                        &malformed_notification[5..]
                    } else {
                        hostile_data.as_slice()
                    };
                    let msg_flags = if notification {
                        libc::MSG_NOTIFICATION
                    } else {
                        0
                    } | if end_of_record { libc::MSG_EOR } else { 0 };
                    let recovery_target =
                        state.bounded_recovery_prefix_target(data.len(), msg_flags);
                    let publication = if visible {
                        SctpCompletionPublication::Visible(Ok(None))
                    } else {
                        SctpCompletionPublication::Unpublished
                    };
                    let action = state.process_metadata_completion(
                        data.len(),
                        SctpRecvHeader {
                            msg_controllen: 0,
                            msg_flags,
                        },
                        data,
                        &data[..recovery_target],
                        publication,
                    );
                    assert!(
                        matches!(action, SctpMetadataCompletion::Consume),
                        "visible={visible} notification={notification} eor={end_of_record}"
                    );

                    if notification {
                        assert_eq!(recovery_target, 19);
                        assert_eq!(
                            state.record_sync,
                            if end_of_record {
                                SctpRecordSync::DataTail
                            } else {
                                SctpRecordSync::DataNotificationTail
                            }
                        );
                        if !end_of_record {
                            assert_eq!(state.nested_prefix_len(), 24);
                            assert_ne!(
                                state.nested_prefix_state & SCTP_NESTED_PREFIX_CLASSIFIED,
                                0
                            );
                            assert_eq!(
                                state.bounded_recovery_prefix_target(
                                    data.len(),
                                    libc::MSG_NOTIFICATION
                                ),
                                0,
                                "classified prefix requested another gather"
                            );
                        }
                    } else {
                        assert_eq!(recovery_target, 0);
                        assert_eq!(
                            state.record_sync,
                            if end_of_record {
                                SctpRecordSync::NotificationTail
                            } else {
                                SctpRecordSync::DataNotificationTail
                            }
                        );
                        if !end_of_record {
                            assert_eq!(state.nested_prefix_len(), 5);
                            assert_eq!(state.nested_notification_prefix, original_prefix);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn terminal_split_pdapi_abort_preserves_visibility_filtering_and_vectored_recovery() {
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let terminal = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

        let mut explicit = SctpRecvState::external();
        explicit.record_sync = SctpRecordSync::DataTail;
        assert!(matches!(
            process_visible_completion_for_test(&mut explicit, &abort[..7], &partial),
            SctpMetadataCompletion::Consume
        ));
        assert_published_meta(
            process_visible_completion_for_test(&mut explicit, &abort[7..], &terminal),
            parse_notification(&abort).expect("PDAPI fixture did not parse"),
            "explicit terminal split PDAPI",
        );
        assert_eq!(explicit.record_sync, SctpRecordSync::Synced);

        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;
        let mut forced = SctpRecvState::configured(metadata_only);
        forced.record_sync = SctpRecordSync::DataTail;
        assert!(forced.should_consume_for_test(&abort[..7], &partial));
        assert!(forced.should_consume_for_test(&abort[7..], &terminal));
        assert_eq!(forced.record_sync, SctpRecordSync::Synced);

        let mut unpublished = SctpRecvState::external();
        unpublished.record_sync = SctpRecordSync::DataTail;
        unpublished.process_unpublished_for_test(&abort[..7], &partial);
        unpublished.process_unpublished_for_test(&abort[7..], &terminal);
        assert_eq!(unpublished.record_sync, SctpRecordSync::Synced);

        let mut vectored = SctpRecvState::external();
        vectored.record_sync = SctpRecordSync::DataTail;
        assert!(matches!(
            process_visible_completion_for_test(&mut vectored, &abort[..7], &partial),
            SctpMetadataCompletion::Consume
        ));
        let (action, data_len, recovery_len) = process_two_segment_completion_for_test(
            &mut vectored,
            &abort[7..],
            5,
            terminal.msg_flags,
            true,
        );
        assert_eq!(data_len, 5);
        assert_eq!(recovery_len, 17, "only the missing prefix was gathered");
        assert_published_meta(
            action,
            parse_notification(&abort).expect("PDAPI fixture did not parse"),
            "vectored terminal split PDAPI",
        );
        assert_eq!(vectored.record_sync, SctpRecordSync::Synced);
    }

    #[test]
    fn sctp_partial_delivery_abort_notification_retires_discard() {
        // Linux UAPI defines SCTP_PARTIAL_DELIVERY_ABORTED as 0. A real
        // kernel PDAPI abort is not deterministic to force on loopback, so the
        // discard decision is pinned with a synthetic Linux-layout event.
        assert_eq!(SCTP_PARTIAL_DELIVERY_ABORTED, 0);

        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(notification_retires_discard_for_test(&data, msg.msg_flags));
        let mut state = SctpRecvState::external();
        state.record_sync = SctpRecordSync::DataTail;
        state.process_unpublished_for_test(&data, &msg);
        assert_eq!(state.record_sync, SctpRecordSync::Synced);

        state.process_unpublished_for_test(&data, &msg);
        assert_eq!(
            state.record_sync,
            SctpRecordSync::Synced,
            "a dropped PDAPI abort must not start discard when none was active"
        );
    }

    #[test]
    fn sctp_live_and_dropped_non_eor_abort_retirement_agree() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;
        let mut live = SctpRecvState::configured(metadata_only);
        live.record_sync = SctpRecordSync::DataTail;
        assert!(
            live.should_consume_for_test(&data, &msg),
            "a FlowIO-forced PDAPI abort remains internal"
        );

        let mut dropped = SctpRecvState::configured(metadata_only);
        dropped.record_sync = SctpRecordSync::DataTail;
        dropped.process_unpublished_for_test(&data, &msg);
        assert_eq!(
            live.record_sync, dropped.record_sync,
            "live and dropped completion retirement must use one oracle"
        );
        assert_eq!(
            live.record_sync,
            SctpRecordSync::Synced,
            "a complete PDAPI abort retires discard even without MSG_EOR"
        );
    }

    #[test]
    fn internal_and_unpublished_record_sync_transitions_are_differentially_equal() {
        let notification = test_fragmented_stream_reset_notification();
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let non_abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED + 1);
        let cases: &[(&str, SctpRecordSync, &[u8], libc::c_int, SctpRecordSync)] = &[
            (
                "hidden notification head",
                SctpRecordSync::Synced,
                &notification[..8],
                libc::MSG_NOTIFICATION,
                SctpRecordSync::NotificationTail,
            ),
            (
                "opaque notification continuation",
                SctpRecordSync::NotificationTail,
                &abort,
                libc::MSG_NOTIFICATION,
                SctpRecordSync::NotificationTail,
            ),
            (
                "terminal notification continuation",
                SctpRecordSync::NotificationTail,
                &notification[40..],
                libc::MSG_NOTIFICATION | libc::MSG_EOR,
                SctpRecordSync::Synced,
            ),
            (
                "data continuation",
                SctpRecordSync::DataTail,
                b"tail",
                0,
                SctpRecordSync::DataTail,
            ),
            (
                "terminal data continuation",
                SctpRecordSync::DataTail,
                b"tail",
                libc::MSG_EOR,
                SctpRecordSync::Synced,
            ),
            (
                "complete PDAPI abort",
                SctpRecordSync::DataTail,
                &abort,
                libc::MSG_NOTIFICATION,
                SctpRecordSync::Synced,
            ),
            (
                "non-abort notification head",
                SctpRecordSync::DataTail,
                &non_abort,
                libc::MSG_NOTIFICATION,
                SctpRecordSync::DataNotificationTail,
            ),
            (
                "unrelated notification EOR",
                SctpRecordSync::DataTail,
                &non_abort,
                libc::MSG_NOTIFICATION | libc::MSG_EOR,
                SctpRecordSync::DataTail,
            ),
        ];

        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;
        for &(name, initial, data, flags, expected) in cases {
            let msg = test_msghdr_with_flags(flags);
            let mut live = SctpRecvState::configured(metadata_only);
            live.record_sync = initial;
            assert!(
                live.should_consume_for_test(data, &msg),
                "{name}: internal completion became caller-visible"
            );

            let mut unpublished = SctpRecvState::configured(metadata_only);
            unpublished.record_sync = initial;
            unpublished.process_unpublished_for_test(data, &msg);
            assert_eq!(
                live.record_sync, unpublished.record_sync,
                "{name}: live/drop drift"
            );
            assert_eq!(live.record_sync, expected, "{name}: wrong transition");
        }
    }

    #[test]
    fn fragmented_stream_reset_tail_is_opaque_until_eor_then_parsing_resumes() {
        let notification = test_fragmented_stream_reset_notification();
        assert_eq!(notification.len(), 44, "live-derived event size drifted");
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let terminal = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        let mut visible = SctpRecvState::external();

        let head = &notification[..8];
        assert!(
            matches!(
                process_visible_completion_for_test(&mut visible, head, &partial),
                SctpMetadataCompletion::Publish(Err(_))
            ),
            "fragmented visible head did not fail closed"
        );
        assert_eq!(visible.record_sync, SctpRecordSync::NotificationTail);

        for start in (8..notification.len()).step_by(8) {
            let end = std::cmp::min(start + 8, notification.len());
            let msg = if end == notification.len() {
                &terminal
            } else {
                &partial
            };
            let continuation = &notification[start..end];
            assert!(matches!(
                process_visible_completion_for_test(&mut visible, continuation, msg),
                SctpMetadataCompletion::Consume
            ));
            assert_eq!(
                visible.record_sync,
                if end == notification.len() {
                    SctpRecordSync::Synced
                } else {
                    SctpRecordSync::NotificationTail
                }
            );
        }

        let mut shutdown = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut shutdown, 8, 7);
        let shutdown_msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        assert!(matches!(
            process_visible_completion_for_test(&mut visible, &shutdown, &shutdown_msg),
            SctpMetadataCompletion::Publish(Ok(SctpRecvMeta::Notification(
                SctpNotification::Shutdown { assoc_id: 7 }
            )))
        ));
    }

    #[test]
    fn unpublished_stream_reset_fragments_recover_at_the_same_terminal_eor() {
        let notification = test_fragmented_stream_reset_notification();
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let terminal = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        let mut state = SctpRecvState::external();

        for start in (0..notification.len()).step_by(8) {
            let end = std::cmp::min(start + 8, notification.len());
            let msg = if end == notification.len() {
                &terminal
            } else {
                &partial
            };
            let completion = &notification[start..end];
            state.process_unpublished_for_test(completion, msg);
            assert_eq!(
                state.record_sync,
                if end == notification.len() {
                    SctpRecordSync::Synced
                } else {
                    SctpRecordSync::NotificationTail
                }
            );
        }
    }

    #[test]
    fn notification_tail_does_not_interpret_pdapi_shaped_continuation() {
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let mut state = SctpRecvState::external();
        state.record_sync = SctpRecordSync::NotificationTail;

        assert!(matches!(
            process_visible_completion_for_test(&mut state, &abort, &partial),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(state.record_sync, SctpRecordSync::NotificationTail);

        let terminal = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        assert!(matches!(
            process_visible_completion_for_test(&mut state, &[0], &terminal),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(state.record_sync, SctpRecordSync::Synced);
    }

    #[test]
    fn nested_pdapi_abort_split_at_every_prefix_boundary_retires_data_tail() {
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        for split in 1..abort.len() {
            let mut state = SctpRecvState::external();
            state.record_sync = SctpRecordSync::DataTail;
            assert!(matches!(
                process_visible_completion_for_test(&mut state, &abort[..split], &partial),
                SctpMetadataCompletion::Consume
            ));
            assert_eq!(state.record_sync, SctpRecordSync::DataNotificationTail);
            assert_eq!(state.nested_prefix_len(), split);

            let action = process_visible_completion_for_test(&mut state, &abort[split..], &partial);
            assert!(matches!(action, SctpMetadataCompletion::Publish(Err(_))));
            assert_eq!(
                state.record_sync,
                SctpRecordSync::Synced,
                "split {split} did not recognize the complete abort prefix"
            );
            assert_eq!(state.nested_prefix_len(), 0);
        }
    }

    #[test]
    fn nested_header_shaped_continuation_cannot_forge_pdapi_abort() {
        let notification = test_fragmented_stream_reset_notification();
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let terminal = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        let mut state = SctpRecvState::external();
        state.record_sync = SctpRecordSync::DataTail;

        assert!(matches!(
            process_visible_completion_for_test(&mut state, &notification[..8], &partial),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(state.record_sync, SctpRecordSync::DataNotificationTail);
        assert!(matches!(
            process_visible_completion_for_test(&mut state, &abort, &partial),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(
            state.record_sync,
            SctpRecordSync::DataNotificationTail,
            "continuation bytes were parsed as a fresh abort header"
        );
        assert_ne!(state.nested_prefix_state & SCTP_NESTED_PREFIX_CLASSIFIED, 0);

        assert!(matches!(
            process_visible_completion_for_test(&mut state, &[0], &terminal),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(
            state.record_sync,
            SctpRecordSync::DataTail,
            "unrelated notification EOR retired the underlying data tail"
        );
    }

    #[test]
    fn nested_short_and_malformed_notification_eor_preserve_data_tail() {
        let terminal = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let malformed = [0xa5; SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN];

        let mut first_eor = SctpRecvState::external();
        first_eor.record_sync = SctpRecordSync::DataTail;
        assert!(matches!(
            process_visible_completion_for_test(&mut first_eor, &[0], &terminal),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(first_eor.record_sync, SctpRecordSync::DataTail);

        let mut later_eor = SctpRecvState::external();
        later_eor.record_sync = SctpRecordSync::DataTail;
        assert!(matches!(
            process_visible_completion_for_test(&mut later_eor, &malformed[..9], &partial),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(later_eor.record_sync, SctpRecordSync::DataNotificationTail);
        assert!(matches!(
            process_visible_completion_for_test(&mut later_eor, &malformed[9..], &terminal),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(
            later_eor.record_sync,
            SctpRecordSync::DataTail,
            "a malformed completed prefix retired the underlying data tail"
        );
    }

    #[test]
    fn vectored_received_slices_preserve_first_iovec_and_recovery_views() {
        let mut empty = IoBuffVecMut::<1>::new();
        let empty_actual = 0usize;
        let empty_target = std::cmp::min(empty_actual, SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN);
        let empty_first_iovec = SCTP_EMPTY_FIRST_IOVEC;
        sctp_vectored_received_slices!(
            &mut empty,
            &empty_first_iovec,
            empty_actual,
            empty_target,
            empty_storage,
            empty_data,
            empty_recovery
        );
        assert!(empty_data.is_empty());
        assert!(empty_recovery.is_empty());

        let bytes = b"abcdefghijkl";
        let mut first = IoBuffMut::new(0, 8, 0).expect("first segment allocation failed");
        let mut second = IoBuffMut::new(0, 4, 0).expect("second segment allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), first.as_mut_ptr(), 8);
            std::ptr::copy_nonoverlapping(bytes.as_ptr().add(8), second.as_mut_ptr(), 4);
        }
        let first_iovec = libc::iovec {
            iov_base: first.as_mut_ptr().cast(),
            iov_len: 8,
        };
        let mut first_complete = IoBuffVecMut::from_array([first, second]);
        sctp_vectored_received_slices!(
            &mut first_complete,
            &first_iovec,
            8,
            8,
            first_complete_storage,
            first_complete_data,
            first_complete_recovery
        );
        assert_eq!(first_complete_data, &bytes[..8]);
        assert_eq!(first_complete_recovery, &bytes[..8]);

        let mut first = IoBuffMut::new(0, 5, 0).expect("first segment allocation failed");
        let mut second = IoBuffMut::new(0, 7, 0).expect("second segment allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), first.as_mut_ptr(), 5);
            std::ptr::copy_nonoverlapping(bytes.as_ptr().add(5), second.as_mut_ptr(), 7);
        }
        let first_iovec = libc::iovec {
            iov_base: first.as_mut_ptr().cast(),
            iov_len: 5,
        };
        let mut cross_segment = IoBuffVecMut::from_array([first, second]);
        sctp_vectored_received_slices!(
            &mut cross_segment,
            &first_iovec,
            bytes.len(),
            bytes.len(),
            cross_segment_storage,
            cross_segment_data,
            cross_segment_recovery
        );
        assert_eq!(cross_segment_data, &bytes[..5]);
        assert_eq!(cross_segment_recovery, bytes);

        sctp_vectored_received_slices!(
            &mut cross_segment,
            &first_iovec,
            bytes.len(),
            5,
            ungathered_storage,
            ungathered_data,
            ungathered_recovery
        );
        assert_eq!(ungathered_data, &bytes[..5]);
        assert_eq!(ungathered_recovery, &bytes[..5]);
    }

    #[test]
    fn vectored_received_slices_evaluate_each_operand_once() {
        let buffer_evaluations = Cell::new(0usize);
        let first_iovec_evaluations = Cell::new(0usize);
        let actual_evaluations = Cell::new(0usize);
        let target_evaluations = Cell::new(0usize);
        let mut empty = IoBuffVecMut::<1>::new();
        let no_first_iovec = SCTP_EMPTY_FIRST_IOVEC;

        sctp_vectored_received_slices!(
            {
                buffer_evaluations.set(buffer_evaluations.get() + 1);
                &mut empty
            },
            {
                first_iovec_evaluations.set(first_iovec_evaluations.get() + 1);
                &no_first_iovec
            },
            {
                actual_evaluations.set(actual_evaluations.get() + 1);
                0usize
            },
            {
                target_evaluations.set(target_evaluations.get() + 1);
                0usize
            },
            prefix_storage,
            data_slice,
            recovery_prefix
        );

        assert!(data_slice.is_empty());
        assert!(recovery_prefix.is_empty());
        assert_eq!(buffer_evaluations.get(), 1);
        assert_eq!(first_iovec_evaluations.get(), 1);
        assert_eq!(actual_evaluations.get(), 1);
        assert_eq!(target_evaluations.get(), 1);
    }

    #[test]
    fn vectored_received_slices_gather_only_target_from_larger_completion() {
        let bytes = b"0123456789abcdefghijklmnopqrst";
        assert_eq!(bytes.len(), 30);
        let mut first = IoBuffMut::new(0, 5, 0).expect("first segment allocation failed");
        let mut second = IoBuffMut::new(0, 19, 0).expect("second segment allocation failed");
        let mut third = IoBuffMut::new(0, 6, 0).expect("third segment allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), first.as_mut_ptr(), 5);
            std::ptr::copy_nonoverlapping(bytes.as_ptr().add(5), second.as_mut_ptr(), 19);
            std::ptr::copy_nonoverlapping(bytes.as_ptr().add(24), third.as_mut_ptr(), 6);
        }
        let first_iovec = libc::iovec {
            iov_base: first.as_mut_ptr().cast(),
            iov_len: 5,
        };
        let mut chain = IoBuffVecMut::from_array([first, second, third]);

        sctp_vectored_received_slices!(
            &mut chain,
            &first_iovec,
            30,
            SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN,
            prefix_storage,
            data_slice,
            recovery_prefix
        );

        assert_eq!(data_slice, &bytes[..5]);
        assert_eq!(
            recovery_prefix,
            &bytes[..SCTP_PDAPI_CLASSIFICATION_PREFIX_LEN]
        );
    }

    #[test]
    fn vectored_nested_pdapi_prefix_spans_iovecs_without_variable_tail_copy() {
        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let mut first = IoBuffMut::new(0, 5, 0).expect("first segment allocation failed");
        let mut second = IoBuffMut::new(0, 7, 0).expect("second segment allocation failed");
        let mut third = IoBuffMut::new(0, 12, 0).expect("third segment allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(abort.as_ptr(), first.as_mut_ptr(), 5);
            std::ptr::copy_nonoverlapping(abort.as_ptr().add(5), second.as_mut_ptr(), 7);
            std::ptr::copy_nonoverlapping(abort.as_ptr().add(12), third.as_mut_ptr(), 12);
        }
        let first_iovec = libc::iovec {
            iov_base: first.as_mut_ptr().cast(),
            iov_len: 5,
        };
        let mut chain = IoBuffVecMut::from_array([first, second, third]);
        let mut storage = MaybeUninit::uninit();
        let prefix =
            unsafe { sctp_vectored_received_prefix(&mut chain, abort.len(), &mut storage) };
        let data_slice = first_iov_view_from_copied_descriptor(&chain, first_iovec, abort.len());
        assert_eq!(prefix, abort);

        let mut state = SctpRecvState::external();
        state.record_sync = SctpRecordSync::DataTail;
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let action = state.process_metadata_completion(
            abort.len(),
            test_recv_header(&msg),
            data_slice,
            prefix,
            SctpCompletionPublication::Unpublished,
        );
        assert!(matches!(action, SctpMetadataCompletion::Consume));
        assert_eq!(state.record_sync, SctpRecordSync::Synced);
    }

    #[test]
    fn notification_layouts_decode_exact_minimum_from_unaligned_storage() {
        #[repr(align(64))]
        struct AlignedNotificationStorage([u8; SCTP_PEER_ADDR_CHANGE_MIN_LEN + 1]);

        let cases = notification_layout_cases();
        assert_eq!(
            cases.len(),
            13,
            "every supported notification arm is pinned"
        );

        for (name, bytes, expected) in cases {
            assert_eq!(
                read_u32_at(&bytes, SCTP_NOTIFICATION_LENGTH_OFFSET),
                Ok(bytes.len() as u32),
                "{name} fixture must declare its exact minimum"
            );
            assert_eq!(
                parse_notification(&bytes).unwrap_or_else(|err| panic!("{name}: {err}")),
                expected,
                "{name} aligned decoding"
            );

            let mut unaligned =
                AlignedNotificationStorage([0xa5; SCTP_PEER_ADDR_CHANGE_MIN_LEN + 1]);
            unaligned.0[1..bytes.len() + 1].copy_from_slice(&bytes);
            let unaligned = &unaligned.0[1..bytes.len() + 1];
            assert_ne!(
                unaligned
                    .as_ptr()
                    .align_offset(std::mem::align_of::<libc::sctp_sndrcvinfo>()),
                0,
                "{name} fixture must be unaligned for legacy send info"
            );
            assert_ne!(
                unaligned
                    .as_ptr()
                    .align_offset(std::mem::align_of::<libc::sctp_sndinfo>()),
                0,
                "{name} fixture must be unaligned for modern send info"
            );
            assert_eq!(
                parse_notification(unaligned)
                    .unwrap_or_else(|err| panic!("unaligned {name}: {err}")),
                expected,
                "{name} unaligned decoding"
            );
        }
    }

    #[test]
    fn notification_layouts_reject_each_one_byte_short_form() {
        let layouts = [
            (
                "association change",
                LOCAL_SCTP_ASSOC_CHANGE,
                SCTP_ASSOC_CHANGE_MIN_LEN,
            ),
            (
                "peer address change",
                LOCAL_SCTP_PEER_ADDR_CHANGE,
                SCTP_PEER_ADDR_CHANGE_MIN_LEN,
            ),
            (
                "legacy send failed",
                LOCAL_SCTP_SEND_FAILED,
                SCTP_LEGACY_SEND_FAILED_MIN_LEN,
            ),
            (
                "remote error",
                LOCAL_SCTP_REMOTE_ERROR,
                SCTP_REMOTE_ERROR_MIN_LEN,
            ),
            ("shutdown", LOCAL_SCTP_SHUTDOWN_EVENT, SCTP_SHUTDOWN_MIN_LEN),
            (
                "adaptation indication",
                LOCAL_SCTP_ADAPTATION_INDICATION,
                SCTP_ADAPTATION_MIN_LEN,
            ),
            (
                "authentication",
                LOCAL_SCTP_AUTHENTICATION_EVENT,
                SCTP_AUTHENTICATION_MIN_LEN,
            ),
            (
                "partial delivery",
                LOCAL_SCTP_PARTIAL_DELIVERY_EVENT,
                SCTP_PARTIAL_DELIVERY_MIN_LEN,
            ),
            (
                "sender dry",
                LOCAL_SCTP_SENDER_DRY_EVENT,
                SCTP_SENDER_DRY_MIN_LEN,
            ),
            (
                "stream reset",
                LOCAL_SCTP_STREAM_RESET_EVENT,
                SCTP_STREAM_RESET_MIN_LEN,
            ),
            (
                "association reset",
                LOCAL_SCTP_ASSOC_RESET_EVENT,
                SCTP_ASSOC_RESET_MIN_LEN,
            ),
            (
                "stream change",
                LOCAL_SCTP_STREAM_CHANGE_EVENT,
                SCTP_STREAM_CHANGE_MIN_LEN,
            ),
            (
                "modern send failed",
                LOCAL_SCTP_SEND_FAILED_EVENT,
                SCTP_SEND_FAILED_EVENT_MIN_LEN,
            ),
        ];
        assert_eq!(layouts.len(), 13, "every supported arm needs one boundary");

        for (name, notification_type, min_len) in layouts {
            let actual_short = test_notification_buffer(notification_type, min_len - 1);
            let err = parse_notification(&actual_short)
                .expect_err("a one-byte-short backing buffer must be rejected");
            assert_short_sctp_notification(
                &err,
                notification_type as u16,
                (min_len - 1) as u32,
                min_len,
                &format!("{name} backing"),
            );

            let mut declared_short = test_notification_buffer(notification_type, min_len);
            write_u32_ne(
                &mut declared_short,
                SCTP_NOTIFICATION_LENGTH_OFFSET,
                (min_len - 1) as u32,
            );
            let err = parse_notification(&declared_short)
                .expect_err("trailing backing bytes cannot extend the declared record");
            assert_short_sctp_notification(
                &err,
                notification_type as u16,
                (min_len - 1) as u32,
                min_len,
                &format!("{name} declared"),
            );
        }
    }

    #[test]
    fn notification_common_header_bounds_and_unknown_layout_are_exact() {
        let err = parse_notification(&[0u8; SCTP_NOTIFICATION_HEADER_LEN - 1])
            .expect_err("a short common header must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let unknown_type = 0x800e;
        let mut declared_short =
            test_notification_buffer(unknown_type, SCTP_NOTIFICATION_HEADER_LEN);
        write_u32_ne(
            &mut declared_short,
            SCTP_NOTIFICATION_LENGTH_OFFSET,
            (SCTP_NOTIFICATION_HEADER_LEN - 1) as u32,
        );
        assert_eq!(
            parse_notification(&declared_short)
                .expect_err("a declared short header must be rejected")
                .kind(),
            io::ErrorKind::InvalidData
        );

        let mut declared_long =
            test_notification_buffer(unknown_type, SCTP_NOTIFICATION_HEADER_LEN);
        write_u32_ne(
            &mut declared_long,
            SCTP_NOTIFICATION_LENGTH_OFFSET,
            (SCTP_NOTIFICATION_HEADER_LEN + 1) as u32,
        );
        assert_eq!(
            parse_notification(&declared_long)
                .expect_err("a declared length beyond backing storage must be rejected")
                .kind(),
            io::ErrorKind::InvalidData
        );

        let mut unknown = test_notification_buffer(unknown_type, SCTP_NOTIFICATION_HEADER_LEN);
        write_u16_ne(&mut unknown, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x1234);
        assert_eq!(
            parse_notification(&unknown).expect("an unknown exact header should remain visible"),
            SctpRecvMeta::Notification(SctpNotification::Other {
                kind: unknown_type as u16,
                flags: 0x1234,
                length: SCTP_NOTIFICATION_HEADER_LEN as u32,
            })
        );
    }

    #[test]
    fn notification_extensions_remain_bounded_and_unmaterialized() {
        let mut authentication = test_notification_buffer(
            LOCAL_SCTP_AUTHENTICATION_EVENT,
            SCTP_AUTHENTICATION_MIN_LEN + 4,
        );
        write_u16_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_KEY_NUMBER_OFFSET,
            0x1122,
        );
        write_u16_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_ALTERNATE_KEY_NUMBER_OFFSET,
            0x3344,
        );
        write_u32_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_INDICATION_OFFSET,
            0x5566_7788,
        );
        write_i32_ne(
            &mut authentication,
            SCTP_AUTHENTICATION_ASSOC_ID_OFFSET,
            0x1020_3040,
        );
        authentication[SCTP_AUTHENTICATION_MIN_LEN..].fill(0xa5);
        let expected_authentication =
            SctpRecvMeta::Notification(SctpNotification::Authentication {
                flags: 0,
                key_number: 0x1122,
                alternate_key_number: 0x3344,
                indication: 0x5566_7788,
                assoc_id: 0x1020_3040,
            });
        assert_eq!(
            parse_notification(&authentication)
                .expect("declared authentication extensions must remain forward-compatible"),
            expected_authentication
        );
        write_u32_ne(
            &mut authentication,
            SCTP_NOTIFICATION_LENGTH_OFFSET,
            SCTP_AUTHENTICATION_MIN_LEN as u32,
        );
        assert_eq!(
            parse_notification(&authentication)
                .expect("trailing backing bytes must not alter authentication decoding"),
            expected_authentication
        );

        let mut stream_reset =
            test_notification_buffer(LOCAL_SCTP_STREAM_RESET_EVENT, SCTP_STREAM_RESET_MIN_LEN + 4);
        write_u16_ne(&mut stream_reset, SCTP_NOTIFICATION_FLAGS_OFFSET, 0x1234);
        write_i32_ne(&mut stream_reset, SCTP_STREAM_RESET_ASSOC_ID_OFFSET, -64);
        write_u16_ne(&mut stream_reset, SCTP_STREAM_RESET_MIN_LEN, 0x1122);
        write_u16_ne(&mut stream_reset, SCTP_STREAM_RESET_MIN_LEN + 2, 0x3344);
        assert_eq!(
            parse_notification(&stream_reset)
                .expect("declared stream-ID tails are accepted without materialization"),
            SctpRecvMeta::Notification(SctpNotification::StreamReset {
                flags: 0x1234,
                assoc_id: -64,
            })
        );
    }

    #[test]
    fn notification_parser_bounds_fields_to_declared_length() {
        let mut shutdown = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut shutdown, 4, 8);
        let err = parse_notification(&shutdown)
            .expect_err("trailing bytes outside sn_length must not supply known fields");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let mut abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        write_u32_ne(&mut abort, 4, 8);
        let parsed_notification = parse_sctp_notification_once(&abort, libc::MSG_NOTIFICATION);
        assert!(matches!(
            parsed_notification,
            Some(Err(ref err)) if err.kind() == io::ErrorKind::InvalidData
        ));
        assert!(
            !sctp_notification_retires_discard(parsed_notification.as_ref()),
            "bytes outside sn_length must not fabricate a PDAPI abort"
        );
    }

    #[test]
    fn preparsed_notification_drives_discard_policy_and_metadata_result() {
        let mut data = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut data, 8, 42);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);
        let mut visible = SctpRecvState::external();

        let parsed_notification = parse_sctp_notification_once(&data, msg.msg_flags)
            .expect("caller-visible notification should be parsed");
        assert!(matches!(
            process_visible_completion_for_test(&mut visible, &data, &msg),
            SctpMetadataCompletion::Publish(Ok(SctpRecvMeta::Notification(
                SctpNotification::Shutdown { assoc_id: 42 }
            )))
        ));

        // A deliberately different, malformed slice proves final metadata
        // consumes the already-parsed value instead of decoding the bytes a
        // second time.
        assert!(matches!(
            parse_recv_meta_with_notification_for_test(
                &[],
                0,
                msg.msg_flags,
                &[0],
                Some(parsed_notification)
            ),
            Ok(SctpRecvMeta::Notification(SctpNotification::Shutdown {
                assoc_id: 42
            }))
        ));

        let malformed = [0u8];
        let parsed_notification = parse_sctp_notification_once(&malformed, msg.msg_flags)
            .expect("caller-visible malformed notification should be parsed once");
        assert!(parsed_notification.is_err());
        assert!(matches!(
            process_visible_completion_for_test(&mut visible, &malformed, &msg),
            SctpMetadataCompletion::Publish(Err(_))
        ));
        assert_eq!(
            parse_recv_meta_with_notification_for_test(
                &[],
                0,
                msg.msg_flags,
                &data,
                Some(parsed_notification)
            )
            .expect_err("the pre-parsed malformed result must be preserved")
            .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn notification_classification_requires_explicit_preparse() {
        let mut notification = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut notification, 8, 42);
        let msg_flags = libc::MSG_NOTIFICATION | libc::MSG_EOR;
        let expected = SctpRecvMeta::Notification(SctpNotification::Shutdown { assoc_id: 42 });

        let missing =
            parse_recv_meta_with_notification_for_test(&[], 0, msg_flags, &notification, None)
                .expect_err("valid-looking bytes must not replace completion-boundary preparsing");
        assert_eq!(missing.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            missing.to_string(),
            "SCTP notification completion was not preparsed"
        );

        assert_eq!(
            parse_recv_meta(&[], 0, msg_flags, &notification, false)
                .expect("production facade should explicitly preparse the notification"),
            expected
        );
        assert_eq!(
            parse_recv_meta_bare(&[], 0, msg_flags, &notification, false)
                .expect("bare facade should explicitly preparse the notification"),
            expected
        );
    }

    #[test]
    fn preparsed_notification_preserves_framing_and_visibility_precedence() {
        let mut shutdown = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut shutdown, 8, 7);

        let truncated =
            test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_TRUNC | libc::MSG_EOR);
        let mut visible = SctpRecvState::external();
        let parsed_notification = parse_sctp_notification_once(&shutdown, truncated.msg_flags)
            .expect("visible notification should be parsed");
        assert!(matches!(
            process_visible_completion_for_test(&mut visible, &shutdown, &truncated),
            SctpMetadataCompletion::Publish(Err(_))
        ));
        assert_eq!(
            parse_recv_meta_with_notification_for_test(
                &[],
                0,
                truncated.msg_flags,
                &shutdown,
                Some(parsed_notification)
            )
            .expect_err("payload truncation must precede the cached notification")
            .to_string(),
            "SCTP recvmsg payload was truncated"
        );

        let partial = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let malformed = [0u8];
        let parsed_notification = parse_sctp_notification_once(&malformed, partial.msg_flags)
            .expect("visible malformed notification should be parsed");
        assert!(parsed_notification.is_err());
        assert!(matches!(
            process_visible_completion_for_test(&mut visible, &malformed, &partial),
            SctpMetadataCompletion::Publish(Err(_))
        ));
        assert_eq!(
            parse_recv_meta_with_notification_for_test(
                &[],
                0,
                partial.msg_flags,
                &malformed,
                Some(parsed_notification)
            )
            .expect_err("missing EOR must precede the cached parser error")
            .to_string(),
            "SCTP recvmsg payload was partial before end-of-record"
        );

        let abort = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;

        let hidden_fragment = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_TRUNC);
        let mut hidden = SctpRecvState::configured(metadata_only);
        assert!(hidden.should_consume_for_test(&malformed, &hidden_fragment));
        assert_eq!(hidden.record_sync, SctpRecordSync::NotificationTail);
        let hidden_eor =
            test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_TRUNC | libc::MSG_EOR);
        assert!(hidden.should_consume_for_test(&malformed, &hidden_eor));
        assert_eq!(hidden.record_sync, SctpRecordSync::Synced);

        let abort_truncated =
            test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_TRUNC | libc::MSG_EOR);
        let mut other_visible = SctpRecvState::configured(SctpSocketConfig {
            recv_rcvinfo: true,
            notifications: SctpNotificationMask {
                association: true,
                ..SctpNotificationMask::none()
            },
            ..SctpSocketConfig::data(SctpInitConfig::default())
        });
        other_visible.record_sync = SctpRecordSync::DataTail;
        assert!(other_visible.should_consume_for_test(&abort, &abort_truncated));
        assert_eq!(other_visible.record_sync, SctpRecordSync::Synced);

        metadata_only.notifications.partial_delivery = true;
        let mut explicit = SctpRecvState::configured(metadata_only);
        explicit.record_sync = SctpRecordSync::DataTail;
        let explicit_action =
            process_visible_completion_for_test(&mut explicit, &abort, &abort_truncated);
        assert!(matches!(
            explicit_action,
            SctpMetadataCompletion::Publish(Err(_))
        ));
        assert_eq!(explicit.record_sync, SctpRecordSync::Synced);
        let parsed_notification = parse_sctp_notification_once(&abort, abort_truncated.msg_flags)
            .expect("explicit PDAPI should be parsed");
        assert_eq!(
            parse_recv_meta_with_notification_for_test(
                &[],
                0,
                abort_truncated.msg_flags,
                &abort,
                Some(parsed_notification)
            )
            .expect_err("visible PDAPI still obeys payload-truncation precedence")
            .kind(),
            io::ErrorKind::InvalidData
        );

        visible.record_sync = SctpRecordSync::DataTail;
        assert!(matches!(
            process_visible_completion_for_test(&mut visible, &malformed, &partial),
            SctpMetadataCompletion::Consume
        ));
        assert_eq!(visible.record_sync, SctpRecordSync::DataNotificationTail);
    }

    #[test]
    fn forced_partial_delivery_abort_is_internal_only_for_metadata_policy() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;
        let mut forced = SctpRecvState::configured(metadata_only);
        assert!(
            forced.should_consume_for_test(&data, &msg),
            "FlowIO-only forced notifications remain internal"
        );

        forced.record_sync = SctpRecordSync::DataTail;
        assert!(forced.should_consume_for_test(&data, &msg));
        assert_eq!(forced.record_sync, SctpRecordSync::Synced);

        metadata_only.notifications.partial_delivery = true;
        let mut explicit = SctpRecvState::configured(metadata_only);
        assert!(matches!(
            process_visible_completion_for_test(&mut explicit, &data, &msg),
            SctpMetadataCompletion::Publish(Ok(SctpRecvMeta::Notification(
                SctpNotification::PartialDelivery {
                    indication: SCTP_PARTIAL_DELIVERY_ABORTED,
                    ..
                }
            )))
        ));

        explicit.record_sync = SctpRecordSync::DataTail;
        assert!(!explicit.should_consume_for_test(&data, &msg));
        assert_eq!(explicit.record_sync, SctpRecordSync::Synced);

        explicit.set_receive_policy(SctpNotificationMask::none(), true);
        assert!(explicit.should_consume_for_test(&data, &msg));

        let mut external = SctpRecvState::external();
        assert!(!external.should_consume_for_test(&data, &msg));
        let fragment = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        assert!(!external.should_consume_for_test(&data[..1], &fragment));

        let mut other_visible = SctpRecvState::configured(SctpSocketConfig {
            recv_rcvinfo: true,
            notifications: SctpNotificationMask {
                association: true,
                ..SctpNotificationMask::none()
            },
            ..SctpSocketConfig::data(SctpInitConfig::default())
        });
        assert!(!other_visible.should_consume_for_test(&data[..1], &fragment));

        let mut fragmented_forced = SctpRecvState::configured(SctpSocketConfig {
            recv_rcvinfo: true,
            ..SctpSocketConfig::data(SctpInitConfig::default())
        });
        assert!(fragmented_forced.should_consume_for_test(&data[..1], &fragment));
        assert_eq!(
            fragmented_forced.record_sync,
            SctpRecordSync::NotificationTail
        );
        assert!(fragmented_forced.should_consume_for_test(&data[1..2], &msg));
        assert_eq!(fragmented_forced.record_sync, SctpRecordSync::Synced);
        let intact = test_msghdr_with_flags(libc::MSG_EOR);
        assert!(!fragmented_forced.should_consume_for_test(b"next", &intact));

        let mut unpublished = SctpRecvState::external();
        unpublished.process_unpublished_for_test(&data, &msg);
        assert_eq!(unpublished.record_sync, SctpRecordSync::Synced);

        let eor = test_msghdr_with_flags(libc::MSG_EOR);
        unpublished.process_unpublished_for_test(b"next", &eor);
        assert_eq!(unpublished.record_sync, SctpRecordSync::Synced);

        let eof = test_msghdr_with_flags(0);
        unpublished.process_unpublished_for_test(&[], &eof);
        assert_eq!(unpublished.record_sync, SctpRecordSync::Synced);
    }

    #[test]
    fn sctp_non_abort_notification_without_eor_keeps_discard_active() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED + 1);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(!notification_retires_discard_for_test(&data, msg.msg_flags));
        let mut state = SctpRecvState::external();
        state.record_sync = SctpRecordSync::DataTail;
        state.process_unpublished_for_test(&data, &msg);
        assert_eq!(state.record_sync, SctpRecordSync::DataNotificationTail);
        assert_ne!(state.nested_prefix_state & SCTP_NESTED_PREFIX_CLASSIFIED, 0);
    }

    #[test]
    fn sctp_dropped_partial_notification_starts_discard() {
        let data = test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 8);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(sctp_msg_partial_nonempty(data.len(), msg.msg_flags));
        let mut state = SctpRecvState::external();
        state.process_unpublished_for_test(&data, &msg);
        assert_eq!(state.record_sync, SctpRecordSync::NotificationTail);
    }

    #[test]
    fn sctp_notification_eor_tail_retires_discard() {
        let data = test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 20);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

        assert!(!notification_retires_discard_for_test(&data, msg.msg_flags));
        let mut data_tail = SctpRecvState::external();
        data_tail.record_sync = SctpRecordSync::DataTail;
        data_tail.process_unpublished_for_test(&data, &msg);
        assert_eq!(
            data_tail.record_sync,
            SctpRecordSync::DataTail,
            "an unrelated notification EOR cannot retire a data-record tail"
        );

        let mut notification_tail = SctpRecvState::external();
        notification_tail.record_sync = SctpRecordSync::NotificationTail;
        notification_tail.process_unpublished_for_test(&data, &msg);
        assert_eq!(notification_tail.record_sync, SctpRecordSync::Synced);
    }

    fn assoc_ipv4_entry(ip: [u8; 4], port: u16, len: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        write_u16_ne(&mut bytes, 0, libc::AF_INET as u16);
        write_u16_be(&mut bytes, 2, port);
        bytes[4..8].copy_from_slice(&ip);
        bytes
    }

    fn assoc_ipv6_entry(
        ip: [u8; 16],
        port: u16,
        flowinfo: u32,
        scope_id: u32,
        len: usize,
    ) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        write_u16_ne(&mut bytes, 0, libc::AF_INET6 as u16);
        write_u16_be(&mut bytes, 2, port);
        write_u32_ne(&mut bytes, 4, flowinfo);
        bytes[8..24].copy_from_slice(&ip);
        write_u32_ne(&mut bytes, 24, scope_id);
        bytes
    }

    fn assoc_addrs_test_capacity(buffer: &[u8]) -> usize {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        (buffer.len() - header_len) / storage_len
    }

    fn assoc_addrs_test_header(buffer: &[u8]) -> SctpGetAddrsHeader {
        unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpGetAddrsHeader) }
    }

    fn write_assoc_ipv4_test_response(
        buffer: &mut [u8],
        assoc_id: libc::sctp_assoc_t,
        addr_count: usize,
        ip: [u8; 4],
        port: u16,
    ) -> usize {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let entry = assoc_ipv4_entry(ip, port, std::mem::size_of::<libc::sockaddr_in>());
        let payload_len = addr_count * entry.len();
        let response_len = header_len + payload_len;
        assert!(response_len <= buffer.len());

        let header = SctpGetAddrsHeader {
            assoc_id,
            addr_num: addr_count as u32,
        };
        unsafe {
            std::ptr::write_unaligned(buffer.as_mut_ptr() as *mut SctpGetAddrsHeader, header);
        }
        for destination in buffer[header_len..response_len].chunks_exact_mut(entry.len()) {
            destination.copy_from_slice(&entry);
        }
        response_len
    }

    #[test]
    fn assoc_addrs_retries_enomem_and_parses_packed_success() {
        const ASSOC_ID: libc::sctp_assoc_t = 37;
        const ADDR_COUNT: usize = MAX_SCTP_ASSOC_ADDR_CAPACITY + 1;
        const IP: [u8; 4] = [192, 0, 2, 7];
        const PORT: u16 = 3868;

        let mut capacities = Vec::new();
        let addrs = get_assoc_addrs_with(SCTP_GET_PEER_ADDRS_OPT, ASSOC_ID, |buffer| {
            assert_eq!(assoc_addrs_test_header(buffer).assoc_id, ASSOC_ID);
            capacities.push(assoc_addrs_test_capacity(buffer));
            let required_len = std::mem::size_of::<SctpGetAddrsHeader>()
                + ADDR_COUNT * std::mem::size_of::<libc::sockaddr_in>();
            if required_len > buffer.len() {
                return Err(io::Error::from_raw_os_error(libc::ENOMEM));
            }
            Ok(write_assoc_ipv4_test_response(
                buffer, ASSOC_ID, ADDR_COUNT, IP, PORT,
            ))
        })
        .expect("ENOMEM retry should return the packed address snapshot");

        assert_eq!(capacities, [8, 16, 32, 64, 128, 256]);
        assert_eq!(
            addrs,
            vec![SocketAddr::from((Ipv4Addr::from(IP), PORT)); ADDR_COUNT]
        );
    }

    #[test]
    fn assoc_addrs_does_not_retry_non_enomem() {
        const ASSOC_ID: libc::sctp_assoc_t = 41;
        let mut calls = 0;
        let err = get_assoc_addrs_with(SCTP_GET_PEER_ADDRS_OPT, ASSOC_ID, |buffer| {
            calls += 1;
            assert_eq!(assoc_addrs_test_header(buffer).assoc_id, ASSOC_ID);
            Err(io::Error::from_raw_os_error(libc::EIO))
        })
        .expect_err("non-ENOMEM query failure should be returned");

        assert_eq!(calls, 1);
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn assoc_addrs_enomem_retry_ladder_is_bounded() {
        const ASSOC_ID: libc::sctp_assoc_t = 43;
        let mut capacities = Vec::new();
        let err = get_assoc_addrs_with(SCTP_GET_LOCAL_ADDRS_OPT, ASSOC_ID, |buffer| {
            assert_eq!(assoc_addrs_test_header(buffer).assoc_id, ASSOC_ID);
            capacities.push(assoc_addrs_test_capacity(buffer));
            Err(io::Error::from_raw_os_error(libc::ENOMEM))
        })
        .expect_err("perpetual ENOMEM should exhaust the bounded retry ladder");

        assert_eq!(capacities, [8, 16, 32, 64, 128, 256, 512, 1024]);
        assert_eq!(err.raw_os_error(), Some(libc::ENOMEM));
    }

    #[test]
    fn assoc_addrs_wrapper_preserves_parser_buffer_range_error() {
        const ASSOC_ID: libc::sctp_assoc_t = 47;
        const DECLARED_ADDR_COUNT: u32 = 2;

        let mut calls = 0;
        let err = get_assoc_addrs_with(SCTP_GET_PEER_ADDRS_OPT, ASSOC_ID, |buffer| {
            calls += 1;
            let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
            let entry = assoc_ipv4_entry([192, 0, 2, 1], 3868, 8);
            let response_len = header_len + 2 * entry.len();
            assert!(response_len <= buffer.len());

            let header = SctpGetAddrsHeader {
                assoc_id: ASSOC_ID,
                addr_num: DECLARED_ADDR_COUNT,
            };
            unsafe {
                std::ptr::write_unaligned(buffer.as_mut_ptr() as *mut SctpGetAddrsHeader, header);
            }
            buffer[header_len..header_len + entry.len()].copy_from_slice(&entry);
            buffer[header_len + entry.len()..response_len].copy_from_slice(&entry);
            Ok(response_len)
        })
        .expect_err("two compact addresses must reach the parser and fail");

        assert_eq!(calls, 1);
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.get_ref()
                .and_then(|error| error.downcast_ref::<BufferRangeError>()),
            Some(&BufferRangeError {
                offset: 0,
                width: std::mem::size_of::<libc::sa_family_t>(),
                len: 0,
            })
        );
    }

    #[test]
    fn assoc_addrs_buffer_len_accepts_bounded_capacity() {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        assert_eq!(
            assoc_addrs_buffer_len(8, header_len, storage_len).expect("bounded len failed"),
            header_len + 8 * storage_len
        );
    }

    #[test]
    fn assoc_addrs_payload_end_normalizes_linux_option_lengths() {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let ipv4_len = std::mem::size_of::<libc::sockaddr_in>();
        let ipv6_len = std::mem::size_of::<libc::sockaddr_in6>();
        let buffer_len = header_len + ipv4_len + ipv6_len;

        for (returned_len, expected_end) in [
            (0, header_len),
            (ipv4_len, header_len + ipv4_len),
            (ipv6_len, header_len + ipv6_len),
            (ipv4_len + ipv6_len, buffer_len),
        ] {
            assert_eq!(
                assoc_addrs_payload_end(
                    SCTP_GET_LOCAL_ADDRS_OPT,
                    returned_len,
                    header_len,
                    buffer_len,
                )
                .expect("valid local-address length should normalize"),
                expected_end
            );
        }

        for returned_len in [header_len, header_len + ipv4_len, header_len + ipv6_len] {
            assert_eq!(
                assoc_addrs_payload_end(
                    SCTP_GET_PEER_ADDRS_OPT,
                    returned_len,
                    header_len,
                    buffer_len,
                )
                .expect("valid peer-address length should normalize"),
                returned_len
            );
        }

        let ip = [
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0xaa, 0xbb, 0xcc, 0xdd,
        ];
        let entry = assoc_ipv6_entry(ip, 0x1234, 0x0102_0304, 0x0506_0708, ipv6_len);
        let mut local_buffer = vec![0u8; header_len];
        local_buffer.extend_from_slice(&entry);
        let payload_end = assoc_addrs_payload_end(
            SCTP_GET_LOCAL_ADDRS_OPT,
            entry.len(),
            header_len,
            local_buffer.len(),
        )
        .expect("complete local IPv6 payload should normalize");
        let parsed = parse_assoc_addrs(&local_buffer[header_len..payload_end], 1)
            .expect("complete local IPv6 payload should parse");

        assert_eq!(
            parsed,
            vec![SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(ip),
                0x1234,
                0x0102_0304,
                0x0506_0708,
            ))]
        );
    }

    #[test]
    fn assoc_addrs_payload_end_rejects_invalid_framing() {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let buffer_len = header_len + std::mem::size_of::<libc::sockaddr_in6>();
        let invalid_cases = [
            (SCTP_GET_PEER_ADDRS_OPT, header_len - 1, "short peer length"),
            (
                SCTP_GET_LOCAL_ADDRS_OPT,
                buffer_len - header_len + 1,
                "oversized local length",
            ),
            (
                SCTP_GET_PEER_ADDRS_OPT,
                buffer_len + 1,
                "oversized peer length",
            ),
            (
                SCTP_GET_LOCAL_ADDRS_OPT,
                usize::MAX,
                "overflowing local length",
            ),
            (-1, header_len, "unknown option"),
        ];

        for (optname, returned_len, label) in invalid_cases {
            let err = assoc_addrs_payload_end(optname, returned_len, header_len, buffer_len)
                .expect_err(label);
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{label}");
        }
    }

    #[test]
    fn assoc_addr_initial_capacity_is_bounded_by_payload() {
        assert_eq!(assoc_addrs_initial_capacity(0, 0), 0);
        assert_eq!(assoc_addrs_initial_capacity(usize::MAX, 0), 0);
        assert_eq!(
            assoc_addrs_initial_capacity(usize::MAX, MIN_SCTP_ASSOC_ADDR_LEN - 1),
            0
        );
        assert_eq!(
            assoc_addrs_initial_capacity(usize::MAX, MIN_SCTP_ASSOC_ADDR_LEN),
            1
        );

        let max_payload =
            MAX_SCTP_ASSOC_ADDR_CAPACITY * std::mem::size_of::<libc::sockaddr_storage>();
        let packed_ipv4_max = max_payload / MIN_SCTP_ASSOC_ADDR_LEN;
        assert!(packed_ipv4_max > MAX_SCTP_ASSOC_ADDR_CAPACITY);
        assert_eq!(
            assoc_addrs_initial_capacity(packed_ipv4_max, max_payload),
            packed_ipv4_max
        );
        assert_eq!(
            assoc_addrs_initial_capacity(usize::MAX, max_payload - 1),
            packed_ipv4_max - 1
        );
    }

    #[test]
    fn assoc_addrs_buffer_len_rejects_over_cap_and_overflow() {
        let over_cap = assoc_addrs_buffer_len(
            MAX_SCTP_ASSOC_ADDR_CAPACITY + 1,
            std::mem::size_of::<SctpGetAddrsHeader>(),
            std::mem::size_of::<libc::sockaddr_storage>(),
        )
        .expect_err("over-cap buffer should fail");
        assert_eq!(over_cap.kind(), io::ErrorKind::InvalidData);

        let overflow = assoc_addrs_buffer_len(
            MAX_SCTP_ASSOC_ADDR_CAPACITY,
            1,
            usize::MAX / MAX_SCTP_ASSOC_ADDR_CAPACITY + 1,
        )
        .expect_err("overflowing buffer should fail");
        assert_eq!(overflow.kind(), io::ErrorKind::InvalidData);

        let over_socklen = assoc_addrs_buffer_len(1, libc::socklen_t::MAX as usize, 1)
            .expect_err("socklen_t overflow should fail");
        assert_eq!(over_socklen.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn notification_mask_uses_modern_send_failure_without_legacy_data_io() {
        let subscribed = SctpEventSubscribe::from_mask(SctpNotificationMask::all());
        assert_eq!(subscribed.sctp_data_io_event, 0);
        assert_eq!(subscribed.sctp_send_failure_event, 0);
        assert_eq!(subscribed.sctp_send_failure_event_event, 1);

        let unsubscribed = SctpEventSubscribe::from_mask(SctpNotificationMask::none());
        assert_eq!(unsubscribed.sctp_data_io_event, 0);
        assert_eq!(unsubscribed.sctp_send_failure_event, 0);
        assert_eq!(unsubscribed.sctp_send_failure_event_event, 0);
    }

    #[test]
    fn metadata_receive_forces_only_partial_delivery_subscription() {
        let data = SctpSocketConfig::data(SctpInitConfig::default());
        let data_options = data.socket_options();
        assert!(!data_options.recv_rcvinfo);
        assert_eq!(data_options.notifications, SctpNotificationMask::none());

        let mut metadata_only = data;
        metadata_only.recv_rcvinfo = true;
        let metadata_options = metadata_only.socket_options();
        assert!(metadata_options.recv_rcvinfo);
        assert_eq!(
            metadata_options.notifications,
            SctpNotificationMask {
                partial_delivery: true,
                ..SctpNotificationMask::none()
            }
        );

        assert_eq!(
            effective_sctp_notification_mask(SctpNotificationMask::none(), true),
            metadata_options.notifications
        );
        assert_eq!(
            effective_sctp_notification_mask(SctpNotificationMask::none(), false),
            SctpNotificationMask::none()
        );
    }

    #[test]
    fn paddr_params_modern_sockopt_round_trips_extended_fields() {
        let params = SctpPeerAddrParams {
            assoc_id: 17,
            heartbeat_interval_ms: 30_000,
            path_max_retransmits: 4,
            path_mtu: 1400,
            sack_delay_ms: 200,
            flags: SctpPeerAddrParams::HEARTBEAT_ENABLE
                | SctpPeerAddrParams::IPV6_FLOWLABEL
                | SctpPeerAddrParams::DSCP,
            ipv6_flow_label: 0xabcde,
            dscp: 46,
            ..SctpPeerAddrParams::for_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)))
        };
        let fields = SctpPaddrParamsFields::from_public(params);
        assert!(fields.requires_modern_sockopt());

        let raw = SctpPaddrParamsRaw::from_fields(fields);
        let bytes = paddr_params_bytes::<_, SCTP_PADDR_PARAMS_RAW_OPT_LEN>(&raw);
        let decoded = decode_peer_addr_params_sockopt(&bytes, SCTP_PADDR_PARAMS_RAW_OPT_LEN)
            .expect("modern paddr params should decode");

        assert_eq!(decoded, params);
    }

    #[test]
    fn paddr_params_legacy_sockopt_round_trips_common_fields() {
        let params = SctpPeerAddrParams {
            assoc_id: 23,
            heartbeat_interval_ms: 15_000,
            path_max_retransmits: 3,
            path_mtu: 1200,
            sack_delay_ms: 120,
            flags: SctpPeerAddrParams::HEARTBEAT_ENABLE | SctpPeerAddrParams::PMTUD_ENABLE,
            ..SctpPeerAddrParams::for_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 3869)))
        };
        let fields = SctpPaddrParamsFields::from_public(params);
        assert!(!fields.requires_modern_sockopt());

        let raw = SctpPaddrParamsRawLegacy::from_fields(fields);
        let legacy_bytes = paddr_params_bytes::<_, SCTP_PADDR_PARAMS_LEGACY_OPT_LEN>(&raw);
        let mut bytes = [0u8; SCTP_PADDR_PARAMS_RAW_OPT_LEN];
        bytes[..legacy_bytes.len()].copy_from_slice(&legacy_bytes);
        let decoded = decode_peer_addr_params_sockopt(&bytes, SCTP_PADDR_PARAMS_LEGACY_OPT_LEN)
            .expect("legacy paddr params should decode");

        assert_eq!(decoded, params);
        assert_eq!(decoded.ipv6_flow_label, 0);
        assert_eq!(decoded.dscp, 0);
    }

    #[test]
    fn paddr_params_sockopt_accepts_only_exact_rounded_lengths() {
        let buffer = [0u8; SCTP_PADDR_PARAMS_RAW_OPT_LEN];

        for optlen in (0..=157).chain(std::iter::once(usize::MAX)) {
            let result = decode_peer_addr_params_sockopt(&buffer, optlen);
            if matches!(
                optlen,
                SCTP_PADDR_PARAMS_LEGACY_OPT_LEN | SCTP_PADDR_PARAMS_RAW_OPT_LEN
            ) {
                result.expect("exact supported peer-parameter length should decode");
                continue;
            }

            let err = result.expect_err("unsupported peer-parameter length should fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                err.to_string(),
                format!(
                    "unexpected SCTP_PEER_ADDR_PARAMS length {} (expected {} or {})",
                    optlen, SCTP_PADDR_PARAMS_RAW_OPT_LEN, SCTP_PADDR_PARAMS_LEGACY_OPT_LEN
                )
            );
        }
    }

    #[test]
    fn paddr_params_extended_fields_force_modern_sockopt() {
        let common = SctpPeerAddrParams {
            flags: SctpPeerAddrParams::HEARTBEAT_ENABLE,
            ..SctpPeerAddrParams::association_default()
        };
        assert!(!SctpPaddrParamsFields::from_public(common).requires_modern_sockopt());

        let by_flow_label = SctpPeerAddrParams {
            ipv6_flow_label: 1,
            ..common
        };
        assert!(SctpPaddrParamsFields::from_public(by_flow_label).requires_modern_sockopt());

        let by_dscp = SctpPeerAddrParams { dscp: 1, ..common };
        assert!(SctpPaddrParamsFields::from_public(by_dscp).requires_modern_sockopt());

        let by_flow_flag = SctpPeerAddrParams {
            flags: SctpPeerAddrParams::IPV6_FLOWLABEL,
            ..common
        };
        assert!(SctpPaddrParamsFields::from_public(by_flow_flag).requires_modern_sockopt());

        let by_dscp_flag = SctpPeerAddrParams {
            flags: SctpPeerAddrParams::DSCP,
            ..common
        };
        assert!(SctpPaddrParamsFields::from_public(by_dscp_flag).requires_modern_sockopt());
    }

    #[test]
    fn optional_socket_addr_storage_round_trips_ipv6_metadata() {
        let expected = SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(
                0x2001, 0x0db8, 0x0102, 0x0304, 0x0506, 0x0708, 0x090a, 0x0b0c,
            ),
            0x1234,
            0x0102_0304,
            0x0506_0708,
        ));
        let storage = option_socket_addr_to_storage(Some(expected));

        assert_eq!(
            sockaddr_len_for_storage(storage).expect("IPv6 storage length should decode"),
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
        );
        assert_eq!(
            storage_to_option_socket_addr(storage).expect("IPv6 storage should decode"),
            Some(expected),
        );
        assert_eq!(
            storage_to_option_socket_addr(option_socket_addr_to_storage(None))
                .expect("empty optional storage should decode"),
            None,
        );
    }

    #[test]
    fn parse_assoc_addrs_accepts_empty_zero_count_payload() {
        let parsed = parse_assoc_addrs(&[], 0).expect("zero-count parse should succeed");

        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_assoc_addrs_walks_mixed_kernel_layout_forward() {
        let ipv4_len = std::mem::size_of::<libc::sockaddr_in>();
        let ipv6_len = std::mem::size_of::<libc::sockaddr_in6>();
        let ipv6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let mut payload = assoc_ipv4_entry([1, 2, 3, 4], 1111, ipv4_len);
        payload.extend_from_slice(&assoc_ipv6_entry(ipv6, 2222, 7, 9, ipv6_len));
        payload.extend_from_slice(&assoc_ipv4_entry([5, 6, 7, 8], 3333, ipv4_len));

        let parsed = parse_assoc_addrs(&payload, 3).expect("mixed address parse failed");

        assert_eq!(
            parsed,
            vec![
                SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 1111)),
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(ipv6), 2222, 7, 9)),
                SocketAddr::from((Ipv4Addr::new(5, 6, 7, 8), 3333)),
            ]
        );
    }

    #[test]
    fn parse_assoc_addrs_rejects_non_kernel_compact_and_padded_layouts() {
        let compact_ipv4 = assoc_ipv4_entry([192, 0, 2, 10], 1234, 8);
        let mut two_compact_ipv4 = assoc_ipv4_entry([1, 2, 3, 4], 1111, 8);
        two_compact_ipv4.extend_from_slice(&assoc_ipv4_entry([5, 6, 7, 8], 2222, 8));
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        let ipv6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let padded_ipv4 = assoc_ipv4_entry([192, 0, 2, 10], 1234, storage_len);
        let padded_ipv6 = assoc_ipv6_entry(ipv6, 4321, 7, 9, storage_len);

        let dense_error = parse_assoc_addrs(&two_compact_ipv4, 2)
            .expect_err("two compact IPv4 entries must not satisfy two kernel-sized entries");
        assert_eq!(dense_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            dense_error
                .get_ref()
                .and_then(|error| error.downcast_ref::<BufferRangeError>()),
            Some(&BufferRangeError {
                offset: 0,
                width: std::mem::size_of::<libc::sa_family_t>(),
                len: 0,
            })
        );

        for (payload, label) in [
            (compact_ipv4, "compact IPv4"),
            (padded_ipv4, "storage-padded IPv4"),
            (padded_ipv6, "storage-padded IPv6"),
        ] {
            let err = parse_assoc_addrs(&payload, 1).expect_err(label);
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{label}");
        }
    }

    #[test]
    fn parse_assoc_addrs_rejects_count_payload_mismatch() {
        let payload = assoc_ipv4_entry([10, 0, 0, 1], 80, std::mem::size_of::<libc::sockaddr_in>());

        let missing_second = parse_assoc_addrs(&payload, 2)
            .expect_err("short payload for declared count should fail");
        assert_eq!(missing_second.kind(), io::ErrorKind::InvalidData);

        let extra_payload = parse_assoc_addrs(&payload, 0)
            .expect_err("non-empty payload with zero count should fail");
        assert_eq!(extra_payload.kind(), io::ErrorKind::InvalidData);

        let huge_count = parse_assoc_addrs(&[], usize::MAX)
            .expect_err("a huge count with no payload must fail without a huge allocation");
        assert_eq!(huge_count.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            huge_count
                .get_ref()
                .and_then(|error| error.downcast_ref::<BufferRangeError>()),
            Some(&BufferRangeError {
                offset: 0,
                width: std::mem::size_of::<libc::sa_family_t>(),
                len: 0,
            })
        );
    }

    #[test]
    fn parse_assoc_addrs_rejects_unsupported_family_and_truncation() {
        let mut unsupported = vec![0u8; std::mem::size_of::<libc::sockaddr_in>()];
        write_u16_ne(&mut unsupported, 0, libc::AF_UNIX as u16);
        let err = parse_assoc_addrs(&unsupported, 1).expect_err("unsupported family should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let truncated = parse_assoc_addrs(&[0], 1).expect_err("truncated family should fail");
        assert_eq!(truncated.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_assoc_addrs_rejects_one_byte_short_ipv6_entry() {
        let ipv6_len = std::mem::size_of::<libc::sockaddr_in6>();
        let mut payload = assoc_ipv6_entry(
            Ipv6Addr::LOCALHOST.octets(),
            0x1234,
            0x0102_0304,
            0x0506_0708,
            ipv6_len,
        );
        payload.pop();

        let err =
            parse_assoc_addrs(&payload, 1).expect_err("one-byte-short IPv6 address should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_assoc_addrs_rejects_one_byte_short_ipv4_entry() {
        let ipv4_len = std::mem::size_of::<libc::sockaddr_in>();
        let mut payload = assoc_ipv4_entry([192, 0, 2, 1], 3868, ipv4_len);
        payload.pop();

        let err =
            parse_assoc_addrs(&payload, 1).expect_err("one-byte-short IPv4 address should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_assoc_addrs_handles_count_above_storage_budget_in_one_forward_pass() {
        const ADDR_COUNT: usize = MAX_SCTP_ASSOC_ADDR_CAPACITY + 1;
        let ipv4_len = std::mem::size_of::<libc::sockaddr_in>();
        let mut payload = Vec::with_capacity(ADDR_COUNT * ipv4_len);
        for index in 0..ADDR_COUNT {
            payload.extend_from_slice(&assoc_ipv4_entry(
                [10, 0, (index / 256) as u8, index as u8],
                1000 + (index % 1000) as u16,
                ipv4_len,
            ));
        }

        let parsed = parse_assoc_addrs(&payload, ADDR_COUNT)
            .expect("above-storage-budget count parse failed");

        assert_eq!(parsed.len(), ADDR_COUNT);
        assert_eq!(
            parsed[0],
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 0), 1000))
        );
        assert_eq!(
            parsed[ADDR_COUNT - 1],
            SocketAddr::from((Ipv4Addr::new(10, 0, 4, 0), 1024))
        );
    }
}
