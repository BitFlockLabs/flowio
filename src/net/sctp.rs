//! One-to-one SCTP transport with message-oriented send and receive operations.
//!
//! # Compatibility
//!
//! This implementation targets the Linux SCTP socket API and FlowIO's
//! crate-wide Linux 5.11-or-newer runtime floor.
//!
//! Baseline one-to-one SCTP operations are expected to work on supported Linux
//! kernels where SCTP is enabled:
//! - [`SctpListener::bind`]
//! - [`SctpListener::accept`]
//! - [`SctpConnector::connect`]
//! - [`SctpStream::send_msg`]
//! - [`SctpStream::recv_msg`]
//!
//! FlowIO uses the 14-byte `SCTP_EVENTS` subscription layout available since
//! Linux 5.5. That predates the binding Linux 5.11 runtime floor, so no legacy
//! 13-byte subscription fallback is attempted.
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

use super::{
    AcceptReadinessSlot as AcceptSlot, MsgHdrInit, checked_read_len, checked_send_len, close_fd,
    close_if_valid, complete_read_with_progress, connect_cqe_result, current_local_addr,
    get_sock_opt, invalid_input, set_reuse_addr, set_sock_opt, socket_addr_from_c,
    socket_addr_to_c, socket_domain, write_msghdr,
};
use crate::net::send_sqe::{build_send_entry, build_sendmsg_entry};
use crate::runtime::buffer::bytes::{
    BufferRangeError, read_i32_at, read_u16_at, read_u16_be_at, read_u32_at,
};
use crate::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, UnsubmittedOpGuard, completed_op_ctx, drop_op_ptr_unchecked, poll_ctx_from_waker,
    refresh_op_waiter_from_waker, submit_initialized_retained_sqe, submit_retained_sqe,
    validate_local_io_result,
};
use crate::runtime::fd::{LingerProvenance, RuntimeFd};
use crate::runtime::op::CompletionState;
use crate::runtime::reactor::Reactor;
use crate::runtime::retained::{RetainedPayload, RetainedPayloadPool, with_raw_retained_slot};
use crate::runtime::timer::{Timeout, TimeoutError, timeout};
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
/// [`SctpStream::recv_msg`]. When `SCTP_RECVRCVINFO` is disabled, ancillary
/// fields are left at their defaults while [`SctpRecvInfo::end_of_record`]
/// still reflects the receive flags. It is not produced by the lean data fast
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
    /// `SCTP_RCVINFO` when enabled; otherwise ancillary fields are defaults
    /// and `end_of_record` still reflects the receive flags.
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
    if (std::mem::size_of::<SctpPaddrParamsRaw>()..=SCTP_PADDR_PARAMS_RAW_OPT_LEN).contains(&optlen)
    {
        let raw = unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpPaddrParamsRaw) };
        return raw.to_fields().to_public();
    }

    if (std::mem::size_of::<SctpPaddrParamsRawLegacy>()..=SCTP_PADDR_PARAMS_LEGACY_OPT_LEN)
        .contains(&optlen)
    {
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

/// Adopts one successful accept result and applies post-accept policy.
fn finish_accepted_runtime_stream(
    accepted_fd: RuntimeFd,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    config: SctpSocketConfig,
) -> io::Result<(SctpStream, SocketAddr)> {
    let remote_addr = socket_addr_from_c(addr, addrlen)?;
    apply_sctp_accepted_established_config(
        accepted_fd.raw_fd(),
        config,
        accepted_fd.linger_provenance(),
    )?;
    Ok((
        SctpStream::from_configured_runtime_fd(accepted_fd, remote_addr, config),
        remote_addr,
    ))
}

#[cfg(test)]
fn finish_accepted_stream(
    accepted_fd: OwnedFd,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    config: SctpSocketConfig,
) -> io::Result<(SctpStream, SocketAddr)> {
    finish_accepted_runtime_stream(
        RuntimeFd::from_fresh_owned(accepted_fd),
        addr,
        addrlen,
        config,
    )
}

/// Reusable connect-side submission state kept by [`SctpConnector`].
struct ConnectSlot {
    /// Completion state for the current or last connect submission.
    state_ptr: *mut CompletionState,
    /// True while a [`ConnectFuture`] is borrowing this slot.
    in_use: bool,
    /// Socket being configured and connected for the current attempt.
    fd: RawFd,
    /// Prepared remote address for the current connect attempt.
    addr: Option<RetainedConnectAddr>,
    /// Socket configuration to apply after association establishment.
    connected_config: SctpSocketConfig,
}

impl ConnectSlot {
    fn new() -> Self {
        Self {
            state_ptr: std::ptr::null_mut(),
            in_use: false,
            fd: -1,
            addr: None,
            connected_config: SctpSocketConfig::default(),
        }
    }

    fn prepare(
        &mut self,
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        config: SctpSocketConfig,
    ) -> io::Result<()> {
        if self.in_use || !self.state_ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.cleanup_fd();
        self.connected_config = config;
        self.in_use = true;
        self.fd = match new_sctp_socket(socket_domain(remote_addr), libc::SOCK_STREAM) {
            Ok(fd) => fd,
            Err(err) => {
                self.in_use = false;
                return Err(err);
            }
        };
        if let Err(err) = configure_sctp_socket(self.fd, config) {
            self.cleanup_fd();
            self.in_use = false;
            return Err(err);
        }

        if let Some(local_addr) = local_addr {
            if let Err(err) = set_reuse_addr(self.fd) {
                self.cleanup_fd();
                self.in_use = false;
                return Err(err);
            }
            let (sockaddr, sockaddr_len) = socket_addr_to_c(local_addr);
            let bind_res = unsafe {
                libc::bind(
                    self.fd,
                    &sockaddr as *const _ as *const libc::sockaddr,
                    sockaddr_len,
                )
            };
            if bind_res < 0 {
                let err = io::Error::last_os_error();
                self.cleanup_fd();
                self.in_use = false;
                return Err(err);
            }
        }

        self.addr = Some(RetainedConnectAddr::from_socket_addr(remote_addr));
        Ok(())
    }

    fn cleanup_fd(&mut self) {
        close_if_valid(&mut self.fd);
    }

    fn take_stream(&mut self, remote_addr: SocketAddr) -> SctpStream {
        let fd = self.fd;
        self.fd = -1;
        // SAFETY: this connect slot owns the successfully created socket, and
        // clearing the sentinel above prevents later slot cleanup from closing
        // the transferred descriptor.
        SctpStream::from_configured_runtime_fd(
            RuntimeFd::from_fresh_owned(unsafe { OwnedFd::from_raw_fd(fd) }),
            remote_addr,
            self.connected_config,
        )
    }

    fn drop_future(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.addr = None;
        self.cleanup_fd();
        self.in_use = false;
    }

    fn drop_cached_state(&mut self) {
        self.drop_future();
    }
}

/// Kernel-visible connect address storage retained until the connect CQE retires.
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
    fd: Rc<RuntimeFd>,
    /// Local address bound to the listening socket.
    local_addr: SocketAddr,
    /// Reusable accept state for at most one in-flight accept future.
    accept_slot: AcceptSlot,
    /// Socket and receive-policy configuration retained for accepted streams.
    accepted_config: SctpSocketConfig,
}

impl SctpListener {
    /// Binds a listener, applies init parameters, enables notifications, and
    /// starts listening.
    ///
    /// This is setup/control-plane work performed once before serving; it is
    /// not on the per-message data fast path.
    pub fn bind(addr: SocketAddr, backlog: i32, initmsg: SctpInitConfig) -> io::Result<Self> {
        Self::bind_with_config(addr, backlog, SctpSocketConfig::rich(initmsg))
    }

    /// Binds a listener using the provided SCTP socket configuration.
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

        let fd = Rc::new(RuntimeFd::from_fresh_raw_fd(fd));
        Ok(Self {
            accept_slot: AcceptSlot::new(Rc::clone(&fd)),
            fd,
            local_addr,
            accepted_config: config,
        })
    }

    /// Returns the local address currently assigned to the listener.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Starts accepting one SCTP association.
    ///
    /// Accepting associations is setup/control-plane work, not the per-message
    /// data fast path. The accepted [`SctpStream`] carries the steady-state
    /// data path.
    ///
    /// This returns a future directly for compatibility with existing callers.
    /// A concurrent accept on the same listener is reported as an error when
    /// the returned future is first polled; safe borrowing makes that path
    /// unreachable except through intentionally leaked/forgotten futures.
    ///
    /// # Errors
    ///
    /// The returned future resolves with [`io::ErrorKind::WouldBlock`] if the
    /// listener's reusable accept slot is occupied or runtime operation
    /// capacity cannot accept the submission. It also preserves the kernel's
    /// `WouldBlock` if a terminal readiness condition has no queued
    /// association, ending this accept attempt without rearming.
    ///
    /// Dropping a pending accept cancels only its readiness wait and leaves an
    /// already queued association for the next accept. If the listener's raw
    /// fd is exposed, the caller must not concurrently accept from it or race
    /// changes to its file-status flags.
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
            connect_slot: ConnectSlot::new(),
            config,
            local_addr: None,
        }
    }

    /// Pins the connector to a specific local address before connecting.
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
        self.connect_slot
            .prepare(self.local_addr, remote_addr, self.config)?;
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

type StashedSctpRecvProcessor = unsafe fn(&PollCtx, *mut CompletionState, usize, &mut bool);

/// Dropped metadata receive retained by the stream until its CQE is processed.
struct StashedSctpRecv {
    /// In-flight or completed operation state adopted from the dropped future.
    state_ptr: *mut CompletionState,
    /// Initialized iovec count needed by the vectored completion processor.
    iov_count: usize,
    /// Type-specific function that consumes the retained payload and updates
    /// record-tail discard state.
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
    /// Whether a prior metadata receive consumed the head of an oversized
    /// record and future metadata receives must discard through MSG_EOR.
    discarding_tail: bool,
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
    /// Dropped metadata receive completion that must be adopted before the
    /// next metadata receive can preserve record-boundary state.
    stashed: StashedSctpRecv,
}

impl SctpRecvState {
    /// Creates receive state for an externally configured descriptor.
    ///
    /// FlowIO cannot infer why an external socket subscribed to PDAPI, so it
    /// preserves the historical behavior of surfacing valid notifications.
    const fn external() -> Self {
        Self {
            discarding_tail: false,
            partial_delivery_visible: Cell::new(true),
            any_notification_visible: Cell::new(true),
            stashed: StashedSctpRecv::empty(),
        }
    }

    /// Creates receive state from the caller-requested FlowIO socket policy.
    const fn configured(config: SctpSocketConfig) -> Self {
        Self {
            discarding_tail: false,
            partial_delivery_visible: Cell::new(config.notifications.partial_delivery),
            any_notification_visible: Cell::new(config.notifications.any()),
            stashed: StashedSctpRecv::empty(),
        }
    }

    /// Records a successfully applied caller-requested notification policy.
    fn set_notification_visibility(&self, mask: SctpNotificationMask) {
        self.partial_delivery_visible.set(mask.partial_delivery);
        self.any_notification_visible.set(mask.any());
    }

    /// Updates discard state and returns true when this completion is internal
    /// recovery work rather than caller-visible metadata.
    fn should_consume_metadata_completion(
        &mut self,
        header: SctpRecvHeader,
        parsed_notification: Option<&io::Result<SctpRecvMeta>>,
    ) -> bool {
        let partial_delivery_abort = sctp_notification_retires_discard(parsed_notification);
        if sctp_msg_notification(header.msg_flags) && !self.any_notification_visible.get() {
            self.discarding_tail = sctp_discarding_after_completion(header, partial_delivery_abort);
            return true;
        }

        if self.discarding_tail {
            self.discarding_tail = sctp_discarding_after_completion(header, partial_delivery_abort);
            return !(partial_delivery_abort && self.partial_delivery_visible.get());
        }

        partial_delivery_abort && !self.partial_delivery_visible.get()
    }

    #[cfg(test)]
    fn should_consume_for_test(&mut self, data_slice: &[u8], msg: &libc::msghdr) -> bool {
        let parsed_notification = parse_sctp_notification_once(data_slice, msg.msg_flags);
        self.should_consume_metadata_completion(
            SctpRecvHeader::from_msghdr(msg),
            parsed_notification.as_ref(),
        )
    }

    /// Transfers an in-flight metadata receive from a dropped future into the
    /// stream-owned recovery slot.
    ///
    /// # Safety
    ///
    /// A non-null `*state_ptr` must be a live operation owned by this stream's
    /// reactor, `process_completed` must match its retained payload type, and
    /// no other receive may already be stashed.
    unsafe fn stash(
        &mut self,
        state_ptr: &mut *mut CompletionState,
        iov_count: usize,
        process_completed: StashedSctpRecvProcessor,
    ) {
        if (*state_ptr).is_null() {
            return;
        }
        debug_assert!(
            self.stashed.state_ptr.is_null(),
            "SCTP stream already has a stashed metadata receive"
        );
        unsafe { (**state_ptr).clear_waiter() };
        self.stashed.state_ptr = *state_ptr;
        self.stashed.iov_count = iov_count;
        self.stashed.process_completed = Some(process_completed);
        *state_ptr = std::ptr::null_mut();
    }

    /// Clears any waiter retained by the stream-owned dropped receive.
    ///
    /// # Safety
    ///
    /// A non-null stashed pointer must identify a live completion state that
    /// this receive state exclusively owns.
    unsafe fn clear_stashed_waiter(&mut self) {
        if !self.stashed.state_ptr.is_null() {
            unsafe { (*self.stashed.state_ptr).clear_waiter() };
        }
    }

    /// Polls and consumes the previously dropped metadata receive, if any.
    ///
    /// # Safety
    ///
    /// Any stashed pointer and processor must satisfy [`SctpRecvState::stash`],
    /// and `cx` must carry the FlowIO waker for the owning reactor.
    unsafe fn poll_stashed(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let state_ptr = self.stashed.state_ptr;
        if state_ptr.is_null() {
            return Poll::Ready(Ok(()));
        }

        if unsafe { !(*state_ptr).is_completed() } {
            if unsafe { refresh_op_waiter_from_waker(cx, state_ptr) } {
                unsafe { self.drop_stashed() };
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
            }
            return Poll::Pending;
        }

        let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), state_ptr) };
        let process_completed = self.stashed.process_completed.take();
        debug_assert!(
            process_completed.is_some(),
            "stashed SCTP recv missing completion processor"
        );
        let process_completed = unsafe { process_completed.unwrap_unchecked() };
        let iov_count = self.stashed.iov_count;
        self.stashed.state_ptr = std::ptr::null_mut();
        self.stashed.iov_count = 0;
        unsafe {
            process_completed(
                op_ctx.origin_poll_ctx(),
                state_ptr,
                iov_count,
                &mut self.discarding_tail,
            )
        };
        if op_ctx.context_rejected() {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Orphans/cancels a stashed receive during stream teardown.
    ///
    /// # Safety
    ///
    /// A non-null stashed pointer must identify the exclusively owned live
    /// operation transferred by [`SctpRecvState::stash`].
    unsafe fn drop_stashed(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.stashed.state_ptr) };
        self.stashed.iov_count = 0;
        self.stashed.process_completed = None;
    }
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
/// tail through the next record boundary before resuming delivery.
///
/// A kernel zero-byte completion with no control message and no flags is clean
/// peer EOF and resolves as
/// `Ok((0, SctpRecvMeta::Data(SctpRecvInfo::default())))`. Both methods reject
/// zero-length caller destinations before submission, so such a request cannot
/// masquerade as EOF. When `SCTP_RECVRCVINFO` is disabled, ordinary data with
/// no control message succeeds with default ancillary fields and the kernel's
/// end-of-record flag. `MSG_CTRUNC` without usable `SCTP_RCVINFO`, or
/// present-but-malformed control, remains `InvalidData`; intact receive info
/// remains usable when only extra control was truncated. Kernel receive errors
/// are returned as `io::Error` values from the completed operation.
///
/// # In-flight drop ownership
///
/// Dropping an in-flight receive or send future relinquishes the caller buffer
/// to the runtime until the original kernel completion retires; the buffer is
/// not returned to the caller on that cancellation path. Dropped metadata
/// receives are adopted by the next metadata receive so SCTP record-boundary
/// resynchronization state is updated from the retired completion. While a
/// metadata receive is discarding an oversized record tail, keep using
/// [`SctpStream::recv_msg`] or [`SctpStream::recv_msg_vectored`] until the next
/// record boundary is reached; the data-only [`SctpStream::recv`] path does
/// not participate in that resynchronization state. Notifications observed
/// during internal discard are consumed as control events, except an
/// explicitly requested partial-delivery abort remains caller-visible while
/// retiring discard. An EOR-marked notification tail or a
/// partial-delivery-aborted notification retires discard; other notification
/// fragments keep discard active.
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
    /// This is socket status/control-plane lookup, not the per-message data
    /// fast path.
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
    /// This relies on a Linux SCTP address-enumeration socket option and may
    /// fail on systems with partial SCTP support. It is status/control-plane
    /// work, not the per-message data fast path.
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        get_assoc_addrs(self.fd.raw_fd(), SCTP_GET_LOCAL_ADDRS_OPT, 0)
    }

    /// Returns all peer addresses currently associated with the stream.
    ///
    /// This relies on a Linux SCTP address-enumeration socket option and may
    /// fail on systems with partial SCTP support. It is status/control-plane
    /// work, not the per-message data fast path.
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
    /// the normal truncated-notification error behavior. The visibility policy
    /// changes only after the kernel accepts the new mask.
    ///
    /// This is signaling setup/control-plane work. Data-only fast paths should
    /// use [`SctpSocketConfig::data`] and avoid per-message mask changes.
    pub fn set_notification_mask(&self, mask: SctpNotificationMask) -> io::Result<()> {
        let recv_rcvinfo: libc::c_int =
            get_sock_opt(self.fd.raw_fd(), libc::IPPROTO_SCTP, libc::SCTP_RECVRCVINFO)?;
        let effective = effective_sctp_notification_mask(mask, recv_rcvinfo != 0);
        set_sctp_events(self.fd.raw_fd(), effective)?;
        self.recv_state.set_notification_visibility(mask);
        Ok(())
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
    /// This data-only path does not drive metadata receive resynchronization;
    /// do not mix it with `recv_msg` / `recv_msg_vectored` while those paths
    /// are discarding an oversized record tail.
    ///
    /// # Errors
    /// Returns `InvalidInput` if `len` is zero or exceeds
    /// `buffer.writable_len()`.
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
        DataRecvFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
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
        DataSendFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
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
    /// `buffer.writable_len()`.
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
        RecvFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
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
        SendFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
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
    /// Returns `InvalidInput` if the chain has no writable bytes. Shared
    /// metadata parsing, truncation, EOF, and record-tail recovery behavior is
    /// documented on [`SctpStream`].
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn recv_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
    ) -> RecvVectoredFuture<'_, N> {
        let (iov_count, writable_len) = buffer.read_iovec_count_and_writable_len();
        let input_error = if writable_len == 0 {
            Some(invalid_zero_length_sctp_recv())
        } else {
            None
        };
        RecvVectoredFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iov_count,
            input_error,
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
    /// Returns `InvalidInput` if the chain has no readable bytes or its
    /// aggregate readable byte count cannot be represented by `usize`.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
        info: SctpSendInfo,
    ) -> SendVectoredFuture<'_, N> {
        let input_error = validate_sctp_vectored_send_len(buffer.checked_len()).err();
        SendVectoredFuture {
            fd: self.fd.raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            input_error,
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

const SCTP_RCVINFO_CONTROL_LEN: usize = cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>());

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
    control: [MaybeUninit<u8>; SCTP_RCVINFO_CONTROL_LEN],
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

            // Preserve the existing alloc-op-before-callback ordering. The
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
                    controllen: SCTP_RCVINFO_CONTROL_LEN,
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

const SCTP_SNDINFO_CONTROL_LEN: usize = cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>());

/// Initializes the non-owning fields shared by retained SCTP send payloads.
///
/// # Safety
///
/// `msghdr` and `control` must point into one raw retained slot at their final
/// addresses, be properly aligned and writable for their complete field
/// sizes, and not overlap each other or the initialized iovec prefix. `iov`
/// must point at `iovlen` initialized iovecs whose backing allocations remain
/// stable until the target CQE retires.
#[inline(always)]
unsafe fn init_retained_sctp_send_fields(
    msghdr: *mut MaybeUninit<libc::msghdr>,
    control: *mut [u8; SCTP_SNDINFO_CONTROL_LEN],
    iov: *mut libc::iovec,
    iovlen: usize,
    sndinfo: libc::sctp_sndinfo,
) {
    unsafe {
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
    control: [MaybeUninit<u8>; SCTP_RCVINFO_CONTROL_LEN],
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
/// reactor. `buffer` must contain one chain whose nonempty writable-segment
/// count is `iov_count`, and the returned payload must be attached to a state
/// owned by that same reactor or consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_sctp_recv_vectored_payload<const N: usize>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<IoBuffVecMut<N>>,
    iov_count: usize,
) -> RetainedPayload<RetainedSctpRecvVectoredPayload<N>> {
    unsafe {
        with_raw_retained_slot::<RetainedSctpRecvVectoredPayload<N>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            let (materialized_count, writable_len) = fill_recv_vectored_iovecs(
                buffer.as_mut().unwrap_unchecked(),
                &mut *std::ptr::addr_of_mut!((*dst).iovecs),
            );
            debug_assert_eq!(
                materialized_count, iov_count,
                "SCTP vectored receive chain shape changed before submission"
            );
            debug_assert!(
                writable_len > 0,
                "SCTP vectored receive lost writable capacity before submission"
            );

            write_msghdr(
                &mut *std::ptr::addr_of_mut!((*dst).msghdr),
                MsgHdrInit {
                    name: std::ptr::null_mut(),
                    namelen: 0,
                    iov: std::ptr::addr_of_mut!((*dst).iovecs).cast::<libc::iovec>(),
                    iovlen: materialized_count,
                    control: std::ptr::addr_of_mut!((*dst).control)
                        .cast::<u8>()
                        .cast::<libc::c_void>(),
                    controllen: SCTP_RCVINFO_CONTROL_LEN,
                },
            );

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            let source = buffer.as_mut().unwrap_unchecked() as *mut IoBuffVecMut<N>;
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            writing.finish()
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
/// the returned payload must be attached to a state owned by that same reactor
/// or consumed through `pool`.
#[inline(always)]
unsafe fn emplace_retained_sctp_send_vectored_payload<const N: usize>(
    pool: NonNull<RetainedPayloadPool>,
    buffer: &mut Option<IoBuffVec<N>>,
    sndinfo: libc::sctp_sndinfo,
) -> RetainedPayload<RetainedSctpSendVectoredPayload<N>> {
    unsafe {
        with_raw_retained_slot::<RetainedSctpSendVectoredPayload<N>, _>(pool, |mut slot| {
            let dst = slot.as_mut_ptr();
            let (iov_count, readable_len) = buffer
                .as_ref()
                .unwrap_unchecked()
                .fill_write_iovecs_and_len(&mut *std::ptr::addr_of_mut!((*dst).iovecs));
            debug_assert!(
                iov_count > 0 && readable_len > 0,
                "SCTP vectored send lost readable data before submission"
            );
            init_retained_sctp_send_fields(
                std::ptr::addr_of_mut!((*dst).msghdr),
                std::ptr::addr_of_mut!((*dst).control),
                std::ptr::addr_of_mut!((*dst).iovecs).cast::<libc::iovec>(),
                iov_count,
                sndinfo,
            );

            let mut writing = slot.begin_writing();
            let dst = writing.as_mut_ptr();
            let source = buffer.as_mut().unwrap_unchecked() as *mut IoBuffVec<N>;
            std::ptr::copy_nonoverlapping(source, std::ptr::addr_of_mut!((*dst).buffer), 1);
            std::ptr::write(buffer, None);
            writing.finish()
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

struct SctpRecvCompletionFields {
    /// Only the kernel-reported initialized control prefix is copied.
    control: [MaybeUninit<u8>; SCTP_RCVINFO_CONTROL_LEN],
    header: SctpRecvHeader,
}

impl SctpRecvCompletionFields {
    #[inline(always)]
    fn control(&self) -> &[u8] {
        let len = self.header.msg_controllen.min(self.control.len());
        // SAFETY: the completion extractor copies exactly this bounded prefix
        // from the kernel-initialized receive control storage.
        unsafe { std::slice::from_raw_parts(self.control.as_ptr().cast::<u8>(), len) }
    }
}

struct SctpRecvCompletion<B> {
    fields: SctpRecvCompletionFields,
    buffer: B,
}

struct SctpRecvVectoredCompletion<const N: usize> {
    fields: SctpRecvCompletionFields,
    first_iovec: Option<libc::iovec>,
    buffer: IoBuffVecMut<N>,
}

struct StashedSctpRecvCompletion<B> {
    header: SctpRecvHeader,
    buffer: B,
}

struct StashedSctpRecvVectoredCompletion<const N: usize> {
    header: SctpRecvHeader,
    first_iovec: Option<libc::iovec>,
    /// Keeps every copied iovec target alive through discard-state processing.
    _buffer: IoBuffVecMut<N>,
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
/// Copies the receive metadata needed after retained backing is released.
///
/// # Safety
///
/// `control` and `msghdr` must point to the initialized fields of one live,
/// uniquely owned retained SCTP receive payload. The kernel-reported control
/// prefix must be initialized.
unsafe fn copy_sctp_recv_completion_fields(
    control: *const [MaybeUninit<u8>; SCTP_RCVINFO_CONTROL_LEN],
    msghdr: *const MaybeUninit<libc::msghdr>,
) -> SctpRecvCompletionFields {
    let header = unsafe { copy_sctp_recv_header(msghdr) };
    let mut copied_control = [MaybeUninit::uninit(); SCTP_RCVINFO_CONTROL_LEN];
    let copied_len = header.msg_controllen.min(SCTP_RCVINFO_CONTROL_LEN);
    unsafe {
        std::ptr::copy_nonoverlapping(
            control.cast::<u8>(),
            copied_control.as_mut_ptr().cast::<u8>(),
            copied_len,
        );
    }
    SctpRecvCompletionFields {
        control: copied_control,
        header,
    }
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
) -> Option<libc::iovec> {
    if iov_count == 0 {
        return None;
    }
    Some(unsafe {
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
) -> SctpRecvCompletion<B> {
    let fields = unsafe {
        copy_sctp_recv_completion_fields(
            std::ptr::addr_of!((*payload).control),
            std::ptr::addr_of!((*payload).msghdr),
        )
    };
    // Move the sole resource-owning field last. Nothing callback-capable may
    // run between this raw move and returning the compact completion.
    let buffer = unsafe { take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)) };
    SctpRecvCompletion { fields, buffer }
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
) -> SctpRecvVectoredCompletion<N> {
    let fields = unsafe {
        copy_sctp_recv_completion_fields(
            std::ptr::addr_of!((*payload).control),
            std::ptr::addr_of!((*payload).msghdr),
        )
    };
    let first_iovec =
        unsafe { copy_sctp_first_iovec(std::ptr::addr_of!((*payload).iovecs), iov_count) };
    // Move the sole resource-owning field last. Nothing callback-capable may
    // run between this raw move and returning the compact completion.
    let buffer = unsafe { take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)) };
    SctpRecvVectoredCompletion {
        fields,
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
        _buffer: buffer,
    }
}

#[inline(always)]
fn sctp_cqe_result(result: i32) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    Ok(result as usize)
}

#[inline(always)]
fn fill_recv_vectored_iovecs<const N: usize>(
    buffer: &mut IoBuffVecMut<N>,
    iovecs: &mut [MaybeUninit<libc::iovec>; N],
) -> (usize, usize) {
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

fn sctp_discarding_after_completion(header: SctpRecvHeader, partial_delivery_abort: bool) -> bool {
    // Linux requeues the truncated SCTP message tail at the receive-queue
    // front. While discarding, the first EOR therefore belongs to that
    // truncated message in normal mode; partial-delivery interleaving is
    // covered by the PDAPI-abort notification retirement path below.
    if sctp_msg_end_of_record(header.msg_flags) {
        return false;
    }

    if sctp_msg_notification(header.msg_flags) {
        return !partial_delivery_abort;
    }

    true
}

fn update_discarding_after_dropped_completion(
    discarding_tail: &mut bool,
    actual: usize,
    header: SctpRecvHeader,
    data_slice: &[u8],
) {
    let parsed_notification = parse_sctp_notification_once(data_slice, header.msg_flags);
    let partial_delivery_abort = sctp_notification_retires_discard(parsed_notification.as_ref());
    if sctp_msg_clean_eof(actual, header) || partial_delivery_abort {
        *discarding_tail = false;
    } else if *discarding_tail {
        *discarding_tail = sctp_discarding_after_completion(header, partial_delivery_abort);
    } else if sctp_msg_partial_nonempty(actual, header.msg_flags) {
        *discarding_tail = true;
    }
}

/// Returns the received prefix visible in the first vectored destination.
///
/// # Safety
///
/// A present `first_iovec` must describe the first writable destination, and
/// its base pointer must remain readable for `min(actual, iov_len)` bytes.
unsafe fn sctp_first_iov_slice(first_iovec: Option<&libc::iovec>, actual: usize) -> &[u8] {
    let Some(first_iov) = first_iovec else {
        return &[];
    };

    let safe_len = std::cmp::min(actual, first_iov.iov_len);
    unsafe { std::slice::from_raw_parts(first_iov.iov_base as *const u8, safe_len) }
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

/// Retires a dropped contiguous metadata receive and updates discard state.
///
/// # Safety
///
/// `state_ptr` must be a completed operation owned by `pctx`'s reactor with a
/// retained `RetainedSctpRecvPayload<B>`.
unsafe fn process_stashed_sctp_recv<B: IoBuffReadWrite>(
    pctx: &PollCtx,
    state_ptr: *mut CompletionState,
    _iov_count: usize,
    discarding_tail: &mut bool,
) {
    let result = unsafe { sctp_cqe_result((*state_ptr).result) };
    if let Ok(actual) = result {
        let mut completion = unsafe {
            (*pctx.reactor())
                .take_retained_payload_with::<RetainedSctpRecvPayload<B>, _>(state_ptr, |payload| {
                    take_stashed_sctp_recv_completion(payload)
                })
        };
        let data_slice = unsafe {
            let ptr = completion.buffer.as_mut_ptr();
            std::slice::from_raw_parts(ptr, actual)
        };
        update_discarding_after_dropped_completion(
            discarding_tail,
            actual,
            completion.header,
            data_slice,
        );
    }

    let mut state_ptr = state_ptr;
    unsafe { free_sctp_state(pctx, &mut state_ptr) };
}

/// Retires a dropped vectored metadata receive and updates discard state.
///
/// # Safety
///
/// `state_ptr` must be a completed operation owned by `pctx`'s reactor with a
/// retained `RetainedSctpRecvVectoredPayload<N>`, and `iov_count` must describe
/// its initialized iovec prefix.
unsafe fn process_stashed_sctp_recv_vectored<const N: usize>(
    pctx: &PollCtx,
    state_ptr: *mut CompletionState,
    iov_count: usize,
    discarding_tail: &mut bool,
) {
    let result = unsafe { sctp_cqe_result((*state_ptr).result) };
    if let Ok(actual) = result {
        let completion = unsafe {
            (*pctx.reactor()).take_retained_payload_with::<RetainedSctpRecvVectoredPayload<N>, _>(
                state_ptr,
                |payload| take_stashed_sctp_recv_vectored_completion(payload, iov_count),
            )
        };
        let data_slice = unsafe { sctp_first_iov_slice(completion.first_iovec.as_ref(), actual) };
        update_discarding_after_dropped_completion(
            discarding_tail,
            actual,
            completion.header,
            data_slice,
        );
    }

    let mut state_ptr = state_ptr;
    unsafe { free_sctp_state(pctx, &mut state_ptr) };
}

#[inline(always)]
/// Releases one completed or unsubmitted SCTP operation slot.
///
/// # Safety
///
/// `*state_ptr` must be non-null, owned by `pctx`'s reactor, and no kernel
/// operation may still reference its state or retained payload.
unsafe fn free_sctp_state(pctx: &PollCtx, state_ptr: &mut *mut CompletionState) {
    unsafe { (*pctx.reactor()).free_op(*state_ptr) };
    *state_ptr = std::ptr::null_mut();
}

#[inline(always)]
/// Extracts selected data from a completed SCTP payload and releases its
/// operation slot.
///
/// # Safety
///
/// A non-null `*state_ptr` must identify a completed FlowIO operation with
/// retained payload type `T`. Cleanup uses its recorded origin reactor, and
/// `extract` must move or drop every initialized field that requires
/// destruction.
unsafe fn take_completed_sctp_payload_with<T: 'static, R>(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
    extract: impl FnOnce(*mut T) -> R,
) -> Option<(io::Result<usize>, R, bool)> {
    if (*state_ptr).is_null() {
        return None;
    }

    let state = unsafe { &**state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = sctp_cqe_result(state.result);
    let op_ctx = unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), *state_ptr) };
    let value =
        unsafe { (*op_ctx.reactor()).take_retained_payload_with::<T, R>(*state_ptr, extract) };
    unsafe { free_sctp_state(op_ctx.origin_poll_ctx(), state_ptr) };
    Some((result, value, op_ctx.context_rejected()))
}

#[inline(always)]
/// Allocates the initial SCTP operation state and registers the current task.
///
/// # Safety
///
/// `*state_ptr` must be null, and `cx` must carry a valid FlowIO waker for the
/// executor/reactor that will own the new operation.
unsafe fn prepare_initial_sctp_state(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> io::Result<PollCtx> {
    debug_assert!(
        (*state_ptr).is_null(),
        "SCTP operation state already allocated"
    );
    let pctx = poll_ctx_from_waker(cx)?;
    let new_state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
    if new_state_ptr.is_null() {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }

    *state_ptr = new_state_ptr;
    unsafe { (*new_state_ptr).register_waiter(pctx.owner_task()) };
    Ok(pctx)
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
    let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
    if state_ptr.is_null() {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }

    let guard = unsafe { UnsubmittedOpGuard::new(pctx.reactor(), state_ptr) };
    unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };
    Ok((pctx, guard))
}

#[doc(hidden)]
pub struct DataRecvFuture<'a, B: IoBuffReadWrite> {
    /// SCTP association socket descriptor used for this data-only receive.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: *mut CompletionState,
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

        if let Some((result, buffer, context_rejected)) = unsafe {
            take_completed_sctp_payload_with::<RetainedDataRecvPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            if context_rejected {
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::NotConnected)), buffer));
            }
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
            let pctx = match unsafe { prepare_initial_sctp_state(cx, &mut this.state_ptr) } {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = this.state_ptr;

            let payload = RetainedDataRecvPayload {
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
                    free_sctp_state(&pctx, &mut this.state_ptr);
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for DataRecvFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct DataSendFuture<'a, B: IoBuffReadOnly> {
    /// SCTP association socket descriptor used for this data-only send.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: *mut CompletionState,
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

        if let Some((result, buffer, context_rejected)) = unsafe {
            take_completed_sctp_payload_with::<RetainedDataSendPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            if context_rejected {
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::NotConnected)), buffer));
            }
            return Poll::Ready((result, buffer));
        }

        if this.state_ptr.is_null() {
            let pctx = match unsafe { prepare_initial_sctp_state(cx, &mut this.state_ptr) } {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = this.state_ptr;

            let payload = RetainedDataSendPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_ptr();
                        Ok(build_sctp_send_entry(
                            this.fd,
                            ptr,
                            this.len,
                            state_ptr as u64,
                        ))
                    })
                {
                    free_sctp_state(&pctx, &mut this.state_ptr);
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadOnly> Drop for DataSendFuture<'_, B> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct RecvFuture<'a, B: IoBuffReadWrite> {
    /// SCTP association socket descriptor used for this recvmsg path.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: *mut CompletionState,
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

        match unsafe { this.recv_state.poll_stashed(cx) } {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(err), buffer));
            }
            Poll::Ready(Ok(())) => {}
        }

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

        if let Some((result, completion, context_rejected)) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpRecvPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_sctp_recv_completion(payload),
            )
        } {
            let mut completion = completion;
            if context_rejected {
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::NotConnected)),
                    completion.buffer,
                ));
            }
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), completion.buffer)),
            };

            let header = completion.fields.header;
            if sctp_msg_clean_eof(actual, header) {
                this.recv_state.discarding_tail = false;
                let completed = unsafe {
                    complete_read_with_progress(
                        completion.buffer,
                        this.write_base_len,
                        0,
                        Ok((0, sctp_eof_recv_meta())),
                    )
                };
                return Poll::Ready(completed);
            }

            let data_slice = unsafe {
                let ptr = completion.buffer.as_mut_ptr();
                std::slice::from_raw_parts(ptr, actual)
            };
            let parsed_notification = parse_sctp_notification_once(data_slice, header.msg_flags);
            let consume_internal = this
                .recv_state
                .should_consume_metadata_completion(header, parsed_notification.as_ref());
            if consume_internal {
                let (_, buffer) = unsafe {
                    complete_read_with_progress(completion.buffer, this.write_base_len, 0, Ok(()))
                };
                // Non-vectored internal recovery has no reusable iovec scratch
                // to refill: the next poll builds a fresh single-iovec payload
                // at the same unchanged caller-visible writable tail.
                this.buffer = Some(buffer);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let partial_nonempty = sctp_msg_partial_nonempty(actual, header.msg_flags);
            let meta = parse_recv_meta_with_notification(
                completion.fields.control(),
                header.msg_controllen,
                header.msg_flags,
                data_slice,
                parsed_notification,
            );

            if meta.is_err() && partial_nonempty {
                this.recv_state.discarding_tail = true;
            }
            let result = meta.map(|meta| (actual, meta));
            let completed = unsafe {
                complete_read_with_progress(completion.buffer, this.write_base_len, actual, result)
            };
            return Poll::Ready(completed);
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
                if let Err((e, payload)) =
                    submit_initialized_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            this.state_ptr = guard.into_state_ptr();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<B: IoBuffReadWrite> Drop for RecvFuture<'_, B> {
    fn drop(&mut self) {
        unsafe {
            if self.state_ptr.is_null() {
                self.recv_state.clear_stashed_waiter();
            } else {
                self.recv_state
                    .stash(&mut self.state_ptr, 0, process_stashed_sctp_recv::<B>);
            }
        }
    }
}

#[doc(hidden)]
pub struct SendFuture<'a, B: IoBuffReadOnly> {
    /// SCTP association socket descriptor used for this sendmsg path.
    fd: RawFd,
    /// Completion state for the submitted send operation.
    state_ptr: *mut CompletionState,
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

        if let Some((result, buffer, context_rejected)) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpSendPayload<B>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            if context_rejected {
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::NotConnected)), buffer));
            }
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
                if let Err((e, payload)) =
                    submit_initialized_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        Ok(build_sctp_sendmsg_entry(
                            this.fd,
                            payload.msghdr.as_ptr(),
                            state_ptr as u64,
                        ))
                    })
                {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            this.state_ptr = guard.into_state_ptr();
            return Poll::Pending;
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
// RecvVectoredFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvVectoredFuture<'a, const N: usize> {
    /// SCTP association socket descriptor used for this vectored recvmsg path.
    fd: RawFd,
    /// Completion state for the submitted receive operation.
    state_ptr: *mut CompletionState,
    /// Caller-owned vectored receive chain returned on completion.
    buffer: Option<IoBuffVecMut<N>>,
    /// Number of nonempty writable segments materialized for each submission.
    iov_count: usize,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Parent stream metadata receive state shared across metadata receives.
    recv_state: &'a mut SctpRecvState,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for RecvVectoredFuture<'_, N> {
    type Output = (io::Result<(usize, SctpRecvMeta)>, IoBuffVecMut<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match unsafe { this.recv_state.poll_stashed(cx) } {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(err), buffer));
            }
            Poll::Ready(Ok(())) => {}
        }

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

        if let Some((result, completion, context_rejected)) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpRecvVectoredPayload<N>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_sctp_recv_vectored_completion(payload, this.iov_count),
            )
        } {
            if context_rejected {
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::NotConnected)),
                    completion.buffer,
                ));
            }
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), completion.buffer)),
            };
            let header = completion.fields.header;
            if sctp_msg_clean_eof(actual, header) {
                this.recv_state.discarding_tail = false;
                let mut buffer = completion.buffer;
                unsafe {
                    buffer.distribute_written(0);
                }
                return Poll::Ready((Ok((0, sctp_eof_recv_meta())), buffer));
            }

            let data_slice =
                unsafe { sctp_first_iov_slice(completion.first_iovec.as_ref(), actual) };
            let parsed_notification = parse_sctp_notification_once(data_slice, header.msg_flags);
            let consume_internal = this
                .recv_state
                .should_consume_metadata_completion(header, parsed_notification.as_ref());
            if consume_internal {
                let mut buffer = completion.buffer;
                unsafe {
                    buffer.distribute_written(0);
                }
                let (iov_count, writable_len) = buffer.read_iovec_count_and_writable_len();
                debug_assert_eq!(
                    iov_count, this.iov_count,
                    "SCTP vectored internal recv changed the receive chain shape"
                );
                debug_assert!(
                    writable_len > 0,
                    "SCTP vectored internal recv lost writable capacity"
                );
                this.iov_count = iov_count;
                this.buffer = Some(buffer);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let partial_nonempty = sctp_msg_partial_nonempty(actual, header.msg_flags);
            let meta = parse_recv_meta_with_notification(
                completion.fields.control(),
                header.msg_controllen,
                header.msg_flags,
                data_slice,
                parsed_notification,
            );

            let mut buffer = completion.buffer;
            unsafe {
                buffer.distribute_written(actual);
            }

            return match meta {
                Ok(meta) => Poll::Ready((Ok((actual, meta)), buffer)),
                Err(err) => {
                    if partial_nonempty {
                        this.recv_state.discarding_tail = true;
                    }
                    Poll::Ready((Err(err), buffer))
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
                emplace_retained_sctp_recv_vectored_payload(
                    Reactor::retained_payload_pool_ptr(pctx.reactor()),
                    &mut this.buffer,
                    this.iov_count,
                )
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_initialized_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            this.state_ptr = guard.into_state_ptr();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<const N: usize> Drop for RecvVectoredFuture<'_, N> {
    fn drop(&mut self) {
        unsafe {
            if self.state_ptr.is_null() {
                self.recv_state.clear_stashed_waiter();
            } else {
                self.recv_state.stash(
                    &mut self.state_ptr,
                    self.iov_count,
                    process_stashed_sctp_recv_vectored::<N>,
                );
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
    state_ptr: *mut CompletionState,
    /// Caller-owned vectored send chain returned on completion.
    buffer: Option<IoBuffVec<N>>,
    /// Deferred validation error returned before any SQE submission.
    input_error: Option<io::Error>,
    /// Public send metadata translated into the kernel ABI layout.
    sndinfo: libc::sctp_sndinfo,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for SendVectoredFuture<'_, N> {
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

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

        if let Some((result, buffer, context_rejected)) = unsafe {
            take_completed_sctp_payload_with::<RetainedSctpSendVectoredPayload<N>, _>(
                cx,
                &mut this.state_ptr,
                |payload| take_sctp_retained_buffer(std::ptr::addr_of!((*payload).buffer)),
            )
        } {
            if context_rejected {
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::NotConnected)), buffer));
            }
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
                emplace_retained_sctp_send_vectored_payload(
                    Reactor::retained_payload_pool_ptr(pctx.reactor()),
                    &mut this.buffer,
                    this.sndinfo,
                )
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_initialized_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        Ok(build_sctp_sendmsg_entry(
                            this.fd,
                            payload.msghdr.as_ptr(),
                            state_ptr as u64,
                        ))
                    })
                {
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
            this.state_ptr = guard.into_state_ptr();
            return Poll::Pending;
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.state_ptr) };
        Poll::Pending
    }
}

impl<const N: usize> Drop for SendVectoredFuture<'_, N> {
    fn drop(&mut self) {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

/// Future returned by [`SctpListener::accept`] for one incoming association.
///
/// It resolves to the connected [`SctpStream`] and its peer address. The
/// future borrows the listener's reusable accept slot, so a listener can have
/// at most one live accept future. Dropping a pending future cancels its
/// readiness wait without consuming an association from the listener backlog.
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

impl Future for AcceptFuture<'_> {
    type Output = io::Result<(SctpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(err) = this.input_error.take() {
            return Poll::Ready(validate_local_io_result(cx, Err(err)));
        }

        let accepted_config = this.accepted_config;
        this.slot.poll_accept(
            cx,
            move |accepted_fd, accepted_linger_provenance, addr, addrlen| {
                let accepted_fd =
                    RuntimeFd::from_owned_with_provenance(accepted_fd, accepted_linger_provenance);
                finish_accepted_runtime_stream(accepted_fd, addr, addrlen, accepted_config)
            },
        )
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

        if !this.slot.state_ptr.is_null() {
            let state = unsafe { &*this.slot.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let op_ctx =
                    unsafe { completed_op_ctx(poll_ctx_from_waker(cx).ok(), this.slot.state_ptr) };
                unsafe { (*op_ctx.reactor()).free_op(this.slot.state_ptr) };
                this.slot.state_ptr = std::ptr::null_mut();
                this.slot.in_use = false;

                if op_ctx.context_rejected() {
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
                if let Err(err) = connect_cqe_result(result) {
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(err));
                }

                if let Err(err) =
                    apply_sctp_established_config(this.slot.fd, this.slot.connected_config)
                {
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
                return Poll::Ready(Ok(this.slot.take_stream(this.remote_addr)));
            }
        }

        if this.slot.state_ptr.is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    this.slot.in_use = false;
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
            };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                this.slot.in_use = false;
                this.slot.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.slot.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = match this.slot.addr.take() {
                Some(payload) => payload,
                None => {
                    unsafe { (*pctx.reactor()).free_op(state_ptr) };
                    this.slot.state_ptr = std::ptr::null_mut();
                    this.slot.in_use = false;
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::InvalidInput)));
                }
            };

            unsafe {
                if let Err((e, _payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let sqe = opcode::Connect::new(
                            types::Fd(this.slot.fd),
                            payload.addr_ptr(),
                            payload.addrlen,
                        )
                        .build()
                        .user_data(state_ptr as u64);
                        Ok(sqe)
                    })
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.slot.state_ptr = std::ptr::null_mut();
                    this.slot.in_use = false;
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(e));
                }
            }
            return Poll::Pending;
        }

        if unsafe { refresh_op_waiter_from_waker(cx, this.slot.state_ptr) } {
            this.slot.drop_future();
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }
        Poll::Pending
    }
}

impl Drop for ConnectFuture<'_> {
    fn drop(&mut self) {
        self.slot.drop_future();
    }
}

fn map_connect_timeout(
    result: Result<io::Result<SctpStream>, TimeoutError>,
) -> io::Result<SctpStream> {
    match result {
        Ok(result) => result,
        Err(TimeoutError::Elapsed) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        Err(TimeoutError::Runtime(err)) => Err(err),
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

fn checked_assoc_addr_count(addr_count: usize, payload_len: usize) -> io::Result<usize> {
    if addr_count > payload_len / MIN_SCTP_ASSOC_ADDR_LEN {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(addr_count)
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
        let addr_count = checked_assoc_addr_count(header.addr_num as usize, payload.len())?;
        return parse_assoc_addrs(payload, addr_count).map_err(|err| io::Error::from(err.kind()));
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
    let mut addrs = Vec::with_capacity(addr_count);
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

fn write_cmsg_sndinfo(control: &mut [u8], sndinfo: libc::sctp_sndinfo) {
    let hdr_len = std::mem::size_of::<libc::cmsghdr>();
    let data_offset = cmsg_align(hdr_len);
    let data_len = std::mem::size_of::<libc::sctp_sndinfo>();
    let needed = data_offset + data_len;
    // Callers size `control` via cmsg_space(size_of::<sctp_sndinfo>()), so
    // `needed` should fit; the guard is defensive and avoids writing past a
    // short slice.
    debug_assert!(control.len() >= needed);
    if control.len() < needed {
        return;
    }

    let hdr = libc::cmsghdr {
        cmsg_len: (hdr_len + data_len) as _,
        cmsg_level: libc::IPPROTO_SCTP,
        cmsg_type: libc::SCTP_SNDINFO,
    };
    unsafe {
        std::ptr::write_unaligned(control.as_mut_ptr() as *mut libc::cmsghdr, hdr);
        let data_ptr = control.as_mut_ptr().add(data_offset);
        std::ptr::write_unaligned(data_ptr as *mut libc::sctp_sndinfo, sndinfo);
    }
}

fn parse_rcvinfo(
    control: &[u8],
    controllen: usize,
    end_of_record: bool,
) -> io::Result<SctpRecvInfo> {
    let hdr_len = std::mem::size_of::<libc::cmsghdr>();
    let available = controllen.min(control.len());
    if available < hdr_len {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let hdr = unsafe { std::ptr::read_unaligned(control.as_ptr() as *const libc::cmsghdr) };
    if hdr.cmsg_level != libc::IPPROTO_SCTP || hdr.cmsg_type != libc::SCTP_RCVINFO {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let data_len = std::mem::size_of::<libc::sctp_rcvinfo>();
    let cmsg_len = hdr.cmsg_len;
    if cmsg_len < hdr_len + data_len || cmsg_len > available {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let needed = cmsg_align(hdr_len) + data_len;
    if available < needed {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let data_ptr = unsafe { control.as_ptr().add(cmsg_align(hdr_len)) };
    let info = unsafe { std::ptr::read_unaligned(data_ptr as *const libc::sctp_rcvinfo) };
    Ok(SctpRecvInfo {
        stream_id: info.rcv_sid,
        ssn: info.rcv_ssn,
        flags: info.rcv_flags,
        ppid: u32::from_be(info.rcv_ppid),
        tsn: info.rcv_tsn,
        cumtsn: info.rcv_cumtsn,
        context: info.rcv_context,
        assoc_id: info.rcv_assoc_id,
        end_of_record,
    })
}

#[cfg(any(feature = "fuzzing", feature = "test-support"))]
pub(crate) fn parse_recv_meta(
    control: &[u8],
    controllen: usize,
    msg_flags: libc::c_int,
    data_slice: &[u8],
) -> io::Result<SctpRecvMeta> {
    parse_recv_meta_with_notification(control, controllen, msg_flags, data_slice, None)
}

fn parse_recv_meta_with_notification(
    control: &[u8],
    controllen: usize,
    msg_flags: libc::c_int,
    data_slice: &[u8],
    parsed_notification: Option<io::Result<SctpRecvMeta>>,
) -> io::Result<SctpRecvMeta> {
    if (msg_flags & libc::MSG_TRUNC) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SCTP recvmsg payload was truncated",
        ));
    }

    let end_of_record = (msg_flags & libc::MSG_EOR) != 0;
    if !end_of_record && !data_slice.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SCTP recvmsg payload was partial before end-of-record",
        ));
    }

    if (msg_flags & libc::MSG_NOTIFICATION) != 0 {
        return match parsed_notification {
            Some(notification) => notification,
            None => parse_notification(data_slice),
        };
    }

    if controllen == 0 && (msg_flags & libc::MSG_CTRUNC) == 0 {
        return Ok(SctpRecvMeta::Data(SctpRecvInfo {
            end_of_record,
            ..SctpRecvInfo::default()
        }));
    }

    match parse_rcvinfo(control, controllen, end_of_record) {
        Ok(info) => {
            // The subscribed SCTP_RCVINFO cmsg was intact. Linux may still set
            // MSG_CTRUNC for extra control records beyond the single metadata
            // record this API consumes, so keep the data path successful.
            Ok(SctpRecvMeta::Data(info))
        }
        Err(_err) if (msg_flags & libc::MSG_CTRUNC) != 0 => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SCTP recvmsg control data was truncated",
        )),
        Err(err) => Err(err),
    }
}

fn send_failed_notification(
    error: u32,
    info: SctpSendInfo,
    assoc_id: libc::sctp_assoc_t,
) -> SctpNotification {
    SctpNotification::SendFailed {
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

// Linux UAPI `struct sctp_send_failed` and `struct sctp_send_failed_event`
// begin with the common 8-byte notification header, followed by a u32 error
// at byte 8 and the kernel send-info payload at byte 12. The association id
// follows that payload. Keep these offsets named so parser and tests pin the
// ABI layout instead of rebuilding offsets independently.
const SCTP_SEND_FAILED_ERROR_OFFSET: usize = 8;
const SCTP_SEND_FAILED_INFO_OFFSET: usize = 12;
const _: () =
    assert!(SCTP_SEND_FAILED_INFO_OFFSET == SCTP_SEND_FAILED_ERROR_OFFSET + size_of::<u32>());

fn parse_legacy_send_failed_notification(buffer: &[u8]) -> io::Result<SctpNotification> {
    let sndrcvinfo_len = std::mem::size_of::<libc::sctp_sndrcvinfo>();
    let min_len = SCTP_SEND_FAILED_INFO_OFFSET + sndrcvinfo_len + 4;
    if buffer.len() < min_len {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let error =
        read_u32_at(buffer, SCTP_SEND_FAILED_ERROR_OFFSET).map_err(byte_range_invalid_data)?;
    let sndrcvinfo_ptr = unsafe {
        buffer.as_ptr().add(SCTP_SEND_FAILED_INFO_OFFSET) as *const libc::sctp_sndrcvinfo
    };
    let sndrcvinfo = unsafe { std::ptr::read_unaligned(sndrcvinfo_ptr) };
    let assoc_base = SCTP_SEND_FAILED_INFO_OFFSET + sndrcvinfo_len;
    let assoc_id = read_i32_at(buffer, assoc_base).map_err(byte_range_invalid_data)?;
    Ok(send_failed_notification(
        error,
        send_info_from_sndrcvinfo(sndrcvinfo),
        assoc_id,
    ))
}

fn parse_send_failed_event_notification(buffer: &[u8]) -> io::Result<SctpNotification> {
    let sndinfo_len = std::mem::size_of::<libc::sctp_sndinfo>();
    let min_len = SCTP_SEND_FAILED_INFO_OFFSET + sndinfo_len + 4;
    if buffer.len() < min_len {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let error =
        read_u32_at(buffer, SCTP_SEND_FAILED_ERROR_OFFSET).map_err(byte_range_invalid_data)?;
    let sndinfo_ptr =
        unsafe { buffer.as_ptr().add(SCTP_SEND_FAILED_INFO_OFFSET) as *const libc::sctp_sndinfo };
    let sndinfo = unsafe { std::ptr::read_unaligned(sndinfo_ptr) };
    let assoc_base = SCTP_SEND_FAILED_INFO_OFFSET + sndinfo_len;
    let assoc_id = read_i32_at(buffer, assoc_base).map_err(byte_range_invalid_data)?;
    Ok(send_failed_notification(
        error,
        send_info_from_sndinfo(sndinfo),
        assoc_id,
    ))
}

pub(crate) fn parse_notification(buffer: &[u8]) -> io::Result<SctpRecvMeta> {
    if buffer.len() < 8 {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let sn_type = read_u16_at(buffer, 0).map_err(byte_range_invalid_data)?;
    let sn_flags = read_u16_at(buffer, 2).map_err(byte_range_invalid_data)?;
    let sn_length = read_u32_at(buffer, 4).map_err(byte_range_invalid_data)?;
    if sn_length < 8 || sn_length as usize > buffer.len() {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let buffer = &buffer[..sn_length as usize];

    let notification = match sn_type as libc::c_int {
        x if x == LOCAL_SCTP_ASSOC_CHANGE => {
            if buffer.len() < 20 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::AssocChange {
                state: read_u16_at(buffer, 8).map_err(byte_range_invalid_data)?,
                error: read_u16_at(buffer, 10).map_err(byte_range_invalid_data)?,
                outbound_streams: read_u16_at(buffer, 12).map_err(byte_range_invalid_data)?,
                inbound_streams: read_u16_at(buffer, 14).map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, 16).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_PEER_ADDR_CHANGE => {
            let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
            let min_len = 8 + storage_len + 12;
            if buffer.len() < min_len {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }

            let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buffer.as_ptr().add(8),
                    &mut storage as *mut _ as *mut u8,
                    storage_len,
                );
            }
            let addr = socket_addr_from_c(
                &storage,
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            )?;
            let base = 8 + storage_len;
            SctpNotification::PeerAddrChange {
                addr,
                state: read_i32_at(buffer, base).map_err(byte_range_invalid_data)?,
                error: read_i32_at(buffer, base + 4).map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, base + 8).map_err(byte_range_invalid_data)?,
            }
        }
        // Defensive only: FlowIO configures sctp_send_failure_event=0, but
        // untrusted/test notification bytes can still exercise this legacy layout.
        x if x == LOCAL_SCTP_SEND_FAILED => parse_legacy_send_failed_notification(buffer)?,
        x if x == LOCAL_SCTP_REMOTE_ERROR => {
            if buffer.len() < 16 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::RemoteError {
                error: read_u16_be_at(buffer, 8).map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, 12).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_SHUTDOWN_EVENT => {
            if buffer.len() < 12 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::Shutdown {
                assoc_id: read_i32_at(buffer, 8).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_ADAPTATION_INDICATION => {
            if buffer.len() < 16 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::Adaptation {
                indication: read_u32_at(buffer, 8).map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, 12).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_PARTIAL_DELIVERY_EVENT => {
            if buffer.len() < 24 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::PartialDelivery {
                indication: read_u32_at(buffer, 8).map_err(byte_range_invalid_data)?,
                assoc_id: read_i32_at(buffer, 12).map_err(byte_range_invalid_data)?,
                stream: read_u32_at(buffer, 16).map_err(byte_range_invalid_data)?,
                sequence: read_u32_at(buffer, 20).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_SENDER_DRY_EVENT => {
            if buffer.len() < 12 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::SenderDry {
                assoc_id: read_i32_at(buffer, 8).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_STREAM_RESET_EVENT => {
            if buffer.len() < 12 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::StreamReset {
                flags: sn_flags,
                assoc_id: read_i32_at(buffer, 8).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_ASSOC_RESET_EVENT => {
            if buffer.len() < 20 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::AssocReset {
                flags: sn_flags,
                assoc_id: read_i32_at(buffer, 8).map_err(byte_range_invalid_data)?,
                local_tsn: read_u32_at(buffer, 12).map_err(byte_range_invalid_data)?,
                remote_tsn: read_u32_at(buffer, 16).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_STREAM_CHANGE_EVENT => {
            if buffer.len() < 16 {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::StreamChange {
                flags: sn_flags,
                assoc_id: read_i32_at(buffer, 8).map_err(byte_range_invalid_data)?,
                inbound_streams: read_u16_at(buffer, 12).map_err(byte_range_invalid_data)?,
                outbound_streams: read_u16_at(buffer, 14).map_err(byte_range_invalid_data)?,
            }
        }
        x if x == LOCAL_SCTP_SEND_FAILED_EVENT => parse_send_failed_event_notification(buffer)?,
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

    fn test_accept_slot_drop_preserves_readiness_mask(cached: bool) -> io::Result<()> {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
        let mut state = CompletionState::empty();
        state.result = fd;
        state.set_completed();

        let listener_fd = Rc::new(RuntimeFd::from_fresh_raw_fd(fd));
        let mut slot = AcceptSlot::new(Rc::clone(&listener_fd));
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
        let mut slot = ConnectSlot::new();
        slot.in_use = true;
        slot.fd = fd;

        slot.drop_future();

        if !slot.state_ptr.is_null() || slot.in_use || slot.fd != -1 {
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
        parse_recv_meta(control, controllen, msg_flags, data_slice)
    }

    /// Returns the local Linux ABI value for `SCTP_ASSOC_CHANGE`.
    pub const fn test_assoc_change_type() -> libc::c_int {
        LOCAL_SCTP_ASSOC_CHANGE
    }

    /// Returns the local Linux ABI value for `SCTP_ADAPTATION_INDICATION`.
    pub const fn test_adaptation_indication_type() -> libc::c_int {
        LOCAL_SCTP_ADAPTATION_INDICATION
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

    unsafe fn reject_abandoned_stashed_processing(
        _pctx: &PollCtx,
        _state_ptr: *mut CompletionState,
        _iov_count: usize,
        _discarding_tail: &mut bool,
    ) {
        panic!("ring-abandoned stashed receive was processed as completed");
    }

    #[test]
    fn ring_abandoned_stashed_receive_returns_unsubmitted_buffer() {
        let mut abandoned = CompletionState::empty();
        abandoned.set_ring_abandoned();
        let mut recv_state = SctpRecvState::external();
        recv_state.stashed = StashedSctpRecv {
            state_ptr: &mut abandoned,
            iov_count: 1,
            process_completed: Some(reject_abandoned_stashed_processing),
        };
        let mut buffer = IoBuffMut::new(0, 16, 0).expect("receive buffer allocation failed");
        let original_buffer_ptr = buffer.as_mut_ptr();
        let mut recv = RecvFuture {
            fd: -1,
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            write_base_len: 0,
            len: 16,
            input_error: None,
            recv_state: &mut recv_state,
            _marker: PhantomData,
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());

        let Poll::Ready((Err(err), mut returned)) = Pin::new(&mut recv).poll(&mut cx) else {
            panic!("ring-abandoned stashed receive did not return a terminal error");
        };
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        assert_eq!(returned.as_mut_ptr(), original_buffer_ptr);
        assert!(recv.recv_state.stashed.state_ptr.is_null());
        assert_eq!(recv.recv_state.stashed.iov_count, 0);
        assert!(recv.recv_state.stashed.process_completed.is_none());
        assert!(abandoned.is_ring_abandoned());
        assert!(!abandoned.is_completed());
    }

    #[cfg(not(miri))]
    #[test]
    fn ring_abandoned_sctp_connect_closes_owned_socket_without_reclaiming_state() {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()
            .expect("SCTP connect fd creation failed");
        let mut state = CompletionState::empty();
        state.set_ring_abandoned();
        let mut slot = ConnectSlot::new();
        slot.state_ptr = &mut state;
        slot.in_use = true;
        slot.fd = fd;
        let mut connect = ConnectFuture {
            slot: &mut slot,
            remote_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());

        assert!(matches!(
            Pin::new(&mut connect).poll(&mut cx),
            Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
        ));
        drop(connect);
        assert!(slot.state_ptr.is_null());
        assert!(!slot.in_use);
        assert!(crate::runtime::fd::raw_fd_is_closed(fd));
        assert!(state.is_ring_abandoned());
        assert!(!state.is_completed());
    }

    #[cfg(not(miri))]
    #[test]
    fn sctp_terminal_accept_readiness_would_block_is_not_rearmed() {
        crate::net::test_terminal_accept_readiness(
            "SCTP",
            AcceptSlot::new,
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
            |slot| slot.state_ptr.is_null() && !slot.in_use,
        );
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

        let (iov_count, writable_len) = fill_recv_vectored_iovecs(&mut chain, &mut iovecs);
        assert_eq!((iov_count, writable_len), (1, 8));
        let first = unsafe { iovecs[0].assume_init_ref() };
        unsafe {
            std::ptr::copy_nonoverlapping(b"note".as_ptr(), first.iov_base.cast::<u8>(), 4);
        }

        let first_iovec = unsafe { copy_sctp_first_iovec(&iovecs, iov_count) };
        let received = unsafe { sctp_first_iov_slice(first_iovec.as_ref(), 4) };
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

    struct RetainedConstructorBuffer {
        bytes: Box<[u8; 32]>,
        reenter_pool: Option<NonNull<RetainedPayloadPool>>,
        pointer_calls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        panic_on_pointer: bool,
    }

    impl RetainedConstructorBuffer {
        fn note_pointer_access(&self) {
            self.pointer_calls.set(self.pointer_calls.get() + 1);
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
            drops,
            panic_on_pointer,
        }
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
    fn sctp_cqe_result_maps_error_zero_and_progress() {
        let err = sctp_cqe_result(-libc::EPIPE).expect_err("negative CQE should map to errno");
        assert_eq!(err.raw_os_error(), Some(libc::EPIPE));
        assert_eq!(sctp_cqe_result(0).expect("zero CQE should succeed"), 0);
        assert_eq!(sctp_cqe_result(9).expect("positive CQE should succeed"), 9);
    }

    #[test]
    fn connected_sctp_recv_payloads_fit_without_peer_address_storage() {
        assert!(
            std::mem::size_of::<RetainedSctpRecvPayload<IoBuffMut>>() <= 256,
            "scalar rich receive no longer fits its reduced retained size class"
        );
        assert!(
            std::mem::size_of::<RetainedSctpRecvVectoredPayload<16>>() <= 1024,
            "N=16 rich receive no longer fits its reduced retained size class"
        );
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
        let expected_control = [0x31, 0x42, 0x53, 0x64, 0x75, 0x86, 0x97];
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

        let completion =
            unsafe { payload.take_with(&mut pool, |payload| take_sctp_recv_completion(payload)) };
        assert_eq!(
            pointer_calls.get(),
            1,
            "completion extraction invoked a caller buffer callback"
        );
        assert_eq!(drops.get(), 0, "completion extraction dropped the owner");
        assert_eq!(completion.buffer.bytes.as_ptr(), expected_buffer_ptr);
        assert_eq!(
            completion.fields.header.msg_controllen,
            expected_control.len()
        );
        assert_eq!(
            completion.fields.header.msg_flags,
            libc::MSG_EOR | libc::MSG_CTRUNC
        );
        assert_eq!(completion.fields.control(), expected_control);
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
                SCTP_RCVINFO_CONTROL_LEN,
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
        let (iov_count, writable_len) = chain.read_iovec_count_and_writable_len();
        assert_eq!((iov_count, writable_len), (2, 18));
        let mut buffer = Some(chain);

        let payload = unsafe {
            emplace_retained_sctp_recv_vectored_payload(pool_ptr, &mut buffer, iov_count)
        };
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
        let (iov_count, writable_len) = chain.read_iovec_count_and_writable_len();
        assert_eq!((iov_count, writable_len), (N, N * 8));
        let mut buffer = Some(chain);

        let mut payload = unsafe {
            emplace_retained_sctp_recv_vectored_payload(pool_ptr, &mut buffer, iov_count)
        };
        let retained_addr = unsafe { payload.as_mut() as *mut _ as usize };
        const TAIL_POISON: u8 = 0x5a;
        let expected_control = [0xa1, 0xb2, 0xc3, 0xd4, 0xe5];
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
            msg.msg_flags = libc::MSG_NOTIFICATION | libc::MSG_EOR;
        }

        let mut completion = unsafe {
            payload.take_with(&mut pool, |payload| {
                take_sctp_recv_vectored_completion(payload, iov_count)
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
            unsafe { sctp_first_iov_slice(Some(first_iovec), 4) },
            b"note"
        );
        assert_eq!(completion.fields.control(), expected_control);
        assert_eq!(
            completion.fields.header.msg_flags,
            libc::MSG_NOTIFICATION | libc::MSG_EOR
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
        let (retry_iov_count, _) = retry_chain.read_iovec_count_and_writable_len();
        let mut retry_buffer = Some(retry_chain);
        let pool_ptr = NonNull::from(&mut pool);
        let mut retry = unsafe {
            emplace_retained_sctp_recv_vectored_payload(
                pool_ptr,
                &mut retry_buffer,
                retry_iov_count,
            )
        };
        assert_eq!(
            unsafe { retry.as_mut() as *mut _ as usize },
            retained_addr,
            "N=16 selective extraction did not recycle retained backing"
        );
        let reused_control = unsafe {
            std::slice::from_raw_parts(
                retry.as_ref().control.as_ptr().cast::<u8>(),
                SCTP_RCVINFO_CONTROL_LEN,
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
        assert!(unsafe { copy_sctp_first_iovec(&uninitialized, 0) }.is_none());
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
        let mut buffer = Some(chain);
        let info = SctpSendInfo {
            stream_id: 11,
            flags: 5,
            ppid: 0xa1b2_c3d4,
            context: 0x5566_7788,
            assoc_id: 17,
        };
        let sndinfo = raw_sndinfo_from_public(info);

        let payload =
            unsafe { emplace_retained_sctp_send_vectored_payload(pool_ptr, &mut buffer, sndinfo) };
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
        write_u16_ne(&mut bytes, 0, notification_type as u16);
        write_u16_ne(&mut bytes, 2, 0);
        write_u32_ne(&mut bytes, 4, len as u32);
        bytes
    }

    fn test_partial_delivery_notification(indication: u32) -> Vec<u8> {
        let mut bytes = test_notification_buffer(LOCAL_SCTP_PARTIAL_DELIVERY_EVENT, 24);
        write_u32_ne(&mut bytes, 8, indication);
        bytes
    }

    fn notification_retires_discard_for_test(data_slice: &[u8], msg_flags: libc::c_int) -> bool {
        let parsed_notification = parse_sctp_notification_once(data_slice, msg_flags);
        sctp_notification_retires_discard(parsed_notification.as_ref())
    }

    fn discarding_after_completion_for_test(msg: &libc::msghdr, data_slice: &[u8]) -> bool {
        let parsed_notification = parse_sctp_notification_once(data_slice, msg.msg_flags);
        let partial_delivery_abort =
            sctp_notification_retires_discard(parsed_notification.as_ref());
        sctp_discarding_after_completion(test_recv_header(msg), partial_delivery_abort)
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
        assert!(!discarding_after_completion_for_test(&msg, &data));

        let mut discarding_tail = true;
        update_discarding_after_dropped_completion(
            &mut discarding_tail,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert!(!discarding_tail);

        let mut synchronized = false;
        update_discarding_after_dropped_completion(
            &mut synchronized,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert!(
            !synchronized,
            "a dropped PDAPI abort must not start discard when none was active"
        );
    }

    #[test]
    fn sctp_live_and_dropped_non_eor_abort_retirement_agree() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);
        let parsed_notification = parse_sctp_notification_once(&data, msg.msg_flags);

        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;
        let mut live = SctpRecvState::configured(metadata_only);
        live.discarding_tail = true;
        assert!(
            live.should_consume_metadata_completion(
                test_recv_header(&msg),
                parsed_notification.as_ref(),
            ),
            "a FlowIO-forced PDAPI abort remains internal"
        );

        let mut dropped = true;
        update_discarding_after_dropped_completion(
            &mut dropped,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert_eq!(
            live.discarding_tail, dropped,
            "live and dropped completion retirement must use one oracle"
        );
        assert!(
            !live.discarding_tail,
            "a complete PDAPI abort retires discard even without MSG_EOR"
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
        assert!(!visible.should_consume_metadata_completion(
            test_recv_header(&msg),
            Some(&parsed_notification)
        ));

        // A deliberately different, malformed slice proves final metadata
        // consumes the already-parsed value instead of decoding the bytes a
        // second time.
        assert!(matches!(
            parse_recv_meta_with_notification(
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
        assert!(!visible.should_consume_metadata_completion(
            test_recv_header(&msg),
            Some(&parsed_notification)
        ));
        assert_eq!(
            parse_recv_meta_with_notification(
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
    fn preparsed_notification_preserves_framing_and_visibility_precedence() {
        let mut shutdown = test_notification_buffer(LOCAL_SCTP_SHUTDOWN_EVENT, 12);
        write_u32_ne(&mut shutdown, 8, 7);

        let truncated =
            test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_TRUNC | libc::MSG_EOR);
        let mut visible = SctpRecvState::external();
        let parsed_notification = parse_sctp_notification_once(&shutdown, truncated.msg_flags)
            .expect("visible notification should be parsed");
        assert!(!visible.should_consume_metadata_completion(
            test_recv_header(&truncated),
            Some(&parsed_notification),
        ));
        assert_eq!(
            parse_recv_meta_with_notification(
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
        assert!(!visible.should_consume_metadata_completion(
            test_recv_header(&partial),
            Some(&parsed_notification)
        ));
        assert_eq!(
            parse_recv_meta_with_notification(
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
        let parsed_notification =
            parse_sctp_notification_once(&malformed, hidden_fragment.msg_flags)
                .expect("a hidden PDAPI fragment still drives discard policy");
        assert!(parsed_notification.is_err());
        assert!(hidden.should_consume_metadata_completion(
            test_recv_header(&hidden_fragment),
            Some(&parsed_notification),
        ));
        assert!(hidden.discarding_tail);
        let hidden_eor =
            test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_TRUNC | libc::MSG_EOR);
        assert!(hidden.should_consume_for_test(&malformed, &hidden_eor));
        assert!(!hidden.discarding_tail);

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
        other_visible.discarding_tail = true;
        let parsed_notification = parse_sctp_notification_once(&abort, abort_truncated.msg_flags)
            .expect("forced PDAPI should be parsed when another event is visible");
        assert!(other_visible.should_consume_metadata_completion(
            test_recv_header(&abort_truncated),
            Some(&parsed_notification),
        ));
        assert!(!other_visible.discarding_tail);

        metadata_only.notifications.partial_delivery = true;
        let mut explicit = SctpRecvState::configured(metadata_only);
        explicit.discarding_tail = true;
        let parsed_notification = parse_sctp_notification_once(&abort, abort_truncated.msg_flags)
            .expect("explicit PDAPI should be parsed");
        assert!(!explicit.should_consume_metadata_completion(
            test_recv_header(&abort_truncated),
            Some(&parsed_notification),
        ));
        assert!(!explicit.discarding_tail);
        assert_eq!(
            parse_recv_meta_with_notification(
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

        visible.discarding_tail = true;
        let parsed_notification = parse_sctp_notification_once(&malformed, partial.msg_flags)
            .expect("visible malformed notification should be parsed");
        assert!(visible.should_consume_metadata_completion(
            test_recv_header(&partial),
            Some(&parsed_notification)
        ));
        assert!(visible.discarding_tail);
    }

    #[test]
    fn forced_partial_delivery_abort_is_internal_only_for_metadata_policy() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

        let mut metadata_only = SctpSocketConfig::data(SctpInitConfig::default());
        metadata_only.recv_rcvinfo = true;
        let mut forced = SctpRecvState::configured(metadata_only);
        let parsed_notification = parse_sctp_notification_once(&data, msg.msg_flags)
            .expect("a hidden PDAPI event still drives discard policy");
        assert!(parsed_notification.is_ok());
        assert!(
            forced.should_consume_metadata_completion(
                test_recv_header(&msg),
                Some(&parsed_notification),
            ),
            "FlowIO-only forced notifications remain internal"
        );

        forced.discarding_tail = true;
        assert!(forced.should_consume_for_test(&data, &msg));
        assert!(!forced.discarding_tail);

        metadata_only.notifications.partial_delivery = true;
        let mut explicit = SctpRecvState::configured(metadata_only);
        let parsed_notification = parse_sctp_notification_once(&data, msg.msg_flags)
            .expect("caller-visible PDAPI notification should be parsed");
        assert!(!explicit.should_consume_metadata_completion(
            test_recv_header(&msg),
            Some(&parsed_notification)
        ));
        assert!(matches!(
            parse_recv_meta_with_notification(
                &[],
                0,
                msg.msg_flags,
                &data,
                Some(parsed_notification)
            ),
            Ok(SctpRecvMeta::Notification(
                SctpNotification::PartialDelivery {
                    indication: SCTP_PARTIAL_DELIVERY_ABORTED,
                    ..
                }
            ))
        ));

        explicit.discarding_tail = true;
        assert!(!explicit.should_consume_for_test(&data, &msg));
        assert!(!explicit.discarding_tail);

        explicit.set_notification_visibility(SctpNotificationMask::none());
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
        assert!(fragmented_forced.discarding_tail);
        assert!(fragmented_forced.should_consume_for_test(&data[1..2], &msg));
        assert!(!fragmented_forced.discarding_tail);
        let intact = test_msghdr_with_flags(libc::MSG_EOR);
        assert!(!fragmented_forced.should_consume_for_test(b"next", &intact));

        let mut synchronized = false;
        update_discarding_after_dropped_completion(
            &mut synchronized,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert!(!synchronized);

        let eor = test_msghdr_with_flags(libc::MSG_EOR);
        update_discarding_after_dropped_completion(
            &mut synchronized,
            4,
            test_recv_header(&eor),
            b"next",
        );
        assert!(!synchronized);

        let eof = test_msghdr_with_flags(0);
        update_discarding_after_dropped_completion(
            &mut synchronized,
            0,
            test_recv_header(&eof),
            &[],
        );
        assert!(!synchronized);
    }

    #[test]
    fn sctp_non_abort_notification_without_eor_keeps_discard_active() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED + 1);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(!notification_retires_discard_for_test(&data, msg.msg_flags));
        assert!(discarding_after_completion_for_test(&msg, &data));

        let mut discarding_tail = true;
        update_discarding_after_dropped_completion(
            &mut discarding_tail,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert!(discarding_tail);
    }

    #[test]
    fn sctp_dropped_partial_notification_starts_discard() {
        let data = test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 8);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(sctp_msg_partial_nonempty(data.len(), msg.msg_flags));
        let mut discarding_tail = false;
        update_discarding_after_dropped_completion(
            &mut discarding_tail,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert!(discarding_tail);
    }

    #[test]
    fn sctp_notification_eor_tail_retires_discard() {
        let data = test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 20);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

        assert!(!notification_retires_discard_for_test(&data, msg.msg_flags));
        assert!(!discarding_after_completion_for_test(&msg, &data));

        let mut discarding_tail = true;
        update_discarding_after_dropped_completion(
            &mut discarding_tail,
            data.len(),
            test_recv_header(&msg),
            &data,
        );
        assert!(!discarding_tail);
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
    fn assoc_addr_count_is_bounded_by_payload_capacity() {
        let one = checked_assoc_addr_count(1, MIN_SCTP_ASSOC_ADDR_LEN)
            .expect("one minimum-sized record should succeed");
        assert_eq!(one, 1);
        let one_byte_short = checked_assoc_addr_count(1, MIN_SCTP_ASSOC_ADDR_LEN - 1)
            .expect_err("a short payload cannot contain one record");
        assert_eq!(one_byte_short.kind(), io::ErrorKind::InvalidData);

        let max_payload =
            MAX_SCTP_ASSOC_ADDR_CAPACITY * std::mem::size_of::<libc::sockaddr_storage>();
        let packed_ipv4_max = max_payload / MIN_SCTP_ASSOC_ADDR_LEN;
        assert!(packed_ipv4_max > MAX_SCTP_ASSOC_ADDR_CAPACITY);
        assert_eq!(
            checked_assoc_addr_count(packed_ipv4_max, max_payload)
                .expect("capacity-derived maximum should succeed"),
            packed_ipv4_max
        );

        let short_payload = checked_assoc_addr_count(packed_ipv4_max, max_payload - 1)
            .expect_err("actual short payload should lower the count bound");
        assert_eq!(short_payload.kind(), io::ErrorKind::InvalidData);

        let err = checked_assoc_addr_count(packed_ipv4_max + 1, max_payload)
            .expect_err("over-cap addr count should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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
        let mut legacy_compact_dense = assoc_ipv4_entry([1, 2, 3, 4], 1111, 8);
        legacy_compact_dense.extend_from_slice(&assoc_ipv4_entry(
            [5, 6, 7, 8],
            2222,
            std::mem::size_of::<libc::sockaddr_in>(),
        ));
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        let ipv6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let padded_ipv4 = assoc_ipv4_entry([192, 0, 2, 10], 1234, storage_len);
        let padded_ipv6 = assoc_ipv6_entry(ipv6, 4321, 7, 9, storage_len);

        for (payload, addr_count, label) in [
            (compact_ipv4, 1, "compact IPv4"),
            (legacy_compact_dense, 2, "legacy compact-plus-dense IPv4"),
            (padded_ipv4, 1, "storage-padded IPv4"),
            (padded_ipv6, 1, "storage-padded IPv6"),
        ] {
            let err = parse_assoc_addrs(&payload, addr_count).expect_err(label);
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
