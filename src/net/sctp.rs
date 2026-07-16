//! One-to-one SCTP transport with message-oriented send and receive operations.
//!
//! # Compatibility
//!
//! This implementation targets the Linux SCTP socket API.
//!
//! Baseline one-to-one SCTP operations are expected to work on Linux systems
//! where SCTP is enabled in the kernel:
//! - [`SctpListener::bind`]
//! - [`SctpListener::accept`]
//! - [`SctpConnector::connect`]
//! - [`SctpStream::send_msg`]
//! - [`SctpStream::recv_msg`]
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
    MsgHdrInit, checked_read_len, checked_send_len, close_fd, close_if_valid,
    complete_read_with_progress, current_local_addr, invalid_input, set_reuse_addr, set_sock_opt,
    socket_addr_from_c, socket_addr_to_c, socket_domain, write_msghdr,
};
use crate::net::send_sqe::{build_send_entry, build_sendmsg_entry};
use crate::runtime::buffer::bytes::{
    BufferRangeError, read_i32_at, read_u16_at, read_u16_be_at, read_u32_at,
};
use crate::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use crate::runtime::executor::{
    PollCtx, completed_op_ctx_from_waker, drop_op_ptr_unchecked, poll_ctx_from_waker,
    refresh_op_waiter_from_waker, submit_retained_sqe, validate_local_io_result,
};
use crate::runtime::fd::RuntimeFd;
use crate::runtime::op::CompletionState;
use crate::runtime::timer::{Timeout, TimeoutError, timeout};
use io_uring::{opcode, squeue, types};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::{MaybeUninit, size_of};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Kernel-facing notification-subscription layout used with SCTP socket
/// options on Linux.
#[repr(C)]
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

/// Per-message SCTP receive metadata extracted from `SCTP_RCVINFO`.
///
/// This belongs to the metadata receive path returned by
/// [`SctpStream::recv_msg`]. It is not produced by the lean data fast path;
/// use [`SctpStream::recv`] when the caller only needs bytes.
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
/// control operations, not for steady-state payload exchange.
///
/// # Example
/// ```no_run
/// use flowio::net::sctp::{SctpResetStreams, SctpReconfigFlags};
///
/// let request = SctpResetStreams::outgoing(&[1, 3]);
/// let flags = SctpReconfigFlags {
///     flags: SctpReconfigFlags::RESET_STREAMS,
///     ..SctpReconfigFlags::association_default()
/// };
/// # let _ = (request, flags);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SctpResetStreams {
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Direction flags (incoming, outgoing, or both).
    pub flags: u16,
    /// Stream numbers to reset.
    pub streams: Vec<u16>,
}

impl SctpResetStreams {
    /// Resets the specified incoming streams.
    pub fn incoming(streams: &[u16]) -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING,
            streams: streams.to_vec(),
        }
    }

    /// Resets the specified outgoing streams.
    pub fn outgoing(streams: &[u16]) -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_OUTGOING,
            streams: streams.to_vec(),
        }
    }

    /// Resets the specified incoming and outgoing streams.
    pub fn bidirectional(streams: &[u16]) -> Self {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING | SCTP_STREAM_RESET_OUTGOING,
            streams: streams.to_vec(),
        }
    }
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
    /// Regular user data was received and the contained metadata came from
    /// `SCTP_RCVINFO`.
    Data(#[doc = "Per-message receive information decoded from ancillary data."] SctpRecvInfo),
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
    /// Partial-delivery notifications.
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
}

impl Default for SctpNotificationMask {
    fn default() -> Self {
        Self::signaling_default()
    }
}

#[repr(C, packed(4))]
/// Linux `sctp_prim` layout used to select the local primary destination.
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

impl SctpAssocParamsRaw {
    fn get(fd: RawFd) -> io::Result<Self> {
        let mut raw = Self {
            assoc_id: 0,
            assoc_max_retrans: 0,
            peer_destinations: 0,
            peer_receiver_window: 0,
            local_receiver_window: 0,
            cookie_life_ms: 0,
        };
        let mut optlen = std::mem::size_of::<Self>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_SCTP,
                libc::SCTP_ASSOCINFO,
                &mut raw as *mut Self as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<Self>() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(raw)
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
        let mut raw = Self {
            assoc_id: 0,
            rto_initial_ms: 0,
            rto_max_ms: 0,
            rto_min_ms: 0,
        };
        let mut optlen = std::mem::size_of::<Self>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_SCTP,
                libc::SCTP_RTOINFO,
                &mut raw as *mut Self as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<Self>() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(raw)
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

/// Adopts one successful accept result and applies connected-socket policy.
fn finish_accepted_stream(
    accepted_fd: OwnedFd,
    addr: &libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    config: SctpSocketConfig,
) -> io::Result<(SctpStream, SocketAddr)> {
    let remote_addr = socket_addr_from_c(addr, addrlen)?;
    apply_sctp_connected_socket_config(accepted_fd.as_raw_fd(), config)?;
    Ok((
        SctpStream::from_owned_fd(accepted_fd, remote_addr),
        remote_addr,
    ))
}

/// Reusable accept-side submission state kept by [`SctpListener`].
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
        // Normal safe use drops AcceptFuture before SctpListener. This also
        // handles safe `mem::forget(AcceptFuture)` teardown, where the slot can
        // still hold an in-flight or completed accept state when the listener
        // is finally dropped. A completed accepted fd is owned by this slot and
        // must be closed before the cached state is released.
        self.drop_future();
    }
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
        SctpStream::from_owned_fd(unsafe { OwnedFd::from_raw_fd(fd) }, remote_addr)
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

/// Kernel-visible accept address storage retained until the accept CQE retires.
struct RetainedAcceptAddr {
    /// Kernel-written peer address storage for the accepted association.
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
    fd: RuntimeFd,
    /// Local address bound to the listening socket.
    local_addr: SocketAddr,
    /// Reusable accept state for at most one in-flight accept future.
    accept_slot: AcceptSlot,
    /// Socket configuration applied to streams returned by accepted fds.
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

        Ok(Self {
            fd: RuntimeFd::new(fd),
            local_addr,
            accept_slot: AcceptSlot::new(),
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
    pub fn accept(&mut self) -> AcceptFuture<'_> {
        let input_error = self.accept_slot.prepare().err();
        let prepared = input_error.is_none();
        AcceptFuture {
            fd: self.fd.as_raw_fd(),
            slot: &mut self.accept_slot,
            accepted_config: self.accepted_config,
            input_error,
            prepared,
        }
    }
}

impl AsRawFd for SctpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
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
    /// Which SCTP notifications are delivered on the socket.
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
    /// does not report EOR or truncation; use a rich/signaling config and
    /// `recv_msg` when those semantics are required.
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
            notifications: self.notifications,
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
    /// Dropped metadata receive completion that must be adopted before the
    /// next metadata receive can preserve record-boundary state.
    stashed: StashedSctpRecv,
}

impl SctpRecvState {
    const fn new() -> Self {
        Self {
            discarding_tail: false,
            stashed: StashedSctpRecv::empty(),
        }
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
            unsafe { refresh_op_waiter_from_waker(cx, state_ptr) };
            return Poll::Pending;
        }

        let op_ctx = unsafe { completed_op_ctx_from_waker(cx, state_ptr) };
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
/// during internal discard are consumed as control events. An EOR-marked
/// notification tail or a partial-delivery-aborted notification retires the
/// discard state; other notification fragments keep discard active.
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
    /// data or metadata APIs it uses.
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
        Self {
            fd: fd.into(),
            remote_addr,
            recv_state: SctpRecvState::new(),
        }
    }

    /// Takes ownership of a bare SCTP socket descriptor and records its peer.
    ///
    /// Callers supplying an external descriptor are responsible for applying
    /// nonblocking mode and socket options compatible with the data or
    /// metadata APIs they use.
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
        current_local_addr(self.fd.as_raw_fd())
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
    /// association setup instead of changing it per receive.
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

    /// Shuts down the read, write, or both halves of this association socket.
    ///
    /// This is association control-plane work, normally used for teardown or
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

    /// Returns all local addresses currently associated with the stream.
    ///
    /// This relies on a Linux SCTP address-enumeration socket option and may
    /// fail on systems with partial SCTP support. It is status/control-plane
    /// work, not the per-message data fast path.
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        get_assoc_addrs(self.fd.as_raw_fd(), SCTP_GET_LOCAL_ADDRS_OPT, 0)
    }

    /// Returns all peer addresses currently associated with the stream.
    ///
    /// This relies on a Linux SCTP address-enumeration socket option and may
    /// fail on systems with partial SCTP support. It is status/control-plane
    /// work, not the per-message data fast path.
    pub fn peer_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        get_assoc_addrs(self.fd.as_raw_fd(), SCTP_GET_PEER_ADDRS_OPT, 0)
    }

    /// Returns current association status, including the current primary path.
    ///
    /// This is capability-dependent and may be unavailable on kernels with
    /// limited SCTP status support. It is status/control-plane work, not the
    /// per-message data fast path.
    pub fn status(&self) -> io::Result<SctpAssocStatus> {
        let mut raw = SctpStatusRaw::new();
        let mut optlen = std::mem::size_of::<SctpStatusRaw>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_SCTP,
                libc::SCTP_STATUS,
                &mut raw as *mut SctpStatusRaw as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<SctpStatusRaw>() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        raw.to_public()
    }

    /// Returns read-only transport information for one peer address.
    ///
    /// This depends on `SCTP_GET_PEER_ADDR_INFO` support in the running
    /// kernel. It is status/control-plane work, not the per-message data fast
    /// path.
    pub fn peer_addr_info(&self, peer_addr: SocketAddr) -> io::Result<SctpPeerAddrInfo> {
        let mut raw = SctpPaddrInfoRaw::from_address(peer_addr);
        let mut optlen = std::mem::size_of::<SctpPaddrInfoRaw>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_SCTP,
                libc::SCTP_GET_PEER_ADDR_INFO,
                &mut raw as *mut SctpPaddrInfoRaw as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<SctpPaddrInfoRaw>() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        raw.to_public()
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
        let mut raw = SctpAssocValueRaw {
            assoc_id: 0,
            assoc_value: 0,
        };
        let mut optlen = std::mem::size_of::<SctpAssocValueRaw>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_SCTP,
                SCTP_RECONFIG_SUPPORTED_OPT,
                &mut raw as *mut SctpAssocValueRaw as *mut libc::c_void,
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<SctpAssocValueRaw>() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

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
            self.fd.as_raw_fd(),
            libc::IPPROTO_SCTP,
            SCTP_ENABLE_STREAM_RESET_OPT,
            &raw,
        )
    }

    /// Requests a stream reset for one or more SCTP streams.
    ///
    /// This requires SCTP stream-reset support and appropriate association
    /// capabilities on the running kernel. It is control-plane work, not the
    /// per-message data fast path.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::{SctpResetStreams, SctpStream};
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// stream.reset_streams(&SctpResetStreams::outgoing(&[1]))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset_streams(&self, request: &SctpResetStreams) -> io::Result<()> {
        if request.streams.len() > (u16::MAX as usize) {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let header_len = std::mem::size_of::<SctpResetStreamsHeader>();
        let streams_len = std::mem::size_of_val(request.streams.as_slice());
        let total_len = header_len + streams_len;
        let mut buffer = vec![0u8; total_len];
        let header = SctpResetStreamsHeader {
            assoc_id: request.assoc_id,
            flags: request.flags,
            number_streams: request.streams.len() as u16,
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

        let rc = unsafe {
            libc::setsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_SCTP,
                SCTP_RESET_STREAMS_OPT,
                buffer.as_ptr() as *const libc::c_void,
                total_len as libc::socklen_t,
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
            self.fd.as_raw_fd(),
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
                self.fd.as_raw_fd(),
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
        apply_peer_addr_params_raw(self.fd.as_raw_fd(), params)
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
            self.fd.as_raw_fd(),
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
            self.fd.as_raw_fd(),
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
            self.fd.as_raw_fd(),
            libc::IPPROTO_SCTP,
            libc::SCTP_DEFAULT_SNDINFO,
            &raw_sndinfo_from_public(info),
        )
    }

    /// Applies a typed SCTP notification subscription mask.
    ///
    /// This is signaling setup/control-plane work. Data-only fast paths should
    /// use [`SctpSocketConfig::data`] and avoid per-message mask changes.
    pub fn set_notification_mask(&self, mask: SctpNotificationMask) -> io::Result<()> {
        set_sctp_events(self.fd.as_raw_fd(), mask)
    }

    /// Applies association-wide retransmission and RTO policy.
    ///
    /// This is association configuration work, not the per-message data fast
    /// path.
    pub fn apply_assoc_config(&self, config: &SctpAssocConfig) -> io::Result<()> {
        apply_assoc_config_raw(self.fd.as_raw_fd(), *config)
    }

    /// Applies association-wide peer-address defaults.
    ///
    /// `params.address` must be `None`; use [`SctpStream::set_peer_addr_params`]
    /// for path-specific overrides instead.
    ///
    /// This is association configuration work, not the per-message data fast
    /// path.
    pub fn set_default_peer_addr_params(&self, params: SctpPeerAddrParams) -> io::Result<()> {
        apply_default_peer_addr_params(self.fd.as_raw_fd(), params)
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
    /// the provided zero write base publish from their beginning. A zero-byte
    /// completion preserves existing logical contents, and the returned count
    /// is relative to this receive.
    /// This data-only path does not drive metadata receive resynchronization;
    /// do not mix it with `recv_msg` / `recv_msg_vectored` while those paths
    /// are discarding an oversized record tail.
    ///
    /// # Errors
    /// Returns `InvalidInput` if `len` exceeds `buffer.writable_len()`.
    /// Kernel receive errors are returned as `io::Error` values from the
    /// completed operation.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn recv<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> DataRecvFuture<'_, B> {
        let write_base_len = buffer.write_base_len();
        let mut input_error = None;
        let len = match checked_read_len(len, buffer.writable_len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        DataRecvFuture {
            fd: self.fd.as_raw_fd(),
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
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send<B: IoBuffReadOnly>(&mut self, buffer: B) -> DataSendFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_send_len(buffer.len()) {
            Ok(len) => len,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        DataSendFuture {
            fd: self.fd.as_raw_fd(),
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
    /// prefer [`SctpStream::recv`].
    ///
    /// # Errors
    /// Returns `InvalidInput` if `len` is zero or exceeds
    /// `buffer.writable_len()`.
    /// Returns `InvalidData` when the kernel reports a truncated receive or a
    /// non-empty receive that has not reached SCTP end-of-record. An
    /// oversized record returns this error once; later metadata receives
    /// discard the unrecoverable record tail until SCTP end-of-record and then
    /// resume delivery at the next record boundary. A kernel zero-byte
    /// completion with no control message and no flags is clean peer EOF and
    /// resolves as `Ok((0, SctpRecvMeta::Data(SctpRecvInfo::default())))`;
    /// zero-length caller receive requests are rejected before submission so
    /// they cannot masquerade as EOF. Dropping a metadata receive does not
    /// lose record-boundary state: the next metadata receive adopts the
    /// retired completion before submitting its own operation. Notifications
    /// observed during internal discard are consumed as control events. An
    /// EOR-marked notification tail or a partial-delivery-aborted notification
    /// retires the discard state; other notification fragments keep discard
    /// active. Kernel receive errors are returned as `io::Error` values from
    /// the completed operation.
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
        let len = match checked_sctp_metadata_read_len(len, buffer.writable_len()) {
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
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send_msg<B: IoBuffReadOnly>(
        &mut self,
        buffer: B,
        info: SctpSendInfo,
    ) -> SendFuture<'_, B> {
        let mut input_error = None;
        let len = match checked_send_len(buffer.len()) {
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
    /// Notification data must fit within the first segment of the chain.
    /// Use this when both segmentation and SCTP metadata/notifications matter.
    /// For a single contiguous data-only receive, prefer [`SctpStream::recv`].
    ///
    /// # Errors
    /// Returns `InvalidInput` if the chain has no writable bytes. Returns
    /// `InvalidData` when a received SCTP message or notification is
    /// truncated or not complete through SCTP end-of-record. An oversized
    /// record returns this error once; later metadata receives discard the
    /// unrecoverable record tail until SCTP end-of-record and then resume
    /// delivery at the next record boundary. A kernel zero-byte completion
    /// with no control message and no flags is clean peer EOF and resolves as
    /// `Ok((0, SctpRecvMeta::Data(SctpRecvInfo::default())))`; zero-length
    /// caller receive requests are rejected before submission so they cannot
    /// masquerade as EOF. Dropping a metadata receive does not lose
    /// record-boundary state: the next metadata receive adopts the retired
    /// completion before submitting its own operation. Notifications observed
    /// during internal discard are consumed as control events. An EOR-marked
    /// notification tail or a partial-delivery-aborted notification retires
    /// the discard state; other notification fragments keep discard active.
    /// Kernel receive errors are returned as `io::Error` values from the
    /// completed operation.
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn recv_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
    ) -> RecvVectoredFuture<'_, N> {
        let mut buffer = buffer;
        let mut iovecs: [MaybeUninit<libc::iovec>; N] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let (iov_count, writable_len) = fill_recv_vectored_iovecs(&mut buffer, &mut iovecs);
        let input_error = if writable_len == 0 {
            Some(invalid_zero_length_sctp_metadata_recv())
        } else {
            None
        };
        RecvVectoredFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            input_error,
            recv_state: &mut self.recv_state,
            _marker: PhantomData,
        }
    }

    /// Gather-send from a vectored buffer chain with SCTP metadata.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes sent is returned in `Ok`. Empty
    /// chains complete with `Ok(0)` without submitting kernel I/O.
    /// Use this when both segmentation and explicit SCTP metadata matter. For
    /// a single contiguous data-only send, prefer [`SctpStream::send`].
    ///
    /// See [`SctpStream`] for in-flight drop ownership.
    pub fn send_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
        info: SctpSendInfo,
    ) -> SendVectoredFuture<'_, N> {
        let mut iovecs: [MaybeUninit<libc::iovec>; N] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let (iov_count, _) = buffer.fill_write_iovecs_and_len(&mut iovecs);
        SendVectoredFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            sndinfo: raw_sndinfo_from_public(info),
            _marker: PhantomData,
        }
    }
}

impl AsRawFd for SctpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
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

// These retained recvmsg/sendmsg payloads become self-referential after their
// msghdr points at embedded iovec, control, and address storage. Initialize
// those pointers only after submit_retained_sqe moves the payload to its stable
// retained address.
struct RetainedSctpRecvPayload<B: IoBuffReadWrite> {
    /// Caller-owned destination buffer retained while recvmsg is live.
    buffer: B,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Kernel-written peer address storage for the received message.
    addr: MaybeUninit<libc::sockaddr_storage>,
    /// Capacity passed to the kernel for `addr`.
    addrlen: libc::socklen_t,
    /// Control-message storage for SCTP receive metadata.
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSctpSendPayload<B: IoBuffReadOnly> {
    /// Caller-owned source buffer retained while sendmsg is live.
    buffer: B,
    /// Single kernel-facing iovec pointing into `buffer`.
    iovec: MaybeUninit<libc::iovec>,
    /// Control-message storage for SCTP send metadata.
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSctpRecvVectoredPayload<const N: usize> {
    /// Caller-owned destination chain retained while recvmsg is live.
    buffer: IoBuffVecMut<N>,
    /// Kernel-facing iovec array pointing into `buffer` segments.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Kernel-written peer address storage for the received message.
    addr: MaybeUninit<libc::sockaddr_storage>,
    /// Capacity passed to the kernel for `addr`.
    addrlen: libc::socklen_t,
    /// Control-message storage for SCTP receive metadata.
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
}

struct RetainedSctpSendVectoredPayload<const N: usize> {
    /// Caller-owned source chain retained while sendmsg is live.
    buffer: IoBuffVec<N>,
    /// Kernel-facing iovec array pointing into `buffer` segments.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Control-message storage for SCTP send metadata.
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
    /// Message header whose pointers target fields in this retained payload.
    msghdr: MaybeUninit<libc::msghdr>,
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

const ZERO_LENGTH_SCTP_METADATA_RECV: &str = "zero-length SCTP metadata receive request";

#[inline(always)]
fn invalid_zero_length_sctp_metadata_recv() -> io::Error {
    invalid_input(ZERO_LENGTH_SCTP_METADATA_RECV)
}

#[inline(always)]
fn checked_sctp_metadata_read_len(requested: usize, writable: usize) -> io::Result<u32> {
    if requested == 0 {
        return Err(invalid_zero_length_sctp_metadata_recv());
    }
    checked_read_len(requested, writable)
}

#[inline(always)]
fn sctp_msg_end_of_record(msg_flags: libc::c_int) -> bool {
    (msg_flags & libc::MSG_EOR) != 0
}

#[inline(always)]
fn sctp_msg_clean_eof(actual: usize, msg: &libc::msghdr) -> bool {
    actual == 0 && msg.msg_controllen == 0 && msg.msg_flags == 0
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

fn sctp_notification_retires_discard(data_slice: &[u8], msg_flags: libc::c_int) -> bool {
    if !sctp_msg_notification(msg_flags) {
        return false;
    }

    matches!(
        parse_notification(data_slice),
        Ok(SctpRecvMeta::Notification(SctpNotification::PartialDelivery {
            indication,
            ..
        })) if indication == SCTP_PARTIAL_DELIVERY_ABORTED
    )
}

fn sctp_discarding_after_completion(msg: &libc::msghdr, data_slice: &[u8]) -> bool {
    // Linux requeues the truncated SCTP message tail at the receive-queue
    // front. While discarding, the first EOR therefore belongs to that
    // truncated message in normal mode; partial-delivery interleaving is
    // covered by the PDAPI-abort notification retirement path below.
    if sctp_msg_end_of_record(msg.msg_flags) {
        return false;
    }

    if sctp_msg_notification(msg.msg_flags) {
        return !sctp_notification_retires_discard(data_slice, msg.msg_flags);
    }

    true
}

fn update_discarding_after_dropped_completion(
    discarding_tail: &mut bool,
    actual: usize,
    msg: &libc::msghdr,
    data_slice: &[u8],
) {
    if sctp_msg_clean_eof(actual, msg) {
        *discarding_tail = false;
    } else if *discarding_tail {
        *discarding_tail = sctp_discarding_after_completion(msg, data_slice);
    } else if sctp_msg_partial_nonempty(actual, msg.msg_flags) {
        *discarding_tail = true;
    }
}

/// Returns the received prefix visible in the first vectored destination.
///
/// # Safety
///
/// When `iov_count` is nonzero, `iovecs[0]` must be initialized and its base
/// pointer must remain readable for `min(actual, iov_len)` bytes.
unsafe fn sctp_vectored_first_iov_slice<const N: usize>(
    iovecs: &[MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    actual: usize,
) -> &[u8] {
    if iov_count == 0 {
        return &[];
    }

    let first_iov = unsafe { &*(iovecs.as_ptr() as *const libc::iovec) };
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
        let mut payload = unsafe {
            (*pctx.reactor()).take_retained_payload::<RetainedSctpRecvPayload<B>>(state_ptr)
        };
        let msg = unsafe { payload.msghdr.assume_init_ref() };
        let data_slice = unsafe {
            let ptr = payload.buffer.as_mut_ptr();
            std::slice::from_raw_parts(ptr, actual)
        };
        update_discarding_after_dropped_completion(discarding_tail, actual, msg, data_slice);
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
        let payload = unsafe {
            (*pctx.reactor()).take_retained_payload::<RetainedSctpRecvVectoredPayload<N>>(state_ptr)
        };
        let msg = unsafe { payload.msghdr.assume_init_ref() };
        let data_slice =
            unsafe { sctp_vectored_first_iov_slice(&payload.iovecs, iov_count, actual) };
        update_discarding_after_dropped_completion(discarding_tail, actual, msg, data_slice);
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
/// Takes a completed SCTP payload and releases its operation slot.
///
/// # Safety
///
/// A non-null `*state_ptr` must identify a completed FlowIO operation with
/// retained payload type `T`. Cleanup uses its recorded origin reactor.
unsafe fn take_completed_sctp_payload<T: 'static>(
    cx: &mut Context<'_>,
    state_ptr: &mut *mut CompletionState,
) -> Option<(io::Result<usize>, T, bool)> {
    if (*state_ptr).is_null() {
        return None;
    }

    let state = unsafe { &**state_ptr };
    if !state.is_completed() {
        return None;
    }

    let result = sctp_cqe_result(state.result);
    let op_ctx = unsafe { completed_op_ctx_from_waker(cx, *state_ptr) };
    let payload = unsafe { (*op_ctx.reactor()).take_retained_payload::<T>(*state_ptr) };
    unsafe { free_sctp_state(op_ctx.origin_poll_ctx(), state_ptr) };
    Some((result, payload, op_ctx.context_rejected()))
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

        if let Some((result, payload, context_rejected)) = unsafe {
            take_completed_sctp_payload::<RetainedDataRecvPayload<B>>(cx, &mut this.state_ptr)
        } {
            let buffer = payload.buffer;
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

        if let Some((result, payload, context_rejected)) = unsafe {
            take_completed_sctp_payload::<RetainedDataSendPayload<B>>(cx, &mut this.state_ptr)
        } {
            let buffer = payload.buffer;
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

        if let Some((result, payload, context_rejected)) = unsafe {
            take_completed_sctp_payload::<RetainedSctpRecvPayload<B>>(cx, &mut this.state_ptr)
        } {
            let mut payload = payload;
            if context_rejected {
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::NotConnected)),
                    payload.buffer,
                ));
            }
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), payload.buffer)),
            };

            // Parse metadata before taking the buffer — notification parsing
            // needs to read the received bytes via the stable pointer.
            let msg = unsafe { payload.msghdr.assume_init_ref() };
            if sctp_msg_clean_eof(actual, msg) {
                this.recv_state.discarding_tail = false;
                let completed = unsafe {
                    complete_read_with_progress(
                        payload.buffer,
                        this.write_base_len,
                        0,
                        Ok((0, sctp_eof_recv_meta())),
                    )
                };
                return Poll::Ready(completed);
            }

            if this.recv_state.discarding_tail {
                let discard_next = unsafe {
                    let ptr = payload.buffer.as_mut_ptr();
                    let data_slice = std::slice::from_raw_parts(ptr, actual);
                    sctp_discarding_after_completion(msg, data_slice)
                };
                this.recv_state.discarding_tail = discard_next;
                let (_, buffer) = unsafe {
                    complete_read_with_progress(payload.buffer, this.write_base_len, 0, Ok(()))
                };
                // Non-vectored discard has no reusable iovec scratch to refill:
                // the next poll builds a fresh single-iovec payload at the same
                // unchanged caller-visible writable tail.
                this.buffer = Some(buffer);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let data_slice = unsafe {
                let ptr = payload.buffer.as_mut_ptr();
                std::slice::from_raw_parts(ptr, actual)
            };
            let partial_nonempty = sctp_msg_partial_nonempty(actual, msg.msg_flags);
            let meta = parse_recv_meta(
                &payload.control[..],
                msg.msg_controllen,
                msg.msg_flags,
                data_slice,
            );

            if meta.is_err() && partial_nonempty {
                this.recv_state.discarding_tail = true;
            }
            let result = meta.map(|meta| (actual, meta));
            let completed = unsafe {
                complete_read_with_progress(payload.buffer, this.write_base_len, actual, result)
            };
            return Poll::Ready(completed);
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

            let payload = RetainedSctpRecvPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                iovec: MaybeUninit::uninit(),
                addr: MaybeUninit::uninit(),
                addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
                control: [0; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_mut_ptr();
                        payload.iovec.write(libc::iovec {
                            iov_base: ptr as *mut libc::c_void,
                            iov_len: this.len as usize,
                        });
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: payload.addr.as_mut_ptr() as *mut libc::c_void,
                                namelen: payload.addrlen,
                                iov: payload.iovec.as_mut_ptr(),
                                iovlen: 1,
                                control: payload.control.as_mut_ptr() as *mut libc::c_void,
                                controllen: payload.control.len(),
                            },
                        );

                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    free_sctp_state(&pctx, &mut this.state_ptr);
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

        if let Some((result, payload, context_rejected)) = unsafe {
            take_completed_sctp_payload::<RetainedSctpSendPayload<B>>(cx, &mut this.state_ptr)
        } {
            let buffer = payload.buffer;
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

            let payload = RetainedSctpSendPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                iovec: MaybeUninit::uninit(),
                control: [0; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let ptr = payload.buffer.as_ptr();
                        payload.iovec.write(libc::iovec {
                            iov_base: ptr as *mut libc::c_void,
                            iov_len: this.len as usize,
                        });
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: std::ptr::null_mut(),
                                namelen: 0,
                                iov: payload.iovec.as_mut_ptr(),
                                iovlen: 1,
                                control: payload.control.as_mut_ptr() as *mut libc::c_void,
                                controllen: payload.control.len(),
                            },
                        );
                        write_cmsg_sndinfo(&mut payload.control[..], this.sndinfo);

                        Ok(build_sctp_sendmsg_entry(
                            this.fd,
                            payload.msghdr.as_ptr(),
                            state_ptr as u64,
                        ))
                    })
                {
                    free_sctp_state(&pctx, &mut this.state_ptr);
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
    /// Kernel-facing iovec scratch materialized from the receive chain.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Number of initialized entries inside `iovecs`.
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

        if let Some((result, payload, context_rejected)) = unsafe {
            take_completed_sctp_payload::<RetainedSctpRecvVectoredPayload<N>>(
                cx,
                &mut this.state_ptr,
            )
        } {
            if context_rejected {
                return Poll::Ready((
                    Err(io::Error::from(io::ErrorKind::NotConnected)),
                    payload.buffer,
                ));
            }
            let actual = match result {
                Ok(actual) => actual,
                Err(err) => return Poll::Ready((Err(err), payload.buffer)),
            };
            let msg = unsafe { payload.msghdr.assume_init_ref() };
            if sctp_msg_clean_eof(actual, msg) {
                this.recv_state.discarding_tail = false;
                let mut buffer = payload.buffer;
                unsafe {
                    buffer.distribute_written(0);
                }
                return Poll::Ready((Ok((0, sctp_eof_recv_meta())), buffer));
            }

            if this.recv_state.discarding_tail {
                let data_slice = unsafe {
                    sctp_vectored_first_iov_slice(&payload.iovecs, this.iov_count, actual)
                };
                this.recv_state.discarding_tail = sctp_discarding_after_completion(msg, data_slice);
                let mut buffer = payload.buffer;
                unsafe {
                    buffer.distribute_written(0);
                }
                let mut iovecs = unsafe { MaybeUninit::uninit().assume_init() };
                let (iov_count, writable_len) = fill_recv_vectored_iovecs(&mut buffer, &mut iovecs);
                debug_assert_eq!(
                    iov_count, this.iov_count,
                    "SCTP vectored recv discard changed the receive chain shape"
                );
                debug_assert!(
                    writable_len > 0,
                    "SCTP vectored recv discard lost writable capacity"
                );
                this.iovecs = iovecs;
                this.iov_count = iov_count;
                this.buffer = Some(buffer);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let data_slice =
                unsafe { sctp_vectored_first_iov_slice(&payload.iovecs, this.iov_count, actual) };

            let partial_nonempty = sctp_msg_partial_nonempty(actual, msg.msg_flags);
            let meta = parse_recv_meta(
                &payload.control[..],
                msg.msg_controllen,
                msg.msg_flags,
                data_slice,
            );

            let mut buffer = payload.buffer;
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
            let pctx = match unsafe { prepare_initial_sctp_state(cx, &mut this.state_ptr) } {
                Ok(pctx) => pctx,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            let state_ptr = this.state_ptr;

            let empty_iovecs = unsafe { MaybeUninit::uninit().assume_init() };
            let iovecs = std::mem::replace(&mut this.iovecs, empty_iovecs);
            let payload = RetainedSctpRecvVectoredPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                iovecs,
                addr: MaybeUninit::uninit(),
                addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
                control: [0; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: payload.addr.as_mut_ptr() as *mut libc::c_void,
                                namelen: payload.addrlen,
                                iov: payload.iovecs.as_mut_ptr() as *mut libc::iovec,
                                iovlen: this.iov_count,
                                control: payload.control.as_mut_ptr() as *mut libc::c_void,
                                controllen: payload.control.len(),
                            },
                        );

                        Ok(
                            opcode::RecvMsg::new(types::Fd(this.fd), payload.msghdr.as_mut_ptr())
                                .build()
                                .user_data(state_ptr as u64),
                        )
                    })
                {
                    free_sctp_state(&pctx, &mut this.state_ptr);
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
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
    /// Kernel-facing iovec scratch materialized from the send chain.
    iovecs: [MaybeUninit<libc::iovec>; N],
    /// Number of initialized entries inside `iovecs`.
    iov_count: usize,
    /// Public send metadata translated into the kernel ABI layout.
    sndinfo: libc::sctp_sndinfo,
    /// Borrows the parent stream for the future lifetime.
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for SendVectoredFuture<'_, N> {
    type Output = (io::Result<usize>, IoBuffVec<N>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.state_ptr.is_null() && this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some((result, payload, context_rejected)) = unsafe {
            take_completed_sctp_payload::<RetainedSctpSendVectoredPayload<N>>(
                cx,
                &mut this.state_ptr,
            )
        } {
            let buffer = payload.buffer;
            if context_rejected {
                return Poll::Ready((Err(io::Error::from(io::ErrorKind::NotConnected)), buffer));
            }
            return Poll::Ready((result, buffer));
        }

        if this.iov_count == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((validate_local_io_result(cx, Ok(0)), buffer));
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

            let empty_iovecs = unsafe { MaybeUninit::uninit().assume_init() };
            let iovecs = std::mem::replace(&mut this.iovecs, empty_iovecs);
            let payload = RetainedSctpSendVectoredPayload {
                buffer: unsafe { opt_take(&mut this.buffer) },
                iovecs,
                control: [0; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
                msghdr: MaybeUninit::uninit(),
            };
            unsafe {
                if let Err((e, payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        write_msghdr(
                            &mut payload.msghdr,
                            MsgHdrInit {
                                name: std::ptr::null_mut(),
                                namelen: 0,
                                iov: payload.iovecs.as_mut_ptr() as *mut libc::iovec,
                                iovlen: this.iov_count,
                                control: payload.control.as_mut_ptr() as *mut libc::c_void,
                                controllen: payload.control.len(),
                            },
                        );
                        write_cmsg_sndinfo(&mut payload.control[..], this.sndinfo);

                        Ok(build_sctp_sendmsg_entry(
                            this.fd,
                            payload.msghdr.as_ptr(),
                            state_ptr as u64,
                        ))
                    })
                {
                    free_sctp_state(&pctx, &mut this.state_ptr);
                    return Poll::Ready((Err(e), payload.buffer));
                }
            }
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

#[doc(hidden)]
pub struct AcceptFuture<'a> {
    /// Listening socket descriptor used for the accept submission.
    fd: RawFd,
    /// Borrowed reusable accept slot owned by the listener.
    slot: &'a mut AcceptSlot,
    /// Socket configuration to apply to the accepted association after accept.
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

        if !this.slot.state_ptr.is_null() {
            let state = unsafe { &*this.slot.state_ptr };
            if state.is_completed() {
                let result = state.result;
                let op_ctx = unsafe { completed_op_ctx_from_waker(cx, this.slot.state_ptr) };
                let payload = unsafe {
                    (*op_ctx.reactor())
                        .take_retained_payload::<RetainedAcceptAddr>(this.slot.state_ptr)
                };
                unsafe { (*op_ctx.reactor()).free_op(this.slot.state_ptr) };
                this.slot.state_ptr = std::ptr::null_mut();
                this.slot.in_use = false;

                if op_ctx.context_rejected() {
                    if result >= 0 {
                        close_fd(result as RawFd);
                    }
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
                if result < 0 {
                    return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                }

                // SAFETY: a successful accept CQE transfers one new descriptor
                // to this slot. No other owner exists, and all later error
                // paths let `OwnedFd` close it exactly once.
                let accepted_fd = unsafe { OwnedFd::from_raw_fd(result as RawFd) };
                return Poll::Ready(finish_accepted_stream(
                    accepted_fd,
                    &payload.addr,
                    payload.addrlen,
                    this.accepted_config,
                ));
            }
        }

        if this.slot.state_ptr.is_null() {
            let pctx = match poll_ctx_from_waker(cx) {
                Ok(pctx) => pctx,
                Err(err) => {
                    this.slot.in_use = false;
                    return Poll::Ready(Err(err));
                }
            };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null() {
                this.slot.in_use = false;
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.slot.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let payload = RetainedAcceptAddr::new();

            unsafe {
                (*state_ptr).set_close_result_fd_on_orphan();
                if let Err((e, _payload)) =
                    submit_retained_sqe(&pctx, state_ptr, payload, |payload| {
                        let sqe = opcode::Accept::new(
                            types::Fd(this.fd),
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
                    this.slot.state_ptr = std::ptr::null_mut();
                    this.slot.in_use = false;
                    return Poll::Ready(Err(e));
                }
            }
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.slot.state_ptr) };
        Poll::Pending
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
                let op_ctx = unsafe { completed_op_ctx_from_waker(cx, this.slot.state_ptr) };
                unsafe { (*op_ctx.reactor()).free_op(this.slot.state_ptr) };
                this.slot.state_ptr = std::ptr::null_mut();
                this.slot.in_use = false;

                if op_ctx.context_rejected() {
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
                }
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(err));
                }

                if let Err(err) =
                    apply_sctp_connect_established_config(this.slot.fd, this.slot.connected_config)
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
        }

        unsafe { refresh_op_waiter_from_waker(cx, this.slot.state_ptr) };
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

/// Connect future with a relative timeout for a reusable [`SctpConnector`].
#[doc(hidden)]
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
    debug_assert!(std::mem::size_of::<T>() <= N);
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

fn apply_sctp_connected_socket_config(fd: RawFd, config: SctpSocketConfig) -> io::Result<()> {
    apply_sctp_socket_options(fd, config.socket_options())?;
    if let Some(assoc) = config.assoc {
        apply_assoc_config_raw(fd, assoc)?;
    }
    if let Some(params) = config.default_peer_addr_params {
        apply_default_peer_addr_params(fd, params)?;
    }
    Ok(())
}

fn apply_sctp_connect_established_config(fd: RawFd, config: SctpSocketConfig) -> io::Result<()> {
    if let Some(assoc) = config.assoc {
        apply_assoc_config_raw(fd, assoc)?;
    }
    if let Some(params) = config.default_peer_addr_params {
        apply_default_peer_addr_params(fd, params)?;
    }
    Ok(())
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
    /// Number of packed addresses returned, or required capacity on retry.
    addr_num: u32,
}

const MAX_SCTP_ASSOC_ADDRS: usize = 1024;

fn checked_assoc_addr_count(addr_count: usize) -> io::Result<usize> {
    if addr_count > MAX_SCTP_ASSOC_ADDRS {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(addr_count)
}

fn assoc_addrs_buffer_len(
    capacity: usize,
    header_len: usize,
    storage_len: usize,
) -> io::Result<usize> {
    if capacity > MAX_SCTP_ASSOC_ADDRS {
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

fn get_assoc_addrs(
    fd: RawFd,
    optname: libc::c_int,
    assoc_id: libc::sctp_assoc_t,
) -> io::Result<Vec<SocketAddr>> {
    const INITIAL_CAPACITY: usize = 8;

    let mut capacity = INITIAL_CAPACITY;
    loop {
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

        let mut optlen = total_len as libc::socklen_t;
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

        if (optlen as usize) < header_len {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let header =
            unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpGetAddrsHeader) };
        let addr_count = checked_assoc_addr_count(header.addr_num as usize)?;
        if addr_count > capacity {
            capacity = addr_count;
            continue;
        }

        let used_len = optlen as usize;
        let payload = &buffer[header_len..used_len];
        return parse_assoc_addrs(payload, addr_count, storage_len)
            .map_err(|err| io::Error::from(err.kind()));
    }
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
    match family {
        libc::AF_INET => Ok(std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t),
        libc::AF_INET6 => Ok(std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t),
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
/// The parser accepts dense, compact IPv4, and `sockaddr_storage`-padded
/// entries, and succeeds only when the full payload is consumed.
pub(crate) fn parse_assoc_addrs(
    payload: &[u8],
    addr_count: usize,
    storage_len: usize,
) -> io::Result<Vec<SocketAddr>> {
    let mut addrs = Vec::with_capacity(addr_count);
    if parse_assoc_addrs_iter(payload, addr_count, storage_len, &mut addrs) {
        return Ok(addrs);
    }

    Err(io::Error::from(io::ErrorKind::InvalidData))
}

fn byte_range_invalid_data(err: BufferRangeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[derive(Clone, Copy)]
/// One iterative backtracking frame for packed association-address parsing.
struct AssocAddrParseFrame {
    /// Byte offset of the next address in the packed payload.
    offset: usize,
    /// Number of addresses still required from this state.
    remaining: usize,
    /// Next family-compatible entry layout to try at this offset.
    next_candidate: usize,
}

fn pop_assoc_addr_parse_frame(
    frames: &mut Vec<AssocAddrParseFrame>,
    addrs: &mut Vec<SocketAddr>,
    failed_states: &mut Vec<(usize, usize)>,
) -> bool {
    if let Some(frame) = frames.pop()
        && !assoc_addr_parse_state_failed(failed_states, frame.offset, frame.remaining)
    {
        failed_states.push((frame.offset, frame.remaining));
    }
    if frames.len() <= addrs.len() {
        let _ = addrs.pop();
    }
    !frames.is_empty()
}

fn assoc_addr_parse_state_failed(
    failed_states: &[(usize, usize)],
    offset: usize,
    remaining: usize,
) -> bool {
    failed_states
        .iter()
        .any(|&(failed_offset, failed_remaining)| {
            failed_offset == offset && failed_remaining == remaining
        })
}

fn assoc_addr_remaining_min_len(remaining: usize) -> usize {
    const MIN_ASSOC_ADDR_ENTRY_LEN: usize = 8;

    remaining.saturating_mul(MIN_ASSOC_ADDR_ENTRY_LEN)
}

fn parse_assoc_addrs_iter(
    payload: &[u8],
    remaining: usize,
    storage_len: usize,
    addrs: &mut Vec<SocketAddr>,
) -> bool {
    let mut frames = Vec::with_capacity(remaining.saturating_add(1));
    let mut failed_states = Vec::with_capacity(remaining.saturating_add(1));
    frames.push(AssocAddrParseFrame {
        offset: 0,
        remaining,
        next_candidate: 0,
    });

    while !frames.is_empty() {
        let frame_index = frames.len() - 1;
        let frame = frames[frame_index];
        if frame.remaining == 0 {
            if frame.offset == payload.len() {
                return true;
            }
            if !pop_assoc_addr_parse_frame(&mut frames, addrs, &mut failed_states) {
                return false;
            }
            continue;
        }

        let current = &payload[frame.offset..];
        if current.len() < std::mem::size_of::<libc::sa_family_t>() {
            if !pop_assoc_addr_parse_frame(&mut frames, addrs, &mut failed_states) {
                return false;
            }
            continue;
        }

        let Ok(family) = read_u16_at(current, 0) else {
            if !pop_assoc_addr_parse_frame(&mut frames, addrs, &mut failed_states) {
                return false;
            }
            continue;
        };
        let family = family as libc::sa_family_t as libc::c_int;
        let candidates = assoc_addr_candidates(family, storage_len);
        let mut advanced = false;

        while frames[frame_index].next_candidate < candidates.len() {
            let candidate = candidates[frames[frame_index].next_candidate];
            frames[frame_index].next_candidate += 1;
            if current.len() < candidate.entry_len {
                continue;
            }
            let next_offset = frame.offset + candidate.entry_len;
            let next_remaining = frame.remaining - 1;
            if payload.len() - next_offset < assoc_addr_remaining_min_len(next_remaining) {
                continue;
            }
            if assoc_addr_parse_state_failed(&failed_states, next_offset, next_remaining) {
                continue;
            }

            let Ok(addr) = parse_assoc_addr_entry(&current[..candidate.addr_len], family) else {
                continue;
            };
            addrs.push(addr);
            frames.push(AssocAddrParseFrame {
                offset: next_offset,
                remaining: next_remaining,
                next_candidate: 0,
            });
            advanced = true;
            break;
        }

        if !advanced && !pop_assoc_addr_parse_frame(&mut frames, addrs, &mut failed_states) {
            return false;
        }
    }

    false
}

#[derive(Clone, Copy)]
/// Candidate packed layout for one association address.
struct AssocAddrCandidate {
    /// Total payload bytes consumed, including any trailing storage padding.
    entry_len: usize,
    /// Leading bytes interpreted as the concrete socket address.
    addr_len: usize,
}

fn assoc_addr_candidates(family: libc::c_int, storage_len: usize) -> &'static [AssocAddrCandidate] {
    const IPV4_DENSE: usize = std::mem::size_of::<libc::sockaddr_in>();
    const IPV4_COMPACT: usize = 8;
    const IPV6_DENSE: usize = std::mem::size_of::<libc::sockaddr_in6>();

    const IPV4_BASE: [AssocAddrCandidate; 2] = [
        AssocAddrCandidate {
            entry_len: IPV4_DENSE,
            addr_len: IPV4_DENSE,
        },
        AssocAddrCandidate {
            entry_len: IPV4_COMPACT,
            addr_len: IPV4_COMPACT,
        },
    ];
    const IPV6_BASE: [AssocAddrCandidate; 1] = [AssocAddrCandidate {
        entry_len: IPV6_DENSE,
        addr_len: IPV6_DENSE,
    }];

    const IPV4_PADDED: [AssocAddrCandidate; 3] = [
        AssocAddrCandidate {
            entry_len: IPV4_DENSE,
            addr_len: IPV4_DENSE,
        },
        AssocAddrCandidate {
            entry_len: IPV4_COMPACT,
            addr_len: IPV4_COMPACT,
        },
        AssocAddrCandidate {
            entry_len: std::mem::size_of::<libc::sockaddr_storage>(),
            addr_len: IPV4_DENSE,
        },
    ];
    const IPV6_PADDED: [AssocAddrCandidate; 2] = [
        AssocAddrCandidate {
            entry_len: IPV6_DENSE,
            addr_len: IPV6_DENSE,
        },
        AssocAddrCandidate {
            entry_len: std::mem::size_of::<libc::sockaddr_storage>(),
            addr_len: IPV6_DENSE,
        },
    ];

    match family {
        libc::AF_INET => {
            if storage_len == std::mem::size_of::<libc::sockaddr_storage>() {
                &IPV4_PADDED
            } else {
                &IPV4_BASE
            }
        }
        libc::AF_INET6 => {
            if storage_len == std::mem::size_of::<libc::sockaddr_storage>() {
                &IPV6_PADDED
            } else {
                &IPV6_BASE
            }
        }
        _ => &[],
    }
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

pub(crate) fn parse_recv_meta(
    control: &[u8],
    controllen: usize,
    msg_flags: libc::c_int,
    data_slice: &[u8],
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
        return parse_notification(data_slice);
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

    /// Verifies dropping an accept future closes an already-completed accepted
    /// descriptor and releases its reusable slot.
    pub fn test_accept_slot_drop_future_closes_completed_fd() -> io::Result<()> {
        let fd = crate::runtime::fd::distinctive_closeable_test_fd()?;
        let mut state = CompletionState::empty();
        state.result = fd;
        state.set_completed();

        let mut slot = AcceptSlot::new();
        slot.in_use = true;
        slot.state_ptr = &mut state;

        slot.drop_future();

        if !slot.state_ptr.is_null() || slot.in_use {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        if !crate::runtime::fd::raw_fd_is_closed(fd) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        Ok(())
    }

    /// Verifies listener teardown closes a completed descriptor left in a
    /// cached accept slot by a forgotten future.
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

    #[test]
    fn sctp_partial_delivery_abort_notification_retires_discard() {
        // Linux UAPI defines SCTP_PARTIAL_DELIVERY_ABORTED as 0. A real
        // kernel PDAPI abort is not deterministic to force on loopback, so the
        // discard decision is pinned with a synthetic Linux-layout event.
        assert_eq!(SCTP_PARTIAL_DELIVERY_ABORTED, 0);

        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(sctp_notification_retires_discard(&data, msg.msg_flags));
        assert!(!sctp_discarding_after_completion(&msg, &data));

        let mut discarding_tail = true;
        update_discarding_after_dropped_completion(&mut discarding_tail, data.len(), &msg, &data);
        assert!(!discarding_tail);
    }

    #[test]
    fn sctp_non_abort_notification_without_eor_keeps_discard_active() {
        let data = test_partial_delivery_notification(SCTP_PARTIAL_DELIVERY_ABORTED + 1);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(!sctp_notification_retires_discard(&data, msg.msg_flags));
        assert!(sctp_discarding_after_completion(&msg, &data));

        let mut discarding_tail = true;
        update_discarding_after_dropped_completion(&mut discarding_tail, data.len(), &msg, &data);
        assert!(discarding_tail);
    }

    #[test]
    fn sctp_dropped_partial_notification_starts_discard() {
        let data = test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 8);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION);

        assert!(sctp_msg_partial_nonempty(data.len(), msg.msg_flags));
        let mut discarding_tail = false;
        update_discarding_after_dropped_completion(&mut discarding_tail, data.len(), &msg, &data);
        assert!(discarding_tail);
    }

    #[test]
    fn sctp_notification_eor_tail_retires_discard() {
        let data = test_notification_buffer(LOCAL_SCTP_ASSOC_CHANGE, 20);
        let msg = test_msghdr_with_flags(libc::MSG_NOTIFICATION | libc::MSG_EOR);

        assert!(!sctp_notification_retires_discard(&data, msg.msg_flags));
        assert!(!sctp_discarding_after_completion(&msg, &data));

        let mut discarding_tail = true;
        update_discarding_after_dropped_completion(&mut discarding_tail, data.len(), &msg, &data);
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
    fn assoc_addr_count_rejects_kernel_over_cap() {
        let err = checked_assoc_addr_count(MAX_SCTP_ASSOC_ADDRS + 1)
            .expect_err("over-cap addr count should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn assoc_addrs_buffer_len_rejects_over_cap_and_overflow() {
        let over_cap = assoc_addrs_buffer_len(
            MAX_SCTP_ASSOC_ADDRS + 1,
            std::mem::size_of::<SctpGetAddrsHeader>(),
            std::mem::size_of::<libc::sockaddr_storage>(),
        )
        .expect_err("over-cap buffer should fail");
        assert_eq!(over_cap.kind(), io::ErrorKind::InvalidData);

        let overflow = assoc_addrs_buffer_len(
            MAX_SCTP_ASSOC_ADDRS,
            1,
            usize::MAX / MAX_SCTP_ASSOC_ADDRS + 1,
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
    fn parse_assoc_addrs_accepts_empty_zero_count_payload() {
        let parsed = parse_assoc_addrs(&[], 0, std::mem::size_of::<libc::sockaddr_storage>())
            .expect("zero-count parse should succeed");

        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_assoc_addrs_backtracks_from_dense_to_compact_ipv4() {
        let compact_len = 8;
        let dense_len = std::mem::size_of::<libc::sockaddr_in>();
        let mut payload = assoc_ipv4_entry([1, 2, 3, 4], 1111, compact_len);
        payload.extend_from_slice(&assoc_ipv4_entry([5, 6, 7, 8], 2222, dense_len));

        let parsed = parse_assoc_addrs(&payload, 2, dense_len).expect("IPv4 parse failed");

        assert_eq!(
            parsed,
            vec![
                SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 1111)),
                SocketAddr::from((Ipv4Addr::new(5, 6, 7, 8), 2222)),
            ]
        );
    }

    #[test]
    fn parse_assoc_addrs_accepts_storage_padded_ipv4_and_ipv6() {
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        let ipv6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let mut payload = assoc_ipv4_entry([192, 0, 2, 10], 1234, storage_len);
        payload.extend_from_slice(&assoc_ipv6_entry(ipv6, 4321, 7, 9, storage_len));

        let parsed =
            parse_assoc_addrs(&payload, 2, storage_len).expect("padded address parse failed");

        assert_eq!(
            parsed,
            vec![
                SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 1234)),
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(ipv6), 4321, 7, 9)),
            ]
        );
    }

    #[test]
    fn parse_assoc_addrs_rejects_count_payload_mismatch() {
        let payload = assoc_ipv4_entry([10, 0, 0, 1], 80, 8);

        let missing_second = parse_assoc_addrs(&payload, 2, 8)
            .expect_err("short payload for declared count should fail");
        assert_eq!(missing_second.kind(), io::ErrorKind::InvalidData);

        let extra_payload = parse_assoc_addrs(&payload, 0, 8)
            .expect_err("non-empty payload with zero count should fail");
        assert_eq!(extra_payload.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_assoc_addrs_rejects_unsupported_family_and_truncation() {
        let mut unsupported = vec![0u8; 8];
        write_u16_ne(&mut unsupported, 0, libc::AF_UNIX as u16);
        let err =
            parse_assoc_addrs(&unsupported, 1, 8).expect_err("unsupported family should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let truncated = parse_assoc_addrs(&[0], 1, 8).expect_err("truncated family should fail");
        assert_eq!(truncated.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_assoc_addrs_handles_max_count_without_recursion() {
        let mut payload = Vec::with_capacity(MAX_SCTP_ASSOC_ADDRS * 8);
        for index in 0..MAX_SCTP_ASSOC_ADDRS {
            payload.extend_from_slice(&assoc_ipv4_entry(
                [10, 0, (index / 256) as u8, index as u8],
                1000 + (index % 1000) as u16,
                8,
            ));
        }

        let parsed =
            parse_assoc_addrs(&payload, MAX_SCTP_ASSOC_ADDRS, 8).expect("max-count parse failed");

        assert_eq!(parsed.len(), MAX_SCTP_ASSOC_ADDRS);
        assert_eq!(
            parsed[0],
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 0), 1000))
        );
        assert_eq!(
            parsed[MAX_SCTP_ASSOC_ADDRS - 1],
            SocketAddr::from((Ipv4Addr::new(10, 0, 3, 255), 1023))
        );
    }
}
