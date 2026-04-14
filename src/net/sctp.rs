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
//! - [`SctpStream::set_primary_addr`]
//! - [`SctpStream::set_peer_primary_addr`]
//! - [`SctpStream::status`]
//! - [`SctpStream::peer_addr_info`]
//! - [`SctpStream::primary_path_info`]
//! - [`SctpStream::reconfig_supported`]
//! - [`SctpStream::enable_stream_reset`]
//! - [`SctpStream::reset_streams`]
//! - [`SctpStream::add_streams`]
//!
//! # Example
//! ```no_run
//! use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpListener, SctpSendInfo};
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
//!     let _ = Executor::spawn(async move {
//!         let (mut stream, _remote) = listener.accept().await.unwrap();
//!         let (res, _buf) = stream.recv_msg(vec![0u8; 256], 256).await;
//!         res.unwrap();
//!     });
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
//!     let _ = connector
//!         .connect_timeout(
//!             SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
//!             Duration::from_secs(1),
//!         )
//!         .unwrap()
//!         .await;
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
//!     let _ = Executor::spawn(async move {
//!         let (mut stream, _remote) = listener.accept().await.unwrap();
//!         let recv = IoBuffVecMut::<2>::from_array([
//!             IoBuffMut::new(0, 5, 0).unwrap(),
//!             IoBuffMut::new(0, 6, 0).unwrap(),
//!         ]);
//!         let (res, _chain) = stream.recv_msg_vectored(recv).await;
//!         let (_len, _meta) = res.unwrap();
//!     });
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
    checked_read_len, close_fd, close_if_valid, current_local_addr, set_reuse_addr, set_sock_opt,
    socket_addr_from_c, socket_addr_to_c, socket_domain,
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
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

#[repr(C)]
struct SctpEventSubscribe
{
    sctp_data_io_event: u8,
    sctp_association_event: u8,
    sctp_address_event: u8,
    sctp_send_failure_event: u8,
    sctp_peer_error_event: u8,
    sctp_shutdown_event: u8,
    sctp_partial_delivery_event: u8,
    sctp_adaptation_layer_event: u8,
    sctp_authentication_event: u8,
    sctp_sender_dry_event: u8,
    sctp_stream_reset_event: u8,
    sctp_assoc_reset_event: u8,
    sctp_stream_change_event: u8,
    sctp_send_failure_event_event: u8,
}

impl SctpEventSubscribe
{
    const fn from_mask(mask: SctpNotificationMask, recv_rcvinfo: bool) -> Self
    {
        Self {
            sctp_data_io_event: recv_rcvinfo as u8,
            sctp_association_event: mask.association as u8,
            sctp_address_event: mask.address as u8,
            sctp_send_failure_event: mask.send_failure as u8,
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
/// This is passed to [`SctpStream::send_msg`] to control stream selection,
/// PPID, flags, and association-scoped metadata.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpSendInfo
{
    /// SCTP stream number to send on.
    pub stream_id: u16,
    /// Send flags (e.g. `SCTP_UNORDERED`).
    pub flags: u16,
    /// Payload Protocol Identifier (network byte order in kernel, host here).
    pub ppid: u32,
    /// Opaque context value returned in send-failed notifications.
    pub context: u32,
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
}

/// Per-message SCTP receive metadata extracted from `SCTP_RCVINFO`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpRecvInfo
{
    /// SCTP stream number the message was received on.
    pub stream_id: u16,
    /// Stream Sequence Number.
    pub ssn: u16,
    /// Receive flags.
    pub flags: u16,
    /// Payload Protocol Identifier.
    pub ppid: u32,
    /// Transmission Sequence Number.
    pub tsn: u32,
    /// Cumulative TSN.
    pub cumtsn: u32,
    /// Opaque context value.
    pub context: u32,
    /// Association ID.
    pub assoc_id: libc::sctp_assoc_t,
}

/// Per-peer-address SCTP parameters used by `SCTP_PEER_ADDR_PARAMS`.
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
pub struct SctpPeerAddrParams
{
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpAssocConfig
{
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpReconfigFlags
{
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Bitmask of `RESET_STREAMS`, `RESET_ASSOC`, `CHANGE_ASSOC`.
    pub flags: u32,
}

impl SctpReconfigFlags
{
    /// Enables stream reset requests on the association.
    pub const RESET_STREAMS: u32 = SCTP_ENABLE_RESET_STREAM_REQ;
    /// Enables association reset requests on the association.
    pub const RESET_ASSOC: u32 = SCTP_ENABLE_RESET_ASSOC_REQ;
    /// Enables association stream-count changes on the association.
    pub const CHANGE_ASSOC: u32 = SCTP_ENABLE_CHANGE_ASSOC_REQ;

    /// Creates an empty association-wide flag block.
    pub const fn association_default() -> Self
    {
        Self {
            assoc_id: 0,
            flags: 0,
        }
    }
}

/// Request parameters for `SCTP_RESET_STREAMS`.
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
pub struct SctpResetStreams
{
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Direction flags (incoming, outgoing, or both).
    pub flags: u16,
    /// Stream numbers to reset.
    pub streams: Vec<u16>,
}

impl SctpResetStreams
{
    /// Resets the specified incoming streams.
    pub fn incoming(streams: &[u16]) -> Self
    {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING,
            streams: streams.to_vec(),
        }
    }

    /// Resets the specified outgoing streams.
    pub fn outgoing(streams: &[u16]) -> Self
    {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_OUTGOING,
            streams: streams.to_vec(),
        }
    }

    /// Resets the specified incoming and outgoing streams.
    pub fn bidirectional(streams: &[u16]) -> Self
    {
        Self {
            assoc_id: 0,
            flags: SCTP_STREAM_RESET_INCOMING | SCTP_STREAM_RESET_OUTGOING,
            streams: streams.to_vec(),
        }
    }
}

/// Request parameters for `SCTP_ADD_STREAMS`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpAddStreams
{
    /// Association ID (0 for the default association).
    pub assoc_id: libc::sctp_assoc_t,
    /// Number of inbound streams to add.
    pub inbound_streams: u16,
    /// Number of outbound streams to add.
    pub outbound_streams: u16,
}

impl SctpAddStreams
{
    /// Requests additional inbound and outbound streams for the association.
    pub const fn new(inbound_streams: u16, outbound_streams: u16) -> Self
    {
        Self {
            assoc_id: 0,
            inbound_streams,
            outbound_streams,
        }
    }
}

impl SctpPeerAddrParams
{
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
    pub const fn association_default() -> Self
    {
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
    pub const fn for_address(address: SocketAddr) -> Self
    {
        Self {
            address: Some(address),
            ..Self::association_default()
        }
    }
}

/// Read-only per-path SCTP state returned by `SCTP_GET_PEER_ADDR_INFO`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpPeerAddrInfo
{
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

impl SctpPeerAddrInfo
{
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpAssocStatus
{
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

impl SctpAssocStatus
{
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

/// Decoded SCTP notifications returned by [`SctpStream::recv_msg`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SctpNotification
{
    /// Association state change such as up/down/restart.
    AssocChange
    {
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
    Shutdown
    {
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Per-path reachability or state change notification.
    PeerAddrChange
    {
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
    RemoteError
    {
        /// SCTP error code from the peer.
        error: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// A send failed and the kernel returned the original send metadata.
    SendFailed
    {
        /// Kernel error code for the failed send.
        error: u32,
        /// Original send metadata supplied with the failed message.
        info: SctpSendInfo,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Adaptation layer indication notification.
    Adaptation
    {
        /// Adaptation indication value from the peer/kernel.
        indication: u32,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Partial-delivery state change notification.
    PartialDelivery
    {
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
    SenderDry
    {
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Stream reset completion or state change notification.
    StreamReset
    {
        /// Kernel flags for the reset event.
        flags: u16,
        /// Association identifier reported by the kernel.
        assoc_id: libc::sctp_assoc_t,
    },
    /// Association reset notification with TSN restart points.
    AssocReset
    {
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
    StreamChange
    {
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
    Other
    {
        /// Raw SCTP notification type.
        kind: u16,
        /// Raw notification flags.
        flags: u16,
        /// Raw notification length.
        length: u32,
    },
}

/// Coarse notification category used by transport consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SctpNotificationKind
{
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

impl SctpNotification
{
    /// Returns the notification kind without exposing payload details.
    pub const fn kind(&self) -> SctpNotificationKind
    {
        match self
        {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SctpRecvMeta
{
    /// Regular user data was received and the contained metadata came from
    /// `SCTP_RCVINFO`.
    Data(SctpRecvInfo),
    /// An SCTP notification was received instead of user data.
    Notification(SctpNotification),
}

impl SctpRecvMeta
{
    /// Returns `true` when the receive completed with user data.
    pub const fn is_data(&self) -> bool
    {
        matches!(self, Self::Data(_))
    }

    /// Returns `true` when the receive completed with an SCTP notification.
    pub const fn is_notification(&self) -> bool
    {
        matches!(self, Self::Notification(_))
    }

    /// Returns a shared reference to the receive data metadata, if present.
    pub const fn data(&self) -> Option<&SctpRecvInfo>
    {
        match self
        {
            Self::Data(info) => Some(info),
            Self::Notification(_) => None,
        }
    }

    /// Returns a shared reference to the SCTP notification, if present.
    pub const fn notification(&self) -> Option<&SctpNotification>
    {
        match self
        {
            Self::Data(_) => None,
            Self::Notification(notification) => Some(notification),
        }
    }

    /// Extracts the receive data metadata, if present.
    pub const fn into_data(self) -> Option<SctpRecvInfo>
    {
        match self
        {
            Self::Data(info) => Some(info),
            Self::Notification(_) => None,
        }
    }

    /// Extracts the SCTP notification, if present.
    pub const fn into_notification(self) -> Option<SctpNotification>
    {
        match self
        {
            Self::Data(_) => None,
            Self::Notification(notification) => Some(notification),
        }
    }
}

/// Typed SCTP notification subscription policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpNotificationMask
{
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

impl SctpNotificationMask
{
    pub const fn none() -> Self
    {
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

    pub const fn all() -> Self
    {
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

    /// Notification set matching the crate's current signaling-oriented rich
    /// receive behavior.
    pub const fn signaling_default() -> Self
    {
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

impl Default for SctpNotificationMask
{
    fn default() -> Self
    {
        Self::signaling_default()
    }
}

#[repr(C, packed(4))]
struct SctpPrimRaw
{
    assoc_id: libc::sctp_assoc_t,
    addr: libc::sockaddr_storage,
}

#[repr(C, packed(4))]
struct SctpSetPeerPrimRaw
{
    assoc_id: libc::sctp_assoc_t,
    addr: libc::sockaddr_storage,
}

#[repr(C, packed(4))]
struct SctpPaddrParamsRaw
{
    assoc_id: libc::sctp_assoc_t,
    address: libc::sockaddr_storage,
    heartbeat_interval_ms: u32,
    path_max_retransmits: u16,
    path_mtu: u32,
    sack_delay_ms: u32,
    flags: u32,
    ipv6_flow_label: u32,
    dscp: u8,
}

#[repr(C)]
struct SctpAssocValueRaw
{
    assoc_id: libc::sctp_assoc_t,
    assoc_value: u32,
}

#[repr(C, packed(4))]
struct SctpAssocParamsRaw
{
    assoc_id: libc::sctp_assoc_t,
    assoc_max_retrans: u16,
    peer_destinations: u16,
    peer_receiver_window: u32,
    local_receiver_window: u32,
    cookie_life_ms: u32,
}

impl SctpAssocParamsRaw
{
    fn get(fd: RawFd) -> io::Result<Self>
    {
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<Self>()
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(raw)
    }
}

#[repr(C, packed(4))]
struct SctpRtoInfoRaw
{
    assoc_id: libc::sctp_assoc_t,
    rto_initial_ms: u32,
    rto_max_ms: u32,
    rto_min_ms: u32,
}

impl SctpRtoInfoRaw
{
    fn get(fd: RawFd) -> io::Result<Self>
    {
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<Self>()
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(raw)
    }
}

#[repr(C)]
struct SctpResetStreamsHeader
{
    assoc_id: libc::sctp_assoc_t,
    flags: u16,
    number_streams: u16,
}

#[repr(C)]
struct SctpAddStreamsRaw
{
    assoc_id: libc::sctp_assoc_t,
    inbound_streams: u16,
    outbound_streams: u16,
}

impl SctpPaddrParamsRaw
{
    fn from_public(params: SctpPeerAddrParams) -> Self
    {
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

    fn to_public(&self) -> io::Result<SctpPeerAddrParams>
    {
        Ok(SctpPeerAddrParams {
            assoc_id: unsafe { std::ptr::addr_of!(self.assoc_id).read_unaligned() },
            address: storage_to_option_socket_addr(unsafe {
                std::ptr::addr_of!(self.address).read_unaligned()
            })?,
            heartbeat_interval_ms: unsafe {
                std::ptr::addr_of!(self.heartbeat_interval_ms).read_unaligned()
            },
            path_max_retransmits: unsafe {
                std::ptr::addr_of!(self.path_max_retransmits).read_unaligned()
            },
            path_mtu: unsafe { std::ptr::addr_of!(self.path_mtu).read_unaligned() },
            sack_delay_ms: unsafe { std::ptr::addr_of!(self.sack_delay_ms).read_unaligned() },
            flags: unsafe { std::ptr::addr_of!(self.flags).read_unaligned() },
            ipv6_flow_label: unsafe { std::ptr::addr_of!(self.ipv6_flow_label).read_unaligned() },
            dscp: unsafe { std::ptr::addr_of!(self.dscp).read_unaligned() },
        })
    }
}

#[repr(C, packed(4))]
struct SctpPaddrInfoRaw
{
    assoc_id: libc::sctp_assoc_t,
    address: libc::sockaddr_storage,
    state: i32,
    congestion_window: u32,
    srtt: u32,
    rto: u32,
    mtu: u32,
}

impl SctpPaddrInfoRaw
{
    fn from_address(address: SocketAddr) -> Self
    {
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

    fn to_public(&self) -> io::Result<SctpPeerAddrInfo>
    {
        Ok(SctpPeerAddrInfo {
            assoc_id: unsafe { std::ptr::addr_of!(self.assoc_id).read_unaligned() },
            address: socket_addr_from_c(
                &unsafe { std::ptr::addr_of!(self.address).read_unaligned() },
                sockaddr_len_for_storage(unsafe {
                    std::ptr::addr_of!(self.address).read_unaligned()
                })?,
            )?,
            state: unsafe { std::ptr::addr_of!(self.state).read_unaligned() },
            congestion_window: unsafe {
                std::ptr::addr_of!(self.congestion_window).read_unaligned()
            },
            srtt: unsafe { std::ptr::addr_of!(self.srtt).read_unaligned() },
            rto: unsafe { std::ptr::addr_of!(self.rto).read_unaligned() },
            mtu: unsafe { std::ptr::addr_of!(self.mtu).read_unaligned() },
        })
    }
}

#[repr(C, packed(4))]
struct SctpStatusRaw
{
    assoc_id: libc::sctp_assoc_t,
    state: i32,
    receiver_window: u32,
    unacked_data_chunks: u16,
    pending_data_chunks: u16,
    inbound_streams: u16,
    outbound_streams: u16,
    fragmentation_point: u32,
    primary_path: SctpPaddrInfoRaw,
}

impl SctpStatusRaw
{
    fn new() -> Self
    {
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

    fn to_public(&self) -> io::Result<SctpAssocStatus>
    {
        Ok(SctpAssocStatus {
            assoc_id: unsafe { std::ptr::addr_of!(self.assoc_id).read_unaligned() },
            state: unsafe { std::ptr::addr_of!(self.state).read_unaligned() },
            receiver_window: unsafe { std::ptr::addr_of!(self.receiver_window).read_unaligned() },
            unacked_data_chunks: unsafe {
                std::ptr::addr_of!(self.unacked_data_chunks).read_unaligned()
            },
            pending_data_chunks: unsafe {
                std::ptr::addr_of!(self.pending_data_chunks).read_unaligned()
            },
            inbound_streams: unsafe { std::ptr::addr_of!(self.inbound_streams).read_unaligned() },
            outbound_streams: unsafe { std::ptr::addr_of!(self.outbound_streams).read_unaligned() },
            fragmentation_point: unsafe {
                std::ptr::addr_of!(self.fragmentation_point).read_unaligned()
            },
            primary_path: unsafe { std::ptr::addr_of!(self.primary_path).read_unaligned() }
                .to_public()?,
        })
    }
}

#[repr(C, packed(4))]
struct SctpPaddrParamsRawLegacy
{
    assoc_id: libc::sctp_assoc_t,
    address: libc::sockaddr_storage,
    heartbeat_interval_ms: u32,
    path_max_retransmits: u16,
    path_mtu: u32,
    sack_delay_ms: u32,
    flags: u32,
}

impl SctpPaddrParamsRawLegacy
{
    fn from_public(params: SctpPeerAddrParams) -> Self
    {
        Self {
            assoc_id: params.assoc_id,
            address: option_socket_addr_to_storage(params.address),
            heartbeat_interval_ms: params.heartbeat_interval_ms,
            path_max_retransmits: params.path_max_retransmits,
            path_mtu: params.path_mtu,
            sack_delay_ms: params.sack_delay_ms,
            flags: params.flags,
        }
    }

    fn to_public(&self) -> io::Result<SctpPeerAddrParams>
    {
        Ok(SctpPeerAddrParams {
            assoc_id: unsafe { std::ptr::addr_of!(self.assoc_id).read_unaligned() },
            address: storage_to_option_socket_addr(unsafe {
                std::ptr::addr_of!(self.address).read_unaligned()
            })?,
            heartbeat_interval_ms: unsafe {
                std::ptr::addr_of!(self.heartbeat_interval_ms).read_unaligned()
            },
            path_max_retransmits: unsafe {
                std::ptr::addr_of!(self.path_max_retransmits).read_unaligned()
            },
            path_mtu: unsafe { std::ptr::addr_of!(self.path_mtu).read_unaligned() },
            sack_delay_ms: unsafe { std::ptr::addr_of!(self.sack_delay_ms).read_unaligned() },
            flags: unsafe { std::ptr::addr_of!(self.flags).read_unaligned() },
            ipv6_flow_label: 0,
            dscp: 0,
        })
    }
}

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
        debug_assert!(
            !self.in_use || self.state_ptr.is_null() || unsafe { (*self.state_ptr).is_completed() },
            "runtime sctp accept slot still in flight"
        );
        debug_assert!(
            self.state_ptr.is_null(),
            "completed sctp accept slot should have been reclaimed before reuse"
        );
        self.in_use = true;
        self.addr = unsafe { std::mem::zeroed() };
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
}

struct ConnectSlot
{
    state_ptr: *mut CompletionState,
    in_use: bool,
    fd: RawFd,
    addr: libc::sockaddr_storage,
    addrlen: libc::socklen_t,
    connected_config: SctpSocketConfig,
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
            connected_config: SctpSocketConfig::default(),
        }
    }

    fn prepare(
        &mut self,
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        config: SctpSocketConfig,
    ) -> io::Result<()>
    {
        debug_assert!(
            !self.in_use || self.state_ptr.is_null() || unsafe { (*self.state_ptr).is_completed() },
            "runtime sctp connect slot still in flight"
        );
        self.cleanup_fd();
        debug_assert!(
            self.state_ptr.is_null(),
            "completed sctp connect slot should have been reclaimed before reuse"
        );
        self.connected_config = config;
        self.in_use = true;
        self.fd = match new_sctp_socket(socket_domain(remote_addr), libc::SOCK_STREAM)
        {
            Ok(fd) => fd,
            Err(err) =>
            {
                self.in_use = false;
                return Err(err);
            }
        };
        if let Err(err) = configure_sctp_socket(self.fd, config)
        {
            self.cleanup_fd();
            self.in_use = false;
            return Err(err);
        }

        if let Some(local_addr) = local_addr
        {
            if let Err(err) = set_reuse_addr(self.fd)
            {
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
            if bind_res < 0
            {
                let err = io::Error::last_os_error();
                self.cleanup_fd();
                self.in_use = false;
                return Err(err);
            }
        }

        let (storage, addrlen) = socket_addr_to_c(remote_addr);
        self.addr = storage;
        self.addrlen = addrlen;
        Ok(())
    }

    fn cleanup_fd(&mut self)
    {
        close_if_valid(&mut self.fd);
    }

    fn take_stream(&mut self, remote_addr: SocketAddr) -> SctpStream
    {
        let fd = self.fd;
        self.fd = -1;
        SctpStream::from_raw_fd(fd, remote_addr)
    }

    fn drop_future(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
        self.cleanup_fd();
        self.in_use = false;
    }

    fn drop_cached_state(&mut self)
    {
        self.drop_future();
    }
}

/// One-to-one SCTP listener with a reusable accept slot.
pub struct SctpListener
{
    fd: RuntimeFd,
    local_addr: SocketAddr,
    accept_slot: AcceptSlot,
    accepted_config: SctpSocketConfig,
}

impl SctpListener
{
    /// Binds a listener, applies init parameters, enables notifications, and starts listening.
    pub fn bind(addr: SocketAddr, backlog: i32, initmsg: SctpInitConfig) -> io::Result<Self>
    {
        Self::bind_with_config(addr, backlog, SctpSocketConfig::rich(initmsg))
    }

    /// Binds a listener using the provided SCTP socket configuration.
    pub fn bind_with_config(
        addr: SocketAddr,
        backlog: i32,
        config: SctpSocketConfig,
    ) -> io::Result<Self>
    {
        let fd = new_sctp_socket(socket_domain(addr), libc::SOCK_STREAM)?;
        if let Err(err) = configure_sctp_socket(fd, config)
        {
            close_fd(fd);
            return Err(err);
        }
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
            accepted_config: config,
        })
    }

    /// Returns the local address currently assigned to the listener.
    pub fn local_addr(&self) -> SocketAddr
    {
        self.local_addr
    }

    /// Starts accepting one SCTP association.
    pub fn accept(&mut self) -> AcceptFuture<'_>
    {
        self.accept_slot.prepare();
        AcceptFuture {
            fd: self.fd.as_raw_fd(),
            slot: &mut self.accept_slot,
            accepted_config: self.accepted_config,
        }
    }
}

impl AsRawFd for SctpListener
{
    fn as_raw_fd(&self) -> RawFd
    {
        self.fd.as_raw_fd()
    }
}

impl Drop for SctpListener
{
    fn drop(&mut self)
    {
        self.accept_slot.drop_cached_state();
    }
}

/// Initial SCTP association parameters used by listeners and connectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpInitConfig
{
    /// Requested outbound stream count during association setup.
    pub outbound_streams: u16,
    /// Requested inbound stream count during association setup.
    pub inbound_streams: u16,
    /// Maximum number of INIT retransmissions before setup fails.
    pub max_attempts: u16,
    /// Maximum timeout for INIT retransmissions, in milliseconds.
    pub max_init_timeout_ms: u16,
}

impl Default for SctpInitConfig
{
    fn default() -> Self
    {
        Self {
            outbound_streams: 16,
            inbound_streams: 16,
            max_attempts: 4,
            max_init_timeout_ms: 0,
        }
    }
}

impl SctpInitConfig
{
    /// Returns a configuration suitable for Diameter-style one-to-one associations.
    pub fn diameter_default() -> Self
    {
        Self::default()
    }

    fn as_raw(self) -> libc::sctp_initmsg
    {
        libc::sctp_initmsg {
            sinit_num_ostreams: self.outbound_streams,
            sinit_max_instreams: self.inbound_streams,
            sinit_max_attempts: self.max_attempts,
            sinit_max_init_timeo: self.max_init_timeout_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SctpSocketOptions
{
    notifications: SctpNotificationMask,
    recv_rcvinfo: bool,
    nodelay: bool,
    default_send_info: Option<SctpSendInfo>,
    send_buffer_size: Option<usize>,
    recv_buffer_size: Option<usize>,
}

/// SCTP socket behavior configuration shared by listeners, connectors, and
/// fast-path stream operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SctpSocketConfig
{
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

impl Default for SctpSocketConfig
{
    fn default() -> Self
    {
        Self::rich(SctpInitConfig::default())
    }
}

impl SctpSocketConfig
{
    /// Rich metadata configuration matching the current `send_msg` / `recv_msg`
    /// defaults.
    pub fn rich(init: SctpInitConfig) -> Self
    {
        Self::signaling(init)
    }

    /// Signaling-oriented configuration: rich metadata plus the notification
    /// mask that current SCTP users expect.
    pub fn signaling(init: SctpInitConfig) -> Self
    {
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
    pub fn data(init: SctpInitConfig) -> Self
    {
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

    fn socket_options(self) -> SctpSocketOptions
    {
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
/// from the reactor pool; the slot only holds the stable socket/address/init
/// state needed to build the next future.
pub struct SctpConnector
{
    connect_slot: ConnectSlot,
    config: SctpSocketConfig,
    local_addr: Option<SocketAddr>,
}

impl SctpConnector
{
    /// Creates a connector with the provided init configuration.
    pub fn new(init: SctpInitConfig) -> Self
    {
        Self::with_config(SctpSocketConfig::rich(init))
    }

    /// Creates a connector with the provided SCTP socket configuration.
    pub fn with_config(config: SctpSocketConfig) -> Self
    {
        Self {
            connect_slot: ConnectSlot::new(),
            config,
            local_addr: None,
        }
    }

    /// Pins the connector to a specific local address before connecting.
    pub fn with_local_addr(mut self, addr: SocketAddr) -> Self
    {
        self.local_addr = Some(addr);
        self
    }

    /// Starts connecting to the provided remote SCTP peer.
    pub fn connect(&mut self, remote_addr: SocketAddr) -> io::Result<ConnectFuture<'_>>
    {
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
    /// provided duration elapses.
    pub fn connect_timeout(
        &mut self,
        remote_addr: SocketAddr,
        timeout_duration: Duration,
    ) -> io::Result<ConnectTimeoutFuture<'_>>
    {
        Ok(ConnectTimeoutFuture {
            inner: timeout(timeout_duration, self.connect(remote_addr)?),
        })
    }
}

impl Drop for SctpConnector
{
    fn drop(&mut self)
    {
        self.connect_slot.drop_cached_state();
    }
}

/// One-to-one SCTP association with generic buffer support.
pub struct SctpStream
{
    fd: RuntimeFd,
    remote_addr: SocketAddr,
}

impl SctpStream
{
    /// Wraps an already-owned SCTP socket and records its remote peer.
    pub fn from_raw_fd(fd: RawFd, remote_addr: SocketAddr) -> Self
    {
        Self {
            fd: RuntimeFd::new(fd),
            remote_addr,
        }
    }

    /// Returns the local address currently assigned to the association socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr>
    {
        current_local_addr(self.fd.as_raw_fd())
    }

    /// Returns the remote address associated with the stream.
    pub fn peer_addr(&self) -> SocketAddr
    {
        self.remote_addr
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

    /// Shuts down the read, write, or both halves of this association socket.
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

    /// Returns all local addresses currently associated with the stream.
    ///
    /// This relies on a Linux SCTP address-enumeration socket option and may
    /// fail on systems with partial SCTP support.
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>>
    {
        get_assoc_addrs(self.fd.as_raw_fd(), SCTP_GET_LOCAL_ADDRS_OPT, 0)
    }

    /// Returns all peer addresses currently associated with the stream.
    ///
    /// This relies on a Linux SCTP address-enumeration socket option and may
    /// fail on systems with partial SCTP support.
    pub fn peer_addrs(&self) -> io::Result<Vec<SocketAddr>>
    {
        get_assoc_addrs(self.fd.as_raw_fd(), SCTP_GET_PEER_ADDRS_OPT, 0)
    }

    /// Returns current association status, including the current primary path.
    ///
    /// This is capability-dependent and may be unavailable on kernels with
    /// limited SCTP status support.
    pub fn status(&self) -> io::Result<SctpAssocStatus>
    {
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<SctpStatusRaw>()
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        raw.to_public()
    }

    /// Returns read-only transport information for one peer address.
    ///
    /// This depends on `SCTP_GET_PEER_ADDR_INFO` support in the running kernel.
    pub fn peer_addr_info(&self, peer_addr: SocketAddr) -> io::Result<SctpPeerAddrInfo>
    {
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<SctpPaddrInfoRaw>()
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        raw.to_public()
    }

    /// Returns read-only information for the primary transport path.
    ///
    /// This is derived from [`SctpStream::status`] and has the same capability
    /// requirements.
    pub fn primary_path_info(&self) -> io::Result<SctpPeerAddrInfo>
    {
        self.status().map(|status| status.primary_path)
    }

    /// Returns SCTP association reconfiguration capabilities reported by the kernel.
    ///
    /// This depends on Linux SCTP reconfiguration support and may fail even
    /// when baseline SCTP messaging is available.
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
    pub fn reconfig_supported(&self) -> io::Result<SctpReconfigFlags>
    {
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }
        if optlen as usize != std::mem::size_of::<SctpAssocValueRaw>()
        {
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
    /// disabled by the kernel or by association policy.
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
    pub fn enable_stream_reset(&self, flags: SctpReconfigFlags) -> io::Result<()>
    {
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
    /// capabilities on the running kernel.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::{SctpResetStreams, SctpStream};
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// stream.reset_streams(&SctpResetStreams::outgoing(&[1]))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset_streams(&self, request: &SctpResetStreams) -> io::Result<()>
    {
        if request.streams.len() > (u16::MAX as usize)
        {
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
            if streams_len != 0
            {
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Requests additional inbound and outbound SCTP streams on this association.
    ///
    /// This requires SCTP association reconfiguration support and may be
    /// rejected by the kernel even when baseline SCTP messaging works.
    ///
    /// # Example
    /// ```no_run
    /// # use flowio::net::sctp::{SctpAddStreams, SctpStream};
    /// # fn demo(stream: &SctpStream) -> std::io::Result<()> {
    /// stream.add_streams(SctpAddStreams::new(1, 1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_streams(&self, request: SctpAddStreams) -> io::Result<()>
    {
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
    pub fn peer_addr_params(&self, address: Option<SocketAddr>) -> io::Result<SctpPeerAddrParams>
    {
        let mut buffer = [0u8; std::mem::size_of::<SctpPaddrParamsRaw>()];
        let request = SctpPaddrParamsRaw::from_public(SctpPeerAddrParams {
            address,
            ..SctpPeerAddrParams::association_default()
        });
        unsafe {
            std::ptr::copy_nonoverlapping(
                &request as *const SctpPaddrParamsRaw as *const u8,
                buffer.as_mut_ptr(),
                std::mem::size_of::<SctpPaddrParamsRawLegacy>(),
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }

        if optlen as usize == std::mem::size_of::<SctpPaddrParamsRaw>()
        {
            let raw =
                unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpPaddrParamsRaw) };
            return raw.to_public();
        }

        if optlen as usize == std::mem::size_of::<SctpPaddrParamsRawLegacy>()
        {
            let raw = unsafe {
                std::ptr::read_unaligned(buffer.as_ptr() as *const SctpPaddrParamsRawLegacy)
            };
            return raw.to_public();
        }

        if (optlen as usize) < std::mem::size_of::<SctpPaddrParamsRawLegacy>()
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let raw = unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpPaddrParamsRaw) };
        raw.to_public()
    }

    /// Applies `SCTP_PEER_ADDR_PARAMS` to the whole association or to one specific path.
    pub fn set_peer_addr_params(&self, params: SctpPeerAddrParams) -> io::Result<()>
    {
        apply_peer_addr_params_raw(self.fd.as_raw_fd(), params)
    }

    /// Requests that the local SCTP stack use the provided peer address as primary.
    pub fn set_primary_addr(&self, peer_addr: SocketAddr) -> io::Result<()>
    {
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

    /// Requests that the peer use the provided local address as primary for this association.
    ///
    /// Some Linux SCTP deployments reject this with `EPERM`/`EACCES` when dynamic address
    /// reconfiguration is disabled by kernel policy or association capabilities.
    pub fn set_peer_primary_addr(&self, local_addr: SocketAddr) -> io::Result<()>
    {
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

    /// Returns the remote address associated with the stream.
    pub fn remote_addr(&self) -> SocketAddr
    {
        self.remote_addr
    }

    /// Applies default SCTP send metadata to this socket. This is used by the
    /// fast-path [`SctpStream::send`] API.
    pub fn set_default_send_info(&self, info: SctpSendInfo) -> io::Result<()>
    {
        set_sock_opt(
            self.fd.as_raw_fd(),
            libc::IPPROTO_SCTP,
            libc::SCTP_DEFAULT_SNDINFO,
            &raw_sndinfo_from_public(info),
        )
    }

    /// Applies a typed SCTP notification subscription mask.
    pub fn set_notification_mask(&self, mask: SctpNotificationMask) -> io::Result<()>
    {
        let recv_rcvinfo: libc::c_int = super::get_sock_opt(
            self.fd.as_raw_fd(),
            libc::IPPROTO_SCTP,
            libc::SCTP_RECVRCVINFO,
        )?;
        set_sctp_events(self.fd.as_raw_fd(), mask, recv_rcvinfo != 0)
    }

    /// Applies association-wide retransmission and RTO policy.
    pub fn apply_assoc_config(&self, config: &SctpAssocConfig) -> io::Result<()>
    {
        apply_assoc_config_raw(self.fd.as_raw_fd(), *config)
    }

    /// Applies association-wide peer-address defaults.
    ///
    /// `params.address` must be `None`; use [`SctpStream::set_peer_addr_params`]
    /// for path-specific overrides.
    pub fn set_default_peer_addr_params(&self, params: SctpPeerAddrParams) -> io::Result<()>
    {
        apply_default_peer_addr_params(self.fd.as_raw_fd(), params)
    }

    /// Starts one connected data receive on the fast path.
    ///
    /// This path is intended for sockets configured without SCTP
    /// notifications and without `SCTP_RCVINFO`. It returns only the received
    /// byte count, matching the common data-only case.
    pub fn recv<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> DataRecvFuture<'_, B>
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
        DataRecvFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            len,
            input_error,
            _marker: PhantomData,
        }
    }

    /// Starts one connected data send on the fast path.
    ///
    /// Per-message metadata comes from the socket's default send info, if any.
    pub fn send<B: IoBuffReadOnly>(&mut self, buffer: B) -> DataSendFuture<'_, B>
    {
        DataSendFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            _marker: PhantomData,
        }
    }

    /// Starts one message-oriented receive.
    pub fn recv_msg<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> RecvFuture<'_, B>
    {
        let mut input_error = None;
        let len = match checked_read_len("recv_msg", len, buffer.writable_len())
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
            iovec: MaybeUninit::uninit(),
            addr: MaybeUninit::uninit(),
            addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            control: [0; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
            msghdr: MaybeUninit::uninit(),
            _marker: PhantomData,
        }
    }

    /// Starts one message-oriented send with explicit SCTP metadata.
    pub fn send_msg<B: IoBuffReadOnly>(
        &mut self,
        buffer: B,
        info: SctpSendInfo,
    ) -> SendFuture<'_, B>
    {
        SendFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovec: MaybeUninit::uninit(),
            control: [0; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
            msghdr: MaybeUninit::uninit(),
            sndinfo: raw_sndinfo_from_public(info),
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
    pub fn recv_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVecMut<N>,
    ) -> RecvVectoredFuture<'_, N>
    {
        let mut buffer = buffer;
        let mut iovecs: [MaybeUninit<libc::iovec>; N] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let (iov_count, _) = buffer.fill_read_iovecs_and_writable_len(&mut iovecs);
        RecvVectoredFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            addr: MaybeUninit::uninit(),
            addrlen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            control: [0; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
            msghdr: MaybeUninit::uninit(),
            _marker: PhantomData,
        }
    }

    /// Gather-send from a vectored buffer chain with SCTP metadata.
    ///
    /// The chain is consumed and returned alongside the result (rental
    /// pattern).  The total number of bytes sent is returned in `Ok`.
    pub fn send_msg_vectored<const N: usize>(
        &mut self,
        buffer: IoBuffVec<N>,
        info: SctpSendInfo,
    ) -> SendVectoredFuture<'_, N>
    {
        let mut iovecs: [MaybeUninit<libc::iovec>; N] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let (iov_count, _) = buffer.fill_write_iovecs_and_len(&mut iovecs);
        SendVectoredFuture {
            fd: self.fd.as_raw_fd(),
            state_ptr: std::ptr::null_mut(),
            buffer: Some(buffer),
            iovecs,
            iov_count,
            control: [0; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
            msghdr: MaybeUninit::uninit(),
            sndinfo: raw_sndinfo_from_public(info),
            _marker: PhantomData,
        }
    }
}

impl AsRawFd for SctpStream
{
    fn as_raw_fd(&self) -> RawFd
    {
        self.fd.as_raw_fd()
    }
}

use super::{opt_mut, opt_ref, opt_take};

#[doc(hidden)]
pub struct DataRecvFuture<'a, B: IoBuffReadWrite>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    len: u32,
    input_error: Option<io::Error>,
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadWrite> Future for DataRecvFuture<'_, B>
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

impl<B: IoBuffReadWrite> Drop for DataRecvFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct DataSendFuture<'a, B: IoBuffReadOnly>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadOnly> Future for DataSendFuture<'_, B>
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

impl<B: IoBuffReadOnly> Drop for DataSendFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct RecvFuture<'a, B: IoBuffReadWrite>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    len: u32,
    input_error: Option<io::Error>,
    iovec: MaybeUninit<libc::iovec>,
    addr: MaybeUninit<libc::sockaddr_storage>,
    addrlen: libc::socklen_t,
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
    msghdr: MaybeUninit<libc::msghdr>,
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<B: IoBuffReadWrite> Future for RecvFuture<'_, B>
{
    type Output = (io::Result<(usize, SctpRecvMeta)>, B);

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

                if result < 0
                {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }

                let actual = result as usize;

                // Parse metadata before taking the buffer — notification parsing
                // needs to read the received bytes via the stable pointer.
                let msg = unsafe { this.msghdr.assume_init_ref() };
                let data_slice = unsafe {
                    let ptr = opt_mut(&mut this.buffer).as_mut_ptr();
                    std::slice::from_raw_parts(ptr, actual)
                };
                let meta = if (msg.msg_flags & libc::MSG_NOTIFICATION) != 0
                {
                    parse_notification(data_slice)
                }
                else
                {
                    parse_rcvinfo(&this.control[..], msg.msg_controllen).map(SctpRecvMeta::Data)
                };

                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe {
                    buffer.set_written_len(actual);
                }

                return match meta
                {
                    Ok(meta) => Poll::Ready((Ok((actual, meta)), buffer)),
                    Err(err) => Poll::Ready((Err(err), buffer)),
                };
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
            this.iovec.write(libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: this.len as usize,
            });
            this.msghdr.write(libc::msghdr {
                msg_name: this.addr.as_mut_ptr() as *mut libc::c_void,
                msg_namelen: this.addrlen,
                msg_iov: this.iovec.as_mut_ptr(),
                msg_iovlen: 1,
                msg_control: this.control.as_mut_ptr() as *mut libc::c_void,
                msg_controllen: this.control.len(),
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

impl<B: IoBuffReadWrite> Drop for RecvFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct SendFuture<'a, B: IoBuffReadOnly>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<B>,
    iovec: MaybeUninit<libc::iovec>,
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
    msghdr: MaybeUninit<libc::msghdr>,
    sndinfo: libc::sctp_sndinfo,
    _marker: PhantomData<&'a mut SctpStream>,
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
            let len = buf.len();
            this.iovec.write(libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: len,
            });
            this.msghdr.write(libc::msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: this.iovec.as_mut_ptr(),
                msg_iovlen: 1,
                msg_control: this.control.as_mut_ptr() as *mut libc::c_void,
                msg_controllen: this.control.len(),
                msg_flags: 0,
            });
            write_cmsg_sndinfo(&mut this.control[..], this.sndinfo);

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

impl<B: IoBuffReadOnly> Drop for SendFuture<'_, B>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// RecvVectoredFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct RecvVectoredFuture<'a, const N: usize>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<IoBuffVecMut<N>>,
    iovecs: [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    addr: MaybeUninit<libc::sockaddr_storage>,
    addrlen: libc::socklen_t,
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_rcvinfo>())],
    msghdr: MaybeUninit<libc::msghdr>,
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for RecvVectoredFuture<'_, N>
{
    type Output = (io::Result<(usize, SctpRecvMeta)>, IoBuffVecMut<N>);

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
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buffer));
                }

                let actual = result as usize;
                let msg = unsafe { this.msghdr.assume_init_ref() };
                let data_slice = if this.iov_count == 0
                {
                    &[]
                }
                else
                {
                    let first_iov = unsafe { &*(this.iovecs.as_ptr() as *const libc::iovec) };
                    let safe_len = std::cmp::min(actual, first_iov.iov_len);
                    unsafe { std::slice::from_raw_parts(first_iov.iov_base as *const u8, safe_len) }
                };

                let meta = if (msg.msg_flags & libc::MSG_NOTIFICATION) != 0
                {
                    parse_notification(data_slice)
                }
                else
                {
                    parse_rcvinfo(&this.control[..], msg.msg_controllen).map(SctpRecvMeta::Data)
                };

                let mut buffer = unsafe { opt_take(&mut this.buffer) };
                unsafe {
                    buffer.distribute_written(actual);
                }

                return match meta
                {
                    Ok(meta) => Poll::Ready((Ok((actual, meta)), buffer)),
                    Err(err) => Poll::Ready((Err(err), buffer)),
                };
            }
        }

        if this.iov_count == 0
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(io::Error::from(io::ErrorKind::InvalidInput)), buffer));
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

            this.msghdr.write(libc::msghdr {
                msg_name: this.addr.as_mut_ptr() as *mut libc::c_void,
                msg_namelen: this.addrlen,
                msg_iov: this.iovecs.as_mut_ptr() as *mut libc::iovec,
                msg_iovlen: this.iov_count,
                msg_control: this.control.as_mut_ptr() as *mut libc::c_void,
                msg_controllen: this.control.len(),
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

impl<const N: usize> Drop for RecvVectoredFuture<'_, N>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

// ---------------------------------------------------------------------------
// SendVectoredFuture
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct SendVectoredFuture<'a, const N: usize>
{
    fd: RawFd,
    state_ptr: *mut CompletionState,
    buffer: Option<IoBuffVec<N>>,
    iovecs: [MaybeUninit<libc::iovec>; N],
    iov_count: usize,
    control: [u8; cmsg_space(std::mem::size_of::<libc::sctp_sndinfo>())],
    msghdr: MaybeUninit<libc::msghdr>,
    sndinfo: libc::sctp_sndinfo,
    _marker: PhantomData<&'a mut SctpStream>,
}

impl<const N: usize> Future for SendVectoredFuture<'_, N>
{
    type Output = (io::Result<usize>, IoBuffVec<N>);

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

        if this.iov_count == 0
        {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Ok(0), buffer));
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

            this.msghdr.write(libc::msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: this.iovecs.as_mut_ptr() as *mut libc::iovec,
                msg_iovlen: this.iov_count,
                msg_control: this.control.as_mut_ptr() as *mut libc::c_void,
                msg_controllen: this.control.len(),
                msg_flags: 0,
            });
            write_cmsg_sndinfo(&mut this.control[..], this.sndinfo);

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

impl<const N: usize> Drop for SendVectoredFuture<'_, N>
{
    fn drop(&mut self)
    {
        unsafe { drop_op_ptr_unchecked(&mut self.state_ptr) };
    }
}

#[doc(hidden)]
pub struct AcceptFuture<'a>
{
    fd: RawFd,
    slot: &'a mut AcceptSlot,
    accepted_config: SctpSocketConfig,
}

impl Future for AcceptFuture<'_>
{
    type Output = io::Result<(SctpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.slot.state_ptr.is_null()
        {
            let state = unsafe { &*this.slot.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(this.slot.state_ptr) };
                this.slot.state_ptr = std::ptr::null_mut();
                this.slot.in_use = false;

                if result < 0
                {
                    return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                }

                let fd = result as RawFd;
                return match socket_addr_from_c(&this.slot.addr, this.slot.addrlen)
                {
                    Ok(remote_addr) =>
                    {
                        if let Err(err) =
                            apply_sctp_connected_socket_config(fd, this.accepted_config)
                        {
                            close_fd(fd);
                            return Poll::Ready(Err(err));
                        }
                        Poll::Ready(Ok((SctpStream::from_raw_fd(fd, remote_addr), remote_addr)))
                    }
                    Err(err) =>
                    {
                        close_fd(fd);
                        Poll::Ready(Err(err))
                    }
                };
            }
        }

        if this.slot.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                this.slot.in_use = false;
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.slot.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let addr_ptr =
                &mut this.slot.addr as *mut libc::sockaddr_storage as *mut libc::sockaddr;
            let addrlen_ptr = &mut this.slot.addrlen as *mut libc::socklen_t;
            let sqe = opcode::Accept::new(types::Fd(this.fd), addr_ptr, addrlen_ptr)
                .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.slot.state_ptr = std::ptr::null_mut();
                    this.slot.in_use = false;
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

impl Drop for AcceptFuture<'_>
{
    fn drop(&mut self)
    {
        self.slot.drop_future();
    }
}

#[doc(hidden)]
pub struct ConnectFuture<'a>
{
    slot: &'a mut ConnectSlot,
    remote_addr: SocketAddr,
}

impl Future for ConnectFuture<'_>
{
    type Output = io::Result<SctpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.slot.state_ptr.is_null()
        {
            let state = unsafe { &*this.slot.state_ptr };
            if state.is_completed()
            {
                let result = state.result;
                let pctx = unsafe { poll_ctx_from_waker(cx) };
                unsafe { (*pctx.reactor()).free_op(this.slot.state_ptr) };
                this.slot.state_ptr = std::ptr::null_mut();
                this.slot.in_use = false;

                if result < 0
                {
                    let err = io::Error::from_raw_os_error(-result);
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(err));
                }

                if let Err(err) =
                    apply_sctp_connected_socket_config(this.slot.fd, this.slot.connected_config)
                {
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(err));
                }
                return Poll::Ready(Ok(this.slot.take_stream(this.remote_addr)));
            }
        }

        if this.slot.state_ptr.is_null()
        {
            let pctx = unsafe { poll_ctx_from_waker(cx) };
            let state_ptr = unsafe { (*pctx.reactor()).alloc_op() };
            if state_ptr.is_null()
            {
                this.slot.in_use = false;
                this.slot.cleanup_fd();
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::WouldBlock)));
            }
            this.slot.state_ptr = state_ptr;

            unsafe { (*state_ptr).register_waiter(pctx.owner_task()) };

            let addr_ptr =
                &this.slot.addr as *const libc::sockaddr_storage as *const libc::sockaddr;
            let sqe = opcode::Connect::new(types::Fd(this.slot.fd), addr_ptr, this.slot.addrlen)
                .build()
                .user_data(state_ptr as u64);

            unsafe {
                if let Err(e) = submit_tracked_sqe(&pctx, sqe)
                {
                    (*pctx.reactor()).free_op(state_ptr);
                    this.slot.state_ptr = std::ptr::null_mut();
                    this.slot.in_use = false;
                    this.slot.cleanup_fd();
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

impl Drop for ConnectFuture<'_>
{
    fn drop(&mut self)
    {
        self.slot.drop_future();
    }
}

#[doc(hidden)]
pub fn test_accept_slot_drop_future_closes_completed_fd() -> io::Result<()>
{
    fn dummy_fd() -> io::Result<RawFd>
    {
        let mut fds = [-1; 2];
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        if rc != 0
        {
            return Err(io::Error::last_os_error());
        }
        close_fd(fds[1]);
        Ok(fds[0])
    }

    fn fd_closed(fd: RawFd) -> io::Result<bool>
    {
        let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if rc == -1
        {
            return Ok(io::Error::last_os_error().raw_os_error() == Some(libc::EBADF));
        }
        Ok(false)
    }

    let fd = dummy_fd()?;
    let mut state = CompletionState {
        result: fd,
        cqe_flags: 0,
        state_flags: 0,
        waiter: std::ptr::null_mut(),
    };
    state.set_completed();

    let mut slot = AcceptSlot::new();
    slot.in_use = true;
    slot.state_ptr = &mut state;

    slot.drop_future();

    if !slot.state_ptr.is_null() || slot.in_use
    {
        return Err(io::Error::from(io::ErrorKind::Other));
    }
    if !fd_closed(fd)?
    {
        return Err(io::Error::from(io::ErrorKind::Other));
    }
    Ok(())
}

#[doc(hidden)]
pub fn test_connect_slot_drop_future_closes_socket_fd() -> io::Result<()>
{
    fn dummy_fd() -> io::Result<RawFd>
    {
        let mut fds = [-1; 2];
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        if rc != 0
        {
            return Err(io::Error::last_os_error());
        }
        close_fd(fds[1]);
        Ok(fds[0])
    }

    fn fd_closed(fd: RawFd) -> io::Result<bool>
    {
        let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if rc == -1
        {
            return Ok(io::Error::last_os_error().raw_os_error() == Some(libc::EBADF));
        }
        Ok(false)
    }

    let fd = dummy_fd()?;
    let mut slot = ConnectSlot::new();
    slot.in_use = true;
    slot.fd = fd;

    slot.drop_future();

    if !slot.state_ptr.is_null() || slot.in_use || slot.fd != -1
    {
        return Err(io::Error::from(io::ErrorKind::Other));
    }
    if !fd_closed(fd)?
    {
        return Err(io::Error::from(io::ErrorKind::Other));
    }
    Ok(())
}

fn map_connect_timeout(result: Result<io::Result<SctpStream>, Elapsed>) -> io::Result<SctpStream>
{
    match result
    {
        Ok(result) => result,
        Err(_) => Err(io::Error::from(io::ErrorKind::TimedOut)),
    }
}

/// Connect future with a relative timeout for a reusable [`SctpConnector`].
pub struct ConnectTimeoutFuture<'a>
{
    inner: Timeout<ConnectFuture<'a>>,
}

impl Future for ConnectTimeoutFuture<'_>
{
    type Output = io::Result<SctpStream>;

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

fn new_sctp_socket(domain: libc::c_int, kind: libc::c_int) -> io::Result<RawFd>
{
    let fd = unsafe {
        libc::socket(
            domain,
            kind | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            libc::IPPROTO_SCTP,
        )
    };
    if fd < 0
    {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

fn raw_sndinfo_from_public(info: SctpSendInfo) -> libc::sctp_sndinfo
{
    libc::sctp_sndinfo {
        snd_sid: info.stream_id,
        snd_flags: info.flags,
        snd_ppid: info.ppid.to_be(),
        snd_context: info.context,
        snd_assoc_id: info.assoc_id,
    }
}

fn set_sctp_events(fd: RawFd, mask: SctpNotificationMask, recv_rcvinfo: bool) -> io::Result<()>
{
    let events = SctpEventSubscribe::from_mask(mask, recv_rcvinfo);
    set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_EVENTS, &events)?;
    Ok(())
}

fn apply_sctp_socket_options(fd: RawFd, options: SctpSocketOptions) -> io::Result<()>
{
    set_sctp_events(fd, options.notifications, options.recv_rcvinfo)?;

    let recv_rcvinfo: libc::c_int = if options.recv_rcvinfo { 1 } else { 0 };
    set_sock_opt(
        fd,
        libc::IPPROTO_SCTP,
        libc::SCTP_RECVRCVINFO,
        &recv_rcvinfo,
    )?;

    let nodelay: libc::c_int = if options.nodelay { 1 } else { 0 };
    set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_NODELAY, &nodelay)?;

    if let Some(size) = options.send_buffer_size
    {
        super::set_sock_send_buffer_size(fd, size)?;
    }

    if let Some(size) = options.recv_buffer_size
    {
        super::set_sock_recv_buffer_size(fd, size)?;
    }

    if let Some(info) = options.default_send_info
    {
        set_sock_opt(
            fd,
            libc::IPPROTO_SCTP,
            libc::SCTP_DEFAULT_SNDINFO,
            &raw_sndinfo_from_public(info),
        )?;
    }

    Ok(())
}

fn apply_assoc_config_raw(fd: RawFd, config: SctpAssocConfig) -> io::Result<()>
{
    if let Some(assoc_max_retrans) = config.assoc_max_retrans
    {
        let mut raw = SctpAssocParamsRaw::get(fd)?;
        raw.assoc_max_retrans = assoc_max_retrans;
        set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_ASSOCINFO, &raw)?;
    }

    if config.rto_initial_ms.is_some() || config.rto_max_ms.is_some() || config.rto_min_ms.is_some()
    {
        let mut raw = SctpRtoInfoRaw::get(fd)?;
        if let Some(rto_initial_ms) = config.rto_initial_ms
        {
            raw.rto_initial_ms = rto_initial_ms;
        }
        if let Some(rto_max_ms) = config.rto_max_ms
        {
            raw.rto_max_ms = rto_max_ms;
        }
        if let Some(rto_min_ms) = config.rto_min_ms
        {
            raw.rto_min_ms = rto_min_ms;
        }
        set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_RTOINFO, &raw)?;
    }

    Ok(())
}

fn apply_peer_addr_params_raw(fd: RawFd, params: SctpPeerAddrParams) -> io::Result<()>
{
    if params.ipv6_flow_label != 0
        || params.dscp != 0
        || (params.flags & (SPP_IPV6_FLOWLABEL | SPP_DSCP)) != 0
    {
        let raw = SctpPaddrParamsRaw::from_public(params);
        set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_PEER_ADDR_PARAMS, &raw)
    }
    else
    {
        let raw = SctpPaddrParamsRawLegacy::from_public(params);
        set_sock_opt(fd, libc::IPPROTO_SCTP, libc::SCTP_PEER_ADDR_PARAMS, &raw)
    }
}

fn apply_default_peer_addr_params(fd: RawFd, params: SctpPeerAddrParams) -> io::Result<()>
{
    if params.address.is_some()
    {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    apply_peer_addr_params_raw(fd, params)
}

fn apply_sctp_connected_socket_config(fd: RawFd, config: SctpSocketConfig) -> io::Result<()>
{
    apply_sctp_socket_options(fd, config.socket_options())?;
    if let Some(assoc) = config.assoc
    {
        apply_assoc_config_raw(fd, assoc)?;
    }
    if let Some(params) = config.default_peer_addr_params
    {
        apply_default_peer_addr_params(fd, params)?;
    }
    Ok(())
}

fn configure_sctp_socket(fd: RawFd, config: SctpSocketConfig) -> io::Result<()>
{
    set_sock_opt(
        fd,
        libc::IPPROTO_SCTP,
        libc::SCTP_INITMSG,
        &config.init.as_raw(),
    )?;
    apply_sctp_socket_options(fd, config.socket_options())
}

#[repr(C)]
struct SctpGetAddrsHeader
{
    assoc_id: libc::sctp_assoc_t,
    addr_num: u32,
}

fn get_assoc_addrs(
    fd: RawFd,
    optname: libc::c_int,
    assoc_id: libc::sctp_assoc_t,
) -> io::Result<Vec<SocketAddr>>
{
    const INITIAL_CAPACITY: usize = 8;

    let mut capacity = INITIAL_CAPACITY;
    loop
    {
        let header_len = std::mem::size_of::<SctpGetAddrsHeader>();
        let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
        let total_len = header_len + capacity * storage_len;
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
        if rc < 0
        {
            return Err(io::Error::last_os_error());
        }

        if (optlen as usize) < header_len
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let header =
            unsafe { std::ptr::read_unaligned(buffer.as_ptr() as *const SctpGetAddrsHeader) };
        let addr_count = header.addr_num as usize;
        if addr_count > capacity
        {
            capacity = addr_count;
            continue;
        }

        let used_len = optlen as usize;
        let payload = &buffer[header_len..used_len];
        return parse_assoc_addrs(payload, addr_count, storage_len)
            .map_err(|err| io::Error::from(err.kind()));
    }
}

fn option_socket_addr_to_storage(addr: Option<SocketAddr>) -> libc::sockaddr_storage
{
    match addr
    {
        Some(addr) => socket_addr_to_c(addr).0,
        None =>
        unsafe { std::mem::zeroed() },
    }
}

fn sockaddr_len_for_storage(storage: libc::sockaddr_storage) -> io::Result<libc::socklen_t>
{
    let family = unsafe {
        *(&storage as *const libc::sockaddr_storage as *const libc::sa_family_t) as libc::c_int
    };
    match family
    {
        libc::AF_INET => Ok(std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t),
        libc::AF_INET6 => Ok(std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t),
        _ => Err(io::Error::from(io::ErrorKind::InvalidData)),
    }
}

fn storage_to_option_socket_addr(storage: libc::sockaddr_storage)
-> io::Result<Option<SocketAddr>>
{
    let family = unsafe {
        *(&storage as *const libc::sockaddr_storage as *const libc::sa_family_t) as libc::c_int
    };
    match family
    {
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

fn parse_assoc_addrs(
    payload: &[u8],
    addr_count: usize,
    storage_len: usize,
) -> io::Result<Vec<SocketAddr>>
{
    let mut addrs = Vec::with_capacity(addr_count);
    if parse_assoc_addrs_inner(payload, addr_count, storage_len, &mut addrs)
    {
        return Ok(addrs);
    }

    Err(io::Error::from(io::ErrorKind::InvalidData))
}

fn parse_assoc_addrs_inner(
    payload: &[u8],
    remaining: usize,
    storage_len: usize,
    addrs: &mut Vec<SocketAddr>,
) -> bool
{
    if remaining == 0
    {
        return payload.is_empty();
    }

    if payload.len() < std::mem::size_of::<libc::sa_family_t>()
    {
        return false;
    }

    let family = libc::sa_family_t::from_ne_bytes([payload[0], payload[1]]) as libc::c_int;
    for candidate in assoc_addr_candidates(family, storage_len)
    {
        if payload.len() < candidate.entry_len
        {
            continue;
        }

        if let Ok(addr) = parse_assoc_addr_entry(&payload[..candidate.addr_len], family)
        {
            addrs.push(addr);
            if parse_assoc_addrs_inner(
                &payload[candidate.entry_len..],
                remaining - 1,
                storage_len,
                addrs,
            )
            {
                return true;
            }
            let _ = addrs.pop();
        }
    }

    false
}

#[derive(Clone, Copy)]
struct AssocAddrCandidate
{
    entry_len: usize,
    addr_len: usize,
}

fn assoc_addr_candidates(family: libc::c_int, storage_len: usize) -> &'static [AssocAddrCandidate]
{
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

    match family
    {
        libc::AF_INET =>
        {
            if storage_len == std::mem::size_of::<libc::sockaddr_storage>()
            {
                &IPV4_PADDED
            }
            else
            {
                &IPV4_BASE
            }
        }
        libc::AF_INET6 =>
        {
            if storage_len == std::mem::size_of::<libc::sockaddr_storage>()
            {
                &IPV6_PADDED
            }
            else
            {
                &IPV6_BASE
            }
        }
        _ => &[],
    }
}

fn parse_assoc_addr_entry(bytes: &[u8], family: libc::c_int) -> io::Result<SocketAddr>
{
    match family
    {
        libc::AF_INET =>
        {
            if bytes.len() >= 8
            {
                let port = u16::from_be_bytes([bytes[2], bytes[3]]);
                let ip = Ipv4Addr::new(bytes[4], bytes[5], bytes[6], bytes[7]);
                Ok(SocketAddr::from((ip, port)))
            }
            else
            {
                Err(io::Error::from(io::ErrorKind::InvalidData))
            }
        }
        libc::AF_INET6 =>
        {
            if bytes.len() < std::mem::size_of::<libc::sockaddr_in6>()
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }

            let port = u16::from_be_bytes([bytes[2], bytes[3]]);
            let flowinfo = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
            let ip = Ipv6Addr::from([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
                bytes[22], bytes[23],
            ]);
            let scope_id = u32::from_ne_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => Err(io::Error::from(io::ErrorKind::InvalidData)),
    }
}

const fn cmsg_align(len: usize) -> usize
{
    let align = std::mem::size_of::<usize>();
    (len + align - 1) & !(align - 1)
}

const fn cmsg_space(data_len: usize) -> usize
{
    cmsg_align(std::mem::size_of::<libc::cmsghdr>()) + cmsg_align(data_len)
}

fn write_cmsg_sndinfo(control: &mut [u8], sndinfo: libc::sctp_sndinfo)
{
    let hdr = unsafe { &mut *(control.as_mut_ptr() as *mut libc::cmsghdr) };
    hdr.cmsg_level = libc::IPPROTO_SCTP;
    hdr.cmsg_type = libc::SCTP_SNDINFO;
    hdr.cmsg_len =
        (std::mem::size_of::<libc::cmsghdr>() + std::mem::size_of::<libc::sctp_sndinfo>()) as _;

    let data_ptr = unsafe {
        control
            .as_mut_ptr()
            .add(cmsg_align(std::mem::size_of::<libc::cmsghdr>()))
    };
    unsafe {
        std::ptr::write_unaligned(data_ptr as *mut libc::sctp_sndinfo, sndinfo);
    }
}

fn parse_rcvinfo(control: &[u8], controllen: usize) -> io::Result<SctpRecvInfo>
{
    let hdr_len = std::mem::size_of::<libc::cmsghdr>();
    if controllen < hdr_len
    {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let hdr = unsafe { &*(control.as_ptr() as *const libc::cmsghdr) };
    if hdr.cmsg_level != libc::IPPROTO_SCTP || hdr.cmsg_type != libc::SCTP_RCVINFO
    {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let needed = cmsg_align(hdr_len) + std::mem::size_of::<libc::sctp_rcvinfo>();
    if controllen < needed
    {
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
    })
}

fn parse_notification(buffer: &[u8]) -> io::Result<SctpRecvMeta>
{
    if buffer.len() < 8
    {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }

    let sn_type = u16::from_ne_bytes([buffer[0], buffer[1]]);
    let sn_flags = u16::from_ne_bytes([buffer[2], buffer[3]]);
    let sn_length = u32::from_ne_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);

    let notification = match sn_type as libc::c_int
    {
        x if x == local_sctp_assoc_change() =>
        {
            if buffer.len() < 20
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::AssocChange {
                state: u16::from_ne_bytes([buffer[8], buffer[9]]),
                error: u16::from_ne_bytes([buffer[10], buffer[11]]),
                outbound_streams: u16::from_ne_bytes([buffer[12], buffer[13]]),
                inbound_streams: u16::from_ne_bytes([buffer[14], buffer[15]]),
                assoc_id: i32::from_ne_bytes([buffer[16], buffer[17], buffer[18], buffer[19]]),
            }
        }
        x if x == local_sctp_peer_addr_change() =>
        {
            let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
            let min_len = 8 + storage_len + 12;
            if buffer.len() < min_len
            {
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
                state: i32::from_ne_bytes([
                    buffer[base],
                    buffer[base + 1],
                    buffer[base + 2],
                    buffer[base + 3],
                ]),
                error: i32::from_ne_bytes([
                    buffer[base + 4],
                    buffer[base + 5],
                    buffer[base + 6],
                    buffer[base + 7],
                ]),
                assoc_id: i32::from_ne_bytes([
                    buffer[base + 8],
                    buffer[base + 9],
                    buffer[base + 10],
                    buffer[base + 11],
                ]),
            }
        }
        x if x == local_sctp_remote_error() =>
        {
            if buffer.len() < 14
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::RemoteError {
                error: u16::from_be_bytes([buffer[8], buffer[9]]),
                assoc_id: i32::from_ne_bytes([buffer[10], buffer[11], buffer[12], buffer[13]]),
            }
        }
        x if x == local_sctp_shutdown_event() =>
        {
            if buffer.len() < 12
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::Shutdown {
                assoc_id: i32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
            }
        }
        x if x == local_sctp_adaptation_indication() =>
        {
            if buffer.len() < 16
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::Adaptation {
                indication: u32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
                assoc_id: i32::from_ne_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]),
            }
        }
        x if x == local_sctp_partial_delivery_event() =>
        {
            if buffer.len() < 24
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::PartialDelivery {
                indication: u32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
                assoc_id: i32::from_ne_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]),
                stream: u32::from_ne_bytes([buffer[16], buffer[17], buffer[18], buffer[19]]),
                sequence: u32::from_ne_bytes([buffer[20], buffer[21], buffer[22], buffer[23]]),
            }
        }
        x if x == local_sctp_sender_dry_event() =>
        {
            if buffer.len() < 12
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::SenderDry {
                assoc_id: i32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
            }
        }
        x if x == local_sctp_stream_reset_event() =>
        {
            if buffer.len() < 12
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::StreamReset {
                flags: sn_flags,
                assoc_id: i32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
            }
        }
        x if x == local_sctp_assoc_reset_event() =>
        {
            if buffer.len() < 20
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::AssocReset {
                flags: sn_flags,
                assoc_id: i32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
                local_tsn: u32::from_ne_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]),
                remote_tsn: u32::from_ne_bytes([buffer[16], buffer[17], buffer[18], buffer[19]]),
            }
        }
        x if x == local_sctp_stream_change_event() =>
        {
            if buffer.len() < 16
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            SctpNotification::StreamChange {
                flags: sn_flags,
                assoc_id: i32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
                inbound_streams: u16::from_ne_bytes([buffer[12], buffer[13]]),
                outbound_streams: u16::from_ne_bytes([buffer[14], buffer[15]]),
            }
        }
        x if x == local_sctp_send_failed_event() =>
        {
            let sndinfo_len = std::mem::size_of::<libc::sctp_sndinfo>();
            let min_len = 12 + sndinfo_len + 4;
            if buffer.len() < min_len
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            let error = u32::from_ne_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
            let sndinfo_ptr = unsafe { buffer.as_ptr().add(12) as *const libc::sctp_sndinfo };
            let sndinfo = unsafe { std::ptr::read_unaligned(sndinfo_ptr) };
            let assoc_base = 12 + sndinfo_len;
            SctpNotification::SendFailed {
                error,
                info: SctpSendInfo {
                    stream_id: sndinfo.snd_sid,
                    flags: sndinfo.snd_flags,
                    ppid: u32::from_be(sndinfo.snd_ppid),
                    context: sndinfo.snd_context,
                    assoc_id: sndinfo.snd_assoc_id,
                },
                assoc_id: i32::from_ne_bytes([
                    buffer[assoc_base],
                    buffer[assoc_base + 1],
                    buffer[assoc_base + 2],
                    buffer[assoc_base + 3],
                ]),
            }
        }
        _ => SctpNotification::Other {
            kind: sn_type,
            flags: sn_flags,
            length: sn_length,
        },
    };

    Ok(SctpRecvMeta::Notification(notification))
}

const fn local_sctp_assoc_change() -> libc::c_int
{
    local_sctp_notification_type(1)
}

const fn local_sctp_peer_addr_change() -> libc::c_int
{
    local_sctp_notification_type(2)
}

const fn local_sctp_remote_error() -> libc::c_int
{
    local_sctp_notification_type(4)
}

const fn local_sctp_shutdown_event() -> libc::c_int
{
    local_sctp_notification_type(5)
}

const fn local_sctp_partial_delivery_event() -> libc::c_int
{
    local_sctp_notification_type(6)
}

const fn local_sctp_adaptation_indication() -> libc::c_int
{
    local_sctp_notification_type(7)
}

const fn local_sctp_sender_dry_event() -> libc::c_int
{
    local_sctp_notification_type(9)
}

const fn local_sctp_stream_reset_event() -> libc::c_int
{
    local_sctp_notification_type(10)
}

const fn local_sctp_assoc_reset_event() -> libc::c_int
{
    local_sctp_notification_type(11)
}

const fn local_sctp_stream_change_event() -> libc::c_int
{
    local_sctp_notification_type(12)
}

const fn local_sctp_send_failed_event() -> libc::c_int
{
    local_sctp_notification_type(13)
}

const fn local_sctp_notification_type(index: libc::c_int) -> libc::c_int
{
    (1 << 15) | index
}

#[doc(hidden)]
pub fn test_parse_notification(buffer: &[u8]) -> io::Result<SctpRecvMeta>
{
    parse_notification(buffer)
}

#[doc(hidden)]
pub const fn test_assoc_change_type() -> libc::c_int
{
    local_sctp_assoc_change()
}

#[doc(hidden)]
pub const fn test_adaptation_indication_type() -> libc::c_int
{
    local_sctp_adaptation_indication()
}

#[doc(hidden)]
pub const fn test_peer_addr_change_type() -> libc::c_int
{
    local_sctp_peer_addr_change()
}

#[doc(hidden)]
pub const fn test_remote_error_type() -> libc::c_int
{
    local_sctp_remote_error()
}

#[doc(hidden)]
pub const fn test_shutdown_event_type() -> libc::c_int
{
    local_sctp_shutdown_event()
}

#[doc(hidden)]
pub const fn test_partial_delivery_event_type() -> libc::c_int
{
    local_sctp_partial_delivery_event()
}

#[doc(hidden)]
pub const fn test_sender_dry_event_type() -> libc::c_int
{
    local_sctp_sender_dry_event()
}

#[doc(hidden)]
pub const fn test_stream_reset_event_type() -> libc::c_int
{
    local_sctp_stream_reset_event()
}

#[doc(hidden)]
pub const fn test_assoc_reset_event_type() -> libc::c_int
{
    local_sctp_assoc_reset_event()
}

#[doc(hidden)]
pub const fn test_stream_change_event_type() -> libc::c_int
{
    local_sctp_stream_change_event()
}

#[doc(hidden)]
pub const fn test_send_failed_event_type() -> libc::c_int
{
    local_sctp_send_failed_event()
}
