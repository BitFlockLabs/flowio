//! Frozen public compatibility guards for the owner-thread descriptor core.

#![allow(clippy::type_complexity)]
#![allow(unexpected_cfgs)]

use flowio::net::sctp::{
    AcceptFuture as SctpAcceptFuture, ConnectFuture as SctpConnectFuture,
    ConnectTimeoutFuture as SctpConnectTimeoutFuture, DataRecvFuture as SctpDataRecvFuture,
    DataSendFuture as SctpDataSendFuture, RecvFuture as SctpRecvFuture,
    RecvVectoredFuture as SctpRecvVectoredFuture, SctpAddStreams, SctpAssocConfig, SctpAssocStatus,
    SctpConnector, SctpInitConfig, SctpListener, SctpNotification, SctpNotificationKind,
    SctpNotificationMask, SctpPeerAddrInfo, SctpPeerAddrParams, SctpReconfigFlags, SctpRecvInfo,
    SctpRecvMeta, SctpResetStreams, SctpSendInfo, SctpSocketConfig, SctpStream,
    SendFuture as SctpSendFuture, SendVectoredFuture as SctpSendVectoredFuture,
};
use flowio::net::tcp::{
    AcceptFuture as TcpAcceptFuture, ConnectFuture as TcpConnectFuture,
    ConnectTimeoutFuture as TcpConnectTimeoutFuture, OwnedConnectFuture, OwnedConnectTimeoutFuture,
    TcpConnector, TcpListener, TcpStream,
};
use flowio::net::tls::{
    TlsClientOptions, TlsClientStream, TlsFlushFuture, TlsHandshakeFuture, TlsReadExactFuture,
    TlsReadFuture, TlsShutdownFuture, TlsWriteAllFuture, TlsWriteFuture,
};
use flowio::net::udp::{
    RecvFromFuture as UdpRecvFromFuture, RecvFuture as UdpRecvFuture,
    RecvMsgFuture as UdpRecvMsgFuture, SendFuture as UdpSendFuture,
    SendToFuture as UdpSendToFuture, UdpSocket,
};
use flowio::net::unix::UnixStream;
use flowio::net::{
    ReadExactAppendFuture, ReadExactFuture, ReadFuture, ReadvExactFuture, ReadvFuture,
    WriteAllFuture, WriteFuture, WritevAllFuture, WritevAllProjectedFuture, WritevFuture,
    WritevPieces, WritevProjectedFuture, WritevProjection,
};
use flowio::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec};
#[cfg(not(miri))]
use flowio::runtime::executor::{Executor, TrySpawnError};
use flowio::runtime::executor::{ExecutorConfig, JoinError};
use flowio::runtime::reactor::ReactorConfig;
use flowio::runtime::timer::TimeoutError;
use static_assertions::{assert_impl_all, assert_not_impl_any};
#[cfg(not(miri))]
use std::future::Future;
use std::io;
use std::mem::{align_of, size_of};
use std::panic::{RefUnwindSafe, UnwindSafe};
#[cfg(not(miri))]
use std::pin::Pin;
use std::task::Waker;
#[cfg(not(miri))]
use std::task::{Context, Poll};

const SEGMENTS: usize = 2;

struct EmptyProjection;

impl WritevProjection for EmptyProjection {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (0, 0)
    }

    fn project_writev<'a>(&'a self, _pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        Ok(())
    }
}

type TcpRead = ReadFuture<'static, Vec<u8>, TcpStream>;
type TcpWrite = WriteFuture<'static, Vec<u8>, TcpStream>;
type TcpWriteAll = WriteAllFuture<'static, Vec<u8>, TcpStream>;
type TcpReadExact = ReadExactFuture<'static, Vec<u8>, TcpStream>;
type TcpReadExactAppend = ReadExactAppendFuture<'static, TcpStream>;
type TcpReadv = ReadvFuture<'static, SEGMENTS, TcpStream>;
type TcpWritev = WritevFuture<'static, IoBuffVec<SEGMENTS>, SEGMENTS, TcpStream>;
type TcpWritevAll = WritevAllFuture<'static, IoBuffVec<SEGMENTS>, SEGMENTS, TcpStream>;
type TcpWritevProjected = WritevProjectedFuture<'static, EmptyProjection, TcpStream>;
type TcpWritevAllProjected = WritevAllProjectedFuture<'static, EmptyProjection, TcpStream>;
type TcpReadvExact = ReadvExactFuture<'static, SEGMENTS, TcpStream>;
type SendChain = IoBuffReadOnlyVec<Vec<u8>, SEGMENTS>;
type TcpWritevSend = WritevFuture<'static, SendChain, SEGMENTS, TcpStream>;
type TcpWritevAllSend = WritevAllFuture<'static, SendChain, SEGMENTS, TcpStream>;

// Slice 305 deliberately narrows exactly these three descriptor handles from
// the baseline's `Send + !Sync` to owner-thread `!Send + !Sync`. Their
// `UnwindSafe + !RefUnwindSafe` baseline remains unchanged.
#[cfg(flowio_slice305_baseline)]
assert_impl_all!(TcpStream: Send, UnwindSafe);
#[cfg(not(flowio_slice305_baseline))]
assert_impl_all!(TcpStream: UnwindSafe);
#[cfg(flowio_slice305_baseline)]
assert_not_impl_any!(TcpStream: Sync, RefUnwindSafe);
#[cfg(not(flowio_slice305_baseline))]
assert_not_impl_any!(TcpStream: Send, Sync, RefUnwindSafe);

#[cfg(flowio_slice305_baseline)]
assert_impl_all!(UnixStream: Send, UnwindSafe);
#[cfg(not(flowio_slice305_baseline))]
assert_impl_all!(UnixStream: UnwindSafe);
#[cfg(flowio_slice305_baseline)]
assert_not_impl_any!(UnixStream: Sync, RefUnwindSafe);
#[cfg(not(flowio_slice305_baseline))]
assert_not_impl_any!(UnixStream: Send, Sync, RefUnwindSafe);

#[cfg(flowio_slice305_baseline)]
assert_impl_all!(UdpSocket: Send, UnwindSafe);
#[cfg(not(flowio_slice305_baseline))]
assert_impl_all!(UdpSocket: UnwindSafe);
#[cfg(flowio_slice305_baseline)]
assert_not_impl_any!(UdpSocket: Sync, RefUnwindSafe);
#[cfg(not(flowio_slice305_baseline))]
assert_not_impl_any!(UdpSocket: Send, Sync, RefUnwindSafe);

// Descriptor-bearing handles that were already local retain their baseline
// matrix. Listeners deliberately retain the older unwind boundary as well.
assert_not_impl_any!(TcpConnector: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_not_impl_any!(TcpListener: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_not_impl_any!(SctpConnector: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_not_impl_any!(SctpStream: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_not_impl_any!(SctpListener: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_not_impl_any!(TlsClientStream: Send, Sync, UnwindSafe, RefUnwindSafe);

macro_rules! assert_local_unwind_boundary {
    ($($future:ty),+ $(,)?) => {
        $(
            assert_not_impl_any!($future: Send, Sync, UnwindSafe, RefUnwindSafe);
        )+
    };
}

// `Vec<u8>` and the projection marker are otherwise-Send owners. These checks
// therefore bind locality to the descriptor-bearing future, not its buffer.
assert_impl_all!(SendChain: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(EmptyProjection: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_local_unwind_boundary!(
    TcpRead,
    TcpWrite,
    TcpWriteAll,
    TcpReadExact,
    TcpReadExactAppend,
    TcpReadv,
    TcpWritevSend,
    TcpWritevAllSend,
    TcpWritevProjected,
    TcpWritevAllProjected,
    TcpReadvExact,
    UdpRecvFuture<'static, Vec<u8>>,
    UdpRecvMsgFuture<'static, Vec<u8>>,
    UdpSendFuture<'static, Vec<u8>>,
    UdpRecvFromFuture<'static, Vec<u8>>,
    UdpSendToFuture<'static, Vec<u8>>,
    SctpDataRecvFuture<'static, Vec<u8>>,
    SctpDataSendFuture<'static, Vec<u8>>,
    SctpRecvFuture<'static, Vec<u8>>,
    SctpSendFuture<'static, Vec<u8>>,
    SctpRecvVectoredFuture<'static, SEGMENTS>,
    SctpSendVectoredFuture<'static, SEGMENTS>,
    TlsHandshakeFuture<'static>,
    TlsReadFuture<'static, Vec<u8>>,
    TlsReadExactFuture<'static, Vec<u8>>,
    TlsWriteFuture<'static, Vec<u8>>,
    TlsWriteAllFuture<'static, Vec<u8>>,
    TlsFlushFuture<'static>,
    TlsShutdownFuture<'static>,
);

// Values suitable for a future bounded inter-executor request/result protocol
// stay runtime-independent. This does not authorize or implement that queue.
assert_impl_all!(ExecutorConfig: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(ReactorConfig: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(Waker: Send, Sync);
assert_impl_all!(JoinError: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(TimeoutError: Send, Sync);
assert_not_impl_any!(TimeoutError: UnwindSafe, RefUnwindSafe);
assert_impl_all!(TlsClientOptions: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpSendInfo: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpRecvInfo: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpPeerAddrParams: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpAssocConfig: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpReconfigFlags: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpResetStreams: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpAddStreams: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpPeerAddrInfo: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpAssocStatus: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpNotification: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpNotificationKind: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpRecvMeta: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpNotificationMask: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpInitConfig: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(SctpSocketConfig: Send, Sync, UnwindSafe, RefUnwindSafe);

fn assert_layout<T>(name: &str, expected_size: usize, expected_align: usize) {
    assert_eq!(size_of::<T>(), expected_size, "{name} size changed");
    assert_eq!(align_of::<T>(), expected_align, "{name} alignment changed");
}

#[test]
fn descriptor_handle_and_future_layouts_match_slice304_baseline() {
    // Exact x86-64 Linux baseline from Slice 304 commit 514d41d1. These are
    // compatibility/admission guards, not portable ABI promises.
    assert_layout::<TcpStream>("TcpStream", 8, 4);
    assert_layout::<TcpListener>("TcpListener", 64, 8);
    assert_layout::<TcpConnector>("TcpConnector", 160, 8);
    assert_layout::<UnixStream>("UnixStream", 8, 4);
    assert_layout::<UdpSocket>("UdpSocket", 40, 4);
    assert_layout::<SctpStream>("SctpStream", 72, 8);
    assert_layout::<SctpListener>("SctpListener", 232, 8);
    assert_layout::<SctpConnector>("SctpConnector", 528, 8);
    assert_layout::<TlsClientStream>("TlsClientStream", 1_256, 8);

    assert_layout::<TcpRead>("ReadFuture<Vec>", 56, 8);
    assert_layout::<TcpWrite>("WriteFuture<Vec>", 48, 8);
    assert_layout::<TcpWriteAll>("WriteAllFuture<Vec>", 64, 8);
    assert_layout::<TcpReadExact>("ReadExactFuture<Vec>", 72, 8);
    assert_layout::<TcpReadExactAppend>("ReadExactAppendFuture", 88, 8);
    assert_layout::<TcpReadv>("ReadvFuture<2>", 128, 8);
    assert_layout::<TcpWritev>("WritevFuture<2>", 112, 8);
    assert_layout::<TcpWritevAll>("WritevAllFuture<2>", 112, 8);
    assert_layout::<TcpWritevProjected>("WritevProjectedFuture", 40, 8);
    assert_layout::<TcpWritevAllProjected>("WritevAllProjectedFuture", 32, 8);
    assert_layout::<TcpReadvExact>("ReadvExactFuture<2>", 168, 8);

    assert_layout::<UdpRecvFuture<'static, Vec<u8>>>("udp::RecvFuture<Vec>", 56, 8);
    assert_layout::<UdpRecvMsgFuture<'static, Vec<u8>>>("udp::RecvMsgFuture<Vec>", 56, 8);
    assert_layout::<UdpSendFuture<'static, Vec<u8>>>("udp::SendFuture<Vec>", 48, 8);
    assert_layout::<UdpRecvFromFuture<'static, Vec<u8>>>("udp::RecvFromFuture<Vec>", 56, 8);
    assert_layout::<UdpSendToFuture<'static, Vec<u8>>>("udp::SendToFuture<Vec>", 80, 8);

    assert_layout::<SctpDataRecvFuture<'static, Vec<u8>>>("sctp::DataRecvFuture<Vec>", 56, 8);
    assert_layout::<SctpDataSendFuture<'static, Vec<u8>>>("sctp::DataSendFuture<Vec>", 48, 8);
    assert_layout::<SctpRecvFuture<'static, Vec<u8>>>("sctp::RecvFuture<Vec>", 64, 8);
    assert_layout::<SctpSendFuture<'static, Vec<u8>>>("sctp::SendFuture<Vec>", 64, 8);
    assert_layout::<SctpRecvVectoredFuture<'static, SEGMENTS>>(
        "sctp::RecvVectoredFuture<2>",
        136,
        8,
    );
    assert_layout::<SctpSendVectoredFuture<'static, SEGMENTS>>(
        "sctp::SendVectoredFuture<2>",
        128,
        8,
    );

    assert_layout::<TlsHandshakeFuture<'static>>("TlsHandshakeFuture", 8, 8);
    assert_layout::<TlsReadFuture<'static, Vec<u8>>>("TlsReadFuture<Vec>", 64, 8);
    assert_layout::<TlsReadExactFuture<'static, Vec<u8>>>("TlsReadExactFuture<Vec>", 72, 8);
    assert_layout::<TlsWriteFuture<'static, Vec<u8>>>("TlsWriteFuture<Vec>", 48, 8);
    assert_layout::<TlsWriteAllFuture<'static, Vec<u8>>>("TlsWriteAllFuture<Vec>", 56, 8);
    assert_layout::<TlsFlushFuture<'static>>("TlsFlushFuture", 8, 8);
    assert_layout::<TlsShutdownFuture<'static>>("TlsShutdownFuture", 8, 8);

    assert_layout::<TcpAcceptFuture<'static>>("tcp::AcceptFuture", 24, 8);
    assert_layout::<TcpConnectFuture<'static>>("tcp::ConnectFuture", 8, 8);
    assert_layout::<TcpConnectTimeoutFuture<'static>>("tcp::ConnectTimeoutFuture", 48, 8);
    assert_layout::<OwnedConnectFuture>("tcp::OwnedConnectFuture", 160, 8);
    assert_layout::<OwnedConnectTimeoutFuture>("tcp::OwnedConnectTimeoutFuture", 200, 8);
    assert_layout::<SctpAcceptFuture<'static>>("sctp::AcceptFuture", 192, 8);
    assert_layout::<SctpConnectFuture<'static>>("sctp::ConnectFuture", 40, 8);
    assert_layout::<SctpConnectTimeoutFuture<'static>>("sctp::ConnectTimeoutFuture", 80, 8);
}

#[cfg(not(miri))]
struct FixedSlotFuture<const N: usize> {
    bytes: [u8; N],
}

#[cfg(not(miri))]
impl<const N: usize> FixedSlotFuture<N> {
    fn new() -> Self {
        Self { bytes: [0; N] }
    }
}

#[cfg(not(miri))]
impl<const N: usize> Future for FixedSlotFuture<N> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        std::hint::black_box(&self.bytes);
        Poll::Ready(())
    }
}

#[test]
#[cfg(not(miri))]
fn fixed_task_slot_admission_boundary_is_unchanged() {
    let mut executor = Executor::new().expect("executor construction failed");
    executor
        .run(async {
            Executor::try_spawn(FixedSlotFuture::<4_078>::new())
                .expect("largest fixed-slot payload should remain admissible")
                .await
                .expect("admitted boundary task should complete");

            match Executor::try_spawn(FixedSlotFuture::<4_079>::new()) {
                Err(TrySpawnError::TaskTooLarge { future: _future }) => {}
                Err(_) => panic!("oversized boundary task returned the wrong rejection"),
                Ok(_) => panic!("first oversized fixed-slot payload was admitted"),
            }
        })
        .expect("executor run failed");
}
