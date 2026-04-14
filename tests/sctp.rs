mod common;

use common::TestIoBuffMut as IoBuffMut;
use flowio::net::sctp::{
    SctpAddStreams, SctpAssocConfig, SctpAssocStatus, SctpConnector, SctpInitConfig, SctpListener,
    SctpNotification, SctpNotificationKind, SctpNotificationMask, SctpPeerAddrInfo,
    SctpPeerAddrParams, SctpReconfigFlags, SctpRecvMeta, SctpResetStreams, SctpSendInfo,
    SctpSocketConfig, test_accept_slot_drop_future_closes_completed_fd,
    test_adaptation_indication_type, test_assoc_change_type, test_assoc_reset_event_type,
    test_connect_slot_drop_future_closes_socket_fd, test_parse_notification,
    test_partial_delivery_event_type, test_peer_addr_change_type, test_remote_error_type,
    test_send_failed_event_type, test_sender_dry_event_type, test_shutdown_event_type,
    test_stream_change_event_type, test_stream_reset_event_type,
};
use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::timeout;
use std::time::Duration;

fn sctp_unsupported(err: &std::io::Error) -> bool
{
    matches!(
        err.raw_os_error(),
        Some(libc::EPROTONOSUPPORT)
            | Some(libc::ESOCKTNOSUPPORT)
            | Some(libc::EAFNOSUPPORT)
            | Some(libc::EPFNOSUPPORT)
            | Some(libc::EINVAL)
    )
}

fn notification_buffer(notification_type: libc::c_int, flags: u16, len: usize) -> Vec<u8>
{
    let mut buf = vec![0u8; len];
    buf[0..2].copy_from_slice(&(notification_type as u16).to_ne_bytes());
    buf[2..4].copy_from_slice(&flags.to_ne_bytes());
    buf[4..8].copy_from_slice(&(len as u32).to_ne_bytes());
    buf
}

fn localhost_sockaddr_storage(port: u16) -> libc::sockaddr_storage
{
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: port.to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
        },
        sin_zero: [0; 8],
    };
    unsafe {
        std::ptr::write_unaligned(
            &mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in,
            addr,
        );
    }
    storage
}

#[test]
fn sctp_accept_slot_drop_future_closes_completed_fd()
{
    test_accept_slot_drop_future_closes_completed_fd().unwrap();
}

#[test]
fn sctp_connect_slot_drop_future_closes_socket_fd()
{
    test_connect_slot_drop_future_closes_socket_fd().unwrap();
}

#[test]
fn parse_assoc_change_notification()
{
    let mut buf = vec![0u8; 20];
    buf[0..2].copy_from_slice(&(test_assoc_change_type() as u16).to_ne_bytes());
    buf[2..4].copy_from_slice(&0u16.to_ne_bytes());
    buf[4..8].copy_from_slice(&(20u32).to_ne_bytes());
    buf[8..10].copy_from_slice(&1u16.to_ne_bytes());
    buf[10..12].copy_from_slice(&2u16.to_ne_bytes());
    buf[12..14].copy_from_slice(&3u16.to_ne_bytes());
    buf[14..16].copy_from_slice(&4u16.to_ne_bytes());
    buf[16..20].copy_from_slice(&5i32.to_ne_bytes());

    let parsed = test_parse_notification(&buf).expect("assoc change parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::AssocChange {
            state: 1,
            error: 2,
            outbound_streams: 3,
            inbound_streams: 4,
            assoc_id: 5,
        })
    );
}

#[test]
fn parse_adaptation_notification()
{
    let mut buf = vec![0u8; 16];
    buf[0..2].copy_from_slice(&(test_adaptation_indication_type() as u16).to_ne_bytes());
    buf[2..4].copy_from_slice(&0u16.to_ne_bytes());
    buf[4..8].copy_from_slice(&(16u32).to_ne_bytes());
    buf[8..12].copy_from_slice(&0x0102_0304u32.to_ne_bytes());
    buf[12..16].copy_from_slice(&7i32.to_ne_bytes());

    let parsed = test_parse_notification(&buf).expect("adaptation parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::Adaptation {
            indication: 0x0102_0304,
            assoc_id: 7,
        })
    );
}

#[test]
fn parse_send_failed_event_notification()
{
    let sndinfo_len = std::mem::size_of::<libc::sctp_sndinfo>();
    let mut buf = vec![0u8; 12 + sndinfo_len + 4];
    buf[0..2].copy_from_slice(&(test_send_failed_event_type() as u16).to_ne_bytes());
    buf[2..4].copy_from_slice(&0u16.to_ne_bytes());
    buf[4..8].copy_from_slice(&((12 + sndinfo_len + 4) as u32).to_ne_bytes());
    buf[8..12].copy_from_slice(&9u32.to_ne_bytes());

    let sndinfo = libc::sctp_sndinfo {
        snd_sid: 3,
        snd_flags: 4,
        snd_ppid: (0x0506_0708u32).to_be(),
        snd_context: 10,
        snd_assoc_id: 11,
    };
    unsafe {
        std::ptr::write_unaligned(buf.as_mut_ptr().add(12) as *mut libc::sctp_sndinfo, sndinfo);
    }
    let assoc_base = 12 + sndinfo_len;
    buf[assoc_base..assoc_base + 4].copy_from_slice(&12i32.to_ne_bytes());

    let parsed = test_parse_notification(&buf).expect("send failed parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::SendFailed {
            error: 9,
            info: SctpSendInfo {
                stream_id: 3,
                flags: 4,
                ppid: 0x0506_0708,
                context: 10,
                assoc_id: 11,
            },
            assoc_id: 12,
        })
    );
}

#[test]
fn parse_peer_addr_change_notification()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
    let mut buf = notification_buffer(test_peer_addr_change_type(), 0, 8 + storage_len + 12);
    let storage = localhost_sockaddr_storage(3868);
    unsafe {
        std::ptr::copy_nonoverlapping(
            &storage as *const libc::sockaddr_storage as *const u8,
            buf.as_mut_ptr().add(8),
            storage_len,
        );
    }
    let base = 8 + storage_len;
    buf[base..base + 4].copy_from_slice(&SctpPeerAddrInfo::ACTIVE.to_ne_bytes());
    buf[base + 4..base + 8].copy_from_slice(&9i32.to_ne_bytes());
    buf[base + 8..base + 12].copy_from_slice(&10i32.to_ne_bytes());

    let parsed = test_parse_notification(&buf).expect("peer addr change parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::PeerAddrChange {
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
            state: SctpPeerAddrInfo::ACTIVE,
            error: 9,
            assoc_id: 10,
        })
    );
}

#[test]
fn parse_remote_error_and_shutdown_notifications()
{
    let mut remote_error = notification_buffer(test_remote_error_type(), 0, 14);
    remote_error[8..10].copy_from_slice(&0x1122u16.to_be_bytes());
    remote_error[10..14].copy_from_slice(&12i32.to_ne_bytes());
    let parsed = test_parse_notification(&remote_error).expect("remote error parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::RemoteError {
            error: 0x1122,
            assoc_id: 12,
        })
    );

    let mut shutdown = notification_buffer(test_shutdown_event_type(), 0, 12);
    shutdown[8..12].copy_from_slice(&13i32.to_ne_bytes());
    let parsed = test_parse_notification(&shutdown).expect("shutdown parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::Shutdown { assoc_id: 13 })
    );

    let mut sender_dry = notification_buffer(test_sender_dry_event_type(), 0, 12);
    sender_dry[8..12].copy_from_slice(&14i32.to_ne_bytes());
    let parsed = test_parse_notification(&sender_dry).expect("sender dry parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::SenderDry { assoc_id: 14 })
    );
}

#[test]
fn parse_partial_delivery_and_reset_notifications()
{
    let mut partial_delivery = notification_buffer(test_partial_delivery_event_type(), 0, 24);
    partial_delivery[8..12].copy_from_slice(&7u32.to_ne_bytes());
    partial_delivery[12..16].copy_from_slice(&15i32.to_ne_bytes());
    partial_delivery[16..20].copy_from_slice(&16u32.to_ne_bytes());
    partial_delivery[20..24].copy_from_slice(&17u32.to_ne_bytes());
    let parsed = test_parse_notification(&partial_delivery).expect("partial delivery parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::PartialDelivery {
            indication: 7,
            assoc_id: 15,
            stream: 16,
            sequence: 17,
        })
    );

    let mut stream_reset = notification_buffer(test_stream_reset_event_type(), 0x0123, 12);
    stream_reset[8..12].copy_from_slice(&18i32.to_ne_bytes());
    let parsed = test_parse_notification(&stream_reset).expect("stream reset parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::StreamReset {
            flags: 0x0123,
            assoc_id: 18,
        })
    );

    let mut assoc_reset = notification_buffer(test_assoc_reset_event_type(), 0x0456, 20);
    assoc_reset[8..12].copy_from_slice(&19i32.to_ne_bytes());
    assoc_reset[12..16].copy_from_slice(&20u32.to_ne_bytes());
    assoc_reset[16..20].copy_from_slice(&21u32.to_ne_bytes());
    let parsed = test_parse_notification(&assoc_reset).expect("assoc reset parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::AssocReset {
            flags: 0x0456,
            assoc_id: 19,
            local_tsn: 20,
            remote_tsn: 21,
        })
    );

    let mut stream_change = notification_buffer(test_stream_change_event_type(), 0x0789, 16);
    stream_change[8..12].copy_from_slice(&22i32.to_ne_bytes());
    stream_change[12..14].copy_from_slice(&23u16.to_ne_bytes());
    stream_change[14..16].copy_from_slice(&24u16.to_ne_bytes());
    let parsed = test_parse_notification(&stream_change).expect("stream change parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::StreamChange {
            flags: 0x0789,
            assoc_id: 22,
            inbound_streams: 23,
            outbound_streams: 24,
        })
    );
}

#[test]
fn parse_unknown_notification_falls_back_to_other()
{
    let parsed =
        test_parse_notification(&notification_buffer(0x9001, 0x0007, 8)).expect("other parse");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::Other {
            kind: 0x9001,
            flags: 0x0007,
            length: 8,
        })
    );
}

#[test]
fn notification_helpers()
{
    let notification = SctpNotification::Shutdown { assoc_id: 7 };
    assert_eq!(notification.kind(), SctpNotificationKind::Shutdown);

    let recv_data = SctpRecvMeta::Data(Default::default());
    assert!(recv_data.is_data());
    assert!(!recv_data.is_notification());
    assert!(recv_data.data().is_some());
    assert!(recv_data.notification().is_none());
    assert!(recv_data.into_data().is_some());

    let recv_notification = SctpRecvMeta::Notification(notification);
    assert!(!recv_notification.is_data());
    assert!(recv_notification.is_notification());
    assert!(recv_notification.data().is_none());
    assert_eq!(
        recv_notification.notification().map(|value| value.kind()),
        Some(SctpNotificationKind::Shutdown)
    );
    assert_eq!(
        recv_notification
            .into_notification()
            .map(|value| value.kind()),
        Some(SctpNotificationKind::Shutdown)
    );
}

#[test]
fn notification_mask_all_and_none_round_trip()
{
    let all = SctpNotificationMask::all();
    assert!(all.association);
    assert!(all.address);
    assert!(all.send_failure);
    assert!(all.peer_error);
    assert!(all.shutdown);
    assert!(all.partial_delivery);
    assert!(all.adaptation);
    assert!(all.authentication);
    assert!(all.sender_dry);
    assert!(all.stream_reset);
    assert!(all.assoc_reset);
    assert!(all.stream_change);

    let none = SctpNotificationMask::none();
    assert!(!none.association);
    assert!(!none.address);
    assert!(!none.send_failure);
    assert!(!none.peer_error);
    assert!(!none.shutdown);
    assert!(!none.partial_delivery);
    assert!(!none.adaptation);
    assert!(!none.authentication);
    assert!(!none.sender_dry);
    assert!(!none.stream_reset);
    assert!(!none.assoc_reset);
    assert!(!none.stream_change);
}

#[test]
fn notification_mask_defaults()
{
    assert_eq!(
        SctpNotificationMask::default(),
        SctpNotificationMask::signaling_default()
    );
    assert!(!SctpNotificationMask::none().association);
    assert!(SctpNotificationMask::all().authentication);
}

#[test]
fn runtime_sctp_ping_pong()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    assert_eq!(init, SctpInitConfig::default());

    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init)
        {
            Ok(listener) => listener,
            Err(err) =>
            {
                if sctp_unsupported(&err)
                {
                    eprintln!("skipping runtime_sctp_ping_pong: SCTP unsupported ({err})");
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(SctpInitConfig::diameter_default());
    let msg_size = 256;

    executor
        .run(async move {
            let srv_buf = vec![0u8; msg_size];
            let cli_send = b"ping".to_vec();
            let cli_recv = vec![0u8; msg_size];

            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                assert_eq!(stream.local_addr().expect("local_addr failed"), addr);

                // Skip notifications until we get a data message.
                let mut current_buf = srv_buf;
                let (recv_len, meta, recv_buf) = loop
                {
                    let recv_res = stream.recv_msg(current_buf, msg_size).await;
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    match meta
                    {
                        SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                        SctpRecvMeta::Data(info) =>
                        {
                            break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                        }
                    }
                };
                assert_eq!(recv_len, 4);
                match meta
                {
                    SctpRecvMeta::Data(info) =>
                    {
                        assert_eq!(info.stream_id, 1);
                        assert_eq!(info.ppid, 0x0102_0304);
                    }
                    _ => panic!("expected data, got notification"),
                }

                let (send_res, _buf) = stream
                    .send_msg(
                        recv_buf,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            assert_eq!(stream.peer_addr(), addr);
            assert_eq!(stream.remote_addr(), addr);

            let client_local_addr = stream.local_addr().expect("client local_addr failed");
            assert_ne!(client_local_addr, addr);

            let client_local_addrs = stream.local_addrs().expect("client local_addrs failed");
            assert!(client_local_addrs.contains(&client_local_addr));

            let client_peer_addrs = stream.peer_addrs().expect("client peer_addrs failed");
            assert!(client_peer_addrs.contains(&addr));

            let status = stream.status().expect("client status failed");
            assert_eq!(status.state, SctpAssocStatus::ESTABLISHED);
            assert!(status.inbound_streams > 0);
            assert!(status.outbound_streams > 0);
            assert_eq!(status.primary_path.address, addr);

            let primary_info = stream
                .primary_path_info()
                .expect("primary_path_info failed");
            assert_eq!(primary_info.address, addr);

            let send_buffer_size = stream.send_buffer_size().expect("send_buffer_size failed");
            assert!(send_buffer_size > 0);
            stream
                .set_send_buffer_size(send_buffer_size)
                .expect("set_send_buffer_size failed");

            let recv_buffer_size = stream.recv_buffer_size().expect("recv_buffer_size failed");
            assert!(recv_buffer_size > 0);
            stream
                .set_recv_buffer_size(recv_buffer_size)
                .expect("set_recv_buffer_size failed");

            stream
                .set_notification_mask(SctpNotificationMask::signaling_default())
                .expect("set_notification_mask failed");

            stream
                .apply_assoc_config(&SctpAssocConfig {
                    assoc_max_retrans: Some(4),
                    rto_initial_ms: Some(1000),
                    rto_min_ms: Some(500),
                    rto_max_ms: Some(4000),
                })
                .expect("apply_assoc_config failed");

            let peer_info = stream.peer_addr_info(addr).expect("peer_addr_info failed");
            assert_eq!(peer_info.address, addr);

            let peer_params = stream
                .peer_addr_params(Some(addr))
                .expect("peer_addr_params failed");
            assert_eq!(peer_params.address, Some(addr));
            stream
                .set_peer_addr_params(peer_params)
                .expect("set_peer_addr_params failed");
            stream
                .set_default_peer_addr_params(SctpPeerAddrParams::association_default())
                .expect("set_default_peer_addr_params failed");
            stream
                .set_primary_addr(addr)
                .expect("set_primary_addr failed");

            // set_peer_primary_addr may fail with EPERM/EACCES/EOPNOTSUPP depending on kernel.
            if let Err(err) = stream.set_peer_primary_addr(client_local_addr)
            {
                let raw = err.raw_os_error();
                assert!(
                    matches!(
                        raw,
                        Some(libc::EPERM) | Some(libc::EACCES) | Some(libc::EOPNOTSUPP)
                    ),
                    "set_peer_primary_addr failed unexpectedly: {err}"
                );
            }

            let reconfig = stream
                .reconfig_supported()
                .expect("reconfig_supported failed");
            let enable_res = stream.enable_stream_reset(SctpReconfigFlags {
                assoc_id: reconfig.assoc_id,
                flags: SctpReconfigFlags::RESET_STREAMS | SctpReconfigFlags::CHANGE_ASSOC,
            });
            // Stream reconfiguration may not be supported on all kernels.
            if let Err(err) = enable_res
            {
                let raw = err.raw_os_error();
                assert!(
                    matches!(
                        raw,
                        Some(libc::EOPNOTSUPP)
                            | Some(libc::ENOPROTOOPT)
                            | Some(libc::EINVAL)
                            | Some(libc::EPERM)
                            | Some(libc::EACCES)
                    ),
                    "enable_stream_reset failed unexpectedly: {err}"
                );
            }
            else
            {
                if let Err(err) = stream.reset_streams(&SctpResetStreams::outgoing(&[1]))
                {
                    let raw = err.raw_os_error();
                    assert!(
                        matches!(
                            raw,
                            Some(libc::EOPNOTSUPP)
                                | Some(libc::ENOPROTOOPT)
                                | Some(libc::EINVAL)
                                | Some(libc::EPERM)
                                | Some(libc::EACCES)
                        ),
                        "reset_streams failed unexpectedly: {err}"
                    );
                }

                if let Err(err) = stream.add_streams(SctpAddStreams::new(1, 1))
                {
                    let raw = err.raw_os_error();
                    assert!(
                        matches!(
                            raw,
                            Some(libc::EOPNOTSUPP)
                                | Some(libc::ENOPROTOOPT)
                                | Some(libc::EINVAL)
                                | Some(libc::EPERM)
                                | Some(libc::EACCES)
                        ),
                        "add_streams failed unexpectedly: {err}"
                    );
                }
            }

            let (send_res, _) = stream
                .send_msg(
                    cli_send,
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            send_res.expect("client send failed");

            // Skip notifications until we get data back.
            let mut current_buf = cli_recv;
            let (recv_len, meta, recv_buf) = loop
            {
                let recv_res = stream.recv_msg(current_buf, msg_size).await;
                let (recv_len, meta) = recv_res.0.expect("client recv failed");
                match meta
                {
                    SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                    SctpRecvMeta::Data(info) =>
                    {
                        break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                    }
                }
            };
            assert_eq!(recv_len, 4);
            assert_eq!(&recv_buf[..recv_len], b"ping");
            match meta
            {
                SctpRecvMeta::Data(info) =>
                {
                    assert_eq!(info.stream_id, 1);
                    assert_eq!(info.ppid, 0x0102_0304);
                }
                _ => panic!("expected data, got notification"),
            }

            stream
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_default_peer_addr_params_rejects_specific_address()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    )
    {
        Ok(listener) => listener,
        Err(err) =>
        {
            if sctp_unsupported(&err)
            {
                eprintln!(
                    "skipping runtime_sctp_default_peer_addr_params_rejects_specific_address: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let _ = listener.accept().await.expect("accept failed");
            })
            .expect("server spawn failed");

            let stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let err = stream
                .set_default_peer_addr_params(SctpPeerAddrParams::for_address(addr))
                .expect_err("specific-address default peer params should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_fast_send_recv()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.default_send_info = Some(SctpSendInfo {
        stream_id: 1,
        flags: 0,
        ppid: 0x0102_0304,
        context: 0,
        assoc_id: 0,
    });

    let mut listener = match SctpListener::bind_with_config(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        socket_config,
    )
    {
        Ok(listener) => listener,
        Err(err) =>
        {
            if sctp_unsupported(&err)
            {
                eprintln!("skipping runtime_sctp_fast_send_recv: SCTP unsupported ({err})");
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(socket_config);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                let recv = IoBuffMut::new(0, 64, 0);
                let (recv_res, recv_buf) = stream.recv(recv, 4).await;
                let recv_len = recv_res.expect("server recv failed");
                assert_eq!(recv_len, 4);
                assert_eq!(recv_buf.payload_bytes(), b"ping");

                let (send_res, _buf) = stream.send(recv_buf).await;
                assert_eq!(send_res.expect("server send failed"), 4);
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let mut send = IoBuffMut::new(0, 64, 0);
            send.payload_append(b"ping").unwrap();
            let (send_res, _buf) = stream.send(send).await;
            assert_eq!(send_res.expect("client send failed"), 4);

            let recv = IoBuffMut::new(0, 64, 0);
            let (recv_res, recv_buf) = stream.recv(recv, 4).await;
            let recv_len = recv_res.expect("client recv failed");
            assert_eq!(recv_len, 4);
            assert_eq!(recv_buf.payload_bytes(), b"ping");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_multistream_long_lived()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig {
        outbound_streams: 8,
        inbound_streams: 8,
        ..SctpInitConfig::diameter_default()
    };
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init)
        {
            Ok(listener) => listener,
            Err(err) =>
            {
                if sctp_unsupported(&err)
                {
                    eprintln!(
                        "skipping runtime_sctp_multistream_long_lived: SCTP unsupported ({err})"
                    );
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);
    let rounds = 32usize;
    let stream_count = 4usize;
    let msg_size = 256usize;

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");

                let mut current_buf = vec![0u8; msg_size];
                for round in 0..rounds
                {
                    let expected_stream = (round % stream_count) as u16;
                    let expected_ppid = 0x0102_0304u32 + round as u32;
                    let expected_payload = format!("ping-{round:02}-stream-{expected_stream}");

                    let (recv_len, info, recv_buf) = loop
                    {
                        let recv_res = stream.recv_msg(current_buf, msg_size).await;
                        let (recv_len, meta) = recv_res.0.expect("server recv failed");
                        match meta
                        {
                            SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                            SctpRecvMeta::Data(info) => break (recv_len, info, recv_res.1),
                        }
                    };
                    assert_eq!(recv_len, expected_payload.len());
                    assert_eq!(&recv_buf[..recv_len], expected_payload.as_bytes());
                    assert_eq!(info.stream_id, expected_stream);
                    assert_eq!(info.ppid, expected_ppid);

                    current_buf = recv_buf;
                    let (send_res, send_buf) = stream
                        .send_msg(
                            current_buf,
                            SctpSendInfo {
                                stream_id: expected_stream,
                                flags: 0,
                                ppid: expected_ppid,
                                context: round as u32,
                                assoc_id: 0,
                            },
                        )
                        .await;
                    assert_eq!(
                        send_res.expect("server send failed"),
                        expected_payload.len()
                    );
                    current_buf = send_buf;
                }
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let mut current_buf = vec![0u8; msg_size];
            for round in 0..rounds
            {
                let stream_id = (round % stream_count) as u16;
                let ppid = 0x0102_0304u32 + round as u32;
                let payload = format!("ping-{round:02}-stream-{stream_id}").into_bytes();

                let (send_res, _buf) = stream
                    .send_msg(
                        payload.clone(),
                        SctpSendInfo {
                            stream_id,
                            flags: 0,
                            ppid,
                            context: round as u32,
                            assoc_id: 0,
                        },
                    )
                    .await;
                assert_eq!(send_res.expect("client send failed"), payload.len());

                let (recv_len, info, recv_buf) = loop
                {
                    let recv_res = stream.recv_msg(current_buf, msg_size).await;
                    let (recv_len, meta) = recv_res.0.expect("client recv failed");
                    match meta
                    {
                        SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                        SctpRecvMeta::Data(info) => break (recv_len, info, recv_res.1),
                    }
                };
                assert_eq!(recv_len, payload.len());
                assert_eq!(&recv_buf[..recv_len], payload.as_slice());
                assert_eq!(info.stream_id, stream_id);
                assert_eq!(info.ppid, ppid);
                current_buf = recv_buf;
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_shutdown_write_peer_observes_terminal_state()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    )
    {
        Ok(listener) => listener,
        Err(err) =>
        {
            if sctp_unsupported(&err)
            {
                eprintln!(
                    "skipping runtime_sctp_shutdown_write_peer_observes_terminal_state: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                let mut current_buf = vec![0u8; 256];
                let mut saw_data = false;
                let mut saw_shutdown_notification = false;
                let mut saw_eof = false;

                while !(saw_data && (saw_shutdown_notification || saw_eof))
                {
                    let recv_res =
                        timeout(Duration::from_secs(1), stream.recv_msg(current_buf, 256))
                            .await
                            .expect("server recv timed out");
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    current_buf = recv_res.1;

                    match meta
                    {
                        SctpRecvMeta::Notification(SctpNotification::Shutdown { .. }) =>
                        {
                            saw_shutdown_notification = true;
                        }
                        SctpRecvMeta::Notification(_) =>
                        {}
                        SctpRecvMeta::Data(info) =>
                        {
                            assert!(!saw_data, "unexpected extra data after shutdown");
                            assert_eq!(recv_len, 4);
                            assert_eq!(&current_buf[..recv_len], b"ping");
                            assert_eq!(info.stream_id, 1);
                            assert_eq!(info.ppid, 0x0102_0304);
                            saw_data = true;
                            if recv_len == 0
                            {
                                saw_eof = true;
                            }
                        }
                    }

                    if recv_len == 0
                    {
                        saw_eof = true;
                    }
                }

                assert!(
                    saw_data,
                    "peer did not receive the queued data before shutdown"
                );
                assert!(
                    saw_shutdown_notification || saw_eof,
                    "peer did not observe SCTP shutdown notification or EOF"
                );
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let (send_res, _buf) = stream
                .send_msg(
                    b"ping".to_vec(),
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            assert_eq!(send_res.expect("client send failed"), 4);

            stream
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_connect_timeout_success()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init)
        {
            Ok(listener) => listener,
            Err(err) =>
            {
                if sctp_unsupported(&err)
                {
                    eprintln!(
                        "skipping runtime_sctp_connect_timeout_success: SCTP unsupported ({err})"
                    );
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let _ = listener.accept().await.expect("accept failed");
            })
            .expect("server spawn failed");

            let stream = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await
                .expect("connect_timeout failed");
            assert_eq!(stream.peer_addr(), addr);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_connect_timeout_propagates_connect_error()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let listener = match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init)
    {
        Ok(listener) => listener,
        Err(err) =>
        {
            if sctp_unsupported(&err)
            {
                eprintln!(
                    "skipping runtime_sctp_connect_timeout_propagates_connect_error: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };
    let addr = listener.local_addr();
    drop(listener);

    let mut executor = Executor::new().expect("failed to construct executor");
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            let result = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result
            {
                Ok(_) => panic!("connect_timeout should propagate connect failure"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
        })
        .expect("executor run failed");
}

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// SCTP ping-pong using IoBuffMut for send/recv instead of Vec<u8>.
#[test]
fn runtime_sctp_ping_pong_iobuff()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init)
        {
            Ok(listener) => listener,
            Err(err) =>
            {
                if sctp_unsupported(&err)
                {
                    eprintln!("skipping runtime_sctp_ping_pong_iobuff: SCTP unsupported ({err})");
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);
    let msg_size = 256;

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");

                // Receive with IoBuffMut, skip notifications.
                // Reset the buffer before reuse: IoBuffMut::as_mut_ptr() advances
                // past already-written payload, so without reset the next recv
                // would write at the wrong offset.
                let mut current_buf = IoBuffMut::new(0, msg_size, 0);
                let (recv_len, meta, recv_buf) = loop
                {
                    let recv_res = stream.recv_msg(current_buf, msg_size).await;
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    match meta
                    {
                        SctpRecvMeta::Notification(_) =>
                        {
                            let mut buf = recv_res.1;
                            buf.reset();
                            current_buf = buf;
                        }
                        SctpRecvMeta::Data(info) =>
                        {
                            break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                        }
                    }
                };
                assert_eq!(recv_len, 4);
                assert_eq!(recv_buf.payload_bytes()[..recv_len], *b"ping");
                match meta
                {
                    SctpRecvMeta::Data(info) =>
                    {
                        assert_eq!(info.stream_id, 1);
                        assert_eq!(info.ppid, 0x0102_0304);
                    }
                    _ => panic!("expected data, got notification"),
                }

                // Echo back using the received IoBuffMut directly.
                let (send_res, _buf) = stream
                    .send_msg(
                        recv_buf,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            // Send with IoBuffMut.
            let mut cli_send = IoBuffMut::new(0, msg_size, 0);
            cli_send.payload_append(b"ping").unwrap();
            let (send_res, _) = stream
                .send_msg(
                    cli_send,
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            send_res.expect("client send failed");

            // Receive with IoBuffMut, skip notifications.
            let mut current_buf = IoBuffMut::new(0, msg_size, 0);
            let (recv_len, meta, recv_buf) = loop
            {
                let recv_res = stream.recv_msg(current_buf, msg_size).await;
                let (recv_len, meta) = recv_res.0.expect("client recv failed");
                match meta
                {
                    SctpRecvMeta::Notification(_) =>
                    {
                        let mut buf = recv_res.1;
                        buf.reset();
                        current_buf = buf;
                    }
                    SctpRecvMeta::Data(info) =>
                    {
                        break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                    }
                }
            };
            assert_eq!(recv_len, 4);
            assert_eq!(recv_buf.payload_bytes()[..recv_len], *b"ping");
            match meta
            {
                SctpRecvMeta::Data(info) =>
                {
                    assert_eq!(info.stream_id, 1);
                    assert_eq!(info.ppid, 0x0102_0304);
                }
                _ => panic!("expected data, got notification"),
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_rejects_oversize_iobuff()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    )
    {
        Ok(listener) => listener,
        Err(err) =>
        {
            if sctp_unsupported(&err)
            {
                eprintln!(
                    "skipping runtime_sctp_recv_msg_rejects_oversize_iobuff: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (_stream, _remote) = listener.accept().await.expect("accept failed");
            })
            .expect("accept spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let recv = IoBuffMut::new(0, 4, 0);
            let (res, buf) = stream.recv_msg(recv, 5).await;
            let err = res.expect_err("oversize recv_msg should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf.payload_len(), 0);
            assert_eq!(buf.payload_remaining(), 4);
        })
        .expect("executor run failed");
}

// ============================================================================
// Vectored I/O (send_msg_vectored / recv_msg_vectored) tests
// ============================================================================

/// SCTP ping-pong using vectored send/recv with IoBuffVecMut.
#[test]
fn runtime_sctp_ping_pong_vectored()
{
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init)
        {
            Ok(listener) => listener,
            Err(err) =>
            {
                if sctp_unsupported(&err)
                {
                    eprintln!("skipping runtime_sctp_ping_pong_vectored: SCTP unsupported ({err})");
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");

                // Receive with single-buffer recv_msg, skip notifications.
                let mut current_buf = vec![0u8; 256];
                let (recv_len, _meta, _recv_buf) = loop
                {
                    let recv_res = stream.recv_msg(current_buf, 256).await;
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    match meta
                    {
                        SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                        SctpRecvMeta::Data(info) =>
                        {
                            break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                        }
                    }
                };
                assert_eq!(recv_len, 11);

                // Echo back using vectored send: 2 segments.
                let mut seg1 = IoBuffMut::new(0, 16, 0);
                seg1.payload_append(b"reply:").unwrap();
                let mut seg2 = IoBuffMut::new(0, 16, 0);
                seg2.payload_append(b"ok").unwrap();

                let mut send_chain = IoBuffVecMut::<2>::new();
                send_chain.push(seg1).unwrap();
                send_chain.push(seg2).unwrap();
                let frozen = send_chain.freeze();

                let (send_res, _chain) = stream
                    .send_msg_vectored(
                        frozen,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send_msg_vectored failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            // Send with vectored: 3 segments "hello" + " " + "world".
            let mut seg1 = IoBuffMut::new(0, 16, 0);
            seg1.payload_append(b"hello").unwrap();
            let mut seg2 = IoBuffMut::new(0, 16, 0);
            seg2.payload_append(b" ").unwrap();
            let mut seg3 = IoBuffMut::new(0, 16, 0);
            seg3.payload_append(b"world").unwrap();

            let mut send_chain = IoBuffVecMut::<3>::new();
            send_chain.push(seg1).unwrap();
            send_chain.push(seg2).unwrap();
            send_chain.push(seg3).unwrap();
            let frozen = send_chain.freeze();

            let (send_res, _chain) = stream
                .send_msg_vectored(
                    frozen,
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            send_res.expect("client send_msg_vectored failed");

            // Receive reply with vectored recv, skip notifications.
            let mut recv_chain = IoBuffVecMut::<2>::new();
            recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
            recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();

            let (recv_len, meta, _recv_chain) = loop
            {
                let recv_res = stream.recv_msg_vectored(recv_chain).await;
                let (recv_len, meta) = recv_res.0.expect("client recv_msg_vectored failed");
                match meta
                {
                    SctpRecvMeta::Notification(_) =>
                    {
                        // Rebuild chain for retry (old one consumed by rental).
                        recv_chain = IoBuffVecMut::<2>::new();
                        recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
                        recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
                    }
                    SctpRecvMeta::Data(info) =>
                    {
                        break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                    }
                }
            };
            assert_eq!(recv_len, 8);
            match meta
            {
                SctpRecvMeta::Data(info) =>
                {
                    assert_eq!(info.stream_id, 1);
                    assert_eq!(info.ppid, 0x0102_0304);
                }
                _ => panic!("expected data, got notification"),
            }
        })
        .expect("executor run failed");
}
